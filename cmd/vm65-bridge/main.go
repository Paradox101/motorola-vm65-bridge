// Command vm65-bridge exposes a Motorola VM65 camera as a local RTSP-over-TCP
// endpoint, tunneling every byte through the reconstructed Magic WEB2 relay.
// Point any RTSP player, go2rtc or Home Assistant at the local address and it
// reaches the camera as if it were on the LAN.
//
// The bridge does not perform the 5GenCare control flow: that flow is the one
// part of the chain not reconstructable from an x86 host. Its outputs — device
// id, SID, device token and relay control host — are supplied to the bridge in
// a credentials file (see -creds). Obtaining and refreshing those credentials
// is out of scope for this tool; see docs/bridge.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/local/motorola-vm65-bridge/internal/app"
	"github.com/local/motorola-vm65-bridge/internal/bridge"
	"github.com/local/motorola-vm65-bridge/internal/buildinfo"
	appconfig "github.com/local/motorola-vm65-bridge/internal/config"
	"github.com/local/motorola-vm65-bridge/internal/devicecontrol"
	"github.com/local/motorola-vm65-bridge/internal/health"
	"github.com/local/motorola-vm65-bridge/internal/ingress"
	"github.com/local/motorola-vm65-bridge/internal/mqttdiscovery"
	"github.com/local/motorola-vm65-bridge/internal/snapshot"
	"github.com/local/motorola-vm65-bridge/internal/webui"
)

// credsFile is the on-disk shape of the credentials. It mirrors the fields
// cmd/tunnelcheck already reads, so the same local file works for both.
type credsFile struct {
	DeviceID      uint32 `json:"device_id"`
	DeviceUDID    string `json:"device_udid"`
	DeviceName    string `json:"device_name"`
	Model         string `json:"model"`
	SID           string `json:"sid"`
	DeviceToken   string `json:"device_token"`
	ControlHost   string `json:"control_host"`
	ControlPort   int    `json:"control_port"`
	TargetPort    int    `json:"target_port"`
	DeviceAPIHost string `json:"device_api_host"`
	DeviceAPIPort int    `json:"device_api_port"`
}

func main() {
	for _, argument := range os.Args[1:] {
		if argument == "-version" || argument == "--version" {
			fmt.Println("vm65-bridge", buildinfo.String())
			return
		}
	}
	cfg, err := appconfig.Load(os.Args[1:], os.LookupEnv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}

	level := slog.LevelInfo
	if cfg.Verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	logger.Info("Motorola Nursery Homeassistant Bridge", "version", buildinfo.String())
	logger.Debug("configuration loaded", "config", cfg.Redacted())

	if err := run(cfg, logger); err != nil {
		logger.Error("bridge exited with error", "err", err)
		os.Exit(1)
	}
}

func run(cfg appconfig.Config, logger *slog.Logger) error {
	credentials, err := loadCredentialSet(cfg)
	if err != nil {
		return err
	}
	registry, err := app.BuildRegistry(cfg.ListenAddr, credentials)
	if err != nil {
		return err
	}
	if len(registry.Cameras) == 0 {
		return errors.New("camera registry is empty")
	}
	primary := registry.Cameras[0].Credentials

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	healthState := health.NewState(time.Now())
	healthState.SetLive(true)
	defer healthState.SetLive(false)
	healthState.SetCredentialsReady(true)
	healthState.SetGo2RTC(cfg.Go2RTCRequired, !cfg.Go2RTCRequired)
	if cfg.Go2RTCRequired {
		go monitorGo2RTC(ctx, &http.Client{Timeout: 2 * time.Second}, cfg.Go2RTCURL, time.Second, healthState)
	}

	if cfg.StatusAddr != "" {
		if err := startHTTPServer(ctx, "health endpoint", cfg.StatusAddr, health.NewHandler(healthState), logger); err != nil {
			return err
		}
	}

	// The runtime is built before the Web UI so the page can report live camera
	// state and restart a camera that got stuck.
	runtime := app.New(app.RuntimeConfig{Registry: registry, Logger: logger, Health: healthState})
	temperatures := newTemperatureStore()

	// The Web UI and the snapshot endpoint share one listener, the one the
	// Supervisor reaches through Ingress. Neither is published on the host.
	web, err := startWebServer(ctx, cfg, registry, runtime, temperatures, healthState, logger)
	if err != nil {
		return err
	}
	snapshots := web.cache()
	defer snapshots.Close()

	// Temperature is read for the Web UI, not for the broker: the store records
	// every reading and only passes it on to MQTT when a broker is configured.
	// Keeping this outside the block below is what lets a camera report its
	// temperature in an add-on that publishes nothing at all.
	temperatureSupervisor := devicecontrol.NewSupervisor(ctx, devicecontrol.SupervisorConfig{
		Client:       devicecontrol.Client{},
		Sink:         temperatures,
		PollInterval: cfg.TemperaturePollInterval,
	})
	temperatureSupervisor.Reconcile(temperatureCameras(registry))
	defer temperatureSupervisor.Close()

	var discovery *mqttdiscovery.Service
	publisher := &discoveryPublisher{snapshotToken: snapshots.Token()}
	healthState.SetMQTT(cfg.MQTT.Host != "", false)
	if cfg.MQTT.Host != "" && primary.DeviceUDID != "" && cfg.StreamURL != "" {
		discovery = mqttdiscovery.NewService(mqttdiscovery.Config{
			Host:                cfg.MQTT.Host,
			Port:                cfg.MQTT.Port,
			Username:            cfg.MQTT.Username,
			Password:            cfg.MQTT.Password,
			TLS:                 cfg.MQTT.TLS,
			DiscoveryPrefix:     cfg.MQTT.DiscoveryPrefix,
			ClientID:            "vm65-bridge-" + primary.DeviceUDID,
			Version:             buildinfo.String(),
			ConfigurationURL:    cfg.SnapshotBase,
			PublishCameraFrames: cfg.CameraRefreshInterval > 0,
			OnConnectionChange: func(connected bool) {
				healthState.SetMQTT(true, connected)
			},
		})
		err = discovery.Start(ctx)
		if err != nil {
			logger.Warn("MQTT discovery unavailable", "err", err)
			healthState.SetLastError(health.ErrorBroker)
			discovery = nil
		} else {
			publisher.service = discovery
			// Discovery is an extra, not the product: a camera whose entity
			// could not be published must not stop the bridge that is about to
			// stream it.
			if publishErr := publisher.publish(ctx, cfg, registry, logger, healthState); publishErr != nil {
				logger.Warn("some cameras could not be published to MQTT discovery", "err", publishErr)
			}
			defer func() {
				shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
				defer cancel()
				_ = discovery.Close(shutdownContext)
			}()
			// The store both records the reading for the Web UI and passes it
			// on, so the page does not need its own control link. Readings
			// taken before the broker was up stay where they are: the entity
			// gets its value at the next poll.
			temperatures.setSink(ctx, discovery, logger)
		}
	}

	logger.Info("starting Motorola Nursery Homeassistant Bridge", "listen", cfg.ListenAddr, "cameras", len(registry.Cameras), "control_host", primary.ControlHost)
	logCameraURLs(cfg, registry, logger)

	// SIGHUP swaps in freshly written credentials. Cameras whose credentials did
	// not change keep streaming, so the periodic refresh no longer costs every
	// viewer their picture.
	go watchForReload(ctx, cfg, runtime, web, publisher, temperatureSupervisor, logger, healthState)

	// Keep the diagnostic entities and per-camera availability in step with the
	// runtime, so Home Assistant shows which camera is down instead of leaving
	// every entity looking healthy.
	if publisher.service != nil {
		go mirrorStateToMQTT(ctx, publisher.service, runtime, healthState, 5*time.Second)
		// Feeding the camera entity is what puts a camera in Home Assistant
		// without anyone adding an integration by hand.
		if cfg.CameraRefreshInterval > 0 && snapshots != nil {
			go publishCameraFrames(ctx, publisher.service, snapshots, registry, cfg.CameraRefreshInterval, logger)
		}
	}

	return runtime.Run(ctx)
}

// logCameraURLs prints the URLs a person needs to add the live video by hand.
// Home Assistant cannot discover an RTSP stream over MQTT, so these are the
// values that go into the camera integration.
func logCameraURLs(cfg appconfig.Config, registry app.Registry, logger *slog.Logger) {
	if cfg.StreamURL == "" {
		return
	}
	for index, camera := range registry.Cameras {
		streamURL, err := discoveryStreamURL(cfg.StreamURL, camera.StreamName, index == 0)
		if err != nil {
			continue
		}
		// The still-image URL is deliberately not logged: it carries the
		// snapshot token, and Home Assistant receives it over MQTT discovery
		// rather than from someone copying it out of the log.
		logger.Info("camera stream ready", "camera", camera.StreamName, "stream_source", streamURL)
	}
}

// mirrorStateToMQTT publishes runtime counters and per-camera availability
// until ctx is cancelled.
func mirrorStateToMQTT(ctx context.Context, service *mqttdiscovery.Service, runtime *app.Runtime, healthState *health.State, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := healthState.Snapshot()
			_ = service.PublishStatus(ctx, mqttdiscovery.Status{
				ActiveSessions: snapshot.ActiveSessions,
				Reconnects:     snapshot.ReconnectsTotal,
			})
			for id, available := range runtime.CameraAvailability() {
				_ = service.SetCameraAvailable(ctx, id, available)
			}
		}
	}
}

// watchForReload applies a new credential file on SIGHUP until ctx is done.
func watchForReload(ctx context.Context, cfg appconfig.Config, runtime *app.Runtime, web *webStack, publisher *discoveryPublisher, temperatureSupervisor *devicecontrol.Supervisor, logger *slog.Logger, healthState *health.State) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	defer signal.Stop(signals)

	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			logger.Info("reloading camera credentials")
			credentials, err := loadCredentialSet(cfg)
			if err != nil {
				logger.Error("credential reload failed; keeping the running cameras", "err", err)
				healthState.SetLastError(health.ErrorConfiguration)
				continue
			}
			registry, err := app.BuildRegistry(cfg.ListenAddr, credentials)
			if err != nil {
				logger.Error("credential reload produced an invalid registry; keeping the running cameras", "err", err)
				healthState.SetLastError(health.ErrorConfiguration)
				continue
			}
			if err := runtime.Reload(registry); err != nil {
				logger.Error("some cameras failed to restart after reload", "err", err)
				healthState.SetLastError(health.ErrorNetwork)
			}
			// The Web UI, the stills and the media proxy each hold a camera
			// list of their own; a camera the account gained is invisible to
			// all three until they are told about it.
			web.reload(cfg, registry, logger)
			if err := publisher.publish(ctx, cfg, registry, logger, healthState); err != nil {
				logger.Error("MQTT discovery refresh failed", "err", err)
				healthState.SetLastError(health.ErrorBroker)
			}
			if temperatureSupervisor != nil {
				temperatureSupervisor.Reconcile(temperatureCameras(registry))
			}
			logger.Info("credential reload complete", "cameras", len(registry.Cameras))
		}
	}
}

func temperatureCameras(registry app.Registry) []devicecontrol.Camera {
	cameras := make([]devicecontrol.Camera, 0, len(registry.Cameras))
	for _, camera := range registry.Cameras {
		credentials := camera.Credentials
		if credentials.DeviceUDID == "" || credentials.DeviceAPIHost == "" || credentials.DeviceAPIPort < 1 || credentials.DeviceToken == "" {
			continue
		}
		cameras = append(cameras, devicecontrol.Camera{
			ID: credentials.DeviceUDID, DeviceID: credentials.DeviceID, Token: credentials.DeviceToken,
			Host: credentials.DeviceAPIHost, Port: credentials.DeviceAPIPort,
		})
	}
	return cameras
}

// discoveryPublisher keeps the retained MQTT discovery topics in step with the
// running registry, including retiring cameras that disappeared from it.
type discoveryPublisher struct {
	service   *mqttdiscovery.Service
	published []string
	// snapshotToken authorizes the snapshot URL published to Home Assistant.
	snapshotToken string
}

func (p *discoveryPublisher) publish(ctx context.Context, cfg appconfig.Config, registry app.Registry, logger *slog.Logger, healthState *health.State) error {
	if p.service == nil {
		return nil
	}
	var failures []error
	current := make(map[string]struct{}, len(registry.Cameras))
	for index, camera := range registry.Cameras {
		name := camera.Credentials.DeviceName
		if name == "" {
			name = "Motorola Nursery Camera"
		}
		// The first camera keeps the historical vm65 alias in go2rtc.
		streamName := publishedStreamName(registry, camera, index)
		streamURL, err := discoveryStreamURL(cfg.StreamURL, camera.StreamName, index == 0)
		if err != nil {
			// One camera whose URL cannot be built is not a reason to leave the
			// others undiscovered, nor to skip retiring the cameras that left
			// the registry — which returning here used to do.
			logger.Warn("could not build the discovery stream URL", "camera", camera.StreamName, "err", err)
			healthState.SetLastError(health.ErrorConfiguration)
			failures = append(failures, err)
			continue
		}
		if err := p.service.Upsert(ctx, mqttdiscovery.Camera{
			ID:          camera.Credentials.DeviceUDID,
			Name:        name,
			Model:       camera.Credentials.Model,
			StreamURL:   streamURL,
			SnapshotURL: snapshot.URL(cfg.SnapshotBase, streamName, p.snapshotToken),
		}); err != nil {
			logger.Warn("MQTT camera discovery unavailable", "camera", camera.StreamName, "err", err)
			healthState.SetLastError(health.ErrorBroker)
		}
		current[camera.Credentials.DeviceUDID] = struct{}{}
	}
	for _, id := range p.published {
		if _, kept := current[id]; kept {
			continue
		}
		if err := p.service.Remove(ctx, id); err != nil {
			logger.Warn("could not retire the discovery entry of a removed camera", "camera_id", id, "err", err)
		}
	}
	p.published = p.published[:0]
	for id := range current {
		p.published = append(p.published, id)
	}
	return errors.Join(failures...)
}

// streamNames lists every go2rtc stream this add-on owns, including the
// historical vm65 alias. Nothing outside this list may be named in a request:
// go2rtc turns an unknown src into a new stream, and "exec:" is one of its
// source schemes.
func streamNames(registry app.Registry) []string {
	names := make([]string, 0, 2*(len(registry.Cameras)+1))
	if registry.LegacyAlias != "" {
		names = append(names, registry.LegacyAlias, registry.LegacyAlias+mjpegSuffix)
	}
	for _, camera := range registry.Cameras {
		names = append(names, camera.StreamName, camera.StreamName+mjpegSuffix)
	}
	return names
}

// publishedStreams lists the one name per camera this add-on actually hands
// out — the historical vm65 alias for the first camera, its own stream name for
// the rest. Everything that touches a camera by name uses this list: go2rtc
// treats every distinct name as a separate stream with its own RTSP session, so
// asking for two names is two relay tunnels to the same camera.
func publishedStreams(registry app.Registry) []string {
	names := make([]string, 0, len(registry.Cameras))
	for index, camera := range registry.Cameras {
		names = append(names, publishedStreamName(registry, camera, index))
	}
	return names
}

// publishedStreamName is publishedStreams for one camera.
func publishedStreamName(registry app.Registry, camera app.Camera, index int) string {
	if index == 0 && registry.LegacyAlias != "" {
		return registry.LegacyAlias
	}
	return camera.StreamName
}

// mjpegSuffix names the companion stream vm65-setup generates for MJPEG. It is
// a separate stream because go2rtc refuses to transcode H264 for a plain MJPEG
// request; the two must agree on the name.
const mjpegSuffix = "-mjpeg"

// temperatureStore keeps the last reading per camera so the Web UI can show it
// without opening a second control link, and passes everything on to MQTT.
type temperatureStore struct {
	mu        sync.RWMutex
	celsius   map[string]float64
	supported map[string]bool
	sink      devicecontrol.Sink
}

func newTemperatureStore() *temperatureStore {
	return &temperatureStore{celsius: map[string]float64{}, supported: map[string]bool{}}
}

// setSink attaches a downstream sink and replays what is already known to it.
// The capability of a camera is read once per control connection, so a sink
// attached after that read would otherwise never learn a camera supports
// temperature at all, and its entity would never be created.
func (t *temperatureStore) setSink(ctx context.Context, sink devicecontrol.Sink, logger *slog.Logger) {
	t.mu.Lock()
	t.sink = sink
	supported := make(map[string]bool, len(t.supported))
	for id, value := range t.supported {
		supported[id] = value
	}
	celsius := make(map[string]float64, len(t.celsius))
	for id, value := range t.celsius {
		celsius[id] = value
	}
	t.mu.Unlock()

	for id, value := range supported {
		if err := sink.SetTemperatureSupported(ctx, id, value); err != nil {
			logger.Warn("could not replay a temperature capability", "camera_id", id, "err", err)
		}
	}
	for id, value := range celsius {
		if err := sink.PublishTemperature(ctx, id, value); err != nil {
			logger.Warn("could not replay a temperature reading", "camera_id", id, "err", err)
		}
	}
}

func (t *temperatureStore) SetTemperatureSupported(ctx context.Context, id string, supported bool) error {
	t.mu.Lock()
	t.supported[id] = supported
	if !supported {
		delete(t.celsius, id)
	}
	sink := t.sink
	t.mu.Unlock()
	if sink == nil {
		return nil
	}
	return sink.SetTemperatureSupported(ctx, id, supported)
}

func (t *temperatureStore) SetTemperatureAvailable(ctx context.Context, id string, available bool) error {
	t.mu.Lock()
	if !available {
		delete(t.celsius, id)
	}
	sink := t.sink
	t.mu.Unlock()
	if sink == nil {
		return nil
	}
	return sink.SetTemperatureAvailable(ctx, id, available)
}

func (t *temperatureStore) PublishTemperature(ctx context.Context, id string, celsius float64) error {
	t.mu.Lock()
	t.celsius[id] = celsius
	sink := t.sink
	t.mu.Unlock()
	if sink == nil {
		return nil
	}
	return sink.PublishTemperature(ctx, id, celsius)
}

// reading reports the last temperature for one camera, if there is one.
func (t *temperatureStore) reading(id string) (float64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	celsius, ok := t.celsius[id]
	return celsius, ok
}

// uiSource adapts the runtime, the health state and the registry to what the
// Web UI renders.
type uiSource struct {
	runtime      *app.Runtime
	health       *health.State
	temperatures *temperatureStore
	streamHost   string

	// mu guards the per-camera maps, which a credential reload replaces while
	// the page is being rendered.
	mu          sync.RWMutex
	streamURLs  map[string]string
	streamNames map[string]string

	// go2rtcURL is the media server this add-on owns, restarted through its own
	// loopback API. allowMediaRestart and allowCredentialRefresh say whether
	// each repair exists in this deployment at all.
	go2rtcURL              string
	allowMediaRestart      bool
	allowCredentialRefresh bool
	// refreshSignal asks the supervising entrypoint for a credential refresh.
	// It is injectable so the test does not signal a real process.
	refreshSignal func() error
}

func newUISource(cfg appconfig.Config, registry app.Registry, runtime *app.Runtime, temperatures *temperatureStore, healthState *health.State) *uiSource {
	source := &uiSource{
		runtime: runtime, health: healthState, temperatures: temperatures,
		streamURLs: map[string]string{}, streamNames: map[string]string{},
		streamHost:             cfg.StreamHost(),
		go2rtcURL:              cfg.Go2RTCURL,
		allowMediaRestart:      cfg.AllowMediaRestart,
		allowCredentialRefresh: cfg.AllowCredentialRefresh,
		refreshSignal:          signalParentRefresh,
	}
	source.setRegistry(cfg, registry)
	return source
}

// setRegistry rebuilds the per-camera stream names and URLs. A reload that
// added a camera has to reach the page too: without it the new camera was
// rendered with no stream URL and played nothing until the add-on restarted.
func (u *uiSource) setRegistry(cfg appconfig.Config, registry app.Registry) {
	names := make(map[string]string, len(registry.Cameras))
	urls := make(map[string]string, len(registry.Cameras))
	for index, camera := range registry.Cameras {
		key := camera.Credentials.DeviceUDID
		if key == "" {
			key = camera.StreamName
		}
		// The first camera keeps the historical vm65 alias in go2rtc, so that
		// is the name the player has to ask for.
		names[key] = publishedStreamName(registry, camera, index)
		if url, err := discoveryStreamURL(cfg.StreamURL, camera.StreamName, index == 0); err == nil {
			urls[key] = url
		}
	}
	u.mu.Lock()
	u.streamNames = names
	u.streamURLs = urls
	u.mu.Unlock()
}

// stream reports the go2rtc stream name and RTSP URL of one camera.
func (u *uiSource) stream(id string) (name, url string) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.streamNames[id], u.streamURLs[id]
}

func (u *uiSource) Overview() webui.Overview {
	snapshot := u.health.Snapshot()
	states := u.runtime.Cameras()
	cameras := make([]webui.Camera, 0, len(states))
	for _, state := range states {
		name := state.Name
		if name == "" {
			name = "Motorola Nursery Camera"
		}
		stream, streamURL := u.stream(state.ID)
		if stream == "" {
			stream = state.StreamName
		}
		camera := webui.Camera{
			ID: state.ID, Name: name, Model: state.Model,
			Stream: stream, MJPEGStream: stream + mjpegSuffix,
			StreamURL: streamURL,
			Serving:   state.Serving, ActiveSessions: state.ActiveSessions,
		}
		if celsius, ok := u.temperatures.reading(state.ID); ok {
			value := celsius
			camera.TemperatureCelsius = &value
		}
		cameras = append(cameras, camera)
	}
	return webui.Overview{
		Cameras:               cameras,
		Version:               buildinfo.String(),
		Ready:                 snapshot.Ready,
		Go2RTCReady:           !snapshot.Go2RTCRequired || snapshot.Go2RTCReady,
		MQTTEnabled:           snapshot.MQTTEnabled,
		MQTTConnected:         snapshot.MQTTConnected,
		Reconnects:            snapshot.ReconnectsTotal,
		UptimeSeconds:         snapshot.UptimeSeconds,
		StreamHost:            u.streamHost,
		CanRestartMedia:       u.allowMediaRestart,
		CanRefreshCredentials: u.allowCredentialRefresh,
	}
}

func (u *uiSource) Restart(id string) error { return u.runtime.RestartCamera(id) }

// RestartMedia asks go2rtc to restart itself over its loopback API. That API is
// not reachable from anywhere else — the ingress proxy blocks this very path,
// because go2rtc has no authentication of its own — so the request is built
// here, after the Supervisor has authenticated a Home Assistant user.
func (u *uiSource) RestartMedia() error {
	if !u.allowMediaRestart {
		return webui.ErrUnsupported
	}
	endpoint, err := url.Parse(u.go2rtcURL)
	if err != nil {
		return fmt.Errorf("parse media server URL: %w", err)
	}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + "/api/restart"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return fmt.Errorf("restart the media server: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("the media server answered %d", response.StatusCode)
	}
	return nil
}

// RefreshCredentials signals the process supervising this one. The add-on's
// entrypoint owns the account session and the go2rtc configuration, so it is
// the only thing that can actually refresh anything; the bridge just asks.
func (u *uiSource) RefreshCredentials() error {
	if !u.allowCredentialRefresh || u.refreshSignal == nil {
		return webui.ErrUnsupported
	}
	return u.refreshSignal()
}

// signalParentRefresh raises SIGUSR1 on the parent process, which the add-on
// entrypoint traps. A parent of 1 means there is no entrypoint to ask — the
// bridge was started directly — so nothing is signalled.
func signalParentRefresh() error {
	parent := os.Getppid()
	if parent <= 1 {
		return webui.ErrUnsupported
	}
	if err := syscall.Kill(parent, syscall.SIGUSR1); err != nil {
		return fmt.Errorf("signal the supervising process: %w", err)
	}
	return nil
}

// publishCameraFrames feeds the Home Assistant camera entity. Each frame costs
// a still from go2rtc, which pulls it over the relay, so the interval is the
// user's to choose and zero turns the whole thing off.
func publishCameraFrames(ctx context.Context, service *mqttdiscovery.Service, snapshots *snapshot.Cache, registry app.Registry, interval time.Duration, logger *slog.Logger) {
	streams := make(map[string]string, len(registry.Cameras))
	for index, camera := range registry.Cameras {
		streams[camera.Credentials.DeviceUDID] = publishedStreamName(registry, camera, index)
	}

	publish := func() {
		for id, stream := range streams {
			image, err := snapshots.Frame(ctx, stream)
			if err != nil {
				logger.Debug("no frame for the camera entity yet", "camera", stream, "err", err)
				continue
			}
			if err := service.PublishFrame(ctx, id, image); err != nil {
				logger.Warn("could not publish a camera frame", "camera", stream, "err", err)
			}
		}
	}
	publish()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}

// webStack is everything the Ingress listener serves that has to learn about a
// camera the account gained: the stills, the media proxy's stream allowlist and
// the page's own per-camera names. Each of them was fixed at startup, so a
// SIGHUP that added a camera left it with a 404 for its thumbnail, "unknown
// stream" for its video and no URL on the page until the add-on was restarted.
type webStack struct {
	snapshots *snapshot.Cache
	media     *ingress.Handler
	source    *uiSource
}

// reload hands a new registry to each of them.
func (w *webStack) reload(cfg appconfig.Config, registry app.Registry, logger *slog.Logger) {
	if w == nil {
		return
	}
	names := streamNames(registry)
	if w.media != nil {
		w.media.SetStreams(names)
	}
	if err := w.snapshots.SetStreams(names, publishedStreams(registry)); err != nil {
		logger.Error("could not apply the new camera list to the snapshot endpoint", "err", err)
	}
	if w.source != nil {
		w.source.setRegistry(cfg, registry)
	}
}

// cache is the snapshot cache, or nil when there is no Web UI at all.
func (w *webStack) cache() *snapshot.Cache {
	if w == nil {
		return nil
	}
	return w.snapshots
}

// startWebServer serves the authenticated Web UI and the snapshot endpoint on
// the Ingress port. It returns nil when no ingress address is configured.
func startWebServer(ctx context.Context, cfg appconfig.Config, registry app.Registry, runtime *app.Runtime, temperatures *temperatureStore, healthState *health.State, logger *slog.Logger) (*webStack, error) {
	if cfg.IngressAddr == "" {
		return nil, nil
	}
	names := streamNames(registry)
	mux := http.NewServeMux()

	// Still images are served whenever there is an ingress listener, because
	// the Web UI shows them on every camera card. SnapshotBase decides
	// something else entirely: whether an address for them is published over
	// MQTT. Tying the two together left the default configuration — no broker —
	// with a page whose thumbnails were a 404.
	token, err := snapshot.LoadOrCreateToken(cfg.SnapshotTokenFile)
	if err != nil {
		return nil, err
	}
	snapshots, err := snapshot.New(snapshot.Config{
		Upstream:     cfg.Go2RTCURL,
		Streams:      names,
		Warm:         publishedStreams(registry),
		Token:        token,
		TrustedCIDRs: cfg.IngressTrustedCIDRs,
		Logger:       logger.With("component", "snapshot"),
	})
	if err != nil {
		return nil, err
	}
	// Home Assistant fetches this one with the token and no headers; the page
	// behind the Web UI uses the trusted variant instead. Registering it here
	// keeps it ahead of the catch-all below.
	mux.Handle(snapshot.Path, snapshots.Handler())

	// go2rtc is still what plays the video, so its media endpoints stay
	// reachable — through the same proxy as before, which refuses anything but
	// a read of a stream this add-on configured.
	media, err := ingress.NewHandler(ingress.Config{
		Upstream:     cfg.Go2RTCURL,
		Streams:      names,
		TrustedCIDRs: cfg.IngressTrustedCIDRs,
		Logger:       logger.With("component", "ingress"),
	})
	if err != nil {
		snapshots.Close()
		return nil, err
	}

	source := newUISource(cfg, registry, runtime, temperatures, healthState)
	ui, err := webui.NewServer(webui.Config{
		Source:       source,
		TrustedCIDRs: cfg.IngressTrustedCIDRs,
		Media:        media,
		Snapshot:     snapshots.TrustedHandler(),
		Logger:       logger.With("component", "webui"),
	})
	if err != nil {
		snapshots.Close()
		return nil, err
	}
	mux.Handle("/", ui.Handler())

	if err := startHTTPServer(ctx, "web UI", cfg.IngressAddr, mux, logger); err != nil {
		snapshots.Close()
		return nil, err
	}
	// go2rtc has to start the relay tunnel, the camera stream and a transcode
	// before it can produce a still frame. Doing that now means the first
	// dashboard to ask for a thumbnail does not wait for it.
	snapshots.Warm()
	return &webStack{snapshots: snapshots, media: media, source: source}, nil
}

func monitorGo2RTC(ctx context.Context, client *http.Client, endpoint string, interval time.Duration, state *health.State) {
	check := func() {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			state.SetGo2RTC(true, false)
			return
		}
		response, err := client.Do(request)
		if err != nil {
			state.SetGo2RTC(true, false)
			return
		}
		_ = response.Body.Close()
		state.SetGo2RTC(true, response.StatusCode >= 200 && response.StatusCode < 400)
	}
	check()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			state.SetGo2RTC(true, false)
			return
		case <-ticker.C:
			check()
		}
	}
}

func discoveryStreamURL(base, streamName string, legacy bool) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse discovery stream URL: %w", err)
	}
	if !legacy {
		parsed.Path = "/" + streamName
	}
	return parsed.String(), nil
}

// startHTTPServer runs one of the bridge's HTTP listeners until ctx is
// cancelled. It binds before returning: a port that is already taken is a
// configuration error worth failing on, and reporting it as a log line while
// the process carried on left the Web UI and the snapshot endpoint silently
// unreachable behind a line that said they were listening.
func startHTTPServer(ctx context.Context, name, addr string, handler http.Handler, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for the %s on %s: %w", name, addr, err)
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		logger.Info(name+" listening", "addr", listener.Addr().String())
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(name+" failed", "err", err)
		}
	}()
	return nil
}

func loadCreds(path string) (bridge.Credentials, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return bridge.Credentials{}, fmt.Errorf("read credentials %q: %w", path, err)
	}
	var f credsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return bridge.Credentials{}, fmt.Errorf("parse credentials %q: %w", path, err)
	}
	return f.credentials()
}

func loadCredentialSet(cfg appconfig.Config) ([]bridge.Credentials, error) {
	if cfg.RegistryPath == "" {
		credentials, err := loadCreds(cfg.CredentialsPath)
		if err != nil {
			return nil, err
		}
		return []bridge.Credentials{credentials}, nil
	}
	raw, err := os.ReadFile(cfg.RegistryPath)
	if err != nil {
		return nil, fmt.Errorf("read camera registry %q: %w", cfg.RegistryPath, err)
	}
	var registry struct {
		Cameras []credsFile `json:"cameras"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		return nil, fmt.Errorf("parse camera registry %q: %w", cfg.RegistryPath, err)
	}
	if len(registry.Cameras) == 0 {
		return nil, errors.New("camera registry must contain at least one camera")
	}
	credentials := make([]bridge.Credentials, 0, len(registry.Cameras))
	for index, camera := range registry.Cameras {
		value, err := camera.credentials()
		if err != nil {
			return nil, fmt.Errorf("camera registry entry %d: %w", index, err)
		}
		credentials = append(credentials, value)
	}
	return credentials, nil
}

func (f credsFile) credentials() (bridge.Credentials, error) {
	if f.SID == "" || f.DeviceToken == "" || f.ControlHost == "" {
		return bridge.Credentials{}, errors.New("credentials file must set sid, device_token and control_host")
	}
	return bridge.Credentials{
		DeviceID:      f.DeviceID,
		DeviceUDID:    f.DeviceUDID,
		DeviceName:    f.DeviceName,
		Model:         f.Model,
		SID:           f.SID,
		DeviceToken:   f.DeviceToken,
		ControlHost:   f.ControlHost,
		ControlPort:   f.ControlPort,
		TargetPort:    f.TargetPort,
		DeviceAPIHost: f.DeviceAPIHost,
		DeviceAPIPort: f.DeviceAPIPort,
	}, nil
}
