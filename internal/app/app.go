// Package app supervises all per-camera bridge listeners as one process.
//
// Supervision is real: a camera bridge that stops on its own is restarted with
// an exponential backoff, and the health state always reflects how many bridges
// are actually serving. Reload swaps in a new registry without disturbing the
// cameras whose credentials did not change, so a credential refresh does not
// interrupt a live stream.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/local/motorola-vm65-bridge/internal/bridge"
	"github.com/local/motorola-vm65-bridge/internal/health"
)

const (
	// DefaultRestartBackoff is the first wait after a camera bridge stops.
	DefaultRestartBackoff = time.Second
	// DefaultRestartBackoffMax caps the doubling.
	DefaultRestartBackoffMax = time.Minute
	// DefaultStatsInterval is how often aggregated counters reach the health
	// state. Aggregation is a handful of atomic loads.
	DefaultStatsInterval = 2 * time.Second
	// stableRun is how long a bridge must serve before its restart backoff is
	// considered recovered and reset to the base value.
	stableRun = time.Minute
)

type CameraServer interface {
	Listen() error
	Serve(context.Context) error
	Close() error
	Addr() net.Addr
	// Stats reports lifetime sessions accepted and currently active sessions.
	Stats() (total, active int64)
}

type ServerFactory func(bridge.Config) (CameraServer, error)

type RuntimeConfig struct {
	Registry  Registry
	Logger    *slog.Logger
	NewServer ServerFactory
	Health    *health.State

	// RestartBackoff and RestartBackoffMax bound the wait between restarts of a
	// camera bridge that stopped on its own. Zero selects the defaults.
	RestartBackoff    time.Duration
	RestartBackoffMax time.Duration

	// StatsInterval is how often session counters are pushed to Health. Zero
	// selects DefaultStatsInterval.
	StatsInterval time.Duration
}

type Runtime struct {
	cfg RuntimeConfig

	mu       sync.Mutex
	cameras  map[string]*cameraSupervisor
	stopping bool

	serving    atomic.Int32
	total      atomic.Int32
	restarts   atomic.Uint64
	baseCtx    context.Context
	baseCancel context.CancelFunc
}

// cameraSupervisor owns one camera's restart loop.
type cameraSupervisor struct {
	camera Camera
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	server CameraServer
}

func (s *cameraSupervisor) setServer(server CameraServer) {
	s.mu.Lock()
	s.server = server
	s.mu.Unlock()
}

func (s *cameraSupervisor) stats() (total, active int64) {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	if server == nil {
		return 0, 0
	}
	return server.Stats()
}

// serving reports whether this camera currently has a server accepting
// connections; between a failure and its restart it has none.
func (s *cameraSupervisor) serving() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.server != nil
}

func (s *cameraSupervisor) closeServer() {
	s.mu.Lock()
	server := s.server
	s.mu.Unlock()
	if server != nil {
		_ = server.Close()
	}
}

func New(cfg RuntimeConfig) *Runtime {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.NewServer == nil {
		cfg.NewServer = func(cfg bridge.Config) (CameraServer, error) {
			return bridge.New(cfg)
		}
	}
	if cfg.RestartBackoff <= 0 {
		cfg.RestartBackoff = DefaultRestartBackoff
	}
	if cfg.RestartBackoffMax < cfg.RestartBackoff {
		cfg.RestartBackoffMax = DefaultRestartBackoffMax
	}
	if cfg.RestartBackoffMax < cfg.RestartBackoff {
		cfg.RestartBackoffMax = cfg.RestartBackoff
	}
	if cfg.StatsInterval <= 0 {
		cfg.StatsInterval = DefaultStatsInterval
	}
	return &Runtime{cfg: cfg, cameras: make(map[string]*cameraSupervisor)}
}

// Run starts every camera in the configured registry and keeps them running
// until ctx is cancelled. Binding a listener for the first time is fatal — a
// port conflict is a configuration error worth failing fast on — but a bridge
// that stops later is restarted rather than silently lost.
func (r *Runtime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.mu.Lock()
	r.baseCtx = runCtx
	r.baseCancel = cancel
	cameras := r.cfg.Registry.Cameras
	r.mu.Unlock()

	for _, camera := range cameras {
		server, err := r.startServer(camera)
		if err != nil {
			r.stopAll()
			return err
		}
		r.add(runCtx, camera, server)
	}
	r.publishBridges()

	ticker := time.NewTicker(r.cfg.StatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			r.stopAll()
			r.publishBridges()
			r.publishCounters()
			return nil
		case <-ticker.C:
			r.publishCounters()
		}
	}
}

// Reload reconciles the running bridges with registry. Cameras whose
// credentials and listen address are unchanged keep serving, so a periodic
// credential refresh never interrupts a live stream. It is safe to call from a
// signal handler while Run is executing.
func (r *Runtime) Reload(registry Registry) error {
	if len(registry.Cameras) == 0 {
		return errors.New("camera registry is empty")
	}

	r.mu.Lock()
	if r.stopping || r.baseCtx == nil {
		r.mu.Unlock()
		return errors.New("runtime is not running")
	}
	ctx := r.baseCtx
	existing := make(map[string]*cameraSupervisor, len(r.cameras))
	for key, supervisor := range r.cameras {
		existing[key] = supervisor
	}
	r.mu.Unlock()

	wanted := make(map[string]Camera, len(registry.Cameras))
	for _, camera := range registry.Cameras {
		wanted[cameraKey(camera)] = camera
	}

	// Stop what disappeared or changed. Everything else keeps its sockets.
	var stopped []*cameraSupervisor
	for key, supervisor := range existing {
		next, keep := wanted[key]
		if keep && sameCamera(supervisor.camera, next) {
			delete(wanted, key)
			continue
		}
		reason := "camera removed from registry"
		if keep {
			reason = "camera credentials changed"
		}
		r.cfg.Logger.Info("stopping camera bridge", "camera", supervisor.camera.StreamName, "reason", reason)
		supervisor.cancel()
		supervisor.closeServer()
		stopped = append(stopped, supervisor)
	}
	for _, supervisor := range stopped {
		<-supervisor.done
		r.remove(cameraKey(supervisor.camera), supervisor)
	}

	// Start what is new or changed.
	var failures []error
	for _, camera := range wanted {
		server, err := r.startServer(camera)
		if err != nil {
			failures = append(failures, err)
			r.cfg.Logger.Error("camera bridge failed to start after reload", "camera", camera.StreamName, "err", err)
			continue
		}
		r.cfg.Logger.Info("starting camera bridge", "camera", camera.StreamName, "listen", camera.ListenAddr)
		r.add(ctx, camera, server)
	}
	// Under the lock: Run reads this field while a SIGHUP reload writes it, and
	// the two run in different goroutines.
	r.mu.Lock()
	r.cfg.Registry = registry
	r.mu.Unlock()
	r.publishBridges()
	return errors.Join(failures...)
}

// Reconnects reports how many times a camera bridge has been restarted.
func (r *Runtime) Reconnects() uint64 { return r.restarts.Load() }

// CameraState is one camera's live runtime state, for the Web UI.
type CameraState struct {
	ID             string
	Name           string
	Model          string
	StreamName     string
	Serving        bool
	ActiveSessions int64
}

// Cameras reports the live state of every camera the runtime supervises.
func (r *Runtime) Cameras() []CameraState {
	r.mu.Lock()
	supervisors := make([]*cameraSupervisor, 0, len(r.cameras))
	for _, supervisor := range r.cameras {
		supervisors = append(supervisors, supervisor)
	}
	r.mu.Unlock()

	states := make([]CameraState, 0, len(supervisors))
	for _, supervisor := range supervisors {
		_, active := supervisor.stats()
		states = append(states, CameraState{
			ID:             cameraKey(supervisor.camera),
			Name:           supervisor.camera.Credentials.DeviceName,
			Model:          supervisor.camera.Credentials.Model,
			StreamName:     supervisor.camera.StreamName,
			Serving:        supervisor.serving(),
			ActiveSessions: active,
		})
	}
	return states
}

// RestartCamera closes one camera's server so its supervisor rebuilds it. A
// relay tunnel that went bad recovers from exactly this, and it leaves every
// other camera streaming.
func (r *Runtime) RestartCamera(id string) error {
	r.mu.Lock()
	supervisor := r.cameras[id]
	r.mu.Unlock()
	if supervisor == nil {
		return errors.New("no such camera")
	}
	// The supervise loop treats a server that stopped on its own as a failure
	// and starts a new one after its backoff, which is the behaviour wanted
	// here; nothing else has to be told about it.
	supervisor.closeServer()
	return nil
}

// CameraAvailability reports, per camera identifier, whether that camera's
// bridge is currently serving. Integrations use it to mark one camera
// unavailable without touching the ones that still work.
func (r *Runtime) CameraAvailability() map[string]bool {
	r.mu.Lock()
	supervisors := make([]*cameraSupervisor, 0, len(r.cameras))
	for _, supervisor := range r.cameras {
		supervisors = append(supervisors, supervisor)
	}
	r.mu.Unlock()

	result := make(map[string]bool, len(supervisors))
	for _, supervisor := range supervisors {
		id := supervisor.camera.Credentials.DeviceUDID
		if id == "" {
			id = supervisor.camera.StreamName
		}
		result[id] = supervisor.serving()
	}
	return result
}

func (r *Runtime) startServer(camera Camera) (CameraServer, error) {
	server, err := r.cfg.NewServer(bridge.Config{
		ListenAddr:  camera.ListenAddr,
		Credentials: camera.Credentials,
		Logger:      r.cfg.Logger.With("camera", camera.StreamName),
	})
	if err != nil {
		return nil, err
	}
	if err := server.Listen(); err != nil {
		_ = server.Close()
		return nil, err
	}
	return server, nil
}

func (r *Runtime) add(ctx context.Context, camera Camera, server CameraServer) {
	cameraCtx, cancel := context.WithCancel(ctx)
	supervisor := &cameraSupervisor{camera: camera, cancel: cancel, done: make(chan struct{})}
	supervisor.setServer(server)

	r.mu.Lock()
	r.cameras[cameraKey(camera)] = supervisor
	r.total.Store(int32(len(r.cameras)))
	r.mu.Unlock()

	go r.supervise(cameraCtx, supervisor, server)
}

func (r *Runtime) remove(key string, supervisor *cameraSupervisor) {
	r.mu.Lock()
	if current, ok := r.cameras[key]; ok && current == supervisor {
		delete(r.cameras, key)
	}
	r.total.Store(int32(len(r.cameras)))
	r.mu.Unlock()
}

// supervise runs one camera's bridge, restarting it with an exponential backoff
// whenever Serve returns while the context is still live.
func (r *Runtime) supervise(ctx context.Context, supervisor *cameraSupervisor, first CameraServer) {
	defer close(supervisor.done)

	log := r.cfg.Logger.With("camera", supervisor.camera.StreamName)
	server := first
	backoff := r.cfg.RestartBackoff

	for {
		if server == nil {
			var err error
			server, err = r.startServer(supervisor.camera)
			if err != nil {
				log.Error("camera bridge could not bind", "err", err, "retry_in", backoff)
				r.setLastError(health.ErrorNetwork)
				if !sleepContext(ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff, r.cfg.RestartBackoffMax)
				continue
			}
			supervisor.setServer(server)
		}

		r.serving.Add(1)
		r.publishBridges()
		started := time.Now()
		err := server.Serve(ctx)
		r.serving.Add(-1)
		r.publishBridges()

		_ = server.Close()
		supervisor.setServer(nil)
		server = nil

		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("camera bridge stopped unexpectedly")
		}
		r.restarts.Add(1)
		r.setLastError(health.ErrorNetwork)

		// A bridge that served for a while and then failed is a fresh incident,
		// not an escalating one.
		if time.Since(started) >= stableRun {
			backoff = r.cfg.RestartBackoff
		}
		log.Error("camera bridge stopped; restarting", "err", err, "retry_in", backoff)
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, r.cfg.RestartBackoffMax)
	}
}

func (r *Runtime) stopAll() {
	r.mu.Lock()
	r.stopping = true
	supervisors := make([]*cameraSupervisor, 0, len(r.cameras))
	for _, supervisor := range r.cameras {
		supervisors = append(supervisors, supervisor)
	}
	r.mu.Unlock()

	for _, supervisor := range supervisors {
		supervisor.cancel()
		supervisor.closeServer()
	}
	for _, supervisor := range supervisors {
		<-supervisor.done
	}
}

func (r *Runtime) publishBridges() {
	if r.cfg.Health == nil {
		return
	}
	ready := int(r.serving.Load())
	if ready < 0 {
		ready = 0
	}
	r.cfg.Health.SetBridges(ready, int(r.total.Load()))
}

// publishCounters aggregates live session counts across every camera so the
// status endpoint reports what is actually happening instead of zeros.
func (r *Runtime) publishCounters() {
	if r.cfg.Health == nil {
		return
	}
	r.mu.Lock()
	supervisors := make([]*cameraSupervisor, 0, len(r.cameras))
	for _, supervisor := range r.cameras {
		supervisors = append(supervisors, supervisor)
	}
	r.mu.Unlock()

	var active int64
	for _, supervisor := range supervisors {
		_, cameraActive := supervisor.stats()
		active += cameraActive
	}
	r.cfg.Health.SetCounters(r.restarts.Load(), active)
}

func (r *Runtime) setLastError(category health.ErrorCategory) {
	if r.cfg.Health != nil {
		r.cfg.Health.SetLastError(category)
	}
}

// sleepContext waits for d and reports whether the wait completed rather than
// being cut short by cancellation.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

// cameraKey identifies a camera across reloads. The UDID is stable; the stream
// name is the fallback for credentials that predate it.
func cameraKey(camera Camera) string {
	if camera.Credentials.DeviceUDID != "" {
		return camera.Credentials.DeviceUDID
	}
	return camera.StreamName
}

// sameCamera reports whether a reload can leave this camera untouched.
func sameCamera(current, next Camera) bool {
	return current.Credentials == next.Credentials &&
		current.ListenAddr == next.ListenAddr &&
		current.StreamName == next.StreamName
}
