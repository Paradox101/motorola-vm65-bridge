// Package mqttdiscovery publishes Home Assistant MQTT Discovery for the bridge.
//
// Home Assistant's MQTT integration validates every discovery payload against
// the schema of the component it names, so the component has to match what is
// actually being published. The MQTT camera platform reads image bytes from a
// `topic` and has no notion of a stream URL — an older payload here carried
// stream_source and therefore created no entity at all. Feeding that topic is
// what puts a camera in Home Assistant without anyone adding an integration by
// hand, so cameras are published as a `camera` entity fed by still frames and
// an `image` entity fed by a snapshot URL, plus `binary_sensor` and `sensor`
// entities for the state worth watching.
//
// Live video still has no MQTT discovery path in Home Assistant. The add-on's
// own Web UI plays it, and the documentation covers adding it to a dashboard
// through the camera integration for anyone who wants it there too.
package mqttdiscovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// DefaultBaseTopic is the root of every state topic this service owns. State
// lives outside the discovery prefix so it can never be mistaken for a
// discovery message.
const DefaultBaseTopic = "motorola-nursery-bridge"

// bridgeObjectID identifies the bridge device itself across restarts.
const bridgeObjectID = "motorola_nursery_bridge"

const (
	payloadOnline  = "online"
	payloadOffline = "offline"
)

type Config struct {
	Host               string
	Port               int
	Username           string
	Password           string
	DiscoveryPrefix    string
	ClientID           string
	OnConnectionChange func(bool)

	// TLS dials the broker over TLS. Home Assistant's own broker service can
	// report a TLS listener, and a plain TCP client against one connects and
	// then waits forever for a handshake that never comes — which reaches the
	// user as entities that simply never appear.
	TLS bool

	// BaseTopic roots the state topics. Zero selects DefaultBaseTopic.
	BaseTopic string
	// BridgeName is the device name shown in Home Assistant.
	BridgeName string
	// Version is reported as the device's software version.
	Version string
	// ConfigurationURL points at the add-on Web UI, when known.
	ConfigurationURL string
	// PublishCameraFrames creates a camera entity per camera, fed by
	// PublishFrame. Home Assistant then has a real camera without anyone
	// adding an integration by hand — which is the only way a camera can be
	// discovered over MQTT, since the platform has no notion of a stream URL.
	PublishCameraFrames bool
}

type Camera struct {
	ID    string
	Name  string
	Model string
	// StreamURL is the RTSP URL for the camera. Home Assistant cannot discover
	// a stream over MQTT; it is reported as an attribute so the value is
	// visible where the entity is.
	StreamURL string
	// SnapshotURL is fetched by Home Assistant for the image entity. Empty
	// means no image entity is published for this camera.
	SnapshotURL string
}

// Status is the runtime state mirrored into diagnostic entities.
type Status struct {
	ActiveSessions int64
	Reconnects     uint64
}

type clientConfig struct {
	Config
	WillTopic   string
	WillPayload string
}

type brokerClient interface {
	Start(context.Context, func(), func(error)) error
	Publish(context.Context, string, bool, []byte) error
	Close(uint)
}

type clientFactory func(clientConfig) brokerClient

type Service struct {
	mu                   sync.RWMutex
	config               Config
	client               brokerClient
	connected            bool
	cameras              map[string]Camera
	available            map[string]bool
	temperatureSupported map[string]bool
	temperatureAvailable map[string]bool
	status               Status

	availability string
	base         string
}

func NewService(config Config) *Service {
	return newService(config, func(config clientConfig) brokerClient {
		return newPahoClient(config)
	})
}

func newService(config Config, factory clientFactory) *Service {
	config.DiscoveryPrefix = strings.Trim(config.DiscoveryPrefix, "/")
	if config.BaseTopic == "" {
		config.BaseTopic = DefaultBaseTopic
	}
	config.BaseTopic = strings.Trim(config.BaseTopic, "/")
	if config.BridgeName == "" {
		config.BridgeName = "Motorola Nursery Homeassistant Bridge"
	}
	availability := config.BaseTopic + "/availability"
	return &Service{
		config:               config,
		client:               factory(clientConfig{Config: config, WillTopic: availability, WillPayload: payloadOffline}),
		cameras:              make(map[string]Camera),
		available:            make(map[string]bool),
		temperatureSupported: make(map[string]bool),
		temperatureAvailable: make(map[string]bool),
		availability:         availability,
		base:                 config.BaseTopic,
	}
}

// SetTemperatureSupported creates or retires the temperature entity for one
// camera after its device-control capability list has been read.
func (s *Service) SetTemperatureSupported(ctx context.Context, id string, supported bool) error {
	s.mu.Lock()
	camera, known := s.cameras[id]
	if !known {
		s.mu.Unlock()
		return errors.New("mqtt temperature camera is not registered")
	}
	s.temperatureSupported[id] = supported
	if !supported {
		delete(s.temperatureAvailable, id)
	}
	available := s.temperatureAvailable[id]
	connected := s.connected
	s.mu.Unlock()
	if !connected {
		return nil
	}
	return s.publishTemperatureEntity(ctx, camera, supported, available)
}

// SetTemperatureAvailable updates only the control link availability. It is
// deliberately separate from media availability because either connection can
// fail without the other one failing.
func (s *Service) SetTemperatureAvailable(ctx context.Context, id string, available bool) error {
	s.mu.Lock()
	_, registered := s.cameras[id]
	supported := s.temperatureSupported[id]
	previous, known := s.temperatureAvailable[id]
	if registered {
		// Only registered cameras are recorded. A control worker that reports
		// after its camera left the registry would otherwise put the entry back
		// that Remove just deleted, and every removed camera would leave one
		// behind for the life of the process.
		s.temperatureAvailable[id] = available
	}
	connected := s.connected
	s.mu.Unlock()
	if !registered || !supported || !connected || (known && previous == available) {
		return nil
	}
	return s.client.Publish(ctx, s.cameraTopic(id, "temperature_availability"), true, []byte(availabilityPayload(available)))
}

// PublishTemperature publishes a numeric Celsius state and marks its control
// connection available after a successful request.
func (s *Service) PublishTemperature(ctx context.Context, id string, celsius float64) error {
	if math.IsNaN(celsius) || math.IsInf(celsius, 0) || celsius <= 0 || celsius >= 50 {
		return errors.New("mqtt temperature must be between 0 and 50 Celsius")
	}
	s.mu.Lock()
	_, registered := s.cameras[id]
	supported := s.temperatureSupported[id]
	connected := s.connected
	if registered {
		s.temperatureAvailable[id] = true
	}
	s.mu.Unlock()
	if !registered || !supported || !connected {
		return nil
	}
	return errors.Join(
		s.client.Publish(ctx, s.cameraTopic(id, "temperature"), true, []byte(strconv.FormatFloat(celsius, 'f', -1, 64))),
		s.client.Publish(ctx, s.cameraTopic(id, "temperature_availability"), true, []byte(payloadOnline)),
	)
}

func (s *Service) Start(ctx context.Context) error {
	if s.config.Host == "" || s.config.Port < 1 || s.config.Port > 65535 || s.config.DiscoveryPrefix == "" {
		return errors.New("mqtt discovery requires host, valid port and discovery prefix")
	}
	return s.client.Start(ctx, s.onConnect, func(error) {
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
		if s.config.OnConnectionChange != nil {
			s.config.OnConnectionChange(false)
		}
	})
}

// Upsert registers a camera and publishes its discovery and state topics.
func (s *Service) Upsert(ctx context.Context, camera Camera) error {
	if camera.ID == "" || camera.Name == "" {
		return errors.New("mqtt camera requires an id and a name")
	}
	if camera.StreamURL != "" {
		parsed, err := url.Parse(camera.StreamURL)
		if err != nil || parsed.Scheme != "rtsp" || parsed.Host == "" {
			return errors.New("mqtt camera stream URL must be absolute RTSP")
		}
	}
	if camera.SnapshotURL != "" {
		parsed, err := url.Parse(camera.SnapshotURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return errors.New("mqtt camera snapshot URL must be absolute http or https")
		}
	}
	s.mu.Lock()
	s.cameras[camera.ID] = camera
	if _, known := s.available[camera.ID]; !known {
		s.available[camera.ID] = true
	}
	connected := s.connected
	s.mu.Unlock()
	if !connected {
		return nil
	}
	return s.publishCamera(ctx, camera)
}

// SetCameraAvailable marks one camera's entities available or unavailable, so a
// single failed bridge is visible instead of every entity looking healthy.
func (s *Service) SetCameraAvailable(ctx context.Context, id string, available bool) error {
	s.mu.Lock()
	previous, known := s.available[id]
	s.available[id] = available
	connected := s.connected
	s.mu.Unlock()
	if !connected || (known && previous == available) {
		return nil
	}
	return s.client.Publish(ctx, s.cameraTopic(id, "availability"), true, []byte(availabilityPayload(available)))
}

// PublishFrame feeds one camera entity a still frame. Frames are retained so a
// Home Assistant restart shows the last picture immediately instead of an empty
// tile until the next refresh.
func (s *Service) PublishFrame(ctx context.Context, id string, jpeg []byte) error {
	if len(jpeg) < 2 || jpeg[0] != 0xFF || jpeg[1] != 0xD8 {
		return errors.New("mqtt camera frame must be a JPEG")
	}
	s.mu.RLock()
	_, known := s.cameras[id]
	connected := s.connected
	s.mu.RUnlock()
	if !known {
		return errors.New("mqtt camera is not registered")
	}
	if !connected || !s.config.PublishCameraFrames {
		return nil
	}
	return s.client.Publish(ctx, s.cameraTopic(id, "image"), true, jpeg)
}

// PublishStatus mirrors the runtime counters into the diagnostic entities.
func (s *Service) PublishStatus(ctx context.Context, status Status) error {
	s.mu.Lock()
	s.status = status
	connected := s.connected
	s.mu.Unlock()
	if !connected {
		return nil
	}
	return s.publishStatus(ctx, status)
}

// Remove retires every entity of a camera that left the registry.
func (s *Service) Remove(ctx context.Context, id string) error {
	s.mu.Lock()
	delete(s.cameras, id)
	delete(s.available, id)
	delete(s.temperatureSupported, id)
	delete(s.temperatureAvailable, id)
	connected := s.connected
	s.mu.Unlock()
	if !connected {
		return nil
	}
	object := objectID(id)
	var errs []error
	for _, topic := range []string{
		s.discoveryTopic("image", object),
		s.discoveryTopic("camera", object),
		s.discoveryTopic("binary_sensor", object+"_link"),
		s.discoveryTopic("sensor", object+"_temperature"),
		s.cameraTopic(id, "image"),
		s.cameraTopic(id, "temperature"),
		s.cameraTopic(id, "temperature_availability"),
	} {
		errs = append(errs, s.client.Publish(ctx, topic, true, nil))
	}
	return errors.Join(errs...)
}

func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	connected := s.connected
	s.connected = false
	s.mu.Unlock()
	var err error
	if connected {
		err = s.client.Publish(ctx, s.availability, true, []byte(payloadOffline))
	}
	s.client.Close(1000)
	return err
}

func (s *Service) onConnect() {
	s.mu.Lock()
	s.connected = true
	cameras := make([]Camera, 0, len(s.cameras))
	for _, camera := range s.cameras {
		cameras = append(cameras, camera)
	}
	status := s.status
	s.mu.Unlock()
	if s.config.OnConnectionChange != nil {
		s.config.OnConnectionChange(true)
	}
	sort.Slice(cameras, func(i, j int) bool { return cameras[i].ID < cameras[j].ID })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.client.Publish(ctx, s.availability, true, []byte(payloadOnline))
	_ = s.publishBridgeEntities(ctx)
	for _, camera := range cameras {
		_ = s.publishCamera(ctx, camera)
	}
	_ = s.publishStatus(ctx, status)
}

// publishBridgeEntities registers the diagnostics that describe the bridge
// itself: whether it is connected, how many stream sessions are live and how
// often a camera bridge had to be restarted.
func (s *Service) publishBridgeEntities(ctx context.Context) error {
	device := s.bridgeDevice()
	connection := entity{
		Name:        "Connection",
		UniqueID:    bridgeObjectID + "_connection",
		StateTopic:  s.availability,
		DeviceClass: "connectivity",
		PayloadOn:   payloadOnline,
		PayloadOff:  payloadOffline,
		// Deliberately no availability block: an entity that is unavailable
		// cannot also report that the bridge is disconnected.
		EntityCategory: "diagnostic",
		Device:         device,
	}
	sessions := entity{
		Name:           "Active sessions",
		UniqueID:       bridgeObjectID + "_active_sessions",
		StateTopic:     s.base + "/status/active_sessions",
		StateClass:     "measurement",
		UnitOfMeasure:  "sessions",
		Icon:           "mdi:video-account",
		EntityCategory: "diagnostic",
		Availability:   s.bridgeAvailability(),
		Device:         device,
	}
	reconnects := entity{
		Name:           "Bridge restarts",
		UniqueID:       bridgeObjectID + "_reconnects",
		StateTopic:     s.base + "/status/reconnects",
		StateClass:     "total_increasing",
		Icon:           "mdi:restart-alert",
		EntityCategory: "diagnostic",
		Availability:   s.bridgeAvailability(),
		Device:         device,
	}
	return errors.Join(
		s.publishEntity(ctx, "binary_sensor", bridgeObjectID+"_connection", connection),
		s.publishEntity(ctx, "sensor", bridgeObjectID+"_active_sessions", sessions),
		s.publishEntity(ctx, "sensor", bridgeObjectID+"_reconnects", reconnects),
	)
}

func (s *Service) publishCamera(ctx context.Context, camera Camera) error {
	object := objectID(camera.ID)
	device := s.cameraDevice(camera)

	s.mu.RLock()
	available := s.available[camera.ID]
	temperatureSupported := s.temperatureSupported[camera.ID]
	temperatureAvailable := s.temperatureAvailable[camera.ID]
	s.mu.RUnlock()

	var errs []error

	// Every camera gets a link sensor, in bundled and external mode alike: it
	// answers "is this camera reachable right now".
	link := entity{
		Name:        "Link",
		UniqueID:    camera.ID + "_link",
		StateTopic:  s.cameraTopic(camera.ID, "availability"),
		DeviceClass: "connectivity",
		PayloadOn:   payloadOnline,
		PayloadOff:  payloadOffline,
		Availability: []availability{{
			Topic:              s.availability,
			PayloadAvailable:   payloadOnline,
			PayloadUnavailable: payloadOffline,
		}},
		EntityCategory: "diagnostic",
		Device:         device,
	}
	errs = append(errs,
		s.publishEntity(ctx, "binary_sensor", object+"_link", link),
		s.client.Publish(ctx, s.cameraTopic(camera.ID, "availability"), true, []byte(availabilityPayload(available))),
	)

	if camera.SnapshotURL != "" {
		image := entity{
			Name:         "Snapshot",
			UniqueID:     camera.ID + "_snapshot",
			URLTopic:     s.cameraTopic(camera.ID, "snapshot_url"),
			Availability: s.cameraAvailability(camera.ID),
			// Both the bridge and this camera have to be up for the snapshot
			// URL to answer.
			AvailabilityMode: "all",
			Device:           device,
		}
		errs = append(errs,
			s.publishEntity(ctx, "image", object, image),
			s.client.Publish(ctx, s.cameraTopic(camera.ID, "snapshot_url"), true, []byte(camera.SnapshotURL)),
		)
	} else {
		// Retire an image entity left over from a previous bundled-mode run.
		errs = append(errs, s.client.Publish(ctx, s.discoveryTopic("image", object), true, nil))
	}

	// A `camera` entity is what puts the camera in Home Assistant without
	// anyone adding an integration by hand. The platform reads image bytes
	// from a topic — it has no stream_source key, which is why an older
	// payload carrying one created no entity at all — so it exists only while
	// frames are being published to it.
	if s.config.PublishCameraFrames {
		// Deliberately not named `camera`: shadowing the camera this function
		// is publishing, inside the block that publishes it, is one rename away
		// from a payload that describes the wrong device.
		cameraEntity := entity{
			Name:             "Camera",
			UniqueID:         camera.ID + "_camera",
			Topic:            s.cameraTopic(camera.ID, "image"),
			Availability:     s.cameraAvailability(camera.ID),
			AvailabilityMode: "all",
			Icon:             "mdi:cctv",
			Device:           device,
		}
		errs = append(errs, s.publishEntity(ctx, "camera", object, cameraEntity))
	} else {
		errs = append(errs,
			s.client.Publish(ctx, s.discoveryTopic("camera", object), true, nil),
			s.client.Publish(ctx, s.cameraTopic(camera.ID, "image"), true, nil),
		)
	}
	errs = append(errs, s.publishTemperatureEntity(ctx, camera, temperatureSupported, temperatureAvailable))

	return errors.Join(errs...)
}

func (s *Service) publishTemperatureEntity(ctx context.Context, camera Camera, supported, available bool) error {
	object := objectID(camera.ID)
	if !supported {
		return errors.Join(
			s.client.Publish(ctx, s.discoveryTopic("sensor", object+"_temperature"), true, nil),
			s.client.Publish(ctx, s.cameraTopic(camera.ID, "temperature"), true, nil),
			s.client.Publish(ctx, s.cameraTopic(camera.ID, "temperature_availability"), true, nil),
		)
	}
	sensor := entity{
		Name:             "Temperature",
		UniqueID:         camera.ID + "_temperature",
		StateTopic:       s.cameraTopic(camera.ID, "temperature"),
		DeviceClass:      "temperature",
		StateClass:       "measurement",
		UnitOfMeasure:    "°C",
		Availability:     append(s.bridgeAvailability(), availability{Topic: s.cameraTopic(camera.ID, "temperature_availability"), PayloadAvailable: payloadOnline, PayloadUnavailable: payloadOffline}),
		AvailabilityMode: "all",
		Device:           s.cameraDevice(camera),
	}
	return errors.Join(
		s.publishEntity(ctx, "sensor", object+"_temperature", sensor),
		s.client.Publish(ctx, s.cameraTopic(camera.ID, "temperature_availability"), true, []byte(availabilityPayload(available))),
	)
}

func (s *Service) publishStatus(ctx context.Context, status Status) error {
	return errors.Join(
		s.client.Publish(ctx, s.base+"/status/active_sessions", true,
			[]byte(strconv.FormatInt(status.ActiveSessions, 10))),
		s.client.Publish(ctx, s.base+"/status/reconnects", true,
			[]byte(strconv.FormatUint(status.Reconnects, 10))),
	)
}

func (s *Service) publishEntity(ctx context.Context, component, object string, payload entity) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, s.discoveryTopic(component, object), true, data)
}

func (s *Service) discoveryTopic(component, object string) string {
	return s.config.DiscoveryPrefix + "/" + component + "/" + object + "/config"
}

func (s *Service) cameraTopic(id, leaf string) string {
	return s.base + "/camera/" + objectID(id) + "/" + leaf
}

func (s *Service) bridgeAvailability() []availability {
	return []availability{{
		Topic:              s.availability,
		PayloadAvailable:   payloadOnline,
		PayloadUnavailable: payloadOffline,
	}}
}

func (s *Service) cameraAvailability(id string) []availability {
	return append(s.bridgeAvailability(), availability{
		Topic:              s.cameraTopic(id, "availability"),
		PayloadAvailable:   payloadOnline,
		PayloadUnavailable: payloadOffline,
	})
}

func (s *Service) bridgeDevice() device {
	return device{
		Identifiers:      []string{bridgeObjectID},
		Name:             s.config.BridgeName,
		Manufacturer:     "Motorola",
		Model:            "Nursery Bridge",
		SoftwareVersion:  s.config.Version,
		ConfigurationURL: s.config.ConfigurationURL,
	}
}

func (s *Service) cameraDevice(camera Camera) device {
	model := camera.Model
	if model == "" {
		model = "Nursery Camera"
	}
	return device{
		Identifiers:  []string{camera.ID},
		Name:         camera.Name,
		Manufacturer: "Motorola",
		Model:        model,
		ViaDevice:    bridgeObjectID,
	}
}

func availabilityPayload(available bool) string {
	if available {
		return payloadOnline
	}
	return payloadOffline
}

// entity is the union of the discovery keys used here. Every key is one Home
// Assistant accepts for the component it is published under; unknown keys are
// silently dropped by the MQTT integration, which is how a wrong payload can
// look like it works while creating no entity at all.
type entity struct {
	Name             string         `json:"name"`
	UniqueID         string         `json:"unique_id"`
	StateTopic       string         `json:"state_topic,omitempty"`
	URLTopic         string         `json:"url_topic,omitempty"`
	Topic            string         `json:"topic,omitempty"`
	DeviceClass      string         `json:"device_class,omitempty"`
	StateClass       string         `json:"state_class,omitempty"`
	UnitOfMeasure    string         `json:"unit_of_measurement,omitempty"`
	PayloadOn        string         `json:"payload_on,omitempty"`
	PayloadOff       string         `json:"payload_off,omitempty"`
	Icon             string         `json:"icon,omitempty"`
	EntityCategory   string         `json:"entity_category,omitempty"`
	Availability     []availability `json:"availability,omitempty"`
	AvailabilityMode string         `json:"availability_mode,omitempty"`
	Device           device         `json:"device"`
}

type availability struct {
	Topic              string `json:"topic"`
	PayloadAvailable   string `json:"payload_available"`
	PayloadUnavailable string `json:"payload_not_available"`
}

type device struct {
	Identifiers      []string `json:"identifiers"`
	Name             string   `json:"name"`
	Manufacturer     string   `json:"manufacturer"`
	Model            string   `json:"model"`
	SoftwareVersion  string   `json:"sw_version,omitempty"`
	ConfigurationURL string   `json:"configuration_url,omitempty"`
	ViaDevice        string   `json:"via_device,omitempty"`
}

func objectID(id string) string {
	var builder strings.Builder
	for _, character := range strings.ToLower(id) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || character == '-' || character == '_' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('_')
		}
	}
	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "camera"
	}
	return result
}

type pahoClient struct {
	config clientConfig
	client mqtt.Client
}

func newPahoClient(config clientConfig) *pahoClient { return &pahoClient{config: config} }

func (p *pahoClient) Start(ctx context.Context, onConnect func(), onLost func(error)) error {
	options := mqtt.NewClientOptions()
	options.AddBroker(brokerURL(p.config.TLS, p.config.Host, p.config.Port))
	clientID := p.config.ClientID
	if clientID == "" {
		clientID = "motorola-nursery-bridge"
	}
	options.SetClientID(clientID)
	options.SetUsername(p.config.Username)
	options.SetPassword(p.config.Password)
	options.SetAutoReconnect(true)
	options.SetConnectRetry(true)
	options.SetConnectRetryInterval(5 * time.Second)
	options.SetMaxReconnectInterval(time.Minute)
	options.SetKeepAlive(30 * time.Second)
	options.SetPingTimeout(10 * time.Second)
	options.SetOrderMatters(false)
	options.SetWill(p.config.WillTopic, p.config.WillPayload, 1, true)
	options.SetOnConnectHandler(func(mqtt.Client) { onConnect() })
	options.SetConnectionLostHandler(func(_ mqtt.Client, err error) { onLost(err) })
	p.client = mqtt.NewClient(options)
	return waitToken(ctx, p.client.Connect())
}

func (p *pahoClient) Publish(ctx context.Context, topic string, retained bool, payload []byte) error {
	if p.client == nil {
		return errors.New("mqtt client is not started")
	}
	return waitToken(ctx, p.client.Publish(topic, 1, retained, payload))
}

func (p *pahoClient) Close(quiesce uint) {
	if p.client != nil && p.client.IsConnected() {
		p.client.Disconnect(quiesce)
	}
}

// brokerURL is the address paho dials. The scheme is the whole difference
// between a working TLS broker and one that never finishes connecting.
func brokerURL(tls bool, host string, port int) string {
	scheme := "tcp://"
	if tls {
		scheme = "ssl://"
	}
	return scheme + host + ":" + strconv.Itoa(port)
}

func waitToken(ctx context.Context, token mqtt.Token) error {
	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return ctx.Err()
		}
	}
	if !token.WaitTimeout(timeout) {
		return fmt.Errorf("mqtt operation timed out: %w", context.DeadlineExceeded)
	}
	return token.Error()
}
