package devicecontrol

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"
)

// event is one thing a supervisor reported about one camera.
type event struct {
	kind    string // "available", "supported" or "temperature"
	id      string
	flag    bool
	celsius float64
}

// recordingSink captures the state a supervisor reports, in order.
type recordingSink struct {
	mu        sync.Mutex
	events    []event
	available map[string][]bool
	supported map[string][]bool
	readings  map[string][]float64
	published chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{
		available: map[string][]bool{},
		supported: map[string][]bool{},
		readings:  map[string][]float64{},
		published: make(chan struct{}, 64),
	}
}

func (s *recordingSink) SetTemperatureSupported(_ context.Context, id string, supported bool) error {
	s.mu.Lock()
	s.supported[id] = append(s.supported[id], supported)
	s.events = append(s.events, event{kind: "supported", id: id, flag: supported})
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *recordingSink) SetTemperatureAvailable(_ context.Context, id string, available bool) error {
	s.mu.Lock()
	s.available[id] = append(s.available[id], available)
	s.events = append(s.events, event{kind: "available", id: id, flag: available})
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *recordingSink) PublishTemperature(_ context.Context, id string, celsius float64) error {
	s.mu.Lock()
	s.readings[id] = append(s.readings[id], celsius)
	s.events = append(s.events, event{kind: "temperature", id: id, celsius: celsius})
	s.mu.Unlock()
	s.signal()
	return nil
}

// after returns everything reported once predicate first held.
func (s *recordingSink) after(predicate func(event) bool) []event {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, item := range s.events {
		if predicate(item) {
			return append([]event(nil), s.events[index+1:]...)
		}
	}
	return nil
}

func (s *recordingSink) signal() {
	select {
	case s.published <- struct{}{}:
	default:
	}
}

func (s *recordingSink) lastAvailable(id string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := s.available[id]
	if len(values) == 0 {
		return false, false
	}
	return values[len(values)-1], true
}

func (s *recordingSink) temperatures(id string) []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]float64(nil), s.readings[id]...)
}

func (s *recordingSink) supportedValues(id string) []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.supported[id]...)
}

// fakeCamera answers the control protocol on one in-process connection.
func fakeCamera(t *testing.T, capability, reading string) (net.Conn, func()) {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverSide.Close()
		reader := bufio.NewReader(serverSide)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			var reply string
			switch {
			case len(line) > 3 && line[:3] == "app":
				reply = "app 1 0\n"
			case line == "caplist\n":
				reply = capability
			default:
				reply = reading
			}
			if _, err := serverSide.Write([]byte(reply)); err != nil {
				return
			}
		}
	}()
	return clientSide, func() { _ = clientSide.Close(); <-done }
}

func waitFor(t *testing.T, sink *recordingSink, condition func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if condition() {
			return
		}
		select {
		case <-sink.published:
		case <-deadline:
			t.Fatal("timed out waiting for the supervisor to report state")
		}
	}
}

func TestSupervisorPublishesTemperatureForASupportedCamera(t *testing.T) {
	conn, stop := fakeCamera(t, "caplist 1 temperature_reading r int 0 0\n", "get 1 temperature_reading 214\n")
	defer stop()

	sink := newRecordingSink()
	supervisor := NewSupervisor(context.Background(), SupervisorConfig{
		Client:       Client{DialContext: func(context.Context, string, *tls.Config) (net.Conn, error) { return conn, nil }},
		Sink:         sink,
		PollInterval: time.Hour,
		RetryDelay:   time.Hour,
	})
	defer supervisor.Close()

	supervisor.Reconcile([]Camera{{ID: "camera-a", DeviceID: 42, Token: "device-token", Host: "camera.example", Port: 2288}})
	waitFor(t, sink, func() bool { return len(sink.temperatures("camera-a")) > 0 })

	if got := sink.temperatures("camera-a"); got[0] != 21.4 {
		t.Fatalf("temperature = %v, want 21.4", got[0])
	}
	if got := sink.supportedValues("camera-a"); len(got) == 0 || !got[len(got)-1] {
		t.Fatalf("supported = %v, want a final true", got)
	}
}

// A camera without the capability is not polled again: its worker reports the
// capability once and stops, instead of holding a control link open forever.
func TestSupervisorRetiresACameraWithoutTemperature(t *testing.T) {
	conn, stop := fakeCamera(t, "caplist 0\n", "get 1 temperature_reading 214\n")
	defer stop()

	sink := newRecordingSink()
	supervisor := NewSupervisor(context.Background(), SupervisorConfig{
		Client:       Client{DialContext: func(context.Context, string, *tls.Config) (net.Conn, error) { return conn, nil }},
		Sink:         sink,
		PollInterval: time.Hour,
		RetryDelay:   time.Hour,
	})
	defer supervisor.Close()

	supervisor.Reconcile([]Camera{{ID: "camera-a", DeviceID: 42, Token: "device-token", Host: "camera.example", Port: 2288}})
	waitFor(t, sink, func() bool {
		values := sink.supportedValues("camera-a")
		return len(values) > 0 && !values[len(values)-1]
	})
	if got := sink.temperatures("camera-a"); len(got) != 0 {
		t.Fatalf("temperatures = %v, want none", got)
	}
}

// Reconcile skips a camera whose credentials are incomplete rather than opening
// a control link that can only be refused.
func TestSupervisorIgnoresIncompleteCameras(t *testing.T) {
	sink := newRecordingSink()
	supervisor := NewSupervisor(context.Background(), SupervisorConfig{
		Client: Client{DialContext: func(context.Context, string, *tls.Config) (net.Conn, error) {
			t.Error("an incomplete camera must not be dialled")
			return nil, net.ErrClosed
		}},
		Sink:         sink,
		PollInterval: time.Hour,
		RetryDelay:   time.Hour,
	})
	defer supervisor.Close()

	supervisor.Reconcile([]Camera{
		{ID: "", DeviceID: 42, Token: "token", Host: "camera.example", Port: 2288},
		{ID: "camera-b", DeviceID: 0, Token: "token", Host: "camera.example", Port: 2288},
		{ID: "camera-c", DeviceID: 42, Token: "", Host: "camera.example", Port: 2288},
		{ID: "camera-d", DeviceID: 42, Token: "token", Host: "", Port: 2288},
		{ID: "camera-e", DeviceID: 42, Token: "token", Host: "camera.example", Port: 0},
	})
	time.Sleep(50 * time.Millisecond)
	if _, reported := sink.lastAvailable("camera-b"); reported {
		t.Fatal("an incomplete camera must report no state")
	}
}

// Cancelling a worker does not stop it: a read blocked on the camera runs to
// its own ten-second deadline first. Until then the replaced worker must count
// as no longer current, or its final "unavailable" lands on the successor that
// is already polling the camera — and neither may it unregister the successor.
func TestAReplacedWorkerIsNoLongerCurrent(t *testing.T) {
	sink := newRecordingSink()
	supervisor := NewSupervisor(context.Background(), SupervisorConfig{
		Client: Client{DialContext: func(ctx context.Context, _ string, _ *tls.Config) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}},
		Sink:         sink,
		PollInterval: time.Hour,
		RetryDelay:   time.Hour,
	})
	defer supervisor.Close()

	camera := Camera{ID: "camera-a", DeviceID: 42, Token: "device-token", Host: "camera.example", Port: 2288}
	supervisor.Reconcile([]Camera{camera})
	first := supervisor.worker("camera-a")
	if first == nil || !supervisor.current(first) {
		t.Fatal("the first worker is not registered")
	}

	refreshed := camera
	refreshed.Token = "rotated-token"
	supervisor.Reconcile([]Camera{refreshed})

	second := supervisor.worker("camera-a")
	if second == nil || second == first {
		t.Fatal("a camera whose credentials changed keeps its old worker")
	}
	if supervisor.current(first) {
		t.Fatal("the replaced worker still counts as current, so its state reports overwrite its successor")
	}
	if supervisor.retire(first) {
		t.Fatal("the replaced worker unregistered its successor")
	}
	if !supervisor.current(second) {
		t.Fatal("retiring the replaced worker removed the successor's registration")
	}
	if !supervisor.retire(second) {
		t.Fatal("the current worker cannot retire itself")
	}
}

// A reload that replaces a worker leaves the successor's state standing.
func TestReplacedWorkerDoesNotOverwriteItsSuccessor(t *testing.T) {
	first, stopFirst := fakeCamera(t, "caplist 1 temperature_reading r int 0 0\n", "get 1 temperature_reading 214\n")
	defer stopFirst()
	second, stopSecond := fakeCamera(t, "caplist 1 temperature_reading r int 0 0\n", "get 1 temperature_reading 220\n")
	defer stopSecond()

	connections := make(chan net.Conn, 2)
	connections <- first
	connections <- second

	sink := newRecordingSink()
	supervisor := NewSupervisor(context.Background(), SupervisorConfig{
		Client: Client{DialContext: func(context.Context, string, *tls.Config) (net.Conn, error) {
			return <-connections, nil
		}},
		Sink:         sink,
		PollInterval: time.Hour,
		RetryDelay:   time.Hour,
	})
	defer supervisor.Close()

	camera := Camera{ID: "camera-a", DeviceID: 42, Token: "device-token", Host: "camera.example", Port: 2288}
	supervisor.Reconcile([]Camera{camera})
	waitFor(t, sink, func() bool { return len(sink.temperatures("camera-a")) > 0 })

	// A credential refresh replaces the worker while the first one is parked in
	// its poll wait.
	refreshed := camera
	refreshed.Token = "rotated-token"
	supervisor.Reconcile([]Camera{refreshed})
	waitFor(t, sink, func() bool {
		temperatures := sink.temperatures("camera-a")
		return len(temperatures) > 1 && temperatures[len(temperatures)-1] == 22
	})

	// Let the replaced worker finish. Everything it still had to say — an
	// availability of false above all — must stay unsaid: after the successor's
	// reading, nothing more may reach the sink.
	time.Sleep(100 * time.Millisecond)
	trailing := sink.after(func(item event) bool {
		return item.kind == "temperature" && item.celsius == 22
	})
	if len(trailing) != 0 {
		t.Fatalf("the replaced worker reported %+v after its successor took over", trailing)
	}
	if got := sink.temperatures("camera-a"); got[len(got)-1] != 22 {
		t.Fatalf("last temperature = %v, want the successor's 22", got[len(got)-1])
	}
}
