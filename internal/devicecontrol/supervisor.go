package devicecontrol

import (
	"context"
	"sync"
	"time"
)

// Sink receives the small temperature state machine without coupling the
// control protocol to MQTT.
type Sink interface {
	SetTemperatureSupported(context.Context, string, bool) error
	SetTemperatureAvailable(context.Context, string, bool) error
	PublishTemperature(context.Context, string, float64) error
}

type SupervisorConfig struct {
	Client       Client
	Sink         Sink
	PollInterval time.Duration
	RetryDelay   time.Duration
}

type Supervisor struct {
	ctx    context.Context
	cancel context.CancelFunc
	config SupervisorConfig

	mu      sync.Mutex
	workers map[string]*worker
}

// worker is one camera's control loop. Workers are held by pointer so a loop
// can tell whether it is still the current one for its camera: a cancelled
// worker keeps running until its blocked read times out, and until then it must
// not report state over the successor that replaced it.
type worker struct {
	camera Camera
	cancel context.CancelFunc
}

func NewSupervisor(parent context.Context, config SupervisorConfig) *Supervisor {
	if config.PollInterval <= 0 {
		config.PollInterval = 30 * time.Second
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 10 * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	return &Supervisor{ctx: ctx, cancel: cancel, config: config, workers: make(map[string]*worker)}
}

func (s *Supervisor) Reconcile(cameras []Camera) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wanted := make(map[string]Camera, len(cameras))
	for _, camera := range cameras {
		if camera.ID == "" || camera.DeviceID == 0 || camera.Token == "" || camera.Host == "" || camera.Port < 1 {
			continue
		}
		wanted[camera.ID] = camera
	}
	for id, existing := range s.workers {
		camera, keep := wanted[id]
		if keep && camera == existing.camera {
			delete(wanted, id)
			continue
		}
		existing.cancel()
		delete(s.workers, id)
	}
	for id, camera := range wanted {
		workerCtx, cancel := context.WithCancel(s.ctx)
		current := &worker{camera: camera, cancel: cancel}
		s.workers[id] = current
		go s.run(workerCtx, current)
	}
}

func (s *Supervisor) Close() {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, worker := range s.workers {
		worker.cancel()
	}
	s.workers = make(map[string]*worker)
}

// worker returns one camera's registered worker, or nil. It exists for the
// tests that assert who owns a camera after a reload.
func (s *Supervisor) worker(id string) *worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[id]
}

// current reports whether self is still the registered worker for its camera.
// Cancelling a worker does not stop it immediately — a read blocked on the
// camera runs to its own deadline first — so every state report is checked
// against this before it can overwrite the state of a replacement.
func (s *Supervisor) current(self *worker) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[self.camera.ID] == self
}

// retire unregisters a worker that is stopping and reports whether it was still
// the current one, so only the last worker for a camera clears its state.
func (s *Supervisor) retire(self *worker) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workers[self.camera.ID] != self {
		return false
	}
	delete(s.workers, self.camera.ID)
	return true
}

func (s *Supervisor) run(ctx context.Context, self *worker) {
	camera := self.camera
	// Report each state only while this worker still owns the camera. A worker
	// replaced by Reconcile would otherwise mark the camera unavailable, or
	// publish a stale reading, after its successor had already reported.
	setAvailable := func(ctx context.Context, available bool) {
		if s.current(self) {
			_ = s.config.Sink.SetTemperatureAvailable(ctx, camera.ID, available)
		}
	}
	setSupported := func(supported bool) {
		if s.current(self) {
			_ = s.config.Sink.SetTemperatureSupported(ctx, camera.ID, supported)
		}
	}
	defer func() {
		// The camera is only marked unavailable by the worker that was still
		// serving it, and on a context that outlives the cancelled one so the
		// last state reaches the sink.
		if s.retire(self) {
			_ = s.config.Sink.SetTemperatureAvailable(context.Background(), camera.ID, false)
		}
	}()

	for {
		setAvailable(ctx, false)
		connection, err := s.config.Client.Connect(ctx, camera)
		if err == nil {
			supported, capabilityErr := connection.SupportsTemperature(ctx)
			if capabilityErr == nil && !supported {
				_ = connection.Close()
				setSupported(false)
				return
			}
			if capabilityErr == nil {
				setSupported(true)
				for {
					temperature, readErr := connection.Temperature(ctx)
					if readErr != nil {
						break
					}
					if s.current(self) {
						_ = s.config.Sink.PublishTemperature(ctx, camera.ID, temperature)
					}
					if !wait(ctx, s.config.PollInterval) {
						_ = connection.Close()
						return
					}
				}
			}
			_ = connection.Close()
		}
		if !wait(ctx, s.config.RetryDelay) {
			return
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
