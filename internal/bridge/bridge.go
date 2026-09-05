// Package bridge exposes the reconstructed Magic WEB2 tunnel as a local TCP
// endpoint. It plays the same role the Android app plays with its dynamic
// listen port (16667 in the measured session): a plain, local RTSP-over-TCP
// socket that any player, go2rtc or Home Assistant can point at, with every
// byte carried transparently to the camera through a magic.Tunnel.
//
// The bridge deliberately performs no 5GenCare control flow. That flow (fresh
// SID, device token, stream access token and relay parameters) is the one part
// of the chain not reconstructable from an x86 host, so its outputs are handed
// to the bridge as Credentials. Everything downstream of those credentials is
// the proven, tested Magic transport.
package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/local/motorola-vm65-bridge/internal/magic"
)

// DefaultTargetPort is the camera target port observed in every measured VM65
// live-view session.
const DefaultTargetPort = 6667

const (
	// DefaultIdleTimeout is how long a session may go without a single byte
	// from the camera before the bridge drops it. A live view sends video
	// continuously, so silence this long means the far end is gone — which the
	// sockets themselves never report when the relay disappears without
	// closing.
	DefaultIdleTimeout = 60 * time.Second

	// DefaultKeepAlivePeriod is the TCP keepalive probe interval on the client
	// and relay sockets. It gives the kernel a way to notice a peer that
	// vanished, independent of whether any data was expected.
	DefaultKeepAlivePeriod = 30 * time.Second

	// DefaultMaxSessions caps concurrent client connections per camera. Steady
	// state needs a couple (the live stream plus a snapshot fetch), but a media
	// server whose producer just died reconnects every consumer at once, and a
	// recovering camera was measured opening well over eight connections inside
	// a tenth of a second. This is the last-resort guard against a client that
	// reconnects faster than its sessions end — not a throttle on normal
	// bursts, which the dial budget and the abandoned-dial path already bound.
	DefaultMaxSessions = 16

	// DefaultDialBudget bounds the whole relay-open sequence for one session,
	// retries and backoff included. The retry loop alone can outlast any client
	// waiting on it — a media server gives up in seconds — and a session still
	// dialling holds its slot, so an unreachable relay would otherwise fill the
	// concurrency cap with attempts nobody is waiting for any more.
	DefaultDialBudget = 25 * time.Second
)

// ErrIdleTimeout reports a session dropped because the camera stopped sending.
var ErrIdleTimeout = errors.New("bridge: no data from the camera within the idle timeout")

// Credentials are the per-camera values the 5GenCare control flow produces.
// The bridge treats them as opaque inputs: it derives the magicUuid from them
// but never fabricates, refreshes or persists them.
type Credentials struct {
	DeviceID      uint32 // numeric device id
	SID           string // camera SID from device discovery
	DeviceToken   string // opaque device token; also the tunnel crypto key
	DeviceUDID    string // stable camera identifier for integrations
	DeviceName    string // display name for integrations
	Model         string // device-reported model; informational, never filtered
	ControlHost   string // Magic relay control host
	ControlPort   int    // defaults to magic.ControlPortDefault when zero
	TargetPort    int    // defaults to DefaultTargetPort when zero
	DeviceAPIHost string // 5GenCare device-control TLS hostname
	DeviceAPIPort int    // defaults to fivegencare.DeviceAPIPort when zero
}

func (c Credentials) validate() error {
	switch {
	case c.SID == "":
		return errors.New("credentials: SID is required")
	case c.DeviceToken == "":
		return errors.New("credentials: device token is required")
	case c.ControlHost == "":
		return errors.New("credentials: control host is required")
	}
	return nil
}

// Config configures a Bridge. ListenAddr and Credentials are required; the rest
// have safe defaults.
type Config struct {
	// ListenAddr is the local address the bridge listens on, e.g.
	// "127.0.0.1:8554". Binding to loopback is strongly recommended: the tunnel
	// carries an unauthenticated RTSP stream.
	ListenAddr string

	Credentials Credentials

	// DialTimeout bounds the Magic WEB2 opening handshake for one client. Zero
	// selects a 15s default. It does not limit stream lifetime.
	DialTimeout time.Duration

	// DialRetries is the number of extra attempts to open the relay after the
	// first fails, per client connection. Zero selects a default of 2; a
	// negative value disables retrying.
	DialRetries int

	// DialBackoff is the base wait between relay-open attempts; it doubles each
	// attempt. Zero selects 1s.
	DialBackoff time.Duration

	// DialBudget bounds the entire relay-open sequence for one session, across
	// all attempts and backoffs. Zero selects DefaultDialBudget; a negative
	// value removes the bound and lets the retry sequence run to its end.
	DialBudget time.Duration

	// IdleTimeout drops a session that has gone this long without a byte from
	// the camera. Zero selects DefaultIdleTimeout; a negative value disables
	// the check and restores the previous behaviour of waiting forever.
	IdleTimeout time.Duration

	// KeepAlivePeriod is the TCP keepalive probe interval set on the client
	// socket and on the relay stream socket. Zero selects
	// DefaultKeepAlivePeriod; a negative value leaves the system default.
	KeepAlivePeriod time.Duration

	// MaxSessions caps how many client connections may be open at once. Zero
	// selects DefaultMaxSessions; a negative value removes the cap.
	MaxSessions int

	// Logger receives structured lifecycle logs. Zero uses slog.Default.
	Logger *slog.Logger

	// Dial injects the raw TCP dialer used for the relay connections. Zero uses
	// a net.Dialer. Tests use it to point at an in-process fake relay.
	Dial magic.DialFunc
}

// Bridge accepts local TCP connections and tunnels each one to the camera
// through an independent Magic WEB2 relay session.
type Bridge struct {
	cfg         Config
	magicUUID   string
	dialTimeout time.Duration
	dialRetries int
	dialBackoff time.Duration
	dialBudget  time.Duration
	idleTimeout time.Duration
	keepAlive   time.Duration
	maxSessions int
	log         *slog.Logger

	listener net.Listener
	sessions int64 // total accepted, atomic
	active   int64 // currently open, atomic

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

// New validates cfg and derives the stable magicUuid, but does not bind a
// socket. Call Listen (or Serve) to start accepting.
func New(cfg Config) (*Bridge, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("bridge: listen address is required")
	}
	if err := cfg.Credentials.validate(); err != nil {
		return nil, err
	}
	magicUUID, err := magic.GenerateMagicUUID(cfg.Credentials.DeviceID, cfg.Credentials.SID, cfg.Credentials.DeviceToken)
	if err != nil {
		return nil, fmt.Errorf("bridge: derive magic uuid: %w", err)
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 15 * time.Second
	}
	dialRetries := cfg.DialRetries
	if dialRetries == 0 {
		dialRetries = 2
	}
	if dialRetries < 0 {
		dialRetries = 0
	}
	dialBackoff := cfg.DialBackoff
	if dialBackoff == 0 {
		dialBackoff = time.Second
	}
	dialBudget := cfg.DialBudget
	if dialBudget == 0 {
		dialBudget = DefaultDialBudget
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = DefaultIdleTimeout
	}
	keepAlive := cfg.KeepAlivePeriod
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlivePeriod
	}
	maxSessions := cfg.MaxSessions
	if maxSessions == 0 {
		maxSessions = DefaultMaxSessions
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Bridge{
		cfg:         cfg,
		magicUUID:   magicUUID,
		dialTimeout: dialTimeout,
		dialRetries: dialRetries,
		dialBackoff: dialBackoff,
		dialBudget:  dialBudget,
		idleTimeout: idleTimeout,
		keepAlive:   keepAlive,
		maxSessions: maxSessions,
		log:         log,
		conns:       make(map[net.Conn]struct{}),
	}, nil
}

// Listen binds the configured address so Addr reports the real bound port
// before any connection arrives. Serve calls it implicitly when needed.
func (b *Bridge) Listen() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listener != nil {
		return nil
	}
	listener, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("bridge: listen on %s: %w", b.cfg.ListenAddr, err)
	}
	b.listener = listener
	return nil
}

// Addr returns the bound listen address, or nil before Listen/Serve.
func (b *Bridge) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// Serve accepts connections until ctx is cancelled or Close is called. It binds
// the listener if Listen has not already been called. Each accepted connection
// is handled in its own goroutine. Serve returns nil on a clean shutdown.
func (b *Bridge) Serve(ctx context.Context) error {
	if err := b.Listen(); err != nil {
		return err
	}
	// Read the listener once, under the lock that Listen and Close use. Accept
	// then runs against a local copy instead of a field another goroutine may
	// be writing.
	b.mu.Lock()
	listener := b.listener
	b.mu.Unlock()
	b.log.Info("bridge listening",
		"addr", b.Addr().String(),
		"control_host", b.cfg.Credentials.ControlHost,
		"target_port", b.targetPort())

	// Unblock Accept when ctx is cancelled.
	stop := make(chan struct{})
	var once sync.Once
	closeStop := func() { once.Do(func() { close(stop) }) }
	defer closeStop()
	go func() {
		select {
		case <-ctx.Done():
			b.Close()
		case <-stop:
		}
	}()

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			closeStop()
			wg.Wait()
			if b.isClosing() || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("bridge: accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handle(ctx, conn)
		}()
	}
}

// Close stops accepting and tears down the listener and all live client
// connections. Tunnels close as their copy loops observe the closed sockets.
func (b *Bridge) Close() error {
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		return nil
	}
	b.closing = true
	listener := b.listener
	conns := make([]net.Conn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()

	var err error
	if listener != nil {
		err = listener.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	return err
}

// Stats reports lifetime counters: total sessions accepted and currently active.
func (b *Bridge) Stats() (total, active int64) {
	return atomic.LoadInt64(&b.sessions), atomic.LoadInt64(&b.active)
}

func (b *Bridge) handle(ctx context.Context, client net.Conn) {
	if !b.reserveSession() {
		b.log.Warn("refused client: too many concurrent sessions for this camera",
			"client", client.RemoteAddr().String(),
			"max_sessions", b.maxSessions)
		_ = client.Close()
		return
	}
	id := atomic.AddInt64(&b.sessions, 1)
	b.trackConn(client, true)
	setKeepAlive(client, b.keepAlive)
	log := b.log.With("session", id, "client", client.RemoteAddr().String())
	log.Info("client connected")

	defer func() {
		_ = client.Close()
		b.trackConn(client, false)
		atomic.AddInt64(&b.active, -1)
		log.Info("client disconnected")
	}()

	// Watch the client for the duration of the dial. An RTSP client sends its
	// first request as soon as the socket is up and then waits, and media
	// servers give up in seconds while a dial with retries runs far longer.
	// Without this the bridge kept dialling for a peer that had already left,
	// and that session held its slot the whole time — enough of them and the
	// concurrency cap starts refusing clients that would have worked.
	watch := watchClient(client)
	tunnel, err := b.dial(ctx, log, watch.gone)
	pending := watch.stop()
	if err != nil {
		if watch.left() {
			log.Info("client left before the relay was ready", "err", err)
			return
		}
		log.Error("relay dial failed", "err", err)
		return
	}
	defer tunnel.Close()
	if err := tunnel.SetKeepAlive(b.keepAlive); err != nil {
		log.Debug("could not set relay keepalive", "err", err)
	}
	log.Info("relay session open",
		"stream_host", tunnel.Response.StreamHost,
		"connection_num", tunnel.Response.ConnectionNumber,
		"mode", tunnel.Response.Mode)

	// When the outer context is cancelled, drop both ends so the copies return.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = client.Close()
			_ = tunnel.Close()
		case <-stop:
		}
	}()

	// Whatever the client sent while the relay was being dialled goes first, so
	// the tunnel carries its request stream in the order it was written.
	if len(pending) > 0 {
		if _, err := tunnel.Write(pending); err != nil {
			log.Error("could not forward the request the client sent while dialling", "err", err)
			return
		}
	}

	fromClient, fromRelay, idled := pipe(client, tunnel, b.idleTimeout, log)
	fromClient += int64(len(pending))
	log.Info("relay session closed", "bytes_to_relay", fromClient, "bytes_from_camera", fromRelay)
	// An idle drop after the camera did stream is a relay or camera that went
	// away without closing the socket. Left alone that session would block in
	// its copy forever, hold two sockets and a goroutine, and keep counting as
	// an active session while nobody is watching.
	if idled && fromRelay > 0 && ctx.Err() == nil {
		log.Warn("dropped an idle session: the camera stopped sending",
			"idle_timeout", b.idleTimeout,
			"bytes_from_camera", fromRelay)
	}
	// A session that opened but never carried a single camera byte is the exact
	// signature of a relay session without an attached camera peer. In the wild
	// this means the 5GenCare-authorized session is missing or expired; make
	// that legible instead of a silent empty stream. Skipped on context cancel,
	// where the empty read is our own shutdown.
	if fromRelay == 0 && ctx.Err() == nil {
		log.Warn("relay opened but camera sent no data; the camera did not attach. "+
			"This is expected without a valid 5GenCare-authorized session "+
			"(fresh SID / device token / stream accessToken). See docs/bridge.md",
			"bytes_to_relay", fromClient)
	}
}

// dial opens a relay session under two bounds the retry loop does not have on
// its own: a budget for the whole attempt sequence, and the client still being
// there to receive the result. Either one ending the dial frees this session's
// slot instead of letting it run out the full retry sequence.
func (b *Bridge) dial(ctx context.Context, log *slog.Logger, gone <-chan struct{}) (*magic.Tunnel, error) {
	// Both branches produce a cancellable context, and only one of them is
	// created: overwriting a context.WithCancel result would drop its cancel
	// func on the floor, and every such context stays attached to the
	// long-lived per-camera context until the process ends.
	var dialCtx context.Context
	var cancel context.CancelFunc
	if b.dialBudget > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, b.dialBudget)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-gone:
			cancel()
		case <-stop:
		}
	}()

	return b.dialWithRetry(dialCtx, log)
}

// clientWatch reads from a client while its relay session is being dialled. It
// keeps what the client sent, for replay toward the relay once the tunnel is
// up, and reports a client that went away in the meantime.
type clientWatch struct {
	conn net.Conn
	done chan struct{} // closed once the reader has stopped
	gone chan struct{} // closed when the client went away on its own

	mu       sync.Mutex
	buf      bytes.Buffer
	stopping bool
}

func watchClient(conn net.Conn) *clientWatch {
	w := &clientWatch{conn: conn, done: make(chan struct{}), gone: make(chan struct{})}
	go w.read()
	return w
}

func (w *clientWatch) read() {
	defer close(w.done)
	buf := make([]byte, 4096)
	for {
		n, err := w.conn.Read(buf)
		if n > 0 {
			w.mu.Lock()
			w.buf.Write(buf[:n])
			w.mu.Unlock()
		}
		if err != nil {
			w.mu.Lock()
			stopping := w.stopping
			w.mu.Unlock()
			// The deadline stop sets to wake this read is our own doing, not a
			// client that left.
			if !stopping {
				close(w.gone)
			}
			return
		}
	}
}

// stop ends the watch and returns every byte the client sent while it ran. The
// read deadline it uses to wake the reader is cleared again, so the session's
// own copy loop starts from a clean connection.
func (w *clientWatch) stop() []byte {
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()

	_ = w.conn.SetReadDeadline(time.Now())
	<-w.done
	_ = w.conn.SetReadDeadline(time.Time{})

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Bytes()
}

// left reports whether the client went away while the watch ran.
func (w *clientWatch) left() bool {
	select {
	case <-w.gone:
		return true
	default:
		return false
	}
}

// dialWithRetry opens a relay session, retrying transient failures with an
// exponential backoff bounded by the outer context. Each attempt gets its own
// dial timeout.
func (b *Bridge) dialWithRetry(ctx context.Context, log *slog.Logger) (*magic.Tunnel, error) {
	backoff := b.dialBackoff
	var lastErr error
	for attempt := 0; attempt <= b.dialRetries; attempt++ {
		if attempt > 0 {
			log.Warn("retrying relay dial", "attempt", attempt, "max", b.dialRetries, "prev_err", lastErr)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		dialCtx, cancel := context.WithTimeout(ctx, b.dialTimeout)
		tunnel, err := magic.Dial(dialCtx, magic.TunnelConfig{
			ControlHost: b.cfg.Credentials.ControlHost,
			ControlPort: b.cfg.Credentials.ControlPort,
			MagicUUID:   b.magicUUID,
			TargetPort:  b.targetPort(),
			SessionName: freshSessionName(),
			DeviceToken: b.cfg.Credentials.DeviceToken,
			Dial:        b.cfg.Dial,
		})
		cancel()
		if err == nil {
			return tunnel, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// pipe copies bytes in both directions until either side closes, then reports
// the number of bytes carried from the client to the relay and from the relay
// (camera) back to the client, and whether the camera direction was dropped for
// being idle.
//
// The idle timeout applies to the camera direction only. A player is entitled
// to send nothing for minutes after PLAY, but a live stream that stops arriving
// means the far end is gone — and a relay that vanishes without closing its
// socket produces no read error at all, so nothing else ends the session.
func pipe(client net.Conn, tunnel *magic.Tunnel, idle time.Duration, log *slog.Logger) (fromClient, fromRelay int64, idled bool) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Each goroutine writes only its own results, exactly once; wg.Wait below
	// establishes the happens-before edge for reading them.
	copyDir := func(dst io.Writer, src io.Reader, dir string, timeout time.Duration, count *int64, timedOut *bool, closeDst func()) {
		defer wg.Done()
		n, err := copyIdle(dst, src, timeout)
		*count = n
		if errors.Is(err, ErrIdleTimeout) {
			*timedOut = true
		} else if err != nil && !isExpectedClose(err) {
			log.Debug("copy ended", "dir", dir, "err", err)
		}
		// Closing the destination unblocks the opposite direction's Read.
		closeDst()
	}

	var clientIdled bool
	go copyDir(tunnel, client, "client->relay", 0, &fromClient, &clientIdled, func() { _ = tunnel.Close() })
	go copyDir(client, tunnel, "relay->client", idle, &fromRelay, &idled, func() { _ = client.Close() })
	wg.Wait()
	return fromClient, fromRelay, idled
}

// copyIdle copies src to dst, giving each read at most idle to produce a byte.
// A non-positive idle, or a source that cannot take a read deadline, falls back
// to a plain copy.
func copyIdle(dst io.Writer, src io.Reader, idle time.Duration) (int64, error) {
	deadliner, ok := src.(interface{ SetReadDeadline(time.Time) error })
	if !ok || idle <= 0 {
		return io.Copy(dst, src)
	}

	buf := make([]byte, 32*1024)
	var total int64
	for {
		if err := deadliner.SetReadDeadline(time.Now().Add(idle)); err != nil {
			return total, err
		}
		read, readErr := src.Read(buf)
		if read > 0 {
			written, writeErr := dst.Write(buf[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written < read {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			switch {
			case errors.Is(readErr, os.ErrDeadlineExceeded):
				return total, ErrIdleTimeout
			case errors.Is(readErr, io.EOF):
				return total, nil
			default:
				return total, readErr
			}
		}
	}
}

// reserveSession claims one of the concurrent-session slots, reporting false
// when the camera is already at its cap.
func (b *Bridge) reserveSession() bool {
	if b.maxSessions <= 0 {
		atomic.AddInt64(&b.active, 1)
		return true
	}
	for {
		active := atomic.LoadInt64(&b.active)
		if active >= int64(b.maxSessions) {
			return false
		}
		if atomic.CompareAndSwapInt64(&b.active, active, active+1) {
			return true
		}
	}
}

// setKeepAlive turns on TCP keepalive probes so a peer that disappeared without
// closing its socket eventually surfaces as a read error instead of silence.
func setKeepAlive(conn net.Conn, period time.Duration) {
	if period <= 0 {
		return
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	_ = tcp.SetKeepAlivePeriod(period)
}

func (b *Bridge) targetPort() int {
	if b.cfg.Credentials.TargetPort != 0 {
		return b.cfg.Credentials.TargetPort
	}
	return DefaultTargetPort
}

func (b *Bridge) trackConn(c net.Conn, add bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if add {
		b.conns[c] = struct{}{}
	} else {
		delete(b.conns, c)
	}
}

func (b *Bridge) isClosing() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closing
}

func isExpectedClose(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)
}

// freshSessionName returns a canonical 36-char UUID used as the client session
// label in the app-discovery request, one per relay session.
func freshSessionName() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
