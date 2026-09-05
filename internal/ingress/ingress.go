// Package ingress serves the add-on Web UI behind Home Assistant Ingress.
//
// go2rtc has no authentication of its own. Publishing its port on the host
// therefore hands anyone on the network the camera credentials — `/api/config`
// returns the generated go2rtc configuration, RTSP password and camera access
// token included — and a way to run commands inside the container, because
// go2rtc creates a stream from any `src=` it is given and `exec:` is a valid
// source scheme.
//
// This proxy is what the Supervisor talks to instead. It enforces three things
// before a single byte reaches go2rtc:
//
//  1. the request arrives over the Supervisor network, so an unpublished port
//     stays unreachable from the LAN even if it is published by hand later;
//  2. the request carries the `X-Remote-User-Id` header the Supervisor adds to
//     every ingress request, so an authenticated Home Assistant user is behind
//     it (https://developers.home-assistant.io/docs/apps/security/);
//  3. the request only asks for something a viewer needs — configured streams,
//     never a new source and never the configuration.
package ingress

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
	"sync"
)

// UserIDHeader is the header the Supervisor sets on every ingress request. Its
// presence is what tells the add-on a Home Assistant session was authenticated;
// the add-on never sees, and never needs, the session token itself.
const UserIDHeader = "X-Remote-User-Id"

// remoteUserHeaders are set by the Supervisor and must never be believed when a
// client sends them: without stripping, anyone able to reach the port could
// forge an authenticated user. They are dropped on the way in and re-added from
// the values this handler validated.
var remoteUserHeaders = []string{
	UserIDHeader,
	"X-Remote-User-Name",
	"X-Remote-User-Display-Name",
}

// blockedPaths never reach go2rtc. `/api/config`, `/api/streams` and
// `/api/streams.dot` expose the generated configuration and the stream sources,
// which carry the camera's RTSP password and access token; the rest change the
// running process. The player needs none of them: the Web UI names the stream
// it wants on the media endpoints directly.
var blockedPaths = map[string]bool{
	"/api/config":      true,
	"/api/exit":        true,
	"/api/restart":     true,
	"/api/streams":     true,
	"/api/streams.dot": true,
}

type Config struct {
	// Upstream is the go2rtc base URL, normally http://127.0.0.1:1984/.
	Upstream string
	// Streams are the stream names a request may name in `src`. Anything else
	// is refused, which is what stops `src=exec:...` from reaching go2rtc.
	Streams []string
	// TrustedCIDRs restricts who may connect. Nil selects
	// netguard.SupervisorCIDRs; an explicitly empty slice disables the check.
	TrustedCIDRs []string
	// RequireUser demands the Supervisor's ingress user header. It defaults to
	// true and only tests turn it off.
	RequireUser *bool
	Logger      *slog.Logger
}

// NewHandler builds the authenticating reverse proxy. The concrete type is
// returned so a credential reload can replace the stream allowlist; it is an
// http.Handler wherever only that is needed.
func NewHandler(cfg Config) (*Handler, error) {
	target, err := url.Parse(cfg.Upstream)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, errors.New("ingress upstream must be an absolute http or https URL")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	authenticator, err := NewAuthenticator(cfg.TrustedCIDRs, cfg.Logger)
	if err != nil {
		return nil, err
	}
	requireUser := true
	if cfg.RequireUser != nil {
		requireUser = *cfg.RequireUser
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.Host = target.Host
			// SetXForwarded replaces whatever the client sent, so a forged
			// X-Forwarded-For cannot reach go2rtc either.
			request.SetXForwarded()
			// go2rtc refuses a WebSocket whose Origin is not its own host, and
			// through Ingress the browser's Origin is Home Assistant — which
			// cost every MSE attempt a 403 and left remote viewing with no
			// transport. Presenting the upstream's own origin is honest here:
			// the request already passed this add-on's authentication and its
			// stream check, which is what Origin would have been guarding.
			if request.In.Header.Get("Origin") != "" {
				request.Out.Header.Set("Origin", target.Scheme+"://"+target.Host)
			}
			for _, header := range remoteUserHeaders {
				if value := request.In.Header.Get(header); value != "" {
					request.Out.Header.Set(header, value)
				} else {
					request.Out.Header.Del(header)
				}
			}
		},
		ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
			cfg.Logger.Warn("media upstream unavailable", "path", request.URL.Path, "err", err)
			http.Error(writer, "the media server is not available", http.StatusBadGateway)
		},
		// A player that gets no picture is the hardest thing to diagnose from
		// a phone, so whatever go2rtc refused is written to the add-on log
		// where it can actually be read.
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode >= http.StatusBadRequest {
				cfg.Logger.Warn("media request refused by go2rtc",
					"path", response.Request.URL.Path,
					"query", response.Request.URL.RawQuery,
					"status", response.StatusCode)
			}
			return nil
		},
	}

	handler := &Handler{
		proxy:         proxy,
		authenticator: authenticator,
		streams:       streamSet(cfg.Streams),
		requireUser:   requireUser,
		logger:        cfg.Logger,
	}
	return handler, nil
}

// Handler is the authenticating go2rtc proxy.
type Handler struct {
	proxy         http.Handler
	authenticator *Authenticator
	requireUser   bool
	logger        *slog.Logger

	mu      sync.RWMutex
	streams map[string]bool
}

// SetStreams replaces the stream names a request may name in `src`. A
// credential reload that added a camera has to hand it over, or the player asks
// for a stream this proxy still refuses as unknown until the add-on restarts.
func (a *Handler) SetStreams(names []string) {
	set := streamSet(names)
	a.mu.Lock()
	a.streams = set
	a.mu.Unlock()
}

func (a *Handler) allows(name string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.streams[name]
}

func streamSet(names []string) map[string]bool {
	streams := make(map[string]bool, len(names))
	for _, name := range names {
		if name != "" {
			streams[name] = true
		}
	}
	return streams
}

func (a *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if a.requireUser {
		if !a.authenticator.Allow(writer, request) {
			return
		}
	} else if !a.authenticator.guard.Allow(request.RemoteAddr) {
		http.Error(writer, "forbidden", http.StatusForbidden)
		return
	}
	if reason, ok := a.refuse(request); !ok {
		a.logger.Warn("refused a Web UI request", "path", request.URL.Path, "method", request.Method, "reason", reason)
		http.Error(writer, "forbidden: "+reason, http.StatusForbidden)
		return
	}
	SetSecurityHeaders(writer.Header())
	a.proxy.ServeHTTP(writer, request)
}

// refuse decides whether one request may reach go2rtc, and says why not.
func (a *Handler) refuse(request *http.Request) (string, bool) {
	// Not named `path`: that is the package normalizePath cleans with.
	requestPath := normalizePath(request.URL.Path)
	if blockedPaths[requestPath] {
		return "this go2rtc endpoint is not exposed through the add-on", false
	}
	// Only reads pass. Every go2rtc write endpoint either edits the running
	// configuration or adds a source, and the add-on owns both.
	switch request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
	case http.MethodPost:
		// WebRTC and MSE negotiation are posts against an existing stream.
		if requestPath != "/api/webrtc" && requestPath != "/api/stream" {
			return "the Web UI is read-only in this add-on", false
		}
	default:
		return "the Web UI is read-only in this add-on", false
	}
	for _, value := range srcValues(request.URL.Query()) {
		if !a.allows(value) {
			// A src that is not a configured stream is a request for go2rtc to
			// create one, and "exec:" is a source scheme.
			return "unknown stream", false
		}
	}
	return "", true
}

// srcValues collects every query parameter go2rtc reads a source from.
func srcValues(query url.Values) []string {
	var values []string
	for _, key := range []string{"src", "source", "dst"} {
		values = append(values, query[key]...)
	}
	return values
}

// normalizePath makes the block list independent of a trailing slash or of a
// path the Supervisor did not clean. path.Clean does the collapsing: without it
// "//api/config" and "/api/./config" miss the block list by a byte while go2rtc
// resolves both to the endpoint that returns the camera's RTSP password.
func normalizePath(requestPath string) string {
	if requestPath == "" {
		return "/"
	}
	if !strings.HasPrefix(requestPath, "/") {
		requestPath = "/" + requestPath
	}
	cleaned := strings.TrimSuffix(path.Clean(requestPath), "/")
	if cleaned == "" {
		return "/"
	}
	return cleaned
}
