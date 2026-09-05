package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/local/motorola-vm65-bridge/internal/app"
	"github.com/local/motorola-vm65-bridge/internal/bridge"
	appconfig "github.com/local/motorola-vm65-bridge/internal/config"
	"github.com/local/motorola-vm65-bridge/internal/health"
)

func TestLoadCredentialSetReadsEveryRegistryCamera(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cameras.json")
	data := `{"cameras":[
		{"device_id":1,"device_udid":"a","device_name":"Room A","model":"VM65CONNECT","sid":"sid-a","device_token":"token-a","control_host":"relay","device_api_host":"shard.example","device_api_port":2288},
		{"device_id":2,"device_udid":"b","device_name":"Room B","model":"MBP99","sid":"sid-b","device_token":"token-b","control_host":"relay"}
	]}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cameras, err := loadCredentialSet(appconfig.Config{RegistryPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if len(cameras) != 2 || cameras[1].Model != "MBP99" {
		t.Fatalf("cameras = %#v", cameras)
	}
	if cameras[0].DeviceAPIHost != "shard.example" || cameras[0].DeviceAPIPort != 2288 {
		t.Fatalf("device API endpoint = %s:%d", cameras[0].DeviceAPIHost, cameras[0].DeviceAPIPort)
	}
}

func TestTemperatureCamerasKeepOnlyCompleteControlCredentials(t *testing.T) {
	registry := app.Registry{Cameras: []app.Camera{
		{Credentials: bridge.Credentials{DeviceID: 7, DeviceUDID: "camera-a", DeviceToken: "token-a", DeviceAPIHost: "shard.example", DeviceAPIPort: 2288}},
		{Credentials: bridge.Credentials{DeviceID: 8, DeviceUDID: "camera-b", DeviceToken: "token-b"}},
	}}
	cameras := temperatureCameras(registry)
	if len(cameras) != 1 {
		t.Fatalf("temperature cameras = %#v", cameras)
	}
	if cameras[0].ID != "camera-a" || cameras[0].DeviceID != 7 || cameras[0].Host != "shard.example" || cameras[0].Port != 2288 {
		t.Fatalf("temperature camera = %#v", cameras[0])
	}
}

func TestMonitorGo2RTCUpdatesReadiness(t *testing.T) {
	var ready atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	state := health.NewState(time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitorGo2RTC(ctx, server.Client(), server.URL, 5*time.Millisecond, state)

	waitForGo2RTC(t, state, false)
	ready.Store(true)
	waitForGo2RTC(t, state, true)
}

func waitForGo2RTC(t *testing.T, state *health.State, want bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if state.Snapshot().Go2RTCReady == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("go2rtc ready = %t, want %t", state.Snapshot().Go2RTCReady, want)
}

func TestStreamNamesCoverEveryCameraAndTheLegacyAlias(t *testing.T) {
	registry, err := app.BuildRegistry("127.0.0.1:8554", []bridge.Credentials{
		{DeviceUDID: "a", DeviceName: "Room A", SID: "s", DeviceToken: "t", ControlHost: "relay"},
		{DeviceUDID: "b", DeviceName: "Room B", SID: "s", DeviceToken: "t", ControlHost: "relay"},
	})
	if err != nil {
		t.Fatal(err)
	}
	names := streamNames(registry)
	// Each camera also carries the MJPEG companion stream, which the proxy has
	// to allow or the last-resort transport is refused as an unknown stream.
	want := map[string]bool{
		"vm65": true, "room-a": true, "room-b": true,
		"vm65-mjpeg": true, "room-a-mjpeg": true, "room-b-mjpeg": true,
	}
	if len(names) != len(want) {
		t.Fatalf("names = %v", names)
	}
	for _, name := range names {
		if !want[name] {
			t.Fatalf("unexpected stream name %q in %v", name, names)
		}
	}
}

// The Web UI and the snapshot endpoint share the Ingress listener: the Web UI
// needs an authenticated Home Assistant user, the snapshot needs the token,
// and neither answers a peer outside the Supervisor network.
func TestWebServerServesAnAuthenticatedUIAndTokenisedSnapshots(t *testing.T) {
	go2rtc := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/frame.jpeg" {
			writer.Header().Set("Content-Type", "image/jpeg")
			_, _ = writer.Write([]byte{0xFF, 0xD8, 0x00})
			return
		}
		_, _ = writer.Write([]byte("go2rtc web ui"))
	}))
	defer go2rtc.Close()

	registry, err := app.BuildRegistry("127.0.0.1:8554", []bridge.Credentials{
		{DeviceUDID: "a", DeviceName: "Room A", SID: "s", DeviceToken: "t", ControlHost: "relay"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(t.TempDir(), "snapshot-token")
	cfg := appconfig.Config{
		IngressAddr:       "127.0.0.1:0",
		SnapshotBase:      "http://local-vm65-bridge:8099",
		SnapshotTokenFile: tokenPath,
		Go2RTCURL:         go2rtc.URL,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.IngressAddr = address

	healthState := health.NewState(time.Now())
	healthState.SetLive(true)
	runtime := app.New(app.RuntimeConfig{Registry: registry, Health: healthState})
	web, err := startWebServer(ctx, cfg, registry, runtime, newTemperatureStore(), healthState,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("startWebServer: %v", err)
	}
	snapshots := web.cache()
	defer snapshots.Close()
	if snapshots.Token() == "" {
		t.Fatal("a published snapshot URL needs a token")
	}

	base := "http://" + address
	waitForServer(t, base)

	cases := []struct {
		name    string
		target  string
		headers map[string]string
		want    int
	}{
		{"web UI without a Home Assistant session", base + "/", nil, http.StatusUnauthorized},
		{"web UI through ingress", base + "/", map[string]string{"X-Remote-User-Id": "01HQ"}, http.StatusOK},
		{"camera overview", base + "/api/cameras", map[string]string{"X-Remote-User-Id": "01HQ"}, http.StatusOK},
		// go2rtc's own page and configuration are no longer proxied at all.
		{"go2rtc configuration", base + "/api/config", map[string]string{"X-Remote-User-Id": "01HQ"}, http.StatusNotFound},
		{"snapshot without a token", base + "/snapshot?src=vm65", nil, http.StatusUnauthorized},
		{"snapshot with the token", base + "/snapshot?src=vm65&token=" + snapshots.Token(), nil, http.StatusOK},
		{"snapshot of an unknown camera", base + "/snapshot?src=exec:id&token=" + snapshots.Token(), nil, http.StatusNotFound},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, testCase.target, nil)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range testCase.headers {
				request.Header.Set(key, value)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != testCase.want {
				t.Fatalf("status = %d, want %d", response.StatusCode, testCase.want)
			}
		})
	}
}

func waitForServer(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(base + "/")
		if err == nil {
			_ = response.Body.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the web server never started listening")
}

// A credential refresh that added a camera reaches the Ingress listener too.
// The stills, the media proxy's stream allowlist and the page's own camera list
// were each fixed at startup, so the new camera used to answer 404 for its
// thumbnail and "unknown stream" for its video until the add-on was restarted.
func TestReloadTeachesTheWebStackANewCamera(t *testing.T) {
	go2rtc := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/frame.jpeg" {
			writer.Header().Set("Content-Type", "image/jpeg")
			_, _ = writer.Write([]byte{0xFF, 0xD8, 0x00})
			return
		}
		_, _ = writer.Write([]byte("go2rtc web ui"))
	}))
	defer go2rtc.Close()

	first := bridge.Credentials{DeviceUDID: "a", DeviceName: "Room A", SID: "s", DeviceToken: "t", ControlHost: "relay"}
	second := bridge.Credentials{DeviceUDID: "b", DeviceName: "Room B", SID: "s", DeviceToken: "t", ControlHost: "relay"}
	registry, err := app.BuildRegistry("127.0.0.1:8554", []bridge.Credentials{first})
	if err != nil {
		t.Fatal(err)
	}
	cfg := appconfig.Config{
		IngressAddr:       "127.0.0.1:0",
		SnapshotTokenFile: filepath.Join(t.TempDir(), "snapshot-token"),
		Go2RTCURL:         go2rtc.URL,
		StreamURL:         "rtsp://stream.example:8555/vm65",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.IngressAddr = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	healthState := health.NewState(time.Now())
	healthState.SetLive(true)
	runtime := app.New(app.RuntimeConfig{Registry: registry, Health: healthState})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	web, err := startWebServer(ctx, cfg, registry, runtime, newTemperatureStore(), healthState, logger)
	if err != nil {
		t.Fatalf("startWebServer: %v", err)
	}
	defer web.cache().Close()

	base := "http://" + cfg.IngressAddr
	waitForServer(t, base)
	token := web.cache().Token()

	if status := getStatus(t, base+"/snapshot?src=room-b&token="+token, nil); status != http.StatusNotFound {
		t.Fatalf("a camera that is not in the registry answered %d, want %d", status, http.StatusNotFound)
	}

	grown, err := app.BuildRegistry("127.0.0.1:8554", []bridge.Credentials{first, second})
	if err != nil {
		t.Fatal(err)
	}
	web.reload(cfg, grown, logger)

	if status := getStatus(t, base+"/snapshot?src=room-b&token="+token, nil); status != http.StatusOK {
		t.Fatalf("the added camera's snapshot answered %d, want %d", status, http.StatusOK)
	}
	ingressHeaders := map[string]string{"X-Remote-User-Id": "01HQ"}
	if status := getStatus(t, base+"/api/frame.jpeg?src=room-b", ingressHeaders); status != http.StatusOK {
		t.Fatalf("the media proxy answered %d for the added camera, want %d", status, http.StatusOK)
	}
	// A stream nobody configured still has to be refused after a reload.
	if status := getStatus(t, base+"/api/frame.jpeg?src=exec:id", ingressHeaders); status != http.StatusForbidden {
		t.Fatalf("an unknown stream answered %d, want %d", status, http.StatusForbidden)
	}
	// And a camera that left the registry stops being served.
	web.reload(cfg, registry, logger)
	if status := getStatus(t, base+"/snapshot?src=room-b&token="+token, nil); status != http.StatusNotFound {
		t.Fatalf("a removed camera answered %d, want %d", status, http.StatusNotFound)
	}
}

func getStatus(t *testing.T, target string, headers map[string]string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

// A port that is already taken is a configuration error. Reporting it as a log
// line while the process carried on left the Web UI and the snapshot endpoint
// unreachable behind a line that claimed they were listening.
func TestStartHTTPServerReportsAPortItCannotBind(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := http.NewServeMux()

	if err := startHTTPServer(ctx, "web UI", occupied.Addr().String(), handler, logger); err == nil {
		t.Fatal("binding a port that is in use reported no error")
	}
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := free.Addr().String()
	if err := free.Close(); err != nil {
		t.Fatal(err)
	}
	if err := startHTTPServer(ctx, "web UI", address, handler, logger); err != nil {
		t.Fatalf("binding a free port failed: %v", err)
	}
	waitForServer(t, "http://"+address)
}
