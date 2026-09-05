package app

import (
	"testing"

	"github.com/local/motorola-vm65-bridge/internal/bridge"
)

func TestBuildRegistryCreatesStableUniqueStreamNamesAndLegacyAlias(t *testing.T) {
	cameras := []bridge.Credentials{
		{DeviceID: 2, DeviceUDID: "z", DeviceName: "Baby Room"},
		{DeviceID: 1, DeviceUDID: "a", DeviceName: "Baby Room"},
		{DeviceID: 3, DeviceUDID: "m", DeviceName: ""},
	}
	registry, err := BuildRegistry("127.0.0.1:8554", cameras)
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Cameras) != 3 {
		t.Fatalf("camera count = %d", len(registry.Cameras))
	}
	wantNames := []string{"baby-room", "camera-m", "baby-room-2"}
	wantAddresses := []string{"127.0.0.1:8554", "127.0.0.1:9554", "127.0.0.1:9555"}
	for index, camera := range registry.Cameras {
		if camera.StreamName != wantNames[index] || camera.ListenAddr != wantAddresses[index] {
			t.Fatalf("camera %d = %#v", index, camera)
		}
	}
	if registry.LegacyAlias != "vm65" || registry.LegacyTarget != "baby-room" {
		t.Fatalf("legacy alias = %q -> %q", registry.LegacyAlias, registry.LegacyTarget)
	}
}

// A camera whose own name already reads like a deduplicated one must not take
// the name another camera was given: two cameras sharing a stream name share
// one go2rtc stream, one snapshot URL and one card on the page.
func TestBuildRegistryNamesStayUniqueAgainstADeduplicatedName(t *testing.T) {
	registry, err := BuildRegistry("127.0.0.1:8554", []bridge.Credentials{
		{DeviceID: 1, DeviceUDID: "a", DeviceName: "Nursery"},
		{DeviceID: 2, DeviceUDID: "b", DeviceName: "Nursery"},
		{DeviceID: 3, DeviceUDID: "c", DeviceName: "Nursery 2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(registry.Cameras))
	for _, camera := range registry.Cameras {
		if seen[camera.StreamName] {
			names := make([]string, 0, len(registry.Cameras))
			for _, item := range registry.Cameras {
				names = append(names, item.StreamName)
			}
			t.Fatalf("duplicate stream name %q in %v", camera.StreamName, names)
		}
		seen[camera.StreamName] = true
	}
}
