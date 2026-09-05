package app

import (
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/local/motorola-vm65-bridge/internal/bridge"
)

type Camera struct {
	Credentials bridge.Credentials
	StreamName  string
	ListenAddr  string
}

type Registry struct {
	Cameras      []Camera
	LegacyAlias  string
	LegacyTarget string
}

func BuildRegistry(baseAddress string, credentials []bridge.Credentials) (Registry, error) {
	host, portText, err := net.SplitHostPort(baseAddress)
	if err != nil {
		return Registry{}, err
	}
	basePort, err := strconv.Atoi(portText)
	if err != nil || basePort < 1 || basePort > 65535 {
		return Registry{}, errors.New("base bridge port must be between 1 and 65535")
	}
	ordered := append([]bridge.Credentials(nil), credentials...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].DeviceUDID < ordered[j].DeviceUDID
	})
	if len(ordered) > 1 && basePort+998+len(ordered) > 65535 {
		return Registry{}, errors.New("camera count exceeds available bridge ports")
	}

	registry := Registry{LegacyAlias: "vm65"}
	// Every name that has been handed out, not a count per base name: counting
	// alone hands "Nursery", "Nursery" and "Nursery 2" the names "nursery",
	// "nursery-2" and "nursery-2" — two cameras sharing one go2rtc stream, one
	// snapshot URL and one card on the page.
	usedNames := make(map[string]bool, len(ordered))
	for index, credential := range ordered {
		listenPort := basePort
		if index > 0 {
			listenPort = basePort + 999 + index
		}
		nameSource := credential.DeviceName
		if nameSource == "" {
			nameSource = "camera-" + credential.DeviceUDID
		}
		baseName := streamName(nameSource)
		name := baseName
		for suffix := 2; usedNames[name]; suffix++ {
			name = baseName + "-" + strconv.Itoa(suffix)
		}
		usedNames[name] = true
		registry.Cameras = append(registry.Cameras, Camera{
			Credentials: credential,
			StreamName:  name,
			ListenAddr:  net.JoinHostPort(host, strconv.Itoa(listenPort)),
		})
	}
	if len(registry.Cameras) > 0 {
		registry.LegacyTarget = registry.Cameras[0].StreamName
	}
	return registry, nil
}

func streamName(name string) string {
	var builder strings.Builder
	separator := false
	for _, character := range strings.ToLower(name) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(character)
			separator = false
			continue
		}
		if builder.Len() > 0 && !separator {
			builder.WriteByte('-')
			separator = true
		}
	}
	result := strings.Trim(builder.String(), "-")
	if result == "" {
		return "camera"
	}
	return result
}
