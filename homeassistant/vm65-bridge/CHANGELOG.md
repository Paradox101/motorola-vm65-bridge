# Changelog

## Unreleased

### Fixed

- A camera added to your account now appears in the add-on on the next
  credential refresh instead of after a restart: its thumbnail, its video and
  its card on the page used to keep answering as if it did not exist.
- The go2rtc endpoints that carry the camera's RTSP password and access token
  are blocked in every spelling of their path, and the stream list is blocked
  too. Live video does not use either.
- A port that is already in use now stops the add-on with the reason, instead of
  leaving the Web UI and snapshots quietly unreachable.
- After a credential refresh, a camera's temperature no longer shows as
  unavailable while it is being read.
- Two cameras with near-identical names no longer end up sharing one video
  stream and one snapshot.

## 0.12.0

### Added

- **An optional burnt-in overlay**: a clock in the top-left corner and the
  camera name in the bottom-right, the way a camera with its own on-screen
  display does it. Turn on `stream_overlay` in the add-on options. It is off by
  default and stays that way on upgrade: the camera cannot draw this itself, so
  the add-on re-encodes every frame to add it, which costs processing power on
  the machine running Home Assistant. Every published name of a camera reads one
  untouched source stream, so the overlay costs a single relay session; the
  camera's audio is copied through rather than dropped; the camera name is
  handed to ffmpeg in a file, so an apostrophe or colon in it is drawn instead
  of silently eating the rest of the name; and the add-on renders one frame
  through the filter before using it, falling back to the plain picture with a
  log line if this build of ffmpeg will not draw it.

### Fixed

- **The concurrency cap could refuse clients that would have worked.** A
  session occupies its slot from the moment it is accepted, including while its
  relay is being dialled — and that dial, with retries and backoff, can run for
  the better part of a minute. A media server gives up on a source after a few
  seconds and reconnects, so an unreachable relay filled every slot with dials
  for clients that had already left, and the reconnect burst that follows —
  measured at eighteen connections inside a tenth of a second — was refused
  outright. The bridge now reads the client while it dials, replays that
  opening request toward the relay once the tunnel is up, and abandons the dial
  as soon as the client goes away. A total dial budget of 25 seconds bounds the
  case where the client does wait.
- The per-camera session cap is raised from eight to sixteen. A media server
  whose producer died reconnects every consumer at once, and that legitimate
  burst was measured above eight; the cap is the last-resort guard, not a
  throttle on normal bursts.

## 0.11.0

### Fixed

- **Sessions that never ended.** A relay that stopped sending without closing
  its socket left both copy loops blocked forever: the session held two sockets
  and a goroutine, and kept counting as active. Reconnects piled new sessions on
  top of the dead ones, which is why one camera could report a dozen. Sessions
  now carry an idle timeout (60s without a byte from the camera) and TCP
  keepalive on both the client and the relay socket, so a peer that disappears
  ends the session instead of holding it open.
- **No bound on concurrent sessions.** A client reconnecting faster than its
  sessions ended could open relay sessions without limit. A camera now accepts
  at most eight at once and refuses the rest with a log line naming the cap.

### Changed

- The Web UI says "N stream connections" instead of "N watching". The number
  always counted the media server's connections to the bridge, never people:
  browsers talk to the media server, and one viewer can account for several
  connections.

## 0.10.7

### Changed

- Publish only the model-neutral Motorola Nursery Homeassistant Bridge GHCR
  packages; VM65 compatibility package tags are no longer created.

## 0.10.6

### Fixed

- Fixed release validation for annotated tags after GitHub Actions checks out
  the tag target commit.

## 0.10.5

### Changed

- The Home Assistant add-on now pulls the model-neutral published GHCR image
  instead of building locally during installation.

## 0.10.4

### Changed

- Renamed the public product to Motorola Nursery Homeassistant Bridge while
  retaining all VM65 integration identifiers for existing installations.
- Published model-neutral GHCR image names alongside the VM65 compatibility
  package aliases.

## 0.10.3

### Fixed

- Lowercase the GitHub Container Registry owner before tagging the published
  add-on images, as GHCR rejects mixed-case repository names.

## 0.10.2

### Fixed

- Republished the add-on release after moving its build source to the fixed
  release branch, so local Home Assistant builds include the entrypoint fix.

## 0.10.1

### Fixed

- The add-on AppArmor profile permitted `/run.sh` to be read but not executed,
  causing S6 to exit immediately with `Permission denied` during startup.

## 0.10.0

A review of the add-on itself — its packaging, its entrypoint and the options
that reach it. The findings and the reasoning behind each fix are in
[docs/review-2026-08-29.md](https://github.com/Paradox101/motorola-babycam-ha-bridge/blob/main/docs/review-2026-08-29.md).

### Fixed

- **Still images and temperature needed MQTT to work at all.** Both were wired
  to the `mqtt_discovery` switch, which is off by default, so in a standard
  installation every camera card asked for a thumbnail that answered 404, "Save
  still" downloaded that 404, and no camera was ever asked for its temperature.
  The Web UI serves its own stills and reads temperature either way now; MQTT
  only decides whether an address for them is published to Home Assistant.
- **The MJPEG fallback could not deliver a picture through Ingress.** It is a
  response that never ends, and the Supervisor buffers ingress responses unless
  the add-on asks it not to. Since MJPEG is the transport that matters exactly
  where WebRTC cannot work — Nabu Casa, a reverse proxy — the last resort was
  missing when it was needed. The add-on now declares `ingress_stream`.
- **A credential refresh could silently break every stream.** The camera access
  token lives in the generated media server configuration, and go2rtc reads
  that file only when it starts. A refresh that rotated the session left go2rtc
  presenting a token the camera no longer accepts, until someone restarted the
  add-on. The media server is now restarted when — and only when — that file
  actually changed.
- **The advertised RTSP port was hard-coded.** Remapping container port 8555 on
  the host produced an RTSP URL in Home Assistant that nothing answered on, with
  no option to correct it.
- **A media server that died took the whole add-on with it**, including a full
  credential round-trip on the way back up, with every camera dark until it
  finished. go2rtc is now restarted on its own with a backoff, the way each
  camera bridge already was.
- **A TLS broker was dialled as if it were plain TCP.** Home Assistant can
  offer a broker on port 8883; the connection then never completed and the
  entities simply never appeared.
- **The add-on did not know its own version**, reporting `devel+<commit>` in the
  Web UI, in the MQTT device and in every bug report.
- A snapshot larger than the 8 MiB cap was truncated and then served as a valid
  picture for the whole cache window.
- A restart request whose content type carried a charset was refused.

### Added

- **Two repairs in the Web UI's diagnostics panel**: restart the media server,
  and refresh credentials now instead of at the next scheduled refresh.
- **A warning when `stream_host` is not an address that reaches this machine.**
  The page compares it with the address your browser actually used, which is
  the first thing to check when live video keeps falling back to MSE or a
  copied RTSP URL resolves nowhere.
- `rtsp_port` and `webrtc_port` options, which mean the same thing in both
  streaming modes. `external_stream_port` meant one in bundled mode and the
  other in external mode; it is deprecated but still wins where it is set, so
  existing configurations keep working.
- `mqtt_tls`, detected automatically for the broker Home Assistant provides.
- Names and descriptions for every option on the configuration page, and this
  changelog on the add-on's own Changelog tab.

### Changed

- The add-on now starts with Home Assistant by default.
- The base image moved from Alpine 3.19, which is end of life, to a pinned
  current one. The ffmpeg installed on top of it is what parses camera data.
- Only the still images each camera actually publishes are fetched at startup.
  Warming the whole allowed stream list opened two relay tunnels per camera
  where one was needed.
- The image the Supervisor builds is now built in CI as well, on every change.

## 0.9.2

Live video now has a working transport, after 0.9.1's on-card reasons turned
out to name three separate faults — one per transport: MSE was refused with a
403 because of an `Origin` check, MJPEG asked for a transcode go2rtc will not
do, and every transport was timed out before the relay tunnel could deliver a
keyframe.

## 0.9.1

The camera picture was covered by a stray badge, WebRTC advertised an address
nothing could reach, and a failed MJPEG attempt left a broken-image icon behind.

## 0.9.0

Live video that plays everywhere: three transports tried in order, with a badge
saying which one is carrying the picture and why the others were refused.

## 0.8.0

Cameras add themselves to Home Assistant, and the add-on got its own camera page
in place of the media server's debugging interface.

## 0.7.0

Pairing moved into the Web UI: no more filling in a code as a configuration
option and restarting twice.

Older entries are in the repository's
[CHANGELOG.md](https://github.com/Paradox101/motorola-babycam-ha-bridge/blob/main/CHANGELOG.md).
