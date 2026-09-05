# Changelog

## Unreleased

### Fixed

- **A camera added to the account stayed invisible in the add-on until it was
  restarted.** A credential refresh hands the bridge a new camera list over
  SIGHUP, and the relay bridges were swapped in place correctly — but the Web
  UI, the still-image endpoint and the media proxy each held the camera list
  they were built with at start. The new camera therefore answered 404 for its
  thumbnail, "unknown stream" for its video and had no URL on the page. All
  three now follow the reload, and a camera that left the account stops being
  served just as promptly.
- **The go2rtc endpoints that carry the camera's RTSP password could be reached
  by writing their path differently.** The block list compared the request path
  as it arrived, so `//api/config` and `/api/./config` missed it by a byte while
  go2rtc resolved both to the configuration endpoint. Paths are cleaned before
  they are matched, and `/api/streams` — which reports the source URL of every
  active stream, access token included — is blocked as well. The player never
  asks for it: it names the stream it wants on the media endpoints directly.
- **A listener that could not bind was reported only as a log line**, after the
  line that said it was listening, while the add-on carried on with an
  unreachable Web UI and snapshot endpoint. A port that is already in use now
  fails the start, which is what the port conflict is.
- **A replaced temperature worker could overwrite the state of its
  replacement.** Cancelling a worker does not stop it — a read waiting on the
  camera runs to its own deadline first — so after a credential refresh the old
  worker's final "unavailable" landed on the camera its successor was already
  polling. Only the worker that still owns a camera reports its state.
- **Two cameras could end up sharing one stream name.** Deduplication counted
  per base name, so cameras named "Nursery", "Nursery" and "Nursery 2" produced
  `nursery`, `nursery-2` and `nursery-2` — two cameras on one go2rtc stream, one
  snapshot URL and one card on the page. Names are now checked against the ones
  already handed out.
- **Every relay session leaked a context.** The dial budget replaced a
  cancellable context without cancelling it, and each abandoned one stayed
  attached to the camera's long-lived context for the life of the process.
- One camera whose discovery URL could not be built no longer stops the others
  from being published, nor skips retiring the cameras that left the registry;
  and a discovery failure no longer stops the bridge from starting, since the
  cameras themselves are fine.
- The MQTT service no longer keeps temperature state for cameras it does not
  know about, so a removed camera leaves nothing behind.

### Added

- Tests for the temperature supervisor, which had none: capability discovery,
  polling, the cameras it must skip, and the replaced-worker case above.

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
[docs/review-2026-08-29.md](docs/review-2026-08-29.md).

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

Live video now has a working transport. The reasons 0.9.1 printed on the card
turned out to name three separate faults, one per transport.

### Fixed

- **MSE was refused with a 403 on every attempt.** go2rtc rejects a WebSocket
  whose `Origin` is not its own host, and through Ingress the browser's origin
  is Home Assistant, so the add-on log read
  `websocket: request origin not allowed by Upgrader.CheckOrigin`. The proxy now
  presents the upstream's own origin. The request has already passed this
  add-on's authentication and its stream check by then, which is exactly what
  Origin would have been guarding.
- **MJPEG could never have worked.** go2rtc does not transcode for a plain
  MJPEG request and answered
  `codecs not matched: video:H264 => video:JPEG`. Snapshots differ — `frame.jpeg`
  does fall back to ffmpeg — which is why stills worked while the last-resort
  video transport did not. Each camera now gets a companion stream that
  transcodes, and the player asks for that one.
- **Every transport was timed out too early.** The relay tunnel has to open and
  a keyframe has to arrive before anything can appear, and six seconds did not
  cover it. WebRTC now gets nine, MSE twelve and MJPEG twenty.

## 0.9.1

### Fixed

- **A giant orange oval covered every camera picture.** The rule meant to make
  the video fill its frame was written as `.frame > *`, so it also stretched the
  status badge — a pill with a 999-pixel corner radius — across the whole card.
  Only the video and the images fill the frame now.
- **WebRTC could never deliver a picture.** Inside a container go2rtc advertises
  only its own Docker address, which nothing on the network can reach, so the
  connection negotiated successfully and then carried no packets. The generated
  configuration now advertises `stream_host` and the WebRTC port as a candidate.
- A failed MJPEG attempt left a broken-image icon sitting over the still that
  was perfectly good.

### Added

- **Each transport now says why it gave up**, on the card itself: a refused
  websocket, a codec the browser will not take, a signalling error with its
  status code, or a timeout with its budget. "Unavailable" on its own is not
  something anyone can act on.
- go2rtc's `{"type":"error"}` websocket reply is read and shown instead of
  being waited out until the timeout.
- Requests go2rtc refuses are logged by the add-on with the path, query and
  status, so a player problem can be diagnosed from the add-on log rather than
  from a phone screen.
- MJPEG gets a longer budget than the other two, because starting an H264
  transcode from cold is not quick.

## 0.9.0

Live video that actually plays, and a Web UI worth opening.

### Fixed

- **Live video did not work outside the local network.** The page spoke only
  WebRTC, and WebRTC media never passes through Ingress — the browser reaches
  the host's UDP port directly. Over Nabu Casa or a reverse proxy there was no
  working transport at all, and the button simply failed. The player now falls
  through three of them: WebRTC (under a second, local network), MSE over the
  proxied WebSocket (about a second, anywhere the page loads), and MJPEG (no
  sound, but nothing can block it). A badge says which one is carrying the
  picture, so lag has an explanation instead of being a mystery.
- Each attempt is timed out rather than trusted to fail loudly. A transport that
  negotiates happily and then delivers no frames — exactly what WebRTC does when
  its UDP port is unreachable — now falls through instead of hanging on a black
  rectangle.

### Added

- Cameras start playing when the page opens, and stop when the tab is hidden, so
  a forgotten tab does not keep pulling video over the relay.
- Sound per camera, fullscreen, saving a still, and click-to-enlarge with Escape
  to go back.
- A diagnostics panel: uptime, bridge restarts, active sessions, cameras
  serving, and the media and broker links.

## 0.8.0

Cameras now appear in Home Assistant by themselves, and the Web UI is the
add-on's own camera page instead of go2rtc's debugging interface.

### Added

- **A camera entity per camera, through MQTT Discovery.** No Generic Camera
  integration to add by hand for a working camera in Home Assistant. Home
  Assistant's camera platform reads image bytes from a topic — it has no stream
  URL at all — so the add-on feeds it a still every
  `camera_refresh_interval` seconds. Entities work with dashboard tiles,
  `camera.snapshot` and automations.
- Add-on option `camera_refresh_interval`, in seconds, default 60, range 5–3600.
  `0` publishes no frames and creates no camera entity: every refresh makes the
  media server pull a frame over the relay, so the cost is the user's to choose.
- **The add-on's own Web UI.** Per camera: a still, live WebRTC video on demand,
  whether the relay tunnel is up, how many people are watching, and the
  temperature when the camera reports one. Each card copies the RTSP URL and can
  restart that one camera's bridge — a tunnel that went bad recovers from it
  while every other camera keeps streaming.

### Changed

- go2rtc's own page and API are no longer served through Ingress. go2rtc stays
  behind the new page and is reached only for the media endpoints the player
  needs, still through the proxy that refuses anything but a read of a
  configured stream. The page loads nothing from the network, like the pairing
  page.
- Live video no longer requires the Generic Camera integration to watch at all;
  it is still the way to put a live stream on a Home Assistant dashboard, and
  the Web UI has a copy button for the URL it wants.

## 0.7.0

Pairing moves into the add-on Web UI. No configuration, no second restart, and
no add-on that has to crash to tell you what to do next.

### Added

- **A pairing page behind Ingress.** Start the add-on, click **Open Web UI**,
  enter the Motorola account address, then the code that arrives by email. The
  cameras start straight away. The page is served by the add-on itself, renders
  from inlined markup under `default-src 'none'`, and sits behind the same gate
  as the rest of the Web UI: the Supervisor's authenticated Home Assistant user,
  on the Supervisor network, or nothing.
- **Send a new code**, for when one expires before it is used.
- While pairing is pending the add-on serves its health endpoint, so the
  Supervisor watchdog does not restart it out from under whoever is reading
  their email.

### Fixed

- **An expired code was a dead end.** A stored challenge was never replaced, so
  a code that expired before it was entered left every subsequent start retrying
  that same dead code. The only way out was deleting
  `/data/5gencare-session.json`, which needs SSH or the Samba add-on. Challenges
  now expire after 15 minutes and are replaced rather than retried; a *wrong*
  code still keeps the challenge, so a typo costs a retry and not a new email.
- The paired email address is remembered in the add-on's own state, so clearing
  the `email` option no longer loses the account it paired with.

### Changed

- The add-on no longer emails a code just because it restarted; a code is sent
  when someone asks for one. `vm65-setup` keeps the old behaviour by default
  and takes `-request-code=false`, `-pair-ui` and `-status`.
- `email` and `otp_code` remain supported for unattended setup and are no longer
  needed for a normal install.

## 0.6.0

Security review against Home Assistant's
[app security guidance](https://developers.home-assistant.io/docs/apps/security/),
plus the snapshot fix that review turned up. Existing add-on options stay valid.

**Upgrade note.** The go2rtc API is no longer published on the host. If you
pointed something at `http://<host>:1984` yourself, use the Web UI through
Ingress instead. RTSP (`8555`) and WebRTC (`8556`) are unchanged, so cameras
already added through the Generic Camera integration keep working.

### Fixed

- **Camera snapshots returned 500.** Two causes, both fixed. The add-on image
  had no `ffmpeg`, which go2rtc needs to turn an H264 keyframe into a JPEG, so
  every still-image request failed at the source. And Home Assistant abandons an
  image fetch after ten seconds, which a cold camera cannot meet: the relay
  tunnel, the camera stream and the transcode all have to start first. The
  bridge now serves the still image itself — one fetch per camera at a time,
  continuing after the requester gave up, with a recent frame answering
  immediately and a slightly stale frame preferred over an error.
- **The snapshot URL depended on `stream_host` resolving from Home Assistant**
  and on a published host port. It is now fetched from the add-on by its
  Supervisor hostname, which always resolves and needs no port mapping.

### Security

- **The Web UI is authenticated.** Ingress pointed straight at go2rtc, which has
  no authentication of its own, and go2rtc's port was published on the host as
  well. Anyone on the local network could therefore read `/api/config` — the
  camera access token and RTSP password are in it — and could make go2rtc build
  a stream from any source, `exec:` included, which is command execution inside
  the container. Ingress now points at the bridge, go2rtc binds container
  loopback, and a request only reaches it when it comes from the Supervisor
  network, carries the `X-Remote-User-Id` header the Supervisor attaches to an
  authenticated Ingress session, is a read or a WebRTC/MSE negotiation, and
  names a stream this add-on configured.
- **Only the media ports are published.** The Web UI, snapshots and the health
  endpoint travel over the internal Supervisor network; the watchdog polls the
  container port directly. Both listeners also refuse peers outside that network
  themselves.
- **The published snapshot URL carries a token**, persisted in
  `/data/snapshot-token` so a restart does not break a URL Home Assistant
  already holds. It is never logged.
- An **AppArmor profile** ships with the add-on, and the sidebar panel is
  administrator-only.
- `tools/ci/check_addon.py` now fails the build on a published go2rtc API port,
  a missing AppArmor profile, a missing `ffmpeg`, a watchdog that depends on a
  host port mapping, or any Supervisor API role or host privilege being
  requested.

### Changed

- `ingress_port` is `8099`, served by the bridge rather than by go2rtc.
- `-snapshot-url-base` now names the bridge's own public base URL rather than a
  go2rtc address; `-ingress`, `-ingress-trusted-cidr` and `-snapshot-token-file`
  are new.

## 0.5.2

### Fixed

- Add-on builds now invalidate the cached source-clone layer whenever add-on
  metadata changes. This prevents a new add-on version from running stale
  bridge binaries that do not recognize newly added command-line options.

## 0.5.1

### Added

- A dedicated Home Assistant add-on icon for Motorola Nursery Bridge.

## 0.5.0

### Added

- Home Assistant MQTT temperature sensors for compatible Motorola Nursery
  cameras. The bridge reads the camera's `temperature_reading` capability over
  Motorola's authenticated TLS control channel and publishes Celsius values
  with dedicated availability tracking.
- Add-on option `temperature_poll_interval`, in seconds, with a default of 30
  and supported range of 10 through 3600 seconds.

## 0.4.0

Bug-fix release from a full review of the 0.3.0 tree. Existing add-on options
stay valid; no configuration change is required to upgrade.

### Fixed

- **The relay handshake could block forever.** `magic.Dial` applied the context
  only to the TCP connects, so a relay that accepted the connection and then
  went silent hung the dial indefinitely: `DialTimeout` never applied, the retry
  never ran, and each stuck client leaked a goroutine and two sockets. The whole
  opening sequence now runs under one deadline and aborts on cancellation.
- **The same fault in the 5GenCare client.** `Client.Timeout` covered only the
  TLS connect, not the response read. Because credentials are fetched before the
  bridge starts, a silent host left the add-on "starting" forever with no log
  line and no health port. Every exchange now has a deadline.
- **MQTT Discovery never created a camera entity.** The payload was published as
  an MQTT `camera` with `stream_source`, but that platform requires an image
  `topic` and has no stream URL key, so Home Assistant rejected every payload.
  Cameras are now published as `image` entities fed by a snapshot URL.
- **The Web UI link did not work off the local network.** `webui` built a URL
  from the host port, which is unreachable over a domain name, reverse proxy or
  Nabu Casa. The Web UI is served through Home Assistant Ingress instead.
- **A failed camera bridge was never restarted** and the watchdog could not see
  it: `/healthz` only reported a flag set once at startup. Bridges now restart
  with exponential backoff, and `/healthz` fails when no bridge is serving.
- **`/status` reported zeros** for `active_sessions` and `reconnects_total`.
- **The credential refresh dropped every live stream** every six hours, whether
  or not anything had changed.
- **The add-on built from the pre-rename repository URL**, which worked only
  through GitHub's rename redirect.
- **`make check` always failed** on a clean tree.
- **The repository policy check always exited 1**, so CI had been red on `main`
  since 28 August: `git grep` exits 1 when it matches nothing, and that code
  became the script's exit code even after it printed that the policy passed.
- **The container build never verified the committed checksums**, because only
  `go.mod` was copied before `go mod download`.

### Added

- Home Assistant diagnostics over MQTT: bridge connectivity, active sessions,
  bridge restarts, and a per-camera link sensor, grouped under a bridge device
  with its software version.
- Per-camera availability, so one failed camera shows as unavailable while the
  others keep working.
- Camera snapshot images in bundled mode.
- MQTT broker settings taken from Home Assistant when Mosquitto is installed.
- Credential refresh in place via `SIGHUP`, leaving unchanged cameras streaming.
- `-version` on both commands, and the build version in logs and `/status`.
- `govulncheck` and a `go mod tidy` drift check in CI.

### Changed

- The add-on no longer requests the unused `share:rw` mapping.
- Live video is added once through the Generic Camera integration; Home
  Assistant has no MQTT Discovery path for an RTSP stream. The add-on logs the
  exact URLs at startup.
- The add-on builds from a `release` branch instead of the version tag. A tag
  cannot exist before the commit that names it, so pinning to one broke every
  add-on build and update between the release commit and the tag push. The
  branch always exists, `SOURCE_REF` is no longer bumped per version, and
  publishing to Home Assistant no longer depends on tagging. The build also
  explains itself now instead of failing with a bare "Remote branch not found".

## 0.3.0

Multi-camera support, Home Assistant add-on, MQTT Discovery and health
endpoints. See the repository history for details.
