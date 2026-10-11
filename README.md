# Bairelay

[![codecov](https://codecov.io/github/mgc8/bairelay/graph/badge.svg?token=BUUBTI4K5E)](https://codecov.io/github/mgc8/bairelay)

A pure-Rust bridge between Reolink **battery** cameras (proprietary "Baichuan" protocol on TCP/9000) and standard RTSP / MQTT, with a local replacement for Reolink's wake-up cloud and motion-push channel.

## Acknowledgements

Bairelay stands on the shoulders of [Neolink](https://github.com/QuantumEntangledAndy/neolink), the project that originally reverse-engineered the Baichuan protocol and proved that a local-only alternative to Reolink's cloud was possible. Enormous thanks to:

- **[thirtythreeforty](https://github.com/thirtythreeforty/neolink)** — original author, who did the bulk of the protocol reverse-engineering and shipped the first working RTSP bridge.
- **[QuantumEntangledAndy](https://github.com/QuantumEntangledAndy/neolink)** — long-term maintainer of the active fork, who added MQTT, motion detection, paused streams, and the deeper battery-camera support that bairelay builds on.

Without their multi-year work this project would not exist. The vendored `bairelay_neolink_core` crate is a modernised descendant of their codebase under the same AGPL-3.0 license.

This project's development made use of **AI agentic coding**, especially for test generation, packet capture analysis, and reverse-engineering tasks against pcap dumps of camera ↔ cloud traffic. The protocol work itself is grounded in public reverse-engineering of cameras the operator owns; no Reolink code, firmware, or proprietary documentation was used.

---

## What is Bairelay?

Bairelay is a small daemon that acts as a proxy between Reolink IP cameras and standard RTSP / MQTT clients. Some Reolink cameras — the **battery line** in particular — do not implement ONVIF or RTSP and instead use a proprietary "Baichuan" protocol on TCP/9000, only compatible with Reolink's own apps and NVRs. Bairelay lets you use NVR software (Frigate-adjacent setups, Shinobi, Blue Iris, Synology Surveillance Station) and Home Assistant to consume video from these cameras directly. The Reolink NVR is not required, and the cameras are unmodified.

Your client connects to bairelay; bairelay maintains the Baichuan session with the camera and re-packages the H.264 / H.265 video and AAC / G.711 audio as RTP-over-RTSP (or RTSPS).

### Supported cameras

Bairelay's exclusive design target is **Reolink battery cameras** — the Argus 3 / Eco / 4, Argus PT, Argus Track, Argus Altas, etc. These are the cameras that:

1. Speak Baichuan only (no native RTSP).
2. Sleep aggressively to preserve battery and need to be woken on demand.
3. Push motion alerts via a cloud channel rather than a LAN broadcast.

Non-battery Reolink cameras (the wired RLC line, doorbells, NVR-attached models) almost universally support **native RTSP** out of the box — there is no need for a translation layer, so they are explicitly out of scope. Doorbells in particular have additional protocol surfaces (two-way audio, button events) that bairelay does not implement.

Always-on Argus models should work incidentally — the wake-lock machinery is harmless when the camera never sleeps.

---

## Why bairelay exists

Bairelay is best thought of as a **continuation of Neolink** rather than a fork. Neolink's use case and bairelay's overlap, but bairelay's tighter scope (battery cameras only) lets it commit to a few decisions Neolink can't:

- **Pure Rust, no GStreamer.** Neolink shells out to GStreamer for RTSP serving and codec handling. Bairelay implements the RTSP server, the H.264 / H.265 / AAC / G.711 RTP packetisers, the SDP generator, and the ADPCM → G.711 transcoder in-process. There is no native dependency to install — `cargo build` produces a single static binary.
- **Stable, jitter-free streaming.** The camera delivers video in bursty GOPs (a ~900 ms burst, then ~1.1 s of silence). Bairelay's video pacer holds a 1.5 s pre-buffer and emits at the codec's natural cadence, so receivers never see underruns. The audio pacer does the same for AAC / G.711, eliminating the periodic A-V hitches you'd otherwise see every few seconds.
- **First-class Home Assistant integration.** Retained MQTT discovery payloads for camera, motion, floodlight, LED, IR, PTZ, battery, and siren entities; preview JPEGs with state overlays (`SLEEPING`, `CONNECTING`); the HA-specific quirks (HEVC parameter-set stripping for go2rtc compatibility, ffmpeg DTS handling) are baked in.
- **Fully local operation.** Bairelay ships its own UDP wake server and its own TCP push listener that — with a small DNS redirect on the operator's LAN — completely replace Reolink's `p2p*.reolink.com` and `pushx.reolink.com` cloud surfaces. Cameras can be firewalled off the public internet and still work end-to-end.

If you are running wired Reolink cameras with native RTSP, bairelay has nothing to offer you. If you are running Argus battery cameras and tired of fighting GStreamer, sleeping cameras, and Reolink's cloud, this is the tool.

---

## Release binaries

Pre-built binaries are attached to each [GitHub Release](https://github.com/mgc8/bairelay/releases). Targets in the v1 release matrix:

| Archive suffix              | Format    | Notes                            |
|-----------------------------|-----------|----------------------------------|
| `x86_64-linux`              | `.tar.gz` | static musl, glibc-independent   |
| `aarch64-linux`             | `.tar.gz` | Pi 4/5, aarch64 NAS              |
| `aarch64-apple-darwin`      | `.tar.gz` | Apple Silicon (M-series)         |
| `x86_64-pc-windows-msvc`    | `.zip`    |                                  |

Releases are not codesigned. macOS / Windows will refuse to run the binary on first launch; the one-time unblock is below.

### Linux

```bash
tar -xzf bairelay-vX.Y.Z-x86_64-linux.tar.gz
cd bairelay-vX.Y.Z-x86_64-linux
./bairelay --help
```

Static musl binaries — run on any modern distribution (glibc or musl), no system dependencies. Move `bairelay` to `~/bin`, `/usr/local/bin`, or wherever your service manager expects it.

### macOS

```bash
tar -xzf bairelay-vX.Y.Z-aarch64-apple-darwin.tar.gz
cd bairelay-vX.Y.Z-aarch64-apple-darwin
xattr -d com.apple.quarantine bairelay
./bairelay --help
```

The `xattr -d` line clears the quarantine flag macOS adds to downloaded files; without it Gatekeeper blocks the unsigned binary with "cannot be opened because the developer cannot be verified". Apple Silicon (M-series) only — Intel Macs need to build from source.

### Windows

Extract the `.zip` and launch `bairelay.exe` from PowerShell or `cmd`. On first launch SmartScreen blocks the unsigned executable — click **More info** → **Run anyway**. Alternatively: right-click `bairelay.exe` → Properties → tick **Unblock** before launching.

Running as a Windows service is not packaged; use Task Scheduler or run interactively.

---

## Build and install

### Prerequisites

- A current stable Rust toolchain (1.93+). Distro-packaged rustc is often too old for the workspace tests; install via [rustup](https://rustup.rs/) for the smoothest path.
- A C toolchain for `aws-lc-rs` (the rustls crypto provider) — `build-essential` on Debian / Ubuntu, Xcode Command Line Tools on macOS, MSVC build tools on Windows.
- That's it. No GStreamer. No OpenSSL. No system MQTT library.

### Build from source

```bash
git clone https://github.com/mgc8/bairelay
cd bairelay
cargo build --release
```

The binary lands at `target/release/bairelay`. Copy it wherever you run services from (`~/bin`, `/usr/local/bin`, a systemd unit's `ExecStart`, a Docker image — your choice).

### Install as a Home Assistant App

On a Home Assistant OS or Supervised installation (HA 2026.2+ — add-ons were renamed to apps):

1. Settings → Apps → **Install app** (bottom-right) → ⋮ (top-right) → Repositories.
2. Paste `https://github.com/mgc8/bairelay` → Add.
3. Install the **Bairelay** app that appears in the list.
4. Configuration tab → set `topic_prefix` (default `bairelay`) and add one entry per camera (`name`, one of `address` or `uid`, `username`, `password`).
5. Start. Advanced settings (TLS, wake server, push listener, per-camera floodlight / PIR / pause) live in a `/config/bairelay/config.toml` overlay — see the app's Documentation tab.

The HA MQTT integration is auto-discovered via `services: ["mqtt:want"]`. The app is published per-arch at `ghcr.io/mgc8/bairelay-hassio-{amd64,aarch64}` on every release; HA Container / Core installations without the Supervisor must use the cargo-build path above.

### Run

```bash
bairelay -c config.toml mqtt-rtsp      # MQTT bridge + RTSP server (typical)
bairelay -c config.toml rtsp           # RTSP server only
bairelay -c config.toml mqtt           # MQTT bridge only
```

Logs go to stderr; redirect or capture via your service manager. `RUST_LOG=bairelay=debug` or `-v / -vv / -vvv` raises verbosity.

### Platform support

Bairelay's primary targets are Linux and macOS — both are exercised by CI. Windows is a secondary target: the workspace builds and the architecture is platform-agnostic, but no CI runner tests it, so use at your own risk. The wake-server and push-listener features bind privileged-ish ports (UDP 9999, UDP 58200, TCP 443 for push) — on Linux you'll either run as root, grant `CAP_NET_BIND_SERVICE`, or rebind the push listener to a non-privileged port and have your firewall NAT 443 to it.

---

## Configuration

Bairelay's config file is TOML. Start from `sample_config.toml` in the repo root — every defaultable option is shown commented out with its default value.

### Migrating from Neolink

Bairelay's config is **a deliberate near-superset of Neolink's** so existing configs mostly drop in. To migrate:

1. **Set the topic prefix.** Neolink publishes under `neolink/`; bairelay defaults to `bairelay/`. To keep your existing HA automations and topic subscriptions working, add this to the `[mqtt]` section:

   ```toml
   [mqtt]
   topic_prefix = "neolink"
   ```

   You can also rename it to anything else later — `[A-Za-z0-9_-]+`.

2. **Battery cameras only.** Bairelay drops support for non-battery models. Remove any `[[cameras]]` blocks for wired cameras and use Reolink's native RTSP for those.

3. **`[cameras.pause]` semantics simplified.** Neolink had `on_motion`, `on_client`, `on_disconnect`, `motion_timeout`, and `mode` knobs. Bairelay treats the battery lifecycle as a single concern: the camera is woken on demand (RTSP client connect, MQTT command, motion push) and sleeps after a grace period when nothing holds a wake lock. The old fields are still **accepted with a startup warning** so you don't have to edit configs urgently — they simply have no effect. The replacement knobs are:

   ```toml
   [[cameras]]
   idle_disconnect = true                   # opt-in for battery cams
   idle_disconnect_timeout_secs = 45        # grace before sleep (default 45 s)
   ```

   `pause.timeout` is still honoured as a soft alias for `idle_disconnect_timeout_secs` if you'd rather not edit. Be aware: bairelay enforces `idle_disconnect_timeout_secs > stream_prune_grace_secs` (default 30 s) so a cached RTSP source can't outlive its Baichuan session.

4. **`update_time`, `debug`, `print_format` are gone.** `update_time` is replaced by the explicit `bairelay set-time <camera>` one-shot command. `debug` and `print_format` are replaced by `RUST_LOG`.

5. **Strict config parsing.** Bairelay's config structs use `#[serde(deny_unknown_fields)]` — typos in keys will cause startup to fail loudly with a pointer to the bad key. This is intentional; silent drops are how stale configs survive.

6. **MQTT discovery is now top-level.** Neolink puts `[cameras.mqtt.discovery]` per camera; bairelay puts `[mqtt.discovery]` at the top level and emits per-camera discovery payloads automatically:

   ```toml
   [mqtt.discovery]
   topic = "homeassistant"
   features = ["floodlight", "camera", "motion", "led", "ir", "reboot", "pt", "battery", "siren"]
   ```

7. **Talk (two-way audio) is not implemented.** If you used `neolink talk`, bairelay has no equivalent yet — see the future-development notes.

### From-scratch config

Minimum viable config for a single camera with MQTT + RTSP:

```toml
bind = "0.0.0.0"

[mqtt]
broker_addr = "192.168.1.10"
port = 1883
credentials = ["mqtt-user", "mqtt-password"]
topic_prefix = "bairelay"

[mqtt.discovery]
topic = "homeassistant"
features = ["floodlight", "camera", "motion", "motion_class", "push", "led", "ir", "reboot", "pt", "battery", "siren"]

[[cameras]]
name = "driveway"
username = "admin"
password = "camera-password"
uid = "ABCDEF0123456789"
idle_disconnect = true
```

Key recommended defaults and why:

- **`bind = "0.0.0.0"`** — listen on all interfaces. The advertise IP for the wake server is computed per peer at runtime, so you don't have to nail the LAN address.
- **`bind_port = 8554`** — RTSP standard. Plain RTSP. If you set `certificate`, TLS RTSP gets a parallel listener on `tls_bind_port = 8555`.
- **`idle_disconnect = true`** — required for battery cams to sleep. Without it, bairelay holds the Baichuan session open forever and the camera will never enter low-power state.
- **`idle_disconnect_timeout_secs = 45`** — grace period after the last RTSP client disconnects before the camera is allowed to sleep. Lower means faster sleep but more wake-up latency on the next connect; higher is the opposite. 30–60 s is the practical band.
- **`gap_threshold_secs = 1.0`** (per-camera, in `[cameras.pause]`) — the placeholder/bridging trigger when the upstream stalls. Sub streams with a naturally low fps may need this raised. Default is fine for main streams.
- **MQTT discovery `features`** — the listed nine cover every entity HA understands. Drop `pt` for non-PTZ cameras, drop `floodlight` / `siren` for models without that hardware. Bairelay won't emit a discovery payload for a feature the camera doesn't advertise capability for; the list is an opt-in cap.

### Camera names and how they appear in MQTT / Home Assistant

Camera names (`name = "..."` in `[[cameras]]`) are restricted to `[A-Za-z0-9_-]+` so they're safe in MQTT topics, HA unique IDs, and URL paths. The name flows into three different places, with one transform along the way:

- **MQTT topic paths** — verbatim. `name = "front_door"` publishes to `bairelay/front_door/status/...`; `name = "Front-Door"` publishes to `bairelay/Front-Door/status/...`. Topics are case-sensitive.
- **HA `unique_id` / device identifier** — verbatim, joined with the topic prefix and a per-entity suffix using `_`. `name = "front_door"` produces `bairelay_front_door_floodlight` etc. These are stable across renames in the HA UI; never edit the camera name once Home Assistant has discovered the device or you'll orphan the existing entity history.
- **HA display labels** (the friendly name shown on dashboards) — title-cased. `_` and `-` are treated as word separators and replaced with a space; the first character of each word is uppercased; **embedded caps are preserved** so an operator-chosen casing is not flattened. Examples:

  | Camera name   | HA display label   |
  |---------------|--------------------|
  | `front_door`  | `Front Door`       |
  | `front-door`  | `Front Door`       |
  | `MyCamera`    | `MyCamera`         |
  | `4K_Terrace`  | `4K Terrace`       |
  | `IPCam-front` | `IPCam Front`      |
  | `frontdoor`   | `Frontdoor`        |

  Per-feature entities append the feature label with a space: `Front Door Floodlight`, `Front Door Floodlight Tasks`, `Front Door Battery`, `Front Door PIR`, etc.

If you've already renamed an entity in the HA UI, your override wins — bairelay's `name` field is only used the first time HA sees the discovery payload.

### TLS (RTSPS)

Set `certificate = "/path/to/fullchain-and-key.pem"` at the top level to enable parallel RTSPS on `tls_bind_port` (default 8555). Plain RTSP keeps running on `bind_port`; set `bind_port = 0` for TLS-only. `tls_client_auth = "none" | "request" | "require"` gates client-cert mTLS, with `tls_client_ca` pointing at the CA bundle.

`tests/scripts/gen-test-certs.sh` produces a self-signed CA + leaf for local testing.

### TLS to the MQTT broker

Set `tls = true` in `[mqtt]` to connect over TLS (port is usually 8883). By default the broker's certificate is verified against the operating system's CA store, which suits a broker with a publicly-signed certificate. For a private CA, set `ca` to its certificate instead (`ca` on its own also turns TLS on; `tls = false` next to `ca` is rejected). For mutual TLS add `client_auth = ["client.pem", "client.key"]`. `ca` and `client_auth` each take a file path (as in Neolink) or the PEM text itself inline. All of it is loaded and checked at startup and by `bairelay check-config`, so a wrong path, a bad certificate or a certificate/key mismatch fails immediately rather than at every broker connect.

### Security defaults you should know about

Bairelay assumes a **trusted LAN** as its deployment context. That assumption shows up in a few places worth calling out:

- **RTSP authentication is opt-in.** Without a `[[users]]` block, the RTSP server accepts anonymous connections. Add a `[[users]]` with name + pass (and optional per-camera `permitted_users` allow-list) to require Digest / Basic auth. Digest enforces a 5-minute nonce TTL and binds the digest URI to the request line per RFC 7616; Basic is offered alongside on plain transport for drop-in compatibility with permissive clients but is, of course, plaintext.
- **Push listener trusts source IP.** Any TCP connection from a known camera's IP (wake-server registry or configured `address`) is treated as motion. This is correct on a trusted LAN behind NAT, and the listener refuses to start when it has no way to match a camera — `[wake_server]` disabled and no camera with an IP `address`.

### Cloud ("account device") cameras

A camera added to a Reolink account stops accepting local-password logins and demands Reolink's cloud-signed **sigV3** login. Bairelay supports this, but it's **discouraged**: it makes a local-only bridge depend on Reolink's cloud (internet needed at every connect, a short-lived token Reolink can revoke, no fallback if the API changes). **Prefer unbinding the camera from the Reolink account** so it logs in with a normal local password.

If you can't unbind, put the account credentials once at the top level and mark each cloud camera `discovery = "cloud"` (no per-camera `password` needed — the cloud token authenticates it):

```toml
cloud_account  = "you@example.com"
cloud_password = "your-reolink-password"

[[cameras]]
name      = "backyard"
uid       = "ABCDEF0123456789"
username  = "admin"
discovery = "cloud"
```

If the login errors out with `8208 "the extra identification is required"`, run the one-time per-host authorisation:

```bash
bairelay cloud-authorise
```

This will prompt Reolink to send an authorisation code (emailed code by default; `--method totp` / `--method backup_code` also work) and afterwards will store a long-lived credential beside the config (`config-cloud-auth.json`) so the host connects headlessly afterwards.

More details can be found in the documentation under [`docs/cloud-account.md`](docs/cloud-account.md).

---

## Command-line usage

Bairelay has three **service modes** (long-running), several **camera commands** (one-shot), and one **utility command** (`check-config`):

```bash
bairelay --help
```

```text
bairelay 0.9.X — RTSP Relay for Reolink Baichuan cameras

Usage: bairelay [OPTIONS] <COMMAND>

Service modes:
  mqtt           Run the MQTT bridge only
  rtsp           Run the RTSP server only
  mqtt-rtsp      Run both the MQTT bridge and RTSP server

Camera commands:
  reboot         Reboot one camera
  snapshot       Capture a JPEG (or H.264/265 with --use-stream-raw) (alias: image)
  battery        Print battery status
  floodlight     Query or toggle the floodlight (held 30 s on set)
  pir            Query or set the PIR sensor
  status-light   Query or toggle the blue status LED
  ptz            Pan / tilt / zoom / preset control
  presets        List PTZ presets (shorthand for `ptz preset`)
  services       Query or configure camera network services (bare form: list all)
  users          List or manage camera user accounts
  set-time       Set the camera clock to the host's current local time
  version        Print firmware + model info
  siren          Trigger the siren once
  abilities      Dump the camera's abilityInfo XML + parsed permissions

Other:
  check-config    Validate the config file and exit (no camera connection)
  cloud-authorise Perform cloud login authorisation (MFA) for this host
  help            Print help for the given subcommand
```

Every camera command takes a camera name (matched against `[[cameras]]` in the config) and exits with a coarse exit code: 0 success, 1 generic, 2 usage, 3 config, 4 connection / auth, 5 protocol, 6 unsupported (`MissingAbility`), 130 interrupted (Ctrl+C / SIGTERM) — so shell scripts can branch without parsing stdout.

Add `--json` to any read command for machine-readable output; `-v` / `-vv` / `-vvv` raises `RUST_LOG` (info → debug → trace). `-c` / `--config` is global and accepted before or after the subcommand (`bairelay -c cfg.toml <cmd>` and `bairelay <cmd> -c cfg.toml` are equivalent).

`check-config` parses + validates the TOML, runs the same neolink-compat / `pause.timeout` shadowing / idle-vs-prune-grace warnings the daemon emits at startup, and exits 0 (OK), 2 (file missing), or 3 (parse / validate failure) without ever touching a camera. Use it in CI, Ansible pre-task hooks, or `systemctl` `ExecStartPre` to fail fast on bad configs.

### Examples

```bash
# Run the bridge:
bairelay -c /etc/bairelay/config.toml mqtt-rtsp

# Capture a JPEG snapshot:
bairelay snapshot driveway -f /tmp/driveway.jpg

# Raw H.264/H.265 first I-frame (decode with `ffmpeg -i - -vframes 1 out.jpg`):
bairelay snapshot driveway --use-stream-raw -f /tmp/driveway.h265

# Battery status:
bairelay battery driveway --json

# Reboot:
bairelay reboot driveway

# PTZ:
bairelay ptz driveway control 32 left            # nudge left, amount 32
bairelay ptz driveway preset                     # list presets
bairelay ptz driveway preset 0                   # go to preset 0
bairelay ptz driveway assign 1 "Front gate"      # save current pos as preset 1
bairelay ptz driveway zoom 0.5                   # zoom level (0.0–1.0)
bairelay presets driveway                        # shorthand for `ptz preset`

# Manage camera user accounts:
bairelay users driveway                          # list (default action)
bairelay users driveway add operator             # prompted for password on TTY
bairelay users driveway delete operator

# Sync the camera clock to host (carries local TZ offset):
bairelay set-time driveway

# Toggle the floodlight (held 30 s on `on`):
bairelay floodlight driveway on
bairelay floodlight driveway off

# Dump abilityInfo for `MissingAbility` debugging:
bairelay abilities driveway --json

# Wake a sleeping battery camera for 5 minutes via MQTT:
mosquitto_pub -h <broker> -u <user> -P <pw> \
  -t "bairelay/driveway/control/wakeup" -m 5

# Validate config without connecting to any camera:
bairelay -c /etc/bairelay/config.toml check-config
bairelay -c /etc/bairelay/config.toml check-config --json   # CI-friendly
```

The MQTT topic surface (status / control / query) is documented in `docs/architecture.md` and follows Neolink's shape closely; existing automations move over by changing only the `topic_prefix`.

---

## Local cloud replacements (experimental)

Bairelay can replace **two** Reolink cloud surfaces locally. Both are opt-in and both depend on the operator redirecting DNS for specific Reolink hostnames at the LAN resolver — bairelay does not modify the camera or its firmware. If your firmware updates change the protocol or hostnames, **expect breakage** until bairelay catches up. These features are inherently cat-and-mouse with Reolink and are explicitly experimental.

The full wire-level reverse-engineering is in `docs/cloud-interception.md` (Part I: wake server, Part II: push listener). The summary below is what you need to operate, not a protocol reference.

### Wake server (UDP)

Reolink's app finds a sleeping camera via `p2p*.reolink.com` (UDP 9999 / 58200). Bairelay's `bairelay_wake_server` crate impersonates that cloud locally:

```toml
[wake_server]
enable = true
```

**What you have to do**: redirect DNS for `p2p.reolink.com` and all subsequent `p2p*.reolink.com` (any subdomain matching the wildcard) to bairelay's bind address on your LAN. This is a single rule on a Pi-hole / AdGuard / unbound / pfSense — *not* `/etc/hosts` on the bairelay box (the camera consults the LAN resolver).

After **every** DNS rule change, verify the rewrite from a different device on the same network — not from the resolver host itself, since some resolvers don't apply their own rewrites locally. Use `dig p2p.reolink.com @<resolver-ip>` or `host p2p.reolink.com <resolver-ip>` and confirm the answer is bairelay's LAN address. Then **reboot the cameras**: Reolink firmware caches DNS aggressively, so a running camera will keep talking to the real cloud unless power-cycled. A quick `bairelay reboot <camera>` (over the existing Baichuan session) is the easiest way; failing that, use the official app to force a reboot.

With that in place:

- Cameras boot, query the (fake) cloud, get our register address.
- Cameras send heartbeats to bairelay every ~20 s.
- When bairelay (or another LAN client) wants to wake a camera, it asks our wake server, which fires a 10-packet UDP burst at the camera. The camera wakes within seconds.
- Operators see one INFO log line per camera registration / staleness eviction.

### Push listener (TCP/443)

Battery cameras report motion to `pushx.reolink.com:443` over TLS. The TLS leaf is cert-pinned, so we can't read the JSON body — but the **TCP connection attempt itself is the motion signal**. There is no other LAN-visible motion traffic from the camera.

```toml
[push_listener]
enable = true
bind_port = 443                       # rebind if 443 is taken
motion_wake_hold_secs = 30
```

**What you have to do**: redirect DNS for `pushx.reolink.com` to bairelay. The camera connects, bairelay maps the peer IP to a camera — through the wake server's heartbeat registry, or failing that a camera whose `address` is that IP — fires a `status/motion=on` MQTT publish, holds a wake-lock for `motion_wake_hold_secs`, and tears the socket. The TLS handshake never completes; the camera's retry state machine moves on within a few hundred milliseconds with no observable downstream effect.

**Keeping Reolink's own notifications.** With `forward_upstream = true`, bairelay relays each connection to the real `pushx.reolink.com:443` instead of closing it. Only bytes pass through: TLS stays end-to-end between the camera and Reolink, the pinned certificate checks out, and the Reolink app is notified as if nothing were in the way — while bairelay still gets its motion edge from the connect. Every connection is relayed, including from Reolink devices bairelay doesn't manage, since the DNS override catches them all. The real address is looked up at `upstream_resolver` (default `1.1.1.1`); it must not be the LAN resolver carrying the override, and an answer pointing back at bairelay is refused rather than looped.

```toml
[push_listener]
enable = true
forward_upstream = true
# upstream_resolver = "1.1.1.1"
```

**Without the wake server.** The push listener doesn't need `[wake_server]` if each camera has a literal IP `address` (with a DHCP reservation, since that's what pushes are matched on). Leaving `p2p*.reolink.com` with Reolink keeps the Reolink app's remote live view working; bairelay then reaches the camera — awake from the motion that triggered the push — through its configured discovery.

The "real" `motion=off` arrives once bairelay reconnects in-session and the camera reports `MotionStatus::Stop` natively. The hold-window timeout is a fallback so HA never sticks on `on`.

**Push vs. motion.** Each push also turns `status/push` on for the same hold window (a later push extends it), surfaced in HA as a `push` binary sensor. In-session motion never touches it. `status/motion` reports every motion event a live session sees — including during live view or a kept-awake session — whereas a push is the camera's own alarm decision, made under its detection and schedule settings. Automations that should fire like a Reolink notification belong on `push`. The entity is only published while `[push_listener]` is enabled.

**Person / vehicle / animal / other.** The push can't say *what* moved — that's in the TLS body. The class comes from the reconnected Baichuan session: the camera's alarm event carries an `AItype` (`people`, `vehicle`, `dog_cat`, `other`), and bairelay publishes it as retained `on`/`off` on `status/motion/person`, `status/motion/vehicle`, `status/motion/animal` and `status/motion/other` alongside the aggregate `status/motion`. `other` is any motion not attributed to the first three, including all motion from cameras without on-board AI. Expect the class a few seconds after the push-driven `motion=on`, once the wake completes; the hold-window fallback clears the class topics along with `motion`. In HA these are four `binary_sensor`s under the `motion_class` discovery feature.

### Caveats

- **Subject to firmware-induced breakage.** New firmware versions can change cert pinning (push), introduce new BcUdp variants (wake), or change the cadence/format of either. Bairelay's tests pin known-good shapes byte-for-byte but a future firmware revision could land before bairelay does.
- **DNS redirect is the operator's job.** Bairelay cannot redirect cloud traffic on its own — the camera consults whatever DNS resolver DHCP handed it.
- **Non-bypassable MITM is not on the table.** The TLS chain to `pushx` is leaf-pinned. Without a Reolink-signed leaf or modified firmware, the JSON body of motion events is unreachable.
- **Don't run the listener on a public IP.** The push listener accepts any TCP connection to port 443 with a registered peer-IP and treats it as motion. Behind NAT this is fine; on a public IP it's a free DDoS-as-motion vector.

---

## Troubleshooting and ergonomics

### Logs

Local-time timestamps, ANSI colours when stderr is a TTY (disabled when piped or `NO_COLOR` is set):

```text
2026-04-28 20:31:32  INFO bairelay: bairelay starting version=0.9.0
2026-04-28 20:31:32  INFO bairelay: Loaded configuration cameras=2 config=config.toml
```

`RUST_LOG=bairelay=debug,bairelay_wake_server=debug` for protocol-level traces. `RUST_LOG=bairelay=trace` for everything.

### MQTT preview JPEGs

By default each camera's `status/preview` topic publishes a base64-encoded JPEG every 2 s when the camera is awake, and a stale JPEG with a `SLEEPING` / `CONNECTING` overlay when not. Disable per-camera with `enable_preview = false` in `[cameras.mqtt]`. Note: rumqttc defaults `max_packet_size` to 10 KiB — bairelay raises it to 16 MiB internally so 4K snapshots don't crash the connection.

### Build version

Bairelay's version is `[workspace.package].version` in the root `Cargo.toml`, exposed via `bairelay --version`. Release cuts happen via `scripts/release.sh <version>` — it bumps the manifest, refreshes the lockfile, drafts a CHANGELOG entry, commits, tags `v<version>`, and pushes; the `release.yml` workflow then matrix-builds the binaries. Between releases every build of a given commit produces an identical binary — see `docs/architecture.md` § Reproducible builds.

### Test rig

- `tests/scripts/manual-verify.sh` drives ffprobe / ffmpeg / mpv / vlc / go2rtc against a live bairelay instance (install them first locally).
- `tests/scripts/ha-verify.sh` provisions Home Assistant entries via the WebSocket API and pulls snapshots end-to-end. Both are in the repo and run against a real camera on the LAN — see `docs/testing.md`.

---

## Development

### CI

Every push and pull request runs the workspace through GitHub Actions (`.github/workflows/ci.yml`):

- `cargo fmt --all --check` (rustfmt with hard tabs, see `rustfmt.toml`)
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --all-targets` on Linux **and** macOS
- `cargo tarpaulin` with `fail-under` set in `tarpaulin.toml`

The same gates run locally:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo tarpaulin
```

### Test inventory

The workspace ships thousands of tests across the four crates plus the binary. They split roughly into:

- **Unit tests** (most of the count) — pure-logic decision tables, parser round-trips, decision-table classification.
- **Integration tests** under each crate's `tests/` — RTSP end-to-end, TLS handshake, MQTT control parsing, BcMedia replay against captured fixtures, wake-server UDP loopback.
- **Property tests** via `proptest` — fuzz BcMedia / NAL whitelist / AAC ADTS / ADPCM block parsers with arbitrary byte input. Contract: never panic, never hang.
- **Defensive regression tests** for every category of malformed input we've seen (oversize Content-Length, MQTT topic injection, RTSP URI traversal, auth header pathologies, cross-UID confusion).

### Benchmarks

`crates/rtsp/benches/` ships [criterion](https://crates.io/crates/criterion) benchmarks for the H.264 / H.265 RTP packetisers and the `LastFrameBuffer` read / write paths. Run with `cargo bench -p bairelay_rtsp`.

### Coding standards

- **Tabs, not spaces** for indentation. Enforced by `rustfmt.toml`.
- **DRY** — three similar lines is the threshold to refactor; two is fine.
- **No half-finished implementations** — the diff documents *what*, the commit body documents *why* and *how*.
- **Definition of done** = code review + lint clean + tests + relevant docs updated.

---

## Not yet implemented

A short, unordered, non-committed list of features bairelay does not have today but might grow into:

- Two-way audio (talk client) — sending audio to the camera speaker via ADPCM.
- Multicast RTSP — extra transport for fan-out to many concurrent LAN viewers.
- Periodic background refresh of the PTZ preset cache (currently query-driven).
- On-board email notifications via the camera's built-in SMTP client.
- Anonymous-RTSP guardrails — startup warning + opt-in `allow_anonymous_rtsp` knob, plus an SDP cache so DESCRIBE on a sleeping camera doesn't acquire a wake-lock.
- Health / observability surfaces — Prometheus metrics, structured event log shipping, per-camera SLOs.

No timeline, but will prioritise if enough people show interest in a specific feature. The aim is to implement only what is necessary and prevent scope-creep.

---

## Disclaimer

This project is not affiliated with, endorsed by, sponsored by, or otherwise connected to Reolink in any way. "Reolink", "Argus", and other product names are trademarks of Reolink Innovation Inc. or their respective owners; their use here is purely descriptive. Bairelay is an independent, community-driven, open-source project created for **interoperability** with cameras the operator already owns. No proprietary code, firmware, signing keys, or internal documentation from Reolink is included or required. The protocol implementation is based exclusively on publicly available reverse-engineering of traffic between cameras and clients, and does not circumvent any technological protection measure — every authentication exchange is cooperative and uses credentials the operator already controls.

Bairelay is provided **"as is", without warranty of any kind** (see `LICENSE` for the AGPL-3.0 warranty and liability disclaimers). The operator is solely responsible for ensuring their use of the software is lawful in their jurisdiction, that they own or are authorised to administer every camera they connect to, and that enabling features such as DNS redirection, the local wake server, or the push-listener interception is consistent with whatever terms of service or contractual obligations apply to their devices. The authors accept no liability for misuse, regulatory consequences, or damage of any kind arising from the use of this software.

## License

Bairelay is free software, released under the **GNU Affero General Public License v3** — see `LICENSE`. This means that if you incorporate bairelay into a piece of software available over the network, you must offer that software's source code to your users.
