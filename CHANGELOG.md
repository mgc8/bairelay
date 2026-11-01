# Changelog

## [Unreleased]

Two-way audio (talk), ported from the neolink talk client:
- `bairelay talk <camera> --file <wav> [--volume <gain>]` plays a 16-bit PCM WAV through the camera speaker. Pure-Rust IMA ADPCM (DVI4 block layout) encoder; no GStreamer.
- MQTT `<prefix>/<cam>/control/talk` (payload: absolute host path to a WAV) with progress on `<prefix>/<cam>/status/talk` (`playing`, `done`, `error: <code>`).
- `[cameras.talk_config]` pins the `TalkConfig` (duplex, stream mode, ADPCM rate and block size). Required for battery doorbells, which neither answer `TalkAbility` nor advertise `two_way_audio`.
- Top-level `talk_dir` confines MQTT talk paths to one directory.

### Changes

- talk: `talk` one-shot and `control/talk` topic
- talk: three-tier `TalkConfig` resolver (config table, camera `TalkAbility`, Reolink defaults)
- talk: WAV reader, linear resampler, DVI4 IMA ADPCM encoder
- config: `[cameras.talk_config]` table and `talk_dir`, validated by `check-config`
- core: `bypass_ability_gate` on `talk`, `talk_stream`, `talk_ability`; fix underflow on a short last talk payload

## [1.1.2] — 2026-06-08

Maintenance release:
- Renamed crates to avoid clashing namespaces
- Published to crates.io
- Updated CI to fix code review issues
- Added automattic crates.io publishing

### Changes

- codeql: build-mode none + v4 actions
- ci: explicit workflow permissions + codeql advanced setup
- ci: auto-publish to crates.io on release publish
- bairelay: fix cargo exclude patterns with leading slash
- bairelay-neolink-core: pin env_logger dev-dep for crates.io
- crates: exclude unneeded files.
- crates: update documentation, add publish-crates.sh
- crates: workspace.dependencies, polish, release.sh bump
- rename: bairelay-* namespace for crates.io publishing

## [1.1.1] — 2026-05-15

HAOS App fixes and improvements:
- separate `address` / `uid` / `username` / `idle_disconnect` fields per camera (was a single `host_or_uid` blob).
- `/config/bairelay/config.toml` is now the authoritative editable config — bootstrapped from an annotated template on first start, merged with the form on every restart. The file no longer needs to restate `username` for partial-overrride camera entries.
- `/config/bairelay/effective.toml` written every start (read-only diagnostic of the merged result, with defaults stripped).
- HA MQTT discovery auto-enabled at default topic `homeassistant`; entities appear in HA without manual `[mqtt.discovery]` configuration.
- Configuration form labels and inline descriptions localised via `translations/en.yaml`.
       
### Changes

- release: mirror CHANGELOG section to hassio/bairelay/CHANGELOG.md
- hassio: config.toml authoritative; auto-discovery; UI labels
- hassio: split address/uid/username; add idle_disconnect (default true)
- hassio: pass --mqtt-ssl as flag; silence bashio Service-not-enabled noise
- docs: HA UI is Settings -> Apps -> Install app (renamed in 2026.2)
- tests: ip-monitor-route daemon kills Colima's re-added eth0 route
- release: arm64 case match; build+release skip on release:published
- ci: force rustup default stable after dtolnay; print rustup show
- Updated CHANGELOG.md and fixed release script.
- ci: pin toolchain via rust-toolchain.toml

## [1.1.0] — 2026-05-15

Added Dockerfile and initial Home Assistant Add-On support under `hassio/` then updated documentation accordingly.

### Changes

- hassio: rename map to homeassistant_config; drop default boot:auto
- hassio: set SHELL ash -o pipefail so sha256sum pipe propagates
- ci: pin hadolint-action to v3.3.0
- tests: persistent fix for Colima VM networking (route + DNS)
- hassio: real icon + logo from docs/logos/ source masters
- hassio: pin base image to ghcr.io/hassio-addons/base:20.1.1
- hassio: field-merge MqttServerConfig — preserve base topic_prefix
- docs: README install-as-add-on section + CHANGELOG 1.1.0 entry
- ha-verify: list --bairelay-as-container in --help; drop dead id file
- ci: hadolint + HA add-on YAML lint on every push
- testing: document HA add-on verification surfaces
- ha-verify: --bairelay-as-container runs the add-on image
- release.sh: replay block covers hassio manifest too
- release.sh: bump hassio/bairelay/config.yaml version in lockstep
- release.yml: docker job pushes per-arch images to GHCR on publish
- release.yml: publish SHA256SUMS alongside tarballs
- hassio: boot:auto + entrypoint /tmp comment
- hassio: treat --mqtt-port 0 as unset sentinel
- hassio: CHANGELOG + README stubs for the add-on directory
- hassio: DOCS.md — install, options form, TOML overlay, debug
- hassio: placeholder transparent icon + logo (replace later)
- hassio: s6 longrun service runs render-hassio-config + bairelay
- hassio: Dockerfile pulls SHA-verified tarball from GH release
- hassio: build.yaml pins hassio-addons/base 15.0.10 per arch
- hassio: config.yaml — slug, options, services, network
- hassio: repository.yaml at repo root for HA scanner
- hassio: rustfmt fixup for cmd.rs
- hassio: end-to-end CLI test for render-hassio-config
- hassio: wire render-hassio-config subcommand into CLI
- hassio: tighten merge — pin wake_server.enable, flag mqtt invariant
- hassio: top-level merge() ties top-level + per-camera passes
- hassio: merge_cameras combines HA-options + overlay by name
- hassio: merge_top_level overlays bairelay-wide knobs
- hassio: parse_overlay reads operator TOML
- hassio: pin build_base_config field mappings; cover half-set creds
- hassio: cover empty-camera + no-mqtt-injection + ssl paths
- hassio: build_base_config maps HassioOptions to Config
- hassio: add MqttServiceFlags struct for Supervisor injection
- hassio: parse Supervisor options.json into HassioOptions
- hassio: scaffold module tree for render-hassio-config
- build: target reproducible releases; bump workspace to 1.0.0
- readme: bump stated rust toolchain floor to 1.93

## [1.0.0] — 2026-05-03

First public binary release.

### Changes

- set_time: read DST and compensate, fix +1h drift
- mqtt: cache + republish status on broker ConnAck for HA recovery
- mqtt: title-case preserves caps + splits on `-`; fix Floodlight Tasks

## [0.9.0] — 2026-05-02

Initial repo published.

Bairelay is a pure-Rust replacement for [Neolink](https://github.com/QuantumEntangledAndy/neolink) for Reolink battery cameras (Argus class). Relevant Baichuan-protocol code from Neolink is vendored in `neolink_core`; everything else is a clean rewrite.

### Highlights vs Neolink
- Pure Rust, no GStreamer — single static binary, no native deps.
- Stable jitter-free RTSP / RTSPS with audio + video pacers, gap bridging and proper battery-camera lifecycle.
- Tighter Home Assistant integration via MQTT including image previews with overlays, reading camera presets, PTZ and more.
- HEVC handling that survives Home Assistant's `ffmpeg:` re-publish and go2rtc's snapshot transcoder.
- Local replacements for Reolink's cloud wake server `p2p.reolink.com` and motion-push listener `pushx.reolink.com` (experimental).
- Improved one-shot CLI subcommands and a `check-config` validator with a coarse exit-code table.
- TLS (`rtsps://`) parallel listener with optional client-cert mTLS.
- Drop-in compatible with Neolink configs via `mqtt.topic_prefix = "neolink"`.
- >2000 workspace tests, fuzz harness, live-verify rigs against real Argus hardware.

### Deferred
- Two-way audio (`talk`).

See `README.md` for full rationale and acknowledgements.
