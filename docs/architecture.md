# Bairelay — Architecture

Static reference for the project's structure, dependencies, and runtime patterns. Day-to-day implementation knowledge lives in `docs/implementation.md`.

---

## Workspace structure

```
bairelay/
├── Cargo.toml              # workspace root + binary crate
├── tarpaulin.toml          # coverage tool defaults
├── src/                    # binary: CLI, config, orchestration, lifecycle
├── crates/
│   ├── core/               # bairelay_neolink_core: Baichuan protocol (vendored)
│   ├── rtsp/               # bairelay_rtsp: RTSP server + RTP packetisation
│   ├── mqtt/               # bairelay_mqtt: MQTT bridge + HA discovery
│   └── wake-server/        # bairelay_wake_server: local BcUdp wake server
├── fuzz/                   # cargo-fuzz harness (excluded from workspace)
├── docs/                   # specification, architecture, build, etc.
└── tests/                  # integration tests, fixtures, scripts
```

## Crate responsibilities

### `bairelay` (binary, `src/`)

Owns the application lifecycle:

- CLI parsing via `clap` — service modes (`mqtt` / `rtsp` / `mqtt-rtsp`), one-shot camera commands (`reboot`, `snapshot`, `battery`, etc.), and `check-config` (parse + validate + warn, no camera connect).
- TOML configuration loading and validation.
- Camera orchestrator: spawns per-camera task trees, manages connections.
- Wake-lock counter with dual `Notify` and Drop-based RAII guards.
- Watchdog: 30 s sweep reconciling camera state vs. active wake locks.
- `Supervisor` (`src/supervisor.rs`) — named-spawn + cancel-on-shutdown for the long-running services (RTSP plain, RTSPS, wake server, push listener, watchdog, startup-wake). MQTT lives outside the supervisor: its event loop has a distinct cancel token because per-camera teardown publishes its final `disconnected` status via MQTT, so MQTT must outlive the orchestrator.
- `MqttBackoff` (`src/mqtt_loop.rs`) — exponential backoff (1, 2, 4, 8, 16, 30 s) with log dedupe + 60 s relog window for the broker reconnect path.
- `sleep_or_cancel` (`src/run_support.rs`) — shared sleep+cancel primitive used by every retry / backoff path.
- All listener sockets bind synchronously in `main.rs` before any "started" log line; bind failures halt startup.
- RTSP `max_connections` semaphore (default 256 in the binary) caps concurrent client handlers.
- Graceful shutdown via `CancellationToken` + supervisor-orchestrated per-task join with 2 s budget.

### `crates/core/` — `bairelay_neolink_core`

Vendored Baichuan protocol implementation. Modernised to edition 2021 with updated dependencies. Public surface:

- `BcCamera` — async API for every camera operation: login, streaming, PTZ, motion detection, battery, LED, reboot, etc.
- `CameraDriver` — `dyn`-compatible trait mirroring the subset of `BcCamera` the binary's non-stream code paths call. Lets test code substitute a `FakeCamera` without a live camera session.
- `BcConnection` — TCP/UDP connection management.
- Discovery: `Discovery` struct with `CameraDiscoverer` trait (local broadcast, remote, map, relay, cellular).
- Protocol encoding/decoding via `nom` parser + `cookie-factory` serialiser.
- AES-CFB encryption, MD5 challenge-response authentication.
- `VideoStream` trait over `StreamData` so video pull loops can be tested against `MockVideoStream`.

### `crates/rtsp/` — `bairelay_rtsp`

Pure-Rust RTSP server:

- RTSP session state machine (OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN).
- SDP generation from codec parameters.
- H.264 RTP packetisation per RFC 6184 (NAL fragmentation, SPS/PPS handling).
- H.265 RTP packetisation per RFC 7798 (FU fragmentation, VPS/SPS/PPS handling).
- AAC (RFC 3640 AU-hbr) and G.711 µ-law (RFC 3551 PT 0) audio.
- ADPCM → G.711 transcode with 16 → 8 kHz resample.
- TCP-interleaved transport (RTP-over-RTSP) and UDP-unicast transport.
- Per-session keepalive watchdog; digest auth with 5 min nonce TTL + RFC 7616 §3.4 URI binding; basic auth on plain transport for drop-in compat.
- 30 s slow-loris timer on every fresh connection (disarmed once a complete request dispatches).
- `max_connections` semaphore (default unlimited at the crate boundary, `Some(256)` set by the binary) caps concurrent client handlers.
- `Content-Length` capped at the request buffer maximum (64 KiB) with `checked_add` arithmetic.
- `RtspServer::serve_with_listener` accepts a pre-bound `TcpListener` so `main.rs` binds synchronously at startup; `serve` is the thin wrapper that binds + delegates.
- `StreamProvider` trait — the binary implements via `CameraProvider`.
- Multi-track SETUP with per-SSRC RTP counters; RTCP Sender Reports are intentionally suppressed (mpv/ffmpeg re-anchor on every SR receipt — see `docs/implementation.md` § RTCP).
- Per-session coordinator (`session_task::run`) spawns parallel `video_dispatch_loop` + `audio_dispatch_loop`. Each holds its own `broadcast::Receiver`, so video FU bursts can't queue audio behind them; the TCP-interleaved write mutex holds at one `$-framed` packet at a time.

### `crates/mqtt/` — `bairelay_mqtt`

MQTT bridge:

- `SharedMqttClient` wrapping `rumqttc::AsyncClient`.
- Status / control / query topic helpers.
- Home Assistant MQTT discovery payloads (light, camera, binary_sensor, switch, select, button, sensor).
- `test_support::mock_client()` returning a `MockHandle` capture sink for unit tests.

### `crates/wake-server/` — `bairelay_wake_server`

Local replacement for Reolink's P2P cloud. Full wire-level reference: `docs/cloud-interception.md` § Part I.

- BcUdp Discovery framing reused from `bairelay_neolink_core::bcudp` (header + CRC + XOR XML).
- Two `tokio::net::UdpSocket` listeners (`middleman` port 9999, `register` port 58200) sharing one `Arc<CameraRegistry>` plus an `Arc<SessionAnchors>` map keyed by camera UID (issued at `M2D_Q_R`, echoed in `R2D_R_R` — cameras anchor to it).
- Middleman: `C2M_Q` (clients) → `M2C_Q_R`; `D2M_Q` (cameras on boot) → `M2D_Q_R` issuing a fresh session token + ac.
- Register: `D2R_R` (camera registration) → `R2D_R_R{rsp:-4, ac}`; `D2R_HB` upserts UID → source-addr at `Instant::now()`; `C2R_C` for a fresh entry spawns 10 × `R2D_C` at 100 ms then replies `R2C_C_R` + `R2C_T`; `D2R_DISC` acked with `R2D_DC_R`.
- Lazy stale-on-lookup registry (default 80 s TTL, ≈ 4 × heartbeat). No background sweep. Long-form / short-form UID prefix-match on lookup so `C2R_C` from operator config matches `D2R_HB`'s firmware-suffixed UID.
- One public entrypoint `run(RuntimeConfig, CancellationToken) -> Result<(), WakeServerError>`. Bind IP inherited from the top-level `bind_addr`; `route::advertise_ip` derives the per-peer local IP when `bind = 0.0.0.0` so we never advertise the wildcard.

## Dependencies

### Core stack

| Purpose                | Crate                  | Version | Notes                            |
|------------------------|------------------------|---------|----------------------------------|
| Async runtime          | `tokio`                | 1.x     | `rt-multi-thread` + `macros`     |
| Async traits           | `async-trait`          | 0.1.x   | Workspace dep                    |
| Logging                | `tracing`              | 0.1.x   | Structured, span-based           |
| Log output             | `tracing-subscriber`   | 0.3.x   | `fmt` layer with `EnvFilter`     |
| Errors (binary)        | `anyhow`               | 1.x     | Top-level error chains           |
| Errors (libraries)     | `thiserror`            | 2.x     | Typed errors in crate APIs       |
| CLI                    | `clap`                 | 4.x     | Derive macro                     |
| Serde                  | `serde`                | 1.x     | Derive macro                     |
| Config format          | `toml`                 | 0.8.x   | TOML parsing                     |
| XML serialisation      | `quick-xml`            | 0.36.x  | Serialiser + deserialiser        |
| Byte buffers           | `bytes`                | 1.x     | XML serialisation buffer         |

### RTSP / RTP (`crates/rtsp/`)

| Purpose                | Crate                  | Version | Notes                            |
|------------------------|------------------------|---------|----------------------------------|
| RTSP messages          | `rtsp-types`           | 0.1.x   | RFC 7826 parse/serialise         |
| RTP packets            | `rtp-types`            | 0.1.x   | RFC 3550 packet framing          |
| SDP                    | `sdp-types`            | 0.1.x   | SDP body generation              |
| TLS                    | `tokio-rustls`         | 0.26    | rustls 0.23 + aws-lc-rs; serves `rtsps://` |

H.264 NAL → RTP (RFC 6184) and H.265 NAL → RTP (RFC 7798) packetisation are implemented in-crate — no existing Rust crate covers this.

### MQTT (`crates/mqtt/`)

| Purpose                | Crate                  | Version |
|------------------------|------------------------|---------|
| MQTT client            | `rumqttc`              | 0.24.x  |
| JSON (HA discovery)    | `serde_json`           | 1.x     |
| Base64 (preview)       | `base64`               | 0.22.x  |

### Protocol core (`crates/core/`)

| Purpose                | Crate                  | Version | Notes                            |
|------------------------|------------------------|---------|----------------------------------|
| Encryption             | `aes` + `cfb-mode`     | latest  | AES-CFB for Baichuan             |
| Hashing                | `md5`                  | 0.7.x   | Challenge-response auth          |
| Parsing                | `nom`                  | 7.x     | Binary protocol parsing          |
| Serialisation          | `cookie-factory`       | 0.3.x   | Binary protocol writing          |
| XML                    | `quick-xml`            | 0.36.x  | Camera XML messages              |
| CRC                    | `crc32fast`            | 1.x     | Packet checksums                 |

### Wake server (`crates/wake-server/`)

Minimal additional dependencies — uses `tokio` UDP, `crc32fast`, `quick-xml`, `serde` / `toml` from the workspace.

## Architecture patterns

### Per-camera task tree

Each camera gets a tokio task tree rooted in the orchestrator. All tasks for a camera share a `CancellationToken` derived from the global token:

```
global CancellationToken
├── MQTT event loop (polls broker, spawns dispatch tasks)
├── watchdog (30 s sweep, requests disconnect for idle cameras)
└── camera "frontdoor" token
    └── connection loop (discover → login → keepalive → reconnect)
        └── session tasks (cancelled on disconnect, aborted after 2 s)
            ├── motion detection listener (retry on error)
            ├── battery poller (configurable interval)
            ├── floodlight tasks poller (configurable interval)
            ├── floodlight event listener (real-time)
            ├── PIR state poller (one-shot on connect, re-publish on set)
            ├── preview poller (camera-lifetime — see Last-frame buffer)
            └── grace period (countdown after last wake-lock release)
```

Cancelling the global token shuts down everything. Each camera has a child token. Each connected session has a session token that cancels pollers and listeners on disconnect.

### Wake lock + grace period

```rust
struct WakeLockInner {
    count: AtomicUsize,
    notify_release: Notify,   // signals 1 → 0
    notify_acquire: Notify,   // signals 0 → 1
}
```

Two separate notifications:

- `notify_acquire` (0 → 1): wakes idle-disconnect cameras waiting for work.
- `notify_release` (1 → 0): triggers the grace-period countdown.

Both use `Notify::notify_one()` (not `notify_waiters()`) so a permit is stored when no waiter is registered — late `.notified().await` calls still fire. The grace-period timer listens on `notify_release` and starts a countdown; any new `acquire()` before the countdown expires resets it.

The watchdog also monitors wake-lock state and can force-disconnect via a `disconnect_signal: Notify` on each camera.

### Watchdog

A tokio task that runs every 30 seconds. For each camera with `idle_disconnect` enabled:

1. If connected with zero wake locks held, call `request_disconnect()` to cancel the session.
2. If there are stream sources with no subscribers older than `stream_prune_grace_secs` (default 30), drop them. The default is intentionally ≤ each camera's `idle_disconnect_timeout_secs` (default 45) so a cached `StreamSource` can never outlive the Baichuan session that feeds it.

The watchdog is a safety net, not the primary lifecycle mechanism.

### RTSP session lifecycle (battery camera)

```
RTSP client connects
  → acquire wake lock
  → if camera sleeping: wake camera, serve last-frame placeholder
  → camera awake: start Baichuan video stream
  → relay frames as RTP to client

RTSP client disconnects
  → stop Baichuan video stream
  → release wake lock
  → grace period starts
  → no new clients: camera disconnects and sleeps
```

The RTSP crate exposes connect/disconnect callbacks; the binary hooks these to the wake-lock system. The RTSP crate knows nothing about cameras.

### Last-frame buffer

```rust
struct LastFrameBuffer {
    frame: RwLock<Option<Bytes>>,  // most recent JPEG
}
```

Owned by `CameraHandle` (one per camera, not per stream). Updated on every video frame during active streaming and by `BcCamera::get_snapshot` calls. Read by:

- The RTSP server as a placeholder while the camera wakes.
- The MQTT preview publisher (`status/preview` topic).

Cleared on service restart; repopulated by the startup-wake cycle. Never persisted to disk.

### Placeholder streams during gaps

Per-`StreamSource` `GapState { Live, Bridging }`. A 200 ms ticker compares `last_live_frame_at` against `gap_threshold_secs` (default 3.0). On exceedance, the source flips to `Bridging` and re-broadcasts cached `VideoBurst::iframe_nals` with synthesised PTS so RTSP clients see continuous RTP. Audio packets are dropped on the wire while bridging but per-codec PTS counters advance via the camera's audio cadence so A/V stays aligned on Live resume.

Per-camera `PreviewState { Live, Connecting, Sleeping }` published via `watch::Sender`. The MQTT preview publisher composites a caption (e.g. `SLEEPING`) on stale JPEGs so HA dashboards distinguish live from stale.

### Trait abstractions for testing

| Trait              | Location                                            | Purpose                                              |
|--------------------|-----------------------------------------------------|------------------------------------------------------|
| `CameraDriver`     | `bairelay_neolink_core::bc_protocol::camera_driver`          | Subset of `BcCamera` the binary calls                |
| `CameraDiscoverer` | `bairelay_neolink_core::bc_protocol::connection::discovery`  | Discovery fallback chain (local/remote/map/relay)    |
| `VideoStream`      | `bairelay_neolink_core::bc_protocol::stream`                 | `BcMedia` pull loop over `StreamData`                |
| `PacketSource`     | `bairelay::stream_source` (binary)                  | `BcMedia` injection for translator-loop tests        |
| `StreamProvider`   | `bairelay_rtsp::provider`                           | RTSP server's view of a camera                       |

Production impls forward to the concrete types (`BcCamera`, `Discovery`, `StreamData`, etc.); test impls (`FakeCamera`, `ScriptedDiscoverer`, `MockVideoStream`, `FakeStreamProvider`) live alongside and let unit tests exercise the same code paths a live camera would drive.

## Error handling strategy

- **Library crates** (`core`, `rtsp`, `mqtt`, `wake-server`): use `thiserror` with typed error enums. Each crate defines its own error type.
- **Binary crate** (`bairelay`): uses `anyhow` for top-level error propagation with context.
- **Connection failures**: logged and retried with exponential backoff. Never crash the process.
- **Authentication failures**: stop retrying permanently. Don't hammer the camera with bad credentials.
- **Protocol errors**: logged at warn / error. Malformed packets from cameras are discarded, not propagated.
- **Configuration errors**: fail fast at startup with clear messages.

The CLI's coarse exit-code table for one-shot commands (see `src/oneshot/classify.rs`):

| Code | Meaning                                                   |
|------|-----------------------------------------------------------|
| 0    | success                                                   |
| 1    | generic failure                                           |
| 2    | usage (bad args, unknown camera, missing config)          |
| 3    | config (malformed TOML, validation failure)               |
| 4    | connection / auth (login refused, transport dead, DNS)    |
| 5    | protocol (malformed reply, XML parse, time-sync issue)    |
| 6    | unsupported (`MissingAbility` — camera lacks the feature) |
| 130  | interrupted (Ctrl+C; SIGTERM on Unix)                     |

Scripts can branch on the exit code without parsing stdout.

## Reproducible builds

Bairelay release artefacts are bit-for-bit reproducible from `(commit, target triple, rustc version)`. The properties this rests on:

- `Cargo.lock` is committed; every CI and release `cargo`/`cross` invocation passes `--locked`.
- No `git = "..."` dependencies — everything resolves through `crates.io` or workspace `path =`.
- `build.rs` is absent. The version comes from `env!("CARGO_PKG_VERSION")`, sourced from `[workspace.package].version`.
- No build-time wall-clock timestamps, hostnames, usernames, or absolute build paths are embedded. Every `SystemTime::now()` / `OffsetDateTime::now_utc()` in the tree is runtime.
- `[profile.release]` sets `strip = "symbols"`. The release workflow additionally exports `RUSTFLAGS=--remap-path-prefix=...` to rewrite the cargo registry + workspace paths that otherwise leak into panic strings. Cargo's unified `trim-paths` profile key remains unstable in Cargo 1.95 — swap in when it stabilises.
- `SOURCE_DATE_EPOCH` is not consulted because no build date is embedded; the contract is the env-var-free baseline. Any future build-time date must go through `SOURCE_DATE_EPOCH` rather than `SystemTime::now()`.

Out-of-tree caveat: `aws-lc-rs` (pulled via `rustls = "0.23"`) compiles C code whose `__DATE__`/`__TIME__` may leak into the static archive. If Debian packaging surfaces it, the documented fallback is the rustls `ring` feature flag — one call site in `crates/rtsp/src/server/tls.rs::install_*`.
