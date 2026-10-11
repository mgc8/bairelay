# Bairelay — Testing Guide

Everything testing: workspace gates, the BcMedia fixture pipeline, the Home Assistant compatibility rig, the manual live-verify harness, and the project's testing discipline.

---

## Workspace gates

Run these locally before pushing; CI runs the equivalent set on Linux + macOS:

```
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo fmt` needs `--all` because it doesn't honour the workspace's `default-members` setting (unlike `clippy` and `test`); the others would, so `--workspace` is redundant. CI swaps `cargo fmt --all` for `cargo fmt --all --check` so it verifies rather than rewrites — locally you want the auto-format variant.

`cargo test` exercises every unit + integration test in every workspace member (covered by `default-members` in the root `Cargo.toml`, so no `--workspace` needed). Current count: **>2000 tests**, 2 ignored (long-running flaky mock-timing scenarios documented in-code).

Coverage is measured via `cargo tarpaulin` from the project root — `tarpaulin.toml` at the repo root pins `workspace = true`, `all-targets = true`, `skip-clean = true`, so plain `cargo tarpaulin` reports the real number:

```
cargo tarpaulin
```

Current baseline is **~88 %**. See `docs/implementation.md` § Coverage policy for per-file targets and documented gaps.

A separate `cargo-fuzz` harness lives in `fuzz/`, excluded from the workspace so `cargo test` never compiles it (libfuzzer-sys is nightly-only). Its targets (listed in `fuzz/README.md`) cover the parsers + state machines facing untrusted bytes — the wake-server UDP entry point, BcUdp / Bc / BcXml deserialization, ADTS audio headers, ADPCM audio blocks, the NAL split-and-decode hot path, and the per-packet `UdpFlowState` send/recv decision table. Day-to-day:

```
scripts/fuzz.sh                       # all targets, 10s each
scripts/fuzz.sh aac_parse_adts        # one target only
FUZZ_TIME=600 scripts/fuzz.sh         # longer window per target
```

The wrapper buries libfuzzer's per-iteration chatter in `fuzz/logs/<target>.log` and prints one verdict line per target (OK + exec count, or CRASH + panic line + reproducer path). Setup (rustup nightly + `cargo install cargo-fuzz`) and direct `cargo fuzz` invocations are in `fuzz/README.md`.

---

## Test layout

```
tests/
├── scripts/
│   ├── manual-verify.sh   # live-camera RTSP/MQTT compat harness
│   ├── ha-verify.sh       # end-to-end Home Assistant ingestion check
│   ├── ha-up.sh           # start HA container (+ colima on macOS); idempotent
│   ├── ha-down.sh         # stop HA; stop colima iff we started it (macOS)
│   ├── ha-config/         # in-repo HA configuration.yaml + state
│   └── go2rtc/            # vendored binary (gitignored)
├── logs/                  # transient output (gitignored except README)
├── ha-token               # long-lived HA access token (ships with rig)
├── fixtures/              # captured BcMedia (gitignored)
├── cli_oneshot_test.rs    # CLI subprocess integration tests
├── cli_test.rs            # CLI argument parsing
├── config_test.rs         # config validation
├── connection_test.rs     # MQTT connection scenarios
├── dispatch_test.rs       # control-command dispatch
├── fixture_replay.rs      # BcMedia fixture replay against real RtspServer
├── grace_period_test.rs   # wake-lock + grace-period state machine
├── integration_test.rs    # orchestrator + camera lifecycle
├── orchestrator_test.rs   # per-camera task tree
├── sample_config_parses.rs
├── wake_lock_test.rs      # dual-Notify wake lock
└── watchdog_test.rs       # periodic-sweep watchdog
```

---

## Test infrastructure crates expose

`bairelay_neolink_core`'s exported test helpers are gated behind the crate's `test-util` Cargo feature; the binary's `[dev-dependencies]` opts in. Entries marked crate-internal are `cfg(test)`-only. The other crates' helpers compile unconditionally. See `docs/implementation.md` § Test infrastructure for full API:

- `bairelay_neolink_core::bc_protocol::FakeCameraBuilder` + `FakeCalls`
- `bairelay_neolink_core::bc_protocol::connection::mock::MockConnection`
- `bairelay_neolink_core::bc_protocol::connection::discovery::test_support::ScriptedDiscoverer` (crate-internal, test-only)
- `bairelay_mqtt::test_support::mock_client()` + `MockHandle`
- `bairelay::stream_source::PacketSource` (binary)
- `bairelay::oneshot::snapshot::MockVideoStream` (binary, test-only)

---

## Offline Bc-protocol decoder (`tests/scripts/decode-bc-pcap/`)

Replays a `tcpdump` capture of a Reolink Baichuan-over-UDP session through `bairelay_neolink_core`'s production parsers + AES-CFB primitives and prints each Bc message's header + decrypted payload. Use it to identify message IDs and XML schemas that bairelay does not yet model.

Standalone cargo project, excluded from the workspace, opts `bairelay_neolink_core` into the `pcap-decode-api` feature (off in production builds). Requires `tshark` on PATH for capture extraction.

Quick invocation:

```
BAIRELAY_DECODE_PASSWORD='...' \
  cargo run --manifest-path tests/scripts/decode-bc-pcap/Cargo.toml --quiet -- \
  tests/logs/real-pcap/<capture>.pcap <camera-ip>:<port> \
  --filter <msg_id>[,<msg_id>...] --brief
```

Full usage + flag table: `tests/scripts/decode-bc-pcap/README.md`. Captures go under `tests/logs/real-pcap/` (gitignored).

---

## BcMedia fixture pipeline

Captures real-camera `BcMedia` streams into files the test harness can replay later — no live camera in the loop. Drives `tests/fixture_replay.rs`.

### Prerequisites

- A real Reolink camera. The project targets **battery cameras only** (Argus series, Duo Battery, Go PT). Always-on Reolinks already expose RTSP natively and don't need bairelay.
- A working `config.toml`.
- Disk space: ~10 MB per minute of 4K HEVC main-stream capture, less for sub or extern.

### Capturing

Pass `--dump-bcmedia <dir>` on the `rtsp` or `mqtt-rtsp` subcommand. Every `BcMedia` packet pulled by the reader task gets mirrored to `<dir>/<camera>-<stream>.bcmedia`. With the flag absent, the hot path is a single `Option::is_some()` branch and no files are touched.

There is **no per-capture duration flag.** Capture runs as long as the process; stop with Ctrl+C (or SIGTERM).

```
RUST_LOG=bairelay=info cargo run -- mqtt-rtsp \
    --dump-bcmedia tests/fixtures \
    -c config.toml
```

Step by step:

1. Bairelay starts, connects every camera, runs the startup-wake cycle. Startup wake subscribes to Main just long enough to warm the last-frame buffer, so the first `<cam>-main.bcmedia` gets bytes immediately.
2. Whenever any `StreamSource` is active (startup wake, MQTT wakeup, RTSP client), packets append to the matching capture file. A sidecar `.meta.json` lands next to the first write.
3. Ctrl+C / SIGTERM triggers the graceful shutdown path; the capture buffer flushes before the camera's stop-video handshake. No packets lost on a clean exit.

### Capturing specific streams

Main is automatic via startup-wake. For Sub / Extern, subscribe via RTSP in a second terminal so the corresponding `StreamSource` starts:

```
ffprobe -rtsp_transport tcp rtsp://127.0.0.1:8554/<cam>/sub
ffprobe -rtsp_transport tcp rtsp://127.0.0.1:8554/<cam>/extern
```

Many battery cameras fall back to Sub when Extern is unavailable (`subscribe_with_extern_fallback` in `crates/rtsp/src/server/`); if you get Sub bytes in an `<cam>-extern.bcmedia` file, that's the camera's choice, not a capture-pipeline bug.

### File layout

For each `(camera, stream_kind)` pair the dump writer (`src/bcmedia_dump.rs`) produces two files under the capture root:

- `<root>/<camera>-<kind>.bcmedia` — concatenated `BcMedia` packets in the wire format. Appends across runs; never truncates.
- `<root>/<camera>-<kind>.meta.json` — sidecar with `camera`, `stream_kind` (`main` / `sub` / `extern`), `capture_started_at` (ISO-8601 UTC), `bairelay_version`. Rewritten on every process start.

### Argus codec mix

Argus emits **H.265 on main, H.264 on sub.** The replay test asserts the codec is one of the two with matching parameter sets (SPS+PPS for H.264, VPS+SPS+PPS for H.265), not a specific codec.

### Reader pattern

```rust
use bytes::BytesMut;
use bairelay_neolink_core::bcmedia::model::BcMedia;
use bairelay_neolink_core::Error;

let raw = std::fs::read("tests/fixtures/cam1-main.bcmedia")?;
let mut buf = BytesMut::from(raw.as_slice());
loop {
    match BcMedia::deserialize(&mut buf) {
        Ok(pkt) => handle(pkt),
        Err(Error::NomIncomplete(_)) => break,    // clean EOF
        Err(e) => return Err(e.into()),           // corrupt / truncated
    }
}
```

`FakeStreamProvider` in `tests/fixture_replay.rs` does exactly this.

### Privacy

Treat fixtures like the camera stream itself.

- **`.bcmedia` files contain raw camera bytes.** The repo's `.gitignore` excludes `tests/fixtures/*.bcmedia` and `*.meta.json`; confirm before committing.
- **Sidecar JSON contains camera names verbatim.** Sanitise camera names in `config.toml` before sharing a fixture file.
- **`RUST_LOG=bairelay=debug` prints camera IPs and UIDs.** Strip them before sharing logs. Keep `=info` for sharable runs.
- **Fixtures are not encrypted at rest.** Put `tests/fixtures/` on an encrypted volume on shared / backed-up machines.

### Replay usage

Drop `.bcmedia` (and `.meta.json`) files into `tests/fixtures/`. Naming must match `<camera>-<kind>.bcmedia` (what `--dump-bcmedia` produces) — the loader splits on the last `-` to recover the stream kind.

```
cargo test --test fixture_replay fake_provider_replays_real
```

With no fixtures, the test exits early with a stderr message and PASSes as a no-op. With at least one file, it drives `FakeStreamProvider` through the real `apply_bcmedia_packet` translator, subscribes, and asserts codec detection + keyframe-first delivery.

For ad-hoc inspection:

```rust
let provider = FakeStreamProvider::from_dir(Path::new("tests/fixtures"))?;
let sub = provider.subscribe("cam1", StreamKind::Main, None).await?;
// sub.sdp_params.video is populated before the first frame.recv().
```

`FakeStreamProvider::from_dir` skips malformed filenames with a `tracing::warn!` rather than failing the scan.

### Wire-format caveat: ADPCM ser/de

`BcMedia::Adpcm` does NOT round-trip byte-exact through `serialize` → `deserialize`. The capture pipeline writes raw camera bytes through a single `serialize` per packet, so on-disk `.bcmedia` files parse cleanly. **Do not** re-serialise captured packets to "normalise" them — the first ADPCM packet desyncs the loop. See `docs/implementation.md` § Wire-format gotchas.

### Troubleshooting

**Empty `.bcmedia` file.**
- Camera never came online during the startup-wake window. Check bairelay's log for warnings; capture starts as soon as a later event wakes the camera.
- Camera never responded to `start_video`. Check for `start_video failed` / `start_video timed out` warnings.

**Huge `.bcmedia` file (> 256 MiB).** The replay harness caps fixture size (`tests/fixture_replay.rs::MAX_FIXTURE_BYTES`). Larger files reject on subscribe.
- Re-capture for a shorter duration.
- If you must truncate, use `head -c <bytes>`. **Do NOT use `dd`** — its default 512-byte block splits packets across the boundary.

**Test says "no fixture files" though I just captured some.**
- Confirm the file ends in `.bcmedia` (not `.bcmedia.bak`).
- Confirm the stem is `<cam>-<kind>` with `kind` exactly `main` / `sub` / `extern` (case-insensitive).
- Run with `RUST_LOG=info cargo test --test fixture_replay` — malformed- filename skips log at `warn`.

---

## Home Assistant compatibility rig

Verifies HA can ingest bairelay's RTSP streams via the Generic Camera integration. Local-only — no auth, no TLS, no reverse proxy. The HA instance is **dedicated to bairelay testing**: camera entries are persisted across runs.

### Prerequisites

The rig ships pre-loaded. The committed state under `tests/scripts/ha-config/` includes `configuration.yaml`, the lovelace `Bairelay Test` dashboard, the `bairelay-verify` admin user, and the matching long-lived token (`tests/ha-token`). HA loads all of this from the bind mount at first start — there is no manual onboarding, no token-generation, no dashboard-building.

**One-time install (Linux):**

```
# Debian / Ubuntu — substitute your distro's package manager.
sudo apt install docker.io mosquitto-clients
sudo usermod -aG docker "$USER"   # log out / back in to take effect

docker pull ghcr.io/home-assistant/home-assistant:stable
docker pull eclipse-mosquitto:2

docker run -d --name homeassistant \
    --restart=unless-stopped \
    -v "$(pwd)/tests/scripts/ha-config:/config" \
    --network=host \
    --add-host=host.docker.internal:host-gateway \
    ghcr.io/home-assistant/home-assistant:stable
```

**One-time install (macOS):**

```
brew install colima docker mosquitto
colima start

tests/scripts/colima-vm-setup/install.sh     # see "Colima VM networking" below

docker pull ghcr.io/home-assistant/home-assistant:stable
docker pull eclipse-mosquitto:2

docker run -d --name homeassistant \
    --restart=unless-stopped \
    -v "$(pwd)/tests/scripts/ha-config:/config" \
    --network=host \
    ghcr.io/home-assistant/home-assistant:stable
```

`--network=host` shares the host network namespace with the container. On Linux that is the host directly; on macOS it is colima's Linux VM, which forwards `localhost:8123` to macOS, so either way HA reaches `http://localhost:8123` on the host. From inside the container, the host is `host.docker.internal` — colima resolves this natively, Linux's Docker needs the explicit `--add-host=host.docker.internal:host-gateway`. bairelay binds `0.0.0.0:8554` by default, so HA reaches it at `host.docker.internal:8554` on either OS.

The `mosquitto` container is created automatically by `ha-up.sh` on first run; no manual `docker run` is needed for it.

### Colima VM networking

Colima's default QEMU profile leaves the VM with two flaws that surface as random `network is unreachable` / `server misbehaving` errors on `docker pull`:

1. **Two default routes.** `col0` (metric 100, via the macOS host) works; `eth0` (metric 200, via QEMU's slirp at `192.168.5.2`) doesn't, and the Docker daemon's outbound source-IP routing picks it.
2. **Flaky DNS proxy.** The slirp gateway at `192.168.5.1` answers the VM shell reliably but drops daemon image-pull queries.

`tests/scripts/colima-vm-setup/install.sh` copies four idempotent systemd units + a watcher script into the Colima VM:

- `colima-fix-network.service` (oneshot at boot) — deletes the eth0 default route + writes `/etc/gai.conf` to prefer IPv4 in `getaddrinfo` (the VM has no IPv6 transit).
- `colima-fix-resolv.service` (oneshot, guarded) — rewrites `/etc/resolv.conf` to `1.1.1.1` + `8.8.8.8`.
- `colima-fix-resolv.path` — re-fires the resolv service whenever Colima's host-side agent resets resolv.conf, which it does ~3 s into every boot.
- `colima-route-watcher.service` (Restart=always daemon) — runs `ip monitor route` and deletes the eth0 default route every time Colima's agent re-adds it after the boot oneshot has already finished.

Run the installer once after `brew install colima` (and re-run after any `colima delete` + recreate). Survives `colima stop` / `colima start` because the VM disk persists.

### Regenerating the access token

If you ever invalidate the pre-loaded token (deleting it from the HA UI, recreating the rig from scratch):

1. User icon (bottom-left) → "Security" tab.
2. "Long-lived access tokens" → "Create Token". Name it `bairelay-verify`, copy.
3. Write to `tests/ha-token`:

```
printf '%s\n' '<paste token>' > tests/ha-token
```

### Start / stop

`ha-up.sh` is idempotent. On macOS it starts colima if not already running and writes `tests/logs/colima.started-by-us` so `ha-down.sh` / `ha-verify.sh` can tell later that they (not you) started it; a pre-existing colima for other projects is never touched. On Linux there is no colima step — the script proceeds straight to the HA + mosquitto containers.

```
tests/scripts/ha-up.sh                 # colima (macOS) + HA + test mosquitto + HA MQTT integration
tests/scripts/ha-up.sh --config-only   # HA + mosquitto only (skip colima even on macOS)
tests/scripts/ha-up.sh --wait 60       # override API-ready timeout

tests/scripts/ha-down.sh               # stop HA + mosquitto; stop colima if marker present (macOS)
tests/scripts/ha-down.sh --ha-only     # stop HA + mosquitto only
tests/scripts/ha-down.sh --colima-only # colima only (macOS; no-op on Linux)
```

`ha-up.sh` also: launches a `bairelay-mosquitto` docker container (image `eclipse-mosquitto:2`) alongside HA, sharing the same `--network=host` namespace so HA reaches it at `127.0.0.1:1884`; registers HA's MQTT integration pointing at the same address if not already present. Both steps are idempotent and `ha-down.sh` stops the container alongside HA. Test creds: `bairelay-test` / `bairelay-test-password`. The image must already be available locally — `docker pull eclipse-mosquitto:2` once on a machine with working outbound (on macOS, that means inside colima — `docker save | docker load` to sideload if your colima can't reach the registry). The bairelay-mosquitto container's lifecycle policy is `--restart=unless-stopped`, mirroring HA.

### Test-rig bairelay config

All HA-related testing uses `tests/bairelay-test.toml` (gitignored) instead of the live `config.toml`. Seed it once from `tests/bairelay-test.toml.example` and fill in real camera UIDs / passwords. The MQTT block points at the local test broker (`127.0.0.1:1884`); `[mqtt.discovery]` is at the top level so HA discovery actually fires (per-camera `discovery.topic` keys are silently ignored — `MqttConfig` lacks `deny_unknown_fields`). `[cameras.mqtt]` enables every per-camera feature flag so the dashboard sees the full set.

`ha-verify.sh` defaults to `tests/bairelay-test.toml`. Pass `-c <other.toml>` to override.

### Bairelay Test dashboard

Dashboard `bairelay-test` (sidebar entry "Bairelay Test") is provisioned in `.storage/lovelace.bairelay_test` and committed with the rig. One section per camera: live picture-glance overlay (battery / motion / floodlight badges on top of the stream), a sub-stream tile, sensor + control entity rows, and a 4-button PT pad. Discovery features `enable_floodlight` / `enable_pir` differ per camera in the seed config, so a camera section can drop entity cards for hardware it lacks. To rebuild from scratch, drive HA's WebSocket API (`lovelace/dashboards/{create,delete}` + `lovelace/config/save`) — the `.storage` files are the canonical source of truth and travel with the repo.

### End-to-end verification

```
tests/scripts/ha-verify.sh
```

Steps:

1. Ensures HA is up via `ha-up.sh`.
2. Builds (release) and starts `bairelay mqtt-rtsp`.
3. Waits for the RTSP listener + startup-wake cycle.
4. Issues `<topic_prefix>/<cam>/control/wakeup 3` via MQTT so each camera holds a wake lock across the full check.
5. For each `(camera, stream_kind)`:
   - Looks for an existing Generic Camera config entry with a matching `stream_source`.
   - If absent, creates one via `POST /api/config/config_entries/flow`.
   - **Never deletes entries on exit.**
6. Resolves `entry_id → entity_id` via HA's `/api/template`. Drives HA's WebSocket API (inside the HA container via `docker exec`) to rename the entry title, device `name_by_user`, and entity registry + `entity_id`. Idempotent — reruns are no-ops.
7. Pulls a snapshot two ways per stream:
   - `ha` — `GET /api/camera_proxy/<entity>` (Generic Camera wraps the RTSP URL with `ffmpeg:`).
   - `g2r` — adds a temporary raw-RTSP stream to HA's bundled go2rtc via its unix socket, fetches `GET /api/frame.jpeg?src=<tmp>`, deletes the temporary stream.
8. Asserts each JPEG is ≥ 1024 bytes and starts with `ff d8 ff`.
9. Stops bairelay. If `ha-up.sh` started colima this run, stops colima and removes the marker. HA container and entries stay.

Flags: `-c <file>`, `--no-build`, `--keep-colima`.

Expected output:

```
PASS  ha/camera_proxy/Cam1/main   size=...
PASS  ha/camera_proxy/Cam1/sub    size=...
PASS  ha/camera_proxy/Cam2/main   size=...
PASS  ha/camera_proxy/Cam2/sub    size=...
PASS  g2r-native/Cam1/main        size=...
PASS  g2r-native/Cam1/sub         size=...
PASS  g2r-native/Cam2/main        size=...
PASS  g2r-native/Cam2/sub         size=...

passed:       8 / 8
failed:       0
```

Exit code is zero when `failed == 0` and `passed > 0`.

### HA MQTT discovery

Bairelay publishes HA MQTT discovery payloads when opted in. Add `[mqtt.discovery]` to `config.toml`:

```toml
[mqtt]
broker_addr = "127.0.0.1"
# topic_prefix = "bairelay"   # default; "neolink" for legacy migration

[mqtt.discovery]
topic = "homeassistant"        # HA's own discovery prefix (required)
# features = ["floodlight", "camera", "motion", "motion_class", "push",
#             "led", "ir", "reboot", "pt", "pt_preset", "battery",
#             "siren", "pir"]   # default: all 13
```

Per camera, bairelay publishes a single HA device named after the camera (underscores → spaces, title-cased: `front_door` → `Front Door`). Entities per feature:

| Feature    | HA entity                         | Notes                                        |
|------------|-----------------------------------|----------------------------------------------|
| Floodlight | `light` + `switch` (tasks)        | State payload JSON `{"state":"on"}`          |
| Camera     | `camera`                          | Base64 JPEG preview from `status/preview`    |
| Motion     | `binary_sensor`                   | `unique_id` suffix `_md`                     |
| Motion class | 4 × `binary_sensor`             | Person / Vehicle / Animal / Other; suffix `_md_<class>` |
| Push       | `binary_sensor`                   | Only with `[push_listener]` enabled          |
| LED        | `switch`                          | Status LED (`ledCtrl` bit 0); `status/led`   |
| IR         | `select`                          | IR LEDs (`ledState` ability); `status/ir`    |
| Reboot     | `button`                          |                                              |
| PT         | 4 × `button`                      | Only when camera reports PTZ support         |
| Battery    | `sensor`                          | HA long-term-statistics fields               |
| Siren      | `button`                          |                                              |

PT buttons are gated on live capability detection via `BcCamera::get_support()` — no model-class guessing. If `get_support()` fails at connect (transient protocol error, mid-wake), bairelay leaves the cache empty and re-probes on the next reconnect.

### Verifying entries directly

```
TOKEN=$(cat tests/ha-token)
curl -s -H "Authorization: Bearer $TOKEN" \
     http://localhost:8123/api/states \
| python3 -c 'import json,sys; [print(s["entity_id"], s["state"]) \
              for s in json.load(sys.stdin) \
              if s["entity_id"].startswith("camera.")]'
```

UI:

- `http://localhost:8123/config/integrations/integration/generic`
- `http://localhost:8123/config/devices/dashboard?domain=generic`

### Troubleshooting

**`token file 'tests/ha-token' missing or empty`** — create it (above).

**`HA API auth check failed (HTTP 401)`** — token expired / revoked. Regenerate.

**`RTSP listener did not open on :8554`** — check `tests/logs/ha-verify/bairelay.log`. Common cause: another process bound to 8554.

**`cannot reach host.docker.internal from HA container`** — verify with `docker exec homeassistant getent hosts host.docker.internal`. If empty: on macOS, `colima restart`; on Linux, recreate the HA container with `--add-host=host.docker.internal:host-gateway` (the create-once command in `ha-up.sh`'s error path shows the correct flags).

**colima gets stopped when I didn't want it to** — pass `--keep-colima`. The script only stops colima if `ha-up.sh` started it in the same run.

**Snapshot returns `exit status NNN`** — body is an ffmpeg exit code propagated by go2rtc:

```
docker exec homeassistant tail -100 /config/home-assistant.log
```

**Delete HA camera entries.** Use the HA UI (Settings → Devices & Services → Generic Camera → click each → Delete) or:

```
TOKEN=$(cat tests/ha-token)
for eid in $(curl -s -H "Authorization: Bearer $TOKEN" \
    'http://localhost:8123/api/config/config_entries/entry?domain=generic' \
    | python3 -c 'import json,sys; [print(e["entry_id"]) for e in json.load(sys.stdin)]'); do
    curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
        "http://localhost:8123/api/config/config_entries/entry/$eid"
done
```

The next `ha-verify.sh` run recreates them.

---

## HA Add-on verification

The HA Add-on at `hassio/bairelay/` has two distinct test surfaces, only one of which the Colima HA Container rig covers.

### Surface 1: container parity (Colima rig)

The HA Container in Colima has no Supervisor and cannot install add-ons. It can, however, run the add-on image as a plain Docker container and observe that the MQTT-side behaviour is identical to `cargo run`. This validates the entrypoint shim, the `render-hassio-config` subcommand, the merge logic, and bairelay end-to-end — everything below the Supervisor integration layer.

Prerequisites:

- The add-on image built locally:

    ```
    docker build hassio/bairelay/ \
        --build-arg BAIRELAY_VERSION=<x.y.z> \
        --build-arg BAIRELAY_SHA256_AMD64=$(sha256sum /path/to/bairelay-vX.Y.Z-x86_64-linux.tar.gz | cut -d' ' -f1) \
        --build-arg BAIRELAY_SHA256_AARCH64=0000 \
        --build-arg TARGETARCH=$(uname -m | sed s/x86_64/amd64/) \
        -t bairelay-hassio-test:<x.y.z>
    ```

    For local tests, the cross-arch SHA can be zeroed out — only the host's arch is actually verified.
- `tests/logs/addon-test/data/options.json` — hand-written Supervisor options for the test cameras:

    ```json
    {
      "topic_prefix": "bairelay",
      "log_level": "info",
      "cameras": [
        {
          "name": "TestCamera",
          "address": "192.168.1.50",
          "username": "admin",
          "password": "<password>",
          "idle_disconnect": true
        }
      ]
    }
    ```
- `tests/logs/addon-test/config/bairelay/config.toml` — the TOML overlay. Should mirror the existing on-host test config shape so the merged result matches what `tests/bairelay-test.toml` produces. The overlay must include `[mqtt]` (since the container has no Supervisor injecting it) and any per-camera knobs not exposed by the form (channel, discovery method, etc.). `username` is supplied by the form so doesn't need to be restated.

Run:

```
tests/scripts/ha-verify.sh --bairelay-as-container
```

The rest of the test rig (HA Container ingest, MQTT entity verification, snapshot probes) runs unchanged. `--bairelay-as-container` implies `--no-build`.

`ha-verify.sh` 8/8 indicates the add-on packaging is functionally equivalent to on-host bairelay for the MQTT path.

### Surface 2: Supervisor integration

Manifest correctness, the options form rendering, `mqtt:want` service injection, `host_network: true` activation, `/config` + `/ssl` mounts, repository discovery in the HA UI — only testable inside a real Supervisor.

Path: HA OS in QEMU, or HA Supervised on a spare Debian host. Not a CI gate; a manual release-prerequisite run before each release tag.

Steps:

1. Boot HA OS in QEMU (see Home Assistant's "Developer install on QEMU" documentation). Alternatively, install HA Supervised on a spare Debian box. HA 2026.2+ uses the "Apps" terminology (formerly "Add-ons"); the steps below assume the current UI.
2. Settings → Apps → Install app (bottom-right) → ⋮ (top-right) → Repositories → paste `https://github.com/mgc8/bairelay` → Add.
3. The Bairelay app appears in the list (from the custom repo just added). Click it → Install.
4. Configuration tab → fill in `topic_prefix`, `log_level`, and at least one camera entry (`name`, one of `address` / `uid`, `username`, `password`). Save.
5. Optional: drop a `/config/bairelay/config.toml` overlay via the File editor app or SSH.
6. Start. Verify the app stays running (Log tab shows `bairelay starting version=X.Y.Z` and no immediate exit).
7. Confirm MQTT entities appear in HA exactly as `ha-verify.sh` expects (same `homeassistant/.../config` discovery payloads, same entity IDs).
8. Stop the app. Uninstall. Verify clean removal (retained MQTT discovery topics are unpublished).

Issues at this surface are usually:

- `services: ["mqtt:want"]` not picking up the broker — the HA MQTT integration must be installed and started before the add-on.
- `host_network: true` failing to bind — another process on the host is already on 8554/8555/9999/58200.
- Options form rejecting input — the regex `^[A-Za-z0-9_-]+$` on camera names doesn't allow dots or slashes; the password field type is `password` (masked) but accepts any string.

---

## Manual live-camera harness

`tests/scripts/manual-verify.sh` exercises the RTSP server + MQTT bridge against real cameras. On-demand only — not wired into `cargo test`. Discovers installed client tools, parses non-secret bits of the real `config.toml` (cameras, MQTT broker, credentials), spawns bairelay with `RUST_LOG=bairelay=debug,bairelay_rtsp=debug`, runs the probe matrix, tears down. Logs go to `tests/logs/manual-verify/` (gitignored).

### Prerequisites

Required:

- A working `config.toml` pointing at real Reolink battery cameras on your LAN.
- A built bairelay binary (`cargo build --release`).
- `ffmpeg` (which also provides `ffprobe`).
- GNU coreutils' `timeout` (the script falls back to `gtimeout` on macOS; install from Homebrew if missing).
- The RTSP port free: stop any deployed bairelay service sharing the config's bind address first. The harness exits 2 if the port is already taken, rather than probing the other instance.

Optional — the harness exercises whichever tools it finds installed and skips the rest:

- `mpv`
- `vlc` (on macOS the script also looks at `/Applications/VLC.app/Contents/MacOS/VLC` if `vlc` isn't on `PATH`).
- `mosquitto-clients` (for `mosquitto_pub` / `mosquitto_sub` — drives the MQTT round-trip stage; set `NO_MQTT=1` to skip).
- `go2rtc` — download the release binary for your platform from <https://github.com/AlexxIT/go2rtc/releases> and place it at `tests/scripts/go2rtc/go2rtc` (`chmod +x` it). The script feeds bairelay's RTSP into go2rtc and re-pulls it back; set `NO_GO2RTC=1` to skip.

**Install (Debian / Ubuntu):**

```
sudo apt install ffmpeg mpv vlc mosquitto-clients coreutils
```

**Install (macOS, Homebrew):**

```
brew install ffmpeg mpv mosquitto coreutils
brew install --cask vlc
```

### Running

```
tests/scripts/manual-verify.sh
tests/scripts/manual-verify.sh --duration 10
tests/scripts/manual-verify.sh --tls           # rtsps:// path; auto-runs gen-test-certs.sh
```

### Probe matrix

Per camera, per stream, per transport:

- `ffprobe` / `ffmpeg` / `mpv` / `vlc` over TCP-interleaved.
- `ffprobe` / `ffmpeg` over UDP.
- `2× ffmpeg` fanout (concurrent broadcast subscribers).
- MQTT round-trip (`query/battery` → `status/battery`).
- go2rtc relay (RTSP-in / RTSP-out via the local go2rtc binary).
- Battery sleep + grace-period observation (skipped via `SKIP_BATTERY_SLEEP=1`).

---

## Live-verify discipline

Every change passes through:

1. Unit tests for pure logic — `cargo test`.
2. Integration tests via mock providers / fixture replay against real RTSP clients.
3. **Live-verify on real battery-camera hardware before merging anything that touches the RTSP path, MQTT bridge, wake server, or push listener.** Mocks pre-populate state that real cameras only fill in over time; race conditions in connect / wake / disconnect cycles only surface on real hardware.

The standard live-verify rig is:

- A working `config.toml` pointing at one or more battery cameras on your LAN.
- `cargo build --release`.
- `tests/scripts/manual-verify.sh` and/or `tests/scripts/ha-verify.sh`.

If you don't have hardware, say so on the PR — a maintainer with cameras will run the rig before merge.

## Local wake server live-verify

Pre-requisite: a real Argus on the same LAN as bairelay, plus a LAN DNS resolver (Pi-hole, dnsmasq, unbound, the gateway's resolver, or whatever the cameras query) that returns the bairelay box's IP for `p2p.reolink.com` and `p2p1.reolink.com` … `p2p11.reolink.com`. **The cameras' DNS resolution is the load-bearing piece** — `/etc/hosts` on the bairelay box is irrelevant because cameras don't query it; they query their configured DNS (typically DHCP-provided, which points at your LAN resolver). Bairelay's outgoing discovery uses the same DNS, so once the resolver is configured both directions land on the wake server.

After flipping the resolver, sleeping cameras may take up to one heartbeat cycle (~20 s) to send their first `D2R_HB` here — but only after their internal DNS cache expires. Some Argus firmwares cache DNS for the lifetime of the connection; if no heartbeats arrive within a few minutes, force a re-resolution by power-cycling the camera or issuing `bairelay reboot <camera>`.

```
1. Configure your LAN resolver (Pi-hole / dnsmasq / unbound / ...) so that
   p2p.reolink.com and p2p1.reolink.com ... p2p11.reolink.com resolve to
   the bairelay box's LAN IP. Restart / reload the resolver as needed.
2. config.toml gains:
     [wake_server]
     enable = true
3. Start bairelay; tail logs for `D2R_HB src=<camera-ip>:<port> uid=<UID>`
   within ~25 s (sleeping cameras) or after the first sleep cycle (awake
   cameras). If still nothing after a few minutes, the camera is likely
   serving a stale DNS cache — `bairelay reboot <camera>` clears it.
4. Trigger wake via MQTT: mosquitto_pub -t bairelay/<cam>/control/wakeup -m 5
5. Tail logs for `C2R_C src=` then 10 `R2D_C ->` lines; confirm the camera
   connects on TCP 9000 immediately after.
6. ha-verify.sh — must remain 8/8.
7. manual-verify.sh — should remain at its current pass count.
```

In-process correctness coverage lives in `crates/wake-server/tests/udp_loopback.rs`; the live-verify above is the integration check, not a CI gate.

## Motion-class live-verify

Checks that the per-class topics (`status/motion/{person,vehicle,animal,other}`) track what the camera actually reports. The parser's vocabulary (`people`, `vehicle`, `dog_cat`, `other` in the alarm's `AItype`) comes from third-party reverse-engineering, so the point of this run is as much to record the real `AItype` values as to check the topics. Use a camera with on-board AI detection, with person / vehicle / animal enabled in the Reolink app's detection settings; on a camera without AI only `other` ever fires.

```
1. config.toml: for the camera under test set
     debug = true
   and run with
     RUST_LOG=info,bairelay=debug,bairelay_neolink_core::bc::de=trace
   so each decrypted AlarmEventList is dumped (`Payload Txt:` lines).
   The dumps carry credential hashes and UIDs — keep the log local.
2. Watch the topics (`#` also matches `status/motion` itself):
     mosquitto_sub -v -t 'bairelay/<cam>/status/motion/#'
3. In-session pass. Keep the camera awake for 10 minutes:
     mosquitto_pub -t bairelay/<cam>/control/wakeup -m 10
4. Walk through the frame. Within a second or two expect:
     log   Motion detected camera=<cam> classes=person
     dump  <AItype>people</AItype>
     mqtt  status/motion on, motion/person on, vehicle/animal/other off
   When the scene clears: status/motion off and all four classes off.
   Repeat with a vehicle and an animal if you can; a classification
   change mid-alarm (other -> person) should flip both topics. Note
   every distinct AItype in the dumps — anything outside people /
   vehicle / dog_cat / other / none needs a parser mapping.
5. HA: the camera's device gains Person / Vehicle / Animal / Other
   Motion binary sensors, and their history matches step 4.
6. Restart bairelay: all four read `unknown` until the first event.
   Restart a non-persistent broker: the last state is re-emitted on
   reconnect.
7. Push pass (needs the push-listener rig, docs/cloud-interception.md
   §II.5). Let the camera fall asleep, then walk past it. Expect
   `Motion push from camera` and status/motion on immediately; the
   class topics follow once the wake reconnects. If motion arrives but
   no class does, look for an AlarmEventList dump after the reconnect —
   the camera may not re-report an alarm already in progress. After
   motion_wake_hold_secs everything ends off.
```
