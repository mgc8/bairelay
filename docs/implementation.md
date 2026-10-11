# Bairelay — Implementation Notes

Day-to-day implementation knowledge: API gotchas, design decisions, and load-bearing details that any contributor needs in working memory.

Static structure and dependency tables live in `docs/architecture.md`.

---

## `bairelay_neolink_core` API surface

`BcCamera` is the production type. `CameraDriver` is the dyn-compatible trait the binary holds — production code mostly takes `Arc<dyn CameraDriver>`. `CameraHandle` keeps a parallel private `bc_camera_concrete: Arc<BcCamera>` for the two operations that aren't on the trait (`logout()` during shutdown; `StreamSource::start` for video pull loops).

### Methods we rely on in production

| Method                                | Notes                                                     |
|---------------------------------------|-----------------------------------------------------------|
| `BcCamera::new(&BcCameraOpt)`         | Connects via discovery. Local/direct: 2–15 s. Remote/relay registration retries 5 rounds w/ backoff, up to ~80 s for an asleep cam. |
| `login_with_maxenc(MaxEncryption)`    | MD5 challenge-response auth.                              |
| `logout()`                            | Concrete-only (not on `CameraDriver`); call with 5 s timeout. |
| `get_linktype()`                      | Used for keepalive (NOT `ping()`; see "Keepalive").       |
| `get_support()`                       | Capability probe (PTZ, talk, etc.). Cache result.         |
| `battery_info()`                      | Returns `BatteryInfo`. **`voltage` is i32 millivolts**, not volts. |
| `listen_on_motion()`                  | Returns `MotionData` with `next_motion()` async stream.   |
| `listen_on_floodlight()`             | Returns `mpsc::Receiver<FloodlightStatusList>`.           |
| `is_floodlight_tasks_enabled()`      | Polls schedule state — distinct from on/off.              |
| `get_pirstate()`                      | Returns `RfAlarmCfg`. One-shot query, NOT a stream.       |
| `pir_set(bool)`                       | Toggle PIR sensor.                                        |
| `send_ptz(Direction, f32)`            | Always pair with `Direction::Stop` (see Control commands). |
| `set_floodlight_manual(bool, u16)`    | Second arg is duration **seconds**. Use 30.               |
| `zoom_to(u32)`                        | Pre-multiply by 1000 (`level * 1000.0` for MQTT float).   |
| `set_ptz_preset(u8, String)`          | Preset ID + name.                                         |
| `start_video(kind, channel, record)` / `stop_video(kind)` | Returns `StreamData`; covered by `VideoStream` trait. |
| `get_snapshot()`                      | JPEG; each call wakes battery cameras — use sparingly.    |

### Calls that can hang

Every async `BcCamera` method can hang indefinitely if the camera drops the TCP connection without sending RST. Always wrap in `tokio::time::timeout`:

| Operation         | Deadline |
|-------------------|----------|
| Connect           | 100 s (backstop; the discovery retry loop self-terminates in ~80 s) |
| Keepalive         | 5 s      |
| Pollers           | 10 s     |
| Control commands  | 30 s     |
| Logout            | 5 s      |

### Calls that spawn internal tasks

`listen_on_motion()` and `listen_on_floodlight()` spawn internal tokio tasks inside `bairelay_neolink_core` that hold references to the `BcConnection`. Dropping the outer `BcCamera` (Arc refcount → 0) does not clean them up immediately — they're parked on channel reads.

**Shutdown sequence (per session, inside `CameraHandle::teardown_session_tasks`):**

`run_connected_session` is the per-session lifecycle coordinator: it sets up via `spawn_session_tasks`, runs the keepalive loop, then on connection loss / shutdown cancels `session_cancel` and hands off to `teardown_session_tasks`, which performs:

1. Wait up to **2 seconds** for `JoinSet` tasks (motion / battery / floodlight / PIR / preview-aggregator) to exit gracefully.
2. After the deadline, call `abort_all()` on the `JoinSet`.
3. `stop_all_stream_sources().await` — fires cancel + awaits each reader task with a 7 s budget. Reader tasks hold their own `Arc<BcCamera>` clones outside the `JoinSet`; without this await the next session's `start_video` can race a still-running previous reader.
4. `logout()` with a 5 s timeout (`LOGOUT_TIMEOUT`). Uses the local `concrete: Option<Arc<BcCamera>>` parameter — the trait surface intentionally omits `logout()`.
5. Clear `bc_camera` + `bc_camera_concrete`, drop the local `concrete` Arc, transition to `Disconnected`.

Without the abort deadline, shutdown hangs because internal tasks hold references that prevent cleanup. Without step 4's await, a detached reader keeps `Arc<BcCamera>` alive for up to its own `STOP_VIDEO_TIMEOUT` (5 s), which bairelay_neolink_core can't reliably handle when the next `BcCamera::new` is already in flight.

### XML parsing brittleness

Reolink firmware routinely adds and removes XML fields between model generations. `crates/core/src/bc/xml.rs` carries `#[serde(default)]` on every field that could plausibly be omitted. When a new firmware surfaces a parse error in production, the fix is almost always one of:

- Add `#[serde(default)]` to the missing field in the relevant struct.
- Add a regression test capturing the new XML payload shape in `crates/core/src/bc/xml_tests.rs`.

Inbound deserialisation tolerates absent fields; outbound serialisation still writes the field with its current value, so there's no wire-level regression for messages we send.

### Error variants of note

- `MissingAbility` — camera doesn't expose the feature. Maps to CLI exit code 6 ("unsupported"), distinct from a real protocol bug. Display: `camera does not support 'X': requested Y permission, has Z`.
- `UnintelligibleReply { reply, why }` / `UnintelligibleXml { reply, why }` — camera replied but the bytes don't parse. Display surfaces `why` (`unexpected camera reply: {why}`).

### `ptzMode = "none"` is fixed-mount, not PTZ

Reolink fixed-mount cameras (e.g. Argus Altas) report `<ptzMode>none</ptzMode>` in the support XML — non-empty but explicitly NOT a motorised mount. A naive `is_empty()` check classifies them as PTZ-capable and HA discovery emits four phantom Pan buttons. The single point of truth is `crate::capabilities::ptz_mode_indicates_ptz`: `none` / `0` / `false` (case-insensitive) and empty all return `false`. Real motorised cameras advertise `pt`, `ptz`, `3d`. Note this only stops *new* publications — leaked retained discovery topics for cameras previously misclassified must be cleaned up manually with `mosquitto_pub -r -m ""`.

### Status LED and IR LEDs are separate features

The **status LED** (small indicator light; `status/led`, `control/led`) and the **IR night-vision LEDs** (`status/ir`, `control/ir`) are unrelated hardware. A camera can have either, both, or neither. They only share a wire message: one `get_ledstate()` reply (`<LedState>`) carries `lightState` (status LED: `open` / `close`) and `state` (IR mode: `auto` / `open` / `close`).

The reply says nothing about presence — a camera answers with a placeholder for hardware it lacks (an Argus with no IR LEDs reports `state=close`; an IR camera with no status LED reports `lightState=open`). Presence comes from two independent signals, resolved at connect by `capabilities::led_presence` into a three-state `Presence` (`Present` / `Absent` / `Unknown`) per feature:

- `status_led` — `<Support>` item `ledCtrl` bit 0 (`capabilities::LED_CTRL_STATUS_LED`). Captures: Argus `0x4047` (has one), IR camera without one `0x40`, E1 Zoom `0x1`. No item for the channel, or no `ledCtrl` → `Unknown`.
- `ir_leds` — the `ledState` ability (`capabilities::IR_LEDS_ABILITY`) via `CameraDriver::ability_advertised`, a cached lookup returning `None` when the login-time `abilityInfo` was never received (account / cloud cameras answer msg 151 with 421) → `Unknown`. Confirmed on two IR cameras (listed) and two Argus without IR (absent); an IR-equipped model whose firmware omits `ledState` would be classed `Absent` — revisit if one turns up.

Behaviour per state:

| | Present | Unknown | Absent |
|---|---|---|---|
| HA entity (LED switch / IR select) | with `state_topic` | without `state_topic` (pre-detection behaviour) | not emitted |
| `status/led` / `status/ir` published | yes | no (value may be a placeholder) | no |
| Retained config + status cleared on discovery publish | no | no | yes (`absent_hardware_retractions`) |

`Unknown` never hides or retracts anything, so a failed or missing probe can't delete an entity. `get_ledstate` / `set_ledstate` carry no ability gate (the old `ledState` gate blocked the status LED on Argus). `camera_tasks::publish_led_state` reads once on connect, only when a feature is `Present`, and publishes only `Present` fields (retained, cached for broker-reconnect replay; the cache field is dropped when the feature turns out `Absent`). `control/led` / `control/ir` always reach the camera; on success the binary re-reads and publishes only the written field, only when `Present`. A per-camera lock serialises these read-modify-write commands. Like PIR, both are configuration values, not live sensors — no polling. `status/ir` is the configured mode; the live day/night switch arrives separately on the cmd 33 alarm push. Don't confuse IR with PIR (passive IR motion sensor), which has its own `get_pirstate` reader.

### Time + DST split across two Bc messages

Reolink cameras autonomously track DST in a Bc message *separate* from `<SystemGeneral>`. Setting the clock requires modelling both. Load-bearing semantics:

- `msg_id = 104` (`GetGeneral`) `<SystemGeneral>`: `<timeZone>` is the **base UTC offset in seconds, Reolink-inverted** (positive = west of UTC, negative = east, **DST excluded**). `<hour>` is **UTC**, not local. `<year>`/`<month>`/`<day>`/`<minute>`/`<second>` are likewise UTC.
- `msg_id = 106` (`GetDst`) `<Dst>`: `<enable>` (0/1), `<offset>` (hours), plus `<startMonth>`/`<startWeekIndex>`/`<startWeekday>`/`<startHour|Minute|Second>` and the symmetric `<end…>` family. Camera applies DST internally on display: `displayed_local = <hour> + (-<timeZone>/3600) + dst_offset_if_in_window`.
- `msg_id = 105` (`SetGeneral`) accepts the same `<SystemGeneral>` shape. Because the camera double-applies DST otherwise, a SET that wants to land at the host's current local time must compute `<timeZone>` as the host's **base** offset (subtract DST if the host is currently in DST) and `<hour>`/etc. as **UTC**. Sending host-local-effective-offset + host-local-wallclock causes a `+dst_offset` drift.
- `msg_id = 107` (presumed `SetDst`) is not currently observed on the wire — the Reolink Mac client only reads DST. Don't write DST unless the operator explicitly asks.

`<SystemGeneral>` also surfaces `<deviceId>`, `<loginLock>`, `<lockTime>`, `<allowedTimes>` on current Argus firmware; these are **not** modelled in `crates/core/src/bc/xml.rs::SystemGeneral` and therefore aren't preserved by the read-modify-write in `set_time`. A SET silently round-trips them to default. Either model them or accept the round-trip.

---

## Wake lock and grace period

The wake-lock counter (`src/wake_lock.rs`) needs **two separate `Notify` instances**:

```rust
struct WakeLockInner {
    count: AtomicUsize,
    notify_release: Notify,   // 1 → 0
    notify_acquire: Notify,   // 0 → 1
}
```

- `notify_acquire` fires on 0 → 1 transition (idle-disconnect cameras need this to wake up when work arrives).
- `notify_release` fires on 1 → 0 transition (the grace-period timer listens for this to start its countdown).

A single-Notify design loses the acquire edge: a wakeup command would acquire the lock but the sleeping camera's `notified().await` (looking for release) would never fire.

**`notify_one()` not `notify_waiters()`.** Both transitions use `Notify::notify_one()`, which stores a single permit when no waiter is registered. A late `.notified().await` therefore still fires. `notify_waiters()` would drop the edge if nobody is currently parked, producing a TOCTOU where an idle-disconnect camera that has logged "waiting for wake lock…" but has not yet polled `wait_for_acquire()` would miss startup-wake's acquire and park forever.

**Stale-permit drain in `wait_for_acquire`.** `notify_one`'s stored permit closes the TOCTOU above but lingers across sessions: a permit set by a probe's 0→1 acquire stays in the slot until consumed. When grace fires and the run loop loops back to `wait_for_acquire().await`, the stale permit resolves the future immediately even though `count == 0` — the camera reconnects with no actual wake-lock holder, the new session has nobody keeping it alive, grace fires again 30 s later, repeat. The fix re-checks `is_idle()` after each notification and parks again on a stale permit; only return when the count is actually held.

**`idle_since` timestamp for the watchdog gate.** `WakeLockCounter` records the wallclock instant of the most recent 1→0 release in a `Mutex<Option<Instant>>`. Cleared on the next 0→1 acquire. The watchdog reads `idle_since().elapsed() >= grace` instead of the instantaneous `is_idle()` it used to check — without the elapsed gate, the watchdog tick races every freshly-arrived `control/wakeup` MQTT publish and can tear a session down a few hundred ms before the wakeup acquire lands. With the gate, `grace_period.rs` handles the normal disconnect path and the watchdog is the safety net its docs always claimed (only fires if grace_period failed to fire, e.g. didn't get spawned or panicked).

The grace period timer is reset by any new `acquire()` before its countdown expires. Default grace is 45 s, configurable via `idle_disconnect_timeout_secs`. Keep this strictly above `stream_prune_grace_secs` (default 30 s) so a cached `StreamSource` can't outlive its Baichuan session.

---

## MQTT bridge

`rumqttc::AsyncClient` is `Clone`. The bridge creates one MQTT connection in `main`, clones the `SharedMqttClient` into each per-camera task, and polls the `MqttEventLoop` in a single background task.

### Subscription strategy

Subscribe to ALL camera topics on `ConnAck` (broker connect/reconnect), **not** when individual cameras connect. Critical: sleeping cameras need to receive `control/wakeup` commands before they can come online.

### Event loop must not block

Control dispatch is spawned as a separate `tokio::spawn` task from the event loop. Dispatching inline would let a 10-second PTZ command block all MQTT processing.

### `rumqttc` 10 KiB packet-size default

`rumqttc 0.24`'s `MqttOptions` defaults `max_incoming_packet_size` and `max_outgoing_packet_size` to **10 KiB** — surprising given MQTT itself has no such default. Any new `MqttOptions` instance must call `set_max_packet_size` (we use 16 MiB) **before** the first JPEG flows.

The error rumqttc emits when the cap is hit reads `broker's maximum packet size of '10240'` — misleading; the cap is client-side. Audit this on every rumqttc bump.

### Last Will limitations

MQTT supports only one Last Will per connection. The bridge sets a global LWT on `{topic_prefix}/status` with payload `"offline"`. Per-camera status is published as `"disconnected"` during graceful shutdown. On crash, only the global LWT fires — per-camera topics retain stale `"connected"` until the next restart.

### Topic prefix

`mqtt.topic_prefix` config knob (default `"bairelay"`, drop-in compat `"neolink"`) drives every owned topic path and every HA discovery `identifier` / `unique_id`. Validation rejects empty / non-alnum-underscore- hyphen values at startup.

### Shutdown ordering

The MQTT event loop is the **last subsystem to shut down**. The shutdown-signal handler (`src/shutdown_signal/`: Ctrl+C everywhere, plus SIGTERM on Unix — what systemd, Docker and the HA app's s6 send) cancels the global token (cameras, RTSP, watchdog, startup-wake); each per-camera teardown publishes its final `connection_state = false`; only after `orchestrator.run().await` returns does `main()` cancel a separate `mqtt_cancel: CancellationToken` and await the event-loop task with a 2 s timeout against a wedged broker.

Without the separate token, per-camera teardown races the event-loop exit and hits `Failed to send mqtt requests to eventloop`.

**Shutdown budget.** With cameras connected a clean exit takes well under 1 s. Every session task and camera-lifetime poller must exit as soon as its cancel token fires — including a camera request already in flight (`camera_tasks::cancellable`) and the grace-period timer (`camera::grace_period_watch`). A task that ignores cancel costs a full drain window (2 s per session teardown, 2 s for the preview poller) on every shutdown and every battery-camera sleep. SIGTERM handlers are registered synchronously at startup (`shutdown_signal::requested`), so there's no window where the default kill action still applies. Tokio never uninstalls its handlers, so after the first signal `shutdown_signal::force_exit_on_next` arms a second listener: a second Ctrl+C / SIGTERM during a wedged teardown exits immediately with 130 instead of being swallowed.

---

## Keepalive

Use `BcCamera::get_linktype()`, **not** `BcCamera::ping()` — matches neolink. Some cameras return `UnintelligibleReply` for link-type queries: treat that as success (camera is alive but doesn't support the query), not failure.

Allow 5 consecutive failures before disconnecting. A single dropped packet shouldn't kill the connection.

Cadence shared between production and tests via `KEEPALIVE_INTERVAL = 5 s` and `KEEPALIVE_MAX_FAILURES = 5` constants in `src/camera.rs`. Two decision-table helpers are exported for testing under a paused virtual clock:

- `classify_keepalive_tick(result)` — folds `Ok(Ok)` / `UnintelligibleReply-still-ok` / `Ok(Err)` / `Elapsed` into a `KeepaliveTickOutcome::{Ok, Failed}` binary outcome.
- `advance_keepalive_counter(state, outcome, max_failures) -> (next_failures, should_break)` — drives the consecutive-failure counter (reset on Ok, saturate at max_failures, signal break at exactly `next == max_failures`).

---

## Pollers

Floodlight and PIR default to **disabled**:

```toml
enable_floodlight = false
enable_pir = false
```

Battery and motion default to enabled. Floodlight and PIR default to disabled so cameras without those features don't generate unsupported-feature errors at connect.

`enable_battery` defaults to `true`, so mains-powered cameras get polled and refuse every probe. `battery_poller` gives up after `BATTERY_MAX_UNSUPPORTED` (3) consecutive refusals *and only if the camera never once answered*, latching the sticky `battery_unsupported` flag that `spawn_session_tasks` checks before respawning it.

Why the guards: `battery_info` raises `UnintelligibleReply` for any reply that isn't a 200 with a payload, so a garbled 200 is indistinguishable from a refusal — one success therefore grants permanent immunity. Transient errors and `MissingAbility` reset the streak (a missing ability key doesn't prove missing hardware). The ticker is `Delay` with the first tick dropped; the default `Burst` would fire the streak's probes back-to-back after any stalled poll.

`[mqtt.discovery].features` gates only the HA entity, never the poller — `[cameras.mqtt] enable_battery` is the sole knob for that, and the only one that also removes the battery entity left at "Unknown" after a give-up.

PIR state (`get_pirstate()`) is a configuration query, **not** a live sensor — it changes only when explicitly set via `control/pir`. Publish once on connect, re-publish after control commands. Do NOT poll periodically.

Floodlight has two aspects on separate topics:

- `status/floodlight` — actual on/off state, published via `listen_on_floodlight()` event stream.
- `status/floodlight_tasks` — schedule enabled/disabled, published via periodic `is_floodlight_tasks_enabled()` polling.

These are distinct topics. Don't conflate them.

Motion likewise has an aggregate topic and four refinements:

- `status/motion` — any alarm active, from `MotionStatus::Start` / `Stop` (or the push listener).
- `status/motion/{person,vehicle,animal,other}` — the `AiDetections` carried on each `Start`, parsed from the alarm's comma-separated `AItype` (`people`, `vehicle`, `dog_cat`, `other`). `other` also covers an alarm with no recognised class, so an active alarm always lights at least one class. Every `Start` republishes all four (the camera re-sends the alarm as its classification changes); `Stop` turns all four off.

Update intervals (`battery_update`, `preview_update`, `floodlight_update`) must be ≥ 500 ms. Validation rejects shorter.

---

## Control commands

Match neolink's wire behaviour exactly:

- **PTZ.** Send the move command with speed = 32.0, sleep `amount / speed` seconds (clamped to 0–10 s), then **always** send `Direction::Stop`. Without the stop, the camera keeps moving forever.
- **Zoom.** MQTT payload is a float (e.g. `1.5`). Call `zoom_to((level * 1000.0) as u32)`.
- **Siren.** `bc.siren()` takes no arguments. Only fire on `"on"` payload; ignore `"off"`.
- **Wakeup.** Acquires a wake lock immediately (triggers the camera connection), then waits for the camera to actually connect before starting the countdown. 2-minute connection timeout, respects the cancellation token.
- **Reply convention.** After every control command, publish `"OK"` or `"FAIL"` (non-retained) to the same control topic. Enables HA automations to confirm command success.
- **Query responses.** `query/battery` and `query/pir` serialise the full struct to XML via `quick_xml::se::to_writer` and publish to `status/battery` and `status/pir` respectively (non-retained).

---

## Wire-format gotchas

### ADPCM ser/de asymmetry

`BcMedia::Adpcm` does **not** round-trip byte-exact through `serialize` → `deserialize`. The serialiser pads on `data.len() % 8`; the deserialiser pads on `(data.len() + 4) % 8` (the wire `payload_size` includes the 4-byte sub-header). The capture pipeline writes raw camera-side bytes through a single `serialize` per packet, so on-disk `.bcmedia` files parse cleanly. **Do not** re-serialise captured packets to "normalise" them — the first ADPCM packet desyncs the loop.

### HEVC NAL whitelist (`is_decodable_nal`)

`crates/rtsp/src/codec/nal.rs::is_decodable_nal` is the single point of truth for "what NAL units a standard receiver expects". Outbound `Frame::Video` is filtered through it in `handle_iframe`, `handle_pframe`, and the bridging-replay path.

- **H.264**: `nal_unit_type` in `1..=13` (VCL slices 1..=5, SEI 6, SPS 7, PPS 8, AUD 9, EOS 10, EOB 11, FD 12, SPS-ext 13). Drops type 0 and 14..=31.
- **H.265**: type in `{0..=9, 16..=21, 32..=40}` AND `nuh_layer_id == 0`. Keeps standard VCL (0..=9), IRAP keyframes (16..=21), VPS/SPS/PPS/AUD/EOS/EOB/FD/SEI (32..=40). Drops reserved 10..=15 and 22..=31, reserved non-VCL 41..=47, and unspecified 48..=63 — Reolink Argus emits `UNSPEC62` as proprietary metadata; ffmpeg's RTP-HEVC depacketizer rejects it. Drops anything with `nuh_layer_id != 0` (ffmpeg logs `Multi-layer HEVC coding is not implemented`).

In addition to the whitelist, outbound `Frame::Video` strips VPS / SPS / PPS (H.265) and SPS / PPS (H.264) on every keyframe. SDP `sprop-vps` / `sprop-sps` / `sprop-pps` fmtp attributes already carry them out-of-band and every practical client consumes them during DESCRIBE / SETUP. The strip is load-bearing for HA + go2rtc: putting VPS + SPS + PPS + IDR at the same RTP timestamp would let HA's `ffmpeg:` re-publish aggregate the three small NALs into an HEVC Aggregation Packet (NAL type 48 per RFC 7798 §4.4.2); go2rtc's RTPDepay passes the AP through opaquely and its `/api/frame.jpeg` transcoder exits with status 183.

### HE-AAC AOT branches

`handle_aac` adds 1024 ticks for AAC-LC (AOT=2), 2048 for HE-AAC (AOT=5) and HE-AACv2 (AOT=29), drops + one-shot-warns for unsupported AOTs. The ADTS path can only yield AOT ∈ {1, 2, 3, 4} (2-bit profile field), so AOT=5 / AOT=29 branches are reachable only by direct `aac_samples_per_au` calls — kept defensively in case a non-ADTS carrier ever feeds `handle_aac`.

---

## Audio + video pacers

`bairelay::stream_source::media_pacer_task(rx, broadcast, cancel, max_lead, initial_latency, snap_on_past)` is the generic pacer; `audio_pacer_task` and `video_pacer_task` are thin wrappers. Each per-source `StreamSource` spawns one of each.

Pacer contract:

- Items are `PacedFrame { frame: Frame, duration: Duration }` pushed via bounded `mpsc::Sender::try_send` from the audio / video handlers (overflow logs once and drops the new packet — preserves recent audio).
- The pacer maintains an absolute emit cursor `next_emit_at`. Each iteration:
  - Receives an item.
  - Computes `target = next_emit_at`. Re-anchor on two conditions: (1) `target > now + max_lead` (cursor too far in the future — queue overflow / startup burst) — always snap to `now`; (2) `target < now` AND `snap_on_past = true` (cursor in the past — queue ran dry while upstream was idle) — snap to `now`.
  - If `target > now`, sleep until target. Else (`target == now` after snap, or past with snap disabled) emit immediately.
  - Broadcasts the frame.
  - Sets `next_emit_at = target + item.duration` — absolute, NOT `now_after + duration`. Per-iteration scheduler overhead would otherwise drift the cursor forward by ~1 ms per frame and the long-term slope would diverge from `clock_rate`.

Audio: `max_lead = 2 s`, `initial_latency = 500 ms`, `snap_on_past = true`. AAC-LC AU duration = `1024 / sample_rate` (= 64 ms at 16 kHz). G.711 µ-law = `samples.len() / 8 000`. The `aac_frames` field of the ADTS header (parsed from `number_of_raw_data_blocks_in_frame + 1` per ISO 13818-7 §6.2) multiplies the per-AU sample count when a stream ever packs multiple frames per ADTS packet.

The `snap_on_past = true` choice for audio is load-bearing. Without it, the audio pacer would keep its absolute anchor across upstream idle periods, so when the queue ran dry between Argus's GOP-aligned audio bursts the next iteration's `target` would be hundreds of ms in the past — the pacer would fire the entire just-arrived burst onto the wire back-to-back (zero sleep_until) until the cursor caught up. Receivers like mpv reported `audio end or underrun` every ~10–20 s with audible 0.1 s glitches under that earlier behaviour. Snapping the cursor forward on past-cursor restores 64 ms-spaced wire output even after a silence; the 500 ms `initial_latency` cushion absorbs the BcMedia decoder jitter that arises because the same TCP stream carries 4 K HEVC keyframes ahead of audio packets in the camera-side serialisation. On real Argus: per-RTP-packet inter-arrival p50=64.0 ms / p99=65.1 ms / max=207 ms over 60 s, 0 mpv underruns over 3 min playback.

Video: `max_lead = 3 s`, `initial_latency = 1.5 s`, `snap_on_past = false`. Argus 4K-HEVC bursts each GOP in ~900 ms then idles ~1.1 s; the 1.5 s startup buffer keeps the per-source mpsc queue stocked across each upstream silence so the wire cadence stays at PTS rate continuously, and the cursor never lands in the past in practice. Per-frame duration = `(pts_90khz - state.last_video_pts_90khz) / 90 000`; the very first frame's duration is `Duration::ZERO` and the initial-latency wait dominates. Burst-drain on past-cursor (the policy with `snap_on_past = false`) preserves the long-term wallclock-PTS slope at exactly `clock_rate`, which matters more for video than for audio because per-frame PTS continuity drives downstream re-muxers' DTS reasoning.

### Pacer vs gap-bridging — division of labour

Two mechanisms cooperate to keep the wire stream usable, addressing different scales of upstream irregularity:

- **Audio + video pacers (above)** absorb sub-second bursty delivery (Argus's normal "GOP burst then 1.1 s idle" pattern). The video pacer's 1.5 s pre-buffer means a typical idle period is invisible to the receiver.
- **Gap-bridging** (`GapState::Bridging`, `emit_replay_frame_if_bridging`, the 200 ms ticker in the translator loop) handles **multi-second upstream silences** — camera reconnects, wifi hiccups, motion-pause flapping. When `last_live_frame_at` exceeds `gap_threshold_secs` (default 3 s) the source flips to `Bridging` and re-broadcasts the cached `VideoBurst::iframe_nals` with a synthesised PTS, plus drops live audio so A-V stays correlated on resume. Bridging writes to `broadcast::Sender` directly, bypassing the pacer.

The 3 s threshold sits above the pacer's 1.5 s buffer — a normal inter-burst gap drains the pacer before bridging fires, but the pacer covers it; only a true camera-side silence reaches bridging. With the pacer in place, bridging rarely fires day-to-day; it's still load-bearing for the "show the last frame instead of a buffering spinner" experience during real silences.

## Per-kind session dispatch

`crates/rtsp/src/server/session_task.rs::run` is a small coordinator. After the PLAY gate it spawns two child tasks — `video_dispatch_loop` and `audio_dispatch_loop` — each holding its own `broadcast::Receiver` (via `resubscribe()`). Each child loops on `frames.recv()`, skips frames of the wrong kind, lazily resolves its `RuntimeTrack` from `session_tracks` on track-miss, and writes RTP packets via the track's transport. The coordinator then awaits cancel + the JoinSet's first completion; either path cascades cancel to the surviving child, awaits both joins, closes transports, and removes the session.

Why split: the unified loop the per-kind dispatchers replaced consumed `Frame::Video` and `Frame::Audio` from a single `tokio::select!` arm. A 4 K HEVC IDR (~150–370 RTP packets after FU fragmentation) monopolised the dispatch task's TCP-write loop while audio frames piled up in the broadcast queue; receivers saw audio drain in a burst once the video write completed. Splitting into two parallel consumers means the audio loop's `frames.recv()` is always free to fire, and the per-packet TCP-interleaved write mutex (held only for one `$-framed` packet at a time, ≤ MTU) lets audio FU fragments interleave between video FU fragments at packet granularity.

Audio bursting on the wire ALSO needed the audio pacer's `snap_on_past` fix above — the dispatch split alone is necessary but not sufficient.

Asymmetry: the video child takes ownership of the original `frames` receiver passed into `run()` (preserving its read position so any pre-PLAY buffered frames + lag detection both flow through it), while the audio child gets a fresh `resubscribe()`. Audio losing a few buffered frames at session start is benign — receivers expect to start at "now" anyway.

`tracks_changed` is plumbed through `run`'s signature for ABI compat but the children don't subscribe to it. Late-SETUP append picks up the new track via the per-child lazy resolution on the next matching-kind frame; using `notify_one` for two waiters would race (only one wakes per call).

## RTCP

The session task does not emit periodic Sender Reports. The receiver falls back to RTP-arrival-time A-V sync, which is what live camera feeds actually need at our latency targets — the audio + video pacers hold the wire cadence at `clock_rate` and the receiver derives slope from successive RTP packets directly.

`build_sender_report`, `ntp_now`, and `ntp_minus` are kept in `crates/rtsp/src/server/rtcp.rs` so any future SR-emitting context (e.g. a recording sink that needs precise NTP↔RTP) is one wire-up away. The SR ticker arm has been removed from `run` (per-kind dispatch coordinator no longer hosts a `select!`); an SR-emitting future would spawn its own ticker task.

---

## Authentication

If `login_with_maxenc()` returns an authentication error (`AuthFailed`, `CameraLoginFail`, `Credential error`), **stop the retry loop permanently**. Don't hammer the camera with bad credentials every 2 – 60 seconds forever.

Two distinct rejection shapes hide behind that one outcome, and both log a `warn` in `crates/core/src/bc_protocol/login.rs` naming which one fired: `CameraLoginFail` (modern login answered with a non-200 `response_code` — the code is included in the warn) and `AuthFailed` (200 with an empty body, no `DeviceInfo`). The binary's `Authentication failed` error line includes the full error chain, so service logs distinguish them too. The one-shot CLI prints the chain via `error: {:#}` — `bairelay battery <cam>` is the cheapest way for an operator to surface the variant.

Reconnect on transient network failures still uses `ReconnectBackoff::sleep_with_cancel` (initial 2 s, doubling, capped at 60 s) — the same path production uses and the `drive_reconnect_with_backoff` test seam exercises.

---

## Configuration

### Validation rules

- Camera names: `[A-Za-z0-9_-]` only (MQTT topic safety).
- Password: required (`None` rejected).
- `bind_port = 0` rejected.
- Update intervals (`battery_update`, `preview_update`, `floodlight_update`): ≥ 500 ms.
- `idle_disconnect`: defaults `false`. Must be explicitly enabled for battery cameras.
- `gap_threshold_secs`: must be finite + positive.
- `idle_disconnect_timeout_secs`: must be finite + positive.

### Neolink-compat fields

`PauseConfig` accepts `on_motion` / `on_client` / `on_disconnect` / `motion_timeout` / `mode` as `Option<_>` for drop-in compat. They warn at startup and have no effect.

`pause.timeout` is a soft alias for `idle_disconnect_timeout_secs` (camera-level). Precedence: explicit `idle_disconnect_timeout_secs` wins; otherwise `pause.timeout` (with warn); otherwise default 45 s. Resolution happens once at session entry via `config::resolve_idle_disconnect_timeout`. The default sits 15 s above `stream_prune_grace_secs` (30 s) — see the invariant on `Config::stream_prune_grace_secs`.

`#[serde(deny_unknown_fields)]` on all config structs makes typos and truly-unknown keys fail at startup.

---

## Logging

Default filter: `info` for bairelay, `warn` for `rumqttc`. Override with `RUST_LOG=bairelay=debug`. `rumqttc`'s debug output is extremely noisy (multiline ping messages every 30 s) — suppress it unless specifically debugging MQTT protocol issues.

Camera UIDs in info / warn lines go through `bairelay_neolink_core::redact::UidTail` — `*` padding plus the last 6 characters, length preserved so short-form and firmware-suffixed long-form UIDs stay distinguishable. The full UID is logged at debug alongside. Info logs get pasted into bug reports; debug is the diagnostic level. Any new info / warn UID log uses the same wrapper.

All MQTT publishes log their values at debug level. All control commands log dispatch and OK / FAIL result.

`-v` / `-vv` / `-vvv` on the CLI maps to `info` / `debug` / `trace` via `run_support::cli_output_mode`.

Per-camera `debug = true` (`verbose` is the neolink alias) sets `BcCameraOpt::debug`, which unlocks the Baichuan codec's trace-level dumps of decrypted control payloads (`Payload Txt:` / `Extension Txt:` in `crates/core/src/bc/de.rs`) — combine with `RUST_LOG=info,bairelay_neolink_core::bc::de=trace`. The leading `info,` is load-bearing: a bare `RUST_LOG=bairelay_neolink_core::bc::de=trace` is a single-target allowlist that disables the default `info` baseline, so every normal line (Connecting, discovery, the `response_code` warn) vanishes and the console looks empty — and if `debug = true` is unset on top of that, even the `bc::de` target emits nothing, so the console is genuinely silent. Keep a baseline level before the comma; `-vvv`'s global `trace` also works (it enables everything). The dumps include credential hashes and camera UIDs; `warn_wire_debug_enabled` reminds the operator at startup. Use for wire-level diagnosis (login rejections on new firmware, unmodelled XML), then turn it back off.

**One-shot stdio writers are plain handles, never `.lock()` guards.** `run_support::run_oneshot` passes `std::io::stdout()` / `std::io::stderr()` into `run_oneshot_to`; the plain handles lock per `write` call. The one-shot future is driven by `block_on` on the main thread and parks whenever camera I/O is in flight, so a `StdoutLock` / `StderrLock` guard held across those awaits deadlocks the whole runtime the moment a spawned task emits a log record: the tracing fmt writer blocks on the stderr lock owned by the parked main thread, the blocked worker never finishes its poll, and no other worker re-acquires the IO driver — socket readiness and timers stop firing process-wide. Stdio locks are reentrant, so records logged from the main thread itself keep working and mask the bug; the first record from a worker-thread task (e.g. the BcUdp discovery reader's trace-level decode dumps) trips it. Tests still inject `Vec<u8>` writers through the `_to` seam.

---

## Test infrastructure

Test helpers in `bairelay_neolink_core` (`FakeCameraBuilder`, `MockConnection`, `BcCamera::from_mock_connection`, `BcCamera::test_set_ability`, `MotionData::test_new`) are gated behind a `test-util` Cargo feature so release builds cannot accidentally substitute a fake for a real camera. The crate's own `cfg(test)` unit tests always see them; downstream test crates opt in via `[dev-dependencies] bairelay_neolink_core = { ..., features = ["test-util"] }` (the binary already does). Helpers in `bairelay_mqtt` (`mock_client()`) and the binary (`PacketSource`, `CameraHandle::set_driver_for_test`, `StreamSource::start_with_packet_source`) compile unconditionally — they're scoped tightly enough that there's no production-leakage risk. `ScriptedDiscoverer` (core) and `MockVideoStream` (binary, `src/oneshot/snapshot.rs`) are `cfg(test)`-only, visible to their own crate's unit tests.

### `FakeCameraBuilder` (`bairelay_neolink_core::bc_protocol::fake_camera`)

Closure-per-method `CameraDriver` impl:

```rust
let fake = FakeCameraBuilder::new()
    .with_battery_info(|| Ok(info))
    .with_snapshot(|| Ok(jpeg_bytes))
    .with_motion_stream(motion_data)
    .build();
let driver: Arc<dyn CameraDriver> = fake.clone();
```

Side-effecting setters (`reboot`, `pir_set`, `set_floodlight_manual`, etc.) record their args in a public `FakeCalls` struct so tests assert dispatch:

```rust
assert_eq!(*fake.calls().reboot.lock().unwrap(), 1);
assert_eq!(fake.calls().pir_set.lock().unwrap().clone(), vec![true]);
```

Unset reads panic with `FakeCamera: <method> not configured for this test` via a single `unset(method) -> !` helper. `*_pending()` builders return a never-resolving future for testing 30 s timeout error branches under `tokio::time::pause`.

### `MockConnection` (`bairelay_neolink_core::bc_protocol::connection::mock`)

Scriptable request → reply harness for testing `BcCamera` command modules without a real socket:

```rust
let mock = MockConnection::new()
    .expect_msg(MSG_ID_BATTERY_INFO)
    .reply_with(|req| reply_200_xml(req, BcXml { ... }))
    .build()
    .await;
let cam = BcCamera::from_mock_connection(mock).await;
let info = cam.battery_info().await?;
```

Three reply helpers:

| Helper                              | Use                                          |
|-------------------------------------|----------------------------------------------|
| `reply_200_xml(req, xml)`           | Standard 200 OK with `BcXml` payload.        |
| `reply_200_empty(req)`              | 200 OK, no payload (reboot, setters).        |
| `reply_err_code(req, code)`         | Non-200 response code (error-path tests).    |

`reply_with_many(|req| vec![Bc, Bc, ...])` scripts multi-reply exchanges (e.g. snap.rs binary chunks). `reply_none()` drops the request silently — emulates a camera that ack'd via msg-id match but doesn't answer; pair with `tokio::time::timeout(Duration::from_millis(200), op)` so the test doesn't hang. `MockInjector` (`mock.injector()`) supports unsolicited inbound messages for `subscribe_to_id` push-semantics tests.

`reply_with_xml(|req, xml| ...)` and `reply_with_xml_opt(|req, xml| ...)` deserialise the request's `BcXml` payload and hand a borrowed reference to the closure before it builds the reply. Use these for set / control tests that need to pin the wire-shape of the request payload — without them, a regression that mapped (e.g.) `LightState::On` → `"close"` instead of `"open"` would still pass a shallow `reply_200_empty` test. Panics if the request is header-only or carries a binary payload — those shapes are caller errors when the test is trying to inspect a set/control request.

### `bairelay_mqtt::test_support::mock_client()`

Returns `(SharedMqttClient, MockHandle)`. The handle exposes:

- `published()` — `Vec<(topic, payload, retained)>` of all publishes.
- `published_topics()` / `published_payloads()` — projections.
- `count()` — total publish count.

The mock doesn't connect to a broker; `EventLoop::poll` is leaked at test construction time so the request channel stays alive.

### Other test seams

| Seam                                                   | Purpose                                          |
|--------------------------------------------------------|--------------------------------------------------|
| `MotionData::test_new(rx)`                             | Wire a caller-supplied `mpsc::Receiver`.         |
| `CameraHandle::set_driver_for_test(driver)`            | Install fake driver + flip state to Connected.   |
| `CameraHandle::run_connected_session_for_test`         | Drive `run()`'s Connected arm directly.          |
| `StreamSource::start_with_packet_source`               | Drive translator loop with scripted `BcMedia`.   |
| `drive_reconnect_with_backoff` + `ReconnectOutcome`    | Pure reconnect-loop timing contract.             |
| `ScriptedDiscoverer` (`test_support` in discovery.rs)  | Script local/remote/map/relay outcomes.          |
| `MockVideoStream`                                      | Drive `drain_first_iframe` with scripted frames. |

`ReconnectBackoff::sleep_with_cancel(&CancellationToken) -> bool` is shared between production `CameraHandle::run` and the test seam — the "wait for next delay, bail on cancel" contract lives in one place.

### Hang-protection discipline

Every mock-based "camera doesn't answer" error-path test wraps the operation in `tokio::time::timeout(Duration::from_millis(200), op)`. A test that awaits a channel/socket with no guaranteed sender hangs `cargo test` indefinitely.

Every test in the core crate completes in under 1 second of wall time.

---

## TLS / `rtsps://`

### Plain + TLS run as parallel listeners

When `certificate` is set, `main.rs` spawns two `RtspServer::serve` tasks — one with `tls = None` on `bind_port`, one with `tls = Some(...)` on `tls_bind_port` (default 8555). Plain stays running unless the operator explicitly opts into TLS-only by setting `bind_port = 0`. Neolink replaces plain with TLS on the same port; bairelay does not — drop-in users get plain on 8554 plus TLS on 8555.

### `rustls 0.23` crypto provider install

`rustls 0.23` requires a default `CryptoProvider` to be installed once per process before any TLS config is built. Two providers are compiled in (aws-lc-rs, plus ring via a dependency), so rustls can't choose one itself and panics on first use. `main.rs` calls `bairelay_rtsp::server::install_crypto_provider()` (aws-lc-rs) unconditionally at startup, before the MQTT client is created — rumqttc's MQTT-over-TLS connect needs it as much as the RTSPS listener does.

`install_default()` returns `Err(CryptoProvider)` if a provider is already installed; the helper ignores that behind a `OnceLock`, so repeated calls (tests sharing a process) are idempotent.

### MQTT broker TLS (`[mqtt] tls`, `ca`, `client_auth`)

`tls = true` enables TLS to the broker, verified against the OS trust store (`bairelay_mqtt::system_ca_bundle`: rustls-native-certs, PEM-encoded so it travels the same path as a `ca` file; honours `SSL_CERT_FILE` / `SSL_CERT_DIR`; unparseable entries skipped like rumqttc does, empty store is an error). `ca` replaces the system store with a private CA and on its own also enables TLS; an explicit `tls = false` next to `ca` is rejected as contradictory. On Windows rustls-native-certs reads the current-user ROOT store, which Windows fills lazily — a public root never used on that machine may be missing (set `ca` there). `client_auth = [cert, key]` adds mutual TLS and requires TLS on. Each value is a file path (Neolink form) or inline PEM, recognised by a `-----BEGIN` marker (`tls_load::read_pem_source`). `tls_load::load_mqtt_tls` reads everything and runs the checks rumqttc's rustls setup would — each `ca` certificate loads into a root store; the `client_auth` key loads and matches the leaf certificate (`CertifiedKey::from_der`, explicit aws-lc-rs provider) — from `mqtt_loop::build_broker_config`, so bad material fails at startup (when the mode runs MQTT) and in `check-config`. Errors name the path, or `inline PEM` (never the PEM text).

### `TlsConfig::build` runs at startup, not on first connection

`bairelay_rtsp::server::TlsConfig` wraps `Arc<rustls::ServerConfig>`. The builder runs `with_single_cert` and (for `Request`/`Require`) `WebPkiClientVerifier::builder(roots).build()` at config-load time, so a bad cert/key pair, empty cert chain, or empty roots store fails fast with a useful operator message instead of surfacing on the first incoming connection.

### Scheme/transport mismatch returns 400

`scheme_matches_transport(uri, is_tls)` rejects a request whose URI scheme contradicts the actual transport (`rtsp://` over TLS or `rtsps://` over plain). Defends against a confused or hostile client cross-routing between the two parallel listeners.

### TLS handshake timeout: 10 s

`tokio::time::timeout(Duration::from_secs(10), acc.accept(stream))` caps each handshake. A slow or malicious client otherwise pins an accept task indefinitely. Same shape as our keepalive bound.

### Test certs

Two paths produce the same PKI shape:

- `tests/scripts/gen-test-certs.sh` produces self-signed CA + server leaf (SAN: `localhost`, `127.0.0.1`, `::1`, `host.docker.internal`) + client leaf, all valid 10 years. Output `tests/test-certs/` is gitignored. Used by `tests/scripts/manual-verify.sh --tls`.
- `crates/rtsp/tests/tls_handshake.rs` uses `rcgen` to mint ephemeral CA + leaves in-memory. `cargo test` is hermetic — no openssl or filesystem state required.

### `client_auth = "require"` rejection timing

Under TLS 1.3 with rustls 0.23, the server's "client cert required" alert can arrive either during the handshake or on the first write/read, depending on whether the client refused to send a cert in `CertificateRequest` mid-handshake. Tests accept either failure mode (`tls_handshake::require_mode_rejects_client_without_cert`).

### TOML key placement: top-level vs section-scoped

`certificate`, `tls_bind_port`, `tls_client_auth`, `tls_client_ca` are **top-level** config keys. Any tooling that builds a `config.toml` programmatically must place them before the first `[section]` header (typically `[mqtt]` or `[[cameras]]`). TOML parses scalar key/value pairs as members of whichever table they appear under, so appending them at the end of an operator's config silently scopes them to the last table — and `[cameras.mqtt]` does not have `#[serde(deny_unknown_fields)]`, so the keys are dropped without any error message. `manual-verify.sh --tls` learned this the hard way; its awk wrapper now inserts before the first `^[[:space:]]*\[` line.

### Client-cert revocation: deliberately not checked

When `tls_client_auth = "request"` or `"require"`, bairelay verifies the client cert against `tls_client_ca` via `WebPkiClientVerifier` — chain validity, SAN/EKU, and signature. **CRL and OCSP are not consulted.** Revoking a leaked client cert is a rotate-the-server-CA operation, not a "publish a CRL entry" one. Acceptable in a single-operator home-LAN deployment; explicitly out of scope for any future fleet / multi-tenant use of the binary. Bumping to a verifier with CRL support (e.g., `rustls::server::WebPkiClientVerifier::builder(roots).with_crls(...)`) is a one-call swap if the threat model ever grows.

### PEM file size: not capped on read

`src/tls_load.rs::load_server_tls` reads the cert / CA PEM via `std::fs::read` with no upfront size cap. A pathological 10 GiB PEM would balloon RAM before parsing fails. Realistically: PEMs are operator-supplied, typically a few KiB; the file system itself caps the read by available memory. Worth a `metadata().len() > 1 MiB` guard if the binary ever ingests untrusted PEMs (CDN cert-bundle download, etc.). Not added today — would clutter the loader for a non-issue at current scope.

---

## Coverage policy

Workspace coverage is measured via `cargo tarpaulin` from project root with `tarpaulin.toml` defaults set to `workspace = true`, `all-targets = true`, `skip-clean = true`, and `fail-under = 87`. Current baseline is **~88.6 %** measured against the full workspace (no source-file exclusions). The 87 % gate is intentionally a hair below the actual baseline so natural coverage drift doesn't trip CI on every PR; raising it requires a coordinated push past the structural under-counters in `src/main.rs` (the `#[tokio::main]` body), `src/cli.rs` (clap-derive macro output), and `src/oneshot/runner.rs` (real-camera socket bind). The per-crate gate stays tighter — each non-binary crate stays ≥ 90 %; `bairelay_wake_server` sits at 94.9 % on its own.

Per-file targets:

- Pure-logic modules: ≥ 95 %.
- Command modules and pollers: ≥ 90 %.
- I/O-adjacent modules (`bc_protocol.rs::find_camera`, `camera.rs::run`, `stream_source.rs`): ≥ 85 % via test seams; the residual is real-socket bind paths.

Files at < 85 % with a documented reason:

- `src/main.rs` — `#[tokio::main]` body, signal handlers, real socket binds. Fundamentally untestable at unit level.
- `src/oneshot/runner.rs` — wraps `BcCamera::new + login_with_maxenc + logout`. Real socket required.
- `src/cli.rs` — `clap` derive macro output.
- `crates/core/src/bc_protocol/connection/{discovery,udpsource}.rs` — UDP socket internals; the trait-level `CameraDiscoverer` fallback chain is fully covered, the per-method UDP frame parsing covered partially.

Run coverage:

```
cargo tarpaulin 2>&1 | tail -5
```

(The flags above come from `tarpaulin.toml`; no extra arguments needed.)

## Wake server

The full wire-level reverse-engineering — every variant, every field, the camera boot sequence, the wake handshake, the cloud topology — lives in `docs/cloud-interception.md` § Part I. The `pushx.reolink.com` motion-push listener (the second cloud surface bairelay intercepts) is documented in § Part II of the same file. This section captures the bairelay-specific implementation notes that aren't in those references.

### Use the UDP source addr, not `<dev>` from D2R_HB

Cameras put their own LAN IP / port in the `<dev>` block of `D2R_HB`. That value is useless for sending replies back to a NAT'd camera — the public-mapped address is what the OS sees on `recv_from`. The handler in `crates/wake-server/src/register.rs::handle_heartbeat` always upserts with `src` (the address from `recv_from`), never `hb.dev`. Reference implementation calls this out explicitly; we matched it.

### Wake burst: fire-and-forget + first-error-then-break

The 10 × `R2D_C` packets are sent from a `tokio::spawn`'d task so the register loop stays responsive to other clients. The burst breaks early on the first send error rather than retrying — a vanished camera or a transient ENETUNREACH means the next heartbeat would fix the address anyway, and a tight retry loop on a stale path floods logs without changing outcomes.

### Stale-on-lookup, not background sweep

`CameraRegistry::lookup_fresh` returns `None` when `now - last_seen > stale_after`. Stale entries are read-as-missing and replied to with `R2C_C_R{rsp = -1}`; the next heartbeat resurrects the entry. Zero background scheduler overhead, identical correctness to a periodic sweep.

### Why no `core::discovery` change

Bairelay's outgoing discovery (`crates/core/src/bc_protocol/connection/discovery.rs`) targets `p2p*.reolink.com:9999` for relay/map modes via DNS. Operator-side DNS redirect makes both cameras and bairelay-itself's discovery resolve to the local box, hitting our wake server. The wake server crate is unaware of `discovery.rs`; `discovery.rs` is unaware of the wake server. Same code path for cloud and self-host — only the DNS answer differs.

### `u32` wraparound in `bcudp::xml_crypto`

`crates/core/src/bcudp/xml_crypto.rs::{encrypt, decrypt}` use `i.wrapping_add(offset)` rather than plain `i + offset` because the `offset` argument is the packet's `tid: u32` and real cameras emit transaction IDs ≥ ~`0x60000000`. A naive add overflows `u32` in debug builds (panic) for any payload long enough to push `i + offset` past `u32::MAX`. The XOR cipher is symmetric over the full `u32` domain, so wrap-around is correct semantics — encrypt-then-decrypt with the same wrapping recovers the plaintext byte-for-byte. Regression tests in `xml_crypto.rs` cover the high-tid path (`tid = 0xFFFF_FFF0` with a multi-byte payload).
