use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;

use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver};
use bairelay_rtsp::buffer::LastFrameBuffer;
use bairelay_rtsp::provider::StreamError;
use bairelay_rtsp::url::StreamKind as RtspStreamKind;

use crate::audio_presence::AudioPresence;
use crate::bcmedia_dump::BcMediaDumpConfig;
use crate::camera_tasks;
use crate::capabilities::CameraCapabilities;
use crate::config::CameraConfig;
use crate::grace_period::GracePeriod;
use crate::preview_state::PreviewState;
use crate::status_cache::StatusCache;
use crate::stream_source::{MutexPoisonRecover as _, RwLockPoisonRecover as _, StreamSource};
use crate::wake_lock::WakeLockCounter;

// ── Camera State ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CameraState {
	Disconnected,
	Connecting,
	Connected,
}

impl CameraState {
	pub fn is_disconnected(&self) -> bool {
		matches!(self, CameraState::Disconnected)
	}

	pub fn is_connecting(&self) -> bool {
		matches!(self, CameraState::Connecting)
	}

	pub fn is_connected(&self) -> bool {
		matches!(self, CameraState::Connected)
	}
}

/// Fold the per-camera [`PreviewState`], the "any active source is
/// currently in `Bridging`" signal, and the aggregator's own last
/// write into the effective state the MQTT overlay should display
///.
///
/// Rules:
/// - [`PreviewState::Sleeping`] passes through unchanged (connect-loop
///   owned — parked / post-grace-disconnect).
/// - [`PreviewState::Live`] + any bridging source → `Connecting`
///   (downgrade).
/// - [`PreviewState::Live`] + no bridging → stays `Live`.
/// - [`PreviewState::Connecting`]:
///   - If `last_own_write == Some(Connecting)` AND no bridging → `Live`
///     (our own downgrade; gap cleared).
///   - Otherwise → `Connecting` (connect-loop owned, or still bridging;
///     don't touch).
fn aggregate_preview_state(
	current: PreviewState,
	any_bridging: bool,
	last_own_write: Option<PreviewState>,
) -> PreviewState {
	match (current, any_bridging, last_own_write) {
		(PreviewState::Sleeping, _, _) => PreviewState::Sleeping,
		(PreviewState::Live, true, _) => PreviewState::Connecting,
		(PreviewState::Live, false, _) => PreviewState::Live,
		(PreviewState::Connecting, false, Some(PreviewState::Connecting)) => PreviewState::Live,
		(PreviewState::Connecting, _, _) => PreviewState::Connecting,
	}
}

/// Pick the PreviewState after `keepalive_loop` returns — the session
/// is over, either because the camera is about to park (idle +
/// idle_disconnect) or because the loop is about to reconnect.
///
/// Returns `Sleeping` only when the next iteration will actually park;
/// otherwise `Connecting`, which avoids a brief SLEEPING flash between
/// reconnect cycles on slow cameras.
fn post_keepalive_preview_state(idle_disconnect: bool, wake_lock_idle: bool) -> PreviewState {
	if idle_disconnect && wake_lock_idle {
		PreviewState::Sleeping
	} else {
		PreviewState::Connecting
	}
}

/// Outcome of a single keepalive probe tick, folded into the counter
/// update that `keepalive_loop` performs. Pure data — extracted so the
/// decision table can be unit-tested without standing up a live
/// `BcCamera` + `keepalive_probe()` round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeepaliveTickOutcome {
	/// Probe succeeded. Reset the failure counter.
	Ok,
	/// Probe failed or timed out. Caller should increment the counter
	/// and compare against `MAX_FAILURES`.
	Failed,
}

/// Fold a `timeout(..., keepalive_probe()).await` outcome into a
/// [`KeepaliveTickOutcome`].
///
/// `Ok(Ok(_))` is success; everything else (any protocol error, any
/// timeout) is a failure. The "camera is alive but doesn't speak
/// link-type" special case used to live here — it now lives one layer
/// down in [`CameraDriver::keepalive_probe`], which matches on
/// `Error::UnintelligibleReply` directly instead of stringifying.
pub(crate) fn classify_keepalive_tick<T, E>(
	outcome: std::result::Result<std::result::Result<T, E>, tokio::time::error::Elapsed>,
) -> KeepaliveTickOutcome {
	match outcome {
		Ok(Ok(_)) => KeepaliveTickOutcome::Ok,
		Ok(Err(_)) => KeepaliveTickOutcome::Failed,
		Err(_) => KeepaliveTickOutcome::Failed,
	}
}

/// Update the running failure counter given a new tick outcome, and
/// report whether the loop should break (consecutive failures reached
/// `max_failures`).
///
/// Pure — consumed by `keepalive_loop` to keep its body small enough
/// to audit. Not part of the `CameraHandle` impl so unit tests can
/// construct scenarios with a single `u32`.
pub(crate) fn advance_keepalive_counter(
	consecutive_failures: u32,
	outcome: KeepaliveTickOutcome,
	max_failures: u32,
) -> (u32, bool) {
	match outcome {
		KeepaliveTickOutcome::Ok => (0, false),
		KeepaliveTickOutcome::Failed => {
			let next = consecutive_failures.saturating_add(1);
			(next, next >= max_failures)
		}
	}
}

// ── CameraHandle ──────────────────────────────────────────────────────

pub struct CameraHandle {
	config: CameraConfig,
	cancel: CancellationToken,
	wake_lock: WakeLockCounter,
	state: Arc<RwLock<CameraState>>,
	bc_camera: std::sync::RwLock<Option<Arc<dyn CameraDriver>>>,
	/// Concrete `BcCamera` handle kept in parallel with `bc_camera`.
	/// `stream_source.rs` still needs the concrete type (out of Phase
	/// 3B scope per design), so the trait-object stored above is
	/// accompanied by the same `Arc<BcCamera>` for stream spawns.
	bc_camera_concrete: std::sync::RwLock<Option<Arc<BcCamera>>>,
	mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
	/// MQTT topic prefix (e.g. `"bairelay"` or `"neolink"`) propagated
	/// from the config so every per-camera task publishes under the
	/// same root. Holds the default `"bairelay"` when MQTT is disabled
	/// — no task consults it in that case.
	topic_prefix: String,
	disconnect_signal: Arc<Notify>,
	stream_sources: std::sync::RwLock<HashMap<RtspStreamKind, Arc<StreamSource>>>,
	/// Serialises the create-and-insert path in [`Self::stream_source`] so
	/// two concurrent SETUPs for the same camera don't each spawn a
	/// [`StreamSource`] (Reolink cameras respond badly to duplicate
	/// `start_video` commands).
	stream_source_create_lock: tokio::sync::Mutex<()>,
	/// Shared last-frame buffer (video burst + MQTT JPEG). Owned by the
	/// camera handle so it persists across stream-source lifetimes; every
	/// [`StreamSource`] spawned for this camera writes into this same
	/// buffer, and the MQTT preview poller reads from it without triggering
	/// a fresh camera snapshot (which would wake battery cameras every tick).
	last_frame_main: Arc<LastFrameBuffer>,
	/// Opt-in fixture-capture configuration. Cloned (cheap `Arc` bump) into
	/// every [`StreamSource`] so reader tasks can mirror raw `BcMedia`
	/// packets to disk. `None` when the operator did not pass
	/// `--dump-bcmedia` on the CLI.
	bcmedia_dump: Option<Arc<BcMediaDumpConfig>>,
	/// Per-camera audio presence. Starts `Unknown`; flipped by the
	/// stream-source reader task on first observed audio packet and by
	/// `startup_wake::warm_one` when its observation window expires.
	/// Read by `CameraProvider::subscribe` to decide whether to wait for
	/// audio SDP before returning a subscription.
	audio_presence: Arc<RwLock<AudioPresence>>,
	/// Cached camera capabilities (PTZ support, etc.), populated once
	/// per successful connect via `BcCamera::get_support`. `None` until
	/// the first connect completes or until the support query
	/// fails-open to default. Read by the HA MQTT discovery publisher
	/// to gate capability-dependent entities.
	capabilities: std::sync::RwLock<Option<CameraCapabilities>>,
	/// Cached PTZ preset list `(id, name)` populated at connect from
	/// `BcCamera::get_ptz_preset()`. Drives the `PtPreset` HA discovery
	/// `select` entity and the dispatcher's name → id resolution for
	/// the `control/ptz/preset` topic. Empty when get_ptz_preset
	/// failed (e.g. fixed-mount camera that returns InvalidXml).
	preset_cache: std::sync::RwLock<Vec<(u8, String)>>,
	/// Optional HA MQTT discovery publisher. `None` when the config
	/// has no `[mqtt.discovery]` table — in that case
	/// [`Self::publish_discovery`] and [`Self::unpublish_discovery`]
	/// are no-ops. Clone-cheap (`SharedMqttClient` is internally
	/// refcounted).
	discovery_publisher: Option<bairelay_mqtt::DiscoveryPublisher>,
	/// Per-camera preview-state broadcaster. Starts `Sleeping`; the
	/// camera connect loop flips it to `Connecting` / `Live` as it
	/// progresses. The MQTT preview publisher subscribes
	/// via [`Self::preview_state_rx`] to overlay captions on the
	/// cached JPEG. `watch::Sender` is not `Clone`, so it lives
	/// behind an `Arc` to share across callers of
	/// [`Self::set_preview_state`]. Written by the connect loop on
	/// each lifecycle transition.
	preview_state_tx: Arc<watch::Sender<PreviewState>>,
	preview_state_rx: watch::Receiver<PreviewState>,
	/// Per-camera cache of the most recently published MQTT status
	/// values (battery, motion, motion classes, push, floodlight,
	/// floodlight_tasks, pir).
	/// Threaded into every status-publishing task so each successful
	/// publish updates the cache; read by `republish_cached_status`
	/// on every broker ConnAck so HA recovers full state even when
	/// the broker has lost retained messages (no-persistence config,
	/// HA-Mosquitto-add-on restart, etc.). See `src/status_cache.rs`.
	status_cache: Arc<StatusCache>,
	/// Set once the camera has refused `battery_info` enough times in a
	/// row for `battery_poller` to conclude it has no battery hardware,
	/// as a mains-powered model does. Sticky for the process lifetime:
	/// `spawn_session_tasks` consults it so a reconnect doesn't restart
	/// the poller and re-run the same futile probes on every wake.
	battery_unsupported: Arc<AtomicBool>,
	/// Serialises `control/ir` / `control/led`. Both are read-modify-
	/// write of one `<LedState>` on the camera, so overlapping commands
	/// would otherwise undo each other's field.
	led_write_lock: tokio::sync::Mutex<()>,
	/// Bumped by the push listener for every push from this camera. A
	/// push's hold window only clears `motion` / `push` if no later
	/// push has arrived, so back-to-back pushes don't cut each other
	/// short.
	push_seq: AtomicU64,
	/// Global `Config::stream_prune_grace_secs` propagated by the
	/// orchestrator at construction. Threaded through to
	/// [`crate::config::resolve_idle_disconnect_timeout`] so the floor
	/// guard can clamp `idle_disconnect_timeout_secs` if an operator
	/// configures it shorter than the prune grace. Defaults to
	/// [`Duration::ZERO`] in test constructors that don't go through
	/// the orchestrator — the floor only fires when configured grace
	/// is strictly less than this, so zero disables it for tests that
	/// want short-grace timing.
	prune_grace: Duration,
}

impl CameraHandle {
	pub fn new(
		config: CameraConfig,
		parent_cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
	) -> Self {
		Self::with_bcmedia_dump(config, parent_cancel, mqtt_client, None)
	}

	/// Constructor variant that lets callers attach a shared
	/// [`BcMediaDumpConfig`]. Use [`CameraHandle::new`] for the common case;
	/// the orchestrator calls this constructor directly when the operator
	/// passed `--dump-bcmedia` on the CLI.
	///
	/// Uses the default MQTT topic prefix `"bairelay"`. To override (for
	/// neolink-legacy migration, for example), use
	/// [`CameraHandle::with_bcmedia_dump_and_prefix`].
	pub fn with_bcmedia_dump(
		config: CameraConfig,
		parent_cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
		bcmedia_dump: Option<Arc<BcMediaDumpConfig>>,
	) -> Self {
		Self::with_bcmedia_dump_and_prefix(
			config,
			parent_cancel,
			mqtt_client,
			"bairelay".to_string(),
			bcmedia_dump,
		)
	}

	/// Full constructor that also takes the `mqtt.topic_prefix` config
	/// value. All other constructors funnel through this one.
	pub fn with_bcmedia_dump_and_prefix(
		config: CameraConfig,
		parent_cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
		topic_prefix: String,
		bcmedia_dump: Option<Arc<BcMediaDumpConfig>>,
	) -> Self {
		let (preview_state_tx, preview_state_rx) = watch::channel(PreviewState::Sleeping);
		let preview_state_tx = Arc::new(preview_state_tx);
		Self {
			config,
			cancel: parent_cancel.child_token(),
			wake_lock: WakeLockCounter::new(),
			state: Arc::new(RwLock::new(CameraState::Disconnected)),
			bc_camera: std::sync::RwLock::new(None),
			bc_camera_concrete: std::sync::RwLock::new(None),
			mqtt_client,
			topic_prefix,
			disconnect_signal: Arc::new(Notify::new()),
			stream_sources: std::sync::RwLock::new(HashMap::new()),
			stream_source_create_lock: tokio::sync::Mutex::new(()),
			last_frame_main: Arc::new(LastFrameBuffer::new()),
			bcmedia_dump,
			audio_presence: Arc::new(RwLock::new(AudioPresence::Unknown)),
			capabilities: std::sync::RwLock::new(None),
			preset_cache: std::sync::RwLock::new(Vec::new()),
			discovery_publisher: None,
			preview_state_tx,
			preview_state_rx,
			status_cache: Arc::new(StatusCache::default()),
			battery_unsupported: Arc::new(AtomicBool::new(false)),
			led_write_lock: tokio::sync::Mutex::new(()),
			push_seq: AtomicU64::new(0),
			prune_grace: Duration::ZERO,
		}
	}

	/// Builder setter: propagate the configured
	/// `Config::stream_prune_grace_secs` so the floor enforcement in
	/// [`crate::config::resolve_idle_disconnect_timeout`] has the right
	/// reference value. Called by the orchestrator after construction.
	#[must_use]
	pub fn with_prune_grace(mut self, prune_grace: Duration) -> Self {
		self.prune_grace = prune_grace;
		self
	}

	/// Builder setter: attach an HA MQTT discovery publisher. Only
	/// meaningful when the binary was built with `[mqtt.discovery]` in
	/// its config. Consumes and returns `self` so callers can chain
	/// this at construction time before wrapping in `Arc`. The
	/// orchestrator is the sole production caller — see
	/// `Orchestrator::with_bcmedia_dump`.
	#[must_use]
	pub fn with_discovery_publisher(
		mut self,
		publisher: bairelay_mqtt::DiscoveryPublisher,
	) -> Self {
		self.discovery_publisher = Some(publisher);
		self
	}

	/// Handle to the shared last-frame buffer for this camera's Main stream.
	///
	/// Safe to call at any time (before/after connect). Reads are cheap so
	/// the MQTT preview poller can query this on a timer without incurring
	/// a camera-side snapshot round-trip.
	pub fn last_frame_main(&self) -> Arc<LastFrameBuffer> {
		Arc::clone(&self.last_frame_main)
	}

	/// Shared handle on this camera's audio presence state. Writers call
	/// `.write()`, subscribers call `.read()`.
	pub fn audio_presence(&self) -> Arc<RwLock<AudioPresence>> {
		Arc::clone(&self.audio_presence)
	}

	/// Current cached capabilities (PTZ support, etc.). Returns `None`
	/// until the first successful connect populates the cache; callers
	/// must treat `None` as "caps not yet known" and either defer
	/// capability-gated work or fall back to defaults. Returning a
	/// `Copy` by value keeps the lock guard off the call site.
	pub fn capabilities(&self) -> Option<CameraCapabilities> {
		*self.capabilities.read_recover()
	}

	/// Snapshot of the cached PTZ preset list. Empty when the camera
	/// hasn't been queried yet, lacks PTZ support, or returned an
	/// invalid PtzPreset XML. Cloned to drop the lock immediately.
	pub fn preset_cache(&self) -> Vec<(u8, String)> {
		self.preset_cache.read_recover().clone()
	}

	/// Test-only helper to install a synthetic preset list. Production
	/// code populates this via `get_ptz_preset()` at connect.
	#[doc(hidden)]
	pub fn set_preset_cache_for_test(&self, presets: Vec<(u8, String)>) {
		*self.preset_cache.write_recover() = presets;
	}

	/// Replace the cached preset list. Production callers are the
	/// connect path (initial populate via `get_support` / `get_ptz_preset`)
	/// and the `query/ptz/preset` MQTT handler (mid-session refresh
	/// after `bairelay ptz assign` or an in-app rename).
	pub fn replace_preset_cache(&self, presets: Vec<(u8, String)>) {
		*self.preset_cache.write_recover() = presets;
	}

	/// Resolve a preset name to its numeric id via the cache. Returns
	/// `None` when the name isn't in the list (cache empty / unknown
	/// preset). Case-insensitive — HA's select normalises options but
	/// firmwares vary in capitalisation across reboots.
	pub fn preset_id_for_name(&self, name: &str) -> Option<u8> {
		let cache = self.preset_cache.read_recover();
		cache
			.iter()
			.find(|(_, n)| n.eq_ignore_ascii_case(name))
			.map(|(id, _)| *id)
	}

	/// Inverse of [`Self::preset_id_for_name`]. Used by the dispatcher
	/// to publish the preset name on `status/ptz_preset` after a
	/// numeric-id move so HA's select reflects the new state.
	pub fn preset_name_for_id(&self, preset_id: u8) -> Option<String> {
		let cache = self.preset_cache.read_recover();
		cache
			.iter()
			.find(|(id, _)| *id == preset_id)
			.map(|(_, n)| n.clone())
	}

	/// Subscribe to this camera's preview-state feed. The returned
	/// [`watch::Receiver`] yields the current [`PreviewState`] on
	/// subscribe and every subsequent transition. Initial value is
	/// [`PreviewState::Sleeping`] until the connect loop runs.
	pub fn preview_state_rx(&self) -> watch::Receiver<PreviewState> {
		self.preview_state_rx.clone()
	}

	/// Update the preview-state channel. Called by the connect loop
	/// on each lifecycle transition. Send errors (no receivers) are
	/// intentionally ignored — the overlay subscriber is optional.
	pub(crate) fn set_preview_state(&self, s: PreviewState) {
		let _ = self.preview_state_tx.send(s);
	}

	/// Publish retained HA discovery config payloads for this camera.
	///
	/// Early-returns `Ok(())` when either (a) no
	/// [`DiscoveryPublisher`] is attached (operator did not opt in
	/// via `[mqtt.discovery]`), or (b) the capability cache is still
	/// `None` (first successful connect has not completed, or the
	/// `get_support` call failed and the next reconnect will re-probe).
	/// In both cases no MQTT traffic is generated.
	///
	/// Intended to be called post-first-connect and on every MQTT
	/// `ConnAck`. Safe to call repeatedly — retained publishes are
	/// idempotent and HA deduplicates by `unique_id`.
	pub async fn publish_discovery(&self) -> Result<(), bairelay_mqtt::MqttError> {
		let Some(publisher) = self.discovery_publisher.as_ref() else {
			return Ok(());
		};
		let Some(caps) = self.capabilities() else {
			return Ok(());
		};
		let flags = bairelay_mqtt::CameraEnableFlags::from(&self.config.mqtt);
		let presets = self.preset_cache();
		publisher
			.publish(
				&self.config.name,
				self.config.address.as_deref(),
				self.config.uid.as_deref(),
				caps.into(),
				&flags,
				&presets,
			)
			.await
	}

	/// Test-only helper: install a synthetic capability set without
	/// going through the connect path. Exposed as `pub` (gated on
	/// `#[doc(hidden)]`) so integration tests in `tests/*.rs` can
	/// construct a camera with populated caps. Production code must
	/// not call this.
	#[doc(hidden)]
	pub fn set_capabilities_for_test(&self, caps: CameraCapabilities) {
		*self.capabilities.write_recover() = Some(caps);
	}

	/// Shared per-camera status cache. Each status-publishing task
	/// (battery / motion / floodlight / floodlight_tasks / pir)
	/// receives a clone of this Arc and writes the just-published
	/// value after every successful publish. Read by
	/// [`Self::republish_cached_status`] on every broker reconnect.
	/// Lock held across an IR / LED read-modify-write + republish.
	pub fn led_write_lock(&self) -> &tokio::sync::Mutex<()> {
		&self.led_write_lock
	}

	pub fn status_cache(&self) -> Arc<StatusCache> {
		Arc::clone(&self.status_cache)
	}

	/// Push counter the push listener bumps; see the `push_seq` field.
	pub fn push_seq(&self) -> &AtomicU64 {
		&self.push_seq
	}

	/// `true` once `battery_poller` has concluded this camera has no
	/// battery hardware and given up. Sticky for the process lifetime;
	/// gates the poller respawn in [`Self::spawn_session_tasks`].
	pub fn battery_unsupported(&self) -> bool {
		self.battery_unsupported.load(Ordering::Acquire)
	}

	/// Re-emit every cached MQTT status value via retained publishes.
	/// Called by `mqtt_loop::handle_connack` after the discovery
	/// republish so HA recovers full state when the broker has lost
	/// retained messages (no-persistence config, broker restart).
	///
	/// No-op when the camera has no MQTT client attached or when no
	/// status has ever been published yet (cache fully empty).
	pub async fn republish_cached_status(&self) -> Result<(), bairelay_mqtt::MqttError> {
		let Some(ref mqtt) = self.mqtt_client else {
			return Ok(());
		};
		let publisher =
			bairelay_mqtt::StatusPublisher::new(mqtt, &self.topic_prefix, &self.config.name);
		if let Some(level) = self.status_cache.battery_level() {
			publisher.publish_battery_level(level).await?;
		}
		if let Some(motion) = self.status_cache.motion() {
			publisher.publish_motion(motion).await?;
		}
		if let Some(detections) = self.status_cache.motion_classes() {
			camera_tasks::publish_motion_classes(&publisher, detections).await?;
		}
		if let Some(push) = self.status_cache.push() {
			publisher.publish_push(push).await?;
		}
		if let Some(on) = self.status_cache.floodlight() {
			publisher.publish_floodlight(on).await?;
		}
		if let Some(enabled) = self.status_cache.floodlight_tasks() {
			publisher.publish_floodlight_tasks_enabled(enabled).await?;
		}
		if let Some(enabled) = self.status_cache.pir() {
			publisher.publish_pir(enabled).await?;
		}
		if let Some(mode) = self.status_cache.ir() {
			publisher.publish_ir(mode).await?;
		}
		if let Some(on) = self.status_cache.led() {
			publisher.publish_led(on).await?;
		}
		Ok(())
	}

	/// Remove retained HA discovery config payloads for this camera
	/// by publishing an empty retained payload on every topic
	/// `publish_discovery` would emit. Same early-return rules apply.
	/// Intended for graceful shutdown so HA clears the entity set.
	pub async fn unpublish_discovery(&self) -> Result<(), bairelay_mqtt::MqttError> {
		let Some(publisher) = self.discovery_publisher.as_ref() else {
			return Ok(());
		};
		let Some(caps) = self.capabilities() else {
			return Ok(());
		};
		let flags = bairelay_mqtt::CameraEnableFlags::from(&self.config.mqtt);
		let presets = self.preset_cache();
		publisher
			.unpublish(
				&self.config.name,
				self.config.address.as_deref(),
				self.config.uid.as_deref(),
				caps.into(),
				&flags,
				&presets,
			)
			.await
	}

	pub fn state(&self) -> CameraState {
		*self.state.read_recover()
	}

	pub fn wake_lock(&self) -> &WakeLockCounter {
		&self.wake_lock
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancel.is_cancelled()
	}

	pub fn name(&self) -> &str {
		&self.config.name
	}

	/// MQTT topic prefix configured for this camera
	/// (`mqtt.topic_prefix` in the config; default `"bairelay"`).
	pub fn topic_prefix(&self) -> &str {
		&self.topic_prefix
	}

	pub fn config(&self) -> &CameraConfig {
		&self.config
	}

	pub fn cancel_token(&self) -> &CancellationToken {
		&self.cancel
	}

	/// Returns the currently-connected camera as a trait object, if any.
	///
	/// Returning `Arc<dyn CameraDriver>` (rather than the concrete
	/// `BcCamera`) lets unit tests substitute a fake camera at this
	/// boundary without touching the binary's call-sites.
	pub fn bc_camera(&self) -> Option<Arc<dyn CameraDriver>> {
		self.bc_camera.read_recover().clone()
	}

	/// Signal the camera to disconnect (used by the watchdog for idle cameras).
	pub fn request_disconnect(&self) {
		self.disconnect_signal.notify_one();
	}

	/// Test-only accessor that returns a clone of the disconnect-signal
	/// `Notify`. Tests can `.notified().await` (with a small timeout)
	/// to assert that `request_disconnect` actually fired — much
	/// stronger than asserting the function "didn't panic".
	#[cfg(test)]
	pub(crate) fn disconnect_signal_for_test(&self) -> Arc<Notify> {
		Arc::clone(&self.disconnect_signal)
	}

	fn set_state(&self, new: CameraState) {
		*self.state.write_recover() = new;
	}

	/// Get (or lazily create) the [`StreamSource`] for the given stream kind.
	///
	/// Waits up to 30 seconds for the camera to reach `Connected` state,
	/// polling every 100 ms. Once connected, the source is started via
	/// `StreamSource::start` and cached in the per-camera registry so
	/// subsequent subscribers share one Baichuan video stream.
	pub async fn stream_source(
		&self,
		kind: RtspStreamKind,
	) -> Result<Arc<StreamSource>, StreamError> {
		// Poll for Connected state up to 30s. Scope guards tightly so no
		// RwLock guard is held across `.await` (clippy::await_holding_lock).
		const POLL_INTERVAL: Duration = Duration::from_millis(100);
		const MAX_WAIT: Duration = Duration::from_secs(30);
		let deadline = std::time::Instant::now() + MAX_WAIT;
		loop {
			if self.cancel.is_cancelled() {
				return Err(StreamError::Unavailable(
					"camera task shutting down".to_string(),
				));
			}
			if self.state() == CameraState::Connected {
				break;
			}
			if std::time::Instant::now() >= deadline {
				return Err(StreamError::Unavailable(format!(
					"camera '{}' did not reach Connected state within {}s",
					self.config.name,
					MAX_WAIT.as_secs()
				)));
			}
			tokio::time::sleep(POLL_INTERVAL).await;
		}

		// Fast path: already registered.
		{
			let guard = self.stream_sources.read_recover();
			if let Some(source) = guard.get(&kind) {
				return Ok(Arc::clone(source));
			}
		}

		// Slow path: hold the async create-mutex across the start + insert
		// so two concurrent callers can't both spawn a StreamSource (and
		// thus both call `start_video` on the camera). The loser awaits the
		// mutex, re-checks the map under the lock, and reuses the winner's
		// source — no duplicate start_video is ever issued.
		let _create_guard = self.stream_source_create_lock.lock().await;

		// Re-check under the mutex: another caller may have just created it.
		{
			let guard = self.stream_sources.read_recover();
			if let Some(source) = guard.get(&kind) {
				return Ok(Arc::clone(source));
			}
		}

		// State re-check under the create-lock: closes the TOCTOU
		// between the polling loop above and the bc_camera_concrete
		// read below. While we waited for the create-lock, the
		// keepalive loop may have failed → `run_connected_session`
		// proceeded to `stop_all_stream_sources()` → just-cleared the
		// registry. Without this guard we would happily insert a new
		// source into the now-empty registry and spawn a reader against
		// a camera that's about to be dropped. Returning Unavailable
		// here surfaces "RTSP SETUP arrived during a teardown window"
		// to the client cleanly — the next SETUP will land in the next
		// session.
		if self.cancel.is_cancelled() || self.state() != CameraState::Connected {
			return Err(StreamError::Unavailable(format!(
				"camera '{}' is no longer in Connected state",
				self.config.name
			)));
		}

		// Need to start a new source. Grab the concrete BcCamera handle —
		// StreamSource still takes `Arc<BcCamera>` (out of scope
		// per design).
		let camera = self
			.bc_camera_concrete
			.read_recover()
			.clone()
			.ok_or_else(|| {
				StreamError::Unavailable(format!(
					"camera '{}' is not currently connected",
					self.config.name
				))
			})?;

		// Start the source. `StreamSource::start` only spawns a task and
		// returns immediately, so it's cheap to do while the async mutex
		// is held. We pass the camera-scoped last-frame buffer so every
		// source for this camera writes to the same buffer (one JPEG per
		// camera, not per stream).
		//
		// Gap threshold: Task 5 will read this in
		// `reader_task` to drive placeholder-frame emission when the
		// camera goes silent. `bridge_gaps = false` folds to
		// `Duration::MAX` so the ticker never fires — no separate
		// "disabled" code path needed.
		let gap_threshold = if self.config.pause.bridge_gaps {
			Duration::from_secs_f64(self.config.pause.gap_threshold_secs)
		} else {
			Duration::MAX
		};
		let source = StreamSource::start(
			camera,
			self.config.name.clone(),
			kind,
			Arc::clone(&self.last_frame_main),
			self.bcmedia_dump.clone(),
			self.audio_presence(),
			gap_threshold,
		);

		// Insert the new source under the sync lock.
		{
			let mut guard = self.stream_sources.write_recover();
			guard.insert(kind, Arc::clone(&source));
		}
		Ok(source)
		// `_create_guard` drops here, unblocking any waiter who will then
		// observe our entry via the re-check.
	}

	/// Remove and stop any [`StreamSource`]s whose broadcast channels have
	/// been without subscribers for at least `grace`. Called by the
	/// watchdog / idle sweeps.
	///
	/// Accepts an explicit `now` and `grace`, enabling
	/// virtual-clock unit tests for the grace window. Production callers
	/// (watchdog) thread the configured `stream_prune_grace_secs` here.
	///
	/// A source is pruned only when it has observed zero subscribers for
	/// at least `grace`. This smooths rapid RTSP disconnect/reconnect so
	/// we don't pay a cold `BcCamera::start_video` handshake on every
	/// reconnect.
	///
	/// A `grace` of zero restores the legacy instantaneous-prune behaviour.
	pub(crate) fn prune_idle_stream_sources_at(&self, now: Instant, grace: Duration) {
		let mut guard = self.stream_sources.write_recover();
		guard.retain(|_kind, source| {
			let subs = source.subscribers();
			let mut marker = source.last_idle_since.lock_recover();
			if subs > 0 {
				*marker = None;
				return true;
			}
			match *marker {
				None => {
					*marker = Some(now);
					// If grace is zero, expire immediately on this sweep.
					if grace.is_zero() {
						// Drop the marker guard before stop/remove to keep
						// the Mutex<Option<Instant>> from being re-locked
						// via the Drop path (belt-and-braces — `Instant` /
						// `Option` have no custom Drop today).
						drop(marker);
						source.stop();
						return false;
					}
					true
				}
				Some(since) if now.saturating_duration_since(since) >= grace => {
					drop(marker);
					source.stop();
					false
				}
				Some(_) => true,
			}
		});
	}

	/// Returns `true` if any currently-registered [`StreamSource`] for
	/// this camera reports [`GapState::Bridging`]. Used by the
	/// per-camera preview-state aggregator to decide
	/// whether to downgrade `PreviewState::Live` → `Connecting` for the
	/// MQTT overlay while upstream frames are stalled.
	pub(crate) fn any_stream_source_bridging(&self) -> bool {
		let guard = self.stream_sources.read_recover();
		guard
			.values()
			.any(|s| s.gap_state() == crate::stream_source::GapState::Bridging)
	}

	/// Stop every registered stream source and clear the registry. Called
	/// during session teardown so no reader task outlives the camera
	/// connection that feeds it.
	///
	/// Awaits each source's reader task with a hard timeout so a
	/// detached reader cannot keep an `Arc<BcCamera>` clone alive past
	/// teardown — that would race the next session's `start_video` on
	/// the same camera. The timeout matches the reader's own
	/// `STOP_VIDEO_TIMEOUT` (5 s) plus a margin: a wedged reader still
	/// gets dropped from the registry, just with a `warn` so the
	/// operator can see the leak.
	async fn stop_all_stream_sources(&self) {
		// Drain entries under the write lock; await outside the lock
		// so we don't hold a sync `RwLockWriteGuard` across `.await`.
		let drained: Vec<(RtspStreamKind, Arc<StreamSource>)> = {
			let mut guard = self.stream_sources.write_recover();
			let entries: Vec<_> = guard.drain().collect();
			entries
		};
		for (kind, source) in drained {
			if let Err(e) = source.stop_and_wait(Self::STREAM_SOURCE_STOP_TIMEOUT).await {
				tracing::warn!(
					camera = %self.config.name,
					stream = ?kind,
					error = %e,
					"stream-source reader did not exit within budget; detached (still holds Arc<BcCamera> until it observes cancel)",
				);
			}
		}
	}

	/// Attempt to connect and login to the camera.
	async fn try_connect(&self) -> anyhow::Result<Arc<BcCamera>> {
		let opts = crate::bc_opts::build_bc_opts(&self.config);
		let max_enc = crate::bc_opts::max_encryption(&self.config);
		let camera = BcCamera::new(&opts).await?;
		camera.login_with_maxenc(max_enc).await?;

		Ok(Arc::new(camera))
	}

	/// Send periodic keepalive probes to detect connection loss.
	/// Uses [`CameraDriver::keepalive_probe`] (a lenient wrapper over
	/// `get_linktype()` matching neolink's choice) and tolerates up to
	/// [`Self::KEEPALIVE_MAX_FAILURES`] consecutive failures before
	/// disconnecting. Cadence and per-probe timeout are both
	/// [`Self::KEEPALIVE_INTERVAL`].
	async fn keepalive_loop(&self, camera: &dyn CameraDriver, session_cancel: &CancellationToken) {
		let mut interval = tokio::time::interval(Self::KEEPALIVE_INTERVAL);
		let mut consecutive_failures: u32 = 0;

		'outer: loop {
			tokio::select! {
				_ = self.cancel.cancelled() => break,
				_ = session_cancel.cancelled() => {
					tracing::info!(camera = %self.config.name, "Session cancelled (grace period or disconnect)");
					break;
				}
				_ = self.disconnect_signal.notified() => {
					tracing::info!(camera = %self.config.name, "Watchdog requested disconnect");
					break;
				}
				_ = interval.tick() => {
					// Race the probe against cancel / disconnect signals so a
					// shutdown that arrives mid-probe preempts immediately
					// instead of stalling for the full
					// `KEEPALIVE_PROBE_TIMEOUT`. Without the inner select,
					// shutdown latency was bounded by the per-probe timeout
					// (previously equal to the interval, so up to 5 s).
					let raw = tokio::select! {
						v = tokio::time::timeout(Self::KEEPALIVE_PROBE_TIMEOUT, camera.keepalive_probe()) => v,
						_ = self.cancel.cancelled() => break 'outer,
						_ = session_cancel.cancelled() => {
							tracing::info!(
								camera = %self.config.name,
								"Session cancelled mid-probe"
							);
							break 'outer;
						}
						_ = self.disconnect_signal.notified() => {
							tracing::info!(
								camera = %self.config.name,
								"Watchdog requested disconnect mid-probe"
							);
							break 'outer;
						}
					};
					let outcome = classify_keepalive_tick(raw);
					let (next, should_break) = advance_keepalive_counter(
						consecutive_failures,
						outcome,
						Self::KEEPALIVE_MAX_FAILURES,
					);
					consecutive_failures = next;
					if outcome == KeepaliveTickOutcome::Failed {
						tracing::debug!(
							camera = %self.config.name,
							failures = consecutive_failures,
							"Keepalive probe failed",
						);
					}
					if should_break {
						tracing::warn!(
							camera = %self.config.name,
							"Keepalive failed {} times, disconnecting",
							Self::KEEPALIVE_MAX_FAILURES,
						);
						break;
					}
				}
			}
		}
	}

	/// Interval between keepalive probes. 5 s matches neolink and is
	/// well below the camera's own idle threshold for sleep-mode
	/// battery devices.
	/// Backstop for a single connect attempt (discovery + socket setup).
	/// The inner discovery retry loop owns the real timing — 5 rounds,
	/// each bounded by `REGISTRATION_ROUND_TIMEOUT`, with exponential
	/// backoff — and self-terminates with a named-relay error in ~80 s.
	/// This deadline sits above that budget so it never preempts a retry
	/// (the old 30 s cut the loop off after ~3 rounds); Ctrl+C stays
	/// responsive via the cancel arm regardless. A still-failing attempt
	/// then falls to the outer `ReconnectBackoff`.
	const CONNECT_TIMEOUT: Duration = Duration::from_secs(100);
	pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
	/// Per-probe timeout: strictly below [`Self::KEEPALIVE_INTERVAL`]
	/// so a slow probe doesn't consume the entire tick budget, AND
	/// races against cancel signals (see `keepalive_loop`) so shutdown
	/// preempts a probe in flight. 3 s is a comfortable upper bound on
	/// `get_linktype` latency observed on real Argus hardware (typical
	/// is under 200 ms).
	pub(crate) const KEEPALIVE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
	/// Hard cap on the per-session courtesy `logout()` call during
	/// teardown. Matches `docs/implementation.md` § "Calls that spawn
	/// internal tasks" step 4. A wedged camera that doesn't reply to
	/// `logout` must not stall the next reconnect cycle.
	const LOGOUT_TIMEOUT: Duration = Duration::from_secs(5);
	/// Hard cap on awaiting a stream-source reader task during session
	/// teardown. Reader's own `STOP_VIDEO_TIMEOUT` is 5 s; we add a
	/// small margin so a clean stop_video has time to settle before we
	/// declare the reader detached.
	const STREAM_SOURCE_STOP_TIMEOUT: Duration = Duration::from_secs(7);
	/// Maximum consecutive probe failures before the loop gives up and
	/// lets the outer reconnect loop take over.
	pub(crate) const KEEPALIVE_MAX_FAILURES: u32 = 5;

	/// Drive the "just-connected" lifecycle: publish status, capability
	/// probe, session-task spawn, keepalive loop, graceful teardown.
	///
	/// Extracted from `run()` so tests can drive the whole thing with a
	/// scripted `Arc<dyn CameraDriver>` + `concrete: None`. When
	/// `concrete` is `Some`, stream-source spawns via
	/// [`Self::stream_source`] work and logout is attempted at the end
	/// of teardown; when `None` (test mode) both paths are skipped.
	async fn run_connected_session(
		self: &Arc<Self>,
		driver: Arc<dyn CameraDriver>,
		concrete: Option<Arc<BcCamera>>,
	) {
		self.set_state(CameraState::Connected);
		self.set_preview_state(PreviewState::Live);
		tracing::info!(camera = %self.config.name, "Connected");

		// Publish connected status via MQTT.
		if let Some(ref mqtt) = self.mqtt_client {
			let publisher =
				bairelay_mqtt::StatusPublisher::new(mqtt, &self.topic_prefix, &self.config.name);
			if let Err(e) = publisher.publish_connection(true).await {
				tracing::warn!(camera = %self.config.name, error = %e, "Failed to publish connected status");
			}
		}

		*self.bc_camera.write_recover() = Some(Arc::clone(&driver));
		*self.bc_camera_concrete.write_recover() = concrete.clone();

		// Populate the capability cache for HA discovery.
		// `get_support()` is idempotent and read-only. On success we
		// cache the view; on failure we leave the cache `None` so the
		// next reconnect re-probes — a transient `get_support` error
		// must not stick `has_ptz = false` for the whole session (HA
		// discovery would then never emit PT buttons even for a PT-
		// capable cam that only fluked one probe).
		match driver.get_support().await {
			Ok(support) => {
				let has_ptz = crate::capabilities::ptz_mode_indicates_ptz(
					support.ptz_mode.as_deref(),
					support.ptz_cfg,
				);
				let (status_led, ir_leds) =
					crate::capabilities::led_presence(&*driver, &support, self.config.channel_id)
						.await;
				// Hardware confirmed absent: drop any cached value so a
				// broker reconnect can't replay it after discovery
				// retracts the entity.
				if status_led.is_absent() {
					self.status_cache.clear_led();
				}
				if ir_leds.is_absent() {
					self.status_cache.clear_ir();
				}
				*self.capabilities.write_recover() = Some(CameraCapabilities {
					has_ptz,
					status_led,
					ir_leds,
				});

				// Populate the preset cache for HA discovery's
				// `PtPreset` select. Only meaningful when has_ptz
				// — fixed-mount cameras return InvalidXml. Treat
				// any read error as "no presets" (empty Vec) and
				// keep the discovery publish going; the select
				// just won't be emitted.
				if has_ptz {
					match driver.get_ptz_preset().await {
						Ok(p) => {
							let presets: Vec<(u8, String)> = p
								.preset_list
								.preset
								.into_iter()
								.filter_map(|preset| preset.name.map(|n| (preset.id, n)))
								.collect();
							*self.preset_cache.write_recover() = presets;
						}
						Err(e) => {
							tracing::debug!(
								camera = %self.config.name,
								error = %e,
								"get_ptz_preset failed; leaving preset cache empty"
							);
						}
					}
				}

				if let Err(e) = self.publish_discovery().await {
					tracing::warn!(
						camera = %self.config.name,
						error = %e,
						"Failed to publish HA discovery payloads post-connect; will retry on next ConnAck"
					);
				}
			}
			Err(e) => {
				tracing::warn!(
					camera = %self.config.name,
					error = %e,
					"Failed to query camera capabilities via get_support(); leaving cache empty for re-probe on next reconnect"
				);
			}
		}

		// Spawn per-session tasks (motion, pollers, grace period).
		let session_cancel = CancellationToken::new();
		let tasks = self.spawn_session_tasks(&driver, &session_cancel).await;

		self.keepalive_loop(&*driver, &session_cancel).await;

		// Close the PreviewState gap: the Live arm is over. Sleeping
		// only if the next iteration will park; otherwise Connecting
		// so a slow reconnect doesn't flash SLEEPING on the HA
		// overlay.
		self.set_preview_state(post_keepalive_preview_state(
			self.config.idle_disconnect,
			self.wake_lock.is_idle(),
		));

		// Connection lost or shutdown -- cancel all session tasks
		// and tear down everything that depends on the live driver.
		session_cancel.cancel();
		self.teardown_session_tasks(tasks, concrete).await;
	}

	/// Spawn the per-session tasks that hold their own clones of the
	/// driver: preview-state aggregator + motion / battery / floodlight
	/// pollers + PIR one-shot publish + grace-period watcher (battery
	/// cameras only). All tasks honour `session_cancel`. Returns the
	/// `JoinSet` so the caller can drive its own shutdown sequence after
	/// `keepalive_loop` returns.
	async fn spawn_session_tasks(
		self: &Arc<Self>,
		driver: &Arc<dyn CameraDriver>,
		session_cancel: &CancellationToken,
	) -> tokio::task::JoinSet<()> {
		let mut tasks = tokio::task::JoinSet::new();

		// Preview-state aggregator.
		{
			let handle = Arc::clone(self);
			let cancel = session_cancel.clone();
			tasks.spawn(async move {
				aggregate_preview_state_loop(handle, cancel).await;
			});
		}

		if let Some(ref mqtt) = self.mqtt_client {
			if self.config.mqtt.enable_motion {
				tasks.spawn(camera_tasks::motion_listener(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.wake_lock.clone(),
					session_cancel.clone(),
					Duration::from_secs_f64(self.config.motion_wake_hold_secs),
					self.status_cache(),
				));
			}
			// `battery_unsupported` latches when the poller concludes the
			// camera has no battery. Skipping the respawn is what makes
			// that verdict permanent rather than per-session — see
			// `camera_tasks::battery_poller`.
			if self.config.mqtt.enable_battery && !self.battery_unsupported() {
				tasks.spawn(camera_tasks::battery_poller(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.config.mqtt.battery_update,
					session_cancel.clone(),
					self.status_cache(),
					Arc::clone(&self.battery_unsupported),
				));
			}
			if self.config.mqtt.enable_floodlight {
				tasks.spawn(camera_tasks::floodlight_poller(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.config.mqtt.floodlight_update,
					session_cancel.clone(),
					self.status_cache(),
				));
				tasks.spawn(camera_tasks::floodlight_listener(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					session_cancel.clone(),
					self.status_cache(),
				));
			}
			if self.config.mqtt.enable_pir {
				camera_tasks::publish_pir_state(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.status_cache(),
				)
				.await;
			}
			// Status LED and IR LEDs are separate features gated on their
			// own capability; the read only runs if either is present.
			// Spawned, not awaited: an unresponsive camera must not hold
			// up the grace-period watcher or ignore session cancel.
			let caps = self.capabilities().unwrap_or_default();
			// Only confirmed hardware is read: with presence unknown the
			// reply value may be a placeholder.
			if caps.status_led.is_present() || caps.ir_leds.is_present() {
				tasks.spawn(camera_tasks::publish_led_state(
					self.config.name.clone(),
					Arc::clone(driver),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.status_cache(),
					caps,
				));
			}
		}

		// Grace-period watcher for battery cameras: when the wake-lock
		// drops to zero, count down `idle_disconnect_timeout_secs`
		// (default 45 s) and then cancel the session, releasing the
		// camera back to sleep.
		if self.config.idle_disconnect {
			let grace =
				crate::config::resolve_idle_disconnect_timeout(&self.config, self.prune_grace);
			let gp = GracePeriod::new(self.wake_lock.clone(), grace);
			tasks.spawn(grace_period_watch(
				gp,
				session_cancel.clone(),
				self.config.name.clone(),
			));
		}

		tasks
	}

	/// Tear down a session: drain the spawned task `JoinSet` (2 s
	/// graceful window then `abort_all`), stop every live stream
	/// source, log out via the concrete `BcCamera` handle if present,
	/// clear the camera's driver/concrete slots, and publish the
	/// disconnected MQTT status. Symmetric with
	/// [`spawn_session_tasks`]; the caller is expected to have
	/// already cancelled `session_cancel` before invoking this.
	async fn teardown_session_tasks(
		self: &Arc<Self>,
		mut tasks: tokio::task::JoinSet<()>,
		concrete: Option<Arc<BcCamera>>,
	) {
		// Give tasks a brief window to exit gracefully, then abort.
		let drain_deadline = tokio::time::sleep(Duration::from_secs(2));
		tokio::pin!(drain_deadline);
		loop {
			tokio::select! {
				result = tasks.join_next() => {
					if result.is_none() { break; } // All done
				}
				_ = &mut drain_deadline => {
					tracing::debug!(camera = %self.config.name, "Aborting remaining session tasks");
					tasks.abort_all();
					while tasks.join_next().await.is_some() {}
					break;
				}
			}
		}

		// Tear down any live stream sources before clearing the
		// BcCamera handle — they reference the camera we are about to
		// drop.
		self.stop_all_stream_sources().await;

		// Per-session courtesy logout, capped with `LOGOUT_TIMEOUT` so a
		// wedged camera can't stall teardown. Uses the local `concrete`
		// handle (the same Arc that `bc_camera_concrete` exposes) — the
		// trait surface intentionally omits `logout()` because no other
		// call-site needs it. `concrete = None` is the test-mode path
		// (`run_connected_session_for_test`); skip cleanly.
		//
		// This is the documented step-4 of the per-session shutdown
		// sequence in `docs/implementation.md` § "Calls that spawn
		// internal tasks". Pre-fix, the only `logout()` call lived in
		// `run()`'s post-loop block, but `bc_camera_concrete` was
		// always nulled before that block ran, so `cam_for_logout` was
		// always `None` and logout was unreachable in production.
		if let Some(ref cam) = concrete {
			match tokio::time::timeout(Self::LOGOUT_TIMEOUT, cam.logout()).await {
				Ok(Ok(())) => tracing::debug!(camera = %self.config.name, "Logged out"),
				Ok(Err(e)) => {
					tracing::debug!(camera = %self.config.name, error = %e, "Logout failed")
				}
				Err(_) => tracing::debug!(camera = %self.config.name, "Logout timed out, dropping"),
			}
		}

		*self.bc_camera.write_recover() = None;
		*self.bc_camera_concrete.write_recover() = None;

		// Drop the concrete handle (Arc<BcCamera>) so its backing
		// connection threads can shut down.
		drop(concrete);

		self.set_state(CameraState::Disconnected);
		tracing::info!(camera = %self.config.name, "Disconnected");

		// Publish disconnected status via MQTT.
		if let Some(ref mqtt) = self.mqtt_client {
			let publisher =
				bairelay_mqtt::StatusPublisher::new(mqtt, &self.topic_prefix, &self.config.name);
			if let Err(e) = publisher.publish_connection(false).await {
				tracing::warn!(camera = %self.config.name, error = %e, "Failed to publish disconnected status");
			}
		}
	}

	/// Main connection loop: connect, keepalive, reconnect on failure.
	pub async fn run(self: &Arc<Self>) {
		// Publish initial "disconnected" / "unknown" states before connecting.
		if let Some(ref mqtt) = self.mqtt_client {
			let publisher =
				bairelay_mqtt::StatusPublisher::new(mqtt, &self.topic_prefix, &self.config.name);
			let _ = publisher.publish_connection(false).await;
			let _ = publisher.publish_motion_unknown().await;
		}

		// Spawn the preview publisher at camera lifetime (NOT session
		// scope): it must keep publishing status/preview with the
		// current overlay caption across disconnect/sleep windows, so
		// HA dashboards see SLEEPING / CONNECTING on stale frames
		// instead of whatever was last published during Live.
		let preview_task = if let Some(ref mqtt) = self.mqtt_client {
			if self.config.mqtt.enable_preview {
				Some(tokio::spawn(camera_tasks::preview_poller(
					self.config.name.clone(),
					Arc::clone(self),
					Arc::clone(&self.last_frame_main),
					mqtt.clone(),
					self.topic_prefix.clone(),
					self.config.mqtt.preview_update,
					self.preview_state_rx(),
					self.config.pause.preview_overlay,
					self.cancel.clone(),
				)))
			} else {
				None
			}
		} else {
			None
		};

		let mut backoff = ReconnectBackoff::new(Duration::from_secs(2), Duration::from_secs(60));

		loop {
			if self.cancel.is_cancelled() {
				break;
			}

			// For battery cameras: wait for someone to need us before connecting.
			// This is the "parked" state — surface it to the preview overlay as
			// `Sleeping` so subscribers (MQTT preview publisher) can caption the
			// cached JPEG while the wake lock is zero.
			if self.config.idle_disconnect && self.wake_lock.is_idle() {
				self.set_preview_state(PreviewState::Sleeping);
				tracing::info!(camera = %self.config.name, "Idle disconnect enabled, waiting for wake lock...");
				tokio::select! {
					_ = self.cancel.cancelled() => break,
					_ = self.wake_lock.wait_for_acquire() => {
						tracing::info!(camera = %self.config.name, "Wake lock acquired, connecting...");
					}
				}
			}

			self.set_state(CameraState::Connecting);
			self.set_preview_state(PreviewState::Connecting);
			tracing::info!(camera = %self.config.name, "Connecting...");

			// Wrap connection attempt in a timeout + cancellation guard
			// so that Ctrl+C works even if bairelay_neolink_core is stuck
			let connect_result = tokio::select! {
				_ = self.cancel.cancelled() => break,
				result = tokio::time::timeout(Self::CONNECT_TIMEOUT, self.try_connect()) => {
					match result {
						Ok(r) => r,
						Err(_) => {
							tracing::warn!(camera = %self.config.name, "Connection timed out ({}s)", Self::CONNECT_TIMEOUT.as_secs());
							Err(anyhow::anyhow!("Connection timed out"))
						}
					}
				}
			};
			match connect_result {
				Ok(camera) => {
					let driver: Arc<dyn CameraDriver> =
						Arc::clone(&camera) as Arc<dyn CameraDriver>;
					backoff.reset();
					self.run_connected_session(driver, Some(Arc::clone(&camera)))
						.await;
				}
				Err(e) => {
					if is_login_failure(&e) {
						tracing::error!(camera = %self.config.name, error = %format!("{:#}", e), "Authentication failed — check credentials. Stopping retries.");
						self.set_state(CameraState::Disconnected);
						break;
					}
					tracing::warn!(camera = %self.config.name, error = %e, "Connection failed");
					self.set_state(CameraState::Disconnected);
				}
			}

			if !backoff.sleep_with_cancel(&self.cancel).await {
				break;
			}
		}

		// `run()` has returned ⇒ the camera is not connected. Most exit
		// paths already land on Disconnected, but a cancel that fires
		// while `try_connect()` is still in flight breaks out of the
		// loop with the state left at `Connecting` — an observable lie
		// for anything that reads `state()` after shutdown. Idempotent
		// on the paths that already set it.
		self.set_state(CameraState::Disconnected);

		// Ensure no stream sources outlive the camera handle even on
		// cancelled/early-exit paths (e.g. auth failure, cancellation
		// during backoff sleep). `run_connected_session` already
		// drained sources + logged out + cleared the BcCamera Arcs on
		// every successful session; this catches the paths that
		// bypassed `run_connected_session` entirely (auth failure,
		// timed-out connect, cancel during backoff).
		self.stop_all_stream_sources().await;

		// Camera-lifetime preview publisher: self.cancel has fired (we
		// exited the outer loop via break), so the task's select arm
		// has fired or will imminently. Await with a small timeout so
		// shutdown doesn't hang on a wedged broker.
		if let Some(task) = preview_task {
			let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
		}
	}
}

/// Session-scoped grace-period watcher: cancels the session once the
/// wake lock has been idle for the grace period. Exits as soon as the
/// session ends for any other reason (shutdown, connection loss) —
/// otherwise teardown would wait out its full drain window on a timer
/// that has nothing left to count down.
async fn grace_period_watch(gp: GracePeriod, session_cancel: CancellationToken, camera: String) {
	tokio::select! {
		_ = gp.run() => {
			tracing::info!(camera = %camera, "Grace period expired, disconnecting");
			session_cancel.cancel();
		}
		_ = session_cancel.cancelled() => {}
	}
}

/// Per-camera preview-state aggregator body. Spawned inside the
/// session scope of [`CameraHandle::run`] and torn
/// down via `session_cancel`.
///
/// Polls every 200 ms. The full state machine lives in
/// [`aggregate_preview_state`]; this loop only tracks `last_write` so
/// the pure fn can distinguish the aggregator's own `Connecting`
/// downgrade from a connect-loop-owned `Connecting` during reconnects.
async fn aggregate_preview_state_loop(
	handle: Arc<CameraHandle>,
	session_cancel: CancellationToken,
) {
	let mut ticker = tokio::time::interval(Duration::from_millis(200));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	// First tick fires immediately — skip it so we don't double-sample.
	ticker.tick().await;
	let mut last_write: Option<PreviewState> = None;
	loop {
		tokio::select! {
			_ = session_cancel.cancelled() => break,
			_ = ticker.tick() => {
				let current = *handle.preview_state_rx.borrow();
				let any_bridging = handle.any_stream_source_bridging();
				let next = aggregate_preview_state(current, any_bridging, last_write);
				if next != current {
					handle.set_preview_state(next);
					last_write = Some(next);
				} else if matches!(current, PreviewState::Sleeping)
					|| (matches!(current, PreviewState::Connecting)
						&& last_write != Some(PreviewState::Connecting))
				{
					// Connect-loop-owned state — clear our bookkeeping so
					// we don't later confuse it for our own downgrade.
					last_write = None;
				}
			}
		}
	}
}

/// Check whether an error indicates a login/credential failure that
/// should not be retried.
///
/// Walks the anyhow source chain and matches typed
/// `bairelay_neolink_core::Error::AuthFailed | CameraLoginFail`. The Debug-
/// substring fallback below catches synthesised `anyhow::anyhow!(...)`
/// errors used by some test paths and any future intermediate wrapper
/// that doesn't preserve a downcastable source — keeping it as a
/// belt-and-braces guard so a future bairelay_neolink_core error-type rename
/// doesn't silently regress to "retry auth failures forever" — the
/// brute-force scenario flagged during the strict-code audit.
fn is_login_failure(err: &anyhow::Error) -> bool {
	use bairelay_neolink_core::Error as CoreError;
	for cause in err.chain() {
		if let Some(core) = cause.downcast_ref::<CoreError>() {
			if matches!(core, CoreError::AuthFailed | CoreError::CameraLoginFail) {
				return true;
			}
		}
	}
	let msg = format!("{:?}", err);
	msg.contains("AuthFailed")
		|| msg.contains("CameraLoginFail")
		|| msg.contains("Credential error")
}

// ── ReconnectBackoff ──────────────────────────────────────────────────

pub struct ReconnectBackoff {
	initial: Duration,
	max: Duration,
	current: Duration,
}

impl ReconnectBackoff {
	pub fn new(initial: Duration, max: Duration) -> Self {
		Self {
			initial,
			max,
			current: initial,
		}
	}

	pub fn next_delay(&mut self) -> Duration {
		let delay = self.current;
		self.current = (self.current * 2).min(self.max);
		delay
	}

	pub fn reset(&mut self) {
		self.current = self.initial;
	}

	/// Sleep for the next backoff delay, racing against `cancel`.
	/// Returns `true` if the sleep completed; `false` if the
	/// cancellation token fired first. Delegates the sleep+cancel race
	/// to [`crate::run_support::sleep_or_cancel`] so the contract is
	/// shared with the MQTT event loop's reconnect path.
	pub async fn sleep_with_cancel(&mut self, cancel: &CancellationToken) -> bool {
		let delay = self.next_delay();
		crate::run_support::sleep_or_cancel(delay, cancel).await
	}
}

/// Outcome of [`drive_reconnect_with_backoff`]: either a successful
/// connect, a bail-out signal from the connect fn (auth failure), or
/// cancellation.
#[cfg(test)]
#[derive(Debug)]
#[allow(dead_code)] // variant payloads inspected via Debug only
pub(crate) enum ReconnectOutcome<T> {
	/// Connect callback returned `Ok(T)` — the caller now owns the
	/// connected handle.
	Connected(T),
	/// Connect callback returned `Err` and `should_bail(err) == true`
	/// — e.g. an auth failure that must not be retried. The original
	/// error is returned so the caller can log it.
	Bailed(anyhow::Error),
	/// `cancel.cancelled()` fired while waiting for the next attempt.
	Cancelled,
}

/// Run the connect / backoff / cancel loop against an injected
/// connect callable and clock, producing a [`ReconnectOutcome`].
///
/// Keeps the pure reconnect-timing logic testable independently of
/// the camera's full [`CameraHandle`] machinery (wake lock, preview
/// state, MQTT publish). The backoff sleep delegates to
/// [`ReconnectBackoff::sleep_with_cancel`] — the same path
/// `CameraHandle::run` uses — so the
/// "sleep-then-retry-with-cancel" contract is genuinely shared.
#[cfg(test)]
pub(crate) async fn drive_reconnect_with_backoff<T, F, Fut>(
	mut backoff: ReconnectBackoff,
	cancel: CancellationToken,
	mut connect: F,
	should_bail: impl Fn(&anyhow::Error) -> bool,
) -> ReconnectOutcome<T>
where
	F: FnMut() -> Fut,
	Fut: std::future::Future<Output = anyhow::Result<T>>,
{
	loop {
		if cancel.is_cancelled() {
			return ReconnectOutcome::Cancelled;
		}
		let attempt = tokio::select! {
			_ = cancel.cancelled() => return ReconnectOutcome::Cancelled,
			r = connect() => r,
		};
		match attempt {
			Ok(value) => return ReconnectOutcome::Connected(value),
			Err(e) => {
				if should_bail(&e) {
					return ReconnectOutcome::Bailed(e);
				}
				if !backoff.sleep_with_cancel(&cancel).await {
					return ReconnectOutcome::Cancelled;
				}
			}
		}
	}
}

#[cfg(test)]
impl CameraHandle {
	/// Test helper: insert a pre-built [`StreamSource`] into the per-camera
	/// registry without going through the connect-and-spawn path. Used by
	/// the prune-grace unit tests.
	pub(crate) fn insert_stream_source_for_test(
		&self,
		kind: RtspStreamKind,
		source: Arc<StreamSource>,
	) {
		self.stream_sources.write_recover().insert(kind, source);
	}

	/// Test helper: does the per-camera registry currently hold a source
	/// for `kind`?
	pub(crate) fn has_stream_source_for_test(&self, kind: RtspStreamKind) -> bool {
		self.stream_sources.read_recover().contains_key(&kind)
	}

	/// Test helper: subscribe to a registered source's broadcast channel
	/// (bumping `receiver_count` by one) without pulling in the full RTSP
	/// provider subscription machinery.
	pub(crate) fn subscribe_stream_for_test(
		&self,
		kind: RtspStreamKind,
	) -> tokio::sync::broadcast::Receiver<bairelay_rtsp::provider::Frame> {
		let guard = self.stream_sources.read_recover();
		guard
			.get(&kind)
			.expect("stream source present for test")
			.subscribe_for_test()
	}

	/// Test helper: install a `CameraDriver` (typically a
	/// `FakeCamera`) + flip state to `Connected` so
	/// `mqtt_dispatch::dispatch_control` finds a driver via
	/// `bc_camera()` and proceeds past the disconnected short-circuit.
	/// Bypasses `try_connect`; the concrete `bc_camera_concrete` slot
	/// stays `None` because fakes are trait-only.
	#[allow(dead_code)] // used by upcoming mqtt_dispatch tests in this phase
	pub(crate) fn set_driver_for_test(&self, driver: Arc<dyn CameraDriver>) {
		*self.bc_camera.write_recover() = Some(driver);
		self.set_state(CameraState::Connected);
	}

	/// Test-only entry into the post-connect session lifecycle.
	/// Runs [`Self::run_connected_session`] with `concrete = None`,
	/// skipping the stream-source spawn / logout paths that require a
	/// real [`BcCamera`]. Use together with a scripted `FakeCamera` to
	/// exercise the capability probe + session-tasks + keepalive +
	/// teardown code without a live TCP socket.
	pub(crate) async fn run_connected_session_for_test(
		self: &Arc<Self>,
		driver: Arc<dyn CameraDriver>,
	) {
		self.run_connected_session(driver, None).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The grace-period watcher exits when the session ends for another
	/// reason — it must not hold teardown's drain window while the wake
	/// lock is still held and its countdown never started.
	#[tokio::test]
	async fn grace_period_watch_exits_on_session_cancel() {
		let lock = WakeLockCounter::new();
		let _held = lock.acquire();
		let gp = GracePeriod::new(lock.clone(), Duration::from_secs(45));
		let session_cancel = CancellationToken::new();
		let watch = tokio::spawn(grace_period_watch(gp, session_cancel.clone(), "cam".into()));
		session_cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), watch)
			.await
			.expect("watcher must exit on session cancel")
			.unwrap();
	}

	#[test]
	fn new_camera_handle_has_audio_presence_unknown() {
		use crate::audio_presence::AudioPresence;
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);
		let presence = *handle.audio_presence().read().expect("presence lock");
		assert_eq!(presence, AudioPresence::Unknown);
	}

	#[test]
	fn capabilities_defaults_to_none() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);
		assert!(handle.capabilities().is_none());
	}

	#[test]
	fn capabilities_cached_after_set() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);
		handle.set_capabilities_for_test(CameraCapabilities {
			has_ptz: true,
			status_led: bairelay_mqtt::discovery::Presence::Present,
			ir_leds: bairelay_mqtt::discovery::Presence::Present,
		});
		let caps = handle.capabilities().expect("caps populated");
		assert!(caps.has_ptz);
	}

	#[test]
	fn camera_handle_initial_preview_state_is_sleeping() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::preview_state::PreviewState;
		use tokio_util::sync::CancellationToken;

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);
		let rx = handle.preview_state_rx();
		assert_eq!(*rx.borrow(), PreviewState::Sleeping);
	}

	/// Observes the `Sleeping → Connecting` transition driven by the
	/// connect loop. We spawn `run()` against an unroutable address so
	/// the loop enters its connect attempt (flipping preview state to
	/// `Connecting`) but never actually establishes a session. The test
	/// cancels as soon as the transition is observed.
	#[tokio::test]
	async fn preview_state_transitions_to_connecting_during_connect() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::preview_state::PreviewState;
		use tokio_util::sync::CancellationToken;

		// Unroutable-but-parseable target: loopback port 1 refuses
		// connections quickly on every supported platform, and the
		// `Local` discovery method short-circuits to a direct connect
		// when an IP is supplied.
		let mut config = minimal_camera_config("cam");
		config.address = Some("127.0.0.1:1".to_string());
		// Disable idle_disconnect so the loop does NOT park in
		// `Sleeping` waiting for a wake lock — we want it to march
		// straight into the connect attempt.
		config.idle_disconnect = false;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel.clone(), None));
		let mut rx = handle.preview_state_rx();
		assert_eq!(*rx.borrow_and_update(), PreviewState::Sleeping);

		let run_handle = {
			let h = Arc::clone(&handle);
			tokio::spawn(async move { h.run().await })
		};

		let saw_connecting = tokio::time::timeout(Duration::from_secs(2), async {
			loop {
				if *rx.borrow_and_update() == PreviewState::Connecting {
					return true;
				}
				if rx.changed().await.is_err() {
					return false;
				}
			}
		})
		.await
		.unwrap_or(false);

		// Tear down before asserting so a failing assert doesn't leak
		// the background task.
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(5), run_handle).await;

		assert!(
			saw_connecting,
			"preview state never transitioned to Connecting"
		);
	}

	// ── Preview-state aggregator ────────────────────

	#[test]
	fn aggregate_preview_state_live_downgrades_on_bridging() {
		assert_eq!(
			aggregate_preview_state(PreviewState::Live, true, None),
			PreviewState::Connecting
		);
		assert_eq!(
			aggregate_preview_state(PreviewState::Live, false, None),
			PreviewState::Live
		);
	}

	#[test]
	fn aggregate_preview_state_sleeping_never_changes() {
		assert_eq!(
			aggregate_preview_state(PreviewState::Sleeping, true, None),
			PreviewState::Sleeping
		);
		assert_eq!(
			aggregate_preview_state(PreviewState::Sleeping, false, None),
			PreviewState::Sleeping
		);
		assert_eq!(
			aggregate_preview_state(PreviewState::Sleeping, true, Some(PreviewState::Connecting)),
			PreviewState::Sleeping
		);
	}

	#[test]
	fn aggregate_upgrades_from_own_downgrade_when_bridging_clears() {
		assert_eq!(
			aggregate_preview_state(
				PreviewState::Connecting,
				false,
				Some(PreviewState::Connecting)
			),
			PreviewState::Live,
		);
	}

	#[test]
	fn aggregate_stays_connecting_on_connect_loop_owned() {
		assert_eq!(
			aggregate_preview_state(PreviewState::Connecting, false, None),
			PreviewState::Connecting,
		);
		assert_eq!(
			aggregate_preview_state(PreviewState::Connecting, true, None),
			PreviewState::Connecting,
		);
	}

	#[test]
	fn aggregate_stays_connecting_when_bridging_still_active() {
		assert_eq!(
			aggregate_preview_state(
				PreviewState::Connecting,
				true,
				Some(PreviewState::Connecting)
			),
			PreviewState::Connecting,
		);
	}

	// ── Post-keepalive PreviewState selector ──────────────────────────

	#[test]
	fn post_keepalive_sleeping_only_when_idle_disconnect_and_wake_lock_idle() {
		// True parked case: idle_disconnect configured + no wake lock held.
		assert_eq!(
			post_keepalive_preview_state(true, true),
			PreviewState::Sleeping,
		);
	}

	#[test]
	fn post_keepalive_connecting_when_wake_lock_held() {
		// Reconnect-in-progress: somebody still wants the camera awake.
		assert_eq!(
			post_keepalive_preview_state(true, false),
			PreviewState::Connecting,
		);
	}

	#[test]
	fn post_keepalive_connecting_when_idle_disconnect_disabled() {
		// Always-on mode: camera never parks, so a keepalive-loop return
		// always implies a reconnect cycle, never a sleep.
		assert_eq!(
			post_keepalive_preview_state(false, true),
			PreviewState::Connecting,
		);
		assert_eq!(
			post_keepalive_preview_state(false, false),
			PreviewState::Connecting,
		);
	}

	// ── Keepalive tick classification & counter ─────────────────────
	//
	// Stage 6 Task 33 coverage. Drives `classify_keepalive_tick` +
	// `advance_keepalive_counter` with scripted inputs so we don't need
	// a live `get_linktype()` round-trip to pin the decision table.

	#[derive(Debug)]
	#[allow(dead_code)] // Field shown in Debug output via the derive.
	struct FakeErr(&'static str);

	#[test]
	fn classify_ok_reply_is_ok() {
		let raw: std::result::Result<
			std::result::Result<(), FakeErr>,
			tokio::time::error::Elapsed,
		> = Ok(Ok(()));
		assert_eq!(classify_keepalive_tick(raw), KeepaliveTickOutcome::Ok);
	}

	// The "UnintelligibleReply ↦ Ok" mapping moved to
	// `CameraDriver::keepalive_probe` and is unit-tested in
	// `crates/core/src/bc_protocol/link.rs`.

	#[test]
	fn classify_other_err_is_failed() {
		let raw: std::result::Result<
			std::result::Result<(), FakeErr>,
			tokio::time::error::Elapsed,
		> = Ok(Err(FakeErr("ConnectionReset")));
		assert_eq!(classify_keepalive_tick(raw), KeepaliveTickOutcome::Failed);
	}

	#[test]
	fn classify_timeout_is_failed() {
		// Construct an `Elapsed` by racing a 0 ms timeout against a
		// never-completing future — only way to get hold of the private
		// `Elapsed` variant in stable tokio. `current_thread` flavour:
		// this test races one timeout to completion and exits, so a
		// worker pool would be wasted overhead.
		let raw: std::result::Result<std::result::Result<(), FakeErr>, _> =
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.unwrap()
				.block_on(async {
					tokio::time::timeout(
						Duration::from_millis(0),
						futures::future::pending::<std::result::Result<(), FakeErr>>(),
					)
					.await
				});
		assert_eq!(classify_keepalive_tick(raw), KeepaliveTickOutcome::Failed);
	}

	#[test]
	fn advance_ok_resets_counter() {
		assert_eq!(
			advance_keepalive_counter(4, KeepaliveTickOutcome::Ok, 5),
			(0, false),
		);
	}

	#[test]
	fn advance_failed_increments_and_does_not_break_below_max() {
		assert_eq!(
			advance_keepalive_counter(3, KeepaliveTickOutcome::Failed, 5),
			(4, false),
		);
	}

	#[test]
	fn advance_failed_breaks_at_max() {
		// Hitting MAX_FAILURES exactly must trigger the break flag —
		// this is the "5 misses and we're out" edge.
		assert_eq!(
			advance_keepalive_counter(4, KeepaliveTickOutcome::Failed, 5),
			(5, true),
		);
	}

	#[test]
	fn advance_saturates_on_overflow() {
		// Paranoia: if we somehow get past the break (caller ignored
		// the flag) the counter must not wrap around to 0 and re-enable
		// the "healthy" branch.
		assert_eq!(
			advance_keepalive_counter(u32::MAX, KeepaliveTickOutcome::Failed, 5),
			(u32::MAX, true),
		);
	}

	// ── End-to-end cadence with the paused virtual clock ────────────
	//
	// Drives the raw `interval` + `classify` + `advance` wiring via the
	// same helpers the real `keepalive_loop` uses. We don't invoke
	// `keepalive_loop` itself because it wants a real `BcCamera`; the
	// contract under test is "one probe every KEEPALIVE_INTERVAL, and
	// MAX_FAILURES consecutive failures terminate the loop."

	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn keepalive_cadence_drives_one_probe_per_interval() {
		let mut interval = tokio::time::interval(CameraHandle::KEEPALIVE_INTERVAL);
		// First tick fires immediately under tokio's default behaviour.
		let t0 = tokio::time::Instant::now();
		interval.tick().await;
		let t1 = tokio::time::Instant::now();
		// Second tick should require advancing the virtual clock by the
		// configured interval.
		tokio::time::advance(CameraHandle::KEEPALIVE_INTERVAL).await;
		interval.tick().await;
		let t2 = tokio::time::Instant::now();
		assert_eq!(t1.duration_since(t0), Duration::ZERO);
		assert_eq!(t2.duration_since(t1), CameraHandle::KEEPALIVE_INTERVAL);
	}

	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn keepalive_counter_breaks_after_max_consecutive_failures() {
		// Scripted sequence of all-failed ticks; counter must break on
		// exactly the MAX_FAILURES-th failure, no sooner, no later.
		let mut failures: u32 = 0;
		let mut broke_on: Option<u32> = None;
		for i in 1..=(CameraHandle::KEEPALIVE_MAX_FAILURES + 2) {
			let (next, should_break) = advance_keepalive_counter(
				failures,
				KeepaliveTickOutcome::Failed,
				CameraHandle::KEEPALIVE_MAX_FAILURES,
			);
			failures = next;
			if should_break {
				broke_on = Some(i);
				break;
			}
		}
		assert_eq!(broke_on, Some(CameraHandle::KEEPALIVE_MAX_FAILURES));
	}

	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn keepalive_counter_resets_after_single_ok_tick() {
		// Four failures then one success must zero the counter — the
		// "intermittent blip" recovery path.
		let mut failures: u32 = 0;
		for _ in 0..4 {
			let (next, should_break) = advance_keepalive_counter(
				failures,
				KeepaliveTickOutcome::Failed,
				CameraHandle::KEEPALIVE_MAX_FAILURES,
			);
			assert!(!should_break);
			failures = next;
		}
		assert_eq!(failures, 4);
		let (next, should_break) = advance_keepalive_counter(
			failures,
			KeepaliveTickOutcome::Ok,
			CameraHandle::KEEPALIVE_MAX_FAILURES,
		);
		assert_eq!(next, 0);
		assert!(!should_break);
	}

	// ── is_login_failure (typed match + Debug-substring fallback) ───

	/// Pin the production contract: a real `bairelay_neolink_core::Error::AuthFailed`
	/// (the variant `try_connect()` actually returns) must drive the
	/// permanent-bail. Pre-fix, this would have only worked accidentally
	/// via the Debug-substring fallback; with typed downcast it's
	/// guaranteed by the `From` impl in anyhow.
	#[test]
	fn is_login_failure_recognises_typed_auth_failed() {
		let e: anyhow::Error = bairelay_neolink_core::Error::AuthFailed.into();
		assert!(is_login_failure(&e));
	}

	#[test]
	fn is_login_failure_recognises_typed_camera_login_fail() {
		let e: anyhow::Error = bairelay_neolink_core::Error::CameraLoginFail.into();
		assert!(is_login_failure(&e));
	}

	/// `try_connect()` may surface auth failures wrapped in `.context(..)`
	/// from a future call-site; the chain walk must catch them.
	#[test]
	fn is_login_failure_recognises_typed_auth_failed_through_context() {
		use anyhow::Context;
		let e: anyhow::Result<()> = Err(bairelay_neolink_core::Error::AuthFailed.into());
		let wrapped = e.context("connect_with_retry").unwrap_err();
		assert!(is_login_failure(&wrapped));
	}

	// Belt-and-braces: the Debug-substring fallback still fires for
	// synthesised `anyhow::anyhow!(...)` errors (used by some test
	// paths and any future intermediate wrapper that doesn't preserve
	// a downcastable source).

	#[test]
	fn is_login_failure_recognises_auth_fail() {
		let e: anyhow::Error = anyhow::anyhow!("remote reported AuthFailed for cam-alpha");
		assert!(is_login_failure(&e));
	}

	#[test]
	fn is_login_failure_recognises_camera_login_fail() {
		let e: anyhow::Error = anyhow::anyhow!("CameraLoginFail on port 9000");
		assert!(is_login_failure(&e));
	}

	#[test]
	fn is_login_failure_recognises_credential_error() {
		let e: anyhow::Error = anyhow::anyhow!("Credential error: invalid password hash");
		assert!(is_login_failure(&e));
	}

	#[test]
	fn is_login_failure_rejects_generic_io() {
		let e: anyhow::Error = anyhow::anyhow!("connection reset by peer during initial handshake");
		assert!(!is_login_failure(&e));
	}

	// ── ReconnectBackoff mechanics ──────────────────────────────────

	#[test]
	fn reconnect_backoff_starts_at_initial_and_doubles() {
		let mut b = ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(400));
		assert_eq!(b.next_delay(), Duration::from_millis(100));
		assert_eq!(b.next_delay(), Duration::from_millis(200));
		assert_eq!(b.next_delay(), Duration::from_millis(400));
		// Clamps at max on subsequent calls.
		assert_eq!(b.next_delay(), Duration::from_millis(400));
	}

	// ── drive_reconnect_with_backoff ────────────────────────────────

	/// After N failed attempts, the next success returns
	/// `Connected(value)` and the virtual clock advanced by exactly
	/// the expected sum of backoff delays. Uses tokio's paused-time
	/// clock so the test is deterministic regardless of real time.
	#[tokio::test(start_paused = true)]
	async fn drive_reconnect_retries_until_success_with_doubling_backoff() {
		use std::sync::atomic::{AtomicU32, Ordering};

		let calls = Arc::new(AtomicU32::new(0));
		let calls_c = Arc::clone(&calls);
		let backoff = ReconnectBackoff::new(Duration::from_secs(2), Duration::from_secs(60));
		let cancel = CancellationToken::new();

		let start = tokio::time::Instant::now();
		let outcome = drive_reconnect_with_backoff::<_, _, _>(
			backoff,
			cancel,
			move || {
				let calls_c = Arc::clone(&calls_c);
				async move {
					let n = calls_c.fetch_add(1, Ordering::AcqRel);
					if n < 3 {
						Err(anyhow::anyhow!("transient failure #{n}"))
					} else {
						Ok::<u32, anyhow::Error>(n)
					}
				}
			},
			|_| false,
		)
		.await;

		match outcome {
			ReconnectOutcome::Connected(n) => assert_eq!(n, 3),
			other => panic!("expected Connected(3), got {:?}", other),
		}
		assert_eq!(
			calls.load(Ordering::Acquire),
			4,
			"connect should be called four times (three Err, one Ok)"
		);

		// Three failures → three sleeps: 2 + 4 + 8 = 14 s.
		let elapsed = start.elapsed();
		assert!(
			elapsed >= Duration::from_secs(14) && elapsed < Duration::from_secs(15),
			"elapsed {:?} should be ~14s (2+4+8 backoff)",
			elapsed
		);
	}

	/// `should_bail` returns true on the first error → the loop exits
	/// with `Bailed(err)` without retrying. Pins the "auth failure
	/// short-circuit" contract used by `is_login_failure` in the real
	/// loop.
	#[tokio::test(start_paused = true)]
	async fn drive_reconnect_bails_on_should_bail() {
		use std::sync::atomic::{AtomicU32, Ordering};

		let calls = Arc::new(AtomicU32::new(0));
		let calls_c = Arc::clone(&calls);
		let backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
		let cancel = CancellationToken::new();

		let outcome = drive_reconnect_with_backoff::<(), _, _>(
			backoff,
			cancel,
			move || {
				calls_c.fetch_add(1, Ordering::AcqRel);
				async move { Err(anyhow::anyhow!("AuthFailed")) }
			},
			|e| format!("{e:?}").contains("AuthFailed"),
		)
		.await;

		assert!(
			matches!(outcome, ReconnectOutcome::Bailed(_)),
			"expected Bailed, got {:?}",
			outcome
		);
		assert_eq!(
			calls.load(Ordering::Acquire),
			1,
			"bail-on-auth must not retry"
		);
	}

	/// Cancellation mid-backoff short-circuits the sleep and returns
	/// `Cancelled` without waiting the full delay. Uses paused time so
	/// the test exits instantly on the cancel signal.
	#[tokio::test(start_paused = true)]
	async fn drive_reconnect_cancels_during_backoff_sleep() {
		let backoff = ReconnectBackoff::new(Duration::from_secs(60), Duration::from_secs(60));
		let cancel = CancellationToken::new();
		let cancel_c = cancel.clone();

		// Trip cancel shortly after the first Err lands. Paused time
		// means the 60 s sleep would otherwise never resolve.
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_c.cancel();
		});

		let outcome = drive_reconnect_with_backoff::<(), _, _>(
			backoff,
			cancel,
			|| async { Err(anyhow::anyhow!("transient")) },
			|_| false,
		)
		.await;

		assert!(
			matches!(outcome, ReconnectOutcome::Cancelled),
			"expected Cancelled, got {:?}",
			outcome
		);
	}

	#[test]
	fn reconnect_backoff_resets_to_initial() {
		let mut b = ReconnectBackoff::new(Duration::from_millis(50), Duration::from_secs(1));
		let _ = b.next_delay();
		let _ = b.next_delay();
		b.reset();
		assert_eq!(b.next_delay(), Duration::from_millis(50));
	}

	#[tokio::test]
	async fn aggregator_upgrades_back_to_live_when_gap_clears() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::preview_state::PreviewState;
		use crate::stream_source::{GapState, StreamSource};

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel, None));

		// Simulate a successful connect by flipping preview state to Live.
		handle.set_preview_state(PreviewState::Live);
		let mut rx = handle.preview_state_rx();
		assert_eq!(*rx.borrow_and_update(), PreviewState::Live);

		// Register an inert source and flip it to `Bridging`.
		let source = StreamSource::start_inert_for_test();
		handle.insert_stream_source_for_test(RtspStreamKind::Main, Arc::clone(&source));
		source.set_gap_state_for_test(GapState::Bridging);

		// Spawn the aggregator loop.
		let session_cancel = CancellationToken::new();
		let loop_handle = {
			let h = Arc::clone(&handle);
			let c = session_cancel.clone();
			tokio::spawn(async move { aggregate_preview_state_loop(h, c).await })
		};

		// Wait for the downgrade to Connecting.
		let saw_downgrade = tokio::time::timeout(Duration::from_secs(2), async {
			loop {
				if *rx.borrow_and_update() == PreviewState::Connecting {
					return true;
				}
				if rx.changed().await.is_err() {
					return false;
				}
			}
		})
		.await
		.unwrap_or(false);
		assert!(
			saw_downgrade,
			"aggregator never downgraded Live → Connecting while Bridging"
		);

		// Gap clears → aggregator should upgrade back to Live.
		source.set_gap_state_for_test(GapState::Live);

		let saw_upgrade = tokio::time::timeout(Duration::from_secs(2), async {
			loop {
				if *rx.borrow_and_update() == PreviewState::Live {
					return true;
				}
				if rx.changed().await.is_err() {
					return false;
				}
			}
		})
		.await
		.unwrap_or(false);

		// Tear down before asserting so a failing assert doesn't leak
		// the background task.
		session_cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;

		assert!(
			saw_upgrade,
			"aggregator never upgraded Connecting → Live after gap cleared"
		);
	}

	/// Drive `run()` against a parked camera with `idle_disconnect = true`
	/// and no wake lock held. The loop must flip `PreviewState` to
	/// `Sleeping` and await wake-lock acquisition before attempting any
	/// connect. Cancellation lifts the loop out cleanly.
	#[tokio::test]
	async fn run_parks_in_sleeping_when_idle_disconnect_and_no_wake_lock() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::preview_state::PreviewState;
		use tokio_util::sync::CancellationToken;

		let mut config = minimal_camera_config("cam-park");
		config.idle_disconnect = true;
		// Unroutable address: if the park arm races our assertion,
		// the test still won't reach the live network.
		config.address = Some("127.0.0.1:1".to_string());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel.clone(), None));
		let mut rx = handle.preview_state_rx();

		let run_handle = {
			let h = Arc::clone(&handle);
			tokio::spawn(async move { h.run().await })
		};

		let saw_sleeping = tokio::time::timeout(Duration::from_secs(2), async {
			loop {
				if *rx.borrow_and_update() == PreviewState::Sleeping {
					return true;
				}
				if rx.changed().await.is_err() {
					return false;
				}
			}
		})
		.await
		.unwrap_or(false);

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(5), run_handle).await;

		assert!(saw_sleeping, "idle_disconnect loop must flip to Sleeping");
	}

	/// `run()` with an MQTT client publishes `status/connection = off`
	/// and a motion-unknown payload before entering the connect loop
	/// (lines 736-741). Parks via `idle_disconnect` so the network
	/// attempt never fires.
	#[tokio::test]
	async fn run_publishes_initial_disconnected_status_when_mqtt_present() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let mut config = minimal_camera_config("cam-init");
		config.idle_disconnect = true;
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel.clone(), Some(mqtt)));

		let run_handle = {
			let h = Arc::clone(&handle);
			tokio::spawn(async move { h.run().await })
		};

		let saw_init = tokio::time::timeout(Duration::from_secs(2), async {
			loop {
				let rows = mock.published();
				let has_conn = rows
					.iter()
					.any(|(t, p, _)| t == "bairelay/cam-init/status" && p == b"disconnected");
				if has_conn {
					return true;
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.unwrap_or(false);

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(5), run_handle).await;
		assert!(
			saw_init,
			"initial disconnected publish must land on connection topic; observed: {:?}",
			mock.published_topics()
		);
	}

	/// `run()` against an unroutable address hits the
	/// `connect_result = Err` → `is_login_failure` branch (returns
	/// false on a connection reset/timeout), logs the connect failure,
	/// and enters the backoff sleep. We cancel before the 2s sleep
	/// finishes so the task exits via the `sleep_with_cancel → false`
	/// branch (line 1046 break).
	#[tokio::test]
	async fn run_backoffs_after_connect_failure_then_exits_on_cancel() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let mut config = minimal_camera_config("cam-fail");
		config.idle_disconnect = false;
		config.address = Some("127.0.0.1:1".to_string());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel.clone(), None));

		let run_handle = {
			let h = Arc::clone(&handle);
			tokio::spawn(async move { h.run().await })
		};

		// A refused connect on 127.0.0.1:1 fails in well under a
		// millisecond, so by now the loop is parked in the 2 s backoff
		// sleep and the cancel below exercises the
		// `sleep_with_cancel → false` branch. On a heavily instrumented
		// runner the connect can still be in flight instead, in which
		// case cancel breaks out of the connect `select!` — the final
		// assertion holds either way, because `run()` guarantees
		// Disconnected on every exit path.
		tokio::time::sleep(Duration::from_millis(500)).await;
		cancel.cancel();
		let join_outcome = tokio::time::timeout(Duration::from_secs(10), run_handle).await;
		// Task must actually have completed (not been canceled by the
		// timeout). A regression that breaks cancel-during-backoff
		// would leave the loop spinning past the 10 s timeout, and the
		// pre-fix `let _ = ...` swallowed that case silently.
		let join_result = join_outcome.expect("run() did not exit within 10 s after cancel");
		join_result.expect("run() task panicked");
		// Final state must be Disconnected — pinned so a future change
		// to leave state as Connecting on cancel-during-backoff would
		// surface here.
		assert_eq!(handle.state(), CameraState::Disconnected);
	}

	/// Cancelling while `try_connect()` is still in flight breaks out of
	/// the connect `select!` rather than the backoff sleep. `run()` must
	/// still leave the camera Disconnected: otherwise anything reading
	/// `state()` after shutdown sees a camera that claims Connecting
	/// forever.
	#[tokio::test]
	async fn run_cancelled_mid_connect_still_ends_disconnected() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let mut config = minimal_camera_config("cam-midconnect");
		config.idle_disconnect = false;
		// TEST-NET-1 blackholes rather than refusing, so the connect
		// stays in flight instead of failing fast like a closed port.
		config.address = Some("192.0.2.1:9000".to_string());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel.clone(), None));

		let run_handle = {
			let h = Arc::clone(&handle);
			tokio::spawn(async move { h.run().await })
		};

		// Long enough for run() to set Connecting and enter try_connect().
		tokio::time::sleep(Duration::from_millis(200)).await;
		cancel.cancel();

		// Exiting well inside CONNECT_TIMEOUT (100 s) proves cancel broke
		// the in-flight connect rather than waiting the attempt out.
		tokio::time::timeout(Duration::from_secs(10), run_handle)
			.await
			.expect("cancel must break an in-flight connect, not wait for CONNECT_TIMEOUT")
			.expect("run() task panicked");

		assert_eq!(handle.state(), CameraState::Disconnected);
	}

	// ── Accessor / miscellaneous surface coverage ───────────────────

	#[test]
	fn simple_accessors_reflect_initial_state() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let config = minimal_camera_config("cam-acc");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);
		assert_eq!(handle.name(), "cam-acc");
		assert_eq!(handle.topic_prefix(), "bairelay");
		assert_eq!(handle.state(), CameraState::Disconnected);
		assert!(!handle.is_cancelled());
		assert!(handle.bc_camera().is_none());
		assert_eq!(handle.config().name, "cam-acc");
		// Touch the cancel_token accessor — it must match the one the
		// `new()` ctor stored internally (child of the parent token).
		assert!(!handle.cancel_token().is_cancelled());
	}

	#[test]
	fn request_disconnect_fires_notify_without_panicking() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-rd"), cancel, None);
		// Safe to call with no waiter — notify_one is edge-triggered.
		handle.request_disconnect();
	}

	#[test]
	fn with_bcmedia_dump_and_prefix_preserves_prefix() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let cancel = CancellationToken::new();
		let handle = CameraHandle::with_bcmedia_dump_and_prefix(
			minimal_camera_config("cam-x"),
			cancel,
			None,
			"neolink".to_string(),
			None,
		);
		assert_eq!(handle.topic_prefix(), "neolink");
	}

	#[test]
	fn last_frame_main_returns_clonable_arc() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-lfm"), cancel, None);
		let a = handle.last_frame_main();
		let b = handle.last_frame_main();
		assert!(Arc::ptr_eq(&a, &b));
	}

	#[test]
	fn camera_state_variant_predicates_are_exclusive() {
		assert!(CameraState::Disconnected.is_disconnected());
		assert!(!CameraState::Disconnected.is_connecting());
		assert!(!CameraState::Disconnected.is_connected());
		assert!(CameraState::Connecting.is_connecting());
		assert!(!CameraState::Connecting.is_disconnected());
		assert!(CameraState::Connected.is_connected());
		assert!(!CameraState::Connected.is_connecting());
	}

	/// `publish_discovery` and `unpublish_discovery` early-return Ok
	/// when no `DiscoveryPublisher` is attached — the common case for
	/// a config without `[mqtt.discovery]`.
	#[tokio::test]
	async fn discovery_no_publisher_noop_ok() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-np"), cancel, None);
		handle.publish_discovery().await.expect("ok noop");
		handle.unpublish_discovery().await.expect("ok noop");
	}

	/// `publish_discovery` / `unpublish_discovery` are also no-ops when
	/// a publisher is present but the capabilities cache is still None
	/// (first connect hasn't finished). Exercises the second early
	/// return branch in both methods.
	#[tokio::test]
	async fn discovery_caps_none_noop_ok() {
		use crate::config::test_helpers::minimal_camera_config;
		use std::collections::HashSet;
		use tokio_util::sync::CancellationToken;

		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let publisher = bairelay_mqtt::DiscoveryPublisher::new(
			mqtt,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			HashSet::new(),
			"test".to_string(),
		);
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-nc"), cancel, None)
			.with_discovery_publisher(publisher);
		// Caps still None → both paths early-return.
		handle
			.publish_discovery()
			.await
			.expect("ok noop on None caps");
		handle
			.unpublish_discovery()
			.await
			.expect("ok noop on None caps");
	}

	/// With caps populated and a publisher attached,
	/// `publish_discovery` emits MQTT traffic and `unpublish_discovery`
	/// emits retained-empty payloads on the same topics. We only
	/// assert the published count increases; the exact topic set is
	/// owned by the bairelay_mqtt crate and has its own tests.
	#[tokio::test]
	async fn discovery_publishes_and_unpublishes_with_caps() {
		use crate::config::test_helpers::minimal_camera_config;
		use std::collections::HashSet;
		use tokio_util::sync::CancellationToken;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let mut features = HashSet::new();
		features.insert(bairelay_mqtt::discovery::Feature::Camera);
		features.insert(bairelay_mqtt::discovery::Feature::Motion);
		let publisher = bairelay_mqtt::DiscoveryPublisher::new(
			mqtt,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-dp"), cancel, None)
			.with_discovery_publisher(publisher);
		handle.set_capabilities_for_test(CameraCapabilities {
			has_ptz: true,
			status_led: bairelay_mqtt::discovery::Presence::Present,
			ir_leds: bairelay_mqtt::discovery::Presence::Present,
		});

		let before = mock.published().len();
		handle.publish_discovery().await.expect("publish ok");
		let after_pub = mock.published().len();
		assert!(
			after_pub > before,
			"publish_discovery must emit retained payloads; before={before} after={after_pub}"
		);

		handle.unpublish_discovery().await.expect("unpublish ok");
		let after_unpub = mock.published().len();
		assert!(
			after_unpub > after_pub,
			"unpublish_discovery must emit retained-empty payloads; after_pub={after_pub} after_unpub={after_unpub}"
		);
	}

	/// `stream_source()` returns `Unavailable` when the parent cancel
	/// token fires before the camera reaches `Connected`.
	#[tokio::test]
	async fn stream_source_returns_unavailable_when_cancelled() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let parent = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-sc"),
			parent.clone(),
			None,
		));
		parent.cancel();

		let err = handle
			.stream_source(RtspStreamKind::Main)
			.await
			.err()
			.expect("cancelled → Unavailable");
		assert!(matches!(err, StreamError::Unavailable(_)));
	}

	/// With state already `Connected` (via `set_driver_for_test`)
	/// but no concrete `BcCamera` available, `stream_source()`
	/// returns `Unavailable` on the "bc_camera_concrete is None"
	/// branch — because `FakeCamera` only installs the trait object.
	#[tokio::test]
	async fn stream_source_returns_unavailable_without_concrete_bc() {
		use crate::config::test_helpers::minimal_camera_config;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use tokio_util::sync::CancellationToken;

		let fake = FakeCameraBuilder::new().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-nc2"),
			cancel,
			None,
		));
		handle.set_driver_for_test(driver);

		let err = handle
			.stream_source(RtspStreamKind::Main)
			.await
			.err()
			.expect("no concrete → Unavailable");
		assert!(
			matches!(err, StreamError::Unavailable(ref msg) if msg.contains("not currently connected")),
			"got {err:?}"
		);
	}

	/// `stop_all_stream_sources` drops every registered source even
	/// when the caller hasn't explicitly unsubscribed. The internal
	/// helper is `fn`, not `pub`, but it's exercised indirectly via
	/// the `run()` loop's teardown; here we poke it through a
	/// test-only entry point by inserting two inert sources and
	/// calling it through a dedicated `#[cfg(test)]` method.
	#[tokio::test]
	async fn stop_all_stream_sources_clears_registry() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::stream_source::StreamSource;
		use bairelay_rtsp::url::StreamKind;
		use tokio_util::sync::CancellationToken;

		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(minimal_camera_config("cam-stop"), cancel, None);
		handle
			.insert_stream_source_for_test(StreamKind::Main, StreamSource::start_inert_for_test());
		handle.insert_stream_source_for_test(StreamKind::Sub, StreamSource::start_inert_for_test());
		assert!(handle.has_stream_source_for_test(StreamKind::Main));
		assert!(handle.has_stream_source_for_test(StreamKind::Sub));

		handle.stop_all_stream_sources().await;

		assert!(!handle.has_stream_source_for_test(StreamKind::Main));
		assert!(!handle.has_stream_source_for_test(StreamKind::Sub));
	}

	/// `stream_source()` fast-path: an already-registered source is
	/// returned directly without going through the concrete-BcCamera
	/// branch. Pins the "shared source for two subscribers" contract.
	#[tokio::test]
	async fn stream_source_fast_path_returns_preregistered_source() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::stream_source::StreamSource;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use tokio_util::sync::CancellationToken;

		let fake = FakeCameraBuilder::new().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-fast"),
			cancel,
			None,
		));
		handle.set_driver_for_test(driver);

		let source = StreamSource::start_inert_for_test();
		handle.insert_stream_source_for_test(RtspStreamKind::Main, Arc::clone(&source));

		let got = handle
			.stream_source(RtspStreamKind::Main)
			.await
			.expect("preregistered source returned");
		// Same Arc → same pointer.
		assert!(Arc::ptr_eq(&got, &source));
	}

	/// `is_login_failure` rejects a plain `anyhow::Error` that doesn't
	/// match any of the three auth substrings. Guards against an
	/// over-eager match that would short-circuit retries on e.g. a
	/// TCP reset.
	#[test]
	fn is_login_failure_rejects_timeout_style_error() {
		let e: anyhow::Error =
			anyhow::anyhow!("deadline elapsed while waiting for handshake reply");
		assert!(!is_login_failure(&e));
	}

	// ── Aggregator fine-grained clearing ────────────────────────────

	#[test]
	fn aggregate_connecting_with_own_downgrade_but_still_bridging_holds() {
		// Own downgrade + still bridging ⇒ stay Connecting.
		assert_eq!(
			aggregate_preview_state(
				PreviewState::Connecting,
				true,
				Some(PreviewState::Connecting)
			),
			PreviewState::Connecting,
		);
	}

	#[test]
	fn aggregate_live_with_own_downgrade_bookkeeping_irrelevant() {
		// `last_own_write` only disambiguates Connecting; Live always
		// passes through based on the bridging flag.
		assert_eq!(
			aggregate_preview_state(PreviewState::Live, false, Some(PreviewState::Live)),
			PreviewState::Live,
		);
	}

	/// Drive `run_connected_session_for_test` against a `FakeCamera`
	/// whose keepalive always errors. Under `start_paused`, the
	/// keepalive loop fires MAX_FAILURES ticks and returns; teardown
	/// then runs synchronously. Asserts on:
	/// - state Connected → Disconnected transition
	/// - publish_connection(true) + publish_connection(false) both
	///   emitted on the MQTT mock
	/// - bc_camera cleared on exit
	/// - session-task span covered (motion, battery, floodlight,
	///   publish_pir_state) — configured via `enable_*` flags.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn run_connected_session_covers_full_lifecycle() {
		use crate::config::test_helpers::minimal_camera_config;
		use bairelay_neolink_core::bc::xml::{FloodlightStatusList, RfAlarmCfg, Support};
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

		let mut config = minimal_camera_config("cam-sess");
		config.mqtt.enable_motion = true;
		config.mqtt.enable_battery = true;
		config.mqtt.enable_floodlight = true;
		config.mqtt.enable_pir = true;
		config.mqtt.battery_update = 60_000;
		config.mqtt.floodlight_update = 60_000;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		// Scripted streams: empty but accept traffic so the two
		// listen_on_* calls succeed cleanly. The floodlight channel
		// is closed so its listener exits via `None → break`.
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (_motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(1);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let (fl_tx, fl_rx) = tokio::sync::mpsc::channel::<FloodlightStatusList>(1);
		drop(fl_tx); // close → listener returns

		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.with_floodlight_stream(fl_rx)
			.with_battery_info(|| {
				Ok(bairelay_neolink_core::bc::xml::BatteryInfo {
					battery_percent: 42,
					..Default::default()
				})
			})
			.with_is_floodlight_tasks_enabled(|| Ok(false))
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			// Argus shape: status LED (`ledCtrl` bit 0), no IR LEDs (no
			// `ledState` ability) — the reply's IR `state` is a placeholder.
			.with_ledstate(|| {
				Ok(bairelay_neolink_core::bc::xml::LedState {
					state: "close".into(),
					light_state: "close".into(),
					..Default::default()
				})
			})
			.with_support(|| {
				Ok(Support {
					ptz_mode: Some("pt".to_string()),
					items: vec![bairelay_neolink_core::bc::xml::SupportItem {
						chn_id: 0,
						led_ctrl: Some(0x4047),
						..Default::default()
					}],
					..Default::default()
				})
			})
			.with_ptz_preset(|| Ok(bairelay_neolink_core::bc::xml::PtzPreset::default()))
			.with_linktype(|| {
				// Always error → keepalive_loop terminates after
				// MAX_FAILURES consecutive failures.
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"scripted link fail",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake.clone();

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::with_bcmedia_dump_and_prefix(
			config,
			cancel,
			Some(mqtt),
			"bairelay".to_string(),
			None,
		));

		// Run the session inline under a hard real-time timeout so
		// a regression (e.g. keepalive never exits) fails in
		// bounded wall time rather than hanging `cargo test`.
		// start_paused virtual time lets the 5-s keepalive cadence
		// + 2-s drain deadline fire without real delay.
		tokio::time::timeout(
			Duration::from_secs(30),
			handle.run_connected_session_for_test(driver),
		)
		.await
		.expect("session must exit after MAX_FAILURES consecutive keepalive errors");

		// Teardown visible: state is Disconnected, bc_camera cleared.
		assert_eq!(handle.state(), CameraState::Disconnected);
		assert!(handle.bc_camera().is_none());

		// Capability cache populated (get_support returned Ok).
		let caps = handle.capabilities().expect("caps populated from fake");
		assert!(caps.has_ptz);

		// Both initial publish_connection(true) and final
		// publish_connection(false) emitted on the broker.
		let pubs = mock.published();
		let conn_on = pubs
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-sess/status" && p == b"connected");
		let conn_off = pubs
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-sess/status" && p == b"disconnected");
		assert!(
			conn_on,
			"publish_connection(true) must land; topics: {:?}",
			mock.published_topics()
		);
		assert!(conn_off, "publish_connection(false) must land at teardown");

		// publish_pir_state ran (configured enable_pir = true).
		let pir_published = pubs
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-sess/status/pir");
		assert!(
			pir_published,
			"enable_pir = true must trigger a status/pir publish"
		);

		// publish_led_state: status LED published, IR placeholder not.
		let has = |topic: &str, payload: &[u8]| {
			pubs.iter()
				.any(|(t, p, r)| t == topic && p == payload && *r)
		};
		assert!(
			has("bairelay/cam-sess/status/led", b"off"),
			"status/led=off"
		);
		assert!(
			!pubs
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam-sess/status/ir"),
			"no IR LEDs → no status/ir"
		);
	}

	/// Once `battery_poller` has latched the "no battery" verdict, a
	/// later session must NOT respawn it — else every wake re-runs the
	/// same futile probes for the life of the process. The fake here has
	/// **no** `battery_info` configured, so `FakeCamera` panics if the
	/// poller is spawned: the session completing at all is the assertion.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn battery_poller_not_respawned_once_camera_is_known_batteryless() {
		use crate::config::test_helpers::minimal_camera_config;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

		let mut config = minimal_camera_config("cam-nobat");
		config.mqtt.enable_battery = true;
		config.mqtt.battery_update = 10;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let fake = FakeCameraBuilder::new()
			// battery_info deliberately unset — a spawned poller panics.
			.with_support(|| Ok(bairelay_neolink_core::bc::xml::Support::default()))
			.with_ledstate(|| Ok(bairelay_neolink_core::bc::xml::LedState::default()))
			.with_linktype(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"scripted link fail",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake.clone();

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::with_bcmedia_dump_and_prefix(
			config,
			cancel,
			Some(mqtt),
			"bairelay".to_string(),
			None,
		));

		// An earlier session's poller already gave up on this camera.
		handle.battery_unsupported.store(true, Ordering::Release);
		assert!(handle.battery_unsupported());

		tokio::time::timeout(
			Duration::from_secs(30),
			handle.run_connected_session_for_test(driver),
		)
		.await
		.expect("session must exit; a respawned battery poller would have panicked");

		assert!(
			!mock
				.published_topics()
				.iter()
				.any(|t| t.contains("battery_level")),
			"a camera known to have no battery must never publish a level"
		);
	}

	/// `get_support` returning Err leaves `capabilities()` as None —
	/// the deliberate "retry on reconnect" path. The session still
	/// runs to completion; the get_support Err-arm is now covered.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn run_connected_session_tolerates_get_support_error() {
		use crate::config::test_helpers::minimal_camera_config;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

		let config = minimal_camera_config("cam-no-caps");
		// No MQTT so publish paths short-circuit.

		let fake = FakeCameraBuilder::new()
			.with_support(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"support probe refused",
				))
			})
			.with_linktype(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"link fail",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel, None));

		tokio::time::timeout(
			Duration::from_secs(30),
			handle.run_connected_session_for_test(driver),
		)
		.await
		.expect("session must exit");

		// Caps cache stays None → "retry on next reconnect" contract.
		assert!(
			handle.capabilities().is_none(),
			"get_support Err must leave caps cache empty",
		);
		assert_eq!(handle.state(), CameraState::Disconnected);
	}

	/// `idle_disconnect = true` spawns the grace-period task; with
	/// `wake_lock.is_idle()` also true, the grace period fires quickly
	/// under paused time and cancels the session via `sc.cancel()`
	/// before keepalive hits MAX_FAILURES.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn run_connected_session_idle_disconnect_grace_path() {
		use crate::config::test_helpers::minimal_camera_config;
		use bairelay_neolink_core::bc::xml::Support;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

		let mut config = minimal_camera_config("cam-idle");
		config.idle_disconnect = true;
		config.idle_disconnect_timeout_secs = Some(1.0);

		let fake = FakeCameraBuilder::new()
			.with_support(|| Ok(Support::default()))
			.with_linktype(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"link fail",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(config, cancel, None));

		tokio::time::timeout(
			Duration::from_secs(30),
			handle.run_connected_session_for_test(driver),
		)
		.await
		.expect("grace period path must exit");

		assert_eq!(handle.state(), CameraState::Disconnected);
	}

	#[tokio::test]
	async fn any_stream_source_bridging_reflects_registry() {
		use crate::config::test_helpers::minimal_camera_config;
		use crate::stream_source::StreamSource;

		let config = minimal_camera_config("cam");
		let cancel = CancellationToken::new();
		let handle = CameraHandle::new(config, cancel, None);

		// Empty registry → no bridging.
		assert!(!handle.any_stream_source_bridging());

		// Inert source defaults to `GapState::Live`.
		let source = StreamSource::start_inert_for_test();
		handle.insert_stream_source_for_test(RtspStreamKind::Main, Arc::clone(&source));
		assert!(!handle.any_stream_source_bridging());

		// Flip the source to `Bridging`.
		source.set_gap_state_for_test(crate::stream_source::GapState::Bridging);
		assert!(handle.any_stream_source_bridging());

		// Back to Live.
		source.set_gap_state_for_test(crate::stream_source::GapState::Live);
		assert!(!handle.any_stream_source_bridging());
	}
}

#[cfg(test)]
mod prune_grace_tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;
	use crate::stream_source::StreamSource;
	use bairelay_rtsp::url::StreamKind;
	use std::time::{Duration, Instant};

	const GRACE: Duration = Duration::from_secs(60);

	fn new_handle() -> Arc<CameraHandle> {
		let config = minimal_camera_config("prune-test-cam");
		let cancel = CancellationToken::new();
		Arc::new(CameraHandle::new(config, cancel, None))
	}

	fn insert_inert_source(handle: &CameraHandle, kind: StreamKind) -> Arc<StreamSource> {
		let source = StreamSource::start_inert_for_test();
		handle.insert_stream_source_for_test(kind, Arc::clone(&source));
		source
	}

	#[tokio::test]
	async fn prune_with_zero_grace_drops_idle_source_on_first_sweep() {
		// The `grace.is_zero()` shortcut: with no grace window
		// configured, the prune sweep drops the source the moment it
		// sees zero subscribers — no "mark + wait" cycle.
		let handle = new_handle();
		let source = insert_inert_source(&handle, StreamKind::Main);
		let rx = source.subscribe_for_test();
		drop(rx);

		handle.prune_idle_stream_sources_at(Instant::now(), Duration::ZERO);
		assert!(!handle.has_stream_source_for_test(StreamKind::Main));
	}

	#[tokio::test]
	async fn prune_retains_source_within_grace_window() {
		let handle = new_handle();
		let source = insert_inert_source(&handle, StreamKind::Main);
		let rx = source.subscribe_for_test();

		// Drop the lone receiver — broadcast subscriber count goes to zero.
		drop(rx);

		// First sweep at t0: marks `last_idle_since`, keeps the source.
		let t0 = Instant::now();
		handle.prune_idle_stream_sources_at(t0, GRACE);
		assert!(handle.has_stream_source_for_test(StreamKind::Main));

		// Halfway through the grace window: still retained.
		handle.prune_idle_stream_sources_at(t0 + Duration::from_secs(30), GRACE);
		assert!(handle.has_stream_source_for_test(StreamKind::Main));
	}

	#[tokio::test]
	async fn prune_drops_source_after_grace_expires() {
		let handle = new_handle();
		let source = insert_inert_source(&handle, StreamKind::Main);
		let rx = source.subscribe_for_test();
		drop(rx);

		let t0 = Instant::now();
		handle.prune_idle_stream_sources_at(t0, GRACE);
		// 61s past t0 — past the 60 s grace.
		handle.prune_idle_stream_sources_at(t0 + Duration::from_secs(61), GRACE);
		assert!(!handle.has_stream_source_for_test(StreamKind::Main));
	}

	#[tokio::test]
	async fn prune_resets_idle_marker_on_new_subscriber() {
		let handle = new_handle();
		let source = insert_inert_source(&handle, StreamKind::Main);
		let rx = source.subscribe_for_test();
		drop(rx);

		let t0 = Instant::now();
		handle.prune_idle_stream_sources_at(t0, GRACE);

		// A new subscriber appears well before the grace expires.
		let rx2 = handle.subscribe_stream_for_test(StreamKind::Main);

		// Next sweep observes subs > 0 — marker cleared, source retained.
		handle.prune_idle_stream_sources_at(t0 + Duration::from_secs(30), GRACE);
		assert!(handle.has_stream_source_for_test(StreamKind::Main));

		// Drop the new subscriber — grace countdown must restart from here,
		// not from `t0`.
		drop(rx2);
		let t1 = t0 + Duration::from_secs(30);
		handle.prune_idle_stream_sources_at(t1, GRACE);
		// 59 s after t1: still within the fresh grace → retained.
		handle.prune_idle_stream_sources_at(t1 + Duration::from_secs(59), GRACE);
		assert!(handle.has_stream_source_for_test(StreamKind::Main));
		// 61 s after t1: past the fresh grace → pruned.
		handle.prune_idle_stream_sources_at(t1 + Duration::from_secs(61), GRACE);
		assert!(!handle.has_stream_source_for_test(StreamKind::Main));
	}
}
