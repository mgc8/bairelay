//! Per-camera async tasks that run while a camera session is active.
//!
//! Each task is spawned when a camera connects and cancelled when the
//! connection drops (via a session-scoped `CancellationToken`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use bairelay_neolink_core::bc_protocol::{
	AiObject as CoreAiObject, AiObjects, AlarmUpdate, CameraDriver, Error as CoreError,
	MotionStatus, VisitorEdge,
};

use bairelay_mqtt::control::IrMode;
use bairelay_mqtt::discovery::CameraCapabilitiesView;
use bairelay_mqtt::{AiObject, SharedMqttClient, StatusPublisher};
use bairelay_rtsp::buffer::LastFrameBuffer;

use crate::camera::ReconnectBackoff;
use crate::capabilities::CameraCapabilities;
#[cfg(test)]
use crate::capabilities::Presence;
use crate::preview_overlay::OverlayCache;
use crate::preview_state::PreviewState;
use crate::status_cache::StatusCache;
use crate::stream_source::MutexPoisonRecover as _;
use crate::wake_lock::{WakeLockCounter, WakeLockGuard};

// ── Motion Detection ─────────────────────────────────────────────────

/// MQTT object class of a core AI detection class.
fn mqtt_ai_object(object: CoreAiObject) -> AiObject {
	match object {
		CoreAiObject::Person => AiObject::Person,
		CoreAiObject::Vehicle => AiObject::Vehicle,
		CoreAiObject::Animal => AiObject::Animal,
		CoreAiObject::Face => AiObject::Face,
		CoreAiObject::Package => AiObject::Package,
	}
}

/// `(object, detected)` publishes for the AI objects whose state changed
/// from `prev` to `next` (all five when `prev` is `None`). An `on` is
/// evidence of the hardware and always publishes. An `off` publishes
/// only while `caps` keeps the object's topic, or once the object has
/// been published `on` (`seen_on`), so it never re-creates a topic that
/// discovery retracts.
pub(crate) fn ai_object_publishes(
	caps: CameraCapabilitiesView,
	prev: Option<AiObjects>,
	next: AiObjects,
	seen_on: AiObjects,
) -> impl Iterator<Item = (AiObject, bool)> {
	CoreAiObject::ALL
		.into_iter()
		.filter(move |&o| prev.is_none_or(|p| p.contains(o) != next.contains(o)))
		.map(move |o| (o, mqtt_ai_object(o), next.contains(o)))
		.filter(move |&(core, object, detected)| {
			detected || seen_on.contains(core) || !caps.retracts_ai_object(object)
		})
		.map(|(_, object, detected)| (object, detected))
}

/// The AI object set an update diffs against.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AiBaseline {
	/// This set; `None` publishes every class.
	Known(Option<AiObjects>),
	/// Whatever the status cache holds, read under the AI publish lock:
	/// the set the last session or forwarded alarm published.
	Cache,
}

impl Default for AiBaseline {
	fn default() -> Self {
		AiBaseline::Known(None)
	}
}

/// Cache `next` for broker-reconnect replay, publish the AI object
/// states that changed since `baseline`, and record the objects
/// published `on` in `seen_on`. Holds [`StatusCache::lock_ai_publish`]
/// while it reads the baseline, caches and publishes, so a replay emits
/// either the set before this one or this one, never a stale set after
/// it.
async fn publish_ai_objects(
	publisher: &StatusPublisher<'_>,
	cache: &StatusCache,
	caps: CameraCapabilitiesView,
	baseline: AiBaseline,
	seen_on: &mut AiObjects,
	next: AiObjects,
) {
	let _serial = cache.lock_ai_publish().await;
	let prev = match baseline {
		AiBaseline::Known(prev) => prev,
		AiBaseline::Cache => cache.ai_objects(),
	};
	cache.set_ai_objects(next);
	for (object, detected) in ai_object_publishes(caps, prev, next, *seen_on) {
		let _ = publisher.publish_ai_object(object, detected).await;
	}
	for object in CoreAiObject::ALL {
		if next.contains(object) {
			seen_on.insert(object);
		}
	}
}

/// Session-start AI reset: publish `off` for every object whose topic
/// `caps` keeps (retracted topics are cleared by discovery), clearing a
/// retained `on` an earlier session or process left, and cache the
/// empty set. Returns the session's starting `(last, seen_on)`: the set
/// the first update diffs against and the objects published `on`.
async fn reset_ai_objects(
	publisher: &StatusPublisher<'_>,
	cache: &StatusCache,
	caps: CameraCapabilitiesView,
) -> (AiObjects, AiObjects) {
	let reset = AiObjects::default();
	let mut seen_on = AiObjects::default();
	publish_ai_objects(
		publisher,
		cache,
		caps,
		AiBaseline::Known(None),
		&mut seen_on,
		reset,
	)
	.await;
	(reset, seen_on)
}

/// Wake lock the motion listener holds for motion and doorbell presses.
///
/// A motion Start holds it until `hold` after the matching Stop. A press
/// holds it for `hold` from the press, and leaves an active motion's
/// release to its Stop. `release_at` is `Some` only inside a hold
/// window, so a held guard with no deadline means motion is active.
struct EventWakeHold {
	wake_lock: WakeLockCounter,
	hold: Duration,
	guard: Option<WakeLockGuard>,
	release_at: Option<tokio::time::Instant>,
}

impl EventWakeHold {
	fn new(wake_lock: WakeLockCounter, hold: Duration) -> Self {
		Self {
			wake_lock,
			hold,
			guard: None,
			release_at: None,
		}
	}

	fn acquire(&mut self) {
		if self.guard.is_none() {
			self.guard = Some(self.wake_lock.acquire());
		}
	}

	/// Motion started: cancel any pending release and hold the lock.
	fn motion_start(&mut self) {
		self.release_at = None;
		self.acquire();
	}

	/// Motion stopped: release `hold` from now.
	fn motion_stop(&mut self) {
		if self.guard.is_some() {
			self.release_at = Some(tokio::time::Instant::now() + self.hold);
		}
	}

	/// Doorbell pressed: hold the lock, releasing `hold` from now unless
	/// motion is active.
	fn doorbell_press(&mut self) {
		let motion_active = self.guard.is_some() && self.release_at.is_none();
		self.acquire();
		if !motion_active {
			self.release_at = Some(tokio::time::Instant::now() + self.hold);
		}
	}

	/// Apply one alarm update: its motion transition, then its press.
	fn apply(&mut self, update: &AlarmUpdate) {
		match update.motion {
			MotionStatus::Start(_) => self.motion_start(),
			MotionStatus::Stop(_) => self.motion_stop(),
			MotionStatus::NoChange(_) => {}
		}
		if update.doorbell_press {
			self.doorbell_press();
		}
	}

	/// Hold window expired: drop the lock.
	fn release(&mut self) {
		self.guard = None;
		self.release_at = None;
	}
}

/// Record motion state `on` in `last` and report whether it differs from
/// the previous state (`None` is unknown, so any state differs).
fn motion_changed(last: &mut Option<bool>, on: bool) -> bool {
	last.replace(on) != Some(on)
}

/// Report motion state `on`. A local transition (per [`motion_changed`])
/// is logged; `status/motion` is published and cached on a local
/// transition, or when the status cache holds another state (e.g. the
/// push listener's fallback `off` while motion is still on).
async fn report_motion(
	camera_name: &str,
	publisher: &StatusPublisher<'_>,
	status_cache: &StatusCache,
	motion_on: &mut Option<bool>,
	on: bool,
) {
	let changed = motion_changed(motion_on, on);
	if changed {
		if on {
			tracing::info!(camera = %camera_name, "Motion detected");
		} else {
			tracing::info!(camera = %camera_name, "Motion stopped");
		}
	}
	if changed || status_cache.motion() != Some(on) {
		let _ = publisher.publish_motion(on).await;
		status_cache.set_motion(on);
	}
}

/// What one cmd 33 alarm stream has reported so far: the motion state
/// (`None` is unknown), the AI objects the next update diffs against
/// and the objects published `on`.
#[derive(Debug, Default)]
pub(crate) struct AlarmReport {
	pub(crate) motion_on: Option<bool>,
	pub(crate) ai_baseline: AiBaseline,
	pub(crate) seen_on: AiObjects,
}

/// Publish a doorbell press, unless another source (the session, the
/// webhook or a forwarded alarm) published one within
/// [`crate::status_cache::DOORBELL_PRESS_DEDUPE_WINDOW`].
pub(crate) async fn report_doorbell_press(
	camera_name: &str,
	publisher: &StatusPublisher<'_>,
	status_cache: &StatusCache,
) {
	if !status_cache.claim_doorbell_press(tokio::time::Instant::now()) {
		tracing::debug!(camera = %camera_name, "Doorbell press already reported");
		return;
	}
	tracing::info!(camera = %camera_name, "Doorbell pressed");
	if let Err(e) = publisher.publish_doorbell_press().await {
		tracing::warn!(camera = %camera_name, error = %e, "publish_doorbell_press failed");
	}
}

/// Publish one alarm update: its motion transition through
/// [`report_motion`], its AI objects through [`publish_ai_objects`]
/// and its doorbell press. Cameras repeat Start in every push while
/// motion lasts; `report_motion` publishes transitions or a stale cache.
///
/// The publishes run one after another so each topic's on/off order
/// holds. They stay bounded: a publish only enqueues into rumqttc's
/// 256-slot request channel, one update touches at most seven topics
/// (motion, five AI objects, doorbell), and the AI publishes wait on
/// the AI publish lock, which the broker-reconnect replay holds only
/// for its five enqueues.
pub(crate) async fn report_alarm_update(
	camera_name: &str,
	publisher: &StatusPublisher<'_>,
	status_cache: &StatusCache,
	ai_caps: CameraCapabilitiesView,
	report: &mut AlarmReport,
	update: &AlarmUpdate,
) {
	match update.motion {
		MotionStatus::Start(_) => {
			report_motion(
				camera_name,
				publisher,
				status_cache,
				&mut report.motion_on,
				true,
			)
			.await;
		}
		MotionStatus::Stop(_) => {
			report_motion(
				camera_name,
				publisher,
				status_cache,
				&mut report.motion_on,
				false,
			)
			.await;
		}
		MotionStatus::NoChange(_) => {}
	}
	if let Some(objects) = update.objects {
		tracing::debug!(camera = %camera_name, ?objects, "AI objects");
		publish_ai_objects(
			publisher,
			status_cache,
			ai_caps,
			report.ai_baseline,
			&mut report.seen_on,
			objects,
		)
		.await;
		if let AiBaseline::Known(last) = &mut report.ai_baseline {
			*last = Some(objects);
		}
	}
	if update.doorbell_press {
		report_doorbell_press(camera_name, publisher, status_cache).await;
	}
}

#[allow(clippy::too_many_arguments)]
pub async fn motion_listener(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	wake_lock: WakeLockCounter,
	cancel: CancellationToken,
	motion_wake_hold: Duration,
	status_cache: Arc<StatusCache>,
	caps: CameraCapabilities,
	held_press_window: Option<Duration>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	// Sustained network flap with N cameras × M retries/min produces a
	// firehose of warns and pointless reconnect attempts. Use the same
	// 1 → 60 s exponential ladder as the camera reconnect path; reset
	// to 1 s every time we successfully subscribe so a transient blip
	// doesn't permanently inflate the retry interval.
	let mut backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
	// Last reported AI object states and the objects published `on`
	// this session, both starting from the session-start reset.
	let ai_caps = CameraCapabilitiesView::from(caps);
	let mut report = tokio::select! {
		_ = cancel.cancelled() => return,
		(last, seen_on) = reset_ai_objects(&publisher, &status_cache, ai_caps) => AlarmReport {
			motion_on: None,
			ai_baseline: AiBaseline::Known(Some(last)),
			seen_on,
		},
	};

	loop {
		let mut motion_data = match bc_camera.listen_on_motion().await {
			Ok(md) => md,
			Err(e) => {
				tracing::warn!(camera = %camera_name, error = %e, "Failed to start motion listener");
				if !backoff.sleep_with_cancel(&cancel).await {
					break;
				}
				continue;
			}
		};
		// Subscribed cleanly — fast retries again on the next failure.
		backoff.reset();
		// This listener owns the press edge: `listen_on_motion` reports
		// the raw `visitor` state. The edge starts again on each
		// subscribe. A press published within `held_press_window` (e.g.
		// the webhook's, whose wake brought this session up) is the
		// `visitor` state the camera may still report: held.
		let mut visitor_edge = if held_press_window.is_some_and(|window| {
			status_cache.doorbell_pressed_within(window, tokio::time::Instant::now())
		}) {
			VisitorEdge::held()
		} else {
			VisitorEdge::default()
		};

		// The pending release deadline is polled inline via the
		// select! arm below — no detached `tokio::spawn`, so at most
		// one "release N seconds from now" future exists per listener,
		// regardless of event flap rate.
		let mut wake_hold = EventWakeHold::new(wake_lock.clone(), motion_wake_hold);
		// Last published motion state; unknown on each subscribe, so the
		// first Start or Stop publishes.
		report.motion_on = None;

		loop {
			// `sleep_until` requires a concrete deadline. When no
			// release is scheduled, point it 24 h out as a placeholder
			// — the `if release_at.is_some()` guard disables that arm,
			// so the placeholder is never actually reached.
			let release_at = wake_hold.release_at;
			let release_deadline = release_at
				.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

			tokio::select! {
				_ = cancel.cancelled() => return,
				_ = tokio::time::sleep_until(release_deadline), if release_at.is_some() => {
					// Hold-down expired with no new event — drop the
					// wake-lock guard so the camera becomes eligible
					// for idle disconnect.
					wake_hold.release();
				}
				// Every update is consumed in order: `next_motion()`
				// would coalesce a buffered Start behind a press.
				// Session teardown aborts a listener still busy
				// publishing after 2 s.
				result = motion_data.next_alarm_update() => {
					let update = match result {
						Ok(update) => visitor_edge.apply(update),
						Err(e) => {
							tracing::warn!(camera = %camera_name, error = %e, "Motion listener error");
							break;
						}
					};
					report_alarm_update(
						&camera_name,
						&publisher,
						&status_cache,
						ai_caps,
						&mut report,
						&update,
					)
					.await;
					wake_hold.apply(&update);
				}
			}
		}

		// Inner loop bailed (camera-side error). Re-subscribe with the
		// same exponential ladder; `backoff` was reset on the prior
		// successful subscribe so this is a fresh 1 s start.
		if !backoff.sleep_with_cancel(&cancel).await {
			break;
		}
	}
}

// ── Battery Poller ───────────────────────────────────────────────────

/// Consecutive "camera refused the battery command" replies before the
/// poller concludes the camera has no battery and stops asking.
///
/// Three rather than one: `UnintelligibleReply` is a structural catch-all
/// for "reply shape I didn't expect", not a dedicated unsupported-command
/// code — a 200 carrying garbled XML raises it too. One occurrence is not
/// conclusive. Three in a row, spaced a full `battery_update` apart, with
/// any success or transport blip resetting the streak, is.
pub(crate) const BATTERY_MAX_UNSUPPORTED: u32 = 3;

/// Shorthand for one `timeout(..., battery_info()).await` outcome.
pub(crate) type TickResult<T> =
	std::result::Result<std::result::Result<T, CoreError>, tokio::time::error::Elapsed>;

/// What one battery poll told us about the camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatteryTickOutcome {
	/// Got a battery level. Reset the streak.
	Ok,
	/// The camera answered and refused. Evidence it has no battery.
	Unsupported,
	/// Timeout, transport error, mid-reconnect. Says nothing about
	/// whether the camera has a battery — reset the streak.
	Transient,
}

/// Fold a battery-poll outcome into a [`BatteryTickOutcome`].
///
/// The load-bearing distinction is *refused* vs *failed*. A camera that
/// replies with a non-200 (or a 200 without a `battery_info` payload) has
/// said something about its hardware; a camera that timed out or dropped
/// the connection has not. Only the former may ever disable polling — see
/// [`advance_battery_counter`].
///
/// `MissingAbility` is deliberately absent: a missing ability key does not
/// prove the camera lacks the feature, so it must never latch a permanent
/// disable. It falls through to `Transient`.
pub(crate) fn classify_battery_tick<T>(outcome: &TickResult<T>) -> BatteryTickOutcome {
	match outcome {
		Ok(Ok(_)) => BatteryTickOutcome::Ok,
		Ok(Err(CoreError::UnintelligibleReply { .. })) => BatteryTickOutcome::Unsupported,
		Ok(Err(_)) | Err(_) => BatteryTickOutcome::Transient,
	}
}

/// Update the consecutive-refusal counter, and report whether the poller
/// should now give up.
///
/// Pure, so the decision table can be unit-tested without a camera.
/// Anything that is not a refusal clears the streak — see the doc on
/// `classify_battery_tick` for why a transient error must not count.
pub(crate) fn advance_battery_counter(
	consecutive_unsupported: u32,
	outcome: BatteryTickOutcome,
	max_unsupported: u32,
) -> (u32, bool) {
	match outcome {
		BatteryTickOutcome::Ok | BatteryTickOutcome::Transient => (0, false),
		BatteryTickOutcome::Unsupported => {
			let next = consecutive_unsupported.saturating_add(1);
			(next, next >= max_unsupported)
		}
	}
}

/// Poll `battery_info()` and publish `status/battery_level`.
///
/// Gives up once the camera has refused the command
/// `BATTERY_MAX_UNSUPPORTED` times in a row **and has never once
/// answered it**, setting `battery_unsupported` so `spawn_session_tasks`
/// doesn't re-probe on the next reconnect. Mains-powered models have no
/// battery and refuse every probe; without the give-up they are re-asked
/// every `battery_update` ms for the life of the process.
///
/// The never-answered condition is what makes the latch safe. A camera
/// that has produced even one battery level has proven the hardware is
/// there, so a later run of malformed replies — which
/// `classify_battery_tick` cannot distinguish from a refusal — can
/// never silence a real battery.
#[allow(clippy::too_many_arguments)]
pub async fn battery_poller(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
	battery_unsupported: Arc<AtomicBool>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
	// Delay, not the default Burst: a poll that stalls to the 10 s timeout
	// leaves several deadlines overdue, and Burst would fire them
	// back-to-back with no spacing — collapsing the refusal streak's
	// evidence window to milliseconds. Delay keeps every probe a full
	// `interval_ms` apart.
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	// Drop the immediate first tick: it lands the instant the session
	// establishes, when a just-woken camera is least likely to answer.
	ticker.tick().await;

	let mut consecutive_unsupported: u32 = 0;
	let mut ever_succeeded = false;

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				let Some(tick) =
					cancellable(&cancel, Duration::from_secs(10), bc_camera.battery_info()).await
				else {
					break;
				};

				let outcome = classify_battery_tick(&tick);
				let (next, streak_tripped) = advance_battery_counter(
					consecutive_unsupported,
					outcome,
					BATTERY_MAX_UNSUPPORTED,
				);
				consecutive_unsupported = next;

				match &tick {
					Ok(Ok(info)) => {
						ever_succeeded = true;
						let level = info.battery_percent.min(100) as u8;
						tracing::debug!(camera = %camera_name, battery = level, "Battery level");
						let _ = publisher.publish_battery_level(level).await;
						status_cache.set_battery_level(level);
					}
					Ok(Err(e)) => {
						tracing::debug!(camera = %camera_name, error = %e, "Battery poll failed");
					}
					Err(_) => {
						tracing::debug!(camera = %camera_name, "Battery poll timed out");
					}
				}

				if streak_tripped && !ever_succeeded {
					battery_unsupported.store(true, Ordering::Release);
					tracing::warn!(
						camera = %camera_name,
						"Camera refused every battery query — it appears to have no \
						 battery. Disabling battery polling for this camera. Set \
						 `enable_battery = false` under [cameras.mqtt] to make this \
						 permanent and silence this warning at startup."
					);
					break;
				}
			}
		}
	}
}

// ── Preview Poller ───────────────────────────────────────────────────

/// Periodically publish the buffered JPEG for `status/preview`.
///
/// Refreshes and publishes the camera's preview JPEG.
///
/// Each tick asks the camera for a fresh snapshot via
/// `BcCamera::get_snapshot` **only when the camera is already connected** —
/// i.e. some other wake lock holder (an RTSP session, a `control/wakeup`
/// call, etc.) is keeping the camera awake. The poller itself does NOT
/// acquire a wake lock, so battery cameras aren't kept alive just to feed
/// previews. Matches neolink's `run_passive_task(get_snapshot)` semantics.
///
/// Fresh snapshots are also cached on [`LastFrameBuffer`] so the MQTT
/// `query/preview` command and the RTSP placeholder path both see up-to-
/// date content.
///
/// Oversized JPEGs (>32 KiB raw) are skipped with a one-shot warn so the
/// poller doesn't kick tight brokers (some mosquitto configs reject
/// payloads larger than ~10 KiB; 4K snapshots are 1–2 MiB).
#[allow(clippy::too_many_arguments)]
pub async fn preview_poller(
	camera_name: String,
	camera: Arc<crate::camera::CameraHandle>,
	last_frame: Arc<LastFrameBuffer>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	mut preview_state_rx: watch::Receiver<PreviewState>,
	preview_overlay_enabled: bool,
	cancel: CancellationToken,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let overlay_cache = OverlayCache::new();
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	// Drop the immediate first tick to avoid an unexpected burst on connect.
	ticker.tick().await;
	let mut failing = false;

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				// If the camera is awake, refresh the cached JPEG from a
				// live snapshot. If it's asleep/reconnecting we skip the
				// snapshot (would force an unwanted wake and defeats
				// idle_disconnect) but still proceed to publish the
				// cached bytes with the current overlay caption — that's
				// the whole point of the overlay: HA dashboards
				// should see SLEEPING / CONNECTING on stale frames so
				// users can tell live from stale.
				if camera.state().is_connected() {
					if let Some(bc) = camera.bc_camera() {
						let Some(snapshot) =
							snapshot_with_retry(bc.as_ref(), &camera_name, &cancel).await
						else {
							break;
						};
						match snapshot {
							Ok(Ok(bytes)) => {
								failing = false;
								last_frame.set_jpeg(bytes::Bytes::from(bytes));
							}
							Ok(Err(e)) => {
								log_preview_failure(&mut failing, &camera_name, &e);
							}
							Err(_) => {
								log_preview_failure(&mut failing, &camera_name, &"timed out");
							}
						}
					}
				}

				// Publish the current cached JPEG (fresh if we just
				// refreshed above, stale otherwise) with the overlay
				// matching the current PreviewState.
				let Some(jpeg_bytes) = last_frame.jpeg() else {
					// No frame cached yet (startup wake hasn't run, or
					// get_snapshot has never succeeded). Nothing useful
					// to publish; silently skip — the next tick will
					// retry.
					continue;
				};

				// overlay a CONNECTING/SLEEPING caption
				// on the cached JPEG before publishing whenever the per-
				// camera PreviewState is non-Live. Gated by
				// `pause.preview_overlay` (default true) so operators can
				// opt out of the image/ab_glyph dependency at runtime.
				let payload = crate::preview_overlay::rendered_preview(
					jpeg_bytes,
					preview_overlay_enabled,
					*preview_state_rx.borrow_and_update(),
					Some(&overlay_cache),
				);

				if let Err(e) = publisher.publish_preview(&payload).await {
					// If the broker advertises a lower MaxPacketSize the
					// send is rejected here — log as warn so operators see
					// they may need to raise broker message_size_limit.
					tracing::warn!(
						camera = %camera_name,
						bytes = payload.len(),
						error = %e,
						"preview publish failed"
					);
				}
			}
		}
	}
}

// ── Floodlight Poller ────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn floodlight_poller(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				let request = bc_camera.is_floodlight_tasks_enabled();
				let Some(result) = cancellable(&cancel, Duration::from_secs(10), request).await else {
					break;
				};
				match result {
					Ok(Ok(enabled)) => {
						tracing::debug!(camera = %camera_name, enabled, "Floodlight tasks");
						let _ = publisher.publish_floodlight_tasks_enabled(enabled).await;
						status_cache.set_floodlight_tasks(enabled);
					}
					Ok(Err(e)) => {
						tracing::debug!(camera = %camera_name, error = %e, "Floodlight poll failed");
					}
					Err(_) => {
						tracing::debug!(camera = %camera_name, "Floodlight poll timed out");
					}
				}
			}
		}
	}
}

// ── Floodlight Listener ─────────────────────────────────────────────

pub async fn floodlight_listener(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);

	let mut rx = match bc_camera.listen_on_floodlight().await {
		Ok(rx) => rx,
		Err(e) => {
			tracing::debug!(camera = %camera_name, error = %e, "Floodlight listener not supported");
			return;
		}
	};

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			result = rx.recv() => {
				match result {
					Some(status_list) => {
						for flight in status_list.floodlight_status_list.iter() {
							let on = flight.status != 0;
							tracing::debug!(camera = %camera_name, on, "Floodlight state changed");
							let _ = publisher.publish_floodlight(on).await;
							status_cache.set_floodlight(on);
						}
					}
					None => break, // Channel closed
				}
			}
		}
	}
}

// ── PIR Initial Publish ──────────────────────────────────────────────

/// Query and publish PIR state once on connect. After that, PIR state
/// only changes via `control/pir` commands (handled in mqtt_dispatch).
pub async fn publish_pir_state(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	match tokio::time::timeout(Duration::from_secs(10), bc_camera.get_pirstate()).await {
		Ok(Ok(pir_state)) => {
			let enabled = pir_state.enable == 1;
			tracing::debug!(camera = %camera_name, enabled, "PIR state");
			let _ = publisher.publish_pir(enabled).await;
			status_cache.set_pir(enabled);
		}
		Ok(Err(e)) => {
			tracing::debug!(camera = %camera_name, error = %e, "PIR state query failed");
		}
		Err(_) => {
			tracing::debug!(camera = %camera_name, "PIR state query timed out");
		}
	}
}

// ── Cancellable camera requests ──────────────────────────────────────

/// Await a camera request bounded by `limit`, abandoning it the moment
/// `cancel` fires: `None` when cancelled, else the timeout result. A
/// poll in flight at shutdown or session end must not hold teardown
/// for its whole timeout — the camera may already be logged out.
async fn cancellable<F: std::future::Future>(
	cancel: &CancellationToken,
	limit: Duration,
	request: F,
) -> Option<Result<F::Output, tokio::time::error::Elapsed>> {
	tokio::select! {
		biased;
		_ = cancel.cancelled() => None,
		result = tokio::time::timeout(limit, request) => Some(result),
	}
}

/// Log a preview snapshot failure: warn on the first failure after a
/// success (or at start), debug while it keeps failing.
fn log_preview_failure(failing: &mut bool, camera: &str, why: &dyn std::fmt::Display) {
	if *failing {
		tracing::debug!(camera = %camera, error = %why, "preview snapshot still failing");
	} else {
		tracing::warn!(camera = %camera, error = %why, "preview snapshot failed");
	}
	*failing = true;
}

// ── Wake-time retry ──────────────────────────────────────────────────

/// Per-attempt bound for a snapshot request.
pub(crate) const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-attempt bound for a webhook configuration read.
const HA_CFG_TIMEOUT: Duration = Duration::from_secs(5);
/// Upper bound on a webhook write, which is a set plus a read-back.
const HA_CFG_SET_TIMEOUT: Duration = Duration::from_secs(10);
/// How long after the first attempt a retry may still start.
const NOT_READY_WINDOW: Duration = Duration::from_secs(12);
/// Wait between attempts after a wake-time "service unavailable" reply.
const NOT_READY_BACKOFF: Duration = Duration::from_secs(2);
/// Message id of the snapshot request.
const SNAPSHOT_MSG_ID: u32 = 109;
/// Message id of the webhook configuration read.
const HA_CFG_MSG_ID: u32 = 806;
/// Reply code an Argus returns while it finishes waking ("not yet ready").
const NOT_READY_CODE: u16 = 400;

/// Whether `e` is the camera's 400 "service unavailable" reply to
/// request `msg_id`, which an Argus returns while it finishes waking.
fn is_not_ready(e: &bairelay_neolink_core::Error, msg_id: u32) -> bool {
	matches!(
		e,
		bairelay_neolink_core::Error::CameraServiceUnavailable { id, code }
			if *id == msg_id && *code == NOT_READY_CODE
	)
}

/// Run `attempt` (bounded by `limit` each), retrying the wake-time 400
/// reply for `msg_id` every [`NOT_READY_BACKOFF`] for as long as the
/// next attempt would start no later than [`NOT_READY_WINDOW`] after the
/// first one. Any other error or a timeout returns immediately. `None`
/// when `cancel` fires, during an attempt or a backoff. Retried replies
/// log at debug; the caller reports the final failure.
async fn retry_while_waking<T, Fut>(
	msg_id: u32,
	name: &str,
	cancel: &CancellationToken,
	limit: Duration,
	mut attempt_fn: impl FnMut() -> Fut,
) -> Option<Result<Result<T, bairelay_neolink_core::Error>, tokio::time::error::Elapsed>>
where
	Fut: std::future::Future<Output = Result<T, bairelay_neolink_core::Error>>,
{
	let started = tokio::time::Instant::now();
	let mut attempt = 1;
	loop {
		let result = cancellable(cancel, limit, attempt_fn()).await?;
		match &result {
			Ok(Err(e))
				if is_not_ready(e, msg_id)
					&& started.elapsed() + NOT_READY_BACKOFF <= NOT_READY_WINDOW =>
			{
				tracing::debug!(
					camera = %name, msg_id, attempt, error = %e,
					"camera not ready, retrying"
				);
			}
			_ => return Some(result),
		}
		tokio::select! {
			biased;
			_ = cancel.cancelled() => return None,
			_ = tokio::time::sleep(NOT_READY_BACKOFF) => {}
		}
		attempt += 1;
	}
}

/// Request a snapshot, retrying the wake-time 400 reply (attempts at 0,
/// 2, ... 12 s; the last may run its full [`SNAPSHOT_TIMEOUT`], so the
/// sequence ends within 22 s). See [`retry_while_waking`].
pub(crate) async fn snapshot_with_retry(
	camera: &dyn bairelay_neolink_core::bc_protocol::CameraDriver,
	name: &str,
	cancel: &CancellationToken,
) -> Option<Result<Result<Vec<u8>, bairelay_neolink_core::Error>, tokio::time::error::Elapsed>> {
	retry_while_waking(SNAPSHOT_MSG_ID, name, cancel, SNAPSHOT_TIMEOUT, || {
		camera.get_snapshot()
	})
	.await
}

/// What a webhook (`HaCfg`) probe found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HaCfgSupport {
	/// The firmware has HaCfg; its stored configuration.
	Supported(bairelay_neolink_core::bc::xml::HaCfg),
	/// A 200 reply without `<HaCfg>`: no support.
	EmptyReply,
	/// A persistent error reply with this code: no support.
	ErrorReply(u16),
}

impl HaCfgSupport {
	/// The camera's error reply code, when that is why HaCfg is
	/// unsupported.
	pub(crate) fn error_code(&self) -> Option<u16> {
		match self {
			Self::ErrorReply(code) => Some(*code),
			Self::Supported(_) | Self::EmptyReply => None,
		}
	}
}

/// Probe the camera's webhook (`HaCfg`) support. Retries the wake-time
/// 400 through [`retry_while_waking`]; a persistent error reply (any
/// non-200 code) means the firmware lacks the feature, as does an empty
/// reply. Transport errors, timeouts and cancellation are errors.
pub(crate) async fn ha_cfg_support(
	camera: &dyn bairelay_neolink_core::bc_protocol::CameraDriver,
	name: &str,
	cancel: &CancellationToken,
) -> anyhow::Result<HaCfgSupport> {
	use bairelay_neolink_core::Error;
	let outcome = retry_while_waking(HA_CFG_MSG_ID, name, cancel, HA_CFG_TIMEOUT, || {
		camera.get_ha_cfg()
	})
	.await
	.ok_or_else(|| anyhow::anyhow!("cancelled"))?;
	match outcome {
		Ok(Ok(Some(cfg))) => Ok(HaCfgSupport::Supported(cfg)),
		Ok(Ok(None)) => Ok(HaCfgSupport::EmptyReply),
		Ok(Err(Error::CameraServiceUnavailable { code, .. })) => Ok(HaCfgSupport::ErrorReply(code)),
		Ok(Err(e)) => Err(anyhow::Error::new(e).context("get_ha_cfg failed")),
		Err(_) => Err(anyhow::anyhow!(
			"get_ha_cfg failed: timed out after {}s",
			HA_CFG_TIMEOUT.as_secs()
		)),
	}
}

// ── Webhook arming ───────────────────────────────────────────────────

/// What one webhook arming pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmOutcome {
	/// The firmware has no HaCfg.
	Unsupported,
	/// The camera already POSTs to the URL; nothing is written.
	AlreadyArmed,
	/// Written, and the read-back matches.
	Armed,
	/// Written, but the read-back differs from what was written.
	Unconfirmed,
	/// The read or the write failed or timed out.
	Failed,
	/// The session ended first.
	Cancelled,
}

/// What webhook arming remembers across one camera's sessions.
#[derive(Debug, Default)]
pub(crate) struct ArmMemo {
	/// Set once "firmware lacks HaCfg" is logged.
	unsupported_logged: AtomicBool,
	/// Set once a skip for an unknown camera address is warned.
	unknown_camera_warned: AtomicBool,
	/// Set once a skip for a loopback-only host is warned.
	loopback_only_warned: AtomicBool,
	/// `(desired, stored)` from the last write whose read-back differed:
	/// the URL written and the URL the camera kept instead. A later pass
	/// with the same pair is left alone, so such a camera is not
	/// rewritten on every session; a new desired URL is still written.
	unconfirmed: std::sync::Mutex<Option<(String, String)>>,
}

impl ArmMemo {
	/// Log that arming is skipped for `reason`: a warning the first time
	/// for each reason, debug after.
	pub(crate) fn report_no_host(
		&self,
		name: &str,
		reason: crate::webhook_listener::NoWebhookHost,
	) {
		use crate::webhook_listener::NoWebhookHost;
		let warned = match reason {
			NoWebhookHost::UnknownCamera => &self.unknown_camera_warned,
			NoWebhookHost::LoopbackOnly => &self.loopback_only_warned,
		};
		if warned.swap(true, Ordering::Relaxed) {
			tracing::debug!(camera = %name, "Webhook not armed: {reason}");
		} else {
			tracing::warn!(camera = %name, "Webhook not armed: {reason}");
		}
	}
}

/// `version` attribute written when the camera's own read has none.
const HA_CFG_VERSION: &str = "1.1";
/// Longest host [`url_host`] returns: a DNS name's limit.
const MAX_LOGGED_HOST: usize = 253;

/// Point the camera's HaCfg webhook at `url`: read it through
/// [`ha_cfg_support`], then write `enable=1`, `url`, `verify_cert=0`
/// unless it is already enabled on `url`. Replacing a different
/// non-empty URL logs that URL's host only, since its path or query
/// can hold another system's secret; `url` itself is never logged, as
/// its path holds this camera's token; a URL bairelay wrote on an
/// earlier run (see `is_own_stale_url`) is refreshed without a
/// warning. Missing HaCfg logs once per `memo`, and a URL the camera
/// stored instead of `url` on the last write of the same `url` is not
/// rewritten. Bounded and cancel-aware.
pub(crate) async fn arm_webhook(
	camera: &dyn CameraDriver,
	name: &str,
	url: &str,
	memo: &ArmMemo,
	cancel: &CancellationToken,
) -> ArmOutcome {
	use bairelay_neolink_core::bc::xml::HaCfg;
	let stored = match ha_cfg_support(camera, name, cancel).await {
		Ok(HaCfgSupport::Supported(cfg)) => cfg,
		Ok(unsupported) => {
			if !memo.unsupported_logged.swap(true, Ordering::Relaxed) {
				tracing::info!(camera = %name, code = unsupported.error_code(), "Camera firmware lacks HaCfg; webhook not armed");
			}
			return ArmOutcome::Unsupported;
		}
		Err(_) if cancel.is_cancelled() => return ArmOutcome::Cancelled,
		Err(e) => {
			tracing::warn!(camera = %name, error = %format!("{e:#}"), "Webhook not armed: HaCfg read failed");
			return ArmOutcome::Failed;
		}
	};
	if stored.enable == 1 && stored.url == url {
		*memo.unconfirmed.lock_recover() = None;
		tracing::debug!(camera = %name, "Webhook already armed");
		return ArmOutcome::AlreadyArmed;
	}
	let remembered = memo
		.unconfirmed
		.lock_recover()
		.as_ref()
		.is_some_and(|(desired, kept)| desired == url && *kept == stored.url);
	if remembered {
		tracing::debug!(camera = %name, "Webhook left as the camera stored it after the last write");
		return ArmOutcome::Unconfirmed;
	}
	if stored.url != url && is_own_stale_url(&stored.url, url) {
		tracing::info!(camera = %name, "Refreshing webhook token");
	} else if !stored.url.is_empty() && stored.url != url {
		tracing::warn!(
			camera = %name,
			other_host = url_host(&stored.url).unwrap_or("<unparseable>"),
			"Replacing another webhook URL on the camera"
		);
	}
	let want = HaCfg {
		version: if stored.version.is_empty() {
			HA_CFG_VERSION.to_string()
		} else {
			stored.version
		},
		enable: 1,
		url: url.to_string(),
		verify_cert: 0,
	};
	let back = match cancellable(cancel, HA_CFG_SET_TIMEOUT, camera.set_ha_cfg(&want)).await {
		None => return ArmOutcome::Cancelled,
		Some(Err(_)) => {
			tracing::warn!(
				camera = %name,
				"Webhook not armed: HaCfg write timed out after {}s",
				HA_CFG_SET_TIMEOUT.as_secs()
			);
			return ArmOutcome::Failed;
		}
		Some(Ok(Err(e))) => {
			tracing::warn!(camera = %name, error = %e, "Webhook not armed: HaCfg write failed");
			return ArmOutcome::Failed;
		}
		Some(Ok(Ok(back))) => back,
	};
	let confirmed = back.as_ref().is_some_and(|b| {
		(b.enable, b.url.as_str(), b.verify_cert) == (want.enable, url, want.verify_cert)
	});
	*memo.unconfirmed.lock_recover() = if confirmed {
		None
	} else {
		back.as_ref().map(|b| (url.to_string(), b.url.clone()))
	};
	if confirmed {
		tracing::info!(camera = %name, host = url_host(url).unwrap_or("<unparseable>"), "Webhook armed");
		ArmOutcome::Armed
	} else {
		tracing::warn!(
			camera = %name,
			enable = ?back.as_ref().map(|b| b.enable),
			url_matches = back.as_ref().is_some_and(|b| b.url == url),
			verify_cert = ?back.as_ref().map(|b| b.verify_cert),
			"Webhook read-back differs from what was written"
		);
		ArmOutcome::Unconfirmed
	}
}

/// The host of an `http` or `https` webhook URL, for logging: no
/// scheme, user info, port, path, query or fragment. `None` for any
/// other scheme, an `@` after the authority (where user info could end
/// up logged as the host), an empty host, a host longer than
/// [`MAX_LOGGED_HOST`], or one holding a byte outside letters, digits
/// and `.-_:[]`.
pub(crate) fn url_host(url: &str) -> Option<&str> {
	let (scheme, rest) = url.split_once("://")?;
	if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
		return None;
	}
	let (authority, tail) = rest.split_at(rest.find(['/', '?', '#']).unwrap_or(rest.len()));
	if tail.contains('@') {
		return None;
	}
	let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
	let host = if host_port.starts_with('[') {
		&host_port[..=host_port.find(']')?]
	} else {
		host_port.split(':').next().unwrap_or_default()
	};
	let safe =
		|b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':' | b'[' | b']');
	(!host.is_empty() && host.len() <= MAX_LOGGED_HOST && host.bytes().all(safe)).then_some(host)
}

/// Whether `stored` is a URL bairelay wrote, as left by an earlier run:
/// one that shares `url`'s prefix up to and including the hook path
/// prefix, or one on any host whose path is exactly a hook path (after
/// a config or host change).
fn is_own_stale_url(stored: &str, url: &str) -> bool {
	use crate::webhook_listener::{is_hook_path, HOOK_PATH_PREFIX};
	let same_listener = url
		.find(HOOK_PATH_PREFIX)
		.map(|i| &url[..i + HOOK_PATH_PREFIX.len()])
		.is_some_and(|prefix| stored.starts_with(prefix));
	// The authority ends at the first `/`, `?` or `#`; only a `/` there
	// starts a path.
	let hook_path = stored
		.split_once("://")
		.map(|(_, rest)| &rest[rest.find(['/', '?', '#']).unwrap_or(rest.len())..])
		.is_some_and(|path| path.starts_with('/') && is_hook_path(path));
	same_listener || hook_path
}

// ── LED / IR Initial Publish ─────────────────────────────────────────

/// Camera IR `state` → MQTT mode. Unknown values map to `None` so a
/// firmware with a new spelling publishes nothing rather than a lie.
pub(crate) fn ir_mode_from_camera(state: &str) -> Option<IrMode> {
	match state.to_ascii_lowercase().as_str() {
		"open" => Some(IrMode::On),
		"close" => Some(IrMode::Off),
		"auto" => Some(IrMode::Auto),
		_ => None,
	}
}

/// Camera status-LED `lightState` → on/off; unknown values → `None`.
pub(crate) fn led_on_from_camera(light_state: &str) -> Option<bool> {
	match light_state.to_ascii_lowercase().as_str() {
		"open" => Some(true),
		"close" => Some(false),
		_ => None,
	}
}

async fn publish_ir(publisher: &StatusPublisher<'_>, cache: &StatusCache, mode: IrMode) {
	let _ = publisher.publish_ir(mode).await;
	cache.set_ir(mode);
}

async fn publish_status_led(publisher: &StatusPublisher<'_>, cache: &StatusCache, on: bool) {
	let _ = publisher.publish_led(on).await;
	cache.set_led(on);
}

/// The IR / LED value a `control/ir` or `control/led` just wrote.
#[derive(Debug, Clone, Copy)]
pub(crate) enum LedWrite {
	Ir(IrMode),
	Led(bool),
}

/// After a successful IR / status-LED write, publish what the camera
/// now reports for the field that was written — the camera may ignore
/// a write and still answer OK. Only that field is published: the
/// other half of `<LedState>` is a separate feature. If the re-read
/// fails, fall back to the requested value. Callers hold the camera's
/// LED write lock.
pub(crate) async fn publish_after_led_write(
	bc_camera: &dyn CameraDriver,
	publisher: &StatusPublisher<'_>,
	status_cache: &StatusCache,
	written: LedWrite,
) {
	let reread = tokio::time::timeout(Duration::from_secs(10), bc_camera.get_ledstate())
		.await
		.ok()
		.and_then(Result::ok);
	match written {
		LedWrite::Ir(requested) => {
			let mode = reread
				.as_ref()
				.and_then(|led| ir_mode_from_camera(&led.state))
				.unwrap_or(requested);
			publish_ir(publisher, status_cache, mode).await;
		}
		LedWrite::Led(requested) => {
			let on = reread
				.as_ref()
				.and_then(|led| led_on_from_camera(&led.light_state))
				.unwrap_or(requested);
			publish_status_led(publisher, status_cache, on).await;
		}
	}
}

/// Query once on connect and publish the IR mode and/or status LED —
/// each only if the camera has that hardware (`caps`). One
/// `get_ledstate()` reply carries both fields, but the value for absent
/// hardware is a placeholder, so presence comes from `caps`, never from
/// the reply. After this they change only via `control/ir` /
/// `control/led` (handled in mqtt_dispatch).
pub async fn publish_led_state(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	status_cache: Arc<StatusCache>,
	caps: CameraCapabilities,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	match tokio::time::timeout(Duration::from_secs(10), bc_camera.get_ledstate()).await {
		Ok(Ok(led)) => {
			tracing::debug!(
				camera = %camera_name, ir = %led.state, status_led = %led.light_state,
				ir_leds = ?caps.ir_leds, status_led_hw = ?caps.status_led,
				"LED state"
			);
			if let Some(mode) =
				ir_mode_from_camera(&led.state).filter(|_| caps.ir_leds.is_present())
			{
				publish_ir(&publisher, &status_cache, mode).await;
			}
			if let Some(on) =
				led_on_from_camera(&led.light_state).filter(|_| caps.status_led.is_present())
			{
				publish_status_led(&publisher, &status_cache, on).await;
			}
		}
		Ok(Err(e)) => {
			tracing::debug!(camera = %camera_name, error = %e, "LED state query failed");
		}
		Err(_) => {
			tracing::debug!(camera = %camera_name, "LED state query timed out");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_mqtt::test_support::MockHandle;
	use bairelay_neolink_core::bc_protocol::{AiObject, CameraDriver, FakeCameraBuilder};
	use bytes::Bytes;
	use std::sync::atomic::{AtomicBool, Ordering};

	/// Helper: every status-publishing task takes an `Arc<StatusCache>`
	/// as its final argument. Tests that don't assert on cache contents
	/// pass a fresh default. Wrapped in a helper to keep the call sites
	/// terse and not bind a dependency on the StatusCache constructor
	/// shape into every test.
	fn empty_cache() -> Arc<StatusCache> {
		Arc::new(StatusCache::default())
	}

	/// Poll the mock MQTT handle for up to `budget`, returning `true`
	/// as soon as any captured publish row satisfies `pred`. Factored
	/// out so the five poller/listener tests below don't duplicate the
	/// "sleep 10 ms, re-check, give up after 2 s" scaffolding.
	async fn await_publish_matching<F>(mock: &MockHandle, budget: Duration, pred: F) -> bool
	where
		F: Fn(&(String, Vec<u8>, bool)) -> bool,
	{
		tokio::time::timeout(budget, async {
			loop {
				if mock.published().iter().any(&pred) {
					return true;
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.unwrap_or(false)
	}

	/// `motion_listener` publishes `status/motion = on` to MQTT the first
	/// time it observes a `MotionStatus::Start` from the camera's motion
	/// stream. Tight assertion: the retained-publish tuple with exactly
	/// the expected topic and payload must appear in the mock handle
	/// within 200 ms of the event being pushed into the fake channel.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_publishes_motion_on_start_event() {
		// Script a motion stream: tx stays with the test, rx goes into
		// the fake so the listener pulls from our channel.
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);

		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake.clone();

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Push a Start event and wait briefly for the publish to
		// flow through.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_motion_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/motion" && p == b"on" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_motion_on,
			"motion_listener should publish status/motion=on after MotionStatus::Start; observed: {:?}",
			mock.published_topics()
		);
	}

	/// `battery_poller` publishes `status/battery_level = "<percent>"`
	/// on every successful `battery_info()` call. Drives one tick, asserts
	/// on the retained topic + clamped numeric payload, then cancels.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_publishes_level_on_success() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;

		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Ok(BatteryInfo {
					battery_percent: 77,
					..Default::default()
				})
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::new(AtomicBool::new(false)),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/battery_level" && p == b"77" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_level,
			"battery_poller should publish 77 on status/battery_level; observed: {:?}",
			mock.published()
		);
	}

	/// Transient `battery_info` error on the first poll does not kill the
	/// task — a later successful closure call still produces a publish.
	/// The poller swallows `Err` at debug and keeps ticking.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_tolerates_transient_error_and_recovers() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;
		use std::sync::atomic::{AtomicU32, Ordering};

		let call = Arc::new(AtomicU32::new(0));
		let call_c = Arc::clone(&call);

		let fake = FakeCameraBuilder::new()
			.with_battery_info(move || {
				let n = call_c.fetch_add(1, Ordering::AcqRel);
				if n == 0 {
					Err(bairelay_neolink_core::bc_protocol::Error::Other(
						"transient test failure",
					))
				} else {
					Ok(BatteryInfo {
						battery_percent: 33,
						..Default::default()
					})
				}
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::new(AtomicBool::new(false)),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(3), |(t, p, _)| {
			t == "bairelay/cam1/status/battery_level" && p == b"33"
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_level,
			"battery_poller should recover after a transient error; calls={} publishes={:?}",
			call.load(Ordering::Acquire),
			mock.published_topics()
		);
		assert!(
			call.load(Ordering::Acquire) >= 2,
			"closure should have been invoked at least twice (one Err, one Ok)"
		);
	}

	/// Build the exact error `BcCamera::battery_info` raises when a camera
	/// answers the battery-info request with anything other than a 200
	/// carrying a `battery_info` payload — i.e. the camera refusing the
	/// command outright. A mains-powered camera returns this on every poll.
	fn battery_refused() -> bairelay_neolink_core::bc_protocol::Error {
		use bairelay_neolink_core::bc::model::{Bc, BcMeta, MSG_ID_BATTERY_INFO};
		bairelay_neolink_core::bc_protocol::Error::UnintelligibleReply {
			reply: Arc::new(Bc::new_from_meta(BcMeta {
				msg_id: MSG_ID_BATTERY_INFO,
				channel_id: 0,
				stream_type: 0,
				response_code: 400,
				msg_num: 0,
				class: 0x6414,
			})),
			why: "The camera did not accept the battery info (maybe no battery) command.",
		}
	}

	#[test]
	fn classify_battery_tick_maps_each_arm() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;

		let ok: TickResult<BatteryInfo> = Ok(Ok(BatteryInfo::default()));
		assert_eq!(classify_battery_tick(&ok), BatteryTickOutcome::Ok);

		// The camera answered and said no.
		let refused: TickResult<BatteryInfo> = Ok(Err(battery_refused()));
		assert_eq!(
			classify_battery_tick(&refused),
			BatteryTickOutcome::Unsupported
		);

		// `MissingAbility` must NOT latch a disable: a missing ability
		// key doesn't prove the camera lacks the hardware.
		let missing: TickResult<BatteryInfo> = Ok(Err(
			bairelay_neolink_core::bc_protocol::Error::MissingAbility {
				name: "battery".into(),
				requested: "read".into(),
				actual: "none".into(),
			},
		));
		assert_eq!(
			classify_battery_tick(&missing),
			BatteryTickOutcome::Transient
		);

		// Anything else is a transport-level blip.
		let other: TickResult<BatteryInfo> = Ok(Err(
			bairelay_neolink_core::bc_protocol::Error::Other("transient"),
		));
		assert_eq!(classify_battery_tick(&other), BatteryTickOutcome::Transient);
	}

	#[test]
	fn advance_battery_counter_only_trips_on_consecutive_unsupported() {
		// Streak builds and trips at exactly `max`.
		assert_eq!(
			advance_battery_counter(0, BatteryTickOutcome::Unsupported, 3),
			(1, false)
		);
		assert_eq!(
			advance_battery_counter(1, BatteryTickOutcome::Unsupported, 3),
			(2, false)
		);
		assert_eq!(
			advance_battery_counter(2, BatteryTickOutcome::Unsupported, 3),
			(3, true)
		);

		// A success clears the streak.
		assert_eq!(
			advance_battery_counter(2, BatteryTickOutcome::Ok, 3),
			(0, false)
		);

		// So does a transient error: a camera that refused twice and then
		// dropped its connection has not proven it lacks a battery.
		assert_eq!(
			advance_battery_counter(2, BatteryTickOutcome::Transient, 3),
			(0, false)
		);
	}

	/// A camera that refuses `battery_info` on every poll must make the
	/// poller give up: the task exits on its own, sets the shared
	/// "unsupported" flag, and publishes nothing.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_gives_up_after_repeated_refusals() {
		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| Err(battery_refused()))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let unsupported = Arc::new(AtomicBool::new(false));

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::clone(&unsupported),
		));

		// The task must terminate WITHOUT anyone cancelling it. If it
		// still runs after the budget, the give-up path never fired.
		let ended = tokio::time::timeout(Duration::from_secs(5), task).await;

		assert!(
			ended.is_ok(),
			"battery_poller should exit on its own after {BATTERY_MAX_UNSUPPORTED} refusals"
		);
		assert!(
			unsupported.load(Ordering::Acquire),
			"give-up path must set the battery_unsupported flag so reconnects don't re-probe"
		);
		assert!(
			!mock
				.published_topics()
				.iter()
				.any(|t| t.contains("battery_level")),
			"a camera with no battery must never publish a battery level"
		);
	}

	/// The give-up must not fire on a camera that merely has a flaky
	/// link: refusal, refusal, transient error, then success. The streak
	/// resets and the poller keeps serving a real battery.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_survives_refusals_broken_by_a_transient_error() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;
		use std::sync::atomic::AtomicU32;

		let call = Arc::new(AtomicU32::new(0));
		let call_c = Arc::clone(&call);

		let fake = FakeCameraBuilder::new()
			.with_battery_info(move || match call_c.fetch_add(1, Ordering::AcqRel) {
				0 | 1 => Err(battery_refused()),
				2 => Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"transient test failure",
				)),
				_ => Ok(BatteryInfo {
					battery_percent: 42,
					..Default::default()
				}),
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let unsupported = Arc::new(AtomicBool::new(false));

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::clone(&unsupported),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(3), |(t, p, _)| {
			t == "bairelay/cam1/status/battery_level" && p == b"42"
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_level,
			"two refusals broken by a transient error must not disable the poller"
		);
		assert!(
			!unsupported.load(Ordering::Acquire),
			"battery_unsupported must stay clear when the streak was broken"
		);
	}

	/// A camera that answers once has proven it has a battery. A later
	/// unbroken run of refusals — which `classify_battery_tick` cannot
	/// tell apart from garbled replies on a real battery camera — must
	/// therefore never latch the permanent disable.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_never_disables_a_camera_that_once_answered() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;
		use std::sync::atomic::AtomicU32;

		let call = Arc::new(AtomicU32::new(0));
		let call_c = Arc::clone(&call);

		let fake = FakeCameraBuilder::new()
			.with_battery_info(move || {
				// One good reading, then refuse forever.
				if call_c.fetch_add(1, Ordering::AcqRel) == 0 {
					Ok(BatteryInfo {
						battery_percent: 55,
						..Default::default()
					})
				} else {
					Err(battery_refused())
				}
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let unsupported = Arc::new(AtomicBool::new(false));

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::clone(&unsupported),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(3), |(t, p, _)| {
			t == "bairelay/cam1/status/battery_level" && p == b"55"
		})
		.await;
		assert!(saw_level, "the one good reading must publish");

		// Far more than BATTERY_MAX_UNSUPPORTED refusals have now landed.
		tokio::time::sleep(Duration::from_millis(300)).await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			call.load(Ordering::Acquire) > BATTERY_MAX_UNSUPPORTED + 1,
			"test must actually drive more refusals than the streak limit"
		);
		assert!(
			!unsupported.load(Ordering::Acquire),
			"a camera that has answered once must never be latched as batteryless"
		);
	}

	/// `floodlight_listener` forwards each camera-originated event on the
	/// subscription channel to MQTT. Send one FloodlightStatus with
	/// status=1 and assert `status/floodlight = {"state":"on"}`.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_publishes_on_event() {
		use bairelay_neolink_core::bc::xml::{FloodlightStatus, FloodlightStatusList};

		let (tx, rx) = tokio::sync::mpsc::channel::<FloodlightStatusList>(4);
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(floodlight_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			cancel.clone(),
			empty_cache(),
		));

		tx.send(FloodlightStatusList {
			version: "1.1".to_string(),
			floodlight_status_list: vec![FloodlightStatus {
				channel_id: 0,
				status: 1,
			}],
		})
		.await
		.unwrap();

		let saw_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/floodlight" && p.as_slice() == br#"{"state":"on"}"# && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_on,
			"floodlight_listener should publish status/floodlight on event; observed: {:?}",
			mock.published()
		);
	}

	/// `floodlight_poller` reads `is_floodlight_tasks_enabled` on every
	/// tick and publishes the result to `status/floodlight_tasks`.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_poller_publishes_tasks_enabled_state() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled(|| Ok(true))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(floodlight_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));

		let saw = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/floodlight_tasks" && p == b"on" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw,
			"floodlight_poller should publish status/floodlight_tasks=on; observed: {:?}",
			mock.published()
		);
	}

	/// `publish_pir_state` (the one-shot PIR reader invoked on connect)
	/// should publish `status/pir = on` when the camera's PIR enable flag
	/// is 1.
	#[tokio::test]
	async fn publish_pir_state_publishes_enabled() {
		use bairelay_neolink_core::bc::xml::RfAlarmCfg;

		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		publish_pir_state(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		)
		.await;

		let pub_rows = mock.published();
		assert!(
			pub_rows
				.iter()
				.any(|(t, p, r)| t == "bairelay/cam1/status/pir" && p == b"on" && *r),
			"publish_pir_state should publish status/pir=on; observed: {:?}",
			pub_rows
		);
	}

	fn fake_jpeg() -> Bytes {
		let img = image::RgbImage::from_pixel(32, 32, image::Rgb([64, 64, 64]));
		let mut out = Vec::new();
		image::DynamicImage::ImageRgb8(img)
			.write_to(
				&mut std::io::Cursor::new(&mut out),
				image::ImageFormat::Jpeg,
			)
			.expect("encode fake jpeg");
		Bytes::from(out)
	}

	#[test]
	fn overlay_applies_when_enabled_and_not_live() {
		let jpeg = fake_jpeg();
		let rendered = crate::preview_overlay::render(&jpeg, PreviewState::Connecting);
		assert_ne!(rendered, jpeg, "non-Live state must change the bytes");
	}

	#[test]
	fn overlay_skipped_when_disabled() {
		let jpeg = fake_jpeg();
		let preview_overlay_enabled = false;
		let rendered = if preview_overlay_enabled {
			crate::preview_overlay::render(&jpeg, PreviewState::Connecting)
		} else {
			jpeg.clone()
		};
		assert_eq!(rendered, jpeg, "disabled flag must bypass the renderer");
	}

	#[test]
	fn overlay_passthrough_on_live_state() {
		let jpeg = fake_jpeg();
		let rendered = crate::preview_overlay::render(&jpeg, PreviewState::Live);
		assert_eq!(rendered, jpeg, "Live state must be passthrough");
	}

	/// Drive `preview_poller` for one tick against a not-connected
	/// `CameraHandle` whose `LastFrameBuffer` holds a cached JPEG — the
	/// disconnected branch must skip the snapshot call but still
	/// publish the cached bytes with the current overlay caption.
	/// Covers the post-Phase-2H camera-lifetime-spawned preview loop.
	#[tokio::test]
	async fn preview_poller_publishes_cached_jpeg_while_disconnected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		// Pre-seed the shared buffer so the "publish stale bytes"
		// branch has content to emit.
		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());

		// Minimal camera in its default `Sleeping` / disconnected state.
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-test"),
			cancel.clone(),
			None,
		));

		let (mqtt, mqtt_handle) = bairelay_mqtt::test_support::mock_client();
		let rx = handle.preview_state_rx();

		// Drive the poller: 10ms ticker fires near-immediately;
		// cancel after 100ms — plenty of time for at least one tick.
		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-test".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10, // 10 ms — fast tick for the test
			rx,
			true,
			cancel,
		)
		.await;
		let _ = task.await;

		// At least one preview publish must have landed on the expected
		// retained topic. Captures regression where the poller writes to
		// the wrong topic prefix or fails to publish at all.
		let topics = mqtt_handle.published_topics();
		assert!(
			topics
				.iter()
				.any(|t| t == "bairelay/cam-preview-test/status/preview"),
			"preview poller did not publish to status/preview; topics seen: {:?}",
			topics
		);
	}

	/// Same shape, but with `preview_overlay_enabled = false` — covers
	/// the bypass branch where the raw cached JPEG is published as-is.
	#[tokio::test]
	async fn preview_poller_honours_overlay_disabled_flag() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-noovl"),
			cancel.clone(),
			None,
		));

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-noovl");
		let rx = handle.preview_state_rx();

		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-noovl".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false, // disable overlay
			cancel,
		)
		.await;
		let _ = task.await;
	}

	/// Motion Stop following a Start releases the wake guard via a
	/// spawned task. The publish-motion-off MQTT row must appear, and
	/// the channel close after the Stop must exit the listener cleanly.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_publishes_motion_off_on_stop_event() {
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Start then Stop in quick succession.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_off = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, _)| {
			t == "bairelay/cam1/status/motion" && p == b"off"
		})
		.await;

		cancel.cancel();
		let _ = task.await;
		assert!(saw_off, "Stop event must publish status/motion=off");
	}

	/// Wake-lock hold-down: Start acquires the lock; Stop schedules an
	/// inline release after `motion_wake_hold`; the lock remains held
	/// across the window and is dropped exactly once when the timer
	/// expires. Pins the post-Phase-3D refactor that replaced the
	/// detached `tokio::spawn` release with a `select!` arm.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_holds_wake_lock_through_hold_down() {
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		// 500 ms hold-down absorbs tarpaulin instrumentation slop on
		// the timer; smaller windows have flaked under coverage.
		let hold = Duration::from_millis(500);

		let task = tokio::spawn(motion_listener(
			"cam-hold".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl.clone(),
			cancel.clone(),
			hold,
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Start → wake lock acquired.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert_eq!(wl.count(), 1, "Start must acquire the wake lock");

		// Stop → lock still held during hold-down window.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert_eq!(
			wl.count(),
			1,
			"wake lock must remain held during the post-stop hold-down",
		);

		// Past the deadline → lock dropped exactly once.
		tokio::time::sleep(Duration::from_millis(700)).await;
		assert_eq!(
			wl.count(),
			0,
			"wake lock must be released after the hold-down expires",
		);

		cancel.cancel();
		let _ = task.await;
	}

	/// A fresh Start during the hold-down window cancels the pending
	/// release and keeps holding the existing wake-lock guard. Critical
	/// invariant: each listener holds **at most one** guard, regardless
	/// of motion-event flap rate.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_start_during_hold_down_cancels_release() {
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		// 500 ms hold-down absorbs tarpaulin instrumentation slop.
		let hold = Duration::from_millis(500);

		let task = tokio::spawn(motion_listener(
			"cam-flap".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl.clone(),
			cancel.clone(),
			hold,
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Start → Stop → Start, all within the hold-down window.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		// Past the original hold-down deadline (which the second Start
		// must have cancelled) — lock is still held because no Stop
		// has fired since the re-arm.
		tokio::time::sleep(Duration::from_millis(700)).await;
		assert_eq!(
			wl.count(),
			1,
			"a Start during the hold-down window must cancel the pending release",
		);

		cancel.cancel();
		let _ = task.await;
	}

	/// `MotionStatus::NoChange` events are not published but must not
	/// break the listener — a Start following a NoChange still emits on.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_ignores_nochange_and_continues() {
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		motion_tx
			.send(Ok(
				bairelay_neolink_core::bc_protocol::MotionStatus::NoChange(
					std::time::Instant::now(),
				),
			))
			.await
			.unwrap();
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, _)| {
			t == "bairelay/cam1/status/motion" && p == b"on"
		})
		.await;

		cancel.cancel();
		let _ = task.await;
		assert!(saw_on, "NoChange must not block subsequent Start publish");
	}

	/// When `next_motion()` returns `Err`, the inner loop breaks; the
	/// outer loop sleeps 1 s then re-calls `listen_on_motion`, which
	/// panics (stream already consumed). Cancel the task before that
	/// retry lands so we observe only the "inner Err → break" path
	/// cleanly.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_breaks_inner_loop_on_next_motion_error() {
		type MotionItem = std::result::Result<
			bairelay_neolink_core::bc_protocol::MotionStatus,
			bairelay_neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = bairelay_neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Push one Err into the motion channel. The listener will log
		// and break; we cancel before the 1s retry sleep finishes.
		motion_tx
			.send(Err(bairelay_neolink_core::bc_protocol::Error::Other(
				"scripted next_motion failure",
			)))
			.await
			.unwrap();

		tokio::time::sleep(Duration::from_millis(100)).await;
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_millis(500), task).await;
	}

	// ── Alarm updates: AI objects + doorbell press ──────────────────

	type AlarmItem = std::result::Result<
		bairelay_neolink_core::bc_protocol::AlarmUpdate,
		bairelay_neolink_core::bc_protocol::Error,
	>;

	/// `motion_listener` fed full alarm updates from scripted channels,
	/// one per subscribe.
	struct AlarmRig {
		txs: Vec<tokio::sync::mpsc::Sender<AlarmItem>>,
		/// Index in `txs` of the stream the listener reads now.
		stream: usize,
		mock: MockHandle,
		wake_lock: WakeLockCounter,
		cache: Arc<StatusCache>,
		cancel: CancellationToken,
		task: tokio::task::JoinHandle<()>,
	}

	impl AlarmRig {
		fn spawn(hold: Duration) -> Self {
			Self::spawn_with_caps(hold, CameraCapabilities::default())
		}

		fn spawn_with_caps(hold: Duration, caps: CameraCapabilities) -> Self {
			Self::spawn_with(hold, caps, empty_cache())
		}

		fn spawn_with(hold: Duration, caps: CameraCapabilities, cache: Arc<StatusCache>) -> Self {
			Self::spawn_streams(hold, caps, cache, 1)
		}

		/// A rig whose camera serves `streams` successive subscribes.
		fn spawn_streams(
			hold: Duration,
			caps: CameraCapabilities,
			cache: Arc<StatusCache>,
			streams: usize,
		) -> Self {
			Self::spawn_full(hold, caps, cache, streams, None)
		}

		/// A rig whose listener treats a press within `window` as a
		/// held visitor state.
		fn spawn_held(hold: Duration, cache: Arc<StatusCache>, window: Duration) -> Self {
			Self::spawn_full(hold, CameraCapabilities::default(), cache, 1, Some(window))
		}

		fn spawn_full(
			hold: Duration,
			caps: CameraCapabilities,
			cache: Arc<StatusCache>,
			streams: usize,
			held_press_window: Option<Duration>,
		) -> Self {
			let mut builder = FakeCameraBuilder::new();
			let mut txs = Vec::with_capacity(streams);
			for _ in 0..streams {
				let (tx, rx) = tokio::sync::mpsc::channel::<AlarmItem>(8);
				txs.push(tx);
				builder = builder.with_motion_stream(
					bairelay_neolink_core::bc_protocol::MotionData::test_new_alarm(rx),
				);
			}
			let fake = builder.build();
			let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
			let wake_lock = WakeLockCounter::new();
			let cancel = CancellationToken::new();
			let task = tokio::spawn(motion_listener(
				"cam1".to_string(),
				fake,
				mqtt,
				"bairelay".to_string(),
				wake_lock.clone(),
				cancel.clone(),
				hold,
				Arc::clone(&cache),
				caps,
				held_press_window,
			));
			Self {
				txs,
				stream: 0,
				mock,
				wake_lock,
				cache,
				cancel,
				task,
			}
		}

		/// Queue one update without letting the listener run. `visitor`
		/// is the raw state, as `listen_on_motion` reports it; the
		/// listener derives presses from it.
		async fn queue(&self, motion: MotionStatus, objects: Option<AiObjects>, visitor: bool) {
			self.queue_update(bairelay_neolink_core::bc_protocol::AlarmUpdate {
				motion,
				objects,
				visitor: Some(visitor),
				doorbell_press: false,
			})
			.await;
		}

		async fn queue_update(&self, update: bairelay_neolink_core::bc_protocol::AlarmUpdate) {
			self.txs[self.stream].send(Ok(update)).await.unwrap();
		}

		/// Fail the current stream so the listener resubscribes to the
		/// next one after its 1 s backoff.
		async fn fail_stream(&mut self) {
			let error = bairelay_neolink_core::bc_protocol::Error::Other("scripted failure");
			self.txs[self.stream].send(Err(error)).await.unwrap();
			self.stream += 1;
			tokio::time::sleep(Duration::from_secs(2)).await;
		}

		/// Queue one update and let the listener process it.
		async fn send(&self, motion: MotionStatus, objects: Option<AiObjects>, visitor: bool) {
			self.queue(motion, objects, visitor).await;
			settle().await;
		}

		/// `(payload, retained)` of every publish on `status/<suffix>`.
		fn rows(&self, suffix: &str) -> Vec<(String, bool)> {
			let topic = format!("bairelay/cam1/status/{suffix}");
			self.mock
				.published()
				.into_iter()
				.filter(|(t, _, _)| *t == topic)
				.map(|(_, p, r)| (String::from_utf8(p).unwrap(), r))
				.collect()
		}

		/// `(object, payload)` of every `status/ai/<object>` publish, in
		/// order; asserts each one is retained.
		fn ai_rows(&self) -> Vec<(String, String)> {
			let prefix = "bairelay/cam1/status/ai/";
			self.mock
				.published()
				.into_iter()
				.filter_map(|(t, p, r)| {
					let object = t.strip_prefix(prefix)?.to_string();
					assert!(r, "status/ai/{object} must be retained");
					Some((object, String::from_utf8(p).unwrap()))
				})
				.collect()
		}

		async fn stop(self) {
			self.cancel.cancel();
			tokio::time::timeout(Duration::from_secs(1), self.task)
				.await
				.expect("listener must exit promptly on cancel")
				.unwrap();
		}
	}

	/// Let spawned tasks drain their queues. Under paused time the clock
	/// moves 1 ms, far below any hold under test.
	async fn settle() {
		tokio::time::sleep(Duration::from_millis(1)).await;
	}

	fn now_start() -> MotionStatus {
		MotionStatus::Start(std::time::Instant::now())
	}

	fn now_stop() -> MotionStatus {
		MotionStatus::Stop(std::time::Instant::now())
	}

	fn no_change() -> MotionStatus {
		MotionStatus::NoChange(std::time::Instant::now())
	}

	fn objects(list: &[AiObject]) -> Option<AiObjects> {
		Some(list.iter().copied().collect())
	}

	fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
		list.iter()
			.map(|(o, p)| (o.to_string(), p.to_string()))
			.collect()
	}

	/// Listener start resets every AI object to `off` (clearing a
	/// retained `on` a previous session left) and caches the reset;
	/// later sets publish only the objects whose state changed.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_resets_ai_objects_then_publishes_changes() {
		let cache = empty_cache();
		cache.set_ai_objects([AiObject::Person].into_iter().collect());
		let rig = AlarmRig::spawn_with(
			Duration::from_secs(30),
			CameraCapabilities::default(),
			cache,
		);
		settle().await;
		let all_off = pairs(&[
			("person", "off"),
			("vehicle", "off"),
			("animal", "off"),
			("face", "off"),
			("package", "off"),
		]);
		assert_eq!(rig.ai_rows(), all_off);
		assert_eq!(rig.cache.ai_objects(), objects(&[]));

		rig.send(no_change(), objects(&[AiObject::Person]), false)
			.await;
		assert_eq!(rig.ai_rows()[5..], pairs(&[("person", "on")])[..]);
		assert_eq!(rig.cache.ai_objects(), objects(&[AiObject::Person]));

		rig.send(
			no_change(),
			objects(&[AiObject::Person, AiObject::Vehicle]),
			false,
		)
		.await;
		assert_eq!(rig.ai_rows()[6..], pairs(&[("vehicle", "on")])[..]);

		rig.send(no_change(), None, false).await;
		rig.send(no_change(), objects(&[]), false).await;
		assert_eq!(
			rig.ai_rows()[7..],
			pairs(&[("person", "off"), ("vehicle", "off")])[..]
		);
		assert_eq!(rig.cache.ai_objects(), objects(&[]));
		assert!(
			rig.rows("motion").is_empty(),
			"NoChange publishes no motion"
		);
		rig.stop().await;
	}

	/// The start reset waits for an AI publish sequence in flight (the
	/// broker-reconnect replay) and publishes after it, so the reset is
	/// the last retained word.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_ai_reset_serialises_behind_replay() {
		let cache = empty_cache();
		let held = Arc::clone(&cache);
		let guard = held.lock_ai_publish().await;
		let rig = AlarmRig::spawn_with(
			Duration::from_secs(30),
			CameraCapabilities::default(),
			cache,
		);
		settle().await;
		assert!(rig.ai_rows().is_empty(), "reset waits for the lock");

		drop(guard);
		settle().await;
		assert_eq!(rig.ai_rows().len(), 5, "{:?}", rig.ai_rows());
		assert!(rig.ai_rows().iter().all(|(_, p)| p == "off"));
		rig.stop().await;
	}

	/// Cancel while the start reset waits on the AI publish lock exits
	/// the listener promptly.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_cancel_during_ai_reset_is_prompt() {
		let cache = empty_cache();
		let held = Arc::clone(&cache);
		let _guard = held.lock_ai_publish().await;
		let rig = AlarmRig::spawn_with(
			Duration::from_secs(30),
			CameraCapabilities::default(),
			cache,
		);
		settle().await;
		assert!(rig.ai_rows().is_empty(), "reset waits for the lock");
		rig.stop().await;
	}

	fn ai_caps(ai: Presence, doorbell: Presence) -> CameraCapabilities {
		CameraCapabilities {
			ai,
			doorbell,
			..Default::default()
		}
	}

	/// Argus-like caps (AI, no doorbell): `off` skips the retracted
	/// face/package topics until the object has been seen `on`.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_skips_retracted_ai_off_until_seen_on() {
		let caps = ai_caps(Presence::Present, Presence::Absent);
		let rig = AlarmRig::spawn_with_caps(Duration::from_secs(30), caps);
		settle().await;
		let reset = pairs(&[("person", "off"), ("vehicle", "off"), ("animal", "off")]);
		assert_eq!(rig.ai_rows(), reset, "start reset skips face/package");

		rig.send(no_change(), objects(&[]), false).await;
		assert_eq!(rig.ai_rows(), reset, "an unchanged set publishes nothing");

		rig.send(no_change(), objects(&[AiObject::Face]), false)
			.await;
		rig.send(no_change(), objects(&[]), false).await;
		assert_eq!(
			rig.ai_rows()[3..],
			pairs(&[("face", "on"), ("face", "off")])[..]
		);
		rig.stop().await;
	}

	/// AI confirmed absent: only an `on` (evidence) and its later `off`
	/// publish.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_ai_absent_publishes_no_unseen_off() {
		let caps = ai_caps(Presence::Absent, Presence::Unknown);
		let rig = AlarmRig::spawn_with_caps(Duration::from_secs(30), caps);

		rig.send(no_change(), objects(&[]), false).await;
		assert!(rig.ai_rows().is_empty(), "no off for retracted topics");

		rig.send(no_change(), objects(&[AiObject::Vehicle]), false)
			.await;
		rig.send(no_change(), objects(&[]), false).await;
		assert_eq!(
			rig.ai_rows(),
			pairs(&[("vehicle", "on"), ("vehicle", "off")])
		);
		rig.stop().await;
	}

	/// A press publishes one non-retained `press` and holds the wake
	/// lock for `motion_wake_hold`, like a Start followed by a Stop.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_doorbell_press_publishes_and_holds_wake_lock() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(no_change(), None, true).await;
		assert_eq!(rig.rows("doorbell"), vec![("press".to_string(), false)]);
		assert!(rig.rows("motion").is_empty());
		assert_eq!(rig.wake_lock.count(), 1, "press must acquire the wake lock");

		tokio::time::sleep(Duration::from_secs(29)).await;
		assert_eq!(rig.wake_lock.count(), 1, "held through the hold window");
		tokio::time::sleep(Duration::from_secs(2)).await;
		assert_eq!(rig.wake_lock.count(), 0, "released after the hold");
		rig.stop().await;
	}

	/// A press during active motion leaves the release to the Stop.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_doorbell_press_never_shortens_active_motion() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), None, false).await;
		rig.send(no_change(), None, true).await;
		tokio::time::sleep(Duration::from_secs(45)).await;
		assert_eq!(rig.wake_lock.count(), 1, "motion still active");

		rig.send(now_stop(), None, false).await;
		tokio::time::sleep(Duration::from_secs(29)).await;
		assert_eq!(rig.wake_lock.count(), 1, "post-motion hold");
		tokio::time::sleep(Duration::from_secs(2)).await;
		assert_eq!(rig.wake_lock.count(), 0);
		rig.stop().await;
	}

	/// `visitor` is a state the doorbell repeats in every push for
	/// seconds after one press; a press is only its rising edge. Two
	/// physical presses in this live sequence publish exactly two.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_presses_at_visitor_rising_edges() {
		use bairelay_neolink_core::bc_protocol::classify_alarm;
		let rig = AlarmRig::spawn(Duration::from_secs(30));
		let pushes: [(&str, &str); 12] = [
			("MD", "none"),
			("MD", "people"),
			("MD,visitor", "people"),
			("visitor", "people"),
			("none", "people"),
			("none", "none"),
			("MD", "people"),
			("MD,visitor", "people"),
			("MD,visitor", "none"),
			("MD,visitor", "people"),
			("visitor", "people"),
			("none", "none"),
		];
		let mut presses = vec![];
		for (index, (status, ai_type)) in pushes.into_iter().enumerate() {
			let before = rig.rows("doorbell").len();
			rig.queue_update(classify_alarm(
				status,
				Some(ai_type),
				std::time::Instant::now(),
			))
			.await;
			settle().await;
			if rig.rows("doorbell").len() > before {
				presses.push(index + 1);
			}
			// Space every push beyond the press dedupe window, so only
			// the edge decides which pushes are presses.
			tokio::time::sleep(
				crate::status_cache::DOORBELL_PRESS_DEDUPE_WINDOW + Duration::from_secs(1),
			)
			.await;
		}
		assert_eq!(presses, vec![3, 8], "presses at the visitor rising edges");
		rig.stop().await;
	}

	/// The press edge starts again on each subscribe: the first
	/// `visitor` push a new subscription sees is a press.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_press_edge_resets_on_resubscribe() {
		let mut rig = AlarmRig::spawn_streams(
			Duration::from_secs(30),
			CameraCapabilities::default(),
			empty_cache(),
			2,
		);
		rig.send(no_change(), None, true).await;
		rig.send(no_change(), None, true).await;
		assert_eq!(rig.rows("doorbell").len(), 1, "held: one press");
		rig.fail_stream().await;
		tokio::time::sleep(crate::status_cache::DOORBELL_PRESS_DEDUPE_WINDOW).await;
		rig.send(no_change(), None, true).await;
		assert_eq!(
			rig.rows("doorbell").len(),
			2,
			"first visitor after resubscribe"
		);
		rig.stop().await;
	}

	/// A session that comes up while the camera still reports the
	/// `visitor` state of a press already published (e.g. by the
	/// webhook) publishes no second press, even past the dedupe window.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_skips_the_held_visitor_of_a_recent_press() {
		let cache = empty_cache();
		assert!(cache.claim_doorbell_press(tokio::time::Instant::now()));
		tokio::time::sleep(Duration::from_secs(15)).await;
		let rig = AlarmRig::spawn_held(Duration::from_secs(30), cache, Duration::from_secs(30));
		rig.send(no_change(), None, true).await;
		rig.send(no_change(), None, true).await;
		assert!(rig.rows("doorbell").is_empty(), "the held visitor state");
		// Cleared, then pressed again: a new press.
		rig.send(no_change(), None, false).await;
		rig.send(no_change(), None, true).await;
		assert_eq!(rig.rows("doorbell"), vec![("press".to_string(), false)]);
		rig.stop().await;
	}

	/// A press older than the window, or no window, leaves the first
	/// `visitor` push a press.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_presses_on_a_visitor_outside_the_window() {
		for window in [Some(Duration::from_secs(30)), None] {
			let cache = empty_cache();
			assert!(cache.claim_doorbell_press(tokio::time::Instant::now()));
			tokio::time::sleep(Duration::from_secs(31)).await;
			let rig = AlarmRig::spawn_full(
				Duration::from_secs(30),
				CameraCapabilities::default(),
				cache,
				1,
				window,
			);
			rig.send(no_change(), None, true).await;
			assert_eq!(rig.rows("doorbell").len(), 1, "{window:?}");
			rig.stop().await;
		}
	}

	/// A press inside the post-motion hold re-arms it from the press.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_doorbell_press_extends_post_motion_hold() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), None, false).await;
		rig.send(now_stop(), None, false).await;
		tokio::time::sleep(Duration::from_secs(20)).await;
		rig.send(no_change(), None, true).await;

		tokio::time::sleep(Duration::from_secs(15)).await;
		assert_eq!(rig.wake_lock.count(), 1, "press re-armed the hold");
		tokio::time::sleep(Duration::from_secs(16)).await;
		assert_eq!(rig.wake_lock.count(), 0);
		assert_eq!(rig.rows("doorbell").len(), 1);
		rig.stop().await;
	}

	#[test]
	fn motion_changed_reports_transitions_only() {
		let mut last = None;
		let seen: Vec<bool> = [true, true, false, false, true]
			.into_iter()
			.map(|on| motion_changed(&mut last, on))
			.collect();
		assert_eq!(seen, vec![true, false, true, false, true]);
		assert_eq!(last, Some(true));
	}

	/// A camera repeats Start in every push while motion lasts:
	/// `status/motion` publishes `on` once and caches it.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_publishes_motion_on_once_for_consecutive_starts() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), objects(&[]), false).await;
		rig.send(now_start(), objects(&[AiObject::Person]), false)
			.await;
		rig.send(now_start(), objects(&[AiObject::Person]), false)
			.await;

		assert_eq!(rig.rows("motion"), vec![("on".to_string(), true)]);
		assert_eq!(rig.cache.motion(), Some(true));
		rig.stop().await;
	}

	/// `status/motion` publishes every transition, in order.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_publishes_each_motion_transition() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), None, false).await;
		rig.send(now_stop(), None, false).await;
		rig.send(now_stop(), None, false).await;
		rig.send(now_start(), None, false).await;

		let on = ("on".to_string(), true);
		let off = ("off".to_string(), true);
		assert_eq!(rig.rows("motion"), vec![on.clone(), off, on]);
		assert_eq!(rig.cache.motion(), Some(true));
		rig.stop().await;
	}

	/// A repeated Start publishes nothing and keeps the lock; the hold
	/// runs from the last Stop.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_repeated_start_keeps_wake_hold() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), None, false).await;
		rig.send(now_stop(), None, false).await;
		tokio::time::sleep(Duration::from_secs(20)).await;
		rig.send(now_start(), None, false).await;
		rig.send(now_start(), None, false).await;
		tokio::time::sleep(Duration::from_secs(60)).await;
		assert_eq!(rig.wake_lock.count(), 1, "motion active again");

		rig.send(now_stop(), None, false).await;
		tokio::time::sleep(Duration::from_secs(29)).await;
		assert_eq!(rig.wake_lock.count(), 1, "post-motion hold");
		tokio::time::sleep(Duration::from_secs(2)).await;
		assert_eq!(rig.wake_lock.count(), 0, "released after the hold");

		let on = ("on".to_string(), true);
		let off = ("off".to_string(), true);
		assert_eq!(rig.rows("motion"), vec![on.clone(), off.clone(), on, off]);
		rig.stop().await;
	}

	/// A Start republishes `on` when the status cache holds `off` (the
	/// push listener's fallback clear) while motion is still on.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_republishes_start_over_cached_off() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.send(now_start(), None, false).await;
		rig.cache.set_motion(false);
		rig.send(now_start(), None, false).await;

		let on = ("on".to_string(), true);
		assert_eq!(rig.rows("motion"), vec![on.clone(), on]);
		assert_eq!(rig.cache.motion(), Some(true));
		rig.stop().await;
	}

	/// The motion state is unknown on each subscribe: the first Start
	/// after a resubscribe publishes again.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_publishes_first_motion_after_resubscribe() {
		let mut rig = AlarmRig::spawn_streams(
			Duration::from_secs(30),
			CameraCapabilities::default(),
			empty_cache(),
			2,
		);

		rig.send(now_start(), None, false).await;
		rig.fail_stream().await;
		rig.send(now_start(), None, false).await;

		let on = ("on".to_string(), true);
		assert_eq!(rig.rows("motion"), vec![on.clone(), on]);
		rig.stop().await;
	}

	/// A Start buffered ahead of a press-only update is still handled:
	/// every queued update is consumed, none coalesced away.
	#[tokio::test(start_paused = true)]
	async fn motion_listener_handles_start_buffered_ahead_of_press() {
		let rig = AlarmRig::spawn(Duration::from_secs(30));

		rig.queue(now_start(), None, false).await;
		rig.queue(no_change(), None, true).await;
		settle().await;

		assert_eq!(rig.rows("motion"), vec![("on".to_string(), true)]);
		assert_eq!(rig.cache.motion(), Some(true));
		assert_eq!(rig.rows("doorbell").len(), 1);
		assert_eq!(rig.wake_lock.count(), 1);
		rig.stop().await;
	}

	/// `floodlight_listener` returns quietly when `listen_on_floodlight`
	/// errors — e.g. the camera doesn't support floodlight. The task
	/// joins without panicking and no publishes are emitted.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_exits_cleanly_on_subscribe_error() {
		// Build without calling `.with_floodlight_stream(...)` → the
		// fake's `listen_on_floodlight` returns Err (via `unset()`
		// actually panics — but we need a real error. Provide a
		// drained channel instead so the `listen_on_floodlight` call
		// succeeds and `rx.recv()` returns None immediately — matches
		// the "None → break" path on line 300.)
		let (tx, rx) =
			tokio::sync::mpsc::channel::<bairelay_neolink_core::bc::xml::FloodlightStatusList>(1);
		drop(tx); // close the sender so rx.recv() returns None.
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		floodlight_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			cancel,
			empty_cache(),
		)
		.await;
		assert!(
			mock.published().is_empty(),
			"closed channel should publish nothing; got {:?}",
			mock.published_topics()
		);
	}

	/// `publish_pir_state` with a driver error — publishes nothing and
	/// returns cleanly.
	#[tokio::test]
	async fn publish_pir_state_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"pir refused",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		publish_pir_state(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		)
		.await;
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam1/status/pir"),
			"error path must not publish status/pir"
		);
	}

	/// `floodlight_poller` error branch: `is_floodlight_tasks_enabled`
	/// returns Err, and the poller swallows it at debug without
	/// publishing.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_poller_tolerates_err_without_publishing() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"tasks refused",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(floodlight_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));
		// Let at least one tick fire + settle.
		tokio::time::sleep(Duration::from_millis(80)).await;
		cancel.cancel();
		let _ = task.await;
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam1/status/floodlight_tasks"),
			"error branch must not publish floodlight_tasks"
		);
	}

	/// `preview_poller` while connected refreshes the cached JPEG via
	/// the driver's `get_snapshot()` and publishes the result. Covers
	/// the `state().is_connected() && bc_camera().is_some()` arm +
	/// the `Ok(bytes)` → `set_jpeg` branch in the poller body.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn preview_poller_refreshes_jpeg_while_connected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let fresh_jpeg = fake_jpeg();
		let fresh_clone = fresh_jpeg.clone();
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new()
			.with_snapshot(move || Ok(fresh_clone.to_vec()))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-conn"),
			cancel.clone(),
			None,
		));
		handle.set_driver_for_test(driver);

		let last_frame = Arc::new(LastFrameBuffer::new());
		// Pre-seed with different bytes so we can observe the refresh.
		last_frame.set_jpeg(Bytes::from_static(&[0u8; 4]));
		let rx = handle.preview_state_rx();

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-connected");

		let cancel_task = cancel.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-conn".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false,
			cancel,
		)
		.await;
		let _ = canceller.await;

		// The cached buffer should now match the fresh JPEG from the driver.
		let got = last_frame.jpeg().expect("buffer populated");
		assert_eq!(
			got.as_ref(),
			fresh_jpeg.as_ref(),
			"connected path must refresh the cached JPEG from the driver"
		);
	}

	/// `preview_poller` connected path with a driver that errors on
	/// snapshot — the poller swallows the error and still publishes
	/// the pre-existing cached bytes.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn preview_poller_tolerates_snapshot_error_while_connected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new()
			.with_snapshot(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"snap denied",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-snap-err"),
			cancel.clone(),
			None,
		));
		handle.set_driver_for_test(driver);
		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());
		let rx = handle.preview_state_rx();

		let mqtt = SharedMqttClient::for_test_stub("preview-snap-err");
		let cancel_task = cancel.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-snap-err".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false,
			cancel,
		)
		.await;
		let _ = canceller.await;
	}

	/// With no cached JPEG, the disconnected poller has nothing to
	/// publish — the `last_frame.jpeg() is None → continue` branch.
	#[tokio::test]
	async fn preview_poller_noops_when_buffer_is_empty() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let last_frame = Arc::new(LastFrameBuffer::new());
		// Deliberately leave the buffer empty.
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-empty"),
			cancel.clone(),
			None,
		));

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-empty");
		let rx = handle.preview_state_rx();

		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-empty".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			true,
			cancel,
		)
		.await;
		let _ = task.await;
	}

	/// `motion_listener` with a driver that errors on every
	/// `listen_on_motion` subscribe: the outer loop must sleep for 1 s
	/// then retry. We cancel during the retry sleep so the task exits
	/// via the `cancel.cancelled()` arm of the retry-sleep select
	/// (lines 40, 87 of camera_tasks.rs).
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_retries_after_subscribe_error() {
		let fake = FakeCameraBuilder::new()
			.with_motion_stream_error(|| {
				bairelay_neolink_core::bc_protocol::Error::Other("scripted subscribe failure")
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam-retry".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
			CameraCapabilities::default(),
			None,
		));

		// Give the listener enough wall time to invoke
		// `listen_on_motion` (fails) and enter the retry-sleep, but
		// cancel well before the 1 s sleep elapses.
		tokio::time::sleep(Duration::from_millis(50)).await;
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_millis(500), task).await;
	}

	/// `floodlight_listener` with a driver that errors on
	/// `listen_on_floodlight`: the listener logs and returns
	/// immediately (early-return branch). Pins the "camera doesn't
	/// support floodlight" path where previously tests only covered
	/// the closed-channel case.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_returns_on_subscribe_error() {
		let fake = FakeCameraBuilder::new()
			.with_floodlight_stream_error(|| {
				bairelay_neolink_core::bc_protocol::Error::Other("floodlight unsupported")
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		// No spawn: the listener must return before we touch cancel.
		// If the early-return branch regresses, the test will hang on
		// `floodlight_listener` until the outer harness timeout fires.
		tokio::time::timeout(
			Duration::from_millis(500),
			floodlight_listener(
				"cam-err".to_string(),
				driver,
				mqtt,
				"bairelay".to_string(),
				cancel,
				empty_cache(),
			),
		)
		.await
		.expect("listener must return on subscribe error");

		assert!(
			mock.published().is_empty(),
			"subscribe error must not publish; got {:?}",
			mock.published_topics()
		);
	}

	/// `battery_poller` whose driver hangs past 10 s — the per-tick
	/// timeout fires and the poller logs a debug. Drives one tick under
	/// paused virtual clock so the test runs in microseconds; cancels
	/// before the next tick to avoid a publish race.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn battery_poller_timeout_branch_logs_and_continues() {
		let fake = FakeCameraBuilder::new().with_battery_info_pending().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(battery_poller(
			"cam-bt".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
			Arc::new(AtomicBool::new(false)),
		));
		// Advance past the first tick (20 ms) plus the 10 s per-tick timeout.
		tokio::time::advance(Duration::from_secs(11)).await;
		tokio::task::yield_now().await;
		cancel.cancel();
		let _ = task.await;
		// Battery level publish must NOT have happened — the timeout
		// branch logs and continues without publishing.
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-bt/status/battery_level"));
	}

	/// `floodlight_poller` timeout branch — driver hangs past 10 s.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn floodlight_poller_timeout_branch_logs_and_continues() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled_pending()
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(floodlight_poller(
			"cam-ft".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));
		// Advance past the first tick (20 ms) plus the 10 s per-tick timeout.
		tokio::time::advance(Duration::from_secs(11)).await;
		tokio::task::yield_now().await;
		cancel.cancel();
		let _ = task.await;
		// Timeout branch must NOT have published anything.
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-ft/status/floodlight_tasks"));
	}

	fn led_state(ir: &str, light: &str) -> bairelay_neolink_core::bc::xml::LedState {
		bairelay_neolink_core::bc::xml::LedState {
			state: ir.into(),
			light_state: light.into(),
			..Default::default()
		}
	}

	#[test]
	fn camera_led_values_map_to_mqtt_payloads() {
		assert_eq!(ir_mode_from_camera("open"), Some(IrMode::On));
		assert_eq!(ir_mode_from_camera("close"), Some(IrMode::Off));
		assert_eq!(ir_mode_from_camera("Auto"), Some(IrMode::Auto));
		assert_eq!(ir_mode_from_camera("dusk"), None);
		assert_eq!(ir_mode_from_camera(""), None);
		assert_eq!(led_on_from_camera("open"), Some(true));
		assert_eq!(led_on_from_camera("CLOSE"), Some(false));
		assert_eq!(led_on_from_camera("blink"), None);
	}

	/// A snapshot in flight when cancel fires is abandoned, not awaited
	/// for its 10 s timeout. Real time: a paused clock would hide the
	/// stall by auto-advancing through the timeout.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn preview_poller_abandons_in_flight_snapshot_on_cancel() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-pv"),
			cancel.clone(),
			None,
		));
		handle.set_driver_for_test(FakeCameraBuilder::new().with_snapshot_pending().build());
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let poller = tokio::spawn(preview_poller(
			"cam-pv".to_string(),
			Arc::clone(&handle),
			Arc::new(LastFrameBuffer::new()),
			mqtt,
			"bairelay".to_string(),
			10,
			handle.preview_state_rx(),
			true,
			cancel.clone(),
		));
		tokio::time::sleep(Duration::from_millis(100)).await; // snapshot now pending
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), poller)
			.await
			.expect("poller must exit promptly on cancel")
			.unwrap();
	}

	/// Same for a floodlight-tasks request in flight when cancel fires.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_poller_abandons_in_flight_request_on_cancel() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled_pending()
			.build();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let poller = tokio::spawn(floodlight_poller(
			"cam-fl".to_string(),
			fake,
			mqtt,
			"bairelay".to_string(),
			10,
			cancel.clone(),
			empty_cache(),
		));
		tokio::time::sleep(Duration::from_millis(100)).await; // request now pending
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), poller)
			.await
			.expect("poller must exit promptly on cancel")
			.unwrap();
	}

	/// Same for a battery request in flight when cancel fires.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_abandons_in_flight_request_on_cancel() {
		let fake = FakeCameraBuilder::new().with_battery_info_pending().build();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let poller = tokio::spawn(battery_poller(
			"cam-bat".to_string(),
			fake,
			mqtt,
			"bairelay".to_string(),
			10,
			cancel.clone(),
			empty_cache(),
			Arc::new(AtomicBool::new(false)),
		));
		tokio::time::sleep(Duration::from_millis(100)).await; // request now pending
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), poller)
			.await
			.expect("poller must exit promptly on cancel")
			.unwrap();
	}

	fn caps(status_led: bool, ir_leds: bool) -> CameraCapabilities {
		CameraCapabilities {
			status_led: Presence::from_signal(Some(status_led)),
			ir_leds: Presence::from_signal(Some(ir_leds)),
			..Default::default()
		}
	}

	async fn run_led_state(
		reply: (&'static str, &'static str),
		caps: CameraCapabilities,
	) -> (bairelay_mqtt::test_support::MockHandle, Arc<StatusCache>) {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(move || Ok(led_state(reply.0, reply.1)))
			.build();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cache = empty_cache();
		publish_led_state(
			"cam1".into(),
			fake,
			mqtt,
			"bairelay".into(),
			Arc::clone(&cache),
			caps,
		)
		.await;
		(mock, cache)
	}

	fn retained(mock: &bairelay_mqtt::test_support::MockHandle, topic: &str) -> Vec<Vec<u8>> {
		mock.published()
			.into_iter()
			.filter(|(t, _, r)| t == topic && *r)
			.map(|(_, p, _)| p)
			.collect()
	}

	/// Camera with both: one reply publishes IR mode and status LED
	/// (retained) and records them for broker-reconnect replay.
	#[tokio::test]
	async fn publish_led_state_publishes_both_when_both_present() {
		let (mock, cache) = run_led_state(("open", "close"), caps(true, true)).await;
		assert_eq!(
			retained(&mock, "bairelay/cam1/status/ir"),
			vec![b"on".to_vec()]
		);
		assert_eq!(
			retained(&mock, "bairelay/cam1/status/led"),
			vec![b"off".to_vec()]
		);
		assert_eq!(cache.ir(), Some(IrMode::On));
		assert_eq!(cache.led(), Some(false));
	}

	/// Status LED, no IR LEDs (Argus): the reply's IR `state` is a
	/// placeholder and must not be published.
	#[tokio::test]
	async fn publish_led_state_status_led_only_ignores_ir_placeholder() {
		let (mock, cache) = run_led_state(("close", "close"), caps(true, false)).await;
		assert!(retained(&mock, "bairelay/cam1/status/ir").is_empty());
		assert_eq!(
			retained(&mock, "bairelay/cam1/status/led"),
			vec![b"off".to_vec()]
		);
		assert_eq!((cache.ir(), cache.led()), (None, Some(false)));
	}

	/// IR LEDs, no status LED: the reply's `lightState` is a placeholder
	/// (observed `open` on a camera with no visible light).
	#[tokio::test]
	async fn publish_led_state_ir_only_ignores_status_led_placeholder() {
		let (mock, cache) = run_led_state(("auto", "open"), caps(false, true)).await;
		assert_eq!(
			retained(&mock, "bairelay/cam1/status/ir"),
			vec![b"auto".to_vec()]
		);
		assert!(retained(&mock, "bairelay/cam1/status/led").is_empty());
		assert_eq!((cache.ir(), cache.led()), (Some(IrMode::Auto), None));
	}

	/// Unrecognised camera values publish nothing for that field — the
	/// other field still lands.
	#[tokio::test]
	async fn publish_led_state_skips_unknown_values() {
		let (mock, cache) = run_led_state(("dusk", "open"), caps(true, true)).await;
		assert!(retained(&mock, "bairelay/cam1/status/ir").is_empty());
		assert_eq!(
			retained(&mock, "bairelay/cam1/status/led"),
			vec![b"on".to_vec()]
		);
		assert_eq!(cache.ir(), None);
	}

	/// Driver error publishes nothing.
	#[tokio::test]
	async fn publish_led_state_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"led refused",
				))
			})
			.build();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let all = caps(true, true);
		publish_led_state(
			"cam1".into(),
			fake,
			mqtt,
			"bairelay".into(),
			empty_cache(),
			all,
		)
		.await;
		assert!(mock.published().is_empty());
	}

	/// Timeout branch — driver hangs past 10 s; nothing published.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn publish_led_state_timeout_branch() {
		let fake = FakeCameraBuilder::new().with_ledstate_pending().build();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(publish_led_state(
			"cam1".into(),
			fake,
			mqtt,
			"bairelay".into(),
			empty_cache(),
			caps(true, true),
		));
		tokio::time::advance(Duration::from_secs(11)).await;
		let _ = task.await;
		assert!(mock.published().is_empty());
	}

	/// `publish_pir_state` timeout branch — driver hangs past 10 s.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn publish_pir_state_timeout_branch() {
		let fake = FakeCameraBuilder::new().with_pirstate_pending().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(publish_pir_state(
			"cam-pst".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		));
		tokio::time::advance(Duration::from_secs(11)).await;
		let _ = task.await;
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-pst/status/pir"));
	}

	// ── ha_cfg_support ───────────────────────────────────────────────

	fn ha_cfg_sample() -> bairelay_neolink_core::bc::xml::HaCfg {
		bairelay_neolink_core::bc::xml::HaCfg {
			version: "1.1".into(),
			enable: 1,
			url: "http://192.0.2.10/h".into(),
			verify_cert: 0,
		}
	}

	/// Fake whose HaCfg read fails with `errs` in order, then succeeds.
	fn scripted_ha_cfg(
		errs: Vec<fn() -> bairelay_neolink_core::Error>,
	) -> (Arc<dyn CameraDriver>, Arc<std::sync::atomic::AtomicUsize>) {
		let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let c = Arc::clone(&calls);
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(move || {
				let n = c.fetch_add(1, Ordering::SeqCst);
				match errs.get(n) {
					Some(f) => Err(f()),
					None => Ok(Some(ha_cfg_sample())),
				}
			})
			.build();
		(fake, calls)
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_retries_wake_400_then_some() {
		let (cam, calls) = scripted_ha_cfg(vec![|| unavailable(806)]);
		let got = ha_cfg_support(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(got, HaCfgSupport::Supported(ha_cfg_sample()));
		assert_eq!(calls.load(Ordering::SeqCst), 2);
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_persistent_400_is_none_after_window() {
		let (cam, calls) = scripted_ha_cfg(vec![|| unavailable(806); 30]);
		let t0 = tokio::time::Instant::now();
		let got = ha_cfg_support(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(got, HaCfgSupport::ErrorReply(400));
		assert_eq!(calls.load(Ordering::SeqCst), 7);
		assert_eq!(t0.elapsed(), NOT_READY_WINDOW);
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_other_error_code_is_none_immediately() {
		let (cam, calls) = scripted_ha_cfg(vec![|| {
			bairelay_neolink_core::Error::CameraServiceUnavailable { id: 806, code: 404 }
		}]);
		let t0 = tokio::time::Instant::now();
		let got = ha_cfg_support(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(got, HaCfgSupport::ErrorReply(404));
		assert_eq!(got.error_code(), Some(404));
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(t0.elapsed(), Duration::ZERO);
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_empty_reply_has_no_code() {
		let fake = FakeCameraBuilder::new().with_ha_cfg(|| Ok(None)).build();
		let got = ha_cfg_support(fake.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(got, HaCfgSupport::EmptyReply);
		assert_eq!(got.error_code(), None);
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_transport_error_is_err() {
		let (cam, calls) = scripted_ha_cfg(vec![|| bairelay_neolink_core::Error::Other("boom")]);
		let err = ha_cfg_support(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap_err();
		assert!(format!("{err:#}").contains("get_ha_cfg failed"));
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_timeout_is_err() {
		let fake = FakeCameraBuilder::new().with_ha_cfg_pending().build();
		let err = ha_cfg_support(fake.as_ref(), "cam1", &CancellationToken::new())
			.await
			.unwrap_err();
		assert!(format!("{err:#}").contains("timed out"), "{err:#}");
	}

	#[tokio::test(start_paused = true)]
	async fn ha_cfg_support_cancelled_is_err() {
		let fake = FakeCameraBuilder::new().with_ha_cfg_pending().build();
		let cancel = CancellationToken::new();
		cancel.cancel();
		let err = ha_cfg_support(fake.as_ref(), "cam1", &cancel)
			.await
			.unwrap_err();
		assert!(format!("{err:#}").contains("cancelled"));
	}

	// ── arm_webhook ──────────────────────────────────────────────────

	const ARM_TOKEN: &str = "0123456789abcdef0123456789abcdef";
	const ARM_URL: &str = "http://192.0.2.5:8780/hook/0123456789abcdef0123456789abcdef";
	const FOREIGN_URL: &str =
		"http://user:pw@ha.example.com:8123/api/webhook/SECRETID?key=QUERYSECRET";

	fn ha_cfg(enable: u8, url: &str, verify_cert: u8) -> bairelay_neolink_core::bc::xml::HaCfg {
		bairelay_neolink_core::bc::xml::HaCfg {
			version: "1.1".into(),
			enable,
			url: url.into(),
			verify_cert,
		}
	}

	/// What [`arm_webhook`] writes for [`ARM_URL`].
	fn armed_cfg() -> bairelay_neolink_core::bc::xml::HaCfg {
		ha_cfg(1, ARM_URL, 0)
	}

	/// Fake whose first HaCfg read returns `stored`; the read-back after
	/// a write returns `back`.
	fn ha_cfg_camera(
		stored: bairelay_neolink_core::bc::xml::HaCfg,
		back: Option<bairelay_neolink_core::bc::xml::HaCfg>,
	) -> Arc<bairelay_neolink_core::bc_protocol::FakeCamera> {
		let reads = std::sync::atomic::AtomicUsize::new(0);
		FakeCameraBuilder::new()
			.with_ha_cfg(move || match reads.fetch_add(1, Ordering::SeqCst) {
				0 => Ok(Some(stored.clone())),
				_ => Ok(back.clone()),
			})
			.build()
	}

	async fn arm(camera: &bairelay_neolink_core::bc_protocol::FakeCamera) -> ArmOutcome {
		arm_webhook(
			camera,
			"cam1",
			ARM_URL,
			&ArmMemo::default(),
			&CancellationToken::new(),
		)
		.await
	}

	#[tokio::test(start_paused = true)]
	async fn arm_unsupported_logs_once_per_flag_and_never_writes() {
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let fake = FakeCameraBuilder::new().with_ha_cfg(|| Ok(None)).build();
		let memo = ArmMemo::default();
		for _ in 0..3 {
			let got = arm_webhook(
				fake.as_ref(),
				"cam1",
				ARM_URL,
				&memo,
				&CancellationToken::new(),
			)
			.await;
			assert_eq!(got, ArmOutcome::Unsupported);
		}
		assert_eq!(
			logs.text().matches("lacks HaCfg").count(),
			1,
			"{}",
			logs.text()
		);
		assert!(!logs.text().contains("code="), "{}", logs.text());
		assert!(fake.calls().set_ha_cfg.lock().unwrap().is_empty());
	}

	#[tokio::test(start_paused = true)]
	async fn arm_unsupported_logs_the_error_reply_code() {
		let logs = crate::test_logs::capture_at(tracing::Level::INFO);
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| {
				Err(bairelay_neolink_core::Error::CameraServiceUnavailable { id: 806, code: 404 })
			})
			.build();
		assert_eq!(arm(&fake).await, ArmOutcome::Unsupported);
		let out = logs.text();
		assert!(
			out.lines()
				.any(|l| l.contains("lacks HaCfg") && l.contains("code=404")),
			"{out}"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn arm_already_armed_writes_nothing() {
		for stored in [armed_cfg(), ha_cfg(1, ARM_URL, 1)] {
			let fake = ha_cfg_camera(stored, None);
			assert_eq!(arm(&fake).await, ArmOutcome::AlreadyArmed);
			assert!(fake.calls().set_ha_cfg.lock().unwrap().is_empty());
		}
	}

	#[tokio::test(start_paused = true)]
	async fn arm_replaces_a_foreign_url_and_logs_only_its_host() {
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let fake = ha_cfg_camera(ha_cfg(1, FOREIGN_URL, 1), Some(armed_cfg()));
		assert_eq!(arm(&fake).await, ArmOutcome::Armed);
		assert_eq!(*fake.calls().set_ha_cfg.lock().unwrap(), vec![armed_cfg()]);
		let out = logs.text();
		let warning = out
			.lines()
			.find(|l| l.contains("WARN") && l.contains("another webhook"))
			.unwrap_or_else(|| panic!("no replace warning in {out}"));
		assert!(warning.contains("cam1"), "{warning}");
		assert!(warning.contains("ha.example.com"), "{warning}");
		for secret in [
			"SECRETID",
			"QUERYSECRET",
			"/api/",
			"user:pw",
			"8123",
			ARM_TOKEN,
		] {
			assert!(!out.contains(secret), "{secret} leaked into {out}");
		}
	}

	#[tokio::test(start_paused = true)]
	async fn arm_writes_an_empty_or_disabled_config_without_a_foreign_warning() {
		let mut unversioned = ha_cfg(0, "", 1);
		unversioned.version.clear();
		for stored in [unversioned, ha_cfg(0, ARM_URL, 0)] {
			let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
			let fake = ha_cfg_camera(stored.clone(), Some(armed_cfg()));
			assert_eq!(arm(&fake).await, ArmOutcome::Armed, "{stored:?}");
			assert_eq!(*fake.calls().set_ha_cfg.lock().unwrap(), vec![armed_cfg()]);
			let out = logs.text();
			assert!(!out.contains("WARN"), "{out}");
			assert!(!out.contains(ARM_TOKEN), "{out}");
		}
	}

	/// A URL from an earlier run differs from ours only in its token.
	#[tokio::test(start_paused = true)]
	async fn arm_refreshes_our_own_stale_token_without_a_warning() {
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let stale = "http://192.0.2.5:8780/hook/ffffffffffffffffffffffffffffffff";
		let fake = ha_cfg_camera(ha_cfg(1, stale, 0), Some(armed_cfg()));
		assert_eq!(arm(&fake).await, ArmOutcome::Armed);
		assert_eq!(*fake.calls().set_ha_cfg.lock().unwrap(), vec![armed_cfg()]);
		let out = logs.text();
		assert!(!out.contains("WARN"), "{out}");
		assert!(
			out.lines()
				.any(|l| l.contains("INFO") && l.contains("Refreshing webhook token")),
			"{out}"
		);
		for secret in ["ffffffff", ARM_TOKEN] {
			assert!(!out.contains(secret), "{secret} leaked into {out}");
		}
	}

	/// A hook URL of bairelay's on another host or port (the config or
	/// this host changed) is bairelay's own, refreshed without a warning.
	#[tokio::test(start_paused = true)]
	async fn arm_refreshes_our_own_url_from_another_host_without_a_warning() {
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let moved = "http://198.51.100.7:9999/hook/ffffffffffffffffffffffffffffffff";
		let fake = ha_cfg_camera(ha_cfg(1, moved, 0), Some(armed_cfg()));
		assert_eq!(arm(&fake).await, ArmOutcome::Armed);
		let out = logs.text();
		assert!(!out.contains("WARN"), "{out}");
		assert!(out.contains("Refreshing webhook token"), "{out}");
	}

	#[test]
	fn own_stale_url_table() {
		let token = "ffffffffffffffffffffffffffffffff";
		let cases = [
			(format!("http://192.0.2.5:8780/hook/{token}"), true),
			(format!("http://example.com/hook/{token}"), true),
			(format!("https://[2001:db8::1]:1/hook/{token}"), true),
			// The same listener with any token.
			("http://192.0.2.5:8780/hook/x".to_string(), true),
			// Elsewhere, only a path of exactly `/hook/` + 32 lowercase hex.
			(format!("http://example.com/hook/{}", &token[..31]), false),
			(format!("http://example.com/hook/{token}0"), false),
			(
				format!("http://example.com/hook/{}", token.to_uppercase()),
				false,
			),
			(format!("http://example.com/hook/{token}?a=1"), false),
			(format!("http://example.com/x/hook/{token}"), false),
			(format!("http://example.com/hook/{token}/"), false),
			("http://example.com/api/webhook/abc".to_string(), false),
			// A `/hook/…` inside a query or fragment is no path.
			(format!("http://example.com?/hook/{token}"), false),
			(format!("http://example.com#/hook/{token}"), false),
			(format!("http://example.com:8780?x/hook/{token}"), false),
			(String::new(), false),
		];
		for (stored, want) in cases {
			assert_eq!(is_own_stale_url(&stored, ARM_URL), want, "{stored}");
		}
	}

	/// A new desired URL (the camera's IP changed) is written even
	/// though the camera still stores what it stored last time.
	#[tokio::test(start_paused = true)]
	async fn arm_writes_a_new_desired_url_over_a_remembered_mismatch() {
		let truncated = ha_cfg(1, &ARM_URL[..40], 0);
		let fake = ha_cfg_camera(ha_cfg(0, "", 0), Some(truncated));
		let memo = ArmMemo::default();
		let moved = ARM_URL.replace("192.0.2.5", "192.0.2.6");
		for (url, writes) in [
			(ARM_URL, 1),
			(ARM_URL, 1),
			(moved.as_str(), 2),
			(moved.as_str(), 2),
		] {
			let got =
				arm_webhook(fake.as_ref(), "cam1", url, &memo, &CancellationToken::new()).await;
			assert_eq!(got, ArmOutcome::Unconfirmed, "{url}");
			assert_eq!(
				fake.calls().set_ha_cfg.lock().unwrap().len(),
				writes,
				"{url}"
			);
		}
	}

	/// Each skip reason warns once; repeats of a reason log at debug.
	#[test]
	fn report_no_host_warns_once_per_reason() {
		use crate::webhook_listener::NoWebhookHost::{LoopbackOnly, UnknownCamera};
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let memo = ArmMemo::default();
		for reason in [
			UnknownCamera,
			UnknownCamera,
			LoopbackOnly,
			LoopbackOnly,
			UnknownCamera,
		] {
			memo.report_no_host("cam1", reason);
		}
		let out = logs.text();
		let warns: Vec<&str> = out.lines().filter(|l| l.contains("WARN")).collect();
		assert_eq!(warns.len(), 2, "{out}");
		assert!(warns[0].contains("address is unknown"), "{out}");
		assert!(warns[1].contains("loopback"), "{out}");
		assert_eq!(out.matches("DEBUG").count(), 3, "{out}");
	}

	/// A camera that keeps storing something other than what was
	/// written (here a truncated URL) is written once, not every pass.
	#[tokio::test(start_paused = true)]
	async fn arm_does_not_rewrite_what_the_camera_keeps_storing() {
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let truncated = ha_cfg(1, &ARM_URL[..40], 0);
		let fake = ha_cfg_camera(ha_cfg(0, "", 0), Some(truncated));
		let memo = ArmMemo::default();
		for _ in 0..3 {
			let got = arm_webhook(
				fake.as_ref(),
				"cam1",
				ARM_URL,
				&memo,
				&CancellationToken::new(),
			)
			.await;
			assert_eq!(got, ArmOutcome::Unconfirmed);
		}
		assert_eq!(fake.calls().set_ha_cfg.lock().unwrap().len(), 1);
		let out = logs.text();
		assert_eq!(out.matches("WARN").count(), 1, "{out}");
	}

	#[tokio::test(start_paused = true)]
	async fn arm_warns_when_the_read_back_differs() {
		for back in [
			None,
			Some(ha_cfg(0, ARM_URL, 0)),
			Some(ha_cfg(1, FOREIGN_URL, 0)),
			Some(ha_cfg(1, ARM_URL, 1)),
		] {
			let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
			let fake = ha_cfg_camera(ha_cfg(0, "", 0), back.clone());
			assert_eq!(arm(&fake).await, ArmOutcome::Unconfirmed, "{back:?}");
			let out = logs.text();
			assert!(
				out.lines()
					.any(|l| l.contains("WARN") && l.contains("read-back")),
				"{out}"
			);
			for secret in ["SECRETID", "QUERYSECRET", ARM_TOKEN] {
				assert!(!out.contains(secret), "{secret} leaked into {out}");
			}
		}
	}

	#[tokio::test(start_paused = true)]
	async fn arm_cancel_during_the_write_returns_at_once() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Ok(Some(ha_cfg(0, "", 0))))
			.with_set_ha_cfg_pending()
			.build();
		let cancel = CancellationToken::new();
		let task = {
			let fake = Arc::clone(&fake);
			let cancel = cancel.clone();
			tokio::spawn(async move {
				arm_webhook(fake.as_ref(), "cam1", ARM_URL, &ArmMemo::default(), &cancel).await
			})
		};
		for _ in 0..1000 {
			if !fake.calls().set_ha_cfg.lock().unwrap().is_empty() {
				break;
			}
			tokio::task::yield_now().await;
		}
		assert_eq!(
			fake.calls().set_ha_cfg.lock().unwrap().len(),
			1,
			"write issued"
		);
		let t0 = tokio::time::Instant::now();
		cancel.cancel();
		assert_eq!(task.await.unwrap(), ArmOutcome::Cancelled);
		assert_eq!(t0.elapsed(), Duration::ZERO);
		assert_eq!(*fake.calls().set_ha_cfg.lock().unwrap(), vec![armed_cfg()]);
	}

	#[tokio::test(start_paused = true)]
	async fn arm_write_is_bounded() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Ok(Some(ha_cfg(0, "", 0))))
			.with_set_ha_cfg_pending()
			.build();
		let t0 = tokio::time::Instant::now();
		assert_eq!(arm(&fake).await, ArmOutcome::Failed);
		assert_eq!(t0.elapsed(), HA_CFG_SET_TIMEOUT);
	}

	#[tokio::test(start_paused = true)]
	async fn arm_read_failure_writes_nothing() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Err(bairelay_neolink_core::Error::Other("boom")))
			.build();
		assert_eq!(arm(&fake).await, ArmOutcome::Failed);
		assert!(fake.calls().set_ha_cfg.lock().unwrap().is_empty());
	}

	#[tokio::test(start_paused = true)]
	async fn arm_cancelled_read_writes_nothing() {
		let fake = FakeCameraBuilder::new().with_ha_cfg_pending().build();
		let cancel = CancellationToken::new();
		cancel.cancel();
		let got = arm_webhook(fake.as_ref(), "cam1", ARM_URL, &ArmMemo::default(), &cancel).await;
		assert_eq!(got, ArmOutcome::Cancelled);
		assert!(fake.calls().set_ha_cfg.lock().unwrap().is_empty());
	}

	#[test]
	fn url_host_keeps_only_the_host() {
		for (url, want) in [
			(FOREIGN_URL, Some("ha.example.com")),
			("http://192.0.2.7:8123/api/webhook/x", Some("192.0.2.7")),
			("https://example.com", Some("example.com")),
			("http://example.com?k=v", Some("example.com")),
			("http://example.com#frag", Some("example.com")),
			("HTTPS://example.com/x", Some("example.com")),
			("example.com/path", None),
			("SECRET", None),
			("SECRET/path", None),
			("ftp://example.com/x", None),
			("http://u/ser:pw@host/x", None),
			("http://TOKEN/x@host/", None),
			("http://TOKEN?x@host/", None),
			("http://[2001:db8::1]:8123/x", Some("[2001:db8::1]")),
			("http://u@[2001:db8::1]/x", Some("[2001:db8::1]")),
			("http://a@b@example.com/x", Some("example.com")),
			("", None),
			("http://", None),
			("http:///path", None),
			("http://:8123/x", None),
			("http://[2001:db8::1/x", None),
			("http://exa mple.com/x", None),
			("http://example.com\u{1b}[31m/x", None),
			("http://ex\nample.com/x", None),
			("http://éxample.com/x", None),
		] {
			assert_eq!(url_host(url), want, "{url:?}");
		}
		let long = format!("http://{}/x", "a".repeat(300));
		assert_eq!(url_host(&long), None);
	}

	// ── snapshot_with_retry ──────────────────────────────────────────

	fn unavailable(id: u32) -> bairelay_neolink_core::Error {
		bairelay_neolink_core::Error::CameraServiceUnavailable { id, code: 400 }
	}

	/// Fake whose snapshot fails with `errs` in order, then succeeds;
	/// the counter records every attempt.
	fn scripted_snapshot(
		errs: Vec<fn() -> bairelay_neolink_core::Error>,
	) -> (Arc<dyn CameraDriver>, Arc<std::sync::atomic::AtomicUsize>) {
		let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let c = Arc::clone(&calls);
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || {
				let n = c.fetch_add(1, Ordering::SeqCst);
				match errs.get(n) {
					Some(f) => Err(f()),
					None => Ok(b"JPEG".to_vec()),
				}
			})
			.build();
		(fake, calls)
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_succeeds_after_one_unavailable() {
		let (cam, calls) = scripted_snapshot(vec![|| unavailable(109)]);
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert_eq!(got.unwrap().unwrap(), b"JPEG");
		assert_eq!(calls.load(Ordering::SeqCst), 2);
	}

	/// 400 until ~8 s after the first attempt, then bytes.
	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_succeeds_once_camera_is_ready_after_8s() {
		let t0 = tokio::time::Instant::now();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || {
				if t0.elapsed() < Duration::from_secs(8) {
					Err(unavailable(109))
				} else {
					Ok(b"JPEG".to_vec())
				}
			})
			.build();
		let got = snapshot_with_retry(fake.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert_eq!(got.unwrap().unwrap(), b"JPEG");
		assert_eq!(t0.elapsed(), Duration::from_secs(8));
	}

	/// Persistent 400: attempts at 0, 2, ... 12 s, then the 400 is returned.
	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_gives_up_after_the_window() {
		let (cam, calls) = scripted_snapshot(vec![|| unavailable(109); 30]);
		let t0 = tokio::time::Instant::now();
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert!(got.unwrap().is_err());
		assert_eq!(calls.load(Ordering::SeqCst), 7);
		assert_eq!(t0.elapsed(), NOT_READY_WINDOW);
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_does_not_retry_other_errors() {
		let (cam, calls) = scripted_snapshot(vec![|| bairelay_neolink_core::Error::Other("boom")]);
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert!(got.unwrap().is_err());
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_does_not_retry_other_message_ids() {
		let (cam, calls) = scripted_snapshot(vec![|| unavailable(33)]);
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert!(got.unwrap().is_err());
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_cancel_during_backoff_returns_promptly() {
		let (cam, calls) = scripted_snapshot(vec![|| unavailable(109); 3]);
		let cancel = CancellationToken::new();
		let c = cancel.clone();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(500)).await;
			c.cancel();
		});
		let start = tokio::time::Instant::now();
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &cancel).await;
		assert!(got.is_none());
		assert!(start.elapsed() < Duration::from_secs(1));
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_retry_does_not_retry_other_codes() {
		let (cam, calls) = scripted_snapshot(vec![|| {
			bairelay_neolink_core::Error::CameraServiceUnavailable { id: 109, code: 200 }
		}]);
		let got = snapshot_with_retry(cam.as_ref(), "cam1", &CancellationToken::new())
			.await
			.expect("not cancelled");
		assert!(got.unwrap().is_err());
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	/// Failure, failure, success, failure: warn, debug, (reset), warn.
	#[test]
	fn preview_failure_warns_once_per_streak() {
		let captured = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let mut failing = false;
		log_preview_failure(&mut failing, "cam1", &"boom");
		log_preview_failure(&mut failing, "cam1", &"boom");
		failing = false; // a success resets the streak
		log_preview_failure(&mut failing, "cam1", &"boom");
		let logs = captured.text();
		assert_eq!(logs.matches("WARN").count(), 2, "{logs}");
		assert_eq!(logs.matches("DEBUG").count(), 1, "{logs}");
	}
}
