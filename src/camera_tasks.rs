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
	AiDetections, CameraDriver, Error as CoreError, MotionStatus,
};

use bairelay_mqtt::control::IrMode;
use bairelay_mqtt::{MotionClass, MqttError, SharedMqttClient, StatusPublisher};
use bairelay_rtsp::buffer::LastFrameBuffer;

use crate::camera::ReconnectBackoff;
use crate::capabilities::CameraCapabilities;
#[cfg(test)]
use crate::capabilities::Presence;
use crate::preview_overlay::OverlayCache;
use crate::preview_state::PreviewState;
use crate::status_cache::StatusCache;
use crate::wake_lock::{WakeLockCounter, WakeLockGuard};

// ── Motion Detection ─────────────────────────────────────────────────

fn class_detected(detections: AiDetections, class: MotionClass) -> bool {
	match class {
		MotionClass::Person => detections.person,
		MotionClass::Vehicle => detections.vehicle,
		MotionClass::Animal => detections.animal,
		MotionClass::Other => detections.other,
	}
}

fn detected_class_names(detections: AiDetections) -> String {
	MotionClass::ALL
		.into_iter()
		.filter(|&class| class_detected(detections, class))
		.map(MotionClass::as_str)
		.collect::<Vec<_>>()
		.join(",")
}

/// Publish every per-class motion topic from one set of detections;
/// `AiDetections::default()` turns every class off.
pub(crate) async fn publish_motion_classes(
	publisher: &StatusPublisher<'_>,
	detections: AiDetections,
) -> Result<(), MqttError> {
	for class in MotionClass::ALL {
		publisher
			.publish_motion_class(class, class_detected(detections, class))
			.await?;
	}
	Ok(())
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
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	// Sustained network flap with N cameras × M retries/min produces a
	// firehose of warns and pointless reconnect attempts. Use the same
	// 1 → 60 s exponential ladder as the camera reconnect path; reset
	// to 1 s every time we successfully subscribe so a transient blip
	// doesn't permanently inflate the retry interval.
	let mut backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));

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

		let mut wake_guard: Option<WakeLockGuard> = None;
		// Pending wake-lock release deadline. `Some(_)` while a Stop has
		// been observed and we're inside the post-motion hold-down
		// window; cleared by either the timer firing (release) or a
		// fresh Start (re-arm). Polled inline via the select! arm below
		// — no detached `tokio::spawn`, so the count of in-flight
		// "release N seconds from now" futures is at most one per
		// listener at any time, regardless of motion-event flap rate.
		let mut release_at: Option<tokio::time::Instant> = None;

		loop {
			// `sleep_until` requires a concrete deadline. When no
			// release is scheduled, point it 24 h out as a placeholder
			// — the `if release_at.is_some()` guard disables that arm,
			// so the placeholder is never actually reached.
			let release_deadline = release_at
				.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

			tokio::select! {
				_ = cancel.cancelled() => return,
				_ = tokio::time::sleep_until(release_deadline), if release_at.is_some() => {
					// Hold-down expired with no new motion — drop the
					// wake-lock guard so the camera becomes eligible
					// for idle disconnect.
					wake_guard = None;
					release_at = None;
				}
				result = motion_data.next_motion() => {
					match result {
						Ok(MotionStatus::Start(_, detections)) => {
							tracing::info!(
								camera = %camera_name,
								classes = %detected_class_names(detections),
								"Motion detected"
							);
							// New motion cancels any pending release;
							// re-acquire only if we don't already hold
							// one (a back-to-back Start without an
							// intervening Stop is rare but harmless).
							release_at = None;
							if wake_guard.is_none() {
								wake_guard = Some(wake_lock.acquire());
							}
							let _ = publisher.publish_motion(true).await;
							status_cache.set_motion(true);
							let _ = publish_motion_classes(&publisher, detections).await;
							status_cache.set_motion_classes(detections);
						}
						Ok(MotionStatus::Stop(_)) => {
							tracing::info!(camera = %camera_name, "Motion stopped");
							let _ = publisher.publish_motion(false).await;
							status_cache.set_motion(false);
							let none = AiDetections::default();
							let _ = publish_motion_classes(&publisher, none).await;
							status_cache.set_motion_classes(none);
							if wake_guard.is_some() {
								release_at = Some(
									tokio::time::Instant::now() + motion_wake_hold,
								);
							}
						}
						Ok(MotionStatus::NoChange(_)) => {}
						Err(e) => {
							tracing::warn!(camera = %camera_name, error = %e, "Motion listener error");
							break;
						}
					}
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
/// [`classify_battery_tick`] for why a transient error must not count.
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
/// [`BATTERY_MAX_UNSUPPORTED`] times in a row **and has never once
/// answered it**, setting `battery_unsupported` so `spawn_session_tasks`
/// doesn't re-probe on the next reconnect. Mains-powered models have no
/// battery and refuse every probe; without the give-up they are re-asked
/// every `battery_update` ms for the life of the process.
///
/// The never-answered condition is what makes the latch safe. A camera
/// that has produced even one battery level has proven the hardware is
/// there, so a later run of malformed replies — which
/// [`classify_battery_tick`] cannot distinguish from a refusal — can
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
	const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let overlay_cache = OverlayCache::new();
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	// Drop the immediate first tick to avoid an unexpected burst on connect.
	ticker.tick().await;

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
							cancellable(&cancel, SNAPSHOT_TIMEOUT, bc.get_snapshot()).await
						else {
							break;
						};
						match snapshot {
							Ok(Ok(bytes)) => {
								last_frame.set_jpeg(bytes::Bytes::from(bytes));
							}
							Ok(Err(e)) => {
								tracing::debug!(camera = %camera_name, error = %e, "preview snapshot failed");
							}
							Err(_) => {
								tracing::debug!(camera = %camera_name, "preview snapshot timed out");
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
	use bairelay_neolink_core::bc_protocol::{CameraDriver, FakeCameraBuilder};
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
		));

		// Push a Start event and wait briefly for the publish to
		// flow through.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
				AiDetections::default(),
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

	/// Each `Start` publishes the full per-class state (so a camera
	/// re-classifying an ongoing alarm flips the right topics) and
	/// `Stop` turns every class off. All retained, all cached.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_publishes_per_class_state() {
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
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cache = empty_cache();
		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			crate::wake_lock::WakeLockCounter::new(),
			cancel.clone(),
			Duration::from_secs(30),
			Arc::clone(&cache),
		));

		let class_states = |mock: &MockHandle| -> Vec<(String, String)> {
			MotionClass::ALL
				.into_iter()
				.map(|class| {
					let topic = format!("bairelay/cam1/status/motion/{}", class.as_str());
					let last = mock
						.published()
						.into_iter()
						.rev()
						.find(|(t, _, r)| *t == topic && *r)
						.map(|(_, p, _)| String::from_utf8(p).unwrap())
						.unwrap_or_default();
					(class.as_str().to_string(), last)
				})
				.collect()
		};
		let expect = |person: &str, vehicle: &str, animal: &str, other: &str| {
			[
				("person", person),
				("vehicle", vehicle),
				("animal", animal),
				("other", other),
			]
			.map(|(c, s)| (c.to_string(), s.to_string()))
			.to_vec()
		};
		async fn send_and_settle(
			tx: &tokio::sync::mpsc::Sender<MotionItem>,
			mock: &MockHandle,
			status: bairelay_neolink_core::bc_protocol::MotionStatus,
		) {
			let before = mock.count();
			tx.send(Ok(status)).await.unwrap();
			// One aggregate + four per-class publishes per event.
			let settled = tokio::time::timeout(Duration::from_secs(2), async {
				while mock.count() < before + 5 {
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
			})
			.await;
			assert!(settled.is_ok(), "event publishes did not land");
		}

		let person = AiDetections {
			person: true,
			..Default::default()
		};
		send_and_settle(
			&motion_tx,
			&mock,
			MotionStatus::Start(std::time::Instant::now(), person),
		)
		.await;
		assert_eq!(class_states(&mock), expect("on", "off", "off", "off"));
		assert_eq!(cache.motion_classes(), Some(person));

		let other = AiDetections {
			other: true,
			..Default::default()
		};
		send_and_settle(
			&motion_tx,
			&mock,
			MotionStatus::Start(std::time::Instant::now(), other),
		)
		.await;
		assert_eq!(class_states(&mock), expect("off", "off", "off", "on"));

		send_and_settle(
			&motion_tx,
			&mock,
			MotionStatus::Stop(std::time::Instant::now()),
		)
		.await;
		assert_eq!(class_states(&mock), expect("off", "off", "off", "off"));
		assert_eq!(cache.motion_classes(), Some(AiDetections::default()));
		assert_eq!(cache.motion(), Some(false));

		cancel.cancel();
		let _ = task.await;
	}

	#[test]
	fn detected_class_names_lists_active_classes_in_order() {
		let d = AiDetections {
			person: true,
			animal: true,
			..Default::default()
		};
		assert_eq!(detected_class_names(d), "person,animal");
		assert_eq!(detected_class_names(AiDetections::default()), "");
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
		));

		// Start then Stop in quick succession.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
				AiDetections::default(),
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
		));

		// Start → wake lock acquired.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
				AiDetections::default(),
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
		));

		// Start → Stop → Start, all within the hold-down window.
		motion_tx
			.send(Ok(bairelay_neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
				AiDetections::default(),
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
				AiDetections::default(),
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
				AiDetections::default(),
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
}
