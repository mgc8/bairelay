//! Wake-lock holds and motion edges shared by the listeners that hear
//! from a sleeping camera: the push listener and the webhook listener.
//!
//! The two listeners can report one wake twice; `claim_wake` folds
//! them into one.
//!
//! Each entry point spawns a detached task bounded by its hold window
//! and the cancel token, so the caller's accept loop never waits on it
//! and nothing outlives shutdown.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use bairelay_mqtt::{SharedMqttClient, StatusPublisher};

use crate::camera::CameraHandle;
use crate::status_cache::WakeSource;

/// Upper bound on the fallback `motion=off` publish, so a wedged broker
/// can't hold the runtime open during shutdown.
const FALLBACK_PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

/// The MQTT client for a camera's motion-family events (motion, AI
/// objects, doorbell presses): `None` without MQTT or when the camera
/// has `[cameras.mqtt] enable_motion = false`.
pub(crate) fn event_mqtt<'a>(
	handle: &CameraHandle,
	mqtt: Option<&'a SharedMqttClient>,
) -> Option<&'a SharedMqttClient> {
	mqtt.filter(|_| handle.config().mqtt.enable_motion)
}

/// Claim a wake for `source`: `false` when the other source already
/// reported this wake (see [`crate::status_cache::WAKE_DEDUPE_WINDOW`]),
/// in which case the caller takes no hold and publishes no motion.
pub(crate) fn claim_wake(handle: &CameraHandle, source: WakeSource) -> bool {
	let claimed = handle
		.status_cache()
		.claim_wake(source, tokio::time::Instant::now());
	if !claimed {
		tracing::debug!(camera = %handle.name(), source = ?source, "Wake already reported by the other source");
	}
	claimed
}

/// A push-listener edge: a [`motion_edge`] unless a webhook wake
/// already covers it. Returns whether the edge was acted on.
pub(crate) fn push_edge(
	handle: Arc<CameraHandle>,
	mqtt: Option<&SharedMqttClient>,
	topic_prefix: &str,
	hold: Duration,
	cancel: &CancellationToken,
) -> bool {
	if !claim_wake(&handle, WakeSource::Push) {
		return false;
	}
	motion_edge(handle, mqtt, topic_prefix, hold, cancel);
	true
}

/// A motion edge from a sleeping camera: publish `status/motion=on`,
/// then [`motion_hold`]. Without MQTT, or with `enable_motion` off, the
/// edge is a wake-lock hold only.
pub(crate) fn motion_edge(
	handle: Arc<CameraHandle>,
	mqtt: Option<&SharedMqttClient>,
	topic_prefix: &str,
	hold: Duration,
	cancel: &CancellationToken,
) {
	spawn_motion_hold(handle, mqtt, topic_prefix, hold, cancel, true);
}

/// Hold the camera's wake lock for `hold` so the connect loop reaches
/// it, then publish a fallback `status/motion=off` so motion never
/// sticks `on` when no session follows. Bumps the camera's
/// [`CameraHandle::push_seq`]: only the newest edge's window clears
/// motion, across push, webhook PIR and forwarded-alarm edges. For a
/// caller that has already published `on`.
pub(crate) fn motion_hold(
	handle: Arc<CameraHandle>,
	mqtt: Option<&SharedMqttClient>,
	topic_prefix: &str,
	hold: Duration,
	cancel: &CancellationToken,
) {
	spawn_motion_hold(handle, mqtt, topic_prefix, hold, cancel, false);
}

fn spawn_motion_hold(
	handle: Arc<CameraHandle>,
	mqtt: Option<&SharedMqttClient>,
	topic_prefix: &str,
	hold: Duration,
	cancel: &CancellationToken,
	publish_on: bool,
) {
	let seq = handle.push_seq().fetch_add(1, Ordering::SeqCst) + 1;
	let mqtt = event_mqtt(&handle, mqtt).cloned();
	let prefix = topic_prefix.to_string();
	let cancel = cancel.clone();
	tokio::spawn(async move {
		let _guard = handle.wake_lock().acquire();
		let publisher = mqtt
			.as_ref()
			.map(|m| StatusPublisher::new(m, &prefix, handle.name()));
		if let (Some(publisher), true) = (&publisher, publish_on) {
			if let Err(e) = publisher.publish_motion(true).await {
				tracing::warn!(camera = %handle.name(), error = %e, "wake motion: publish_motion(true) failed");
			}
			handle.status_cache().set_motion(true);
		}

		crate::run_support::sleep_or_cancel(hold, &cancel).await;

		if handle.push_seq().load(Ordering::SeqCst) != seq {
			tracing::debug!(camera = %handle.name(), "wake motion: a later edge owns the fallback clear");
			return;
		}
		let Some(publisher) = publisher else {
			return;
		};
		match tokio::time::timeout(FALLBACK_PUBLISH_TIMEOUT, publisher.publish_motion(false)).await
		{
			Ok(Ok(())) => {
				handle.status_cache().set_motion(false);
			}
			Ok(Err(e)) => {
				tracing::warn!(camera = %handle.name(), error = %e, "wake motion: fallback publish_motion(false) failed");
			}
			Err(_) => {
				tracing::debug!(
					camera = %handle.name(),
					"wake motion: fallback publish_motion(false) timed out (likely shutdown)"
				);
			}
		}
	});
}

/// Hold the camera's wake lock for `hold` (or until cancel) so the
/// connect loop reaches the camera while it is awake.
pub(crate) fn spawn_wake_hold(
	handle: Arc<CameraHandle>,
	hold: Duration,
	cancel: &CancellationToken,
) {
	let cancel = cancel.clone();
	tokio::spawn(async move {
		let _guard = handle.wake_lock().acquire();
		crate::run_support::sleep_or_cancel(hold, &cancel).await;
	});
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;

	fn handle() -> Arc<CameraHandle> {
		Arc::new(CameraHandle::new(
			minimal_camera_config("cam1"),
			CancellationToken::new(),
			None,
		))
	}

	fn motion_payloads(mock: &bairelay_mqtt::test_support::MockHandle) -> Vec<Vec<u8>> {
		mock.published()
			.into_iter()
			.filter(|(t, _, _)| t == "bairelay/cam1/status/motion")
			.map(|(_, p, _)| p)
			.collect()
	}

	#[tokio::test(start_paused = true)]
	async fn wake_hold_releases_after_hold() {
		let h = handle();
		let cancel = CancellationToken::new();
		spawn_wake_hold(Arc::clone(&h), Duration::from_secs(5), &cancel);
		tokio::time::sleep(Duration::from_secs(1)).await;
		assert_eq!(h.wake_lock().count(), 1);
		tokio::time::sleep(Duration::from_secs(5)).await;
		assert_eq!(h.wake_lock().count(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn wake_hold_releases_on_cancel() {
		let h = handle();
		let cancel = CancellationToken::new();
		spawn_wake_hold(Arc::clone(&h), Duration::from_secs(3600), &cancel);
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert_eq!(h.wake_lock().count(), 1);
		cancel.cancel();
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert_eq!(h.wake_lock().count(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn motion_edge_publishes_on_then_fallback_off() {
		let h = handle();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		motion_edge(
			Arc::clone(&h),
			Some(&mqtt),
			"bairelay",
			Duration::from_secs(5),
			&cancel,
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
		assert_eq!(motion_payloads(&mock), [b"on".to_vec()]);
		assert_eq!(h.wake_lock().count(), 1);
		tokio::time::sleep(Duration::from_secs(5)).await;
		assert_eq!(motion_payloads(&mock), [b"on".to_vec(), b"off".to_vec()]);
		assert_eq!(h.status_cache().motion(), Some(false));
		assert_eq!(h.wake_lock().count(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn motion_edge_newest_edge_owns_the_clear() {
		let h = handle();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let hold = Duration::from_secs(6);
		motion_edge(Arc::clone(&h), Some(&mqtt), "bairelay", hold, &cancel);
		tokio::time::sleep(Duration::from_secs(4)).await;
		motion_edge(Arc::clone(&h), Some(&mqtt), "bairelay", hold, &cancel);
		tokio::time::sleep(Duration::from_secs(4)).await;
		assert_eq!(motion_payloads(&mock), [b"on".to_vec(), b"on".to_vec()]);
		tokio::time::sleep(Duration::from_secs(4)).await;
		assert_eq!(
			motion_payloads(&mock),
			[b"on".to_vec(), b"on".to_vec(), b"off".to_vec()]
		);
	}

	#[tokio::test(start_paused = true)]
	async fn push_edge_inside_a_webhook_wake_is_absorbed() {
		let h = handle();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		assert!(claim_wake(&h, WakeSource::Webhook));
		let hold = Duration::from_secs(5);
		assert!(!push_edge(
			Arc::clone(&h),
			Some(&mqtt),
			"bairelay",
			hold,
			&cancel
		));
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert!(mock.published().is_empty());
		assert_eq!(h.wake_lock().count(), 0);
		assert_eq!(h.push_seq().load(Ordering::SeqCst), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn push_edges_alone_are_never_folded() {
		let h = handle();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let hold = Duration::from_secs(6);
		assert!(push_edge(
			Arc::clone(&h),
			Some(&mqtt),
			"bairelay",
			hold,
			&cancel
		));
		tokio::time::sleep(Duration::from_secs(4)).await;
		assert!(push_edge(
			Arc::clone(&h),
			Some(&mqtt),
			"bairelay",
			hold,
			&cancel
		));
		tokio::time::sleep(Duration::from_secs(4)).await;
		assert_eq!(motion_payloads(&mock), [b"on".to_vec(), b"on".to_vec()]);
		assert_eq!(h.wake_lock().count(), 1, "the later edge extends the hold");
		tokio::time::sleep(Duration::from_secs(4)).await;
		assert_eq!(
			motion_payloads(&mock),
			[b"on".to_vec(), b"on".to_vec(), b"off".to_vec()]
		);
	}

	#[tokio::test(start_paused = true)]
	async fn motion_edge_respects_enable_motion() {
		let mut cfg = minimal_camera_config("cam1");
		cfg.mqtt.enable_motion = false;
		let h = Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None));
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		motion_edge(
			Arc::clone(&h),
			Some(&mqtt),
			"bairelay",
			Duration::from_secs(5),
			&cancel,
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
		assert_eq!(h.wake_lock().count(), 1, "the wake hold still happens");
		tokio::time::sleep(Duration::from_secs(5)).await;
		assert!(mock.published().is_empty());
		assert_eq!(h.wake_lock().count(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn motion_edge_without_mqtt_holds_the_wake_lock_only() {
		let h = handle();
		let cancel = CancellationToken::new();
		motion_edge(
			Arc::clone(&h),
			None,
			"bairelay",
			Duration::from_secs(5),
			&cancel,
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
		assert_eq!(h.wake_lock().count(), 1);
		assert_eq!(h.push_seq().load(Ordering::SeqCst), 1);
		tokio::time::sleep(Duration::from_secs(5)).await;
		assert_eq!(h.wake_lock().count(), 0);
	}
}
