use std::time::Duration;

#[test]
fn backoff_increases_exponentially() {
	let mut backoff =
		bairelay::camera::ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
	assert_eq!(backoff.next_delay(), Duration::from_secs(1));
	assert_eq!(backoff.next_delay(), Duration::from_secs(2));
	assert_eq!(backoff.next_delay(), Duration::from_secs(4));
	assert_eq!(backoff.next_delay(), Duration::from_secs(8));
}

#[test]
fn backoff_caps_at_max() {
	let mut backoff =
		bairelay::camera::ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(5));
	for _ in 0..10 {
		backoff.next_delay();
	}
	assert_eq!(backoff.next_delay(), Duration::from_secs(5));
}

#[test]
fn backoff_resets() {
	let mut backoff =
		bairelay::camera::ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
	backoff.next_delay();
	backoff.next_delay();
	backoff.reset();
	assert_eq!(backoff.next_delay(), Duration::from_secs(1));
}

// ── republish_cached_status ─────────────────────────────────────────

/// On a broker reconnect, every cached status value flows back out via
/// retained publishes so HA recovers full state even when the broker
/// has lost retained messages. Empty cache → no publishes.
#[tokio::test]
async fn republish_cached_status_emits_each_set_field_as_retained() {
	use bairelay::camera::CameraHandle;
	use bairelay::config::test_helpers::minimal_camera_config;
	use tokio_util::sync::CancellationToken;

	let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
	let cancel = CancellationToken::new();
	let cam = CameraHandle::new(minimal_camera_config("cam1"), cancel, Some(mqtt));

	cam.status_cache().set_battery_level(73);
	cam.status_cache().set_motion(true);
	cam.status_cache().set_floodlight(false);
	cam.status_cache().set_floodlight_tasks(true);
	cam.status_cache().set_pir(false);
	cam.status_cache()
		.set_ir(bairelay_mqtt::control::IrMode::Auto);
	cam.status_cache().set_led(true);
	cam.status_cache().set_ai_objects(
		[bairelay_neolink_core::bc_protocol::AiObject::Vehicle]
			.into_iter()
			.collect(),
	);

	cam.republish_cached_status()
		.await
		.expect("republish must succeed");

	let pubs = mock.published();
	let saw = |topic: &str, payload: &[u8]| {
		pubs.iter()
			.any(|(t, p, r)| t == topic && p == payload && *r)
	};

	assert!(
		saw("bairelay/cam1/status/battery_level", b"73"),
		"battery_level=73 retained must land; got {:?}",
		mock.published_topics()
	);
	assert!(
		saw("bairelay/cam1/status/motion", b"on"),
		"motion=on retained must land"
	);
	assert!(
		saw("bairelay/cam1/status/floodlight", br#"{"state":"off"}"#),
		"floodlight off retained must land"
	);
	assert!(
		saw("bairelay/cam1/status/floodlight_tasks", b"on"),
		"floodlight_tasks=on retained must land"
	);
	assert!(
		saw("bairelay/cam1/status/pir", b"off"),
		"pir=off retained must land"
	);
	assert!(
		saw("bairelay/cam1/status/ir", b"auto"),
		"ir=auto retained must land"
	);
	assert!(
		saw("bairelay/cam1/status/led", b"on"),
		"led=on retained must land"
	);
	for object in ["person", "vehicle", "animal", "face", "package"] {
		let payload: &[u8] = if object == "vehicle" { b"on" } else { b"off" };
		assert!(
			saw(&format!("bairelay/cam1/status/ai/{object}"), payload),
			"ai/{object} retained must land"
		);
	}
}

/// Replay skips a cached `off` for an AI object whose topic discovery
/// retracts (doorbell-only objects on a camera without a doorbell);
/// a cached `on` is evidence and replays.
#[tokio::test]
async fn republish_cached_status_skips_retracted_ai_off() {
	use bairelay::camera::CameraHandle;
	use bairelay::capabilities::{CameraCapabilities, Presence};
	use bairelay::config::test_helpers::minimal_camera_config;
	use bairelay_neolink_core::bc_protocol::AiObject;
	use tokio_util::sync::CancellationToken;

	let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
	let cancel = CancellationToken::new();
	let cam = CameraHandle::new(minimal_camera_config("cam1"), cancel, Some(mqtt));
	cam.set_capabilities_for_test(CameraCapabilities {
		ai: Presence::Present,
		doorbell: Presence::Absent,
		..Default::default()
	});
	cam.status_cache()
		.set_ai_objects([AiObject::Package].into_iter().collect());

	cam.republish_cached_status()
		.await
		.expect("republish must succeed");

	let ai: Vec<(String, Vec<u8>)> = mock
		.published()
		.into_iter()
		.filter(|(t, _, _)| t.starts_with("bairelay/cam1/status/ai/"))
		.map(|(t, p, _)| (t, p))
		.collect();
	let row = |object: &str, payload: &[u8]| {
		(
			format!("bairelay/cam1/status/ai/{object}"),
			payload.to_vec(),
		)
	};
	assert_eq!(
		ai,
		vec![
			row("person", b"off"),
			row("vehicle", b"off"),
			row("animal", b"off"),
			row("package", b"on"),
		]
	);
}

/// Replay reads the AI cache under the AI publish lock: a set the
/// motion listener caches while the replay waits (its session-start
/// reset) is the set the replay emits, never the stale one.
#[tokio::test(start_paused = true)]
async fn republish_cached_status_reads_ai_cache_under_publish_lock() {
	use bairelay::camera::CameraHandle;
	use bairelay::config::test_helpers::minimal_camera_config;
	use bairelay_neolink_core::bc_protocol::{AiObject, AiObjects};
	use std::sync::Arc;
	use tokio_util::sync::CancellationToken;

	let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
	let cam = Arc::new(CameraHandle::new(
		minimal_camera_config("cam1"),
		CancellationToken::new(),
		Some(mqtt),
	));
	let cache = cam.status_cache();
	cache.set_ai_objects([AiObject::Vehicle].into_iter().collect());

	let guard = cache.lock_ai_publish().await;
	let replay = tokio::spawn({
		let cam = Arc::clone(&cam);
		async move { cam.republish_cached_status().await }
	});
	tokio::time::sleep(Duration::from_millis(1)).await;
	assert!(!replay.is_finished(), "replay waits on the AI lock");
	cache.set_ai_objects(AiObjects::default());
	drop(guard);
	tokio::time::timeout(Duration::from_secs(1), replay)
		.await
		.expect("replay must finish once the lock is free")
		.unwrap()
		.expect("republish must succeed");

	let ai: Vec<(String, Vec<u8>)> = mock
		.published()
		.into_iter()
		.filter(|(t, _, _)| t.starts_with("bairelay/cam1/status/ai/"))
		.map(|(t, p, _)| (t, p))
		.collect();
	assert_eq!(ai.len(), 5, "{ai:?}");
	assert!(
		ai.iter().all(|(_, p)| p == b"off"),
		"stale on replayed: {ai:?}"
	);
}

#[tokio::test]
async fn republish_cached_status_is_noop_when_cache_empty() {
	use bairelay::camera::CameraHandle;
	use bairelay::config::test_helpers::minimal_camera_config;
	use tokio_util::sync::CancellationToken;

	let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
	let cancel = CancellationToken::new();
	let cam = CameraHandle::new(minimal_camera_config("cam1"), cancel, Some(mqtt));

	cam.republish_cached_status()
		.await
		.expect("empty-cache republish must succeed");

	assert!(
		mock.published().is_empty(),
		"empty cache must publish nothing; got {:?}",
		mock.published_topics()
	);
}

#[tokio::test]
async fn republish_cached_status_is_noop_without_mqtt_client() {
	use bairelay::camera::CameraHandle;
	use bairelay::config::test_helpers::minimal_camera_config;
	use tokio_util::sync::CancellationToken;

	let cancel = CancellationToken::new();
	let cam = CameraHandle::new(minimal_camera_config("cam1"), cancel, None);
	cam.status_cache().set_battery_level(50);

	// No mqtt_client → republish is a no-op; the call must succeed
	// without panicking even with cache data present.
	cam.republish_cached_status()
		.await
		.expect("no-mqtt republish must succeed");
}
