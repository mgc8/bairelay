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
	cam.status_cache()
		.set_motion_classes(bairelay_neolink_core::bc_protocol::AiDetections {
			vehicle: true,
			..Default::default()
		});
	cam.status_cache().set_push(true);
	cam.status_cache().set_floodlight(false);
	cam.status_cache().set_floodlight_tasks(true);
	cam.status_cache().set_pir(false);
	cam.status_cache()
		.set_ir(bairelay_mqtt::control::IrMode::Auto);
	cam.status_cache().set_led(true);

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
	for (class, payload) in [
		("person", "off"),
		("vehicle", "on"),
		("animal", "off"),
		("other", "off"),
	] {
		let topic = format!("bairelay/cam1/status/motion/{class}");
		assert!(
			saw(&topic, payload.as_bytes()),
			"{topic} retained must land; got {:?}",
			mock.published_topics()
		);
	}
	assert!(
		saw("bairelay/cam1/status/push", b"on"),
		"push=on retained must land"
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
