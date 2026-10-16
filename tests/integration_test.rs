use tokio_util::sync::CancellationToken;

use bairelay::config::{parse_config, validate_config};
use bairelay::orchestrator::Orchestrator;

#[tokio::test]
async fn full_lifecycle_smoke_test() {
	// 1. Parse config from string.
	let toml_str = r#"
		[[cameras]]
		name = "test_cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
		idle_disconnect = true
	"#;
	let config = parse_config(toml_str).unwrap();
	validate_config(&config).unwrap();

	// 2. Create orchestrator.
	let cancel = CancellationToken::new();
	let orch = Orchestrator::new(config, cancel.clone(), None);
	assert_eq!(orch.camera_count(), 1);

	// 3. Access camera and verify initial state.
	let cam = orch.get_camera("test_cam").unwrap();
	assert!(cam.state().is_disconnected());
	assert!(cam.wake_lock().is_idle());

	// 4. Acquire and release wake lock.
	let guard = cam.wake_lock().acquire();
	assert!(!cam.wake_lock().is_idle());
	assert_eq!(cam.wake_lock().count(), 1);
	drop(guard);
	assert!(cam.wake_lock().is_idle());

	// 5. Clean shutdown.
	cancel.cancel();
}

#[test]
fn mqtt_control_round_trip() {
	use bairelay_mqtt::control::parse_control_message;
	use bairelay_mqtt::topics;

	let prefix = "bairelay";
	let topic = topics::control_floodlight(prefix, "garden");
	let cmd = parse_control_message(prefix, &topic, b"on");
	assert!(cmd.is_some());
}

// ── Camera handle initial state ──────────────────────────────────────

#[tokio::test]
async fn camera_handle_starts_with_no_bc_camera() {
	let config = bairelay::config::test_helpers::minimal_camera_config("test");
	let cancel = CancellationToken::new();
	let cam = std::sync::Arc::new(bairelay::camera::CameraHandle::new(
		config,
		cancel.clone(),
		None,
	));
	assert!(
		cam.bc_camera().is_none(),
		"bc_camera() should be None before connecting"
	);
	assert!(
		cam.state().is_disconnected(),
		"initial state should be Disconnected"
	);
	cancel.cancel();
}

#[tokio::test]
async fn camera_handle_name_matches_config() {
	let config = bairelay::config::test_helpers::minimal_camera_config("my-cam");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None);
	assert_eq!(cam.name(), "my-cam");
	cancel.cancel();
}

#[tokio::test]
async fn camera_handle_wake_lock_starts_idle() {
	let config = bairelay::config::test_helpers::minimal_camera_config("idle-cam");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None);
	assert!(cam.wake_lock().is_idle());
	assert_eq!(cam.wake_lock().count(), 0);
	cancel.cancel();
}

// ── Camera state enum helpers ────────────────────────────────────────

#[test]
fn camera_state_enum_helpers() {
	use bairelay::camera::CameraState;

	let disconnected = CameraState::Disconnected;
	assert!(disconnected.is_disconnected());
	assert!(!disconnected.is_connecting());
	assert!(!disconnected.is_connected());

	let connecting = CameraState::Connecting;
	assert!(!connecting.is_disconnected());
	assert!(connecting.is_connecting());
	assert!(!connecting.is_connected());

	let connected = CameraState::Connected;
	assert!(!connected.is_disconnected());
	assert!(!connected.is_connecting());
	assert!(connected.is_connected());
}

// ── Wakeup boundary tests ────────────────────────────────────────────

#[test]
fn wakeup_cap_rejected_above_1440() {
	let cmd = bairelay_mqtt::control::parse_control_message(
		"bairelay",
		"bairelay/cam/control/wakeup",
		b"1441",
	);
	assert!(cmd.is_none(), "1441 minutes should be rejected");
}

#[test]
fn wakeup_max_accepted() {
	let cmd = bairelay_mqtt::control::parse_control_message(
		"bairelay",
		"bairelay/cam/control/wakeup",
		b"1440",
	);
	assert!(cmd.is_some(), "1440 minutes should be accepted");
}

#[test]
fn wakeup_zero_rejected() {
	let cmd = bairelay_mqtt::control::parse_control_message(
		"bairelay",
		"bairelay/cam/control/wakeup",
		b"0",
	);
	assert!(cmd.is_none(), "0 minutes should be rejected");
}

#[test]
fn wakeup_one_accepted() {
	let cmd = bairelay_mqtt::control::parse_control_message(
		"bairelay",
		"bairelay/cam/control/wakeup",
		b"1",
	);
	assert!(cmd.is_some(), "1 minute should be accepted");
}

// ── HA discovery publish/unpublish plumbing ──────────────────────────
//
// These tests exercise the binary-level wiring between
// `CameraHandle::publish_discovery` / `unpublish_discovery`, the
// capability cache, and the optional `DiscoveryPublisher`. The actual
// payload content is covered by the pure-function tests in
// `crates/mqtt/src/discovery/`. Here we check the early-return
// guards that stop `publish`/`unpublish` from generating MQTT
// traffic under the wrong conditions.

fn dummy_discovery_publisher() -> bairelay_mqtt::DiscoveryPublisher {
	// The `compute_payloads` path exercised by `publish`/`unpublish`
	// is pure — the stub client's event loop is never driven.
	let shared = bairelay_mqtt::SharedMqttClient::for_test_stub("integration-test");
	bairelay_mqtt::DiscoveryPublisher::new(
		shared,
		"bairelay".to_string(),
		"homeassistant".to_string(),
		bairelay_mqtt::discovery::Feature::ALL
			.iter()
			.copied()
			.collect(),
		"0.0.0-test".to_string(),
	)
}

#[tokio::test]
async fn publish_discovery_without_publisher_is_a_noop() {
	// No `.with_discovery_publisher(...)` attached → early-return
	// `Ok(())` regardless of capability cache state. Confirms the
	// binary gracefully degrades when `[mqtt.discovery]` is absent.
	let config = bairelay::config::test_helpers::minimal_camera_config("no-discovery");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None);
	assert!(cam.publish_discovery().await.is_ok());
	assert!(cam.unpublish_discovery().await.is_ok());
	cancel.cancel();
}

#[tokio::test]
async fn publish_discovery_without_capabilities_is_a_noop() {
	// Publisher attached but capability cache is still `None` — the
	// post-first-connect path has not run yet. Must early-return;
	// emitting here would leak `has_ptz = false` onto retained
	// topics before we know better.
	let config = bairelay::config::test_helpers::minimal_camera_config("caps-unknown");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None)
		.with_discovery_publisher(dummy_discovery_publisher());
	assert!(cam.capabilities().is_none());
	assert!(cam.publish_discovery().await.is_ok());
	assert!(cam.unpublish_discovery().await.is_ok());
	cancel.cancel();
}

#[tokio::test]
async fn publish_discovery_with_capabilities_emits_ok() {
	// Publisher + populated capability cache → publish should
	// succeed end-to-end. The stub broker is unreachable; rumqttc
	// buffers internally, so the QoS-1 publishes queue up without
	// blocking. Failure here would indicate the wiring (enable
	// flags, caps view conversion, ctx construction) is broken.
	use bairelay::capabilities::CameraCapabilities;

	let config = bairelay::config::test_helpers::minimal_camera_config("caps-set");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None)
		.with_discovery_publisher(dummy_discovery_publisher());
	cam.set_capabilities_for_test(CameraCapabilities {
		has_ptz: true,
		status_led: bairelay_mqtt::discovery::Presence::Present,
		ir_leds: bairelay_mqtt::discovery::Presence::Present,
		..Default::default()
	});
	assert!(cam.capabilities().is_some());
	if let Err(e) = cam.publish_discovery().await {
		panic!("publish_discovery failed: {e}");
	}
	if let Err(e) = cam.unpublish_discovery().await {
		panic!("unpublish_discovery failed: {e}");
	}
	cancel.cancel();
}

#[tokio::test]
async fn unpublish_discovery_without_prior_publish_is_ok() {
	// Simulates the "shutdown first, never successfully published"
	// race: capability cache populated post-connect but the publish
	// call itself never landed (e.g. broker was unreachable at that
	// instant). Unpublish on shutdown must still not error — it
	// re-emits retained-empty on the same topic set, which is a
	// safe no-op for HA either way.
	use bairelay::capabilities::CameraCapabilities;

	let config = bairelay::config::test_helpers::minimal_camera_config("shutdown-path");
	let cancel = CancellationToken::new();
	let cam = bairelay::camera::CameraHandle::new(config, cancel.clone(), None)
		.with_discovery_publisher(dummy_discovery_publisher());
	cam.set_capabilities_for_test(CameraCapabilities {
		has_ptz: false,
		status_led: bairelay_mqtt::discovery::Presence::Present,
		ir_leds: bairelay_mqtt::discovery::Presence::Present,
		..Default::default()
	});
	if let Err(e) = cam.unpublish_discovery().await {
		panic!("unpublish_discovery failed: {e}");
	}
	cancel.cancel();
}
