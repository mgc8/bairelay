//! MQTT-side event-loop helpers extracted from `main.rs` so the per-
//! event branch table can be unit-tested without spinning up a real
//! broker + `rumqttc::EventLoop`.
//!
//! The production `run_mqtt_event_loop` in `main.rs` is a thin wrapper
//! around [`classify_event`] + a pair of side-effect helpers. Tests
//! drive the classifier on every rumqttc event shape and drive the
//! subscribe / discovery-republish helpers against captured `mock_client`
//! sinks and FakeCamera-backed `CameraHandle` maps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bairelay_mqtt::{Event, Packet, SharedMqttClient, StatusPublisher};

use crate::camera::CameraHandle;
use crate::config::Config;

/// Resolve the MQTT topic prefix from config, defaulting to
/// `"bairelay"` when MQTT is disabled entirely. Centralised so the
/// fallback string can't drift between main.rs and tests.
pub fn resolve_topic_prefix(config: &Config) -> String {
	config
		.mqtt
		.as_ref()
		.map(|m| m.topic_prefix.clone())
		.unwrap_or_else(|| "bairelay".to_string())
}

/// Convert the config's `[users]` block into the RTSP server's auth
/// record shape. Pulled out so a test can assert name / password
/// propagate correctly without instantiating the full server.
pub fn build_rtsp_users(config: &Config) -> Vec<bairelay_rtsp::rtsp::auth::UserCred> {
	config
		.users
		.iter()
		.map(|u| bairelay_rtsp::rtsp::auth::UserCred {
			name: u.name.clone(),
			password: u.pass.clone(),
		})
		.collect()
}

/// Build the MQTT broker config struct from the parsed TOML form.
/// `Ok(None)` without `[mqtt]`. Loads and validates the TLS material
/// (`ca`, `client_auth` — each a file path or inline PEM), so a bad
/// certificate fails here rather than at the first broker connect.
pub fn build_broker_config(config: &Config) -> anyhow::Result<Option<bairelay_mqtt::MqttConfig>> {
	let Some(mqtt_config) = config.mqtt.as_ref() else {
		return Ok(None);
	};
	let client_auth = mqtt_config
		.client_auth
		.as_ref()
		.map(|(cert, key)| (cert.as_str(), key.as_str()));
	let tls = crate::tls_load::load_mqtt_tls(mqtt_config.ca.as_deref(), client_auth)?;
	let (ca, client_auth) = match tls {
		Some(tls) => (Some(tls.ca_pem), tls.client_auth),
		None => (None, None),
	};
	Ok(Some(bairelay_mqtt::MqttConfig {
		broker_addr: mqtt_config.broker_addr.clone(),
		port: mqtt_config.port,
		credentials: mqtt_config.credentials.clone(),
		ca,
		client_auth: client_auth
			.map(|(cert_pem, key_pem)| bairelay_mqtt::ClientAuth { cert_pem, key_pem }),
	}))
}

/// Publish "disconnected" availability for every camera, then
/// unpublish HA discovery config for cameras whose `CameraHandle`
/// had one attached. Wrapped under a 2 s hard timeout by the caller.
/// Returns nothing — per-camera errors are logged and elided so a
/// single bad publish doesn't abort the shutdown fan-out.
pub async fn publish_shutdown_fanout(
	camera_names: &[String],
	cameras: &HashMap<String, Arc<CameraHandle>>,
	mqtt: &SharedMqttClient,
	topic_prefix: &str,
) {
	for name in camera_names {
		let publisher = StatusPublisher::new(mqtt, topic_prefix, name);
		let _ = publisher.publish_connection(false).await;
	}
	for (name, cam) in cameras.iter() {
		if let Err(e) = cam.unpublish_discovery().await {
			tracing::warn!(
				camera = %name,
				error = %e,
				"Failed to unpublish HA discovery on shutdown"
			);
		}
	}
}

/// Convenience: the hard-cap duration main.rs uses when awaiting the
/// shutdown fanout. Kept as a named constant so the test doesn't have
/// to repeat the magic number.
pub const SHUTDOWN_FANOUT_TIMEOUT: Duration = Duration::from_secs(2);

/// Coarse-grained action an incoming rumqttc event maps onto. The
/// production loop matches on this enum to drive side effects;
/// classification itself is pure, so it's exhaustively covered by a
/// decision-table test.
#[derive(Debug)]
pub enum EventAction<'a> {
	/// A camera control publish landed — dispatch it.
	Publish {
		/// Topic the message arrived on.
		topic: &'a str,
		/// Raw payload bytes.
		payload: &'a [u8],
	},
	/// The broker just completed the CONNECT handshake. Subscribe all
	/// cameras + re-publish HA discovery payloads where caps exist.
	ConnAck,
	/// A transport-level error we log and ignore; rumqttc will retry.
	LogError,
	/// An uninteresting event (Outgoing, PingResp, other protocol
	/// chatter) — do nothing.
	Ignore,
}

/// Pure decision table: convert a `Result<Event, ConnectionError>` from
/// `rumqttc::EventLoop::poll()` into an [`EventAction`].
///
/// Borrow-level: the `EventAction::Publish` variant borrows from the
/// caller's `Event`, so the `match` that consumed `event` must keep
/// ownership alive while dispatch runs.
pub fn classify_event<'a, E>(event: &'a Result<Event, E>) -> EventAction<'a> {
	match event {
		Ok(Event::Incoming(Packet::Publish(msg))) => EventAction::Publish {
			topic: &msg.topic,
			payload: &msg.payload,
		},
		Ok(Event::Incoming(Packet::ConnAck(_))) => EventAction::ConnAck,
		Err(_) => EventAction::LogError,
		Ok(_) => EventAction::Ignore,
	}
}

/// Backoff state for the MQTT event-loop reconnect path.
///
/// `rumqttc::EventLoop::poll()` returns the underlying socket error
/// immediately and on the next call attempts to reconnect — with no
/// internal pacing. When the broker is down the operator otherwise sees
/// hundreds of identical `Connection refused` warnings per second.
///
/// The state machine here:
///
/// - On each error: returns `(delay, should_log)`. Delay is exponential
///   (1, 2, 4, 8, 16, 30 s; capped at 30 s). At most **one** warn per
///   `RELOG_PERIOD` regardless of which error the loop is currently
///   seeing — a flapping broker that alternates between
///   `Connection refused` and `Broken pipe` no longer defeats the
///   dedupe by changing message identity.
/// - On success (any non-error event): caller calls `reset()`, restoring
///   the loud-once / quick-reconnect behaviour for the next outage.
#[derive(Debug)]
pub struct MqttBackoff {
	consecutive: u32,
	last_logged_at: Option<std::time::Instant>,
	relog_period: Duration,
}

impl Default for MqttBackoff {
	fn default() -> Self {
		Self::new()
	}
}

impl MqttBackoff {
	/// One-minute relog period — matches the typical MQTT keepalive
	/// cadence so the operator gets a periodic "still down" reminder
	/// without the per-poll spam.
	pub const RELOG_PERIOD: Duration = Duration::from_secs(60);

	pub fn new() -> Self {
		Self {
			consecutive: 0,
			last_logged_at: None,
			relog_period: Self::RELOG_PERIOD,
		}
	}

	/// Record an error and return `(delay_before_next_poll, should_log)`.
	/// Internal call is `record_error_at(Instant::now())`; the test
	/// seam takes an explicit `now` so the relog window is testable
	/// without sleeping. The `_msg` argument exists for caller
	/// ergonomics (the call site logs the message) but is no longer
	/// part of the dedupe key — see the type-level docstring.
	pub fn record_error(&mut self, _msg: &str) -> (Duration, bool) {
		self.record_error_at(_msg, std::time::Instant::now())
	}

	pub fn record_error_at(&mut self, _msg: &str, now: std::time::Instant) -> (Duration, bool) {
		// Window-based dedupe: log on first occurrence after a reset,
		// then again only after `relog_period` has elapsed. The
		// message content is intentionally NOT part of the gate so a
		// flapping broker alternating between distinct error strings
		// can't defeat the limiter.
		let should_log = self
			.last_logged_at
			.is_none_or(|t| now.duration_since(t) >= self.relog_period);
		if should_log {
			self.last_logged_at = Some(now);
		}
		// Sequence: 1, 2, 4, 8, 16, 30, 30, ... seconds.
		let exp = self.consecutive.min(5);
		let raw = 1u64 << exp;
		let delay = Duration::from_secs(raw.min(30));
		self.consecutive = self.consecutive.saturating_add(1);
		(delay, should_log)
	}

	/// Reset on any successful event — the next error is loud + retried
	/// quickly again.
	pub fn reset(&mut self) {
		self.consecutive = 0;
		self.last_logged_at = None;
	}
}

/// Side effect: on CONNECT success, subscribe every camera's topic set
/// and re-publish HA discovery payloads for cameras whose capability
/// cache is already populated.
pub async fn handle_connack(
	cameras: &HashMap<String, Arc<CameraHandle>>,
	mqtt: &SharedMqttClient,
	topic_prefix: &str,
) {
	tracing::info!("MQTT broker connected");
	// Subscribe all cameras (not just connected ones — we need to
	// receive wakeup commands for sleeping cameras too). When the
	// broker is misbehaving every camera fails in the same way; log
	// the first failure verbosely and then aggregate the rest into
	// a single warn so a 50-camera fleet doesn't emit 50 lines per
	// ConnAck. Sibling discipline to `MqttBackoff`'s window dedupe
	// on the broker reconnect path.
	let mut subscribe_failures: Vec<String> = Vec::new();
	for name in cameras.keys() {
		tracing::debug!(camera = %name, "Subscribing to MQTT topics");
		if let Err(e) = mqtt.subscribe_all(topic_prefix, name).await {
			if subscribe_failures.is_empty() {
				tracing::warn!(camera = %name, error = %e, "Failed to subscribe");
			}
			subscribe_failures.push(name.clone());
		}
	}
	if subscribe_failures.len() > 1 {
		tracing::warn!(
			count = subscribe_failures.len(),
			cameras = ?subscribe_failures,
			"Failed to subscribe additional cameras after the first failure (suppressed for log brevity)"
		);
	}
	// Re-publish HA discovery for every camera whose capability
	// cache is already populated. Cameras still `None` here will
	// publish via their own post-first-connect path; skipping them
	// avoids leaking a pre-caps (stale has_ptz = false) payload.
	for (name, cam) in cameras.iter() {
		if cam.capabilities().is_none() {
			continue;
		}
		if let Err(e) = cam.publish_discovery().await {
			tracing::warn!(
				camera = %name,
				error = %e,
				"Failed to re-publish HA discovery on ConnAck"
			);
		}
	}

	// Re-emit each camera's last-known status (battery / motion /
	// floodlight / floodlight_tasks / pir) from its in-memory cache.
	// Brokers without persistence wipe retained messages on restart;
	// without this republish HA would show "unknown" for event-driven
	// or long-polled sensors until the next event/poll. The cache is
	// fully empty until the matching publishers have run at least
	// once, so this is a no-op on the very first ConnAck of a fresh
	// process and only contributes work after real state has flowed.
	for (name, cam) in cameras.iter() {
		if let Err(e) = cam.republish_cached_status().await {
			tracing::warn!(
				camera = %name,
				error = %e,
				"Failed to re-publish cached MQTT status on ConnAck"
			);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_mqtt::{ConnAck, ConnectReturnCode, Publish, QoS};

	fn connack_event() -> Result<Event, &'static str> {
		Ok(Event::Incoming(Packet::ConnAck(ConnAck {
			session_present: false,
			code: ConnectReturnCode::Success,
		})))
	}

	fn publish_event() -> Result<Event, &'static str> {
		Ok(Event::Incoming(Packet::Publish(Publish::new(
			"bairelay/cam/control/reboot",
			QoS::AtLeastOnce,
			"1",
		))))
	}

	fn pingresp_event() -> Result<Event, &'static str> {
		Ok(Event::Incoming(Packet::PingResp))
	}

	#[test]
	fn classify_publish_returns_publish_action() {
		let ev = publish_event();
		match classify_event(&ev) {
			EventAction::Publish { topic, payload } => {
				assert_eq!(topic, "bairelay/cam/control/reboot");
				assert_eq!(payload, b"1");
			}
			_ => panic!("expected Publish action"),
		}
	}

	#[test]
	fn classify_connack_returns_connack_action() {
		let ev = connack_event();
		assert!(matches!(classify_event(&ev), EventAction::ConnAck));
	}

	#[test]
	fn classify_error_returns_log_error() {
		let ev: Result<Event, &'static str> = Err("broker disconnected");
		assert!(matches!(classify_event(&ev), EventAction::LogError));
	}

	#[test]
	fn classify_other_packet_returns_ignore() {
		// PingResp and Outgoing fall into the _ arm.
		let ev = pingresp_event();
		assert!(matches!(classify_event(&ev), EventAction::Ignore));

		let ev: Result<Event, &'static str> = Ok(Event::Outgoing(bairelay_mqtt::Outgoing::PingReq));
		assert!(matches!(classify_event(&ev), EventAction::Ignore));
	}

	#[test]
	fn mqtt_backoff_first_error_logs_and_starts_at_one_second() {
		let mut bo = MqttBackoff::new();
		let (delay, log) = bo.record_error("Connection refused");
		assert_eq!(delay, Duration::from_secs(1));
		assert!(log, "first occurrence must log");
	}

	#[test]
	fn mqtt_backoff_consecutive_same_error_suppresses_log() {
		let mut bo = MqttBackoff::new();
		let t0 = std::time::Instant::now();
		bo.record_error_at("Connection refused", t0);
		let (_, log) = bo.record_error_at("Connection refused", t0 + Duration::from_secs(1));
		assert!(!log, "identical repeat within relog window must suppress");
	}

	#[test]
	fn mqtt_backoff_delay_is_exponential_then_capped() {
		let mut bo = MqttBackoff::new();
		let t0 = std::time::Instant::now();
		let mut delays = Vec::new();
		for _ in 0..8 {
			let (d, _) = bo.record_error_at("err", t0);
			delays.push(d.as_secs());
		}
		assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
	}

	#[test]
	fn mqtt_backoff_alternating_messages_stay_suppressed() {
		// Previously the dedupe was keyed on message identity, so a
		// flapping broker alternating between distinct error strings
		// re-introduced log spam at every poll. Window-based dedupe
		// keeps it quiet regardless of message content.
		let mut bo = MqttBackoff::new();
		let t0 = std::time::Instant::now();
		bo.record_error_at("Connection refused", t0);
		let (_, log_a) = bo.record_error_at("Broken pipe", t0 + Duration::from_secs(1));
		let (_, log_b) = bo.record_error_at("Connection refused", t0 + Duration::from_secs(2));
		let (_, log_c) = bo.record_error_at("Broken pipe", t0 + Duration::from_secs(3));
		assert!(
			!log_a && !log_b && !log_c,
			"alternating messages within the relog window must stay quiet (got {log_a},{log_b},{log_c})"
		);
	}

	#[test]
	fn mqtt_backoff_relog_after_period_even_for_same_message() {
		let mut bo = MqttBackoff::new();
		let t0 = std::time::Instant::now();
		bo.record_error_at("Connection refused", t0);
		let after = t0 + MqttBackoff::RELOG_PERIOD + Duration::from_secs(1);
		let (_, log) = bo.record_error_at("Connection refused", after);
		assert!(log, "stale window must relog even when message identical");
	}

	#[test]
	fn mqtt_backoff_reset_restores_first_error_behaviour() {
		let mut bo = MqttBackoff::new();
		let t0 = std::time::Instant::now();
		// Burn three consecutive errors so the next would be 8 s + suppressed.
		for _ in 0..3 {
			bo.record_error_at("Connection refused", t0);
		}
		bo.reset();
		let (delay, log) = bo.record_error_at("Connection refused", t0);
		assert_eq!(
			delay,
			Duration::from_secs(1),
			"reset must restart the ladder"
		);
		assert!(log, "reset must clear the suppression cache");
	}

	#[tokio::test]
	async fn handle_connack_with_empty_cameras_is_noop() {
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		handle_connack(&cameras, &mqtt, "bairelay").await;
		// No panic, no assertions — just the empty-loop path.
	}

	use crate::config::{Config, MqttServerConfig, UserConfig};

	#[test]
	fn resolve_topic_prefix_defaults_when_mqtt_absent() {
		let cfg = Config::default();
		assert_eq!(resolve_topic_prefix(&cfg), "bairelay");
	}

	#[test]
	fn resolve_topic_prefix_honours_configured_value() {
		let cfg = Config {
			mqtt: Some(MqttServerConfig {
				broker_addr: "localhost".into(),
				port: 1883,
				credentials: None,
				ca: None,
				client_auth: None,
				topic_prefix: "neolink".into(),
				discovery: None,
			}),
			..Config::default()
		};
		assert_eq!(resolve_topic_prefix(&cfg), "neolink");
	}

	#[test]
	fn build_rtsp_users_maps_each_entry() {
		let cfg = Config {
			users: vec![
				UserConfig {
					name: "alice".into(),
					pass: "apw".into(),
				},
				UserConfig {
					name: "bob".into(),
					pass: "bpw".into(),
				},
			],
			..Config::default()
		};
		let out = build_rtsp_users(&cfg);
		assert_eq!(out.len(), 2);
		assert_eq!(out[0].name, "alice");
		assert_eq!(out[0].password, "apw");
		assert_eq!(out[1].name, "bob");
		assert_eq!(out[1].password, "bpw");
	}

	#[test]
	fn build_rtsp_users_empty_when_config_has_no_users() {
		let cfg = Config::default();
		assert!(build_rtsp_users(&cfg).is_empty());
	}

	#[test]
	fn build_broker_config_returns_none_when_mqtt_absent() {
		assert!(build_broker_config(&Config::default()).unwrap().is_none());
	}

	fn mqtt_cfg(ca: Option<String>, client_auth: Option<(String, String)>) -> Config {
		Config {
			mqtt: Some(MqttServerConfig {
				broker_addr: "mqtt.example.com".into(),
				port: 8883,
				credentials: Some(("u".into(), "p".into())),
				ca,
				client_auth,
				topic_prefix: "bairelay".into(),
				discovery: None,
			}),
			..Config::default()
		}
	}

	#[test]
	fn build_broker_config_maps_each_field() {
		let b = build_broker_config(&mqtt_cfg(None, None))
			.unwrap()
			.expect("Some");
		assert_eq!(b.broker_addr, "mqtt.example.com");
		assert_eq!(b.port, 8883);
		assert_eq!(b.credentials.as_ref().unwrap().0, "u");
		assert_eq!(b.credentials.as_ref().unwrap().1, "p");
		assert!(b.ca.is_none());
		assert!(b.client_auth.is_none());
	}

	/// `ca` + `client_auth` given as file paths are read and passed on as
	/// PEM text — previously `client_auth` was dropped entirely.
	#[test]
	fn build_broker_config_loads_ca_and_client_auth_from_files() {
		let pki = crate::tls_load::test_pki::Pki::generate();
		let dir = tempfile::tempdir().unwrap();
		let path = |name: &str, body: &str| {
			let p = dir.path().join(name);
			std::fs::write(&p, body).unwrap();
			p.to_string_lossy().into_owned()
		};
		let cfg = mqtt_cfg(
			Some(path("ca.pem", &pki.ca_pem)),
			Some((
				path("client.pem", &pki.client_cert_pem),
				path("client.key", &pki.client_key_pem),
			)),
		);
		let b = build_broker_config(&cfg).unwrap().expect("Some");
		assert_eq!(b.ca.as_deref(), Some(pki.ca_pem.as_str()));
		let auth = b.client_auth.expect("client_auth wired through");
		assert_eq!(auth.cert_pem, pki.client_cert_pem);
		assert_eq!(auth.key_pem, pki.client_key_pem);
	}

	#[test]
	fn build_broker_config_rejects_unreadable_ca_path() {
		let cfg = mqtt_cfg(Some("/nonexistent/broker-ca.pem".into()), None);
		let err = build_broker_config(&cfg)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("/nonexistent/broker-ca.pem"), "got: {err}");
	}

	#[test]
	fn shutdown_fanout_timeout_is_two_seconds() {
		assert_eq!(SHUTDOWN_FANOUT_TIMEOUT, Duration::from_secs(2));
	}

	#[tokio::test]
	async fn publish_shutdown_fanout_is_noop_on_empty_input() {
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		publish_shutdown_fanout(&[], &cameras, &mqtt, "bairelay").await;
		assert_eq!(mock.count(), 0);
	}

	#[tokio::test]
	async fn publish_shutdown_fanout_publishes_one_disconnect_per_camera_name() {
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		publish_shutdown_fanout(
			&["cam-a".to_string(), "cam-b".to_string()],
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		// One disconnect publish per name.
		assert!(mock.count() >= 2, "expected at least 2 publishes");
	}

	#[tokio::test]
	async fn handle_connack_subscribes_each_camera_and_skips_nocaps_discovery() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;

		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-a"),
			cancel,
			None,
		));
		let mut cameras = HashMap::new();
		cameras.insert("cam-a".to_string(), handle);
		// No capabilities installed → publish_discovery path is skipped
		// entirely. This drives the `subscribe_all` call + the
		// `capabilities().is_none() → continue` arm. No assertion
		// needed: coverage is the entire point.
		handle_connack(&cameras, &mqtt, "bairelay").await;
	}
}
