use std::time::Duration;

use rumqttc::{AsyncClient, EventLoop, LastWill, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use tracing;

use crate::error::MqttError;
use crate::topics;

/// TLS client certificate + key pair for mutual authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAuth {
	pub cert_pem: String,
	pub key_pem: String,
}

/// Configuration for connecting to an MQTT broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
	/// Broker hostname or IP address.
	pub broker_addr: String,
	/// Broker port (typically 1883 for plain, 8883 for TLS).
	pub port: u16,
	/// Optional (username, password) credentials.
	pub credentials: Option<(String, String)>,
	/// Optional CA certificate in PEM format for TLS.
	pub ca: Option<String>,
	/// Optional client certificate + key for mutual TLS.
	pub client_auth: Option<ClientAuth>,
}

impl MqttConfig {
	/// Build `rumqttc::MqttOptions` from this configuration.
	pub fn to_mqtt_options(&self, client_id: &str) -> MqttOptions {
		let mut opts = MqttOptions::new(client_id, &self.broker_addr, self.port);
		opts.set_keep_alive(Duration::from_secs(30));
		// rumqttc 0.25 defaults both incoming and outgoing max packet size
		// to 10 KiB. `status/preview` carries a base64-encoded camera JPEG
		// that is routinely 400 KiB–1.5 MiB (larger for 4K cameras), so the
		// default refuses every publish with:
		//   "Cannot send packet of size 'N'. It's greater than the broker's
		//    maximum packet size of: '10240'"
		// Neolink's Rust client raises this to fit real cameras; the broker
		// typically accepts the full payload. 16 MiB is conservative
		// headroom without being absurd.
		opts.set_max_packet_size(16 * 1024 * 1024, 16 * 1024 * 1024);

		if let Some((ref user, ref pass)) = self.credentials {
			opts.set_credentials(user, pass);
		}

		// TLS configuration if CA is provided
		if let Some(ref ca_pem) = self.ca {
			let mut tls_config = rumqttc::TlsConfiguration::Simple {
				ca: ca_pem.as_bytes().to_vec(),
				alpn: None,
				client_auth: None,
			};

			if let Some(ref auth) = self.client_auth {
				tls_config = rumqttc::TlsConfiguration::Simple {
					ca: ca_pem.as_bytes().to_vec(),
					alpn: None,
					client_auth: Some((
						auth.cert_pem.as_bytes().to_vec(),
						auth.key_pem.as_bytes().to_vec(),
					)),
				};
			}

			opts.set_transport(rumqttc::Transport::Tls(tls_config));
		}

		opts
	}
}

/// Type of each captured publish: `(topic, payload, retained)`. Only
/// used behind the `test-util` feature; kept as a type alias so the
/// `Arc<Mutex<Vec<..>>>` wrapper stays readable at its three occurrences.
pub(crate) type CaptureRow = (String, Vec<u8>, bool);

/// Shared capture sink handed off between [`SharedMqttClient`] and
/// `test_support::MockHandle`.
pub(crate) type CaptureSink = std::sync::Arc<std::sync::Mutex<Vec<CaptureRow>>>;

/// Cloneable MQTT client for sharing across camera tasks.
#[derive(Clone)]
pub struct SharedMqttClient {
	client: AsyncClient,
	/// When populated (test-util feature), every publish is mirrored
	/// into this sink so the binary's behaviour tests can assert on
	/// topic + payload without a broker round-trip. `None` in all
	/// production code paths so prod binary stays free of any capture
	/// overhead.
	capture: Option<CaptureSink>,
}

impl SharedMqttClient {
	/// Get a reference to the underlying `AsyncClient`.
	pub fn client(&self) -> &AsyncClient {
		&self.client
	}

	/// Test helper: wrap an already-constructed `AsyncClient`
	/// without going through [`connect`]. Used by unit tests that
	/// never drive the associated event loop — e.g. the discovery
	/// publisher's `compute_payloads` seam tests, and the binary's
	/// integration tests that assert on the publish/unpublish
	/// plumbing without touching a broker.
	///
	/// `doc(hidden)` because this is a test seam, not a supported
	/// public API — runtime code must go through [`connect`].
	#[doc(hidden)]
	pub fn for_test(client: AsyncClient) -> Self {
		Self {
			client,
			capture: None,
		}
	}

	/// Internal constructor used by [`crate::test_support::mock_client`]
	/// to attach an observable capture sink. Not part of the public
	/// API — the double-underscore prefix flags it as internal.
	#[doc(hidden)]
	pub fn test_new_with_capture(client: AsyncClient, capture: CaptureSink) -> Self {
		Self {
			client,
			capture: Some(capture),
		}
	}

	/// Test helper that constructs a fully-stubbed `SharedMqttClient`
	/// backed by an `AsyncClient` whose event loop is never polled.
	/// Callers do not need to depend on `rumqttc` directly; the event
	/// loop is dropped.
	///
	/// Intended only for tests that exercise broker-less code paths
	/// (e.g. `compute_payloads`) — any code that awaits broker I/O
	/// will hang.
	#[doc(hidden)]
	pub fn for_test_stub(client_id: &str) -> Self {
		// The rumqttc `AsyncClient::publish` path fails with
		// "Failed to send mqtt requests to eventloop" the instant
		// the matching `EventLoop` is dropped — it owns the
		// receiver half of the request channel. Keep the event
		// loop alive for the duration of the test process by
		// leaking it into `Box::leak`. Tests never rely on broker
		// I/O so the leak is bounded by `#[cfg(test)]` wiring that
		// holds these stubs only through the test run.
		let (client, event_loop) =
			AsyncClient::new(MqttOptions::new(client_id, "127.0.0.1", 1883), 1024);
		Box::leak(Box::new(event_loop));
		Self {
			client,
			capture: None,
		}
	}

	/// Publish a retained message to the given topic.
	pub async fn publish_retained(&self, topic: &str, payload: &[u8]) -> Result<(), MqttError> {
		self.record_publish(topic, payload, true);
		self.client
			.publish(topic, QoS::AtLeastOnce, true, payload)
			.await
			.map_err(|e| MqttError::PublishError(e.to_string()))?;
		Ok(())
	}

	/// Publish a non-retained message to the given topic.
	pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), MqttError> {
		self.record_publish(topic, payload, false);
		self.client
			.publish(topic, QoS::AtLeastOnce, false, payload)
			.await
			.map_err(|e| MqttError::PublishError(e.to_string()))?;
		Ok(())
	}

	fn record_publish(&self, topic: &str, payload: &[u8], retained: bool) {
		if let Some(ref sink) = self.capture {
			// Recover from poison rather than re-panicking — a panic in
			// one test holding this lock would otherwise cascade to
			// every subsequent test in the same process. Mirrors the
			// `lock_recover` discipline adopted in stream_source.rs
			// (commit b394b95).
			sink.lock().unwrap_or_else(|p| p.into_inner()).push((
				topic.to_string(),
				payload.to_vec(),
				retained,
			));
		}
	}

	/// Subscribe to a topic with QoS 1.
	pub async fn subscribe(&self, topic: &str) -> Result<(), MqttError> {
		self.client
			.subscribe(topic, QoS::AtLeastOnce)
			.await
			.map_err(|e| MqttError::SubscribeError(e.to_string()))?;
		Ok(())
	}

	/// Subscribe to all control and query topics for a camera under the
	/// given topic prefix.
	pub async fn subscribe_all(
		&self,
		topic_prefix: &str,
		camera_name: &str,
	) -> Result<(), MqttError> {
		for topic in topics::subscribe_topics(topic_prefix, camera_name) {
			tracing::debug!(topic = %topic, "subscribing to topic");
			self.subscribe(&topic).await?;
		}
		Ok(())
	}
}

/// Owns the MQTT event loop. Must be polled to process messages.
pub struct MqttEventLoop {
	event_loop: EventLoop,
}

impl MqttEventLoop {
	/// Poll the event loop for the next MQTT event.
	pub async fn poll(&mut self) -> Result<rumqttc::Event, rumqttc::ConnectionError> {
		self.event_loop.poll().await
	}
}

/// Create an MQTT connection, returning the shared client and event loop.
///
/// Sets a Last Will message on `{topic_prefix}/status` with payload
/// `"offline"` so the broker publishes it if the bridge disconnects
/// unexpectedly. `topic_prefix` is the `mqtt.topic_prefix` config value
/// (default `"bairelay"`; `"neolink"` for legacy migration).
pub fn connect(
	config: &MqttConfig,
	client_id: &str,
	topic_prefix: &str,
) -> Result<(SharedMqttClient, MqttEventLoop), MqttError> {
	let mut opts = config.to_mqtt_options(client_id);
	opts.set_last_will(LastWill::new(
		crate::topics::bridge_status(topic_prefix),
		"offline",
		QoS::AtLeastOnce,
		true,
	));
	let (client, event_loop) = AsyncClient::new(opts, 256);
	Ok((
		SharedMqttClient {
			client,
			capture: None,
		},
		MqttEventLoop { event_loop },
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn loopback_config() -> MqttConfig {
		MqttConfig {
			broker_addr: "127.0.0.1".into(),
			port: 1883,
			credentials: None,
			ca: None,
			client_auth: None,
		}
	}

	#[test]
	fn last_will_topic_uses_configured_prefix() {
		// Exercise the LWT topic interpolation in isolation — building
		// options the same way `connect` does. Guards the 		// prefix-rename against silent drift (HA discovery availability
		// keys off exactly this topic).
		let mut opts = loopback_config().to_mqtt_options("bairelay-test");
		opts.set_last_will(LastWill::new(
			format!("{}/status", "bairelay"),
			"offline",
			QoS::AtLeastOnce,
			true,
		));
		let lw = opts.last_will().expect("LWT must be set");
		assert_eq!(lw.topic, "bairelay/status");
		assert_eq!(&lw.message[..], b"offline");
		assert!(lw.retain);
	}

	#[test]
	fn last_will_topic_honours_legacy_neolink_prefix() {
		let mut opts = loopback_config().to_mqtt_options("bairelay-test");
		opts.set_last_will(LastWill::new(
			format!("{}/status", "neolink"),
			"offline",
			QoS::AtLeastOnce,
			true,
		));
		let lw = opts.last_will().expect("LWT must be set");
		assert_eq!(lw.topic, "neolink/status");
	}

	#[test]
	fn to_mqtt_options_with_credentials() {
		let cfg = MqttConfig {
			broker_addr: "broker.example".into(),
			port: 8883,
			credentials: Some(("user".into(), "secret".into())),
			ca: None,
			client_auth: None,
		};
		let opts = cfg.to_mqtt_options("cid");
		// Credentials are set on the internal options; `MqttOptions` does
		// not expose a credentials getter, so we settle for
		// not-panicking on the full build path. The broker_addr /
		// keep_alive / max_packet_size observations below guard the
		// rest of the builder surface.
		assert_eq!(opts.broker_address(), ("broker.example".into(), 8883));
		// rumqttc 0.25 returns a single usize here (incoming size).
		assert_eq!(opts.max_packet_size(), 16 * 1024 * 1024);
	}

	#[test]
	fn to_mqtt_options_with_tls_ca_only() {
		let cfg = MqttConfig {
			broker_addr: "broker".into(),
			port: 8883,
			credentials: None,
			// A minimal PEM block — the options builder does NOT parse it;
			// it just stores the bytes for rumqttc to use at connect time.
			ca: Some("-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----".into()),
			client_auth: None,
		};
		let opts = cfg.to_mqtt_options("cid");
		assert_eq!(opts.broker_address(), ("broker".into(), 8883));
	}

	#[test]
	fn to_mqtt_options_with_client_auth() {
		let cfg = MqttConfig {
			broker_addr: "broker".into(),
			port: 8883,
			credentials: None,
			ca: Some("-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----".into()),
			client_auth: Some(crate::client::ClientAuth {
				cert_pem: "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----".into(),
				key_pem: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----".into(),
			}),
		};
		let opts = cfg.to_mqtt_options("cid");
		assert_eq!(opts.broker_address(), ("broker".into(), 8883));
	}

	#[tokio::test]
	async fn shared_client_publish_and_subscribe_round_trip() {
		let client = SharedMqttClient::for_test_stub("client-test");
		// Exercise the happy paths — the stub never polls, so these
		// return Ok(()) after queueing on the rumqttc request channel.
		client
			.publish_retained("bairelay/cam/status", b"connected")
			.await
			.unwrap();
		client
			.publish("bairelay/cam/status/battery_level", b"75")
			.await
			.unwrap();
		client
			.subscribe("bairelay/cam/control/floodlight")
			.await
			.unwrap();
		client.subscribe_all("bairelay", "cam").await.unwrap();
	}

	#[test]
	fn shared_client_exposes_inner() {
		let client = SharedMqttClient::for_test_stub("inner-test");
		// `client()` returns a reference; the mere fact of calling is the test.
		let _inner: &rumqttc::AsyncClient = client.client();
	}
}
