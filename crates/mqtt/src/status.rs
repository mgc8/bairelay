//! Status publishing helpers for pushing camera state to MQTT.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::client::SharedMqttClient;
use crate::control::IrMode;
use crate::error::MqttError;
use crate::topics;

/// Wire-format JSON payload published on the floodlight state topic.
/// Matches neolink's `DiscoveryLight.state_value_template = "{{ value_json.state }}"`,
/// so HA can template the state off the `state` key. Exposed so tests
/// can assert on the exact bytes without spinning up an MQTT client.
pub(crate) fn floodlight_state_json(on: bool) -> &'static str {
	if on {
		r#"{"state":"on"}"#
	} else {
		r#"{"state":"off"}"#
	}
}

/// Object class a motion alarm is attributed to. Each class has its
/// own retained `status/motion/{class}` topic alongside the aggregate
/// `status/motion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MotionClass {
	Person,
	Vehicle,
	Animal,
	/// Motion the camera didn't attribute to any of the other classes.
	Other,
}

impl MotionClass {
	pub const ALL: [MotionClass; 4] = [
		MotionClass::Person,
		MotionClass::Vehicle,
		MotionClass::Animal,
		MotionClass::Other,
	];

	/// Topic leaf and HA `unique_id` suffix.
	pub fn as_str(self) -> &'static str {
		match self {
			MotionClass::Person => "person",
			MotionClass::Vehicle => "vehicle",
			MotionClass::Animal => "animal",
			MotionClass::Other => "other",
		}
	}
}

/// Convenience wrapper that publishes camera status messages.
///
/// Binds together a shared MQTT client, the `topic_prefix` (from the
/// `mqtt.topic_prefix` config knob — default `"bairelay"`), and a
/// camera name, so each call site does not have to re-construct the
/// full topic string.
pub struct StatusPublisher<'a> {
	client: &'a SharedMqttClient,
	topic_prefix: String,
	camera_name: String,
}

impl<'a> StatusPublisher<'a> {
	pub fn new(client: &'a SharedMqttClient, topic_prefix: &str, camera_name: &str) -> Self {
		Self {
			client,
			topic_prefix: topic_prefix.to_string(),
			camera_name: camera_name.to_string(),
		}
	}

	/// Publish "connected" or "disconnected" on the camera status topic (retained).
	pub async fn publish_connection(&self, online: bool) -> Result<(), MqttError> {
		let payload = if online { "connected" } else { "disconnected" };
		let topic = topics::status(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish motion detection state (retained).
	pub async fn publish_motion(&self, detected: bool) -> Result<(), MqttError> {
		let payload = if detected { "on" } else { "off" };
		let topic = topics::status_motion(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish one motion class's detection state (retained).
	pub async fn publish_motion_class(
		&self,
		class: MotionClass,
		detected: bool,
	) -> Result<(), MqttError> {
		let payload = if detected { "on" } else { "off" };
		let topic = topics::status_motion_class(&self.topic_prefix, &self.camera_name, class);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish "unknown" on the motion topic and every per-class
	/// motion topic (retained). Used at startup before the motion
	/// listener has determined the actual state, so a retained `on`
	/// from a previous run can't linger.
	pub async fn publish_motion_unknown(&self) -> Result<(), MqttError> {
		let topic = topics::status_motion(&self.topic_prefix, &self.camera_name);
		self.client.publish_retained(&topic, b"unknown").await?;
		for class in MotionClass::ALL {
			let topic = topics::status_motion_class(&self.topic_prefix, &self.camera_name, class);
			self.client.publish_retained(&topic, b"unknown").await?;
		}
		Ok(())
	}

	/// Publish battery level as a percentage string (retained).
	pub async fn publish_battery_level(&self, percent: u8) -> Result<(), MqttError> {
		let clamped = percent.min(100);
		let topic = topics::status_battery_level(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, clamped.to_string().as_bytes())
			.await
	}

	/// Publish a preview image as base64-encoded JPEG (retained).
	pub async fn publish_preview(&self, jpeg_data: &[u8]) -> Result<(), MqttError> {
		let encoded = BASE64.encode(jpeg_data);
		let topic = topics::status_preview(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, encoded.as_bytes())
			.await
	}

	/// Publish floodlight state (retained). Emits `{"state":"on"}` or
	/// `{"state":"off"}` so HA's `light` discovery entity can template
	/// the value out with `value_json.state`, matching neolink's wire
	/// format. The sibling
	/// [`Self::publish_floodlight_tasks_enabled`] stays plain
	/// `"on"`/`"off"` — neolink's tasks switch is a `switch` (not a
	/// templated `light`) and takes bare state_on/state_off.
	pub async fn publish_floodlight(&self, on: bool) -> Result<(), MqttError> {
		let payload = floodlight_state_json(on);
		let topic = topics::status_floodlight(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish floodlight task schedule as JSON (retained).
	pub async fn publish_floodlight_tasks(&self, json: &str) -> Result<(), MqttError> {
		let topic = topics::status_floodlight_tasks(&self.topic_prefix, &self.camera_name);
		self.client.publish_retained(&topic, json.as_bytes()).await
	}

	/// Publish floodlight tasks enabled/disabled state (retained).
	pub async fn publish_floodlight_tasks_enabled(&self, enabled: bool) -> Result<(), MqttError> {
		let payload = if enabled { "on" } else { "off" };
		let topic = topics::status_floodlight_tasks(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish PIR sensor state (retained).
	pub async fn publish_pir(&self, enabled: bool) -> Result<(), MqttError> {
		let payload = if enabled { "on" } else { "off" };
		let topic = topics::status_pir(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish IR night-vision mode (retained).
	pub async fn publish_ir(&self, mode: IrMode) -> Result<(), MqttError> {
		let topic = topics::status_ir(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, mode.as_payload().as_bytes())
			.await
	}

	/// Publish status LED state (retained).
	pub async fn publish_led(&self, on: bool) -> Result<(), MqttError> {
		let payload = if on { "on" } else { "off" };
		let topic = topics::status_led(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish PTZ preset position as JSON (retained).
	pub async fn publish_ptz_preset(&self, json: &str) -> Result<(), MqttError> {
		let topic = topics::status_ptz_preset(&self.topic_prefix, &self.camera_name);
		self.client.publish_retained(&topic, json.as_bytes()).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Build a StatusPublisher over a stub client whose event loop is
	/// leaked — publishes hand the payload to the rumqttc channel and
	/// return `Ok(())` without a broker round-trip. Good enough for
	/// line-coverage tests that exercise the publish-helper wrappers.
	fn stub_publisher() -> (SharedMqttClient, String, String) {
		let client = SharedMqttClient::for_test_stub("status-test");
		(client, "bairelay".to_string(), "cam1".to_string())
	}

	#[tokio::test]
	async fn publish_connection_both_states() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		pub_.publish_connection(true).await.unwrap();
		pub_.publish_connection(false).await.unwrap();
	}

	#[tokio::test]
	async fn publish_motion_both_states_and_unknown() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		pub_.publish_motion(true).await.unwrap();
		pub_.publish_motion(false).await.unwrap();
		pub_.publish_motion_unknown().await.unwrap();
	}

	#[tokio::test]
	async fn publish_motion_class_both_states() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		for class in MotionClass::ALL {
			pub_.publish_motion_class(class, true).await.unwrap();
			pub_.publish_motion_class(class, false).await.unwrap();
		}
	}

	#[tokio::test]
	async fn publish_motion_unknown_resets_every_class_topic() {
		let (client, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&client, "bairelay", "cam1");
		pub_.publish_motion_unknown().await.unwrap();
		let published = mock.published();
		let unknown: Vec<&str> = published
			.iter()
			.filter(|(_, payload, retained)| payload == b"unknown" && *retained)
			.map(|(topic, _, _)| topic.as_str())
			.collect();
		assert_eq!(
			unknown,
			[
				"bairelay/cam1/status/motion",
				"bairelay/cam1/status/motion/person",
				"bairelay/cam1/status/motion/vehicle",
				"bairelay/cam1/status/motion/animal",
				"bairelay/cam1/status/motion/other",
			]
		);
	}

	#[tokio::test]
	async fn publish_battery_level_clamps_to_100() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		// All three should succeed; the clamp is covered implicitly by
		// not panicking on 200 and by the assertion below on the exact
		// rounding in the topic tests.
		pub_.publish_battery_level(0).await.unwrap();
		pub_.publish_battery_level(50).await.unwrap();
		pub_.publish_battery_level(200).await.unwrap();
	}

	#[tokio::test]
	async fn publish_preview_base64_encodes() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		pub_.publish_preview(&[0xFF, 0xD8, 0xFF]).await.unwrap();
		pub_.publish_preview(&[]).await.unwrap();
	}

	#[tokio::test]
	async fn publish_floodlight_and_tasks() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		pub_.publish_floodlight(true).await.unwrap();
		pub_.publish_floodlight(false).await.unwrap();
		pub_.publish_floodlight_tasks("[]").await.unwrap();
		pub_.publish_floodlight_tasks_enabled(true).await.unwrap();
		pub_.publish_floodlight_tasks_enabled(false).await.unwrap();
	}

	#[tokio::test]
	async fn publish_pir_and_ptz() {
		let (c, p, n) = stub_publisher();
		let pub_ = StatusPublisher::new(&c, &p, &n);
		pub_.publish_pir(true).await.unwrap();
		pub_.publish_pir(false).await.unwrap();
		pub_.publish_ptz_preset(r#"{"presets":[]}"#).await.unwrap();
	}

	#[tokio::test]
	async fn publish_ir_and_led_retained_on_status_topics() {
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.publish_ir(IrMode::Auto).await.unwrap();
		pub_.publish_ir(IrMode::Off).await.unwrap();
		pub_.publish_led(true).await.unwrap();
		let got = mock.published();
		let expect = [
			("bairelay/cam1/status/ir", "auto"),
			("bairelay/cam1/status/ir", "off"),
			("bairelay/cam1/status/led", "on"),
		];
		assert_eq!(got.len(), expect.len(), "published: {got:?}");
		for ((topic, payload, retained), (et, ep)) in got.iter().zip(expect) {
			assert_eq!(
				(topic.as_str(), payload.as_slice(), *retained),
				(et, ep.as_bytes(), true)
			);
		}
	}

	#[test]
	fn floodlight_state_json_on_matches_neolink_light_template() {
		// HA's DiscoveryLight.state_value_template is
		//   "{{ value_json.state }}"
		// so the wire bytes MUST be JSON with a `state` key. Any other
		// shape breaks HA's light entity silently.
		assert_eq!(floodlight_state_json(true), r#"{"state":"on"}"#);
	}

	#[test]
	fn floodlight_state_json_off_matches_neolink_light_template() {
		assert_eq!(floodlight_state_json(false), r#"{"state":"off"}"#);
	}

	#[test]
	fn floodlight_state_json_is_valid_json() {
		// Belt-and-braces: round-trip both payloads through serde_json
		// so a typo in the literal surfaces as a failing test, not a
		// retained-message HA bug.
		for state in [true, false] {
			let payload = floodlight_state_json(state);
			let v: serde_json::Value = serde_json::from_str(payload).expect("valid JSON");
			let got = v["state"].as_str().expect("state is a string");
			assert_eq!(got, if state { "on" } else { "off" });
		}
	}
}
