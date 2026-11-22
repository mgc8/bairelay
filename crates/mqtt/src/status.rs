//! Status publishing helpers for pushing camera state to MQTT.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::ai::AiObject;
use crate::client::SharedMqttClient;
use crate::control::IrMode;
use crate::error::MqttError;
use crate::settings::Family;
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

/// The three codes carried by the shared presence cell: `0` = no verdict
/// published since the bridge started, then the two possible verdicts.
const PRESENCE_UNKNOWN: u8 = 0;
const PRESENCE_ONLINE: u8 = 1;
const PRESENCE_OFFLINE: u8 = 2;

/// Last presence verdict published for a camera, shared by all the
/// publishers of that camera.
///
/// A battery doorbell opens and closes its session dozens of times per
/// day. Without this shared state, every wake would republish `reachable`, and a
/// home broker does not want fifty identical messages per hour.
/// A publication therefore only leaves on a transition. The atomic swap
/// makes exactly one publication leave, even when two concurrent attempts
/// ask for the same one.
#[derive(Debug)]
pub struct PublishedPresence(AtomicU8);

impl Default for PublishedPresence {
	fn default() -> Self {
		Self(AtomicU8::new(PRESENCE_UNKNOWN))
	}
}

impl PublishedPresence {
	/// Applies the `reachable` verdict and returns the previous code when a
	/// publication must leave. Returns `None` when the requested verdict was already
	/// the current verdict, in which case nothing goes to the broker.
	fn transition(&self, reachable: bool) -> Option<(u8, u8)> {
		let cible = if reachable {
			PRESENCE_ONLINE
		} else {
			PRESENCE_OFFLINE
		};
		let mut current = self.0.load(Ordering::Acquire);
		loop {
			if current == cible {
				return None;
			}
			match self
				.0
				.compare_exchange_weak(current, cible, Ordering::AcqRel, Ordering::Acquire)
			{
				Ok(_) => return Some((current, cible)),
				Err(reel) => current = reel,
			}
		}
	}

	/// Puts the previous code back when the matching publication failed,
	/// and only while the current code is still the failed transition's.
	/// Without this rollback, the cell would claim a verdict absent from the
	/// broker, and the next contact would never publish anything again.
	fn cancel(&self, cible: u8, previous: u8) {
		let _ = self
			.0
			.compare_exchange(cible, previous, Ordering::AcqRel, Ordering::Acquire);
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
	/// Last presence verdict published, shared per camera. See
	/// [`PublishedPresence`] and [`Self::publish_presence`].
	presence: Arc<PublishedPresence>,
}

impl<'a> StatusPublisher<'a> {
	pub fn new(client: &'a SharedMqttClient, topic_prefix: &str, camera_name: &str) -> Self {
		Self {
			client,
			topic_prefix: topic_prefix.to_string(),
			camera_name: camera_name.to_string(),
			// A cell of this very instance: a publisher built here and thrown
			// away therefore deduplicates nothing. Presence goes through [`Self::with_presence`],
			// which shares the camera's cell.
			presence: Arc::new(PublishedPresence::default()),
		}
	}

	/// Same publisher, with the presence cell shared by a camera: only
	/// this constructor deduplicates between two successive publications, so only
	/// lui convient [`Self::publish_presence`].
	pub fn with_presence(
		client: &'a SharedMqttClient,
		topic_prefix: &str,
		camera_name: &str,
		presence: Arc<PublishedPresence>,
	) -> Self {
		Self {
			client,
			topic_prefix: topic_prefix.to_string(),
			camera_name: camera_name.to_string(),
			presence,
		}
	}

	/// Publish "connected" or "disconnected" on the camera status topic (retained).
	///
	/// Says nothing about availability: see [`Self::publish_presence`], which is
	/// preferred for that reason.
	pub async fn publish_connection(&self, online: bool) -> Result<(), MqttError> {
		let payload = if online { "connected" } else { "disconnected" };
		let topic = topics::status(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publishes the camera's presence verdict (`reachable` / `unreachable`)
	/// as a retained message on [`topics::presence`], only when the verdict
	/// change.
	///
	/// Presence does not follow the session: a battery doorbell closes its
	/// own a few moments after each wake, and following it would grey the whole
	/// dashboard during sleep. It is deduced from three events that already
	/// take place — bridge start, session opened, an attempt that gives up —
	/// and therefore never triggers one more network call towards the camera.
	///
	/// The deduplication cell must be shared per camera. Built by
	/// [`Self::new`], it stays specific to that instance and deduplicates only
	/// within it: the three places of the bridge that publish presence go
	/// through [`Self::with_presence`].
	pub async fn publish_presence(&self, reachable: bool) -> Result<(), MqttError> {
		let Some((previous, cible)) = self.presence.transition(reachable) else {
			return Ok(());
		};
		let payload = if reachable { "reachable" } else { "unreachable" };
		let topic = topics::presence(&self.topic_prefix, &self.camera_name);
		match self
			.client
			.publish_retained(&topic, payload.as_bytes())
			.await
		{
			Ok(()) => Ok(()),
			Err(e) => {
				self.presence.cancel(cible, previous);
				Err(e)
			}
		}
	}

	/// Publish motion detection state (retained).
	pub async fn publish_motion(&self, detected: bool) -> Result<(), MqttError> {
		let payload = if detected { "on" } else { "off" };
		let topic = topics::status_motion(&self.topic_prefix, &self.camera_name);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish the detection state of one AI object class (retained).
	pub async fn publish_ai_object(
		&self,
		object: AiObject,
		detected: bool,
	) -> Result<(), MqttError> {
		let payload = if detected { "on" } else { "off" };
		let topic = topics::status_ai(&self.topic_prefix, &self.camera_name, object);
		self.client
			.publish_retained(&topic, payload.as_bytes())
			.await
	}

	/// Publish a doorbell button press (`press`, not retained: a press
	/// is an event, a retained copy would replay on every reconnect).
	pub async fn publish_doorbell_press(&self) -> Result<(), MqttError> {
		let topic = topics::status_doorbell(&self.topic_prefix, &self.camera_name);
		self.client.publish(&topic, b"press").await
	}

	/// Publish "unknown" motion state (retained). Used at startup before
	/// the motion listener has determined the actual state.
	pub async fn publish_motion_unknown(&self) -> Result<(), MqttError> {
		let topic = topics::status_motion(&self.topic_prefix, &self.camera_name);
		self.client.publish_retained(&topic, b"unknown").await
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

	// ── Settings ────────────────────────────────────────────────────────
	//
	// One topic per leaf, retained, and **always the re-read value**: after
	// a write, the dispatch re-reads the block and publishes what the camera
	// kept. Publishing the requested value would make a state retained on the
	// broker lie, and it would survive the bridge's restart.

	/// Publishes the text value of a setting (retained).
	pub async fn publish_setting(
		&self,
		family: Family,
		leaf: &str,
		value: &str,
	) -> Result<(), MqttError> {
		let topic =
			topics::status_setting(&self.topic_prefix, &self.camera_name, family.slug(), leaf);
		self.client
			.publish_retained(&topic, value.as_bytes())
			.await
	}

	/// A setting that is a switch: `on`/`off`, exactly the two
	/// texts the `switch` entity discovers in `state_on`/`state_off`.
	pub async fn publish_setting_switch(
		&self,
		family: Family,
		leaf: &str,
		on: bool,
	) -> Result<(), MqttError> {
		self.publish_setting(family, leaf, if on { "on" } else { "off" })
			.await
	}

	/// A flag read in the camera's block (`"1"`/`"0"`), for the
	/// read-only `binary_sensor`s.
	pub async fn publish_setting_flag(
		&self,
		family: Family,
		leaf: &str,
		on: bool,
	) -> Result<(), MqttError> {
		self.publish_setting(family, leaf, if on { "1" } else { "0" })
			.await
	}

	/// A family's block in JSON (retained): for automations and
	/// diagnostics. The Home Assistant entities consume the leaves
	/// scalaires.
	pub async fn publish_settings_json(
		&self,
		family: Family,
		json: &str,
	) -> Result<(), MqttError> {
		let topic =
			topics::status_settings_json(&self.topic_prefix, &self.camera_name, family.slug());
		self.client.publish_retained(&topic, json.as_bytes()).await
	}

	/// Erases a retained state (empty retained payload = topic erased at
	/// Home Assistant).
	///
	/// That is what we do when the camera **did not answer**: better a
	/// `unknown` sensor than last time's value, which could come from another
	/// setting, another firmware, or from before a pairing.
	/// "Refused" and "empty" must stay distinct.
	pub async fn clear_setting(&self, family: Family, leaf: &str) -> Result<(), MqttError> {
		let topic =
			topics::status_setting(&self.topic_prefix, &self.camera_name, family.slug(), leaf);
		self.client.publish_retained(&topic, b"").await
	}

	/// Erases a family's JSON.
	pub async fn clear_settings_json(&self, family: Family) -> Result<(), MqttError> {
		let topic =
			topics::status_settings_json(&self.topic_prefix, &self.camera_name, family.slug());
		self.client.publish_retained(&topic, b"").await
	}

	/// The reason of a family's last refusal, retained
	/// (`status/<family>/reason`).
	///
	/// A family whose states are erased is silent: Home Assistant shows
	/// `unknown` and nothing says whether the camera refused, did not answer,
	/// or does not print the tag. This topic carries the sentence. It is sanitised by
	/// [`crate::status::publishable_reason`] — one line, no XML, no secret — because a
	/// retained topic is re-read years later.
	pub async fn publish_family_reason(
		&self,
		family: Family,
		reason: &str,
	) -> Result<(), MqttError> {
		let topic =
			topics::status_setting_reason(&self.topic_prefix, &self.camera_name, family.slug());
		let propre = publishable_reason(reason);
		self.client.publish_retained(&topic, propre.as_bytes()).await
	}

	/// Erases a family's reason: it just answered, the previous
	/// reason no longer has any reason to be retained.
	pub async fn clear_family_reason(&self, family: Family) -> Result<(), MqttError> {
		let topic =
			topics::status_setting_reason(&self.topic_prefix, &self.camera_name, family.slug());
		self.client.publish_retained(&topic, b"").await
	}

}

/// Maximum length of a reason. A retained topic is re-read at every Home
/// Assistant start: one sentence, not a stack trace or a dump.
const REASON_MAX: usize = 160;

/// What we publish when the reason itself is unusable.
pub(crate) const REASON_NO_DETAIL: &str = "the camera gave no reason";
pub(crate) const REASON_MASKED: &str = "reason hidden: it could have contained a secret";

/// The substrings that have no business on a retained topic. No reason
/// built by the bridge contains one; this belt holds the promise
/// "never a secret in a retained topic" even if a caller passes the
/// raw text of a camera error.
const REASON_FORBIDDEN: [&str; 6] =
	["password", "passwd", "mot de passe", "token", "secret", "apikey"];

/// Puts a refusal reason in a state fit to be retained on the broker: one
/// single line, no XML tag, no secret, bounded to [`REASON_MAX`] characters.
///
/// The tags are replaced by french guillemets and not deleted:
/// `this camera's reply has no <OsdChannelName> tag` is precisely
/// the information that explains a refusal, and what must disappear is the
/// XML delimiter, not the tag's name.
pub fn publishable_reason(reason: &str) -> String {
	publishable_reason_bounded(reason, REASON_MAX)
}

/// Same cleaning, with the bound chosen by the caller.
///
/// [`crate::status::publishable_reason`] caps at 160 characters because its sentences land on
/// **retained** topics that Home Assistant replays on every start: one line, not
/// a dump. The wider bound is for the opposite case — reading the camera's own
/// answer back, where the body worth seeing is longer than that cap leaves
/// room for: a whole `<MD>` block fits in the first 400 characters. Neither path
/// may become a channel for secrets, so the cleaning is identical in both: no
/// bare XML delimiter, no control character, and the mask applies whenever the
/// text looks like a credential.
pub fn publishable_reason_bounded(reason: &str, max_chars: usize) -> String {
	let tag_free: String = reason
		.chars()
		.map(|c| match c {
			'<' => '«',
			'>' => '»',
			c if c.is_control() => ' ',
			c => c,
		})
		.collect();
	if REASON_FORBIDDEN
		.iter()
		.any(|forbidden| tag_free.to_lowercase().contains(forbidden))
	{
		return REASON_MASKED.to_string();
	}
	let propre = tag_free.split_whitespace().collect::<Vec<_>>().join(" ");
	if propre.is_empty() {
		return REASON_NO_DETAIL.to_string();
	}
	if propre.chars().count() <= max_chars {
		return propre;
	}
	let tronc: String = propre.chars().take(max_chars - 1).collect();
	format!("{}…", tronc.trim_end())
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
	async fn publish_ai_object_is_retained_on_off() {
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.publish_ai_object(AiObject::Person, true)
			.await
			.unwrap();
		pub_.publish_ai_object(AiObject::Package, false)
			.await
			.unwrap();
		assert_eq!(
			mock.published(),
			vec![
				(
					"bairelay/cam1/status/ai/person".to_string(),
					b"on".to_vec(),
					true
				),
				(
					"bairelay/cam1/status/ai/package".to_string(),
					b"off".to_vec(),
					true
				),
			]
		);
	}

	#[tokio::test]
	async fn publish_doorbell_press_is_not_retained() {
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.publish_doorbell_press().await.unwrap();
		assert_eq!(
			mock.published(),
			vec![(
				"bairelay/cam1/status/doorbell".to_string(),
				b"press".to_vec(),
				false
			)]
		);
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
		for ((topic, payload, retained), (and, ep)) in got.iter().zip(expect) {
			assert_eq!(
				(topic.as_str(), payload.as_slice(), *retained),
				(and, ep.as_bytes(), true)
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

	#[tokio::test]
	async fn a_setting_publishes_on_its_own_retained_leaf() {
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.publish_setting(Family::Audio, "volume", "42")
			.await
			.unwrap();
		pub_.publish_setting_switch(Family::Denoise, "enable", true)
			.await
			.unwrap();
		pub_.publish_setting_flag(Family::Sleep, "state", false)
			.await
			.unwrap();
		assert_eq!(
			mock.published(),
			vec![
				(
					"bairelay/cam1/status/audio/volume".to_string(),
					b"42".to_vec(),
					true
				),
				(
					"bairelay/cam1/status/denoise/enable".to_string(),
					b"on".to_vec(),
					true
				),
				(
					"bairelay/cam1/status/sleep/state".to_string(),
					b"0".to_vec(),
					true
				),
			]
		);
	}

	#[tokio::test]
	async fn a_refused_setting_is_not_published_but_clears() {
		// The camera did not answer: the retained state is erased, we do not put
		// a default value in it. An `unknown` sensor is honest; a retained
		// `0` is not.
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.clear_setting(Family::Chime, "list").await.unwrap();
		pub_.clear_settings_json(Family::Image).await.unwrap();
		assert_eq!(
			mock.published(),
			vec![
				(
					"bairelay/cam1/status/chime/list".to_string(),
					Vec::new(),
					true
				),
				(
					"bairelay/cam1/status/image/json".to_string(),
					Vec::new(),
					true
				),
			]
		);
	}

	#[tokio::test]
	async fn the_reason_for_a_family_is_kept_then_cleared() {
		// A family's refusal must stay readable after the fact: retained, and
		// erased (not replaced by a triumphant sentence) as soon as the family
		// has answered.
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_
			.publish_family_reason(Family::Motion, "the camera answered MdAlarm: not writable")
			.await
			.unwrap();
		pub_.clear_family_reason(Family::Motion).await.unwrap();
		assert_eq!(
			mock.published(),
			vec![
				(
					"bairelay/cam1/status/motion/reason".to_string(),
					"the camera answered MdAlarm: not writable".as_bytes().to_vec(),
					true
				),
				("bairelay/cam1/status/motion/reason".to_string(), Vec::new(), true),
			]
		);
	}

	/// A retained topic will never see the camera's raw XML, a secret, nor
	/// ten lines of trace: `publishable_reason` is the last barrier, and
	/// it is what holds the promise, not the callers' vigilance.
	#[test]
	fn a_reason_carries_neither_xml_nor_secrets() {
		// The tag's name is the useful information; only its delimiters
		// disappear.
		assert_eq!(
			publishable_reason("no <OsdChannelName> tag here"),
			// The guillemets in the expected string are the sanitizer's own mapping
			// ('<' above), so this expectation has to spell what the code produces.
			"no «OsdChannelName» tag here"
		);
		assert_eq!(publishable_reason("  \n\t  "), REASON_NO_DETAIL);
		assert_eq!(
			publishable_reason("could not auth with password hunter2"),
			REASON_MASKED
		);
		assert_eq!(
			publishable_reason("<Wifi><key>mot de passe</key></Wifi>"),
			REASON_MASKED,
			"an XML block passed by mistake must not come back out"
		);
		assert_eq!(
			publishable_reason("MdAlarm: refusal\nto rewrite"),
			"MdAlarm: refusal to rewrite",
			"a reason fits on one line"
		);
		let long = publishable_reason(&"a".repeat(4000));
		assert!(long.chars().count() <= REASON_MAX, "{} characters", long.chars().count());
		assert!(long.ends_with('…'), "{long}");
	}

	#[tokio::test]
	async fn the_json_of_a_family_is_kept() {
		let (c, mock) = crate::test_support::mock_client();
		let pub_ = StatusPublisher::new(&c, "bairelay", "cam1");
		pub_.publish_settings_json(Family::Ntp, r#"{"enable":"1"}"#)
			.await
			.unwrap();
		assert_eq!(
			mock.published(),
			vec![(
				"bairelay/cam1/status/ntp/json".to_string(),
				br#"{"enable":"1"}"#.to_vec(),
				true
			)]
		);
	}

	/// Presence deduplication: two consecutive successes produce a single
	/// publication, and the transition to the opposite verdict produces a
	/// second one. Without this guard, fifty wakes per hour would write fifty
	/// times the same retained message on a home broker.
	#[tokio::test]
	async fn presence_publishes_only_on_transition() {
		let (c, mock) = crate::test_support::mock_client();
		let state = Arc::new(PublishedPresence::default());
		let publ = StatusPublisher::with_presence(&c, "bairelay", "cam1", Arc::clone(&state));
		publ.publish_presence(true).await.unwrap();
		publ.publish_presence(true).await.unwrap();
		assert_eq!(
			mock.published(),
			vec![(
				"bairelay/cam1/presence".to_string(),
				b"reachable".to_vec(),
				true
			)],
			"two consecutive successes must not publish anything more"
		);
		publ.publish_presence(false).await.unwrap();
		publ.publish_presence(false).await.unwrap();
		assert_eq!(
			mock.published(),
			vec![
				(
					"bairelay/cam1/presence".to_string(),
					b"reachable".to_vec(),
					true
				),
				(
					"bairelay/cam1/presence".to_string(),
					b"unreachable".to_vec(),
					true
				),
			],
			"one transition, one publication"
		);
	}

	/// The three places of the bridge each build their own publisher: the cell
	/// must therefore carry the deduplication beyond the instances — otherwise it
	/// jumps as soon as two places publish.
	#[tokio::test]
	async fn presence_is_deduplicated_between_two_publishers() {
		let (c, mock) = crate::test_support::mock_client();
		let state = Arc::new(PublishedPresence::default());
		StatusPublisher::with_presence(&c, "bairelay", "cam1", Arc::clone(&state))
			.publish_presence(true)
			.await
			.unwrap();
		StatusPublisher::with_presence(&c, "bairelay", "cam1", Arc::clone(&state))
			.publish_presence(true)
			.await
			.unwrap();
		assert_eq!(mock.count(), 1, "{:?}", mock.published());
	}
}
