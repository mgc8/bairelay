//! MQTT topic string helpers.
//!
//! All topics follow the pattern `{prefix}/{camera_name}/{category}/{subtopic}`
//! where `prefix` is the `mqtt.topic_prefix` config value (default
//! `"bairelay"`; `"neolink"` for drop-in migration from a legacy
//! deployment). Every helper is a pure function of `(prefix, cam)` —
//! no global state, no hidden defaults.

use crate::ai::AiObject;

// ── Status topics ──────────────────────────────────────────────────────

/// Camera online/offline status: `{prefix}/{cam}/status`
pub fn status(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status")
}

/// Availability topic of the bridge process itself: `{prefix}/status`.
/// Published `online` (retained) by the bridge on every broker ConnAck and
/// `offline` (retained) by the broker's last will if the process dies, so
/// Home Assistant's `unavailable` means "the bridge is gone" instead of
/// "the battery doorbell happens to be asleep".
pub fn bridge_status(prefix: &str) -> String {
	format!("{prefix}/status")
}

/// The camera's presence: `{prefix}/{cam}/presence`.
///
/// `reachable` when an attempt to reach the camera succeeded, `unreachable`
/// when one failed, after the normal reconnect or wake flow finished. A topic
/// separate from [`status`]: a battery doorbell closes its session a moment
/// after every wake, and hanging entity availability off the session would grey
/// all of them out during the night. The message is retained and published only
/// when the verdict changes, so the same sentence is not written fifty times
/// over to a home broker.
pub fn presence(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/presence")
}

/// Motion detection events: `{prefix}/{cam}/status/motion`
pub fn status_motion(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/motion")
}

/// Battery level percentage: `{prefix}/{cam}/status/battery_level`
pub fn status_battery_level(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/battery_level")
}

/// Full battery info XML (query response): `{prefix}/{cam}/status/battery`
pub fn status_battery(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/battery")
}

/// Preview image (base64 JPEG): `{prefix}/{cam}/status/preview`
pub fn status_preview(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/preview")
}

/// Floodlight state: `{prefix}/{cam}/status/floodlight`
pub fn status_floodlight(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/floodlight")
}

/// Floodlight task schedule: `{prefix}/{cam}/status/floodlight_tasks`
pub fn status_floodlight_tasks(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/floodlight_tasks")
}

/// PIR sensor state: `{prefix}/{cam}/status/pir`
pub fn status_pir(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/pir")
}

/// IR night-vision mode (`on` / `off` / `auto`): `{prefix}/{cam}/status/ir`
pub fn status_ir(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/ir")
}

/// Status LED on/off: `{prefix}/{cam}/status/led`
pub fn status_led(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/led")
}

/// AI detection state of one object class:
/// `{prefix}/{cam}/status/ai/{object}`
pub fn status_ai(prefix: &str, cam: &str, object: AiObject) -> String {
	format!("{prefix}/{cam}/status/ai/{}", object.topic_name())
}

/// Doorbell button press events: `{prefix}/{cam}/status/doorbell`
pub fn status_doorbell(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/doorbell")
}

/// PTZ preset position: `{prefix}/{cam}/status/ptz/preset`
pub fn status_ptz_preset(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/ptz/preset")
}

// ── Control topics ─────────────────────────────────────────────────────

/// Floodlight on/off: `{prefix}/{cam}/control/floodlight`
pub fn control_floodlight(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/floodlight")
}

/// Floodlight schedule tasks: `{prefix}/{cam}/control/floodlight_tasks`
pub fn control_floodlight_tasks(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/floodlight_tasks")
}

/// Status LED on/off: `{prefix}/{cam}/control/led`
pub fn control_led(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/led")
}

/// IR night‐vision mode: `{prefix}/{cam}/control/ir`
pub fn control_ir(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/ir")
}

/// PIR sensor enable/disable: `{prefix}/{cam}/control/pir`
pub fn control_pir(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/pir")
}

/// Reboot camera: `{prefix}/{cam}/control/reboot`
pub fn control_reboot(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/reboot")
}

/// PTZ directional movement: `{prefix}/{cam}/control/ptz`
pub fn control_ptz(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/ptz")
}

/// PTZ go to preset: `{prefix}/{cam}/control/ptz/preset`
pub fn control_ptz_preset(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/ptz/preset")
}

/// PTZ assign current position to preset: `{prefix}/{cam}/control/ptz/assign`
pub fn control_ptz_assign(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/ptz/assign")
}

/// Zoom level control: `{prefix}/{cam}/control/zoom`
pub fn control_zoom(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/zoom")
}

/// Siren on/off: `{prefix}/{cam}/control/siren`
pub fn control_siren(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/siren")
}

/// Wake battery camera for N minutes: `{prefix}/{cam}/control/wakeup`
pub fn control_wakeup(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/control/wakeup")
}

// ── Query topics ───────────────────────────────────────────────────────

/// Request battery level: `{prefix}/{cam}/query/battery`
pub fn query_battery(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/query/battery")
}

/// Request preview snapshot: `{prefix}/{cam}/query/preview`
pub fn query_preview(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/query/preview")
}

/// Request PIR state: `{prefix}/{cam}/query/pir`
pub fn query_pir(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/query/pir")
}

/// Request PTZ preset list: `{prefix}/{cam}/query/ptz/preset`
pub fn query_ptz_preset(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/query/ptz/preset")
}

// ── Settings ─────────────────────────────────────────────────────────
//
// Settings follow the shape of the existing topics exactly, with one more
// segment for the family: `status/<family>/<leaf>` for reading,
// `control/<family>/<leaf>` for writing. One leaf per Home Assistant entity,
// because a shared JSON topic does not read back cleanly after a write, and
// because a retained state should be the smallest possible one.

/// A setting's state: `{prefix}/{cam}/status/{family}/{leaf}`
pub fn status_setting(prefix: &str, cam: &str, family: &str, leaf: &str) -> String {
	format!("{prefix}/{cam}/status/{family}/{leaf}")
}

/// A whole family block, as the camera's raw JSON: `{prefix}/{cam}/status/
/// {family}/json`. For automations and debugging; the HA entities consume the
/// scalar leaves instead.
pub fn status_settings_json(prefix: &str, cam: &str, family: &str) -> String {
	format!("{prefix}/{cam}/status/{family}/json")
}

/// A family's diagnostic leaf: the readable reason for its last refusal.
/// refusal.
///
/// Clearing a family's states says it has no value, not **why**. This leaf
/// carries one short sentence - never the camera's XML, never an identifier,
/// never a password (see
/// [`crate::status::publishable_reason`], qui l'assainit avant publication).
///
/// This is not an entity leaf: no Home Assistant entity discovers it, and no
/// `SettingCommand` accepts it as a write target.
pub const REASON_LEAF: &str = "reason";

/// Reason for a family's last refusal: `{prefix}/{cam}/status/{family}/reason`
pub fn status_setting_reason(prefix: &str, cam: &str, family: &str) -> String {
	status_setting(prefix, cam, family, REASON_LEAF)
}

/// Writing a setting: `{prefix}/{cam}/control/{family}/{leaf}`
pub fn control_setting(prefix: &str, cam: &str, family: &str, leaf: &str) -> String {
	format!("{prefix}/{cam}/control/{family}/{leaf}")
}

/// (Re)reading every setting: `{prefix}/{cam}/query/settings`
pub fn query_settings(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/query/settings")
}

/// (Re)reading one family: `{prefix}/{cam}/query/settings/{family}`
pub fn query_settings_family(prefix: &str, cam: &str, family: &str) -> String {
	format!("{prefix}/{cam}/query/settings/{family}")
}

// ── Subscribe list ─────────────────────────────────────────────────────

/// Returns all control + query topics that the bridge should subscribe to
/// for the given camera.
pub fn subscribe_topics(prefix: &str, cam: &str) -> Vec<String> {
	vec![
		// Control topics
		control_floodlight(prefix, cam),
		control_floodlight_tasks(prefix, cam),
		control_led(prefix, cam),
		control_ir(prefix, cam),
		control_pir(prefix, cam),
		control_reboot(prefix, cam),
		control_ptz(prefix, cam),
		control_ptz_preset(prefix, cam),
		control_ptz_assign(prefix, cam),
		control_zoom(prefix, cam),
		control_siren(prefix, cam),
		control_wakeup(prefix, cam),
		// Settings: any leaf under a family. The wildcard is deliberate - the
		// list of valid families and leaves lives in `crate::settings`, not here,
		// and an unknown pair is rejected at parsing, not by the broker filter.
		// It also covers `control/ptz/{preset,assign}`, already listed
		// explicitly above.
		format!("{prefix}/{cam}/control/+/+"),
		// Query topics
		query_battery(prefix, cam),
		query_preview(prefix, cam),
		query_pir(prefix, cam),
		query_ptz_preset(prefix, cam),
		query_settings(prefix, cam),
		format!("{prefix}/{cam}/query/settings/+"),
	]
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_setting_topics_follow_the_existing_shape() {
		assert_eq!(
			status_setting("bairelay", "cam1", "audio", "volume"),
			"bairelay/cam1/status/audio/volume"
		);
		assert_eq!(
			control_setting("bairelay", "cam1", "denoise", "level"),
			"bairelay/cam1/control/denoise/level"
		);
		assert_eq!(
			status_settings_json("bairelay", "cam1", "image"),
			"bairelay/cam1/status/image/json"
		);
		// The reason for the last refusal is a leaf of the family, at the same
		// level as its JSON: nothing new for the broker to learn.
		assert_eq!(
			status_setting_reason("bairelay", "cam1", "audio"),
			"bairelay/cam1/status/audio/reason"
		);
		assert_eq!(
			status_setting("bairelay", "cam1", "audio", REASON_LEAF),
			status_setting_reason("bairelay", "cam1", "audio"),
"the refusal reason must stay a leaf of the family, not a topic of its own"
		);
		assert_eq!(query_settings("bairelay", "cam1"), "bairelay/cam1/query/settings");
		assert_eq!(
			query_settings_family("bairelay", "cam1", "pir"),
			"bairelay/cam1/query/settings/pir"
		);
	}

	#[test]
	fn the_subscription_list_covers_the_settings() {
		let topics = subscribe_topics("bairelay", "cam1");
		assert!(
			topics.contains(&"bairelay/cam1/control/+/+".to_string()),
"settings leaves would never be received: {topics:?}"
		);
		assert!(topics.contains(&"bairelay/cam1/query/settings".to_string()));
		assert!(topics.contains(&"bairelay/cam1/query/settings/+".to_string()));
		// No duplicates: one extra subscription is invisible but misreports the
		// number of subscriptions declared.
		let mut tri = topics.clone();
		tri.sort();
		tri.dedup();
		assert_eq!(tri.len(), topics.len(), "{topics:?}");
	}

	#[test]
	fn the_presence_topic_is_distinct_from_the_session_topic() {
		assert_eq!(presence("bairelay", "cam1"), "bairelay/cam1/presence");
		assert_eq!(presence("neolink", "porche"), "neolink/porche/presence");
		// Presence does not follow the session: the two topics must stay
		// separate, or a sleeping doorbell would grey out its whole dashboard
		// every time a session closed.
		assert_ne!(presence("bairelay", "cam1"), status("bairelay", "cam1"));
		assert_ne!(presence("bairelay", "cam1"), bridge_status("bairelay"));
	}
}
