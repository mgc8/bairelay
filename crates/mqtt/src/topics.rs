//! MQTT topic string helpers.
//!
//! All topics follow the pattern `{prefix}/{camera_name}/{category}/{subtopic}`
//! where `prefix` is the `mqtt.topic_prefix` config value (default
//! `"bairelay"`; `"neolink"` for drop-in migration from a legacy
//! deployment). Every helper is a pure function of `(prefix, cam)` —
//! no global state, no hidden defaults.

use crate::status::MotionClass;

// ── Status topics ──────────────────────────────────────────────────────

/// Camera online/offline status: `{prefix}/{cam}/status`
pub fn status(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status")
}

/// Motion detection events: `{prefix}/{cam}/status/motion`
pub fn status_motion(prefix: &str, cam: &str) -> String {
	format!("{prefix}/{cam}/status/motion")
}

/// Per-class motion detection: `{prefix}/{cam}/status/motion/{class}`
pub fn status_motion_class(prefix: &str, cam: &str, class: MotionClass) -> String {
	format!("{prefix}/{cam}/status/motion/{}", class.as_str())
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
		// Query topics
		query_battery(prefix, cam),
		query_preview(prefix, cam),
		query_pir(prefix, cam),
		query_ptz_preset(prefix, cam),
	]
}
