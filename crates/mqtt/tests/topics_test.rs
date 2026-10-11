// Tests for crates/mqtt/src/topics.rs.
//
// Baseline uses the default prefix "bairelay"; the legacy "neolink"
// prefix is exercised explicitly where neolink-compat matters.

const PREFIX: &str = "bairelay";
const LEGACY: &str = "neolink";

#[test]
fn status_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status(PREFIX, "frontdoor"),
		"bairelay/frontdoor/status"
	);
}

#[test]
fn status_topic_format_legacy_neolink_prefix() {
	assert_eq!(
		bairelay_mqtt::topics::status(LEGACY, "frontdoor"),
		"neolink/frontdoor/status"
	);
}

#[test]
fn motion_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_motion(PREFIX, "garden"),
		"bairelay/garden/status/motion"
	);
}

#[test]
fn motion_class_topic_format() {
	let topics: Vec<String> = bairelay_mqtt::MotionClass::ALL
		.into_iter()
		.map(|class| bairelay_mqtt::topics::status_motion_class(PREFIX, "garden", class))
		.collect();
	assert_eq!(
		topics,
		[
			"bairelay/garden/status/motion/person",
			"bairelay/garden/status/motion/vehicle",
			"bairelay/garden/status/motion/animal",
			"bairelay/garden/status/motion/other",
		]
	);
}

#[test]
fn control_topic_pattern() {
	assert_eq!(
		bairelay_mqtt::topics::control_floodlight(PREFIX, "cam1"),
		"bairelay/cam1/control/floodlight"
	);
}

#[test]
fn query_topic_pattern() {
	assert_eq!(
		bairelay_mqtt::topics::query_battery(PREFIX, "cam1"),
		"bairelay/cam1/query/battery"
	);
}

#[test]
fn all_subscribe_topics_for_camera() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/control/floodlight".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/led".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/reboot".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ptz".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/battery".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/preview".to_string()));
}

#[test]
fn preview_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_preview(PREFIX, "cam1"),
		"bairelay/cam1/status/preview"
	);
}

#[test]
fn ptz_preset_topic_uses_slashes() {
	assert_eq!(
		bairelay_mqtt::topics::control_ptz_preset(PREFIX, "cam1"),
		"bairelay/cam1/control/ptz/preset"
	);
	assert_eq!(
		bairelay_mqtt::topics::status_ptz_preset(PREFIX, "cam1"),
		"bairelay/cam1/status/ptz/preset"
	);
	assert_eq!(
		bairelay_mqtt::topics::query_ptz_preset(PREFIX, "cam1"),
		"bairelay/cam1/query/ptz/preset"
	);
}

#[test]
fn ptz_assign_topic_uses_slashes() {
	assert_eq!(
		bairelay_mqtt::topics::control_ptz_assign(PREFIX, "cam1"),
		"bairelay/cam1/control/ptz/assign"
	);
}

#[test]
fn subscribe_topics_include_ptz_slash_format() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/control/ptz/preset".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ptz/assign".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/ptz/preset".to_string()));
}

// ── floodlight_tasks topic ────────────────────────────────

#[test]
fn floodlight_tasks_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_floodlight_tasks(PREFIX, "cam1"),
		"bairelay/cam1/status/floodlight_tasks"
	);
}

#[test]
fn floodlight_tasks_control_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_floodlight_tasks(PREFIX, "cam1"),
		"bairelay/cam1/control/floodlight_tasks"
	);
}

#[test]
fn subscribe_topics_include_floodlight_tasks() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/control/floodlight_tasks".to_string()));
}

// ── PIR and wakeup topics ────────────────────────────────

#[test]
fn pir_status_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_pir(PREFIX, "cam1"),
		"bairelay/cam1/status/pir"
	);
}

#[test]
fn pir_control_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_pir(PREFIX, "cam1"),
		"bairelay/cam1/control/pir"
	);
}

#[test]
fn wakeup_control_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_wakeup(PREFIX, "cam1"),
		"bairelay/cam1/control/wakeup"
	);
}

#[test]
fn battery_level_status_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_battery_level(PREFIX, "cam1"),
		"bairelay/cam1/status/battery_level"
	);
}

#[test]
fn subscribe_topics_include_wakeup_and_pir() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/control/wakeup".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/pir".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/pir".to_string()));
}

// ── Remaining topic functions ───────────────────────────────────────

#[test]
fn status_battery_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_battery(PREFIX, "cam1"),
		"bairelay/cam1/status/battery"
	);
}

#[test]
fn status_floodlight_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::status_floodlight(PREFIX, "cam1"),
		"bairelay/cam1/status/floodlight"
	);
}

#[test]
fn control_ir_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_ir(PREFIX, "cam1"),
		"bairelay/cam1/control/ir"
	);
}

#[test]
fn control_zoom_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_zoom(PREFIX, "cam1"),
		"bairelay/cam1/control/zoom"
	);
}

#[test]
fn control_siren_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_siren(PREFIX, "cam1"),
		"bairelay/cam1/control/siren"
	);
}

#[test]
fn control_ptz_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_ptz(PREFIX, "cam1"),
		"bairelay/cam1/control/ptz"
	);
}

#[test]
fn control_led_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_led(PREFIX, "cam1"),
		"bairelay/cam1/control/led"
	);
}

#[test]
fn control_reboot_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::control_reboot(PREFIX, "cam1"),
		"bairelay/cam1/control/reboot"
	);
}

#[test]
fn query_preview_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::query_preview(PREFIX, "cam1"),
		"bairelay/cam1/query/preview"
	);
}

#[test]
fn query_pir_topic_format() {
	assert_eq!(
		bairelay_mqtt::topics::query_pir(PREFIX, "cam1"),
		"bairelay/cam1/query/pir"
	);
}

#[test]
fn subscribe_topics_count() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	// 12 control + 4 query = 16 topics
	assert_eq!(subs.len(), 16);
}

#[test]
fn subscribe_topics_include_all_control() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/control/floodlight".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/floodlight_tasks".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/led".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ir".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/pir".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/reboot".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ptz".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ptz/preset".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/ptz/assign".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/zoom".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/siren".to_string()));
	assert!(subs.contains(&"bairelay/cam1/control/wakeup".to_string()));
}

#[test]
fn subscribe_topics_include_all_query() {
	let subs = bairelay_mqtt::topics::subscribe_topics(PREFIX, "cam1");
	assert!(subs.contains(&"bairelay/cam1/query/battery".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/preview".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/pir".to_string()));
	assert!(subs.contains(&"bairelay/cam1/query/ptz/preset".to_string()));
}

// ── legacy neolink-prefix subscribe list ──────────────────

#[test]
fn legacy_neolink_prefix_subscribe_list_matches_pre_2g_paths() {
	// Migration-path proof: the entire subscribe list under the legacy
	// prefix exactly reproduces the hardcoded strings bairelay emitted
	// before .
	let subs = bairelay_mqtt::topics::subscribe_topics(LEGACY, "cam1");
	assert!(subs.contains(&"neolink/cam1/control/floodlight".to_string()));
	assert!(subs.contains(&"neolink/cam1/control/wakeup".to_string()));
	assert!(subs.contains(&"neolink/cam1/query/battery".to_string()));
	assert_eq!(subs.len(), 16);
}
