// Tests for crates/mqtt/src/control.rs. These tests exercise the full
// parser + control_topic round-trip.
//
// Uses the legacy `"neolink"` prefix throughout to keep the topic
// literals aligned with the neolink reference wire format. The
// bairelay-default prefix is covered explicitly in one top-of-file
// test so coverage is not neolink-only.

use bairelay_mqtt::control::{parse_control_message, ControlCommand};

const P: &str = "neolink";

#[test]
fn parse_floodlight_on() {
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", b"on");
	assert!(
		matches!(cmd, Some(ControlCommand::Floodlight { ref camera, state: true }) if camera == "cam1")
	);
}

#[test]
fn parse_floodlight_on_default_bairelay_prefix() {
	let cmd = parse_control_message("bairelay", "bairelay/cam1/control/floodlight", b"on");
	assert!(
		matches!(cmd, Some(ControlCommand::Floodlight { ref camera, state: true }) if camera == "cam1")
	);
}

#[test]
fn parse_floodlight_off() {
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", b"off");
	assert!(
		matches!(cmd, Some(ControlCommand::Floodlight { ref camera, state: false }) if camera == "cam1")
	);
}

#[test]
fn parse_floodlight_tasks_on_off() {
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight_tasks", b"on");
	assert!(
		matches!(cmd, Some(ControlCommand::FloodlightTasks { ref camera, state: true }) if camera == "cam1")
	);
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight_tasks", b"off");
	assert!(
		matches!(cmd, Some(ControlCommand::FloodlightTasks { ref camera, state: false }) if camera == "cam1")
	);
}

#[test]
fn reject_floodlight_tasks_json() {
	let cmd = parse_control_message(
		P,
		"neolink/cam1/control/floodlight_tasks",
		b"{\"key\":\"val\"}",
	);
	assert!(cmd.is_none());
}

#[test]
fn parse_ptz_with_amount() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", b"left 64");
	assert!(
		matches!(cmd, Some(ControlCommand::Ptz { ref camera, ref direction, amount }) if camera == "cam1" && amount == 64.0)
	);
}

#[test]
fn parse_ptz_default_amount() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", b"up");
	assert!(
		matches!(cmd, Some(ControlCommand::Ptz { ref camera, ref direction, amount }) if camera == "cam1" && amount == 32.0)
	);
}

#[test]
fn parse_reboot() {
	let cmd = parse_control_message(P, "neolink/cam1/control/reboot", b"");
	assert!(matches!(cmd, Some(ControlCommand::Reboot { ref camera }) if camera == "cam1"));
}

#[test]
fn parse_wakeup_minutes() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"5");
	assert!(
		matches!(cmd, Some(ControlCommand::Wakeup { ref camera, minutes: 5 }) if camera == "cam1")
	);
}

#[test]
fn wakeup_accepts_max_1440() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"1440");
	assert!(matches!(
		cmd,
		Some(ControlCommand::Wakeup { minutes: 1440, .. })
	));
}

#[test]
fn wakeup_rejects_above_1440() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"1441");
	assert!(cmd.is_none());
}

#[test]
fn parse_ir_auto() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ir", b"auto");
	assert!(matches!(cmd, Some(ControlCommand::Ir { ref camera, .. }) if camera == "cam1"));
}

#[test]
fn reject_unknown_topic() {
	let cmd = parse_control_message(P, "neolink/cam1/control/unknown", b"x");
	assert!(cmd.is_none());
}

#[test]
fn reject_malformed_payload() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"not_a_number");
	assert!(cmd.is_none());
}

/// Camera-name validator must be ASCII-only. Pre-fix
/// `c.is_alphanumeric()` was Unicode-permissive — "café" / "中文"
/// passed and silently broke downstream HA discovery (`title_case`
/// is ASCII-only) plus any broker that rejects non-ASCII topics.
#[test]
fn reject_non_ascii_camera_name_in_topic() {
	for cam in ["café", "中文", "frontдвер", "üp"] {
		let topic = format!("neolink/{cam}/control/floodlight");
		let cmd = parse_control_message(P, &topic, b"on");
		assert!(
			cmd.is_none(),
			"non-ASCII camera name {cam:?} must be rejected by the topic parser"
		);
	}
}

// ── C1: PTZ topics use slashes (5-part topics) ────────────────────────

#[test]
fn parse_ptz_preset_5part() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"42");
	assert!(
		matches!(cmd, Some(ControlCommand::PtzPreset { ref camera, preset_id: 42 }) if camera == "cam1")
	);
}

#[test]
fn parse_ptz_assign_5part() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/assign", b"7 Living Room");
	assert!(
		matches!(cmd, Some(ControlCommand::PtzAssign { ref camera, preset_id: 7, ref name }) if camera == "cam1" && name == "Living Room")
	);
}

#[test]
fn parse_query_ptz_preset_5part() {
	let cmd = parse_control_message(P, "neolink/cam1/query/ptz/preset", b"");
	assert!(matches!(cmd, Some(ControlCommand::QueryPtzPreset { ref camera }) if camera == "cam1"));
}

// ── I1: ptz_preset validates 0-255 ────────────────────────────────────

#[test]
fn ptz_preset_valid_u8() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"0");
	assert!(matches!(
		cmd,
		Some(ControlCommand::PtzPreset { preset_id: 0, .. })
	));

	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"255");
	assert!(matches!(
		cmd,
		Some(ControlCommand::PtzPreset { preset_id: 255, .. })
	));
}

#[test]
fn ptz_preset_out_of_range_falls_through_to_name_variant() {
	// "256" doesn't fit u8, so the parser treats it as a preset NAME
	// (HA's discovery select publishes option labels as the payload —
	// see `ControlCommand::PtzPresetByName`). The dispatcher will
	// reject unknown names later via the camera's preset cache.
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"256")
		.expect("valid name-variant");
	match cmd {
		ControlCommand::PtzPresetByName { name, .. } => assert_eq!(name, "256"),
		_ => panic!("expected PtzPresetByName for non-u8 payload"),
	}
}

#[test]
fn ptz_preset_non_numeric_payload_is_a_name() {
	// HA's preset select fires control/ptz/preset with the option
	// label. Non-numeric payloads are valid preset names.
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"Sky")
		.expect("valid name-variant");
	match cmd {
		ControlCommand::PtzPresetByName { name, .. } => assert_eq!(name, "Sky"),
		_ => panic!("expected PtzPresetByName for string payload"),
	}
}

#[test]
fn ptz_preset_filters_reply_tokens() {
	// The dispatcher publishes "OK"/"FAIL" replies onto the same
	// control topic. Without this filter, every reply would re-parse
	// as a `PtzPresetByName{name=OK}` and feed back into the
	// dispatcher, producing an infinite log spam loop. No real
	// preset is named OK or FAIL — drop them at parse time.
	for tok in ["OK", "ok", "Ok", "FAIL", "fail", "Fail"] {
		let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", tok.as_bytes());
		assert!(cmd.is_none(), "reply token {tok} must not parse");
	}
}

#[test]
fn control_topics_filter_reply_tokens() {
	// Every `control/...` topic is also the dispatcher's reply topic
	// (`OK`/`FAIL`). Closed-payload actions (floodlight, led, ir, pir,
	// siren, zoom, wakeup, ptz) reject the reply tokens at the per-arm
	// parser, but `reboot` accepts ANY payload — without the
	// `is_reserved_reply_token` guard at the top of
	// `parse_control_action`, the dispatcher's `OK` reply re-parses as
	// a fresh `Reboot` and an unbounded loop hammers the camera.
	let topics = [
		"neolink/cam1/control/reboot",
		"neolink/cam1/control/floodlight",
		"neolink/cam1/control/led",
		"neolink/cam1/control/ir",
		"neolink/cam1/control/pir",
		"neolink/cam1/control/siren",
		"neolink/cam1/control/wakeup",
	];
	for topic in topics {
		for tok in ["OK", "ok", "FAIL", "fail"] {
			let cmd = parse_control_message(P, topic, tok.as_bytes());
			assert!(cmd.is_none(), "reply token {tok} on {topic} must not parse");
		}
	}
}

#[test]
fn query_topics_filter_reply_tokens() {
	// Every `query/...` topic is also the dispatcher's reply topic
	// (`OK`/`FAIL`). Without this filter, every reply re-parses as a
	// fresh query, dispatches against the camera, publishes another
	// `OK`, and so on — an unbounded self-loop. Drop the reserved
	// reply tokens at the parse step. Verified live against an Argus
	// after the refresh handler made the loop visible.
	let topics = [
		"neolink/cam1/query/battery",
		"neolink/cam1/query/preview",
		"neolink/cam1/query/pir",
		"neolink/cam1/query/ptz/preset",
	];
	for topic in topics {
		for tok in ["OK", "ok", "FAIL", "fail"] {
			let cmd = parse_control_message(P, topic, tok.as_bytes());
			assert!(cmd.is_none(), "reply token {tok} on {topic} must not parse");
		}
	}
}

#[test]
fn ptz_preset_rejects_empty() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"");
	assert!(cmd.is_none());
}

// ── I2: ptz_assign parses {id} {name} ─────────────────────────────────

#[test]
fn ptz_assign_rejects_id_only() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/assign", b"7");
	assert!(cmd.is_none());
}

#[test]
fn ptz_assign_rejects_empty() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/assign", b"");
	assert!(cmd.is_none());
}

#[test]
fn ptz_assign_rejects_bad_id() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/assign", b"999 name");
	assert!(cmd.is_none());
}

#[test]
fn ptz_assign_rejects_empty_name() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/assign", b"7 ");
	assert!(cmd.is_none());
}

// ── I3: Zoom accepts f32 (neolink-compatible scaling) ─────────────────

#[test]
fn zoom_valid_range() {
	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"0");
	assert!(matches!(cmd, Some(ControlCommand::Zoom { level, .. }) if level == 0.0));

	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"1.0");
	assert!(
		matches!(cmd, Some(ControlCommand::Zoom { level, .. }) if (level - 1.0).abs() < f32::EPSILON)
	);

	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"0.5");
	assert!(
		matches!(cmd, Some(ControlCommand::Zoom { level, .. }) if (level - 0.5).abs() < f32::EPSILON)
	);

	// Integer values are still accepted (parsed as f32)
	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"2");
	assert!(
		matches!(cmd, Some(ControlCommand::Zoom { level, .. }) if (level - 2.0).abs() < f32::EPSILON)
	);
}

#[test]
fn zoom_rejects_negative() {
	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"-1");
	assert!(cmd.is_none());
}

#[test]
fn zoom_rejects_non_numeric() {
	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"abc");
	assert!(cmd.is_none());
}

// ── S7: Security tests for MQTT control parsing ──────────────────────

#[test]
fn reject_topic_traversal_attack() {
	let cmd = parse_control_message(P, "neolink/../admin/control/reboot", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_null_bytes_in_camera_name() {
	let cmd = parse_control_message(P, "neolink/cam\x001/control/reboot", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_empty_camera_name() {
	let cmd = parse_control_message(P, "neolink//control/reboot", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_oversized_payload() {
	// Wakeup with a number larger than u32::MAX
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"99999999999");
	assert!(cmd.is_none());
}

#[test]
fn reject_unicode_direction() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", "üp".as_bytes());
	assert!(cmd.is_none());
}

// ── camera_name() accessor ───────────────────────────────────────────

#[test]
fn camera_name_from_floodlight() {
	let cmd = parse_control_message(P, "neolink/garden/control/floodlight", b"on").unwrap();
	assert_eq!(cmd.camera_name(), "garden");
}

#[test]
fn camera_name_from_floodlight_tasks() {
	let cmd = parse_control_message(P, "neolink/deck/control/floodlight_tasks", b"off").unwrap();
	assert_eq!(cmd.camera_name(), "deck");
}

#[test]
fn camera_name_from_led() {
	let cmd = parse_control_message(P, "neolink/garage/control/led", b"on").unwrap();
	assert_eq!(cmd.camera_name(), "garage");
}

#[test]
fn camera_name_from_ir() {
	let cmd = parse_control_message(P, "neolink/backyard/control/ir", b"auto").unwrap();
	assert_eq!(cmd.camera_name(), "backyard");
}

#[test]
fn camera_name_from_pir() {
	let cmd = parse_control_message(P, "neolink/porch/control/pir", b"on").unwrap();
	assert_eq!(cmd.camera_name(), "porch");
}

#[test]
fn camera_name_from_reboot() {
	let cmd = parse_control_message(P, "neolink/shed/control/reboot", b"").unwrap();
	assert_eq!(cmd.camera_name(), "shed");
}

#[test]
fn camera_name_from_ptz() {
	let cmd = parse_control_message(P, "neolink/driveway/control/ptz", b"left").unwrap();
	assert_eq!(cmd.camera_name(), "driveway");
}

#[test]
fn camera_name_from_ptz_preset() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset", b"5").unwrap();
	assert_eq!(cmd.camera_name(), "cam1");
}

#[test]
fn camera_name_from_ptz_assign() {
	let cmd = parse_control_message(P, "neolink/cam2/control/ptz/assign", b"3 Front Door").unwrap();
	assert_eq!(cmd.camera_name(), "cam2");
}

#[test]
fn camera_name_from_zoom() {
	let cmd = parse_control_message(P, "neolink/cam3/control/zoom", b"0.25").unwrap();
	assert_eq!(cmd.camera_name(), "cam3");
}

#[test]
fn camera_name_from_siren() {
	let cmd = parse_control_message(P, "neolink/alarm-cam/control/siren", b"on").unwrap();
	assert_eq!(cmd.camera_name(), "alarm-cam");
}

#[test]
fn camera_name_from_wakeup() {
	let cmd = parse_control_message(P, "neolink/battery_cam/control/wakeup", b"10").unwrap();
	assert_eq!(cmd.camera_name(), "battery_cam");
}

#[test]
fn camera_name_from_query_battery() {
	let cmd = parse_control_message(P, "neolink/front/query/battery", b"").unwrap();
	assert_eq!(cmd.camera_name(), "front");
}

#[test]
fn camera_name_from_query_preview() {
	let cmd = parse_control_message(P, "neolink/side/query/preview", b"").unwrap();
	assert_eq!(cmd.camera_name(), "side");
}

#[test]
fn camera_name_from_query_pir() {
	let cmd = parse_control_message(P, "neolink/back/query/pir", b"").unwrap();
	assert_eq!(cmd.camera_name(), "back");
}

#[test]
fn camera_name_from_query_ptz_preset() {
	let cmd = parse_control_message(P, "neolink/cam5/query/ptz/preset", b"").unwrap();
	assert_eq!(cmd.camera_name(), "cam5");
}

// ── control_topic() for all command types ────────────────────────────

#[test]
fn control_topic_floodlight() {
	let cmd = parse_control_message(P, "neolink/c/control/floodlight", b"on").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/floodlight");
}

#[test]
fn control_topic_floodlight_tasks() {
	let cmd = parse_control_message(P, "neolink/c/control/floodlight_tasks", b"off").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/floodlight_tasks");
}

#[test]
fn control_topic_led() {
	let cmd = parse_control_message(P, "neolink/c/control/led", b"on").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/led");
}

#[test]
fn control_topic_ir() {
	let cmd = parse_control_message(P, "neolink/c/control/ir", b"auto").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/ir");
}

#[test]
fn control_topic_pir() {
	let cmd = parse_control_message(P, "neolink/c/control/pir", b"on").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/pir");
}

#[test]
fn control_topic_reboot() {
	let cmd = parse_control_message(P, "neolink/c/control/reboot", b"").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/reboot");
}

#[test]
fn control_topic_ptz() {
	let cmd = parse_control_message(P, "neolink/c/control/ptz", b"up 10").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/ptz");
}

#[test]
fn control_topic_ptz_preset() {
	let cmd = parse_control_message(P, "neolink/c/control/ptz/preset", b"1").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/ptz/preset");
}

#[test]
fn control_topic_ptz_assign() {
	let cmd = parse_control_message(P, "neolink/c/control/ptz/assign", b"1 Spot").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/ptz/assign");
}

#[test]
fn control_topic_zoom() {
	let cmd = parse_control_message(P, "neolink/c/control/zoom", b"1.5").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/zoom");
}

#[test]
fn control_topic_siren() {
	let cmd = parse_control_message(P, "neolink/c/control/siren", b"on").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/siren");
}

#[test]
fn control_topic_wakeup() {
	let cmd = parse_control_message(P, "neolink/c/control/wakeup", b"10").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/control/wakeup");
}

#[test]
fn control_topic_query_battery() {
	let cmd = parse_control_message(P, "neolink/c/query/battery", b"").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/query/battery");
}

#[test]
fn control_topic_query_preview() {
	let cmd = parse_control_message(P, "neolink/c/query/preview", b"").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/query/preview");
}

#[test]
fn control_topic_query_pir() {
	let cmd = parse_control_message(P, "neolink/c/query/pir", b"").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/query/pir");
}

#[test]
fn control_topic_query_ptz_preset() {
	let cmd = parse_control_message(P, "neolink/c/query/ptz/preset", b"").unwrap();
	assert_eq!(cmd.control_topic(P), "neolink/c/query/ptz/preset");
}

// ── Edge cases: empty and whitespace payloads ────────────────────────

#[test]
fn empty_payload_rejected_for_floodlight() {
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", b"");
	assert!(cmd.is_none());
}

#[test]
fn whitespace_payload_rejected_for_floodlight() {
	let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", b"   ");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_led() {
	let cmd = parse_control_message(P, "neolink/cam1/control/led", b"");
	assert!(cmd.is_none());
}

#[test]
fn whitespace_payload_rejected_for_led() {
	let cmd = parse_control_message(P, "neolink/cam1/control/led", b"  \t ");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_ir() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ir", b"");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_pir() {
	let cmd = parse_control_message(P, "neolink/cam1/control/pir", b"");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_siren() {
	let cmd = parse_control_message(P, "neolink/cam1/control/siren", b"");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_zoom() {
	let cmd = parse_control_message(P, "neolink/cam1/control/zoom", b"");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_wakeup() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_rejected_for_ptz() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", b"");
	assert!(cmd.is_none());
}

#[test]
fn whitespace_payload_rejected_for_ptz() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", b"   ");
	assert!(cmd.is_none());
}

#[test]
fn empty_payload_accepted_for_reboot() {
	// Reboot accepts empty or any payload
	let cmd = parse_control_message(P, "neolink/cam1/control/reboot", b"");
	assert!(cmd.is_some());
}

#[test]
fn whitespace_payload_accepted_for_reboot() {
	let cmd = parse_control_message(P, "neolink/cam1/control/reboot", b"  ");
	assert!(cmd.is_some());
}

#[test]
fn wakeup_rejects_zero_minutes() {
	let cmd = parse_control_message(P, "neolink/cam1/control/wakeup", b"0");
	assert!(cmd.is_none());
}

// ── On/off parsing aliases ──────────────────────────────────────────

#[test]
fn floodlight_accepts_true_1_on() {
	for payload in &[b"true" as &[u8], b"1", b"on"] {
		let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", payload);
		assert!(
			matches!(cmd, Some(ControlCommand::Floodlight { state: true, .. })),
			"payload {:?} should parse as on",
			std::str::from_utf8(payload).unwrap()
		);
	}
}

#[test]
fn floodlight_accepts_false_0_off() {
	for payload in &[b"false" as &[u8], b"0", b"off"] {
		let cmd = parse_control_message(P, "neolink/cam1/control/floodlight", payload);
		assert!(
			matches!(cmd, Some(ControlCommand::Floodlight { state: false, .. })),
			"payload {:?} should parse as off",
			std::str::from_utf8(payload).unwrap()
		);
	}
}

// ── IR mode parsing ─────────────────────────────────────────────────

#[test]
fn ir_on_off_auto() {
	let on = parse_control_message(P, "neolink/cam1/control/ir", b"on");
	assert!(matches!(
		on,
		Some(ControlCommand::Ir {
			mode: bairelay_mqtt::control::IrMode::On,
			..
		})
	));

	let off = parse_control_message(P, "neolink/cam1/control/ir", b"off");
	assert!(matches!(
		off,
		Some(ControlCommand::Ir {
			mode: bairelay_mqtt::control::IrMode::Off,
			..
		})
	));

	let auto = parse_control_message(P, "neolink/cam1/control/ir", b"auto");
	assert!(matches!(
		auto,
		Some(ControlCommand::Ir {
			mode: bairelay_mqtt::control::IrMode::Auto,
			..
		})
	));
}

#[test]
fn ir_rejects_invalid_mode() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ir", b"blink");
	assert!(cmd.is_none());
}

// ── PTZ direction parsing ───────────────────────────────────────────

#[test]
fn ptz_all_directions() {
	for dir in &["up", "down", "left", "right"] {
		let cmd = parse_control_message(P, "neolink/cam1/control/ptz", dir.as_bytes());
		assert!(cmd.is_some(), "direction '{dir}' should be accepted");
	}
}

#[test]
fn ptz_rejects_negative_amount() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz", b"up -5");
	assert!(cmd.is_none());
}

// ── Topic structure edge cases ──────────────────────────────────────

#[test]
fn reject_too_short_topic() {
	let cmd = parse_control_message(P, "neolink/cam1", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_wrong_prefix() {
	let cmd = parse_control_message(P, "other/cam1/control/reboot", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_mismatched_prefix() {
	// Caller runs with prefix "bairelay" but the topic carries the legacy
	// prefix. Must be rejected so a pre-migration retained message cannot
	// trigger a command under the new prefix.
	let cmd = parse_control_message("bairelay", "neolink/cam1/control/reboot", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_unknown_ptz_sub() {
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/unknown", b"1");
	assert!(cmd.is_none());
}

#[test]
fn reject_unknown_query_ptz_sub() {
	let cmd = parse_control_message(P, "neolink/cam1/query/ptz/unknown", b"");
	assert!(cmd.is_none());
}

#[test]
fn reject_unknown_query_action() {
	let cmd = parse_control_message(P, "neolink/cam1/query/unknown", b"");
	assert!(cmd.is_none());
}

// ── Topic edge cases — defensive against MQTT topic injection ─────────
//
// `parse_control_message` validates the camera-name segment to
// [A-Za-z0-9_-] only. The tests below pin that filter against
// pathological topic shapes a hostile or buggy publisher could send.

#[test]
fn reject_null_byte_in_camera_segment() {
	// Embedded NUL inside the camera name must fail validation.
	let cmd = parse_control_message(P, "neolink/cam\x001/control/reboot", b"1");
	assert!(cmd.is_none(), "NUL in camera name must reject");
}

#[test]
fn reject_mqtt_wildcards_as_camera_name() {
	// `+` and `#` are MQTT wildcards. A subscriber that received them
	// as the camera segment is a misconfiguration; a publisher that
	// uses them is hostile. Reject either way.
	assert!(parse_control_message(P, "neolink/+/control/reboot", b"1").is_none());
	assert!(parse_control_message(P, "neolink/#/control/reboot", b"1").is_none());
}

#[test]
fn reject_dotdot_camera_name() {
	// `..` would be a path-traversal attempt if the camera name ever
	// flowed into a filesystem context. The alphanumeric filter blocks
	// it at parse — pin that.
	let cmd = parse_control_message(P, "neolink/../control/reboot", b"1");
	assert!(cmd.is_none(), "../ must reject as camera name");
}

#[test]
fn reject_very_long_camera_name() {
	// 16 KiB camera name — the parser must short-circuit on length /
	// validation, never panic or allocate unbounded.
	let huge_cam = "a".repeat(16 * 1024);
	let topic = format!("neolink/{huge_cam}/control/reboot");
	// 16 KiB of `a` is alphanumeric, so this actually parses — the
	// safety property is "doesn't panic on a giant topic". The
	// downstream dispatcher does its own ACL via the cameras map.
	let _cmd = parse_control_message(P, &topic, b"1");
}

#[test]
fn reject_prefix_mismatch_even_when_topic_starts_with_prefix_chars() {
	// A topic whose first segment merely starts with the prefix string
	// (but isn't equal) must not match — defends against a future
	// switch from `==` to `starts_with`.
	let cmd = parse_control_message(P, "neolinkx/cam1/control/reboot", b"1");
	assert!(cmd.is_none(), "prefix mismatch must reject");
}

#[test]
fn reject_empty_camera_segment() {
	// Double slash collapses the camera segment to "".
	let cmd = parse_control_message(P, "neolink//control/reboot", b"1");
	assert!(cmd.is_none(), "empty camera segment must reject");
}

#[test]
fn reject_extra_path_segments() {
	// Six-segment topic — should fall through to the catch-all None.
	let cmd = parse_control_message(P, "neolink/cam1/control/ptz/preset/extra", b"1");
	assert!(cmd.is_none(), "extra segments must reject");
}

#[test]
fn parse_talk_absolute_path() {
	let cmd = parse_control_message(P, "neolink/cam1/control/talk", b" /srv/chime.wav\n");
	match cmd {
		Some(ControlCommand::Talk { camera, path }) => {
			assert_eq!(camera, "cam1");
			assert_eq!(path, "/srv/chime.wav");
		}
		other => panic!("expected Talk, got {other:?}"),
	}
	let cmd = ControlCommand::Talk {
		camera: "cam1".into(),
		path: "/a.wav".into(),
	};
	assert_eq!(cmd.control_topic(P), "neolink/cam1/control/talk");
	assert_eq!(cmd.camera_name(), "cam1");
}

#[test]
fn talk_rejects_relative_empty_and_reply_tokens() {
	for payload in [&b"chime.wav"[..], b"", b"OK", b"FAIL"] {
		assert!(
			parse_control_message(P, "neolink/cam1/control/talk", payload).is_none(),
			"payload {:?} must not parse",
			String::from_utf8_lossy(payload)
		);
	}
}
