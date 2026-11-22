//! Deep-merge a HA-options-derived [`Config`] with a TOML overlay.

use crate::config::{CameraConfig, Config, MqttServerConfig};

pub fn parse_overlay(toml_src: &str) -> Result<Config, String> {
	// `Config::cameras` and `CameraConfig::username` are non-default at the
	// parser level (they're always supplied by users editing the regular
	// `config.toml`), but the HA overlay describes only the deltas the
	// operator wants on top of the form-driven base. Two common cases:
	//
	// - A cameras-free overlay (top-level settings only) — inject an empty
	//   `cameras = []`.
	// - A `[[cameras]]` entry that overrides one field but doesn't restate
	//   `username` — inject `username = ""`. The merge logic preserves
	//   the base's username when the overlay's is empty.
	let mut doc: toml::Value =
		toml::from_str(toml_src).map_err(|e| format!("overlay parse error: {e}"))?;
	if let Some(table) = doc.as_table_mut() {
		table
			.entry("cameras")
			.or_insert_with(|| toml::Value::Array(Vec::new()));
		if let Some(toml::Value::Array(cams)) = table.get_mut("cameras") {
			for cam in cams {
				if let Some(t) = cam.as_table_mut() {
					t.entry("username")
						.or_insert_with(|| toml::Value::String(String::new()));
				}
			}
		}
	}
	doc.try_into()
		.map_err(|e| format!("overlay parse error: {e}"))
}

/// Overlay a single non-`name` field from `over` onto `entry`. The variant
/// suffix picks the override rule appropriate for the field's type.
macro_rules! overlay_field {
	($base:expr, $over:expr, $defs:expr, $field:ident) => {
		if $over.$field != $defs.$field {
			$base.$field = $over.$field.clone();
		}
	};
	($base:expr, $over:expr, $field:ident @opt) => {
		if $over.$field.is_some() {
			$base.$field = $over.$field.clone();
		}
	};
	($base:expr, $over:expr, $field:ident @str_nonempty) => {
		if !$over.$field.is_empty() {
			$base.$field = $over.$field.clone();
		}
	};
	($base:expr, $over:expr, $field:ident @vec_nonempty) => {
		if !$over.$field.is_empty() {
			$base.$field = $over.$field.clone();
		}
	};
}

/// Merge `overlay` camera entries onto `base` by `name`. Existing entries
/// merge field-by-field (overlay wins when it deviates from the
/// per-field default); unmatched overlay entries are appended.
pub fn merge_cameras(base: Vec<CameraConfig>, overlay: Vec<CameraConfig>) -> Vec<CameraConfig> {
	let defs = CameraConfig::default();
	let mut out: Vec<CameraConfig> = base;

	for over in overlay {
		if let Some(idx) = out.iter().position(|b| b.name == over.name) {
			let entry = &mut out[idx];
			// Identity-ish fields (name is the merge key, never overridden).
			overlay_field!(entry, over, address @opt);
			overlay_field!(entry, over, uid @opt);
			overlay_field!(entry, over, username @str_nonempty);
			overlay_field!(entry, over, password @opt);
			// Per-camera knobs.
			overlay_field!(entry, over, defs, channel_id);
			overlay_field!(entry, over, defs, stream);
			overlay_field!(entry, over, defs, discovery);
			overlay_field!(entry, over, defs, max_encryption);
			overlay_field!(entry, over, defs, idle_disconnect);
			overlay_field!(entry, over, idle_disconnect_timeout_secs @opt);
			overlay_field!(entry, over, defs, motion_wake_hold_secs);
			overlay_field!(entry, over, defs, probe_session_secs);
			overlay_field!(entry, over, defs, retry_after_secs);
			overlay_field!(entry, over, defs, enabled);
			overlay_field!(entry, over, defs, webhook);
			// Media pacer tuning (see the pacer keys on `CameraConfig`).
			overlay_field!(entry, over, defs, video_pacer_latency_ms);
			overlay_field!(entry, over, defs, video_pacer_max_lead_ms);
			overlay_field!(entry, over, defs, audio_pacer_latency_ms);
			overlay_field!(entry, over, defs, audio_pacer_max_lead_ms);
			// Whole-struct override: relies on build_base_config leaving
			// per-camera mqtt + pause at default. If HA options ever start
			// populating these, switch to a field-level deep merge or this
			// clobbers them.
			overlay_field!(entry, over, defs, mqtt);
			overlay_field!(entry, over, defs, pause);
			overlay_field!(entry, over, permitted_users @vec_nonempty);
			// Neolink-compat fields (warned + ignored at startup; we keep
			// overlay's intent visible so the warning fires on the merged
			// config rather than disappearing into the overlay).
			overlay_field!(entry, over, debug @opt);
			overlay_field!(entry, over, print_format @opt);
			overlay_field!(entry, over, update_time @opt);
			overlay_field!(entry, over, buffer_duration @opt);
			overlay_field!(entry, over, use_splash @opt);
			overlay_field!(entry, over, splash_pattern @opt);
			overlay_field!(entry, over, max_discovery_retries @opt);
			overlay_field!(entry, over, push_notifications @opt);
			overlay_field!(entry, over, strict @opt);
		} else {
			out.push(over);
		}
	}
	out
}

/// Top-level merge: combines top-level fields + cameras into one [`Config`].
pub fn merge(base: Config, overlay: Config) -> Config {
	let cameras = merge_cameras(base.cameras.clone(), overlay.cameras.clone());
	let mut merged = merge_top_level(base, overlay);
	merged.cameras = cameras;
	merged
}

/// Field-merge two [`MqttServerConfig`]s. The four `Option` cases
/// follow the natural "if-some-else-passthrough" rule; in the both-some
/// case every overlay field that differs from the per-field serde
/// default overrides base. This avoids the whole-struct swap that
/// silently clobbered base's `topic_prefix` when the overlay only set
/// (say) `ca = "/ssl/myca.pem"` — serde would fill `topic_prefix` with
/// `"bairelay"` and the prior swap propagated that default over the
/// HA-form's chosen prefix.
fn merge_mqtt(
	base: Option<MqttServerConfig>,
	overlay: Option<MqttServerConfig>,
) -> Option<MqttServerConfig> {
	match (base, overlay) {
		(None, None) => None,
		(Some(b), None) => Some(b),
		(None, Some(o)) => Some(o),
		(Some(mut b), Some(o)) => {
			let defs = MqttServerConfig::default();
			overlay_field!(b, o, defs, broker_addr);
			overlay_field!(b, o, defs, port);
			overlay_field!(b, o, tls @opt);
			overlay_field!(b, o, defs, topic_prefix);
			overlay_field!(b, o, credentials @opt);
			overlay_field!(b, o, ca @opt);
			overlay_field!(b, o, client_auth @opt);
			overlay_field!(b, o, discovery @opt);
			Some(b)
		}
	}
}

/// Merge top-level fields from `overlay` into `base`. Overlay values
/// that differ from the type's default win; defaults are passthrough.
/// `cameras` is handled separately by [`merge_cameras`].
pub fn merge_top_level(mut base: Config, overlay: Config) -> Config {
	let defaults = Config::default();

	if overlay.bind_addr != defaults.bind_addr {
		base.bind_addr = overlay.bind_addr;
	}
	if overlay.bind_port != defaults.bind_port {
		base.bind_port = overlay.bind_port;
	}
	if overlay.certificate.is_some() {
		base.certificate = overlay.certificate;
	}
	if overlay.tls_bind_port.is_some() {
		base.tls_bind_port = overlay.tls_bind_port;
	}
	if overlay.tls_client_ca.is_some() {
		base.tls_client_ca = overlay.tls_client_ca;
	}
	if overlay.tls_client_auth != defaults.tls_client_auth {
		base.tls_client_auth = overlay.tls_client_auth;
	}
	if !overlay.users.is_empty() {
		base.users = overlay.users;
	}
	base.mqtt = merge_mqtt(base.mqtt, overlay.mqtt);
	base.wake_server = merge_wake_server(base.wake_server, overlay.wake_server);
	if overlay.push_listener.is_some() {
		base.push_listener = overlay.push_listener;
	}
	if overlay.webhook.is_some() {
		base.webhook = overlay.webhook;
	}
	// Whole block, like `[webhook]` just above. The form has no field for the startup
	// scan, so the overlay file is the only place it can come from; and unlike
	// `[wake_server]`, there is no form-side half to merge field by field against, so a
	// field-level merge would have nothing to fall back to. What that costs is stated
	// once, in the note on [`merge_wake_server`]: a block written here cannot switch off
	// something the form turned on, and it cannot inherit a credential the operator
	// wrote in a different block.
	if overlay.startup_discovery.is_some() {
		base.startup_discovery = overlay.startup_discovery;
	}
	if overlay.stream_prune_grace_secs != defaults.stream_prune_grace_secs {
		base.stream_prune_grace_secs = overlay.stream_prune_grace_secs;
	}

	base
}

/// Field-level merge of the `[wake_server]` section.
///
/// The form now fills this section, so the whole-block replace this used to do
/// would have thrown away every rhythm the operator did not bother to repeat:
/// writing one key in the overlay file is not a statement about the other five.
/// Each field is taken from the overlay when it differs from the built-in value,
/// and from the form otherwise.
///
/// One limitation has to be said out loud, because it bites exactly once and
/// that once is at night: with serde defaults there is no way to tell "the
/// operator wrote the built-in value" from "the operator wrote nothing", so a
/// switch the form turned on cannot be turned back off by restating the default
/// in the overlay. The way to turn something off is to leave it off in the form,
/// or to name the value actually wanted. The same limitation already governs
/// `tls_client_auth` above; it comes from defaults, not from this function.
fn merge_wake_server(
	base: Option<bairelay_wake_server::WakeServerConfig>,
	overlay: Option<bairelay_wake_server::WakeServerConfig>,
) -> Option<bairelay_wake_server::WakeServerConfig> {
	use bairelay_wake_server::WakeServerConfig as W;
	let (base, over) = match (base, overlay) {
		(b, None) => return b,
		(None, o) => return o,
		(Some(b), Some(o)) => (b, o),
	};
	let d = W::default();
	Some(W {
		enable: pick(over.enable != d.enable, over.enable, base.enable),
		middleman_port: pick(
			over.middleman_port != d.middleman_port,
			over.middleman_port,
			base.middleman_port,
		),
		register_port: pick(
			over.register_port != d.register_port,
			over.register_port,
			base.register_port,
		),
		heartbeat_ms: pick(
			over.heartbeat_ms != d.heartbeat_ms,
			over.heartbeat_ms,
			base.heartbeat_ms,
		),
		// `None` here means "advertise the cadence above", which is a value of
		// its own, so the form keeps it unless the overlay named one.
		hb_advice_ms: over.hb_advice_ms.or(base.hb_advice_ms),
		stale_after_ms: pick(
			over.stale_after_ms != d.stale_after_ms,
			over.stale_after_ms,
			base.stale_after_ms,
		),
		explain_declared_uid: pick(
			over.explain_declared_uid != d.explain_declared_uid,
			over.explain_declared_uid,
			base.explain_declared_uid,
		),
		bind: base.bind.or(over.bind),
	})
}

/// Take the overlay's value when the operator said something, the base's
/// otherwise. Written once so every field above reads the same way.
fn pick<T>(asked: bool, overlay: T, base: T) -> T {
	if asked {
		overlay
	} else {
		base
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_overlay_with_wake_server() {
		let src = r#"
			bind = "0.0.0.0"
			bind_port = 8554
			cameras = []

			[wake_server]
			enable = true
		"#;
		let cfg = parse_overlay(src).expect("parse ok");
		assert_eq!(cfg.bind_addr, "0.0.0.0");
		assert!(cfg.wake_server.is_some());
	}

	#[test]
	fn rejects_malformed_toml() {
		let src = "not = valid =";
		assert!(parse_overlay(src).is_err());
	}

	#[test]
	fn overlay_without_cameras_defaults_to_empty() {
		// HA-app templates carry only top-level settings + commented examples.
		// The required `cameras` field must be auto-supplied so they parse.
		let src = r#"
			bind = "0.0.0.0"
			[wake_server]
			enable = true
		"#;
		let cfg = parse_overlay(src).expect("parse ok");
		assert!(cfg.cameras.is_empty());
		assert!(cfg.wake_server.is_some());
	}

	#[test]
	fn overlay_camera_without_username_parses() {
		// An operator overrides one per-camera field (e.g. discovery) but
		// doesn't restate `username`. The form base already supplied it;
		// the overlay must parse + leave the base's value untouched.
		let src = r#"
			[[cameras]]
			name = "Cam1"
			discovery = "local"
		"#;
		let cfg = parse_overlay(src).expect("parse ok");
		assert_eq!(cfg.cameras.len(), 1);
		assert_eq!(cfg.cameras[0].name, "Cam1");
		// Empty username is the injected sentinel; merge preserves base.
		assert_eq!(cfg.cameras[0].username, "");
	}

	#[test]
	fn overlay_overrides_top_level_fields() {
		let base = Config {
			bind_addr: "0.0.0.0".into(),
			bind_port: 8554,
			..Config::default()
		};
		let overlay_src = r#"
			bind = "127.0.0.1"
			stream_prune_grace_secs = 60
			cameras = []
		"#;
		let overlay = parse_overlay(overlay_src).unwrap();
		let merged = merge_top_level(base, overlay);
		assert_eq!(merged.bind_addr, "127.0.0.1", "overlay overrides bind_addr");
		assert_eq!(merged.bind_port, 8554, "base bind_port preserved");
		assert_eq!(merged.stream_prune_grace_secs, 60);
	}

	#[test]
	fn camera_overlay_merges_by_name_and_adds_new_entries() {
		use crate::config::CameraConfig;
		let base_cams = vec![CameraConfig {
			name: "Hallway".into(),
			address: Some("ABC123".into()),
			uid: None,
			username: "admin".into(),
			password: Some("secret".into()),
			..CameraConfig::default()
		}];
		let overlay_cams = vec![
			CameraConfig {
				name: "Hallway".into(),
				channel_id: 1,
				..CameraConfig::default()
			},
			CameraConfig {
				name: "Driveway".into(),
				address: Some("192.168.1.50".into()),
				username: "operator".into(),
				password: Some("dr".into()),
				..CameraConfig::default()
			},
		];
		let merged = merge_cameras(base_cams, overlay_cams);
		assert_eq!(merged.len(), 2);
		let hallway = merged.iter().find(|c| c.name == "Hallway").unwrap();
		assert_eq!(
			hallway.address.as_deref(),
			Some("ABC123"),
			"base address preserved"
		);
		assert_eq!(hallway.channel_id, 1, "overlay channel applied");
		assert_eq!(
			hallway.password.as_deref(),
			Some("secret"),
			"base password preserved"
		);
		let driveway = merged.iter().find(|c| c.name == "Driveway").unwrap();
		assert_eq!(driveway.address.as_deref(), Some("192.168.1.50"));
		assert_eq!(driveway.username, "operator");
	}

	#[test]
	fn webhook_overlay_applies_block_and_camera_opt_in() {
		let base = parse_overlay(
			r#"
			[[cameras]]
			name = "cam1"
			username = "admin"
			password = "x"
			address = "192.0.2.10"
		"#,
		)
		.unwrap();
		let overlay = parse_overlay(
			r#"
			[webhook]
			enable = true
			webhook_port = 9090

			[[cameras]]
			name = "cam1"
			webhook = true
		"#,
		)
		.unwrap();
		let merged = merge(base, overlay);
		assert_eq!(merged.webhook.as_ref().unwrap().webhook_port, 9090);
		assert!(merged.cameras[0].webhook, "per-camera opt-in merges");
		crate::config::validate_config(&merged).expect("merged config validates");
	}

	#[test]
	fn camera_overlay_pause_block_overrides_base() {
		use crate::config::{CameraConfig, PauseConfig};
		let base_cams = vec![CameraConfig {
			name: "Hallway".into(),
			address: Some("ABC123".into()),
			username: "admin".into(),
			password: Some("secret".into()),
			..CameraConfig::default()
		}];
		let custom_pause = PauseConfig {
			gap_threshold_secs: 5.0,
			..PauseConfig::default()
		};
		let overlay_cams = vec![CameraConfig {
			name: "Hallway".into(),
			pause: custom_pause.clone(),
			..CameraConfig::default()
		}];
		let merged = merge_cameras(base_cams, overlay_cams);
		assert_eq!(merged[0].pause.gap_threshold_secs, 5.0);
		assert_eq!(merged[0].address.as_deref(), Some("ABC123"));
	}

	#[test]
	fn end_to_end_merge_validates_clean() {
		use crate::hassio::options::{
			build_base_config, HassioCamera, HassioOptions, MqttServiceFlags,
		};

		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![HassioCamera {
				name: "Hallway".into(),
				address: Some("192.168.1.50".into()),
				uid: None,
				username: "admin".into(),
				password: "secret".into(),
				idle_disconnect: true,
				..Default::default()
			}],
		..Default::default()
		};
		let mqtt = MqttServiceFlags {
			host: Some("core-mosquitto".into()),
			port: Some(1883),
			username: Some("addons".into()),
			password: Some("pw".into()),
			ssl: false,
		};
		let overlay = parse_overlay(
			r#"
			cameras = []

			[wake_server]
			enable = true
		"#,
		)
		.unwrap();

		let base = build_base_config(&opts, &mqtt);
		let merged = merge(base, overlay);
		assert!(merged.wake_server.is_some());
		assert!(
			merged.wake_server.as_ref().unwrap().enable,
			"overlay enable round-trips"
		);
		assert_eq!(merged.cameras.len(), 1);

		crate::config::validate_config(&merged).expect("merged config validates");
	}

	#[test]
	fn overlay_mqtt_block_preserves_base_topic_prefix() {
		// Regression: an overlay [mqtt] block specifying only ca = "..."
		// used to clobber the base's topic_prefix back to "bairelay" via
		// whole-struct override. Field-merge: overlay-set fields override;
		// overlay-default fields pass the base through.
		use crate::config::MqttServerConfig;
		let base = Config {
			mqtt: Some(MqttServerConfig {
				broker_addr: "broker.example".into(),
				port: 1883,
				credentials: None,
				tls: None,
				ca: None,
				client_auth: None,
				topic_prefix: "neolink".into(),
				discovery: None,
			}),
			..Config::default()
		};
		// broker_addr is a required serde field — operator must restate it
		// when adding any [mqtt] overlay block. Same value as base here so
		// the test isolates the topic_prefix preservation behaviour.
		let overlay_src = r#"
			cameras = []

			[mqtt]
			broker_addr = "broker.example"
			ca = "/ssl/myca.pem"
		"#;
		let overlay = parse_overlay(overlay_src).unwrap();
		let merged = merge_top_level(base, overlay);
		let m = merged.mqtt.as_ref().expect("mqtt merged");
		assert_eq!(
			m.topic_prefix, "neolink",
			"overlay should not clobber base topic_prefix"
		);
		assert_eq!(
			m.ca.as_deref(),
			Some("/ssl/myca.pem"),
			"overlay ca overrides"
		);
		assert_eq!(
			m.broker_addr, "broker.example",
			"base broker_addr preserved"
		);
	}

	#[test]
	fn overlay_mqtt_overlay_only_path_still_works() {
		// When base has no mqtt at all (Supervisor MQTT integration absent
		// and HA options form left it empty), overlay's [mqtt] is the sole
		// source.
		let base = Config {
			mqtt: None,
			..Config::default()
		};
		let overlay_src = r#"
			cameras = []

			[mqtt]
			broker_addr = "broker.example"
			port = 8883
			topic_prefix = "custom"
		"#;
		let overlay = parse_overlay(overlay_src).unwrap();
		let merged = merge_top_level(base, overlay);
		let m = merged.mqtt.as_ref().expect("overlay-only mqtt");
		assert_eq!(m.broker_addr, "broker.example");
		assert_eq!(m.port, 8883);
		assert_eq!(m.topic_prefix, "custom");
	}

	#[test]
	fn empty_overlay_preserves_base() {
		let base = Config {
			bind_addr: "127.0.0.1".into(),
			bind_port: 9000,
			stream_prune_grace_secs: 45,
			..Config::default()
		};
		let overlay = parse_overlay("cameras = []").expect("empty parses");
		let merged = merge_top_level(base, overlay);
		assert_eq!(merged.bind_addr, "127.0.0.1");
		assert_eq!(merged.bind_port, 9000);
		assert_eq!(merged.stream_prune_grace_secs, 45);
	}

	/// The form has to reach the config, or the add-on is a decoration.
	#[test]
	fn the_form_rhythms_reach_the_config() {
		use crate::hassio::options::{
			build_base_config, HassioCadence, HassioCamera, HassioOptions, MqttServiceFlags,
		};
		let opts = HassioOptions {
			wake_server: Some(HassioCadence {
				enable: Some(true),
				heartbeat_ms: Some(60000),
				stale_after_ms: Some(150000),
				hb_advice_ms: Some(10000),
				explain_declared_uid: Some(true),
			}),
			cameras: vec![HassioCamera {
				name: "Hallway".into(),
				password: "secret".into(),
				idle_disconnect_timeout_secs: Some(60.0),
				probe_session_secs: Some(7),
				motion_wake_hold_secs: Some(12.0),
				discovery: Some("local".into()),
				..Default::default()
			}],
			..Default::default()
		};
		let cfg = build_base_config(&opts, &MqttServiceFlags::default());
		let ws = cfg.wake_server.as_ref().expect("the group reaches the config");
		assert!(ws.enable);
		assert_eq!(ws.heartbeat_ms, 60000);
		assert_eq!(ws.stale_after_ms, 150000);
		assert_eq!(ws.hb_advice_ms, Some(10000));
		assert!(ws.explain_declared_uid);
		let cam = &cfg.cameras[0];
		assert_eq!(cam.idle_disconnect_timeout_secs, Some(60.0));
		assert_eq!(cam.probe_session_secs, 7);
		assert_eq!(cam.motion_wake_hold_secs, 12.0);
		assert_eq!(cam.discovery, crate::config::DiscoveryMethod::Local);
	}

	/// A blank field is not a decision, and an absent group is not either: both
	/// have to leave today's behaviour untouched, which is the only reason these
	/// keys can be added without touching one existing installation.
	#[test]
	fn nothing_asked_for_changes_nothing() {
		use crate::hassio::options::{
			build_base_config, HassioCamera, HassioOptions, MqttServiceFlags,
		};
		let cfg = build_base_config(
			&HassioOptions {
				cameras: vec![HassioCamera {
					name: "Hallway".into(),
					password: "secret".into(),
					..Default::default()
				}],
				..Default::default()
			},
			&MqttServiceFlags::default(),
		);
		assert!(cfg.wake_server.is_none(), "no group, no wake server");
		let cam = &cfg.cameras[0];
		let defs = crate::config::CameraConfig::default();
		assert_eq!(cam.probe_session_secs, defs.probe_session_secs);
		assert_eq!(cam.motion_wake_hold_secs, defs.motion_wake_hold_secs);
		assert_eq!(cam.idle_disconnect_timeout_secs, None);
		assert_eq!(cam.discovery, defs.discovery);
	}

	/// The operator's file always wins, and wins key by key: restating one
	/// rhythm does not silently undo the ones the form set.
	#[test]
	fn the_operator_file_wins_and_the_rest_survives() {
		use crate::hassio::options::{
			build_base_config, HassioCadence, HassioCamera, HassioOptions, MqttServiceFlags,
		};
		let opts = HassioOptions {
			wake_server: Some(HassioCadence {
				enable: Some(true),
				heartbeat_ms: Some(60000),
				stale_after_ms: Some(150000),
				..Default::default()
			}),
			cameras: vec![HassioCamera {
				name: "Hallway".into(),
				password: "secret".into(),
				probe_session_secs: Some(7),
				..Default::default()
			}],
			..Default::default()
		};
		let base = build_base_config(&opts, &MqttServiceFlags::default());
		let overlay = parse_overlay(
			r#"
			[wake_server]
			stale_after_ms = 240000

			[[cameras]]
			name = "Hallway"
			probe_session_secs = 20
			"#,
		)
		.unwrap();
		// merge, not merge_top_level: per-camera rhythms are handled apart.
		let merged = merge(base, overlay);
		let ws = merged
			.wake_server
			.as_ref()
			.expect("the section survives the merge");
		assert_eq!(ws.stale_after_ms, 240000, "the operator's number wins");
		assert_eq!(ws.heartbeat_ms, 60000, "the form's other key survives it");
		assert!(ws.enable, "and so does the switch the form asked for");
		assert_eq!(
			merged.cameras[0].probe_session_secs,
			20,
			"per-camera rhythms merge the same way"
		);
	}

	/// The cap the manifest promises has to be in the code too, because a schema
	/// is only as strong as the mapping behind it.
	#[test]
	fn the_form_cannot_ask_for_a_scope_that_leaves_the_network() {
		use crate::config::DiscoveryMethod as D;
		use crate::hassio::options::discovery_from_form;
		assert_eq!(discovery_from_form(Some("local")), Some(D::Local));
		assert_eq!(discovery_from_form(Some("none")), Some(D::None));
		for asked in ["relay", "remote", "map", "cellular", "cloud", "", "  "] {
			assert_eq!(
				discovery_from_form(Some(asked)),
				None,
				"{asked} must not be reachable from the form"
			);
		}
		assert_eq!(discovery_from_form(None), None);
	}

	/// The startup scan can only be armed from the operator's file, and arming it must
	/// not disturb anything the form filled. The reverse half matters just as much: an
	/// overlay that says nothing about the mode leaves the rendered config exactly as it
	/// was, which is what "off by default" has to mean for the add-on too — the add-on's
	/// rendered file is generated, so a field that appeared in it whenever the merge ran
	/// would be a mode switched on by the packaging rather than by the operator.
	#[test]
	fn the_startup_scan_survives_the_merge_and_its_absence_does_nothing() {
		use crate::hassio::options::{
			build_base_config, HassioCamera, HassioOptions, MqttServiceFlags,
		};
		let opts = HassioOptions {
			cameras: vec![HassioCamera {
				name: "Hallway".into(),
				password: "secret".into(),
				..Default::default()
			}],
			..Default::default()
		};

		let without = merge(
			build_base_config(&opts, &MqttServiceFlags::default()),
			parse_overlay("bind_port = 8555\n").unwrap(),
		);
		assert!(
			without.startup_discovery.is_none(),
			"an overlay that does not mention the mode leaves it out"
		);

		let armed = merge(
			build_base_config(&opts, &MqttServiceFlags::default()),
			parse_overlay(
				r#"
				bind_port = 8555

				[startup_discovery]
				enable = true
				wait_secs = 8
				username = "admin"
				password = "shared-lan-password"
				"#,
			)
			.unwrap(),
		);
		let scan = armed
			.startup_discovery
			.as_ref()
			.expect("the operator's file arms the scan");
		assert!(scan.enable);
		assert_eq!(scan.wait_secs, 8);
		assert_eq!(scan.username.as_deref(), Some("admin"));
		assert_eq!(scan.password.as_deref(), Some("shared-lan-password"));
		assert_eq!(armed.cameras.len(), 1, "the form's camera is still there");
		assert_eq!(armed.bind_port, 8555, "and so is the rest of the overlay");
	}
}
