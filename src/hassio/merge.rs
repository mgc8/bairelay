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
			overlay_field!(entry, over, defs, enabled);
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
	if overlay.wake_server.is_some() {
		base.wake_server = overlay.wake_server;
	}
	if overlay.push_listener.is_some() {
		base.push_listener = overlay.push_listener;
	}
	if overlay.stream_prune_grace_secs != defaults.stream_prune_grace_secs {
		base.stream_prune_grace_secs = overlay.stream_prune_grace_secs;
	}

	base
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
			}],
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
}
