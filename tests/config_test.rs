use bairelay::config::{
	parse_config, resolve_idle_disconnect_timeout, test_helpers, validate_config, CameraConfig,
	Config, DiscoveryMethod, MaxEncryption, MqttConfig, PauseConfig, StreamConfig, TlsClientAuth,
};

// ── Enum variant deserialization ──────────────────────────────────────

#[test]
fn tls_client_auth_request() {
	let toml_str = r#"
		tls_client_auth = "request"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.tls_client_auth, TlsClientAuth::Request);
}

#[test]
fn tls_client_auth_require() {
	let toml_str = r#"
		tls_client_auth = "require"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.tls_client_auth, TlsClientAuth::Require);
}

#[test]
fn tls_client_auth_default_is_none() {
	let config = Config::default();
	assert_eq!(config.tls_client_auth, TlsClientAuth::None);
}

#[test]
fn tls_client_auth_required_alias_neolink_compat() {
	// Neolink writes `"required"`; bairelay accepts that as a serde alias
	// for Require so a copy-pasted neolink config Just Works.
	let toml_str = r#"
		tls_client_auth = "required"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.tls_client_auth, TlsClientAuth::Require);
}

fn cfg_with_cam() -> Config {
	Config {
		cameras: vec![test_helpers::minimal_camera_config("cam")],
		..Default::default()
	}
}

#[test]
fn validate_rejects_bind_port_zero_when_no_tls() {
	let config = Config {
		bind_port: 0,
		..cfg_with_cam()
	};
	let err = validate_config(&config).expect_err("must reject");
	assert!(err.contains("bind_port"), "got: {err}");
}

#[test]
fn validate_allows_bind_port_zero_when_certificate_set() {
	let config = Config {
		bind_port: 0,
		certificate: Some("/tmp/cert.pem".into()),
		..cfg_with_cam()
	};
	validate_config(&config).expect("TLS-only mode must validate");
}

#[test]
fn validate_rejects_request_without_client_ca() {
	let config = Config {
		certificate: Some("/tmp/cert.pem".into()),
		tls_client_auth: TlsClientAuth::Request,
		..cfg_with_cam()
	};
	let err = validate_config(&config).expect_err("must reject");
	assert!(err.contains("tls_client_ca"), "got: {err}");
}

#[test]
fn validate_rejects_require_without_client_ca() {
	let config = Config {
		certificate: Some("/tmp/cert.pem".into()),
		tls_client_auth: TlsClientAuth::Require,
		..cfg_with_cam()
	};
	let err = validate_config(&config).expect_err("must reject");
	assert!(err.contains("tls_client_ca"), "got: {err}");
}

#[test]
fn validate_rejects_client_auth_without_certificate() {
	let config = Config {
		tls_client_auth: TlsClientAuth::Require,
		tls_client_ca: Some("/tmp/ca.pem".into()),
		..cfg_with_cam()
	};
	let err = validate_config(&config).expect_err("must reject");
	assert!(err.contains("certificate"), "got: {err}");
}

#[test]
fn validate_rejects_same_port_for_plain_and_tls() {
	let config = Config {
		bind_port: 8554,
		certificate: Some("/tmp/cert.pem".into()),
		tls_bind_port: Some(8554),
		..cfg_with_cam()
	};
	let err = validate_config(&config).expect_err("must reject");
	assert!(err.contains("must differ"), "got: {err}");
}

#[test]
fn validate_accepts_distinct_plain_and_tls_ports() {
	let config = Config {
		bind_port: 8554,
		certificate: Some("/tmp/cert.pem".into()),
		tls_bind_port: Some(8555),
		..cfg_with_cam()
	};
	validate_config(&config).expect("dual port must validate");
}

#[test]
fn pause_config_new_shape_roundtrips() {
	let toml = r#"
		bridge_gaps = true
		gap_threshold_secs = 2.5
		preview_overlay = false
	"#;
	let cfg: PauseConfig = toml::from_str(toml).expect("parse");
	assert!(cfg.bridge_gaps);
	assert!((cfg.gap_threshold_secs - 2.5).abs() < f64::EPSILON);
	assert!(!cfg.preview_overlay);
}

#[test]
fn pause_config_defaults() {
	let cfg: PauseConfig = toml::from_str("").expect("parse");
	assert!(cfg.bridge_gaps);
	assert!((cfg.gap_threshold_secs - 3.0).abs() < f64::EPSILON);
	assert!(cfg.preview_overlay);
}

#[test]
fn pause_config_truly_unknown_field_rejected() {
	let toml = r#"bogus_field = true"#;
	let err = toml::from_str::<PauseConfig>(toml).expect_err("must reject");
	assert!(err.to_string().contains("unknown field"));
}

// Convenience for the resolve_* tests: the configured prune grace
// most production paths see is 30 s. None of the assertions in this
// block exercise the floor itself (see the `_floor_*` tests below),
// so 30 s is just a stable input.
const PRUNE_30: std::time::Duration = std::time::Duration::from_secs(30);

#[test]
fn resolve_idle_disconnect_timeout_default_is_45s() {
	// Default raised from 30 → 45 s so it sits strictly above the
	// 30 s `stream_prune_grace_secs` default. See the invariant on
	// `Config::stream_prune_grace_secs` for the reasoning.
	let cam = test_helpers::minimal_camera_config("c");
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(45)
	);
}

#[test]
fn resolve_idle_disconnect_timeout_honours_explicit_field() {
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.idle_disconnect_timeout_secs = Some(90.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(90)
	);
}

#[test]
fn resolve_idle_disconnect_timeout_falls_back_to_pause_timeout() {
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.pause.timeout = Some(45.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(45)
	);
}

#[test]
fn resolve_idle_disconnect_timeout_explicit_wins_over_pause_alias() {
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.idle_disconnect_timeout_secs = Some(60.0);
	cam.pause.timeout = Some(10.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(60)
	);
}

#[test]
fn resolve_idle_disconnect_floor_clamps_when_shorter_than_prune() {
	// Operator misconfig: idle (10 s) < prune (30 s). Clamp to
	// prune + 15 s = 45 s so the cached StreamSource cannot outlive
	// the Baichuan session that feeds it.
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.idle_disconnect_timeout_secs = Some(10.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(45)
	);
}

#[test]
fn resolve_idle_disconnect_floor_does_not_clamp_at_boundary() {
	// Boundary: idle == prune. No clamp — at the moment the source
	// is pruned, the camera also disconnects, so the windows just
	// touch instead of overlapping.
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.idle_disconnect_timeout_secs = Some(30.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, PRUNE_30),
		std::time::Duration::from_secs(30)
	);
}

#[test]
fn resolve_idle_disconnect_floor_disabled_when_prune_is_zero() {
	// `prune_grace = 0` (e.g. test default, or operator-disabled
	// caching) means no `configured < prune_grace` value exists, so
	// the floor never fires. Lets the camera's grace-period unit
	// tests use sub-second timeouts under paused tokio time.
	let mut cam = test_helpers::minimal_camera_config("c");
	cam.idle_disconnect_timeout_secs = Some(1.0);
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, std::time::Duration::ZERO),
		std::time::Duration::from_secs_f64(1.0)
	);
}

#[test]
fn resolve_idle_disconnect_floor_with_large_prune_picks_safe_floor() {
	// Operator picks a long prune cache (120 s) but leaves idle
	// at the default 45 s. 45 < 120 so we clamp to 120 + 15 = 135 s.
	let cam = test_helpers::minimal_camera_config("c");
	assert_eq!(
		resolve_idle_disconnect_timeout(&cam, std::time::Duration::from_secs(120)),
		std::time::Duration::from_secs(135)
	);
}

#[test]
fn pause_config_neolink_compat_fields_parse() {
	// Neolink migration: old fields must parse cleanly (no
	// deny_unknown_fields error) so existing configs still load.
	// Warnings are emitted at startup via warn_deprecated_pause_fields;
	// here we only assert the parse survives.
	let toml = r#"
		on_motion = true
		on_client = true
		on_disconnect = false
		motion_timeout = 2.5
		mode = "still"
		timeout = 45.0
	"#;
	let cfg: PauseConfig = toml::from_str(toml).expect("parse");
	assert_eq!(cfg.on_motion, Some(true));
	assert_eq!(cfg.on_client, Some(true));
	assert_eq!(cfg.on_disconnect, Some(false));
	assert_eq!(cfg.motion_timeout, Some(2.5));
	assert_eq!(cfg.mode.as_deref(), Some("still"));
	assert_eq!(cfg.timeout, Some(45.0));
	// Defaults for the real fields stay intact.
	assert!(cfg.bridge_gaps);
	assert_eq!(cfg.gap_threshold_secs, 3.0);
	assert!(cfg.preview_overlay);
}

#[test]
fn stream_config_none() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		stream = "none"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].stream, StreamConfig::None);
}

#[test]
fn stream_config_main() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		stream = "main"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].stream, StreamConfig::Main);
}

#[test]
fn stream_config_sub() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		stream = "sub"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].stream, StreamConfig::Sub);
}

#[test]
fn stream_config_extern() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		stream = "extern"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].stream, StreamConfig::Extern);
}

#[test]
fn stream_config_default_is_all() {
	let config = CameraConfig::default();
	assert_eq!(config.stream, StreamConfig::All);
}

#[test]
fn discovery_method_local() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		discovery = "local"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].discovery, DiscoveryMethod::Local);
}

#[test]
fn discovery_method_remote() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		discovery = "remote"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].discovery, DiscoveryMethod::Remote);
}

#[test]
fn discovery_method_map() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		discovery = "map"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].discovery, DiscoveryMethod::Map);
}

#[test]
fn discovery_method_cellular() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		discovery = "cellular"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].discovery, DiscoveryMethod::Cellular);
}

#[test]
fn discovery_method_default_is_relay() {
	let config = CameraConfig::default();
	assert_eq!(config.discovery, DiscoveryMethod::Relay);
}

#[test]
fn max_encryption_none() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		max_encryption = "none"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].max_encryption, MaxEncryption::None);
}

#[test]
fn max_encryption_bcencrypt() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		max_encryption = "bcencrypt"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert_eq!(config.cameras[0].max_encryption, MaxEncryption::BcEncrypt);
}

#[test]
fn max_encryption_default_is_aes() {
	let config = CameraConfig::default();
	assert_eq!(config.max_encryption, MaxEncryption::Aes);
}

// ── Validation edge cases ─────────────────────────────────────────────

#[test]
fn accept_camera_name_with_hyphens_and_underscores() {
	let toml_str = r#"
		[[cameras]]
		name = "front-door_cam-2"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_ok());
}

#[test]
fn reject_preview_update_below_500() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		[cameras.mqtt]
		preview_update = 499
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("preview_update"),
		"error should mention preview_update, got: {}",
		err
	);
}

#[test]
fn reject_floodlight_update_below_500() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		[cameras.mqtt]
		floodlight_update = 100
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("floodlight_update"),
		"error should mention floodlight_update, got: {}",
		err
	);
}

#[test]
fn accept_camera_with_uid_no_address() {
	let toml_str = r#"
		[[cameras]]
		name = "battery_cam"
		username = "admin"
		password = "pass"
		uid = "ABCDEF0123456789"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(config.cameras[0].address.is_none());
	assert!(config.cameras[0].uid.is_some());
	assert!(validate_config(&config).is_ok());
}

#[test]
fn parse_config_error_on_invalid_toml() {
	let result = parse_config("this is not valid toml [[[");
	assert!(result.is_err());
	let err = result.unwrap_err();
	assert!(
		err.contains("Failed to parse config"),
		"error should contain 'Failed to parse config', got: {}",
		err
	);
}

#[test]
fn mqtt_config_default_values() {
	let mqtt = MqttConfig::default();
	assert!(mqtt.enable_motion);
	assert!(mqtt.enable_light);
	assert!(mqtt.enable_battery);
	assert_eq!(mqtt.battery_update, 2000);
	assert!(mqtt.enable_preview);
	assert_eq!(mqtt.preview_update, 2000);
	assert!(!mqtt.enable_floodlight);
	assert_eq!(mqtt.floodlight_update, 2000);
	assert!(!mqtt.enable_pir);
}

#[test]
fn config_default_values() {
	let config = Config::default();
	assert_eq!(config.bind_addr, "0.0.0.0");
	assert_eq!(config.bind_port, 8554);
	assert!(config.certificate.is_none());
	assert_eq!(config.tls_client_auth, TlsClientAuth::None);
	assert!(config.users.is_empty());
	assert!(config.mqtt.is_none());
	assert_eq!(config.stream_prune_grace_secs, 30);
	assert!(config.cameras.is_empty());
}

#[test]
fn camera_config_default_values() {
	let cam = CameraConfig::default();
	assert!(cam.name.is_empty());
	assert!(cam.address.is_none());
	assert!(cam.uid.is_none());
	assert!(cam.username.is_empty());
	assert!(cam.password.is_none());
	assert_eq!(cam.channel_id, 0);
	assert_eq!(cam.stream, StreamConfig::All);
	assert_eq!(cam.discovery, DiscoveryMethod::Relay);
	assert_eq!(cam.max_encryption, MaxEncryption::Aes);
	assert!(!cam.idle_disconnect);
	assert!(cam.enabled);
}

// ── Existing tests ────────────────────────────────────────────────────

#[test]
fn parse_minimal_config() {
	let toml_str = r#"
[[cameras]]
name = "front_door"
username = "admin"
password = "secret"
address = "192.168.1.100:9000"
"#;
	let config = parse_config(toml_str).expect("should parse minimal config");
	assert_eq!(config.bind_addr, "0.0.0.0");
	assert_eq!(config.bind_port, 8554);
	assert_eq!(config.cameras.len(), 1);
	assert_eq!(config.cameras[0].name, "front_door");
	assert_eq!(config.cameras[0].username, "admin");
	assert_eq!(config.cameras[0].password, Some("secret".to_string()));
	assert_eq!(
		config.cameras[0].address,
		Some("192.168.1.100:9000".to_string())
	);
	assert!(config.mqtt.is_none());
	assert!(config.users.is_empty());
	validate_config(&config).expect("minimal config should be valid");
}

#[test]
fn parse_full_config_with_mqtt() {
	let toml_str = r#"
bind = "192.168.1.10"
bind_port = 9000

[mqtt]
broker_addr = "192.168.1.50"
port = 1883

[[cameras]]
name = "backyard"
uid = "ABCDEF1234567890"
username = "admin"
password = "password123"
channel_id = 2
stream = "main"
idle_disconnect = true
max_encryption = "none"
discovery = "local"

[cameras.mqtt]
enable_motion = true
enable_light = false
battery_update = 3000

[cameras.pause]
bridge_gaps = false
gap_threshold_secs = 2.0
preview_overlay = false
"#;
	let config = parse_config(toml_str).expect("should parse full config");
	assert_eq!(config.bind_addr, "192.168.1.10");
	assert_eq!(config.bind_port, 9000);

	let mqtt_server = config.mqtt.as_ref().expect("mqtt should be present");
	assert_eq!(mqtt_server.broker_addr, "192.168.1.50");
	assert_eq!(mqtt_server.port, 1883);

	let cam = &config.cameras[0];
	assert_eq!(cam.name, "backyard");
	assert_eq!(cam.uid, Some("ABCDEF1234567890".to_string()));
	assert!(cam.address.is_none());
	assert_eq!(cam.channel_id, 2);
	assert!(cam.idle_disconnect);
	assert!(!cam.mqtt.enable_light);
	assert_eq!(cam.mqtt.battery_update, 3000);
	assert!(!cam.pause.bridge_gaps);
	assert!((cam.pause.gap_threshold_secs - 2.0).abs() < f64::EPSILON);
	assert!(!cam.pause.preview_overlay);

	validate_config(&config).expect("full config should be valid");
}

#[test]
fn reject_camera_without_address_or_uid() {
	let toml_str = r#"
[[cameras]]
name = "orphan"
username = "admin"
password = "test"
"#;
	let config = parse_config(toml_str).expect("should parse");
	let result = validate_config(&config);
	assert!(result.is_err());
	let err = result.unwrap_err();
	assert!(
		err.contains("address") || err.contains("uid"),
		"error should mention address or uid, got: {}",
		err
	);
}

#[test]
fn reject_empty_camera_name() {
	let toml_str = r#"
[[cameras]]
name = ""
username = "admin"
password = "test"
address = "192.168.1.1:9000"
"#;
	let config = parse_config(toml_str).expect("should parse");
	let result = validate_config(&config);
	assert!(result.is_err());
	let err = result.unwrap_err();
	assert!(
		err.contains("empty") || err.contains("name"),
		"error should mention empty name, got: {}",
		err
	);
}

#[test]
fn reject_duplicate_camera_names() {
	let toml_str = r#"
[[cameras]]
name = "front"
username = "admin"
password = "test"
address = "192.168.1.1:9000"

[[cameras]]
name = "front"
username = "admin"
password = "test"
address = "192.168.1.2:9000"
"#;
	let config = parse_config(toml_str).expect("should parse");
	let result = validate_config(&config);
	assert!(result.is_err());
	let err = result.unwrap_err();
	assert!(
		err.contains("duplicate") || err.contains("Duplicate"),
		"error should mention duplicate, got: {}",
		err
	);
}

#[test]
fn reject_bind_port_zero() {
	let toml_str = r#"
        bind_port = 0
        [[cameras]]
        name = "test"
        username = "admin"
        password = "pass"
        address = "192.168.1.1:9000"
    "#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let result = validate_config(&config);
	assert!(result.is_err());
}

#[test]
fn reject_battery_update_below_minimum() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		[cameras.mqtt]
		battery_update = 499
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_err());
}

#[test]
fn accept_battery_update_at_minimum() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "pass"
		address = "192.168.1.1:9000"
		[cameras.mqtt]
		battery_update = 500
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_ok());
}

#[test]
fn reject_camera_name_with_special_chars() {
	let toml_str = r#"
		[[cameras]]
		name = "front door"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let result = validate_config(&config);
	assert!(result.is_err());
	let err = result.unwrap_err();
	assert!(
		err.contains("alphanumeric"),
		"error should mention alphanumeric constraint, got: {}",
		err
	);
}

#[test]
fn accept_camera_name_with_valid_chars() {
	let toml_str = r#"
		[[cameras]]
		name = "front-door_2"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_ok());
}

#[test]
fn reject_camera_without_password() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_err());
}

#[test]
fn permitted_users_unknown_user_is_rejected() {
	let toml_str = r#"
		[[users]]
		name = "alice"
		pass = "secret"

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
		permitted_users = ["alice", "ghost"]
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("ghost") && err.contains("permitted_users"),
		"error should mention the unknown user and permitted_users, got: {}",
		err
	);
}

#[test]
fn permitted_users_known_user_is_accepted() {
	let toml_str = r#"
		[[users]]
		name = "alice"
		pass = "secret"

		[[users]]
		name = "bob"
		pass = "hunter2"

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
		permitted_users = ["alice", "bob"]
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	assert!(validate_config(&config).is_ok());
}

#[test]
fn unknown_field_on_config_is_rejected() {
	let toml_str = r#"
		# Typo: bindaddr instead of bind. Should fail with deny_unknown_fields.
		bindaddr = "0.0.0.0"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let err = parse_config(toml_str).unwrap_err();
	assert!(
		err.contains("bindaddr") || err.contains("unknown"),
		"expected unknown-field error, got: {err}"
	);
}

#[test]
fn unknown_field_on_camera_is_rejected() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
		# Typo: sub_strea instead of stream.
		sub_strea = "main"
	"#;
	let err = parse_config(toml_str).unwrap_err();
	assert!(
		err.contains("sub_strea") || err.contains("unknown"),
		"expected unknown-field error, got: {err}"
	);
}

#[test]
fn empty_user_password_is_rejected() {
	let toml_str = r#"
		[[users]]
		name = "alice"
		pass = ""

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("empty password") && err.contains("alice"),
		"expected empty-password error referencing alice, got: {err}"
	);
}

#[test]
fn user_without_pass_field_is_rejected_at_validation() {
	// `pass` defaults to empty (see UserConfig); absence is treated as empty
	// and should therefore be rejected.
	let toml_str = r#"
		[[users]]
		name = "alice"

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(err.contains("empty password"), "got: {err}");
}

#[test]
fn duplicate_user_names_are_rejected() {
	let toml_str = r#"
		[[users]]
		name = "alice"
		pass = "x"

		[[users]]
		name = "alice"
		pass = "y"

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("Duplicate user name") && err.contains("alice"),
		"got: {err}"
	);
}

#[test]
fn non_empty_user_password_is_accepted() {
	let toml_str = r#"
		[[users]]
		name = "alice"
		pass = "hunter2"

		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config: Config = toml::from_str(toml_str).unwrap();
	validate_config(&config).expect("valid");
}

// ── mqtt.topic_prefix knob ─────────────────────────────────

#[test]
fn topic_prefix_defaults_to_bairelay() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).unwrap();
	assert_eq!(config.mqtt.as_ref().unwrap().topic_prefix, "bairelay");
	validate_config(&config).expect("default prefix must validate");
}

#[test]
fn topic_prefix_neolink_legacy_valid() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		topic_prefix = "neolink"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).unwrap();
	validate_config(&config).unwrap();
	assert_eq!(config.mqtt.as_ref().unwrap().topic_prefix, "neolink");
}

#[test]
fn topic_prefix_rejects_slashes() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		topic_prefix = "neolink/foo"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("topic_prefix") && err.contains("neolink/foo"),
		"expected slash rejection referencing topic_prefix, got: {err}"
	);
}

#[test]
fn topic_prefix_rejects_empty() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		topic_prefix = ""
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).unwrap();
	let err = validate_config(&config).unwrap_err();
	assert!(
		err.contains("topic_prefix") && err.contains("empty"),
		"expected empty-prefix rejection, got: {err}"
	);
}

// ── stream_prune_grace_secs ───────────────────────────────

#[test]
fn stream_prune_grace_secs_defaults_to_30() {
	// Lowered from 60 → 30 s so the cached `StreamSource` cannot
	// outlive the Baichuan session that feeds it (default
	// `idle_disconnect_timeout_secs` is 45 s). See the invariant on
	// `Config::stream_prune_grace_secs`.
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	assert_eq!(config.stream_prune_grace_secs, 30);
}

#[test]
fn stream_prune_grace_secs_can_be_overridden() {
	let toml_str = r#"
		stream_prune_grace_secs = 30
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	assert_eq!(config.stream_prune_grace_secs, 30);
}

#[test]
fn stream_prune_grace_secs_zero_is_allowed() {
	let toml_str = r#"
		stream_prune_grace_secs = 0
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	assert_eq!(config.stream_prune_grace_secs, 0);
	validate_config(&config).expect("zero grace is legal");
}

// ── MQTT HA discovery config ──────────────────────────────────────────

#[test]
fn discovery_absent_by_default() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	let mqtt = config.mqtt.expect("mqtt present");
	assert!(mqtt.discovery.is_none());
}

#[test]
fn discovery_present_defaults_to_all_features() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[mqtt.discovery]
		topic = "homeassistant"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	validate_config(&config).expect("valid");
	let d = config.mqtt.unwrap().discovery.unwrap();
	assert_eq!(d.topic, "homeassistant");
	// Feature::ALL has 12 entries — full default set.
	assert_eq!(d.features.len(), 12);
}

#[test]
fn discovery_features_can_be_narrowed() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[mqtt.discovery]
		topic = "homeassistant"
		features = ["camera", "battery"]
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_str).expect("parses");
	let d = config.mqtt.unwrap().discovery.unwrap();
	assert_eq!(d.features.len(), 2);
}

#[test]
fn discovery_topic_rejects_slashes_and_empty() {
	// Slashes split the discovery root and silently relocate every
	// config topic.
	let toml_slash = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[mqtt.discovery]
		topic = "ha/prefix"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_slash).expect("parses");
	assert!(validate_config(&config).is_err());

	let toml_empty = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		[mqtt.discovery]
		topic = ""
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
	"#;
	let config = parse_config(toml_empty).expect("parses");
	assert!(validate_config(&config).is_err());
}

// ── PauseConfig.gap_threshold_secs validation ──────────────

#[test]
fn validate_rejects_negative_gap_threshold() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].pause.bridge_gaps = true;
	cfg.cameras[0].pause.gap_threshold_secs = -1.0;
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("gap_threshold_secs"), "error: {err}");
}

#[test]
fn validate_rejects_nan_gap_threshold() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].pause.bridge_gaps = true;
	cfg.cameras[0].pause.gap_threshold_secs = f64::NAN;
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("gap_threshold_secs"), "error: {err}");
}

// ── idle_disconnect_timeout_secs validation ────────────────

#[test]
fn validate_rejects_negative_idle_disconnect_timeout() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].idle_disconnect_timeout_secs = Some(-1.0);
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("idle_disconnect_timeout_secs"), "error: {err}");
}

#[test]
fn validate_rejects_infinite_idle_disconnect_timeout() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].idle_disconnect_timeout_secs = Some(f64::INFINITY);
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("idle_disconnect_timeout_secs"), "error: {err}");
}

#[test]
fn validate_rejects_zero_idle_disconnect_timeout() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].idle_disconnect_timeout_secs = Some(0.0);
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("idle_disconnect_timeout_secs"), "error: {err}");
}

// ── motion_wake_hold_secs validation ─────────────────────────────────

#[test]
fn motion_wake_hold_secs_defaults_to_30() {
	let cam = test_helpers::minimal_camera_config("c");
	assert_eq!(cam.motion_wake_hold_secs, 30.0);
}

#[test]
fn motion_wake_hold_secs_can_be_overridden() {
	let toml_str = r#"
		[[cameras]]
		name = "cam"
		username = "admin"
		password = "test"
		address = "192.168.1.1:9000"
		motion_wake_hold_secs = 5.0
	"#;
	let config = parse_config(toml_str).expect("parses");
	assert_eq!(config.cameras[0].motion_wake_hold_secs, 5.0);
}

#[test]
fn validate_rejects_negative_motion_wake_hold() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].motion_wake_hold_secs = -1.0;
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("motion_wake_hold_secs"), "error: {err}");
}

#[test]
fn validate_rejects_infinite_motion_wake_hold() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].motion_wake_hold_secs = f64::INFINITY;
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("motion_wake_hold_secs"), "error: {err}");
}

#[test]
fn validate_accepts_zero_motion_wake_hold() {
	// 0 means "release the wake lock immediately on Stop" — legal.
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].motion_wake_hold_secs = 0.0;
	validate_config(&cfg).expect("zero hold-down is legal");
}

#[test]
fn validate_rejects_negative_pause_timeout() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].pause.timeout = Some(-0.5);
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("pause.timeout"), "error: {err}");
}

#[test]
fn validate_rejects_nan_pause_timeout() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.cameras[0].pause.timeout = Some(f64::NAN);
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("pause.timeout"), "error: {err}");
}

// ── warn_idle_timeout_below_prune_floor runs without panicking ──────

#[test]
fn warn_idle_timeout_below_prune_floor_runs_for_clamped_camera() {
	use bairelay::config::warn_idle_timeout_below_prune_floor;
	let mut cfg = Config {
		stream_prune_grace_secs: 30,
		cameras: vec![test_helpers::minimal_camera_config("cam-clamp")],
		..Default::default()
	};
	cfg.cameras[0].idle_disconnect_timeout_secs = Some(5.0);
	// Branch where a warning is emitted: configured < prune_grace.
	// Output isn't captured here; we just exercise the path.
	warn_idle_timeout_below_prune_floor(&cfg);
}

#[test]
fn warn_idle_timeout_below_prune_floor_silent_when_above_prune() {
	use bairelay::config::warn_idle_timeout_below_prune_floor;
	let cfg = Config {
		stream_prune_grace_secs: 30,
		cameras: vec![test_helpers::minimal_camera_config("cam-ok")],
		..Default::default()
	};
	// Default cam → 45 s timeout > 30 s prune. No-op branch.
	warn_idle_timeout_below_prune_floor(&cfg);
}

// ── warn_deprecated_pause_fields runs without panicking ────

#[test]
fn warn_deprecated_pause_fields_exercises_every_compat_field() {
	use bairelay::config::warn_deprecated_pause_fields;
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	let p = &mut cfg.cameras[0].pause;
	p.on_motion = Some(true);
	p.on_client = Some(true);
	p.on_disconnect = Some(false);
	p.motion_timeout = Some(2.5);
	p.mode = Some("still".into());
	p.timeout = Some(45.0);
	// Runs without panicking; tracing output isn't captured here but the
	// function executes every branch when all fields are set.
	warn_deprecated_pause_fields(&cfg);
}

#[test]
fn warn_deprecated_pause_fields_flags_shadowed_timeout() {
	use bairelay::config::warn_deprecated_pause_fields;
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	// Both set: the warn path reports the shadow-by-override case.
	cfg.cameras[0].idle_disconnect_timeout_secs = Some(60.0);
	cfg.cameras[0].pause.timeout = Some(10.0);
	warn_deprecated_pause_fields(&cfg);
}

#[test]
fn warn_deprecated_pause_fields_noop_when_clean() {
	use bairelay::config::warn_deprecated_pause_fields;
	let cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	// No compat fields set → function iterates and does nothing.
	warn_deprecated_pause_fields(&cfg);
}

// ── Layer 2: TOML top-level key placement scan ───────────────────────

#[test]
fn placement_scan_flags_certificate_inside_mqtt_section() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"

		# Operator typo: this should be at the document root.
		certificate = "/etc/ssl/cert.pem"
	"#;
	let err = parse_config(toml_str).expect_err("misplaced TLS key must error");
	assert!(err.contains("misplaced top-level keys"), "msg: {err}");
	assert!(err.contains("`certificate`"), "msg: {err}");
	assert!(err.contains("[mqtt]"), "msg: {err}");
}

#[test]
fn placement_scan_flags_multiple_misplaced_keys() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"

		certificate = "/etc/ssl/cert.pem"
		tls_bind_port = 8555
		tls_client_auth = "require"
	"#;
	let err = parse_config(toml_str).expect_err("must error");
	assert!(err.contains("`certificate`"), "msg: {err}");
	assert!(err.contains("`tls_bind_port`"), "msg: {err}");
	assert!(err.contains("`tls_client_auth`"), "msg: {err}");
}

#[test]
fn placement_scan_flags_keys_inside_nested_table() {
	// `[mqtt.discovery]` is a nested table; an unrelated `bind_port`
	// landing inside it must still be flagged.
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"

		[mqtt.discovery]
		topic = "homeassistant"
		bind_port = 9999
	"#;
	let err = parse_config(toml_str).expect_err("must error");
	assert!(err.contains("`bind_port`"), "msg: {err}");
	assert!(err.contains("[mqtt.discovery]"), "msg: {err}");
}

#[test]
fn push_listener_uses_prefixed_keys_not_bind() {
	// `[push_listener]` exposes `push_listener_addr` / `push_listener_port`
	// rather than `bind_addr` / `bind_port` so the placement scanner can
	// stay strict (any `bind_addr` / `bind_port` outside the document root
	// is still a misplaced top-level key) and operators can't shadow the
	// RTSP `bind_addr` by accident.
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"

		[wake_server]
		enable = true

		[push_listener]
		enable = true
		push_listener_addr = "0.0.0.0"
		push_listener_port = 8443

		[[cameras]]
		name = "cam1"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	parse_config(toml_str).expect("prefixed push_listener keys must parse");
}

#[test]
fn placement_scan_still_flags_bind_addr_inside_push_listener() {
	// Operators following the original (pre-rename) docs may still type
	// `bind_addr` inside `[push_listener]`. With `deny_unknown_fields`
	// on the section + the placement scanner watching every section for
	// stray top-level scalars, the misplaced-key path catches it first.
	let toml_str = r#"
		[wake_server]
		enable = true

		[push_listener]
		enable = true
		bind_addr = "0.0.0.0"
		bind_port = 8443
	"#;
	let err = parse_config(toml_str).expect_err("must error");
	assert!(err.contains("misplaced top-level keys"), "msg: {err}");
	assert!(err.contains("`bind_addr`"), "msg: {err}");
	assert!(err.contains("[push_listener]"), "msg: {err}");
}

#[test]
fn placement_scan_flags_keys_inside_array_of_tables() {
	// `[[cameras]]` enters a per-camera table; misplaced top-level key
	// here must surface with a `cameras[N]` path.
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		stream_prune_grace_secs = 30
	"#;
	let err = parse_config(toml_str).expect_err("must error");
	assert!(err.contains("`stream_prune_grace_secs`"), "msg: {err}");
	assert!(err.contains("cameras[0]"), "msg: {err}");
}

#[test]
fn placement_scan_passes_well_formed_config() {
	let toml_str = r#"
		certificate = "/etc/ssl/cert.pem"
		tls_bind_port = 8555
		stream_prune_grace_secs = 30

		[mqtt]
		broker_addr = "127.0.0.1"

		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	let cfg = parse_config(toml_str).expect("well-formed config must parse");
	assert_eq!(cfg.tls_bind_port, Some(8555));
	assert_eq!(cfg.stream_prune_grace_secs, 30);
}

#[test]
fn placement_scan_does_not_flag_unrelated_keys_inside_sections() {
	// `enable_motion` is a normal per-camera-mqtt key, not a top-level
	// field; it must not trip the scanner.
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"

		[cameras.mqtt]
		enable_motion = false
	"#;
	parse_config(toml_str).expect("section-local keys must parse");
}

// ── Real aliases from neolink ────────────────────────────────────────

#[test]
fn stream_both_alias_maps_to_all() {
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		stream = "both"
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert_eq!(cfg.cameras[0].stream, StreamConfig::All);
}

#[test]
fn idle_disc_alias_maps_to_idle_disconnect() {
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		idle_disc = true
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert!(cfg.cameras[0].idle_disconnect);
}

#[test]
fn username_alias_maps_to_user_name() {
	let toml_str = r#"
		[[users]]
		username = "admin"
		password = "x"

		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		permitted_users = ["admin"]
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert_eq!(cfg.users[0].name, "admin");
}

// ── Neolink compat optional fields (accept-and-warn) ────────────────

#[test]
fn neolink_top_level_tokio_console_parses_as_optional() {
	let toml_str = r#"
		tokio_console = true
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert_eq!(cfg.tokio_console, Some(true));
}

#[test]
fn neolink_per_camera_compat_fields_all_parse() {
	// Every neolink per-camera field that bairelay accepts as a
	// no-op compat alias. None of these has a runtime effect; the
	// goal is "old config parses cleanly".
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		print_format = "human"
		update_time = true
		buffer_duration = 5000
		use_splash = false
		splash_pattern = "smpte"
		max_discovery_retries = 5
		push_notifications = false
		strict = true
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	let cam = &cfg.cameras[0];
	assert_eq!(cam.print_format.as_deref(), Some("human"));
	assert_eq!(cam.update_time, Some(true));
	assert_eq!(cam.buffer_duration, Some(5000));
	assert_eq!(cam.use_splash, Some(false));
	assert_eq!(cam.splash_pattern.as_deref(), Some("smpte"));
	assert_eq!(cam.max_discovery_retries, Some(5));
	assert_eq!(cam.push_notifications, Some(false));
	assert_eq!(cam.strict, Some(true));
}

#[test]
fn neolink_per_camera_aliases_all_parse() {
	// Same fields under their neolink aliases.
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		verbose = true
		print = "human"
		time = false
		duration = 3000
		splash = true
		pattern = "snow"
		retries = 10
		push = true
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	let cam = &cfg.cameras[0];
	assert_eq!(cam.debug, Some(true));
	assert_eq!(cam.print_format.as_deref(), Some("human"));
	assert_eq!(cam.update_time, Some(false));
	assert_eq!(cam.buffer_duration, Some(3000));
	assert_eq!(cam.use_splash, Some(true));
	assert_eq!(cam.splash_pattern.as_deref(), Some("snow"));
	assert_eq!(cam.max_discovery_retries, Some(10));
	assert_eq!(cam.push_notifications, Some(true));
}

#[test]
fn neolink_push_noti_alias_alone_parses() {
	// `push_noti` is the alternate alias; serde rejects when both
	// `push` and `push_noti` appear (duplicate field). Tested in
	// isolation here.
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		push_noti = false
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert_eq!(cfg.cameras[0].push_notifications, Some(false));
}

#[test]
fn neolink_per_camera_mqtt_discovery_parses_as_optional() {
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"

		[cameras.mqtt.discovery]
		topic = "homeassistant"
		features = ["motion"]
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert!(cfg.cameras[0].mqtt.discovery.is_some());
}

#[test]
fn warn_neolink_compat_fields_runs_for_every_field() {
	use bairelay::config::warn_neolink_compat_fields;
	let mut cfg = Config {
		tokio_console: Some(true),
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	let cam = &mut cfg.cameras[0];
	cam.print_format = Some("human".into());
	cam.update_time = Some(true);
	cam.buffer_duration = Some(3000);
	cam.use_splash = Some(true);
	cam.splash_pattern = Some("snow".into());
	cam.max_discovery_retries = Some(10);
	cam.push_notifications = Some(false);
	cam.strict = Some(true);
	cam.mqtt.discovery = Some(bairelay::config::MqttDiscoveryConfig {
		topic: "homeassistant".into(),
		features: Default::default(),
	});
	// Exercises every branch; tracing output isn't captured.
	warn_neolink_compat_fields(&cfg);
}

#[test]
fn warn_neolink_compat_fields_noop_when_clean() {
	use bairelay::config::warn_neolink_compat_fields;
	let cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	warn_neolink_compat_fields(&cfg);
}

// ── Per-camera wire-debug knob ──────────────────────────────────────

#[test]
fn camera_debug_knob_parses_and_defaults_off() {
	let toml_str = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
		debug = true
	"#;
	let cfg = parse_config(toml_str).expect("parses");
	assert_eq!(cfg.cameras[0].debug, Some(true));

	let clean = r#"
		[[cameras]]
		name = "front"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	let cfg = parse_config(clean).expect("parses");
	assert_eq!(cfg.cameras[0].debug, None);
}

#[test]
fn warn_wire_debug_enabled_covers_set_and_unset() {
	use bairelay::config::warn_wire_debug_enabled;
	let mut cfg = Config {
		cameras: vec![
			test_helpers::minimal_camera_config("cam1"),
			test_helpers::minimal_camera_config("cam2"),
			test_helpers::minimal_camera_config("cam3"),
		],
		..Default::default()
	};
	cfg.cameras[0].debug = Some(true); // warns
	cfg.cameras[1].debug = Some(false); // silent
									 // cameras[2] stays None — silent.
									 // Exercises every branch; tracing output isn't captured.
	warn_wire_debug_enabled(&cfg);
}

// ── deny_unknown_fields enforcement on previously-lax structs ───────

#[test]
fn unknown_field_on_users_is_rejected() {
	let toml_str = r#"
		[[users]]
		name = "admin"
		pass = "x"
		not_a_real_field = true

		[[cameras]]
		name = "c"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	let err = parse_config(toml_str).expect_err("must reject");
	assert!(
		err.contains("unknown field") || err.contains("not_a_real_field"),
		"msg: {err}"
	);
}

#[test]
fn unknown_field_on_mqtt_server_is_rejected() {
	let toml_str = r#"
		[mqtt]
		broker_addr = "127.0.0.1"
		bogus_key = 42

		[[cameras]]
		name = "c"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"
	"#;
	let err = parse_config(toml_str).expect_err("must reject");
	assert!(
		err.contains("unknown field") || err.contains("bogus_key"),
		"msg: {err}"
	);
}

#[test]
fn unknown_field_on_camera_mqtt_is_rejected() {
	let toml_str = r#"
		[[cameras]]
		name = "c"
		username = "admin"
		password = "x"
		address = "192.168.1.1:9000"

		[cameras.mqtt]
		enable_motion = true
		typo_field = false
	"#;
	let err = parse_config(toml_str).expect_err("must reject");
	assert!(
		err.contains("unknown field") || err.contains("typo_field"),
		"msg: {err}"
	);
}

// ── Validate rejects empty user name ────────────────────────────────

#[test]
fn validate_rejects_empty_user_name() {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.users.push(bairelay::config::UserConfig {
		name: "   ".into(),
		pass: "pw".into(),
	});
	let err = validate_config(&cfg).expect_err("must reject");
	assert!(err.contains("name must not be empty"), "error: {err}");
}

// ── [push_listener] validation ────────────────────────────────────────
//
// Camera-level `motion_wake_hold_secs` is gated on finite + non-negative
// (see config.rs:1137); the matching field on `[push_listener]` had no
// validation, so a NaN / infinite / negative value would panic later
// inside `Duration::from_secs_f64` at startup. `bind_port = 0` is also
// rejected: the chosen ephemeral port is never surfaced, so DNS-redirect
// from the camera would never reach it. And push_listener requires
// wake_server.enable=true because the shared `CameraRegistry` is only
// populated by the wake server's `D2R_HB` handler — without it every
// `lookup_by_ip` misses silently.

fn push_listener_cfg(
	motion_wake_hold_secs: f64,
	push_listener_port: u16,
) -> bairelay::config::Config {
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.wake_server = Some(bairelay_wake_server::WakeServerConfig {
		enable: true,
		..Default::default()
	});
	cfg.push_listener = Some(bairelay::config::PushListenerConfig {
		enable: true,
		push_listener_addr: None,
		push_listener_port,
		motion_wake_hold_secs,
	});
	cfg
}

#[test]
fn validate_rejects_push_listener_nan_hold() {
	let cfg = push_listener_cfg(f64::NAN, 443);
	let err = validate_config(&cfg).expect_err("NaN must reject");
	assert!(
		err.contains("push_listener") && err.contains("motion_wake_hold_secs"),
		"error: {err}"
	);
}

#[test]
fn validate_rejects_push_listener_negative_hold() {
	let cfg = push_listener_cfg(-1.0, 443);
	let err = validate_config(&cfg).expect_err("negative must reject");
	assert!(
		err.contains("push_listener") && err.contains("motion_wake_hold_secs"),
		"error: {err}"
	);
}

#[test]
fn validate_rejects_push_listener_infinite_hold() {
	let cfg = push_listener_cfg(f64::INFINITY, 443);
	let err = validate_config(&cfg).expect_err("infinite must reject");
	assert!(
		err.contains("push_listener") && err.contains("motion_wake_hold_secs"),
		"error: {err}"
	);
}

#[test]
fn validate_accepts_push_listener_zero_hold() {
	let cfg = push_listener_cfg(0.0, 443);
	validate_config(&cfg).expect("zero hold is legal");
}

#[test]
fn validate_rejects_push_listener_zero_port() {
	let cfg = push_listener_cfg(30.0, 0);
	let err = validate_config(&cfg).expect_err("port 0 must reject");
	assert!(
		err.contains("push_listener") && err.contains("push_listener_port"),
		"error: {err}"
	);
}

#[test]
fn validate_accepts_push_listener_without_wake_server_when_a_camera_has_an_ip() {
	// No heartbeat registry, but `minimal_camera_config`'s literal IP
	// `address` lets pushes be matched by source IP.
	let mut cfg = push_listener_cfg(30.0, 443);
	cfg.wake_server = None;
	validate_config(&cfg).expect("address-matched push_listener must pass");
}

#[test]
fn validate_rejects_push_listener_with_no_way_to_match_a_camera() {
	// Wake server off and no camera has a literal IP: every push would
	// miss silently.
	for wake_server in [
		None,
		Some(bairelay_wake_server::WakeServerConfig {
			enable: false,
			..Default::default()
		}),
	] {
		let mut cfg = push_listener_cfg(30.0, 443);
		cfg.wake_server = wake_server;
		cfg.cameras[0].address = Some("cam.local:9000".to_string());
		cfg.cameras[0].uid = Some("9527000TESTCAM00".to_string());
		let err = validate_config(&cfg).expect_err("must reject");
		assert!(
			err.contains("push_listener") && err.contains("wake_server") && err.contains("address"),
			"error: {err}"
		);
	}
}

#[test]
fn validate_accepts_disabled_push_listener_without_wake_server() {
	// A disabled `[push_listener]` block should not force wake_server on.
	let mut cfg = Config {
		cameras: vec![test_helpers::minimal_camera_config("cam1")],
		..Default::default()
	};
	cfg.push_listener = Some(bairelay::config::PushListenerConfig {
		enable: false,
		push_listener_addr: None,
		push_listener_port: 443,
		motion_wake_hold_secs: 30.0,
	});
	validate_config(&cfg).expect("disabled push_listener must pass");
}
