//! Typed view of Supervisor's `/data/options.json`.

use serde::Deserialize;

use crate::config::{CameraConfig, Config, MqttServerConfig};

/// Flags supplied by the entrypoint shim from `bashio::services 'mqtt' '<field>'`.
/// Each field is `None` when Supervisor's MQTT integration isn't installed —
/// in that case the user's TOML overlay must carry the broker config.
#[derive(Debug, Clone, Default)]
pub struct MqttServiceFlags {
	pub host: Option<String>,
	pub port: Option<u16>,
	pub username: Option<String>,
	pub password: Option<String>,
	pub ssl: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HassioOptions {
	#[serde(default = "default_topic_prefix")]
	pub topic_prefix: String,
	#[serde(default = "default_log_level")]
	pub log_level: String,
	#[serde(default)]
	pub cameras: Vec<HassioCamera>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HassioCamera {
	pub name: String,
	#[serde(default)]
	pub address: Option<String>,
	#[serde(default)]
	pub uid: Option<String>,
	#[serde(default = "default_username")]
	pub username: String,
	pub password: String,
	#[serde(default = "default_idle_disconnect")]
	pub idle_disconnect: bool,
}

fn default_username() -> String {
	"admin".into()
}

fn default_idle_disconnect() -> bool {
	true
}

fn default_topic_prefix() -> String {
	"bairelay".into()
}

fn default_log_level() -> String {
	"info".into()
}

/// Map the Supervisor-provided HA options and MQTT service flags onto a
/// bairelay [`Config`]. This is the minimal base; the operator's TOML
/// overlay merges on top to fill in fields the HA options form doesn't
/// expose (CA cert path for MQTT TLS, per-camera username/uid, discovery
/// settings, etc.).
///
/// - `mqtt.host == None` leaves `cfg.mqtt = None` so the overlay can supply
///   it. Set both `username` and `password` together to populate
///   `credentials`; missing either leaves the broker unauthenticated.
/// - `mqtt.ssl` is intentionally ignored here — the Supervisor-provided
///   broker host (`core-mosquitto`) wouldn't match a public certificate,
///   and its CA isn't surfaced by `bashio::services 'mqtt'`; MQTT TLS is
///   configured in the overlay (`tls` / `ca` / `client_auth`). The field
///   stays on [`MqttServiceFlags`] for forward-compat.
/// - Each camera carries `address` + `uid` as separate optional fields,
///   mirroring [`CameraConfig`]. Empty strings collapse to `None` so a
///   blank form field doesn't masquerade as a configured value.
/// - `username` defaults to `"admin"` (Reolink's stock account) when the
///   field isn't supplied.
pub fn build_base_config(opts: &HassioOptions, mqtt: &MqttServiceFlags) -> Config {
	let mut cfg = Config::default();

	if let Some(host) = &mqtt.host {
		let credentials = match (&mqtt.username, &mqtt.password) {
			(Some(u), Some(p)) => Some((u.clone(), p.clone())),
			_ => None,
		};
		cfg.mqtt = Some(MqttServerConfig {
			broker_addr: host.clone(),
			port: mqtt.port.unwrap_or(1883),
			credentials,
			tls: None,
			ca: None,
			client_auth: None,
			topic_prefix: opts.topic_prefix.clone(),
			// HA MQTT discovery is the whole point of running bairelay as
			// an HA app — auto-enable so entities show up without the
			// operator hand-editing the overlay. Override by setting a
			// different `[mqtt.discovery] topic` in the overlay, or
			// remove the `[mqtt]` block entirely to run RTSP-only.
			discovery: Some(crate::config::MqttDiscoveryConfig {
				topic: "homeassistant".into(),
				features: bairelay_mqtt::discovery::Feature::ALL
					.iter()
					.copied()
					.collect(),
			}),
		});
	}

	cfg.cameras = opts
		.cameras
		.iter()
		.map(|c| CameraConfig {
			name: c.name.clone(),
			address: c.address.clone().filter(|s| !s.is_empty()),
			uid: c.uid.clone().filter(|s| !s.is_empty()),
			username: c.username.clone(),
			password: Some(c.password.clone()),
			idle_disconnect: c.idle_disconnect,
			..CameraConfig::default()
		})
		.collect();

	cfg
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_minimal_options_json() {
		let json = r#"{
			"topic_prefix": "bairelay",
			"log_level": "info",
			"cameras": [
				{"name": "Hallway", "address": "192.168.1.50", "password": "secret"}
			]
		}"#;
		let opts: HassioOptions = serde_json::from_str(json).unwrap();
		assert_eq!(opts.topic_prefix, "bairelay");
		assert_eq!(opts.log_level, "info");
		assert_eq!(opts.cameras.len(), 1);
		assert_eq!(opts.cameras[0].name, "Hallway");
		assert_eq!(opts.cameras[0].address.as_deref(), Some("192.168.1.50"));
		assert!(opts.cameras[0].uid.is_none());
		assert_eq!(opts.cameras[0].username, "admin");
		assert_eq!(opts.cameras[0].password, "secret");
		assert!(opts.cameras[0].idle_disconnect, "default true");
	}

	#[test]
	fn parses_options_json_with_uid_and_custom_username() {
		let json = r#"{
			"topic_prefix": "bairelay",
			"log_level": "info",
			"cameras": [
				{"name": "Garage", "uid": "9527000ABCDEF123", "username": "operator", "password": "pw"}
			]
		}"#;
		let opts: HassioOptions = serde_json::from_str(json).unwrap();
		assert!(opts.cameras[0].address.is_none());
		assert_eq!(opts.cameras[0].uid.as_deref(), Some("9527000ABCDEF123"));
		assert_eq!(opts.cameras[0].username, "operator");
	}

	#[test]
	fn mqtt_service_flags_defaults_to_unset() {
		let flags = MqttServiceFlags::default();
		assert!(flags.host.is_none());
		assert!(flags.port.is_none());
		assert!(flags.username.is_none());
		assert!(flags.password.is_none());
		assert!(!flags.ssl);
	}

	#[test]
	fn builds_base_config_with_cameras_and_mqtt() {
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
		let cfg = build_base_config(&opts, &mqtt);
		assert_eq!(cfg.cameras.len(), 1);
		assert_eq!(cfg.cameras[0].name, "Hallway");
		let m = cfg.mqtt.as_ref().expect("mqtt set");
		assert_eq!(m.broker_addr, "core-mosquitto");
		assert_eq!(m.port, 1883);
		assert_eq!(m.topic_prefix, "bairelay");
		assert_eq!(cfg.cameras[0].address.as_deref(), Some("192.168.1.50"));
		assert!(cfg.cameras[0].uid.is_none());
		assert_eq!(cfg.cameras[0].username, "admin");
		assert_eq!(cfg.cameras[0].password.as_deref(), Some("secret"));
		let creds = m.credentials.as_ref().expect("creds set");
		assert_eq!(creds.0, "addons");
		assert_eq!(creds.1, "pw");

		// Round-trip through validate_config — catches future drift in required-field invariants for free.
		crate::config::validate_config(&cfg).expect("base config validates");
	}

	#[test]
	fn uid_camera_routes_to_uid_field_not_address() {
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![HassioCamera {
				name: "Garage".into(),
				address: None,
				uid: Some("9527000ABCDEF123".into()),
				username: "admin".into(),
				password: "p".into(),
				idle_disconnect: true,
			}],
		};
		let cfg = build_base_config(&opts, &MqttServiceFlags::default());
		assert!(cfg.cameras[0].address.is_none());
		assert_eq!(cfg.cameras[0].uid.as_deref(), Some("9527000ABCDEF123"));
		crate::config::validate_config(&cfg).expect("uid-only camera validates");
	}

	#[test]
	fn empty_address_and_uid_collapse_to_none() {
		// HA forms submit empty strings rather than null when a field is
		// left blank; we filter them so validate_config doesn't see
		// Some("") (which it accepts but bairelay's connect path can't use).
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![HassioCamera {
				name: "Cam".into(),
				address: Some("".into()),
				uid: Some("ABC".into()),
				username: "admin".into(),
				password: "p".into(),
				idle_disconnect: true,
			}],
		};
		let cfg = build_base_config(&opts, &MqttServiceFlags::default());
		assert!(cfg.cameras[0].address.is_none());
		assert_eq!(cfg.cameras[0].uid.as_deref(), Some("ABC"));
	}

	#[test]
	fn no_cameras_yields_empty_camera_list() {
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let cfg = build_base_config(&opts, &MqttServiceFlags::default());
		assert!(cfg.cameras.is_empty());
	}

	#[test]
	fn no_mqtt_injection_leaves_mqtt_unset() {
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let cfg = build_base_config(&opts, &MqttServiceFlags::default());
		assert!(
			cfg.mqtt.is_none(),
			"mqtt must stay None for overlay to fill in"
		);
	}

	#[test]
	fn ssl_flag_does_not_break_mqtt_propagation() {
		// Supervisor may report ssl=true (HA broker on 8883). The minimal
		// builder ignores it — TLS to MQTT requires a CA cert path the
		// overlay TOML must supply (`[mqtt] ca = "..."`). This test pins
		// that the base builder still produces a valid Config; the
		// overlay step is where TLS materialises.
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let mqtt = MqttServiceFlags {
			host: Some("broker.example".into()),
			port: Some(8883),
			username: None,
			password: None,
			ssl: true,
		};
		let cfg = build_base_config(&opts, &mqtt);
		let m = cfg.mqtt.as_ref().expect("mqtt set");
		assert_eq!(m.broker_addr, "broker.example");
		assert_eq!(m.port, 8883);
		assert!(m.ca.is_none(), "TLS deferred to overlay");
	}

	#[test]
	fn half_set_mqtt_credentials_yields_none() {
		// Supervisor reports either both username+password or neither;
		// a half-set tuple is meaningless to the broker. The builder
		// collapses (Some, None) and (None, Some) to credentials: None
		// so bairelay's MQTT client connects anonymously rather than
		// with a malformed credential pair.
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let user_only = MqttServiceFlags {
			host: Some("b.example".into()),
			port: Some(1883),
			username: Some("u".into()),
			password: None,
			ssl: false,
		};
		let cfg = build_base_config(&opts, &user_only);
		assert!(
			cfg.mqtt.as_ref().unwrap().credentials.is_none(),
			"user-only injection collapses to None"
		);

		let pass_only = MqttServiceFlags {
			host: Some("b.example".into()),
			port: Some(1883),
			username: None,
			password: Some("p".into()),
			ssl: false,
		};
		let cfg = build_base_config(&opts, &pass_only);
		assert!(
			cfg.mqtt.as_ref().unwrap().credentials.is_none(),
			"password-only injection collapses to None"
		);
	}

	#[test]
	fn mqtt_port_zero_is_sentinel_for_unset() {
		// Port 0 from the s6 entrypoint's bashio-fallback path means "no
		// MQTT integration injected" — the builder should treat it as if
		// `host` were also empty and produce cfg.mqtt = None.
		// (Filter happens in cmd::run before build_base_config; this test
		// pins the contract that the builder TREATS host=None the same
		// regardless of port value.)
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let mqtt = MqttServiceFlags {
			host: None,
			port: Some(0),
			username: None,
			password: None,
			ssl: false,
		};
		let cfg = build_base_config(&opts, &mqtt);
		assert!(cfg.mqtt.is_none(), "host=None overrides any port value");
	}

	#[test]
	fn mqtt_port_unset_defaults_to_1883() {
		let opts = HassioOptions {
			topic_prefix: "bairelay".into(),
			log_level: "info".into(),
			cameras: vec![],
		};
		let mqtt = MqttServiceFlags {
			host: Some("b.example".into()),
			port: None,
			username: None,
			password: None,
			ssl: false,
		};
		let cfg = build_base_config(&opts, &mqtt);
		assert_eq!(cfg.mqtt.as_ref().unwrap().port, 1883);
	}
}
