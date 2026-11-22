//! Implementation of the `bairelay render-hassio-config` subcommand.

use std::path::Path;

use crate::config::{validate_config, CameraConfig, Config};
use crate::hassio::{merge, options};

#[allow(clippy::too_many_arguments)]
pub fn run(
	options_json: &Path,
	overlay: Option<&Path>,
	mqtt_host: Option<String>,
	mqtt_port: Option<u16>,
	mqtt_user: Option<String>,
	mqtt_pass: Option<String>,
	mqtt_ssl: bool,
	output: Option<&Path>,
) -> anyhow::Result<()> {
	let opts_src = std::fs::read_to_string(options_json)
		.map_err(|e| anyhow::anyhow!("read options.json at {}: {e}", options_json.display()))?;
	let opts: options::HassioOptions =
		serde_json::from_str(&opts_src).map_err(|e| anyhow::anyhow!("parse options.json: {e}"))?;

	let mqtt_flags = options::MqttServiceFlags {
		host: mqtt_host.filter(|s| !s.is_empty()),
		// Treat 0 as "unset" — the s6 entrypoint passes `--mqtt-port 0` when
		// bashio's MQTT service lookup fails (integration uninstalled). Port 0
		// is meaningless as a client connection target so it's a safe sentinel.
		port: mqtt_port.filter(|&p| p != 0),
		username: mqtt_user.filter(|s| !s.is_empty()),
		password: mqtt_pass.filter(|s| !s.is_empty()),
		ssl: mqtt_ssl,
	};

	let mut cfg = options::build_base_config(&opts, &mqtt_flags);

	if let Some(p) = overlay {
		if p.exists() {
			let src = std::fs::read_to_string(p)
				.map_err(|e| anyhow::anyhow!("read overlay at {}: {e}", p.display()))?;
			let overlay_cfg = merge::parse_overlay(&src).map_err(|e| anyhow::anyhow!("{e}"))?;
			cfg = merge::merge(cfg, overlay_cfg);
		}
	}

	// Same normalisation `parse_config` does for the file-only path: a pacer
	// lead cap below its own latency is a tuning mistake, so the effective
	// config the operator sees and the pacer runs on carries the corrected
	// pair, with the warning in this command's output.
	crate::config::clamp_pacer_lead_limits(&mut cfg);

	validate_config(&cfg).map_err(|e| anyhow::anyhow!("merged config invalid: {e}"))?;

	let rendered = minimize_render(&cfg)?;
	match output {
		Some(p) => std::fs::write(p, rendered)
			.map_err(|e| anyhow::anyhow!("write {}: {e}", p.display()))?,
		None => print!("{rendered}"),
	}
	Ok(())
}

/// Serialise `cfg` to TOML with default-valued fields stripped, so the
/// emitted file shows only what the operator actually configured (form
/// inputs + overlay overrides + injected MQTT broker creds). Loading the
/// stripped file back through `Config::deserialize` reapplies the same
/// defaults, so behaviour is unchanged — only the on-disk noise is.
fn minimize_render(cfg: &Config) -> anyhow::Result<String> {
	let mut value =
		toml::Value::try_from(cfg).map_err(|e| anyhow::anyhow!("serialise config: {e}"))?;
	let cfg_default = toml::Value::try_from(Config::default())
		.map_err(|e| anyhow::anyhow!("serialise default config: {e}"))?;
	let cam_default = toml::Value::try_from(CameraConfig::default())
		.map_err(|e| anyhow::anyhow!("serialise default camera: {e}"))?;

	strip_defaults(&mut value, &cfg_default);
	if let Some(toml::Value::Array(cameras)) =
		value.as_table_mut().and_then(|t| t.get_mut("cameras"))
	{
		for cam in cameras {
			strip_defaults(cam, &cam_default);
		}
	}

	toml::to_string_pretty(&value).map_err(|e| anyhow::anyhow!("serialise stripped config: {e}"))
}

/// Recursively remove from `value` every table key whose value matches
/// the same key's value in `default`. Nested tables are recursed into;
/// if a nested table becomes empty after stripping, it is also removed.
fn strip_defaults(value: &mut toml::Value, default: &toml::Value) {
	let (toml::Value::Table(t), toml::Value::Table(d)) = (value, default) else {
		return;
	};
	let keys: Vec<String> = t.keys().cloned().collect();
	for k in keys {
		let Some(dv) = d.get(&k).cloned() else {
			continue;
		};
		let v = t.get(&k).cloned().expect("key just enumerated");
		if v == dv {
			t.remove(&k);
			continue;
		}
		if v.is_table() {
			let mut nested = v;
			strip_defaults(&mut nested, &dv);
			match &nested {
				toml::Value::Table(nt) if nt.is_empty() => {
					t.remove(&k);
				}
				_ => {
					t.insert(k, nested);
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn minimize_strips_top_level_defaults() {
		let mut cfg = Config::default();
		cfg.cameras.push(CameraConfig {
			name: "cam1".into(),
			username: "admin".into(),
			password: Some("pw".into()),
			uid: Some("UID1".into()),
			..CameraConfig::default()
		});
		let out = minimize_render(&cfg).unwrap();
		// All default top-level fields gone.
		assert!(!out.contains("bind ="), "stripped: bind\n{out}");
		assert!(!out.contains("bind_port"), "stripped: bind_port\n{out}");
		assert!(!out.contains("users"), "stripped: users\n{out}");
		assert!(!out.contains("tls_client_auth"));
		assert!(!out.contains("stream_prune_grace_secs"));
		// Camera defaults gone, only the operator-supplied fields remain.
		assert!(out.contains(r#"name = "cam1""#));
		assert!(out.contains(r#"uid = "UID1""#));
		assert!(out.contains(r#"username = "admin""#));
		assert!(out.contains(r#"password = "pw""#));
		assert!(!out.contains("channel_id"));
		assert!(!out.contains("discovery"));
		assert!(!out.contains("max_encryption"));
		assert!(!out.contains("[cameras.mqtt]"));
		assert!(!out.contains("[cameras.pause]"));
		assert!(!out.contains("permitted_users"));
	}

	#[test]
	fn minimize_keeps_non_default_overrides() {
		let mut cfg = Config::default();
		cfg.cameras.push(CameraConfig {
			name: "cam1".into(),
			username: "admin".into(),
			password: Some("pw".into()),
			idle_disconnect: true,
			..CameraConfig::default()
		});
		let out = minimize_render(&cfg).unwrap();
		assert!(out.contains("idle_disconnect = true"));
	}
}
