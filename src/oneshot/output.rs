use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub enum Mode {
	Human,
	Json,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Outcome {
	Reboot,
	Snapshot {
		bytes: usize,
		path: Option<String>,
		/// `jpeg` for `get_snapshot` output, `h264` or `h265` for
		/// `--use-stream` raw-bitstream dumps.
		format: String,
	},
	Battery {
		percent: u32,
		/// Battery voltage in raw integer millivolts. Carried on the wire
		/// as `BatteryInfo.voltage: i32` (millivolts) by bairelay_neolink_core; we
		/// preserve it that way so JSON output is stable. Earlier
		/// versions emitted `voltage_v: f32` which serialised as
		/// `3.941000061035156` in `--json` output for some integer-mV
		/// inputs (f32 → f64 widening). Human formatter divides by 1000
		/// for the operator-facing volts string.
		voltage_mv: u32,
		charge_status: String,
		low_power: bool,
	},
	Floodlight {
		state: bool,
	},
	Presets {
		presets: Vec<Preset>,
	},
	SetTime {
		utc: String,
	},
	Version {
		model: String,
		firmware: String,
		hardware: String,
		build_day: String,
	},
	Siren,
	Pir {
		enable: bool,
		sensitivity: Option<u8>,
	},
	StatusLight {
		state: bool,
	},
	/// `supported` is false when the firmware has no `<HaCfg>`; `code`
	/// is then the camera's error reply code, or `None` for an empty
	/// reply, and the other fields are `None`.
	Webhook {
		supported: bool,
		#[serde(skip_serializing_if = "Option::is_none")]
		code: Option<u16>,
		enable: Option<bool>,
		url: Option<String>,
		verify_cert: Option<bool>,
	},
	PtzMoveTo {
		preset_id: u8,
	},
	PtzAssign {
		preset_id: u8,
		name: String,
	},
	PtzControl {
		direction: String,
		amount: u32,
		speed: Option<u32>,
	},
	PtzZoom {
		amount: f32,
	},
	Service {
		service: String,
		port: u32,
		enabled: Option<bool>,
	},
	ServiceList {
		services: Vec<ServiceEntry>,
	},
	UsersList {
		users: Vec<UserInfo>,
	},
	UserChanged {
		name: String,
		action: String,
	},
	Abilities {
		/// Login username the camera scoped these abilities to.
		username: String,
		/// Re-serialised XML form of the camera's `AbilityInfo` reply.
		/// Verbatim contents of the `quick_xml::se::to_writer` output —
		/// stable enough to drop into a fixture and cheap to grep through.
		xml: String,
		/// Flat list of every `(module, name, kind)` triple parsed out of
		/// the XML. `kind` is `"rw"` or `"ro"` per the abilityValue suffix
		/// (matches the gate `populate_abilities` builds against).
		entries: Vec<AbilityEntry>,
	},
}

#[derive(Debug, Serialize, PartialEq)]
pub struct AbilityEntry {
	pub module: String,
	pub name: String,
	pub kind: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Preset {
	pub id: u8,
	pub name: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct UserInfo {
	pub name: String,
	pub level: u8,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct ServiceEntry {
	pub name: String,
	pub port: u32,
	pub enabled: Option<bool>,
}

pub fn format_success(mode: Mode, outcome: &Outcome) -> (String, String) {
	match mode {
		Mode::Human => format_human(outcome),
		Mode::Json => (format_json_success(outcome), String::new()),
	}
}

fn format_human(outcome: &Outcome) -> (String, String) {
	match outcome {
		Outcome::Reboot => (String::new(), "ok (reboot sent)\n".into()),
		Outcome::Siren => (String::new(), "ok (siren triggered)\n".into()),
		Outcome::Snapshot {
			bytes,
			path,
			format,
		} => (
			String::new(),
			match path {
				Some(p) => format!("ok (wrote {} bytes of {} to {})\n", bytes, format, p),
				None => format!("ok ({} bytes of {})\n", bytes, format),
			},
		),
		Outcome::Floodlight { state } => (
			// Stdout for both read and set paths — the reported state IS
			// the confirmation. Matches the pir / status-light shape so
			// piping works uniformly.
			format!("Floodlight: {}\n", if *state { "on" } else { "off" }),
			String::new(),
		),
		Outcome::SetTime { utc } => (String::new(), format!("ok (set camera clock to {})\n", utc)),
		Outcome::Battery {
			percent,
			voltage_mv,
			charge_status,
			low_power,
		} => (
			format!(
				"Battery: {}% ({:.2} V, charge={}, low_power={})\n",
				percent,
				*voltage_mv as f32 / 1000.0,
				charge_status,
				low_power
			),
			String::new(),
		),
		Outcome::Presets { presets } => {
			let mut out = String::from("id  name\n");
			for p in presets {
				out.push_str(&format!(
					"{:<3} {}\n",
					p.id,
					p.name.as_deref().unwrap_or("(unnamed)")
				));
			}
			(out, String::new())
		}
		Outcome::Version {
			model,
			firmware,
			hardware,
			build_day,
		} => (
			format!(
				"model={}\nfirmware={}\nhardware={}\nbuild={}\n",
				model, firmware, hardware, build_day
			),
			String::new(),
		),
		Outcome::Pir {
			enable,
			sensitivity,
		} => {
			let s = match sensitivity {
				Some(v) => format!(
					"PIR: {} (sensitivity {})\n",
					if *enable { "on" } else { "off" },
					v
				),
				None => format!("PIR: {}\n", if *enable { "on" } else { "off" }),
			};
			(s, String::new())
		}
		Outcome::StatusLight { state } => (
			format!("Status light: {}\n", if *state { "on" } else { "off" }),
			String::new(),
		),
		Outcome::Webhook {
			supported: false,
			code,
			..
		} => (
			match code {
				Some(code) => format!("Webhook: unsupported (camera replied {code})\n"),
				None => "Webhook: unsupported\n".into(),
			},
			String::new(),
		),
		Outcome::Webhook {
			enable,
			url,
			verify_cert,
			..
		} => {
			let on_off = |v: &Option<bool>| match v {
				Some(true) => "on",
				_ => "off",
			};
			let url = match url.as_deref() {
				Some(u) if !u.is_empty() => display_text(u, URL_DISPLAY_MAX),
				_ => "(empty)".to_string(),
			};
			(
				format!(
					"Webhook: supported\n  enable: {}\n  url: {}\n  verify_cert: {}\n",
					on_off(enable),
					url,
					on_off(verify_cert),
				),
				String::new(),
			)
		}
		Outcome::PtzMoveTo { preset_id } => (
			String::new(),
			format!("ok (moved to preset {})\n", preset_id),
		),
		Outcome::PtzAssign { preset_id, name } => (
			String::new(),
			format!("ok (saved preset {} as '{}')\n", preset_id, name),
		),
		Outcome::PtzControl {
			direction,
			amount,
			speed,
		} => (
			String::new(),
			match speed {
				Some(s) => format!("ok ({} by {} at speed {})\n", direction, amount, s),
				None => format!("ok ({} by {})\n", direction, amount),
			},
		),
		Outcome::PtzZoom { amount } => (String::new(), format!("ok (zoomed to {:.2})\n", amount)),
		Outcome::Service {
			service,
			port,
			enabled,
		} => {
			let enabled_str = match enabled {
				Some(true) => "enabled",
				Some(false) => "disabled",
				None => "(unknown)",
			};
			(
				format!("{}: port {} ({})\n", service, port, enabled_str),
				String::new(),
			)
		}
		Outcome::ServiceList { services } => {
			let mut out = String::from("service    port   state\n");
			for s in services {
				let enabled_str = match s.enabled {
					Some(true) => "enabled",
					Some(false) => "disabled",
					None => "(unknown)",
				};
				out.push_str(&format!("{:<10} {:<6} {}\n", s.name, s.port, enabled_str));
			}
			(out, String::new())
		}
		Outcome::UsersList { users } => {
			let mut out = String::from("name                 level\n");
			for u in users {
				let level_str = match u.level {
					0 => "user",
					1 => "admin",
					_ => "?",
				};
				out.push_str(&format!("{:<20} {}\n", u.name, level_str));
			}
			(out, String::new())
		}
		Outcome::UserChanged { name, action } => {
			(String::new(), format!("ok ({} user '{}')\n", action, name))
		}
		Outcome::Abilities {
			username,
			xml,
			entries,
		} => {
			let mut out = format!("user: {}\n", username);
			out.push_str(&format!("abilities ({}):\n", entries.len()));
			out.push_str("module       name                      kind\n");
			for e in entries {
				out.push_str(&format!("{:<12} {:<25} {}\n", e.module, e.name, e.kind));
			}
			out.push_str("\nxml:\n");
			out.push_str(xml);
			out.push('\n');
			(out, String::new())
		}
	}
}

/// Longest camera-supplied string, in characters, printed in human mode.
const URL_DISPLAY_MAX: usize = 512;

/// Replace control characters with their escaped form so camera-supplied
/// text cannot inject terminal escape sequences.
pub fn sanitize_display(s: &str) -> String {
	s.chars()
		.map(|c| {
			if c.is_control() {
				c.escape_default().to_string()
			} else {
				c.to_string()
			}
		})
		.collect()
}

/// [`sanitize_display`] after cutting `s` to `max` characters, marking a
/// cut with an ellipsis.
fn display_text(s: &str, max: usize) -> String {
	let mut out = sanitize_display(&s.chars().take(max).collect::<String>());
	if s.chars().nth(max).is_some() {
		out.push('…');
	}
	out
}

fn format_json_success(outcome: &Outcome) -> String {
	let mut v = serde_json::to_value(outcome).expect("result must serialize");
	if let serde_json::Value::Object(ref mut m) = v {
		// #[serde(tag = "kind")] makes every variant serialize to an object; strip + insert are safe.
		m.remove("kind");
		m.insert("ok".into(), serde_json::Value::Bool(true));
	}
	serde_json::to_string_pretty(&v).expect("value must serialize") + "\n"
}

pub fn format_failure(mode: Mode, err: &anyhow::Error, kind: &str) -> String {
	match mode {
		Mode::Human => format!("error: {:#}\n", err),
		Mode::Json => {
			let obj = serde_json::json!({
				"ok": false,
				"kind": kind,
				"error": format!("{:#}", err),
			});
			serde_json::to_string_pretty(&obj).expect("JSON must serialize") + "\n"
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn webhook_supported() -> Outcome {
		Outcome::Webhook {
			supported: true,
			code: None,
			enable: Some(true),
			url: Some("http://192.0.2.10/h".into()),
			verify_cert: Some(false),
		}
	}

	fn webhook_unsupported() -> Outcome {
		Outcome::Webhook {
			supported: false,
			code: None,
			enable: None,
			url: None,
			verify_cert: None,
		}
	}

	#[test]
	fn human_webhook_supported_lists_fields() {
		let (out, _) = format_success(Mode::Human, &webhook_supported());
		assert!(out.contains("Webhook: supported"), "{out}");
		assert!(out.contains("enable: on"), "{out}");
		assert!(out.contains("url: http://192.0.2.10/h"), "{out}");
		assert!(out.contains("verify_cert: off"), "{out}");
	}

	#[test]
	fn human_webhook_empty_url_is_shown_as_empty() {
		let (out, _) = format_success(
			Mode::Human,
			&Outcome::Webhook {
				supported: true,
				code: None,
				enable: Some(false),
				url: Some(String::new()),
				verify_cert: Some(true),
			},
		);
		assert!(out.contains("enable: off"), "{out}");
		assert!(out.contains("url: (empty)"), "{out}");
		assert!(out.contains("verify_cert: on"), "{out}");
	}

	#[test]
	fn human_webhook_url_control_chars_are_escaped() {
		let (out, _) = format_success(
			Mode::Human,
			&Outcome::Webhook {
				supported: true,
				code: None,
				enable: Some(true),
				url: Some("http://x/\u{1b}[31m".into()),
				verify_cert: Some(false),
			},
		);
		assert!(!out.contains('\u{1b}'), "{out:?}");
		assert!(out.contains("http://x/\\u{1b}[31m"), "{out}");
	}

	#[test]
	fn human_webhook_long_url_is_truncated_with_ellipsis() {
		let (out, _) = format_success(
			Mode::Human,
			&Outcome::Webhook {
				supported: true,
				code: None,
				enable: Some(true),
				url: Some("a".repeat(URL_DISPLAY_MAX + 10)),
				verify_cert: Some(false),
			},
		);
		let line = out.lines().find(|l| l.contains("url:")).unwrap();
		assert_eq!(
			line.trim(),
			format!("url: {}…", "a".repeat(URL_DISPLAY_MAX))
		);
	}

	#[test]
	fn sanitize_display_escapes_controls_only() {
		assert_eq!(sanitize_display("ok é\n\t\u{7f}"), "ok é\\n\\t\\u{7f}");
	}

	#[test]
	fn human_webhook_unsupported() {
		let (out, _) = format_success(Mode::Human, &webhook_unsupported());
		assert_eq!(out, "Webhook: unsupported\n");
	}

	fn webhook_error_reply() -> Outcome {
		Outcome::Webhook {
			supported: false,
			code: Some(404),
			enable: None,
			url: None,
			verify_cert: None,
		}
	}

	#[test]
	fn human_webhook_unsupported_names_the_reply_code() {
		let (out, _) = format_success(Mode::Human, &webhook_error_reply());
		assert_eq!(out, "Webhook: unsupported (camera replied 404)\n");
	}

	#[test]
	fn json_webhook_supported_and_unsupported() {
		let (out, _) = format_success(Mode::Json, &webhook_supported());
		let v: serde_json::Value = serde_json::from_str(&out).unwrap();
		assert_eq!(v["ok"], true);
		assert_eq!(v["supported"], true);
		assert_eq!(v["enable"], true);
		assert_eq!(v["url"], "http://192.0.2.10/h");
		assert_eq!(v["verify_cert"], false);
		assert!(v.get("code").is_none(), "{out}");
		let (out, _) = format_success(Mode::Json, &webhook_unsupported());
		let v: serde_json::Value = serde_json::from_str(&out).unwrap();
		assert_eq!(v["supported"], false);
		assert!(v["url"].is_null());
		assert!(v.get("code").is_none(), "{out}");
		let (out, _) = format_success(Mode::Json, &webhook_error_reply());
		let v: serde_json::Value = serde_json::from_str(&out).unwrap();
		assert_eq!(v["supported"], false);
		assert_eq!(v["code"], 404);
	}

	#[test]
	fn human_battery() {
		let r = Outcome::Battery {
			percent: 87,
			voltage_mv: 3940,
			charge_status: "none".into(),
			low_power: false,
		};
		let (stdout, stderr) = format_success(Mode::Human, &r);
		assert_eq!(
			stdout,
			"Battery: 87% (3.94 V, charge=none, low_power=false)\n"
		);
		assert_eq!(stderr, "");
	}

	#[test]
	fn json_battery_is_single_object() {
		let r = Outcome::Battery {
			percent: 87,
			voltage_mv: 3940,
			charge_status: "none".into(),
			low_power: false,
		};
		let (stdout, stderr) = format_success(Mode::Json, &r);
		let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(parsed["ok"], true);
		assert_eq!(parsed["percent"], 87);
		// Raw integer mV — no f32 → f64 widening, no `3.941000061035156`.
		assert_eq!(parsed["voltage_mv"], 3940);
		assert!(
			parsed.get("voltage_v").is_none(),
			"legacy voltage_v field must not appear; got {parsed}"
		);
		assert_eq!(parsed["charge_status"], "none");
		assert_eq!(parsed["low_power"], false);
		assert!(parsed.get("kind").is_none());
		assert_eq!(stderr, "");
	}

	#[test]
	fn human_reboot_goes_to_stderr() {
		let (stdout, stderr) = format_success(Mode::Human, &Outcome::Reboot);
		assert_eq!(stdout, "");
		assert!(stderr.contains("reboot"));
	}

	#[test]
	fn human_snapshot_without_path_counts_bytes_on_stderr() {
		let r = Outcome::Snapshot {
			bytes: 12345,
			path: None,
			format: "jpeg".into(),
		};
		let (stdout, stderr) = format_success(Mode::Human, &r);
		assert_eq!(stdout, "");
		assert!(stderr.contains("12345"));
		assert!(stderr.contains("jpeg"));
	}

	#[test]
	fn human_snapshot_with_path_mentions_path() {
		let r = Outcome::Snapshot {
			bytes: 100,
			path: Some("/tmp/x.jpg".into()),
			format: "jpeg".into(),
		};
		let (_, stderr) = format_success(Mode::Human, &r);
		assert!(stderr.contains("/tmp/x.jpg"));
		assert!(stderr.contains("100"));
	}

	#[test]
	fn human_snapshot_stream_mentions_format() {
		let r = Outcome::Snapshot {
			bytes: 54321,
			path: Some("/tmp/x.h264".into()),
			format: "h264".into(),
		};
		let (_, stderr) = format_success(Mode::Human, &r);
		assert!(stderr.contains("h264"));
		assert!(stderr.contains("/tmp/x.h264"));
	}

	#[test]
	fn human_presets_lists_rows() {
		let r = Outcome::Presets {
			presets: vec![
				Preset {
					id: 0,
					name: Some("home".into()),
				},
				Preset { id: 1, name: None },
			],
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(stdout.contains("home"));
		assert!(stdout.contains("0"));
		assert!(stdout.contains("1"));
	}

	#[test]
	fn human_version_prints_model_firmware() {
		let r = Outcome::Version {
			model: "Argus 3 Pro".into(),
			firmware: "v3.1.0.1234_23112233".into(),
			hardware: "IPC_523SD8MP".into(),
			build_day: "build 23112233".into(),
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(stdout.contains("Argus 3 Pro"));
		assert!(stdout.contains("v3.1.0.1234_23112233"));
		assert!(stdout.contains("IPC_523SD8MP"));
	}

	#[test]
	fn human_floodlight_on_vs_off() {
		// Floodlight state is a query-result (used for both read and
		// set paths), so it goes to stdout.
		let (stdout_on, _) = format_success(Mode::Human, &Outcome::Floodlight { state: true });
		let (stdout_off, _) = format_success(Mode::Human, &Outcome::Floodlight { state: false });
		assert!(stdout_on.contains("on"));
		assert!(stdout_off.contains("off"));
	}

	#[test]
	fn human_set_time_mentions_utc() {
		let r = Outcome::SetTime {
			utc: "2026-04-24T19:43:17Z".into(),
		};
		let (_, stderr) = format_success(Mode::Human, &r);
		assert!(stderr.contains("2026-04-24T19:43:17Z"));
	}

	#[test]
	fn human_siren_goes_to_stderr() {
		let (stdout, stderr) = format_success(Mode::Human, &Outcome::Siren);
		assert_eq!(stdout, "");
		assert!(stderr.contains("siren"));
	}

	#[test]
	fn json_reboot_has_ok_true() {
		let (stdout, _) = format_success(Mode::Json, &Outcome::Reboot);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert!(v.get("kind").is_none());
	}

	#[test]
	fn json_failure_shape() {
		let e = anyhow::anyhow!("login timed out");
		let s = format_failure(Mode::Json, &e, "connection");
		let v: serde_json::Value = serde_json::from_str(&s).unwrap();
		assert_eq!(v["ok"], false);
		assert_eq!(v["kind"], "connection");
		assert_eq!(v["error"], "login timed out");
	}

	#[test]
	fn human_failure_shape() {
		let e = anyhow::anyhow!("login timed out");
		let s = format_failure(Mode::Human, &e, "connection");
		assert!(s.starts_with("error:"));
		assert!(s.contains("login timed out"));
	}

	#[test]
	fn json_output_ends_with_newline() {
		let (stdout, _) = format_success(Mode::Json, &Outcome::Reboot);
		assert!(stdout.ends_with('\n'));
	}

	#[test]
	fn json_presets_emits_array_of_objects() {
		let r = Outcome::Presets {
			presets: vec![
				Preset {
					id: 0,
					name: Some("home".into()),
				},
				Preset { id: 1, name: None },
			],
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert!(v.get("kind").is_none());
		let arr = v["presets"]
			.as_array()
			.expect("presets must be a JSON array");
		assert_eq!(arr.len(), 2);
		assert_eq!(arr[0]["id"], 0);
		assert_eq!(arr[0]["name"], "home");
		assert_eq!(arr[1]["id"], 1);
		assert!(arr[1]["name"].is_null());
	}

	#[test]
	fn human_pir_with_and_without_sensitivity() {
		let (on_some, _) = format_success(
			Mode::Human,
			&Outcome::Pir {
				enable: true,
				sensitivity: Some(50),
			},
		);
		assert!(on_some.contains("on"));
		assert!(on_some.contains("50"));
		let (off_none, _) = format_success(
			Mode::Human,
			&Outcome::Pir {
				enable: false,
				sensitivity: None,
			},
		);
		assert!(off_none.contains("off"));
		assert!(!off_none.contains("sensitivity"));
	}

	#[test]
	fn human_status_light_on_and_off() {
		let (on, _) = format_success(Mode::Human, &Outcome::StatusLight { state: true });
		let (off, _) = format_success(Mode::Human, &Outcome::StatusLight { state: false });
		assert!(on.contains("on"));
		assert!(off.contains("off"));
	}

	#[test]
	fn human_ptz_move_to_mentions_preset() {
		let (_, stderr) = format_success(Mode::Human, &Outcome::PtzMoveTo { preset_id: 4 });
		assert!(stderr.contains("preset"));
		assert!(stderr.contains("4"));
	}

	#[test]
	fn human_ptz_assign_mentions_name() {
		let (_, stderr) = format_success(
			Mode::Human,
			&Outcome::PtzAssign {
				preset_id: 2,
				name: "deck".into(),
			},
		);
		assert!(stderr.contains("2"));
		assert!(stderr.contains("deck"));
	}

	#[test]
	fn human_ptz_control_with_and_without_speed() {
		let (_, with_speed) = format_success(
			Mode::Human,
			&Outcome::PtzControl {
				direction: "up".into(),
				amount: 3,
				speed: Some(5),
			},
		);
		assert!(with_speed.contains("up"));
		assert!(with_speed.contains("speed 5"));
		let (_, no_speed) = format_success(
			Mode::Human,
			&Outcome::PtzControl {
				direction: "left".into(),
				amount: 1,
				speed: None,
			},
		);
		assert!(no_speed.contains("left"));
		assert!(!no_speed.contains("speed"));
	}

	#[test]
	fn human_ptz_zoom_prints_amount() {
		let (_, s) = format_success(Mode::Human, &Outcome::PtzZoom { amount: 2.5 });
		assert!(s.contains("2.50"));
	}

	#[test]
	fn human_service_all_enabled_states() {
		let (enabled, _) = format_success(
			Mode::Human,
			&Outcome::Service {
				service: "http".into(),
				port: 80,
				enabled: Some(true),
			},
		);
		assert!(enabled.contains("enabled"));
		assert!(enabled.contains("80"));
		let (disabled, _) = format_success(
			Mode::Human,
			&Outcome::Service {
				service: "http".into(),
				port: 80,
				enabled: Some(false),
			},
		);
		assert!(disabled.contains("disabled"));
		let (unknown, _) = format_success(
			Mode::Human,
			&Outcome::Service {
				service: "rtsp".into(),
				port: 554,
				enabled: None,
			},
		);
		assert!(unknown.contains("unknown"));
	}

	#[test]
	fn human_service_list_renders_all_three_states() {
		let (out, _) = format_success(
			Mode::Human,
			&Outcome::ServiceList {
				services: vec![
					ServiceEntry {
						name: "a".into(),
						port: 1,
						enabled: Some(true),
					},
					ServiceEntry {
						name: "b".into(),
						port: 2,
						enabled: Some(false),
					},
					ServiceEntry {
						name: "c".into(),
						port: 3,
						enabled: None,
					},
				],
			},
		);
		assert!(out.contains("service"));
		assert!(out.contains("enabled"));
		assert!(out.contains("disabled"));
		assert!(out.contains("unknown"));
	}

	#[test]
	fn human_users_list_maps_levels() {
		let (out, _) = format_success(
			Mode::Human,
			&Outcome::UsersList {
				users: vec![
					UserInfo {
						name: "admin".into(),
						level: 1,
					},
					UserInfo {
						name: "viewer".into(),
						level: 0,
					},
					UserInfo {
						name: "weird".into(),
						level: 42,
					},
				],
			},
		);
		assert!(out.contains("admin"));
		assert!(out.contains("viewer"));
		assert!(out.contains("user"));
		assert!(out.contains("?"));
	}

	#[test]
	fn human_user_changed_message() {
		let (_, stderr) = format_success(
			Mode::Human,
			&Outcome::UserChanged {
				name: "bob".into(),
				action: "added".into(),
			},
		);
		assert!(stderr.contains("added"));
		assert!(stderr.contains("bob"));
	}

	#[test]
	fn human_abilities_lists_entries_and_xml() {
		let r = Outcome::Abilities {
			username: "admin".into(),
			xml: "<abilityInfo>...</abilityInfo>".into(),
			entries: vec![
				AbilityEntry {
					module: "system".into(),
					name: "reboot".into(),
					kind: "rw".into(),
				},
				AbilityEntry {
					module: "ptz".into(),
					name: "two_way_audio".into(),
					kind: "ro".into(),
				},
			],
		};
		let (stdout, stderr) = format_success(Mode::Human, &r);
		assert_eq!(stderr, "");
		assert!(stdout.contains("user: admin"));
		assert!(stdout.contains("abilities (2):"));
		assert!(stdout.contains("system"));
		assert!(stdout.contains("reboot"));
		assert!(stdout.contains("rw"));
		assert!(stdout.contains("two_way_audio"));
		assert!(stdout.contains("ro"));
		assert!(stdout.contains("<abilityInfo>...</abilityInfo>"));
	}

	#[test]
	fn json_abilities_emits_username_xml_and_entries_array() {
		let r = Outcome::Abilities {
			username: "admin".into(),
			xml: "<abilityInfo/>".into(),
			entries: vec![AbilityEntry {
				module: "network".into(),
				name: "ping".into(),
				kind: "rw".into(),
			}],
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert!(v.get("kind").is_none());
		assert_eq!(v["username"], "admin");
		assert_eq!(v["xml"], "<abilityInfo/>");
		let arr = v["entries"]
			.as_array()
			.expect("entries must be a JSON array");
		assert_eq!(arr.len(), 1);
		// AbilityEntry.kind survives the top-level "kind" strip — strip
		// is positional (top-level only), nested struct fields untouched.
		assert_eq!(arr[0]["module"], "network");
		assert_eq!(arr[0]["name"], "ping");
		assert_eq!(arr[0]["kind"], "rw");
	}

	#[test]
	fn json_version_emits_all_four_fields() {
		let r = Outcome::Version {
			model: "Argus 3 Pro".into(),
			firmware: "v3.1.0.1234_23112233".into(),
			hardware: "IPC_523SD8MP".into(),
			build_day: "build 23112233".into(),
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert_eq!(v["model"], "Argus 3 Pro");
		assert_eq!(v["firmware"], "v3.1.0.1234_23112233");
		assert_eq!(v["hardware"], "IPC_523SD8MP");
		assert_eq!(v["build_day"], "build 23112233");
		assert!(v.get("kind").is_none());
	}
}
