//! MQTT control topic parsing and command dispatch.
//!
//! Parses incoming MQTT messages on `{prefix}/{cam}/control/*` and
//! `{prefix}/{cam}/query/*` topics into typed `ControlCommand` values.
//! `{prefix}` is the `mqtt.topic_prefix` config value (default
//! `"bairelay"`; `"neolink"` for legacy migration).

use std::str;

/// PTZ movement direction.
#[derive(Debug, Clone, PartialEq)]
pub enum PtzDirection {
	Up,
	Down,
	Left,
	Right,
}

/// IR night-vision mode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IrMode {
	On,
	Off,
	Auto,
}

impl IrMode {
	/// Canonical MQTT payload: the same strings `control/ir` accepts and
	/// the HA `select` entity lists as options.
	pub fn as_payload(self) -> &'static str {
		match self {
			IrMode::On => "on",
			IrMode::Off => "off",
			IrMode::Auto => "auto",
		}
	}
}

/// A validated control or query command extracted from an MQTT message.
#[derive(Debug, Clone)]
pub enum ControlCommand {
	Floodlight {
		camera: String,
		state: bool,
	},
	FloodlightTasks {
		camera: String,
		state: bool,
	},
	Led {
		camera: String,
		state: bool,
	},
	Ir {
		camera: String,
		mode: IrMode,
	},
	Pir {
		camera: String,
		state: bool,
	},
	Reboot {
		camera: String,
	},
	Ptz {
		camera: String,
		direction: PtzDirection,
		amount: f32,
	},
	PtzPreset {
		camera: String,
		preset_id: u8,
	},
	/// Variant used by HA's preset `select` entity, which publishes the
	/// preset *name* (the option label). The dispatcher resolves the
	/// name to an id via the camera's `preset_cache`.
	PtzPresetByName {
		camera: String,
		name: String,
	},
	PtzAssign {
		camera: String,
		preset_id: u8,
		name: String,
	},
	Zoom {
		camera: String,
		level: f32,
	},
	Siren {
		camera: String,
		state: bool,
	},
	Wakeup {
		camera: String,
		minutes: u32,
	},
	// Query commands (triggered by publishing to query topics)
	QueryBattery {
		camera: String,
	},
	QueryPreview {
		camera: String,
	},
	QueryPir {
		camera: String,
	},
	QueryPtzPreset {
		camera: String,
	},
}

impl ControlCommand {
	/// Returns the MQTT control topic this command was received on.
	/// Used for publishing OK/FAIL replies. `prefix` is the configured
	/// `mqtt.topic_prefix`.
	pub fn control_topic(&self, prefix: &str) -> String {
		let cam = self.camera_name();
		match self {
			ControlCommand::Floodlight { .. } => format!("{prefix}/{cam}/control/floodlight"),
			ControlCommand::FloodlightTasks { .. } => {
				format!("{prefix}/{cam}/control/floodlight_tasks")
			}
			ControlCommand::Led { .. } => format!("{prefix}/{cam}/control/led"),
			ControlCommand::Ir { .. } => format!("{prefix}/{cam}/control/ir"),
			ControlCommand::Pir { .. } => format!("{prefix}/{cam}/control/pir"),
			ControlCommand::Reboot { .. } => format!("{prefix}/{cam}/control/reboot"),
			ControlCommand::Ptz { .. } => format!("{prefix}/{cam}/control/ptz"),
			ControlCommand::PtzPreset { .. } => format!("{prefix}/{cam}/control/ptz/preset"),
			ControlCommand::PtzPresetByName { .. } => format!("{prefix}/{cam}/control/ptz/preset"),
			ControlCommand::PtzAssign { .. } => format!("{prefix}/{cam}/control/ptz/assign"),
			ControlCommand::Zoom { .. } => format!("{prefix}/{cam}/control/zoom"),
			ControlCommand::Siren { .. } => format!("{prefix}/{cam}/control/siren"),
			ControlCommand::Wakeup { .. } => format!("{prefix}/{cam}/control/wakeup"),
			ControlCommand::QueryBattery { .. } => format!("{prefix}/{cam}/query/battery"),
			ControlCommand::QueryPreview { .. } => format!("{prefix}/{cam}/query/preview"),
			ControlCommand::QueryPir { .. } => format!("{prefix}/{cam}/query/pir"),
			ControlCommand::QueryPtzPreset { .. } => format!("{prefix}/{cam}/query/ptz/preset"),
		}
	}

	/// Returns the camera name that this command targets.
	pub fn camera_name(&self) -> &str {
		match self {
			ControlCommand::Floodlight { camera, .. } => camera,
			ControlCommand::FloodlightTasks { camera, .. } => camera,
			ControlCommand::Led { camera, .. } => camera,
			ControlCommand::Ir { camera, .. } => camera,
			ControlCommand::Pir { camera, .. } => camera,
			ControlCommand::Reboot { camera, .. } => camera,
			ControlCommand::Ptz { camera, .. } => camera,
			ControlCommand::PtzPreset { camera, .. } => camera,
			ControlCommand::PtzPresetByName { camera, .. } => camera,
			ControlCommand::PtzAssign { camera, .. } => camera,
			ControlCommand::Zoom { camera, .. } => camera,
			ControlCommand::Siren { camera, .. } => camera,
			ControlCommand::Wakeup { camera, .. } => camera,
			ControlCommand::QueryBattery { camera } => camera,
			ControlCommand::QueryPreview { camera } => camera,
			ControlCommand::QueryPir { camera } => camera,
			ControlCommand::QueryPtzPreset { camera } => camera,
		}
	}
}

/// Parse an MQTT topic + payload into a `ControlCommand`.
///
/// Returns `None` if the topic is unrecognised or the payload is
/// malformed. `prefix` must match the leading segment of `topic`;
/// mismatched prefixes return `None` so a reused subscription on a
/// second prefix cannot trigger a command. All payloads are validated
/// — no raw user data is passed through unchecked.
pub fn parse_control_message(prefix: &str, topic: &str, payload: &[u8]) -> Option<ControlCommand> {
	let parts: Vec<&str> = topic.split('/').collect();

	// Minimum: {prefix} / {cam} / {category} / {action}
	// Status topic ({prefix}/{cam}/status) has 3 parts but we don't handle that here.
	// Control/query: {prefix}/{cam}/control/{action} or {prefix}/{cam}/query/{action}
	if parts.len() < 3 || parts[0] != prefix {
		return None;
	}

	let camera = parts[1].to_string();

	// Validate camera name: must be non-empty and contain only ASCII
	// alphanumerics + `_` + `-`. `char::is_alphanumeric` is the
	// **Unicode** Alphabetic + Numeric category (e.g. "café", "中文" all
	// pass) — the doc claims ASCII-only and downstream code (HA
	// discovery's title_case at discovery/mod.rs:269-272) assumes
	// closed-world ASCII. Use `is_ascii_alphanumeric` to keep the
	// promise.
	if camera.is_empty()
		|| !camera
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
	{
		return None;
	}

	let payload_str = str::from_utf8(payload).ok()?;
	let payload_trimmed = payload_str.trim();

	if parts.len() == 5 && parts[2] == "control" && parts[3] == "ptz" {
		let sub = parts[4];
		parse_control_ptz_sub(&camera, sub, payload_trimmed)
	} else if parts.len() == 5 && parts[2] == "query" && parts[3] == "ptz" {
		// Every query topic is also the dispatcher's reply topic
		// (`OK`/`FAIL` published back on `query/...`). Without filtering
		// the reply parses as a fresh query, which dispatches and
		// publishes another `OK`, etc. — an unbounded self-loop hammering
		// the camera. Drop the two reserved reply tokens at the parse
		// step. Mirrors the same guard on `control/ptz/preset`.
		if is_reserved_reply_token(payload_trimmed) {
			return None;
		}
		let sub = parts[4];
		parse_query_ptz_sub(&camera, sub)
	} else if parts.len() == 4 && parts[2] == "control" {
		let action = parts[3];
		parse_control_action(&camera, action, payload_trimmed)
	} else if parts.len() == 4 && parts[2] == "query" {
		if is_reserved_reply_token(payload_trimmed) {
			return None;
		}
		let action = parts[3];
		parse_query_action(&camera, action)
	} else {
		None
	}
}

fn is_reserved_reply_token(payload: &str) -> bool {
	payload.eq_ignore_ascii_case("OK") || payload.eq_ignore_ascii_case("FAIL")
}

fn parse_on_off(payload: &str) -> Option<bool> {
	match payload.to_lowercase().as_str() {
		"on" | "true" | "1" => Some(true),
		"off" | "false" | "0" => Some(false),
		_ => None,
	}
}

fn parse_ir_mode(payload: &str) -> Option<IrMode> {
	match payload.to_lowercase().as_str() {
		"on" | "true" | "1" => Some(IrMode::On),
		"off" | "false" | "0" => Some(IrMode::Off),
		"auto" => Some(IrMode::Auto),
		_ => None,
	}
}

fn parse_ptz_direction(s: &str) -> Option<PtzDirection> {
	match s.to_lowercase().as_str() {
		"up" => Some(PtzDirection::Up),
		"down" => Some(PtzDirection::Down),
		"left" => Some(PtzDirection::Left),
		"right" => Some(PtzDirection::Right),
		_ => None,
	}
}

fn parse_control_action(camera: &str, action: &str, payload: &str) -> Option<ControlCommand> {
	// Every control topic is also the dispatcher's reply topic (`OK` /
	// `FAIL` published back on the same `control/...`). Closed-payload
	// actions (`floodlight`, `led`, `ir`, `pir`, `siren`, `zoom`,
	// `wakeup`, `ptz`, ...) reject those tokens at the per-arm parser
	// below, but **`reboot` accepts ANY payload**, so the dispatcher's
	// own `OK` reply re-parses as a fresh `Reboot` and an unbounded
	// loop hammers the camera. Filtering at the top of
	// `parse_control_action` guards every present and future open-
	// payload action by default. Mirrors the same guard on the query
	// branches in `parse_control_message`.
	if is_reserved_reply_token(payload) {
		return None;
	}
	match action {
		"floodlight" => {
			let state = parse_on_off(payload)?;
			Some(ControlCommand::Floodlight {
				camera: camera.to_string(),
				state,
			})
		}
		"floodlight_tasks" => {
			let state = parse_on_off(payload)?;
			Some(ControlCommand::FloodlightTasks {
				camera: camera.to_string(),
				state,
			})
		}
		"led" => {
			let state = parse_on_off(payload)?;
			Some(ControlCommand::Led {
				camera: camera.to_string(),
				state,
			})
		}
		"ir" => {
			let mode = parse_ir_mode(payload)?;
			Some(ControlCommand::Ir {
				camera: camera.to_string(),
				mode,
			})
		}
		"pir" => {
			let state = parse_on_off(payload)?;
			Some(ControlCommand::Pir {
				camera: camera.to_string(),
				state,
			})
		}
		"reboot" => {
			// Reboot takes no meaningful payload; accept empty or any value
			Some(ControlCommand::Reboot {
				camera: camera.to_string(),
			})
		}
		"ptz" => {
			// Payload format: "{direction}" or "{direction} {amount}"
			let parts: Vec<&str> = payload.split_whitespace().collect();
			if parts.is_empty() {
				return None;
			}
			let direction = parse_ptz_direction(parts[0])?;
			let amount = if parts.len() >= 2 {
				parts[1].parse::<f32>().ok()?
			} else {
				32.0 // default amount
			};
			// Validate amount range
			if !amount.is_finite() || amount < 0.0 {
				return None;
			}
			Some(ControlCommand::Ptz {
				camera: camera.to_string(),
				direction,
				amount,
			})
		}
		"zoom" => {
			let level = payload.parse::<f32>().ok()?;
			if !level.is_finite() || level < 0.0 {
				return None;
			}
			Some(ControlCommand::Zoom {
				camera: camera.to_string(),
				level,
			})
		}
		"siren" => {
			let state = parse_on_off(payload)?;
			Some(ControlCommand::Siren {
				camera: camera.to_string(),
				state,
			})
		}
		"wakeup" => {
			let minutes = payload.parse::<u32>().ok()?;
			if minutes == 0 || minutes > 1440 {
				return None;
			}
			Some(ControlCommand::Wakeup {
				camera: camera.to_string(),
				minutes,
			})
		}
		_ => None,
	}
}

fn parse_query_action(camera: &str, action: &str) -> Option<ControlCommand> {
	match action {
		"battery" => Some(ControlCommand::QueryBattery {
			camera: camera.to_string(),
		}),
		"preview" => Some(ControlCommand::QueryPreview {
			camera: camera.to_string(),
		}),
		"pir" => Some(ControlCommand::QueryPir {
			camera: camera.to_string(),
		}),
		_ => None,
	}
}

fn parse_control_ptz_sub(camera: &str, sub: &str, payload: &str) -> Option<ControlCommand> {
	match sub {
		"preset" => {
			// Numeric → PtzPreset (back-compat with neolink); anything
			// else → PtzPresetByName, which the dispatcher resolves
			// against the camera's preset cache. HA's mqtt-discovered
			// `select` entity always emits the option label, so the
			// name path is the live one.
			if let Ok(preset_id) = payload.parse::<u8>() {
				return Some(ControlCommand::PtzPreset {
					camera: camera.to_string(),
					preset_id,
				});
			}
			let name = payload.trim();
			if name.is_empty() {
				return None;
			}
			// `control/<topic>` is *also* the reply topic for the
			// dispatcher's `OK`/`FAIL` convention. For closed-payload
			// commands the reply doesn't reparse; for the open-name
			// preset variant it would, producing an infinite
			// self-loop ("OK" reply → reparsed as PtzPresetByName{
			// name="OK"} → dispatch warns + publishes another "OK"
			// → ...). Filter the two reserved reply tokens at the
			// parse step so the loop can't even start. No real preset
			// is named "OK" or "FAIL".
			if is_reserved_reply_token(name) {
				return None;
			}
			Some(ControlCommand::PtzPresetByName {
				camera: camera.to_string(),
				name: name.to_string(),
			})
		}
		"assign" => {
			// Payload format: "{id} {name}" (space-separated)
			let (id_str, name) = payload.split_once(' ')?;
			let preset_id = id_str.parse::<u8>().ok()?;
			let name = name.trim();
			if name.is_empty() {
				return None;
			}
			Some(ControlCommand::PtzAssign {
				camera: camera.to_string(),
				preset_id,
				name: name.to_string(),
			})
		}
		_ => None,
	}
}

fn parse_query_ptz_sub(camera: &str, sub: &str) -> Option<ControlCommand> {
	match sub {
		"preset" => Some(ControlCommand::QueryPtzPreset {
			camera: camera.to_string(),
		}),
		_ => None,
	}
}
