//! Settings entities for the Home Assistant discovery.
//!
//! The whole settings specification lives in the operator's private protocol
//! notebook; this file is only its MQTT shopfront. It is
//! **table-driven** for one reason: every entity described here has to
//! land exactly on the topics the binary publishes (`status/<family>/<leaf>`)
//! and accepts (`control/<family>/<leaf>`). One single specification list,
//! read by both the discovery emitter and the tests, is the only
//! way to guarantee the two sides do not drift apart from each other.
//!
//! Two HA platforms missing from the original version are added here:
//!
//! * `number` — a bounded numeric setting. The bounds come **from the protocol**,
//!   never from taste: 0-100 for the volumes, 1-100 for the
//!   denoiser, 0-255 for the image sliders, 0-50 for the motion
//!   sensitivity. An unknown bound is not invented: the setting is then
//!   exposed read-only, or not exposed at all.
//! * `text` — a string (NTP server, names). Bounded in length, because
//!   the firmware does not say what it accepts beyond what it prints.

use std::collections::HashSet;

use serde::Serialize;

use crate::settings::Family;

use super::{
	DiscoveryAvailability, DiscoveryContext, DiscoveryDevice, Feature, availability_blocks,
	cam_topic, config_topic, device_block, to_json, unique,
};

/// An HA `number` entity: bounded `command_topic` + `state_topic`.
#[derive(Serialize, Debug)]
pub struct DiscoveryNumber {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: Vec<DiscoveryAvailability>,
	pub command_topic: String,
	pub state_topic: String,
	pub min: f64,
	pub max: f64,
	pub step: f64,
	/// `"slider"` (a slider) or `"box"` (a text box) — HA requires one of the two.
	pub mode: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub unit_of_measurement: Option<&'static str>,
}

/// An HA `text` entity: a string bounded in length.
#[derive(Serialize, Debug)]
pub struct DiscoveryText {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: Vec<DiscoveryAvailability>,
	pub command_topic: String,
	pub state_topic: String,
	/// Minimum accepted length (0 = clearing allowed).
	pub min: usize,
	/// Maximum accepted length, the one the bridge validates before writing.
	pub max: usize,
	/// `"text"` (readable) — never `"password"`: no setting of this bridge
	/// is a secret, and what could be one (`<Wifi><key>`) is
	/// not published at all.
	pub mode: &'static str,
}

/// A state sensor whose unit may be absent.
///
/// `DiscoverySensor` (the historical struct, for the battery) imposes a
/// unit. Several settings states have **no** documented
/// unit — Wi-Fi's `<signal>` is a number nobody knows to be in dBm or
/// in percent. Giving them a unit would be a lie displayed in HA,
/// so the unit is optional here.
#[derive(Serialize, Debug)]
pub struct DiscoveryStateSensor {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: Vec<DiscoveryAvailability>,
	pub state_topic: String,
	/// `measurement` for a number, `None` for a textual state. Home
	/// Assistant refuses a numeric `state_class` on a state that is not
	/// one: publishing one on a language or a chime list would replace
	/// a silence with a warning repeated in the log.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_class: Option<&'static str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub unit_of_measurement: Option<&'static str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub device_class: Option<&'static str>,
}

/// The HA platform and its settings for one entity specification.
#[derive(Debug, Clone, Copy)]
pub enum EntityKind {
	/// Bounded numeric setting (the `number` platform).
	Number {
		min: f64,
		max: f64,
		step: f64,
		unit: Option<&'static str>,
		/// `true` = slider, `false` = text box.
		slider: bool,
	},
	/// Bounded string (the `text` platform).
	Text { max_len: usize },
	/// On/off `switch`, re-read state as `"on"`/`"off"`.
	Switch,
	/// The **readable** twin of a `switch`: `binary_sensor` on the same
	/// `"on"`/`"off"` texts, with no command topic. It is not a
	/// `FlagSensor`: that one reads the protocol's raw `"1"`/`"0"`, whereas
	/// a switch publishes the text the `switch` entity expects.
	OnOffSensor,
	/// `select` with closed options, exactly the texts the camera accepts.
	Select(&'static [&'static str]),
	/// Read-only `binary_sensor` on a camera `"1"`/`"0"`.
	FlagSensor {
		device_class: Option<&'static str>,
	},
	/// Numeric or textual `sensor`, read-only.
	StateSensor {
		unit: Option<&'static str>,
		device_class: Option<&'static str>,
		/// `measurement` (kept in the history) or `None`.
		class: Option<&'static str>,
	},
	/// `button`: one press, one MQTT message.
	Button { payload: &'static str },
}

/// A settings entity: the topic family/leaf, the HA label, and the
/// platform. `family`/`leaf` **must** match
/// [`crate::settings::Family`] / [`crate::settings::SettingCommand`] on the
/// parsing side, and the topics the dispatch publishes after a re-read.
#[derive(Debug, Clone, Copy)]
pub struct EntitySpec {
	pub feature: Feature,
	pub family: &'static str,
	pub leaf: &'static str,
	/// Label glued after the camera name ("Porch Intercom volume").
	pub label: &'static str,
	pub icon: Option<&'static str>,
	pub kind: EntityKind,
}

/// What, among the kinds, **never** has a command topic. An entity
/// built on one of these kinds cannot be actuated from Home
/// Assistant: it is the shape in which a settings family is published
/// until the camera has proved it re-reads it and accepts it back.
pub fn is_read_only(kind: &EntityKind) -> bool {
	matches!(
		kind,
		EntityKind::FlagSensor { .. } | EntityKind::OnOffSensor | EntityKind::StateSensor { .. }
	)
}

impl EntitySpec {
	/// The HA platform (`switch`, `number`, …) and the topic segment
	/// `homeassistant/<platform>/...`. It is the **built** kind that decides,
	/// not the one written in the table: the readable copy of a `number` is a
	/// `sensor`, so its configuration topic changes segment — hence the
	/// operator's obligation to delete the entity Home Assistant
	/// keeps in memory (see “Ghost entities” in `docs/settings.md`).
	fn ha_platform(kind: &EntityKind) -> &'static str {
		match kind {
			EntityKind::Number { .. } => "number",
			EntityKind::Text { .. } => "text",
			EntityKind::Switch => "switch",
			EntityKind::Select(_) => "select",
			EntityKind::FlagSensor { .. } | EntityKind::OnOffSensor => "binary_sensor",
			EntityKind::StateSensor { .. } => "sensor",
			EntityKind::Button { .. } => "button",
		}
	}

	/// The HA unique identifier: `{prefix}_{cam}_{family}_{leaf}` — the
	/// family is in the id because several families have a
	/// `enable` or `sensitivity` leaf.
	pub fn unique_id(&self, ctx: &DiscoveryContext) -> String {
		unique(ctx.topic_prefix, ctx.camera_name, &format!("{}_{}", self.family, self.leaf))
	}

	/// The state topic the entity reads.
	pub fn state_topic(&self, ctx: &DiscoveryContext) -> String {
		cam_topic(ctx, "status", &format!("{}/{}", self.family, self.leaf))
	}

	/// The command topic, absent for what is read-only. The built
	/// kind decides, not the table's kind: that is what makes an
	/// un-re-read family publish with no command at all.
	pub fn command_topic(&self, ctx: &DiscoveryContext, kind: &EntityKind) -> Option<String> {
		if is_read_only(kind) {
			return None;
		}
		Some(cam_topic(ctx, "control", &format!("{}/{}", self.family, self.leaf)))
	}

	/// Builds `(config_topic, payload)` of the kind written in the table: the
	/// **modifiable** shape of the entity.
	pub fn build(&self, ctx: &DiscoveryContext, friendly: &str) -> (String, Vec<u8>) {
		self.build_kind(ctx, friendly, &self.kind)
	}

	/// Builds the entity as the family must be published **today**:
	/// modifiable if the camera answered a read of this family and
	/// accepts having the block sent back to it (`writable`), otherwise its read-only
	/// copy — same `unique_id`, same state topic, different platform, no
	/// `command_topic`.
	///
	/// `None` = no entity at all. A button has no readable equivalent:
	/// there is nothing to display, only something to trigger. Publishing it
	/// before the camera confirmed the chime would promise a button
	/// that does not ring — exactly the reproach made to the original version.
	pub fn build_from(
		&self,
		ctx: &DiscoveryContext,
		friendly: &str,
		writable: bool,
	) -> Option<(String, Vec<u8>)> {
		if writable {
			return Some(self.build(ctx, friendly));
		}
		let twin = self.read_only_twin()?;
		Some(self.build_kind(ctx, friendly, &twin))
	}

	/// The kind that displays the same leaf without offering a command. The bounds
	/// of a `number` are not in it: those are **write** bounds, and an
	/// entity that cannot write has nothing to do with them. The unit does stay:
	/// it says what the number read means.
	fn read_only_twin(&self) -> Option<EntityKind> {
		Some(match self.kind {
			EntityKind::Number { unit, .. } => {
				EntityKind::StateSensor { unit, device_class: None, class: Some("measurement") }
			}
			// A text and a choice are states like any other: no unit, no
			// `state_class` (Home Assistant refuses measured non-numeric).
			EntityKind::Text { .. } | EntityKind::Select(_) => {
				EntityKind::StateSensor { unit: None, device_class: None, class: None }
			}
			EntityKind::Switch => EntityKind::OnOffSensor,
			EntityKind::Button { .. } => return None,
			lu @ (EntityKind::FlagSensor { .. }
			| EntityKind::OnOffSensor
			| EntityKind::StateSensor { .. }) => lu,
		})
	}

	/// `(config_topic, payload)` for a given kind — the table's or its
	/// readable twin. The two share the same code so that a read-only
	/// copy cannot forget a block (device, availability).
	fn build_kind(
		&self,
		ctx: &DiscoveryContext,
		friendly: &str,
		kind: &EntityKind,
	) -> (String, Vec<u8>) {
		let unique_id = self.unique_id(ctx);
		// The label says **what it is**, never to whom it belongs: Home Assistant
		// already displays the device above the entity, and repeating the camera's
		// name ("Doorbell General volume") truncates the useful label to add
		// nothing. `unique_id` and the `device` block keep the camera's
		// name: those are identifiers, they are not renamed.
		let name = self.label.to_string();
		let device = device_block(ctx, friendly);
		let availability = availability_blocks(ctx);
		let state_topic = self.state_topic(ctx);
		let command_topic = self.command_topic(ctx, kind);
		let payload = match kind {
			EntityKind::Number {
				min,
				max,
				step,
				unit,
				slider,
			} => to_json(&DiscoveryNumber {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				command_topic: command_topic.expect("a number has a command_topic"),
				state_topic,
				min: *min,
				max: *max,
				step: *step,
				mode: if *slider { "slider" } else { "box" },
				unit_of_measurement: *unit,
			}),
			EntityKind::Text { max_len } => to_json(&DiscoveryText {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				command_topic: command_topic.expect("a text has a command_topic"),
				state_topic,
				min: 0,
				max: *max_len,
				mode: "text",
			}),
			EntityKind::Switch => to_json(&super::DiscoverySwitch {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				command_topic: command_topic.expect("a switch has a command_topic"),
				payload_on: "on".to_string(),
				payload_off: "off".to_string(),
				state_topic: Some(state_topic),
				state_on: Some("on".to_string()),
				state_off: Some("off".to_string()),
			}),
			EntityKind::OnOffSensor => to_json(&super::DiscoveryBinarySensor {
				name,
				unique_id: unique_id.clone(),
				device_class: None,
				icon: self.icon.map(str::to_string),
				device,
				availability,
				// The two texts `put_switch` publishes after a re-read.
				payload_on: "on".to_string(),
				payload_off: "off".to_string(),
				state_topic,
			}),
			EntityKind::Select(options) => to_json(&super::DiscoverySelect {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				command_topic: command_topic.expect("a select has a command_topic"),
				options: options.iter().map(|s| s.to_string()).collect(),
				state_topic: Some(state_topic),
			}),
			EntityKind::FlagSensor { device_class } => to_json(&super::DiscoveryBinarySensor {
				name,
				unique_id: unique_id.clone(),
				device_class: device_class.map(str::to_string),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				payload_on: "1".to_string(),
				payload_off: "0".to_string(),
				state_topic,
			}),
			EntityKind::StateSensor {
				unit,
				device_class,
				class,
			} => to_json(&DiscoveryStateSensor {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				state_topic,
				// `class` is that line's decision, not a default value:
				// a textual line (JSON list, charge state, melody) that
				// claimed to be `measurement` would see Home Assistant refuse
				// its non-numeric states — the entity would stay "unknown".
				state_class: *class,
				unit_of_measurement: *unit,
				device_class: *device_class,
			}),
			EntityKind::Button { payload } => to_json(&super::DiscoveryButton {
				name,
				unique_id: unique_id.clone(),
				icon: self.icon.map(str::to_string),
				device,
				availability,
				command_topic: command_topic.expect("a button has a command_topic"),
				payload_press: Some((*payload).to_string()),
			}),
		};
		(
			config_topic(ctx, Self::ha_platform(kind), &unique_id),
			payload,
		)
	}

	/// The platform of a read-only copy (`sensor` or `binary_sensor`),
	/// for the tests and the log. `None` = no readable entity: only the
	/// button, which has no state, disappears entirely until the family
	/// is re-read.
	pub fn read_only_platform(&self) -> Option<&'static str> {
		Some(Self::ha_platform(&self.read_only_twin()?))
	}
}

/// The days accepted by `<AutoReboot><weekDay>`, in display order.
pub const WEEKDAYS: [&str; 8] = [
	"everyday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

/// The OSD date formats.
pub const OSD_FORMATS: [&str; 3] = ["DMY", "MDY", "YMD"];

/// The day/night modes normalised by the reference library.
pub const DAY_NIGHT_MODES: [&str; 3] = ["auto", "color", "blackAndWhite"];

/// Every settings entity, grouped by topic family.
///
/// The order is stable (the tests compare counters and lists of
/// topics). Each line carries the **protocol's** range: changing a bound
/// without a local capture behind you is writing a value the camera
/// rejettera en silence.
pub const SPECS: &[EntitySpec] = &[
	// ── audioCfg (264 read / 265 write) ─────────────────────────────────
	//
	// The block prints **three** volumes, and each one drives a different
	// speaker. The original version discovered only two of them, under a
	// generic label: "Intercom volume" wrote `<talkAndReplyVolume>` without
	// saying so, and `<volume>` — the camera's loudspeaker master volume —
	// had no entity at all. Each line therefore names the XML tag it
	// carries: it is the only thing that can be checked against a capture.
	EntitySpec {
		feature: Feature::Audio,
		family: "audio",
		// `<volume>`. Identifiant **nouveau** (`bairelay_<cam>_audio_general_volume`) :
		// `..._audio_volume` and `..._audio_visitor_volume` already exist at the
		// operator's and are neither renamed nor recycled.
		leaf: "general_volume",
		label: "Speaker volume",
		icon: Some("mdi:volume-high"),
		kind: EntityKind::Number { min: 0.0, max: 100.0, step: 1.0, unit: Some("%"), slider: true },
	},
	EntitySpec {
		feature: Feature::Audio,
		family: "audio",
		// `<talkAndReplyVolume>`: what the visitor hears during the exchange.
		leaf: "volume",
		label: "Intercom volume (talk)",
		icon: Some("mdi:walkie-talkie"),
		kind: EntityKind::Number { min: 0.0, max: 100.0, step: 1.0, unit: Some("%"), slider: true },
	},
	EntitySpec {
		feature: Feature::Audio,
		family: "audio",
		// `<visitorVolume>`: what the camera sends into the walkie-talkie.
		leaf: "visitor_volume",
		label: "Visitor volume",
		icon: Some("mdi:volume-heart"),
		kind: EntityKind::Number { min: 0.0, max: 100.0, step: 1.0, unit: Some("%"), slider: true },
	},
	EntitySpec {
		feature: Feature::Audio,
		family: "audio",
		leaf: "visitor_loudspeaker",
		label: "Visitor loudspeaker",
		icon: Some("mdi:speaker"),
		kind: EntityKind::Switch,
	},
	// ── aiDenoise (439 / 440) ───────────────────────────────────────────
	EntitySpec {
		feature: Feature::Denoise,
		family: "denoise",
		leaf: "enable",
		label: "Microphone denoise",
		icon: Some("mdi:noise-reduction"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::Denoise,
		family: "denoise",
		leaf: "level",
		label: "Force denoise",
		icon: Some("mdi:tune-variant"),
		kind: EntityKind::Number { min: 1.0, max: 100.0, step: 1.0, unit: Some("%"), slider: true },
	},
	// ── Image (26 / 25) : curseurs 0-255 ────────────────────────────────
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "bright",
		label: "Brightness",
		icon: Some("mdi:brightness-7"),
		kind: EntityKind::Number { min: 0.0, max: 255.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "contrast",
		label: "Contrast",
		icon: Some("mdi:contrast-box"),
		kind: EntityKind::Number { min: 0.0, max: 255.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "saturation",
		label: "Saturation",
		icon: Some("mdi:water-opacity"),
		kind: EntityKind::Number { min: 0.0, max: 255.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "hue",
		label: "Hue",
		icon: Some("mdi:palette"),
		kind: EntityKind::Number { min: 0.0, max: 255.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "sharpen",
		label: "Sharpness",
		icon: Some("mdi:image-filter-center-focus"),
		kind: EntityKind::Number { min: 0.0, max: 255.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Image,
		family: "image",
		leaf: "day_night",
		label: "Day/night mode",
		icon: Some("mdi:weather-night"),
		kind: EntityKind::Select(&DAY_NIGHT_MODES),
	},
	// ── OSD (44 / 45) ───────────────────────────────────────────────────
	EntitySpec {
		feature: Feature::Osd,
		family: "osd",
		leaf: "datetime_enable",
		label: "On-screen timestamp",
		icon: Some("mdi:clock-outline"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::Osd,
		family: "osd",
		leaf: "name_enable",
		label: "On-screen name",
		icon: Some("mdi:signature-text"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::Osd,
		family: "osd",
		leaf: "name",
		label: "On-screen name text",
		icon: Some("mdi:account-box"),
		kind: EntityKind::Text { max_len: 31 },
	},
	EntitySpec {
		feature: Feature::Osd,
		family: "osd",
		leaf: "watermark",
		label: "Watermark",
		icon: Some("mdi:watermark"),
		kind: EntityKind::Switch,
	},
	// ── Motion detection (46 / 47) ──────────────────────────────────────
	EntitySpec {
		feature: Feature::MotionSettings,
		family: "motion",
		leaf: "enable",
		label: "Motion detection",
		icon: Some("mdi:run-fast"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::MotionSettings,
		family: "motion",
		leaf: "sensitivity",
		label: "Motion sensitivity",
		icon: Some("mdi:speedometer"),
		// 0-50, higher = more sensitive (measured on the operator's doorbell).
		kind: EntityKind::Number { min: 0.0, max: 50.0, step: 1.0, unit: None, slider: true },
	},
	// ── PIR (212 / 213): the switch comes from `build_pir` ──────────────
	EntitySpec {
		feature: Feature::Pir,
		family: "pir",
		leaf: "sensitivity",
		label: "PIR sensitivity",
		icon: Some("mdi:sensor-outline"),
		// `sensiValue` has no documented range; 0-100 is the bound
		// used by the reference client, and the camera keeps what
		// qu'elle accepte.
		kind: EntityKind::Number { min: 0.0, max: 100.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Pir,
		family: "pir",
		leaf: "interval",
		label: "PIR trigger interval",
		icon: Some("mdi:timer-sand"),
		// `<interval>` of the same `<RfAlarmCfg>` (212/213): the rest time the
		// camera imposes between two PIR alarms, **in seconds**. The 5-120
		// range is the one the Reolink app exposes for this setting; the
		// firmware announces its own bounds in `intervalSecMin`/`intervalSecMax`,
		// visible in `status/pir/json`. A value outside the range does not leave.
		kind: EntityKind::Number { min: 5.0, max: 120.0, step: 1.0, unit: Some("s"), slider: true },
	},
	// ── NTP (38 / 39) ───────────────────────────────────────────────────
	EntitySpec {
		feature: Feature::Ntp,
		family: "ntp",
		leaf: "enable",
		label: "NTP sync",
		icon: Some("mdi:clock-check"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::Ntp,
		family: "ntp",
		leaf: "server",
		label: "NTP server",
		icon: Some("mdi:server-network"),
		kind: EntityKind::Text { max_len: 63 },
	},
	EntitySpec {
		feature: Feature::Ntp,
		family: "ntp",
		leaf: "interval",
		label: "Sync interval",
		icon: Some("mdi:restore"),
		// Minutes; 1440 = the reference client's default (24 h).
		kind: EntityKind::Number {
			min: 1.0,
			max: 1440.0,
			step: 1.0,
			unit: Some("min"),
			slider: false,
		},
	},
	// ── Daylight saving (106 / 107) ───────────────────────────────────────
	EntitySpec {
		feature: Feature::Dst,
		family: "dst",
		leaf: "enable",
		label: "Daylight saving",
		icon: Some("mdi:clock-alert-outline"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::Dst,
		family: "dst",
		leaf: "offset",
		label: "Daylight saving offset",
		icon: Some("mdi:clock-fast"),
		kind: EntityKind::Number {
			min: -14.0,
			max: 14.0,
			step: 1.0,
			unit: Some("h"),
			slider: false,
		},
	},
	// ── Automatic reboot (101 / 100) ────────────────────────────────────
	EntitySpec {
		feature: Feature::AutoReboot,
		family: "autoreboot",
		leaf: "enable",
		label: "Auto reboot",
		icon: Some("mdi:restart-alert"),
		kind: EntityKind::Switch,
	},
	EntitySpec {
		feature: Feature::AutoReboot,
		family: "autoreboot",
		leaf: "weekday",
		label: "Reboot day",
		icon: Some("mdi:calendar-week"),
		kind: EntityKind::Select(&WEEKDAYS),
	},
	EntitySpec {
		feature: Feature::AutoReboot,
		family: "autoreboot",
		leaf: "hour",
		label: "Reboot hour",
		icon: Some("mdi:clock-time-three-outline"),
		kind: EntityKind::Number { min: 0.0, max: 23.0, step: 1.0, unit: Some("h"), slider: false },
	},
	EntitySpec {
		feature: Feature::AutoReboot,
		family: "autoreboot",
		leaf: "minute",
		label: "Reboot minute",
		icon: Some("mdi:clock-time-three-outline"),
		kind: EntityKind::Number { min: 0.0, max: 59.0, step: 1.0, unit: Some("min"), slider: false },
	},
	// ── System (104 / 105) ──────────────────────────────────────────────
	EntitySpec {
		feature: Feature::General,
		family: "general",
		leaf: "osd_format",
		label: "Date format",
		icon: Some("mdi:calendar-format"),
		kind: EntityKind::Select(&OSD_FORMATS),
	},
	EntitySpec {
		feature: Feature::General,
		family: "general",
		leaf: "time_format",
		label: "Time format (0=24h, 1=12h)",
		icon: Some("mdi:clock-outline"),
		kind: EntityKind::Select(&["0", "1"]),
	},
	EntitySpec {
		feature: Feature::General,
		family: "general",
		leaf: "language",
		label: "Language",
		icon: Some("mdi:web"),
		kind: EntityKind::Text { max_len: 16 },
	},
	EntitySpec {
		feature: Feature::General,
		family: "general",
		leaf: "device_name",
		label: "Device system name",
		icon: Some("mdi:identifier"),
		kind: EntityKind::Text { max_len: 31 },
	},
	EntitySpec {
		feature: Feature::General,
		family: "general",
		leaf: "is_dst",
		label: "Currently in daylight saving",
		icon: Some("mdi:clock-in"),
		kind: EntityKind::FlagSensor { device_class: None },
	},
	// ── Privacy masks (52) — read-only ──────────────────────────────────
	EntitySpec {
		feature: Feature::PrivacyMask,
		family: "mask",
		leaf: "enable",
		label: "Masking active",
		icon: Some("mdi:eye-off"),
		kind: EntityKind::FlagSensor { device_class: None },
	},
	EntitySpec {
		feature: Feature::PrivacyMask,
		family: "mask",
		leaf: "max_num",
		label: "Mask slots",
		icon: Some("mdi:tag-multiple"),
		// The number of slots is a numeric quantity: it is entitled to
		// `state_class`, which the textual lines will no longer have.
		kind: EntityKind::StateSensor {
			unit: None,
			device_class: None,
			class: Some("measurement"),
		},
	},
	// ── Sleep (574) and Wi-Fi (115) ─────────────────────────────────────
	EntitySpec {
		feature: Feature::SleepState,
		family: "sleep",
		leaf: "state",
		label: "Sleeping",
		icon: Some("mdi:sleep"),
		kind: EntityKind::FlagSensor { device_class: None },
	},
	EntitySpec {
		feature: Feature::WifiSignal,
		family: "wifi",
		leaf: "signal",
		label: "Wi-Fi signal",
		icon: Some("mdi:wifi"),
		// No unit: the protocol does not say whether `<signal>` is in dBm or
		// en pourcents.
		kind: EntityKind::StateSensor { unit: None, device_class: None, class: Some("measurement") },
	},
	// ── Battery: the cheap states of the 253 ────────────────────────────
	EntitySpec {
		feature: Feature::BatteryExtra,
		family: "battery",
		leaf: "voltage",
		label: "Battery voltage",
		icon: Some("mdi:flash"),
		kind: EntityKind::StateSensor {
			unit: Some("mV"),
			device_class: Some("voltage"),
			class: Some("measurement"),
		},
	},
	EntitySpec {
		feature: Feature::BatteryExtra,
		family: "battery",
		leaf: "current",
		label: "Battery current",
		icon: Some("mdi:current-dc"),
		kind: EntityKind::StateSensor {
			unit: Some("mA"),
			device_class: Some("current"),
			class: Some("measurement"),
		},
	},
	EntitySpec {
		feature: Feature::BatteryExtra,
		family: "battery",
		leaf: "temperature",
		label: "Battery temperature",
		icon: Some("mdi:thermometer"),
		kind: EntityKind::StateSensor {
			unit: Some("°C"),
			device_class: Some("temperature"),
			class: Some("measurement"),
		},
	},
	EntitySpec {
		feature: Feature::BatteryExtra,
		family: "battery",
		leaf: "charge_status",
		label: "Charge status",
		icon: Some("mdi:battery-charging"),
		kind: EntityKind::StateSensor { unit: None, device_class: None, class: None },
	},
	EntitySpec {
		feature: Feature::BatteryExtra,
		family: "battery",
		leaf: "low_power",
		label: "Battery low",
		icon: Some("mdi:battery-alert"),
		kind: EntityKind::FlagSensor { device_class: Some("battery") },
	},
	// ── Carillon (483-487 / 609 / 610) ─────────────────────────────────
	EntitySpec {
		feature: Feature::Chime,
		family: "chime",
		leaf: "list",
		label: "Paired chimes",
		icon: Some("mdi:bell-outline"),
		// JSON list. **No state at all when the camera refused**: "did not
		// answer" must stay distinct from "empty list", which reads `[]`.
		kind: EntityKind::StateSensor { unit: None, device_class: None, class: None },
	},
	EntitySpec {
		feature: Feature::Chime,
		family: "chime",
		leaf: "volume",
		label: "Chime volume",
		icon: Some("mdi:bell-ring-outline"),
		// The chime's `<volLevel>`, not a percentage: the reference documentation
		// says "0-4 typical" and is careful not to assert a bound,
		// haute (`types.ts` `ChimeParams.volLevel`, `docs/lights.md`). On publie
		// so the **range seen**, without rounding to 0-100 as the
		// "volume" reflex would want: an invented bound is an invented promise.
		// The write goes out on 485 `setParam`, which only accepts `<volLevel>`
		// if the `getParam` printed it — the usual "never invent a tag"
		// rule applied on the write side, not here.
		kind: EntityKind::Number { min: 0.0, max: 4.0, step: 1.0, unit: None, slider: true },
	},
	EntitySpec {
		feature: Feature::Chime,
		family: "chime",
		leaf: "music",
		label: "Chime melody",
		icon: Some("mdi:music-circle-outline"),
		// The melody identifier **read** in the 485 of the polled chime.
		// Read-only verdict assumed: no protocol command enumerates the
		// melodies ("id not found" in the protocol notebook), and a
		// `select` entity whose options were invented is exactly what
		// this bridge refuses to publish. The identifiers the camera prints
		// itself are published — see `melodies` just below.
		kind: EntityKind::StateSensor { unit: None, device_class: None, class: None },
	},
	EntitySpec {
		feature: Feature::Chime,
		family: "chime",
		leaf: "melodies",
		label: "Camera-declared melodies",
		icon: Some("mdi:playlist-music"),
		// The `musicId`s **the camera** printed in its `<alarminCfg>`
		// (msg 486) and in its 485, copied as they came. It is the only melody
		// list this bridge allows itself to publish: it comes from an
		// answer, not from a table. Empty = cleared, never " 0 ".
		kind: EntityKind::StateSensor { unit: None, device_class: None, class: None },
	},
	EntitySpec {
		feature: Feature::Chime,
		family: "chime",
		leaf: "ring",
		label: "Ring the chime",
		icon: Some("mdi:bell-ring"),
		// The payload is a melody id; the chime targeted is the one paired
		// with the last read. Without a paired chime the command fails, and
		// FAIL says so.
		kind: EntityKind::Button { payload: "1" },
	},
];

/// Every specification of a discovery family, in order.
pub fn specs_for(feature: Feature) -> impl Iterator<Item = &'static EntitySpec> {
	SPECS.iter().filter(move |s| s.feature == feature)
}

/// Emits `(config_topic, payload)` for each entity of a feature, in the
/// shape its family deserves today.
///
/// `writable` = the families the camera **answered a read of** and
/// accepts having rewritten. A family absent from this set comes out read-only
/// (and a button does not come out at all): it is the rule
/// "nothing actionable before proof", applied where it is checkable —
/// the published platform follows what was read, never what is hoped.
pub fn build_feature_entities(
	ctx: &DiscoveryContext,
	feature: Feature,
	writable: &HashSet<Family>,
) -> Vec<(String, Vec<u8>)> {
	let friendly = super::title_case(ctx.camera_name);
	specs_for(feature)
		.filter_map(|spec| {
			// A family the parsing does not know cannot be
			// read or written: it stays readable, indefinitely. The test
			// `every_family_of_a_spec_is_known_of_parsing` checks that this
			// case does not occur.
			let writable =
				Family::from_slug(spec.family).is_some_and(|f| writable.contains(&f));
			spec.build_from(ctx, &friendly, writable)
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::discovery::{CameraCapabilitiesView, Presence, DiscoveryContext};

	static CAPS: CameraCapabilitiesView = CameraCapabilitiesView {
		has_ptz: false,
		status_led: Presence::Unknown,
		ir_leds: Presence::Unknown,
		ai: Presence::Unknown,
		doorbell: Presence::Unknown,
	};
	static PRESETS: [(u8, String); 0] = [];

	/// A minimal discovery context, the same values as the publisher's tests,
	/// so the expected topics can be read at a glance.
	fn ctx() -> DiscoveryContext<'static> {
		DiscoveryContext {
			topic_prefix: "bairelay",
			ha_topic: "homeassistant",
			camera_name: "cam1",
			camera_addr: Some("192.0.2.7:9000"),
			camera_uid: Some("UID"),
			sw_version: "1.1.2",
			capabilities: &CAPS,
			presets: &PRESETS,
		}
	}

	#[test]
	fn each_spec_has_a_unique_id() {
		let mut ids: Vec<String> = Vec::new();
		for spec in SPECS {
			let c = ctx();
			let id = spec.unique_id(&c);
			assert!(
				!ids.contains(&id),
				"two entities share the unique_id {id}: HA would merge the two"
			);
			ids.push(id);
		}
	}

	#[test]
	fn family_and_leaf_are_ascii_slugs() {
		for spec in SPECS {
			for slug in [spec.family, spec.leaf] {
				assert!(
					!slug.is_empty()
						&& slug
							.chars()
							.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
					"invalid slug in the discovery: {slug:?}"
				);
			}
		}
	}

	#[test]
	fn the_control_topics_match_the_parsing() {
		// The `control/<family>/<leaf>` bridge is only accepted for the
		// families/leaves `crate::settings` knows how to parse: a spec that
		// diverges would create an HA entity whose command is ignored.
		let c = ctx();
		for spec in SPECS {
			// We test the **modifiable** shape: it is the one that promises a
			// control. The readable copy structurally has no
			// `command_topic` (the `a_family_not_reread_publishes_without_a_command`
//		// test checks that).
			if spec.command_topic(&c, &spec.kind).is_none() {
				continue;
			}
			let (family, leaf) = (spec.family, spec.leaf);
			assert!(
				crate::settings::accepts(family, leaf),
				"discovery emits control/{family}/{leaf} that the parsing refuses"
			);
		}
	}

	#[test]
	fn every_family_of_a_spec_is_known_of_parsing() {
		// A discovery-table family that `Family` does not know could
		// never be marked "read and writable": its entities
		// would stay mute on writes forever, without anything
		// saying so. This test is the barrier that prevents that state.
		for spec in SPECS {
			assert!(
				crate::settings::Family::from_slug(spec.family).is_some(),
				"spec {}/{} carries a family the parsing does not know",
				spec.family,
				spec.leaf
			);
		}
	}

	#[test]
	fn the_numbers_are_bounded_and_coherent() {
		for spec in SPECS {
			if let EntityKind::Number { min, max, step, .. } = spec.kind {
				assert!(
					min < max && step > 0.0 && step <= (max - min),
					"incoherent range for {}/{}: {min}..{max} not {step}",
					spec.family,
					spec.leaf
				);
			}
		}
	}

	#[test]
	fn payload_number_serialises_the_ha_fields() {
		let c = ctx();
		let spec = SPECS
			.iter()
			.find(|s| s.family == "denoise" && s.leaf == "level")
			.expect("spec denoise/level");
		let (topic, payload) = spec.build(&c, "Porche");
		assert_eq!(
			topic,
			"homeassistant/number/bairelay_cam1_denoise_level/config"
		);
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		assert_eq!(v["min"], 1.0);
		assert_eq!(v["max"], 100.0);
		assert_eq!(v["mode"], "slider");
		assert_eq!(v["unit_of_measurement"], "%");
		assert_eq!(
			v["command_topic"],
			"bairelay/cam1/control/denoise/level",
			"the command goes back to the topic the dispatch listens on"
		);
		assert_eq!(
			v["state_topic"],
			"bairelay/cam1/status/denoise/level",
			"the state comes from the re-read published by the dispatch"
		);
		assert_eq!(
			v["availability"],
			serde_json::json!([
				{
					"topic": "bairelay/status",
					"payload_available": "online",
					"payload_not_available": "offline"
				},
				{
					"topic": "bairelay/cam1/presence",
					"payload_available": "reachable",
					"payload_not_available": "unreachable"
				}
			]),
			"two blocks: the bridge, then the camera's presence"
		);
		assert_eq!(v["device"]["identifiers"][0], "bairelay_cam1");
	}

	#[test]
	fn a_read_only_sensor_has_no_command_topic() {
		let c = ctx();
		let mut seen = 0;
		for spec in SPECS {
			// The same predicate as the constructor: if the rule changes here, this
			// test changes with it, instead of letting a new kind slip through.
			if !is_read_only(&spec.kind) {
				continue;
			}
			seen += 1;
			let (_, payload) = spec.build(&c, "Porche");
			let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
			assert!(
				v.get("command_topic").is_none(),
				"{}/{} is read-only but exposes a command_topic",
				spec.family,
				spec.leaf
			);
		}
		assert!(seen > 0, "no read-only spec to check");
	}

	#[test]
	fn the_wifi_sensor_shows_no_unit() {
		let c = ctx();
		let spec = SPECS
			.iter()
			.find(|s| s.family == "wifi")
			.expect("spec wifi");
		let (_, payload) = spec.build(&c, "Porche");
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		assert!(
			v.get("unit_of_measurement").is_none(),
			"the protocol does not define the unit of <signal>: {v}"
		);
	}

	#[test]
	fn the_chime_button_publishes_a_melody_id() {
		let c = ctx();
		let spec = SPECS
			.iter()
			.find(|s| s.leaf == "ring")
			.expect("spec chime/ring");
		let (topic, payload) = spec.build(&c, "Porche");
		assert_eq!(topic, "homeassistant/button/bairelay_cam1_chime_ring/config");
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		assert_eq!(v["payload_press"], "1");
		assert_eq!(v["command_topic"], "bairelay/cam1/control/chime/ring");
	}

	#[test]
	fn the_select_options_are_the_camera_texts() {
		let c = ctx();
		let spec = SPECS
			.iter()
			.find(|s| s.leaf == "weekday")
			.expect("spec autoreboot/weekday");
		let (_, payload) = spec.build(&c, "Porche");
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		let options: Vec<&str> = v["options"].as_array().expect("options").iter().map(|o| o.as_str().expect("str")).collect();
		assert_eq!(options, WEEKDAYS.to_vec(), "the options are the protocol's texts, not labels");
	}

	/// The three volumes of `audioCfg`: three entities, three identifiers, one
	/// XML tag each (documented on the table line). The original
	/// defect was not a clumsy label: `<volume>` had no
	/// entity at all, and "Intercom volume" wrote `<talkAndReplyVolume>` without
	/// telling anybody.
	#[test]
	fn the_three_volumes_are_three_separate_entities() {
		let c = ctx();
		let audio: Vec<&EntitySpec> = specs_for(Feature::Audio).collect();
		let attendus = [
			// leaf, label, unique_id — the general volume's identifier is
			// new, the other two are the ones already published at the operator's.
			("general_volume", "Speaker volume", "bairelay_cam1_audio_general_volume"),
			("volume", "Intercom volume (talk)", "bairelay_cam1_audio_volume"),
			("visitor_volume", "Visitor volume", "bairelay_cam1_audio_visitor_volume"),
		];
		let mut ids: Vec<&str> = Vec::new();
		for (leaf, label, identifiant) in attendus {
			let spec = audio
				.iter()
				.find(|s| s.leaf == leaf)
				.unwrap_or_else(|| panic!("no audio/{leaf} spec"));
			assert_eq!(spec.label, label, "audio/{leaf}");
			assert_eq!(spec.unique_id(&c), identifiant);
			assert!(
				crate::settings::accepts("audio", leaf),
				"discovery publishes audio/{leaf} that the parsing refuses"
			);
			// And three distinct commands, otherwise two entities would write the
			// same tag under two different names.
			let cmd = crate::settings::SettingCommand::parse(Family::Audio, leaf, "50")
				.unwrap_or_else(|| panic!("audio/{leaf} refused by the parsing"));
			assert_eq!(cmd.family(), Family::Audio);
			assert_eq!(cmd.leaf(), leaf);
			ids.push(spec.leaf);
		}
		let mut tri = ids.clone();
		tri.sort();
		tri.dedup();
		assert_eq!(tri.len(), 3, "{ids:?}");
	}

	/// Until the camera has answered a read of a family, its
	/// entities publish read-only: no command anywhere, and a
	/// button is not published at all. Re-read, the same family gets its
	/// commands back **without changing identifier**.
	#[test]
	fn a_family_not_reread_publishes_without_a_command() {
		let c = ctx();
		let non_read: HashSet<Family> = HashSet::new();
		let read: HashSet<Family> = [Family::Audio].into_iter().collect();

		let published_entities = build_feature_entities(&c, Feature::Audio, &non_read);
		assert!(!published_entities.is_empty(), "the audio family publishes nothing at all anymore");
		let mut ids_read: Vec<String> = Vec::new();
		for (topic, payload) in &published_entities {
			let v: serde_json::Value = serde_json::from_slice(payload).expect("json");
			assert!(
				v.get("command_topic").is_none(),
				"an entity published without a read carries a command: {topic}"
			);
			assert!(
				topic.starts_with("homeassistant/sensor/")
					|| topic.starts_with("homeassistant/binary_sensor/"),
				"modifiable platform published without a read: {topic}"
			);
			ids_read.push(v["unique_id"].as_str().expect("unique_id").to_string());
		}

		let writable = build_feature_entities(&c, Feature::Audio, &read);
		let mut ids_modifs: Vec<String> = Vec::new();
		for (topic, payload) in &writable {
			let v: serde_json::Value = serde_json::from_slice(payload).expect("json");
			assert!(v.get("command_topic").is_some(), "{topic} still offers no command");
			ids_modifs.push(v["unique_id"].as_str().expect("unique_id").to_string());
		}

		// The `chime/ring` button is the only one to disappear in read-only; the
		// audio family has none, so the two sets match each other.
		ids_read.sort();
		ids_modifs.sort();
		assert_eq!(
			ids_read, ids_modifs,
			"identifiers must not depend on what was read"
		);
	}

	/// The readable copy is not a degenerate sensor: it keeps what makes the
	/// state understandable (the unit) and loses what would be a lie (the write
	/// bounds, and a `state_class` on text).
	#[test]
	fn the_readable_copy_keeps_the_unit_and_drops_the_bounds() {
		let c = ctx();
		let spec = SPECS
			.iter()
			.find(|s| s.family == "audio" && s.leaf == "general_volume")
			.expect("spec audio/general_volume");
		let (topic, payload) = spec.build_from(&c, "Porche", false).expect("jumeau lisible");
		assert_eq!(
			topic,
			"homeassistant/sensor/bairelay_cam1_audio_general_volume/config",
			"a read number is a sensor: the configuration topic changes segment"
		);
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		assert_eq!(v["unit_of_measurement"], "%", "the number read must stay a percentage");
		assert_eq!(v["state_class"], "measurement");
		assert!(v.get("min").is_none() && v.get("max").is_none(), "bounds without writing");
		assert!(v.get("command_topic").is_none());

		// A switch becomes a binary_sensor that reads the texts the bridge
		// really publishes ("on"/"off"), not the protocol's raw "1"/"0" —
		// otherwise the entity would stay off whatever the value.
		let switch = SPECS
			.iter()
			.find(|s| s.family == "audio" && s.leaf == "visitor_loudspeaker")
			.expect("spec audio/visitor_loudspeaker");
		let (topic, payload) = switch.build_from(&c, "Porche", false).expect("jumeau lisible");
		assert_eq!(
			topic,
			"homeassistant/binary_sensor/bairelay_cam1_audio_visitor_loudspeaker/config"
		);
		let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
		assert_eq!(v["payload_on"], "on");
		assert_eq!(v["payload_off"], "off");

		// The button has no state to display: nothing to publish before a read.
		let bouton = SPECS
			.iter()
			.find(|s| matches!(s.kind, EntityKind::Button { .. }))
			.expect("a button spec");
		assert!(bouton.build_from(&c, "Porche", false).is_none());
		assert_eq!(bouton.read_only_platform(), None);
	}

	/// A `text` and a `select` that were read become **textual** sensors: a
	/// `state_class: measurement` on a language or a weekday would
	/// fill Home Assistant's log with incompatibility warnings.
	#[test]
	fn a_textual_state_is_never_marked_measured() {
		let c = ctx();
		let mut seen = 0;
		for spec in SPECS {
			if !matches!(spec.kind, EntityKind::Text { .. } | EntityKind::Select(_)) {
				continue;
			}
			seen += 1;
			let (_, payload) = spec.build_from(&c, "Porche", false).expect("jumeau lisible");
			let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
			let spec = format!("{}/{}", spec.family, spec.leaf);
			assert!(v.get("state_class").is_none(), "{spec} measured when it is text");
			assert!(v.get("unit_of_measurement").is_none(), "{spec} carries a unit");
			assert!(v.get("options").is_none(), "{spec} offers choices without a command");
			assert!(
				v.get("command_topic").is_none(),
				"{spec} promises a command it does not have at this stage"
			);
		}
		assert!(seen >= 2, "no text/select spec to test");
	}

	/// The availability factory is unique: every settings entity carries
	/// both blocks, the bridge then the presence, like the historical entities.
	/// A mistake here would repeat on every entity of the table.
	#[test]
	fn each_setting_entity_carries_both_availability_blocks() {
		let c = ctx();
		let mut seen = 0;
		for spec in SPECS {
			let (_, payload) = spec.build(&c, "Porche");
			let v: serde_json::Value = serde_json::from_slice(&payload).expect("json");
			let blocs = v["availability"].as_array().unwrap_or_else(|| {
				panic!(
					"{}.{}: availability should be an array",
					spec.family, spec.leaf
				)
			});
			assert_eq!(blocs.len(), 2, "{}.{} : {blocs:?}", spec.family, spec.leaf);
			assert_eq!(
				blocs[0]["topic"],
				"bairelay/status",
				"{}.{}: the bridge block",
				spec.family, spec.leaf
			);
			assert_eq!(
				blocs[1]["topic"],
				"bairelay/cam1/presence",
				"{}.{}: the presence block",
				spec.family, spec.leaf
			);
			assert_eq!(
				blocs[1]["payload_available"],
				"reachable",
				"{}.{}",
				spec.family, spec.leaf
			);
			seen += 1;
		}
		assert!(seen > 0, "no settings spec was verified");
	}

	/// The labels of the spec table are what Home Assistant shows the operator, and
	/// what its automations match on: `entity_id` is pinned at first discovery, so a
	/// french label that ships once survives in somebody's automation for years. This
	/// is the discovery half of the guard written after nine french reason sentences
	/// reached the broker behind a message saying published text was english.
	#[test]
	fn every_spec_label_and_topic_segment_is_english() {
		for feature in Feature::ALL {
			for spec in specs_for(feature) {
				for (what, value) in
					[("label", spec.label), ("family", spec.family), ("leaf", spec.leaf), ("unique id parts", spec.leaf)]
				{
					assert!(value.is_ascii(), "{what} is not ascii on the wire: {value:?}");
					let lowered = value.to_lowercase();
						let bad: Vec<&str> = lowered
						.split(|c: char| !c.is_alphabetic() && c != '-')
						.filter(|w| {
							[
								"la", "les", "des", "du", "une", "est", "dans", "pour", "sur", "par", "avec", "sans",
								// The French words below are the guard's own data: they are the strings this test has
								// to catch in published text. They stay in the language they detect.
								"que", "qui", "pas", "plus", "interphone", "caméra", "reglages", "réglages",
								"balise", "motif", "famille", "lecture", "écriture", "carillon", "sonnerie", "muet",
							]
							.contains(w)
						})
						.collect();
					assert!(bad.is_empty(), "spec of {feature:?} would publish french: {what} = {value:?} ({bad:?})");
				}
			}
		}
	}

}
