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
	/// Loudspeaker and intercom volumes (`<audioCfg>`, cmd 264 / 265). `tag`
	/// is the element spelling the camera used — the name a write goes back
	/// out under. `changed` is false for a plain read, and `block` is then
	/// simply what the camera holds. After a write, `block` is the **re-read**
	/// and each `changes` entry pairs the old text with what the camera kept,
	/// which is the only proof available that the block was stored.
	AudioSettings {
		tag: String,
		changed: bool,
		block: AudioCfgInfo,
		changes: Vec<AudioCfgChange>,
	},
	/// Paired-chime settings (cmds 483 / 484 / 485 / 486 / 609, and 485
	/// `ringWithMusic` for the `--sonner` test). The chime is spread over
	/// five questions the doorbell answers for it, and each part a camera
	/// doesn't have is **refused, not answered empty** — so a part is absent
	/// from `block` when the camera refused that question (no wireless
	/// chime, no wired input), which is a finding about the camera, not an
	/// empty block it wrote. `changed` is false for a plain read; after a
	/// write `block` is the **re-read** and each `changes` entry pairs the
	/// old text with what the camera kept — the only proof available that
	/// the block was stored. `ring` appears only when `--sonner` asked for
	/// the live test ring; the sound it should produce is the observation.
	ChimeSettings {
		changed: bool,
		block: DingDongCfgInfo,
		changes: Vec<DingDongChange>,
		#[serde(skip_serializing_if = "Option::is_none")]
		ring: Option<ChimeRingInfo>,
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
	/// Ungated `TalkAbility` probe (msg id 10, sent without consulting the
	/// `two_way_audio` ability table). `found` is false when the reply
	/// carried no `<TalkAbility>` element; `code` is the camera's raw
	/// header response code in every case — `200` OK, `405` unknown command
	/// on this family, `400` refused, `482` seen on other firmware — and is
	/// never mapped onto a coarser label. `payload` names what the reply
	/// actually carried (`"xml"`, `"none"`, `"binary(N bytes)"`,
	/// `"legacy"`), which separates "empty ack" from "answered with
	/// unrelated XML". A camera that never answered at all is an *error*,
	/// not this outcome, so the four cases stay distinguishable.
	TalkAbility {
		found: bool,
		code: u16,
		payload: String,
		/// Re-serialised `<TalkAbility>` element, when one arrived. Same
		/// caveat as [`Outcome::Abilities::xml`]: re-serialised from the
		/// parsed form, so field order is ours, not the camera's.
		#[serde(skip_serializing_if = "Option::is_none")]
		xml: Option<String>,
		/// Every `<audioConfig>` the camera listed (codec, rate, precision,
		/// block size, track count).
		#[serde(skip_serializing_if = "Vec::is_empty")]
		configs: Vec<TalkAudioConfig>,
		/// `<duplexList>` values (`"FDX"` / `"FBX"`).
		#[serde(skip_serializing_if = "Vec::is_empty")]
		duplex: Vec<String>,
		/// `<audioStreamModeList>` values (`"followVideoStream"`).
		#[serde(skip_serializing_if = "Vec::is_empty")]
		audio_stream_modes: Vec<String>,
	},
	/// One-shot talk transmission (`talk`): the camera accepted the
	/// `TalkConfig` and every 202 payload was written.
	///
	/// `audio_seconds` is the length of what we encoded, `wall_seconds` the
	/// time the send actually took — the two must agree within the pacing the
	/// sender imposes (one 64 ms block per block). Neither says anything about
	/// whether a human heard the loudspeaker: bairelay has no microphone on
	/// this side, so the human renderer says so out loud instead of claiming
	/// success.
	Talk {
		/// Path as given on the command line.
		file: String,
		/// `"wav"` or `"raw"`.
		origin: String,
		samples: usize,
		/// `samples / 16000`.
		audio_seconds: f64,
		/// Whole ADPCM blocks written on the wire.
		blocks: usize,
		/// Bytes per block: `length_per_encoder / 2 + 4`.
		block_bytes: usize,
		/// `length_per_encoder` as the camera reported it.
		length_per_encoder: u16,
		/// Codec echo of the `<audioConfig>` we drove the encoder from.
		audio_type: String,
		sample_rate: u16,
		sample_precision: u16,
		sound_track: String,
		duplex: String,
		/// Response code of the `<TalkAbility>` probe the config came from.
		ability_code: u16,
		/// Wall-clock seconds the whole send took, pacing included.
		wall_seconds: f64,
		/// Trailing bytes with no pair (an incomplete final sample).
		#[serde(skip_serializing_if = "is_zero")]
		dropped_bytes: usize,
	},
	/// One `audio-bridge` bridge run, reported when the bridge ends (stdin
	/// closed, idle gap exceeded, or Ctrl+C).
	AudioBridge {
		/// Talk sessions opened — one per speech burst, see the module docs.
		bursts: usize,
		/// Total blocks handed to a session across all bursts (what the
		/// protocol accepted, not what anyone heard).
		blocks: usize,
		/// Total PCM bytes consumed from stdin.
		bytes_in: usize,
		/// Seconds the process ran.
		wall_seconds: f64,
		/// Why the bridge stopped: `"eof"` (stdin closed — the normal end of a
		/// call), `"interrupted"` (Ctrl+C after closing the session) or
		/// `"repeated-failures"`.
		reason: String,
		/// Codec echo, so a log line explains itself without a second probe.
		audio_type: String,
		sample_rate: u16,
		block_bytes: usize,
		/// `--idle` in force, in seconds.
		idle_secs: f64,
	},
}

/// Serde helper: omit counters that happened to be zero.
fn is_zero(v: &usize) -> bool {
	*v == 0
}

/// One `<audioConfig>` entry from the camera's `<TalkAbility>` reply —
/// exactly the shape a `TalkConfig` must be built from before any audio can
/// be sent, which is the whole reason this probe exists.
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
pub struct TalkAudioConfig {
	/// `audioType` — `"adpcm"` is the only value the core will accept.
	pub audio_type: String,
	/// `sampleRate` in Hz (`16000` per the protocol notes).
	pub sample_rate: u16,
	/// `samplePrecision` in bits (`16`).
	pub sample_precision: u16,
	/// `lengthPerEncoder` — twice the ADPCM block size.
	pub length_per_encoder: u16,
	/// `soundTrack` — `"mono"`.
	pub sound_track: String,
	/// `priority`; the camera only sends it during a TalkAbility request.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub priority: Option<u32>,
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

/// The camera's `<audioCfg>` block, flattened for `--json`, with every value
/// kept as the **camera's own text** (the protocol layer never parses these
/// into numbers, precisely so an unparseable or undocumented value survives a
/// read/write round trip — see [`bairelay_neolink_core::bc::xml::AudioCfg`]).
///
/// The three doorbell tags are omitted from JSON when this firmware doesn't
/// print them: an absent tag is a finding about the camera, not an empty
/// string it wrote.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AudioCfgInfo {
	/// `@version` of the reply.
	pub version: String,
	/// `channelId`.
	pub channel_id: String,
	/// `volume`: the loudspeaker volume (0-100 per the protocol note).
	pub volume: String,
	/// `timeout` — meaning undocumented; shown for comparison and echoed
	/// back unchanged on a write.
	pub timeout: String,
	/// `audioSelect` — meaning undocumented, never written by the web UI.
	pub audio_select: String,
	/// `preAlarm` — meaning undocumented, never written by the web UI.
	pub pre_alarm: String,
	/// `audioListId` — meaning undocumented, never written by the web UI.
	pub audio_list_id: String,
	/// `talkAndReplyVolume`: how loud the answer you speak through the app
	/// comes out of the doorbell. The reason this command exists.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub talk_and_reply_volume: Option<String>,
	/// `visitorVolume`: how loud the pre-set greeting clips play.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub visitor_volume: Option<String>,
	/// `visitorLoudspeaker`: whether the loudspeaker is live for a visitor.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub visitor_loudspeaker: Option<String>,
	/// True when the camera spelled the element `<AudioCfg>` instead of
	/// `<audioCfg>` — recorded so a write can answer with the same name.
	pub pascal: bool,
}

/// One tag a `audio-settings` write touched, and what became of it: `before`
/// is the text the read held, `requested` the text asked for, `after` the text
/// the camera holds when the block is read back. `after != requested` means the
/// firmware took the block and kept something else — which is why the write is
/// always followed by a read.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AudioCfgChange {
	/// The XML tag name, as the camera names it.
	pub tag: String,
	pub before: String,
	pub requested: String,
	pub after: String,
}

/// The chime family aggregated for `--json`: what [`bairelay_neolink_core::bc::xml::DingDongCfg`]
/// holds after the read sweep (483 wired block, 484 paired list, 485 params,
/// 609 silent mode, 486 per-event melodies). Values are the **camera's own
/// text** for the same reason `AudioCfgInfo` keeps text: an undocumented value
/// must survive a read/write round trip untouched.
///
/// A part is *absent* (omitted from JSON) when the camera refused or never
/// answered that question — `None` and `Some(vec![])` in the core aggregate
/// mean "refused" and "answered with no entries", and this flattening keeps
/// that difference: refused parts vanish, answered-empty parts print as an
/// empty list.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct DingDongCfgInfo {
	/// The chime id the per-chime questions (485 / 609) were asked about —
	/// the first numeric id in the 484 list, online preferred.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub chime_id: Option<String>,
	/// The 484 paired-chime list, one entry per chime.
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub chimes: Vec<ChimeDeviceInfo>,
	/// The 483 hardwired-chime block, when this doorbell has a wired input.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ctrl: Option<ChimeCtrlInfo>,
	/// The 485 per-chime parameters (`name` / `volLevel` / `ledState`).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub params: Option<ChimeParamsInfo>,
	/// The 609 silent-mode block (`<time>`, unit undocumented).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub silent: Option<ChimeSilentInfo>,
	/// The 486 per-event melody blocks, one entry per device.
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub events: Vec<ChimeEventInfo>,
}

/// One `<dingdongDeviceInfo>` of the 484 list.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeDeviceInfo {
	/// The addressable id (`deviceId` or `id`, whichever the camera wrote).
	pub id: String,
	/// `deviceName` / `name`, when the camera gave the chime one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// `netState` / `netstate`: `1` online, `0` not answering keep-alives.
	/// Absent when the camera printed neither spelling.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub net_state: Option<String>,
}

/// The 483 hardwired-chime block. Both switch spellings appear exactly as the
/// camera wrote them — `bopen` or `enable`, never both — because a write
/// returns the spelling the camera used.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeCtrlInfo {
	pub version: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<String>,
	#[serde(rename = "type", skip_serializing_if = "Option::is_none")]
	pub cfg_type: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub bopen: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub bsave: Option<String>,
	/// Meaning (unit!) undocumented; echoed back unchanged on a write.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time: Option<String>,
	/// A refusal **reason**, not a setting: a battery doorbell with no wired
	/// chime answers `adptNotAC`. Printed so `bopen = 0` stops looking like a
	/// choice somebody made.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub result: Option<String>,
}

/// The 485 per-chime parameters.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeParamsInfo {
	pub version: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// 0-4 typical; the true ceiling is undocumented.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub vol_level: Option<String>,
	/// LED switch, 0 or 1.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub led_state: Option<String>,
	/// Only some firmwares echo a melody here; the writable melodies are the
	/// 486/487 events, not this tag.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub music_id: Option<String>,
}

/// The 609 silent-mode block.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeSilentInfo {
	pub version: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// Mute window; unit undocumented (seconds is the reference client's
	/// behaviour, not a documented fact).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub time: Option<String>,
	#[serde(rename = "type", skip_serializing_if = "Option::is_none")]
	pub cfg_type: Option<String>,
}

/// One 486 `<deviceCfg>` block: the chime (or doorbell) and its per-event
/// melodies.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeEventInfo {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// Read-only ring id some firmware prints; never written back.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ring_id: Option<String>,
	/// The chime's name as the 486 reply prints it beside the id (read-only
	/// label — the writable name is the 485 one).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// The chime's own firmware, the `<version>` CHILD of `<deviceCfg>`
	/// (measured `00A2`) — not the XML version attribute.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub firm_version: Option<String>,
	#[serde(skip_serializing_if = "Vec::is_empty")]
	pub entries: Vec<ChimeEventEntryInfo>,
}

/// One `<alarminCfg>`: what event (`type`), whether it rings (`valid` or
/// `switch`, whichever spelling arrived — the event vocabulary is whatever
/// the camera printed), and which melody.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeEventEntryInfo {
	#[serde(rename = "type", skip_serializing_if = "Option::is_none")]
	pub event_type: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub valid: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub switch: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub music_id: Option<String>,
}

/// One tag a `chime-settings` write touched, in the same
/// before/requested/after shape as [`AudioCfgChange`]. `tag` names the XML tag
/// (`volLevel`, `ledState`, `musicId(<event>)`) or `silentTime` for the
/// `<time>` inside `<dingdongSilentMode>`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DingDongChange {
	pub tag: String,
	pub before: String,
	pub requested: String,
	pub after: String,
}

/// The live test ring `--sonner` performed after the (re-)read: the chime id
/// ringed and the melody asked for. Success here means the doorbell answered
/// the chime family; **hearing** the ring is the operator's observation the
/// protocol can never report.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChimeRingInfo {
	pub chime_id: String,
	pub music_id: String,
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
		Outcome::AudioSettings {
			tag,
			changed,
			block,
			changes,
		} => {
			// Every value here arrived as camera text, so it goes through
			// `display_text`: a broken block must not put escape sequences on
			// the operator's terminal.
			let shown = |v: &str| {
				if v.is_empty() {
					"(empty)".to_string()
				} else {
					display_text(v, AUDIO_CFG_FIELD_DISPLAY_MAX)
				}
			};
			let mut out = format!(
				"audioCfg <{tag}>: {}\n",
				if *changed {
					"read, written, read back"
				} else {
					"read"
				}
			);
			if *changed {
				out.push_str("  modified:\n");
				for c in changes {
					// The firmware can answer 200 and keep the old value, so
					// the line reports what the camera holds, not what was
					// asked for.
					let note = if c.after == c.requested {
						String::new()
					} else {
						format!(" -> not stored, camera has {}", shown(&c.after))
					};
					out.push_str(&format!(
						"    {}: {} -> {}{}\n",
						c.tag,
						shown(&c.before),
						shown(&c.requested),
						note,
					));
				}
			}
			out.push_str(&format!("  volume: {}\n", shown(&block.volume)));
			for (name, value) in [
				(
					"talkAndReplyVolume (interphone)",
					block.talk_and_reply_volume.as_deref(),
				),
				("visitorVolume", block.visitor_volume.as_deref()),
				("visitorLoudspeaker", block.visitor_loudspeaker.as_deref()),
			] {
				match value {
					Some(v) => out.push_str(&format!("  {name}: {}\n", shown(v))),
					// Not printed as `(none)`: this firmware does not have the
					// tag, and saying so is the point.
					None => out.push_str(&format!("  {name}: (not in this camera's block)\n")),
				}
			}
			// Undocumented, but part of the block and sent back verbatim by a
			// write, so they belong in the report.
			out.push_str(&format!("  timeout: {}\n", shown(&block.timeout)));
			out.push_str(&format!("  audioSelect: {}\n", shown(&block.audio_select)));
			out.push_str(&format!("  preAlarm: {}\n", shown(&block.pre_alarm)));
			out.push_str(&format!("  audioListId: {}\n", shown(&block.audio_list_id)));
			out.push_str(&format!("  channelId: {}\n", shown(&block.channel_id)));
			out.push_str(&format!("  version: {}\n", shown(&block.version)));
			(out, String::new())
		}
		Outcome::ChimeSettings {
			changed,
			block,
			changes,
			ring,
		} => {
			// Every value arrived as camera text; `display_text` bounds what a
			// broken block can push into the terminal.
			let shown = |v: &str| {
				if v.is_empty() {
					"(empty)".to_string()
				} else {
					display_text(v, CHIME_FIELD_DISPLAY_MAX)
				}
			};
			let mut out = format!(
				"carillon: {}\n",
				if *changed {
					"read, written, read back"
				} else {
					"read"
				}
			);
			if *changed {
				out.push_str("  modified:\n");
				for c in changes {
					// A chime SET answering 200 while keeping the old text is
					// possible (this family shares the 265 silence quirk), so
					// report what the camera holds, not what was asked.
					let note = if c.after == c.requested {
						String::new()
					} else {
						format!(" -> not stored, camera has {}", shown(&c.after))
					};
					out.push_str(&format!(
						"    {}: {} -> {}{}\n",
						c.tag,
						shown(&c.before),
						shown(&c.requested),
						note,
					));
				}
			}
			match block.chime_id.as_deref() {
				Some(id) => out.push_str(&format!("  chime: {}\n", shown(id))),
				// Three situations used to collapse into this one line, and one
				// of them was a lie: the field camera DOES name its chime — this
				// build just could not read the container. An empty list and an
				// unreadable list are not the same finding.
				None if !block.chimes.is_empty() => out.push_str(&format!(
					"  chime: (484 named {} chime(s), none with an addressable id)\n",
					block.chimes.len()
				)),
				None => out.push_str(
					"  chime: (484 printed nothing readable — refused, or a shape this build cannot decode)\n",
				),
			}
			for d in &block.chimes {
				let state = match d.net_state.as_deref() {
					Some("1") => "online".to_string(),
					Some("0") => "offline".to_string(),
					Some(other) => format!("netState {}", shown(other)),
					None => "state unknown".to_string(),
				};
				let name = d.name.as_deref().map(shown).unwrap_or_else(|| "(unnamed)".into());
				out.push_str(&format!("  device {}: {name} ({state})\n", shown(&d.id)));
			}
			match &block.ctrl {
				Some(c) => {
					// Show the switch under the spelling the camera used.
					let (tag, state) = match (c.bopen.as_deref(), c.enable.as_deref()) {
						(Some(v), _) => ("bopen", Some(v)),
						(None, Some(v)) => ("enable", Some(v)),
						(None, None) => ("", None),
					};
					match state {
						Some(v) => out.push_str(&format!("  ctrl (hardwired): {tag} = {}\n", shown(v))),
						None => out.push_str("  ctrl (hardwired): (no switch tag)\n"),
					}
					if let Some(t) = c.time.as_deref() {
						out.push_str(&format!("  ctrl time: {} (unit undocumented)\n", shown(t)));
					}
					if let Some(t) = c.cfg_type.as_deref() {
						out.push_str(&format!("  ctrl type: {}\n", shown(t)));
					}
					out.push_str(&format!("  ctrl version: {}\n", shown(&c.version)));
					if let Some(r) = c.result.as_deref() {
						out.push_str(&format!(
							"  ctrl result: {} (the camera's own reason — not a setting)\n",
							shown(r)
						));
					}
				}
				// Refused and unanswered collapse to this one line; the raw
				// code lives in the command's stderr diagnostics, not here.
				None => out.push_str("  ctrl (hardwired): (483 not answered)\n"),
			}
			match &block.params {
				Some(p) => {
					for (name, value) in [
						("name", p.name.as_deref()),
						("volLevel", p.vol_level.as_deref()),
						("ledState", p.led_state.as_deref()),
					] {
						match value {
							Some(v) => out.push_str(&format!("  params {name}: {}\n", shown(v))),
							None => {
								out.push_str(&format!(
									"  params {name}: (not in this chime's block)\n"
								))
							}
						}
					}
				}
				None => out.push_str("  params: (485 not answered)\n"),
			}
			match &block.silent {
				Some(s) => match s.time.as_deref() {
					Some(t) => out.push_str(&format!(
						"  silent: time {} (0 = not muted; unit undocumented)\n",
						shown(t)
					)),
					None => out.push_str("  silent: (no <time> in this block)\n"),
				},
				None => out.push_str("  silent: (609 not answered)\n"),
			}
			if block.events.is_empty() {
				out.push_str("  events (486): (none printed)\n");
			}
			for dev in &block.events {
				let dev_id = dev.id.as_deref().map(shown).unwrap_or_else(|| "(no id)".into());
				let mut ligne = format!("  events: device {dev_id}");
				if let Some(n) = dev.name.as_deref() {
					ligne.push_str(&format!(" — {}", shown(n)));
				}
				if let Some(f) = dev.firm_version.as_deref() {
					ligne.push_str(&format!(" (chime firmware {})", shown(f)));
				}
				out.push_str(&ligne);
				out.push('\n');
				for e in &dev.entries {
					let event = e.event_type.as_deref().map(shown).unwrap_or_else(|| "(no type)".into());
					let (tag, state) = match (e.valid.as_deref(), e.switch.as_deref()) {
						(Some(v), _) => ("valid", Some(v)),
						(None, Some(v)) => ("switch", Some(v)),
						(None, None) => ("", None),
					};
					let state = match state {
						Some(v) => format!("{tag} {}", shown(v)),
						None => "(no switch tag)".into(),
					};
					let music = match e.music_id.as_deref() {
						Some(m) => shown(m),
						None => "(no musicId)".into(),
					};
					out.push_str(&format!("    {event}: {state}, musicId {music}\n"));
				}
			}
			if let Some(r) = ring {
				out.push_str(&format!(
					"  ring: one ring asked on chime {} musicId {} — hearing it is the proof\n",
					shown(&r.chime_id),
					shown(&r.music_id),
				));
			}
			(out, String::new())
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
		Outcome::TalkAbility {
			found: false,
			code,
			payload,
			..
		} => (
			format!(
				"TalkAbility: no answer with <TalkAbility> (code {code}, payload: {payload})\n"
			),
			String::new(),
		),
		Outcome::TalkAbility {
			code,
			payload,
			xml,
			configs,
			duplex,
			audio_stream_modes,
			..
		} => {
			let mut out = format!("TalkAbility: present (code {code}, payload: {payload})\n");
			out.push_str(&format!("  duplex: {}\n", join_or_none(duplex)));
			out.push_str(&format!(
				"  audioStreamMode: {}\n",
				join_or_none(audio_stream_modes)
			));
			out.push_str(&format!("  audioConfig ({}):\n", configs.len()));
			if configs.is_empty() {
				out.push_str("    (none)\n");
			} else {
				// Same widths as the rows below, or every column from
				// `rate` onward sits one space left of its values.
				out.push_str(&format!(
					"    {:<11} {:<5} {:<4} {:<11} {:<6} {}\n",
					"audioType", "rate", "bits", "perEncoder", "track", "priority",
				));
				for c in configs {
					let shown = |v: &str| {
						if v.is_empty() {
							"(empty)".to_string()
						} else {
							sanitize_display(v)
						}
					};
					let priority = match c.priority {
						Some(p) => p.to_string(),
						None => "-".to_string(),
					};
					out.push_str(&format!(
						"    {:<11} {:<5} {:<4} {:<11} {:<6} {}\n",
						shown(&c.audio_type),
						c.sample_rate,
						c.sample_precision,
						c.length_per_encoder,
						shown(&c.sound_track),
						priority,
					));
				}
			}
			out.push_str("\nxml:\n");
			out.push_str(xml.as_deref().unwrap_or("(unavailable)"));
			out.push('\n');
			(out, String::new())
		}
		Outcome::Talk {
			file,
			origin,
			samples,
			audio_seconds,
			blocks,
			block_bytes,
			length_per_encoder,
			audio_type,
			sample_rate,
			sample_precision,
			sound_track,
			duplex,
			ability_code,
			wall_seconds,
			dropped_bytes,
		} => {
			let mut out = format!(
				"talk: {} ({}) — {} samples, {:.3} s of audio\n",
				sanitize_display(file),
				origin,
				samples,
				audio_seconds,
			);
			out.push_str(&format!(
				"  codec: {}/{}Hz/{}b/{}/{} — the entry the camera asked for (TalkAbility code {ability_code})\n",
				sanitize_display(audio_type),
				sample_rate,
				sample_precision,
				length_per_encoder,
				sanitize_display(sound_track),
			));
			out.push_str(&format!(
				"  duplex: {} — sent {} block(s) of {} B in {:.2} s wall\n",
				sanitize_display(duplex),
				blocks,
				block_bytes,
				wall_seconds,
			));
			if *dropped_bytes > 0 {
				out.push_str(&format!("  ignored {} trailing byte(s)\n", dropped_bytes));
			}
			// Never claim the sound arrived: nothing on this side can hear the
			// loudspeaker. The operator's ears are the measurement.
			out.push_str(
				"  the session was accepted and the blocks were written; bairelay cannot hear \
				 the loudspeaker, so whether anything was audible is yours to measure\n",
			);
			(out, String::new())
		}
		Outcome::AudioBridge {
			bursts,
			blocks,
			bytes_in,
			wall_seconds,
			reason,
			audio_type,
			sample_rate,
			block_bytes,
			idle_secs,
		} => {
			let mut out = format!(
				"audio-bridge: {} talk session(s), {} block(s) of {} B, {} byte(s) in, {:.1} s wall\n",
				bursts, blocks, block_bytes, bytes_in, wall_seconds,
			);
			// Phrased as the policy in force, not as a past event: this line is
			// printed even when stdin closed before a single byte arrived and no
			// session was ever opened, and a summary must not claim a close that
			// never happened.
			out.push_str(&format!(
				"  codec: {}/{}Hz, idle limit {:.1} s (a session closes once stdin has been quiet that long)\n",
				sanitize_display(audio_type),
				sample_rate,
				idle_secs,
			));
			out.push_str(&format!("  stopped: {}\n", sanitize_display(reason)));
			out.push_str(
				"  the audio you did or did not hear is the measurement; this side cannot \
				 hear the loudspeaker\n",
			);
			(out, String::new())
		}
	}
}

/// `", "`-joined list, or `"(none)"` when empty. Camera-supplied values go
/// through [`sanitize_display`] so a broken reply can't inject escapes.
fn join_or_none(items: &[String]) -> String {
	if items.is_empty() {
		return "(none)".to_string();
	}
	items
		.iter()
		.map(|s| sanitize_display(s))
		.collect::<Vec<_>>()
		.join(", ")
}

/// Longest camera-supplied string, in characters, printed in human mode.
const URL_DISPLAY_MAX: usize = 512;

/// Longest `<audioCfg>` value printed in human mode. These are numbers by
/// protocol; the cap exists only to bound what a broken reply can print.
const AUDIO_CFG_FIELD_DISPLAY_MAX: usize = 64;

/// Longest chime-family value printed in human mode. Chime names are
/// user-chosen text (the operator can call a chime anything), so the cap
/// bounds length only — control characters are still escaped by
/// `sanitize_display`.
const CHIME_FIELD_DISPLAY_MAX: usize = 96;

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
		// This module keeps non-ASCII values on purpose: they stand in for what an operator
		// would type or a camera would answer, so they are data being tested, not prose to translate.
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

	fn audio_cfg_block() -> AudioCfgInfo {
		AudioCfgInfo {
			version: "1.1".into(),
			channel_id: "0".into(),
			volume: "50".into(),
			timeout: "10".into(),
			audio_select: "0".into(),
			pre_alarm: "0".into(),
			audio_list_id: "0".into(),
			talk_and_reply_volume: Some("40".into()),
			visitor_volume: Some("70".into()),
			visitor_loudspeaker: Some("1".into()),
			pascal: false,
		}
	}

	/// The whole point of the probe: a refusal must show up as a result with
	/// the camera's own code, not as a formatted error.
	#[test]
	fn human_talk_ability_absent_prints_raw_code() {
		let r = Outcome::TalkAbility {
			found: false,
			code: 405,
			payload: "none".into(),
			xml: None,
			configs: vec![],
			duplex: vec![],
			audio_stream_modes: vec![],
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(stdout.contains("no answer with <TalkAbility>"), "{stdout}");
		assert!(stdout.contains("code 405"), "{stdout}");
		assert!(stdout.contains("payload: none"), "{stdout}");
	}

	fn talk_ability_present() -> Outcome {
		Outcome::TalkAbility {
			found: true,
			code: 200,
			payload: "xml".into(),
			xml: Some("<TalkAbility/>".into()),
			configs: vec![TalkAudioConfig {
				audio_type: "adpcm".into(),
				sample_rate: 16000,
				sample_precision: 16,
				length_per_encoder: 64,
				sound_track: "mono".into(),
				priority: None,
			}],
			duplex: vec!["FDX".into()],
			audio_stream_modes: vec!["followVideoStream".into()],
		}
	}

	#[test]
	fn audio_settings_human_reads_print_the_block() {
		let r = Outcome::AudioSettings {
			tag: "audioCfg".into(),
			changed: false,
			block: audio_cfg_block(),
			changes: vec![],
		};
		let (stdout, stderr) = format_success(Mode::Human, &r);
		assert_eq!(stderr, "");
		assert!(
			stdout.starts_with("audioCfg <audioCfg>: read\n"),
			"{stdout}"
		);
		for line in [
			"  volume: 50\n",
			"  talkAndReplyVolume (interphone): 40\n",
			"  visitorVolume: 70\n",
			"  visitorLoudspeaker: 1\n",
			// The tags nobody documented are printed too: they are part of
			// what a write sends back, so the operator must be able to see them.
			"  timeout: 10\n",
			"  audioSelect: 0\n",
			"  preAlarm: 0\n",
			"  audioListId: 0\n",
			"  version: 1.1\n",
		] {
			assert!(stdout.contains(line), "missing {line:?} in:\n{stdout}");
		}
		assert!(
			!stdout.contains("modified:"),
			"a read writes nothing: {stdout}"
		);
	}

	#[test]
	fn audio_settings_human_write_shows_before_and_after() {
		let r = Outcome::AudioSettings {
			tag: "audioCfg".into(),
			changed: true,
			block: AudioCfgInfo {
				talk_and_reply_volume: Some("90".into()),
				..audio_cfg_block()
			},
			changes: vec![AudioCfgChange {
				tag: "talkAndReplyVolume".into(),
				before: "40".into(),
				requested: "90".into(),
				after: "90".into(),
			}],
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(
			stdout.contains("read, written, read back"),
			"the write must say it read back: {stdout}"
		);
		assert!(
			stdout.contains("talkAndReplyVolume: 40 -> 90\n"),
			"{stdout}"
		);
		// The block printed after the write is the re-read, not the request.
		assert!(
			stdout.contains("talkAndReplyVolume (interphone): 90\n"),
			"{stdout}"
		);
		assert!(!stdout.contains("not stored"), "{stdout}");
	}

	/// A firmware that answers 200 and keeps the old value is the failure this
	/// report exists to make visible.
	#[test]
	fn audio_settings_human_flags_a_write_the_camera_ignored() {
		let r = Outcome::AudioSettings {
			tag: "AudioCfg".into(),
			changed: true,
			block: audio_cfg_block(),
			changes: vec![AudioCfgChange {
				tag: "talkAndReplyVolume".into(),
				before: "40".into(),
				requested: "90".into(),
				after: "40".into(),
			}],
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(stdout.starts_with("audioCfg <AudioCfg>:"), "{stdout}");
		assert!(stdout.contains("not stored"), "{stdout}");
		assert!(stdout.contains("camera has 40"), "{stdout}");
	}

	#[test]
	fn audio_settings_json_keeps_camera_text_and_omits_absent_tags() {
		let block = AudioCfgInfo {
			visitor_volume: None,
			visitor_loudspeaker: None,
			pascal: true,
			..audio_cfg_block()
		};
		let r = Outcome::AudioSettings {
			tag: "AudioCfg".into(),
			changed: false,
			block,
			changes: vec![],
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert!(v.get("kind").is_none());
		assert_eq!(v["tag"], "AudioCfg");
		assert_eq!(v["changed"], false);
		// Text, not numbers: what the camera wrote is what a write sends back.
		assert_eq!(v["block"]["volume"], "50");
		assert_eq!(v["block"]["talk_and_reply_volume"], "40");
		assert_eq!(v["block"]["pascal"], true);
		// A tag this firmware doesn't print is absent, not empty.
		assert!(v["block"].get("visitor_volume").is_none(), "{stdout}");
		assert!(v["block"].get("visitor_loudspeaker").is_none(), "{stdout}");
		assert!(v["changes"].is_array());
	}

	fn chime_block() -> DingDongCfgInfo {
		DingDongCfgInfo {
			chime_id: Some("1".into()),
			chimes: vec![ChimeDeviceInfo {
				id: "1".into(),
				name: Some("Carillon".into()),
				net_state: Some("1".into()),
			}],
			ctrl: None,
			params: Some(ChimeParamsInfo {
				version: "1.1".into(),
				id: Some("1".into()),
				name: Some("Carillon".into()),
				vol_level: Some("2".into()),
				led_state: Some("1".into()),
				music_id: None,
			}),
			silent: Some(ChimeSilentInfo {
				version: "1.1".into(),
				id: Some("1".into()),
				time: Some("0".into()),
				cfg_type: None,
			}),
			events: vec![ChimeEventInfo {
				id: Some("1".into()),
				ring_id: Some("7".into()),
				name: Some("Reolink Chime".into()),
				firm_version: Some("00A2".into()),
				entries: vec![
					ChimeEventEntryInfo {
						event_type: Some("people".into()),
						valid: Some("1".into()),
						switch: None,
						music_id: Some("0".into()),
					},
					ChimeEventEntryInfo {
						event_type: Some("visitor".into()),
						valid: None,
						switch: Some("0".into()),
						music_id: Some("2".into()),
					},
				],
			}],
		}
	}

	#[test]
	fn chime_settings_human_read_prints_every_answered_part() {
		let r = Outcome::ChimeSettings {
			changed: false,
			block: chime_block(),
			changes: vec![],
			ring: None,
		};
		let (stdout, stderr) = format_success(Mode::Human, &r);
		assert_eq!(stderr, "");
		assert!(stdout.starts_with("carillon: read\n"), "{stdout}");
		for line in [
			"  chime: 1\n",
			"  device 1: Carillon (online)\n",
			"  params volLevel: 2\n",
			"  params ledState: 1\n",
			"  silent: time 0 ",
			"    people: valid 1, musicId 0\n",
			// The second spelling shows under its own name — the two mean the
			// same thing and the report says so by using the camera's word.
			"    visitor: switch 0, musicId 2\n",
		] {
			assert!(stdout.contains(line), "missing {line:?} in:\n{stdout}");
		}
		// A refused part says it was refused instead of printing nothing:
		// "no wired chime" is a finding, not a formatting gap.
		assert!(stdout.contains("ctrl (hardwired): (483 not answered)"), "{stdout}");
		assert!(!stdout.contains("modified:"), "a read writes nothing: {stdout}");
		assert!(!stdout.contains("ring:"), "{stdout}");
	}

	#[test]
	fn chime_settings_human_write_shows_before_after_and_ring() {
		let r = Outcome::ChimeSettings {
			changed: true,
			block: chime_block(),
			changes: vec![DingDongChange {
				tag: "silentTime".into(),
				before: "0".into(),
				requested: "600".into(),
				after: "0".into(),
			}],
			ring: Some(ChimeRingInfo {
				chime_id: "1".into(),
				music_id: "2".into(),
			}),
		};
		let (stdout, _) = format_success(Mode::Human, &r);
		assert!(stdout.contains("read, written, read back"), "{stdout}");
		// The firmware kept the old value: visible, not implied.
		assert!(
			stdout.contains("silentTime: 0 -> 600 -> not stored, camera has 0"),
			"{stdout}"
		);
		assert!(
			stdout.contains("ring: one ring asked on chime 1 musicId 2"),
			"{stdout}"
		);
	}

	#[test]
	fn chime_settings_json_omits_refused_parts_and_absent_ring() {
		let r = Outcome::ChimeSettings {
			changed: false,
			block: chime_block(),
			changes: vec![],
			ring: None,
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert_eq!(v["changed"], false);
		// Camera text, not numbers.
		assert_eq!(v["block"]["chime_id"], "1");
		assert_eq!(v["block"]["params"]["vol_level"], "2");
		assert_eq!(v["block"]["events"][0]["entries"][1]["switch"], "0");
		// Refused 483: absent, not empty; a ring that didn't happen: absent.
		assert!(v["block"].get("ctrl").is_none(), "{stdout}");
		assert!(v.get("ring").is_none(), "{stdout}");
		assert!(v["changes"].is_array());
	}
	fn human_talk_ability_present_prints_table_and_xml() {
		let (stdout, _) = format_success(Mode::Human, &talk_ability_present());
		assert!(stdout.contains("TalkAbility: present (code 200"), "{stdout}");
		assert!(stdout.contains("duplex: FDX"), "{stdout}");
		assert!(stdout.contains("adpcm"), "{stdout}");
		assert!(stdout.contains("16000"), "{stdout}");
		assert!(stdout.contains("priority"), "{stdout}");
		assert!(stdout.contains("-\n") || stdout.contains(" -"), "{stdout}");
		assert!(stdout.contains("<TalkAbility/>"), "{stdout}");
	}

	#[test]
	fn json_talk_ability_keeps_code_and_parsed_fields() {
		let (stdout, _) = format_success(Mode::Json, &talk_ability_present());
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["ok"], true);
		assert!(v.get("kind").is_none());
		assert_eq!(v["found"], true);
		assert_eq!(v["code"], 200);
		assert_eq!(v["payload"], "xml");
		assert_eq!(v["xml"], "<TalkAbility/>");
		let cfg = &v["configs"][0];
		assert_eq!(cfg["audio_type"], "adpcm");
		assert_eq!(cfg["sample_rate"], 16000);
		assert_eq!(cfg["sample_precision"], 16);
		assert_eq!(cfg["length_per_encoder"], 64);
		assert_eq!(cfg["sound_track"], "mono");
		// `priority: None` is skipped, not null.
		assert!(cfg.get("priority").is_none());
		assert_eq!(v["duplex"][0], "FDX");
		assert_eq!(v["audio_stream_modes"][0], "followVideoStream");
	}

	/// An empty `<audioConfigList>` must still render (and still serialize
	/// as an absent key rather than a null one).
	#[test]
	fn json_talk_ability_present_without_configs() {
		let r = Outcome::TalkAbility {
			found: true,
			code: 200,
			payload: "xml".into(),
			xml: Some("<TalkAbility/>".into()),
			configs: vec![],
			duplex: vec![],
			audio_stream_modes: vec![],
		};
		let (stdout, _) = format_success(Mode::Json, &r);
		let v: serde_json::Value = serde_json::from_str(&stdout).unwrap();
		assert_eq!(v["found"], true);
		assert!(v.get("configs").is_none());
		let (human, _) = format_success(Mode::Human, &r);
		assert!(human.contains("audioConfig (0)"), "{human}");
		assert!(human.contains("(none)"), "{human}");
	}

}
