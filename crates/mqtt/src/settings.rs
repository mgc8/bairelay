//! Parsing of the setting commands arriving over MQTT.
//!
//! The topic is `{prefix}/{cam}/control/{family}/{leaf}` and the payload is the
//! value. One thing is added beyond decoding: **validation against the protocol's
//! own bounds**, here and nowhere further upstream. A value outside the range
//! never leaves for the camera, for three reasons:
//!
//! * some firmwares accept the request (200) and then store anything — a lying
//!   `OK`;
//! * others silently switch the setting concerned off;
//! * and above all the operator's notebook of protocol captures: a block that is
//!   partly invalid can be cancelled **without any error reply**.
//!
//! The refusal comes back as a `FAIL` on the same control topic, which is what
//! Home Assistant shows; the setting itself has not moved.
//!
//! The ranges come from the protocol document, not from guesswork: 0-100 for the
//! `<audioCfg>` volumes, 1-100 for `<aiDenoise><level>` (a `level <= 0` switches
//! the denoiser off, which is not a value to send at random), 0-255 for the image
//! sliders, 0-50 for detection sensitivity, 1-1440 minutes for the NTP sync.

use std::str;

	/// The setting families, as they appear in the topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
	/// `<audioCfg>` (264/265): the intercom.
	Audio,
	/// `<aiDenoise>` (439/440): the microphone denoiser.
	Denoise,
	/// `<VideoInput>` + `<InputAdvanceCfg>` (26/25).
	Image,
	/// `<OsdDatetime>` + `<OsdChannelName>` (44/45).
	Osd,
	/// `<MD>` / `<MdAlarm>` (46/47).
	Motion,
	/// `<RfAlarmCfg>` (212/213).
	Pir,
	/// `<Ntp>` (38/39).
	Ntp,
	/// `<Dst>` (106/107).
	Dst,
	/// `<AutoReboot>` (101/100).
	AutoReboot,
	/// `<SystemGeneral>` (104/105).
	General,
	/// The chime (483-487, 609, 610).
	Chime,
	/// `<Shelter>` (52) — read-only.
	Mask,
	/// `<sleepState>` (574) — read-only.
	Sleep,
	/// `<WifiSignal>` (115) — read-only.
	Wifi,
	/// `<BatteryInfo>` (253) — read-only, and never worth a wake.
	Battery,
	/// The recognitions (`ai/*`): the family exists so its entities have a
	/// published verdict and reason, **not** so it can be re-read.
	///
	/// Nothing here corresponds to a single command: the state comes from push 33,
	/// and the per-type settings (`<AiDetectCfg>`, 342/343) are queried **one type
	/// at a time**, with an enable tag nobody has ever read back — neither in the
	/// application nor in the operator's captures. The family is therefore pinned
	/// read-only until a 342 dump exists, and it is `status/ai/reason` that says
	/// so, instead of the current silence.
	Ai,
}

impl Family {
	/// The topic segment.
	pub fn slug(self) -> &'static str {
		match self {
			Family::Audio => "audio",
			Family::Denoise => "denoise",
			Family::Image => "image",
			Family::Osd => "osd",
			Family::Motion => "motion",
			Family::Pir => "pir",
			Family::Ntp => "ntp",
			Family::Dst => "dst",
			Family::AutoReboot => "autoreboot",
			Family::General => "general",
			Family::Chime => "chime",
			Family::Mask => "mask",
			Family::Sleep => "sleep",
			Family::Wifi => "wifi",
			Family::Battery => "battery",
			Family::Ai => "ai",
		}
	}

	/// The query topic that triggers the (re)read of the family.
	pub fn from_slug(slug: &str) -> Option<Self> {
		Some(match slug {
			"audio" => Family::Audio,
			"denoise" => Family::Denoise,
			"image" => Family::Image,
			"osd" => Family::Osd,
			"motion" => Family::Motion,
			"pir" => Family::Pir,
			"ntp" => Family::Ntp,
			"dst" => Family::Dst,
			"autoreboot" => Family::AutoReboot,
			"general" => Family::General,
			"chime" => Family::Chime,
			"mask" => Family::Mask,
			"sleep" => Family::Sleep,
			"wifi" => Family::Wifi,
			"battery" => Family::Battery,
			"ai" => Family::Ai,
			_ => return None,
		})
	}

	/// Every family, in the order a complete `query/settings` re-reads them in.
	///
	/// `Ai` comes **last** and costs no radio command at all: it is the verdict
	/// "nothing here is adjustable until 342 has been dumped", placed at the end of
	/// the burst — not a re-read.
	pub const ALL: [Family; 16] = [
		Family::Battery,
		Family::Audio,
		Family::Denoise,
		Family::Image,
		Family::Osd,
		Family::Motion,
		Family::Pir,
		Family::Ntp,
		Family::Dst,
		Family::AutoReboot,
		Family::General,
		Family::Mask,
		Family::Sleep,
		Family::Wifi,
		Family::Chime,
		Family::Ai,
	];
}

/// A validated setting command. Each variant carries its value **exactly as it
/// has to be written into the block**, that is in camera text: the dispatch does
/// the read, patches this leaf, and hands back the re-read.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingCommand {
	// ── audioCfg ────────────────────────────────────────────────────────
	/// `<volume>`, 0-100: the **general volume** of the camera's loudspeaker.
	///
	/// `audioCfg` prints three volumes and the bridge discovered two. This leaf is
	/// the one that was missing; it carries the `<volume>` tag, which the 264 model
	/// prints first.
	AudioGeneralVolume(String),
	/// `<talkAndReplyVolume>`, 0-100: the **talk** volume (intercom: what the
	/// visitor hears during the exchange), not the general volume.
	AudioVolume(String),
	/// `<visitorVolume>`, 0-100: the **visitor** volume (what the camera sends
	/// into the talk path).
	AudioVisitorVolume(String),
	/// `<visitorLoudspeaker>`, `"1"`/`"0"`.
	AudioVisitorLoudspeaker(bool),
	// ── aiDenoise ───────────────────────────────────────────────────────
	/// `<enable>`, `"1"`/`"0"`.
	DenoiseEnable(bool),
	/// `<level>`, 1-100.
	DenoiseLevel(String),
	// ── image ───────────────────────────────────────────────────────────
	ImageBright(String),
	ImageContrast(String),
	ImageSaturation(String),
	ImageHue(String),
	ImageSharpen(String),
	/// `<dayNight>`, one of the texts the camera normalises to.
	ImageDayNight(DayNight),
	// ── osd ─────────────────────────────────────────────────────────────
	OsdDatetimeEnable(bool),
	OsdNameEnable(bool),
	/// `<OsdChannelName><name>`: what is printed on the picture.
	OsdName(String),
	/// `<enWatermark>`, `"1"`/`"0"`.
	OsdWatermark(bool),
	// ── motion ──────────────────────────────────────────────────────────
	MotionEnable(bool),
	/// `<sensitivityDefault>`, 0-50, higher = more sensitive.
	MotionSensitivity(String),
	// ── pir ─────────────────────────────────────────────────────────────
	/// `<sensiValue>`. The switch itself stays on `control/pir`.
	PirSensitivity(String),
	/// The `<interval>` of that same `<RfAlarmCfg>`: resting seconds between two
	/// PIR alarms. Carried by the 212/213 pair already understood; the 5-120 range
	/// is the one the application exposes, and the bounds the firmware announces
	/// (`intervalSecMin`/`intervalSecMax`) can be read in `status/pir/json`.
	PirInterval(String),
	// ── ntp ─────────────────────────────────────────────────────────────
	NtpEnable(bool),
	NtpServer(String),
	/// `<synchronizeInterval>` in **minutes**, 1-1440.
	NtpInterval(String),
	// ── dst ─────────────────────────────────────────────────────────────
	DstEnable(bool),
	/// `<offset>` in hours, -14 to 14.
	DstOffset(String),
	// ── autoreboot ──────────────────────────────────────────────────────
	AutoRebootEnable(bool),
	AutoRebootWeekday(Weekday),
	AutoRebootHour(String),
	AutoRebootMinute(String),
	// ── general ─────────────────────────────────────────────────────────
	GeneralOsdFormat(OsdFormat),
	/// `<timeFormat>`: `"0"` = 24 h, `"1"` = 12 h.
	GeneralTimeFormat(String),
	GeneralLanguage(String),
	/// `<deviceName>`: the system name, neither the on-screen name nor the MQTT name.
	GeneralDeviceName(String),
	// ── chime ───────────────────────────────────────────────────────────
	/// The wireless chime's `<volLevel>`, **0-4**: the bound seen in the reference
	/// document ("0-4 typical"), not a range rounded to a percentage. The
	/// firmware's real upper bound is UNKNOWN; if the field shows a higher one, it
	/// is changed here, together with the document line that proves it.
	ChimeVolume(String),
	/// Make it ring: melody `music_id` on chime `chime_id` (or on the paired one
	/// when no id is given).
	ChimeRing { chime_id: Option<String>, music_id: String },
}

/// The normalised `<dayNight>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayNight {
	Auto,
	Color,
	BlackAndWhite,
}

impl DayNight {
	/// The text the camera expects.
	pub fn as_text(self) -> &'static str {
		match self {
			DayNight::Auto => "auto",
			DayNight::Color => "color",
			DayNight::BlackAndWhite => "blackAndWhite",
		}
	}
}

/// `<AutoReboot><weekDay>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Weekday {
	Everyday,
	Sunday,
	Monday,
	Tuesday,
	Wednesday,
	Thursday,
	Friday,
	Saturday,
}

impl Weekday {
	/// The text the camera expects.
	pub fn as_text(self) -> &'static str {
		match self {
			Weekday::Everyday => "everyday",
			Weekday::Sunday => "Sunday",
			Weekday::Monday => "Monday",
			Weekday::Tuesday => "Tuesday",
			Weekday::Wednesday => "Wednesday",
			Weekday::Thursday => "Thursday",
			Weekday::Friday => "Friday",
			Weekday::Saturday => "Saturday",
		}
	}
}

/// `<SystemGeneral><osdFormat>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsdFormat {
	Dmy,
	Mdy,
	Ymd,
}

impl OsdFormat {
	/// The text the camera expects.
	pub fn as_text(self) -> &'static str {
		match self {
			OsdFormat::Dmy => "DMY",
			OsdFormat::Mdy => "MDY",
			OsdFormat::Ymd => "YMD",
		}
	}
}

impl SettingCommand {
	/// The leaf of the topic the command came from. Used to rebuild the
	/// `OK`/`FAIL` reply topic.
	pub fn leaf(&self) -> &'static str {
		use SettingCommand::*;
		match self {
			AudioGeneralVolume(_) => "general_volume",
			AudioVolume(_) => "volume",
			AudioVisitorVolume(_) => "visitor_volume",
			AudioVisitorLoudspeaker(_) => "visitor_loudspeaker",
			DenoiseEnable(_) => "enable",
			DenoiseLevel(_) => "level",
			ImageBright(_) => "bright",
			ImageContrast(_) => "contrast",
			ImageSaturation(_) => "saturation",
			ImageHue(_) => "hue",
			ImageSharpen(_) => "sharpen",
			ImageDayNight(_) => "day_night",
			OsdDatetimeEnable(_) => "datetime_enable",
			OsdNameEnable(_) => "name_enable",
			OsdName(_) => "name",
			OsdWatermark(_) => "watermark",
			MotionEnable(_) => "enable",
			MotionSensitivity(_) => "sensitivity",
			PirSensitivity(_) => "sensitivity",
			PirInterval(_) => "interval",
			NtpEnable(_) => "enable",
			NtpServer(_) => "server",
			NtpInterval(_) => "interval",
			DstEnable(_) => "enable",
			DstOffset(_) => "offset",
			AutoRebootEnable(_) => "enable",
			AutoRebootWeekday(_) => "weekday",
			AutoRebootHour(_) => "hour",
			AutoRebootMinute(_) => "minute",
			GeneralOsdFormat(_) => "osd_format",
			GeneralTimeFormat(_) => "time_format",
			GeneralLanguage(_) => "language",
			GeneralDeviceName(_) => "device_name",
			ChimeVolume(_) => "volume",
			ChimeRing { .. } => "ring",
		}
	}

	/// The family of the topic the command came from.
	pub fn family(&self) -> Family {
		use SettingCommand::*;
		match self {
			AudioGeneralVolume(_) | AudioVolume(_) | AudioVisitorVolume(_)
			| AudioVisitorLoudspeaker(_) => Family::Audio,
			DenoiseEnable(_) | DenoiseLevel(_) => Family::Denoise,
			ImageBright(_)
			| ImageContrast(_)
			| ImageSaturation(_)
			| ImageHue(_)
			| ImageSharpen(_)
			| ImageDayNight(_) => Family::Image,
			OsdDatetimeEnable(_) | OsdNameEnable(_) | OsdName(_) | OsdWatermark(_) => Family::Osd,
			MotionEnable(_) | MotionSensitivity(_) => Family::Motion,
			PirSensitivity(_) | PirInterval(_) => Family::Pir,
			NtpEnable(_) | NtpServer(_) | NtpInterval(_) => Family::Ntp,
			DstEnable(_) | DstOffset(_) => Family::Dst,
			AutoRebootEnable(_) | AutoRebootWeekday(_) | AutoRebootHour(_)
			| AutoRebootMinute(_) => Family::AutoReboot,
			GeneralOsdFormat(_) | GeneralTimeFormat(_) | GeneralLanguage(_)
			| GeneralDeviceName(_) => Family::General,
			ChimeRing { .. } | ChimeVolume(_) => Family::Chime,
		}
	}

	/// Decodes and validates `(family, leaf, payload)`. `None` = unknown command or
	/// value outside the range: the dispatch will answer `FAIL` having never
	/// touched the camera.
	pub fn parse(family: Family, leaf: &str, payload: &str) -> Option<Self> {
		use SettingCommand::*;
		let p = payload.trim();
		Some(match (family, leaf) {
			// The three volumes of `audioCfg`, all 0-100 but three distinct tags:
			// `<volume>` (general), `<talkAndReplyVolume>` (talk) and `<visitorVolume>`
			// (visitor). Confusing them writes the right value to the wrong loudspeaker.
			(Family::Audio, "general_volume") => AudioGeneralVolume(int_in(p, 0, 100)?),
			(Family::Audio, "volume") => AudioVolume(int_in(p, 0, 100)?),
			(Family::Audio, "visitor_volume") => AudioVisitorVolume(int_in(p, 0, 100)?),
			(Family::Audio, "visitor_loudspeaker") => AudioVisitorLoudspeaker(on_off(p)?),
			(Family::Denoise, "enable") => DenoiseEnable(on_off(p)?),
			// 1-100: a level <= 0 switches the denoiser off inside the firmware, which
			// is not what "turning the strength down" means.
			(Family::Denoise, "level") => DenoiseLevel(int_in(p, 1, 100)?),
			(Family::Image, "bright") => ImageBright(int_in(p, 0, 255)?),
			(Family::Image, "contrast") => ImageContrast(int_in(p, 0, 255)?),
			(Family::Image, "saturation") => ImageSaturation(int_in(p, 0, 255)?),
			(Family::Image, "hue") => ImageHue(int_in(p, 0, 255)?),
			(Family::Image, "sharpen") => ImageSharpen(int_in(p, 0, 255)?),
			(Family::Image, "day_night") => ImageDayNight(match p {
				"auto" => DayNight::Auto,
				"color" => DayNight::Color,
				"blackAndWhite" => DayNight::BlackAndWhite,
				_ => return None,
			}),
			(Family::Osd, "datetime_enable") => OsdDatetimeEnable(on_off(p)?),
			(Family::Osd, "name_enable") => OsdNameEnable(on_off(p)?),
			(Family::Osd, "name") => OsdName(text(p, 1, 31)?),
			(Family::Osd, "watermark") => OsdWatermark(on_off(p)?),
			(Family::Motion, "enable") => MotionEnable(on_off(p)?),
			(Family::Motion, "sensitivity") => MotionSensitivity(int_in(p, 0, 50)?),
			(Family::Pir, "sensitivity") => PirSensitivity(int_in(p, 0, 100)?),
			// 5-120 s: the range the Reolink application exposes for this setting
			// (observed in the application). The firmware may announce narrower ones in
			// `intervalSecMin`/`intervalSecMax`: in that case it keeps what it accepts,
			// and it is the re-read that gets published — not the requested value.
			(Family::Pir, "interval") => PirInterval(int_in(p, 5, 120)?),
			(Family::Ntp, "enable") => NtpEnable(on_off(p)?),
			(Family::Ntp, "server") => NtpServer(host(p, 63)?),
			(Family::Ntp, "interval") => NtpInterval(int_in(p, 1, 1440)?),
			(Family::Dst, "enable") => DstEnable(on_off(p)?),
			(Family::Dst, "offset") => DstOffset(int_in(p, -14, 14)?),
			(Family::AutoReboot, "enable") => AutoRebootEnable(on_off(p)?),
			(Family::AutoReboot, "weekday") => AutoRebootWeekday(match p {
				"everyday" => Weekday::Everyday,
				"Sunday" => Weekday::Sunday,
				"Monday" => Weekday::Monday,
				"Tuesday" => Weekday::Tuesday,
				"Wednesday" => Weekday::Wednesday,
				"Thursday" => Weekday::Thursday,
				"Friday" => Weekday::Friday,
				"Saturday" => Weekday::Saturday,
				_ => return None,
			}),
			(Family::AutoReboot, "hour") => AutoRebootHour(int_in(p, 0, 23)?),
			(Family::AutoReboot, "minute") => AutoRebootMinute(int_in(p, 0, 59)?),
			(Family::General, "osd_format") => GeneralOsdFormat(match p {
				"DMY" => OsdFormat::Dmy,
				"MDY" => OsdFormat::Mdy,
				"YMD" => OsdFormat::Ymd,
				_ => return None,
			}),
			(Family::General, "time_format") => GeneralTimeFormat(match p {
				"0" | "1" => p.to_string(),
				_ => return None,
			}),
			(Family::General, "language") => GeneralLanguage(text(p, 1, 16)?),
			(Family::General, "device_name") => GeneralDeviceName(text(p, 1, 31)?),
			// The chime volume: the range actually **seen** (0-4), not a 0-100 that is
			// only the "volume" reflex. A value outside this range does not leave: the
			// 485 `setParam` stores whatever it is given and answers 200, which is
			// exactly the lying "OK" this file has refused since its first line.
			(Family::Chime, "volume") => ChimeVolume(int_in(p, 0, 4)?),
			(Family::Chime, "ring") => {
				let (chime_id, music_id) = match p.split_once(' ') {
					Some((chime, music)) => (Some(numeric(chime)?), numeric(music)?),
					None => (None, numeric(p)?),
				};
				ChimeRing { chime_id, music_id }
			}
			_ => return None,
		})
	}
}

/// Is `(family, leaf)` a command this bridge can write? Used by the discovery
/// coherence test: every HA entity with a `command_topic` has to land here.
pub fn accepts(family_slug: &str, leaf: &str) -> bool {
	Family::from_slug(family_slug)
		.map(|f| SettingCommand::parse(f, leaf, sample_value(f, leaf)).is_some())
		.unwrap_or(false)
}

/// A value that necessarily passes this leaf's validation, for [`accepts`].
/// Families without commands answer `""` (and `accepts` then returns `false`).
fn sample_value(family: Family, leaf: &str) -> &'static str {
	match (family, leaf) {
		// The special cases come before `(Family::Image, _)`, otherwise a text leaf
		// like `day_night` would inherit a numeric sample.
		(Family::Image, "day_night") => "auto",
		(Family::Audio, "general_volume")
		| (Family::Audio, "volume")
		| (Family::Audio, "visitor_volume")
		| (Family::Denoise, "level")
		| (Family::Image, _)
		| (Family::Pir, "sensitivity")
		| (Family::Pir, "interval")
		| (Family::Motion, "sensitivity") => "10",
		(Family::Ntp, "interval") => "1440",
		(Family::AutoReboot, "hour") => "3",
		(Family::AutoReboot, "minute") => "30",
		(Family::Dst, "offset") => "1",
		(Family::Ntp, "server") => "pool.ntp.org",
		(Family::Osd, "name") | (Family::General, "device_name") => "Porch",
		(Family::General, "language") => "French",
		(Family::General, "time_format") => "0",
		(Family::General, "osd_format") => "DMY",
		(Family::AutoReboot, "weekday") => "everyday",
		(Family::Chime, "ring") => "1",
		(Family::Chime, "volume") => "2",
		(_, leaf)
			if matches!(
				leaf,
				"enable" | "datetime_enable" | "name_enable" | "visitor_loudspeaker" | "watermark"
			) =>
		{
			"on"
		}
		_ => "",
	}
}

/// `"on"`/`"off"` (and `"1"`/`"0"`, and `"true"`/`"false"` — HA sends all three
/// depending on the card) → boolean.
fn on_off(payload: &str) -> Option<bool> {
	match payload {
		"on" | "1" | "true" => Some(true),
		"off" | "0" | "false" => Some(false),
		_ => None,
	}
}

/// An integer in `[lo, hi]`, rendered in its **camera text form**: no redundant
/// sign, no padding zeros. A `"007"` accepted as it stands would go out as it
/// stands, and the camera might read it back differently.
fn int_in(payload: &str, lo: i32, hi: i32) -> Option<String> {
	let v: i32 = payload.parse().ok()?;
	if v < lo || v > hi {
		return None;
	}
	Some(v.to_string())
}

/// A bounded printable string. Newlines are excluded: they break the XML frame
/// and cannot come from a legitimate HA field.
fn text(payload: &str, min: usize, max: usize) -> Option<String> {
	let len = payload.chars().count();
	if len < min || len > max {
		return None;
	}
	if payload.chars().any(|c| c.is_control()) {
		return None;
	}
	Some(payload.to_string())
}

/// A hostname or IP address of an NTP server: no spaces, no URL pointer,
/// bounded.
fn host(payload: &str, max: usize) -> Option<String> {
	if !payload.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':'))
	{
		return None;
	}
	text(payload, 1, max)
}

/// A purely numeric identifier (a chime id or a melody id).
fn numeric(payload: &str) -> Option<String> {
	if payload.is_empty() || !payload.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	Some(payload.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_families_round_trip_as_slugs() {
		for f in Family::ALL {
			assert_eq!(Family::from_slug(f.slug()), Some(f), "slug {}", f.slug());
		}
		assert_eq!(Family::from_slug("nosuchfamily"), None);
	}

	#[test]
	fn an_out_of_range_volume_is_not_sent() {
		assert_eq!(
			SettingCommand::parse(Family::Audio, "volume", "100"),
			Some(SettingCommand::AudioVolume("100".into()))
		);
		assert_eq!(SettingCommand::parse(Family::Audio, "volume", "101"), None);
		assert_eq!(SettingCommand::parse(Family::Audio, "volume", "-1"), None);
		assert_eq!(SettingCommand::parse(Family::Audio, "volume", "12.5"), None);
		assert_eq!(SettingCommand::parse(Family::Audio, "volume", ""), None);
	}

	#[test]
	fn the_denoiser_refuses_zero_and_accepts_hundred() {
		// `level <= 0` switches the denoiser off inside the firmware: that is not a
		// setting value, it is a switch in disguise.
		assert_eq!(SettingCommand::parse(Family::Denoise, "level", "0"), None);
		assert_eq!(
			SettingCommand::parse(Family::Denoise, "level", "1"),
			Some(SettingCommand::DenoiseLevel("1".into()))
		);
		assert_eq!(
			SettingCommand::parse(Family::Denoise, "level", "100"),
			Some(SettingCommand::DenoiseLevel("100".into()))
		);
		assert_eq!(SettingCommand::parse(Family::Denoise, "level", "101"), None);
	}

	#[test]
	fn the_motion_sensitivity_is_bounded_at_fifty() {
		assert!(SettingCommand::parse(Family::Motion, "sensitivity", "50").is_some());
		assert_eq!(SettingCommand::parse(Family::Motion, "sensitivity", "51"), None);
	}

	#[test]
	fn the_switches_accept_the_three_ha_spellings() {
		for yes in ["on", "1", "true"] {
			assert_eq!(
				SettingCommand::parse(Family::Ntp, "enable", yes),
				Some(SettingCommand::NtpEnable(true)),
				"{yes}"
			);
		}
		for no in ["off", "0", "false"] {
			assert_eq!(
				SettingCommand::parse(Family::Ntp, "enable", no),
				Some(SettingCommand::NtpEnable(false)),
				"{no}"
			);
		}
		assert_eq!(SettingCommand::parse(Family::Ntp, "enable", "not-a-switch"), None);
	}

	#[test]
	fn the_padding_zeros_are_normalised() {
		assert_eq!(
			SettingCommand::parse(Family::Image, "bright", "007"),
			Some(SettingCommand::ImageBright("7".into())),
			"the camera reads back what it stores: only the canonical form is sent"
		);
	}

	#[test]
	fn a_dubious_ntp_server_is_refused() {
		assert!(SettingCommand::parse(Family::Ntp, "server", "pool.ntp.org").is_some());
		assert!(SettingCommand::parse(Family::Ntp, "server", "192.168.1.10").is_some());
		for suspect in ["", " ", "ntp; rm -rf /", "http://x", "a b", "a\nb", "x".repeat(64).as_str()]
		{
			assert_eq!(
				SettingCommand::parse(Family::Ntp, "server", suspect),
				None,
				"{suspect:?} must not reach the camera"
			);
		}
	}

	#[test]
	fn a_parent_name_is_printable_and_bounded() {
		assert!(SettingCommand::parse(Family::General, "device_name", "Porch").is_some());
		assert!(SettingCommand::parse(Family::General, "device_name", "Entrée ✨").is_some());
		assert_eq!(SettingCommand::parse(Family::General, "device_name", ""), None);
		assert_eq!(
			SettingCommand::parse(Family::General, "device_name", &"n".repeat(32)),
			None
		);
		assert_eq!(
			SettingCommand::parse(Family::General, "device_name", "ligne1\nligne2"),
			None,
			"a newline would break the XML frame"
		);
	}

	#[test]
	fn the_text_counts_in_chars_not_bytes() {
		// "Entrée" with its accent: 6 characters, 7 bytes in UTF-8. A bound counted in
		// bytes would refuse names the camera accepts.
		let ok = SettingCommand::parse(Family::Osd, "name", "Entrée");
		assert_eq!(ok, Some(SettingCommand::OsdName("Entrée".into())));
	}

	#[test]
	fn the_dst_offset_is_signed() {
		assert_eq!(
			SettingCommand::parse(Family::Dst, "offset", "-1"),
			Some(SettingCommand::DstOffset("-1".into()))
		);
		assert_eq!(SettingCommand::parse(Family::Dst, "offset", "15"), None);
		assert_eq!(SettingCommand::parse(Family::Dst, "offset", "-15"), None);
	}

	#[test]
	fn the_chime_accepts_a_melody_alone_or_a_full_pair() {
		assert_eq!(
			SettingCommand::parse(Family::Chime, "ring", "3"),
			Some(SettingCommand::ChimeRing {
				chime_id: None,
				music_id: "3".into()
			})
		);
		assert_eq!(
			SettingCommand::parse(Family::Chime, "ring", "2 7"),
			Some(SettingCommand::ChimeRing {
				chime_id: Some("2".into()),
				music_id: "7".into()
			})
		);
		assert_eq!(SettingCommand::parse(Family::Chime, "ring", "two"), None);
		assert_eq!(SettingCommand::parse(Family::Chime, "ring", "2 seven"), None);
	}

	#[test]
	fn a_read_only_leaf_is_not_commandable() {
		// Wi-Fi, sleep, the masks and the battery have no SET: a command has to be
		// refused, not ignored in silence.
		for (family, leaf) in [
			(Family::Wifi, "signal"),
			(Family::Sleep, "state"),
			(Family::Mask, "enable"),
			(Family::Battery, "voltage"),
			(Family::General, "is_dst"),
		] {
			assert_eq!(
				SettingCommand::parse(family, leaf, "1"),
				None,
				"{}/{} is not writable",
				family.slug(),
				leaf
			);
		}
	}

	#[test]
	fn an_unknown_leaf_of_a_known_family_is_refused() {
		assert_eq!(SettingCommand::parse(Family::Ntp, "mot_de_passe", "x"), None);
	}

	#[test]
	fn family_and_leaf_of_the_command_land_right() {
		let cmd = SettingCommand::parse(Family::AutoReboot, "weekday", "Tuesday")
			.expect("Tuesday is a commanded leaf");
		assert_eq!(cmd.family(), Family::AutoReboot);
		assert_eq!(cmd.leaf(), "weekday");
	}

	#[test]
	fn accepts_mirror_parse() {
		assert!(accepts("audio", "volume"));
		assert!(accepts("chime", "ring"));
		assert!(accepts("chime", "volume"));
		assert!(!accepts("wifi", "signal"));
		assert!(!accepts("general", "is_dst"));
		assert!(!accepts("nosuchfamily", "volume"));
		assert!(!accepts("audio", "nosuchleaf"));
		assert!(!accepts("ai", "enable"), "no enable tag is known for this family");
	}

	/// The chime volume carries the range **seen** ("0-4 typical"), not a 0-100
	/// inherited from the general volume. A value outside it does not leave: the 485
	/// would store it and answer 200, which is a lying "OK".
	#[test]
	fn a_chime_volume_never_leaves_the_observed_range() {
		for v in ["0", "2", "4"] {
			let cmd = SettingCommand::parse(Family::Chime, "volume", v)
				.unwrap_or_else(|| panic!("the value {v} is inside the documented range"));
			assert_eq!(cmd, SettingCommand::ChimeVolume(v.to_string()));
			assert_eq!(cmd.family(), Family::Chime);
			assert_eq!(cmd.leaf(), "volume");
		}
		for v in ["5", "-1", "40", "2.5", "four", ""] {
			assert_eq!(
				SettingCommand::parse(Family::Chime, "volume", v),
				None,
				"{v} is not a value the document knows"
			);
		}
	}
}
