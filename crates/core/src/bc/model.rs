use crate::Credentials;

pub use super::crypto::EncryptionProtocol;
pub use super::xml::{BcPayloads, BcXml, Extension};
use std::collections::HashSet;

pub(super) const MAGIC_HEADER: u32 = 0x0abcdef0;
/// "Reversed" magic header, accepted by the deserialiser identically to
/// [`MAGIC_HEADER`]. Field-observed on certain replies (notably `snap`
/// JPEG payloads) where every other header field is still little-endian
/// — the byte-reversed magic appears to be a hint about *binary
/// payload* endianness, NOT the header itself.
///
/// Today bairelay drops the hint: every consumer treats the payload as
/// the codec / framing already specifies (JPEGs are JPEGs regardless,
/// XML is UTF-8 text). If a future feature ever needs to honour the
/// hint, the place to thread it through is `bc::de::bc_header` →
/// `BcHeader` so the typed reply path can branch on it. Documenting the
/// silence rather than removing the constant: the wire format includes
/// it and we still need to accept it.
pub(super) const MAGIC_HEADER_REV: u32 = 0x0fedcba0;

/// Login messages have this ID
pub const MSG_ID_LOGIN: u32 = 1;
/// Logout messages have this ID
pub const MSG_ID_LOGOUT: u32 = 2;
/// Video and Audio Streams messages have this ID
pub const MSG_ID_VIDEO: u32 = 3;
/// ID used to stop the video stream
pub const MSG_ID_VIDEO_STOP: u32 = 4;
/// TalkAbility messages have this ID
pub const MSG_ID_TALKABILITY: u32 = 10;
/// TalkReset messages have this ID
pub const MSG_ID_TALKRESET: u32 = 11;
/// PtzControl messages have this ID
pub const MSG_ID_PTZ_CONTROL: u32 = 18;
/// PTZ goto preset position
pub const MSG_ID_PTZ_CONTROL_PRESET: u32 = 19;
/// Reboot messages have this ID
pub const MSG_ID_REBOOT: u32 = 23;
/// Request motion detection messages
pub const MSG_ID_MOTION_REQUEST: u32 = 31;
/// Motion detection messages
pub const MSG_ID_MOTION: u32 = 33;
/// Write the `<VideoInput>` / `<InputAdvanceCfg>` image block. The SET wants
/// the block the GET (26) just printed, both carriers together.
pub const MSG_ID_SET_IMAGE: u32 = 25;
/// Read the image block: one reply carries **both** `<VideoInput>` and
/// `<InputAdvanceCfg>`.
pub const MSG_ID_GET_IMAGE: u32 = 26;
/// Set service ports
pub const MSG_ID_SET_SERVICE_PORTS: u32 = 36;
/// Get service ports
pub const MSG_ID_GET_SERVICE_PORTS: u32 = 37;
/// Read the `<Ntp>` clock-synchronisation block.
pub const MSG_ID_GET_NTP: u32 = 38;
/// Write the `<Ntp>` block back (read-modify-write of the 38 reply).
pub const MSG_ID_SET_NTP: u32 = 39;
/// Get Email setting
pub const MSG_ID_GET_EMAIL: u32 = 42;
/// Set email settings
pub const MSG_ID_SET_EMAIL: u32 = 43;
/// Read the on-screen-display blocks: one reply carries **both**
/// `<OsdDatetime>` and `<OsdChannelName>`. Never read OSD through 26 —
/// the reference client's pre-0.6 bug, which wrote tags the 25 setter
/// does not have.
pub const MSG_ID_GET_OSD: u32 = 44;
/// Write the OSD blocks back (both carriers, patched in place).
pub const MSG_ID_SET_OSD: u32 = 45;
/// Read the motion-detection configuration. The reply carries `<MdAlarm>`
/// on the firmwares recorded on the operator's doorbell, and `<MD>`
/// on the ones that print the setter's own shape; both are modelled.
pub const MSG_ID_GET_MOTION_DETECT: u32 = 46;
/// Write the motion-detection configuration. Documented request body is
/// `<MD version="1.1">`; see `BcCamera::set_motion_cfg` for why a camera
/// that only printed `<MdAlarm>` is not written.
pub const MSG_ID_SET_MOTION_DETECT: u32 = 47;
/// Read the privacy-mask (shelter) block `<Shelter>`.
pub const MSG_ID_GET_PRIVACY_MASK: u32 = 52;
/// Write the `<Shelter>` block back.
pub const MSG_ID_SET_PRIVACY_MASK: u32 = 53;
/// Get users and general system info
pub const MSG_ID_GET_ABILITY_SUPPORT: u32 = 58;
/// Update, create and remove users
pub const MSG_ID_UPDATE_USER_LIST: u32 = 59;
/// Version messages have this ID
pub const MSG_ID_VERSION: u32 = 80;
/// Ping messages have this ID
pub const MSG_ID_PING: u32 = 93;
/// Write the `<AutoReboot>` schedule. Note the asymmetry: the setter is the
/// **lower** id (100) and the getter 101.
pub const MSG_ID_SET_AUTO_REBOOT: u32 = 100;
/// Read the `<AutoReboot>` schedule.
pub const MSG_ID_GET_AUTO_REBOOT: u32 = 101;
/// General system info messages have this ID
pub const MSG_ID_GET_GENERAL: u32 = 104;
/// Setting general system info (clock mostly) messages have this ID
pub const MSG_ID_SET_GENERAL: u32 = 105;
/// Read the camera's DST configuration. The reply body is `<Dst>`, often
/// flagged binary on the wire even though the bytes are UTF-8 XML — see
/// `BcCamera::get_dst` for the parse handling.
pub const MSG_ID_GET_DST: u32 = 106;
/// Write the camera's DST configuration. Symmetric to `MSG_ID_GET_DST`;
/// presumed but not yet exercised in the wild — bairelay does not write
/// DST today.
pub const MSG_ID_SET_DST: u32 = 107;
/// Snapshot to get a jpeg image
pub const MSG_ID_SNAP: u32 = 109;
/// Used to grab the UID
pub const MSG_ID_UID: u32 = 114;
/// Read `<WifiSignal>`: the single `<signal>` field's unit is undocumented
/// (dBm or %; the reference client never converts it), so it is relayed as
/// the camera's own text.
pub const MSG_ID_GET_WIFI_SIGNAL: u32 = 115;
/// Read `<Wifi>`: the joined network. The firmware may return the `<key>`,
/// so nothing from this block is ever published to MQTT.
pub const MSG_ID_GET_WIFI: u32 = 116;
/// Used to pass the token and client ID for push notifications
pub const MSG_ID_PUSH_INFO: u32 = 124;
/// Send a test email configuration
pub const MSG_ID_TEST_EMAIL: u32 = 141;
/// StreamInfoList messages have this ID
pub const MSG_ID_STREAM_INFO_LIST: u32 = 146;
/// Used to get the abilities of a user
pub const MSG_ID_ABILITY_INFO: u32 = 151;
/// Get the available PTZ position presets
pub const MSG_ID_GET_PTZ_PRESET: u32 = 190;
/// Get the support details (ptz, talk et)
pub const MSG_ID_GET_SUPPORT: u32 = 199;
/// Will send the talk config for talk back data to follow this msg
pub const MSG_ID_TALKCONFIG: u32 = 201;
/// Used to send talk back binary data
pub const MSG_ID_TALK: u32 = 202;
/// Getting the LED status is done with this ID
pub const MSG_ID_GET_LED_STATUS: u32 = 208;
/// Setting the LED status is done with this ID
pub const MSG_ID_SET_LED_STATUS: u32 = 209;
/// Getting PIR status messages have this ID
pub const MSG_ID_GET_PIR_ALARM: u32 = 212;
/// Setting PIR status messages have this ID
pub const MSG_ID_START_PIR_ALARM: u32 = 213;
/// Set Email Task
pub const MSG_ID_SET_EMAIL_TASK: u32 = 216;
/// Get Email Task
pub const MSG_ID_GET_EMAIL_TASK: u32 = 217;
/// UDP Keep alive
pub const MSG_ID_UDP_KEEP_ALIVE: u32 = 234;
/// Battery message initiaed by the camera
pub const MSG_ID_BATTERY_INFO_LIST: u32 = 252;
/// Battery message initiaed by the client
pub const MSG_ID_BATTERY_INFO: u32 = 253;
/// Used for to play sounds like the siren
pub const MSG_ID_PLAY_AUDIO: u32 = 263;
/// Read the `<audioCfg>` loudspeaker / intercom block (volumes, visitor
/// loudspeaker). The only place `talkAndReplyVolume` exists.
pub const MSG_ID_GET_AUDIO_CFG: u32 = 264;
/// Write the `<audioCfg>` block back. The firmware wants the whole block it
/// just printed with one tag changed, not a partial block.
pub const MSG_ID_SET_AUDIO_CFG: u32 = 265;
/// Manual Floodlight Control
pub const MSG_ID_FLOODLIGHT_MANUAL: u32 = 288;
/// Set Floodlight tasks xml
pub const MSG_ID_FLOODLIGHT_TASKS_WRITE: u32 = 290;
/// Floodlight status report from the camera
pub const MSG_ID_FLOODLIGHT_STATUS_LIST: u32 = 291;
/// Used for camera Zoom read
pub const MSG_ID_GET_ZOOM_FOCUS: u32 = 294;
/// Used for camera Zoom write
pub const MSG_ID_SET_ZOOM_FOCUS: u32 = 295;
/// Read one AI-detection type's `<AiDetectCfg>`. The request names the type,
/// so there is no single all-types read.
///
/// **Documented in the upstream sources**: the two
/// upstream sources contradict each other on the setter (343 vs 345), but the
/// **executable code** of the reference library uses only **343** - so 343 is
/// what is coded here, and 345 stays a note. `bairelay` does not write this
/// block yet: the encoding of fractions (`5.000000e-01`, two-digit exponent)
/// and the exact body of request 342 have no local capture, and a truncation
/// silently accepted would write thresholds.
pub const MSG_ID_GET_AI_DETECT: u32 = 342;
/// Write an AI-detection type's `<AiDetectCfg>` (see
/// [`MSG_ID_GET_AI_DETECT`]: 343 is the setter the reference client executes).
pub const MSG_ID_SET_AI_DETECT: u32 = 343;
/// Observed in app traffic; semantics **not** established (see
/// [`MSG_ID_GET_AI_DETECT`] for why 343 is the one we side with). Never sent.
pub const MSG_ID_AI_DETECT_UNKNOWN: u32 = 345;
/// Get the floodlight task xml
pub const MSG_ID_FLOODLIGHT_TASKS_READ: u32 = 438;
/// Read the microphone AI denoiser block `<aiDenoise>` (enable + level).
/// This is the closest thing the protocol has to a microphone setting: the
/// reference client's own UI calls it "On-device denoiser applied to the
/// camera mic", and no mic gain / mic mute command exists anywhere.
pub const MSG_ID_GET_AI_DENOISE: u32 = 439;
/// Write the `<aiDenoise>` block back (read-modify-write of the 439 reply).
pub const MSG_ID_SET_AI_DENOISE: u32 = 440;
/// Hardwired (mains) chime control: `<dingdongCtrl>` with
/// `machineStateGet` (read) / `machineStateSet` (write). The wired chime
/// never appears in the wireless list (484) — this is its only face.
pub const MSG_ID_DING_DONG_CTRL: u32 = 483;
/// List the chimes paired to the doorbell: empty body, the reply carries
/// repeated `<dingdongDeviceInfo>` blocks. The camera also pushes this
/// list unsolicited when pairing changes; the push is not modelled here.
/// A 400 / timeout on this id is how a model without a wireless chime
/// answers (inferred, not documented as an error table).
pub const MSG_ID_GET_DING_DONG_LIST: u32 = 484;
/// Chime device options on `<dingdongDeviceOpt>`: `getParam` reads
/// `name` / `volLevel` / `ledState`, `setParam` writes them, and
/// `ringWithMusic` makes the chime ring once — the only live proof a
/// paired chime answers through the doorbell session.
pub const MSG_ID_DING_DONG_OPT: u32 = 485;
/// Per-event chime configuration: empty body, the reply carries repeated
/// `<deviceCfg>` blocks, each with `<id>` / `<ringId>` and one
/// `<alarminCfg>` per ringable event type.
pub const MSG_ID_GET_DING_DONG_CFG: u32 = 486;
/// Write one `<deviceCfg>` block back inside a `<dingdongCfg>` wrapper.
/// Like other SETs, this firmware may swallow a truncated block silently.
pub const MSG_ID_SET_DING_DONG_CFG: u32 = 487;
/// Read `<sleepState>`: whether the battery camera is asleep, plus its PTZ
/// park position. Read-only — there is no documented setter, and waking or
/// sleeping the device is done by the wake path, not by a settings write.
pub const MSG_ID_GET_SLEEP_STATE: u32 = 574;
/// Chime silent mode read: `<dingdongSilentMode><id>` in, `<time>` back.
pub const MSG_ID_GET_DING_DONG_SILENT: u32 = 609;
/// Chime silent mode write: `<id>` + `<time>` + `<type>63</type>` (the
/// type value is hard-coded upstream; its meaning is undocumented).
/// Re-writing the block RE-ARMS the mute countdown, which is why
/// `set_ding_dong_cfg` only sends it when the text actually changed.
pub const MSG_ID_SET_DING_DONG_SILENT: u32 = 610;
/// Get the HTTP webhook configuration (`HaCfg`)
pub const MSG_ID_GET_HA_CFG: u32 = 806;
/// Set the HTTP webhook configuration (`HaCfg`)
pub const MSG_ID_SET_HA_CFG: u32 = 807;

/// An empty password in legacy format
pub const EMPTY_LEGACY_PASSWORD: &str =
	"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

/// Top level bc message
#[derive(Debug, PartialEq)]
pub struct Bc {
	/// header part of the message
	pub meta: BcMeta,
	/// body part of the message which can either be Legacy or Modern
	pub body: BcBody,
}

///
///  The body of a bc message is either legacy or modern
///
#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum BcBody {
	/// Legacy is unsupported except for login where it is used
	/// to negotiate the initial login and upgrade to modern
	LegacyMsg(LegacyMsg),
	/// Modern is the current reolink protocol it is mostly
	/// xml based
	ModernMsg(ModernMsg),
}

/// Modern messages have two payloads split by the `payload_offset` in the header
///
/// The first payload is extension which describes the second payload. If the
/// `payload_offset` is `0` then their is no `extension` (usually because it has
/// already been negotiated in a previous message) and it is `None`
///
/// The second payload contains the actual data of interest and is all bytes after
/// the `payload_offset` up to the `body_len`. If `payload_offset`
/// equals `body_len` then there is not payload and it is `None`
///
/// If `payload_offset` is `0` and equal to `body_len` then there is neither
/// `extension` or `payload` these are header only messages. This usually occurs to acknoledge receipt
/// of a command. In such cases the header `response_code` should be checked.
///
#[derive(Debug, Default, PartialEq)]
pub struct ModernMsg {
	/// Extension describes the following payload such as which channel id it is for
	pub extension: Option<Extension>,
	/// Primary payload which is dependant on MsgID
	pub payload: Option<BcPayloads>,
}

/// Legacy login formats. Only login is supported
#[derive(Debug, PartialEq, Eq)]
pub enum LegacyMsg {
	/// Legacy login data is constructed from username and password
	/// that will (usually but not always, depending on camera) be hashed
	LoginMsg {
		/// Username for a legacy login
		username: String,
		/// Password for a legacy login
		password: String,
	},
	/// Sent to upgrade to modern and not exposed the MD5 username/password
	LoginUpgrade,
	/// Any other type of legacy message will be collected here
	UnknownMsg,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct BcHeader {
	pub body_len: u32,
	pub msg_id: u32,
	pub channel_id: u8,
	pub stream_type: u8,
	pub msg_num: u16,
	pub response_code: u16,
	pub class: u16,
	pub payload_offset: Option<u32>,
}

/// The components of the Baichuan TLV header that are not
/// descriptions of the Body (the application dictates these)
#[derive(Debug, PartialEq, Eq)]
pub struct BcMeta {
	/// Message ID dictaes the major content of the message
	pub msg_id: u32,
	/// In most cases 0 but can be other values for NVRs
	pub channel_id: u8,
	/// In most cases this is unimportant but 0 means Clear Stream while 1 means Fluent stream
	/// This is only really used during `[MSG_ID_VIDEO]` streams when the SD `subStreams` are requested
	pub stream_type: u8,
	/// On modern messages this is the response code
	/// When sending a command it is set to `0`. The reply from the camera can be
	///
	/// - `200` for OK
	///
	/// - `400` for bad request
	///
	/// A malformed packet will return a `400` code
	pub response_code: u16,
	/// A message ID is used to match replies with requests. The camera will parrot back
	/// this number in its reply
	///
	/// If there a message is too long to fit in one packet it will be split over multiple
	/// messages all with the same `msg_num` (this can happing in video streams, talk and when
	/// sending a firmware update)
	pub msg_num: u16,
	/// The class is mostly an unknown quanitiy but does dictate the size of the header
	/// know values are
	///
	/// - 0x6514: "legacy" 20 bytes
	/// - 0x6614: "modern" 20 bytes
	/// - 0x6414: "modern" 24 bytes
	/// - 0x0000: "modern" 24 bytes
	pub class: u16,
}

#[derive(Debug)]
pub(crate) struct BcContext {
	pub(crate) credentials: Credentials,
	pub(crate) in_bin_mode: HashSet<u16>,
	pub(crate) encryption_protocol: EncryptionProtocol,
	pub(crate) debug: bool,
}

impl Bc {
	/// Constructs a xml payload only Bc message
	pub fn new_from_xml(meta: BcMeta, xml: BcXml) -> Bc {
		Self::new(meta, None, Some(BcPayloads::BcXml(xml)))
	}

	/// Constructs an Extension only Bc message
	pub fn new_from_ext(meta: BcMeta, ext: Extension) -> Bc {
		Self::new(meta, Some(ext), None)
	}

	/// Constucts a header only Bc message
	pub fn new_from_meta(meta: BcMeta) -> Bc {
		Self::new(meta, None, None)
	}

	/// Constructs a message with both extension and xml payload
	pub fn new_from_ext_xml(meta: BcMeta, ext: Extension, xml: BcXml) -> Bc {
		Self::new(meta, Some(ext), Some(BcPayloads::BcXml(xml)))
	}

	/// General method to constructs a Bc message
	///
	/// Use this if your constructing a binary payload but otherwise the other constructors
	/// are better suited to the task
	pub fn new(meta: BcMeta, extension: Option<Extension>, payload: Option<BcPayloads>) -> Bc {
		Bc {
			meta,
			body: BcBody::ModernMsg(ModernMsg { extension, payload }),
		}
	}
}

impl BcContext {
	pub(crate) fn new(credentials: Credentials) -> BcContext {
		BcContext {
			credentials,
			in_bin_mode: HashSet::new(),
			encryption_protocol: EncryptionProtocol::Unencrypted,
			debug: false,
		}
	}

	#[allow(unused)] // Used in tests
	pub(crate) fn new_with_encryption(encryption_protocol: EncryptionProtocol) -> BcContext {
		BcContext {
			// Anonymous-login shape — replaces the dropped
			// `Credentials::default()` factory-password footgun.
			credentials: super::super::bc_protocol::Credentials::new("", None::<String>),
			in_bin_mode: HashSet::new(),
			encryption_protocol,
			debug: false,
		}
	}

	pub(crate) fn set_encrypted(&mut self, encryption_protocol: EncryptionProtocol) {
		self.encryption_protocol = encryption_protocol;
	}

	pub(crate) fn get_encrypted(&self) -> &EncryptionProtocol {
		&self.encryption_protocol
	}

	pub(crate) fn binary_on(&mut self, msg_id: u16) {
		self.in_bin_mode.insert(msg_id);
	}

	#[allow(unused)] // Used in tests
	pub(crate) fn binary_off(&mut self, msg_id: u16) {
		self.in_bin_mode.remove(&msg_id);
	}

	pub(crate) fn debug_on(&mut self) {
		self.debug = true;
	}
}

impl BcHeader {
	/// Check if this header corresponds to a known modern message class
	pub fn is_modern(&self) -> bool {
		// Most modern messages have an extra word at the end of the header; this
		// serves as the start offset of the appended payload data, if any.
		// A notable exception is the encrypted reply to the login message;
		// in this case the message is modern (with XML encryption etc), but there is
		// no extra word.
		// Here are the message classes:
		// 0x6514: legacy, no  bin offset (initial login message, encrypted or not)
		// 0x6614: modern, no  bin offset (reply to encrypted 0x6514 login)
		// 0x6414: modern, has bin offset, encrypted if supported (re-sent login message)
		// 0x0000, modern, has bin offset (most modern messages)
		self.class != 0x6514
	}

	/// Converts a header into a `BcMeta` this mostly works by striping aspects that are
	/// not desciptions of the data such as `msg_len`
	pub fn to_meta(&self) -> BcMeta {
		BcMeta {
			msg_id: self.msg_id,
			msg_num: self.msg_num,
			channel_id: self.channel_id,
			response_code: self.response_code,
			stream_type: self.stream_type,
			class: self.class,
		}
	}

	/// Constuct a [`BcHeader`] from a [`BcMeta`]
	///
	/// This requires additional data such as the `body_len`
	///
	/// # Parameters
	///
	/// * `meta` - the [`BcMeta`] to convert
	///
	/// * `body_len` - The length of the body (extension and payload) in bytes
	///
	/// * `payload_offset` - The location in bytes where the payload starts and extension ends
	///
	/// # Returns
	///
	/// returns the new `BcHeader`
	///
	pub fn from_meta(meta: &BcMeta, body_len: u32, payload_offset: Option<u32>) -> BcHeader {
		BcHeader {
			payload_offset,
			body_len,
			msg_id: meta.msg_id,
			channel_id: meta.channel_id,
			stream_type: meta.stream_type,
			response_code: meta.response_code,
			msg_num: meta.msg_num,
			class: meta.class,
		}
	}
}

pub(super) fn has_payload_offset(class: u16) -> bool {
	// See BcHeader::is_modern() for a description of which packets have the bin offset
	class == 0x6414 || class == 0x0000
}
