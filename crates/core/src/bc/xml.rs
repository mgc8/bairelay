#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};
use std::{io::BufRead, io::Write};

#[cfg(test)]
use indoc::indoc;

/// There are two types of payloads xml and binary
#[derive(PartialEq, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BcPayloads {
	/// XML payloads are the more common ones and include payloads for camera controls
	BcXml(BcXml),
	/// Binary payloads are received from the camera for streams and sent to the camera
	/// for talk-back and firmware updates
	Binary(Vec<u8>),
}

/// The top level BC Xml
#[derive(PartialEq, Default, Debug, Deserialize, Serialize)]
#[serde(default, rename = "body")]
pub struct BcXml {
	/// Encryption xml is received during login and contain the NONCE
	#[serde(
		default,
		rename = "Encryption",
		skip_serializing_if = "Option::is_none"
	)]
	pub encryption: Option<Encryption>,
	/// LoginUser xml is used during modern login
	#[serde(default, rename = "LoginUser", skip_serializing_if = "Option::is_none")]
	pub login_user: Option<LoginUser>,
	/// LoginNet xml is used during modern login
	#[serde(default, rename = "LoginNet", skip_serializing_if = "Option::is_none")]
	pub login_net: Option<LoginNet>,
	/// The final part of a login sequence will return DeviceInfo xml
	#[serde(
		default,
		rename = "DeviceInfo",
		skip_serializing_if = "Option::is_none"
	)]
	pub device_info: Option<DeviceInfo>,
	/// The VersionInfo xml is received in reply to a version request
	#[serde(
		default,
		rename = "VersionInfo",
		skip_serializing_if = "Option::is_none"
	)]
	pub version_info: Option<VersionInfo>,
	/// Preview xml is used as part of the stream request to set the stream quality and channel
	#[serde(default, rename = "Preview", skip_serializing_if = "Option::is_none")]
	pub preview: Option<Preview>,
	#[serde(
		default,
		rename = "SystemGeneral",
		skip_serializing_if = "Option::is_none"
	)]
	/// SystemGeneral xml is sent or received as part of the clock get/setting
	pub system_general: Option<SystemGeneral>,
	/// Received as part of the Genral system info request
	#[serde(default, rename = "Norm", skip_serializing_if = "Option::is_none")]
	pub norm: Option<Norm>,
	/// Daylight-saving-time configuration. Carried in the body of
	/// `MSG_ID_GET_DST` (106) replies. Camera autonomously applies the
	/// `<offset>` to displayed local time when the current date is
	/// inside the start/end window — clients writing
	/// `<SystemGeneral>` must therefore use the BASE UTC offset (DST
	/// excluded) for `<timeZone>` and UTC for the wallclock fields,
	/// otherwise the camera double-applies DST and drifts forward by
	/// `<offset>` hours.
	#[serde(default, rename = "Dst", skip_serializing_if = "Option::is_none")]
	pub dst: Option<Dst>,
	/// Received as part of the LEDState info request
	#[serde(default, rename = "LedState", skip_serializing_if = "Option::is_none")]
	pub led_state: Option<LedState>,
	/// Sent as part of the TalkConfig to prepare the camera for audio talk-back
	#[serde(
		default,
		rename = "TalkConfig",
		skip_serializing_if = "Option::is_none"
	)]
	pub talk_config: Option<TalkConfig>,
	/// rfAlarmCfg xml is sent or received as part of the PIR get/setting
	#[serde(
		default,
		rename = "rfAlarmCfg",
		skip_serializing_if = "Option::is_none"
	)]
	pub rf_alarm_cfg: Option<RfAlarmCfg>,
	/// Revieced as part of the TalkAbility request
	#[serde(
		default,
		rename = "TalkAbility",
		skip_serializing_if = "Option::is_none"
	)]
	pub talk_ability: Option<TalkAbility>,
	/// Received when motion is detected
	#[serde(
		default,
		rename = "AlarmEventList",
		skip_serializing_if = "Option::is_none"
	)]
	pub alarm_event_list: Option<AlarmEventList>,
	/// Sent to move the camera
	#[serde(
		default,
		rename = "PtzControl",
		skip_serializing_if = "Option::is_none"
	)]
	pub ptz_control: Option<PtzControl>,
	/// Sent to manually control the floodlight
	#[serde(
		default,
		rename = "FloodlightManual",
		skip_serializing_if = "Option::is_none"
	)]
	pub floodlight_manual: Option<FloodlightManual>,
	/// Received when the floodlight status is updated
	#[serde(
		rename = "FloodlightStatusList",
		skip_serializing_if = "Option::is_none"
	)]
	pub floodlight_status_list: Option<FloodlightStatusList>,
	/// Sent or received for the PTZ preset functionality
	#[serde(default, rename = "PtzPreset", skip_serializing_if = "Option::is_none")]
	pub ptz_preset: Option<PtzPreset>,
	/// Received on login/low battery events
	#[serde(
		default,
		rename = "BatteryList",
		skip_serializing_if = "Option::is_none"
	)]
	pub battery_list: Option<BatteryList>,
	/// Received on request for battery info
	#[serde(
		default,
		rename = "BatteryInfo",
		skip_serializing_if = "Option::is_none"
	)]
	pub battery_info: Option<BatteryInfo>,
	/// Received on request for a users persmissions/capabilitoes
	#[serde(
		default,
		rename = "AbilityInfo",
		skip_serializing_if = "Option::is_none"
	)]
	pub ability_info: Option<AbilityInfo>,
	/// Received on request for a users persmissions/capabilitoes
	#[serde(default, rename = "PushInfo", skip_serializing_if = "Option::is_none")]
	pub push_info: Option<PushInfo>,
	/// Received on request for a link type
	#[serde(default, rename = "LinkType", skip_serializing_if = "Option::is_none")]
	pub link_type: Option<LinkType>,
	/// Received AND send for the snap message
	#[serde(default, rename = "Snap", skip_serializing_if = "Option::is_none")]
	pub snap: Option<Snap>,
	/// The list of streams and their configuration
	#[serde(
		default,
		rename = "StreamInfoList",
		skip_serializing_if = "Option::is_none"
	)]
	pub stream_info_list: Option<StreamInfoList>,
	/// Thre list of streams and their configuration
	#[serde(default, rename = "Uid", skip_serializing_if = "Option::is_none")]
	pub uid: Option<Uid>,
	/// The floodlight settings for automatically turning on/off on schedule/motion
	#[serde(
		default,
		rename = "FloodlightTask",
		skip_serializing_if = "Option::is_none"
	)]
	pub floodlight_task: Option<FloodlightTask>,
	/// For geting the zoom anf focus of the camera
	#[serde(
		default,
		rename = "PtzZoomFocus",
		skip_serializing_if = "Option::is_none"
	)]
	pub ptz_zoom_focus: Option<PtzZoomFocus>,
	/// For zooming the camera
	#[serde(
		default,
		rename = "StartZoomFocus",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_zoom_focus: Option<StartZoomFocus>,
	/// Get the support xml
	#[serde(default, rename = "Support", skip_serializing_if = "Option::is_none")]
	pub support: Option<Support>,
	/// Play a sound
	#[serde(
		default,
		rename = "audioPlayInfo",
		skip_serializing_if = "Option::is_none"
	)]
	pub audio_play_info: Option<AudioPlayInfo>,
	/// For changing baichaun server port
	#[serde(
		default,
		rename = "ServerPort",
		skip_serializing_if = "Option::is_none"
	)]
	pub server_port: Option<ServerPort>,
	/// For changing http server port
	#[serde(default, rename = "HttpPort", skip_serializing_if = "Option::is_none")]
	pub http_port: Option<HttpPort>,
	/// For changing https server port
	#[serde(default, rename = "HttpsPort", skip_serializing_if = "Option::is_none")]
	pub https_port: Option<HttpsPort>,
	/// For changing rtsp server port
	#[serde(default, rename = "RtspPort", skip_serializing_if = "Option::is_none")]
	pub rtsp_port: Option<RtspPort>,
	/// For changing rtmp server port
	#[serde(default, rename = "RtmpPort", skip_serializing_if = "Option::is_none")]
	pub rtmp_port: Option<RtmpPort>,
	/// For changing rtmp server port
	#[serde(default, rename = "OnvifPort", skip_serializing_if = "Option::is_none")]
	pub onvif_port: Option<OnvifPort>,
	/// Email for setting the email notifications
	#[serde(default, rename = "Email", skip_serializing_if = "Option::is_none")]
	pub email: Option<Email>,
	/// EmailTask for turning the email notifications on/off
	#[serde(default, rename = "EmailTask", skip_serializing_if = "Option::is_none")]
	pub email_task: Option<EmailTask>,
	/// Read and write users
	#[serde(default, rename = "UserList", skip_serializing_if = "Option::is_none")]
	pub user_list: Option<UserList>,
}

impl BcXml {
	/// Parse a BcXml payload from an arbitrary reader.
	///
	/// `quick_xml::de` does not expose a configurable depth cap in the
	/// 0.36 series, but bairelay clips depth-bomb risk *upstream* via
	/// `bc::de::MAX_BC_BODY_LEN` (8 MiB) — the body length is verified
	/// in `bc_header` before the XML payload reaches this parser, so a
	/// pathological deeply-nested input is bounded by the same byte
	/// budget the rest of the wire path enforces. If quick-xml ever
	/// gains a `Deserializer::with_max_depth(...)` knob, wire it in
	/// here and drop this comment.
	pub(crate) fn try_parse(s: impl BufRead) -> Result<Self, quick_xml::de::DeError> {
		quick_xml::de::from_reader(s)
	}
	pub(crate) fn serialize<W: Write>(&self, mut w: W) -> Result<W, quick_xml::de::DeError> {
		let mut buf: Vec<u8> = Vec::new();
		{
			let mut writer = quick_xml::writer::Writer::new(&mut buf);
			// Bubble the (in practice unreachable, since the inputs are
			// fixed string literals) error rather than `expect`ing —
			// keeps a public serializer in `crates/core/` panic-free.
			writer
				.write_event(quick_xml::events::Event::Decl(
					quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None),
				))
				.map_err(quick_xml::de::DeError::from)?;
			writer.write_serializable("body", &self)?;
		}
		// The camera firmware's TiXml parser condenses raw whitespace inside
		// text nodes. That collapses the newlines of the multi-line PEM
		// carried in `<certChain>` on the sigV3 login, corrupting the cert so
		// the cloud token fails validation. The official app emits those
		// newlines as `&#x0A;` entities, which survive condensing — replicate
		// that. quick-xml emits compact XML, so the only raw newlines in `buf`
		// are inside text values, where this re-encoding is value-preserving.
		w.write_all(&encode_certchain_newlines(&buf))
			.map_err(|e| quick_xml::de::DeError::Custom(e.to_string()))?;
		Ok(w)
	}
}

/// Within the `<certChain>…</certChain>` span only, replace raw `\n`/`\r`
/// with their XML numeric-character-reference entities so a
/// whitespace-condensing parser preserves the PEM newlines. Returns `buf`
/// unchanged when there is no `<certChain>` element (every non-sigV3-login
/// message).
fn encode_certchain_newlines(buf: &[u8]) -> Vec<u8> {
	const OPEN: &[u8] = b"<certChain>";
	const CLOSE: &[u8] = b"</certChain>";
	let find = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).position(|w| w == needle);
	let Some(open) = find(buf, OPEN) else {
		return buf.to_vec();
	};
	let inner = open + OPEN.len();
	let Some(rel_close) = find(&buf[inner..], CLOSE) else {
		return buf.to_vec();
	};
	let close = inner + rel_close;
	let mut out = Vec::with_capacity(buf.len() + 256);
	out.extend_from_slice(&buf[..inner]);
	for &b in &buf[inner..close] {
		match b {
			b'\n' => out.extend_from_slice(b"&#x0A;"),
			b'\r' => out.extend_from_slice(b"&#x0D;"),
			_ => out.push(b),
		}
	}
	out.extend_from_slice(&buf[close..]);
	out
}

impl Extension {
	pub(crate) fn try_parse(s: impl BufRead) -> Result<Self, quick_xml::de::DeError> {
		quick_xml::de::from_reader(s)
	}
	pub(crate) fn serialize<W: Write>(&self, mut w: W) -> Result<W, quick_xml::de::DeError> {
		let mut writer = quick_xml::writer::Writer::new(&mut w);
		// Bubble the (in practice unreachable, since the inputs are
		// fixed string literals) error rather than `expect`ing —
		// keeps a public serializer in `crates/core/` panic-free.
		writer
			.write_event(quick_xml::events::Event::Decl(
				quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None),
			))
			.map_err(quick_xml::de::DeError::from)?;
		writer.write_serializable("Extension", &self)?;
		Ok(w)
	}
}

/// Encryption xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Encryption {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	#[serde(default, rename = "type")]
	/// The hashing algorithm used. Only observed the value of "md5"
	pub type_: String,
	/// The nonce used to negotiate the login and to generate the AES key
	#[serde(default)]
	pub nonce: String,
	/// Auth methods the camera will accept. Newer firmware advertises
	/// `password` / `sigV1` / `sigV3` / `authLogin` / `getAccesskey`;
	/// absent on older firmware. Drives the legacy-vs-sigV3 login branch.
	#[serde(
		default,
		rename = "authTypeList",
		skip_serializing_if = "Option::is_none"
	)]
	pub auth_type_list: Option<AuthTypeList>,
	/// Signature scheme version (`v3` on firmware that requires the
	/// ECDHE-signed login). Absent on older firmware.
	#[serde(default, rename = "sigVer", skip_serializing_if = "Option::is_none")]
	pub sig_ver: Option<String>,
	/// ECDHE key-agreement parameters the camera offers for sigV3 login.
	/// Absent on older firmware.
	#[serde(default, rename = "ECDHE", skip_serializing_if = "Option::is_none")]
	pub ecdhe: Option<Ecdhe>,
}

/// `<authTypeList>` wrapper carrying the repeated `<authType>` elements
/// from the camera's encryption-negotiation reply.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AuthTypeList {
	/// Each accepted auth method, e.g. `password`, `sigV3`.
	#[serde(default, rename = "authType")]
	pub auth_type: Vec<String>,
}

/// `<ECDHE>` block: the camera's ephemeral X25519 public key plus the
/// KDF iteration count for the sigV3 login handshake. Field semantics
/// are documented in `docs/baichuan-sigv3-login.md` once recovered.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Ecdhe {
	/// Key-agreement algorithm. Observed value: `X25519`.
	#[serde(default, rename = "publicKeyAlgo")]
	pub public_key_algo: String,
	/// The camera's ephemeral public key (base64).
	#[serde(default, rename = "publicKey")]
	pub public_key: String,
	/// Signature over the camera's public key (base64).
	#[serde(default, rename = "publicKeySign")]
	pub public_key_sign: String,
	/// KDF iteration count (observed: 1000).
	#[serde(default)]
	pub iterations: u32,
}

/// LoginUser xml. Custom `Debug` redacts `password` so an
/// auto-derived `Debug` on a wrapping struct (e.g. `Bc`,
/// `BcXml`) cannot leak the plaintext credential to logs —
/// `logout.rs` sends this struct on every disconnect with the
/// password in the clear, and `UnintelligibleReply.reply` could
/// otherwise print it on a parse failure.
#[derive(PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct LoginUser {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Camera-local login method when the legacy + sigV3 logins don't
	/// apply: `getAccesskey` (request the authLogin challenge) or
	/// `authLogin` (final challenge-response). Absent on every other
	/// login — see `login_authlogin`.
	#[serde(default, rename = "authType", skip_serializing_if = "Option::is_none")]
	pub auth_type: Option<String>,
	/// Username to login as
	#[serde(default, rename = "userName", skip_serializing_if = "String::is_empty")]
	pub user_name: String,
	/// Password for login in plain text
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub password: String,
	/// Unknown always `1`
	#[serde(default, rename = "userVer", skip_serializing_if = "is_zero_u32")]
	pub user_ver: u32,
	/// Client kind, e.g. `"app"`. Sent on the sigV3 login; absent on the
	/// legacy login.
	#[serde(
		default,
		rename = "clientType",
		skip_serializing_if = "Option::is_none"
	)]
	pub client_type: Option<String>,
	/// sigV3 only: base64 of our ephemeral X25519 public key.
	#[serde(default, rename = "publicKey", skip_serializing_if = "Option::is_none")]
	pub public_key: Option<String>,
	/// sigV3 only: cloud-issued token key (base64) from the `getAccesskey`
	/// bundle. Echoed verbatim — not ECDHE-derived. Field order matters:
	/// the official app emits `tokenKey` BEFORE `cipherContent`, so keep
	/// this declared before `cipher_content` (serde serializes in order).
	#[serde(default, rename = "tokenKey", skip_serializing_if = "Option::is_none")]
	pub token_key: Option<String>,
	/// sigV3 only: base64 of `AES-128-CFB(cipherContent-JSON)` — the
	/// password proof keyed by the ECDHE shared secret.
	#[serde(
		default,
		rename = "cipherContent",
		skip_serializing_if = "Option::is_none"
	)]
	pub cipher_content: Option<String>,
	/// sigV3 only: the reolink.com PEM certificate chain the camera validates
	/// the cloud token against.
	#[serde(default, rename = "certChain", skip_serializing_if = "Option::is_none")]
	pub cert_chain: Option<String>,
	/// `getAccesskey` step only: the `<AuthInfo>` carrying `authCode`.
	#[serde(default, rename = "AuthInfo", skip_serializing_if = "Option::is_none")]
	pub auth_info: Option<AuthInfo>,
	/// sigV3 only, never on the wire: the derived post-login session AES
	/// `(key, iv)`. `run_sigv3_direct` attaches it; the codec reads it while
	/// encoding the signed login and arms the control session to switch to AES
	/// once the camera accepts the login. See `bc/codex.rs`.
	#[serde(skip)]
	pub session_aes: Option<([u8; 16], [u8; 16])>,
}

/// `<AuthInfo>` block sent on the `authType=getAccesskey` login step. The
/// camera answers with the AES-encrypted authLogin challenge.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AuthInfo {
	/// `md5(password + nonce)` truncated to 31 hex chars — the same proof
	/// carried by the legacy `<password>` field.
	#[serde(default, rename = "authCode")]
	pub auth_code: String,
	/// Client model string. The app sends its phone model; bairelay omits
	/// it (the camera does not require it).
	#[serde(
		default,
		rename = "phoneModel",
		skip_serializing_if = "Option::is_none"
	)]
	pub phone_model: Option<String>,
}

/// serde `skip_serializing_if` predicate for `userVer` so the
/// `getAccesskey` request (which omits it) doesn't emit `<userVer>0</…>`.
fn is_zero_u32(v: &u32) -> bool {
	*v == 0
}

impl std::fmt::Debug for LoginUser {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("LoginUser")
			.field("version", &self.version)
			.field("auth_type", &self.auth_type)
			.field("user_name", &self.user_name)
			.field(
				"password",
				&if self.password.is_empty() {
					"<empty>"
				} else {
					"<redacted>"
				},
			)
			.field("user_ver", &self.user_ver)
			.field("client_type", &self.client_type)
			.field("public_key", &self.public_key)
			.field("cipher_content", &self.cipher_content)
			// tokenKey is cloud-issued session-key material — length only.
			.field("token_key", &self.token_key.as_ref().map(|k| k.len()))
			.field("cert_chain", &self.cert_chain.as_ref().map(|c| c.len()))
			.field("auth_info", &self.auth_info)
			// session_aes is the derived AES key/iv — never print it.
			.field("session_aes", &self.session_aes.map(|_| "<redacted>"))
			.finish()
	}
}

/// LoginNet xml
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct LoginNet {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Type of connection usually LAN (even on wifi)
	#[serde(default, rename = "type")]
	pub type_: String,
	/// The port for the udp will be `0` for tcp
	#[serde(default, rename = "udpPort")]
	pub udp_port: u16,
}

impl Default for LoginNet {
	fn default() -> Self {
		LoginNet {
			version: xml_ver(),
			type_: "LAN".to_string(),
			udp_port: 0,
		}
	}
}

/// DeviceInfo xml
///
/// There is more to this xml but we don't deserialize it all
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct DeviceInfo {
	/// Version of device info
	#[serde(default, rename = "@version")]
	pub version: Option<String>,
	/// The resolution xml block
	/// Does not exist for floodlights
	#[serde(default)]
	pub resolution: Option<Resolution>,
}

/// VersionInfo xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct VersionInfo {
	/// Name assigned to the camera
	#[serde(default)]
	pub name: String,
	/// Model Name
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub model: Option<String>,
	/// Camera's serial number
	#[serde(default)]
	pub serialNumber: String,
	/// The camera build day e.g. `"build 19110800"`
	#[serde(default)]
	pub buildDay: String,
	/// The hardware version e.g. `"IPC_517SD5"`
	#[serde(default)]
	pub hardwareVersion: String,
	/// The config version e.g. `"v2.0.0.0"`
	#[serde(default)]
	pub cfgVersion: String,
	/// Firmware version usually a combination of config and build versions e.g.
	/// `"v2.0.0.587_19110800"`
	#[serde(default)]
	pub firmwareVersion: String,
	/// Unusure possibly a more detailed hardware version e.g. `"IPC_51716M110000000100000"`
	#[serde(default)]
	pub detail: String,
}

/// Resolution xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Resolution {
	/// Resolution name is in the format "width*height" i.e. "2304*1296"
	#[serde(default, rename = "resolutionName")]
	pub name: String,
	/// Height of the stream in pixels
	#[serde(default)]
	pub width: u32,
	/// Width of the stream in pixels
	#[serde(default)]
	pub height: u32,
}

/// Preview xml
///
/// This xml is used to request a stream to start
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Preview {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,

	/// Channel id is usually zero unless using a NVR
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Handle usually 0 for mainStream and 1 for subStream
	#[serde(default)]
	pub handle: u32,
	/// Either `"mainStream"` or `"subStream"`
	#[serde(
		default,
		rename = "streamType",
		skip_serializing_if = "Option::is_none"
	)]
	pub stream_type: Option<String>,
}

/// Extension xml
///
/// This is used to describe the subsequent payload passed the `payload_offset`
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(default, rename = "Extension")]
pub struct Extension {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// If the subsequent payload is binary this will be set to 1. Otherwise it is ommited
	#[serde(
		default,
		rename = "binaryData",
		skip_serializing_if = "Option::is_none"
	)]
	pub binary_data: Option<u32>,
	/// Certain requests such `AbilitySupport` require to know which user this
	/// ability support request is for (why camera doesn't know this based on who
	/// is logged in is unknown... Possible security hole)
	#[serde(default, rename = "userName", skip_serializing_if = "Option::is_none")]
	pub user_name: Option<String>,
	/// Certain requests such as `AbilitySupport` require details such as what type of
	/// abilities are you intested in. This is a comma seperated list such as
	/// `"system, network, alarm, record, video, image"`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
	/// The channel ID. This is usually `0` unless using an NVR
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<u8>,
	/// The rfID used in the PIR
	#[serde(default, rename = "rfId", skip_serializing_if = "Option::is_none")]
	pub rf_id: Option<u8>,
	/// Encrypted binary has this to verify successful decryption
	#[serde(default, rename = "checkPos", skip_serializing_if = "Option::is_none")]
	pub check_pos: Option<u32>,
	/// Encrypted binary has this to verify successful decryption
	#[serde(
		default,
		rename = "checkValue",
		skip_serializing_if = "Option::is_none"
	)]
	pub check_value: Option<i32>,
	/// Used in newer encrypted payload packets
	#[serde(
		default,
		rename = "encryptLen",
		skip_serializing_if = "Option::is_none"
	)]
	pub encrypt_len: Option<u32>,
}

impl Default for Extension {
	fn default() -> Extension {
		Extension {
			version: xml_ver(),
			binary_data: None,
			user_name: None,
			token: None,
			channel_id: None,
			rf_id: None,
			check_pos: None,
			check_value: None,
			encrypt_len: None,
		}
	}
}

/// SystemGeneral xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct SystemGeneral {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,

	/// Time zone is negative seconds offset from UTC. So +7:00 is -25200
	#[serde(default, rename = "timeZone", skip_serializing_if = "Option::is_none")]
	pub time_zone: Option<i32>,
	/// Current year
	#[serde(default, rename = "year", skip_serializing_if = "Option::is_none")]
	pub year: Option<i32>,
	/// Current month
	#[serde(default, rename = "month", skip_serializing_if = "Option::is_none")]
	pub month: Option<u8>,
	/// Current day
	#[serde(default, rename = "day", skip_serializing_if = "Option::is_none")]
	pub day: Option<u8>,
	/// Current hour
	#[serde(skip_serializing_if = "Option::is_none")]
	pub hour: Option<u8>,
	/// Current minute
	#[serde(skip_serializing_if = "Option::is_none")]
	pub minute: Option<u8>,
	/// Current second
	#[serde(skip_serializing_if = "Option::is_none")]
	pub second: Option<u8>,

	/// Format to use for On Screen Display usually `"DMY"`
	#[serde(default, rename = "osdFormat", skip_serializing_if = "Option::is_none")]
	pub osd_format: Option<String>,
	/// Unknown usually `0`
	#[serde(
		default,
		rename = "timeFormat",
		skip_serializing_if = "Option::is_none"
	)]
	pub time_format: Option<u8>,

	/// Language e.g. `English` will set the language on the reolink app
	#[serde(skip_serializing_if = "Option::is_none")]
	pub language: Option<String>,
	/// Name assigned to the camera
	#[serde(
		default,
		rename = "deviceName",
		skip_serializing_if = "Option::is_none"
	)]
	pub device_name: Option<String>,
}

/// Norm xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Norm {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	// This is usually just `"NTSC"`
	#[serde(default)]
	norm: String,
}

/// `Dst` xml — camera-side daylight-saving-time configuration.
///
/// The camera tracks DST autonomously: it stores `<SystemGeneral>` with
/// the **base** UTC offset and UTC wallclock, then on display adds
/// `<Dst><offset></offset></Dst>` hours when the current date is inside
/// the `[start_*, end_*)` window per the schedule below.
///
/// Clients setting the clock via `MSG_ID_SET_GENERAL` (105) must NOT
/// pre-bake DST into `<SystemGeneral>`'s `<timeZone>` or wallclock fields
/// when the camera has DST enabled and the current moment falls inside
/// the window — doing so produces a `+offset` drift because the camera's
/// own DST application stacks on top.
///
/// `<startWeekIndex>` semantics: `1`–`4` = "Nth occurrence of `<startWeekday>`
/// in `<startMonth>`"; `5` = "last occurrence in the month". Symmetric
/// for `<endWeekIndex>`.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct Dst {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// `1` = camera applies DST autonomously; `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
	/// DST offset in hours. EU schedules use `1`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub offset: Option<i32>,
	/// Month the DST window opens, `1`–`12`.
	#[serde(
		default,
		rename = "startMonth",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_month: Option<u8>,
	/// `1`–`4` = Nth occurrence of `start_weekday` in `start_month`;
	/// `5` = last occurrence.
	#[serde(
		default,
		rename = "startWeekIndex",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_week_index: Option<u8>,
	/// English weekday name: `"Sunday"`, `"Monday"`, …, `"Saturday"`.
	#[serde(
		default,
		rename = "startWeekday",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_weekday: Option<String>,
	/// Wall-time hour of the start transition (in the camera's local time).
	#[serde(default, rename = "startHour", skip_serializing_if = "Option::is_none")]
	pub start_hour: Option<u8>,
	/// Wall-time minute of the start transition.
	#[serde(
		default,
		rename = "startMinute",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_minute: Option<u8>,
	/// Wall-time second of the start transition.
	#[serde(
		default,
		rename = "startSecond",
		skip_serializing_if = "Option::is_none"
	)]
	pub start_second: Option<u8>,
	/// Month the DST window closes, `1`–`12`.
	#[serde(default, rename = "endMonth", skip_serializing_if = "Option::is_none")]
	pub end_month: Option<u8>,
	/// `1`–`4` = Nth occurrence of `end_weekday` in `end_month`;
	/// `5` = last occurrence.
	#[serde(
		default,
		rename = "endWeekIndex",
		skip_serializing_if = "Option::is_none"
	)]
	pub end_week_index: Option<u8>,
	/// English weekday name for the end transition.
	#[serde(
		default,
		rename = "endWeekday",
		skip_serializing_if = "Option::is_none"
	)]
	pub end_weekday: Option<String>,
	/// Wall-time hour of the end transition.
	#[serde(default, rename = "endHour", skip_serializing_if = "Option::is_none")]
	pub end_hour: Option<u8>,
	/// Wall-time minute of the end transition.
	#[serde(default, rename = "endMinute", skip_serializing_if = "Option::is_none")]
	pub end_minute: Option<u8>,
	/// Wall-time second of the end transition.
	#[serde(default, rename = "endSecond", skip_serializing_if = "Option::is_none")]
	pub end_second: Option<u8>,
}

/// LedState xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct LedState {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel ID of camera to get/set its LED state
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// LED Version, observed value is "2". Should be None when setting the LedState
	#[serde(
		default,
		rename = "ledVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub led_version: Option<u32>,
	/// State of the IR LEDs values are "auto", "open", "close"
	#[serde(default)]
	pub state: String,
	/// State of the LED status light (blue on light), values are "open", "close"
	#[serde(default, rename = "lightState")]
	pub light_state: String,
}

/// FloodlightStatus xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct FloodlightStatus {
	/// Channel ID of floodlight
	#[serde(default, rename = "channel")]
	pub channel_id: u8,
	/// On or off
	#[serde(default)]
	pub status: u8,
}

/// FloodlightStatusList xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct FloodlightStatusList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// List of events
	#[serde(default, rename = "FloodlightStatus")]
	pub floodlight_status_list: Vec<FloodlightStatus>,
}

/// FloodlightManual xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct FloodlightManual {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel ID of floodlight
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// On or off
	#[serde(default)]
	pub status: u8,
	/// How long the manual control should apply for
	#[serde(default)]
	pub duration: u16,
}

/// rfAlarmCfg xml
///
/// Field presence varies by firmware version. Older firmware includes
/// rfID, sensitivity, timeBlockList, alarmHandle. Newer firmware (v1.1+)
/// omits these and adds interval, maxAlarmTime, intervalUseRange, etc.
/// All optional fields use `skip_serializing_if` to preserve round-trip
/// correctness — only fields received from the camera are sent back.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct RfAlarmCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Rfid (present in older firmware, absent in v1.1+)
	#[serde(default, rename = "rfID", skip_serializing_if = "Option::is_none")]
	pub rf_id: Option<u8>,
	/// PIR enabled (0=off, 1=on)
	#[serde(default)]
	pub enable: u8,
	/// PIR sensitivity (older firmware)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sensitivity: Option<u8>,
	/// PIR sensitivity value (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sensiValue: Option<u8>,
	/// Reduce false alarm boolean
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub reduceFalseAlarm: Option<u8>,
	/// Alarm interval (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub interval: Option<u32>,
	/// Max alarm time (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub maxAlarmTime: Option<u32>,
	/// Interval use range flag (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub intervalUseRange: Option<u8>,
	/// Minimum interval in seconds (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub intervalSecMin: Option<u32>,
	/// Maximum interval in seconds (v1.1+)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub intervalSecMax: Option<u32>,
	/// XML time block for all week days (older firmware)
	#[serde(
		rename = "timeBlockList",
		default,
		skip_serializing_if = "Option::is_none"
	)]
	pub time_block_list: Option<TimeBlockList>,
	/// The alarm handle to attach to this Rf (older firmware)
	#[serde(
		rename = "alarmHandle",
		default,
		skip_serializing_if = "Option::is_none"
	)]
	pub alarm_handle: Option<AlarmHandle>,
}

/// TimeBlockList XML
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
#[serde(default, rename = "timeBlockList")]
pub struct TimeBlockList {
	/// List of time block entries which disable/enable the PIR at a time
	#[serde(default, rename = "timeBlock")]
	pub time_block: Vec<TimeBlock>,
}

/// TimeBlock XML Used to set the time to enable/disable PIR dectection
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
#[serde(default, rename = "timeBlock")]
pub struct TimeBlock {
	/// Whether to enable or disable for this time block
	#[serde(default)]
	pub enable: u8,
	/// The day of the week for this block, Monday, Tuesday, Etc
	#[serde(default, rename = "weekDay")]
	pub week_day: String,
	/// Time to start this block
	#[serde(default, rename = "beginHour")]
	pub begin_hour: u8,
	/// Time to end this block
	#[serde(default, rename = "endHour")]
	pub end_hour: u8,
}

#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
/// AlarmHandle Xml
pub struct AlarmHandle {
	/// Items in the alarm handle
	#[serde(default)]
	pub item: Vec<AlarmHandleItem>,
}

#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
/// An item in the alarm handle
#[serde(default, rename = "item")]
pub struct AlarmHandleItem {
	/// The channel ID
	#[serde(default)]
	pub channel: u8,
	/// The handle type: Known values, comma seperated list of snap,rec,push
	#[serde(default, rename = "handleType")]
	pub handle_type: String,
}

/// TalkConfig xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct TalkConfig {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel ID of camera to set the TalkConfig
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Duplex known values `"FDX"`
	#[serde(default)]
	pub duplex: String,
	/// audioStreamMode known values `"followVideoStream"`
	#[serde(default, rename = "audioStreamMode")]
	pub audio_stream_mode: String,
	/// AudioConfig contans the details of the audio to follow
	#[serde(default, rename = "audioConfig")]
	pub audio_config: AudioConfig,
}

/// audioConfig xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
#[serde(default, rename = "audioConfig")]
pub struct AudioConfig {
	/// Unknown only sent during TalkAbility request from the camera
	#[serde(skip_serializing_if = "Option::is_none")]
	pub priority: Option<u32>,
	/// Audio type known values are `"adpcm"`
	///
	/// Do not expect camera to support anything else.
	#[serde(default, rename = "audioType")]
	pub audio_type: String,
	/// Audio sample rate known values are `16000`
	#[serde(default, rename = "sampleRate")]
	pub sample_rate: u16,
	/// Precision of data known vaues are `16` (i.e. 16bit)
	#[serde(default, rename = "samplePrecision")]
	pub sample_precision: u16,
	/// Number of audio samples this should be twice the block size for adpcm
	#[serde(default, rename = "lengthPerEncoder")]
	pub length_per_encoder: u16,
	/// Sound track is the number of tracks known values are `"mono"`
	///
	/// Do not expect camera to support anything else
	#[serde(default, rename = "soundTrack")]
	pub sound_track: String,
}

/// TalkAbility xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct TalkAbility {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Duplexes known values `"FDX"`
	#[serde(default, rename = "duplexList")]
	pub duplex_list: Vec<DuplexList>,
	/// audioStreamModes known values `"followVideoStream"`
	#[serde(default, rename = "audioStreamModeList")]
	pub audio_stream_mode_list: Vec<AudioStreamModeList>,
	/// AudioConfigs contans the details of the audio to follow
	#[serde(default, rename = "audioConfigList")]
	pub audio_config_list: Vec<AudioConfigList>,
}

/// duplexList xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct DuplexList {
	/// The supported duplex known values are "FBX"
	#[serde(default)]
	pub duplex: String,
}

/// audioStreamModeList xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AudioStreamModeList {
	/// The supported audio stream mode
	#[serde(default, rename = "audioStreamMode")]
	pub audio_stream_mode: String,
}

/// audioConfigList xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AudioConfigList {
	/// The supported audio configs
	#[serde(default, rename = "audioConfig")]
	pub audio_config: AudioConfig,
}

/// An XML that desctibes a list of events such as motion detection
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AlarmEventList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// List of events
	#[serde(default, rename = "AlarmEvent")]
	pub alarm_events: Vec<AlarmEvent>,
}

/// An alarm event. Camera can send multiple per message as an array in AlarmEventList.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AlarmEvent {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The channel the event occured on. Usually zero unless from an NVR
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Motion status. Known values are `"MD"` or `"none"`
	#[serde(default)]
	pub status: String,
	/// AI status. A comma-separated list; known values are `"people"`,
	/// `"vehicle"`, `"dog_cat"`, `"other"` or `"none"`
	#[serde(default, rename = "AItype", skip_serializing_if = "Option::is_none")]
	pub ai_type: Option<String>,
	/// The recording status. Known values `0` or `1`
	#[serde(default)]
	pub recording: i32,
	/// The timestamp associated with the recording. `0` if not recording
	#[serde(default, rename = "timeStamp")]
	pub timeStamp: i32,
}

/// The Ptz messages used to move the camera
#[derive(PartialEq, Default, Debug, Deserialize, Serialize)]
pub struct PtzControl {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The channel the event occured on. Usually zero unless from an NVR
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// The amount of movement to perform
	#[serde(default)]
	pub speed: f32,
	/// The direction to transverse. Known values are `"left"`, `"right"`, `"up"`, `"down"`,
	/// `"leftUp"`, `"leftDown"`, `"rightUp"`, `"rightDown"` and `"stop"`
	#[serde(default)]
	pub command: String,
}

/// An XML that describes a list of available PTZ presets
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct PtzPreset {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The channel ID. Usually zero unless from an NVR
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// List of presets
	#[serde(default, rename = "presetList")]
	pub preset_list: PresetList,
}

/// A preset list
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct PresetList {
	/// List of Presets
	#[serde(default)]
	pub preset: Vec<Preset>,
}

/// A preset. Either contains the ID and the name or the ID and the command
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Preset {
	/// The ID of the preset
	#[serde(default)]
	pub id: u8,
	/// The preset name
	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// Command: Known values: `"toPos"` and `"setPos"`.
	///
	/// Newer Argus firmware (observed v3.0.0.5649_25111355) omits the
	/// `<command>` element when returning the preset list. Missing value
	/// deserializes to the empty string; downstream code treats
	/// `command` as opaque.
	#[serde(default)]
	pub command: String,
}

/// A list of battery infos. This message is sent from the camera as
/// an event
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct BatteryList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Battery info items
	#[serde(default, rename = "BatteryInfo")]
	pub battery_info: Vec<BatteryInfo>,
}

/// The individual battery info
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct BatteryInfo {
	/// The channel the for the camera usually 0
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Charge status known values, "chargeComplete", "charging", "none",
	#[serde(default, rename = "chargeStatus")]
	pub charge_status: String,
	/// Status of charging port known values: "solarPanel"
	#[serde(default, rename = "adapterStatus")]
	pub adapter_status: String,
	/// Voltage
	#[serde(default)]
	pub voltage: i32,
	/// Current
	#[serde(default)]
	pub current: i32,
	/// Temperture
	#[serde(default)]
	pub temperature: i32,
	/// % charge from 0-100
	#[serde(default, rename = "batteryPercent")]
	pub battery_percent: u32,
	/// Low power flag. Known values 0, 1 (0=false)
	#[serde(default, rename = "lowPower")]
	pub low_power: u32,
	/// Battery version info: Known values 2
	#[serde(default, rename = "batteryVersion")]
	pub battery_version: u32,
}

/// The ability battery info
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AbilityInfo {
	/// Username with this ability
	#[serde(default, rename = "userName")]
	pub username: String,
	/// System permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub system: Option<AbilityInfoToken>,
	/// Network permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub network: Option<AbilityInfoToken>,
	/// Alarm permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub alarm: Option<AbilityInfoToken>,
	/// Image permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub image: Option<AbilityInfoToken>,
	/// Video permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub video: Option<AbilityInfoToken>,
	/// Secutiry permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub security: Option<AbilityInfoToken>,
	/// Replay permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub replay: Option<AbilityInfoToken>,
	/// PTZ permissions
	#[serde(default, rename = "PTZ", skip_serializing_if = "Option::is_none")]
	pub ptz: Option<AbilityInfoToken>,
	/// IO permissions
	#[serde(default, rename = "IO", skip_serializing_if = "Option::is_none")]
	pub io: Option<AbilityInfoToken>,
	/// Streaming permissions
	#[serde(skip_serializing_if = "Option::is_none")]
	pub streaming: Option<AbilityInfoToken>,
}

/// Ability info for system token
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AbilityInfoToken {
	/// Submodule for this ability info token
	#[serde(default, rename = "subModule")]
	pub sub_module: Vec<AbilityInfoSubModule>,
}

/// Token submodule infomation
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
#[serde(default, rename = "subModule")]
pub struct AbilityInfoSubModule {
	/// The channel the for the camera usually 0
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<u8>,
	/// The comma seperated list of permissions like this: `general_rw, norm_rw, version_ro`
	#[serde(default, rename = "abilityValue")]
	pub ability_value: String,
}

/// PushInfo XML
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct PushInfo {
	/// The token from FCM registration
	#[serde(default)]
	pub token: String,
	/// The phone type, known values: `reo_iphone`
	#[serde(default, rename = "phoneType")]
	pub phone_type: String,
	/// A client ID, seems to be an all CAPS MD5 hash of something
	#[serde(default, rename = "clientID")]
	pub client_id: String,
}

/// The Link Type contains the type of connection present
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct LinkType {
	#[serde(default, rename = "type")]
	/// Type of connection known values `"LAN"`
	pub link_type: String,
}

/// The Snap contains the binary jpeg image details
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Snap {
	/// The snap xml version. Observed values "1.1"
	#[serde(default, rename = "@version")]
	pub version: String,
	#[serde(default, rename = "channelId")]
	/// The channel id to get the snapshot from
	pub channel_id: u8,
	/// Unknown, observed values: 0
	/// value is only set on request
	#[serde(
		default,
		rename = "logicChannel",
		skip_serializing_if = "Option::is_none"
	)]
	pub logic_channel: Option<u8>,
	/// Time of snapshot, zero when requesting
	#[serde(default)]
	pub time: u32,
	/// Request a full frame, observed values: 0
	/// value is only set on request
	#[serde(default, rename = "fullFrame", skip_serializing_if = "Option::is_none")]
	pub full_frame: Option<u32>,
	/// Stream name, observed values: `main`, `sub`
	/// value is only set on request
	#[serde(
		default,
		rename = "streamType",
		skip_serializing_if = "Option::is_none"
	)]
	pub stream_type: Option<String>,
	/// File name, usually of the form `01_20230518140240.jpg`
	/// value is only set on receive
	#[serde(default, rename = "fileName", skip_serializing_if = "Option::is_none")]
	pub file_name: Option<String>,
	/// Size in bytes of the picture
	/// value is only set on receive
	#[serde(
		default,
		rename = "pictureSize",
		skip_serializing_if = "Option::is_none"
	)]
	pub picture_size: Option<u32>,
}

/// The primary reply when asked about the stream info
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct StreamInfoList {
	/// The stream infos. There is usually only one of these
	#[serde(default, rename = "StreamInfo")]
	pub stream_infos: Vec<StreamInfo>,
}

/// The individual reply about the stream info
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct StreamInfo {
	/// Bits in the channel number. Observed values `1`
	#[serde(default, rename = "channelBits")]
	pub channel_bits: u32,
	/// List of encode tabeles. These hold the actual stream data
	#[serde(default, rename = "encodeTable")]
	pub encode_tables: Vec<EncodeTable>,
}

/// The individual reply about the stream info
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct EncodeTable {
	/// The internal name of the stream observed values `"mainStream"`, `"subStream"`
	#[serde(default, rename = "type")]
	pub name: String,
	/// The resolution of the stream
	#[serde(default)]
	pub resolution: StreamResolution,
	/// The default framerate. This is sometimes an index into the table
	#[serde(default, rename = "defaultFramerate")]
	pub default_framerate: u32,
	/// The default bitrate. This is sometimes an index into the table
	#[serde(default, rename = "defaultBitrate")]
	pub default_bitrate: u32,
	/// Table of valid framerates
	#[serde(default, rename = "framerateTable")]
	pub framerate_table: String,
	/// Table of valid bitrates
	#[serde(default, rename = "bitrateTable")]
	pub bitrate_table: String,
}

/// The resolution of the stream
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct StreamResolution {
	/// Width of the stream
	#[serde(default)]
	pub width: u32,
	/// Height of the stream
	#[serde(default)]
	pub height: u32,
}

/// Uid xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Uid {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// This the UID of the camera
	#[serde(default)]
	pub uid: String,
}

/// FloodlightTask xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct FloodlightTask {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel of the camera
	#[serde(default)]
	pub channel: u8,
	/// Alarm Mode: Observed values 1
	#[serde(default, rename = "alarmMode")]
	pub alarm_mode: u32,
	/// Enable/Disable floor light on motion
	#[serde(default)]
	pub enable: u32,
	/// Last Alarm Mode: Observed values 2
	#[serde(default, rename = "lastAlarmMode")]
	pub last_alarm_mode: u32,
	/// Preview Auto: Observed values 0
	#[serde(default)]
	pub preview_auto: u32,
	/// Duration of auto floodlight: Observed values 300 (assume seconds for 5mins)
	#[serde(default)]
	pub duration: u32,
	/// Current brightness of floodlight (in %)
	#[serde(default)]
	pub brightness_cur: u32,
	/// Max brightness (in %)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub brightness_max: Option<u32>,
	/// Min brightness (in %)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub brightness_min: Option<u32>,
	/// Schedule fot auto floodlight
	#[serde(default)]
	pub schedule: ScheduleFloodLight,
	/// Threshold settings for light sensor to consider nightime
	#[serde(default, rename = "lightSensThreshold")]
	pub light_sens_threshold: LightSensThreshold,
	/// Light of schedled auto floodlights
	#[serde(default, rename = "FloodlightScheduleList")]
	pub floodlight_schedule_list: FloodlightScheduleList,
	/// Some sort of multi brightness
	#[serde(default, rename = "nightLongViewMultiBrightness")]
	pub night_long_view_multi_brightness: NightLongViewMultiBrightness,
	/// Detection Type: Observed values none
	#[serde(default, rename = "detectType")]
	pub detect_type: String,
}

/// Schedule for Floodlight Task
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct ScheduleFloodLight {
	/// startHour
	#[serde(default, rename = "startHour")]
	pub start_hour: u32,
	/// startMin: Observed values 0
	#[serde(default, rename = "startMin", skip_serializing_if = "Option::is_none")]
	pub start_min: Option<u32>,
	/// endHour
	#[serde(default, rename = "endHour")]
	pub end_hour: u32,
	/// endMin: Observed values 0
	#[serde(default, rename = "endMin", skip_serializing_if = "Option::is_none")]
	pub end_min: Option<u32>,
}

/// Light Sensor Threshold for FloodLightTask
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct LightSensThreshold {
	/// Min: Observed values 1000
	#[serde(skip_serializing_if = "Option::is_none")]
	pub min: Option<u32>,
	/// Max: OBserved values 2300
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max: Option<u32>,
	/// Light Current Value: Observed Value 1000
	#[serde(default, rename = "lightCur")]
	pub light_cur: u32,
	/// Dark Current Value: Observed Value 1900
	#[serde(default, rename = "darkCur")]
	pub dark_cur: u32,
	/// Light Default: Observed Value 1000
	#[serde(default, rename = "lightDef", skip_serializing_if = "Option::is_none")]
	pub light_def: Option<u32>,
	/// Dark Default: Observed Value 1900
	#[serde(default, rename = "darkDef", skip_serializing_if = "Option::is_none")]
	pub dark_def: Option<u32>,
}

/// Floodlight schdule list for FloodlightTask
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct FloodlightScheduleList {
	/// Max Num observed values 32
	#[serde(default, rename = "maxNum")]
	pub max_num: u32,
}

/// NightView Brightness for FloodLightTask
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct NightLongViewMultiBrightness {
	/// Enabled: Observed values 0, 1
	#[serde(default)]
	pub enable: u8,
	/// alarmBrightness settings
	#[serde(default, rename = "alarmBrightness")]
	pub alarm_brightness: AlarmBrightness,
	/// alarmDelay settings
	#[serde(default, rename = "alarmDelay")]
	pub alarm_delay: AlarmDelay,
}

/// Alarm brightness for NightLongViewMultiBrightness
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AlarmBrightness {
	/// Min: Observed values 1
	#[serde(skip_serializing_if = "Option::is_none")]
	pub min: Option<u32>,
	/// Max: Observed values 100
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max: Option<u32>,
	/// Current: Observed values 100
	#[serde(default)]
	pub cur: u32,
	/// Default: Observed values 100
	#[serde(skip_serializing_if = "Option::is_none")]
	pub def: Option<u32>,
}

/// Alarm delay for NightLongViewMultiBrightness
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AlarmDelay {
	/// Min: Observed values 5
	#[serde(skip_serializing_if = "Option::is_none")]
	pub min: Option<u32>,
	/// Max: Observed values 600
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max: Option<u32>,
	/// Current: Observed values 10
	#[serde(default)]
	pub cur: u32,
	/// Default: Observed values 10
	#[serde(skip_serializing_if = "Option::is_none")]
	pub def: Option<u32>,
}

/// PtzZoomFocus xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct PtzZoomFocus {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel ID
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Max, min and current zoom. (Read Only)
	#[serde(default)]
	pub zoom: HelperPosition,
	/// Max, min and current focus. (Read Only)
	#[serde(default)]
	pub focus: HelperPosition,
}

/// StartZoomFocus xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct StartZoomFocus {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel ID
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Command: Observed values: zoomPos. (Write Only)
	pub command: String,
	/// Target Position: Observed Values: 2994, 2508, 2888, 3089, 3194, 3163. (Write Only)
	#[serde(default, rename = "movePos")]
	pub move_pos: u32,
}

/// Helper for Max, Min, Curr pos of zoom/focus
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct HelperPosition {
	/// Max value
	#[serde(default, rename = "maxPos")]
	pub max_pos: u32,
	/// Min value
	#[serde(default, rename = "minPos")]
	pub min_pos: u32,
	/// Curr value
	#[serde(default, rename = "curPos")]
	pub cur_pos: u32,
}

/// Support xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Support {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// IO port number (input)
	#[serde(
		default,
		rename = "IOInputPortNum",
		skip_serializing_if = "Option::is_none"
	)]
	pub io_input_port_num: Option<u32>,
	/// IO port number (output)
	#[serde(
		default,
		rename = "IOOutputPortNum",
		skip_serializing_if = "Option::is_none"
	)]
	pub io_output_port_num: Option<u32>,
	#[serde(default, rename = "diskNum")]
	/// Number of disks
	#[serde(skip_serializing_if = "Option::is_none")]
	pub disk_num: Option<u32>,
	/// Number of video channels
	#[serde(
		default,
		rename = "channelNum",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_num: Option<u32>,
	/// Number of audio channels
	#[serde(default, rename = "audioNum", skip_serializing_if = "Option::is_none")]
	pub audio_num: Option<u32>,
	/// The supported PTZ Mode: pt
	#[serde(default, rename = "ptzMode", skip_serializing_if = "Option::is_none")]
	pub ptz_mode: Option<String>,
	/// PTZ cfg: 0
	#[serde(default, rename = "ptzCfg", skip_serializing_if = "Option::is_none")]
	pub ptz_cfg: Option<u32>,
	/// Use b485 ptz
	#[serde(default, rename = "b485", skip_serializing_if = "Option::is_none")]
	pub B485: Option<u32>,
	/// Support autoupdate
	#[serde(
		default,
		rename = "autoUpdate",
		skip_serializing_if = "Option::is_none"
	)]
	pub auto_update: Option<u32>,
	/// Support push notificaion alarms
	#[serde(default, rename = "pushAlarm", skip_serializing_if = "Option::is_none")]
	pub push_alarm: Option<u32>,
	/// Support ftp
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ftp: Option<u32>,
	/// Support test for ftp
	#[serde(default, rename = "ftpTest", skip_serializing_if = "Option::is_none")]
	pub ftp_test: Option<u32>,
	/// Support email notification
	#[serde(skip_serializing_if = "Option::is_none")]
	pub email: Option<u32>,
	/// Support wifi connections
	#[serde(skip_serializing_if = "Option::is_none")]
	pub wifi: Option<u32>,
	/// Support recording
	#[serde(skip_serializing_if = "Option::is_none")]
	pub record: Option<u32>,
	/// Support test for wifi
	#[serde(default, rename = "wifiTest", skip_serializing_if = "Option::is_none")]
	pub wifi_test: Option<u32>,
	/// Support rtsp
	#[serde(skip_serializing_if = "Option::is_none")]
	pub rtsp: Option<u32>,
	/// Support onvif
	#[serde(skip_serializing_if = "Option::is_none")]
	pub onvif: Option<u32>,
	/// Support audio talk
	#[serde(default, rename = "audioTalk", skip_serializing_if = "Option::is_none")]
	pub audio_talk: Option<u32>,
	/// RF version
	#[serde(default, rename = "rfVersion", skip_serializing_if = "Option::is_none")]
	pub rf_version: Option<u32>,
	/// Support rtmp
	#[serde(skip_serializing_if = "Option::is_none")]
	pub rtmp: Option<u32>,
	/// Has external stream
	#[serde(
		default,
		rename = "noExternStream",
		skip_serializing_if = "Option::is_none"
	)]
	pub no_extern_stream: Option<u32>,
	/// Time format
	#[serde(
		default,
		rename = "timeFormat",
		skip_serializing_if = "Option::is_none"
	)]
	pub time_format: Option<u32>,
	/// DDNS version
	#[serde(
		default,
		rename = "ddnsVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub ddns_version: Option<u32>,
	/// Email version
	#[serde(
		default,
		rename = "emailVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub email_version: Option<u32>,
	/// Push notification version
	#[serde(
		default,
		rename = "pushVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub push_version: Option<u32>,
	/// Push notification type: 1
	#[serde(default, rename = "pushType", skip_serializing_if = "Option::is_none")]
	pub push_type: Option<u32>,
	/// Support audio alarm
	#[serde(
		default,
		rename = "audioAlarm",
		skip_serializing_if = "Option::is_none"
	)]
	pub audio_alarm: Option<u32>,
	/// Support AP
	#[serde(default, rename = "apMode", skip_serializing_if = "Option::is_none")]
	pub ap_mode: Option<u32>,
	/// Could version
	#[serde(
		default,
		rename = "cloudVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub cloud_version: Option<u32>,
	/// Replay version
	#[serde(
		default,
		rename = "replayVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub replay_version: Option<u32>,
	/// mobComVersion
	#[serde(
		default,
		rename = "mobComVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub mob_com_version: Option<u32>,
	/// Export images
	#[serde(
		default,
		rename = "ExportImport",
		skip_serializing_if = "Option::is_none"
	)]
	pub export_import: Option<u32>,
	/// Language version
	#[serde(
		default,
		rename = "languageVer",
		skip_serializing_if = "Option::is_none"
	)]
	pub language_ver: Option<u32>,
	/// Video standard
	#[serde(
		default,
		rename = "videoStandard",
		skip_serializing_if = "Option::is_none"
	)]
	pub video_standard: Option<u32>,
	/// Support sync time
	#[serde(default, rename = "syncTime", skip_serializing_if = "Option::is_none")]
	pub sync_time: Option<u32>,
	/// Support net port
	#[serde(default, rename = "netPort", skip_serializing_if = "Option::is_none")]
	pub net_port: Option<u32>,
	/// NAS version
	#[serde(
		default,
		rename = "nasVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub nas_version: Option<u32>,
	/// Reboot required
	#[serde(
		default,
		rename = "needReboot",
		skip_serializing_if = "Option::is_none"
	)]
	pub need_reboot: Option<u32>,
	/// Support reboot
	#[serde(skip_serializing_if = "Option::is_none")]
	pub reboot: Option<u32>,
	/// Support Audio config
	#[serde(default, rename = "audioCfg", skip_serializing_if = "Option::is_none")]
	pub audio_cfg: Option<u32>,
	/// Support network diagnosis
	#[serde(
		default,
		rename = "networkDiagnosis",
		skip_serializing_if = "Option::is_none"
	)]
	pub network_diagnosis: Option<u32>,
	/// Support height adjustment
	#[serde(
		default,
		rename = "heightDiffAdjust",
		skip_serializing_if = "Option::is_none"
	)]
	pub height_diff_adjust: Option<u32>,
	/// Support upgrade
	#[serde(skip_serializing_if = "Option::is_none")]
	pub upgrade: Option<u32>,
	/// Support GPS
	#[serde(skip_serializing_if = "Option::is_none")]
	pub gps: Option<u32>,
	/// Support power save config
	#[serde(
		default,
		rename = "powerSavingCfg",
		skip_serializing_if = "Option::is_none"
	)]
	pub power_saving_cfg: Option<u32>,
	/// Login Locked
	#[serde(
		default,
		rename = "loginLocked",
		skip_serializing_if = "Option::is_none"
	)]
	pub login_locked: Option<u32>,
	/// View plan
	#[serde(default, rename = "viewPlan", skip_serializing_if = "Option::is_none")]
	pub view_plan: Option<u32>,
	/// Preview replay limit
	#[serde(
		default,
		rename = "previewReplayLimit",
		skip_serializing_if = "Option::is_none"
	)]
	pub preview_replay_limit: Option<u32>,
	/// IOT link
	#[serde(default, rename = "IOTLink", skip_serializing_if = "Option::is_none")]
	pub iot_link: Option<u32>,
	/// IOT link maximum actions
	#[serde(
		default,
		rename = "IOTLinkActionMax",
		skip_serializing_if = "Option::is_none"
	)]
	pub iot_link_action_max: Option<u32>,
	/// Support record config
	#[serde(default, rename = "recordCfg", skip_serializing_if = "Option::is_none")]
	pub record_cfg: Option<u32>,
	/// Has large battery
	#[serde(
		default,
		rename = "largeBattery",
		skip_serializing_if = "Option::is_none"
	)]
	pub large_battery: Option<u32>,
	/// Smart home config
	#[serde(default, rename = "smartHome", skip_serializing_if = "Option::is_none")]
	pub smart_home: Option<SmartHome>,
	/// Support config for specific channels
	#[serde(default, rename = "item")]
	pub items: Vec<SupportItem>,
}

/// List of smart home items
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct SmartHome {
	/// Versionm
	#[serde(default)]
	pub version: u32,
	/// The smarthome items
	#[serde(default, rename = "item")]
	pub items: Vec<SmartHomeItem>,
}

/// Smart home items, are name:version pairs
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct SmartHomeItem {
	/// Name of item: Option<"googleHome">, "amazonAlexa"
	#[serde(default)]
	pub name: String,
	/// Version of item: 1
	#[serde(default)]
	pub ver: u32,
}

/// Support Items for an individual channel
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct SupportItem {
	/// Channel ID of the item
	#[serde(default, rename = "chnID")]
	pub chn_id: u32,
	/// PTZ type of the channel
	#[serde(default, rename = "ptzType", skip_serializing_if = "Option::is_none")]
	pub ptz_type: Option<u32>,
	/// RF config
	#[serde(default, rename = "rfCfg", skip_serializing_if = "Option::is_none")]
	pub rf_cfg: Option<u32>,
	/// Support audio
	#[serde(default, rename = "noAudio", skip_serializing_if = "Option::is_none")]
	pub no_audio: Option<u32>,
	/// Support auto focus
	#[serde(default, rename = "autoFocus", skip_serializing_if = "Option::is_none")]
	pub auto_focus: Option<u32>,
	/// Support video clip
	#[serde(default, rename = "videoClip", skip_serializing_if = "Option::is_none")]
	pub video_clip: Option<u32>,
	/// Has battery
	#[serde(skip_serializing_if = "Option::is_none")]
	pub battery: Option<u32>,
	/// ISP config
	#[serde(default, rename = "ispCfg", skip_serializing_if = "Option::is_none")]
	pub isp_cfg: Option<u32>,
	/// OSD config
	#[serde(default, rename = "osdCfg", skip_serializing_if = "Option::is_none")]
	pub osd_cfg: Option<u32>,
	/// Support battery analysis
	#[serde(
		default,
		rename = "batAnalysis",
		skip_serializing_if = "Option::is_none"
	)]
	pub bat_analysis: Option<u32>,
	/// Supports dynamic resolution
	#[serde(
		default,
		rename = "dynamicReso",
		skip_serializing_if = "Option::is_none"
	)]
	pub dynamic_reso: Option<u32>,
	/// Audio version
	#[serde(
		default,
		rename = "audioVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub audio_version: Option<u32>,
	/// Supports LED control
	#[serde(default, rename = "ledCtrl", skip_serializing_if = "Option::is_none")]
	pub led_ctrl: Option<u32>,
	/// Supports PTZ Control
	#[serde(
		default,
		rename = "ptzControl",
		skip_serializing_if = "Option::is_none"
	)]
	pub ptz_control: Option<u32>,
	/// Supports new ISP config
	#[serde(default, rename = "newIspCfg", skip_serializing_if = "Option::is_none")]
	pub new_isp_cfg: Option<u32>,
	/// Supports PTZ presets
	#[serde(default, rename = "ptzPreset", skip_serializing_if = "Option::is_none")]
	pub ptz_preset: Option<u32>,
	/// Supports PTZ patrol
	#[serde(default, rename = "ptzPatrol", skip_serializing_if = "Option::is_none")]
	pub ptz_patrol: Option<u32>,
	/// Supports PTZ Tattern
	#[serde(
		default,
		rename = "ptzTattern",
		skip_serializing_if = "Option::is_none"
	)]
	pub ptz_tattern: Option<u32>,
	/// Supports Auto PT
	#[serde(default, rename = "autoPt", skip_serializing_if = "Option::is_none")]
	pub auto_pt: Option<u32>,
	/// H264 Profile: 7
	#[serde(
		default,
		rename = "h264Profile",
		skip_serializing_if = "Option::is_none"
	)]
	pub h264_profile: Option<u32>,
	/// Supports motion alarm
	#[serde(skip_serializing_if = "Option::is_none")]
	pub motion: Option<u32>,
	/// AI Type
	#[serde(default, rename = "aitype", skip_serializing_if = "Option::is_none")]
	pub ai_type: Option<u32>,
	/// Animal AI Type
	#[serde(
		default,
		rename = "aiAnimalType",
		skip_serializing_if = "Option::is_none"
	)]
	pub ai_animal_type: Option<u32>,
	/// Supports time lapse
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timelapse: Option<u32>,
	/// Supports snap
	#[serde(skip_serializing_if = "Option::is_none")]
	pub snap: Option<u32>,
	/// Supports encoding control
	#[serde(default, rename = "encCtrl", skip_serializing_if = "Option::is_none")]
	pub enc_ctrl: Option<u32>,
	/// Has Zoom focus backlash
	#[serde(
		default,
		rename = "zfBacklash",
		skip_serializing_if = "Option::is_none"
	)]
	pub zf_backlash: Option<u32>,
	/// Supports IOT Link Ability
	#[serde(
		default,
		rename = "IOTLinkAbility",
		skip_serializing_if = "Option::is_none"
	)]
	pub iot_link_ability: Option<u32>,
	/// Supports IPC audio talk
	#[serde(
		default,
		rename = "ipcAudioTalk",
		skip_serializing_if = "Option::is_none"
	)]
	pub ipc_audio_talk: Option<u32>,
	/// Supports Bino Config
	#[serde(default, rename = "binoCfg", skip_serializing_if = "Option::is_none")]
	pub bino_cfg: Option<u32>,
	/// Supports thumbnail
	#[serde(skip_serializing_if = "Option::is_none")]
	pub thumbnail: Option<u32>,
}

/// Instruct camera to play an audio alarm, usually this is the siren
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AudioPlayInfo {
	/// Channel ID
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// Playmode: 0
	#[serde(default, rename = "playMode")]
	pub play_mode: u32,
	/// Duration: 0
	#[serde(default, rename = "playDuration")]
	pub play_duration: u32,
	/// Times to play: 1
	#[serde(default, rename = "playTimes")]
	pub play_times: u32,
	/// On or Off: 0
	#[serde(default, rename = "onOff")]
	pub on_off: u32,
}

/// Server port for baichaun defaults 9000
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct ServerPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "serverPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Server port for http defaults to 80
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct HttpPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "httpPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Server port for https defaults to 443
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct HttpsPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "httpsPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Server port for Rtsp defaults to 554
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct RtspPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "rtspPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Server port for Rtmp defaults to 1935
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct RtmpPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "rtmpPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`, can be `None` on cameras that can't change it
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Server port for Onvif defaults to 8000
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct OnvifPort {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The port number
	#[serde(default, rename = "onvifPort")]
	pub port: u32,
	/// The enable status known values are `1`, `0`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable: Option<u32>,
}

/// Email settings for notificaitons
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct Email {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// SMTP server address
	#[serde(default, rename = "smtpServer")]
	pub smtp_server: String,
	/// SMTP username
	#[serde(default, rename = "userName")]
	pub user_name: String,
	/// SMTP password
	#[serde(default)]
	pub password: String,
	/// Send email address
	#[serde(default)]
	pub address1: String,
	/// Send email address can be empty
	#[serde(default)]
	pub address2: String,
	/// Send email address can be empty
	#[serde(default)]
	pub address3: String,
	/// 465
	#[serde(default, rename = "smtpPort")]
	pub smtp_port: u16,
	/// Name of recipient to use on the email
	#[serde(default, rename = "sendNickname")]
	pub send_nickname: String,
	/// Observed value `1`
	#[serde(default)]
	pub attachment: u8,
	/// Observed value `picture`, `video`
	#[serde(
		default,
		rename = "attachmentType",
		skip_serializing_if = "Option::is_none"
	)]
	pub attachment_type: Option<String>,
	/// Observed value `withText`
	#[serde(default, rename = "textType")]
	pub text_type: String,
	/// Observed value `1`
	#[serde(default)]
	pub ssl: u8,
	/// Observed value `30`
	#[serde(default)]
	pub interval: u32,
	/// Max length of message. Observed value `127`
	///   Read Only
	#[serde(
		default,
		rename = "senderMaxLen",
		skip_serializing_if = "Option::is_none"
	)]
	pub sender_max_len: Option<u32>,
}

/// EmailTask settings that controls the times/enables the email
/// notifications
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct EmailTask {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel number
	#[serde(default, rename = "channelId")]
	pub channel_id: u8,
	/// 1 for enable 0 for disable
	#[serde(default, rename = "enable")]
	pub enable: u8,
	/// The list of schedule to turn on/off the email notifications
	#[serde(
		default,
		rename = "ScheduleList",
		skip_serializing_if = "Option::is_none"
	)]
	pub schedule_list: Option<ScheduleList>,
}

/// List of schedule items for turning on/off the notifications
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct ScheduleList {
	/// List of schedules
	#[serde(default, rename = "Schedule")]
	pub schedule: Schedule,
}

/// Schedule item for turning on/off the notifications
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct Schedule {
	/// The alarm type. Observed values: `MD`
	#[serde(default, rename = "alarmType")]
	pub alarm_type: String,
	/// The list of time blocks
	#[serde(default, rename = "timeBlockList")]
	pub time_block_list: TimeBlockList,
}

/// List of users
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct UserList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The actual user-list
	#[serde(default, rename = "User", skip_serializing_if = "Option::is_none")]
	pub user_list: Option<Vec<User>>,
}

/// A struct for reading and writing camera user records
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct User {
	/// The user_name is used to identify the user in the API
	#[serde(default, rename = "userName")]
	pub user_name: String,
	/// The password seems to only be included when creating or modifying a user
	#[serde(
		rename = "password",
		skip_serializing_if = "Option::is_none",
		skip_deserializing
	)]
	pub password: Option<String>,
	/// The user_id does not seem to have a purpose. It is not included when creating a user.
	#[serde(default, rename = "userId", skip_serializing_if = "Option::is_none")]
	pub user_id: Option<u32>,
	/// User type, 0 is User and 1 is Administrator
	#[serde(default, rename = "userLevel")]
	pub user_level: u8,
	/// Unknown, seems to be 1 for the current API user
	#[serde(
		default,
		rename = "loginState",
		skip_serializing_if = "Option::is_none"
	)]
	pub login_state: Option<u8>,
	/// The user_set_state states what will happen with a user-record. 4 different values have been
	/// observed: none | add | delete | modify
	///
	/// | Value  | Description                                                                                                        |
	/// | ---    | ---                                                                                                                |
	/// | none   | This is the state set when reading Users. When writing this seems to indicate that the user should not be modified |
	/// | add    | Indicates that a new User should be created                                                                        |
	/// | delete | Indicates that the user should be removed                                                                          |
	/// | modify | Indicates that the user should be modified. It seems like only the password can be changed.                        |
	#[serde(default, rename = "userSetState")]
	pub user_set_state: String,
}

/// Convience function to return the xml version used throughout the library
pub fn xml_ver() -> String {
	"1.1".to_string()
}

#[test]
fn test_encryption_deser() {
	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <Encryption version="1.1">
        <type>md5</type>
        <nonce>9E6D1FCB9E69846D</nonce>
        </Encryption>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let enc = b.encryption.as_ref().unwrap();

	assert_eq!(enc.version, "1.1");
	assert_eq!(enc.nonce, "9E6D1FCB9E69846D");
	assert_eq!(enc.type_, "md5");

	let t = BcXml::try_parse(sample.as_bytes()).unwrap();
	match t {
		top_b if top_b == b => {}
		_ => panic!(),
	}
}

#[test]
fn test_login_deser() {
	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <LoginUser version="1.1">
        <userName>9F07915E819A076E2E14169830769D6</userName>
        <password>8EFECD610524A98390F118D2789BE3B</password>
        <userVer>1</userVer>
        </LoginUser>
        <LoginNet version="1.1">
        <type>LAN</type>
        <udpPort>0</udpPort>
        </LoginNet>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let login_user = b.login_user.unwrap();
	let login_net = b.login_net.unwrap();

	assert_eq!(login_user.version, "1.1");
	assert_eq!(login_user.user_name, "9F07915E819A076E2E14169830769D6");
	assert_eq!(login_user.password, "8EFECD610524A98390F118D2789BE3B");
	assert_eq!(login_user.user_ver, 1);

	assert_eq!(login_net.version, "1.1");
	assert_eq!(login_net.type_, "LAN");
	assert_eq!(login_net.udp_port, 0);
}

#[test]
fn test_login_ser() {
	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <LoginUser version="1.1">
        <userName>9F07915E819A076E2E14169830769D6</userName>
        <password>8EFECD610524A98390F118D2789BE3B</password>
        <userVer>1</userVer>
        </LoginUser>
        <LoginNet version="1.1">
        <type>LAN</type>
        <udpPort>0</udpPort>
        </LoginNet>
        </body>"#
	);

	let b = BcXml {
		login_user: Some(LoginUser {
			version: "1.1".to_string(),
			user_name: "9F07915E819A076E2E14169830769D6".to_string(),
			password: "8EFECD610524A98390F118D2789BE3B".to_string(),
			user_ver: 1,
			..Default::default()
		}),
		login_net: Some(LoginNet {
			version: "1.1".to_string(),
			type_: "LAN".to_string(),
			udp_port: 0,
		}),
		..BcXml::default()
	};

	let b2 = BcXml::try_parse(sample.as_bytes()).unwrap();
	let b3 = BcXml::try_parse(b.serialize(vec![]).unwrap().as_ref()).unwrap();
	assert_eq!(b, b2);
	assert_eq!(b, b3);
	assert_eq!(b2, b3);
}

#[test]
fn test_deviceinfo_partial_deser() {
	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <DeviceInfo version="1.1">
        <firmVersion>00000000000000</firmVersion>
        <IOInputPortNum>0</IOInputPortNum>
        <IOOutputPortNum>0</IOOutputPortNum>
        <diskNum>0</diskNum>
        <type>ipc</type>
        <channelNum>1</channelNum>
        <audioNum>1</audioNum>
        <ipChannel>0</ipChannel>
        <analogChnNum>1</analogChnNum>
        <resolution>
        <resolutionName>3840*2160</resolutionName>
        <width>3840</width>
        <height>2160</height>
        </resolution>
        <language>English</language>
        <sdCard>0</sdCard>
        <ptzMode>none</ptzMode>
        <typeInfo>IPC</typeInfo>
        <softVer>33554880</softVer>
        <hardVer>0</hardVer>
        <panelVer>0</panelVer>
        <hdChannel1>0</hdChannel1>
        <hdChannel2>0</hdChannel2>
        <hdChannel3>0</hdChannel3>
        <hdChannel4>0</hdChannel4>
        <norm>NTSC</norm>
        <osdFormat>YMD</osdFormat>
        <B485>0</B485>
        <supportAutoUpdate>0</supportAutoUpdate>
        <userVer>1</userVer>
        </DeviceInfo>
        <StreamInfoList version="1.1">
        <StreamInfo>
        <channelBits>1</channelBits>
        <encodeTable>
        <type>mainStream</type>
        <resolution>
        <width>3840</width>
        <height>2160</height>
        </resolution>
        <defaultFramerate>20</defaultFramerate>
        <defaultBitrate>6144</defaultBitrate>
        <framerateTable>20,18,16,15,12,10,8,6,4,2</framerateTable>
        <bitrateTable>4096,5120,6144,7168,8192</bitrateTable>
        </encodeTable>
        <encodeTable>
        <type>subStream</type>
        <resolution>
        <width>640</width>
        <height>360</height>
        </resolution>
        <defaultFramerate>7</defaultFramerate>
        <defaultBitrate>160</defaultBitrate>
        <framerateTable>15,10,7,4</framerateTable>
        <bitrateTable>64,128,160,192,256,384,512</bitrateTable>
        </encodeTable>
        </StreamInfo>
        <StreamInfo>
        <channelBits>1</channelBits>
        <encodeTable>
        <type>mainStream</type>
        <resolution>
        <width>2560</width>
        <height>1440</height>
        </resolution>
        <defaultFramerate>25</defaultFramerate>
        <defaultBitrate>0</defaultBitrate>
        <framerateTable>25,22,20,18,16,15,12,10,8,6,4,2</framerateTable>
        <bitrateTable>1024,1536,2048,3072,4096,5120,6144,7168,8192</bitrateTable>
        </encodeTable>
        <encodeTable>
        <type>subStream</type>
        <resolution>
        <width>640</width>
        <height>360</height>
        </resolution>
        <defaultFramerate>7</defaultFramerate>
        <defaultBitrate>160</defaultBitrate>
        <framerateTable>15,10,7,4</framerateTable>
        <bitrateTable>64,128,160,192,256,384,512</bitrateTable>
        </encodeTable>
        </StreamInfo>
        <StreamInfo>
        <channelBits>1</channelBits>
        <encodeTable>
        <type>mainStream</type>
        <resolution>
        <width>2304</width>
        <height>1296</height>
        </resolution>
        <defaultFramerate>25</defaultFramerate>
        <defaultBitrate>0</defaultBitrate>
        <framerateTable>25,22,20,18,16,15,12,10,8,6,4,2</framerateTable>
        <bitrateTable>1024,1536,2048,3072,4096,5120,6144,7168,8192</bitrateTable>
        </encodeTable>
        <encodeTable>
        <type>subStream</type>
        <resolution>
        <width>640</width>
        <height>360</height>
        </resolution>
        <defaultFramerate>7</defaultFramerate>
        <defaultBitrate>160</defaultBitrate>
        <framerateTable>15,10,7,4</framerateTable>
        <bitrateTable>64,128,160,192,256,384,512</bitrateTable>
        </encodeTable>
        </StreamInfo>
        </StreamInfoList>
        </body>
"#
	);

	let b = BcXml::try_parse(sample.as_bytes()).unwrap();
	match b {
		BcXml {
			device_info:
				Some(DeviceInfo {
					resolution:
						Some(Resolution {
							width: 3840,
							height: 2160,
							..
						}),
					..
				}),
			..
		} => {}
		_ => panic!(),
	}
}

#[test]
fn test_binary_deser() {
	let _ = env_logger::builder().is_test(true).try_init();

	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <Extension version="1.1">
        <binaryData>1</binaryData>
        </Extension>
    "#
	);
	let b = Extension::try_parse(sample.as_bytes()).unwrap();
	match b {
		Extension {
			binary_data: Some(1),
			..
		} => {}
		_ => panic!(),
	}
}

#[test]
fn test_enc3_extension() {
	let _ = env_logger::builder().is_test(true).try_init();
	let sample = indoc!(
		r#"<?xml version="1.0\" encoding="UTF-8" ?>
        <Extension version="1.1">
        <encryptLen>1024</encryptLen>
        <binaryData>1</binaryData>
        <checkPos>0</checkPos>
        <checkValue>1667510320</checkValue>
        </Extension>
        "#
	);
	let b = Extension::try_parse(sample.as_bytes()).unwrap();
	match b {
		Extension {
			encrypt_len: Some(1024),
			binary_data: Some(1),
			check_pos: Some(0),
			check_value: Some(1667510320),
			..
		} => {}
		_ => panic!(),
	}

	let sample = indoc!(
		r#"
    <?xml version="1.0" encoding="UTF-8" ?>
    <Extension version="1.1">
    <checkPos>0</checkPos>
    <checkValue>-1211658</checkValue>
    </Extension>
    "#
	);
	let b = Extension::try_parse(sample.as_bytes()).unwrap();
	match b {
		Extension {
			check_pos: Some(0),
			check_value: Some(-1211658),
			..
		} => {}
		_ => panic!(),
	}

	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <Extension version="1.1">
        <checkPos>0</checkPos>
        <checkValue>-1821213800</checkValue>
        </Extension>
        "#
	);
	let b = Extension::try_parse(sample.as_bytes()).unwrap();
	match b {
		Extension {
			check_pos: Some(0),
			check_value: Some(-1821213800),
			..
		} => {}
		_ => panic!(),
	}
}

#[test]
fn test_empty_floodlight_status_list() {
	let _ = env_logger::builder().is_test(true).try_init();
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <FloodlightStatusList version="1.1" />
        </body>
        "#
	);
	let b = BcXml::try_parse(sample.as_bytes()).unwrap();
	match b {
		BcXml {
			floodlight_status_list:
				Some(FloodlightStatusList {
					version,
					floodlight_status_list,
				}),
			..
		} if version == "1.1" && floodlight_status_list.is_empty() => {}
		_ => panic!(),
	}
}

#[test]
fn test_rf_alarm_cfg_v11_without_rfid() {
	// Real response from Reolink battery camera firmware v1.1.
	// Does NOT include rfID, sensitivity, timeBlockList, alarmHandle.
	// Includes interval fields not present in older firmware.
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <rfAlarmCfg version="1.1">
        <enable>1</enable>
        <sensiValue>11</sensiValue>
        <reduceFalseAlarm>0</reduceFalseAlarm>
        <interval>0</interval>
        <maxAlarmTime>6320216</maxAlarmTime>
        <intervalUseRange>1</intervalUseRange>
        <intervalSecMin>5</intervalSecMin>
        <intervalSecMax>120</intervalSecMax>
        </rfAlarmCfg>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let cfg = b.rf_alarm_cfg.as_ref().unwrap();

	assert_eq!(cfg.version, "1.1");
	assert_eq!(cfg.enable, 1);
	assert_eq!(cfg.rf_id, None);
	assert_eq!(cfg.sensitivity, None);
	assert_eq!(cfg.sensiValue, Some(11));
	assert_eq!(cfg.reduceFalseAlarm, Some(0));
	assert_eq!(cfg.interval, Some(0));
	assert_eq!(cfg.maxAlarmTime, Some(6320216));
	assert_eq!(cfg.intervalUseRange, Some(1));
	assert_eq!(cfg.intervalSecMin, Some(5));
	assert_eq!(cfg.intervalSecMax, Some(120));
	assert!(cfg.time_block_list.is_none());
	assert!(cfg.alarm_handle.is_none());
}

#[test]
fn test_ptz_preset_missing_command_field() {
	// Real response captured from Reolink Altas PT Ultra
	// firmware v3.0.0.5649_25111355: the camera omits
	// <command> elements inside <preset> entries. Prior to the
	// fix this failed with `missing field "command"` and the
	// whole PtzPreset parse aborted. Assert we now tolerate it.
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <PtzPreset version="1.1">
        <channelId>0</channelId>
        <presetList>
        <preset>
        <id>0</id>
        <name>Default</name>
        </preset>
        <preset>
        <id>1</id>
        <name>Sky</name>
        </preset>
        <preset>
        <id>2</id>
        <name>Left</name>
        </preset>
        <preset>
        <id>3</id>
        <name>DefaultUp</name>
        </preset>
        </presetList>
        </PtzPreset>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let ptz_preset = b.ptz_preset.as_ref().unwrap();

	assert_eq!(ptz_preset.channel_id, 0);
	assert_eq!(ptz_preset.preset_list.preset.len(), 4);

	let expected = [
		(0u8, "Default"),
		(1u8, "Sky"),
		(2u8, "Left"),
		(3u8, "DefaultUp"),
	];
	for (preset, (expected_id, expected_name)) in
		ptz_preset.preset_list.preset.iter().zip(expected.iter())
	{
		assert_eq!(preset.id, *expected_id);
		assert_eq!(preset.name.as_deref(), Some(*expected_name));
		// The camera omitted <command>; serde(default) must
		// give us an empty string rather than erroring out.
		assert_eq!(preset.command, "");
	}
}

#[test]
fn test_version_info_minimal_deser() {
	// Verify that VersionInfo tolerates missing primitive
	// fields. Newer firmware has been observed to omit several
	// optional-looking string fields; the default for String is
	// empty, which is safe — callers treat these as opaque.
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <VersionInfo version="1.1">
        <firmwareVersion>v3.0.0.5649_25111355</firmwareVersion>
        </VersionInfo>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let vi = b.version_info.as_ref().unwrap();

	assert_eq!(vi.firmwareVersion, "v3.0.0.5649_25111355");
	// All the other mandatory-looking fields default to empty.
	assert_eq!(vi.name, "");
	assert_eq!(vi.serialNumber, "");
	assert_eq!(vi.buildDay, "");
	assert_eq!(vi.hardwareVersion, "");
	assert_eq!(vi.cfgVersion, "");
	assert_eq!(vi.detail, "");
}

#[test]
fn test_battery_info_missing_fields() {
	// Assert BatteryInfo tolerates a subset of the documented
	// fields — firmware variants sometimes omit low_power,
	// battery_version, temperature, etc.
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <BatteryInfo>
        <channelId>0</channelId>
        <batteryPercent>87</batteryPercent>
        <chargeStatus>none</chargeStatus>
        </BatteryInfo>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let bi = b.battery_info.as_ref().unwrap();

	assert_eq!(bi.channel_id, 0);
	assert_eq!(bi.battery_percent, 87);
	assert_eq!(bi.charge_status, "none");
	assert_eq!(bi.adapter_status, "");
	assert_eq!(bi.voltage, 0);
	assert_eq!(bi.current, 0);
	assert_eq!(bi.temperature, 0);
	assert_eq!(bi.low_power, 0);
	assert_eq!(bi.battery_version, 0);
}

#[test]
fn test_alarm_event_minimal() {
	// Some firmware has been observed to emit AlarmEvent
	// without the timeStamp / recording fields. Assert the
	// hardened defaults kick in.
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <AlarmEventList version="1.1">
        <AlarmEvent version="1.1">
        <channelId>0</channelId>
        <status>MD</status>
        </AlarmEvent>
        </AlarmEventList>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let evs = &b.alarm_event_list.as_ref().unwrap().alarm_events;
	assert_eq!(evs.len(), 1);
	assert_eq!(evs[0].channel_id, 0);
	assert_eq!(evs[0].status, "MD");
	assert_eq!(evs[0].recording, 0);
	assert_eq!(evs[0].timeStamp, 0);
	assert!(evs[0].ai_type.is_none());
}

#[test]
fn test_rf_alarm_cfg_v11_round_trip() {
	// Verify that parsing then serializing preserves the data
	// (important for pir_set which does get → modify → set)
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <rfAlarmCfg version="1.1">
        <enable>1</enable>
        <sensiValue>11</sensiValue>
        <reduceFalseAlarm>0</reduceFalseAlarm>
        <interval>0</interval>
        <maxAlarmTime>6320216</maxAlarmTime>
        <intervalUseRange>1</intervalUseRange>
        <intervalSecMin>5</intervalSecMin>
        <intervalSecMax>120</intervalSecMax>
        </rfAlarmCfg>
        </body>"#
	);
	let b: BcXml = quick_xml::de::from_str(sample).unwrap();
	let cfg = b.rf_alarm_cfg.as_ref().unwrap();

	// Modify enable (simulates pir_set)
	let mut modified = cfg.clone();
	modified.enable = 0;

	// Serialize back
	let xml_str = quick_xml::se::to_string(&modified).unwrap();

	// Should contain the modified enable and preserved fields
	assert!(xml_str.contains("<enable>0</enable>"));
	assert!(xml_str.contains("<sensiValue>11</sensiValue>"));
	assert!(xml_str.contains("<maxAlarmTime>6320216</maxAlarmTime>"));
	// Should NOT contain fields the camera didn't send
	assert!(!xml_str.contains("rfID"));
	assert!(!xml_str.contains("timeBlockList"));
	assert!(!xml_str.contains("alarmHandle"));
}

#[test]
fn login_user_debug_redacts_password() {
	let user = LoginUser {
		version: "1.1".to_string(),
		user_name: "admin".to_string(),
		password: "hunter2".to_string(),
		user_ver: 1,
		..Default::default()
	};
	let dbg = format!("{user:?}");
	assert!(
		!dbg.contains("hunter2"),
		"LoginUser Debug must not include the plaintext password; got {dbg}"
	);
	assert!(dbg.contains("admin"), "username should still surface");
	assert!(
		dbg.contains("redacted"),
		"redaction marker should surface; got {dbg}"
	);

	// And the wrapping `BcXml` must not leak through its
	// auto-derived Debug — the field-level redaction is what
	// closes that hole.
	let body = BcXml {
		login_user: Some(user),
		..Default::default()
	};
	let dbg = format!("{body:?}");
	assert!(
		!dbg.contains("hunter2"),
		"BcXml Debug must inherit LoginUser's redaction; got {dbg}"
	);
}

#[test]
fn login_user_debug_marks_empty_password_distinctly() {
	let user = LoginUser {
		version: "1.1".to_string(),
		user_name: "admin".to_string(),
		password: String::new(),
		user_ver: 1,
		..Default::default()
	};
	let dbg = format!("{user:?}");
	assert!(
		dbg.contains("empty"),
		"empty password should surface as <empty>, not <redacted>; got {dbg}"
	);
}

#[test]
fn login_user_sigv3_serializes_extra_fields() {
	// The sigV3 login adds clientType / publicKey / cipherContent on the
	// wire; field names must match what the camera expects.
	let sigv3 = BcXml {
		login_user: Some(LoginUser {
			version: "1.1".to_string(),
			user_name: "md5user".to_string(),
			password: "md5pass".to_string(),
			user_ver: 1,
			client_type: Some("app".to_string()),
			public_key: Some("PUBKEYB64".to_string()),
			cipher_content: Some("CIPHERB64".to_string()),
			..Default::default()
		}),
		..Default::default()
	};
	let xml = String::from_utf8(sigv3.serialize(vec![]).unwrap()).unwrap();
	assert!(xml.contains("<clientType>app</clientType>"), "{xml}");
	assert!(xml.contains("<publicKey>PUBKEYB64</publicKey>"), "{xml}");
	assert!(
		xml.contains("<cipherContent>CIPHERB64</cipherContent>"),
		"{xml}"
	);

	// Legacy login (no sigV3 fields set) must omit them entirely so the
	// old-firmware path is byte-for-byte unchanged.
	let legacy = BcXml {
		login_user: Some(LoginUser {
			version: "1.1".to_string(),
			user_name: "md5user".to_string(),
			password: "md5pass".to_string(),
			user_ver: 1,
			..Default::default()
		}),
		..Default::default()
	};
	let xml = String::from_utf8(legacy.serialize(vec![]).unwrap()).unwrap();
	assert!(
		!xml.contains("publicKey"),
		"legacy must omit sigV3 fields: {xml}"
	);
	assert!(!xml.contains("cipherContent"), "{xml}");
	assert!(!xml.contains("clientType"), "{xml}");
}

#[test]
fn certchain_newlines_emitted_as_entities_on_the_wire() {
	let login = BcXml {
		login_user: Some(LoginUser {
			version: "1.1".to_string(),
			user_name: "u".to_string(),
			cert_chain: Some("-----BEGIN-----\nAAAA\n-----END-----\n".to_string()),
			..Default::default()
		}),
		..Default::default()
	};
	let xml = String::from_utf8(login.serialize(vec![]).unwrap()).unwrap();
	assert!(
		xml.contains("<certChain>-----BEGIN-----&#x0A;AAAA&#x0A;-----END-----&#x0A;</certChain>"),
		"{xml}"
	);
	let inner = &xml[xml.find("<certChain>").unwrap()..xml.find("</certChain>").unwrap()];
	assert!(
		!inner.contains('\n'),
		"no raw newline survives in certChain"
	);
}

#[test]
fn encode_certchain_newlines_is_scoped() {
	// No <certChain> -> buffer unchanged (no global newline rewrite).
	assert_eq!(
		encode_certchain_newlines(b"<foo>a\nb</foo>").as_slice(),
		b"<foo>a\nb</foo>"
	);
	// Only the certChain span is rewritten; surrounding newlines are untouched.
	assert_eq!(
		encode_certchain_newlines(b"x\n<certChain>a\nb\r</certChain>\ny").as_slice(),
		b"x\n<certChain>a&#x0A;b&#x0D;</certChain>\ny".as_slice()
	);
}
