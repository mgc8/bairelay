#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};
use std::io::Write;

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
	/// HTTP webhook configuration carried by `MSG_ID_GET_HA_CFG` /
	/// `MSG_ID_SET_HA_CFG`
	#[serde(default, rename = "HaCfg", skip_serializing_if = "Option::is_none")]
	pub ha_cfg: Option<HaCfg>,
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
	/// The loudspeaker / intercom volume block, **PascalCase spelling**.
	///
	/// Read with msg 264, written back with msg 265 — see
	/// [`BcCamera::get_audio_cfg`](crate::bc_protocol::BcCamera::get_audio_cfg).
	/// This is the only place the doorbell's intercom volume
	/// (`talkAndReplyVolume`) lives.
	///
	/// Cameras of this family have been seen to name the element both
	/// `<AudioCfg>` and `<audioCfg>` (the internal protocol note lists both),
	/// and quick-xml's serde decoding matches element names **exactly**, so
	/// the block is carried by this field *and* by
	/// [`audio_cfg_camel`](Self::audio_cfg_camel) below. Read it through
	/// [`BcXml::audio_cfg_block`](Self::audio_cfg_block) and write it through
	/// [`BcXml::body_with_audio_cfg`](Self::body_with_audio_cfg), which pick the
	/// spelling the camera actually used; a SET under the other spelling is
	/// the one thing this firmware is known to shrug off.
	#[serde(default, rename = "AudioCfg", skip_serializing_if = "Option::is_none")]
	pub audio_cfg: Option<AudioCfg>,
	/// The same block as [`audio_cfg`](Self::audio_cfg), for a camera that
	/// spells the element `<audioCfg>`. Never set together with it.
	#[serde(default, rename = "audioCfg", skip_serializing_if = "Option::is_none")]
	pub audio_cfg_camel: Option<AudioCfg>,
	/// The hardwired-chime control block, msg 483 (`machineStateGet` in,
	/// `machineStateSet` back) — see
	/// [`BcCamera::get_ding_dong_cfg`](crate::bc_protocol::BcCamera::get_ding_dong_cfg).
	/// One carrier spelling only: unlike `<audioCfg>`, no second casing has
	/// ever been observed or documented for this element.
	#[serde(default, rename = "dingdongCtrl", skip_serializing_if = "Option::is_none")]
	pub dingdong_ctrl: Option<DingDongCtrl>,
	/// The paired-chime list a msg 484 reply carries: one entry per chime.
	/// Blocks whose id is missing or non-numeric are ignored by the reader
	/// (see [`DingDongDevice::identifier`]), matching how the upstream
	/// reference implementation walks the list.
	#[serde(
		default,
		rename = "dingdongDeviceInfo",
		skip_serializing_if = "Vec::is_empty"
	)]
	pub dingdong_device_info: Vec<DingDongDevice>,
	/// The **other** documented spelling of the very same list: msg 484 is
	/// double-use, reply *and* device push, and the pushed variant is rooted
	/// `<dingdongList>` — the reference client lists it as the "pushed variant"
	/// of the 484 reply.
	///
	/// Read-only alias, never sent: the bridge asks 484 with an empty body and
	/// has no reason to invent a root. **This element is a container, not the
	/// entries** — measured on a `v3.0.0.4636_25060550` doorbell, the reply is
	/// `<dingdongList><maxPairNumber>5</maxPairNumber><pairedList>
	/// <dingdongDeviceInfo>…`. Modelling it as the list itself (what this field
	/// used to be) decodes exactly one entry with every tag absent, which is
	/// how a paired, answering chime was being reported as `chime not found`. That
	/// sentence is this bridge's own text, not a firmware string: it travelled in
	/// french until the rename, and the renaming table in
	/// [`docs/settings.md`](../../../../docs/settings.md) lists what is published
	/// today. [`DingDongList`] holds both nestings a firmware can use.
	#[serde(default, rename = "dingdongList", skip_serializing_if = "Option::is_none")]
	pub dingdong_list: Option<DingDongList>,
	/// The chime device-options carrier, msg 485 — request and `getParam`
	/// reply share this element; which frame it is, is decided by `<opt>`.
	#[serde(
		default,
		rename = "dingdongDeviceOpt",
		skip_serializing_if = "Option::is_none"
	)]
	pub dingdong_device_opt: Option<DingDongDeviceOpt>,
	/// The per-event chime config blocks a msg 486 reply carries, one per
	/// ringable device. Written back one at a time inside a
	/// [`dingdong_cfg`](Self::dingdong_cfg) wrapper by msg 487.
	#[serde(default, rename = "deviceCfg", skip_serializing_if = "Vec::is_empty")]
	pub device_cfg: Vec<DingDongDeviceCfg>,
	/// The msg 487 write wrapper: `<dingdongCfg><deviceCfg>…`.
	#[serde(default, rename = "dingdongCfg", skip_serializing_if = "Option::is_none")]
	pub dingdong_cfg: Option<DingDongCfgReq>,
	/// The chime silent-mode block, msgs 609 (read) / 610 (write). Writing
	/// it re-arms the mute countdown, so a caller that means "change
	/// nothing" must send nothing — see
	/// [`BcCamera::set_ding_dong_cfg`](crate::bc_protocol::BcCamera::set_ding_dong_cfg).
	#[serde(
		default,
		rename = "dingdongSilentMode",
		skip_serializing_if = "Option::is_none"
	)]
	pub dingdong_silent_mode: Option<DingDongSilent>,
	/// The NTP block, msgs 38 (read) / 39 (write).
	#[serde(default, rename = "Ntp", skip_serializing_if = "Option::is_none")]
	pub ntp: Option<Ntp>,
	/// The automatic-reboot schedule, msgs 101 (read) / 100 (write).
	#[serde(default, rename = "AutoReboot", skip_serializing_if = "Option::is_none")]
	pub auto_reboot: Option<AutoReboot>,
	/// The microphone AI-denoiser block, msgs 439 (read) / 440 (write).
	#[serde(default, rename = "aiDenoise", skip_serializing_if = "Option::is_none")]
	pub ai_denoise: Option<AiDenoise>,
	/// The sleep state, msg 574 (read-only).
	#[serde(default, rename = "sleepState", skip_serializing_if = "Option::is_none")]
	pub sleep_state: Option<SleepState>,
	/// The Wi-Fi signal report, msg 115 (read-only).
	#[serde(default, rename = "WifiSignal", skip_serializing_if = "Option::is_none")]
	pub wifi_signal: Option<WifiSignal>,
	/// The image sliders, msg 26 (read) / 25 (write). Arrives in the same
	/// reply as [`input_advance_cfg`](Self::input_advance_cfg); the 25 setter
	/// wants both back.
	#[serde(default, rename = "VideoInput", skip_serializing_if = "Option::is_none")]
	pub video_input: Option<VideoInput>,
	/// The advanced image block, companion of
	/// [`video_input`](Self::video_input).
	#[serde(
		default,
		rename = "InputAdvanceCfg",
		skip_serializing_if = "Option::is_none"
	)]
	pub input_advance_cfg: Option<InputAdvanceCfg>,
	/// The OSD timestamp block, msgs 44 (read) / 45 (write). Arrives in the
	/// same reply as [`osd_channel_name`](Self::osd_channel_name).
	#[serde(default, rename = "OsdDatetime", skip_serializing_if = "Option::is_none")]
	pub osd_datetime: Option<OsdDatetime>,
	/// The OSD channel-name block (and its watermark flag).
	#[serde(
		default,
		rename = "OsdChannelName",
		skip_serializing_if = "Option::is_none"
	)]
	pub osd_channel_name: Option<OsdChannelName>,
	/// The privacy-mask block, msg 52 (read). See [`Shelter`] for why the
	/// 53 setter is refused.
	#[serde(default, rename = "Shelter", skip_serializing_if = "Option::is_none")]
	pub shelter: Option<Shelter>,
	/// The motion-detection block in the setter's shape, msg 46 (read) /
	/// 47 (write). Mutually exclusive with [`md_alarm`](Self::md_alarm):
	/// which one a firmware prints decides whether writing is allowed.
	#[serde(default, rename = "MD", skip_serializing_if = "Option::is_none")]
	pub md: Option<MdCfg>,
	/// The motion-detection block in the getter's shape. Read-only.
	#[serde(default, rename = "MdAlarm", skip_serializing_if = "Option::is_none")]
	pub md_alarm: Option<MdAlarm>,
}

impl BcXml {
	/// Parse a BcXml payload. See [`parse_lossy_utf8`] for the decoding rules.
	pub(crate) fn try_parse(s: &[u8]) -> Result<Self, quick_xml::de::DeError> {
		parse_lossy_utf8(s)
	}
	/// The `<audioCfg>` block, with [`AudioCfg::tag`] set to the spelling the
	/// camera used for the element.
	///
	/// The two carrier fields ([`audio_cfg`](Self::audio_cfg) /
	/// [`audio_cfg_camel`](Self::audio_cfg_camel)) exist only because
	/// quick-xml matches tag names case-sensitively; this folds them back
	/// into one answer so callers never have to know about the pair, and it
	/// stamps the casing onto the returned block — which is what makes
	/// [`BcXml::body_with_audio_cfg`] send the name back that the camera printed.
	/// Reading through anything else is what leaves a block stamped with the
	/// wrong spelling, so read through this.
	pub fn audio_cfg_block(&self) -> Option<AudioCfg> {
		let (cfg, tag) = if let Some(cfg) = self.audio_cfg.as_ref() {
			(cfg, AudioCfgTag::Pascal)
		} else {
			(self.audio_cfg_camel.as_ref()?, AudioCfgTag::Camel)
		};
		Some(AudioCfg { tag, ..cfg.clone() })
	}
	/// A `<body>` carrying `cfg` under the exact spelling it was read with
	/// ([`AudioCfg::tag`]), which is what msg 265 expects: this firmware
	/// answers a SET whose element it doesn't recognise by ignoring it.
	pub fn body_with_audio_cfg(cfg: &AudioCfg) -> Self {
		let mut body = BcXml::default();
		match cfg.tag {
			AudioCfgTag::Pascal => body.audio_cfg = Some(cfg.clone()),
			AudioCfgTag::Camel => body.audio_cfg_camel = Some(cfg.clone()),
		}
		body
	}
	/// The paired wireless chimes, whichever of the shapes the camera used:
	/// `<dingdongDeviceInfo>` at the body root (the documented reply), or the
	/// `<dingdongList>` container — entries inside its `<pairedList>` wrapper,
	/// or printed directly under it (the reference client's note — msg 484 is
	/// reply *and* push).
	///
	/// Folds the carriers exactly the way [`BcXml::audio_cfg_block`] folds the two
	/// `<audioCfg>` casings, so no caller has to know the shapes exist.
	/// Entries come back as read, including ones with no addressable id —
	/// [`DingDongDevice::identifier`] is what decides whether an entry can be
	/// talked to, and nothing here guesses an id.
	pub fn dingdong_chimes(&self) -> &[DingDongDevice] {
		if !self.dingdong_device_info.is_empty() {
			return &self.dingdong_device_info;
		}
		let Some(list) = self.dingdong_list.as_ref() else {
			return &[];
		};
		if list.device_info.is_empty() {
			list.paired_list.as_ref().map(|p| p.device_info.as_slice()).unwrap_or(&[])
		} else {
			&list.device_info
		}
	}
	/// The per-event chime blocks of a msg 486 reply, under either nesting:
	/// printed bare at the body root, or wrapped in `<dingdongCfg>` — which is
	/// what the field camera prints, and also the shape the 487 write frame
	/// has always used, so the reply arrives in the very shape the camera
	/// expects back.
	pub fn dingdong_events(&self) -> &[DingDongDeviceCfg] {
		if !self.device_cfg.is_empty() {
			return &self.device_cfg;
		}
		self.dingdong_cfg.as_ref().map(|c| c.device_cfg.as_slice()).unwrap_or(&[])
	}
	/// The msg 483 read frame: `<dingdongCtrl><opt>machineStateGet</opt>`.
	/// No id tag — the wired chime is per channel, and the channelId lives in
	/// the message header/extension, like the other chime-family requests.
	pub fn body_ding_dong_ctrl_get() -> Self {
		BcXml {
			dingdong_ctrl: Some(DingDongCtrl {
				version: xml_ver(),
				opt: Some("machineStateGet".into()),
				..Default::default()
			}),
			..Default::default()
		}
	}
	/// A `<body>` carrying `ctrl` under `<dingdongCtrl>` for msg 483's
	/// `machineStateSet` write. The caller passes the block it read with the
	/// patched tags — this builder only stamps `<opt>machineStateSet</opt>`
	/// and, per the documented write frame, `<bsave>1</bsave>` when the read
	/// didn't carry a `bsave` text of its own. It never adds a tag the read
	/// omitted.
	pub fn body_with_ding_dong_ctrl(ctrl: &DingDongCtrl) -> Self {
		BcXml {
			dingdong_ctrl: Some(DingDongCtrl {
				opt: Some("machineStateSet".into()),
				bsave: Some(ctrl.bsave.clone().unwrap_or_else(|| "1".into())),
				..ctrl.clone()
			}),
			..Default::default()
		}
	}
	/// The msg 485 `getParam` read frame for one chime.
	pub fn body_ding_dong_opt_get(chime_id: &str) -> Self {
		BcXml {
			dingdong_device_opt: Some(DingDongDeviceOpt {
				version: xml_ver(),
				opt: Some("getParam".into()),
				id: Some(chime_id.into()),
				..Default::default()
			}),
			..Default::default()
		}
	}
	/// A `<body>` carrying a msg 485 frame as-is. The caller builds either a
	/// `setParam` block (the printed tags, patched — nothing invented) or a
	/// `ringWithMusic` block (`<id>` + `<musicId>`); this wrapper adds
	/// nothing, because "only the tags you pass" is the documented frame.
	pub fn body_with_ding_dong_opt(opt: &DingDongDeviceOpt) -> Self {
		BcXml {
			dingdong_device_opt: Some(opt.clone()),
			..Default::default()
		}
	}
	/// The msg 609 silent-mode read frame for one chime.
	pub fn body_ding_dong_silent_get(chime_id: &str) -> Self {
		BcXml {
			dingdong_silent_mode: Some(DingDongSilent {
				version: xml_ver(),
				id: Some(chime_id.into()),
				..Default::default()
			}),
			..Default::default()
		}
	}
	/// A `<body>` carrying `silent` under `<dingdongSilentMode>` for the msg
	/// 610 write, stamping the hard-coded `<type>63</type>` the documented
	/// write frame carries when the read didn't print a `type` of its own.
	/// What 63 means is undocumented; it is sent because the reference
	/// implementation sends it, and that is all anyone knows.
	pub fn body_with_ding_dong_silent(silent: &DingDongSilent) -> Self {
		BcXml {
			dingdong_silent_mode: Some(DingDongSilent {
				type_: Some(silent.type_.clone().unwrap_or_else(|| "63".into())),
				..silent.clone()
			}),
			..Default::default()
		}
	}
	/// A `<body>` wrapping one `cfg` block in `<dingdongCfg>` for msg 487.
	pub fn body_with_ding_dong_cfg(cfg: &DingDongDeviceCfg) -> Self {
		BcXml {
			dingdong_cfg: Some(DingDongCfgReq {
				version: xml_ver(),
				device_cfg: vec![cfg.clone()],
			}),
			..Default::default()
		}
	}
	pub(crate) fn serialize<W: Write>(&self, mut w: W) -> Result<W, quick_xml::SeError> {
		let mut buf: Vec<u8> = Vec::new();
		{
			let mut writer = quick_xml::writer::Writer::new(&mut buf);
			// Bubble the (in practice unreachable, since the inputs are
			// fixed string literals) error rather than `expect`ing —
			// keeps a public serializer in `crates/core/` panic-free.
			writer.write_event(quick_xml::events::Event::Decl(
				quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None),
			))?;
			writer.write_serializable("body", &self)?;
		}
		// The camera firmware's TiXml parser condenses raw whitespace inside
		// text nodes. That collapses the newlines of the multi-line PEM
		// carried in `<certChain>` on the sigV3 login, corrupting the cert so
		// the cloud token fails validation. The official app emits those
		// newlines as `&#x0A;` entities, which survive condensing — replicate
		// that. quick-xml emits compact XML, so the only raw newlines in `buf`
		// are inside text values, where this re-encoding is value-preserving.
		w.write_all(&encode_certchain_newlines(&buf))?;
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

/// Deserialize camera XML, replacing invalid UTF-8 with U+FFFD first.
///
/// quick-xml rejects a whole document on any invalid UTF-8 byte. Camera
/// firmware can carry legacy-encoded text (device names, OSD strings) in
/// fields we never read, and one such byte must not fail the message, so
/// we decode lossily. Nesting depth is capped by quick-xml's default
/// recursion limit (128) on top of the `bc::de::MAX_BC_BODY_LEN` byte cap.
pub(crate) fn parse_lossy_utf8<T: serde::de::DeserializeOwned>(
	bytes: &[u8],
) -> Result<T, quick_xml::de::DeError> {
	quick_xml::de::from_str(&String::from_utf8_lossy(bytes))
}

impl Extension {
	pub(crate) fn try_parse(s: &[u8]) -> Result<Self, quick_xml::de::DeError> {
		parse_lossy_utf8(s)
	}
	pub(crate) fn serialize<W: Write>(&self, mut w: W) -> Result<W, quick_xml::SeError> {
		let mut writer = quick_xml::writer::Writer::new(&mut w);
		// Bubble the (in practice unreachable, since the inputs are
		// fixed string literals) error rather than `expect`ing —
		// keeps a public serializer in `crates/core/` panic-free.
		writer.write_event(quick_xml::events::Event::Decl(
			quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None),
		))?;
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
	/// Device class, e.g. `"wifi_solo_ipc"` (battery camera) or
	/// `"smart_bell"` (doorbell)
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub device_type: Option<String>,
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
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
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
	/// Marker the firmware requires when a msg 105 payload only renames the
	/// device: `<deviceNameOnly>1</deviceNameOnly>`. Absent otherwise, and
	/// never invented — `set_general` sets it when, and only when, the patch
	/// touched `deviceName` and nothing else.
	#[serde(
		default,
		rename = "deviceNameOnly",
		skip_serializing_if = "Option::is_none"
	)]
	pub device_name_only: Option<u8>,
	/// Whether DST is currently in effect. **Read-only** on the firmware:
	/// the writable DST switch is `<Dst><enable>` (msgs 106/107).
	#[serde(default, rename = "isDst", skip_serializing_if = "Option::is_none")]
	pub is_dst: Option<u8>,
	/// Login-bruteforce lock flag, as printed.
	#[serde(default, rename = "loginLock", skip_serializing_if = "Option::is_none")]
	pub login_lock: Option<u8>,
	/// Login lock duration, as printed.
	#[serde(default, rename = "lockTime", skip_serializing_if = "Option::is_none")]
	pub lock_time: Option<u32>,
	/// Allowed failed attempts before the lock, as printed.
	#[serde(
		default,
		rename = "allowedTimes",
		skip_serializing_if = "Option::is_none"
	)]
	pub allowed_times: Option<u32>,
	/// Device id the firmware reports, as printed.
	#[serde(default, rename = "deviceId", skip_serializing_if = "Option::is_none")]
	pub device_id: Option<String>,
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

/// HaCfg xml: the camera's HTTP webhook target
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct HaCfg {
	/// XML Version, observed value is "1.1"
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Webhook enabled (0=off, 1=on)
	#[serde(default)]
	pub enable: u8,
	/// Target URL; empty when disabled
	#[serde(default)]
	pub url: String,
	/// Verify the target's TLS certificate (0=off, 1=on)
	#[serde(default)]
	pub verify_cert: u8,
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

/// `<duplexList>` wrapper carrying the repeated `<duplex>` elements.
///
/// A collection, like [`AuthTypeList`] models the repeated `<authType>`:
/// the tag is a *list*, so a camera advertising both `FDX` and `FBX` in one
/// `<duplexList>` sends two `<duplex>` children, which a scalar field cannot
/// hold (quick_xml fails the whole reply with ``duplicate field `duplex` ``).
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct DuplexList {
	/// Each supported duplex mode, known values `"FDX"`, `"FBX"`.
	#[serde(default)]
	pub duplex: Vec<String>,
}

/// `<audioStreamModeList>` wrapper carrying the repeated `<audioStreamMode>`
/// elements.
///
/// Measured on hardware: one `<audioStreamModeList>` carrying *two*
/// `<audioStreamMode>` children (`followVideoStream`, `mixAudioStream`),
/// which is what made a scalar field drop the entire `<TalkAbility>` reply
/// with ``duplicate field `audioStreamMode` ``.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AudioStreamModeList {
	/// Each supported audio stream mode, known values `"followVideoStream"`,
	/// `"mixAudioStream"`.
	#[serde(default, rename = "audioStreamMode")]
	pub audio_stream_mode: Vec<String>,
}

/// `<audioConfigList>` wrapper carrying the repeated `<audioConfig>` elements.
///
/// Same reasoning as [`AudioStreamModeList`]: nothing stops a firmware from
/// advertising several codecs (different `lengthPerEncoder`, say) inside one
/// `<audioConfigList>`, and a scalar field would lose the whole reply.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize)]
pub struct AudioConfigList {
	/// Each supported audio config.
	#[serde(default, rename = "audioConfig")]
	pub audio_config: Vec<AudioConfig>,
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
	/// AI status. Known values are `"people"` or `"none"`
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
	/// Doorbell version; present and non-zero on doorbells (e.g. `16`)
	#[serde(
		default,
		rename = "doorbellVersion",
		skip_serializing_if = "Option::is_none"
	)]
	pub doorbell_version: Option<u32>,
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

/// Which spelling the camera used for the loudspeaker-config element.
///
/// Not a field of the block: it is a property of the *envelope*, recorded by
/// the read (264) and honoured by the write (265), because this firmware only
/// re-accepts a block it recognises — and the name it printed is the name it
/// matches. See [`BcXml::audio_cfg_block`].
#[derive(PartialEq, Eq, Default, Debug, Clone, Copy)]
pub enum AudioCfgTag {
	/// `<audioCfg>` — the spelling the protocol note prints first, and the
	/// one Reolink's own web UI sends. The default for a hand-built block.
	#[default]
	Camel,
	/// `<AudioCfg>`.
	Pascal,
}

impl AudioCfgTag {
	/// The element name this spelling stands for — what the camera printed, and
	/// therefore what a write has to print.
	pub fn element_name(&self) -> &'static str {
		match self {
			AudioCfgTag::Camel => "audioCfg",
			AudioCfgTag::Pascal => "AudioCfg",
		}
	}
}

/// The `<audioCfg>` loudspeaker / intercom block (read: msg 264, write:
/// msg 265).
///
/// Every value is kept as the **text the camera sent**, never parsed into a
/// number: the block is read, patched tag by tag and sent straight back, so
/// re-serialising it has to reproduce the firmware's own text, and a value
/// this crate doesn't understand must survive the round trip unchanged.
/// Interpretation (volumes are 0-100 per the protocol note) belongs to the
/// caller.
///
/// The three doorbell-only tags are `Option`: a camera that has no intercom
/// does not print them, and inventing them on the way back is how a SET gets
/// rejected. A camera that *does* print them always gets them back.
///
/// Caveat to keep in mind when reading a block off an unfamiliar model: serde
/// drops what it doesn't model, so a tag absent from this struct is also
/// absent from the block that gets written back. The tag list here is the one
/// the internal protocol note documents.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct AudioCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The channel this block belongs to, as the camera wrote it.
	#[serde(default, rename = "channelId")]
	pub channel_id: String,
	/// Unknown; documented as part of the block, never written by the web UI.
	#[serde(default, rename = "timeout")]
	pub timeout: String,
	/// Unknown; documented as part of the block, never written by the web UI.
	#[serde(default, rename = "audioSelect")]
	pub audio_select: String,
	/// The loudspeaker volume (0-100).
	#[serde(default, rename = "volume")]
	pub volume: String,
	/// Unknown; documented as part of the block, never written by the web UI.
	#[serde(default, rename = "preAlarm")]
	pub pre_alarm: String,
	/// Unknown; documented as part of the block, never written by the web UI.
	#[serde(default, rename = "audioListId")]
	pub audio_list_id: String,
	/// The intercom volume (0-100): how loud the answer you speak through the
	/// app comes out of the doorbell. Doorbell / intercom firmwares only.
	#[serde(
		default,
		rename = "talkAndReplyVolume",
		skip_serializing_if = "Option::is_none"
	)]
	pub talk_and_reply_volume: Option<String>,
	/// How loud the pre-set greeting clips play (0-100). Doorbell only.
	#[serde(
		default,
		rename = "visitorVolume",
		skip_serializing_if = "Option::is_none"
	)]
	pub visitor_volume: Option<String>,
	/// Whether the loudspeaker is live for a visitor (`0` / `1`). Doorbell only.
	#[serde(
		default,
		rename = "visitorLoudspeaker",
		skip_serializing_if = "Option::is_none"
	)]
	pub visitor_loudspeaker: Option<String>,
	/// The spelling the camera used for this block, recorded by the read and
	/// reused by the write. `#[serde(skip)]`: it is not a tag of the block,
	/// and the read fills it from the [`BcXml`] field the block arrived in
	/// (see [`BcXml::audio_cfg_block`]).
	#[serde(skip)]
	pub tag: AudioCfgTag,
}

/// The `<dingdongCtrl>` hardwired-chime block (msg 483: `machineStateGet`
/// reads it, `machineStateSet` writes it).
///
/// Same law as [`AudioCfg`]: values are the camera's own text, and a tag the
/// camera didn't print is never added on the way back. The on/off tag is
/// documented under **two spellings** — `<bopen>` in the request frame,
/// `<bopen>` or `<enable>` in replies — so both are modelled and only the one
/// the camera printed goes back out (exactly one of them is set by a read).
///
/// What `<type>` selects (the documented example value is `dingdong`) and what
/// unit `<time>` counts in are **undocumented**; both are echoed verbatim. The
/// reference client warns that issuing the read can itself make a wired chime
/// sound — reading this block is not physically silent.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongCtrl {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// `machineStateGet` / `machineStateSet`. Read as absent (replies don't
	/// document it carrying one); stamped by the write builder.
	#[serde(default, rename = "opt", skip_serializing_if = "Option::is_none")]
	pub opt: Option<String>,
	/// Present in some replies; the request keeps the channel in the header.
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<String>,
	/// Chime type selector, documented example `dingdong`. Meaning unknown.
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<String>,
	/// The switch, `<bopen>` spelling (the one the write frame documents).
	#[serde(default, rename = "bopen", skip_serializing_if = "Option::is_none")]
	pub bopen: Option<String>,
	/// The same switch, `<enable>` spelling. A write returns whichever the
	/// camera printed, never both.
	#[serde(default, rename = "enable", skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// Write frame only: `1` asks the camera to persist. Read as printed.
	#[serde(default, rename = "bsave", skip_serializing_if = "Option::is_none")]
	pub bsave: Option<String>,
	/// Unit undocumented (reference client re-sends the read value as-is).
	#[serde(default, rename = "time", skip_serializing_if = "Option::is_none")]
	pub time: Option<String>,
	/// A refusal reason instead of a switch. Measured on a battery doorbell
	/// with no wired chime: 483 answers `200` with `<result>adptNotAC</result>`
	/// next to `<bopen>0</bopen>` —"no mains adapter", that is: the wired chime
	/// is not wired up. Reporting it is what turns a bare `bopen = 0`
	/// from a setting into an explanation. Never written.
	#[serde(default, rename = "result", skip_serializing_if = "Option::is_none")]
	pub result: Option<String>,
}

impl DingDongCtrl {
	/// The on/off switch text, under whichever spelling the camera used.
	pub fn switch_state(&self) -> Option<&str> {
		self.bopen.as_deref().or(self.enable.as_deref())
	}
}

/// The `<dingdongList>` carrier of msg 484 — **a container, not the entries**.
///
/// Captured verbatim from a `v3.0.0.4636_25060550` doorbell with a paired
/// chime:
///
/// ```xml
/// <dingdongList version="1.1">
///   <maxPairNumber>5</maxPairNumber>
///   <pairedList>
///     <dingdongDeviceInfo>
///       <id>882092649</id><netstate>2</netstate><name>Reolink Chime</name>
///     </dingdongDeviceInfo>
///   </pairedList>
/// </dingdongList>
/// ```
///
/// Both nestings are modelled: the wrapper (measured) and entries printed
/// directly under `<dingdongList>` (the pushed variant the protocol note
/// mentions). Whichever the camera filled is what
/// [`BcXml::dingdong_chimes`] returns.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// How many chimes the camera will pair (5 measured). Read-only ceiling,
	/// never sent.
	#[serde(default, rename = "maxPairNumber", skip_serializing_if = "Option::is_none")]
	pub max_pair_number: Option<String>,
	/// The measured wrapper the entries sit in.
	#[serde(default, rename = "pairedList", skip_serializing_if = "Option::is_none")]
	pub paired_list: Option<DingDongPairedList>,
	/// Entries printed directly under the container instead.
	#[serde(default, rename = "dingdongDeviceInfo", skip_serializing_if = "Vec::is_empty")]
	pub device_info: Vec<DingDongDevice>,
}

/// The `<pairedList>` wrapper: nothing but repeated `<dingdongDeviceInfo>`.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongPairedList {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The paired chimes, as printed.
	#[serde(default, rename = "dingdongDeviceInfo")]
	pub device_info: Vec<DingDongDevice>,
}

/// One `<dingdongDeviceInfo>` entry of the paired-chime list (msg 484).
///
/// Both documented spellings of each name are modelled (`deviceId`/`id`,
/// `deviceName`/`name`, `netState`/`netstate`) because the reference client
/// reads each tag with a fallback, and quick-xml matches names exactly. The
/// values stay camera text.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongDevice {
	/// `deviceId` spelling of the chime id.
	#[serde(default, rename = "deviceId", skip_serializing_if = "Option::is_none")]
	pub device_id: Option<String>,
	/// `id` spelling of the chime id.
	#[serde(default, rename = "id", skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// `deviceName` spelling of the chime name (free text).
	#[serde(default, rename = "deviceName", skip_serializing_if = "Option::is_none")]
	pub device_name: Option<String>,
	/// `name` spelling of the chime name.
	#[serde(default, rename = "name", skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// `netState` spelling: documented as `0` offline, `1` online. The only
	/// presence indicator the list offers — and the documentation is
	/// incomplete: the field camera prints **`2`** for a chime that is paired
	/// and answers its own per-chime reads. The value is kept as camera text;
	/// nothing here reinterprets it.
	#[serde(default, rename = "netState", skip_serializing_if = "Option::is_none")]
	pub net_state: Option<String>,
	/// `netstate` spelling of the same field.
	#[serde(default, rename = "netstate", skip_serializing_if = "Option::is_none")]
	pub netstate: Option<String>,
}

impl DingDongDevice {
	/// The chime id — `deviceId` if present, else `id` — but **only when it
	/// is a non-empty run of digits**: blocks without a numeric id are not
	/// addressable, and the reference client skips them the same way.
	pub fn identifier(&self) -> Option<&str> {
		let raw = self.device_id.as_deref().or(self.id.as_deref())?;
		let id = raw.trim();
		if !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()) {
			Some(id)
		} else {
			None
		}
	}
	/// The chime's display name, `deviceName` preferred, as camera text.
	pub fn display_name(&self) -> Option<&str> {
		self.device_name.as_deref().or(self.name.as_deref())
	}
	/// The online flag under whichever spelling arrived: `Some("1")` means
	/// the chime answered its last keep-alive.
	pub fn net_state(&self) -> Option<&str> {
		self.net_state.as_deref().or(self.netstate.as_deref())
	}
	/// True only when the list says `1` — offline and unread both say no.
	///
	/// Strict on purpose, and **not** a presence test: this camera family also
	/// prints `2` for a reachable chime, so a caller that wants "the chime to
	/// talk to "must fall back to any entry with an
	/// [`identifier`](Self::identifier) — which is what
	/// [`crate::bc_protocol::BcCamera::get_ding_dong_cfg`] does.
	pub fn is_online(&self) -> bool {
		self.net_state() == Some("1")
	}
}

/// The `<dingdongDeviceOpt>` carrier of msg 485. One element, three frames,
/// told apart by `<opt>`: `getParam` (read; reply carries `name` / `volLevel`
/// / `ledState`), `setParam` (write; **only the tags you pass** are sent) and
/// `ringWithMusic` (`<id>` + `<musicId>`, makes the chime ring once).
///
/// `volLevel` is documented as "0-4 typical" — the firmware's real ceiling is
/// **undocumented**. `musicId` is a free integer; **no melody list exists
/// anywhere** in the protocol material, so the only guidance is the app's own
/// picker. Values stay camera text like everywhere else in this module.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongDeviceOpt {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// `getParam` / `setParam` / `ringWithMusic`.
	#[serde(default, rename = "opt", skip_serializing_if = "Option::is_none")]
	pub opt: Option<String>,
	/// The chime this frame addresses (a 484 list id).
	#[serde(default, rename = "id", skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// The chime's name — writable, free text, no documented length limit.
	#[serde(default, rename = "name", skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// Chime volume, "0-4 typical"; true range undocumented.
	#[serde(default, rename = "volLevel", skip_serializing_if = "Option::is_none")]
	pub vol_level: Option<String>,
	/// The chime's LED: `0` or `1`.
	#[serde(default, rename = "ledState", skip_serializing_if = "Option::is_none")]
	pub led_state: Option<String>,
	/// Melody id for `ringWithMusic` / the event config. Integer, no list.
	#[serde(default, rename = "musicId", skip_serializing_if = "Option::is_none")]
	pub music_id: Option<String>,
}

/// The `<dingdongSilentMode>` block, msgs 609 (read: `<id>` in, `<time>`
/// back) and 610 (write: `<id>` + `<time>` + hard-coded `<type>63</type>`).
///
/// `<time>` is the silent window; its unit is **undocumented** (seconds is
/// what the app's mute picker appears to send, but nothing states it). The
/// write RE-ARMS the countdown even for an unchanged value — which is why the
/// core only sends this block when the text actually changed.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongSilent {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The chime this applies to.
	#[serde(default, rename = "id", skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// The silent-mode value as the camera wrote it.
	#[serde(default, rename = "time", skip_serializing_if = "Option::is_none")]
	pub time: Option<String>,
	/// Write frame only; `63` is copied from the reference client, meaning
	/// unknown.
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<String>,
}

/// One `<deviceCfg>` block of the per-event chime config (msg 486 reply;
/// msg 487 writes one back inside a [`DingDongCfgReq`]).
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongDeviceCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The chime these events belong to (a 484 list id).
	#[serde(default, rename = "id", skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	/// Seen in 486 replies; role not established. Echoed by nothing — the
	/// 487 write frame documents `id` only — so it is reported, not rewritten.
	#[serde(default, rename = "ringId", skip_serializing_if = "Option::is_none")]
	pub ring_id: Option<String>,
	/// The chime's name, which the 486 reply prints beside its id (measured:
	/// `Reolink Chime`). Read-only label — the writable name lives in the 485
	/// frame.
	#[serde(default, rename = "name", skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// The **chime's own firmware**, printed as the child `<version>` (measured
	/// `00A2`). Not the XML `version` attribute this struct also carries, and
	/// not writable: reported only.
	#[serde(default, rename = "version", skip_serializing_if = "Option::is_none")]
	pub firm_version: Option<String>,
	/// One block per ringable event type the camera offers.
	#[serde(default, rename = "alarminCfg", skip_serializing_if = "Vec::is_empty")]
	pub alarmin_cfg: Vec<DingDongAlarmCfg>,
}

/// One `<alarminCfg>` entry: the ring switch and melody for one event type.
///
/// `<type>` is an event-type **string the camera chose** — passed verbatim
/// into writes, no validation table exists (the recommendation is to only
/// ever write types the camera itself returned). The switch arrives as
/// `<valid>` in some paths and `<switch>` in others; both are modelled, and a
/// write returns whichever arrived, under the `valid` spelling the 487 frame
/// documents — omitted entirely if the camera printed neither.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongAlarmCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Event type, camera-chosen string, passed verbatim everywhere.
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<String>,
	/// Switch, `valid` spelling (the 487 write frame's name).
	#[serde(default, rename = "valid", skip_serializing_if = "Option::is_none")]
	pub valid: Option<String>,
	/// Same switch, `switch` spelling. (`switch_` in Rust — keyword.)
	#[serde(default, rename = "switch", skip_serializing_if = "Option::is_none")]
	pub switch_: Option<String>,
	/// Melody id for this event. Integer, no documented list.
	#[serde(default, rename = "musicId", skip_serializing_if = "Option::is_none")]
	pub music_id: Option<String>,
}

impl DingDongAlarmCfg {
	/// The ring switch text, `valid` preferred (its documented default when
	/// the camera printed neither tag is effectively "off").
	pub fn state(&self) -> Option<&str> {
		self.valid.as_deref().or(self.switch_.as_deref())
	}
}

/// The `<dingdongCfg>` wrapper — msg 487's request frame **and**, measured on
/// the field camera, the root its 486 reply arrives under. The read therefore
/// lands here rather than in the bare [`device_cfg`](Self::device_cfg) field,
/// and [`BcXml::dingdong_events`] folds the two.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct DingDongCfgReq {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// The block(s) to store. This crate writes exactly one per request.
	#[serde(default, rename = "deviceCfg")]
	pub device_cfg: Vec<DingDongDeviceCfg>,
}

/// The whole chime configuration as one aggregate — the shape
/// [`BcCamera::get_ding_dong_cfg`](crate::bc_protocol::BcCamera::get_ding_dong_cfg)
/// returns and
/// [`set_ding_dong_cfg`](crate::bc_protocol::BcCamera::set_ding_dong_cfg)
/// accepts.
///
/// It is **not** one XML element: the firmware spreads the chime over five
/// reads (484 list, 483 wired control, 485 params, 609 silent, 486 events),
/// each of which a camera without that part may refuse. Every field is
/// therefore an `Option`, and `Option<Vec>` distinguishes "the camera refused
/// this part" (`None`) from "the camera answered with an empty list"
/// (`Some(vec![])`). A `set` diffed against a fresh read writes **only the
/// parts whose text differs** — the chime SETs carry side effects (the silent
/// mode re-arms its countdown just by arriving), so "rewrite the received
/// block" here means "re-send exactly the blocks whose tags changed, whole".
#[derive(PartialEq, Eq, Default, Debug, Clone, Serialize)]
pub struct DingDongCfg {
	/// The chime the params/silent/event parts were read for: the first
	/// numeric id in the 484 list, preferring one the list marks online.
	/// `None` when no addressable chime is paired.
	pub chime_id: Option<String>,
	/// The 484 paired-chime list; `None` when the camera refused 484 **or**
	/// answered it with no entries at all. Entries that carry a name but no
	/// addressable id are **kept** — they are what the camera printed, and
	/// [`list_answered_without_entry`](Self::list_answered_without_entry) is
	/// what says none of them can be written to.
	pub chimes: Option<Vec<DingDongDevice>>,
	/// Msg 484 answered `200` and no entry — under either documented spelling
	/// (`<dingdongDeviceInfo>`, `<dingdongList>`) or either id tag
	/// (`<deviceId>`, `<id>`) — carried an id this bridge can address.
	///
	/// This is the field that separates "the camera has no paired wireless chime"
	/// from "the camera answered 484, but in a shape this bridge cannot read"—
	/// and it is the only honest way to
	/// report a paired chime the inventory does not show. It publishes as
	/// `status/chime/reason` next to an empty `status/chime/list`.
	#[serde(skip_serializing_if = "std::ops::Not::not")]
	pub list_answered_without_entry: bool,
	/// The 483 hardwired-chime block; `None` when refused (normal on
	/// wireless-only installs, where 483 is not answered as a block).
	pub ctrl: Option<DingDongCtrl>,
	/// The 485 `getParam` reply — the wireless chime's name/volume/LED;
	/// `None` when refused or when no chime id could be picked.
	pub params: Option<DingDongDeviceOpt>,
	/// The 609 silent-mode block; same `None` conditions as
	/// [`params`](Self::params).
	pub silent: Option<DingDongSilent>,
	/// The 486 per-event config; `None` when refused.
	pub events: Option<Vec<DingDongDeviceCfg>>,
}

/// `<Ntp version="1.1">` — the clock-synchronisation block, read with msg
/// **38** and written back with msg **39**.
///
/// Every value is kept as the **text the camera printed** (like
/// [`AudioCfg`]): the setter is a read-modify-write of this same block, and a
/// value that round-trips through an integer can come back formatted
/// differently (`01`, `+01`) than the firmware wrote it, which is how a
/// "successful" write silently changes a field nobody asked it to touch.
/// `<synchronizeInterval>` is in **minutes** (the reference client's default
/// is 1440, i.e. 24 h — inferred from its default, not from the camera).
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct Ntp {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel the block belongs to, as printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// `1` = synchronise from the server, `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// NTP hostname, as the camera stored it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub server: Option<String>,
	/// NTP port (the reference client's default is 123).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub port: Option<String>,
	/// Re-sync period in **minutes**.
	#[serde(
		default,
		rename = "synchronizeInterval",
		skip_serializing_if = "Option::is_none"
	)]
	pub synchronize_interval: Option<String>,
	/// Time zone the NTP sync should render in, printed as an offset in
	/// seconds with the POSIX convention (UTC+1 → `-3600`).
	#[serde(default, rename = "timeZone", skip_serializing_if = "Option::is_none")]
	pub time_zone: Option<String>,
}

/// `<AutoReboot version="1.1">` — the weekly (or daily) automatic reboot,
/// read with msg **101** and written with msg **100** (the ids are inverted
/// relative to every other pair in this protocol: the setter is the lower).
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct AutoReboot {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// `1` = reboot on schedule, `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// `"Sunday"`…`"Saturday"`, or the special value `"everyday"`.
	#[serde(default, rename = "weekDay", skip_serializing_if = "Option::is_none")]
	pub week_day: Option<String>,
	/// Hour of the reboot, `0`–`23`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hour: Option<String>,
	/// Minute of the reboot, `0`–`59`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub minute: Option<String>,
	/// Second of the reboot, `0`–`59`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub second: Option<String>,
}

/// `<aiDenoise>` — the on-device denoiser applied to the camera microphone,
/// read with msg **439** and written with msg **440**.
///
/// The element name is lower-case while its version attribute is documented
/// as printed **capitalised** (`Version="1.1"`), so both spellings are
/// carried: whichever the camera used comes back out, and the other stays
/// absent. This block is the closest thing the protocol has to a microphone
/// setting — no mic gain or mic mute exists anywhere in the surface that was
/// measured on this doorbell).
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct AiDenoise {
	/// XML Version, capitalised spelling (`<aiDenoise Version="1.1">`).
	#[serde(default, rename = "@Version", skip_serializing_if = "Option::is_none")]
	pub version_caps: Option<String>,
	/// XML Version, lower-case spelling, for firmware that prints it that way.
	#[serde(default, rename = "@version", skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
	/// `1` = denoiser on, `0` = off. The reference client notes that a
	/// `level <= 0` also flips the denoiser off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// Strength, `1`–`100`; only applied while enabled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub level: Option<String>,
	/// Channel, where the firmware prints one.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
}

/// `<sleepState>` — whether the (battery) camera is asleep, read with msg
/// **574**. Read-only: no setter is documented, and putting a battery device
/// to sleep from a settings write is not something this bridge does.
///
/// `<mode>` is numeric and its meaning is **undocumented**; it is relayed as
/// text, never interpreted.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct SleepState {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// `1` = asleep, `0` = awake.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sleep: Option<String>,
	/// Numeric, meaning undocumented.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<String>,
	/// PTZ park position (PTZ cameras only).
	#[serde(default, rename = "panPos", skip_serializing_if = "Option::is_none")]
	pub pan_pos: Option<String>,
	/// PTZ park position (PTZ cameras only).
	#[serde(default, rename = "tiltPos", skip_serializing_if = "Option::is_none")]
	pub tilt_pos: Option<String>,
	/// Image name the sleeping camera serves.
	#[serde(default, rename = "imageName", skip_serializing_if = "Option::is_none")]
	pub image_name: Option<String>,
}

/// `<WifiSignal>` — the Wi-Fi report, read with msg **115**.
///
/// `<signal>` is a number whose **unit is undocumented** (dBm or %; the
/// reference client never converts it), so it is published as the camera's
/// own text and no unit is claimed on the HA side.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct WifiSignal {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, where printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// Signal strength, unit undocumented.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub signal: Option<String>,
}

/// `<VideoInput>` — the image block, read with msg **26** and written with
/// msg **25**. The five sliders (`bright`, `contrast`, `saturation`, `hue`,
/// `sharpen`) are **0–255** on this family, defaults around 128.
///
/// The 26 reply carries this block **and** [`InputAdvanceCfg`]; the 25 setter
/// wants both back, so [`BcCamera::set_image_cfg`](crate::bc_protocol::BcCamera::set_image_cfg)
/// takes the pair as one [`crate::bc_protocol::ImageCfg`] carrier.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct VideoInput {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel the block belongs to, as printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// Brightness slider, 0–255.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub bright: Option<String>,
	/// Contrast slider, 0–255.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub contrast: Option<String>,
	/// Saturation slider, 0–255.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub saturation: Option<String>,
	/// Hue slider, 0–255.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hue: Option<String>,
	/// Sharpness slider, 0–255.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sharpen: Option<String>,
	/// IR-cut swap, as printed.
	#[serde(default, rename = "irCutSwap", skip_serializing_if = "Option::is_none")]
	pub ir_cut_swap: Option<String>,
	/// Day/night mode, printed flat on this family (`color`, `auto`,
	/// `blackAndWhite` are the values the reference client normalises to).
	#[serde(default, rename = "dayNight", skip_serializing_if = "Option::is_none")]
	pub day_night: Option<String>,
	/// Whether the 9:16 corridor mode is available on this model.
	#[serde(
		default,
		rename = "corridorAbility",
		skip_serializing_if = "Option::is_none"
	)]
	pub corridor_ability: Option<String>,
	/// Corridor (9:16 rotation) mode — the only mirror/rotation-shaped tag
	/// the protocol has; no setter in the reference client writes it, so
	/// `bairelay` only echoes what the camera printed.
	#[serde(default, rename = "corridorMode", skip_serializing_if = "Option::is_none")]
	pub corridor_mode: Option<String>,
}

/// `<InputAdvanceCfg>` — the second half of the image block, in the same 26
/// reply as [`VideoInput`] and required by the same 25 setter. Field names
/// keep the camera's mixed casing.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct InputAdvanceCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, where printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// Exposure sub-block.
	#[serde(default, rename = "Exposure", skip_serializing_if = "Option::is_none")]
	pub exposure: Option<ExposureCfg>,
	/// Scene mode, as printed.
	#[serde(default, rename = "Scene", skip_serializing_if = "Option::is_none")]
	pub scene: Option<String>,
	/// Mains-frequency flicker suppression, as printed.
	#[serde(
		default,
		rename = "PowerLineFrequency",
		skip_serializing_if = "Option::is_none"
	)]
	pub power_line_frequency: Option<String>,
	/// Digital channel marker, as printed.
	#[serde(
		default,
		rename = "digitalChannel",
		skip_serializing_if = "Option::is_none"
	)]
	pub digital_channel: Option<String>,
}

/// `<Exposure>` inside [`InputAdvanceCfg`].
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct ExposureCfg {
	/// Exposure mode (lower-case on the wire).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<String>,
}

/// `<OsdDatetime>` — the on-screen timestamp, read with msg **44** and
/// written with msg **45**, in the same reply as [`OsdChannelName`].
///
/// `<topLeftX>`/`<topLeftY>` are **not pixels**: they are 16.16 normalised
/// coordinates (`0` or `1` = start edge, `65536` = opposite edge). They are
/// therefore kept as opaque camera text — this bridge never reinterprets or
/// recomputes them.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct OsdDatetime {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, as printed.
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<String>,
	/// `1` = timestamp drawn, `0` = hidden.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// 16.16 normalised X, as printed.
	#[serde(default, rename = "topLeftX", skip_serializing_if = "Option::is_none")]
	pub top_left_x: Option<String>,
	/// 16.16 normalised Y, as printed.
	#[serde(default, rename = "topLeftY", skip_serializing_if = "Option::is_none")]
	pub top_left_y: Option<String>,
	/// Box width; the real unit behind it is undocumented.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub width: Option<String>,
	/// Box height; the real unit behind it is undocumented.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub height: Option<String>,
	/// OSD language, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub language: Option<String>,
}

/// `<OsdChannelName>` — the on-screen camera name (and its watermark flag),
/// in the same 44 reply as [`OsdDatetime`]. Not to be confused with the
/// system device name (`<SystemGeneral><deviceName>`, msg 105).
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct OsdChannelName {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, as printed.
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<String>,
	/// The name drawn on the picture.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	/// `1` = name drawn, `0` = hidden.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// 16.16 normalised X, as printed.
	#[serde(default, rename = "topLeftX", skip_serializing_if = "Option::is_none")]
	pub top_left_x: Option<String>,
	/// 16.16 normalised Y, as printed.
	#[serde(default, rename = "topLeftY", skip_serializing_if = "Option::is_none")]
	pub top_left_y: Option<String>,
	/// Watermark flag — a member of this block, not a root element.
	#[serde(default, rename = "enWatermark", skip_serializing_if = "Option::is_none")]
	pub en_watermark: Option<String>,
	/// Background-colour flag, as printed.
	#[serde(default, rename = "enBgcolor", skip_serializing_if = "Option::is_none")]
	pub en_bgcolor: Option<String>,
}

/// `<Shelter>` — the privacy-mask block, read with msg **52**.
///
/// **Read-only here, deliberately.** The documented shape names a
/// `shelterList` whose per-mask child tags (and the `pixel << 16 | canvas`
/// coordinate coding) have no local capture, and the firmware "rejects, or
/// silently cancels, a block that omits fields it sent"
/// (observed on the operator's doorbell). Serialising this struct would therefore
/// send back a block amputated of the masks themselves — exactly the failure
/// the house rules forbid — so `set_privacy_masks` refuses instead of
/// guessing. `enable`/`maxNum` are still worth reading.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct Shelter {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, where printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// `1` = masking active, `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// How many mask slots the model offers.
	#[serde(default, rename = "maxNum", skip_serializing_if = "Option::is_none")]
	pub max_num: Option<String>,
	/// PTZ-only tracking masks: `bairelay` reads it to know the model has
	/// them and never rewrites it.
	#[serde(default, rename = "trackEnable", skip_serializing_if = "Option::is_none")]
	pub track_enable: Option<String>,
}

/// `<MD version="1.1">` — the motion-detection block in the shape the msg
/// **47** setter documents. Some firmwares print this same shape from the
/// **46** getter; those may be written back. Others answer `<MdAlarm>`
/// instead — see [`MdAlarm`] for why that carrier is read-only.
///
/// Sensitivity is **0–50, higher = more sensitive**, and lives in
/// `<sensitivityDefault>`. The `<scope>` bitmap (96×64 = 6144 bits, base64)
/// is kept as the camera's own text so a write of `enable`/`sensitivity`
/// cannot damage the zones.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct MdCfg {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, as printed.
	#[serde(default, rename = "channelId", skip_serializing_if = "Option::is_none")]
	pub channel_id: Option<String>,
	/// `1` = motion detection on, `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// Whether the MD alarm is gated by the PIR, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub usepir: Option<String>,
	/// Zone-grid width in cells, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub width: Option<String>,
	/// Zone-grid height in cells, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub height: Option<String>,
	/// The zone bitmap carrier.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub scope: Option<MdScope>,
	/// Sensitivity, 0–50, higher = more sensitive.
	#[serde(
		default,
		rename = "sensitivityDefault",
		skip_serializing_if = "Option::is_none"
	)]
	pub sensitivity_default: Option<String>,
}

/// `<scope>` inside [`MdCfg`]: the zone grid and its base64 bitmap.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct MdScope {
	/// Grid columns, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub columns: Option<String>,
	/// Grid rows, as printed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub rows: Option<String>,
	/// Base64 bitmap, relayed untouched: 6144 bits the bridge does not decode.
	#[serde(default, rename = "valueTable", skip_serializing_if = "Option::is_none")]
	pub value_table: Option<String>,
}

/// `<MdAlarm>` — the shape the msg **46** getter is documented to answer on
/// the firmwares measured on the operator's doorbell. Readable, **never
/// written**: the block's "champs libres" are not enumerated anywhere, and a
/// write built from this struct would drop them.
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct MdAlarm {
	/// XML Version
	#[serde(default, rename = "@version")]
	pub version: String,
	/// Channel, where printed.
	#[serde(
		default,
		rename = "channelId",
		skip_serializing_if = "Option::is_none"
	)]
	pub channel_id: Option<String>,
	/// The enable/sensitivity pair this shape exposes.
	#[serde(
		default,
		rename = "sensInfoNew",
		skip_serializing_if = "Option::is_none"
	)]
	pub sens_info_new: Option<SensInfoNew>,
}

/// `<sensInfoNew>` inside [`MdAlarm`].
#[derive(PartialEq, Eq, Default, Debug, Clone, Deserialize, Serialize)]
pub struct SensInfoNew {
	/// `1` = motion detection on, `0` = off.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<String>,
	/// Sensitivity, as printed (0–50 on the shapes that document a range).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub sensitivity: Option<String>,
	/// Alarm type, as printed.
	#[serde(default, rename = "alarmType", skip_serializing_if = "Option::is_none")]
	pub alarm_type: Option<String>,
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
	crate::test_logs::init();

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
	crate::test_logs::init();
	let sample = indoc!(
		r#"<?xml version="1.0" encoding="UTF-8" ?>
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
	crate::test_logs::init();
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

#[test]
fn certchain_crlf_never_reaches_the_wire_raw() {
	// quick-xml escapes `\r` in text itself (`&#13;`); our pass handles `\n`.
	// Either way no raw CR/LF may survive, and the value must round-trip.
	let pem = "-----BEGIN-----\r\nAAAA\r\n-----END-----\r\n";
	let login = BcXml {
		login_user: Some(LoginUser {
			version: "1.1".to_string(),
			user_name: "u".to_string(),
			cert_chain: Some(pem.to_string()),
			..Default::default()
		}),
		..Default::default()
	};
	let wire = login.serialize(vec![]).unwrap();
	let xml = String::from_utf8(wire.clone()).unwrap();
	let inner = &xml[xml.find("<certChain>").unwrap()..xml.find("</certChain>").unwrap()];
	assert!(!inner.contains(['\r', '\n']), "{inner}");
	let back = BcXml::try_parse(&wire).unwrap();
	assert_eq!(back.login_user.unwrap().cert_chain.as_deref(), Some(pem));
}

#[test]
fn invalid_utf8_is_replaced_not_fatal() {
	// One stray legacy-encoded byte must not fail the whole message.
	let ext = Extension::try_parse(
		b"<Extension version=\"1.1\"><userName>ad\xffmin</userName><channelId>3</channelId></Extension>",
	)
	.unwrap();
	assert_eq!(ext.user_name.as_deref(), Some("ad\u{FFFD}min"));
	assert_eq!(ext.channel_id, Some(3));

	// Same inside an element we don't model.
	let ext = Extension::try_parse(
		b"<Extension version=\"1.1\"><osdName>\xfe\xff</osdName><channelId>4</channelId></Extension>",
	)
	.unwrap();
	assert_eq!(ext.channel_id, Some(4));
}

/// quick-xml skips unknown elements iteratively, so unknown nesting of
/// any depth parses without recursion. No `BcXml` field type is
/// recursive, so no modelled path can reach the 128-level cap.
#[test]
fn deeply_nested_input_does_not_overflow() {
	let depth = 10_000;
	let mut xml = String::from("<body>");
	xml.push_str(&"<a>".repeat(depth));
	xml.push_str(&"</a>".repeat(depth));
	xml.push_str("</body>");
	let parsed = BcXml::try_parse(xml.as_bytes());
	assert!(parsed.is_ok(), "unexpected parse error: {parsed:?}");
}

/// The 484 list arrives with either spelling of every name, and blocks
/// without an addressable id must drop out — the reference client skips
/// them, and an unaddressable chime would otherwise poison the pick.
#[test]
fn dingdong_list_decodes_both_spellings_and_skips_idless_blocks() {
	let sample = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <dingdongDeviceInfo version="1.1">
        <deviceId>1</deviceId>
        <deviceName>Carillon</deviceName>
        <netState>1</netState>
        </dingdongDeviceInfo>
        <dingdongDeviceInfo version="1.1">
        <id>2</id>
        <name>Second</name>
        <netstate>0</netstate>
        </dingdongDeviceInfo>
        <dingdongDeviceInfo version="1.1">
        <name>no id at all</name>
        </dingdongDeviceInfo>
        </body>
    "#
	);
	let parsed = BcXml::try_parse(sample.as_bytes()).unwrap();
	assert_eq!(parsed.dingdong_device_info.len(), 3);
	let first = &parsed.dingdong_device_info[0];
	assert_eq!(first.identifier(), Some("1"));
	assert_eq!(first.display_name(), Some("Carillon"));
	assert!(first.is_online());
	let second = &parsed.dingdong_device_info[1];
	assert_eq!(second.identifier(), Some("2"));
	assert_eq!(second.display_name(), Some("Second"));
	assert_eq!(second.net_state(), Some("0"));
	assert!(!second.is_online());
	// Skipped, not defaulted: neither tag, so no id is invented.
	assert_eq!(parsed.dingdong_device_info[2].identifier(), None);
}

/// The msg 484 reply of a `v3.0.0.4636_25060550` doorbell with a paired chime,
/// captured off the wire and copied here unedited. This exact block is what
/// this model used to turn into the empty `{}` published as
/// `status/chime/json`: `<dingdongList>` is a **container** whose entries sit
/// under `<pairedList>`. The test is pinned to the field text, not to what the
/// reference client's types imply.
#[test]
fn dingdong_list_container_decodes_the_field_capture() {
	let capture = indoc!(
		r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <dingdongList version="1.1">
        <maxPairNumber>5</maxPairNumber>
        <pairedList>
        <dingdongDeviceInfo>
        <id>882092649</id>
        <netstate>2</netstate>
        <name>Reolink Chime</name>
        </dingdongDeviceInfo>
        </pairedList>
        </dingdongList>
        </body>
    "#
	);
	let parsed = BcXml::try_parse(capture.as_bytes()).unwrap();
	let list = parsed.dingdong_list.as_ref().expect("the container itself decodes");
	assert_eq!(list.max_pair_number.as_deref(), Some("5"), "how many chimes it will pair");
	let chimes = parsed.dingdong_chimes();
	assert_eq!(chimes.len(), 1, "one paired chime — not the empty block we used to read");
	let chime = &chimes[0];
	assert_eq!(chime.identifier(), Some("882092649"), "the id that unlocks 485 / 609");
	assert_eq!(chime.display_name(), Some("Reolink Chime"));
	assert_eq!(chime.net_state(), Some("2"));
	// `2` is NOT the documented `1`. Kept as printed and called what it is:
	// this camera's way of describing a chime that answers its own per-chime
	// reads. Nothing here is allowed to quietly redefine the flag.
	assert!(!chime.is_online(), "2 must not be silently read as online");
}

/// The msg 486 reply of the same camera, same capture session: the block
/// arrives **wrapped in `<dingdongCfg>`** — the root the 487 write has always
/// used — carrying the chime's name and its own firmware beside the id. Read
/// only as bare `<deviceCfg>` elements at the body root, this reply decoded to
/// nothing, which is how a camera that had just enumerated its melodies was
/// reported as having none.
#[test]
fn dingdong_events_decode_the_wrapped_field_capture() {
	let capture = indoc!(
		r#"
        <body>
        <dingdongCfg version="1.1">
        <deviceCfg>
        <id>882092649</id>
        <name>Reolink Chime</name>
        <version>00A2</version>
        <alarminCfg><type>visitor</type><valid>1</valid><musicId>0</musicId></alarminCfg>
        <alarminCfg><type>people</type><valid>0</valid><musicId>0</musicId></alarminCfg>
        <alarminCfg><type>vehicle</type><valid>0</valid><musicId>0</musicId></alarminCfg>
        <alarminCfg><type>package</type><valid>0</valid><musicId>0</musicId></alarminCfg>
        </deviceCfg>
        </dingdongCfg>
        </body>"#
	);
	let parsed = BcXml::try_parse(capture.as_bytes()).unwrap();
	let events = parsed.dingdong_events();
	assert_eq!(events.len(), 1, "the wrapped reply IS the reply");
	assert!(parsed.device_cfg.is_empty(), "and the bare-root field stays untouched by it");
	let dev = &events[0];
	assert_eq!(dev.id.as_deref(), Some("882092649"));
	assert_eq!(dev.name.as_deref(), Some("Reolink Chime"));
	assert_eq!(dev.firm_version.as_deref(), Some("00A2"), "the child <version>, not the attribute");
	assert_eq!(dev.version, "", "the reply carries no version attribute on deviceCfg");
	let melodies = &dev.alarmin_cfg;
	assert_eq!(melodies.len(), 4);
	assert_eq!(melodies[0].type_.as_deref(), Some("visitor"));
	assert_eq!(melodies[0].state(), Some("1"), "the chime rings on a visit");
	assert_eq!(melodies[1].state(), Some("0"), "and not on a person");
	assert_eq!(melodies[0].music_id.as_deref(), Some("0"));
}

/// A 483 refusal arrives as `<result>`, next to a `<bopen>0</bopen>` that is
/// NOT a setting the operator made: on a battery doorbell with no wired chime
/// the camera answers `adptNotAC`. The reason is reported, and a write frame
/// built from such a read must not carry it back.
#[test]
fn dingdong_ctrl_reports_a_refusal_reason_without_returning_it() {
	let parsed = BcXml::try_parse(
		br#"<body><dingdongCtrl version="1.1"><opt>machineStateGet</opt><type>unknow</type><bopen>0</bopen><time>0</time><bsave>0</bsave><result>adptNotAC</result></dingdongCtrl></body>"#,
	)
	.unwrap();
	let ctrl = parsed.dingdong_ctrl.unwrap();
	assert_eq!(ctrl.result.as_deref(), Some("adptNotAC"));
	assert_eq!(ctrl.switch_state(), Some("0"));
	assert_eq!(ctrl.type_.as_deref(), Some("unknow"));
}

/// The 483 switch exists under two spellings and the read must record
/// which one it saw, because a write sends back only the spelling the
/// camera printed.
#[test]
fn dingdong_ctrl_accepts_bopen_and_enable_without_inventing_the_other() {
	let parsed = BcXml::try_parse(
		br#"<body><dingdongCtrl version="1.1"><enable>1</enable><time>30</time></dingdongCtrl></body>"#,
	)
	.unwrap();
	let ctrl = parsed.dingdong_ctrl.unwrap();
	assert_eq!(ctrl.enable.as_deref(), Some("1"));
	assert_eq!(ctrl.bopen, None, "a camera that said `enable` never gets a `bopen` back");
	assert_eq!(ctrl.switch_state(), Some("1"));
	assert_eq!(ctrl.time.as_deref(), Some("30"));

	let parsed = BcXml::try_parse(
		br#"<body><dingdongCtrl version="1.1"><type>dingdong</type><bopen>0</bopen></dingdongCtrl></body>"#,
	)
	.unwrap();
	let ctrl = parsed.dingdong_ctrl.unwrap();
	assert_eq!(ctrl.switch_state(), Some("0"));
	assert_eq!(ctrl.enable, None);

	// The write keeps the spelling and stamps the documented frame tags.
	let wire = String::from_utf8(BcXml::body_with_ding_dong_ctrl(&ctrl).serialize(vec![]).unwrap()).unwrap();
	assert!(wire.contains("<opt>machineStateSet</opt>"), "{wire}");
	assert!(wire.contains("<bopen>0</bopen>"), "{wire}");
	assert!(wire.contains("<bsave>1</bsave>"), "{wire}");
	assert!(!wire.contains("<enable>"), "must not add the spelling the camera didn't use: {wire}");
	assert!(!wire.contains("<time>"), "must not invent a tag the read omitted: {wire}");
}

/// The 486 reply repeats `<deviceCfg>`, each with repeated `<alarminCfg>`,
/// and the switch may arrive as `valid` or as `switch`.
#[test]
fn dingdong_device_cfg_decodes_repeated_blocks_and_both_switch_spellings() {
	let sample = r#"
        <body>
        <deviceCfg version="1.1">
        <id>1</id>
        <ringId>7</ringId>
        <alarminCfg version="1.1"><type>people</type><valid>1</valid><musicId>2</musicId></alarminCfg>
        <alarminCfg version="1.1"><type>visitor</type><switch>0</switch><musicId>0</musicId></alarminCfg>
        </deviceCfg>
        <deviceCfg version="1.1">
        <id>2</id>
        <alarminCfg version="1.1"><type>car</type><valid>0</valid></alarminCfg>
        </deviceCfg>
        </body>"#;
	let parsed = BcXml::try_parse(sample.as_bytes()).unwrap();
	assert_eq!(parsed.device_cfg.len(), 2);
	assert_eq!(parsed.device_cfg[0].id.as_deref(), Some("1"));
	assert_eq!(parsed.device_cfg[0].ring_id.as_deref(), Some("7"));
	let events = &parsed.device_cfg[0].alarmin_cfg;
	assert_eq!(events.len(), 2);
	assert_eq!(events[0].type_.as_deref(), Some("people"));
	assert_eq!(events[0].state(), Some("1"));
	assert_eq!(events[0].music_id.as_deref(), Some("2"));
	assert_eq!(events[1].state(), Some("0"));
	assert_eq!(events[1].valid, None, "the `switch` spelling must not be mirrored into `valid`");
	// A missing musicId stays missing — the melody of an entry the camera
	// didn't print is never implied.
	assert_eq!(parsed.device_cfg[1].alarmin_cfg[0].music_id, None);

	// The 487 request wraps exactly one block in <dingdongCfg>. The builder
	// passes the frame through untouched (same law as the other chime
	// builders), so the write path in `ding_dong.rs` builds its frame
	// WITHOUT the read-only `ringId` — verified there against the wire.
	let mut write_frame = parsed.device_cfg[0].clone();
	write_frame.ring_id = None;
	let wire =
		String::from_utf8(BcXml::body_with_ding_dong_cfg(&write_frame).serialize(vec![]).unwrap())
			.unwrap();
	assert!(wire.contains("<dingdongCfg"), "{wire}");
	assert!(wire.contains("<id>1</id>"), "{wire}");
	assert!(wire.contains("<musicId>2</musicId>"), "{wire}");
	assert!(!wire.contains("<ringId>"), "ringId is not part of the documented write frame: {wire}");
}

/// The 485 frames are exactly the documented tag sets: the read asks with
/// `<opt>getParam</opt><id>`, the silent read asks with `<id>`, and the
/// silent write stamps the hard-coded type but no `<opt>`.
#[test]
fn dingdong_request_frames_carry_only_the_documented_tags() {
	let wire = String::from_utf8(BcXml::body_ding_dong_opt_get("1").serialize(vec![]).unwrap()).unwrap();
	assert!(wire.contains("<dingdongDeviceOpt"), "{wire}");
	assert!(wire.contains("<opt>getParam</opt>"), "{wire}");
	assert!(wire.contains("<id>1</id>"), "{wire}");
	assert!(!wire.contains("<volLevel>"), "a read must not carry writable tags: {wire}");

	let wire = String::from_utf8(BcXml::body_ding_dong_ctrl_get().serialize(vec![]).unwrap()).unwrap();
	assert!(wire.contains("<opt>machineStateGet</opt>"), "{wire}");

	let wire = String::from_utf8(BcXml::body_ding_dong_silent_get("1").serialize(vec![]).unwrap()).unwrap();
	assert!(wire.contains("<dingdongSilentMode"), "{wire}");
	assert!(!wire.contains("<time>"), "the read carries only the id: {wire}");

	let wire = String::from_utf8(
		BcXml::body_with_ding_dong_silent(&DingDongSilent {
			version: xml_ver(),
			id: Some("1".into()),
			time: Some("0".into()),
			type_: None,
		})
		.serialize(vec![])
		.unwrap(),
	)
	.unwrap();
	assert!(wire.contains("<id>1</id>"), "{wire}");
	assert!(wire.contains("<time>0</time>"), "{wire}");
	assert!(wire.contains("<type>63</type>"), "the documented frame type must survive: {wire}");
}
