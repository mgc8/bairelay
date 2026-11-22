//! The camera's settings, block by block. The measurements behind these rules
//! live in the operator's private notebook, so every number is restated here.
//!
//! Every family follows the same law, taken from reading the reference
//! library (`Api.ts`: `setAudioCfg`, `setImage`, `setMask`, `setNtp`,
//! `setDst`, `setAutoReboot`…) and recalled in that document:
//!
//! 1. **The SET is the patched answer of the GET.** Every tag the
//!    camera printed goes back out; only the ones that changed are touched.
//!    The firmware "rejects (or silently cancels) a block that omits
//!    fields it sent itself" — an `Ok(())` on an amputated block is therefore a
//!    **false success**, worse than an error.
//! 2. **A tag the camera never printed is not invented.**
//!    That is why every field is an `Option` with
//!    `skip_serializing_if`: a camera with no intercom is not pushed into
//!    believing it has one.
//! 3. **Values stay camera-side text.** A setting that passes back through an
//!    integer may come out formatted differently (`01`, `+01`); text,
//!    on the contrary, round-trips to the byte.
//! 4. **Every write is followed by a re-read**, and it is the re-read
//!    that is returned — never the requested value. A `SET` acked with 200
//!    is not proof that the value was stored.
//!
//! No ability guard here, deliberately (as for the chime family): this firmware's
//! `abilityInfo` **under-declares** — the same family answers
//! `405` on `<HaCfg>` while accepting `<Email>`. An absent ability says
//! nothing about what the command returns; only the camera is evidence.

use std::sync::Arc;

use serde::Serialize;

use super::set_helpers::{SET_QUIRK_TIMEOUT, await_set_reply_with_quirk};
use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, inspect_xml, MockConnection,
};

/// The two blocks the image answer (26) brings together, and that setter
/// 25 asks for together: writing `bright` without sending `<InputAdvanceCfg>`
/// back is the amputation the firmware swallows in silence.
#[derive(PartialEq, Eq, Default, Debug, Clone, Serialize)]
pub struct ImageCfg {
	/// `<VideoInput>`: the five 0-255 sliders and day/night.
	pub video_input: VideoInput,
	/// `<InputAdvanceCfg>`: exposure, scene, mains frequency. `None`
	/// when the camera only printed `<VideoInput>` — in which case only that
/// block goes back to the 25.
	pub advance: Option<InputAdvanceCfg>,
}

/// The two blocks the OSD answer (44) brings together, and that the setter
/// 45 asks for together.
#[derive(PartialEq, Eq, Default, Debug, Clone, Serialize)]
pub struct OsdCfg {
	/// `<OsdDatetime>`: the timestamp on screen.
	pub datetime: OsdDatetime,
	/// `<OsdChannelName>`: the name on screen and its watermark.
	pub channel_name: Option<OsdChannelName>,
}

/// The motion-detection block, in **the shape the camera printed**.
///
/// The document describes a GET 46 as `<MdAlarm>` and a SET 47 as
/// `<MD version="1.1">`. Both exist; a given firmware prints
/// one or the other. One can only patch and send back **what** it
/// printed: `<MdAlarm>` has "free fields" nobody has
/// enumerated, so rebuilding it would amputate the block — hence the refusal of
/// [`crate::bc_protocol::BcCamera::set_motion_cfg`], which returns the write refused
/// instead of returning it falsely successful.
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum MotionCfg {
	/// The shape of setter 47: writing is possible.
	Md(MdCfg),
	/// The flat shape of getter 46: read-only.
	MdAlarm(MdAlarm),
}

impl MotionCfg {
	/// The activation state as the camera printed it.
	pub fn enable(&self) -> Option<&str> {
		match self {
			MotionCfg::Md(m) => m.enable.as_deref(),
			MotionCfg::MdAlarm(m) => m.sens_info_new.as_ref().and_then(|s| s.enable.as_deref()),
		}
	}

	/// The sensitivity as the camera printed it (0-50, higher =
	/// more sensitive, on firmwares that document a range).
	pub fn sensitivity(&self) -> Option<&str> {
		match self {
			MotionCfg::Md(m) => m.sensitivity_default.as_deref(),
			MotionCfg::MdAlarm(m) => m
				.sens_info_new
				.as_ref()
				.and_then(|s| s.sensitivity.as_deref()),
		}
	}

	/// Patches the activation in place, on the carrier that was read.
	pub fn set_enable(&mut self, value: impl Into<String>) {
		let value = Some(value.into());
		match self {
			MotionCfg::Md(m) => m.enable = value,
			MotionCfg::MdAlarm(m) => {
				m.sens_info_new
					.get_or_insert_with(SensInfoNew::default)
					.enable = value;
			}
		}
	}

	/// Patches the sensitivity in place, on the carrier that was read.
	pub fn set_sensitivity(&mut self, value: impl Into<String>) {
		let value = Some(value.into());
		match self {
			MotionCfg::Md(m) => m.sensitivity_default = value,
			MotionCfg::MdAlarm(m) => {
				m.sens_info_new
					.get_or_insert_with(SensInfoNew::default)
					.sensitivity = value;
			}
		}
	}
}

impl BcCamera {
	// ── Shared plumbing ─────────────────────────────────────────────────

	/// GET of a settings block: empty body (the document notes "no request"
	/// for these families), a one-way trip, and a non-200 that becomes
	/// [`Error::CameraRefused`] — "the camera refuses this command" is
	/// never "the setting is empty". The variant, and not its textless twin,
	/// because that refusal arrives **with** an answer from the camera: it is the
	/// one time the dialect explains its no.
	async fn raw_get(&self, msg_id: u32) -> Result<Bc> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(msg_id, msg_num).await?;
		// The shape of the request is no longer uniform: see
		// [`frame_shape`], measured with the MQTT probe on 13/11 on this
		// doorbell. Four reads changed their mind just from adding
		// the extension that names the channel.
		let (header_channel, canal_extension) = frame_shape(msg_id, self.channel_id);
		let req = Bc {
			meta: BcMeta {
				msg_id,
				channel_id: header_channel,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(match canal_extension {
				None => ModernMsg::default(),
				Some(channel) => {
					let mut ext = Extension::default();
					ext.channel_id = Some(channel);
					ModernMsg { extension: Some(ext), payload: None }
				}
			}),
		};
		sub.send(req).await?;
		let msg = sub.recv().await?;
		if msg.meta.response_code != 200 {
			// The refusal comes through here, and the body the camera wrote **in
			// answer to the refusal** went back into the void: `CameraServiceUnavailable`
			// carries only the code. On this doorbell, four families die
			// at this line and nobody had ever read what they get
			// back — measured on 13/11, after installing a `log` receiver
			// which revealed that the bridge printed none of it.
			//
			// One line per refusal, at `warn` level: it is the only clue that
			// distinguishes "wrong request shape" from "family absent from this
			// firmware" from "camera not woken up". The text is the one the
			// camera sent, not a paraphrase.
			let returned = match &msg.body {
				BcBody::ModernMsg(moderne) => {
					let extension = moderne
						.extension
						.as_ref()
						.map(text_extension)
						.unwrap_or_default();
					let body = match &moderne.payload {
						Some(BcPayloads::BcXml(bloc)) => text_xml(bloc),
						Some(BcPayloads::Binary(bytes)) => format!("<{} octets>", bytes.len()),
						None => "<no body>".to_string(),
					};
					format!("{extension} {body}")
				}
				other => format!("{other:?}"),
			};
			log::warn!(
				"msg {} refused in {} — camera answer: {}",
				msg.meta.msg_id,
				msg.meta.response_code,
				cut(&returned, 400)
			);
			// The text travels with the error, and not only in the log:
			// it is what the operator will read on `status/<family>/reason`,
			// cleaned and bounded a second time further up by `publishable_reason`.
			return Err(Error::CameraRefused {
				id: msg.meta.msg_id,
				code: msg.meta.response_code,
				detail: cut(&returned, 200),
			});
		}
		Ok(msg)
	}

	// ── The 400 probe ───────────────────────────────────────────────────

	/// A read request **hand-built**, for the forensics of the `400`s.
	///
	/// [`raw_get`](Self::raw_get) has one shape only: a 24-byte
	/// header, no extension and no body. When a family refuses, that single
	/// shape is the only variable we ever varied — that is to say
	/// none. This probe varies precisely what had never been measured:
	///
	/// * [`RawRequest::extension_channel`] — `<Extension><channelId>0</channelId></Extension>`,
	///   the shape **every** command this camera serves sends
	///   (audio 264, PIR 212, chime, AbilityInfo 151) and that none of the
	///   quatre familles en 400 n'envoie ;
	/// * [`RawRequest::extension_rf`] — `<rfId>`, the other addressing this
	///   firmware knows (the PIR's);
	/// * [`RawRequest::body`] — a body that **names the block asked for**, the
	///   shape the documentation attributes to other firmwares;
	/// * [`RawRequest::header_channel`] — byte 12 of the header, for the
	///   variantes NVR.
	///
	/// The body goes through [`BcXml`], therefore through the normal encryption: a
	/// variant sent as `BcPayloads::Binary` would leave **in clear**
	/// (`bc::ser::bc_payload_bytes` only encrypts XML payloads) and
	/// would confuse "wrong request shape" with "unencrypted body".
	///
	/// Nothing here retries or judges: the probe returns the code and what the
	/// camera wrote, and it is the operator who reads.
	pub async fn raw_exchange(&self, msg_id: u32, d: RawRequest) -> Result<RawResponse> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(msg_id, msg_num).await?;
		let mut ext = Extension::default();
		ext.channel_id = d.extension_channel;
		ext.rf_id = d.extension_rf;
		let meta = BcMeta {
			msg_id,
			channel_id: d.header_channel.unwrap_or(self.channel_id),
			msg_num,
			response_code: 0,
			stream_type: 0,
			class: d.header_class.unwrap_or(0x6414),
		};
		let req = match (ext.channel_id.is_some() || ext.rf_id.is_some(), d.body) {
			(true, Some(xml)) => Bc::new_from_ext_xml(meta, ext, xml),
			(true, None) => Bc::new_from_ext(meta, ext),
			(false, Some(xml)) => Bc::new_from_xml(meta, xml),
			(false, None) => Bc::new_from_meta(meta),
		};
		sub.send(req).await?;
		let msg = sub.recv().await?;
		let mut brut = RawResponse {
			code: msg.meta.response_code,
			msg_id: msg.meta.msg_id,
			classe_recue: msg.meta.class,
			kind: match &msg.body {
				BcBody::ModernMsg(_) => "modern".to_string(),
				BcBody::LegacyMsg(_) => "herite".to_string(),
			},
			..Default::default()
		};
		if let BcBody::ModernMsg(m) = &msg.body {
			brut.extension = m.extension.as_ref().map(|e| text_extension(e));
			brut.body = match &m.payload {
				Some(BcPayloads::BcXml(xml)) => Some(text_xml(xml)),
				Some(BcPayloads::Binary(bytes)) => {
					Some(format!("<{} octets binaires>", bytes.len()))
				}
				None => None,
			};
		}
		Ok(brut)
	}

	/// Pulls the requested block out of an answer. The `Bc` is returned as is when the
	/// block is not in it, so the caller can use it as an error anchor without
	/// having to clone it.
	fn settings_block<T>(
		msg: Bc,
		extract: impl FnOnce(&BcXml) -> Option<T>,
	) -> std::result::Result<T, Bc> {
		match &msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(xml)),
				..
			}) => match extract(xml) {
				Some(block) => Ok(block),
				None => Err(msg),
			},
			_ => Err(msg),
		}
	}

	/// SET of a settings block, with the quirk "some firmwares do not
	/// answer SETs": a non-200 is a refusal, the absence of an
	/// answer inside the window is a success **to be verified by the re-read**
	/// the caller performs right after.
	async fn raw_set(&self, msg_id: u32, xml: BcXml) -> Result<()> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(msg_id, msg_num).await?;
		// Same rule as for the read ([`frame_shape`]): the SET left with
		// the previous frame and the camera answered `400`, measured on 13/11 in
		// the evening by toggling motion detection from Home Assistant.
		let (header_channel, canal_extension) = frame_shape(msg_id, self.channel_id);
		let meta = BcMeta {
			msg_id,
			channel_id: header_channel,
			msg_num,
			response_code: 0,
			stream_type: 0,
			class: 0x6414,
		};
		let set = match canal_extension {
			None => Bc::new_from_xml(meta, xml),
			Some(channel) => {
				let mut ext = Extension::default();
				ext.channel_id = Some(channel);
				Bc::new_from_ext_xml(meta, ext, xml)
			}
		};
		sub.send(set).await?;
		await_set_reply_with_quirk(&mut sub, SET_QUIRK_TIMEOUT).await
	}

	// ── NTP (38 / 39) ───────────────────────────────────────────────────

	/// Reads `<Ntp>` (msg 38): activation, server, port and sync period.
	/// resynchronisation **en minutes**.
	pub async fn get_ntp(&self) -> Result<Ntp> {
		let msg = self.raw_get(MSG_ID_GET_NTP).await?;
		Self::settings_block(msg, |xml| xml.ntp.clone()).map_err(|msg| Error::UnintelligibleReply {
			reply: Arc::new(msg),
			why: "Expected Ntp xml but it was not received",
		})
	}

	/// Writes `<Ntp>` (msg 39) then re-reads the block (38) and returns **what the
	/// camera stored afterwards**. `cfg` must come from [`get_ntp`](Self::get_ntp)
	/// with only the intended tags touched.
	pub async fn set_ntp(&self, cfg: &Ntp) -> Result<Ntp> {
		self.raw_set(
			MSG_ID_SET_NTP,
			BcXml {
				ntp: Some(cfg.clone()),
				..Default::default()
			},
		)
		.await?;
		self.get_ntp().await
	}

	// ── Automatic reboot (101 / 100) ────────────────────────────────────

	/// Reads `<AutoReboot>` (msg 101). Mind the asymmetry of the ids: here the
	/// setter is the **smaller** of the two.
	pub async fn get_auto_reboot(&self) -> Result<AutoReboot> {
		let msg = self.raw_get(MSG_ID_GET_AUTO_REBOOT).await?;
		Self::settings_block(msg, |xml| xml.auto_reboot.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected AutoReboot xml but it was not received",
			})
	}

	/// Writes `<AutoReboot>` (msg 100) then re-reads (101). A `weekDay` outside
	/// `Sunday`…`Saturday` / `everyday` is not validated here: it is the text
	/// the camera printed that goes back, and it is the camera that decides.
	pub async fn set_auto_reboot(&self, cfg: &AutoReboot) -> Result<AutoReboot> {
		self.raw_set(
			MSG_ID_SET_AUTO_REBOOT,
			BcXml {
				auto_reboot: Some(cfg.clone()),
				..Default::default()
			},
		)
		.await?;
		self.get_auto_reboot().await
	}

	// ── Microphone denoise (439 / 440) ──────────────────────────────────

	/// Reads `<aiDenoise>` (msg 439): activation and `1`-`100` force of the
	/// denoiser applied to the microphone. The protocol has **no** microphone gain nor
	/// microphone mute; this is the closest thing to it.
	pub async fn get_ai_denoise(&self) -> Result<AiDenoise> {
		let msg = self.raw_get(MSG_ID_GET_AI_DENOISE).await?;
		Self::settings_block(msg, |xml| xml.ai_denoise.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected aiDenoise xml but it was not received",
			})
	}

	/// Writes `<aiDenoise>` (msg 440) then re-reads (439). The case of the version
	/// attribute goes back as it arrived — capital `Version` as the reference
	/// library documents it, or lowercase `version`.
	pub async fn set_ai_denoise(&self, cfg: &AiDenoise) -> Result<AiDenoise> {
		self.raw_set(
			MSG_ID_SET_AI_DENOISE,
			BcXml {
				ai_denoise: Some(cfg.clone()),
				..Default::default()
			},
		)
		.await?;
		self.get_ai_denoise().await
	}

	// ── Sleep (574) and Wi-Fi signal (115) — read-only ──────────────────

	/// Reads `<sleepState>` (msg 574): is the battery camera asleep?
	///
	/// Read-only verdict: no setter is documented, and putting a battery device
	/// to sleep through a settings write is not this bridge's role.
	/// It is also the only way to know **without waking it up**.
	pub async fn get_sleep_state(&self) -> Result<SleepState> {
		let msg = self.raw_get(MSG_ID_GET_SLEEP_STATE).await?;
		Self::settings_block(msg, |xml| xml.sleep_state.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected sleepState xml but it was not received",
			})
	}

	/// Reads `<WifiSignal>` (msg 115). The unit of `<signal>` is documented
	/// nowhere (dBm or %), so the text is relayed as is and no
	/// unit is claimed towards Home Assistant.
	///
	/// `<Wifi>` (116) is **not** exposed: the firmware may return the access
	/// point's key in it, and an MQTT state is readable by the whole broker.
	pub async fn get_wifi_signal(&self) -> Result<WifiSignal> {
		let msg = self.raw_get(MSG_ID_GET_WIFI_SIGNAL).await?;
		Self::settings_block(msg, |xml| xml.wifi_signal.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected WifiSignal xml but it was not received",
			})
	}

	// ── Image (26 / 25) ─────────────────────────────────────────────────

	/// Reads the image blocks (msg 26): `<VideoInput>` and, most often in
	/// the same answer, `<InputAdvanceCfg>`.
	///
	/// A 200 answer that brings **neither** of the two is
	/// [`Error::UnintelligibleReply`]: "the camera said nothing" must
	/// never be read as "the sliders are at zero".
	pub async fn get_image_cfg(&self) -> Result<ImageCfg> {
		let msg = self.raw_get(MSG_ID_GET_IMAGE).await?;
		let (video_input, advance) = match &msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(xml)),
				..
			}) => (xml.video_input.clone(), xml.input_advance_cfg.clone()),
			_ => (None, None),
		};
		Ok(ImageCfg {
			video_input: video_input.ok_or_else(|| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected VideoInput xml but it was not received",
			})?,
			advance,
		})
	}

	/// Writes the image blocks (msg 25) — **both of them**, then re-reads (26) and
	/// returns the re-read.
	pub async fn set_image_cfg(&self, cfg: &ImageCfg) -> Result<ImageCfg> {
		self.raw_set(
			MSG_ID_SET_IMAGE,
			BcXml {
				video_input: Some(cfg.video_input.clone()),
				input_advance_cfg: cfg.advance.clone(),
				..Default::default()
			},
		)
		.await?;
		self.get_image_cfg().await
	}

	// ── OSD (44 / 45) ───────────────────────────────────────────────────

	/// Reads the OSD blocks (msg 44): `<OsdDatetime>` and `<OsdChannelName>`.
	///
	/// The OSD exists **only** there: the reference library's historical bug
	/// (reading the OSD on 26, writing `<Osd>` on 25) produced writes
	/// that were never applied — it is not replayed here.
	pub async fn get_osd_cfg(&self) -> Result<OsdCfg> {
		let msg = self.raw_get(MSG_ID_GET_OSD).await?;
		let (datetime, channel_name) = match &msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(xml)),
				..
			}) => (xml.osd_datetime.clone(), xml.osd_channel_name.clone()),
			_ => (None, None),
		};
		Ok(OsdCfg {
			datetime: datetime.ok_or_else(|| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected OsdDatetime xml but it was not received",
			})?,
			channel_name,
		})
	}

	/// Writes the OSD blocks (msg 45), the two together, then re-reads (44).
	/// `<topLeftX>`/`<topLeftY>` are normalised 16.16 values: they go back
	/// as they arrived, never recomputed.
	pub async fn set_osd_cfg(&self, cfg: &OsdCfg) -> Result<OsdCfg> {
		self.raw_set(
			MSG_ID_SET_OSD,
			BcXml {
				osd_datetime: Some(cfg.datetime.clone()),
				osd_channel_name: cfg.channel_name.clone(),
				..Default::default()
			},
		)
		.await?;
		self.get_osd_cfg().await
	}

	// ── Motion detection (46 / 47) ──────────────────────────────────────

	/// Reads motion detection (msg 46), in the shape the camera
	/// printed: [`MotionCfg::Md`] if it answered `<MD>`,
	/// [`MotionCfg::MdAlarm`] if it answered `<MdAlarm>`.
	pub async fn get_motion_cfg(&self) -> Result<MotionCfg> {
		let msg = self.raw_get(MSG_ID_GET_MOTION_DETECT).await?;
		let (md, md_alarm) = match &msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(xml)),
				..
			}) => (xml.md.clone(), xml.md_alarm.clone()),
			_ => (None, None),
		};
		if let Some(m) = md {
			return Ok(MotionCfg::Md(m));
		}
		if let Some(m) = md_alarm {
			return Ok(MotionCfg::MdAlarm(m));
		}
		Err(Error::UnintelligibleReply {
			reply: Arc::new(msg),
			why: "Expected MD or MdAlarm xml but neither was received",
		})
	}

	/// Writes motion detection (msg 47) then re-reads (46).
	///
	/// Only writes the `<MD>` carrier: that is the shape the setter documents.
	/// On a `<MdAlarm>` carrier, returns [`Error::Usage`] **without sending an
	/// octet of radio** — rebuilding that block would drop its "free fields", and the
	/// firmware would take that in silence.
	pub async fn set_motion_cfg(&self, cfg: &MotionCfg) -> Result<MotionCfg> {
		match cfg {
			MotionCfg::Md(m) => {
				self.raw_set(
					MSG_ID_SET_MOTION_DETECT,
					BcXml {
						md: Some(m.clone()),
						..Default::default()
					},
				)
				.await?;
				self.get_motion_cfg().await
			}
			MotionCfg::MdAlarm(_) => Err(Error::Usage(
				"refusing to write motion detection: the camera answered 46 with <MdAlarm>, \
				 whose unenumerated fields a rewritten block would drop; only <MD> is writable"
					.to_string(),
			)),
		}
	}

	// ── Privacy masks (52) — read-only ──────────────────────────────────

	/// Reads `<Shelter>` (msg 52): whether masking is on and how many
	/// slots the model offers.
	///
	/// **No setter 53**: the child tags of `shelterList` (and the
	/// `pixel << 16 | canvas` encoding of the coordinates) have no local
	/// capture, and returning a `<Shelter>` amputated of its masks is exactly the
	/// write the firmware swallows without a word. The refusal is therefore in
	/// the design, not in the call.
	pub async fn get_privacy_masks(&self) -> Result<Shelter> {
		let msg = self.raw_get(MSG_ID_GET_PRIVACY_MASK).await?;
		Self::settings_block(msg, |xml| xml.shelter.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Expected Shelter xml but it was not received",
			})
	}

	// ── Daylight saving (106 / 107) ─────────────────────────────────────

	/// Writes `<Dst>` (msg 107) then re-reads (106) and returns the re-read.
	///
	/// `cfg` must come from [`get_dst`](Self::get_dst): the calendar
/// (`startMonth`, `startWeekIndex`, `startWeekday`, …) goes back intact, only
/// the touched tag changes.
	pub async fn set_dst(&self, cfg: &Dst) -> Result<Dst> {
		self.raw_set(
			MSG_ID_SET_DST,
			BcXml {
				dst: Some(cfg.clone()),
				..Default::default()
			},
		)
		.await?;
		self.get_dst().await
	}

	// ── System (104 / 105) ──────────────────────────────────────────────

	/// Lit `<SystemGeneral>` (msg 104) : fuseau, formats d'affichage, langue,
	/// the device's system name.
	///
	/// The system name is **not** the name on screen
	/// (`<OsdChannelName><name>`, msg 45) nor the camera's MQTT name: those are
	/// three different things.
	pub async fn get_general(&self) -> Result<SystemGeneral> {
		let msg = self.raw_get(MSG_ID_GET_GENERAL).await?;
		Self::settings_block(msg, |xml| xml.system_general.clone())
			.map_err(|msg| Error::UnintelligibleReply {
				reply: Arc::new(msg),
				why: "Reply did not contain a SystemGeneral block",
			})
	}

	/// Writes `<SystemGeneral>` (msg 105) **without touching the clock**, then re-reads
	/// (104) and returns the re-read.
	///
	/// Two markers imposed by the firmware (measured, and the payloads confirmed
	/// by pcap) are set here and nowhere else:
	///
	/// * `<year>0</year>` = "do not re-set the manual time". Without it the
	///   camera re-sets the clock from the block's `year…second` fields, which
	///   a language or name change has no business doing.
	/// * `<deviceNameOnly>1</deviceNameOnly>` = "only the name changes", required
	///   when the patch touches `deviceName` alone. It is **not** set when
	///   the patch also touches something else, because that would lie to the
	///   firmware about the payload's contents.
	pub async fn set_general(&self, cfg: &SystemGeneral) -> Result<SystemGeneral> {
		let mut out = cfg.clone();
		out.year = Some(0);
		let only_name = out.device_name.is_some()
			&& out.time_zone.is_none()
			&& out.osd_format.is_none()
			&& out.time_format.is_none()
			&& out.language.is_none();
		if only_name {
			out.device_name_only = Some(1);
		}
		self.raw_set(
			MSG_ID_SET_GENERAL,
			BcXml {
				system_general: Some(out),
				..Default::default()
			},
		)
		.await?;
		self.get_general().await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// An `<aiDenoise>` block as the reference library prints it: version attribute
	/// **capital**, `level` between 1 and 100.
	fn denoise_block() -> AiDenoise {
		AiDenoise {
			version_caps: Some("1.1".into()),
			version: None,
			enable: Some("1".into()),
			level: Some("80".into()),
			channel_id: None,
		}
	}

	fn ntp_block() -> Ntp {
		Ntp {
			version: "1.1".into(),
			channel_id: Some("0".into()),
			enable: Some("1".into()),
			server: Some("pool.ntp.org".into()),
			port: Some("123".into()),
			synchronize_interval: Some("1440".into()),
			time_zone: Some("-3600".into()),
		}
	}

	fn ntp_reply(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				ntp: Some(ntp_block()),
				..Default::default()
			},
		)
	}

	// ── NTP ─────────────────────────────────────────────────────────────

	#[tokio::test]
	async fn get_ntp_relays_the_camera_text() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(ntp_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let ntp = cam.get_ntp().await.expect("ok");
		assert_eq!(ntp.server.as_deref(), Some("pool.ntp.org"));
		assert_eq!(ntp.enable.as_deref(), Some("1"));
		assert_eq!(ntp.synchronize_interval.as_deref(), Some("1440"));
		assert_eq!(ntp.time_zone.as_deref(), Some("-3600"));
	}

	#[tokio::test]
	async fn get_ntp_tells_refusal_apart_from_empty() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_ntp().await.expect_err("400");
		assert!(
			matches!(
				err,
				Error::CameraRefused { code: 400, .. }
					| Error::CameraServiceUnavailable { code: 400, .. }
			),
			"a refusal must stay a refusal, not an empty block: {err:?}"
		);
		// And since 13/11, a refused settings read carries the text the
		// camera wrote under its code — it is the only trace that says
		// "wrong shape" or "family absent".
		if let Error::CameraRefused { detail, .. } = &err {
			assert!(!detail.is_empty(), "the read refusal must carry the camera answer");
		}
	}

	#[tokio::test]
	async fn get_ntp_200_without_block_is_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert!(matches!(
			cam.get_ntp().await.expect_err("no block"),
			Error::UnintelligibleReply { .. }
		));
	}

	#[tokio::test]
	async fn set_ntp_returns_the_reread_not_the_request() {
		// Stubborn camera: it acks the 39 but keeps its server. The value
		// returned must be the re-read's.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_NTP)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let ntp = sent.ntp.as_ref().expect("the SET carries <Ntp>");
				assert_eq!(
					ntp.server.as_deref(),
					Some("time.nist.gov"),
					"the patched tag must go back out"
				);
				assert_eq!(
					ntp.synchronize_interval.as_deref(),
					Some("1440"),
					"an untouched tag must go back out as it stands"
				);
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(ntp_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut want = ntp_block();
		want.server = Some("time.nist.gov".into());
		let after = cam.set_ntp(&want).await.expect("ok");
		assert_eq!(
			after.server.as_deref(),
			Some("pool.ntp.org"),
			"the re-read is the evidence, never the request"
		);
	}

	#[tokio::test]
	async fn a_rejected_ntp_set_does_not_reread() {
		// The script deliberately plans no 38: a re-read after a
		// refusal would fail the mock — that is the intended assertion (an octet of
		// radio saved on a battery device).
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_NTP)
			.reply_with(|req| reply_err_code(req, 409))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.set_ntp(&ntp_block()).await.expect_err("409");
		assert!(
			matches!(err, Error::CameraServiceUnavailable { code: 409, .. }),
			"{err:?}"
		);
	}

	// ── Microphone denoise ──────────────────────────────────────────────

	#[tokio::test]
	async fn get_ai_denoise_accepts_the_capitalised_version() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AI_DENOISE)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ai_denoise: Some(denoise_block()),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let d = cam.get_ai_denoise().await.expect("ok");
		assert_eq!(d.version_caps.as_deref(), Some("1.1"));
		assert_eq!(d.level.as_deref(), Some("80"));
	}

	#[tokio::test]
	async fn set_ai_denoise_returns_the_case_as_read_without_inventing_the_other() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_AI_DENOISE)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let d = sent.ai_denoise.as_ref().expect("<aiDenoise>");
				assert_eq!(d.version_caps.as_deref(), Some("1.1"));
				assert!(
					d.version.is_none(),
					"the unused version slot must not be invented"
				);
				assert_eq!(d.level.as_deref(), Some("80"));
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_AI_DENOISE)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ai_denoise: Some(denoise_block()),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut want = denoise_block();
		want.enable = Some("0".into());
		let after = cam.set_ai_denoise(&want).await.expect("ok");
		// The scripted camera did not move: this is indeed its re-read.
		assert_eq!(after.enable.as_deref(), Some("1"));
	}

	// ── Image: the two blocks together ──────────────────────────────────

	fn video_input_block() -> VideoInput {
		VideoInput {
			version: "1.1".into(),
			channel_id: Some("0".into()),
			bright: Some("128".into()),
			contrast: Some("130".into()),
			saturation: Some("120".into()),
			hue: Some("128".into()),
			sharpen: Some("60".into()),
			ir_cut_swap: Some("0".into()),
			day_night: Some("auto".into()),
			corridor_ability: Some("0".into()),
			corridor_mode: None,
		}
	}

	fn advance_block() -> InputAdvanceCfg {
		InputAdvanceCfg {
			version: "1.1".into(),
			channel_id: Some("0".into()),
			exposure: Some(ExposureCfg {
				mode: Some("auto".into()),
			}),
			scene: Some("normal".into()),
			power_line_frequency: Some("50".into()),
			digital_channel: None,
		}
	}

	fn image_reply(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				video_input: Some(video_input_block()),
				input_advance_cfg: Some(advance_block()),
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn get_image_cfg_brings_both_blocks() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(image_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_image_cfg().await.expect("ok");
		assert_eq!(cfg.video_input.bright.as_deref(), Some("128"));
		assert_eq!(cfg.video_input.day_night.as_deref(), Some("auto"));
		assert_eq!(
			cfg.advance
				.as_ref()
				.and_then(|a| a.exposure.as_ref())
				.and_then(|e| e.mode.as_deref()),
			Some("auto")
		);
	}

	#[tokio::test]
	async fn set_image_cfg_sends_both_blocks_back_at_25() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_IMAGE)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				assert!(
					sent.video_input.is_some() && sent.input_advance_cfg.is_some(),
					"25 must receive VideoInput AND InputAdvanceCfg"
				);
				let v = sent.video_input.as_ref().expect("<VideoInput>");
				assert_eq!(v.sharpen.as_deref(), Some("90"), "the patched slider goes back");
				assert_eq!(v.bright.as_deref(), Some("128"), "the others go back");
				assert!(
					v.corridor_mode.is_none(),
					"a tag absent from the GET is not invented"
				);
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(image_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = ImageCfg {
			video_input: video_input_block(),
			advance: Some(advance_block()),
		};
		cfg.video_input.sharpen = Some("90".into());
		let after = cam.set_image_cfg(&cfg).await.expect("ok");
		assert_eq!(
			after.video_input.sharpen.as_deref(),
			Some("60"),
			"the re-read is the evidence"
		);
	}

	#[tokio::test]
	async fn get_image_cfg_200_without_block_is_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert!(matches!(
			cam.get_image_cfg().await.expect_err("no block"),
			Error::UnintelligibleReply { .. }
		));
	}

	// ── OSD ─────────────────────────────────────────────────────────────

	fn osd_cfg() -> OsdCfg {
		OsdCfg {
			datetime: OsdDatetime {
				version: "1.1".into(),
				channel_id: Some("0".into()),
				enable: Some("1".into()),
				top_left_x: Some("1".into()),
				top_left_y: Some("1".into()),
				width: Some("602".into()),
				height: Some("91".into()),
				language: Some("English".into()),
			},
			channel_name: Some(OsdChannelName {
				version: "1.1".into(),
				channel_id: Some("0".into()),
				name: Some("Porch".into()),
				enable: Some("0".into()),
				top_left_x: Some("1".into()),
				top_left_y: Some("65535".into()),
				en_watermark: Some("0".into()),
				en_bgcolor: Some("0".into()),
			}),
		}
	}

	fn osd_reply(req: &Bc) -> Bc {
		let cfg = osd_cfg();
		reply_200_xml(
			req,
			BcXml {
				osd_datetime: Some(cfg.datetime),
				osd_channel_name: cfg.channel_name,
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn get_osd_cfg_brings_both_blocks() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_OSD)
			.reply_with(osd_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_osd_cfg().await.expect("ok");
		assert_eq!(cfg.datetime.enable.as_deref(), Some("1"));
		assert_eq!(
			cfg.channel_name.as_ref().and_then(|c| c.name.as_deref()),
			Some("Porch")
		);
	}

	#[tokio::test]
	async fn set_osd_cfg_sends_both_blocks_back_without_recomputing_coordinates() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_OSD)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let dn = sent
					.osd_datetime
					.as_ref()
					.expect("45 receives <OsdDatetime>");
				let cn = sent
					.osd_channel_name
					.as_ref()
					.expect("45 receives <OsdChannelName>");
				assert_eq!(dn.enable.as_deref(), Some("0"), "the patched tag");
				assert_eq!(dn.top_left_x.as_deref(), Some("1"));
				assert_eq!(dn.top_left_y.as_deref(), Some("1"));
				assert_eq!(cn.top_left_y.as_deref(), Some("65535"), "the 16.16 values go back");
				assert_eq!(cn.name.as_deref(), Some("Porch"), "the name goes back");
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_OSD)
			.reply_with(osd_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = osd_cfg();
		cfg.datetime.enable = Some("0".into());
		let after = cam.set_osd_cfg(&cfg).await.expect("ok");
		assert_eq!(after.datetime.enable.as_deref(), Some("1"));
	}

	// ── Motion detection: the two carriers ──────────────────────────────

	fn md_block() -> MdCfg {
		MdCfg {
			version: "1.1".into(),
			channel_id: Some("0".into()),
			enable: Some("1".into()),
			usepir: Some("0".into()),
			width: Some("60".into()),
			height: Some("33".into()),
			scope: Some(MdScope {
				columns: Some("96".into()),
				rows: Some("64".into()),
				value_table: Some("AAAA".into()),
			}),
			sensitivity_default: Some("30".into()),
		}
	}

	fn md_reply(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				md: Some(md_block()),
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn motion_on_md_writes_without_damaging_the_zones() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(md_reply)
			.expect_msg(MSG_ID_SET_MOTION_DETECT)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let m = sent.md.as_ref().expect("47 receives <MD>");
				assert_eq!(m.enable.as_deref(), Some("0"));
				assert_eq!(
					m.scope.as_ref().and_then(|s| s.value_table.as_deref()),
					Some("AAAA"),
					"the zones bitmap must not be damaged"
				);
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(md_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = cam.get_motion_cfg().await.expect("ok");
		assert_eq!(cfg.enable(), Some("1"));
		assert_eq!(cfg.sensitivity(), Some("30"));
		cfg.set_enable("0");
		let after = cam.set_motion_cfg(&cfg).await.expect("ok");
		assert_eq!(after.enable(), Some("1"), "the re-read is the evidence");
	}

	#[test]
	fn the_frame_shape_is_bounded_to_the_four_measured_families() {
		// These four families refused without saying anything; the 13/11 measurement says
		// they need the channel, in the extension and in the header — in
		// read, and in write by symmetry (the `47` refuses without it).
		for id in [
			MSG_ID_GET_IMAGE,
			MSG_ID_SET_IMAGE,
			MSG_ID_GET_OSD,
			MSG_ID_SET_OSD,
			MSG_ID_GET_MOTION_DETECT,
			MSG_ID_SET_MOTION_DETECT,
			MSG_ID_GET_PRIVACY_MASK,
			MSG_ID_SET_PRIVACY_MASK,
		] {
			assert_eq!(
				frame_shape(id, 7),
				(0, Some(0)),
				"msg {id} must leave with channel 0 everywhere"
			);
		}
		// And what already answers does not move: no measurement says these
		// families would accept the new frame.
		for id in [
			MSG_ID_GET_NTP,
			MSG_ID_SET_NTP,
			MSG_ID_GET_GENERAL,
			MSG_ID_BATTERY_INFO,
		] {
			assert_eq!(
				frame_shape(id, 7),
				(7, None),
				"msg {id} must keep today's frame"
			);
		}
	}

	#[tokio::test]
	async fn writing_motion_carries_the_channel_like_the_read() {
		// Home Assistant used to flip the switch with no effect: the `47` left
		// without the extension and the camera answered `400`. The SET must therefore leave
		// with the same shape as the GET — and it is the re-read, already in
		// `set_motion_cfg`, that will say whether the write took.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(md_reply)
			.expect_msg(MSG_ID_SET_MOTION_DETECT)
			.reply_with(|req| {
				let BcBody::ModernMsg(msg) = &req.body else {
					panic!("the doorbell is on the modern frame");
				};
				assert_eq!(
					msg.extension.as_ref().and_then(|e| e.channel_id),
					Some(0),
					"the 47 without extension is refused 400: the switch does not flip"
				);
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(md_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = cam.get_motion_cfg().await.expect("lu");
		cfg.set_enable("0");
		cam.set_motion_cfg(&cfg).await.expect("written");
	}

	#[tokio::test]
	async fn motion_46_replies_only_on_the_channel_asked() {
		// The 46 answers 400 **with no body** as long as the request does not name
		// the channel. This test carries the shape on the real sending chain, not
		// only on the table: a correct table applied wrongly would not be seen,
		// the camera never saying why it refuses.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(|req| {
				let BcBody::ModernMsg(msg) = &req.body else {
					panic!("the doorbell is on the modern frame");
				};
				assert_eq!(
					msg.extension.as_ref().and_then(|e| e.channel_id),
					Some(0),
					"without the channel in the extension, the camera answers 400 with no body"
				);
				assert_eq!(req.meta.channel_id, 0, "the header must carry the same channel");
				md_reply(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_motion_cfg().await.expect("lu");
		assert_eq!(cfg.enable(), Some("1"));
	}

	#[tokio::test]
	async fn motion_on_md_alarm_refusal_sends_nothing() {
		// The script plans ONLY the 46: sending a 47 would fail the
		// mock, which is exactly the intended assertion.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						md_alarm: Some(MdAlarm {
							version: "1.1".into(),
							channel_id: Some("0".into()),
							sens_info_new: Some(SensInfoNew {
								enable: Some("1".into()),
								sensitivity: Some("3".into()),
								alarm_type: Some("MdAlarm".into()),
							}),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = cam.get_motion_cfg().await.expect("ok");
		assert!(matches!(cfg, MotionCfg::MdAlarm(_)));
		assert_eq!(cfg.sensitivity(), Some("3"));
		cfg.set_enable("0");
		let err = cam.set_motion_cfg(&cfg).await.expect_err("refused");
		match err {
			Error::Usage(msg) => {
				assert!(msg.contains("MdAlarm"), "the refusal names the tag read: {msg}")
			}
			other => panic!("attendu Usage, obtenu {other:?}"),
		}
	}

	#[tokio::test]
	async fn motion_200_without_carrier_is_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert!(matches!(
			cam.get_motion_cfg().await.expect_err("no block"),
			Error::UnintelligibleReply { .. }
		));
	}

	// ── Veille, Wi-Fi, masques ──────────────────────────────────────────

	#[tokio::test]
	async fn sleep_state_and_wifi_signal_are_read_back() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_SLEEP_STATE)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						sleep_state: Some(SleepState {
							version: "1.1".into(),
							sleep: Some("1".into()),
							mode: Some("2".into()),
							pan_pos: None,
							tilt_pos: None,
							image_name: Some("sleep.jpg".into()),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_WIFI_SIGNAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						wifi_signal: Some(WifiSignal {
							version: "1.1".into(),
							channel_id: Some("0".into()),
							signal: Some("-67".into()),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert_eq!(
			cam.get_sleep_state().await.expect("s").sleep.as_deref(),
			Some("1")
		);
		assert_eq!(
			cam.get_wifi_signal().await.expect("w").signal.as_deref(),
			Some("-67")
		);
	}

	#[tokio::test]
	async fn masks_are_read_back() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_PRIVACY_MASK)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						shelter: Some(Shelter {
							version: "1.1".into(),
							channel_id: Some("0".into()),
							enable: Some("0".into()),
							max_num: Some("4".into()),
							track_enable: None,
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let s = cam.get_privacy_masks().await.expect("ok");
		assert_eq!(s.max_num.as_deref(), Some("4"));
		assert_eq!(s.enable.as_deref(), Some("0"));
	}

	// ── Automatic reboot ────────────────────────────────────────────────

	fn reboot_block() -> AutoReboot {
		AutoReboot {
			version: "1.1".into(),
			enable: Some("1".into()),
			week_day: Some("everyday".into()),
			hour: Some("3".into()),
			minute: Some("30".into()),
			second: Some("0".into()),
		}
	}

	fn reboot_reply(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				auto_reboot: Some(reboot_block()),
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn auto_reboot_writes_the_right_id_and_rereads() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUTO_REBOOT)
			.reply_with(reboot_reply)
			.expect_msg(MSG_ID_SET_AUTO_REBOOT)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let a = sent
					.auto_reboot
					.as_ref()
					.expect("100 receives <AutoReboot>");
				assert_eq!(a.enable.as_deref(), Some("0"));
				assert_eq!(a.hour.as_deref(), Some("3"), "the untouched hour goes back");
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_AUTO_REBOOT)
			.reply_with(reboot_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut cfg = cam.get_auto_reboot().await.expect("ok");
		cfg.enable = Some("0".into());
		let after = cam.set_auto_reboot(&cfg).await.expect("ok");
		assert_eq!(after.enable.as_deref(), Some("1"));
		assert_eq!(after.week_day.as_deref(), Some("everyday"));
	}

	// ── Daylight saving ─────────────────────────────────────────────────

	#[tokio::test]
	async fn set_dst_rereads_the_106_after_the_107() {
		let dst = Dst {
			version: "1.1".into(),
			enable: Some(1),
			offset: Some(1),
			start_month: Some(3),
			..Default::default()
		};
		let again = dst.clone();
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_DST)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let d = sent.dst.as_ref().expect("107 receives <Dst>");
				assert_eq!(d.enable, Some(0), "the patched switch goes back");
				assert_eq!(d.start_month, Some(3), "the calendar goes back");
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(move |req| {
				reply_200_xml(
					req,
					BcXml {
						dst: Some(again.clone()),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let mut want = dst.clone();
		want.enable = Some(0);
		let after = cam.set_dst(&want).await.expect("ok");
		assert_eq!(after.enable, Some(1), "the camera kept its activation");
	}

	// ── System ──────────────────────────────────────────────────────────

	fn general_block() -> SystemGeneral {
		SystemGeneral {
			version: "1.1".into(),
			time_zone: Some(-3600),
			year: Some(2026),
			month: Some(11),
			day: Some(11),
			hour: Some(18),
			minute: Some(35),
			second: Some(4),
			osd_format: Some("DMY".into()),
			time_format: Some(0),
			language: Some("English".into()),
			device_name: Some("Porch".into()),
			..Default::default()
		}
	}

	fn general_reply(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				system_general: Some(general_block()),
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn set_general_touches_no_clock_and_marks_the_rename_only() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let g = sent
					.system_general
					.as_ref()
					.expect("105 receives <SystemGeneral>");
				assert_eq!(g.year, Some(0), "<year>0</year> is mandatory");
				assert_eq!(
					g.device_name_only,
					Some(1),
					"a rename alone carries <deviceNameOnly>1</deviceNameOnly>"
				);
				assert_eq!(g.device_name.as_deref(), Some("Entrance"));
				assert_eq!(g.language.as_deref(), None, "nothing else is added");
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(general_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let want = SystemGeneral {
			version: "1.1".into(),
			device_name: Some("Entrance".into()),
			..Default::default()
		};
		let after = cam.set_general(&want).await.expect("ok");
		assert_eq!(after.device_name.as_deref(), Some("Porch"), "re-read");
		assert_eq!(after.time_zone, Some(-3600));
	}

	#[tokio::test]
	async fn set_general_marks_a_rename_only_when_nothing_else_changed() {		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with(|req| {
				let sent = inspect_xml(req);
				let g = sent.system_general.as_ref().expect("<SystemGeneral>");
				assert_eq!(
					g.device_name_only, None,
					"marking 'name only' while the language changes too lies to the firmware"
				);
				assert_eq!(g.year, Some(0));
				assert_eq!(g.language.as_deref(), Some("French"));
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(general_reply)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let want = SystemGeneral {
			version: "1.1".into(),
			device_name: Some("Entrance".into()),
			language: Some("French".into()),
			..Default::default()
		};
		cam.set_general(&want).await.expect("ok");
	}

	/// What the wire really contains. The tests above re-read the XML
	/// deserialised; this one looks at the **octet stream**, because it is he
	/// the firmware will judge: tag case (`<MD>` not `<Md>`,
	/// `<aiDenoise Version>` capital), and above all the total absence of any tag
	/// the camera did not print.
	#[test]
	fn the_wire_invents_no_tag() {
		let xml = BcXml {
			ntp: Some(ntp_block()),
			ai_denoise: Some(denoise_block()),
			md: Some(md_block()),
			..Default::default()
		};
		let wire = quick_xml::se::to_string(&xml).expect("infaillible");
		assert!(wire.starts_with("<body"), "body wrapped: {wire}");
		assert!(wire.contains(r#"<Ntp version="1.1">"#), "case of <Ntp>: {wire}");
		assert!(
			wire.contains(r#"<aiDenoise Version="1.1">"#),
			"the capital version attribute must go back as it stands: {wire}"
		);
		assert!(
			wire.contains(r#"<MD version="1.1">"#),
			"<MD> keeps its case: {wire}"
		);
		assert!(wire.contains("<sensitivityDefault>30</sensitivityDefault>"));
		assert!(
			!wire.contains("MdAlarm"),
			"the carrier not read is not serialised: {wire}"
		);
		for absent in ["<Shelter", "<sleepState", "<WifiSignal", "<VideoInput"] {
			assert!(
				!wire.contains(absent),
				"{absent} was not read so it must not be written: {wire}"
			);
		}
	}
}

/// What the camera answered a [`RawRequest`]: the code, and the two
/// halves of the body **as they were understood**.
#[derive(Debug, Default, Clone)]
pub struct RawResponse {
	/// The header's `response_code`: 200, 400, 405…
	pub code: u16,
	/// The `msg_id` the camera returned (usually ours).
	pub msg_id: u32,
	/// The `class` of the header received. As useful as the code: a camera
	/// answering in a class other than the one asked for speaks another
	/// dialect, and that shows up nowhere else.
	pub classe_recue: u16,
	/// Which variant the received body was: `modern`, `legacy`, or `none`.
	///
	/// Without this field, a **legacy** answer would read as an empty body —
	/// the decoding below only looks at `ModernMsg`. The 13/11 measurement
	/// ("no body") only means something because that variant really was
	/// `modern`: modern header, no block behind it.
	pub kind: String,
	/// The decrypted `<Extension>`, rendered as XML.
	pub extension: Option<String>,
	/// The decrypted body, rendered as XML.
	///
	/// **This text is not the raw capture**: it is re-serialised from the
	/// parsed `BcXml`, so tags this repository does not model are absent from it.
	/// A 400 whose body only said `<result>notSupport</result>`
	/// reads here as an empty body — the camera's full text only comes out of
	/// the `Payload Txt` trace, and that is exactly what this probe is
	/// here to make it unnecessary to pay for twice.
	pub body: Option<String>,
}

/// A variant of the read request. All `None` = exactly today's
/// frame, which serves as the control in the series. Consumed by the call:
/// `BcXml` is not `Clone`, and the body goes out as is on the cable.
#[derive(Debug, Default)]
pub struct RawRequest {
	/// The **header** `channelId` (byte 12). `None` = the camera's.
	pub header_channel: Option<u8>,
	/// `Some(n)`: adds `<channelId>n</channelId>` to the extension.
	pub extension_channel: Option<u8>,
	/// `Some(n)`: adds `<rfId>n</rfId>` to the extension.
	pub extension_rf: Option<u8>,
	/// `Some(xml)`: the body to send, typed therefore encrypted.
	pub body: Option<BcXml>,
	/// The header's `class`. `None` = `0x6414`, the modern XML class.
	///
	/// The variable that remained to be varied after the 13/11 measurement: the four
	/// families in 400 answer **with no body at all**, while reads
	/// sent identically — `ntp` 38, `denoise` 439, `sleep` 574 — return
	/// 200. The body's shape is therefore not the only possible difference between
	/// a served command and a refused one; the request's declared class is
	/// another, and it cost nothing to expose.
	pub header_class: Option<u16>,
}

impl RawRequest {
	/// The control: the frame the library sends today.
	pub fn flag() -> Self {
		Self::default()
	}

	/// Extension with `<channelId>`, the shape of the commands that do pass.
	pub fn channel(n: u8) -> Self {
		Self { extension_channel: Some(n), ..Default::default() }
	}

	/// Extension with `<rfId>`, the PIR's addressing.
	pub fn rf(n: u8) -> Self {
		Self { extension_rf: Some(n), ..Default::default() }
	}

	/// A body that names the block, without an extension.
	pub fn body(xml: BcXml) -> Self {
		Self { body: Some(xml), ..Default::default() }
	}

	/// The full shape the reference clients expect: extension
	/// **and** body.
	pub fn channel_and_body(n: u8, xml: BcXml) -> Self {
		Self { extension_channel: Some(n), body: Some(xml), ..Default::default() }
	}
}

/// The shape **this** firmware expects for a settings frame — read
/// **and** write —, measured on 13/11/2026 on the operator's doorbell with an
/// MQTT probe that lives outside this repository. The `RawRequest` series below
/// reproduces the measurement row by row, so the table can be re-run without
/// the probe; nothing in this project consumes or serves the probe's own topic,
/// and no bridge topic should be inferred from it.
///
/// Four families refused `400` without ever writing an answer. The variable
/// was neither the wake nor the body: it is the absence of an `<Extension>` naming the
/// channel. Session of 13/11, camera awake, single session held by the bridge:
///
/// | request | answer |
/// |---|---|
/// | `msg 46` alone | `400`, no body |
/// | `msg 46` + `<channelId>0</channelId>` | `200` — `<MD version="1.1">…` |
/// | `msg 46` + a `<MD>` body, no extension | `400` |
/// | `msg 26` + `<channelId>0</channelId>` | `200` — `<Shelter>…` |
/// | `msg 44` + extension alone | `400` |
/// | `msg 44` + extension **and** header `channelId` at `0` | `200` — `<OsdDatetime>…` |
/// | `msg 52` + extension alone | `400` |
/// | `msg 52` + extension and header at `0` | `200` — `<OsdDatetime>…` |
/// | `msg 38` (ntp) and `104` (general) in the same shape | `200` — the shape does not break what worked |
/// | `msg 44` with `<channelId>1</channelId>` | `421` — the firmware reads the channel and refuses it |
/// | `msg 47` (SET motion) **without** extension, from Home Assistant | `400` — the switch stays at `on` |
///
/// The rule applied is therefore **restricted to those four families**, in read
/// as in writing, and is not made general: `ntp`, `general`, `battery` and
/// the others already answer with the previous frame, and no measurement says
/// they would accept this one. A general rule never measured on the
/// families that work today would be trading four known failures
/// for ten possible ones.
///
/// On the write side the table carries the `SET` ids by symmetry with the
/// read: that is an **inference**, not a measurement — what was measured is
/// that the `47` refuses `400` without the extension. The inference stays checkable in
/// the field because every `set_*` re-reads the block after writing: an
/// write swallowed without effect leaves a trace there, and it is the re-read that
/// is the evidence, even in the tests.
///
/// Two things this table does **not** say: this firmware's slots do not follow
/// the usual table — `26` answers `<Shelter>` (the masks) and
/// `52` answers `<OsdDatetime>`, where the library expected
/// `<VideoInput>` and `<Shelter>`; and nothing here says where this firmware serves
/// the image settings.
fn frame_shape(msg_id: u32, canal_camera: u8) -> (u8, Option<u8>) {
	match msg_id {
		MSG_ID_GET_IMAGE
		| MSG_ID_SET_IMAGE
		| MSG_ID_GET_OSD
		| MSG_ID_SET_OSD
		| MSG_ID_GET_MOTION_DETECT
		| MSG_ID_SET_MOTION_DETECT
		| MSG_ID_GET_PRIVACY_MASK
		| MSG_ID_SET_PRIVACY_MASK => (0, Some(0)),
		_ => (canal_camera, None),
	}
}

/// Renders the XML of a received block. `pub(crate)`: the probe is the only thing
/// that needs it, and the camera's text has no business in higher-level
/// APIs (same precautions as `publishable_reason` on the MQTT side).
fn text_xml(xml: &BcXml) -> String {
	let mut tampon = Vec::new();
	match xml.serialize(&mut tampon) {
		Ok(_) => String::from_utf8_lossy(&tampon).into_owned(),
		Err(e) => format!("<block received but not re-serialisable: {e}>"),
	}
}

fn text_extension(ext: &Extension) -> String {
	let mut tampon = Vec::new();
	match ext.serialize(&mut tampon) {
		Ok(_) => String::from_utf8_lossy(&tampon).into_owned(),
		Err(e) => format!("<extension not re-serialisable: {e}>"),
	}
}

/// Cutting a rendered text **by characters**, never by bytes: a camera
/// answer can carry UTF-8 and a `&t[..400]` landing on a byte in the middle of an
/// accent panics. The log is meant to be read, not to be exhaustive.
fn cut(text: &str, max_chars: usize) -> String {
	if text.chars().count() <= max_chars {
		return text.to_string();
	}
	let mut kept: String = text.chars().take(max_chars).collect();
	kept.push_str(" […]");
	kept
}
