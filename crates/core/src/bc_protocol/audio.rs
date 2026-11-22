//! The `<audioCfg>` block: what comes out of the loudspeaker, and how loud.
//!
//! Read with msg **264**, written back with msg **265**. This is the only
//! place the intercom volume (`talkAndReplyVolume`) exists — no other command
//! in the protocol surface carries it — so it is the one setting that cannot
//! be reached through any of the other config blocks.
//!
//! The write is a **read-modify-write**, and not by taste: the firmware wants
//! the block it just printed with one tag changed. A block that omits tags the
//! camera sent is rejected, or — worse, because it looks like success —
//! accepted and ignored. So [`crate::bc_protocol::BcCamera::set_audio_cfg`] is documented to take what
//! [`crate::bc_protocol::BcCamera::get_audio_cfg`] returned, and the caller is expected to touch only the
//! tags it came to change: everything else, including the tags whose meaning
//! nobody has worked out (`audioSelect`, `audioListId`, `preAlarm`,
//! `timeout`), goes back exactly as it arrived.
//!
//! Both commands keep the block as **text** — see [`AudioCfg`] for why — and
//! both honour the spelling the camera used for the element, which the read
//! records on the block itself ([`AudioCfg::tag`]) and the write sends back.

use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
};

/// The `abilityInfo` token that would advertise this block if the report were
/// complete. Used only when a caller asks for the gate; see
/// [`crate::bc_protocol::BcCamera::get_audio_cfg`].
const AUDIO_CFG_ABILITY: &str = "audioCfg";

impl BcCamera {
	/// Read the `<audioCfg>` block (msg 264): the loudspeaker `volume` and, on
	/// a doorbell, `talkAndReplyVolume` (how loud the answer you speak through
	/// the app comes out), `visitorVolume` (the pre-set greeting clips) and
	/// `visitorLoudspeaker`.
	///
	/// The returned block keeps every tag as the text the camera sent, and
	/// carries in [`AudioCfg::tag`] the spelling the camera used for the
	/// element — which is what [`crate::bc_protocol::BcCamera::set_audio_cfg`] has to send back.
	///
	/// `gate` is the ability veto, and it is **off by default at the CLI**:
	/// this doorbell's login-time `abilityInfo` under-reports (the same family
	/// answers `405` to `<HaCfg>` while accepting `<Email>`, which is why the
	/// ungated `TalkAbility` probe exists), so a report that doesn't mention
	/// `audioCfg` says nothing about whether msg 264 works.
	/// Pass `true` to let the report refuse the command before it is
	/// sent — the conservative behaviour, worth keeping available for cameras
	/// where a wrong command has a cost; pass `false` to let the camera answer,
	/// which is the only real proof of support.
	///
	/// A 200 reply that carries no `<audioCfg>` is
	/// [`Error::UnintelligibleReply`], not an empty block: the caller must not
	/// be able to read "no volume" out of a reply that said nothing.
	pub async fn get_audio_cfg(&self, gate: bool) -> Result<AudioCfg> {
		if gate {
			self.has_ability_ro(AUDIO_CFG_ABILITY).await?;
		}
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_GET_AUDIO_CFG, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_AUDIO_CFG,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: None,
			}),
		};

		sub_get.send(get).await?;
		let msg = sub_get.recv().await?;
		if msg.meta.response_code != 200 {
			return Err(Error::CameraServiceUnavailable {
				id: msg.meta.msg_id,
				code: msg.meta.response_code,
			});
		}

		// The block arrives under one of two carrier fields (the two spellings
		// of the element); `audio_cfg_block` folds them back into one answer
		// and stamps the casing onto it. Borrowed, so `msg` is still whole for
		// the error below.
		let block = match &msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(xml)),
				..
			}) => xml.audio_cfg_block(),
			_ => None,
		};
		block.ok_or_else(|| Error::UnintelligibleReply {
			reply: std::sync::Arc::new(msg),
			why: "Expected audioCfg xml but it was not received",
		})
	}

	/// Write the `<audioCfg>` block back (msg 265), then read it again with
	/// [`crate::bc_protocol::BcCamera::get_audio_cfg`] and return **what the camera has stored afterwards**.
	///
	/// `cfg` must be the block as read, with only the intended tags patched:
	/// `BcXml::body_with_audio_cfg` sends the element under the spelling
	/// recorded in [`AudioCfg::tag`] and every tag present on the way in goes
	/// out again, so a block built from scratch (or trimmed before sending) is
	/// the failure mode this API is shaped to prevent. A tag the camera never
	/// printed stays absent — `talkAndReplyVolume` and friends are `Option`
	/// precisely so a camera without an intercom is not handed one.
	///
	/// `gate` is the same veto as on [`crate::bc_protocol::BcCamera::get_audio_cfg`], checked against
	/// read-write permission for `audioCfg`; the read-back inherits it.
	pub async fn set_audio_cfg(&self, cfg: &AudioCfg, gate: bool) -> Result<AudioCfg> {
		if gate {
			self.has_ability_rw(AUDIO_CFG_ABILITY).await?;
		}
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_SET_AUDIO_CFG, msg_num).await?;
		let set = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SET_AUDIO_CFG,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: Some(BcPayloads::BcXml(BcXml::body_with_audio_cfg(cfg))),
			}),
		};

		sub_set.send(set).await?;
		// The SET quirk of `set_helpers`: a non-200 is a rejection and must
		// surface, but this family also accepts some SETs in silence, and
		// waiting forever on a reply that never comes is a battery camera held
		// awake for nothing. 500 ms, then trust the read-back below — which is
		// the real proof of what was stored.
		super::set_helpers::await_set_reply_with_quirk(
			&mut sub_set,
			super::set_helpers::SET_QUIRK_TIMEOUT,
		)
		.await?;
		drop(sub_set);
		self.get_audio_cfg(gate).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A doorbell's block as the camera prints it: every documented tag, the
	/// three intercom tags present, and the values kept as camera text.
	fn doorbell_block() -> AudioCfg {
		AudioCfg {
			version: "1.1".into(),
			channel_id: "0".into(),
			timeout: "10".into(),
			audio_select: "0".into(),
			volume: "50".into(),
			pre_alarm: "0".into(),
			audio_list_id: "0".into(),
			talk_and_reply_volume: Some("40".into()),
			visitor_volume: Some("70".into()),
			visitor_loudspeaker: Some("1".into()),
			tag: AudioCfgTag::Camel,
		}
	}

	/// Answer as a camera that spells the element the way `cfg.tag` says.
	fn reply_block(req: &Bc, cfg: AudioCfg) -> Bc {
		let xml = match cfg.tag {
			AudioCfgTag::Camel => BcXml {
				audio_cfg_camel: Some(cfg),
				..Default::default()
			},
			AudioCfgTag::Pascal => BcXml {
				audio_cfg: Some(cfg),
				..Default::default()
			},
		};
		reply_200_xml(req, xml)
	}

	/// The request must carry the channel in its extension, like every other
	/// get of this family.
	fn assert_channel_extension(req: &Bc) {
		let BcBody::ModernMsg(ModernMsg { extension, .. }) = &req.body else {
			panic!("modern body expected");
		};
		let ext = extension.as_ref().expect("extension on request");
		assert_eq!(ext.channel_id, Some(0));
	}

	#[tokio::test]
	async fn get_audio_cfg_returns_every_tag_and_records_the_camel_spelling() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| {
				assert_channel_extension(req);
				reply_block(req, doorbell_block())
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_audio_cfg(false).await.expect("ok");
		assert_eq!(cfg.version, "1.1");
		assert_eq!(cfg.channel_id, "0");
		assert_eq!(cfg.volume, "50");
		assert_eq!(cfg.talk_and_reply_volume.as_deref(), Some("40"));
		assert_eq!(cfg.visitor_volume.as_deref(), Some("70"));
		assert_eq!(cfg.visitor_loudspeaker.as_deref(), Some("1"));
		// The tags nobody understands come back as the text they arrived in.
		assert_eq!(cfg.timeout, "10");
		assert_eq!(cfg.audio_select, "0");
		assert_eq!(cfg.pre_alarm, "0");
		assert_eq!(cfg.audio_list_id, "0");
		assert_eq!(cfg.tag, AudioCfgTag::Camel);
	}

	/// The protocol note lists both `<audioCfg>` and `<AudioCfg>`, and
	/// quick-xml matches names case-sensitively, so a block printed either way
	/// has to decode — the spelling is recorded, not guessed.
	#[tokio::test]
	async fn get_audio_cfg_records_the_pascal_spelling() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| {
				let mut pascal = doorbell_block();
				pascal.tag = AudioCfgTag::Pascal;
				reply_block(req, pascal)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_audio_cfg(false).await.expect("ok");
		assert_eq!(cfg.volume, "50");
		assert_eq!(cfg.talk_and_reply_volume.as_deref(), Some("40"));
		assert_eq!(cfg.tag, AudioCfgTag::Pascal);
	}

	/// The whole point of the `gate` switch: a camera whose `abilityInfo`
	/// doesn't mention `audioCfg` still gets to answer msg 264.
	#[tokio::test]
	async fn get_audio_cfg_ungated_reads_despite_an_under_reporting_ability_list() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| reply_block(req, doorbell_block()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// No `test_set_ability` at all: the ability map is empty, as it is on
		// the doorbell.
		assert_eq!(
			cam.get_audio_cfg(false).await.expect("ungated").volume,
			"50"
		);
	}

	#[tokio::test]
	async fn get_audio_cfg_gated_refuses_unless_the_ability_is_advertised() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| reply_block(req, doorbell_block()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_audio_cfg(true).await.expect_err("gate");
		assert!(
			matches!(&err, Error::MissingAbility { name, .. } if name == AUDIO_CFG_ABILITY),
			"{err:?}"
		);
		// With the token advertised, the same gate lets the read through.
		cam.test_set_ability(AUDIO_CFG_ABILITY, true).await;
		assert_eq!(
			cam.get_audio_cfg(true).await.expect("gated ok").volume,
			"50"
		);
	}

	#[tokio::test]
	async fn get_audio_cfg_not_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| reply_err_code(req, 405))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_audio_cfg(false).await.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 405, .. }
		));
	}

	/// A 200 that carries no block must not read as "all volumes empty".
	#[tokio::test]
	async fn get_audio_cfg_without_the_block_is_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_audio_cfg(false).await.expect_err("err");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	/// The contract of msg 265: the block the camera printed comes back
	/// complete, under the same element name, with exactly one tag changed —
	/// and the caller is handed what the camera kept.
	#[tokio::test]
	async fn set_audio_cfg_resends_the_whole_block_with_one_tag_changed() {
		let mut after = doorbell_block();
		after.talk_and_reply_volume = Some("90".into());
		let expected_set = after.clone();

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| reply_block(req, doorbell_block()))
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with_xml(move |req, xml| {
				let sent = xml.audio_cfg_block().expect("audioCfg on the SET request");
				// Full equality: any tag dropped, added or rewritten by the
				// round trip fails here.
				assert_eq!(sent, expected_set);
				assert!(
					xml.audio_cfg.is_none(),
					"a camera that printed <audioCfg> must be answered with <audioCfg>"
				);
				let wire = String::from_utf8(xml.serialize(vec![]).unwrap()).unwrap();
				assert!(wire.contains("<audioCfg"), "{wire}");
				assert!(!wire.contains("<AudioCfg"), "{wire}");
				assert!(
					wire.contains("<talkAndReplyVolume>90</talkAndReplyVolume>"),
					"{wire}"
				);
				for untouched in [
					"<volume>50</volume>",
					"<visitorVolume>70</visitorVolume>",
					"<visitorLoudspeaker>1</visitorLoudspeaker>",
					"<audioSelect>0</audioSelect>",
					"<audioListId>0</audioListId>",
					"<preAlarm>0</preAlarm>",
					"<timeout>10</timeout>",
					"<channelId>0</channelId>",
				] {
					assert!(wire.contains(untouched), "missing {untouched} in {wire}");
				}
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(move |req| reply_block(req, after.clone()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;

		let before = cam.get_audio_cfg(false).await.expect("read");
		let mut patched = before.clone();
		patched.talk_and_reply_volume = Some("90".into());
		let stored = cam.set_audio_cfg(&patched, false).await.expect("set");

		assert_eq!(before.talk_and_reply_volume.as_deref(), Some("40"));
		assert_eq!(stored.talk_and_reply_volume.as_deref(), Some("90"));
		assert_eq!(stored.volume, "50", "untouched tags survive the write");
		assert_eq!(stored.tag, AudioCfgTag::Camel);
	}

	/// Same contract, other spelling: the casing the camera used is the casing
	/// it gets back, which is the one thing a truncated-but-correct block can
	/// still get wrong.
	#[tokio::test]
	async fn set_audio_cfg_echoes_the_pascal_spelling() {
		let mut pascal = doorbell_block();
		pascal.tag = AudioCfgTag::Pascal;
		let mut after = pascal.clone();
		after.volume = "80".into();

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(move |req| reply_block(req, pascal.clone()))
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with_xml(|req, xml| {
				let sent = xml.audio_cfg_block().expect("AudioCfg on the SET request");
				assert_eq!(sent.tag, AudioCfgTag::Pascal);
				assert_eq!(sent.volume, "80");
				assert!(
					xml.audio_cfg_camel.is_none(),
					"a camera that printed <AudioCfg> must be answered with <AudioCfg>"
				);
				let wire = String::from_utf8(xml.serialize(vec![]).unwrap()).unwrap();
				assert!(wire.contains("<AudioCfg"), "{wire}");
				assert!(!wire.contains("<audioCfg"), "{wire}");
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(move |req| reply_block(req, after.clone()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;

		let before = cam.get_audio_cfg(false).await.expect("read");
		let mut patched = before.clone();
		patched.volume = "80".into();
		let stored = cam.set_audio_cfg(&patched, false).await.expect("set");
		assert_eq!(before.volume, "50");
		assert_eq!(stored.volume, "80");
		assert_eq!(stored.tag, AudioCfgTag::Pascal);
	}

	#[tokio::test]
	async fn set_audio_cfg_not_200_returns_err_and_does_not_read_back() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// The mock closes the reply channel once its expectations run out, so a
		// read-back after the rejection would fail rather than return — the
		// rejection alone must end the call.
		let err = cam
			.set_audio_cfg(&doorbell_block(), false)
			.await
			.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn set_audio_cfg_gated_refuses_before_sending() {
		// No expectation at all: nothing may reach the wire.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.set_audio_cfg(&doorbell_block(), true)
			.await
			.expect_err("gate");
		assert!(matches!(
			&err,
			Error::MissingAbility { name, .. } if name == AUDIO_CFG_ABILITY
		));
	}
}
