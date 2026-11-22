//! The paired-chime (ding-dong) family: msgs 483 / 484 / 485 / 486 / 487 /
//! 609 / 610, reached **through the doorbell's session** — the chime has no
//! network address of its own; the doorbell speaks for it.
//!
//! # Shape of the family (and why this file looks nothing like `audio.rs`)
//!
//! `<audioCfg>` is one block read whole and written whole. The chime is spread
//! over five questions:
//!
//! | msg | asks | answers |
//! | --- | --- | --- |
//! | 484 | (empty body) | `<dingdongDeviceInfo>` per paired wireless chime |
//! | 483 | `<dingdongCtrl><opt>machineStateGet` | hardwired chime block |
//! | 485 | `<dingdongDeviceOpt><opt>getParam` + `<id>` | `name` / `volLevel` / `ledState` |
//! | 609 | `<dingdongSilentMode><id>` | `<time>` |
//! | 486 | (empty body) | `<deviceCfg>` per device, `<alarminCfg>` per event |
//!
//! Each part a camera doesn't have is **refused, not answered empty**, so
//! [`crate::bc_protocol::BcCamera::get_ding_dong_cfg`] reads all five softly — a non-200 turns that *part*
//! into `None` and costs nothing else — and only refuses entirely when the
//! camera answered none of the family (that is a connection talking nonsense,
//! not a doorbell without a chime).
//!
//! Writes are the mirror: [`crate::bc_protocol::BcCamera::set_ding_dong_cfg`] re-reads, diffs the caller's
//! block against what the camera holds **right now**, and sends only the parts
//! whose text actually changed — whole blocks, never partial ones. The diff is
//! not an optimisation: msg 610 re-arms the chime's mute countdown just by
//! arriving, so blindly "re-writing the received block"would silently mute a
//! user's chime again. And as everywhere in this crate, a tag the camera never
//! printed is never invented: asking to change one is a caller error and
//! nothing goes on the wire.
//!
//! # Why there is no ability gate
//!
//! `get_audio_cfg` can check `audioCfg` in the camera's `abilityInfo` because
//! that token is documented. The chime family has **no abilityInfo token at
//! all** (the protocol note is explicit that the chime ability is queried by
//! the commands themselves), so a gate could only check a token invented here
//! — which would veto exactly the cameras that do have a chime. The answer to
//! 484 is the only support test that counts, and a soft read already says it.
//! If a future `abilities` dump shows a real chime token, adding the gate is
//! five lines and one decision.
//!
//! # Known unknowns (settled only on hardware — do not "fix"these by guessing)
//!
//! - The **root element of replies** is documented only by its inner tags. The
//!   read assumes the reply echoes the request's carrier (`dingdongCtrl` /
//!   `dingdongDeviceInfo` / `dingdongDeviceOpt` / `deviceCfg` /
//!   `dingdongSilentMode`). For the chime list the **second documented root**
//!   is read too — msg 484 is double-use, reply and push, and the pushed
//!   variant is `<dingdongList>` — through [`BcXml::dingdong_chimes`]. Any
//!   other root still decodes to `None` / `UnintelligibleReply`: wrong-but-safe,
//!   no invented tag, and 484 answering 200 with nothing addressable is
//!   reported as [`DingDongCfg::list_answered_without_entry`] rather than as
//!   "no chime paired ".
//! - `volLevel` is "0-4 typical", the real ceiling unknown; `musicId` is a
//!   free integer with **no melody list** anywhere; the unit of 483's `<time>`
//!   and 609's `<time>` is undocumented; 610's `<type>63</type>` is copied
//!   from the reference client with its meaning unknown; the event-type
//!   vocabulary of 486 is whatever the camera printed (discover by reading).
//! - Values are kept as camera `String` text throughout, so an undocumented
//!   value survives read → report → write-back unchanged. Same caveat as
//!   [`AudioCfg`](crate::bc::xml::AudioCfg): serde drops tags this crate
//!   doesn't model, and they would be absent from a write-back.

use super::{BcCamera, Error, Result};
use crate::bc::model::*;
use crate::bc::xml::*;

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, inspect_xml, MockConnection,
	MockConnectionBuilder,
};

/// A caller error phrased like the CLI's usage errors: this is never a
/// camera or network fault, and the caller fixes it by not asking.
fn usage(verb: &str, msg: impl Into<String>) -> Error {
	Error::Usage(format!("refusing to {verb} the chime: {}", msg.into()))
}

/// Borrow the XML payload of a reply, if it carried one.
fn reply_xml(msg: &Bc) -> Option<&BcXml> {
	match &msg.body {
		BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::BcXml(xml)),
			..
		}) => Some(xml),
		_ => None,
	}
}

/// The caller's patched text where it changed, else the text the camera
/// printed. `want` is expected to be a full copy of the read with only the
/// intended tags touched — anything else the diff refuses.
fn patched(want: &Option<String>, fresh: &Option<String>) -> Option<String> {
	want.clone().or_else(|| fresh.clone())
}

impl BcCamera {
	/// Read the whole chime configuration as one aggregate.
	///
	/// Sends 483, 484, then (for the chosen chime id) 485 `getParam` and 609,
	/// then 486 — in that order, so the chime id from 484 is known before the
	/// per-chime questions. Every part answered with a non-200 or without its
	/// block becomes `None` in the result; only if **no part at all** produced
	/// a block is this an [`Error::UnintelligibleReply`] anchored on the first
	/// reply (a camera that answers none of the family is not "a doorbell
	/// without a chime", it is a connection not speaking this protocol).
	///
	/// The chime id chosen for the per-chime reads is the first numeric id in
	/// the 484 list, preferring one the list marks online (`netState` 1) —
	/// offline is a chime that didn't answer the last keep-alive, and its
	/// params are the more likely to be stale.
	///
	/// Note the 483 read is not physically silent on a wired install: the
	/// reference client warns it can make the chime sound. Five commands also
	/// cost a battery doorbell real radio time; this is a diagnostic read, not
	/// something to poll.
	pub async fn get_ding_dong_cfg(&self) -> Result<DingDongCfg> {
		// -- 483: hardwired chime. Soft: what a wireless-only install answers
		// here is unknown, and a refusal must not sink the rest of the read.
		let msg_ctrl = self
			.ding_dong_msg(MSG_ID_DING_DONG_CTRL, Some(BcXml::body_ding_dong_ctrl_get()))
			.await?;
		let ctrl = self.soft_block(&msg_ctrl, "dingdongCtrl", |xml| xml.dingdong_ctrl.clone());

		// -- 484: paired-chime list. A model without a wireless chime refuses
		// this one (inferred from the reference client: 400/timeout — there is
		// no documented error table, so it stays an inference).
		//
		// Both documented spellings are read, through `dingdong_chimes()`: the
		// reply root `<dingdongDeviceInfo>`, and the `<dingdongList>` container
		// — whose entries a `v3.0.0.4636` doorbell nests two levels deep, under
		// `<pairedList>` (the reference client's note — msg 484 is reply *and*
		// push). And a 200 that identifies nothing is deliberately **not**
		// reported as "a list of zero chimes ": on the field camera a chime was
		// paired and the inventory came back `[]`, which is precisely the case
		// where "vide "and "je n'ai pas su lire "must not look alike.
		let msg_list = self.ding_dong_msg(MSG_ID_GET_DING_DONG_LIST, None).await?;
		let list_answered = msg_list.meta.response_code == 200 && reply_xml(&msg_list).is_some();
		let chimes = self.soft_block(&msg_list, "dingdongDeviceInfo", |xml| {
			let list = xml.dingdong_chimes();
			(!list.is_empty()).then(|| list.to_vec())
		});

		// Pick the chime the per-chime questions are about: first numeric id,
		// online preferred (see the method doc). Blocks without an
		// addressable id were already made invisible by `identifier()`.
		let chime_id = chimes.as_ref().and_then(|list| {
			list.iter()
				.find(|d| d.is_online() && d.identifier().is_some())
				.or_else(|| list.iter().find(|d| d.identifier().is_some()))
				.and_then(|d| d.identifier())
				.map(str::to_string)
		});
		// "484 answered, but with nothing addressable"is not the same sentence
		// as "484 did not answer", and it is exactly the one the field camera
		// forces: a chime was paired to it, so publishing an empty inventory alone
		// would have lied. Entries without an id stay in `chimes` - they are what
		// the `status/chime/json` payload shows the operator.
		let list_answered_without_entry = list_answered && chime_id.is_none();

		// -- 485 getParam / 609: per-chime, so only when an id was found.
		let mut params = None;
		let mut silent = None;
		if let Some(id) = chime_id.as_deref() {
			let msg_opt = self
				.ding_dong_msg(MSG_ID_DING_DONG_OPT, Some(BcXml::body_ding_dong_opt_get(id)))
				.await?;
			params = self.soft_block(&msg_opt, "dingdongDeviceOpt", |xml| {
				xml.dingdong_device_opt.clone()
			});
			let msg_silent = self
				.ding_dong_msg(
					MSG_ID_GET_DING_DONG_SILENT,
					Some(BcXml::body_ding_dong_silent_get(id)),
				)
				.await?;
			silent = self.soft_block(&msg_silent, "dingdongSilentMode", |xml| {
				xml.dingdong_silent_mode.clone()
			});
		}

		// -- 486: per-event config (empty body, like 484). The field camera
		// wraps its reply in `<dingdongCfg>` — the same root the 487 write uses
		// — so the read folds both nestings instead of looking only at a bare
		// `<deviceCfg>` at the body root, which is how a camera that HAD
		// answered its melodies was being reported with none.
		let msg_cfg = self.ding_dong_msg(MSG_ID_GET_DING_DONG_CFG, None).await?;
		let events = self.soft_block(&msg_cfg, "deviceCfg", |xml| {
			let cfgs = xml.dingdong_events();
			(!cfgs.is_empty()).then(|| cfgs.to_vec())
		});

		if ctrl.is_none()
			&& chimes.is_none()
			&& !list_answered
			&& params.is_none()
			&& silent.is_none()
			&& events.is_none()
		{
			// Every reply arrived and none carried a chime block. Anchor on
			// 483 — the first reply is as good a specimen of "not speaking
			// this family"as any, and it is the one already in hand.
			//
			// A 484 that answered 200 counts as an answer: the camera *is*
			// speaking this family, it just didn't name a chime, and the
			// bridge has to publish an empty inventory with the reason
			// instead of pretending the whole family is nonsense.
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg_ctrl),
				why: "Expected a chime block (483 dingdongCtrl / 484 dingdongDeviceInfo / 485 dingdongDeviceOpt / 486 deviceCfg / 609 dingdongSilentMode) but the camera answered none of the chime family with one",
			});
		}

		Ok(DingDongCfg {
			chime_id,
			chimes,
			list_answered_without_entry,
			ctrl,
			params,
			silent,
			events,
		})
	}

	/// Write the changed parts of a chime block back, then read the whole
	/// configuration again and return **what the camera holds afterwards**.
	///
	/// `cfg` must be a block from [`crate::bc_protocol::BcCamera::get_ding_dong_cfg`] with only the intended
	/// tags patched — the same law as [`crate::bc_protocol::BcCamera::set_audio_cfg`], because the same
	/// firmware silently shrugs off a truncated block. The write is staged:
	///
	/// 1. a fresh full read (the diff baseline — and the reason this method
	///    costs ten commands when something changed);
	/// 2. refuse anything that would invent a tag or a block the fresh read
	///    doesn't show, or rewrite an event `type` / switch (on 487 blocks
	///    only `musicId` is writable);
	/// 3. send only the parts whose text differs — 485 `setParam`, 483
	///    `machineStateSet` (+ `<bsave>1</bsave>`), 610 silent (which carries
	///    the hard-coded `<type>63</type>`), one 487 per changed device, in
	///    that order — each bounded by the SET quirk timeout, and a non-200 is
	///    an error **with no re-read** (the block wasn't stored; reading again
	///    would only cost the battery);
	/// 4. the re-read. When nothing differed at step 3, no command was sent
	///    and the fresh read is returned as the answer.
	///
	/// The silent-mode part is the reason for the diff: an unchanged 610
	/// re-arms the mute countdown, so "no change"must mean "no command", not
	/// "faithful echo".
	pub async fn set_ding_dong_cfg(&self, cfg: &DingDongCfg) -> Result<DingDongCfg> {
		let fresh = self.get_ding_dong_cfg().await?;
		let mut wrote = false;

		// -- params (485 setParam). The frame re-sends the printed tags,
		// patched — exactly what `setParam` documents ("only the tags you
		// pass", and the printed ones go back out as the camera wrote them).
		if let Some(want) = cfg.params.as_ref() {
			let base = fresh.params.as_ref().ok_or_else(|| {
				usage("write", "the camera printed no <dingdongDeviceOpt> params block, so chime parameters cannot be written without inventing one")
			})?;
			let id = fresh.chime_id.as_deref().ok_or_else(|| {
				usage("write", "no addressable chime id in the 484 list, so there is nothing for a params write to name")
			})?;
			for (tag, want, fresh) in [
				("name", want.name.as_deref(), base.name.as_deref()),
				("volLevel", want.vol_level.as_deref(), base.vol_level.as_deref()),
				("ledState", want.led_state.as_deref(), base.led_state.as_deref()),
			] {
				if want.is_some() && fresh.is_none() {
					return Err(usage("write", format!(
						"this chime's block has no <{tag}> tag, and a tag the firmware did not print is never added"
					)));
				}
			}
			let frame = DingDongDeviceOpt {
				version: if base.version.is_empty() { xml_ver() } else { base.version.clone() },
				opt: Some("setParam".into()),
				id: Some(id.into()),
				name: patched(&want.name, &base.name),
				vol_level: patched(&want.vol_level, &base.vol_level),
				led_state: patched(&want.led_state, &base.led_state),
				// Melody belongs to the event config (487); a 485 block that
				// printed no musicId never starts writing one.
				music_id: None,
			};
			if frame.name != base.name
				|| frame.vol_level != base.vol_level
				|| frame.led_state != base.led_state
			{
				self.ding_dong_write(MSG_ID_DING_DONG_OPT, BcXml::body_with_ding_dong_opt(&frame))
					.await?;
				wrote = true;
			}
		}

		// -- ctrl (483 machineStateSet). The switch goes back out under the
		// spelling the camera used (`bopen` or `enable`), never both; the
		// builder stamps `machineStateSet` and the documented <bsave>1</bsave>.
		if let Some(want) = cfg.ctrl.as_ref() {
			let base = fresh.ctrl.as_ref().ok_or_else(|| {
				usage("write", "the camera printed no <dingdongCtrl> block, so hardwired-chime settings cannot be written without inventing one")
			})?;
			for (tag, want, fresh) in [
				("type", want.type_.as_deref(), base.type_.as_deref()),
				("bopen", want.bopen.as_deref(), base.bopen.as_deref()),
				("enable", want.enable.as_deref(), base.enable.as_deref()),
				("time", want.time.as_deref(), base.time.as_deref()),
			] {
				if want.is_some() && fresh.is_none() {
					return Err(usage("write", format!(
						"this camera's <dingdongCtrl> block has no <{tag}> tag, and a tag the firmware did not print is never added"
					)));
				}
			}
			let frame = DingDongCtrl {
				version: if base.version.is_empty() { xml_ver() } else { base.version.clone() },
				// `body_with_ding_dong_ctrl` stamps opt and bsave.
				opt: None,
				bsave: base.bsave.clone(),
				channel_id: base.channel_id.clone(),
				type_: patched(&want.type_, &base.type_),
				bopen: patched(&want.bopen, &base.bopen),
				enable: patched(&want.enable, &base.enable),
				time: patched(&want.time, &base.time),
				// `<result>` is a refusal reason the camera prints (measured:
				// `adptNotAC`), never a setting: it is reported and dropped
				// here, so a write cannot smuggle it back as if it were one.
				result: None,
			};
			if frame.type_ != base.type_
				|| frame.bopen != base.bopen
				|| frame.enable != base.enable
				|| frame.time != base.time
			{
				self.ding_dong_write(MSG_ID_DING_DONG_CTRL, BcXml::body_with_ding_dong_ctrl(&frame))
					.await?;
				wrote = true;
			}
		}

		// -- silent (610). Only a changed `<time>` is ever sent: the block
		// re-arms the mute countdown merely by arriving (see the method doc).
		if let Some(want) = cfg.silent.as_ref() {
			let base = fresh.silent.as_ref().ok_or_else(|| {
				usage("write", "the camera printed no <dingdongSilentMode> block, so silent mode cannot be written without inventing one")
			})?;
			if want.time.is_some() && base.time.is_none() {
				return Err(usage(
					"write",
					"this chime's silent block has no <time> tag, and a tag the firmware did not print is never added",
				));
			}
			if want.time.is_some() && want.time != base.time {
				let id = base.id.clone().or_else(|| fresh.chime_id.clone()).ok_or_else(|| {
					usage("write", "no chime id available for the silent-mode frame")
				})?;
				let frame = DingDongSilent {
					version: if base.version.is_empty() { xml_ver() } else { base.version.clone() },
					id: Some(id),
					time: want.time.clone(),
					type_: base.type_.clone(),
				};
				self.ding_dong_write(MSG_ID_SET_DING_DONG_SILENT, BcXml::body_with_ding_dong_silent(&frame))
					.await?;
				wrote = true;
			}
		}

		// -- events (487, one request per changed device). Only `<musicId>`
		// may move: the event `type` is camera-owned, and the switch goes back
		// out as it arrived — a block that arrived under `<switch>` is
		// re-stamped to the 487 frame's documented `<valid>` name with its
		// text untouched (the tag is the frame's choice, not ours; flagged as
		// a hardware unknown in the module docs). A block that printed
		// neither prints neither again.
		if let Some(want_events) = cfg.events.as_ref() {
			let base_events = fresh.events.as_ref().ok_or_else(|| {
				usage("write", "the camera printed no <deviceCfg> blocks, so event melodies cannot be written without inventing them")
			})?;
			if want_events.len() != base_events.len() {
				return Err(usage(
					"write",
					"the number of <deviceCfg> blocks differs from what the camera holds; blocks are never added or removed",
				));
			}
			for (want_dev, base_dev) in want_events.iter().zip(base_events.iter()) {
				if want_dev.id.is_some() && want_dev.id != base_dev.id {
					return Err(usage(
						"write",
						"a <deviceCfg> block does not carry the id the camera read it under; blocks may not be reordered or retargeted",
					));
				}
				if want_dev.alarmin_cfg.len() != base_dev.alarmin_cfg.len() {
					return Err(usage(
						"write",
						"the number of <alarminCfg> entries differs from what the camera holds; entries are never added or removed",
					));
				}
				let mut changed = false;
				for (want_cfg, base_cfg) in
					want_dev.alarmin_cfg.iter().zip(base_dev.alarmin_cfg.iter())
				{
					if want_cfg.type_ != base_cfg.type_
						|| want_cfg.valid != base_cfg.valid
						|| want_cfg.switch_ != base_cfg.switch_
					{
						return Err(usage(
							"write",
							"only <musicId> is writable on a chime event; its <type> and switch must stay exactly as read",
						));
					}
					if want_cfg.music_id.is_some() && base_cfg.music_id.is_none() {
						return Err(usage(
							"write",
							"this event's block has no <musicId> tag, and a tag the firmware did not print is never added",
						));
					}
					changed |= want_cfg.music_id != base_cfg.music_id;
				}
				if !changed {
					continue;
				}
				let frame = DingDongDeviceCfg {
					version: xml_ver(),
					id: base_dev.id.clone().or_else(|| fresh.chime_id.clone()),
					// `ringId` is not part of the documented write frame.
					ring_id: None,
					// Neither is the chime's name or its own firmware, both of
					// which the 486 reply prints beside the id: they are read
					// labels, and echoing them back here would be inventing a
					// tag the camera never documented on a write.
					name: None,
					firm_version: None,
					alarmin_cfg: want_dev
						.alarmin_cfg
						.iter()
						.zip(base_dev.alarmin_cfg.iter())
						.map(|(w, b)| DingDongAlarmCfg {
							version: xml_ver(),
							type_: b.type_.clone(),
							valid: b.valid.clone().or_else(|| b.switch_.clone()),
							switch_: None,
							music_id: patched(&w.music_id, &b.music_id),
						})
						.collect(),
				};
				self.ding_dong_write(MSG_ID_SET_DING_DONG_CFG, BcXml::body_with_ding_dong_cfg(&frame))
					.await?;
				wrote = true;
			}
		}

		if wrote {
			self.get_ding_dong_cfg().await
		} else {
			// Nothing differed, so nothing was sent — and the fresh read
			// already *is* what the camera holds.
			Ok(fresh)
		}
	}

	/// Make the paired chime ring once (`ringWithMusic` on msg 485).
	///
	/// This is the only thing in the chime family with **physical feedback**:
	/// a `200` here plus the sound from the chime is the proof that the
	/// doorbell reaches it. Deliberately its own method rather than a side
	/// effect of a write — the operator asks for exactly one ring, with
	/// `music_id` among the melody numbers the camera's own picker uses
	/// (integers; no list is documented). The reply carries no state to read
	/// back, so the SET quirk timeout is the whole wait.
	///
	/// `chime_id` must be numeric — it has to be an id the 484 list printed,
	/// and an invented one is exactly the guessing this crate refuses.
	/// `music_id` is passed as text without validation (it lands in an
	/// escaped XML text node, so it cannot break the frame out).
	pub async fn ring_ding_dong(&self, chime_id: &str, music_id: &str) -> Result<()> {
		if chime_id.is_empty() || !chime_id.bytes().all(|b| b.is_ascii_digit()) {
			return Err(usage(
				"ring",
				format!("chime id {chime_id:?} is not a numeric id the 484 list could have printed"),
			));
		}
		let frame = DingDongDeviceOpt {
			version: xml_ver(),
			opt: Some("ringWithMusic".into()),
			id: Some(chime_id.into()),
			name: None,
			vol_level: None,
			led_state: None,
			music_id: Some(music_id.into()),
		};
		self.ding_dong_write(MSG_ID_DING_DONG_OPT, BcXml::body_with_ding_dong_opt(&frame))
			.await
	}

	/// One chime-family request/response exchange: subscribe on the msg id,
	/// send the frame with the channel in its extension (the 484/486 "empty
	/// body"reads pass `None`, like `get_audio_cfg`), and hand back the raw
	/// reply. The response code is the caller's decision — soft for reads,
	/// strict for writes.
	async fn ding_dong_msg(&self, msg_id: u32, body: Option<BcXml>) -> Result<Bc> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(msg_id, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id,
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
				payload: body.map(BcPayloads::BcXml),
			}),
		};
		sub.send(get).await?;
		sub.recv().await
	}

	/// Soft read decoding: a non-200, or a 200 that didn't carry this
	/// carrier, means "this camera doesn't have this part"→ `None`. Never an
	/// error — see [`crate::bc_protocol::BcCamera::get_ding_dong_cfg`] for why one refused part must not
	/// sink the whole read.
	fn soft_block<T>(
		&self,
		msg: &Bc,
		carrier: &str,
		extract: impl FnOnce(&BcXml) -> Option<T>,
	) -> Option<T> {
		if msg.meta.response_code != 200 {
			log::debug!(
				"chime: msg {} refused with code {} — that part of the chime is reported as absent",
				msg.meta.msg_id,
				msg.meta.response_code
			);
			return None;
		}
		match reply_xml(msg).and_then(extract) {
			Some(block) => Some(block),
			None => {
				log::debug!(
					"chime: msg {} answered 200 without a <{carrier}> block",
					msg.meta.msg_id
				);
				None
			}
		}
	}

	/// One chime-family write: send, then bound the wait exactly like the
	/// audio SET (this family shares the quirk: some of these SETs are
	/// accepted in silence, and a battery doorbell should not be held awake
	/// for a reply that never comes). A non-200 is a real rejection and
	/// surfaces immediately — callers must not report a write that was
	/// refused, and `set_ding_dong_cfg` deliberately skips its re-read then.
	async fn ding_dong_write(&self, msg_id: u32, xml: BcXml) -> Result<()> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(msg_id, msg_num).await?;
		let set = Bc {
			meta: BcMeta {
				msg_id,
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
				payload: Some(BcPayloads::BcXml(xml)),
			}),
		};
		sub.send(set).await?;
		super::set_helpers::await_set_reply_with_quirk(
			&mut sub,
			super::set_helpers::SET_QUIRK_TIMEOUT,
		)
		.await?;
		drop(sub);
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;

	/// The full happy-path answer of a battery doorbell **with** a paired
	/// wireless chime: 483 answers 200-without-a-block (what a wireless-only
	/// install truly answers is unknown; this keeps the fixture honest about
	/// that), one online chime in 484, params, silent off, two events on one
	/// device — one of them under each switch spelling.
	fn paired_fixture() -> DingDongCfg {
		DingDongCfg {
			chime_id: Some("1".into()),
			chimes: Some(vec![DingDongDevice {
				device_id: Some("1".into()),
				device_name: Some("Carillon".into()),
				net_state: Some("1".into()),
				..Default::default()
			}]),
			list_answered_without_entry: false,
			ctrl: None,
			params: Some(DingDongDeviceOpt {
				version: "1.1".into(),
				opt: Some("getParam".into()),
				id: Some("1".into()),
				name: Some("Carillon".into()),
				vol_level: Some("2".into()),
				led_state: Some("1".into()),
				music_id: None,
			}),
			silent: Some(DingDongSilent {
				version: "1.1".into(),
				id: Some("1".into()),
				time: Some("0".into()),
				type_: None,
			}),
			events: Some(vec![DingDongDeviceCfg {
				version: "1.1".into(),
				id: Some("1".into()),
				ring_id: Some("7".into()),
				name: Some("Carillon".into()),
				firm_version: Some("00A2".into()),
				alarmin_cfg: vec![
					DingDongAlarmCfg {
						version: "1.1".into(),
						type_: Some("people".into()),
						valid: Some("1".into()),
						switch_: None,
						music_id: Some("0".into()),
					},
					DingDongAlarmCfg {
						version: "1.1".into(),
						type_: Some("visitor".into()),
						valid: None,
						switch_: Some("0".into()),
						music_id: Some("2".into()),
					},
				],
			}]),
		}
	}

	/// One reply of the read sweep carrying `cfg`'s part for `msg_id`. Also
	/// checks, on the per-chime frames, that the REQUEST carries the chosen
	/// chime id — the sweep's central promise.
	fn reply_part(req: &Bc, msg_id: u32, cfg: &DingDongCfg) -> Bc {
		match msg_id {
			MSG_ID_DING_DONG_CTRL => match &cfg.ctrl {
				// No block: a 200 that carries nothing — the soft-part case.
				None => reply_200_empty(req),
				Some(c) => reply_200_xml(
					req,
					BcXml {
						dingdong_ctrl: Some(c.clone()),
						..Default::default()
					},
				),
			},
			MSG_ID_GET_DING_DONG_LIST => match &cfg.chimes {
				None => reply_err_code(req, 400),
				Some(list) => reply_200_xml(
					req,
					BcXml {
						dingdong_device_info: list.clone(),
						..Default::default()
					},
				),
			},
			MSG_ID_DING_DONG_OPT => {
				let xml = inspect_xml(req);
				let opt = xml
					.dingdong_device_opt
					.as_ref()
					.expect("getParam rides dingdongDeviceOpt");
				assert_eq!(opt.opt.as_deref(), Some("getParam"), "reads must not ask setParam");
				assert_eq!(
					opt.id.as_deref(),
					cfg.chime_id.as_deref(),
					"the per-chime read must carry the chosen id"
				);
				match &cfg.params {
					None => reply_err_code(req, 400),
					Some(p) => reply_200_xml(
						req,
						BcXml {
							dingdong_device_opt: Some(p.clone()),
							..Default::default()
						},
					),
				}
			}
			MSG_ID_GET_DING_DONG_SILENT => {
				let xml = inspect_xml(req);
				assert_eq!(
					xml.dingdong_silent_mode
						.as_ref()
						.expect("609 rides dingdongSilentMode")
						.id
						.as_deref(),
					cfg.chime_id.as_deref(),
					"the silent read must carry the chosen id"
				);
				match &cfg.silent {
					None => reply_err_code(req, 400),
					Some(s) => reply_200_xml(
						req,
						BcXml {
							dingdong_silent_mode: Some(s.clone()),
							..Default::default()
						},
					),
				}
			}
			MSG_ID_GET_DING_DONG_CFG => match &cfg.events {
				None => reply_err_code(req, 400),
				Some(e) => reply_200_xml(
					req,
					BcXml {
						device_cfg: e.clone(),
						..Default::default()
					},
				),
			},
			other => panic!("not a chime read msg id: {other}"),
		}
	}

	/// Script the whole read sweep answering with `cfg`'s parts, in the order
	/// [`crate::bc_protocol::BcCamera::get_ding_dong_cfg`] asks them. The 485/609 questions are only asked
	/// — and only scripted — when the fixture has a chime id.
	fn expect_read(mock: MockConnectionBuilder, cfg: &Arc<DingDongCfg>) -> MockConnectionBuilder {
		let m = {
			let cfg = cfg.clone();
			mock.expect_msg(MSG_ID_DING_DONG_CTRL)
				.reply_with(move |req| reply_part(req, MSG_ID_DING_DONG_CTRL, &cfg))
		};
		let m = {
			let cfg = cfg.clone();
			m.expect_msg(MSG_ID_GET_DING_DONG_LIST)
				.reply_with(move |req| reply_part(req, MSG_ID_GET_DING_DONG_LIST, &cfg))
		};
		let m = if cfg.chime_id.is_some() {
			let cfg2 = cfg.clone();
			let m = m.expect_msg(MSG_ID_DING_DONG_OPT).reply_with(move |req| {
				reply_part(req, MSG_ID_DING_DONG_OPT, &cfg2)
			});
			let cfg3 = cfg.clone();
			m.expect_msg(MSG_ID_GET_DING_DONG_SILENT)
				.reply_with(move |req| reply_part(req, MSG_ID_GET_DING_DONG_SILENT, &cfg3))
		} else {
			m
		};
		let cfg = cfg.clone();
		m.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(move |req| reply_part(req, MSG_ID_GET_DING_DONG_CFG, &cfg))
	}

	fn assert_channel_extension(req: &Bc) {
		let BcBody::ModernMsg(ModernMsg { extension, .. }) = &req.body else {
			panic!("modern body expected");
		};
		assert_eq!(
			extension.as_ref().expect("extension").channel_id,
			Some(0),
			"the chime rides the doorbell's session, so every frame carries its channel"
		);
	}

	#[tokio::test]
	async fn get_assembles_every_part_and_picks_the_online_chime() {
		let cfg = Arc::new(paired_fixture());
		let mock = expect_read(MockConnection::new(), &cfg).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.get_ding_dong_cfg()
			.await
			.expect("a fully answered sweep reads cleanly");
		assert_eq!(
			got, *cfg,
			"the returned block must equal what the camera sent, tag for tag"
		);
	}

	#[tokio::test]
	async fn get_asks_per_chime_reads_for_the_online_id_first() {
		// Offline first in the list, online second: the per-chime questions
		// must carry the ONLINE id (reply_part asserts the request bodies),
		// because the offline chime's params are the stale ones.
		let mut cfg = paired_fixture();
		cfg.chimes = Some(vec![
			DingDongDevice {
				id: Some("2".into()),
				name: Some("Bas".into()),
				netstate: Some("0".into()),
				..Default::default()
			},
			DingDongDevice {
				device_id: Some("1".into()),
				// This module keeps non-ASCII values on purpose: they stand in for what an operator
				// would type or a camera would answer, so they are data being tested, not prose to translate.
				device_name: Some("Étage".into()),
				net_state: Some("1".into()),
				..Default::default()
			},
		]);
		let cfg = Arc::new(cfg);
		let mock = expect_read(MockConnection::new(), &cfg).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam.get_ding_dong_cfg().await.unwrap();
		assert_eq!(got.chime_id.as_deref(), Some("1"));
	}

	#[tokio::test]
	async fn get_softens_refused_parts_but_keeps_the_rest() {
		// A doorbell without a wireless chime: 484 refuses, the per-chime
		// reads are never even asked, and the aggregate still carries the
		// wired block.
		let mut cfg = paired_fixture();
		cfg.chimes = None;
		cfg.chime_id = None;
		cfg.params = None;
		cfg.silent = None;
		cfg.ctrl = Some(DingDongCtrl {
			version: "1.1".into(),
			type_: Some("dingdong".into()),
			bopen: Some("1".into()),
			time: Some("30".into()),
			..Default::default()
		});
		let cfg = Arc::new(cfg);
		let mock = expect_read(MockConnection::new(), &cfg).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.get_ding_dong_cfg()
			.await
			.expect("one refused part must not sink the read");
		assert_eq!(got, *cfg, "refused parts are None, answered parts intact");
		assert_eq!(got.chimes, None, "refused 484 is 'absent', not 'empty list'");
	}

	#[tokio::test]
	async fn get_with_no_chime_block_at_all_is_unintelligible_not_empty() {
		// Every part answers 200-with-nothing: the camera is talking, but it
		// is not speaking the chime family. "No chime"is a list of None
		// parts; "nonsense"is this error — the caller must not read a
		// missing chime out of a reply that merely carried nothing.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with(|req| reply_200_empty(req))
			.expect_msg(MSG_ID_GET_DING_DONG_LIST)
			.reply_with(|req| reply_200_empty(req))
			.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(|req| reply_200_empty(req))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.get_ding_dong_cfg()
			.await
			.expect_err("no block anywhere must be an error");
		assert!(
			matches!(err, Error::UnintelligibleReply { .. }),
			"wrong error variant: {err:?}"
		);
	}

	/// The pushed spelling of the 484 inventory: `<dingdongList>` instead of
	/// `<dingdongDeviceInfo>` (msg 484 is reply *and* push). Reading only
	/// the reply spelling is precisely how a camera with a paired chime ends up
	/// publishing an empty inventory: the unknown root decodes to an empty
	/// vector without complaining, because [`BcXml`] has no
	/// `deny_unknown_fields`.
	#[tokio::test]
	async fn get_reads_the_dingdong_list_spelling_of_484() {
		use crate::bc::xml::DingDongDevice;
		let chime = DingDongDevice {
			device_id: Some("7".into()),
			device_name: Some("Carillon du couloir".into()),
			net_state: Some("1".into()),
			..Default::default()
		};
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_DING_DONG_LIST)
			.reply_with(move |req| {
				reply_200_xml(
					req,
					BcXml {
						dingdong_list: Some(crate::bc::xml::DingDongList {
							device_info: vec![chime.clone()],
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_DING_DONG_SILENT)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.get_ding_dong_cfg()
			.await
			.expect("a chime listed under the pushed spelling is a paired chime");
		assert_eq!(
			got.chimes.as_ref().map(|l| l.len()),
			Some(1),
			"the `<dingdongList>` entries must arrive as the inventory"
		);
		assert_eq!(
			got.chime_id.as_deref(),
			Some("7"),
			"and be addressable: this is the id every later 485/487 frame needs"
		);
		assert!(
			!got.list_answered_without_entry,
			"an entry was read, so nothing is 'unread'"
		);
	}

	/// What the field camera forced, and what the bridge could not do: the 484
	/// answers inside a `<dingdongList>` / `<pairedList>` container. While that
	/// shape was not decoded, no id was retained - so the 485 (volume, LED) and
	/// the 609 (silent mode) were **never asked**, and the 486, wrapped the same
	/// way, never surfaced. This test holds the whole chain: the list as it came
	/// back from the field, the questions addressed to the named chime, and what
	/// the answers contain.
	#[tokio::test]
	async fn get_follows_the_field_camera_into_the_per_chime_reads() {
		use crate::bc::xml::{DingDongCtrl, DingDongDeviceOpt, DingDongSilent};
		// The two replies copied exactly as they arrived from the field.
		// [`BcXml`] is not `Clone`, so every bug report re-decodes them from the
		// text instead of cloning a 96 KB block.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						dingdong_ctrl: Some(DingDongCtrl {
							version: "1.1".into(),
							opt: None,
							channel_id: None,
							type_: Some("unknow".into()),
							bopen: Some("0".into()),
							enable: None,
							bsave: Some("0".into()),
							time: Some("0".into()),
							result: Some("adptNotAC".into()),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_DING_DONG_LIST)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml::try_parse(br#"<body><dingdongList version="1.1"><maxPairNumber>5</maxPairNumber><pairedList><dingdongDeviceInfo><id>882092649</id><netstate>2</netstate><name>Reolink Chime</name></dingdongDeviceInfo></pairedList></dingdongList></body>"#).unwrap(),
				)
			})
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with(|req| {
				let xml = inspect_xml(req);
				assert_eq!(
					xml.dingdong_device_opt.as_ref().and_then(|o| o.id.as_deref()),
					Some("882092649"),
					"485 must be addressed to the chime the container just named"
				);
				reply_200_xml(
					req,
					BcXml {
						dingdong_device_opt: Some(DingDongDeviceOpt {
							version: "1.1".into(),
							opt: Some("getParam".into()),
							id: Some("882092649".into()),
							name: Some("Reolink Chime".into()),
							vol_level: Some("3".into()),
							led_state: Some("1".into()),
							music_id: None,
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_DING_DONG_SILENT)
			.reply_with(|req| {
				let xml = inspect_xml(req);
				assert_eq!(
					xml.dingdong_silent_mode.as_ref().and_then(|s| s.id.as_deref()),
					Some("882092649"),
					"609 too"
				);
				reply_200_xml(
					req,
					BcXml {
						dingdong_silent_mode: Some(DingDongSilent {
							version: "1.1".into(),
							id: Some("882092649".into()),
							time: Some("0".into()),
							type_: None,
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml::try_parse(br#"<body><dingdongCfg version="1.1"><deviceCfg><id>882092649</id><name>Reolink Chime</name><version>00A2</version><alarminCfg><type>visitor</type><valid>1</valid><musicId>0</musicId></alarminCfg></deviceCfg></dingdongCfg></body>"#).unwrap(),
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.get_ding_dong_cfg()
			.await
			.expect("the field camera's chime reads end to end");
		assert_eq!(got.chime_id.as_deref(), Some("882092649"));
		assert!(!got.list_answered_without_entry, "something was addressed");
		assert_eq!(
			got.params.as_ref().and_then(|p| p.vol_level.as_deref()),
			Some("3"),
			"the chime volume — never once asked for until this shape decoded"
		);
		assert!(got.silent.is_some(), "the mute window was asked for too");
		let events = got.events.expect("the wrapped 486 reply IS the events block");
		assert_eq!(events[0].firm_version.as_deref(), Some("00A2"));
		assert_eq!(events[0].alarmin_cfg[0].type_.as_deref(), Some("visitor"));
		assert_eq!(
			got.ctrl.as_ref().and_then(|c| c.result.as_deref()),
			Some("adptNotAC"),
			"and the wired chime's reason comes back with it"
		);
	}

	/// The other half of the same measurement: 484 answers `200` but names
	/// nothing it is possible to address. That is **not** "no chime paired "—
	/// on the field camera a chime *was* paired — and it must not be reported as
	/// an empty list either. `list_answered_without_entry` is the flag the bridge
	/// publishes as the reason beside the empty inventory.
	#[tokio::test]
	async fn get_reports_a_list_that_answered_without_naming_anything() {
		use crate::bc::xml::DingDongDevice;
		// A device block that carries a name and no numeric id: a real shape for
		// "the camera described the chime but did not address it ".
		let mut silent = DingDongDevice::default();
		silent.name = Some("Carillon".into());
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_DING_DONG_LIST)
			.reply_with(move |req| {
				reply_200_xml(
					req,
					BcXml {
						dingdong_list: Some(crate::bc::xml::DingDongList {
							device_info: vec![silent.clone()],
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.get_ding_dong_cfg()
			.await
			.expect("an answered 484 is an answer, even without an entry");
		assert_eq!(
			got.chimes.as_ref().map(|l| l.len()),
			Some(1),
			"the entry the camera printed stays visible: it is the evidence in the re-read JSON"
		);
		assert_eq!(
			got.chimes.as_ref().and_then(|l| l.first()).and_then(|d| d.identifier()),
			None,
			"a name without a numeric id is not an address"
		);
		assert_eq!(got.chime_id, None, "and therefore nothing to address in 485");
		assert!(
			got.list_answered_without_entry,
			"but the camera did answer 484: that distinction is the published reason"
		);
	}

	#[tokio::test]
	async fn set_writes_only_the_changed_part_and_returns_the_reread() {
		let before = Arc::new(paired_fixture());
		let mut after = paired_fixture();
		after.params.as_mut().unwrap().vol_level = Some("4".into());
		let after = Arc::new(after);

		let mut want = paired_fixture();
		want.params.as_mut().unwrap().vol_level = Some("4".into());

		// Exactly: one read sweep, ONE 485 set, one read sweep. A 483 / 610 /
		// 487 frame — the wired block, the (unchanged) silent mode and the
		// melodies are untouched — would hit an expectation that doesn't
		// exist, kill the mux and turn the re-read into a transport error.
		let mock = expect_read(MockConnection::new(), &before)
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with_xml(move |req, xml| {
				let sent = xml
					.dingdong_device_opt
					.as_ref()
					.expect("the setParam frame rides dingdongDeviceOpt");
				assert_eq!(sent.opt.as_deref(), Some("setParam"));
				assert_eq!(sent.id.as_deref(), Some("1"));
				assert_eq!(
					sent.vol_level.as_deref(),
					Some("4"),
					"the patched tag carries the requested text"
				);
				assert_eq!(
					sent.led_state.as_deref(),
					Some("1"),
					"unchanged printed tags go back out as the camera wrote them"
				);
				assert_eq!(sent.name.as_deref(), Some("Carillon"));
				assert!(sent.music_id.is_none(), "a melody is 487's business");
				reply_200_empty(req)
			});
		let mock = expect_read(mock, &after).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect("a one-tag change writes and reads back");
		assert_eq!(
			got, *after,
			"the method returns the re-read, whole: the caller compares it, not our word"
		);
	}

	#[tokio::test]
	async fn set_refuses_a_tag_the_camera_never_printed_and_sends_nothing() {
		// Only the read sweep is scripted: a SET frame would reach the mock
		// with no expectation and produce a transport error — so getting
		// THIS error text back proves nothing was written.
		let mut thin = paired_fixture();
		thin.params.as_mut().unwrap().vol_level = None;
		let thin = Arc::new(thin);
		let mock = expect_read(MockConnection::new(), &thin).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// `want` came from a richer read; the fresh one has no volLevel, so
		// writing it would invent a tag.
		let mut want = paired_fixture();
		want.params.as_mut().unwrap().vol_level = Some("4".into());
		let err = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect_err("inventing volLevel must be refused");
		let msg = err.to_string();
		assert!(msg.contains("volLevel"), "must name the tag: {msg}");
		assert!(msg.contains("never added"), "must say why: {msg}");
	}

	#[tokio::test]
	async fn set_rejected_write_is_an_error_and_skips_the_reread() {
		let before = Arc::new(paired_fixture());
		let mut want = paired_fixture();
		want.params.as_mut().unwrap().vol_level = Some("4".into());
		// 500 on the SET, and NO re-read scripted: the error that comes back
		// must be the camera's 500 — a code that read again anyway would
		// instead die on the exhausted mock with a transport error.
		let mock = expect_read(MockConnection::new(), &before)
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect_err("a refused write must surface");
		match err {
			Error::CameraServiceUnavailable { code, .. } => assert_eq!(code, 500),
			other => panic!("wrong error: {other:?}"),
		}
	}

	#[tokio::test]
	async fn set_keeps_the_enable_spelling_and_stamps_the_set_frame() {
		// A wired chime that answered with <enable>: the write must go back
		// out with <enable>, machineStateSet and bsave=1 — and must NOT add
		// <bopen>, the second spelling is not ours to introduce. The re-read
		// sweep completes the exchange, so the frame assertions above are
		// load-bearing: if they panic the mux dies and the set() fails.
		let mut before = paired_fixture();
		before.ctrl = Some(DingDongCtrl {
			version: "1.1".into(),
			enable: Some("1".into()),
			time: Some("30".into()),
			..Default::default()
		});
		let before = Arc::new(before);
		let mut after = (*before).clone();
		after.ctrl.as_mut().unwrap().time = Some("45".into());
		let after = Arc::new(after);

		let mut want = (*before).clone();
		want.ctrl.as_mut().unwrap().time = Some("45".into());

		let mock = expect_read(MockConnection::new(), &before)
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with_xml(move |req, xml| {
				let sent = xml
					.dingdong_ctrl
					.as_ref()
					.expect("machineStateSet rides dingdongCtrl");
				assert_eq!(sent.opt.as_deref(), Some("machineStateSet"));
				assert_eq!(sent.bsave.as_deref(), Some("1"), "the documented frame persists");
				assert_eq!(sent.enable.as_deref(), Some("1"));
				assert_eq!(sent.time.as_deref(), Some("45"));
				let wire = String::from_utf8(xml.serialize(vec![]).unwrap()).unwrap();
				assert!(wire.contains("<enable>1</enable>"), "{wire}");
				assert!(!wire.contains("<bopen>"), "second spelling invented: {wire}");
				reply_200_empty(req)
			});
		let mock = expect_read(mock, &after).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect("the write must be accepted and read back");
		assert_eq!(got, *after);
	}

	#[tokio::test]
	async fn set_silent_sends_time_and_hardcoded_kind_only_when_changed() {
		let before = Arc::new(paired_fixture());
		let mut after = (*before).clone();
		after.silent.as_mut().unwrap().time = Some("600".into());
		let after = Arc::new(after);
		let mut want = (*before).clone();
		want.silent.as_mut().unwrap().time = Some("600".into());

		let mock = expect_read(MockConnection::new(), &before)
			.expect_msg(MSG_ID_SET_DING_DONG_SILENT)
			.reply_with_xml(move |req, xml| {
				let sent = xml
					.dingdong_silent_mode
					.as_ref()
					.expect("the silent frame rides dingdongSilentMode");
				assert_eq!(sent.id.as_deref(), Some("1"));
				assert_eq!(sent.time.as_deref(), Some("600"));
				assert_eq!(
					sent.type_.as_deref(),
					Some("63"),
					"the documented frame's hard-coded type, meaning notwithstanding"
				);
				reply_200_empty(req)
			});
		let mock = expect_read(mock, &after).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect("a changed silent time writes and reads back");
		assert_eq!(got, *after);

		// And the re-arm guard: an unchanged block sends NOTHING and returns
		// the single fresh read — the mock scripts exactly one sweep, so a
		// "faithful echo"SET would hit the exhausted mock and fail below.
		let mock = expect_read(MockConnection::new(), &before).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.set_ding_dong_cfg(&(*before).clone())
			.await
			.expect("an unchanged block writes nothing and returns the fresh read");
		assert_eq!(got, *before);
	}

	#[tokio::test]
	async fn set_melody_writes_one_full_devicecfg_per_changed_device() {
		let before = Arc::new(paired_fixture());
		let mut after = (*before).clone();
		let mut want = (*before).clone();
		for e in after.events.as_mut().unwrap() {
			for c in e.alarmin_cfg.iter_mut() {
				c.music_id = Some("5".into());
			}
		}
		for e in want.events.as_mut().unwrap() {
			for c in e.alarmin_cfg.iter_mut() {
				c.music_id = Some("5".into());
			}
		}
		let after = Arc::new(after);
		// One device, both entries changed → exactly ONE 487.
		let mock = expect_read(MockConnection::new(), &before)
			.expect_msg(MSG_ID_SET_DING_DONG_CFG)
			.reply_with_xml(move |req, xml| {
				let wrap = xml
					.dingdong_cfg
					.as_ref()
					.expect("487 wraps the block in dingdongCfg");
				assert_eq!(wrap.device_cfg.len(), 1, "one block per request");
				let dev = &wrap.device_cfg[0];
				assert_eq!(dev.id.as_deref(), Some("1"));
				assert!(dev.ring_id.is_none(), "ringId is not part of the write frame");
				assert_eq!(dev.alarmin_cfg.len(), 2, "the whole block, not a fragment");
				// Spelling: `valid` stays `valid`; `switch` moves to the
				// documented 487 name with its text untouched.
				assert_eq!(dev.alarmin_cfg[0].valid.as_deref(), Some("1"));
				assert_eq!(dev.alarmin_cfg[0].music_id.as_deref(), Some("5"));
				assert_eq!(dev.alarmin_cfg[1].valid.as_deref(), Some("0"));
				assert_eq!(dev.alarmin_cfg[1].switch_, None);
				assert_eq!(dev.alarmin_cfg[1].type_.as_deref(), Some("visitor"));
				reply_200_empty(req)
			});
		let mock = expect_read(mock, &after).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect("the melody write is accepted and read back");
		assert_eq!(got, *after);
	}

	#[tokio::test]
	async fn set_refuses_to_add_or_retarget_event_blocks() {
		let before = Arc::new(paired_fixture());
		// Same device, one MORE entry: that's inventing an event.
		let mut want = (*before).clone();
		want.events.as_mut().unwrap()[0]
			.alarmin_cfg
			.push(DingDongAlarmCfg {
				type_: Some("cat".into()),
				valid: Some("1".into()),
				music_id: Some("0".into()),
				..Default::default()
			});
		let mock = expect_read(MockConnection::new(), &before).build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.set_ding_dong_cfg(&want)
			.await
			.expect_err("an invented <alarminCfg> must be refused");
		assert!(err.to_string().contains("never added or removed"));
	}

	#[tokio::test]
	async fn ring_sends_the_documented_frame_and_a_rejection_surfaces() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with_xml(|req, xml| {
				assert_channel_extension(req);
				let sent = xml.dingdong_device_opt.as_ref().unwrap();
				assert_eq!(sent.opt.as_deref(), Some("ringWithMusic"));
				assert_eq!(sent.id.as_deref(), Some("1"));
				assert_eq!(sent.music_id.as_deref(), Some("2"));
				assert!(sent.vol_level.is_none());
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.ring_ding_dong("1", "2").await.expect("a 200 is success");

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_OPT)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.ring_ding_dong("1", "0")
			.await
			.expect_err("a refused ring must surface, not be swallowed");
		assert!(matches!(err, Error::CameraServiceUnavailable { code: 400, .. }));

		// Non-numeric ids never reach the wire — no expectation scripted, so
		// a frame sent anyway would break the mock differently.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.ring_ding_dong("</dingdongDeviceOpt>", "0")
			.await
			.expect_err("an invented chime id is a caller error");
		assert!(err.to_string().contains("numeric"));
	}
}
