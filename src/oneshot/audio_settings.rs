//! `audio-settings` one-shot — read the camera's `<audioCfg>` block (msg 264)
//! and, with a volume option, write it back (msg 265).
//!
//! # Why this command exists
//!
//! The intercom volume (`talkAndReplyVolume`) exists **nowhere else** in the
//! protocol: not in `<TalkConfig>`, not in `<AudioPlay>`, not in any of the
//! other config blocks this binary already reads. If the doorbell is too quiet
//! to hear from the app, this block is the setting to change.
//!
//! # Why the write looks like this
//!
//! There is no per-field setter. Msg 265 stores the whole block, and this
//! firmware is documented (and behaved) to reject — or, worse, silently ignore
//! — a block that leaves out tags it just sent. So the command always reads
//! first, patches **only** the tags the operator named, and sends the block
//! back complete, under the element spelling the camera used. Tags nobody has
//! worked out (`audioSelect`, `audioListId`, `preAlarm`, `timeout`) are
//! therefore echoed back untouched rather than dropped.
//!
//! A tag the camera never printed is never invented either: `--interphone` on a
//! firmware that has no `talkAndReplyVolume` is a usage error, because adding
//! the tag would be a guess about a block we don't control.
//!
//! The block is read again after the write, and each named tag is reported as
//! `before → after` with what the camera actually kept, so a write the firmware
//! swallowed is visible instead of implied.
//!
//! # Why it wants the concrete `BcCamera`
//!
//! `get_audio_cfg` / `set_audio_cfg` live on `BcCamera` and pointedly not on the
//! `CameraDriver` trait — the trait is the seam the services go through, and a
//! volume setting that needs a read-modify-write of a raw block belongs to the
//! CLI diagnostic, not to every caller. Same route as `talk-abilite`: [`run`]
//! takes `&BcCamera` and is reached from `run_oneshot`'s concrete-handle branch,
//! while [`run_via_driver`] answers the trait-object route with a clear error.
//!
//! # The ability gate is off unless `--gate`
//!
//! This doorbell's `abilityInfo` under-reports (the same family answers `405` to
//! `<HaCfg>` while accepting `<Email>`), so the advertised list is not evidence
//! that msg 264 is unsupported. Reading anyway is free and harmless; the answer
//! is the proof. `--gate` restores the check for anyone who wants the old veto.

use anyhow::{Context, Result};
use bairelay_neolink_core::bc::xml::{AudioCfg, AudioCfgTag};
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver};

use super::errors::UsageError;
use super::output::{AudioCfgChange, AudioCfgInfo, Outcome};

// The four tags this command writes, named the way the camera names them, so
// the report and the XML agree.
const TAG_VOLUME: &str = "volume";
const TAG_INTERCOM: &str = "talkAndReplyVolume";
const TAG_MESSAGES: &str = "visitorVolume";
const TAG_VISITOR: &str = "visitorLoudspeaker";

/// What the operator asked for. Every volume is an override; `None` means
/// "leave the camera's text alone".
#[derive(Debug, Default, Clone)]
pub struct Edit {
	pub volume: Option<u8>,
	pub talk: Option<u8>,
	pub visitor_volume: Option<u8>,
	/// `visitorLoudspeaker`: 0 or 1.
	pub loudspeaker: Option<u8>,
	/// Let the advertised abilities veto the command (off by default).
	pub gate: bool,
}

impl Edit {
	/// True when nothing was asked for, i.e. a plain read.
	pub fn is_empty(&self) -> bool {
		self.volume.is_none()
			&& self.talk.is_none()
			&& self.visitor_volume.is_none()
			&& self.loudspeaker.is_none()
	}

	/// Reject an out-of-range value **before the camera is woken**: a battery
	/// doorbell that is poked awake to be told `--volume 120` costs a wake
	/// window and learns nothing. The documented range is 0-100 for the three
	/// volumes and 0/1 for the visitor loudspeaker.
	pub fn check(&self) -> Result<()> {
		for (flag, value) in [
			("--volume", self.volume),
			("--talk", self.talk),
			("--visitor-volume", self.visitor_volume),
		] {
			if let Some(v) = value {
				if v > 100 {
					return Err(UsageError::new(format!(
						"{flag} must be between 0 and 100, got {v}"
					))
					.into());
				}
			}
		}
		if let Some(v) = self.loudspeaker {
			if v > 1 {
				return Err(UsageError::new(format!(
					"--loudspeaker is a switch: pass 0 or 1, got {v}"
				))
				.into());
			}
		}
		Ok(())
	}
}

/// Read the block, and write it back with only the named tags changed.
pub async fn run(cam: &BcCamera, edit: Edit) -> Result<Outcome> {
	// Argument errors first: nothing here should cost a wake-up.
	edit.check()?;

	let before = cam
		.get_audio_cfg(edit.gate)
		.await
		.context("audioCfg read (cmd 264): the camera refused or never answered")?;

	if edit.is_empty() {
		return Ok(Outcome::AudioSettings {
			tag: before.tag.element_name().to_string(),
			changed: false,
			block: info(&before),
			changes: Vec::new(),
		});
	}

	// Read-modify-write: start from the block as the camera printed it, and
	// track what was asked for so the re-read can be reported against it.
	let mut patched = before.clone();
	let mut asked: Vec<(&'static str, String, String)> = Vec::new();
	if let Some(v) = edit.volume {
		asked.push((TAG_VOLUME, patched.volume.clone(), v.to_string()));
		patched.volume = v.to_string();
	}
	if let Some(v) = edit.talk {
		let from = present(
			patched.talk_and_reply_volume.as_deref(),
			TAG_INTERCOM,
			"--talk",
		)?;
		asked.push((TAG_INTERCOM, from, v.to_string()));
		patched.talk_and_reply_volume = Some(v.to_string());
	}
	if let Some(v) = edit.visitor_volume {
		let from = present(
			patched.visitor_volume.as_deref(),
			TAG_MESSAGES,
			"--visitor-volume",
		)?;
		asked.push((TAG_MESSAGES, from, v.to_string()));
		patched.visitor_volume = Some(v.to_string());
	}
	if let Some(v) = edit.loudspeaker {
		let from = present(
			patched.visitor_loudspeaker.as_deref(),
			TAG_VISITOR,
			"--loudspeaker",
		)?;
		asked.push((TAG_VISITOR, from, v.to_string()));
		patched.visitor_loudspeaker = Some(v.to_string());
	}

	// `set_audio_cfg` sends this block as it stands — same element spelling,
	// every tag still in it — and returns what the camera holds afterwards.
	let after = cam
		.set_audio_cfg(&patched, edit.gate)
		.await
		.context("audioCfg write (cmd 265): the camera refused or never answered")?;

	let changes = asked
		.into_iter()
		.map(|(tag, from, requested)| AudioCfgChange {
			tag: tag.to_string(),
			before: from,
			requested,
			after: value_of(&after, tag).unwrap_or_default(),
		})
		.collect();

	Ok(Outcome::AudioSettings {
		tag: after.tag.element_name().to_string(),
		changed: true,
		block: info(&after),
		changes,
	})
}

/// Trait-object route: see the module docs — this command cannot run through
/// [`CameraDriver`]. Mirrors [`crate::oneshot::chime_settings::run_via_driver`].
pub async fn run_via_driver(_cam: &dyn CameraDriver) -> Result<Outcome> {
	anyhow::bail!(
		"audio-settings requires a concrete BcCamera; not supported through CameraDriver"
	);
}

/// The camera printed this tag, so we may change it; it didn't, so asking to
/// change it is a usage error rather than a licence to invent it.
fn present(current: Option<&str>, tag: &str, flag: &str) -> Result<String> {
	current.map(str::to_string).ok_or_else(|| {
		anyhow::Error::from(UsageError::new(format!(
			"this camera's audioCfg block has no <{tag}> tag, so {flag} cannot set it; \
			 refusing to add a tag the firmware did not send"
		)))
	})
}

/// The text the camera holds for one of the four writable tags.
fn value_of(cfg: &AudioCfg, tag: &str) -> Option<String> {
	match tag {
		TAG_VOLUME => Some(cfg.volume.clone()),
		TAG_INTERCOM => cfg.talk_and_reply_volume.clone(),
		TAG_MESSAGES => cfg.visitor_volume.clone(),
		TAG_VISITOR => cfg.visitor_loudspeaker.clone(),
		_ => None,
	}
}

/// Flatten the block for the report: every tag as the camera wrote it, with
/// the doorbell tags omitted (not emptied) when this firmware doesn't have them.
fn info(cfg: &AudioCfg) -> AudioCfgInfo {
	AudioCfgInfo {
		version: cfg.version.clone(),
		channel_id: cfg.channel_id.clone(),
		volume: cfg.volume.clone(),
		timeout: cfg.timeout.clone(),
		audio_select: cfg.audio_select.clone(),
		pre_alarm: cfg.pre_alarm.clone(),
		audio_list_id: cfg.audio_list_id.clone(),
		talk_and_reply_volume: cfg.talk_and_reply_volume.clone(),
		visitor_volume: cfg.visitor_volume.clone(),
		visitor_loudspeaker: cfg.visitor_loudspeaker.clone(),
		pascal: matches!(cfg.tag, AudioCfgTag::Pascal),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn block() -> AudioCfg {
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

	#[test]
	fn audio_settings_edit_is_empty_only_without_options() {
		assert!(Edit::default().is_empty());
		// `--gate` alone is still a read: it changes who is allowed to ask,
		// never what gets sent, so it must not turn the read into a write.
		assert!(
			Edit {
				gate: true,
				..Default::default()
			}
			.is_empty(),
			"--gate on its own must stay read-only"
		);
		assert!(!Edit {
			talk: Some(0),
			..Default::default()
		}
		.is_empty());
		// `0` is a value, not an absence: the quietest intercom is still a write.
		assert!(!Edit {
			volume: Some(0),
			..Default::default()
		}
		.is_empty());
	}

	#[test]
	fn audio_settings_check_refuses_out_of_range_values() {
		for (flag, edit) in [
			(
				"--volume",
				Edit {
					volume: Some(101),
					..Default::default()
				},
			),
			(
				"--talk",
				Edit {
					talk: Some(200),
					..Default::default()
				},
			),
			(
				"--visitor-volume",
				Edit {
					visitor_volume: Some(101),
					..Default::default()
				},
			),
		] {
			let err = edit
				.check()
				.expect_err("a volume above 100 must be refused");
			assert!(
				format!("{:#}", err).contains(flag),
				"the error must name {flag}: {err:#}"
			);
		}
		let err = Edit {
			loudspeaker: Some(2),
			..Default::default()
		}
		.check()
		.expect_err("--loudspeaker 2 is not a switch value");
		assert!(format!("{:#}", err).contains("--loudspeaker"));
		// The edges of the documented range are fine.
		Edit {
			volume: Some(0),
			talk: Some(100),
			loudspeaker: Some(0),
			..Default::default()
		}
		.check()
		.expect("0 and 100 are in range");
	}

	#[test]
	fn audio_settings_value_of_reads_the_four_writable_tags() {
		let cfg = block();
		assert_eq!(value_of(&cfg, TAG_VOLUME).as_deref(), Some("50"));
		assert_eq!(
			value_of(&cfg, TAG_INTERCOM).as_deref(),
			Some("40"),
			"the intercom volume is the whole reason this command exists"
		);
		assert_eq!(value_of(&cfg, TAG_MESSAGES).as_deref(), Some("70"));
		assert_eq!(value_of(&cfg, TAG_VISITOR).as_deref(), Some("1"));
		assert_eq!(value_of(&cfg, "nope"), None);
	}

	#[test]
	fn audio_settings_missing_doorbell_tag_is_a_usage_error_not_an_addition() {
		let mut cfg = block();
		cfg.talk_and_reply_volume = None;
		let err = present(
			cfg.talk_and_reply_volume.as_deref(),
			TAG_INTERCOM,
			"--talk",
		)
		.expect_err("a tag the camera didn't print may not be invented");
		let msg = format!("{:#}", err);
		assert!(msg.contains(TAG_INTERCOM), "{msg}");
		assert!(msg.contains("--talk"), "{msg}");
		// A tag that is present is copied out verbatim, including an empty one.
		assert_eq!(present(Some(""), TAG_MESSAGES, "--visitor-volume").unwrap(), "");
	}

	#[test]
	fn audio_settings_info_keeps_undocumented_tags_and_omits_absent_ones() {
		let mut cfg = block();
		cfg.timeout = "0010".into();
		cfg.visitor_volume = None;
		let info = info(&cfg);
		assert_eq!(info.timeout, "0010", "camera text is kept as written");
		assert_eq!(info.audio_select, "0");
		assert_eq!(info.audio_list_id, "0");
		assert_eq!(info.pre_alarm, "0");
		assert_eq!(info.visitor_volume, None);
		assert_eq!(info.talk_and_reply_volume.as_deref(), Some("40"));
		assert!(!info.pascal);
		let json = serde_json::to_value(&info).unwrap();
		assert!(json.get("visitor_volume").is_none(), "{json}");
		assert_eq!(json["talk_and_reply_volume"].as_str(), Some("40"));
	}

	#[tokio::test]
	async fn audio_settings_run_via_driver_bails_with_named_reason() {
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build();
		let err = run_via_driver(&*fake)
			.await
			.expect_err("trait route must not succeed");
		assert!(format!("{:#}", err).contains("requires a concrete BcCamera"));
	}
}
