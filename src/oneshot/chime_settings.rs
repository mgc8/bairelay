//! `chime-settings` one-shot — read the paired-chime (ding-dong) family
//! (read sweep 483 / 484 / 485 / 609 / 486) and, with an option, write the
//! changed parts back (485 / 483 / 610 / 487), then read the whole family
//! again. With `--sonner`, make the chime ring once (485 `ringWithMusic`).
//!
//! # Why this command exists
//!
//! The paired chime answers **through the doorbell's session** — it has no
//! address of its own — and none of its settings are anywhere else in the
//! protocol: volume (`volLevel`), the status LED (`ledState`), the per-event
//! melodies (`musicId`) and the mute window (609's `<time>`) are only
//! reachable through this family. If the chime is inaudible or plays the wrong
//! tune, this is the block to look at.
//!
//! # Why the read is five questions
//!
//! The chime is spread over five commands (see
//! [`bairelay_neolink_core::bc_protocol::BcCamera::get_ding_dong_cfg`]): the
//! wired block (483), the paired-chime list (484), the per-chime parameters
//! (485), the silent mode (609) and the per-event melodies (486). Each part a
//! camera doesn't have **refuses its question**, so the report distinguishes
//! "refused" (part absent — no wireless chime, no wired input) from "answered
//! with nothing" (empty list). A fully silent camera is an error, not an empty
//! report — a missing chime must never be inferred from a reply that merely
//! carried nothing.
//!
//! # Why the write is a diff
//!
//! Same law as `audio-settings`: a tag the camera never printed is never
//! invented (asking for it is a usage error and nothing goes on the wire),
//! and whole blocks are rewritten, never fragments — this firmware silently
//! ignores truncated blocks. On top of that, this family shares the 265
//! silence quirk (a SET can be accepted without a reply), and msg 610 re-arms
//! the mute countdown just by arriving, so `set_ding_dong_cfg` re-reads first
//! and sends **only the parts whose text actually changed**. An unchanged
//! `--silencieux` therefore costs no command: re-writing it would re-mute the
//! chime for no reason. After a write the whole family is read again and each
//! named tag is reported as `before → after` with what the camera actually
//! kept.
//!
//! # Why `--sonner` belongs here
//!
//! It is the only command in the family with **physical feedback**: the doorbell
//! answering 485 proves the session reaches the chime, but only the operator's
//! ears prove the chime itself is alive. That makes a deliberate, single ring
//! the definitive pairing test — so it is opt-in here and nowhere automatic:
//! it makes noise, which is precisely the one side effect a settings command
//! must never have unasked.
//!
//! # Why there is no `--gate`
//!
//! `audio-settings` can consult the advertised abilities because `audioCfg` is
//! an abilityInfo token. The chime family has **no abilityInfo token at all**
//! — the commands themselves are the support test. A gate could only check a
//! token this tool invented, which would veto exactly the cameras that do have
//! a chime. If a future `abilities` dump shows a real chime token, the gate is
//! five lines and one decision.
//!
//! # Why the wired chime (483) has no option
//!
//! The hardwired block is read and reported (`ctrl`), and the core layer will
//! write it if a caller changes it — but no flag here maps to it on purpose.
//! Its `<time>` unit is undocumented (writing a number we can't interpret is
//! the guessing this tool refuses), and the read itself can physically ring a
//! wired chime, so a battery install shouldn't pay it casually. Volume, LED,
//! melodies and the mute window cover what the app itself exposes.
//!
//! # Why it wants the concrete `BcCamera`
//!
//! As with `audio-settings`: the chime methods live on `BcCamera`, pointedly
//! not on the `CameraDriver` trait — a read-modify-write of raw blocks belongs
//! to the CLI diagnostic, not to every service caller. [`run`] takes
//! `&BcCamera` and is reached from the concrete-handle branch; [`run_via_driver`]
//! answers the trait route with a clear error.

use anyhow::{Context, Result};
use bairelay_neolink_core::bc::xml::{DingDongCfg, DingDongDeviceCfg};
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver};

use super::errors::UsageError;
use super::output::{
	ChimeCtrlInfo, ChimeDeviceInfo, ChimeEventEntryInfo, ChimeEventInfo, ChimeParamsInfo,
	ChimeRingInfo, ChimeSilentInfo, DingDongCfgInfo, DingDongChange, Outcome,
};

/// What the operator asked for. Every value is an override; `None` means
/// "leave the camera's text alone". `sonner` performs no write — it rings
/// once after the read (and after the write, if there was one).
#[derive(Debug, Default, Clone)]
pub struct Edit {
	/// `volLevel`: chime volume, 0-4 per the documented range.
	pub volume: Option<u8>,
	/// `ledState`: status LED switch, 0 or 1.
	pub led: Option<u8>,
	/// `musicId`: melody for every event whose block the camera printed a
	/// melody on. Any integer — no melody list is documented anywhere.
	pub melody: Option<u32>,
	/// `<time>` of the silent-mode block: mute the chime for `N` (the unit is
	/// undocumented; the reference client sends seconds). `0` unmutes.
	pub silent: Option<u32>,
	/// Ring once (`ringWithMusic`) after the rest, as the live test.
	pub ring: bool,
}

impl Edit {
	/// True when no **write** was asked for. `--sonner` alone keeps this true:
	/// it changes what happens after the read, not what gets stored.
	pub fn is_empty(&self) -> bool {
		self.volume.is_none()
			&& self.led.is_none()
			&& self.melody.is_none()
			&& self.silent.is_none()
	}

	/// Reject an out-of-range value **before the camera is woken**: a battery
	/// doorbell poked awake to be told `--volume 90` costs a wake window and
	/// learns nothing. Only the two documented switches are bounded; `musicId`
	/// and the silent `<time>` have no documented range, so they are passed
	/// through as the camera's own text-domain (refusing a guess here would
	/// be a guess too).
	pub fn check(&self) -> Result<()> {
		if let Some(v) = self.volume {
			if v > 4 {
				return Err(UsageError::new(format!(
					"--volume must be between 0 and 4 (the documented chime range; \
					 the true ceiling is undocumented), got {v}"
				))
				.into());
			}
		}
		if let Some(v) = self.led {
			if v > 1 {
				return Err(UsageError::new(format!(
					"--led is a switch: pass 0 or 1, got {v}"
				))
				.into());
			}
		}
		Ok(())
	}
}

/// Which writable value an `asked` entry refers to, so the re-read can be
/// looked up by the same route the patch took.
#[derive(Debug, Clone)]
enum Asked {
	VolLevel,
	LedState,
	SilentTime,
	/// `musicId` of the event with this `<type>` (the display name carries it).
	Melody(String),
}

/// Read the family, and write back only what changed.
pub async fn run(cam: &BcCamera, edit: Edit) -> Result<Outcome> {
	// Argument errors first: nothing here should cost a wake-up.
	edit.check()?;

	let before = cam
		.get_ding_dong_cfg()
		.await
		.context("carillon read (cmds 483/484/485/609/486): the camera refused or never answered any part of the chime family")?;

	// Read-modify-write: start from the aggregate as the camera printed it,
	// and track what was asked so the re-read can be reported against it.
	let mut patched = before.clone();
	let mut asked: Vec<(Asked, String, String, String)> = Vec::new();

	if !edit.is_empty() {
		if let Some(v) = edit.volume {
			let params = patched.params.as_mut().ok_or_else(|| {
				UsageError::new(
					"the camera did not answer the 485 params question (no paired chime, \
					 or it refused it), so --volume has no block to change",
				)
			})?;
			let from = present(params.vol_level.as_deref(), "volLevel", "--volume")?;
			asked.push((Asked::VolLevel, "volLevel".into(), from, v.to_string()));
			params.vol_level = Some(v.to_string());
		}
		if let Some(v) = edit.led {
			let params = patched.params.as_mut().ok_or_else(|| {
				UsageError::new(
					"the camera did not answer the 485 params question, so --led has no block to change",
				)
			})?;
			let from = present(params.led_state.as_deref(), "ledState", "--led")?;
			asked.push((Asked::LedState, "ledState".into(), from, v.to_string()));
			params.led_state = Some(v.to_string());
		}
		if let Some(v) = edit.melody {
			let events = patched.events.as_mut().ok_or_else(|| {
				UsageError::new(
					"the camera did not answer the 486 events question, so --melody has no block to change",
				)
			})?;
			let mut touched = 0;
			for dev in events.iter_mut() {
				for entry in dev.alarmin_cfg.iter_mut() {
					// Entries the camera didn't print a melody for are left
					// alone: adding `<musicId>` would be inventing a tag.
					let Some(from) = entry.music_id.clone() else {
						continue;
					};
					let name = entry
						.type_
						.clone()
						.unwrap_or_else(|| "(no type)".into());
					asked.push((
						Asked::Melody(name.clone()),
						format!("musicId({name})"),
						from,
						v.to_string(),
					));
					entry.music_id = Some(v.to_string());
					touched += 1;
				}
			}
			if touched == 0 {
				return Err(UsageError::new(
					"no <alarminCfg> entry printed a <musicId>, so --melody cannot change one; \
					 refusing to add a tag the firmware did not send",
				)
				.into());
			}
		}
		if let Some(v) = edit.silent {
			let silent = patched.silent.as_mut().ok_or_else(|| {
				UsageError::new(
					"the camera did not answer the 609 silent-mode question, so --silent \
					 has no block to change",
				)
			})?;
			let from = present(silent.time.as_deref(), "time", "--silent")?;
			asked.push((Asked::SilentTime, "silentTime".into(), from, v.to_string()));
			silent.time = Some(v.to_string());
		}
	}

	// `set_ding_dong_cfg` diffs against a fresh read, sends only the changed
	// parts, and returns what the camera holds afterwards (the re-read after
	// any write; with nothing to write it sends nothing and just reads).
	let after = if asked.is_empty() {
		if edit.is_empty() {
			before
		} else {
			// Unreachable: every non-empty edit pushes an `asked` entry (or
			// returns a usage error). Keep the flow explicit anyway.
			patched
		}
	} else {
		cam.set_ding_dong_cfg(&patched)
			.await
			.context("carillon write (cmds 485/483/610/487): the camera refused or never answered")?
	};

	let changes = asked
		.into_iter()
		.map(|(which, tag, from, requested)| DingDongChange {
			tag,
			before: from,
			requested,
			after: after_value(&after, &which).unwrap_or_default(),
		})
		.collect::<Vec<_>>();

	// The live test last: if a write happened, the ring reflects it.
	let ring = if edit.ring {
		let chime_id = after.chime_id.as_deref().ok_or_else(|| {
			UsageError::new(
				"the 484 list printed no addressable chime id, so --ring has no chime to ring",
			)
		})?;
		let music_id = edit.melody.unwrap_or(0).to_string();
		cam.ring_ding_dong(chime_id, &music_id).await.map_err(|e| {
			let e = anyhow::Error::from(e);
			if changes.is_empty() {
				e.context("carillon ring (cmd 485 ringWithMusic): the camera refused or never answered")
			} else {
				// The settings are already stored; don't let the noisy test
				// failure imply they weren't.
				e.context(
					"the chime settings were written; the ring test (cmd 485 ringWithMusic) \
					 failed — re-run chime-settings to read them back",
				)
			}
		})?;
		Some(ChimeRingInfo {
			chime_id: chime_id.to_string(),
			music_id,
		})
	} else {
		None
	};

	Ok(Outcome::ChimeSettings {
		changed: !changes.is_empty(),
		block: info(&after),
		changes,
		ring,
	})
}

/// Trait-object route: see the module docs — this command cannot run through
/// [`CameraDriver`]. Mirrors [`crate::oneshot::audio_settings::run_via_driver`].
pub async fn run_via_driver(_cam: &dyn CameraDriver) -> Result<Outcome> {
	anyhow::bail!(
		"chime-settings requires a concrete BcCamera; not supported through CameraDriver"
	);
}

/// The camera printed this tag, so we may change it; it didn't, so asking to
/// change it is a usage error rather than a licence to invent it.
fn present(current: Option<&str>, tag: &str, flag: &str) -> Result<String> {
	current.map(str::to_string).ok_or_else(|| {
		anyhow::Error::from(UsageError::new(format!(
			"this camera's chime block has no <{tag}> tag, so {flag} cannot set it; \
			 refusing to add a tag the firmware did not send"
		)))
	})
}

/// The text the camera holds for one of the writable values after the re-read.
fn after_value(cfg: &DingDongCfg, which: &Asked) -> Option<String> {
	match which {
		Asked::VolLevel => cfg.params.as_ref()?.vol_level.clone(),
		Asked::LedState => cfg.params.as_ref()?.led_state.clone(),
		Asked::SilentTime => cfg.silent.as_ref()?.time.clone(),
		Asked::Melody(name) => cfg
			.events
			.as_ref()?
			.iter()
			.flat_map(|dev| dev.alarmin_cfg.iter())
			.find(|e| e.type_.as_deref().unwrap_or("(no type)") == name)
			.and_then(|e| e.music_id.clone()),
	}
}

/// Flatten the aggregate for the report. Refused parts stay absent (they are
/// findings about the camera), answered-empty parts stay empty lists.
fn info(cfg: &DingDongCfg) -> DingDongCfgInfo {
	DingDongCfgInfo {
		chime_id: cfg.chime_id.clone(),
		chimes: cfg
			.chimes
			.as_ref()
			.map(|list| {
				list.iter()
					.map(|d| ChimeDeviceInfo {
						id: d.identifier().unwrap_or_default().to_string(),
						name: d.display_name().map(str::to_string),
						net_state: d.net_state().map(str::to_string),
					})
					.collect()
			})
			.unwrap_or_default(),
		ctrl: cfg.ctrl.as_ref().map(|c| ChimeCtrlInfo {
			version: c.version.clone(),
			channel_id: c.channel_id.clone(),
			cfg_type: c.type_.clone(),
			bopen: c.bopen.clone(),
			enable: c.enable.clone(),
			bsave: c.bsave.clone(),
			time: c.time.clone(),
			result: c.result.clone(),
		}),
		params: cfg.params.as_ref().map(|p| ChimeParamsInfo {
			version: p.version.clone(),
			id: p.id.clone(),
			name: p.name.clone(),
			vol_level: p.vol_level.clone(),
			led_state: p.led_state.clone(),
			music_id: p.music_id.clone(),
		}),
		silent: cfg.silent.as_ref().map(|s| ChimeSilentInfo {
			version: s.version.clone(),
			id: s.id.clone(),
			time: s.time.clone(),
			cfg_type: s.type_.clone(),
		}),
		events: cfg
			.events
			.as_ref()
			.map(|devs| devs.iter().map(info_device).collect())
			.unwrap_or_default(),
	}
}

fn info_device(dev: &DingDongDeviceCfg) -> ChimeEventInfo {
	ChimeEventInfo {
		id: dev.id.clone(),
		ring_id: dev.ring_id.clone(),
		name: dev.name.clone(),
		firm_version: dev.firm_version.clone(),
		entries: dev
			.alarmin_cfg
			.iter()
			.map(|e| ChimeEventEntryInfo {
				event_type: e.type_.clone(),
				valid: e.valid.clone(),
				switch: e.switch_.clone(),
				music_id: e.music_id.clone(),
			})
			.collect(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::{
		DingDongAlarmCfg, DingDongCtrl, DingDongDevice, DingDongDeviceOpt, DingDongSilent,
	};

	fn fixture() -> DingDongCfg {
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
				id: Some("1".into()),
				name: Some("Carillon".into()),
				vol_level: Some("2".into()),
				led_state: Some("1".into()),
				..Default::default()
			}),
			silent: Some(DingDongSilent {
				version: "1.1".into(),
				id: Some("1".into()),
				time: Some("0".into()),
				..Default::default()
			}),
			events: Some(vec![DingDongDeviceCfg {
				version: "1.1".into(),
				id: Some("1".into()),
				ring_id: Some("7".into()),
				name: Some("Reolink Chime".into()),
				firm_version: Some("00A2".into()),
				alarmin_cfg: vec![
					DingDongAlarmCfg {
						version: "1.1".into(),
						type_: Some("people".into()),
						valid: Some("1".into()),
						music_id: Some("0".into()),
						..Default::default()
					},
					DingDongAlarmCfg {
						version: "1.1".into(),
						type_: Some("visitor".into()),
						switch_: Some("0".into()),
						music_id: Some("2".into()),
						..Default::default()
					},
				],
			}]),
		}
	}

	#[test]
	fn chime_settings_edit_is_empty_only_without_write_options() {
		assert!(Edit::default().is_empty());
		// `--sonner` alone makes noise but stores nothing: still "no write".
		assert!(
			Edit {
				ring: true,
				..Default::default()
			}
			.is_empty(),
			"--ring on its own must not turn the read into a write"
		);
		// `0` is a value, not an absence: silencing for 0 (unmute) is a write.
		assert!(!Edit {
			silent: Some(0),
			..Default::default()
		}
		.is_empty());
		assert!(!Edit {
			volume: Some(0),
			..Default::default()
		}
		.is_empty());
	}

	#[test]
	fn chime_settings_check_refuses_out_of_range_switch_values() {
		let err = Edit {
			volume: Some(5),
			..Default::default()
		}
		.check()
		.expect_err("--volume 5 is outside the documented range");
		assert!(format!("{:#}", err).contains("--volume"));
		let err = Edit {
			led: Some(2),
			..Default::default()
		}
		.check()
		.expect_err("--led 2 is not a switch value");
		assert!(format!("{:#}", err).contains("--led"));
		// The documented edges are fine, and the unbounded options pass
		// through (their ranges are undocumented; refusing a number here would
		// be a guess dressed as a check).
		Edit {
			volume: Some(0),
			led: Some(1),
			melody: Some(9999),
			silent: Some(u32::MAX),
			ring: true,
		}
		.check()
		.expect("documented edges and unbounded options pass");
	}

	#[test]
	fn chime_settings_missing_tag_is_a_usage_error_not_an_addition() {
		let err = present(None, "volLevel", "--volume")
			.expect_err("a tag the camera didn't print may not be invented");
		let msg = format!("{:#}", err);
		assert!(msg.contains("volLevel"), "{msg}");
		assert!(msg.contains("--volume"), "{msg}");
		assert_eq!(present(Some(""), "time", "--silent").unwrap(), "");
	}

	#[test]
	fn chime_settings_after_value_finds_each_written_tag() {
		let cfg = fixture();
		assert_eq!(after_value(&cfg, &Asked::VolLevel).as_deref(), Some("2"));
		assert_eq!(after_value(&cfg, &Asked::LedState).as_deref(), Some("1"));
		assert_eq!(after_value(&cfg, &Asked::SilentTime).as_deref(), Some("0"));
		assert_eq!(
			after_value(&cfg, &Asked::Melody("visitor".into())).as_deref(),
			Some("2"),
			"the per-event melody is looked up by its camera-printed type"
		);
		assert_eq!(after_value(&cfg, &Asked::Melody("ghost".into())), None);
	}

	#[test]
	fn chime_settings_info_keeps_text_and_drops_refused_parts() {
		let mut cfg = fixture();
		cfg.ctrl = Some(DingDongCtrl {
			version: "1.1".into(),
			enable: Some("1".into()),
			time: Some("30".into()),
			..Default::default()
		});
		let flat = info(&cfg);
		assert_eq!(flat.chime_id.as_deref(), Some("1"));
		assert_eq!(flat.chimes[0].net_state.as_deref(), Some("1"));
		// The `enable` spelling survives into the report as `enable`, not a
		// normalized boolean.
		assert_eq!(flat.ctrl.as_ref().unwrap().enable.as_deref(), Some("1"));
		assert_eq!(flat.events[0].entries[1].switch.as_deref(), Some("0"));
		let json = serde_json::to_value(&flat).unwrap();
		assert_eq!(json["params"]["vol_level"], "2");
		// Refused 485 would be absent; here every part answered, so none is:
		assert!(json.get("ctrl").is_some() && json.get("silent").is_some());

		// And the wireless-refused case: refused parts vanish from JSON
		// entirely, they are not serialized as null.
		let mut thin = fixture();
		thin.chimes = None;
		thin.chime_id = None;
		thin.params = None;
		thin.silent = None;
		thin.events = None;
		let json = serde_json::to_value(info(&thin)).unwrap();
		for key in ["chime_id", "chimes", "params", "silent", "events"] {
			assert!(json.get(key).is_none(), "{key} must be absent: {json}");
		}
	}

	#[tokio::test]
	async fn chime_settings_run_via_driver_bails_with_named_reason() {
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build();
		let err = run_via_driver(&*fake)
			.await
			.expect_err("trait route must not succeed");
		assert!(format!("{:#}", err).contains("requires a concrete BcCamera"));
	}
}
