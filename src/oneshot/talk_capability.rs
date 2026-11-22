//! `talk-ability` one-shot — ask the camera for its `<TalkAbility>`
//! (msg id 10) **without** the `two_way_audio` ability gate, and print the
//! raw answer.
//!
//! # Why ungated
//!
//! The normal entry point, `BcCamera::talk_ability()`, refuses to send
//! anything unless the login's `AbilityInfo` advertised `two_way_audio`.
//! That is the right default for a caller that intends to talk — it saves a
//! round trip on a camera that cannot — but it makes the question
//! "does this box actually have a talk path?" unanswerable on firmware that
//! under-reports its abilities. This family has already been caught
//! answering `405` to `<HaCfg>` while happily accepting `<Email>`, both
//! absent from its advertised list. So this diagnostic calls
//! [`BcCamera::talk_ability_probe`] (a copy of the request with the gate
//! removed) and reports exactly what came back: XML, empty ack, or a bare
//! error code. The gate itself is untouched for every other caller.
//!
//! Read-only by construction: msg id 10 is the capability read. Nothing
//! here sends `TalkConfig` (201), audio (202) or a talk reset (11).
//!
//! # Why it wants the concrete `BcCamera`
//!
//! `talk_ability_probe` lives on `BcCamera`, not on the `CameraDriver`
//! trait — deliberately, since the trait is the seam every service-side
//! caller goes through and this is a CLI diagnostic, not a feature. Same
//! shape as `snapshot --use-stream-raw`: [`run`] takes `&BcCamera` and is
//! reached from `run_oneshot`'s concrete-handle branch, while
//! [`run_via_driver`] answers the trait-object route with a clear error.

use anyhow::{Context, Result};
use bairelay_neolink_core::bc::xml::TalkAbility;
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver, TalkAbilityProbe};

use super::output::{Outcome, TalkAudioConfig};

/// Probe the camera's `<TalkAbility>` and report the reply verbatim.
///
/// The three shapes the camera can produce all come back as a successful
/// [`Outcome`] — a `405` is a *result*, not a failure of this command, which
/// is what makes the exit code useful in a script. Only a camera that never
/// answered (transport, timeout, cancellation) is an `Err`, so "refused" and
/// "silent" can never be confused.
pub async fn run(cam: &BcCamera) -> Result<Outcome> {
	let probe = cam
		.talk_ability_probe()
		.await
		.context("TalkAbility probe: no answer from the camera (transport/timeout)")?;
	Ok(match probe {
		TalkAbilityProbe::Xml { code, ability } => {
			// Each wrapper (`<audioConfigList>` …) can carry several entries —
			// the measured camera sends two `<audioStreamMode>` in one list —
			// so every list is flattened across wrappers.
			let configs = ability
				.audio_config_list
				.iter()
				.flat_map(|list| list.audio_config.iter())
				.map(|c| TalkAudioConfig {
					audio_type: c.audio_type.clone(),
					sample_rate: c.sample_rate,
					sample_precision: c.sample_precision,
					length_per_encoder: c.length_per_encoder,
					sound_track: c.sound_track.clone(),
					priority: c.priority,
				})
				.collect();
			let duplex = ability
				.duplex_list
				.iter()
				.flat_map(|list| list.duplex.iter())
				.cloned()
				.collect();
			let audio_stream_modes = ability
				.audio_stream_mode_list
				.iter()
				.flat_map(|list| list.audio_stream_mode.iter())
				.cloned()
				.collect();
			Outcome::TalkAbility {
				found: true,
				code,
				payload: "xml".to_string(),
				xml: Some(serialise_xml(&ability)?),
				configs,
				duplex,
				audio_stream_modes,
			}
		}
		TalkAbilityProbe::ErrorCode { code, payload } => Outcome::TalkAbility {
			found: false,
			code,
			payload,
			xml: None,
			configs: Vec::new(),
			duplex: Vec::new(),
			audio_stream_modes: Vec::new(),
		},
		TalkAbilityProbe::NoXml { code, payload } => Outcome::TalkAbility {
			found: false,
			code,
			payload,
			xml: None,
			configs: Vec::new(),
			duplex: Vec::new(),
			audio_stream_modes: Vec::new(),
		},
	})
}

/// Trait-object route: this command cannot run through
/// [`CameraDriver`], which has no talk surface at all (that absence is the
/// finding this probe exists to investigate). Mirrors
/// [`crate::oneshot::snapshot::run_via_driver`].
pub async fn run_via_driver(_cam: &dyn CameraDriver) -> Result<Outcome> {
	anyhow::bail!("talk-ability requires a concrete BcCamera; not supported through CameraDriver");
}

/// Round-trip the parsed `<TalkAbility>` through `quick_xml::se` so the
/// operator sees the element the camera sent, at the cost of our field
/// order rather than the camera's. Same trade as the `abilities` command
/// (see [`crate::oneshot::abilities`]); the raw wire bytes are not retained
/// by the protocol layer, so this is the closest form available.
fn serialise_xml(ability: &TalkAbility) -> Result<String> {
	let mut buf = bytes::BytesMut::new();
	quick_xml::se::to_writer(&mut buf, ability).context("re-serialise TalkAbility as XML")?;
	let s = std::str::from_utf8(&buf)
		.context("TalkAbility XML was not valid utf-8 — protocol corruption?")?;
	Ok(s.to_owned())
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The trait-object route must fail with a named reason rather than
	/// silently doing nothing. Needs no camera: the function never touches
	/// its argument.
	#[tokio::test]
	async fn run_via_driver_bails_with_named_reason() {
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build();
		let err = run_via_driver(&*fake)
			.await
			.expect_err("trait route must not succeed");
		assert!(format!("{:#}", err).contains("requires a concrete BcCamera"));
	}
}
