//! `talk` one-shot — send one PCM file out of the doorbell's loudspeaker.
//!
//! # Shape
//!
//! The file is validated by [`super::pcm`] (mono / 16-bit signed / 16000 Hz,
//! WAV container or `--raw` headerless), the camera's own `<TalkAbility>`
//! answer is *probed* rather than assumed, the samples are encoded into that
//! answer's codec, and [`BcCamera::talk`] carries them. Nothing here is
//! hardcoded from a datasheet: codec, sample rate and block size all come from
//! msg id 10, so a camera that answers `8000 Hz / lengthPerEncoder 400` drives
//! a 204-byte block encoder instead of this one's 516.
//!
//! # Why the probe is ungated
//!
//! `talk()` is behind `has_ability_rw("two_way_audio")`, which the battery
//! doorbell does not advertise even though it answers `<TalkAbility>` with a
//! full FDX/adpcm configuration. Re-running the ungated probe here (msg id 10,
//! read-only, one round trip) is how the command builds a `TalkConfig` it can
//! justify instead of inventing one. The gate itself is lifted only by the
//! per-camera `two_way_audio = true` config entry (applied in
//! [`crate::oneshot::runner`]), never from here — if the operator has not set
//! it, `talk()` refuses and this command says what to do about that.
//!
//! # Pacing
//!
//! At `lengthPerEncoder = 1024` and 16000 Hz one block is 64 ms of audio, so a
//! burst of blocks is not a file upload: [`BcCamera::talk`] sleeps for the
//! playback duration of every payload it writes, which means a 2 s clip takes
//! ~2 s of wall clock and the camera never receives more audio than it can
//! play. The outcome reports both numbers so the pacing is visible, and says
//! plainly that nothing on this side can hear the loudspeaker.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bairelay_neolink_core::bc::xml::{AudioConfig, TalkConfig};
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver, TalkAbilityProbe};
use bairelay_rtsp::transcode::adpcm::{AdpcmEncoder, HeaderConvention};

use super::output::Outcome;
use super::pcm::{PcmInput, PcmOrigin, TALK_SAMPLE_RATE};

/// Block header the wire format adds to every block ("Block size + predictor
/// state"), mirroring `talk()`'s `full_block_size = block_size + 4`.
const BLOCK_HEADER_BYTES: usize = 4;

/// Smallest `lengthPerEncoder` worth driving: below this a block is mostly
/// header and the camera's own numbers cannot be right.
const MIN_LENGTH_PER_ENCODER: u16 = 8;

/// Slack added on top of the audio duration when sizing the op deadline.
const OP_SLACK: Duration = Duration::from_secs(120);

/// Floor for the op deadline, so a 0-byte or tiny file still leaves room for
/// connect + login + probe + the tail `talk_stop`.
const OP_FLOOR: Duration = Duration::from_secs(60);

/// Op deadline for one file, derived from its audio length rather than guessed.
///
/// The send is paced at wall-clock speed, so the generic `OP_TIMEOUT` of 30 s
/// would cut a long clip off mid-transmission. Sizing from the sample count
/// keeps a real budget (audio duration plus a third again for connect, login,
/// the probe and the tail `talk_stop`) without opening an unbounded one: a
/// camera that stops answering mid-clip still fails in about that time instead
/// of hanging until Ctrl+C.
///
/// Takes the *decoded sample count*, not a file size: the caller has already
/// read the file to validate it, and a WAV's header or a `LIST` chunk is not
/// audio time.
pub fn op_timeout_for_samples(samples: usize) -> Duration {
	let audio = Duration::from_secs_f64(samples as f64 / f64::from(TALK_SAMPLE_RATE));
	OP_FLOOR.max(audio + OP_SLACK / 3)
}

/// Everything a talk sender needs, read off the camera's own `<TalkAbility>`
/// answer. Shared with [`super::audio_bridge`] so both commands negotiate the
/// same way and a difference between them can never be a difference in how
/// they *asked*.
#[derive(Debug, Clone)]
pub struct Negotiation {
	/// Response code of the `<TalkAbility>` reply the config was built from.
	pub ability_code: u16,
	/// The `TalkConfig` to hand to `talk` / `talk_stream`, verbatim echo of
	/// the camera's entry (`priority` included).
	pub talk_config: TalkConfig,
	/// Bytes per wire block: `length_per_encoder / 2 + 4`.
	pub block_bytes: usize,
	/// Samples per wire block (= `length_per_encoder`).
	pub block_samples: usize,
	/// Milliseconds of audio one block carries.
	pub block_millis: u32,
	pub audio_type: String,
	pub sample_rate: u16,
	pub sample_precision: u16,
	pub sound_track: String,
	pub length_per_encoder: u16,
	pub duplex: String,
	/// What the 4 header bytes of each block are spent on. Set by the caller
	/// from `--header`; the default carries this block's first sample, which is
	/// what an IMA decoder emits as the block's first output sample (see
	/// [`HeaderConvention`] — the alternative replays one sample per block).
	pub convention: HeaderConvention,
}

impl Negotiation {
	/// A fresh encoder for this geometry. Call [`AdpcmEncoder::reset`] between
	/// talk sessions, not a new encoder, so the allocation is reused.
	pub fn encoder(&self) -> AdpcmEncoder {
		AdpcmEncoder::with_convention(
			usize::from(self.length_per_encoder) / 2,
			self.convention,
		)
	}
}

/// Wire geometry for a reported `lengthPerEncoder`, refusing values that
/// cannot describe an ADPCM block.
pub fn block_geometry(length_per_encoder: u16) -> Result<usize> {
	if length_per_encoder < MIN_LENGTH_PER_ENCODER || length_per_encoder % 2 != 0 {
		return Err(anyhow!(
			"camera reported lengthPerEncoder {}, which is not a usable ADPCM block size",
			length_per_encoder
		));
	}
	Ok(usize::from(length_per_encoder / 2) + BLOCK_HEADER_BYTES)
}

/// Ask the camera what its talk path speaks (msg id 10, ungated) and turn the
/// answer into a [`Negotiation`].
///
/// Refuses anything this binary cannot feed — no `adpcm` entry, a sample rate
/// other than 16 kHz, a non-mono track, an impossible block size — rather than
/// sending audio the camera would play back as noise or reject mid-session.
pub async fn negotiate(cam: &BcCamera, channel_id: u8) -> Result<Negotiation> {
	let probe = cam
		.talk_ability_probe()
		.await
		.context("TalkAbility probe: no answer from the camera (transport/timeout)")?;
	let (ability_code, ability) = match probe {
		TalkAbilityProbe::Xml { code, ability } => (code, ability),
		TalkAbilityProbe::ErrorCode { code, payload } => {
			return Err(anyhow!(
				"camera refused TalkAbility with code {code} (payload: {payload}) — no audio was sent"
			))
		}
		TalkAbilityProbe::NoXml { code, payload } => {
			return Err(anyhow!(
				"camera answered TalkAbility with code {code} but no <TalkAbility> element \
				 (payload: {payload}) — no audio was sent"
			))
		}
	};

	// Each `<audioConfigList>` wrapper can carry several entries, so flatten
	// across them, then prefer an exact adpcm @ 16 kHz match.
	let configs: Vec<&AudioConfig> = ability
		.audio_config_list
		.iter()
		.flat_map(|list| list.audio_config.iter())
		.collect();
	let chosen = configs
		.iter()
		.copied()
		.find(|c| {
			c.audio_type.eq_ignore_ascii_case("adpcm")
				&& u32::from(c.sample_rate) == TALK_SAMPLE_RATE
		})
		.or_else(|| configs.iter().copied().find(|c| c.audio_type.eq_ignore_ascii_case("adpcm")))
		.ok_or_else(|| {
			anyhow!(
				"camera listed no adpcm <audioConfig> (found: {}) — no audio was sent",
				summarise_configs(&configs)
			)
		})?;

	if u32::from(chosen.sample_rate) != TALK_SAMPLE_RATE {
		return Err(anyhow!(
			"camera's adpcm <audioConfig> asks for {} Hz while the talk input of this command \
			 is fixed at {TALK_SAMPLE_RATE} Hz mono 16-bit PCM; nothing here resamples, so no \
			 audio was sent",
			chosen.sample_rate
		));
	}
	if !chosen.sound_track.is_empty() && !chosen.sound_track.eq_ignore_ascii_case("mono") {
		return Err(anyhow!(
			"camera's adpcm <audioConfig> asks for soundTrack \"{}\"; the input is mono — \
			 no audio was sent",
			chosen.sound_track
		));
	}
	let block_bytes = block_geometry(chosen.length_per_encoder)?;

	let duplex = ability
		.duplex_list
		.iter()
		.flat_map(|list| list.duplex.iter())
		.next()
		.cloned()
		.unwrap_or_default();
	let audio_stream_mode = ability
		.audio_stream_mode_list
		.iter()
		.flat_map(|list| list.audio_stream_mode.iter())
		.next()
		.cloned()
		.unwrap_or_default();

	Ok(Negotiation {
		ability_code,
		talk_config: TalkConfig {
			version: ability.version.clone(),
			channel_id,
			duplex: duplex.clone(),
			audio_stream_mode,
			// Echo the camera's own entry verbatim: this is a negotiation, and
			// the cheapest way to learn what the firmware checks is to answer
			// it with exactly what it asked for.
			audio_config: chosen.clone(),
		},
		block_bytes,
		// The wire block carries `lengthPerEncoder` nibble slots *plus* the
		// predictor slot in front of them, and an IMA decoder emits that slot as
		// the block's first sample: the camera plays 1025 samples per 516-byte
		// block, which is exactly what the core's own `(len - 4) * 2 + 1`
		// arithmetic in `talk()` counts.
		block_samples: usize::from(chosen.length_per_encoder) + 1,
		block_millis: (f64::from(chosen.length_per_encoder) * 1000.0
			/ f64::from(TALK_SAMPLE_RATE)) as u32,
		audio_type: chosen.audio_type.clone(),
		sample_rate: chosen.sample_rate,
		sample_precision: chosen.sample_precision,
		sound_track: chosen.sound_track.clone(),
		length_per_encoder: chosen.length_per_encoder,
		duplex,
		convention: HeaderConvention::default(),
	})
}

/// Encode `samples` for a negotiated camera.
///
/// Returns the wire bytes (whole blocks only — the encoder pads a partial
/// tail), the block geometry, and the block count. Exposed for tests and reused
/// by [`super::audio_bridge`].
pub fn encode_for_camera(neg: &Negotiation, samples: &[i16]) -> Result<(Vec<u8>, usize, usize)> {
	let block_bytes = block_geometry(neg.length_per_encoder)?;
	let mut encoder = neg.encoder();
	let mut wire: Vec<u8> = Vec::with_capacity(samples.len() / 2 + 1024);
	encoder.encode(samples, &mut wire);
	encoder.finish(&mut wire);
	let blocks = wire.len() / block_bytes;
	debug_assert_eq!(
		wire.len() % block_bytes,
		0,
		"the encoder must only ever emit whole blocks"
	);
	Ok((wire, block_bytes, blocks))
}

/// Send `path` through the camera's loudspeaker.
///
/// The input is passed in already read and validated by the caller
/// (`run_support`), because reading it is also the *pre-flight* check: a file
/// this command will refuse must never cost a battery camera a wake-up.
pub async fn run(
	cam: &BcCamera,
	channel_id: u8,
	path: &Path,
	input: &PcmInput,
	convention: HeaderConvention,
) -> Result<Outcome> {
	if input.is_empty() {
		anyhow::bail!(
			"{} contains no audio samples to send ({})",
			path.display(),
			match input.origin {
				PcmOrigin::Raw => "empty headerless input",
				PcmOrigin::Wav => "empty data chunk",
			}
		);
	}

	// One msg-id-10 round trip, and every number below is read off it.
	let mut neg = negotiate(cam, channel_id).await?;
	// The only thing the operator can override about the wire format; the rest
	// of the negotiation is the camera's own answer, verbatim.
	neg.convention = convention;
	let (wire, block_bytes, blocks) = encode_for_camera(&neg, &input.samples)?;

	let started = Instant::now();
	cam.talk(&wire, neg.talk_config.clone()).await.context(
		"talk session failed. When the reason is a missing ability, this camera \
		 does not advertise two_way_audio: add `two_way_audio = true` to its \
		 [[cameras]] entry (see README) — a deliberate per-camera override this \
		 command is not allowed to decide for itself",
	)?;
	let wall = started.elapsed().as_secs_f64();

	Ok(Outcome::Talk {
		file: path.display().to_string(),
		origin: input.origin.label().to_string(),
		samples: input.len(),
		audio_seconds: input.duration_secs(),
		blocks,
		block_bytes,
		length_per_encoder: neg.length_per_encoder,
		audio_type: neg.audio_type.clone(),
		sample_rate: neg.sample_rate,
		sample_precision: neg.sample_precision,
		sound_track: neg.sound_track.clone(),
		duplex: neg.duplex.clone(),
		ability_code: neg.ability_code,
		wall_seconds: wall,
		dropped_bytes: input.dropped_bytes,
	})
}

/// `adpcm/16000Hz/16b/1024/mono` per entry, for refusal messages that have to
/// say what the camera actually offered.
fn summarise_configs(configs: &[&AudioConfig]) -> String {
	if configs.is_empty() {
		return "(no audioConfig at all)".to_string();
	}
	configs
		.iter()
		.map(|c| {
			format!(
				"{}/{}Hz/{}b/{}/{}",
				if c.audio_type.is_empty() {
					"(empty codec)"
				} else {
					&c.audio_type
				},
				c.sample_rate,
				c.sample_precision,
				c.length_per_encoder,
				if c.sound_track.is_empty() {
					"(empty track)"
				} else {
					&c.sound_track
				},
			)
		})
		.collect::<Vec<_>>()
		.join(", ")
}

/// Trait-object route, mirroring [`crate::oneshot::talk_abilite`]: the
/// [`CameraDriver`] trait has no talk surface at all, so this command needs the
/// concrete handle.
pub async fn run_via_driver(_cam: &dyn CameraDriver) -> Result<Outcome> {
	anyhow::bail!("talk requires a concrete BcCamera; not supported through CameraDriver");
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn run_via_driver_bails_with_named_reason() {
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build();
		let err = run_via_driver(&*fake)
			.await
			.expect_err("trait route must not succeed");
		assert!(format!("{:#}", err).contains("requires a concrete BcCamera"));
	}

	#[test]
	fn timeout_scales_with_the_audio_and_keeps_a_floor() {
		// 2 s of audio: 2 + 40 s of slack, held up to the 60 s floor so a tiny
		// clip still leaves room for connect + login + probe + talk_stop.
		assert_eq!(
			op_timeout_for_samples(2 * 16_000),
			Duration::from_secs(60),
			"a short clip must still get the floor"
		);
		// 50 s of audio: 50 + 40 s.
		assert_eq!(
			op_timeout_for_samples(50 * 16_000),
			Duration::from_secs(90),
			"a 50 s clip must not be cut off mid-transmission"
		);
		assert_eq!(op_timeout_for_samples(0), Duration::from_secs(60));
		let an_hour = op_timeout_for_samples(3600 * 16_000);
		assert!(
			an_hour > Duration::from_secs(3600) && an_hour < Duration::from_secs(3600 * 2),
			"the budget follows the audio and stays bounded, got {an_hour:?}"
		);
	}

	/// A negotiation with the geometry this fork's doorbell reported, at
	/// whatever `lengthPerEncoder` the test wants.
	fn neg(length_per_encoder: u16) -> Negotiation {
		Negotiation {
			ability_code: 200,
			talk_config: TalkConfig::default(),
			block_bytes: usize::from(length_per_encoder / 2) + BLOCK_HEADER_BYTES,
			block_samples: usize::from(length_per_encoder) + 1,
			block_millis: 64,
			audio_type: "adpcm".into(),
			sample_rate: 16_000,
			sample_precision: 16,
			sound_track: "mono".into(),
			length_per_encoder,
			duplex: "FDX".into(),
			convention: HeaderConvention::default(),
		}
	}

	#[test]
	fn encoder_geometry_follows_the_cameras_number() {
		let (wire, block_bytes, blocks) = encode_for_camera(&neg(1024), &[1000; 2500]).unwrap();
		assert_eq!(block_bytes, 516);
		assert_eq!(blocks, 3, "two full blocks plus one padded tail");
		assert_eq!(wire.len(), 3 * 516);

		// A different camera number must move the geometry with it.
		let (wire, block_bytes, blocks) = encode_for_camera(&neg(400), &[1000; 2500]).unwrap();
		assert_eq!(block_bytes, 204);
		assert_eq!(blocks, 7, "2500 samples / 400 per block");
		assert_eq!(wire.len(), 7 * 204);
	}

	#[test]
	fn absurd_block_size_is_refused() {
		let err = block_geometry(2).expect_err("must refuse");
		assert!(format!("{err}").contains("lengthPerEncoder 2"));
		assert!(block_geometry(1023).is_err(), "an odd size cannot pack nibbles");
		assert_eq!(block_geometry(1024).unwrap(), 516);
	}

	#[test]
	fn summarise_names_every_entry() {
		let cfg = AudioConfig {
			priority: Some(0),
			audio_type: "".into(),
			sample_rate: 8000,
			sample_precision: 8,
			length_per_encoder: 320,
			sound_track: "".into(),
		};
		assert_eq!(
			summarise_configs(&[&cfg]),
			"(empty codec)/8000Hz/8b/320/(empty track)"
		);
		assert_eq!(summarise_configs(&[]), "(no audioConfig at all)");
	}
}
