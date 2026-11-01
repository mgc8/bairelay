//! Two-way audio (`talk`): play a WAV file through the camera speaker.
//!
//! Shared by the `talk` one-shot subcommand and the MQTT
//! `control/talk` topic. The flow ports the neolink talk client: query
//! `TalkAbility`, resolve the `TalkConfig` with a three-tier fallback,
//! encode the audio to DVI4 IMA ADPCM, and send it with one `talk`
//! call. GStreamer is replaced by the pure-Rust [`wav`] reader and
//! [`adpcm`] encoder.

pub mod adpcm;
pub mod wav;

use std::time::Duration;

use anyhow::{bail, Context, Result};
use bairelay_neolink_core::bc::xml::{AudioConfig, TalkAbility, TalkConfig};
use bairelay_neolink_core::bc_protocol::CameraDriver;

use crate::config::{CameraConfig, TalkConfigOverride};
use wav::MonoPcm;

/// Longest clip `talk` accepts. Playback holds the camera awake for
/// the whole clip, so a mistaken multi-hour file must not drain a
/// battery camera.
pub const MAX_CLIP: Duration = Duration::from_secs(600);

/// Margin above the clip length for the connect-free part of a talk
/// operation (TalkAbility query, TalkConfig handshake, talk-stop).
pub const TALK_OP_MARGIN: Duration = Duration::from_secs(30);

/// Which tier of [`resolve_talk_config`] produced the `TalkConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TalkConfigSource {
	/// `[cameras.talk_config]` in the config file.
	ConfigOverride,
	/// The camera's `TalkAbility` reply.
	CameraAbility,
	/// The standard Reolink ADPCM parameters.
	Defaults,
}

impl TalkConfigSource {
	pub fn as_str(self) -> &'static str {
		match self {
			TalkConfigSource::ConfigOverride => "config",
			TalkConfigSource::CameraAbility => "camera",
			TalkConfigSource::Defaults => "defaults",
		}
	}
}

/// Resolve the `TalkConfig` to send, in priority order:
///
/// 1. the `[cameras.talk_config]` override, when present;
/// 2. the first entry of each list in the camera's `TalkAbility`, when
///    all three lists are non-empty;
/// 3. the standard Reolink ADPCM defaults (FDX, followVideoStream,
///    adpcm, 16000 Hz, 16 bit, 1024, mono).
///
/// A failed `TalkAbility` query is passed as `TalkAbility::default()`
/// (empty lists). `channel_id` is left at 0; the caller sets it.
pub fn resolve_talk_config(
	ability: &TalkAbility,
	override_cfg: Option<&TalkConfigOverride>,
) -> TalkConfig {
	resolve_with_source(ability, override_cfg).0
}

/// [`resolve_talk_config`] plus the tier that produced the result.
pub fn resolve_with_source(
	ability: &TalkAbility,
	override_cfg: Option<&TalkConfigOverride>,
) -> (TalkConfig, TalkConfigSource) {
	if let Some(o) = override_cfg {
		return (from_override(o), TalkConfigSource::ConfigOverride);
	}
	if let (Some(duplex), Some(mode), Some(audio)) = (
		ability.duplex_list.first(),
		ability.audio_stream_mode_list.first(),
		ability.audio_config_list.first(),
	) {
		let cfg = TalkConfig {
			duplex: duplex.duplex.clone(),
			audio_stream_mode: mode.audio_stream_mode.clone(),
			audio_config: audio.audio_config.clone(),
			..Default::default()
		};
		return (cfg, TalkConfigSource::CameraAbility);
	}
	(
		from_override(&TalkConfigOverride::default()),
		TalkConfigSource::Defaults,
	)
}

fn from_override(o: &TalkConfigOverride) -> TalkConfig {
	TalkConfig {
		duplex: o.duplex.clone(),
		audio_stream_mode: o.audio_stream_mode.clone(),
		audio_config: AudioConfig {
			priority: None,
			audio_type: o.audio_type.clone(),
			sample_rate: o.sample_rate,
			sample_precision: o.sample_precision,
			length_per_encoder: o.length_per_encoder,
			sound_track: o.sound_track.clone(),
		},
		..Default::default()
	}
}

/// Deadline for loading a WAV file (metadata, read, decode).
pub const LOAD_TIMEOUT: Duration = Duration::from_secs(10);

/// Deadline for the `TalkAbility` query. Battery doorbells never answer
/// it; the query then falls back to the defaults.
pub const ABILITY_TIMEOUT: Duration = Duration::from_secs(5);

/// A talk failure, with a short stable [`code`](TalkError::code) for
/// the MQTT `status/talk` topic. `Display` never includes the path.
#[derive(Debug, thiserror::Error)]
pub enum TalkError {
	#[error("WAV file not found or not accessible")]
	NotFound,
	#[error("not a regular file")]
	NotAFile,
	#[error("path is outside talk_dir")]
	OutsideTalkDir,
	#[error("cannot use WAV: {0:#}")]
	Decode(anyhow::Error),
	#[error("WAV load timed out after {} s", LOAD_TIMEOUT.as_secs())]
	LoadTimeout,
	#[error("talk already playing on this camera")]
	Busy,
	#[error("{0:#}")]
	Camera(anyhow::Error),
}

impl TalkError {
	/// Short code published as `error: <code>` on `status/talk`.
	pub fn code(&self) -> &'static str {
		match self {
			TalkError::NotFound => "not_found",
			TalkError::NotAFile => "not_a_file",
			TalkError::OutsideTalkDir => "outside_talk_dir",
			TalkError::Decode(_) => "decode",
			TalkError::LoadTimeout => "timeout",
			TalkError::Busy => "busy",
			TalkError::Camera(_) => "camera",
		}
	}
}

/// Check `--volume` before any camera contact.
pub fn check_volume(volume: f32) -> Result<()> {
	if !volume.is_finite() || volume < 0.0 {
		bail!("volume must be a finite number >= 0, got {volume}");
	}
	Ok(())
}

/// Load a WAV clip for `talk` and check its length, on a blocking
/// thread under [`LOAD_TIMEOUT`]. Runs before any camera connection so
/// a bad file never wakes the camera. With `talk_dir`, the path must
/// canonicalise to a location under it.
pub async fn load_clip(
	path: &std::path::Path,
	talk_dir: Option<&std::path::Path>,
) -> std::result::Result<MonoPcm, TalkError> {
	let path = path.to_path_buf();
	let talk_dir = talk_dir.map(|d| d.to_path_buf());
	let task = tokio::task::spawn_blocking(move || load_clip_blocking(&path, talk_dir.as_deref()));
	match tokio::time::timeout(LOAD_TIMEOUT, task).await {
		Ok(Ok(r)) => r,
		Ok(Err(join)) => Err(TalkError::Decode(anyhow::anyhow!(
			"load task failed: {join}"
		))),
		Err(_) => Err(TalkError::LoadTimeout),
	}
}

fn load_clip_blocking(
	path: &std::path::Path,
	talk_dir: Option<&std::path::Path>,
) -> std::result::Result<MonoPcm, TalkError> {
	let resolved;
	let path = match talk_dir {
		Some(dir) => {
			resolved = std::fs::canonicalize(path).map_err(|_| TalkError::NotFound)?;
			let dir = std::fs::canonicalize(dir).map_err(|_| TalkError::OutsideTalkDir)?;
			if !resolved.starts_with(&dir) {
				return Err(TalkError::OutsideTalkDir);
			}
			resolved.as_path()
		}
		None => path,
	};
	let clip = wav::read_wav_file(path)?;
	if clip.samples.is_empty() {
		return Err(TalkError::Decode(anyhow::anyhow!("no audio samples")));
	}
	let d = clip_duration(&clip);
	if d > MAX_CLIP {
		return Err(TalkError::Decode(anyhow::anyhow!(
			"clip is {:.0} s long; talk accepts at most {} s",
			d.as_secs_f64(),
			MAX_CLIP.as_secs()
		)));
	}
	Ok(clip)
}

/// Playback length of `clip`.
pub fn clip_duration(clip: &MonoPcm) -> Duration {
	Duration::from_secs_f64(clip.samples.len() as f64 / f64::from(clip.sample_rate))
}

/// Encode `clip` for `cfg`: resample to `cfg.audio_config.sample_rate`,
/// apply `volume`, encode DVI4 ADPCM blocks of
/// `length_per_encoder / 2 + 4` bytes.
pub fn encode_for(clip: &MonoPcm, cfg: &TalkConfig, volume: f32) -> Result<Vec<u8>> {
	let ac = &cfg.audio_config;
	if ac.audio_type != "adpcm" {
		bail!(
			"talk audio type {:?} is not supported; only adpcm",
			ac.audio_type
		);
	}
	if ac.sample_rate == 0 || ac.length_per_encoder < 2 {
		bail!(
			"invalid talk parameters (sample_rate={}, length_per_encoder={})",
			ac.sample_rate,
			ac.length_per_encoder
		);
	}
	check_volume(volume)?;
	if ac.sound_track != "mono" {
		tracing::warn!(
			sound_track = %ac.sound_track,
			"talk: encoder produces mono; camera asked for another sound track"
		);
	}
	let target = u32::from(ac.sample_rate);
	if clip.sample_rate != target {
		tracing::info!(
			from = clip.sample_rate,
			to = target,
			"talk: resampling input (linear)"
		);
	}
	let mut pcm = wav::resample_linear(&clip.samples, clip.sample_rate, target);
	wav::apply_volume(&mut pcm, volume);
	Ok(adpcm::encode_dvi4(
		&pcm,
		adpcm::block_align_for(ac.length_per_encoder),
	))
}

/// Result of one [`play`] call.
#[derive(Debug, Clone, PartialEq)]
pub struct TalkSummary {
	pub source: TalkConfigSource,
	pub sample_rate: u16,
	pub blocks: usize,
	pub duration: Duration,
}

/// Resolve the `TalkConfig`, encode `clip`, and play it with one `talk`
/// call on an already-connected camera.
///
/// A camera with a `[cameras.talk_config]` table skips the
/// `TalkAbility` query (battery doorbells never answer message 10) and
/// bypasses the core's `two_way_audio` ability gate: the operator
/// pinned the parameters. Without the table, the query runs under
/// [`ABILITY_TIMEOUT`] (falling back to the defaults) and the gate
/// stays.
pub async fn play(
	cam: &dyn CameraDriver,
	cam_cfg: &CameraConfig,
	clip: &MonoPcm,
	volume: f32,
) -> Result<TalkSummary> {
	let name = cam_cfg.name.as_str();
	let bypass_ability_gate = cam_cfg.talk_config.is_some();
	let ability = if bypass_ability_gate {
		TalkAbility::default()
	} else {
		match tokio::time::timeout(ABILITY_TIMEOUT, cam.talk_ability(false)).await {
			Ok(Ok(a)) => a,
			Ok(Err(e)) => {
				tracing::warn!(camera = %name, error = %e, "talk: TalkAbility query failed; using fallback config");
				TalkAbility::default()
			}
			Err(_) => {
				tracing::warn!(camera = %name, "talk: TalkAbility query timed out; using fallback config");
				TalkAbility::default()
			}
		}
	};
	let (mut cfg, source) = resolve_with_source(&ability, cam_cfg.talk_config.as_ref());
	cfg.channel_id = cam_cfg.channel_id;
	let ac = &cfg.audio_config;
	tracing::info!(
		camera = %name,
		source = source.as_str(),
		duplex = %cfg.duplex,
		mode = %cfg.audio_stream_mode,
		audio_type = %ac.audio_type,
		sample_rate = ac.sample_rate,
		length_per_encoder = ac.length_per_encoder,
		"talk: TalkConfig resolved"
	);
	let adpcm = encode_for(clip, &cfg, volume)?;
	let block_align = adpcm::block_align_for(ac.length_per_encoder);
	let summary = TalkSummary {
		source,
		sample_rate: ac.sample_rate,
		blocks: adpcm.len() / block_align,
		duration: clip_duration(clip),
	};
	// `context` keeps the core error downcastable, so the one-shot exit
	// code classifier still maps MissingAbility to its own code.
	cam.talk(&adpcm, cfg, bypass_ability_gate)
		.await
		.context("talk failed")?;
	Ok(summary)
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::{AudioConfigList, AudioStreamModeList, DuplexList};
	use bairelay_neolink_core::bc_protocol::{Error as BcError, FakeCameraBuilder};

	fn camera_ability() -> TalkAbility {
		TalkAbility {
			duplex_list: vec![DuplexList {
				duplex: "HDX".into(),
			}],
			audio_stream_mode_list: vec![AudioStreamModeList {
				audio_stream_mode: "mixAudioStream".into(),
			}],
			audio_config_list: vec![AudioConfigList {
				audio_config: AudioConfig {
					priority: Some(0),
					audio_type: "adpcm".into(),
					sample_rate: 8000,
					sample_precision: 16,
					length_per_encoder: 512,
					sound_track: "mono".into(),
				},
			}],
			..Default::default()
		}
	}

	fn defaults() -> TalkConfig {
		TalkConfig {
			duplex: "FDX".into(),
			audio_stream_mode: "followVideoStream".into(),
			audio_config: AudioConfig {
				priority: None,
				audio_type: "adpcm".into(),
				sample_rate: 16000,
				sample_precision: 16,
				length_per_encoder: 1024,
				sound_track: "mono".into(),
			},
			..Default::default()
		}
	}

	#[test]
	fn tier1_config_override_wins_over_camera_ability() {
		let o = TalkConfigOverride {
			duplex: "HDX".into(),
			sample_rate: 8000,
			length_per_encoder: 256,
			..Default::default()
		};
		let (cfg, src) = resolve_with_source(&camera_ability(), Some(&o));
		assert_eq!(src, TalkConfigSource::ConfigOverride);
		assert_eq!(cfg.duplex, "HDX");
		assert_eq!(cfg.audio_stream_mode, "followVideoStream");
		assert_eq!(cfg.audio_config.sample_rate, 8000);
		assert_eq!(cfg.audio_config.length_per_encoder, 256);
		assert_eq!(cfg.audio_config.priority, None);
		assert_eq!(resolve_talk_config(&camera_ability(), Some(&o)), cfg);
	}

	#[test]
	fn tier2_camera_ability_used_when_lists_present() {
		let (cfg, src) = resolve_with_source(&camera_ability(), None);
		assert_eq!(src, TalkConfigSource::CameraAbility);
		assert_eq!(cfg.duplex, "HDX");
		assert_eq!(cfg.audio_stream_mode, "mixAudioStream");
		assert_eq!(cfg.audio_config.sample_rate, 8000);
		assert_eq!(cfg.audio_config.length_per_encoder, 512);
	}

	#[test]
	fn tier3_defaults_when_ability_empty_or_partial() {
		let (cfg, src) = resolve_with_source(&TalkAbility::default(), None);
		assert_eq!(src, TalkConfigSource::Defaults);
		assert_eq!(cfg, defaults());
		// One empty list is enough to fall through (doorbell reply shape).
		let mut partial = camera_ability();
		partial.audio_config_list.clear();
		assert_eq!(resolve_talk_config(&partial, None), defaults());
	}

	#[test]
	fn empty_override_table_equals_defaults() {
		let o: TalkConfigOverride = toml::from_str("").unwrap();
		assert_eq!(
			resolve_talk_config(&TalkAbility::default(), Some(&o)),
			defaults()
		);
	}

	#[test]
	fn encode_for_resamples_to_config_rate() {
		let clip = MonoPcm {
			sample_rate: 8000,
			samples: vec![100; 8000],
		};
		let out = encode_for(&clip, &defaults(), 1.0).unwrap();
		// 16000 samples at 1025 per 516-byte block → 16 blocks.
		assert_eq!(out.len(), 16 * 516);
	}

	#[test]
	fn encode_for_rejects_non_adpcm_and_bad_params() {
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 10],
		};
		let mut c = defaults();
		c.audio_config.audio_type = "aac".into();
		assert!(encode_for(&clip, &c, 1.0).is_err());
		let mut c = defaults();
		c.audio_config.length_per_encoder = 0;
		assert!(encode_for(&clip, &c, 1.0).is_err());
		assert!(encode_for(&clip, &defaults(), f32::NAN).is_err());
	}

	#[tokio::test]
	async fn play_falls_back_to_defaults_when_ability_query_fails() {
		let fake = FakeCameraBuilder::new()
			.with_talk_ability(|| Err(BcError::Other("no reply")))
			.build();
		let mut cfg = crate::config::test_helpers::minimal_camera_config("door");
		cfg.channel_id = 2;
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 2050],
		};
		let s = play(&*fake, &cfg, &clip, 1.0).await.unwrap();
		assert_eq!(s.source, TalkConfigSource::Defaults);
		assert_eq!(s.blocks, 2);
		let calls = fake.calls().talk.lock().unwrap();
		assert_eq!(calls.len(), 1);
		assert_eq!(calls[0].0.len(), 2 * 516);
		let mut want = defaults();
		want.channel_id = 2;
		assert_eq!(calls[0].1, want);
		assert!(!calls[0].2, "no override → gate kept");
	}

	/// No override, and the camera never answers the TalkAbility query
	/// (message 10): `play` falls back to the defaults after 5 s.
	#[tokio::test(start_paused = true)]
	async fn play_times_out_unanswered_ability_query() {
		let fake = FakeCameraBuilder::new().with_talk_ability_pending().build();
		let cfg = crate::config::test_helpers::minimal_camera_config("door");
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 1025],
		};
		let t0 = tokio::time::Instant::now();
		let s = play(&*fake, &cfg, &clip, 1.0).await.unwrap();
		assert_eq!(t0.elapsed(), ABILITY_TIMEOUT);
		assert_eq!(s.source, TalkConfigSource::Defaults);
		assert_eq!(*fake.calls().talk_ability.lock().unwrap(), 1);
		assert_eq!(fake.calls().talk.lock().unwrap().len(), 1);
	}

	/// With an override the query is never sent, so a camera that never
	/// answers message 10 costs no wait.
	#[tokio::test(start_paused = true)]
	async fn play_with_override_skips_ability_query() {
		let fake = FakeCameraBuilder::new().with_talk_ability_pending().build();
		let mut cfg = crate::config::test_helpers::minimal_camera_config("door");
		cfg.talk_config = Some(TalkConfigOverride::default());
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 1025],
		};
		let t0 = tokio::time::Instant::now();
		play(&*fake, &cfg, &clip, 1.0).await.unwrap();
		assert_eq!(t0.elapsed(), Duration::ZERO);
		assert_eq!(*fake.calls().talk_ability.lock().unwrap(), 0);
	}

	#[tokio::test]
	async fn load_clip_enforces_talk_dir() {
		let root = tempfile::tempdir().unwrap();
		let allowed = root.path().join("allowed");
		std::fs::create_dir(&allowed).unwrap();
		let inside = allowed.join("a.wav");
		let outside = root.path().join("b.wav");
		let wav = wav::tests::make_wav(16000, 1, &[1, 2]);
		std::fs::write(&inside, &wav).unwrap();
		std::fs::write(&outside, &wav).unwrap();
		assert!(load_clip(&inside, Some(&allowed)).await.is_ok());
		let err = load_clip(&outside, Some(&allowed)).await.unwrap_err();
		assert_eq!(err.code(), "outside_talk_dir");
		// `..` escapes are resolved before the check.
		let sneaky = allowed.join("../b.wav");
		let err = load_clip(&sneaky, Some(&allowed)).await.unwrap_err();
		assert_eq!(err.code(), "outside_talk_dir");
		let err = load_clip(&allowed.join("nope.wav"), Some(&allowed))
			.await
			.unwrap_err();
		assert_eq!(err.code(), "not_found");
		// No talk_dir: any regular file is accepted; a directory is not.
		assert!(load_clip(&outside, None).await.is_ok());
		let err = load_clip(root.path(), None).await.unwrap_err();
		assert_eq!(err.code(), "not_a_file");
		assert!(!format!("{err}").contains(root.path().to_str().unwrap()));
	}

	#[test]
	fn check_volume_rejects_nan_inf_negative() {
		assert!(check_volume(1.0).is_ok());
		assert!(check_volume(0.0).is_ok());
		for v in [f32::NAN, f32::INFINITY, -0.5] {
			assert!(check_volume(v).is_err(), "{v}");
		}
	}

	/// Override present, `two_way_audio` not advertised: the gate is
	/// bypassed and the override's TalkConfig is played.
	#[tokio::test]
	async fn play_with_override_bypasses_missing_ability_gate() {
		let fake = FakeCameraBuilder::new().with_abilities(&[]).build();
		let mut cfg = crate::config::test_helpers::minimal_camera_config("door");
		cfg.talk_config = Some(TalkConfigOverride {
			sample_rate: 8000,
			..Default::default()
		});
		let clip = MonoPcm {
			sample_rate: 8000,
			samples: vec![0; 1025],
		};
		let s = play(&*fake, &cfg, &clip, 1.0).await.unwrap();
		assert_eq!(s.source, TalkConfigSource::ConfigOverride);
		let calls = fake.calls().talk.lock().unwrap();
		assert_eq!(calls.len(), 1);
		assert!(calls[0].2, "override → gate bypassed");
		assert_eq!(calls[0].1.audio_config.sample_rate, 8000);
	}

	/// No override, `two_way_audio` not advertised: the gate still fails
	/// the talk with `MissingAbility`, and nothing is played.
	#[tokio::test]
	async fn play_without_override_keeps_missing_ability_gate() {
		let fake = FakeCameraBuilder::new().with_abilities(&[]).build();
		let cfg = crate::config::test_helpers::minimal_camera_config("door");
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 1025],
		};
		let err = play(&*fake, &cfg, &clip, 1.0).await.unwrap_err();
		assert!(format!("{err:#}").contains("two_way_audio"), "{err:#}");
		assert!(fake.calls().talk.lock().unwrap().is_empty());
	}

	#[tokio::test]
	async fn play_surfaces_talk_error() {
		let fake = FakeCameraBuilder::new()
			.with_talk_ability(|| Ok(TalkAbility::default()))
			.with_talk_result(|| Err(BcError::Other("rejected")))
			.build();
		let cfg = crate::config::test_helpers::minimal_camera_config("door");
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 10],
		};
		let err = play(&*fake, &cfg, &clip, 1.0).await.unwrap_err();
		assert!(format!("{err:#}").contains("talk failed"));
	}
}
