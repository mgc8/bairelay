//! `talk` one-shot: play a WAV clip through the camera speaker. The
//! clip is loaded by the caller before connecting; see
//! [`crate::talk`] for the flow.

use anyhow::Result;
use bairelay_neolink_core::bc_protocol::CameraDriver;

use super::output::Outcome;
use crate::config::CameraConfig;
use crate::talk::{self, wav::MonoPcm};

pub async fn run(
	cam: &dyn CameraDriver,
	cam_cfg: &CameraConfig,
	clip: &MonoPcm,
	volume: f32,
) -> Result<Outcome> {
	let s = talk::play(cam, cam_cfg, clip, volume).await?;
	Ok(Outcome::Talk {
		config_source: s.source.as_str().to_string(),
		sample_rate: s.sample_rate,
		blocks: s.blocks,
		duration_ms: s.duration.as_millis() as u64,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::TalkAbility;
	use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

	#[tokio::test]
	async fn talk_returns_talk_outcome_and_calls_driver_once() {
		let fake = FakeCameraBuilder::new()
			.with_talk_ability(|| Ok(TalkAbility::default()))
			.build();
		let cfg = crate::config::test_helpers::minimal_camera_config("door");
		let clip = MonoPcm {
			sample_rate: 16000,
			samples: vec![0; 16000],
		};
		let out = run(&*fake, &cfg, &clip, 1.0).await.unwrap();
		assert_eq!(
			out,
			Outcome::Talk {
				config_source: "defaults".into(),
				sample_rate: 16000,
				blocks: 16,
				duration_ms: 1000,
			}
		);
		assert_eq!(fake.calls().talk.lock().unwrap().len(), 1);
	}
}
