//! `webhook` one-shot: read the camera's HTTP webhook configuration
//! (`<HaCfg>`). Read-only; also the support probe, since only a
//! successful read proves the firmware has the feature.

use anyhow::Result;
use bairelay_neolink_core::bc_protocol::CameraDriver;
use tokio_util::sync::CancellationToken;

use super::output::Outcome;
use crate::camera_tasks::HaCfgSupport;

pub async fn run(cam: &dyn CameraDriver) -> Result<Outcome> {
	let support =
		crate::camera_tasks::ha_cfg_support(cam, "oneshot", &CancellationToken::new()).await?;
	Ok(match support {
		HaCfgSupport::Supported(c) => Outcome::Webhook {
			supported: true,
			code: None,
			enable: Some(c.enable != 0),
			url: Some(c.url),
			verify_cert: Some(c.verify_cert != 0),
		},
		unsupported => Outcome::Webhook {
			supported: false,
			code: unsupported.error_code(),
			enable: None,
			url: None,
			verify_cert: None,
		},
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::HaCfg;
	use bairelay_neolink_core::bc_protocol::{Error, FakeCameraBuilder};

	fn cfg(enable: u8, url: &str, verify_cert: u8) -> HaCfg {
		HaCfg {
			version: "1.1".into(),
			enable,
			url: url.into(),
			verify_cert,
		}
	}

	#[tokio::test]
	async fn webhook_supported_reports_stored_values() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Ok(Some(cfg(1, "http://192.0.2.10/h", 0))))
			.build();
		let outcome = run(&*fake).await.unwrap();
		assert_eq!(
			outcome,
			Outcome::Webhook {
				supported: true,
				code: None,
				enable: Some(true),
				url: Some("http://192.0.2.10/h".into()),
				verify_cert: Some(false),
			}
		);
		assert!(fake.calls().set_ha_cfg.lock().unwrap().is_empty());
	}

	#[tokio::test]
	async fn webhook_unsupported_reports_no_values() {
		let fake = FakeCameraBuilder::new().with_ha_cfg(|| Ok(None)).build();
		let outcome = run(&*fake).await.unwrap();
		assert_eq!(
			outcome,
			Outcome::Webhook {
				supported: false,
				code: None,
				enable: None,
				url: None,
				verify_cert: None,
			}
		);
	}

	#[tokio::test]
	async fn webhook_error_reply_reports_its_code() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Err(Error::CameraServiceUnavailable { id: 806, code: 404 }))
			.build();
		let outcome = run(&*fake).await.unwrap();
		assert_eq!(
			outcome,
			Outcome::Webhook {
				supported: false,
				code: Some(404),
				enable: None,
				url: None,
				verify_cert: None,
			}
		);
	}

	#[tokio::test]
	async fn webhook_read_error_propagates() {
		let fake = FakeCameraBuilder::new()
			.with_ha_cfg(|| Err(Error::Other("boom")))
			.build();
		let err = run(&*fake).await.unwrap_err();
		assert!(format!("{:#}", err).contains("get_ha_cfg failed"));
	}
}
