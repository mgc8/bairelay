use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
};

impl BcCamera {
	/// Read the camera's HTTP webhook configuration (`<HaCfg>`, cmd 806).
	/// `None` means a 200 reply with an empty body or without a `<HaCfg>`
	/// element. An error reply code is an `Err`; the binary's
	/// `ha_cfg_support` retries the wake-time 400 and maps a persistent
	/// error reply to "unsupported". No ability gate: `Support.webhook`
	/// does not advertise HaCfg, so only a successful read proves support.
	pub async fn get_ha_cfg(&self) -> Result<Option<HaCfg>> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_GET_HA_CFG, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_HA_CFG,
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

		Ok(match msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(BcXml { ha_cfg, .. })),
				..
			}) => ha_cfg,
			_ => None,
		})
	}

	/// Write the webhook configuration (cmd 807), then read it back with
	/// [`get_ha_cfg`](Self::get_ha_cfg) so the caller sees what the camera
	/// stored. `None` means the read-back found no `<HaCfg>`.
	pub async fn set_ha_cfg(&self, cfg: &HaCfg) -> Result<Option<HaCfg>> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_SET_HA_CFG, msg_num).await?;
		let set = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SET_HA_CFG,
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
				payload: Some(BcPayloads::BcXml(BcXml {
					ha_cfg: Some(cfg.clone()),
					..Default::default()
				})),
			}),
		};

		sub_set.send(set).await?;
		super::set_helpers::await_set_reply_with_quirk(
			&mut sub_set,
			super::set_helpers::SET_QUIRK_TIMEOUT,
		)
		.await?;
		drop(sub_set);
		self.get_ha_cfg().await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sample() -> HaCfg {
		HaCfg {
			version: "1.1".into(),
			enable: 1,
			url: "http://192.0.2.10:8080/hook/cam1".into(),
			verify_cert: 0,
		}
	}

	fn reply_cfg(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				ha_cfg: Some(sample()),
				..Default::default()
			},
		)
	}

	/// The request must carry the channel in its extension.
	fn assert_channel_extension(req: &Bc) {
		let BcBody::ModernMsg(ModernMsg { extension, .. }) = &req.body else {
			panic!("modern body expected");
		};
		let ext = extension.as_ref().expect("extension on request");
		assert_eq!(ext.channel_id, Some(0));
	}

	#[tokio::test]
	async fn get_ha_cfg_supported_returns_some() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(|req| {
				assert_channel_extension(req);
				reply_cfg(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let cfg = cam.get_ha_cfg().await.expect("ok").expect("supported");
		assert_eq!(cfg, sample());
	}

	#[tokio::test]
	async fn get_ha_cfg_empty_body_is_unsupported() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert_eq!(cam.get_ha_cfg().await.expect("ok"), None);
	}

	#[tokio::test]
	async fn get_ha_cfg_body_without_ha_cfg_is_unsupported() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert_eq!(cam.get_ha_cfg().await.expect("ok"), None);
	}

	#[tokio::test]
	async fn get_ha_cfg_error_reply_is_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_ha_cfg().await.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 400, .. }
		));
	}

	#[tokio::test]
	async fn set_ha_cfg_writes_then_reads_back() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_HA_CFG)
			.reply_with_xml(|req, xml| {
				assert_channel_extension(req);
				assert_eq!(xml.ha_cfg.as_ref().expect("ha_cfg on SET"), &sample());
				reply_200_empty(req)
			})
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(|req| {
				assert_channel_extension(req);
				reply_cfg(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let back = cam.set_ha_cfg(&sample()).await.expect("ok");
		assert_eq!(back, Some(sample()));
	}

	#[tokio::test]
	async fn set_ha_cfg_unsupported_readback_is_none() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_HA_CFG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_GET_HA_CFG)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert_eq!(cam.set_ha_cfg(&sample()).await.expect("ok"), None);
	}

	#[tokio::test]
	async fn set_ha_cfg_error_reply_skips_readback() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_HA_CFG)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.set_ha_cfg(&sample()).await.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 400, .. }
		));
	}
}
