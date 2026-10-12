use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
};

impl BcCamera {
	/// Get the [LedState] xml. One reply carries two unrelated features:
	/// `state` (IR night-vision LEDs) and `lightState` (status LED). A
	/// camera answers with a placeholder for whichever it lacks, so the
	/// reply says nothing about presence — callers decide that per
	/// feature. No ability gate: Argus models with a status LED don't
	/// advertise `ledState` yet answer this message correctly.
	pub async fn get_ledstate(&self) -> Result<LedState> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_GET_LED_STATUS, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_LED_STATUS,
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

		if let BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::BcXml(BcXml {
				led_state: Some(ledstate),
				..
			})),
			..
		}) = msg.body
		{
			Ok(ledstate)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Expected LEDState xml but it was not received",
			})
		}
	}

	/// Set the led lights using the [LedState] xml
	pub async fn set_ledstate(&self, mut led_state: LedState) -> Result<()> {
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_SET_LED_STATUS, msg_num).await?;

		// led_version is a field received from the camera but not sent
		// we set to None to ensure we don't send it to the camera
		led_state.led_version = None;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SET_LED_STATUS,
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
					led_state: Some(led_state),
					..Default::default()
				})),
			}),
		};

		sub_set.send(get).await?;
		super::set_helpers::await_set_reply_with_quirk(
			&mut sub_set,
			super::set_helpers::SET_QUIRK_TIMEOUT,
		)
		.await
	}

	/// This is a convience function to control the IR LED lights
	///
	/// This is for the RED IR lights that can come on automaitcally
	/// during low light.
	pub async fn irled_light_set(&self, state: LightState) -> Result<()> {
		let mut led_state = self.get_ledstate().await?;
		led_state.state = match state {
			LightState::On => "open".to_string(),
			LightState::Off => "close".to_string(),
			LightState::Auto => "auto".to_string(),
		};
		self.set_ledstate(led_state).await?;
		Ok(())
	}

	/// This is a convience function to control the LED light
	/// True is on and false is off
	///
	/// This is for the little blue on light of some camera
	pub async fn led_light_set(&self, state: bool) -> Result<()> {
		let mut led_state = self.get_ledstate().await?;
		led_state.light_state = match state {
			true => "open".to_string(),
			false => "close".to_string(),
		};
		self.set_ledstate(led_state).await?;
		Ok(())
	}
}

/// This is pased to `irled_light_set` to turn it on, off or set it to light based auto
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LightState {
	/// Turn the light on
	On,
	/// Turn the light off
	Off,
	/// Set the light to light based auto
	Auto,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn get_ledstate_happy_path_parses_reply() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_LED_STATUS)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						led_state: Some(LedState {
							version: "1.1".to_string(),
							channel_id: 0,
							led_version: Some(2),
							state: "auto".to_string(),
							light_state: "open".to_string(),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", false).await;
		let ls = cam.get_ledstate().await.expect("ok");
		assert_eq!(ls.state, "auto");
		assert_eq!(ls.light_state, "open");
	}

	/// No `ledState` ability advertised: the request is still sent and
	/// the reply parsed (status-LED-only cameras omit the ability).
	#[tokio::test]
	async fn get_ledstate_sends_without_ledstate_ability() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_LED_STATUS)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						led_state: Some(sample_led_state()),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let ls = cam.get_ledstate().await.expect("sent despite no ability");
		assert_eq!(ls.light_state, sample_led_state().light_state);
	}

	fn sample_led_state() -> LedState {
		LedState {
			version: "1".into(),
			channel_id: 0,
			led_version: Some(2),
			state: "auto".into(),
			light_state: "close".into(),
		}
	}

	#[tokio::test]
	async fn get_ledstate_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_LED_STATUS)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", false).await;
		let err = cam.get_ledstate().await.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn get_ledstate_missing_payload_returns_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_LED_STATUS)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", false).await;
		let err = cam.get_ledstate().await.expect_err("err");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn set_ledstate_happy_path() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_LED_STATUS)
			.reply_with_xml(|req, xml| {
				let ls = xml
					.led_state
					.as_ref()
					.expect("led_state on the SET request");
				assert_eq!(ls.state, "auto");
				assert_eq!(ls.light_state, "close");
				// led_version must be stripped on outbound; the camera
				// sends it but bairelay does not echo it back.
				assert!(
					ls.led_version.is_none(),
					"led_version must be stripped on outbound"
				);
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", true).await;
		cam.set_ledstate(sample_led_state()).await.expect("ok");
	}

	#[tokio::test]
	async fn set_ledstate_no_reply_returns_ok() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_LED_STATUS)
			.reply_none()
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", true).await;
		cam.set_ledstate(sample_led_state())
			.await
			.expect("no-reply → Ok");
	}

	#[tokio::test]
	async fn set_ledstate_non_200_returns_service_unavailable() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_LED_STATUS)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("ledState", true).await;
		let err = cam.set_ledstate(sample_led_state()).await.expect_err("err");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	/// No `ledState` ability advertised: the write still reaches the camera.
	#[tokio::test]
	async fn set_ledstate_sends_without_ledstate_ability() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SET_LED_STATUS)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.set_ledstate(sample_led_state())
			.await
			.expect("sent despite no ability");
	}

	#[tokio::test]
	async fn irled_light_set_covers_all_three_states() {
		// Pin the wire mapping for each LightState branch — without
		// this, an `On → "close"` regression in the impl would still
		// pass the test (the original failure mode the audit catches).
		for (state, expected_state, expected_light_state) in [
			(LightState::On, "open", "close"),
			(LightState::Off, "close", "close"),
			(LightState::Auto, "auto", "close"),
		] {
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_GET_LED_STATUS)
				.reply_with(|req| {
					reply_200_xml(
						req,
						BcXml {
							led_state: Some(LedState {
								version: "1".into(),
								channel_id: 0,
								led_version: Some(2),
								state: "auto".into(),
								light_state: "close".into(),
							}),
							..Default::default()
						},
					)
				})
				.expect_msg(MSG_ID_SET_LED_STATUS)
				.reply_with_xml(move |req, xml| {
					let ls = xml.led_state.as_ref().expect("led_state on SET");
					assert_eq!(
						ls.state, expected_state,
						"irled_light_set({:?}) must send state={:?}",
						state, expected_state,
					);
					// light_state is preserved from the GET — the IR
					// helper only mutates `state`, not `light_state`.
					assert_eq!(ls.light_state, expected_light_state);
					reply_200_empty(req)
				})
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			cam.test_set_ability("ledState", true).await;
			cam.irled_light_set(state).await.expect("ok");
		}
	}

	#[tokio::test]
	async fn led_light_set_on_and_off() {
		// Same audit as irled_light_set: pin true→"open"/false→"close"
		// for the blue status LED helper.
		for (state, expected_light_state) in [(true, "open"), (false, "close")] {
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_GET_LED_STATUS)
				.reply_with(|req| {
					reply_200_xml(
						req,
						BcXml {
							led_state: Some(LedState {
								version: "1".into(),
								channel_id: 0,
								led_version: Some(2),
								state: "auto".into(),
								light_state: "close".into(),
							}),
							..Default::default()
						},
					)
				})
				.expect_msg(MSG_ID_SET_LED_STATUS)
				.reply_with_xml(move |req, xml| {
					let ls = xml.led_state.as_ref().expect("led_state on SET");
					assert_eq!(
						ls.light_state, expected_light_state,
						"led_light_set({}) must send light_state={:?}",
						state, expected_light_state,
					);
					// `state` is preserved from the GET — the helper
					// only mutates `light_state`.
					assert_eq!(ls.state, "auto");
					reply_200_empty(req)
				})
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			cam.test_set_ability("ledState", true).await;
			cam.led_light_set(state).await.expect("ok");
		}
	}

	#[test]
	fn light_state_derives_match() {
		// Touch Clone/Copy/Debug/Eq derivation for completeness.
		let a = LightState::On;
		let b = a;
		assert_eq!(a, b);
		let _ = format!("{:?}", LightState::Auto);
		assert_ne!(LightState::Off, LightState::Auto);
	}
}
