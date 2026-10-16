use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
};

/// Directions used for Ptz
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
	/// To move the camera Up
	Up,
	/// To move the camera Down
	Down,
	/// To move the camera Left
	Left,
	/// To move the camera Right
	Right,
	/// To stop currently active PTZ command
	Stop,
}

impl BcCamera {
	/// Send a PTZ message to the camera.
	///
	/// `amount` reaches the wire as `<speed>` inside the
	/// `PtzControl` XML. Reolink firmware accepts the field as a
	/// non-negative finite number; an MQTT control payload of
	/// `NaN`, `Infinity`, or a negative value would either render
	/// as the textual literal in the XML serialiser or quietly
	/// swing the motor in the opposite direction. Validate at the
	/// boundary so the bad value never reaches the camera.
	pub async fn send_ptz(&self, direction: Direction, amount: f32) -> Result<()> {
		if !amount.is_finite() || amount < 0.0 {
			return Err(Error::Other(
				"send_ptz: amount must be a finite, non-negative f32",
			));
		}
		self.has_ability_rw("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_PTZ_CONTROL, msg_num).await?;

		let direction_str = match direction {
			Direction::Up => "up",
			Direction::Down => "down",
			Direction::Left => "left",
			Direction::Right => "right",
			Direction::Stop => "stop",
		}
		.to_string();
		let send = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_PTZ_CONTROL,
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
					ptz_control: Some(PtzControl {
						version: xml_ver(),
						channel_id: self.channel_id,
						speed: amount,
						command: direction_str,
					}),
					..Default::default()
				})),
			}),
		};

		sub_set.send(send).await?;
		let msg = sub_set.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(())
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the PtzControl xml",
			})
		}
	}

	/// Get the [PtzPreset] XML which contains the list of the preset positions known to the camera
	pub async fn get_ptz_preset(&self) -> Result<PtzPreset> {
		// Read-only operation — `get_zoom` uses the same `control`
		// ability with `_ro`. Earlier `_rw` rejected RO-only camera
		// users from listing presets.
		self.has_ability_ro("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_GET_PTZ_PRESET, msg_num).await?;

		let send = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_PTZ_PRESET,
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

		sub_set.send(send).await?;
		let msg = sub_set.recv().await?;

		if let BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::BcXml(BcXml {
				ptz_preset: Some(ptz_preset),
				..
			})),
			..
		}) = msg.body
		{
			Ok(ptz_preset)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not return a valid PtzPreset xml",
			})
		}
	}

	/// Set a PTZ preset.
	///
	/// The current position will be saved as a preset with the given `preset_id` and `name`
	pub async fn set_ptz_preset(&self, preset_id: u8, name: String) -> Result<()> {
		self.has_ability_rw("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection
			.subscribe(MSG_ID_PTZ_CONTROL_PRESET, msg_num)
			.await?;

		let preset = Preset {
			id: preset_id,
			name: Some(name),
			command: "setPos".to_owned(),
		};
		let send = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_PTZ_CONTROL_PRESET,
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
					ptz_preset: Some(PtzPreset {
						preset_list: PresetList {
							preset: vec![preset],
						},
						..Default::default()
					}),
					..Default::default()
				})),
			}),
		};

		sub_set.send(send).await?;
		let msg = sub_set.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(())
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the PtzPreset xml",
			})
		}
	}

	/// The camera will attempt to move to the preset with the given ID.
	pub async fn moveto_ptz_preset(&self, preset_id: u8) -> Result<()> {
		self.has_ability_rw("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection
			.subscribe(MSG_ID_PTZ_CONTROL_PRESET, msg_num)
			.await?;

		let preset = Preset {
			id: preset_id,
			name: None,
			command: "toPos".to_owned(),
		};
		let send = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_PTZ_CONTROL_PRESET,
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
					ptz_preset: Some(PtzPreset {
						preset_list: PresetList {
							preset: vec![preset],
						},
						..Default::default()
					}),
					..Default::default()
				})),
			}),
		};

		sub_set.send(send).await?;
		let msg = sub_set.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(())
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the PtzPreset xml",
			})
		}
	}

	/// The camera will zoom to a given zoom amount.
	/// Not sure what the units for this are, seems to be 1000 is 1x and 2000 is 2x
	pub async fn zoom_to(&self, zoom_pos: u32) -> Result<()> {
		let current = self.get_zoom().await?;
		let zoom_pos = zoom_pos.clamp(current.zoom.min_pos, current.zoom.max_pos);

		self.has_ability_rw("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set = connection.subscribe(MSG_ID_SET_ZOOM_FOCUS, msg_num).await?;
		let send = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SET_ZOOM_FOCUS,
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
					start_zoom_focus: Some(StartZoomFocus {
						version: xml_ver(),
						channel_id: self.channel_id,
						command: "zoomPos".to_string(),
						move_pos: zoom_pos,
					}),
					..Default::default()
				})),
			}),
		};

		sub_set.send(send).await?;

		let msg = sub_set.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(())
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the StartZoomFocus xml",
			})
		}
	}

	/// Get the zoom xml, that has current min and max zoom values
	pub async fn get_zoom(&self) -> Result<PtzZoomFocus> {
		self.has_ability_ro("control").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_GET_ZOOM_FOCUS, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_ZOOM_FOCUS,
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
				ptz_zoom_focus: Some(xml),
				..
			})),
			..
		}) = msg.body
		{
			Ok(xml)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Expected PtzZoomFocus xml but it was not received",
			})
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn send_ptz_happy_path() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL)
			.reply_with_xml(|req, xml| {
				let p = xml
					.ptz_control
					.as_ref()
					.expect("ptz_control on PTZ request");
				assert_eq!(p.command, "up");
				assert!((p.speed - 1.0).abs() < f32::EPSILON);
				assert_eq!(p.channel_id, 0);
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		cam.send_ptz(Direction::Up, 1.0)
			.await
			.expect("send_ptz should succeed");
	}

	#[tokio::test]
	async fn send_ptz_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.send_ptz(Direction::Up, 1.0)
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn send_ptz_every_direction_sends_request() {
		// Pin the wire-string for every Direction. Without this, a
		// regression that mapped every direction to "up" would still
		// pass — exactly the failure mode the audit catches.
		for (d, expected_command) in [
			(Direction::Up, "up"),
			(Direction::Down, "down"),
			(Direction::Left, "left"),
			(Direction::Right, "right"),
			(Direction::Stop, "stop"),
		] {
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_PTZ_CONTROL)
				.reply_with_xml(move |req, xml| {
					let p = xml.ptz_control.as_ref().expect("ptz_control");
					assert_eq!(
						p.command, expected_command,
						"Direction::{:?} must map to wire {:?}",
						d, expected_command,
					);
					assert!((p.speed - 0.5).abs() < f32::EPSILON);
					reply_200_empty(req)
				})
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			cam.test_set_ability("control", true).await;
			cam.send_ptz(d, 0.5).await.expect("send_ptz ok");
		}
	}

	#[tokio::test]
	async fn send_ptz_rejects_nan_inf_and_negative() {
		// Validation must reject these BEFORE any IO. Build a
		// camera with a closed mock so the `subscribe` call would
		// never succeed — if validation accidentally lets the
		// value through we'd see a different error shape.
		for amount in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.5_f32] {
			let mock = MockConnection::new().build().await;
			let cam = BcCamera::from_mock_connection(mock).await;
			cam.test_set_ability("control", true).await;
			let err = cam
				.send_ptz(Direction::Up, amount)
				.await
				.expect_err("must reject");
			assert!(
				matches!(err, Error::Other(msg) if msg.contains("send_ptz")),
				"unexpected error for amount={amount}: {err:?}"
			);
		}
	}

	#[tokio::test]
	async fn send_ptz_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let err = cam
			.send_ptz(Direction::Up, 1.0)
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn get_ptz_preset_happy_path_parses_reply() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_PTZ_PRESET)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ptz_preset: Some(PtzPreset {
							version: "1.1".to_string(),
							channel_id: 0,
							preset_list: PresetList {
								preset: vec![Preset {
									id: 1,
									name: Some("home".to_string()),
									command: "setPos".to_string(),
								}],
							},
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let p = cam.get_ptz_preset().await.expect("ok");
		assert_eq!(p.preset_list.preset.len(), 1);
		assert_eq!(p.preset_list.preset[0].id, 1);
	}

	#[tokio::test]
	async fn get_ptz_preset_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_ptz_preset().await.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn get_ptz_preset_missing_xml_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_PTZ_PRESET)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let err = cam.get_ptz_preset().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn set_ptz_preset_happy_path() {
		// set_ptz_preset must wire-encode a single Preset with the
		// requested id, the requested name, and command="setPos" —
		// the field that distinguishes save from move in this RPC.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL_PRESET)
			.reply_with_xml(|req, xml| {
				let p = xml.ptz_preset.as_ref().expect("ptz_preset on SET request");
				assert_eq!(p.preset_list.preset.len(), 1);
				let preset = &p.preset_list.preset[0];
				assert_eq!(preset.id, 1);
				assert_eq!(preset.name.as_deref(), Some("home"));
				assert_eq!(preset.command, "setPos");
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		cam.set_ptz_preset(1, "home".to_string()).await.expect("ok");
	}

	#[tokio::test]
	async fn set_ptz_preset_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.set_ptz_preset(1, "home".to_string())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn set_ptz_preset_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL_PRESET)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let err = cam
			.set_ptz_preset(1, "home".to_string())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn moveto_ptz_preset_happy_path() {
		// moveto_ptz_preset shares the msg_id with set_ptz_preset; the
		// distinction is command="toPos" vs "setPos" and name=None vs
		// Some(_). Pin both so the dispatch can't accidentally save
		// over the preset when the operator asked to move to it.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL_PRESET)
			.reply_with_xml(|req, xml| {
				let p = xml.ptz_preset.as_ref().expect("ptz_preset on MOVE");
				assert_eq!(p.preset_list.preset.len(), 1);
				let preset = &p.preset_list.preset[0];
				assert_eq!(preset.id, 2);
				assert_eq!(preset.name, None);
				assert_eq!(preset.command, "toPos");
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		cam.moveto_ptz_preset(2).await.expect("ok");
	}

	#[tokio::test]
	async fn moveto_ptz_preset_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.moveto_ptz_preset(1).await.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn moveto_ptz_preset_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_PTZ_CONTROL_PRESET)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let err = cam.moveto_ptz_preset(1).await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn get_zoom_happy_path() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_ZOOM_FOCUS)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ptz_zoom_focus: Some(PtzZoomFocus {
							version: "1.1".to_string(),
							channel_id: 0,
							zoom: HelperPosition {
								cur_pos: 1500,
								min_pos: 1000,
								max_pos: 2000,
							},
							focus: HelperPosition {
								cur_pos: 0,
								min_pos: 0,
								max_pos: 0,
							},
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", false).await;
		let zf = cam.get_zoom().await.expect("ok");
		assert_eq!(zf.zoom.cur_pos, 1500);
	}

	#[tokio::test]
	async fn get_zoom_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_zoom().await.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn get_zoom_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_ZOOM_FOCUS)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", false).await;
		let err = cam.get_zoom().await.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn get_zoom_missing_xml_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_ZOOM_FOCUS)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", false).await;
		let err = cam.get_zoom().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn zoom_to_clamps_and_sends() {
		// zoom_to first calls get_zoom to learn the min/max, then issues
		// MSG_ID_SET_ZOOM_FOCUS. Requested value 5000 clamps to max 2000.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_ZOOM_FOCUS)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ptz_zoom_focus: Some(PtzZoomFocus {
							version: "1.1".to_string(),
							channel_id: 0,
							zoom: HelperPosition {
								cur_pos: 1500,
								min_pos: 1000,
								max_pos: 2000,
							},
							focus: HelperPosition {
								cur_pos: 0,
								min_pos: 0,
								max_pos: 0,
							},
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_ZOOM_FOCUS)
			.reply_with_xml(|req, xml| {
				let zf = xml
					.start_zoom_focus
					.as_ref()
					.expect("start_zoom_focus on SET");
				// Requested 5000 must clamp to max_pos=2000.
				assert_eq!(zf.move_pos, 2000);
				assert_eq!(zf.command, "zoomPos");
				assert_eq!(zf.channel_id, 0);
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		cam.zoom_to(5000).await.expect("ok");
	}

	#[tokio::test]
	async fn zoom_to_non_200_on_set_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_ZOOM_FOCUS)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ptz_zoom_focus: Some(PtzZoomFocus {
							version: "1.1".to_string(),
							channel_id: 0,
							zoom: HelperPosition {
								cur_pos: 1500,
								min_pos: 1000,
								max_pos: 2000,
							},
							focus: HelperPosition {
								cur_pos: 0,
								min_pos: 0,
								max_pos: 0,
							},
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_ZOOM_FOCUS)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("control", true).await;
		let err = cam.zoom_to(1500).await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}
}
