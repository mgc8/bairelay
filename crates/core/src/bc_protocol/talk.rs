use super::{BcCamera, Error, Result};
use crate::{bc::model::*, bc::xml::*, bcmedia::model::*};
use crossbeam_channel::Receiver;
use std::io::{BufRead, Error as IoError, ErrorKind, Read};

type IoResult<T> = std::result::Result<T, IoError>;

impl BcCamera {
	///
	/// Finish Talk
	///
	/// The send the talk finish to the camera.
	///
	/// It is also sent when the request for talk config returns status code 422
	///
	pub async fn talk_stop(&self) -> Result<()> {
		// Two-way audio gates on the `two_way_audio` ability — battery
		// Argus cameras don't have it. The talk path is deferred per
		// spec §10 today; gating it here is a no-cost MissingAbility
		// shape for any future bairelay caller plus the live neolink
		// CLI.
		self.has_ability_rw("two_way_audio").await?;
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALKRESET, msg_num).await?;

		let msg = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKRESET,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
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

		sub.send(msg).await?;
		let msg = sub.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the talk stop command.",
			});
		}

		Ok(())
	}

	///
	/// Requests the [`TalkAbility`] xml
	///
	pub async fn talk_ability(&self) -> Result<TalkAbility> {
		// Same gate as the active talk methods — querying TalkAbility
		// on a non-talk camera should surface MissingAbility, not the
		// generic protocol error the camera replies with.
		self.has_ability_ro("two_way_audio").await?;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_TALKABILITY, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKABILITY,
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

		if let BcBody::ModernMsg(ModernMsg {
			payload:
				Some(BcPayloads::BcXml(BcXml {
					talk_ability: Some(talk_ability),
					..
				})),
			..
		}) = msg.body
		{
			Ok(talk_ability)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Expected TalkAbility xml but it was not received",
			})
		}
	}

	///
	/// Send sound to the camera
	///
	/// The data should be in the format as described in `<TalkAbility>` xml
	/// This method assumes that you have set up the data in the desired format
	/// in the `<TalkAbility>` xml
	///
	/// It also checks that it is ADPCM as the code is written to accept only that
	///
	/// # Parameters
	///
	/// * `adpcm` - Data must be adpcm in DVI-4 format
	///
	/// * `talk_config` - The talk config that describes the adpcm data
	///
	///
	pub async fn talk(&self, adpcm: &[u8], talk_config: TalkConfig) -> Result<()> {
		self.has_ability_rw("two_way_audio").await?;
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALKCONFIG, msg_num).await?;

		if &talk_config.audio_config.audio_type != "adpcm" {
			return Err(Error::UnknownTalkEncoding);
		}

		let block_size = talk_config.audio_config.length_per_encoder / 2;
		let sample_rate = talk_config.audio_config.sample_rate;

		let build_talk_config_msg = |talk_config: TalkConfig| Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKCONFIG,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: Some(BcPayloads::BcXml(BcXml {
					talk_config: Some(talk_config),
					..Default::default()
				})),
			}),
		};

		sub.send(build_talk_config_msg(talk_config.clone())).await?;
		let mut msg = sub.recv().await?;

		// If another client is already talking OR if we crashed before sending
		// msgid 11 the camera will reply 422. The official client retries
		// the original TalkConfig request — re-send the request, NOT the
		// 422 reply.
		if let BcMeta {
			response_code: 422, ..
		} = msg.meta
		{
			self.talk_stop().await?;
			sub.send(build_talk_config_msg(talk_config)).await?;
			msg = sub.recv().await?;
		}

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why:
					"The camera did not accept the TalkConfig xml. Audio format is likely incorrect",
			});
		}

		let full_block_size = block_size + 4; // Block size + predictor state
		let msg_num = self.new_message_num();
		let sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;

		const BLOCK_PER_PAYLOAD: usize = 4;
		const BLOCK_HEADER_SIZE: usize = 4;
		const SAMPLES_PER_BYTE: usize = 2;

		for payload_bytes in adpcm.chunks(full_block_size as usize * BLOCK_PER_PAYLOAD) {
			let mut payload = vec![];
			for bytes in payload_bytes.chunks(full_block_size as usize) {
				let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm {
					data: bytes.to_vec(),
				});
				payload = bcmedia_adpcm.serialize(payload)?;
			}

			let msg = Bc {
				meta: BcMeta {
					msg_id: MSG_ID_TALK,
					channel_id: self.channel_id,
					msg_num,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: Some(Extension {
						channel_id: Some(self.channel_id),
						binary_data: Some(1),
						..Default::default()
					}),
					payload: Some(BcPayloads::Binary(payload)),
				}),
			};

			sub.send(msg).await?;

			let adpcm_len = payload_bytes.len();
			// There are two samples per byte
			//
			// To calculate the bytes we subtract the block headers from the len
			//
			// There is 1 initial sample stored in the block header so we add that in the end
			//
			let samples_sent = (adpcm_len - BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD)
				* SAMPLES_PER_BYTE
				+ BLOCK_PER_PAYLOAD;

			// Time to play the sample in seconds
			let play_length = samples_sent as f32 / sample_rate as f32;
			tokio::time::sleep(std::time::Duration::from_secs_f32(play_length)).await;
		}

		self.talk_stop().await?;

		Ok(())
	}

	///
	/// Send sound to the camera through a channel
	///
	/// This is similar to [`talk`](Self::talk) except it uses a channel to receive data
	///
	/// The data should be in the format as described in `<TalkAbility>` xml
	/// This method assumes that you have set up the data in the desired format
	/// in the `<TalkAbility>` xml
	///
	/// It also checks that it is ADPCM as the code is written to accept only that
	///
	/// # Parameters
	///
	/// * `adpcm` - Data must be adpcm in DVI-4 format
	///
	/// * `talk_config` - The talk config that describes the adpcm data
	///
	///
	pub async fn talk_stream(&self, rx: Receiver<Vec<u8>>, talk_config: TalkConfig) -> Result<()> {
		self.has_ability_rw("two_way_audio").await?;
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALKCONFIG, msg_num).await?;

		if &talk_config.audio_config.audio_type != "adpcm" {
			return Err(Error::UnknownTalkEncoding);
		}

		let block_size = talk_config.audio_config.length_per_encoder / 2;
		let sample_rate = talk_config.audio_config.sample_rate;

		let build_talk_config_msg = |talk_config: TalkConfig| Bc {
			meta: BcMeta {
				msg_id: MSG_ID_TALKCONFIG,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: Some(BcPayloads::BcXml(BcXml {
					talk_config: Some(talk_config),
					..Default::default()
				})),
			}),
		};

		sub.send(build_talk_config_msg(talk_config.clone())).await?;
		let mut msg = sub.recv().await?;

		// If another client is already talking OR if we crashed before sending
		// msgid 11 the camera will reply 422. The official client retries
		// the original TalkConfig request — re-send the request, NOT the
		// 422 reply.
		if let BcMeta {
			response_code: 422, ..
		} = msg.meta
		{
			self.talk_stop().await?;
			sub.send(build_talk_config_msg(talk_config)).await?;
			msg = sub.recv().await?;
		}

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why:
					"The camera did not accept the TalkConfig xml. Audio format is likely incorrect",
			});
		}

		let full_block_size = block_size + 4; // Block size + predictor state
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_TALK, msg_num).await?;

		const BLOCK_PER_PAYLOAD: usize = 1;
		const BLOCK_HEADER_SIZE: usize = 4;
		const SAMPLES_PER_BYTE: usize = 2;

		let mut buffered_recv = BufferedStream::from_rx(rx);

		let target_chunks = full_block_size as usize * BLOCK_PER_PAYLOAD;

		let mut end_of_stream = false;
		let mut expected_stream_end = std::time::Instant::now();
		while !end_of_stream {
			let mut payload_bytes = vec![];
			while payload_bytes.len() < target_chunks {
				let mut buffer = vec![255; target_chunks - payload_bytes.len()];
				if let Ok(read) = buffered_recv.read(&mut buffer) {
					payload_bytes.extend(&buffer[..read]);
				} else {
					// Error should occur if the channel is dropped
					// and all bytes are consumed
					end_of_stream = true;
				}
				if end_of_stream {
					break;
				}
			}

			let mut payload = vec![];
			for block_bytes in payload_bytes.chunks(full_block_size as usize) {
				let bytes: Vec<u8> = block_bytes.to_vec();
				let bcmedia_adpcm = BcMedia::Adpcm(BcMediaAdpcm { data: bytes });
				payload = bcmedia_adpcm.serialize(payload)?;
			}

			let adpcm_len = payload_bytes.len();

			// There are two samples per byte
			//
			// To calculate the bytes we subtract the block headers from the len
			//
			// There is 1 initial sample stored in the block header so we add that in the end
			//
			let samples_sent = if adpcm_len >= BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD {
				(adpcm_len - BLOCK_HEADER_SIZE * BLOCK_PER_PAYLOAD) * SAMPLES_PER_BYTE
					+ BLOCK_PER_PAYLOAD
			} else {
				// Zero samples in this block
				break;
			};

			// Time to play the sample in seconds
			let play_length = samples_sent as f32 / sample_rate as f32;

			let msg = Bc {
				meta: BcMeta {
					msg_id: MSG_ID_TALK,
					channel_id: self.channel_id,
					msg_num,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: Some(Extension {
						channel_id: Some(self.channel_id),
						binary_data: Some(1),
						..Default::default()
					}),
					payload: Some(BcPayloads::Binary(payload)),
				}),
			};

			let time_sent = std::time::Instant::now();
			sub.send(msg).await?;
			let play_length = std::time::Duration::from_secs_f32(play_length);
			if time_sent > expected_stream_end {
				expected_stream_end = time_sent + play_length;
			} else {
				expected_stream_end += play_length;
			}
			let _ = sub.recv().await?;
		}

		// Chunks are still being played, while talk_stop will interrupt them. Wait until we expect
		// the stream to end (+ and extra 100ms) before issuing talk_stop.
		// `saturating_duration_since`: if scheduling drift pushed `now()`
		// past `expected_stream_end`, the difference is zero, not a panic.
		let remaining_stream_duration =
			expected_stream_end.saturating_duration_since(std::time::Instant::now());
		tokio::time::sleep(remaining_stream_duration + std::time::Duration::from_secs_f32(0.1))
			.await;

		self.talk_stop().await?;

		Ok(())
	}
}

struct BufferedStream {
	rx: Receiver<Vec<u8>>,
	buffer: Vec<u8>,
	consumed: usize,
}

impl BufferedStream {
	pub fn from_rx(rx: Receiver<Vec<u8>>) -> BufferedStream {
		BufferedStream {
			rx,
			buffer: vec![],
			consumed: 0,
		}
	}
}

impl Read for BufferedStream {
	fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
		let buffer = self.fill_buf()?;
		let amt = std::cmp::min(buf.len(), buffer.len());

		// First check if the amount of bytes we want to read is small:
		// `copy_from_slice` will generally expand to a call to `memcpy`, and
		// for a single byte the overhead is significant.
		if amt == 1 {
			buf[0] = buffer[0];
		} else {
			buf[..amt].copy_from_slice(&buffer[..amt]);
		}

		self.consume(amt);

		Ok(amt)
	}
}

impl BufRead for BufferedStream {
	fn fill_buf(&mut self) -> IoResult<&[u8]> {
		const CLEAR_CONSUMED_AT: usize = 1024;
		// This is a trade off between caching too much dead memory
		// and calling the drain method too often
		if self.consumed > CLEAR_CONSUMED_AT {
			let _ = self.buffer.drain(0..self.consumed).collect::<Vec<u8>>();
			self.consumed = 0;
		}
		while self.buffer.len() <= self.consumed {
			let data = self
				.rx
				.recv()
				.map_err(|err| IoError::new(ErrorKind::ConnectionReset, err))?;
			self.buffer.extend(data);
		}

		Ok(&self.buffer.as_slice()[self.consumed..])
	}

	fn consume(&mut self, amt: usize) {
		assert!(self.consumed + amt <= self.buffer.len());
		self.consumed += amt;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc_protocol::connection::mock::{reply_200_empty, reply_err_code, MockConnection};
	use std::io::Read;
	use std::time::Duration;

	fn ok_talk_config() -> TalkConfig {
		TalkConfig {
			version: "1.1".to_string(),
			channel_id: 0,
			duplex: "FDX".to_string(),
			audio_stream_mode: "followVideoStream".to_string(),
			audio_config: AudioConfig {
				priority: None,
				audio_type: "adpcm".to_string(),
				sample_rate: 16000,
				sample_precision: 16,
				length_per_encoder: 1024,
				sound_track: "mono".to_string(),
			},
		}
	}

	fn bad_talk_config() -> TalkConfig {
		let mut t = ok_talk_config();
		t.audio_config.audio_type = "aac".to_string();
		t
	}

	// ── talk_stop ─────────────────────────────────────────────────────

	#[tokio::test]
	async fn talk_stop_happy_path_accepts_200() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		cam.talk_stop().await.expect("talk_stop 200");
	}

	#[tokio::test]
	async fn talk_stop_non_200_returns_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam.talk_stop().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk_ability ──────────────────────────────────────────────────

	#[tokio::test]
	async fn talk_ability_happy_path_parses_xml() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKABILITY)
			.reply_with(|req| {
				Bc::new_from_xml(
					BcMeta {
						msg_id: MSG_ID_TALKABILITY,
						channel_id: req.meta.channel_id,
						msg_num: req.meta.msg_num,
						stream_type: 0,
						response_code: 200,
						class: 0x6414,
					},
					BcXml {
						talk_ability: Some(TalkAbility {
							version: "1.1".to_string(),
							duplex_list: vec![DuplexList {
								duplex: "FDX".to_string(),
							}],
							audio_stream_mode_list: vec![AudioStreamModeList {
								audio_stream_mode: "followVideoStream".to_string(),
							}],
							audio_config_list: vec![AudioConfigList {
								audio_config: ok_talk_config().audio_config,
							}],
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let ability = cam.talk_ability().await.expect("ability");
		assert_eq!(ability.version, "1.1");
		assert_eq!(ability.duplex_list.len(), 1);
		assert_eq!(ability.audio_config_list.len(), 1);
	}

	#[tokio::test]
	async fn talk_ability_missing_xml_returns_unintelligible() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKABILITY)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam.talk_ability().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk: input validation ────────────────────────────────────────

	#[tokio::test]
	async fn talk_rejects_non_adpcm_audio_type_before_network() {
		// Non-adpcm config must short-circuit before any reply is
		// scripted; we script nothing to prove no traffic occurs.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_none()
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let err = cam
			.talk(&[0u8; 16], bad_talk_config())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnknownTalkEncoding));
	}

	#[tokio::test]
	async fn talk_stream_rejects_non_adpcm_audio_type_before_network() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_none()
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let (_tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		let err = cam
			.talk_stream(rx, bad_talk_config())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnknownTalkEncoding));
	}

	// ── talk: non-200 on TalkConfig reply ─────────────────────────────

	#[tokio::test]
	async fn talk_non_200_on_talkconfig_returns_unintelligible() {
		// TalkConfig reply returns 500 directly (not 422, so no retry).
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let r = tokio::time::timeout(
			Duration::from_millis(300),
			cam.talk(&[0u8; 16], ok_talk_config()),
		)
		.await;
		let err = r.expect("timely").expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── talk: happy path (TALKCONFIG 200 → TALK block → TALKRESET) ────

	#[tokio::test]
	async fn talk_happy_path_sends_config_block_and_stop() {
		// Use tiny adpcm buffer (16 bytes) so only one TALK payload
		// fires and std::thread::sleep stays well under 100 ms.
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8; // block_size = 4
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let adpcm = vec![0u8; 32]; // 4 blocks of 8 bytes = 1 payload
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&adpcm, cfg))
			.await
			.expect("no hang")
			.expect("talk ok");
	}

	#[tokio::test]
	async fn talk_retries_once_on_422_config_reply() {
		// First TALKCONFIG → 422, which triggers an internal talk_stop
		// (TALKRESET → 200) and a retry (TALKCONFIG → 200).
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8;
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 422))
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_none()
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let adpcm = vec![0u8; 32];
		tokio::time::timeout(Duration::from_secs(2), cam.talk(&adpcm, cfg))
			.await
			.expect("no hang")
			.expect("retry ok");
	}

	// ── talk_stream: happy path through ADPCM block loop ─────────────

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn talk_stream_happy_path_streams_one_block_then_closes_channel() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8; // full_block_size = 8 bytes
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;

		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		// Push exactly one full block (8 bytes), then close.
		tx.send(vec![0x10, 0x20, 0x30, 0x40, 0x55, 0x66, 0x77, 0x88])
			.unwrap();
		drop(tx);

		let r = tokio::time::timeout(Duration::from_secs(2), cam.talk_stream(rx, cfg)).await;
		r.expect("no hang").expect("talk_stream ok");
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn talk_stream_422_retry_on_talkconfig_then_streams() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8;
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 422))
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALK)
			.reply_with(reply_200_empty)
			.expect_msg(MSG_ID_TALKRESET)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;

		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		tx.send(vec![1u8, 2, 3, 4, 5, 6, 7, 8]).unwrap();
		drop(tx);

		let r = tokio::time::timeout(Duration::from_secs(2), cam.talk_stream(rx, cfg)).await;
		r.expect("no hang").expect("retry ok");
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn talk_stream_non_200_on_config_returns_unintelligible() {
		let mut cfg = ok_talk_config();
		cfg.audio_config.length_per_encoder = 8;
		cfg.audio_config.sample_rate = 16000;

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_TALKCONFIG)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("two_way_audio", true).await;
		let (_tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();

		let r = tokio::time::timeout(Duration::from_millis(500), cam.talk_stream(rx, cfg)).await;
		let err = r.expect("timely").expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ── BufferedStream (Read / BufRead) ───────────────────────────────

	#[test]
	fn buffered_stream_reads_contiguous_chunks_across_multiple_recvs() {
		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		tx.send(b"hello ".to_vec()).unwrap();
		tx.send(b"world".to_vec()).unwrap();
		drop(tx); // close channel so second fill_buf surfaces a ConnectionReset

		let mut stream = BufferedStream::from_rx(rx);
		let mut out = vec![0u8; 11];
		stream.read_exact(&mut out).unwrap();
		assert_eq!(&out, b"hello world");

		// Next read hits the empty-channel ConnectionReset path.
		let mut tail = [0u8; 1];
		let err = stream.read(&mut tail).unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
	}

	#[test]
	fn buffered_stream_single_byte_read_uses_scalar_copy_path() {
		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		tx.send(vec![0xAB, 0xCD]).unwrap();
		let mut stream = BufferedStream::from_rx(rx);
		let mut one = [0u8; 1];
		stream.read_exact(&mut one).unwrap();
		assert_eq!(one[0], 0xAB);
	}

	#[test]
	fn buffered_stream_drains_consumed_prefix_past_threshold() {
		// Feed > CLEAR_CONSUMED_AT (1024) bytes, read them all, then
		// feed more to trigger the drain branch inside fill_buf.
		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		let big = vec![0xEEu8; 2048];
		tx.send(big.clone()).unwrap();
		tx.send(vec![0xFFu8; 32]).unwrap();
		drop(tx);

		let mut stream = BufferedStream::from_rx(rx);
		let mut sink = vec![0u8; 2048];
		stream.read_exact(&mut sink).unwrap();
		assert!(sink.iter().all(|&b| b == 0xEE));

		// Triggering fill_buf after consuming >1024 bytes forces the
		// drain branch that clears `consumed`.
		let mut tail = vec![0u8; 32];
		stream.read_exact(&mut tail).unwrap();
		assert!(tail.iter().all(|&b| b == 0xFF));
	}

	#[test]
	fn buffered_stream_consume_panics_past_buffer_len() {
		let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		tx.send(vec![1u8, 2, 3]).unwrap();
		let mut stream = BufferedStream::from_rx(rx);
		// Prime the buffer via fill_buf.
		let _ = stream.fill_buf().unwrap();

		let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			<BufferedStream as BufRead>::consume(&mut stream, 999);
		}));
		assert!(panicked.is_err(), "consume past buffer len must panic");
	}

	/// Each public talk method gates on the `two_way_audio` ability;
	/// a fresh camera with no abilities populated must surface the
	/// gate as `MissingAbility` (CLI exit code 6) rather than the
	/// generic protocol error a non-talk camera would otherwise
	/// reply with.
	#[tokio::test]
	async fn talk_methods_gate_on_two_way_audio_ability() {
		// All four entrypoints share the same gate.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert!(matches!(
			cam.talk_stop().await.unwrap_err(),
			Error::MissingAbility { .. }
		));
		assert!(matches!(
			cam.talk_ability().await.unwrap_err(),
			Error::MissingAbility { .. }
		));
		// `talk` and `talk_stream` need TalkConfig args; a default-shaped
		// one suffices because we never reach the body — the gate fires
		// first.
		let cfg = TalkConfig::default();
		assert!(matches!(
			cam.talk(&[], cfg.clone()).await.unwrap_err(),
			Error::MissingAbility { .. }
		));
		let (_tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
		assert!(matches!(
			cam.talk_stream(rx, cfg).await.unwrap_err(),
			Error::MissingAbility { .. }
		));
	}
}
