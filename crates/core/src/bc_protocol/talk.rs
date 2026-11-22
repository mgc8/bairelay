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
		// The msg 11 sent above is what releases the doorbell's talk slot;
		// whether this firmware answers it is another matter — the same battery
		// doorbell never answers msg 202 either. Waiting without a bound here
		// meant that on any firmware staying silent the caller never returned,
		// so every cleanup placed after `talk_stop` was skipped and the camera
		// kept holding the session (observed: a talk slot still occupied eight
		// minutes after the caller had hung up). Wait briefly for the 200 and
		// treat silence as the release it asked for.
		let reply = tokio::time::timeout(std::time::Duration::from_millis(1500), sub.recv()).await;
		let msg = match reply {
			Ok(msg) => msg?,
			Err(_elapsed) => {
				// Silent firmware: the 11 went out, which is the part that
				// mattered. Let the caller finish its cleanup.
				log::debug!("talk stop: no reply to msg 11 within 1.5 s, assuming the camera let go");
				return Ok(());
			}
		};

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
	/// Diagnostic TalkAbility probe: sends the very same
	/// [`MSG_ID_TALKABILITY`] request as [`Self::talk_ability`] but does
	/// **not** consult the ability table first.
	///
	/// This exists for exactly one purpose. `AbilityInfo` is known to
	/// under-report what a box actually answers — a `<HaCfg>` read returning
	/// `405` on firmware that happily accepts `<Email>` is the in-tree
	/// example — so on a camera whose ability list carries no
	/// `two_way_audio` the gated [`Self::talk_ability`] can never tell
	/// "no talk hardware" apart from "talk present, not advertised". The
	/// only way to tell them apart is to send the read and look at the raw
	/// reply, which is what this does.
	///
	/// Deliberately narrow:
	///
	/// * The gate in [`Self::talk_ability`], [`Self::talk`],
	///   [`Self::talk_stream`] and [`Self::talk_stop`] is untouched. Normal
	///   callers keep getting [`Error::MissingAbility`] without spending a
	///   round trip; only this diagnostic bypasses it.
	/// * It sends msg id 10 and nothing else — no `TalkConfig` (201), no
	///   audio (202), no talk reset (11). Read-only, like `abilityInfo`.
	/// * Nothing is swallowed: the reply's header `response_code` is handed
	///   back verbatim so the operator can see `405` / `400` / `482` /
	///   whatever this firmware uses, rather than a normalised error.
	///
	/// Errors from the transport (connect, subscribe, send, recv, timeout)
	/// propagate as `Err`; that is the "no answer at all" case, kept
	/// separate from "answered with an error code" by the [`Ok`] variants.
	///
	pub async fn talk_ability_probe(&self) -> Result<TalkAbilityProbe> {
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

		let code = msg.meta.response_code;
		// Describe the payload before consuming `msg.body` so the
		// "answered, but not with TalkAbility" case can say what did come
		// back instead of just "nothing".
		let payload = describe_payload(&msg.body);

		match msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload:
					Some(BcPayloads::BcXml(BcXml {
						talk_ability: Some(ability),
						..
					})),
				..
			}) => Ok(TalkAbilityProbe::Xml { code, ability }),
			_ if code != 200 => Ok(TalkAbilityProbe::ErrorCode { code, payload }),
			_ => Ok(TalkAbilityProbe::NoXml { code, payload }),
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

		// Pace against one absolute deadline, never a relative sleep per payload.
		//
		// Measured on this machine with `bairelay audio-check --bench 51` (no
		// camera, no network): a relative `sleep(64 ms)` lands 9.3 ms late on
		// average (worst 16.8 ms) because Windows resolves sleep timers on its
		// ~15.6 ms system tick — and a *relative* sleep keeps that lateness, so
		// 51 payloads drift +430 to +500 ms and 200 of them +1862 ms. The same
		// steps as deadlines computed from one start instant drift +2.8 ms over
		// the whole 200 steps. A sender that runs 13 % slow starves the camera's
		// playback buffer, which is an audible artifact all by itself, and it
		// stretches a melody: 3.26 s of audio went out in 3.71 s on hardware.
		let mut next_deadline = std::time::Instant::now();

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
			// Deadline pacing, see `next_deadline` above: the wait is whatever is
			// left until the schedule says this payload should be over, so a
			// timer that fires 9 ms late is paid back by the *next* payload's
			// wait instead of being added to the stream forever.
			let play_duration = std::time::Duration::from_secs_f32(play_length);
			let now = std::time::Instant::now();
			if now.saturating_duration_since(next_deadline) > play_duration {
				// Behind by more than this payload's own playback time: the
				// camera has already run dry, so this audio can no longer be
				// heard on time. Re-anchor the schedule here rather than try to
				// catch up on sound that is already late — but only here, so a
				// single timer hiccup never moves the timeline.
				next_deadline = now + play_duration;
			} else {
				next_deadline += play_duration;
				let now = std::time::Instant::now();
				if next_deadline > now {
					tokio::time::sleep(next_deadline.saturating_duration_since(now)).await;
				}
			}
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
			// Keep the timeline unless we are behind by more than a whole block.
			//
			// This branch used to fire on *any* lateness (`time_sent >
			// expected_stream_end`), which turns the system's sleep granularity
			// into permanent drift: every wait fires ~9 ms late on Windows
			// (measured with `bairelay audio-check --bench 51`: mean +9.3 ms,
			// worst +16.8 ms, and a 15.6 ms system tick explains it), each late
			// send then pushed `expected_stream_end` to `time_sent + play_length`,
			// and the lateness was never paid back — 51 blocks of a 3.26 s stream
			// left the sender ~0.45 s behind the audio, which is what the operator
			// measured as "51 blocs en 3,71 s". Advancing the anchor by
			// `play_length` instead lets the next wait be shorter by exactly that
			// lateness, so the stream ends when the audio says it should.
			// Re-anchoring is still right when the producer stalled for more than
			// a block: that audio cannot be heard on time any more, and pushing
			// the schedule avoids bursting the rest of the stream at the camera.
			if time_sent.saturating_duration_since(expected_stream_end) > play_length {
				expected_stream_end = time_sent + play_length;
			} else {
				expected_stream_end += play_length;
			}
			// Pace on the camera's per-payload ack *when it sends one*, but never
			// block on it unconditionally: this used to be `sub.recv().await?`,
			// which hangs forever on firmware that accepts the TalkConfig (200 to
			// msg 201) and then stays silent about every msg 202 — measured on a
			// battery doorbell with 32 blocks written, no ack ever arriving, so
			// the flush and the `talk_stop()` below were never reached and the
			// talk session was left open for the camera to time out on its own.
			// `talk()` never had that problem because it sleeps out the playback
			// duration instead of waiting for feedback (`talk()` is the path the
			// same camera answers).
			//
			// The window is exactly the playback time still scheduled for what we
			// have already sent, so an acking camera still drives the loop at its
			// own pace (unchanged behaviour there), and a silent camera is paced at
			// wall-clock speed, which is what this loop was always meant to do.
			// Dropping the pending `recv()` on timeout is safe: it is an mpsc
			// receiver, so a cancelled receive loses nothing but the wait.
			// The clock is the only authority on *when* the next block may go
			// out. An ack may tell us the camera is unhappy, it may never come,
			// but it must never set the pace: 1025 samples at 16 kHz take 64 ms
			// to play on this doorbell's speaker no matter how fast its firmware
			// answers.
			//
			// Measured on the battery doorbell with the bridge's per-second
			// probe: answering the ack and moving on immediately drained the
			// producer's queue at ~20.6 blocks per second while the microphone
			// produced 15.6 — the camera was fed 1.32x as much audio as it could
			// play, its buffer ran dry about six seconds in, and the loudspeaker
			// went silent mid-call with the session still open. The same loop
			// under WebRTC died at 14 to 28 s, which is the same starvation with
			// a different amount of pre-buffered audio in front of it.
			let deadline = tokio::time::Instant::from_std(expected_stream_end);
			tokio::select! {
				reply = sub.recv() => {
					reply?;
				}
				_ = tokio::time::sleep_until(deadline) => {}
			}
			// A reply that arrived early says nothing about playback: still wait
			// out the block's own playback time before sending the next one.
			let maintenant = tokio::time::Instant::now();
			if deadline > maintenant {
				tokio::time::sleep_until(deadline).await;
			}
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

/// What an ungated [`BcCamera::talk_ability_probe`] got back.
///
/// The three `Ok` shapes are deliberately kept apart from the `Err` case:
/// "the camera refused", "the camera answered with something else" and "we
/// never got an answer" are three different diagnoses, and collapsing them
/// into one error is what made the gated path useless for this question.
#[derive(Debug, PartialEq, Eq)]
pub enum TalkAbilityProbe {
	/// The reply carried a `<TalkAbility>` element. `code` is the header
	/// `response_code` (200 on the happy path — a non-200 that still
	/// carries the XML is reported here rather than as a refusal, because
	/// the payload is what the operator came for).
	Xml {
		/// Header response code, verbatim.
		code: u16,
		/// The parsed element.
		ability: TalkAbility,
	},
	/// A non-200 reply that carried no `<TalkAbility>`, i.e. the camera
	/// refused the command. `code` is the raw refusal (`405` is "unknown
	/// command" on this family, `400` bad request, `482` appears on other
	/// firmware) — deliberately not mapped onto an enum of known codes.
	ErrorCode {
		/// Header response code, verbatim.
		code: u16,
		/// What the payload did contain (`"none"`, `"xml"`,
		/// `"binary(N bytes)"`, `"legacy"`).
		payload: String,
	},
	/// A 200 reply with no `<TalkAbility>` element: an ack-only reply
	/// (`payload: "none"`) or an XML body that simply lacks the element
	/// (`payload: "xml"`).
	NoXml {
		/// Header response code, verbatim (200 by construction).
		code: u16,
		/// What the payload did contain.
		payload: String,
	},
}

/// One-line description of a reply body's payload, for
/// [`TalkAbilityProbe::ErrorCode`] / [`TalkAbilityProbe::NoXml`]. Borrows,
/// so it can run before the body is destructured.
fn describe_payload(body: &BcBody) -> String {
	match body {
		BcBody::LegacyMsg(_) => "legacy".to_string(),
		BcBody::ModernMsg(ModernMsg { payload: None, .. }) => "none".to_string(),
		BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::BcXml(_)),
			..
		}) => "xml".to_string(),
		BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::Binary(b)),
			..
		}) => format!("binary({} bytes)", b.len()),
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
			// A bounded wait, not `recv()`. This is a blocking crossbeam receive
			// called from an `async fn`, so while it waits it parks a runtime
			// worker and cannot be interrupted by a timeout or a cancellation
			// token. Measured on the battery doorbell: the microphone was 4 %
			// short of real time, the queue emptied nine seconds in, and this
			// line held the whole talk task — including the caller's own select
			// loop and the stdin reader behind it — until the process was
			// killed, so msg 11 was never sent. A producer that has nothing to
			// say for this long has stopped saying anything: return an error,
			// which the caller already treats as end of stream, and the session
			// closes properly.
			let data = self
				.rx
				.recv_timeout(std::time::Duration::from_secs(3))
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
								duplex: vec!["FDX".to_string()],
							}],
							audio_stream_mode_list: vec![AudioStreamModeList {
								audio_stream_mode: vec!["followVideoStream".to_string()],
							}],
							audio_config_list: vec![AudioConfigList {
								audio_config: vec![ok_talk_config().audio_config],
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
		// Each wrapper holds a collection now, so a `<duplexList>` carrying a
		// single `<duplex>` still reads back as exactly that one value.
		assert_eq!(ability.duplex_list[0].duplex, vec!["FDX".to_string()]);
		assert_eq!(
			ability.audio_stream_mode_list[0].audio_stream_mode,
			vec!["followVideoStream".to_string()]
		);
		assert_eq!(ability.audio_config_list[0].audio_config.len(), 1);
		assert_eq!(ability.audio_config_list[0].audio_config[0].audio_type, "adpcm");
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
