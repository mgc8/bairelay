//! `audio-bridge` — the live bridge between Home Assistant and the doorbell.
//!
//! # What this is
//!
//! go2rtc (Home Assistant add-on) pipes the browser microphone into this
//! process's **stdin** as headerless signed 16-bit little-endian PCM at
//! 16000 Hz mono — its `exec` source with `#backchannel=1#audio=s16le/16000` —
//! and reads the camera's own audio back out of the RTSP stream it is already
//! pulling, which is why nothing here sends audio *up* the pipe: the return
//! path is the camera's regular stream, and this protocol has no file upload at
//! all. So this command has exactly one job: turn the bytes on stdin into the
//! camera's ADPCM blocks and write them to its loudspeaker, for as long as the
//! caller speaks.
//!
//! # Session lifecycle (the part that can wedge a doorbell)
//!
//! One BC login for the whole process — reconnecting costs 2–4 s and would sit
//! in the middle of a sentence. Talk sessions, however, are opened **per speech
//! burst**: `TalkConfig` (msg 201) at the first byte, audio blocks (msg 202) as
//! they arrive, `talk_reset` (msg 11) when the caller stops. The reason is
//! orphan avoidance. A process that is SIGKILLed — go2rtc recycling the
//! source, the OOM killer, a container restart — cannot send msg 11 *ever*, so
//! a session left open across silence is an orphan the camera holds open on its
//! own timer, and while it holds it the doorbell is deaf to its own microphone
//! and the talk slot is taken. Keeping a session open across silence buys a few
//! hundred milliseconds on the next utterance and risks exactly the state we
//! cannot clear from here; closing it on every gap risks nothing. That is the
//! whole trade, and it is why `--idle` defaults to 2 s.
//!
//! On stdin EOF (go2rtc ending the call is *normal*, not an error) and on
//! Ctrl+C the sender is dropped, which is the end-of-stream signal
//! [`BcCamera::talk_stream`] understands: it flushes what is queued, waits out
//! the audio still scheduled to play, and sends msg 11 itself. Ctrl+C therefore
//! costs one graceful close, not an orphan — provided the signal reaches us
//! (a `SIGKILL` is the case no program can handle, which is the other half of
//! the reasoning above).
//!
//! # Latency and memory
//!
//! Blocks are produced as stdin delivers them, so latency is one block
//! (64 ms at this camera's `lengthPerEncoder = 1024`). If the camera stops
//! draining its TCP socket, the queue is capped at ~10 s of audio and the burst
//! is cut rather than letting an unbounded backlog grow in a process that is
//! meant to survive for hours; stale audio in a live intercom is worthless, and
//! a stalled camera deserves a closed session, not a memory leak.

use std::io::Read;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver};
use bairelay_neolink_core::crossbeam_channel::Sender;
use bairelay_rtsp::transcode::adpcm::{AdpcmEncoder, HeaderConvention};
use tokio_util::sync::CancellationToken;

use super::input::Input;
use super::output::Outcome;
use super::talk::{self, Negotiation};

/// Read granularity of the stdin thread. 4096 bytes is 128 ms of audio: large
/// enough that the thread is never hot, small enough that it never adds
/// perceptible delay (the encoder's own boundary is 64 ms).
const STDIN_CHUNK_BYTES: usize = 4096;
/// Chunks held between the reader thread and the pump. 64 × 4 KiB of μ-law is
/// 32 seconds of speech — far more than the engine's own pipe can hold anyway,
/// so the bound only ever bites when the camera has stopped draining.
const STDIN_QUEUE_CHUNKS: usize = 64;

/// How much encoded audio may sit in the queue towards the camera before the
/// burst is cut. 10 s of live intercom is already useless; the cap exists to
/// bound memory if the socket stalls, not to absorb jitter.
const MAX_QUEUED_SECONDS: f64 = 10.0;

/// How much microphone may lead the camera at the start of a burst. Opening a
/// talk session takes ~3 s, so without this the call would begin with 3 s of
/// audio nobody can still react to and carry 3 s of delay to every word after
/// it. 300 ms is the usual intercom compromise: enough to absorb a scheduler
/// hiccup, short enough that the answer still feels live.
const PRE_ROLL: f64 = 0.3;

/// How long to wait for `talk_stream` to flush and send msg 11 after the
/// sender is dropped, before giving up on the session and leaving it to the
/// camera's own timeout.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Backoff between retries after a session fails, so a refusing camera does not
/// turn the bridge into a hot loop of 201s.
const FAIL_BACKOFF: Duration = Duration::from_millis(250);

/// Give up after this many consecutive sessions that failed on their own.
const MAX_CONSECUTIVE_FAILURES: u32 = 5;

/// Process budget. Finite on purpose: `tokio::time::timeout` is not meant for
/// `Duration::MAX`, and a bridge that ignored its cancellation token would
/// otherwise run forever. A week is far beyond any single Home Assistant call,
/// and go2rtc closes stdin (→ EOF → clean exit) when a call ends anyway.
const OP_BUDGET: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Default silence that ends a talk session.
pub const DEFAULT_IDLE: Duration = Duration::from_secs(2);

/// Op deadline for the bridge (see [`OP_BUDGET`]).
pub fn op_timeout() -> Duration {
	OP_BUDGET
}

/// Why the bridge exited. `eof` is the normal end of a call, not a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndReason {
	/// stdin closed (or was never given anything).
	Eof,
	/// Ctrl+C / SIGTERM: the session was closed before returning.
	Interrupted,
	/// Too many sessions failed back to back.
	Failing,
}

impl EndReason {
	fn label(&self) -> &'static str {
		match self {
			EndReason::Eof => "eof",
			EndReason::Interrupted => "interrupted",
			EndReason::Failing => "repeated-failures",
		}
	}
}

/// Why one burst ended.
#[derive(Debug)]
enum Stop {
	/// No more bytes, ever.
	Eof,
	/// `--idle` elapsed with nothing to send.
	Idle,
	/// Ctrl+C during a burst.
	Cancelled,
	/// The queue towards the camera stopped draining.
	Stalled,
	/// `talk_stream` returned by itself: a protocol error, or a camera that
	/// never acks a 202. Carries the rendered cause.
	SessionEnded(Result<(), String>),
}

/// PCM bytes → whole ADPCM blocks, carrying state across chunks and bursts.
struct Pump {
	encoder: AdpcmEncoder,
	block_bytes: usize,
	/// Low byte of a sample split across two stdin reads.
	pending_byte: Option<u8>,
	samples: Vec<i16>,
	/// Encoded bytes not yet handed to the session.
	out: Vec<u8>,
	blocks: usize,
	bytes_in: usize,
}

/// The sender went away: the talk session is over, nothing to push to.
/// `Debug` so tests can `unwrap()` a `Result<_, SendLost>`.
#[derive(Debug, Clone, Copy)]
struct SendLost;

impl Pump {
	fn new(neg: &Negotiation) -> Self {
		Self {
			encoder: neg.encoder(),
			block_bytes: neg.block_bytes,
			pending_byte: None,
			samples: Vec::with_capacity(neg.block_samples * 4),
			out: Vec::with_capacity(neg.block_bytes * 8),
			blocks: 0,
			bytes_in: 0,
		}
	}

	/// Called at the start of every burst: each talk session begins from a
	/// silent predictor, exactly like the first block of a fresh session.
	fn start_burst(&mut self) {
		self.encoder.reset();
		self.out.clear();
		self.pending_byte = None;
	}

	/// Called at the end of a session that already went away: drop whatever was
	/// half-encoded, keep the counters.
	fn reset_burst(&mut self) {
		self.start_burst();
	}

	/// Absorb one stdin chunk, pushing every completed block to `tx`.
	fn feed(&mut self, chunk: &[u8], tx: &Sender<Vec<u8>>) -> Result<(), SendLost> {
		self.bytes_in += chunk.len();
		self.samples.clear();
		let mut rest = chunk;
		if let Some(lo) = self.pending_byte.take() {
			let Some((&first, tail)) = chunk.split_first() else {
				self.pending_byte = Some(lo);
				return Ok(());
			};
			self.samples.push(i16::from_le_bytes([lo, first]));
			rest = tail;
		}
		let mut pairs = rest.chunks_exact(2);
		self.samples
			.extend(pairs.by_ref().map(|p| i16::from_le_bytes([p[0], p[1]])));
		if let Some(&tail_byte) = pairs.remainder().first() {
			self.pending_byte = Some(tail_byte);
		}
		self.encoder.encode(&self.samples, &mut self.out);
		self.drain(tx)
	}

	/// Pad the last partial block (so the tail of a sentence is not lost),
	/// push it, then reset for the next session.
	fn finish_burst(&mut self, tx: &Sender<Vec<u8>>) {
		self.encoder.finish(&mut self.out);
		let _ = self.drain(tx);
		self.start_burst();
	}

	fn drain(&mut self, tx: &Sender<Vec<u8>>) -> Result<(), SendLost> {
		while self.out.len() >= self.block_bytes {
			let block: Vec<u8> = self.out.drain(..self.block_bytes).collect();
			tx.send(block).map_err(|_| SendLost)?;
			self.blocks += 1;
		}
		Ok(())
	}
}

/// Raise (or lower) the level of little-endian s16 samples in place, before the
/// ADPCM encoder sees them. A browser microphone usually delivers a good deal
/// less than full scale and the doorbell's amplifier is small, so the bridge is
/// audibly quiet untouched. Clamped, never wrapped: a wrapped peak turns into a
/// crack, and we have spent long enough on cracks today.
fn apply_gain(bytes: &mut [u8], gain: f32) {
	if gain == 1.0 {
		return;
	}
	for pair in bytes.chunks_exact_mut(2) {
		let sample = i16::from_le_bytes([pair[0], pair[1]]) as f32 * gain;
		let clamped = sample.clamp(-32_768.0, 32_767.0) as i16;
		pair.copy_from_slice(&clamped.to_le_bytes());
	}
}

/// Outcome for a bridge that never reached the camera: nothing was negotiated,
/// so every field the camera would have reported stays at zero rather than
/// claiming a format the session never had.
fn no_session(idle: Duration, reason: EndReason) -> Outcome {
	tracing::info!("audio-bridge: no microphone bytes, camera never contacted ({})", reason.label());
	Outcome::AudioBridge {
		bursts: 0,
		blocks: 0,
		bytes_in: 0,
		wall_seconds: 0.0,
		reason: reason.label().to_string(),
		audio_type: String::new(),
		sample_rate: 0,
		block_bytes: 0,
		idle_secs: idle.as_secs_f64(),
	}
}

/// Bridge stdin to the camera's loudspeaker until EOF or Ctrl+C.
///
/// `input` says what the pipe actually carries. go2rtc writes the browser's
/// negotiated payload verbatim (`pkg/pcm/backchannel.go`), and a browser
/// negotiates G.711 at 8 kHz, so the widening to the camera's talk rate happens
/// here, in the reader thread, before any block accounting — everything
/// downstream of the converter still sees s16le at the negotiated rate.
pub async fn run(
	cam: &BcCamera,
	channel_id: u8,
	idle: Duration,
	convention: HeaderConvention,
	input: Input,
	gain: f32,
	cancel: CancellationToken,
) -> Result<Outcome> {
	// stdin on its own OS thread, and started *before* anything is negotiated:
	// the pipe go2rtc writes into holds about a second of G.711, and a camera
	// that has to wake answers slower than that. Reading only after `talk_start`
	// would leave go2rtc blocked on its write — the first words of the call lost,
	// the browser's audio track stalled. The queue is bounded on purpose: once it
	// is full the reader blocks, the pipe fills, and the engine drops audio. A
	// live intercom has no use for a backlog of stale speech.
	let (tx_in, mut rx_in) = tokio::sync::mpsc::channel::<Vec<u8>>(STDIN_QUEUE_CHUNKS);
	std::thread::spawn(move || {
		let mut stdin = std::io::stdin();
		let mut buf = vec![0u8; STDIN_CHUNK_BYTES];
		loop {
			match stdin.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					if tx_in.blocking_send(buf[..n].to_vec()).is_err() {
						break;
					}
				}
				Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
				Err(_) => break,
			}
		}
	});

	// Wait until someone actually speaks before touching the camera. go2rtc
	// starts this process the moment a WebRTC page opens, and a page left open
	// with nobody talking must not wake the doorbell, let alone hold a talk
	// session that occupies the camera's audio slot. The reader thread keeps
	// draining the pipe into a bounded queue meanwhile, so negotiating *after*
	// the first bytes costs nothing: the queue absorbs the whole handshake.
	let first_bytes = tokio::select! {
		biased;
		_ = cancel.cancelled() => return Ok(no_session(idle, EndReason::Interrupted)),
		chunk = rx_in.recv() => match chunk {
			Some(c) => c,
			None => return Ok(no_session(idle, EndReason::Eof)),
		},
	};

	let mut neg = talk::negotiate(cam, channel_id).await?;
	// Same single wire-format knob as `talk --header`.
	neg.convention = convention;
	// A gap shorter than two blocks would close the session between two
	// consecutive blocks of the *same* sentence, so refuse it rather than
	// pretend the knob is free.
	let min_idle = Duration::from_millis(u64::from(neg.block_millis) * 2);
	if idle < min_idle {
		return Err(anyhow!(
			"--idle {} ms is shorter than two audio blocks ({} ms each at this \
			 camera); the session would close between blocks of one sentence",
			idle.as_millis(),
			neg.block_millis
		));
	}
	let max_queued_blocks = ((MAX_QUEUED_SECONDS * f64::from(neg.sample_rate)
		/ neg.block_samples as f64)
		.ceil() as usize)
		.max(1);

	// Bytes read above are still in the engine's own format. The camera reports
	// its talk rate in a u16 (65535 Hz is the protocol ceiling). Widening here,
	// in this single task, keeps every stage below the converter in the camera's
	// terms and leaves the interpolator's history in one owner.
	let hz_camera = u32::from(neg.sample_rate);
	let mut converter = input.converter(hz_camera)?;
	tracing::info!("audio-bridge: stdin {}", converter.describe());

	let started = Instant::now();
	let mut pump = Pump::new(&neg);
	let mut bursts = 0usize;
	let mut failures = 0u32;
	// The bytes that woke the camera are the first bytes of burst 1.
	let mut pending = Some(first_bytes);

	let reason: EndReason = loop {
		// ── wait for the first byte of a new burst ───────────────────────────
		// No idle timer here: waiting for a caller to speak is the steady state
		// of this process and costs nothing.
		let mut raw = match pending.take() {
			Some(deja_la) => deja_la,
			None => {
				tokio::select! {
					biased;
					_ = cancel.cancelled() => break EndReason::Interrupted,
					chunk = rx_in.recv() => match chunk {
						Some(c) => c,
						None => break EndReason::Eof,
					},
				}
			}
		};
		// Swallow whatever else already piled up while the session was being
		// opened. Discovery, login and `<TalkConfig>` take about three seconds
		// and the relay keeps feeding stdin the whole time, so several seconds of
		// microphone are waiting here on every single call.
		while let Ok(stale) = rx_in.try_recv() {
			raw.extend_from_slice(&stale);
		}
		let mut first = converter.convert(&raw);
		// An intercom is live or it is useless. Playing three seconds of stale
		// microphone first adds exactly that much delay to *every* word for the
		// rest of the call (the queue never catches up: the camera consumes at
		// the same rate the microphone produces), and nobody is listening to the
		// part that was spoken before the loudspeaker could answer. Keep the
		// last PRE_ROLL seconds, drop the rest, on a sample boundary.
		let keep = ((PRE_ROLL * f64::from(neg.sample_rate) * 2.0) as usize) & !1;
		if first.len() > keep {
			let skip = (first.len() - keep) & !1;
			tracing::info!(
				"audio-bridge: {:.2} s of microphone buffered while the session opened, \
				 dropping the first {:.2} s",
				first.len() as f64 / (f64::from(neg.sample_rate) * 2.0),
				skip as f64 / (f64::from(neg.sample_rate) * 2.0).max(1.0)
			);
			first = first.split_off(skip);
		}
		apply_gain(&mut first, gain);
		if first.is_empty() {
			// A single stray byte (half a sample) can arrive; the interpolator
			// keeps nothing from it. Keep waiting, do not open a burst for it.
			continue;
		}

		bursts += 1;
		pump.start_burst();
		// `talk_stream` blocks on this channel while the queue is empty, so an
		// empty queue stalls *this* task, never the producer (a separate OS
		// thread) and never another worker (multi-thread runtime).
		let (tx, rx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		let stream = cam.talk_stream(rx, neg.talk_config.clone());
		tokio::pin!(stream);
		tracing::debug!("audio-bridge: burst {bursts} open ({} B blocks)", neg.block_bytes);

		let mut feed = first;
		// One line per second: what the bridge produced, what the camera still
		// has to send, and what is waiting. A talk session that stops says
		// nothing, returns nothing, closes nothing — it falls silent, and the
		// only way to tell who went quiet is to watch the rhythm. Without this
		// line, "audio cuts out after ten seconds" is indistinguishable from
		// "the microphone stopped delivering".
		let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
		heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut blocks_seen = pump.blocks;
		let mut bytes_seen = pump.bytes_in;
		let loop_start = Instant::now();
		let stop: Stop = loop {
			if let Err(SendLost) = pump.feed(&feed, &tx) {
				break Stop::SessionEnded(Err(
					"the talk session closed while audio was being fed".to_string(),
				));
			}
			if tx.len() > max_queued_blocks {
				tracing::warn!(
					"audio-bridge: {} blocks queued (>{max_queued_blocks}, ~{MAX_QUEUED_SECONDS:.0} s): \
					 the camera stopped draining, closing this burst",
					tx.len()
				);
				break Stop::Stalled;
			}
			feed = tokio::select! {
				biased;
				res = &mut stream => break Stop::SessionEnded(res.map_err(|e| e.to_string())),
				_ = cancel.cancelled() => break Stop::Cancelled,
				chunk = rx_in.recv() => match chunk {
					Some(c) => {
						let mut bytes = converter.convert(&c);
						apply_gain(&mut bytes, gain);
						bytes
					}
					None => break Stop::Eof,
				},
				_ = heartbeat.tick() => {
					// Empty chunk on purpose: feeding nothing keeps the encoder
					// state (a pending half-sample survives) while `continue`
					// would have re-sent the previous chunk to the camera.
					let blocks_now = pump.blocks;
					let bytes_now = pump.bytes_in;
					tracing::info!(
						"audio-bridge: +{:.0} blocks (= {:.2} s of speech) in the last second, \
						 {} queued for the camera, +{:.0} bytes read, {:.0} s into the session",
						blocks_now - blocks_seen,
						f64::from((blocks_now * neg.block_samples as usize) as u32)
							/ f64::from(neg.sample_rate.max(1)),
						tx.len(),
						bytes_now - bytes_seen,
						loop_start.elapsed().as_secs_f32(),
					);
					blocks_seen = blocks_now;
					bytes_seen = bytes_now;
					Vec::new()
				},
				_ = tokio::time::sleep(idle) => break Stop::Idle,
			};
		};

		// Close the session unless its own future already finished. Dropping the
		// sender *is* the end-of-stream signal: `talk_stream` flushes, waits out
		// the playback it scheduled, then sends msg 11.
		let exit: Option<EndReason> = match stop {
			Stop::SessionEnded(Err(msg)) => {
				// The session is already gone; there is nothing to flush to.
				pump.reset_burst();
				failures += 1;
				tracing::warn!("audio-bridge: talk session {bursts} ended: {msg}");
				if bursts == 1 && pump.blocks == 0 {
					return Err(anyhow!(
						"the first talk session failed before any audio reached the camera: {msg}"
					));
				}
				if failures >= MAX_CONSECUTIVE_FAILURES {
					Some(EndReason::Failing)
				} else {
					tokio::time::sleep(FAIL_BACKOFF).await;
					None
				}
			}
			Stop::SessionEnded(Ok(())) => {
				// Only reachable if the queue was closed from under us; treat it
				// as an ordinary burst end rather than a panic-worthy state.
				pump.reset_burst();
				failures = 0;
				None
			}
			other => {
				// Pad the tail so the end of the sentence is not lost, then let
				// the session see end-of-stream and give it time to say msg 11.
				pump.finish_burst(&tx);
				drop(tx);
				match tokio::time::timeout(SHUTDOWN_GRACE, &mut stream).await {
					Ok(Ok(())) => failures = 0,
					Ok(Err(e)) => {
						failures += 1;
						tracing::warn!("audio-bridge: closing talk session {bursts} failed: {e}");
					}
					Err(_) => {
						tracing::warn!(
							"audio-bridge: talk session {bursts} did not close within {} s; \
							 sending the reset (msg 11) directly so the camera is not left \
							 holding the talk slot",
							SHUTDOWN_GRACE.as_secs()
						);
						// The session future is stuck on something inside itself, so
						// dropping it is what stops the send loop — but dropping it
						// says nothing to the camera. Send msg 11 ourselves: a
						// duplicate reset is harmless, and the alternative is an
						// orphaned session that only the camera's own timeout can
						// clear.
						if let Err(e) = cam.talk_stop().await {
							failures += 1;
							tracing::warn!("audio-bridge: direct talk_stop failed: {e}");
						}
					}
				}
				if failures >= MAX_CONSECUTIVE_FAILURES {
					return Err(anyhow!(
						"{failures} talk sessions in a row could not be closed cleanly"
					));
				}
				match other {
					Stop::Eof => Some(EndReason::Eof),
					Stop::Cancelled => Some(EndReason::Interrupted),
					Stop::Idle | Stop::Stalled => None,
					Stop::SessionEnded(_) => None,
				}
			}
		};
		if let Some(reason) = exit {
			break reason;
		}
	};

	tracing::info!(
		"audio-bridge: stopping ({}) — {bursts} talk sessions, {} blocks, {} bytes in, {:.1} s",
		reason.label(),
		pump.blocks,
		pump.bytes_in,
		started.elapsed().as_secs_f64(),
	);
	Ok(Outcome::AudioBridge {
		bursts,
		blocks: pump.blocks,
		bytes_in: pump.bytes_in,
		wall_seconds: started.elapsed().as_secs_f64(),
		reason: reason.label().to_string(),
		audio_type: neg.audio_type.clone(),
		sample_rate: neg.sample_rate,
		block_bytes: neg.block_bytes,
		idle_secs: idle.as_secs_f64(),
	})
}

/// Trait-object route, mirroring [`crate::oneshot::talk_abilite`] and
/// [`super::talk`]: no talk surface exists on [`CameraDriver`].
pub async fn run_via_driver(_cam: &dyn CameraDriver) -> Result<Outcome> {
	anyhow::bail!("audio-bridge requires a concrete BcCamera; not supported through CameraDriver");
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

	fn neg() -> Negotiation {
		Negotiation {
			ability_code: 200,
			talk_config: Default::default(),
			block_bytes: 516,
			block_samples: 1025,
			block_millis: 64,
			audio_type: "adpcm".into(),
			sample_rate: 16_000,
			sample_precision: 16,
			sound_track: "mono".into(),
			length_per_encoder: 1024,
			duplex: "FDX".into(),
			convention: HeaderConvention::default(),
		}
	}

	/// A chunk boundary in the middle of a sample must not lose or duplicate a
	/// sample: the bridge gets whatever the pipe decided to deliver.
	#[test]
	fn pump_carries_an_odd_byte_across_chunks() {
		let mut pump = Pump::new(&neg());
		let (tx, rx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		// One byte short of a whole block: a block is 1025 samples (the header
		// sample plus 1024 nibbles), so 2050 bytes.
		let chunk: Vec<u8> = (0..2049).map(|i| (i % 251) as u8).collect();
		pump.feed(&chunk, &tx).expect("sender alive");
		assert_eq!(pump.pending_byte, Some(chunk[2048]));
		assert_eq!(pump.bytes_in, 2049);
		assert_eq!(rx.len(), 0, "1024 samples < one 1025-sample block");

		pump.feed(&[0x00], &tx).expect("sender alive");
		assert_eq!(pump.pending_byte, None);
		assert_eq!(pump.bytes_in, 2050, "the carried byte completed the sample");
		assert_eq!(rx.len(), 1, "2050 bytes = 1025 samples = exactly one block");
	}

	/// Nothing reaches the camera until a whole block exists, and the tail must
	/// be padded out at the end of the burst rather than dropped.
	#[test]
	fn pump_emits_whole_blocks_and_pads_the_tail() {
		let mut pump = Pump::new(&neg());
		let (tx, rx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		pump.feed(&[0x10; 2000], &tx).unwrap();
		assert_eq!(rx.len(), 0, "1000 samples < one 1025-sample block");
		pump.feed(&[0x10; 2200], &tx).unwrap();
		assert_eq!(rx.len(), 2, "2100 samples = 2 blocks, 50 left over");
		// Collect before checking lengths: iterating the receiver consumes
		// what it walks over, and `rx.len()` afterwards would only see the
		// blocks that the iteration left behind.
		let blocks: Vec<Vec<u8>> = rx.try_iter().collect();
		assert!(blocks.iter().all(|b| b.len() == 516));

		pump.finish_burst(&tx);
		assert_eq!(rx.len(), 1, "the 50-sample tail becomes one padded block");
		assert_eq!(pump.blocks, 3);
	}

	/// A burst must not inherit the previous burst's half-encoded bytes or its
	/// encoder state: every talk session starts on its own first sample.
	#[test]
	fn start_burst_resets_the_encoder_and_the_partial_block() {
		// Two bursts with different sample values (257 and 127), so a block
		// header says which burst its first sample came from.
		let loud: Vec<u8> = vec![0x01u8; 3000];
		let next: Vec<u8> = (0..2050).map(|i| if i % 2 == 0 { 0x7F } else { 0x00 }).collect();

		// Control: the same two feeds *without* a reset. The second block then
		// starts from the first burst's leftover samples — proof that the
		// assertion below can fail.
		let (ctx, crx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		let mut control = Pump::new(&neg());
		control.feed(&loud, &ctx).unwrap();
		control.feed(&next, &ctx).unwrap();
		let carried = crx.try_iter().nth(1).expect("two blocks");
		assert_eq!(
			i16::from_le_bytes([carried[0], carried[1]]),
			257,
			"sanity: without a reset the second block still starts from the old burst's leftovers"
		);

		// The bridge: a reset between bursts, so the new burst owns its
		// block boundary from the first sample.
		let (tx, rx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		let mut pump = Pump::new(&neg());
		pump.feed(&loud, &tx).unwrap();
		assert_eq!(pump.blocks, 1);
		let first: Vec<Vec<u8>> = rx.try_iter().collect();
		assert_eq!(
			i16::from_le_bytes([first[0][0], first[0][1]]),
			257,
			"the loud burst's own block starts from its own first sample"
		);
		pump.pending_byte = Some(0x02);
		pump.start_burst();
		assert_eq!(pump.pending_byte, None, "start_burst drops the half sample");

		pump.feed(&next, &tx).unwrap();
		let blocks: Vec<Vec<u8>> = rx.try_iter().collect();
		assert_eq!(
			blocks.len(),
			1,
			"the reset burst contributes exactly one block, not one inflated by the old tail"
		);
		assert_eq!(
			i16::from_le_bytes([blocks[0][0], blocks[0][1]]),
			127,
			"start_burst must put the new burst's first sample in the header"
		);
		assert_eq!(blocks[0].len(), 516);
		assert_eq!(pump.blocks, 2, "only what was actually queued is counted");
	}

	/// Feeding a dead sender must surface as `SendLost`, not panic: that is how
	/// the bridge notices the session was closed under it.
	#[test]
	fn feed_reports_a_dead_session() {
		let mut pump = Pump::new(&neg());
		let (tx, rx) = bairelay_neolink_core::crossbeam_channel::unbounded::<Vec<u8>>();
		drop(rx);
		pump.feed(&[0u8; 8192], &tx)
			.expect_err("the session is gone");
	}

	#[test]
	fn queue_cap_is_ten_seconds_of_audio() {
		let neg = neg();
		let cap = ((MAX_QUEUED_SECONDS * f64::from(neg.sample_rate) / neg.block_samples as f64)
			.ceil() as usize)
			.max(1);
		// 10 s of 16 kHz audio in 1025-sample blocks: 160000 / 1025 = 156.1, so
		// 157 blocks — one block of slack never hurts on a ten-second budget.
		assert_eq!(cap, 157);
	}
}
