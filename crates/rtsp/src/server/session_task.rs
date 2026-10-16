//! Session send loop.
//!
//! Receives `Frame`s via `broadcast::Receiver`, packetizes them through
//! the RTP dispatcher, and writes packets to each session track's own
//! `Transport`. Periodic RTCP Sender Reports are intentionally NOT
//! emitted; the SR helpers in `crate::server::rtcp` remain for any
//! future SR-emitting context. Terminates on `cancel`, broadcast closed,
//! or unrecoverable transport error.
//!
//! The session task is a small **coordinator** that, after the PLAY
//! gate, spawns two independent per-kind dispatch loops —
//! `video_dispatch_loop` and `audio_dispatch_loop`. Each owns its
//! own `broadcast::Receiver`, polls `session_tracks` for its kind's
//! track lazily on track-miss, and writes packets via the track's
//! transport. The shared `TcpInterleavedTransport` mutex is held only
//! for one `$-framed` packet at a time, so audio FU fragments
//! interleave between video FU fragments at packet granularity. This
//! prevents a 4 K HEVC IDR from monopolising the wire while audio
//! frames pile up — the unified-loop design that preceded this task
//! produced audible 0.1–1.4 s audio gaps at the camera's GOP cadence.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::broadcast;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::buffer::LastFrameBuffer;
use crate::provider::Frame;
use crate::rtp::RtpCounters;
use crate::server::registry::{FirstVideoRtpSlot, SessionRegistry, TrackEntry, TrackKind};
use crate::server::transport::Transport;

/// Per-track runtime state maintained by the session send loop.
///
/// Mirrors a [`TrackEntry`] from the registry, plus the running RTP
/// sequence counters and RTCP SR totals. Reconstructed on every loop
/// iteration from the current `SessionEntry.tracks` snapshot so that a
/// track appended after the session task started (via
/// [`SessionRegistry::append_track`]) is picked up automatically.
///
/// Track-specific state that MUST persist across iterations (counters,
/// packet/octet totals, last RTP timestamp) is preserved by keying the
/// reconstruction on the track's SSRC — existing tracks keep their
/// state, new tracks get fresh counters.
struct RuntimeTrack {
	kind: TrackKind,
	transport: Arc<dyn Transport>,
	counters: RtpCounters,
	packet_count: u32,
	octet_count: u32,
	last_rtp_ts: u32,
	/// Monotonic instant at which `last_rtp_ts` was recorded. At SR fire
	/// time we extrapolate the RTP timestamp forward to "now" using this
	/// baseline and the track's clock rate, so the (NTP, RTP) pair in
	/// the SR corresponds to the same wall-clock instant per RFC 3550
	/// §6.4.1. `None` until the first packet is dispatched.
	last_rtp_instant: Option<Instant>,
	/// RTP clock rate in Hz. Video = 90000 (H.264/H.265 RTP convention).
	/// Audio = codec sample rate (16000 for AAC-LC on Argus, 8000 for
	/// G.711). Used to extrapolate RTP at SR fire time.
	clock_rate: u32,
}

/// Rebuild the `Vec<RuntimeTrack>` from the registry's current track
/// list. For every entry in `shared`, reuse the corresponding
/// `RuntimeTrack` from `existing` (matched by SSRC) so counters and
/// totals survive across iterations. Tracks newly appended since the
/// last call get fresh counters (random starting sequence, zero
/// totals).
fn rebuild_runtime(
	shared: &Arc<Mutex<Vec<TrackEntry>>>,
	existing: &[RuntimeTrack],
) -> Vec<RuntimeTrack> {
	let snapshot: Vec<(TrackKind, Arc<dyn Transport>, u32, u32)> = {
		let guard = shared.lock().expect("session tracks lock poisoned");
		guard
			.iter()
			.map(|t| (t.kind, Arc::clone(&t.transport), t.ssrc, t.clock_rate))
			.collect()
	};
	let mut out = Vec::with_capacity(snapshot.len());
	for (kind, transport, ssrc, clock_rate) in snapshot {
		if let Some(prev) = existing.iter().find(|p| p.counters.ssrc == ssrc) {
			out.push(RuntimeTrack {
				kind,
				transport,
				counters: RtpCounters {
					ssrc: prev.counters.ssrc,
					seq: prev.counters.seq,
				},
				packet_count: prev.packet_count,
				octet_count: prev.octet_count,
				last_rtp_ts: prev.last_rtp_ts,
				last_rtp_instant: prev.last_rtp_instant,
				clock_rate: prev.clock_rate,
			});
		} else {
			// Fresh track — random starting seq per RFC 3550.
			out.push(RuntimeTrack {
				kind,
				transport,
				counters: RtpCounters {
					ssrc,
					seq: rand::random::<u16>(),
				},
				packet_count: 0,
				octet_count: 0,
				last_rtp_ts: 0,
				last_rtp_instant: None,
				clock_rate,
			});
		}
	}
	out
}

/// Close every track's transport. Called on any teardown path so UDP
/// sockets / TCP writers see the close signal.
async fn close_all(runtime: &[RuntimeTrack]) {
	for t in runtime {
		t.transport.close().await;
	}
}

/// Dispatch one [`Frame`] to a single [`RuntimeTrack`].
///
/// Returns `Err(())` on the first unrecoverable transport error so the
/// caller (`video_dispatch_loop` / `audio_dispatch_loop`) can tear the
/// session down. The kind-vs-track match is the caller's responsibility:
/// each per-kind dispatch loop only invokes this with frames of its
/// own kind.
///
/// `first_video_rtp` is `Some(...)` only for the video loop — audio
/// frames don't contribute to the `RTP-Info:` PLAY response header.
async fn dispatch_one(
	frame: &Frame,
	track: &mut RuntimeTrack,
	first_video_rtp: Option<&FirstVideoRtpSlot>,
) -> Result<(), ()> {
	let pts_for_first = if let Frame::Video { pts_90khz, .. } = frame {
		Some(*pts_90khz)
	} else {
		None
	};
	let seq_for_first = track.counters.seq;

	let packets = crate::server::packetizer::dispatch(frame, &mut track.counters);

	for pkt in &packets {
		if track.transport.send_rtp(pkt).await.is_err() {
			return Err(());
		}
		track.packet_count = track.packet_count.wrapping_add(1);
		// Packets are always < MTU (~1500 bytes), so the cast to u32
		// is safe in practice.
		#[allow(clippy::cast_possible_truncation)]
		let len = pkt.len() as u32;
		track.octet_count = track.octet_count.wrapping_add(len);
	}

	let now = Instant::now();
	match frame {
		Frame::Video { pts_90khz, .. } => {
			track.last_rtp_ts = *pts_90khz;
			track.last_rtp_instant = Some(now);
		}
		Frame::Audio { pts, .. } => {
			track.last_rtp_ts = *pts;
			track.last_rtp_instant = Some(now);
		}
	}

	// First video packet's (seq, rtp_ts) is recorded once for the
	// PLAY handler's `RTP-Info:` header.
	if let (Some(slot), TrackKind::Video) = (first_video_rtp, track.kind) {
		if !packets.is_empty() {
			if let Ok(mut guard) = slot.lock() {
				if guard.is_none() {
					if let Some(pts) = pts_for_first {
						*guard = Some((seq_for_first, pts));
					}
				}
			}
		}
	}

	Ok(())
}

/// Build a [`RuntimeTrack`] for the requested `kind` from the registry's
/// shared track list. Returns `None` when no track of that kind has
/// been appended yet (typical for audio between PLAY and the second
/// SETUP). When `prev` is supplied AND the SSRC matches, counters /
/// totals carry forward — used by the dispatch loops on lazy refresh
/// after a track-miss so the receiver's per-track sequence numbering
/// stays stable across rebuilds.
fn build_runtime_for_kind(
	shared: &Arc<Mutex<Vec<TrackEntry>>>,
	kind: TrackKind,
	prev: Option<&RuntimeTrack>,
) -> Option<RuntimeTrack> {
	// Clone the few small fields we need rather than the whole
	// `TrackEntry` (which doesn't implement `Clone` because
	// `Arc<dyn Transport>` is the heavy bit). Hold the registry lock
	// only across the lookup + Arc bump.
	let (transport, ssrc, clock_rate) = {
		let guard = shared.lock().expect("session tracks lock poisoned");
		let entry = guard.iter().find(|t| t.kind == kind)?;
		(Arc::clone(&entry.transport), entry.ssrc, entry.clock_rate)
	};
	if let Some(prev) = prev.filter(|p| p.counters.ssrc == ssrc) {
		Some(RuntimeTrack {
			kind,
			transport,
			counters: RtpCounters {
				ssrc,
				seq: prev.counters.seq,
			},
			packet_count: prev.packet_count,
			octet_count: prev.octet_count,
			last_rtp_ts: prev.last_rtp_ts,
			last_rtp_instant: prev.last_rtp_instant,
			clock_rate: prev.clock_rate,
		})
	} else {
		Some(RuntimeTrack {
			kind,
			transport,
			counters: RtpCounters {
				ssrc,
				seq: rand::random::<u16>(),
			},
			packet_count: 0,
			octet_count: 0,
			last_rtp_ts: 0,
			last_rtp_instant: None,
			clock_rate,
		})
	}
}

/// Run the per-session send loop until cancelled or the upstream broadcast
/// closes.
///
/// **Architecture (2026-05-01):** the session task is a small
/// **coordinator** that, after the PLAY gate, spawns two independent
/// per-kind dispatch loops — `video_dispatch_loop` and
/// `audio_dispatch_loop`. Each loop:
/// - holds its own `broadcast::Receiver` (via `resubscribe()`), so a
///   slow video write never queues audio behind it on the consumer side;
/// - writes packets through its track's `Arc<dyn Transport>`, which
///   internally locks the shared TCP writer for ONE `$-framed` packet
///   at a time (≤ MTU, typically 1400 bytes), so audio FU fragments
///   interleave between video FU fragments at packet granularity;
/// - polls `session_tracks` lazily on track-miss, so a late audio
///   SETUP that appends a track is picked up on the next matching
///   frame (no `tracks_changed` subscription — `notify_one` would only
///   wake one of two waiters and we'd race).
///
/// The video loop runs `replay_burst` once before its live-dispatch
/// phase so the cached parameter-sets + I-frame still arrive before the
/// first live frame on the wire. Audio doesn't wait for replay.
///
/// **Why split:** running audio + video through a single `tokio::select!`
/// arm meant a 4 K HEVC IDR (~200–500 KB / ~150–370 RTP packets)
/// monopolised the dispatch task while it was being TCP-written. Audio
/// frames built up in the broadcast queue and drained in a burst once
/// the video write completed — audible 0.1–1.4 s gaps every ~2 s
/// (camera GOP cadence). Per-RTP-packet probe before/after this
/// refactor is in `tests/scripts/rtsp_probe.py`-style tooling.
///
/// **Failure model:** any child loop returns on broadcast `Lagged`,
/// `Closed`, or unrecoverable transport I/O. The coordinator awaits
/// `cancel` and the JoinSet's first completion — whichever wins, the
/// other child is then cancelled and joined, transports closed, and
/// the session removed from the registry (which drops the
/// `SubscriptionHandle` + its `WakeLockGuard`).
#[allow(clippy::too_many_arguments)]
pub async fn run(
	frames: broadcast::Receiver<Frame>,
	last_frame: Arc<LastFrameBuffer>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	sessions: Arc<SessionRegistry>,
	session_id: String,
	first_video_rtp: FirstVideoRtpSlot,
	cancel: CancellationToken,
	_tracks_changed: Arc<Notify>,
	play_signal: Arc<Notify>,
	play_fired: Arc<std::sync::atomic::AtomicBool>,
) {
	// RFC 2326 §10.4/§10.5: PLAY starts media delivery. Between SETUP
	// and PLAY the server MUST NOT send media data. Violating this breaks
	// downstream RTSP re-publishers — HA's go2rtc `ffmpeg:` wrap breaks
	// its RECORD pipe when RTP arrives before its second SETUP completes.
	// Park until PLAY fires or the session is cancelled.
	//
	// Notify ordering safety: PLAY handler sets `play_fired` THEN calls
	// `notify_waiters`. We register the notified() future BEFORE
	// re-checking the flag, so any PLAY that happens between these two
	// points latches a permit the await will consume — no lost edge.
	{
		let notified = play_signal.notified();
		tokio::pin!(notified);
		if !play_fired.load(std::sync::atomic::Ordering::SeqCst) {
			tokio::select! {
				_ = cancel.cancelled() => {
					let _ = sessions.remove(&session_id);
					return;
				}
				_ = notified.as_mut() => {}
			}
		}
	}

	// Spawn per-kind dispatch loops. Each child holds its own
	// `broadcast::Receiver` — independent positions, no head-of-line
	// blocking between video and audio.
	//
	// Asymmetry by design: the video child takes ownership of the
	// original `frames` receiver (preserving its read position so
	// pre-PLAY buffered frames + lag detection both flow through it),
	// while the audio child gets a fresh `resubscribe()` (starts at
	// the channel tail). Audio losing a few buffered frames at session
	// start is benign — receivers expect to start at "now" anyway.
	let audio_frames = frames.resubscribe();
	let mut joinset = tokio::task::JoinSet::new();

	{
		let last_frame = Arc::clone(&last_frame);
		let session_tracks = Arc::clone(&session_tracks);
		let cancel = cancel.clone();
		let first_video_rtp = first_video_rtp.clone();
		joinset.spawn(async move {
			video_dispatch_loop(frames, last_frame, session_tracks, cancel, first_video_rtp).await;
		});
	}
	{
		let session_tracks = Arc::clone(&session_tracks);
		let cancel = cancel.clone();
		joinset.spawn(async move {
			audio_dispatch_loop(audio_frames, session_tracks, cancel).await;
		});
	}

	// Wait for either an external cancel or the first child to exit
	// (broadcast lag/closed, transport error). Any child exit cascades
	// to the other via `cancel`.
	tokio::select! {
		biased;
		_ = cancel.cancelled() => {}
		_ = joinset.join_next() => {}
	}
	cancel.cancel();
	while joinset.join_next().await.is_some() {}

	// Cleanup. `rebuild_runtime` here is a fresh snapshot just to drive
	// `close_all` — counter state in the children is dropped, which is
	// fine on teardown.
	let final_runtime = rebuild_runtime(&session_tracks, &[]);
	close_all(&final_runtime).await;
	let _ = sessions.remove(&session_id);
}

/// Per-session video dispatch loop. Owns its own broadcast receiver,
/// runs `replay_burst` once at startup, then forwards `Frame::Video`
/// packets via the video track's transport.
async fn video_dispatch_loop(
	mut frames: broadcast::Receiver<Frame>,
	last_frame: Arc<LastFrameBuffer>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	cancel: CancellationToken,
	first_video_rtp: FirstVideoRtpSlot,
) {
	let mut track: Option<RuntimeTrack> =
		build_runtime_for_kind(&session_tracks, TrackKind::Video, None);

	// Replay cached burst on the video track if we have one. Audio
	// dispatch has already started in parallel — replay does not block
	// audio.
	let mut awaiting_live_keyframe = false;
	if let Some(t) = track.as_mut() {
		if let Some(burst) = last_frame.video_snapshot() {
			// Only arm the keyframe gate when replay actually emitted
			// something. An empty `iframe_nals` burst would otherwise
			// suppress live P-frames until the camera's next IDR for
			// nothing in return.
			awaiting_live_keyframe = replay_burst(&burst, &t.transport, &mut t.counters).await;
		}
	}

	loop {
		let frame = tokio::select! {
			biased;
			_ = cancel.cancelled() => return,
			recv = frames.recv() => match recv {
				Ok(f) => f,
				Err(broadcast::error::RecvError::Lagged(n)) => {
					tracing::warn!(missed = n, kind = "video", "subscriber lagged; dropping session");
					return;
				}
				Err(broadcast::error::RecvError::Closed) => return,
			},
		};

		// Skip non-video frames; the audio loop handles those.
		if !matches!(frame, Frame::Video { .. }) {
			continue;
		}

		// Lazy track resolution. Refreshes after a track-miss in case
		// the video track was appended late (uncommon — video is
		// usually the first SETUP).
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Video, None);
			if track.is_none() {
				continue;
			}
		}
		let track = track.as_mut().expect("just resolved Some above");

		// Suppress post-burst live video until a fresh IDR arrives.
		// Cached IDR was from the previous GOP; live P-frames before
		// the next live IDR would log `Could not find ref with POC N`
		// in the receiver's decoder.
		if awaiting_live_keyframe {
			match &frame {
				Frame::Video { keyframe: true, .. } => awaiting_live_keyframe = false,
				_ => continue,
			}
		}

		if dispatch_one(&frame, track, Some(&first_video_rtp))
			.await
			.is_err()
		{
			return;
		}
	}
}

/// Per-session audio dispatch loop. Owns its own broadcast receiver,
/// forwards `Frame::Audio` via the audio track's transport.
async fn audio_dispatch_loop(
	mut frames: broadcast::Receiver<Frame>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	cancel: CancellationToken,
) {
	let mut track: Option<RuntimeTrack> =
		build_runtime_for_kind(&session_tracks, TrackKind::Audio, None);

	loop {
		let frame = tokio::select! {
			biased;
			_ = cancel.cancelled() => return,
			recv = frames.recv() => match recv {
				Ok(f) => f,
				Err(broadcast::error::RecvError::Lagged(n)) => {
					tracing::warn!(missed = n, kind = "audio", "subscriber lagged; dropping session");
					return;
				}
				Err(broadcast::error::RecvError::Closed) => return,
			},
		};

		if !matches!(frame, Frame::Audio { .. }) {
			continue;
		}

		// Lazy track resolution. Audio is frequently SETUP'd late
		// (after the client has issued its first DESCRIBE → SETUP
		// pair on video), so this branch fires on real traffic.
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Audio, None);
			if track.is_none() {
				continue;
			}
		}
		let track = track.as_mut().expect("just resolved Some above");

		if dispatch_one(&frame, track, None).await.is_err() {
			return;
		}
	}
}

/// Replay the cached `VideoBurst` on `transport`. Returns `true` when at
/// least one RTP packet was successfully sent, `false` for an empty
/// `iframe_nals` burst or when the first send returned an error.
async fn replay_burst(
	burst: &crate::buffer::VideoBurst,
	transport: &Arc<dyn Transport>,
	counters: &mut RtpCounters,
) -> bool {
	use crate::codec::{h264, h265, VideoCodec};
	// Use the camera-side pts captured at burst recording time. This
	// keeps the replay packets continuous with subsequent live-frame
	// packets (whose pts come from the same camera clock), avoiding a
	// large timestamp jump that breaks downstream re-muxers (HA's
	// go2rtc `ffmpeg:` wrap in particular — without this replay at
	// ts=0 seeded 500+ packets before a live pts of 60M+ ticks,
	// causing its RECORD pipeline to drop the feed).
	let ts = burst.captured_pts_90khz;
	// Parameter sets are advertised to clients via the SDP
	// `sprop-parameter-sets` / `sprop-vps|sps|pps` fmtp attribute, so the
	// receiving decoder already has them before any RTP packet arrives.
	// Sending them in-band as separate single-NAL packets makes
	// re-packaging intermediaries (notably HA's go2rtc `ffmpeg:` wrap)
	// aggregate them into an HEVC RFC 7798 §4.4.2 AP (Aggregation
	// Packet, NAL type 48) on the downstream re-publish, which go2rtc's
	// own RTPDepay does not de-aggregate — the raw AP bytes reach its
	// `/api/frame.jpeg` JPEG transcoder and ffmpeg exits status 183.
	// So we omit the in-band parameter-set replay entirely and send only
	// the cached I-frame NAL(s); the live stream continues to deliver
	// future parameter sets in-band exactly as the camera emits them.
	// The marker bit on the LAST RTP packet of an access unit signals
	// end-of-frame to the receiver (RFC 3550 §5.1, RFC 7798 §4.1). Without
	// it ffmpeg's RTSP demuxer never flushes the IDR to its muxer — the
	// frame sits in the reassembly buffer indefinitely. This is what broke
	// HA's go2rtc `ffmpeg:` wrap on HEVC main: ffmpeg received the full
	// IDR bytes but couldn't emit them downstream, so go2rtc timed out on
	// the first frame and tore down the producer.
	//
	// Set marker_on_last=true on the FINAL FU/single packet of the FINAL
	// iframe NAL only — not on earlier NALs or fragments within the same
	// NAL.
	let Some(last_nal_idx) = burst.iframe_nals.len().checked_sub(1) else {
		return false;
	};
	let mut sent = false;
	for (i, nal) in burst.iframe_nals.iter().enumerate() {
		let is_last_nal = i == last_nal_idx;
		let pkts = match burst.codec {
			VideoCodec::H264 => {
				if nal.len() + 12 <= h264::DEFAULT_MTU {
					vec![h264::packetize_single(nal, counters, ts, is_last_nal)]
				} else {
					h264::packetize_fu_a(nal, counters, ts, is_last_nal, h264::DEFAULT_MTU)
				}
			}
			VideoCodec::H265 => {
				if nal.len() + 12 <= h265::DEFAULT_MTU {
					vec![h265::packetize_single(nal, counters, ts, is_last_nal)]
				} else {
					h265::packetize_fu(nal, counters, ts, is_last_nal, h265::DEFAULT_MTU)
				}
			}
		};
		for pkt in &pkts {
			if transport.send_rtp(pkt).await.is_err() {
				return sent;
			}
			sent = true;
		}
	}
	sent
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	// ====== run() coverage tests ======

	use crate::codec::VideoCodec;
	use crate::provider::{AudioPayload, Frame};
	use crate::server::transport::noop_transport_for_tests;
	use bytes::Bytes;
	use std::sync::atomic::AtomicBool;

	fn video_track(ssrc: u32) -> TrackEntry {
		TrackEntry {
			kind: TrackKind::Video,
			transport: noop_transport_for_tests(),
			ssrc,
			clock_rate: 90_000,
		}
	}

	fn audio_track(ssrc: u32) -> TrackEntry {
		TrackEntry {
			kind: TrackKind::Audio,
			transport: noop_transport_for_tests(),
			ssrc,
			clock_rate: 16_000,
		}
	}

	fn video_frame(keyframe: bool, pts: u32) -> Frame {
		Frame::Video {
			codec: VideoCodec::H264,
			nals: vec![Bytes::from_static(&[0x65, 0xaa, 0xbb])],
			pts_90khz: pts,
			keyframe,
			access_unit_end: true,
		}
	}

	fn audio_frame(pts: u32) -> Frame {
		Frame::Audio {
			payload: AudioPayload::G711Ulaw {
				samples: Bytes::from_static(&[0x7f; 160]),
			},
			pts,
		}
	}

	/// Sets up a session with one video track, fires PLAY signal, and
	/// returns the pieces needed to drive run() from a test.
	#[allow(clippy::type_complexity)]
	fn fixture(
		tracks: Vec<TrackEntry>,
	) -> (
		broadcast::Sender<Frame>,
		broadcast::Receiver<Frame>,
		Arc<LastFrameBuffer>,
		Arc<Mutex<Vec<TrackEntry>>>,
		Arc<SessionRegistry>,
		FirstVideoRtpSlot,
		CancellationToken,
		Arc<Notify>,
		Arc<Notify>,
		Arc<AtomicBool>,
	) {
		let (tx, rx) = broadcast::channel::<Frame>(32);
		let last_frame = Arc::new(LastFrameBuffer::new());
		let shared_tracks = Arc::new(Mutex::new(tracks));
		let registry = Arc::new(SessionRegistry::new());
		let first_video_rtp = Arc::new(Mutex::new(None));
		let cancel = CancellationToken::new();
		let tracks_changed = Arc::new(Notify::new());
		let play_signal = Arc::new(Notify::new());
		let play_fired = Arc::new(AtomicBool::new(true)); // pre-armed
		(
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		)
	}

	#[tokio::test(start_paused = true)]
	async fn run_dispatches_video_frame_and_cancels_cleanly() {
		let (
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		) = fixture(vec![video_track(0xdead_beef)]);

		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-1".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));

		tx.send(video_frame(true, 90_000)).unwrap();
		// Yield to let the spawned task consume the broadcast frame.
		// Replaces the earlier real-clock `sleep(30ms)` — paused clock
		// makes the test deterministic under tarpaulin instrumentation.
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.expect("run did not exit on cancel")
			.unwrap();
	}

	#[tokio::test]
	async fn run_cancels_before_play_fires() {
		// Pre-arm the cancel token before PLAY fires — run should return
		// from the PLAY-gate select arm.
		let (
			_tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			_pf,
		) = fixture(vec![video_track(1)]);
		let play_fired = Arc::new(AtomicBool::new(false));

		cancel.cancel();
		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-cancel".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.expect("cancel before PLAY did not exit promptly")
			.unwrap();
	}

	#[tokio::test(start_paused = true)]
	async fn run_replays_cached_burst_on_startup() {
		let (
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		) = fixture(vec![video_track(0x0101_0101)]);

		// Seed a cached burst so replay_burst runs.
		last_frame.replace_video(crate::buffer::VideoBurst {
			codec: VideoCodec::H264,
			parameter_sets: vec![vec![0x67, 0x42, 0x00, 0x1f], vec![0x68, 0xce]],
			iframe_nals: vec![vec![0x65, 0xaa, 0xbb, 0xcc]],
			pframe_nals: vec![],
			captured_at: Instant::now(),
			captured_pts_90khz: 12_345,
		});

		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-burst".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));
		// Drive the spawned task through replay_burst — paused-clock
		// equivalent of the previous `sleep(30ms)`.
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.unwrap()
			.unwrap();
		drop(tx);
	}

	#[tokio::test(start_paused = true)]
	async fn run_drops_audio_when_no_audio_track() {
		// No-track filter branch: send audio frames while only a video
		// track is attached. After NO_TRACK_DROP_REBUILD_THRESHOLD (4)
		// drops, the task performs a defensive rebuild.
		let (
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		) = fixture(vec![video_track(1)]);

		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-drop".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));
		for p in 0..6 {
			tx.send(audio_frame(p * 1024)).unwrap();
		}
		// Yield enough times to let run() drain all 6 frames AND fire
		// the NO_TRACK_DROP_REBUILD_THRESHOLD rebuild. Was a 30 ms
		// real-clock sleep.
		for _ in 0..16 {
			tokio::task::yield_now().await;
		}
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.unwrap()
			.unwrap();
	}

	#[tokio::test(start_paused = true)]
	async fn run_rebuilds_on_tracks_changed() {
		let (
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		) = fixture(vec![video_track(1)]);

		let handle = tokio::spawn(run(
			rx,
			last_frame,
			Arc::clone(&shared_tracks),
			Arc::clone(&registry),
			"sess-rebuild".to_string(),
			first_video_rtp,
			cancel.clone(),
			Arc::clone(&tracks_changed),
			play_signal,
			play_fired,
		));

		// Append an audio track and fire the notify: the rebuild arm must run.
		shared_tracks.lock().unwrap().push(audio_track(2));
		tracks_changed.notify_one();
		// Two yield batches replace the two real-clock 20 ms sleeps —
		// first lets the rebuild fire, second lets the audio frame
		// dispatch through the freshly-rebuilt route table.
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		tx.send(audio_frame(0)).unwrap();
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.unwrap()
			.unwrap();
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_closed() {
		let (
			tx,
			rx,
			last_frame,
			shared_tracks,
			registry,
			first_video_rtp,
			cancel,
			tracks_changed,
			play_signal,
			play_fired,
		) = fixture(vec![video_track(1)]);
		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-closed".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));
		drop(tx); // close broadcast → run exits on Closed arm
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.expect("run did not exit on broadcast close")
			.unwrap();
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_lagged() {
		// Build a tiny-capacity channel so we can force a Lagged error.
		let (tx, rx) = broadcast::channel::<Frame>(2);
		let last_frame = Arc::new(LastFrameBuffer::new());
		let shared_tracks = Arc::new(Mutex::new(vec![video_track(1)]));
		let registry = Arc::new(SessionRegistry::new());
		let first_video_rtp = Arc::new(Mutex::new(None));
		let cancel = CancellationToken::new();
		let tracks_changed = Arc::new(Notify::new());
		let play_signal = Arc::new(Notify::new());
		let play_fired = Arc::new(AtomicBool::new(true));

		// Fill beyond capacity so rx lags.
		for i in 0..10 {
			let _ = tx.send(video_frame(false, i));
		}

		let handle = tokio::spawn(run(
			rx,
			last_frame,
			shared_tracks,
			Arc::clone(&registry),
			"sess-lag".to_string(),
			first_video_rtp,
			cancel.clone(),
			tracks_changed,
			play_signal,
			play_fired,
		));
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.expect("run did not exit on lagged")
			.unwrap();
	}

	// `run_handles_sr_interval_tick_after_first_frame` was removed
	// 2026-05-01 along with the SR_INTERVAL ticker arm. RTCP SRs are
	// suppressed at the protocol level; the ticker had no body, so a
	// "ticker still ticks without panic" test had no behavioural
	// signal left to verify after the audio/video split refactor.

	#[tokio::test]
	async fn build_runtime_for_kind_returns_none_when_kind_absent() {
		// Replaces the old `dispatch_frame_returns_err_on_missing_track`
		// — the single-track `dispatch_one` no longer takes a slice, so
		// the missing-track check moved up into the per-kind dispatch
		// loop's lazy-resolution path. Verify that path's primitive:
		// resolution returns None when no track of the kind is in the
		// registry.
		let shared: Arc<Mutex<Vec<TrackEntry>>> = Arc::new(Mutex::new(vec![]));
		assert!(build_runtime_for_kind(&shared, TrackKind::Video, None).is_none());
		assert!(build_runtime_for_kind(&shared, TrackKind::Audio, None).is_none());
	}

	/// Transport that fails every send — used to exercise the Err arms
	/// in `dispatch_one` / `replay_burst`.
	struct FailingTransport;
	#[async_trait::async_trait]
	impl Transport for FailingTransport {
		async fn send_rtp(&self, _: &[u8]) -> std::io::Result<()> {
			Err(std::io::Error::other("boom"))
		}
		async fn send_rtcp(&self, _: &[u8]) -> std::io::Result<()> {
			Err(std::io::Error::other("boom"))
		}
		async fn close(&self) {}
	}

	#[tokio::test]
	async fn dispatch_one_returns_err_when_transport_send_fails() {
		let first_video_rtp: FirstVideoRtpSlot = Arc::new(Mutex::new(None));
		let mut track = RuntimeTrack {
			kind: TrackKind::Video,
			transport: Arc::new(FailingTransport),
			counters: RtpCounters { ssrc: 1, seq: 0 },
			packet_count: 0,
			octet_count: 0,
			last_rtp_ts: 0,
			last_rtp_instant: None,
			clock_rate: 90_000,
		};
		let frame = video_frame(true, 0);
		let res = dispatch_one(&frame, &mut track, Some(&first_video_rtp)).await;
		assert!(res.is_err());
	}

	#[tokio::test]
	async fn replay_burst_h264_fu_a_path_with_failing_transport() {
		// Large IDR NAL forces packetize_fu_a (line 480) and the
		// failing transport returns Err on the first packet, hitting
		// the `return` at line 493.
		use crate::codec::VideoCodec;
		let burst = crate::buffer::VideoBurst {
			codec: VideoCodec::H264,
			parameter_sets: vec![vec![0x67, 0x42, 0x00, 0x1f], vec![0x68, 0xce]],
			iframe_nals: vec![{
				let mut big = vec![0x65u8]; // IDR header
				big.extend(vec![0xaa; 5000]);
				big
			}],
			pframe_nals: vec![],
			captured_at: Instant::now(),
			captured_pts_90khz: 0,
		};
		let transport: Arc<dyn Transport> = Arc::new(FailingTransport);
		let mut counters = RtpCounters { ssrc: 7, seq: 0 };
		replay_burst(&burst, &transport, &mut counters).await;
	}

	#[tokio::test]
	async fn replay_burst_h265_fu_path_with_failing_transport() {
		use crate::codec::VideoCodec;
		let burst = crate::buffer::VideoBurst {
			codec: VideoCodec::H265,
			parameter_sets: vec![],
			iframe_nals: vec![{
				let mut big = vec![0x26u8, 0x01]; // IDR_W_RADL
				big.extend(vec![0xaa; 5000]);
				big
			}],
			pframe_nals: vec![],
			captured_at: Instant::now(),
			captured_pts_90khz: 0,
		};
		let transport: Arc<dyn Transport> = Arc::new(FailingTransport);
		let mut counters = RtpCounters { ssrc: 8, seq: 0 };
		replay_burst(&burst, &transport, &mut counters).await;
	}

	#[test]
	fn rebuild_runtime_preserves_counters_across_rebuilds() {
		let tracks = Arc::new(Mutex::new(vec![video_track(42)]));
		let first = rebuild_runtime(&tracks, &[]);
		assert_eq!(first.len(), 1);
		let seq_before = first[0].counters.seq;
		// Second rebuild from the same snapshot reuses the counters.
		let second = rebuild_runtime(&tracks, &first);
		assert_eq!(second.len(), 1);
		assert_eq!(second[0].counters.seq, seq_before);
		assert_eq!(second[0].counters.ssrc, 42);
	}
}
