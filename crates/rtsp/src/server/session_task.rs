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
//!
//! **Key frame requests (2026-11-15).** A client that loses packets asks for
//! a new key frame in RTCP (PLI / FIR). Until now nothing in this crate read
//! inbound RTCP at all, so the ask died at the foot of the bridge and the
//! client waited for the camera's own next IDR with a frozen picture. The
//! connection task and the UDP RTCP reader now hand recognised requests to
//! the session's [`KeyframeGate`], and `video_dispatch_loop` answers one by
//! replaying the cached burst on that client's own transport. See
//! [`crate::server::rtcp::find_keyframe_request`].

use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::broadcast;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::buffer::LastFrameBuffer;
use crate::codec::nal::{H264NalType, H265NalType};
use crate::codec::VideoCodec;
use crate::provider::Frame;
use crate::rtp::RtpCounters;
use crate::server::registry::{
	FirstVideoRtpSlot, KeyframeGate, SessionRegistry, TrackEntry, TrackKind,
};
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
/// first live frame on the wire. Audio doesn't wait for replay. It also
/// drains `keyframe_gate`: a PLI or FIR the client sent on either transport
/// becomes a second, on-demand replay of that same burst for that one
/// session (audio has no key frame to offer, so the audio loop takes no
/// gate).
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
	keyframe_gate: Arc<KeyframeGate>,
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
		let keyframe_gate = Arc::clone(&keyframe_gate);
		// The loop labels its diagnostics with the session; `Arc<str>` so
		// the label costs one refcount clone, not a copy per line.
		let session_id: Arc<str> = Arc::from(session_id.as_str());
		joinset.spawn(async move {
			video_dispatch_loop(
				frames,
				last_frame,
				session_tracks,
				cancel,
				first_video_rtp,
				keyframe_gate,
				session_id,
			)
			.await;
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
	keyframe_gate: Arc<KeyframeGate>,
	session_id: Arc<str>,
) {
	let mut track: Option<RuntimeTrack> =
		build_runtime_for_kind(&session_tracks, TrackKind::Video, None);

	// Replay cached burst on the video track if we have one. Audio
	// dispatch has already started in parallel — replay does not block
	// audio.
	//
	// `keyframe-diag` (2026-11-15): on the operator's phone the picture
	// stayed still while this loop pushed ~38 KiB/s of live RTP with no gap,
	// on sessions opened against a camera that had to wake up first. A fresh
	// session can ONLY be primed by this replay — outbound `Frame::Video`
	// has its SPS/PPS stripped at the translator boundary
	// (`stream_source::handle_iframe`; see the `codec::h264` module docs) —
	// so whether the cache held a burst, and whether the replay reached the
	// transport, is the difference between a moving picture and a still one.
	// One debug block per session, prefixed `keyframe-diag` so it can be
	// grepped; nothing below this line changes what is sent.
	let startup_snapshot = last_frame.video_snapshot();
	let startup_burst_pts = startup_snapshot.as_ref().map(|b| b.captured_pts_90khz);
	let mut awaiting_live_keyframe = false;
	match (track.as_mut(), &startup_snapshot) {
		(Some(t), Some(burst)) => {
			// Only arm the keyframe gate when replay actually emitted
			// something. An empty `iframe_nals` burst would otherwise
			// suppress live P-frames until the camera's next IDR for
			// nothing in return.
			awaiting_live_keyframe = replay_burst(burst, &t.transport, &mut t.counters).await;
			tracing::debug!(
				"keyframe-diag session={session_id} stage=start video_track=yes \
				 cache_burst=yes burst_captured_pts_90khz={} burst_age_ms={} \
				 burst_iframe_nals={} burst_parameter_sets={} replay_emitted={} \
				 live_keyframe_guard_armed={}",
				burst.captured_pts_90khz,
				burst.captured_at.elapsed().as_millis(),
				burst.iframe_nals.len(),
				burst.parameter_sets.len(),
				awaiting_live_keyframe,
				awaiting_live_keyframe,
			);
		}
		(Some(_), None) => {
			tracing::debug!(
				"keyframe-diag session={session_id} stage=start video_track=yes \
				 cache_burst=no replay_emitted=no live_keyframe_guard_armed=false \
				 reason=no cached burst yet; session waits for the camera's own IDR",
			);
		}
		(None, snapshot) => {
			tracing::debug!(
				"keyframe-diag session={session_id} stage=start video_track=no \
				 cache_burst={} replay_emitted=no \
				 reason=no video track when the loop started",
				snapshot.is_some(),
			);
		}
	}

	// Inbound RTCP from this client (PLI / FIR) arrives through the
	// session's gate. The watch is level-triggered, so a request landing
	// while we are inside a frame dispatch is still visible on the next
	// poll — no wake-up can be lost, which is why this is a watch and not
	// another `Notify`.
	let mut keyframe_requests = keyframe_gate.subscribe();
	let mut last_keyframe_request = *keyframe_requests.borrow_and_update();

	// `keyframe-diag`: reduced copies of this session's first access units,
	// summarised once (see `summarize_video_startup`).
	let mut startup_sample: Vec<Frame> = Vec::new();
	let mut startup_sample_logged = false;
	let mut first_live_logged = false;

	loop {
		// `biased`, and inbound RTCP first: when a client says "my decoder
		// lost its reference", the answer outranks one more P-frame.
		let mut keyframe_requested = false;
		let frame = tokio::select! {
			biased;
			_ = cancel.cancelled() => return,
			changed = keyframe_requests.changed() => {
				if changed.is_err() {
					// Gate dropped with the session entry: nobody left to serve.
					return;
				}
				// The gate counts accepted requests only — the anti-storm
				// window already ran on the reader side — so every change
				// here is a replay the client is owed.
				let seen = *keyframe_requests.borrow_and_update();
				keyframe_requested = seen != last_keyframe_request;
				last_keyframe_request = seen;
				None
			}
			recv = frames.recv() => match recv {
				Ok(f) => Some(f),
				Err(broadcast::error::RecvError::Lagged(n)) => {
					tracing::warn!(missed = n, kind = "video", "subscriber lagged; dropping session");
					return;
				}
				Err(broadcast::error::RecvError::Closed) => return,
			},
		};

		if keyframe_requested {
			replay_on_keyframe_request(&last_frame, &session_tracks, &mut track, &session_id).await;
			continue;
		}

		let Some(frame) = frame else {
			// Unreachable: the frames arm yields a frame or returns.
			continue;
		};

		// Skip non-video frames; the audio loop handles those.
		if !matches!(frame, Frame::Video { .. }) {
			continue;
		}

		// `keyframe-diag`: keep a reduced copy of the first access units.
		// Deliberately BEFORE the lazy track resolution and the
		// live-keyframe guard below, so units that never reached the wire
		// are still counted — "the session was fed P-frames it could not
		// decode" is exactly what a frozen phone looks like from here.
		if !startup_sample_logged && startup_sample.len() < VIDEO_STARTUP_SAMPLE_UNITS {
			if let Some(reduced) = shrink_for_sample(&frame) {
				startup_sample.push(reduced);
			}
			if startup_sample.len() == VIDEO_STARTUP_SAMPLE_UNITS {
				startup_sample_logged = true;
				tracing::debug!(
					"keyframe-diag session={session_id} stage=first-units {}",
					summarize_video_startup(&startup_sample),
				);
				// Free the sample: it is never read again.
				startup_sample = Vec::new();
			}
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

		// `keyframe-diag`, second half: the burst timestamp against the
		// first live packet this session really dispatched — that pair is
		// what the receiver's decoder sees. A large positive delta reads as
		// "stale base, then live media far ahead of it"; `first_live` with
		// the guard still armed is the signature of a session that never got
		// a decodable base at all. Once per session, at debug.
		if !first_live_logged {
			first_live_logged = true;
			if let Frame::Video { pts_90khz, keyframe, .. } = frame {
				let (burst_label, delta_label) = match startup_burst_pts {
					Some(b) => (
						b.to_string(),
						i64::from(pts_90khz).wrapping_sub(i64::from(b)).to_string(),
					),
					None => ("none".to_string(), "n-a".to_string()),
				};
				tracing::debug!(
					"keyframe-diag session={session_id} stage=first-live \
					 burst_captured_pts_90khz={burst_label} first_live_pts_90khz={pts_90khz} \
					 delta_90khz={delta_label} first_live_keyframe={keyframe} \
					 live_keyframe_guard_armed={awaiting_live_keyframe}",
				);
			}
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

/// How many video access units the startup diagnostic samples per session.
///
/// Twelve is about one second of a 12 fps sub stream: enough to see whether
/// a session ever received a parameter set and an IDR, few enough that the
/// one debug line stays readable on a phone screen.
const VIDEO_STARTUP_SAMPLE_UNITS: usize = 12;

/// Reduce a frame to what [`summarize_video_startup`] reads: the codec, the
/// `keyframe` flag, the timestamp, and the first octet of every NAL (the NAL
/// header, which is all the type label needs). Keeping a dozen main-stream
/// access units whole would pin roughly a megabyte per session on a
/// Raspberry Pi for no reason. Returns `None` for audio frames.
fn shrink_for_sample(frame: &Frame) -> Option<Frame> {
	match frame {
		Frame::Video {
			codec,
			nals,
			pts_90khz,
			keyframe,
			access_unit_end,
		} => Some(Frame::Video {
			codec: *codec,
			nals: nals
				.iter()
				.map(|n| n.slice(..n.len().min(1)))
				.collect(),
			pts_90khz: *pts_90khz,
			keyframe: *keyframe,
			access_unit_end: *access_unit_end,
		}),
		Frame::Audio { .. } => None,
	}
}

/// Human label for a NAL's type, using only what [`crate::codec::nal`]
/// already recognises. Anything else keeps its numeric value, so the
/// diagnostic can never hide a type we have not seen before.
fn nal_type_label(codec: VideoCodec, nal: &[u8]) -> String {
	let Some(&first) = nal.first() else {
		return "empty".to_string();
	};
	match codec {
		VideoCodec::H264 => match H264NalType::from_header_byte(first) {
			H264NalType::NON_IDR_SLICE => "NON_IDR",
			H264NalType::IDR_SLICE => "IDR",
			H264NalType::SEI => "SEI",
			H264NalType::SPS => "SPS",
			H264NalType::PPS => "PPS",
			H264NalType::AUD => "AUD",
			other => return format!("other{other}"),
		},
		VideoCodec::H265 => match H265NalType::from_header_byte(first) {
			0 | 1 => "NON_IDR",
			H265NalType::VPS => "VPS",
			H265NalType::SPS => "SPS",
			H265NalType::PPS => "PPS",
			H265NalType::AUD => "AUD",
			// IRAP slices: BLA_W_LP .. CRA plus the two reserved slots
			// (H265NalType::BLA_W_LP = 16, IDR_W_RADL = 19, IDR_N_LP = 20,
			// CRA = 21).
			t if (16..=23).contains(&t) => "IDR",
			other => return format!("other{other}"),
		},
	}
	.to_string()
}

/// Startup diagnostic: one line summarising a session's first video access
/// units.
///
/// Why this exists: outbound `Frame::Video` has its SPS/PPS stripped at the
/// translator boundary (`stream_source::handle_iframe`; the `codec::h264`
/// module docs record why), so a fresh session's only route to a decodable
/// base is the SDP `sprop-parameter-sets` attribute plus the cached burst
/// replay. When the field reports "the server is sending and the phone is
/// still", the answer is in whether these units ever carried a parameter set
/// and an IDR — hence: NAL types seen, parameter-set presence, the
/// `keyframe` flag and the timestamp span, on one line, at `debug`.
///
/// Pure: frames in, line out — no I/O, no clock, no session — so the
/// claim "it says the parameter sets are absent when they are absent" is
/// testable without a socket.
pub(crate) fn summarize_video_startup(frames: &[Frame]) -> String {
	let Some(codec) = frames.iter().find_map(|f| match f {
		Frame::Video { codec, .. } => Some(*codec),
		Frame::Audio { .. } => None,
	}) else {
		return "codec=- units=0 keyframes=0 nal_types=[] sps=absent pps=absent \
			pts_90khz=[none]"
			.to_string();
	};
	let codec_label = match codec {
		VideoCodec::H264 => "h264",
		VideoCodec::H265 => "h265",
	};

	let mut labels: Vec<String> = Vec::new();
	let mut units = 0usize;
	let mut keyframes = 0usize;
	let mut first_pts: Option<u32> = None;
	let mut last_pts: Option<u32> = None;
	let mut saw_sps = false;
	let mut saw_pps = false;
	let mut saw_vps = false;

	for frame in frames {
		let Frame::Video { nals, pts_90khz, keyframe, .. } = frame else {
			continue;
		};
		units += 1;
		if first_pts.is_none() {
			first_pts = Some(*pts_90khz);
		}
		last_pts = Some(*pts_90khz);
		if *keyframe {
			keyframes += 1;
		}
		for nal in nals {
			let label = nal_type_label(codec, nal);
			match label.as_str() {
				"SPS" => saw_sps = true,
				"PPS" => saw_pps = true,
				"VPS" => saw_vps = true,
				_ => {}
			}
			if !labels.contains(&label) {
				labels.push(label);
			}
		}
	}

	// H.264 has no VPS; naming one for it would only confuse the reader.
	let vps_part = match codec {
		VideoCodec::H265 => format!(" vps={}", presence(saw_vps)),
		VideoCodec::H264 => String::new(),
	};
	let (first_label, last_label) = match (first_pts, last_pts) {
		(Some(f), Some(l)) => (f.to_string(), l.to_string()),
		_ => ("none".to_string(), "none".to_string()),
	};
	format!(
		"codec={codec_label} units={units} keyframes={keyframes} nal_types=[{}] \
			sps={} pps={}{} pts_90khz=[{first_label}..{last_label}]",
		labels.join(","),
		presence(saw_sps),
		presence(saw_pps),
		vps_part,
	)
}

/// `present` / `absent`, spelled out because an operator reads this line off
/// a phone screen, not a diff.
fn presence(seen: bool) -> &'static str {
	if seen {
		"present"
	} else {
		"absent"
	}
}

/// Answer a client's key frame request (RTCP PLI / FIR) by replaying the
/// cached burst on that session's own video transport.
///
/// Reuses [`replay_burst`] and the track's own `RtpCounters`, so the replayed
/// packets continue the session's sequence numbering and the client sees a
/// gap-free stream, exactly as for the start-of-session replay.
///
/// The `awaiting_live_keyframe` guard is deliberately NOT touched here, and
/// the caller does not touch it either. That guard means "no *live* IDR has
/// been seen on this session yet"; a replay hands the client a base already
/// sitting in the cache, which is not a live IDR, so arming or clearing the
/// guard from this path would silently change what happens to live frames.
/// The guard keeps whatever state the startup path gave it.
///
/// Logs exactly one debug line prefixed `keyframe-replay`, saying what went
/// out or why nothing could.
async fn replay_on_keyframe_request(
	last_frame: &Arc<LastFrameBuffer>,
	session_tracks: &Arc<Mutex<Vec<TrackEntry>>>,
	track: &mut Option<RuntimeTrack>,
	session_id: &str,
) {
	// The video track is normally resolved at loop start; resolve again to
	// cover a track appended after a track-miss.
	if track.is_none() {
		*track = build_runtime_for_kind(session_tracks, TrackKind::Video, None);
	}
	let Some(t) = track.as_mut() else {
		tracing::debug!("keyframe-replay session={session_id} replayed=no reason=no video track");
		return;
	};
	// Read the cache, never fill it: `src/stream_source.rs` owns what goes
	// into `LastFrameBuffer` (`replace_video` on I-frame, `append_pframe` on
	// P-frame).
	let Some(burst) = last_frame.video_snapshot() else {
		tracing::debug!(
			"keyframe-replay session={session_id} replayed=no \
			 reason=burst cache empty (no IDR recorded from this camera yet)",
		);
		return;
	};
	let emitted = replay_burst(&burst, &t.transport, &mut t.counters).await;
	tracing::debug!(
		"keyframe-replay session={session_id} replayed={} burst_captured_pts_90khz={} \
			burst_age_ms={} burst_iframe_nals={}",
		emitted,
		burst.captured_pts_90khz,
		burst.captured_at.elapsed().as_millis(),
		burst.iframe_nals.len(),
	);
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
	use crate::server::session_task::summarize_video_startup;
	use crate::server::registry::KEYFRAME_REPLAY_MIN_INTERVAL;
	use crate::server::transport::{counting_transport_for_tests, noop_transport_for_tests};
	use bytes::Bytes;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

	// ====== Key frame requests: PLI / FIR drive a burst replay ======

	/// Hand-built PLI (RFC 4585 §6.3.1, 12 octets) as the client sends it.
	fn pli_packet(sender: u32, media_ssrc: u32) -> Vec<u8> {
		let mut p = vec![0x81, 206, 0x00, 0x02];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&media_ssrc.to_be_bytes());
		p
	}

	/// Hand-built RR (RFC 3550 §6.4.2, one block, 32 octets). Same first
	/// octet as a PLI; only PT differs — this is the packet a streaming
	/// client sends about once a second.
	fn rr_packet(sender: u32, reported: u32) -> Vec<u8> {
		let mut p = vec![0x81, 201, 0x00, 0x07];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&reported.to_be_bytes());
		p.extend_from_slice(&[0u8; 20]);
		p
	}

	/// A burst whose single IDR NAL fits in one RTP packet, so a replay is
	/// exactly one packet on the counting transport.
	fn small_idr_burst() -> crate::buffer::VideoBurst {
		crate::buffer::VideoBurst {
			codec: VideoCodec::H264,
			parameter_sets: vec![vec![0x67, 0x42, 0x00, 0x1f], vec![0x68, 0xce]],
			iframe_nals: vec![vec![0x65, 0xaa, 0xbb, 0xcc]],
			pframe_nals: vec![],
			captured_at: std::time::Instant::now(),
			captured_pts_90khz: 42,
		}
	}

	/// Spawn `video_dispatch_loop` against a counting transport with the
	/// burst cache deliberately EMPTY at spawn time, so the loop's
	/// start-of-session replay emits nothing and every packet counted
	/// afterwards is an answer to a key frame request.
	fn spawn_video_loop_with_counter() -> (
		tokio::task::JoinHandle<()>,
		Arc<LastFrameBuffer>,
		Arc<KeyframeGate>,
		Arc<AtomicUsize>,
		broadcast::Sender<Frame>,
		CancellationToken,
	) {
		let (frames_tx, frames_rx) = broadcast::channel::<Frame>(32);
		let (transport, sent) = counting_transport_for_tests();
		let tracks = Arc::new(Mutex::new(vec![video_track_with(0xfeed_face, transport)]));
		let last_frame = Arc::new(LastFrameBuffer::new());
		let gate = Arc::new(KeyframeGate::new());
		let cancel = CancellationToken::new();
		let first_video_rtp: FirstVideoRtpSlot = Arc::new(Mutex::new(None));
		let handle = tokio::spawn(video_dispatch_loop(
			frames_rx,
			Arc::clone(&last_frame),
			Arc::clone(&tracks),
			cancel.clone(),
			first_video_rtp,
			Arc::clone(&gate),
			Arc::from("sess-pli"),
		));
		(handle, last_frame, gate, sent, frames_tx, cancel)
	}

	fn video_track_with(ssrc: u32, transport: Arc<dyn Transport>) -> TrackEntry {
		TrackEntry {
			kind: TrackKind::Video,
			transport,
			ssrc,
			clock_rate: 90_000,
		}
	}

	/// Let the spawned loop reach its `select!` and settle again. Same
	/// idiom the other tests in this module use under the paused clock.
	async fn settle() {
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
	}

	async fn stop(handle: tokio::task::JoinHandle<()>, cancel: CancellationToken) {
		cancel.cancel();
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.expect("video loop did not exit on cancel")
			.unwrap();
	}

	#[tokio::test(start_paused = true)]
	async fn pli_replays_cached_burst_exactly_once() {
		// The measured failure (15/11): go2rtc served RTP while the phone's
		// decoder sat on a lost reference because nothing read the PLI. One
		// accepted request must equal one replayed IDR on this session's own
		// transport.
		let (handle, last_frame, gate, sent, _frames, cancel) = spawn_video_loop_with_counter();
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 0, "no replay before the client asks");

		last_frame.replace_video(small_idr_burst());
		assert!(
			gate.handle_inbound_rtcp(&pli_packet(7, 0xfeed_face), "sess-pli", "tcp-interleaved"),
			"a PLI is a key frame request"
		);
		settle().await;
		assert_eq!(
			sent.load(Ordering::SeqCst),
			1,
			"the PLI must replay the cached IDR exactly once"
		);

		stop(handle, cancel).await;
	}

	#[tokio::test(start_paused = true)]
	async fn second_pli_inside_storm_window_replays_nothing() {
		let (handle, last_frame, gate, sent, _frames, cancel) = spawn_video_loop_with_counter();
		settle().await;
		last_frame.replace_video(small_idr_burst());
		let pli = pli_packet(7, 0xfeed_face);
		assert!(gate.handle_inbound_rtcp(&pli, "sess-pli", "tcp-interleaved"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 1);

		// A phone under loss emits ten per second; none of them may add a
		// burst while the window is closed.
		for _ in 0..10 {
			assert!(
				!gate.handle_inbound_rtcp(&pli, "sess-pli", "tcp-interleaved"),
				"the anti-storm window must reject this request"
			);
		}
		settle().await;
		assert_eq!(
			sent.load(Ordering::SeqCst),
			1,
			"no extra burst may go out inside the window"
		);

		// And the window does reopen, so a client that really is still stuck
		// is not silenced forever (paused clock advances Instant::now()).
		tokio::time::sleep(KEYFRAME_REPLAY_MIN_INTERVAL).await;
		assert!(gate.handle_inbound_rtcp(&pli, "sess-pli", "tcp-interleaved"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 2, "the window reopens");

		stop(handle, cancel).await;
	}

	#[tokio::test(start_paused = true)]
	async fn sender_and_receiver_reports_replay_nothing() {
		// A streaming client answers about once a second with an RR whose
		// first octet is 0x81, byte-identical to a PLI's; a compound packet
		// may also start with an SR. Neither may replay, and parsing one
		// must not break anything.
		let (handle, last_frame, gate, sent, _frames, cancel) = spawn_video_loop_with_counter();
		settle().await;
		last_frame.replace_video(small_idr_burst());

		let sr = crate::server::rtcp::build_sender_report(1, 2, 3, 4, 5, 6).to_vec();
		let mut compound = sr;
		compound.extend_from_slice(&rr_packet(7, 0xfeed_face));
		compound.extend_from_slice(&rr_packet(7, 0xfeed_face));
		assert!(!gate.handle_inbound_rtcp(&compound, "sess-pli", "udp"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 0, "reports must not replay anything");
		assert_eq!(gate.accepted(), 0);

		// The session is still healthy: a real request afterwards works.
		assert!(gate.handle_inbound_rtcp(&pli_packet(7, 0xfeed_face), "sess-pli", "udp"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 1, "the loop survived the reports");

		stop(handle, cancel).await;
	}

	#[tokio::test(start_paused = true)]
	async fn pli_with_empty_cache_writes_nothing_and_does_not_panic() {
		// Session opened before the camera produced any IDR: the request is
		// accepted but there is nothing to replay. Zero bytes on the wire,
		// no panic, and live frames keep flowing.
		let (handle, _last_frame, gate, sent, frames_tx, cancel) = spawn_video_loop_with_counter();
		settle().await;
		assert!(gate.handle_inbound_rtcp(&pli_packet(7, 1), "sess-pli", "tcp-interleaved"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 0, "an empty cache must write nothing");
		assert!(!handle.is_finished(), "a PLI must not tear the loop down");

		frames_tx.send(video_frame(true, 90_000)).unwrap();
		settle().await;
		assert_eq!(
			sent.load(Ordering::SeqCst),
			1,
			"the loop is still dispatching live frames after the miss"
		);

		stop(handle, cancel).await;
	}

	#[tokio::test(start_paused = true)]
	async fn pli_replay_leaves_the_live_keyframe_guard_alone() {
		// The guard belongs to the startup path: with an armed guard (startup
		// replay emitted, no live IDR yet) a PLI replay must not clear it —
		// live P-frames stay suppressed until a live IDR, exactly as before
		// this feature existed.
		let (frames_tx, frames_rx) = broadcast::channel::<Frame>(32);
		let (transport, sent) = counting_transport_for_tests();
		let tracks = Arc::new(Mutex::new(vec![video_track_with(0x1234, transport)]));
		let last_frame = Arc::new(LastFrameBuffer::new());
		// Burst already in cache at spawn → startup replay emits one packet
		// and arms the guard.
		last_frame.replace_video(small_idr_burst());
		let gate = Arc::new(KeyframeGate::new());
		let cancel = CancellationToken::new();
		let handle = tokio::spawn(video_dispatch_loop(
			frames_rx,
			Arc::clone(&last_frame),
			Arc::clone(&tracks),
			cancel.clone(),
			Arc::new(Mutex::new(None)),
			Arc::clone(&gate),
			Arc::from("sess-guard"),
		));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 1, "startup replay emitted the base");

		assert!(gate.handle_inbound_rtcp(&pli_packet(7, 1), "sess-guard", "udp"));
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 2, "PLI replay emitted a second base");

		// Live P-frame: still suppressed, because the PLI replay did not
		// touch the guard.
		frames_tx.send(video_frame(false, 100_000)).unwrap();
		settle().await;
		assert_eq!(
			sent.load(Ordering::SeqCst),
			2,
			"a PLI replay must not lift the live-keyframe guard"
		);

		// Live IDR: the guard lifts as it always did.
		frames_tx.send(video_frame(true, 180_000)).unwrap();
		settle().await;
		assert_eq!(sent.load(Ordering::SeqCst), 3, "a live IDR resumes the stream");

		stop(handle, cancel).await;
	}

	// ====== Startup access-unit summary (pure function) ======

	fn stripped_p_frame(pts: u32) -> Frame {
		Frame::Video {
			codec: VideoCodec::H264,
			// What the translator boundary leaves: a slice, no parameter
			// sets (SPS=7 / PPS=8 NAL headers are stripped upstream).
			nals: vec![Bytes::from_static(&[0x41, 0x9a, 0x01])],
			pts_90khz: pts,
			keyframe: false,
			access_unit_end: true,
		}
	}

	fn full_idr_frame(pts: u32) -> Frame {
		Frame::Video {
			codec: VideoCodec::H264,
			nals: vec![
				Bytes::from_static(&[0x67, 0x42, 0x00, 0x1f]), // SPS
				Bytes::from_static(&[0x68, 0xce, 0x01]), // PPS
				Bytes::from_static(&[0x65, 0xaa, 0xbb]), // IDR slice
			],
			pts_90khz: pts,
			keyframe: true,
			access_unit_end: true,
		}
	}

	#[test]
	fn startup_summary_reports_parameter_sets_absent_when_stripped() {
		// The bug's fingerprint: a session fed only slices, no SPS, no PPS.
		// The line must say so out loud, in words an operator can read.
		let frames: Vec<Frame> = (0..12).map(|i| stripped_p_frame(960_000 + i * 3_000)).collect();
		let line = summarize_video_startup(&frames);
		assert!(line.contains("sps=absent"), "must say SPS absent, got: {line}");
		assert!(line.contains("pps=absent"), "must say PPS absent, got: {line}");
		assert!(line.contains("units=12"), "must count the units, got: {line}");
		assert!(line.contains("keyframes=0"), "no IDR here, got: {line}");
		assert!(line.contains("NON_IDR"), "must name the slice type, got: {line}");
		assert!(!line.contains("SPS"), "must not claim an SPS, got: {line}");
		assert!(
			line.contains("pts_90khz=[960000..993000]"),
			"must span first to last pts, got: {line}"
		);
	}

	#[test]
	fn startup_summary_reports_parameter_sets_and_types_when_present() {
		let mut frames = vec![full_idr_frame(1000)];
		frames.extend((1..12).map(|i| stripped_p_frame(1000 + i * 3_000)));
		let line = summarize_video_startup(&frames);
		assert!(line.contains("sps=present"), "must say SPS present, got: {line}");
		assert!(line.contains("pps=present"), "must say PPS present, got: {line}");
		assert!(line.contains("keyframes=1"), "one IDR, got: {line}");
		assert!(line.contains("IDR"), "must name the IDR, got: {line}");
		assert!(line.contains("codec=h264"), "must name the codec, got: {line}");
	}

	#[test]
	fn startup_summary_of_nothing_says_so() {
		let line = summarize_video_startup(&[]);
		assert!(line.contains("units=0"), "got: {line}");
		assert!(line.contains("sps=absent"), "got: {line}");
	}

	#[test]
	fn shrink_for_sample_keeps_only_what_the_summary_reads() {
		// The sample must stay tiny on a main stream, but keep the NAL
		// header byte the type label is derived from.
		let big = Frame::Video {
			codec: VideoCodec::H264,
			nals: vec![
				Bytes::from_static(&[0x67, 0x42, 0x00, 0x1f]),
				Bytes::from(vec![0x65u8; 4096]),
			],
			pts_90khz: 7,
			keyframe: true,
			access_unit_end: true,
		};
		let reduced = shrink_for_sample(&big).expect("video frame");
		let Frame::Video { nals, keyframe, pts_90khz, .. } = &reduced else {
			panic!("expected a video frame back");
		};
		assert_eq!(nals.iter().map(|n| n.len()).sum::<usize>(), 2, "one octet per NAL");
		assert!(nals[1].starts_with(&[0x65]), "the IDR header byte survives");
		assert!(*keyframe && *pts_90khz == 7);
		// And the summary still reads the reduced copy the same way.
		let line = summarize_video_startup(std::slice::from_ref(&reduced));
		assert!(line.contains("sps=present"), "got: {line}");
		assert!(line.contains("IDR"), "got: {line}");
		assert!(shrink_for_sample(&audio_frame(0)).is_none(), "audio is not sampled");
	}

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
		Arc<KeyframeGate>,
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
		let keyframe_gate = Arc::new(KeyframeGate::new());
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
			keyframe_gate,
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
		let keyframe_gate = Arc::new(KeyframeGate::new());
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
			keyframe_gate,
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
