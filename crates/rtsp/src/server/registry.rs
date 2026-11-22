//! Per-connection session registry.
//!
//! An RTSP connection may host zero or more concurrent sessions (rare in
//! practice — ffmpeg uses one session per DESCRIBE/PLAY/TEARDOWN sequence).
//! The registry lets handlers find the session targeted by an RTSP
//! request's `Session:` header.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::server::rtcp::InboundRtcpTrace;
use crate::server::transport::Transport;

/// Shared slot holding the `(sequence_number, rtp_timestamp)` of the first
/// video RTP packet sent on a session. Populated by the session send loop
/// and read by the PLAY handler to emit `RTP-Info:`.
pub type FirstVideoRtpSlot = Arc<Mutex<Option<(u16, u32)>>>;

/// Kind of media carried by a [`TrackEntry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackKind {
	/// Video track (H.264 / H.265).
	Video,
	/// Audio track (AAC / G.711).
	Audio,
}

/// A single track within a session. A session holds one entry per SETUP
/// call that succeeded. A session may host multiple RTP tracks — at
/// least video, plus optional audio — with additional tracks appended
/// after the first SETUP and picked up by the session send loop without
/// needing to restart it.
pub struct TrackEntry {
	/// Which media kind this track carries.
	pub kind: TrackKind,
	/// Write-side transport for this track's RTP/RTCP packets.
	pub transport: Arc<dyn Transport>,
	/// SSRC echoed in the SETUP Transport response and used by the session
	/// send loop when framing RTP packets for this track.
	pub ssrc: u32,
	/// RTP clock rate in Hz. Video uses 90000 (RTP convention for H.264
	/// / H.265). Audio uses the codec sample rate (16000 for AAC-LC on
	/// Argus, 8000 for G.711 µ-law). Used by the session send loop to
	/// extrapolate RTP timestamps at RTCP SR fire time so the SR's
	/// (NTP, RTP) pair refers to the same instant per RFC 3550 §6.4.1.
	pub clock_rate: u32,
}

/// Per-session state tracked by the registry.
pub struct SessionEntry {
	/// Cancellation token for the session send loop — cancelled on TEARDOWN
	/// or connection drop.
	pub cancel: CancellationToken,
	/// The opaque SDP-negotiated session controller (hands off the
	/// `SubscriptionHandle` so its Drop can release the wake lock).
	pub subscription: crate::provider::SubscriptionHandle,
	/// Timestamp of the most recent request that referenced this session.
	/// Used by [`SessionRegistry::sweep_expired`] to drop idle sessions so
	/// the wake lock is released when a client disappears without sending
	/// TEARDOWN.
	pub last_activity: Mutex<Instant>,
	/// Tracks attached to this session via one or more SETUP calls. One
	/// track per SETUP. Wrapped in `Arc<Mutex<_>>` so the session send
	/// loop (which holds its own `Arc` clone) observes appends made by a
	/// second SETUP on the next loop iteration, without having to
	/// restart the task.
	pub tracks: Arc<Mutex<Vec<TrackEntry>>>,
	/// `(sequence_number, rtp_timestamp)` of the first RTP packet sent on
	/// the session's video track. Populated by the session send loop on
	/// its first `Frame::Video` transmission and read by the PLAY handler
	/// to render the `RTP-Info:` response header (RFC 2326 §12.33).
	///
	/// `None` until the first packet is on the wire. If PLAY is answered
	/// before that (rare — the send loop starts at SETUP), the header is
	/// omitted; most clients tolerate the absence.
	pub first_video_rtp: FirstVideoRtpSlot,
	/// Gate through which this session's inbound RTCP reaches its
	/// video dispatch loop (key frame requests: PLI / FIR). Created
	/// with the entry; the SETUP handler hands the same `Arc` to the
	/// track's inbound-RTCP path, and the send loop subscribes to it.
	pub keyframe_gate: Arc<KeyframeGate>,
	/// Notifies the session send loop whenever a track is appended to
	/// `tracks` after the session was created. Fired by
	/// [`append_track`](Self::append_track) and its [`SessionRegistry`]
	/// wrapper. Listened for by the session loop's `select!` (B3), which
	/// then rebuilds its `RuntimeTrack` snapshot — no per-frame poll
	/// needed.
	pub tracks_changed: Arc<tokio::sync::Notify>,
	/// Gate that releases the session send loop on PLAY, per RFC 2326
	/// §10.4/§10.5: a server MUST NOT send media data until PLAY.
	/// Without this gate the send loop replays the cached burst +
	/// live frames immediately on SETUP, which confuses downstream
	/// RTSP re-publishers (observed live: HA's go2rtc `ffmpeg:` wrap
	/// broke its downstream RECORD pipe after the first flushed
	/// video packet because RTP arrived between the two SETUPs).
	/// Fired once by the PLAY handler via `mark_playing`.
	pub play_signal: Arc<tokio::sync::Notify>,
	/// Has PLAY been issued for this session? Paired with
	/// `play_signal` so the session task can skip the wait if PLAY
	/// fired BEFORE the task parked on the notify. Ordering: set
	/// flag first, then fire notify — the waiter checks the flag
	/// after registering for the notify, so it can't miss the edge.
	pub play_fired: Arc<AtomicBool>,
}

impl SessionEntry {
	/// Construct a new entry with a fresh `last_activity` and a freshly
	/// allocated `first_video_rtp` slot. `tracks` holds whatever initial
	/// tracks the caller has already negotiated (normally exactly one
	/// Video track from the first SETUP; an audio track may be appended
	/// later via [`append_track`](Self::append_track)).
	pub fn new(
		cancel: CancellationToken,
		subscription: crate::provider::SubscriptionHandle,
		tracks: Vec<TrackEntry>,
	) -> Self {
		Self {
			cancel,
			subscription,
			last_activity: Mutex::new(Instant::now()),
			tracks: Arc::new(Mutex::new(tracks)),
			first_video_rtp: Arc::new(Mutex::new(None)),
			keyframe_gate: Arc::new(KeyframeGate::new()),
			tracks_changed: Arc::new(Notify::new()),
			play_signal: Arc::new(Notify::new()),
			play_fired: Arc::new(AtomicBool::new(false)),
		}
	}

	/// Append a track to this session's track list. Called by the SETUP
	/// handler when a client issues a second SETUP against an existing
	/// session ID to attach its audio track.
	pub fn append_track(&self, track: TrackEntry) {
		self.tracks
			.lock()
			.expect("tracks lock poisoned")
			.push(track);
		// Order matters: notify AFTER push so a waiter woken by the
		// notify sees the new track on its next snapshot rebuild.
		self.tracks_changed.notify_one();
	}

	/// Mark the session as playing and release the session send loop.
	/// Idempotent — multiple PLAYs on the same session (client resend
	/// or paused→play) just refresh the notify. The flag survives so
	/// subsequent send-loop iterations don't re-park.
	pub fn mark_playing(&self) {
		self.play_fired.store(true, Ordering::SeqCst);
		self.play_signal.notify_waiters();
	}
}

impl Drop for SessionEntry {
	fn drop(&mut self) {
		// Belt-and-suspenders: any code path that drops a SessionEntry
		// without going through remove/clear/sweep_expired still cancels
		// the send loop. Idempotent with the explicit cancel() calls on
		// those paths.
		self.cancel.cancel();
	}
}

/// Concurrent session map keyed by RTSP Session ID.
pub struct SessionRegistry {
	entries: Mutex<HashMap<String, SessionEntry>>,
}

impl SessionRegistry {
	/// Create an empty registry.
	pub fn new() -> Self {
		Self {
			entries: Mutex::new(HashMap::new()),
		}
	}

	/// Insert a new session.
	pub fn insert(&self, id: String, entry: SessionEntry) {
		self.entries
			.lock()
			.expect("registry lock poisoned")
			.insert(id, entry);
	}

	/// Cancel and remove a session by ID. Returns the removed entry if any.
	pub fn remove(&self, id: &str) -> Option<SessionEntry> {
		let entry = self
			.entries
			.lock()
			.expect("registry lock poisoned")
			.remove(id)?;
		entry.cancel.cancel();
		Some(entry)
	}

	/// Drop all sessions (cancels all) — called on connection close.
	pub fn clear(&self) {
		let entries: HashMap<String, SessionEntry> =
			std::mem::take(&mut self.entries.lock().expect("registry lock poisoned"));
		for (_, entry) in entries {
			entry.cancel.cancel();
		}
	}

	/// Does a session with this ID exist?
	pub fn contains(&self, id: &str) -> bool {
		self.entries
			.lock()
			.expect("registry lock poisoned")
			.contains_key(id)
	}

	/// True when no sessions are currently registered. Drives the
	/// connection-level slow-loris timer: once at least one session
	/// exists, the keepalive watchdog handles idle reaping; before
	/// that, the connection-level deadline is the only protection
	/// against a slot-hogging client.
	pub fn is_empty(&self) -> bool {
		self.entries
			.lock()
			.expect("registry lock poisoned")
			.is_empty()
	}

	/// Clone the `Arc` holding the first-video-packet (seq, rtptime) slot
	/// for the given session, if it exists. Returns `None` if the session
	/// is not registered. The returned Arc may still contain `None`
	/// inside its mutex until the session task has sent its first video
	/// packet.
	pub fn first_video_rtp(&self, id: &str) -> Option<FirstVideoRtpSlot> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries
			.get(id)
			.map(|e| std::sync::Arc::clone(&e.first_video_rtp))
	}

	/// Clone the `Arc` holding the track list for the given session, if
	/// it exists. Returns `None` if the session is not registered. Used
	/// by the session send-loop spawn so that tracks appended later by
	/// [`append_track`](Self::append_track) become visible to the loop
	/// on its next iteration.
	pub fn tracks_arc(&self, id: &str) -> Option<Arc<Mutex<Vec<TrackEntry>>>> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries.get(id).map(|e| Arc::clone(&e.tracks))
	}

	/// Clone the `Arc<Notify>` fired on track append for the given session.
	/// Returned so the session send-loop spawn can await appends without
	/// per-iteration polling.
	pub fn tracks_changed_arc(&self, id: &str) -> Option<Arc<tokio::sync::Notify>> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries.get(id).map(|e| Arc::clone(&e.tracks_changed))
	}

	/// Clone the `(Arc<Notify>, Arc<AtomicBool>)` pair that gates the
	/// session send loop on PLAY. Returned so the spawn can park until
	/// the PLAY handler fires `mark_playing`.
	pub fn play_gate_arc(&self, id: &str) -> Option<(Arc<tokio::sync::Notify>, Arc<AtomicBool>)> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries
			.get(id)
			.map(|e| (Arc::clone(&e.play_signal), Arc::clone(&e.play_fired)))
	}

	/// Clone the session's [`KeyframeGate`], or `None` when the
	/// session is not registered. The SETUP handler calls this to wire
	/// the track's inbound RTCP — the interleaved channel map for TCP,
	/// a reader task for UDP — once the entry exists.
	pub fn keyframe_gate(&self, id: &str) -> Option<Arc<KeyframeGate>> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries.get(id).map(|e| Arc::clone(&e.keyframe_gate))
	}

	/// Clone the session's cancellation token, or `None` when the
	/// session is not registered. Cloning a [`CancellationToken`] shares
	/// its cancel state, so a task holding a clone exits exactly when
	/// the session is torn down. The UDP inbound-RTCP reader needs this:
	/// it must not outlive the session whose RTCP port it reads.
	pub fn cancel_token(&self, id: &str) -> Option<CancellationToken> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries.get(id).map(|e| e.cancel.clone())
	}

	/// Mark a session as playing — called by the PLAY handler. Idempotent.
	pub fn mark_playing(&self, id: &str) {
		let entries = self.entries.lock().expect("registry lock poisoned");
		if let Some(e) = entries.get(id) {
			e.mark_playing();
		}
	}

	/// Look up the audio sample rate advertised in the session's SDP, if
	/// audio was present in the subscription. Returns `None` if the
	/// session is not registered or the subscription's SDP had no audio
	/// track. Used by the SETUP append path to stamp the right clock
	/// rate on late-attached audio [`TrackEntry`]s for RTCP SR
	/// extrapolation.
	pub fn audio_sample_rate(&self, id: &str) -> Option<u32> {
		let entries = self.entries.lock().expect("registry lock poisoned");
		entries.get(id).and_then(|e| {
			e.subscription
				.sdp_params
				.audio
				.as_ref()
				.map(|a| a.sample_rate)
		})
	}

	/// Append a track to an existing session. No-op if the session ID is
	/// unknown — callers should verify via [`contains`](Self::contains)
	/// first when that matters.
	pub fn append_track(&self, id: &str, track: TrackEntry) {
		let entries = self.entries.lock().expect("registry lock poisoned");
		if let Some(entry) = entries.get(id) {
			entry.append_track(track);
		}
	}

	/// Mark the session identified by `id` as freshly active.
	///
	/// Called by the connection loop whenever an RTSP request carrying a
	/// matching `Session:` header is dispatched. If the session is not
	/// registered (e.g. already torn down) the call is a no-op.
	pub fn touch(&self, id: &str) {
		let entries = self.entries.lock().expect("registry lock poisoned");
		if let Some(entry) = entries.get(id) {
			*entry
				.last_activity
				.lock()
				.expect("last_activity lock poisoned") = Instant::now();
		}
	}

	/// Remove every session whose most recent activity is older than
	/// `max_idle`. Returns the IDs that were removed so the caller can log
	/// them. Each removed session has its cancellation token fired and its
	/// [`SubscriptionHandle`] dropped (releasing the wake lock).
	///
	/// The registry lock is held only long enough to drain the expired
	/// entries into a local `Vec`; token cancellation and the subsequent
	/// `Drop` side effects run with the lock released to avoid re-entrancy
	/// risks.
	///
	/// [`SubscriptionHandle`]: crate::provider::SubscriptionHandle
	pub fn sweep_expired(&self, max_idle: Duration) -> Vec<String> {
		let expired: Vec<(String, SessionEntry)> = {
			let mut entries = self.entries.lock().expect("registry lock poisoned");
			let expired_ids: Vec<String> = entries
				.iter()
				.filter_map(|(id, entry)| {
					let last = *entry
						.last_activity
						.lock()
						.expect("last_activity lock poisoned");
					if last.elapsed() > max_idle {
						Some(id.clone())
					} else {
						None
					}
				})
				.collect();
			expired_ids
				.into_iter()
				.filter_map(|id| entries.remove(&id).map(|e| (id, e)))
				.collect()
		};
		let mut removed_ids = Vec::with_capacity(expired.len());
		for (id, entry) in expired {
			entry.cancel.cancel();
			removed_ids.push(id);
		}
		removed_ids
	}
}

impl Default for SessionRegistry {
	fn default() -> Self {
		Self::new()
	}
}

/// Shortest spacing between two accepted key frame requests on one session.
///
/// A phone on a lossy radio link asks again, and often: a WebRTC client
/// emits a PLI per lost frame group, which under real loss means several per
/// second, and RFC 5104 §4.3.1.3 lets a FIR be repeated every RTT (while
/// §4.3.1.2 says FIR is not a picture-loss reaction, so a confused client may
/// add FIRs on top). Every accepted request replays one H.264 IDR onto that
/// same lossy uplink — the sub-stream IDR is a few tens of kilobytes, a
/// main-stream one several hundred. Without a floor, a request storm pushes
/// IDR after IDR over the link that is already dropping packets, starving the
/// live media behind it and flooding the journal.
///
/// Three seconds, because that is longer than the camera's own IDR cadence:
/// Argus runs 1-2 s GOPs (see
/// [`crate::buffer::LastFrameBuffer::append_pframe`]), so a suppressed
/// request is answered by the camera's next natural key frame inside the
/// window anyway. Nothing the operator needs is withheld — only the duplicate
/// replay is dropped.
///
/// A named constant on purpose, not a config key: a new key means a module
/// schema entry, a settings-form field and a documentation page, and none of
/// that is worth it before the field tells us 3 s is the wrong number.
pub const KEYFRAME_REPLAY_MIN_INTERVAL: Duration = Duration::from_secs(3);

/// Per-session gate between "inbound RTCP was read from the client" and
/// "that session's video dispatch loop".
///
/// Why a `watch` channel: the signal is a level, not a stream. The reader
/// side (the TCP read loop routing `$`-framed packets, or the UDP RTCP
/// reader task) never needs to know how many requests came in, and the
/// consumer must not be able to miss one while it is busy writing an IDR to
/// the wire. `Notify::notify_waiters` would lose a request landing between
/// two `select!` registrations — which is exactly why the PLAY gate below
/// pairs its notify with an explicit flag — while a watch keeps its latest
/// value, so the video loop always sees a count it has not acted on yet and
/// `Receiver::changed` resolves immediately when the value already moved.
///
/// The per-session shared-slot shape mirrors [`FirstVideoRtpSlot`]: one
/// `Arc` created with the registry entry, handed to whoever must reach the
/// send loop.
pub struct KeyframeGate {
	/// Count of accepted (non-suppressed) requests. Monotonic, and the
	/// value the video dispatch loop watches.
	requests: watch::Sender<u64>,
	/// When the last request was accepted. `std::sync::Mutex`, not tokio's:
	/// the critical section reads two `Instant`s and never spans an await —
	/// the same convention as [`SessionEntry::last_activity`].
	last_accept: Mutex<Option<tokio::time::Instant>>,
	/// Arrival trace behind the once-per-session `keyframe-rtcp` line: every
	/// inbound RTCP buffer this session is handed, on either transport, whatever
	/// it carried. One atomic counter, so neither inbound path ever blocks on it
	/// and a lost update would cost a digit, never a wake-up.
	rtcp_trace: InboundRtcpTrace,
}

impl KeyframeGate {
	/// Fresh gate with no request recorded yet.
	pub fn new() -> Self {
		Self {
			requests: watch::channel(0).0,
			last_accept: Mutex::new(None),
			rtcp_trace: InboundRtcpTrace::default(),
		}
	}

	/// Subscription for the session's video dispatch loop.
	pub fn subscribe(&self) -> watch::Receiver<u64> {
		self.requests.subscribe()
	}

	/// How many requests have been accepted so far.
	pub fn accepted(&self) -> u64 {
		*self.requests.borrow()
	}

	/// How many inbound RTCP buffers this session has been handed, on either
	/// transport, whatever they carried. Zero across a whole session means the
	/// client never put a single packet on the RTCP path — the fact the one
	/// `keyframe-rtcp` line exists to make readable, and the number a session-end
	/// summary would print if this crate grew one.
	pub fn rtcp_arrived(&self) -> u64 {
		self.rtcp_trace.arrived()
	}

	/// Inspect one inbound RTCP compound packet, exactly as read from either
	/// transport (the `$`-framed payload on TCP, the datagram on UDP).
	///
	/// Recognises PLI/FIR through
	/// [`crate::server::rtcp::find_keyframe_request`], applies
	/// [`KEYFRAME_REPLAY_MIN_INTERVAL`], and on acceptance bumps the count
	/// the video dispatch loop watches — which is what replays the cached
	/// burst on the requesting client's own transport.
	///
	/// Returns `true` when a replay is now owed. Everything it does not
	/// understand — SR, RR, NACK, junk — returns `false` without a word beyond
	/// the one line described below: a streaming client sends a receiver report
	/// every second and that must not reach the journal. A recognised request logs
	/// exactly one `debug` line, prefixed `keyframe-request` for grep-ability,
	/// carrying the session and the transport it arrived on; the anti-storm window
	/// caps that line's rate too, which is the other half of why it exists.
	///
	/// The one exception is the first arrival of a session, whatever it carries:
	/// a single `debug` line prefixed `keyframe-rtcp`, see [`InboundRtcpTrace`].
	/// It is what makes an empty journal mean something. Before it, a session with
	/// no line at all was ambiguous between "the browser never asked for a key
	/// frame" and "go2rtc never forwarded the ask"; one line naming a receiver
	/// report says the RTCP path is alive and that no PLI is coming down it. And
	/// still nothing at all is logged when nothing arrives — that silence is the
	/// measurement, so there is no periodic "heard nothing" line to dilute it.
	pub fn handle_inbound_rtcp(&self, packet: &[u8], session: &str, transport: &str) -> bool {
		// Counted off the raw octets, before the parser and independently of it: this
		// half only cares that a packet arrived, not what it asked for. The parser
		// below still recognises PLI and FIR and nothing else.
		if let Some(line) = self.rtcp_trace.note(transport, packet) {
			tracing::debug!("keyframe-rtcp session={session} {line}");
		}
		let Some(kind) = crate::server::rtcp::find_keyframe_request(packet) else {
			return false;
		};
		let now = tokio::time::Instant::now();
		let count = {
			let mut last = self.last_accept.lock().expect("keyframe gate lock poisoned");
			if let Some(prev) = *last {
				if now.saturating_duration_since(prev) < KEYFRAME_REPLAY_MIN_INTERVAL {
					return false;
				}
			}
			*last = Some(now);
			// Window check and increment are one decision under this mutex,
			// so two readers (TCP interleaved + UDP RTCP) on one session can
			// never both pass the window on the same instant.
			//
			// `send_modify`, not `send`: `send` refuses when no receiver
			// exists yet and leaves the stored value untouched, and a client
			// can ask for a key frame before the session task has reached its
			// `select!`. The level must stay pending for the consumer instead
			// of evaporating in that gap.
			let mut accepted = 0u64;
			self.requests.send_modify(|v| {
				*v = v.wrapping_add(1);
				accepted = *v;
			});
			accepted
		};
		tracing::debug!(
			"keyframe-request accepted session={session} transport={transport} kind={} requests={count}",
			kind.label(),
		);
		true
	}
}

impl Default for KeyframeGate {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// ============ KeyframeGate: inbound RTCP into a replay request ============

	/// Hand-built PLI (RFC 4585 §6.3.1, 12 octets).
	fn pli_bytes(sender: u32, media_ssrc: u32) -> Vec<u8> {
		let mut p = vec![0x81, 206, 0x00, 0x02];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&media_ssrc.to_be_bytes());
		p
	}

	/// Hand-built single-FCI FIR (RFC 5104 §4.3.1.1, 20 octets).
	fn fir_bytes(sender: u32, target: u32) -> Vec<u8> {
		let mut p = vec![0x84, 206, 0x00, 0x04];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&0u32.to_be_bytes());
		p.extend_from_slice(&target.to_be_bytes());
		p.extend_from_slice(&[0x07, 0x00, 0x00, 0x00]);
		p
	}

	// ============ keyframe-rtcp: the line the field will be grepped for ============

	/// A `tracing` subscriber that keeps the rendered text of every event handed
	/// to it, so a test can assert on what reached the journal instead of on
	/// internal state. Written against `tracing` alone: this crate has no
	/// subscriber dependency, and adding one to test a log line would drag the
	/// whole tree along for it.
	#[derive(Default)]
	struct EventCapture {
		lines: Arc<Mutex<Vec<String>>>,
	}

	impl tracing::Subscriber for EventCapture {
		fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
			true
		}

		fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
			tracing::span::Id::from_u64(0)
		}

		fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

		fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

		fn event(&self, event: &tracing::Event<'_>) {
			let mut visitor = MessageCapture::default();
			event.record(&mut visitor);
			self.lines
				.lock()
				.expect("capture lock poisoned")
				.push(visitor.text);
		}

		fn enter(&self, _span: &tracing::span::Id) {}

		fn exit(&self, _span: &tracing::span::Id) {}
	}

	/// Pulls the rendered message out of an event: `debug!("... {}", x)` records it
	/// as the field named `message`, which is the half a log reader reads.
	#[derive(Default)]
	struct MessageCapture {
		text: String,
	}

	impl tracing::field::Visit for MessageCapture {
		fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
			if field.name() == "message" {
				self.text = format!("{value:?}");
			}
		}
	}

	/// The messages the capture kept that carry the grep keyword.
	fn keyframe_rtcp_lines(lines: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
		lines
			.lock()
			.expect("capture lock poisoned")
			.iter()
			.filter(|line| line.contains("keyframe-rtcp"))
			.cloned()
			.collect()
	}

	#[test]
	fn first_inbound_rtcp_packet_writes_one_keyframe_rtcp_line() {
		// The line this exists for. A Sender Report is not a request, so the gate
		// must stay shut — and it must still be VISIBLE. That is exactly the pair the
		// field cannot tell apart today: "the browser never asked" and "nothing
		// reached us" both look like an empty journal.
		let lines = Arc::new(Mutex::new(Vec::<String>::new()));
		let gate = KeyframeGate::new();
		let sr = crate::server::rtcp::build_sender_report(1, 2, 3, 4, 5, 6).to_vec();
		let rr = {
			let mut p = vec![0x81, 201, 0x00, 0x07];
			p.extend_from_slice(&[0u8; 28]);
			p
		};
		tracing::subscriber::with_default(EventCapture { lines: Arc::clone(&lines) }, || {
			assert!(
				!gate.handle_inbound_rtcp(&sr, "sess-rtcp", "tcp-interleaved"),
				"an SR is not a request"
			);
			assert!(
				!gate.handle_inbound_rtcp(&rr, "sess-rtcp", "tcp-interleaved"),
				"an RR is not a request either"
			);
			assert!(
				gate.handle_inbound_rtcp(&pli_bytes(1, 2), "sess-rtcp", "tcp-interleaved"),
				"a PLI still is: the trace changed nothing about recognition"
			);
		});
		let traced = keyframe_rtcp_lines(&lines);
		assert_eq!(
			traced.len(),
			1,
			"one line per session, whatever arrives after it: {traced:?}"
		);
		let line = &traced[0];
		assert!(line.starts_with("keyframe-rtcp session=sess-rtcp"), "{line}");
		assert!(line.contains("stage=first"), "{line}");
		assert!(line.contains("transport=tcp-interleaved"), "{line}");
		assert!(line.contains("pt=200(SR)"), "{line}");
		assert!(line.contains("len=28"), "{line}");
		assert!(line.contains("rtcp_total=1"), "{line}");
		assert_eq!(gate.rtcp_arrived(), 3, "all three arrivals were counted");
		assert_eq!(gate.accepted(), 1, "the PLI opened the gate as before");
	}

	#[test]
	fn junk_on_the_rtcp_port_is_visible_too() {
		// Whatever arrives is evidence that the path is alive, including a buffer
		// that is not RTCP at all. Refusing to log it would put junk back in the same
		// silence as nothing, which is the ambiguity this line removes.
		let lines = Arc::new(Mutex::new(Vec::<String>::new()));
		let gate = KeyframeGate::new();
		tracing::subscriber::with_default(EventCapture { lines: Arc::clone(&lines) }, || {
			assert!(
				!gate.handle_inbound_rtcp(&[0x00, 0xf9], "sess-junk", "udp"),
				"junk opens nothing"
			);
		});
		let traced = keyframe_rtcp_lines(&lines);
		assert_eq!(traced.len(), 1, "{traced:?}");
		assert!(traced[0].contains("pt=other<249>"), "{}", traced[0]);
		assert!(traced[0].contains("transport=udp"), "{}", traced[0]);
		assert_eq!(gate.accepted(), 0, "and still no replay is owed");
	}

	#[test]
	fn silence_stays_silent() {
		// No arrival, no line — not even a periodic "still nothing". The absence of
		// the line is itself the measurement, so nothing may dilute it.
		let lines = Arc::new(Mutex::new(Vec::<String>::new()));
		let gate = KeyframeGate::new();
		tracing::subscriber::with_default(EventCapture { lines: Arc::clone(&lines) }, || {
			assert_eq!(gate.rtcp_arrived(), 0, "an idle gate has counted nothing");
		});
		assert!(
			lines.lock().expect("capture lock poisoned").is_empty(),
			"an idle gate writes nothing to the journal"
		);
	}

	#[tokio::test(start_paused = true)]
	async fn gate_accepts_one_request_then_suppresses_the_storm() {
		let gate = KeyframeGate::new();
		let pli = pli_bytes(1, 2);
		assert!(
			gate.handle_inbound_rtcp(&pli, "sess-gate", "tcp-interleaved"),
			"the first PLI must be accepted"
		);
		assert_eq!(gate.accepted(), 1);
		// A storm: nine more in the same instant, none may pass.
		for _ in 0..9 {
			assert!(!gate.handle_inbound_rtcp(&pli, "sess-gate", "tcp-interleaved"));
		}
		assert_eq!(gate.accepted(), 1, "the window must swallow the storm");
		// Past the window the same client may ask again. Paused tokio clock:
		// the sleep advances `tokio::time::Instant::now()` with it, so this
		// is deterministic and costs no wall time.
		tokio::time::sleep(KEYFRAME_REPLAY_MIN_INTERVAL).await;
		assert!(
			gate.handle_inbound_rtcp(&pli, "sess-gate", "tcp-interleaved"),
			"the window must reopen"
		);
		assert_eq!(gate.accepted(), 2);
	}

	#[test]
	fn gate_ignores_reports_and_junk_and_honours_pli_and_fir() {
		// SR and RR are what a client really sends every second: they must
		// not open the gate. PLI and FIR must.
		let sr = crate::server::rtcp::build_sender_report(1, 2, 3, 4, 5, 6).to_vec();
		let mut rr = vec![0x81, 201, 0x00, 0x07];
		rr.extend_from_slice(&[0u8; 28]);
		let gate = KeyframeGate::new();
		assert!(!gate.handle_inbound_rtcp(&sr, "s", "udp"), "SR is not a request");
		assert!(!gate.handle_inbound_rtcp(&rr, "s", "udp"), "RR is not a request");
		assert!(!gate.handle_inbound_rtcp(&[0x00, 0x01], "s", "udp"), "junk is not a request");
		assert_eq!(gate.accepted(), 0, "no report may open the gate");
		assert!(gate.handle_inbound_rtcp(&pli_bytes(1, 2), "s", "udp"), "PLI is a request");
		// FIR gets its own gate: the window above is now armed, and a FIR
		// suppressed by it would prove nothing about recognition.
		let fir_gate = KeyframeGate::new();
		assert!(fir_gate.handle_inbound_rtcp(&fir_bytes(1, 2), "s", "udp"), "FIR is a request");
	}

	#[test]
	fn session_entry_gate_is_shared_and_wakes_the_subscriber() {
		// The gate is per session and shared by `Arc` clone: SETUP hands one
		// clone to the inbound-RTCP path, the send loop subscribes to the
		// same channel, so both must see one count.
		let entry =
			SessionEntry::new(CancellationToken::new(), stub_subscription(), vec![track()]);
		let mut rx = entry.keyframe_gate.subscribe();
		assert_eq!(*rx.borrow_and_update(), 0, "a fresh session owes no replay");
		assert!(!rx.has_changed().expect("channel open"));
		entry
			.keyframe_gate
			.handle_inbound_rtcp(&pli_bytes(1, 2), "s", "tcp-interleaved");
		assert!(rx.has_changed().expect("channel open"), "a PLI must wake the video loop");
		assert_eq!(*rx.borrow_and_update(), 1);
		assert_eq!(Arc::clone(&entry.keyframe_gate).accepted(), 1, "one Arc, one count");
	}

	#[test]
	fn keyframe_gate_and_cancel_token_are_reachable_by_session_id() {
		// SETUP wires inbound RTCP after the entry is inserted, so it needs
		// both handles back out of the registry — and a cloned token must
		// cancel together with the session.
		let registry = SessionRegistry::new();
		let cancel = CancellationToken::new();
		registry.insert(
			"sess-wire".to_string(),
			SessionEntry::new(cancel.clone(), stub_subscription(), vec![track()]),
		);
		let gate = registry.keyframe_gate("sess-wire").expect("gate for live session");
		let token = registry.cancel_token("sess-wire").expect("token for live session");
		assert!(gate.handle_inbound_rtcp(&pli_bytes(1, 2), "sess-wire", "udp"));
		assert!(!token.is_cancelled());
		cancel.cancel();
		assert!(token.is_cancelled(), "the reader task must exit with the session");
		assert!(registry.keyframe_gate("nope").is_none());
		assert!(registry.cancel_token("nope").is_none());
	}
	use std::sync::Arc;
	use tokio_util::sync::CancellationToken;

	/// Minimal [`SubscriptionHandle`] stub for registry tests. The receiver
	/// is never driven; it just needs to exist and drop cleanly.
	fn stub_subscription() -> crate::provider::SubscriptionHandle {
		use crate::buffer::LastFrameBuffer;
		use crate::provider::{Frame, SubscriptionHandle};
		use crate::sdp::SdpParams;

		let (_tx, rx) = tokio::sync::broadcast::channel::<Frame>(1);
		SubscriptionHandle {
			frames: rx,
			sdp_params: SdpParams {
				server_ip: "0".into(),
				session_id: "0".into(),
				session_name: "test".into(),
				video: None,
				audio: None,
			},
			last_frame: Arc::new(LastFrameBuffer::new()),
			guard: Box::new(()),
		}
	}

	#[test]
	fn registry_compiles() {
		let _ = SessionRegistry::new();
	}

	#[test]
	fn session_entry_new_holds_initial_tracks() {
		let cancel = CancellationToken::new();
		let track = TrackEntry {
			kind: TrackKind::Video,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0x1234_5678,
			clock_rate: 90_000,
		};
		let entry = SessionEntry::new(cancel, stub_subscription(), vec![track]);
		assert_eq!(entry.tracks.lock().expect("tracks lock").len(), 1);
		assert_eq!(
			entry.tracks.lock().expect("tracks lock")[0].kind,
			TrackKind::Video,
		);
	}

	#[test]
	fn session_entry_append_track_grows_list() {
		let cancel = CancellationToken::new();
		let video = TrackEntry {
			kind: TrackKind::Video,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0xAAAA,
			clock_rate: 90_000,
		};
		let audio = TrackEntry {
			kind: TrackKind::Audio,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0xBBBB,
			clock_rate: 16_000,
		};
		let entry = SessionEntry::new(cancel, stub_subscription(), vec![video]);
		entry.append_track(audio);
		let tracks = entry.tracks.lock().expect("tracks lock");
		assert_eq!(tracks.len(), 2);
		assert_eq!(tracks[0].kind, TrackKind::Video);
		assert_eq!(tracks[1].kind, TrackKind::Audio);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn append_track_fires_tracks_changed_notify() {
		use std::time::Duration;
		use tokio::time::timeout;

		let cancel = CancellationToken::new();
		let video = TrackEntry {
			kind: TrackKind::Video,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0xAAAA,
			clock_rate: 90_000,
		};
		let audio = TrackEntry {
			kind: TrackKind::Audio,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0xBBBB,
			clock_rate: 16_000,
		};
		let entry = SessionEntry::new(cancel, stub_subscription(), vec![video]);
		let tracks = Arc::clone(&entry.tracks);
		let notify = Arc::clone(&entry.tracks_changed);

		// Park a listener. notify_one before notified() is fine —
		// Notify latches one permit.
		let listener = tokio::spawn(async move {
			notify.notified().await;
			// Ordering invariant: when the notify fires, the push MUST
			// already have landed in the Vec. Otherwise B3's session loop
			// could wake, rebuild against a stale list, and miss the
			// appended track.
			let len = tracks.lock().expect("tracks lock poisoned").len();
			assert!(len >= 2, "expected len >= 2 after append; got {len}");
		});

		// Append a track — notify should fire.
		entry.append_track(audio);

		// Listener must complete within a short deadline.
		timeout(Duration::from_millis(500), listener)
			.await
			.expect("append_track must fire tracks_changed notify")
			.expect("spawned task joined ok");
	}

	// ── Registry: insert / sweep / default ───────────────────────────

	fn track() -> TrackEntry {
		TrackEntry {
			kind: TrackKind::Video,
			transport: crate::server::transport::noop_transport_for_tests(),
			ssrc: 0xABCDEF01,
			clock_rate: 90_000,
		}
	}

	#[test]
	fn sweep_expired_removes_and_cancels_stale_entries() {
		let registry = SessionRegistry::new();
		let cancel = CancellationToken::new();
		let entry = SessionEntry::new(cancel.clone(), stub_subscription(), vec![track()]);
		registry.insert("sess-1".to_string(), entry);

		// A zero-duration threshold means "anything at all is too old",
		// so the freshly-inserted entry is immediately expired.
		let removed = registry.sweep_expired(Duration::from_nanos(0));
		assert_eq!(removed, vec!["sess-1".to_string()]);
		assert!(cancel.is_cancelled(), "expired entry's cancel token fires");
	}

	#[test]
	fn sweep_expired_keeps_recent_entries() {
		let registry = SessionRegistry::new();
		let cancel = CancellationToken::new();
		let entry = SessionEntry::new(cancel.clone(), stub_subscription(), vec![track()]);
		registry.insert("sess-recent".to_string(), entry);

		// Threshold far larger than any elapsed time → nothing removed.
		let removed = registry.sweep_expired(Duration::from_secs(3600));
		assert!(removed.is_empty());
		assert!(!cancel.is_cancelled());
	}

	#[test]
	fn registry_default_matches_new() {
		let r: SessionRegistry = Default::default();
		// Empty registry returns no removals on a zero-threshold sweep.
		assert!(r.sweep_expired(Duration::from_secs(0)).is_empty());
	}

	#[test]
	fn clear_drops_all_entries_and_fires_each_cancel_token() {
		// `clear()` is the load-bearing wake-lock release path on TCP
		// connection drop: every session's cancel token must fire so its
		// task tree exits and drops the wake-lock guard.
		let registry = SessionRegistry::new();
		let cancel_a = CancellationToken::new();
		let cancel_b = CancellationToken::new();
		registry.insert(
			"sess-a".to_string(),
			SessionEntry::new(cancel_a.clone(), stub_subscription(), vec![track()]),
		);
		registry.insert(
			"sess-b".to_string(),
			SessionEntry::new(cancel_b.clone(), stub_subscription(), vec![track()]),
		);
		assert!(registry.contains("sess-a"));
		assert!(registry.contains("sess-b"));

		registry.clear();

		assert!(!registry.contains("sess-a"));
		assert!(!registry.contains("sess-b"));
		assert!(registry.is_empty());
		assert!(cancel_a.is_cancelled(), "sess-a cancel token must fire");
		assert!(cancel_b.is_cancelled(), "sess-b cancel token must fire");
	}

	#[test]
	fn clear_on_empty_registry_is_noop() {
		let registry = SessionRegistry::new();
		registry.clear();
		assert!(registry.is_empty());
	}
}
