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
use tokio_util::sync::CancellationToken;

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

#[cfg(test)]
mod tests {
	use super::*;
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
