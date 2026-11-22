//! In-memory UID → address registry populated by `D2R_HB` heartbeats,
//! plus a per-UID session-state map populated by `M2D_Q_R` issuance.
//!
//! Lazy stale-on-lookup — entries are never proactively swept. A registry
//! lookup that finds an entry older than `stale_after` reads as `None`,
//! which is what the wake-handler treats as "camera not registered".
//!
//! Session-state is the bookkeeping the camera-side handshake needs:
//! when the middleman issues a `M2D_Q_R`, it stores the `(token, ac)`
//! pair we put in the reply so the register loop can echo back the same
//! `ac` in the subsequent `R2D_R_R` (live-verify proved cameras
//! anchor to that value).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Hard cap on each in-memory map (registered cameras + session
/// anchors). At design scale the realistic deployment has 1-100
/// cameras; 1024 leaves headroom for unusual fleets without giving an
/// attacker who can reach the wake-server's UDP ports an unbounded
/// memory-amplification vector. Inserts above the cap are refused
/// (logged at `warn` once per overflow).
pub const MAX_MAP_ENTRIES: usize = 1024;

/// Token + ac pair issued during `M2D_Q_R` and echoed in `R2D_R_R`.
#[derive(Debug, Clone, Copy)]
pub struct SessionAnchor {
	/// Echoed back by the camera in `D2R_R`.
	pub token: u64,
	/// Echoed back by the server in `R2D_R_R`.
	pub ac: u32,
	/// When this anchor was created — used for housekeeping (rotate on
	/// re-issue, drop on stale).
	pub issued_at: Instant,
}

/// Shared session-anchor map. Keyed by camera UID (long form including
/// the firmware suffix).
#[derive(Default)]
pub struct SessionAnchors {
	by_uid: Mutex<HashMap<String, SessionAnchor>>,
}

impl SessionAnchors {
	/// Empty constructor for [`std::sync::Arc::new`] sites.
	pub fn new() -> Self {
		Self::default()
	}

	/// Issue or refresh the `(token, ac)` pair for a given UID. Called
	/// during `M2D_Q_R`. Repeated calls overwrite — the camera always
	/// uses the latest pair from the most-recent `M2D_Q_R` it saw.
	///
	/// Returns `true` if the anchor was stored (or refreshed an
	/// existing entry), `false` if the map was already at
	/// [`MAX_MAP_ENTRIES`] AND the UID was new — in that case the
	/// caller should log a warn and drop the request rather than
	/// growing memory unbounded under a flood.
	pub fn issue(&self, uid: &str, token: u64, ac: u32, now: Instant) -> bool {
		let mut map = self.by_uid.lock().expect("SessionAnchors mutex poisoned");
		// Refreshing an existing UID is always allowed (no size growth).
		// Only new inserts are bounded.
		if map.len() >= MAX_MAP_ENTRIES && !map.contains_key(uid) {
			return false;
		}
		map.insert(
			uid.to_string(),
			SessionAnchor {
				token,
				ac,
				issued_at: now,
			},
		);
		true
	}

	/// Drop entries past `stale_after` and return the evicted UIDs.
	/// Mirrors [`CameraRegistry::purge_stale`]. Lazy: callers invoke
	/// from a hot path (e.g. heartbeat handler) so the map size never
	/// drifts past the rate of stale entries created.
	pub fn purge_stale(&self, now: Instant, stale_after: Duration) -> Vec<String> {
		let mut map = self.by_uid.lock().expect("SessionAnchors mutex poisoned");
		let stale: Vec<String> = map
			.iter()
			.filter(|(_, a)| now.saturating_duration_since(a.issued_at) > stale_after)
			.map(|(uid, _)| uid.clone())
			.collect();
		for uid in &stale {
			map.remove(uid);
		}
		stale
	}

	/// Look up the anchor for a UID. Returns `None` if no `M2D_Q_R` has
	/// been issued for this UID, OR if the anchor is older than the
	/// supplied TTL (caller decides the policy).
	pub fn lookup(&self, uid: &str, now: Instant, stale_after: Duration) -> Option<SessionAnchor> {
		self.by_uid
			.lock()
			.expect("SessionAnchors mutex poisoned")
			.get(uid)
			.copied()
			.filter(|a| now.saturating_duration_since(a.issued_at) <= stale_after)
	}

	/// Total number of anchors currently held; observability helper.
	pub fn len(&self) -> usize {
		self.by_uid
			.lock()
			.expect("SessionAnchors mutex poisoned")
			.len()
	}

	/// True when no anchors are held (clippy `len_without_is_empty`).
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// Snapshot of a camera's most recent heartbeat — what the wake handler needs
/// to forward `R2C_T` toward the device.
#[derive(Debug, Clone)]
pub struct CameraEntry {
	/// Source address the heartbeat arrived from; where wake replies are sent.
	pub addr: SocketAddr,
	/// Camera-supplied session token echoed back to operators / future ops
	/// (currently observability only).
	pub token: u64,
	/// Wall-clock-free timestamp used for stale-on-lookup eviction.
	pub last_seen: Instant,
}

/// Concurrent UID → `CameraEntry` table. Cheap to share across tasks; all
/// access is mutex-guarded internally.
#[derive(Default)]
pub struct CameraRegistry {
	cameras: Mutex<HashMap<String, CameraEntry>>,
	/// Per-camera beat accounting, see [`CameraRegistry::note_beat`]. Keyed by the
	/// same UIDs as `cameras` and pruned by the same sweep, so it cannot outgrow the
	/// registry it belongs to.
	/// Keyed by the uid **as the beat carried it**. A camera that ever showed up under
	/// both of its shapes - the 20-character form from the beat and the 16-character one
	/// from a binary header - would get two half windows and two reports, each internally
	/// correct and neither complete. Same property as the entry table above, and it belongs
	/// with the one-directional prefix fallback rather than to a key-fusing fix.
	beats: Mutex<HashMap<String, BeatWindow>>,
}

/// How much beat history one line summarises. Ten minutes is long enough that a
/// reported mean is not dominated by a single wake, and short enough that an
/// unstable period shows up within one reading of it.
pub(crate) const BEAT_REPORT_WINDOW: Duration = Duration::from_secs(600);

/// One camera's window of beats.
#[derive(Debug, Clone, Copy)]
struct BeatWindow {
	start: Instant,
	prev: Instant,
	beats: u32,
	/// Shortest and longest gap seen in this window. The operator's own words for
	/// the beat period were "quite unstable"; a mean alone would have hidden
	/// exactly that, so the spread is reported next to it.
	min_ms: u64,
	max_ms: u64,
}

/// A closed window, ready to log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeatReport {
	pub window_secs: u64,
	/// Beats seen in this window. The **intervals** reported below are
	/// `beats - 1`: a window is bounded by the arrivals it holds.
	pub beats: u32,
	pub min_ms: u64,
	pub max_ms: u64,
	pub mean_ms: u64,
}

impl CameraRegistry {
	/// Empty registry; populated as `D2R_HB` heartbeats arrive.
	pub fn new() -> Self {
		Self::default()
	}

	/// Insert or overwrite. Refreshes `last_seen` to `now`. Returns
	/// `Some(true)` if the UID was newly registered, `Some(false)` if
	/// this refreshed an existing entry, and `None` if the map was
	/// full ([`MAX_MAP_ENTRIES`]) and the UID was new — in that case
	/// the caller should log a warn and drop the request. The cap
	/// stops a flood of D2R_HB-with-random-UIDs from growing memory
	/// unbounded.
	pub fn upsert(&self, uid: &str, addr: SocketAddr, token: u64, now: Instant) -> Option<bool> {
		let mut map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		// Refreshes never grow the map; only new inserts are bounded.
		if map.len() >= MAX_MAP_ENTRIES && !map.contains_key(uid) {
			return None;
		}
		let prev = map.insert(
			uid.to_string(),
			CameraEntry {
				addr,
				token,
				last_seen: now,
			},
		);
		Some(prev.is_none())
	}

	/// Record one beat for `uid` and, when its window closes, hand back the summary
	/// to log. A window opens on the first beat recorded for that uid, not at process
	/// start: a camera that only started beating four minutes ago has four minutes of
	/// evidence, and claiming otherwise would attribute silence to tempo. Returns `None` when the uid is not a registered camera — the window
	/// follows the registry, so a flood of beats for unknown UIDs cannot grow this
	/// map, and a camera evicted as stale stops being counted the moment it stops
	/// being a camera. The clock is an argument, so tests drive windows without
	/// sleeping.
	pub fn note_beat(&self, uid: &str, now: Instant) -> Option<BeatReport> {
		if !self.cameras.lock().expect("CameraRegistry mutex poisoned").contains_key(uid) {
			return None;
		}
		let mut map = self.beats.lock().expect("CameraRegistry beat map poisoned");
		let Some(window) = map.get_mut(uid) else {
			map.insert(
				uid.to_string(),
				BeatWindow {
					start: now,
					prev: now,
					beats: 1,
					min_ms: u64::MAX,
					max_ms: 0,
				},
			);
			return None;
		};
		let gap = now.saturating_duration_since(window.prev).as_millis() as u64;
		window.prev = now;
		window.beats += 1;
		window.min_ms = window.min_ms.min(gap);
		window.max_ms = window.max_ms.max(gap);
		let elapsed = now.saturating_duration_since(window.start);
		if elapsed < BEAT_REPORT_WINDOW {
			return None;
		}
		let total = elapsed.as_millis() as u64;
		let beats = window.beats;
		let report = BeatReport {
			window_secs: total / 1000,
			beats,
			min_ms: window.min_ms,
			max_ms: window.max_ms,
			// The mean of the **same gaps** `min_ms` and `max_ms` bound, so the three
			// numbers in one report answer one question: what did the beat actually do
			// between its first and last arrival. Dividing the window by the beat count
			// instead would mix species - a window of three beats holds two intervals,
			// and a reader comparing a mean against min and max would be comparing a
			// cadence against gaps. A one-beat window holds no interval at all: it
			// reports 0 here and its extremes are not to be read.
			mean_ms: if beats >= 2 {
				total / u64::from(beats - 1)
			} else {
				0
			},
		};
		window.start = now;
		window.prev = now;
		window.beats = 1;
		window.min_ms = u64::MAX;
		window.max_ms = 0;
		Some(report)
	}

	/// Drop entries past `stale_after` and return the evicted UIDs.
	/// Callers log info on each eviction so operators see deregistration.
	/// Lazy: the registry has no background sweeper, so call this from
	/// hot paths (heartbeat / connect handlers) whenever there's
	/// something to log against.
	pub fn purge_stale(&self, now: Instant, stale_after: Duration) -> Vec<String> {
		let mut map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		let stale: Vec<String> = map
			.iter()
			.filter(|(_, e)| now.saturating_duration_since(e.last_seen) > stale_after)
			.map(|(uid, _)| uid.clone())
			.collect();
		for uid in &stale {
			map.remove(uid);
		}
		// Beat windows belong to cameras, not to addresses: whatever leaves the
		// registry stops being counted, so this map cannot accumulate ghosts.
		self.beats
			.lock()
			.expect("CameraRegistry beat map poisoned")
			.retain(|uid, _| !stale.contains(uid));
		stale
	}

	/// Lookup honouring `stale_after`. Entries older than `stale_after`
	/// from `now` read as `None`.
	///
	/// **Prefix-match fallback:** Argus firmware reports UIDs in their
	/// long form (config UID + 4-char firmware suffix) on `D2R_HB`, while
	/// `C2R_C` from bairelay-itself / neolink sends the short configured
	/// UID. If exact match fails, fall back to scanning for any stored
	/// UID that **starts with** the requested UID — that handles the
	/// short→long mismatch without requiring operators to know about the
	/// suffix.
	pub fn lookup_fresh(
		&self,
		uid: &str,
		now: Instant,
		stale_after: Duration,
	) -> Option<CameraEntry> {
		let map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		let fresh = |e: &CameraEntry| now.saturating_duration_since(e.last_seen) <= stale_after;
		if let Some(e) = map.get(uid).filter(|e| fresh(e)) {
			return Some(e.clone());
		}
		map.iter()
			.find(|(stored, e)| stored.starts_with(uid) && fresh(e))
			.map(|(_, e)| e.clone())
	}

	/// Reverse lookup: find the UID + entry whose `D2R_HB` source IP
	/// matches `ip`. Used by the push-listener: the camera's
	/// motion-time TCP connection to `pushx.reolink.com` carries the
	/// same source IP as its UDP heartbeats — different ephemeral port,
	/// same host — so an IP match is the link from "TCP accept on :443"
	/// to "this UID's CameraHandle". Stale entries are excluded.
	pub fn lookup_by_ip(
		&self,
		ip: std::net::IpAddr,
		now: Instant,
		stale_after: Duration,
	) -> Option<(String, CameraEntry)> {
		let map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		map.iter()
			.find(|(_, e)| {
				e.addr.ip() == ip && now.saturating_duration_since(e.last_seen) <= stale_after
			})
			.map(|(uid, e)| (uid.clone(), e.clone()))
	}

	/// Total registered UID count; used by tests + observability.
	pub fn len(&self) -> usize {
		self.cameras
			.lock()
			.expect("CameraRegistry mutex poisoned")
			.len()
	}

	/// Convenience pair to `len()`; clippy expects it whenever `len()` exists.
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[cfg(test)]
mod tests {
	/// A registry with one camera that beat at `t0`.
	fn rig(t0: Instant) -> CameraRegistry {
		let reg = CameraRegistry::new();
		reg.upsert("9527000SYNTHTEST", "192.0.2.7:50124".parse().unwrap(), 7, t0);
		reg
	}

	#[test]
	fn the_cadence_window_closes_only_after_the_full_window() {
		let t0 = Instant::now();
		let reg = rig(t0);
		let mut beat = t0;
		for _ in 0..50 {
			beat += Duration::from_secs(11);
			assert!(reg.note_beat("9527000SYNTHTEST", beat).is_none(), "ten minutes have not passed");
		}
		// The window opened on the first beat above, at t0 + 11 s, so it closes a full
		// window after that beat and not after t0.
		let closed = reg.note_beat("9527000SYNTHTEST", t0 + BEAT_REPORT_WINDOW + Duration::from_secs(11));
		assert!(closed.is_some(), "the window closes once it is at least ten minutes wide");
	}

	/// The reason the spread exists: a period the operator calls unstable is
	/// invisible in a mean. A wake doubles one gap while the mean barely moves.
	#[test]
	fn the_window_reports_the_spread_and_not_only_the_mean() {
		let t0 = Instant::now();
		let reg = CameraRegistry::new();
		// A beat for a uid that is not a camera is not counted at all, so the window
		// below really does open on the second call.
		reg.note_beat("U", t0);
		reg.upsert("U", "192.0.2.8:50124".parse().unwrap(), 1, t0);
		assert!(reg.note_beat("U", t0).is_none(), "the window opens on this beat");
		assert!(reg.note_beat("U", t0 + Duration::from_secs(10)).is_none());
		let report = reg
			.note_beat("U", t0 + BEAT_REPORT_WINDOW + Duration::from_secs(1))
			.expect("the window closes");
		assert_eq!(report.beats, 3, "the beats counted in the window");
		assert_eq!(report.min_ms, 10_000, "the tight gap is reported");
		assert_eq!(report.max_ms, 591_000, "the wake-sized gap is reported, not averaged away");
		assert_eq!(
			report.mean_ms, 300_500,
			"the mean of the two gaps the window holds (10 s and 591 s), not its span over beat count"
		);
		assert_eq!(report.window_secs, 601);
		// Why four numbers and not two: 10 s against 591 s inside the same ten minutes
		// is the instability the operator described, and a mean of 200 s on its own
		// would have hidden it completely.
	}

	#[test]
	fn a_window_never_outlives_its_camera() {
		let t0 = Instant::now();
		let reg = rig(t0);
		reg.note_beat("9527000SYNTHTEST", t0 + Duration::from_secs(5));
		// An unknown uid is not a camera: counting it would let a flood of beats
		// with invented UIDs grow this map.
		assert!(reg.note_beat("9527000GHOST000000", t0).is_none());
		let evicted = reg.purge_stale(t0 + Duration::from_secs(90_000), Duration::from_secs(80));
		assert_eq!(evicted, vec!["9527000SYNTHTEST".to_string()]);
		assert!(
			reg.note_beat("9527000SYNTHTEST", t0 + Duration::from_secs(90_001)).is_none(),
			"an evicted camera stops being counted the moment it stops being a camera"
		);
	}

	#[test]
	fn the_window_starts_over_after_it_reports() {
		let t0 = Instant::now();
		let reg = rig(t0);
		let open = t0 + Duration::from_secs(5);
		reg.note_beat("9527000SYNTHTEST", open);
		let first = reg.note_beat("9527000SYNTHTEST", open + BEAT_REPORT_WINDOW).expect("closes");
		let second = reg
			.note_beat("9527000SYNTHTEST", open + 2 * BEAT_REPORT_WINDOW)
			.expect("closes again");
		assert_eq!(first.beats, 2, "{:?}", first);
		assert_eq!(second.beats, 2, "the counter restarts, it does not accumulate: {:?}", second);
		assert_eq!(second.window_secs, 600);
		assert_eq!(second.min_ms, 600_000, "a single gap in a window is both extremes");
		assert_eq!(second.max_ms, 600_000);
	}

	use super::*;
	use std::net::SocketAddr;

	#[test]
	fn a_two_beat_window_reports_the_single_gap_it_measured() {
		// The smallest case that tells the two definitions of `mean_ms` apart: a window of
		// two beats holds one interval, so the mean must equal both extremes. Dividing the
		// span by the beat count would print half the gap against a max that is the whole
		// gap, and the report line would carry both numbers side by side.
		let t0 = Instant::now();
		let reg = CameraRegistry::new();
		reg.upsert("9527000SYNTHWINDOW", "192.0.2.9:50124".parse().unwrap(), 3, t0);
		assert!(reg.note_beat("9527000SYNTHWINDOW", t0).is_none(), "the window opens here");
		let report = reg
			.note_beat(
				"9527000SYNTHWINDOW",
				t0 + BEAT_REPORT_WINDOW + Duration::from_secs(1),
			)
			.expect("a window this wide closes");
		assert_eq!(report.beats, 2, "{:?}", report);
		assert_eq!(
			report.mean_ms, report.max_ms,
			"one interval: the mean is that interval, not half of it"
		);
		assert_eq!(report.mean_ms, report.min_ms);
		assert!(
			report.mean_ms >= 600_000,
			"the gap is reported whole: {:?}",
			report
		);
	}

	fn addr(s: &str) -> SocketAddr {
		s.parse().unwrap()
	}
	fn now() -> Instant {
		Instant::now()
	}

	#[test]
	fn upsert_then_lookup_returns_entry() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 42, t);
		let e = reg
			.lookup_fresh("UID1", t, Duration::from_secs(60))
			.unwrap();
		assert_eq!(e.addr, addr("10.0.0.1:5000"));
		assert_eq!(e.token, 42);
	}

	#[test]
	fn lookup_missing_returns_none() {
		let reg = CameraRegistry::new();
		assert!(reg
			.lookup_fresh("NOPE", now(), Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn upsert_overwrites_address_and_token() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 42, t);
		let t2 = t + Duration::from_secs(1);
		reg.upsert("UID1", addr("10.0.0.2:6000"), 99, t2);
		let e = reg
			.lookup_fresh("UID1", t2, Duration::from_secs(60))
			.unwrap();
		assert_eq!(e.addr, addr("10.0.0.2:6000"));
		assert_eq!(e.token, 99);
	}

	#[test]
	fn lookup_returns_none_past_ttl() {
		let reg = CameraRegistry::new();
		let inserted = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, inserted);
		let later = inserted + Duration::from_secs(120);
		let stale = Duration::from_secs(60);
		assert!(reg.lookup_fresh("UID1", later, stale).is_none());
	}

	#[test]
	fn lookup_at_ttl_boundary_returns_some() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		let stale = Duration::from_secs(60);
		let on_edge = t + stale;
		assert!(reg.lookup_fresh("UID1", on_edge, stale).is_some());
	}

	#[test]
	fn is_empty_returns_true_when_unpopulated_then_false_after_upsert() {
		let reg = CameraRegistry::new();
		assert!(reg.is_empty());
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, now());
		assert!(!reg.is_empty());
	}

	#[test]
	fn lookup_by_ip_finds_entry_when_ip_matches() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		reg.upsert("UID2", addr("10.0.0.2:6000"), 2, t);
		let (uid, entry) = reg
			.lookup_by_ip("10.0.0.2".parse().unwrap(), t, Duration::from_secs(60))
			.expect("entry for 10.0.0.2");
		assert_eq!(uid, "UID2");
		assert_eq!(entry.token, 2);
	}

	#[test]
	fn lookup_by_ip_returns_none_when_no_ip_matches() {
		let reg = CameraRegistry::new();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, now());
		assert!(reg
			.lookup_by_ip("10.0.0.9".parse().unwrap(), now(), Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn lookup_by_ip_skips_stale_entries() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		let later = t + Duration::from_secs(120);
		assert!(reg
			.lookup_by_ip("10.0.0.1".parse().unwrap(), later, Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn len_tracks_distinct_uids() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("A", addr("10.0.0.1:1"), 1, t);
		reg.upsert("B", addr("10.0.0.2:2"), 2, t);
		reg.upsert("A", addr("10.0.0.3:3"), 3, t);
		assert_eq!(reg.len(), 2);
	}

	#[test]
	fn upsert_returns_some_true_on_first_insert_some_false_on_refresh() {
		let reg = CameraRegistry::new();
		let t = now();
		assert_eq!(reg.upsert("UID1", addr("10.0.0.1:1"), 1, t), Some(true));
		assert_eq!(reg.upsert("UID1", addr("10.0.0.1:1"), 2, t), Some(false));
		assert_eq!(reg.upsert("UID2", addr("10.0.0.2:2"), 3, t), Some(true));
	}

	/// Once the registry is at MAX_MAP_ENTRIES, new UIDs are rejected
	/// (returns None). Refreshes of existing UIDs still succeed —
	/// otherwise legitimate cameras would lose their slot under
	/// attack.
	#[test]
	fn upsert_rejects_new_uid_when_at_capacity_but_allows_refresh() {
		let reg = CameraRegistry::new();
		let t = now();
		// Fill to capacity.
		for i in 0..MAX_MAP_ENTRIES {
			let uid = format!("UID{i}");
			assert_eq!(
				reg.upsert(&uid, addr(&format!("10.0.0.{}:1", i % 250 + 1)), 1, t),
				Some(true)
			);
		}
		// New UID is rejected.
		assert_eq!(reg.upsert("OVERFLOW", addr("10.0.0.1:1"), 1, t), None);
		// Existing UID can still refresh (legitimate camera survives).
		assert_eq!(
			reg.upsert("UID0", addr("10.0.0.1:1"), 99, t + Duration::from_secs(1)),
			Some(false)
		);
	}

	#[test]
	fn purge_stale_evicts_only_past_ttl_and_returns_uids() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("OLD", addr("10.0.0.1:1"), 1, t);
		reg.upsert("FRESH", addr("10.0.0.2:2"), 2, t + Duration::from_secs(50));
		let later = t + Duration::from_secs(80);
		let evicted = reg.purge_stale(later, Duration::from_secs(60));
		assert_eq!(evicted, vec!["OLD".to_string()]);
		assert_eq!(reg.len(), 1);
		assert!(reg
			.lookup_fresh("FRESH", later, Duration::from_secs(60))
			.is_some());
	}

	#[test]
	fn purge_stale_is_noop_when_nothing_is_stale() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:1"), 1, t);
		assert!(reg.purge_stale(t, Duration::from_secs(60)).is_empty());
		assert_eq!(reg.len(), 1);
	}
}
