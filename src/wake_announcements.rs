//! The bridge's side of the wake announcement the camera pushes by itself.
//!
//! The register server can recognise an announcement but cannot use it: it knows
//! neither the configured cameras nor the read engine. So the binary installs an
//! observer ([`bairelay_wake_server::set_observer`]) and this module drains what
//! the observer hands over, one announcement at a time, into the guarded read of
//! [`crate::polling`].
//!
//! Before this module existed the engine in [`crate::polling`] was complete and
//! had no caller: the packets meant to feed it were classified "bad packet" on the
//! registration port and dropped. A battery camera that woke by itself went back to
//! sleep with its battery level and its settings unread, and the log said nothing,
//! because from every other angle everything was working.
//!
//! What an announcement does *not* do, and this is the contract:
//!
//! - it does not wake a camera on its own account. This module takes the wake
//!   lock of a camera that already announced itself, which is the same gesture
//!   the read burst makes, and no more: no session is opened here, no setting is
//!   read. The one thing it puts on the network is the button press itself,
//!   published through [`crate::camera_tasks::report_doorbell_press`] so that it
//!   shares the dedupe window of every other press source. The session's own alarm
//!   listener does the reading, because it already holds that session's channel.
//! - it cannot fill memory. The hand-off is an unbounded channel so the observer,
//!   which runs inside the listen loop, never waits; it is bounded by hand at
//!   `CHANNEL_MAX`, and past that an announcement is counted and lost instead
//!   of queued.
//! - it does not learn cameras. An announcement naming a uid this bridge does not
//!   configure is remembered in a capped set so the operator can see it, and
//!   nothing else happens.
//!
//! The three rules that decide whether a read happens are separate, because they
//! were measured separately on the network and each answers to a different cost:
//!
//! 1. `read_on_wake` on the camera decides whether its announcements are read at
//!    all.
//! 2. `read_hold_secs` says how long the wake lock is held waiting for the
//!    session, which is the only way a slow camera gets read at all.
//! 3. `read_interval_secs` is the floor between two reads **that got a session**.
//!    A read that got nothing is not charged to it: it cost nothing, so it must
//!    not forbid the next one.
//!
//! Which announcement is a press is a separate question, and it now has a measured
//! answer rather than an inferred one. `event` 4 says an activity ended; `reason`
//! 19 says that activity was the button. The pair *can* be a finger leaving the
//! button, and when the announced activity was short it is: that is the press
//! detector this path exists for, and it stays in place. The same pair is also what
//! the camera sends when the wake session it opened for that button runs out, and
//! then the announcement is the end of the session, not a press. `duration` tells
//! the two apart, so `is_press` reads it and refuses the long one: the press this
//! bridge publishes is the button, never the camera putting itself to sleep.
//!
//! Retransmissions are already merged on the register side, so what arrives here
//! is one announcement per event.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bairelay_neolink_core::bcudp::announcement::Announcement;
use bairelay_neolink_core::redact::UidTail;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::camera::CameraHandle;
use bairelay_mqtt::{SharedMqttClient, StatusPublisher};

use crate::polling::{self, Guard};
use crate::push_listener::match_camera_by_uid;

/// Hand-off cap, held by hand: the channel itself is unbounded so the observer
/// cannot block, and this counter is what keeps it from growing instead. Announced
/// wakes come one at a time, so this is dozens of times what a doorbell can
/// produce; the number bounds the worst case, it does not describe the norm.
const CHANNEL_MAX: usize = 48;

/// How many uids this bridge has seen but does not configure are remembered. The
/// set exists so the operator can see a camera talking that the configuration does
/// not name, and is capped because a peer can invent uids faster than anyone can
/// read them.
const UNKNOWN_UIDS_MAX: usize = 8;

/// `event` 3: the camera says it is awake. `event` 4 closes an activity, and it
/// lands when the activity ends rather than a few seconds after it began: the
/// only one this installation's camera has ever sent outside a press carried
/// `duration=Some(119449)` — two minutes in. Measured or not, neither is it a
/// reason to spend a second session on one wake.
const EVENT_WAKE: u32 = 3;
/// An activity ended.
const EVENT_PRESS_END: u32 = 4;
/// The reason that says the activity was a button press.
const REASON_BUTTON: u32 = 19;

/// The longest `event` 4 / `reason` 19 activity still taken for a button press, in
/// milliseconds. The two ends of the observed range picked this number; nothing in
/// between was guessed.
///
/// The upper end is the reason the bound exists. The only `event` 4 `reason` 19
/// announcement this installation's camera ever sent *outside* a press carried
/// `duration=Some(119449)`, and it landed at 17:31:00 against a wake that opened at
/// 17:29:01 — the announced activity and the wake session are the same 119 seconds,
/// to the second. It is the camera closing the session the button had opened, and
/// publishing a press for it rang the operator's phone for days. Nobody holds a
/// doorbell button for two minutes.
///
/// The lower end is why the bound is not the 10 to 15 seconds that "a press is
/// counted in seconds" suggests. The longest button release ever captured off this
/// firmware is `duration=19_281` ms — that is the `PRESS_FRAME` capture below —
/// with capture D of `crates/core/src/bcudp/announcement.rs` at `18_873` ms, both
/// read off the operator's own camera and both labelled a release. A bound of
/// 15 seconds would refuse both of them, and with them the press of every camera
/// whose button activity runs as long as these did: the detector would have gone
/// silent on real presses to catch one ghost.
///
/// So: 30 seconds clears the longest credible release by half again, and stays
/// under a quarter of the session end that had to be refused. Not 60 either, which
/// is the other round number in range — a session this bridge holds open ends at
/// about two minutes on this installation, so the room between a release and a
/// session end is roughly 19 s to 119 s, and every extra second of bound only lets
/// more end-of-session announcements through while no observed press needed any of
/// it. The one measurement outside this tree points the same way: the module this
/// one descends from records a `reason` 19 announcement 248 753 ms after the press
/// that caused it, the bridge's own session having stretched the recording to four
/// minutes. Session-inflated durations are counted in minutes, releases in tens of
/// seconds, and 30 s sits between the two. Note too that the dedupe window in
/// `crate::status_cache` is 10 seconds, so a press some other path already reported
/// is long gone before this bound matters.
///
/// This is a bound on plausibility, not a dedupe: it refuses what cannot credibly be
/// a press, and proves nothing about what it lets through.
const MAX_PRESS_DURATION_MS: u64 = 30_000;

/// Drain announcements until cancellation and read the camera that announced
/// itself. Call once, from the binary, with the same camera map the MQTT dispatch
/// uses, and install it *before* the register loop starts: an announcement heard
/// before the observer exists is a wake nobody will ever read.
pub async fn run(
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	cancel: CancellationToken,
) {
	let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
	let pending = Arc::new(AtomicUsize::new(0));
	let dropped = Arc::new(AtomicUsize::new(0));
	bairelay_wake_server::set_observer(observer(tx, Arc::clone(&pending), Arc::clone(&dropped)));
	info!(
		capacity = CHANNEL_MAX,
		"wake announcements: draining the camera's own announcements"
	);
	let mut reader = Reader {
		mqtt,
		topic_prefix,
		cameras,
		guards: HashMap::new(),
		unknown_uids: HashSet::new(),
		unknown_overflow: 0,
		cancel: cancel.clone(),
	};
	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			seen = rx.recv() => {
				let Some(announcement) = seen else { break };
				// The observer counted this on the way in, so draining it here is
				// what releases the room.
				pending.fetch_sub(1, Ordering::Relaxed);
				if let Heard::Press(handle) = reader.hear(&announcement) {
					reader.publish_press(&handle).await;
				}
			}
		}
	}
	let dropped = dropped.load(Ordering::Relaxed);
	if dropped > 0 {
		warn!(
			dropped,
			capacity = CHANNEL_MAX,
			"wake announcements: announcements lost while the queue was full"
		);
	}
}

/// The observer handed to the wake server: count, hand over, return.
///
/// It runs inside the register listen loop, so it must wait for nothing. When the
/// queue is full the announcement is counted and dropped: dropping is visible in
/// the log and costs one event, while blocking would cost every camera on the
/// port.
fn observer(
	tx: UnboundedSender<Announcement>,
	pending: Arc<AtomicUsize>,
	dropped: Arc<AtomicUsize>,
) -> impl Fn(Announcement) + Send + Sync + 'static {
	move |announcement: Announcement| {
		if pending.fetch_add(1, Ordering::Relaxed) >= CHANNEL_MAX {
			pending.fetch_sub(1, Ordering::Relaxed);
			let lost = dropped.fetch_add(1, Ordering::Relaxed) + 1;
			warn!(
				tid = announcement.tid(),
				capacity = CHANNEL_MAX,
				dropped = lost,
				"wake announcements: queue full, announcement dropped"
			);
			return;
		}
		if tx.send(announcement).is_err() {
			pending.fetch_sub(1, Ordering::Relaxed);
			debug!("wake announcements: no consumer left, announcement dropped");
		}
	}
}

/// Whether this announcement is a button press.
///
/// `event` 4 marks the end of an activity and `reason` 19 says that activity was
/// the button, so the pair *may* be a finger leaving the button — but only while the
/// announced activity was short enough to have been the button itself. With the pair,
/// the announcement is taken for a press when it carries no `duration` at all, or one
/// at or under [`MAX_PRESS_DURATION_MS`]. Past that it is the camera closing the wake
/// session the button opened, and publishing a press for that rings a chime nobody
/// rang: that is the 119 s announcement of 17:31:00, which was this pair exactly.
///
/// A `duration` of `None` stays a press. Nothing measured says a missing duration
/// ever means "the session is over": every capture on this tree that carried no
/// duration was a plain `{"event":3}`, and the only long one ever seen carried its
/// duration out loud. Refusing `None` would take the detector away from a firmware
/// that does not fill the field, to guard against a case nobody has observed.
///
/// `event` 4 with any other reason is still nothing at all: a recording that ended, a
/// camera torn off the wall. That rule is untouched, and so is the `event` 3 path.
fn is_press(announcement: &Announcement) -> bool {
	if announcement.event() != EVENT_PRESS_END || announcement.reason() != Some(REASON_BUTTON) {
		return false;
	}
	announcement
		.duration()
		.is_none_or(|ms| ms <= MAX_PRESS_DURATION_MS)
}

/// Which configured camera an announcement's uid belongs to, if any. This defers to
/// [`match_camera_by_uid`], the single rule of the tree, so the announcement path and
/// the push path can never disagree about who owns an uid.
fn camera_for<'a>(
	cameras: &'a HashMap<String, Arc<CameraHandle>>,
	wire_uid: &str,
) -> Option<&'a Arc<CameraHandle>> {
	match_camera_by_uid(cameras, wire_uid)
}

/// What hearing one announcement means for the camera it belongs to. The decision is
/// separated from the act on purpose: deciding is synchronous and testable, publishing
/// the press needs the async MQTT client, and the consumer does it.
enum Heard {
	/// A read was asked for through the guard, or was refused by the guard.
	Read,
	/// This announcement is a button press, for this camera: `event` 4 with
	/// `reason` 19 and an activity short enough to have been the button, which is
	/// the only reading of that pair the bridge publishes. It is published and asks
	/// for no read. The same pair with a longer duration is the end of the wake
	/// session and lands in [`Heard::Nothing`] instead.
	Press(Arc<CameraHandle>),
	/// Nothing this bridge acts on: another camera's uid, an event nobody reads, or a
	/// camera configured not to be read on announcement.
	Nothing,
}

/// Routes what the channel delivers to the camera it belongs to, and holds each
/// camera's guard.
struct Reader {
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	/// One guard per camera, never one global guard: two cameras have no business
	/// rationing each other's reads. Bounded by the number of configured cameras,
	/// because entries are created on first announcement for a name that had to be
	/// configured to be matched at all.
	guards: HashMap<String, Arc<Guard>>,
	unknown_uids: HashSet<String>,
	unknown_overflow: usize,
	cancel: CancellationToken,
}

impl Reader {
	/// One announcement: what to do about it, decided without acting on it.
	///
	/// Split out from [`run`] so the three rules can be tested without a socket, a
	/// listen loop, or a camera behind them.
	fn hear(&mut self, announcement: &Announcement) -> Heard {
		let Some(handle) = camera_for(&self.cameras, announcement.uid()) else {
			self.note_unknown(announcement.uid(), announcement.event());
			return Heard::Nothing;
		};
		let config = handle.config();
		if is_press(announcement) {
			// A button activity that ended, and ended short: the camera is telling us
			// the button was used. This is the fastest press detector available: no
			// session and no registry in the way, so it survives a restart of the
			// module. It publishes through the same door as the session's alarm and
			// the webhook, so the dedupe window in `status_cache` decides which of
			// them speaks, and it asks for no read: the press is already known, and a
			// session bought here would only cost antenna time. The wake handle is
			// deliberately not passed on, because `report_doorbell_press` proposes a
			// read burst when it holds one.
			return Heard::Press(Arc::clone(handle));
		}
		if announcement.event() == EVENT_PRESS_END && announcement.reason() == Some(REASON_BUTTON)
		{
			// Only reachable when `is_press` refused the pair, which for this pair now
			// means one thing: the announced button activity outlasted the press
			// window, so the camera is closing its session and nobody rang. Logging it
			// is the whole point — a press refused here is otherwise invisible, and a
			// firmware that starts reporting genuine releases with a long duration
			// shows up on this line instead of as a doorbell that stopped working.
			debug!(
				camera = handle.name(),
				duration = ?announcement.duration(),
				cap_ms = MAX_PRESS_DURATION_MS,
				"wake announcement: button activity outlasted the press window, not a press"
			);
		}
		if announcement.event() != EVENT_WAKE {
			debug!(
				camera = handle.name(),
				event = announcement.event(),
				reason = ?announcement.reason(),
				"wake announcement: not a wake, nothing to read"
			);
			return Heard::Nothing;
		}
		if !config.read_on_wake {
			debug!(
				camera = handle.name(),
				"wake announcement: this camera is not read on wake (read_on_wake = false)"
			);
			return Heard::Nothing;
		}
		let guard = self
			.guards
			.entry(handle.name().to_owned())
			.or_insert_with(|| Arc::new(Guard::new()));
		if polling::try_once(
			guard,
			Arc::clone(handle),
			Duration::from_secs(config.read_hold_secs),
			Duration::from_secs(config.read_interval_secs),
			&self.cancel,
		) {
			// The guard let it through, so a read was asked for for this camera.
			Heard::Read
		} else {
			// Already in flight, or inside the interval between two reads that got
			// a session: the guard refused, which is the rule, not a failure.
			Heard::Nothing
		}
	}

	/// Publish the press, sharing the dedupe window with every other press source.
	async fn publish_press(&self, handle: &CameraHandle) {
		let Some(mqtt) = self.mqtt.as_ref() else {
			tracing::info!(
				camera = %handle.name(),
				"button press announced but this bridge has no MQTT client, so the \
				 press is in the log only"
			);
			return;
		};
		let publisher = StatusPublisher::new(mqtt, &self.topic_prefix, handle.name());
		let cache = handle.status_cache();
		crate::camera_tasks::report_doorbell_press(handle.name(), &publisher, &cache, None).await;
	}

	/// Bookkeeping for an announcement nobody owns: reported once per uid while the
	/// cap leaves room, counted silently after. The operator has to be able to see
	/// that a camera is talking without the configuration naming it, and a peer
	/// inventing uids must not be able to fill either the log or memory.
	fn note_unknown(&mut self, uid: &str, event: u32) {
		if self.unknown_uids.len() < UNKNOWN_UIDS_MAX && self.unknown_uids.insert(uid.to_owned()) {
			info!(
				uid = %UidTail(uid),
				event,
				"wake announcement from a uid this bridge does not configure"
			);
		} else {
			self.unknown_overflow += 1;
			debug!(
				remembered = UNKNOWN_UIDS_MAX,
				unremembered = self.unknown_overflow,
				"wake announcement from an unconfigured uid, not remembered"
			);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::CameraConfig;

	/// `{"event":3}`: the announcement that says the camera is up. Real bytes off
	/// the wire, held inline: they are the proof of this wiring, so it must not
	/// depend on a file somewhere else.
	const WAKE_FRAME: &str = concat!(
		"31cf872a0b0000008c010000ffffffff00000000",
		// // Captured from the wire on the operator's camera; the uid field is a synthetic
		// // stand-in of the same width, the bytes around it are the capture as it arrived.
		"3935323730303053594e54485445535400000000000000000000000000000000",
		"b539c75fac1f48697cef1878ed1c6a",
	);

	/// `{"event":4,"duration":19281,…,"reason":19}`: the end of the recording a press
	/// started, which arrives well after the wake it belongs to. This is capture F of
	/// `crates/core/src/bcudp/announcement.rs` — real bytes off the wire, with the uid
	/// field replaced by the same stand-in as in `WAKE_FRAME` — and it is a genuine
	/// release: the longest one measured here, at 19.3 s. That number is the floor
	/// under `MAX_PRESS_DURATION_MS`; a bound below it would refuse a press this file
	/// proves happened.
	const PRESS_FRAME: &str = concat!(
		"31cf872a4000000096010000ffffffff00000000",
		"3935323730303053594e54485445535400000000000000000000000000000000",
		"1f2924649a1f486946ef1878db1b3b1a941103e3b57250e84cc3f79c78db40af",
		"19a19d638051496a51e018338e413502c25643b7f2280faa4c8ba3c4398c1fa101e4cb6a",
	);

	fn bytes(hex: &str) -> Vec<u8> {
		(0..hex.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
			.collect()
	}

	fn decode(hex: &str) -> Announcement {
		bairelay_neolink_core::bcudp::announcement::decode_announcement(&bytes(hex))
			.expect("a captured announcement decodes")
	}

	/// An announcement carrying exactly the fields a rule reads, so a test can name
	/// `duration` instead of inheriting whatever a capture happened to carry. The body
	/// is written out as the JSON the camera allegedly sent — that is the part the
	/// rule reads, and it is in plain sight here — and is then laid into a frame the
	/// real decoder accepts, so nothing bypasses the decoding path.
	///
	/// Unlike `WAKE_FRAME` and `PRESS_FRAME`, this is **not** a capture: no camera
	/// said these. They are the cases the captures do not cover, and they are labelled
	/// as such in each test below.
	fn said(event: u32, reason: Option<u32>, duration: Option<u64>) -> Announcement {
		let mut body = format!(r#"{{"event":{event}"#);
		if let Some(duration) = duration {
			body.push_str(&format!(r#","duration":{duration}"#));
		}
		if let Some(reason) = reason {
			body.push_str(&format!(r#","reason":{reason}"#));
		}
		body.push('}');
		let frame = frame(0x0190, "9527000SYNTHTEST", body.as_bytes());
		bairelay_neolink_core::bcudp::announcement::decode_announcement(&frame)
			.unwrap_or_else(|e| panic!("{body} does not survive the round trip: {e}"))
	}

	/// The `0x31` envelope: magic, body length, `tid`, the two reserved words, the
	/// 32-byte uid field, the CRC over the **encrypted** body, then that body.
	///
	/// The body cipher and the CRC are rebuilt here because the core crate keeps
	/// `xml_crypto::encrypt` and `crc::calc_crc` `pub(crate)` to itself: its own
	/// decoder tests lay frames the same way, and everything outside that crate can
	/// only reach them through the wire. If either ever disagrees with the decoder,
	/// `said` panics on the round trip rather than testing against a frame the bridge
	/// would have thrown away.
	fn frame(tid: u32, uid: &str, plain: &[u8]) -> Vec<u8> {
		let body = body_cipher(tid, plain);
		let mut uid_field = [0u8; 32];
		uid_field[..uid.len()].copy_from_slice(uid.as_bytes());
		let mut frame = Vec::with_capacity(56 + body.len());
		frame.extend_from_slice(&0x2a87_cf31u32.to_le_bytes());
		frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
		frame.extend_from_slice(&tid.to_le_bytes());
		frame.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
		frame.extend_from_slice(&0u32.to_le_bytes());
		frame.extend_from_slice(&uid_field);
		frame.extend_from_slice(&bc_crc(&body).to_le_bytes());
		frame.extend_from_slice(&body);
		frame
	}

	/// The body cipher of `bcudp/xml_crypto.rs`: eight key words, each offset by
	/// `tid`, cycled over the body and XORed. Symmetric, so it encrypts and decrypts.
	fn body_cipher(tid: u32, buf: &[u8]) -> Vec<u8> {
		const XML_KEY: [u32; 8] = [
			0x1f2d_3c4b, 0x5a6c_7f8d, 0x3817_2e4b, 0x8271_635a, 0x863f_1a2b, 0xa5c6_f7d8,
			0x8371_e1b4, 0x17f2_d3a5,
		];
		buf.iter()
			.zip(
				XML_KEY
					.iter()
					.flat_map(|word| word.wrapping_add(tid).to_le_bytes())
					.cycle(),
			)
			.map(|(byte, key)| byte ^ key)
			.collect()
	}

	/// The non-standard CRC of `bcudp/crc.rs`: CRC-32, polynomial `0xedb88320`
	/// reflected, initial `0`, no final xor — which is what that helper ends up
	/// computing through `crc32fast`. Bit at a time, because a test body is forty
	/// bytes and a table would be the larger part of this module's tests.
	fn bc_crc(payload: &[u8]) -> u32 {
		let mut register = 0u32;
		for byte in payload {
			register ^= *byte as u32;
			for _ in 0..8 {
				register = if register & 1 != 0 {
					(register >> 1) ^ 0xedb8_8320
				} else {
					register >> 1
				};
			}
		}
		register
	}

	/// A reader wired to a camera **and** to a recording MQTT client, so a press can
	/// be asserted where it lands: on the broker, on one topic, once, not retained.
	fn press_rig() -> (
		Reader,
		Arc<CameraHandle>,
		bairelay_mqtt::test_support::MockHandle,
		CancellationToken,
	) {
		let handle = doorbell(|_| {});
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		reader.mqtt = Some(mqtt);
		reader.topic_prefix = "bairelay".into();
		(reader, handle, mock, cancel)
	}

	/// Everything published to a doorbell topic, as `(topic, payload, retained)`.
	fn doorbell_publishes(
		mock: &bairelay_mqtt::test_support::MockHandle,
	) -> Vec<(String, Vec<u8>, bool)> {
		mock.published()
			.into_iter()
			.filter(|(topic, _, _)| topic.ends_with("status/doorbell"))
			.collect()
	}

	#[tokio::test]
	async fn a_press_announcement_publishes_the_press_and_asks_for_no_read() {
		let (mut reader, handle, mock, _cancel) = press_rig();
		let announcement = decode(PRESS_FRAME);
		assert!(
			!matches!(reader.hear(&announcement), Heard::Read),
			"a press announced by the camera must not buy a session"
		);
		let cam = camera_for(&reader.cameras, announcement.uid()).expect("the press is routed");
		reader.publish_press(cam).await;
		assert_eq!(
			doorbell_publishes(&mock),
			vec![(
				format!("bairelay/{}/status/doorbell", handle.name()),
				b"press".to_vec(),
				false
			)],
			"the press lands on the doorbell topic, once, not retained"
		);
	}

	#[tokio::test]
	async fn the_session_reporting_the_same_press_inside_the_window_publishes_nothing_more() {
		let (mut reader, handle, mock, _cancel) = press_rig();
		let announcement = decode(PRESS_FRAME);
		reader.hear(&announcement);
		let cam = camera_for(&reader.cameras, announcement.uid()).expect("the press is routed");
		reader.publish_press(cam).await;
		// The session's alarm now reports the same press through its own door: the
		// dedupe window in `status_cache` decides, and the right is already claimed.
		let mqtt = reader.mqtt.clone().expect("the rig wired a client");
		let publisher = bairelay_mqtt::StatusPublisher::new(&mqtt, "bairelay", handle.name());
		let cache = handle.status_cache();
		crate::camera_tasks::report_doorbell_press(handle.name(), &publisher, &cache, None).await;
		assert_eq!(
			doorbell_publishes(&mock).len(),
			1,
			"one press, whichever source got there first"
		);
	}

	#[tokio::test]
	async fn a_press_publishes_before_the_first_beat_after_a_restart() {
		// What the restart window is worth, measured in memory instead of on disk: a
		// fresh process, an address registry that is empty because no beat has arrived
		// yet, and a camera whose configured address will never answer. The push
		// listener would discard this press — it matches by source address against a
		// registry built by beats. The announcement path matches by uid alone, so the
		// press is published anyway, in the first second after a restart.
		let (reader, handle, mock, _cancel) = press_rig();
		let announcement = decode(PRESS_FRAME);
		let cam = camera_for(&reader.cameras, announcement.uid()).expect("uid alone is enough");
		reader.publish_press(cam).await;
		assert_eq!(
			doorbell_publishes(&mock),
			vec![(
				format!("bairelay/{}/status/doorbell", handle.name()),
				b"press".to_vec(),
				false
			)],
			"a press must not wait for the registry to fill"
		);
	}

	#[tokio::test]
	async fn an_announcement_that_is_not_a_press_publishes_nothing() {
		let (mut reader, _handle, mock, _cancel) = press_rig();
		let wake = decode(WAKE_FRAME);
		assert!(!is_press(&wake), "a wake announcement is not a press");
		reader.hear(&wake);
		assert!(doorbell_publishes(&mock).is_empty(), "nothing was pressed");
		// The predicate is the whole rule. The captures on this tree carry `reason`
		// 19 or no reason at all, so an `event` 4 with another reason is excluded by
		// that predicate, not by a second branch somewhere else — and since the
		// 119 s announcement of 17:31:00 the same predicate also weighs the
		// `duration` the pair carries.
		assert_eq!((EVENT_PRESS_END, REASON_BUTTON), (4, 19));
	}

	/// The pair with no duration at all is a press. Nothing measured says a missing
	/// duration means "the session ended", and a firmware that does not fill the
	/// field must not lose the detector for a case nobody has observed.
	#[test]
	fn a_press_announcement_without_a_duration_is_a_press() {
		assert!(
			is_press(&said(4, Some(19), None)),
			"no duration to weigh, so the pair stands on its own"
		);
	}

	/// A short activity is the button, which is what this detector was built for: the
	/// press arrives with no session and no registry behind it.
	#[test]
	fn a_press_announcement_of_eight_hundred_milliseconds_is_a_press() {
		assert!(
			is_press(&said(4, Some(19), Some(800))),
			"0.8 s is a finger on a button, and well inside the window"
		);
	}

	/// The announcement that started this. Production, 17:31:00, on the wake that
	/// opened at 17:29:01: `event=4 reason=Some(19) duration=Some(119449)` — 119 s,
	/// matching the wake session to the second. The press of 17:29:05 had already been
	/// published off the alarm push, so this announcement rang a second chime for a
	/// press nobody made. It is the camera closing its session, and it must stay a
	/// press-free announcement all the way to the broker.
	#[tokio::test]
	async fn the_119_second_announcement_that_ended_the_wake_session_is_not_a_press() {
		let ghost = said(4, Some(19), Some(119_449));
		assert!(
			!is_press(&ghost),
			"a button activity that outlasted the session is the session ending, not a press"
		);
		let (mut reader, _handle, mock, _cancel) = press_rig();
		assert!(
			!matches!(reader.hear(&ghost), Heard::Press(_)),
			"the consumer must not be handed a press to publish"
		);
		assert!(
			!matches!(reader.hear(&ghost), Heard::Read),
			"and it is no reason to buy a session either"
		);
		assert!(
			doorbell_publishes(&mock).is_empty(),
			"the operator's phone stays silent"
		);
	}

	/// The capture, not a construction: the longest release ever measured, 19 281 ms,
	/// is still a press. This is the test that pins `MAX_PRESS_DURATION_MS` from
	/// below — set the bound under 19.3 s and this fails on a real capture, which is
	/// exactly why the bound is not the 15 s the "a press is seconds" intuition
	/// suggests.
	#[test]
	fn the_captured_press_release_of_19_seconds_is_still_a_press() {
		let release = decode(PRESS_FRAME);
		assert_eq!(
			release.duration(),
			Some(19_281),
			"the capture's own duration, so this test cannot drift away from it"
		);
		assert!(
			is_press(&release),
			"a bound that refuses the longest release off this firmware costs the operator their presses"
		);
	}

	/// `event` 4 with another reason stays nothing, exactly as it was before the
	/// duration joined the rule: a recording that ended is not a press whatever its
	/// length.
	#[test]
	fn event_4_with_another_reason_is_still_not_a_press() {
		assert!(
			!is_press(&said(4, Some(20), None)),
			"an unknown reason is not the button, with or without a duration"
		);
		assert!(
			!is_press(&said(4, Some(20), Some(800))),
			"a short duration does not turn another reason into the button"
		);
		assert!(
			!is_press(&said(4, Some(20), Some(119_449))),
			"neither does a long one"
		);
	}

	/// The `event` 3 path does not look at `duration`: the bound is a rule about
	/// presses, and a wake that announces a long activity behind it is still the wake
	/// the read is owed.
	#[tokio::test]
	async fn the_wake_path_never_looks_at_the_duration() {
		// The captured wake first, as the baseline the constructed one is measured
		// against. Two separate readers, because the guard of one reader is what
		// refuses a second announcement inside its window — and the guard is not what
		// this test is about.
		let handle = doorbell(|c| {
			c.read_on_wake = true;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(
			matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"the captured wake still asks for the read"
		);
		cancel.cancel();

		let handle = doorbell(|c| {
			c.read_on_wake = true;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		let long_wake = said(3, Some(19), Some(119_449));
		assert!(
			!is_press(&long_wake),
			"event 3 is never a press, whatever it carries"
		);
		assert!(
			matches!(reader.hear(&long_wake), Heard::Read),
			"a wake naming a long activity is still a wake, and the guard is what decides"
		);
		cancel.cancel();
	}

	/// A camera at an address that will never answer: the guard still asks for the
	/// read, and the read then fails on its own. Nothing below depends on a camera
	/// being reachable.
	fn doorbell(configure: impl FnOnce(&mut CameraConfig)) -> Arc<CameraHandle> {
		let mut config = CameraConfig {
			name: "Doorbell".into(),
			uid: Some("9527000SYNTHTEST-FW".into()),
			address: Some("127.1.2.3".into()),
			username: "admin".into(),
			password: Some("unused-in-this-test".into()),
			..Default::default()
		};
		configure(&mut config);
		Arc::new(CameraHandle::new(config, CancellationToken::new(), None))
	}

	fn reader_for(handle: Arc<CameraHandle>, cancel: CancellationToken) -> Reader {
		let mut cameras = HashMap::new();
		cameras.insert(handle.name().to_owned(), handle);
		Reader {
			mqtt: None,
			topic_prefix: "bairelay".into(),
			cameras: Arc::new(cameras),
			guards: HashMap::new(),
			unknown_uids: HashSet::new(),
			unknown_overflow: 0,
			cancel,
		}
	}

	/// Wait for the read's wake lock to appear, so a test can distinguish "asked
	/// and running" from "asked and already finished".
	async fn lock_appears(handle: &CameraHandle) -> bool {
		for _ in 0..100 {
			if handle.wake_lock().count() > 0 {
				return true;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		false
	}

	/// Rule 1, and its opposite: the camera's own announcement reads the camera,
	/// and `read_on_wake = false` is the operator's way to say no.
	#[tokio::test]
	async fn an_announcement_reads_the_camera_that_sent_it() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(
			matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"a wake announcement must ask for the guarded read"
		);
		assert!(
			lock_appears(&handle).await,
			"the read was asked for but never took the wake lock"
		);
		cancel.cancel();
	}

	#[tokio::test]
	async fn a_camera_not_read_on_wake_starts_nothing() {
		let handle = doorbell(|c| {
			c.read_on_wake = false;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(
			!matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"the operator switched reading off for this camera"
		);
		assert_eq!(handle.wake_lock().count(), 0, "and nothing was taken");
		cancel.cancel();
	}

	/// Rule 2: the hold comes from the camera, because it is the camera's network
	/// cost that decides it. A one-second hold must be given up after about one
	/// second; a hold is not an open-ended promise.
	#[tokio::test]
	async fn the_hold_window_comes_from_the_camera_config() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
			c.read_hold_secs = 1;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read));
		assert!(lock_appears(&handle).await, "the read took the lock");
		for _ in 0..300 {
			if handle.wake_lock().count() == 0 {
				cancel.cancel();
				return;
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		panic!("a 1 s hold was still holding after 6 s: the window is not coming from the config");
	}

	/// The end-of-recording confirmation of a press is not a second wake: reading
	/// on it too would spend a second session on one wake.
	#[tokio::test]
	async fn a_press_release_is_not_a_wake() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(!matches!(reader.hear(&decode(PRESS_FRAME)), Heard::Read), "event 4 confirms, it does not wake");
		assert_eq!(handle.wake_lock().count(), 0, "no lock was taken");
		cancel.cancel();
	}

	/// Rule 3, the half that protects the battery: while a read is running, a
	/// second announcement of the same camera is refused. Motion detection wakes a
	/// battery camera on every moving object, and each one would otherwise buy a
	/// session.
	#[tokio::test]
	async fn a_second_announcement_inside_the_guard_does_not_ask_again() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
			c.read_hold_secs = 30;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read), "the first one asks");
		assert!(
			!matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"the second one must be refused while the first is still running"
		);
		cancel.cancel();
	}

	/// Rule 3, the half that is easy to get wrong: only a read that *got a session*
	/// is charged to the interval. A read that reached nothing cost nothing, so it
	/// must not forbid the next announcement; charging a failed attempt would
	/// silence a camera whose link is merely slow.
	#[tokio::test]
	async fn a_read_that_got_nothing_is_not_charged_to_the_interval() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
			c.read_hold_secs = 1;
			c.read_interval_secs = 600;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read), "the first read goes out");
		assert!(lock_appears(&handle).await);
		// Let the read finish against an address that never answers: it obtains no
		// session, so it writes nothing to the interval.
		for _ in 0..300 {
			if handle.wake_lock().count() == 0 {
				break;
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		assert_eq!(handle.wake_lock().count(), 0, "the read gave up");
		assert!(
			matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"a failed read must not forbid the next announcement"
		);
		cancel.cancel();
	}

	/// Guards are keyed per camera, never global.
	#[tokio::test]
	async fn the_guard_is_keyed_per_camera() {
		let handle = doorbell(|c| {
			c.read_on_wake = true;
			c.read_hold_secs = 30;
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read));
		assert!(!matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read));
		assert_eq!(reader.guards.len(), 1, "one guard, keyed by the camera name");
		assert!(reader.guards.contains_key("Doorbell"));
		cancel.cancel();
	}

	/// An announcement naming a camera this bridge does not configure starts
	/// nothing.
	#[tokio::test]
	async fn an_unconfigured_uid_starts_no_read() {
		let handle = doorbell(|c| {
			c.uid = Some("0000000ZZZZZZZZ-FW".into());
		});
		let cancel = CancellationToken::new();
		let mut reader = reader_for(Arc::clone(&handle), cancel.clone());
		assert!(
			!matches!(reader.hear(&decode(WAKE_FRAME)), Heard::Read),
			"the captured uid belongs to no configured camera"
		);
		assert_eq!(handle.wake_lock().count(), 0, "no read for a stranger");
		assert_eq!(reader.unknown_uids.len(), 1, "and it is noticed once");
		cancel.cancel();
	}

	/// The memory of unconfigured uids is capped, and what falls outside it is
	/// counted rather than stored.
	#[tokio::test]
	async fn an_unconfigured_uid_is_remembered_up_to_a_cap() {
		let cancel = CancellationToken::new();
		let mut reader = reader_for(doorbell(|_| {}), cancel.clone());
		for i in 0..(UNKNOWN_UIDS_MAX + 30) {
			reader.note_unknown(&format!("UID{i:08}"), EVENT_WAKE);
		}
		assert_eq!(reader.unknown_uids.len(), UNKNOWN_UIDS_MAX, "capped");
		assert_eq!(reader.unknown_overflow, 30, "the rest are counted, not stored");
		cancel.cancel();
	}

	/// The hand-off is bounded by hand: past the cap an announcement is counted
	/// and lost, never queued, and the counter drains as the consumer takes them.
	#[tokio::test]
	async fn the_queue_stays_bounded_and_counts_what_it_drops() {
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		let pending = Arc::new(AtomicUsize::new(0));
		let dropped = Arc::new(AtomicUsize::new(0));
		let obs = observer(tx, Arc::clone(&pending), Arc::clone(&dropped));
		for _ in 0..(CHANNEL_MAX * 2) {
			obs(decode(WAKE_FRAME));
		}
		assert_eq!(pending.load(Ordering::Relaxed), CHANNEL_MAX, "capped");
		assert_eq!(dropped.load(Ordering::Relaxed), CHANNEL_MAX, "and counted");
		for _ in 0..CHANNEL_MAX {
			rx.recv().await.expect("the queued announcements are there");
			// What the consumer in `run` does on the way out: without this the
			// bound would fill once and stay full for the life of the process.
			pending.fetch_sub(1, Ordering::Relaxed);
		}
		assert_eq!(pending.load(Ordering::Relaxed), 0, "the bound drains");
	}

	/// A consumer that is gone loses the announcement instead of panicking, and
	/// leaves no count behind.
	#[tokio::test]
	async fn a_missing_consumer_loses_the_announcement_without_panic() {
		let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
		drop(rx);
		let pending = Arc::new(AtomicUsize::new(0));
		let dropped = Arc::new(AtomicUsize::new(0));
		observer(tx, Arc::clone(&pending), dropped)(decode(WAKE_FRAME));
		assert_eq!(pending.load(Ordering::Relaxed), 0, "the counter does not leak");
	}
}
