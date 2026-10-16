//! Per-camera cache of the most-recent MQTT status values.
//!
//! Each camera task that publishes a status topic (battery, motion,
//! floodlight, floodlight_tasks, pir, ir, led, ai) writes the just-published value
//! into this cache. On a broker reconnect, `handle_connack` walks
//! every camera's cache and re-emits any set value via the standard
//! `publish_*` retained publishers.
//!
//! Why: MQTT brokers without persistence (or whose retained store is
//! reset alongside HA) lose retained messages on restart. Bairelay's
//! own publishes are then the only source of state, but several
//! topics are event-driven (motion, floodlight) or polled on an
//! interval, so HA can show "unknown" until the next tick after such
//! a reset. Re-emitting the cache on every broker reconnect closes
//! the gap.
//!
//! What's NOT cached:
//!
//! - Connection status (`status`) — already published on every
//!   connect/disconnect transition.
//! - Discovery payloads — re-emitted by `mqtt_loop::handle_connack`.
//! - Preview JPEG — refreshed by `preview_poller` every 5 s default.
//! - Doorbell presses (`status/doorbell`) — one-shot, non-retained events.
//! - PTZ preset (`status/ptz/preset`) — bairelay genuinely doesn't
//!   know which preset the camera selected pre-restart, so the
//!   discovery publisher intentionally clears it on every reconnect.

use crate::stream_source::RwLockPoisonRecover as _;
use bairelay_mqtt::control::IrMode;
use bairelay_neolink_core::bc_protocol::AiObjects;
use std::sync::RwLock;
use std::time::Duration;

/// Doorbell presses closer together than this are one press: the
/// webhook, a forwarded alarm and the session can each report the same
/// button push.
pub const DOORBELL_PRESS_DEDUPE_WINDOW: Duration = Duration::from_secs(10);

/// A push edge and a webhook wake closer together than this are one
/// wake of a sleeping camera: the camera opens its push connection and
/// POSTs its webhook for the same wake. The same window as presses.
pub const WAKE_DEDUPE_WINDOW: Duration = DOORBELL_PRESS_DEDUPE_WINDOW;

/// What reported a sleeping camera's wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeSource {
	/// The camera's connect to the push listener.
	Push,
	/// A `doorbell` or `pir` wake POST to the webhook listener.
	Webhook,
}

#[derive(Debug, Default)]
pub struct StatusCache {
	battery_level: RwLock<Option<u8>>,
	motion: RwLock<Option<bool>>,
	floodlight: RwLock<Option<bool>>,
	floodlight_tasks: RwLock<Option<bool>>,
	pir: RwLock<Option<bool>>,
	ir: RwLock<Option<IrMode>>,
	led: RwLock<Option<bool>>,
	/// Last published per-object AI detection states (all five objects).
	ai_objects: RwLock<Option<AiObjects>>,
	/// Held across each AI cache-update-and-publish sequence, across
	/// capability detection's AI clear and caps write, and across the
	/// replay's read-and-publish, so none of them interleave.
	ai_publish: tokio::sync::Mutex<()>,
	/// When the last doorbell press was published.
	last_doorbell_press: RwLock<Option<tokio::time::Instant>>,
	/// The source and time of the last wake that was acted on.
	last_wake: RwLock<Option<(WakeSource, tokio::time::Instant)>>,
}

impl StatusCache {
	pub fn set_battery_level(&self, level: u8) {
		*self.battery_level.write_recover() = Some(level);
	}
	pub fn battery_level(&self) -> Option<u8> {
		*self.battery_level.read_recover()
	}

	pub fn set_motion(&self, detected: bool) {
		*self.motion.write_recover() = Some(detected);
	}
	pub fn motion(&self) -> Option<bool> {
		*self.motion.read_recover()
	}

	pub fn set_floodlight(&self, on: bool) {
		*self.floodlight.write_recover() = Some(on);
	}
	pub fn floodlight(&self) -> Option<bool> {
		*self.floodlight.read_recover()
	}

	pub fn set_floodlight_tasks(&self, enabled: bool) {
		*self.floodlight_tasks.write_recover() = Some(enabled);
	}
	pub fn floodlight_tasks(&self) -> Option<bool> {
		*self.floodlight_tasks.read_recover()
	}

	pub fn set_pir(&self, enabled: bool) {
		*self.pir.write_recover() = Some(enabled);
	}
	pub fn pir(&self) -> Option<bool> {
		*self.pir.read_recover()
	}

	pub fn set_ir(&self, mode: IrMode) {
		*self.ir.write_recover() = Some(mode);
	}
	pub fn ir(&self) -> Option<IrMode> {
		*self.ir.read_recover()
	}
	pub fn clear_ir(&self) {
		*self.ir.write_recover() = None;
	}

	pub fn set_led(&self, on: bool) {
		*self.led.write_recover() = Some(on);
	}
	pub fn led(&self) -> Option<bool> {
		*self.led.read_recover()
	}
	pub fn clear_led(&self) {
		*self.led.write_recover() = None;
	}

	pub fn set_ai_objects(&self, objects: AiObjects) {
		*self.ai_objects.write_recover() = Some(objects);
	}
	pub fn ai_objects(&self) -> Option<AiObjects> {
		*self.ai_objects.read_recover()
	}
	pub fn clear_ai_objects(&self) {
		*self.ai_objects.write_recover() = None;
	}
	/// Serialise an AI publish sequence: the motion listener holds this
	/// while it caches and publishes a set, capability detection while
	/// it clears the cache and installs new caps, and the
	/// broker-reconnect replay while it reads caps and cache and
	/// re-publishes, so the replay never re-emits a set the listener has
	/// already replaced or pairs stale objects with new caps. Never
	/// awaited on the MQTT event-loop task: holders wait on its drain.
	pub async fn lock_ai_publish(&self) -> tokio::sync::MutexGuard<'_, ()> {
		self.ai_publish.lock().await
	}

	/// Claim the right to publish a doorbell press at `now`: `true`, and
	/// recorded, unless a press was claimed less than
	/// [`DOORBELL_PRESS_DEDUPE_WINDOW`] before `now`.
	pub fn claim_doorbell_press(&self, now: tokio::time::Instant) -> bool {
		let mut last = self.last_doorbell_press.write_recover();
		if last.is_some_and(|t| within(t, now, DOORBELL_PRESS_DEDUPE_WINDOW)) {
			return false;
		}
		*last = Some(now);
		true
	}

	/// Whether a doorbell press was claimed less than `window` before
	/// `now`.
	pub fn doorbell_pressed_within(&self, window: Duration, now: tokio::time::Instant) -> bool {
		self.last_doorbell_press
			.read_recover()
			.is_some_and(|t| within(t, now, window))
	}

	/// Claim a wake reported by `source` at `now`: `true`, and recorded,
	/// unless the other source's wake was claimed less than
	/// [`WAKE_DEDUPE_WINDOW`] before `now`. Wakes from one source are
	/// never folded together, so a later push edge still extends the
	/// hold and owns the fallback `off`.
	pub fn claim_wake(&self, source: WakeSource, now: tokio::time::Instant) -> bool {
		let mut last = self.last_wake.write_recover();
		if last.is_some_and(|(s, t)| s != source && within(t, now, WAKE_DEDUPE_WINDOW)) {
			return false;
		}
		*last = Some((source, now));
		true
	}
}

/// `true` when `now` falls less than `window` after `last`.
fn within(last: tokio::time::Instant, now: tokio::time::Instant, window: Duration) -> bool {
	now.saturating_duration_since(last) < window
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn doorbell_press_claims_dedupe_inside_the_window() {
		let c = StatusCache::default();
		let t0 = tokio::time::Instant::now();
		assert!(c.claim_doorbell_press(t0));
		assert!(!c.claim_doorbell_press(t0));
		assert!(!c.claim_doorbell_press(t0 + Duration::from_secs(9)));
		let t1 = t0 + DOORBELL_PRESS_DEDUPE_WINDOW;
		assert!(c.claim_doorbell_press(t1), "the window ends");
		assert!(
			!c.claim_doorbell_press(t1 + Duration::from_secs(1)),
			"anchored at t1"
		);
	}

	#[test]
	fn doorbell_pressed_within_reads_the_last_claim() {
		let c = StatusCache::default();
		let t0 = tokio::time::Instant::now();
		let window = Duration::from_secs(30);
		assert!(!c.doorbell_pressed_within(window, t0), "no press yet");
		assert!(c.claim_doorbell_press(t0));
		assert!(c.doorbell_pressed_within(window, t0));
		assert!(c.doorbell_pressed_within(window, t0 + Duration::from_secs(29)));
		assert!(!c.doorbell_pressed_within(window, t0 + window));
	}

	#[test]
	fn wake_claims_fold_the_other_source_inside_the_window() {
		use WakeSource::{Push, Webhook};
		let c = StatusCache::default();
		let t0 = tokio::time::Instant::now();
		let at = |s| t0 + Duration::from_secs(s);
		assert!(c.claim_wake(Push, t0));
		assert!(!c.claim_wake(Webhook, at(3)), "the same wake");
		assert!(c.claim_wake(Push, at(5)), "one source is never folded");
		assert!(!c.claim_wake(Webhook, at(14)), "anchored at the later push");
		assert!(c.claim_wake(Webhook, at(15)), "the window ends");
		assert!(!c.claim_wake(Push, at(15)), "and works both ways");
		assert!(c.claim_wake(Webhook, at(16)));
		assert!(c.claim_wake(Push, at(26)));
	}

	#[test]
	fn defaults_to_none() {
		let c = StatusCache::default();
		assert_eq!(c.battery_level(), None);
		assert_eq!(c.motion(), None);
		assert_eq!(c.floodlight(), None);
		assert_eq!(c.floodlight_tasks(), None);
		assert_eq!(c.pir(), None);
		assert_eq!(c.ir(), None);
		assert_eq!(c.led(), None);
	}

	#[test]
	fn round_trips_each_field() {
		let c = StatusCache::default();
		c.set_battery_level(72);
		c.set_motion(true);
		c.set_floodlight(false);
		c.set_floodlight_tasks(true);
		c.set_pir(false);
		c.set_ir(IrMode::Auto);
		c.set_led(true);
		assert_eq!(c.battery_level(), Some(72));
		assert_eq!(c.motion(), Some(true));
		assert_eq!(c.floodlight(), Some(false));
		assert_eq!(c.floodlight_tasks(), Some(true));
		assert_eq!(c.pir(), Some(false));
		assert_eq!(c.ir(), Some(IrMode::Auto));
		assert_eq!(c.led(), Some(true));
	}

	#[test]
	fn ai_objects_round_trip_and_clear() {
		use bairelay_neolink_core::bc_protocol::{AiObject, AiObjects};
		let c = StatusCache::default();
		assert_eq!(c.ai_objects(), None);
		let set: AiObjects = [AiObject::Person, AiObject::Package].into_iter().collect();
		c.set_ai_objects(set);
		assert_eq!(c.ai_objects(), Some(set));
		c.clear_ai_objects();
		assert_eq!(c.ai_objects(), None);
	}

	#[test]
	fn last_write_wins() {
		let c = StatusCache::default();
		c.set_battery_level(50);
		c.set_battery_level(48);
		assert_eq!(c.battery_level(), Some(48));
	}
}
