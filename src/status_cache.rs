//! Per-camera cache of the most-recent MQTT status values.
//!
//! Each camera task that publishes a status topic (battery, motion,
//! per-class motion, push, floodlight, floodlight_tasks, pir, ir, led) writes the just-published value
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
//! - PTZ preset (`status/ptz/preset`) — bairelay genuinely doesn't
//!   know which preset the camera selected pre-restart, so the
//!   discovery publisher intentionally clears it on every reconnect.

use crate::stream_source::RwLockPoisonRecover as _;
use bairelay_mqtt::control::IrMode;
use bairelay_neolink_core::bc_protocol::AiDetections;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct StatusCache {
	battery_level: RwLock<Option<u8>>,
	motion: RwLock<Option<bool>>,
	motion_classes: RwLock<Option<AiDetections>>,
	push: RwLock<Option<bool>>,
	floodlight: RwLock<Option<bool>>,
	floodlight_tasks: RwLock<Option<bool>>,
	pir: RwLock<Option<bool>>,
	ir: RwLock<Option<IrMode>>,
	led: RwLock<Option<bool>>,
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

	pub fn set_motion_classes(&self, detections: AiDetections) {
		*self.motion_classes.write_recover() = Some(detections);
	}
	pub fn motion_classes(&self) -> Option<AiDetections> {
		*self.motion_classes.read_recover()
	}

	pub fn set_push(&self, active: bool) {
		*self.push.write_recover() = Some(active);
	}
	pub fn push(&self) -> Option<bool> {
		*self.push.read_recover()
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
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn defaults_to_none() {
		let c = StatusCache::default();
		assert_eq!(c.battery_level(), None);
		assert_eq!(c.motion(), None);
		assert_eq!(c.motion_classes(), None);
		assert_eq!(c.push(), None);
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
		let person = AiDetections {
			person: true,
			..Default::default()
		};
		c.set_motion_classes(person);
		c.set_push(true);
		c.set_floodlight(false);
		c.set_floodlight_tasks(true);
		c.set_pir(false);
		c.set_ir(IrMode::Auto);
		c.set_led(true);
		assert_eq!(c.battery_level(), Some(72));
		assert_eq!(c.motion(), Some(true));
		assert_eq!(c.motion_classes(), Some(person));
		assert_eq!(c.push(), Some(true));
		assert_eq!(c.floodlight(), Some(false));
		assert_eq!(c.floodlight_tasks(), Some(true));
		assert_eq!(c.pir(), Some(false));
		assert_eq!(c.ir(), Some(IrMode::Auto));
		assert_eq!(c.led(), Some(true));
	}

	#[test]
	fn last_write_wins() {
		let c = StatusCache::default();
		c.set_battery_level(50);
		c.set_battery_level(48);
		assert_eq!(c.battery_level(), Some(48));
	}
}
