//! Per-camera capability cache.
//!
//! Capabilities are discovered once per successful connect — from
//! [`bairelay_neolink_core::bc_protocol::BcCamera::get_support`] plus the
//! login-time `abilityInfo` — and the relevant booleans cached on
//! [`crate::camera::CameraHandle`]. The HA MQTT discovery publisher
//! consults this cache to gate capability-dependent entities (PT
//! buttons on fixed-position cameras, etc.).
//!
//! Keep this struct minimal — add a field only when a live consumer
//! needs to gate on it.

pub use bairelay_mqtt::discovery::Presence;

/// Subset of the camera's reported `Support` XML relevant to HA
/// discovery. `Default` yields "no optional hardware" — the safe
/// assumption when the `get_support` call fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct CameraCapabilities {
	/// `true` when the camera reports pan-tilt hardware via either
	/// `ptzMode` (non-empty string) or `ptzCfg` (non-zero integer).
	/// Drives the PT-button entity set in HA discovery.
	pub has_ptz: bool,
	/// Status LED (small indicator light) — `ledCtrl` bit 0 of the
	/// channel's `<Support>` item. Independent of `ir_leds`.
	pub status_led: Presence,
	/// IR night-vision LEDs — the `ledState` ability in the login-time
	/// `abilityInfo`. Independent of `status_led`.
	pub ir_leds: Presence,
}

/// `<Support>` item `ledCtrl` bit marking a status LED. Decoding from
/// nodelink-js (`src/reolink/baichuan/capabilities.ts`: "bit 0 = status
/// LED"); consistent with live captures — Argus `0x4047` (has one), an
/// IR camera without one `0x40`, E1 Zoom `0x1`. The `<LedState>` reply
/// can't tell: cameras return a placeholder for hardware they lack.
pub const LED_CTRL_STATUS_LED: u32 = 1 << 0;

/// Ability key advertised only by cameras with IR night-vision LEDs.
/// Status-LED-only cameras (Argus) omit it, so it must not gate the
/// status LED.
pub const IR_LEDS_ABILITY: &str = "ledState";

/// `(status LED, IR LEDs)` presence for `channel`, each from its own
/// signal: `<Support>` `ledCtrl` bit 0, and the cached `ledState`
/// ability. Neither is derived from the `<LedState>` reply.
pub async fn led_presence(
	driver: &dyn bairelay_neolink_core::bc_protocol::CameraDriver,
	support: &bairelay_neolink_core::bc::xml::Support,
	channel: u8,
) -> (Presence, Presence) {
	let status_led = support_status_led(support, channel);
	let ir_leds = Presence::from_signal(driver.ability_advertised(IR_LEDS_ABILITY).await);
	(status_led, ir_leds)
}

/// Status-LED presence from the `<Support>` item for `channel`.
/// `Unknown` when the item or its `ledCtrl` field is missing.
pub fn support_status_led(
	support: &bairelay_neolink_core::bc::xml::Support,
	channel: u8,
) -> Presence {
	let bits = support
		.items
		.iter()
		.find(|item| item.chn_id == u32::from(channel))
		.and_then(|item| item.led_ctrl);
	Presence::from_signal(bits.map(|b| b & LED_CTRL_STATUS_LED != 0))
}

impl From<CameraCapabilities> for bairelay_mqtt::discovery::CameraCapabilitiesView {
	fn from(caps: CameraCapabilities) -> Self {
		Self {
			has_ptz: caps.has_ptz,
			status_led: caps.status_led,
			ir_leds: caps.ir_leds,
		}
	}
}

/// Decide whether a `Support.ptz_mode` / `Support.ptz_cfg` pair indicates
/// real motorised PTZ hardware. Reolink fixed-mount cameras (e.g. Altas)
/// report `ptzMode=none` in their support XML — non-empty but explicitly
/// NOT PTZ — which a naive `is_empty()` check misclassifies. Only modes
/// like `pt`, `ptz`, `3d` count as PTZ.
pub fn ptz_mode_indicates_ptz(ptz_mode: Option<&str>, ptz_cfg: Option<u32>) -> bool {
	let mode_active = ptz_mode
		.map(|s| {
			let t = s.trim().to_ascii_lowercase();
			!t.is_empty() && t != "none" && t != "0" && t != "false"
		})
		.unwrap_or(false);
	mode_active || ptz_cfg.map(|v| v != 0).unwrap_or(false)
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::{Support, SupportItem};

	#[test]
	fn ptz_mode_pt_is_ptz() {
		assert!(ptz_mode_indicates_ptz(Some("pt"), None));
		assert!(ptz_mode_indicates_ptz(Some("PTZ"), None));
		assert!(ptz_mode_indicates_ptz(Some(" 3d "), None));
	}

	#[test]
	fn ptz_mode_none_is_not_ptz() {
		assert!(!ptz_mode_indicates_ptz(Some("none"), None));
		assert!(!ptz_mode_indicates_ptz(Some("None"), None));
		assert!(!ptz_mode_indicates_ptz(Some(" NONE "), None));
		assert!(!ptz_mode_indicates_ptz(Some("0"), None));
		assert!(!ptz_mode_indicates_ptz(Some(""), None));
		assert!(!ptz_mode_indicates_ptz(None, None));
	}

	#[test]
	fn ptz_cfg_nonzero_is_ptz_even_when_mode_says_none() {
		assert!(ptz_mode_indicates_ptz(Some("none"), Some(1)));
	}

	#[test]
	fn ptz_cfg_zero_is_not_ptz() {
		assert!(!ptz_mode_indicates_ptz(None, Some(0)));
	}

	fn support_with_led_ctrl(chn_id: u32, led_ctrl: Option<u32>) -> Support {
		Support {
			items: vec![SupportItem {
				chn_id,
				led_ctrl,
				..Default::default()
			}],
			..Default::default()
		}
	}

	#[test]
	fn led_ctrl_bit0_marks_status_led() {
		// Captured values: Argus (status LED, no IR), IR camera without a
		// status LED, E1 Zoom (status LED).
		let at = |bits| support_status_led(&support_with_led_ctrl(0, Some(bits)), 0);
		assert_eq!(at(0x4047), Presence::Present);
		assert_eq!(at(0x40), Presence::Absent);
		assert_eq!(at(1), Presence::Present);
	}

	/// Each signal decides only its own feature; an unreceived ability
	/// report (account cameras) leaves IR unknown, not absent.
	#[tokio::test]
	async fn led_presence_reads_each_feature_from_its_own_signal() {
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use Presence::*;
		let argus = support_with_led_ctrl(0, Some(0x4047));
		let ir_cam = support_with_led_ctrl(0, Some(0x40));

		let fake = FakeCameraBuilder::new().with_abilities(&["motion"]).build();
		assert_eq!(led_presence(&*fake, &argus, 0).await, (Present, Absent));

		let fake = FakeCameraBuilder::new()
			.with_abilities(&["ledState"])
			.build();
		assert_eq!(led_presence(&*fake, &ir_cam, 0).await, (Absent, Present));

		let fake = FakeCameraBuilder::new().build(); // no ability report
		assert_eq!(
			led_presence(&*fake, &Support::default(), 0).await,
			(Unknown, Unknown)
		);
	}

	#[test]
	fn missing_led_ctrl_or_channel_is_unknown_not_absent() {
		let unknown = Presence::Unknown;
		assert_eq!(
			support_status_led(&support_with_led_ctrl(0, None), 0),
			unknown
		);
		assert_eq!(
			support_status_led(&support_with_led_ctrl(1, Some(1)), 0),
			unknown
		);
		assert_eq!(support_status_led(&Support::default(), 0), unknown);
	}
}
