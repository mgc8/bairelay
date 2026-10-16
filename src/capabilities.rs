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

use bairelay_neolink_core::bc::xml::{Support, SupportItem};

/// Subset of the camera's reported `Support` XML relevant to HA
/// discovery. `Default` yields no PTZ and every presence `Unknown`,
/// which neither hides nor retracts an entity — the safe assumption
/// when the `get_support` call fails.
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
	/// AI object detection — the channel `<Support>` item's `aitype`.
	pub ai: Presence,
	/// Doorbell button — the channel `<Support>` item's
	/// `doorbellVersion`, or a login `DeviceInfo` type naming a bell.
	pub doorbell: Presence,
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
	support: &Support,
	channel: u8,
) -> (Presence, Presence) {
	let status_led = support_status_led(support, channel);
	let ir_leds = Presence::from_signal(driver.ability_advertised(IR_LEDS_ABILITY).await);
	(status_led, ir_leds)
}

/// Every capability of `channel`, from the `<Support>` reply, the cached
/// login abilities and the login `DeviceInfo` type.
pub async fn camera_capabilities(
	driver: &dyn bairelay_neolink_core::bc_protocol::CameraDriver,
	support: &Support,
	channel: u8,
	device_type: Option<&str>,
) -> CameraCapabilities {
	let (status_led, ir_leds) = led_presence(driver, support, channel).await;
	CameraCapabilities {
		has_ptz: ptz_mode_indicates_ptz(support.ptz_mode.as_deref(), support.ptz_cfg),
		status_led,
		ir_leds,
		ai: support_ai(support, channel),
		doorbell: doorbell_presence(support, channel, device_type),
	}
}

/// The `<Support>` item describing `channel`, if the camera sent one.
fn channel_item(support: &Support, channel: u8) -> Option<&SupportItem> {
	support
		.items
		.iter()
		.find(|item| item.chn_id == u32::from(channel))
}

/// Status-LED presence from the `<Support>` item for `channel`.
/// `Unknown` when the item or its `ledCtrl` field is missing.
pub fn support_status_led(support: &Support, channel: u8) -> Presence {
	let bits = channel_item(support, channel).and_then(|item| item.led_ctrl);
	Presence::from_signal(bits.map(|b| b & LED_CTRL_STATUS_LED != 0))
}

/// AI detection presence from the `<Support>` item for `channel`:
/// non-zero `aitype` is present, zero absent. `Unknown` when the item
/// or its `aitype` field is missing.
pub fn support_ai(support: &Support, channel: u8) -> Presence {
	let ai_type = channel_item(support, channel).and_then(|item| item.ai_type);
	Presence::from_signal(ai_type.map(|n| n > 0))
}

/// Doorbell presence: present when the `<Support>` item for `channel`
/// has a non-zero `doorbellVersion` or the login `DeviceInfo` type
/// contains `bell` (any case); absent when the item exists without
/// either; `Unknown` when the camera sent no item for `channel`.
pub fn doorbell_presence(support: &Support, channel: u8, device_type: Option<&str>) -> Presence {
	let item = channel_item(support, channel);
	let versioned = item
		.and_then(|item| item.doorbell_version)
		.is_some_and(|v| v > 0);
	let bell_type = device_type.is_some_and(|t| t.to_ascii_lowercase().contains("bell"));
	if versioned || bell_type {
		Presence::Present
	} else if item.is_some() {
		Presence::Absent
	} else {
		Presence::Unknown
	}
}

impl From<CameraCapabilities> for bairelay_mqtt::discovery::CameraCapabilitiesView {
	fn from(caps: CameraCapabilities) -> Self {
		Self {
			has_ptz: caps.has_ptz,
			status_led: caps.status_led,
			ir_leds: caps.ir_leds,
			ai: caps.ai,
			doorbell: caps.doorbell,
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

	fn support_item(chn_id: u32, ai_type: Option<u32>, doorbell_version: Option<u32>) -> Support {
		Support {
			items: vec![SupportItem {
				chn_id,
				ai_type,
				doorbell_version,
				..Default::default()
			}],
			..Default::default()
		}
	}

	#[test]
	fn support_ai_table() {
		use Presence::*;
		let cases: &[(Support, Presence)] = &[
			(support_item(0, Some(1), None), Present),
			(support_item(0, Some(0x7f), None), Present),
			(support_item(0, Some(0), None), Absent),
			(support_item(0, None, Some(1)), Unknown),
			(support_item(1, Some(1), None), Unknown),
			(Support::default(), Unknown),
		];
		for (i, (support, want)) in cases.iter().enumerate() {
			assert_eq!(support_ai(support, 0), *want, "case {i}");
		}
	}

	#[test]
	fn doorbell_presence_table() {
		use Presence::*;
		let cases: &[(Support, Option<&str>, Presence)] = &[
			(support_item(0, None, Some(1)), None, Present),
			(support_item(0, None, Some(0)), None, Absent),
			(support_item(0, None, None), None, Absent),
			(support_item(0, None, None), Some("smart_bell"), Present),
			(
				support_item(0, None, None),
				Some("Battery_DoorBell"),
				Present,
			),
			(support_item(0, None, Some(0)), Some("BELL"), Present),
			(support_item(0, None, None), Some("wifi_solo_ipc"), Absent),
			(support_item(1, None, Some(1)), None, Unknown),
			(Support::default(), None, Unknown),
			(Support::default(), Some("ipc"), Unknown),
			(Support::default(), Some("smart_bell"), Present),
		];
		for (i, (support, device_type, want)) in cases.iter().enumerate() {
			assert_eq!(
				doorbell_presence(support, 0, *device_type),
				*want,
				"case {i}"
			);
		}
	}

	/// Every field comes from its own detector for the camera's channel.
	#[tokio::test]
	async fn camera_capabilities_combines_every_detector() {
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use Presence::*;
		let mut support = support_item(0, Some(1), None);
		support.ptz_mode = Some("pt".to_string());
		support.items[0].led_ctrl = Some(0x4047);
		let fake = FakeCameraBuilder::new().with_abilities(&["motion"]).build();

		let caps = camera_capabilities(&*fake, &support, 0, Some("smart_bell")).await;
		assert!(caps.has_ptz);
		assert_eq!(
			(caps.status_led, caps.ir_leds, caps.ai, caps.doorbell),
			(Present, Absent, Present, Present)
		);

		let caps = camera_capabilities(&*fake, &support, 0, None).await;
		assert_eq!(caps.doorbell, Absent);

		let caps = camera_capabilities(&*fake, &Support::default(), 0, None).await;
		assert!(!caps.has_ptz);
		assert_eq!((caps.ai, caps.doorbell), (Unknown, Unknown));
	}

	#[test]
	fn capabilities_view_carries_ai_and_doorbell() {
		let caps = CameraCapabilities {
			ai: Presence::Absent,
			doorbell: Presence::Present,
			..Default::default()
		};
		let view = bairelay_mqtt::discovery::CameraCapabilitiesView::from(caps);
		assert_eq!(
			(view.ai, view.doorbell),
			(Presence::Absent, Presence::Present)
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
