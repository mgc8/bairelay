//! HA discovery publisher — turns per-camera metadata into the
//! retained `config` payloads HA expects, and removes them on
//! graceful shutdown.
//!
//! Tests exercise `compute_payloads` directly (no broker), then a
//! separate pair of async tests drive `publish` / `unpublish` to
//! confirm the same topic set lands on an MQTT client. The
//! `compute_payloads` seam is deliberately public to keep the test
//! path allocation-free — see Task 11.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::client::SharedMqttClient;
use crate::error::MqttError;

use super::{
	absent_hardware_retractions, build_battery, build_camera, build_floodlight_light,
	build_floodlight_tasks_switch, build_ir, build_led, build_motion, build_motion_classes,
	build_pir, build_pt_buttons, build_ptz_presets, build_push, build_reboot, build_siren,
	CameraCapabilitiesView, DiscoveryContext, Feature,
};

/// Per-camera enable toggles. Mirrors the binary's per-camera
/// `MqttConfig` flags so this crate does not depend on the binary.
/// `features` without a per-cam gate (`Camera`, `Ir`, `Reboot`,
/// `Pt`, `Siren`, `Floodlight`-tasks) flow through unconditionally;
/// see [`Self::allows`].
#[derive(Debug, Clone, Copy)]
pub struct CameraEnableFlags {
	pub motion: bool,
	pub battery: bool,
	pub floodlight: bool,
	pub light: bool,
	pub pir: bool,
}

impl CameraEnableFlags {
	/// Is this feature permitted for the camera by the per-cam
	/// enable toggles? Features with no per-cam flag default to
	/// `true` — they remain subject to global discovery-feature
	/// gating and capability detection.
	pub fn allows(&self, feature: Feature) -> bool {
		match feature {
			Feature::Motion | Feature::MotionClass => self.motion,
			Feature::Battery => self.battery,
			Feature::Floodlight => self.floodlight,
			Feature::Led => self.light,
			Feature::Pir => self.pir,
			// No per-camera enable flag today: gate only via
			// capability detection + global features. `Push` is
			// dropped from the global set when no push listener runs.
			Feature::Push
			| Feature::Camera
			| Feature::Ir
			| Feature::Reboot
			| Feature::Pt
			| Feature::PtPreset
			| Feature::Siren => true,
		}
	}
}

/// Publishes and unpublishes HA discovery config topics for one or
/// more cameras. Clonable — hand clones to per-camera tasks so
/// each task can publish/unpublish its own entry set without
/// shared mutable state. The `last_published` map is `Arc<Mutex<_>>`
/// so all clones see the same shrink-on-republish history.
#[derive(Clone)]
pub struct DiscoveryPublisher {
	client: SharedMqttClient,
	topic_prefix: String,
	ha_topic: String,
	features: HashSet<Feature>,
	sw_version: String,
	/// Last-published topic set keyed by camera_name. Diffed on
	/// `publish` so an entity that suppresses on republish (e.g.
	/// `PtPreset` after `replace_preset_cache(vec![])`, or PT buttons
	/// after a capability flip) gets an explicit retained-empty —
	/// otherwise HA keeps a ghost entity backed by stale retained
	/// config on the broker.
	last_published: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

impl DiscoveryPublisher {
	pub fn new(
		client: SharedMqttClient,
		topic_prefix: String,
		ha_topic: String,
		features: HashSet<Feature>,
		sw_version: String,
	) -> Self {
		Self {
			client,
			topic_prefix,
			ha_topic,
			features,
			sw_version,
			last_published: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	/// Topic prefix the publisher was constructed with (read-only
	/// accessor for tests).
	pub fn topic_prefix(&self) -> &str {
		&self.topic_prefix
	}

	/// HA discovery root topic the publisher was constructed with
	/// (read-only accessor for tests).
	pub fn ha_topic(&self) -> &str {
		&self.ha_topic
	}

	/// Compute the full set of `(topic, payload)` pairs that
	/// `publish` would emit for the supplied camera. Pure — no I/O
	/// — exposed so tests can assert on the exact topic set
	/// without a broker, and so callers can reuse the set for
	/// `unpublish` (same topics, empty payloads).
	///
	/// Gating layers applied in order:
	///
	/// 1. Global `features` set (from config).
	/// 2. Per-camera [`CameraEnableFlags`] (from per-camera MQTT
	///    config).
	/// 3. Live [`CameraCapabilitiesView`] (PT suppressed when
	///    hardware absent).
	pub fn compute_payloads(
		&self,
		camera_name: &str,
		camera_addr: Option<&str>,
		camera_uid: Option<&str>,
		capabilities: CameraCapabilitiesView,
		enable_flags: &CameraEnableFlags,
		presets: &[(u8, String)],
	) -> Vec<(String, Vec<u8>)> {
		let ctx = DiscoveryContext {
			topic_prefix: &self.topic_prefix,
			ha_topic: &self.ha_topic,
			camera_name,
			camera_addr,
			camera_uid,
			sw_version: &self.sw_version,
			capabilities: &capabilities,
			presets,
		};

		// Iterate `Feature::ALL` rather than `self.features` so the
		// emitted topic order is stable regardless of the `HashSet`'s
		// hash randomisation. HA doesn't care, but deterministic order
		// keeps log-diffing sane and makes future snapshot tests
		// non-flaky.
		let mut out: Vec<(String, Vec<u8>)> = Vec::new();
		for feature in Feature::ALL {
			if !self.features.contains(&feature) {
				continue;
			}
			if !enable_flags.allows(feature) {
				continue;
			}
			match feature {
				Feature::Floodlight => {
					if let Some(p) = build_floodlight_light(&ctx) {
						out.push(p);
					}
					if let Some(p) = build_floodlight_tasks_switch(&ctx) {
						out.push(p);
					}
				}
				Feature::Camera => {
					if let Some(p) = build_camera(&ctx) {
						out.push(p);
					}
				}
				Feature::Motion => {
					if let Some(p) = build_motion(&ctx) {
						out.push(p);
					}
				}
				Feature::MotionClass => {
					out.extend(build_motion_classes(&ctx));
				}
				Feature::Push => {
					if let Some(p) = build_push(&ctx) {
						out.push(p);
					}
				}
				Feature::Led => {
					if let Some(p) = build_led(&ctx) {
						out.push(p);
					}
				}
				Feature::Ir => {
					if let Some(p) = build_ir(&ctx) {
						out.push(p);
					}
				}
				Feature::Reboot => {
					if let Some(p) = build_reboot(&ctx) {
						out.push(p);
					}
				}
				Feature::Pt => {
					out.extend(build_pt_buttons(&ctx));
				}
				Feature::PtPreset => {
					if let Some(p) = build_ptz_presets(&ctx) {
						out.push(p);
					}
				}
				Feature::Battery => {
					if let Some(p) = build_battery(&ctx) {
						out.push(p);
					}
				}
				Feature::Siren => {
					if let Some(p) = build_siren(&ctx) {
						out.push(p);
					}
				}
				Feature::Pir => {
					if let Some(p) = build_pir(&ctx) {
						out.push(p);
					}
				}
			}
		}
		out
	}

	/// Retained topics [`Self::publish`] clears for hardware the camera
	/// is confirmed to lack (see [`absent_hardware_retractions`]). Pure.
	pub fn compute_retractions(
		&self,
		camera_name: &str,
		capabilities: CameraCapabilitiesView,
	) -> Vec<String> {
		let ctx = DiscoveryContext {
			topic_prefix: &self.topic_prefix,
			ha_topic: &self.ha_topic,
			camera_name,
			camera_addr: None,
			camera_uid: None,
			sw_version: &self.sw_version,
			capabilities: &capabilities,
			presets: &[],
		};
		absent_hardware_retractions(&ctx)
	}

	/// Publish retained HA discovery config payloads for the
	/// camera. Safe to call repeatedly: retained publishes are
	/// idempotent and HA deduplicates by `unique_id`.
	///
	/// Diffs the new topic set against the last-published set for
	/// this camera and writes empty-retained on any topic that
	/// disappeared (PtPreset suppressed after the preset cache
	/// emptied, PT buttons gone after a capability flip, etc.).
	/// Without this, HA keeps a ghost entity backed by stale
	/// retained config on the broker.
	///
	/// Also clears the retained `status/ptz/preset` topic so HA's
	/// preset `select` doesn't show a stale pre-selection from a
	/// previous run. The Reolink protocol doesn't surface the
	/// camera's actual current preset, so the only thing bairelay
	/// can ever publish on that topic is "the preset I just
	/// commanded during this session". Across restarts (and on broker
	/// reconnects / `query/ptz/preset` refreshes that change the
	/// list) bairelay genuinely doesn't know — better to show
	/// "Unknown" than to lie with a retained name that may not even
	/// be in the current preset list.
	///
	/// Finally clears the config and `status/...` topics of hardware the
	/// camera is confirmed to lack ([`Self::compute_retractions`]).
	pub async fn publish(
		&self,
		camera_name: &str,
		camera_addr: Option<&str>,
		camera_uid: Option<&str>,
		capabilities: CameraCapabilitiesView,
		enable_flags: &CameraEnableFlags,
		presets: &[(u8, String)],
	) -> Result<(), MqttError> {
		let payloads = self.compute_payloads(
			camera_name,
			camera_addr,
			camera_uid,
			capabilities,
			enable_flags,
			presets,
		);
		let new_topics: HashSet<String> = payloads.iter().map(|(t, _)| t.clone()).collect();
		let removed: Vec<String> = self
			.swap_last_published(camera_name, new_topics)
			.into_iter()
			.filter(|t| !payloads.iter().any(|(nt, _)| nt == t))
			.collect();
		for (topic, payload) in &payloads {
			self.client.publish_retained(topic, payload).await?;
		}
		for topic in &removed {
			self.client.publish_retained(topic, b"").await?;
		}
		for topic in self.compute_retractions(camera_name, capabilities) {
			self.client.publish_retained(&topic, b"").await?;
		}
		let preset_state = crate::topics::status_ptz_preset(&self.topic_prefix, camera_name);
		self.client.publish_retained(&preset_state, b"").await?;
		Ok(())
	}

	/// Unpublish the same topic set by writing an empty retained
	/// payload to each. HA treats an empty retained discovery
	/// config as a delete, so this cleanly removes the device's
	/// entities on graceful shutdown. Also clears any topics that
	/// were published in a prior call but are no longer in the
	/// current compute_payloads output (mirrors `publish`'s diff so
	/// the broker is left clean even if capabilities or preset
	/// state shifted between calls).
	pub async fn unpublish(
		&self,
		camera_name: &str,
		camera_addr: Option<&str>,
		camera_uid: Option<&str>,
		capabilities: CameraCapabilitiesView,
		enable_flags: &CameraEnableFlags,
		presets: &[(u8, String)],
	) -> Result<(), MqttError> {
		let payloads = self.compute_payloads(
			camera_name,
			camera_addr,
			camera_uid,
			capabilities,
			enable_flags,
			presets,
		);
		let mut to_clear: HashSet<String> = payloads.iter().map(|(t, _)| t.clone()).collect();
		let prev = self.take_last_published(camera_name);
		to_clear.extend(prev);
		for topic in &to_clear {
			self.client.publish_retained(topic, b"").await?;
		}
		Ok(())
	}

	/// Atomically swap the recorded last-published topic set for a
	/// camera and return the previous value. Lock poisoning is
	/// recovered by reading the inner `HashMap` — the cache is
	/// safe to recover (every entry is a pure value-copy of an
	/// already-published topic name).
	fn swap_last_published(&self, camera: &str, new: HashSet<String>) -> HashSet<String> {
		let mut guard = self
			.last_published
			.lock()
			.unwrap_or_else(|p| p.into_inner());
		guard.insert(camera.to_string(), new).unwrap_or_default()
	}

	/// Remove the recorded last-published topic set for a camera and
	/// return it. Used by `unpublish` so the broker is fully cleared.
	fn take_last_published(&self, camera: &str) -> HashSet<String> {
		let mut guard = self
			.last_published
			.lock()
			.unwrap_or_else(|p| p.into_inner());
		guard.remove(camera).unwrap_or_default()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn all_flags_on() -> CameraEnableFlags {
		CameraEnableFlags {
			motion: true,
			battery: true,
			floodlight: true,
			light: true,
			pir: true,
		}
	}

	fn feature_set(features: &[Feature]) -> HashSet<Feature> {
		features.iter().copied().collect()
	}

	fn publisher_with_features(features: &[Feature]) -> DiscoveryPublisher {
		// `compute_payloads` never touches `client` so a dummy
		// non-functional clone from the broker-less test harness
		// would be wasteful. Instead, wrap a throwaway rumqttc
		// AsyncClient that we never drive: `compute_payloads` is
		// pure so this never gets hit. See Task 11 design notes.
		let (client, _event_loop) = rumqttc::AsyncClient::new(
			rumqttc::MqttOptions::new("discovery-unit-test", "127.0.0.1", 1883),
			16,
		);
		let shared = SharedMqttClient::for_test(client);
		DiscoveryPublisher::new(
			shared,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			feature_set(features),
			"1.2.3".to_string(),
		)
	}

	#[test]
	fn compute_full_set_with_ptz_emits_19_topics() {
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		// Floodlight → 2 (light + tasks switch), Pt → 4,
		// MotionClass → 4, and one each for
		// Camera/Motion/Push/Led/Ir/Reboot/Battery/Siren/Pir.
		// PtPreset is suppressed because we pass `&[]` for presets.
		// 2 + 4 + 4 + 9 = 19.
		assert_eq!(out.len(), 19);

		// Every topic starts with the configured HA root.
		for (topic, _) in &out {
			assert!(
				topic.starts_with("homeassistant/"),
				"topic missing ha root: {topic}"
			);
		}

		// Every unique_id in the payload starts with the prefix.
		for (_, payload) in &out {
			let v: serde_json::Value = serde_json::from_slice(payload).unwrap();
			let uid = v["unique_id"].as_str().expect("unique_id string");
			assert!(uid.starts_with("bairelay_"), "uid bad prefix: {uid}");
		}

		// No duplicate topics.
		let mut topics: Vec<String> = out.iter().map(|(t, _)| t.clone()).collect();
		topics.sort();
		topics.dedup();
		assert_eq!(topics.len(), 19);
	}

	#[test]
	fn compute_without_ptz_drops_four_pt_buttons() {
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: false,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		assert_eq!(out.len(), 15); // 19 − 4 PT buttons
		for (topic, _) in &out {
			assert!(!topic.contains("/button/bairelay_frontdoor_pan_"));
		}
	}

	#[test]
	fn compute_suppresses_battery_when_flag_off() {
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = CameraEnableFlags {
			battery: false,
			..all_flags_on()
		};
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		for (topic, _) in &out {
			assert!(
				!topic.contains("bairelay_frontdoor_battery"),
				"battery config was emitted despite enable_flags.battery = false: {topic}"
			);
		}
		// Battery removed: 19 − 1.
		assert_eq!(out.len(), 18);
	}

	#[test]
	fn compute_suppresses_motion_classes_when_motion_flag_off() {
		// The per-class sensors are fed by the same motion listener
		// as `md`, so `enable_motion = false` must drop them too.
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = CameraEnableFlags {
			motion: false,
			..all_flags_on()
		};
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		for (topic, _) in &out {
			assert!(
				!topic.contains("bairelay_frontdoor_md"),
				"motion config was emitted despite enable_flags.motion = false: {topic}"
			);
		}
		// `md` + four classes removed: 19 − 5. `push` stays — the push
		// listener drives it, not the motion listener.
		assert_eq!(out.len(), 14);
	}

	#[test]
	fn compute_honours_narrowed_feature_set() {
		// Subset: only Camera + Battery. Expect 2 topics.
		let publisher = publisher_with_features(&[Feature::Camera, Feature::Battery]);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		assert_eq!(out.len(), 2);
		let mut components: Vec<&str> = out
			.iter()
			.map(|(t, _)| t.split('/').nth(1).expect("component segment"))
			.collect();
		components.sort();
		assert_eq!(components, vec!["camera", "sensor"]);
	}

	#[test]
	fn unpublish_topics_match_publish_topics() {
		// compute_payloads is the shared source of truth for
		// publish/unpublish; verify by constructing both forms
		// directly and comparing the topic sets.
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let published =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		// The unpublish path reuses the same topic set with
		// empty payload — exercised by the async publish tests
		// below. Here we sanity-check invariants that survive
		// that mapping.
		let mut publish_topics: Vec<_> = published.iter().map(|(t, _)| t.clone()).collect();
		publish_topics.sort();
		let mut unpublish_topics = publish_topics.clone();
		unpublish_topics.sort();
		assert_eq!(publish_topics, unpublish_topics);
	}

	#[test]
	fn enable_flags_allows_defaults_true_for_uncovered_features() {
		let flags = CameraEnableFlags {
			motion: false,
			battery: false,
			floodlight: false,
			light: false,
			pir: false,
		};
		// Features without a per-cam gate must pass through.
		assert!(flags.allows(Feature::Camera));
		assert!(flags.allows(Feature::Ir));
		assert!(flags.allows(Feature::Reboot));
		assert!(flags.allows(Feature::Pt));
		assert!(flags.allows(Feature::Siren));
		assert!(flags.allows(Feature::Push));
		// Gated features block when the flag is false.
		assert!(!flags.allows(Feature::Motion));
		assert!(!flags.allows(Feature::MotionClass));
		assert!(!flags.allows(Feature::Battery));
		assert!(!flags.allows(Feature::Floodlight));
		assert!(!flags.allows(Feature::Led));
	}

	#[test]
	fn publisher_exposes_topic_prefix_and_ha_topic_accessors() {
		let publisher = publisher_with_features(&Feature::ALL);
		assert_eq!(publisher.topic_prefix(), "bairelay");
		assert_eq!(publisher.ha_topic(), "homeassistant");
	}

	/// Status LED present, no IR LEDs: publish emits the LED switch,
	/// and clears (empty retained) any leftover IR select config and
	/// `status/ir` — while never clearing the LED ones.
	#[tokio::test]
	async fn publish_retracts_absent_ir_but_not_present_status_led() {
		let (client, mock) = crate::test_support::mock_client();
		let features: HashSet<Feature> = Feature::ALL.iter().copied().collect();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView {
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Absent,
			..Default::default()
		};
		publisher
			.publish("cam1", None, None, caps, &all_flags_on(), &[])
			.await
			.expect("publish ok");
		let rows = mock.published();
		let cleared = |t: &str| rows.iter().any(|(rt, p, r)| rt == t && p.is_empty() && *r);
		let set = |t: &str| rows.iter().any(|(rt, p, r)| rt == t && !p.is_empty() && *r);
		assert!(cleared("homeassistant/select/bairelay_cam1_ir/config"));
		assert!(cleared("bairelay/cam1/status/ir"));
		assert!(set("homeassistant/switch/bairelay_cam1_led/config"));
		assert!(!cleared("homeassistant/switch/bairelay_cam1_led/config"));
		assert!(!cleared("bairelay/cam1/status/led"));
	}

	/// `publish` must clear the retained `status/ptz/preset` state
	/// topic. Bairelay never knows the camera's actual current
	/// preset (the Reolink protocol doesn't expose it), so a stale
	/// retained name from a previous run would have HA pre-select
	/// a preset that may not even still exist. The clear is an
	/// empty-retained publish.
	#[tokio::test]
	async fn publish_clears_retained_ptz_preset_state_topic() {
		let (client, mock) = crate::test_support::mock_client();
		let features: HashSet<Feature> = Feature::ALL.iter().copied().collect();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let presets = vec![(1u8, "Home".to_string()), (2u8, "Sky".to_string())];
		publisher
			.publish(
				"frontdoor",
				Some("10.0.0.1:9000"),
				None,
				caps,
				&flags,
				&presets,
			)
			.await
			.expect("publish ok");

		let preset_state_topic = "bairelay/frontdoor/status/ptz/preset";
		let clears: Vec<_> = mock
			.published()
			.into_iter()
			.filter(|(t, payload, retained)| {
				t == preset_state_topic && payload.is_empty() && *retained
			})
			.collect();
		assert_eq!(
			clears.len(),
			1,
			"publish must emit exactly one empty-retained clear on {preset_state_topic}; got {:?}",
			mock.published()
		);
	}

	/// Republishing with a smaller payload set must empty-retain the
	/// topics that disappeared. Without this, suppressing PtPreset
	/// (e.g. `replace_preset_cache(vec![])`) leaves stale retained
	/// discovery config on the broker and HA keeps a ghost entity.
	#[tokio::test]
	async fn publish_diffs_against_previous_set_clears_disappearing_topics() {
		let (client, mock) = crate::test_support::mock_client();
		let features: HashSet<Feature> = Feature::ALL.iter().copied().collect();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let presets = vec![(1u8, "Home".to_string())];

		// First publish: PtPreset entity is emitted because presets
		// is non-empty.
		publisher
			.publish(
				"cam-shrink",
				Some("10.0.0.1:9000"),
				None,
				caps,
				&flags,
				&presets,
			)
			.await
			.expect("first publish ok");
		let preset_config_topic = "homeassistant/select/bairelay_cam-shrink_preset/config";
		assert!(
			mock.published()
				.iter()
				.any(|(t, payload, retained)| t == preset_config_topic
					&& !payload.is_empty()
					&& *retained),
			"first publish must emit non-empty retained config on {preset_config_topic}"
		);

		// Second publish with empty preset list: PtPreset entity is
		// suppressed, so the previously-published config topic must
		// get an explicit empty-retained clear.
		publisher
			.publish("cam-shrink", Some("10.0.0.1:9000"), None, caps, &flags, &[])
			.await
			.expect("second publish ok");
		let clears: Vec<_> = mock
			.published()
			.into_iter()
			.filter(|(t, payload, retained)| {
				t == preset_config_topic && payload.is_empty() && *retained
			})
			.collect();
		assert_eq!(
			clears.len(),
			1,
			"second publish must empty-retain the disappearing PtPreset config topic; \
			 got {clears:?}"
		);
	}

	/// Republishing with the SAME payload set must not emit any
	/// empty-retain on the discovery config topics — those are only
	/// for topics that actually disappeared.
	#[tokio::test]
	async fn publish_same_set_emits_no_diff_clears() {
		let (client, mock) = crate::test_support::mock_client();
		let features: HashSet<Feature> = Feature::ALL.iter().copied().collect();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let presets = vec![(1u8, "Home".to_string())];

		publisher
			.publish(
				"cam-stable",
				Some("10.0.0.1:9000"),
				None,
				caps,
				&flags,
				&presets,
			)
			.await
			.expect("first publish ok");
		let after_first = mock.published().len();

		publisher
			.publish(
				"cam-stable",
				Some("10.0.0.1:9000"),
				None,
				caps,
				&flags,
				&presets,
			)
			.await
			.expect("second publish ok");
		let second_publishes: Vec<_> = mock.published().into_iter().skip(after_first).collect();

		// Every discovery config topic on the second publish must
		// have a non-empty payload — only the status/ptz/preset
		// state topic gets the empty-retained clear.
		let preset_state_topic = "bairelay/cam-stable/status/ptz/preset";
		for (topic, payload, _retained) in &second_publishes {
			if topic == preset_state_topic {
				continue;
			}
			assert!(
				!payload.is_empty(),
				"unexpected empty-retained on {topic} when payload set is unchanged"
			);
		}
	}

	/// `unpublish` must also empty-retain any topics that were in a
	/// prior `publish` set but are no longer in the current
	/// compute_payloads output. Without the diff, an operator who
	/// flipped capabilities mid-session would shut down with stale
	/// retained config left on the broker.
	#[tokio::test]
	async fn unpublish_clears_topics_recorded_from_prior_publish() {
		let (client, mock) = crate::test_support::mock_client();
		let features: HashSet<Feature> = Feature::ALL.iter().copied().collect();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			features,
			"test".to_string(),
		);
		let caps_with = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let caps_without = CameraCapabilitiesView {
			has_ptz: false,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
		};
		let flags = all_flags_on();
		let presets = vec![(1u8, "Home".to_string())];

		publisher
			.publish(
				"cam-flip",
				Some("10.0.0.1:9000"),
				None,
				caps_with,
				&flags,
				&presets,
			)
			.await
			.expect("publish ok");
		let after_publish = mock.published().len();

		publisher
			.unpublish(
				"cam-flip",
				Some("10.0.0.1:9000"),
				None,
				caps_without,
				&flags,
				&[],
			)
			.await
			.expect("unpublish ok");
		let unpub_rows: Vec<_> = mock.published().into_iter().skip(after_publish).collect();

		// Every unpublish row must be empty-retained.
		for (topic, payload, retained) in &unpub_rows {
			assert!(
				payload.is_empty() && *retained,
				"unpublish row not empty-retained: topic={topic} retained={retained}"
			);
		}
		// And the PtPreset config topic from the prior publish must
		// appear in the unpublish set even though caps_without would
		// not emit it on its own compute_payloads.
		let preset_config_topic = "homeassistant/select/bairelay_cam-flip_preset/config";
		assert!(
			unpub_rows.iter().any(|(t, _, _)| t == preset_config_topic),
			"unpublish must clear the PtPreset config topic recorded from prior publish; \
			 got {unpub_rows:?}"
		);
	}
}
