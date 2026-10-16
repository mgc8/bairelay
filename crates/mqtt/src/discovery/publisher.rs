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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::client::SharedMqttClient;
use crate::error::MqttError;

use super::{
	absent_hardware_retractions, build_ai_binary_sensors, build_battery, build_camera,
	build_doorbell, build_floodlight_light, build_floodlight_tasks_switch, build_ir, build_led,
	build_motion, build_pir, build_pt_buttons, build_ptz_presets, build_reboot, build_siren,
	CameraCapabilitiesView, DiscoveryContext, Feature,
};

/// Per-camera enable toggles. Mirrors the binary's per-camera
/// `MqttConfig` flags so this crate does not depend on the binary.
/// `features` without a per-cam gate (`Camera`, `Ir`, `Reboot`,
/// `Pt`, `Siren`) flow through unconditionally; `Ai` and `Doorbell`
/// follow `motion`. See [`Self::allows`].
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
			// AI objects and doorbell presses share the motion
			// listener, so they follow its flag.
			Feature::Motion | Feature::Ai | Feature::Doorbell => self.motion,
			Feature::Battery => self.battery,
			Feature::Floodlight => self.floodlight,
			Feature::Led => self.light,
			Feature::Pir => self.pir,
			// No per-camera enable flag today: gate only via
			// capability detection + global features.
			Feature::Camera
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
	last_published: Arc<Mutex<HashMap<String, Recorded>>>,
	/// Set by [`Self::seal`] at shutdown, before it waits on `in_flight`.
	/// `publish` does nothing once it is set, so no publish re-creates a
	/// config after the shutdown unpublish, even if the seal's wait was
	/// dropped.
	sealed: Arc<AtomicBool>,
	/// Read-held by `publish` across its sends; `seal` takes it for
	/// writing to wait for those publishes to finish.
	in_flight: Arc<tokio::sync::RwLock<()>>,
}

/// A camera's recorded topic set: every topic that may hold a retained
/// config on the broker. `generation` advances on each `publish`, so a
/// publish only forgets the topics it deleted while it is still the
/// latest one.
#[derive(Default)]
struct Recorded {
	topics: HashSet<String>,
	generation: u64,
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
			sealed: Arc::new(AtomicBool::new(false)),
			in_flight: Arc::new(tokio::sync::RwLock::new(())),
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
				Feature::Ai => {
					out.extend(build_ai_binary_sensors(&ctx));
				}
				Feature::Doorbell => {
					if let Some(p) = build_doorbell(&ctx) {
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
	///
	/// Topics hidden only by an `Unknown` presence stay published
	/// (Unknown never deletes); see `swap_last_published`. A deleted
	/// topic stays recorded until its clear is sent, so a publish
	/// dropped mid-way leaves it for the next publish to delete.
	///
	/// A no-op once [`Self::seal`] has run.
	pub async fn publish(
		&self,
		camera_name: &str,
		camera_addr: Option<&str>,
		camera_uid: Option<&str>,
		capabilities: CameraCapabilitiesView,
		enable_flags: &CameraEnableFlags,
		presets: &[(u8, String)],
	) -> Result<(), MqttError> {
		if self.sealed.load(Ordering::SeqCst) {
			return Ok(());
		}
		let _in_flight = self.in_flight.read().await;
		if self.sealed.load(Ordering::SeqCst) {
			return Ok(());
		}
		let payloads = self.compute_payloads(
			camera_name,
			camera_addr,
			camera_uid,
			capabilities,
			enable_flags,
			presets,
		);
		let new_topics: HashSet<String> = payloads.iter().map(|(t, _)| t.clone()).collect();
		let retractions = self.compute_retractions(camera_name, capabilities);
		// Entities hidden only by an `Unknown` presence are candidates
		// to stay on the broker: Unknown never deletes.
		let held_candidates: HashSet<String> = self
			.compute_payloads(
				camera_name,
				camera_addr,
				camera_uid,
				capabilities.unknown_as_present(),
				enable_flags,
				presets,
			)
			.into_iter()
			.map(|(t, _)| t)
			.filter(|t| !new_topics.contains(t) && !retractions.contains(t))
			.collect();
		let (removed, generation) =
			self.swap_last_published(camera_name, new_topics, &held_candidates);
		for (topic, payload) in &payloads {
			self.client.publish_retained(topic, payload).await?;
		}
		// A removed topic that is also retracted is cleared once, below.
		for topic in removed.iter().filter(|t| !retractions.contains(t)) {
			self.client.publish_retained(topic, b"").await?;
		}
		for topic in &retractions {
			self.client.publish_retained(topic, b"").await?;
		}
		self.forget_deleted(camera_name, generation, &removed);
		let preset_state = crate::topics::status_ptz_preset(&self.topic_prefix, camera_name);
		self.client.publish_retained(&preset_state, b"").await?;
		Ok(())
	}

	/// Seal the publisher and every clone: later `publish` calls are
	/// no-ops from the moment this is first polled, even if it is then
	/// dropped. Returns once publishes already in flight have finished.
	/// `unpublish` ignores the seal, so the shutdown fan-out seals first
	/// and then clears.
	pub async fn seal(&self) {
		self.sealed.store(true, Ordering::SeqCst);
		drop(self.in_flight.write().await);
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

	/// Atomically record a camera's new topic set and return the
	/// previously published topics to delete, with the new generation.
	/// The recorded set is `new`, the `held_candidates` that were
	/// previously published, and the deleted topics themselves until
	/// [`Self::forget_deleted`] drops them once their clears are sent.
	/// Read and write share one lock. Lock poisoning is recovered by
	/// reading the inner `HashMap` — the cache is safe to recover (every
	/// entry is a pure value-copy of an already-published topic name).
	fn swap_last_published(
		&self,
		camera: &str,
		mut new: HashSet<String>,
		held_candidates: &HashSet<String>,
	) -> (Vec<String>, u64) {
		let mut guard = self.lock_last_published();
		let entry = guard.entry(camera.to_string()).or_default();
		new.extend(entry.topics.intersection(held_candidates).cloned());
		let removed: Vec<String> = entry.topics.difference(&new).cloned().collect();
		new.extend(removed.iter().cloned());
		entry.topics = new;
		entry.generation += 1;
		(removed, entry.generation)
	}

	/// Drop `deleted` from a camera's recorded set once their clears
	/// are sent, unless a later publish (or `unpublish`) has replaced the
	/// record since `generation`: that one owns the set now.
	fn forget_deleted(&self, camera: &str, generation: u64, deleted: &[String]) {
		let mut guard = self.lock_last_published();
		if let Some(entry) = guard.get_mut(camera) {
			if entry.generation == generation {
				for topic in deleted {
					entry.topics.remove(topic);
				}
			}
		}
	}

	/// Take the recorded last-published topic set for a camera, leaving
	/// it empty under a new generation. Used by `unpublish` so the broker
	/// is fully cleared.
	fn take_last_published(&self, camera: &str) -> HashSet<String> {
		let mut guard = self.lock_last_published();
		let entry = guard.entry(camera.to_string()).or_default();
		entry.generation += 1;
		std::mem::take(&mut entry.topics)
	}

	fn lock_last_published(&self) -> std::sync::MutexGuard<'_, HashMap<String, Recorded>> {
		self.last_published
			.lock()
			.unwrap_or_else(|p| p.into_inner())
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
	fn compute_full_set_with_ptz_emits_18_topics() {
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
			..Default::default()
		};
		let flags = all_flags_on();
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		// Floodlight → 2 (light + tasks switch), Pt → 4, Ai → 3
		// (presence unknown: no face/package), and one each for
		// Camera/Motion/Led/Ir/Reboot/Battery/Siren/Pir/Doorbell.
		// PtPreset is suppressed because we pass `&[]` for presets.
		// 2 + 4 + 3 + 9 = 18.
		assert_eq!(out.len(), 18);

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
		assert_eq!(topics.len(), 18);
	}

	#[test]
	fn compute_without_ptz_drops_four_pt_buttons() {
		let publisher = publisher_with_features(&Feature::ALL);
		let caps = CameraCapabilitiesView {
			has_ptz: false,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
			..Default::default()
		};
		let flags = all_flags_on();
		let out =
			publisher.compute_payloads("frontdoor", Some("10.0.0.1:9000"), None, caps, &flags, &[]);
		assert_eq!(out.len(), 14); // 18 − 4 PT buttons
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
			..Default::default()
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
		// Battery removed: 18 − 1.
		assert_eq!(out.len(), 17);
	}

	#[test]
	fn compute_honours_narrowed_feature_set() {
		// Subset: only Camera + Battery. Expect 2 topics.
		let publisher = publisher_with_features(&[Feature::Camera, Feature::Battery]);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
			..Default::default()
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
			..Default::default()
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
		// Gated features block when the flag is false.
		assert!(!flags.allows(Feature::Motion));
		assert!(!flags.allows(Feature::Battery));
		assert!(!flags.allows(Feature::Floodlight));
		assert!(!flags.allows(Feature::Led));
	}

	#[test]
	fn motion_flag_gates_ai_and_doorbell() {
		// AI objects and doorbell presses arrive on the motion
		// listener, so `enable_motion = false` hides their entities.
		let off = CameraEnableFlags {
			motion: false,
			..all_flags_on()
		};
		assert!(!off.allows(Feature::Ai));
		assert!(!off.allows(Feature::Doorbell));
		assert!(all_flags_on().allows(Feature::Ai));
		assert!(all_flags_on().allows(Feature::Doorbell));

		let publisher = publisher_with_features(&Feature::ALL);
		let out = publisher.compute_payloads(
			"cam1",
			None,
			None,
			CameraCapabilitiesView::default(),
			&off,
			&[],
		);
		for (topic, _) in &out {
			assert!(
				!topic.contains("_ai_")
					&& !topic.contains("_doorbell")
					&& !topic.contains("cam1_md/"),
				"motion-listener entity emitted with motion off: {topic}"
			);
		}
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
			..Default::default()
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
			..Default::default()
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

	/// A presence moving Present -> Unknown (failed probe on reconnect)
	/// hides face/package but must not delete their configs: Unknown
	/// never retracts. A later Absent still clears them.
	#[tokio::test]
	async fn publish_unknown_presence_does_not_delete_earlier_configs() {
		use crate::discovery::Presence::*;
		let (client, mock) = crate::test_support::mock_client();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			Feature::ALL.iter().copied().collect(),
			"test".to_string(),
		);
		let caps = |doorbell| CameraCapabilitiesView {
			ai: Present,
			doorbell,
			..Default::default()
		};
		let flags = all_flags_on();
		let face = "homeassistant/binary_sensor/bairelay_cam1_ai_face/config";
		let package = "homeassistant/binary_sensor/bairelay_cam1_ai_package/config";
		let cleared = |t: &str| {
			mock.published()
				.iter()
				.filter(|(rt, p, r)| rt == t && p.is_empty() && *r)
				.count()
		};
		publisher
			.publish("cam1", None, None, caps(Present), &flags, &[])
			.await
			.expect("first publish");
		publisher
			.publish("cam1", None, None, caps(Unknown), &flags, &[])
			.await
			.expect("unknown publish");
		assert_eq!(cleared(face), 0, "Unknown must not delete face config");
		assert_eq!(
			cleared(package),
			0,
			"Unknown must not delete package config"
		);
		publisher
			.publish("cam1", None, None, caps(Absent), &flags, &[])
			.await
			.expect("absent publish");
		assert_eq!(cleared(face), 1, "Absent clears face config once");
		assert_eq!(cleared(package), 1, "Absent clears package config once");
	}

	/// A `publish` dropped after it recorded the new set but before its
	/// deletions ran keeps those topics recorded, so the next publish
	/// still deletes them.
	#[tokio::test]
	async fn publish_dropped_before_deletions_keeps_them_pending() {
		let (client, mock) = crate::test_support::mock_client();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			Feature::ALL.iter().copied().collect(),
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			..Default::default()
		};
		let flags = all_flags_on();
		let preset = "homeassistant/select/bairelay_cam1_preset/config";
		let count = |empty: bool| {
			mock.published()
				.iter()
				.filter(|(t, p, r)| t == preset && p.is_empty() == empty && *r)
				.count()
		};
		publisher
			.publish("cam1", None, None, caps, &flags, &[(1, "Home".into())])
			.await
			.expect("first publish");
		assert_eq!(count(false), 1, "precondition: preset config published");

		// A clone whose request channel never drains (rendezvous channel,
		// receiver never polled): its publish records the new set, then
		// stalls on the first send. Poll it once, then drop it.
		let (stalled_client, event_loop) =
			rumqttc::AsyncClient::new(rumqttc::MqttOptions::new("stalled", "127.0.0.1", 1883), 0);
		Box::leak(Box::new(event_loop));
		let mut stalled = publisher.clone();
		stalled.client = SharedMqttClient::for_test(stalled_client);
		let stalled_publish = stalled.publish("cam1", None, None, caps, &flags, &[]);
		let finished = tokio::select! {
			biased;
			_ = stalled_publish => true,
			_ = std::future::ready(()) => false,
		};
		assert!(!finished, "precondition: stalled publish pends");
		assert_eq!(
			count(true),
			0,
			"precondition: preset config not cleared yet"
		);

		publisher
			.publish("cam1", None, None, caps, &flags, &[])
			.await
			.expect("third publish");
		assert_eq!(count(true), 1, "next publish clears the preset config");
	}

	/// After `seal`, `publish` sends nothing; `unpublish` still clears.
	#[tokio::test]
	async fn sealed_publish_is_a_noop_and_unpublish_still_clears() {
		let (client, mock) = crate::test_support::mock_client();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			Feature::ALL.iter().copied().collect(),
			"test".to_string(),
		);
		let caps = CameraCapabilitiesView::default();
		let flags = all_flags_on();
		publisher
			.publish("cam1", None, None, caps, &flags, &[])
			.await
			.expect("first publish");
		let published = mock.count();
		assert!(published > 0, "precondition: unsealed publish sends");

		publisher.clone().seal().await;
		publisher
			.publish("cam1", None, None, caps, &flags, &[])
			.await
			.expect("sealed publish");
		assert_eq!(mock.count(), published, "sealed publish sends nothing");

		publisher
			.unpublish("cam1", None, None, caps, &flags, &[])
			.await
			.expect("unpublish");
		let rows = mock.published();
		assert!(
			rows[published..].iter().all(|(_, p, r)| p.is_empty() && *r),
			"unpublish sends only empty retains"
		);
		assert!(rows.len() > published, "unpublish clears after sealing");
	}

	/// A publisher on a one-slot channel whose event loop the test
	/// drains by hand, with a publish in flight stalled on its 2nd send.
	struct StalledPublish {
		publisher: DiscoveryPublisher,
		event_loop: rumqttc::EventLoop,
		sink: crate::client::CaptureSink,
		in_flight: tokio::task::JoinHandle<Result<(), MqttError>>,
	}

	impl StalledPublish {
		async fn start() -> Self {
			let (client, event_loop) =
				rumqttc::AsyncClient::new(rumqttc::MqttOptions::new("seal", "127.0.0.1", 1883), 1);
			let sink = crate::client::CaptureSink::default();
			let shared = SharedMqttClient::__test_new_with_capture(client, Arc::clone(&sink));
			let publisher = DiscoveryPublisher::new(
				shared,
				"bairelay".to_string(),
				"homeassistant".to_string(),
				Feature::ALL.iter().copied().collect(),
				"test".to_string(),
			);
			let in_flight = tokio::spawn({
				let publisher = publisher.clone();
				async move { publisher.publish_default("cam1").await }
			});
			let stalled = Self {
				publisher,
				event_loop,
				sink,
				in_flight,
			};
			for _ in 0..100 {
				if stalled.recorded() >= 2 {
					break;
				}
				tokio::task::yield_now().await;
			}
			assert_eq!(
				stalled.recorded(),
				2,
				"precondition: publish stalls on its 2nd send"
			);
			stalled
		}

		fn recorded(&self) -> usize {
			self.sink.lock().unwrap().len()
		}
	}

	impl DiscoveryPublisher {
		/// `publish` with default caps, all flags on, no presets.
		async fn publish_default(&self, camera: &str) -> Result<(), MqttError> {
			let caps = CameraCapabilitiesView::default();
			self.publish(camera, None, None, caps, &all_flags_on(), &[])
				.await
		}
	}

	/// Poll `fut` once; `true` if it completed.
	async fn poll_once(fut: impl std::future::Future) -> bool {
		tokio::select! {
			biased;
			_ = fut => true,
			_ = std::future::ready(()) => false,
		}
	}

	/// A `seal` dropped before it drained in-flight publishes (the
	/// shutdown fan-out timed out) still seals: later publishes are
	/// no-ops.
	#[tokio::test]
	async fn dropped_seal_still_seals() {
		let stalled = StalledPublish::start().await;
		assert!(
			!poll_once(stalled.publisher.seal()).await,
			"precondition: seal waits for the in-flight publish"
		);
		let before = stalled.recorded();
		assert!(
			poll_once(stalled.publisher.publish_default("cam1")).await,
			"later publish returns at once"
		);
		assert_eq!(stalled.recorded(), before, "later publish sends nothing");
	}

	/// `seal` waits for a publish already in flight: it returns only
	/// after that publish has sent everything.
	#[tokio::test]
	async fn seal_waits_for_in_flight_publish() {
		let StalledPublish {
			publisher,
			mut event_loop,
			sink,
			in_flight,
		} = StalledPublish::start().await;
		let recorded = move || sink.lock().unwrap().len();

		let seal = tokio::spawn({
			let publisher = publisher.clone();
			async move { publisher.seal().await }
		});
		for _ in 0..10 {
			tokio::task::yield_now().await;
		}
		assert!(!seal.is_finished(), "seal waits for the in-flight publish");

		let mut drained = 0;
		for _ in 0..1000 {
			if seal.is_finished() {
				break;
			}
			event_loop.clean();
			drained += event_loop.pending.drain(..).count();
			tokio::task::yield_now().await;
		}
		assert!(seal.is_finished(), "seal returns once the publish drains");
		assert!(
			in_flight.is_finished(),
			"publish finished before seal returned"
		);
		in_flight.await.unwrap().expect("in-flight publish ok");
		event_loop.clean();
		drained += event_loop.pending.drain(..).count();
		assert_eq!(drained, recorded(), "every recorded publish was sent");
	}

	/// A publish forgets its deleted topics only while it is the latest:
	/// a topic a later publish re-recorded stays recorded.
	#[test]
	fn forget_deleted_skips_superseded_generation() {
		let publisher = publisher_with_features(&[]);
		let set = |ts: &[&str]| ts.iter().map(|t| t.to_string()).collect::<HashSet<_>>();
		let none = HashSet::new();
		publisher.swap_last_published("cam1", set(&["a"]), &none);
		let (removed, generation) = publisher.swap_last_published("cam1", set(&[]), &none);
		assert_eq!(removed, vec!["a".to_string()]);
		publisher.swap_last_published("cam1", set(&["a"]), &none);
		publisher.forget_deleted("cam1", generation, &removed);
		assert_eq!(publisher.take_last_published("cam1"), set(&["a"]));

		let (removed, generation) = publisher.swap_last_published("cam1", set(&[]), &none);
		assert!(removed.is_empty(), "unpublish emptied the record");
		publisher.swap_last_published("cam1", set(&["b"]), &none);
		let (removed, _) = publisher.swap_last_published("cam1", set(&[]), &none);
		publisher.forget_deleted("cam1", generation, &removed);
		assert_eq!(publisher.take_last_published("cam1"), set(&["b"]));
	}

	/// Topics excluded by the per-camera flags or the global `features`
	/// are still deleted when a presence goes Unknown; only the ones
	/// hidden by Unknown alone are kept.
	#[tokio::test]
	async fn publish_unknown_still_deletes_excluded_topics() {
		use crate::discovery::Presence::*;
		let caps = |p| CameraCapabilitiesView {
			ai: p,
			doorbell: p,
			..Default::default()
		};
		let ai = "homeassistant/binary_sensor/bairelay_cam1_ai_person/config";
		let door = "homeassistant/event/bairelay_cam1_doorbell/config";
		let face = "homeassistant/binary_sensor/bairelay_cam1_ai_face/config";
		let motion = "homeassistant/binary_sensor/bairelay_cam1_md/config";

		// Flag exclusion: motion off drops ai + doorbell + motion.
		let (client, mock) = crate::test_support::mock_client();
		let publisher = DiscoveryPublisher::new(
			client,
			"bairelay".to_string(),
			"homeassistant".to_string(),
			Feature::ALL.iter().copied().collect(),
			"test".to_string(),
		);
		let on = all_flags_on();
		let off = CameraEnableFlags {
			motion: false,
			..on
		};
		publisher
			.publish("cam1", None, None, caps(Present), &on, &[])
			.await
			.expect("first");
		publisher
			.publish("cam1", None, None, caps(Unknown), &off, &[])
			.await
			.expect("second");
		let cleared = |t: &str| {
			mock.published()
				.iter()
				.filter(|(rt, p, r)| rt == t && p.is_empty() && *r)
				.count()
		};
		for t in [ai, door, face, motion] {
			assert_eq!(cleared(t), 1, "{t} excluded by flags: cleared once");
		}

		// Feature exclusion: `doorbell` omitted drops the doorbell config
		// even though its presence is Unknown; face/package, hidden only
		// by the Unknown doorbell presence, stay.
		let (client, mock) = crate::test_support::mock_client();
		let first = DiscoveryPublisher::new(
			client.clone(),
			"bairelay".to_string(),
			"homeassistant".to_string(),
			Feature::ALL.iter().copied().collect(),
			"test".to_string(),
		);
		first
			.publish("cam1", None, None, caps(Present), &on, &[])
			.await
			.expect("first");
		let mut narrowed = first.clone();
		narrowed.features.remove(&Feature::Doorbell);
		let ai_present = CameraCapabilitiesView {
			ai: Present,
			doorbell: Unknown,
			..Default::default()
		};
		narrowed
			.publish("cam1", None, None, ai_present, &on, &[])
			.await
			.expect("second");
		let cleared = |t: &str| {
			mock.published()
				.iter()
				.filter(|(rt, p, r)| rt == t && p.is_empty() && *r)
				.count()
		};
		let package = "homeassistant/binary_sensor/bairelay_cam1_ai_package/config";
		assert_eq!(
			cleared(door),
			1,
			"doorbell excluded by features: cleared once"
		);
		assert_eq!(cleared(face), 0, "face hidden by Unknown only: kept");
		assert_eq!(cleared(package), 0, "package hidden by Unknown only: kept");
		assert_eq!(cleared(ai), 0, "person still emitted: kept");
	}

	/// An explicit `features` list without `ai` / `doorbell` emits no
	/// AI or doorbell entities, while retractions still follow presence.
	#[tokio::test]
	async fn explicit_features_without_ai_or_doorbell_emit_none() {
		use crate::discovery::Presence::*;
		let publisher = publisher_with_features(&[Feature::Motion, Feature::Battery]);
		let caps = CameraCapabilitiesView {
			ai: Present,
			doorbell: Present,
			..Default::default()
		};
		let out = publisher.compute_payloads("cam1", None, None, caps, &all_flags_on(), &[]);
		assert!(
			out.iter()
				.all(|(t, _)| !t.contains("_ai_") && !t.contains("doorbell")),
			"unexpected entities: {:?}",
			out.iter().map(|(t, _)| t).collect::<Vec<_>>()
		);
		assert!(publisher.compute_retractions("cam1", caps).is_empty());
		let absent = CameraCapabilitiesView {
			ai: Absent,
			doorbell: Absent,
			..Default::default()
		};
		let retracted = publisher.compute_retractions("cam1", absent);
		assert!(retracted.iter().any(|t| t.contains("_ai_person")));
		assert!(retracted.iter().any(|t| t.contains("_doorbell")));
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
			..Default::default()
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
			..Default::default()
		};
		let caps_without = CameraCapabilitiesView {
			has_ptz: false,
			status_led: crate::discovery::Presence::Present,
			ir_leds: crate::discovery::Presence::Present,
			..Default::default()
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
