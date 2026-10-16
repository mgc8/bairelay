//! Home Assistant MQTT discovery payload types and pure builders.
//!
//! Mirrors Neolink's HA discovery payloads, with the following
//! deliberate deviations:
//!
//! - `device.model` is the literal `"Bairelay"` (neolink's is
//!   `"Neolink"`). Model is our software identity, not a
//!   compat-switch knob.
//! - `unique_id` / `identifiers` embed the configurable
//!   `topic_prefix` (default `"bairelay"`, `"neolink"` for drop-in
//!   migration).
//! - The typo `DiscoveryAvaliablity` is fixed to
//!   `DiscoveryAvailability`.
//! - Floodlight state topic keeps neolink's JSON-template shape —
//!   `StatusPublisher::publish_floodlight` emits
//!   `{"state":"on"}` and HA's `state_value_template` templates it
//!   out.
//!
//! Each `build_*` function returns `(topic, payload_bytes)` ready to
//! hand straight to `SharedMqttClient::publish_retained`. All
//! builders are pure — no I/O, no async — so the discovery publisher
//! (Task 11) can test its topic set without a broker.

use crate::ai::AiObject;
use crate::topics;
use serde::{Deserialize, Serialize, Serializer};

pub mod publisher;

pub use publisher::{CameraEnableFlags, DiscoveryPublisher};

// ── Feature enum ──────────────────────────────────────────────────────

/// Every HA discovery feature bairelay knows how to emit.
///
/// Drives three things at runtime:
///
/// 1. The `[mqtt.discovery] features = [...]` config: operators can
///    narrow the default full-set down to the ones they care about.
/// 2. The publisher loop in [`publisher`] — each variant maps to one
///    or more `build_*` calls.
/// 3. Per-camera capability / enable gating: PT is suppressed when
///    `has_ptz = false`, motion is suppressed when the per-cam
///    `enable_motion = false`, etc.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
	Floodlight,
	Camera,
	Motion,
	Led,
	Ir,
	Reboot,
	Pt,
	PtPreset,
	Battery,
	Siren,
	Pir,
	Ai,
	Doorbell,
}

impl Feature {
	/// Full set of features emitted when `[mqtt.discovery]` is
	/// present and no explicit `features = [...]` list is supplied.
	pub const ALL: [Feature; 13] = [
		Feature::Floodlight,
		Feature::Camera,
		Feature::Motion,
		Feature::Led,
		Feature::Ir,
		Feature::Reboot,
		Feature::Pt,
		Feature::PtPreset,
		Feature::Battery,
		Feature::Siren,
		Feature::Pir,
		Feature::Ai,
		Feature::Doorbell,
	];
}

// ── Capability view ──────────────────────────────────────────────────

/// Crate-local copy of the binary's `CameraCapabilities`. Duplicated
/// to avoid a dep from `bairelay_mqtt` on the binary crate. The
/// binary constructs this at publish time from its own cache.
#[derive(Debug, Default, Clone, Copy)]
pub struct CameraCapabilitiesView {
	pub has_ptz: bool,
	/// Status LED — gates the LED switch.
	pub status_led: Presence,
	/// IR night-vision LEDs — gates the IR select.
	pub ir_leds: Presence,
	/// Per-object AI detection — gates the person/vehicle/animal
	/// binary sensors (and, with `doorbell`, face/package).
	pub ai: Presence,
	/// Doorbell button — gates the press event entity and the
	/// doorbell-only AI sensors (face, package).
	pub doorbell: Presence,
}

impl CameraCapabilitiesView {
	/// Whether discovery emits `object`'s entity: `ai` is not absent
	/// and, for the doorbell-only objects, `doorbell` is present. Not
	/// the inverse of [`Self::retracts_ai_object`]: an unknown
	/// `doorbell` neither emits nor retracts face/package.
	pub fn emits_ai_object(&self, object: AiObject) -> bool {
		!self.ai.is_absent() && (!object.is_doorbell_only() || self.doorbell.is_present())
	}

	/// Copy with every `Unknown` presence read as `Present`: the
	/// entity set that could exist once the unknowns resolve.
	pub fn unknown_as_present(&self) -> Self {
		Self {
			has_ptz: self.has_ptz,
			status_led: self.status_led.unknown_as_present(),
			ir_leds: self.ir_leds.unknown_as_present(),
			ai: self.ai.unknown_as_present(),
			doorbell: self.doorbell.unknown_as_present(),
		}
	}

	/// Whether discovery retracts `object`'s entity and status topic:
	/// every object when `ai` is absent, and the doorbell-only objects
	/// when `doorbell` is absent.
	pub fn retracts_ai_object(&self, object: AiObject) -> bool {
		self.ai.is_absent() || (object.is_doorbell_only() && self.doorbell.is_absent())
	}
}

/// Whether a camera has a piece of optional hardware. `Unknown` when
/// the signal wasn't available (probe failed, field absent) — never
/// treated as absent, so a failed probe can't hide or retract an
/// entity.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
	#[default]
	Unknown,
	Present,
	Absent,
}

impl Presence {
	/// `Some(true)` → Present, `Some(false)` → Absent, `None` → Unknown.
	pub fn from_signal(signal: Option<bool>) -> Self {
		match signal {
			Some(true) => Presence::Present,
			Some(false) => Presence::Absent,
			None => Presence::Unknown,
		}
	}
	/// `Unknown` read as `Present`; `Present` and `Absent` unchanged.
	pub fn unknown_as_present(self) -> Self {
		if self == Presence::Unknown {
			Presence::Present
		} else {
			self
		}
	}
	pub fn is_present(self) -> bool {
		self == Presence::Present
	}
	pub fn is_absent(self) -> bool {
		self == Presence::Absent
	}
}

// ── Payload type definitions ─────────────────────────────────────────

/// Connection tuple (`[type, id]`) in the HA discovery device block.
/// Neolink emits e.g. `["camera_addr", "192.168.1.10:9000"]`. Matches
/// HA's expected 2-tuple wire shape via a custom `Serialize`.
#[derive(Debug, Clone)]
pub struct DiscoveryConnection {
	pub connection_type: String,
	pub connection_id: String,
}

impl Serialize for DiscoveryConnection {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		vec![&self.connection_type, &self.connection_id].serialize(serializer)
	}
}

/// HA device block attached to every entity this camera emits.
#[derive(Serialize, Debug, Clone)]
pub struct DiscoveryDevice {
	pub name: String,
	pub connections: Vec<DiscoveryConnection>,
	pub identifiers: Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub manufacturer: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub model: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sw_version: Option<String>,
}

/// HA availability block. `payload_not_available` intentionally
/// omitted — HA treats any other value as unavailable.
#[derive(Serialize, Debug, Clone)]
pub struct DiscoveryAvailability {
	pub topic: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload_available: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload_not_available: Option<String>,
}

/// Encoding hint for the HA `camera` entity's preview topic. Only
/// `Base64` is emitted today (preview JPEGs are base64-encoded by
/// `StatusPublisher::publish_preview`). Neolink's enum also has a
/// `None` variant serialised as-omitted; we drop it because no
/// consumer in this crate picks it and the plan bars new
/// `#[allow(dead_code)]` markers.
#[derive(Serialize, Debug, Clone, Copy)]
pub enum Encoding {
	#[serde(rename = "b64")]
	Base64,
}

#[derive(Serialize, Debug)]
pub struct DiscoveryLight {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_topic: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_value_template: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub command_topic: Option<String>,
	pub payload_on: String,
	pub payload_off: String,
}

#[derive(Serialize, Debug)]
pub struct DiscoveryCamera {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub topic: String,
	pub image_encoding: Encoding,
}

#[derive(Serialize, Debug)]
pub struct DiscoverySwitch {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub command_topic: String,
	pub payload_off: String,
	pub payload_on: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_topic: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_off: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_on: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct DiscoverySelect {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub command_topic: String,
	pub options: Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_topic: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct DiscoveryBinarySensor {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub device_class: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub payload_off: String,
	pub payload_on: String,
	pub state_topic: String,
}

/// HA MQTT `event` entity (doorbell press).
#[derive(Serialize, Debug)]
pub struct DiscoveryEvent {
	pub name: String,
	pub unique_id: String,
	pub device_class: String,
	pub icon: String,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub event_types: Vec<String>,
	pub state_topic: String,
	pub value_template: String,
}

#[derive(Serialize, Debug)]
pub struct DiscoveryButton {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub command_topic: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload_press: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct DiscoverySensor {
	pub name: String,
	pub unique_id: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub icon: Option<String>,
	pub device: DiscoveryDevice,
	pub availability: DiscoveryAvailability,
	pub state_topic: String,
	pub state_class: String,
	pub unit_of_measurement: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub device_class: Option<String>,
}

// ── Context ──────────────────────────────────────────────────────────

/// Bundle of everything a `build_*` function needs. Constructed once
/// per camera at publish time, consumed by every builder on the same
/// pass so they all see a consistent view.
pub struct DiscoveryContext<'a> {
	pub topic_prefix: &'a str,
	pub ha_topic: &'a str,
	pub camera_name: &'a str,
	pub camera_addr: Option<&'a str>,
	pub camera_uid: Option<&'a str>,
	pub sw_version: &'a str,
	pub capabilities: &'a CameraCapabilitiesView,
	/// Cached PTZ preset list `(id, name)`. Populated when the binary
	/// has called `get_ptz_preset()` for this camera; empty otherwise.
	/// Drives `build_ptz_presets`'s select options.
	pub presets: &'a [(u8, String)],
}

// ── Helpers ──────────────────────────────────────────────────────────

/// ASCII-only title-case helper: `front_door` -> `Front Door`.
/// Splits on `_` AND `-` so an operator naming their camera
/// `front-door` or `cam_back-yard` gets the same per-word display
/// treatment in HA. Embedded caps after the first char of each
/// segment are preserved (`MyCamera` -> `MyCamera`, `4K_Terrace`
/// -> `4K Terrace`, `IPCam-front` -> `IPCam Front`); only a
/// lowercase first char is upgraded to uppercase. Camera names
/// are validated in `src/config.rs` to contain only alphanumeric +
/// `_` + `-`, so this stays a closed-world ASCII transform.
fn title_case(name: &str) -> String {
	name.split(['_', '-'])
		.map(|word| {
			let mut chars = word.chars();
			match chars.next() {
				Some(first) => {
					let head = first.to_ascii_uppercase();
					let tail: &str = chars.as_str();
					format!("{head}{tail}")
				}
				None => String::new(),
			}
		})
		.collect::<Vec<_>>()
		.join(" ")
}

fn identifier(prefix: &str, cam: &str) -> String {
	format!("{prefix}_{cam}")
}

fn unique(prefix: &str, cam: &str, suffix: &str) -> String {
	format!("{prefix}_{cam}_{suffix}")
}

fn availability_block(ctx: &DiscoveryContext) -> DiscoveryAvailability {
	DiscoveryAvailability {
		topic: format!("{}/{}/status", ctx.topic_prefix, ctx.camera_name),
		payload_available: Some("connected".to_string()),
		payload_not_available: None,
	}
}

fn device_block(ctx: &DiscoveryContext, friendly: &str) -> DiscoveryDevice {
	let mut connections = Vec::new();
	if let Some(addr) = ctx.camera_addr {
		connections.push(DiscoveryConnection {
			connection_type: "camera_addr".to_string(),
			connection_id: addr.to_string(),
		});
	}
	if let Some(uid) = ctx.camera_uid {
		connections.push(DiscoveryConnection {
			connection_type: "camera_uid".to_string(),
			connection_id: uid.to_string(),
		});
	}
	DiscoveryDevice {
		name: friendly.to_string(),
		connections,
		identifiers: vec![identifier(ctx.topic_prefix, ctx.camera_name)],
		manufacturer: Some("Reolink".to_string()),
		model: Some("Bairelay".to_string()),
		sw_version: Some(ctx.sw_version.to_string()),
	}
}

fn cam_topic(ctx: &DiscoveryContext, category: &str, leaf: &str) -> String {
	format!(
		"{}/{}/{}/{}",
		ctx.topic_prefix, ctx.camera_name, category, leaf
	)
}

fn config_topic(ctx: &DiscoveryContext, component: &str, unique_id: &str) -> String {
	format!("{}/{}/{}/config", ctx.ha_topic, component, unique_id)
}

fn to_json(value: &impl Serialize) -> Vec<u8> {
	// Infallible for the types defined here (all `Serialize` derives
	// over plain strings / enums). `expect` panics loudly if that
	// ever regresses — better than returning an ambiguous `Result` up
	// the call stack.
	serde_json::to_vec(value).expect("discovery payload serialises to JSON")
}

// ── Builders (one per feature/entity) ────────────────────────────────

/// `light` entity driving the floodlight on/off + JSON state topic.
pub fn build_floodlight_light(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "floodlight");
	let payload = DiscoveryLight {
		name: format!("{friendly} Floodlight"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:spotlight-beam".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		state_topic: Some(cam_topic(ctx, "status", "floodlight")),
		state_value_template: Some("{{ value_json.state }}".to_string()),
		command_topic: Some(cam_topic(ctx, "control", "floodlight")),
		payload_on: "on".to_string(),
		payload_off: "off".to_string(),
	};
	Some((config_topic(ctx, "light", &unique_id), to_json(&payload)))
}

/// `switch` entity driving the floodlight task schedule.
pub fn build_floodlight_tasks_switch(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "floodlight_tasks");
	let payload = DiscoverySwitch {
		name: format!("{friendly} Floodlight Tasks"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:spotlight-beam".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "floodlight_tasks"),
		payload_off: "off".to_string(),
		payload_on: "on".to_string(),
		state_topic: Some(cam_topic(ctx, "status", "floodlight_tasks")),
		state_off: Some("off".to_string()),
		state_on: Some("on".to_string()),
	};
	Some((config_topic(ctx, "switch", &unique_id), to_json(&payload)))
}

/// `camera` entity displaying the base64 preview JPEG.
pub fn build_camera(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "camera");
	let payload = DiscoveryCamera {
		name: format!("{friendly} Camera"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:camera-iris".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		topic: cam_topic(ctx, "status", "preview"),
		image_encoding: Encoding::Base64,
	};
	Some((config_topic(ctx, "camera", &unique_id), to_json(&payload)))
}

/// `binary_sensor` entity reporting motion events. Neolink uses
/// `_md` (not `_motion`) in the unique_id; we preserve that for
/// drop-in HA-registry compatibility when operators migrate.
pub fn build_motion(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "md");
	let payload = DiscoveryBinarySensor {
		name: format!("{friendly} MD"),
		unique_id: unique_id.clone(),
		device_class: None,
		icon: Some("mdi:motion-sensor".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		payload_off: "off".to_string(),
		payload_on: "on".to_string(),
		state_topic: cam_topic(ctx, "status", "motion"),
	};
	Some((
		config_topic(ctx, "binary_sensor", &unique_id),
		to_json(&payload),
	))
}

/// `(unique_id, config topic)` of the status-LED switch.
fn led_ids(ctx: &DiscoveryContext) -> (String, String) {
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "led");
	let topic = config_topic(ctx, "switch", &unique_id);
	(unique_id, topic)
}

/// `(unique_id, config topic)` of the IR-mode select.
fn ir_ids(ctx: &DiscoveryContext) -> (String, String) {
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "ir");
	let topic = config_topic(ctx, "select", &unique_id);
	(unique_id, topic)
}

/// Retained topics to clear for hardware the camera is confirmed to lack
/// (`Presence::Absent`; `Unknown` never retracts):
/// each entity's discovery config plus its `status/...` topic. Clears
/// leftovers from earlier versions (which published both entities
/// unconditionally) or from an unclean shutdown. Status LED and IR
/// LEDs are judged independently.
pub fn absent_hardware_retractions(ctx: &DiscoveryContext) -> Vec<String> {
	let mut out = Vec::new();
	if ctx.capabilities.status_led.is_absent() {
		out.push(led_ids(ctx).1);
		out.push(cam_topic(ctx, "status", "led"));
	}
	if ctx.capabilities.ir_leds.is_absent() {
		out.push(ir_ids(ctx).1);
		out.push(cam_topic(ctx, "status", "ir"));
	}
	let caps = ctx.capabilities;
	if caps.doorbell.is_absent() {
		out.push(doorbell_ids(ctx).1);
	}
	for object in AiObject::ALL {
		if caps.retracts_ai_object(object) {
			out.push(ai_ids(ctx, object).1);
			out.push(topics::status_ai(ctx.topic_prefix, ctx.camera_name, object));
		}
	}
	out
}

/// `(unique_id, config topic)` of the doorbell press event.
fn doorbell_ids(ctx: &DiscoveryContext) -> (String, String) {
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "doorbell");
	let topic = config_topic(ctx, "event", &unique_id);
	(unique_id, topic)
}

/// `(unique_id, config topic)` of one AI object binary sensor.
fn ai_ids(ctx: &DiscoveryContext, object: AiObject) -> (String, String) {
	let suffix = format!("ai_{}", object.topic_name());
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, &suffix);
	let topic = config_topic(ctx, "binary_sensor", &unique_id);
	(unique_id, topic)
}

/// `binary_sensor` entities for the per-object AI detections. Person,
/// vehicle and animal are emitted unless `ai` is absent; face and
/// package additionally need `doorbell` present.
pub fn build_ai_binary_sensors(ctx: &DiscoveryContext) -> Vec<(String, Vec<u8>)> {
	let caps = ctx.capabilities;
	let friendly = title_case(ctx.camera_name);
	AiObject::ALL
		.into_iter()
		.filter(|&o| caps.emits_ai_object(o))
		.map(|object| {
			let (unique_id, topic) = ai_ids(ctx, object);
			let payload = DiscoveryBinarySensor {
				name: format!("{friendly} {}", object.display_name()),
				unique_id,
				device_class: Some(object.device_class().to_string()),
				icon: Some(object.icon().to_string()),
				device: device_block(ctx, &friendly),
				availability: availability_block(ctx),
				payload_off: "off".to_string(),
				payload_on: "on".to_string(),
				state_topic: topics::status_ai(ctx.topic_prefix, ctx.camera_name, object),
			};
			(topic, to_json(&payload))
		})
		.collect()
}

/// `event` entity firing on each doorbell button press. The plain
/// `press` payload is wrapped into the JSON HA's event platform expects.
pub fn build_doorbell(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	if ctx.capabilities.doorbell.is_absent() {
		return None;
	}
	let friendly = title_case(ctx.camera_name);
	let (unique_id, topic) = doorbell_ids(ctx);
	let payload = DiscoveryEvent {
		name: format!("{friendly} Doorbell"),
		unique_id,
		device_class: "doorbell".to_string(),
		icon: "mdi:doorbell".to_string(),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		event_types: vec!["press".to_string()],
		state_topic: topics::status_doorbell(ctx.topic_prefix, ctx.camera_name),
		value_template: r#"{"event_type":"{{ value }}"}"#.to_string(),
	};
	Some((topic, to_json(&payload)))
}

/// `switch` entity driving the status LED. State mirrors the
/// `lightState` bairelay reads via `get_ledstate()` on connect and
/// republishes after every `control/led`.
pub fn build_led(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let presence = ctx.capabilities.status_led;
	if presence.is_absent() {
		return None;
	}
	let friendly = title_case(ctx.camera_name);
	let (unique_id, topic) = led_ids(ctx);
	let payload = DiscoverySwitch {
		name: format!("{friendly} LED"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:led-on".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "led"),
		payload_off: "off".to_string(),
		payload_on: "on".to_string(),
		// State only for confirmed hardware: with presence unknown the
		// reply may be a placeholder, so no `status/led` is published.
		state_topic: presence
			.is_present()
			.then(|| cam_topic(ctx, "status", "led")),
		state_off: presence.is_present().then(|| "off".to_string()),
		state_on: presence.is_present().then(|| "on".to_string()),
	};
	Some((topic, to_json(&payload)))
}

/// `select` entity driving the IR night-vision mode. State mirrors the
/// LED-status `state` field (configured mode, not the live day/night
/// switch), read on connect and republished after every `control/ir`.
pub fn build_ir(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let presence = ctx.capabilities.ir_leds;
	if presence.is_absent() {
		return None;
	}
	let friendly = title_case(ctx.camera_name);
	let (unique_id, topic) = ir_ids(ctx);
	let payload = DiscoverySelect {
		name: format!("{friendly} IR"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:lightbulb-night".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "ir"),
		options: vec!["on".to_string(), "off".to_string(), "auto".to_string()],
		state_topic: presence
			.is_present()
			.then(|| cam_topic(ctx, "status", "ir")),
	};
	Some((topic, to_json(&payload)))
}

/// `switch` entity driving the PIR (passive infrared) motion
/// detector. State topic mirrors the `enable` flag bairelay reads via
/// `get_pirstate()` and republishes after every `pir_set`. Distinct
/// from IR (the night-vision LED): PIR detects motion by body heat;
/// IR illuminates the scene at night. Per-camera `enable_pir` (in
/// `[cameras.mqtt]`) gates whether bairelay polls / publishes state
/// at all — without it the entity would render `unknown`.
pub fn build_pir(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "pir");
	let payload = DiscoverySwitch {
		name: format!("{friendly} PIR"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:motion-sensor".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "pir"),
		payload_off: "off".to_string(),
		payload_on: "on".to_string(),
		state_topic: Some(cam_topic(ctx, "status", "pir")),
		state_off: Some("off".to_string()),
		state_on: Some("on".to_string()),
	};
	Some((config_topic(ctx, "switch", &unique_id), to_json(&payload)))
}

/// `select` entity exposing the camera's PTZ preset list. Options
/// come from `ctx.presets` (populated by the binary at connect via
/// `get_ptz_preset()`); when the cache is empty (camera hasn't been
/// queried yet, or get_ptz_preset failed) we suppress the entity to
/// avoid emitting an empty select. Gated on `has_ptz`. Selection
/// publishes the preset NAME on `control/ptz/preset`; the dispatcher
/// resolves name → id via the same cached list.
pub fn build_ptz_presets(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	if !ctx.capabilities.has_ptz {
		return None;
	}
	if ctx.presets.is_empty() {
		return None;
	}
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "preset");
	let mut options: Vec<String> = ctx.presets.iter().map(|(_, name)| name.clone()).collect();
	// HA select requires at least one option. Deduplicate while
	// preserving order — Reolink occasionally returns repeat entries.
	let mut seen = std::collections::HashSet::new();
	options.retain(|n| seen.insert(n.clone()));
	let payload = DiscoverySelect {
		name: format!("{friendly} Preset"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:target".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "ptz/preset"),
		options,
		// Production publishes the active preset on the canonical
		// slashed topic via `topics::status_ptz_preset` (5 segments).
		// Pre-fix this used `cam_topic(ctx, "status", "ptz_preset")`
		// (4 segments, underscore not slash) — HA subscribed to a
		// topic that was never published to and the select silently
		// stayed in "unknown". Use the canonical builder so the
		// publisher and discovery payload can't drift again.
		state_topic: Some(crate::topics::status_ptz_preset(
			ctx.topic_prefix,
			ctx.camera_name,
		)),
	};
	Some((config_topic(ctx, "select", &unique_id), to_json(&payload)))
}

/// `button` entity that reboots the camera.
pub fn build_reboot(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "reboot");
	let payload = DiscoveryButton {
		name: format!("{friendly} Reboot"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:restart".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "reboot"),
		payload_press: None,
	};
	Some((config_topic(ctx, "button", &unique_id), to_json(&payload)))
}

/// Four `button` entities (left / right / up / down) driving the
/// `control/ptz` topic with a directional payload. Returns an empty
/// `Vec` when the camera doesn't report PTZ hardware.
pub fn build_pt_buttons(ctx: &DiscoveryContext) -> Vec<(String, Vec<u8>)> {
	if !ctx.capabilities.has_ptz {
		return Vec::new();
	}
	let friendly = title_case(ctx.camera_name);
	let mut out = Vec::with_capacity(4);
	for dir in ["left", "right", "up", "down"] {
		let unique_id = unique(ctx.topic_prefix, ctx.camera_name, &format!("pan_{dir}"));
		let payload = DiscoveryButton {
			name: format!("{friendly} Pan {dir}"),
			unique_id: unique_id.clone(),
			icon: Some(format!("mdi:pan-{dir}")),
			device: device_block(ctx, &friendly),
			availability: availability_block(ctx),
			command_topic: cam_topic(ctx, "control", "ptz"),
			payload_press: Some(dir.to_string()),
		};
		out.push((config_topic(ctx, "button", &unique_id), to_json(&payload)));
	}
	out
}

/// `sensor` entity reporting battery percentage. HA long-term
/// statistics require all three: `state_class = measurement`,
/// `unit_of_measurement = %`, `device_class = battery`.
pub fn build_battery(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "battery");
	let payload = DiscoverySensor {
		name: format!("{friendly} Battery"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:battery".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		state_topic: cam_topic(ctx, "status", "battery_level"),
		state_class: "measurement".to_string(),
		unit_of_measurement: "%".to_string(),
		device_class: Some("battery".to_string()),
	};
	Some((config_topic(ctx, "sensor", &unique_id), to_json(&payload)))
}

/// `button` entity triggering the camera's alarm siren.
pub fn build_siren(ctx: &DiscoveryContext) -> Option<(String, Vec<u8>)> {
	let friendly = title_case(ctx.camera_name);
	let unique_id = unique(ctx.topic_prefix, ctx.camera_name, "siren");
	let payload = DiscoveryButton {
		name: format!("{friendly} Siren"),
		unique_id: unique_id.clone(),
		icon: Some("mdi:bell".to_string()),
		device: device_block(ctx, &friendly),
		availability: availability_block(ctx),
		command_topic: cam_topic(ctx, "control", "siren"),
		payload_press: Some("on".to_string()),
	};
	Some((config_topic(ctx, "button", &unique_id), to_json(&payload)))
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
	use super::*;

	struct Fixture {
		prefix: String,
		ha: String,
		cam: String,
		addr: String,
		sw: String,
	}

	impl Fixture {
		fn new(prefix: &str) -> Self {
			Self {
				prefix: prefix.to_string(),
				ha: "homeassistant".to_string(),
				cam: "frontdoor".to_string(),
				addr: "192.168.1.10:9000".to_string(),
				sw: "1.2.3".to_string(),
			}
		}

		fn ctx<'a>(&'a self, caps: &'a CameraCapabilitiesView) -> DiscoveryContext<'a> {
			DiscoveryContext {
				topic_prefix: &self.prefix,
				ha_topic: &self.ha,
				camera_name: &self.cam,
				camera_addr: Some(&self.addr),
				camera_uid: None,
				sw_version: &self.sw,
				capabilities: caps,
				presets: &[],
			}
		}
	}

	fn parse(json: &[u8]) -> serde_json::Value {
		serde_json::from_slice(json).expect("payload is valid JSON")
	}

	#[test]
	fn feature_all_is_exhaustive() {
		// Adding a `Feature` variant without extending `Feature::ALL`
		// breaks discovery emission silently. The match below is
		// exhaustive, so a new variant makes this a compile error
		// rather than a runtime-undercount; the length assertion
		// catches someone deleting an entry from `ALL` without
		// removing the variant.
		fn touch(f: Feature) {
			match f {
				Feature::Floodlight
				| Feature::Camera
				| Feature::Motion
				| Feature::Led
				| Feature::Ir
				| Feature::Reboot
				| Feature::Pt
				| Feature::PtPreset
				| Feature::Battery
				| Feature::Siren
				| Feature::Pir
				| Feature::Ai
				| Feature::Doorbell => {}
			}
		}
		for f in Feature::ALL {
			touch(f);
		}
		assert_eq!(Feature::ALL.len(), 13);
	}

	#[test]
	fn floodlight_light_payload_topic_and_unique_id() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView::default();
		let ctx = f.ctx(&caps);
		let (topic, json) = build_floodlight_light(&ctx).expect("emitted");
		assert_eq!(
			topic,
			"homeassistant/light/bairelay_frontdoor_floodlight/config"
		);
		let v = parse(&json);
		assert_eq!(v["unique_id"], "bairelay_frontdoor_floodlight");
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/floodlight");
		assert_eq!(v["state_value_template"], "{{ value_json.state }}");
		assert_eq!(v["command_topic"], "bairelay/frontdoor/control/floodlight");
		assert_eq!(v["payload_on"], "on");
		assert_eq!(v["payload_off"], "off");
		assert_eq!(v["device"]["manufacturer"], "Reolink");
		assert_eq!(v["device"]["model"], "Bairelay");
		assert_eq!(v["device"]["identifiers"][0], "bairelay_frontdoor");
		assert_eq!(v["device"]["name"], "Frontdoor");
		assert_eq!(v["availability"]["topic"], "bairelay/frontdoor/status");
		assert_eq!(v["availability"]["payload_available"], "connected");
	}

	#[test]
	fn pt_emits_four_buttons_when_ptz_supported() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: Presence::Present,
			ir_leds: Presence::Present,
			..Default::default()
		};
		let ctx = f.ctx(&caps);
		let emitted = build_pt_buttons(&ctx);
		assert_eq!(emitted.len(), 4);
		let topics: Vec<_> = emitted.iter().map(|(t, _)| t.as_str()).collect();
		for dir in ["left", "right", "up", "down"] {
			let expected = format!("homeassistant/button/bairelay_frontdoor_pan_{dir}/config");
			assert!(
				topics.contains(&expected.as_str()),
				"missing topic for direction {dir}: {topics:?}"
			);
		}
		// Spot-check one payload's shape.
		let (_, json) = &emitted[0];
		let v = parse(json);
		assert_eq!(v["command_topic"], "bairelay/frontdoor/control/ptz");
		assert!(v["payload_press"].is_string());
		assert_eq!(v["device"]["model"], "Bairelay");
	}

	#[test]
	fn pt_suppressed_when_ptz_absent() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView {
			has_ptz: false,
			status_led: Presence::Present,
			ir_leds: Presence::Present,
			..Default::default()
		};
		let ctx = f.ctx(&caps);
		assert!(build_pt_buttons(&ctx).is_empty());
	}

	#[test]
	fn neolink_prefix_produces_legacy_paths() {
		let f = Fixture::new("neolink");
		let caps = CameraCapabilitiesView::default();
		let ctx = f.ctx(&caps);
		let (topic, json) = build_camera(&ctx).expect("emitted");
		assert_eq!(
			topic,
			"homeassistant/camera/neolink_frontdoor_camera/config"
		);
		let v = parse(&json);
		assert_eq!(v["unique_id"], "neolink_frontdoor_camera");
		assert_eq!(v["topic"], "neolink/frontdoor/status/preview");
		assert_eq!(v["device"]["identifiers"][0], "neolink_frontdoor");
	}

	#[test]
	fn battery_sensor_has_ha_long_term_stats_fields() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView::default();
		let ctx = f.ctx(&caps);
		let (_, json) = build_battery(&ctx).expect("emitted");
		let v = parse(&json);
		assert_eq!(v["state_class"], "measurement");
		assert_eq!(v["unit_of_measurement"], "%");
		assert_eq!(v["device_class"], "battery");
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/battery_level");
	}

	#[test]
	fn camera_payload_uses_base64_image_encoding() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView::default();
		let ctx = f.ctx(&caps);
		let (_, json) = build_camera(&ctx).expect("emitted");
		let v = parse(&json);
		assert_eq!(v["image_encoding"], "b64");
	}

	#[test]
	fn ptz_presets_state_topic_matches_canonical_status_ptz_preset_builder() {
		// Pre-fix the discovery payload's state_topic was
		// `bairelay/frontdoor/status/ptz_preset` (no slash) while
		// production publishes on
		// `bairelay/frontdoor/status/ptz/preset` (slashed) via
		// `topics::status_ptz_preset`. HA subscribed to a topic that
		// was never published to and the preset select silently never
		// updated. Pin cross-module agreement.
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView {
			has_ptz: true,
			status_led: Presence::Present,
			ir_leds: Presence::Present,
			..Default::default()
		};
		let presets: Vec<(u8, String)> = vec![(1, "Home".to_string()), (2, "Sky".to_string())];
		let ctx = DiscoveryContext {
			topic_prefix: &f.prefix,
			ha_topic: &f.ha,
			camera_name: &f.cam,
			camera_addr: Some(&f.addr),
			camera_uid: None,
			sw_version: &f.sw,
			capabilities: &caps,
			presets: &presets,
		};
		let (_topic, json) = build_ptz_presets(&ctx).expect("emitted");
		let v = parse(&json);
		assert_eq!(
			v["state_topic"],
			crate::topics::status_ptz_preset(&f.prefix, &f.cam),
			"state_topic must match the canonical status_ptz_preset builder",
		);
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/ptz/preset");
		assert_eq!(v["command_topic"], "bairelay/frontdoor/control/ptz/preset");
	}

	#[test]
	fn motion_uses_md_not_motion_in_unique_id() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView::default();
		let ctx = f.ctx(&caps);
		let (topic, json) = build_motion(&ctx).expect("emitted");
		assert_eq!(
			topic,
			"homeassistant/binary_sensor/bairelay_frontdoor_md/config"
		);
		let v = parse(&json);
		assert_eq!(v["unique_id"], "bairelay_frontdoor_md");
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/motion");
	}

	fn caps(status_led: Presence, ir_leds: Presence) -> CameraCapabilitiesView {
		CameraCapabilitiesView {
			status_led,
			ir_leds,
			..Default::default()
		}
	}

	#[test]
	fn led_and_ir_gated_independently() {
		use Presence::*;
		let f = Fixture::new("bairelay");
		let led_only = caps(Present, Absent);
		assert!(build_led(&f.ctx(&led_only)).is_some());
		assert!(build_ir(&f.ctx(&led_only)).is_none());
		let ir_only = caps(Absent, Present);
		assert!(build_led(&f.ctx(&ir_only)).is_none());
		assert!(build_ir(&f.ctx(&ir_only)).is_some());
	}

	/// Unknown presence keeps the entity (as before capability
	/// detection) but without a state topic — the camera's value may be
	/// a placeholder.
	#[test]
	fn unknown_presence_emits_entity_without_state() {
		let f = Fixture::new("bairelay");
		let unknown = CameraCapabilitiesView::default();
		let (_, led) = build_led(&f.ctx(&unknown)).expect("emitted");
		let (_, ir) = build_ir(&f.ctx(&unknown)).expect("emitted");
		assert!(parse(&led).get("state_topic").is_none());
		assert!(parse(&ir).get("state_topic").is_none());
	}

	#[test]
	fn absent_hardware_retractions_cover_config_and_status() {
		use Presence::*;
		let f = Fixture::new("bairelay");
		assert_eq!(
			absent_hardware_retractions(&f.ctx(&caps(Present, Absent))),
			vec![
				"homeassistant/select/bairelay_frontdoor_ir/config".to_string(),
				"bairelay/frontdoor/status/ir".to_string(),
			]
		);
		assert!(absent_hardware_retractions(&f.ctx(&caps(Present, Present))).is_empty());
		assert_eq!(
			absent_hardware_retractions(&f.ctx(&caps(Absent, Absent))).len(),
			4
		);
		// Unknown never retracts: a failed probe must not delete entities.
		assert!(absent_hardware_retractions(&f.ctx(&caps(Unknown, Unknown))).is_empty());
	}

	fn ai_caps(ai: Presence, doorbell: Presence) -> CameraCapabilitiesView {
		CameraCapabilitiesView {
			ai,
			doorbell,
			..Default::default()
		}
	}

	#[test]
	fn retracts_ai_object_table() {
		use Presence::*;
		let retracted = |ai, doorbell| -> Vec<&str> {
			AiObject::ALL
				.into_iter()
				.filter(|&o| ai_caps(ai, doorbell).retracts_ai_object(o))
				.map(AiObject::topic_name)
				.collect()
		};
		let all = ["person", "vehicle", "animal", "face", "package"];
		assert_eq!(retracted(Present, Absent), ["face", "package"]);
		assert!(retracted(Present, Present).is_empty());
		assert!(retracted(Present, Unknown).is_empty());
		assert!(retracted(Unknown, Unknown).is_empty());
		assert_eq!(retracted(Unknown, Absent), ["face", "package"]);
		assert_eq!(retracted(Absent, Present), all);
		assert_eq!(retracted(Absent, Unknown), all);
	}

	#[test]
	fn emits_ai_object_table() {
		use Presence::*;
		let emitted = |ai, doorbell| -> Vec<&str> {
			AiObject::ALL
				.into_iter()
				.filter(|&o| ai_caps(ai, doorbell).emits_ai_object(o))
				.map(AiObject::topic_name)
				.collect()
		};
		let base = ["person", "vehicle", "animal"];
		let all = ["person", "vehicle", "animal", "face", "package"];
		assert_eq!(emitted(Present, Present), all);
		assert_eq!(emitted(Unknown, Present), all);
		assert_eq!(emitted(Present, Unknown), base);
		assert_eq!(emitted(Present, Absent), base);
		assert_eq!(emitted(Unknown, Unknown), base);
		assert!(emitted(Absent, Present).is_empty());
		assert!(emitted(Absent, Unknown).is_empty());
	}

	#[test]
	fn unknown_as_present_lifts_only_unknown() {
		use Presence::*;
		let c = CameraCapabilitiesView {
			has_ptz: true,
			status_led: Unknown,
			ir_leds: Absent,
			ai: Unknown,
			doorbell: Present,
		}
		.unknown_as_present();
		assert!(c.has_ptz);
		assert_eq!(
			(c.status_led, c.ir_leds, c.ai, c.doorbell),
			(Present, Absent, Present, Present)
		);
	}

	fn ai_unique_ids(caps: &CameraCapabilitiesView) -> Vec<String> {
		let f = Fixture::new("bairelay");
		build_ai_binary_sensors(&f.ctx(caps))
			.iter()
			.map(|(_, p)| parse(p)["unique_id"].as_str().unwrap().to_string())
			.collect()
	}

	#[test]
	fn feature_parses_ai_and_doorbell() {
		let ai: Feature = serde_json::from_str("\"ai\"").unwrap();
		let db: Feature = serde_json::from_str("\"doorbell\"").unwrap();
		assert_eq!((ai, db), (Feature::Ai, Feature::Doorbell));
		assert!(Feature::ALL.contains(&Feature::Ai));
		assert!(Feature::ALL.contains(&Feature::Doorbell));
	}

	#[test]
	fn ai_sensor_payload_shape() {
		let f = Fixture::new("bairelay");
		let caps = ai_caps(Presence::Present, Presence::Present);
		let sensors = build_ai_binary_sensors(&f.ctx(&caps));
		let (topic, payload) = &sensors[0];
		assert_eq!(
			topic,
			"homeassistant/binary_sensor/bairelay_frontdoor_ai_person/config"
		);
		let v = parse(payload);
		assert_eq!(v["name"], "Frontdoor Person");
		assert_eq!(v["unique_id"], "bairelay_frontdoor_ai_person");
		assert_eq!(v["device_class"], "motion");
		assert_eq!(v["icon"], "mdi:account");
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/ai/person");
		assert_eq!(v["payload_on"], "on");
		assert_eq!(v["payload_off"], "off");
		let (_, motion) = build_motion(&f.ctx(&caps)).unwrap();
		let m = parse(&motion);
		assert_eq!(v["availability"], m["availability"]);
		assert_eq!(v["device"], m["device"]);
	}

	#[test]
	fn ai_sensor_classes_and_icons() {
		let f = Fixture::new("bairelay");
		let caps = ai_caps(Presence::Present, Presence::Present);
		let got: Vec<_> = build_ai_binary_sensors(&f.ctx(&caps))
			.iter()
			.map(|(_, p)| {
				let v = parse(p);
				(
					v["name"].as_str().unwrap().to_string(),
					v["device_class"].as_str().unwrap().to_string(),
					v["icon"].as_str().unwrap().to_string(),
				)
			})
			.collect();
		let expect = [
			("Frontdoor Person", "motion", "mdi:account"),
			("Frontdoor Vehicle", "motion", "mdi:car"),
			("Frontdoor Animal", "motion", "mdi:paw"),
			("Frontdoor Face", "motion", "mdi:face-recognition"),
			(
				"Frontdoor Package",
				"presence",
				"mdi:package-variant-closed",
			),
		];
		assert_eq!(got.len(), 5);
		for (g, e) in got.iter().zip(expect) {
			assert_eq!((g.0.as_str(), g.1.as_str(), g.2.as_str()), e);
		}
	}

	#[test]
	fn ai_sensor_gating_by_presence() {
		use Presence::*;
		let base = |o: &str| format!("bairelay_frontdoor_ai_{o}");
		let three = vec![base("person"), base("vehicle"), base("animal")];
		let mut five = three.clone();
		five.push(base("face"));
		five.push(base("package"));
		assert_eq!(ai_unique_ids(&ai_caps(Present, Present)), five);
		assert_eq!(ai_unique_ids(&ai_caps(Present, Unknown)), three);
		assert_eq!(ai_unique_ids(&ai_caps(Present, Absent)), three);
		assert_eq!(ai_unique_ids(&ai_caps(Unknown, Unknown)), three);
		assert_eq!(ai_unique_ids(&ai_caps(Unknown, Present)), five);
		assert!(ai_unique_ids(&ai_caps(Absent, Present)).is_empty());
		assert!(ai_unique_ids(&ai_caps(Absent, Unknown)).is_empty());
	}

	#[test]
	fn doorbell_event_payload_and_gating() {
		use Presence::*;
		let f = Fixture::new("bairelay");
		let caps = ai_caps(Unknown, Present);
		let (topic, payload) = build_doorbell(&f.ctx(&caps)).expect("emitted");
		assert_eq!(
			topic,
			"homeassistant/event/bairelay_frontdoor_doorbell/config"
		);
		let v = parse(&payload);
		assert_eq!(v["name"], "Frontdoor Doorbell");
		assert_eq!(v["unique_id"], "bairelay_frontdoor_doorbell");
		assert_eq!(v["device_class"], "doorbell");
		assert_eq!(v["event_types"], serde_json::json!(["press"]));
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/doorbell");
		assert_eq!(v["value_template"], r#"{"event_type":"{{ value }}"}"#);
		assert_eq!(v["icon"], "mdi:doorbell");
		assert!(v.get("availability").is_some());
		assert!(v.get("device").is_some());
		assert!(build_doorbell(&f.ctx(&ai_caps(Unknown, Unknown))).is_some());
		assert!(build_doorbell(&f.ctx(&ai_caps(Present, Absent))).is_none());
	}

	#[test]
	fn ai_and_doorbell_retractions() {
		use Presence::*;
		let f = Fixture::new("bairelay");
		let cfg = |o: &str| format!("homeassistant/binary_sensor/bairelay_frontdoor_ai_{o}/config");
		let st = |o: &str| format!("bairelay/frontdoor/status/ai/{o}");
		let door_cfg = "homeassistant/event/bairelay_frontdoor_doorbell/config".to_string();
		let r = |a, d| absent_hardware_retractions(&f.ctx(&ai_caps(a, d)));
		assert!(r(Present, Present).is_empty());
		assert!(r(Unknown, Unknown).is_empty());
		assert!(r(Present, Unknown).is_empty());
		// Doorbell absent: doorbell entity + face/package only.
		assert_eq!(
			r(Present, Absent),
			vec![
				door_cfg.clone(),
				cfg("face"),
				st("face"),
				cfg("package"),
				st("package"),
			]
		);
		// AI absent: all five objects, doorbell untouched.
		let all_ai: Vec<String> = ["person", "vehicle", "animal", "face", "package"]
			.iter()
			.flat_map(|o| [cfg(o), st(o)])
			.collect();
		assert_eq!(r(Absent, Present), all_ai);
		// Both absent: no duplicates.
		let both = r(Absent, Absent);
		assert_eq!(both.len(), all_ai.len() + 1);
		assert!(both.contains(&door_cfg));
		let mut dedup = both.clone();
		dedup.sort();
		dedup.dedup();
		assert_eq!(dedup.len(), both.len());
	}

	#[test]
	fn led_and_ir_carry_state_topics() {
		let f = Fixture::new("bairelay");
		let caps = CameraCapabilitiesView {
			status_led: Presence::Present,
			ir_leds: Presence::Present,
			..Default::default()
		};
		let ctx = f.ctx(&caps);
		let (_, led) = build_led(&ctx).expect("emitted");
		let v = parse(&led);
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/led");
		assert_eq!(v["state_on"], "on");
		assert_eq!(v["state_off"], "off");
		let (_, ir) = build_ir(&ctx).expect("emitted");
		let v = parse(&ir);
		assert_eq!(v["state_topic"], "bairelay/frontdoor/status/ir");
	}

	#[test]
	fn title_case_handles_underscores_and_single_words() {
		assert_eq!(title_case("frontdoor"), "Frontdoor");
		assert_eq!(title_case("front_door"), "Front Door");
		assert_eq!(title_case("4k_terrace"), "4k Terrace");
	}

	#[test]
	fn title_case_treats_hyphen_as_word_separator() {
		assert_eq!(title_case("front-door"), "Front Door");
		assert_eq!(title_case("cam_back-yard"), "Cam Back Yard");
	}

	#[test]
	fn title_case_preserves_embedded_caps_after_first_char() {
		// Operators naming a camera in CamelCase / acronym style keep
		// their casing — only a lowercase first char of each segment
		// is upgraded to uppercase.
		assert_eq!(title_case("MyCamera"), "MyCamera");
		assert_eq!(title_case("4K_Terrace"), "4K Terrace");
		assert_eq!(title_case("IPCam"), "IPCam");
		assert_eq!(title_case("IPCam-front"), "IPCam Front");
	}

	#[test]
	fn connection_serialises_as_two_tuple() {
		let c = DiscoveryConnection {
			connection_type: "camera_addr".to_string(),
			connection_id: "10.0.0.1:9000".to_string(),
		};
		let json = serde_json::to_value(&c).unwrap();
		assert!(json.is_array());
		assert_eq!(json[0], "camera_addr");
		assert_eq!(json[1], "10.0.0.1:9000");
	}
}
