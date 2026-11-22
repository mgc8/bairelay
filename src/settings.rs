//! Applying MQTT settings and publishing what the camera re-read.
//!
//! The `control/<family>/<leaf>` message arrives already validated by
//! [`bairelay_mqtt::settings`] (range, length, allowed text): no out-of-range
//! value ever reaches the camera from here. This module does three things, in this
//! order:
//!
//! 1. it **re-reads** the block concerned — a Baichuan write is a
//!    read-modify-write of the whole block (see `docs/protocol-notes.md`), and
//!    adding to the block a tag the firmware did not send is the fault this
//!    bridge refuses everywhere (same rule as
//!    `src/oneshot/audio_settings.rs::present`) ;
//! 2. it writes the modified block ;
//! 3. it publishes, leaf by leaf, **what the camera re-read**.
//!
//! Point 3 is the law of this module. A retained state on the broker that repeated
//! the *requested* value would survive a bridge restart, hide a firmware that
//! ignores the write, and make Home Assistant lie.
//!
//! When the camera **refuses** a family, that family's retained states are cleared
//! (empty retained payload: HA shows “unknown”) and not replaced
//! by a default value. “Refused” and “empty” must stay distinct —
//! that is what, on a doorbell with no chime paired, tells a legitimate empty list
//! (`[]`) apart from a camera that did not answer (no state at all).
//!
//! Settings do not go through the [`bairelay_neolink_core::bc_protocol::CameraDriver`] trait: they are read/write pairs
//! specific to the Baichuan protocol, and the trait is not meant to
//! carry thirty methods. The dispatch therefore goes through the concrete [`BcCamera`]
//! this module knows how to interrogate, block by block.

use bairelay_mqtt::discovery::{EntityKind, SPECS};
use bairelay_mqtt::settings::{Family, SettingCommand};
use bairelay_mqtt::StatusPublisher;

use bairelay_neolink_core::bc::xml::{
	AiDenoise, AudioCfg, AutoReboot, BatteryInfo, DingDongCfg, Dst, Ntp, RfAlarmCfg, Shelter,
	SleepState, SystemGeneral, WifiSignal,
};
use bairelay_neolink_core::bc_protocol::{BcCamera, Error, ImageCfg, MotionCfg, OsdCfg};

/// The result of one camera command. The core's alias is `pub(crate)` over there,
/// so the `(T, Error)` pair is written once, here.
type CoreResult<T> = core::result::Result<T, Error>;

/// Result of a setting write. `Err` means “the camera refused” (or
/// returned an unintelligible block): the dispatch answers `FAIL` on the command
/// topic, and **nothing at all will have been published**.
pub(crate) type Result = core::result::Result<(), Error>;

// ── Publishing the leaves ───────────────────────────────────────────────
//
// One leaf = one state topic = one discovery entity. Publication errors are
// logged but do not propagate: what decides OK/FAIL
// is the camera, not the broker.

/// Publishes a leaf's textual value, or clears it when the camera does not print
/// that tag. A leaf with no state at HA means “unknown”; leaving
/// last time's value there would mean “unchanged”, which is not
/// what we know.
async fn put(publ: &StatusPublisher<'_>, family: Family, leaf: &str, value: Option<&str>) {
	let outcome = match value {
		Some(v) => publ.publish_setting(family, leaf, v).await,
		None => publ.clear_setting(family, leaf).await,
	};
	if let Err(e) = outcome {
		tracing::warn!(
			family = family.slug(),
			leaf,
			error = %e,
			"publishing a setting's state failed"
		);
	}
}

/// A switch: `on`/`off`, the two texts the `switch` entity discovers.
async fn put_switch(publ: &StatusPublisher<'_>, family: Family, leaf: &str, value: Option<&str>) {
	match on_off_text(value) {
		Some(on) => {
			let text = if on { "on" } else { "off" };
			put(publ, family, leaf, Some(text)).await;
		}
		// `None` covers two cases: tag absent, and tag present with a
		// text no documented firmware prints. In both cases we
		// do not guess.
		None => put(publ, family, leaf, None).await,
	}
}

/// A `binary_sensor`: `1`/`0`.
async fn put_flag(publ: &StatusPublisher<'_>, family: Family, leaf: &str, value: Option<&str>) {
	match on_off_text(value) {
		Some(on) => {
			let text = if on { "1" } else { "0" };
			put(publ, family, leaf, Some(text)).await;
		}
		None => put(publ, family, leaf, None).await,
	}
}

/// The `on`/`off` text of a camera value, or `None` when the text is not
/// one of the two known states. A `2` read on a switch is not “on”.
fn on_off_text(value: Option<&str>) -> Option<bool> {
	match value.map(str::trim)? {
		"1" | "on" | "true" => Some(true),
		"0" | "off" | "false" => Some(false),
		_ => None,
	}
}

/// The family's whole block as JSON (topic `status/<family>/json`), for
/// automations and diagnostics. The HA entities read the leaves.
async fn publish_json<T: serde::Serialize>(publ: &StatusPublisher<'_>, family: Family, block: &T) {
	match serde_json::to_string(block) {
		Ok(json) => {
			if let Err(e) = publ.publish_settings_json(family, &json).await {
				tracing::warn!(family = family.slug(), error = %e, "publishing a setting's JSON failed");
			}
		}
		Err(e) => tracing::warn!(family = family.slug(), error = %e, "setting block not serialisable to JSON"),
	}
}

/// Count of leaves published: three for audio, two for denoise…
/// For logs and tests only, which want to know whether a family really
/// did speak.
type Published = core::result::Result<usize, Error>;

async fn publish_audio(publ: &StatusPublisher<'_>, cfg: &AudioCfg) {
	// The block's three volumes, one per XML tag: `<volume>` (general),
	// `<talkAndReplyVolume>` (intercom), `<visitorVolume>` (doorbell).
	// `AudioCfg::volume` is a `String` and not an `Option`: an empty string
	// means “the camera did not print this tag”, not “the volume is
	// zero”. Publishing `""` there would invent a state; we clear.
	let general = Some(cfg.volume.as_str()).filter(|v| !v.trim().is_empty());
	put(publ, Family::Audio, "general_volume", general).await;
	put(publ, Family::Audio, "volume", cfg.talk_and_reply_volume.as_deref()).await;
	put(publ, Family::Audio, "visitor_volume", cfg.visitor_volume.as_deref()).await;
	put_switch(publ, Family::Audio, "visitor_loudspeaker", cfg.visitor_loudspeaker.as_deref()).await;
	publish_json(publ, Family::Audio, cfg).await;
}

async fn publish_denoise(publ: &StatusPublisher<'_>, cfg: &AiDenoise) {
	put_switch(publ, Family::Denoise, "enable", cfg.enable.as_deref()).await;
	put(publ, Family::Denoise, "level", cfg.level.as_deref()).await;
	publish_json(publ, Family::Denoise, cfg).await;
}

async fn publish_image(publ: &StatusPublisher<'_>, cfg: &ImageCfg) {
	let v = &cfg.video_input;
	put(publ, Family::Image, "bright", v.bright.as_deref()).await;
	put(publ, Family::Image, "contrast", v.contrast.as_deref()).await;
	put(publ, Family::Image, "saturation", v.saturation.as_deref()).await;
	put(publ, Family::Image, "hue", v.hue.as_deref()).await;
	put(publ, Family::Image, "sharpen", v.sharpen.as_deref()).await;
	put(publ, Family::Image, "day_night", v.day_night.as_deref()).await;
	publish_json(publ, Family::Image, cfg).await;
}

async fn publish_osd(publ: &StatusPublisher<'_>, cfg: &OsdCfg) {
	put_switch(publ, Family::Osd, "datetime_enable", cfg.datetime.enable.as_deref()).await;
	// The name and the watermark live in the second block (44 returns both).
	let name = cfg.channel_name.as_ref();
	put_switch(publ, Family::Osd, "name_enable", name.and_then(|c| c.enable.as_deref())).await;
	put(publ, Family::Osd, "name", name.and_then(|c| c.name.as_deref())).await;
	put_switch(publ, Family::Osd, "watermark", name.and_then(|c| c.en_watermark.as_deref())).await;
	publish_json(publ, Family::Osd, cfg).await;
}

async fn publish_motion(publ: &StatusPublisher<'_>, cfg: &MotionCfg) {
	put_switch(publ, Family::Motion, "enable", cfg.enable()).await;
	put(publ, Family::Motion, "sensitivity", cfg.sensitivity()).await;
	// No JSON here: the `<MdAlarm>` shape (the only answer some
	// cameras give to msg 46) has fields the protocol does not enumerate, and
	// publishing them adds nothing the two leaves above do not already say.
}

/// The PIR has two state topics: the historic switch
/// (`status/pir`, published by the dispatch) and its sensitivity. `pub(crate)`
/// because writing the switch (`control/pir`) goes through the historic
/// dispatch and wants the same spelling rule.
pub(crate) async fn publish_pir(publ: &StatusPublisher<'_>, cfg: &RfAlarmCfg) {
	// `sensiValue` (firmware v1.1+) and `sensitivity` (older) are two
	// spellings of the same dial; we publish the one that exists.
	let sens = cfg
		.sensiValue
		.map(|v| v.to_string())
		.or_else(|| cfg.sensitivity.map(|v| v.to_string()));
	put(publ, Family::Pir, "sensitivity", sens.as_deref()).await;
	// The rest time between two alarms, in seconds, when the firmware
	// prints it. `intervalUseRange` is not published as a separate entity: it is
	// in the JSON below, and that is where to go read it if the value
	// written does not survive the re-read.
	put(
		publ,
		Family::Pir,
		"interval",
		cfg.interval.map(|v| v.to_string()).as_deref(),
	)
	.await;
	publish_json(publ, Family::Pir, cfg).await;
}

async fn publish_ntp(publ: &StatusPublisher<'_>, cfg: &Ntp) {
	put_switch(publ, Family::Ntp, "enable", cfg.enable.as_deref()).await;
	put(publ, Family::Ntp, "server", cfg.server.as_deref()).await;
	put(publ, Family::Ntp, "interval", cfg.synchronize_interval.as_deref()).await;
	publish_json(publ, Family::Ntp, cfg).await;
}

async fn publish_dst(publ: &StatusPublisher<'_>, cfg: &Dst) {
	let enable = cfg.enable.map(|v| v.to_string());
	let offset = cfg.offset.map(|v| v.to_string());
	put_switch(publ, Family::Dst, "enable", enable.as_deref()).await;
	put(publ, Family::Dst, "offset", offset.as_deref()).await;
	publish_json(publ, Family::Dst, cfg).await;
}

async fn publish_auto_reboot(publ: &StatusPublisher<'_>, cfg: &AutoReboot) {
	put_switch(publ, Family::AutoReboot, "enable", cfg.enable.as_deref()).await;
	put(publ, Family::AutoReboot, "weekday", cfg.week_day.as_deref()).await;
	put(publ, Family::AutoReboot, "hour", cfg.hour.as_deref()).await;
	put(publ, Family::AutoReboot, "minute", cfg.minute.as_deref()).await;
	publish_json(publ, Family::AutoReboot, cfg).await;
}

async fn publish_general(publ: &StatusPublisher<'_>, cfg: &SystemGeneral) {
	put(publ, Family::General, "osd_format", cfg.osd_format.as_deref()).await;
	let time_format = cfg.time_format.map(|v| v.to_string());
	put(publ, Family::General, "time_format", time_format.as_deref()).await;
	put(publ, Family::General, "language", cfg.language.as_deref()).await;
	put(publ, Family::General, "device_name", cfg.device_name.as_deref()).await;
	// `isDst` is read-only at the protocol level: the camera computes it. So a
	// sensor, not a switch.
	let is_dst = cfg.is_dst.map(|v| v.to_string());
	put_flag(publ, Family::General, "is_dst", is_dst.as_deref()).await;
	publish_json(publ, Family::General, cfg).await;
}

async fn publish_masks(publ: &StatusPublisher<'_>, cfg: &Shelter) {
	put_flag(publ, Family::Mask, "enable", cfg.enable.as_deref()).await;
	put(publ, Family::Mask, "max_num", cfg.max_num.as_deref()).await;
	publish_json(publ, Family::Mask, cfg).await;
}

async fn publish_sleep(publ: &StatusPublisher<'_>, cfg: &SleepState) {
	put_flag(publ, Family::Sleep, "state", cfg.sleep.as_deref()).await;
}

async fn publish_wifi(publ: &StatusPublisher<'_>, cfg: &WifiSignal) {
	put(publ, Family::Wifi, "signal", cfg.signal.as_deref()).await;
}

/// The bonus states of frame 253. No JSON for this family:
/// `<BatteryInfo>` carries a `<username>` and the camera's `system`/`network`/`alarm`
/// tokens, which have no business in a state retained on a broker.
async fn publish_battery(publ: &StatusPublisher<'_>, cfg: &BatteryInfo) {
	put(publ, Family::Battery, "voltage", Some(&cfg.voltage.to_string())).await;
	put(publ, Family::Battery, "current", Some(&cfg.current.to_string())).await;
	put(publ, Family::Battery, "temperature", Some(&cfg.temperature.to_string())).await;
	put(publ, Family::Battery, "charge_status", Some(cfg.charge_status.as_str())).await;
	put(publ, Family::Battery, "low_power", Some(&cfg.low_power.to_string())).await;
}

/// The list of paired chimes, as JSON.
///
/// `chimes == None` (msg 484 refused) publishes **nothing**: that is “the camera
/// did not answer”, not “there is no chime”. `Some(vec![])` publishes
/// `[]`, which does mean “paired: none”.
async fn publish_chime(publ: &StatusPublisher<'_>, cfg: &DingDongCfg) {
	match cfg.chimes.as_ref() {
		Some(list) => {
			let items: Vec<serde_json::Value> = list
				.iter()
				// A block without an addressable id is not a chime, and the rule the
				// bridge follows is to ignore it: publishing
				// `{"id": null}` would suggest an entry worth acting on.
				.filter(|d| d.identifier().is_some())
				.map(|d| {
					serde_json::json!({
						"id": d.identifier(),
						"name": d.device_name.as_deref().or(d.name.as_deref()),
						// `online` stays strict: `true` only on `<netState>1</netState>`,
						// the documented value. The field camera writes **2** for a paired
						// chime that answers its per-chime reads; redefining 2 as online
						// would be inventing. The raw text is therefore published alongside,
						// and that is the text the operator reads.
						"online": d.is_online(),
						"netstate": d.net_state(),
					})
				})
				.collect();
			let json = serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string());
			put(publ, Family::Chime, "list", Some(&json)).await;
		}
		None => put(publ, Family::Chime, "list", None).await,
	}
	// Volume and melody come from 485 (`dingdongDeviceOpt`), which is only queried
	// when a chime was found. Absent = no state, not a “0”.
	let params = cfg.params.as_ref();
	put(publ, Family::Chime, "volume", params.and_then(|p| p.vol_level.as_deref())).await;
	put(publ, Family::Chime, "music", params.and_then(|p| p.music_id.as_deref())).await;
	// The melodies **the camera** printed: the `musicId`s of its
	// `<alarminCfg>` (486) plus the one from its 485. This is the only melody
	// list this bridge publishes — no local table and no invented id, because
	// the camera's melody listing carries no usable identifier.
	let param_melody =
		params.and_then(|p| p.music_id.as_deref()).map(str::trim).filter(|m| !m.is_empty());
	let evenements = cfg.events.as_deref().unwrap_or(&[]);
	let mut melodies: Vec<&str> = evenements
		.iter()
		.flat_map(|bloc| bloc.alarmin_cfg.iter())
		.filter_map(|alarme| alarme.music_id.as_deref())
		.map(str::trim)
		.filter(|m: &&str| !m.is_empty())
		.chain(param_melody)
		.collect();
	melodies.sort_by_key(|m: &&str| m.parse::<u32>().ok().map_or(u32::MAX, |n| n));
	melodies.dedup();
	let melodies = if melodies.is_empty() { None } else { Some(melodies.join(",")) };
	put(publ, Family::Chime, "melodies", melodies.as_deref()).await;
	// The whole block as JSON (`status/chime/json`): it is what lets someone verify
	// in the field what the camera actually answered on
	// 483/484/485/486/609, without hooking a sniffer up to the camera.
	publish_json(publ, Family::Chime, cfg).await;
}

// ── Verdict only: synchronisation on demand ─────────────────────────────

/// The leaves Home Assistant discovery declares for a family, excluding
/// buttons (a button has no state to clear).
fn leaves_of(family: Family) -> impl Iterator<Item = &'static str> {
	SPECS
		.iter()
		.filter(move |s| s.family == family.slug())
		.filter(|s| !matches!(s.kind, EntityKind::Button { .. }))
		.map(|s| s.leaf)
}

/// Clears every state of a family: what we do when it refused.
///
/// The reason for that refusal is published next to it, not in its place: see
/// [`publish_reason`]. Clearing without explaining gives back exactly the silence
/// the operator cannot diagnose.
pub(crate) async fn clear_family(publ: &StatusPublisher<'_>, family: Family) {
	for leaf in leaves_of(family) {
		put(publ, family, leaf, None).await;
	}
	let _ = publ.clear_settings_json(family).await;
}

/// The reason a family did not answer in time. The timeout is raised by the
/// dispatch, not by the protocol core: this is where we are able to say so.
pub(crate) const REASON_TIMEOUT: &str = "the camera did not answer in time";

/// Puts a family in a state where it can be read but not written, and says so.
pub(crate) const REASON_READ_ONLY: &str =
	"the camera reads this setting but refuses the block back: read-only";

/// The bridge has no open Baichuan session: the command could not leave.
/// This is not the camera refusing, but it is a refusal — and a refusal that
/// stays silent is indistinguishable from a dead bridge.
pub(crate) const REASON_NO_SESSION: &str =
	"no Baichuan session is open: nothing was sent to the camera";

/// What the camera just answered was wrong **once**: the setting is not locked for
/// all that, and the topic has to say so. Without this suffix,
/// a `400` read during a burst would read as a permanent prohibition —
/// exactly the inverse false promise this module fixes.
///
/// Built in two parts because the first half is the technical reason,
/// which varies with the error: see [`publish_reason_unknown`].
pub(crate) const REASON_NO_LOCK: &str =
	" - setting not locked: the family keeps its state";

/// The consequence of a **transient** refusal, published this time **first** in
/// the sentence: [`bairelay_mqtt::status::publishable_reason`] cuts at 160 characters keeping the
/// beginning, so the end of a sentence does not survive a long technical reason
/// (`the camera's answer is unintelligible: …`). Yet that is precisely the half
/// the operator looks at: is the family usable or not.
pub(crate) const REASON_TRANSIENT: &str =
	"transient read error: the family stays published as writable";

/// **Local** and structural refusal: the tag we would have to send back in order
/// to write is not in the camera's answer. The camera did not answer
/// wrongly — we are the ones who cannot write without inventing a tag, the one
/// thing this bridge refuses everywhere. Naming the tag is the point of the
/// sentence: it is what is left to read when the camera is not at hand.
pub(crate) const REASON_NO_OSDCHANNEL: &str = concat!(
	"the camera's answer carries no <OsdChannelName>: ",
	"nothing to send back, read-only"
);

/// A **local** refusal as well: the camera did answer 484 (the list of paired
/// chimes), but no entry of that answer carried a usable
/// identifier — under either documented spelling of the
/// carrier (`<dingdongDeviceInfo>`, `<dingdongList>`). That is exactly
/// the field situation observed: a physical chime paired
/// and an empty list published. The reason says so instead of publishing `[]`
/// in silence, because `[]` reads as “no chime” when what has to be read is “the
/// camera does not enumerate this one”.
pub(crate) const REASON_CHIME_NO_ENTRY: &str = concat!(
	"484 answered with nothing recognised: chime not enumerated, ",
	"so there is nothing to address on 485"
);

/// Same verdict, other cause: 484 did not answer at all (a model without a
/// wireless chime, or a refusal). The nuance matters, so it is kept distinct.
pub(crate) const REASON_CHIME_NO_ID: &str = concat!(
	"the camera did not answer 484: no chime id, ",
	"so there is nothing to address on 485"
);

/// The detection entities publish a state and **no** command
/// topic, and that is not the bridge forgetting: the tag that turns on a detection
/// type has never been read. `342`/`343` (`<AiDetectCfg>`, queried
/// **per type**) document only `<sensitivity>`, `<stayTime>` and target sizes.
/// Publishing a `command_topic` here would promise the writing of an invented
/// tag; the reason therefore names what has to be reported from the field.
pub(crate) const REASON_AI_NO_TAG: &str = concat!(
	"no known enable tag for this type (342/AiDetectCfg documents only ",
	"sensitivity, stayTime, sizes): no command topic"
);

/// Publishes the reason for a refusal on `status/<family>/reason` (retained).
///
/// The sentence is short, in english where the bridge knows how to form it, and
/// scrubbed by [`bairelay_mqtt::status::publishable_reason`]: no raw XML, no
/// identifier, no secret. A publication error does not propagate — the broker is
/// not what decides OK/FAIL — but it is logged.
pub(crate) async fn publish_reason(publ: &StatusPublisher<'_>, family: Family, reason: &str) {
	if let Err(e) = publ.publish_family_reason(family, reason).await {
		tracing::warn!(family = family.slug(), error = %e, "publishing the reason for a refusal failed");
	}
}

/// Publishes the reason for a **non-structural** refusal: the camera answered
/// wrongly, or did not answer, and the bridge changes nothing else but this topic.
///
/// The sentence carries the technical reason **and** the consequence for Home
/// Assistant ([`REASON_NO_LOCK`]): what is written here locks no
/// entity. That is the half the operator looks at first, and without it a
/// `400` during a burst of fifteen commands looks like a forbidden setting.
pub(crate) async fn publish_reason_unknown(
	publ: &StatusPublisher<'_>,
	family: Family,
	reason: &str,
) {
	publish_reason(publ, family, &format!("{reason}{REASON_NO_LOCK}")).await;
}

/// Publishes the reason for a **transient** refusal: the camera answered badly, too
/// tard, or in an unusable block, and the family **stays published as writable**.
///
/// The consequence ([`REASON_TRANSIENT`]) comes first, before the technical reason
/// : it is the half the operator looks at and it has to survive the cut at
/// 160 characters. A `400` torn out during a burst of fifteen radio commands must
/// never again look like a forbidden setting.
pub(crate) async fn publish_reason_transient(
	publ: &StatusPublisher<'_>,
	family: Family,
	reason: &str,
) {
	publish_reason(publ, family, &format!("{REASON_TRANSIENT}, {reason}")).await;
}

/// Makes the reason disappear: the family just answered, so the reason for a
/// past refusal has no business being left on the broker.
pub(crate) async fn clear_reason(publ: &StatusPublisher<'_>, family: Family) {
	if let Err(e) = publ.clear_family_reason(family).await {
		tracing::warn!(family = family.slug(), error = %e, "clearing the reason for a refusal failed");
	}
}

/// Translates a protocol-core error into the short sentence the operator
/// will read on the `reason` topic.
///
/// The structured variants have a sentence of their own — these are the
/// refusals actually met in the field, and they deserve to be understood without
/// opening a protocol note. The other cases keep the original message after an
/// english introduction: it is often the only detail left, and
/// `publishable_reason` guarantees it will not spill into XML or a secret.
pub(crate) fn cause(err: &Error) -> String {
	match err {
		Error::Timeout(_)
		| Error::TimeoutDisconnected
		| Error::TimeoutError(_)
		| Error::DiscoveryTimeout => REASON_TIMEOUT.to_string(),
		Error::CameraServiceUnavailable { code, .. } => {
			format!("the camera refused the command (code {code})")
		}
		// The same refusal when it was a **settings read** that met it:
		// the camera wrote a response carrying its code, and this text is the only
		// thing that says why. It already arrives bounded from the core and
		// `publishable_reason` puts it on one tagless line before it touches
		// the broker — the tag names stay readable, and they carry the information.
		Error::CameraRefused { code, detail, .. } => {
			format!("the camera refused the command (code {code}) - it answered: {detail}")
		}
		Error::CameraLoginFail => "the camera refused the login".to_string(),
		Error::ConnectionUnavailable | Error::CannotInitCamera => {
			"the camera is unreachable".to_string()
		}
		Error::DroppedConnection
		| Error::DroppedConnectionTry(_)
		| Error::ConnectionShutdown
		| Error::TokioBcSendError => "the connection to the camera dropped".to_string(),
		Error::MissingAbility { name, .. } => {
			format!("the camera does not declare the {name} ability for this setting")
		}
		Error::UnintelligibleXml { why, .. } | Error::UnintelligibleReply { why, .. } => {
			format!("the camera's answer is unintelligible: {why}")
		}
		// `Usage` and `Other` already carry a sentence, but prefixed in english
		// by their `Display` (“Usage error: …”). The bare text is taken so the
		// line read at Home Assistant starts with the reason itself. The two
		// arms are split because `Usage` holds a `String` and `Other` a
		// `&'static str`: one shared pattern cannot bind both.
		Error::Usage(why) => format!("setting refused: {why}"),
		Error::Other(why) => format!("setting refused: {why}"),
		other => format!("setting refused: {other}"),
	}
}

/// What discovery has to keep from one read attempt on a family.
///
/// Four verdicts, not two, because four situations do not teach the same
/// thing. Confusing them is the regression found in the field: a family written
/// successfully, then refused with a `400` in the middle of a burst of fifteen
/// commands, fell back to read-only because the bridge read a read failure as
/// proof that writing is impossible. It is not — and the field showed it twice:
/// `image` (26) answers `400` to the **read** and still applies the contrast
/// write, re-read and verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
	/// The camera gave its block **and** we know how to send it back: the
	/// family is writable. A write that succeeds is the same proof,
	/// stronger.
	Writable,
	/// **Local** and structural proof that writing is impossible: the carrier read
	/// does not send back (`<MdAlarm>`, refused by
	/// [`BcCamera::set_motion_cfg`]), or the tag to modify is not in the
	/// answer (`<OsdChannelName>` for the name and the watermark). In both
	/// cases it is **us** refusing, not the camera: these are the only
	/// verdicts, with the one for a family whose block was never read, that lock.
	NotWritable,
	/// The camera answered wrongly, too late, or in an unintelligible block:
	/// `400`, deadline passed, unusable answer. This proves **nothing** against
	/// writing — a sleeping firmware answers `400` to the video blocks and gives
	/// them back twenty seconds later, and `image` is the lived example of a read
	/// refusing while the write goes through. The family is therefore published
	/// **writable**, as before this verdict, and the reason for the refusal is
	/// published next to it: that pair is what the operator asked for, usable but not
	/// silent.
	Transient,
	/// Nothing could be attempted at all: no Baichuan session, login
	/// refused, camera unreachable, ability missing. This is not an answer from
	/// the camera, so the bridge promises nothing it could not verify: the
	/// discovery changes **nothing**; the reason, on its side, is published.
	Unknown,
}

impl Verdict {
	/// The boolean the discovery publisher understands. `None` = "does not change
	/// what is published" ([`Verdict::Unknown`]).
	pub(crate) fn writable(self) -> Option<bool> {
		match self {
			Verdict::Writable => Some(true),
			Verdict::NotWritable => Some(false),
			// A read that did not succeed for a transient reason does not take away
			// a command that used to work: that is the heart of the correction.
			Verdict::Transient => Some(true),
			Verdict::Unknown => None,
		}
	}
}

/// What the branch of [`sync_family`] understood about the carrier it just
/// read.
///
/// These flags travel by parameter rather than by return value: the
/// fifteen branches of [`sync_family`] are separate `async` blocks for
/// stack reasons (see their doc) and each returns its own leaf count.
/// Changing fifteen signatures for a piece of information only three branches
/// know would be the worst place to pay for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Writability {
	/// The camera accepts having the block it just sent returned to it.
	pub writable: bool,
	/// The sentence saying why it does not accept it, when the general case
	/// ([`REASON_READ_ONLY`]) is not enough to name the tag at fault.
	pub cause: Option<&'static str>,
}

impl Writability {
	/// A fresh verdict: "writable, nothing to report".
	pub(crate) fn new() -> Self {
		Writability { writable: true, cause: None }
	}
}

/// Reads a family, publishes its leaves, and updates the `reason` topic: empty
/// when the family is healthy, the refusal sentence otherwise. Answers
/// `(family, verdict)`, that is what discovery has to publish — or not
/// publish at all ([`Verdict::Unknown`]).
///
/// It is **here**, and not in the caller, that the family switches: read
/// succeeded → no reason left; refusal → states cleared **and** reason published. The
/// two halves of the rule ("never silent again" / "refused is not empty")
/// stay in the same block that way.
///
/// The refusal, precisely, has three kinds, and only one locks:
///
/// * **the local proof** — a carrier that does not send back, a missing tag: it
///   is taken inside [`sync_family`] itself, as `Ok`, and gives
///   [`Verdict::NotWritable`];
/// * **the sulky answer** — `400`, deadline passed, unintelligible block: it
///   proves nothing at all and gives [`Verdict::Transient`]. The family is
///   published writable, as before, with the reason for the refusal written next to it;
/// * **the impossible attempt** — no session, camera unreachable: the bridge
///   could verify nothing and returns [`Verdict::Unknown`], which changes nothing in
///   the discovery.
///
/// On a refusal that looks like a "not now", a **second** attempt
/// is made right here: the settings GETs do not have the replay loop the
/// repository already applies elsewhere to the same code, and without it a full burst
/// loses the four blocks the camera ends up giving once it has
/// woken up. One attempt more, no further: `query/settings` is
/// already about fifteen radio commands on a battery doorbell. The passed
/// deadline, on its side, is not replayed — retrying a command the camera
/// did not answer is paying twice for a radio that is asleep.
pub(crate) async fn read_family(
	bc: &BcCamera,
	publ: &StatusPublisher<'_>,
	family: Family,
) -> (Family, Verdict) {
	let mut scope = Writability::new();
	match sync_family(bc, publ, family, &mut scope).await {
		Ok(n) => promise(publ, family, scope, n).await,
		Err(e) if is_retryable(&e) => {
			tracing::debug!(
				family = family.slug(),
				error = %e,
				"settings read refused with a transient code: a second attempt"
			);
			let mut second = Writability::new();
			match sync_family(bc, publ, family, &mut second).await {
				Ok(n) => promise(publ, family, second, n).await,
				Err(deuxieme) => fallback(publ, family, &deuxieme).await,
			}
		}
		Err(e) => fallback(publ, family, &e).await,
	}
}

/// Battery **already read** by the caller: publishes the five fields and returns the
/// same verdict as [`read_family`], without paying frame 253 a second time.
///
/// It exists because the wake burst ([`crate::burst`]) reads the battery
/// itself — it fires for its level — and still has to
/// publish the reasons and the verdict the way `query/settings` does. Two paths
/// publishing the battery with two verdict rules is exactly the
/// divergence this module refuses everywhere else.
pub(crate) async fn battery_read(
	publ: &StatusPublisher<'_>,
	outcome: &CoreResult<BatteryInfo>,
) -> (Family, Verdict) {
	match outcome {
		Ok(_) => promise(publ, Family::Battery, Writability::new(), 5).await,
		Err(e) => fallback(publ, Family::Battery, e).await,
	}
}

/// The family answered its block. Publishes the verdict that follows: reason emptied
/// if we know how to send the block back, otherwise the sentence explaining why the
/// write will never be promised. The states stay in place — they are right.
async fn promise(
	publ: &StatusPublisher<'_>,
	family: Family,
	scope: Writability,
	leaves: usize,
) -> (Family, Verdict) {
	tracing::debug!(
		family = family.slug(),
		leaves = leaves,
		writable = scope.writable,
		"settings synchronised"
	);
	if scope.writable {
		clear_reason(publ, family).await;
		return (family, Verdict::Writable);
	}
	// Not a refusal from the camera: a promise we are not going to make.
	publish_reason(publ, family, scope.cause.unwrap_or(REASON_READ_ONLY)).await;
	(family, Verdict::NotWritable)
}

/// The camera gave nothing usable: this is the fallback, and it is what
/// decides which kind of missing we have in front of us.
///
/// In both cases the retained states are cleared — "we do not know" is not
/// rendered with last time's value — and the reason is published. What
/// changes is the verdict:
///
/// * **passable** (400, deadline, unintelligible block) → [`Verdict::Transient`]:
///   the family is published writable with the reason next to it, because a refusal
///   of a **read** never proved anything against the **write**;
/// * **impossible attempt** (no session, camera unreachable, ability
///   missing) → [`Verdict::Unknown`]: the bridge cannot promise what it did not
///   get to attempt, discovery keeps what it had.
async fn fallback(publ: &StatusPublisher<'_>, family: Family, e: &Error) -> (Family, Verdict) {
	clear_family(publ, family).await;
	if is_transient(e) {
		tracing::debug!(
			family = family.slug(),
			error = %e,
			"settings read transiently refused: states cleared, reason published, family left writable"
		);
		publish_reason_transient(publ, family, &cause(e)).await;
		return (family, Verdict::Transient);
	}
	tracing::debug!(
		family = family.slug(),
		error = %e,
		"settings read unsuccessful: states cleared, reason published, verdict unchanged"
	);
	publish_reason_unknown(publ, family, &cause(e)).await;
	(family, Verdict::Unknown)
}

/// Is this refusal a **transient reason**, which says nothing about writing?
///
/// Three families of symptoms, and they are the three the field saw:
/// the `400` of this dialect, which means "not now" and not "not that";
/// the passed deadline, when the doorbell is asleep or fifteen commands in a row
/// have winded it; and the block received but unusable, which a sleeping firmware
/// A refusal met during a read. The three transient shapes (a `400`, a
/// refusal carrying text, a timeout) do **not** lock: the bridge could not
fn is_transient(e: &Error) -> bool {
	match e {
		Error::CameraServiceUnavailable { code: 400, .. } => true,
		// A read refusal carrying text is still a `400`: same verdict, and the
		// text changes nothing about the refusal being transient.
		Error::CameraRefused { code: 400, .. } => true,
		Error::Timeout(_)
		| Error::TimeoutDisconnected
		| Error::TimeoutError(_)
		| Error::DiscoveryTimeout
		| Error::UnintelligibleXml { .. }
		| Error::UnintelligibleReply { .. } => true,
		_ => false,
	}
}

/// Does this refusal deserve the **second** attempt of [`read_family`]?
/// Only the `400`: it is the only one this repo already treats as retryable —
/// see the loop in `bc_protocol::services.rs`, which tries five times before
/// giving up. A passed deadline is paid twice over radio on a battery camera,
/// and an unintelligible answer does not improve by asking again.
fn is_retryable(e: &Error) -> bool {
	matches!(
		e,
		Error::CameraServiceUnavailable { code: 400, .. } | Error::CameraRefused { code: 400, .. }
	)
}

/// Reads a family and publishes its leaves. The `Ok` carries the number of
/// leaves published, for logs and tests.
///
/// On failure (refusal, timeout, unintelligible block) the error propagates and
/// the dispatch clears the family's retained states. This call costs at least
/// one camera command: it is reserved for explicit
/// demand (`query/settings`), never for a running cycle.
///
/// Each branch is an `async` block of its own, and that is not
/// aesthetics: the protocol blocks (`ImageCfg`, `SystemGeneral`,
/// `DingDongCfg`…) run to several kilobytes, and an unoptimised stack frame
/// keeps all their temporaries alive at once. One single function
/// handling the fifteen blocks together overflowed the stack of the thread that
/// dispatches — 2 MiB for a tokio worker, and that is exactly the path that
/// has to fit inside it. One block per family: each frame now knows only the
/// block it is re-reading.
pub(crate) async fn sync_family(
	bc: &BcCamera,
	publ: &StatusPublisher<'_>,
	family: Family,
	scope: &mut Writability,
) -> Published {
	match family {
		Family::Audio => async {
			let cfg = bc.get_audio_cfg(false).await?;
			publish_audio(publ, &cfg).await;
			// Four leaves since the third volume got its entity:
			// `<volume>`, `<talkAndReplyVolume>`, `<visitorVolume>`,
			// `<visitorLoudspeaker>`.
			Ok(4)
		}
		.await,
		Family::Denoise => async {
			let cfg = bc.get_ai_denoise().await?;
			publish_denoise(publ, &cfg).await;
			Ok(2)
		}
		.await,
		Family::Image => async {
			let cfg = bc.get_image_cfg().await?;
			publish_image(publ, &cfg).await;
			Ok(6)
		}
		.await,
		Family::Osd => async {
			let cfg = bc.get_osd_cfg().await?;
			// Three of the four OSD leaves — the displayed name, showing the
			// name, the watermark — live in `<OsdChannelName>`. Without
			// that tag we have nothing to send back: that is our refusal, not
			// the camera's, and it is the only one that locks a family.
			// The timestamp switch, on its side, would have stayed writable; the
			// granularity discovery offers is the family, so it pays for the other
			// three — a trade-off accepted and written down with the OSD analysis.
			if cfg.channel_name.is_none() {
				scope.writable = false;
				scope.cause = Some(REASON_NO_OSDCHANNEL);
			}
			publish_osd(publ, &cfg).await;
			Ok(4)
		}
		.await,
		Family::Motion => async {
			let cfg = bc.get_motion_cfg().await?;
			// Two carriers, only one rewritable: `<MD>` sends back, `<MdAlarm>`
			// does not (msg 47 expects a `<MD>`). Reading one of the two therefore does
			// not authorise promising the write — which is exactly what discovery
			// must stop doing.
			scope.writable = matches!(cfg, MotionCfg::Md(_));
			publish_motion(publ, &cfg).await;
			Ok(2)
		}
		.await,
		Family::Pir => async {
			let cfg = bc.get_pirstate().await?;
			publish_pir(publ, &cfg).await;
			Ok(2)
		}
		.await,
		Family::Ntp => async {
			let cfg = bc.get_ntp().await?;
			publish_ntp(publ, &cfg).await;
			Ok(3)
		}
		.await,
		Family::Dst => async {
			let cfg = bc.get_dst().await?;
			publish_dst(publ, &cfg).await;
			Ok(2)
		}
		.await,
		Family::AutoReboot => async {
			let cfg = bc.get_auto_reboot().await?;
			publish_auto_reboot(publ, &cfg).await;
			Ok(4)
		}
		.await,
		Family::General => async {
			let cfg = bc.get_general().await?;
			publish_general(publ, &cfg).await;
			Ok(5)
		}
		.await,
		Family::Mask => async {
			let cfg = bc.get_privacy_masks().await?;
			publish_masks(publ, &cfg).await;
			Ok(2)
		}
		.await,
		Family::Sleep => async {
			let cfg = bc.get_sleep_state().await?;
			publish_sleep(publ, &cfg).await;
			Ok(1)
		}
		.await,
		Family::Wifi => async {
			let cfg = bc.get_wifi_signal().await?;
			publish_wifi(publ, &cfg).await;
			Ok(1)
		}
		.await,
		Family::Battery => async {
			let cfg = bc.battery_info().await?;
			publish_battery(publ, &cfg).await;
			Ok(5)
		}
		.await,
		Family::Chime => async {
			// Five commands (483, 484, 485, 486, 609): the most expensive family
			// of the protocol, and the only one whose read refuses itself on
			// a model without a chime.
			let cfg = bc.get_ding_dong_cfg().await?;
			publish_chime(publ, &cfg).await;
			// No identifier = nothing to screw the 485's `<id>` onto: the volume
			// is not “refused by the camera”, it is **unaddressable**, and
			// writing it would send a command with no recipient. Local verdict,
			// like `<MdAlarm>` and `<OsdChannelName>` — not a 400.
			if cfg.chime_id.is_none() {
				scope.writable = false;
				scope.cause = Some(if cfg.list_answered_without_entry {
					REASON_CHIME_NO_ENTRY
				} else {
					REASON_CHIME_NO_ID
				});
			}
			Ok(4)
		}
		.await,
		Family::Ai => async {
			// No camera command at all: this is the verdict, not a re-read.
			//
			// The `ai/*` entities publish their state (they come from the
			// 33 push) and have **no** command topic. The cause is not in
			// the bridge: no activation tag for a detection type has ever been
			// read anywhere (the documented answers to 342/343 show only
			// `<sensitivity>`/`<stayTime>`/sizes, and 342 is queried
			// **per type**). Publishing a `command_topic` would promise a write
			// on an invented tag. Hence this reason, which replaces silence with the
			// check to perform; and hence no state touched here — the event
			// sensors' states are not read back.
			scope.writable = false;
			scope.cause = Some(REASON_AI_NO_TAG);
			Ok(0)
		}
		.await,
	}
}

/// Full synchronisation, in the order of [`Family::ALL`] (battery first:
/// that is frame 253, the one that does not wake the camera and the one the
/// operator needs most often).
///
/// **Never periodic.** On a battery doorbell every family is a radio
/// command; this function is only called on `query/settings`, that is on
/// human demand or an explicit automation.
///
/// The return — a `Vec` the caller supplies and that fills up as we go
/// — lists **all** families in the order read, each with its verdict
/// ([`Verdict`]): that is what discovery needs to publish the writable
/// settings, leave the read-only ones, or keep them where they are.
/// A family refused with a transient code appears there as [`Verdict::Transient`] and
/// is published **writable** with its reason next to it; a family we could not
/// attempt at all (no session, camera unreachable) appears as [`Verdict::Unknown`]
/// and **does not change status**. Confusing them was the regression of
/// the first field day: four families locked by a `400` while
/// the camera was waking up.
///
/// The `Vec` is a parameter and not a return value: when the
/// full synchronisation is cut short by the dispatch deadline, the
/// families already re-read before the interruption are still published by the
/// discovery. Throwing them away with the rest would leave Home Assistant promising
/// a write on a family we have just re-read.
pub(crate) async fn sync_all(
	bc: &BcCamera,
	publ: &StatusPublisher<'_>,
	read: &mut Vec<(Family, Verdict)>,
) {
	read.reserve(Family::ALL.len());
	for family in Family::ALL {
		read.push(read_family(bc, publ, family).await);
	}
}

// ── Writing ─────────────────────────────────────────────────────────────

/// The camera printed this tag, so we may modify it. Otherwise refuse:
/// writing `<tag>` into a block that does not contain one is sending the firmware
/// a tag it never declared — exactly the write it swallows without
/// saying anything.
fn tag_present(current: Option<&str>, tag: &str) -> CoreResult<()> {
	current.map(|_| ()).ok_or_else(|| {
		Error::Usage(format!(
			"this camera's reply has no <{tag}> tag, so this setting cannot be written; \
			 refusing to invent a tag the firmware did not send"
		))
	})
}

/// Same for a numeric tag (`<enable>`, `<offset>`), whose absence shows
/// just as well on the `Option`.
fn tag_present_opt<T>(current: &Option<T>, tag: &str) -> CoreResult<()> {
	current
		.as_ref()
		.map(|_| ())
		.ok_or_else(|| {
			Error::Usage(format!(
				"this camera's reply has no <{tag}> tag, so this setting cannot be written; \
				 refusing to invent a tag the firmware did not send"
			))
		})
}

/// Applies a setting command: read the block, edit the one
/// tag asked for, write, re-read, publish the re-read.
///
/// The `Err` goes up as is to the dispatch, which answers `FAIL` on the command
/// topic: that is what tells a refused write apart from a write
/// accepted and then ignored (in the latter case the published value is not the one
/// that was requested, and the log says so).
pub(crate) async fn apply_setting(
	bc: &BcCamera,
	publ: &StatusPublisher<'_>,
	cmd: &SettingCommand,
) -> Result {
	match cmd {
		// ── audioCfg (264 / 265) ────────────────────────────────────────
		// Three tags, three entities: `<volume>` (general),
		// `<talkAndReplyVolume>` (intercom speech), `<visitorVolume>` (visitor). Each one
		// is re-read, edited on its own, re-written, re-read.
		SettingCommand::AudioGeneralVolume(v) => {
			let mut cfg = bc.get_audio_cfg(false).await?;
			// `AudioCfg::volume` is a `String` and not an `Option`: model
			// 264 prints the tag, a firmware that does not know it leaves the
			// string empty. Writing anyway would send the firmware a tag
			// it never declared — refusing is the right move.
			tag_present(
				Some(cfg.volume.as_str()).filter(|s| !s.trim().is_empty()),
				"volume",
			)?;
			cfg.volume = v.clone();
			let after = bc.set_audio_cfg(&cfg, false).await?;
			warn_if_ignored("volume", v, Some(after.volume.as_str()));
			publish_audio(publ, &after).await;
		}
		SettingCommand::AudioVolume(v) => {
			let mut cfg = bc.get_audio_cfg(false).await?;
			tag_present(cfg.talk_and_reply_volume.as_deref(), "talkAndReplyVolume")?;
			cfg.talk_and_reply_volume = Some(v.clone());
			let after = bc.set_audio_cfg(&cfg, false).await?;
			warn_if_ignored("talkAndReplyVolume", v, after.talk_and_reply_volume.as_deref());
			publish_audio(publ, &after).await;
		}
		SettingCommand::AudioVisitorVolume(v) => {
			let mut cfg = bc.get_audio_cfg(false).await?;
			tag_present(cfg.visitor_volume.as_deref(), "visitorVolume")?;
			cfg.visitor_volume = Some(v.clone());
			let after = bc.set_audio_cfg(&cfg, false).await?;
			warn_if_ignored("visitorVolume", v, after.visitor_volume.as_deref());
			publish_audio(publ, &after).await;
		}
		SettingCommand::AudioVisitorLoudspeaker(on) => {
			let mut cfg = bc.get_audio_cfg(false).await?;
			tag_present(cfg.visitor_loudspeaker.as_deref(), "visitorLoudspeaker")?;
			let text = bit(*on);
			cfg.visitor_loudspeaker = Some(text.to_string());
			let after = bc.set_audio_cfg(&cfg, false).await?;
			warn_if_ignored("visitorLoudspeaker", text, after.visitor_loudspeaker.as_deref());
			publish_audio(publ, &after).await;
		}
		// ── aiDenoise (439 / 440) ───────────────────────────────────────
		SettingCommand::DenoiseEnable(on) => {
			let mut cfg = bc.get_ai_denoise().await?;
			tag_present(cfg.enable.as_deref(), "enable")?;
			cfg.enable = Some(bit(*on).to_string());
			let after = bc.set_ai_denoise(&cfg).await?;
			warn_if_ignored("aiDenoise.enable", bit(*on), after.enable.as_deref());
			publish_denoise(publ, &after).await;
		}
		SettingCommand::DenoiseLevel(v) => {
			let mut cfg = bc.get_ai_denoise().await?;
			tag_present(cfg.level.as_deref(), "level")?;
			cfg.level = Some(v.clone());
			let after = bc.set_ai_denoise(&cfg).await?;
			warn_if_ignored("aiDenoise.level", v, after.level.as_deref());
			publish_denoise(publ, &after).await;
		}
		// ── image (26 / 25) ─────────────────────────────────────────────
		SettingCommand::ImageBright(v) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.bright.as_deref(), "bright")?;
			cfg.video_input.bright = Some(v.clone());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		SettingCommand::ImageContrast(v) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.contrast.as_deref(), "contrast")?;
			cfg.video_input.contrast = Some(v.clone());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		SettingCommand::ImageSaturation(v) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.saturation.as_deref(), "saturation")?;
			cfg.video_input.saturation = Some(v.clone());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		SettingCommand::ImageHue(v) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.hue.as_deref(), "hue")?;
			cfg.video_input.hue = Some(v.clone());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		SettingCommand::ImageSharpen(v) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.sharpen.as_deref(), "sharpen")?;
			cfg.video_input.sharpen = Some(v.clone());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		SettingCommand::ImageDayNight(mode) => {
			let mut cfg = bc.get_image_cfg().await?;
			tag_present(cfg.video_input.day_night.as_deref(), "dayNight")?;
			cfg.video_input.day_night = Some(mode.as_text().to_string());
			publish_image(publ, &bc.set_image_cfg(&cfg).await?).await;
		}
		// ── osd (44 / 45) ───────────────────────────────────────────────
		SettingCommand::OsdDatetimeEnable(on) => {
			let mut cfg = bc.get_osd_cfg().await?;
			tag_present(cfg.datetime.enable.as_deref(), "enable")?;
			cfg.datetime.enable = Some(bit(*on).to_string());
			publish_osd(publ, &bc.set_osd_cfg(&cfg).await?).await;
		}
		SettingCommand::OsdNameEnable(on) => {
			let mut cfg = bc.get_osd_cfg().await?;
			let mut block = cfg.channel_name.clone().ok_or_else(|| {
				Error::Usage(
					"this camera's osd reply has no <OsdChannelName> block, so the channel \
					 name cannot be written"
						.to_string(),
				)
			})?;
			tag_present(block.enable.as_deref(), "enable")?;
			block.enable = Some(bit(*on).to_string());
			cfg.channel_name = Some(block);
			publish_osd(publ, &bc.set_osd_cfg(&cfg).await?).await;
		}
		SettingCommand::OsdName(name) => {
			let mut cfg = bc.get_osd_cfg().await?;
			let mut block = cfg.channel_name.clone().ok_or_else(|| {
				Error::Usage(
					"this camera's osd reply has no <OsdChannelName> block, so the channel \
					 name cannot be written"
						.to_string(),
				)
			})?;
			tag_present(block.name.as_deref(), "name")?;
			block.name = Some(name.clone());
			cfg.channel_name = Some(block);
			publish_osd(publ, &bc.set_osd_cfg(&cfg).await?).await;
		}
		SettingCommand::OsdWatermark(on) => {
			let mut cfg = bc.get_osd_cfg().await?;
			let mut block = cfg.channel_name.clone().ok_or_else(|| {
				Error::Usage(
					"this camera's osd reply has no <OsdChannelName> block, so the watermark \
					 switch cannot be written"
						.to_string(),
				)
			})?;
			tag_present(block.en_watermark.as_deref(), "enWatermark")?;
			block.en_watermark = Some(bit(*on).to_string());
			cfg.channel_name = Some(block);
			publish_osd(publ, &bc.set_osd_cfg(&cfg).await?).await;
		}
		// ── motion (46 / 47) ─────────────────────────────────────────
		SettingCommand::MotionEnable(on) => {
			let mut cfg = bc.get_motion_cfg().await?;
			tag_present(cfg.enable(), "enable")?;
			cfg.set_enable(bit(*on));
			publish_motion(publ, &bc.set_motion_cfg(&cfg).await?).await;
		}
		SettingCommand::MotionSensitivity(v) => {
			let mut cfg = bc.get_motion_cfg().await?;
			tag_present(cfg.sensitivity(), "sensitivity")?;
			cfg.set_sensitivity(v.clone());
			publish_motion(publ, &bc.set_motion_cfg(&cfg).await?).await;
		}
		// ── PIR: the sensitivity only (the switch stays on control/pir)
		SettingCommand::PirSensitivity(v) => {
			let mut cfg = bc.get_pirstate().await?;
			// The two spellings depending on the firmware; we edit the one that is
			// there, and refuse if neither is.
			if cfg.sensiValue.is_some() {
				tag_present_opt(&cfg.sensiValue, "sensiValue")?;
				cfg.sensiValue = Some(v.parse::<u8>().map_err(|_| {
					Error::Usage(format!("<sensiValue> must be an 8-bit integer, got {v}"))
				})?);
			} else if cfg.sensitivity.is_some() {
				tag_present_opt(&cfg.sensitivity, "sensitivity")?;
				cfg.sensitivity = Some(v.parse::<u8>().map_err(|_| {
					Error::Usage(format!("<sensitivity> must be an 8-bit integer, got {v}"))
				})?);
			} else {
				return Err(Error::Usage(
					"this camera's PIR reply has neither <sensiValue> nor <sensitivity>, so \
					 the sensitivity cannot be written"
						.to_string(),
				));
			}
			bc.set_pirstate(cfg).await?;
			// Re-read: what the hardware kept is what gets published.
			let after = bc.get_pirstate().await?;
			publish_pir(publ, &after).await;
		}
		// ── PIR: the rest time between two alarms (same carrier, 212/213)
		SettingCommand::PirInterval(v) => {
			let mut cfg = bc.get_pirstate().await?;
			// `<interval>` only exists on firmware v1.1+. A camera that never
			// printed it receives **nothing**: the bridge does not invent for the
			// firmware a tag it did not declare, and the reason published under
			// `status/pir/reason` says so instead of a mute failure.
			tag_present_opt(&cfg.interval, "interval")?;
			cfg.interval = Some(v.parse::<u32>().map_err(|_| {
				Error::Usage(format!("<interval> must be a whole number of seconds, got {v}"))
			})?);
			bc.set_pirstate(cfg).await?;
			let after = bc.get_pirstate().await?;
			publish_pir(publ, &after).await;
			warn_if_ignored(
				"interval",
				&v,
				after.interval.map(|g| g.to_string()).as_deref(),
			);
		}
		// ── ntp (38 / 39) ───────────────────────────────────────────────
		SettingCommand::NtpEnable(on) => {
			let mut cfg = bc.get_ntp().await?;
			tag_present(cfg.enable.as_deref(), "enable")?;
			cfg.enable = Some(bit(*on).to_string());
			publish_ntp(publ, &bc.set_ntp(&cfg).await?).await;
		}
		SettingCommand::NtpServer(host) => {
			let mut cfg = bc.get_ntp().await?;
			tag_present(cfg.server.as_deref(), "server")?;
			cfg.server = Some(host.clone());
			publish_ntp(publ, &bc.set_ntp(&cfg).await?).await;
		}
		SettingCommand::NtpInterval(v) => {
			let mut cfg = bc.get_ntp().await?;
			tag_present(cfg.synchronize_interval.as_deref(), "synchronizeInterval")?;
			cfg.synchronize_interval = Some(v.clone());
			publish_ntp(publ, &bc.set_ntp(&cfg).await?).await;
		}
		// ── daylight saving (106 / 107) ─────────────────────────────────────
		SettingCommand::DstEnable(on) => {
			let mut cfg = bc.get_dst().await?;
			tag_present_opt(&cfg.enable, "enable")?;
			cfg.enable = Some(u32::from(*on));
			publish_dst(publ, &bc.set_dst(&cfg).await?).await;
		}
		SettingCommand::DstOffset(v) => {
			let mut cfg = bc.get_dst().await?;
			tag_present_opt(&cfg.offset, "offset")?;
			cfg.offset = Some(v.parse::<i32>().map_err(|_| {
				Error::Usage(format!("<offset> must be a whole number of hours, got {v}"))
			})?);
			publish_dst(publ, &bc.set_dst(&cfg).await?).await;
		}
		// ── auto reboot (101 / 100) ─────────────────────────────────────────────
		SettingCommand::AutoRebootEnable(on) => {
			let mut cfg = bc.get_auto_reboot().await?;
			tag_present(cfg.enable.as_deref(), "enable")?;
			cfg.enable = Some(bit(*on).to_string());
			publish_auto_reboot(publ, &bc.set_auto_reboot(&cfg).await?).await;
		}
		SettingCommand::AutoRebootWeekday(d) => {
			let mut cfg = bc.get_auto_reboot().await?;
			tag_present(cfg.week_day.as_deref(), "weekDay")?;
			cfg.week_day = Some(d.as_text().to_string());
			publish_auto_reboot(publ, &bc.set_auto_reboot(&cfg).await?).await;
		}
		SettingCommand::AutoRebootHour(v) => {
			let mut cfg = bc.get_auto_reboot().await?;
			tag_present(cfg.hour.as_deref(), "hour")?;
			cfg.hour = Some(v.clone());
			publish_auto_reboot(publ, &bc.set_auto_reboot(&cfg).await?).await;
		}
		SettingCommand::AutoRebootMinute(v) => {
			let mut cfg = bc.get_auto_reboot().await?;
			tag_present(cfg.minute.as_deref(), "minute")?;
			cfg.minute = Some(v.clone());
			publish_auto_reboot(publ, &bc.set_auto_reboot(&cfg).await?).await;
		}
		// ── system (104 / 105) ──────────────────────────────────────────────────
		SettingCommand::GeneralOsdFormat(f) => {
			let mut cfg = bc.get_general().await?;
			tag_present(cfg.osd_format.as_deref(), "osdFormat")?;
			cfg.osd_format = Some(f.as_text().to_string());
			publish_general(publ, &bc.set_general(&cfg).await?).await;
		}
		SettingCommand::GeneralTimeFormat(v) => {
			let mut cfg = bc.get_general().await?;
			tag_present_opt(&cfg.time_format, "timeFormat")?;
			cfg.time_format = Some(v.parse::<u8>().map_err(|_| {
				Error::Usage(format!("<timeFormat> must be 0 or 1, got {v}"))
			})?);
			publish_general(publ, &bc.set_general(&cfg).await?).await;
		}
		SettingCommand::GeneralLanguage(v) => {
			let mut cfg = bc.get_general().await?;
			tag_present(cfg.language.as_deref(), "language")?;
			cfg.language = Some(v.clone());
			publish_general(publ, &bc.set_general(&cfg).await?).await;
		}
		SettingCommand::GeneralDeviceName(v) => {
			let mut cfg = bc.get_general().await?;
			tag_present(cfg.device_name.as_deref(), "deviceName")?;
			cfg.device_name = Some(v.clone());
			publish_general(publ, &bc.set_general(&cfg).await?).await;
		}
		// ── chime ───────────────────────────────────────────────────────────────
		SettingCommand::ChimeRing { chime_id, music_id } => {
			// Without an explicit id, we ring the chime the camera prefers
			// (the one `get_ding_dong_cfg` kept). The refusal is explicit:
			// "no paired chime" is not a mute success.
			let id = match chime_id.as_deref() {
				Some(id) => id.to_string(),
				None => {
					let cfg = Box::pin(bc.get_ding_dong_cfg()).await?;
					cfg.chime_id.ok_or_else(|| {
						Error::Usage(
							"no paired wireless chime: nothing to ring (the camera listed none)"
								.to_string(),
						)
					})?
				}
			};
			bc.ring_ding_dong(&id, music_id).await?;
			// Ringing is not a state: we do not re-read the chime list
			// for that, that would be five radio commands for nothing.
		}
		SettingCommand::ChimeVolume(v) => {
			// The volume comes back on 485 `setParam`, so it takes a chime
			// **addressable** and a `<volLevel>` tag the camera printed.
			// Both refusals are local and explicit: without an id the command
			// has no recipient; without a tag read, writing it would invent
			// a tag — this file's rule, applied on the writing side.
			let mut cfg = Box::pin(bc.get_ding_dong_cfg()).await?;
			cfg.chime_id.clone().ok_or_else(|| {
				Error::Usage(
					"no addressable wireless chime: nothing to set the volume of (the camera listed none)"
						.to_string(),
				)
			})?;
			let params = cfg.params.as_mut().ok_or_else(|| {
				Error::Usage(
					"the chime answered 485 getParam without a block: <volLevel> was never printed"
						.to_string(),
				)
			})?;
			tag_present(params.vol_level.as_deref(), "volLevel")?;
			params.vol_level = Some(v.clone());
			let reread = bc.set_ding_dong_cfg(&cfg).await?;
			// It is the re-read that gets published, like every other leaf.
			publish_chime(publ, &reread).await;
			warn_if_ignored(
				"volLevel",
				&v,
				reread.params.as_ref().and_then(|p| p.vol_level.as_deref()),
			);
		}
	}
	Ok(())
}

/// `1`/`0`, the text firmwares expect for a flag.
fn bit(on: bool) -> &'static str {
	if on {
		"1"
	} else {
		"0"
	}
}

/// The firmware answered 200 then kept another value: this is the
/// "write accepted, ignored" case that the cache of the requested value always
/// masked. The log says it, and so does the published state (it is the re-read).
fn warn_if_ignored(tag: &str, requested: &str, kept: Option<&str>) {
	if kept != Some(requested) {
		tracing::warn!(
			tag,
			requested,
			kept = kept.unwrap_or("<absent>"),
			"the camera accepted the write then kept another value: it is the one published"
		);
	}
}

#[cfg(test)]
mod tests {
	use bairelay_neolink_core::bc::xml::{BcXml, DingDongCfg, SystemGeneral};
	use bairelay_neolink_core::bc_protocol::ImageCfg;

	/// The size of [`BcXml`] is the bridge's hidden stack constraint. Every
	/// camera command moves at least one of these blocks — often two when a
	/// write is followed by a re-read — and the block moves by value.
	/// The settings added here fill its variants more: the weight
	/// per block is the same as the blocks already there, but the chain
	/// `dispatch_control → family → command` is longer than before.
	///
	/// This test is a wake-up call, not a diploma: if the block grows further, the
	/// question “does it still fit in a tokio worker's 2 MiB?” has to be asked
	/// again — and it is the tests in `crate::mqtt_dispatch`, run in release, that
	/// answer it. The figures are printed for the report.
	#[test]
	fn bcxml_guards_a_known_size() {
		let blocs = [
			("BcXml", core::mem::size_of::<BcXml>()),
			("ImageCfg", core::mem::size_of::<ImageCfg>()),
			("DingDongCfg", core::mem::size_of::<DingDongCfg>()),
			("SystemGeneral", core::mem::size_of::<SystemGeneral>()),
		];
		for (nom, taille) in blocs {
			eprintln!("size_of::<{nom}>() = {taille} octets");
		}
		assert!(
			core::mem::size_of::<BcXml>() < 96 * 1024,
			"BcXml grew past what the bridge knows how to stack; this dialogue point is a weak signal, not a ceiling: the threshold is raised to 96 KiB to absorb the settings blocks, and an overflow should start the discussion about the tokio workers ' stack"
		);
	}

	/// The reason for a **read** refusal carries the camera's answer, and
	/// stays publishable as is: one line, no tag, bounded.
	///
	/// This is the measurement of 13 November: four families on this doorbell die
	/// on a `400` and the broker only said “code 400”. What the camera writes under
	/// its code is the only thing that tells a bad request shape apart from a family
	/// missing from the firmware — so it has to reach the operator, and not stop
	/// in a container's log.
	#[test]
	fn the_reason_for_a_read_refusal_carries_the_answer() {
		let err = bairelay_neolink_core::Error::CameraRefused {
			id: 26,
			code: 400,
			detail: "<error><rspCode>-4</rspCode>\n<rspString>cmd not support</rspString></error>"
				.to_string(),
		};
		let cause = super::cause(&err);
		assert!(cause.contains("400"), "the code stays in the sentence: {cause}");
		assert!(
			cause.contains("cmd not support"),
			"the camera has to be readable in the reason: {cause}"
		);

		let published = bairelay_mqtt::status::publishable_reason(&cause);
		assert!(!published.contains('<'), "no tag goes to the broker: {published}");
		assert!(
			published.chars().count() <= 160,
			"the line stays bounded: {} characters",
			published.chars().count()
		);
		assert!(
			published.contains("cmd not support"),
			"the cut must not swallow the camera answer: {published}"
		);
	}
}
