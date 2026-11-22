//! A read burst triggered by the camera's **spontaneous wake**.
//!
//! # Pourquoi ce module existe
//!
//! The battery is the field emergency: no timer reads the camera any more
//! (`idle_disconnect = true`, `battery_update` set to "never"), so the battery
//! level only came back up if a person asked for
//! `query/settings`. The camera, on its side, wakes itself up: a press on the
//! button, a motion report, a recording pushed on the fake cloud's
//! channel. Every wake is a window where one can read **for free** — the
//! radio is already open.
//!
//! A burst is that window: a few commands, paid for by the wake, never
//! ailleurs.
//!
//! # The four rules, and why tests lock them
//!
//! 1. **One wake = one burst.** The guard ([`BurstGuard`]`) refuses a second
//!    burst while the first one runs *and* for [`GUARD_WINDOW`] after
//!    it. Three sources can announce the same wake (the session, the
//!    webhook, the push): without this, one would pay three bursts at once.
//! 2. **Never periodic.** This module has no `interval` and no cadencing `sleep`:
//!    the memory of [`crate::family_state::proven_families`] is what publishes the
//!    discovery without radio. The module's only `sleep` is the session-wait probe
//!    ([`await_session`]), bounded, which emits no command at all.
//!
//!    Exactly one caller is not a wake the camera took: [`crate::startup_wake`],
//!    with `Wake::Startup`. It wakes nothing - it reads the families the memory
//!    has never proved, inside the session the startup wake had already opened to
//!    warm the last-frame buffer, and it does not run at all when the memory has
//!    nothing left to prove. The rule used to be "never at startup", and that word
//!    cost an installation every write entity it owned: the state file had been
//!    renamed, its proof was not read, and the only way back was a wake the camera
//!    happened to take - which a battery doorbell in maximum power saving may not do
//!    for days. A rule that leaves the operator unable to move a slider is not a
//!    battery rule any more, it is an accident.
//! 3. **Never during a stream.** The burst refuses to start while a
//!    `StreamSource` has subscribers: a camera sending video is not the moment
//!    to ask it for ten XML blocks. What the guard cannot see is a session
//!    opened by **another process** — subscriber counts are in-process — so
//!    anything that takes the camera from outside this binary is not held off
//!    by this rule, and no code here pretends otherwise.
//! 4. **A high bound on commands, logged.** [`COMMAND_BUDGET`] is the
//!    maximum spend of a burst, evaluated before leaving ([`build_plan`])
//!    and written in the log together with what it dropped. An expensive family
//!    (`chime`, five commands) cannot eat the whole window unnoticed.
//!
//! # What happens when the camera refuses
//!
//! Two refusals, and they do not look alike.
//!
//! * **The camera answered nothing** ([`Verdict::Unknown`]: no session,
//!   unreachable): the burst stops **on the spot**, the families already read
//!   stay published, and the lock is returned — insisting on a camera that does
//!   not answer is not paid for twice.
//! * **The camera answered "no"** ([`Verdict::Transient`]: a `400`, a
//!   deadline, an unusable block): the burst **continues**. Measured on 13 November on
//!   this doorbell: `image` (26), `osd` (44), `motion` (46) and `mask` (52)
//!   return `400` even with the camera **properly awake**, while battery (253),
//!   PIR (212) and the chime answer in the same session. One family's refusal
//!   therefore teaches nothing about the next one, and the old rule — stop at
//!   the first `400` — kept the chime from ever being read for weeks:
//!   the tour dead at four commands out of eight, `chime` never reached.
//!
//! Two guard-rails keep that "continue" from turning into waste: two
//! **consecutive** refusals return the session ([`MAX_CONSECUTIVE_REFUSALS`]) — a camera
//! that says no twice in a row probably will not say anything better — and a
//! family refused on [`FAMILY_MAX_REFUSALS`] wakes in a row leaves the plan
//! ([`BurstGuard::family_saturated`]): one does not buy the same closed
//! door a hundred times. The reason for each refusal stays published by
//! [`crate::settings::read_family`] itself.
//!
//! # The memory of families
//!
//! The burst guesses no family: it re-reads the ones whose writability was
//! **already proven** ([`crate::family_state`]), and writes that memory
//! **only once** per burst — a single call to
//! [`CameraHandle::mark_families_read`] at the end of the burst, which is the only
//! path that touches the disk.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bairelay_mqtt::settings::Family;
use bairelay_mqtt::{SharedMqttClient, StatusPublisher};
use bairelay_neolink_core::bc_protocol::BcCamera;
use tokio::task::JoinHandle;

use crate::camera::CameraHandle;
use crate::settings::{self, read_family, Verdict};

/// Window during which a second wake does **not** restart a burst.
///
/// A few seconds, no more: the field shows the button, the push and
/// the webhook alarm inside the same half second. Too short, one pays two bursts
/// for one finger; too long, a second visit thirty seconds later reads
/// nothing. Twenty seconds covers the typical motion hold.
pub(crate) const GUARD_WINDOW: Duration = Duration::from_secs(20);

/// High bound on the number of radio commands a burst may pay.
///
/// This is **the** line to move if the operator finds the burst too stingy or
/// too expensive: it is written in every burst's log, and the
/// families it made drop are counted alongside it.
pub(crate) const COMMAND_BUDGET: usize = 8;

/// Ceiling on a burst's duration, past which the session is returned.
///
/// A camera that sleeps badly answers one time in ten and the bridge must not
/// hold the wake open for that: past the ceiling, what is read is published, the rest
/// will wait for the next wake.
const TIME_BUDGET: Duration = Duration::from_secs(25);

/// How long to wait for the Baichuan session to be established.
///
/// The wake holds the lock, the camera's connection loop does the
/// rest: twenty seconds cover a slow establishment over Wi-Fi without
/// pinning the camera if it is dead.
const SESSION_WAIT: Duration = Duration::from_secs(20);

/// Period of the session-wait probe.
const SESSION_POLL: Duration = Duration::from_millis(200);

/// After that many consecutive wakes with no answer to frame 253, the burst
/// stops paying for it.
///
/// The burst's counterpart of the poller's `battery_unsupported` lock —
/// which the burst cannot wait for: with `battery_update` set to
/// "never", the poller no longer ticks and therefore never locks anything. A
/// mains camera does not deserve one lost command per press, indefinitely.
const BATTERY_MAX_REFUSALS: usize = 3;

/// After that many consecutive wakes refusing a family, the burst stops
/// paying for it — until the next restart of the bridge.
///
/// Three, not one: the field shows `400`s that clear from one wake to
/// the next (`image` has already returned its block after a refusal). Three misses in a
/// row, on a camera we know is fine since the battery and the PIR
/// do answer, is a closed door: each new attempt is one more second
/// of radio and nothing else.
const FAMILY_MAX_REFUSALS: usize = 3;

/// That many refusals **in a row in the same tour** return the session.
///
/// The "continue" acquired on 13 November — a `400` on one family says nothing about
/// the next — must not become a tour of fifteen closed doors. Two is
/// the compromise: enough to give `chime` its chance behind `image`, enough
/// few not to burn a wake against a mute firmware.
const MAX_CONSECUTIVE_REFUSALS: usize = 2;

/// What woke the camera up. Published with the burst state: "does
/// a button press cost me a burst?" is the first question of
/// an operator who suspects the battery of draining fast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wake {
	/// Press on the doorbell's button.
	Press,
	/// Motion report (session alarm, relayed alarm, PIR webhook).
	Motion,
	/// Inbound frame on the fake cloud's channel (TCP push).
	Push,
	/// The bridge came up and the startup wake is holding a session open for its
	/// own reasons. Not a wake the camera took: the radio is open because **we**
	/// opened it, so the question asked on top of it costs commands and not a wake.
	Startup,
}

impl Wake {
	/// The short word published in the burst state.
	pub(crate) fn slug(self) -> &'static str {
		match self {
			Wake::Press => "press",
			Wake::Motion => "motion",
			Wake::Push => "push",
			Wake::Startup => "startup",
		}
	}
}

/// Why the burst stopped — and therefore what the operator will read on
/// `status/<cam>/status/burst`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
	/// No MQTT: nothing to publish, so nothing to read.
	NoMqtt,
	/// A stream is running: rule 3.
	Stream,
	/// No Baichuan session was established during [`SESSION_WAIT`].
	NoSession,
	/// [`MAX_CONSECUTIVE_REFUSALS`] refusals in a row, or an impossible attempt
	/// ([`Verdict::Unknown`]): the burst stops there, reasons published.
	Refused,
	/// The [`COMMAND_BUDGET`] bound made at least one family drop.
	Budget,
	/// [`TIME_BUDGET`] exceeded.
	Duration,
	/// The bridge is stopping (the camera's token was cancelled).
	Cancelled,
	/// The whole plan went through.
	Done,
}

impl Stop {
	/// The short word published in the burst state.
	pub(crate) fn slug(self) -> &'static str {
		match self {
			Stop::NoMqtt => "no-mqtt",
			Stop::Stream => "stream",
			Stop::NoSession => "no-session",
			Stop::Refused => "refused",
			Stop::Budget => "budget",
			Stop::Duration => "duration",
			Stop::Cancelled => "cancelled",
			Stop::Done => "done",
		}
	}
}

/// What a camera already knows how to do, between two bursts.
///
/// One instance per camera, sitting on the [`crate::camera::CameraHandle`]: it is the object
/// that answers "did I already pay a burst for this wake?", a question asked
/// simultaneously from the session, the webhook and the push.
#[derive(Debug)]
pub(crate) struct BurstGuard {
	/// Is a burst running right now?
	in_flight: AtomicBool,
	/// Start instant of the last accepted burst.
	last: Mutex<Option<Instant>>,
	/// Refusal window after a burst ([`GUARD_WINDOW`] in production).
	window: Duration,
	/// Consecutive refusals of frame 253, seen by the bursts.
	batterie_refus: AtomicUsize,
	/// Consecutive refusals per family, seen by the bursts of **this** camera.
	///
	/// In memory only, and that is intended: the batch memory
	/// ([`crate::family_state`]) says whether a family is *writable*, a durable
	/// question; here the question is "do I keep paying a
	/// read this camera has been refusing for three wakes?", a session
	/// question. A restart therefore reopens the three attempts — that is exactly
	/// what a restart means for the operator.
	refus_familles: Mutex<HashMap<Family, usize>>,
}

impl BurstGuard {
	/// A fresh guard, with the refusal window to apply.
	pub(crate) fn new(window: Duration) -> Self {
		BurstGuard {
			in_flight: AtomicBool::new(false),
			last: Mutex::new(None),
			window,
			batterie_refus: AtomicUsize::new(0),
			refus_familles: Mutex::new(HashMap::new()),
		}
	}

	/// Reserve the burst: `true` if the caller may run it.
	///
	/// The `now` parameter exists because the window is checked to within two
	/// instants and a test cannot wait twenty seconds. A refusal
	/// leaves the guard available (`in_flight` put back to `false`): it is the caller
	/// who is not allowed to run, not the camera.
	pub(crate) fn take(&self, now: Instant) -> bool {
		if self.in_flight.swap(true, Ordering::SeqCst) {
			return false;
		}
		let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
		if let Some(since) = *last {
			if now.duration_since(since) < self.window {
				drop(last);
				self.in_flight.store(false, Ordering::SeqCst);
				return false;
			}
		}
		*last = Some(now);
		drop(last);
		true
	}

	/// A burst just finished: the guard is available again, but the
	/// window is still running.
	pub(crate) fn release_guard(&self) {
		self.in_flight.store(false, Ordering::SeqCst);
	}

	/// Is a burst running? Observed by the tests: production decides
	/// with [`BurstGuard::take`], which reserves and answers in the same move.
	#[cfg(test)]
	pub(crate) fn in_flight(&self) -> bool {
		self.in_flight.load(Ordering::SeqCst)
	}

	/// Should we stop paying for frame 253?
	pub(crate) fn battery_depleted(&self) -> bool {
		self.batterie_refus.load(Ordering::SeqCst) >= BATTERY_MAX_REFUSALS
	}

	/// Count the outcome of a frame 253: a success puts the counter back to zero.
	pub(crate) fn note_battery(&self, repondu: bool) {
		if repondu {
			self.batterie_refus.store(0, Ordering::SeqCst);
		} else {
			self.batterie_refus.fetch_add(1, Ordering::SeqCst);
		}
	}

	/// A family was just refused: count, and say how many times in a
	/// row. A read that succeeds calls [`BurstGuard::note_family_read`]
	/// and puts the counter back to zero — a family that comes back is not a
	/// dead family.
	pub(crate) fn note_family_refused(&self, family: Family) -> usize {
		let mut refused = self.refus_familles.lock().unwrap_or_else(|p| p.into_inner());
		let compteur = refused.entry(family).or_insert(0);
		*compteur += 1;
		*compteur
	}

	/// A family just answered: it is no longer in debt. The key stays
	/// in place at `0` (one key per family, fifteen at most) rather than
	/// unbalancing the table at every burst.
	pub(crate) fn note_family_read(&self, family: Family) {
		let mut refused = self.refus_familles.lock().unwrap_or_else(|p| p.into_inner());
		refused.insert(family, 0);
	}

	/// Should we stop paying for this family? [`FAMILY_MAX_REFUSALS`] refusals in a
	/// row, and nothing more until the next success — which, by definition, cannot
	/// come while the family is set aside: it is the bridge's restart
	/// that replays its chance, and that is assumed.
	pub(crate) fn family_saturated(&self, family: Family) -> bool {
		self.refus_familles
			.lock()
			.unwrap_or_else(|p| p.into_inner())
			.get(&family)
			.is_some_and(|n| *n >= FAMILY_MAX_REFUSALS)
	}
}

/// Returns the guard when the burst task ends, including by panicking.
///
/// Without that, a panic in a settings read would condemn every following
/// burst of this camera — the worst silence there is on a doorbell
/// whose battery is exactly what we are watching.
struct BurstRelease(Arc<BurstGuard>);

impl Drop for BurstRelease {
	fn drop(&mut self) {
		self.0.release_guard();
	}
}

/// The plan of a burst: what will be read, what it costs, what was dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BurstPlan {
	/// The families that will be re-read, in read order.
	pub(crate) families: Vec<Family>,
	/// Total of commands evaluated ([`cost`]).
	pub(crate) commands: usize,
	/// Families set aside because the bound would not pay for them.
	pub(crate) skipped: usize,
}

/// How many radio commands re-reading a family costs.
///
/// The numbers come from [`crate::settings::sync_family`], branch by
/// branch — not an estimate: `chime` is the only family at five commands
/// (483, 484, 485, 486, 609), and `ai` costs none because no activation
/// tag was ever documented.
pub(crate) fn cost(family: Family) -> usize {
	match family {
		Family::Battery => 1,
		Family::Chime => 5,
		Family::Ai => 0,
		_ => 1,
	}
}

/// The read rank inside the window, measured on 13 November.
///
/// The [`COMMAND_BUDGET`] bound is not the real constraint. What cuts
/// The refusal quoted below is the payload the camera answered on the 13th; the
/// `etat` field keeps the wire's French spelling because it is quoted as the evidence.
/// the tour is the camera: the 13 November `status/burst` answers `etat:"refus"` at
/// four commands out of eight, both for `raison:"appui"` and for
/// `raison:"mouvement"`, and the `event 4` line of the same minute prices the
/// session at about fifteen seconds. Sorting only by cost therefore let
/// the families leave in catalogue order and return the session before
/// the ones that are of use.
///
/// Three ranks, each backed by a measurement:
///
/// * **first, `motion` and `osd`** — the two families whose state is used every
///   day, and the two that answer `200` in the frame shape this
///   firmware accepts (the probe campaign of the same day);
/// * **then everything else**, from cheapest to dearest, as before;
/// * **last, `image`** — slot 26 returns a `<Shelter>` and not the
///   `<VideoInput>` asked for, the read ends in a name error that
///   erases the values already published, and the command costs exactly what
///   a useful read costs. The family is not removed: it keeps its
///   verdict
///   and its reason, it simply goes after the ones that bring something
///   back when the window closes.
fn rank(family: Family) -> u8 {
	match family {
		Family::Motion | Family::Osd => 0,
		Family::Image => 2,
		_ => 1,
	}
}

/// Establish the plan: the measured rank first, then the cheapest family,
/// up to the bound, and skip the rest without stopping there.
///
/// Three rules, and the test is what states them:
///
/// * **skip and continue**, and not stop at the first family too
///   expensive — a five-command family must not lose the six
///   one-command reads that do fit in the same window;
/// * **rank before cost** ([`rank`]): when the camera returns the session at
///   the fourth order, the entry order is what determines what came back;
/// * **pay from cheapest to dearest** at equal rank — at equal budget, what
///   counts is the number of families verified as back per wake of the
///   camera. The sort is **stable**: at equal rank and cost the received order is
///   kept, and two bursts on the same set of families look alike.
///
/// It is also what makes the bound **high** in the strict sense:
/// [`BurstPlan::commands`] never exceeds `budget`.
pub(crate) fn build_plan(families: &[Family], budget: usize) -> BurstPlan {
	let mut paid: Vec<(u8, usize, Family)> =
		families.iter().copied().map(|f| (rank(f), cost(f), f)).collect();
	paid.sort_by_key(|(rank, cost, _)| (*rank, *cost));
	let mut plan = BurstPlan { families: Vec::new(), commands: 0, skipped: 0 };
	for (_, cost, family) in paid {
		if plan.commands + cost > budget {
			plan.skipped += 1;
			continue;
		}
		plan.commands += cost;
		plan.families.push(family);
	}
	plan
}

/// The families the memory says are proven, minus the battery.
///
/// The battery is read separately, first: that is why the burst exists,
/// and its level is published on a topic `sync_family` does not fill.
pub(crate) fn families_to_read(camera: &str) -> Vec<Family> {
	crate::family_state::proven_families(camera)
		.into_iter()
		.filter(|f| *f != Family::Battery)
		.collect()
}

/// The families the memory has **never** proved, minus the two a burst cannot
/// prove whatever it spends: the battery, which is read first and outside the plan
/// on its own frame, and `ai`, whose activation tag has never been documented - a
/// read of it cannot make anything writable, so it would buy a reason and nothing
/// else.
///
/// This is what a startup read asks. It is not the same question as
/// [`families_to_read`]: after a press the burst re-reads what the memory proved,
/// because refreshing values is what a press window is for. At startup the camera is
/// awake on our account, and the worth-asking question is the one never asked.
pub(crate) fn families_never_read(camera: &str) -> Vec<Family> {
	unproven_from(&crate::family_state::proven_families(camera))
}

/// [`families_never_read`] without the file, so the rule is testable: the state
/// file's location in [`crate::family_state`] is resolved once per process and
/// cannot be pointed at a fixture per test.
fn unproven_from(proven: &[Family]) -> Vec<Family> {
	Family::ALL
		.into_iter()
		.filter(|f| !proven.contains(f))
		.filter(|f| *f != Family::Battery && *f != Family::Ai)
		.collect()
}

/// Trigger a burst if the wake is entitled to its own.
///
/// Called **after** the publication of the event that triggered it — never
/// before: a press must be published before the first read command, otherwise
/// the operator's automation discovers the press with the delay of the whole
/// burst. The decision to leave is made **here**, synchronously, because two wakes
/// a millisecond apart must give a single burst.
///
/// Returns the task handle for the tests; production callers
/// ignore it: the burst is detached by construction.
pub(crate) fn trigger(handle: Arc<CameraHandle>, reason: Wake) -> Option<JoinHandle<()>> {
	if !handle.burst().take(Instant::now()) {
		tracing::debug!(
			camera = %handle.name(),
			reason = reason.slug(),
			"no burst for this wake: a burst is already running, or this wake falls in the previous one's window"
		);
		return None;
	}
	tracing::info!(
		camera = %handle.name(),
		reason = reason.slug(),
		budget = COMMAND_BUDGET,
		"wake burst started"
	);
	Some(tokio::spawn(task(handle, reason)))
}

/// The body of the burst, once the guard is reserved.
async fn task(handle: Arc<CameraHandle>, reason: Wake) {
	let _fin = BurstRelease(Arc::clone(handle.burst()));

	let Some(mqtt) = handle.mqtt_client() else {
		let stop = Stop::NoMqtt;
		tracing::debug!(
			camera = %handle.name(),
			reason = reason.slug(),
			state = stop.slug(),
			"burst without MQTT: nothing to publish, so nothing to read"
		);
		return;
	};
	let mqtt = SharedMqttClient::clone(mqtt);
	let prefix = handle.topic_prefix().to_string();
	let publisher = StatusPublisher::new(&mqtt, &prefix, handle.name());

	// Rule 3: never during a stream.
	if handle.stream_in_flight() {
		tracing::info!(
			camera = %handle.name(),
			reason = reason.slug(),
			"burst refused: a stream is running"
		);
		publish_now(&mqtt, &prefix, handle.name(), reason, Stop::Stream, 0, 0).await;
		return;
	}

	// Hold the wake: without this lock, `idle_disconnect` would give the camera
	// back while we are reading, and the burst would lose its own session.
	let _lock = handle.wake_lock().acquire();

	let Some(bc) = await_session(&handle).await else {
		tracing::info!(
			camera = %handle.name(),
			reason = reason.slug(),
			salle = ?SESSION_WAIT,
			"burst without a Baichuan session: the camera would not be reached"
		);
		publish_now(&mqtt, &prefix, handle.name(), reason, Stop::NoSession, 0, 0).await;
		return;
	};

	// The families this camera has been refusing for [`FAMILY_MAX_REFUSALS`] wakes
	// do not come back to the budget: the [`COMMAND_BUDGET`] bound is too
	// precious for a door already closed three times.
	let guard = handle.burst();
	let possible = families_to_read(handle.name());
	let to_read: Vec<Family> = possible
		.iter()
		.copied()
		.filter(|f| !guard.family_saturated(*f))
		.collect();
	let ecartees = possible.len() - to_read.len();
	let plan = build_plan(&to_read, COMMAND_BUDGET);
	tracing::info!(
		camera = %handle.name(),
		reason = reason.slug(),
		families = plan.families.len(),
		skipped = plan.skipped,
		ecartees,
		commands = plan.commands,
		budget = COMMAND_BUDGET,
		"burst plan"
	);

	let mut read: Vec<(Family, Verdict)> = Vec::new();
	let mut commands = 0usize;
	let tournee = tokio::time::timeout(
		TIME_BUDGET,
		burst(&handle, &bc, &publisher, &plan, &mut read, &mut commands),
	)
	.await;
	let mut stop = match tournee {
		Ok(stop) => stop,
		Err(_) => Stop::Duration,
	};
	if stop == Stop::Done && plan.skipped > 0 {
		stop = Stop::Budget;
	}

	// The only write of the batch memory — one call, one write.
	handle.mark_families_read(&read).await;

	tracing::info!(
		camera = %handle.name(),
		reason = reason.slug(),
		state = stop.slug(),
		commands,
		budget = COMMAND_BUDGET,
		families = read.len(),
		writable = read.iter().filter(|(_, v)| *v == Verdict::Writable).count(),
		passageres = read.iter().filter(|(_, v)| *v == Verdict::Transient).count(),
		"wake burst finished: session returned"
	);
	publish_now(&mqtt, &prefix, handle.name(), reason, stop, commands, read.len()).await;
	// `_lock` and `_end` are released here: the camera can fall asleep again.
}

/// The read tour itself: the battery, then the families of the plan.
///
/// Stops at the first refusal, at the bound, or on cancellation. The command
/// counter is a parameter and not a return value, as for
/// [`crate::settings::sync_all`]: when the tour is carried off by
/// [`TIME_BUDGET`], what has already been paid must stay visible in the log.
async fn burst(
	handle: &CameraHandle,
	bc: &BcCamera,
	publ: &StatusPublisher<'_>,
	plan: &BurstPlan,
	read: &mut Vec<(Family, Verdict)>,
	commands: &mut usize,
) -> Stop {
	let guard = handle.burst();

	// The battery first: that is frame 253, the one that does not wake the
	// camera, and the reason the burst exists.
	if guard.battery_depleted() {
		tracing::debug!(
			camera = %handle.name(),
			refused = BATTERY_MAX_REFUSALS,
			"burst without frame 253: the battery has not answered for enough wakes to stop paying for it"
		);
	} else {
		let issu = bc.battery_info().await;
		if let Ok(infos) = &issu {
			let niveau = infos.battery_percent.min(100) as u8;
			if let Err(e) = publ.publish_battery_level(niveau).await {
				tracing::debug!(camera = %handle.name(), error = %e, "publishing the battery level failed");
			}
			handle.status_cache().set_battery_level(niveau);
		}
		guard.note_battery(issu.is_ok());
		let (family, verdict) = settings::battery_read(publ, &issu).await;
		read.push((family, verdict));
		*commands += 1;
		// A `400` on frame 253 must not switch the tour off either:
		// that is exactly the measured case where the battery refuses and the chime,
		// right behind it, would have answered. Frame 253's own "stop" already has
		// its guard-rail ([`BATTERY_MAX_REFUSALS`], counted by
		// [`BurstGuard::note_battery`]).
		if verdict == Verdict::Unknown {
			tracing::info!(
				camera = %handle.name(),
				family = family.slug(),
				"burst stopped on the battery: nothing answered, the session is returned"
			);
			return Stop::Refused;
		}
	}

	// Refusals **in a row in this tour**: reset to zero as soon as a family
	// answers. It is this counter that returns the session, not the per-family one.
	let mut refus_tournee = 0usize;
	for &family in &plan.families {
		let cost = cost(family);
		if *commands + cost > COMMAND_BUDGET {
			tracing::info!(
				camera = %handle.name(),
				family = family.slug(),
				commands = *commands,
				budget = COMMAND_BUDGET,
				"burst stopped by the command bound"
			);
			return Stop::Budget;
		}
		*commands += cost;
		if handle.cancel_token().is_cancelled() {
			return Stop::Cancelled;
		}
		let (family, verdict) = read_family(bc, publ, family).await;
		read.push((family, verdict));
		match verdict {
			// The camera said no: it is therefore alive and it answers. This
			// refusal says **nothing** about the next family, and this
			// doorbell's history is there to prove it — the tour used to stop at
			// four commands and the chime was never read again. One notes,
			// one continues, and one does not pass [`MAX_CONSECUTIVE_REFUSALS`].
			Verdict::Transient => {
				// Two counters, and not one: `refused` is that of **this
				// family** across wakes (it decides whether we keep paying for
				// it), `tour` is that of the tour in progress (it decides whether
				// we return the session). Confusing them would switch the tour off at the
				// first family, or let it run fifteen closed doors.
				let refused = guard.note_family_refused(family);
				refus_tournee += 1;
				tracing::info!(
					camera = %handle.name(),
					family = family.slug(),
					refused,
					plafond = FAMILY_MAX_REFUSALS,
					tournee = refus_tournee,
					"family refused: the burst continues, the next one is not condemned"
				);
				if refus_tournee >= MAX_CONSECUTIVE_REFUSALS {
					tracing::info!(
						camera = %handle.name(),
						family = family.slug(),
						plafond = MAX_CONSECUTIVE_REFUSALS,
						"burst returned after two consecutive refusals: the families read stay published"
					);
					return Stop::Refused;
				}
			}
			// Nothing answered: that is not a refusal, that is an absence. Holding
			// the session can no longer bring anything back.
			Verdict::Unknown => {
				tracing::info!(
					camera = %handle.name(),
					family = family.slug(),
					"burst stopped on an impossible attempt: the families already read stay published, the session is returned"
				);
				return Stop::Refused;
			}
			// The family answered: it is no longer in refusal debt, and the
			// refusal streak of the tour is broken.
			other => {
				guard.note_family_read(family);
				refus_tournee = 0;
				let _ = other;
			}
		}
	}

	Stop::Done
}

/// Wait for this camera's Baichuan session to be there.
///
/// No command is emitted here: it is the camera's connection loop that
/// establishes the session, held by the lock the burst (and the wake's hold)
/// owns. The probe is bounded by [`SESSION_WAIT`] and cut by
/// the bridge's stop.
///
/// `pub(crate)` because [`crate::polling`] waits for the same session for
/// the same reason: a lock taken without a session obtained is useless.
pub(crate) async fn await_session(handle: &CameraHandle) -> Option<Arc<BcCamera>> {
	let deadline = tokio::time::Instant::now() + SESSION_WAIT;
	loop {
		if let Some(bc) = handle.bc_camera_concrete() {
			return Some(bc);
		}
		if handle.cancel_token().is_cancelled() {
			return None;
		}
		let now = tokio::time::Instant::now();
		if now >= deadline {
			return None;
		}
		let remaining = deadline - now;
		tokio::select! {
			_ = handle.cancel_token().cancelled() => return None,
			_ = tokio::time::sleep(remaining.min(SESSION_POLL)) => {}
		}
	}
}

/// Publish the burst state: `status/<cam>/status/burst`, retained.
///
/// The topic has no Home Assistant discovery entity — deliberately: it is
/// a diagnostic topic, and discovery would publish an entity the
/// operator did not ask for. The detailed reason, family by family, is
/// already published on `status/<family>/reason` by [`crate::settings`].
async fn publish_now(
	mqtt: &SharedMqttClient,
	prefix: &str,
	camera: &str,
	reason: Wake,
	state: Stop,
	commands: usize,
	families: usize,
) {
	let topic = format!("{prefix}/{camera}/status/burst");
	let payload = format!(
		r#"{{"reason":"{reason}","state":"{state}","commands":{commands},"budget":{budget},"families":{families}}}"#,
		reason = reason.slug(),
		state = state.slug(),
		commands = commands,
		budget = COMMAND_BUDGET,
		families = families,
	);
	if let Err(e) = mqtt.publish_retained(&topic, payload.as_bytes()).await {
		tracing::debug!(camera = %camera, error = %e, "publishing the burst state failed");
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;
	use bairelay_mqtt::test_support::mock_client;
	use bairelay_neolink_core::bc::model::{
		BcXml, MSG_ID_BATTERY_INFO, MSG_ID_GET_MOTION_DETECT, MSG_ID_GET_PRIVACY_MASK,
	};
	use bairelay_neolink_core::bc_protocol::connection::mock::{
		reply_200_xml, reply_err_code, MockConnection,
	};
	use bairelay_neolink_core::bc_protocol::BcCamera;
	use std::sync::{Arc, Mutex};
	use tokio_util::sync::CancellationToken;

	/// A doorbell whose battery answers 63 %.
	fn battery(answer: u32) -> BcXml {
		BcXml {
			battery_info: Some(bairelay_neolink_core::bc::xml::BatteryInfo {
				battery_percent: answer,
				charge_status: "none".to_string(),
				..Default::default()
			}),
			..Default::default()
		}
	}

	/// The same harness as `mqtt_dispatch::tests::on_worker`: a full `BcXml`
	/// runs to several kilobytes and every camera command moves at least
	/// two copies of it, so the body runs on a thread with an explicit stack.
	type Body = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

	fn on_worker(body: fn() -> Body) {
		let pile = if cfg!(debug_assertions) {
			8 * 1024 * 1024
		} else {
			2 * 1024 * 1024
		};
		std::thread::Builder::new()
			.stack_size(pile)
			.spawn(move || {
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.start_paused(true)
					.build()
					.expect("test thread runtime");
				rt.block_on(body());
			})
			.expect("test thread with an explicit stack")
			.join()
			.expect("the burst must not panic");
	}

	/// Rule 1, and the heart of the batch: two wakes, one single burst.
	#[test]
	fn a_wake_triggers_a_single_burst() {
		on_worker(|| Box::pin(a_single_burst()));
	}

	async fn a_single_burst() {
		let trace = Arc::new(Mutex::new(Vec::new()));
		let trace_cmd = Arc::clone(&trace);
		// Two scripted commands more than needed: if a second burst
		// left, it would pay them and the trace would grow.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(move |req| {
				trace_cmd.lock().unwrap().push(req.meta.msg_id);
				reply_200_xml(req, battery(63))
			})
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(|req| reply_200_xml(req, battery(63)))
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(|req| reply_200_xml(req, battery(63)))
			.build()
			.await;
		let (mqtt, capture) = mock_client();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-burst"),
			CancellationToken::new(),
			Some(mqtt),
		));
		handle.set_bc_camera_for_test(Arc::new(BcCamera::from_mock_connection(mock).await));

		let first = trigger(Arc::clone(&handle), Wake::Press).expect("the first burst starts");
		// The second wake of the same wake: the webhook alarm that follows the press.
		let second = trigger(Arc::clone(&handle), Wake::Motion);
		assert!(second.is_none(), "a second wake inside the window pays no burst");
		first.await.expect("the burst did not panic");

		assert_eq!(
			*trace.lock().unwrap(),
			vec![MSG_ID_BATTERY_INFO],
			"one burst = one frame 253, and nothing else"
		);
		let state = capture
			.published_payloads()
			.into_iter()
			.find(|(t, _)| t == "bairelay/cam-burst/status/burst")
			.expect("the burst state is published");
		let state = String::from_utf8_lossy(&state.1).into_owned();
		assert!(state.starts_with("{\"reason\":\"press\""), "{state}");
		assert!(state.contains("\"commands\":1"), "{state}");
		assert!(!state.contains("\"state\":\"refus\""), "{state}");
	}

	/// No wake = not one command. That is the property the battery
	/// asks for: without it, the bridge is just one more poller.
	#[test]
	fn no_wake_sends_no_command() {
		on_worker(|| Box::pin(no_wake()));
	}

	async fn no_wake() {
		let trace = Arc::new(Mutex::new(Vec::new()));
		let trace_cmd = Arc::clone(&trace);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(move |req| {
				trace_cmd.lock().unwrap().push(req.meta.msg_id);
				reply_200_xml(req, battery(63))
			})
			.build()
			.await;
		let (mqtt, _capture) = mock_client();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-repos"),
			CancellationToken::new(),
			Some(mqtt),
		));
		handle.set_bc_camera_for_test(Arc::new(BcCamera::from_mock_connection(mock).await));

		// Time to let even one task leave: nothing leaves.
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert!(trace.lock().unwrap().is_empty(), "no wake, no camera command");
		assert_eq!(handle.wake_lock().count(), 0, "without a burst, nothing holds the wake");
	}

	/// The `press` must be **published** before the burst's first command,
	/// otherwise the operator's automation discovers the press with the delay of
	/// the whole read.
	#[test]
	fn the_press_starts_before_the_first_command() {
		on_worker(|| Box::pin(press_first()));
	}

	async fn press_first() {
		let vus_a_la_commande: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
		let (mqtt, capture) = mock_client();
		let capture_a_la_commande = capture.clone();
		let trace_cmd = Arc::clone(&vus_a_la_commande);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(move |req| {
				// The exact instant the burst's first command leaves: the
				// press must already be on the bus.
				trace_cmd
					.lock()
					.unwrap()
					.extend(capture_a_la_commande.published_topics());
				reply_200_xml(req, battery(63))
			})
			.build()
			.await;
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-appui"),
			CancellationToken::new(),
			Some(mqtt),
		));
		handle.set_bc_camera_for_test(Arc::new(BcCamera::from_mock_connection(mock).await));

		// The production path: the press report publishes the press, **then**
		// declares the wake.
		let publisher = StatusPublisher::new(
			handle.mqtt_client().expect("this harness has an MQTT client"),
			handle.topic_prefix(),
			handle.name(),
		);
		crate::camera_tasks::report_doorbell_press(
			"cam-appui",
			&publisher,
			&handle.status_cache(),
			Some(&handle),
		)
		.await;
		// The burst is detached by construction: let it finish. Time
		// is virtual (`start_paused`), this wait costs nothing in real time.
		tokio::time::sleep(Duration::from_secs(2)).await;

		let topics = capture.published_topics();
		let position_press = topics
			.iter()
			.position(|t| t == "bairelay/cam-appui/status/doorbell")
			.expect("the press is published on the ring topic");
		let position_state = topics
			.iter()
			.position(|t| t == "bairelay/cam-appui/status/burst")
			.expect("the burst state is published at the end of the burst");
		assert!(
			position_press < position_state,
			"the press before the end of the burst; topics: {topics:?}"
		);
		let seen = vus_a_la_commande.lock().unwrap();
		assert!(
			seen.iter().any(|t| t == "bairelay/cam-appui/status/doorbell"),
			"at the burst's first command the press was already published; observed: {seen:?}"
		);
	}

	/// A family that refuses does not condemn the tour; two refusals in a row
	/// return the session, and the reasons are published.
	#[test]
	fn an_interrupted_burst_publishes_and_releases_the_session() {
		on_worker(|| Box::pin(burst_interrupted()));
	}

	async fn burst_interrupted() {
		// The battery answers, then **two** families are refused with a `400` —
		// the code this dialect reads "not now", not "not that".
		// `read_family` attempts two per family: **all four are
		// scripted**, otherwise the mock answers nothing any more and `raw_get`,
		// which has no deadline of its own and must not have one, would wait here
		// until the test ends.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(|req| reply_200_xml(req, battery(20)))
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_PRIVACY_MASK)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_PRIVACY_MASK)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let (mqtt, capture) = mock_client();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-maussade"),
			CancellationToken::new(),
			Some(mqtt),
		));
		handle.set_bc_camera_for_test(Arc::new(BcCamera::from_mock_connection(mock).await));

		let plan = BurstPlan {
			families: vec![Family::Motion, Family::Mask],
			commands: 2,
			skipped: 0,
		};
		let (stop, commands) = {
			let publisher = StatusPublisher::new(
				handle.mqtt_client().expect("harness with MQTT"),
				handle.topic_prefix(),
				handle.name(),
			);
			let bc = handle.bc_camera_concrete().expect("scripted session");
			let mut read = Vec::new();
			let mut paid = 0usize;
			let fin = burst(&handle, &bc, &publisher, &plan, &mut read, &mut paid).await;
			// Both families must have been gone through: the first one refused
			// did not switch the second one off. That is the 13 November correction, measured
			// on the real doorbell — `image` in `400` kills the chime there.
			assert!(read
				.iter()
				.any(|(f, v)| *f == Family::Motion && *v == Verdict::Transient));
			assert!(
				read.iter()
					.any(|(f, v)| *f == Family::Mask && *v == Verdict::Transient),
				"the family after a `400` is attempted anyway; observed: {read:?}"
			);
			(fin, paid)
		};
		assert_eq!(
			stop,
			Stop::Refused,
			"two consecutive refusals return the session ([MAX_CONSECUTIVE_REFUSALS])"
		);
		assert_eq!(
			commands, 3,
			"frame 253 (1) + the two refused families (1 + 1): both refused reads were paid for, so attempted"
		);
		let topics = capture.published_topics();
		assert!(
			topics.iter().any(|t| t.ends_with("status/motion/reason")),
			"the reason of the first refused family is published; observed: {topics:?}"
		);
		assert!(
			topics.iter().any(|t| t.ends_with("status/mask/reason")),
			"the reason of the second one too, since its read was attempted; observed: {topics:?}"
		);

		// And the session returned. `trigger` is the only path that takes the
		// wake lock — so it is the one tested for the restitution,
		// with a fresh session: the script above is exhausted, and an
		// exhausted mock answers nothing, which would teach nothing.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_BATTERY_INFO)
			.reply_with(|req| reply_200_xml(req, battery(35)))
			.build()
			.await;
		handle.set_bc_camera_for_test(Arc::new(BcCamera::from_mock_connection(mock).await));
		let task = trigger(Arc::clone(&handle), Wake::Push).expect("burst allowed");
		task.await.expect("no panic");
		assert_eq!(
			handle.wake_lock().count(),
			0,
			"a burst that ended, even interrupted, returned the wake lock"
		);
		assert!(!handle.burst().in_flight(), "the guard is released");
		assert!(
			capture.published_topics().iter().any(|t| t.ends_with("status/burst")),
			"the detached burst published its state: {:?}",
			capture.published_topics()
		);
	}

	/// The guard refuses the second burst inside the window and allows it after.
	#[test]
	fn the_guard_holds_the_window() {
		let guard = BurstGuard::new(Duration::from_millis(500));
		let start = Instant::now();
		assert!(guard.take(start));
		assert!(!guard.take(start + Duration::from_millis(10)), "the window is running");
		guard.release_guard();
		assert!(
			!guard.take(start + Duration::from_millis(60)),
			"a burst finished inside the window restarts nothing"
		);
		assert!(guard.take(start + Duration::from_millis(600)), "window past");
	}

	/// A family the camera has been refusing for [`FAMILY_MAX_REFUSALS`] wakes no
	/// longer comes back to the budget; a family that answers clears its debt.
	///
	/// That is the half that protects the battery: without it, "continue after a
	/// `400`" would mean buying four closed doors at every press, every
	/// day, on a battery doorbell.
	#[test]
	fn a_family_refused_three_times_stays_out_of_the_plan() {
		let guard = BurstGuard::new(Duration::from_millis(500));
		assert!(!guard.family_saturated(Family::Image), "fresh, it is in the plan");
		assert_eq!(guard.note_family_refused(Family::Image), 1);
		assert_eq!(guard.note_family_refused(Family::Image), 2);
		assert!(!guard.family_saturated(Family::Image), "two refusals, it is still in");
		assert_eq!(guard.note_family_refused(Family::Image), FAMILY_MAX_REFUSALS);
		assert!(guard.family_saturated(Family::Image), "three refusals in a row, stop");

		// The other families have nothing to do with it: `image` refuses and `chime`
		// still shows up at the next wake.
		assert!(!guard.family_saturated(Family::Chime), "one family does not drag the others");

		// And a success reopens the door: the debt is cleared.
		guard.note_family_read(Family::Image);
		assert!(!guard.family_saturated(Family::Image), "a family that answered starts over at zero");

		// The plan does not pay for it any more: with the budget counted, `image` (1) and
		// `chime` (5) on a bound of 6, minus `image` set aside, leaves `chime`'s 5.
		let guard = BurstGuard::new(Duration::from_millis(500));
		for _ in 0..FAMILY_MAX_REFUSALS {
			guard.note_family_refused(Family::Image);
		}
		let possibles = [Family::Image, Family::Chime];
		let to_read: Vec<Family> = possibles
			.iter()
			.copied()
			.filter(|f| !guard.family_saturated(*f))
			.collect();
		assert_eq!(to_read, vec![Family::Chime], "the saturated family does not come back to the budget");
		assert_eq!(build_plan(&to_read, 6).families, vec![Family::Chime]);
	}

	/// The measured rank comes before the cost. The real window holding four orders
	/// out of eight, `motion` and `osd` must already be read when the camera returns the
	/// session, and `image` — which answers an unknown block and erases its values —
	/// must be the last to leave, or the first to be left on the platform.
	#[test]
	fn motion_and_osd_pass_before_masks_and_image_after() {
		let entries = [
			Family::Audio,
			Family::Image,
			Family::Ntp,
			Family::Osd,
			Family::Motion,
			Family::Chime,
		];
		let plan = build_plan(&entries, COMMAND_BUDGET);
		let head: Vec<&str> = plan.families.iter().take(2).map(|f| f.slug()).collect();
		assert_eq!(head, vec!["osd", "motion"], "the two families that are of use enter first");
		assert_eq!(
			*plan.families.last().expect("the plan is not empty"),
			Family::Image,
			"`image` brings up the rear"
		);
		assert!(plan.commands <= COMMAND_BUDGET, "{} commands", plan.commands);

		// A narrower window: it is still `image` left on the platform, and
		// not one of the two first ones.
		let plan = build_plan(&entries, 4);
		assert!(
			!plan.families.contains(&Family::Image),
			"{:?} — the useless family is set aside before the others",
			plan.families
		);
		assert_eq!(plan.families.len(), 4, "the four that pay off fit: {:?}", plan.families);
	}

	/// The bound is **high**: no plan exceeds it, and an expensive family at the
	/// front does not starve the others.
	#[test]
	fn the_plan_never_exceeds_the_bound() {
		let toutes: Vec<Family> = Family::ALL.to_vec();
		let plan = build_plan(&toutes, COMMAND_BUDGET);
		assert!(
			plan.commands <= COMMAND_BUDGET,
			"{} commands for a bound of {}",
			plan.commands,
			COMMAND_BUDGET
		);
		assert!(plan.skipped > 0, "sixteen families cost more than the bound");

		// `chime` (5) in front of one-command families: the plan pays them from
		// the cheapest to the dearest, so the small ones go first.
		let plan = build_plan(&[Family::Chime, Family::Audio, Family::Pir], 7);
		assert_eq!(
			plan.families,
			vec![Family::Audio, Family::Pir, Family::Chime],
			"1 + 1 + 5 = 7, the bound is reached without being exceeded"
		);
		let plan = build_plan(&[Family::Chime, Family::Audio, Family::Pir], 6);
		assert_eq!(
			plan.families,
			vec![Family::Audio, Family::Pir],
			"the expensive family is skipped, not the whole window"
		);
		assert_eq!(plan.skipped, 1);

		// `ai` costs nothing: it stays in the plan, and its reason with it.
		let plan = build_plan(&[Family::Ai], 0);
		assert_eq!(plan.families, vec![Family::Ai], "a family with no command does not eat the bound");
	}

	#[test]
	fn a_camera_nothing_has_proved_leaves_everything_to_ask() {
		let nothing = unproven_from(&[]);
		assert!(nothing.contains(&Family::Audio), "audio was never proved, so it is asked");
		assert!(nothing.contains(&Family::Chime), "the chime was never proved either");
		assert_eq!(
			nothing.len(),
			Family::ALL.len() - 2,
			"everything but the battery and ai: neither can be proved by a read"
		);
	}

	#[test]
	fn a_fully_proven_camera_leaves_nothing_to_ask() {
		// The ordinary restart. Proving this list empty is what makes the startup read
		// cost zero commands on a bridge that has already met its camera.
		let proven: Vec<Family> = Family::ALL.into_iter().collect();
		assert!(unproven_from(&proven).is_empty(), "nothing left to prove, nothing to spend");
	}

	#[test]
	fn the_startup_list_never_carries_battery_or_ai() {
		let nothing = unproven_from(&[]);
		assert!(!nothing.contains(&Family::Battery), "the battery is read outside the plan");
		assert!(!nothing.contains(&Family::Ai), "no read can make ai writable");
		let plan = build_plan(&nothing, COMMAND_BUDGET);
		assert!(plan.commands <= COMMAND_BUDGET, "the bound holds at startup too");
	}

	/// A camera without MQTT starts nothing: nobody is waiting for the read.
	#[test]
	fn without_mqtt_the_burst_does_not_start() {
		on_worker(|| Box::pin(without_mqtt()));
	}

	async fn without_mqtt() {
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-no-broker"),
			CancellationToken::new(),
			None,
		));
		let t = trigger(Arc::clone(&handle), Wake::Press).expect("the guard opens all the same");
		t.await.expect("no panic");
		assert_eq!(handle.wake_lock().count(), 0);
	}
}
