//! Classification of cmd 33 `<AlarmEventList>` pushes.
//!
//! Each push carries a comma-separated `<status>` list and an optional
//! comma-separated `<AItype>` list. [`classify_alarm`] turns one
//! `<AlarmEvent>` into an [`AlarmUpdate`]: a motion transition, the set
//! of AI object classes in view, and the doorbell's `visitor` state.
//! Tokens are trimmed and compared case-insensitively; empty tokens
//! (e.g. from a trailing comma) are ignored.
//!
//! A doorbell holds `visitor` in every push for seconds after one
//! button press; `VisitorEdge` turns that state into presses.

use super::MotionStatus;
use crate::bc::xml::AlarmEvent;
use std::time::Instant;

/// `<status>` token the doorbell holds for seconds after a button press.
const STATUS_VISITOR: &str = "visitor";
/// `<status>` / `<AItype>` token for "nothing active".
const TOKEN_NONE: &str = "none";
/// `<status>` tokens that carry no motion information on their own:
/// the doorbell's `visitor` state and the day/night switch notifications.
const STATUS_NON_MOTION: [&str; 3] = [STATUS_VISITOR, "dn", "daynight"];

/// An object class reported by the camera's AI detector in `<AItype>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AiObject {
	/// `people`, `person` or `human`.
	Person,
	/// `vehicle` or `car`.
	Vehicle,
	/// `dog_cat`, `dog`, `cat` or `pet`.
	Animal,
	/// `face`.
	Face,
	/// `package`.
	Package,
}

impl AiObject {
	/// Every object class, in a stable order.
	pub const ALL: [AiObject; 5] = [
		AiObject::Person,
		AiObject::Vehicle,
		AiObject::Animal,
		AiObject::Face,
		AiObject::Package,
	];

	/// Map one normalised (trimmed, lowercase) `<AItype>` token to its
	/// object class. `none`, `other` and unrecognised tokens map to
	/// `None`.
	fn from_token(token: &str) -> Option<Self> {
		match token {
			"people" | "person" | "human" => Some(AiObject::Person),
			"vehicle" | "car" => Some(AiObject::Vehicle),
			"dog_cat" | "dog" | "cat" | "pet" => Some(AiObject::Animal),
			"face" => Some(AiObject::Face),
			"package" => Some(AiObject::Package),
			_ => None,
		}
	}
}

/// The set of AI object classes active in one alarm push.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiObjects {
	/// A person is in view.
	pub person: bool,
	/// A vehicle is in view.
	pub vehicle: bool,
	/// An animal is in view.
	pub animal: bool,
	/// A face is in view.
	pub face: bool,
	/// A package is in view.
	pub package: bool,
}

impl AiObjects {
	/// Whether `object` is in the set.
	pub fn contains(&self, object: AiObject) -> bool {
		match object {
			AiObject::Person => self.person,
			AiObject::Vehicle => self.vehicle,
			AiObject::Animal => self.animal,
			AiObject::Face => self.face,
			AiObject::Package => self.package,
		}
	}

	/// Add `object` to the set.
	pub fn insert(&mut self, object: AiObject) {
		let flag = match object {
			AiObject::Person => &mut self.person,
			AiObject::Vehicle => &mut self.vehicle,
			AiObject::Animal => &mut self.animal,
			AiObject::Face => &mut self.face,
			AiObject::Package => &mut self.package,
		};
		*flag = true;
	}

	/// Whether no object class is in the set.
	pub fn is_empty(&self) -> bool {
		*self == Self::default()
	}
}

impl FromIterator<AiObject> for AiObjects {
	fn from_iter<I: IntoIterator<Item = AiObject>>(iter: I) -> Self {
		let mut set = Self::default();
		for object in iter {
			set.insert(object);
		}
		set
	}
}

/// Everything one cmd 33 alarm push reports for a channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlarmUpdate {
	/// Motion transition: `Start`, `Stop`, or `NoChange` when the push
	/// carries no motion information.
	pub motion: MotionStatus,
	/// AI object classes in view. `None` leaves the previous set
	/// unchanged; `Some` replaces it (an empty set clears every class).
	pub objects: Option<AiObjects>,
	/// The push reports the doorbell's `visitor` state. `None` (no event
	/// for the channel) leaves the previous state unchanged.
	pub visitor: Option<bool>,
	/// The doorbell button was pressed: the rising edge of `visitor`,
	/// set by a caller that tracks `visitor` across pushes with a
	/// [`VisitorEdge`]. [`classify_alarm`] and `listen_on_motion` leave
	/// it `false`.
	pub doorbell_press: bool,
}

impl AlarmUpdate {
	/// An update that changes nothing.
	pub fn no_change(at: Instant) -> Self {
		Self::from(MotionStatus::NoChange(at))
	}
}

impl From<MotionStatus> for AlarmUpdate {
	/// A motion-only update: objects and visitor state unchanged, no
	/// doorbell press.
	fn from(motion: MotionStatus) -> Self {
		Self {
			motion,
			objects: None,
			visitor: None,
			doorbell_press: false,
		}
	}
}

/// Split a comma-separated wire list into trimmed, lowercase,
/// non-empty tokens.
fn tokens(list: &str) -> Vec<String> {
	list.split(',')
		.map(|t| t.trim().to_ascii_lowercase())
		.filter(|t| !t.is_empty())
		.collect()
}

/// Classify one `<AlarmEvent>` from its `<status>` and `<AItype>` lists.
///
/// - `visitor` is `Some(true)` when `status` contains `visitor`, else
///   `Some(false)`; `doorbell_press` stays `false`.
/// - A push whose `status` holds only `visitor` / `dn` / `daynight`
///   tokens and no active `AItype` changes neither motion nor objects.
/// - Otherwise motion is `Start` when `status` holds any token besides
///   `none` / `visitor` / `dn` / `daynight`, or any `AItype` token is
///   other than `none`; else `Stop`. Objects are the mapped `AItype`
///   classes, possibly empty.
pub fn classify_alarm(status: &str, ai_type: Option<&str>, at: Instant) -> AlarmUpdate {
	let status_tokens = tokens(status);
	let ai_tokens = ai_type.map(tokens).unwrap_or_default();
	let is_non_motion = |t: &String| STATUS_NON_MOTION.contains(&t.as_str());

	let visitor = Some(status_tokens.iter().any(|t| t == STATUS_VISITOR));
	let ai_active = ai_tokens.iter().any(|t| t != TOKEN_NONE);
	let non_motion_only = !status_tokens.is_empty() && status_tokens.iter().all(is_non_motion);
	if non_motion_only && !ai_active {
		return AlarmUpdate {
			visitor,
			..AlarmUpdate::no_change(at)
		};
	}

	let motion_token = status_tokens
		.iter()
		.any(|t| t != TOKEN_NONE && !is_non_motion(t));
	AlarmUpdate {
		motion: if motion_token || ai_active {
			MotionStatus::Start(at)
		} else {
			MotionStatus::Stop(at)
		},
		objects: Some(
			ai_tokens
				.iter()
				.filter_map(|t| AiObject::from_token(t))
				.collect(),
		),
		visitor,
		doorbell_press: false,
	}
}

/// Classify the first event in `events` whose channel matches
/// `channel_id`. No matching event yields [`AlarmUpdate::no_change`].
pub fn classify_alarm_events(events: &[AlarmEvent], channel_id: u8, at: Instant) -> AlarmUpdate {
	events
		.iter()
		.find(|e| e.channel_id == channel_id)
		.map(|e| classify_alarm(&e.status, e.ai_type.as_deref(), at))
		.unwrap_or_else(|| AlarmUpdate::no_change(at))
}

/// Doorbell press detection over a stream of [`AlarmUpdate`]s.
///
/// A press is the rising edge of `visitor`: a push reporting `visitor`
/// after one without it, or as the first push seen. Updates with
/// `visitor: None` leave the state unchanged. Starts with no visitor,
/// or with it held ([`VisitorEdge::held`]).
#[derive(Debug, Default)]
pub struct VisitorEdge {
	visitor: bool,
}

impl VisitorEdge {
	/// An edge that starts with the visitor state held, for a press
	/// already reported elsewhere: the first `visitor` push is no
	/// press, and one after a push without `visitor` is.
	pub fn held() -> Self {
		Self { visitor: true }
	}

	/// `update` with `doorbell_press` set when it is a rising edge.
	pub fn apply(&mut self, update: AlarmUpdate) -> AlarmUpdate {
		let Some(visitor) = update.visitor else {
			return update;
		};
		let doorbell_press = visitor && !self.visitor;
		self.visitor = visitor;
		AlarmUpdate {
			doorbell_press,
			..update
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Shorthand for an `AiObjects` set built from a list.
	fn set(objects: &[AiObject]) -> AiObjects {
		objects.iter().copied().collect()
	}

	/// Expected classification, timestamp-free.
	#[derive(Debug, PartialEq)]
	enum Motion {
		Start,
		Stop,
		NoChange,
	}

	fn motion_of(status: MotionStatus) -> Motion {
		match status {
			MotionStatus::Start(_) => Motion::Start,
			MotionStatus::Stop(_) => Motion::Stop,
			MotionStatus::NoChange(_) => Motion::NoChange,
		}
	}

	/// (status, AItype, motion, objects, visitor)
	type Case = (
		&'static str,
		Option<&'static str>,
		Motion,
		Option<AiObjects>,
		bool,
	);

	#[test]
	fn classify_alarm_table() {
		use AiObject::*;
		let empty = Some(AiObjects::default());
		let cases: &[Case] = &[
			// Plain motion / idle.
			("MD", None, Motion::Start, empty, false),
			("none", None, Motion::Stop, empty, false),
			("none", Some("none"), Motion::Stop, empty, false),
			("MD", Some("none"), Motion::Start, empty, false),
			// Any non-`none` status token counts as motion.
			("PIR", None, Motion::Start, empty, false),
			("somethingnew", None, Motion::Start, empty, false),
			// `visitor` alone: no motion / object change.
			("visitor", None, Motion::NoChange, None, true),
			("visitor", Some("none"), Motion::NoChange, None, true),
			// Day/night switch alone: nothing.
			("dn", None, Motion::NoChange, None, false),
			("daynight", Some("none"), Motion::NoChange, None, false),
			("dn,visitor", None, Motion::NoChange, None, true),
			// Press alongside motion.
			("visitor,MD", None, Motion::Start, empty, true),
			(
				"MD,visitor",
				Some("people"),
				Motion::Start,
				set(&[Person]).into(),
				true,
			),
			// Non-motion status with an active AItype → motion.
			(
				"visitor",
				Some("people"),
				Motion::Start,
				set(&[Person]).into(),
				true,
			),
			(
				"dn",
				Some("vehicle"),
				Motion::Start,
				set(&[Vehicle]).into(),
				false,
			),
			// AItype mapping, including synonyms.
			(
				"MD",
				Some("people"),
				Motion::Start,
				set(&[Person]).into(),
				false,
			),
			(
				"MD",
				Some("person"),
				Motion::Start,
				set(&[Person]).into(),
				false,
			),
			(
				"MD",
				Some("human"),
				Motion::Start,
				set(&[Person]).into(),
				false,
			),
			(
				"MD",
				Some("vehicle"),
				Motion::Start,
				set(&[Vehicle]).into(),
				false,
			),
			(
				"MD",
				Some("car"),
				Motion::Start,
				set(&[Vehicle]).into(),
				false,
			),
			(
				"MD",
				Some("dog_cat"),
				Motion::Start,
				set(&[Animal]).into(),
				false,
			),
			(
				"MD",
				Some("dog"),
				Motion::Start,
				set(&[Animal]).into(),
				false,
			),
			(
				"MD",
				Some("cat"),
				Motion::Start,
				set(&[Animal]).into(),
				false,
			),
			(
				"MD",
				Some("pet"),
				Motion::Start,
				set(&[Animal]).into(),
				false,
			),
			(
				"MD",
				Some("face"),
				Motion::Start,
				set(&[Face]).into(),
				false,
			),
			(
				"MD",
				Some("package"),
				Motion::Start,
				set(&[Package]).into(),
				false,
			),
			// Comma lists.
			(
				"MD",
				Some("people,vehicle,dog_cat"),
				Motion::Start,
				set(&[Person, Vehicle, Animal]).into(),
				false,
			),
			(
				"MD",
				Some("face,package,none"),
				Motion::Start,
				set(&[Face, Package]).into(),
				false,
			),
			// Whitespace + case.
			(
				" md , VISITOR ",
				Some(" People , CAR "),
				Motion::Start,
				set(&[Person, Vehicle]).into(),
				true,
			),
			("NONE", Some("NONE"), Motion::Stop, empty, false),
			("Visitor", None, Motion::NoChange, None, true),
			("DayNight", None, Motion::NoChange, None, false),
			// Empty tokens from stray commas are ignored.
			(
				"MD,",
				Some("people,"),
				Motion::Start,
				set(&[Person]).into(),
				false,
			),
			("visitor,", None, Motion::NoChange, None, true),
			// Unknown AItype tokens: active (motion) but no object.
			("MD", Some("other"), Motion::Start, empty, false),
			("none", Some("unicorn"), Motion::Start, empty, false),
			// Wired-power doorbell motion: status none + AItype other.
			("none", Some("other"), Motion::Start, empty, false),
			// `none` mixed with `visitor` is not a non-motion-only push.
			("none,visitor", None, Motion::Stop, empty, true),
			// Empty status: no motion tokens, nothing active.
			("", None, Motion::Stop, empty, false),
		];
		let at = Instant::now();
		for (status, ai_type, motion, objects, visitor) in cases {
			let got = classify_alarm(status, *ai_type, at);
			let label = format!("status={status:?} AItype={ai_type:?}");
			assert_eq!(motion_of(got.motion), *motion, "motion for {label}");
			assert_eq!(got.objects, *objects, "objects for {label}");
			assert_eq!(got.visitor, Some(*visitor), "visitor for {label}");
			assert!(!got.doorbell_press, "press for {label}");
		}
	}

	#[test]
	fn classify_alarm_carries_timestamp() {
		let at = Instant::now();
		assert_eq!(
			classify_alarm("MD", None, at).motion,
			MotionStatus::Start(at)
		);
		assert_eq!(
			classify_alarm("none", None, at).motion,
			MotionStatus::Stop(at)
		);
		assert_eq!(
			classify_alarm("dn", None, at),
			AlarmUpdate {
				visitor: Some(false),
				..AlarmUpdate::no_change(at)
			}
		);
	}

	fn event(channel_id: u8, status: &str, ai_type: Option<&str>) -> AlarmEvent {
		AlarmEvent {
			channel_id,
			status: status.into(),
			ai_type: ai_type.map(Into::into),
			..Default::default()
		}
	}

	#[test]
	fn classify_alarm_events_uses_first_matching_channel() {
		let at = Instant::now();
		let events = [
			event(3, "MD", Some("people")),
			event(0, "visitor", None),
			event(0, "MD", None),
		];
		assert_eq!(
			classify_alarm_events(&events, 0, at),
			classify_alarm("visitor", None, at)
		);
		assert_eq!(
			classify_alarm_events(&events, 3, at),
			classify_alarm("MD", Some("people"), at)
		);
	}

	#[test]
	fn classify_alarm_events_without_match_is_no_change() {
		let at = Instant::now();
		assert_eq!(
			classify_alarm_events(&[event(7, "MD", None)], 0, at),
			AlarmUpdate::no_change(at)
		);
		assert_eq!(
			classify_alarm_events(&[], 0, at),
			AlarmUpdate::no_change(at)
		);
	}

	#[test]
	fn ai_objects_set_operations() {
		let mut s = AiObjects::default();
		assert!(s.is_empty());
		for object in AiObject::ALL {
			assert!(!s.contains(object));
			s.insert(object);
			assert!(s.contains(object));
		}
		assert!(!s.is_empty());
		assert_eq!(
			s,
			AiObjects {
				person: true,
				vehicle: true,
				animal: true,
				face: true,
				package: true,
			}
		);
	}

	#[test]
	fn alarm_update_from_motion_status_is_motion_only() {
		let at = Instant::now();
		let u = AlarmUpdate::from(MotionStatus::Start(at));
		assert_eq!(u.motion, MotionStatus::Start(at));
		assert_eq!(u.objects, None);
		assert_eq!(u.visitor, None);
		assert!(!u.doorbell_press);
	}

	/// `doorbell_press` after each of `statuses` (`None`: no event for
	/// the channel), fed through one [`VisitorEdge`].
	fn presses(statuses: &[Option<&str>]) -> Vec<bool> {
		let at = Instant::now();
		let mut edge = VisitorEdge::default();
		statuses
			.iter()
			.map(|status| {
				let update = match status {
					Some(status) => classify_alarm(status, None, at),
					None => AlarmUpdate::no_change(at),
				};
				edge.apply(update).doorbell_press
			})
			.collect()
	}

	#[test]
	fn visitor_edge_presses_on_rising_edge_only() {
		let cases: &[(&[Option<&str>], &[bool])] = &[
			// First push seen holding `visitor` is a press.
			(&[Some("visitor")], &[true]),
			// Held state: one press.
			(
				&[Some("MD,visitor"), Some("visitor"), Some("visitor")],
				&[true, false, false],
			),
			// Cleared, then held again: a second press.
			(
				&[Some("visitor"), Some("none"), Some("visitor")],
				&[true, false, true],
			),
			// Day/night push without `visitor` clears the state.
			(
				&[Some("visitor"), Some("dn"), Some("visitor")],
				&[true, false, true],
			),
			// No event for the channel leaves the state unchanged.
			(
				&[Some("visitor"), None, Some("visitor")],
				&[true, false, false],
			),
			(&[None, Some("visitor")], &[false, true]),
			// No `visitor` at all: no press.
			(&[Some("MD"), Some("none")], &[false, false]),
		];
		for (statuses, expected) in cases {
			assert_eq!(presses(statuses), *expected, "statuses {statuses:?}");
		}
	}

	#[test]
	fn held_visitor_edge_presses_only_after_a_clear() {
		let at = Instant::now();
		let mut edge = VisitorEdge::held();
		let got: Vec<bool> = ["visitor", "visitor", "none", "visitor"]
			.into_iter()
			.map(|status| edge.apply(classify_alarm(status, None, at)).doorbell_press)
			.collect();
		assert_eq!(got, [false, false, false, true]);
	}

	#[test]
	fn visitor_edge_keeps_the_rest_of_the_update() {
		let at = Instant::now();
		let update = classify_alarm("MD,visitor", Some("people"), at);
		let pressed = VisitorEdge::default().apply(update);
		assert!(pressed.doorbell_press);
		assert_eq!(
			pressed,
			AlarmUpdate {
				doorbell_press: true,
				..update
			}
		);
	}
}
