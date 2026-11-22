//! The other half of the rule: reason sentences and labels this crate publishes.
//!
//! Same incident as the root crate's guard — french reason sentences reaching Home
//! Assistant behind a message saying they were english. Discovery matters more than
//! the reasons do, because Home Assistant pins `entity_id` at first discovery: a
//! french label that ships once becomes a name somebody's automation references for
//! years. The spec table's own labels are asserted in `discovery::settings::tests`,
//! next to the data they come from; this module walks what is reachable from here.

#[cfg(test)]
mod tests {
	use crate::ai::AiObject;
	use crate::settings::Family;
	use crate::status::{REASON_MASKED, REASON_NO_DETAIL};
	use crate::topics::REASON_LEAF;

	/// French words that are not english words; see the root crate's guard for why the
	/// list stays homograph-free.
	const FRENCH: &[&str] = &[
		"la", "les", "des", "du", "une", "est", "dans", "pour", "sur", "par", "avec", "sans", "que", "qui", "pas",
		"plus", "tres", "tout", "toute", "autre", "chaque", "comme", "aussi", "donc", "mais", "seul", "seule",
		// The French words below are the guard's own data: they are the strings this test has
		// to catch in published text. They stay in the language they detect.
		"caméra", "refusé", "reglages", "réglages", "balise", "aucun", "aucune", "rien", "motif", "famille",
		"familles", "lecture", "écriture", "détail", "détails", "masqué", "publie", "dit", "nomme",
	];

	pub(crate) fn published_value_is_english(what: &str, value: &str) {
		assert!(value.is_ascii(), "{what} is not ascii on the wire: {value:?}");
		let lowered = value.to_lowercase();
			let bad: Vec<&str> = lowered
			.split(|c: char| !c.is_alphabetic() && c != '-')
			.filter(|w| FRENCH.contains(w))
			.collect();
		assert!(bad.is_empty(), "{what} would publish french to Home Assistant: {value:?} contains {bad:?}");
	}

	#[test]
	fn every_reason_sentence_of_this_crate_is_english() {
		published_value_is_english("topics::REASON_LEAF", REASON_LEAF);
		published_value_is_english("REASON_NO_DETAIL", REASON_NO_DETAIL);
		published_value_is_english("REASON_MASKED", REASON_MASKED);
	}

	#[test]
	fn every_ai_label_is_english() {
		for object in AiObject::ALL {
			published_value_is_english("AiObject::topic_name", object.topic_name());
			published_value_is_english("AiObject::display_name", object.display_name());
		}
	}

	#[test]
	fn every_family_slug_is_english() {
		for family in Family::ALL {
			published_value_is_english("Family::slug", family.slug());
		}
	}
}
