//! Nothing in french may ever reach the network again.
//!
//! This test exists because the defect it guards against already happened: nine
//! reason sentences were still french on the wire — `status/<family>/reason` was
//! The French sentence quoted below is the published payload this guard protects: it is
//! quoted because it is the thing under test, not because the comment forgot its language.
//! carrying `la caméra a refusé la commande (code 400)` to Home Assistant — behind a
//! commit message stating that published text had been switched to english. A
//! published french value is not cosmetic: the operator's automations compare those
//! sentences, and when they stop matching they fail for a reason nobody can see.
//! Review counts comment prose, not published values, so nothing caught it; this does,
//! and it does not rot.
//!
//! Scope: the values this crate hands to MQTT — reason sentences, the short words of
//! the burst state, the topic segment of every family. The discovery payload is
//! covered by the sibling guard in `bairelay_mqtt` (its own spec table lives there).
//! Test fixtures and the credential mask's own word list are not walked: the mask
//! looks for `mot de passe` on purpose, so that string is supposed to be french.

#[cfg(test)]
mod tests {
	/// The **second** renaming table in `docs/settings.md` lists the reason and error
	/// sentences — the text that goes out on `status/<family>/reason` and in the refusal
	/// payloads. Those are the strings that first travelled in french on the network, so
	/// this is the table that matters most, and until now it was compared with nothing.
	///
	/// The check lives in this crate, not in `bairelay-mqtt`, and for a stated reason: most
	/// of these sentences are produced here (`src/settings.rs` owns `REASON_READ_ONLY`,
	/// `REASON_NO_SESSION`, `REASON_NO_LOCK`, `REASON_TRANSIENT`, `REASON_NO_OSDCHANNEL`, the
	/// chime ones), and the two that live in the mqtt crate are `pub(crate)` constants that
	/// this test cannot see. Re-exporting a private constant for the convenience of a test
	/// would widen the crate's surface to feed its own test suite; reading the source files
	/// as text needs no visibility at all and covers every crate in one pass.
	///
	/// Three assertions:
	///
	/// * every `now` of the table is present, verbatim, in the sources — a row whose text is
	///   gone means the sentence was rewritten and the operator was left with the old one;
	/// * no `was` of the table is present anywhere — an unfinished rename;
	/// * every `REASON_*` constant in the reason-producing files is listed in the table, so a
	///   new published reason cannot appear without a row telling the operator about it.
	///
	/// Two facts about the corpus, because they decide whether the test is honest: the
	/// needles drop markdown backticks (the doc formats `<OsdChannelName>`, the code
	/// publishes it bare) and stop at `…` (rows whose sentence is long are shown truncated);
	/// and the corpus is built by concatenating the **string literals** of each file, because
	/// `REASON_NO_OSDCHANNEL` is a `concat!` of two literals and `REASON_TRANSIENT` is
	/// interpolated at the call site — searching raw source text would report a sentence that
	/// is published as "missing" purely because of how it is written down.
	#[test]
	fn the_reason_table_matches_the_sentences_the_bridge_publishes() {
		let doc = include_str!("../docs/settings.md");
		let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

		// the second `| was | now |` table: reasons and error sentences
		let mut tables: Vec<Vec<String>> = Vec::new();
		for l in doc.lines() {
			if l.trim() == "| was | now |" {
				tables.push(Vec::new());
				continue;
			}
			if let Some(cur) = tables.last_mut() {
				if l.trim().is_empty() {
					// a markdown table ends at the blank line: without this, the rows of
					// the *next* table (the vocabulary list, the topic list) leak in and
					// the test compares reasons against unrelated cells
					tables.push(Vec::new());
					continue;
				}
				if l.trim_start().starts_with("| ") && !l.trim_start().starts_with("| ---") {
					cur.push(l.to_string());
				}
			}
		}
		let reasons = tables
			.iter()
			.find(|t| t.len() >= 10 && t.iter().any(|r| r.contains("did not answer in time")))
			.expect("the reasons table (the one carrying 'the camera did not answer in time')");
		assert!(
			reasons.len() >= 10,
			"the reasons table was read as {:?} — the header changed or the section moved; \
			 check the parser before believing anything below",
			tables.iter().map(|t| t.len()).collect::<Vec<_>>()
		);

		// every string literal of every crate, in source order
		let mut corpus = String::new();
		let mut files = 0usize;
		for dir in ["src", "crates/mqtt/src", "crates/core/src"] {
			let mut stack = vec![root.join(dir)];
			while let Some(d) = stack.pop() {
				let Ok(rd) = std::fs::read_dir(&d) else { continue };
				for e in rd.flatten() {
					let p = e.path();
					if p.is_dir() {
						stack.push(p);
						continue;
					}
					if p.extension().and_then(|x| x.to_str()) != Some("rs") {
						continue;
					}
					let Ok(text) = std::fs::read_to_string(&p) else { continue };
					files += 1;
					let mut it = text.chars().peekable();
					while let Some(c) = it.next() {
						if c != '"' {
							continue;
						}
						let mut lit = String::new();
						while let Some(c) = it.next() {
							if c == '\\' {
								it.next();
								continue;
							}
							if c == '"' {
								break;
							}
							lit.push(c);
						}
						corpus.push_str(&lit);
						// no separator: REASON_NO_OSDCHANNEL is one sentence split by
						// concat!, and searching raw source text would call it missing
						// because of how it is written down
					}
				}
			}
		}
		assert!(files > 60, "only {files} source files read: the workspace moved?");

		let needle = |raw: &str| -> String {
			// a cell may *start* by quoting the exact text it means ("`value` (a note)"):
			// take that span. Only when it starts there — a cell that merely mentions
			// `<OsdChannelName>` in the middle of a sentence is spelling a tag, not
			// delimiting its own value, and treating the tag as the value made this test
			// report a still-published XML tag name as an unfinished rename
			let s = if let Some(rest) = raw.strip_prefix('`') {
				rest.split('`').next().unwrap_or(raw)
			} else {
				raw
			};
			let s = s.replace('`', "");
			let s = s.split('…').next().unwrap_or("").to_string();
			s.trim().trim_end_matches([':', ',', ';', '-', ' ']).to_string()
		};

		let mut problems: Vec<String> = Vec::new();
		let mut listed: Vec<String> = Vec::new();
		for row in reasons {
			let cells: Vec<&str> = row.split('|').map(|c| c.trim()).collect();
			if cells.len() < 4 {
				continue;
			}
			let (was, now) = (needle(cells[1]), needle(cells[2]));
			listed.push(was.clone());
			listed.push(now.clone());
			if !now.is_empty() && !corpus.contains(&now) {
				problems.push(format!(
					"the table promises {now:?} but no source file contains it: the sentence was \
					 rewritten and this table still shows the old one"
				));
			}
			if !was.is_empty() && corpus.contains(&was) {
				problems.push(format!(
					"the table says {was:?} is gone, but a source file still contains it: the \
					 rename was never finished"
				));
			}
		}

		// the other direction: every published reason constant must have a row
		let mut constants = 0usize;
		for dir in ["src", "crates/mqtt/src"] {
			let mut stack = vec![root.join(dir)];
			while let Some(d) = stack.pop() {
				let Ok(rd) = std::fs::read_dir(&d) else { continue };
				for e in rd.flatten() {
					let p = e.path();
					if p.is_dir() {
						stack.push(p);
						continue;
					}
					let Some(name) = p.file_name().and_then(|x| x.to_str()) else { continue };
					if !name.ends_with(".rs") {
						continue;
					}
					let Ok(text) = std::fs::read_to_string(&p) else { continue };
					for (i, line) in text.lines().enumerate() {
						let d = line.trim();
						// a declaration only: the doc comments and the scanning code in
						// this very file mention these names without declaring them
						let declared = d.starts_with("const REASON_")
							|| d.starts_with("pub(crate) const REASON_")
							|| d.starts_with("pub const REASON_");
						if !declared {
							continue;
						}
						// REASON_FORBIDDEN is input-side vocabulary: substrings that must
						// never leave the bridge, never published as a sentence
						if line.contains("REASON_FORBIDDEN") {
							continue;
						}
						// the value: every literal from here to the end of the declaration
						let mut value = String::new();
						for l in text.lines().skip(i) {
							let mut chars = l.split('"');
							chars.next();
							for part in chars.by_ref().step_by(2) {
								value.push_str(part);
							}
							if l.contains(';') {
								break;
							}
						}
						let value = needle(&value);
						constants += 1;
						if value.is_empty() {
							continue;
						}
						let rel = p.strip_prefix(root).unwrap_or(&p).display().to_string();
						if !listed.iter().any(|l| l.contains(&value) || value.contains(l.as_str())) {
							problems.push(format!(
								"{rel} publishes the reason {value:?} and the reasons table has no \
								 row for it: an operator reading status/<family>/reason will meet a \
								 sentence nobody documented"
							));
						}
					}
				}
			}
		}
		assert!(
			constants >= 7,
			"only {constants} reason constants found — the naming changed, and this half of the \
			 check is silently reading nothing"
		);

		assert!(
			problems.is_empty(),
			"{} problems between the reasons table and the sentences the bridge publishes:\n        {}",
			problems.len(),
			problems.join("\n        ")
		);
	}

	use crate::burst::{Stop, Wake};
	use crate::settings::{
		REASON_AI_NO_TAG, REASON_CHIME_NO_ENTRY, REASON_CHIME_NO_ID, REASON_NO_LOCK, REASON_NO_OSDCHANNEL,
		REASON_NO_SESSION, REASON_READ_ONLY, REASON_TIMEOUT, REASON_TRANSIENT,
	};
	use bairelay_mqtt::settings::Family;

	/// French words that are not english words. Deliberately small, and deliberately
	/// without homographs: `budget`, `part`, `port`, `tour`, `camera`, `status` and
	/// `state` exist in both languages, and a guard that cries wolf gets silenced
	/// inside a month.
	const FRENCH: &[&str] = &[
		"la", "les", "des", "du", "une", "est", "dans", "pour", "sur", "par", "avec", "sans", "que", "qui", "pas",
		"plus", "tres", "tout", "toute", "autre", "chaque", "comme", "aussi", "donc", "mais", "seul", "seule",
		"caméra", "refusé", "réponse", "reglages", "réglages", "balise", "aucun", "aucune", "rien", "toujours",
		"jamais", "encore", "voici", "veut", "peut", "faire", "dit", "nomme", "publie", "motif", "famille",
		"familles", "lecture", "écriture", "sauf", "touche", "arrive", "restent", "revient",
	];

	/// Asserts one published value: printable ascii, and no french word in it.
	pub(crate) fn published_value_is_english(what: &str, value: &str) {
		assert!(value.is_ascii(), "{what} is not ascii on the wire: {value:?}");
		assert!(
			value.chars().all(|c| c.is_ascii_graphic() || c == ' '),
			"{what} carries a control character: {value:?}"
		);
		let lowered = value.to_lowercase();
			let bad: Vec<&str> = lowered
			.split(|c: char| !c.is_alphabetic() && c != '-')
			.filter(|w| FRENCH.contains(w))
			.collect();
		assert!(bad.is_empty(), "{what} would publish french to Home Assistant: {value:?} contains {bad:?}");
	}

	#[test]
	fn every_reason_sentence_is_english() {
		for (name, value) in [
			("REASON_TIMEOUT", REASON_TIMEOUT),
			("REASON_READ_ONLY", REASON_READ_ONLY),
			("REASON_NO_SESSION", REASON_NO_SESSION),
			("REASON_NO_LOCK", REASON_NO_LOCK),
			("REASON_TRANSIENT", REASON_TRANSIENT),
			("REASON_NO_OSDCHANNEL", REASON_NO_OSDCHANNEL),
			("REASON_CHIME_NO_ENTRY", REASON_CHIME_NO_ENTRY),
			("REASON_CHIME_NO_ID", REASON_CHIME_NO_ID),
			("REASON_AI_NO_TAG", REASON_AI_NO_TAG),
		] {
			published_value_is_english(name, value);
		}
	}

	#[test]
	fn every_burst_state_word_is_english() {
		for wake in [Wake::Press, Wake::Motion, Wake::Push, Wake::Startup] {
			published_value_is_english("Wake::slug", wake.slug());
		}
		for stop in [
			Stop::NoMqtt,
			Stop::Stream,
			Stop::NoSession,
			Stop::Refused,
			Stop::Budget,
			Stop::Duration,
			Stop::Cancelled,
			Stop::Done,
		] {
			published_value_is_english("Stop::slug", stop.slug());
		}
	}

	#[test]
	fn every_family_topic_segment_is_english() {
		for family in Family::ALL {
			published_value_is_english("Family::slug", family.slug());
		}
	}
}
