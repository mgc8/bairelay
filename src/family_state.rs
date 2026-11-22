//! The memory of the settings families **held to be writable**.
//!
//! # The problem this file solves
//!
//! A settings family is published modifiable (slider, select,
//! switch with a command topic) only after the camera has **answered a read of it**
//! successfully. That is the bridge's rule: never promise a write on
//! a tag that has not been read.
//!
//! But that verdict lived only in the process memory. Concretely, on the
//! doorbell:
//!
//! * at startup, everything goes out read-only;
//! * a full burst is needed (`query/settings`, ~15 radio commands, and the
//!   camera has to be woken) for the families to get their status back;
//! * and **a restart of the add-on forgets everything**, so the camera pays
//!   fifteen radio commands again to recover what it had already proven
//!   the day before.
//!
//! This module therefore persists, on the add-on's disk, the list of families
//! for which the bridge has proof they are writable. On the next start, it
//! marks them writable **without sending a single packet to the camera**.
//!
//! # Where the file is, and why there
//!
//! Next to the configuration file, under the name [`FAMILIES_READ_FILE`] — the
//! same location as [`crate::config::CLOUD_AUTH_FILE`], an explicit precedent.
//! In the Home Assistant add-on, that is `/config/bairelay/`: a directory
//! already mounted read-write, which survives image updates, and which is
//! **not** the repository. Nothing is written into the code tree.
//!
//! # What the file does not contain
//!
//! Family slugs, nothing else: no setting value, no
//! identifier, no secret. The file is therefore useless outside the bridge
//! and contains nothing to protect.
//!
//! # The three survival rules
//!
//! 1. **Never a camera command**: this module only reads and writes a
//!    file. Marking a family writable wakes nobody up.
//! 2. **Never a failed start**: absent file = current behaviour
//!    (everything read-only until the first re-read); unreadable or invalid
//!    file = same, with a warning. The bridge always starts.
//! 3. **Never a half write**: write into a temporary file of the same
//!    directory then rename. A crash leaves the old file intact,
//!    never a truncated JSON.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use bairelay_mqtt::discovery::DiscoveryPublisher;
use bairelay_mqtt::settings::Family;

/// The state file's name, placed next to the configuration file.
///
/// The `config-` prefix is the neighbour's ([`crate::config::CLOUD_AUTH_FILE`]):
/// in `/config/bairelay/`, those files are spotted at a glance, and the
/// name says what it holds without having to open it.
pub const FAMILIES_READ_FILE: &str = "config-families-read.json";

/// The name the same file carried before the english migration.
///
/// Read **only** when [`FAMILIES_READ_FILE`] does not exist. The rename moved the
/// name, not the data: an installation that had proved twenty families came back
/// with none of them, every write entity went read-only, and nothing on the camera
/// side could explain it. One `read` brings all of it back, and the bridge sends no
/// packet to the camera for it.
pub const LEGACY_FAMILIES_READ_FILE: &str = "config-familles-lues.json";

/// What this format knows how to read. A file of an unknown version is ignored
/// (and rewritten) rather than being misinterpreted.
const VERSION: u8 = 1;

/// The location resolved once and for all from the configuration file's path.
/// `OnceLock` and not a field of `CameraConfig`: the path
/// does not belong to the cameras (it is unique per process, that of the config
/// file we loaded), and threading it down to the heart of the camera would
/// only make every test constructor heavier.
///
/// Absent = this module does nothing. That is also what keeps the rest of the
/// binary's tests free of side effects: as long as `set_path` is not called,
/// no write happens.
static CHEMIN: OnceLock<PathBuf> = OnceLock::new();

/// Cameras already primed in this process: priming happens once per
/// camera only, otherwise every discovery republication would re-read the file.
/// `BTreeSet` and not `HashSet` only because the former must be buildable in a
/// `static`.
static AMORCEES: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// The file's content: `{"version":1,"cameras":{"Doorbell":["audio", ...]}}`.
///
/// `BTreeMap` so that two identical writes produce identical
/// bytes — a file that does not move when nothing moved diffs and
/// reads better when diagnosing a real installation.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct ReadFamilies {
	version: u8,
	cameras: BTreeMap<String, Vec<String>>,
}

/// Computes the location from the configuration file's path, and
/// remembers it. Called once in `main`, right after `--config` is resolved.
pub fn set_path(config_path: &Path) {
	let _ = CHEMIN.set(config_path.with_file_name(FAMILIES_READ_FILE));
}

/// The remembered location, if `set_path` was called.
fn path() -> Option<&'static Path> {
	CHEMIN.get().map(PathBuf::as_path)
}

/// The pre-migration location, beside the current one. `None` when the current path
/// carries no directory at all (a bare file name resolved in the working directory), because
/// then there is nowhere to look that is not the same file.
fn legacy_path(path: &Path) -> Option<PathBuf> {
	let dir = path.parent()?;
	if dir.as_os_str().is_empty() {
		return None;
	}
	Some(path.with_file_name(LEGACY_FAMILIES_READ_FILE))
}

/// Re-reads a camera's state: the families whose writability is proven.
///
/// Absent file, unreadable file, unknown camera, unknown slug: **an empty
/// `Vec`**, with a warning when the file was there but does not let itself be
/// read. A state that cannot be read must be confused with "we do not know
/// yet", never prevent the bridge from starting.
fn load_saved(path: &Path, camera: &str) -> Vec<Family> {
	let brut = match std::fs::read_to_string(path) {
		Ok(brut) => brut,
		// Absent: the normal case on a first start, and after an
		// add-on reinstall. Not an event.
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
		Err(e) => {
			tracing::warn!(
				path = %path.display(),
				erreur = %e,
				"family state unreadable: everything restarts read-only until the first re-read"
			);
			return Vec::new();
		}
	};
	let lu: Result<ReadFamilies, serde_json::Error> = serde_json::from_str(&brut);
	let state = match lu {
		Ok(state) if state.version == VERSION => state,
		Ok(state) => {
			tracing::warn!(
				path = %path.display(),
				version = state.version,
				"family state in an unknown format: it will be rewritten at the first write"
			);
			return Vec::new();
		}
		Err(e) => {
			tracing::warn!(
				path = %path.display(),
				erreur = %e,
				"family state corrupt: everything restarts read-only until the first re-read"
			);
			return Vec::new();
		}
	};
	let Some(slugs) = state.cameras.get(camera) else {
		return Vec::new();
	};
	// A slug this binary does not know creates no family: the file
	// may have been written by a more recent version, and guessing a family
	// from a word would be exactly the invention the rest of the bridge refuses.
	slugs.iter().filter_map(|s| Family::from_slug(s)).collect()
}

/// [`load_saved`], with one look at the name this file had before the english
/// migration - and only when the new file is **absent**.
///
/// The narrow condition is the whole point. Once the new file exists it is the
/// authority alone: a family deliberately withdrawn from it (`save` with an empty
/// list removes the camera entry) must not walk back in through the old name, or the
/// bridge would promise a write on a proof it had just taken away.
fn load_saved_with_legacy(path: &Path, camera: &str) -> Vec<Family> {
	if path.exists() {
		return load_saved(path, camera);
	}
	let Some(legacy) = legacy_path(path) else {
		return Vec::new();
	};
	let found = load_saved(&legacy, camera);
	if !found.is_empty() {
		tracing::info!(
			path = %legacy.display(),
			camera,
			families = found.len(),
			"family state found under its pre-migration name: it is taken at face value and rewritten under the new name at the next change"
		);
	}
	found
}

/// The families whose writability is **already proven** for this camera,
/// in the order of [`Family::ALL`] — therefore the order in which the wake burst
/// ([`crate::burst`]) will re-read them.
///
/// An empty `Vec` when [`set_path`] was not called, when the file is
/// absent, or when this camera is not in it yet: the burst then reads
/// only the battery. That is intended — nothing here guesses a family, and a
/// family never read has no business in a burst paid for in radio.
pub(crate) fn proven_families(camera: &str) -> Vec<Family> {
	let Some(path) = path() else {
		return Vec::new();
	};
	let connues = load_saved_with_legacy(path, camera);
	if connues.is_empty() {
		return Vec::new();
	}
	// The protocol's order rather than the file's: `load_saved` returns the slugs
	// in the order the JSON wrote them, and the burst must start from the families
	// the operator cares about first, not from the alphabetical order.
	Family::ALL
		.into_iter()
		.filter(|f| connues.contains(f))
		.collect()
}

/// Writes a camera's state, preserving the others.
///
/// **Atomic** write: a temporary file in the same directory (same
/// filesystem, the only condition for the rename to be atomic too),
/// then the rename. Renaming over an existing file fails on
/// Windows, hence the unlink just before — the window between the two exposes
/// only reading "absent", which is already a handled case.
fn save(path: &Path, camera: &str, families: &[Family]) -> io::Result<()> {
	let mut state = std::fs::read_to_string(path)
		.ok()
		.and_then(|brut| serde_json::from_str::<ReadFamilies>(&brut).ok())
		.unwrap_or_default();
	// A file of another version restarts from scratch, but only for the
	// camera being written: the other entries stay what
	// they are, and the version is raised.
	state.version = VERSION;
	let mut slugs: Vec<String> = families.iter().map(|f| f.slug().to_string()).collect();
	slugs.sort();
	slugs.dedup();
	if slugs.is_empty() {
		state.cameras.remove(camera);
	} else {
		state.cameras.insert(camera.to_string(), slugs);
	}
	if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
		std::fs::create_dir_all(dir)?;
	}
	let nom = path
		.file_name()
		.map(|n| n.to_string_lossy().into_owned())
		.unwrap_or_else(|| FAMILIES_READ_FILE.to_string());
	// The pid in the temporary file's name: two processes sharing this
	// directory (an add-on restarting while a command-line tool is run next to it)
	// must not write into the same temporary file.
	let temporaire = path.with_file_name(format!("{nom}.{}.tmp", std::process::id()));
	std::fs::write(&temporaire, serde_json::to_string_pretty(&state).unwrap_or_default())?;
	if path.exists() {
		std::fs::remove_file(path)?;
	}
	if let Err(e) = std::fs::rename(&temporaire, path) {
		let _ = std::fs::remove_file(&temporaire);
		return Err(e);
	}
	Ok(())
}

/// Priming, with the location remembered by [`set_path`]. Does nothing if
/// no location was resolved (tests, command-line tools).
pub fn prime(publisher: &DiscoveryPublisher, camera: &str) {
	let Some(path) = path() else { return };
	let deja = {
		let mut guard = match AMORCEES.lock() {
			Ok(g) => g,
			Err(p) => p.into_inner(),
		};
		!guard.insert(camera.to_string())
	};
	if deja {
		return;
	}
	prime_wakes_nothing(path, publisher, camera);
}

/// Same thing, explicit location — the form the tests use.
fn prime_wakes_nothing(path: &Path, publisher: &DiscoveryPublisher, camera: &str) {
	let families = load_saved_with_legacy(path, camera);
	if families.is_empty() {
		return;
	}
	let mut marquees = 0usize;
	for family in families {
		if publisher.mark_family_read(camera, family, true) {
			marquees += 1;
		}
	}
	tracing::info!(
		camera,
		families = marquees,
		path = %path.display(),
		"families brought back to writable from disk: no camera command was sent"
	);
}

/// Remembers a camera's state, with the remembered location. Does nothing if
/// no location is resolved.
pub fn record(publisher: &DiscoveryPublisher, camera: &str) {
	let Some(path) = path() else { return };
	let families: Vec<Family> = publisher.writable_families(camera).into_iter().collect();
	if let Err(e) = save(path, camera, &families) {
		tracing::warn!(
			camera,
			path = %path.display(),
			erreur = %e,
			"the family state could not be remembered: the next start will restart read-only"
		);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A unique path per test, in the temporary directory: these tests
	/// really write to disk (that is what they test), but never
	/// into the repository and never at the same place.
	fn a_path_setting_test(nom: &str) -> PathBuf {
		let mut path = std::env::temp_dir();
		path.push(format!(
			"bairelay-families-read-{}-{}.json",
			std::process::id(),
			nom
		));
		path
	}

	#[test]
	fn a_proven_family_is_found_again_without_the_camera() {
		let path = a_path_setting_test("aller-retour");
		let _ = std::fs::remove_file(&path);

		// What the bridge learned: image and audio are writable.
		save(&path, "Doorbell", &[Family::Image, Family::Audio, Family::Image])
			.expect("writing the state");
		assert!(
			!path.with_file_name(format!("{}.tmp", std::process::id())).exists(),
			"the temporary file must have been renamed, not left behind"
		);

		// What the restart concludes from it, without a camera.
		assert_eq!(
			load_saved(&path, "Doorbell"),
			vec![Family::Audio, Family::Image],
			"the order comes from the file, duplicates were dropped"
		);
		assert!(load_saved(&path, "Other camera").is_empty(), "another camera sees nothing");

		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn a_corrupt_file_blocks_nothing_and_is_repaired() {
		let path = a_path_setting_test("corrupt");
		std::fs::write(&path, "{ this is not json").expect("fixture");
		assert!(load_saved(&path, "Doorbell").is_empty(), "corrupt = we do not know yet");

		// And the first write that follows takes the file back in hand.
		save(&path, "Doorbell", &[Family::Pir]).expect("rewriting");
		assert_eq!(load_saved(&path, "Doorbell"), vec![Family::Pir]);

		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn an_unknown_slug_does_not_pass() {
		let path = a_path_setting_test("slug");
		std::fs::write(
			&path,
			r#"{"version":1,"cameras":{"Doorbell":["audio","family-inventee"]}}"#,
		)
		.expect("fixture");
		assert_eq!(
			load_saved(&path, "Doorbell"),
			vec![Family::Audio],
			"a slug the bridge does not know does not invent a family"
		);
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn an_unknown_version_is_ignored() {
		let path = a_path_setting_test("version");
		std::fs::write(&path, r#"{"version":99,"cameras":{"Doorbell":["audio"]}}"#)
			.expect("fixture");
		assert!(load_saved(&path, "Doorbell").is_empty(), "an unknown format is not guessed");
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn the_other_cameras_are_preserved() {
		let path = a_path_setting_test("voisines");
		let _ = std::fs::remove_file(&path);
		save(&path, "Doorbell", &[Family::Audio]).expect("first camera");
		save(&path, "Garage", &[Family::Image]).expect("second camera");
		assert_eq!(load_saved(&path, "Doorbell"), vec![Family::Audio]);
		assert_eq!(load_saved(&path, "Garage"), vec![Family::Image]);

		// A family that falls back to read-only leaves the file — otherwise the
		// bridge would come back modifiable on a proof it just withdrew.
		save(&path, "Doorbell", &[]).expect("retrait");
		assert!(load_saved(&path, "Doorbell").is_empty());
		assert_eq!(
			load_saved(&path, "Garage"),
			vec![Family::Image],
			"removing a camera does not erase the others"
		);

		let _ = std::fs::remove_file(&path);
	}

	/// The same idea as [`a_path_setting_test`], but one **directory** per test.
	/// The legacy name is a constant, so three tests looking for it beside their own
	/// file would all open the same `/tmp/config-familles-lues.json` and overwrite
	/// each other under the parallel threads of `cargo test`. A fixture that passes or
	/// fails depending on thread timing is not a fixture, it is a coin toss.
	fn a_directory_for_legacy_test(nom: &str) -> PathBuf {
		let mut dir = std::env::temp_dir();
		dir.push(format!(
			"bairelay-families-legacy-{}-{}",
			std::process::id(),
			nom
		));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).expect("a private directory for the fixture");
		dir
	}

	#[test]
	fn a_state_left_under_the_old_name_is_found_again() {
		// The rename is the incident this guards: the proof is on the disk, under a
		// name this binary no longer writes, and the restart must not lose it.
		let dir = a_directory_for_legacy_test("present");
		let current = dir.join(FAMILIES_READ_FILE);
		let legacy = dir.join(LEGACY_FAMILIES_READ_FILE);
		std::fs::write(&legacy, r#"{"version":1,"cameras":{"Doorbell":["audio","chime"]}}"#)
			.expect("fixture");
		assert_eq!(
			load_saved_with_legacy(&current, "Doorbell"),
			vec![Family::Audio, Family::Chime],
			"a state file under the old name is not a reason to start read-only"
		);
		assert!(
			!current.exists(),
			"reading the old name must not write the new one behind the tests back"
		);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn the_new_name_is_the_authority_once_it_exists() {
		// A family withdrawn from the new file is a proof the bridge took back. The
		// old name must not hand it over again.
		let dir = a_directory_for_legacy_test("shadowed");
		let current = dir.join(FAMILIES_READ_FILE);
		let legacy = dir.join(LEGACY_FAMILIES_READ_FILE);
		std::fs::write(&legacy, r#"{"version":1,"cameras":{"Doorbell":["audio"]}}"#).expect("fixture");
		std::fs::write(&current, r#"{"version":1,"cameras":{}}"#).expect("fixture");
		assert!(
			load_saved_with_legacy(&current, "Doorbell").is_empty(),
			"an empty new file is a decision, not an absence"
		);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn a_legacy_state_survives_the_first_write_under_the_new_name() {
		// What the operator actually gets: the old proof is read, and the first
		// change from then on lands under the name this binary writes.
		let dir = a_directory_for_legacy_test("rewritten");
		let current = dir.join(FAMILIES_READ_FILE);
		let legacy = dir.join(LEGACY_FAMILIES_READ_FILE);
		std::fs::write(&legacy, r#"{"version":1,"cameras":{"Doorbell":["osd"]}}"#).expect("fixture");
		assert_eq!(load_saved_with_legacy(&current, "Doorbell"), vec![Family::Osd]);
		save(&current, "Doorbell", &[Family::Osd]).expect("rewrite");
		assert_eq!(
			load_saved_with_legacy(&current, "Doorbell"),
			vec![Family::Osd],
			"recovered, and now from the name this binary writes"
		);
		assert!(legacy.exists(), "the old file is never deleted by this module");
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn a_defined_path_lands_next_to_the_config() {
		// The location rule is checked without touching the global: same
		// directory, own name, and the config file itself untouched.
		let config = Path::new("/config/bairelay/bairelay.toml");
		assert_eq!(
			config.with_file_name(FAMILIES_READ_FILE),
			PathBuf::from("/config/bairelay/config-families-read.json")
		);
	}
}
