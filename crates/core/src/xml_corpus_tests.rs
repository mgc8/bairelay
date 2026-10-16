//! Snapshot harness over a directory of real camera XML payloads.
//!
//! Dormant unless `BAIRELAY_XML_CORPUS=<dir>` is set. Each `<dir>/*.xml`
//! is parsed with the production parser picked by its root element
//! (`<body>` → `BcXml`, `<Extension>` → `Extension`, `<P2P>` → `UdpXml`)
//! and re-serialized. The parsed structure plus the outbound bytes form
//! the file's snapshot, stored next to it as `<name>.xml.snap`.
//!
//! `BAIRELAY_XML_CORPUS_BLESS=1` (re)writes the snapshots; otherwise the
//! test fails listing every file whose snapshot changed. Run it before
//! and after an XML-stack upgrade to see exactly what changed.
//!
//! The corpus holds decrypted camera payloads (UIDs, credential hashes),
//! so it lives in a gitignored directory such as `tests/logs/xml-corpus/`.
//! `docs/testing.md` § XML corpus snapshots covers how to capture one.

use crate::bc::xml::{BcXml, Extension};
use crate::bcudp::xml::UdpXml;
use std::path::Path;

/// The document's root element: past any `<?xml …?>` declaration.
fn root_of(text: &str) -> &str {
	let t = text.trim_start();
	let t = match t.strip_prefix("<?xml") {
		Some(rest) => rest.split_once("?>").map_or(rest, |(_, r)| r),
		None => t,
	};
	t.trim_start()
}

fn snapshot<T: std::fmt::Debug, E: std::fmt::Display, S: std::fmt::Display>(
	parsed: Result<T, E>,
	serialize: impl Fn(&T) -> Result<Vec<u8>, S>,
) -> String {
	match parsed {
		Ok(parsed) => {
			let out = match serialize(&parsed) {
				Ok(bytes) => format!("{:?}", String::from_utf8_lossy(&bytes)),
				Err(e) => format!("serialize error: {e}"),
			};
			format!("{parsed:#?}\n--- serialized ---\n{out}\n")
		}
		Err(e) => format!("parse error: {e}\n"),
	}
}

fn render(xml: &[u8]) -> String {
	let text = String::from_utf8_lossy(xml);
	let root = root_of(&text);
	if root.starts_with("<P2P") {
		snapshot(UdpXml::try_parse(xml), |x| x.serialize(Vec::new()))
	} else if root.starts_with("<Extension") {
		snapshot(Extension::try_parse(xml), |x| x.serialize(Vec::new()))
	} else {
		snapshot(BcXml::try_parse(xml), |x| x.serialize(Vec::new()))
	}
}

#[test]
fn xml_corpus_snapshots() {
	let Some(dir) = std::env::var_os("BAIRELAY_XML_CORPUS") else {
		eprintln!("BAIRELAY_XML_CORPUS unset; skipping XML corpus snapshots");
		return;
	};
	let bless = std::env::var_os("BAIRELAY_XML_CORPUS_BLESS").is_some();
	let mut files: Vec<_> = std::fs::read_dir(Path::new(&dir))
		.expect("read BAIRELAY_XML_CORPUS dir")
		.filter_map(|e| e.ok().map(|e| e.path()))
		.filter(|p| p.extension().is_some_and(|x| x == "xml"))
		.collect();
	files.sort();
	assert!(!files.is_empty(), "no *.xml files in {dir:?}");

	let mut missing = Vec::new();
	let mut differs = Vec::new();
	for path in &files {
		let xml = std::fs::read(path).expect("read corpus file");
		let actual = render(&xml);
		let snap = path.with_extension("xml.snap");
		if bless {
			std::fs::write(&snap, &actual).expect("write snapshot");
			continue;
		}
		let group = match std::fs::read_to_string(&snap) {
			Ok(expected) if expected == actual => continue,
			Ok(_) => &mut differs,
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => &mut missing,
			Err(e) => panic!("read snapshot {}: {e}", snap.display()),
		};
		std::fs::write(path.with_extension("xml.snap.new"), &actual).expect("write new snapshot");
		group.push(path.display().to_string());
	}
	assert!(
		missing.is_empty() && differs.is_empty(),
		"{} of {} files failed (see *.xml.snap.new)\n\
		 no snapshot yet (run with BAIRELAY_XML_CORPUS_BLESS=1): {}\n{}\n\
		 snapshot differs: {}\n{}",
		missing.len() + differs.len(),
		files.len(),
		missing.len(),
		missing.join("\n"),
		differs.len(),
		differs.join("\n")
	);
}
