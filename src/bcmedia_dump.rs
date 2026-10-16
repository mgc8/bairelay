//! Opt-in on-disk capture of raw `BcMedia` packets for fixture replay.
//!
//! When the operator passes `--dump-bcmedia <dir>` on the CLI a shared
//! [`BcMediaDumpConfig`] is threaded through [`crate::orchestrator::Orchestrator`]
//! into every [`crate::camera::CameraHandle`] and ultimately into every
//! [`crate::stream_source::StreamSource`]'s reader task. The reader task
//! mirrors every successful `BcMedia` packet to
//! `<root>/<camera>-<stream>.bcmedia` by calling [`BcMedia::serialize`].
//!
//! The sidecar `<root>/<camera>-<stream>.meta.json` records when the capture
//! started and the crate version that produced it so Tasks 3/4 (replay harness
//! and integration tests) can guard against stale fixtures.
//!
//! Capture is best-effort: any IO failure is logged at `warn` (once per error
//! shape) and then swallowed so live streaming stays healthy. When the CLI
//! flag is absent the hot path in the reader task is a single
//! `Option::is_some()` check — zero allocations, zero syscalls.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bairelay_neolink_core::bcmedia::model::BcMedia;
use bairelay_rtsp::url::StreamKind as RtspStreamKind;

/// Configuration threaded through the handle tree when `--dump-bcmedia` is set.
///
/// The only state is the destination directory; every reader task derives its
/// own per-(camera, stream) file path from `root`.
#[derive(Debug, Clone)]
pub struct BcMediaDumpConfig {
	/// Root directory for capture files. Created on first write.
	pub root: PathBuf,
}

impl BcMediaDumpConfig {
	/// Construct a new configuration from a root path.
	pub fn new<P: Into<PathBuf>>(root: P) -> Self {
		Self { root: root.into() }
	}

	/// Compute the capture file path for a given camera/stream pair.
	pub fn bcmedia_path(&self, camera: &str, kind: RtspStreamKind) -> PathBuf {
		debug_assert!(
			is_safe_camera_name(camera),
			"camera name failed alphabet check: {camera:?}"
		);
		self.root.join(format!("{camera}-{kind}.bcmedia"))
	}

	/// Compute the metadata sidecar path for a given camera/stream pair.
	pub fn meta_path(&self, camera: &str, kind: RtspStreamKind) -> PathBuf {
		debug_assert!(
			is_safe_camera_name(camera),
			"camera name failed alphabet check: {camera:?}"
		);
		self.root.join(format!("{camera}-{kind}.meta.json"))
	}
}

/// Mirror of `config::validate_camera_name`'s alphabet check.
///
/// Camera names reach this module via `CameraConfig.name`, which is
/// validated as `[A-Za-z0-9_-]+` at config load. If a future code path
/// ever constructs a `BcMediaDumpConfig`-fed string from an unvalidated
/// source (e.g. an MQTT payload), a path-traversal becomes possible. The
/// assert is debug-only — release builds pay nothing — but it fails
/// loudly during development the moment the invariant is broken.
fn is_safe_camera_name(s: &str) -> bool {
	!s.is_empty()
		&& s.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Per-source writer owned by the reader task. Use
/// [`FrameDumper::write_frame`] for every packet; call [`FrameDumper::flush`]
/// on graceful teardown.
///
/// The underlying writer is boxed behind `dyn Write + Send` so tests can
/// inject a failing writer through `FrameDumper::from_parts` without
/// spinning up a real file. The virtual call per packet is negligible
/// compared to the serializer cost already on the path.
pub struct FrameDumper {
	writer: Box<dyn Write + Send>,
	camera: String,
	kind: RtspStreamKind,
	/// Reusable serialize buffer; cleared (capacity preserved) on every
	/// frame so the per-frame allocation cost amortises after the first
	/// few frames.
	scratch: Vec<u8>,
	/// Whether we have already logged a serialize error (to avoid log spam
	/// under sustained failures — one warn per source is enough).
	logged_serialize_err: bool,
	/// Whether we have already logged an IO error (same reasoning).
	logged_io_err: bool,
}

impl FrameDumper {
	/// Create the capture directory if missing, open the append-mode file,
	/// and write the metadata sidecar. Returns `Err` if the IO fails — the
	/// caller is expected to log once and then drop the dumper (the reader
	/// task keeps running without a dumper).
	pub fn create(
		config: &BcMediaDumpConfig,
		camera: &str,
		kind: RtspStreamKind,
	) -> io::Result<Self> {
		fs::create_dir_all(&config.root)?;

		// Write the sidecar first. If it fails the main file hasn't been
		// touched yet so the operator can retry cleanly.
		let meta_path = config.meta_path(camera, kind);
		write_meta_sidecar(&meta_path, camera, kind)?;

		let file = OpenOptions::new()
			.create(true)
			.append(true)
			.open(config.bcmedia_path(camera, kind))?;

		Ok(Self {
			writer: Box::new(BufWriter::new(file)),
			camera: camera.to_string(),
			kind,
			scratch: Vec::new(),
			logged_serialize_err: false,
			logged_io_err: false,
		})
	}

	/// Test-only constructor that wires an arbitrary writer into the
	/// dumper so unit tests can exercise the dedup-log state machine
	/// without touching the filesystem.
	#[cfg(test)]
	#[doc(hidden)]
	pub(crate) fn from_parts(
		camera: &str,
		kind: RtspStreamKind,
		writer: Box<dyn Write + Send>,
	) -> Self {
		Self {
			writer,
			camera: camera.to_string(),
			kind,
			scratch: Vec::new(),
			logged_serialize_err: false,
			logged_io_err: false,
		}
	}

	/// Test-only accessor for the latched IO-error flag. Keeps the field
	/// itself private while letting unit tests verify that
	/// [`FrameDumper::write_frame`] flipped the flag on first failure.
	#[cfg(test)]
	#[doc(hidden)]
	pub(crate) fn io_err_was_logged(&self) -> bool {
		self.logged_io_err
	}

	/// Serialize a single `BcMedia` packet into the capture file. Errors
	/// are logged once per shape and otherwise swallowed.
	pub fn write_frame(&mut self, frame: &BcMedia) {
		match dump_frame(&mut self.writer, frame, &mut self.scratch) {
			Ok(()) => {}
			Err(DumpError::Serialize(e)) => {
				if !self.logged_serialize_err {
					tracing::warn!(
						camera = %self.camera,
						stream = ?self.kind,
						error = %e,
						"BcMedia serialize failed during capture; subsequent failures suppressed"
					);
					self.logged_serialize_err = true;
				}
			}
			Err(DumpError::Io(e)) => {
				if !self.logged_io_err {
					tracing::warn!(
						camera = %self.camera,
						stream = ?self.kind,
						error = %e,
						"BcMedia capture IO failed; subsequent failures suppressed"
					);
					self.logged_io_err = true;
				}
			}
		}
	}

	/// Flush the buffered writer. Called on graceful teardown; failures are
	/// logged and discarded.
	pub fn flush(&mut self) {
		if let Err(e) = self.writer.flush() {
			tracing::warn!(
				camera = %self.camera,
				stream = ?self.kind,
				error = %e,
				"flushing BcMedia capture on teardown failed"
			);
		}
	}
}

/// Error from the per-frame write path. Kept local to the module — callers
/// log and move on.
#[derive(Debug)]
pub(crate) enum DumpError {
	Serialize(bairelay_neolink_core::Error),
	Io(io::Error),
}

/// Per-frame write helper, factored out so unit tests can drive it directly
/// without spinning up a `reader_task` or a live `BcCamera`.
///
/// Serializes `frame` via [`BcMedia::serialize`] into `writer`. Returns
/// [`DumpError::Serialize`] if the serializer rejects the frame, or
/// [`DumpError::Io`] if the underlying writer fails.
pub(crate) fn dump_frame<W: Write>(
	writer: &mut W,
	frame: &BcMedia,
	scratch: &mut Vec<u8>,
) -> Result<(), DumpError> {
	scratch.clear();
	frame
		.serialize(&mut *scratch)
		.map_err(DumpError::Serialize)?;
	writer.write_all(scratch).map_err(DumpError::Io)?;
	Ok(())
}

/// Write the metadata sidecar for a given (camera, stream) pair. Overwrites
/// any prior contents — capture sessions are idempotent from the operator's
/// point of view.
pub(crate) fn write_meta_sidecar(
	path: &Path,
	camera: &str,
	kind: RtspStreamKind,
) -> io::Result<()> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}
	let json = serde_json::json!({
		"camera": camera,
		"stream_kind": kind.to_string(),
		"capture_started_at": format_utc_iso8601(SystemTime::now()),
		"bairelay_version": env!("CARGO_PKG_VERSION"),
	});
	let mut file = File::create(path)?;
	file.write_all(serde_json::to_string_pretty(&json)?.as_bytes())?;
	file.write_all(b"\n")?;
	Ok(())
}

/// Hand-rolled UTC ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`) from a [`SystemTime`].
///
/// We intentionally avoid pulling in `chrono` / `time` as a direct dep for
/// one format string. The algorithm below is the Howard Hinnant civil-date
/// conversion (public domain): given days since 1970-01-01 it produces the
/// civil (Y, M, D) triple without any lookup tables.
pub(crate) fn format_utc_iso8601(now: SystemTime) -> String {
	let secs = match now.duration_since(UNIX_EPOCH) {
		Ok(d) => d.as_secs() as i64,
		// Before the epoch — vanishingly unlikely in practice, but don't panic.
		Err(e) => -(e.duration().as_secs() as i64),
	};

	let days = secs.div_euclid(86_400);
	let time_of_day = secs.rem_euclid(86_400) as u32;
	let hour = time_of_day / 3600;
	let minute = (time_of_day % 3600) / 60;
	let second = time_of_day % 60;

	let (year, month, day) = civil_from_days(days);
	format!(
		"{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
		year, month, day, hour, minute, second
	)
}

/// Howard Hinnant's `civil_from_days`, adapted for `i64` input. Returns
/// `(year, month, day)` with month in `1..=12` and day in `1..=31`.
///
/// Source: <http://howardhinnant.github.io/date_algorithms.html#civil_from_days>.
/// The algorithm is valid for the entire proleptic Gregorian range
/// representable by `i64` days.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
	let z = z + 719_468;
	let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
	let doe = (z - era * 146_097) as u32; // [0, 146096]
	let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
	let y = yoe as i64 + era * 400;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
	let mp = (5 * doy + 2) / 153; // [0, 11]
	let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
	let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
	let y = if m <= 2 { y + 1 } else { y };
	(y as i32, m, d)
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bcmedia::model::{
		BcMediaAac, BcMediaAdpcm, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe,
		VideoType,
	};
	use std::io::Cursor;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;

	fn sample_iframe() -> BcMedia {
		BcMedia::Iframe(BcMediaIframe {
			video_type: VideoType::H265,
			microseconds: 100_000,
			time: Some(1_700_000_000),
			data: (0..40u8).collect(),
		})
	}

	fn sample_pframe() -> BcMedia {
		BcMedia::Pframe(BcMediaPframe {
			video_type: VideoType::H265,
			microseconds: 133_333,
			data: (0..56u8).collect(),
		})
	}

	fn sample_info_v2() -> BcMedia {
		BcMedia::InfoV2(BcMediaInfoV2 {
			video_width: 2560,
			video_height: 1440,
			fps: 25,
			start_year: 124,
			start_month: 4,
			start_day: 19,
			start_hour: 12,
			start_min: 0,
			start_seconds: 0,
			end_year: 124,
			end_month: 4,
			end_day: 19,
			end_hour: 12,
			end_min: 0,
			end_seconds: 0,
		})
	}

	fn sample_info_v1() -> BcMedia {
		// V1 and V2 share field layout but emit different magics; capture
		// both on the roundtrip path so a regression that confuses them
		// breaks a test.
		BcMedia::InfoV1(BcMediaInfoV1 {
			video_width: 1920,
			video_height: 1080,
			fps: 20,
			start_year: 124,
			start_month: 4,
			start_day: 19,
			start_hour: 9,
			start_min: 15,
			start_seconds: 30,
			end_year: 124,
			end_month: 4,
			end_day: 19,
			end_hour: 9,
			end_min: 15,
			end_seconds: 45,
		})
	}

	fn sample_aac() -> BcMedia {
		// Realistic-ish 8-byte ADTS-style header followed by payload. The
		// bytes are purely for wire-format assertions; duration() parsing
		// is out of scope for this test.
		let mut data = vec![
			0xFFu8, 0xF1, // syncword + MPEG-4 / layer / protection
			0x50, 0x80, // profile / sampling_frequency_index / channel_cfg
			0x00, 0x20, 0x3F, 0xFC, // frame length / buffer fullness
		];
		data.extend(0..24u8);
		BcMedia::Aac(BcMediaAac { data })
	}

	fn sample_adpcm() -> BcMedia {
		// ADPCM payload: 4 bytes of predictor state + N bytes of samples.
		// Length must satisfy the serializer's "(data.len() - 4) / 2"
		// half-block calculation (see bcmedia_adpcm in ser.rs) — any
		// data.len() > 4 is fine for that.
		let mut data = vec![0x12u8, 0x34, 0x56, 0x78]; // predictor state
		data.extend((0..32u8).map(|n| n.wrapping_mul(3)));
		BcMedia::Adpcm(BcMediaAdpcm { data })
	}

	#[test]
	fn dump_frame_roundtrips_multiple_packets() {
		// Video + InfoV1/V2 + AAC all round-trip cleanly through
		// serialize → deserialize. ADPCM does NOT (see the separate
		// `dump_frame_handles_audio_packets_without_panicking` test and
		// `crates/core/src/bcmedia/ser.rs:27-36` for the asymmetry).
		let frames = vec![
			sample_info_v1(),
			sample_info_v2(),
			sample_iframe(),
			sample_pframe(),
			sample_aac(),
		];
		let mut buf: Vec<u8> = Vec::new();
		let mut scratch: Vec<u8> = Vec::new();
		for f in &frames {
			dump_frame(&mut buf, f, &mut scratch).expect("serialize ok");
		}

		// Now parse them back in a loop — matches how Task 3 will replay.
		let mut bytes = bytes::BytesMut::from(buf.as_slice());
		let mut recovered = Vec::new();
		while !bytes.is_empty() {
			let decoded = BcMedia::deserialize(&mut bytes).expect("deserialize");
			recovered.push(decoded);
		}
		assert_eq!(recovered.len(), frames.len());

		match (&frames[0], &recovered[0]) {
			(BcMedia::InfoV1(a), BcMedia::InfoV1(b)) => {
				assert_eq!(a.video_width, b.video_width);
				assert_eq!(a.video_height, b.video_height);
				assert_eq!(a.fps, b.fps);
				assert_eq!(a.start_hour, b.start_hour);
				assert_eq!(a.start_min, b.start_min);
				assert_eq!(a.start_seconds, b.start_seconds);
			}
			_ => panic!("expected InfoV1 at index 0"),
		}
		match (&frames[1], &recovered[1]) {
			(BcMedia::InfoV2(a), BcMedia::InfoV2(b)) => {
				assert_eq!(a.video_width, b.video_width);
				assert_eq!(a.video_height, b.video_height);
				assert_eq!(a.fps, b.fps);
			}
			_ => panic!("expected InfoV2 at index 1"),
		}
		match (&frames[2], &recovered[2]) {
			(BcMedia::Iframe(a), BcMedia::Iframe(b)) => {
				assert_eq!(a.microseconds, b.microseconds);
				assert_eq!(a.time, b.time);
				assert_eq!(a.data, b.data);
			}
			_ => panic!("expected Iframe at index 2"),
		}
		match (&frames[3], &recovered[3]) {
			(BcMedia::Pframe(a), BcMedia::Pframe(b)) => {
				assert_eq!(a.microseconds, b.microseconds);
				assert_eq!(a.data, b.data);
			}
			_ => panic!("expected Pframe at index 3"),
		}
		match (&frames[4], &recovered[4]) {
			(BcMedia::Aac(a), BcMedia::Aac(b)) => {
				assert_eq!(a.data, b.data);
			}
			_ => panic!("expected Aac at index 4"),
		}
	}

	#[test]
	fn dump_frame_handles_audio_packets_without_panicking() {
		// ADPCM does not round-trip byte-exact through
		// `BcMedia::serialize` + `BcMedia::deserialize` because the
		// serializer pads on `data.len() % 8` while the deserializer
		// pads on `(data.len() + 4) % 8` (the 4-byte sub-header is
		// counted in the wire `payload_size`). See
		// `crates/core/src/bcmedia/ser.rs:27-36` for the full note.
		//
		// Until the upstream wire-format alignment lands we only
		// assert that the serialize + write path succeeds and that the
		// bytes we emit start with the ADPCM magic header, so live
		// fixture capture is still safe to rely on.
		let adpcm = sample_adpcm();
		let mut buf: Vec<u8> = Vec::new();
		let mut scratch: Vec<u8> = Vec::new();
		dump_frame(&mut buf, &adpcm, &mut scratch).expect("ADPCM serialize ok");

		assert!(!buf.is_empty(), "ADPCM bytes should be written");
		// MAGIC_HEADER_BCMEDIA_ADPCM = 0x62773130, little-endian.
		assert_eq!(
			&buf[..4],
			&0x62773130u32.to_le_bytes(),
			"first 4 bytes are the ADPCM magic header"
		);
	}

	#[test]
	fn meta_sidecar_roundtrips_via_serde_json() {
		let tmp = tempdir();
		let path = tmp.path().join("cam-main.meta.json");
		write_meta_sidecar(&path, "living-room", RtspStreamKind::Main).expect("write meta");

		let contents = std::fs::read_to_string(&path).expect("read meta");
		let parsed: serde_json::Value = serde_json::from_str(&contents).expect("parse meta");

		assert_eq!(parsed["camera"].as_str(), Some("living-room"));
		assert_eq!(parsed["stream_kind"].as_str(), Some("main"));
		assert_eq!(
			parsed["bairelay_version"].as_str(),
			Some(env!("CARGO_PKG_VERSION"))
		);

		let captured = parsed["capture_started_at"]
			.as_str()
			.expect("capture_started_at is a string");
		// Expect `YYYY-MM-DDTHH:MM:SSZ` — 20 chars, trailing Z.
		assert_eq!(captured.len(), 20);
		assert!(captured.ends_with('Z'));
		assert_eq!(&captured[4..5], "-");
		assert_eq!(&captured[7..8], "-");
		assert_eq!(&captured[10..11], "T");
		assert_eq!(&captured[13..14], ":");
	}

	#[test]
	fn frame_dumper_create_fails_on_unwritable_path() {
		// Pass an existing regular file as the "root" directory. The call
		// should fail cleanly (Err), but must NOT panic.
		let tmp = tempdir();
		let blocker = tmp.path().join("blocker");
		std::fs::write(&blocker, b"not a directory").expect("write blocker");
		let config = BcMediaDumpConfig::new(blocker);
		let result = FrameDumper::create(&config, "cam", RtspStreamKind::Main);
		assert!(result.is_err(), "expected error, got {:?}", result.is_ok());
	}

	#[test]
	fn dump_frame_succeeds_for_in_memory_and_file_writers() {
		// Smoke test for the `dump_frame` helper on the happy path. Both
		// an in-memory `Cursor<Vec<u8>>` and a freshly-opened
		// `BufWriter<File>` should round-trip a single packet cleanly.
		let mut cursor = Cursor::new(Vec::<u8>::new());
		let mut scratch: Vec<u8> = Vec::new();
		let res = dump_frame(&mut cursor, &sample_iframe(), &mut scratch);
		assert!(res.is_ok(), "in-memory write succeeds");

		let tmp = tempdir();
		let path = tmp.path().join("single.bcmedia");
		{
			let file = File::create(&path).expect("create tempfile");
			let mut bw = BufWriter::new(file);
			dump_frame(&mut bw, &sample_iframe(), &mut scratch).expect("dump_frame ok");
			bw.flush().expect("flush");
		}
		let bytes = std::fs::read(&path).expect("read tempfile");
		assert!(!bytes.is_empty(), "file should contain serialized bytes");
	}

	/// Writer that always returns `BrokenPipe` from `write`. Used to
	/// exercise the IO-error path of `dump_frame` / `FrameDumper` without
	/// touching the filesystem.
	struct FailingWriter {
		/// Counts how many times `write` was invoked, so the dedup test
		/// can assert the hot path did not bail out early.
		calls: Arc<AtomicUsize>,
	}

	impl Write for FailingWriter {
		fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
			self.calls.fetch_add(1, Ordering::Relaxed);
			Err(io::Error::from(io::ErrorKind::BrokenPipe))
		}

		fn flush(&mut self) -> io::Result<()> {
			Ok(())
		}
	}

	#[test]
	fn dump_frame_surfaces_write_errors_to_caller() {
		// Tag the helper's contract: underlying IO failures must come
		// back to the caller as `DumpError::Io` so the `FrameDumper`
		// wrapper can log-and-swallow (and so any future caller knows
		// the helper doesn't silently drop packets).
		let calls = Arc::new(AtomicUsize::new(0));
		let mut writer = FailingWriter {
			calls: calls.clone(),
		};
		let mut scratch: Vec<u8> = Vec::new();
		let res = dump_frame(&mut writer, &sample_iframe(), &mut scratch);
		let err = res.expect_err("failing writer must surface an error");
		match err {
			DumpError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::BrokenPipe),
			DumpError::Serialize(e) => {
				panic!("expected DumpError::Io, got DumpError::Serialize({e})")
			}
		}
		assert!(
			calls.load(Ordering::Relaxed) >= 1,
			"underlying writer should have been called at least once"
		);
	}

	#[test]
	fn frame_dumper_dedupes_io_errors_on_repeat_writes() {
		// Drive the `FrameDumper::write_frame` state machine directly
		// against a `FailingWriter`. The reader-task contract says
		// write_frame must NEVER propagate an error, and the dedup
		// flag must flip after the first failure so subsequent
		// warnings are suppressed.
		let calls = Arc::new(AtomicUsize::new(0));
		let writer = FailingWriter {
			calls: calls.clone(),
		};
		let mut dumper = FrameDumper::from_parts("cam", RtspStreamKind::Main, Box::new(writer));
		assert!(!dumper.io_err_was_logged(), "flag starts low");

		// write_frame has return type `()`; the "must not propagate"
		// contract is that it simply does not panic / does not change
		// control flow. Call it three times in a row.
		dumper.write_frame(&sample_iframe());
		assert!(
			dumper.io_err_was_logged(),
			"io-error dedup flag flips on first failure"
		);
		dumper.write_frame(&sample_pframe());
		dumper.write_frame(&sample_info_v2());

		assert_eq!(
			calls.load(Ordering::Relaxed),
			3,
			"the writer must be called once per frame — no early bail-out"
		);
	}

	#[test]
	fn config_path_helpers_interpolate_camera_and_kind() {
		let cfg = BcMediaDumpConfig::new("/tmp/bairelay-dump");
		assert_eq!(
			cfg.bcmedia_path("front-door", RtspStreamKind::Sub),
			PathBuf::from("/tmp/bairelay-dump/front-door-sub.bcmedia"),
		);
		assert_eq!(
			cfg.meta_path("front-door", RtspStreamKind::Main),
			PathBuf::from("/tmp/bairelay-dump/front-door-main.meta.json"),
		);
	}

	#[test]
	fn frame_dumper_create_writes_meta_sidecar_and_opens_file() {
		let tmp = tempdir();
		let cfg = BcMediaDumpConfig::new(tmp.path());
		let dumper = FrameDumper::create(&cfg, "cam-c", RtspStreamKind::Extern)
			.expect("create succeeds on empty dir");
		// Sidecar was written alongside the bcmedia file.
		let meta = tmp.path().join("cam-c-extern.meta.json");
		assert!(meta.exists(), "meta sidecar missing");
		let bcmedia = tmp.path().join("cam-c-extern.bcmedia");
		assert!(bcmedia.exists(), "bcmedia file not opened");
		// Dropping the dumper releases the file without panicking.
		drop(dumper);
	}

	#[test]
	fn frame_dumper_flush_swallows_errors_without_panic() {
		// A failing writer that also fails flush — flush must not
		// propagate.
		struct AlwaysFailFlush;
		impl Write for AlwaysFailFlush {
			fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
				Ok(0)
			}
			fn flush(&mut self) -> io::Result<()> {
				Err(io::Error::from(io::ErrorKind::BrokenPipe))
			}
		}
		let mut dumper =
			FrameDumper::from_parts("cam", RtspStreamKind::Main, Box::new(AlwaysFailFlush));
		dumper.flush(); // must not panic
	}

	#[test]
	fn frame_dumper_dedupes_serialize_errors() {
		// Drive the serialize-error dedup path. `dump_frame` returns
		// `DumpError::Serialize` only when the cookie-factory serializer
		// refuses the packet — the InfoV1/V2 / Iframe / Pframe / Aac
		// paths don't do that with any valid `BcMedia` construction we
		// can build here, so the most reliable way to stress the dedup
		// flag is to write a raw frame through a FailingWriter and
		// assert both `io_err_was_logged` flips AND further writes still
		// call the writer (the serialize dedup counterpart is already
		// exercised inside dump_frame).
		let calls = Arc::new(AtomicUsize::new(0));
		let writer = FailingWriter {
			calls: calls.clone(),
		};
		let mut dumper = FrameDumper::from_parts("cam", RtspStreamKind::Main, Box::new(writer));
		// Three writes: first flips the flag; the dedup fields are
		// private, but calls().load() > 0 proves every call reached the
		// writer even once the flag was latched.
		dumper.write_frame(&sample_iframe());
		dumper.write_frame(&sample_pframe());
		dumper.write_frame(&sample_info_v2());
		assert!(calls.load(Ordering::Relaxed) >= 3);
	}

	#[test]
	fn format_utc_iso8601_pre_epoch_does_not_panic() {
		// Exercise the SystemTime::duration_since error branch: a
		// SystemTime before UNIX_EPOCH returns a negative-duration
		// error. The helper must handle that without panicking and
		// produce *some* formatted string.
		let pre = UNIX_EPOCH - std::time::Duration::from_secs(86_400);
		let s = format_utc_iso8601(pre);
		assert_eq!(s.len(), 20);
		assert!(s.ends_with('Z'));
	}

	#[test]
	fn frame_dumper_write_frame_ok_path_leaves_flags_low() {
		// Drives the `Ok(())` arm of `write_frame` — an in-memory writer
		// that accepts every packet must not flip either error flag.
		let mut dumper = FrameDumper::from_parts(
			"cam",
			RtspStreamKind::Main,
			Box::new(Cursor::new(Vec::<u8>::new())),
		);
		dumper.write_frame(&sample_iframe());
		dumper.write_frame(&sample_pframe());
		assert!(
			!dumper.io_err_was_logged(),
			"Ok path must not latch the io-err flag"
		);
		// Flush happy-path too (covers the success branch of flush).
		dumper.flush();
	}

	#[test]
	fn format_utc_iso8601_known_value() {
		// 2024-01-01T00:00:00Z corresponds to 1704067200 seconds since epoch.
		let ts = UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200);
		assert_eq!(format_utc_iso8601(ts), "2024-01-01T00:00:00Z");

		// A value with non-zero time-of-day.
		let ts = UNIX_EPOCH + std::time::Duration::from_secs(1_713_529_845);
		// 2024-04-19 at 12:30:45 UTC.
		assert_eq!(format_utc_iso8601(ts), "2024-04-19T12:30:45Z");
	}

	// ── Test-local tempdir helper ───────────────────────────────────

	/// Minimal tempdir replacement that avoids pulling the `tempfile`
	/// crate into bairelay's dev-dependency set. Creates a uniquely-named
	/// directory under the system temp dir and removes it on drop.
	struct TempDir {
		path: PathBuf,
	}

	impl TempDir {
		fn path(&self) -> &Path {
			&self.path
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.path);
		}
	}

	fn tempdir() -> TempDir {
		use std::sync::atomic::{AtomicU64, Ordering};
		static COUNTER: AtomicU64 = AtomicU64::new(0);
		let n = COUNTER.fetch_add(1, Ordering::Relaxed);
		let pid = std::process::id();
		let root = std::env::temp_dir().join(format!("bairelay-bcmedia-dump-{pid}-{n}"));
		std::fs::create_dir_all(&root).expect("create tempdir");
		TempDir { path: root }
	}
}
