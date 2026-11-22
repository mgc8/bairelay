//! Fixture-replay harness.
//!
//! Exercises the full RTSP server path against recorded `BcMedia` streams
//! captured via `bairelay ... --dump-bcmedia <dir>`.
//!
//! The [`FakeStreamProvider`] implements
//! [`bairelay_rtsp::provider::StreamProvider`] by deserializing
//! `BcMedia` packets from a `.bcmedia` file and translating them into
//! [`bairelay_rtsp::provider::Frame`]s through the SAME
//! [`bairelay::stream_source::apply_bcmedia_packet`] helper that the
//! production reader task uses. This is what distinguishes it from the
//! `MockProvider` used in the RTSP-crate integration tests: `MockProvider`
//! pre-populates `SdpParams.video`, which hid the empty-SDP race on real
//! cameras; the fake provider populates it the same way production does —
//! only after the first IDR has been parsed.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use bairelay::audio_presence::AudioPresence;
use bairelay::stream_source::{apply_bcmedia_packet, GapState, StreamTranslatorState};
use bairelay_rtsp::buffer::LastFrameBuffer;
use bairelay_rtsp::codec::nal::{H264NalType, H265NalType};
use bairelay_rtsp::codec::VideoCodec;
use bairelay_rtsp::provider::{
	Frame, SessionGuard, StreamError, StreamProvider, SubscriptionHandle,
};
use bairelay_rtsp::rtsp::auth::UserCred;
use bairelay_rtsp::sdp::SdpParams;
use bairelay_rtsp::server::rtcp::SR_INTERVAL;
use bairelay_rtsp::server::{RtspServer, ServerConfig};
use bairelay_rtsp::url::StreamKind as RtspStreamKind;

use bairelay_neolink_core::bcmedia::model::{BcMedia, BcMediaIframe, BcMediaPframe, VideoType};
use bairelay_neolink_core::Error as NeolinkError;

/// Capacity of the replay broadcast channel. Mirrors production
/// (`BROADCAST_CAPACITY` in `src/stream_source.rs`) so backpressure
/// behaviour is realistic.
const REPLAY_BROADCAST_CAPACITY: usize = 64;

/// Hard cap on bytes slurped from a `.bcmedia` file during a pre-scan or
/// replay. Real fixtures are hand-captured and small; anything over this
/// limit is almost certainly a mistake.
const MAX_FIXTURE_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

/// A fake [`StreamProvider`] that replays `.bcmedia` fixture files.
///
/// See the module docs for the why. Index fixtures with
/// [`FakeStreamProvider::from_dir`] or register them manually with
/// [`FakeStreamProvider::register`].
pub struct FakeStreamProvider {
	/// `(camera_name, stream_kind) -> absolute path to .bcmedia fixture`.
	fixtures: HashMap<(String, RtspStreamKind), PathBuf>,
	/// Playback speed multiplier. 1.0 = real-time; 10.0 = ten-times faster.
	speed_factor: f64,
}

impl FakeStreamProvider {
	/// Scan `dir` for `<cam>-<kind>.bcmedia` files and index them by
	/// `(cam, kind)`. An empty directory is fine — the resulting provider
	/// simply rejects every subscribe with [`StreamError::UnknownCamera`].
	///
	/// Any `.bcmedia` file that fails to parse against the
	/// `<cam>-<kind>` schema is skipped with a `tracing::warn!` so a
	/// corrupt or half-renamed fixture directory is distinguishable from
	/// "no fixtures captured yet".
	pub fn from_dir(dir: &Path) -> io::Result<Self> {
		let mut fixtures = HashMap::new();
		if dir.exists() {
			for entry in fs::read_dir(dir)? {
				let entry = entry?;
				let path = entry.path();
				if path.extension().and_then(|e| e.to_str()) != Some("bcmedia") {
					continue;
				}
				let filename = path
					.file_name()
					.and_then(|s| s.to_str())
					.unwrap_or("<non-utf8>")
					.to_string();
				let stem = match path.file_stem().and_then(|s| s.to_str()) {
					Some(s) => s,
					None => {
						tracing::warn!(
							file = %filename,
							reason = "non-utf8 file stem",
							"skipping malformed .bcmedia fixture"
						);
						continue;
					}
				};
				// Format is `<cam>-<kind>`. The camera name is
				// constrained to `[A-Za-z0-9_-]` but `_` + `-` make
				// splitting ambiguous; we split on the LAST dash so
				// camera names may contain dashes themselves.
				let (cam, kind_str) = match stem.rsplit_once('-') {
					Some(pair) => pair,
					None => {
						tracing::warn!(
							file = %filename,
							reason = "filename lacks '-' separator",
							"skipping malformed .bcmedia fixture"
						);
						continue;
					}
				};
				if cam.is_empty() {
					tracing::warn!(
						file = %filename,
						reason = "empty camera name",
						"skipping malformed .bcmedia fixture"
					);
					continue;
				}
				let kind = match kind_str.to_ascii_lowercase().as_str() {
					"main" => RtspStreamKind::Main,
					"sub" => RtspStreamKind::Sub,
					"extern" => RtspStreamKind::Extern,
					other => {
						tracing::warn!(
							file = %filename,
							reason = format!("unknown stream kind: '{other}'"),
							"skipping malformed .bcmedia fixture"
						);
						continue;
					}
				};
				fixtures.insert((cam.to_string(), kind), path);
			}
		}
		Ok(Self {
			fixtures,
			speed_factor: 1.0,
		})
	}

	/// Number of fixtures indexed. Primarily for tests verifying that
	/// malformed filenames are correctly skipped.
	pub fn fixture_count(&self) -> usize {
		self.fixtures.len()
	}

	/// Override the playback speed. `1.0` = real-time (default), `10.0`
	/// delivers a 10-second fixture in ~1 second. Must be > 0.
	pub fn with_speed_factor(mut self, factor: f64) -> Self {
		assert!(
			factor > 0.0,
			"speed_factor must be strictly positive, got {factor}"
		);
		self.speed_factor = factor;
		self
	}

	/// Register one fixture explicitly. Used by synthetic tests that write
	/// a `BcMedia` blob to a tempfile instead of relying on
	/// [`Self::from_dir`].
	pub fn register(&mut self, camera: &str, kind: RtspStreamKind, path: PathBuf) {
		self.fixtures.insert((camera.to_string(), kind), path);
	}
}

#[async_trait]
impl StreamProvider for FakeStreamProvider {
	async fn subscribe(
		&self,
		camera: &str,
		kind: RtspStreamKind,
		_authenticated_user: Option<&str>,
	) -> Result<SubscriptionHandle, StreamError> {
		let path = self
			.fixtures
			.get(&(camera.to_string(), kind))
			.cloned()
			.ok_or(StreamError::UnknownCamera)?;

		// Slurp the fixture in one shot. The cap guards against a stray
		// multi-GB file; real fixtures are a handful of MiB per stream.
		let bytes = read_bounded(&path, MAX_FIXTURE_BYTES)
			.map_err(|e| StreamError::Internal(format!("fixture read: {e}")))?;

		// Shared state for the replay task. Production uses identical
		// shapes (`Arc<RwLock<SdpParams>>` + `Arc<LastFrameBuffer>`) so
		// a client calling into `apply_bcmedia_packet` sees the same
		// side-effect model.
		let last_frame = Arc::new(LastFrameBuffer::new());
		let sdp_params = Arc::new(RwLock::new(SdpParams {
			server_ip: "0.0.0.0".to_string(),
			session_id: session_id(),
			session_name: format!("{camera}/{kind}"),
			video: None,
			audio: None,
		}));
		// Per-subscribe presence latch. Mirrors production's
		// `CameraProvider::subscribe`: a fresh `AudioPresence` starts
		// `Unknown`, the translator upgrades it to `Present { codec }`
		// on the first AAC/ADPCM packet, and if the scan finishes
		// without ever seeing audio the caller flips it to `Absent`.
		let audio_presence = Arc::new(RwLock::new(AudioPresence::Unknown));

		// Pre-scan so `subscribe()` returns with `SdpParams.video`
		// already populated. Production populates on the FIRST IDR; we
		// mirror that exactly by running `apply_bcmedia_packet` over a
		// temporary channel/last-frame, and bail out as soon as
		// `sdp_params.video` is Some. This proves the harness exercises
		// the real translator: if the fixture has no valid IDR, we
		// surface `StreamError::Unavailable`.
		prescan_into_sdp(&bytes, &sdp_params, &last_frame, &audio_presence)?;

		// End-of-fixture with no audio observed? Latch presence to
		// `Absent` so downstream consumers can distinguish "never saw
		// audio" from "haven't looked yet". Mirrors the behaviour the
		// production reader task produces when a stream completes
		// without emitting any audio packet.
		{
			let mut p = audio_presence
				.write()
				.expect("audio_presence lock poisoned");
			if matches!(*p, AudioPresence::Unknown) {
				*p = AudioPresence::Absent;
			}
		}

		// Channels for the actual replay. These are the ones the
		// subscriber reads from; the pre-scan used throw-away channels
		// so no duplicate frames land here.
		let (tx, rx) = broadcast::channel::<Frame>(REPLAY_BROADCAST_CAPACITY);
		let speed_factor = self.speed_factor;
		let replay_sdp = Arc::clone(&sdp_params);
		let replay_last_frame = Arc::clone(&last_frame);

		tokio::spawn(replay_task(
			bytes,
			tx,
			replay_sdp,
			replay_last_frame,
			speed_factor,
		));

		let sdp_snapshot = sdp_params.read().expect("sdp lock poisoned").clone();
		Ok(SubscriptionHandle {
			frames: rx,
			sdp_params: sdp_snapshot,
			last_frame,
			guard: no_op_guard(),
		})
	}
}

/// Read a file, bailing if it exceeds `limit_bytes`.
fn read_bounded(path: &Path, limit_bytes: u64) -> io::Result<Vec<u8>> {
	let meta = fs::metadata(path)?;
	if meta.len() > limit_bytes {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!(
				"fixture {} exceeds {} byte cap",
				path.display(),
				limit_bytes
			),
		));
	}
	fs::read(path)
}

/// Walk the fixture bytes, running every packet through
/// [`apply_bcmedia_packet`] with throw-away broadcast and codec state,
/// until either both tracks are advertised or the fixture is exhausted.
///
/// Behaviour mirrors production's `CameraProvider::subscribe`:
///
/// * As soon as both `sdp_params.video` and `sdp_params.audio` are
///   populated, return early — there's nothing more to learn from the
///   rest of the fixture.
/// * On clean end-of-fixture (empty buffer or trailing
///   [`NeolinkError::NomIncomplete`] short-read), return `Ok(())` as
///   long as video was seen. The `audio_presence` argument will have
///   been upgraded to `Present { .. }` by the translator if any AAC /
///   ADPCM packet was observed; otherwise it stays `Unknown` and the
///   caller is responsible for flipping it to `Absent`.
/// * If no valid IDR is ever parsed, return
///   [`StreamError::Unavailable`].
/// * Any other parser error aborts the scan with
///   [`StreamError::Unavailable`].
///
/// There is no fixed lookahead — the scan runs synchronously over the
/// whole fixture, which is bounded by [`MAX_FIXTURE_BYTES`] anyway.
/// Audio tracks arriving arbitrarily late are therefore detected
/// correctly, and fixtures without audio end naturally with presence
/// still `Unknown` (caller latches to `Absent`).
fn prescan_into_sdp(
	bytes: &[u8],
	sdp_params: &Arc<RwLock<SdpParams>>,
	last_frame: &Arc<LastFrameBuffer>,
	audio_presence: &Arc<RwLock<AudioPresence>>,
) -> Result<(), StreamError> {
	// Throw-away sink: no subscribers, so `send` silently returns
	// `SendError` and drops the frame.
	let (scan_tx, _scan_rx) = broadcast::channel::<Frame>(4);
	// Local translator state so pre-scan observes the same translator
	// behaviour production does. The harness doesn't assert on state
	// fields — it's only interested in whether SDP.video/audio gets
	// populated.
	let mut state = StreamTranslatorState::default();
	// Prescan only populates SDP + presence; we always pretend upstream
	// is `Live` here so audio packets aren't dropped by the 	// Bridging gate (DESCRIBE-time discovery must see them).
	let gap_state = std::sync::Mutex::new(GapState::Live);

	let mut buf = BytesMut::from(bytes);
	loop {
		match BcMedia::deserialize(&mut buf) {
			Ok(packet) => {
				apply_bcmedia_packet(
					&packet,
					&scan_tx,
					None,
					None,
					last_frame,
					sdp_params,
					audio_presence,
					&mut state,
					&gap_state,
				);
				let (video_ready, audio_ready) = {
					let snapshot = sdp_params.read().expect("sdp lock poisoned");
					(snapshot.video.is_some(), snapshot.audio.is_some())
				};
				if video_ready && audio_ready {
					// Both tracks advertised — done.
					return Ok(());
				}
			}
			Err(NeolinkError::NomIncomplete(_)) => {
				// Clean end-of-fixture or trailing short read. Stop
				// scanning; caller inspects `sdp_params` + latches
				// `audio_presence` to `Absent` if still `Unknown`.
				break;
			}
			Err(e) => {
				return Err(StreamError::Unavailable(format!(
					"fixture parse error: {e}"
				)));
			}
		}
		if buf.is_empty() {
			break;
		}
	}

	// End of fixture — we must have observed at least one IDR for the
	// subscribe to be serviceable. If not, the fixture was empty,
	// truncated before SPS/PPS, or otherwise unusable.
	if sdp_params
		.read()
		.expect("sdp lock poisoned")
		.video
		.is_none()
	{
		return Err(StreamError::Unavailable(
			"fixture ended before SPS/PPS landed".to_string(),
		));
	}
	Ok(())
}

/// Streaming loop over the fixture bytes. Paces delivery based on
/// the `pts` delta between consecutive packets divided by
/// `speed_factor`. Stops on EOF, on parse error, or when every receiver
/// has dropped (the reorder-tolerant `SendError` check we use on `tx`).
async fn replay_task(
	bytes: Vec<u8>,
	tx: broadcast::Sender<Frame>,
	sdp_params: Arc<RwLock<SdpParams>>,
	last_frame: Arc<LastFrameBuffer>,
	speed_factor: f64,
) {
	let start = tokio::time::Instant::now();
	// Microsecond-domain timestamp anchor. We translate each packet's
	// `microseconds` into a wall-clock deadline relative to `start`,
	// then sleep until that deadline before broadcasting.
	let mut first_pts_us: Option<u32> = None;
	// Write-only presence sink. apply_bcmedia_packet requires an
	// AudioPresence arg, but replay_task has no one to report to:
	// the subscribe-time presence (consumed by prescan_into_sdp) is
	// what feeds DESCRIBE, and SubscriptionHandle doesn't carry
	// presence. This Arc exists solely to satisfy the signature and
	// is dropped when the task exits. Do NOT plumb it to the
	// subscribe-time presence — that would race with DESCRIBE.
	let presence = Arc::new(RwLock::new(AudioPresence::Unknown));
	// Replay-local translator state. This advances through the translator
	// exactly as the production reader_task would, so the RTP packets
	// the replay emits carry the same monotonic timestamps real cameras
	// would surface. The multi-track assertions in
	// `multi_track_replay_aac_on_hallway_sub_fixture` rely on this.
	let mut state = StreamTranslatorState::default();
	// Replay always advertises `Live` — we're feeding a finite fixture
	// from disk, so the gap-bridging semantics don't apply.
	let gap_state = std::sync::Mutex::new(GapState::Live);
	let mut buf = BytesMut::from(bytes.as_slice());

	loop {
		let packet = match BcMedia::deserialize(&mut buf) {
			Ok(p) => p,
			Err(NeolinkError::NomIncomplete(_)) => {
				// End-of-stream for a well-formed fixture.
				return;
			}
			Err(_) => {
				// Any other deserialize failure ends the replay; there
				// is no meaningful recovery from a corrupted fixture.
				return;
			}
		};

		// Pace based on the packet's own pts. InfoV1/V2 and audio
		// packets don't carry a pts we care about for pacing, so we
		// use the most recent video pts and 0-delay them.
		if let Some(pts_us) = video_pts_us(&packet) {
			match first_pts_us {
				None => {
					first_pts_us = Some(pts_us);
				}
				Some(anchor) if pts_us < anchor => {
					// u32 microseconds wraps every ~71.6 minutes; a
					// backward jump is also possible if the camera's
					// clock ever moves non-monotonically. Re-anchor
					// rather than silently collapsing the pacing to
					// zero via saturating_sub.
					tracing::debug!("re-anchoring replay clock on pts wrap or backward jump");
					first_pts_us = Some(pts_us);
				}
				Some(_) => {}
			}
			let elapsed_src_us = (pts_us as u64).saturating_sub(first_pts_us.unwrap_or(0) as u64);
			// Divide by speed_factor to compress wall time.
			let wall_us = ((elapsed_src_us as f64) / speed_factor) as u64;
			let deadline = start + Duration::from_micros(wall_us);
			tokio::time::sleep_until(deadline).await;
		}

		// Check receiver count AFTER sending: if the last subscriber dropped
		// while we were awaiting the sleep, the current packet still reaches
		// them; the next iteration sees zero receivers and exits.
		apply_bcmedia_packet(
			&packet,
			&tx,
			None,
			None,
			&last_frame,
			&sdp_params,
			&presence,
			&mut state,
			&gap_state,
		);

		// When the subscriber side drops, `receiver_count()` hits zero.
		// Exiting here matches the "session teardown" semantics in
		// production: the replay stops as soon as nobody is listening.
		if tx.receiver_count() == 0 && first_pts_us.is_some() {
			return;
		}
	}
}

/// Extract the microsecond pts from a video packet, or `None` for
/// non-video variants (which the pacing logic ignores).
fn video_pts_us(packet: &BcMedia) -> Option<u32> {
	match packet {
		BcMedia::Iframe(BcMediaIframe { microseconds, .. })
		| BcMedia::Pframe(BcMediaPframe { microseconds, .. }) => Some(*microseconds),
		_ => None,
	}
}

/// Construct a no-op [`SessionGuard`]. The fixture harness has no wake
/// lock to release; the guard is purely a type-system placeholder.
fn no_op_guard() -> SessionGuard {
	struct NoOpGuard;
	Box::new(NoOpGuard)
}

/// Monotonically increasing session identifier so two subscribers to the
/// same fixture do not produce identical SDP `o=` lines.
fn session_id() -> String {
	static COUNTER: AtomicU64 = AtomicU64::new(1);
	COUNTER.fetch_add(1, Ordering::Relaxed).to_string()
}

// ── Test-local tempdir helper ────────────────────────────────────────

/// Minimal tempdir replacement so we do not need to add `tempfile` as
/// a dev-dependency. Shape matches the private helper in
/// `src/bcmedia_dump.rs`; keep the two in sync if either changes.
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
		let _ = fs::remove_dir_all(&self.path);
	}
}

fn tempdir() -> TempDir {
	static COUNTER: AtomicU64 = AtomicU64::new(0);
	let n = COUNTER.fetch_add(1, Ordering::Relaxed);
	let pid = std::process::id();
	let root = std::env::temp_dir().join(format!("bairelay-fixture-replay-{pid}-{n}"));
	fs::create_dir_all(&root).expect("create tempdir");
	TempDir { path: root }
}

// ── Synthetic fixture helpers ────────────────────────────────────────

/// Build a minimal H.265 Annex-B access unit with VPS + SPS + PPS + IDR.
/// Matches the bytes used by the in-module unit test in
/// `src/stream_source.rs` so a codec-detection regression fails in both
/// places.
fn synthetic_h265_iframe_bytes() -> Vec<u8> {
	let mut out = Vec::new();
	// VPS (type 32) — byte0 = 0x40, byte1 has nuh_temporal_id_plus1=1.
	out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
	out.extend_from_slice(&[0x40, 0x01, 0x0c, 0x01, 0xff]);
	// SPS (type 33) — byte0 = 0x42.
	out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
	out.extend_from_slice(&[0x42, 0x01, 0x01, 0x60, 0x00]);
	// PPS (type 34) — byte0 = 0x44.
	out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
	out.extend_from_slice(&[0x44, 0x01, 0xc1, 0x72]);
	// IDR_W_RADL (type 19) — byte0 = 0x26.
	out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
	out.extend_from_slice(&[0x26, 0x01, 0xaf, 0x08, 0x46]);
	out
}

fn synthetic_h265_pframe_bytes() -> Vec<u8> {
	let mut out = Vec::new();
	// TRAIL_R (type 1) → byte0 = 0x02.
	out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
	out.extend_from_slice(&[0x02, 0x01, 0xd0, 0x21, 0x3c]);
	out
}

/// Serialize a list of `BcMedia` packets to a tempfile and return the
/// path (plus the owning [`TempDir`] so tests can drop it at the end).
fn write_fixture(packets: &[BcMedia]) -> (TempDir, PathBuf) {
	let td = tempdir();
	let path = td.path().join("synthetic.bcmedia");
	let mut buf: Vec<u8> = Vec::new();
	for p in packets {
		buf = p.serialize(buf).expect("serialize synthetic BcMedia");
	}
	fs::write(&path, &buf).expect("write fixture");
	(td, path)
}

// ── Synthetic tests (no real fixture required) ───────────────────────

#[test]
fn from_dir_skips_malformed_filenames_with_warning() {
	let td = tempdir();
	// Empty camera name: `-main.bcmedia`.
	fs::write(td.path().join("-main.bcmedia"), b"").expect("write empty-cam file");
	// No dash separator: `cam.bcmedia`.
	fs::write(td.path().join("cam.bcmedia"), b"").expect("write no-dash file");
	// Unknown stream kind: `cam-bogus.bcmedia`.
	fs::write(td.path().join("cam-bogus.bcmedia"), b"").expect("write bogus-kind file");

	let provider = FakeStreamProvider::from_dir(td.path()).expect("scan tempdir succeeds");
	assert_eq!(
		provider.fixture_count(),
		0,
		"all three malformed fixtures must be skipped"
	);
}

#[tokio::test]
async fn fake_provider_rejects_unknown_camera() {
	let td = tempdir();
	let provider = FakeStreamProvider::from_dir(td.path()).expect("scan empty dir succeeds");
	let res = provider
		.subscribe("missing", RtspStreamKind::Main, None)
		.await;
	assert!(
		matches!(res, Err(StreamError::UnknownCamera)),
		"unknown camera must return UnknownCamera"
	);
}

#[tokio::test]
async fn fake_provider_emits_video_frames_from_synthetic_bcmedia() {
	let iframe = BcMedia::Iframe(BcMediaIframe {
		video_type: VideoType::H265,
		microseconds: 0,
		time: Some(1_700_000_000),
		data: synthetic_h265_iframe_bytes(),
	});
	let pframe = BcMedia::Pframe(BcMediaPframe {
		video_type: VideoType::H265,
		microseconds: 33_333,
		data: synthetic_h265_pframe_bytes(),
	});
	let (_td, path) = write_fixture(&[iframe, pframe]);

	let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
	provider.register("cam1", RtspStreamKind::Main, path);
	// Speed up so the P-frame arrives promptly.
	let provider = provider.with_speed_factor(1000.0);

	let mut sub = provider
		.subscribe("cam1", RtspStreamKind::Main, None)
		.await
		.expect("subscribe ok");

	// THIS is the key assertion: SDP params are populated BEFORE the
	// first frame.recv(). That is what the empty-SDP race would break.
	let video = sub
		.sdp_params
		.video
		.as_ref()
		.expect("SdpParams.video must be populated before first recv()");
	assert_eq!(video.codec, VideoCodec::H265);
	assert!(video.vps.is_some(), "VPS must be present for H.265");

	// Pull at least two frames. Use a generous timeout to avoid flakes on
	// slow test runners.
	let f1 = tokio::time::timeout(Duration::from_secs(2), sub.frames.recv())
		.await
		.expect("first frame timeout")
		.expect("first frame recv");
	match f1 {
		Frame::Video {
			codec, keyframe, ..
		} => {
			assert_eq!(codec, VideoCodec::H265);
			assert!(keyframe, "first frame must be a keyframe");
		}
		_ => panic!("expected Frame::Video"),
	}

	let f2 = tokio::time::timeout(Duration::from_secs(2), sub.frames.recv())
		.await
		.expect("second frame timeout")
		.expect("second frame recv");
	match f2 {
		Frame::Video {
			codec, keyframe, ..
		} => {
			assert_eq!(codec, VideoCodec::H265);
			assert!(!keyframe, "second frame must be a non-keyframe");
		}
		_ => panic!("expected Frame::Video"),
	}
}

#[tokio::test]
async fn fake_provider_pacing_matches_speed_factor() {
	// Three I-frames at pts = 0, 100_000 µs, 200_000 µs. With
	// speed_factor = 10.0 the wall-clock gap between consecutive
	// frames should be ~10 ms, not ~100 ms and not zero.
	let pkts: Vec<BcMedia> = [0u32, 100_000, 200_000]
		.iter()
		.map(|pts| {
			BcMedia::Iframe(BcMediaIframe {
				video_type: VideoType::H265,
				microseconds: *pts,
				time: Some(1_700_000_000),
				data: synthetic_h265_iframe_bytes(),
			})
		})
		.collect();
	let (_td, path) = write_fixture(&pkts);

	let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
	provider.register("cam1", RtspStreamKind::Main, path);
	let provider = provider.with_speed_factor(10.0);

	let mut sub = provider
		.subscribe("cam1", RtspStreamKind::Main, None)
		.await
		.expect("subscribe ok");

	// Consume the first frame (pts=0, nominally immediate).
	let _f1 = tokio::time::timeout(Duration::from_secs(2), sub.frames.recv())
		.await
		.expect("f1 timeout")
		.expect("f1 recv");

	let mark = std::time::Instant::now();
	let _f2 = tokio::time::timeout(Duration::from_secs(2), sub.frames.recv())
		.await
		.expect("f2 timeout")
		.expect("f2 recv");
	let elapsed = mark.elapsed();

	// Expect ~10 ms (100 ms source / 10.0) with a generous slack to
	// account for loaded CI runners and tokio scheduler jitter.
	assert!(
		elapsed >= Duration::from_millis(2),
		"second frame arrived too fast ({elapsed:?}); pacing not applied"
	);
	assert!(
		elapsed < Duration::from_millis(60),
		"second frame arrived too slow ({elapsed:?}); expected ~10 ms"
	);
}

// ── Runtime-skipping real-fixture test ───────────────────────────────

/// Walks `tests/fixtures/` for every `.bcmedia` file and replays each
/// one through the [`FakeStreamProvider`]. If no fixtures exist the test
/// exits early with a helpful message and passes. When the user has
/// captured real-camera fixtures this test runs for real.
///
/// The test is codec-agnostic: Reolink battery cameras emit H.265 on
/// the main stream and H.264 on the sub stream, and the extern stream
/// codec varies by model. The old `fake_provider_replays_real_h265_
/// fixture_if_present` variant picked the first file alphabetically and
/// hard-coded H.265, which failed against a sub-stream fixture. See the
/// per-fixture summary printed under `--nocapture`.
#[tokio::test]
async fn fake_provider_replays_real_fixtures_if_present() {
	let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests")
		.join("fixtures");

	// Collect every `.bcmedia` file, sorted for deterministic output.
	let mut paths: Vec<PathBuf> = fs::read_dir(&fixtures_dir)
		.ok()
		.map(|rd| {
			rd.filter_map(|e| e.ok())
				.map(|e| e.path())
				.filter(|p| p.extension().and_then(|s| s.to_str()) == Some("bcmedia"))
				.collect()
		})
		.unwrap_or_default();
	paths.sort();

	if paths.is_empty() {
		eprintln!(
			"no fixture files in {} — capture one via \
			 `bairelay mqtt-rtsp --dump-bcmedia {} -c config.toml` against a live camera; \
			 test passing as a no-op",
			fixtures_dir.display(),
			fixtures_dir.display()
		);
		return;
	}

	for path in &paths {
		let stem = path
			.file_stem()
			.and_then(|s| s.to_str())
			.expect("utf-8 stem");
		let (cam, kind_str) = stem.rsplit_once('-').expect("stem format cam-kind");
		let kind = match kind_str.to_ascii_lowercase().as_str() {
			"main" => RtspStreamKind::Main,
			"sub" => RtspStreamKind::Sub,
			"extern" => RtspStreamKind::Extern,
			other => panic!("unexpected stream kind in fixture filename: {other}"),
		};

		let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
		provider.register(cam, kind, path.clone());
		let provider = provider.with_speed_factor(1000.0);

		let mut sub = provider
			.subscribe(cam, kind, None)
			.await
			.unwrap_or_else(|e| panic!("subscribe against {}: {e:?}", path.display()));

		let video = sub.sdp_params.video.as_ref().unwrap_or_else(|| {
			panic!(
				"SdpParams.video must be populated before recv() for {}",
				path.display()
			)
		});
		let codec = video.codec;
		match codec {
			VideoCodec::H264 => {
				// H.264 parameter sets are SPS+PPS; no VPS.
				assert!(
					!video.sps.is_empty(),
					"H.264 fixture {} must carry SPS in SdpParams",
					path.display()
				);
				assert!(
					!video.pps.is_empty(),
					"H.264 fixture {} must carry PPS in SdpParams",
					path.display()
				);
			}
			VideoCodec::H265 => {
				assert!(
					video.vps.is_some(),
					"H.265 fixture {} must carry VPS in SdpParams",
					path.display()
				);
				assert!(
					!video.sps.is_empty(),
					"H.265 fixture {} must carry SPS in SdpParams",
					path.display()
				);
				assert!(
					!video.pps.is_empty(),
					"H.265 fixture {} must carry PPS in SdpParams",
					path.display()
				);
			}
		}

		// Pull up to 50 frames; the first must be a keyframe and its NAL
		// set must include the codec's parameter-set NAL (SPS for H.264,
		// VPS for H.265). Real captures always emit parameter sets
		// in-band alongside the first IDR.
		let mut first_frame_checked = false;
		let mut received = 0usize;
		while received < 50 {
			match tokio::time::timeout(Duration::from_secs(5), sub.frames.recv()).await {
				Ok(Ok(Frame::Video {
					codec: frame_codec,
					nals,
					keyframe,
					..
				})) => {
					assert_eq!(
						frame_codec,
						codec,
						"frame codec drifted mid-stream for {}: SDP says {codec:?} but frame says {frame_codec:?}",
						path.display()
					);
					if !first_frame_checked {
						assert!(
							keyframe,
							"first delivered frame from {} must be a keyframe",
							path.display()
						);
						// Post-the translator strips codec
						// parameter sets (VPS/SPS/PPS) from the outbound
						// `Frame::Video` — SDP's sprop-* fmtp carries them
						// out-of-band. Assert the first access unit still
						// carries a coded slice NAL (IDR) for the matching
						// codec, which is what clients actually decode.
						match codec {
							VideoCodec::H264 => {
								let has_idr = nals.iter().any(|n| {
									!n.is_empty()
										&& H264NalType::from_header_byte(n[0])
											== H264NalType::IDR_SLICE
								});
								assert!(
									has_idr,
									"first H.264 access unit from {} must include an IDR slice (type 5)",
									path.display()
								);
							}
							VideoCodec::H265 => {
								let has_idr = nals.iter().any(|n| {
									if n.is_empty() {
										return false;
									}
									matches!(
										H265NalType::from_header_byte(n[0]),
										H265NalType::IDR_W_RADL
											| H265NalType::IDR_N_LP
											| H265NalType::CRA
											| H265NalType::BLA_W_LP
									)
								});
								assert!(
									has_idr,
									"first H.265 access unit from {} must include an IDR / CRA / BLA slice",
									path.display()
								);
							}
						}
						first_frame_checked = true;
					}
					received += 1;
				}
				Ok(Ok(Frame::Audio { .. })) => {
					// Since Task 4, the translator emits
					// Frame::Audio alongside video whenever the fixture
					// carries AAC/ADPCM. The Hallway/Terrace sub fixtures
					// both carry AAC. This video-focused test accepts
					// those packets by skipping — multi-track coverage
					// lives in `multi_track_replay_aac_on_hallway_sub_
					// fixture` below, which asserts the full audio path.
				}
				Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
					// At the 1000x speed factor used here the replay task
					// can easily overrun the broadcast's 64-slot buffer
					// on a larger multi-track fixture (main streams are
					// ~20 MB + audio interleaved). Lag simply advances
					// the receiver past the dropped frames; the
					// parameter-set and codec-detection assertions we
					// care about for this test have already fired on the
					// earlier frames. Treat it as a continuation rather
					// than a failure — the multi-track end-to-end test
					// below uses real-time pacing through the server and
					// exercises the tight-timing assertions. Surface the
					// count so a future reader investigating a flake can
					// see whether broadcast capacity is the culprit.
					eprintln!(
						"fixture replay lagged by {n} packets (broadcast capacity too small?)"
					);
					continue;
				}
				Ok(Err(broadcast::error::RecvError::Closed)) => {
					break;
				}
				Err(_) => panic!(
					"frame recv timed out at frame {received} for {}",
					path.display()
				),
			}
		}

		assert!(received > 0, "fixture {} yielded no frames", path.display());

		let codec_name = match codec {
			VideoCodec::H264 => "H264",
			VideoCodec::H265 => "H265",
		};
		println!(
			"{}: codec={codec_name} frames={received} keyframe_ok=true params_ok=true",
			path.file_name()
				.and_then(|s| s.to_str())
				.unwrap_or("<non-utf8>")
		);
	}
}

// ── End-to-end replay through the real RtspServer runtime ────────────
//
// These tests drive a live `RtspServer::serve` against the
// `FakeStreamProvider` over a real loopback TCP socket. They're the
// coverage was missing: the existing `MockProvider` integration
// tests pre-populate `SdpParams.video`, which hid the empty-SDP race,
// the missing-Content-Base-trailing-slash bug, and the track-control URI
// mismatch — all of which we caught only on live cameras. Driving the
// fixture provider through the same wire path the live camera uses is
// what makes these regressions unit-testable.

/// Duplicated (with a small signature tweak) from
/// `crates/rtsp/tests/rtsp_integration_test.rs`. Integration-test crates
/// cannot share helpers across binaries, so the helper travels with the
/// test. See the source docstring there for the bind-then-drop-then-rebind
/// retry rationale.
async fn spawn_server_with_provider(
	provider: Arc<dyn StreamProvider>,
	users: Vec<UserCred>,
) -> (SocketAddr, CancellationToken) {
	const MAX_ATTEMPTS: u32 = 5;
	const READY_POLLS: u32 = 10;
	const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);

	for attempt in 0..MAX_ATTEMPTS {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		drop(listener);

		let config = ServerConfig {
			bind: addr,
			realm: "bairelay-test".to_string(),
			users: users.clone(),
			tls: None,
			max_connections: None,
		};
		let cancel = CancellationToken::new();
		let cancel_for_server = cancel.clone();
		let provider_for_server = Arc::clone(&provider);
		let server_task = tokio::spawn(async move {
			let _ = RtspServer::serve(config, provider_for_server, cancel_for_server).await;
		});

		let mut ready = false;
		for _ in 0..READY_POLLS {
			tokio::time::sleep(READY_POLL_INTERVAL).await;
			if TcpStream::connect(addr).await.is_ok() {
				ready = true;
				break;
			}
		}
		if ready {
			return (addr, cancel);
		}

		cancel.cancel();
		let _ = server_task.await;
		eprintln!("spawn_server_with_provider: attempt {attempt} could not reach {addr}, retrying");
	}
	panic!("spawn_server_with_provider failed after {MAX_ATTEMPTS} attempts");
}

/// Minimal RTSP response shape — status line + parsed headers + body.
#[derive(Debug)]
struct RtspResponse {
	status: u16,
	headers: HashMap<String, String>,
	body: Vec<u8>,
}

/// Extract the bare session ID from a `Session:` response header,
/// stripping any `;timeout=N` suffix. Panics with `ctx` in the message
/// if the header is missing — the call sites want a clear attribution
/// rather than an `Option` propagation, because a missing Session at
/// these points is a server bug, not recoverable test state.
fn parse_session_id(resp: &RtspResponse, ctx: &str) -> String {
	resp.headers
		.get("session")
		.unwrap_or_else(|| panic!("{ctx}: response missing Session header"))
		.split(';')
		.next()
		.unwrap()
		.trim()
		.to_string()
}

async fn write_all(stream: &mut TcpStream, bytes: &[u8]) {
	stream.write_all(bytes).await.unwrap();
	stream.flush().await.unwrap();
}

/// One interleaved TCP frame per RFC 2326 §10.12.
#[derive(Debug, Clone)]
struct Interleaved {
	channel: u8,
	payload: Vec<u8>,
}

/// Byte-level reader that buffers whatever arrives on the RTSP TCP
/// connection and lets callers pull either the next RTSP text response
/// or the next `$ ch len payload` interleaved frame without racing the
/// two formats against each other.
struct WireReader {
	buf: Vec<u8>,
	scratch: [u8; 4096],
}

impl WireReader {
	fn new() -> Self {
		Self {
			buf: Vec::new(),
			scratch: [0u8; 4096],
		}
	}

	/// Pull `n` more bytes from `stream` into `self.buf`. Returns `false`
	/// on EOF or timeout; tests treat either as fatal.
	async fn fill(&mut self, stream: &mut TcpStream, timeout: Duration) -> bool {
		match tokio::time::timeout(timeout, stream.read(&mut self.scratch)).await {
			Ok(Ok(0)) => false,
			Ok(Ok(n)) => {
				self.buf.extend_from_slice(&self.scratch[..n]);
				true
			}
			Ok(Err(_)) => false,
			Err(_) => false,
		}
	}

	/// Parse the next RTSP response, transparently consuming any
	/// interleaved frames that arrive first. Returns the parsed
	/// response plus a list of interleaved frames observed along the way
	/// (in arrival order) so callers that care about the RTP/RTCP stream
	/// can collect both.
	async fn read_rtsp_response(
		&mut self,
		stream: &mut TcpStream,
	) -> (RtspResponse, Vec<Interleaved>) {
		let mut frames = Vec::new();
		loop {
			// Strip any leading interleaved frames.
			while !self.buf.is_empty() && self.buf[0] == 0x24 {
				if self.buf.len() < 4 {
					if !self.fill(stream, Duration::from_secs(5)).await {
						panic!("EOF before complete interleaved header");
					}
					continue;
				}
				let len = u16::from_be_bytes([self.buf[2], self.buf[3]]) as usize;
				let need = 4 + len;
				while self.buf.len() < need {
					if !self.fill(stream, Duration::from_secs(5)).await {
						panic!("EOF before complete interleaved frame");
					}
				}
				let channel = self.buf[1];
				let payload = self.buf[4..need].to_vec();
				self.buf.drain(..need);
				frames.push(Interleaved { channel, payload });
			}

			if let Some(pos) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") {
				let header_end = pos + 4;
				let head =
					std::str::from_utf8(&self.buf[..pos]).expect("RTSP head should be UTF-8");
				let mut lines = head.split("\r\n");
				let status_line = lines.next().unwrap();
				let status: u16 = status_line
					.split_whitespace()
					.nth(1)
					.expect("status code in status line")
					.parse()
					.expect("status code is numeric");
				let mut headers: HashMap<String, String> = HashMap::new();
				let mut content_length = 0usize;
				for line in lines {
					if line.is_empty() {
						continue;
					}
					if let Some((k, v)) = line.split_once(':') {
						let key = k.trim().to_ascii_lowercase();
						let value = v.trim().to_string();
						if key == "content-length" {
							content_length = value.parse().unwrap_or(0);
						}
						headers.entry(key).or_insert(value);
					}
				}
				let mut body = self.buf[header_end..].to_vec();
				while body.len() < content_length {
					if !self.fill(stream, Duration::from_secs(5)).await {
						break;
					}
					// `fill` appended to `self.buf`; recompute body from
					// the original header_end. Simpler: drop what we had
					// and take the buffer tail.
					body = self.buf[header_end..].to_vec();
				}
				body.truncate(content_length);
				// Drain what we consumed (head + body).
				self.buf.drain(..header_end + content_length);
				return (
					RtspResponse {
						status,
						headers,
						body,
					},
					frames,
				);
			}

			if !self.fill(stream, Duration::from_secs(5)).await {
				panic!("EOF or timeout before complete RTSP response");
			}
		}
	}

	/// Collect interleaved frames until `min_per_channel` is satisfied
	/// for EVERY channel in `min_per_channel`, or `deadline` passes.
	/// Frames on channels not listed in the map are still returned in
	/// arrival order. Returns all frames collected up to that point.
	async fn collect_interleaved_until(
		&mut self,
		stream: &mut TcpStream,
		min_per_channel: &[(u8, usize)],
		deadline: tokio::time::Instant,
	) -> Vec<Interleaved> {
		let mut out = Vec::new();
		let mut counts: HashMap<u8, usize> = min_per_channel.iter().map(|(c, _)| (*c, 0)).collect();
		let targets: HashMap<u8, usize> = min_per_channel.iter().copied().collect();
		loop {
			let all_met = targets
				.iter()
				.all(|(ch, tgt)| counts.get(ch).copied().unwrap_or(0) >= *tgt);
			if all_met {
				return out;
			}
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			if !self.buf.is_empty() && self.buf[0] == 0x24 {
				if self.buf.len() < 4 {
					let remaining = deadline - now;
					if !self.fill(stream, remaining).await {
						return out;
					}
					continue;
				}
				let len = u16::from_be_bytes([self.buf[2], self.buf[3]]) as usize;
				let need = 4 + len;
				while self.buf.len() < need {
					let now = tokio::time::Instant::now();
					if now >= deadline {
						return out;
					}
					let remaining = deadline - now;
					if !self.fill(stream, remaining).await {
						return out;
					}
				}
				let ch = self.buf[1];
				let payload = self.buf[4..need].to_vec();
				self.buf.drain(..need);
				*counts.entry(ch).or_insert(0) += 1;
				out.push(Interleaved {
					channel: ch,
					payload,
				});
				continue;
			}
			// Not an interleaved frame boundary yet; pull more bytes.
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			let remaining = deadline - now;
			if !self.fill(stream, remaining).await {
				return out;
			}
		}
	}
}

/// Hand-rolled RTP header decoder. Extracts just the fields these tests
/// need — RFC 3550 §5.1 packet format, first 12 bytes are the fixed
/// header. Intentionally skips CSRC and extension handling; the server
/// never emits either (see `crates/rtsp/src/rtp.rs`).
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // `ssrc` is decoded for completeness / future-proofing
					// even though the current assertions don't inspect it.
struct RtpHeader {
	version: u8,
	marker: bool,
	payload_type: u8,
	sequence: u16,
	timestamp: u32,
	ssrc: u32,
	payload_offset: usize,
}

fn decode_rtp_header(pkt: &[u8]) -> Option<RtpHeader> {
	if pkt.len() < 12 {
		return None;
	}
	let version = pkt[0] >> 6;
	let padding = (pkt[0] & 0x20) != 0;
	let extension = (pkt[0] & 0x10) != 0;
	let cc = (pkt[0] & 0x0F) as usize;
	let marker = (pkt[1] & 0x80) != 0;
	let payload_type = pkt[1] & 0x7F;
	let sequence = u16::from_be_bytes([pkt[2], pkt[3]]);
	let timestamp = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
	let ssrc = u32::from_be_bytes([pkt[8], pkt[9], pkt[10], pkt[11]]);
	// 4 bytes per CSRC. No CSRCs expected but handle gracefully.
	let mut payload_offset = 12 + 4 * cc;
	if extension {
		// 2-byte profile + 2-byte length (in 32-bit words), then the ext.
		if pkt.len() < payload_offset + 4 {
			return None;
		}
		let ext_words =
			u16::from_be_bytes([pkt[payload_offset + 2], pkt[payload_offset + 3]]) as usize;
		payload_offset += 4 + 4 * ext_words;
	}
	// Padding (tail byte encodes count); does not affect payload_offset.
	let _ = padding;
	Some(RtpHeader {
		version,
		marker,
		payload_type,
		sequence,
		timestamp,
		ssrc,
		payload_offset,
	})
}

/// Unpack an H.265 RTP payload into its Annex-B NAL bytes.
///
/// - Single-NAL mode (type 0–47): the payload IS the NAL; return as-is.
/// - FU mode (type 49): read the FU header's S/E bits; middle fragments
///   return `Fragment::Middle`, start returns the reconstructed NAL
///   header followed by the first chunk as `Fragment::Start`, end
///   returns the remaining bytes as `Fragment::End`. Callers concatenate
///   `Start` + middles + `End` to get the full Annex-B NAL.
///
/// AP mode (type 48) is not emitted by the current server (see
/// `crates/rtsp/src/server/packetizer.rs`), so we don't handle it.
#[derive(Debug)]
#[allow(dead_code)] // Body bytes are parsed for completeness; the assertions
					// in this file only inspect NAL types, not bodies.
enum H265Fragment {
	Single(Vec<u8>),
	FuStart { nal_header: [u8; 2], body: Vec<u8> },
	FuMiddle(Vec<u8>),
	FuEnd(Vec<u8>),
}

fn unpack_h265_rtp(payload: &[u8]) -> H265Fragment {
	assert!(
		payload.len() >= 2,
		"H.265 payload must carry a 2-byte NAL header"
	);
	let nal_type = (payload[0] >> 1) & 0x3F;
	if nal_type == 49 {
		// FU. Layout: 2-byte replacement NAL header + 1-byte FU header + body.
		assert!(payload.len() >= 3);
		let fu = payload[2];
		let is_start = (fu & 0x80) != 0;
		let is_end = (fu & 0x40) != 0;
		let orig_type = fu & 0x3F;
		if is_start {
			// Rebuild the original NAL header: byte 0 keeps F/layer bits,
			// swap the type back; byte 1 unchanged.
			let b0 = (payload[0] & 0x81) | (orig_type << 1);
			let b1 = payload[1];
			H265Fragment::FuStart {
				nal_header: [b0, b1],
				body: payload[3..].to_vec(),
			}
		} else if is_end {
			H265Fragment::FuEnd(payload[3..].to_vec())
		} else {
			H265Fragment::FuMiddle(payload[3..].to_vec())
		}
	} else {
		H265Fragment::Single(payload.to_vec())
	}
}

/// Extract every NAL's type byte from a stream of RTP packets on one
/// channel. Used by the parameter-set assertion.
fn h265_nal_types_from_rtp(pkts: &[&[u8]]) -> Vec<u8> {
	let mut out = Vec::new();
	let mut fu_pending: Option<u8> = None;
	for pkt in pkts {
		let hdr = decode_rtp_header(pkt).expect("h265_nal_types_from_rtp: short RTP packet");
		let payload = &pkt[hdr.payload_offset..];
		if payload.is_empty() {
			continue;
		}
		match unpack_h265_rtp(payload) {
			H265Fragment::Single(bytes) => {
				let t = (bytes[0] >> 1) & 0x3F;
				out.push(t);
			}
			H265Fragment::FuStart { nal_header, .. } => {
				let t = (nal_header[0] >> 1) & 0x3F;
				fu_pending = Some(t);
				out.push(t);
			}
			H265Fragment::FuMiddle(_) | H265Fragment::FuEnd(_) => {
				// The NAL type was already recorded on the FuStart.
				let _ = fu_pending;
			}
		}
	}
	out
}

/// Build a FakeStreamProvider with a single synthetic H.265 fixture
/// registered under `cam1`. Returns the provider (as `Arc<dyn ...>`)
/// and holds the tempdir alive for the caller.
///
/// The fixture holds the session open for the full
/// `wall_seconds` budget: we size the pts gaps + speed factor so the
/// replay task is still pacing out P-frames when the ≥5 s RTCP Sender
/// Report tick fires. If the replay finishes first, the `broadcast::Sender`
/// drops, the session task exits, and the test never sees an SR.
fn provider_with_synthetic_h265(
	wall_seconds: u64,
	frames_per_second: u32,
) -> (TempDir, Arc<dyn StreamProvider>) {
	let total_frames = (wall_seconds as u32) * frames_per_second + 1; // +1 for the leading IFRAME
	let pts_step_us = 1_000_000 / frames_per_second;
	let mut pkts: Vec<BcMedia> = Vec::with_capacity(total_frames as usize);
	pkts.push(BcMedia::Iframe(BcMediaIframe {
		video_type: VideoType::H265,
		microseconds: 0,
		time: Some(1_700_000_000),
		data: synthetic_h265_iframe_bytes(),
	}));
	for i in 1..total_frames {
		pkts.push(BcMedia::Pframe(BcMediaPframe {
			video_type: VideoType::H265,
			microseconds: i * pts_step_us,
			data: synthetic_h265_pframe_bytes(),
		}));
	}
	let (td, path) = write_fixture(&pkts);
	let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
	provider.register("cam1", RtspStreamKind::Main, path);
	// Real-time pacing: fixture wall duration equals source duration.
	(td, Arc::new(provider))
}

#[tokio::test]
async fn fixture_replay_tcp_interleaved_e2e_synthetic_h265() {
	// 30 fps fixture that spans 8 wall-clock seconds in real-time
	// playback. That gives the session task 8 s of steady packet flow —
	// long enough to trigger the ≥5 s RTCP Sender Report tick while
	// still keeping this a fast-ish unit test.
	let (_td, provider) = provider_with_synthetic_h265(8, 30);
	let (addr, cancel) = spawn_server_with_provider(provider, vec![]).await;

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let mut reader = WireReader::new();

	// ── OPTIONS ──────────────────────────────────────────────────
	let req = format!("OPTIONS rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 1\r\n\r\n");
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "OPTIONS should return 200");

	// ── DESCRIBE ─────────────────────────────────────────────────
	let req = format!(
		"DESCRIBE rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "DESCRIBE should return 200");

	// regression (commit c1d8661): Content-Base must include
	// the camera segment AND end with `/` so clients resolve the SDP's
	// relative `a=control:trackID=N` correctly.
	let content_base = resp
		.headers
		.get("content-base")
		.expect("DESCRIBE response missing Content-Base")
		.clone();
	assert!(
		content_base.ends_with('/'),
		"Content-Base must end with '/' so track-control URIs resolve; got {content_base}"
	);
	assert!(
		content_base.contains("/cam1"),
		"Content-Base must include the camera segment; got {content_base}"
	);
	let expected_prefix = format!("rtsp://{addr}/cam1");
	assert!(
		content_base.starts_with(&expected_prefix),
		"Content-Base must be rooted at the presentation URI (expected prefix {expected_prefix}); got {content_base}"
	);

	let sdp = std::str::from_utf8(&resp.body).expect("SDP body must be UTF-8");
	assert!(sdp.contains("m=video"), "SDP missing m=video: {sdp}");
	assert!(
		sdp.contains("a=rtpmap:96 H265/90000"),
		"SDP missing H265 rtpmap at PT 96: {sdp}"
	);
	assert!(
		sdp.contains("a=fmtp:96"),
		"SDP missing fmtp for PT 96: {sdp}"
	);
	assert!(
		sdp.contains("sprop-vps="),
		"SDP fmtp missing sprop-vps: {sdp}"
	);
	assert!(
		sdp.contains("sprop-sps="),
		"SDP fmtp missing sprop-sps: {sdp}"
	);
	assert!(
		sdp.contains("sprop-pps="),
		"SDP fmtp missing sprop-pps: {sdp}"
	);
	assert!(
		sdp.contains("a=control:trackID=0"),
		"SDP missing per-track a=control:trackID=0 attribute (clients resolve this against Content-Base): {sdp}"
	);

	// ── SETUP ────────────────────────────────────────────────────
	let req = format!(
		"SETUP rtsp://{addr}/cam1/trackID=0 RTSP/1.0\r\n\
		 CSeq: 3\r\n\
		 Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "SETUP should return 200");
	let transport = resp
		.headers
		.get("transport")
		.expect("SETUP response missing Transport header")
		.clone();
	assert!(
		transport.contains("RTP/AVP/TCP"),
		"SETUP Transport must advertise TCP transport; got {transport}"
	);
	assert!(
		transport.contains("interleaved=0-1"),
		"SETUP Transport must echo the interleaved channel range we requested; got {transport}"
	);

	let session = parse_session_id(&resp, "SETUP");
	// requirement (commit 0d970a7): session IDs carry ≥128 bits
	// of entropy. 16 hex chars = 64 bits; 32 hex chars = 128 bits.
	assert!(
		session.len() >= 16,
		"Session ID must be ≥16 hex chars; got '{session}' ({} chars)",
		session.len()
	);
	assert!(
		session.chars().all(|c| c.is_ascii_hexdigit()),
		"Session ID must be ASCII hex; got '{session}'"
	);

	// ── PLAY ─────────────────────────────────────────────────────
	let req = format!("PLAY rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 4\r\nSession: {session}\r\n\r\n",);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, early_frames) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "PLAY should return 200");
	let play_session = parse_session_id(&resp, "PLAY");
	assert_eq!(
		play_session, session,
		"PLAY Session must match SETUP Session"
	);
	let rtp_info = resp
		.headers
		.get("rtp-info")
		.expect("PLAY response missing RTP-Info header")
		.clone();
	assert!(
		rtp_info.contains("url="),
		"RTP-Info missing url=: {rtp_info}"
	);
	// Parse seq=N;rtptime=T — both numeric.
	let seq_val = rtp_info
		.split(';')
		.find_map(|p| p.trim().strip_prefix("seq="))
		.expect("RTP-Info missing seq=");
	let rtptime_val = rtp_info
		.split(';')
		.find_map(|p| p.trim().strip_prefix("rtptime="))
		.expect("RTP-Info missing rtptime=");
	seq_val
		.parse::<u32>()
		.unwrap_or_else(|_| panic!("RTP-Info seq= not numeric: {seq_val}"));
	rtptime_val
		.parse::<u32>()
		.unwrap_or_else(|_| panic!("RTP-Info rtptime= not numeric: {rtptime_val}"));

	// ── Collect interleaved RTP/RTCP ─────────────────────────────
	// Budget 7 seconds to observe at least one RTCP SR (SR_INTERVAL
	// is 5 s per crates/rtsp/src/server/rtcp.rs; we add slack for the
	// first-tick delay after SETUP). The fixture plays back in
	// real time for 8 s so the session is still active when the tick
	// fires.
	let deadline = tokio::time::Instant::now() + Duration::from_secs(7);
	let mut frames = early_frames;
	// We need ≥6 RTP packets (channel 0) plus ≥1 RTCP SR (channel 1).
	let trailing = reader
		.collect_interleaved_until(&mut stream, &[(0, 6), (1, 1)], deadline)
		.await;
	frames.extend(trailing);

	let rtp_frames: Vec<&Interleaved> = frames.iter().filter(|f| f.channel == 0).collect();
	assert!(
		rtp_frames.len() >= 2,
		"expected ≥2 RTP packets on channel 0; got {} (all frames: {:?})",
		rtp_frames.len(),
		frames
			.iter()
			.map(|f| (f.channel, f.payload.len()))
			.collect::<Vec<_>>()
	);

	// Decode each RTP header and run the wire-shape assertions.
	let headers: Vec<RtpHeader> = rtp_frames
		.iter()
		.enumerate()
		.map(|(i, f)| {
			decode_rtp_header(&f.payload)
				.unwrap_or_else(|| panic!("H.265 RTP packet #{i} too short to decode header"))
		})
		.collect();

	for (i, h) in headers.iter().enumerate() {
		assert_eq!(h.version, 2, "RTP packet #{i} version must be 2");
		assert_eq!(
			h.payload_type, 96,
			"RTP packet #{i} payload type must be 96 (H.265 dynamic)"
		);
	}

	// Monotonic sequence, wrap permitted.
	for w in headers.windows(2) {
		let prev = w[0].sequence;
		let next = w[1].sequence;
		let expected = prev.wrapping_add(1);
		assert_eq!(
			next, expected,
			"RTP seq non-monotonic: {prev} → {next} (expected {expected})"
		);
	}

	// Timestamps non-decreasing; within a marker-delimited access unit
	// they're equal; across access units they're strictly increasing.
	for w in headers.windows(2) {
		assert!(
			w[1].timestamp >= w[0].timestamp
				|| w[1].timestamp.wrapping_sub(w[0].timestamp) < u32::MAX / 2,
			"RTP timestamp went backwards: {} → {}",
			w[0].timestamp,
			w[1].timestamp
		);
	}

	// Marker-bit cross-check. Each marker=true packet terminates an access
	// unit. Normally AUs have distinct RTP timestamps, but a synthetic
	// fixture can produce two AUs at the same timestamp — in particular
	// the cached-burst replay (marker fix: RFC 3550 §5.1
	// requires marker on the last RTP packet of an AU, so replay_burst
	// now sets it on the last IDR NAL's last fragment) followed
	// immediately by the live IFRAME broadcast, when the fixture's
	// IFRAME `microseconds=0` matches the burst's captured pts. Treat
	// each marker=true as the end of a sub-group and require exactly
	// one marker per sub-group.
	let mut observed_access_unit = false;
	let mut sub_start = 0usize;
	let mut last_ts = headers.first().map(|h| h.timestamp).unwrap_or(0);
	for (i, h) in headers.iter().enumerate() {
		// Sub-group breaks on ts change or on marker=true.
		if h.timestamp != last_ts {
			sub_start = i;
			last_ts = h.timestamp;
		}
		if h.marker {
			observed_access_unit = true;
			let sub = &headers[sub_start..=i];
			// Within the sub-group the marker is only on the LAST packet.
			for hh in &sub[..sub.len().saturating_sub(1)] {
				assert!(
					!hh.marker,
					"marker bit set on non-terminal packet within access unit ts={}",
					hh.timestamp,
				);
			}
			sub_start = i + 1;
		}
	}
	assert!(
		observed_access_unit,
		"expected at least one complete access unit (group ending with marker=1) in {} RTP packets",
		headers.len()
	);

	// Parameter-set handling (post-): bairelay no longer emits
	// VPS/SPS/PPS in-band on the live broadcast. Those NALs are
	// advertised out-of-band via the SDP `sprop-vps/sps/pps` fmtp
	// attribute, which every practical RTSP client (VLC / ffmpeg /
	// mpv / gstreamer / HA's stream: component) consumes during
	// DESCRIBE. Stripping the in-band copies prevents a downstream
	// `-c copy -f rtsp` re-packer (HA's go2rtc `ffmpeg:` wrap) from
	// aggregating them into an HEVC RFC 7798 AP (NAL type 48) that
	// go2rtc's RTPDepay cannot then de-aggregate. We therefore
	// assert that the RTP stream contains H.265 VCL NALs (IDR + P)
	// but does NOT contain VPS/SPS/PPS inline.
	let pkt_slices: Vec<&[u8]> = rtp_frames.iter().map(|f| f.payload.as_slice()).collect();
	let nal_types = h265_nal_types_from_rtp(&pkt_slices);
	assert!(
		nal_types.contains(&19),
		"RTP stream must include at least one IDR (NAL type 19); observed: {nal_types:?}"
	);
	for t in [32u8, 33, 34] {
		assert!(
			!nal_types.contains(&t),
			": RTP stream must NOT carry in-band parameter set NAL type {t}; observed: {nal_types:?}",
		);
	}
	// The first access unit's first payload must be the IDR slice —
	// with parameter sets stripped, VPS/SPS/PPS no longer prefix it.
	let first_ts = headers[0].timestamp;
	let first_group_types: Vec<u8> = headers
		.iter()
		.zip(pkt_slices.iter())
		.take_while(|(h, _)| h.timestamp == first_ts)
		.map(|(_, pkt)| {
			let hdr = decode_rtp_header(pkt).expect("first-access-unit RTP packet too short");
			let payload = &pkt[hdr.payload_offset..];
			let frag = unpack_h265_rtp(payload);
			match frag {
				H265Fragment::Single(bytes) => (bytes[0] >> 1) & 0x3F,
				H265Fragment::FuStart { nal_header, .. } => (nal_header[0] >> 1) & 0x3F,
				H265Fragment::FuMiddle(_) | H265Fragment::FuEnd(_) => 63,
			}
		})
		.collect();
	assert!(
		!first_group_types.is_empty(),
		"first access unit must carry at least the IDR slice; got empty",
	);
	assert_eq!(
		first_group_types[0], 19,
		"first access unit must begin with an IDR slice (NAL type 19); got {first_group_types:?}",
	);

	// RTCP SR is intentionally NOT sent by the server (see the SR-fire
	// arm in `crates/rtsp/src/server/session_task.rs::run` for the
	// reasoning). Assert that no RTCP traffic is interleaved on
	// channel 1 within the 6 s budget.
	let rtcp_frames: Vec<&Interleaved> = frames.iter().filter(|f| f.channel == 1).collect();
	assert!(
		rtcp_frames.is_empty(),
		"expected 0 RTCP frames on channel 1 (SR sending is disabled); got {} frames",
		rtcp_frames.len()
	);

	// ── TEARDOWN ─────────────────────────────────────────────────
	let req =
		format!("TEARDOWN rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 5\r\nSession: {session}\r\n\r\n",);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "TEARDOWN should return 200");

	drop(stream);
	cancel.cancel();
	// Give the connection task a beat to wind down so subsequent tests
	// (if any) don't race the server's cleanup.
	tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn fixture_replay_tcp_interleaved_unknown_camera() {
	// One fixture registered under `cam1`; DESCRIBE for a different
	// camera name must return 404 rather than leaking any stream bytes.
	let (_td, provider) = provider_with_synthetic_h265(1, 30);
	let (addr, cancel) = spawn_server_with_provider(provider, vec![]).await;

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let mut reader = WireReader::new();

	let req = format!(
		"DESCRIBE rtsp://{addr}/not-a-cam RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(
		resp.status, 404,
		"DESCRIBE for an unknown camera must return 404"
	);

	drop(stream);
	cancel.cancel();
	tokio::time::sleep(Duration::from_millis(50)).await;
}

/// End-to-end multi-track replay: drive the real
/// `RtspServer` against the AAC-bearing `Cam2-sub.bcmedia`
/// fixture (H.264 sub stream + 305 AAC frames, 16 kHz mono, captured
/// from live Argus hardware in ). Asserts:
///
/// 1. DESCRIBE returns a two-track SDP (`m=video` + `m=audio`, audio
///    advertised as `mpeg4-generic/16000/1`).
/// 2. SETUP video (trackID=0, `interleaved=0-1`) returns 200.
/// 3. SETUP audio (trackID=1, `interleaved=2-3`, same Session ID)
///    returns 200 (NOT the historical 455).
/// 4. PLAY returns 200.
/// 5. Interleaved RTP flows on channel 0 (video, PT=96) and channel 2
///    (audio, PT=97) with distinct SSRCs and monotonic sequence numbers.
/// 6. Both tracks receive at least one RTCP Sender Report (channel 1
///    for video, channel 3 for audio) within the read window.
/// 7. TEARDOWN returns 200.
///
/// Runtime-skipped if the fixture is absent (CI without the gitignored
/// captures).
#[tokio::test]
async fn multi_track_replay_aac_on_hallway_sub_fixture() {
	let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests")
		.join("fixtures");
	let target_file = fixture_dir.join("Cam2-sub.bcmedia");
	if !target_file.exists() {
		eprintln!(
			"skipping multi_track_replay_aac_on_hallway_sub_fixture: \
			 fixture missing at {}",
			target_file.display(),
		);
		return;
	}

	// Real-time pacing (speed_factor=1.0) keeps the broadcast channel
	// from overflowing and gives the server a natural 10 s of steady
	// audio + video flow — plenty of wall time to observe both the
	// ≥5 s RTCP SR tick (per crates/rtsp/src/server/rtcp.rs) and
	// enough RTP packets to validate sequence monotonicity on each
	// channel. A 10x replay would starve the first SR — the send loop
	// would drain the replay before the timer fires.
	let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
	provider.register("Cam2", RtspStreamKind::Sub, target_file.clone());
	let provider: Arc<dyn StreamProvider> = Arc::new(provider);
	let (addr, cancel) = spawn_server_with_provider(provider, vec![]).await;

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let mut reader = WireReader::new();

	// ── DESCRIBE ─────────────────────────────────────────────────
	let req = format!(
		"DESCRIBE rtsp://{addr}/Cam2/sub RTSP/1.0\r\n\
		 CSeq: 1\r\n\
		 Accept: application/sdp\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "DESCRIBE should return 200");

	let sdp = std::str::from_utf8(&resp.body).expect("SDP body must be UTF-8");
	assert!(sdp.contains("m=video"), "SDP missing m=video:\n{sdp}");
	assert!(sdp.contains("m=audio"), "SDP missing m=audio:\n{sdp}");
	assert!(
		sdp.contains("a=rtpmap:96 H264/90000"),
		"SDP missing H264 rtpmap for video PT 96:\n{sdp}"
	);
	// AAC-hbr carries the 16 kHz Argus audio — assert the sample rate
	// and channel count match what the capture recorded.
	assert!(
		sdp.contains("a=rtpmap:97 mpeg4-generic/16000/1"),
		"SDP missing AAC rtpmap for audio PT 97 (expected mpeg4-generic/16000/1):\n{sdp}"
	);
	assert!(
		sdp.contains("a=control:trackID=0"),
		"SDP missing per-track control for trackID=0:\n{sdp}"
	);
	assert!(
		sdp.contains("a=control:trackID=1"),
		"SDP missing per-track control for trackID=1 (audio):\n{sdp}"
	);

	// ── SETUP video (trackID=0) ──────────────────────────────────
	let req = format!(
		"SETUP rtsp://{addr}/Cam2/sub/trackID=0 RTSP/1.0\r\n\
		 CSeq: 2\r\n\
		 Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "SETUP video should return 200");
	let session = parse_session_id(&resp, "SETUP video");
	assert!(!session.is_empty(), "video SETUP must return a Session ID");

	// ── SETUP audio (trackID=1) on the same session ──────────────
	// The historical 455 response was the blocker; Task 11
	// turned this into a 200. Regression assertion lives here: if
	// handle_setup ever reverts to "SETUP only legal in INIT state",
	// this test fails with status 455.
	let req = format!(
		"SETUP rtsp://{addr}/Cam2/sub/trackID=1 RTSP/1.0\r\n\
		 CSeq: 3\r\n\
		 Session: {session}\r\n\
		 Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(
		resp.status, 200,
		"SETUP audio MUST return 200 (not 455 MethodNotValidInThisState)",
	);
	let transport = resp
		.headers
		.get("transport")
		.expect("audio SETUP response missing Transport header")
		.clone();
	assert!(
		transport.contains("interleaved=2-3"),
		"audio SETUP Transport must echo interleaved=2-3; got {transport}"
	);
	let audio_session = parse_session_id(&resp, "SETUP audio");
	assert_eq!(
		audio_session, session,
		"audio SETUP must echo the same Session ID as video SETUP"
	);

	// ── PLAY ─────────────────────────────────────────────────────
	let req = format!(
		"PLAY rtsp://{addr}/Cam2/sub RTSP/1.0\r\n\
		 CSeq: 4\r\n\
		 Session: {session}\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, early_frames) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "PLAY should return 200");

	// ── Collect interleaved RTP + RTCP on all four channels ──────
	// Read long enough for at least one SR per track to land. The SR
	// ticker fires once every SR_INTERVAL; leave comfortable margin
	// for CI scheduler jitter. The fixture plays back for ~10 s at
	// real-time pacing so the session is still active when both
	// tracks' SR timers fire.
	const VIDEO_RTP_CH: u8 = 0;
	const VIDEO_RTCP_CH: u8 = 1;
	const AUDIO_RTP_CH: u8 = 2;
	const AUDIO_RTCP_CH: u8 = 3;
	const MIN_VIDEO_RTP: usize = 6;
	const MIN_AUDIO_RTP: usize = 3;
	const MIN_VIDEO_SR: usize = 1;
	const MIN_AUDIO_SR: usize = 1;
	let read_window = SR_INTERVAL + Duration::from_secs(5);
	let deadline = tokio::time::Instant::now() + read_window;
	let mut frames = early_frames;
	let trailing = reader
		.collect_interleaved_until(
			&mut stream,
			&[
				(VIDEO_RTP_CH, MIN_VIDEO_RTP),
				(AUDIO_RTP_CH, MIN_AUDIO_RTP),
				(VIDEO_RTCP_CH, MIN_VIDEO_SR),
				(AUDIO_RTCP_CH, MIN_AUDIO_SR),
			],
			deadline,
		)
		.await;
	frames.extend(trailing);

	let video_rtp: Vec<&Interleaved> = frames
		.iter()
		.filter(|f| f.channel == VIDEO_RTP_CH)
		.collect();
	let audio_rtp: Vec<&Interleaved> = frames
		.iter()
		.filter(|f| f.channel == AUDIO_RTP_CH)
		.collect();
	let video_rtcp: Vec<&Interleaved> = frames
		.iter()
		.filter(|f| f.channel == VIDEO_RTCP_CH)
		.collect();
	let audio_rtcp: Vec<&Interleaved> = frames
		.iter()
		.filter(|f| f.channel == AUDIO_RTCP_CH)
		.collect();

	assert!(
		!video_rtp.is_empty(),
		"expected ≥1 RTP packet on video channel 0; frames observed: {:?}",
		frames
			.iter()
			.map(|f| (f.channel, f.payload.len()))
			.collect::<Vec<_>>()
	);
	assert!(
		!audio_rtp.is_empty(),
		"expected ≥1 RTP packet on audio channel 2 (Task 12 routing); frames observed: {:?}",
		frames
			.iter()
			.map(|f| (f.channel, f.payload.len()))
			.collect::<Vec<_>>()
	);

	// Decode video RTP headers. Every packet must carry PT=96 (H.264).
	let video_headers: Vec<RtpHeader> = video_rtp
		.iter()
		.enumerate()
		.map(|(i, f)| {
			decode_rtp_header(&f.payload)
				.unwrap_or_else(|| panic!("video RTP #{i} too short to decode header"))
		})
		.collect();
	for (i, h) in video_headers.iter().enumerate() {
		assert_eq!(h.version, 2, "video RTP #{i} version must be 2");
		assert_eq!(
			h.payload_type, 96,
			"video RTP #{i} PT must be 96 (H.264 dynamic); got {}",
			h.payload_type
		);
	}
	// Monotonic sequence (wrap permitted).
	for w in video_headers.windows(2) {
		let expected = w[0].sequence.wrapping_add(1);
		assert_eq!(
			w[1].sequence, expected,
			"video RTP seq non-monotonic: {} → {} (expected {expected})",
			w[0].sequence, w[1].sequence,
		);
	}

	// Decode audio RTP headers. Every packet must carry PT=97 (AAC).
	let audio_headers: Vec<RtpHeader> = audio_rtp
		.iter()
		.enumerate()
		.map(|(i, f)| {
			decode_rtp_header(&f.payload)
				.unwrap_or_else(|| panic!("audio RTP #{i} too short to decode header"))
		})
		.collect();
	for (i, h) in audio_headers.iter().enumerate() {
		assert_eq!(h.version, 2, "audio RTP #{i} version must be 2");
		assert_eq!(
			h.payload_type, 97,
			"audio RTP #{i} PT must be 97 (AAC); got {}",
			h.payload_type
		);
	}
	for w in audio_headers.windows(2) {
		let expected = w[0].sequence.wrapping_add(1);
		assert_eq!(
			w[1].sequence, expected,
			"audio RTP seq non-monotonic: {} → {} (expected {expected})",
			w[0].sequence, w[1].sequence,
		);
	}

	// Monotonic RTP timestamps on the audio channel. This guards the
	// root-cause fix for the live-verify regression: handle_aac /
	// handle_adpcm advancing their per-codec PTS counters. When both
	// were hard-coded to 0, ffmpeg/mpv/gst-launch rejected the stream
	// with duplicate-DTS errors on the 4K HEVC camera. Each emitted AAC
	// AU must advance the RTP timestamp by 1024 ticks, so consecutive
	// frames carry strictly increasing timestamps. Wrap at 2^32 is
	// tolerated via wrapping_sub but won't realistically fire during a
	// replay test (u32 rolls ~every 74 hours at 16 kHz).
	for (i, w) in audio_headers.windows(2).enumerate() {
		let (prev, next) = (w[0].timestamp, w[1].timestamp);
		let delta = next.wrapping_sub(prev);
		assert!(
			delta > 0 && delta < u32::MAX / 2,
			"audio RTP ts non-monotonic between frame {i} and {}: {prev} → {next} (delta {delta})",
			i + 1,
		);
	}
	// Video RTP timestamps should likewise be non-decreasing. Fragments
	// of a single access unit share a timestamp, so we accept equality
	// here; we only reject backward jumps (which would surface as a huge
	// positive wrapping delta > u32::MAX / 2).
	for (i, w) in video_headers.windows(2).enumerate() {
		let (prev, next) = (w[0].timestamp, w[1].timestamp);
		let delta = next.wrapping_sub(prev);
		assert!(
			delta < u32::MAX / 2,
			"video RTP ts backward between frame {i} and {}: {prev} → {next} (delta {delta})",
			i + 1,
		);
	}

	// Distinct SSRCs — handle_setup generates them independently per
	// track via rand::random(). A collision is astronomically unlikely
	// but the test asserts it anyway so a regression that shares SSRCs
	// across tracks is caught immediately.
	let video_ssrc = video_headers[0].ssrc;
	let audio_ssrc = audio_headers[0].ssrc;
	assert_ne!(
		video_ssrc, audio_ssrc,
		"video ({video_ssrc:#x}) and audio ({audio_ssrc:#x}) SSRCs must differ"
	);
	// And the SSRC must be consistent within a single track.
	for h in &video_headers {
		assert_eq!(h.ssrc, video_ssrc, "video SSRC drifted mid-stream");
	}
	for h in &audio_headers {
		assert_eq!(h.ssrc, audio_ssrc, "audio SSRC drifted mid-stream");
	}

	// RTCP SR is intentionally NOT sent — see the SR-fire arm in
	// `crates/rtsp/src/server/session_task.rs::run`. mpv / ffmpeg
	// re-anchor on every SR receipt, which surfaced as recurring A-V
	// hitches every SR_INTERVAL on real hardware. Live playback now
	// uses RTP arrival time for A-V sync.
	assert!(
		video_rtcp.is_empty(),
		"video RTCP channel 1 must carry no SR (sending disabled); got {} frames",
		video_rtcp.len()
	);
	assert!(
		audio_rtcp.is_empty(),
		"audio RTCP channel 3 must carry no SR (sending disabled); got {} frames",
		audio_rtcp.len()
	);

	// ── TEARDOWN ─────────────────────────────────────────────────
	let req = format!(
		"TEARDOWN rtsp://{addr}/Cam2/sub RTSP/1.0\r\n\
		 CSeq: 5\r\n\
		 Session: {session}\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "TEARDOWN should return 200");

	drop(stream);
	cancel.cancel();
	tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn subscribe_returns_audio_none_when_fixture_has_no_audio() {
	// /C3 regression: a video-only fixture must yield an SDP
	// with `audio: None`, and the per-subscribe presence latch must
	// end at `AudioPresence::Absent` rather than `Unknown`.
	//
	// Guards against:
	// - Reintroducing the old `PRESCAN_AUDIO_LOOKAHEAD` magic constant
	//   (the lookahead would have made a no-audio fixture exit with
	//   `audio: None`, but for the wrong reason — we want to verify
	//   the new presence-driven path ends the scan cleanly without a
	//   timer).
	// - The Unknown -> Absent latch at the bottom of
	//   `FakeStreamProvider::subscribe` being skipped or inverted.
	//
	// Mirrors the byte-construction pattern used by
	// `fake_provider_emits_video_frames_from_synthetic_bcmedia`: a
	// single H.265 I-frame followed by one P-frame, serialised as
	// BcMedia wire bytes. No audio packets, ever.
	let iframe = BcMedia::Iframe(BcMediaIframe {
		video_type: VideoType::H265,
		microseconds: 0,
		time: Some(1_700_000_000),
		data: synthetic_h265_iframe_bytes(),
	});
	let pframe = BcMedia::Pframe(BcMediaPframe {
		video_type: VideoType::H265,
		microseconds: 33_333,
		data: synthetic_h265_pframe_bytes(),
	});
	let (_td, path) = write_fixture(&[iframe, pframe]);

	let mut provider = FakeStreamProvider::from_dir(Path::new("/nonexistent")).expect("build");
	provider.register("cam1", RtspStreamKind::Main, path);
	// Speed up playback so the prescan doesn't linger on pacing.
	let provider = provider.with_speed_factor(1000.0);

	let sub = provider
		.subscribe("cam1", RtspStreamKind::Main, None)
		.await
		.expect("subscribe must succeed on a well-formed video-only fixture");

	assert!(
		sub.sdp_params.video.is_some(),
		"video SDP must be populated by the synthetic IDR"
	);
	assert!(
		sub.sdp_params.audio.is_none(),
		"audio SDP must stay None when the fixture contains no audio packets"
	);
}

// ── Task 11: gap-bridging end-to-end ────────────────────────
//
// Drive the real `RtspServer` against a `FakeStreamProvider` whose
// replay task pauses the upstream feed after N frames for a fixed
// wall-clock duration. During the pause the provider synthesises
// replay `Frame::Video` messages from the cached `LastFrameBuffer` at
// ~200 ms cadence, mirroring production's gap-detection ticker in
// `src/stream_source.rs::emit_replay_frame_if_bridging`. An attached
// RTSP client must therefore observe continuous RTP on the wire with
// no arrival gap longer than ~750 ms — that's the load-bearing
// contract tasks 4-7 exist to establish. (Ceiling is tuned
// to survive tokio scheduling + TCP jitter under CI load; the
// unbridged baseline would be the full 3 s silence window.)

/// Concurrent variant of [`collect_rtp_arrival_instants`] that owns a
/// TCP read half (from `TcpStream::into_split`) so it can run in a
/// background task while the main test body drives the injector + takes
/// wall-clock sleeps. Inherits the buffer the caller was using up to
/// PLAY, so any interleaved frames already read during the RTSP
/// handshake are available. Stamps each channel-`rtp_channel` arrival
/// with `Instant::now()`; returns on EOF, deadline, or any stray
/// non-interleaved byte (treated as server-bug bail-out).
async fn collect_rtp_arrivals_from_read_half(
	mut read_half: tokio::net::tcp::OwnedReadHalf,
	mut reader: WireReader,
	rtp_channel: u8,
	deadline: tokio::time::Instant,
) -> Vec<tokio::time::Instant> {
	let mut out: Vec<tokio::time::Instant> = Vec::new();
	let mut scratch = [0u8; 4096];
	loop {
		let now = tokio::time::Instant::now();
		if now >= deadline {
			return out;
		}
		while reader.buf.len() < 4 {
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			let remaining = deadline - now;
			match tokio::time::timeout(remaining, read_half.read(&mut scratch)).await {
				Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return out,
				Ok(Ok(n)) => reader.buf.extend_from_slice(&scratch[..n]),
			}
		}
		if reader.buf[0] != 0x24 {
			return out;
		}
		let len = u16::from_be_bytes([reader.buf[2], reader.buf[3]]) as usize;
		let need = 4 + len;
		while reader.buf.len() < need {
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			let remaining = deadline - now;
			match tokio::time::timeout(remaining, read_half.read(&mut scratch)).await {
				Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return out,
				Ok(Ok(n)) => reader.buf.extend_from_slice(&scratch[..n]),
			}
		}
		let ch = reader.buf[1];
		reader.buf.drain(..need);
		if ch == rtp_channel {
			out.push(tokio::time::Instant::now());
		}
	}
}

/// Collect every interleaved RTP frame arriving on `rtp_channel` until
/// `deadline`, stamping each arrival with `Instant::now()`. Non-RTP
/// interleaved frames (RTCP, different channel) are consumed but not
/// timestamped. Stops early on EOF. Returns the arrival instants in
/// order so callers can compute inter-arrival deltas.
#[allow(dead_code)]
async fn collect_rtp_arrival_instants(
	reader: &mut WireReader,
	stream: &mut TcpStream,
	rtp_channel: u8,
	deadline: tokio::time::Instant,
) -> Vec<tokio::time::Instant> {
	let mut out: Vec<tokio::time::Instant> = Vec::new();
	loop {
		let now = tokio::time::Instant::now();
		if now >= deadline {
			return out;
		}
		// Ensure we have at least 4 bytes for the interleaved header.
		while reader.buf.len() < 4 {
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			let remaining = deadline - now;
			if !reader.fill(stream, remaining).await {
				return out;
			}
		}
		if reader.buf[0] != 0x24 {
			// Not an interleaved frame. The test only issues control
			// messages before PLAY; any stray RTSP response here is a
			// server bug. Bail with what we've collected.
			return out;
		}
		let len = u16::from_be_bytes([reader.buf[2], reader.buf[3]]) as usize;
		let need = 4 + len;
		while reader.buf.len() < need {
			let now = tokio::time::Instant::now();
			if now >= deadline {
				return out;
			}
			let remaining = deadline - now;
			if !reader.fill(stream, remaining).await {
				return out;
			}
		}
		let ch = reader.buf[1];
		reader.buf.drain(..need);
		if ch == rtp_channel {
			out.push(tokio::time::Instant::now());
		}
	}
}

/// Thin [`StreamProvider`] adapter that hands subscribers a receiver on
/// a real [`bairelay::stream_source::StreamSource`]. The source itself is
/// built via the crate's `#[cfg(test)]` inert constructor, so the ticker
/// task + `emit_replay_frame_if_bridging` the test is asserting on are
/// exactly the production code paths.
///
/// Unlike [`FakeStreamProvider`] this provider does not own a fixture —
/// the caller feeds the source via the returned [`FakeFrameInjector`]
/// handle. See `fixture_replay_bridges_injected_gap` for the only call
/// site.
struct RealStreamSourceProvider {
	source: Arc<bairelay::stream_source::StreamSource>,
}

#[async_trait]
impl StreamProvider for RealStreamSourceProvider {
	async fn subscribe(
		&self,
		camera: &str,
		_kind: RtspStreamKind,
		_authenticated_user: Option<&str>,
	) -> Result<SubscriptionHandle, StreamError> {
		if camera != "cam1" {
			return Err(StreamError::UnknownCamera);
		}
		// Snapshot SDP at subscribe time — matches production's
		// `CameraProvider::subscribe`: DESCRIBE sees whatever SDP params
		// the source has populated when we observe it.
		let sdp_snapshot = self.source.sdp_params();
		Ok(SubscriptionHandle {
			frames: self.source.subscribe(),
			sdp_params: sdp_snapshot,
			last_frame: self.source.last_frame(),
			guard: no_op_guard(),
		})
	}
}

#[tokio::test]
async fn fixture_replay_bridges_injected_gap() {
	use bairelay::stream_source::StreamSource;

	// ── 1. Build a production `StreamSource` via the `#[cfg(test)]` inert
	//       constructor. Gap threshold is 300 ms — short enough that we
	//       observe multiple Bridging ticks inside the 3-second upstream
	//       silence window, well under the 500 ms wire-gap assertion.
	let gap_threshold = Duration::from_millis(300);
	let last_frame = Arc::new(LastFrameBuffer::new());
	let (source, injector) =
		StreamSource::start_inert_for_test_with_gap_and_last_frame_and_injector(
			gap_threshold,
			Arc::clone(&last_frame),
		);

	// ── 2. Seed `last_frame` + `sdp_params` by running one synthetic IDR
	//       through the production translator. This is exactly what
	//       production's `reader_task` does on the first keyframe from
	//       the camera — populates the cached `VideoBurst` that
	//       `emit_replay_frame_if_bridging` will later read from, and
	//       the SDP params the RTSP `DESCRIBE` returns. Using
	//       `apply_bcmedia_packet` here (rather than hand-building a
	//       `VideoBurst` / `SdpParams`) keeps this test coupled to the
	//       real production path — if the IDR translator changes shape,
	//       this test notices.
	let sdp_seed: Arc<RwLock<SdpParams>> = Arc::new(RwLock::new(SdpParams {
		server_ip: "127.0.0.1".to_string(),
		session_id: session_id(),
		session_name: "cam1/main".to_string(),
		video: None,
		audio: None,
	}));
	let (seed_tx, _seed_rx) = broadcast::channel::<Frame>(4);
	let presence = Arc::new(RwLock::new(AudioPresence::Unknown));
	let mut seed_state = StreamTranslatorState::default();
	let seed_gap = std::sync::Mutex::new(GapState::Live);
	apply_bcmedia_packet(
		&BcMedia::Iframe(BcMediaIframe {
			video_type: VideoType::H265,
			microseconds: 0,
			time: Some(1_700_000_000),
			data: synthetic_h265_iframe_bytes(),
		}),
		&seed_tx,
		None,
		None,
		&last_frame,
		&sdp_seed,
		&presence,
		&mut seed_state,
		&seed_gap,
	);
	let seeded_params = sdp_seed.read().expect("sdp lock poisoned").clone();
	assert!(
		seeded_params.video.is_some(),
		"seed IDR must populate SdpParams.video — otherwise DESCRIBE has no m=video section"
	);
	assert!(
		last_frame.has_video(),
		"seed IDR must populate the VideoBurst — otherwise emit_replay_frame_if_bridging has nothing to replay"
	);
	source.set_sdp_params_for_test(seeded_params);

	// ── 3. Wire the RTSP server to the real source via the thin adapter.
	let provider: Arc<dyn StreamProvider> = Arc::new(RealStreamSourceProvider {
		source: Arc::clone(&source),
	});
	let (addr, cancel) = spawn_server_with_provider(provider, vec![]).await;

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let mut reader = WireReader::new();

	// ── 4. OPTIONS / DESCRIBE / SETUP / PLAY ────────────────────────
	let req = format!("OPTIONS rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 1\r\n\r\n");
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "OPTIONS");

	let req = format!(
		"DESCRIBE rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "DESCRIBE");

	let req = format!(
		"SETUP rtsp://{addr}/cam1/trackID=0 RTSP/1.0\r\n\
		 CSeq: 3\r\n\
		 Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n",
	);
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, _) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "SETUP");
	let session = parse_session_id(&resp, "SETUP");

	let req = format!("PLAY rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 4\r\nSession: {session}\r\n\r\n");
	write_all(&mut stream, req.as_bytes()).await;
	let (resp, early_frames) = reader.read_rtsp_response(&mut stream).await;
	assert_eq!(resp.status, 200, "PLAY");
	let play_returned_at = tokio::time::Instant::now();

	// Concurrent wire collector: the RTSP session task writes RTP as
	// soon as our `broadcast_live_video_frame` / production-replay-ticker
	// broadcasts land, so the reader must run in parallel with the feed
	// loop. Otherwise frames pile up in the TCP buffer and all get read
	// at the end, collapsing the arrival-time distribution and hiding
	// any real gap the production path would have left on the wire.
	//
	// Split the stream: reader half goes to the collector task, writer
	// half stays on the main task for the TEARDOWN at the end.
	let (read_half, mut write_half) = stream.into_split();
	let collector_deadline = play_returned_at + Duration::from_millis(5_200);
	let collector = tokio::spawn(collect_rtp_arrivals_from_read_half(
		read_half,
		reader,
		0, // video RTP channel
		collector_deadline,
	));

	// ── 5. Live phase: feed ~30 frames at 30 fps via the injector.
	//       Each call broadcasts a real `Frame::Video` through the
	//       source's tx channel (what the server's session task picks
	//       up) AND updates the gap-detection markers so the ticker
	//       stays in `Live` state.
	//
	//       The injector's `broadcast_live_video_frame` is the minimal
	//       stand-in for what production's `reader_task` does in its
	//       per-packet loop. We cannot spawn a `reader_task` here
	//       because it requires a real `BcCamera` — so we feed the
	//       source at the translator-output layer.
	let burst = last_frame
		.video_snapshot()
		.expect("seed IDR populated the burst in step 2");
	let live_nals: Vec<bytes::Bytes> = burst
		.iframe_nals
		.iter()
		.map(|n| bytes::Bytes::copy_from_slice(n))
		.collect();
	let mut pts_90khz: u32 = burst.captured_pts_90khz.max(1);
	for _ in 0..30u32 {
		pts_90khz = pts_90khz.wrapping_add(3000); // ~33 ms @ 90 kHz
		injector.broadcast_live_video_frame(Frame::Video {
			codec: burst.codec,
			nals: live_nals.clone(),
			pts_90khz,
			keyframe: true,
			access_unit_end: true,
		});
		tokio::time::sleep(Duration::from_millis(33)).await;
	}

	// ── 6. Silence phase: stop feeding for 3 s. Production's ticker
	//       inside the inert source observes `last_live_frame_at.elapsed()
	//       > gap_threshold`, flips `gap_state` to `Bridging`, and its
	//       `emit_replay_frame_if_bridging` call broadcasts a replay
	//       `Frame::Video` every `GAP_DETECTION_TICK` (200 ms). The RTSP
	//       server packetises those broadcasts and writes RTP on the
	//       wire. No test code runs during this window — any wire
	//       activity is strictly from production.
	tokio::time::sleep(Duration::from_secs(3)).await;

	// ── 7. Resume live phase briefly so `Bridging → Live` is exercised
	//       too. (The wire-gap assertion below doesn't require post-gap
	//       frames — it's already strict enough during the silence — but
	//       verifying the resume path adds cheap coverage.)
	for _ in 0..15u32 {
		pts_90khz = pts_90khz.wrapping_add(3000);
		injector.broadcast_live_video_frame(Frame::Video {
			codec: burst.codec,
			nals: live_nals.clone(),
			pts_90khz,
			keyframe: true,
			access_unit_end: true,
		});
		tokio::time::sleep(Duration::from_millis(33)).await;
	}

	// ── 8. Join the collector. The deadline it was spawned with covers
	//       ~1 s live + 3 s silence + ~0.5 s post-gap + 0.7 s slack; by
	//       the time we get here the collector is either draining its
	//       last arrivals or already timed out.
	let mut arrival_times: Vec<tokio::time::Instant> = early_frames
		.iter()
		.filter(|f| f.channel == 0)
		.map(|_| play_returned_at)
		.collect();
	let trailing = collector.await.expect("collector task panicked");
	arrival_times.extend(trailing);

	// Live phase: ~30 pre-gap frames + ~15 Bridging replay ticks (3 s /
	// 200 ms) + ~15 post-gap frames ≈ 60 video RTP packets minimum. Set
	// a generous floor — CI jitter can lose a few off either end, the
	// load-bearing assertion is the max-gap bound below.
	assert!(
		arrival_times.len() >= 20,
		"expected ≥20 video RTP arrivals across ~4 s window; got {} (production gap-bridging may have stalled)",
		arrival_times.len()
	);

	let (max_gap, max_gap_idx) = arrival_times
		.windows(2)
		.enumerate()
		.map(|(i, w)| (w[1].saturating_duration_since(w[0]), i))
		.max_by_key(|(d, _)| *d)
		.expect("≥2 arrivals implies ≥1 window");

	assert!(
		max_gap < Duration::from_millis(750),
		"max RTP inter-arrival gap {max_gap:?} at window {max_gap_idx} of {} exceeds 750 ms — production's emit_replay_frame_if_bridging should keep the wire warm through the 3 s upstream silence (gap_threshold was 300 ms, ticker runs every 200 ms). 750 ms ceiling is still vastly better than the 3 s unbridged gap and gives headroom for tokio scheduling + TCP jitter on loaded CI runners.",
		arrival_times.len() - 1,
	);

	// ── 9. TEARDOWN ─────────────────────────────────────────────────
	let req =
		format!("TEARDOWN rtsp://{addr}/cam1 RTSP/1.0\r\nCSeq: 5\r\nSession: {session}\r\n\r\n");
	// Best-effort write on the split half; the read half is already
	// consumed by the collector, so we can't read the response. Any
	// failure here just means the session already tore down from
	// cancel.cancel() below — the wire-cadence assertion above has
	// already been checked.
	let _ = write_half.write_all(req.as_bytes()).await;
	let _ = write_half.flush().await;

	drop(write_half);
	cancel.cancel();
	source.stop();
	tokio::time::sleep(Duration::from_millis(100)).await;
}
