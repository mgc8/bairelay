use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use bairelay_neolink_core::bc_protocol::{BcCamera, CameraDriver, StreamKind, VideoStream};
use bairelay_neolink_core::bcmedia::model::{BcMedia, VideoType};

use super::errors::UsageError;
use super::output::Outcome;

// Bound for how long to wait on the stream for the first I-frame.
// Matches neolink's `image --use-stream` default window.
const FIRST_IFRAME_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(
	cam: &BcCamera,
	output_path: Option<&Path>,
	mode_json: bool,
	use_stream: bool,
) -> Result<Outcome> {
	check_json_output(mode_json, output_path)?;
	if use_stream {
		capture_via_stream(cam, output_path).await
	} else {
		// Trait-object call so the snap path can be driven by a FakeCamera
		// in tests.
		capture_via_snap(cam as &dyn CameraDriver, output_path).await
	}
}

/// Driver-only entry used by the `dispatch_oneshot` unit tests. Same
/// contract as [`run`] but rejects `use_stream = true` up front — the
/// stream path requires a concrete [`BcCamera`], which the trait-object
/// harness doesn't supply. Production `run_oneshot` always has a
/// `BcCamera` in hand and routes through [`run`].
pub async fn run_via_driver(
	cam: &dyn CameraDriver,
	output_path: Option<&Path>,
	mode_json: bool,
	use_stream: bool,
) -> Result<Outcome> {
	check_json_output(mode_json, output_path)?;
	if use_stream {
		anyhow::bail!("snapshot --use-stream-raw requires a concrete BcCamera; not supported through CameraDriver");
	}
	capture_via_snap(cam, output_path).await
}

/// JSON mode mixes a status-line on stdout with the capture payload on
/// stdout — refuse up front so the operator doesn't truncate the JPEG
/// when piping. Returns `Ok(())` for every valid combination, a
/// [`UsageError`] otherwise.
pub(crate) fn check_json_output(mode_json: bool, output_path: Option<&Path>) -> Result<()> {
	if mode_json && output_path.is_none() {
		return Err(UsageError::new(
			"snapshot --json requires --output <path> (can't mix JSON and bytes on stdout)",
		)
		.into());
	}
	Ok(())
}

pub(crate) async fn capture_via_snap(
	cam: &dyn CameraDriver,
	output_path: Option<&Path>,
) -> Result<Outcome> {
	let jpeg = cam.get_snapshot().await.context("get_snapshot failed")?;
	let (bytes, path) = write_payload(&jpeg, output_path, "JPEG")?;
	Ok(Outcome::Snapshot {
		bytes,
		path,
		format: "jpeg".into(),
	})
}

/// Start a video stream, grab the first I-frame's raw Annex-B NAL
/// bytes, stop the stream, and write the bitstream out. No decode —
/// users who need a JPEG pipe through `ffmpeg -i <file> -vframes 1`.
async fn capture_via_stream(cam: &BcCamera, output_path: Option<&Path>) -> Result<Outcome> {
	let mut stream = cam
		.start_video(StreamKind::Main, 100, false)
		.await
		.context("start_video failed")?;
	let result = drain_first_iframe(&mut stream, FIRST_IFRAME_TIMEOUT).await;
	let _ = cam.stop_video(StreamKind::Main).await;
	finish_stream_capture(result, output_path)
}

/// Bottom half of [`capture_via_stream`] — owns the I-frame→`Outcome`
/// translation so tests can exercise every branch (`start_video` error,
/// drain error, drain success for H.264 and H.265) without needing a
/// concrete [`BcCamera`].
pub(crate) fn finish_stream_capture(
	drain_result: Result<bairelay_neolink_core::bcmedia::model::BcMediaIframe>,
	output_path: Option<&Path>,
) -> Result<Outcome> {
	let iframe = drain_result?;
	let format = iframe_format_label(&iframe);
	let (bytes, path) = write_payload(&iframe.data, output_path, format)?;
	Ok(Outcome::Snapshot {
		bytes,
		path,
		format: format.into(),
	})
}

/// Pull packets off `stream` until we see the first I-frame or the
/// overall deadline elapses. Extracted so tests can drive a scripted
/// [`VideoStream`] without a live camera — production uses
/// [`bairelay_neolink_core::bc_protocol::StreamData`]; tests use a `MockVideoStream`.
///
/// On error / timeout the caller is still responsible for sending
/// `stop_video` — this helper only owns the pull loop.
pub(crate) async fn drain_first_iframe(
	stream: &mut dyn VideoStream,
	window: Duration,
) -> Result<bairelay_neolink_core::bcmedia::model::BcMediaIframe> {
	let deadline = tokio::time::Instant::now() + window;
	loop {
		let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
		if remaining.is_zero() {
			anyhow::bail!("no I-frame received within {}s", window.as_secs());
		}
		let step = match tokio::time::timeout(remaining, stream.get_data()).await {
			Ok(inner) => classify_stream_step(Some(inner)),
			Err(_) => classify_stream_step(None),
		};
		match step {
			StreamStep::Got(i) => return Ok(i),
			StreamStep::Continue => continue,
			StreamStep::ReadError(e) => {
				return Err(anyhow::anyhow!(e)).context("stream read error")
			}
			StreamStep::ReceiveError(e) => {
				return Err(anyhow::anyhow!(e)).context("stream receive error")
			}
			StreamStep::TimedOut => {
				anyhow::bail!("no I-frame received within {}s", window.as_secs());
			}
		}
	}
}

/// Label the format string for an I-frame's codec.
pub(crate) fn iframe_format_label(
	i: &bairelay_neolink_core::bcmedia::model::BcMediaIframe,
) -> &'static str {
	match i.video_type {
		VideoType::H264 => "h264",
		VideoType::H265 => "h265",
	}
}

/// One step of the stream-capture loop body, pulled out as a pure
/// decision function so the rich `match` tree can be unit-tested
/// without a live `BcCamera` + `StreamData`. `None` encodes a
/// `timeout()` elapse; `Some(inner)` carries the recv result.
pub(crate) enum StreamStep {
	/// Received an I-frame — break the outer loop with this payload.
	Got(bairelay_neolink_core::bcmedia::model::BcMediaIframe),
	/// Non-I-frame packet (audio / p-frame / parameter-set) — loop again.
	Continue,
	/// Inner parse / decode error from the stream.
	ReadError(bairelay_neolink_core::Error),
	/// Outer mpsc dropped / cancelled error.
	ReceiveError(bairelay_neolink_core::Error),
	/// `tokio::time::timeout` elapsed.
	TimedOut,
}

pub(crate) fn classify_stream_step(
	inner: Option<
		std::result::Result<
			std::result::Result<BcMedia, bairelay_neolink_core::Error>,
			bairelay_neolink_core::Error,
		>,
	>,
) -> StreamStep {
	match inner {
		Some(Ok(Ok(BcMedia::Iframe(i)))) => StreamStep::Got(i),
		Some(Ok(Ok(_))) => StreamStep::Continue,
		Some(Ok(Err(e))) => StreamStep::ReadError(e),
		Some(Err(e)) => StreamStep::ReceiveError(e),
		None => StreamStep::TimedOut,
	}
}

fn write_payload(
	data: &[u8],
	output_path: Option<&Path>,
	label: &str,
) -> Result<(usize, Option<String>)> {
	write_payload_to(&mut std::io::stdout(), data, output_path, label)
}

/// Testable inner form of [`write_payload`] — any `Write` sink can
/// stand in for `stdout`, so the stdout branch is unit-testable.
pub(crate) fn write_payload_to<W: Write>(
	sink: &mut W,
	data: &[u8],
	output_path: Option<&Path>,
	label: &str,
) -> Result<(usize, Option<String>)> {
	let bytes = data.len();
	let path = match output_path {
		Some(p) => {
			std::fs::write(p, data).with_context(|| format!("failed to write {}", p.display()))?;
			Some(p.display().to_string())
		}
		None => {
			sink.write_all(data)
				.with_context(|| format!("failed to write {} to stdout", label))?;
			None
		}
	};
	Ok((bytes, path))
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc_protocol::{Error, FakeCameraBuilder};
	use bairelay_neolink_core::bcmedia::model::BcMediaIframe;

	/// Scripted `VideoStream` implementation for `drain_first_iframe`
	/// coverage. Feeds a fixed sequence of packets (or errors), then
	/// blocks forever — mirroring a real camera's "no more frames yet"
	/// idle state so the deadline path exercises correctly.
	struct MockVideoStream {
		steps: std::collections::VecDeque<MockStep>,
		shutdown_called: std::sync::Arc<std::sync::atomic::AtomicUsize>,
	}

	enum MockStep {
		/// Yield Ok(Ok(BcMedia)) immediately.
		Frame(BcMedia),
		/// Yield Ok(Err(..)) — camera-side parse error.
		InnerErr(Error),
		/// Yield Err(..) — outer mpsc error.
		OuterErr(Error),
		/// Block forever so the deadline timeout fires.
		Hang,
	}

	impl MockVideoStream {
		fn new(steps: Vec<MockStep>) -> Self {
			Self {
				steps: steps.into(),
				shutdown_called: Default::default(),
			}
		}
	}

	#[async_trait::async_trait]
	impl VideoStream for MockVideoStream {
		async fn get_data(
			&mut self,
		) -> std::result::Result<std::result::Result<BcMedia, Error>, Error> {
			match self.steps.pop_front() {
				Some(MockStep::Frame(f)) => Ok(Ok(f)),
				Some(MockStep::InnerErr(e)) => Ok(Err(e)),
				Some(MockStep::OuterErr(e)) => Err(e),
				Some(MockStep::Hang) | None => {
					std::future::pending::<()>().await;
					unreachable!()
				}
			}
		}
		async fn shutdown(&mut self) -> std::result::Result<(), Error> {
			self.shutdown_called
				.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			Ok(())
		}
	}

	fn ifr(vt: VideoType) -> BcMediaIframe {
		BcMediaIframe {
			video_type: vt,
			microseconds: 0,
			time: None,
			data: vec![0xAB, 0xCD],
		}
	}

	#[test]
	fn write_payload_to_sink_captures_bytes_when_no_path() {
		let mut sink: Vec<u8> = Vec::new();
		let (bytes, path) = write_payload_to(&mut sink, b"HELLO", None, "label").unwrap();
		assert_eq!(bytes, 5);
		assert!(path.is_none());
		assert_eq!(sink, b"HELLO");
	}

	#[test]
	fn write_payload_to_sink_propagates_write_error() {
		// A 0-capacity slice writer fails on any non-empty write.
		struct FailingSink;
		impl Write for FailingSink {
			fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
				Err(std::io::Error::other("no"))
			}
			fn flush(&mut self) -> std::io::Result<()> {
				Ok(())
			}
		}
		let mut sink = FailingSink;
		let err = write_payload_to(&mut sink, b"data", None, "label").unwrap_err();
		assert!(format!("{:#}", err).contains("failed to write label to stdout"));
	}

	#[test]
	fn finish_stream_capture_h264_writes_format() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let iframe = ifr(VideoType::H264);
		let outcome =
			finish_stream_capture(Ok(iframe), Some(tmp.path())).expect("stream finish ok");
		let Outcome::Snapshot { format, bytes, .. } = outcome else {
			panic!("wrong variant");
		};
		assert_eq!(format, "h264");
		assert_eq!(bytes, 2);
	}

	#[test]
	fn finish_stream_capture_h265_writes_format() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let outcome = finish_stream_capture(Ok(ifr(VideoType::H265)), Some(tmp.path()))
			.expect("stream finish ok");
		let Outcome::Snapshot { format, .. } = outcome else {
			panic!("wrong variant");
		};
		assert_eq!(format, "h265");
	}

	#[test]
	fn finish_stream_capture_propagates_drain_error() {
		let err = finish_stream_capture(Err(anyhow::anyhow!("drain failed")), None)
			.expect_err("should fail");
		assert!(format!("{:#}", err).contains("drain failed"));
	}

	#[tokio::test]
	async fn snap_writes_to_file_when_path_given() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"JPEG-bytes".to_vec()))
			.build();
		let outcome = capture_via_snap(&*fake, Some(&path)).await.unwrap();
		let Outcome::Snapshot {
			bytes,
			path: out_path,
			format,
		} = outcome
		else {
			panic!("wrong variant");
		};
		assert_eq!(bytes, 10);
		assert_eq!(format, "jpeg");
		assert_eq!(
			out_path.as_deref(),
			Some(path.display().to_string().as_str())
		);
		let on_disk = std::fs::read(&path).unwrap();
		assert_eq!(on_disk, b"JPEG-bytes");
	}

	#[tokio::test]
	async fn snap_error_propagates() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Err(Error::Other("no jpeg")))
			.build();
		let err = capture_via_snap(&*fake, None).await.unwrap_err();
		assert!(format!("{:#}", err).contains("get_snapshot failed"));
	}

	#[tokio::test]
	async fn snap_empty_jpeg_still_writes_zero_bytes() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(Vec::new()))
			.build();
		let outcome = capture_via_snap(&*fake, Some(&path)).await.unwrap();
		let Outcome::Snapshot { bytes, format, .. } = outcome else {
			panic!("wrong variant");
		};
		assert_eq!(bytes, 0);
		assert_eq!(format, "jpeg");
	}

	#[tokio::test]
	async fn write_payload_to_file_reports_path() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let (bytes, reported) = write_payload(b"hello", Some(&path), "test").unwrap();
		assert_eq!(bytes, 5);
		assert_eq!(
			reported.as_deref(),
			Some(path.display().to_string().as_str())
		);
		assert_eq!(std::fs::read(&path).unwrap(), b"hello");
	}

	#[test]
	fn write_payload_to_unwritable_dir_errors() {
		// Point at a path whose parent does not exist so the write fails.
		let bad = std::path::PathBuf::from("/nonexistent/definitely-missing/x.bin");
		let err = write_payload(b"data", Some(&bad), "jpeg").unwrap_err();
		assert!(format!("{:#}", err).contains("failed to write"));
	}

	#[test]
	fn check_json_output_rejects_json_without_path() {
		let err = check_json_output(true, None).unwrap_err();
		assert!(format!("{:#}", err).contains("--json requires --output"));
	}

	#[test]
	fn check_json_output_allows_json_with_path() {
		let p = std::path::PathBuf::from("/tmp/x.jpg");
		assert!(check_json_output(true, Some(&p)).is_ok());
	}

	#[test]
	fn check_json_output_allows_human_without_path() {
		assert!(check_json_output(false, None).is_ok());
	}

	#[test]
	fn check_json_output_allows_human_with_path() {
		let p = std::path::PathBuf::from("/tmp/x.jpg");
		assert!(check_json_output(false, Some(&p)).is_ok());
	}

	// ---- classify_stream_step decision-table tests ----

	fn iframe(vt: VideoType) -> bairelay_neolink_core::bcmedia::model::BcMediaIframe {
		bairelay_neolink_core::bcmedia::model::BcMediaIframe {
			video_type: vt,
			microseconds: 0,
			time: None,
			data: vec![1, 2, 3],
		}
	}

	#[test]
	fn classify_stream_step_iframe_breaks_loop() {
		let step = classify_stream_step(Some(Ok(Ok(BcMedia::Iframe(iframe(VideoType::H264))))));
		match step {
			StreamStep::Got(i) => assert!(matches!(i.video_type, VideoType::H264)),
			_ => panic!("expected Got"),
		}
	}

	#[test]
	fn classify_stream_step_other_media_continues() {
		// A non-Iframe BcMedia variant — use Pframe which has minimal
		// fields.
		use bairelay_neolink_core::bcmedia::model::BcMediaPframe;
		let step = classify_stream_step(Some(Ok(Ok(BcMedia::Pframe(BcMediaPframe {
			video_type: VideoType::H264,
			microseconds: 0,
			data: vec![],
		})))));
		assert!(matches!(step, StreamStep::Continue));
	}

	#[test]
	fn classify_stream_step_inner_err_is_read_error() {
		let step =
			classify_stream_step(Some(Ok(Err(bairelay_neolink_core::Error::Other("parse")))));
		assert!(matches!(step, StreamStep::ReadError(_)));
	}

	#[test]
	fn classify_stream_step_outer_err_is_receive_error() {
		let step = classify_stream_step(Some(Err(bairelay_neolink_core::Error::DroppedSubscriber)));
		assert!(matches!(step, StreamStep::ReceiveError(_)));
	}

	#[test]
	fn classify_stream_step_none_is_timed_out() {
		let step = classify_stream_step(None);
		assert!(matches!(step, StreamStep::TimedOut));
	}

	#[test]
	fn iframe_format_label_covers_both_codecs() {
		assert_eq!(iframe_format_label(&iframe(VideoType::H264)), "h264");
		assert_eq!(iframe_format_label(&iframe(VideoType::H265)), "h265");
	}

	// ---- run_via_driver tests ----

	#[tokio::test]
	async fn run_via_driver_snap_happy_path() {
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"JPEG".to_vec()))
			.build();
		let outcome = run_via_driver(&*fake, Some(&path), false, false)
			.await
			.unwrap();
		assert!(matches!(outcome, Outcome::Snapshot { .. }));
	}

	#[tokio::test]
	async fn run_via_driver_use_stream_rejected_up_front() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"JPEG".to_vec()))
			.build();
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let err = run_via_driver(&*fake, Some(tmp.path()), false, true)
			.await
			.unwrap_err();
		assert!(format!("{:#}", err).contains("requires a concrete BcCamera"));
	}

	#[tokio::test]
	async fn run_via_driver_json_without_path_rejected() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"JPEG".to_vec()))
			.build();
		let err = run_via_driver(&*fake, None, true, false).await.unwrap_err();
		assert!(format!("{:#}", err).contains("--json"));
	}

	// ---- drain_first_iframe scripted-stream tests ----

	#[tokio::test]
	async fn drain_returns_first_iframe_immediately() {
		let mut stream =
			MockVideoStream::new(vec![MockStep::Frame(BcMedia::Iframe(ifr(VideoType::H264)))]);
		let got = drain_first_iframe(&mut stream, Duration::from_millis(500))
			.await
			.expect("ok");
		assert!(matches!(got.video_type, VideoType::H264));
		assert_eq!(got.data, vec![0xAB, 0xCD]);
	}

	#[tokio::test]
	async fn drain_skips_pframe_before_iframe() {
		use bairelay_neolink_core::bcmedia::model::BcMediaPframe;
		let mut stream = MockVideoStream::new(vec![
			MockStep::Frame(BcMedia::Pframe(BcMediaPframe {
				video_type: VideoType::H264,
				microseconds: 0,
				data: vec![0x42],
			})),
			MockStep::Frame(BcMedia::Iframe(ifr(VideoType::H265))),
		]);
		let got = drain_first_iframe(&mut stream, Duration::from_millis(500))
			.await
			.expect("ok");
		assert!(matches!(got.video_type, VideoType::H265));
	}

	#[tokio::test]
	async fn drain_propagates_inner_error_as_read_error() {
		let mut stream = MockVideoStream::new(vec![MockStep::InnerErr(Error::Other("boom"))]);
		let err = drain_first_iframe(&mut stream, Duration::from_millis(500))
			.await
			.expect_err("should fail");
		assert!(format!("{:#}", err).contains("stream read error"));
	}

	#[tokio::test]
	async fn drain_propagates_outer_error_as_receive_error() {
		let mut stream = MockVideoStream::new(vec![MockStep::OuterErr(Error::DroppedSubscriber)]);
		let err = drain_first_iframe(&mut stream, Duration::from_millis(500))
			.await
			.expect_err("should fail");
		assert!(format!("{:#}", err).contains("stream receive error"));
	}

	#[tokio::test]
	async fn drain_times_out_when_stream_hangs() {
		let mut stream = MockVideoStream::new(vec![MockStep::Hang]);
		// Short window so the test completes fast.
		let err = drain_first_iframe(&mut stream, Duration::from_millis(50))
			.await
			.expect_err("should time out");
		assert!(format!("{:#}", err).contains("no I-frame received"));
	}

	#[tokio::test]
	async fn drain_times_out_with_no_frames_at_all() {
		// Empty queue — pop_front yields None → Hang path (pending forever).
		let mut stream = MockVideoStream::new(vec![]);
		let err = drain_first_iframe(&mut stream, Duration::from_millis(50))
			.await
			.expect_err("should time out");
		assert!(format!("{:#}", err).contains("no I-frame received"));
	}

	#[tokio::test]
	async fn drain_times_out_when_window_zero() {
		// Deadline already passed before first poll — loop immediately
		// bails without touching the stream.
		let mut stream =
			MockVideoStream::new(vec![MockStep::Frame(BcMedia::Iframe(ifr(VideoType::H264)))]);
		let err = drain_first_iframe(&mut stream, Duration::from_millis(0))
			.await
			.expect_err("should time out");
		assert!(format!("{:#}", err).contains("no I-frame received"));
	}

	// ---- run() entry-point coverage ----

	/// `run` with `use_stream: false` must dispatch to the snap path on
	/// the concrete `BcCamera`. Drive a `BcCamera` via `MockConnection`
	/// scripted with a multi-chunk SNAP reply (mirrors the snap.rs
	/// happy-path test). Since the SNAP happy-path is covered in
	/// `bc_protocol::snap::tests`, here we just confirm `run`'s
	/// snap-vs-stream branch picks the right path. Use the `--json`
	/// rejection path for a synchronous error that doesn't need a
	/// camera at all.
	#[tokio::test]
	async fn run_rejects_json_without_path_before_touching_camera() {
		use bairelay_neolink_core::bc_protocol::connection::mock::MockConnection;
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// mode_json=true, output=None -> UsageError before any camera call.
		let err = run(&cam, None, true, false).await.unwrap_err();
		assert!(format!("{:#}", err).contains("--json"));
	}

	/// `run` with `use_stream: false` and a mocked SNAP reply — exercises
	/// the dispatch arm that calls `capture_via_snap(cam as &dyn …)`.
	#[tokio::test]
	async fn run_snap_path_via_mock_connection() {
		use bairelay_neolink_core::bc::model::*;
		use bairelay_neolink_core::bc::xml::*;
		use bairelay_neolink_core::bc_protocol::connection::mock::{reply_200_xml, MockConnection};

		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(3),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		// `get_snapshot` now gates on the `preview` ability — populate
		// it before the call so we exercise the snap path, not the
		// MissingAbility guard.
		cam.test_set_ability("preview", false).await;

		let injector_task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(50)).await;
			let push = bairelay_neolink_core::bc::model::Bc {
				meta: bairelay_neolink_core::bc::model::BcMeta {
					msg_id: MSG_ID_SNAP,
					channel_id: 0,
					msg_num: 42,
					stream_type: 0,
					response_code: 201,
					class: 0x6414,
				},
				body: bairelay_neolink_core::bc::model::BcBody::ModernMsg(
					bairelay_neolink_core::bc::model::ModernMsg {
						extension: Some(Extension {
							binary_data: Some(1),
							..Default::default()
						}),
						payload: Some(bairelay_neolink_core::bc::model::BcPayloads::Binary(vec![
							0xDE, 0xAD, 0xBE,
						])),
					},
				),
			};
			injector.push(push).await;
		});

		let outcome = tokio::time::timeout(
			Duration::from_millis(500),
			run(&cam, Some(&path), false, false),
		)
		.await
		.expect("did not hang")
		.expect("ok");
		assert!(matches!(outcome, Outcome::Snapshot { .. }));
		injector_task.await.ok();
	}

	/// `run` with `use_stream=true` (the `--use-stream-raw` path)
	/// against a mock-connection BcCamera that lacks the "preview"
	/// ability — drives through `capture_via_stream`, hits the
	/// start_video Err arm (line 81-84), runs the unconditional
	/// `stop_video` line 86, then propagates the error through
	/// `finish_stream_capture`. Pins the use_stream branch of run().
	#[tokio::test]
	async fn run_use_stream_propagates_start_video_error() {
		use bairelay_neolink_core::bc_protocol::connection::mock::MockConnection;
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// No "preview" ability set → start_video errors with
		// MissingAbility, which bubbles through drain_first_iframe.
		let err = run(&cam, None, false, true).await.expect_err("should fail");
		assert!(format!("{:#}", err).contains("start_video failed"));
	}

	#[tokio::test]
	async fn drain_skips_multiple_non_iframes_then_succeeds() {
		use bairelay_neolink_core::bcmedia::model::{BcMediaAac, BcMediaPframe};
		let mut stream = MockVideoStream::new(vec![
			MockStep::Frame(BcMedia::Aac(BcMediaAac {
				data: vec![0x01, 0x02],
			})),
			MockStep::Frame(BcMedia::Pframe(BcMediaPframe {
				video_type: VideoType::H264,
				microseconds: 0,
				data: vec![],
			})),
			MockStep::Frame(BcMedia::Iframe(ifr(VideoType::H264))),
		]);
		let got = drain_first_iframe(&mut stream, Duration::from_millis(500))
			.await
			.expect("ok");
		assert!(matches!(got.video_type, VideoType::H264));
	}
}
