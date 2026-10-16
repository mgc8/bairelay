//! Startup wake cycle: warm each camera's last-frame buffer at boot.
//!
//! For every configured camera we briefly hold a wake lock, pull the Main
//! stream long enough to capture an I-frame, then ask the camera for a JPEG
//! snapshot and stash it in the shared [`LastFrameBuffer`](bairelay_rtsp::buffer::LastFrameBuffer). When the wake
//! lock guard drops at the end of the per-camera task the normal grace
//! period kicks in, so battery cameras can return to sleep without any
//! special-casing.
//!
//! Runs concurrently across cameras and tolerates per-camera failures: a
//! camera that cannot be reached at boot simply ends up with an empty
//! buffer until the next real client request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::camera::CameraHandle;
use crate::camera_tasks::SNAPSHOT_TIMEOUT;

/// Maximum total time spent warming a single camera (acquire wake lock,
/// wait for connect, pull I-frame, capture snapshot).
const PER_CAMERA_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum time to wait for the first I-frame after the stream source has
/// been requested.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(15);

/// Poll interval while waiting for the first I-frame.
const FRAME_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Window during which startup wake watches for a first audio packet
/// after the first I-frame has landed. Whatever arrives (or doesn't)
/// commits the camera's `AudioPresence` — so subsequent RTSP subscribes
/// see `Present { codec }` or `Absent` rather than `Unknown`.
///
/// 2 s is a reasonable fixed value for the observed Argus firmware; no
/// config knob yet — introduce one only if a future model needs longer.
const AUDIO_OBSERVE_WINDOW: Duration = Duration::from_secs(2);

/// Warm the last-frame buffer for every camera in `cameras`.
///
/// Each camera is processed concurrently in its own spawned task and capped
/// by `PER_CAMERA_TIMEOUT`. Returns once every per-camera task has
/// finished (success or failure). This is a mandatory part of boot: the
/// last-frame buffer backs the RTSP placeholder during wake, the MQTT
/// preview topic, and HA discovery's capability-gated entities (PTZ
/// detection happens during the brief connect that this cycle triggers).
///
/// `cancel` is the global shutdown token. If it fires during the warm cycle
/// each in-flight camera task bails out on the next poll so Ctrl+C during
/// startup doesn't stall up to `PER_CAMERA_TIMEOUT` per camera waiting on
/// a dead network.
pub async fn warm_last_frame_buffers(
	cameras: &Arc<HashMap<String, Arc<CameraHandle>>>,
	cancel: CancellationToken,
) {
	if cameras.is_empty() {
		return;
	}

	tracing::info!(cameras = cameras.len(), "Starting startup wake cycle");

	let mut set = tokio::task::JoinSet::new();
	for (name, handle) in cameras.iter() {
		let handle = Arc::clone(handle);
		let name = name.clone();
		let cancel_task = cancel.clone();
		set.spawn(async move {
			// Hold a wake lock for the lifetime of this task. The drop at
			// end of scope kicks off the normal grace-period countdown.
			let _guard = handle.wake_lock().acquire();
			tokio::select! {
				_ = cancel_task.cancelled() => {
					tracing::debug!(camera = %name, "startup wake cancelled");
				}
				result = tokio::time::timeout(
					PER_CAMERA_TIMEOUT,
					warm_one(&handle, &name, cancel_task.clone()),
				) => match result {
					Ok(Ok(())) => {}
					Ok(Err(e)) => {
						tracing::warn!(camera = %name, error = %e, "startup wake failed");
					}
					Err(_) => {
						tracing::warn!(
							camera = %name,
							timeout_s = PER_CAMERA_TIMEOUT.as_secs(),
							"startup wake timed out"
						);
					}
				},
			}
		});
	}

	while set.join_next().await.is_some() {}
	tracing::info!("Startup wake cycle complete");
}

/// Warm a single camera: wait for Connected, pull an I-frame, capture and
/// store a JPEG snapshot. The caller owns the wake-lock guard.
///
/// Every awaited step is raced against `cancel` so Ctrl+C during startup
/// returns immediately rather than waiting for per-step timeouts.
async fn warm_one(
	handle: &CameraHandle,
	name: &str,
	cancel: CancellationToken,
) -> anyhow::Result<()> {
	use bairelay_rtsp::url::StreamKind;

	tracing::info!(camera = %name, "Warming last-frame buffer");

	// 1) Get the Main stream source (this internally waits for Connected).
	let source = tokio::select! {
		_ = cancel.cancelled() => anyhow::bail!("cancelled"),
		r = handle.stream_source(StreamKind::Main) => {
			r.map_err(|e| anyhow::anyhow!("stream_source failed: {e}"))?
		}
	};

	// 2) Wait up to FIRST_FRAME_TIMEOUT for the buffer to receive at least
	//    one I-frame so SDP params are populated and the burst is real.
	let last_frame = source.last_frame();
	let start = std::time::Instant::now();
	while !last_frame.has_video() {
		if start.elapsed() > FIRST_FRAME_TIMEOUT {
			anyhow::bail!("no video in {}s", FIRST_FRAME_TIMEOUT.as_secs());
		}
		tokio::select! {
			_ = cancel.cancelled() => anyhow::bail!("cancelled"),
			_ = tokio::time::sleep(FRAME_POLL_INTERVAL) => {}
		}
	}

	// 3) Observe audio for a bounded window. Whatever arrives (or doesn't)
	//    commits the camera's AudioPresence so the next subscribe skips its
	//    own Unknown bonus window. If `cancel` fires mid-window we settle
	//    for Absent; later mid-session observations can still upgrade
	//    Absent → Present via AudioPresence::observed.
	let sdp_handle = source.sdp_params_handle();
	let observed = tokio::select! {
		_ = cancel.cancelled() => None,
		c = observe_audio_presence(&sdp_handle, AUDIO_OBSERVE_WINDOW) => c,
	};
	let new_presence = match observed {
		Some(c) => crate::audio_presence::AudioPresence::Present { codec: c },
		None => crate::audio_presence::AudioPresence::Absent,
	};
	*handle
		.audio_presence()
		.write()
		.expect("presence lock poisoned") = new_presence;
	tracing::info!(
		camera = %name,
		presence = ?new_presence,
		"audio presence observed at startup"
	);

	// 4) Request a JPEG snapshot from the camera and stash it.
	if let Some(camera) = handle.bc_camera() {
		capture_snapshot_into_buffer(&camera, name, &last_frame, &cancel).await;
	} else {
		tracing::warn!(camera = %name, "bc_camera unavailable for snapshot");
	}

	// 5) Release our own Arc<StreamSource>, but leave the source registered
	//    on the CameraHandle. An RTSP client that connects during or just
	//    after the warm cycle reuses the live source instead of triggering
	//    a fresh `start_video` — which is important because a concurrent
	//    teardown here would race with the client's SETUP/PLAY path and
	//    strand the client waiting for RTP on a dead broadcast receiver.
	//    Normal teardown happens via the wake-lock release → grace period
	//    on `CameraHandle` (the `_guard` dropped at the end of the spawned
	//    task in `warm_last_frame_buffers`), at which point the handle's
	//    `stop_all_stream_sources` runs.
	drop(source);

	Ok(())
}

/// Request a JPEG snapshot via [`CameraDriver::get_snapshot`] (retrying the wake-time 400 and bounded per attempt by [`SNAPSHOT_TIMEOUT`]) and push successful bytes into
/// `last_frame`. Errors / timeouts log at warn and leave the buffer
/// untouched. Lifted out of [`warm_one`] so behaviour tests can drive
/// it against a `FakeCamera` without the stream / audio dependencies.
pub(crate) async fn capture_snapshot_into_buffer(
	camera: &std::sync::Arc<dyn bairelay_neolink_core::bc_protocol::CameraDriver>,
	name: &str,
	last_frame: &bairelay_rtsp::buffer::LastFrameBuffer,
	cancel: &CancellationToken,
) {
	let Some(result) =
		crate::camera_tasks::snapshot_with_retry(camera.as_ref(), name, cancel).await
	else {
		return;
	};
	match result {
		Ok(Ok(bytes)) => {
			last_frame.set_jpeg(bytes::Bytes::from(bytes));
			tracing::info!(camera = %name, "Captured startup JPEG snapshot");
		}
		Ok(Err(e)) => {
			tracing::warn!(camera = %name, error = %e, "snapshot request failed");
		}
		Err(_) => {
			tracing::warn!(
				camera = %name,
				timeout_s = SNAPSHOT_TIMEOUT.as_secs(),
				"snapshot request timed out"
			);
		}
	}
}

/// Poll `sdp.audio` for up to `deadline`. Returns the detected codec
/// on arrival, or `None` on timeout. The caller then updates
/// `AudioPresence`:
///   - `Some(codec)` → `AudioPresence::Present { codec }`.
///   - `None`        → `AudioPresence::Absent`.
///
/// Lives in startup_wake (not stream_source) because only the
/// startup-wake path makes the `Absent` decision; during normal
/// streaming the reader task only upgrades on observation, never
/// closes the window.
pub(crate) async fn observe_audio_presence(
	sdp: &std::sync::Arc<std::sync::RwLock<bairelay_rtsp::sdp::SdpParams>>,
	deadline: std::time::Duration,
) -> Option<bairelay_rtsp::codec::AudioCodec> {
	let start = std::time::Instant::now();
	loop {
		if let Some(a) = sdp.read().expect("sdp lock poisoned").audio.as_ref() {
			return Some(a.codec);
		}
		if start.elapsed() > deadline {
			return None;
		}
		tokio::time::sleep(crate::stream_source::SDP_POLL_INTERVAL).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_rtsp::codec::AudioCodec;
	use bairelay_rtsp::sdp::{AudioParams, SdpParams};
	use std::sync::{Arc, RwLock};
	use std::time::Duration;

	fn empty_sdp() -> Arc<RwLock<SdpParams>> {
		Arc::new(RwLock::new(SdpParams {
			server_ip: "0".into(),
			session_id: "0".into(),
			session_name: "u".into(),
			video: None,
			audio: None,
		}))
	}

	#[tokio::test(flavor = "current_thread")]
	async fn observe_audio_returns_codec_when_arrives() {
		let sdp = empty_sdp();
		let sdp2 = Arc::clone(&sdp);
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(50)).await;
			sdp2.write().unwrap().audio = Some(AudioParams {
				codec: AudioCodec::Aac,
				payload_type: 97,
				sample_rate: 16_000,
				channels: 1,
				asc_hex: Some("1408".into()),
			});
		});
		let r = observe_audio_presence(&sdp, Duration::from_secs(1)).await;
		assert_eq!(r, Some(AudioCodec::Aac));
	}

	#[tokio::test(flavor = "current_thread")]
	async fn observe_audio_returns_none_on_timeout() {
		let sdp = empty_sdp();
		let r = observe_audio_presence(&sdp, Duration::from_millis(200)).await;
		assert_eq!(r, None);
	}

	/// Happy-path snapshot capture: the `FakeCamera`'s closure returns
	/// a known byte blob, and `capture_snapshot_into_buffer` pushes
	/// those exact bytes into the shared `LastFrameBuffer`.
	#[tokio::test]
	async fn capture_snapshot_into_buffer_warms_last_frame() {
		use bairelay_neolink_core::bc_protocol::{CameraDriver, FakeCameraBuilder};
		use bairelay_rtsp::buffer::LastFrameBuffer;

		let jpeg_bytes: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0xAA, 0xBB];
		let expected = jpeg_bytes.clone();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || Ok(jpeg_bytes.clone()))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let lfb = LastFrameBuffer::new();
		capture_snapshot_into_buffer(&driver, "cam1", &lfb, &CancellationToken::new()).await;

		let got = lfb
			.jpeg()
			.expect("buffer should be populated after snapshot");
		assert_eq!(got.as_ref(), expected.as_slice());
	}

	/// If the camera returns `Err` on snapshot, the buffer stays
	/// empty — the warn-and-move-on path. Guards against a regression
	/// where an error path silently writes garbage into the buffer.
	#[tokio::test]
	async fn capture_snapshot_into_buffer_leaves_buffer_empty_on_error() {
		use bairelay_neolink_core::bc_protocol::{CameraDriver, FakeCameraBuilder};
		use bairelay_rtsp::buffer::LastFrameBuffer;

		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"camera declined snapshot",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let lfb = LastFrameBuffer::new();
		capture_snapshot_into_buffer(&driver, "cam1", &lfb, &CancellationToken::new()).await;

		assert!(
			lfb.jpeg().is_none(),
			"snapshot error must not populate last_frame_buffer"
		);
	}

	/// `warm_last_frame_buffers` on an empty map returns immediately —
	/// the early-return guard on line 62.
	#[tokio::test]
	async fn warm_last_frame_buffers_empty_map_returns_instantly() {
		use std::collections::HashMap;
		let cameras: Arc<HashMap<String, Arc<crate::camera::CameraHandle>>> =
			Arc::new(HashMap::new());
		let cancel = CancellationToken::new();
		// Any non-trivial budget is fine — the function should return
		// almost immediately on an empty map.
		tokio::time::timeout(
			Duration::from_millis(200),
			warm_last_frame_buffers(&cameras, cancel),
		)
		.await
		.expect("warm on empty map should return well under 200ms");
	}

	/// `warm_last_frame_buffers` with a single camera whose handle
	/// never connects — `warm_one` fails on the `stream_source()` wait
	/// branch, is timed out at `PER_CAMERA_TIMEOUT`, and the outer
	/// function still returns. The camera task respects `cancel` so
	/// we shut it down immediately rather than waiting 30 s.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn warm_last_frame_buffers_propagates_cancel() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;
		use std::collections::HashMap;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-nc"),
			cancel.clone(),
			None,
		));
		let mut map = HashMap::new();
		map.insert("cam-nc".to_string(), handle);
		let cameras = Arc::new(map);

		// Cancel slightly after launch so every per-camera task exits
		// on the cancellation arm of the select rather than the
		// 30 s per-camera timeout.
		let cancel_task = cancel.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			cancel_task.cancel();
		});

		warm_last_frame_buffers(&cameras, cancel).await;
		let _ = canceller.await;
	}

	/// `warm_last_frame_buffers` surfaces a per-camera failure (warm_one
	/// returned Err because state never reached Connected and the outer
	/// timeout fired) without propagating the error to the caller. Uses
	/// paused time to advance past `PER_CAMERA_TIMEOUT` virtually —
	/// real wall time stays sub-millisecond.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn warm_last_frame_buffers_times_out_per_camera() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;
		use std::collections::HashMap;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-to"),
			cancel.clone(),
			None,
		));
		let mut map = HashMap::new();
		map.insert("cam-to".to_string(), handle);
		let cameras = Arc::new(map);

		// Drive the full warm cycle: `warm_one` polls for Connected
		// (max 30s) and the outer wrapper times out at PER_CAMERA_TIMEOUT
		// (30s). Paused time means the whole thing completes in a single
		// scheduler tick once we advance enough virtual seconds.
		warm_last_frame_buffers(&cameras, cancel).await;
	}

	/// Drive `warm_one` through the frame-wait loop to its timeout
	/// branch: Connected state + pre-registered inert `StreamSource`
	/// lets `stream_source()` succeed via the fast path, then the
	/// `has_video()` polling loop runs until `FIRST_FRAME_TIMEOUT`
	/// expires and the fn bails out.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn warm_last_frame_buffers_completes_with_inert_source() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;
		use crate::stream_source::StreamSource;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use bairelay_rtsp::url::StreamKind;
		use std::collections::HashMap;
		use std::sync::Arc as StdArc;

		let jpeg: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0, 0xAA, 0xBB];
		let jpeg_clone = jpeg.clone();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || Ok(jpeg_clone.clone()))
			.build();
		// Prove the fake is dyn-compatible (same pattern the driver
		// uses elsewhere).
		let _: StdArc<dyn bairelay_neolink_core::bc_protocol::CameraDriver> = fake.clone();

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-inert"),
			cancel.clone(),
			None,
		));
		// Make `stream_source()` take the fast path by registering an
		// inert source up front. Flip state to Connected + install the
		// driver so `bc_camera()` also resolves.
		handle
			.insert_stream_source_for_test(StreamKind::Main, StreamSource::start_inert_for_test());
		handle.set_driver_for_test(fake);

		let mut map = HashMap::new();
		map.insert("cam-inert".to_string(), handle);
		let cameras = Arc::new(map);
		warm_last_frame_buffers(&cameras, cancel).await;
	}

	/// Drive `warm_last_frame_buffers` through the happy path of
	/// `warm_one`: Connected + pre-seeded `VideoBurst` so `has_video()`
	/// returns true immediately, `observe_audio_presence` runs its 2 s
	/// window and commits `Absent`, then `capture_snapshot_into_buffer`
	/// stashes the JPEG. Covers warm_one lines 129-187.
	#[tokio::test]
	async fn warm_last_frame_buffers_exercises_full_warm_one_happy_path() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;
		use crate::stream_source::StreamSource;
		use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;
		use bairelay_rtsp::buffer::VideoBurst;
		use bairelay_rtsp::codec::VideoCodec;
		use bairelay_rtsp::url::StreamKind;
		use std::collections::HashMap;
		use std::time::Instant as StdInstant;

		let jpeg_bytes: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x12, 0x34];
		let expected_jpeg = jpeg_bytes.clone();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || Ok(jpeg_bytes.clone()))
			.build();

		let (source, last_frame) =
			StreamSource::start_inert_for_test_with_gap_and_last_frame(Duration::from_secs(1));
		last_frame.replace_video(VideoBurst {
			codec: VideoCodec::H264,
			parameter_sets: vec![vec![0x67, 0x42, 0x00, 0x1f]],
			iframe_nals: vec![vec![0x65, 0xaa]],
			pframe_nals: vec![],
			captured_at: StdInstant::now(),
			captured_pts_90khz: 0,
		});

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-happy"),
			cancel.clone(),
			None,
		));
		handle.insert_stream_source_for_test(StreamKind::Main, Arc::clone(&source));
		handle.set_driver_for_test(fake);

		let mut map = HashMap::new();
		map.insert("cam-happy".to_string(), Arc::clone(&handle));
		let cameras = Arc::new(map);

		warm_last_frame_buffers(&cameras, cancel).await;

		// warm_one writes the snapshot JPEG into the source's
		// `LastFrameBuffer` (returned by `source.last_frame()`), not
		// the camera handle's top-level buffer.
		let got = source
			.last_frame()
			.jpeg()
			.expect("source buffer populated with snapshot");
		assert_eq!(got.as_ref(), expected_jpeg.as_slice());

		let presence = *handle.audio_presence().read().unwrap();
		assert_eq!(presence, crate::audio_presence::AudioPresence::Absent);
	}

	/// `observe_audio_presence` short-circuits when the SDP already
	/// carries audio — covers the fast-path `read().audio.is_some()`.
	#[tokio::test(flavor = "current_thread")]
	async fn observe_audio_short_circuits_when_sdp_already_has_audio() {
		let sdp = Arc::new(RwLock::new(SdpParams {
			server_ip: "0".into(),
			session_id: "0".into(),
			session_name: "u".into(),
			video: None,
			audio: Some(AudioParams {
				codec: AudioCodec::Aac,
				payload_type: 97,
				sample_rate: 16_000,
				channels: 1,
				asc_hex: None,
			}),
		}));
		let r = observe_audio_presence(&sdp, Duration::from_secs(5)).await;
		assert_eq!(r, Some(AudioCodec::Aac));
	}

	/// If the buffer already holds good bytes (e.g. from a prior
	/// successful poll) and a *subsequent* snapshot fails, the prior
	/// bytes must stay intact — an error path must never clobber the
	/// last-known-good frame. Stronger contract than the empty-buffer
	/// case above.
	#[tokio::test]
	async fn capture_snapshot_error_does_not_clobber_prior_frame() {
		use bairelay_neolink_core::bc_protocol::{CameraDriver, FakeCameraBuilder};
		use bairelay_rtsp::buffer::LastFrameBuffer;
		use bytes::Bytes;

		let good: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0, 0xCA, 0xFE];
		let lfb = LastFrameBuffer::new();
		lfb.set_jpeg(Bytes::from(good.clone()));

		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"snapshot declined",
				))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		capture_snapshot_into_buffer(&driver, "cam1", &lfb, &CancellationToken::new()).await;

		let after = lfb.jpeg().expect("prior frame must still be present");
		assert_eq!(
			after.as_ref(),
			good.as_slice(),
			"snapshot error must not replace cached frame bytes"
		);
	}
}
