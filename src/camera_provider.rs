//! Adapter from the RTSP `StreamProvider` trait to the binary's
//! `CameraHandle` + `StreamSource`.
//!
//! Sits between [`bairelay_rtsp`]'s server runtime (which only knows the
//! `StreamProvider` trait) and the per-camera task tree owned by the
//! orchestrator. Looks up the requested camera, enforces any per-camera
//! ACL, acquires a wake lock for the session lifetime, and hands back a
//! [`SubscriptionHandle`] populated with the live broadcast receiver,
//! current SDP parameters, and the shared last-frame buffer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bairelay_rtsp::provider::{StreamError, StreamProvider, SubscriptionHandle};
use bairelay_rtsp::url::StreamKind;

use crate::camera::CameraHandle;
use crate::config::StreamConfig;
use crate::wake_lock::WakeLockGuard;

/// Deadline the provider waits for the first video SDP parameters before
/// giving up and returning `StreamError::Unavailable`. Must cover a cold
/// battery-camera wake followed by the first keyframe.
const SDP_READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Return (should_await_audio, audio_bonus_window) based on the camera's
/// current `AudioPresence`. `wait_audio = true` means subscribe must wait
/// for `SdpParams.audio.is_some()` in addition to video before returning
/// a subscription to the RTSP server. `bonus_window` is zero unless the
/// state is `Unknown`, in which case the subscribe path grants a short
/// additional deadline after video is ready to observe audio before
/// committing to a video-only SDP response.
pub(crate) fn classify_presence(
	p: crate::audio_presence::AudioPresence,
) -> (bool, std::time::Duration) {
	use crate::audio_presence::AudioPresence;
	match p {
		AudioPresence::Present { .. } => (true, std::time::Duration::ZERO),
		AudioPresence::Absent => (false, std::time::Duration::ZERO),
		AudioPresence::Unknown => (false, std::time::Duration::from_secs(2)),
	}
}

/// How the `stream` option spells itself, so a refusal can quote the line the
/// operator has to change.
fn stream_option(config: StreamConfig) -> &'static str {
	match config {
		StreamConfig::None => "none",
		StreamConfig::All => "all",
		StreamConfig::Main => "main",
		StreamConfig::Sub => "sub",
		StreamConfig::Extern => "extern",
	}
}

/// Answer a subscribe only for the streams this camera was configured to serve.
///
/// The URL suffix picks which stream a client *asks for*; `stream` in the
/// camera's config block decides which ones the camera will *answer*. That split
/// is the whole point of the option, and on a battery doorbell it is not a
/// cosmetic one: a main-stream request nobody configured wakes the radio,
/// brings the sensor up at full bitrate and holds the wake lock for the length
/// of the session.
///
/// `all` is the default, and what every configuration that never mentioned the
/// option gets, so this changes nothing for anyone who left it alone.
///
/// The refusal is `Unavailable` rather than a new error because that is the
/// signal the RTSP server already acts on: an `extern` request answered
/// unavailable is retried as `sub`. A camera configured `sub` therefore still
/// serves `/extern`, on the substream, through that retry; a camera configured
/// `main` refuses both halves of it and the client sees the error.
fn serve_stream(
	camera: &str,
	configured: StreamConfig,
	requested: StreamKind,
) -> Result<(), StreamError> {
	let allowed = match configured {
		StreamConfig::All => true,
		StreamConfig::None => false,
		StreamConfig::Main => requested == StreamKind::Main,
		StreamConfig::Sub => requested == StreamKind::Sub,
		StreamConfig::Extern => requested == StreamKind::Extern,
	};
	if allowed {
		return Ok(());
	}
	let message = if configured == StreamConfig::None {
		format!(
			"camera {camera:?} is configured with stream = \"none\" and serves no RTSP stream; \
			 set stream = \"all\" (or the stream it should serve) to answer {requested}"
		)
	} else {
		format!(
			"camera {camera:?} is configured with stream = \"{}\"; refusing {requested}; \
			 set stream = \"all\" to serve every stream",
			stream_option(configured)
		)
	};
	Err(StreamError::Unavailable(message))
}

/// Provider that adapts a camera registry into the RTSP `StreamProvider`
/// trait.
pub struct CameraProvider {
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
}

impl CameraProvider {
	/// Construct from the orchestrator's camera registry (typically
	/// obtained via `Orchestrator::cameras_arc`).
	pub fn new(cameras: Arc<HashMap<String, Arc<CameraHandle>>>) -> Self {
		Self { cameras }
	}
}

#[async_trait::async_trait]
impl StreamProvider for CameraProvider {
	async fn subscribe(
		&self,
		camera: &str,
		kind: StreamKind,
		authenticated_user: Option<&str>,
	) -> Result<SubscriptionHandle, StreamError> {
		let handle = self.cameras.get(camera).ok_or(StreamError::UnknownCamera)?;

		// Per-camera access control. An empty allowlist means "any
		// authenticated user (or anonymous, when no auth is configured)
		// is allowed". Task 24 will add validation that the listed names
		// exist in the global `[[users]]` table.
		if let Some(user) = authenticated_user {
			let permitted = &handle.config().permitted_users;
			if !permitted.is_empty() && !permitted.iter().any(|u| u == user) {
				return Err(StreamError::AccessDenied);
			}
		}

		// Which streams this camera may serve. Before the wake lock on purpose:
		// a request the camera will not serve must not cost a sleeping camera a
		// wake-up, nor hold a reference that keeps it awake while it is refused.
		serve_stream(camera, handle.config().stream, kind)?;

		// Acquire a wake lock for the session. The guard rides inside
		// the returned `SubscriptionHandle` and is dropped by the RTSP
		// session task when the client disconnects, releasing the
		// reference and letting the grace-period watcher tear the
		// camera down if no other consumers remain.
		let guard: WakeLockGuard = handle.wake_lock().acquire();

		let source = handle.stream_source(kind).await?;

		// Branch on the camera's observed audio presence. On timeout we
		// surface Unavailable → 503, which is the appropriate user-visible
		// signal that the camera never produced usable bitstream. See
		// `classify_presence` for the policy; §3.3 of the 		// design doc spells out the rationale.
		let presence = *handle
			.audio_presence()
			.read()
			.expect("presence lock poisoned");
		let (wait_audio, bonus_window) = classify_presence(presence);

		let sdp_params = if wait_audio {
			// Present { codec }: wait for both video AND audio within
			// the same 15 s budget.
			source
				.await_sdp_both_ready(SDP_READY_TIMEOUT)
				.await
				.map_err(|e| StreamError::Unavailable(e.to_string()))?
		} else {
			// Absent or Unknown: wait for video only. The `?` drops the
			// returned snapshot — we re-read `sdp_params()` below so the
			// Unknown bonus window's result is reflected.
			source
				.await_sdp_ready(SDP_READY_TIMEOUT)
				.await
				.map_err(|e| StreamError::Unavailable(e.to_string()))?;
			if !bonus_window.is_zero() {
				// Unknown: give audio a bounded bonus window to show up.
				// Ignore the result — if audio didn't arrive, the final
				// snapshot stays video-only. A bonus-window timeout is
				// not a subscribe failure; the camera is already
				// streaming video.
				let _ = source.await_audio(bonus_window).await;
			}
			// Re-read: audio may have populated during the bonus window
			// (reader task's CAS upgraded presence to Present).
			source.sdp_params()
		};

		let frames = source.subscribe();
		let last_frame = source.last_frame();

		Ok(SubscriptionHandle {
			frames,
			sdp_params,
			last_frame,
			guard: Box::new(guard),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Compile-time check that `CameraProvider` satisfies the
	/// `StreamProvider` trait bound (which itself implies `Send + Sync +
	/// 'static`).
	#[test]
	fn camera_provider_implements_stream_provider() {
		fn assert_impl<T: StreamProvider>() {}
		assert_impl::<CameraProvider>();
	}

	#[test]
	fn classify_present_waits_for_audio_no_bonus() {
		use bairelay_rtsp::codec::AudioCodec;
		let (wait, bonus) = classify_presence(crate::audio_presence::AudioPresence::Present {
			codec: AudioCodec::Aac,
		});
		assert!(wait);
		assert_eq!(bonus, std::time::Duration::ZERO);
	}

	#[test]
	fn classify_absent_no_wait_no_bonus() {
		let (wait, bonus) = classify_presence(crate::audio_presence::AudioPresence::Absent);
		assert!(!wait);
		assert_eq!(bonus, std::time::Duration::ZERO);
	}

	#[test]
	fn classify_unknown_bonus_window_two_seconds() {
		let (wait, bonus) = classify_presence(crate::audio_presence::AudioPresence::Unknown);
		assert!(!wait);
		assert_eq!(bonus, std::time::Duration::from_secs(2));
	}

	#[tokio::test]
	async fn camera_provider_subscribe_unknown_camera_rejects() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;
		// Non-empty registry, but a subscribe for a different name must
		// return UnknownCamera without touching anything else.
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("registered"),
			cancel,
			None,
		));
		let mut map = HashMap::new();
		map.insert("registered".to_string(), handle);
		let provider = CameraProvider::new(Arc::new(map));
		let err = provider
			.subscribe("nope", StreamKind::Main, None)
			.await
			.err()
			.expect("unknown camera");
		assert!(matches!(err, StreamError::UnknownCamera));
	}

	#[tokio::test]
	async fn camera_provider_subscribe_rejects_user_not_in_acl() {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;
		// Camera with a non-empty permitted_users list. A request by a
		// different authenticated user must return AccessDenied before
		// we ever touch the wake lock or the stream source.
		let cancel = CancellationToken::new();
		let mut cfg = minimal_camera_config("locked");
		cfg.permitted_users = vec!["alice".to_string()];
		let handle = Arc::new(CameraHandle::new(cfg, cancel, None));
		let mut map = HashMap::new();
		map.insert("locked".to_string(), handle);
		let provider = CameraProvider::new(Arc::new(map));
		let err = provider
			.subscribe("locked", StreamKind::Main, Some("bob"))
			.await
			.err()
			.expect("acl denies bob");
		assert!(matches!(err, StreamError::AccessDenied));
	}

	/// A provider around one camera configured with `stream`. The camera's
	/// cancellation token is already cancelled, so a request that *passes* the
	/// stream gate gets as far as `stream_source` and fails there, naming the
	/// shutdown: that is how these tests tell "refused by the option" apart from
	/// "allowed, and then no session behind it".
	fn provider_serving(stream: StreamConfig) -> CameraProvider {
		use crate::config::test_helpers::minimal_camera_config;
		use tokio_util::sync::CancellationToken;
		let mut cfg = minimal_camera_config("cam");
		cfg.stream = stream;
		let cancel = CancellationToken::new();
		cancel.cancel();
		let mut map = HashMap::new();
		map.insert("cam".to_string(), Arc::new(CameraHandle::new(cfg, cancel, None)));
		CameraProvider::new(Arc::new(map))
	}

	/// Subscribe and report why it failed. `pass` marks the shutdown failure: the
	/// request cleared the gate and there was simply no session to serve it.
	async fn outcome(provider: &CameraProvider, kind: StreamKind) -> String {
		match provider.subscribe("cam", kind, None).await {
			Ok(_) => "subscribed".to_string(),
			Err(StreamError::Unavailable(msg)) if msg.contains("shutting down") => "pass".to_string(),
			Err(StreamError::Unavailable(msg)) => msg,
			Err(e) => format!("other: {e:?}"),
		}
	}

	#[tokio::test]
	async fn stream_all_serves_every_stream() {
		let provider = provider_serving(StreamConfig::All);
		for kind in [StreamKind::Main, StreamKind::Sub, StreamKind::Extern] {
			assert_eq!(outcome(&provider, kind).await, "pass", "{kind} is configured");
		}
	}

	#[tokio::test]
	async fn stream_none_refuses_every_stream_and_names_the_option() {
		let provider = provider_serving(StreamConfig::None);
		for kind in [StreamKind::Main, StreamKind::Sub, StreamKind::Extern] {
			let why = outcome(&provider, kind).await;
			assert!(why.contains("stream = \"none\""), "{kind}: {why}");
			assert!(why.contains("stream = \"all\""), "{why} should name the line to change");
		}
	}

	#[tokio::test]
	async fn stream_main_serves_main_only() {
		let provider = provider_serving(StreamConfig::Main);
		assert_eq!(outcome(&provider, StreamKind::Main).await, "pass");
		for kind in [StreamKind::Sub, StreamKind::Extern] {
			let why = outcome(&provider, kind).await;
			assert!(why.contains("stream = \"main\""), "{kind}: {why}");
			assert!(why.contains(&format!("refusing {kind}")), "{kind}: {why}");
		}
	}

	#[tokio::test]
	async fn stream_sub_serves_sub_only() {
		let provider = provider_serving(StreamConfig::Sub);
		assert_eq!(outcome(&provider, StreamKind::Sub).await, "pass");
		let why = outcome(&provider, StreamKind::Main).await;
		assert!(why.contains("stream = \"sub\""), "the refusal quotes the option: {why}");
		assert!(why.contains("refusing main"), "and names what it refused: {why}");
		assert!(why.contains("stream = \"all\""), "and the way to allow it: {why}");
	}

	#[tokio::test]
	async fn stream_extern_serves_extern_only() {
		let provider = provider_serving(StreamConfig::Extern);
		assert_eq!(outcome(&provider, StreamKind::Extern).await, "pass");
		for kind in [StreamKind::Main, StreamKind::Sub] {
			let why = outcome(&provider, kind).await;
			assert!(why.contains("stream = \"extern\""), "{kind}: {why}");
		}
	}

	/// The fallback the RTSP server runs on its side: an `extern` request that
	/// comes back `Unavailable` is retried as `sub`. A camera configured for the
	/// substream therefore *does* serve `/extern`, on the substream — the refusal
	/// above is what routes it there, not what breaks it.
	#[tokio::test]
	async fn extern_on_a_sub_camera_ends_up_on_the_substream() {
		let provider = provider_serving(StreamConfig::Sub);
		let refused = outcome(&provider, StreamKind::Extern).await;
		assert!(refused.contains("refusing extern"), "extern is refused: {refused}");
		assert_eq!(
			outcome(&provider, StreamKind::Sub).await,
			"pass",
			"the server's retry as sub must clear the gate, or /extern breaks"
		);
	}

	/// The other half of the same rule: with `stream = "main"` there is nothing
	/// for the fallback to fall back to, so both halves fail and the client gets
	/// the error — twice the same reason, which is what the operator reads.
	#[tokio::test]
	async fn extern_on_a_main_camera_fails_twice() {
		let provider = provider_serving(StreamConfig::Main);
		let first = outcome(&provider, StreamKind::Extern).await;
		let second = outcome(&provider, StreamKind::Sub).await;
		for why in [&first, &second] {
			assert!(why.contains("stream = \"main\""), "both halves name the option: {why}");
		}
		assert!(first.contains("refusing extern") && second.contains("refusing sub"));
	}
}
