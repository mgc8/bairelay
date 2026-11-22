//! Shared connect → op → logout runner for CLI oneshot subcommands.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bairelay_neolink_core::bc_protocol::BcCamera;
use futures::future::BoxFuture;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::bc_opts::{build_bc_opts_resolved, max_encryption};
use crate::config::CameraConfig;
use crate::oneshot::errors::InterruptedError;

// Backstop only — the discovery retry loop owns the real timing. It runs 5
// rounds (each bounded by REGISTRATION_ROUND_TIMEOUT = 8 s) with exponential
// backoff (1,2,4,8,16 s), so it self-terminates with a named-relay error in
// ~80 s worst case and returns the instant a reachable camera answers. This
// deadline just has to sit above that budget so it never preempts a retry
// (the old 30 s cut the loop off after ~2 rounds); it only fires on a genuine
// hang in the non-retry discovery / socket-setup paths.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(100);
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
const OP_TIMEOUT: Duration = Duration::from_secs(30);
const LOGOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect, run `op`, log out. Honours Ctrl+C / SIGTERM via `cancel`; on cancel
/// we skip logout, short-circuit with `InterruptedError`, and rely on
/// the camera to park itself via its own idle timeout once our TCP
/// session drops.
pub async fn run<F, T>(cfg: &CameraConfig, cancel: CancellationToken, op: F) -> Result<T>
where
	F: for<'a> FnOnce(&'a BcCamera) -> BoxFuture<'a, Result<T>>,
{
	run_with_op_timeout(cfg, cancel, OP_TIMEOUT, op).await
}

/// [`run`] with an explicit op deadline.
///
/// `OP_TIMEOUT` is sized for request/response commands. A talk transmission
/// paces itself at wall-clock speed (a 40 s clip takes 40 s to send) and
/// `audio-bridge` is a long-running bridge, so those pass their own budget
/// instead of being cut off at 30 s. Connect / login / logout budgets and the
/// cancel handling are unchanged.
pub async fn run_with_op_timeout<F, T>(
	cfg: &CameraConfig,
	cancel: CancellationToken,
	op_timeout: Duration,
	op: F,
) -> Result<T>
where
	F: for<'a> FnOnce(&'a BcCamera) -> BoxFuture<'a, Result<T>>,
{
	tokio::select! {
		// A shutdown signal wins the race — skip logout; dropping the TCP session
		// is enough for the camera to park via its own idle timeout.
		// `biased` would force this arm first, but tokio::select!'s
		// random polling already resolves a pre-cancelled token in
		// roughly the same number of poll cycles.
		_ = cancel.cancelled() => Err(InterruptedError::new().into()),
		res = connect_login_logout(cfg, op_timeout, op) => res,
	}
}

/// [`run_with_op_timeout`] **without** the outer cancellation race, for the one
/// op that owns state the camera keeps alive on its side.
///
/// A talk session is only released by msg id 11 (`talk_stop`). Racing the
/// cancellation token would drop the op future the instant Ctrl+C lands: no
/// flush of the queued blocks, no `talk_stop`, and the doorbell sits in a
/// half-open talk session — mute to its own microphone, holding the talk slot —
/// until its internal timer clears it. Here the token is handed *into* the op
/// instead, so the bridge can drain, close the session, and return
/// [`InterruptedError`] itself; logout then runs normally.
///
/// The op deadline is the only backstop, so an op using this entry point must
/// watch the token. That is a real trade, stated plainly: a bridge that ignored
/// cancellation would run until `op_timeout`, which is why
/// [`super::audio_bridge`] caps itself at a finite (if generous) budget rather
/// than no deadline at all.
pub async fn run_graceful<F, T>(
	cfg: &CameraConfig,
	cancel: CancellationToken,
	op_timeout: Duration,
	op: F,
) -> Result<T>
where
	F: for<'a> FnOnce(&'a BcCamera, CancellationToken) -> BoxFuture<'a, Result<T>>,
{
	let token = cancel.clone();
	connect_login_logout(cfg, op_timeout, move |camera| op(camera, token.clone())).await
}

/// Connect, log in, apply the per-camera ability override, run `op` under
/// `op_timeout`, log out. Shared by both entry points above so the session
/// bookkeeping cannot drift between them.
async fn connect_login_logout<F, T>(cfg: &CameraConfig, op_timeout: Duration, op: F) -> Result<T>
where
	F: for<'a> FnOnce(&'a BcCamera) -> BoxFuture<'a, Result<T>>,
{
	let max_enc = max_encryption(cfg);
	let opts = build_bc_opts_resolved(cfg).await?;
	let camera = timeout(CONNECT_TIMEOUT, BcCamera::new(&opts))
		.await
		.context("connect timed out")?
		.context("connect failed")?;
	timeout(LOGIN_TIMEOUT, camera.login_with_maxenc(max_enc))
		.await
		.context("login timed out")?
		.context("login failed")?;
	// Per-camera ability override, off by default. The battery doorbell
	// answers `<TalkAbility>` (msg 10) with a complete FDX adpcm 16 kHz
	// configuration while its `abilityInfo` lists no `two_way_audio` entry at
	// all — a firmware under-declaration verified with the `talk-ability`
	// probe. Only a camera whose config says `two_way_audio = true` gets its
	// gate lifted; every other camera keeps refusing the talk path exactly as
	// before, and the `abilities` diagnostic keeps reporting the camera's own
	// (incomplete) report either way.
	if cfg.two_way_audio {
		camera.declare_ability("two_way_audio").await;
	}
	// Run the op + logout in sequence regardless of op outcome.
	// Previously a `?` on the timeout-Elapsed case skipped the
	// logout: a hung op would leave the camera holding our
	// session-table slot until its own idle timer expired,
	// instead of releasing immediately on `logout()`. Battery
	// cams park on TCP-drop either way, but explicit logout is
	// faster and matches the comment's intent.
	let op_result: Result<T> = match timeout(op_timeout, op(&camera)).await {
		Ok(inner) => inner,
		Err(_) => Err(anyhow!(
			"operation timed out after {} s",
			op_timeout.as_secs()
		)),
	};
	let _ = timeout(LOGOUT_TIMEOUT, camera.logout()).await;
	op_result
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;

	/// A pre-cancelled token resolves the `cancel.cancelled()` arm
	/// before the body's `BcCamera::new` ever attempts a socket connect.
	/// Verifies the cancel branch surfaces `InterruptedError` and skips
	/// the op/logout. This is the only run() path reachable without a
	/// real socket — the rest of `runner::run` is covered by `manual-
	/// verify.sh` against live cameras (see `docs/testing.md`).
	#[tokio::test]
	async fn pre_cancelled_token_short_circuits_with_interrupted_error() {
		let cfg = minimal_camera_config("cancel-test");
		let cancel = CancellationToken::new();
		cancel.cancel();

		// Trip-wire op: must NOT run. If reached, the cancel arm lost
		// the race and we have a regression.
		let op_ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
		let op_ran_inner = op_ran.clone();

		let result: Result<()> = run(&cfg, cancel, move |_cam| {
			let op_ran_inner = op_ran_inner.clone();
			Box::pin(async move {
				op_ran_inner.store(true, std::sync::atomic::Ordering::Relaxed);
				Ok(())
			})
		})
		.await;

		let err = result.expect_err("pre-cancel must error");
		assert!(
			err.downcast_ref::<InterruptedError>().is_some(),
			"expected InterruptedError, got: {err:#}"
		);
		assert!(
			!op_ran.load(std::sync::atomic::Ordering::Relaxed),
			"op closure must not run when cancel fires first"
		);
	}
}
