//! Shared connect → op → logout runner for CLI oneshot subcommands.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bairelay_neolink_core::bc_protocol::BcCamera;
use futures::future::BoxFuture;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::bc_opts::{build_bc_opts, max_encryption};
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
	let opts = build_bc_opts(cfg);
	let max_enc = max_encryption(cfg);

	tokio::select! {
		// A shutdown signal wins the race — skip logout; dropping the TCP session
		// is enough for the camera to park via its own idle timeout.
		// `biased` would force this arm first, but tokio::select!'s
		// random polling already resolves a pre-cancelled token in
		// roughly the same number of poll cycles.
		_ = cancel.cancelled() => Err(InterruptedError::new().into()),
		res = async move {
			let camera = timeout(CONNECT_TIMEOUT, BcCamera::new(&opts))
				.await
				.context("connect timed out")?
				.context("connect failed")?;
			timeout(LOGIN_TIMEOUT, camera.login_with_maxenc(max_enc))
				.await
				.context("login timed out")?
				.context("login failed")?;
			// Run the op + logout in sequence regardless of op outcome.
			// Previously a `?` on the timeout-Elapsed case skipped the
			// logout: a hung op would leave the camera holding our
			// session-table slot until its own idle timer expired,
			// instead of releasing immediately on `logout()`. Battery
			// cams park on TCP-drop either way, but explicit logout is
			// faster and matches the comment's intent.
			let op_result: Result<T> = match timeout(OP_TIMEOUT, op(&camera)).await {
				Ok(inner) => inner,
				Err(_) => Err(anyhow!("operation timed out")),
			};
			let _ = timeout(LOGOUT_TIMEOUT, camera.logout()).await;
			op_result
		} => res,
	}
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
