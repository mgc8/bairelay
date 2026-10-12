//! Process shutdown request: Ctrl+C on every platform, plus SIGTERM on
//! Unix. systemd, Docker and the HA app's s6 supervisor all stop
//! services with SIGTERM, whose default action kills the process before
//! camera logout, MQTT status / discovery cleanup, or stream teardown
//! can run.
//!
//! Platform-specific listeners live in `unix.rs` / `other.rs`.

#[cfg(not(unix))]
mod other;
#[cfg(unix)]
mod unix;

#[cfg(not(unix))]
use other as platform;
#[cfg(unix)]
use unix as platform;

use std::future::Future;

/// Register the shutdown-signal handlers now (SIGINT + SIGTERM on Unix;
/// Ctrl+C elsewhere registers on first poll) and return a future that
/// resolves with the signal's name on the first request. Registering
/// eagerly (not on first poll) closes the window where an early
/// SIGTERM would still hit the default kill action. Must be called
/// inside a Tokio runtime.
pub fn requested() -> impl Future<Output = &'static str> + Send + 'static {
	platform::requested()
}

/// Force-exit on the next shutdown signal. Call right after the first
/// one: Tokio never uninstalls its handlers, so without this a second
/// Ctrl+C / SIGTERM during a wedged teardown would be swallowed and
/// only SIGKILL could stop the process.
pub fn force_exit_on_next(exit_code: i32) {
	let next = requested();
	tokio::spawn(async move {
		let signal = next.await;
		tracing::warn!(signal, "second shutdown signal; exiting immediately");
		std::process::exit(exit_code);
	});
}
