//! Unix: SIGINT (Ctrl+C) or SIGTERM.

use std::future::Future;
use tokio::signal::unix::{signal, Signal, SignalKind};

pub(super) fn requested() -> impl Future<Output = &'static str> + Send + 'static {
	// `signal()` installs each handler synchronously, replacing the
	// default kill action from this point on.
	let int = listen(SignalKind::interrupt(), "SIGINT");
	let term = listen(SignalKind::terminate(), "SIGTERM");
	async move {
		tokio::select! {
			_ = recv(int) => "SIGINT",
			_ = recv(term) => "SIGTERM",
		}
	}
}

fn listen(kind: SignalKind, name: &str) -> Option<Signal> {
	signal(kind)
		.inspect_err(|e| tracing::warn!(signal = name, error = %e, "cannot listen for signal"))
		.ok()
}

/// Resolve on delivery; never resolve when the listener couldn't be
/// installed, so a registration failure can't pose as a shutdown.
async fn recv(signal: Option<Signal>) {
	match signal {
		Some(mut s) => {
			s.recv().await;
		}
		None => std::future::pending::<()>().await,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	/// SIGTERM delivered to this process resolves the future. The handler
	/// is installed by `requested()` before the signal is sent, so the
	/// test binary can't be killed by the default action.
	#[tokio::test]
	async fn sigterm_resolves_shutdown_request() {
		let shutdown = requested();
		let status = std::process::Command::new("kill")
			.args(["-TERM", &std::process::id().to_string()])
			.status()
			.expect("run kill");
		assert!(status.success());
		let signal = tokio::time::timeout(Duration::from_secs(2), shutdown)
			.await
			.expect("SIGTERM must resolve the shutdown future");
		assert_eq!(signal, "SIGTERM");
	}
}
