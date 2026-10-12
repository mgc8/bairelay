//! Non-Unix (Windows): Ctrl+C only.

use std::future::Future;

pub(super) fn requested() -> impl Future<Output = &'static str> + Send + 'static {
	async {
		// Never resolve when the listener can't be installed, so a
		// registration failure can't pose as a shutdown request.
		if let Err(e) = tokio::signal::ctrl_c().await {
			tracing::warn!(error = %e, "cannot listen for Ctrl+C");
			std::future::pending::<()>().await;
		}
		"Ctrl+C"
	}
}
