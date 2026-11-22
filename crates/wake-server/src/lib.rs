//! Local replacement for Reolink's P2P cloud servers — wakes battery cameras over the LAN.
//!
//! Wire-level reverse-engineering and operator-facing details live in
//! `docs/cloud-interception.md` § Part I.

pub mod config;
pub mod packet;
pub mod registry;
pub mod route;

mod middleman;
mod register;

use std::io;
use std::net::SocketAddr;

pub use config::WakeServerConfig;
pub use register::set_observer;

/// Public error type at the library boundary.
#[derive(Debug, thiserror::Error)]
pub enum WakeServerError {
	/// Surfaces port conflicts at startup so the operator sees which port is taken.
	#[error("wake server failed to bind to {addr}: {source}")]
	Bind { addr: SocketAddr, source: io::Error },

	/// Wraps unrecoverable post-bind socket I/O so callers can distinguish it from framing errors.
	#[error("wake server socket error: {0}")]
	Socket(#[from] io::Error),

	/// Signals corrupt or truncated BcUdp framing so we can drop the packet instead of crashing.
	#[error("BcUdp framing error: {0}")]
	Frame(#[from] bairelay_neolink_core::Error),

	/// Signals XML payloads that don't match the BcUdp schema so malformed peers never panic the loop.
	#[error("BcUdp XML error: {0}")]
	Xml(#[from] quick_xml::de::DeError),

	/// Carries config-validation and join-error messages without leaking internal types.
	#[error("invalid wake-server config: {0}")]
	Config(String),

	/// Flags BcUdp packet kinds we don't expect on a given listener so we can log and drop.
	#[error("wake server received unexpected BcUdp packet kind: {kind}")]
	UnexpectedPacketKind { kind: &'static str },
}

use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

/// Build a fresh `Arc<CameraRegistry>` for the binary to share between
/// the wake server and downstream consumers.
/// Tests can call this too — keeps registry construction in one place.
pub fn make_registry() -> Arc<registry::CameraRegistry> {
	Arc::new(registry::CameraRegistry::new())
}

/// Internal entrypoint used by both production `run` and integration tests.
/// Tests bind sockets to ephemeral ports first; production binds based on
/// `RuntimeConfig`. Returns `Ok(())` on graceful cancellation.
///
/// `registry` is supplied by the caller so other tasks (e.g. the
/// push-listener that reads source IPs out of the same map) can share state.
pub async fn run_with_sockets(
	cfg: config::RuntimeConfig,
	registry: Arc<registry::CameraRegistry>,
	middleman_sock: UdpSocket,
	register_sock: UdpSocket,
	cancel: CancellationToken,
) -> Result<(), WakeServerError> {
	let middleman_sock = Arc::new(middleman_sock);
	let register_sock = Arc::new(register_sock);

	// Per-UID `(token, ac)` map shared between the middleman (which
	// issues anchors during `M2D_Q_R`) and the register loop (which
	// echoes the `ac` back in `R2D_R_R`).
	let anchors = Arc::new(registry::SessionAnchors::new());

	let register_local = register_sock
		.local_addr()
		.map_err(WakeServerError::Socket)?;

	// Spawn middleman + register loops; race against cancellation. The
	// first listener that exits (success or error) takes the result; the
	// other is cancelled by sharing the same token.
	let mid = {
		let sock = Arc::clone(&middleman_sock);
		let cancel = cancel.clone();
		let bind = cfg.bind;
		let anchors = Arc::clone(&anchors);
		tokio::spawn(
			async move { middleman::run(sock, register_local, bind, anchors, cancel).await },
		)
	};
	let reg = {
		let sock = Arc::clone(&register_sock);
		let cancel = cancel.clone();
		let registry = Arc::clone(&registry);
		let anchors = Arc::clone(&anchors);
		let cfg = cfg.clone();
		tokio::spawn(async move { register::run(sock, registry, anchors, cfg, cancel).await })
	};

	let res: Result<(), WakeServerError> = tokio::select! {
		r = mid => match r {
			Ok(inner) => inner,
			Err(join_err) => Err(WakeServerError::Config(format!("middleman join: {join_err}"))),
		},
		r = reg => match r {
			Ok(inner) => inner,
			Err(join_err) => Err(WakeServerError::Config(format!("register join: {join_err}"))),
		},
	};
	cancel.cancel();
	res
}

/// Production entrypoint: binds the configured sockets and runs the
/// listener pair until the cancellation token fires. The caller supplies
/// the `Arc<CameraRegistry>` so it can be shared with the push-listener
/// and any future readers.
pub async fn run(
	cfg: config::RuntimeConfig,
	registry: Arc<registry::CameraRegistry>,
	cancel: CancellationToken,
) -> Result<(), WakeServerError> {
	let middleman_addr = std::net::SocketAddr::new(cfg.bind, cfg.middleman_port);
	let register_addr = std::net::SocketAddr::new(cfg.bind, cfg.register_port);
	let middleman =
		UdpSocket::bind(middleman_addr)
			.await
			.map_err(|source| WakeServerError::Bind {
				addr: middleman_addr,
				source,
			})?;
	let register =
		UdpSocket::bind(register_addr)
			.await
			.map_err(|source| WakeServerError::Bind {
				addr: register_addr,
				source,
			})?;
	run_with_sockets(cfg, registry, middleman, register, cancel).await
}

#[cfg(test)]
mod error_tests {
	use super::*;
	use std::net::Ipv4Addr;

	#[test]
	fn bind_error_displays_addr_and_source() {
		let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9999);
		let source = io::Error::new(io::ErrorKind::AddrInUse, "in use");
		let err = WakeServerError::Bind { addr, source };
		let s = format!("{err}");
		assert!(s.contains("9999"));
		assert!(s.contains("in use"));
	}
}
