//! This module handles connections and subscribers
//!
//! This includes a tcp and udp connections. As well
//! as subscribers to binary streams that are encoded
//! in the bc packets.
//!
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

mod bcconn;
mod bcsub;
#[cfg(test)]
mod chain;
/// UID lookup over UDP. Public for two helpers only — `broadcast_destinations`
/// and `broadcast_socket`, re-exported from `bc_protocol` — which the binary's
/// startup scan needs in order to ask the LAN a question it cannot ask through
/// `Discoverer` (that type is created for one uid and drives a handshake to
/// completion). Everything else in the module stays `pub(crate)` exactly as it
/// was; making the module public publishes nothing but those two functions.
pub mod discovery;
#[cfg(any(test, feature = "test-util"))]
pub mod mock;
mod tcpsource;
#[cfg(not(feature = "fuzz-api"))]
mod udpsource;
/// UDP send/recv flow primitives. Public only under the `fuzz-api`
/// feature so the out-of-tree fuzz harness can reach `UdpFlowState`
/// and `REORDER_CAP`; the production build keeps the module private.
#[cfg(feature = "fuzz-api")]
pub mod udpsource;

pub(crate) use self::{
	bcconn::BcConnection, bcconn::*, bcsub::BcSubscription, discovery::CameraDiscoverer,
	discovery::Discovery, tcpsource::TcpSource, udpsource::UdpSource,
};

#[derive(Debug)]
pub(crate) struct DiscoveryResult {
	socket: Arc<UdpSocket>,
	addr: SocketAddr,
	client_id: i32,
	camera_id: i32,
	/// sigV3 login nonce from the `D2C_C_R` handshake (account cameras only).
	nc: Option<i64>,
	/// sigV3 ECDHE offer (`pl` line) from the handshake (account cameras only).
	pl: Option<String>,
}

impl DiscoveryResult {
	/// Get the address discovered
	pub(crate) fn get_addr(&self) -> &SocketAddr {
		&self.addr
	}

	/// Take the sigV3 handshake `(nonce, pl)` if this was an account-camera
	/// connect. Consumed once by `BcCamera::new` and handed to the login layer.
	pub(crate) fn take_sigv3_handshake(&mut self) -> Option<(i64, String)> {
		match (self.nc.take(), self.pl.take()) {
			(Some(nc), Some(pl)) => Some((nc, pl)),
			_ => None,
		}
	}
}
