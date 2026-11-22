//! Transport abstraction: TCP-interleaved vs UDP-unicast.
//!
//! `Transport` is the RTSP server's write-side interface for sending RTP
//! and RTCP packets to a subscribed client. Two concrete implementations:
//! - `TcpInterleavedTransport` — writes framed packets on the RTSP TCP
//!   connection (RFC 7826 §14).
//! - `UdpUnicastTransport` — sends to a client-supplied address.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::server::udp_pool::{UdpPortLease, UdpPortPool};

/// Writes RTP/RTCP packets to a subscribed RTSP client.
///
/// Implementations are `Send + Sync` so the server can hand them into
/// tokio tasks via `Arc`.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
	/// Send an RTP packet.
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()>;

	/// Send an RTCP packet.
	async fn send_rtcp(&self, packet: &[u8]) -> io::Result<()>;

	/// Shut down the transport, freeing any allocated resources. Best-effort.
	async fn close(&self);
}

/// TCP-interleaved transport. Packets are prefixed with `$ ch len` and
/// written on a shared TCP stream (the same connection that carries RTSP).
pub struct TcpInterleavedTransport {
	writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
	channel_rtp: u8,
	channel_rtcp: u8,
}

impl TcpInterleavedTransport {
	/// Create a new transport. `writer` is the shared half of the RTSP TCP
	/// connection's split write half.
	pub fn new(
		writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
		channel_rtp: u8,
		channel_rtcp: u8,
	) -> Self {
		Self {
			writer,
			channel_rtp,
			channel_rtcp,
		}
	}

	async fn write_framed(&self, channel: u8, packet: &[u8]) -> io::Result<()> {
		if packet.len() > u16::MAX as usize {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"packet too large",
			));
		}
		// Mutex held across all four awaits so the four-byte `$ ch len`
		// header + payload + flush stay atomic on the wire. Without this,
		// concurrent senders (parallel video + audio dispatch tasks) would
		// interleave bytes mid-frame and the receiver would resync at the
		// next `$` it found, mis-attributing the trailing payload bytes
		// to the wrong RTP channel. RFC 7826 §14 framing has no recovery
		// once the boundary is lost. Per-packet locking is the contract;
		// the lock is held for at most one ≤ MTU packet.
		let mut w = self.writer.lock().await;
		w.write_all(&[0x24, channel]).await?;
		w.write_all(&(packet.len() as u16).to_be_bytes()).await?;
		w.write_all(packet).await?;
		w.flush().await?;
		Ok(())
	}
}

#[async_trait::async_trait]
impl Transport for TcpInterleavedTransport {
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()> {
		self.write_framed(self.channel_rtp, packet).await
	}

	async fn send_rtcp(&self, packet: &[u8]) -> io::Result<()> {
		self.write_framed(self.channel_rtcp, packet).await
	}

	async fn close(&self) {
		// TCP connection is shared and owned by the connection task;
		// it will be closed there. Nothing to do here.
	}
}

/// UDP-unicast transport. Sends RTP to `client_rtp_addr`, RTCP to
/// `client_rtcp_addr`, from server-side sockets held in `UdpPortLease`.
///
/// The [`UdpPortLease`] is held as a private field so the port pair is
/// guaranteed to stay reserved for the transport's lifetime; dropping the
/// transport drops the lease and returns the ports to the pool.
pub struct UdpUnicastTransport {
	pub(crate) rtp_sock: Arc<UdpSocket>,
	pub(crate) rtcp_sock: Arc<UdpSocket>,
	pub(crate) client_rtp_addr: SocketAddr,
	pub(crate) client_rtcp_addr: SocketAddr,
	/// Keeps the reserved UDP port pair held for this transport's lifetime.
	/// Also the source of truth for `server_rtp_port` / `server_rtcp_port`
	/// (both accessors read directly from the lease rather than calling
	/// `local_addr()` on the socket, so they're infallible and never
	/// surface the `0` "any port" sentinel).
	lease: UdpPortLease,
}

#[async_trait::async_trait]
impl Transport for UdpUnicastTransport {
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()> {
		self.rtp_sock.send_to(packet, self.client_rtp_addr).await?;
		Ok(())
	}

	async fn send_rtcp(&self, packet: &[u8]) -> io::Result<()> {
		self.rtcp_sock
			.send_to(packet, self.client_rtcp_addr)
			.await?;
		Ok(())
	}

	async fn close(&self) {
		// Sockets are dropped with the transport; no explicit close needed.
	}
}

impl UdpUnicastTransport {
	/// Bind UDP sockets on the server's pooled ports and send RTP/RTCP to
	/// the client addresses.
	///
	/// Returns the transport which holds the port lease until dropped; when
	/// the transport is dropped the lease is released and the port pair is
	/// returned to `pool`.
	pub async fn bind(
		server_bind_ip: IpAddr,
		pool: Arc<UdpPortPool>,
		client_rtp_addr: SocketAddr,
		client_rtcp_addr: SocketAddr,
	) -> io::Result<Self> {
		// A leased port can still be occupied at the OS level — a
		// TIME_WAIT socket from a prior session, or an unrelated process
		// on the same port. Rather than fail the SETUP on the first such
		// pair, step to the next one. Failed leases are parked (not
		// dropped) so `acquire` skips them until a pair binds; the parked
		// leases release when `_parked` drops at end of scope.
		let mut _parked = Vec::new();
		loop {
			let lease = pool.acquire().map_err(io::Error::other)?;

			match Self::try_bind_pair(server_bind_ip, &lease).await {
				Ok((rtp_sock, rtcp_sock)) => {
					return Ok(Self {
						rtp_sock,
						rtcp_sock,
						client_rtp_addr,
						client_rtcp_addr,
						lease,
					});
				}
				Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
					// Pair unusable — park the lease so the next acquire
					// hands back a different port, and retry.
					_parked.push(lease);
				}
				// A non-conflict error (bad bind IP, host out of sockets)
				// won't be fixed by trying another port.
				Err(e) => return Err(e),
			}
		}
	}

	/// Bind the RTP + RTCP sockets for a single leased pair. Binding the
	/// RTCP socket only after the RTP socket succeeds means an
	/// `AddrInUse` on either surfaces as `AddrInUse` to the caller.
	async fn try_bind_pair(
		server_bind_ip: IpAddr,
		lease: &UdpPortLease,
	) -> io::Result<(Arc<UdpSocket>, Arc<UdpSocket>)> {
		let rtp_sock =
			Arc::new(UdpSocket::bind(SocketAddr::new(server_bind_ip, lease.rtp_port)).await?);
		let rtcp_sock =
			Arc::new(UdpSocket::bind(SocketAddr::new(server_bind_ip, lease.rtcp_port)).await?);
		Ok((rtp_sock, rtcp_sock))
	}

	/// Server-assigned RTP port (even). Read directly from the held lease,
	/// so this is infallible and never returns the `0` "any port" sentinel.
	pub fn server_rtp_port(&self) -> u16 {
		self.lease.rtp_port
	}

	/// Server-assigned RTCP port (odd). Read directly from the held lease,
	/// so this is infallible and never returns the `0` "any port" sentinel.
	pub fn server_rtcp_port(&self) -> u16 {
		self.lease.rtcp_port
	}
}

/// No-op [`Transport`] for unit tests that need a concrete `Arc<dyn
/// Transport>` but don't care about the wire side. `send_rtp` / `send_rtcp`
/// return `Ok(())` without touching any socket; `close` does nothing.
#[cfg(test)]
pub(crate) fn noop_transport_for_tests() -> std::sync::Arc<dyn Transport> {
	struct Noop;
	#[async_trait::async_trait]
	impl Transport for Noop {
		async fn send_rtp(&self, _packet: &[u8]) -> io::Result<()> {
			Ok(())
		}
		async fn send_rtcp(&self, _packet: &[u8]) -> io::Result<()> {
			Ok(())
		}
		async fn close(&self) {}
	}
	std::sync::Arc::new(Noop)
}

/// No-op [`Transport`] that counts `send_rtp` calls. For tests that must
/// prove a burst replay really reached the wire (and was not merely a call
/// that returned `Ok`). Returns the transport and the shared counter.
#[cfg(test)]
pub(crate) fn counting_transport_for_tests() -> (
	std::sync::Arc<dyn Transport>,
	std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
	use std::sync::atomic::{AtomicUsize, Ordering};

	struct Counting(std::sync::Arc<AtomicUsize>);

	#[async_trait::async_trait]
	impl Transport for Counting {
		async fn send_rtp(&self, _packet: &[u8]) -> io::Result<()> {
			self.0.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}
		async fn send_rtcp(&self, _packet: &[u8]) -> io::Result<()> {
			Ok(())
		}
		async fn close(&self) {}
	}

	let sent = std::sync::Arc::new(AtomicUsize::new(0));
	let transport = std::sync::Arc::new(Counting(std::sync::Arc::clone(&sent)));
	(transport, sent)
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::io::AsyncReadExt;

	#[tokio::test]
	async fn tcp_interleaved_frames_packet_correctly() {
		let (client, server) = tokio::io::duplex(4096);
		let (mut client_read, _client_write) = tokio::io::split(client);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));

		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 0, 1);
		let payload = b"\x80\x60\x00\x01rtp-payload";
		transport.send_rtp(payload).await.unwrap();

		// Read framed packet: $ (1 byte) + channel (1 byte) + len (2 bytes) + payload.
		let mut header = [0u8; 4];
		client_read.read_exact(&mut header).await.unwrap();
		assert_eq!(header[0], 0x24);
		assert_eq!(header[1], 0); // channel_rtp
		let len = u16::from_be_bytes([header[2], header[3]]) as usize;
		assert_eq!(len, payload.len());
		let mut body = vec![0u8; len];
		client_read.read_exact(&mut body).await.unwrap();
		assert_eq!(&body, payload);
	}

	#[tokio::test]
	async fn tcp_interleaved_uses_rtcp_channel_for_rtcp() {
		let (client, server) = tokio::io::duplex(4096);
		let (mut client_read, _client_write) = tokio::io::split(client);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));

		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 2, 3);
		transport.send_rtcp(b"rtcp-packet").await.unwrap();

		let mut header = [0u8; 4];
		client_read.read_exact(&mut header).await.unwrap();
		assert_eq!(header[1], 3); // channel_rtcp
	}

	#[tokio::test]
	async fn tcp_interleaved_rejects_oversize_packet() {
		let (_client, server) = tokio::io::duplex(4096);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));
		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 0, 1);
		// One byte over u16::MAX - framing header has 2-byte length field.
		let too_big = vec![0u8; u16::MAX as usize + 1];
		let err = transport.send_rtp(&too_big).await.unwrap_err();
		assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
	}

	#[tokio::test]
	async fn noop_transport_returns_ok() {
		let t = noop_transport_for_tests();
		t.send_rtp(b"ignored").await.unwrap();
		t.send_rtcp(b"ignored").await.unwrap();
		t.close().await;
	}

	#[tokio::test]
	async fn udp_transport_sends_to_client_addr() {
		use std::net::Ipv4Addr;
		// Bind a loopback receiver for both RTP and RTCP.
		let client_rtp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let client_rtcp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let client_rtp_addr = client_rtp_sock.local_addr().unwrap();
		let client_rtcp_addr = client_rtcp_sock.local_addr().unwrap();

		let pool = Arc::new(UdpPortPool::new());
		let transport = UdpUnicastTransport::bind(
			IpAddr::V4(Ipv4Addr::LOCALHOST),
			Arc::clone(&pool),
			client_rtp_addr,
			client_rtcp_addr,
		)
		.await
		.unwrap();

		transport.send_rtp(b"rtp-data").await.unwrap();
		transport.send_rtcp(b"rtcp-data").await.unwrap();

		let mut buf = [0u8; 64];
		let (n, _) = client_rtp_sock.recv_from(&mut buf).await.unwrap();
		assert_eq!(&buf[..n], b"rtp-data");
		let (n, _) = client_rtcp_sock.recv_from(&mut buf).await.unwrap();
		assert_eq!(&buf[..n], b"rtcp-data");
	}

	/// `bind` must step past a leased pair whose ports are already
	/// occupied at the OS level, rather than fail the SETUP. Occupy the
	/// pool's first RTP port, then bind and assert the transport landed
	/// on a later pair.
	#[tokio::test]
	async fn udp_bind_skips_occupied_pair() {
		use crate::server::udp_pool::POOL_START;
		use std::net::Ipv4Addr;

		let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
		// Squat on the even (RTP) port of the pool's first pair.
		let squatter = UdpSocket::bind(SocketAddr::new(ip, POOL_START)).await;
		// If something else already holds POOL_START this test can't set
		// up its precondition; skip rather than false-fail.
		let Ok(_squatter) = squatter else {
			eprintln!("POOL_START busy; skipping");
			return;
		};

		let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let caddr = client.local_addr().unwrap();

		let pool = Arc::new(UdpPortPool::new());
		let transport = UdpUnicastTransport::bind(ip, Arc::clone(&pool), caddr, caddr)
			.await
			.expect("bind must retry past the occupied first pair");

		assert_ne!(
			transport.server_rtp_port(),
			POOL_START,
			"transport must not claim the occupied port"
		);
		assert!(transport.server_rtp_port() > POOL_START);
	}
}
