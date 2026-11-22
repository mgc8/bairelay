use crate::bc::model::*;
use crate::Result;
use crate::{bc::codex::BcCodex, Credentials};
use delegate::delegate;
use futures::{sink::Sink, stream::Stream};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpSocket, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Bound on how long `TcpStream::connect` may take to a single
/// destination before we give up. Real-world camera connects on the LAN
/// resolve in tens of milliseconds; the default OS connect timeout
/// (~75 s on Linux to a non-routable destination) makes a stale `dev`
/// or `relay` IP — including ones an attacker can steer us toward via
/// DNS hijack on `p2p*.reolink.com` — wedge a connection attempt for
/// over a minute. Discovery's outer `*TCP_WAIT` (4 s) caps that path,
/// but `BcCamera::new`'s direct-IP path otherwise relies entirely on
/// the caller's `tokio::time::timeout`. Belt-and-braces.
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

pub(crate) struct TcpSource {
	inner: Framed<TcpStream, BcCodex>,
}

impl TcpSource {
	pub(crate) async fn new<T: Into<String>, U: Into<String>>(
		addr: SocketAddr,
		username: T,
		password: Option<U>,
		debug: bool,
	) -> Result<TcpSource> {
		let stream = connect_to(addr).await?;

		let codex = if debug {
			BcCodex::new_with_debug(Credentials::new(username, password))
		} else {
			BcCodex::new(Credentials::new(username, password))
		};
		Ok(Self {
			inner: Framed::new(stream, codex),
		})
	}
}

impl Stream for TcpSource {
	type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

	delegate! {
		to Pin::new(&mut self.inner) {
			fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
		}
	}

	delegate! {
		to self.inner {
			fn size_hint(&self) -> (usize, Option<usize>);
		}
	}
}

impl Sink<Bc> for TcpSource {
	type Error = <BcCodex as Encoder<Bc>>::Error;

	delegate! {
		to Pin::new(&mut self.inner) {
			fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
			fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
		}
	}
}

/// Helper to create a TcpStream with a connect timeout (see
/// [`TCP_CONNECT_TIMEOUT`]). Returns `Error::Io(TimedOut)` rather than
/// blocking on the OS connect syscall.
async fn connect_to(addr: SocketAddr) -> Result<TcpStream> {
	let socket = match addr {
		SocketAddr::V4(_) => TcpSocket::new_v4()?,
		SocketAddr::V6(_) => TcpSocket::new_v6()?,
	};

	match tokio::time::timeout(TCP_CONNECT_TIMEOUT, socket.connect(addr)).await {
		Ok(Ok(stream)) => {
			// The Baichuan control channel is strictly request/response: a
			// small write (login, get_param, talk config, PTZ) followed by a
			// wait for the camera's reply. Nagle's algorithm holds a small
			// segment until the previous one is ACKed, which adds up to a
			// round trip per request on a lossy Wi-Fi link — exactly the
			// stall operators see as slow wake-ups and laggy talk setup.
			// Media flows on a separate socket, so disabling Nagle here only
			// removes latency, it does not change how bursts are shaped.
			// A failure is not actionable and never fatal: the connection
			// works, just with Nagle on.
			if let Err(e) = stream.set_nodelay(true) {
				log::debug!("TCP_NODELAY on {addr} failed: {e}; continuing with Nagle");
			}
			Ok(stream)
		}
		Ok(Err(e)) => Err(e.into()),
		Err(_) => Err(std::io::Error::new(
			std::io::ErrorKind::TimedOut,
			format!("TCP connect to {addr} timed out after {TCP_CONNECT_TIMEOUT:?}"),
		)
		.into()),
	}
}
