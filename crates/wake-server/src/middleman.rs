//! Middleman UDP loop on port 9999 (default). Handles `C2M_Q` (clients)
//! and `D2M_Q` (cameras) queries and replies pointing the peer at our
//! register listener. No state — the loop owns nothing beyond its socket.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use bairelay_neolink_core::bcudp::xml::{EmptyTag, IpPort, M2cQr, M2dQr, UdpXml};
use bairelay_neolink_core::redact::UidTail;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::packet::{decode_discovery, encode_discovery};
use crate::registry::SessionAnchors;
use crate::route::advertise_ip;
use crate::WakeServerError;

pub(crate) async fn run(
	sock: Arc<UdpSocket>,
	register_addr: SocketAddr,
	bind: IpAddr,
	anchors: Arc<SessionAnchors>,
	cancel: CancellationToken,
) -> Result<(), WakeServerError> {
	let local = sock.local_addr().ok();
	tracing::info!(?local, register = %register_addr, %bind, "wake-server middleman listening");
	let mut buf = vec![0u8; 4096];
	loop {
		tokio::select! {
			_ = cancel.cancelled() => {
				tracing::info!("wake-server middleman cancelled");
				return Ok(());
			}
			res = sock.recv_from(&mut buf) => {
				let (n, src) = match res {
					Ok(p) => p,
					Err(e) => { warn!(error = %e, "middleman recv_from"); continue; }
				};
				handle(&sock, register_addr, bind, &anchors, src, &buf[..n]).await;
			}
		}
	}
}

async fn handle(
	sock: &UdpSocket,
	register_addr: SocketAddr,
	bind: IpAddr,
	anchors: &SessionAnchors,
	src: SocketAddr,
	raw: &[u8],
) {
	let (tid, payload) = match decode_discovery(raw) {
		Ok(v) => v,
		Err(e) => {
			debug!(%src, error = %e, "middleman: bad packet");
			return;
		}
	};
	// Resolve which local IP we should advertise to this peer. When
	// `bind` is `0.0.0.0` we ask the kernel which interface IP it would
	// use to reach `src`; advertising the wildcard makes Argus firmware
	// silently reject the reply.
	let advertise = advertise_ip(bind, src).to_string();
	let reg_port = register_addr.port();
	match payload {
		UdpXml::C2mQ(q) => {
			debug!(%src, uid = %q.uid, advertise = %advertise, "C2M_Q");
			let reply = UdpXml::M2cQr(M2cQr {
				reg: Some(IpPort {
					ip: advertise.clone(),
					port: reg_port,
				}),
				relay: Some(IpPort {
					ip: advertise.clone(),
					port: reg_port,
				}),
				log: Some(IpPort {
					ip: advertise.clone(),
					port: reg_port,
				}),
				t: Some(IpPort {
					ip: advertise,
					port: reg_port,
				}),
			});
			match encode_discovery(tid, reply) {
				Ok(bytes) => {
					if let Err(e) = sock.send_to(&bytes, src).await {
						warn!(%src, error = %e, "middleman: send M2C_Q_R");
					} else {
						debug!(%src, "middleman -> M2C_Q_R");
					}
				}
				Err(e) => warn!(%src, error = %e, "middleman: encode M2C_Q_R"),
			}
		}
		UdpXml::D2mQ(q) => {
			debug!(%src, uid = %q.uid, revision = ?q.revision, advertise = %advertise, "D2M_Q");
			// Verified shape from a real Reolink cloud capture (			// live-verify): reg + log addresses, empty `<timer/>` and
			// `<retry/>` markers, rsp/token/ac. Token + ac are
			// session-bound; the cloud generates fresh values per query,
			// so we do too. Stash the pair under the camera UID so the
			// register loop can echo `ac` back in the matching
			// `R2D_R_R` (cameras anchor to that value).
			let token = rand::random::<u64>();
			let ac = rand::random::<u32>();
			if !anchors.issue(&q.uid, token, ac, std::time::Instant::now()) {
				// Full UID is in the D2M_Q debug line above.
				warn!(
					%src, uid = %UidTail(&q.uid),
					cap = crate::registry::MAX_MAP_ENTRIES,
					"session-anchor map at capacity; rejecting D2M_Q"
				);
				return;
			}
			let reply = UdpXml::M2dQr(M2dQr {
				reg: IpPort {
					ip: advertise.clone(),
					port: reg_port,
				},
				log: IpPort {
					ip: advertise,
					port: reg_port,
				},
				timer: EmptyTag::default(),
				retry: EmptyTag::default(),
				rsp: 0,
				token,
				ac,
			});
			match encode_discovery(tid, reply) {
				Ok(bytes) => {
					if let Err(e) = sock.send_to(&bytes, src).await {
						warn!(%src, error = %e, "middleman: send M2D_Q_R");
					} else {
						debug!(%src, "middleman -> M2D_Q_R");
					}
				}
				Err(e) => warn!(%src, error = %e, "middleman: encode M2D_Q_R"),
			}
		}
		other => debug!(%src, ?other, "middleman: unexpected payload"),
	}
}
