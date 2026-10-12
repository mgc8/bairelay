//! Register UDP loop on port 58200 (default). Handles D2R_HB, C2R_C,
//! D2R_DISC, D2R_C_R, C2R_CFM. The wake burst is fire-and-forget.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bairelay_neolink_core::bcudp::xml::{
	D2rHb, D2rR, HbTimer, IpPort, R2cCr, R2cT, R2dC, R2dDcr, R2dHbr, R2dRr, UdpXml,
};
use bairelay_neolink_core::redact::UidTail;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::RuntimeConfig;
use crate::packet::{decode_discovery, encode_discovery, random_sid};
use crate::registry::{CameraRegistry, SessionAnchors};
use crate::WakeServerError;

const WAKE_BURST_COUNT: usize = 10;
const WAKE_BURST_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) async fn run(
	sock: Arc<UdpSocket>,
	registry: Arc<CameraRegistry>,
	anchors: Arc<SessionAnchors>,
	cfg: RuntimeConfig,
	cancel: CancellationToken,
) -> Result<(), WakeServerError> {
	let local = sock.local_addr().ok();
	info!(?local, "wake-server register listening");
	let stale = Duration::from_millis(cfg.stale_after_ms);
	let mut buf = vec![0u8; 4096];
	loop {
		tokio::select! {
			_ = cancel.cancelled() => {
				info!("wake-server register cancelled");
				return Ok(());
			}
			res = sock.recv_from(&mut buf) => {
				let (n, src) = match res {
					Ok(p) => p,
					Err(e) => { warn!(error = %e, "register recv_from"); continue; }
				};
				handle(&sock, &registry, &anchors, &cfg, stale, src, &buf[..n], &cancel).await;
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
async fn handle(
	sock: &Arc<UdpSocket>,
	registry: &Arc<CameraRegistry>,
	anchors: &Arc<SessionAnchors>,
	cfg: &RuntimeConfig,
	stale_after: Duration,
	src: SocketAddr,
	raw: &[u8],
	cancel: &CancellationToken,
) {
	let (tid, payload) = match decode_discovery(raw) {
		Ok(v) => v,
		Err(e) => {
			debug!(%src, error = %e, "register: bad packet");
			return;
		}
	};
	match payload {
		UdpXml::D2rHb(hb) => {
			handle_heartbeat(sock, registry, anchors, cfg, stale_after, src, tid, hb).await
		}
		UdpXml::C2rC(c) => {
			handle_connect(sock, registry, cfg, stale_after, src, tid, c, cancel).await
		}
		UdpXml::D2rR(r) => handle_register(sock, anchors, src, tid, r).await,
		UdpXml::D2rDisc(disc) => {
			debug!(%src, sid = disc.sid, "D2R_DISC");
			let reply = UdpXml::R2dDcr(R2dDcr {
				sid: disc.sid,
				rsp: 0,
			});
			send_reply(sock, src, tid, reply, "register: encode R2D_DC_R").await;
		}
		UdpXml::D2rCr(cr) => {
			debug!(%src, sid = cr.sid, rsp = cr.rsp, "D2R_C_R (camera awake)");
		}
		UdpXml::C2rCfm(cfm) => {
			debug!(%src, sid = cfm.sid, conn = %cfm.conn, "C2R_CFM");
		}
		other => debug!(%src, ?other, "register: unhandled payload"),
	}
}

/// Handle the `D2R_R` registration request the camera sends straight after
/// receiving our `M2D_Q_R`. Echo back the `<ac>` we issued during the
/// preceding `M2D_Q_R`; cameras anchor to that value before they will
/// proceed to `D2R_HB`. Cloud observed to send `<rsp>-4</rsp>` here
/// (informational, not fatal) — we mirror that.
async fn handle_register(
	sock: &UdpSocket,
	anchors: &Arc<SessionAnchors>,
	src: SocketAddr,
	tid: u32,
	r: D2rR,
) {
	let now = std::time::Instant::now();
	// Anchors live for as long as a session does; reuse the heartbeat
	// stale window as a generous TTL.
	let lookup_ttl = Duration::from_secs(60 * 60);
	let ac = match anchors.lookup(&r.uid, now, lookup_ttl) {
		Some(a) if a.token == r.token => a.ac,
		Some(a) => {
			// Token mismatch: someone is replaying or guessing. Drop
			// silently rather than reply with the stored `ac` — the
			// reply would otherwise confirm the (correct) ac to the
			// peer, leaking session state.
			debug!(
				%src, uid = %r.uid,
				expected_token = a.token, got_token = r.token,
				"D2R_R token mismatch; dropping packet"
			);
			return;
		}
		None => {
			// No `M2D_Q_R` anchor for this UID — the camera (or, more
			// likely, a hostile peer) is sending `D2R_R` without
			// completing the documented handshake. Drop silently;
			// don't synthesise an ac that would let an on-LAN
			// attacker drive a real camera's state machine.
			debug!(%src, uid = %r.uid, "D2R_R for unseen UID; dropping packet");
			return;
		}
	};
	debug!(%src, uid = %r.uid, ac, "D2R_R");
	let reply = UdpXml::R2dRr(R2dRr { rsp: -4, ac });
	send_reply(sock, src, tid, reply, "register: encode R2D_R_R").await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_connect(
	sock: &Arc<UdpSocket>,
	registry: &CameraRegistry,
	cfg: &RuntimeConfig,
	stale_after: Duration,
	src: SocketAddr,
	tid: u32,
	c: bairelay_neolink_core::bcudp::xml::C2rC,
	cancel: &CancellationToken,
) {
	debug!(%src, uid = %c.uid, cid = c.cid, "C2R_C");
	// Tokio's `Instant::now()` honours `tokio::time::pause` / `advance`,
	// which lets unit tests drive stale-eviction without sleeping.
	let now = tokio::time::Instant::now().into_std();
	let cam = registry.lookup_fresh(&c.uid, now, stale_after);
	match cam {
		None => {
			debug!(%src, uid = %c.uid, "C2R_C for unknown or stale UID");
			let reply = UdpXml::R2cCr(R2cCr {
				dev: None,
				dmap: None,
				relay: None,
				relayt: None,
				nat: "NULL".into(),
				sid: None,
				rsp: -1,
				ac: 0,
			});
			send_reply(sock, src, tid, reply, "register: encode R2C_C_R(unknown)").await;
		}
		Some(entry) => {
			let sid = random_sid();
			// Spawn the wake burst before replying so the client has the
			// strongest chance of seeing the camera awake by the time it
			// tries TCP 9000.
			let burst_sock = Arc::clone(sock);
			let cam_addr = entry.addr;
			// `c.cli` is the client's self-reported "talk back to me at
			// this address" hint, but we replace its IP with the actual
			// UDP source IP — otherwise a malicious or buggy client can
			// direct the camera at an arbitrary host (small UDP-
			// reflection vector via R2D_C). The client's port is kept
			// intact: a legitimate client may listen on a different
			// port for the camera's response than for the wake request.
			let cli_src_ip = src.ip().to_string();
			if c.cli.ip != cli_src_ip {
				warn!(
					%src, claimed_cli_ip = %c.cli.ip, actual_src_ip = %cli_src_ip,
					"C2R_C cli.ip mismatch with UDP source; substituting actual source IP",
				);
			}
			let cli = IpPort {
				ip: cli_src_ip.clone(),
				port: c.cli.port,
			};
			let cmap = IpPort {
				ip: cli_src_ip,
				port: src.port(),
			};
			// Resolve the local IP we should advertise as our relay
			// address. When `bind = 0.0.0.0` we derive it from each peer
			// per-direction: cameras need our LAN IP, clients need
			// whatever local IP the OS would use to reach them. A bare
			// `cfg.bind.to_string()` would write `0.0.0.0` into the
			// reply, which Argus firmware silently rejects.
			let relay_ip_for_camera = crate::route::advertise_ip(cfg.bind, cam_addr).to_string();
			let relay_ip_for_client = crate::route::advertise_ip(cfg.bind, src).to_string();
			let relay = IpPort {
				ip: relay_ip_for_camera,
				port: cfg.register_port,
			};
			let cid = c.cid;
			let cancel = cancel.clone();
			// Outer supervisor task: spawns the burst, awaits its
			// JoinHandle, and surfaces any panic at warn level. Without
			// this, a panic inside the burst (e.g. encode_discovery
			// regression, future contributor's `unwrap`) is silently
			// swallowed by tokio — operators see "wake commands
			// intermittently fail" with no log signal.
			tokio::spawn(async move {
				let burst = tokio::spawn(async move {
					for i in 0..WAKE_BURST_COUNT {
						let wake = UdpXml::R2dC(R2dC {
							cli: cli.clone(),
							cmap: cmap.clone(),
							relay: relay.clone(),
							sid,
							cid,
						});
						match encode_discovery(tid, wake) {
							Ok(bytes) => {
								if let Err(e) = burst_sock.send_to(&bytes, cam_addr).await {
									warn!(%cam_addr, i, error = %e, "wake burst send failed; aborting");
									break;
								}
								debug!(%cam_addr, i, "R2D_C ->");
							}
							Err(e) => {
								warn!(error = %e, "wake burst encode failed");
								break;
							}
						}
						if i + 1 < WAKE_BURST_COUNT {
							tokio::select! {
								_ = sleep(WAKE_BURST_INTERVAL) => {}
								_ = cancel.cancelled() => {
									debug!(%cam_addr, "wake burst cancelled mid-flight");
									break;
								}
							}
						}
					}
				});
				if let Err(e) = burst.await {
					if e.is_panic() {
						warn!(
							%cam_addr, sid, cid,
							"wake burst task panicked; camera will not receive R2D_C — operator should check logs",
						);
					}
				}
			});

			// R2C_C_R reply to the client.
			let cam_ip = entry.addr.ip().to_string();
			let cam_port = entry.addr.port();
			let reply = UdpXml::R2cCr(R2cCr {
				dev: Some(IpPort {
					ip: cam_ip.clone(),
					port: cam_port,
				}),
				dmap: Some(IpPort {
					ip: cam_ip.clone(),
					port: cam_port,
				}),
				relay: Some(IpPort {
					ip: relay_ip_for_client,
					port: cfg.register_port,
				}),
				relayt: None,
				nat: "NULL".into(),
				sid: Some(sid),
				rsp: 0,
				ac: 0,
			});
			send_reply(sock, src, tid, reply, "register: encode R2C_C_R").await;

			// R2C_T reply to the client (matches reference).
			let r2t = UdpXml::R2cT(R2cT {
				dev: Some(IpPort {
					ip: cam_ip.clone(),
					port: cam_port,
				}),
				dmap: Some(IpPort {
					ip: cam_ip,
					port: cam_port,
				}),
				sid,
				cid: c.cid,
			});
			send_reply(sock, src, tid, r2t, "register: encode R2C_T").await;
		}
	}
}

async fn send_reply(
	sock: &UdpSocket,
	dst: SocketAddr,
	tid: u32,
	payload: UdpXml,
	err_ctx: &'static str,
) {
	match encode_discovery(tid, payload) {
		Ok(bytes) => {
			if let Err(e) = sock.send_to(&bytes, dst).await {
				warn!(%dst, error = %e, "register: send_to");
			} else {
				debug!(%dst, "register -> reply");
			}
		}
		Err(e) => warn!(error = %e, "{err_ctx}"),
	}
}

#[allow(clippy::too_many_arguments)]
async fn handle_heartbeat(
	sock: &UdpSocket,
	registry: &CameraRegistry,
	anchors: &Arc<SessionAnchors>,
	cfg: &RuntimeConfig,
	stale_after: Duration,
	src: SocketAddr,
	tid: u32,
	hb: D2rHb,
) {
	debug!(%src, uid = %hb.uid, token = hb.token, "D2R_HB");
	// CRITICAL: use the UDP source addr, not hb.dev (which is the camera's
	// own LAN IP — useless for sending a reply back to a NAT'd camera).
	// Tokio's `Instant::now()` honours `tokio::time::pause` / `advance`,
	// which lets unit tests drive stale-eviction without sleeping.
	let now = tokio::time::Instant::now().into_std();
	// Info / warn lines show only the UID tail (they end up in bug
	// reports); the full UID is logged at debug.
	match registry.upsert(&hb.uid, src, hb.token, now) {
		Some(true) => {
			info!(%src, uid = %UidTail(&hb.uid), "wake-server registered camera");
			debug!(%src, uid = %hb.uid, "wake-server registered camera (full UID)");
		}
		Some(false) => {} // refresh, no log
		None => {
			debug!(%src, uid = %hb.uid, "registry full; rejected UID");
			warn!(
				%src, uid = %UidTail(&hb.uid),
				cap = crate::registry::MAX_MAP_ENTRIES,
				"camera registry at capacity; rejecting D2R_HB"
			);
			return;
		}
	}
	// Heartbeats are the most reliable sweep trigger: every camera the
	// wake server cares about emits one every ~20 s, so a stale peer is
	// detected within one heartbeat cycle of its TTL. No background task.
	for evicted in registry.purge_stale(now, stale_after) {
		info!(uid = %UidTail(&evicted), "wake-server deregistered stale camera");
		debug!(uid = %evicted, "wake-server deregistered stale camera (full UID)");
	}
	// Same hot-path purge for session anchors so a flood of D2M_Q with
	// random UIDs cannot grow the anchor map past stale_after.
	for evicted in anchors.purge_stale(now, stale_after) {
		debug!(uid = %evicted, "wake-server purged stale session anchor");
	}
	if hb.needrsp == Some(1) {
		let now_secs = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|d| d.as_secs())
			.unwrap_or(0);
		let reply = UdpXml::R2dHbr(R2dHbr {
			rsp: 0,
			time_t: now_secs,
			timer: HbTimer {
				hb: cfg.heartbeat_ms,
			},
		});
		match encode_discovery(tid, reply) {
			Ok(bytes) => {
				if let Err(e) = sock.send_to(&bytes, src).await {
					warn!(%src, error = %e, "register: send R2D_HB_R");
				} else {
					debug!(%src, "register -> R2D_HB_R");
				}
			}
			Err(e) => warn!(%src, error = %e, "register: encode R2D_HB_R"),
		}
	}
}

// `IpPort`, `R2cCr`, `R2cT`, `R2dC`, `R2dDcr`, `random_sid`, `WAKE_BURST_*`
// are imported above so subsequent tasks slot into this module without
// re-touching the import list.
