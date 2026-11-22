//! Register UDP loop on port 58200 (default). Handles D2R_HB, C2R_C,
//! D2R_DISC, D2R_C_R, C2R_CFM. The wake burst is fire-and-forget.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bairelay_neolink_core::bcudp::xml::{
	D2rHb, D2rR, HbTimer, IpPort, R2cCr, R2cT, R2dC, R2dDcr, R2dHbr, R2dRr, UdpXml,
};
use bairelay_neolink_core::bcudp::announcement::{decode_announcement, Announcement};
use bairelay_neolink_core::redact::UidTail;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{refusal_explanation, RuntimeConfig};
use crate::packet::{decode_discovery, encode_discovery, random_sid};
use crate::registry::{CameraRegistry, SessionAnchors};
use crate::WakeServerError;

const WAKE_BURST_COUNT: usize = 10;
const WAKE_BURST_INTERVAL: Duration = Duration::from_millis(100);

/// Observer of the event announcements (`0x31`) a camera pushes by itself.
///
/// The register server has no idea what to do with one: it knows neither the
/// configured cameras nor any bus. It must not throw them away without showing
/// them either, because while a camera is asleep an announcement is the only
/// thing it says. The binary installs this observer once, next to the startup
/// wiring.
///
/// It is called **from the listen loop**, so it has to be non-blocking and must
/// not wait on the network: handing the announcement to an mpsc sender is the
/// right gesture, and the consumer belongs to the binary.
static OBSERVER: std::sync::OnceLock<
	std::sync::Arc<dyn Fn(Announcement) + Send + Sync + 'static>,
> = std::sync::OnceLock::new();

/// Installs the announcement observer. Call once before [`run`], like the other
/// startup wiring. The first call wins, so a second caller cannot swap the
/// observer out from under a running loop.
pub fn set_observer(observer: impl Fn(Announcement) + Send + Sync + 'static) {
	let _ = OBSERVER.set(std::sync::Arc::new(observer));
}

/// Retransmission merge window. The camera repeats one datagram several times
/// within a few milliseconds and answers itself byte for byte, so two
/// announcements with identical content inside this window are the same
/// announcement. Two presses of the button are seconds apart and carry a
/// different duration, so they are not collapsed.
const RETRANSMISSION: Duration = Duration::from_millis(2_000);

/// Cap on the hosts remembered below. One entry per source address is not by
/// itself bounded: a peer spraying source addresses would grow this map without
/// end, so it is capped and the oldest entry gives way.
const SEEN_HOSTS_MAX: usize = 64;

/// Last announcement seen per host, `(json, instant)`: see [`RETRANSMISSION`].
#[derive(Default)]
struct Seen {
	hosts: std::collections::HashMap<std::net::IpAddr, (String, Instant)>,
}

impl Seen {
	/// True when this announcement has to be passed on. The camera sends the
	/// same datagram up to four times in two milliseconds: without this filter a
	/// single press would write several log lines and trigger several reads. The
	/// same content seen again inside the window is the same announcement; past
	/// it, or with different content, it is a new one. Two presses are always
	/// further apart than this window and carry a different `tid` and duration.
	fn is_new(&mut self, src: std::net::IpAddr, announcement: &Announcement) -> bool {
		let now = Instant::now();
		let repeat = self.hosts.get(&src).is_some_and(|(json, when)| {
			json == announcement.json() && now < *when + RETRANSMISSION
		});
		if repeat {
			return false;
		}
		if !self.hosts.contains_key(&src) && self.hosts.len() >= SEEN_HOSTS_MAX {
			// Bounded by construction: the oldest remembered host gives way.
			let oldest = self
				.hosts
				.iter()
				.min_by_key(|(_, (_, when))| *when)
				.map(|(ip, _)| *ip);
			if let Some(ip) = oldest {
				self.hosts.remove(&ip);
			}
		}
		self.hosts.insert(src, (announcement.json().to_owned(), now));
		true
	}
}

/// When this process started. The wake server is the process for every deployment this
/// crate ships into, so process uptime is the delay an operator is asking about: how long
/// after the module came up did this camera's first beat arrive. Measured from the first
/// call rather than from a constructor so the register path does not need a field passed
/// through four signatures; it is monotonic for the life of the process, which is all the
/// number is used for.
fn uptime() -> Duration {
	static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
	START.get_or_init(Instant::now).elapsed()
}

/// What the register server does with an event announcement (`0x31`).
///
/// It shows it to the observer installed by the binary, and nothing else.
/// Deliberately:
///
/// - **no wake.** An announcement is a fact the camera tells, not a request;
///   holding a wake lock for it would spend the battery that has to last a
///   month. What the binary makes of a heard announcement is its business, and
///   it is the only place that knows the configured cameras.
/// - **no network write from the listen loop**: the observer is a hand-off, and
///   that call is non-blocking by contract (see [`set_observer`]).
///
/// The camera repeats the same datagram and replies to itself byte for byte, so
/// [`Seen::is_new`] recognises the repeats by content and drops them. Two
/// different announcements carry different content, so they pass.
fn handle_announcement(
	announcement: &Announcement,
	src: std::net::SocketAddr,
	seen: &mut Seen,
	observer: Option<&(dyn Fn(Announcement) + Send + Sync + 'static)>,
) {
	if !seen.is_new(src.ip(), announcement) {
		debug!(
			%src,
			tid = announcement.tid(),
			"register: announcement already seen (retransmission)"
		);
		return;
	}
	// The binary's own announcement line names the camera and is the one an operator
	// correlates with the published `press`, so it stays at info. This line carries the
	// wire detail - tid, masked uid, reason, duration - and would otherwise print a
	// second line for every wake in a default journal; it is a level up away.
	debug!(
		%src,
		tid = announcement.tid(),
		uid = %announcement.masked_uid(),
		event = announcement.event(),
		reason = ?announcement.reason(),
		duration = ?announcement.duration(),
		"register: camera event announcement (0x31)"
	);
	if let Some(observer) = observer {
		observer(announcement.clone());
	}
}

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
	// Retransmission memory belongs to the loop: see [`Seen`].
	let mut seen = Seen::default();
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
				handle(
					&sock,
					&registry,
					&anchors,
					&cfg,
					stale,
					src,
					&buf[..n],
					&cancel,
					&mut seen,
				)
				.await;
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
	seen: &mut Seen,
) {
	// The event announcement is judged first: its magic is not discovery's, and
	// letting it fall into the "bad packet" line below is letting a button press
	// vanish without a trace.
	if let Ok(announcement) = decode_announcement(raw) {
		handle_announcement(&announcement, src, seen, OBSERVER.get().map(|o| &**o));
		return;
	}
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
			match refusal_explanation(&cfg.declared, &c.uid) {
				Some(reason) => info!(%src, uid = %UidTail(&c.uid), "{reason}"),
				None => debug!(%src, uid = %c.uid, "C2R_C for unknown or stale UID"),
			}
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
			// The first beat of this uid since the process came up. Readable at the
			// default level (`warn,bairelay=info`), with the two numbers that make the
			// beat measurable on site: how long the camera took to show up after
			// startup, and the cadence this server actually told it to use. The advice
			// is read from `RuntimeConfig`, the same structure that refuses a freshness
			// window shorter than the beat, so the number in this line and the number in
			// that refusal are the same kind of number and can be compared. Without the
			// advice here, a long-advice experiment is unreadable at the level anybody
			// runs. The beat itself stays at debug because of **volume** - one line per
			// camera every ten seconds, forever - and not because this crate is out of
			// reach: `bairelay=info` matches `bairelay_wake_server` by prefix, so the
			// lines below are in a default journal already. Nobody should go looking for a
			// filter to raise in order to see them.
			info!(
				%src,
				uid = %UidTail(&hb.uid),
				first_beat_after_startup_ms = uptime().as_millis() as u64,
				advised_heartbeat_ms = cfg.hb_advice_ms,
				"wake-server registered camera"
			);
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
	// The cadence instrument. The beat itself is a debug line, and the novelty line
	// the deployed binary prints is keyed on the source port, which carries value:
	// a new port is the camera reopening a socket. Neither can measure a period, so
	// nothing at the level the operator runs could tell whether a longer advice
	// changed anything. One bounded line per camera per window, with the spread and
	// the advice we sent, which is the pair the experiment needs.
	if let Some(window) = registry.note_beat(&hb.uid, now) {
		info!(
			uid = %UidTail(&hb.uid),
			window_secs = window.window_secs,
			beats = window.beats,
			beat_min_ms = window.min_ms,
			beat_max_ms = window.max_ms,
			beat_mean_ms = window.mean_ms,
			advised_heartbeat_ms = cfg.hb_advice_ms,
			"wake-server beat cadence"
		);
	}
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
				hb: cfg.hb_advice_ms,
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

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bcudp::announcement::decode_announcement;

	// The camera pushes these by itself, unasked, and while it sleeps they are the
	// only path by which a button press arrives. Real bytes off the wire, inline.

	/// Press release: `{"event":4,"duration":19281,…,"reason":19}`.
	const ANNOUNCE_PRESS: &str = concat!(
		"31cf872a4000000096010000ffffffff00000000",
		// // Captured from the wire on the operator's camera; the uid field is a synthetic
		// // stand-in of the same width, the bytes around it are the capture as it arrived.
		"3935323730303053594e54485445535400000000000000000000000000000000",
		"1f2924649a1f486946ef1878db1b3b1a941103e3b57250e84cc3f79c78db40af",
		"19a19d638051496a51e018338e413502c25643b7f2280faa4c8ba3c4398c1fa101e4cb6a",
	);

	/// Activity end: `{"event":3}`, 67 bytes.
	const ANNOUNCE_ACTIVITY_END: &str = concat!(
		"31cf872a0b0000008c010000ffffffff00000000",
		"3935323730303053594e54485445535400000000000000000000000000000000",
		"b539c75fac1f48697cef1878ed1c6a",
	);

	fn bytes(hex: &str) -> Vec<u8> {
		(0..hex.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
			.collect()
	}

	fn decode(hex: &str) -> Announcement {
		decode_announcement(&bytes(hex)).expect("a captured announcement decodes")
	}

	/// The point of the whole hook: a heard announcement leaves the listen loop
	/// and lands in the binary's hands.
	#[test]
	fn an_announcement_reaches_the_observer() {
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		let observer = move |a: Announcement| {
			let _ = tx.send((a.event(), a.reason(), a.uid().to_owned()));
		};
		let mut seen = Seen::default();
		handle_announcement(
			&decode(ANNOUNCE_PRESS),
			"192.168.1.50:58200".parse().unwrap(),
			&mut seen,
			Some(&observer),
		);
		let (event, reason, uid) = rx.try_recv().expect("the announcement must come out");
		assert_eq!(event, 4, "a press release is event 4");
		assert_eq!(reason, Some(19), "reason 19 is the button");
		assert_eq!(uid, "9527000SYNTHTEST", "the uid is the short wire form");
	}

	/// The camera sends the same datagram up to four times. Passed through, one
	/// press would become four reads of a battery camera, which is exactly the
	/// cost the battery rule exists to stop.
	#[test]
	fn retransmissions_collapse_into_one() {
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		let observer = move |a: Announcement| {
			let _ = tx.send(a.event());
		};
		let mut seen = Seen::default();
		let src: std::net::SocketAddr = "192.168.1.50:58200".parse().unwrap();
		for _ in 0..4 {
			handle_announcement(&decode(ANNOUNCE_PRESS), src, &mut seen, Some(&observer));
		}
		assert_eq!(rx.try_recv().unwrap(), 4);
		assert!(
			rx.try_recv().is_err(),
			"three retransmissions must not reach the observer"
		);
	}

	/// The filter recognises repeats by content, not by address: two different
	/// announcements from the same camera are two events, or the second press of
	/// the evening would be swallowed by the first.
	#[test]
	fn a_different_announcement_is_not_a_retransmission() {
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		let observer = move |a: Announcement| {
			let _ = tx.send(a.event());
		};
		let mut seen = Seen::default();
		let src: std::net::SocketAddr = "192.168.1.50:58200".parse().unwrap();
		handle_announcement(&decode(ANNOUNCE_PRESS), src, &mut seen, Some(&observer));
		handle_announcement(&decode(ANNOUNCE_ACTIVITY_END), src, &mut seen, Some(&observer));
		assert_eq!(rx.try_recv().unwrap(), 4);
		assert_eq!(rx.try_recv().unwrap(), 3, "the activity end is its own event");
	}

	/// A frame that only looks like an announcement must not be handed to anyone
	/// as if it were one.
	#[test]
	fn a_damaged_announcement_reaches_nobody() {
		let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
		let observer = move |a: Announcement| {
			let _ = tx.send(a.event());
		};
		let mut seen = Seen::default();
		let mut frame = bytes(ANNOUNCE_PRESS);
		let last = frame.len() - 1;
		frame[last] ^= 0xff;
		// The same shape the dispatch uses: a frame that does not verify is not
		// an announcement, and nothing is passed on.
		if let Ok(a) = decode_announcement(&frame) {
			handle_announcement(&a, "192.168.1.50:58200".parse().unwrap(), &mut seen, Some(&observer));
		}
		assert!(rx.try_recv().is_err(), "a damaged frame must not reach the observer");
	}

	/// The retransmission map is per host and capped: a peer that sprays source
	/// addresses must not be able to grow the wake server's memory by sending
	/// announcements nobody asked for.
	#[test]
	fn the_retransmission_map_stays_bounded() {
		let mut seen = Seen::default();
		for i in 0..(SEEN_HOSTS_MAX + 40) {
			let src = std::net::SocketAddr::from(([10, 0, 0, (i % 250) as u8], 58200));
			seen.is_new(src.ip(), &decode(ANNOUNCE_ACTIVITY_END));
		}
		assert_eq!(
			seen.hosts.len(),
			SEEN_HOSTS_MAX,
			"the map holds at most one entry per remembered host, and at most that many hosts"
		);
	}

	/// The observer is optional by contract: a binary that did not install one
	/// still runs, and an announcement is then logged and dropped.
	#[test]
	fn no_observer_installed_is_not_a_failure() {
		let mut seen = Seen::default();
		handle_announcement(
			&decode(ANNOUNCE_PRESS),
			"192.168.1.50:58200".parse().unwrap(),
			&mut seen,
			None,
		);
	}
}
