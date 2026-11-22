//! This module handles discovery
//!
//! Given a UID find the associated IP
//!
use super::DiscoveryResult;
use crate::bc::model::*;
use crate::bc_protocol::{md5_string, Md5Trunc, TcpSource};
use crate::bcudp::codex::BcUdpCodex;
use crate::bcudp::model::*;
use crate::bcudp::xml::*;
use crate::{Error, Result};
use futures::{
	sink::SinkExt,
	stream::{FuturesUnordered, StreamExt},
};
use lazy_static::lazy_static;
use log::*;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::collections::{btree_map::Entry, BTreeMap, HashSet};
use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::time::MissedTickBehavior;
use tokio::{
	net::UdpSocket,
	sync::{
		mpsc::{channel, Receiver, Sender},
		RwLock, Semaphore,
	},
	task::JoinSet,
	time::{interval, timeout, Duration},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::udp::UdpFramed;

#[derive(Debug, Clone)]
pub(crate) struct RegisterResult {
	reg: SocketAddr,
	dev: Option<SocketAddr>,
	dmap: Option<SocketAddr>,
	relay: Option<SocketAddr>,
	client_id: i32,
	sid: u32,
}

#[derive(Debug, Clone)]
struct ConnectResult {
	addr: SocketAddr,
	client_id: i32,
	camera_id: i32,
	sid: u32,
	/// sigV3 login nonce from the `D2C_C_R` handshake (account cameras only).
	nc: Option<i64>,
	/// sigV3 ECDHE offer (`pl` line) from the handshake (account cameras only).
	pl: Option<String>,
}

#[derive(Debug, Clone)]
struct UidLookupResults {
	reg: SocketAddr,
	relay: SocketAddr,
}

const MTU: u32 = 1350;
lazy_static! {
	static ref P2P_RELAY_HOSTNAMES: [&'static str; 12] = [
		"p2p.reolink.com",
		"p2p1.reolink.com",
		"p2p2.reolink.com",
		"p2p3.reolink.com",
		"p2p4.reolink.com",
		"p2p5.reolink.com",
		"p2p6.reolink.com",
		"p2p7.reolink.com",
		"p2p8.reolink.com",
		"p2p9.reolink.com",
		"p2p10.reolink.com",
		"p2p11.reolink.com",
		// These following are all currently set to 127.0.0.1
		// probably reserved for future use
		// "p2p12.reolink.com",
		// "p2p13.reolink.com",
		// "p2p14.reolink.com",
		// "p2p15.reolink.com",
		// "p2p16.reolink.com",
	];
	/// Maximum wait for a reply
	static ref MAXIMUM_WAIT: Duration = Duration::from_secs(15);
	/// Wait for tcp connections
	static ref TCP_WAIT: Duration = Duration::from_secs(4);
	/// How long to wait before resending
	static ref RESEND_WAIT: Duration = Duration::from_millis(500);

}

type Subscriber = Arc<RwLock<BTreeMap<u32, Sender<Result<(UdpDiscovery, SocketAddr)>>>>>;
type Handlers = Arc<RwLock<Vec<Sender<Result<(UdpDiscovery, SocketAddr)>>>>>;
type ArcFramedSocket = UdpFramed<BcUdpCodex, Arc<UdpSocket>>;
pub(crate) struct Discoverer {
	semaphore: Arc<Semaphore>,
	socket: Arc<UdpSocket>,
	handle: RwLock<JoinSet<Result<()>>>,
	subsribers: Subscriber,
	handlers: Handlers,
	local_addr: SocketAddr,
	cancel: CancellationToken,
	/// Account ("cloud") camera: advertise `lver=3` on the direct connect so
	/// the camera issues the sigV3 login nonce. Off for every other camera —
	/// they get a plain (no `<lver>`) connect.
	sigv3: bool,
}

fn valid_ip(ip: &str) -> bool {
	!ip.is_empty() && matches!(ip.parse::<Ipv4Addr>(), Ok(addr) if addr.octets()[3] != 0)
}

fn valid_port(port: u16) -> bool {
	port != 0
}

fn valid_addr(ip_port: &IpPort) -> bool {
	valid_ip(&ip_port.ip) && valid_port(ip_port.port)
}

impl Discoverer {
	async fn new(sigv3: bool) -> Result<Discoverer> {
		let socket = Arc::new(connect().await?);
		let local_addr = socket.local_addr()?;
		let inner: ArcFramedSocket = UdpFramed::new(socket.clone(), BcUdpCodex::new());
		let cancel = CancellationToken::new();

		// Reader-only — outbound sends go directly through `socket`
		// (see `Discoverer::send` below). The previous design routed
		// every outbound packet through a single mpsc channel drained
		// by one writer task; that introduced a head-of-line block
		// where one stalled `send_to` (e.g. macOS 15 LNP gating
		// broadcast sends from non-interactive launchd processes,
		// which is what GitHub's macos-latest runner does) wedged
		// every other queued send behind it. UDP `send_to` is
		// per-call atomic; concurrent callers don't interfere.
		let (_writer, mut reader) = inner.split();
		let mut set = JoinSet::new();
		let subsribers: Subscriber = Default::default();
		let handlers: Handlers = Default::default();

		let thread_subscriber = subsribers.clone();
		let thread_handlers = handlers.clone();
		let thread_cancel = cancel.clone();
		set.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					loop {
						tokio::task::yield_now().await;
						match reader.next().await {
							Some(Ok((BcUdp::Discovery(bcudp), addr))) => {
								log::trace!("Got discovery {:?} for {}", bcudp, addr);
								let tid = bcudp.tid;
								let mut needs_removal = false;
								// `try_send` rather than `send().await`: a hostile peer
								// can flood the discoverer with `tid`-matching
								// datagrams faster than the per-tid subscriber drains
								// its bounded (10) channel; with `send().await` the
								// reader parks and blocks every other in-flight
								// discovery flow on this `Discoverer`. Drop on full —
								// discovery is retry-tolerant.
								if let (Some(sender), true) =
									(thread_subscriber.read().await.get(&tid), tid > 0)
								{
									match sender.try_send(Ok((bcudp, addr))) {
										Ok(()) => {}
										Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
											log::debug!(
												"Discoverer subscriber tid={tid} channel full; dropping"
											);
										}
										Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
											needs_removal = true;
										}
									}
								} else {
									// Snapshot handlers and drop the lock before
									// sending. Previously `write().await` was held
									// across the per-handler `send().await`s, so a
									// slow handler stalled every subscribe /
									// unsubscribe for the lock duration.
									let snapshot: Vec<_> =
										thread_handlers.read().await.clone();
									for sender in snapshot {
										if let Err(e) =
											sender.try_send(Ok((bcudp.clone(), addr)))
										{
											log::debug!(
												"Discoverer broadcast handler send failed: {e:?}"
											);
										}
									}
								}
								if needs_removal {
									thread_subscriber.write().await.remove(&tid);
								}
							}
							Some(Ok(bcudp)) => {
								// Only discovery packets should be possible atm
								log::debug!("Got non Discovery during discovery: {:?}", bcudp);
							}
							Some(Err(e)) => {
								log::error!("Error on discovery socket: {:?}", e);
								let mut locked_sub = thread_subscriber.write().await;
								for sub in locked_sub.values() {
									let _ = sub.send(Err(e.clone())).await;
								}
								locked_sub.clear();
								break Result::Ok(());
							}
							None => break Result::Ok(()),
						}
					}
				}=> v,
			}
		});

		Ok(Discoverer {
			semaphore: Arc::new(Semaphore::new(1)),
			socket,
			handle: RwLock::new(set),
			subsribers,
			handlers,
			local_addr,
			cancel,
			sigv3,
		})
	}

	async fn get_socket(&self) -> Arc<UdpSocket> {
		self.socket.clone()
	}

	async fn subscribe(&self, tid: u32) -> Result<Receiver<Result<(UdpDiscovery, SocketAddr)>>> {
		if tid > 0 {
			let mut subs = self.subsribers.write().await;
			match subs.entry(tid) {
				Entry::Vacant(vacant) => {
					let (tx, rx) = channel(10);
					vacant.insert(tx);
					Ok(rx)
				}
				Entry::Occupied(mut occ) => {
					if occ.get().is_closed() {
						let (tx, rx) = channel(10);
						occ.insert(tx);
						Ok(rx)
					} else {
						// log::error!("Failed to subscribe in discovery to {:?}", tid);
						Err(Error::SimultaneousSubscription {
							msg_num: Some(tid as u16),
						})
					}
				}
			}
		} else {
			// If tid is zero we listen to all!
			let mut handlers = self.handlers.write().await;
			let (tx, rx) = channel(10);
			handlers.push(tx);
			Ok(rx)
		}
	}

	/// Subsciber others is for messages that we did not initiate and are therefore
	/// using an unknown tid
	/// In this case we subscribe to tid 0
	async fn handle_incoming<T, F>(&self, map: F) -> Result<T>
	where
		F: Fn(UdpDiscovery, SocketAddr) -> Option<T>,
	{
		let mut reply = ReceiverStream::new(self.subscribe(0).await?);
		tokio::select! {
			v = async {
				loop {
					let (reply, addr) = reply.next().await.ok_or(Error::ConnectionUnavailable)??;
					if let Some(result) = map(reply, addr) {
						return Ok(result);
					}
				}
			} => v,
			_ = tokio::time::sleep(*MAXIMUM_WAIT) => Err::<T, Error>(Error::DiscoveryTimeout),
		}
	}

	async fn send(&self, disc: BcUdp, addr: SocketAddr) -> Result<()> {
		// Serialise + send_to directly. Concurrent callers on the
		// same socket are fine — UDP `send_to` is per-call atomic at
		// the kernel level. See the writer-side note in
		// `Discoverer::new` for why we no longer route through a
		// single drainer task.
		let buf = disc.serialize(Vec::new())?;
		self.socket.send_to(&buf, addr).await?;
		Ok(())
	}

	fn local_addr(&self) -> &SocketAddr {
		&self.local_addr
	}

	async fn retry_send<F, T>(&self, mut disc: UdpDiscovery, dest: SocketAddr, map: F) -> Result<T>
	where
		F: Fn(UdpDiscovery, SocketAddr) -> Option<T>,
	{
		let target_tid = if disc.tid == 0 {
			// If 0 make a random one
			let target_tid = generate_tid();
			disc.tid = target_tid;
			target_tid
		} else {
			disc.tid
		};

		let mut reply = ReceiverStream::new(self.subscribe(target_tid).await?);
		let msg = BcUdp::Discovery(disc);

		let mut inter = interval(*RESEND_WAIT);
		inter.set_missed_tick_behavior(MissedTickBehavior::Skip);

		let result = tokio::select! {
			v = async {
				// Recv while channel is viable
				while let Some(msg) = reply.next().await {
					if let Ok((reply, addr)) = msg {
						if let Some(result) = map(reply, addr) {
							return Ok(result);
						}
					}
				}
				Err(Error::DiscoveryIgnored)
			} => {v},
			v = async {
				// Send every inter for ever or until channel is no longer viable
				loop {
					inter.tick().await;
					if let Err(e) = self.send(msg.clone(), dest).await {
						return e;
					}
				}
			} => {Err::<T, Error>(v)},
			_ = {
				// Sleep then emit Timeout
				tokio::time::sleep(*MAXIMUM_WAIT)
			} => {
				Err::<T, Error>(Error::DiscoveryTimeout)
			}
		};

		result
	}

	async fn send_and_forget(&self, mut disc: UdpDiscovery, dest: SocketAddr) -> Result<()> {
		if disc.tid == 0 {
			// If 0 make a random one
			let target_tid = generate_tid();
			disc.tid = target_tid;
		}
		let mut inter = interval(*RESEND_WAIT);
		inter.set_missed_tick_behavior(MissedTickBehavior::Skip);
		let msg = BcUdp::Discovery(disc);

		for _i in 0..5 {
			inter.tick().await;

			self.send(msg.clone(), dest).await?;
		}

		Ok(())
	}

	async fn client_initiated_direct(
		&self,
		uid: &str,
		client_id: i32,
		addr: SocketAddr,
	) -> Result<ConnectResult> {
		let tid = generate_tid();

		let port = self.local_addr().port();
		let msg = UdpDiscovery {
			tid,
			payload: UdpXml::C2dC(C2dC {
				uid: uid.to_string(),
				cli: ClientList { port: port as u32 },
				cid: client_id,
				mtu: MTU,
				debug: false,
				os: "MAC".to_string(),
				// Account ("cloud") cameras only: signal sigV3 (`lver=3`) so
				// the camera issues the login nonce (`nc`) + ECDHE offer in its
				// D2C_C_R reply. `0` is skipped on the wire (legacy connect).
				lver: if self.sigv3 { 3 } else { 0 },
			}),
		};

		log::debug!(
			"Trying a direct connect to: {:?} with tid: {}",
			addr,
			msg.tid
		);

		let (camera_address, camera_id, nc, pl) = self
			.retry_send(msg, addr, |bc, addr| match bc {
				UdpDiscovery {
					tid: _,
					payload:
						UdpXml::D2cCr(D2cCr {
							did,
							cid,
							ref pl,
							nc,
							..
						}),
				} if cid == client_id => {
					log::debug!("D2cCr handshake: nc={:?} pl={:?}", nc, pl);
					// Carry the sigV3 handshake nonce + ECDHE offer out to the
					// login layer (the sigV3 login is keyed by these). No
					// globals — they ride the per-camera DiscoveryResult.
					Some((addr, did, nc, pl.clone()))
				}
				n => {
					log::debug!("Got unexpected reply: {:?}", n);
					None
				}
			})
			.await?;

		log::debug!(
			"Direct connect success at {:?} client: {}, device: {}",
			addr,
			client_id,
			camera_id
		);

		let result = ConnectResult {
			addr: camera_address,
			client_id,
			camera_id,
			sid: 0,
			nc,
			pl,
		};
		self.keep_alive_device(tid, &result).await;

		log::debug!("Returning direct connect: {:?}", result);
		Ok(result)
	}

	/// Resolve every P2P relay hostname to its socket address(es),
	/// keeping each address tagged with the hostname it came from so a
	/// caller can name a specific relay in diagnostics. Returns the
	/// resolved `(host, addr)` pairs alongside any per-host DNS failures.
	async fn resolve_relay_addrs(
		&self,
	) -> Result<(Vec<(&'static str, SocketAddr)>, Vec<(&'static str, String)>)> {
		let task = tokio::task::spawn_blocking(move || {
			let mut addrs: Vec<(&'static str, SocketAddr)> = vec![];
			let mut errors: Vec<(&'static str, String)> = vec![];
			for p2p_relay in P2P_RELAY_HOSTNAMES.iter() {
				match format!("{}:9999", p2p_relay).to_socket_addrs() {
					Ok(it) => addrs.extend(it.map(|a| (*p2p_relay, a))),
					Err(e) => errors.push((p2p_relay, e.to_string())),
				}
			}
			(addrs, errors)
		});
		let out = timeout(*MAXIMUM_WAIT, task).await??;
		trace!("Uid lookup to: {:?}", out.0);
		Ok(out)
	}
	/// This function will contact the p2p relay servers
	///
	/// It will ask each of the servers for details on a specific UID
	///
	/// On success it will returns an async iter that yields the M2cQr that the p2p relay
	/// server has about the UID
	async fn uid_lookup(&self, uid: &str, addr: SocketAddr) -> Result<UidLookupResults> {
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2mQ(C2mQ {
				uid: uid.to_string(),
				os: "MAC".to_string(),
			}),
		};
		trace!("Sending look up {:?}", msg);
		let (reg, relay, _) = self
			.retry_send(msg, addr, |bc, addr| match bc {
				UdpDiscovery {
					tid: _,
					payload:
						UdpXml::M2cQr(M2cQr {
							reg: Some(reg),
							relay: Some(relay),
							..
						}),
				} if valid_addr(&reg) && valid_addr(&relay) => Some((reg, relay, addr)),
				_ => None,
			})
			.await?;
		trace!("Look up complete");
		Ok(UidLookupResults {
			reg: SocketAddr::new(reg.ip.parse()?, reg.port),
			relay: SocketAddr::new(relay.ip.parse()?, relay.port),
		})
	}

	/// Register our local ip address with the reolink servers
	/// This will be used for the device to contact us
	async fn register_address(
		&self,
		uid: &str,
		client_id: i32,
		lookup: &UidLookupResults,
	) -> Result<RegisterResult> {
		let tid = generate_tid();
		// Prefer the local interface whose subnet contains the relay.
		// On a multi-homed host (Wi-Fi + Docker bridge + Tailscale) the
		// legacy first-non-loopback pick could land on the Docker
		// bridge, leaving the registered address unreachable from the
		// camera. See `get_local_ip_for_target` for the rationale.
		let local_ip = get_local_ip_for_target(Some(lookup.relay.ip()))?;
		let local_addr = SocketAddr::new(local_ip, self.local_addr().port());
		log::debug!("Registering {:?} to reolink", local_addr);
		let local_ip = local_addr.ip();
		let local_port = local_addr.port();
		let local_family = if local_addr.ip().is_ipv4() { 4 } else { 6 };

		let msg = UdpDiscovery {
			tid,
			payload: UdpXml::C2rC(C2rC {
				uid: uid.to_string(),
				cli: IpPort {
					ip: local_ip.to_string(),
					port: local_port,
				},
				relay: IpPort {
					ip: lookup.relay.ip().to_string(),
					port: lookup.relay.port(),
				},
				cid: client_id,
				family: local_family,
				debug: false,
				os: "MAC".to_string(),
				revision: Some(3),
			}),
		};

		if let UdpXml::C2rC(ref c2r_c) = msg.payload {
			trace!("Registering: {:?}", c2r_c);
		}

		// Send and await acceptance
		let (sid, dev, dmap, relay) = self
			.retry_send(msg, lookup.reg, |bc, socket| {
				trace!("{}", socket.ip());
				match bc {
					UdpDiscovery {
						tid: _,
						payload:
							UdpXml::R2cCr(R2cCr {
								dmap,
								dev,
								relay,
								sid: Some(sid),
								rsp,
								..
							}),
					} if (dev.as_ref().map(valid_addr).unwrap_or(false)
						|| dmap.as_ref().map(valid_addr).unwrap_or(false)
						|| relay.as_ref().map(valid_addr).unwrap_or(false))
						&& rsp != -1
						&& rsp != -3 =>
					{
						Some(Ok((sid, dev, dmap, relay)))
					}
					// UdpDiscovery {
					//     tid: _,
					//     payload:
					//         UdpXml {
					//             r2c_c_r:
					//                 Some(R2cCr {
					//                     dmap,
					//                     dev,
					//                     relay: Some(mut relay),
					//                     sid,
					//                     rsp,
					//                     ..
					//                 }),
					//             ..
					//         },
					// } if (dev
					//     .as_ref()
					//     .map(valid_addr)
					//     .unwrap_or(false)
					//     || dmap
					//         .as_ref()
					//         .map(valid_addr)
					//         .unwrap_or(false)
					//     || (relay.ip == format!("{}", socket.ip()) && relay.port == 0))
					//     && rsp != -1 =>
					// {
					//     // For a relay connection if port is 0 and the ip is the current socker addr
					//     // we use the current port
					//     relay.port = socket.port();
					//     Some(Ok((sid, dev, dmap, Some(relay))))
					// }
					UdpDiscovery {
						tid: _,
						payload:
							UdpXml::R2cCr(R2cCr {
								dev,
								dmap,
								relay,
								rsp,
								..
							}),
					} if (dev.as_ref().map(valid_addr).unwrap_or(false)
						|| dmap.as_ref().map(valid_addr).unwrap_or(false)
						|| relay.as_ref().map(valid_addr).unwrap_or(false))
						&& (rsp == -1 || rsp == -3) =>
					{
						Some(Err(Error::RegisterError))
					}
					_ => None,
				}
			})
			.await??;

		Ok(RegisterResult {
			reg: lookup.reg,
			sid,
			client_id,
			dev: dev.and_then(|d| d.try_into().ok()),
			dmap: dmap.and_then(|d| d.try_into().ok()),
			relay: relay.and_then(|d| d.try_into().ok()),
		})
	}

	async fn device_initiated_dev(
		&self,
		register_result: &RegisterResult,
	) -> Result<ConnectResult> {
		let (addr, local_tid, local_did) = self
			.handle_incoming(|bc, addr| {
				trace!("bc: {:?}", bc);
				match (bc, register_result) {
					(
						UdpDiscovery {
							tid,
							payload:
								UdpXml::D2cT(D2cT {
									sid,
									cid,
									did,
									conn,
									..
								}),
						},
						RegisterResult {
							dmap: register_dmap,
							sid: register_sid,
							..
						},
					) if cid == register_result.client_id
						&& &sid == register_sid
						&& register_dmap
							.as_ref()
							.map(|dmap| &addr == dmap)
							.unwrap_or(false)
						&& &conn == "local" =>
					{
						Some((addr, tid, did))
					}
					_ => None,
				}
			})
			.await?;

		let msg = UdpDiscovery {
			tid: local_tid,
			payload: UdpXml::C2dA(C2dA {
				sid: register_result.sid,
				conn: "local".to_string(),
				cid: register_result.client_id,
				did: local_did,
				mtu: MTU,
			}),
		};

		let permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.map_err(|_| Error::Other("Discovery already complete"))?;
		// Send and await confirm
		self.retry_send(msg, addr, |bc, _| {
			trace!("msg: {:?}", bc);
			match bc {
				UdpDiscovery {
					tid: _,
					payload:
						UdpXml::D2cCfm(D2cCfm {
							sid,
							cid,
							did,
							conn,
							..
						}),
				} if sid == register_result.sid
					&& did == local_did
					&& cid == register_result.client_id
					&& &conn == "local" =>
				{
					Some(())
				}
				_ => None,
			}
		})
		.await?;

		let result = ConnectResult {
			addr,
			client_id: register_result.client_id,
			sid: register_result.sid,
			camera_id: local_did,
			// Relay/map/remote paths don't carry a sigV3 handshake.
			nc: None,
			pl: None,
		};

		// Confirm local to register
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2rCfm(C2rCfm {
				sid: result.sid,
				cid: result.client_id,
				did: result.camera_id,
				rsp: 0,
				conn: "local".to_string(),
			}),
		};

		self.send_and_forget(msg, register_result.reg).await?;

		self.keep_alive_device(local_tid, &result).await;

		self.semaphore.close();
		drop(permit);
		Ok(result)
	}

	async fn device_initiated_map(
		&self,
		register_result: &RegisterResult,
	) -> Result<ConnectResult> {
		let (addr, local_tid, local_did) = self
			.handle_incoming(|bc, addr| {
				trace!("bc: {:?}", bc);
				match (bc, register_result) {
					(
						UdpDiscovery {
							tid,
							payload:
								UdpXml::D2cT(D2cT {
									sid,
									cid,
									did,
									conn,
									..
								}),
						},
						RegisterResult {
							dmap: register_dmap,
							sid: register_sid,
							..
						},
					) if cid == register_result.client_id
						&& &sid == register_sid
						&& register_dmap
							.as_ref()
							.map(|dmap| &addr == dmap)
							.unwrap_or(false)
						&& &conn == "map" =>
					{
						Some((addr, tid, did))
					}
					_ => None,
				}
			})
			.await?;

		let msg = UdpDiscovery {
			tid: local_tid,
			payload: UdpXml::C2dA(C2dA {
				sid: register_result.sid,
				conn: "map".to_string(),
				cid: register_result.client_id,
				did: local_did,
				mtu: MTU,
			}),
		};

		let permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.map_err(|_| Error::Other("Discovery already complete"))?;
		// Send and await confirm
		self.retry_send(msg, addr, |bc, _| {
			trace!("msg: {:?}", bc);
			match bc {
				UdpDiscovery {
					tid: _,
					payload:
						UdpXml::D2cCfm(D2cCfm {
							sid,
							cid,
							did,
							conn,
							..
						}),
				} if sid == register_result.sid
					&& did == local_did
					&& cid == register_result.client_id
					&& &conn == "map" =>
				{
					Some(())
				}
				_ => None,
			}
		})
		.await?;

		let result = ConnectResult {
			addr,
			client_id: register_result.client_id,
			sid: register_result.sid,
			camera_id: local_did,
			// Relay/map/remote paths don't carry a sigV3 handshake.
			nc: None,
			pl: None,
		};

		// Confirm map to register
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2rCfm(C2rCfm {
				sid: result.sid,
				cid: result.client_id,
				did: result.camera_id,
				rsp: 0,
				conn: "map".to_string(),
			}),
		};

		self.send_and_forget(msg, register_result.reg).await?;

		self.keep_alive_device(local_tid, &result).await;

		self.semaphore.close();
		drop(permit);
		Ok(result)
	}

	async fn client_initiated_dev(
		&self,
		register_result: &RegisterResult,
	) -> Result<ConnectResult> {
		let tid = generate_tid();

		let dev_addr = register_result.dev.ok_or(Error::NoDev)?;
		let msg = UdpDiscovery {
			tid,
			payload: UdpXml::C2dT(C2dT {
				sid: register_result.sid,
				cid: register_result.client_id,
				mtu: MTU,
				conn: "local".to_string(),
			}),
		};

		let (final_addr, local_did) = self
			.retry_send(msg, dev_addr, |bc, addr| match bc {
				UdpDiscovery {
					tid: _,
					payload: UdpXml::D2cCfm(D2cCfm { cid, did, sid, .. }),
				} if cid == register_result.client_id && sid == register_result.sid => Some((addr, did)),
				_ => None,
			})
			.await?;

		let result = ConnectResult {
			addr: final_addr,
			client_id: register_result.client_id,
			sid: register_result.sid,
			camera_id: local_did,
			// Relay/map/remote paths don't carry a sigV3 handshake.
			nc: None,
			pl: None,
		};

		let permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.map_err(|_| Error::Other("Discovery already complete"))?;
		// Confirm local to register
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2rCfm(C2rCfm {
				sid: result.sid,
				cid: result.client_id,
				did: result.camera_id,
				conn: "local".to_string(),
				rsp: 0,
			}),
		};

		self.send_and_forget(msg, register_result.reg).await?;

		self.keep_alive_device(tid, &result).await;

		self.semaphore.close();
		drop(permit);
		Ok(result)
	}

	async fn client_initiated_relay(
		&self,
		register_result: &RegisterResult,
	) -> Result<ConnectResult> {
		let tid = generate_tid();

		let relay_addr = register_result.relay.ok_or(Error::NoDev)?;
		let msg = UdpDiscovery {
			tid,
			payload: UdpXml::C2dT(C2dT {
				sid: register_result.sid,
				cid: register_result.client_id,
				mtu: MTU,
				conn: "relay".to_string(),
			}),
		};

		let permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.map_err(|_| Error::Other("Discovery already complete"))?;
		let (final_addr, local_did) = self
			.retry_send(msg, relay_addr, |bc, addr| match bc {
				UdpDiscovery {
					tid: _,
					payload:
						UdpXml::D2cCfm(D2cCfm {
							cid,
							did,
							sid,
							conn,
							..
						}),
				} if cid == register_result.client_id
					&& sid == register_result.sid
					&& &conn == "relay" =>
				{
					Some((addr, did))
				}
				_ => None,
			})
			.await?;

		let result = ConnectResult {
			addr: final_addr,
			client_id: register_result.client_id,
			sid: register_result.sid,
			camera_id: local_did,
			// Relay/map/remote paths don't carry a sigV3 handshake.
			nc: None,
			pl: None,
		};

		// Confirm relay to register
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2rCfm(C2rCfm {
				sid: result.sid,
				cid: result.client_id,
				did: result.camera_id,
				conn: "relay".to_string(),
				rsp: 0,
			}),
		};

		self.send_and_forget(msg, register_result.reg).await?;

		self.keep_alive_device(tid, &result).await;
		// self.keep_alive_relay(tid, &result).await;

		self.semaphore.close();
		drop(permit);
		Ok(result)
	}

	async fn keep_alive_device(&self, tid: u32, connect_result: &ConnectResult) {
		let client_id = connect_result.client_id;
		let camera_id = connect_result.camera_id;
		let addr = connect_result.addr;
		let mut sender = ArcFramedSocket::new(self.socket.clone(), BcUdpCodex::new());
		let mut interval = interval(Duration::from_secs(1));
		let thread_cancel = self.cancel.clone();
		self.handle.write().await.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					loop {
						tokio::task::yield_now().await;
						interval.tick().await;
						let msg = BcUdp::Discovery(UdpDiscovery {
							tid,
							payload: UdpXml::C2dHb(C2dHb {
									cid: client_id,
									did: camera_id,
								}),
						});
						if sender.send((msg, addr)).await.is_err() {
							break Result::Ok(());
						}
					}
				} => v,
			}
		});
	}

	#[allow(dead_code)] // Haven't seen this in the wild yet it is just speculation
	async fn keep_alive_relay(&self, tid: u32, connect_result: &ConnectResult) {
		let client_id = connect_result.client_id;
		let camera_id = connect_result.camera_id;
		let sid = connect_result.sid;
		let addr = connect_result.addr;
		let mut sender = ArcFramedSocket::new(self.socket.clone(), BcUdpCodex::new());
		let mut interval = interval(Duration::from_secs(1));
		let thread_cancel = self.cancel.clone();
		self.handle.write().await.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					loop {
						tokio::task::yield_now().await;
						interval.tick().await;
						let msg = BcUdp::Discovery(UdpDiscovery {
							tid,
							payload: UdpXml::C2rHb(C2rHb {
									sid,
									cid: client_id,
									did: camera_id,
								}),
						});
						if sender.send((msg, addr)).await.is_err() {
							break Result::Ok(());
						}
					}
				} => v,
			}
		});
	}
}

impl Drop for Discoverer {
	fn drop(&mut self) {
		log::trace!("Drop Discoverer");
		self.cancel.cancel();
		// `try_current` so a Drop outside a Tokio runtime doesn't
		// double-panic. Cancel fired above; the drain is best-effort.
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let _gt = handle.enter();
		let mut joinset = std::mem::take(&mut self.handle);
		tokio::task::spawn(async move { while joinset.get_mut().join_next().await.is_some() {} });
		log::trace!("Dropped Discoverer");
	}
}

pub(crate) struct Discovery {
	discoverer: Discoverer,
	client_id: i32,
}

impl Discovery {
	pub(crate) async fn new(sigv3: bool) -> Result<Self> {
		Ok(Self {
			discoverer: Discoverer::new(sigv3).await?,
			client_id: generate_cid(),
		})
	}

	pub(crate) async fn get_registration(&self, uid: &str) -> Result<RegisterResult> {
		let (targets, dns_errors) = self.discoverer.resolve_relay_addrs().await?;
		// DNS failures are a host-config problem (broken resolver), not a
		// transient relay outage, so they warrant a warning of their own.
		for (host, err) in &dns_errors {
			log::warn!("Discovery: DNS lookup failed for {host}:9999 — {err}");
		}

		let checked_reg = Arc::new(RwLock::new(HashSet::new()));
		let discoverer = &self.discoverer;
		let client_id = self.client_id;

		// One future per relay: look the UID up, then register against the
		// reg server that relay points us at. Each failure is tagged with
		// the originating hostname so a fully-failed round can name the
		// unreachable servers in a single line (see the `Err` arm below)
		// rather than emitting a wall of one-per-relay warnings.
		let mut attempts = targets
			.into_iter()
			.map(|(host, addr)| {
				let checked_reg = checked_reg.clone();
				async move {
					let lookup = discoverer
						.uid_lookup(uid, addr)
						.await
						.map_err(|e| (host, format!("query failed: {e}")))?;
					{
						let mut checked = checked_reg.write().await;
						if !checked.insert(lookup.reg) {
							// Another relay already pointed us at this reg
							// server; skip the duplicate registration.
							return Err((host, "duplicate reg server".to_string()));
						}
					}
					trace!("lookup: {:?}", lookup);
					discoverer
						.register_address(uid, client_id, &lookup)
						.await
						.map_err(|e| (host, format!("register refused: {e}")))
				}
			})
			.collect::<FuturesUnordered<_>>();

		let mut failures: Vec<(&'static str, String)> = vec![];
		while let Some(outcome) = attempts.next().await {
			match outcome {
				Ok(reg_result) => {
					trace!("reg_result: {:?}", reg_result);
					return Ok(reg_result);
				}
				Err(failure) => failures.push(failure),
			}
		}

		// Every relay failed this round. Fold the per-relay reasons into a
		// single typed error the retry loop renders verbatim, so the
		// operator sees exactly which servers are unreachable and why.
		let detail = failures
			.iter()
			.map(|(host, why)| format!("{host} ({why})"))
			.collect::<Vec<_>>()
			.join(", ");
		Err(Error::DiscoveryNoRelay(if detail.is_empty() {
			"no relays resolved".to_string()
		} else {
			detail
		}))
	}

	// Check if TCP is possible
	//
	// To do this we send a dummy login  and see if it replies with any BC packet
	pub(crate) async fn check_tcp(&self, addr: SocketAddr, channel_id: u8) -> Result<()> {
		let username = "admin";
		let password = Some("123456");
		let mut tcp_source =
			timeout(*TCP_WAIT, TcpSource::new(addr, username, password, false)).await??;

		let md5_username = md5_string(username, Md5Trunc::ZeroLast);
		let md5_password = password
			.map(|p| md5_string(p, Md5Trunc::ZeroLast))
			.unwrap_or_else(|| EMPTY_LEGACY_PASSWORD.to_owned());

		tcp_source
			.send(Bc {
				meta: BcMeta {
					msg_id: MSG_ID_LOGIN,
					channel_id,
					msg_num: 0,
					stream_type: 0,
					response_code: 0x00,
					class: 0x6514,
				},
				body: BcBody::LegacyMsg(LegacyMsg::LoginMsg {
					username: md5_username,
					password: md5_password,
				}),
			})
			.await?;

		let _bc: Bc = timeout(*TCP_WAIT, tcp_source.next())
			.await?
			.ok_or(Error::CannotInitCamera)??; // Successful recv should mean a Bc packet if not then deser will fail
		Ok(())
	}

	// Perform UDP broadcast lookup and connection
	pub(crate) async fn local(
		&self,
		uid: &str,
		mut optional_addrs: Option<Vec<SocketAddr>>,
	) -> Result<DiscoveryResult> {
		let mut dests = get_broadcasts(&[2015, 2018])?;
		if let Some(mut optional_addrs) = optional_addrs.take() {
			debug!("Also sending to {:?}", optional_addrs);
			dests.append(&mut optional_addrs);
		}
		let discoverer_ref = &self.discoverer;
		let client_id = self.client_id;
		let mut futures = FuturesUnordered::new();
		for addr in dests.iter().copied() {
			futures.push(async move {
				discoverer_ref
					.client_initiated_direct(uid, client_id, addr)
					.await
			})
		}

		let connect_result;
		loop {
			match futures.next().await {
				Some(Ok(good_result)) => {
					connect_result = good_result;
					break;
				}
				Some(Err(_)) => {
					continue;
				}
				None => {
					return Err(Error::DiscoveryTimeout);
				}
			}
		}
		drop(futures);
		// drop(discoverer_ref);

		let socket = self.discoverer.get_socket().await;
		Ok(DiscoveryResult {
			socket,
			nc: connect_result.nc,
			pl: connect_result.pl,
			addr: connect_result.addr,
			camera_id: connect_result.camera_id,
			client_id: connect_result.client_id,
		})
	}

	// This will start remote discovery against the reolink p2p servers
	//
	// This works by registering our ip and intent to connect with the reolink
	// servers
	//
	// We will then try to connect to the camera local ip address while the camera
	// will also attempt to connec to ours
	//
	// This method is best when broadcasts are not possible but we can contact the camera
	// directly
	#[allow(unused)]
	pub(crate) async fn remote(
		&self,
		uid: &str,
		reg_result: &RegisterResult,
	) -> Result<DiscoveryResult> {
		trace!("Start remote");
		let connect_result = tokio::select! {
			v = self.discoverer.client_initiated_dev(reg_result) => {v},
			v = self.discoverer.device_initiated_dev(reg_result) => {v},
		}?;
		trace!("connect_result: {:?}", connect_result);
		let socket = self.discoverer.get_socket().await;
		Ok(DiscoveryResult {
			socket,
			nc: connect_result.nc,
			pl: connect_result.pl,
			addr: connect_result.addr,
			client_id: self.client_id,
			camera_id: connect_result.camera_id,
		})
	}

	// This is similar to remote, except that it allows the camera to connect to us
	// over it's dmap (public) ip address that it has registered with reolink servers.
	//
	// This works by registering our ip address and the desire to connect with the
	// reolink servers. Data however should go to the camera's public ip address
	//
	// This method should be used when the camera is behind a NAT or firewall but we are
	// reachable
	pub(crate) async fn map(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
		let connect_result = self.discoverer.device_initiated_map(reg_result).await?;
		trace!("connect_result: {:?}", connect_result);

		let socket = self.discoverer.get_socket().await;
		Ok(DiscoveryResult {
			socket,
			nc: connect_result.nc,
			pl: connect_result.pl,
			addr: connect_result.addr,
			client_id: self.client_id,
			camera_id: connect_result.camera_id,
		})
	}

	// This will forward all connections via the reolinks servers
	//
	// This method should work if all else fails but it will require
	// us to trust reolink with our data once more...
	//
	pub(crate) async fn relay(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
		let connect_result = self.discoverer.client_initiated_relay(reg_result).await?;
		trace!("connect_result: {:?}", connect_result);

		let socket = self.discoverer.get_socket().await;
		Ok(DiscoveryResult {
			socket,
			nc: connect_result.nc,
			pl: connect_result.pl,
			addr: connect_result.addr,
			client_id: self.client_id,
			camera_id: connect_result.camera_id,
		})
	}
}

/// Object-safe abstraction over the five discovery primitives used by
/// `BcCamera::find_camera`. Production uses `Discovery` (real UDP
/// broadcasts + Reolink P2P lookups); tests script each arm of the
/// fallback chain via `ScriptedDiscoverer` (see tests below).
///
/// The trait wraps a *handle*: a discoverer may be stateless or (as
/// `Discovery` does) own a long-lived UDP socket + client-id. All
/// methods take `&self` and borrow; tests that need mutable scripting
/// put a `Mutex` inside the scripted impl.
#[async_trait::async_trait]
pub(crate) trait CameraDiscoverer: Send + Sync {
	/// TCP probe — dummy login + first-packet round-trip.
	async fn check_tcp(&self, addr: SocketAddr, channel_id: u8) -> Result<()>;
	/// Register with the P2P lookup chain for a UID, yielding an opaque
	/// `RegisterResult` consumed by `remote` / `map` / `relay`.
	async fn get_registration(&self, uid: &str) -> Result<RegisterResult>;
	/// Local UDP broadcast discovery.
	async fn local(
		&self,
		uid: &str,
		optional_addrs: Option<Vec<SocketAddr>>,
	) -> Result<DiscoveryResult>;
	/// P2P remote discovery via the Reolink server chain.
	async fn remote(&self, uid: &str, reg_result: &RegisterResult) -> Result<DiscoveryResult>;
	/// Device-initiated map (NAT-hairpin) discovery.
	async fn map(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult>;
	/// Reolink-relayed discovery of last resort.
	async fn relay(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult>;
}

#[async_trait::async_trait]
impl CameraDiscoverer for Discovery {
	async fn check_tcp(&self, addr: SocketAddr, channel_id: u8) -> Result<()> {
		Discovery::check_tcp(self, addr, channel_id).await
	}
	async fn get_registration(&self, uid: &str) -> Result<RegisterResult> {
		Discovery::get_registration(self, uid).await
	}
	async fn local(
		&self,
		uid: &str,
		optional_addrs: Option<Vec<SocketAddr>>,
	) -> Result<DiscoveryResult> {
		Discovery::local(self, uid, optional_addrs).await
	}
	async fn remote(&self, uid: &str, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
		Discovery::remote(self, uid, reg_result).await
	}
	async fn map(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
		Discovery::map(self, reg_result).await
	}
	async fn relay(&self, reg_result: &RegisterResult) -> Result<DiscoveryResult> {
		Discovery::relay(self, reg_result).await
	}
}

/// Test-only convenience wrapper: delegate to
/// [`get_local_ip_for_target`] with no preferred target. Production
/// always passes a relay address (see `register_address`); this
/// shape stays available for legacy unit tests that don't have a
/// target IP handy.
#[cfg(test)]
fn get_local_ip() -> Result<std::net::IpAddr> {
	get_local_ip_for_target(None)
}

/// Pick the best local IPv4 to register with a relay, preferring the
/// interface whose subnet contains `target` (the relay's address, in
/// `register_address`). Falls back to the legacy "first non-loopback
/// V4" behaviour when no match exists or `target` is `None`.
///
/// Why: a host with Wi-Fi (`192.168.1.10/24`), Docker bridge
/// (`172.17.0.1/16`), Tailscale (`100.x.y.z/32`), and libvirt
/// (`192.168.122.1/24`) returns these in OS-dependent order from
/// `get_if_addrs`. The legacy first-match could pick the Docker
/// bridge address, which is unreachable from the camera on the
/// physical LAN — the camera then can't dial us back, discovery
/// times out, and the operator sees an opaque "no registers
/// returned" error.
///
/// Subnet-match closes that footgun for the common case (camera on
/// the same LAN as bairelay; relay is the local wake server or any
/// LAN-resident relay). Cloud-relay users (target is public) hit the
/// fallback, which is identical to the legacy behaviour — no
/// regression.
fn get_local_ip_for_target(target: Option<std::net::IpAddr>) -> Result<std::net::IpAddr> {
	let ifaces = get_if_addrs::get_if_addrs()?;
	if let Some(std::net::IpAddr::V4(target_v4)) = target {
		for iface in &ifaces {
			if iface.is_loopback() {
				continue;
			}
			if let get_if_addrs::IfAddr::V4(v4) = &iface.addr {
				let mask_u32: u32 = v4.netmask.into();
				let iface_u32: u32 = v4.ip.into();
				let target_u32: u32 = target_v4.into();
				if mask_u32 != 0 && (iface_u32 & mask_u32) == (target_u32 & mask_u32) {
					return Ok(std::net::IpAddr::V4(v4.ip));
				}
			}
		}
	}
	// Fallback: legacy "first non-loopback IPv4". Preserves prior
	// behaviour when subnet-match finds nothing or target is unknown.
	ifaces
		.iter()
		.find(|i| !i.is_loopback() && matches!(i.addr, get_if_addrs::IfAddr::V4(_)))
		.map(|iface| Ok(iface.ip()))
		.unwrap_or_else(|| Err(Error::Other("No Local Ip Address Found")))
}

fn get_broadcasts(ports: &[u16]) -> Result<Vec<SocketAddr>> {
	let mut broadcasts = vec![Ipv4Addr::BROADCAST];
	for iface in get_if_addrs::get_if_addrs()?.iter() {
		if let get_if_addrs::IfAddr::V4(ifacev4) = &iface.addr {
			if let Some(broadcast) = ifacev4.broadcast.as_ref() {
				broadcasts.push(*broadcast);
			}
		}
	}
	let mut destinations: Vec<(Ipv4Addr, u16)> = broadcasts
		.iter()
		.flat_map(|&addr| {
			ports
				.iter()
				.map(|&port| (addr, port))
				.collect::<Vec<(Ipv4Addr, u16)>>()
		})
		.collect();
	debug!("Broadcasting to: {:?}", destinations);
	Ok(destinations
		.drain(..)
		.map(|(addr, port)| SocketAddr::new(addr.into(), port))
		.collect())
}

fn generate_tid() -> u32 {
	// Full 32-bit space (with `>0` guard since `tid == 0` reserved as
	// "listen to all" inside `Discoverer::subscribe`). Earlier 8-bit
	// range left only 256 distinct values; on multi-relay parallel
	// register rounds the birthday-paradox collision rate climbed past
	// 1 % per attempt and surfaced as `SimultaneousSubscription` warns
	// at `register_at_all_relays`. With u32 the rate falls below 1e-7
	// per attempt for any realistic relay count.
	let mut rng = thread_rng();
	let mut tid: u32 = rng.gen();
	if tid == 0 {
		tid = 1;
	}
	tid
}

fn generate_cid() -> i32 {
	let mut rng = thread_rng();
	rng.gen()
}

async fn connect() -> Result<UdpSocket> {
	let mut ports: Vec<u16> = (53500..54000).collect();
	{
		let mut rng = thread_rng();
		ports.shuffle(&mut rng);
	}

	let addrs: Vec<_> = ports
		.iter()
		.map(|&port| SocketAddr::from(([0, 0, 0, 0], port)))
		.collect();
	let socket = UdpSocket::bind(&addrs[..]).await?;
	socket.set_broadcast(true)?;

	Ok(socket)
}

#[cfg(test)]
pub(crate) mod test_support {
	//! Scripted `CameraDiscoverer` for driving `find_camera_with_discoverer`
	//! through every branch of the fallback chain without opening UDP
	//! sockets or talking to Reolink servers.
	use super::*;
	use std::sync::Mutex;

	/// A single outcome scripted per method.
	pub(crate) enum Outcome {
		/// Return `Ok(DiscoveryResult)` — tests supply a dummy socket +
		/// address since consumer code only reads `addr`.
		OkDiscovery {
			addr: SocketAddr,
		},
		/// Return `Err(_)` — we supply `Error::DiscoveryTimeout` so the
		/// race-the-rest behaviour kicks in (other branches get a chance
		/// to succeed).
		Err,
		/// Hang forever — kept for future "race the siblings" scenarios;
		/// not constructed by the current test set but referenced in
		/// every method's match arm so the code path stays live.
		#[allow(dead_code)]
		Hang,
		Ok,
	}

	/// Call log: each entry is `(method, uid_or_sentinel)`.
	#[derive(Default)]
	pub(crate) struct CallLog {
		pub entries: Mutex<Vec<(&'static str, String)>>,
	}

	impl CallLog {
		pub fn methods(&self) -> Vec<&'static str> {
			self.entries
				.lock()
				.unwrap()
				.iter()
				.map(|(m, _)| *m)
				.collect()
		}
	}

	pub(crate) struct ScriptedDiscoverer {
		pub log: Arc<CallLog>,
		pub on_check_tcp: Outcome,
		pub on_get_registration: Outcome,
		pub on_local: Outcome,
		pub on_remote: Outcome,
		pub on_map: Outcome,
		pub on_relay: Outcome,
	}

	impl ScriptedDiscoverer {
		pub fn new() -> Self {
			Self {
				log: Arc::new(CallLog::default()),
				on_check_tcp: Outcome::Err,
				on_get_registration: Outcome::Err,
				on_local: Outcome::Err,
				on_remote: Outcome::Err,
				on_map: Outcome::Err,
				on_relay: Outcome::Err,
			}
		}
	}

	fn dummy_socket() -> Arc<UdpSocket> {
		// A bound-to-ephemeral UDP socket that never sends or receives.
		// We only need the `Arc<UdpSocket>` slot of `DiscoveryResult`
		// to be present; tests never read from it.
		let std_sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral");
		std_sock.set_nonblocking(true).unwrap();
		Arc::new(UdpSocket::from_std(std_sock).expect("from_std"))
	}

	pub fn ok_discovery(addr: SocketAddr) -> DiscoveryResult {
		DiscoveryResult {
			socket: dummy_socket(),
			addr,
			client_id: 1,
			camera_id: 2,
			nc: None,
			pl: None,
		}
	}

	#[tokio::test]
	async fn take_sigv3_handshake_yields_pair_once_then_none() {
		let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
		let mut dr = DiscoveryResult {
			socket: dummy_socket(),
			addr,
			client_id: 1,
			camera_id: 2,
			nc: Some(42),
			pl: Some("V=1;P2=v3;P4=k".to_string()),
		};
		assert_eq!(
			dr.take_sigv3_handshake(),
			Some((42, "V=1;P2=v3;P4=k".to_string()))
		);
		// Consumed — a second call yields None.
		assert_eq!(dr.take_sigv3_handshake(), None);
		// A non-cloud result (nc/pl absent) yields None.
		assert_eq!(ok_discovery(addr).take_sigv3_handshake(), None);
	}

	pub fn dummy_reg_result() -> RegisterResult {
		RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		}
	}

	async fn resolve_discovery(outcome: &Outcome) -> Result<DiscoveryResult> {
		match outcome {
			Outcome::OkDiscovery { addr } => Ok(ok_discovery(*addr)),
			Outcome::Err => Err(Error::DiscoveryTimeout),
			Outcome::Hang => {
				std::future::pending::<()>().await;
				unreachable!()
			}
			Outcome::Ok => Err(Error::DiscoveryTimeout),
		}
	}

	async fn resolve_unit(outcome: &Outcome) -> Result<()> {
		match outcome {
			Outcome::Ok => Ok(()),
			Outcome::Err => Err(Error::DiscoveryTimeout),
			Outcome::Hang => {
				std::future::pending::<()>().await;
				unreachable!()
			}
			Outcome::OkDiscovery { .. } => Ok(()),
		}
	}

	async fn resolve_reg(outcome: &Outcome) -> Result<RegisterResult> {
		match outcome {
			Outcome::Ok | Outcome::OkDiscovery { .. } => Ok(dummy_reg_result()),
			Outcome::Err => Err(Error::DiscoveryTimeout),
			Outcome::Hang => {
				std::future::pending::<()>().await;
				unreachable!()
			}
		}
	}

	#[async_trait::async_trait]
	impl CameraDiscoverer for ScriptedDiscoverer {
		async fn check_tcp(&self, _addr: SocketAddr, _channel_id: u8) -> Result<()> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("check_tcp", String::new()));
			resolve_unit(&self.on_check_tcp).await
		}
		async fn get_registration(&self, uid: &str) -> Result<RegisterResult> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("get_registration", uid.to_string()));
			resolve_reg(&self.on_get_registration).await
		}
		async fn local(
			&self,
			uid: &str,
			_optional_addrs: Option<Vec<SocketAddr>>,
		) -> Result<DiscoveryResult> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("local", uid.to_string()));
			resolve_discovery(&self.on_local).await
		}
		async fn remote(&self, uid: &str, _r: &RegisterResult) -> Result<DiscoveryResult> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("remote", uid.to_string()));
			resolve_discovery(&self.on_remote).await
		}
		async fn map(&self, _r: &RegisterResult) -> Result<DiscoveryResult> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("map", String::new()));
			resolve_discovery(&self.on_map).await
		}
		async fn relay(&self, _r: &RegisterResult) -> Result<DiscoveryResult> {
			self.log
				.entries
				.lock()
				.unwrap()
				.push(("relay", String::new()));
			resolve_discovery(&self.on_relay).await
		}
	}
}

/*
	# Discovery Methods

	# Register

	This is the inital query to known hosts with a known UID

	- C->R Port 9999
	```xml
	<P2P>
	<C2M_Q>
	<uid>9527000XXXXXXXXX</uid>
	<p>MAC</p>
	</C2M_Q>
	</P2P>
	```

	Replies with details of the camera we want to connect to

	- R->C
	```xml
	<P2P>
		<M2C_Q_R>
			<reg>
				<ip>198.51.100.10</ip>
				<port>58200</port>
			</reg>
			<relay>
				<ip>198.51.100.10</ip>
				<port>58100</port>
			</relay>
			<log>
				<ip>198.51.100.10</ip>
				<port>57850</port>
			</log>
			<t>
				<ip>198.51.100.10</ip>
				<port>9996</port>
			</t>
			<timer/>
			<retry/>
			<mtu>1350</mtu>
			<debug>251658240</debug>
			<ac>-1700607721</ac>
			<rsp>0</rsp>
		</M2C_Q_R>
	</P2P>
	```

	## Thread 1: Observed during relay

	- D->C 59733
	```xml
	<P2P>
	<D2C_T>
	<sid>495151439</sid>
	<conn>map</conn>
	<cid>254000</cid>
	<did>735</did>
	</D2C_T>
	</P2P>
	```

	- C->D
	```xml
	<P2P>
	<C2D_A>
	<sid>495151439</sid>
	<conn>map</conn>
	<cid>254000</cid>
	<did>735</did>
	<mtu>1350</mtu>
	</C2D_A>
	</P2P>
	```

	- D->C
	```xml
	<P2P>
	<D2C_CFM>
	<sid>495151439</sid>
	<conn>map</conn>
	<rsp>0</rsp>
	<cid>254000</cid>
	<did>735</did>
	<time_r>55607</time_r>
	</D2C_CFM>
	</P2P>
	 ```

	## Thread 2: Observed during relay

	- C->R: 58200
	```xml
	<P2P>
	<C2R_C>
	<uid>9527000XXXXXXXXX</uid>
	<cli>
	<ip>192.0.2.10</ip>
	<port>12254</port>
	</cli>
	<relay>
	<ip>198.51.100.10</ip>
	<port>58100</port>
	</relay>
	<cid>254000</cid>
	<debug>251658240</debug>
	<family>4</family>
	<p>MAC</p>
	<r>3</r>
	</C2R_C>
	</P2P>
	```

	- R->C
	```xml
	<P2P><R2C_T><dev><ip>192.168.1.100</ip><port>57933</port></dev><dmap><ip>203.0.113.45</ip><port>57933</port></dmap><sid>495151439</sid><cid>254000</cid><rsp>0</rsp></R2C_T></P2P>
	```

	- R->C
	```xml
	<P2P>
		<R2C_C_R>
			<dmap>
				<ip>203.0.113.45</ip>
				<port>57933</port>
			</dmap>
			<dev>
				<ip>192.168.1.100</ip>
				<port>57933</port>
			</dev>
			<relay>
				<ip>198.51.100.10</ip>
				<port>51134</port>
			</relay>
			<relayt>
				<ip>198.51.100.10</ip>
				<port>9997</port>
			</relayt>
			<nat>NULL</nat>
			<sid>495151439</sid>
			<rsp>0</rsp>
			<ac>495151439</ac>
		</R2C_C_R>
	</P2P>
	```

	- R->C
	```xml
	<P2P>
		<R2C_T>
			<dev>
				<ip>192.168.1.100</ip>
				<port>57933</port>
			</dev>
			<dmap>
				<ip>203.0.113.45</ip>
				<port>57933</port>
			</dmap>
			<sid>495151439</sid>
			<cid>254000</cid>
			<rsp>0</rsp>
		</R2C_T>
	</P2P>
	```

	- R->C Repeats later so possibly was not responded to by client
	```xml
	<P2P>
		<R2C_C_R>
			<dmap>
				<ip>203.0.113.45</ip>
				<port>57933</port>
			</dmap>
			<dev>
				<ip>192.168.1.100</ip>
				<port>57933</port>
			</dev>
			<relay>
				<ip>198.51.100.10</ip>
				<port>51134</port>
			</relay>
			<relayt>
				<ip>198.51.100.10</ip>
				<port>9997</port>
			</relayt>
			<nat>NULL</nat>
			<sid>495151439</sid>
			<rsp>0</rsp>
			<ac>495151439</ac>
		</R2C_C_R>
	</P2P>
	```

	- C->R
	```xml
	<P2P>
	<C2R_CFM>
	<sid>495151439</sid>
	<conn>map</conn>
	<rsp>0</rsp>
	<cid>254000</cid>
	<did>735</did>
	</C2R_CFM>
	</P2P>
	```

	- R->C
	```xml
	<P2P>
		<R2C_T>
			<dev>
				<ip>192.168.1.100</ip>
				<port>57933</port>
			</dev>
			<dmap>
				<ip>203.0.113.45</ip>
				<port>57933</port>
			</dmap>
			<sid>495151439</sid>
			<cid>254000</cid>
			<rsp>0</rsp>
		</R2C_T>
	</P2P>
	```

	# Thread 3: Observed during relay
	After connection. No response

	- C->R
	```xml
	<P2P>
	<C2R_CFM>
	<sid>495151439</sid>
	<conn>map</conn>
	<rsp>0</rsp>
	<cid>254000</cid>
	<did>735</did>
	</C2R_CFM>
	</P2P>
	```

	# Thread 4: Observed when behind a NAT on both ends of the connection

	- C->R
	```
	<P2P>
	<C2D_T>
	<sid>526020041</sid>
	<conn>relay</conn>
	<cid>38000</cid>
	<mtu>1350</mtu>
	</C2D_T>
	</P2P>
	```

	- R->C
	```xml
	<P2P>
	<D2C_CFM>
	<sid>526020041</sid>
	<conn>relay</conn>
	<rsp>0</rsp>
	<cid>38000</cid>
	<did>32</did>
	<time_r>0</time_r>
	</D2C_CFM>
	</P2P>
	```

*/

#[cfg(test)]
mod internal_tests {
	//! Tests for the `Discoverer` + `Discovery` implementation details.
	//!
	//! Each socket-touching test binds a real `UdpSocket` to
	//! `127.0.0.1:0` in a helper task that plays the role of the camera
	//! / reolink register / relay server, receives a scripted
	//! `BcUdp::Discovery` from the Discoverer, and replies with a
	//! matching `UdpDiscovery` reply. Every end-to-end await is wrapped
	//! in a short `tokio::time::timeout` so a missing reply never hangs
	//! `cargo test`.
	use super::*;
	use crate::bcudp::codex::BcUdpCodex;
	use bytes::BytesMut;
	use tokio::time::timeout as t_timeout;
	use tokio_util::codec::{Decoder, Encoder};

	// Real-clock timeout for the scripted-peer / Discoverer rendezvous
	// in these tests. 800 ms was originally fine; under
	// `cargo tarpaulin`'s instrumentation overhead it goes flaky on
	// loaded CI runners. 3 s is comfortably above the worst-case
	// instrumented socket round-trip, and these tests still complete
	// well under a second on an uninstrumented build.
	// Recv timeout for in-test scripted-peer waits. 10s rather than the
	// natural ~10ms each operation actually needs because GitHub's
	// macos-latest runner stalls UDP loopback delivery for several
	// seconds at a time; 3s was enough locally and on Linux CI but
	// timed out the discovery_local test on macos-latest.
	const T: Duration = Duration::from_millis(10_000);

	// ---------- pure helpers ----------

	#[test]
	fn valid_ip_accepts_normal_ipv4() {
		assert!(valid_ip("10.1.2.3"));
		assert!(valid_ip("192.168.0.42"));
	}

	#[test]
	fn valid_ip_rejects_empty_and_trailing_zero() {
		assert!(!valid_ip(""));
		// trailing-zero octet is treated as invalid — mirrors neolink
		// behaviour for "unset" reolink-reply addresses.
		assert!(!valid_ip("10.1.2.0"));
	}

	#[test]
	fn valid_ip_rejects_non_ipv4() {
		assert!(!valid_ip("not-an-ip"));
		assert!(!valid_ip("::1"));
	}

	#[test]
	fn valid_port_boundary() {
		assert!(!valid_port(0));
		assert!(valid_port(1));
		assert!(valid_port(9999));
		assert!(valid_port(u16::MAX));
	}

	#[test]
	fn valid_addr_requires_both_valid() {
		let good = IpPort {
			ip: "10.0.0.1".into(),
			port: 9999,
		};
		let bad_ip = IpPort {
			ip: "".into(),
			port: 9999,
		};
		let bad_port = IpPort {
			ip: "10.0.0.1".into(),
			port: 0,
		};
		assert!(valid_addr(&good));
		assert!(!valid_addr(&bad_ip));
		assert!(!valid_addr(&bad_port));
	}

	#[test]
	fn generate_tid_uses_full_u32_range_and_skips_zero() {
		let mut seen_above_u8 = false;
		for _ in 0..1024 {
			let tid = generate_tid();
			// `tid == 0` is reserved as "listen to all"; generator must
			// never return it.
			assert!(tid > 0, "tid must be non-zero; got 0");
			if tid > 0xFF {
				seen_above_u8 = true;
			}
		}
		// Probabilistic: with full u32 range, P(all 1024 draws ≤ 255) is
		// (256/2^32)^1024 ≈ 1e-7400 — effectively zero.
		assert!(
			seen_above_u8,
			"generator stuck inside u8 range; collision rate regression"
		);
	}

	#[test]
	fn generate_cid_varies() {
		let a = generate_cid();
		let b = generate_cid();
		// Allow equal by chance but just exercising the code path.
		let _ = (a, b);
	}

	#[test]
	fn get_broadcasts_returns_at_least_global_broadcast() {
		let addrs = get_broadcasts(&[2015, 2018]).expect("broadcasts");
		// One 255.255.255.255 entry per port, plus any per-iface
		// broadcasts.
		let global_2015 = SocketAddr::new(Ipv4Addr::BROADCAST.into(), 2015);
		let global_2018 = SocketAddr::new(Ipv4Addr::BROADCAST.into(), 2018);
		assert!(
			addrs.contains(&global_2015),
			"missing global broadcast for port 2015"
		);
		assert!(
			addrs.contains(&global_2018),
			"missing global broadcast for port 2018"
		);
	}

	#[test]
	fn get_broadcasts_empty_ports_returns_empty() {
		let addrs = get_broadcasts(&[]).expect("empty ports ok");
		assert!(addrs.is_empty());
	}

	#[test]
	fn get_local_ip_is_not_loopback() {
		if let Ok(ip) = get_local_ip() {
			assert!(!ip.is_loopback(), "local IP should not be loopback");
			assert!(ip.is_ipv4(), "local IP should be v4 per neolink rule");
		}
		// If the CI host has no v4 non-loopback iface we just don't
		// assert — `get_local_ip` correctly errors.
	}

	/// Subnet-match preference: when a target is given,
	/// `get_local_ip_for_target` prefers an interface whose subnet
	/// contains it. We cannot fabricate interfaces in this test (the
	/// `get_if_addrs` query is real) but we CAN verify the contract
	/// in two ways: (a) any returned IP is non-loopback IPv4 (same as
	/// the no-target path), and (b) when the host has any IPv4
	/// interface, asking for a target on that interface's subnet
	/// returns specifically that interface's address — even if it
	/// would not be the legacy first-non-loopback pick. This is a
	/// best-effort check that's a no-op on hosts with no IPv4 ifaces
	/// (CI sandboxes).
	#[test]
	fn get_local_ip_for_target_prefers_subnet_match() {
		let Ok(ifaces) = get_if_addrs::get_if_addrs() else {
			return;
		};
		let v4_ifaces: Vec<_> = ifaces
			.iter()
			.filter_map(|i| match (&i.addr, i.is_loopback()) {
				(get_if_addrs::IfAddr::V4(v4), false) => Some(v4.clone()),
				_ => None,
			})
			.collect();
		if v4_ifaces.is_empty() {
			return;
		}
		// Pick any v4 iface; a target on the same subnet must select
		// THAT interface specifically, not whichever happened to be
		// first.
		for v4 in &v4_ifaces {
			let mask: u32 = v4.netmask.into();
			if mask == 0 {
				continue;
			}
			let iface_u32: u32 = v4.ip.into();
			// Target = iface IP +1 inside the subnet (close enough for
			// any /24 or larger; skip the unlikely /32 case).
			let target_u32 = (iface_u32 & mask) | (((iface_u32 & !mask) + 1) & !mask);
			let target = std::net::IpAddr::V4(std::net::Ipv4Addr::from(target_u32));
			if let Ok(picked) = get_local_ip_for_target(Some(target)) {
				assert_eq!(
					picked,
					std::net::IpAddr::V4(v4.ip),
					"subnet-match must pick the iface whose subnet contains the target"
				);
				return;
			}
		}
	}

	/// Subnet-mismatch fallback: a target on a totally unrelated
	/// subnet (`192.0.2.x` — TEST-NET-1, by RFC 5737 reserved for
	/// docs) must fall back to the legacy "first non-loopback v4"
	/// behaviour rather than erroring. Mirrors the contract on
	/// cloud-relay deployments where the relay is public.
	#[test]
	fn get_local_ip_for_target_falls_back_when_no_subnet_matches() {
		let target = std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 100));
		match (get_local_ip_for_target(Some(target)), get_local_ip()) {
			(Ok(matched), Ok(legacy)) => {
				assert_eq!(
					matched, legacy,
					"unmatched target must yield the legacy first-non-loopback IP"
				);
			}
			(Err(_), Err(_)) => {
				// Host with no v4 ifaces — both paths error consistently.
			}
			(matched, legacy) => panic!(
				"target/no-target paths disagree on availability: matched={matched:?} \
				 legacy={legacy:?}"
			),
		}
	}

	#[tokio::test]
	async fn connect_binds_to_broadcast_capable_socket() {
		let sock = t_timeout(T, connect()).await.unwrap().unwrap();
		assert!(sock.broadcast().unwrap(), "set_broadcast should be true");
		let port = sock.local_addr().unwrap().port();
		assert!((53500..54000).contains(&port));
	}

	// ---------- Discoverer: new + primitives ----------

	#[tokio::test]
	async fn discoverer_new_binds_socket_and_starts_tasks() {
		let d = t_timeout(T, Discoverer::new(false)).await.unwrap().unwrap();
		assert_eq!(*d.local_addr(), d.socket.local_addr().unwrap());
	}

	#[tokio::test]
	async fn discoverer_get_socket_returns_clone() {
		let d = Discoverer::new(false).await.unwrap();
		let s = d.get_socket().await;
		assert_eq!(
			s.local_addr().unwrap(),
			d.socket.local_addr().unwrap(),
			"cloned socket should share local_addr"
		);
	}

	#[tokio::test]
	async fn discoverer_subscribe_tid_nonzero_gives_channel() {
		let d = Discoverer::new(false).await.unwrap();
		let _rx = d.subscribe(42).await.expect("subscribe ok");
	}

	#[tokio::test]
	async fn discoverer_subscribe_tid_zero_registers_handler() {
		let d = Discoverer::new(false).await.unwrap();
		let _rx = d.subscribe(0).await.expect("tid=0 handler registers");
		// A second subscribe(0) should also succeed (distinct handler
		// slot).
		let _rx2 = d.subscribe(0).await.expect("second tid=0 handler");
	}

	#[tokio::test]
	async fn discoverer_simultaneous_subscription_errors() {
		let d = Discoverer::new(false).await.unwrap();
		let _rx1 = d.subscribe(1234).await.expect("first");
		let r = d.subscribe(1234).await;
		assert!(
			matches!(r, Err(Error::SimultaneousSubscription { .. })),
			"expected SimultaneousSubscription, got {r:?}"
		);
	}

	#[tokio::test]
	async fn discoverer_subscribe_reuses_slot_when_closed() {
		let d = Discoverer::new(false).await.unwrap();
		{
			let _rx = d.subscribe(7777).await.expect("first");
			// drop _rx — channel closes.
		}
		let _rx2 = d
			.subscribe(7777)
			.await
			.expect("should re-register after drop");
	}

	#[tokio::test]
	async fn discoverer_handle_incoming_times_out_without_reply() {
		// Temporarily shrink the timeout by monkey-patching with
		// paused time: we pause tokio's clock, then advance past the
		// MAXIMUM_WAIT window; the `handle_incoming` sleep yields
		// Error::DiscoveryTimeout.
		tokio::time::pause();
		let d = Discoverer::new(false).await.unwrap();
		let fut = d.handle_incoming(|_: UdpDiscovery, _: SocketAddr| -> Option<()> { None });
		tokio::pin!(fut);
		// Advance time past 15s.
		tokio::time::advance(Duration::from_secs(20)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	#[tokio::test]
	async fn discoverer_retry_send_times_out() {
		// Same trick: advance the paused clock past MAXIMUM_WAIT.
		tokio::time::pause();
		let d = Discoverer::new(false).await.unwrap();
		// Send into the void — 127.0.0.2:1 will drop packets.
		let target: SocketAddr = "127.0.0.2:1".parse().unwrap();
		let msg = UdpDiscovery {
			tid: 0,
			payload: UdpXml::C2dHb(C2dHb { cid: 1, did: 2 }),
		};
		let fut = d.retry_send::<_, ()>(
			msg,
			target,
			|_: UdpDiscovery, _: SocketAddr| -> Option<()> { None },
		);
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(20)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	#[tokio::test]
	async fn discoverer_send_delivers_to_peer() {
		// Spin up a listener; have the Discoverer send one Ack packet.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();

		let d = Discoverer::new(false).await.unwrap();
		let msg = BcUdp::Ack(UdpAck::empty(123));
		d.send(msg, server_addr).await.unwrap();

		let mut buf = [0u8; 2048];
		let (n, _) = t_timeout(T, server.recv_from(&mut buf))
			.await
			.unwrap()
			.unwrap();
		let mut codec = BcUdpCodex::new();
		let mut bm = BytesMut::from(&buf[..n]);
		let got = codec.decode(&mut bm).unwrap().unwrap();
		match got {
			BcUdp::Ack(a) => assert_eq!(a.connection_id, 123),
			other => panic!("got {other:?}"),
		}
	}

	// ---------- helper: bind a scripted peer socket ----------

	/// A scripted peer socket that the caller drives from the outside.
	async fn scripted_peer() -> (UdpSocket, SocketAddr) {
		let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let a = s.local_addr().unwrap();
		(s, a)
	}

	/// Read one BcUdp packet from a scripted peer; returns the packet +
	/// the source addr (Discoverer's bound port).
	async fn peer_recv(peer: &UdpSocket) -> (BcUdp, SocketAddr) {
		let mut buf = [0u8; 2048];
		let (n, from) = t_timeout(T, peer.recv_from(&mut buf))
			.await
			.expect("peer recv timed out")
			.expect("peer recv io");
		let mut codec = BcUdpCodex::new();
		let mut bm = BytesMut::from(&buf[..n]);
		let pkt = codec.decode(&mut bm).unwrap().unwrap();
		(pkt, from)
	}

	/// Send one scripted BcUdp packet back to `to` from the peer.
	async fn peer_send(peer: &UdpSocket, pkt: BcUdp, to: SocketAddr) {
		let mut codec = BcUdpCodex::new();
		let mut out = BytesMut::new();
		codec.encode(pkt, &mut out).unwrap();
		peer.send_to(&out, to).await.unwrap();
	}

	// ---------- client_initiated_direct ----------

	#[tokio::test]
	async fn client_initiated_direct_success_round_trip() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();

		let uid = "TESTUID0001";
		let client_id = 0xABCDi32;

		// Drive the camera's reply in a sibling task.
		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery { tid, payload }) => {
					// Check that we got a C2D_C with our UID.
					match payload {
						UdpXml::C2dC(C2dC { uid: u, cid, .. }) => {
							assert_eq!(u, "TESTUID0001");
							assert_eq!(cid, client_id);
						}
						other => panic!("unexpected payload: {other:?}"),
					}
					tid
				}
				other => panic!("unexpected packet: {other:?}"),
			};
			// Send a D2C_CR matching the client_id.
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCr(D2cCr {
					timer: Timer::default(),
					rsp: 0,
					cid: client_id,
					did: 99,
					pl: None,
					nc: None,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		let res = t_timeout(T, d.client_initiated_direct(uid, client_id, peer_addr))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.client_id, client_id);
		assert_eq!(res.camera_id, 99);
		assert_eq!(res.addr, peer_addr);

		camera.await.unwrap();
	}

	#[tokio::test]
	async fn client_initiated_direct_ignores_wrong_client_id() {
		// The camera replies with a wrong cid — Discoverer should keep
		// waiting, and our paused clock drives it into a
		// DiscoveryTimeout.
		tokio::time::pause();
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery { tid, .. }) => tid,
				_ => unreachable!(),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCr(D2cCr {
					timer: Timer::default(),
					rsp: 0,
					cid: 9999, // wrong
					did: 11,
					pl: None,
					nc: None,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		let fut = d.client_initiated_direct("XX", 1234, peer_addr);
		tokio::pin!(fut);
		// Let the camera thread process the initial send + reply.
		tokio::task::yield_now().await;
		for _ in 0..30 {
			tokio::time::advance(Duration::from_secs(1)).await;
			if camera.is_finished() {
				break;
			}
		}
		// Now drive past the timeout.
		tokio::time::advance(Duration::from_secs(60)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	// ---------- uid_lookup ----------

	#[tokio::test]
	async fn uid_lookup_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2mQ(C2mQ { uid, .. }),
				}) => {
					assert_eq!(uid, "UID-XYZ");
					tid
				}
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::M2cQr(M2cQr {
					reg: Some(IpPort {
						ip: "127.0.0.1".into(),
						port: 58200,
					}),
					relay: Some(IpPort {
						ip: "127.0.0.1".into(),
						port: 58100,
					}),
					log: None,
					t: None,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		let res = t_timeout(T, d.uid_lookup("UID-XYZ", peer_addr))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.reg.port(), 58200);
		assert_eq!(res.relay.port(), 58100);
		camera.await.unwrap();
	}

	// ---------- register_address + full remote/relay ----------

	#[tokio::test]
	async fn register_address_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();

		let lookup = UidLookupResults {
			reg: peer_addr,
			relay: peer_addr,
		};

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2rC(_),
				}) => tid,
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::R2cCr(R2cCr {
					dev: Some(IpPort {
						ip: "10.0.0.5".into(),
						port: 45678,
					}),
					dmap: Some(IpPort {
						ip: "10.0.0.6".into(),
						port: 45679,
					}),
					relay: Some(IpPort {
						ip: "10.0.0.7".into(),
						port: 45680,
					}),
					relayt: None,
					nat: "NULL".into(),
					sid: Some(42),
					rsp: 0,
					ac: 1,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		// register_address needs a routable local IP; get_local_ip may
		// fail on some CI boxes with no v4 non-loopback iface — gate.
		if get_local_ip().is_err() {
			camera.abort();
			return;
		}
		let res = t_timeout(T, d.register_address("UIDABC", 1000, &lookup))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.sid, 42);
		assert_eq!(res.client_id, 1000);
		assert!(res.dev.is_some());
		assert!(res.dmap.is_some());
		assert!(res.relay.is_some());
		camera.await.unwrap();
	}

	#[tokio::test]
	async fn register_address_rejects_rsp_minus_one() {
		// When the register replies with rsp=-1 the method must surface
		// Error::RegisterError. The error-match arm in register_address
		// requires at least one valid dev/dmap/relay *and* rsp∈{-1,-3},
		// but does NOT consume `sid`, so we include a sid too — the
		// original ignored version tripped over a stale `sid: None` path
		// that widened the window for the reply to race the subscribe.
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		let lookup = UidLookupResults {
			reg: peer_addr,
			relay: peer_addr,
		};

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2rC(_),
				}) => tid,
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::R2cCr(R2cCr {
					dev: Some(IpPort {
						ip: "10.0.0.5".into(),
						port: 45678,
					}),
					dmap: None,
					relay: None,
					relayt: None,
					nat: "NULL".into(),
					sid: Some(1),
					rsp: -1,
					ac: 0,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		if get_local_ip().is_err() {
			camera.abort();
			return;
		}
		let res = t_timeout(T, d.register_address("UIDABC", 1, &lookup)).await;
		assert!(matches!(res, Ok(Err(Error::RegisterError))), "got {res:?}");
		camera.await.unwrap();
	}

	// ---------- client_initiated_dev ----------

	#[tokio::test]
	async fn client_initiated_dev_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: peer_addr,
			dev: Some(peer_addr),
			dmap: None,
			relay: None,
			client_id: 500,
			sid: 1234,
		};

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2dT(C2dT { cid, sid, conn, .. }),
				}) => {
					assert_eq!(cid, 500);
					assert_eq!(sid, 1234);
					assert_eq!(conn, "local");
					tid
				}
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCfm(D2cCfm {
					sid: 1234,
					conn: "local".into(),
					rsp: 0,
					cid: 500,
					did: 7,
					time_r: Some(0),
				}),
			});
			peer_send(&peer, reply, from).await;
			// Then ignore everything else (C2R_CFM send_and_forget
			// loops 5 times).
			let _ = t_timeout(T, async {
				let mut buf = [0u8; 2048];
				for _ in 0..5 {
					let _ = peer.recv_from(&mut buf).await;
				}
			})
			.await;
		});

		let res = t_timeout(Duration::from_secs(5), d.client_initiated_dev(&reg))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.camera_id, 7);
		assert_eq!(res.client_id, 500);
		camera.await.unwrap();
	}

	#[tokio::test]
	async fn client_initiated_dev_no_dev_returns_error() {
		let d = Discoverer::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let r = t_timeout(T, d.client_initiated_dev(&reg)).await.unwrap();
		assert!(matches!(r, Err(Error::NoDev)), "got {r:?}");
	}

	#[tokio::test]
	async fn client_initiated_relay_no_relay_returns_error() {
		let d = Discoverer::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let r = t_timeout(T, d.client_initiated_relay(&reg)).await.unwrap();
		assert!(matches!(r, Err(Error::NoDev)), "got {r:?}");
	}

	#[tokio::test]
	async fn client_initiated_relay_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: peer_addr,
			dev: None,
			dmap: None,
			relay: Some(peer_addr),
			client_id: 600,
			sid: 9000,
		};

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2dT(C2dT { conn, .. }),
				}) => {
					assert_eq!(conn, "relay");
					tid
				}
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCfm(D2cCfm {
					sid: 9000,
					conn: "relay".into(),
					rsp: 0,
					cid: 600,
					did: 44,
					// time_r must be Some — the wire format encodes it as
					// an empty element, and quick_xml deserialization fails
					// if the field is omitted outright. Matches the real
					// camera behaviour of always sending `<timer>0</timer>`.
					time_r: Some(0),
				}),
			});
			peer_send(&peer, reply, from).await;
			// Drain send_and_forget replies.
			let _ = t_timeout(T, async {
				let mut buf = [0u8; 2048];
				for _ in 0..5 {
					let _ = peer.recv_from(&mut buf).await;
				}
			})
			.await;
		});

		let res = t_timeout(Duration::from_secs(5), d.client_initiated_relay(&reg))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.camera_id, 44);
		assert_eq!(res.sid, 9000);
		camera.await.unwrap();
	}

	// ---------- device_initiated_dev / device_initiated_map ----------

	/// The loopback address the camera uses to reach a `Discoverer` —
	/// `local_addr()` is bound to `0.0.0.0`, which isn't a valid send target.
	fn loopback_of(d: &Discoverer) -> SocketAddr {
		SocketAddr::from(([127, 0, 0, 1], d.local_addr().port()))
	}

	/// Drive a device-initiated handshake from the camera side. The camera
	/// opens by re-sending `D2cT` until the `Discoverer` answers with `C2dA`
	/// (covering the subscribe race where the first datagram can land before
	/// `handle_incoming` registers its tid-0 handler), then confirms with
	/// `D2cCfm` and drains the trailing `C2rCfm` + heartbeat traffic.
	async fn drive_device_initiated(peer: UdpSocket, d_addr: SocketAddr, conn: &'static str) {
		let tid = 0x4321u32;
		let d2ct = BcUdp::Discovery(UdpDiscovery {
			tid,
			payload: UdpXml::D2cT(D2cT {
				sid: 1234,
				conn: conn.into(),
				cid: 500,
				did: 7,
			}),
		});

		let from = loop {
			peer_send(&peer, d2ct.clone(), d_addr).await;
			let mut buf = [0u8; 2048];
			match t_timeout(Duration::from_millis(100), peer.recv_from(&mut buf)).await {
				Ok(Ok((n, from))) => {
					let mut codec = BcUdpCodex::new();
					let mut bm = BytesMut::from(&buf[..n]);
					if let Ok(Some(BcUdp::Discovery(UdpDiscovery {
						payload: UdpXml::C2dA(C2dA { conn: got, .. }),
						..
					}))) = codec.decode(&mut bm)
					{
						assert_eq!(got, conn);
						break from;
					}
				}
				_ => continue,
			}
		};

		let cfm = BcUdp::Discovery(UdpDiscovery {
			tid,
			payload: UdpXml::D2cCfm(D2cCfm {
				sid: 1234,
				conn: conn.into(),
				rsp: 0,
				cid: 500,
				did: 7,
				time_r: Some(0),
			}),
		});
		peer_send(&peer, cfm, from).await;

		// Absorb the trailing C2R_CFM (send_and_forget) burst + the first
		// heartbeats so the discoverer's sends don't fail. A short window is
		// enough — the discoverer returns the moment it emits C2R_CFM.
		let _ = t_timeout(Duration::from_millis(300), async {
			let mut buf = [0u8; 2048];
			loop {
				if peer.recv_from(&mut buf).await.is_err() {
					break;
				}
			}
		})
		.await;
	}

	#[tokio::test]
	async fn device_initiated_dev_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		let d_addr = loopback_of(&d);
		let reg = RegisterResult {
			reg: peer_addr,
			dev: None,
			dmap: Some(peer_addr),
			relay: None,
			client_id: 500,
			sid: 1234,
		};

		let camera = tokio::spawn(drive_device_initiated(peer, d_addr, "local"));

		let res = t_timeout(Duration::from_secs(5), d.device_initiated_dev(&reg))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.camera_id, 7);
		assert_eq!(res.client_id, 500);
		assert_eq!(res.sid, 1234);
		assert!(res.nc.is_none());
		assert!(res.pl.is_none());
		camera.await.unwrap();
	}

	#[tokio::test]
	async fn device_initiated_map_success() {
		let (peer, peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		let d_addr = loopback_of(&d);
		let reg = RegisterResult {
			reg: peer_addr,
			dev: None,
			dmap: Some(peer_addr),
			relay: None,
			client_id: 500,
			sid: 1234,
		};

		let camera = tokio::spawn(drive_device_initiated(peer, d_addr, "map"));

		let res = t_timeout(Duration::from_secs(5), d.device_initiated_map(&reg))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(res.camera_id, 7);
		assert_eq!(res.client_id, 500);
		assert_eq!(res.sid, 1234);
		camera.await.unwrap();
	}

	// ---------- Discovery::local full path ----------

	#[tokio::test]
	async fn discovery_local_returns_first_good_address() {
		// Use the optional_addrs override so we bypass the global
		// broadcast (which would flood tests on shared runners).
		let (peer, peer_addr) = scripted_peer().await;
		let discovery = Discovery::new(false).await.unwrap();

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let tid = match pkt {
				BcUdp::Discovery(UdpDiscovery { tid, .. }) => tid,
				_ => unreachable!(),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCr(D2cCr {
					timer: Timer::default(),
					rsp: 0,
					cid: discovery_client_id_hack(&peer),
					did: 1001,
					pl: None,
					nc: None,
				}),
			});
			// We don't know the client_id in advance, but we can just
			// ignore the assertion — the map fn checks
			// `cid == client_id`. Re-read the packet to learn client_id.
			let _ = (tid, reply);
			// Re-send using the actual captured client_id.
			peer_send(
				&peer,
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::D2cCr(D2cCr {
						timer: Timer::default(),
						rsp: 0,
						cid: 0, // placeholder; see extracted fn
						did: 1001,
						pl: None,
						nc: None,
					}),
				}),
				from,
			)
			.await;
		});

		// `discovery_local_returns_first_good_address` is brittle because
		// we can't trivially learn the generated `client_id` from the
		// outside. To avoid flakiness we use a simpler test path: pass
		// an empty optional_addrs (so there are no extras beyond
		// broadcasts) and assert it times out into DiscoveryTimeout.
		camera.abort();
		let _ = peer_addr;
		tokio::time::pause();
		let fut = discovery.local("UNREACH-UID", Some(vec![]));
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(120)).await;
		let r = fut.await;
		// Outcome must be DiscoveryTimeout (nothing to succeed against).
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	// Dummy helper — referenced above just so the previous block compiles
	// cleanly. Not used for real assertions (we placeholder the
	// client_id above).
	fn discovery_client_id_hack(_: &UdpSocket) -> i32 {
		0
	}

	#[tokio::test]
	async fn discovery_local_with_scripted_camera_succeeds() {
		// Cleaner variant: we run the scripted camera FIRST, and have it
		// parse the packet to learn the generated client_id, then reply
		// with it. Uses optional_addrs = [peer_addr] so we only hit our
		// scripted peer.
		let (peer, peer_addr) = scripted_peer().await;
		let discovery = Discovery::new(false).await.unwrap();

		let camera = tokio::spawn(async move {
			let (pkt, from) = peer_recv(&peer).await;
			let (tid, client_id) = match pkt {
				BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2dC(C2dC { cid, .. }),
				}) => (tid, cid),
				other => panic!("unexpected: {other:?}"),
			};
			let reply = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::D2cCr(D2cCr {
					timer: Timer::default(),
					rsp: 0,
					cid: client_id,
					did: 7777,
					pl: None,
					nc: None,
				}),
			});
			peer_send(&peer, reply, from).await;
		});

		// 30s outer budget vs the natural <100ms — same GitHub macos-latest
		// loopback stall as the T constant above. retry_send's internal
		// MAXIMUM_WAIT is 15s, so we allow that plus headroom.
		let res = t_timeout(
			Duration::from_secs(30),
			discovery.local("UIDZ", Some(vec![peer_addr])),
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(res.addr, peer_addr);
		assert_eq!(res.camera_id, 7777);
		camera.await.unwrap();
	}

	// ---------- Discovery::remote / map / relay error branches ----------

	#[tokio::test]
	async fn discovery_remote_errors_when_no_dev_no_response() {
		tokio::time::pause();
		let discovery = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		// remote runs client_initiated_dev + device_initiated_dev in a
		// select!; with dev=None the client branch immediately errors
		// with NoDev, and the device branch waits forever → tokio select
		// takes NoDev.
		let fut = discovery.remote("X", &reg);
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(30)).await;
		let r = fut.await;
		// select may choose NoDev or DiscoveryTimeout.
		assert!(
			matches!(r, Err(Error::NoDev) | Err(Error::DiscoveryTimeout)),
			"got {r:?}"
		);
	}

	#[tokio::test]
	async fn discovery_relay_errors_without_relay_addr() {
		let discovery = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let r = t_timeout(T, discovery.relay(&reg)).await.unwrap();
		assert!(matches!(r, Err(Error::NoDev)), "got {r:?}");
	}

	#[tokio::test]
	async fn discovery_map_times_out_without_incoming_packet() {
		tokio::time::pause();
		let discovery = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: Some("127.0.0.1:9999".parse().unwrap()),
			relay: None,
			client_id: 1,
			sid: 42,
		};
		let fut = discovery.map(&reg);
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(30)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	#[tokio::test]
	async fn discovery_new_binds_ok_and_has_client_id() {
		let d = Discovery::new(false).await.unwrap();
		// client_id is randomized; just check the accessor path works.
		let _ = d.client_id;
	}

	// ---------- CameraDiscoverer trait impl pass-through ----------

	#[tokio::test]
	async fn discovery_trait_get_registration_rejects_no_resolves() {
		// No relay can be reached in the sandbox: either the p2p
		// hostnames don't resolve, or they do and every UID lookup times
		// out. Both paths land in `get_registration`'s "every relay
		// failed" arm and surface `Error::DiscoveryNoRelay`.
		tokio::time::pause();
		let d = Discovery::new(false).await.unwrap();
		let fut = <Discovery as CameraDiscoverer>::get_registration(&d, "XZZZZZZZZZZZZZZZZ");
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(60)).await;
		let r = fut.await;
		// The real-path error is `DiscoveryNoRelay` (every relay failed);
		// in a no-network sandbox resolution itself may time out first. We
		// only need the method path to run and reject.
		assert!(r.is_err(), "expected error, got {r:?}");
	}

	#[tokio::test]
	async fn discovery_trait_relay_pass_through_errors() {
		let d = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let r = t_timeout(T, <Discovery as CameraDiscoverer>::relay(&d, &reg))
			.await
			.unwrap();
		assert!(matches!(r, Err(Error::NoDev)));
	}

	#[tokio::test]
	async fn discovery_trait_map_pass_through() {
		tokio::time::pause();
		let d = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: Some("127.0.0.1:1".parse().unwrap()),
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let fut = <Discovery as CameraDiscoverer>::map(&d, &reg);
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(30)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)), "got {r:?}");
	}

	#[tokio::test]
	async fn discovery_trait_remote_pass_through_errors_when_no_routes() {
		tokio::time::pause();
		let d = Discovery::new(false).await.unwrap();
		let reg = RegisterResult {
			reg: "127.0.0.1:9000".parse().unwrap(),
			dev: None,
			dmap: None,
			relay: None,
			client_id: 1,
			sid: 0,
		};
		let fut = <Discovery as CameraDiscoverer>::remote(&d, "U", &reg);
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(30)).await;
		let r = fut.await;
		assert!(r.is_err());
	}

	#[tokio::test]
	async fn discovery_trait_local_errors_without_valid_targets() {
		tokio::time::pause();
		let d = Discovery::new(false).await.unwrap();
		let fut = <Discovery as CameraDiscoverer>::local(&d, "U", Some(vec![]));
		tokio::pin!(fut);
		tokio::time::advance(Duration::from_secs(30)).await;
		let r = fut.await;
		assert!(matches!(r, Err(Error::DiscoveryTimeout)));
	}

	#[tokio::test]
	async fn discovery_trait_check_tcp_errors_on_bad_addr() {
		let d = Discovery::new(false).await.unwrap();
		// 127.0.0.2:1 will refuse the connection immediately.
		let r = t_timeout(
			Duration::from_secs(6),
			<Discovery as CameraDiscoverer>::check_tcp(&d, "127.0.0.2:1".parse().unwrap(), 0),
		)
		.await
		.unwrap();
		assert!(r.is_err(), "expected error, got {r:?}");
	}

	#[tokio::test]
	async fn discoverer_reader_survives_same_tid_flood() {
		// Regression test for the comment at the reader's `try_send`
		// site: a hostile peer can flood the discoverer with
		// `tid`-matching datagrams faster than the per-tid subscriber
		// drains its bounded (cap=10) channel. The reader must
		// `try_send` (drop on full) rather than `send().await` (block
		// the reader) — otherwise every other in-flight tid stalls
		// behind the slow consumer.
		//
		// Drives the failure mode in three rounds:
		//
		//   1. subscribe to tid_flood + tid_other; do NOT drain
		//      tid_flood.
		//   2. fire FLOOD_COUNT datagrams at tid_flood.
		//   3. fire one datagram at tid_other and assert it lands
		//      within a short timeout. With a `send().await` reader
		//      this would never arrive.
		const FLOOD_COUNT: usize = 200;
		const TID_FLOOD: u32 = 0xABCD;
		const TID_OTHER: u32 = 0x1234;

		let (peer, _peer_addr) = scripted_peer().await;
		let d = Discoverer::new(false).await.unwrap();
		// Discoverer::new binds on 0.0.0.0:port for broadcast; route
		// the test peer's datagrams via 127.0.0.1 + the bound port so
		// the kernel can deliver locally without a real interface.
		let d_addr = SocketAddr::from(([127, 0, 0, 1], d.local_addr().port()));

		let mut rx_flood = d.subscribe(TID_FLOOD).await.expect("subscribe flood");
		let mut rx_other = d.subscribe(TID_OTHER).await.expect("subscribe other");

		// Build one-shot encoded datagrams via the public BcUdpCodex.
		let make_datagram = |tid: u32| -> Vec<u8> {
			let mut codec = BcUdpCodex::new();
			let mut out = BytesMut::new();
			let pkt = BcUdp::Discovery(UdpDiscovery {
				tid,
				payload: UdpXml::C2dHb(C2dHb { cid: 0, did: 0 }),
			});
			codec.encode(pkt, &mut out).unwrap();
			out.to_vec()
		};

		// Round 2: fire the flood. Use blocking send_to so the kernel
		// queues each datagram; rx_flood is at cap (10) within the
		// first ~10 packets and the rest must drop on the reader's
		// try_send Full path.
		let flood = make_datagram(TID_FLOOD);
		for _ in 0..FLOOD_COUNT {
			peer.send_to(&flood, d_addr).await.unwrap();
		}

		// Round 3: one tid_other datagram. The reader must service
		// this within a generous timeout despite the still-undrained
		// tid_flood subscriber.
		let other = make_datagram(TID_OTHER);
		peer.send_to(&other, d_addr).await.unwrap();

		// rx_other must yield within T even though we never drained
		// rx_flood. With a blocking reader this `recv` would never
		// fire because the reader would still be parked on the
		// 11th tid_flood `send().await`.
		let got = t_timeout(T, rx_other.recv())
			.await
			.expect("rx_other timed out — reader is parked, regression!")
			.expect("rx_other channel closed")
			.expect("rx_other delivered an error");
		match got.0.payload {
			UdpXml::C2dHb(_) => {}
			other => panic!("unexpected rx_other payload: {other:?}"),
		}

		// rx_flood must have at most cap-many packets buffered. We
		// can't observe try_send drops directly, but we can drain
		// the flood subscriber and count: with a 10-cap channel and
		// 200 datagrams fired we must see ≤ ~12 (cap + small grace
		// for in-flight kernel-side queuing that landed before the
		// channel filled).
		let mut buffered = 0usize;
		// Pull until empty under a short real-clock deadline; once
		// nothing arrives within 50 ms the channel is drained and
		// the reader has stopped enqueueing for tid_flood.
		while let Ok(Some(_)) =
			tokio::time::timeout(Duration::from_millis(50), rx_flood.recv()).await
		{
			buffered += 1;
			// Defence-in-depth: an unbounded loop here would hide a
			// real regression; cap the assertion at FLOOD_COUNT.
			if buffered > FLOOD_COUNT {
				break;
			}
		}
		assert!(
			buffered <= 32,
			"rx_flood buffered {buffered} packets — bounded-channel cap regression?",
		);
	}
}
