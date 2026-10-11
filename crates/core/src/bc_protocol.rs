use crate::bc;
use crate::redact::UidTail;
use futures::stream::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::{
	collections::HashMap,
	sync::atomic::{AtomicBool, AtomicU16, Ordering},
};
use tokio::sync::RwLock;

use Md5Trunc::*;

mod abilityinfo;
mod battery;
mod camera_driver;
pub mod connection;
mod credentials;
mod email;
mod errors;
#[cfg(any(test, feature = "test-util"))]
mod fake_camera;
mod floodlight;
mod keepalive;
mod ledstate;
mod link;
mod login;
mod login_authlogin;
mod login_sigv3;
mod logout;
mod motion;
mod ping;
mod pirstate;
mod ptz;
mod pushinfo;
mod reboot;
mod resolution;
mod services;
mod set_helpers;
mod siren;
mod snap;
mod stream;
mod stream_info;
mod support;
mod talk;
mod time;
mod uid;
mod users;
mod version;

pub use camera_driver::CameraDriver;
pub(crate) use connection::*;
pub use credentials::*;
pub use errors::Error;
#[cfg(any(test, feature = "test-util"))]
pub use fake_camera::{FakeCalls, FakeCamera, FakeCameraBuilder};
pub use ledstate::LightState;
pub use login::MaxEncryption;
pub use motion::{AiDetections, MotionData, MotionStatus};
pub use pirstate::PirState;
pub use ptz::Direction;
pub use pushinfo::PhoneType;
pub use resolution::*;
use std::sync::Arc;
pub use stream::{StreamData, StreamKind, VideoStream};

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Per-round budget for one P2P registration attempt. A reachable relay
/// answers the lookup + register round-trip in well under a second, so a
/// round that drags on is a dead/firewalled relay holding the whole
/// attempt at its 15 s socket timeout. Cut it short and let the retry
/// loop's exponential backoff (1,2,4,8,16 s) do the waiting instead.
const REGISTRATION_ROUND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

#[derive(Clone, Copy)]
enum ReadKind {
	ReadOnly,
	ReadWrite,
	None,
}

///
/// This is the primary struct of this library when interacting with the camera
///
pub struct BcCamera {
	channel_id: u8,
	connection: Arc<BcConnection>,
	logged_in: AtomicBool,
	message_num: AtomicU16,
	// Reolink protocol requires plaintext credentials in the logout
	// payload (see `logout.rs`). The Baichuan session encrypts the
	// wire so the plaintext does not leave the host outside that case.
	credentials: Credentials,
	abilities: RwLock<HashMap<String, ReadKind>>,
	/// This camera is configured `discovery = "cloud"` — it authenticates with a
	/// minted cloud token (sigV3), never a local password. The account/UID below
	/// are propagated to *every* camera from the document root, so this flag —
	/// NOT `cloud_account.is_some()` — is what gates the cloud login path.
	is_cloud: bool,
	/// Account ("cloud") cameras only: the sigV3 `(nonce, pl)` captured from the
	/// discovery handshake, and the Reolink account + UID used to mint a fresh
	/// token bundle at login. All `None` for ordinary cameras.
	sigv3_handshake: Option<(i64, String)>,
	cloud_account: Option<String>,
	cloud_password: Option<String>,
	cloud_uid: Option<String>,
	/// Host's stored MFA trust token (from a `cloud-authorise` bootstrap), sent
	/// with the cloud mint's password grant to clear login verification on an
	/// IP Reolink would otherwise challenge. `None` when not bootstrapped.
	cloud_mfa_trust_token: Option<String>,
	/// Host's stored refresh token (same bootstrap). Used for a refresh grant
	/// that skips the password grant — and thus MFA — entirely; the fallback
	/// when Reolink issued no `mfa_trust_token`. `None` when absent.
	cloud_refresh_token: Option<String>,
}

/// Options used to construct a camera
#[derive(Debug)]
pub struct BcCameraOpt {
	/// Name, mostly used for message logs
	pub name: String,
	/// Channel the camera is on 0 unless using a NVR
	pub channel_id: u8,
	/// IPs of the camera
	pub addrs: Vec<IpAddr>,
	/// The UID of the camera
	pub uid: Option<String>,
	/// Port to try optional. When not given all known BC ports will be tried
	/// When given all known bc port AND the given port will be tried
	pub port: Option<u16>,
	/// Protocol decides if UDP/TCP are used for the camera
	pub protocol: ConnectionProtocol,
	/// Discovery method to allow
	pub discovery: DiscoveryMethods,
	/// Maximum number of retries for discovery
	pub max_discovery_retries: usize,
	/// Credentials for login
	pub credentials: Credentials,
	/// Reolink account e-mail for account ("cloud") cameras. Used with
	/// `cloud_password` to mint the sigV3 token bundle. Only consulted when
	/// `discovery` is [`DiscoveryMethods::Cloud`].
	pub cloud_account: Option<String>,
	/// Reolink account password for account ("cloud") cameras.
	pub cloud_password: Option<String>,
	/// Host's stored MFA trust token (from a `cloud-authorise` bootstrap), sent
	/// with the cloud mint to clear login verification on an IP Reolink would
	/// otherwise challenge. `None` when not bootstrapped / not needed.
	pub cloud_mfa_trust_token: Option<String>,
	/// Host's stored refresh token (same bootstrap) — refresh-grant fallback
	/// when no `mfa_trust_token` was issued. `None` when absent.
	pub cloud_refresh_token: Option<String>,
	/// Toggle debug print of underlying data
	pub debug: bool,
}

/// Used to choose the print format of various status messages like battery levels
///
/// Currently this is just the format of battery levels but if we ever got more status
/// messages then they will also use this information
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PrintFormat {
	/// None, don't print
	None,
	/// A human readable output
	Human,
	/// Xml formatted
	Xml,
}

/// Type of connection to try
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConnectionProtocol {
	/// TCP and UDP
	#[default]
	TcpUdp,
	/// TCP only
	Tcp,
	/// Udp only
	Udp,
}

pub(crate) enum CameraLocation {
	Tcp(SocketAddr),
	Udp(DiscoveryResult),
}

#[cfg(test)]
impl std::fmt::Debug for CameraLocation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			CameraLocation::Tcp(a) => write!(f, "Tcp({a})"),
			CameraLocation::Udp(d) => write!(f, "Udp({})", d.get_addr()),
		}
	}
}

/// Factory the registration-retry loop calls on each iteration to
/// mint a fresh `CameraDiscoverer` — production reopens a new UDP
/// socket + client-id; tests typically return a cloned scripted
/// handle so the same call log is reused across retries.
pub(crate) type RegFactory = std::sync::Arc<
	dyn Fn() -> std::pin::Pin<
			Box<
				dyn std::future::Future<
						Output = std::result::Result<std::sync::Arc<dyn CameraDiscoverer>, Error>,
					> + Send,
			>,
		> + Send
		+ Sync,
>;

/// Build the TCP socket-candidate list for a camera's `find_camera`
/// TCP probe.
///
/// - `port = None` or `port = Some(9000)` → one socket per addr at 9000.
/// - `port = Some(n)` for any other n → each addr tried at both `n` and
///   9000 (operator override + standard fallback).
///
/// Extracted from `BcCamera::find_camera` so the port-fallback policy
/// is unit-testable without standing up `Discovery`.
pub(crate) fn pick_tcp_sockets(addrs: &[IpAddr], port: Option<u16>) -> Vec<SocketAddr> {
	let mut sockets = Vec::new();
	match port {
		Some(9000) | None => {
			for addr in addrs.iter() {
				sockets.push(SocketAddr::new(*addr, 9000));
			}
		}
		Some(n) => {
			for addr in addrs.iter() {
				sockets.push(SocketAddr::new(*addr, n));
				sockets.push(SocketAddr::new(*addr, 9000));
			}
		}
	}
	sockets
}

/// Build the UDP socket-candidate list for a camera's `find_camera`
/// local-UDP probe.
///
/// - `port = None` / `Some(2015)` / `Some(2018)` → each addr tried at
///   2018 and 2015 (standard Baichuan UDP ports).
/// - `port = Some(n)` for any other n → each addr tried at `n`, 2015,
///   and 2018 (override first, then both standard fallbacks).
pub(crate) fn pick_udp_sockets(addrs: &[IpAddr], port: Option<u16>) -> Vec<SocketAddr> {
	let mut sockets = Vec::new();
	match port {
		None | Some(2015) | Some(2018) => {
			for addr in addrs.iter() {
				sockets.push(SocketAddr::new(*addr, 2018));
				sockets.push(SocketAddr::new(*addr, 2015));
			}
		}
		Some(n) => {
			for addr in addrs.iter() {
				sockets.push(SocketAddr::new(*addr, n));
				sockets.push(SocketAddr::new(*addr, 2015));
				sockets.push(SocketAddr::new(*addr, 2018));
			}
		}
	}
	sockets
}

impl BcCamera {
	/// Try to connect to the camera via appropaite methods and return
	/// the location that should be used. Production wrapper: builds the
	/// real UDP-socket-backed `Discovery` and a fresh `reg_factory` that
	/// mints a new `Discovery` on each re-registration retry (the inner
	/// registration loop deliberately resets client IDs between tries,
	/// which was the only subtlety driving the old open-coded form).
	async fn find_camera(options: &BcCameraOpt) -> Result<CameraLocation> {
		// Account ("cloud") cameras advertise sigV3 (`lver=3`) on the connect.
		let sigv3 = matches!(options.discovery, DiscoveryMethods::Cloud);
		let discovery = std::sync::Arc::new(Discovery::new(sigv3).await?)
			as std::sync::Arc<dyn CameraDiscoverer>;
		let reg_factory: RegFactory = std::sync::Arc::new(move || {
			Box::pin(async move {
				Ok::<std::sync::Arc<dyn CameraDiscoverer>, Error>(std::sync::Arc::new(
					Discovery::new(sigv3).await?,
				)
					as std::sync::Arc<dyn CameraDiscoverer>)
			})
		});
		Self::find_camera_with_discoverer(options, discovery, reg_factory).await
	}

	/// Core discovery state machine, parameterised on the `Discoverer`
	/// so tests can script every branch without opening a real UDP
	/// socket. The `reg_factory` recreates the "registration" handle on
	/// each retry (production uses a fresh `Discovery` with a new client
	/// id each pass; tests reuse a single scripted handle by cloning).
	pub(crate) async fn find_camera_with_discoverer(
		options: &BcCameraOpt,
		discovery: std::sync::Arc<dyn CameraDiscoverer>,
		reg_factory: RegFactory,
	) -> Result<CameraLocation> {
		if let ConnectionProtocol::Tcp | ConnectionProtocol::TcpUdp = options.protocol {
			let mut sockets = pick_tcp_sockets(&options.addrs, options.port);
			if !sockets.is_empty() {
				info!("{}: Trying TCP discovery", options.name);
				for socket in sockets.drain(..) {
					let channel_id: u8 = options.channel_id;
					if let Ok(addr) = discovery.check_tcp(socket, channel_id).await.map(|_| {
						info!("{}: TCP Discovery success at {:?}", options.name, socket);
						socket
					}) {
						return Ok(CameraLocation::Tcp(addr));
					}
				}
			}
		}

		if let (Some(uid), ConnectionProtocol::Udp | ConnectionProtocol::TcpUdp) =
			(options.uid.as_ref(), options.protocol)
		{
			let sockets = pick_udp_sockets(&options.addrs, options.port);
			let flags = resolution::discovery_flags_for(options.discovery);
			let (allow_local, allow_remote, allow_map, allow_relay) =
				(flags.local, flags.remote, flags.map, flags.relay);
			// Full UID at debug; the info-level success lines below show
			// only its tail (info logs end up in bug reports).
			debug!("{}: UDP discovery for UID {}", options.name, uid);

			let res = tokio::select! {
				Ok(v) = async {
					let uid_local = uid.clone();
					info!("{}: Trying local discovery", options.name);
					let result = discovery.local(&uid_local, Some(sockets)).await;
					match result {
						Ok(disc) => {
							info!(
								"{}: Local discovery success {} at {}",
								options.name,
								UidTail(&uid_local),
								disc.get_addr()
							);
							Ok(CameraLocation::Udp(disc))
						},
						Err(e) => Err(e)
					}
				}, if allow_local => Ok(v),
				Ok(v) = async {
					let mut reg_disc = reg_factory().await?;
					let reg_result;
					// Registration is looped as it seems that reolink
					// only updates the registration lazily when someone attempts
					// to connect. The first few connects fails until the server data
					// is updated
					//
					// We loop infinitly and allow the caller to timeout at the
					// interval they desire
					let mut retry = 0;
					let max_retry: usize = options.max_discovery_retries;
					loop {
						tokio::task::yield_now().await;
						// Bound each round so a single hung relay can't stall
						// the whole budget at the 15 s socket timeout — a live
						// relay answers in well under a second, so cutting a
						// dead round short and backing off is strictly better.
						let round = tokio::time::timeout(
							REGISTRATION_ROUND_TIMEOUT,
							reg_disc.get_registration(uid),
						)
						.await;
						let round_err = match round {
							Ok(Ok(result)) => {
								reg_result = result;
								break;
							}
							// Relay-level failure carries a per-relay summary.
							Ok(Err(e)) => e,
							// Round exceeded its budget (all relays slow/hung).
							Err(_) => Error::DiscoveryTimeout,
						};
						if retry >= max_retry && max_retry > 0 {
							// Surface the real reason (which relays failed),
							// not a bare timeout.
							return Err(round_err);
						}
						// Exponential backoff between attempts: 1,2,4,8,16 s
						// (capped at 30). Reolink only updates a camera's
						// registration lazily — and a battery camera may be asleep
						// — so hammering with a flat 1 s gap just burns the retry
						// budget before either has a chance to come good. Waiting
						// progressively gives both time to catch up.
						let backoff = (1u64 << (retry.min(5) as u32)).min(30);
						log::info!(
							"{}: discovery attempt {}/{} failed ({round_err}); retrying in {backoff}s",
							options.name,
							retry + 1,
							if max_retry > 0 {
								format!("{max_retry}")
							} else {
								"infinite".to_string()
							}
						);
						retry += 1;
						tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
						// New discovery to get new client IDs
						reg_disc = reg_factory().await?;
					};
					tokio::select! {
						Ok(v) = async {
							let uid_remote = uid.clone();
							info!("{}: Trying remote discovery", options.name);
							let result = reg_disc
								.remote(&uid_remote, &reg_result)
								.await;
							match result {
								Ok(disc) => {
									info!(
										"{}: Remote discovery success {} at {}",
										options.name,
										UidTail(&uid_remote),
										disc.get_addr()
									);
									Ok(CameraLocation::Udp(disc))
								},
								Err(e) => Err(e)
							}
						}, if allow_remote => Ok(v),
						Ok(v) = async {
							let uid_map = uid.clone();
							tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
							info!("{}: Trying map discovery", options.name);
							let result = reg_disc.map(&reg_result).await;
							match result {
								Ok(disc) => {
									info!(
										"{}: Map success {} at {}",
										options.name,
										UidTail(&uid_map),
										disc.get_addr()
									);
									Ok(CameraLocation::Udp(disc))
								},
								Err(e) => Err(e),
							}
						}, if allow_map => Ok(v),
						Ok(v) = async {
							let uid_relay = uid.clone();
							tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
							info!("{}: Trying relay discovery", options.name);
							let result = reg_disc.relay(&reg_result).await;
							match result {
								Ok(disc) => {
									info!(
										"{}: Relay success {} at {}",
										options.name,
										UidTail(&uid_relay),
										disc.get_addr()
									);
									Ok(CameraLocation::Udp(disc))
								},
								Err(e) => Err(e),
							}
						}, if allow_relay => Ok(v),
						else => Err(Error::DiscoveryTimeout),
					}
				}, if allow_remote || allow_map || allow_relay => Ok(v),
				else => Err(Error::DiscoveryTimeout),
			}?;

			return Ok(res);
		}

		info!("{}: Discovery failed", options.name);
		// Nothing works
		Err(Error::CannotInitCamera)
	}

	///
	/// Create a new camera interface
	///
	/// # Parameters
	///
	/// * `options` - Camera information see [`BcCameraOpt]
	///
	/// # Returns
	///
	/// returns either an error or the camera
	///
	pub async fn new(options: &BcCameraOpt) -> Result<Self> {
		let username: String = options.credentials.username.clone();
		let passwd: Option<String> = options.credentials.password.clone();

		// Account-camera sigV3 handshake `(nonce, pl)`, captured from the
		// discovery reply before the DiscoveryResult is consumed below.
		let mut sigv3_handshake: Option<(i64, String)> = None;
		let (sink, source): (BcConnSink, BcConnSource) = {
			match BcCamera::find_camera(options).await? {
				CameraLocation::Tcp(addr) => {
					let (x, r) = TcpSource::new(addr, &username, passwd.as_ref(), options.debug)
						.await?
						.split();
					(Box::new(x), Box::new(r))
				}
				CameraLocation::Udp(mut discovery) => {
					sigv3_handshake = discovery.take_sigv3_handshake();
					let (x, r) = UdpSource::new_from_discovery(
						discovery,
						&username,
						passwd.as_ref(),
						options.debug,
					)
					.await?
					.split();
					(Box::new(x), Box::new(r))
				}
			}
		};

		let conn = BcConnection::new(sink, source).await?;

		trace!("Success");
		let me = Self {
			connection: Arc::new(conn),
			// Random starting point so wraparound timing decorrelates
			// across sessions. With sequential 0-start, an always-on
			// camera could in theory hit `(msg_id, msg_num)` collisions
			// every 65 536 keepalives if a previous-session subscription
			// somehow lingered; randomising removes the alignment.
			message_num: AtomicU16::new(rand::random::<u16>()),
			channel_id: options.channel_id,
			logged_in: AtomicBool::new(false),
			credentials: Credentials::new(username, passwd),
			abilities: Default::default(),
			is_cloud: matches!(options.discovery, DiscoveryMethods::Cloud),
			sigv3_handshake,
			cloud_account: options.cloud_account.clone(),
			cloud_password: options.cloud_password.clone(),
			cloud_uid: options.uid.clone(),
			cloud_mfa_trust_token: options.cloud_mfa_trust_token.clone(),
			cloud_refresh_token: options.cloud_refresh_token.clone(),
		};
		// `keepalive` registers the inbound handler. If it fails, tear
		// the just-started BcConnection down explicitly so we don't
		// leak the spawned poller task on the error path.
		if let Err(e) = me.keepalive().await {
			let _ = me.connection.shutdown().await;
			return Err(e);
		}
		Ok(me)
	}

	/// This method will get a new message number and increment the message count atomically
	pub fn new_message_num(&self) -> u16 {
		self.message_num.fetch_add(1, Ordering::Relaxed)
	}

	fn get_connection(&self) -> Arc<BcConnection> {
		self.connection.clone()
	}

	// Certains commands like logout need the username and password
	// this command will return
	// This will only work after login
	fn get_credentials(&self) -> &Credentials {
		&self.credentials
	}

	async fn has_ability<T: Into<String>>(&self, name: T) -> ReadKind {
		let abilities = self.abilities.read().await;
		if let Some(kind) = abilities.get(&name.into()).copied() {
			kind
		} else {
			ReadKind::None
		}
	}
	/// Whether the login-time `abilityInfo` lists `name` (any
	/// permission): `Some(true/false)` once the report was received,
	/// `None` when it wasn't — account (cloud) cameras answer msg 151
	/// with 421, leaving the map empty, which says nothing about the
	/// camera. Reads the cached map — no camera round trip.
	pub async fn ability_advertised(&self, name: &str) -> Option<bool> {
		if self.abilities.read().await.is_empty() {
			return None;
		}
		Some(!matches!(self.has_ability(name).await, ReadKind::None))
	}

	async fn has_ability_ro<T: Into<String>>(&self, name: T) -> Result<()> {
		let s: String = name.into();
		match self.has_ability(&s).await {
			ReadKind::ReadWrite | ReadKind::ReadOnly => Ok(()),
			ReadKind::None => Err(Error::MissingAbility {
				name: s.clone(),
				requested: "read".to_string(),
				actual: "none".to_string(),
			}),
		}
	}
	async fn has_ability_rw<T: Into<String>>(&self, name: T) -> Result<()> {
		let s: String = name.into();
		match self.has_ability(&s).await {
			ReadKind::ReadWrite => Ok(()),
			ReadKind::ReadOnly => Err(Error::MissingAbility {
				name: s.clone(),
				requested: "write".to_string(),
				actual: "read".to_string(),
			}),
			ReadKind::None => Err(Error::MissingAbility {
				name: s.clone(),
				requested: "write".to_string(),
				actual: "none".to_string(),
			}),
		}
	}

	/// Wait for all thread to finish
	///
	/// If an error is returned in any thread it will return the first error
	pub async fn join(&self) -> Result<()> {
		self.connection.join().await
	}

	/// Disconnect from the camera. This is done by sending cancel to
	/// all threads then waiting for the join
	pub async fn shutdown(&self) -> Result<()> {
		self.connection.shutdown().await?;
		Ok(())
	}

	/// Construct a `BcCamera` wired to a scripted `MockConnection`,
	/// skipping discovery / TCP-UDP / login / keepalive. The resulting
	/// camera has `channel_id = 0`, no credentials, `logged_in = true`,
	/// and an empty ability table. Tests that exercise ability-gated
	/// commands must install abilities via [`BcCamera::test_set_ability`].
	///
	/// Gated on `#[cfg(any(test, feature = "test-util"))]` so release
	/// builds cannot accidentally substitute a fake for a real camera.
	#[cfg(any(test, feature = "test-util"))]
	pub async fn from_mock_connection(mock: connection::mock::MockConnection) -> Self {
		Self::from_mock_connection_with_credentials(
			mock,
			Credentials::new("admin".to_string(), None::<String>),
		)
		.await
	}

	/// Variant of [`Self::from_mock_connection`] that accepts an explicit
	/// [`Credentials`] payload. Used by login-flow tests that need to
	/// drive code paths gated on the password's presence / shape (e.g.
	/// the empty-password fail-fast under `MaxEncryption::Aes`).
	#[cfg(any(test, feature = "test-util"))]
	pub async fn from_mock_connection_with_credentials(
		mock: connection::mock::MockConnection,
		credentials: Credentials,
	) -> Self {
		use std::sync::atomic::AtomicBool;
		Self {
			connection: mock.into_arc(),
			message_num: AtomicU16::new(0),
			channel_id: 0,
			logged_in: AtomicBool::new(true),
			credentials,
			abilities: Default::default(),
			is_cloud: false,
			sigv3_handshake: None,
			cloud_account: None,
			cloud_password: None,
			cloud_uid: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
		}
	}

	/// Test-only: install an ability so `has_ability_ro` /
	/// `has_ability_rw` checks succeed. Gated on `test-util`.
	#[cfg(any(test, feature = "test-util"))]
	pub async fn test_set_ability(&self, name: impl Into<String>, read_write: bool) {
		let kind = if read_write {
			ReadKind::ReadWrite
		} else {
			ReadKind::ReadOnly
		};
		self.abilities.write().await.insert(name.into(), kind);
	}
}

/// The Baichuan library has a very peculiar behavior where it always zeros the last byte.  I
/// believe this is because the MD5'ing of the user/password is a recent retrofit to the code and
/// the original code wanted to prevent a buffer overflow with strcpy.  The modern and legacy login
/// messages have a slightly different behavior; the legacy message has a 32-byte buffer and the
/// modern message uses XML.  The legacy code copies all 32 bytes with memcpy, and the XML value is
/// copied from a C-style string, so the appended null byte is dropped by the XML library - see the
/// test below.
/// Emulate this behavior by providing a configurable mangling of the last character.
#[derive(PartialEq, Eq)]
enum Md5Trunc {
	ZeroLast,
	Truncate,
}

fn md5_string(input: &str, trunc: Md5Trunc) -> String {
	let mut md5 = format!("{:X}\0", md5::compute(input));
	md5.replace_range(31.., if trunc == Truncate { "" } else { "\0" });
	md5
}

#[test]
fn test_md5_string() {
	// Note that these literals are only 31 characters long - see explanation above.
	assert_eq!(
		md5_string("admin", Truncate),
		"21232F297A57A5A743894A0E4A801FC"
	);
	assert_eq!(
		md5_string("admin", ZeroLast),
		"21232F297A57A5A743894A0E4A801FC\0"
	);
}

#[cfg(test)]
mod bccamera_helpers_tests {
	//! Coverage for the non-IO BcCamera helpers that are callable from a
	//! mock-wired camera: constructor defaults, `new_message_num`
	//! monotonic counter, ability gating, credential access, and clean
	//! `shutdown` / `join` lifecycle with no expectations pending.
	use super::*;
	use crate::bc_protocol::connection::mock::MockConnection;

	#[tokio::test]
	async fn new_message_num_increments_monotonically() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		// Counter is AtomicU16 seeded at 0 in `from_mock_connection`.
		assert_eq!(cam.new_message_num(), 0);
		assert_eq!(cam.new_message_num(), 1);
		assert_eq!(cam.new_message_num(), 2);
	}

	#[tokio::test]
	async fn default_ability_lookup_returns_none_kind() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		assert!(matches!(
			cam.has_ability("unknownFeature").await,
			ReadKind::None
		));
	}

	#[tokio::test]
	async fn test_set_ability_ro_flows_through_rw_check() {
		// RO ability → has_ability_ro OK, has_ability_rw Err(write).
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("feature", false).await;
		cam.has_ability_ro("feature")
			.await
			.expect("ro check should pass");
		let err = cam
			.has_ability_rw("feature")
			.await
			.expect_err("rw on RO should fail");
		match err {
			Error::MissingAbility {
				requested, actual, ..
			} => {
				assert_eq!(requested, "write");
				assert_eq!(actual, "read");
			}
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn test_set_ability_rw_satisfies_both_checks() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("feature", true).await;
		cam.has_ability_ro("feature").await.expect("ro ok");
		cam.has_ability_rw("feature").await.expect("rw ok");
	}

	#[tokio::test]
	async fn missing_ability_ro_reports_none_actual() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.has_ability_ro("absent").await.expect_err("no ability");
		match err {
			Error::MissingAbility { actual, .. } => assert_eq!(actual, "none"),
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn missing_ability_rw_reports_none_actual() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.has_ability_rw("absent").await.expect_err("no ability");
		match err {
			Error::MissingAbility {
				requested, actual, ..
			} => {
				assert_eq!(requested, "write");
				assert_eq!(actual, "none");
			}
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn get_credentials_returns_seeded_admin() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let creds = cam.get_credentials();
		assert_eq!(creds.username, "admin");
		assert!(creds.password.is_none());
	}

	#[tokio::test]
	async fn get_connection_returns_shared_arc() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let a = cam.get_connection();
		let b = cam.get_connection();
		// Same underlying connection.
		assert!(Arc::ptr_eq(&a, &b));
	}

	#[tokio::test]
	async fn shutdown_on_mock_connection_returns_ok() {
		// Covers BcCamera::shutdown() → BcConnection::shutdown().
		// `from_mock_connection` wires a connection with no active
		// subscriptions so the shutdown path is the clean one.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.shutdown().await.expect("clean shutdown");
	}

	#[tokio::test]
	async fn shutdown_clears_every_spawned_task() {
		// `BcConnection::new` spawns three internal tasks (rx pump,
		// sink pump, poller). The shutdown path must cancel ALL of
		// them via the shared CancellationToken — a regression that
		// added a fourth task without wiring it to the token would
		// hang the `join_next` drain forever, but only on real
		// hardware where the task does work. Pin the post-shutdown
		// task count at zero here so the discipline catches a leak
		// at unit-test time.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let pre = cam.get_connection().task_count().await;
		assert!(pre > 0, "expected spawned tasks pre-shutdown, got {pre}");
		cam.shutdown().await.expect("clean shutdown");
		// After shutdown, every drained handle is removed from the
		// JoinSet — `join_next` returns None and `len() == 0`.
		let post = cam.get_connection().task_count().await;
		assert_eq!(
			post, 0,
			"shutdown left {post} tasks alive in rx JoinSet — leak / missing cancel?",
		);
	}

	#[tokio::test]
	async fn join_on_mock_connection_returns_ok() {
		// Covers BcCamera::join(). After a clean shutdown the
		// underlying task set is empty so join returns Ok.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.shutdown().await.expect("clean shutdown");
		cam.join().await.expect("join returns ok after shutdown");
	}

	#[tokio::test]
	async fn md5_string_truncate_vs_zero_last_differ_only_in_last_byte() {
		let tru = md5_string("root", Truncate);
		let zer = md5_string("root", ZeroLast);
		assert_eq!(tru.len() + 1, zer.len());
		assert!(zer.ends_with('\0'));
		assert_eq!(&tru[..], &zer[..tru.len()]);
	}
}

#[cfg(test)]
mod find_camera_guardrail_tests {
	//! End-to-end coverage for the non-discovery fallthrough paths
	//! of `BcCamera::find_camera` — the ones that never enter the
	//! `tokio::select!` UDP chain and so don't need mocked Reolink
	//! registration servers.
	use super::*;
	use crate::bc_protocol::resolution::DiscoveryMethods;

	fn base_opts() -> BcCameraOpt {
		BcCameraOpt {
			name: "cam-guard".into(),
			channel_id: 0,
			addrs: vec![],
			uid: None,
			port: None,
			protocol: ConnectionProtocol::Udp,
			discovery: DiscoveryMethods::Local,
			max_discovery_retries: 1,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		}
	}

	#[tokio::test]
	async fn find_camera_udp_without_uid_yields_cannot_init() {
		// UDP-only protocol, no UID → the `if let (Some(uid), ...)`
		// guard fails, control falls through to
		// `Err(Error::CannotInitCamera)` (line 315 in the original).
		let opts = base_opts();
		let err = BcCamera::find_camera(&opts).await.expect_err("must err");
		assert!(
			matches!(err, Error::CannotInitCamera),
			"expected CannotInitCamera, got {err:?}"
		);
	}

	#[tokio::test]
	async fn find_camera_tcp_empty_addrs_falls_through() {
		// TCP protocol but no addrs → TCP candidate list is empty,
		// skips the inner loop, and since there's no UID the UDP
		// block is skipped too → CannotInitCamera.
		let mut opts = base_opts();
		opts.protocol = ConnectionProtocol::Tcp;
		let err = BcCamera::find_camera(&opts).await.expect_err("must err");
		assert!(matches!(err, Error::CannotInitCamera));
	}

	#[tokio::test]
	async fn find_camera_tcp_udp_no_uid_no_addr_falls_through() {
		// TcpUdp (default) but neither TCP candidates nor UID — same
		// outcome as the other two, confirming the guard-and-fall-
		// through chain is correct for every combination that can't
		// possibly reach a live camera.
		let mut opts = base_opts();
		opts.protocol = ConnectionProtocol::TcpUdp;
		let err = BcCamera::find_camera(&opts).await.expect_err("must err");
		assert!(matches!(err, Error::CannotInitCamera));
	}

	#[tokio::test]
	async fn find_camera_tcp_with_closed_port_falls_through_to_cannot_init() {
		// Closed port → each `check_tcp` fails fast (ECONNREFUSED),
		// exercises the `for socket in sockets.drain` loop body, then
		// with no UID we fall to CannotInitCamera. Keeps us out of
		// the 4 s TCP_WAIT by using a loopback address that rejects
		// immediately.
		let mut opts = base_opts();
		opts.protocol = ConnectionProtocol::Tcp;
		opts.addrs = vec!["127.0.0.1".parse().unwrap()];
		opts.port = Some(1);
		let err = tokio::time::timeout(
			std::time::Duration::from_secs(10),
			BcCamera::find_camera(&opts),
		)
		.await
		.expect("did not hang")
		.expect_err("must err");
		assert!(matches!(err, Error::CannotInitCamera));
	}

	#[tokio::test]
	async fn find_camera_udp_all_methods_disabled_returns_discovery_timeout() {
		// UDP + UID but every discovery method disabled — the tokio
		// select falls straight into the inner `else => Err(
		// DiscoveryTimeout)`. Pins the "nothing is enabled" guard.
		let mut opts = base_opts();
		opts.uid = Some("UID123".into());
		opts.discovery = DiscoveryMethods::None;
		let err = BcCamera::find_camera(&opts)
			.await
			.expect_err("no methods → err");
		assert!(
			matches!(err, Error::DiscoveryTimeout),
			"expected DiscoveryTimeout, got {err:?}",
		);
	}
}

#[cfg(test)]
mod discoverer_fallback_tests {
	//! Covers every branch of the `find_camera_with_discoverer` UDP
	//! fallback chain by scripting `CameraDiscoverer` outcomes. Each
	//! test runs under a 5 s safety timeout — the 250 ms / 500 ms
	//! sleep staggers inside the select make this the busiest test
	//! module in the crate, but no single arm depends on wall time.
	use super::*;
	use crate::bc_protocol::connection::discovery::test_support::{
		CallLog, Outcome, ScriptedDiscoverer,
	};
	use crate::bc_protocol::resolution::DiscoveryMethods;
	use std::sync::Arc;
	use std::time::Duration;

	fn opts_udp_with_uid() -> BcCameraOpt {
		BcCameraOpt {
			name: "cam".into(),
			channel_id: 0,
			addrs: vec![],
			uid: Some("UID".into()),
			port: None,
			protocol: ConnectionProtocol::Udp,
			discovery: DiscoveryMethods::Local,
			max_discovery_retries: 1,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		}
	}

	fn factory_of(
		disc: Arc<dyn CameraDiscoverer>,
	) -> (RegFactory, Arc<std::sync::atomic::AtomicUsize>) {
		let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let counter2 = counter.clone();
		let disc2 = disc.clone();
		let factory: RegFactory = Arc::new(move || {
			counter2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			let d = disc2.clone();
			Box::pin(async move { Ok::<Arc<dyn CameraDiscoverer>, Error>(d) })
		});
		(factory, counter)
	}

	async fn run_with_timeout(
		opts: BcCameraOpt,
		scripted: ScriptedDiscoverer,
	) -> (Result<CameraLocation>, Arc<CallLog>) {
		let log = scripted.log.clone();
		let disc: Arc<dyn CameraDiscoverer> = Arc::new(scripted);
		let (factory, _) = factory_of(disc.clone());
		let res = tokio::time::timeout(
			Duration::from_secs(5),
			BcCamera::find_camera_with_discoverer(&opts, disc, factory),
		)
		.await
		.expect("did not hang");
		(res, log)
	}

	#[tokio::test]
	async fn local_succeeds_skips_remote_map_relay() {
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::OkDiscovery {
			addr: "127.0.0.1:9000".parse().unwrap(),
		};
		let opts = opts_udp_with_uid();
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("ok");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		// The registration branch is spawned concurrently — it may call
		// `get_registration` before the local arm wins. What we assert
		// is that at least `local` ran and we got an Ok.
		assert!(log.methods().contains(&"local"));
	}

	#[tokio::test]
	async fn local_err_remote_err_map_err_relay_ok_wins() {
		// Local disabled (to keep the inner select predictable),
		// remote/map/relay all disabled except relay — so relay arm
		// wins without racing.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_get_registration = Outcome::Ok;
		sc.on_relay = Outcome::OkDiscovery {
			addr: "127.0.0.1:9001".parse().unwrap(),
		};
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Relay;
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("ok");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"get_registration"));
		assert!(log.methods().contains(&"relay"));
	}

	#[tokio::test]
	async fn all_methods_disabled_short_circuits_to_timeout() {
		let sc = ScriptedDiscoverer::new();
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::None;
		let (res, _log) = run_with_timeout(opts, sc).await;
		let err = res.expect_err("should err");
		assert!(matches!(err, Error::DiscoveryTimeout));
	}

	#[tokio::test]
	async fn registration_fails_until_max_retries_exhausted() {
		let sc = ScriptedDiscoverer::new();
		let mut opts = opts_udp_with_uid();
		// Disable local so registration retries are observable.
		opts.discovery = DiscoveryMethods::Relay;
		opts.max_discovery_retries = 1;
		// Wrap in a tight hard timeout since inner loop sleeps 1s per retry.
		// (1 retry → ~1s sleep → should return DiscoveryTimeout.)
		let t0 = std::time::Instant::now();
		let (res, _log) = run_with_timeout(opts, sc).await;
		let err = res.expect_err("should err");
		assert!(matches!(err, Error::DiscoveryTimeout));
		assert!(t0.elapsed() < Duration::from_secs(4));
	}

	#[tokio::test]
	async fn map_wins_when_local_and_remote_fail() {
		// DiscoveryMethods::Map enables local + remote + map arms.
		// With local + remote errored and map Ok, the map arm must win.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Err;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::Err;
		sc.on_map = Outcome::OkDiscovery {
			addr: "127.0.0.1:9002".parse().unwrap(),
		};
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Map;
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("ok");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"map"));
	}

	#[tokio::test]
	async fn remote_wins_when_local_fails() {
		// DiscoveryMethods::Remote enables local + remote arms. With
		// local errored and remote Ok, remote arm must win — exercises
		// the remote Ok log + CameraLocation::Udp wrap.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Err;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::OkDiscovery {
			addr: "127.0.0.1:9003".parse().unwrap(),
		};
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Remote;
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("ok");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"remote"));
	}

	#[tokio::test]
	async fn relay_err_is_recorded_and_falls_through_to_timeout() {
		// All inner arms error → select falls through the relay Err(e)
		// arm (line 378) into the inner else → DiscoveryTimeout.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Err;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::Err;
		sc.on_map = Outcome::Err;
		sc.on_relay = Outcome::Err;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Relay;
		let (res, log) = run_with_timeout(opts, sc).await;
		let err = res.expect_err("should err");
		assert!(matches!(err, Error::DiscoveryTimeout));
		// Relay arm was entered (even though it failed).
		assert!(log.methods().contains(&"relay"));
	}

	#[tokio::test]
	async fn map_err_is_recorded() {
		// With only map enabled and map returning Err, it falls through
		// to DiscoveryTimeout — exercises the map Err match arm (line
		// 360) for branch coverage alongside the relay / remote siblings.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Err;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::Err;
		sc.on_map = Outcome::Err;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Map;
		let (res, _log) = run_with_timeout(opts, sc).await;
		assert!(matches!(res, Err(Error::DiscoveryTimeout)));
	}

	#[tokio::test]
	async fn remote_err_is_recorded() {
		// Remote-only method with remote errored → DiscoveryTimeout via
		// the inner else. Exercises the remote Err(e) arm (line 342).
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Err;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::Err;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Remote;
		let (res, _log) = run_with_timeout(opts, sc).await;
		assert!(matches!(res, Err(Error::DiscoveryTimeout)));
	}

	#[tokio::test]
	async fn tcp_with_discoverer_path_succeeds_via_check_tcp() {
		// TCP path uses only `check_tcp`. Provide a sc that returns Ok.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_check_tcp = Outcome::Ok;
		let opts = BcCameraOpt {
			name: "cam".into(),
			channel_id: 0,
			addrs: vec!["127.0.0.1".parse().unwrap()],
			uid: None,
			port: Some(9000),
			protocol: ConnectionProtocol::Tcp,
			discovery: DiscoveryMethods::Local,
			max_discovery_retries: 1,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		};
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("ok");
		assert!(matches!(loc, CameraLocation::Tcp(_)));
		assert_eq!(log.methods(), vec!["check_tcp"]);
	}

	#[tokio::test]
	async fn tcp_check_fails_falls_through_to_cannot_init_without_uid() {
		let mut sc = ScriptedDiscoverer::new();
		sc.on_check_tcp = Outcome::Err;
		let opts = BcCameraOpt {
			name: "cam".into(),
			channel_id: 0,
			addrs: vec!["127.0.0.1".parse().unwrap()],
			uid: None,
			port: Some(9000),
			protocol: ConnectionProtocol::Tcp,
			discovery: DiscoveryMethods::Local,
			max_discovery_retries: 1,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		};
		let (res, _log) = run_with_timeout(opts, sc).await;
		let err = res.expect_err("should err");
		assert!(matches!(err, Error::CannotInitCamera));
	}

	#[tokio::test]
	async fn tcp_udp_hybrid_tcp_success_wins_without_uid_check() {
		// When TCP probe succeeds, UDP block is not entered even with
		// UID set.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_check_tcp = Outcome::Ok;
		let opts = BcCameraOpt {
			name: "cam".into(),
			channel_id: 0,
			addrs: vec!["127.0.0.1".parse().unwrap()],
			uid: Some("UID".into()),
			port: Some(9000),
			protocol: ConnectionProtocol::TcpUdp,
			discovery: DiscoveryMethods::Local,
			max_discovery_retries: 1,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		};
		let (res, log) = run_with_timeout(opts, sc).await;
		assert!(matches!(res.expect("ok"), CameraLocation::Tcp(_)));
		let methods = log.methods();
		assert!(methods.contains(&"check_tcp"));
		assert!(!methods.contains(&"local"), "local must not be called");
	}

	// ---------- race-ordering tests ----------
	//
	// The plain "method X errors and Y succeeds" tests above all use
	// `Outcome::Err` to silence the losing arms. That proves the Ok
	// branch propagates but does NOT prove the select! cancels its
	// concurrent siblings — `Outcome::Err` returns immediately too.
	//
	// These three tests substitute `Outcome::Hang` (a future that
	// never resolves) on the losing arms. If the select!'s drop
	// semantics ever regressed (e.g., a refactor moved an arm out of
	// the select), the function would hang past the harness's 5 s
	// hard timeout and fail. With the current biased-select! shape
	// each test completes in well under a second.

	#[tokio::test]
	async fn local_ok_cancels_remote_map_relay_hangers() {
		// Local resolves immediately; remote, map, and relay would
		// otherwise run forever. The select! must drop the inner
		// futures the moment local's Ok arm fires.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::OkDiscovery {
			addr: "127.0.0.1:9000".parse().unwrap(),
		};
		sc.on_get_registration = Outcome::Hang;
		sc.on_remote = Outcome::Hang;
		sc.on_map = Outcome::Hang;
		sc.on_relay = Outcome::Hang;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Relay; // enable all four arms
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("local must win quickly");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"local"));
	}

	#[tokio::test]
	async fn remote_ok_wins_when_local_and_map_relay_hang() {
		// Local + map + relay hang, remote returns Ok. Inside the
		// inner select! remote has no startup sleep, map has a
		// 250 ms sleep, relay has 500 ms — remote must resolve
		// first regardless of timing.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Hang;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::OkDiscovery {
			addr: "127.0.0.1:9001".parse().unwrap(),
		};
		sc.on_map = Outcome::Hang;
		sc.on_relay = Outcome::Hang;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Relay;
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("remote must win, others cancelled");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"remote"));
	}

	#[tokio::test]
	async fn map_ok_wins_when_local_remote_and_relay_hang() {
		// Local + remote + relay hang, map Ok after its 250 ms
		// sleep arms. Relay's 500 ms sleep means map's arm becomes
		// resolvable first; the select must pick map and cancel
		// relay's still-sleeping future.
		let mut sc = ScriptedDiscoverer::new();
		sc.on_local = Outcome::Hang;
		sc.on_get_registration = Outcome::Ok;
		sc.on_remote = Outcome::Hang;
		sc.on_map = Outcome::OkDiscovery {
			addr: "127.0.0.1:9002".parse().unwrap(),
		};
		sc.on_relay = Outcome::Hang;
		let mut opts = opts_udp_with_uid();
		opts.discovery = DiscoveryMethods::Relay;
		let (res, log) = run_with_timeout(opts, sc).await;
		let loc = res.expect("map must win, relay sleep cancelled");
		assert!(matches!(loc, CameraLocation::Udp(_)));
		assert!(log.methods().contains(&"map"));
	}
}

#[cfg(test)]
mod pick_sockets_tests {
	//! Pure-logic coverage for the TCP / UDP socket-candidate
	//! builders extracted from `find_camera`. These pin the
	//! port-fallback policy without requiring a live `Discovery`.
	use super::*;

	fn v4(s: &str) -> IpAddr {
		s.parse().unwrap()
	}

	#[test]
	fn tcp_default_port_yields_one_socket_per_addr() {
		let addrs = [v4("10.0.0.1"), v4("10.0.0.2")];
		let out = pick_tcp_sockets(&addrs, None);
		assert_eq!(
			out,
			vec![
				SocketAddr::new(addrs[0], 9000),
				SocketAddr::new(addrs[1], 9000),
			]
		);
	}

	#[test]
	fn tcp_explicit_9000_is_same_as_none() {
		let addrs = [v4("10.0.0.1")];
		assert_eq!(
			pick_tcp_sockets(&addrs, Some(9000)),
			pick_tcp_sockets(&addrs, None),
		);
	}

	#[test]
	fn tcp_non_standard_port_falls_back_to_9000() {
		let addrs = [v4("10.0.0.5")];
		let out = pick_tcp_sockets(&addrs, Some(9500));
		assert_eq!(
			out,
			vec![
				SocketAddr::new(addrs[0], 9500),
				SocketAddr::new(addrs[0], 9000),
			]
		);
	}

	#[test]
	fn tcp_empty_addrs_yields_empty_list() {
		let out = pick_tcp_sockets(&[], None);
		assert!(out.is_empty());
		let out = pick_tcp_sockets(&[], Some(9500));
		assert!(out.is_empty());
	}

	#[test]
	fn udp_default_port_yields_2018_and_2015_per_addr() {
		let addrs = [v4("10.0.0.1")];
		let out = pick_udp_sockets(&addrs, None);
		assert_eq!(
			out,
			vec![
				SocketAddr::new(addrs[0], 2018),
				SocketAddr::new(addrs[0], 2015),
			]
		);
	}

	#[test]
	fn udp_standard_ports_match_default() {
		let addrs = [v4("10.0.0.1")];
		assert_eq!(
			pick_udp_sockets(&addrs, Some(2015)),
			pick_udp_sockets(&addrs, None)
		);
		assert_eq!(
			pick_udp_sockets(&addrs, Some(2018)),
			pick_udp_sockets(&addrs, None)
		);
	}

	#[test]
	fn udp_non_standard_port_tries_override_then_both_fallbacks() {
		let addrs = [v4("10.0.0.5")];
		let out = pick_udp_sockets(&addrs, Some(2020));
		assert_eq!(
			out,
			vec![
				SocketAddr::new(addrs[0], 2020),
				SocketAddr::new(addrs[0], 2015),
				SocketAddr::new(addrs[0], 2018),
			]
		);
	}

	#[test]
	fn udp_scales_with_addrs() {
		let addrs = [v4("10.0.0.1"), v4("10.0.0.2")];
		let out = pick_udp_sockets(&addrs, None);
		assert_eq!(out.len(), 4);
	}
}

#[cfg(test)]
mod bccamera_opt_tests {
	//! Pure struct-shape tests for the options + enums. These catch
	//! serde-rename regressions and the Default derive.
	use super::*;

	#[test]
	fn connection_protocol_default_is_tcp_udp() {
		// Camera-side default keeps the widest discovery reach on.
		let p: ConnectionProtocol = Default::default();
		assert!(matches!(p, ConnectionProtocol::TcpUdp));
	}

	#[test]
	fn bccamera_opt_constructs_and_debug_prints() {
		let opt = BcCameraOpt {
			name: "cam0".to_string(),
			channel_id: 0,
			addrs: vec!["127.0.0.1".parse().unwrap()],
			uid: Some("UID".to_string()),
			port: Some(9000),
			protocol: ConnectionProtocol::Tcp,
			discovery: resolution::DiscoveryMethods::Local,
			max_discovery_retries: 3,
			credentials: Credentials::new("admin".to_string(), None::<String>),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		};
		let dbg = format!("{opt:?}");
		assert!(dbg.contains("cam0"));
	}

	/// Pins the redaction contract: `BcCameraOpt`'s auto-derived
	/// `Debug` walks `credentials: Credentials`, which has a custom
	/// redacting impl. Any future refactor that swaps `Credentials`
	/// for an inline struct, derives `Debug` on a wrapper, or
	/// otherwise short-circuits the field-level Debug must update
	/// this test or fail the password-leak guarantee.
	#[test]
	fn bccamera_opt_debug_does_not_leak_password() {
		let opt = BcCameraOpt {
			name: "cam0".to_string(),
			channel_id: 0,
			addrs: vec!["127.0.0.1".parse().unwrap()],
			uid: Some("UID".to_string()),
			port: Some(9000),
			protocol: ConnectionProtocol::Tcp,
			discovery: resolution::DiscoveryMethods::Local,
			max_discovery_retries: 3,
			credentials: Credentials::new("admin".to_string(), Some("hunter2".to_string())),
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			debug: false,
		};
		let dbg = format!("{opt:?}");
		assert!(
			!dbg.contains("hunter2"),
			"Debug of BcCameraOpt must not include the password; got {dbg}"
		);
		assert!(dbg.contains("admin"), "username should still surface");
		assert!(dbg.contains("******"), "redaction marker should surface");
	}

	#[test]
	fn print_format_equality_and_copy() {
		let a = PrintFormat::Human;
		let b = a;
		assert_eq!(a, b);
		assert_ne!(PrintFormat::Human, PrintFormat::Xml);
	}

	#[test]
	fn connection_protocol_is_copy() {
		let a = ConnectionProtocol::Udp;
		let b = a;
		assert!(matches!(
			(a, b),
			(ConnectionProtocol::Udp, ConnectionProtocol::Udp)
		));
	}
}
