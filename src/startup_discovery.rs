//! Cameras that answer the LAN, adopted at startup without having been declared.
//!
//! Every camera this bridge has ever managed was named in the configuration file
//! first, because every path that reaches a camera is keyed on its UID: the wake
//! request is addressed to it, the register registry looks a camera up by it, the
//! cloud token is minted for it. `[startup_discovery]` is the one door that opens the
//! other way — ask the network who is there, and manage what answers. The operator
//! asked for exactly that, having spent long enough copying identifiers off camera
//! labels into a file.
//!
//! # What the scan does
//!
//! Broadcast the protocol's *any-camera* discovery request ([`UdpXml::C2dS`],
//! documented in `crates/core/src/bcudp/xml.rs` as "Discovery of any client", sent to
//! port 2015), listen for the configured window, and treat every answer that names a
//! camera as a camera: address = where the answer came from, credentials = the pair
//! written in the block, `discovery = "local"` — because the only thing an answer to a
//! LAN broadcast proves is that the camera is on this LAN, and `local` is also the one
//! scope that keeps the UID at home (see [`crate::config::uid_travels_beyond_local`]).
//!
//! The broadcast destinations and the socket are the ones the discovery code already
//! uses ([`broadcast_destinations`], [`broadcast_socket`], both taken from
//! `Discoverer::local`'s own helpers) rather than a second implementation of "how do I
//! reach my LAN". The framing helpers are the wake server's ([`encode_discovery`],
//! [`decode_discovery`], [`payload_uid`]), which already bounds the UID field a hostile
//! peer could stuff with kilobytes.
//!
//! # Why `C2D_S` and not the packet the project normally sends
//!
//! The request `Discoverer::local()` broadcasts is `C2D_C`, and it asks for *a UID*:
//! the packet's own documentation is "This will start a connection with any camera that
//! has this UID". With the field left empty it asks for the camera whose UID is "",
//! which is no camera. And the reply it would provoke cannot help anyway — `D2C_C_R`
//! carries a timer, a response code, a client id and a device id, and **no UID** (see
//! `D2cCr` in `crates/core/src/bcudp/xml.rs`), so even a helpful camera answering that
//! request would not say who it is. `C2D_S` is the only request in the model that does
//! not need a UID to be sent, which is the whole requirement here.
//!
//! # What is not established, and how the log handles it
//!
//! No capture in this repository shows a camera answering a UID-less request with a
//! packet that names it. `C2D_S` is modelled here and has never been *sent* by this
//! code — `grep C2D_S` reaches tests only — and `docs/wake-local-addressing.md` says of
//! the neighbouring request that a real `D2C_CR` "does not exist in the corpus". The
//! UIDs this bridge has demonstrably learned off the wire arrived camera-initiated on
//! the register port (`docs/cloud-interception.md`), which is a different path and is
//! not what this module listens on.
//!
//! So the scan reports, at the levels the project already uses for "this is what
//! happened, and it may not be what you hoped":
//!
//! * `info` — one line per adopted camera: UID tail and the address it answered from;
//! * `warn` — one line per camera it could not adopt for want of credentials, naming
//!   it and naming the key that is missing;
//! * `warn` — the window closed and nothing answered at all;
//! * `debug` — an answer that named nobody, and an answer already covered by a
//!   declared camera.
//!
//! Nothing is dropped without a line about it. A UID-less answer in particular is
//! counted: an operator who suspects their firmware does talk back should be able to
//! turn on `RUST_LOG=debug`, run the scan, and see *that* something answered, which is
//! a different diagnosis from "the LAN is empty".
//!
//! # Off means off
//!
//! With no `[startup_discovery]` table, or with `enable = false`, [`adopt`] returns the
//! configuration it was handed without touching it: no socket is opened, no datagram is
//! sent, no task is spawned. That is not only politeness — a LAN broadcast is a wake-up
//! call that a battery camera pays for in battery, so it happens when it was asked for
//! and never otherwise.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use bairelay_neolink_core::bc_protocol::{broadcast_destinations, broadcast_socket};
use bairelay_neolink_core::bcudp::xml::{C2dS, PortList, UdpXml};
use bairelay_neolink_core::redact::UidTail;
use bairelay_wake_server::packet::{decode_discovery, encode_discovery, payload_uid};
use tokio::net::UdpSocket;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{CameraConfig, Config, DiscoveryMethod, StartupDiscoveryConfig};

/// Port the any-camera request is documented to be broadcast to.
///
/// Only this port, not the pair `Discoverer::local()` uses: `2018` is where `C2D_C`
/// goes, and sending a request whose reply shape cannot name a camera to a second port
/// would buy nothing but a second port's worth of wake-up calls.
pub const DISCOVERY_PORT: u16 = 2015;

/// How many times the request is sent during the window.
///
/// Three, spaced by [`PROBE_SPACING`]: one datagram is enough for a camera that hears
/// it, and the repeats are for the switch or the Wi-Fi access point that drops the
/// first. The cadence is the discovery code's own (`RESEND_WAIT` in
/// `crates/core/src/bc_protocol/connection/discovery.rs`), so this scan is no chattier
/// on the wire than a connect attempt already is. It stops repeating once the burst is
/// spent: unlike a connect, there is no answer that tells us to stop asking, and
/// broadcasting for the whole window would be a wake-up storm aimed at every battery
/// camera on the LAN.
const PROBE_BURST: usize = 3;

/// Spacing between the repeats of the probe.
const PROBE_SPACING: Duration = Duration::from_millis(500);

/// Size of the receive buffer. A discovery packet is a small XML document — the
/// announcement path in `bcudp::announcement.rs` caps its body at 256 bytes — so
/// anything longer is either noise or something that will not decode, and a bigger
/// buffer only buys room to be confused in.
const READ_BUFFER: usize = 2048;

/// Characters of an answered UID that go into an adopted camera's name.
///
/// Sixteen, which is the UID without the 4-character firmware suffix a battery
/// camera's heartbeat appends. The suffix is not identity — it changes when the
/// firmware does — and a name that changed with it would re-name the camera in every
/// downstream consumer, which is precisely the thing a name has to survive. Truncating
/// to the base form also means the name is the same whether a camera spelled itself in
/// the 16-character or the 20-character form on the way in, which is what makes a
/// restart produce the same list as the start before it.
const NAME_UID_CHARS: usize = 16;

/// One camera's answer: who says they are, and where they said it from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
	/// The UID as the camera wrote it. Trimmed by [`merge`], kept verbatim otherwise.
	pub uid: String,
	/// Source address of the datagram. This becomes the adopted camera's `address`.
	pub from: SocketAddr,
}

/// A camera the scan added, paired with the answer it came from.
#[derive(Debug, Clone)]
pub struct Adopted {
	/// The answer this camera was built from.
	pub answer: Answer,
	/// The entry appended to `Config::cameras`.
	pub camera: CameraConfig,
}

/// What one merge decided. Pure data: nothing here is logged by [`merge`], so a test
/// can assert on the decision without capturing a log, and the reporting stays in one
/// place that a test can also drive.
#[derive(Debug, Default, Clone)]
pub struct Adoption {
	/// New cameras, in the order they were adopted.
	pub adopted: Vec<Adopted>,
	/// Answers that are already a camera: matched a declaration in the file, or a
	/// second form of a UID adopted earlier in this same run. Merged into one list
	/// because the outcome is one outcome — nothing was added.
	pub already_known: Vec<Answer>,
	/// Answers this run could not adopt because the block carries no credential pair.
	pub without_credentials: Vec<Answer>,
	/// Addresses that answered without naming a camera.
	pub uid_less: Vec<SocketAddr>,
}

impl Adoption {
	/// Whether the merge changes nothing at all.
	pub fn is_empty(&self) -> bool {
		self.adopted.is_empty()
			&& self.already_known.is_empty()
			&& self.without_credentials.is_empty()
			&& self.uid_less.is_empty()
	}
}

/// Do these two spellings name the same camera?
///
/// The rule is the project's, not a new one: a configured UID and a UID seen on the
/// wire match when either starts with the other. It is spelled out here, taking the
/// pair, because the merge needs the longest form of a UID it decides to adopt, which a
/// yes/no predicate cannot give back — but it is deliberately the same rule as
/// `DeclaredUids::is_declared` in `crates/wake-server/src/config.rs` and
/// `match_camera_by_uid` in `src/push_listener.rs`, both of which exist for the same
/// reason: the 20-character form comes out of a heartbeat and the 16-character form out
/// of a binary header, so an exact comparison would see one camera and adopt two.
///
/// `docs/wake-local-addressing.md` records that "which form of a uid counts as the same
/// camera" is a question the corpus does not settle. This answer is the conservative
/// one for the mode it is used in: merging costs nothing (the camera is managed either
/// way) while splitting would put two entries, two MQTT device records and two sets of
/// Home Assistant entities on one physical camera.
fn uid_forms_match(a: &str, b: &str) -> bool {
	a == b || a.starts_with(b) || b.starts_with(a)
}

/// Decide what to do with the answers a scan heard.
///
/// Pure: no clock, no socket, no logging. `configured` is the `[[cameras]]` the operator
/// wrote, and it always wins — a UID that matches one of those declarations is not
/// adopted, and the declaration keeps every field the operator gave it, because the
/// answer adds no information that the file does not already carry better.
///
/// The order is the one that matters for the warnings: a declared camera that answers is
/// *not* complained about (it is already managed, credentials and all), a UID already
/// adopted this run is not adopted twice, and only then does the question "do we have
/// anything to log into this thing with?" get asked — because that is the question whose
/// answer the operator has to see.
pub fn merge(
	configured: &[CameraConfig],
	block: &StartupDiscoveryConfig,
	answers: &[Answer],
) -> Adoption {
	let mut adoption = Adoption::default();

	// One deterministic order before anything else: answers arrive in whatever order
	// the packets did, and a camera list that changed order between two restarts would
	// give cameras different names on different days. Names and topics follow the list
	// order, so the sort is what makes a restart idempotent.
	let mut heard = answers.to_vec();
	heard.sort_by(|a, b| {
		a.uid
			.trim()
			.cmp(b.uid.trim())
			.then_with(|| a.from.cmp(&b.from))
	});

	let declared: Vec<&str> = configured
		.iter()
		.filter_map(|c| c.uid.as_deref())
		.map(str::trim)
		.filter(|u| !u.is_empty())
		.collect();
	let mut names: HashSet<String> = configured.iter().map(|c| c.name.clone()).collect();

	for answer in heard {
		let uid = answer.uid.trim();
		if uid.is_empty() {
			// Defensive: the scan does not build an answer without a UID. Counted
			// rather than skipped so a future caller that lets one through cannot
			// lose it silently.
			adoption.uid_less.push(answer.from);
			continue;
		}

		if declared.iter().any(|d| uid_forms_match(d, uid)) {
			adoption.already_known.push(answer);
			continue;
		}

		if let Some(existing) = adoption
			.adopted
			.iter()
			.position(|a| uid_forms_match(a.camera.uid.as_deref().unwrap_or_default(), uid))
		{
			// Same camera, other spelling. Keep the longer form: it is the one the
			// heartbeat carries, so it is the one the register will key on, and a wake
			// request addressed with it is the one that gets through.
			let current = adoption.adopted[existing]
				.camera
				.uid
				.as_deref()
				.unwrap_or_default()
				.to_string();
			if uid.len() > current.len() {
				adoption.adopted[existing].camera.uid = Some(uid.to_string());
				adoption.adopted[existing].answer.uid = uid.to_string();
			}
			adoption.already_known.push(answer);
			continue;
		}

		let Some((username, password)) = block.credentials() else {
			adoption.without_credentials.push(answer);
			continue;
		};

		let camera = adopted_camera(uid, answer.from, username, password, &mut names);
		adoption.adopted.push(Adopted { answer, camera });
	}

	adoption
}

/// Build the entry for an adopted camera.
///
/// The result is an ordinary `CameraConfig`, deliberately: from here on it is managed,
/// published over MQTT, woken and streamed by the same code as a declared camera, and a
/// discovered camera that behaved differently in any of those paths would be a second
/// camera system to reason about. `discovery = "local"` is the one field worth naming:
/// a broadcast answered from `addr` is evidence about *this* network, and `local` is the
/// scope that asks nothing of it — no relay hostname sees the UID, and the address the
/// camera answered from is already in hand.
fn adopted_camera(
	uid: &str,
	from: SocketAddr,
	username: &str,
	password: &str,
	names: &mut HashSet<String>,
) -> CameraConfig {
	let base = camera_name(uid);
	let mut name = base.clone();
	let mut suffix = 2;
	while !names.insert(name.clone()) {
		name = format!("{base}-{suffix}");
		suffix += 1;
	}
	CameraConfig {
		name,
		// The answer's source address and nothing else: no port (the camera's own
		// default applies, exactly as for a declared camera written as a bare IP), and
		// never an address that was not in the packet.
		address: Some(from.ip().to_string()),
		uid: Some(uid.to_string()),
		username: username.to_string(),
		password: Some(password.to_string()),
		discovery: DiscoveryMethod::Local,
		..CameraConfig::default()
	}
}

/// The name an adopted camera is given: its UID, with the prefix that says where it came
/// from.
///
/// Names end up in MQTT topics and in Home Assistant entity ids, and `validate_config`
/// refuses anything outside ASCII alphanumeric, `_` and `-` — so characters outside that
/// set are dropped rather than passed through. A UID is 16-20 alphanumeric characters in
/// every capture this project has, but it arrived from the network, and a camera that
/// answered with control characters in its UID still gets a name a person can use.
///
/// The consequence worth knowing before adopting anything: Home Assistant remembers
/// entity names longer than this file exists. A camera discovered once under
/// `discovered-9527000ABCDEF01` and later declared with `name = "doorbell"` keeps the
/// old entities and gains new ones. That is the price of not writing the identifier
/// down, and it is why the declared name always wins in [`merge`].
fn camera_name(uid: &str) -> String {
	let cleaned: String = uid
		.chars()
		.filter(char::is_ascii_alphanumeric)
		.take(NAME_UID_CHARS)
		.collect();
	if cleaned.is_empty() {
		// A UID with no usable character at all. The count suffix in
		// `adopted_camera` keeps such names unique.
		return "discovered".to_string();
	}
	format!("discovered-{cleaned}")
}

/// Say what the scan did.
///
/// Split out from [`merge`] so the decisions and their wording can be tested
/// separately, and so the level choices are in one place: adopted cameras and the empty
/// window are the two facts worth seeing at `info`/`warn`; everything else is `debug`.
/// The password is never part of any line — an adopted camera is described by its UID
/// tail and its address, which is also what [`UidTail`] exists for.
///
/// `undecodable` is the count of datagrams that arrived on this socket and were not
/// protocol packets. They are individually `debug` (any software on a LAN can send to a
/// port it was not written for), but the count goes on the summary line, so that
/// "nothing is out there" and "somebody answered and this code could not read it" remain
/// two different sentences. Nothing this module ignores is ignored without a number.
fn report(
	adoption: &Adoption,
	block: &StartupDiscoveryConfig,
	declared: usize,
	undecodable: usize,
) {
	for adopted in &adoption.adopted {
		info!(
			uid = %UidTail(adopted.answer.uid.trim()),
			addr = %adopted.answer.from.ip(),
			name = %adopted.camera.name,
			"startup discovery: camera answered the discovery broadcast and is now managed"
		);
	}

	let missing = block
		.missing_credentials()
		.unwrap_or("username and password");
	for answer in &adoption.without_credentials {
		warn!(
			uid = %UidTail(answer.uid.trim()),
			addr = %answer.from.ip(),
			missing,
			"startup discovery: a camera answered and was not adopted: [startup_discovery] has \
			 no {missing}, and the credentials of a camera this file does not name cannot be \
			 guessed. Add them to [startup_discovery] to adopt this camera on the next start"
		);
	}

	for answer in &adoption.already_known {
		// `debug`, not `warn`: the camera is managed, which is the outcome the
		// operator wanted. The count is in the summary line below.
		debug!(
			uid = %UidTail(answer.uid.trim()),
			addr = %answer.from.ip(),
			"startup discovery: answered, and already a camera this bridge manages \
			 (declared in the file, or adopted from another form of the same uid); not added twice"
		);
	}
	for from in &adoption.uid_less {
		debug!(
			addr = %from.ip(),
			"startup discovery: answered without naming a camera, so there is nothing to adopt \
			 by; this is what a firmware that replies to the any-camera request without \
			 identifying itself looks like from here"
		);
	}

	if adoption.is_empty() {
		warn!(
			wait_secs = block.wait_secs,
			cameras = declared,
			undecodable,
			"startup discovery: nothing answered the discovery broadcast within the window; \
			 continuing with the cameras that were declared. This is not a failure — no capture \
			 in this project shows a camera answering a request that does not name it, so a \
			 silent LAN is an expected outcome; declare the cameras instead"
		);
		return;
	}

	info!(
		adopted = adoption.adopted.len(),
		already_known = adoption.already_known.len(),
		without_credentials = adoption.without_credentials.len(),
		uid_less = adoption.uid_less.len(),
		undecodable,
		cameras = declared + adoption.adopted.len(),
		wait_secs = block.wait_secs,
		"startup discovery: window closed"
	);
}

/// What the window heard.
struct Scan {
	answers: Vec<Answer>,
	uid_less: Vec<SocketAddr>,
	/// Datagrams that arrived and did not decode as protocol packets. Kept as a count
	/// rather than a list: the interesting part is that somebody spoke, not who.
	noise: usize,
	/// The window was cut short by shutdown, so whatever was heard is not usable:
	/// the process is on its way out and no camera list should be rewritten on the way.
	aborted: bool,
}

/// Open a socket, build the request, and hand both to [`collect`].
///
/// Every way this can fail is a warning and an empty scan, never an error out of
/// `adopt`: the mode asks the network for a favour, and a host that cannot ask — a
/// container without the capability, an interface with no broadcast address — starts
/// from the cameras it was given instead of refusing to start.
async fn scan(wait_secs: u64, cancel: &CancellationToken) -> Scan {
	let socket = match broadcast_socket().await {
		Ok(socket) => socket,
		Err(e) => {
			warn!(
				error = %e,
				"startup discovery: could not open a broadcast socket, so nothing was asked of the \
				 network; starting with the declared cameras. Discovery needs a UDP socket bound \
				 in the range discovery uses, which a container without that capability cannot get"
			);
			return Scan {
				answers: vec![],
				uid_less: vec![],
				noise: 0,
				aborted: false,
			};
		}
	};
	let destinations = match broadcast_destinations(&[DISCOVERY_PORT]) {
		Ok(destinations) if !destinations.is_empty() => destinations,
		Ok(_) => {
			warn!(
				"startup discovery: this host has no broadcast address to send to, so nothing was \
				 asked of the network; starting with the declared cameras"
			);
			return Scan {
				answers: vec![],
				uid_less: vec![],
				noise: 0,
				aborted: false,
			};
		}
		Err(e) => {
			warn!(
				error = %e,
				"startup discovery: the broadcast addresses of this host could not be listed; \
				 starting with the declared cameras"
			);
			return Scan {
				answers: vec![],
				uid_less: vec![],
				noise: 0,
				aborted: false,
			};
		}
	};
	let port = socket.local_addr().map(|a| a.port()).unwrap_or(0);
	let probe = match probe_bytes(port) {
		Ok(probe) => probe,
		Err(e) => {
			warn!(error = %e, "startup discovery: the discovery request could not be encoded");
			return Scan {
				answers: vec![],
				uid_less: vec![],
				noise: 0,
				aborted: false,
			};
		}
	};

	collect(&socket, &destinations, &probe, wait_secs, cancel).await
}

/// Ask the LAN who is there, for as long as the block allows, on a socket someone
/// else chose.
///
/// The socket is a parameter rather than something this builds, because the two halves
/// of the scan are testable for different reasons: opening a broadcast socket needs a
/// network a test does not have, while the window — decode what arrives, keep what
/// names a camera, count what does not, stop on the deadline or on shutdown — needs
/// nothing but a socket a second task can write to. Passing an empty `destinations`
/// sends nothing at all, which is how the tests below feed the window a packet without
/// putting a broadcast on the operator's LAN.
async fn collect(
	socket: &UdpSocket,
	destinations: &[SocketAddr],
	probe: &[u8],
	wait_secs: u64,
	cancel: &CancellationToken,
) -> Scan {
	let mut answers: Vec<Answer> = Vec::new();
	let mut uid_less: Vec<SocketAddr> = Vec::new();
	let mut noise = 0usize;
	let mut send_errors = 0usize;
	let mut aborted = false;

	let mut resend = tokio::time::interval(PROBE_SPACING);
	resend.set_missed_tick_behavior(MissedTickBehavior::Delay);
	let mut sent = 0usize;
	let deadline = tokio::time::sleep(Duration::from_secs(wait_secs));
	tokio::pin!(deadline);
	let mut buf = [0u8; READ_BUFFER];

	loop {
		tokio::select! {
			biased;
			_ = &mut deadline => break,
			_ = cancel.cancelled() => { aborted = true; break }
			// The burst, then nothing: see `PROBE_BURST`.
			_ = resend.tick(), if sent < PROBE_BURST => {
				sent += 1;
				for destination in destinations {
					if let Err(e) = socket.send_to(probe, destination).await {
						send_errors += 1;
						debug!(
							destination = %destination,
							error = %e,
							"startup discovery: could not reach this broadcast address"
						);
					}
				}
			}
			received = socket.recv_from(&mut buf) => {
				match received {
					Ok((read, from)) => match decode_discovery(&buf[..read]) {
						Ok((_tid, payload)) => match payload_uid(&payload) {
							Some(uid) if !uid.trim().is_empty() => answers.push(Answer {
								uid: uid.to_string(),
								from,
							}),
							// A packet we can read, from somebody, that does not say
							// which camera it is: counted, reported at debug.
							_ => uid_less.push(from),
						},
						Err(e) => {
							noise += 1;
							debug!(
							addr = %from.ip(),
							error = ?e,
							"startup discovery: ignored a datagram that is not a discovery \
							 packet; anything on this port is unsolicited, so this is noise \
							 from elsewhere on the LAN more often than it is a camera"
							)
						}
					},
					Err(e) => {
						warn!(
							error = %e,
							"startup discovery: the listening socket failed; the window ends here"
						);
						break;
					}
				}
			}
		}
	}

	if send_errors > 0 {
		warn!(
			errors = send_errors,
			attempts = sent * destinations.len(),
			"startup discovery: some broadcast addresses could not be reached; a camera behind \
			 one of them cannot hear the request"
		);
	}

	Scan {
		answers,
		uid_less,
		noise,
		aborted,
	}
}

/// The any-camera request, framed. `tid` 0 because nothing is being correlated: the
/// scan does not track a transaction per camera, it listens to whatever arrives (the
/// same convention the discovery code uses for its un-correlated sends, e.g. the
/// `C2R_CFM` it fires at a relay).
fn probe_bytes(port: u16) -> Result<Vec<u8>, String> {
	let payload = UdpXml::C2dS(C2dS {
		// "The destination to reply to": our own port, so that whatever the camera
		// decides to send comes back to the socket that is listening, instead of to a
		// port nothing has bound.
		to: PortList {
			port: u32::from(port),
		},
	});
	encode_discovery(0, payload).map_err(|e| format!("{e:?}"))
}

/// Add the cameras that answer, and hand the configuration back.
///
/// Called once, on the startup path, before anything reads the camera list: the
/// orchestrator's map, the wake server's "which UIDs did this bridge declare" list and
/// the MQTT discovery payloads are all built from what this returns. It is `await`ed
/// rather than spawned precisely because those consumers cannot be built from a list
/// that is still being filled in.
///
/// The cost is bounded by `wait_secs`, which `validate_config` refuses above
/// [`crate::config::MAX_STARTUP_DISCOVERY_WAIT_SECS`], and shutdown cuts the window
/// short: a `ctrl-c` during the scan returns the configuration as it was rather than
/// delaying the exit by the rest of the window.
pub async fn adopt(config: Config, cancel: &CancellationToken) -> Config {
	let mut config = config;
	let Some(block) = config.startup_discovery_active().cloned() else {
		return config;
	};
	let declared = config.cameras.len();
	debug!(
		wait_secs = block.wait_secs,
		port = DISCOVERY_PORT,
		cameras = declared,
		"startup discovery: asking the LAN who is there"
	);

	let scan = scan(block.wait_secs, cancel).await;
	if scan.aborted {
		debug!(
			"startup discovery: shutdown was requested inside the window; nothing was adopted \
			 and the cameras are left exactly as the file declares them"
		);
		return config;
	}

	let mut adoption = merge(&config.cameras, &block, &scan.answers);
	// Answers that named nobody never become cameras, but they are part of what
	// happened, so they ride along into the report.
	adoption.uid_less.extend(scan.uid_less);
	report(&adoption, &block, declared, scan.noise);
	config
		.cameras
		.extend(adoption.adopted.into_iter().map(|a| a.camera));
	config
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::StartupDiscoveryConfig;

	/// A battery camera's long form: 16 characters of UID plus the 4-character
	/// firmware suffix the heartbeat carries.
	const LONG: &str = "9527000SYNTHLONG0001";
	/// The same camera as the binary header spells it.
	const SHORT: &str = "9527000SYNTHLONG";
	/// A camera the operator wrote in the file.
	const DECLARED: &str = "9527000SYNTHDECLARED";
	/// A second camera nobody declared.
	const OTHER: &str = "9527000SYNTHOTHER999";

	fn answer(uid: &str) -> Answer {
		Answer {
			uid: uid.to_string(),
			from: format!("192.168.1.{}:2015", 40 + uid.len())
				.parse()
				.unwrap(),
		}
	}

	fn configured(uid: &str) -> CameraConfig {
		CameraConfig {
			name: "front-door".into(),
			uid: Some(uid.into()),
			address: Some("192.168.1.50".into()),
			username: "operator".into(),
			password: Some("her-own-password".into()),
			..CameraConfig::default()
		}
	}

	fn block(username: Option<&str>, password: Option<&str>) -> StartupDiscoveryConfig {
		StartupDiscoveryConfig {
			enable: true,
			wait_secs: 5,
			username: username.map(str::to_string),
			password: password.map(str::to_string),
		}
	}

	/// The case the operator asked for: nothing in the file, a camera on the LAN.
	#[test]
	fn a_lone_answer_becomes_a_camera() {
		let adoption = merge(
			&[],
			&block(Some("admin"), Some("adopt-me")),
			&[answer(LONG)],
		);

		assert_eq!(adoption.adopted.len(), 1, "one answer, one camera");
		let cam = &adoption.adopted[0].camera;
		assert_eq!(cam.uid.as_deref(), Some(LONG), "the UID it answered with");
		assert_eq!(
			cam.address.as_deref(),
			Some("192.168.1.60"),
			"the address it answered from, and no other"
		);
		assert_eq!(cam.username, "admin");
		assert_eq!(cam.password.as_deref(), Some("adopt-me"));
		assert_eq!(
			cam.discovery,
			DiscoveryMethod::Local,
			"a LAN answer is evidence about the LAN, so nothing leaves it"
		);
		assert_eq!(
			cam.name, "discovered-9527000SYNTHLONG",
			"the name says where the camera came from, and stops at the UID's base form"
		);
		assert!(adoption.without_credentials.is_empty());
	}

	/// A camera that is in the file is not a discovery, even when it answers.
	#[test]
	fn a_declared_camera_is_never_added_twice_and_keeps_what_the_operator_wrote() {
		let declared = configured(DECLARED);
		let adoption = merge(
			&[declared.clone()],
			&block(Some("admin"), Some("adopt-me")),
			&[answer(DECLARED), answer(OTHER)],
		);

		assert_eq!(
			adoption.adopted.len(),
			1,
			"only the undeclared camera is added"
		);
		assert_eq!(adoption.adopted[0].camera.uid.as_deref(), Some(OTHER));
		assert_eq!(adoption.already_known.len(), 1, "and that is said out loud");
		assert_eq!(adoption.already_known[0].uid, DECLARED);
	}

	/// One camera spells itself two ways on the wire. Two entries would be two MQTT
	/// device records and two sets of Home Assistant entities on one doorbell.
	#[test]
	fn the_two_forms_of_one_uid_are_one_camera() {
		let adoption = merge(
			&[],
			&block(Some("admin"), Some("adopt-me")),
			&[answer(SHORT), answer(LONG)],
		);

		assert_eq!(
			adoption.adopted.len(),
			1,
			"16 and 20 characters, one camera"
		);
		assert_eq!(
			adoption.adopted[0].camera.uid.as_deref(),
			Some(LONG),
			"the long form wins: it is what the heartbeat carries, so it is what the \
			 register keys on and what a wake request has to be addressed with"
		);
		assert_eq!(adoption.already_known.len(), 1);
	}

	/// A restart must not re-name a camera it already adopted, or every MQTT device
	/// record and Home Assistant entity it published the first time round is orphaned.
	/// The name comes from the UID's base form, so which spelling answered — or whether
	/// the firmware grew a suffix since — cannot move it.
	#[test]
	fn the_name_survives_the_next_restart() {
		let block = block(Some("admin"), Some("adopt-me"));
		let first = merge(&[], &block, &[answer(LONG)]);
		let upgrade = merge(&[], &block, &[answer(SHORT)]);
		let both = merge(&[], &block, &[answer(LONG), answer(SHORT)]);

		assert_eq!(
			first.adopted[0].camera.name, upgrade.adopted[0].camera.name,
			"the long form answering now and the short form answering next week give one name"
		);
		assert_eq!(both.adopted[0].camera.name, first.adopted[0].camera.name);
		assert_eq!(
			both.adopted[0].camera.uid, first.adopted[0].camera.uid,
			"and the UID kept is the longest form either way"
		);
	}

	/// The order packets arrive in must not decide anything an operator can see.
	#[test]
	fn the_merge_does_not_care_which_answer_arrived_first() {
		let block = block(Some("admin"), Some("adopt-me"));
		let one = merge(&[], &block, &[answer(SHORT), answer(OTHER)]);
		let other = merge(&[], &block, &[answer(OTHER), answer(SHORT)]);

		let names = |a: &Adoption| -> Vec<String> {
			a.adopted.iter().map(|x| x.camera.name.clone()).collect()
		};
		assert_eq!(names(&one), names(&other), "same names, same order");
	}

	/// With nothing on the LAN, the declared list is handed on untouched — the
	/// "normal start" the mode has to keep doing.
	#[test]
	fn a_silent_window_changes_nothing() {
		let declared = vec![configured(DECLARED)];
		let adoption = merge(&declared, &block(Some("admin"), Some("adopt-me")), &[]);
		assert!(adoption.is_empty(), "no answers, no decisions to report");
	}

	/// A camera that answers while there is nothing to log into it with is named, in a
	/// warning, together with the key to write. This is the "never silently ignored"
	/// requirement, and the reason the pair is not refused at validation time.
	#[test]
	fn an_answer_without_credentials_is_refused_out_loud() {
		let block = block(Some("admin"), None);
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		let adoption = merge(&[], &block, &[answer(LONG)]);
		report(&adoption, &block, 0, 0);
		let text = captured.text();

		assert_eq!(adoption.without_credentials.len(), 1);
		assert!(adoption.adopted.is_empty(), "no credentials, no adoption");
		assert_eq!(
			text.matches("WARN").count(),
			1,
			"one line, for the one camera: {text}"
		);
		assert!(
			!text.contains(LONG),
			"the full UID is not printed, only enough of it to match up: {text}"
		);
		assert!(text.contains("NG0001"), "its tail is: {text}");
		assert!(
			text.contains("password"),
			"what is missing is named: {text}"
		);
		assert!(
			text.contains("[startup_discovery]"),
			"and where to write it: {text}"
		);
	}

	/// The whole point of the redaction rule: no log line, at any level this module
	/// writes, carries the password it was given.
	#[test]
	fn no_log_line_carries_a_secret() {
		let block = block(Some("admin"), Some("hunter2-super-secret"));
		let captured = crate::test_logs::capture_at(tracing::Level::DEBUG);
		let adoption = merge(
			&[configured(DECLARED)],
			&block,
			&[answer(LONG), answer(DECLARED)],
		);
		report(&adoption, &block, 1, 0);
		let text = captured.text();

		assert!(
			!text.contains("hunter2"),
			"the password never appears: {text}"
		);
		assert!(
			!text.contains("her-own-password"),
			"nor the declared one: {text}"
		);
		assert!(
			text.contains("already a camera this bridge manages"),
			"the declared camera's answer is reported at debug: {text}"
		);
		assert!(
			text.contains("camera answered the discovery broadcast and is now managed"),
			"and the adopted one at info: {text}"
		);
	}

	/// An empty LAN is a reported outcome, at `warn`, in words that say what to do
	/// instead — not a failure, and not silence.
	#[test]
	fn a_window_that_heard_nothing_says_so() {
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		report(&Adoption::default(), &block(Some("admin"), Some("adopt-me")), 2, 0);
		let text = captured.text();

		assert!(
			text.contains("nothing answered the discovery broadcast"),
			"{text}"
		);
		assert!(
			text.contains("not a failure"),
			"and it says what it is not: {text}"
		);

		// The two silences stay apart on one line: "the LAN is empty" and "somebody sent
		// datagrams this code could not read" are different diagnoses, and an operator
		// choosing between them should not have to turn on `debug` to tell them apart.
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		report(&Adoption::default(), &block(Some("admin"), Some("adopt-me")), 2, 3);
		let text = captured.text();
		assert!(
			text.contains("undecodable=3"),
			"what was ignored is counted, so that ignoring is not the same as not looking: \
			 {text}"
		);
	}

	/// A declared camera that answers must not be complained about: it has its own
	/// password, written by the operator, and the scan has nothing to add.
	#[test]
	fn a_declared_answer_does_not_complain_about_missing_credentials() {
		let block = block(None, None);
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		let adoption = merge(&[configured(DECLARED)], &block, &[answer(DECLARED)]);
		report(&adoption, &block, 1, 0);
		let text = captured.text();

		assert!(
			adoption.without_credentials.is_empty(),
			"it is already managed"
		);
		assert_eq!(adoption.already_known.len(), 1);
		assert!(text.is_empty(), "and nothing is warned about it: {text}");
	}

	/// A UID is ASCII alphanumerics in every capture this project has, but it came off
	/// the network, and the name it produces ends up in an MQTT topic that
	/// `validate_config` refuses to carry anything else.
	#[test]
	fn names_stay_inside_the_characters_a_topic_allows() {
		assert_eq!(
			camera_name("9527000AB/CD\tEF0123456789"),
			"discovered-9527000ABCDEF012",
			"strange characters are dropped, and the name stops at the UID's base form"
		);
		assert_eq!(
			camera_name("\u{1}\\/\0"),
			"discovered",
			"nothing usable is still a name"
		);

		let mut names = HashSet::new();
		let first = adopted_camera(
			"weird uid!",
			"10.0.0.1:2015".parse().unwrap(),
			"u",
			"p",
			&mut names,
		);
		let second = adopted_camera(
			"weird-uid",
			"10.0.0.2:2015".parse().unwrap(),
			"u",
			"p",
			&mut names,
		);
		assert_eq!(first.name, "discovered-weirduid");
		assert_eq!(
			second.name, "discovered-weirduid-2",
			"and two of them get two names"
		);
		for name in [first.name, second.name] {
			assert!(
				name.chars()
					.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
				"{name} would be refused by validate_config"
			);
		}
	}

	/// The off switch, at the top level: a configuration that did not ask for this
	/// comes back as the same value, with no socket opened and nothing broadcast. That
	/// is what "off by default" has to mean for this module to be safe to ship.
	#[tokio::test(flavor = "current_thread")]
	async fn a_file_that_did_not_ask_is_returned_untouched() {
		let token = CancellationToken::new();
		let mut asked_for_nothing = Config::default();
		asked_for_nothing.cameras = vec![configured(DECLARED)];

		let after = adopt(asked_for_nothing.clone(), &token).await;
		assert_eq!(
			after.cameras.len(),
			1,
			"no block: the camera list is what the file said"
		);

		let mut switched_off = asked_for_nothing.clone();
		switched_off.startup_discovery = Some(StartupDiscoveryConfig {
			enable: false,
			wait_secs: 60,
			username: Some("admin".into()),
			password: Some("anything".into()),
		});
		let after = adopt(switched_off, &token).await;
		assert_eq!(
			after.cameras.len(),
			1,
			"and `enable = false` is the same door"
		);
	}

	/// The two rules the module relies on about UID spelling, pinned where they are
	/// written rather than in a comment.
	#[test]
	fn the_prefix_rule_works_in_both_directions() {
		assert!(uid_forms_match(LONG, SHORT), "long contains short");
		assert!(uid_forms_match(SHORT, LONG), "and the other way round");
		assert!(uid_forms_match(LONG, LONG), "the same spelling");
		assert!(
			!uid_forms_match(DECLARED, OTHER),
			"two cameras whose UIDs merely share a prefix root are not one camera"
		);
	}

	/// The window itself, on a socket nobody broadcast to. `merge` is pure and cannot
	/// see this: whether a datagram turns into an answer, whether a datagram that is not
	/// the protocol is survived rather than fatal, and whether one that decodes but names
	/// no camera is counted instead of vanishing, all live in `collect`. An empty
	/// destination list puts nothing on the operator's LAN while the window runs for
	/// real, deadline included.
	#[tokio::test(flavor = "current_thread")]
	async fn a_packet_written_to_the_socket_becomes_an_answer() {
		use bairelay_neolink_core::bcudp::xml::D2mQ;

		let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let camera = UdpSocket::bind("127.0.0.1:0").await.unwrap();

		// A battery camera asking the middleman where to register: of everything the
		// decoder accepts, the packet a camera of this family really sends on a LAN,
		// which is also the reason `payload_uid` knows the variant at all.
		let announcing = encode_discovery(
			7,
			UdpXml::D2mQ(D2mQ {
				uid: LONG.into(),
				revision: Some(2),
			}),
		)
		.unwrap();
		camera.send_to(&announcing, addr).await.unwrap();
		// Something else on the LAN, on a port this socket happens to own.
		camera.send_to(b"not the protocol", addr).await.unwrap();
		// And a protocol packet that does not say which camera it is.
		let silent = encode_discovery(
			8,
			UdpXml::C2dS(C2dS {
				to: PortList { port: 9 },
			}),
		)
		.unwrap();
		camera.send_to(&silent, addr).await.unwrap();

		let token = CancellationToken::new();
		let scan = collect(&listener, &[], &announcing, 1, &token).await;

		assert!(!scan.aborted, "the window ran out on its own");
		assert_eq!(scan.answers.len(), 1, "one packet named a camera");
		assert_eq!(scan.answers[0].uid, LONG);
		assert_eq!(
			scan.answers[0].from.ip().to_string(),
			"127.0.0.1",
			"the address kept is the one that answered, not one that was guessed"
		);
		assert_eq!(
			scan.uid_less.len(),
			1,
			"the readable packet that named nothing is counted, and the noise is not"
		);
		assert_eq!(
			scan.noise, 1,
			"the noise is counted too, and reaches the summary line as a number"
		);
	}

	/// A shutdown during the window is not a discovery: `aborted` is what `adopt` acts
	/// on, and the minute the file asked to wait must not hold the process for a second
	/// longer than it has to.
	#[tokio::test(flavor = "current_thread")]
	async fn shutdown_ends_the_window_before_the_deadline() {
		let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let token = CancellationToken::new();
		let closing = token.clone();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(50)).await;
			closing.cancel();
		});

		let began = tokio::time::Instant::now();
		let scan = collect(&listener, &[], b"", 60, &token).await;

		assert!(scan.aborted, "the shutdown is the news, not what was heard");
		assert!(scan.answers.is_empty());
		assert!(
			began.elapsed() < Duration::from_secs(5),
			"the window was cut, not waited out: {:?}",
			began.elapsed()
		);
	}
}
