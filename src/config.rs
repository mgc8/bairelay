use bairelay_mqtt::discovery::Feature;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// ── Enums ──────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TlsClientAuth {
	#[default]
	None,
	Request,
	/// Bairelay accepts both `"require"` (the bairelay/clap-style spelling)
	/// and `"required"` (neolink's spelling) for drop-in compat.
	#[serde(alias = "required")]
	Require,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StreamConfig {
	None,
	#[default]
	#[serde(alias = "both")]
	All,
	Main,
	Sub,
	Extern,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryMethod {
	/// Do not search for the camera at all: only a configured address works, and a
	/// failure to reach it is reported instead of turning into a hunt. The only scope
	/// that cannot put a discovery packet on the network, which is why the add-on
	/// form offers it next to `local` and nothing else.
	None,
	Local,
	Remote,
	Map,
	#[default]
	Relay,
	Cellular,
	/// Account ("cloud") camera — bound to a Reolink account and reachable only
	/// via the sigV3 login. Requires top-level `cloud_account`/`cloud_password`.
	Cloud,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MaxEncryption {
	None,
	#[default]
	Aes,
	BcEncrypt,
}

// ── Top-level Config ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
	#[serde(rename = "bind", default = "default_bind_addr")]
	pub bind_addr: String,

	#[serde(default = "default_bind_port")]
	pub bind_port: u16,

	/// Path to a single PEM file containing the server's cert chain and
	/// private key (concatenated). Neolink-compat. When set, bairelay
	/// spawns a TLS listener on `tls_bind_port`.
	#[serde(default)]
	pub certificate: Option<String>,

	/// Client-cert mode for the TLS listener. Neolink-compat: accepts
	/// `"none"` / `"request"` / `"require"`, plus `"required"` as a serde
	/// alias for `Require`. Has no effect when `certificate` is unset.
	#[serde(default)]
	pub tls_client_auth: TlsClientAuth,

	/// TCP port for the TLS listener. Bairelay extension (neolink runs
	/// TLS on the same port and replaces plain). Default 8555 when
	/// `certificate` is set; ignored otherwise.
	#[serde(default)]
	pub tls_bind_port: Option<u16>,

	/// PEM bundle containing CA cert(s) used to verify client certs in
	/// `tls_client_auth = "request" | "require"` modes. Bairelay
	/// extension; required when `tls_client_auth` is non-`none`.
	#[serde(default)]
	pub tls_client_ca: Option<String>,

	#[serde(default)]
	pub users: Vec<UserConfig>,

	#[serde(default)]
	pub mqtt: Option<MqttServerConfig>,

	/// Optional `[wake_server]` block. Disabled by default; when `enable =
	/// true` the orchestrator spawns the local BcUdp wake server alongside
	/// RTSP and MQTT. Operators must also redirect DNS for
	/// `p2p*.reolink.com` to the bairelay box for cameras to register.
	#[serde(default)]
	pub wake_server: Option<bairelay_wake_server::WakeServerConfig>,

	/// Optional `[push_listener]` block. Disabled by default. When enabled,
	/// bairelay binds a TCP listener on `bind_addr:bind_port` and treats
	/// every incoming connection from a registered camera's IP as a
	/// motion event — fires `status/motion=on` and acquires a
	/// `motion_wake_hold_secs` wake-lock so the connect loop reconnects.
	/// Pairs with a DNS hijack of `pushx.reolink.com` → bairelay's IP at
	/// the operator's resolver. See `docs/cloud-interception.md` § Part II.
	#[serde(default)]
	pub push_listener: Option<PushListenerConfig>,

	/// Optional `[webhook]` block. Disabled by default. When enabled,
	/// bairelay serves `POST /hook/<token>` for cameras with
	/// `webhook = true`, which report wake reasons and alarms there.
	#[serde(default)]
	pub webhook: Option<WebhookConfig>,

	/// Optional `[startup_discovery]` block. Disabled by default. When enabled,
	/// the cameras that answer the LAN discovery broadcast are added to
	/// `cameras` once at startup, without having been declared — see
	/// [`StartupDiscoveryConfig`].
	#[serde(default)]
	pub startup_discovery: Option<StartupDiscoveryConfig>,

	/// Neolink-compat: tokio-console debugging knob. Bairelay does not
	/// integrate `tokio-console`; verbosity is driven by `RUST_LOG`
	/// (or `-v` / `-vv` / `-vvv` on the CLI). Accepted so old configs
	/// parse cleanly; produces a one-line warning at startup.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokio_console: Option<bool>,

	/// Grace window in seconds before an idle `StreamSource` is pruned by
	/// the watchdog. `0` restores the legacy instantaneous-prune behaviour
	/// shipped pre-Phase-2G; the default `30` smooths rapid RTSP
	/// disconnect/reconnect so clients don't pay a fresh `start_video`
	/// handshake on every reconnect. See `docs/architecture.md`.
	///
	/// **Invariant: keep this ≤ each camera's `idle_disconnect_timeout_secs`.**
	/// Otherwise an idle `StreamSource` outlives the underlying Baichuan
	/// session — a fast reconnect that lands during the gap finds a cached
	/// source pointing at a disconnected camera and pays a stale-broadcast
	/// then cold-wake penalty before recovering. The 30 s default sits
	/// strictly below the 45 s `idle_disconnect_timeout_secs` default by
	/// design. If an operator inverts the relationship,
	/// [`resolve_idle_disconnect_timeout`] silently clamps the per-camera
	/// value to `prune_grace + 15 s` and
	/// [`warn_idle_timeout_below_prune_floor`] logs a one-line warning at
	/// startup so the override is visible.
	///
	/// The watchdog sweeps every 30 s, so an idle source is actually
	/// dropped between `grace` and `grace + 30 s` after its last
	/// subscriber leaves. Don't expect sub-sweep precision.
	#[serde(default = "default_prune_grace_secs")]
	pub stream_prune_grace_secs: u64,

	/// Reolink account e-mail, used to mint sigV3 token bundles for cameras with
	/// `discovery = "cloud"`. Document-root level; shared by all cloud cameras.
	#[serde(default)]
	pub cloud_account: Option<String>,

	/// Reolink account password, paired with `cloud_account`.
	#[serde(default)]
	pub cloud_password: Option<String>,

	pub cameras: Vec<CameraConfig>,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			bind_addr: default_bind_addr(),
			bind_port: default_bind_port(),
			certificate: None,
			tls_client_auth: TlsClientAuth::default(),
			tls_bind_port: None,
			tls_client_ca: None,
			users: Vec::new(),
			mqtt: None,
			wake_server: None,
			push_listener: None,
			webhook: None,
			startup_discovery: None,
			tokio_console: None,
			stream_prune_grace_secs: default_prune_grace_secs(),
			cloud_account: None,
			cloud_password: None,
			cameras: Vec::new(),
		}
	}
}

impl Config {
	/// The `[startup_discovery]` block, but only when it is switched on.
	///
	/// `None` covers both "the file has no such table" and "the table says
	/// `enable = false`", because to the startup path those are the same request: do
	/// not go looking. Every consumer asks this instead of matching on the field, so
	/// the off switch lives in one place and an operator turning it off cannot leave
	/// a half-armed scan behind — the rest of the table (a wait, a pair of
	/// credentials) is inert the moment `enable` is not `true`.
	pub fn startup_discovery_active(&self) -> Option<&StartupDiscoveryConfig> {
		self.startup_discovery.as_ref().filter(|s| s.enable)
	}
}

// ── PushListenerConfig ─────────────────────────────────────────────────

/// TCP listener that observes the camera's motion-time HTTPS
/// connect attempt to `pushx.reolink.com` and treats it as a motion
/// event. Operator-side DNS hijack for `pushx.reolink.com` →
/// `push_listener_addr` is the prerequisite. Listener doesn't
/// terminate TLS — the connection's existence is the signal; we
/// close immediately.
///
/// Field names are deliberately prefixed (`push_listener_addr` /
/// `push_listener_port`) rather than `bind_addr` / `bind_port` so an
/// operator pasting them into the wrong scope can't silently shadow
/// the top-level RTSP `bind_addr` and so the placement scanner has
/// no ambiguous keys to special-case.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PushListenerConfig {
	#[serde(default)]
	pub enable: bool,

	/// Bind address. `None` falls back (in priority order) to the
	/// `[wake_server]` bind, then the top-level `bind_addr`. Override
	/// when the wake-server box runs another HTTPS service (HA, nginx).
	#[serde(default)]
	pub push_listener_addr: Option<String>,

	/// Bind port. Default `443` matches `pushx.reolink.com`'s port —
	/// override when something else owns 443 on this host.
	#[serde(default = "default_push_listener_port")]
	pub push_listener_port: u16,

	/// How long a single push event holds the wake-lock. The connect
	/// loop has this long to establish a Baichuan session before the
	/// camera goes back to sleep. Default 30 s; lower values miss
	/// slow reconnects, higher values hold the camera awake longer.
	#[serde(default = "default_motion_wake_hold_secs")]
	pub motion_wake_hold_secs: f64,
}

impl Default for PushListenerConfig {
	fn default() -> Self {
		Self {
			enable: false,
			push_listener_addr: None,
			push_listener_port: default_push_listener_port(),
			motion_wake_hold_secs: default_motion_wake_hold_secs(),
		}
	}
}

fn default_push_listener_port() -> u16 {
	443
}

// ── WebhookConfig ──────────────────────────────────────────────────────

/// Default `[webhook] webhook_port`.
pub const DEFAULT_WEBHOOK_PORT: u16 = 8780;

/// HTTP listener that receives the camera's HaCfg webhook POSTs. Keys
/// are prefixed (`webhook_addr` / `webhook_port`) for the same reason
/// as [`PushListenerConfig`]'s.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
	#[serde(default)]
	pub enable: bool,

	/// Bind address. `None` falls back to the top-level `bind_addr`.
	#[serde(default)]
	pub webhook_addr: Option<String>,

	/// Bind port.
	#[serde(default = "default_webhook_port")]
	pub webhook_port: u16,

	/// `host:port` the cameras are told to POST to, when the bind
	/// address is not what they can reach (NAT, reverse proxy, a
	/// wildcard bind). Parsed by [`parse_webhook_advertise`].
	#[serde(default)]
	pub webhook_advertise: Option<String>,

	/// How long one webhook wake or alarm edge holds the camera's wake
	/// lock, so the connect loop reaches it while it is awake.
	#[serde(default = "default_motion_wake_hold_secs")]
	pub motion_wake_hold_secs: f64,
}

impl Default for WebhookConfig {
	fn default() -> Self {
		Self {
			enable: false,
			webhook_addr: None,
			webhook_port: DEFAULT_WEBHOOK_PORT,
			webhook_advertise: None,
			motion_wake_hold_secs: default_motion_wake_hold_secs(),
		}
	}
}

fn default_webhook_port() -> u16 {
	DEFAULT_WEBHOOK_PORT
}

/// A parsed `webhook_advertise`: an IP or hostname plus a non-zero port.
/// `Display` gives `host:port`, bracketing an IPv6 host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookAdvertise {
	pub host: String,
	pub port: u16,
}

impl std::fmt::Display for WebhookAdvertise {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.host.contains(':') {
			write!(f, "[{}]:{}", self.host, self.port)
		} else {
			write!(f, "{}:{}", self.host, self.port)
		}
	}
}

impl WebhookAdvertise {
	/// Whether the host is loopback: an IP literal in a loopback range,
	/// or `localhost`.
	pub fn is_loopback(&self) -> bool {
		match self.host.parse::<std::net::IpAddr>() {
			Ok(ip) => ip.to_canonical().is_loopback(),
			Err(_) => self
				.host
				.strip_suffix('.')
				.unwrap_or(&self.host)
				.eq_ignore_ascii_case("localhost"),
		}
	}
}

/// Warn when `[webhook] webhook_advertise` names a loopback host: only
/// a camera on this host (a simulator) can reach it. Called once at
/// startup and by `check-config`, alongside [`warn_wire_debug_enabled`].
/// Whether a camera's configured discovery scope puts its uid on the network beyond
/// this machine's own broadcast domain.
///
/// `local` is the only scope that sends nothing outside: it addresses the LAN and the
/// camera's own configured address. Everything above it (`remote`, `map`, `relay`,
/// `cellular`, `cloud`) asks the relay hostnames where the uid lives, and the uid is the
/// content of that question.
fn uid_travels_beyond_local(method: DiscoveryMethod) -> bool {
	!matches!(method, DiscoveryMethod::None | DiscoveryMethod::Local)
}

/// Say once per camera, at startup, what leaving the relay scope does to the camera's
/// uid. Said at startup because it is a property of the network, not of an event: the
/// uid is handed to twelve hostnames on every lookup, and which of them end up on this
/// LAN and which on somebody else's server is decided by a DNS this project does not
/// own and cannot see.
///
/// Called once at startup alongside [`warn_neolink_compat_fields`], and by
/// `check-config`, so the same words are visible without starting the bridge.
///
/// The message itself is quoted verbatim in `docs/settings.md`, under "The twelve relay
/// hostnames", so that the journal and the documentation cannot drift apart. If you edit
/// one, edit the other in the same commit.
pub fn warn_uid_beyond_local(config: &Config) {
	for cam in &config.cameras {
		let Some(uid) = cam.uid.as_deref() else { continue };
		if !uid_travels_beyond_local(cam.discovery) {
			continue;
		}
		tracing::warn!(
			name = %cam.name,
			uid_tail = %&uid[uid.len().saturating_sub(6)..],
			discovery = ?cam.discovery,
			"camera uid is sent outside this network: a lookup asks the twelve relay \
			 hostnames (p2p.reolink.com and p2p1 through p2p11) where this uid is, and the \
			 same question goes to each of them because the lookup does not stop at the first \
			 answer. Which of those names land on this LAN is decided by the network's DNS, not by \
			 this project: check that list before describing this deployment as offline. `local` \
			 asks none of them, but without an IP-literal `address` it broadcasts only, which a \
			 sleeping battery camera cannot hear"
		);
	}
}

/// Say, once, that the startup scan is armed with nothing to adopt a camera with.
///
/// This is the half of the mode the operator cannot see by reading the file: the scan
/// will run, it may well hear something, and it will then refuse to act on what it
/// heard, because a camera this file does not name is a camera whose password nobody
/// knows. Without this line the outcome looks exactly like an empty network. With it,
/// the missing key is named before the first broadcast goes out — and the scan still
/// repeats the warning per camera it had to leave alone, because "something answered
/// and we did not use it" is the fact that has to survive into the log of that run.
///
/// A warning, not a refusal: the bridge starts and serves every declared camera
/// whether or not a scan can adopt anything, and a file that refused to load because
/// of a table whose only job is to add cameras would be a worse file than the one it
/// replaced.
pub fn warn_startup_discovery_without_credentials(config: &Config) {
	let Some(sd) = config.startup_discovery_active() else {
		return;
	};
	let Some(missing) = sd.missing_credentials() else {
		return;
	};
	tracing::warn!(
		missing = missing,
		wait_secs = sd.wait_secs,
		"[startup_discovery] is on but {missing} is not configured: a camera that answers the \
		 discovery broadcast cannot be adopted, because its credentials cannot be guessed. Write \
		 [startup_discovery] username and password to adopt the cameras that answer, or leave the \
		 table out if every camera is declared in [[cameras]]"
	);
}

pub fn warn_loopback_webhook_advertise(config: &Config) {
	let advertise = config
		.webhook
		.as_ref()
		.filter(|w| w.enable)
		.and_then(|w| w.webhook_advertise.as_deref())
		.and_then(|raw| parse_webhook_advertise(raw).ok());
	if let Some(advertise) = advertise.filter(WebhookAdvertise::is_loopback) {
		tracing::warn!(
			"config: [webhook] webhook_advertise {advertise} is a loopback address; only a camera on this host can reach it"
		);
	}
}

/// Parse a `webhook_advertise` value: `ip:port`, `hostname:port` or
/// `[ipv6]:port`, with a port above 0. Pure; no DNS.
pub fn parse_webhook_advertise(raw: &str) -> Result<WebhookAdvertise, String> {
	let parsed = crate::bc_opts::parse_address(raw)?;
	let port = match parsed.port {
		Some(0) => return Err("port must be greater than 0".into()),
		Some(port) => port,
		None => return Err("a port is required (host:port)".into()),
	};
	let host = match parsed.host {
		crate::bc_opts::AddressHost::Ip(ip) if ip.is_unspecified() || ip.is_multicast() => {
			return Err(format!("{ip} is not a unicast address cameras can reach"));
		}
		crate::bc_opts::AddressHost::Ip(ip) => ip.to_string(),
		crate::bc_opts::AddressHost::Name(name) => name,
	};
	Ok(WebhookAdvertise { host, port })
}

// ── StartupDiscoveryConfig ─────────────────────────────────────────────

/// Longest window `[startup_discovery]` will wait for an answer.
///
/// The number exists to bound the wait, not to offer it. The scan sits on the
/// startup path, before the RTSP listener and the camera tasks exist, so every
/// second of the window is a second in which no camera is reachable by anybody.
/// A window longer than this is a misconfiguration wearing the shape of a
/// preference: a camera that is going to answer a broadcast answers within the
/// first second or two, and one that is asleep will not answer at all.
pub const MAX_STARTUP_DISCOVERY_WAIT_SECS: u64 = 60;

/// `[startup_discovery]`: adopt the cameras that answer the LAN discovery
/// broadcast, without their UIDs having been written in this file.
///
/// Every other path in this bridge starts from a UID the operator wrote down. A
/// UID is what a wake request is addressed to, what the register registry keys on,
/// and what a cloud token is minted for, so "declare the camera" has always meant
/// "copy the number off the label". This block is the one place that instead asks
/// the LAN: at startup it broadcasts the protocol's any-camera discovery request
/// (`C2D_S`, see `crates/core/src/bcudp/xml.rs`), listens for `wait_secs`, and
/// takes whatever answers that names a camera as a camera — with the address it
/// answered from, the credentials named below, and `discovery = "local"`, because
/// the only thing the broadcast proves is that the camera is on this LAN.
///
/// What it deliberately does not do:
///
/// * It does not invent an address. The address adopted is the source address of
///   the answer, which is the one address this run has evidence for.
/// * It does not guess credentials. With no pair configured below, a camera that
///   answers is named in a warning and left alone — see [`StartupDiscoveryConfig::missing_credentials`].
/// * It does not duplicate a declaration. An answer whose UID matches a camera
///   this file already declares, in either of the two forms the firmware uses, is
///   not added a second time, and the declared camera keeps every field the
///   operator wrote for it.
///
/// What is **not** established, and the reason this is opt-in and a timeout is a
/// warning rather than a failure: no capture in this repository contains a camera
/// answering a UID-less request with a packet that names itself. `C2D_S` is
/// modelled here — documented as "Discovery of any client", broadcast to port 2015
/// — and has never been sent by this code; `grep C2D_S` reaches tests only. The
/// UIDs this bridge has actually learned off the wire arrived camera-initiated on
/// the register port, which is a different path (see `docs/wake-local-addressing.md`
/// and `docs/cloud-interception.md`). A window that finds nothing is therefore an
/// expected outcome, and the log says so in those words instead of failing.
///
/// Opt-in for a second reason too: a LAN broadcast is a wake-up call. A sleeping
/// battery camera that hears it spends battery answering, and a bridge that
/// broadcasts on every start without being asked is the last thing the operator of
/// a battery doorbell wants on their network.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StartupDiscoveryConfig {
	/// Off unless this is written `true`. Absent, or `false`, and the startup path
	/// is byte-for-byte what it was before this block existed: no socket is
	/// opened, nothing is broadcast, and the camera list is exactly `[[cameras]]`.
	#[serde(default)]
	pub enable: bool,

	/// How long to listen for answers, in seconds. Bounded above by
	/// [`MAX_STARTUP_DISCOVERY_WAIT_SECS`] and refused outside `1..=` that,
	/// because a window of `0` would mean "ask and never listen" and a window of
	/// an hour would mean "the bridge does not start".
	#[serde(default = "default_startup_discovery_wait_secs")]
	pub wait_secs: u64,

	/// Username adopted cameras are given. Adoption needs a pair: see
	/// [`StartupDiscoveryConfig::credentials`].
	#[serde(default)]
	pub username: Option<String>,

	/// Password adopted cameras are given. Adopted cameras are ordinary cameras
	/// from here on, so they need a password like any other: it is what the
	/// login handshake sends. Never logged — the startup log lines name a UID
	/// tail and an address, never a secret.
	#[serde(default)]
	pub password: Option<String>,
}

impl Default for StartupDiscoveryConfig {
	fn default() -> Self {
		Self {
			enable: false,
			wait_secs: default_startup_discovery_wait_secs(),
			username: None,
			password: None,
		}
	}
}

impl StartupDiscoveryConfig {
	/// The credentials an answer would be adopted with, or `None` when the
	/// operator has not written a complete pair.
	///
	/// There is no fallback here, and that is the point. A discovered camera is a
	/// camera this file does not name, so there is no per-camera password to borrow
	/// and no "obvious" account to try: the only pair this bridge may claim for it
	/// is the one the operator wrote for exactly this purpose. Falling back to the
	/// first declared camera's credentials would be a guess dressed as a default —
	/// and it would be a guess that logs into one camera under the identity of
	/// another.
	pub fn credentials(&self) -> Option<(&str, &str)> {
		let username = self.username.as_deref().filter(|u| !u.trim().is_empty())?;
		let password = self.password.as_deref().filter(|p| !p.is_empty())?;
		Some((username, password))
	}

	/// Which half of the credential pair is missing, phrased for a warning that
	/// has to say what to write. `None` means a pair is there to adopt with.
	///
	/// The two tests are the same two filters [`StartupDiscoveryConfig::credentials`]
	/// applies, written the same way: what counts as a filled half has to be one
	/// definition, or the warning can name a missing half that the adoption then
	/// accepts, or the reverse.
	pub fn missing_credentials(&self) -> Option<&'static str> {
		let username = self.username.as_deref().filter(|u| !u.trim().is_empty());
		let password = self.password.as_deref().filter(|p| !p.is_empty());
		match (username, password) {
			(Some(_), Some(_)) => None,
			(None, None) => Some("username and password"),
			(None, Some(_)) => Some("username"),
			(Some(_), None) => Some("password"),
		}
	}
}

// ── UserConfig ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
	#[serde(alias = "username")]
	pub name: String,
	#[serde(alias = "password", default)]
	pub pass: String,
}

// ── MqttServerConfig ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttServerConfig {
	#[serde(alias = "server")]
	pub broker_addr: String,

	#[serde(default = "default_mqtt_port")]
	pub port: u16,

	#[serde(default)]
	pub credentials: Option<(String, String)>,

	/// TLS to the broker. `Some(true)` with `ca` unset verifies the
	/// broker against the OS trust store; `ca` alone also enables TLS.
	/// `Option` so an explicit `tls = false` next to `ca` can be
	/// rejected rather than silently overridden.
	#[serde(default)]
	pub tls: Option<bool>,

	#[serde(default)]
	pub ca: Option<String>,

	#[serde(default)]
	pub client_auth: Option<(String, String)>,

	/// Root segment for all bairelay-owned MQTT topics
	/// (`{topic_prefix}/{cam}/status`, etc.) and the driver of HA
	/// discovery identifier/unique_id strings. Default `"bairelay"`.
	/// Set to `"neolink"` for drop-in migration from an existing
	/// neolink deployment — every topic path and HA identifier flips
	/// in lockstep. See `docs/architecture.md`.
	#[serde(default = "default_topic_prefix")]
	pub topic_prefix: String,

	/// Home Assistant MQTT discovery settings. Presence of the
	/// `[mqtt.discovery]` table enables discovery; absence disables
	/// it entirely. Matches neolink's shape. See
	/// `docs/architecture.md`.
	#[serde(default)]
	pub discovery: Option<MqttDiscoveryConfig>,
}

impl Default for MqttServerConfig {
	fn default() -> Self {
		Self {
			broker_addr: String::new(),
			port: default_mqtt_port(),
			credentials: None,
			tls: None,
			ca: None,
			client_auth: None,
			topic_prefix: default_topic_prefix(),
			discovery: None,
		}
	}
}

// ── MqttDiscoveryConfig ────────────────────────────────────────────────

/// Configuration for publishing Home Assistant MQTT discovery
/// payloads. Operator must commit to a `topic` (HA's discovery
/// prefix — conventionally `"homeassistant"` but not hard-coded) and
/// may narrow `features` from the default full set.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttDiscoveryConfig {
	/// HA discovery root topic. Required; no default. Must be a
	/// single segment — no embedded slashes.
	pub topic: String,

	/// Set of HA discovery feature families to emit. Defaults to
	/// the full [`Feature::ALL`] set. Per-camera `enable_*` flags
	/// and live capability detection further gate what actually
	/// goes on the wire.
	#[serde(default = "default_features")]
	pub features: HashSet<Feature>,
}

fn default_features() -> HashSet<Feature> {
	Feature::ALL.iter().copied().collect()
}

// ── CameraConfig ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CameraConfig {
	pub name: String,

	#[serde(default)]
	pub address: Option<String>,

	#[serde(default)]
	pub uid: Option<String>,

	pub username: String,

	#[serde(alias = "pass", default)]
	pub password: Option<String>,

	/// Reolink account credentials for `discovery = "cloud"` cameras. Not parsed
	/// from the `[[cameras]]` table — copied from the document-root
	/// `cloud_account` / `cloud_password` by [`parse_config`].
	#[serde(skip)]
	pub cloud_account: Option<String>,
	#[serde(skip)]
	pub cloud_password: Option<String>,
	/// Host's MFA trust token from a `cloud-authorise` bootstrap. Not parsed
	/// from TOML — loaded from the sibling `config-cloud-auth.json` by
	/// [`apply_cloud_auth`] and consulted only for `discovery = "cloud"`.
	#[serde(skip)]
	pub cloud_mfa_trust_token: Option<String>,
	/// Host's refresh token from the same bootstrap (refresh-grant fallback).
	#[serde(skip)]
	pub cloud_refresh_token: Option<String>,

	#[serde(default, alias = "channel")]
	pub channel_id: u8,

	#[serde(default)]
	pub stream: StreamConfig,

	#[serde(default)]
	pub discovery: DiscoveryMethod,

	#[serde(default)]
	pub max_encryption: MaxEncryption,

	#[serde(default, alias = "idle", alias = "idle_disc")]
	pub idle_disconnect: bool,
	/// How often the session probe pokes the camera to prove the session is
	/// still alive. **5 s by default; `0` removes the probe entirely.**
	///
	/// The probe is not a nicety. It is the only thing in this bridge that
	/// notices a session whose camera has left the network: the socket reader
	/// returns on EOF without reporting anything, no command on that socket
	/// carries a timeout, and `SO_KEEPALIVE` is never set anywhere in the tree.
	/// Without a probe, a camera that drops off Wi-Fi without sending a FIN
	/// keeps its session open for ever: the connected-session call never
	/// returns, so the outer reconnect loop and its backoff are never reached,
	/// MQTT keeps publishing `connected`, presence keeps publishing reachable,
	/// `bc_camera()` hands out a dead camera, and `control/wakeup` answers
	/// success without touching the device.
	///
	/// Detection is not one tick: the probe loop tolerates five consecutive
	/// failures before it gives up (`KEEPALIVE_MAX_FAILURES` in
	/// `src/camera.rs`, a private constant so named in prose rather than linked),
	/// so a session is declared dead after roughly `probe_session_secs` x 5
	/// seconds.
	///
	/// `0` is refused together with `idle_disconnect = false`, because the two
	/// together leave nothing at all to end a session. With
	/// `idle_disconnect = true` the bound that remains is the grace period
	/// (`idle_disconnect_timeout_secs`, 45 s by default, see
	/// [`resolve_idle_disconnect_timeout`]), so the session still closes, only
	/// later, and only on the idle path: a camera that vanishes *while
	/// something is watching* is not covered by that bound either.
	#[serde(default = "default_probe_session_secs")]
	pub probe_session_secs: u64,

	/// After a contact attempt gives up, wait this many seconds and try
	/// **once** more, then stop and wait for a reason to talk to the camera.
	/// `0` (the default) keeps today's behaviour: no further attempt.
	///
	/// **This only does anything in the parked regime**, that is with
	/// `idle_disconnect = true`. Without it the connect loop already retries
	/// for ever on its own backoff, and this key changes nothing there: a
	/// startup warning says so rather than letting the key look broken.
	///
	/// The deferred attempt is deliberately narrower than the normal one. It is
	/// built by [`crate::bc_opts::build_retry_bc_opts`], which pins the
	/// protocol to TCP and drops the uid, so it cannot enter the UID discovery
	/// chain: no LAN broadcast, no remote lookup, no map, no relay, no wake
	/// burst. It dials only addresses this bridge can prove, which means a
	/// camera reachable only through the relay is not retried at all and the log
	/// says that in one line. The reason for the narrowness is the battery: a
	/// local broadcast is a possible wake-up, and a periodic timer that rings
	/// the camera's radio is exactly the cost this bridge exists to avoid.
	/// Anything a person asked for keeps its full reach; only the automatic
	/// path is restricted.
	#[serde(default = "default_retry_after_secs")]
	pub retry_after_secs: u64,

	/// When this camera announces its own wake-up, ask it once, at that moment,
	/// instead of waiting for the recording that follows to end. Off means the
	/// announcement is still logged, and nothing else happens.
	#[serde(default = "default_read_on_wake")]
	pub read_on_wake: bool,

	/// How long the bridge waits for that session to appear, in seconds, holding
	/// the wake lock meanwhile. A camera that answers is released as soon as the
	/// session is up, so raising this only lengthens the wait of one that does not.
	#[serde(default = "default_read_hold_secs")]
	pub read_hold_secs: u64,

	/// Minimum spacing between two reads that got a session, in seconds. A read
	/// that got nothing is not charged to this window, because it cost nothing.
	#[serde(default = "default_read_interval_secs")]
	pub read_interval_secs: u64,

	/// Seconds to wait after the last wake lock drops before the camera
	/// is told to sleep. `None` uses the 45 s default. A `[cameras.pause]
	/// timeout = N` (Neolink compat) overrides this at startup.
	#[serde(default)]
	pub idle_disconnect_timeout_secs: Option<f64>,

	/// Seconds the motion-driven wake lock is held after a `MotionStop`
	/// event before being released. Spec §4.2 calls this the "configurable
	/// timeout after motion stops"; this knob exposes it. Default 30 s.
	/// Lower values make battery cameras sleep sooner after motion ends;
	/// raise to keep RTSP / preview warm for a few extra seconds.
	#[serde(default = "default_motion_wake_hold_secs")]
	pub motion_wake_hold_secs: f64,

	#[serde(default = "default_true", alias = "enable")]
	pub enabled: bool,

	#[serde(default)]
	pub mqtt: MqttConfig,

	#[serde(default)]
	pub pause: PauseConfig,

	/// Per-camera RTSP access control allowlist. Empty means "any
	/// authenticated user (or anonymous, when no auth is configured)".
	/// Task 24 will add validation that each entry matches a globally
	/// configured `[[users]]` name.
	#[serde(default)]
	pub permitted_users: Vec<String>,

	/// Wire-level protocol debugging. When `true`, the Baichuan codec
	/// dumps each decrypted control payload (login replies included) at
	/// trace level — set `RUST_LOG=info,bairelay_neolink_core::bc::de=trace`
	/// to see them. The leading `info,` is load-bearing: a bare
	/// `bairelay_neolink_core::bc::de=trace` is a single-target allowlist
	/// that silences every other line (a silent console), so keep a
	/// baseline level before the comma. The dumps contain credential
	/// hashes and camera UIDs; don't share them publicly. `verbose` is
	/// the neolink-compat alias.
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "verbose")]
	pub debug: Option<bool>,

	/// Arm the camera's HaCfg webhook so it reports wake reasons and
	/// alarms to the `[webhook]` listener. Requires `[webhook] enable`.
	#[serde(default)]
	pub webhook: bool,

	/// Declare that this camera can do two-way audio even though its
	/// `abilityInfo` does not list `two_way_audio`, so the talk path
	/// (`talk`, `audio-bridge`) is allowed for **this camera only**.
	///
	/// This is a firmware under-declaration, verified by `talk-ability`: the
	/// doorbell answers `<TalkAbility>` (msg 10) with `version 1.1`, `FDX`
	/// duplex and an `adpcm` 16000 Hz / 16 bit / 1024-sample-per-block
	/// configuration, while its 56-entry `abilityInfo` lists no talk ability
	/// at all. Default `false` keeps the ability gate exactly as strict as it
	/// has always been for every other camera — and setting it on a camera
	/// that genuinely lacks the microphone only makes the camera refuse the
	/// talk config, it does not make bairelay any less strict elsewhere.
	#[serde(default)]
	pub two_way_audio: bool,

	// ── Media pacer tuning ─────────────────────────────────────────────
	// Every decoded frame goes through a per-source pacer before it reaches
	// subscribers: it holds the frame, then re-emits it at the codec's own
	// rate (see `stream_source::media_pacer_task`). Each pacer has two
	// numbers. The *latency* is what the very first frame of a source waits
	// — the pre-buffer, and therefore the floor on how long ago the picture
	// a viewer sees is. The *lead cap* is how far the emit cursor may run
	// ahead of the wall clock while the camera bursts, which is the other
	// way latency grows. Both are deliberate delay: they buy the smooth
	// cadence that stops mpv from re-buffering every two seconds, and they
	// cost the seconds a live doorbell picture is late by. A go2rtc / WebRTC
	// consumer re-buffers on its own and wants much less of both, so the
	// defaults below — measured for mpv-over-RTSP — are per-camera keys.
	// Set them from the measured wait, not from taste: run with
	// `RUST_LOG=debug,bairelay=debug` and read `ms=` out of the
	// `pacer emitted frame` lines.
	/// Milliseconds the first frame a source queues waits before it is
	/// emitted. Default 1500, the value the video pacer has always shipped
	/// with; a live-viewing setup usually wants a few hundred.
	#[serde(default = "default_video_pacer_latency_ms")]
	pub video_pacer_latency_ms: u32,

	/// How far ahead of the wall clock the video emit cursor may run when
	/// the camera bursts a whole GOP at once. Default 3000. A value below
	/// [`CameraConfig::video_pacer_latency_ms`] contradicts itself — the
	/// first frame could never be emitted on time — so it is raised to the
	/// latency at startup, with a warning.
	#[serde(default = "default_video_pacer_max_lead_ms")]
	pub video_pacer_max_lead_ms: u32,

	/// Milliseconds the first audio frame of a source waits. Default 500,
	/// the shipped value, which covers the 100–500 ms burst jitter of Argus
	/// AAC with margin. Lower it and a late packet reaches the listener as
	/// an audible clip instead of as waiting.
	#[serde(default = "default_audio_pacer_latency_ms")]
	pub audio_pacer_latency_ms: u32,

	/// Audio counterpart of [`CameraConfig::video_pacer_max_lead_ms`].
	/// Default 2000, raised to the latency if it is set below it.
	#[serde(default = "default_audio_pacer_max_lead_ms")]
	pub audio_pacer_max_lead_ms: u32,

	// ── Neolink migration compat (deprecated; warn + ignore) ──────────
	// Each Option<_> exists only so old configs parse cleanly under
	// `deny_unknown_fields`. `warn_neolink_compat_fields` emits a one-
	// line message per set field at startup pointing operators at the
	// bairelay equivalent (or stating that the knob is out of scope).
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "print")]
	pub print_format: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "time")]
	pub update_time: Option<bool>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "duration",
		alias = "buffer"
	)]
	pub buffer_duration: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "splash")]
	pub use_splash: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "pattern")]
	pub splash_pattern: Option<String>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "retries",
		alias = "max_retries"
	)]
	pub max_discovery_retries: Option<u64>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "push",
		alias = "push_noti"
	)]
	pub push_notifications: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub strict: Option<bool>,
}

impl Default for CameraConfig {
	fn default() -> Self {
		Self {
			name: String::new(),
			address: None,
			uid: None,
			username: String::new(),
			password: None,
			cloud_account: None,
			cloud_password: None,
			cloud_mfa_trust_token: None,
			cloud_refresh_token: None,
			channel_id: 0,
			stream: StreamConfig::default(),
			discovery: DiscoveryMethod::default(),
			max_encryption: MaxEncryption::default(),
			idle_disconnect: false,
			probe_session_secs: default_probe_session_secs(),
			retry_after_secs: default_retry_after_secs(),
			read_on_wake: default_read_on_wake(),
			read_hold_secs: default_read_hold_secs(),
			read_interval_secs: default_read_interval_secs(),
			idle_disconnect_timeout_secs: None,
			motion_wake_hold_secs: default_motion_wake_hold_secs(),
			enabled: true,
			mqtt: MqttConfig::default(),
			pause: PauseConfig::default(),
			permitted_users: Vec::new(),
			debug: None,
			webhook: false,
			two_way_audio: false,
			video_pacer_latency_ms: default_video_pacer_latency_ms(),
			video_pacer_max_lead_ms: default_video_pacer_max_lead_ms(),
			audio_pacer_latency_ms: default_audio_pacer_latency_ms(),
			audio_pacer_max_lead_ms: default_audio_pacer_max_lead_ms(),
			print_format: None,
			update_time: None,
			buffer_duration: None,
			use_splash: None,
			splash_pattern: None,
			max_discovery_retries: None,
			push_notifications: None,
			strict: None,
		}
	}
}

// ── Media pacer tuning ─────────────────────────────────────────────────

/// Default [`CameraConfig::video_pacer_latency_ms`]: the startup latency the
/// video pacer has always shipped with, measured for mpv-over-RTSP, which
/// flips into `(Buffering)` every couple of seconds if the pacer pre-buffers
/// less. It stays the default so that no existing configuration changes
/// behaviour by this key existing.
pub const DEFAULT_VIDEO_PACER_LATENCY_MS: u32 = 1500;

/// Default [`CameraConfig::video_pacer_max_lead_ms`]: the latency plus 1.5 s
/// of headroom for a camera-side GOP burst, which is what the shipped pacer
/// allowed.
pub const DEFAULT_VIDEO_PACER_MAX_LEAD_MS: u32 = 3000;

/// Default [`CameraConfig::audio_pacer_latency_ms`]: the ~8-packet AU buffer
/// the audio pacer has always started with, sized for the observed 100–500 ms
/// AAC arrival jitter on Argus hardware.
pub const DEFAULT_AUDIO_PACER_LATENCY_MS: u32 = 500;

/// Default [`CameraConfig::audio_pacer_max_lead_ms`]: the audio lead cap this
/// bridge has always used.
pub const DEFAULT_AUDIO_PACER_MAX_LEAD_MS: u32 = 2000;

/// Kept as functions so `serde(default)` and [`CameraConfig::default`] cannot
/// drift apart, and so the pacer's own fallback constants in
/// [`crate::stream_source`] are derived from the same numbers.
const fn default_video_pacer_latency_ms() -> u32 {
	DEFAULT_VIDEO_PACER_LATENCY_MS
}

const fn default_video_pacer_max_lead_ms() -> u32 {
	DEFAULT_VIDEO_PACER_MAX_LEAD_MS
}

const fn default_audio_pacer_latency_ms() -> u32 {
	DEFAULT_AUDIO_PACER_LATENCY_MS
}

const fn default_audio_pacer_max_lead_ms() -> u32 {
	DEFAULT_AUDIO_PACER_MAX_LEAD_MS
}

/// One pacer's wall-clock timing, in the form the pacer constructors take.
/// Built from the per-camera millisecond keys by
/// [`CameraConfig::video_pacer_timings`] / [`CameraConfig::audio_pacer_timings`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacerTimings {
	/// Delay applied to the first frame a source queues, so a buffer exists
	/// before anything reaches subscribers.
	pub initial_latency: std::time::Duration,
	/// How far ahead of the wall clock the emit cursor may run under burst
	/// pressure.
	pub max_lead: std::time::Duration,
}

impl PacerTimings {
	/// Millisecond config values to durations, applying the one rule the
	/// pair has to obey: the lead cap is never below the latency. A cap
	/// under the latency is self-contradictory — the first frame would be
	/// scheduled past the point where the cap re-anchors the cursor — so
	/// the larger of the two is used for both. This is the last line of
	/// defence for configurations that never went through
	/// [`clamp_pacer_lead_limits`]; it never logs and never fails.
	pub fn from_ms(latency_ms: u32, max_lead_ms: u32) -> Self {
		let latency = std::time::Duration::from_millis(latency_ms as u64);
		let max_lead = std::time::Duration::from_millis(max_lead_ms.max(latency_ms) as u64);
		Self {
			initial_latency: latency,
			max_lead,
		}
	}
}

impl CameraConfig {
	/// Effective video-pacer timings for this camera.
	pub fn video_pacer_timings(&self) -> PacerTimings {
		PacerTimings::from_ms(self.video_pacer_latency_ms, self.video_pacer_max_lead_ms)
	}

	/// Effective audio-pacer timings for this camera.
	pub fn audio_pacer_timings(&self) -> PacerTimings {
		PacerTimings::from_ms(self.audio_pacer_latency_ms, self.audio_pacer_max_lead_ms)
	}
}

// ── MqttConfig (per-camera) ───────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
	#[serde(default = "default_true")]
	pub enable_motion: bool,

	#[serde(default = "default_true")]
	pub enable_light: bool,

	#[serde(default = "default_true")]
	pub enable_battery: bool,

	#[serde(default = "default_2000")]
	pub battery_update: u64,

	#[serde(default = "default_true")]
	pub enable_preview: bool,

	#[serde(default = "default_2000")]
	pub preview_update: u64,

	#[serde(default)]
	pub enable_floodlight: bool,

	#[serde(default = "default_2000")]
	pub floodlight_update: u64,

	#[serde(default)]
	pub enable_pir: bool,

	/// Neolink-compat: per-camera HA discovery override. Bairelay uses
	/// a global `[mqtt.discovery]` table only; this per-camera knob is
	/// accepted with a warning and has no effect. Operators should
	/// move the `topic` / `features` to the global table.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub discovery: Option<MqttDiscoveryConfig>,
}

impl Default for MqttConfig {
	fn default() -> Self {
		MqttConfig {
			enable_motion: true,
			enable_light: true,
			enable_battery: true,
			battery_update: 2000,
			enable_preview: true,
			preview_update: 2000,
			enable_floodlight: false,
			floodlight_update: 2000,
			discovery: None,
			enable_pir: false,
		}
	}
}

impl From<&MqttConfig> for bairelay_mqtt::discovery::CameraEnableFlags {
	/// Map the per-camera `[cameras.mqtt]` toggles onto the
	/// discovery-side `CameraEnableFlags`. Features without a
	/// per-camera flag (`camera`, `ir`, `reboot`, `pt`, `siren`) pass
	/// through unconditionally — gating is delegated to the global
	/// `features` config and live capability detection.
	fn from(m: &MqttConfig) -> Self {
		Self {
			motion: m.enable_motion,
			battery: m.enable_battery,
			floodlight: m.enable_floodlight,
			light: m.enable_light,
			pir: m.enable_pir,
		}
	}
}

// ── PauseConfig ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PauseConfig {
	#[serde(default = "default_true")]
	pub bridge_gaps: bool,

	#[serde(default = "default_gap_threshold")]
	pub gap_threshold_secs: f64,

	#[serde(default = "default_true")]
	pub preview_overlay: bool,

	// ── Neolink migration compat (deprecated; warn + ignore) ─────────
	// These fields were valid in neolink's `[cameras.pause]` block
	// under the motion-driven pause model. Bairelay's gap-bridging
	// framing has no direct equivalent, so they are accepted to keep
	// old configs parsing but produce a migration warning at startup
	// and have no runtime effect.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_motion: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_client: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_disconnect: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub motion_timeout: Option<f64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<String>,

	/// Neolink migration compat: `timeout` was the pause-on-motion
	/// post-motion delay. Bairelay has no equivalent in the gap-
	/// bridging model, so we reuse this key to set the idle-disconnect
	/// grace period (the closest analogue — "seconds the camera lingers
	/// before sleeping"). Overrides the camera's
	/// `idle_disconnect_timeout_secs` if that field is unset.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<f64>,
}

impl Default for PauseConfig {
	fn default() -> Self {
		PauseConfig {
			bridge_gaps: true,
			gap_threshold_secs: default_gap_threshold(),
			preview_overlay: true,
			on_motion: None,
			on_client: None,
			on_disconnect: None,
			motion_timeout: None,
			mode: None,
			timeout: None,
		}
	}
}

// ── Default value functions ────────────────────────────────────────────

fn default_bind_addr() -> String {
	"0.0.0.0".to_string()
}

fn default_bind_port() -> u16 {
	8554
}

fn default_mqtt_port() -> u16 {
	1883
}

fn default_topic_prefix() -> String {
	"bairelay".to_string()
}

const fn default_true() -> bool {
	true
}

const fn default_2000() -> u64 {
	2000
}

fn default_gap_threshold() -> f64 {
	// Argus 4K-HEVC cameras deliver each GOP as a sub-second burst then
	// idle briefly before the next; observed inter-burst wallclock
	// varies 700–1400 ms even on a healthy stream. A 1 s threshold
	// misfired during normal operation, Bridging synth_pts then raced
	// past the camera's PTS, and clients (mpv, HA) saw 1 s backward
	// timestamp jumps + audio underruns every ~2 s. 3 s leaves headroom
	// for normal bursting while still catching multi-second silences
	// (camera waking, reconnecting).
	3.0
}

const fn default_prune_grace_secs() -> u64 {
	30
}

const fn default_motion_wake_hold_secs() -> f64 {
	30.0
}

/// Five seconds, which is ten of the retransmission gaps the discovery code
/// already uses (`RESEND_WAIT` in `crates/core/src/bc_protocol/connection/discovery.rs`):
/// enough to cover a broadcast that the switch drops the first time round, and
/// short enough that a bridge with nothing on its LAN still starts promptly. A
/// camera that will answer, answers on the first round trip — the wait is for
/// packet loss, not for the camera to wake up.
const fn default_startup_discovery_wait_secs() -> u64 {
	5
}

// ── Parsing ────────────────────────────────────────────────────────────

/// Top-level scalar config keys. Used by [`validate_top_level_key_placement`]
/// to detect operator typos where one of these is pasted *after* a `[section]`
/// header — TOML scopes the key to the open section instead of the document
/// root, and it then either fails serde with a cryptic "unknown field" or
/// (worse, before this layer existed) silently disappeared.
///
/// Includes serde aliases (e.g. `bind` for `bind_addr`) so an operator who
/// types either form gets caught.
const TOP_LEVEL_SCALAR_KEYS: &[&str] = &[
	"bind",
	"bind_addr",
	"bind_port",
	"certificate",
	"tls_bind_port",
	"tls_client_auth",
	"tls_client_ca",
	"tokio_console",
	"stream_prune_grace_secs",
];

/// Walk a parsed TOML document and flag any scalar key that lives inside
/// a non-root table but matches a known top-level Config field. Catches
/// the common typo:
///
/// ```toml
/// [mqtt]
/// broker_addr = "..."
///
/// certificate = "/path"   # ← lands inside [mqtt], not at the root
/// ```
///
/// Errors with one line per misplaced key naming the section path and
/// telling the operator how to fix. Called from [`parse_config`] before
/// serde deserialisation so the message is friendlier than serde's
/// "unknown field" or, worse, a silent drop on a struct that is missing
/// `deny_unknown_fields`.
pub fn validate_top_level_key_placement(value: &toml::Value) -> Result<(), String> {
	let toml::Value::Table(root) = value else {
		// Not a table at the document root — `parse_config` will surface
		// the real error from serde. Don't double-report.
		return Ok(());
	};
	let mut misplaced: Vec<(String, String)> = Vec::new();
	for (key, child) in root {
		walk_for_misplaced(key, child, &mut misplaced);
	}
	if misplaced.is_empty() {
		return Ok(());
	}
	let mut msg = String::from(
		"config: misplaced top-level keys (TOML scopes scalars to the most-recently-opened table; \
		 these belong at the document root, before any [section] header):\n",
	);
	for (path, key) in &misplaced {
		msg.push_str(&format!(
			"  - `{key}` is inside `[{path}]`. Move it to the document root, before the first [section] header.\n",
		));
	}
	Err(msg)
}

fn walk_for_misplaced(path: &str, value: &toml::Value, out: &mut Vec<(String, String)>) {
	match value {
		toml::Value::Table(t) => {
			for (k, v) in t {
				match v {
					toml::Value::Table(_) => {
						walk_for_misplaced(&format!("{path}.{k}"), v, out);
					}
					toml::Value::Array(arr) => {
						for (i, item) in arr.iter().enumerate() {
							if matches!(item, toml::Value::Table(_)) {
								walk_for_misplaced(&format!("{path}.{k}[{i}]"), item, out);
							}
						}
					}
					_ => {
						if TOP_LEVEL_SCALAR_KEYS.contains(&k.as_str()) {
							out.push((path.to_string(), k.clone()));
						}
					}
				}
			}
		}
		toml::Value::Array(arr) => {
			for (i, item) in arr.iter().enumerate() {
				if matches!(item, toml::Value::Table(_)) {
					walk_for_misplaced(&format!("{path}[{i}]"), item, out);
				}
			}
		}
		_ => {}
	}
}

/// Tables this configuration language used to have, in French, and what replaced
/// them.
///
/// Serde's own message for an unknown table lists the fields it expected, which tells the
/// operator where to look only if they already know that their settings moved. The file
/// still carrying these tables is a real file, written on request, and the next reader is
/// going to have it under their eyes: so the two names are recognised here and answered in
/// plain terms, with the successor of every key and a pointer to the migration table.
const RETIRED_TABLES: &[(&str, &str)] = &[
	(
		"annonces",
		"it is now three settings on each camera, not one table for all of them: 		 `interroger` -> `read_on_wake`, `tenue_secs` -> `read_hold_secs`, `intervalle_secs` 		 -> `read_interval_secs`. Move them into the `[[cameras]]` block they belong to: two 		 cameras on one installation do not need the same rhythm. The migration table in 		 `docs/settings.md` lists the three.",
	),
	(
		"carnet",
		"it has no successor. What the camera announces is published as the doorbell press 		 event (`status/doorbell`, payload `press`, not retained) and written to the log with 		 `event`, `reason` and the durations; this bridge keeps no notebook file, and nothing 		 reads one. Delete the table. The migration table in `docs/settings.md` says why.",
	),
];

/// Refuse a configuration that still carries a retired table, naming it and saying what
/// replaced it. Runs before serde for the same reason the placement scanner does: the
/// generic "unknown field" message would be the last useful thing this operator reads.
fn validate_renamed_tables(value: &toml::Value) -> Result<(), String> {
	let Some(root) = value.as_table() else {
		return Ok(());
	};
	let found: Vec<String> = RETIRED_TABLES
		.iter()
		.filter(|(name, _)| root.contains_key(*name))
		.map(|(name, advice)| {
			format!("the [{name}] table no longer exists: {advice}")
		})
		.collect();
	if found.is_empty() {
		return Ok(());
	}
	Err(found.join("\n"))
}

/// Decide whether a file that declares no camera may still be accepted.
///
/// A missing `[[cameras]]` is a parse error, and has been all along: a bridge with
/// nothing to do is more likely to be a file the operator thinks they wrote than a
/// deliberate choice. `[startup_discovery]` is the one case where "no camera in the
/// file" *is* the instruction — asking the LAN who is there is the operator's way of
/// not writing identifiers — so the empty list is spelled out for serde here, and
/// nowhere else. A file without that table, or with it switched off, is not touched
/// by this function at all: its parse error, if it has one, is the one it always
/// produced.
///
/// Switched-off-but-present gets its own sentence rather than serde's `missing field
/// \`cameras\``, because that operator has just asked for a mode they did not get, and
/// "cameras" is not the word they were looking for.
fn implicit_camera_list(value: &mut toml::Value) -> Result<(), String> {
	let root = match value.as_table_mut() {
		Some(root) => root,
		None => return Ok(()),
	};
	let Some(discovery) = root.get("startup_discovery") else {
		return Ok(());
	};
	let armed = discovery.get("enable").and_then(toml::Value::as_bool);
	if root.contains_key("cameras") {
		return Ok(());
	}
	match armed {
		Some(true) => {
			root.insert("cameras".to_string(), toml::Value::Array(Vec::new()));
			Ok(())
		}
		_ => Err(
			"the [startup_discovery] table is present but `enable` is not true, so nothing goes \
			 looking for cameras on the network: set enable = true to adopt the ones that answer, \
			 or declare them with [[cameras]]"
				.to_string(),
		),
	}
}

/// Parse a TOML string into a Config. Runs the placement scanner
/// before serde deserialisation so misplaced top-level keys get a
/// dedicated diagnostic instead of serde's "unknown field" error.
pub fn parse_config(toml_str: &str) -> Result<Config, String> {
	let mut value: toml::Value =
		toml::from_str(toml_str).map_err(|e| format!("Failed to parse config: {e}"))?;
	validate_top_level_key_placement(&value)?;
	validate_renamed_tables(&value)?;
	implicit_camera_list(&mut value)?;
	let mut config: Config = value
		.try_into()
		.map_err(|e: toml::de::Error| format!("Failed to parse config: {e}"))?;
	// The Reolink account for "cloud" cameras lives at the document root; copy
	// it onto each camera so the per-camera connect path can mint a bundle.
	for cam in &mut config.cameras {
		cam.cloud_account = config.cloud_account.clone();
		cam.cloud_password = config.cloud_password.clone();
	}
	// A pacer lead cap below its own latency is a tuning mistake, not a config
	// error: normalise it once here so every consumer sees one coherent pair.
	clamp_pacer_lead_limits(&mut config);
	Ok(config)
}

/// Raise every camera's pacer lead cap to its latency floor, warning once per
/// adjusted pair. Never fails and never panics: an inconsistent
/// `(latency, max_lead)` pair says what the operator meant (a tighter
/// pipeline), and quietly keeping the impossible half of it would leave them
/// chasing a latency that never changes.
///
/// [`parse_config`] calls this. The add-on path assembles its config through
/// [`crate::hassio::merge`] instead, so that command calls it explicitly too,
/// and [`PacerTimings::from_ms`] clamps again as a last line of defence for
/// anything built in code.
pub fn clamp_pacer_lead_limits(config: &mut Config) {
	for cam in &mut config.cameras {
		cam.video_pacer_max_lead_ms = clamp_lead_to_latency(
			&cam.name,
			"video",
			cam.video_pacer_latency_ms,
			cam.video_pacer_max_lead_ms,
		);
		cam.audio_pacer_max_lead_ms = clamp_lead_to_latency(
			&cam.name,
			"audio",
			cam.audio_pacer_latency_ms,
			cam.audio_pacer_max_lead_ms,
		);
	}
}

/// The floor itself, and the one warning that goes with it. Returns the value
/// to use: the cap, unless it sat below the latency, in which case the
/// latency.
fn clamp_lead_to_latency(
	camera: &str,
	kind: &'static str,
	latency_ms: u32,
	max_lead_ms: u32,
) -> u32 {
	if max_lead_ms >= latency_ms {
		return max_lead_ms;
	}
	tracing::warn!(
		camera,
		kind,
		latency_ms,
		max_lead_ms,
		"config: pacer max_lead_ms is below the pacer latency_ms; using the latency as the cap. \
		 The first frame is held for latency_ms, so a smaller cap would release it early and \
		 cancel the buffering the latency is there to buy."
	);
	latency_ms
}

/// Filename of the host's MFA trust-token store, written by `cloud-authorise`
/// and read here. Lives beside the config file.
pub const CLOUD_AUTH_FILE: &str = "config-cloud-auth.json";

/// Attach the host's stored MFA trust token (from `config-cloud-auth.json`
/// beside `config_path`) to each `discovery = "cloud"` camera, so the cloud
/// mint can clear Reolink's login verification headlessly. A missing file is
/// the normal case (host not bootstrapped); an account-mismatched or expired
/// token is ignored with a warning pointing at `cloud-authorise`.
pub fn apply_cloud_auth(config: &mut Config, config_path: &std::path::Path) {
	let auth_path = config_path.with_file_name(CLOUD_AUTH_FILE);
	let raw = match std::fs::read_to_string(&auth_path) {
		Ok(s) => s,
		Err(_) => return,
	};
	let auth: bairelay_neolink_core::cloud::CloudAuth = match serde_json::from_str(&raw) {
		Ok(a) => a,
		Err(e) => {
			tracing::warn!("ignoring {}: {e}", auth_path.display());
			return;
		}
	};
	if config.cloud_account.as_deref() != Some(auth.account.as_str()) {
		tracing::warn!(
			"{} is for a different Reolink account than cloud_account; ignoring it",
			auth_path.display()
		);
		return;
	}
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|d| d.as_secs() as i64)
		.unwrap_or(0);
	if !auth.is_valid(now) {
		tracing::warn!(
			"cloud credential in {} has expired/empty — run `cloud-authorise` to re-authorise this host",
			auth_path.display()
		);
		return;
	}
	for cam in &mut config.cameras {
		if matches!(cam.discovery, DiscoveryMethod::Cloud) {
			cam.cloud_mfa_trust_token = auth.mfa_trust_token.clone();
			cam.cloud_refresh_token = auth.refresh_token.clone();
		}
	}
}

// ── Validation ─────────────────────────────────────────────────────────

// ── Test helpers ──────────────────────────────────────────────────────

pub mod test_helpers {
	use super::*;

	pub fn minimal_camera_config(name: &str) -> CameraConfig {
		CameraConfig {
			name: name.to_string(),
			address: Some("192.168.1.1:9000".to_string()),
			username: "admin".to_string(),
			password: Some("test".to_string()),
			..Default::default()
		}
	}

	pub fn two_camera_config() -> Config {
		Config {
			cameras: vec![minimal_camera_config("cam1"), minimal_camera_config("cam2")],
			..Default::default()
		}
	}
}

/// Log a one-line migration warning for each deprecated neolink-style
/// `[cameras.pause]` field present in the parsed config. Called once at
/// startup after `parse_config` and before `validate_config`.
///
/// `on_motion` / `on_client` / `on_disconnect` / `motion_timeout` /
/// `mode` are accepted only so old configs continue to parse; they
/// have no runtime effect in the gap-bridging model.
///
/// `timeout` is kept as a soft alias for
/// [`CameraConfig::idle_disconnect_timeout_secs`] — see
/// [`resolve_idle_disconnect_timeout`].
pub fn warn_deprecated_pause_fields(config: &Config) {
	for cam in &config.cameras {
		let p = &cam.pause;
		if p.on_motion.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_motion is deprecated (neolink migration compat); the motion-driven pause model is replaced by upstream gap-bridging. Remove this field."
			);
		}
		if p.on_client.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_client is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.on_disconnect.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_disconnect is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.motion_timeout.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] motion_timeout is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.mode.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] mode is deprecated (neolink migration compat); bairelay's overlay always draws the PreviewState label. Remove this field."
			);
		}
		if let Some(t) = p.timeout {
			if cam.idle_disconnect_timeout_secs.is_some() {
				tracing::warn!(
					camera = %cam.name,
					"config: [cameras.pause] timeout is deprecated and overridden by idle_disconnect_timeout_secs; remove this field."
				);
			} else {
				tracing::warn!(
					camera = %cam.name,
					seconds = t,
					"config: [cameras.pause] timeout is deprecated (neolink migration compat); mapped to idle_disconnect_timeout_secs. Move the value under [cameras] to silence this warning."
				);
			}
		}
	}
}

/// Log a one-line migration warning for each neolink-only field that
/// bairelay accepts (so old configs parse cleanly under
/// `deny_unknown_fields`) but has no runtime effect, paired with a
/// pointer to the bairelay equivalent. Called once at startup after
/// [`warn_deprecated_pause_fields`].
///
/// Top-level `tokio_console` and per-camera `print_format` /
/// `update_time` / `buffer_duration` / `use_splash` / `splash_pattern` /
/// `max_discovery_retries` / `push_notifications` / `strict` are all
/// no-ops in bairelay; the per-camera `[cameras.mqtt] discovery` block
/// is also a no-op (bairelay uses a single global `[mqtt.discovery]`).
pub fn warn_neolink_compat_fields(config: &Config) {
	if config.tokio_console.is_some() {
		tracing::warn!(
			"config: tokio_console is a neolink debugging knob; bairelay drives verbosity via RUST_LOG (or -v / -vv / -vvv on the CLI). Remove this field."
		);
	}
	for cam in &config.cameras {
		if cam.print_format.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] print_format is a neolink stdout-dump knob; bairelay uses --dump-bcmedia <dir> on the CLI for fixture capture. Remove this field."
			);
		}
		if cam.update_time.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] update_time/time is a neolink connect-time clock-sync knob; bairelay exposes this via the `bairelay set-time <camera>` one-shot command. Remove this field."
			);
		}
		if cam.buffer_duration.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] buffer_duration/duration/buffer is a neolink GStreamer queue knob; bairelay's audio + video pacers handle bursty delivery internally. Remove this field."
			);
		}
		if cam.use_splash.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] use_splash/splash is a neolink stream-overlay toggle; bairelay's nearest equivalent is `[cameras.pause] preview_overlay` (default true), which captions the MQTT preview JPEG. Remove this field."
			);
		}
		if cam.splash_pattern.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] splash_pattern/pattern is a neolink GStreamer test-pattern selector; bairelay only renders CONNECTING / SLEEPING captions. Remove this field."
			);
		}
		if cam.max_discovery_retries.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] max_discovery_retries/retries/max_retries is a neolink discovery cap; bairelay uses exponential backoff with cancellation. Remove this field."
			);
		}
		if cam.push_notifications.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] push_notifications/push/push_noti is a neolink FCM toggle (deferred — spec §10); bairelay does not consume Reolink push at present. Remove this field."
			);
		}
		if cam.strict.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] strict is a neolink media-validation toggle; bairelay always validates parameter sets and NAL whitelists. Remove this field."
			);
		}
		if cam.mqtt.discovery.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.mqtt] discovery is a neolink per-camera HA discovery override; bairelay uses a single global [mqtt.discovery] table. Move topic/features there. Remove this field."
			);
		}
	}
}

/// Log a one-line notice for each camera with wire-level protocol
/// debugging enabled (`[cameras] debug = true`). The trace-level
/// payload dumps it unlocks include credential hashes and camera UIDs,
/// so the operator should know the knob is live before sharing logs.
/// Called once at startup alongside [`warn_neolink_compat_fields`].
pub fn warn_wire_debug_enabled(config: &Config) {
	for cam in &config.cameras {
		if cam.debug == Some(true) {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] debug enables trace-level dumps of decrypted protocol payloads (set RUST_LOG=info,bairelay_neolink_core::bc::de=trace to see them — the leading info, is required or the console goes silent); they include credential hashes — do not share these logs publicly"
			);
		}
	}
}

/// The session probe's default cadence: 5 s, the value this bridge has always
/// used. Kept as a function so `serde(default)` and [`CameraConfig::default`]
/// cannot drift apart.
const fn default_probe_session_secs() -> u64 {
	5
}

/// Off by default: no deferred attempt, which is what this bridge has always
/// done. `0` is a value with a meaning, not a missing value.
const fn default_retry_after_secs() -> u64 {
	0
}

/// On by default, for the reason the camera itself gives: the announcement arrives
/// when the camera is already awake, because that is what it is announcing. What it
/// buys is a session at that moment instead of at the end of whatever the camera
/// woke for, so a wake that carries no press still leaves the battery level and the
/// settings current. What it costs is one session per announced wake, including
/// detections that carry nothing, which is why it can be switched off per camera.
/// It is not the way to learn that a button was pressed sooner: the announcement of
/// a press is published directly, without a session.
const fn default_read_on_wake() -> bool {
	true
}

/// How long the bridge waits for that session to appear, holding the wake lock
/// meanwhile. A camera that answers is released as soon as the session is up, so
/// the long end of this window is only paid by a camera that does not answer.
/// The default covers a camera whose link takes a few seconds to come up.
const fn default_read_hold_secs() -> u64 {
	20
}

/// Minimum spacing between two reads that got a session. Motion detection wakes a
/// battery camera on every moving object, so without a floor every moving object
/// buys a session and the battery budget is gone. A read that got no session is
/// not charged to it: it cost nothing.
const fn default_read_interval_secs() -> u64 {
	60
}

/// The refusal message for a camera with no probe and no idle disconnect.
/// One sentence, because it is printed at startup and the operator has to be
/// able to act on it without opening a manual.
fn probe_without_idle_error(name: &str) -> String {
	format!(
		"camera {name}: probe_session_secs = 0 removes the only detector of a half-dead session \
		 (the socket reader gives up on EOF without reporting it, no command on that socket has a \
		 timeout, and SO_KEEPALIVE is set nowhere in this tree), and with idle_disconnect = false \
		 nothing else ever ends the session: set probe_session_secs above 0, or idle_disconnect \
		 to true and let the grace period close it"
	)
}

/// Resolve the effective idle-disconnect grace period for a camera,
/// honouring the deprecated `[cameras.pause] timeout` alias. Falls back
/// to 45 s if neither is set.
///
/// **Runtime floor.** When the configured value is **strictly shorter**
/// than `prune_grace`, this returns `prune_grace + 15 s` instead — the
/// invariant on `Config::stream_prune_grace_secs` says a cached
/// `StreamSource` must not outlive the Baichuan session that feeds it,
/// so we clamp silently at resolution time and let
/// [`warn_idle_timeout_below_prune_floor`] log the operator-facing
/// warning once at startup. Equal values do not trigger the clamp:
/// at the moment the source prunes, the camera also disconnects, so
/// the windows just touch instead of overlapping. The 45/30 default
/// pair clears the floor with a 15 s margin by design.
pub fn resolve_idle_disconnect_timeout(
	cam: &CameraConfig,
	prune_grace: std::time::Duration,
) -> std::time::Duration {
	let secs = cam
		.idle_disconnect_timeout_secs
		.or(cam.pause.timeout)
		.unwrap_or(45.0);
	let configured = std::time::Duration::from_secs_f64(secs);
	if configured < prune_grace {
		prune_grace + std::time::Duration::from_secs(15)
	} else {
		configured
	}
}

/// Log a one-line config warning for each camera whose configured
/// `idle_disconnect_timeout_secs` (or `pause.timeout` alias) is shorter
/// than the global `stream_prune_grace_secs`. The runtime clamps the
/// value silently via [`resolve_idle_disconnect_timeout`]; this helper
/// surfaces the clamp to operators so they know their explicit config
/// was overridden. Called once at startup, alongside
/// [`warn_deprecated_pause_fields`].
pub fn warn_idle_timeout_below_prune_floor(config: &Config) {
	let prune = std::time::Duration::from_secs(config.stream_prune_grace_secs);
	let safe_floor = prune + std::time::Duration::from_secs(15);
	for cam in &config.cameras {
		let secs = cam
			.idle_disconnect_timeout_secs
			.or(cam.pause.timeout)
			.unwrap_or(45.0);
		let configured = std::time::Duration::from_secs_f64(secs);
		if configured < prune {
			tracing::warn!(
				camera = %cam.name,
				"config: idle_disconnect_timeout_secs ({:.1}s) is shorter than stream_prune_grace_secs ({}s); \
				 clamped at runtime to {}s so the cached StreamSource cannot outlive the Baichuan session. \
				 Raise idle_disconnect_timeout_secs (recommended >= {}s) or lower stream_prune_grace_secs.",
				configured.as_secs_f64(),
				prune.as_secs(),
				safe_floor.as_secs(),
				safe_floor.as_secs(),
			);
		}
	}
}

/// Effective TLS listener port (used when `certificate` is set).
pub const DEFAULT_TLS_BIND_PORT: u16 = 8555;

/// Validate a parsed Config. Returns Ok(()) or Err with a description.
pub fn validate_config(config: &Config) -> Result<(), String> {
	// Plain RTSP listener: bind_port = 0 means "skip plain", which is
	// only valid if the TLS listener will run instead. relaxed
	// the previous unconditional rejection.
	if config.bind_port == 0 && config.certificate.is_none() {
		return Err(
			"bind_port must be greater than 0 unless certificate is set (TLS-only mode)"
				.to_string(),
		);
	}

	// TLS validation. Only meaningful when certificate is set; else the
	// other TLS knobs are silently ignored to mirror neolink's behaviour
	// of starting plain when no cert is present.
	if config.certificate.is_some() {
		let tls_port = config.tls_bind_port.unwrap_or(DEFAULT_TLS_BIND_PORT);
		if tls_port == 0 {
			return Err("tls_bind_port must be greater than 0".to_string());
		}
		if config.bind_port != 0 && config.bind_port == tls_port {
			return Err(format!(
				"bind_port ({}) and tls_bind_port ({}) must differ",
				config.bind_port, tls_port
			));
		}
		if matches!(
			config.tls_client_auth,
			TlsClientAuth::Request | TlsClientAuth::Require
		) && config.tls_client_ca.is_none()
		{
			return Err(format!(
				"tls_client_auth = \"{}\" requires tls_client_ca to be set",
				match config.tls_client_auth {
					TlsClientAuth::Request => "request",
					TlsClientAuth::Require => "require",
					TlsClientAuth::None => unreachable!(),
				}
			));
		}
	} else if matches!(
		config.tls_client_auth,
		TlsClientAuth::Request | TlsClientAuth::Require
	) {
		return Err("tls_client_auth requires certificate to be set".to_string());
	}

	// Validate the MQTT topic prefix: non-empty, alphanumeric/_/- only.
	// Slashes are forbidden because the prefix is a single segment and
	// embedding one would silently shift every child topic.
	if let Some(ref mqtt) = config.mqtt {
		if mqtt.topic_prefix.is_empty() {
			return Err("mqtt.topic_prefix must not be empty".to_string());
		}
		// ASCII-only intentionally — `char::is_alphanumeric` is Unicode
		// (e.g. "café", "中文" pass) but the MQTT topic flows through
		// every downstream consumer (HA discovery, broker ACLs, log
		// formatters) under an ASCII-only assumption.
		if !mqtt
			.topic_prefix
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
		{
			return Err(format!(
				"mqtt.topic_prefix '{}' must contain only ASCII alphanumeric characters, underscores, and hyphens",
				mqtt.topic_prefix
			));
		}

		// HA discovery config: if present, the discovery root topic
		// must be a single non-empty segment. Slashes would split the
		// root and silently shift every child config topic out from
		// under HA.
		if let Some(ref d) = mqtt.discovery {
			if d.topic.is_empty() {
				return Err("mqtt.discovery.topic must not be empty".to_string());
			}
			if d.topic.contains('/') {
				return Err(format!(
					"mqtt.discovery.topic '{}' must not contain '/'",
					d.topic
				));
			}
		}
	}

	// Each RTSP user must have a non-empty password. Empty passwords
	// degrade both Basic and Digest auth to "accept any input that
	// hashes/encodes the empty string", which is indistinguishable from
	// disabling auth for that account and easy to trip over
	// accidentally. Refuse at startup so operators notice.
	let mut user_names_seen: HashSet<&str> = HashSet::new();
	for (i, user) in config.users.iter().enumerate() {
		if user.name.trim().is_empty() {
			return Err(format!("User #{i}: name must not be empty"));
		}
		if user.pass.is_empty() {
			return Err(format!(
				"User '{}' has an empty password; set a non-empty pass or remove the user",
				user.name
			));
		}
		if !user_names_seen.insert(user.name.as_str()) {
			return Err(format!("Duplicate user name: '{}'", user.name));
		}
	}

	let mut names = HashSet::new();

	for (i, cam) in config.cameras.iter().enumerate() {
		// No empty names
		if cam.name.trim().is_empty() {
			return Err(format!("Camera #{}: name must not be empty", i));
		}

		// Name must be MQTT-safe: ASCII alphanumeric + `_` + `-`. ASCII
		// is intentional — see the topic_prefix validation above for
		// the rationale (downstream consumers assume ASCII).
		if !cam
			.name
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
		{
			return Err(format!(
				"Camera '{}': name must contain only ASCII alphanumeric characters, underscores, and hyphens",
				cam.name
			));
		}

		// No duplicate names
		if !names.insert(&cam.name) {
			return Err(format!("Duplicate camera name: '{}'", cam.name));
		}

		// A local password is required — except for "cloud" cameras, which
		// authenticate with a cloud-minted sigV3 token, not a device password.
		if cam.password.is_none() && cam.discovery != DiscoveryMethod::Cloud {
			return Err(format!("Camera '{}': password is required", cam.name));
		}
		// Cloud cameras need the account credentials to mint that token.
		if cam.discovery == DiscoveryMethod::Cloud
			&& (cam.cloud_account.is_none() || cam.cloud_password.is_none())
		{
			return Err(format!(
				"Camera '{}': discovery = \"cloud\" requires top-level \
				 cloud_account and cloud_password",
				cam.name
			));
		}
		// The cloud token is minted per-UID, so a cloud camera must have one.
		if cam.discovery == DiscoveryMethod::Cloud && cam.uid.is_none() {
			return Err(format!(
				"Camera '{}': discovery = \"cloud\" requires a uid",
				cam.name
			));
		}

		if let Some(addr) = &cam.address {
			if let Err(e) = crate::bc_opts::parse_address(addr) {
				return Err(format!("Camera '{}': invalid address: {e}", cam.name));
			}
		}

		// Camera must have address or uid
		if cam.address.is_none() && cam.uid.is_none() {
			return Err(format!(
				"Camera '{}': either address or uid must be provided",
				cam.name
			));
		}

		// MQTT update intervals >= 500
		if cam.mqtt.battery_update < 500 {
			return Err(format!(
				"Camera '{}': battery_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.battery_update
			));
		}
		if cam.mqtt.preview_update < 500 {
			return Err(format!(
				"Camera '{}': preview_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.preview_update
			));
		}
		if cam.mqtt.floodlight_update < 500 {
			return Err(format!(
				"Camera '{}': floodlight_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.floodlight_update
			));
		}

		// `Duration::from_secs_f64` panics on NaN, negative, infinite, or
		// otherwise non-representable values. The threshold is consumed
		// inside `CameraHandle::stream_source` long after parse, so any
		// bad value would surface as a late runtime panic. Refuse at
		// startup while the error still has context.
		if cam.pause.bridge_gaps
			&& (!cam.pause.gap_threshold_secs.is_finite() || cam.pause.gap_threshold_secs <= 0.0)
		{
			return Err(format!(
				"Camera '{}': pause.gap_threshold_secs must be a positive finite number (got {})",
				cam.name, cam.pause.gap_threshold_secs,
			));
		}

		// idle_disconnect_timeout_secs: same sanity check as above.
		if let Some(t) = cam.idle_disconnect_timeout_secs {
			if !t.is_finite() || t <= 0.0 {
				return Err(format!(
					"Camera '{}': idle_disconnect_timeout_secs must be a positive finite number (got {})",
					cam.name, t,
				));
			}
		}

		// Neolink pause.timeout migration alias: same sanity check.
		if let Some(t) = cam.pause.timeout {
			if !t.is_finite() || t <= 0.0 {
				return Err(format!(
					"Camera '{}': pause.timeout must be a positive finite number (got {})",
					cam.name, t,
				));
			}
		}

		// motion_wake_hold_secs: same sanity check. 0 is allowed (release
		// the wake lock immediately on Stop), but NaN / negative / infinite
		// would panic in `Duration::from_secs_f64`.
		if !cam.motion_wake_hold_secs.is_finite() || cam.motion_wake_hold_secs < 0.0 {
			return Err(format!(
				"Camera '{}': motion_wake_hold_secs must be a non-negative finite number (got {})",
				cam.name, cam.motion_wake_hold_secs,
			));
		}
	}

	// `[push_listener]` validation. The block itself is optional and the
	// enable=false case is a no-op; everything below is gated on enable.
	if let Some(ref pl) = config.push_listener {
		if pl.enable {
			// `Duration::from_secs_f64` panics on NaN / negative / infinite.
			// 0 is allowed — release the wake-lock immediately.
			if !pl.motion_wake_hold_secs.is_finite() || pl.motion_wake_hold_secs < 0.0 {
				return Err(format!(
					"[push_listener]: motion_wake_hold_secs must be a non-negative finite number (got {})",
					pl.motion_wake_hold_secs,
				));
			}
			// Operator config rejects port 0; the chosen ephemeral port is
			// never surfaced, so DNS-redirect from cameras can't reach it.
			if pl.push_listener_port == 0 {
				return Err(
					"[push_listener]: push_listener_port must be greater than 0".to_string()
				);
			}
			// The push listener depends on the shared `CameraRegistry` being
			// populated by the wake server's `D2R_HB` handler. Without it
			// every `lookup_by_ip` misses silently and motion never fires.
			let wake_enabled = config.wake_server.as_ref().is_some_and(|w| w.enable);
			if !wake_enabled {
				return Err(
					"[push_listener] requires [wake_server] enable = true; the push listener resolves peer IPs against the wake server's heartbeat registry"
						.to_string(),
				);
			}
		}
	}

	// `[startup_discovery]` validation, gated on `enable` like the blocks above.
	if let Some(sd) = config.startup_discovery_active() {
		// The window is the one part of this mode that costs the operator
		// something they cannot see: seconds of startup in which no camera is
		// reachable. Out-of-range values are refused rather than clamped, because a
		// clamp would silently replace the number they wrote with a different
		// promise about how long the bridge takes to come up.
		if sd.wait_secs == 0 || sd.wait_secs > MAX_STARTUP_DISCOVERY_WAIT_SECS {
			return Err(format!(
				"[startup_discovery]: wait_secs must be between 1 and {MAX_STARTUP_DISCOVERY_WAIT_SECS} \
				 (got {}); the scan runs before the listeners start, so the window is time no camera \
				 is serving",
				sd.wait_secs,
			));
		}
		// Missing credentials are *not* refused here. The mode's whole job is to hear
		// from cameras this file does not name, and refusing the file because the pair
		// is absent would hide the one thing worth telling the operator: that a camera
		// may well answer and be left unadopted. `warn_startup_discovery_without_credentials`
		// says so at startup and in `check-config`; the scan itself repeats it per
		// camera that answers, naming it.
	}

	// The session probe is the only life detector for an open session, so a
	// camera that switches it off has to have the other closer switched on.
	// Refused at startup rather than discovered the first night the doorbell
	// wanders off Wi-Fi and the bridge reports `connected` until the sun burns out.
	for cam in &config.cameras {
		if cam.probe_session_secs == 0 && !cam.idle_disconnect {
			return Err(probe_without_idle_error(&cam.name));
		}
	}
	// `[webhook]` validation, plus the per-camera opt-in it gates.
	let webhook_enabled = config.webhook.as_ref().is_some_and(|w| w.enable);
	if let Some(wh) = config.webhook.as_ref().filter(|w| w.enable) {
		if wh.webhook_port == 0 {
			return Err("[webhook]: webhook_port must be greater than 0".to_string());
		}
		// The other TCP listeners that run alongside, by config key.
		let tls_port = config
			.certificate
			.as_ref()
			.map(|_| config.tls_bind_port.unwrap_or(DEFAULT_TLS_BIND_PORT));
		let push_port = config
			.push_listener
			.as_ref()
			.filter(|p| p.enable)
			.map(|p| p.push_listener_port);
		let others = [
			(
				"bind_port",
				(config.bind_port != 0).then_some(config.bind_port),
			),
			("tls_bind_port", tls_port),
			("[push_listener] push_listener_port", push_port),
		];
		for (key, port) in others {
			if port == Some(wh.webhook_port) {
				return Err(format!(
					"[webhook]: webhook_port ({}) and {key} must differ",
					wh.webhook_port
				));
			}
		}
		// `Duration::from_secs_f64` panics on NaN / negative / infinite.
		if !wh.motion_wake_hold_secs.is_finite() || wh.motion_wake_hold_secs < 0.0 {
			return Err(format!(
				"[webhook]: motion_wake_hold_secs must be a non-negative finite number (got {})",
				wh.motion_wake_hold_secs,
			));
		}
		if let Some(addr) = &wh.webhook_addr {
			if addr.parse::<std::net::IpAddr>().is_err() {
				return Err(format!(
					"[webhook]: webhook_addr '{addr}' must be an IP address"
				));
			}
		}
		if let Some(adv) = &wh.webhook_advertise {
			if let Err(e) = parse_webhook_advertise(adv) {
				return Err(format!("[webhook]: invalid webhook_advertise '{adv}': {e}"));
			}
		}
	}
	if let Some(cam) = config.cameras.iter().find(|c| c.webhook) {
		if !webhook_enabled {
			return Err(format!(
				"Camera '{}': webhook = true requires [webhook] enable = true",
				cam.name
			));
		}
	}

	// Each entry of `permitted_users` on a camera must reference a globally
	// configured `[[users]]` name. This catches typos at startup rather
	// than at the first failed RTSP authentication.
	let user_names: HashSet<&str> = config.users.iter().map(|u| u.name.as_str()).collect();
	for camera in &config.cameras {
		for pu in &camera.permitted_users {
			if !user_names.contains(pu.as_str()) {
				return Err(format!(
					"camera '{}' permitted_users references unknown user '{}'",
					camera.name, pu
				));
			}
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	/// The scope decides, and the test pins which side of the line each scope is on.
	#[test]
	fn only_none_and_local_keep_the_uid_at_home() {
		assert!(!uid_travels_beyond_local(DiscoveryMethod::None));
		assert!(!uid_travels_beyond_local(DiscoveryMethod::Local));
		for far in [
			DiscoveryMethod::Remote,
			DiscoveryMethod::Map,
			DiscoveryMethod::Relay,
			DiscoveryMethod::Cellular,
		] {
			assert!(uid_travels_beyond_local(far), "{far:?} asks the relays");
		}
	}

	/// The warning is one line per offending camera, naming the tail and the scope, and
	/// says nothing about a camera that stays on the LAN.
	#[test]
	fn the_relay_scope_is_named_at_startup_and_local_is_silent() {
		let mut cfg = Config::default();
		let mut relayed = CameraConfig {
			name: "Doorbell".into(),
			uid: Some("9527000SYNTHTESTWXYZ".into()),
			..CameraConfig::default()
		};
		relayed.discovery = DiscoveryMethod::Relay;
		let mut local = relayed.clone();
		local.name = "Garden".into();
		local.discovery = DiscoveryMethod::Local;
		cfg.cameras = vec![relayed, local];

		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		warn_uid_beyond_local(&cfg);
		let text = captured.text();
		assert_eq!(text.matches("WARN").count(), 1, "one line, for the offending camera: {text}");
		assert!(text.contains("twelve relay"), "{text}");
		assert!(text.contains("does not stop at the first"), "{text}");
		assert!(text.contains("decided by the network's DNS"), "{text}");
		assert!(text.contains("broadcasts only"), "the cost of `local` is said in the same breath: {text}");
		assert!(!text.contains("9527000SYNTHTESTWXYZ"), "the full uid is not printed: {text}");
		assert!(text.contains("STWXYZ"), "the tail is, so it can be matched to the file: {text}");
		assert!(!text.contains("Garden"), "the LAN camera is not mentioned: {text}");
	}

	/// The operator's own file, reduced to what matters for this check: the two retired
	/// tables with their real keys, and one camera. Addresses and credentials are
	/// placeholders, because the point is the table names, not her installation.
	const OPERATOR_FILE: &str = r#"
[wake_server]
enable = true

[carnet]
enable = true
interface = ""
camera = ["aa:bb:cc:dd:ee:ff", "192.0.2.199"]
journal = "/config/bairelay/frames-journal.txt"
taille_max_octets = 2000000
durree_secs = 21600

[annonces]
interroger = true
tenue_secs = 25
intervalle_secs = 30

[[cameras]]
name = "Doorbell"
address = "192.0.2.10"
username = "admin"
password = "placeholder"
"#;

	#[test]
	fn a_file_carrying_the_retired_tables_is_refused_by_name_and_answered() {
		let err = parse_config(OPERATOR_FILE).expect_err("both retired tables must be refused");
		assert!(err.contains("[annonces]"), "{err}");
		assert!(err.contains("[carnet]"), "{err}");
		// The successors, each named: an operator has to be able to edit the file from
		// this message alone.
		for key in ["read_on_wake", "read_hold_secs", "read_interval_secs"] {
			assert!(err.contains(key), "the message must name {key}: {err}");
		}
		for old in ["interroger", "tenue_secs", "intervalle_secs"] {
			assert!(err.contains(old), "the message must say which key moved: {old}");
		}
		assert!(err.contains("no successor"), "carnet has none: {err}");
		assert!(err.contains("docs/settings.md"), "and where to read it: {err}");
	}

	#[test]
	fn one_retired_table_is_named_without_naming_the_other() {
		let only = OPERATOR_FILE.replace(
			r#"[carnet]
enable = true
interface = ""
camera = ["aa:bb:cc:dd:ee:ff", "192.0.2.199"]
journal = "/config/bairelay/frames-journal.txt"
taille_max_octets = 2000000
durree_secs = 21600
"#,
			"",
		);
		let err = parse_config(&only).expect_err("[annonces] alone is still refused");
		assert!(err.contains("[annonces]"), "{err}");
		assert!(!err.contains("[carnet]"), "nothing was said about a table absent: {err}");
	}

	#[test]
	fn a_file_without_retired_tables_is_not_touched_by_this_check() {
		let without = OPERATOR_FILE
			.split("\n[annonces]")
			.next()
			.expect("the file has the annonces table")
			.split("\n[carnet]")
			.next()
			.expect("the file has the carnet table")
			.to_string()
			+ r#"
[[cameras]]
name = "Doorbell"
address = "192.0.2.10"
username = "admin"
password = "placeholder"
"#;
		match parse_config(&without) {
			Ok(_) => {}
			Err(e) => assert!(
				!e.contains("no longer exists"),
				"a file with no retired table must not get that message: {e}"
			),
		}
	}

	#[test]
	fn a_camera_with_no_discovery_key_still_defaults_to_the_relay() {
		// `DiscoveryMethod::None` is an additive variant: it exists to emit nothing,
		// not to look less far. Nothing may move the default away from the relay,
		// because that default is what every existing configuration gets to keep. If
		// this test ever fails, a camera that used to reach the relay has silently
		// stopped trying to.
		let cfg = CameraConfig::default();
		assert!(
			matches!(cfg.discovery, DiscoveryMethod::Relay),
			"the default discovery is the relay"
		);
	}

	use super::*;

	/// A config with `[webhook] enable` and `webhook_advertise`.
	fn advertising(enable: bool, advertise: &str) -> Config {
		Config {
			webhook: Some(WebhookConfig {
				enable,
				webhook_advertise: Some(advertise.to_string()),
				..WebhookConfig::default()
			}),
			..Config::default()
		}
	}

	#[test]
	fn loopback_webhook_advertise_warns_only_when_enabled_and_loopback() {
		for (enable, advertise, warns) in [
			(true, "127.0.0.1:8780", true),
			(true, "localhost:8780", true),
			(true, "[::1]:8780", true),
			(true, "192.0.2.5:8780", false),
			(true, "example.com:8780", false),
			(true, "not an address", false),
			(false, "127.0.0.1:8780", false),
		] {
			let logs = crate::test_logs::capture_at(tracing::Level::WARN);
			warn_loopback_webhook_advertise(&advertising(enable, advertise));
			let out = logs.text();
			assert_eq!(
				out.matches("WARN").count(),
				usize::from(warns),
				"{advertise} (enable={enable}): {out}"
			);
			if warns {
				assert!(out.contains("webhook_advertise"), "{out}");
			}
		}
		let logs = crate::test_logs::capture_at(tracing::Level::WARN);
		warn_loopback_webhook_advertise(&Config::default());
		assert!(logs.text().is_empty());
	}

	fn now_s() -> i64 {
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_secs() as i64)
			.unwrap_or(0)
	}

	/// Build a config + sibling auth file in a fresh temp dir; returns the
	/// (config_path, dir) so the caller can run `apply_cloud_auth` + clean up.
	fn auth_fixture(
		tag: &str,
		account: &str,
		auth_json: &str,
	) -> (std::path::PathBuf, std::path::PathBuf) {
		let dir =
			std::env::temp_dir().join(format!("bairelay-cfgauth-{}-{tag}", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		std::fs::write(dir.join(CLOUD_AUTH_FILE), auth_json).unwrap();
		let cfg_path = dir.join("config.toml");
		let _ = &account;
		(cfg_path, dir)
	}

	fn cloud_config(account: &str) -> Config {
		let mut config = Config {
			cloud_account: Some(account.into()),
			..Config::default()
		};
		let cloud = CameraConfig {
			name: "Cloud".into(),
			discovery: DiscoveryMethod::Cloud,
			..CameraConfig::default()
		};
		let local = CameraConfig {
			name: "Local".into(),
			discovery: DiscoveryMethod::Local,
			..CameraConfig::default()
		};
		config.cameras = vec![cloud, local];
		config
	}

	#[test]
	fn apply_cloud_auth_attaches_valid_token_to_cloud_cameras_only() {
		let json = format!(
			r#"{{"account":"owner@example.com","mfa_trust_token":"TT","expiry_unix_s":{}}}"#,
			now_s() + 30 * 86_400
		);
		let (cfg_path, dir) = auth_fixture("valid", "owner@example.com", &json);
		let mut config = cloud_config("owner@example.com");
		apply_cloud_auth(&mut config, &cfg_path);
		assert_eq!(
			config.cameras[0].cloud_mfa_trust_token.as_deref(),
			Some("TT")
		);
		// Non-cloud camera must NOT receive the token.
		assert_eq!(config.cameras[1].cloud_mfa_trust_token, None);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn apply_cloud_auth_ignores_expired_token() {
		let json = format!(
			r#"{{"account":"owner@example.com","mfa_trust_token":"TT","expiry_unix_s":{}}}"#,
			now_s() - 10
		);
		let (cfg_path, dir) = auth_fixture("expired", "owner@example.com", &json);
		let mut config = cloud_config("owner@example.com");
		apply_cloud_auth(&mut config, &cfg_path);
		assert_eq!(config.cameras[0].cloud_mfa_trust_token, None);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn apply_cloud_auth_ignores_account_mismatch() {
		let json = format!(
			r#"{{"account":"someone-else@example.com","mfa_trust_token":"TT","expiry_unix_s":{}}}"#,
			now_s() + 30 * 86_400
		);
		let (cfg_path, dir) = auth_fixture("mismatch", "owner@example.com", &json);
		let mut config = cloud_config("owner@example.com");
		apply_cloud_auth(&mut config, &cfg_path);
		assert_eq!(config.cameras[0].cloud_mfa_trust_token, None);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn apply_cloud_auth_ignores_malformed_json() {
		let (cfg_path, dir) = auth_fixture("malformed", "owner@example.com", "{not valid json");
		let mut config = cloud_config("owner@example.com");
		apply_cloud_auth(&mut config, &cfg_path);
		assert_eq!(config.cameras[0].cloud_mfa_trust_token, None);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn apply_cloud_auth_missing_file_is_noop() {
		let dir =
			std::env::temp_dir().join(format!("bairelay-cfgauth-{}-none", std::process::id()));
		std::fs::create_dir_all(&dir).unwrap();
		let mut config = cloud_config("owner@example.com");
		apply_cloud_auth(&mut config, &dir.join("config.toml"));
		assert_eq!(config.cameras[0].cloud_mfa_trust_token, None);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn wake_server_block_parses_with_enable_true() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[wake_server]
			enable = true

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let ws = cfg.wake_server.expect("wake_server present");
		assert!(ws.enable);
		assert_eq!(ws.middleman_port, 9999);
	}

	#[test]
	fn cloud_camera_propagates_account_and_validates() {
		let toml = r#"
			bind = "0.0.0.0"
			cloud_account = "owner@example.com"
			cloud_password = "secret"

			[[cameras]]
			name = "hallway"
			uid = "9527000TESTCAM00"
			username = "admin"
			discovery = "cloud"
		"#;
		let cfg = parse_config(toml).expect("parse");
		let cam = &cfg.cameras[0];
		// Top-level creds copied onto the camera; no local password required.
		assert_eq!(cam.cloud_account.as_deref(), Some("owner@example.com"));
		assert_eq!(cam.cloud_password.as_deref(), Some("secret"));
		assert!(cam.password.is_none());
		validate_config(&cfg).expect("cloud camera with account is valid");
	}

	#[test]
	fn cloud_camera_without_uid_is_rejected() {
		let toml = r#"
			bind = "0.0.0.0"
			cloud_account = "owner@example.com"
			cloud_password = "secret"

			[[cameras]]
			name = "hallway"
			address = "192.168.1.50:9000"
			username = "admin"
			discovery = "cloud"
		"#;
		let cfg = parse_config(toml).expect("parse");
		let err = validate_config(&cfg).expect_err("cloud needs a uid");
		assert!(err.contains("requires a uid"), "got: {err}");
	}

	#[test]
	fn malformed_address_is_rejected_and_hostname_accepted() {
		let mk = |addr: &str| {
			format!(
				"[[cameras]]\nname = \"cam1\"\naddress = \"{addr}\"\nusername = \"admin\"\npassword = \"pw\"\n"
			)
		};
		for bad in ["cam1:abc", ":9000", "[::1"] {
			let cfg = parse_config(&mk(bad)).expect("parse");
			let err = validate_config(&cfg).expect_err(bad);
			assert!(
				err.contains("cam1") && err.contains("invalid address"),
				"got: {err}"
			);
		}
		for good in [
			"cam1.example.com",
			"cam1.example.com:9000",
			"127.0.0.1:9000",
		] {
			validate_config(&parse_config(&mk(good)).expect("parse")).expect(good);
		}
	}

	#[test]
	fn cloud_camera_without_account_is_rejected() {
		let toml = r#"
			bind = "0.0.0.0"

			[[cameras]]
			name = "hallway"
			uid = "9527000TESTCAM00"
			username = "admin"
			discovery = "cloud"
		"#;
		let cfg = parse_config(toml).expect("parse");
		let err = validate_config(&cfg).expect_err("cloud needs an account");
		assert!(err.contains("cloud_account"), "got: {err}");
	}

	#[test]
	fn a_non_cloud_camera_still_requires_a_password() {
		let toml = r#"
			bind = "0.0.0.0"

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			discovery = "relay"
		"#;
		let cfg = parse_config(toml).expect("parse");
		let err = validate_config(&cfg).expect_err("relay camera needs a password");
		assert!(err.contains("password is required"), "got: {err}");
	}

	#[test]
	fn wake_server_block_absent_yields_none() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		assert!(cfg.wake_server.is_none());
	}

	#[test]
	fn push_listener_block_parses_defaults() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let pl = cfg.push_listener.expect("push_listener present");
		assert!(pl.enable);
		assert_eq!(pl.push_listener_port, 443);
		assert!(pl.push_listener_addr.is_none());
		assert!((pl.motion_wake_hold_secs - 30.0).abs() < f64::EPSILON);
	}

	#[test]
	fn push_listener_block_absent_yields_none() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		assert!(cfg.push_listener.is_none());
	}

	#[test]
	fn push_listener_overrides_addr_and_port() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true
			push_listener_addr = "10.0.0.5"
			push_listener_port = 8443
			motion_wake_hold_secs = 12.5

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let pl = cfg.push_listener.expect("push_listener present");
		assert_eq!(pl.push_listener_addr.as_deref(), Some("10.0.0.5"));
		assert_eq!(pl.push_listener_port, 8443);
		assert!((pl.motion_wake_hold_secs - 12.5).abs() < f64::EPSILON);
	}

	#[test]
	fn push_listener_rejects_unknown_field() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true
			totally_made_up = "x"

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		assert!(toml::from_str::<Config>(toml).is_err());
	}

	#[test]
	fn probe_session_secs_defaults_to_the_cadence_this_bridge_already_used() {
		// Two paths must agree or an operator gets one cadence from the code and
		// another from the parser.
		assert_eq!(CameraConfig::default().probe_session_secs, 5, "the struct default");
		let cam: CameraConfig = toml::from_str(
			"name = \"Cam\"\naddress = \"192.168.1.5\"\nusername = \"admin\"\npassword = \"p\"\n",
		)
		.unwrap();
		assert_eq!(
			cam.probe_session_secs,
			CameraConfig::default().probe_session_secs,
			"serde's default and the struct's default are two code paths; they must not drift"
		);
	}

	#[test]
	fn probe_session_secs_is_read_from_the_toml() {
		let cfg: Config = toml::from_str(
			r#"
			[[cameras]]
			name = "Cam"
			address = "192.168.1.5"
			username = "admin"
			password = "p"
			probe_session_secs = 0
			idle_disconnect = true
			"#,
		)
		.unwrap();
		assert_eq!(cfg.cameras[0].probe_session_secs, 0);
	}

	#[test]
	fn removing_the_probe_without_the_idle_disconnect_is_refused() {
		// The two settings are the only ways this bridge ends a session. Both
		// off means a camera that leaves the network without a FIN holds the
		// session, the status topics and the wake path open for ever.
		let mut cfg = Config::default();
		cfg.cameras = vec![CameraConfig {
			name: "Doorbell".into(),
			address: Some("192.168.1.9".into()),
			password: Some("p".into()),
			probe_session_secs: 0,
			idle_disconnect: false,
			..CameraConfig::default()
		}];
		let err = validate_config(&cfg).expect_err("probe off plus idle_disconnect off");
		assert!(err.contains("Doorbell"), "the message names the camera: {err}");
		assert!(
			err.contains("probe_session_secs") && err.contains("idle_disconnect"),
			"the message names both settings the operator can change: {err}"
		);
		assert!(
			err.contains("SO_KEEPALIVE") || err.contains("EOF"),
			"the message says why, not just that: {err}"
		);
	}

	#[test]
	fn removing_the_probe_is_allowed_when_the_grace_period_closes_the_session() {
		let mut cfg = Config::default();
		cfg.cameras = vec![CameraConfig {
			name: "Doorbell".into(),
			address: Some("192.168.1.9".into()),
			password: Some("p".into()),
			probe_session_secs: 0,
			idle_disconnect: true,
			..CameraConfig::default()
		}];
		validate_config(&cfg).expect("the grace period is a real bound");
	}

	#[test]
	fn a_probe_cadence_of_one_second_is_not_refused() {
		// An operator probing harder than the default is not a contradiction;
		// the per-probe timeout is clamped to the tick instead of being refused.
		let mut cfg = Config::default();
		cfg.cameras = vec![CameraConfig {
			name: "Fast".into(),
			address: Some("192.168.1.9".into()),
			password: Some("p".into()),
			probe_session_secs: 1,
			idle_disconnect: false,
			..CameraConfig::default()
		}];
		validate_config(&cfg).expect("a fast probe is legal");
	}

	#[test]
	fn no_deferred_attempt_is_the_default() {
		assert_eq!(CameraConfig::default().retry_after_secs, 0, "today's behaviour");
		let cam: CameraConfig = toml::from_str(
			"name = \"Cam\"\naddress = \"192.168.1.5\"\nusername = \"admin\"\npassword = \"p\"\n",
		)
		.unwrap();
		assert_eq!(cam.retry_after_secs, 0, "the parser agrees with the struct");
	}

	#[test]
	fn the_deferred_attempt_delay_is_read_from_the_toml() {
		let cfg: Config = toml::from_str(
			r#"
			[[cameras]]
			name = "Cam"
			address = "192.168.1.5"
			username = "admin"
			password = "p"
			retry_after_secs = 90
			idle_disconnect = true
			"#,
		)
		.unwrap();
		assert_eq!(cfg.cameras[0].retry_after_secs, 90);
	}

	/// The whole point of the mode, at the parser: a file that names no camera and
	/// asks for the scan is a file that means something, and it has to load.
	#[test]
	fn a_file_that_lets_the_network_name_the_cameras_loads() {
		let cfg = parse_config(
			r#"
			[startup_discovery]
			enable = true
			username = "admin"
			password = "shared-lan-password"
			"#,
		)
		.expect("no [[cameras]] is the request, not a mistake");
		assert!(
			cfg.cameras.is_empty(),
			"nothing to declare means nothing to build"
		);
		let scan = cfg.startup_discovery_active().expect("the table is armed");
		assert_eq!(scan.wait_secs, 5, "and the wait is the documented default");
		validate_config(&cfg).expect("a scan-only file is a valid file");
	}

	/// The same file without the switch is the same file the operator would have
	/// written before this mode existed — and it fails the way it always failed, with a
	/// sentence that names the table they did write instead of the one they did not.
	#[test]
	fn a_scan_table_that_is_not_armed_still_needs_cameras() {
		let err = parse_config(
			r#"
			[startup_discovery]
			wait_secs = 3
			username = "admin"
			password = "shared-lan-password"
			"#,
		)
		.expect_err("a table nobody armed cannot fill the camera list");
		assert!(err.contains("[startup_discovery]"), "{err}");
		assert!(err.contains("enable = true"), "{err}");

		// And a file with no table at all keeps the error it has always produced:
		// serde's own, about the missing field. Nothing in this mode leaks into it.
		let plain = parse_config("bind = \"127.0.0.1\"\n").expect_err("no cameras, no mode");
		assert!(
			plain.contains("cameras") && !plain.contains("[startup_discovery]"),
			"{plain}"
		);
	}

	/// The window is bounded, and a value outside the bound is refused rather than
	/// quietly replaced with something else.
	#[test]
	fn the_wait_is_bounded_and_says_so() {
		let file = |secs: u64| {
			parse_config(&format!(
				r#"
				[startup_discovery]
				enable = true
				wait_secs = {secs}
				username = "admin"
				password = "p"
				"#,
			))
		};
		for bad in [0, MAX_STARTUP_DISCOVERY_WAIT_SECS + 1] {
			let err = validate_config(&file(bad).expect("the table parses"))
				.expect_err("a window outside the bound is a refusal, not a clamp");
			assert!(err.contains("wait_secs"), "{err}");
			assert!(
				err.contains("1 and"),
				"the range is given back to them: {err}"
			);
		}
		validate_config(&file(MAX_STARTUP_DISCOVERY_WAIT_SECS).expect("parses"))
			.expect("the top of the range is legal, not refused");
	}

	/// Credentials are the one thing a discovered camera cannot be guessed into
	/// having, so the mode says what is missing — once, before the scan — and stays
	/// quiet when the pair is there or when the mode is off.
	#[test]
	fn the_credential_warning_names_the_missing_half_and_only_that_one() {
		let mut cfg = Config {
			startup_discovery: Some(StartupDiscoveryConfig {
				enable: true,
				username: Some("admin".into()),
				password: None,
				..StartupDiscoveryConfig::default()
			}),
			..Config::default()
		};

		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		warn_startup_discovery_without_credentials(&cfg);
		let text = captured.text();
		assert_eq!(text.matches("WARN").count(), 1, "{text}");
		assert!(
			text.contains("but password is not configured"),
			"the missing half is named: {text}"
		);
		assert!(
			!text.contains("but username and password"),
			"not the half that is already there: {text}"
		);

		cfg.startup_discovery.as_mut().expect("armed").password = Some("p".into());
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		warn_startup_discovery_without_credentials(&cfg);
		assert!(
			captured.text().is_empty(),
			"a complete pair has nothing to warn about"
		);

		// The same table with the switch off is not a mode, and not a warning.
		cfg.startup_discovery.as_mut().expect("still there").password = None;
		cfg.startup_discovery.as_mut().expect("still there").enable = false;
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		warn_startup_discovery_without_credentials(&cfg);
		assert!(
			captured.text().is_empty(),
			"the off switch silences the table as completely as it silences the scan"
		);
	}

	/// `startup_discovery_active` is the only door: `Some` means "the operator wants
	/// the scan", and nothing else in the codebase gets to decide that from the field.
	#[test]
	fn only_an_armed_table_counts_as_a_mode() {
		let mut cfg = Config::default();
		assert!(cfg.startup_discovery_active().is_none(), "absent: off");
		cfg.startup_discovery = Some(StartupDiscoveryConfig::default());
		assert!(
			cfg.startup_discovery_active().is_none(),
			"present, default: off"
		);
		cfg.startup_discovery.as_mut().expect("present").enable = true;
		assert!(cfg.startup_discovery_active().is_some(), "armed");
	}

	/// The credential pair is only a pair when both halves have something in them.
	/// Whitespace is not a username: an operator who typed a space in the field meant
	/// to fill it, and a camera that is logged into with `" "` is not a fallback worth
	/// having.
	#[test]
	fn a_credential_pair_is_both_halves_or_neither() {
		let pair = |username: Option<&str>, password: Option<&str>| StartupDiscoveryConfig {
			enable: true,
			username: username.map(str::to_string),
			password: password.map(str::to_string),
			..StartupDiscoveryConfig::default()
		};
		assert_eq!(
			pair(Some("admin"), Some("p")).credentials(),
			Some(("admin", "p"))
		);
		assert_eq!(pair(Some("  "), Some("p")).credentials(), None);
		assert_eq!(pair(Some("admin"), Some("")).credentials(), None);
		assert_eq!(pair(None, None).credentials(), None);
		assert_eq!(
			pair(None, None).missing_credentials(),
			Some("username and password")
		);
		assert_eq!(
			pair(Some("admin"), None).missing_credentials(),
			Some("password")
		);
		assert_eq!(
			pair(None, Some("p")).missing_credentials(),
			Some("username")
		);
		assert_eq!(pair(Some("admin"), Some("p")).missing_credentials(), None);
	}

	// ── Media pacer keys ───────────────────────────────────────────────

	/// One camera, with the given pacer lines spliced in verbatim (an empty
	/// string means the keys are absent, i.e. defaulted).
	fn pacer_toml(lines: &str) -> String {
		format!(
			"bind = \"0.0.0.0\"\nbind_port = 8554\n\n[[cameras]]\nname = \"doorbell\"\naddress = \"10.0.0.10:9000\"\nusername = \"admin\"\npassword = \"x\"\n{lines}"
		)
	}

	#[test]
	fn pacer_keys_default_to_historical_values() {
		let cfg = parse_config(&pacer_toml("")).expect("parse");
		let cam = &cfg.cameras[0];
		assert_eq!(cam.video_pacer_latency_ms, DEFAULT_VIDEO_PACER_LATENCY_MS);
		assert_eq!(cam.video_pacer_max_lead_ms, DEFAULT_VIDEO_PACER_MAX_LEAD_MS);
		assert_eq!(cam.audio_pacer_latency_ms, DEFAULT_AUDIO_PACER_LATENCY_MS);
		assert_eq!(cam.audio_pacer_max_lead_ms, DEFAULT_AUDIO_PACER_MAX_LEAD_MS);
		// The shipped defaults are exactly what was measured as 1.5 s / 3 s
		// video and 0.5 s / 2 s audio. Adding the keys may not move them.
		assert_eq!(cam.video_pacer_latency_ms, 1500);
		assert_eq!(cam.video_pacer_max_lead_ms, 3000);
		assert_eq!(cam.audio_pacer_latency_ms, 500);
		assert_eq!(cam.audio_pacer_max_lead_ms, 2000);
		// The `Default` impl and the serde defaults must not drift apart.
		let defs = CameraConfig::default();
		assert_eq!(cam.video_pacer_latency_ms, defs.video_pacer_latency_ms);
		assert_eq!(cam.video_pacer_max_lead_ms, defs.video_pacer_max_lead_ms);
		assert_eq!(cam.audio_pacer_latency_ms, defs.audio_pacer_latency_ms);
		assert_eq!(cam.audio_pacer_max_lead_ms, defs.audio_pacer_max_lead_ms);
	}

	#[test]
	fn pacer_keys_parse_operator_values() {
		let cfg = parse_config(&pacer_toml(
			"video_pacer_latency_ms = 300\nvideo_pacer_max_lead_ms = 800\naudio_pacer_latency_ms = 200\naudio_pacer_max_lead_ms = 600\n",
		))
		.expect("parse");
		let cam = &cfg.cameras[0];
		assert_eq!(cam.video_pacer_latency_ms, 300);
		assert_eq!(cam.video_pacer_max_lead_ms, 800);
		assert_eq!(cam.audio_pacer_latency_ms, 200);
		assert_eq!(cam.audio_pacer_max_lead_ms, 600);
		assert_eq!(
			cam.video_pacer_timings().initial_latency,
			std::time::Duration::from_millis(300)
		);
		assert_eq!(
			cam.audio_pacer_timings().max_lead,
			std::time::Duration::from_millis(600)
		);
	}

	#[test]
	fn pacer_lead_cap_below_latency_is_raised_with_warning() {
		let logs = crate::test_logs::capture_at(tracing::Level::WARN);
		let cfg = parse_config(&pacer_toml(
			"video_pacer_latency_ms = 1500\nvideo_pacer_max_lead_ms = 500\naudio_pacer_latency_ms = 500\naudio_pacer_max_lead_ms = 200\n",
		))
		.expect("parse");
		let cam = &cfg.cameras[0];
		// Both caps sat below their floor, and both were raised to it.
		assert_eq!(cam.video_pacer_max_lead_ms, 1500);
		assert_eq!(cam.audio_pacer_max_lead_ms, 500);
		let out = logs.text();
		assert_eq!(out.matches("WARN").count(), 2, "got: {out}");
		assert!(out.contains("max_lead_ms"), "got: {out}");
		assert!(out.contains("doorbell"), "got: {out}");
		// Latencies are the operator's own numbers and stay untouched.
		assert_eq!(cam.video_pacer_latency_ms, 1500);
		assert_eq!(cam.audio_pacer_latency_ms, 500);
	}

	#[test]
	fn pacer_lead_cap_equal_to_latency_is_left_alone() {
		let logs = crate::test_logs::capture_at(tracing::Level::WARN);
		let cfg = parse_config(&pacer_toml(
			"video_pacer_latency_ms = 300\nvideo_pacer_max_lead_ms = 300\n",
		))
		.expect("parse");
		assert_eq!(cfg.cameras[0].video_pacer_max_lead_ms, 300);
		assert!(logs.text().is_empty(), "got: {}", logs.text());
	}

	#[test]
	fn pacer_zero_latency_is_accepted_and_uncapped_by_default() {
		// `0` latency means "emit the first frame immediately". The default
		// lead cap still applies, so nothing clamps and nothing warns.
		let logs = crate::test_logs::capture_at(tracing::Level::WARN);
		let cfg = parse_config(&pacer_toml("video_pacer_latency_ms = 0\n")).expect("parse");
		let cam = &cfg.cameras[0];
		assert_eq!(cam.video_pacer_latency_ms, 0);
		assert_eq!(cam.video_pacer_max_lead_ms, DEFAULT_VIDEO_PACER_MAX_LEAD_MS);
		assert!(cam.video_pacer_timings().initial_latency.is_zero());
		assert!(logs.text().is_empty(), "got: {}", logs.text());
	}

	/// The accessor clamps too, so a `CameraConfig` built in code (the add-on
	/// merge path, a test) can never hand the pacer a self-contradictory pair,
	/// even though `parse_config` never produced one.
	#[test]
	fn pacer_timings_accessor_clamps_without_parser() {
		let cam = CameraConfig {
			video_pacer_latency_ms: 900,
			video_pacer_max_lead_ms: 100,
			audio_pacer_latency_ms: 400,
			audio_pacer_max_lead_ms: 400,
			..CameraConfig::default()
		};
		let v = cam.video_pacer_timings();
		assert_eq!(v.initial_latency, std::time::Duration::from_millis(900));
		assert_eq!(v.max_lead, std::time::Duration::from_millis(900));
		assert_eq!(
			cam.audio_pacer_timings().max_lead,
			std::time::Duration::from_millis(400)
		);
	}
}
