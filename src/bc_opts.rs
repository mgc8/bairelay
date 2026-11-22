//! `BcCameraOpt` builder shared by the camera connect loop and CLI.

use crate::config::{self, CameraConfig};
use bairelay_neolink_core::bc_protocol::Error as CoreError;
use bairelay_neolink_core::bc_protocol::{
	BcCameraOpt, ConnectionProtocol, Credentials, DiscoveryMethods, MaxEncryption,
};
use std::net::IpAddr;
use std::time::Duration;

/// Upper bound on a single hostname lookup.
pub const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Host part of a camera `address`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressHost {
	Ip(IpAddr),
	Name(String),
}

/// A parsed camera `address`: `ip`, `ip:port`, `host`, `host:port`,
/// `[ipv6]` / `[ipv6]:port`, or bare IPv6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAddress {
	pub host: AddressHost,
	pub port: Option<u16>,
}

fn parse_port(s: &str) -> Result<u16, String> {
	s.parse::<u16>().map_err(|_| format!("invalid port '{s}'"))
}

fn valid_hostname(h: &str) -> bool {
	!h.is_empty()
		&& h.len() <= 253
		&& h.chars()
			.all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
}

/// Parse a camera `address` string. Pure; no DNS.
pub fn parse_address(raw: &str) -> Result<ParsedAddress, String> {
	let s = raw.trim();
	if s.is_empty() {
		return Err("address is empty".into());
	}
	if let Some(rest) = s.strip_prefix('[') {
		let (inner, tail) = rest
			.split_once(']')
			.ok_or_else(|| "missing ']' in bracketed address".to_string())?;
		let ip: std::net::Ipv6Addr = inner
			.parse()
			.map_err(|_| format!("invalid IPv6 address '{inner}'"))?;
		let port = match tail {
			"" => None,
			t => Some(parse_port(
				t.strip_prefix(':')
					.ok_or_else(|| format!("unexpected '{t}' after ']'"))?,
			)?),
		};
		return Ok(ParsedAddress {
			host: AddressHost::Ip(IpAddr::V6(ip)),
			port,
		});
	}
	if let Ok(ip) = s.parse::<IpAddr>() {
		return Ok(ParsedAddress {
			host: AddressHost::Ip(ip),
			port: None,
		});
	}
	let (host, port) = match s.split_once(':') {
		None => (s, None),
		Some((h, p)) => {
			if p.contains(':') {
				return Err(format!("invalid address '{s}'"));
			}
			(h, Some(parse_port(p)?))
		}
	};
	if host.is_empty() {
		return Err("host is empty".into());
	}
	if let Ok(ip) = host.parse::<IpAddr>() {
		return Ok(ParsedAddress {
			host: AddressHost::Ip(ip),
			port,
		});
	}
	if !valid_hostname(host) {
		return Err(format!("invalid host '{host}'"));
	}
	Ok(ParsedAddress {
		host: AddressHost::Name(host.to_string()),
		port,
	})
}

/// Resolve a hostname to IPs under [`RESOLVE_TIMEOUT`]. Failure carries
/// `CoreError::AddrResolutionError` so one-shots exit with the
/// connection code.
pub async fn resolve_host(host: &str) -> anyhow::Result<Vec<IpAddr>> {
	let fail = |why: String| {
		anyhow::Error::new(CoreError::AddrResolutionError)
			.context(format!("cannot resolve host '{host}': {why}"))
	};
	let found = tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host((host, 0)))
		.await
		.map_err(|_| fail("timed out".into()))?
		.map_err(|e| fail(e.to_string()))?;
	let mut ips: Vec<IpAddr> = vec![];
	for sa in found {
		if !ips.contains(&sa.ip()) {
			ips.push(sa.ip());
		}
	}
	if ips.is_empty() {
		return Err(fail("no addresses returned".into()));
	}
	Ok(ips)
}

/// Why a configured `address` failed to contribute a known IP address **and nobody
/// else says so**.
///
/// That is a narrower question than "why is there no address", and the narrower answer
/// is the honest one: a host name is already reported by `build_bc_opts_resolved`,
/// which owns that case and has a rule about how loudly to report it. What is left is
/// the value that does not parse, which no path mentions at all — the `if let Some(Ok(
/// ..))` above swallows it, discovery quietly loses its unicast destination, and the
/// operator's file appears to name a camera that the bridge then cannot wake.
///
/// `None` therefore means: no address configured (a choice), a usable IP literal, or a
/// name (owned elsewhere).
fn unusable_address_reason(cfg: &CameraConfig) -> Option<&'static str> {
	let raw = cfg.address.as_deref()?;
	match parse_address(raw) {
		// A name is not this path's to report. `build_bc_opts_resolved` owns that
		// case: it warns once when resolution fails and a uid can carry the attempt,
		// and it stays deliberately silent when there is no uid, because the error it
		// returns names the host and the caller logs it. Two tests pin both halves of
		// that contract, and a second warning here would contradict the silence that
		// one of them asserts.
		Ok(_) => None,
		Err(_) => Some("the value is not a parseable address"),
	}
}

/// Build a `BcCameraOpt` from the service `CameraConfig` using the same
/// rules the long-running camera connect loop uses. Hostnames are not
/// resolved here; see [`build_bc_opts_resolved`].
pub fn build_bc_opts(cfg: &CameraConfig) -> BcCameraOpt {
	let mut addrs = vec![];
	let mut port = None;

	if let Some(Ok(parsed)) = cfg.address.as_deref().map(parse_address) {
		if let AddressHost::Ip(ip) = parsed.host {
			addrs.push(ip);
		}
		port = parsed.port;
	}
	// An address that cannot be used as a known address is not a TCP problem: the
	// TCP connect path resolves names elsewhere. It is a wake problem, and a quiet
	// one. Local discovery sends its client-initiated packet to the broadcasts on
	// 2015/2018 plus every address in this list, so with an empty list it broadcasts
	// and nothing more — and a doorbell on battery that is asleep does not hear a
	// broadcast. Say so once, loudly, at the level the operator reads, instead of
	// letting the field evidence be a camera that never answers.
	if let Some(reason) = unusable_address_reason(cfg) {
		tracing::warn!(
			name = %cfg.name,
			address = cfg.address.as_deref().unwrap_or(""),
			reason,
			"camera address gives the bridge no known IP address: local discovery will 			 broadcast only, which a sleeping battery camera cannot hear"
		);
	}

	let discovery = match cfg.discovery {
		config::DiscoveryMethod::None => DiscoveryMethods::None,
		config::DiscoveryMethod::Local => DiscoveryMethods::Local,
		config::DiscoveryMethod::Remote => DiscoveryMethods::Remote,
		config::DiscoveryMethod::Map => DiscoveryMethods::Map,
		config::DiscoveryMethod::Relay => DiscoveryMethods::Relay,
		config::DiscoveryMethod::Cellular => DiscoveryMethods::Cellular,
		config::DiscoveryMethod::Cloud => DiscoveryMethods::Cloud,
	};

	BcCameraOpt {
		name: cfg.name.clone(),
		channel_id: cfg.channel_id,
		addrs,
		uid: cfg.uid.clone(),
		port,
		protocol: ConnectionProtocol::TcpUdp,
		discovery,
		// 5 rounds with exponential backoff (1,2,4,8,16 s) is plenty for
		// Reolink's lazy registration to catch up / a battery cam to wake;
		// each round is itself bounded by REGISTRATION_ROUND_TIMEOUT.
		max_discovery_retries: 5,
		credentials: Credentials {
			username: cfg.username.clone(),
			password: cfg.password.clone(),
		},
		cloud_account: cfg.cloud_account.clone(),
		cloud_password: cfg.cloud_password.clone(),
		cloud_mfa_trust_token: cfg.cloud_mfa_trust_token.clone(),
		cloud_refresh_token: cfg.cloud_refresh_token.clone(),
		debug: cfg.debug.unwrap_or(false),
	}
}

/// The options for the one deferred attempt a parked camera is allowed.
///
/// Unlike [`build_bc_opts`] this builder takes the addresses from the caller
/// and pins everything that could put a packet on the air:
///
/// * `addrs` is exactly the addresses the caller can prove, and an empty list
///   means no attempt at all. This function returns `None` in that case rather
///   than build options that would fall through to discovery.
/// * `protocol` is TCP. The UDP arm of `find_camera_with_discoverer` needs a
///   uid, and the uid is dropped below, so discovery cannot be entered even if
///   the two guards below were somehow removed.
/// * `uid` is dropped. With no uid there is nothing to look up, so the local
///   broadcast, the remote lookup, the map and the relay are all unreachable.
/// * `discovery` is `DiscoveryMethods::None`, documented upstream as "Forbid
///   all discovery methods. Only TCP connections with known addresses will
///   work". This is not belt-and-braces over the previous point, it is the
///   reason the previous point holds: the operator's `discovery` key is not
///   inherited here, so a configuration that says `relay` cannot make the
///   automatic path contact a Reolink server. The default for that key is
///   `Relay`, so inheriting it would mean shipping an unbounded path to the
///   real cloud on every failed attempt.
///
/// What a user asked for is a different matter and keeps the full reach: this
/// function is only for the attempt the timer takes on the operator's behalf.
pub fn build_retry_bc_opts(cfg: &CameraConfig, proven: &[IpAddr]) -> Option<BcCameraOpt> {
	let mut opts = build_bc_opts(cfg);
	if proven.is_empty() {
		return None;
	}
	opts.addrs = proven.to_vec();
	opts.uid = None;
	opts.protocol = ConnectionProtocol::Tcp;
	opts.discovery = DiscoveryMethods::None;
	opts.max_discovery_retries = 0;
	Some(opts)
}

/// [`build_bc_opts`] plus DNS resolution of a hostname `address`, run
/// on every connect attempt so DNS/DHCP changes are picked up. With a
/// `uid` set, a failure is warned (camera name + host only) and the
/// connect continues with empty `addrs` so UID discovery runs. Without
/// one, the failure is returned unlogged, naming the host, for the
/// caller to report as a connect failure.
pub async fn build_bc_opts_resolved(cfg: &CameraConfig) -> anyhow::Result<BcCameraOpt> {
	let mut opts = build_bc_opts(cfg);
	if let Some(Ok(ParsedAddress {
		host: AddressHost::Name(host),
		..
	})) = cfg.address.as_deref().map(parse_address)
	{
		match resolve_host(&host).await {
			Ok(ips) => opts.addrs = ips,
			Err(e) if cfg.uid.is_some() => {
				tracing::warn!(camera = %cfg.name, host = %host, "hostname resolution failed, trying UID discovery: {e:#}");
			}
			Err(e) => return Err(e),
		}
	}
	Ok(opts)
}

/// Translate the config-level max-encryption enum into the bairelay_neolink_core
/// value that `BcCamera::login_with_maxenc` expects.
pub fn max_encryption(cfg: &CameraConfig) -> MaxEncryption {
	match cfg.max_encryption {
		config::MaxEncryption::None => MaxEncryption::None,
		config::MaxEncryption::Aes => MaxEncryption::Aes,
		config::MaxEncryption::BcEncrypt => MaxEncryption::BcEncrypt,
	}
}

#[cfg(test)]
mod tests {
	/// The three shapes an operator writes, and what this path can do with them.
	#[test]
	fn an_address_that_yields_no_known_ip_says_which_kind_it_is() {
		let base = CameraConfig {
			name: "Doorbell".into(),
			..CameraConfig::default()
		};
		let mut ip = base.clone();
		ip.address = Some("192.0.2.10:9000".into());
		assert_eq!(unusable_address_reason(&ip), None, "an IP literal is usable");

		// A name is not ours to report: `build_bc_opts_resolved` warns once when it
		// cannot resolve one and a uid can carry the attempt, and asserts its own
		// silence when there is no uid. A second warning here would make that
		// deliberate silence false, which is what two existing tests are for.
		let mut named = base.clone();
		named.address = Some("doorbell.lan:9000".into());
		assert_eq!(unusable_address_reason(&named), None, "the name belongs to the resolved builder");

		let mut broken = base.clone();
		broken.address = Some("192.0.2.10:9000:extra".into());
		assert_eq!(
			unusable_address_reason(&broken),
			Some("the value is not a parseable address"),
			"a malformed value must be named as such"
		);

		// No address at all is a choice, not a mistake: nothing to warn about.
		assert_eq!(unusable_address_reason(&base), None);
	}

	/// The warning must not fire on the path that does resolve names, because that
	/// path does get an address: the guard is about this builder, not about the file.
	#[test]
	fn a_hostname_still_reaches_the_resolved_builder() {
		let mut cfg = CameraConfig {
			name: "Doorbell".into(),
			address: Some("doorbell.lan".into()),
			..CameraConfig::default()
		};
		let opts = build_bc_opts(&cfg);
		assert!(opts.addrs.is_empty(), "this builder does not resolve names");
		cfg.address = Some("192.0.2.10".into());
		let opts = build_bc_opts(&cfg);
		assert_eq!(opts.addrs.len(), 1, "an IP literal is carried to discovery");
		assert_eq!(opts.port, None, "no port written means no port forced");
	}

	use super::*;

	fn sample_cfg() -> CameraConfig {
		toml::from_str(
			r#"
			name = "driveway"
			username = "admin"
			password = "s3cret"
			address = "192.168.1.187:9000"
			uid = "9527000TESTCAM00"
			channel_id = 0
			stream = "all"
			discovery = "relay"
			max_encryption = "aes"
			idle_disconnect = true
			enabled = true
			"#,
		)
		.unwrap()
	}

	#[test]
	fn build_bc_opts_pins_every_field() {
		let cfg = sample_cfg();
		let opts = build_bc_opts(&cfg);

		assert_eq!(opts.name, "driveway");
		assert_eq!(opts.channel_id, 0);
		assert_eq!(opts.uid.as_deref(), Some("9527000TESTCAM00"));
		assert_eq!(opts.port, Some(9000));
		assert!(matches!(opts.protocol, ConnectionProtocol::TcpUdp));
		assert!(matches!(opts.discovery, DiscoveryMethods::Relay));
		assert_eq!(opts.max_discovery_retries, 5);
		assert_eq!(opts.credentials.username, "admin");
		assert_eq!(opts.credentials.password.as_deref(), Some("s3cret"));
		assert!(!opts.debug);
		// Address parse: 192.168.1.187 must be present.
		assert!(opts.addrs.iter().any(|a| a.to_string() == "192.168.1.187"));
	}

	#[test]
	fn max_encryption_maps_every_variant() {
		// sample_cfg() uses max_encryption = "aes".
		assert!(matches!(max_encryption(&sample_cfg()), MaxEncryption::Aes));

		let none_cfg: CameraConfig = toml::from_str(
			r#"
			name = "c"
			username = "u"
			address = "1.2.3.4:9000"
			channel_id = 0
			stream = "all"
			discovery = "local"
			max_encryption = "none"
			"#,
		)
		.unwrap();
		assert!(matches!(max_encryption(&none_cfg), MaxEncryption::None));

		let bc_cfg: CameraConfig = toml::from_str(
			r#"
			name = "c"
			username = "u"
			address = "1.2.3.4:9000"
			channel_id = 0
			stream = "all"
			discovery = "local"
			max_encryption = "bcencrypt"
			"#,
		)
		.unwrap();
		assert!(matches!(max_encryption(&bc_cfg), MaxEncryption::BcEncrypt));
	}

	#[test]
	fn build_bc_opts_wires_debug_knob_and_verbose_alias() {
		for knob in ["debug = true", "verbose = true"] {
			let cfg: CameraConfig = toml::from_str(&format!(
				r#"
				name = "c"
				username = "u"
				address = "1.2.3.4:9000"
				channel_id = 0
				stream = "all"
				discovery = "local"
				max_encryption = "none"
				{knob}
				"#
			))
			.unwrap();
			assert!(build_bc_opts(&cfg).debug, "{knob} should wire through");
		}
	}

	#[test]
	fn build_bc_opts_handles_bare_ip_without_port() {
		let cfg: CameraConfig = toml::from_str(
			r#"
			name = "bare"
			username = "u"
			address = "10.0.0.5"
			channel_id = 0
			stream = "all"
			discovery = "local"
			max_encryption = "none"
			"#,
		)
		.unwrap();
		let opts = build_bc_opts(&cfg);

		assert_eq!(opts.port, None);
		assert!(opts.addrs.iter().any(|a| a.to_string() == "10.0.0.5"));
	}

	#[test]
	fn build_bc_opts_maps_each_discovery_variant() {
		// Cover every arm of the `cfg.discovery → DiscoveryMethods`
		// match so a future addition forces this table to grow.
		let tests = [
			("local", DiscoveryMethods::Local),
			("remote", DiscoveryMethods::Remote),
			("map", DiscoveryMethods::Map),
			("relay", DiscoveryMethods::Relay),
			("cellular", DiscoveryMethods::Cellular),
			("cloud", DiscoveryMethods::Cloud),
		];
		for (name, expected) in tests {
			let cfg: CameraConfig = toml::from_str(&format!(
				r#"
				name = "c"
				username = "u"
				address = "1.2.3.4:9000"
				channel_id = 0
				stream = "all"
				discovery = "{name}"
				max_encryption = "none"
				"#
			))
			.unwrap();
			let opts = build_bc_opts(&cfg);
			assert!(
				std::mem::discriminant(&opts.discovery) == std::mem::discriminant(&expected),
				"discovery {name} should map to {expected:?}"
			);
		}
	}

	#[test]
	fn build_bc_opts_handles_uid_only_no_address() {
		let cfg: CameraConfig = toml::from_str(
			r#"
			name = "uid-only"
			username = "u"
			uid = "ABC123"
			channel_id = 0
			stream = "all"
			discovery = "relay"
			max_encryption = "none"
			"#,
		)
		.unwrap();
		let opts = build_bc_opts(&cfg);

		assert!(opts.addrs.is_empty());
		assert_eq!(opts.port, None);
		assert_eq!(opts.uid.as_deref(), Some("ABC123"));
	}

	#[test]
	fn parse_address_table() {
		let ip = |s: &str| AddressHost::Ip(s.parse().unwrap());
		let name = |s: &str| AddressHost::Name(s.to_string());
		let ok = [
			("127.0.0.1", ip("127.0.0.1"), None),
			("127.0.0.1:9000", ip("127.0.0.1"), Some(9000)),
			("cam1", name("cam1"), None),
			(
				"cam1.example.com:9001",
				name("cam1.example.com"),
				Some(9001),
			),
			("::1", ip("::1"), None),
			("[::1]", ip("::1"), None),
			("[::1]:9000", ip("::1"), Some(9000)),
		];
		for (raw, host, port) in ok {
			assert_eq!(
				parse_address(raw),
				Ok(ParsedAddress { host, port }),
				"{raw}"
			);
		}
		for bad in [
			"",
			" ",
			":9000",
			"cam1:",
			"cam1:abc",
			"cam1:70000",
			"[::1",
			"[::1]x",
			"[zz]:1",
			"a:b:c",
			"bad host",
			"cam1/x",
			"fe80::1%eth0",
		] {
			assert!(parse_address(bad).is_err(), "{bad} should be rejected");
		}
	}

	#[test]
	fn build_bc_opts_keeps_hostname_port_without_ips() {
		let mut cfg = sample_cfg();
		cfg.address = Some("cam1.example.com:9100".into());
		let opts = build_bc_opts(&cfg);
		assert!(opts.addrs.is_empty());
		assert_eq!(opts.port, Some(9100));
	}

	#[tokio::test]
	async fn resolve_host_resolves_localhost() {
		let ips = resolve_host("localhost").await.unwrap();
		assert!(ips.iter().any(|ip| ip.is_loopback()));
	}

	#[tokio::test]
	async fn resolve_host_failure_is_connection_error() {
		let err = resolve_host("nonexistent.invalid").await.unwrap_err();
		assert!(format!("{err:#}").contains("nonexistent.invalid"));
		assert_eq!(crate::oneshot::classify::classify(&err), 4);
	}

	#[tokio::test]
	async fn build_bc_opts_resolved_fills_addrs_for_hostname() {
		let mut cfg = sample_cfg();
		cfg.address = Some("localhost:9000".into());
		let opts = build_bc_opts_resolved(&cfg).await.unwrap();
		assert!(opts.addrs.iter().any(|ip| ip.is_loopback()));
		assert_eq!(opts.port, Some(9000));
	}

	/// Run `fut` on this thread with a WARN-level subscriber; returns its
	/// output and every WARN line it emitted.
	async fn capture_warns<T>(fut: impl std::future::Future<Output = T>) -> (T, String) {
		let captured = crate::test_logs::capture_at(tracing::Level::WARN);
		let out = fut.await;
		(out, captured.text())
	}

	/// Without a uid the failure is the caller's to log: the error names
	/// the host and nothing is warned here.
	#[tokio::test]
	async fn build_bc_opts_resolved_fails_unresolvable_host_without_uid() {
		let mut cfg = sample_cfg();
		cfg.uid = None;
		cfg.address = Some("nonexistent.invalid".into());
		let (res, logs) = capture_warns(build_bc_opts_resolved(&cfg)).await;
		let err = res.unwrap_err();
		assert!(
			format!("{err:#}").contains("nonexistent.invalid"),
			"{err:#}"
		);
		assert_eq!(crate::oneshot::classify::classify(&err), 4);
		assert!(logs.is_empty(), "no warn on the no-uid path, got: {logs}");
	}

	/// With a uid the failure is warned once and UID discovery runs.
	#[tokio::test]
	async fn build_bc_opts_resolved_falls_back_to_uid_on_unresolvable_host() {
		let mut cfg = sample_cfg();
		cfg.uid = Some("UIDCAM1".into());
		cfg.address = Some("nonexistent.invalid:9000".into());
		let (res, logs) = capture_warns(build_bc_opts_resolved(&cfg)).await;
		let opts = res.unwrap();
		assert!(opts.addrs.is_empty());
		assert_eq!(opts.uid.as_deref(), Some("UIDCAM1"));
		assert_eq!(logs.matches("WARN").count(), 1, "{logs}");
		assert!(logs.contains("trying UID discovery"), "{logs}");
	}

	/// The deferred attempt must be structurally incapable of reaching a
	/// server, so the test sets the configuration to the widest reach the
	/// operator can ask for and checks what survives.
	#[test]
	fn the_deferred_attempt_cannot_reach_a_server_even_when_the_toml_asks_for_the_relay() {
		let mut cfg = sample_cfg();
		cfg.address = Some("192.168.1.9:9000".into());
		cfg.uid = Some("UIDCAM1".into());
		cfg.discovery = config::DiscoveryMethod::Relay;
		let proven: Vec<std::net::IpAddr> = vec!["192.168.1.9".parse().unwrap()];
		let opts = build_retry_bc_opts(&cfg, &proven).expect("one proven address is enough");
		assert!(opts.uid.is_none(), "nothing to look up, so nothing to broadcast");
		assert!(
			matches!(opts.protocol, ConnectionProtocol::Tcp),
			"TCP only: {:?}",
			opts.protocol
		);
		assert_eq!(
			opts.discovery,
			DiscoveryMethods::None,
			"the operator's relay is not inherited"
		);
		assert_eq!(opts.addrs, proven, "only the addresses that were handed in");
		assert_eq!(opts.max_discovery_retries, 0);
		// The same configuration, on the normal path, still has its reach: the
		// narrowing belongs to the deferred attempt and to nothing else.
		let normal = build_bc_opts(&cfg);
		assert_eq!(normal.uid.as_deref(), Some("UIDCAM1"));
		assert_eq!(normal.discovery, DiscoveryMethods::Relay);
	}

	#[test]
	fn the_deferred_attempt_is_not_built_without_a_proven_address() {
		let cfg = sample_cfg();
		assert!(
			build_retry_bc_opts(&cfg, &[]).is_none(),
			"no address means no attempt, not an attempt that goes looking"
		);
	}
}
