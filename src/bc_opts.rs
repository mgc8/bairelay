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

	let discovery = match cfg.discovery {
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

	use std::sync::{Arc, Mutex};

	/// Shared in-memory sink for log lines emitted under
	/// [`capture_warns`].
	#[derive(Clone, Default)]
	struct LogSink(Arc<Mutex<Vec<u8>>>);

	impl std::io::Write for LogSink {
		fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
			self.0.lock().unwrap().extend_from_slice(buf);
			Ok(buf.len())
		}
		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}

	/// Run `fut` on this thread with a WARN-level subscriber; returns its
	/// output and every WARN line it emitted.
	async fn capture_warns<T>(fut: impl std::future::Future<Output = T>) -> (T, String) {
		let sink = LogSink::default();
		let writer = sink.clone();
		let subscriber = tracing_subscriber::fmt()
			.with_max_level(tracing::Level::WARN)
			.with_ansi(false)
			.with_writer(move || writer.clone())
			.finish();
		let guard = tracing::subscriber::set_default(subscriber);
		let out = fut.await;
		drop(guard);
		let logs = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
		(out, logs)
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
}
