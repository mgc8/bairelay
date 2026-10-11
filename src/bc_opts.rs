//! `BcCameraOpt` builder shared by the camera connect loop and CLI.

use crate::config::{self, CameraConfig};
use bairelay_neolink_core::bc_protocol::{
	BcCameraOpt, ConnectionProtocol, Credentials, DiscoveryMethods, MaxEncryption,
};
use std::net::IpAddr;

/// Split a camera `address` (`ip`, `ip:port`, `[v6]:port`) into its IP
/// and optional port. Hostnames yield no IP — only literals are used.
pub fn parse_address(addr: &str) -> (Option<IpAddr>, Option<u16>) {
	if let Ok(ip) = addr.parse::<IpAddr>() {
		return (Some(ip), None);
	}
	match addr.rsplit_once(':') {
		Some((host, port)) => (
			host.trim_start_matches('[')
				.trim_end_matches(']')
				.parse()
				.ok(),
			port.parse().ok(),
		),
		None => (None, None),
	}
}

/// Build a `BcCameraOpt` from the service `CameraConfig` using the same
/// rules the long-running camera connect loop uses.
pub fn build_bc_opts(cfg: &CameraConfig) -> BcCameraOpt {
	let (ip, port) = cfg.address.as_deref().map_or((None, None), parse_address);
	let addrs: Vec<IpAddr> = ip.into_iter().collect();

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
	fn parse_address_accepts_ip_with_and_without_port() {
		let v4: IpAddr = "192.168.1.187".parse().unwrap();
		let v6: IpAddr = "fe80::1".parse().unwrap();
		assert_eq!(parse_address("192.168.1.187"), (Some(v4), None));
		assert_eq!(parse_address("192.168.1.187:9000"), (Some(v4), Some(9000)));
		assert_eq!(parse_address("fe80::1"), (Some(v6), None));
		assert_eq!(parse_address("[fe80::1]:9000"), (Some(v6), Some(9000)));
		assert_eq!(parse_address("cam.local:9000"), (None, Some(9000)));
		assert_eq!(parse_address("cam.local"), (None, None));
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
}
