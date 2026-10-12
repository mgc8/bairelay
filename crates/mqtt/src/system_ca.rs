//! The operating system's trust store as a PEM bundle, for MQTT over
//! TLS without a configured CA (`[mqtt] tls = true`, no `ca`). PEM keeps
//! the bundle on the same path as a `ca` file, so client certificates
//! and validation work identically.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::error::MqttError;

/// Load the OS trust store and return it PEM-encoded. Unreadable
/// sources are logged and skipped; an empty store is an error.
pub fn system_ca_bundle() -> Result<String, MqttError> {
	let loaded = rustls_native_certs::load_native_certs();
	for e in &loaded.errors {
		tracing::warn!(error = %e, "skipping unreadable system CA source");
	}
	if loaded.certs.is_empty() {
		return Err(MqttError::ConfigError(
			"[mqtt] tls = true without ca, but no system CA certificates were found; \
			 set ca to the broker's CA file"
				.into(),
		));
	}
	Ok(pem_bundle(loaded.certs.iter().map(|c| c.as_ref())))
}

/// PEM-encode DER certificates, 64-column base64 per RFC 7468.
fn pem_bundle<'a>(certs: impl Iterator<Item = &'a [u8]>) -> String {
	let mut out = String::new();
	for der in certs {
		out.push_str("-----BEGIN CERTIFICATE-----\n");
		let b64 = BASE64.encode(der);
		for line in b64.as_bytes().chunks(64) {
			out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
			out.push('\n');
		}
		out.push_str("-----END CERTIFICATE-----\n");
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn pem_bundle_wraps_at_64_columns_with_markers() {
		let der = vec![0xABu8; 100]; // 136 base64 chars → 64 + 64 + 8
		let pem = pem_bundle([der.as_slice()].into_iter());
		let lines: Vec<&str> = pem.lines().collect();
		assert_eq!(lines.first(), Some(&"-----BEGIN CERTIFICATE-----"));
		assert_eq!(lines.last(), Some(&"-----END CERTIFICATE-----"));
		assert_eq!(lines[1].len(), 64);
		assert_eq!(lines[2].len(), 64);
		assert_eq!(lines[3].len(), 8);
		assert_eq!(BASE64.decode(lines[1..4].concat()).unwrap(), der);
	}

	#[test]
	fn pem_bundle_concatenates_certificates() {
		let pem = pem_bundle([&[1u8][..], &[2u8][..]].into_iter());
		assert_eq!(pem.matches("-----BEGIN CERTIFICATE-----").count(), 2);
	}
}
