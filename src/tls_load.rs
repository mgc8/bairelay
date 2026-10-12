//! Load and parse the TLS PEMs referenced by [`Config`]: the RTSPS
//! server certificate (+ optional client CA) and the MQTT broker CA /
//! client certificate.
//!
//! All file I/O happens here so `main.rs` only sees parsed material.
//! Errors carry the offending path (or config field, for inline PEM) so
//! operators do not have to grep their config to find what bairelay
//! tried to open.

use std::fs;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bairelay_rtsp::server::{ClientAuthMode, TlsConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;

use crate::config::TlsClientAuth;

/// Parsed TLS material ready to hand to `bairelay_rtsp::server::ServerConfig`.
#[derive(Debug)]
pub struct LoadedTls {
	pub tls_config: TlsConfig,
}

/// Read and parse the server PEM, optionally read+parse the client-CA
/// PEM, then build the rustls server config.
pub fn load_server_tls(
	cert_path: &str,
	tls_client_auth: TlsClientAuth,
	client_ca_path: Option<&str>,
) -> Result<LoadedTls> {
	let pem =
		fs::read(cert_path).with_context(|| format!("failed to read TLS cert PEM: {cert_path}"))?;
	let PemItems { certs: chain, key } = parse_pem(&pem, cert_path)?;
	if chain.is_empty() {
		return Err(anyhow!("TLS cert PEM has no certificate: {cert_path}"));
	}
	let key = key.ok_or_else(|| anyhow!("TLS cert PEM has no private key: {cert_path}"))?;

	let client_auth = match tls_client_auth {
		TlsClientAuth::None => ClientAuthMode::None,
		other => {
			let ca_path = client_ca_path.ok_or_else(|| {
				anyhow!(
					"internal: tls_client_auth requires tls_client_ca; \
					 should have been caught by config validation"
				)
			})?;
			let roots = load_ca_roots(ca_path)?;
			match other {
				TlsClientAuth::Request => ClientAuthMode::Request { roots },
				TlsClientAuth::Require => ClientAuthMode::Require { roots },
				TlsClientAuth::None => unreachable!(),
			}
		}
	};

	let tls_config = TlsConfig::build(chain, key, client_auth)
		.with_context(|| format!("invalid TLS config (cert {cert_path})"))?;
	Ok(LoadedTls { tls_config })
}

/// Certificates and the (at most one) private key found in a PEM blob.
struct PemItems {
	certs: Vec<CertificateDer<'static>>,
	key: Option<PrivateKeyDer<'static>>,
}

/// Parse every certificate and private key in `pem`. `label` (a path or
/// config field) names the source in errors.
fn parse_pem(pem: &[u8], label: &str) -> Result<PemItems> {
	let mut certs = Vec::new();
	let mut key = None;
	for item in rustls_pemfile::read_all(&mut Cursor::new(pem)) {
		let parsed = item.with_context(|| format!("PEM parse error in {label}"))?;
		match parsed {
			rustls_pemfile::Item::X509Certificate(c) => certs.push(c),
			rustls_pemfile::Item::Pkcs1Key(k) => {
				set_key_or_reject(&mut key, PrivateKeyDer::Pkcs1(k), label)?
			}
			rustls_pemfile::Item::Pkcs8Key(k) => {
				set_key_or_reject(&mut key, PrivateKeyDer::Pkcs8(k), label)?
			}
			rustls_pemfile::Item::Sec1Key(k) => {
				set_key_or_reject(&mut key, PrivateKeyDer::Sec1(k), label)?
			}
			_ => {}
		}
	}
	Ok(PemItems { certs, key })
}

/// Reject a PEM bundle that carries more than one private key.
///
/// `cat key1.pem key2.pem cert.pem` would otherwise silently let the
/// last key parsed become the server key — likely mismatched against
/// the cert chain, surfacing as an opaque rustls "key does not match
/// certificate" error at startup. Fail fast with the operator's path.
fn set_key_or_reject(
	slot: &mut Option<PrivateKeyDer<'static>>,
	new_key: PrivateKeyDer<'static>,
	cert_path: &str,
) -> Result<()> {
	if slot.is_some() {
		return Err(anyhow!(
			"TLS cert PEM contains more than one private key: {cert_path}"
		));
	}
	*slot = Some(new_key);
	Ok(())
}

fn load_ca_roots(ca_path: &str) -> Result<Arc<RootCertStore>> {
	let pem =
		fs::read(ca_path).with_context(|| format!("failed to read tls_client_ca: {ca_path}"))?;
	let certs = parse_pem(&pem, ca_path)?.certs;
	if certs.is_empty() {
		return Err(anyhow!("tls_client_ca PEM has no certificates: {ca_path}"));
	}
	let mut roots = RootCertStore::empty();
	for der in certs {
		roots
			.add(der)
			.with_context(|| format!("rustls rejected CA cert in {ca_path}"))?;
	}
	Ok(Arc::new(roots))
}

// ── MQTT broker TLS ─────────────────────────────────────────────────

/// MQTT client TLS material, validated and ready for the broker client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttTls {
	/// CA bundle (PEM) the broker certificate is verified against.
	pub ca_pem: String,
	/// Client certificate + private key (PEM) for mutual TLS.
	pub client_auth: Option<(String, String)>,
}

/// Read a config value that is either a PEM file path or inline PEM
/// text. Inline is recognised by a `-----BEGIN` marker; anything else
/// is a path (the Neolink form). `field` names the config key in errors.
pub fn read_pem_source(field: &str, value: &str) -> Result<String> {
	if value.contains("-----BEGIN") {
		return Ok(value.to_string());
	}
	let label = pem_label(value);
	let bytes = fs::read(value).with_context(|| format!("failed to read {field}: {label}"))?;
	String::from_utf8(bytes).map_err(|_| anyhow!("{field} is not a PEM text file: {label}"))
}

/// Load and validate `[mqtt] ca` / `client_auth`. `Ok(None)` when TLS
/// isn't configured. Runs the same checks the MQTT client's rustls
/// setup does — every CA certificate must load into a root store, and
/// the client key must load and match the leaf certificate — so a
/// wrong path, bad PEM or mismatched pair fails at startup (and in
/// `check-config`) rather than on every broker connect.
pub fn load_mqtt_tls(
	ca: Option<&str>,
	client_auth: Option<(&str, &str)>,
) -> Result<Option<MqttTls>> {
	let Some(ca) = ca else {
		if client_auth.is_some() {
			return Err(anyhow!(
				"[mqtt] client_auth needs ca: TLS to the broker is enabled by setting ca"
			));
		}
		return Ok(None);
	};
	let ca_pem = read_pem_source("[mqtt] ca", ca)?;
	let ca_certs = parse_pem(ca_pem.as_bytes(), "[mqtt] ca")?.certs;
	if ca_certs.is_empty() {
		return Err(anyhow!("[mqtt] ca has no certificates: {}", pem_label(ca)));
	}
	let mut roots = RootCertStore::empty();
	for der in ca_certs {
		roots
			.add(der)
			.with_context(|| format!("[mqtt] ca: unusable CA certificate in {}", pem_label(ca)))?;
	}
	let client_auth = match client_auth {
		None => None,
		Some((cert, key)) => {
			let cert_pem = read_pem_source("[mqtt] client_auth certificate", cert)?;
			let chain = parse_pem(cert_pem.as_bytes(), "[mqtt] client_auth certificate")?.certs;
			if chain.is_empty() {
				return Err(anyhow!(
					"[mqtt] client_auth certificate has no certificate: {}",
					pem_label(cert)
				));
			}
			let key_pem = read_pem_source("[mqtt] client_auth key", key)?;
			let Some(private_key) = parse_pem(key_pem.as_bytes(), "[mqtt] client_auth key")?.key
			else {
				return Err(anyhow!(
					"[mqtt] client_auth key has no private key: {}",
					pem_label(key)
				));
			};
			// Same check rustls runs when the MQTT client builds its TLS
			// config: the key must load and match the first (leaf) cert.
			// Explicit provider, so `check-config` needs no global install.
			let provider = rustls::crypto::aws_lc_rs::default_provider();
			rustls::sign::CertifiedKey::from_der(chain, private_key, &provider).map_err(|e| {
				anyhow!(
					"[mqtt] client_auth: certificate and key don't form a usable pair ({e}): {} / {}",
					pem_label(cert),
					pem_label(key)
				)
			})?;
			Some((cert_pem, key_pem))
		}
	};
	Ok(Some(MqttTls {
		ca_pem,
		client_auth,
	}))
}

/// How a PEM source appears in errors: its path, or a description for
/// inline text — including inline text missing its `-----BEGIN` line,
/// which is otherwise read as a path — so key material never lands in
/// logs.
fn pem_label(value: &str) -> &str {
	if value.contains("-----BEGIN") {
		"inline PEM"
	} else if value.contains('\n') || value.len() > 256 {
		"inline value without a -----BEGIN line"
	} else {
		value
	}
}

/// Throwaway PKI for tests: a CA plus a client certificate it signed.
#[cfg(test)]
pub(crate) mod test_pki {
	use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};

	pub(crate) struct Pki {
		pub ca_pem: String,
		pub client_cert_pem: String,
		pub client_key_pem: String,
	}

	impl Pki {
		pub(crate) fn generate() -> Self {
			let ca_kp = KeyPair::generate().unwrap();
			let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
			ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
			ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
			let ca = ca_params.self_signed(&ca_kp).unwrap();
			let client_kp = KeyPair::generate().unwrap();
			let client = CertificateParams::new(vec!["bairelay".into()])
				.unwrap()
				.signed_by(&client_kp, &ca, &ca_kp)
				.unwrap();
			Pki {
				ca_pem: ca.pem(),
				client_cert_pem: client.pem(),
				client_key_pem: client_kp.serialize_pem(),
			}
		}
	}
}

#[cfg(test)]
mod mqtt_tls_tests {
	use super::test_pki::Pki;
	use super::*;

	fn file(dir: &tempfile::TempDir, name: &str, body: &str) -> String {
		let p = dir.path().join(name);
		std::fs::write(&p, body).unwrap();
		p.to_string_lossy().into_owned()
	}

	#[test]
	fn no_ca_and_no_client_auth_is_plain_mqtt() {
		assert_eq!(load_mqtt_tls(None, None).unwrap(), None);
	}

	/// Neolink form: file paths.
	#[test]
	fn paths_are_read_and_validated() {
		let pki = Pki::generate();
		let dir = tempfile::tempdir().unwrap();
		let ca = file(&dir, "ca.pem", &pki.ca_pem);
		let cert = file(&dir, "c.pem", &pki.client_cert_pem);
		let key = file(&dir, "c.key", &pki.client_key_pem);
		let tls = load_mqtt_tls(Some(&ca), Some((&cert, &key)))
			.unwrap()
			.unwrap();
		assert_eq!(tls.ca_pem, pki.ca_pem);
		assert_eq!(
			tls.client_auth,
			Some((pki.client_cert_pem, pki.client_key_pem))
		);
	}

	/// Existing configs with the PEM inline keep working.
	#[test]
	fn inline_pem_is_accepted() {
		let pki = Pki::generate();
		let tls = load_mqtt_tls(Some(&pki.ca_pem), None).unwrap().unwrap();
		assert_eq!(tls.ca_pem, pki.ca_pem);
		assert_eq!(tls.client_auth, None);
	}

	#[test]
	fn client_auth_without_ca_is_rejected() {
		let pki = Pki::generate();
		let auth = Some((pki.client_cert_pem.as_str(), pki.client_key_pem.as_str()));
		let err = load_mqtt_tls(None, auth)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("client_auth needs ca"), "got: {err}");
	}

	#[test]
	fn ca_without_certificates_is_rejected() {
		let pki = Pki::generate();
		let dir = tempfile::tempdir().unwrap();
		let ca = file(&dir, "ca.pem", &pki.client_key_pem); // a key, no cert
		let err = load_mqtt_tls(Some(&ca), None)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("ca has no certificates"), "got: {err}");
		assert!(err.contains(&ca), "error names the path: {err}");
	}

	/// Cert and key swapped in `client_auth` fail at load time.
	#[test]
	fn swapped_client_cert_and_key_are_rejected() {
		let pki = Pki::generate();
		let auth = Some((pki.client_key_pem.as_str(), pki.client_cert_pem.as_str()));
		let err = load_mqtt_tls(Some(&pki.ca_pem), auth)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("has no certificate"), "got: {err}");
	}

	/// Errors about inline PEM name the field, never echo the PEM text.
	#[test]
	fn inline_pem_errors_do_not_echo_the_pem() {
		let pki = Pki::generate();
		let err = load_mqtt_tls(Some(&pki.client_key_pem), None)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("inline PEM"), "got: {err}");
		assert!(!err.contains("PRIVATE KEY"), "PEM leaked into error: {err}");
	}

	/// A client cert and a key from different pairs fail at load time,
	/// not at every broker connect.
	#[test]
	fn mismatched_client_cert_and_key_are_rejected() {
		let a = Pki::generate();
		let b = Pki::generate();
		let auth = Some((a.client_cert_pem.as_str(), b.client_key_pem.as_str()));
		let err = load_mqtt_tls(Some(&a.ca_pem), auth)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("don't form a usable pair"), "got: {err}");
	}

	/// Inline key text whose `-----BEGIN` line got lost is read as a
	/// path; the failure must not echo it.
	#[test]
	fn malformed_inline_value_is_not_echoed() {
		let pki = Pki::generate();
		let body: String = pki
			.client_key_pem
			.lines()
			.filter(|l| !l.starts_with("-----"))
			.collect::<Vec<_>>()
			.join("\n");
		let err = load_mqtt_tls(Some(&body), None)
			.expect_err("must fail")
			.to_string();
		assert!(err.contains("without a -----BEGIN line"), "got: {err}");
		assert!(!err.contains(&body[..20]), "key text leaked: {err}");
	}

	#[test]
	fn missing_file_names_field_and_path() {
		let err = read_pem_source("[mqtt] ca", "/nonexistent/ca.pem")
			.expect_err("must fail")
			.to_string();
		assert!(
			err.contains("[mqtt] ca") && err.contains("/nonexistent/ca.pem"),
			"got: {err}"
		);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
	use std::io::Write;
	use tempfile::NamedTempFile;

	fn install_crypto() {
		// Forward to the shared `bairelay_rtsp::server::install_crypto_provider`
		// helper so the OnceLock + Err-swallow semantics live in exactly one
		// place across the workspace.
		bairelay_rtsp::server::install_crypto_provider();
	}

	fn write_temp_pem(contents: &[u8]) -> NamedTempFile {
		let mut f = NamedTempFile::new().unwrap();
		f.write_all(contents).unwrap();
		f.flush().unwrap();
		f
	}

	fn ca_and_server_bundle() -> (String, String) {
		// (server-bundle.pem, ca.pem)
		let ca_kp = KeyPair::generate().unwrap();
		let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
		ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
		ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
		let ca = ca_params.self_signed(&ca_kp).unwrap();

		let server_kp = KeyPair::generate().unwrap();
		let server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
		let server = server_params.signed_by(&server_kp, &ca, &ca_kp).unwrap();

		let server_bundle = format!("{}{}", server.pem(), server_kp.serialize_pem());
		let ca_pem = ca.pem();
		(server_bundle, ca_pem)
	}

	#[test]
	fn load_server_tls_happy_path_no_client_auth() {
		install_crypto();
		let (bundle, _ca) = ca_and_server_bundle();
		let bundle_file = write_temp_pem(bundle.as_bytes());
		let res = load_server_tls(
			bundle_file.path().to_str().unwrap(),
			TlsClientAuth::None,
			None,
		);
		assert!(res.is_ok(), "load failed: {res:?}");
	}

	#[test]
	fn load_server_tls_happy_path_require_client_auth() {
		install_crypto();
		let (bundle, ca) = ca_and_server_bundle();
		let bundle_file = write_temp_pem(bundle.as_bytes());
		let ca_file = write_temp_pem(ca.as_bytes());
		let res = load_server_tls(
			bundle_file.path().to_str().unwrap(),
			TlsClientAuth::Require,
			Some(ca_file.path().to_str().unwrap()),
		);
		assert!(res.is_ok(), "load failed: {res:?}");
	}

	#[test]
	fn load_server_tls_missing_cert_file() {
		install_crypto();
		let res = load_server_tls("/nonexistent/path.pem", TlsClientAuth::None, None);
		let err = res.expect_err("must fail").to_string();
		assert!(err.contains("failed to read TLS cert PEM"), "got: {err}");
	}

	#[test]
	fn load_server_tls_pem_without_certificate() {
		install_crypto();
		let server_kp = KeyPair::generate().unwrap();
		let key_only = server_kp.serialize_pem();
		let f = write_temp_pem(key_only.as_bytes());
		let res = load_server_tls(f.path().to_str().unwrap(), TlsClientAuth::None, None);
		let err = res.expect_err("must fail").to_string();
		assert!(err.contains("no certificate"), "got: {err}");
	}

	#[test]
	fn load_server_tls_pem_without_private_key() {
		install_crypto();
		// Mint a fresh server cert and write ONLY the cert (no key) so
		// the failure mode under test is the operator's "I forgot to
		// concatenate the key into the bundle" mistake, not "I fed the
		// CA cert by accident."
		let ca_kp = KeyPair::generate().unwrap();
		let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
		ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
		ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
		let ca = ca_params.self_signed(&ca_kp).unwrap();
		let server_kp = KeyPair::generate().unwrap();
		let server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
		let server = server_params.signed_by(&server_kp, &ca, &ca_kp).unwrap();

		let cert_only = server.pem();
		let f = write_temp_pem(cert_only.as_bytes());
		let res = load_server_tls(f.path().to_str().unwrap(), TlsClientAuth::None, None);
		let err = res.expect_err("must fail").to_string();
		assert!(err.contains("no private key"), "got: {err}");
	}

	#[test]
	fn load_server_tls_rejects_multiple_keys_in_bundle() {
		install_crypto();
		// `cat key1.pem key2.pem cert.pem` → previously last-key-wins,
		// silently mismatched against the cert. Now an explicit error.
		let (mut bundle, _ca) = ca_and_server_bundle();
		let stray_kp = KeyPair::generate().unwrap();
		bundle.push_str(&stray_kp.serialize_pem());
		let f = write_temp_pem(bundle.as_bytes());
		let res = load_server_tls(f.path().to_str().unwrap(), TlsClientAuth::None, None);
		let err = res.expect_err("must fail").to_string();
		assert!(err.contains("more than one private key"), "got: {err}");
	}

	#[test]
	fn load_server_tls_client_ca_empty() {
		install_crypto();
		let (bundle, _ca) = ca_and_server_bundle();
		let bundle_file = write_temp_pem(bundle.as_bytes());
		// Write a non-PEM file so certs() yields nothing.
		let empty = write_temp_pem(b"# nothing here\n");
		let res = load_server_tls(
			bundle_file.path().to_str().unwrap(),
			TlsClientAuth::Request,
			Some(empty.path().to_str().unwrap()),
		);
		let err = res.expect_err("must fail").to_string();
		assert!(err.contains("no certificates"), "got: {err}");
	}
}
