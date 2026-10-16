//! Cloud bundle minting for account ("cloud") cameras.
//!
//! When a Reolink battery camera is bound to a Reolink account it stops
//! accepting the legacy local login: the only way in is the **sigV3** login,
//! which carries a short-lived, cloud-signed device-access token. This module
//! mints that token bundle from `apis.reolink.com` using the operator's account
//! credentials, exactly as the official app does:
//!
//! 1. OAuth2 password grant → access + refresh token.
//! 2. Refresh grant with `grant_session_code=true` → the session-scoped access
//!    token the app uses for device-access calls.
//! 3. `POST /v2/devices/access-authorization` → the `{token{p,s,k}, certChain}`
//!    bundle, solving a hashcash proof-of-work challenge if the cloud demands
//!    one (`error.code == 8214`).
//!
//! The bundle is short-lived, so [`mint_bundle`](crate::cloud::mint_bundle) caches it per-UID only until
//! shortly before its `exp` and re-mints after that — battery cameras wake
//! often, and replaying the full OAuth flow per wake would risk Reolink-side
//! rate-limiting. See `login::run_sigv3_direct`.

use crate::{Error, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const BASE: &str = "https://apis.reolink.com";
/// The official Reolink iOS app's OAuth client id + User-Agent. The cloud only
/// issues tokens to a recognised client; bairelay replaces the app, so it must
/// present the app's identity.
const CLIENT_ID: &str = "REO-BHAPEi1tILWrc37S|Zit";
const USER_AGENT: &str = "Reolink iOS App/4.60.3.0 (REO-BHAPEi1tILWrc37S|Zit; iPadOS/26.4)";
/// Cap on a single cloud response body. The cloud is treated as hostile; a
/// compromised / hijacked endpoint must not be able to OOM the process by
/// streaming an unbounded body. Real bundles are ~4 KiB.
const MAX_RESP_BYTES: usize = 1 << 20; // 1 MiB
/// Re-mint margin: drop a cached bundle this many seconds before its `exp` so a
/// login never races the token's expiry.
const CACHE_MARGIN_SECS: i64 = 120;
/// Diagnostic-message truncation cap (chars). Generous so a full Reolink error
/// envelope — e.g. the MFA `allowMethods` block on an `8208` — is shown intact,
/// while still bounding a pathological body.
const SHORT_MAX_CHARS: usize = 2000;

/// Test-only override for the API base URL (set by the mock-server test).
#[cfg(test)]
static TEST_BASE: OnceLock<String> = OnceLock::new();

/// The API base URL — the real cloud, or a test mock when overridden.
fn base() -> &'static str {
	#[cfg(test)]
	if let Some(b) = TEST_BASE.get() {
		return b.as_str();
	}
	BASE
}

/// A cloud-signed device-access bundle for the sigV3 login.
#[derive(Debug, Clone)]
pub struct Sigv3Bundle {
	/// `token.p` — the signed claims (JSON string: ver/sid/sub/exp/role/…).
	pub token_p: String,
	/// `token.s` — the RS256 signature over `token_p` (base64).
	pub token_s: String,
	/// `token.k` — the `tokenKey` echoed verbatim in the login (base64).
	pub token_k: String,
	/// The reolink.com PEM certificate chain the camera validates `token_s`
	/// against. Newlines are normalised to `\n`.
	pub cert_chain: String,
}

fn cloud<S: Into<String>>(msg: S) -> Error {
	Error::Cloud(msg.into())
}

/// Shared reqwest client: built once with timeouts (a slow / hijacked
/// `apis.reolink.com` must not be able to wedge a camera connect forever) and
/// reused across mints so the per-wake calls keep connections warm.
fn http() -> Result<&'static reqwest::Client> {
	static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
	if let Some(c) = CLIENT.get() {
		return Ok(c);
	}
	let c = reqwest::Client::builder()
		.user_agent(USER_AGENT)
		.connect_timeout(Duration::from_secs(10))
		.timeout(Duration::from_secs(20))
		.build()
		.map_err(|e| cloud(format!("http client: {e}")))?;
	Ok(CLIENT.get_or_init(|| c))
}

/// Cache of minted bundles keyed by UID, with the token's `exp` (unix secs).
/// Battery cameras wake often; without this, every wake would replay the full
/// OAuth password-grant + refresh + access-authorization, risking Reolink-side
/// rate-limiting of the operator's account.
fn cache() -> &'static Mutex<HashMap<String, (Sigv3Bundle, i64)>> {
	static CACHE: OnceLock<Mutex<HashMap<String, (Sigv3Bundle, i64)>>> = OnceLock::new();
	CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// `exp` (unix seconds) from a `token.p` claims JSON, if present.
fn token_exp(token_p: &str) -> Option<i64> {
	serde_json::from_str::<Value>(token_p)
		.ok()?
		.get("exp")?
		.as_i64()
}

/// Mint (or reuse a still-valid cached) sigV3 bundle for `uid` using the
/// operator's Reolink account. `mfa_trust_token` is the host's stored MFA
/// trust token (from a prior `cloud-authorise` bootstrap); when present it is
/// sent with the password grant so a host whose IP Reolink would otherwise
/// challenge (`8208 mfa_required`) clears MFA headlessly.
pub async fn mint_bundle(
	account: &str,
	password: &str,
	uid: &str,
	mfa_trust_token: Option<&str>,
	refresh_token: Option<&str>,
) -> Result<Sigv3Bundle> {
	let now = time::OffsetDateTime::now_utc().unix_timestamp();
	if let Some((bundle, exp)) = cache().lock().unwrap().get(uid) {
		if *exp - CACHE_MARGIN_SECS > now {
			return Ok(bundle.clone());
		}
	}

	let bundle = mint_uncached(account, password, uid, mfa_trust_token, refresh_token).await?;
	if let Some(exp) = token_exp(&bundle.token_p) {
		cache()
			.lock()
			.unwrap()
			.insert(uid.to_owned(), (bundle.clone(), exp));
	}
	Ok(bundle)
}

/// Test-only: seed the per-UID bundle cache so [`mint_bundle`] returns it
/// without any network call. Lets login-flow tests drive the full cloud login
/// (mint → signed login) offline. Pair with [`drop_cache_for_test`].
#[cfg(test)]
pub(crate) fn seed_cache_for_test(uid: &str, bundle: Sigv3Bundle, exp: i64) {
	cache()
		.lock()
		.unwrap()
		.insert(uid.to_owned(), (bundle, exp));
}

/// Test-only: remove a [`seed_cache_for_test`] entry (the cache is a
/// process-global static, so tests clean up after themselves).
#[cfg(test)]
pub(crate) fn drop_cache_for_test(uid: &str) {
	cache().lock().unwrap().remove(uid);
}

async fn mint_uncached(
	account: &str,
	password: &str,
	uid: &str,
	mfa_trust_token: Option<&str>,
	refresh_token: Option<&str>,
) -> Result<Sigv3Bundle> {
	let client = http()?;

	// 1. Obtain a refresh token for the session-scoped grant. A stored refresh
	//    token (from `cloud-authorise`) is reused directly — that skips the
	//    password grant, and therefore MFA, entirely. Otherwise do the password
	//    grant, sending a stored trust token (session_mode + mfa_trusted) when
	//    present so an otherwise-challenged host clears MFA without the email.
	let refresh: String = if let Some(rt) = refresh_token {
		rt.to_owned()
	} else {
		let mut form = vec![
			("client_id", CLIENT_ID),
			("grant_type", "password"),
			("username", account),
			("password", password),
		];
		if let Some(token) = mfa_trust_token {
			form.push(("session_mode", "true"));
			form.push(("mfa_trusted", "true"));
			form.push(("mfa_trust_token", token));
		}
		let tok = oauth(client, &form).await?;
		tok.get("refresh_token")
			.and_then(Value::as_str)
			.ok_or_else(|| cloud("login returned no refresh_token (bad account/password?)"))?
			.to_owned()
	};

	// 2. refresh grant with session code -> the device-access bearer.
	let session = oauth(
		client,
		&[
			("refresh_token", refresh.as_str()),
			("client_id", CLIENT_ID),
			("grant_session_code", "true"),
			("grant_type", "refresh_token"),
		],
	)
	.await?;
	let bearer = session
		.get("access_token")
		.and_then(Value::as_str)
		.ok_or_else(|| cloud("session grant returned no access_token"))?;

	// 3. access-authorization -> bundle (solving PoW once if challenged).
	let body = serde_json::json!({ "uid": uid, "protocol": 3, "certChain": true });
	let mut resp = access_authorization(client, bearer, &body, None).await?;
	if let Some(challenge) = pow_challenge(&resp) {
		// The hashcash loop is pure CPU and the difficulty is cloud-supplied;
		// keep it off the async worker threads.
		let header = tokio::task::spawn_blocking(move || solve_pow(&challenge))
			.await
			.map_err(|e| cloud(format!("pow task: {e}")))??;
		resp = access_authorization(client, bearer, &body, Some(&header)).await?;
	}

	parse_bundle(&resp)
}

/// Extract the bundle from a successful `access-authorization` response. The
/// real API returns `token` / `certChain` at the top level; a `data` object is
/// accepted as a wrapper, but an error envelope (no token) is rejected.
fn parse_bundle(resp: &Value) -> Result<Sigv3Bundle> {
	if let Some(err) = resp.get("error").filter(|e| !e.is_null()) {
		return Err(cloud(format!("access-authorization error: {}", short(err))));
	}
	let data = resp.get("data").filter(|d| d.is_object()).unwrap_or(resp);
	let token = data.get("token").filter(|t| t.is_object()).ok_or_else(|| {
		cloud(format!(
			"access-authorization had no token: {}",
			short(resp)
		))
	})?;
	let get = |k: &str| -> Result<String> {
		token
			.get(k)
			.and_then(Value::as_str)
			.map(str::to_owned)
			.ok_or_else(|| cloud(format!("token missing field '{k}'")))
	};
	let cert_chain = data
		.get("certChain")
		.and_then(Value::as_str)
		.ok_or_else(|| cloud("access-authorization had no certChain"))?
		.replace("\r\n", "\n");
	Ok(Sigv3Bundle {
		token_p: get("p")?,
		token_s: get("s")?,
		token_k: get("k")?,
		cert_chain,
	})
}

fn common(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
	req.header("x-client-id", CLIENT_ID)
		.header("x-api-challenge-accept", "pow/1,captcha/1")
		.header("accept", "*/*")
}

async fn oauth(client: &reqwest::Client, form: &[(&str, &str)]) -> Result<Value> {
	let resp = common(client.post(format!("{}/v1.0/oauth2/token/", base())))
		.form(form)
		.send()
		.await
		.map_err(|e| cloud(format!("oauth request: {e}")))?;
	let v = json(resp).await?;
	// Require the token grant to actually carry a token; a Reolink error
	// envelope (which can still contain a `data` key) is a failure, not success.
	let has_token = |o: &Value| {
		o.get("access_token").and_then(Value::as_str).is_some()
			|| o.get("refresh_token").and_then(Value::as_str).is_some()
	};
	if has_token(&v) || v.get("data").is_some_and(has_token) {
		Ok(v)
	} else {
		Err(oauth_error(&v))
	}
}

/// Map a token-less OAuth response to an error. Reolink returns
/// `error.code 8208` ("the extra identification is required") when the account
/// demands login verification (MFA): either the explicit two-step verification
/// toggle, OR a risk-based check on an unfamiliar device / IP — which fires from
/// a new or datacenter IP even with two-step verification turned off. Point the
/// operator at `cloud-authorise` (which clears it once and stores a reusable
/// token) and prefix the raw envelope (its `allowMethods` block shows the
/// account's methods). Any other failure (e.g. `8200` wrong password) keeps just
/// the raw envelope, which is already self-explanatory.
fn oauth_error(v: &Value) -> Error {
	let code = v
		.pointer("/error/code")
		.or_else(|| v.pointer("/data/error/code"))
		.and_then(Value::as_i64);
	if code == Some(8208) {
		return cloud(format!(
			"Reolink account requires login verification (MFA) for this login — \
			 two-step verification, or a risk-based check on an unfamiliar \
			 device/IP (it fires from a new or datacenter IP even with two-step \
			 verification off). Run `bairelay cloud-authorise -c <config>` once on \
			 this host to clear it (it stores a reusable trust/refresh token so \
			 later connects skip MFA); re-run it if the stored token has expired. \
			 Or unbind the camera and use a local login. Server said: {}",
			short(v)
		));
	}
	cloud(format!("oauth failed: {}", short(v)))
}

async fn access_authorization(
	client: &reqwest::Client,
	bearer: &str,
	body: &Value,
	challenge: Option<&str>,
) -> Result<Value> {
	let mut req = common(client.post(format!("{}/v2/devices/access-authorization", base())))
		.bearer_auth(bearer)
		.json(body);
	if let Some(c) = challenge {
		req = req.header("X-Api-Challenge", c);
	}
	let resp = req
		.send()
		.await
		.map_err(|e| cloud(format!("access-authorization request: {e}")))?;
	json(resp).await
}

async fn json(resp: reqwest::Response) -> Result<Value> {
	let status = resp.status();
	// Reject an over-large declared body up front; then read with a hard cap so
	// a hostile endpoint cannot stream past the ceiling and OOM us.
	if resp
		.content_length()
		.is_some_and(|n| n as usize > MAX_RESP_BYTES)
	{
		return Err(cloud(format!("HTTP {status}: response too large")));
	}
	let mut resp = resp;
	let mut buf: Vec<u8> = Vec::new();
	while let Some(chunk) = resp
		.chunk()
		.await
		.map_err(|e| cloud(format!("reading response: {e}")))?
	{
		if buf.len() + chunk.len() > MAX_RESP_BYTES {
			return Err(cloud(format!("HTTP {status}: response exceeded cap")));
		}
		buf.extend_from_slice(&chunk);
	}
	serde_json::from_slice(&buf).map_err(|_| {
		cloud(format!(
			"HTTP {status}: non-JSON body ({} bytes)",
			buf.len()
		))
	})
}

/// A JSON value rendered for a diagnostic message, truncated to
/// [`SHORT_MAX_CHARS`] — char-safe (`Value::to_string` emits raw UTF-8, so a
/// byte slice could split a codepoint and panic on a hostile body).
fn short(v: &Value) -> String {
	let s = v.to_string();
	let truncated: String = s.chars().take(SHORT_MAX_CHARS).collect();
	if truncated.len() < s.len() {
		format!("{truncated}…")
	} else {
		s
	}
}

// ===========================================================================
// MFA bootstrap — clearing Reolink's login verification on an untrusted host.
//
// A host whose outbound IP Reolink doesn't recognise gets `8208 mfa_required`
// on the password grant. The app's flow (recovered from the RN bundle) is:
//   1. POST /v2/auth/mfa/codes  {clientId, scenario, method, data}  -> verify id
//      (email method mails a code; totp / backup_code expect an operator-held one)
//   2. POST /v2/auth/mfa/sessions {id, code}                        -> {id, code}
//   3. POST /v1.0/oauth2/token/  (session_mode) with x-verify-* headers
//      -> tokens + an `mfa_trust_token` good for ~30 days.
// The token is persisted ([`CloudAuth`]) and replayed by [`mint_bundle`] so the
// mint clears MFA headlessly until it expires, then `cloud-authorise` re-runs.
// ===========================================================================

/// Login-verification scenario for the account password login.
pub const MFA_LOGIN_SCENARIO: &str = "users.login_with_password";

/// Persisted credential for this host, written by the `cloud-authorise`
/// bootstrap and read at connect time. Lets a host Reolink would otherwise
/// challenge clear login verification without the interactive step until
/// `expiry_unix_s`. A verified login always returns a `refresh_token`; it
/// *sometimes* also returns an `mfa_trust_token` (Reolink won't issue a second
/// while one is already active for the account). Either clears MFA on the mint,
/// so both are stored and the mint prefers whichever is present.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CloudAuth {
	/// Reolink account the credential belongs to (so a stale file for a
	/// different account isn't used by mistake).
	pub account: String,
	/// The `mfa_trust_token` from a verified login, if Reolink issued one.
	/// Sent with the password grant (`session_mode`/`mfa_trusted`) to clear MFA.
	#[serde(default)]
	pub mfa_trust_token: Option<String>,
	/// The `refresh_token` from the verified login (90 days). Used for a refresh
	/// grant that skips the password grant — and thus MFA — entirely.
	#[serde(default)]
	pub refresh_token: Option<String>,
	/// Absolute expiry, unix seconds (of whichever credential is the backstop).
	pub expiry_unix_s: i64,
}

impl CloudAuth {
	/// Whether the credential is still valid (with a small margin) at
	/// `now_unix_s` and actually carries a usable token.
	pub fn is_valid(&self, now_unix_s: i64) -> bool {
		(self.mfa_trust_token.is_some() || self.refresh_token.is_some())
			&& self.expiry_unix_s - CACHE_MARGIN_SECS > now_unix_s
	}
}

/// A string field read from a JSON body, top-level or nested under `data`.
fn data_str(v: &Value, key: &str) -> Option<String> {
	v.get(key)
		.and_then(Value::as_str)
		.or_else(|| {
			v.get("data")
				.and_then(|d| d.get(key))
				.and_then(Value::as_str)
		})
		.map(str::to_owned)
}

/// Step 1: request an MFA verify `id` for `method` (`email` / `totp` /
/// `backup_code`). The `email` method mails a code; the others expect a code the
/// operator already holds (authenticator app / saved backup list).
pub async fn mfa_create_code(method: &str, account: &str) -> Result<String> {
	let client = http()?;
	let data = if method == "email" {
		serde_json::json!({ "emailAddress": account })
	} else {
		serde_json::json!({ "account": format!("email:{account}") })
	};
	let body = serde_json::json!({
		"clientId": CLIENT_ID,
		"scenario": MFA_LOGIN_SCENARIO,
		"method": method,
		"data": data,
	});
	let resp = common(client.post(format!("{}/v2/auth/mfa/codes", base())))
		.json(&body)
		.send()
		.await
		.map_err(|e| cloud(format!("mfa/codes request: {e}")))?;
	let v = json(resp).await?;
	data_str(&v, "id").ok_or_else(|| cloud(format!("mfa/codes returned no id: {}", short(&v))))
}

/// Step 2: exchange the verify `id` + the operator's `code` for a verified MFA
/// session `(id, code)` used as the `x-verify-*` headers on the login.
pub async fn mfa_create_session(id: &str, code: &str) -> Result<(String, String)> {
	let client = http()?;
	let body = serde_json::json!({ "id": id, "code": code });
	let resp = common(client.post(format!("{}/v2/auth/mfa/sessions", base())))
		.json(&body)
		.send()
		.await
		.map_err(|e| cloud(format!("mfa/sessions request: {e}")))?;
	let v = json(resp).await?;
	let sid = data_str(&v, "id")
		.ok_or_else(|| cloud(format!("mfa/sessions returned no id: {}", short(&v))))?;
	let scode = data_str(&v, "code")
		.ok_or_else(|| cloud(format!("mfa/sessions returned no code: {}", short(&v))))?;
	Ok((sid, scode))
}

/// Step 3: the verified password login (`session_mode`, `mfa_trusted`, the
/// `x-verify-*` headers). Returns the [`CloudAuth`] trust token to persist.
pub async fn mfa_verified_login(
	account: &str,
	password: &str,
	verify_id: &str,
	verify_code: &str,
) -> Result<CloudAuth> {
	let client = http()?;
	let resp = common(client.post(format!("{}/v1.0/oauth2/token/", base())))
		.header("x-verify-scenario", MFA_LOGIN_SCENARIO)
		.header("x-verify-id", verify_id)
		.header("x-verify-code", verify_code)
		.form(&[
			("client_id", CLIENT_ID),
			("grant_type", "password"),
			("username", account),
			("password", password),
			("session_mode", "true"),
			("mfa_trusted", "true"),
		])
		.send()
		.await
		.map_err(|e| cloud(format!("verified-login request: {e}")))?;
	let v = json(resp).await?;
	let trust = v
		.get("mfa_trust_token")
		.and_then(Value::as_str)
		.map(str::to_owned);
	let refresh = v
		.get("refresh_token")
		.and_then(Value::as_str)
		.map(str::to_owned);
	if trust.is_none() && refresh.is_none() {
		return Err(cloud(format!(
			"verified login returned neither mfa_trust_token nor refresh_token: {}",
			short(&v)
		)));
	}
	let now = time::OffsetDateTime::now_utc().unix_timestamp();
	// Expiry of the backstop credential. `mfa_trust_token_expires_in` is an
	// absolute unix-MILLIseconds timestamp (e.g. 1784395562403); when only a
	// refresh token came back, `refresh_token_expires_in` is a DURATION in
	// seconds (e.g. 7776000 = 90 days). Fall back to ~29 days out.
	let expiry = match v.get("mfa_trust_token_expires_in").and_then(Value::as_i64) {
		Some(ms) if ms > 1_000_000_000_000 => ms / 1000,
		_ => match v.get("refresh_token_expires_in").and_then(Value::as_i64) {
			Some(secs) if secs > 0 => now + secs,
			_ => now + 29 * 86_400,
		},
	};
	Ok(CloudAuth {
		account: account.to_owned(),
		mfa_trust_token: trust,
		refresh_token: refresh,
		expiry_unix_s: expiry,
	})
}

/// Full interactive MFA bootstrap: request a code, obtain it from `get_code`
/// (the caller prints the prompt + reads it — `get_code` is handed the
/// `method` so it can word the prompt), then complete the verified login and
/// return the [`CloudAuth`] trust token to persist. Network orchestration lives
/// here (mockable); the binary's `cloud-authorise` wraps it with a stdin reader.
pub async fn mfa_bootstrap<F>(
	account: &str,
	password: &str,
	method: &str,
	get_code: F,
) -> Result<CloudAuth>
where
	F: FnOnce(&str) -> String,
{
	let id = mfa_create_code(method, account).await?;
	let code = get_code(method);
	let code = code.trim();
	if code.is_empty() {
		return Err(cloud("no verification code provided"));
	}
	let (sid, scode) = mfa_create_session(&id, code).await?;
	mfa_verified_login(account, password, &sid, &scode).await
}

// ---- proof of work (hashcash; only when error.code == 8214) ----

struct PowChallenge {
	id: String,
	prefix: String,
	charset: Vec<char>,
	difficulties: Vec<u32>,
}

fn pow_challenge(v: &Value) -> Option<PowChallenge> {
	let err = v.get("error")?;
	if err.get("code")?.as_u64()? != 8214 {
		return None;
	}
	let ch = err.get("metadata")?.get("challenge")?;
	let data = ch.get("data")?;
	Some(PowChallenge {
		id: ch.get("id")?.as_str()?.to_owned(),
		prefix: data.get("r")?.as_str()?.to_owned(),
		charset: data.get("c")?.as_str()?.chars().collect(),
		difficulties: data
			.get("p")?
			.as_array()?
			.iter()
			.filter_map(|p| p.get("d").and_then(Value::as_u64).map(|d| d as u32))
			.collect(),
	})
}

fn leading_zero_bits(bytes: &[u8]) -> u32 {
	let mut n = 0;
	for &b in bytes {
		if b == 0 {
			n += 8;
			continue;
		}
		n += b.leading_zeros();
		break;
	}
	n
}

fn solve_pow(ch: &PowChallenge) -> Result<String> {
	let cs = &ch.charset;
	if cs.is_empty() {
		return Err(cloud("PoW challenge had empty charset"));
	}
	let target = *ch.difficulties.first().unwrap_or(&0);
	let mut i: u64 = 0;
	loop {
		// Deterministic counter -> 32-char nonce over the charset. `checked_shr`
		// yields 0 past the 64-bit width (a plain `>>` would panic).
		let nonce: String = (0..32u32)
			.map(|k| cs[(i.checked_shr(5 * k).unwrap_or(0) as usize) % cs.len()])
			.collect();
		let mut hasher = Sha256::new();
		hasher.update(ch.prefix.as_bytes());
		hasher.update(nonce.as_bytes());
		if leading_zero_bits(&hasher.finalize()) >= target {
			return Ok(format!("type=pow/1;id={};token={nonce}", ch.id));
		}
		i += 1;
		if i > 100_000_000 {
			return Err(cloud(format!("PoW giveup at difficulty {target}")));
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn leading_zero_bits_counts_correctly() {
		assert_eq!(leading_zero_bits(&[0x00, 0x00, 0xff]), 16);
		assert_eq!(leading_zero_bits(&[0x0f, 0xff]), 4);
		assert_eq!(leading_zero_bits(&[0xff]), 0);
		assert_eq!(leading_zero_bits(&[0x00]), 8);
	}

	#[test]
	fn solve_pow_meets_difficulty() {
		let ch = PowChallenge {
			id: "abc".into(),
			prefix: "challenge-".into(),
			charset: "0123456789abcdef".chars().collect(),
			difficulties: vec![8],
		};
		let header = solve_pow(&ch).unwrap();
		// header shape + the solution actually clears 8 leading zero bits.
		let token = header.rsplit("token=").next().unwrap();
		let mut h = Sha256::new();
		h.update(ch.prefix.as_bytes());
		h.update(token.as_bytes());
		assert!(leading_zero_bits(&h.finalize()) >= 8);
		assert!(header.starts_with("type=pow/1;id=abc;token="));
	}

	#[test]
	fn pow_challenge_ignored_when_not_8214() {
		let v = serde_json::json!({"error": {"code": 1234}});
		assert!(pow_challenge(&v).is_none());
	}

	#[test]
	fn pow_challenge_parsed_when_8214() {
		let v = serde_json::json!({
			"error": {
				"code": 8214,
				"metadata": {"challenge": {
					"id": "ID",
					"data": {"r": "RR", "c": "0123", "p": [{"d": 4}]}
				}}
			}
		});
		let ch = pow_challenge(&v).expect("8214 -> challenge");
		assert_eq!(ch.id, "ID");
		assert_eq!(ch.prefix, "RR");
		assert_eq!(ch.charset, vec!['0', '1', '2', '3']);
		assert_eq!(ch.difficulties, vec![4]);
	}

	#[test]
	fn oauth_error_flags_two_factor_8208() {
		let v = serde_json::json!({
			"error": {
				"code": 8208,
				"message": "The extra identification is required.",
				"metadata": {"allowMethods": {"backup_email": {"enabled": true}}}
			}
		});
		let msg = oauth_error(&v).to_string();
		// Actionable hint: names login verification / MFA, not a wrong password.
		assert!(
			msg.contains("login verification") || msg.contains("MFA"),
			"got: {msg}"
		);
		assert!(msg.contains("two-step verification"), "got: {msg}");
		// AND the full raw envelope is included so the operator can see which
		// verification methods the account offers.
		assert!(msg.contains("allowMethods"), "got: {msg}");
	}

	#[test]
	fn oauth_error_falls_back_to_raw_for_other_codes() {
		let v = serde_json::json!({
			"error": {"code": 8200, "symbol": "incorrect_password"}
		});
		let msg = oauth_error(&v).to_string();
		assert!(msg.contains("oauth failed"), "got: {msg}");
		assert!(msg.contains("incorrect_password"), "got: {msg}");
	}

	#[test]
	fn parse_bundle_top_level_and_data_wrapper() {
		let inner = serde_json::json!({
			"uid": "U",
			"token": {"p": "PP", "s": "SS", "k": "KK"},
			"certChain": "a\r\nb\r\n"
		});
		let b = parse_bundle(&inner).unwrap();
		assert_eq!(
			(b.token_p.as_str(), b.token_s.as_str(), b.token_k.as_str()),
			("PP", "SS", "KK")
		);
		assert_eq!(b.cert_chain, "a\nb\n"); // \r\n normalised
		let wrapped = serde_json::json!({ "data": inner });
		assert_eq!(parse_bundle(&wrapped).unwrap().token_p, "PP");
	}

	#[test]
	fn parse_bundle_rejects_error_envelope_and_missing_fields() {
		let err = serde_json::json!({"error": {"code": 8198, "symbol": "token_expired"}});
		assert!(parse_bundle(&err).is_err());
		assert!(parse_bundle(&serde_json::json!({"uid": "U", "certChain": "x"})).is_err());
		let bad_token = serde_json::json!({"token": {"p": "P", "s": "S"}, "certChain": "x"});
		assert!(parse_bundle(&bad_token).is_err());
		let no_cert = serde_json::json!({"token": {"p": "P", "s": "S", "k": "K"}});
		assert!(parse_bundle(&no_cert).is_err());
	}

	#[test]
	fn short_is_char_safe_at_boundary() {
		// Multibyte chars past the cap: the truncation point splits a codepoint
		// byte-wise — `short` must cut on a char boundary, not panic.
		let v = Value::String("é".repeat(SHORT_MAX_CHARS + 100));
		let out = short(&v);
		assert!(out.ends_with('…'));
		assert!(out.chars().count() <= SHORT_MAX_CHARS + 1);
	}

	#[test]
	fn short_shows_full_error_envelope() {
		// A realistic 8208 MFA envelope must survive intact (no truncation) so
		// the operator sees which verification methods the account requires.
		let v = serde_json::json!({"error":{"code":8208,
			"message":"The extra identification is required.",
			"metadata":{"allowMethods":{
				"backup_code":{"codeLength":8,"enabled":false},
				"backup_email":{"codeLength":8,"enabled":false,"hint":"a***@example.com"},
				"email":{"codeLength":6,"enabled":true,"hint":"a***@example.com"}}}}});
		let out = short(&v);
		assert!(
			!out.ends_with('…'),
			"envelope should not be truncated: {out}"
		);
		assert!(out.contains("allowMethods") && out.contains("\"email\""));
	}

	#[test]
	fn token_exp_parses_or_none() {
		assert_eq!(token_exp(r#"{"exp": 1781664236}"#), Some(1781664236));
		assert_eq!(token_exp(r#"{"sub": "x"}"#), None);
		assert_eq!(token_exp("not json"), None);
	}

	#[tokio::test]
	async fn mint_bundle_returns_cached_without_network() {
		let uid = "9527000CACHETEST0";
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let bundle = Sigv3Bundle {
			token_p: "P".into(),
			token_s: "S".into(),
			token_k: "K".into(),
			cert_chain: "C".into(),
		};
		cache()
			.lock()
			.unwrap()
			.insert(uid.to_owned(), (bundle, now + 3600));
		// Bogus creds would fail the network; the fresh cache entry short-circuits.
		let got = mint_bundle("nobody@example.com", "wrong", uid, None, None)
			.await
			.unwrap();
		assert_eq!(got.token_p, "P");
		cache().lock().unwrap().remove(uid);
	}

	#[test]
	fn expired_cache_entry_is_not_served() {
		// Unit-test the freshness predicate directly (the network path is
		// covered by the live manual-verify harness, not here).
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let fresh_exp = now + 3600;
		let stale_exp = now - 10;
		assert!(fresh_exp - CACHE_MARGIN_SECS > now);
		assert!(stale_exp - CACHE_MARGIN_SECS <= now);
	}

	#[tokio::test]
	async fn mint_bundle_full_flow_against_mock_server() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		use tokio::net::TcpListener;

		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		// First setter wins; this is the only test that drives the network.
		let _ = TEST_BASE.set(format!("http://127.0.0.1:{port}"));

		let token_p =
			r#"{"ver":3,"sid":"S","sub":"9527000MOCKTEST0","exp":4102444800,"role":"admin"}"#;
		let bundle = format!(
			"{{\"uid\":\"9527000MOCKTEST0\",\"token\":{{\"p\":{token_p:?},\"s\":\"SIG\",\"k\":\"KEY\"}},\
			 \"certChain\":\"-----BEGIN-----\\r\\nAAAA\\r\\n-----END-----\\r\\n\"}}"
		);

		let server = tokio::spawn(async move {
			loop {
				let Ok((mut s, _)) = listener.accept().await else {
					break;
				};
				// Read the full request (headers + Content-Length body).
				let mut buf = vec![0u8; 8192];
				let mut total = 0;
				loop {
					let n = s.read(&mut buf[total..]).await.unwrap_or(0);
					total += n;
					let txt = String::from_utf8_lossy(&buf[..total]);
					if let Some(h) = txt.find("\r\n\r\n") {
						let cl = txt
							.lines()
							.find_map(|l| {
								l.to_ascii_lowercase()
									.strip_prefix("content-length:")
									.map(|v| v.trim().parse::<usize>().unwrap_or(0))
							})
							.unwrap_or(0);
						if total >= h + 4 + cl {
							break;
						}
					}
					if n == 0 || total >= buf.len() {
						break;
					}
				}
				let req = String::from_utf8_lossy(&buf[..total]).to_string();
				// Sentinels in the request let the one mock drive the error /
				// edge arms too (no second listener — TEST_BASE is global).
				let body = if req.contains("mfa/codes") {
					if req.contains("noid@") {
						"{}".to_string() // missing id -> mfa_create_code error arm
					} else {
						r#"{"id":"CODEID"}"#.to_string()
					}
				} else if req.contains("mfa/sessions") {
					if req.contains("BADSESS") {
						"{}".to_string() // missing id/code -> mfa_create_session error arm
					} else {
						// `data`-wrapped to exercise data_str's nested-object branch.
						r#"{"data":{"id":"SID","code":"SCODE"}}"#.to_string()
					}
				} else if req.contains("oauth2/token") {
					if req.contains("x-verify-code") || req.contains("session_mode=true") {
						if req.contains("notoken") {
							r#"{"access_token":"A"}"#.to_string() // no mfa_trust_token -> error arm
						} else if req.contains("shortexp") {
							// token present but no absolute-ms expiry -> fallback (~29d).
							r#"{"access_token":"A","mfa_trust_token":"TT"}"#.to_string()
						} else if req.contains("refreshonly") {
							// no trust token, only a refresh token (90-day duration).
							r#"{"access_token":"A","refresh_token":"R","refresh_token_expires_in":7776000}"#.to_string()
						} else {
							// Verified login: hand back the 30-day trust token
							// (expiry as absolute unix-MILLIseconds, like the real API).
							r#"{"access_token":"A","refresh_token":"R","mfa_trust_token":"TT","mfa_trust_token_expires_in":4102444800000}"#.to_string()
						}
					} else if req.contains("grant_type=password") {
						r#"{"refresh_token":"R","access_token":"A1"}"#.to_string()
					} else {
						r#"{"access_token":"BEARER"}"#.to_string()
					}
				} else {
					bundle.clone()
				};
				let resp = format!(
					"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
					body.len()
				);
				let _ = s.write_all(resp.as_bytes()).await;
				let _ = s.shutdown().await;
			}
		});

		let got = mint_bundle("me@example.com", "pw", "9527000MOCKTEST0", None, None)
			.await
			.expect("mint against mock");

		// A mint WITH a stored trust token exercises the session_mode /
		// mfa_trusted password-grant branch (different UID dodges the cache).
		let got_trusted = mint_bundle(
			"me@example.com",
			"pw",
			"9527000MOCKTEST2",
			Some("TRUST"),
			None,
		)
		.await
		.expect("mint with trust token");
		assert_eq!(got_trusted.token_s, "SIG");
		cache().lock().unwrap().remove("9527000MOCKTEST2");

		// Same mock server also drives the full MFA bootstrap (codes ->
		// sessions -> verified login), since TEST_BASE is a process-global
		// OnceLock and only one test may own the network. The closure stands
		// in for the operator pasting the emailed code.
		let auth = mfa_bootstrap("me@example.com", "pw", "email", |m| {
			assert_eq!(m, "email");
			"12345678".to_string()
		})
		.await
		.expect("mfa bootstrap");
		assert_eq!(auth.account, "me@example.com");
		assert_eq!(auth.mfa_trust_token.as_deref(), Some("TT"));
		assert_eq!(auth.refresh_token.as_deref(), Some("R"));
		// 4102444800000 ms / 1000 = 4102444800 (year 2100) -> far-future, valid.
		assert_eq!(auth.expiry_unix_s, 4_102_444_800);
		assert!(auth.is_valid(time::OffsetDateTime::now_utc().unix_timestamp()));

		// `totp` exercises the non-email `data:{account:…}` request shape.
		let totp = mfa_bootstrap("me@example.com", "pw", "totp", |m| {
			assert_eq!(m, "totp");
			"999111".to_string()
		})
		.await
		.expect("totp bootstrap");
		assert_eq!(totp.mfa_trust_token.as_deref(), Some("TT"));

		// An empty code is rejected before the session call.
		let empty = mfa_bootstrap("me@example.com", "pw", "email", |_| "  ".to_string()).await;
		assert!(empty.is_err(), "empty code must error");

		// Refresh-only verified login (no mfa_trust_token issued) — the common
		// case once a trust token is already active. Expiry comes from the
		// refresh token's DURATION (90 d), and a mint can reuse it (refresh
		// grant, no password grant / MFA).
		let refresh_only = mfa_verified_login("me@example.com", "pw", "vid", "refreshonly")
			.await
			.expect("refresh-only login");
		assert_eq!(refresh_only.mfa_trust_token, None);
		assert_eq!(refresh_only.refresh_token.as_deref(), Some("R"));
		let now_s = time::OffsetDateTime::now_utc().unix_timestamp();
		assert!(refresh_only.expiry_unix_s > now_s + 80 * 86_400);
		let got_refresh = mint_bundle("me@example.com", "pw", "9527000MOCKTEST3", None, Some("R"))
			.await
			.expect("mint via stored refresh token");
		assert_eq!(got_refresh.token_s, "SIG");
		cache().lock().unwrap().remove("9527000MOCKTEST3");

		// Error / edge arms (sentinel-driven from the same mock):
		assert!(
			mfa_create_code("email", "noid@example.com").await.is_err(),
			"missing id -> error"
		);
		assert!(
			mfa_create_session("BADSESS", "x").await.is_err(),
			"missing session id/code -> error"
		);
		assert!(
			mfa_verified_login("me@example.com", "pw", "vid", "notoken")
				.await
				.is_err(),
			"missing mfa_trust_token -> error"
		);
		// Token present but no absolute-ms expiry -> ~29-day fallback.
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let fallback = mfa_verified_login("me@example.com", "pw", "vid", "shortexp")
			.await
			.expect("token present");
		assert!(fallback.expiry_unix_s > now + 28 * 86_400);

		server.abort();
		assert_eq!(got.token_s, "SIG");
		assert_eq!(got.token_k, "KEY");
		assert!(got.token_p.contains("\"role\":\"admin\""));
		// \r\n in the cert is normalised to \n.
		assert_eq!(got.cert_chain, "-----BEGIN-----\nAAAA\n-----END-----\n");
		cache().lock().unwrap().remove("9527000MOCKTEST0");
	}
}
