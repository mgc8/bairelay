use super::credentials::Credentials;
use super::{md5_string, BcCamera, Error, Result, Truncate};
use crate::bc::{model::*, xml::*};
use std::sync::atomic::Ordering;

#[cfg(test)]
use crate::bc_protocol::connection::mock::{reply_200_xml, MockConnection};

/// Legacy-login `response_code` byte the client sends to announce the
/// highest encryption it's willing to negotiate. The camera replies
/// with the actual encryption that will be used — never higher than the
/// requested ceiling.
const LEGACY_LOGIN_ENC_NONE: u16 = 0xdc00;
const LEGACY_LOGIN_ENC_BCENCRYPT: u16 = 0xdc01;
const LEGACY_LOGIN_ENC_AES: u16 = 0xdc12;

/// Baichuan message-class bytes for the login exchange. Legacy login
/// uses class `0x6514` (LegacyMsg body), the modern follow-up uses
/// `0x6414` (ModernMsg body with the BcXml LoginUser/LoginNet payload).
/// `0x6414` is the generic modern-msg class re-used across many other
/// commands (abilityinfo, version, etc.); the constant name here pins
/// its meaning at this call site.
const MSG_CLASS_LEGACY_LOGIN: u16 = 0x6514;
const MSG_CLASS_MODERN_LOGIN: u16 = 0x6414;
/// The sigV3 direct login (cloud-bound / account cameras) is sent with Bc
/// class `0x0000`, NOT `0x6414`. The camera routes `0x6414` to the
/// legacy/modern-encryption login handler (which rejects the cloud token with
/// 417) and `0x0000` to the sigV3 token handler. Verified on the wire against
/// the official app + an end-to-end pure-client login. Both classes carry the
/// payload-offset header (`has_payload_offset`).
const MSG_CLASS_SIGV3_LOGIN: u16 = 0x0000;

/// The requested encryption level to request
/// to the camera
///
/// The camera may use a lower one depending on support
///
/// Note the reolink camera only encrypt the control messages
/// the camera feed is always accessible
#[derive(Debug, Clone, Copy)]
pub enum MaxEncryption {
	/// No encryption
	None,
	/// BCEncrypt is a simple XOR algortirhm with a fixed key
	/// used in many older models
	BcEncrypt,
	/// AES is used in newer model
	Aes,
}

impl BcCamera {
	/// Login to the camera.
	///
	/// This should be called before most other commands
	pub async fn login(&self) -> Result<DeviceInfo> {
		self.login_with_maxenc(MaxEncryption::Aes).await
	}
	/// Login to the camera.
	///
	/// This should be called before most other commands
	pub async fn login_with_maxenc(&self, max_encryption: MaxEncryption) -> Result<DeviceInfo> {
		// sigV3 direct login (cloud-bound camera): the P2P handshake (lver=3)
		// already delivered the login nonce (`nc`) + ECDHE (`pl`). The sigV3
		// login is sent DIRECTLY, keyed by that nonce — NO Bc LoginUpgrade
		// (the camera does not answer a LoginUpgrade after the lver=3 sigV3
		// handshake). See `login_sigv3`.
		let handshake_sigv3 = match &self.sigv3_handshake {
			Some((nc, pl)) => {
				super::login_sigv3::parse_pl(pl).map(|(cam_pub, iters)| (*nc, cam_pub, iters))
			}
			None => None,
		};
		if let Some((nc, cam_pub, iters)) = handshake_sigv3 {
			let di = self.run_sigv3_direct(nc, &cam_pub, iters).await?;
			return Ok(self.finish_cloud_login(di).await);
		}

		let device_info;
		// This { is here due to the connection and set_credentials both requiring a mutable borrow
		{
			let credentials = self.get_credentials();
			let connection = self.get_connection();
			let msg_num = self.new_message_num();
			let mut sub_login = connection.subscribe(MSG_ID_LOGIN, msg_num).await?;

			// Login flow is: Send legacy login message, expect back a modern message with Encryption
			// details.  Then, re-send the login as a modern login message.  Expect back a device info
			// congratulating us on logging in.

			// In the legacy scheme, username/password are MD5'd if they are encrypted (which they need
			// to be to "upgrade" to the modern login flow), then the hex of the MD5 is sent.
			// Note: I suspect there may be a buffer overflow opportunity in the firmware since in the
			// Baichuan library, these strings are capped at 32 bytes with a null terminator.  This
			// could also be a mistake in the library, the effect being it only compares 31 chars, not 32.
			// let md5_username = md5_string(&credentials.username, ZeroLast);
			// let md5_password = credentials
			//     .password
			//     .as_ref()
			//     .map(|p| md5_string(p, ZeroLast))
			//     .unwrap_or_else(|| EMPTY_LEGACY_PASSWORD.to_owned());

			// Refuse to log in with an absent / empty password under
			// AES or BcEncrypt: the wire path silently wraps `None` to
			// `""`, MD5s `format!("{}-{}", "", nonce)`, and the camera
			// answers with a generic 401 — the operator has no way to
			// tell that the misconfiguration is at THEIR end vs. a real
			// auth failure. Anonymous-login (`None` encryption) is the
			// only path where empty password is meaningful.
			// A cloud ("account") camera authenticates with the minted cloud
			// token, never a local password — so the empty-password refusal
			// must NOT fire for it (it would reject before the camera can even
			// advertise sigV3 over the Encryption negotiation). The cloud login
			// is driven below once the negotiation reveals the sigV3 offer.
			// Gate on `is_cloud` (discovery = "cloud"), NOT cloud_account —
			// the account is propagated to every camera from the config root.
			if !self.is_cloud {
				match max_encryption {
					MaxEncryption::Aes | MaxEncryption::BcEncrypt => {
						let pw_present = credentials
							.password
							.as_deref()
							.map(|p| !p.is_empty())
							.unwrap_or(false);
						if !pw_present {
							log::warn!(
								"login refused: {:?} requires a non-empty password",
								max_encryption
							);
							return Err(Error::AuthFailed);
						}
					}
					MaxEncryption::None => {}
				}
			}

			let enc_byte = match max_encryption {
				MaxEncryption::None => LEGACY_LOGIN_ENC_NONE,
				MaxEncryption::BcEncrypt => LEGACY_LOGIN_ENC_BCENCRYPT,
				MaxEncryption::Aes => LEGACY_LOGIN_ENC_AES,
			};
			let legacy_login = Bc {
				meta: BcMeta {
					msg_id: MSG_ID_LOGIN,
					channel_id: self.channel_id,
					msg_num,
					stream_type: 0,
					response_code: enc_byte,
					class: MSG_CLASS_LEGACY_LOGIN,
				},
				body: BcBody::LegacyMsg(LegacyMsg::LoginUpgrade),
			};

			sub_login.send(legacy_login).await?;

			let legacy_reply = sub_login.recv().await?;

			let nonce: String;
			let sigv3: Option<(String, u32)>;
			// Auth methods the camera advertises (`password` / `sigV1` /
			// `sigV3` / `authLogin` / `getAccesskey`). Empty on older
			// firmware. Drives the camera-local authLogin branch below.
			let auth_types: Vec<String>;
			match &legacy_reply.body {
				BcBody::ModernMsg(ModernMsg {
					payload:
						Some(BcPayloads::BcXml(BcXml {
							encryption: Some(encryption),
							..
						})),
					..
				}) => {
					nonce = encryption.nonce.clone();
					auth_types = encryption
						.auth_type_list
						.as_ref()
						.map(|l| l.auth_type.clone())
						.unwrap_or_default();
					// Newer firmware advertises an X25519 ECDHE block +
					// sigVer=v3 and rejects the legacy plain-MD5 login (406).
					// Capture the camera's offer so we send a signed login.
					sigv3 = match (&encryption.ecdhe, encryption.sig_ver.as_deref()) {
						(Some(ecdhe), Some("v3")) => {
							log::info!(
								"login: camera requires sigV3/ECDHE (algo={}, iterations={})",
								ecdhe.public_key_algo,
								ecdhe.iterations
							);
							Some((ecdhe.public_key.clone(), ecdhe.iterations))
						}
						_ => None,
					};
				}
				_ => {
					return Err(Error::UnintelligibleReply {
						reply: std::sync::Arc::new(legacy_reply),
						why: "Expected an Encryption message back",
					})
				}
			}

			log::info!(
				"login: camera offered auth types {:?} (sigV3={})",
				auth_types,
				sigv3.is_some()
			);

			// Cloud ("account") camera reached over a path that did NOT capture
			// the lver=3 discovery handshake — i.e. remote/map/relay discovery,
			// or any connect where local broadcast didn't win (a server off the
			// camera's broadcast domain but routed/VPN'd to it). The camera
			// advertises sigV3 over this Encryption negotiation; mint the cloud
			// token and send the signed login keyed to THIS nonce + ECDHE. This
			// is what makes a cloud camera log in regardless of how it was
			// found, instead of falling through to the password login it can't
			// satisfy. Gate on `is_cloud` (discovery =
			// "cloud"), NOT cloud_account — the account is propagated to every
			// camera from the config root, so keying on it would hijack the
			// login of ordinary password cameras in the same config.
			if self.is_cloud {
				// Cloud ("account") camera: the full cloud login machinery, and
				// the ONLY place it runs. A cloud camera that advertises sigV3
				// uses the signed cloud-token login; one that advertises
				// getAccesskey without mandating sigV3 uses the camera-local
				// authLogin handshake. The decision keys on discovery="cloud"
				// (`is_cloud`), NOT on what the camera lists in its auth-type
				// table — a normal local camera that merely *advertises* sigV3 /
				// getAccesskey must not be dragged into a handshake it does not
				// need (it logs in with the plain password login below).
				if let Some((cam_pub, iters)) = sigv3.clone() {
					drop(sub_login);
					let bundle = self.mint_cloud_bundle().await?;
					let di = self
						.run_sigv3_login(&nonce, &cam_pub, iters, &bundle)
						.await?;
					return Ok(self.finish_cloud_login(di).await);
				}
				if auth_types.iter().any(|t| t == "getAccesskey") {
					let di = self
						.run_authlogin(&mut sub_login, credentials, &nonce, msg_num)
						.await?;
					return Ok(self.finish_cloud_login(di).await);
				}
				return Err(Error::Cloud(format!(
					"cloud camera offered neither sigV3 nor authLogin over the login \
					 negotiation (auth types {auth_types:?}); cannot complete a cloud login"
				)));
			}

			// In the modern login flow, the username/password are concat'd with the server's nonce
			// string, then MD5'd, then the hex of this MD5 is sent as the password.  This nonce
			// prevents replay attacks if the server were to require modern flow, but not rainbow table
			// attacks (since  the plain user/password MD5s have already been sent).  The upshot is that
			// you should use a very strong random password that is not found in a rainbow table and
			// not feasibly crackable with John the Ripper.

			// Non-cloud camera: the plain modern (MD5) password login, and the
			// ONLY login a non-cloud camera performs. The special new-firmware
			// paths (sigV3 cloud token, camera-local authLogin) are reserved for
			// discovery="cloud" and handled above; a normal local camera that
			// merely advertises them in its auth-type list still logs in here.
			{
				let modern_password = credentials.password.clone().unwrap_or_default();
				let concat_username = format!("{}{}", credentials.username, nonce);
				let concat_password = format!("{}{}", modern_password, nonce);
				let md5_username = md5_string(&concat_username, Truncate);
				let md5_password = md5_string(&concat_password, Truncate);

				// The cloud-token sigV3 login is handled exclusively by the
				// direct handshake path (`run_sigv3_direct`, early-returned
				// above). Here — the legacy/modern (LoginUpgrade) flow — only
				// the plain MD5 login is sent.
				let login_user = LoginUser {
					version: xml_ver(),
					user_name: md5_username,
					password: md5_password,
					user_ver: 1,
					..Default::default()
				};

				let modern_login = Bc::new_from_xml(
					BcMeta {
						msg_id: MSG_ID_LOGIN,
						channel_id: self.channel_id,
						msg_num,
						stream_type: 0,
						response_code: 0,
						class: MSG_CLASS_MODERN_LOGIN,
					},
					BcXml {
						login_user: Some(login_user),
						login_net: Some(LoginNet::default()),
						..Default::default()
					},
				);

				// A non-cloud camera that nonetheless advertises sigV3 wants the
				// cloud-token login, which is only available via discovery="cloud".
				// Send the plain login anyway (some firmware still accepts it) but
				// hint the operator so a rejection is self-explanatory.
				if sigv3.is_some() {
					log::warn!(
						"login: camera advertises sigV3 but is not configured \
						 discovery=\"cloud\"; sending the plain password login"
					);
				}
				sub_login.send(modern_login).await?;
				let modern_reply = sub_login.recv().await?;
				if modern_reply.meta.response_code != 200 {
					log::warn!(
						"login rejected: camera replied response_code={} to the modern login",
						modern_reply.meta.response_code
					);
					if sigv3.is_some() {
						log::warn!(
							"login: this camera requires sigV3 — set discovery=\"cloud\" with \
							 cloud_account/cloud_password (see docs/cloud-account.md)"
						);
					}
					log::warn!("login reject reply body: {:?}", modern_reply.body);
					return Err(Error::CameraLoginFail);
				}

				match modern_reply.body {
					BcBody::ModernMsg(ModernMsg {
						payload:
							Some(BcPayloads::BcXml(BcXml {
								device_info: Some(info),
								..
							})),
						..
					}) => {
						// Login succeeded!
						self.logged_in.store(true, Ordering::Relaxed);
						device_info = info;
					}
					BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload: None,
					}) => {
						log::warn!(
							"login rejected: camera replied 200 with an empty body (no DeviceInfo)"
						);
						return Err(Error::AuthFailed);
					}
					other => {
						return Err(Error::UnintelligibleReply {
							reply: std::sync::Arc::new(Bc {
								meta: modern_reply.meta,
								body: other,
							}),
							why: "Expected a DeviceInfo message back from login",
						})
					}
				}
			}
		}

		// Populate the list of abilities this user has with the camera
		log::debug!("Populating abilities");
		self.populate_abilities().await?;
		Ok(device_info)
	}

	/// sigV3 direct login for cloud-bound cameras. The P2P handshake (lver=3)
	/// already provided the login nonce `nc` and the ECDHE (`cam_pub` +
	/// `iters`); send the signed `LoginUser` directly — no Bc LoginUpgrade.
	/// The codec pins BCEncrypt for this signed login + the DeviceInfo reply,
	/// then switches the session to AES once the camera accepts it. The cloud
	/// token bundle is minted fresh here from the camera's account credentials.
	async fn run_sigv3_direct(&self, nc: i64, cam_pub: &str, iters: u32) -> Result<DeviceInfo> {
		let bundle = self.mint_cloud_bundle().await?;
		self.run_sigv3_login(&nc.to_string(), cam_pub, iters, &bundle)
			.await
	}

	/// Mint a fresh cloud token bundle for this connect from the camera's
	/// account credentials (cached per-UID until shortly before it expires —
	/// see `cloud::mint_bundle`). Shared by the lver=3-handshake path
	/// (`run_sigv3_direct`) and the Encryption-negotiation path.
	async fn mint_cloud_bundle(&self) -> Result<crate::cloud::Sigv3Bundle> {
		let account = self
			.cloud_account
			.as_deref()
			.ok_or_else(|| Error::Cloud("cloud_account not set for a 'cloud' camera".into()))?;
		let password = self
			.cloud_password
			.as_deref()
			.ok_or_else(|| Error::Cloud("cloud_password not set for a 'cloud' camera".into()))?;
		let uid = self
			.cloud_uid
			.as_deref()
			.ok_or_else(|| Error::Cloud("camera uid required to mint the cloud bundle".into()))?;
		crate::cloud::mint_bundle(
			account,
			password,
			uid,
			self.cloud_mfa_trust_token.as_deref(),
			self.cloud_refresh_token.as_deref(),
		)
		.await
	}

	/// Finish a successful cloud sigV3 login. Account cameras answer the
	/// AbilityInfo query (msg 151) with 421 right after login; AbilityInfo is
	/// advisory (a missing entry never means a feature is absent), so a failure
	/// here must not sink an otherwise-successful login — log and carry on.
	async fn finish_cloud_login(&self, device_info: DeviceInfo) -> DeviceInfo {
		if let Err(e) = self.populate_abilities().await {
			log::warn!("sigV3: AbilityInfo unavailable ({e}); continuing without it");
		}
		device_info
	}

	/// Build + send the sigV3 `LoginUser` from an already-minted `bundle` and
	/// parse the camera's `DeviceInfo` reply. Split out of `run_sigv3_direct`
	/// so the login itself is unit-testable without the cloud network mint.
	async fn run_sigv3_login(
		&self,
		nonce: &str,
		cam_pub: &str,
		iters: u32,
		bundle: &crate::cloud::Sigv3Bundle,
	) -> Result<DeviceInfo> {
		let credentials = self.get_credentials();
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_LOGIN, msg_num).await?;

		// Account-camera sigV3: the `password` field is md5(EMPTY + nonce) —
		// the cloud token (cipherContent/tokenKey/certChain) authenticates,
		// NOT the local device password. Verified against the app: its
		// <password> == md5("" + nonce). userName stays md5(username + nonce).
		// `nonce` is whichever the camera issued for this login — the lver=3
		// handshake `nc` (direct path) or the `<Encryption>` nonce (negotiated
		// path); both are equivalent inputs to the proof.
		let md5_username = md5_string(&format!("{}{}", credentials.username, nonce), Truncate);
		let md5_password = md5_string(nonce, Truncate);
		let unix_time = time::OffsetDateTime::now_utc().unix_timestamp();

		let extras = super::login_sigv3::build_sigv3_extras(
			nonce,
			cam_pub,
			iters,
			unix_time,
			&bundle.token_p,
			&bundle.token_s,
		)?;
		// The codec arms the post-login AES switch from this (skipped) field.
		let session_aes = Some((extras.session_key, extras.session_iv));
		let login_user = LoginUser {
			version: xml_ver(),
			user_name: md5_username,
			password: md5_password,
			user_ver: 1,
			client_type: Some("app".to_string()),
			public_key: Some(extras.public_key),
			cipher_content: Some(extras.cipher_content),
			token_key: Some(bundle.token_k.clone()),
			cert_chain: Some(bundle.cert_chain.clone()),
			auth_type: None,
			auth_info: None,
			session_aes,
		};
		let modern_login = Bc::new_from_xml(
			BcMeta {
				msg_id: MSG_ID_LOGIN,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: MSG_CLASS_SIGV3_LOGIN,
			},
			BcXml {
				login_user: Some(login_user),
				login_net: Some(LoginNet::default()),
				..Default::default()
			},
		);
		log::info!("sigV3 login: sending (nonce={nonce}, iterations={iters})");
		log::debug!("sigV3 login body: {:?}", modern_login.body);
		sub.send(modern_login).await?;
		let reply = sub.recv().await?;
		log::info!(
			"sigV3 login: camera replied response_code={}",
			reply.meta.response_code
		);
		if reply.meta.response_code != 200 {
			log::warn!("sigV3 login reject body: {:?}", reply.body);
			return Err(Error::CameraLoginFail);
		}
		match reply.body {
			BcBody::ModernMsg(ModernMsg {
				payload:
					Some(BcPayloads::BcXml(BcXml {
						device_info: Some(info),
						..
					})),
				..
			}) => {
				log::info!("sigV3 login accepted by camera (response_code=200)");
				self.logged_in.store(true, Ordering::Relaxed);
				Ok(info)
			}
			other => Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(Bc {
					meta: reply.meta,
					body: other,
				}),
				why: "Expected a DeviceInfo message back from sigV3 login",
			}),
		}
	}

	/// Camera-local `getAccesskey` + `authLogin` login for new-firmware
	/// battery cameras that reject the legacy login (406) and advertise
	/// `getAccesskey` in their authTypeList. Needs no Reolink cloud bundle
	/// (unlike sigV3). Protocol + crypto live in `login_authlogin`.
	async fn run_authlogin(
		&self,
		sub_login: &mut super::connection::BcSubscription<'_>,
		credentials: &Credentials,
		nonce: &str,
		msg_num: u16,
	) -> Result<DeviceInfo> {
		let password = credentials.password.clone().unwrap_or_default();
		log::info!(
			"login: using camera-local authLogin (camera offered getAccesskey; no cloud bundle needed)"
		);

		// Step 1: getAccesskey — prove password knowledge with
		// authCode = md5(password+nonce); the camera replies with an
		// AES-encrypted challenge.
		let auth_code = super::login_authlogin::auth_code(&password, nonce);
		let getaccesskey = Bc::new_from_xml(
			BcMeta {
				msg_id: MSG_ID_LOGIN,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: MSG_CLASS_MODERN_LOGIN,
			},
			BcXml {
				login_user: Some(LoginUser {
					version: xml_ver(),
					auth_type: Some("getAccesskey".to_string()),
					auth_info: Some(AuthInfo {
						auth_code,
						phone_model: None,
					}),
					..Default::default()
				}),
				login_net: Some(LoginNet::default()),
				..Default::default()
			},
		);
		log::debug!(
			"authLogin: sending getAccesskey LoginUser: {:?}",
			getaccesskey.body
		);
		sub_login.send(getaccesskey).await?;
		let challenge_reply = sub_login.recv().await?;
		log::info!(
			"authLogin: getAccesskey reply response_code={}",
			challenge_reply.meta.response_code
		);
		log::debug!(
			"authLogin: getAccesskey reply body: {:?}",
			challenge_reply.body
		);

		// The challenge is two AES-encrypted base64 tokens carried in the
		// reply's binary payload. Log the raw bytes before parsing so an
		// unexpected framing is diagnosable from the tester's log.
		let payload: Vec<u8> = match &challenge_reply.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::Binary(bytes)),
				..
			}) => bytes.clone(),
			other => {
				log::warn!(
					"authLogin: getAccesskey reply was not a binary challenge \
					 (response_code={}); body: {:?}",
					challenge_reply.meta.response_code,
					other
				);
				return Err(Error::CameraLoginFail);
			}
		};
		log::info!(
			"authLogin: challenge payload = {} bytes: {}",
			payload.len(),
			hex_preview(&payload)
		);

		let (token_a, token_b) = match super::login_authlogin::parse_challenge(&payload) {
			Some(tokens) => tokens,
			None => {
				log::warn!(
					"authLogin: challenge payload ({} bytes) did not hold two tokens: {}",
					payload.len(),
					hex_preview(&payload)
				);
				return Err(Error::CameraLoginFail);
			}
		};
		let dec_a = super::login_authlogin::decrypt_token(&token_a, credentials, nonce)?;
		let dec_b = super::login_authlogin::decrypt_token(&token_b, credentials, nonce)?;
		log::debug!(
			"authLogin: decrypted challenge tokens (lengths {} / {})",
			dec_a.len(),
			dec_b.len()
		);

		// Step 2: authLogin — final login keyed by md5(token+nonce).
		let authlogin = Bc::new_from_xml(
			BcMeta {
				msg_id: MSG_ID_LOGIN,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: MSG_CLASS_MODERN_LOGIN,
			},
			BcXml {
				login_user: Some(LoginUser {
					version: xml_ver(),
					auth_type: Some("authLogin".to_string()),
					user_name: super::login_authlogin::authlogin_field(&dec_a, nonce),
					password: super::login_authlogin::authlogin_field(&dec_b, nonce),
					user_ver: 1,
					..Default::default()
				}),
				login_net: Some(LoginNet::default()),
				..Default::default()
			},
		);
		log::info!("authLogin: sending final authLogin login");
		log::debug!("authLogin: final LoginUser: {:?}", authlogin.body);
		sub_login.send(authlogin).await?;
		let final_reply = sub_login.recv().await?;
		log::info!(
			"authLogin: final reply response_code={}",
			final_reply.meta.response_code
		);
		if final_reply.meta.response_code != 200 {
			log::warn!(
				"login rejected: camera replied response_code={} to the authLogin login",
				final_reply.meta.response_code
			);
			return Err(Error::CameraLoginFail);
		}
		match final_reply.body {
			BcBody::ModernMsg(ModernMsg {
				payload:
					Some(BcPayloads::BcXml(BcXml {
						device_info: Some(info),
						..
					})),
				..
			}) => {
				log::info!("authLogin: login accepted by camera (response_code=200)");
				self.logged_in.store(true, Ordering::Relaxed);
				Ok(info)
			}
			other => Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(Bc {
					meta: final_reply.meta,
					body: other,
				}),
				why: "Expected a DeviceInfo message back from authLogin",
			}),
		}
	}
}

/// Hex preview of up to the first 64 bytes of a buffer, for diagnostic
/// logging of an unexpected `getAccesskey` challenge framing.
fn hex_preview(bytes: &[u8]) -> String {
	use std::fmt::Write as _;
	let mut s = String::new();
	for b in bytes.iter().take(64) {
		let _ = write!(s, "{:02x}", b);
	}
	if bytes.len() > 64 {
		s.push_str("...");
	}
	s
}

#[cfg(test)]
mod tests {
	use super::*;

	// A full login happy-path covers three scripted exchanges (legacy
	// login -> encryption xml, modern login -> device_info, then
	// MSG_ID_ABILITY_INFO for `populate_abilities`) plus valid md5
	// nonce math. Out of scope for this harness — exercised via the
	// real-hardware live-fire tests. We cover the legacy-reply-parse
	// error path here instead.

	#[tokio::test]
	async fn login_legacy_reply_missing_encryption_returns_err() {
		// Camera replies 200 but with no Encryption xml — login
		// should report `UnintelligibleReply`.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[test]
	fn max_encryption_enum_derives_debug_and_clone() {
		// Lock the three-variant matchup and the derive trait set.
		let a = MaxEncryption::None;
		let _ = format!("{:?}", a);
		let _b = a;
		let _ = format!("{:?}", MaxEncryption::BcEncrypt);
		let _ = format!("{:?}", MaxEncryption::Aes);
	}

	fn encryption_reply(nonce: &str) -> BcXml {
		BcXml {
			encryption: Some(Encryption {
				version: "1".into(),
				nonce: nonce.into(),
				type_: "md5".into(),
				..Default::default()
			}),
			..Default::default()
		}
	}

	fn encryption_reply_with_authtypes(nonce: &str, types: &[&str]) -> BcXml {
		BcXml {
			encryption: Some(Encryption {
				version: "1".into(),
				nonce: nonce.into(),
				type_: "md5".into(),
				auth_type_list: Some(AuthTypeList {
					auth_type: types.iter().map(|s| s.to_string()).collect(),
				}),
				..Default::default()
			}),
			..Default::default()
		}
	}

	/// Encryption reply advertising sigV3 + an ECDHE offer, as a cloud camera
	/// sends over the `LoginUpgrade` negotiation.
	fn encryption_reply_sigv3(nonce: &str, cam_pub: &str) -> BcXml {
		BcXml {
			encryption: Some(Encryption {
				version: "1".into(),
				nonce: nonce.into(),
				type_: "md5".into(),
				auth_type_list: Some(AuthTypeList {
					auth_type: vec!["sigV3".into()],
				}),
				sig_ver: Some("v3".into()),
				ecdhe: Some(Ecdhe {
					public_key_algo: "X25519".into(),
					public_key: cam_pub.into(),
					public_key_sign: String::new(),
					iterations: 1000,
				}),
			}),
			..Default::default()
		}
	}

	#[tokio::test]
	async fn login_all_three_max_encryption_variants_hit_enc_byte_branch() {
		// The enc_byte match has three arms. We don't need the full
		// crypto flow — the legacy-reply step alone exercises the
		// match. Fail-path in the legacy reply short-circuits before
		// any crypto runs. Seed a non-empty password so the AES /
		// BcEncrypt fail-fast (added 2026-05-01) doesn't intercept.
		use crate::bc_protocol::Credentials;
		for max_enc in [
			MaxEncryption::None,
			MaxEncryption::BcEncrypt,
			MaxEncryption::Aes,
		] {
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_LOGIN)
				.reply_with(|req| reply_200_xml(req, BcXml::default()))
				.build()
				.await;
			let creds = Credentials::new("admin", Some("hunter2"));
			let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
			let err = cam.login_with_maxenc(max_enc).await.expect_err("err");
			assert!(matches!(err, Error::UnintelligibleReply { .. }));
		}
	}

	#[tokio::test]
	async fn login_modern_reply_non_200_returns_credential_error() {
		// Legacy reply has an Encryption nonce → advance to modern
		// step; modern reply comes back with response_code != 200 →
		// CameraLoginFail.
		use crate::bc_protocol::connection::mock::reply_err_code;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("abc123")))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_err_code(req, 401))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("err");
		assert!(matches!(err, Error::CameraLoginFail));
	}

	#[tokio::test]
	async fn login_modern_reply_empty_modern_msg_is_auth_failed() {
		// Modern reply: ModernMsg { extension: None, payload: None } →
		// AuthFailed branch.
		use crate::bc_protocol::connection::mock::reply_200_empty;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("abc123")))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("err");
		assert!(matches!(err, Error::AuthFailed));
	}

	#[tokio::test]
	async fn login_happy_path_full_three_exchange_round_trip() {
		// Legacy → Encryption; Modern → DeviceInfo; then ability-info
		// subscription for populate_abilities. This exercises the
		// DeviceInfo success arm (lines 142-154) + the tail
		// populate_abilities + Ok(device_info) return (lines 168-171).
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("nonce-xyz")))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("1".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							system: Some(AbilityInfoToken {
								sub_module: vec![AbilityInfoSubModule {
									channel_id: Some(0),
									ability_value: "reboot_rw".into(),
								}],
							}),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("ok");
		assert_eq!(info.version.as_deref(), Some("1"));
		assert!(cam.logged_in.load(Ordering::Relaxed));
	}

	#[tokio::test]
	async fn non_cloud_camera_with_propagated_account_uses_normal_login() {
		// Regression guard: `cloud_account`/`cloud_password` are propagated to
		// EVERY camera from the config root, so an ordinary password camera
		// (is_cloud = false, i.e. discovery != "cloud") can have them set. It
		// must still take the normal modern login — NOT the cloud sigV3 branch,
		// which would wrongly fail it with "cloud camera did not offer sigV3".
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("nonce-xyz")))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("7".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection(mock).await;
		// Simulate top-level account propagation onto a non-cloud camera.
		cam.cloud_account = Some("you@example.com".into());
		cam.cloud_password = Some("pw".into());
		cam.cloud_uid = Some("UID".into());
		// is_cloud stays false — this camera is NOT discovery = "cloud".
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("normal login, not hijacked by the cloud branch");
		assert_eq!(info.version.as_deref(), Some("7"));
	}

	#[tokio::test]
	async fn non_cloud_camera_advertising_getaccesskey_uses_normal_login() {
		// A non-cloud camera on new firmware advertises the full auth-type
		// table — including `authLogin` + `getAccesskey` — yet must log in with
		// the plain modern (MD5) password login. The getAccesskey
		// *advertisement* must not divert a non-cloud login into the
		// camera-local authLogin handshake (reserved for discovery="cloud").
		// The second LOGIN reply here is a plain modern DeviceInfo, which the
		// authLogin path would reject as a non-binary challenge — so a
		// successful login proves the plain path was taken with a non-empty
		// password and getAccesskey on offer.
		use crate::bc_protocol::Credentials;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					encryption_reply_with_authtypes(
						"nonce-xyz",
						&["password", "sigV1", "sigV3", "authLogin", "getAccesskey"],
					),
				)
			})
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("9".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let creds = Credentials::new("admin", Some("hunter2"));
		let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("non-cloud camera must use the plain modern login, not authLogin");
		assert_eq!(info.version.as_deref(), Some("9"));
		assert!(cam.logged_in.load(Ordering::Relaxed));
	}

	#[tokio::test]
	async fn login_default_calls_login_with_maxenc_aes() {
		// `login()` is a thin delegate to login_with_maxenc(Aes). The
		// legacy-reply-parse error short-circuits before crypto runs, so
		// we just need a mock that replies with a 200 empty body. AES
		// requires a non-empty password (fail-fast); seed one so we
		// reach the wire.
		use crate::bc_protocol::Credentials;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let creds = Credentials::new("admin", Some("hunter2"));
		let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		let err = cam.login().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn login_aes_with_no_password_fails_before_wire_io() {
		// `password = None` under Aes used to silently MD5(nonce) and
		// hand the camera an obviously-wrong digest. Now refused
		// up-front with `AuthFailed`. Mock has no `expect_msg` — proves
		// no MSG_ID_LOGIN was ever sent.
		use crate::bc_protocol::Credentials;
		let mock = MockConnection::new().build().await;
		let creds = Credentials::new("admin", None::<String>);
		let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::Aes)
			.await
			.expect_err("must refuse AES + empty");
		assert!(matches!(err, Error::AuthFailed));
	}

	#[tokio::test]
	async fn login_bcencrypt_with_empty_password_fails_before_wire_io() {
		use crate::bc_protocol::Credentials;
		let mock = MockConnection::new().build().await;
		let creds = Credentials::new("admin", Some(""));
		let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::BcEncrypt)
			.await
			.expect_err("must refuse BcEncrypt + empty");
		assert!(matches!(err, Error::AuthFailed));
	}

	#[tokio::test]
	async fn login_none_with_empty_password_proceeds_to_wire() {
		// Anonymous-login (None encryption) is the only path where
		// empty password is meaningful — must not short-circuit.
		use crate::bc_protocol::Credentials;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let creds = Credentials::new("admin", Some(""));
		let cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("legacy reply was empty → UnintelligibleReply");
		// Reaches the wire, then dies on the missing Encryption xml —
		// proves the fail-fast did NOT trigger for None encryption.
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn login_modern_reply_missing_device_info_returns_unintelligible() {
		// Modern reply 200 but body is neither DeviceInfo nor empty
		// (payload is Some but of a non-DeviceInfo shape) → matches
		// the wildcard UnintelligibleReply branch.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("abc123")))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				// A 200 reply with an unrelated xml payload (encryption)
				// — not DeviceInfo and not empty → wildcard path.
				reply_200_xml(req, encryption_reply("xyz"))
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("err");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	// ---- sigV3 direct login (account / "cloud" cameras) ----

	fn test_bundle() -> crate::cloud::Sigv3Bundle {
		crate::cloud::Sigv3Bundle {
			token_p: "TOKENP".into(),
			token_s: "TOKENS".into(),
			token_k: "TOKENK".into(),
			cert_chain: "-----BEGIN-----\nAAAA\n-----END-----\n".into(),
		}
	}

	// A real 32-byte X25519 camera pubkey (base64), so build_sigv3_extras runs
	// the ECDHE for real without needing a live handshake.
	const CAM_PUB: &str = "FkTDv8H1jQKkU/nZkWPfxT8A7JArl7OqWwNQ4jerHCw=";

	#[tokio::test]
	async fn run_sigv3_login_builds_login_and_parses_device_info() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("9".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let info = cam
			.run_sigv3_login("123456", CAM_PUB, 1000, &test_bundle())
			.await
			.expect("sigV3 login ok");
		assert_eq!(info.version.as_deref(), Some("9"));
		assert!(cam.logged_in.load(Ordering::Relaxed));
	}

	#[tokio::test]
	async fn run_sigv3_login_rejects_non_200() {
		use crate::bc_protocol::connection::mock::reply_err_code;
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_err_code(req, 417))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.run_sigv3_login("1", CAM_PUB, 1000, &test_bundle())
			.await
			.expect_err("417 -> CameraLoginFail");
		assert!(matches!(err, Error::CameraLoginFail));
	}

	#[tokio::test]
	async fn run_sigv3_login_rejects_non_device_info_200() {
		// 200 but the payload is not a DeviceInfo -> UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("nonce")))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.run_sigv3_login("1", CAM_PUB, 1000, &test_bundle())
			.await
			.expect_err("non-DeviceInfo 200 -> err");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn cloud_camera_without_sigv3_offer_errors_clearly() {
		// A cloud ("account") camera reached over a non-handshake path (no
		// lver=3 nc/pl) does LoginUpgrade -> Encryption. If the camera does NOT
		// advertise sigV3 (and offers no getAccesskey), the login fails with a
		// clear Error::Cloud. (No sigV3 offer => no cloud-token mint => no
		// network call.)
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply("abc123")))
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection(mock).await;
		cam.is_cloud = true;
		cam.cloud_account = Some("you@example.com".into());
		let err = cam
			.login()
			.await
			.expect_err("no sigV3 offer -> cloud error");
		assert!(matches!(err, Error::Cloud(_)), "got {err:?}");
	}

	#[tokio::test]
	async fn cloud_camera_logs_in_via_encryption_negotiation() {
		// A cloud camera reached over a non-handshake path (no lver=3 `nc`/`pl`)
		// does LoginUpgrade -> Encryption(sigV3) -> mint the cloud bundle ->
		// signed login -> DeviceInfo. Seeding the per-UID bundle cache makes the
		// mint return offline (no network).
		let uid = "9527000ENCNEGOTEST";
		crate::cloud::seed_cache_for_test(
			uid,
			crate::cloud::Sigv3Bundle {
				token_p: r#"{"exp":4102444800}"#.into(),
				token_s: "SIG".into(),
				token_k: "KEY".into(),
				cert_chain: "-----BEGIN CERTIFICATE-----\nAA\n-----END CERTIFICATE-----\n".into(),
			},
			4102444800,
		);

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, encryption_reply_sigv3("987654321", CAM_PUB)))
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("5".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection(mock).await;
		cam.is_cloud = true;
		cam.cloud_account = Some("you@example.com".into());
		cam.cloud_password = Some("pw".into());
		cam.cloud_uid = Some(uid.into());
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("cloud login over Encryption negotiation");
		assert_eq!(info.version.as_deref(), Some("5"));
		assert!(cam.logged_in.load(Ordering::Relaxed));
		crate::cloud::drop_cache_for_test(uid);
	}

	#[tokio::test]
	async fn cloud_camera_prefers_sigv3_over_getaccesskey() {
		// Priority guard: a cloud camera advertising BOTH sigV3 and getAccesskey
		// must take the sigV3 cloud-token login, not the camera-local authLogin
		// handshake. Seeding the bundle cache lets the mint return offline. If
		// the authLogin arm wrongly won, the second reply (a plain DeviceInfo)
		// would be rejected as a non-binary challenge and login would fail — so
		// a successful DeviceInfo proves sigV3 was chosen first.
		let uid = "9527000SIGV3PRIORITY";
		crate::cloud::seed_cache_for_test(
			uid,
			crate::cloud::Sigv3Bundle {
				token_p: r#"{"exp":4102444800}"#.into(),
				token_s: "SIG".into(),
				token_k: "KEY".into(),
				cert_chain: "-----BEGIN CERTIFICATE-----\nAA\n-----END CERTIFICATE-----\n".into(),
			},
			4102444800,
		);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						encryption: Some(Encryption {
							version: "1".into(),
							nonce: "987654321".into(),
							type_: "md5".into(),
							auth_type_list: Some(AuthTypeList {
								auth_type: vec!["sigV3".into(), "getAccesskey".into()],
							}),
							sig_ver: Some("v3".into()),
							ecdhe: Some(Ecdhe {
								public_key_algo: "X25519".into(),
								public_key: CAM_PUB.into(),
								public_key_sign: String::new(),
								iterations: 1000,
							}),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("5".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection(mock).await;
		cam.is_cloud = true;
		cam.cloud_account = Some("you@example.com".into());
		cam.cloud_password = Some("pw".into());
		cam.cloud_uid = Some(uid.into());
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("sigV3 must win over getAccesskey");
		assert_eq!(info.version.as_deref(), Some("5"));
		crate::cloud::drop_cache_for_test(uid);
	}

	#[tokio::test]
	async fn login_authlogin_path_full_round_trip() {
		// Drive login_with_maxenc through the camera-local getAccesskey +
		// authLogin path: legacy → Encryption{authTypeList: getAccesskey, no
		// ECDHE} → getAccesskey (binary AES challenge) → authLogin → DeviceInfo.
		use crate::bc::crypto::EncryptionProtocol;
		use crate::bc_protocol::connection::mock::reply_200_xml;
		use crate::bc_protocol::Credentials;
		use base64::Engine as _;

		let b64 = base64::engine::general_purpose::STANDARD;
		let nonce = "noncexyz";
		let creds = Credentials::new("admin", Some("pw".to_string()));
		let key = creds.make_aeskey(nonce);
		// Encrypt the two challenge tokens exactly as the camera would.
		let token = |plain: &[u8]| b64.encode(EncryptionProtocol::aes(key).encrypt(0, plain));
		let (ta, tb) = (token(b"TOKENA"), token(b"TOKENB"));
		let mut challenge = vec![0u8; 0x80];
		challenge[..ta.len()].copy_from_slice(ta.as_bytes());
		challenge[0x40..0x40 + tb.len()].copy_from_slice(tb.as_bytes());

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(move |req| {
				reply_200_xml(
					req,
					encryption_reply_with_authtypes(nonce, &["password", "getAccesskey"]),
				)
			})
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(move |req| Bc {
				meta: BcMeta {
					msg_id: req.meta.msg_id,
					channel_id: req.meta.channel_id,
					msg_num: req.meta.msg_num,
					stream_type: 0,
					response_code: 200,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: None,
					payload: Some(BcPayloads::Binary(challenge.clone())),
				}),
			})
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						device_info: Some(DeviceInfo {
							version: Some("7".into()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_ABILITY_INFO)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ability_info: Some(AbilityInfo {
							username: "admin".into(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		// authLogin runs only for cloud cameras (discovery="cloud"); one
		// offering getAccesskey without sigV3 routes into it.
		cam.is_cloud = true;
		let info = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect("authLogin login ok");
		assert_eq!(info.version.as_deref(), Some("7"));
		assert!(cam.logged_in.load(Ordering::Relaxed));
	}

	#[tokio::test]
	async fn run_authlogin_rejects_non_binary_challenge() {
		// getAccesskey reply that isn't a binary challenge -> CameraLoginFail.
		use crate::bc_protocol::connection::mock::reply_200_xml;
		use crate::bc_protocol::Credentials;
		let nonce = "n";
		let creds = Credentials::new("admin", Some("pw".to_string()));
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(move |req| {
				reply_200_xml(
					req,
					encryption_reply_with_authtypes(nonce, &["getAccesskey"]),
				)
			})
			.expect_msg(MSG_ID_LOGIN)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let mut cam = BcCamera::from_mock_connection_with_credentials(mock, creds).await;
		// authLogin is reachable only for cloud cameras now.
		cam.is_cloud = true;
		let err = cam
			.login_with_maxenc(MaxEncryption::None)
			.await
			.expect_err("non-binary challenge -> err");
		assert!(matches!(err, Error::CameraLoginFail));
	}

	#[tokio::test]
	async fn run_sigv3_direct_errors_without_cloud_credentials() {
		// from_mock_connection leaves cloud_account/uid None, so the mint
		// pre-flight in run_sigv3_direct fails fast with a clear Cloud error
		// (no network call).
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.run_sigv3_direct(1, CAM_PUB, 1000)
			.await
			.expect_err("no cloud_account -> Cloud err");
		assert!(matches!(err, Error::Cloud(_)));
	}
}
