//! Operator-facing configuration for the wake server.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Operator-facing config block. Bind IP is inherited from the top-level
/// `bind_addr`, not duplicated here.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakeServerConfig {
	#[serde(default)]
	pub enable: bool,

	#[serde(default = "default_middleman_port")]
	pub middleman_port: u16,

	#[serde(default = "default_register_port")]
	pub register_port: u16,

	/// The heartbeat cadence this bridge assumes when it sizes its own
	/// freshness window: `stale_after_ms` must cover at least one of these
	/// intervals, because a camera we have not heard from for longer than the
	/// window is treated as gone and its wake requests are refused. It is also
	/// what gets advertised when `hb_advice_ms` is left unset.
	#[serde(default = "default_heartbeat_ms")]
	pub heartbeat_ms: u32,

	/// The heartbeat cadence advertised to cameras in `R2D_HB_R`. Unset by
	/// default, which means "advertise `heartbeat_ms`", so a configuration that
	/// predates this key advertises exactly what it advertises today.
	///
	/// **The advertised value is advice, not an order.** The firmware picks its
	/// own cadence: `docs/cloud-interception.md` records the observed
	/// heartbeat at roughly every 10 s while the advertised `R2D_HB_R` carries
	/// 20000 ms, and calls the cadence firmware-controlled in as many places.
	/// Setting this key changes what we say, not what we hear, which is exactly
	/// why the freshness window is not derived from it: a window sized on an
	/// advice the camera ignores would evict live cameras from the registry and
	/// refuse wake requests for cameras that are awake and beating. That is why
	/// `stale_after_ms >= heartbeat_ms` is checked against `heartbeat_ms` and
	/// not against this key.
	///
	/// That attribution is `docs/cloud-interception.md`. Tonight's field observation neither
	/// confirms nor refutes it - the info line is filtered by novelty, so it measures nothing -
	/// and the beat-window counter exists now, so this sentence is waiting on a number rather
	/// than on another capture.
	#[serde(default)]
	pub hb_advice_ms: Option<u32>,

	#[serde(default = "default_stale_after_ms")]
	pub stale_after_ms: u64,

	/// Off by default. When on, a wake request refused for a UID that the
	/// bridge's own `[[cameras]]` declares is logged with the reason that
	/// matters, instead of the generic "unknown or stale UID" line that reads
	/// like a typo in the configuration.
	///
	/// This changes a log sentence and nothing else. The answer sent to the
	/// client stays `rsp: -1`, because the client treats only `rsp != -1 &&
	/// rsp != -3` as success: inventing a code here would turn a clean "not
	/// right now" into a client that waits for a camera that was never touched.
	/// No address is fabricated either. The entry point that has to stay shut
	/// is unchanged for everyone, declared or not.
	#[serde(default)]
	/// The match follows the push listener's prefix rule, so the short form a header carries
	/// explains itself against the long form this bridge declared; see [`DeclaredUids`].
	pub explain_declared_uid: bool,

	/// Bind IP, populated from the top-level `bind_addr` when the binary
	/// constructs a `RuntimeConfig` (see [`RuntimeConfig::from_block`]).
	/// Skipped at TOML parse time so operators never set it directly.
	#[serde(skip)]
	pub bind: Option<IpAddr>,
}

impl Default for WakeServerConfig {
	fn default() -> Self {
		Self {
			enable: false,
			middleman_port: default_middleman_port(),
			register_port: default_register_port(),
			heartbeat_ms: default_heartbeat_ms(),
			hb_advice_ms: None,
			stale_after_ms: default_stale_after_ms(),
			explain_declared_uid: false,
			bind: None,
		}
	}
}

fn default_middleman_port() -> u16 {
	9999
}
fn default_register_port() -> u16 {
	58200
}
fn default_heartbeat_ms() -> u32 {
	20000
}
fn default_stale_after_ms() -> u64 {
	80000
}

/// Validated runtime view consumed by `run()`. Constructed by the binary
/// from a `WakeServerConfig` plus the top-level `bind_addr`.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
	pub bind: IpAddr,
	pub middleman_port: u16,
	pub register_port: u16,
	/// The cadence to advertise in `R2D_HB_R`: `hb_advice_ms` when the operator
	/// set it, `heartbeat_ms` otherwise. Named for what the server actually
	/// sends, because the two operator keys it can come from have different
	/// jobs and only this one ever reaches the wire.
	pub hb_advice_ms: u32,
	pub stale_after_ms: u64,
	/// Which refused wake requests deserve an explanation, and which UIDs the
	/// bridge itself declared. Supplied by the binary, not by this block: the
	/// wake server has no business reading the camera list.
	pub declared: DeclaredUids,
}

/// The declared half of a refused wake request.
///
/// Held as trimmed strings and matched by the **same prefix rule the push listener uses**
/// (`match_camera_by_uid` in `src/push_listener.rs`, commit `cc07ff0`): a configured uid and a
/// uid seen on the wire match when either starts with the other. The rule is not decoration. The
/// 20-character form comes out of the `D2R_HB` heartbeat and the 16-character form out of the
/// binary header, so under an exact comparison a request asking the short form against a long
/// declaration in the file would not match - and this predicate is what decides whether the
/// operator gets the explanation written for exactly that moment, or the generic "unknown or
/// stale UID" sentence. Matching either direction widens **an explanation only**: it never
/// selects an address, never authorises an order, and cannot reach a camera that was not declared.
///
/// A declared UID that differs
/// from what the camera actually sends by case or padding falls back to the
/// generic sentence, which is the honest answer: this bridge has not heard that
/// string, and saying otherwise would blame the wrong thing.
#[derive(Debug, Clone, Default)]
pub struct DeclaredUids {
	pub explain: bool,
	pub uids: Vec<String>,
}

impl DeclaredUids {
	pub fn is_declared(&self, uid: &str) -> bool {
		self.uids.iter().any(|declared| {
			declared == uid
				|| uid.starts_with(declared.as_str())
				|| declared.starts_with(uid)
		})
	}
}

/// The sentence that replaces the generic refusal, or `None` when the generic
/// refusal is the right answer. A function rather than an inline literal so the
/// sentence can be pinned by a test without capturing logs.
pub fn refusal_explanation<'a>(declared: &'a DeclaredUids, uid: &str) -> Option<&'static str> {
	if declared.is_declared(uid) {
		Some(
			"C2R_C refused: this UID is declared in the bridge's configuration, but no live UDP path \
				was ever heard for it, so no wake packet was sent and no address was invented; \
				the camera has to reach this bridge first (announce, heartbeat, or any packet on the \
				register port)",
		)
	} else {
		None
	}
}

impl RuntimeConfig {
	/// Build a runtime config from a parsed `[wake_server]` block plus a
	/// resolved bind IP. Returns `Err(_)` with a human-readable message on
	/// validation failure.
	pub fn from_block(block: &WakeServerConfig, bind: IpAddr) -> Result<Self, String> {
		if block.middleman_port == 0 {
			return Err("middleman_port must be > 0".into());
		}
		if block.register_port == 0 {
			return Err("register_port must be > 0".into());
		}
		if block.middleman_port == block.register_port {
			return Err(format!(
				"middleman_port and register_port must differ (both {})",
				block.middleman_port
			));
		}
		if block.heartbeat_ms < 1000 {
			return Err(format!(
				"heartbeat_ms must be >= 1000 (got {})",
				block.heartbeat_ms
			));
		}
		if let Some(advice) = block.hb_advice_ms {
			if advice < 1000 {
				return Err(format!(
					"hb_advice_ms must be >= 1000 when set (got {}); the same floor as heartbeat_ms, \
					 because a sub-second advice is a firmware that will not honour it either way",
					advice
				));
			}
		}
		if block.stale_after_ms < block.heartbeat_ms as u64 {
			return Err(format!(
				"stale_after_ms ({}) must be >= heartbeat_ms ({})",
				block.stale_after_ms, block.heartbeat_ms
			));
		}
		let hb_advice_ms = block.hb_advice_ms.unwrap_or(block.heartbeat_ms);
		if advice_outlives_the_window(hb_advice_ms as u64, block.stale_after_ms) {
			// A warning, never a refusal: the advice is not obeyed, so a camera
			// that ignores it keeps working fine, and refusing a number the
			// firmware itself ignores would break configurations that are
			// healthy today.
			tracing::warn!(
				hb_advice_ms,
				stale_after_ms = block.stale_after_ms,
				"advertised heartbeat is not shorter than the freshness window: a camera that obeys \
				 it would be evicted between two beats, and its wake requests refused"
			);
		}
		Ok(Self {
			bind,
			middleman_port: block.middleman_port,
			register_port: block.register_port,
			hb_advice_ms,
			stale_after_ms: block.stale_after_ms,
			declared: DeclaredUids {
				explain: block.explain_declared_uid,
				uids: Vec::new(),
			},
		})
	}

	/// Tell the server which UIDs the bridge declared, so a refusal can be
	/// explained when the answer is "declared, but never heard". Kept as a
	/// builder rather than a parameter of `from_block`: the list belongs to the
	/// camera configuration, which the binary holds and this block does not.
	#[must_use]
	pub fn with_declared_uids<I: IntoIterator<Item = String>>(mut self, uids: I) -> Self {
		self.declared.uids = uids.into_iter().map(|u| u.trim().to_string()).collect();
		self
	}
}

/// Whether an advertised cadence would outlive the window that keeps a camera
/// in the registry. Pure so the rule the warning above uses can be pinned by a
/// unit test instead of being read out of a log line.
const fn advice_outlives_the_window(advice_ms: u64, stale_after_ms: u64) -> bool {
	advice_ms >= stale_after_ms
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::Ipv4Addr;

	fn loopback() -> IpAddr {
		Ipv4Addr::LOCALHOST.into()
	}

	#[test]
	fn defaults_match_spec() {
		let cfg: WakeServerConfig = toml::from_str("").unwrap();
		assert!(!cfg.enable);
		assert_eq!(cfg.middleman_port, 9999);
		assert_eq!(cfg.register_port, 58200);
		assert_eq!(cfg.heartbeat_ms, 20000);
		assert_eq!(cfg.stale_after_ms, 80000);
		// Unset, not 20000: the difference matters, because the fallback is
		// `heartbeat_ms`, which an operator may have raised.
		assert_eq!(cfg.hb_advice_ms, None);
	}

	#[test]
	fn deny_unknown_fields() {
		let result: Result<WakeServerConfig, _> = toml::from_str("totally_made_up_field = 1");
		assert!(result.is_err());
	}

	#[test]
	fn runtime_rejects_zero_ports() {
		let block = WakeServerConfig {
			middleman_port: 0,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback()).is_err());
	}

	#[test]
	fn from_block_zero_register_port_message_mentions_register_port() {
		let block = WakeServerConfig {
			register_port: 0,
			..WakeServerConfig::default()
		};
		let err = RuntimeConfig::from_block(&block, loopback())
			.expect_err("zero register_port must error");
		assert!(
			err.contains("register_port"),
			"expected error to mention register_port, got: {err}"
		);
	}

	#[test]
	fn runtime_rejects_equal_ports() {
		let block = WakeServerConfig {
			middleman_port: 5000,
			register_port: 5000,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback()).is_err());
	}

	#[test]
	fn runtime_rejects_low_heartbeat() {
		let block = WakeServerConfig {
			heartbeat_ms: 500,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback()).is_err());
	}

	#[test]
	fn runtime_rejects_stale_below_heartbeat() {
		let block = WakeServerConfig {
			heartbeat_ms: 5000,
			stale_after_ms: 1000,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback()).is_err());
	}

	#[test]
	fn runtime_accepts_defaults_with_loopback_bind() {
		let block = WakeServerConfig::default();
		let rt = RuntimeConfig::from_block(&block, loopback()).unwrap();
		assert_eq!(rt.middleman_port, 9999);
		assert_eq!(rt.register_port, 58200);
	}

	#[test]
	fn an_existing_config_admits_what_it_always_advertised() {
		// The reason `hb_advice_ms` is an Option and not a plain 20000: an
		// operator who raised `heartbeat_ms` must keep advertising that number
		// after upgrading, not silently drop to the default.
		let block = WakeServerConfig {
			heartbeat_ms: 60000,
			..WakeServerConfig::default()
		};
		let rt = RuntimeConfig::from_block(&block, loopback()).unwrap();
		assert_eq!(rt.hb_advice_ms, 60000);
	}

	#[test]
	fn the_advice_can_be_set_apart_from_the_assumed_cadence() {
		let block = WakeServerConfig {
			heartbeat_ms: 40000,
			hb_advice_ms: Some(10000),
			stale_after_ms: 80000,
			..WakeServerConfig::default()
		};
		let rt = RuntimeConfig::from_block(&block, loopback()).unwrap();
		assert_eq!(rt.hb_advice_ms, 10000, "what the wire carries");
	}

	#[test]
	fn the_advice_is_read_from_the_toml() {
		let cfg: WakeServerConfig = toml::from_str("hb_advice_ms = 15000").unwrap();
		assert_eq!(cfg.hb_advice_ms, Some(15000));
	}

	#[test]
	fn a_sub_second_advice_is_refused_naming_its_own_key() {
		let block = WakeServerConfig {
			hb_advice_ms: Some(200),
			..WakeServerConfig::default()
		};
		let err = RuntimeConfig::from_block(&block, loopback()).unwrap_err();
		assert!(
			err.starts_with("hb_advice_ms"),
			"the operator has to know which key to edit: {err}"
		);
	}

	#[test]
	fn the_freshness_window_is_still_measured_against_the_assumed_cadence() {
		// The old rule, unchanged, and deliberately not moved to the new key:
		// the window protects us from a camera we have not heard from, and what
		// we have not heard from is governed by the cadence we assume, not by
		// the advice the camera is free to ignore.
		let block = WakeServerConfig {
			heartbeat_ms: 5000,
			hb_advice_ms: Some(90000),
			stale_after_ms: 1000,
			..WakeServerConfig::default()
		};
		let err = RuntimeConfig::from_block(&block, loopback()).unwrap_err();
		assert!(err.contains("heartbeat_ms"), "names the key to fix: {err}");
	}

	#[test]
	fn an_advice_that_outlives_the_window_is_spotted_but_not_refused() {
		// The firmware is not obliged to obey, so this combination is a camera
		// that keeps working today. It is a warning at startup, never a refusal.
		assert!(advice_outlives_the_window(90000, 80000));
		assert!(advice_outlives_the_window(80000, 80000), "equal: one beat exactly");
		assert!(!advice_outlives_the_window(20000, 80000));
		let block = WakeServerConfig {
			hb_advice_ms: Some(90000),
			..WakeServerConfig::default()
		};
		let rt = RuntimeConfig::from_block(&block, loopback()).expect("a warning, not an error");
		assert_eq!(rt.hb_advice_ms, 90000);
	}

	#[test]
	fn explaining_a_refusal_is_off_by_default() {
		let cfg: WakeServerConfig = toml::from_str("").unwrap();
		assert!(!cfg.explain_declared_uid);
		assert!(!WakeServerConfig::default().explain_declared_uid);
		let rt = RuntimeConfig::from_block(&cfg, loopback()).unwrap();
		assert!(
			refusal_explanation(&rt.declared, "ANYUID").is_none(),
			"off means the generic sentence, for declared uids too"
		);
	}

	#[test]
	fn a_declared_uid_gets_told_why_it_was_refused() {
		let block = WakeServerConfig {
			explain_declared_uid: true,
			..WakeServerConfig::default()
		};
		let rt = RuntimeConfig::from_block(&block, loopback())
			.unwrap()
			.with_declared_uids(vec![" 9527000A00000001 ".to_string()]);
		let reason = refusal_explanation(&rt.declared, "9527000A00000001")
			.expect("a declared UID has a reason worth printing");
		assert!(
			reason.contains("configuration") && reason.contains("no wake packet was sent"),
			"the sentence has to say what did not happen: {reason}"
		);
		assert!(
			!reason.contains("address was invented").then(|| reason).is_none(),
			"the sentence must say no address was made up: {reason}"
		);
		assert_eq!(
			refusal_explanation(&rt.declared, "OTHERUID"),
			None,
			"an undeclared UID keeps the generic line"
		);
	}

	#[test]
	fn the_short_form_from_a_header_explains_against_a_long_declaration() {
		// The 20-character form comes out of the beat, the 16-character one out of the
		// binary header. A refusal asked in one shape against a declaration in the other is
		// the exact moment this explanation was written for, and an exact comparison let it
		// fall through to the generic "unknown or stale UID" sentence.
		let block = WakeServerConfig {
			explain_declared_uid: true,
			..WakeServerConfig::default()
		};
		let rt = RuntimeConfig::from_block(&block, loopback())
			.expect("a valid block")
			.with_declared_uids(vec!["9527000SYNTHLONG0001".to_string()]);
		assert!(rt.declared.is_declared("9527000SYNTHLONG"), "short form of a long declaration");
		assert!(rt.declared.is_declared("9527000SYNTHLONG0001"), "the declared form itself");
		assert!(
			rt.declared.is_declared("9527000SYNTHLONG0001EXTRA"),
			"a declaration asked back with a firmware suffix appended"
		);
		assert!(
			!rt.declared.is_declared("9527000OTHERSHAPE01"),
			"an unrelated uid keeps the generic line"
		);
		assert!(
			refusal_explanation(&rt.declared, "9527000SYNTHLONG").is_some(),
			"the short form gets the written explanation, not the generic sentence"
		);
	}


	#[test]
	fn the_declared_list_is_trimmed_but_otherwise_taken_literally() {
		let rt = RuntimeConfig::from_block(&WakeServerConfig::default(), loopback())
			.unwrap()
			.with_declared_uids(vec!["  ABC123  ".to_string()]);
		assert!(rt.declared.uids.iter().any(|u| u == "ABC123"));
	}
}
