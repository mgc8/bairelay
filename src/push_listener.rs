//! TCP listener that turns the camera's motion-time HTTPS connect
//! attempt into a local motion event.
//!
//! Live-verified discovery (2026-04-28 pcap) showed that an Argus battery
//! camera's only outbound traffic on motion — while bairelay is
//! disconnected — is a single HTTPS connection to `pushx.reolink.com`.
//! With operator-side DNS hijack of `pushx.reolink.com` →
//! `push_listener.push_listener_addr`, that connection lands on us. The camera
//! pins the cert chain, so we can't decode the JSON body — but we don't
//! need to. The TCP-SYN itself is the motion signal: in a 100 s baseline
//! capture, the camera made zero connections to `pushx.reolink.com`
//! outside the two motion edges. So we treat any `accept()` from a
//! registered camera's IP as a motion event, fire `status/motion=on`
//! plus a `motion_wake_hold_secs` wake-lock, and close the socket.
//!
//! The wake-lock acquire kicks the connect loop, which uses the local
//! BcUdp wake server to bring the camera online. Once the
//! Baichuan session is up, the existing `motion_listener` subscribes
//! and publishes the eventual `motion=off` from the camera itself.
//! We publish a fallback `motion=off` after `motion_wake_hold_secs` so
//! HA never gets stuck on "motion=on" if reconnect failed. A push that
//! lands within `WAKE_DEDUPE_WINDOW` of a webhook wake from the same
//! camera is that wake, already reported, and is dropped
//! (`wake_events::push_edge`).
//!
//! **Threat model — source-IP trust.** A peer IP that resolves through
//! the wake-server registry is treated as authentic; we have no further
//! authentication on the TCP-accept itself (the camera's TLS handshake
//! never completes — the cert chain is pinned to Reolink's CA). On a
//! flat home LAN, anyone able to spoof the camera's IP can fire a fake
//! motion edge, which at worst wakes the camera for `motion_wake_hold`
//! and publishes `status/motion=on` — bounded blast radius. Acceptable
//! for the single-operator deployment target; explicitly out of scope
//! for any future multi-tenant use.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use bairelay_mqtt::SharedMqttClient;
use bairelay_wake_server::registry::CameraRegistry;

use crate::camera::CameraHandle;

/// Validated runtime config; the binary builds this from
/// `[push_listener]` + the resolved bind fallback chain.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
	pub bind_addr: IpAddr,
	pub bind_port: u16,
	pub motion_wake_hold: Duration,
	/// TTL for `CameraRegistry::lookup_by_ip`. Mirrors the wake server's
	/// `stale_after_ms` so a long-silent camera doesn't get its push
	/// matched against an ancient heartbeat.
	pub stale_after: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum PushListenerError {
	#[error("push listener failed to bind to {addr}: {source}")]
	Bind {
		addr: SocketAddr,
		source: std::io::Error,
	},
	#[error("push listener accept error: {0}")]
	Accept(#[from] std::io::Error),
}

/// Spawn the listener; returns when the cancellation token fires or a
/// fatal accept error occurs. Convenience wrapper that binds the socket
/// and forwards to [`run_with_listener`]. The binary uses the pre-bind
/// path so a port conflict fails the daemon synchronously at startup.
pub async fn run(
	cfg: RuntimeConfig,
	registry: Arc<CameraRegistry>,
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	cancel: CancellationToken,
) -> Result<(), PushListenerError> {
	let addr = SocketAddr::new(cfg.bind_addr, cfg.bind_port);
	let listener = TcpListener::bind(addr)
		.await
		.map_err(|source| PushListenerError::Bind { addr, source })?;
	run_with_listener(listener, cfg, registry, cameras, mqtt, topic_prefix, cancel).await
}

/// Run the accept loop against an already-bound `TcpListener`. The
/// binary binds in `main.rs` so a startup-time bind error halts the
/// process before any "started" log line.
pub async fn run_with_listener(
	listener: TcpListener,
	cfg: RuntimeConfig,
	registry: Arc<CameraRegistry>,
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	cancel: CancellationToken,
) -> Result<(), PushListenerError> {
	tracing::info!(
		bind_addr = %cfg.bind_addr,
		bind_port = cfg.bind_port,
		motion_wake_hold_secs = cfg.motion_wake_hold.as_secs_f64(),
		"Push listener started"
	);

	// One report per source per window: see [`Misses`].
	let mut misses = Misses::default();
	loop {
		tokio::select! {
			_ = cancel.cancelled() => {
				tracing::debug!("Push listener cancelled");
				return Ok(());
			}
			accept = listener.accept() => {
				match accept {
					Ok((sock, peer)) => {
						drop(sock);
						handle_push(
							peer.ip(),
							&registry,
							&cameras,
							mqtt.as_ref(),
							&topic_prefix,
							&cfg,
							&cancel,
							&mut misses,
						);
					}
					Err(e) => {
						tracing::warn!(error = %e, "push listener accept failed");
					}
				}
			}
		}
	}
}

/// Match a peer IP to a known camera and (when matched) fire the motion
/// event in a detached task so the accept loop returns to listening
/// immediately. Detached tasks are bounded by `motion_wake_hold` plus
/// the cancel token, so they can't outlive shutdown.
fn handle_push(
	peer_ip: IpAddr,
	registry: &Arc<CameraRegistry>,
	cameras: &Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<&SharedMqttClient>,
	topic_prefix: &str,
	cfg: &RuntimeConfig,
	cancel: &CancellationToken,
	misses: &mut Misses,
) {
	let Some((registry_uid, _entry)) =
		registry.lookup_by_ip(peer_ip, Instant::now(), cfg.stale_after)
	else {
		// This is where a doorbell press dies quietly. The registry lives in RAM,
		// so between a module restart and this camera's next beat every push is
		// unmatchable: that has to be said at the level the operator reads, naming
		// the address and saying what was thrown away.
		if misses.report(peer_ip, Instant::now()) {
			tracing::info!(
				peer_ip = %peer_ip,
				fresh_window_ms = cfg.stale_after.as_millis() as u64,
				"push listener discarded a connection: no camera is registered from 				 this address inside the freshness window, so no motion event was 				 published and no wake was held. The registry is memory-only: it is 				 empty after a restart until this camera's next beat."
			);
		}
		return;
	};

	let Some(handle) = match_camera_by_uid(cameras, &registry_uid) else {
		if misses.report(peer_ip, Instant::now()) {
			tracing::info!(
				peer_ip = %peer_ip,
				registry_uid = %registry_uid,
				"push listener discarded a connection: the address belongs to a 				 registered camera but no configured camera carries that UID, so no 				 motion event was published and no wake was held"
			);
		}
		return;
	};

	if crate::wake_events::push_edge(
		Arc::clone(handle),
		mqtt,
		topic_prefix,
		cfg.motion_wake_hold,
		cancel,
	) {
		tracing::info!(
			camera = %handle.name(),
			peer_ip = %peer_ip,
			"Motion push from camera (treating TCP-accept as motion edge)"
		);
	}
}

/// Report rate limit for discarded pushes: one line per source address per
/// window, and a bounded number of addresses remembered. A camera retrying a push
/// it will never get would otherwise write one line per connection, and a peer
/// spraying addresses must not be able to grow this map. The reasoning is the one
/// the register server's retransmission memory uses.
#[derive(Default)]
struct Misses {
	last: HashMap<IpAddr, Instant>,
}

impl Misses {
	/// Window between two reports for one address: long enough that a burst of
	/// retries is a single line, short enough that an outage keeps showing up
	/// while it lasts.
	const WINDOW: Duration = Duration::from_secs(30);
	/// Cap on remembered addresses; the oldest gives way.
	const HOSTS: usize = 64;

	fn report(&mut self, peer_ip: IpAddr, now: Instant) -> bool {
		if let Some(seen) = self.last.get(&peer_ip) {
			if now - *seen < Self::WINDOW {
				return false;
			}
		}
		if !self.last.contains_key(&peer_ip) && self.last.len() >= Self::HOSTS {
			let oldest = self.last.iter().min_by_key(|(_, when)| **when).map(|(ip, _)| *ip);
			if let Some(ip) = oldest {
				self.last.remove(&ip);
			}
		}
		self.last.insert(peer_ip, now);
		true
	}
}

/// Resolve a `(name → CameraHandle)` map entry from a registry-side UID.
///
/// The uid a camera writes on the network is the short form: 16 characters, no
/// firmware suffix. A configured uid may be that same form or the long form that
/// carries the suffix, depending on where the operator copied it from. So the two
/// relate by prefix **in either direction**, and testing one direction only — which
/// this function used to do — silently matches nothing when the configuration
/// carries the long form: a 16-character registry uid is never prefixed by a
/// 20-character configured one, and the event that arrived is thrown away as if
/// nobody owned it. The longest shared prefix wins, so two cameras whose uids share
/// a root cannot quietly cross, and the name breaks a tie so the answer does not
/// depend on hash order.
/// Long-form (firmware-suffixed) UIDs from `D2R_HB` need to match short
/// configured UIDs via `starts_with`, mirroring
/// [`CameraRegistry::lookup_fresh`].
///
/// Resolution is **deterministic**: when two configured UIDs both
/// prefix the registry UID (e.g. `UID-A` and `UID-AB` both prefix
/// `UID-AB-XYZ`), the **longest** match wins — never the
/// HashMap-iteration-order accident. This guards the prefix logic
/// against silent cross-camera dispatch when configured UIDs share
/// roots.
///
/// Made `pub(crate)` so the unit test below can drive it without a
/// live socket.
pub(crate) fn match_camera_by_uid<'a>(
	cameras: &'a HashMap<String, Arc<CameraHandle>>,
	registry_uid: &str,
) -> Option<&'a Arc<CameraHandle>> {
	cameras
		.values()
		.filter_map(|h| {
			let configured = h.config().uid.as_deref()?;
			let related =
				registry_uid.starts_with(configured) || configured.starts_with(registry_uid);
			related.then(|| (common_prefix_len(configured, registry_uid), h.name(), h))
		})
		.max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
		.map(|(_, _, handle)| handle)
}

/// Length of the shared prefix of two strings, in bytes.
fn common_prefix_len(a: &str, b: &str) -> usize {
	a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::camera::CameraHandle;
	use crate::config::test_helpers::minimal_camera_config;
	use std::sync::Arc;
	use tokio_util::sync::CancellationToken;

	fn handle_with_uid(name: &str, uid: Option<&str>) -> Arc<CameraHandle> {
		let mut cfg = minimal_camera_config(name);
		cfg.uid = uid.map(str::to_string);
		Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None))
	}

	#[test]
	fn a_discarded_push_is_reported_once_per_source_per_window() {
		let mut misses = Misses::default();
		let a: IpAddr = "192.0.2.10".parse().unwrap();
		let now = Instant::now();
		assert!(misses.report(a, now), "the first discard has to be said out loud");
		assert!(
			!misses.report(a, now + Duration::from_millis(1)),
			"a retrying camera must not write a line per connection"
		);
		assert!(
			!misses.report(a, now + Misses::WINDOW - Duration::from_secs(1)),
			"still inside the window"
		);
		assert!(
			misses.report(a, now + Misses::WINDOW),
			"an outage that is still going has to keep showing up"
		);
	}

	#[test]
	fn each_source_gets_its_own_report() {
		let mut misses = Misses::default();
		let now = Instant::now();
		let a: IpAddr = "192.0.2.10".parse().unwrap();
		let b: IpAddr = "192.0.2.11".parse().unwrap();
		assert!(misses.report(a, now));
		assert!(misses.report(b, now), "a second camera is not silenced by the first");
		assert!(!misses.report(a, now + Duration::from_secs(1)));
	}

	#[test]
	fn the_report_map_stays_bounded() {
		// A peer spraying source addresses must not be able to grow this map.
		let mut misses = Misses::default();
		let now = Instant::now();
		for i in 0..(Misses::HOSTS as u32 + 40) {
			let ip: IpAddr = format!("192.0.2.{}", i % 251).parse().unwrap();
			misses.report(ip, now + Duration::from_secs(i as u64));
		}
		assert!(misses.last.len() <= Misses::HOSTS, "{} entries", misses.last.len());
	}

	#[test]
	fn a_short_registry_uid_matches_a_long_configured_uid() {
		// The form the camera actually writes (16 characters) against the form an
		// operator's file carries (the long one). While the rule was one-directional
		// this matched nothing, and the push was discarded as unowned.
		let mut cameras = HashMap::new();
		cameras.insert("front".into(), handle_with_uid("front", Some("9527000FRONT0123-FW")));
		let h = match_camera_by_uid(&cameras, "9527000FRONT0123")
			.expect("the short wire form matches the long configuration");
		assert_eq!(h.name(), "front");
	}

	#[test]
	fn two_cameras_sharing_a_uid_root_do_not_cross() {
		let mut cameras = HashMap::new();
		cameras.insert("short".into(), handle_with_uid("short", Some("9527000A")));
		cameras.insert("long".into(), handle_with_uid("long", Some("9527000ABCDEFG")));
		let h = match_camera_by_uid(&cameras, "9527000ABCDEFG").expect("the longest overlap wins");
		assert_eq!(h.name(), "long");
		let h = match_camera_by_uid(&cameras, "9527000A").expect("the exact one still wins");
		assert_eq!(h.name(), "short");
	}

	#[test]
	fn match_by_uid_exact() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert("front".into(), handle_with_uid("front", Some("UIDA")));
		cameras.insert("back".into(), handle_with_uid("back", Some("UIDB")));
		let h = match_camera_by_uid(&cameras, "UIDA").expect("match");
		assert_eq!(h.name(), "front");
	}

	#[test]
	fn match_by_uid_long_form_prefix() {
		// Argus heartbeats long-form (config UID + firmware suffix);
		// configured UID is the short form. Same pattern as
		// `CameraRegistry::lookup_fresh`'s prefix fallback.
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert(
			"front".into(),
			handle_with_uid("front", Some("9527000FRONT")),
		);
		let h = match_camera_by_uid(&cameras, "9527000FRONT0123").expect("match");
		assert_eq!(h.name(), "front");
	}

	#[test]
	fn match_by_uid_returns_none_when_no_camera_configured_uid() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert("front".into(), handle_with_uid("front", None));
		assert!(match_camera_by_uid(&cameras, "UIDA").is_none());
	}

	#[test]
	fn match_by_uid_prefers_longest_prefix_match() {
		// Registry returns `UID-AB-FW1234`. Two configured cameras
		// share the prefix root (`UID-A` and `UID-AB`). The longest
		// match — `UID-AB` — must win, never the HashMap-iteration
		// accident. Repeat with both insertion orders to guard
		// against any incidental ordering.
		for (first, second) in [("short", "long"), ("long", "short")] {
			let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
			let short = handle_with_uid("short", Some("UID-A"));
			let long = handle_with_uid("long", Some("UID-AB"));
			cameras.insert(
				first.into(),
				if first == "short" {
					Arc::clone(&short)
				} else {
					Arc::clone(&long)
				},
			);
			cameras.insert(
				second.into(),
				if second == "short" {
					Arc::clone(&short)
				} else {
					Arc::clone(&long)
				},
			);
			let h = match_camera_by_uid(&cameras, "UID-AB-FW1234").expect("match");
			assert_eq!(
				h.name(),
				"long",
				"longest match must win regardless of insertion order"
			);
		}
	}

	#[test]
	fn match_by_uid_returns_none_on_no_match() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert("front".into(), handle_with_uid("front", Some("UIDA")));
		assert!(match_camera_by_uid(&cameras, "UIDX").is_none());
	}

	#[tokio::test]
	async fn run_with_listener_returns_on_cancel() {
		// Pre-bind on an ephemeral port and pass to run_with_listener —
		// the same shape `main.rs` uses to surface bind errors at startup.
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let registry = bairelay_wake_server::make_registry();
		let cameras: Arc<HashMap<String, Arc<CameraHandle>>> = Arc::new(HashMap::new());
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(10),
			stale_after: Duration::from_secs(80),
		};
		let cancel_clone = cancel.clone();
		let task = tokio::spawn(async move {
			run_with_listener(
				listener,
				cfg,
				registry,
				cameras,
				None,
				"bairelay".into(),
				cancel_clone,
			)
			.await
		});
		tokio::time::sleep(Duration::from_millis(20)).await;
		cancel.cancel();
		let res = tokio::time::timeout(Duration::from_secs(2), task)
			.await
			.expect("task joined within deadline")
			.expect("task panic-free");
		assert!(res.is_ok(), "graceful cancel must return Ok, got {res:?}");
	}

	#[tokio::test]
	async fn run_returns_on_cancel() {
		let registry = bairelay_wake_server::make_registry();
		let cameras: Arc<HashMap<String, Arc<CameraHandle>>> = Arc::new(HashMap::new());
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: 0, // ephemeral
			motion_wake_hold: Duration::from_millis(10),
			stale_after: Duration::from_secs(80),
		};
		let cancel_clone = cancel.clone();
		let task = tokio::spawn(async move {
			run(
				cfg,
				registry,
				cameras,
				None,
				"bairelay".into(),
				cancel_clone,
			)
			.await
		});
		// Give the listener a moment to bind, then cancel.
		tokio::time::sleep(Duration::from_millis(20)).await;
		cancel.cancel();
		let res = tokio::time::timeout(Duration::from_secs(2), task)
			.await
			.expect("task joined within deadline")
			.expect("task panic-free");
		assert!(res.is_ok(), "graceful cancel must return Ok, got {res:?}");
	}

	#[tokio::test]
	async fn bind_error_surfaces_addr_and_source() {
		// Bind 127.0.0.1:0 once to grab a port, then try to bind it
		// again from the listener — expect a Bind error with the
		// occupied port surfaced.
		let squatter = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = squatter.local_addr().unwrap().port();
		let registry = bairelay_wake_server::make_registry();
		let cameras: Arc<HashMap<String, Arc<CameraHandle>>> = Arc::new(HashMap::new());
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(10),
			stale_after: Duration::from_secs(80),
		};
		let err = run(cfg, registry, cameras, None, "bairelay".into(), cancel)
			.await
			.expect_err("port is taken; bind must fail");
		match err {
			PushListenerError::Bind { addr, .. } => assert_eq!(addr.port(), port),
			other => panic!("expected Bind, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn end_to_end_tcp_connect_no_mqtt_takes_wake_only_path() {
		// MQTT-disabled deployments still drive the wake-lock path —
		// that is the wake-lock-only path of `motion_edge`. Connect from a
		// matching peer IP, observe the wake-lock count rises, then
		// the hold expires.
		let registry = bairelay_wake_server::make_registry();
		registry.upsert(
			"WAKEUID00000",
			"127.0.0.1:55003".parse().unwrap(),
			0xCAFE_BABE,
			Instant::now(),
		);
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let handle = handle_with_uid("nomqtt", Some("WAKEUID0"));
		cameras.insert("nomqtt".into(), Arc::clone(&handle));
		let cameras = Arc::new(cameras);

		let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = probe.local_addr().unwrap().port();
		drop(probe);

		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(120),
			stale_after: Duration::from_secs(80),
		};
		let cancel_for_run = cancel.clone();
		let server = tokio::spawn(async move {
			run(
				cfg,
				registry,
				cameras,
				None, // no MQTT — drives the wake-lock-only path
				"bairelay".into(),
				cancel_for_run,
			)
			.await
		});

		tokio::time::sleep(Duration::from_millis(30)).await;
		let _conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.expect("connect");

		// Wake lock should rise within the hold window.
		for _ in 0..50 {
			if handle.wake_lock().count() >= 1 {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		assert!(
			handle.wake_lock().count() >= 1,
			"the wake-lock-only path must acquire a wake-lock"
		);

		// Wait through hold, then the lock falls back to 0.
		tokio::time::sleep(Duration::from_millis(200)).await;
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	#[tokio::test]
	async fn end_to_end_tcp_connect_unrelated_uid_does_not_fire_wrong_camera() {
		// Defence-in-depth around the long-form/short-form prefix
		// match. Registry maps IP→UID-ALPHA; the only configured
		// camera has UID-BRAVO. A push from that IP must NOT fire
		// motion on the BRAVO camera (different UID), and there is
		// no ALPHA camera to fire either, so the listener's
		// observable behaviour is silent — no MQTT publish, no wake
		// lock acquired.
		let registry = bairelay_wake_server::make_registry();
		registry.upsert(
			"ALPHAUID0123",
			"127.0.0.1:55001".parse().unwrap(),
			0xDEAD_BEEF,
			Instant::now(),
		);

		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let bravo = handle_with_uid("bravo_cam", Some("BRAVOUID"));
		cameras.insert("bravo_cam".into(), Arc::clone(&bravo));
		let cameras = Arc::new(cameras);

		let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = probe.local_addr().unwrap().port();
		drop(probe);

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(150),
			stale_after: Duration::from_secs(80),
		};
		let cancel_for_run = cancel.clone();
		let registry_for_run = Arc::clone(&registry);
		let cameras_for_run = Arc::clone(&cameras);
		let server = tokio::spawn(async move {
			run(
				cfg,
				registry_for_run,
				cameras_for_run,
				Some(mqtt),
				"bairelay".into(),
				cancel_for_run,
			)
			.await
		});

		// Wait for bind, then connect — same shape as the happy path.
		tokio::time::sleep(Duration::from_millis(30)).await;
		let _conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.expect("connect");

		// Give the listener a generous window to (mistakenly) publish.
		tokio::time::sleep(Duration::from_millis(200)).await;
		let bravo_publish = mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/bravo_cam/status/motion");
		assert!(
			!bravo_publish,
			"BRAVO camera must not receive motion when registry UID is ALPHA"
		);
		assert_eq!(
			bravo.wake_lock().count(),
			0,
			"BRAVO wake-lock must stay at 0"
		);

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	#[tokio::test]
	async fn end_to_end_tcp_connect_drives_wake_lock_and_motion_publish() {
		// Stand up the listener on an ephemeral port. Pre-populate the
		// registry with a UID-IP mapping that matches our connection's
		// peer IP (always 127.0.0.1). Configure one camera with the
		// short UID so the prefix-match resolves. Connect once;
		// expect status/motion=on within ~1 s and a wake-lock
		// acquire on that handle.
		let registry = bairelay_wake_server::make_registry();
		registry.upsert(
			"FAKEUID01234",
			"127.0.0.1:55000".parse().unwrap(),
			0xDEAD_BEEF,
			Instant::now(),
		);

		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let handle = handle_with_uid("front", Some("FAKEUID0"));
		cameras.insert("front".into(), Arc::clone(&handle));
		let cameras = Arc::new(cameras);

		// Pre-bind to learn a free port, then close so the listener can claim it.
		let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = probe.local_addr().unwrap().port();
		drop(probe);

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(150),
			stale_after: Duration::from_secs(80),
		};
		let cancel_for_run = cancel.clone();
		let registry_for_run = Arc::clone(&registry);
		let cameras_for_run = Arc::clone(&cameras);
		let server = tokio::spawn(async move {
			run(
				cfg,
				registry_for_run,
				cameras_for_run,
				Some(mqtt),
				"bairelay".into(),
				cancel_for_run,
			)
			.await
		});

		// Wait for bind, then connect (and immediately close like the
		// camera does after our cert is rejected).
		tokio::time::sleep(Duration::from_millis(30)).await;
		let _conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.expect("connect");

		// Allow the spawned motion-edge task to run publish_motion(true).
		let saw_on = poll_published(&mock, Duration::from_secs(2), |t, p| {
			t == "bairelay/front/status/motion" && p == b"on"
		})
		.await;
		assert!(saw_on, "expected status/motion=on after connect");

		// Wake-lock count should be 1 while the hold is in effect.
		assert!(handle.wake_lock().count() >= 1);

		// Wait through the hold, expect the fallback off publish.
		let saw_off = poll_published(&mock, Duration::from_millis(500), |t, p| {
			t == "bairelay/front/status/motion" && p == b"off"
		})
		.await;
		assert!(saw_off, "expected fallback status/motion=off after hold");

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	#[tokio::test]
	async fn later_push_owns_the_fallback_clear() {
		// Push A at t=0 and B at t≈400 ms, hold 600 ms. A's window ends
		// first but B's is still running, so only B may clear — at
		// t≈1000 ms, not 600 ms.
		let registry = bairelay_wake_server::make_registry();
		registry.upsert(
			"YARDUID01234",
			"127.0.0.1:55004".parse().unwrap(),
			0xFEED_FACE,
			Instant::now(),
		);
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let handle = handle_with_uid("yard", Some("YARDUID0"));
		cameras.insert("yard".into(), Arc::clone(&handle));
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(600),
			stale_after: Duration::from_secs(80),
		};
		let server = tokio::spawn(run_with_listener(
			listener,
			cfg,
			registry,
			Arc::new(cameras),
			Some(mqtt),
			"bairelay".into(),
			cancel.clone(),
		));

		let motion_payloads = |mock: &bairelay_mqtt::test_support::MockHandle| -> Vec<Vec<u8>> {
			mock.published()
				.into_iter()
				.filter(|(t, _, _)| t == "bairelay/yard/status/motion")
				.map(|(_, p, _)| p)
				.collect()
		};

		let _a = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(400)).await;
		let _b = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.unwrap();

		// t≈800 ms: past A's window, inside B's, ~200 ms of slack each way.
		tokio::time::sleep(Duration::from_millis(400)).await;
		assert_eq!(
			motion_payloads(&mock),
			[b"on".to_vec(), b"on".to_vec()],
			"A's expiry must not clear while B is live"
		);
		assert_eq!(handle.status_cache().motion(), Some(true));

		let cleared = tokio::time::timeout(Duration::from_secs(2), async {
			while motion_payloads(&mock).len() < 3 {
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await;
		assert!(cleared.is_ok(), "B's expiry clears motion");
		assert_eq!(
			motion_payloads(&mock),
			[b"on".to_vec(), b"on".to_vec(), b"off".to_vec()]
		);
		assert_eq!(handle.status_cache().motion(), Some(false));

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	#[tokio::test]
	async fn a_push_inside_a_webhook_wake_is_absorbed() {
		let registry = bairelay_wake_server::make_registry();
		registry.upsert(
			"GATEUID01234",
			"127.0.0.1:55005".parse().unwrap(),
			0xC0DE_CAFE,
			Instant::now(),
		);
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let handle = handle_with_uid("gate", Some("GATEUID0"));
		cameras.insert("gate".into(), Arc::clone(&handle));
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_secs(5),
			stale_after: Duration::from_secs(80),
		};
		let server = tokio::spawn(run_with_listener(
			listener,
			cfg,
			registry,
			Arc::new(cameras),
			Some(mqtt),
			"bairelay".into(),
			cancel.clone(),
		));

		// The webhook reported this wake first.
		let logs = crate::test_logs::capture_at(tracing::Level::DEBUG);
		assert!(crate::wake_events::claim_wake(
			&handle,
			crate::status_cache::WakeSource::Webhook
		));
		let _conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.unwrap();
		// The listener reached the camera and dropped the edge.
		let absorbed = tokio::time::timeout(Duration::from_secs(2), async {
			while !logs
				.text()
				.contains("Wake already reported by the other source")
			{
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await;
		assert!(absorbed.is_ok(), "{}", logs.text());
		// An absorbed edge spawns nothing, so its outcome is final here.
		assert!(mock.published().is_empty(), "{:?}", mock.published_topics());
		assert_eq!(handle.wake_lock().count(), 0);
		assert!(!logs.text().contains("Motion push from camera"));

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	async fn poll_published(
		mock: &bairelay_mqtt::test_support::MockHandle,
		deadline: Duration,
		mut pred: impl FnMut(&str, &[u8]) -> bool,
	) -> bool {
		let start = Instant::now();
		while start.elapsed() < deadline {
			if mock.published().iter().any(|(t, p, _)| pred(t, p)) {
				return true;
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		false
	}
}
