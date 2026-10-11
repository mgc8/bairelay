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
//! known camera's IP as a motion event, fire `status/motion=on`
//! plus a `motion_wake_hold_secs` wake-lock, and close the socket.
//!
//! A camera is known by its source IP: first through the wake server's
//! heartbeat registry, then through a camera's configured IP `address`
//! — the latter lets the listener run without the wake server, leaving
//! `p2p*.reolink.com` (and so the Reolink app) untouched.
//!
//! The wake-lock acquire kicks the connect loop, which uses the local
//! BcUdp wake server to bring the camera online. Once the
//! Baichuan session is up, the existing `motion_listener` subscribes
//! and publishes the eventual `motion=off` from the camera itself.
//! We publish a fallback `motion=off` after `motion_wake_hold_secs` so
//! HA never gets stuck on "motion=on" if reconnect failed.
//!
//! The push itself is class-blind: person / vehicle / animal / other
//! lives in the TLS body we can't read. The per-class
//! `status/motion/{class}` topics are only published once the
//! reconnected session's `motion_listener` sees the camera's
//! `AlarmEvent`; the fallback clears them alongside `motion`.
//!
//! **Threat model — source-IP trust.** A peer IP that resolves through
//! the wake-server registry or a configured address is treated as
//! authentic; we have no further
//! authentication on the TCP-accept itself (the camera's TLS handshake
//! never completes — the cert chain is pinned to Reolink's CA). On a
//! flat home LAN, anyone able to spoof the camera's IP can fire a fake
//! motion edge, which at worst wakes the camera for `motion_wake_hold`
//! and publishes `status/motion=on` — bounded blast radius. Acceptable
//! for the single-operator deployment target; explicitly out of scope
//! for any future multi-tenant use.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use bairelay_mqtt::{SharedMqttClient, StatusPublisher};
use bairelay_neolink_core::bc_protocol::AiDetections;
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
						// An IPv4 camera reaching a dual-stack bind shows
						// up as `::ffff:a.b.c.d`; match on the plain form.
						handle_push(peer.ip().to_canonical(), &registry, &cameras, mqtt.as_ref(), &topic_prefix, &cfg, &cancel);
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
) {
	let by_registry = registry
		.lookup_by_ip(peer_ip, Instant::now(), cfg.stale_after)
		.and_then(|(registry_uid, _entry)| {
			let handle = match_camera_by_uid(cameras, &registry_uid);
			if handle.is_none() {
				tracing::debug!(
					peer_ip = %peer_ip,
					registry_uid = %registry_uid,
					"push matched registry UID but no camera handle has a matching configured UID"
				);
			}
			handle
		});
	let Some(handle) = by_registry.or_else(|| match_camera_by_address(cameras, peer_ip)) else {
		tracing::debug!(peer_ip = %peer_ip, "push from unknown IP — no registry or address match");
		return;
	};

	tracing::info!(
		camera = %handle.name(),
		peer_ip = %peer_ip,
		"Motion push from camera (treating TCP-accept as motion edge)"
	);

	let handle = Arc::clone(handle);
	let seq = handle.push_seq().fetch_add(1, Ordering::SeqCst) + 1;
	let Some(mqtt_client) = mqtt.cloned() else {
		// MQTT disabled in config — wake-lock alone still drives the
		// connect loop, so observability suffers but the wake-on-motion
		// behaviour is intact.
		spawn_wake_only(handle, cfg.motion_wake_hold, cancel.clone());
		return;
	};

	let prefix = topic_prefix.to_string();
	let hold = cfg.motion_wake_hold;
	let cancel = cancel.clone();
	tokio::spawn(async move {
		fire_motion(handle, mqtt_client, prefix, hold, seq, cancel).await;
	});
}

/// Resolve a `(name → CameraHandle)` map entry from a registry-side UID.
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
		.filter(|h| {
			h.config()
				.uid
				.as_deref()
				.is_some_and(|cfg_uid| registry_uid.starts_with(cfg_uid))
		})
		.max_by_key(|h| h.config().uid.as_deref().map_or(0, str::len))
}

/// Resolve the camera whose configured `address` is `peer_ip`. Needs a
/// literal IP (and, in practice, a DHCP reservation). Two cameras on
/// one IP are ambiguous, so neither is matched.
pub(crate) fn match_camera_by_address(
	cameras: &HashMap<String, Arc<CameraHandle>>,
	peer_ip: IpAddr,
) -> Option<&Arc<CameraHandle>> {
	let mut matches = cameras.values().filter(|h| {
		h.config()
			.address
			.as_deref()
			.and_then(|a| crate::bc_opts::parse_address(a).0)
			.map(|ip| ip.to_canonical())
			== Some(peer_ip)
	});
	let first = matches.next()?;
	matches.next().is_none().then_some(first)
}

/// Fire the motion event: publish `status/motion=on`, hold a wake-lock
/// for `hold`, then publish a fallback `status/motion=off` (and every
/// per-class topic off) so HA never gets stuck on `on` if the
/// in-session `motion_listener` never picks up the live `Stop`.
/// Idempotent w.r.t. the in-session publisher — duplicate `off` is
/// harmless (HA dedups). `seq` is this push's
/// [`CameraHandle::push_seq`]; a later push takes over the clear.
async fn fire_motion(
	handle: Arc<CameraHandle>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	hold: Duration,
	seq: u64,
	cancel: CancellationToken,
) {
	let _guard = handle.wake_lock().acquire();
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, handle.name());
	if let Err(e) = publisher.publish_motion(true).await {
		tracing::warn!(camera = %handle.name(), error = %e, "push motion: publish_motion(true) failed");
	}
	handle.status_cache().set_motion(true);

	// Hold for `hold`, bailing on cancel — same primitive used by the
	// camera reconnect path so the contract lives in one place.
	crate::run_support::sleep_or_cancel(hold, &cancel).await;

	if handle.push_seq().load(Ordering::SeqCst) != seq {
		tracing::debug!(camera = %handle.name(), "push motion: a later push owns the fallback clear");
		return;
	}

	// Cap the fallback publish so a wedged broker (Ctrl+C race against
	// detached fire_motion tasks) can't hold the runtime open during
	// shutdown. 1 s is generous for an alive broker; on shutdown, a
	// dead broker hits the timeout and we move on quietly.
	const FALLBACK_PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);
	let none = AiDetections::default();
	let clear = async {
		publisher.publish_motion(false).await?;
		crate::camera_tasks::publish_motion_classes(&publisher, none).await
	};
	match tokio::time::timeout(FALLBACK_PUBLISH_TIMEOUT, clear).await {
		Ok(Ok(())) => {
			handle.status_cache().set_motion(false);
			handle.status_cache().set_motion_classes(none);
		}
		Ok(Err(e)) => {
			tracing::warn!(camera = %handle.name(), error = %e, "push motion: fallback motion=off publish failed");
		}
		Err(_) => {
			tracing::debug!(
				camera = %handle.name(),
				"push motion: fallback publish_motion(false) timed out (likely shutdown)"
			);
		}
	}
}

/// Wake-lock-only path used when no MQTT client is configured. Holds
/// the lock for the same window so the connect loop has time to land.
fn spawn_wake_only(handle: Arc<CameraHandle>, hold: Duration, cancel: CancellationToken) {
	tokio::spawn(async move {
		let _guard = handle.wake_lock().acquire();
		crate::run_support::sleep_or_cancel(hold, &cancel).await;
	});
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

	fn handle_with_address(name: &str, address: Option<&str>) -> Arc<CameraHandle> {
		let mut cfg = minimal_camera_config(name);
		cfg.address = address.map(str::to_string);
		Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None))
	}

	#[test]
	fn match_by_address_with_and_without_port() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert(
			"front".into(),
			handle_with_address("front", Some("192.168.1.20:9000")),
		);
		cameras.insert(
			"back".into(),
			handle_with_address("back", Some("192.168.1.21")),
		);
		cameras.insert(
			"named".into(),
			handle_with_address("named", Some("cam.local")),
		);
		let front = match_camera_by_address(&cameras, "192.168.1.20".parse().unwrap());
		assert_eq!(front.expect("match").name(), "front");
		let back = match_camera_by_address(&cameras, "192.168.1.21".parse().unwrap());
		assert_eq!(back.expect("match").name(), "back");
		assert!(match_camera_by_address(&cameras, "192.168.1.22".parse().unwrap()).is_none());
	}

	#[test]
	fn match_by_address_refuses_ambiguous_ip() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert("a".into(), handle_with_address("a", Some("192.168.1.20")));
		cameras.insert(
			"b".into(),
			handle_with_address("b", Some("192.168.1.20:9000")),
		);
		assert!(match_camera_by_address(&cameras, "192.168.1.20".parse().unwrap()).is_none());
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
	async fn end_to_end_tcp_connect_matches_by_address_without_registry() {
		// Wake server off: the registry stays empty and the camera is
		// found by its configured IP instead.
		let registry = bairelay_wake_server::make_registry();
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		let handle = handle_with_address("porch", Some("127.0.0.1:9000"));
		cameras.insert("porch".into(), Arc::clone(&handle));
		let cameras = Arc::new(cameras);

		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let cfg = RuntimeConfig {
			bind_addr: "127.0.0.1".parse().unwrap(),
			bind_port: port,
			motion_wake_hold: Duration::from_millis(150),
			stale_after: Duration::from_secs(80),
		};
		let server = tokio::spawn(run_with_listener(
			listener,
			cfg,
			registry,
			cameras,
			Some(mqtt),
			"bairelay".into(),
			cancel.clone(),
		));

		let _conn = tokio::net::TcpStream::connect(("127.0.0.1", port))
			.await
			.expect("connect");
		let saw_on = poll_published(&mock, Duration::from_secs(2), |t, p| {
			t == "bairelay/porch/status/motion" && p == b"on"
		})
		.await;
		assert!(saw_on, "address match must fire status/motion=on");
		assert!(handle.wake_lock().count() >= 1);

		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
	}

	#[tokio::test]
	async fn end_to_end_tcp_connect_no_mqtt_takes_wake_only_path() {
		// MQTT-disabled deployments still drive the wake-lock path —
		// that's the `spawn_wake_only` branch. Connect from a
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
				None, // no MQTT — drives spawn_wake_only
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
			"spawn_wake_only must acquire a wake-lock"
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

		// Allow the spawned fire_motion task to run publish_motion(true).
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

		// The push can't classify, so it never turns a class on —
		// but the fallback clears every class so a session that died
		// mid-alarm can't leave e.g. `person=on` behind. `other` is
		// the last class published, so wait on it.
		let saw_classes_off = poll_published(&mock, Duration::from_millis(500), |t, p| {
			t == "bairelay/front/status/motion/other" && p == b"off"
		})
		.await;
		assert!(saw_classes_off, "expected fallback to clear class topics");
		for class in ["person", "vehicle", "animal", "other"] {
			let topic = format!("bairelay/front/status/motion/{class}");
			let payloads: Vec<Vec<u8>> = mock
				.published()
				.into_iter()
				.filter(|(t, _, _)| *t == topic)
				.map(|(_, p, _)| p)
				.collect();
			assert_eq!(
				payloads,
				[b"off".to_vec()],
				"{topic} must only see the fallback off"
			);
		}
		assert_eq!(
			handle.status_cache().motion_classes(),
			Some(AiDetections::default())
		);

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
