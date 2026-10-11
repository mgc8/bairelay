use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;

use bairelay::bcmedia_dump::BcMediaDumpConfig;
use bairelay::camera::CameraHandle;
use bairelay::cli::{Cli, Command};
use bairelay::cli_convert::{should_emit_ansi, verbosity_env_filter};
use bairelay::config::{
	warn_deprecated_pause_fields, warn_idle_timeout_below_prune_floor, warn_neolink_compat_fields,
	warn_wire_debug_enabled,
};
use bairelay::local_time::LocalTimer;
use bairelay::mqtt_dispatch::dispatch_control;
use bairelay::mqtt_loop::{
	build_broker_config, build_rtsp_users, classify_event, handle_connack, publish_shutdown_fanout,
	resolve_topic_prefix, EventAction, MqttBackoff, SHUTDOWN_FANOUT_TIMEOUT,
};
use bairelay::orchestrator::Orchestrator;
use bairelay::run_support::{camera_names, load_validated_config};
use bairelay::watchdog::Watchdog;
use bairelay_mqtt::{parse_control_message, MqttEventLoop, SharedMqttClient};

/// Concurrent-RTSP-connection cap. Picked an order of magnitude above
/// realistic deployments (a handful of clients × a handful of cameras)
/// while still bounded enough that a fork-bomb client can't exhaust
/// file descriptors.
const DEFAULT_MAX_RTSP_CONNECTIONS: usize = 256;

fn main() -> Result<()> {
	// Capture the host's local UTC offset BEFORE tokio's worker pool
	// spawns — `time::UtcOffset::current_local_offset()` returns Err
	// once the process is multi-threaded. The offset feeds both the
	// log timestamp formatter and the `set-time` one-shot.
	bairelay::local_time::init();

	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()?
		.block_on(async_main())
}

async fn async_main() -> Result<()> {
	// Parse CLI arguments.
	let cli = Cli::parse();

	// Route logs to stderr so stdout stays free for --json / JPEG output
	// from one-shot subcommands. Disable ANSI colour escapes when stderr
	// is not a TTY (file redirect, pipe, supervisor capture) or when
	// `NO_COLOR` is set — keeps redirected logs free of escape garbage.
	let ansi = should_emit_ansi(
		std::io::IsTerminal::is_terminal(&std::io::stderr()),
		std::env::var_os("NO_COLOR").is_some(),
	);
	tracing_subscriber::fmt()
		.with_writer(std::io::stderr)
		.with_ansi(ansi)
		.with_timer(LocalTimer)
		.with_env_filter(verbosity_env_filter(cli.verbose))
		.init();

	// `render-hassio-config` is a pure-templating subcommand for the HA
	// add-on entrypoint; it must not flow through the camera-touching
	// one-shot pipeline. Dispatch it here and exit before any config
	// load / orchestrator bring-up.
	if let Command::RenderHassioConfig {
		options_json,
		overlay,
		mqtt_host,
		mqtt_port,
		mqtt_user,
		mqtt_pass,
		mqtt_ssl,
		output,
	} = &cli.command
	{
		if let Err(e) = bairelay::hassio::cmd::run(
			options_json,
			overlay.as_deref(),
			mqtt_host.clone(),
			*mqtt_port,
			mqtt_user.clone(),
			mqtt_pass.clone(),
			*mqtt_ssl,
			output.as_deref(),
		) {
			eprintln!("{e}");
			std::process::exit(1);
		}
		return Ok(());
	}

	// One-shot subcommands exit before any MQTT / RTSP / orchestrator
	// bring-up; service modes fall through to the existing pipeline.
	if cli.is_oneshot() {
		let code = bairelay::run_support::run_oneshot(&cli).await;
		std::process::exit(code);
	}

	info!(version = %env!("CARGO_PKG_VERSION"), "bairelay starting");

	// Load, parse, and validate the config file.
	let config_path = cli.config_path();
	let config = load_validated_config(config_path)?;

	// Emit one-line migration warnings for deprecated neolink
	// `[cameras.pause]` fields kept only for smooth upgrades.
	warn_deprecated_pause_fields(&config);
	// Same shape, broader scope: every neolink top-level / per-camera
	// field bairelay accepts but does not honour gets a single-line
	// pointer to the bairelay equivalent.
	warn_neolink_compat_fields(&config);
	// Surface any camera with wire-level protocol debugging live —
	// the trace dumps it unlocks contain credential hashes.
	warn_wire_debug_enabled(&config);
	// Surface any per-camera `idle_disconnect_timeout_secs` shorter
	// than the global `stream_prune_grace_secs`; the runtime clamps
	// silently, this warning makes the override visible.
	warn_idle_timeout_below_prune_floor(&config);

	info!(
		cameras = config.cameras.len(),
		config = %config_path.display(),
		"Loaded configuration"
	);

	// Set up cancellation token (shutdown-signal handler registered after MQTT is ready).
	let token = CancellationToken::new();

	// Capture the MQTT topic prefix (default "bairelay") — must be read
	// before `config` is consumed by the orchestrator below. The
	// fallback string is only used when MQTT is disabled entirely; in
	// that case nothing consumes the value.
	let topic_prefix = resolve_topic_prefix(&config);

	// Connect to MQTT broker if configured AND the CLI subcommand requests it.
	// Suffix the client_id with our PID so two bairelay processes pointed at
	// the same broker (e.g. live + test rig, or a leftover instance) don't
	// fight over a single id and drown the log in alternating Broken Pipe /
	// Connection Reset reconnect storms — MQTT brokers disconnect the prior
	// session whenever a duplicate id reconnects, and rumqttc auto-retries.
	// Install rustls's process-wide crypto provider before anything can
	// build a TLS config. Two providers are compiled in (aws-lc-rs, plus
	// ring via a dependency), so rustls can't pick one itself and panics
	// on first use — rumqttc's MQTT-over-TLS connect included.
	bairelay_rtsp::server::install_crypto_provider();
	// MQTT TLS material is loaded only when this mode runs MQTT: a broken
	// `[mqtt]` section mustn't stop `rtsp`-only mode (`check-config`
	// still validates it).
	let broker_config = if cli.wants_mqtt() {
		build_broker_config(&config).context("Invalid [mqtt] configuration")?
	} else {
		None
	};
	let mqtt_client = match (broker_config, cli.wants_mqtt()) {
		(Some(broker_config), true) => {
			let client_id = format!("bairelay-{}", std::process::id());
			let (client, event_loop) =
				bairelay_mqtt::connect(&broker_config, &client_id, &topic_prefix)
					.context("Failed to create MQTT client")?;
			info!(broker = %broker_config.broker_addr, port = broker_config.port, prefix = %topic_prefix, client_id = %client_id, "MQTT client created");
			Some((client, event_loop))
		}
		_ => None,
	};

	let (mqtt_shared, mqtt_event_loop) = match mqtt_client {
		Some((client, event_loop)) => (Some(client), Some(event_loop)),
		None => (None, None),
	};

	// Capture RTSP bind details + user list before the config is consumed
	// by the orchestrator (it takes the config by value).
	let rtsp_bind_addr = config.bind_addr.clone();
	let rtsp_bind_port = config.bind_port;
	let stream_prune_grace = Duration::from_secs(config.stream_prune_grace_secs);
	let rtsp_users = build_rtsp_users(&config);

	// Parse TLS PEMs at startup so any error surfaces here, not on first
	// connection (crypto provider installed above).
	let tls_loaded: Option<bairelay::tls_load::LoadedTls> =
		if let Some(ref cert_path) = config.certificate {
			Some(
				bairelay::tls_load::load_server_tls(
					cert_path,
					config.tls_client_auth,
					config.tls_client_ca.as_deref(),
				)
				.context("loading TLS config")?,
			)
		} else {
			None
		};
	let tls_bind_port = config
		.tls_bind_port
		.unwrap_or(bairelay::config::DEFAULT_TLS_BIND_PORT);

	// Build the optional BcMedia capture config if `--dump-bcmedia` was set.
	// `Arc` so every camera handle shares one `PathBuf` without duplicating.
	let bcmedia_dump = cli
		.dump_bcmedia_path()
		.map(|p| Arc::new(BcMediaDumpConfig::new(p.to_path_buf())));
	if let Some(ref cfg) = bcmedia_dump {
		info!(
			root = %cfg.root.display(),
			"BcMedia capture enabled (opt-in); packets will be mirrored for every active stream"
		);
	}

	// Build the optional HA MQTT discovery publisher. Only present
	// when the operator opted in via `[mqtt.discovery]` AND an MQTT
	// client is available (no broker → nothing to publish onto).
	let discovery_publisher = match (&mqtt_shared, config.mqtt.as_ref()) {
		(Some(client), Some(mqtt_cfg)) => mqtt_cfg.discovery.as_ref().map(|d| {
			let features = bairelay::config::effective_discovery_features(&config, d);
			info!(
				ha_topic = %d.topic,
				features = features.len(),
				"HA MQTT discovery enabled"
			);
			bairelay_mqtt::DiscoveryPublisher::new(
				client.clone(),
				topic_prefix.clone(),
				d.topic.clone(),
				features,
				env!("CARGO_PKG_VERSION").to_string(),
			)
		}),
		_ => None,
	};

	// Create orchestrator. After this point the per-camera name list
	// lives on the orchestrator; the shutdown handler captures a
	// clone of the cameras map so it can unpublish HA discovery
	// payloads on shutdown.
	let camera_names_list: Vec<String> = camera_names(&config);
	let orchestrator = Orchestrator::with_bcmedia_dump_and_discovery(
		config,
		token.clone(),
		mqtt_shared.clone(),
		bcmedia_dump,
		discovery_publisher,
	);
	info!("Managing {} camera(s)", orchestrator.camera_count());

	// Register the shutdown-signal handler (Ctrl+C; SIGTERM on Unix). Publishes "disconnected" for all cameras,
	// unpublishes HA discovery config (retained-empty), flushes, then
	// cancels the global token. MQTT is cancelled separately by main()
	// AFTER orchestrator.run() returns, so per-camera teardown paths
	// can still publish their own final state without racing a dead
	// event loop.
	//
	// Shutdown order on a shutdown signal:
	// 1. Publish "disconnected" availability per camera (HA sees the outage).
	// 2. Unpublish HA discovery config topics (retained empty → HA deletes entities).
	// 3. 200 ms flush pause so the event loop drains the retained writes.
	// 4. Global cancellation token fires:
	//    - RTSP server stops accepting new connections; existing connections see select! cancel and close.
	//    - Session send loops observe cancel, call transport.close(), drop SubscriptionHandle (releases wake lock).
	//    - Orchestrator's per-camera tasks observe cancel on their child tokens and tear down, publishing their own "disconnected" status along the way (MQTT is still alive).
	// 5. After orchestrator.run() returns, main() cancels mqtt_cancel
	//    and awaits the event-loop task with a 2 s guard. That's when
	//    MQTT actually stops.
	{
		let shutdown_token = token.clone();
		let shutdown_mqtt = mqtt_shared.clone();
		let shutdown_prefix = topic_prefix.clone();
		let shutdown_cameras = orchestrator.cameras_arc();
		let shutdown_requested = bairelay::shutdown_signal::requested();
		tokio::spawn(async move {
			let signal = shutdown_requested.await;
			info!(signal, "Shutdown requested, shutting down...");
			bairelay::shutdown_signal::force_exit_on_next(
				bairelay::oneshot::classify::EXIT_INTERRUPTED,
			);

			// 1. Publish "disconnected" while MQTT is still running.
			// 2. Unpublish HA discovery so HA clears the Bairelay
			//    device cards. Safe on cameras without a publisher
			//    attached (no-op) and on cameras whose caps are
			//    `None` (no-op — nothing was ever published for them).
			//
			// Both steps are wrapped in a 2-second hard timeout.
			// rumqttc's per-AsyncClient Request queue (256 slots) can
			// saturate if the broker was already flapping when the signal
			// fired; without this guard, `publish().await` can block
			// indefinitely waiting for queue space, stalling the
			// whole shutdown. On timeout we log and fall through to
			// cancel — the broker will re-sync retained state when
			// the next bairelay restarts.
			if let Some(ref mqtt) = shutdown_mqtt {
				let result = tokio::time::timeout(
					SHUTDOWN_FANOUT_TIMEOUT,
					publish_shutdown_fanout(
						&camera_names_list,
						&shutdown_cameras,
						mqtt,
						&shutdown_prefix,
					),
				)
				.await;
				if result.is_err() {
					tracing::warn!(
						"MQTT shutdown publishes timed out after 2s; broker may be unreachable — proceeding with cancel"
					);
				}

				// 3. Brief flush so whatever landed can drain to the
				//    broker before the MQTT task is cancelled. Kept
				//    short because a broker outage was already handled
				//    by the timeout above.
				tokio::time::sleep(Duration::from_millis(200)).await;
			}

			// 4. Cancel everyone else.
			shutdown_token.cancel();
		});
	}

	// Independent cancel for the MQTT event loop. The global `token`
	// fires every other subsystem (RTSP, watchdog, per-camera run
	// loops); `mqtt_cancel` is cancelled only AFTER orchestrator.run()
	// returns (i.e. all cameras have finished teardown — which
	// includes their own final "disconnected" publishes). That makes
	// MQTT genuinely the last subsystem to shut down, as the design
	// doc promises, instead of racing camera teardown on the same
	// cancel edge.
	let mqtt_cancel = CancellationToken::new();

	// Spawn MQTT event loop with control dispatch.
	let mqtt_task = if let Some(event_loop) = mqtt_event_loop {
		let cameras_for_mqtt = orchestrator.cameras_arc();
		let mqtt_for_dispatch = mqtt_shared
			.clone()
			.expect("mqtt_shared must exist when event_loop exists");
		let cancel = mqtt_cancel.clone();
		let prefix = topic_prefix.clone();
		Some(tokio::spawn(async move {
			run_mqtt_event_loop(
				event_loop,
				cameras_for_mqtt,
				mqtt_for_dispatch,
				prefix,
				cancel,
			)
			.await;
		}))
	} else {
		None
	};

	// Centralised supervisor for every long-running service that
	// shares the global cancel token (watchdog, startup-wake, RTSP
	// listeners, wake server, push listener). MQTT lives outside the
	// supervisor: its event loop has a distinct cancel token because
	// per-camera teardown publishes its final `disconnected` status
	// via MQTT, so MQTT must outlive the orchestrator.
	let mut sup = bairelay::supervisor::Supervisor::new(token.clone());

	// Start watchdog (30s interval).
	let watchdog = Watchdog::new(Duration::from_secs(30), stream_prune_grace, token.clone());
	let cameras_ref = orchestrator.cameras_arc();
	sup.spawn("watchdog", move |_cancel| async move {
		watchdog.run(cameras_ref).await;
	});

	// Spawn the startup wake cycle (non-blocking) so each camera's
	// last-frame buffer is populated before the first RTSP DESCRIBE lands
	// and its capability cache is probed before HA discovery publishes.
	// The global cancel token is forwarded so a shutdown during the warm
	// cycle doesn't stall up to PER_CAMERA_TIMEOUT per camera.
	{
		let cameras = orchestrator.cameras_arc();
		sup.spawn("startup_wake", move |cancel| async move {
			bairelay::startup_wake::warm_last_frame_buffers(&cameras, cancel).await;
		});
	}

	// Spawn RTSP server(s) if requested. Plain and TLS run as
	// independent listeners on separate ports. Plain skips when
	// bind_port = 0 (operator opted into TLS-only). TLS skips when no
	// certificate is configured.
	//
	// Each socket is bound synchronously here so a port-conflict or
	// permission failure halts startup with a clear error before any
	// "started" log line. Without pre-bind, the operator sees a
	// successful-startup log even though the listener never bound.
	if cli.wants_rtsp() {
		let provider: Arc<dyn bairelay_rtsp::provider::StreamProvider> = Arc::new(
			bairelay::camera_provider::CameraProvider::new(orchestrator.cameras_arc()),
		);

		if rtsp_bind_port > 0 {
			let bind = format!("{}:{}", rtsp_bind_addr, rtsp_bind_port)
				.parse::<std::net::SocketAddr>()
				.with_context(|| {
					format!("Invalid bind address {}:{}", rtsp_bind_addr, rtsp_bind_port)
				})?;
			let listener = tokio::net::TcpListener::bind(bind).await.with_context(|| {
				format!("RTSP bind failed on {}:{}", rtsp_bind_addr, rtsp_bind_port)
			})?;
			let server_config = bairelay_rtsp::server::ServerConfig {
				bind,
				realm: "bairelay".to_string(),
				users: rtsp_users.clone(),
				tls: None,
				max_connections: Some(DEFAULT_MAX_RTSP_CONNECTIONS),
			};
			let provider_plain = Arc::clone(&provider);
			sup.spawn("rtsp", move |cancel| async move {
				if let Err(e) = bairelay_rtsp::server::RtspServer::serve_with_listener(
					listener,
					server_config,
					provider_plain,
					cancel,
				)
				.await
				{
					tracing::error!(error = %e, "RTSP server exited");
				}
			});
			info!(
				"RTSP server started on {}:{}",
				rtsp_bind_addr, rtsp_bind_port
			);
		}

		if let Some(loaded) = tls_loaded {
			let tls_bind = format!("{}:{}", rtsp_bind_addr, tls_bind_port)
				.parse::<std::net::SocketAddr>()
				.with_context(|| {
					format!(
						"Invalid TLS bind address {}:{}",
						rtsp_bind_addr, tls_bind_port
					)
				})?;
			let listener = tokio::net::TcpListener::bind(tls_bind)
				.await
				.with_context(|| {
					format!("RTSPS bind failed on {}:{}", rtsp_bind_addr, tls_bind_port)
				})?;
			let server_config = bairelay_rtsp::server::ServerConfig {
				bind: tls_bind,
				realm: "bairelay".to_string(),
				users: rtsp_users,
				tls: Some(loaded.tls_config),
				max_connections: Some(DEFAULT_MAX_RTSP_CONNECTIONS),
			};
			let provider_tls = Arc::clone(&provider);
			sup.spawn("rtsps", move |cancel| async move {
				if let Err(e) = bairelay_rtsp::server::RtspServer::serve_with_listener(
					listener,
					server_config,
					provider_tls,
					cancel,
				)
				.await
				{
					tracing::error!(error = %e, "RTSP TLS server exited");
				}
			});
			info!(
				"RTSP TLS (rtsps://) server started on {}:{}",
				rtsp_bind_addr, tls_bind_port
			);
		}
	}

	// Spawn the local wake server when [wake_server] enable=true.
	// The bind IP is inherited from the top-level bind_addr. Failures to
	// bind here are fatal at startup, like TLS misconfiguration is.
	//
	// When [push_listener] enable=true is also set, the same
	// `Arc<CameraRegistry>` is shared with the push-listener so a TCP
	// connect from a registered camera's IP fires a motion event.
	//
	// Both subsystems pre-bind synchronously below, so a UDP / TCP port
	// conflict halts startup before either subsystem logs "started".
	{
		let registry = bairelay_wake_server::make_registry();

		if let Some(ws_block) = orchestrator
			.wake_server_config()
			.cloned()
			.filter(|w| w.enable)
		{
			let bind_ip: std::net::IpAddr = rtsp_bind_addr
				.parse()
				.with_context(|| format!("Invalid wake-server bind IP {}", rtsp_bind_addr))?;
			let runtime =
				bairelay_wake_server::config::RuntimeConfig::from_block(&ws_block, bind_ip)
					.map_err(|e| anyhow::anyhow!("[wake_server] {e}"))?;
			let middleman_addr = std::net::SocketAddr::new(runtime.bind, runtime.middleman_port);
			let register_addr = std::net::SocketAddr::new(runtime.bind, runtime.register_port);
			let middleman_sock = tokio::net::UdpSocket::bind(middleman_addr)
				.await
				.with_context(|| format!("wake server bind failed on {}", middleman_addr))?;
			let register_sock = tokio::net::UdpSocket::bind(register_addr)
				.await
				.with_context(|| format!("wake server bind failed on {}", register_addr))?;
			let middleman_port = runtime.middleman_port;
			let register_port = runtime.register_port;
			let registry_for_server = registry.clone();
			sup.spawn("wake_server", move |cancel| async move {
				if let Err(e) = bairelay_wake_server::run_with_sockets(
					runtime,
					registry_for_server,
					middleman_sock,
					register_sock,
					cancel,
				)
				.await
				{
					tracing::error!(error = %e, "wake server exited with error");
				}
			});
			info!(
				bind = %rtsp_bind_addr,
				middleman_port,
				register_port,
				"Wake server started"
			);
		}

		if let Some(pl_block) = orchestrator
			.push_listener_config()
			.cloned()
			.filter(|p| p.enable)
		{
			// Resolve the bind chain: explicit > wake-server bind > top-level
			// bind. Fall through to the top-level so a push-listener-only
			// deployment still has a valid address.
			let bind_str = pl_block
				.push_listener_addr
				.clone()
				.unwrap_or_else(|| rtsp_bind_addr.to_string());
			let bind_addr: std::net::IpAddr = bind_str
				.parse()
				.with_context(|| format!("Invalid push_listener_addr {bind_str}"))?;
			// Mirror the wake server's stale_after when configured, else
			// keep the wake-server crate default of 80 s.
			let stale_after = orchestrator
				.wake_server_config()
				.map(|w| std::time::Duration::from_millis(w.stale_after_ms))
				.unwrap_or_else(|| std::time::Duration::from_millis(80_000));
			let forward = if pl_block.forward_upstream {
				Some(bairelay::push_listener::ForwardConfig {
					resolver: pl_block
						.upstream_resolver_addr()
						.map_err(|e| anyhow::anyhow!(e))?,
					host: bairelay::push_listener::PUSH_HOST.to_string(),
					port: 443,
				})
			} else {
				None
			};
			let cfg = bairelay::push_listener::RuntimeConfig {
				bind_addr,
				bind_port: pl_block.push_listener_port,
				motion_wake_hold: std::time::Duration::from_secs_f64(
					pl_block.motion_wake_hold_secs,
				),
				stale_after,
				forward,
			};
			let listener_addr = std::net::SocketAddr::new(bind_addr, pl_block.push_listener_port);
			let listener = tokio::net::TcpListener::bind(listener_addr)
				.await
				.with_context(|| format!("push listener bind failed on {}", listener_addr))?;
			let registry_for_pl = registry.clone();
			let cameras_for_pl = orchestrator.cameras_arc();
			let mqtt_for_pl = orchestrator.mqtt_client().cloned();
			let prefix_for_pl = orchestrator.topic_prefix().to_string();
			let bind_port_log = pl_block.push_listener_port;
			sup.spawn("push_listener", move |cancel| async move {
				if let Err(e) = bairelay::push_listener::run_with_listener(
					listener,
					cfg,
					registry_for_pl,
					cameras_for_pl,
					mqtt_for_pl,
					prefix_for_pl,
					cancel,
				)
				.await
				{
					tracing::error!(error = %e, "push listener exited with error");
				}
			});
			info!(
				bind = %bind_str,
				port = bind_port_log,
				"Push listener started"
			);
		}
	}

	// Run the orchestrator (blocks until all camera tasks exit).
	// During their teardown the per-camera tasks publish a final
	// "disconnected" status — these succeed because the MQTT event
	// loop is still polling (mqtt_cancel hasn't fired yet).
	orchestrator.run().await;

	// Cameras are drained; gracefully shut down every supervisor-managed
	// service (RTSP listeners, wake server, push listener, watchdog,
	// startup-wake). Each service holds the global cancel token; the
	// supervisor cancels it (idempotent — the shutdown signal already fired it) and
	// joins each task with a 2 s budget so a wedged subsystem can't
	// hold the process open.
	sup.shutdown(Duration::from_secs(2)).await;

	// MQTT is the last subsystem to shut down. Bounded wait so a wedged
	// broker connection can't hold the process open indefinitely.
	if let Some(handle) = mqtt_task {
		mqtt_cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
	}

	info!("Shutdown complete");

	Ok(())
}

async fn run_mqtt_event_loop(
	mut event_loop: MqttEventLoop,
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	cancel: CancellationToken,
) {
	let mut backoff = MqttBackoff::new();
	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			event = event_loop.poll() => {
				match classify_event(&event) {
					EventAction::Publish { topic, payload } => {
						backoff.reset();
						let payload_str = String::from_utf8_lossy(payload);
						tracing::debug!(topic = %topic, payload = %payload_str, "MQTT message received");
						if let Some(cmd) = parse_control_message(&topic_prefix, topic, payload) {
							let cameras = Arc::clone(&cameras);
							let mqtt = mqtt.clone();
							let prefix = topic_prefix.clone();
							tokio::spawn(async move {
								dispatch_control(cmd, &cameras, &mqtt, &prefix).await;
							});
						}
					}
					EventAction::ConnAck => {
						backoff.reset();
						handle_connack(&cameras, &mqtt, &topic_prefix).await;
					}
					EventAction::LogError => {
						// `rumqttc::EventLoop::poll()` returns errors with
						// no internal pacing — without our own backoff, a
						// missing broker produces hundreds of identical
						// "Connection refused" warnings per second.
						if let Err(ref e) = event {
							let msg = e.to_string();
							let (delay, should_log) = backoff.record_error(&msg);
							if should_log {
								tracing::warn!(error = %msg, "MQTT event loop error; retrying with backoff");
							}
							if !bairelay::run_support::sleep_or_cancel(delay, &cancel).await {
								break;
							}
						}
					}
					EventAction::Ignore => {}
				}
			}
		}
	}
}
