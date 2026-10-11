use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::bcmedia_dump::BcMediaDumpConfig;
use crate::camera::CameraHandle;
use crate::config::Config;

// ── Orchestrator ──────────────────────────────────────────────────────

pub struct Orchestrator {
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	cancel: CancellationToken,
	wake_server_config: Option<bairelay_wake_server::WakeServerConfig>,
	push_listener_config: Option<crate::config::PushListenerConfig>,
	mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
	topic_prefix: String,
}

impl Orchestrator {
	pub fn new(
		config: Config,
		cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
	) -> Self {
		Self::with_bcmedia_dump(config, cancel, mqtt_client, None)
	}

	/// Constructor variant that forwards a shared [`BcMediaDumpConfig`] to
	/// every camera handle. Used when the operator passes `--dump-bcmedia`
	/// on the CLI; prefer [`Orchestrator::new`] for the common case.
	pub fn with_bcmedia_dump(
		config: Config,
		cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
		bcmedia_dump: Option<Arc<BcMediaDumpConfig>>,
	) -> Self {
		Self::with_bcmedia_dump_and_discovery(config, cancel, mqtt_client, bcmedia_dump, None)
	}

	/// Full constructor that also takes an optional
	/// [`bairelay_mqtt::DiscoveryPublisher`]. Only the binary builds
	/// this when `[mqtt.discovery]` is present in config; tests and
	/// the bcmedia-dump variant pass `None`. Cloned into each
	/// `CameraHandle` so `publish_discovery`/`unpublish_discovery` can
	/// reach the shared MQTT client without a separate plumbing path.
	pub fn with_bcmedia_dump_and_discovery(
		config: Config,
		cancel: CancellationToken,
		mqtt_client: Option<bairelay_mqtt::SharedMqttClient>,
		bcmedia_dump: Option<Arc<BcMediaDumpConfig>>,
		discovery_publisher: Option<bairelay_mqtt::DiscoveryPublisher>,
	) -> Self {
		// Capture the configured MQTT topic prefix before the config is
		// moved. Default to "bairelay" when MQTT is disabled — no
		// CameraHandle task consults this value in that case.
		let topic_prefix = config
			.mqtt
			.as_ref()
			.map(|m| m.topic_prefix.clone())
			.unwrap_or_else(|| "bairelay".to_string());

		// Capture the global prune grace before config is moved. Each
		// CameraHandle gets a copy so its grace-period watcher can
		// enforce the `idle_disconnect_timeout_secs >= prune_grace`
		// invariant via the floor in `resolve_idle_disconnect_timeout`.
		let prune_grace = Duration::from_secs(config.stream_prune_grace_secs);

		// Capture the wake-server block before config.cameras is consumed
		// below. Validation is deferred to RuntimeConfig::from_block at
		// spawn time (in main.rs), not at orchestrator construction.
		let wake_server_config = config.wake_server.clone();
		let push_listener_config = config.push_listener.clone();

		let cameras = config
			.cameras
			.into_iter()
			.filter(|c| c.enabled)
			.map(|cam_config| {
				let name = cam_config.name.clone();
				let mut handle = CameraHandle::with_bcmedia_dump_and_prefix(
					cam_config,
					cancel.clone(),
					mqtt_client.clone(),
					topic_prefix.clone(),
					bcmedia_dump.clone(),
				)
				.with_prune_grace(prune_grace);
				if let Some(ref publisher) = discovery_publisher {
					handle = handle.with_discovery_publisher(publisher.clone());
				}
				(name, Arc::new(handle))
			})
			.collect();

		Self {
			cameras: Arc::new(cameras),
			cancel,
			wake_server_config,
			push_listener_config,
			mqtt_client,
			topic_prefix,
		}
	}

	pub fn wake_server_config(&self) -> Option<&bairelay_wake_server::WakeServerConfig> {
		self.wake_server_config.as_ref()
	}

	pub fn push_listener_config(&self) -> Option<&crate::config::PushListenerConfig> {
		self.push_listener_config.as_ref()
	}

	pub fn mqtt_client(&self) -> Option<&bairelay_mqtt::SharedMqttClient> {
		self.mqtt_client.as_ref()
	}

	pub fn topic_prefix(&self) -> &str {
		&self.topic_prefix
	}

	pub fn camera_count(&self) -> usize {
		self.cameras.len()
	}

	pub fn get_camera(&self, name: &str) -> Option<&Arc<CameraHandle>> {
		self.cameras.get(name)
	}

	pub fn cameras(&self) -> impl Iterator<Item = &Arc<CameraHandle>> {
		self.cameras.values()
	}

	/// Returns a shared reference to the cameras map for use by other
	/// components (e.g. the watchdog).
	pub fn cameras_arc(&self) -> Arc<HashMap<String, Arc<CameraHandle>>> {
		Arc::clone(&self.cameras)
	}

	pub fn cancel_token(&self) -> &CancellationToken {
		&self.cancel
	}

	/// Spawn per-camera connection loops and wait for all to complete.
	pub async fn run(&self) {
		let mut set = tokio::task::JoinSet::new();

		for (name, camera) in self.cameras.iter() {
			let cam = Arc::clone(camera);
			let name = name.clone();
			set.spawn(async move {
				tracing::info!(camera = %name, "Starting camera task");
				cam.run().await;
			});
		}

		// Wait for all tasks (they exit on cancellation)
		while let Some(result) = set.join_next().await {
			if let Err(e) = result {
				tracing::error!("Camera task panicked: {}", e);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;

	fn two_camera_config() -> Config {
		let mut cfg = Config::default();
		cfg.cameras.push(minimal_camera_config("alpha"));
		cfg.cameras.push(minimal_camera_config("beta"));
		cfg
	}

	#[test]
	fn new_collects_every_enabled_camera() {
		let cfg = two_camera_config();
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel, None);
		assert_eq!(orch.camera_count(), 2);
		assert!(orch.get_camera("alpha").is_some());
		assert!(orch.get_camera("beta").is_some());
		assert!(orch.get_camera("nope").is_none());
	}

	#[test]
	fn new_skips_disabled_cameras() {
		let mut cfg = two_camera_config();
		cfg.cameras[1].enabled = false;
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel, None);
		assert_eq!(orch.camera_count(), 1);
		assert!(orch.get_camera("alpha").is_some());
		assert!(orch.get_camera("beta").is_none());
	}

	#[test]
	fn cameras_iterator_and_arc_accessor_agree() {
		let cfg = two_camera_config();
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel.clone(), None);
		let iter_count = orch.cameras().count();
		let arc_count = orch.cameras_arc().len();
		assert_eq!(iter_count, arc_count);
		assert_eq!(iter_count, 2);
		// The cancel token round-trips through the accessor.
		assert!(!orch.cancel_token().is_cancelled());
		cancel.cancel();
		assert!(orch.cancel_token().is_cancelled());
	}

	#[test]
	fn orchestrator_getters_round_trip() {
		// Cover the small public getters: wake_server_config /
		// push_listener_config / mqtt_client / topic_prefix. None of
		// these are exercised elsewhere because main.rs can't be
		// unit-tested.
		let mut cfg = two_camera_config();
		// Set [mqtt] so topic_prefix flows through.
		cfg.mqtt = Some(crate::config::MqttServerConfig {
			broker_addr: "127.0.0.1".to_string(),
			port: 1883,
			credentials: None,
			tls: None,
			ca: None,
			client_auth: None,
			topic_prefix: "myprefix".to_string(),
			discovery: None,
		});
		// Set wake_server + push_listener so the optional getters
		// return Some.
		cfg.wake_server = Some(bairelay_wake_server::WakeServerConfig {
			enable: true,
			..Default::default()
		});
		cfg.push_listener = Some(crate::config::PushListenerConfig {
			enable: true,
			push_listener_addr: None,
			push_listener_port: 443,
			motion_wake_hold_secs: 30.0,
			..Default::default()
		});
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel, None);
		assert!(orch.wake_server_config().is_some());
		assert!(orch.wake_server_config().unwrap().enable);
		assert!(orch.push_listener_config().is_some());
		assert_eq!(orch.push_listener_config().unwrap().push_listener_port, 443);
		// mqtt_client stays None: orchestrator is built without one.
		assert!(orch.mqtt_client().is_none());
		assert_eq!(orch.topic_prefix(), "myprefix");
	}

	#[tokio::test]
	async fn run_with_no_cameras_returns_immediately() {
		let cfg = Config::default();
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel, None);
		// No cameras → no spawned tasks → run() should complete fast.
		tokio::time::timeout(std::time::Duration::from_millis(500), orch.run())
			.await
			.expect("run returns without pending cameras");
	}

	#[tokio::test]
	async fn run_returns_when_cancel_fires_with_unroutable_cameras() {
		// Two cameras pointed at loopback port 1 (refused instantly).
		// Their run loops spin into the Connecting + backoff cycle;
		// cancel after 200 ms should tear them down and let run() exit.
		let mut cfg = two_camera_config();
		for cam in &mut cfg.cameras {
			cam.address = Some("127.0.0.1:1".to_string());
		}
		let cancel = CancellationToken::new();
		let orch = Orchestrator::new(cfg, cancel.clone(), None);
		let run_fut = orch.run();
		let cancel_fut = async {
			tokio::time::sleep(std::time::Duration::from_millis(200)).await;
			cancel.cancel();
		};
		tokio::join!(cancel_fut, run_fut);
	}
}
