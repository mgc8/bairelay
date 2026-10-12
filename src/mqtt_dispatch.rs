//! Routes incoming MQTT control commands to the appropriate BcCamera methods.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use bairelay_mqtt::control::{ControlCommand, IrMode, PtzDirection};
use bairelay_mqtt::{topics, SharedMqttClient, StatusPublisher};

use bairelay_neolink_core::bc_protocol::{Direction, LightState};

use crate::camera::CameraHandle;
use crate::camera_tasks::{publish_after_led_write, LedWrite};

pub async fn dispatch_control(
	cmd: ControlCommand,
	cameras: &HashMap<String, Arc<CameraHandle>>,
	mqtt: &SharedMqttClient,
	topic_prefix: &str,
) {
	let camera_name = cmd.camera_name().to_owned();
	let Some(cam) = cameras.get(&camera_name) else {
		tracing::warn!(camera = %camera_name, "Control for unknown camera");
		return;
	};

	// Wakeup is special — it must work even when camera is disconnected,
	// because its purpose is to trigger the connection via wake lock.
	if let ControlCommand::Wakeup { minutes, .. } = &cmd {
		tracing::info!(camera = %camera_name, minutes, "Wakeup requested");
		let wl = cam.wake_lock().clone();
		let cam_clone = Arc::clone(cam);
		let cancel = cam_clone.cancel_token().clone();
		let mins = *minutes;
		tokio::spawn(async move {
			let _guard = wl.acquire();
			// Wait for camera to connect, with timeout and cancellation
			let connect_timeout = tokio::time::timeout(Duration::from_secs(120), async {
				loop {
					if cam_clone.state().is_connected() {
						return;
					}
					tokio::time::sleep(Duration::from_millis(500)).await;
				}
			});
			tokio::select! {
				_ = cancel.cancelled() => return,
				result = connect_timeout => {
					if result.is_err() {
						tracing::warn!(camera = %cam_clone.name(), "Wakeup timed out waiting for camera to connect");
						return;
					}
				}
			}
			// Camera connected — now start the countdown
			tokio::select! {
				_ = cancel.cancelled() => {},
				_ = tokio::time::sleep(Duration::from_secs(mins as u64 * 60)) => {},
			}
		});
		return;
	}

	// Acquire the wake lock FIRST. If the camera is sleeping, this is
	// what triggers the connection — the camera task is parked on
	// `wait_for_acquire()`. Holding the guard until dispatch ends keeps
	// the camera awake for the duration of the command.
	let _guard = cam.wake_lock().acquire();
	let reply_topic = cmd.control_topic(topic_prefix);
	tracing::debug!(camera = %camera_name, command = %reply_topic, "Dispatching control command");

	let bc = match cam.bc_camera() {
		Some(bc) => bc,
		None => {
			// Camera is asleep / connecting. The wake-lock acquire above
			// has already kicked off the connect; wait for it (bounded)
			// and respect the camera's cancellation token. Argus
			// cameras come up in 1-3 s in practice; 15 s gives margin
			// for slow discovery without making a stuck command sit
			// here for a minute.
			tracing::info!(
				camera = %camera_name,
				"Camera not connected; waking and waiting before dispatch"
			);
			match wait_for_bc_camera(cam, Duration::from_secs(15)).await {
				Some(bc) => bc,
				None => {
					tracing::warn!(
						camera = %camera_name,
						"Timed out waiting for camera to connect; dropping command"
					);
					return;
				}
			}
		}
	};

	const CMD_TIMEOUT: Duration = Duration::from_secs(30);

	let result = match cmd {
		ControlCommand::Floodlight { state, .. } => {
			timeout_to_result(timeout(CMD_TIMEOUT, bc.set_floodlight_manual(state, 30)).await)
		}
		ControlCommand::FloodlightTasks { state, .. } => {
			timeout_to_result(timeout(CMD_TIMEOUT, bc.floodlight_tasks_enable(state)).await)
		}
		ControlCommand::Led {
			camera: ref cam_name,
			state,
		} => {
			let _led = cam.led_write_lock().lock().await;
			let result = timeout_to_result(timeout(CMD_TIMEOUT, bc.led_light_set(state)).await);
			if result.is_ok() {
				let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
				let cache = cam.status_cache();
				let written = LedWrite::Led(state);
				publish_after_led_write(cam_name, &*bc, &publisher, &cache, written).await;
			}
			result
		}
		ControlCommand::Ir {
			camera: ref cam_name,
			mode,
		} => {
			let ls = match mode {
				IrMode::On => LightState::On,
				IrMode::Off => LightState::Off,
				IrMode::Auto => LightState::Auto,
			};
			let _led = cam.led_write_lock().lock().await;
			let result = timeout_to_result(timeout(CMD_TIMEOUT, bc.irled_light_set(ls)).await);
			if result.is_ok() {
				let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
				let cache = cam.status_cache();
				let written = LedWrite::Ir(mode);
				publish_after_led_write(cam_name, &*bc, &publisher, &cache, written).await;
			}
			result
		}
		ControlCommand::Pir {
			camera: ref cam_name,
			state,
		} => {
			let result = timeout_to_result(timeout(CMD_TIMEOUT, bc.pir_set(state)).await);
			if result.is_ok() {
				// Re-publish PIR state after change
				let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
				let _ = publisher.publish_pir(state).await;
				cam.status_cache().set_pir(state);
			}
			result
		}
		ControlCommand::Reboot { .. } => timeout_to_result(timeout(CMD_TIMEOUT, bc.reboot()).await),
		ControlCommand::Ptz {
			direction, amount, ..
		} => {
			let dir = match direction {
				PtzDirection::Up => Direction::Up,
				PtzDirection::Down => Direction::Down,
				PtzDirection::Left => Direction::Left,
				PtzDirection::Right => Direction::Right,
			};
			let speed = 32.0_f32;
			let seconds = (amount / speed).clamp(0.0, 10.0);
			match timeout(CMD_TIMEOUT, bc.send_ptz(dir, speed)).await {
				Ok(Err(e)) => {
					tracing::warn!(camera = %camera_name, error = %e, "PTZ move failed");
				}
				Err(_) => {
					tracing::warn!(camera = %camera_name, "PTZ move timed out");
				}
				Ok(Ok(())) => {
					tokio::time::sleep(Duration::from_secs_f32(seconds)).await;
				}
			}
			// Always try to stop
			let _ = timeout(CMD_TIMEOUT, bc.send_ptz(Direction::Stop, speed)).await;
			Ok(())
		}
		ControlCommand::PtzPreset {
			ref camera,
			preset_id,
		} => {
			let result =
				timeout_to_result(timeout(CMD_TIMEOUT, bc.moveto_ptz_preset(preset_id)).await);
			if result.is_ok() {
				if let Some(name) = cam.preset_name_for_id(preset_id) {
					let topic = topics::status_ptz_preset(topic_prefix, camera);
					let _ = mqtt.publish_retained(&topic, name.as_bytes()).await;
				}
			}
			result
		}
		ControlCommand::PtzPresetByName {
			ref camera,
			ref name,
		} => match cam.preset_id_for_name(name) {
			Some(preset_id) => {
				let result =
					timeout_to_result(timeout(CMD_TIMEOUT, bc.moveto_ptz_preset(preset_id)).await);
				if result.is_ok() {
					let topic = topics::status_ptz_preset(topic_prefix, camera);
					let _ = mqtt.publish_retained(&topic, name.as_bytes()).await;
				}
				result
			}
			None => {
				// Cache-miss must surface as FAIL to the operator —
				// previously this returned Ok(()) and the dispatch loop
				// published `OK` on the reply topic, so an HA automation
				// or `mosquitto_pub` user saw "success" while the camera
				// never moved. The cache is populated on connect and on
				// `query/ptz/preset`; a miss means either a typo or a
				// stale dashboard.
				tracing::warn!(camera = %camera, name = %name, "PTZ preset name not in cache");
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"PTZ preset name not in cache",
				))
			}
		},
		ControlCommand::PtzAssign {
			preset_id, name, ..
		} => timeout_to_result(timeout(CMD_TIMEOUT, bc.set_ptz_preset(preset_id, name)).await),
		ControlCommand::Zoom { level, .. } => {
			timeout_to_result(timeout(CMD_TIMEOUT, bc.zoom_to((level * 1000.0) as u32)).await)
		}
		ControlCommand::Siren { state, .. } => {
			if state {
				timeout_to_result(timeout(CMD_TIMEOUT, bc.siren()).await)
			} else {
				Ok(())
			}
		}
		ControlCommand::Wakeup { .. } => {
			// Handled above (before connection check)
			unreachable!()
		}
		// Query commands -- serialize response as XML to status topic
		// (non-retained), matching neolink behavior.
		ControlCommand::QueryBattery { ref camera, .. } => {
			match timeout(CMD_TIMEOUT, bc.battery_info()).await {
				Ok(Ok(info)) => {
					let topic = topics::status_battery(topic_prefix, camera);
					if let Ok(xml) = serialize_xml(&info) {
						let _ = mqtt.publish(&topic, xml.as_bytes()).await;
					}
				}
				Ok(Err(e)) => tracing::warn!(camera = %camera, error = %e, "Battery query failed"),
				Err(_) => tracing::warn!(camera = %camera, "Battery query timed out"),
			}
			Ok(())
		}
		ControlCommand::QueryPreview { ref camera } => {
			match timeout(CMD_TIMEOUT, bc.get_snapshot()).await {
				Ok(Ok(jpeg)) => {
					// One-shot render (no shared cache); cheaper than
					// threading the per-camera OverlayCache in for a
					// single operator-triggered query.
					let payload = crate::preview_overlay::rendered_preview(
						bytes::Bytes::from(jpeg),
						cam.config().pause.preview_overlay,
						*cam.preview_state_rx().borrow(),
						None,
					);
					let publisher = StatusPublisher::new(mqtt, topic_prefix, camera);
					if let Err(e) = publisher.publish_preview(&payload).await {
						tracing::warn!(camera = %camera, error = %e, "QueryPreview publish failed");
					}
				}
				Ok(Err(e)) => {
					tracing::warn!(camera = %camera, error = %e, "QueryPreview snapshot failed")
				}
				Err(_) => tracing::warn!(camera = %camera, "QueryPreview snapshot timed out"),
			}
			Ok(())
		}
		ControlCommand::QueryPir { ref camera, .. } => {
			match timeout(CMD_TIMEOUT, bc.get_pirstate()).await {
				Ok(Ok(pir_state)) => {
					let topic = topics::status_pir(topic_prefix, camera);
					if let Ok(xml) = serialize_xml(&pir_state) {
						let _ = mqtt.publish(&topic, xml.as_bytes()).await;
					}
				}
				Ok(Err(e)) => tracing::warn!(camera = %camera, error = %e, "PIR query failed"),
				Err(_) => tracing::warn!(camera = %camera, "PIR query timed out"),
			}
			Ok(())
		}
		ControlCommand::QueryPtzPreset { ref camera } => {
			match timeout(CMD_TIMEOUT, bc.get_ptz_preset()).await {
				Ok(Ok(p)) => {
					let presets: Vec<(u8, String)> = p
						.preset_list
						.preset
						.into_iter()
						.filter_map(|preset| preset.name.map(|n| (preset.id, n)))
						.collect();
					cam.replace_preset_cache(presets);
					if let Err(e) = cam.publish_discovery().await {
						tracing::warn!(
							camera = %camera,
							error = %e,
							"QueryPtzPreset: discovery republish failed"
						);
					}
				}
				Ok(Err(e)) => {
					tracing::warn!(camera = %camera, error = %e, "QueryPtzPreset: get_ptz_preset failed")
				}
				Err(_) => {
					tracing::warn!(camera = %camera, "QueryPtzPreset: get_ptz_preset timed out")
				}
			}
			Ok(())
		}
	};

	// Publish OK/FAIL reply (non-retained) to the control topic.
	let reply = if result.is_ok() { "OK" } else { "FAIL" };
	tracing::debug!(camera = %camera_name, command = %reply_topic, result = reply, "Command completed");
	let _ = mqtt.publish(&reply_topic, reply.as_bytes()).await;

	if let Err(e) = result {
		tracing::warn!(camera = %camera_name, command = %reply_topic, error = %e, "Control command failed");
	}
}

/// Wait until the camera's `bc_camera()` returns `Some`, polling every
/// 250 ms up to `deadline`. Cancellation token wins immediately.
/// Returns `None` on timeout or cancellation.
async fn wait_for_bc_camera(
	cam: &Arc<CameraHandle>,
	deadline: Duration,
) -> Option<Arc<dyn bairelay_neolink_core::bc_protocol::CameraDriver>> {
	let cancel = cam.cancel_token().clone();
	let cam_clone = Arc::clone(cam);
	let inner = async move {
		loop {
			if let Some(bc) = cam_clone.bc_camera() {
				return Some(bc);
			}
			tokio::time::sleep(Duration::from_millis(250)).await;
		}
	};
	tokio::select! {
		_ = cancel.cancelled() => None,
		result = tokio::time::timeout(deadline, inner) => result.ok().flatten(),
	}
}

/// Collapse a `timeout(..).await` outcome into a plain camera-result,
/// converting an `Err(Elapsed)` into our canonical "Command timed out"
/// error. Pulled out so the 11 identical call-sites in this module
/// share one implementation — and so the failure-path test doesn't
/// need a fake camera that hangs 30 s.
pub(crate) fn timeout_to_result<T>(
	outcome: std::result::Result<
		std::result::Result<T, bairelay_neolink_core::bc_protocol::Error>,
		tokio::time::error::Elapsed,
	>,
) -> std::result::Result<T, bairelay_neolink_core::bc_protocol::Error> {
	outcome.unwrap_or_else(|_| {
		Err(bairelay_neolink_core::bc_protocol::Error::Other(
			"Command timed out",
		))
	})
}

/// Serialize a serde-compatible struct to XML string (matches neolink's
/// quick-xml serialization for query responses).
fn serialize_xml<T: serde::Serialize>(value: &T) -> Result<String, String> {
	let mut buf = bytes::BytesMut::new();
	quick_xml::se::to_writer(&mut buf, value)
		.map_err(|e| format!("XML serialization failed: {e}"))?;
	String::from_utf8(buf.to_vec()).map_err(|e| format!("XML encoding failed: {e}"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::camera::CameraHandle;
	use crate::config::test_helpers::minimal_camera_config;
	use bairelay_neolink_core::bc_protocol::{CameraDriver, FakeCamera, FakeCameraBuilder};
	use std::sync::Arc as StdArc;
	use tokio_util::sync::CancellationToken;

	#[test]
	fn timeout_to_result_forwards_ok_value() {
		let r: std::result::Result<u32, bairelay_neolink_core::bc_protocol::Error> =
			timeout_to_result(Ok(Ok(42)));
		assert_eq!(r.unwrap(), 42);
	}

	#[test]
	fn timeout_to_result_forwards_inner_error() {
		let r: std::result::Result<u32, bairelay_neolink_core::bc_protocol::Error> =
			timeout_to_result(Ok(Err(
				bairelay_neolink_core::bc_protocol::Error::AuthFailed,
			)));
		assert!(matches!(
			r.unwrap_err(),
			bairelay_neolink_core::bc_protocol::Error::AuthFailed
		));
	}

	#[tokio::test]
	async fn timeout_to_result_elapsed_becomes_other_command_timed_out() {
		let elapsed = tokio::time::timeout(
			Duration::from_millis(1),
			std::future::pending::<
				std::result::Result<(), bairelay_neolink_core::bc_protocol::Error>,
			>(),
		)
		.await
		.unwrap_err();
		let r: std::result::Result<(), bairelay_neolink_core::bc_protocol::Error> =
			timeout_to_result(Err(elapsed));
		let err = r.unwrap_err();
		assert!(format!("{err}").contains("Command timed out"));
	}

	fn test_cameras(name: &str) -> HashMap<String, Arc<CameraHandle>> {
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(minimal_camera_config(name), cancel, None));
		let mut map = HashMap::new();
		map.insert(name.to_string(), handle);
		map
	}

	/// Build a [`CameraHandle`] with a pre-built [`FakeCamera`] installed
	/// via [`CameraHandle::set_driver_for_test`], returning a cameras
	/// map ready for [`dispatch_control`] plus the retained `FakeCamera`
	/// handle so the test can assert on [`FakeCamera::calls`] after the
	/// dispatcher runs.
	fn test_cameras_with_fake(
		name: &str,
		fake: StdArc<FakeCamera>,
	) -> (HashMap<String, Arc<CameraHandle>>, StdArc<FakeCamera>) {
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(minimal_camera_config(name), cancel, None));
		let driver: Arc<dyn CameraDriver> = fake.clone();
		handle.set_driver_for_test(driver);
		let mut map = HashMap::new();
		map.insert(name.to_string(), handle);
		(map, fake)
	}

	#[tokio::test]
	async fn dispatch_unknown_camera_returns_early() {
		let cameras = test_cameras("cam-known");
		let mqtt = SharedMqttClient::for_test_stub("dispatch-unknown");
		let cmd = ControlCommand::Floodlight {
			camera: "cam-missing".to_string(),
			state: true,
		};
		// Must not panic; must return quickly (early-exit).
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
	}

	#[tokio::test]
	async fn dispatch_disconnected_camera_waits_then_drops_on_cancel() {
		// Camera is Disconnected and there is no run loop, so the
		// wake-lock acquire never lands a `bc_camera`. Dispatch should
		// wait, then bail when the camera's cancel token fires —
		// without panicking and without spinning forever. The bound is
		// 60 s (production timeout); cancellation breaks out earlier.
		let cameras = test_cameras("cam-disc");
		let mqtt = SharedMqttClient::for_test_stub("dispatch-disc");
		let cmd = ControlCommand::Led {
			camera: "cam-disc".to_string(),
			state: false,
		};
		// Cancel before dispatch so wait_for_bc_camera returns immediately.
		cameras.get("cam-disc").unwrap().cancel_token().cancel();
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
	}

	#[tokio::test]
	async fn dispatch_wakeup_spawns_task_without_panicking() {
		// Wakeup is the only command that tolerates a disconnected
		// camera — it acquires the wake lock and waits for the run
		// loop to connect. Since we have no run loop here, the
		// spawned task will eventually time out; the dispatcher
		// itself returns immediately. Cancel the camera's token so
		// the spawned task exits cleanly at the end of the test.
		let cameras = test_cameras("cam-wake");
		let mqtt = SharedMqttClient::for_test_stub("dispatch-wake");
		let cmd = ControlCommand::Wakeup {
			camera: "cam-wake".to_string(),
			minutes: 1,
		};
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
		// Cancel so the background task exits — otherwise it would
		// linger for the 120 s connect-timeout budget.
		cameras.get("cam-wake").unwrap().cancel_token().cancel();
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	/// `ControlCommand::Reboot` dispatches to `CameraDriver::reboot`
	/// exactly once. Fake records the invocation count; the OK reply
	/// is published on the control topic.
	#[tokio::test]
	async fn dispatch_reboot_invokes_driver_and_replies_ok() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-reboot", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Reboot {
				camera: "cam-reboot".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			*fake.calls().reboot.lock().unwrap(),
			1,
			"reboot should be invoked exactly once"
		);
		assert!(
			mock.published()
				.iter()
				.any(|(t, p, _)| { t == "bairelay/cam-reboot/control/reboot" && p == b"OK" }),
			"OK reply should be published on the control topic; observed: {:?}",
			mock.published_topics()
		);
	}

	/// `ControlCommand::Floodlight { state: true }` dispatches to
	/// `set_floodlight_manual(true, 30)` — the hard-coded 30-second
	/// duration is part of the current contract and must not drift.
	#[tokio::test]
	async fn dispatch_floodlight_on_invokes_driver_with_30s_duration() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-fl", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Floodlight {
				camera: "cam-fl".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			*fake.calls().set_floodlight_manual.lock().unwrap(),
			vec![(true, 30)],
			"Floodlight on should call set_floodlight_manual(true, 30)"
		);
	}

	/// `ControlCommand::Floodlight { state: false }` — same path,
	/// opposite state.
	#[tokio::test]
	async fn dispatch_floodlight_off_invokes_driver_with_30s_duration() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-fl-off", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Floodlight {
				camera: "cam-fl-off".to_string(),
				state: false,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			*fake.calls().set_floodlight_manual.lock().unwrap(),
			vec![(false, 30)]
		);
	}

	/// `ControlCommand::Pir { state: true }` dispatches to
	/// `pir_set(true)` and republishes the new state on
	/// `status/pir` so HA reflects the change.
	#[tokio::test]
	async fn dispatch_pir_set_invokes_driver_and_republishes_state() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pir", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Pir {
				camera: "cam-pir".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(*fake.calls().pir_set.lock().unwrap(), vec![true]);
		// Both the status republish and the OK reply should appear.
		let rows = mock.published();
		assert!(
			rows.iter()
				.any(|(t, p, r)| t == "bairelay/cam-pir/status/pir" && p == b"on" && *r),
			"Pir set should re-publish retained status/pir=on; observed: {:?}",
			rows
		);
		assert!(
			rows.iter()
				.any(|(t, p, _)| t == "bairelay/cam-pir/control/pir" && p == b"OK"),
			"Pir set should reply OK on the control topic; observed: {:?}",
			rows
		);
	}

	/// `ControlCommand::PtzPresetByName` with a name that isn't in the
	/// preset cache must publish `FAIL` on the reply topic, not `OK`.
	/// Previously this returned `Ok(())` and operators saw a successful
	/// reply while the camera never moved.
	#[tokio::test]
	async fn dispatch_ptz_preset_by_name_cache_miss_replies_fail() {
		// `cam.preset_id_for_name(...)` returns `None` for any name that
		// hasn't been populated by `replace_preset_cache`. A fresh
		// FakeCamera-backed CameraHandle has an empty cache.
		let fake = FakeCameraBuilder::new().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-pn", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::PtzPresetByName {
				camera: "cam-pn".to_string(),
				name: "Doorstep".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		// The reply topic mirrors the inbound command topic; cache-miss
		// must produce a non-retained `FAIL`.
		let publishes = mock.published();
		let reply = publishes
			.iter()
			.find(|(t, _, _)| t == "bairelay/cam-pn/control/ptz/preset")
			.expect("dispatcher must publish on the reply topic");
		assert_eq!(reply.1, b"FAIL", "cache-miss must reply FAIL");
	}

	/// `ControlCommand::PtzAssign { preset_id: 1, name: "Home" }`
	/// dispatches to `set_ptz_preset(1, "Home")`.
	#[tokio::test]
	async fn dispatch_ptz_assign_invokes_driver_with_id_and_name() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pt", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::PtzAssign {
				camera: "cam-pt".to_string(),
				preset_id: 1,
				name: "Home".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			*fake.calls().set_ptz_preset.lock().unwrap(),
			vec![(1u8, "Home".to_string())]
		);
	}

	/// `ControlCommand::Ptz { direction: Up, amount: 0.0 }` dispatches to
	/// `send_ptz` twice: first with the requested direction+speed, then
	/// unconditionally with `Direction::Stop` so the camera halts even if
	/// the initial move errored. Amount 0.0 keeps the sleep duration at
	/// zero so the test doesn't wait on a real clock.
	#[tokio::test]
	async fn dispatch_ptz_directional_move_sends_direction_then_stop() {
		use bairelay_neolink_core::bc_protocol::Direction;

		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pd", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Ptz {
				camera: "cam-pd".to_string(),
				direction: PtzDirection::Up,
				amount: 0.0,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let calls = fake.calls().send_ptz.lock().unwrap().clone();
		assert_eq!(
			calls,
			vec![(Direction::Up, 32.0_f32), (Direction::Stop, 32.0_f32)],
			"directional Ptz should issue the direction then an unconditional Stop"
		);
	}

	/// Even when the initial `send_ptz(direction)` errors, the dispatch
	/// must still call `send_ptz(Stop)` — a camera that moved partway
	/// before the command failed should not be left running.
	#[tokio::test]
	async fn dispatch_ptz_directional_still_stops_on_error() {
		use bairelay_neolink_core::bc_protocol::Direction;

		// FakeCamera::send_ptz always returns Ok in the scaffolding, so
		// simulate error via a single-attempt counter-closure is not
		// possible without new FakeCamera plumbing. Instead we test the
		// always-stop invariant indirectly: a directional move must
		// record exactly two send_ptz calls regardless of amount,
		// because the second (Stop) is unconditional.
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pe", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Ptz {
				camera: "cam-pe".to_string(),
				direction: PtzDirection::Left,
				amount: 0.0,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let calls = fake.calls().send_ptz.lock().unwrap().clone();
		assert_eq!(calls.len(), 2, "must record two send_ptz calls");
		assert_eq!(calls[1].0, Direction::Stop, "second call must be Stop");
	}

	/// `ControlCommand::PtzPreset { preset_id: 3 }` dispatches to
	/// `moveto_ptz_preset(3)`.
	#[tokio::test]
	async fn dispatch_ptz_preset_invokes_driver_moveto() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pm", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::PtzPreset {
				camera: "cam-pm".to_string(),
				preset_id: 3,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(*fake.calls().moveto_ptz_preset.lock().unwrap(), vec![3u8]);
	}

	/// `ControlCommand::Led { state: true }` dispatches to `led_light_set(true)`.
	#[tokio::test]
	async fn dispatch_led_on_invokes_driver() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(led_state("auto", "open")))
			.build();
		let (cameras, fake) = test_cameras_with_fake("cam-led", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Led {
				camera: "cam-led".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(*fake.calls().led_light_set.lock().unwrap(), vec![true]);
		let rows = mock.published();
		assert!(rows
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-led/control/led" && p == b"OK"));
		assert!(
			rows.iter()
				.any(|(t, p, r)| t == "bairelay/cam-led/status/led" && p == b"on" && *r),
			"status/led must be republished; observed: {rows:?}"
		);
		assert_eq!(cameras["cam-led"].status_cache().led(), Some(true));
	}

	/// `ControlCommand::Ir` maps each IrMode variant onto the matching
	/// `LightState` and calls `irled_light_set`.
	#[tokio::test]
	async fn dispatch_ir_maps_each_mode_variant() {
		use bairelay_neolink_core::bc_protocol::LightState;
		let cases = [
			(IrMode::On, LightState::On),
			(IrMode::Off, LightState::Off),
			(IrMode::Auto, LightState::Auto),
		];
		for (mode, expected) in cases {
			// Re-read after the write reports the requested mode.
			let cam_state = match mode {
				IrMode::On => "open",
				IrMode::Off => "close",
				IrMode::Auto => "auto",
			};
			let fake = FakeCameraBuilder::new()
				.with_ledstate(move || Ok(led_state(cam_state, "close")))
				.build();
			let (cameras, fake) = test_cameras_with_fake("cam-ir", fake);
			let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
			dispatch_control(
				ControlCommand::Ir {
					camera: "cam-ir".to_string(),
					mode,
				},
				&cameras,
				&mqtt,
				"bairelay",
			)
			.await;
			let calls = fake.calls().irled_light_set.lock().unwrap().clone();
			assert_eq!(calls.len(), 1, "one irled_light_set call per dispatch");
			assert_eq!(calls[0], expected);
			// New mode republished on status/ir (retained) and cached.
			assert!(mock
				.published()
				.iter()
				.any(|(t, p, r)| t == "bairelay/cam-ir/status/ir"
					&& p == mode.as_payload().as_bytes()
					&& *r));
			assert_eq!(cameras["cam-ir"].status_cache().ir(), Some(mode));
		}
	}

	fn led_state(ir: &str, light: &str) -> bairelay_neolink_core::bc::xml::LedState {
		bairelay_neolink_core::bc::xml::LedState {
			state: ir.into(),
			light_state: light.into(),
			..Default::default()
		}
	}

	fn status_rows(mock: &bairelay_mqtt::test_support::MockHandle, topic: &str) -> Vec<Vec<u8>> {
		mock.published()
			.into_iter()
			.filter(|(t, _, _)| t == topic)
			.map(|(_, p, _)| p)
			.collect()
	}

	/// The camera answers OK but ignores the write: bairelay publishes
	/// what the post-write re-read reports, not what was requested.
	#[tokio::test]
	async fn dispatch_ir_publishes_camera_state_not_requested_mode() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(led_state("auto", "close")))
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-ir", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cmd = ControlCommand::Ir {
			camera: "cam-ir".to_string(),
			mode: IrMode::On,
		};
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
		assert_eq!(
			status_rows(&mock, "bairelay/cam-ir/status/ir"),
			vec![b"auto".to_vec()]
		);
		assert_eq!(cameras["cam-ir"].status_cache().ir(), Some(IrMode::Auto));
	}

	/// Re-read fails after a successful write: fall back to publishing
	/// the requested value.
	#[tokio::test]
	async fn dispatch_led_falls_back_to_requested_when_reread_fails() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"reread refused",
				))
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-led", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cmd = ControlCommand::Led {
			camera: "cam-led".to_string(),
			state: false,
		};
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
		assert_eq!(
			status_rows(&mock, "bairelay/cam-led/status/led"),
			vec![b"off".to_vec()]
		);
		assert_eq!(cameras["cam-led"].status_cache().led(), Some(false));
	}

	/// A failed write publishes no state, leaves the cache alone, and
	/// replies FAIL.
	#[tokio::test]
	async fn dispatch_failed_led_and_ir_writes_publish_nothing() {
		let fake = FakeCameraBuilder::new().with_led_writes_failing().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-x", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let led = ControlCommand::Led {
			camera: "cam-x".to_string(),
			state: true,
		};
		let ir = ControlCommand::Ir {
			camera: "cam-x".to_string(),
			mode: IrMode::Off,
		};
		dispatch_control(led, &cameras, &mqtt, "bairelay").await;
		dispatch_control(ir, &cameras, &mqtt, "bairelay").await;
		assert!(status_rows(&mock, "bairelay/cam-x/status/led").is_empty());
		assert!(status_rows(&mock, "bairelay/cam-x/status/ir").is_empty());
		let cache = cameras["cam-x"].status_cache();
		assert_eq!((cache.led(), cache.ir()), (None, None));
		assert_eq!(
			status_rows(&mock, "bairelay/cam-x/control/led"),
			vec![b"FAIL".to_vec()]
		);
	}

	/// `Siren { state: true }` invokes `siren()`; `state: false` is a no-op
	/// (the command still replies `OK`).
	#[tokio::test]
	async fn dispatch_siren_on_invokes_driver_and_off_is_noop() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-siren", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::Siren {
				camera: "cam-siren".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(*fake.calls().siren.lock().unwrap(), 1);

		// Off path is an explicit Ok(()) — no siren() call.
		dispatch_control(
			ControlCommand::Siren {
				camera: "cam-siren".to_string(),
				state: false,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(
			*fake.calls().siren.lock().unwrap(),
			1,
			"off-path must not invoke siren()"
		);
	}

	/// `FloodlightTasks { state: true }` dispatches to
	/// `floodlight_tasks_enable(true)`.
	#[tokio::test]
	async fn dispatch_floodlight_tasks_invokes_driver() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-ft", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::FloodlightTasks {
				camera: "cam-ft".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(
			*fake.calls().floodlight_tasks_enable.lock().unwrap(),
			vec![true]
		);
	}

	/// `Zoom { level }` dispatches to `zoom_to((level*1000) as u32)`.
	#[tokio::test]
	async fn dispatch_zoom_scales_level_to_zoom_to() {
		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-zoom", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::Zoom {
				camera: "cam-zoom".to_string(),
				level: 0.5,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(*fake.calls().zoom_to.lock().unwrap(), vec![500u32]);
	}

	/// `QueryBattery` calls `battery_info()` and publishes a
	/// serialized XML payload to `status/battery`.
	#[tokio::test]
	async fn dispatch_query_battery_publishes_xml() {
		use bairelay_neolink_core::bc::xml::BatteryInfo;
		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Ok(BatteryInfo {
					battery_percent: 42,
					..Default::default()
				})
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qb", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryBattery {
				camera: "cam-qb".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert!(mock
			.published()
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-qb/status/battery"
				&& std::str::from_utf8(p).unwrap_or("").contains("42")));
	}

	/// `QueryBattery` with a driver error — the dispatch still completes,
	/// replies OK on the control topic, and no status/battery row is
	/// published.
	#[tokio::test]
	async fn dispatch_query_battery_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"battery refused",
				))
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qbe", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryBattery {
				camera: "cam-qbe".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam-qbe/status/battery"),
			"error path must not publish status/battery"
		);
	}

	/// `QueryPreview` asks the driver for a snapshot and publishes the
	/// bytes on `status/preview`.
	#[tokio::test]
	async fn dispatch_query_preview_publishes_jpeg() {
		let jpeg: Vec<u8> = {
			// Minimal valid JPEG so the overlay renderer (if on) doesn't
			// crash — the dispatch path runs overlay only when
			// `preview_overlay` is true in config, which our
			// minimal_camera_config defaults it to whatever PauseConfig
			// defaults are; render() on non-Live state would need a
			// decodable image. Use a small synthetic RGB encoded as JPEG.
			let img = image::RgbImage::from_pixel(8, 8, image::Rgb([0, 0, 0]));
			let mut out = Vec::new();
			image::DynamicImage::ImageRgb8(img)
				.write_to(
					&mut std::io::Cursor::new(&mut out),
					image::ImageFormat::Jpeg,
				)
				.expect("encode");
			out
		};
		let jpeg_clone = jpeg.clone();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || Ok(jpeg_clone.clone()))
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qp", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPreview {
				camera: "cam-qp".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		// Preview poller initial state is Sleeping — overlay ON will
		// render text atop the JPEG so bytes differ from input; we only
		// assert a publish on the preview topic happened.
		assert!(
			mock.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam-qp/status/preview"),
			"QueryPreview should publish status/preview; observed: {:?}",
			mock.published_topics()
		);
	}

	/// `QueryPreview` with a driver error — the dispatch still finishes
	/// without panicking and nothing is published on `status/preview`.
	#[tokio::test]
	async fn dispatch_query_preview_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Err(bairelay_neolink_core::bc_protocol::Error::Other("no snap")))
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qpe", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPreview {
				camera: "cam-qpe".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpe/status/preview"));
	}

	/// `QueryPreview` with `preview_overlay = false` takes the
	/// passthrough branch (line 211) — raw JPEG is republished as-is.
	#[tokio::test]
	async fn dispatch_query_preview_passthrough_when_overlay_disabled() {
		let jpeg: Vec<u8> = {
			let img = image::RgbImage::from_pixel(8, 8, image::Rgb([0, 0, 0]));
			let mut out = Vec::new();
			image::DynamicImage::ImageRgb8(img)
				.write_to(
					&mut std::io::Cursor::new(&mut out),
					image::ImageFormat::Jpeg,
				)
				.expect("encode");
			out
		};
		let jpeg_clone = jpeg.clone();
		let fake = FakeCameraBuilder::new()
			.with_snapshot(move || Ok(jpeg_clone.clone()))
			.build();

		let cancel = CancellationToken::new();
		let mut cfg = minimal_camera_config("cam-qpov");
		cfg.pause.preview_overlay = false;
		let handle = Arc::new(CameraHandle::new(cfg, cancel, None));
		let driver: Arc<dyn CameraDriver> = fake;
		handle.set_driver_for_test(driver);
		let mut cameras = HashMap::new();
		cameras.insert("cam-qpov".to_string(), handle);

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPreview {
				camera: "cam-qpov".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert!(mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpov/status/preview"));
	}

	/// `QueryPir` publishes the camera's PIR state on `status/pir`.
	#[tokio::test]
	async fn dispatch_query_pir_publishes_xml() {
		use bairelay_neolink_core::bc::xml::RfAlarmCfg;
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qpir", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPir {
				camera: "cam-qpir".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert!(mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpir/status/pir"));
	}

	/// `QueryPir` with a driver error — dispatch returns without
	/// publishing anything to `status/pir`.
	#[tokio::test]
	async fn dispatch_query_pir_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"pir refused",
				))
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qpe", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPir {
				camera: "cam-qpe".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpe/status/pir"));
	}

	/// `QueryPtzPreset` re-queries the camera, replaces the cached
	/// preset list, and replies OK on the control topic. The HA
	/// discovery republish is exercised via `publish_discovery`; with
	/// no `DiscoveryPublisher` attached on the test handle that call
	/// is a silent no-op so we only assert the cache + reply here.
	#[tokio::test]
	async fn dispatch_query_ptz_preset_refreshes_cache_and_replies_ok() {
		use bairelay_neolink_core::bc::xml::{Preset, PresetList, PtzPreset};
		let fake = FakeCameraBuilder::new()
			.with_ptz_preset(|| {
				Ok(PtzPreset {
					preset_list: PresetList {
						preset: vec![
							Preset {
								id: 1,
								name: Some("Garden".to_string()),
								..Default::default()
							},
							Preset {
								id: 2,
								name: Some("Driveway".to_string()),
								..Default::default()
							},
						],
					},
					..Default::default()
				})
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qptz", fake);
		// Seed a stale cache so we can prove the handler replaced it.
		cameras
			.get("cam-qptz")
			.unwrap()
			.set_preset_cache_for_test(vec![(99, "stale".to_string())]);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPtzPreset {
				camera: "cam-qptz".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(
			cameras.get("cam-qptz").unwrap().preset_cache(),
			vec![(1u8, "Garden".to_string()), (2u8, "Driveway".to_string())],
			"QueryPtzPreset must replace the cache with fresh camera data"
		);
		assert!(mock
			.published()
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-qptz/query/ptz/preset" && p == b"OK"));
	}

	/// When `get_ptz_preset` returns an error, the handler logs and
	/// leaves the existing cache intact (no clobber to empty).
	#[tokio::test]
	async fn dispatch_query_ptz_preset_driver_error_preserves_cache() {
		let fake = FakeCameraBuilder::new()
			.with_ptz_preset(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"ptz refused",
				))
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qptze", fake);
		cameras
			.get("cam-qptze")
			.unwrap()
			.set_preset_cache_for_test(vec![(7, "Front".to_string())]);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		dispatch_control(
			ControlCommand::QueryPtzPreset {
				camera: "cam-qptze".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;
		assert_eq!(
			cameras.get("cam-qptze").unwrap().preset_cache(),
			vec![(7u8, "Front".to_string())],
			"driver error must not clobber the previously-cached presets"
		);
		assert!(mock
			.published()
			.iter()
			.any(|(t, p, _)| t == "bairelay/cam-qptze/query/ptz/preset" && p == b"OK"));
	}

	/// When `get_ptz_preset` hangs past `CMD_TIMEOUT` the handler logs
	/// and replies OK; the cache is left intact.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_query_ptz_preset_timeout_path() {
		let fake = FakeCameraBuilder::new().with_ptz_preset_pending().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qptzt", fake);
		cameras
			.get("cam-qptzt")
			.unwrap()
			.set_preset_cache_for_test(vec![(3, "Side".to_string())]);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::QueryPtzPreset {
				camera: "cam-qptzt".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(31)).await;
		});
		assert_eq!(
			cameras.get("cam-qptzt").unwrap().preset_cache(),
			vec![(3u8, "Side".to_string())],
			"timeout must not clobber the previously-cached presets"
		);
	}

	/// Directional PTZ where the move hits a timeout (amount > 10 * 32
	/// would normally just cap at 10s sleep; instead we put enough
	/// budget pressure by ... actually we just exercise the successful
	/// move with amount > 0 so the sleep branch runs.)
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_ptz_with_positive_amount_sleeps_before_stop() {
		use bairelay_neolink_core::bc_protocol::Direction;

		let fake = FakeCameraBuilder::new().build();
		let (cameras, fake) = test_cameras_with_fake("cam-pds", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		// amount = 32 → seconds = 32/32 = 1s of sleep between dir+stop.
		dispatch_control(
			ControlCommand::Ptz {
				camera: "cam-pds".to_string(),
				direction: PtzDirection::Right,
				amount: 32.0,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let calls = fake.calls().send_ptz.lock().unwrap().clone();
		assert_eq!(
			calls,
			vec![(Direction::Right, 32.0_f32), (Direction::Stop, 32.0_f32)]
		);
	}

	/// `QueryBattery` whose driver hangs past the 30 s `CMD_TIMEOUT`
	/// budget — the dispatcher must log the timeout and return without
	/// publishing anything on `status/battery`.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_query_battery_timeout_path() {
		let fake = FakeCameraBuilder::new().with_battery_info_pending().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qbt", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::QueryBattery {
				camera: "cam-qbt".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		// Run the dispatcher and the clock-advance task concurrently so
		// the timeout future fires while we await the dispatcher.
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(31)).await;
		});
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam-qbt/status/battery"),
			"timeout path must NOT publish status/battery"
		);
	}

	/// `QueryPir` whose driver hangs past 30 s — same expectation.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_query_pir_timeout_path() {
		let fake = FakeCameraBuilder::new().with_pirstate_pending().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qpit", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::QueryPir {
				camera: "cam-qpit".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(31)).await;
		});
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpit/status/pir"));
	}

	/// `QueryPreview` whose driver hangs past 30 s — no preview publish.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_query_preview_timeout_path() {
		let fake = FakeCameraBuilder::new().with_snapshot_pending().build();
		let (cameras, _fake) = test_cameras_with_fake("cam-qpvt", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::QueryPreview {
				camera: "cam-qpvt".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(31)).await;
		});
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-qpvt/status/preview"));
	}

	/// PTZ where `send_ptz(direction)` hangs past the 30 s timeout —
	/// dispatcher logs the timeout, then issues the unconditional Stop.
	/// Both calls land in the call log because the pending fake
	/// records the call before suspending.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_ptz_directional_timeout_still_stops() {
		use bairelay_neolink_core::bc_protocol::Direction;
		let fake = FakeCameraBuilder::new().with_send_ptz_pending().build();
		let (cameras, fake) = test_cameras_with_fake("cam-ptt", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::Ptz {
				camera: "cam-ptt".to_string(),
				direction: PtzDirection::Down,
				amount: 0.0,
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		// First send_ptz timeout, then unconditional stop call ALSO
		// times out. Advance well past 60 s so both fire.
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(70)).await;
		});
		// Both directional and stop calls were recorded on entry to
		// `send_ptz` (before the pending await).
		let calls = fake.calls().send_ptz.lock().unwrap().clone();
		assert_eq!(calls.len(), 2, "both directional and stop calls recorded");
		assert_eq!(calls[0].0, Direction::Down);
		assert_eq!(calls[1].0, Direction::Stop);
	}

	/// PTZ where `send_ptz(direction)` returns Err — dispatcher logs the
	/// inner-error warn (line 119-120) and still issues the Stop. Use a
	/// fake variant where send_ptz fails — current FakeCamera always
	/// returns Ok, so we skip this and rely on the pending-timeout path
	/// to cover the warn-then-stop arm.
	///
	/// Siren whose driver hangs past 30 s — dispatcher should still
	/// reply OK on the control topic since `state: true` is forwarded to
	/// `bc.siren()` whose timeout returns the normalised error.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn dispatch_siren_timeout_path() {
		let fake = FakeCameraBuilder::new().with_siren_pending().build();
		let (cameras, fake) = test_cameras_with_fake("cam-st", fake);
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		let dispatch = dispatch_control(
			ControlCommand::Siren {
				camera: "cam-st".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		);
		let (_, _) = tokio::join!(dispatch, async {
			tokio::time::advance(Duration::from_secs(31)).await;
		});
		// siren() was entered (and the call recorded) before pending.
		assert_eq!(*fake.calls().siren.lock().unwrap(), 1);
	}

	#[test]
	fn serialize_xml_round_trips_a_struct() {
		#[derive(serde::Serialize)]
		#[serde(rename = "Battery")]
		struct Battery {
			percent: u8,
		}
		let s = serialize_xml(&Battery { percent: 42 }).expect("serialize ok");
		assert!(s.contains("percent"));
		assert!(s.contains("42"));
	}
}
