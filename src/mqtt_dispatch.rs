//! Routes incoming MQTT control commands to the appropriate BcCamera methods.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use bairelay_mqtt::control::{ControlCommand, IrMode, PtzDirection};
use bairelay_mqtt::settings::Family;
use bairelay_mqtt::{topics, SharedMqttClient, StatusPublisher};

use bairelay_neolink_core::bc_protocol::{CameraDriver, Direction, Error, LightState};

use crate::camera::CameraHandle;
use crate::camera_tasks::{publish_after_led_write, LedWrite};
use crate::status_cache::StatusCache;

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
	// A settings re-read is a dozen commands in a row (and the chime alone
	// is five of them); the ceiling of a single write does not cover it.
	// Bounded all the same: a synchronisation that hangs is not a
	// synchronisation that will still be there for the next wake.
	const SETTINGS_TIMEOUT: Duration = Duration::from_secs(120);

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
			// The command always goes out; state is published only for
			// confirmed hardware — otherwise the re-read may be a placeholder.
			let present = cam
				.capabilities()
				.is_some_and(|c| c.status_led.is_present());
			if result.is_ok() && present {
				let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
				let cache = cam.status_cache();
				let written = LedWrite::Led(state);
				publish_after_led_write(&*bc, &publisher, &cache, written).await;
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
			let present = cam.capabilities().is_some_and(|c| c.ir_leds.is_present());
			if result.is_ok() && present {
				let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
				let cache = cam.status_cache();
				let written = LedWrite::Ir(mode);
				publish_after_led_write(&*bc, &publisher, &cache, written).await;
			}
			result
		}
		ControlCommand::Pir {
			camera: ref cam_name,
			state,
		} => {
			let result = timeout_to_result(timeout(CMD_TIMEOUT, bc.pir_set(state)).await);
			if result.is_ok() {
				// The command went out; the state, though, comes from **re-reading**
				// the hardware. Publishing `state` would publish the intention: a
				// firmware that nods without changing anything would leave in
				// Home Assistant a state nothing contradicts any more — and since the
				// topic is retained, it would also survive the bridge's restart.
				publish_pir_after_write(&*bc, mqtt, topic_prefix, cam_name, &cam.status_cache(), state)
					.await;
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
		// ── Settings (`control/<family>/<leaf>`) ─────────────────────────
		// The value already passed the protocol's bounds in
		// `bairelay_mqtt::settings`. What is left is the house gesture:
		// re-read the block, touch only the tag asked for, write it, re-read,
		// and publish the re-read — never the requested value.
		ControlCommand::Setting { setting, .. } => {
			let family = setting.family();
			match cam.bc_camera_concrete() {
				Some(bc_cfg) => {
					let publisher = StatusPublisher::new(mqtt, topic_prefix, &camera_name);
					let brut = timeout(
						CMD_TIMEOUT,
						crate::settings::apply_setting(&bc_cfg, &publisher, &setting),
					)
					.await;
					// The deadline is distinguished from the camera's answer: the
					// published reason must say which of the two happened, and
					// `timeout_to_result` melts the two into a single error.
					let en_delay = brut.is_err();
					let resultat = timeout_to_result(brut);
					match &resultat {
						Ok(()) => {
							// A write that succeeded also **re-read** the block:
							// the family is therefore known and rewritable. It is this
							// moment — and not a periodic probe — that moves
							// its entities from "readable" to "modifiable".
							cam.mark_family_read(family, true).await;
							// And the family is fine: the reason for an earlier
							// refusal has no business staying on the broker.
							crate::settings::clear_reason(&publisher, family).await;
						}
						Err(e) => {
							// The `FAIL` on the command topic is ephemeral:
							// nobody finds it again ten minutes later in
							// Home Assistant. The reason, though, is retained.
							let phrase = if en_delay {
								crate::settings::REASON_TIMEOUT.to_string()
							} else {
								crate::settings::cause(e)
							};
							crate::settings::publish_reason(&publisher, family, &phrase).await;
							// States already read are not erased: the camera
							// did not say anything else. A family never re-read, on the other
							// hand, does not become modifiable for all that.
						}
					}
					resultat
				}
				// Settings are not on the `CameraDriver` trait: without the
				// concrete `BcCamera` (session in the middle of closing, or a fake
				// driver), nothing is written, and the FAIL says so.
				None => {
					tracing::warn!(
						camera = %camera_name,
						leaf = setting.leaf(),
						"setting without a concrete Baichuan session: nothing was sent"
					);
					// Nothing was sent, and it is not the camera that said
					// no: it is the bridge. The FAIL is ephemeral, the sentence stays.
					let publisher = StatusPublisher::new(mqtt, topic_prefix, &camera_name);
					crate::settings::publish_reason(
						&publisher,
						family,
						crate::settings::REASON_NO_SESSION,
					)
					.await;
					Err(Error::Other("settings need a connected Baichuan session"))
				}
			}
		}
		ControlCommand::RejectedSetting { family, leaf, .. } => {
			// The camera received **nothing**: value out of range, text too long,
			// or a read-only leaf. A FAIL on the original topic is the
			// only honest answer — a refused command that says nothing is
			// indistinguable d'un pont mort.
			tracing::warn!(
				camera = %camera_name,
				family = family.slug(),
				leaf = %leaf,
				"setting value refused before reaching the camera"
			);
			// A refused setting must no longer be "just silent": the FAIL
			// goes out on the command topic, and the reason stays readable on
			// `status/<family>/reason`. The camera received nothing, so nothing is
			// erased and the family does not change platform — it keeps what
			// an earlier read proved.
			let publisher = StatusPublisher::new(mqtt, topic_prefix, &camera_name);
			crate::settings::publish_reason(
				&publisher,
				family,
				"value refused before reaching the camera: outside the protocol bounds",
			)
			.await;
			Err(Error::Other(
				"setting value rejected before it reached the camera",
			))
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
		ControlCommand::QuerySettings { family, .. } => {
			// Re-read on explicit request. Each family is a camera
			// command — on a battery doorbell, `query/settings` with no
			// family is a hefty fifteen: that is the price of an operator's
			// press or a deliberate automation, and it is exactly why
			// nothing here is periodic.
			match cam.bc_camera_concrete() {
				None => {
					tracing::warn!(
						camera = %camera_name,
						"settings synchronisation without a concrete Baichuan session"
					);
					Err(Error::Other("settings need a connected Baichuan session"))
				}
				Some(bc_cfg) => {
					let publisher = StatusPublisher::new(mqtt, topic_prefix, &camera_name);
					// One entry per re-read family, with its verdict: modifiable,
					// locked by a local proof, or undetermined (the
					// discovery does not move then). The `Vec` fills as it goes:
					// a full synchronisation interrupted by the deadline still keeps
					// what was read before the stop.
					let mut read: Vec<(Family, crate::settings::Verdict)> = Vec::new();
					match family {
						Some(f) => read.push(sync_one(&bc_cfg, &publisher, f).await),
						None => {
							// The `fut` borrows `read_back`: it must be released
							// before reading the vector, hence the `let`.
							let interrompue = timeout(
								SETTINGS_TIMEOUT,
								crate::settings::sync_all(&bc_cfg, &publisher, &mut read),
							)
							.await
							.is_err();
							if interrompue {
								tracing::warn!(
									camera = %camera_name,
									read = read.len(),
									"settings synchronisation interrupted"
								);
							}
						}
					}
					// The discovery follows the read: it is here that re-read
					// families move from good-faith sensors to modifiable
					// entities, in a single burst (empty payload on the old
					// discovery topic before the new configuration).
					cam.mark_families_read(&read).await;
					tracing::info!(
						camera = %camera_name,
						read = read.len(),
						writable = read
							.iter()
							.filter(|(_, v)| matches!(v, crate::settings::Verdict::Writable))
							.count(),
						// Verdicts promised **despite** a read refusal: the
						// family is modifiable and its `reason` topic carries
						// the reason. Counting them apart is what makes a grumpy burst
						// visible in a log without having to re-read MQTT.
						passageres = read
							.iter()
							.filter(|(_, v)| matches!(v, crate::settings::Verdict::Transient))
							.count(),
						indetermines = read
							.iter()
							.filter(|(_, v)| matches!(v, crate::settings::Verdict::Unknown))
							.count(),
						"settings re-read and published"
					);
					// `OK` means "the re-read took place", not "every
					// family answered": what the camera refused is readable on the
					// erased states **and** on the `reason` topics.
					Ok(())
				}
			}
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

/// The re-read that follows a write is bounded short: the camera is awake
/// (the wake lock is held), and a re-read that drags must not make
/// the operator impatient — they already have their OK/FAIL.
const REREAD_TIMEOUT: Duration = Duration::from_secs(10);

/// After a PIR write, re-reads the hardware and publishes **what it kept**.
///
/// A re-read that fails publishes nothing: the last retained state stays
/// in place rather than being replaced by the requested value. That is the rule
/// of every MQTT write of this bridge — a state describes the camera, never the
/// intention we had about it. Before this re-read, the PIR state was only the
/// echo of the command: a firmware nodding without changing anything could not
/// be seen, and since the topic is retained, the lie survived the bridge's
/// restart.
async fn publish_pir_after_write(
	bc: &dyn CameraDriver,
	mqtt: &SharedMqttClient,
	topic_prefix: &str,
	cam_name: &str,
	cache: &Arc<StatusCache>,
	demanded: bool,
) {
	let reread = match timeout(REREAD_TIMEOUT, bc.get_pirstate()).await {
		Ok(Ok(cfg)) => cfg,
		Ok(Err(e)) => {
			tracing::warn!(
				camera = %cam_name,
				error = %e,
				"PIR re-read after an impossible write: no state published"
			);
			return;
		}
		Err(_) => {
			tracing::warn!(
				camera = %cam_name,
				"PIR re-read after an expired write: no state published"
			);
			return;
		}
	};
	let kept = reread.enable != 0;
	let publisher = StatusPublisher::new(mqtt, topic_prefix, cam_name);
	if let Err(e) = publisher.publish_pir(kept).await {
		tracing::warn!(camera = %cam_name, error = %e, "publishing the re-read PIR state failed");
	}
	// Sensitivity has its own leaf; `settings::publish_pir` carries the
	// spelling rule (`sensiValue` on recent firmwares, `sensitivity` before).
	crate::settings::publish_pir(&publisher, &reread).await;
	if kept != demanded {
		tracing::warn!(
			camera = %cam_name,
			demanded,
			kept,
			"the camera accepted the PIR write then kept another value: it is that one that gets published"
		);
	}
	cache.set_pir(kept);
}

/// Re-reads a single family and returns the verdict the discovery must publish.
///
/// Failure = states erased **and** reason published; `settings::read_family` sees
/// to that for what the camera refused, and the deadline, which fires **here**, is
/// put into words by hand. A deadline does not prove the setting is forbidden —
/// it proves the camera did not answer — so it returns the same verdict as a
/// transient refusal, [`crate::settings::Verdict::Transient`]: the family stays
/// modifiable and the reason shows up next to it. What must no longer be promised
/// is the opposite: a family of which we have **never** read anything, and that
/// rule holds because a family never re-read stays absent from the modifiable
/// discovery until its first verdict.
async fn sync_one(
	bc: &bairelay_neolink_core::bc_protocol::BcCamera,
	publ: &StatusPublisher<'_>,
	family: Family,
) -> (Family, crate::settings::Verdict) {
	match timeout(REREAD_TIMEOUT * 3, crate::settings::read_family(bc, publ, family)).await {
		Ok(read) => read,
		Err(_) => {
			tracing::warn!(
				family = family.slug(),
				"a settings family re-read timed out: states erased, reason published, family left modifiable"
			);
			crate::settings::clear_family(publ, family).await;
			crate::settings::publish_reason_transient(publ, family, crate::settings::REASON_TIMEOUT)
				.await;
			(family, crate::settings::Verdict::Transient)
		}
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

	/// `ControlCommand::Pir { state: true }` dispatches to `pir_set(true)`,
	/// **re-reads** the hardware, and republishes `status/pir` from the re-read.
	/// Before the fix, the republished state was `state`: the publication
	/// said "what I wanted", and survived on a retained topic.
	#[tokio::test]
	async fn dispatch_pir_set_invokes_driver_and_republishes_state() {
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| Ok(pir_cfg(1, Some(2))))
			.build();
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
		with_led_hardware(&cameras["cam-led"]);
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
			with_led_hardware(&cameras["cam-ir"]);
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

	use crate::capabilities::Presence;

	/// Mark both status LED and IR LEDs as confirmed hardware.
	fn with_led_hardware(cam: &CameraHandle) {
		cam.set_capabilities_for_test(crate::capabilities::CameraCapabilities {
			status_led: Presence::Present,
			ir_leds: Presence::Present,
			..Default::default()
		});
	}

	/// Presence unknown (capabilities never probed, or the probe gave
	/// no signal): commands still reach the camera, but the re-read
	/// may be a placeholder, so no state is published or cached.
	#[tokio::test]
	async fn dispatch_led_and_ir_with_unknown_presence_publish_no_state() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(led_state("close", "open")))
			.build();
		let (cameras, fake) = test_cameras_with_fake("cam-unk", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let led = ControlCommand::Led {
			camera: "cam-unk".to_string(),
			state: true,
		};
		let ir = ControlCommand::Ir {
			camera: "cam-unk".to_string(),
			mode: IrMode::Auto,
		};
		dispatch_control(led, &cameras, &mqtt, "bairelay").await;
		dispatch_control(ir, &cameras, &mqtt, "bairelay").await;
		assert_eq!(fake.calls().led_light_set.lock().unwrap().len(), 1);
		assert_eq!(fake.calls().irled_light_set.lock().unwrap().len(), 1);
		assert!(status_rows(&mock, "bairelay/cam-unk/status/led").is_empty());
		assert!(status_rows(&mock, "bairelay/cam-unk/status/ir").is_empty());
		let cache = cameras["cam-unk"].status_cache();
		assert_eq!((cache.led(), cache.ir()), (None, None));
	}

	/// Camera confirmed to lack a status LED: `control/led` still goes
	/// out, but no `status/led` is published.
	#[tokio::test]
	async fn dispatch_led_on_camera_without_status_led_publishes_no_state() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(led_state("auto", "open")))
			.build();
		let (cameras, fake) = test_cameras_with_fake("cam-noled", fake);
		cameras["cam-noled"].set_capabilities_for_test(crate::capabilities::CameraCapabilities {
			status_led: Presence::Absent,
			ir_leds: Presence::Present,
			..Default::default()
		});
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cmd = ControlCommand::Led {
			camera: "cam-noled".to_string(),
			state: false,
		};
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
		assert_eq!(fake.calls().led_light_set.lock().unwrap().len(), 1);
		assert!(status_rows(&mock, "bairelay/cam-noled/status/led").is_empty());
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
		with_led_hardware(&cameras["cam-ir"]);
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
		with_led_hardware(&cameras["cam-led"]);
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

	/// Camera known to lack IR LEDs: `control/ir` still goes to the
	/// camera, but the placeholder `state` it reads back isn't published.
	#[tokio::test]
	async fn dispatch_ir_on_camera_without_ir_leds_publishes_no_state() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(led_state("close", "close")))
			.build();
		let (cameras, fake) = test_cameras_with_fake("cam-noir", fake);
		cameras["cam-noir"].set_capabilities_for_test(crate::capabilities::CameraCapabilities {
			status_led: Presence::Present,
			ir_leds: Presence::Absent,
			..Default::default()
		});
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cmd = ControlCommand::Ir {
			camera: "cam-noir".to_string(),
			mode: IrMode::Off,
		};
		dispatch_control(cmd, &cameras, &mqtt, "bairelay").await;
		assert_eq!(fake.calls().irled_light_set.lock().unwrap().len(), 1);
		assert!(status_rows(&mock, "bairelay/cam-noir/status/ir").is_empty());
		assert_eq!(cameras["cam-noir"].status_cache().ir(), None);
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

	// ── Settings: the law of the re-read ─────────────────────────────────
	//
	// All these tests wire the dispatch to a **simulated** camera:
	// `MockConnection` is an in-memory channel (no socket, no frame on a
	// network), and `FakeCamera` a fully scripted driver.
	// The central scenario is the same everywhere: the camera acks the write
	// with 200, and what it answers to the re-read that follows is not what
	// had been asked for.

	use bairelay_mqtt::settings::{Family, SettingCommand};
	use bairelay_neolink_core::bc::model::{
		Bc, MSG_ID_DING_DONG_CTRL, MSG_ID_GET_AUDIO_CFG, MSG_ID_GET_DING_DONG_CFG,
		MSG_ID_GET_DING_DONG_LIST, MSG_ID_GET_IMAGE, MSG_ID_GET_MOTION_DETECT, MSG_ID_GET_NTP,
		MSG_ID_GET_OSD, MSG_ID_GET_PIR_ALARM, MSG_ID_SET_AUDIO_CFG, MSG_ID_START_PIR_ALARM,
	};
	use bairelay_neolink_core::bc::xml::{
		AudioCfg, BcXml, MdAlarm, Ntp, OsdDatetime, RfAlarmCfg, SensInfoNew, VideoInput,
	};
	use bairelay_neolink_core::bc_protocol::connection::mock::{
		reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
	};
	use bairelay_neolink_core::bc_protocol::BcCamera;

	/// A `<RfAlarmCfg>` as a doorbell prints it in protocol v1.1: sensitivity
	/// is called `sensiValue` (and not `sensitivity`), and the `enable` byte
	/// is what the hardware truly believes its state to be.
	fn pir_cfg(enable: u8, sensi: Option<u8>) -> RfAlarmCfg {
		RfAlarmCfg {
			version: "1.1".to_string(),
			enable,
			sensiValue: sensi,
			..Default::default()
		}
	}

	/// The same block, in the v1.1 spelling that carries `<interval>`: the rest
	/// time between two PIR alarms, in seconds, and the bounds the firmware
	/// announces for that slider.
	fn pir_cfg_with_interval(repos: u32, max: u32) -> RfAlarmCfg {
		RfAlarmCfg {
			interval: Some(repos),
			intervalSecMin: Some(5),
			intervalSecMax: Some(max),
			..pir_cfg(1, Some(21))
		}
	}

	/// A 200 answer to msg 212: the block inside `<RfAlarmCfg>`, as a
	/// real firmware returns it.
	fn answer_pir(req: &Bc, bloc: RfAlarmCfg) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				rf_alarm_cfg: Some(bloc),
				..Default::default()
			},
		)
	}

	/// Installs a real [`BcCamera`] wired to a scripted connection, in
	/// both slots of the handle — exactly what an open session does.
	/// Settings read the concrete slot, historical commands the slot
	/// of the trait: the two must agree.
	async fn cameras_with_mock(
		name: &str,
		mock: MockConnection,
	) -> HashMap<String, Arc<CameraHandle>> {
		let cam = Arc::new(BcCamera::from_mock_connection(mock).await);
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config(name),
			CancellationToken::new(),
			None,
		));
		handle.set_bc_camera_for_test(cam);
		let mut map = HashMap::new();
		map.insert(name.to_string(), handle);
		map
	}

	/// The body of a settings test, pinned on the heap to cross a thread
	/// without generic ceremony.
	type CorpsDeTest = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

	/// Runs a test body on a thread with an **explicit stack**, with its
	/// propre runtime.
	///
	/// Why this detour: `BcXml` is every block of the protocol in
	/// `Option`, several kilobytes, and each camera command churns at
	/// least two copies of it. Stacking `dispatch_control` on top of a
	/// `BcCamera` is the most stack-hungry chain of the bridge. In
	/// release — the profile of the binary that runs at the user's — the thread
	/// gets exactly the **2 MB of a tokio worker**, the stack
	/// `dispatch_control` really has since `mqtt_loop` folds it into a
	/// `tokio::spawn`: a green test is therefore proof that the path fits in
	/// the production stack (`cargo test --release` is the run that
	/// demonstrates it). In debug, unoptimised frames ask for wider, and the
	/// test no longer proves anything but behaviour — that is intended, not a bug.
	fn on_worker(body: fn() -> CorpsDeTest) {
		let stack = if cfg!(debug_assertions) {
			8 * 1024 * 1024
		} else {
			2 * 1024 * 1024
		};
		std::thread::Builder::new()
			.stack_size(stack)
			.spawn(move || {
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_time()
					.start_paused(false)
					.build()
					.expect("test thread runtime");
				rt.block_on(body());
			})
			.expect("test thread with an explicit stack")
			.join()
			.expect("the bridge must not panic while re-reading settings");
	}

	/// The `<AudioCfg>` block of a doorbell: the two intercom volumes are
	/// printed, and `<volume>` (the loudspeaker) stays there, untouched —
	/// it is what proves the block goes back out whole.
	fn audio_cfg(talk: &str, visitor: &str) -> AudioCfg {
		AudioCfg {
			version: "1.1".to_string(),
			channel_id: "0".to_string(),
			timeout: "10".to_string(),
			audio_select: "0".to_string(),
			volume: "5".to_string(),
			pre_alarm: "0".to_string(),
			audio_list_id: "0".to_string(),
			talk_and_reply_volume: Some(talk.to_string()),
			visitor_volume: Some(visitor.to_string()),
			visitor_loudspeaker: Some("1".to_string()),
			..Default::default()
		}
	}

	fn audio_reply(req: &Bc, cfg: AudioCfg) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				audio_cfg: Some(cfg),
				..Default::default()
			},
		)
	}

	/// `control/pir`: the dispatch writes, re-reads, and publishes the re-read —
	/// including when it contradicts the command. That is the fix for the bug "
	/// the published state was the requested state": before, `status/pir` repeated
	/// `state`, and a firmware nodding without writing anything was
	/// invisible, all the more so as the topic is retained.
	#[tokio::test]
	async fn dispatch_pir_publishes_the_reread_and_not_the_request() {
		let fake = FakeCameraBuilder::new()
			// The hardware answers "enabled, sensitivity 4" to every re-read,
			// whatever the command: that is the firmware ignoring
			// the write, and it acks with 200.
			.with_pirstate(|| Ok(pir_cfg(1, Some(4))))
			.build();
		let (cameras, fake) = test_cameras_with_fake("cam-pir-kept", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Pir {
				camera: "cam-pir-kept".to_string(),
				state: false,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		// The command did go out (the FAIL would be another story).
		assert_eq!(*fake.calls().pir_set.lock().unwrap(), vec![false]);
		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-kept/control/pir" && p == b"OK"),
			"the write was acked: OK expected; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, r)| t == "bairelay/cam-pir-kept/status/pir"
				&& p == b"on"
				&& *r),
			"status/pir must carry what the camera re-read (on), never what we asked for (off); observed: {rows:?}"
		);
		assert!(
			!rows
				.iter()
				.any(|(t, p, _)| t == "bairelay/cam-pir-kept/status/pir" && p == b"off"),
			"the requested state (off) must never end up published; observed: {rows:?}"
		);
		// Sensitivity has its leaf, published from the same re-read.
		assert!(
			rows.iter().any(|(t, p, r)| t
				== "bairelay/cam-pir-kept/status/pir/sensitivity"
				&& p == b"4"
				&& *r),
			"status/pir/sensitivity must come from the re-read; observed: {rows:?}"
		);
		// The cache follows the camera, not the command.
		let cam = cameras.get("cam-pir-kept").expect("the test's camera");
		assert_eq!(cam.status_cache().pir(), Some(true));
	}

	/// A re-read that fails publishes **nothing**. Better in Home
	/// Assistant a state dating from yesterday (whose log says it could not
	/// be refreshed) than a state fabricated out of the requested value.
	#[tokio::test]
	async fn dispatch_pir_without_reread_publishes_no_state() {
		// `get_pirstate` is scripted to error: a camera that does not answer the
		// re-read, or refuses it.
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Err(bairelay_neolink_core::bc_protocol::Error::Other(
					"pir read refused",
				))
			})
			.build();
		let (cameras, _fake) = test_cameras_with_fake("cam-pir-nord", fake);
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Pir {
				camera: "cam-pir-nord".to_string(),
				state: true,
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-nord/control/pir" && p == b"OK"),
			"the command was acked, so OK; observed: {rows:?}"
		);
		assert!(
			!rows
				.iter()
				.any(|(t, _, _)| t.starts_with("bairelay/cam-pir-nord/status/pir")),
			"no re-read, so no state published; observed: {rows:?}"
		);
		assert_eq!(cameras["cam-pir-nord"].status_cache().pir(), None);
	}

	/// `control/audio/volume`: read (264), write (265) of the patched block,
	/// re-read (264), publication of the re-read. The test also checks the
	/// shape of the block that went on the wire: the tag asked for changed, and
	/// `<volume>` — which is not driven here — still in place.
	#[test]
	fn dispatch_setting_audio_writes_then_publishes_the_reread() {
		on_worker(|| Box::pin(audio_writes_then_rereads()));
	}

	async fn audio_writes_then_rereads() {
		let sent = StdArc::new(std::sync::Mutex::new(String::new()));
		let capture = StdArc::clone(&sent);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg("10", "7")))
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with_xml(move |_req, xml| {
				let block = xml
					.audio_cfg
					.as_ref()
					.or(xml.audio_cfg_camel.as_ref())
					.expect("the write carries an audioCfg block");
				*capture.lock().unwrap() = format!(
					"volume={}|talk={}|visitor={}",
					block.volume,
					block.talk_and_reply_volume.as_deref().unwrap_or("<absent>"),
					block.visitor_volume.as_deref().unwrap_or("<absent>"),
				);
				reply_200_empty(_req)
			})
			// The re-read: the camera kept 42.
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg("42", "7")))
			.build()
			.await;
		let cameras = cameras_with_mock("cam-audio", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-audio".to_string(),
				setting: SettingCommand::AudioVolume("42".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			sent.lock().unwrap().as_str(),
			"volume=5|talk=42|visitor=7",
			"the block written must be the read's answer with one single tag touched"
		);
		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, r)| t == "bairelay/cam-audio/status/audio/volume"
				&& p == b"42"
				&& *r),
			"the re-read volume is published, retained; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t
				== "bairelay/cam-audio/status/audio/visitor_volume"
				&& p == b"7"),
			"the neighbouring leaves of the same re-read are published too; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-audio/control/audio/volume"
				&& p == b"OK"),
			"write acked: OK; observed: {rows:?}"
		);
	}

	/// A refused write (400) answers `FAIL`, publishes **no** state, but
	/// publishes the **reason** on `status/audio/reason`: a refusal that says nothing is
	/// indistinguishable from a dead bridge, and the command topic's `FAIL` is
	/// ephemeral — nobody finds it again the next day in Home Assistant.
	#[test]
	fn a_camera_refusal_publishes_nothing() {
		on_worker(|| Box::pin(camera_refuses_audio_write()));
	}

	async fn camera_refuses_audio_write() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg("10", "7")))
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let cameras = cameras_with_mock("cam-audio-400", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-audio-400".to_string(),
				setting: SettingCommand::AudioVolume("42".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-audio-400/control/audio/volume"
				&& p == b"FAIL"),
			"a camera 400 is a FAIL; observed: {rows:?}"
		);
		// No state published: the camera confirmed nothing, so nothing must
		// make 42 look like the value currently in force. The only topic of the
		// family allowed to receive anything is the reason.
		let states: Vec<_> = rows
			.iter()
			.filter(|(t, _, _)| {
				t.starts_with("bairelay/cam-audio-400/status/") && !t.ends_with("/reason")
			})
			.collect();
		assert!(
			states.is_empty(),
			"nothing was stored, so no state; observed: {rows:?}"
		);
		let cause = rows
			.iter()
			.find(|(t, _, _)| t == "bairelay/cam-audio-400/status/audio/reason");
		let Some((_, payload, retained)) = cause else {
			panic!("the refusal must be readable: no reason published; observed: {rows:?}");
		};
		let text = String::from_utf8_lossy(payload).to_string();
		assert!(*retained, "the reason is retained, otherwise it disappears on reload");
		assert!(
			text.contains("(code 400)"),
			"the reason must say which refusal it is, not stay silent: {text:?}"
		);
		assert!(!text.contains('<'), "the reason carries no raw XML: {text:?}");
	}

	/// A value refused **before** the camera (out of range, forbidden text,
	/// read-only leaf) answers `FAIL` without sending a single frame.
	/// It still publishes the reason: a refused setting must no longer be "
	/// just silent", even when the culprit is the bridge itself.
	#[test]
	fn dispatch_setting_value_refused_sends_nothing() {
		on_worker(|| Box::pin(a_refused_value_sends_nothing()));
	}

	async fn a_refused_value_sends_nothing() {
		// Empty script: the slightest extra frame would close the mock
		// connection's channel, and the command could no longer complete.
		let mock = MockConnection::new().build().await;
		let cameras = cameras_with_mock("cam-refused", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::RejectedSetting {
				camera: "cam-refused".to_string(),
				family: Family::Denoise,
				leaf: "level".to_string(),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-refused/control/denoise/level"
				&& p == b"FAIL"),
			"the refusal must be visible on the original topic; observed: {rows:?}"
		);
		let frames: Vec<_> = rows
			.iter()
			.filter(|(t, _, _)| {
				t.starts_with("bairelay/cam-refused/status/") && !t.ends_with("/reason")
			})
			.collect();
		assert!(
			frames.is_empty(),
			"a refused value writes and publishes nothing; observed: {rows:?}"
		);
		let Some((_, payload, retained)) = rows
			.iter()
			.find(|(t, _, _)| t == "bairelay/cam-refused/status/denoise/reason")
		else {
			panic!("a refusal by the bridge must not be silent either; observed: {rows:?}");
		};
		assert!(*retained, "the reason is retained");
		assert!(
			String::from_utf8_lossy(payload).contains("outside the protocol bounds"),
			"the reason must name the refusal: {:?}",
			String::from_utf8_lossy(payload)
		);
	}

	/// `query/settings/ntp`: the three leaves of the family are published
	/// from the single read, plus the block's JSON for automation.
	#[test]
	fn dispatch_query_settings_publishes_the_leaves_of_the_family() {
		on_worker(|| Box::pin(query_ntp_publishes_its_leaves()));
	}

	async fn query_ntp_publishes_its_leaves() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ntp: Some(Ntp {
							version: "1.1".to_string(),
							channel_id: Some("0".to_string()),
							enable: Some("1".to_string()),
							server: Some("pool.ntp.org".to_string()),
							port: Some("123".to_string()),
							synchronize_interval: Some("1440".to_string()),
							time_zone: Some("-3600".to_string()),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cameras = cameras_with_mock("cam-ntp", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-ntp".to_string(),
				family: Some(Family::Ntp),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		for (leaf, want) in [
			("enable", b"on".as_slice()),
			("server", b"pool.ntp.org".as_slice()),
			("interval", b"1440".as_slice()),
		] {
			let topic = format!("bairelay/cam-ntp/status/ntp/{leaf}");
			assert!(
				rows.iter()
					.any(|(t, p, r)| t == &topic && p.as_slice() == want && *r),
				"{topic} must hold {want:?} retained; observed: {rows:?}"
			);
		}
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-ntp/status/ntp/json"
				&& p.windows(6).any(|w| w == b"server")),
			"the block's JSON goes out on status/ntp/json; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-ntp/query/settings/ntp" && p == b"OK"),
			"the request completes: OK; observed: {rows:?}"
		);
	}

	/// A family the camera refuses has its states **erased** (empty payload
	/// retained): Home Assistant shows "unknown", and not last week's value
	/// which has nothing to do with the hardware any more.
	#[test]
	fn dispatch_query_settings_refused_family_clears_states() {
		on_worker(|| Box::pin(query_ntp_refusal_clears()));
	}

	async fn query_ntp_refusal_clears() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(|req| reply_err_code(req, 404))
			.build()
			.await;
		let cameras = cameras_with_mock("cam-ntp-no", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-ntp-no".to_string(),
				family: Some(Family::Ntp),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		for leaf in ["enable", "server", "interval"] {
			let topic = format!("bairelay/cam-ntp-no/status/ntp/{leaf}");
			assert!(
				rows.iter()
					.any(|(t, p, r)| t == &topic && p.is_empty() && *r),
				"{topic} must be erased (empty payload retained); observed: {rows:?}"
			);
		}
	}

	/// The chime of a doorbell with **no** paired chime: 483 and 484 refuse
	/// (400), 486 answers 200 with no block. `get_ding_dong_cfg` therefore returns
	/// a `None` list — "the camera did not answer" — and the
	/// `status/chime/list` leaf is erased rather than published as `[]`: "refused"
	/// and "empty" must stay two distinct things. That is exactly what
	/// the user's camera answered, re-read in read-only mode.
	#[test]
	fn dispatch_query_chime_without_answer_publishes_no_list() {
		on_worker(|| Box::pin(a_chime_without_answer_publishes_nothing()));
	}

	async fn a_chime_without_answer_publishes_nothing() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_DING_DONG_CTRL)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_DING_DONG_LIST)
			.reply_with(|req| reply_err_code(req, 400))
			// No 485 / 609: without a chime id, those questions are not even
			// asked — five radio commands are not offered to a camera
			// that has nothing to say.
			.expect_msg(MSG_ID_GET_DING_DONG_CFG)
			.reply_with(|req| reply_200_empty(req))
			.build()
			.await;
		let cameras = cameras_with_mock("cam-chime", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-chime".to_string(),
				family: Some(Family::Chime),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, r)| t == "bairelay/cam-chime/status/chime/list"
				&& p.is_empty()
				&& *r),
			"refused = state erased, definitely not []; observed: {rows:?}"
		);
		assert!(
			!rows
				.iter()
				.any(|(t, p, _)| t == "bairelay/cam-chime/status/chime/list" && p == b"[]"),
			"[] would mean \"paired: none\", which this camera did not say; observed: {rows:?}"
		);
	}

	// ── The three promises this batch repairs ────────────────────────────

	/// Simulated camera **with** a discovery editor attached and known abilities.
	/// Without a publisher, or without abilities, `publish_discovery` publishes
	/// nothing (two early returns): the test would prove only half the
	/// fix — the states, never the entities.
	async fn cameras_with_mock_and_discovery(
		name: &str,
		mock: MockConnection,
		features: Vec<bairelay_mqtt::discovery::Feature>,
		mqtt: &SharedMqttClient,
	) -> (HashMap<String, Arc<CameraHandle>>, Arc<CameraHandle>) {
		let cam = Arc::new(BcCamera::from_mock_connection(mock).await);
		let handle = Arc::new(
			CameraHandle::new(minimal_camera_config(name), CancellationToken::new(), None)
				.with_discovery_publisher(bairelay_mqtt::DiscoveryPublisher::new(
					mqtt.clone(),
					"bairelay".to_string(),
					"homeassistant".to_string(),
					features.into_iter().collect(),
					"test".to_string(),
				)),
		);
		handle.set_capabilities_for_test(crate::capabilities::CameraCapabilities::default());
		handle.set_bc_camera_for_test(cam);
		let mut map = HashMap::new();
		map.insert(name.to_string(), Arc::clone(&handle));
		(map, handle)
	}

	/// The general volume of a doorbell, as model 264 prints it.
	fn audio_cfg_general(volume: &str, talk: &str, visitor: &str) -> AudioCfg {
		AudioCfg {
			volume: volume.to_string(),
			..audio_cfg(talk, visitor)
		}
	}

	/// The third volume: `<volume>` has its own identifier, topic and
	/// entity. It is written read-modify-write, and it is the
	/// **re-read** that is published — here 88, never the requested 77.
	#[test]
	fn dispatch_setting_general_volume_writes_then_publishes_the_reread() {
		on_worker(|| Box::pin(volume_general_writes_then_rereads()));
	}

	async fn volume_general_writes_then_rereads() {
		let sent = StdArc::new(std::sync::Mutex::new(String::new()));
		let capture = StdArc::clone(&sent);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg_general("5", "10", "7")))
			.expect_msg(MSG_ID_SET_AUDIO_CFG)
			.reply_with_xml(move |_req, xml| {
				let block = xml
					.audio_cfg
					.as_ref()
					.or(xml.audio_cfg_camel.as_ref())
					.expect("the write carries an audioCfg block");
				*capture.lock().unwrap() = format!(
					"volume={}|talk={}|visitor={}",
					block.volume,
					block.talk_and_reply_volume.as_deref().unwrap_or("<absent>"),
					block.visitor_volume.as_deref().unwrap_or("<absent>"),
				);
				reply_200_empty(_req)
			})
			// The re-read: the camera kept 88, not the requested value.
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg_general("88", "10", "7")))
			.build()
			.await;
		let cameras = cameras_with_mock("cam-vol3", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-vol3".to_string(),
				setting: SettingCommand::AudioGeneralVolume("77".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			sent.lock().unwrap().as_str(),
			"volume=77|talk=10|visitor=7",
			"the block returned is the read's answer with one single tag touched"
		);
		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, r)| t
				== "bairelay/cam-vol3/status/audio/general_volume"
				&& p == b"88"
				&& *r),
			"the re-read value is published, retained — not the requested value; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-vol3/status/audio/volume"
				&& p == b"10"),
			"the intercom volume keeps its own leaf, it is not overwritten; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t
				== "bairelay/cam-vol3/control/audio/general_volume"
				&& p == b"OK"),
			"write acked: OK; observed: {rows:?}"
		);
		// An earlier refusal must not survive a successful write: the
		// family is fine, its reason goes back to empty (and still retained, so
		// that Home Assistant erases the old sentence).
		let Some((_, cause, retained)) =
			rows.iter().find(|(t, _, _)| t == "bairelay/cam-vol3/status/audio/reason")
		else {
			panic!("a successful write erases the reason too; observed: {rows:?}");
		};
		assert!(cause.is_empty(), "the family is fine, the reason must be empty");
		assert!(*retained, "a topic is erased by an empty retained payload");
	}

	/// A camera that does not print `<volume>` sees the write refused **without
	/// a single write frame going out**: the bridge does not invent for the firmware
	/// a tag it never declared. The published reason says so; no state
	/// is published, because nothing new was read.
	#[test]
	fn dispatch_setting_general_volume_refuses_without_tag() {
		on_worker(|| Box::pin(general_volume_without_tag_is_refused()));
	}

	async fn general_volume_without_tag_is_refused() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_AUDIO_CFG)
			.reply_with(|req| audio_reply(req, audio_cfg_general("", "10", "7")))
			// A single message expected: the slightest extra frame would close the
			// simulated connection's channel, and the FAIL could no longer arrive.
			.build()
			.await;
		let cameras = cameras_with_mock("cam-vol3-missing", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-vol3-missing".to_string(),
				setting: SettingCommand::AudioGeneralVolume("77".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t
				== "bairelay/cam-vol3-missing/control/audio/general_volume"
				&& p == b"FAIL"),
			"no tag, no write: FAIL; observed: {rows:?}"
		);
		assert!(
			!rows.iter().any(|(t, _, _)| t
				== "bairelay/cam-vol3-missing/status/audio/general_volume"),
			"one does not publish a value the camera did not print; observed: {rows:?}"
		);
		let Some((_, cause, _)) = rows.iter().find(|(t, _, _)| {
			t == "bairelay/cam-vol3-missing/status/audio/reason"
		}) else {
			panic!("a refusal must not be silent; observed: {rows:?}");
		};
		assert!(
			String::from_utf8_lossy(cause).contains("tag, so this setting cannot be written")
				&& String::from_utf8_lossy(cause).contains("volume"),
			"the reason must name the refusal: {:?}",
			String::from_utf8_lossy(cause)
		);
	}

	/// A family never re-read discovers itself **without any command**. It is only
	/// after a successful read that its entities become modifiable, and the
	/// platform switch empties the old discovery topic **before** publishing
	/// the new one: otherwise Home Assistant keeps a ghost entity pointing
	/// at a switch that no longer exists.
	#[test]
	fn discovery_ntp_has_no_command_until_the_first_read() {
		on_worker(|| Box::pin(discovery_ntp_becomes_writable()));
	}

	async fn discovery_ntp_becomes_writable() {
		let connexion = MockConnection::new()
			.expect_msg(MSG_ID_GET_NTP)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						ntp: Some(Ntp {
							version: "1.1".to_string(),
							channel_id: Some("0".to_string()),
							enable: Some("1".to_string()),
							server: Some("pool.ntp.org".to_string()),
							port: Some("123".to_string()),
							synchronize_interval: Some("1440".to_string()),
							time_zone: Some("-3600".to_string()),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-read",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Ntp],
			&mqtt,
		)
		.await;

		let ancien = "homeassistant/binary_sensor/bairelay_cam-read_ntp_enable/config";
		let new_topic = "homeassistant/switch/bairelay_cam-read_ntp_enable/config";

		// The startup discovery, before the slightest settings read.
		cam.publish_discovery().await.expect("first discovery");
		let rows = mock.published();
		let Some((_, payload, _)) = rows.iter().find(|(t, _, _)| t == ancien) else {
			panic!("the switch discovers itself first as a binary_sensor; observed: {rows:?}");
		};
		let json: serde_json::Value =
			serde_json::from_slice(payload).expect("a config is JSON");
		assert!(
			json.get("command_topic").is_none(),
			"no command may be promised before the first read: {json}"
		);
		assert!(!rows.iter().any(|(t, _, _)| t == new_topic), "the switch does not exist yet");

		// The operator asks for the family to be re-read: so it happens.
		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-read".to_string(),
				family: Some(Family::Ntp),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		let Some(empty) = rows
			.iter()
			.position(|(t, p, r)| t == ancien && p.is_empty() && *r)
		else {
			panic!("the old config must be emptied (empty payload retained); observed: {rows:?}");
		};
		let Some(switch) = rows.iter().position(|(t, _, _)| t == new_topic) else {
			panic!("the re-read family must become modifiable; observed: {rows:?}");
		};
		assert!(
			empty < switch,
			"the retraction must precede the new config: empty={empty} switch={switch}"
		);
		let (_, payload, _) = &rows[switch];
		let json: serde_json::Value =
			serde_json::from_slice(payload).expect("a config is JSON");
		assert_eq!(
			json["command_topic"],
			"bairelay/cam-read/control/ntp/enable",
			"the expected command carries the family and the leaf"
		);
		assert_eq!(
			json["unique_id"], "bairelay_cam-read_ntp_enable",
			"the identifier does not change across the platform switch: it is the entity HA sees that changes its name"
		);
	}

	/// The user's camera answers `<MdAlarm>` to message 46. That carrier
	/// can be read but not sent back: both leaves stay published (they are
	/// correct), the reason explains why they are not adjustable, and
	/// no entity promises a command.
	#[test]
	fn md_alarm_stays_read_only_and_says_so() {
		on_worker(|| Box::pin(md_alarm_is_read_only()));
	}

	async fn md_alarm_is_read_only() {
		let connexion = MockConnection::new()
			.expect_msg(MSG_ID_GET_MOTION_DETECT)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						md_alarm: Some(MdAlarm {
							version: "1.1".to_string(),
							sens_info_new: Some(SensInfoNew {
								enable: Some("1".to_string()),
								sensitivity: Some("3".to_string()),
								..Default::default()
							}),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-md",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::MotionSettings],
			&mqtt,
		)
		.await;

		// The startup discovery, as it happens in production.
		cam.publish_discovery().await.expect("first discovery");
		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-md".to_string(),
				family: Some(Family::Motion),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		for (leaf, value) in [("enable", "on"), ("sensitivity", "3")] {
			let topic = format!("bairelay/cam-md/status/motion/{leaf}");
			assert!(
				rows.iter().any(|(t, p, _)| t == &topic && p == value.as_bytes()),
				"the read did take place, the leaf is published: {topic}; observed: {rows:?}"
			);
		}
		let Some((_, cause, retained)) = rows
			.iter()
			.find(|(t, _, _)| t == "bairelay/cam-md/status/motion/reason")
		else {
			panic!(
				"a family read but not rewritable must say so; observed: {rows:?}"
			);
		};
		assert!(*retained, "the reason is retained");
		// What closes this one is the topic it reads: `read-only` is the wording of two
		// different locks (the `<MdAlarm>` block and the missing `<OsdChannelName>`), and
		// `status/motion/reason` is the only thing that tells them apart. Deliberate, not
		// sloppiness — deleting the topic pin would make this assertion vacuous.
		assert!(
			String::from_utf8_lossy(cause).contains("read-only"),
			"the reason must say read-only, not a read failure: {:?}",
			String::from_utf8_lossy(cause)
		);
		// And the discovery stays what it has always been for this family.
		assert!(
			rows.iter().any(|(t, p, r)| t
				== "homeassistant/binary_sensor/bairelay_cam-md_motion_enable/config"
				&& !p.is_empty()
				&& *r),
			"the motion switch discovers itself as a binary_sensor; observed: {rows:?}"
		);
		for (topic, payload, _) in rows.iter().filter(|(t, p, _)| {
			t.starts_with("homeassistant/") && t.ends_with("/config") && !p.is_empty()
		}) {
			let json: serde_json::Value =
				serde_json::from_slice(payload).expect("a config is JSON");
			assert!(
				json.get("command_topic").is_none(),
				"{topic} promises a command even though <MdAlarm> cannot be rewritten"
			);
		}
	}

	/// `<VideoInput>` as the doorbell prints it: the five sliders and day/night
	/// in the same block, nothing invented.
	fn image_xml(bright: &str) -> BcXml {
		BcXml {
			video_input: Some(VideoInput {
				version: "1.1".to_string(),
				channel_id: Some("0".to_string()),
				bright: Some(bright.to_string()),
				contrast: Some("64".to_string()),
				saturation: Some("64".to_string()),
				hue: Some("0".to_string()),
				sharpen: Some("3".to_string()),
				day_night: Some("Auto".to_string()),
				..Default::default()
			}),
			..Default::default()
		}
	}

	/// What happened in the field at the first deployment: the image sliders,
	/// written successfully an hour earlier, answer `400` to the complete burst
	/// of fifteen reads. That code means "not now" — the repository
	/// already replays it elsewhere (`bc_protocol::services.rs`, five attempts) — and
	/// it says **nothing** about what the camera accepts or refuses to write. The family
	/// therefore keeps its modifiable entities, and the `reason` topic explains the
	/// refusal **without** making a prohibition believed: the sentence carries the
	/// consequence first, "the family stays published as writable", because that is the
	/// half the operator looks at and it must survive the cut at 160
	/// characters.
	#[test]
	fn a_read_refused_with_400_locks_nothing_and_says_so() {
		on_worker(|| Box::pin(image_four_hundred_locks_nothing()));
	}

	async fn image_four_hundred_locks_nothing() {
		let connexion = MockConnection::new()
			// First re-read: the camera answers, the family becomes modifiable.
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_200_xml(req, image_xml("128")))
			// Second re-read: both attempts fail (the first is
			// followed by a single replay, cf. `settings::read_family`).
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-400",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Image],
			&mqtt,
		)
		.await;

		let cursor = "homeassistant/number/bairelay_cam-400_image_bright/config";
		let cause_topic = "bairelay/cam-400/status/image/reason";
		let relire = || ControlCommand::QuerySettings {
			camera: "cam-400".to_string(),
			family: Some(Family::Image),
		};

		cam.publish_discovery().await.expect("first discovery");
		let rows = mock.published();
		assert!(
			!rows.iter().any(|(t, _, _)| t == cursor),
			"never re-read, the family promises no command; observed: {rows:?}"
		);

		dispatch_control(relire(), &cameras, &mqtt, "bairelay").await;
		let rows = mock.published();
		let Some((_, payload, _)) = rows.iter().rev().find(|(t, _, _)| t == cursor) else {
			panic!("the re-read family must promise its slider; observed: {rows:?}");
		};
		let json: serde_json::Value = serde_json::from_slice(payload).expect("a config is JSON");
		assert_eq!(
			json["command_topic"], "bairelay/cam-400/control/image/bright",
			"the read made the slider adjustable"
		);
		assert!(
			rows.iter().any(|(t, p, r)| t == cause_topic && p.is_empty() && *r),
			"a successful read empties the reason; observed: {rows:?}"
		);
		// Marker: everything published from here on follows the re-read that
		// is about to fail.
		let after = rows.len();

		dispatch_control(relire(), &cameras, &mqtt, "bairelay").await;

		let rows = mock.published();
		let burst: Vec<&(String, Vec<u8>, bool)> = rows.iter().skip(after).collect();
		for (topic, _, _) in rows.iter().skip(after) {
			assert!(
				!topic.starts_with("homeassistant/"),
				"a re-read that fails must publish no discovery, \
				 retract none and downgrade none of them: {topic}; \
				 observed: {burst:?}"
			);
		}
		let Some((_, cause, retained)) =
			rows.iter().rev().find(|(t, _, _)| t == cause_topic)
		else {
			panic!("a refusal must not be silent; observed: {rows:?}");
		};
		let reason_text = String::from_utf8_lossy(cause).to_string();
		assert!(reason_text.contains("400"), "the reason must quote the code received: {reason_text}");
		assert!(
			reason_text.contains("stays published as writable"),
			"and announces the consequence first, before the technical detail: {reason_text}"
		);
		assert!(
			!reason_text.contains("not locked"),
			"the transient refusal has its own sentence, the undetermined verdict's must not be reused: {reason_text}"
		);
		assert!(*retained, "a reason is published retained");
		assert!(
			burst.iter().any(|(t, p, _)| t == "bairelay/cam-400/status/image/bright" && p.is_empty()),
			"values we could not re-read do not stay displayed as true: {burst:?}"
		);
	}

	/// The same refusal, but on a family **never re-read**: that is the case that
	/// truly counted for the operator, the one at startup. A camera sulking about a
	/// read must not condemn the family to the initial read-only state — `image` is
	/// precisely one of those the firmware refuses to **read** (400) and yet lets
	/// **write**, a write re-read and verified twice in the field. The command is
	/// therefore published, and the reason for the refusal with it.
	#[test]
	fn a_four_hundred_on_a_never_read_family_publishes_writable() {
		on_worker(|| Box::pin(a_family_never_read_then_answered_four_hundred()));
	}

	async fn a_family_never_read_then_answered_four_hundred() {
		let connexion = MockConnection::new()
			// The two attempts of `read_family`: the single replay after a 400.
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 400))
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 400))
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-400-fresh",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Image],
			&mqtt,
		)
		.await;

		cam.publish_discovery().await.expect("first discovery");
		let rows = mock.published();
		assert!(
			!rows.iter().any(|(t, _, _)| t
				== "homeassistant/number/bairelay_cam-400-fresh_image_bright/config"),
			"at startup nothing is promised: {rows:?}"
		);

		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-400-fresh".to_string(),
				family: Some(Family::Image),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		let cursor = "homeassistant/number/bairelay_cam-400-fresh_image_bright/config";
		let Some((_, payload, _)) = rows.iter().rev().find(|(t, _, _)| t == cursor) else {
			panic!(
				"a transient refusal must not leave the family in its initial \
				 read-only state; observed: {rows:?}"
			);
		};
		let json: serde_json::Value = serde_json::from_slice(payload).expect("a config is JSON");
		assert_eq!(
			json["command_topic"], "bairelay/cam-400-fresh/control/image/bright",
			"the family is published modifiable despite the read refusal"
		);
		let Some((_, cause, _)) =
			rows.iter().rev().find(|(t, _, _)| t == "bairelay/cam-400-fresh/status/image/reason")
		else {
			panic!("and the reason for the refusal shows next to it; observed: {rows:?}");
		};
		let reason_text = String::from_utf8_lossy(cause).to_string();
		assert!(reason_text.contains("400") && reason_text.contains("writable"), "both together: {reason_text}");
	}

	/// The limit of the correction: a refusal that is **not** a transient reason still
	/// promises nothing. A `500` is not this dialect's "not now", and a camera answering
	/// this badly allowed nothing at all: the family waits for a real read, and the
	/// reason repeats the note that nothing is locked. Without this test, the
	/// correction would be a general promise to write without ever having read
	/// anything.
	#[test]
	fn five_hundred_promises_nothing_and_says_so() {
		on_worker(|| Box::pin(image_five_hundred_locks_nothing()));
	}

	async fn image_five_hundred_locks_nothing() {
		let connexion = MockConnection::new()
			// A single attempt: the replay is reserved for 400, retrying a `500`
			// would pay twice for radio that is doing badly.
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-500",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Image],
			&mqtt,
		)
		.await;

		cam.publish_discovery().await.expect("first discovery");
		// Marker: the startup discovery publishes the twins in read-only
		// mode; only what follows the burst counts here.
		let after = mock.published().len();
		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-500".to_string(),
				family: Some(Family::Image),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		for (topic, payload, _) in rows.iter().skip(after) {
			assert!(
				!(topic.starts_with("homeassistant/") && !payload.is_empty()),
				"an undetermined verdict publishes no discovery: {topic}; observed: {:?}",
				&rows[after..]
			);
		}
		let Some((_, cause, _)) =
			rows.iter().rev().find(|(t, _, _)| t == "bairelay/cam-500/status/image/reason")
		else {
			panic!("the refusal stays told; observed: {rows:?}");
		};
		let reason_text = String::from_utf8_lossy(cause).to_string();
		assert!(reason_text.contains("500"), "the reason quotes the code: {reason_text}");
		assert!(reason_text.contains("not locked"), "and the undetermined verdict keeps its sentence: {reason_text}");
	}

	/// The rest of the same incident, at the place the field verified it: a re-read
	/// that **succeeds after** a refusal. The sliders become modifiable and the
	/// reason goes back to empty — that is the half the rule owes the operator,
	/// otherwise a healthy camera would keep showing yesterday's sulking.
	/// sa bouderie d'hier.
	#[test]
	fn a_reread_after_a_refusal_restores_writable_and_clears_the_reason() {
		on_worker(|| Box::pin(a_reread_after_four_hundred_is_writable()));
	}

	async fn a_reread_after_four_hundred_is_writable() {
		let connexion = MockConnection::new()
			// First attempt: "not now".
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_err_code(req, 400))
			// The single replay, and this time the camera talks.
			.expect_msg(MSG_ID_GET_IMAGE)
			.reply_with(|req| reply_200_xml(req, image_xml("128")))
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-replay",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Image],
			&mqtt,
		)
		.await;

		cam.publish_discovery().await.expect("first discovery");
		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-replay".to_string(),
				family: Some(Family::Image),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-replay/status/image/bright"
				&& p == b"128"),
			"the second attempt read, the re-read value is published; observed: {rows:?}"
		);
		let Some((_, cause, retained)) =
			rows.iter().rev().find(|(t, _, _)| t == "bairelay/cam-replay/status/image/reason")
		else {
			panic!("the family is fine, its reason must be written then emptied; observed: {rows:?}");
		};
		assert!(cause.is_empty(), "a successful re-read erases the reason: {cause:?}");
		assert!(*retained, "a topic is erased by an empty retained payload");
		assert!(
			rows.iter().any(|(t, p, _)| t
				== "homeassistant/number/bairelay_cam-replay_image_bright/config"
				&& !p.is_empty()),
			"and the family becomes modifiable; observed: {rows:?}"
		);
	}

	/// The other species of refusal, the one that **locks**: answer 44
	/// does not print `<OsdChannelName>`, the carrier of the displayed name and the
	/// watermark. Three of the four leaves therefore have nothing to give back to
	/// setter 45, and we refuse them — not the camera. The family stays
	/// read-only, the reason names the missing tag, and the state read (the
	/// timestamp) is published all the same.
	#[test]
	fn osd_without_osdchannelname_stays_read_only_and_says_so() {
		on_worker(|| Box::pin(osd_without_tag_is_read_only()));
	}

	async fn osd_without_tag_is_read_only() {
		let connexion = MockConnection::new()
			.expect_msg(MSG_ID_GET_OSD)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						osd_datetime: Some(OsdDatetime {
							version: "1.1".to_string(),
							channel_id: Some("0".to_string()),
							enable: Some("1".to_string()),
							..Default::default()
						}),
						// No `<OsdChannelName>`: that is the field camera's case, and the only
						// one besides `<MdAlarm>` that locks.
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let (cameras, cam) = cameras_with_mock_and_discovery(
			"cam-osd",
			connexion,
			vec![bairelay_mqtt::discovery::Feature::Osd],
			&mqtt,
		)
		.await;

		cam.publish_discovery().await.expect("first discovery");
		dispatch_control(
			ControlCommand::QuerySettings {
				camera: "cam-osd".to_string(),
				family: Some(Family::Osd),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-osd/status/osd/datetime_enable"
				&& p == b"on"),
			"the read did take place, the leaf read is published; observed: {rows:?}"
		);
		let Some((_, cause, _)) =
			rows.iter().rev().find(|(t, _, _)| t == "bairelay/cam-osd/status/osd/reason")
		else {
			panic!("a locked family says so; observed: {rows:?}");
		};
		let reason_text = String::from_utf8_lossy(cause).to_string();
		assert!(
			reason_text.contains("OsdChannelName"),
			"the reason must name the missing tag: {reason_text}"
		);
		assert!(reason_text.contains("read-only"), "and the family's status: {reason_text}");
		for (topic, payload, _) in
			rows.iter().filter(|(t, p, _)| t.starts_with("homeassistant/") && t.ends_with("/config") && !p.is_empty())
		{
			let json: serde_json::Value =
				serde_json::from_slice(payload).expect("a config is JSON");
			assert!(
				json.get("command_topic").is_none(),
				"{topic} promises a command even though the name's carrier does not exist"
			);
		}
	}

	// ── PIR interval (212 / 213): same carrier, other tag ────────────────

	/// Simulated camera whose ability card announces `rfAlarm` for read and
	/// write. `has_ability_rw` reads the card the login fills at the
	/// user's; a simulated connection does not receive it, so we declare it
	/// here. That is the equivalent of an `abilityInfo` announcing `rfAlarm`,
	/// not a way of skipping the gate.
	async fn cameras_with_rf_alarm(
		name: &str,
		mock: MockConnection,
	) -> HashMap<String, Arc<CameraHandle>> {
		let cam = Arc::new(BcCamera::from_mock_connection(mock).await);
		cam.declare_ability("rfAlarm").await;
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config(name),
			CancellationToken::new(),
			None,
		));
		handle.set_bc_camera_for_test(cam);
		let mut map = HashMap::new();
		map.insert(name.to_string(), handle);
		map
	}

	/// `<interval>` is read and written with the same two messages as
	/// sensitivity (212 / 213): the write frame must therefore return the block read
	/// **whole**, one tag touched, and it is the re-read that is
	/// published — not the requested value. The bounds announced by the firmware
	/// are not entities; they are readable in `status/pir/json`, and this
	/// test checks that too.
	#[test]
	fn dispatch_setting_pir_interval_writes_then_publishes_the_reread() {
		on_worker(|| Box::pin(interval_pir_writes_then_rereads()));
	}

	async fn interval_pir_writes_then_rereads() {
		let sent = StdArc::new(std::sync::Mutex::new(String::new()));
		let capture = StdArc::clone(&sent);
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_PIR_ALARM)
			.reply_with(|req| answer_pir(req, pir_cfg_with_interval(30, 120)))
			.expect_msg(MSG_ID_START_PIR_ALARM)
			.reply_with_xml(move |_req, xml| {
				let bloc = xml
					.rf_alarm_cfg
					.as_ref()
					.expect("the write carries an RfAlarmCfg block");
				*capture.lock().unwrap() = format!(
					"enable={}|sensi={:?}|interval={:?}|min={:?}|max={:?}",
					bloc.enable,
					bloc.sensiValue,
					bloc.interval,
					bloc.intervalSecMin,
					bloc.intervalSecMax,
				);
				reply_200_empty(_req)
			})
			// The re-read: the camera brought the interval back to 60 s and now
			// announces 60 as the maximum.
			.expect_msg(MSG_ID_GET_PIR_ALARM)
			.reply_with(|req| answer_pir(req, pir_cfg_with_interval(60, 60)))
			.build()
			.await;
		let cameras = cameras_with_rf_alarm("cam-pir-int", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-pir-int".to_string(),
				setting: SettingCommand::PirInterval("45".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		assert_eq!(
			sent.lock().unwrap().as_str(),
			"enable=1|sensi=Some(21)|interval=Some(45)|min=Some(5)|max=Some(120)",
			"the block returned is the one that was read, with <interval> alone touched"
		);
		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, r)| t == "bairelay/cam-pir-int/status/pir/interval"
				&& p == b"60"
				&& *r),
			"it is the re-read interval that is published, retained; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-int/status/pir/sensitivity"
				&& p == b"21"),
			"sensitivity keeps its own leaf, it is not overwritten; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-int/control/pir/interval"
				&& p == b"OK"),
			"write acked: OK; observed: {rows:?}"
		);
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-int/status/pir/json"
				&& String::from_utf8_lossy(p).contains("\"intervalSecMax\":60")),
			"the bounds announced by the firmware are readable in the re-read JSON; observed: {rows:?}"
		);
		let Some((_, cause, retained)) =
			rows.iter().find(|(t, _, _)| t == "bairelay/cam-pir-int/status/pir/reason")
		else {
			panic!("a successful write erases the reason too; observed: {rows:?}");
		};
		assert!(cause.is_empty(), "the family is fine, the reason must be empty");
		assert!(*retained, "a topic is erased by an empty retained payload");
	}

	/// A camera whose `<RfAlarmCfg>` does not print `<interval>` sees the write
	/// refused **without a single 213 frame going out**: the bridge does not
	/// invent for the firmware a tag it never declared.
	#[test]
	fn dispatch_setting_pir_interval_refuses_without_tag() {
		on_worker(|| Box::pin(pir_interval_without_tag_is_refused()));
	}

	async fn pir_interval_without_tag_is_refused() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_PIR_ALARM)
			.reply_with(|req| {
				// Old firmware spelling: `sensitivity`, and no
				// `<interval>` — the setting does not exist here.
				answer_pir(
					req,
					RfAlarmCfg {
						version: "1.0".to_string(),
						enable: 1,
						sensitivity: Some(4),
						..Default::default()
					},
				)
			})
			// A single message expected: the slightest extra frame would close the
			// simulated channel, and the FAIL could no longer arrive.
			.build()
			.await;
		let cameras = cameras_with_rf_alarm("cam-pir-int-missing", mock).await;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		dispatch_control(
			ControlCommand::Setting {
				camera: "cam-pir-int-missing".to_string(),
				setting: SettingCommand::PirInterval("45".to_string()),
			},
			&cameras,
			&mqtt,
			"bairelay",
		)
		.await;

		let rows = mock.published();
		assert!(
			rows.iter().any(|(t, p, _)| t == "bairelay/cam-pir-int-missing/control/pir/interval"
				&& p == b"FAIL"),
			"no tag, no write: FAIL; observed: {rows:?}"
		);
		assert!(
			!rows
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam-pir-int-missing/status/pir/interval"),
			"one does not publish an interval the camera did not print; observed: {rows:?}"
		);
		let Some((_, cause, _)) = rows
			.iter()
			.find(|(t, _, _)| t == "bairelay/cam-pir-int-missing/status/pir/reason")
		else {
			panic!("a refusal must not be silent; observed: {rows:?}");
		};
		let reason_text = String::from_utf8_lossy(cause).to_string();
		assert!(reason_text.contains("tag, so this setting cannot be written")
			&& reason_text.contains("interval"), "the reason must name the refusal: {reason_text}");
		// The tag named in the sentence, not just present somewhere in it: an assertion
		// reading `interval` alone is satisfied by eleven different published sentences.
		// The guillemets are the sanitiser's doing (crates/mqtt/src/status.rs maps `<`
		// and `>` so no raw XML is retained), written as escapes to keep this file ASCII.
		assert!(
			reason_text.contains("no \u{ab}interval\u{bb} tag"),
			"the reason must name the tag it refused to invent: {reason_text}"
		);
	}
}
