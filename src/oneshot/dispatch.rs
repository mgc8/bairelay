//! Pure dispatch of a parsed [`Command`] to the matching one-shot
//! subcommand body. Extracted from `main::run_oneshot` so the 16
//! match arms can be unit-tested against a `FakeCamera` without
//! standing up `runner::run` + a live TCP socket.
//!
//! The two service-mode variants (`Mqtt` / `Rtsp` / `MqttRtsp`) are
//! filtered upstream by `Cli::is_oneshot()` before we get here; if one
//! leaks through it's a bug in the CLI layer and the match arm returns
//! a usage error instead of unwinding.

use std::path::Path;

use anyhow::Result;
use bairelay_neolink_core::bc_protocol::CameraDriver;

use crate::cli::{Command, FloodlightState, OnOff, PtzCommand};
use crate::cli_convert::{
	clone_ptz_cmd, clone_service_action, clone_user_action, ptz_direction_to_core,
	service_name_to_core,
};
use crate::oneshot::{
	abilities, battery, errors::UsageError, floodlight, output::Outcome, pir, presets, ptz, reboot,
	services, set_time, siren, snapshot, status_light, users, version, webhook,
};

/// Pre-flight check for `snapshot --json`: the JSON status line and
/// the JPEG bytes both target stdout, so writing both would truncate
/// the image. Catch this up-front so bad invocations exit in ms
/// rather than after a 30-second connect timeout.
///
/// Returns `Err` only when `json_mode = true` AND the command is a
/// `Snapshot` with no `--output` path. Every other combination is
/// valid and returns `Ok(())`.
pub fn snapshot_json_preflight(json_mode: bool, cmd: &Command) -> Result<()> {
	if json_mode {
		if let Command::Snapshot { output: None, .. } = cmd {
			return Err(UsageError::new(
				"snapshot --json requires --output <path> (can't mix JSON and bytes on stdout)",
			)
			.into());
		}
	}
	Ok(())
}

/// Look up `camera_name` in `config.cameras`. Returns a cloned
/// [`crate::config::CameraConfig`] on match, `Err(UsageError)` when
/// the name isn't present.
///
/// Maps to `EXIT_USAGE = 2` per the table in `docs/architecture.md` §
/// Error handling: an unknown camera name is an operator typo, not a
/// malformed config. `EXIT_CONFIG = 3` is reserved for "config file
/// present but malformed" — the camera-not-found case shares the
/// `check-config` precedent of treating "operator pointed somewhere
/// that doesn't exist" as usage.
pub fn find_camera_config(
	config: &crate::config::Config,
	camera_name: &str,
) -> Result<crate::config::CameraConfig> {
	use crate::oneshot::errors::UsageError;
	config
		.cameras
		.iter()
		.find(|c| c.name == camera_name)
		.cloned()
		.ok_or_else(|| {
			UsageError::new(format!("camera '{}' not found in config", camera_name)).into()
		})
}

/// Run the single one-shot body for `cmd` against `cam`.
///
/// `json` is threaded through solely to feed the `snapshot` pre-flight
/// check (JSON mode without `--output` refuses to run — mixing JSON
/// status and JPEG bytes on stdout truncates the payload).
///
/// Returns the command's [`Outcome`] on success, or any error the
/// underlying subcommand body produced. Errors flow up to
/// `main::run_oneshot` for exit-code classification.
pub async fn dispatch_oneshot(
	cam: &dyn CameraDriver,
	cmd: &Command,
	json: bool,
) -> Result<Outcome> {
	match cmd {
		Command::Reboot(_) => reboot::run(cam).await,
		Command::Battery(_) => battery::run(cam).await,
		Command::Version(_) => version::run(cam).await,
		Command::Siren(_) => siren::run(cam).await,
		Command::Abilities(_) => abilities::run(cam).await,
		Command::Webhook(_) => webhook::run(cam).await,
		Command::Presets(_) => presets::run(cam).await,
		Command::SetTime(_) => set_time::run(cam).await,
		Command::Snapshot {
			output,
			use_stream,
			use_stream_raw,
			..
		} => {
			if *use_stream {
				// Neolink-compat no-op: the operator asked for
				// stream-sourced JPEG, but every battery camera we
				// support has the `snap` ability, which produces a
				// JPEG directly. Tell them once, then delegate.
				tracing::info!(
					"--use-stream is a neolink-compat no-op on bairelay (battery cams all support `snap`); \
					delegating to get_snapshot. Use --use-stream-raw if you want NAL bytes."
				);
			}
			let path: Option<&Path> = output.as_deref();
			snapshot::run_via_driver(cam, path, json, *use_stream_raw).await
		}
		Command::Floodlight { state, .. } => {
			let set = state.as_ref().map(|s| matches!(s, FloodlightState::On));
			floodlight::run(cam, set).await
		}
		Command::Pir { state, .. } => {
			let set = state.as_ref().map(|s| matches!(s, OnOff::On));
			pir::run(cam, set).await
		}
		Command::StatusLight { state, .. } => {
			let set = state.as_ref().map(|s| matches!(s, OnOff::On));
			status_light::run(cam, set).await
		}
		Command::Ptz { cmd, .. } => {
			let cmd = clone_ptz_cmd(cmd);
			match cmd {
				PtzCommand::Preset { preset_id } => ptz::preset(cam, preset_id).await,
				PtzCommand::Assign { preset_id, name } => ptz::assign(cam, preset_id, name).await,
				PtzCommand::Control {
					amount,
					direction,
					speed,
				} => ptz::control(cam, ptz_direction_to_core(direction), amount, speed).await,
				PtzCommand::Zoom { amount } => ptz::zoom(cam, amount).await,
			}
		}
		Command::Services {
			service, action, ..
		} => {
			// Bare `services <cam>` → list all six; anything else
			// goes through the single-service path with the usual
			// get-default treatment of an absent action.
			match service {
				None => services::run_all(cam).await,
				Some(svc) => {
					let svc = service_name_to_core(*svc);
					let action = clone_service_action(action);
					services::run(cam, svc, action).await
				}
			}
		}
		Command::Users { action, .. } => {
			// `clone_user_action` resolves any omitted password via TTY
			// prompt or stdin (see `oneshot::password_source`). That
			// blocks the current task on stdin/getch, which is fine in
			// the one-shot path — there's no concurrent work to starve.
			let action = clone_user_action(action)?;
			users::run(cam, action).await
		}
		Command::Mqtt | Command::Rtsp { .. } | Command::MqttRtsp { .. } => {
			// Reaching this arm means upstream (Cli::is_oneshot,
			// run_oneshot's service-mode short-circuit) failed to
			// route a service-mode command. That's a programmer bug,
			// not a user mistake — exit code 1 (EXIT_UNEXPECTED), not
			// the usage-error 2 the previous shape produced. Plain
			// `anyhow::anyhow!` deliberately bypasses every classifier
			// arm in `classify::classify` so it falls through to the
			// "unknown" branch.
			Err(anyhow::anyhow!(
				"internal: service-mode command routed through oneshot dispatch"
			))
		}
		Command::CheckConfig => {
			// Same shape as above: `run_support::run_oneshot`
			// short-circuits `check-config` before reaching
			// `dispatch_oneshot`; if we got here, the early-return was
			// bypassed. Programmer bug → EXIT_UNEXPECTED.
			Err(anyhow::anyhow!(
				"internal: check-config routed through camera dispatch"
			))
		}
		Command::CloudAuthorise { .. } => {
			// `run_oneshot` short-circuits `cloud-authorise` (no camera)
			// before dispatch; reaching here means that was bypassed.
			Err(anyhow::anyhow!(
				"internal: cloud-authorise routed through camera dispatch"
			))
		}
		Command::RenderHassioConfig { .. } => {
			// `main::async_main` dispatches `render-hassio-config`
			// before the one-shot pipeline runs; reaching this arm
			// means the early-return was bypassed. Programmer bug.
			Err(anyhow::anyhow!(
				"internal: render-hassio-config routed through camera dispatch"
			))
		}
	}
}

#[cfg(test)]
mod tests {
	//! Cover every dispatch arm against a `FakeCamera`. Each test
	//! builds the minimal `Command` payload for its variant, scripts
	//! the matching FakeCamera method (read or setter), and asserts
	//! on the returned `Outcome` + `FakeCalls` log.

	use super::*;
	use crate::cli::{
		OneShotArgs, PtzCommand, PtzDirection, ServiceAction, ServiceName, UserAction,
	};
	use bairelay_neolink_core::bc::xml::{
		BatteryInfo, HttpPort, HttpsPort, LedState, OnvifPort, PtzPreset, RfAlarmCfg, RtmpPort,
		RtspPort, ServerPort, UserList, VersionInfo,
	};
	use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

	fn args() -> OneShotArgs {
		OneShotArgs {
			camera: "cam1".to_string(),
		}
	}

	#[tokio::test]
	async fn dispatch_reboot_runs_reboot_branch() {
		let fake = FakeCameraBuilder::new().build();
		let out = dispatch_oneshot(&*fake, &Command::Reboot(args()), false)
			.await
			.unwrap();
		assert_eq!(out, Outcome::Reboot);
		assert_eq!(*fake.calls().reboot.lock().unwrap(), 1);
	}

	#[tokio::test]
	async fn dispatch_battery_runs_battery_branch() {
		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Ok(BatteryInfo {
					battery_percent: 50,
					voltage: 3800,
					charge_status: "none".into(),
					low_power: 0,
					..Default::default()
				})
			})
			.build();
		let out = dispatch_oneshot(&*fake, &Command::Battery(args()), false)
			.await
			.unwrap();
		match out {
			Outcome::Battery { percent, .. } => assert_eq!(percent, 50),
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn dispatch_version_runs_version_branch() {
		let fake = FakeCameraBuilder::new()
			.with_version(|| Ok(VersionInfo::default()))
			.build();
		let out = dispatch_oneshot(&*fake, &Command::Version(args()), false)
			.await
			.unwrap();
		assert!(matches!(out, Outcome::Version { .. }));
	}

	#[tokio::test]
	async fn dispatch_siren_runs_siren_branch() {
		let fake = FakeCameraBuilder::new().build();
		let out = dispatch_oneshot(&*fake, &Command::Siren(args()), false)
			.await
			.unwrap();
		assert_eq!(out, Outcome::Siren);
		assert_eq!(*fake.calls().siren.lock().unwrap(), 1);
	}

	#[tokio::test]
	async fn dispatch_presets_runs_presets_branch() {
		let fake = FakeCameraBuilder::new()
			.with_ptz_preset(|| Ok(PtzPreset::default()))
			.build();
		let out = dispatch_oneshot(&*fake, &Command::Presets(args()), false)
			.await
			.unwrap();
		assert!(matches!(out, Outcome::Presets { .. }));
	}

	#[tokio::test]
	async fn dispatch_set_time_runs_set_time_branch() {
		let fake = FakeCameraBuilder::new().build();
		let out = dispatch_oneshot(&*fake, &Command::SetTime(args()), false)
			.await
			.unwrap();
		assert!(matches!(out, Outcome::SetTime { .. }));
		assert_eq!(fake.calls().set_time.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_snapshot_runs_snap_path() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"JPEG".to_vec()))
			.build();
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let cmd = Command::Snapshot {
			common: args(),
			output: Some(path.clone()),
			use_stream: false,
			use_stream_raw: false,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		match out {
			Outcome::Snapshot { bytes, format, .. } => {
				assert_eq!(bytes, 4);
				assert_eq!(format, "jpeg");
			}
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn dispatch_snapshot_use_stream_logs_and_still_snaps() {
		let fake = FakeCameraBuilder::new()
			.with_snapshot(|| Ok(b"X".to_vec()))
			.build();
		let tmp = tempfile::NamedTempFile::new().unwrap();
		let path = tmp.path().to_path_buf();
		let cmd = Command::Snapshot {
			common: args(),
			output: Some(path),
			use_stream: true, // exercises the neolink-compat log branch
			use_stream_raw: false,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::Snapshot { .. }));
	}

	#[tokio::test]
	async fn dispatch_snapshot_use_stream_raw_rejected_via_driver() {
		let fake = FakeCameraBuilder::new().build();
		let cmd = Command::Snapshot {
			common: args(),
			output: None,
			use_stream: false,
			use_stream_raw: true, // driver-only harness rejects this
		};
		let err = dispatch_oneshot(&*fake, &cmd, false).await.unwrap_err();
		assert!(format!("{err:#}").contains("use-stream-raw requires a concrete BcCamera"));
	}

	#[tokio::test]
	async fn dispatch_floodlight_read_runs_read_branch() {
		use bairelay_neolink_core::bc::xml::{FloodlightStatus, FloodlightStatusList};
		let (tx, rx) = tokio::sync::mpsc::channel(1);
		tx.send(FloodlightStatusList {
			floodlight_status_list: vec![FloodlightStatus {
				channel_id: 0,
				status: 1,
			}],
			..Default::default()
		})
		.await
		.unwrap();
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let cmd = Command::Floodlight {
			common: args(),
			state: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::Floodlight { .. }));
	}

	#[tokio::test]
	async fn dispatch_floodlight_on_runs_set_branch() {
		use bairelay_neolink_core::bc::xml::FloodlightStatusList;
		let (tx, rx) = tokio::sync::mpsc::channel(1);
		tx.send(FloodlightStatusList::default()).await.unwrap();
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let cmd = Command::Floodlight {
			common: args(),
			state: Some(FloodlightState::On),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().set_floodlight_manual.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_pir_read_runs_read_branch() {
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			.build();
		let cmd = Command::Pir {
			common: args(),
			state: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::Pir { .. }));
	}

	#[tokio::test]
	async fn dispatch_pir_set_runs_set_branch() {
		// pir::run also reads back state after set, so the fake must
		// supply pirstate too.
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			.build();
		let cmd = Command::Pir {
			common: args(),
			state: Some(OnOff::On),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(*fake.calls().pir_set.lock().unwrap(), vec![true]);
	}

	#[tokio::test]
	async fn dispatch_webhook_runs_read_branch() {
		let fake = FakeCameraBuilder::new().with_ha_cfg(|| Ok(None)).build();
		let out = dispatch_oneshot(&*fake, &Command::Webhook(args()), false)
			.await
			.unwrap();
		assert!(matches!(
			out,
			Outcome::Webhook {
				supported: false,
				..
			}
		));
	}

	#[tokio::test]
	async fn dispatch_status_light_read_runs_read_branch() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(LedState::default()))
			.build();
		let cmd = Command::StatusLight {
			common: args(),
			state: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::StatusLight { .. }));
	}

	#[tokio::test]
	async fn dispatch_status_light_set_runs_set_branch() {
		let fake = FakeCameraBuilder::new()
			.with_ledstate(|| Ok(LedState::default()))
			.build();
		let cmd = Command::StatusLight {
			common: args(),
			state: Some(OnOff::Off),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(!fake.calls().led_light_set.lock().unwrap().is_empty());
	}

	#[tokio::test]
	async fn dispatch_ptz_preset_runs_preset_branch() {
		let fake = FakeCameraBuilder::new()
			.with_ptz_preset(|| Ok(PtzPreset::default()))
			.build();
		let cmd = Command::Ptz {
			common: args(),
			cmd: Some(PtzCommand::Preset { preset_id: None }),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
	}

	#[tokio::test]
	async fn dispatch_ptz_assign_runs_assign_branch() {
		let fake = FakeCameraBuilder::new().build();
		let cmd = Command::Ptz {
			common: args(),
			cmd: Some(PtzCommand::Assign {
				preset_id: 1,
				name: "spot".into(),
			}),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().set_ptz_preset.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_ptz_control_runs_send_branch() {
		let fake = FakeCameraBuilder::new().build();
		let cmd = Command::Ptz {
			common: args(),
			cmd: Some(PtzCommand::Control {
				amount: 1,
				direction: PtzDirection::Up,
				speed: Some(32),
			}),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().send_ptz.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_ptz_zoom_runs_zoom_branch() {
		let fake = FakeCameraBuilder::new().build();
		let cmd = Command::Ptz {
			common: args(),
			cmd: Some(PtzCommand::Zoom { amount: 1500.0 }),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().zoom_to.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_services_none_runs_run_all_branch() {
		let fake = FakeCameraBuilder::new()
			.with_serverport(|| Ok(ServerPort::default()))
			.with_http(|| Ok(HttpPort::default()))
			.with_https(|| Ok(HttpsPort::default()))
			.with_rtsp(|| Ok(RtspPort::default()))
			.with_rtmp(|| Ok(RtmpPort::default()))
			.with_onvif(|| Ok(OnvifPort::default()))
			.build();
		let cmd = Command::Services {
			common: args(),
			service: None,
			action: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::ServiceList { .. }));
	}

	#[tokio::test]
	async fn dispatch_services_some_runs_single_service_branch() {
		let fake = FakeCameraBuilder::new()
			.with_http(|| Ok(HttpPort::default()))
			.build();
		let cmd = Command::Services {
			common: args(),
			service: Some(ServiceName::Http),
			action: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::Service { .. }));
	}

	#[tokio::test]
	async fn dispatch_services_with_action_runs_set_path() {
		let fake = FakeCameraBuilder::new()
			.with_http(|| Ok(HttpPort::default()))
			.build();
		let cmd = Command::Services {
			common: args(),
			service: Some(ServiceName::Http),
			action: Some(ServiceAction::On),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().set_http.lock().unwrap().len(), 1);
	}

	#[tokio::test]
	async fn dispatch_abilities_runs_abilities_branch() {
		use bairelay_neolink_core::bc::xml::{AbilityInfo, AbilityInfoSubModule, AbilityInfoToken};
		let fake = FakeCameraBuilder::new()
			.with_abilityinfo(|| {
				Ok(AbilityInfo {
					username: "admin".into(),
					system: Some(AbilityInfoToken {
						sub_module: vec![AbilityInfoSubModule {
							channel_id: Some(0),
							ability_value: "reboot_rw".into(),
						}],
					}),
					..Default::default()
				})
			})
			.build();
		let out = dispatch_oneshot(&*fake, &Command::Abilities(args()), false)
			.await
			.unwrap();
		match out {
			Outcome::Abilities {
				username, entries, ..
			} => {
				assert_eq!(username, "admin");
				assert_eq!(entries.len(), 1);
			}
			other => panic!("unexpected: {other:?}"),
		}
	}

	#[tokio::test]
	async fn dispatch_users_list_runs_users_branch() {
		let fake = FakeCameraBuilder::new()
			.with_users(|| Ok(UserList::default()))
			.build();
		let cmd = Command::Users {
			common: args(),
			action: None,
		};
		let out = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert!(matches!(out, Outcome::UsersList { .. }));
	}

	#[tokio::test]
	async fn dispatch_users_delete_runs_delete_branch() {
		let fake = FakeCameraBuilder::new()
			.with_users(|| {
				Ok(UserList {
					version: "1.1".into(),
					user_list: Some(vec![bairelay_neolink_core::bc::xml::User {
						user_name: "alice".into(),
						user_level: 1,
						..Default::default()
					}]),
				})
			})
			.build();
		let cmd = Command::Users {
			common: args(),
			action: Some(UserAction::Delete {
				name: "alice".into(),
			}),
		};
		let _ = dispatch_oneshot(&*fake, &cmd, false).await.unwrap();
		assert_eq!(fake.calls().delete_user.lock().unwrap().len(), 1);
	}

	#[test]
	fn snapshot_json_preflight_rejects_snapshot_without_output() {
		let cmd = Command::Snapshot {
			common: args(),
			output: None,
			use_stream: false,
			use_stream_raw: false,
		};
		let err = snapshot_json_preflight(true, &cmd).unwrap_err();
		assert!(format!("{err:#}").contains("requires --output"));
	}

	#[test]
	fn snapshot_json_preflight_allows_snapshot_with_output() {
		let cmd = Command::Snapshot {
			common: args(),
			output: Some(std::path::PathBuf::from("/tmp/x.jpg")),
			use_stream: false,
			use_stream_raw: false,
		};
		snapshot_json_preflight(true, &cmd).unwrap();
	}

	#[test]
	fn snapshot_json_preflight_allows_non_snapshot_in_json_mode() {
		// --json on any non-Snapshot command is fine.
		snapshot_json_preflight(true, &Command::Reboot(args())).unwrap();
	}

	#[test]
	fn snapshot_json_preflight_allows_snapshot_without_json_mode() {
		let cmd = Command::Snapshot {
			common: args(),
			output: None,
			use_stream: false,
			use_stream_raw: false,
		};
		snapshot_json_preflight(false, &cmd).unwrap();
	}

	#[test]
	fn find_camera_config_returns_clone_when_present() {
		use crate::config::test_helpers::minimal_camera_config;
		let mut cfg = crate::config::Config::default();
		cfg.cameras.push(minimal_camera_config("cam-a"));
		cfg.cameras.push(minimal_camera_config("cam-b"));
		let got = find_camera_config(&cfg, "cam-b").unwrap();
		assert_eq!(got.name, "cam-b");
	}

	#[test]
	fn find_camera_config_errors_when_name_missing() {
		let cfg = crate::config::Config::default();
		let err = find_camera_config(&cfg, "nope").unwrap_err();
		assert!(format!("{err:#}").contains("not found in config"));
	}

	#[tokio::test]
	async fn dispatch_service_variants_classify_as_unexpected() {
		use crate::oneshot::classify::{classify, EXIT_UNEXPECTED, EXIT_USAGE};

		// Reaching `dispatch_oneshot` with a service-mode variant or
		// CheckConfig is a programmer bug — must classify to
		// EXIT_UNEXPECTED, not the usage-error EXIT_USAGE the
		// previous wrapper accidentally produced.
		let cases = [Command::Mqtt, Command::CheckConfig];
		for cmd in &cases {
			let fake = FakeCameraBuilder::new().build();
			let err = dispatch_oneshot(&*fake, cmd, false).await.unwrap_err();
			let code = classify(&err);
			assert_eq!(
				code, EXIT_UNEXPECTED,
				"command {cmd:?} should classify as EXIT_UNEXPECTED ({EXIT_UNEXPECTED}), \
				 not {code} (EXIT_USAGE = {EXIT_USAGE}); err = {err:#}"
			);
			assert!(
				format!("{err:#}").contains("internal:"),
				"error must mark itself as an internal/programmer-bug, got: {err:#}"
			);
		}
	}
}
