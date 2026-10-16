//! CLI → oneshot-action converters and log-directive helpers.
//!
//! Pure glue lifted out of `main.rs` so unit tests can cover every
//! branch without spawning the full binary. The functions here bridge
//! the clap-derived enums in [`crate::cli`] with the action / type
//! enums the `oneshot` modules consume. Living in the library (rather
//! than as `From` impls on either side) keeps `crate::oneshot` free of
//! CLI concerns and `crate::cli` free of `bairelay_neolink_core` concerns —
//! each enum stays focused on its own boundary.

use tracing_subscriber::EnvFilter;

use crate::cli;
use crate::oneshot;

/// Decide whether to emit ANSI colour codes on the log stream. Off
/// when stderr is not a TTY (redirected to a file, piped to another
/// program, captured by a process supervisor) or when `NO_COLOR` is
/// set per <https://no-color.org>. Keeps the four-combination
/// decision table testable; the live TTY probe happens in `main.rs`.
pub fn should_emit_ansi(stderr_is_tty: bool, no_color_set: bool) -> bool {
	stderr_is_tty && !no_color_set
}

/// Translate a `-v` count into a `RUST_LOG` directive. `RUST_LOG`
/// always wins if set in the environment (explicit beats implicit —
/// operator-visible).
pub fn verbosity_env_filter(verbose: u8) -> EnvFilter {
	if let Ok(explicit) = EnvFilter::try_from_default_env() {
		return explicit;
	}
	let directive = match verbose {
		0 => "warn,bairelay=info,rumqttc=warn",
		1 => "info,bairelay=debug",
		2 => "debug,bairelay=trace,bairelay_neolink_core=debug",
		_ => "trace",
	};
	EnvFilter::new(directive)
}

/// Clone / normalise a `bairelay::cli::PtzCommand`. An absent
/// sub-command defaults to listing presets (`Preset { preset_id:
/// None }`) — the same shape as an explicit `ptz <cam> preset` with
/// no id.
pub fn clone_ptz_cmd(cmd: &Option<cli::PtzCommand>) -> cli::PtzCommand {
	use cli::PtzCommand::*;
	match cmd {
		Some(Preset { preset_id }) => Preset {
			preset_id: *preset_id,
		},
		Some(Assign { preset_id, name }) => Assign {
			preset_id: *preset_id,
			name: name.clone(),
		},
		Some(Control {
			amount,
			direction,
			speed,
		}) => Control {
			amount: *amount,
			direction: *direction,
			speed: *speed,
		},
		Some(Zoom { amount }) => Zoom { amount: *amount },
		None => Preset { preset_id: None },
	}
}

/// Map a CLI-level PTZ direction onto the bairelay_neolink_core value.
pub fn ptz_direction_to_core(
	d: cli::PtzDirection,
) -> bairelay_neolink_core::bc_protocol::Direction {
	use bairelay_neolink_core::bc_protocol::Direction;
	use cli::PtzDirection;
	match d {
		PtzDirection::Up => Direction::Up,
		PtzDirection::Down => Direction::Down,
		PtzDirection::Left => Direction::Left,
		PtzDirection::Right => Direction::Right,
		PtzDirection::Stop => Direction::Stop,
	}
}

/// Map a CLI-level service-name onto the oneshot `Service` enum.
pub fn service_name_to_core(s: cli::ServiceName) -> oneshot::services::Service {
	use cli::ServiceName;
	use oneshot::services::Service;
	match s {
		ServiceName::Baichuan => Service::Baichuan,
		ServiceName::Http => Service::Http,
		ServiceName::Https => Service::Https,
		ServiceName::Rtmp => Service::Rtmp,
		ServiceName::Rtsp => Service::Rtsp,
		ServiceName::Onvif => Service::Onvif,
	}
}

/// Clone / normalise a `cli::ServiceAction`. An absent action
/// defaults to `get` (read-mode).
pub fn clone_service_action(a: &Option<cli::ServiceAction>) -> oneshot::services::Action {
	use cli::{OnOff, ServiceAction};
	use oneshot::services::Action;
	match a {
		None | Some(ServiceAction::Get) => Action::Get,
		Some(ServiceAction::On) => Action::On,
		Some(ServiceAction::Off) => Action::Off,
		Some(ServiceAction::Port { port }) => Action::Port(*port),
		Some(ServiceAction::Set { port, enabled }) => Action::Set {
			port: *port,
			enabled: matches!(enabled, OnOff::On),
		},
	}
}

/// Clone / normalise a `cli::UserAction`. An absent action defaults
/// to `list`.
/// Translate a CLI [`cli::UserAction`] into the oneshot
/// [`oneshot::users::Action`].
///
/// `Add` / `Password` carry the password as `Option<String>` to allow
/// the operator to omit it on the command line; this function calls
/// [`oneshot::password_source::resolve`] to fill it in via TTY prompt
/// or stdin read. That path performs blocking I/O — fine for a one-
/// shot CLI but worth noting for callers in test contexts (tests
/// always pass `Some(_)` to bypass the prompt).
pub fn clone_user_action(a: &Option<cli::UserAction>) -> anyhow::Result<oneshot::users::Action> {
	use cli::{UserAction, UserTypeArg};
	use oneshot::password_source::{self, PROMPT_ADD, PROMPT_PASSWORD};
	use oneshot::users::{Action, UserType};
	let action = match a {
		None | Some(UserAction::List) => Action::List,
		Some(UserAction::Add {
			name,
			password,
			user_type,
		}) => {
			let password = password_source::resolve(password.clone(), PROMPT_ADD)?;
			Action::Add {
				name: name.clone(),
				password,
				user_type: match user_type {
					UserTypeArg::User => UserType::User,
					UserTypeArg::Administrator => UserType::Administrator,
				},
			}
		}
		Some(UserAction::Password { name, password }) => Action::Password {
			name: name.clone(),
			password: password_source::resolve(password.clone(), PROMPT_PASSWORD)?,
		},
		Some(UserAction::Delete { name }) => Action::Delete { name: name.clone() },
	};
	Ok(action)
}

/// Deep-clone a [`cli::Command`] value.
///
/// `Command` isn't `Clone` (the clap `Subcommand` variants hold
/// sub-commands that aren't `Clone` either), but tests and the
/// async-dispatch bridge need an owned copy so the `'static`-ish
/// closure bound on `runner::run` is satisfiable. This helper does
/// the hand-rolled deep copy — service-mode variants round-trip
/// through their payload fields; every one-shot variant preserves
/// `OneShotArgs` + any sub-flags.
pub fn clone_command(cmd: &cli::Command) -> cli::Command {
	use cli::Command;
	match cmd {
		Command::Mqtt => Command::Mqtt,
		Command::Rtsp { dump_bcmedia } => Command::Rtsp {
			dump_bcmedia: dump_bcmedia.clone(),
		},
		Command::MqttRtsp { dump_bcmedia } => Command::MqttRtsp {
			dump_bcmedia: dump_bcmedia.clone(),
		},
		Command::Reboot(a) => Command::Reboot(clone_oneshot_args(a)),
		Command::Battery(a) => Command::Battery(clone_oneshot_args(a)),
		Command::Presets(a) => Command::Presets(clone_oneshot_args(a)),
		Command::SetTime(a) => Command::SetTime(clone_oneshot_args(a)),
		Command::Version(a) => Command::Version(clone_oneshot_args(a)),
		Command::Siren(a) => Command::Siren(clone_oneshot_args(a)),
		Command::Abilities(a) => Command::Abilities(clone_oneshot_args(a)),
		Command::Webhook(a) => Command::Webhook(clone_oneshot_args(a)),
		Command::Snapshot {
			common,
			output,
			use_stream,
			use_stream_raw,
		} => Command::Snapshot {
			common: clone_oneshot_args(common),
			output: output.clone(),
			use_stream: *use_stream,
			use_stream_raw: *use_stream_raw,
		},
		Command::Floodlight { common, state } => Command::Floodlight {
			common: clone_oneshot_args(common),
			state: *state,
		},
		Command::Pir { common, state } => Command::Pir {
			common: clone_oneshot_args(common),
			state: *state,
		},
		Command::StatusLight { common, state } => Command::StatusLight {
			common: clone_oneshot_args(common),
			state: *state,
		},
		Command::Ptz { common, cmd } => Command::Ptz {
			common: clone_oneshot_args(common),
			cmd: cmd.as_ref().map(clone_ptz_sub),
		},
		Command::Services {
			common,
			service,
			action,
		} => Command::Services {
			common: clone_oneshot_args(common),
			service: *service,
			action: action.as_ref().map(clone_service_sub),
		},
		Command::Users { common, action } => Command::Users {
			common: clone_oneshot_args(common),
			action: action.as_ref().map(clone_user_sub),
		},
		Command::CheckConfig => Command::CheckConfig,
		Command::CloudAuthorise { method } => Command::CloudAuthorise {
			method: method.clone(),
		},
		Command::RenderHassioConfig {
			options_json,
			overlay,
			mqtt_host,
			mqtt_port,
			mqtt_user,
			mqtt_pass,
			mqtt_ssl,
			output,
		} => Command::RenderHassioConfig {
			options_json: options_json.clone(),
			overlay: overlay.clone(),
			mqtt_host: mqtt_host.clone(),
			mqtt_port: *mqtt_port,
			mqtt_user: mqtt_user.clone(),
			mqtt_pass: mqtt_pass.clone(),
			mqtt_ssl: *mqtt_ssl,
			output: output.clone(),
		},
	}
}

fn clone_oneshot_args(a: &cli::OneShotArgs) -> cli::OneShotArgs {
	cli::OneShotArgs {
		camera: a.camera.clone(),
	}
}

/// Sub-helper for `clone_command`: deep-clone a `cli::PtzCommand`
/// variant without normalising `None` the way `clone_ptz_cmd` does.
fn clone_ptz_sub(c: &cli::PtzCommand) -> cli::PtzCommand {
	use cli::PtzCommand::*;
	match c {
		Preset { preset_id } => Preset {
			preset_id: *preset_id,
		},
		Assign { preset_id, name } => Assign {
			preset_id: *preset_id,
			name: name.clone(),
		},
		Control {
			amount,
			direction,
			speed,
		} => Control {
			amount: *amount,
			direction: *direction,
			speed: *speed,
		},
		Zoom { amount } => Zoom { amount: *amount },
	}
}

/// Sub-helper: deep-clone a `cli::ServiceAction`.
fn clone_service_sub(a: &cli::ServiceAction) -> cli::ServiceAction {
	use cli::ServiceAction::*;
	match a {
		Get => Get,
		On => On,
		Off => Off,
		Port { port } => Port { port: *port },
		Set { port, enabled } => Set {
			port: *port,
			enabled: *enabled,
		},
	}
}

/// Sub-helper: deep-clone a `cli::UserAction`.
fn clone_user_sub(a: &cli::UserAction) -> cli::UserAction {
	use cli::UserAction::*;
	match a {
		List => List,
		Add {
			name,
			password,
			user_type,
		} => Add {
			name: name.clone(),
			password: password.clone(),
			user_type: *user_type,
		},
		Password { name, password } => Password {
			name: name.clone(),
			password: password.clone(),
		},
		Delete { name } => Delete { name: name.clone() },
	}
}

/// Coarse-grained label for a one-shot exit code, for error reporting
/// to operators.
pub fn exit_code_to_kind(code: i32) -> &'static str {
	use crate::oneshot::classify::{
		EXIT_CONFIG, EXIT_CONNECTION, EXIT_INTERRUPTED, EXIT_PROTOCOL, EXIT_UNSUPPORTED, EXIT_USAGE,
	};
	match code {
		EXIT_USAGE => "usage",
		EXIT_CONFIG => "config",
		EXIT_CONNECTION => "connection",
		EXIT_PROTOCOL => "protocol",
		EXIT_UNSUPPORTED => "unsupported",
		EXIT_INTERRUPTED => "interrupted",
		_ => "internal",
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn should_emit_ansi_only_when_tty_and_no_color_unset() {
		// TTY + no NO_COLOR → colour on (interactive terminal).
		assert!(should_emit_ansi(true, false));
		// Not a TTY (file redirect, pipe) → colour off regardless.
		assert!(!should_emit_ansi(false, false));
		// NO_COLOR set → colour off even on a TTY.
		assert!(!should_emit_ansi(true, true));
		// Both off-conditions → colour off.
		assert!(!should_emit_ansi(false, true));
	}

	#[test]
	fn verbosity_env_filter_maps_each_level() {
		// Make sure RUST_LOG isn't set in the test env so the
		// implicit mapping path runs (don't touch global env here —
		// we just verify the fn doesn't panic and builds an EnvFilter
		// for each level).
		// SAFETY (single-threaded test, scoped flip): ensure the
		// explicit env-var branch is not taken.
		let previous = std::env::var("RUST_LOG").ok();
		unsafe {
			std::env::remove_var("RUST_LOG");
		}
		for v in [0u8, 1, 2, 3, 255] {
			let _f = verbosity_env_filter(v);
		}
		if let Some(v) = previous {
			unsafe {
				std::env::set_var("RUST_LOG", v);
			}
		}
	}

	#[test]
	fn clone_ptz_cmd_none_defaults_to_preset_list() {
		match clone_ptz_cmd(&None) {
			cli::PtzCommand::Preset { preset_id: None } => {}
			other => panic!("expected Preset{{None}}, got {other:?}"),
		}
	}

	#[test]
	fn clone_ptz_cmd_preserves_preset_id() {
		let got = clone_ptz_cmd(&Some(cli::PtzCommand::Preset { preset_id: Some(3) }));
		match got {
			cli::PtzCommand::Preset { preset_id: Some(3) } => {}
			other => panic!("expected Preset{{Some(3)}}, got {other:?}"),
		}
	}

	#[test]
	fn clone_ptz_cmd_preserves_assign() {
		let got = clone_ptz_cmd(&Some(cli::PtzCommand::Assign {
			preset_id: 5,
			name: "home".into(),
		}));
		match got {
			cli::PtzCommand::Assign { preset_id: 5, name } if name == "home" => {}
			other => panic!("expected Assign{{5,\"home\"}}, got {other:?}"),
		}
	}

	#[test]
	fn clone_ptz_cmd_preserves_control() {
		let got = clone_ptz_cmd(&Some(cli::PtzCommand::Control {
			amount: 3,
			direction: cli::PtzDirection::Up,
			speed: Some(32),
		}));
		match got {
			cli::PtzCommand::Control {
				amount,
				direction: cli::PtzDirection::Up,
				speed,
			} => {
				assert_eq!(amount, 3);
				assert_eq!(speed, Some(32));
			}
			other => panic!("expected Control{{..}}, got {other:?}"),
		}
	}

	#[test]
	fn clone_ptz_cmd_preserves_zoom() {
		let got = clone_ptz_cmd(&Some(cli::PtzCommand::Zoom { amount: 0.75 }));
		match got {
			cli::PtzCommand::Zoom { amount } => assert_eq!(amount, 0.75),
			other => panic!("expected Zoom, got {other:?}"),
		}
	}

	#[test]
	fn ptz_direction_to_core_maps_every_variant() {
		use bairelay_neolink_core::bc_protocol::Direction;
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Up),
			Direction::Up
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Down),
			Direction::Down
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Left),
			Direction::Left
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Right),
			Direction::Right
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Stop),
			Direction::Stop
		));
	}

	#[test]
	fn service_name_to_core_maps_every_variant() {
		use oneshot::services::Service;
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Baichuan),
			Service::Baichuan
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Http),
			Service::Http
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Https),
			Service::Https
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Rtmp),
			Service::Rtmp
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Rtsp),
			Service::Rtsp
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Onvif),
			Service::Onvif
		));
	}

	#[test]
	fn clone_service_action_defaults_to_get_when_absent() {
		use oneshot::services::Action;
		assert!(matches!(clone_service_action(&None), Action::Get));
	}

	#[test]
	fn clone_service_action_maps_each_variant() {
		use cli::{OnOff, ServiceAction};
		use oneshot::services::Action;
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Get)),
			Action::Get
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::On)),
			Action::On
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Off)),
			Action::Off
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Port { port: 8080 })),
			Action::Port(8080)
		));
		let set_on = clone_service_action(&Some(ServiceAction::Set {
			port: 443,
			enabled: OnOff::On,
		}));
		match set_on {
			Action::Set { port, enabled } => {
				assert_eq!(port, 443);
				assert!(enabled);
			}
			other => panic!("expected Set, got {other:?}"),
		}
		let set_off = clone_service_action(&Some(ServiceAction::Set {
			port: 80,
			enabled: OnOff::Off,
		}));
		match set_off {
			Action::Set { port, enabled } => {
				assert_eq!(port, 80);
				assert!(!enabled);
			}
			other => panic!("expected Set(off), got {other:?}"),
		}
	}

	#[test]
	fn clone_user_action_defaults_to_list_when_absent() {
		use oneshot::users::Action;
		assert!(matches!(
			clone_user_action(&None).expect("ok"),
			Action::List
		));
	}

	#[test]
	fn clone_user_action_maps_each_variant() {
		use cli::{UserAction, UserTypeArg};
		use oneshot::users::{Action, UserType};
		assert!(matches!(
			clone_user_action(&Some(UserAction::List)).expect("ok"),
			Action::List
		));
		let add = clone_user_action(&Some(UserAction::Add {
			name: "bob".into(),
			password: Some("pw".into()),
			user_type: UserTypeArg::Administrator,
		}))
		.expect("ok");
		match add {
			Action::Add {
				name,
				password,
				user_type: UserType::Administrator,
			} => {
				assert_eq!(name, "bob");
				assert_eq!(password, "pw");
			}
			other => panic!("expected Add Administrator, got {other:?}"),
		}
		let add_user = clone_user_action(&Some(UserAction::Add {
			name: "a".into(),
			password: Some("b".into()),
			user_type: UserTypeArg::User,
		}))
		.expect("ok");
		assert!(matches!(
			add_user,
			Action::Add {
				user_type: UserType::User,
				..
			}
		));

		let pw = clone_user_action(&Some(UserAction::Password {
			name: "x".into(),
			password: Some("y".into()),
		}))
		.expect("ok");
		match pw {
			Action::Password { name, password } => {
				assert_eq!(name, "x");
				assert_eq!(password, "y");
			}
			other => panic!("expected Password, got {other:?}"),
		}
		let del = clone_user_action(&Some(UserAction::Delete { name: "z".into() })).expect("ok");
		match del {
			Action::Delete { name } => assert_eq!(name, "z"),
			other => panic!("expected Delete, got {other:?}"),
		}
	}

	#[test]
	fn clone_user_action_empty_provided_password_rejected() {
		use cli::{UserAction, UserTypeArg};
		// Empty positional should classify as a usage error (operator
		// pilot error: `bairelay users add bob user ""`). The
		// password resolver enforces non-empty regardless of source.
		let err = clone_user_action(&Some(UserAction::Add {
			name: "bob".into(),
			password: Some(String::new()),
			user_type: UserTypeArg::User,
		}))
		.expect_err("must reject empty password");
		assert!(
			err.chain()
				.any(|c| c.is::<crate::oneshot::errors::UsageError>()),
			"empty password must classify as UsageError; chain: {err:#}"
		);
	}

	fn args(name: &str) -> cli::OneShotArgs {
		cli::OneShotArgs {
			camera: name.into(),
		}
	}

	#[test]
	fn clone_command_service_modes() {
		use std::path::PathBuf;
		assert!(matches!(
			clone_command(&cli::Command::Mqtt),
			cli::Command::Mqtt
		));

		let r = clone_command(&cli::Command::Rtsp {
			dump_bcmedia: Some(PathBuf::from("/tmp/dump")),
		});
		match r {
			cli::Command::Rtsp { dump_bcmedia } => {
				assert_eq!(dump_bcmedia.unwrap(), PathBuf::from("/tmp/dump"))
			}
			_ => panic!(),
		}
		let r = clone_command(&cli::Command::Rtsp { dump_bcmedia: None });
		match r {
			cli::Command::Rtsp { dump_bcmedia: None } => {}
			_ => panic!(),
		}
		let r = clone_command(&cli::Command::MqttRtsp {
			dump_bcmedia: Some(PathBuf::from("/tmp/a")),
		});
		match r {
			cli::Command::MqttRtsp { dump_bcmedia } => {
				assert_eq!(dump_bcmedia.unwrap(), PathBuf::from("/tmp/a"))
			}
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_oneshot_simple_variants() {
		match clone_command(&cli::Command::Reboot(args("c1"))) {
			cli::Command::Reboot(a) => assert_eq!(a.camera, "c1"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Battery(args("c2"))) {
			cli::Command::Battery(a) => assert_eq!(a.camera, "c2"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Presets(args("c3"))) {
			cli::Command::Presets(a) => assert_eq!(a.camera, "c3"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::SetTime(args("c4"))) {
			cli::Command::SetTime(a) => assert_eq!(a.camera, "c4"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Version(args("c5"))) {
			cli::Command::Version(a) => assert_eq!(a.camera, "c5"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Siren(args("c6"))) {
			cli::Command::Siren(a) => assert_eq!(a.camera, "c6"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Abilities(args("c7"))) {
			cli::Command::Abilities(a) => assert_eq!(a.camera, "c7"),
			_ => panic!(),
		}
		match clone_command(&cli::Command::Webhook(args("c8"))) {
			cli::Command::Webhook(a) => assert_eq!(a.camera, "c8"),
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_snapshot_preserves_every_field() {
		use std::path::PathBuf;
		let src = cli::Command::Snapshot {
			common: args("cam"),
			output: Some(PathBuf::from("/tmp/out.jpg")),
			use_stream: true,
			use_stream_raw: false,
		};
		match clone_command(&src) {
			cli::Command::Snapshot {
				common,
				output,
				use_stream,
				use_stream_raw,
			} => {
				assert_eq!(common.camera, "cam");
				assert_eq!(output.unwrap(), PathBuf::from("/tmp/out.jpg"));
				assert!(use_stream);
				assert!(!use_stream_raw);
			}
			_ => panic!(),
		}
		// variant with raw=true
		let src = cli::Command::Snapshot {
			common: args("cam"),
			output: None,
			use_stream: false,
			use_stream_raw: true,
		};
		match clone_command(&src) {
			cli::Command::Snapshot {
				output: None,
				use_stream: false,
				use_stream_raw: true,
				..
			} => {}
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_floodlight_pir_statuslight() {
		use cli::{FloodlightState, OnOff};
		match clone_command(&cli::Command::Floodlight {
			common: args("fc"),
			state: Some(FloodlightState::On),
		}) {
			cli::Command::Floodlight {
				common,
				state: Some(FloodlightState::On),
			} => {
				assert_eq!(common.camera, "fc")
			}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Floodlight {
			common: args("fc"),
			state: None,
		}) {
			cli::Command::Floodlight { state: None, .. } => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Pir {
			common: args("p"),
			state: Some(OnOff::Off),
		}) {
			cli::Command::Pir {
				state: Some(OnOff::Off),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::StatusLight {
			common: args("s"),
			state: Some(OnOff::On),
		}) {
			cli::Command::StatusLight {
				state: Some(OnOff::On),
				..
			} => {}
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_ptz_all_subcommands() {
		// None → kept as None (clone_ptz_sub only runs when Some)
		match clone_command(&cli::Command::Ptz {
			common: args("p"),
			cmd: None,
		}) {
			cli::Command::Ptz { cmd: None, .. } => {}
			_ => panic!(),
		}
		// Preset
		match clone_command(&cli::Command::Ptz {
			common: args("p"),
			cmd: Some(cli::PtzCommand::Preset { preset_id: Some(9) }),
		}) {
			cli::Command::Ptz {
				cmd: Some(cli::PtzCommand::Preset { preset_id: Some(9) }),
				..
			} => {}
			_ => panic!(),
		}
		// Assign
		match clone_command(&cli::Command::Ptz {
			common: args("p"),
			cmd: Some(cli::PtzCommand::Assign {
				preset_id: 1,
				name: "n".into(),
			}),
		}) {
			cli::Command::Ptz {
				cmd: Some(cli::PtzCommand::Assign { preset_id: 1, name }),
				..
			} if name == "n" => {}
			_ => panic!(),
		}
		// Control
		match clone_command(&cli::Command::Ptz {
			common: args("p"),
			cmd: Some(cli::PtzCommand::Control {
				amount: 2,
				direction: cli::PtzDirection::Left,
				speed: Some(10),
			}),
		}) {
			cli::Command::Ptz {
				cmd:
					Some(cli::PtzCommand::Control {
						amount: 2,
						direction: cli::PtzDirection::Left,
						speed: Some(10),
					}),
				..
			} => {}
			_ => panic!(),
		}
		// Zoom
		match clone_command(&cli::Command::Ptz {
			common: args("p"),
			cmd: Some(cli::PtzCommand::Zoom { amount: 0.5 }),
		}) {
			cli::Command::Ptz {
				cmd: Some(cli::PtzCommand::Zoom { amount }),
				..
			} => {
				assert_eq!(amount, 0.5)
			}
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_services_all_subactions() {
		use cli::{OnOff, ServiceAction, ServiceName};
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: None,
			action: None,
		}) {
			cli::Command::Services {
				service: None,
				action: None,
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: Some(ServiceName::Http),
			action: Some(ServiceAction::Get),
		}) {
			cli::Command::Services {
				action: Some(ServiceAction::Get),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: Some(ServiceName::Https),
			action: Some(ServiceAction::On),
		}) {
			cli::Command::Services {
				action: Some(ServiceAction::On),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: Some(ServiceName::Rtsp),
			action: Some(ServiceAction::Off),
		}) {
			cli::Command::Services {
				action: Some(ServiceAction::Off),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: Some(ServiceName::Onvif),
			action: Some(ServiceAction::Port { port: 8080 }),
		}) {
			cli::Command::Services {
				action: Some(ServiceAction::Port { port: 8080 }),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Services {
			common: args("s"),
			service: Some(ServiceName::Rtmp),
			action: Some(ServiceAction::Set {
				port: 443,
				enabled: OnOff::On,
			}),
		}) {
			cli::Command::Services {
				action: Some(ServiceAction::Set {
					port: 443,
					enabled: OnOff::On,
				}),
				..
			} => {}
			_ => panic!(),
		}
	}

	#[test]
	fn clone_command_users_all_subactions() {
		use cli::{UserAction, UserTypeArg};
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: None,
		}) {
			cli::Command::Users { action: None, .. } => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: Some(UserAction::List),
		}) {
			cli::Command::Users {
				action: Some(UserAction::List),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: Some(UserAction::Add {
				name: "a".into(),
				password: Some("b".into()),
				user_type: UserTypeArg::Administrator,
			}),
		}) {
			cli::Command::Users {
				action:
					Some(UserAction::Add {
						name,
						password,
						user_type: UserTypeArg::Administrator,
					}),
				..
			} => {
				assert_eq!(name, "a");
				assert_eq!(password.as_deref(), Some("b"));
			}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: Some(UserAction::Add {
				name: "a".into(),
				password: Some("b".into()),
				user_type: UserTypeArg::User,
			}),
		}) {
			cli::Command::Users {
				action: Some(UserAction::Add {
					user_type: UserTypeArg::User,
					..
				}),
				..
			} => {}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: Some(UserAction::Password {
				name: "a".into(),
				password: Some("b".into()),
			}),
		}) {
			cli::Command::Users {
				action: Some(UserAction::Password { name, password }),
				..
			} => {
				assert_eq!(name, "a");
				assert_eq!(password.as_deref(), Some("b"));
			}
			_ => panic!(),
		}
		match clone_command(&cli::Command::Users {
			common: args("u"),
			action: Some(UserAction::Delete { name: "z".into() }),
		}) {
			cli::Command::Users {
				action: Some(UserAction::Delete { name }),
				..
			} => {
				assert_eq!(name, "z");
			}
			_ => panic!(),
		}
	}

	#[test]
	fn verbosity_honours_explicit_rust_log() {
		// Force the explicit branch by setting RUST_LOG.
		let previous = std::env::var("RUST_LOG").ok();
		unsafe {
			std::env::set_var("RUST_LOG", "warn");
		}
		let _ = verbosity_env_filter(0);
		if let Some(v) = previous {
			unsafe {
				std::env::set_var("RUST_LOG", v);
			}
		} else {
			unsafe {
				std::env::remove_var("RUST_LOG");
			}
		}
	}

	#[test]
	fn exit_code_to_kind_covers_every_label() {
		use crate::oneshot::classify::{
			EXIT_CONFIG, EXIT_CONNECTION, EXIT_INTERRUPTED, EXIT_PROTOCOL, EXIT_UNSUPPORTED,
			EXIT_USAGE,
		};
		assert_eq!(exit_code_to_kind(EXIT_USAGE), "usage");
		assert_eq!(exit_code_to_kind(EXIT_CONFIG), "config");
		assert_eq!(exit_code_to_kind(EXIT_CONNECTION), "connection");
		assert_eq!(exit_code_to_kind(EXIT_PROTOCOL), "protocol");
		assert_eq!(exit_code_to_kind(EXIT_UNSUPPORTED), "unsupported");
		assert_eq!(exit_code_to_kind(EXIT_INTERRUPTED), "interrupted");
		assert_eq!(exit_code_to_kind(0), "internal");
		assert_eq!(exit_code_to_kind(999), "internal");
	}
}
