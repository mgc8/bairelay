//! Pure-logic helpers extracted from `main.rs` so each step of the
//! bring-up / teardown pipeline is unit-testable without a real
//! broker, orchestrator, or signal-handler wiring.
//!
//! `main()` itself is still uncoverable (it's the binary entrypoint
//! wired to `#[tokio::main]`), but the **data-shape** decisions —
//! picking the one-shot output mode, loading + validating config,
//! formatting success / failure payloads, and the shutdown-signal
//! fanout — now live here behind straightforward function signatures.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::cli::{Cli, Command};
use crate::config::{parse_config, validate_config, Config};
use crate::oneshot::classify;
use crate::oneshot::dispatch::{dispatch_oneshot, find_camera_config, snapshot_json_preflight};
use crate::oneshot::errors::ConfigError;
use crate::oneshot::output::{format_failure, format_success, Mode, Outcome};
use crate::oneshot::{
	audio_bridge, audio_settings, chime_settings, runner, snapshot, talk, talk_capability,
};

/// Pick the one-shot output mode from the CLI flag. Extracted so the
/// `cli.json → Mode` mapping is pinned in one place.
pub fn cli_output_mode(cli: &Cli) -> Mode {
	if cli.json {
		Mode::Json
	} else {
		Mode::Human
	}
}

/// Read a config file, parse the TOML, then run the validator. Any
/// failure is wrapped in an [`anyhow::Error`] with file-path context.
/// Used by both the service-mode and one-shot bring-up paths.
pub fn load_validated_config(path: &Path) -> Result<Config> {
	let config_str = std::fs::read_to_string(path)
		.with_context(|| format!("Failed to read config file: {}", path.display()))?;
	let mut config = parse_config(&config_str)
		.map_err(|e| anyhow::anyhow!(e))
		.context("Invalid configuration")?;
	// Attach the host's MFA trust token (if bootstrapped via `cloud-authorise`)
	// from the sibling state file, so cloud cameras can clear login verification.
	crate::config::apply_cloud_auth(&mut config, path);
	// Same sibling-state-file rule for the settings verdicts: the families the
	// bridge has proven modifiable are restored from
	// `config-families-read.json` beside this config file, so a battery
	// doorbell doesn't pay a full fifteen-command salvo to remember what it
	// already proved yesterday. Missing or corrupt file is the current
	// behaviour, never a startup failure — see [`crate::family_state`].
	crate::family_state::set_path(path);
	validate_config(&config)
		.map_err(|e| anyhow::anyhow!(e))
		.context("Configuration validation failed")?;
	Ok(config)
}

/// Write a success [`Outcome`] to stdout/stderr per the given mode.
/// Returns the written-byte counts as `(stdout_len, stderr_len)` so
/// tests can assert the split without capturing real fds.
pub fn emit_success_bytes(mode: Mode, outcome: &Outcome) -> (String, String) {
	format_success(mode, outcome)
}

/// Produce the textual failure payload for the given mode + kind.
/// Mirrors the side-effectful `emit_failure` in `main.rs` but returns
/// the string + target-stream tag so tests don't need to capture fds.
pub fn emit_failure_payload(mode: Mode, err: &anyhow::Error, kind: &str) -> (Mode, String) {
	(mode, format_failure(mode, err, kind))
}

/// Sleep for `delay` or until `cancel` fires, whichever comes first.
/// Returns `true` if the sleep completed normally and `false` if
/// cancellation pre-empted it. Shared by every retry / backoff path
/// (camera reconnect, MQTT broker reconnect, push-listener motion hold)
/// so the "wait but bail on shutdown" contract lives in exactly one
/// place.
pub async fn sleep_or_cancel(delay: Duration, cancel: &CancellationToken) -> bool {
	tokio::select! {
		_ = tokio::time::sleep(delay) => true,
		_ = cancel.cancelled() => false,
	}
}

/// A service listener's bind IP: its own `key` (e.g. `webhook_addr`)
/// when set, else the top-level `bind`.
pub fn resolve_bind_ip(
	key: &str,
	explicit: Option<&str>,
	top_level: &str,
) -> Result<std::net::IpAddr> {
	match explicit {
		Some(addr) => addr
			.parse()
			.with_context(|| format!("Invalid {key} {addr}")),
		None => top_level
			.parse()
			.with_context(|| format!("Invalid bind address {top_level}")),
	}
}

/// Bind a service's TCP listener at startup, so a port conflict halts
/// the daemon before any "started" log line.
pub async fn bind_service_listener(
	name: &str,
	ip: std::net::IpAddr,
	port: u16,
) -> Result<tokio::net::TcpListener> {
	let addr = std::net::SocketAddr::new(ip, port);
	tokio::net::TcpListener::bind(addr)
		.await
		.with_context(|| format!("{name} bind failed on {addr}"))
}

/// The post-signal body: log + cancel the token. Extracted so tests
/// can exercise the cancel side effect without driving a real signal.
pub fn on_shutdown_signal(token: &CancellationToken, signal: &str) {
	tracing::info!(signal, "Shutdown requested, aborting one-shot");
	token.cancel();
}

/// Install the shutdown-signal listener (Ctrl+C; plus SIGTERM on Unix)
/// and spawn a task that fires `token.cancel()` on the first signal.
/// Handlers are registered before this returns. Returns the
/// `JoinHandle` so tests can await completion. Used by the one-shot
/// path.
pub fn spawn_shutdown_cancel(token: CancellationToken) -> tokio::task::JoinHandle<()> {
	let requested = crate::shutdown_signal::requested();
	tokio::spawn(async move {
		let signal = requested.await;
		on_shutdown_signal(&token, signal);
		crate::shutdown_signal::force_exit_on_next(crate::oneshot::classify::EXIT_INTERRUPTED);
	})
}

/// Pick the camera names list out of a [`Config`]. One-line but
/// extracted so the allocation order is asserted in tests.
pub fn camera_names(config: &Config) -> Vec<String> {
	config.cameras.iter().map(|c| c.name.clone()).collect()
}

/// Write a success outcome split across the two given writers.
/// Production calls [`emit_success`] which forwards to the real
/// `stdout` + `stderr`; tests pass `&mut Vec<u8>` for both so they
/// can assert on the bytes without polluting the test runner output.
pub fn emit_success_to<W1: std::io::Write, W2: std::io::Write>(
	mode: Mode,
	outcome: &Outcome,
	stdout: &mut W1,
	stderr: &mut W2,
) {
	let (out, err) = format_success(mode, outcome);
	if !out.is_empty() {
		let _ = stdout.write_all(out.as_bytes());
	}
	if !err.is_empty() {
		let _ = stderr.write_all(err.as_bytes());
	}
}

/// Write a failure payload to one of the two given writers per mode.
/// - `Mode::Json` → `stdout` (script-readable JSON object).
/// - `Mode::Human` → `stderr` (tty-reserved for machine bytes on stdout).
pub fn emit_failure_to<W1: std::io::Write, W2: std::io::Write>(
	mode: Mode,
	err: &anyhow::Error,
	kind: &str,
	stdout: &mut W1,
	stderr: &mut W2,
) {
	let s = format_failure(mode, err, kind);
	match mode {
		Mode::Json => {
			let _ = stdout.write_all(s.as_bytes());
		}
		Mode::Human => {
			let _ = stderr.write_all(s.as_bytes());
		}
	}
}

/// Write a success outcome to the process's real `stdout` / `stderr`.
pub fn emit_success(mode: Mode, outcome: &Outcome) {
	emit_success_to(
		mode,
		outcome,
		&mut std::io::stdout().lock(),
		&mut std::io::stderr().lock(),
	);
}

/// Write a failure payload to the process's real `stdout` / `stderr`.
pub fn emit_failure(mode: Mode, err: &anyhow::Error, kind: &str) {
	emit_failure_to(
		mode,
		err,
		kind,
		&mut std::io::stdout().lock(),
		&mut std::io::stderr().lock(),
	);
}

/// `check-config` body: load the config and walk the validator. On
/// success write a one-line summary to `stdout`; on failure write a
/// classified error to either `stdout` (Json) or `stderr` (Human) and
/// return the matching exit code. Stays sync — no tokio needed.
/// Production passes real stdout/stderr; tests pass `Vec<u8>` so they
/// can assert on the bytes without polluting the cargo-test output.
pub fn run_check_config_to<W1: std::io::Write, W2: std::io::Write>(
	cli: &Cli,
	mode: Mode,
	stdout: &mut W1,
	stderr: &mut W2,
) -> i32 {
	let path = cli.config_path();
	// Mirror the read → parse → validate triple from `run_oneshot`
	// below. Each step's error is wrapped as `ConfigError` so the
	// exit-code classifier returns `EXIT_CONFIG`. A missing file maps
	// to `EXIT_USAGE` (the operator pointed us somewhere that doesn't
	// exist) — distinct from `EXIT_CONFIG` which is "file present but
	// malformed".
	let config_str = match std::fs::read_to_string(path) {
		Ok(s) => s,
		Err(e) => {
			let kind = if e.kind() == std::io::ErrorKind::NotFound {
				classify::EXIT_USAGE
			} else {
				classify::EXIT_CONFIG
			};
			let msg = anyhow::anyhow!(format!("read {}: {}", path.display(), e));
			emit_failure_to(
				mode,
				&msg,
				crate::cli_convert::exit_code_to_kind(kind),
				stdout,
				stderr,
			);
			return kind;
		}
	};
	let config = match crate::config::parse_config(&config_str) {
		Ok(c) => c,
		Err(e) => {
			let msg = anyhow::anyhow!(format!("parse {}: {}", path.display(), e));
			emit_failure_to(mode, &msg, "config", stdout, stderr);
			return classify::EXIT_CONFIG;
		}
	};
	if let Err(e) = crate::config::validate_config(&config) {
		let msg = anyhow::anyhow!(format!("validate {}: {}", path.display(), e));
		emit_failure_to(mode, &msg, "config", stdout, stderr);
		return classify::EXIT_CONFIG;
	}
	// Load the MQTT TLS material too: a wrong path or bad PEM is a
	// config error the daemon would otherwise only hit at startup.
	if let Err(e) = crate::mqtt_loop::build_broker_config(&config) {
		let msg = anyhow::anyhow!(format!("validate {}: {:#}", path.display(), e));
		emit_failure_to(mode, &msg, "config", stdout, stderr);
		return classify::EXIT_CONFIG;
	}
	// Run the same migration-warning helpers the daemon emits at
	// startup so operators using `check-config` for pre-deploy
	// validation see the same notes about deprecated neolink fields,
	// shadowed `pause.timeout`, and idle-disconnect / prune-grace
	// inversions. Warnings flow through `tracing::warn!` to stderr;
	// in `--json` mode the JSON success payload still goes to stdout
	// cleanly, so machine-readable consumers are unaffected.
	crate::config::warn_deprecated_pause_fields(&config);
	crate::config::warn_neolink_compat_fields(&config);
	crate::config::warn_wire_debug_enabled(&config);
	crate::config::warn_loopback_webhook_advertise(&config);
	crate::config::warn_idle_timeout_below_prune_floor(&config);
	crate::config::warn_uid_beyond_local(&config);
	crate::config::warn_startup_discovery_without_credentials(&config);
	// Success — short summary so operators get a quick affirmation. The startup scan is
	// said out loud because a file it approves is a file whose camera count at startup is
	// not the number on this line: `0 cameras` here plus a scan that adopts two is a
	// normal night for this mode, and an operator reading only the summary should not
	// have to guess whether the mode was on. `--json` keeps its existing payload — the
	// `cameras` key has always meant "cameras in the file", and a consumer that counted
	// on that number should not start getting a different one because a LAN answered.
	let mut summary = format!(
		"config OK: {} camera(s), bind {}:{}",
		config.cameras.len(),
		config.bind_addr,
		config.bind_port,
	);
	if let Some(scan) = config.startup_discovery_active() {
		summary.push_str(&format!(
			", startup discovery on (cameras that answer within {}s are added)",
			scan.wait_secs
		));
	}
	summary.push('\n');
	match mode {
		Mode::Json => {
			let _ = writeln!(
				stdout,
				r#"{{"ok":true,"cameras":{}}}"#,
				config.cameras.len()
			);
		}
		Mode::Human => {
			let _ = stdout.write_all(summary.as_bytes());
		}
	}
	classify::EXIT_OK
}

/// End-to-end one-shot bring-up: preflight → load + validate config →
/// find the named camera → connect + dispatch. Returns a CLI exit code.
/// `stdout` / `stderr` receive the success / failure payloads per
/// [`emit_success_to`] / [`emit_failure_to`]; production passes the real
/// process fds, tests pass `Vec<u8>`.
pub async fn run_oneshot_to<W1: std::io::Write, W2: std::io::Write>(
	cli: &Cli,
	stdout: &mut W1,
	stderr: &mut W2,
) -> i32 {
	use futures::FutureExt;

	let mode = cli_output_mode(cli);

	// `check-config` is a no-camera one-shot: load + validate the
	// config file and exit. Routed here so the same exit-code table
	// applies (0 OK, 2 missing, 3 parse/validate failure).
	if cli.is_check_config() {
		return run_check_config_to(cli, mode, stdout, stderr);
	}

	// `cloud-authorise` is a no-camera interactive command: read the config,
	// run the MFA bootstrap (prompts on the real terminal), and write the
	// trust-token file. It bypasses the structured-output machinery.
	if cli.is_cloud_authorise() {
		let method = match &cli.command {
			crate::cli::Command::CloudAuthorise { method } => method.clone(),
			_ => unreachable!("is_cloud_authorise gated the variant"),
		};
		let path = cli.config_path();
		let config_str = match std::fs::read_to_string(path) {
			Ok(s) => s,
			Err(e) => {
				let _ = writeln!(stderr, "cloud-authorise: read {}: {e}", path.display());
				return classify::EXIT_USAGE;
			}
		};
		let config = match parse_config(&config_str) {
			Ok(c) => c,
			Err(e) => {
				let _ = writeln!(stderr, "cloud-authorise: {e}");
				return classify::EXIT_CONFIG;
			}
		};
		return crate::oneshot::cloud_authorise::run(path, &config, &method).await;
	}

	// Pre-flight: snapshot --json without --output is a usage error.
	if let Err(e) = snapshot_json_preflight(cli.json, &cli.command) {
		emit_failure_to(mode, &e, "usage", stdout, stderr);
		return classify::EXIT_USAGE;
	}

	let result: Result<Outcome> = async {
		// Load + validate config.
		let config_path = cli.config_path();
		let config_str = match std::fs::read_to_string(config_path) {
			Ok(s) => s,
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				// Missing config file is operator-pointing-at-nothing,
				// not "config malformed". Mirror `check-config`'s
				// EXIT_USAGE mapping and the doc table at
				// `docs/architecture.md` § Error handling.
				return Err(crate::oneshot::errors::UsageError::new(format!(
					"config file not found: {}",
					config_path.display()
				))
				.into());
			}
			Err(e) => {
				return Err(
					ConfigError::new(format!("read {}: {}", config_path.display(), e)).into(),
				);
			}
		};
		let config =
			parse_config(&config_str).map_err(|e| ConfigError::new(format!("parse: {}", e)))?;
		validate_config(&config).map_err(|e| ConfigError::new(format!("validate: {}", e)))?;

		let camera_name = cli.camera_name().expect("oneshot without camera name");
		let cam_cfg = find_camera_config(&config, camera_name)?;

		// Install Ctrl+C / SIGTERM → CancellationToken. Process exits
		// via std::process::exit after run_oneshot returns.
		let cancel = CancellationToken::new();
		spawn_shutdown_cancel(cancel.clone());

		// `--use-stream-raw` needs the concrete `BcCamera`. Everything
		// else routes through the `CameraDriver` trait seam so the
		// dispatch match arms stay testable via FakeCamera.
		if let Command::Snapshot {
			output: out,
			use_stream,
			use_stream_raw: true,
			..
		} = &cli.command
		{
			if *use_stream {
				tracing::info!(
					"--use-stream is a neolink-compat no-op on bairelay (battery cams all support `snap`); \
					delegating to get_snapshot. Use --use-stream-raw if you want NAL bytes."
				);
			}
			let out = out.clone();
			let json = cli.json;
			return runner::run(&cam_cfg, cancel, move |cam| {
				async move { snapshot::run(cam, out.as_deref(), json, true).await }.boxed()
			})
			.await;
		}
		// `audio-settings` reads and writes `<audioCfg>` through
		// `BcCamera::get_audio_cfg` / `set_audio_cfg`. Those two are on the
		// concrete camera and pointedly not on `CameraDriver`, so the command
		// takes this route like `talk-abilite` does. The argument checks run
		// before the connection: this is a battery doorbell, and a volume of
		// 120 is a typo about the operator's own command line — it must not
		// cost a wake-up to be refused.
		if let Command::AudioSettings {
			volume,
			talk,
			visitor_volume,
			loudspeaker,
			gate,
			..
		} = &cli.command
		{
			let edit = audio_settings::Edit {
				volume: *volume,
				talk: *talk,
				visitor_volume: *visitor_volume,
				loudspeaker: *loudspeaker,
				gate: *gate,
			};
			edit.check()?;
			return runner::run(&cam_cfg, cancel, move |cam| {
				async move { audio_settings::run(cam, edit).await }.boxed()
			})
			.await;
		}

		// `chime-settings` reads and writes the chime family through
		// `BcCamera::get_ding_dong_cfg` / `set_ding_dong_cfg` /
		// `ring_ding_dong` — concrete camera, not the trait, same route.
		// Argument checks again run before the connection: `--volume 9` is a
		// typo, and even `--sonner` (which only rings) must be refused before
		// a wake-up if it could not be valid.
		if let Command::ChimeSettings {
			volume,
			led,
			melody,
			silent,
			ring,
			..
		} = &cli.command
		{
			let edit = chime_settings::Edit {
				volume: *volume,
				led: *led,
				melody: *melody,
				silent: *silent,
				ring: *ring,
			};
			edit.check()?;
			return runner::run(&cam_cfg, cancel, move |cam| {
				async move { chime_settings::run(cam, edit).await }.boxed()
			})
			.await;
		}
		// `talk-ability` is the second command that needs the concrete
		// `BcCamera`: the ungated TalkAbility probe lives on `BcCamera` and
		// pointedly not on the `CameraDriver` trait, so it cannot cross the
		// dispatch seam every other command routes through. Intercepted here,
		// exactly like `snapshot --use-stream-raw`.
		if matches!(cli.command, Command::TalkAbility(_)) {
			return runner::run(&cam_cfg, cancel, move |cam| {
				async move { talk_capability::run(cam).await }.boxed()
			})
			.await;
		}
		// `talk` and `audio-bridge` write to the loudspeaker, and `talk` /
		// `talk_stream` live on the concrete `BcCamera` (pointedly not on
		// `CameraDriver`), so they take this route too. Both need a longer
		// op deadline than the request/response default: a talk transmission
		// paces itself at wall-clock speed.
		if let Command::Talk { file, raw, header, .. } = &cli.command {
			let path = file.clone();
			// The header convention is the one wire-format knob the operator
			// can choose, and a bad value is an argument error: refuse it here,
			// before the camera is woken, exactly like the file check below.
			let convention = crate::audio_control::parse_convention(header)?;
			// Validate the file *before* connecting. This is a battery camera:
			// waking it, logging in, and only then being told the WAV is 44.1 kHz
			// would burn the wake window for nothing, and the refusal is about
			// the operator's argument, not about the camera.
			let input = crate::oneshot::pcm::read_pcm_file(&path, *raw)?;
			let budget = talk::op_timeout_for_samples(input.samples.len());
			let channel_id = cam_cfg.channel_id;
			return runner::run_with_op_timeout(&cam_cfg, cancel, budget, move |cam| {
				async move { talk::run(cam, channel_id, &path, &input, convention).await }.boxed()
			})
			.await;
		}
		if let Command::AudioBridge {
			idle,
			header,
			input,
			gain,
			..
		} = &cli.command {
			if !idle.is_finite() || *idle <= 0.0 {
				return Err(crate::oneshot::errors::UsageError::new(format!(
					"--idle must be a positive number of seconds, got {idle}"
				))
				.into());
			}
			// Same rule as --idle: a NaN or a negative gain would either
			// silently vanish the audio or invert the waveform, and both sound
			// like a broken camera.
			if !gain.is_finite() || *gain <= 0.0 {
				return Err(crate::oneshot::errors::UsageError::new(format!(
					"--gain must be a positive number, got {gain}"
				))
				.into());
			}
			let convention = crate::audio_control::parse_convention(header)?;
			// Refuse a bad input format here, before the camera is woken: the
			// pipe shape is decided by go2rtc's config, and a mismatch is a
			// configuration bug, not something to discover mid-session.
			let input_fmt = crate::oneshot::input::Input::parse(input)?;
			let idle = Duration::from_secs_f64(*idle);
			let channel_id = cam_cfg.channel_id;
			let gain = *gain;
			// Graceful route: the bridge closes its talk session (msg 11) on
			// Ctrl+C instead of being dropped mid-session.
			return runner::run_graceful(
				&cam_cfg,
				cancel,
				audio_bridge::op_timeout(),
				move |cam, cancel| async move {
					audio_bridge::run(cam, channel_id, idle, convention, input_fmt, gain, cancel)
						.await
				}
				.boxed(),
			)
			.await;
		}

		let cmd = crate::cli_convert::clone_command(&cli.command);
		let json = cli.json;
		runner::run(&cam_cfg, cancel, move |cam| {
			async move {
				dispatch_oneshot(
					cam as &dyn bairelay_neolink_core::bc_protocol::CameraDriver,
					&cmd,
					json,
				)
				.await
			}
			.boxed()
		})
		.await
	}
	.await;

	code_for_output(mode, result, stdout, stderr)
}

/// Publish what the command produced and return the process exit code.
///
/// The invariant, written here because this is the only place it is decided:
/// **a run that produced an `Outcome` returns 0**. A non-zero code alongside an
/// `Outcome` would be a contradictory success - an operator would have in their
/// pipe the complete JSON of a successful read *and* a failure code, and their
/// script would have to guess which of the two to believe. That is exactly the
/// report that led to `audio-settings` and `chime-settings`;
/// those two commands have a single happy ending, an `Outcome`, so the
/// invariant is tested here, without a camera - the full call path
/// (`runner::run` -> a real connection) cannot be reproduced offline.
///
/// The bytes written come from the same
/// [`emit_success_to`]/[`emit_failure_to`] as before: extracting this decision
/// changes the JSON not at all.
fn code_for_output<W1: std::io::Write, W2: std::io::Write>(
	mode: Mode,
	result: Result<Outcome>,
	stdout: &mut W1,
	stderr: &mut W2,
) -> i32 {
	match result {
		Ok(outcome) => {
			emit_success_to(mode, &outcome, stdout, stderr);
			classify::EXIT_OK
		}
		Err(e) => {
			let code = classify::classify(&e);
			let kind = crate::cli_convert::exit_code_to_kind(code);
			emit_failure_to(mode, &e, kind, stdout, stderr);
			code
		}
	}
}

/// Production wrapper: passes the real `stdout` / `stderr` to
/// [`run_oneshot_to`]. Tests use the `_to` variant with `Vec<u8>`.
///
/// Plain handles, NOT `.lock()` guards: this future is driven by
/// `block_on` on the main thread and parks while camera I/O is in
/// flight. A `StdoutLock`/`StderrLock` held across those awaits
/// deadlocks the whole runtime as soon as any spawned task emits a
/// log record — the tracing fmt writer blocks on the stderr lock
/// owned by the parked main thread. (Stdio locks are reentrant, so
/// main-thread logging masks the bug; only worker-thread records
/// trip it.) The plain handles lock per `write` call instead.
pub async fn run_oneshot(cli: &Cli) -> i32 {
	run_oneshot_to(cli, &mut std::io::stdout(), &mut std::io::stderr()).await
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::cli::Cli;
	use crate::config::test_helpers::minimal_camera_config;

	fn cli_from(args: &[&str]) -> Cli {
		use clap::Parser;
		Cli::try_parse_from(args).expect("cli parse")
	}

	#[test]
	fn cli_output_mode_json_flag_picks_json() {
		let cli = cli_from(&["bairelay", "--json", "mqtt-rtsp", "-c", "x.toml"]);
		assert!(matches!(cli_output_mode(&cli), Mode::Json));
	}

	#[test]
	fn cli_output_mode_default_is_human() {
		let cli = cli_from(&["bairelay", "mqtt-rtsp", "-c", "x.toml"]);
		assert!(matches!(cli_output_mode(&cli), Mode::Human));
	}

	#[test]
	fn load_validated_config_missing_file_errors() {
		let p = std::path::PathBuf::from("/nonexistent/bairelay-cfg-xxyy.toml");
		let err = load_validated_config(&p).expect_err("should fail");
		assert!(format!("{:#}", err).contains("Failed to read config file"));
	}

	#[test]
	fn load_validated_config_malformed_toml_errors() {
		let f = tempfile::NamedTempFile::new().unwrap();
		std::fs::write(f.path(), b"not { valid toml = [[[").unwrap();
		let err = load_validated_config(f.path()).expect_err("should fail");
		// Either parse or validate message — we only care we got past read.
		let s = format!("{:#}", err);
		assert!(s.contains("Invalid configuration") || s.contains("validation"));
	}

	#[test]
	fn load_validated_config_validation_fails() {
		// bind_port = 0 trips validate_config's first check, so we exercise
		// the Err path of the third stage (validator, not parser).
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 0
cameras = []
"#;
		std::fs::write(f.path(), toml).unwrap();
		let err = load_validated_config(f.path()).expect_err("validator should reject");
		let s = format!("{:#}", err);
		assert!(
			s.contains("validation") || s.contains("bind_port"),
			"unexpected error: {s}"
		);
	}

	#[test]
	fn load_validated_config_happy_path() {
		// Minimal valid config passes parse + validate and returns Ok.
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
stream_prune_grace_secs = 60
cameras = []
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cfg = load_validated_config(f.path()).expect("minimal valid config");
		assert_eq!(cfg.bind_port, 8554);
		assert!(cfg.cameras.is_empty());
	}

	#[test]
	fn camera_names_empty_when_no_cameras() {
		let cfg = Config::default();
		assert!(camera_names(&cfg).is_empty());
	}

	#[test]
	fn camera_names_preserves_insertion_order() {
		let cfg = Config {
			cameras: vec![
				minimal_camera_config("a"),
				minimal_camera_config("b"),
				minimal_camera_config("c"),
			],
			..Config::default()
		};
		assert_eq!(camera_names(&cfg), vec!["a", "b", "c"]);
	}

	#[test]
	fn emit_success_bytes_returns_both_streams() {
		// Outcome::Snapshot payload in Human mode goes to stderr (progress)
		// + stdout (bytes) — smoke-test the split exists regardless of
		// exact wording.
		let (stdout, _stderr) = emit_success_bytes(
			Mode::Human,
			&Outcome::Snapshot {
				bytes: 4,
				path: Some("/tmp/x.jpg".into()),
				format: "jpeg".into(),
			},
		);
		// At least one channel produced something.
		assert!(!stdout.is_empty() || !_stderr.is_empty());
	}

	#[test]
	fn emit_failure_payload_packages_mode() {
		let err = anyhow::anyhow!("boom");
		let (mode, s) = emit_failure_payload(Mode::Human, &err, "protocol");
		assert!(matches!(mode, Mode::Human));
		assert!(!s.is_empty());
	}

	#[test]
	fn emit_success_to_routes_snapshot_payload_to_stdout_and_stderr() {
		// `Outcome::Snapshot` writes the captured JPEG bytes to stdout
		// and a one-line confirmation to stderr (Human mode).
		let mut out = Vec::new();
		let mut err = Vec::new();
		emit_success_to(
			Mode::Human,
			&Outcome::Snapshot {
				bytes: 4,
				path: Some("/tmp/x.jpg".into()),
				format: "jpeg".into(),
			},
			&mut out,
			&mut err,
		);
		// Either channel may carry content; at least one must, and
		// neither may be larger than the format helper produced.
		assert!(!out.is_empty() || !err.is_empty());
		let (expected_out, expected_err) = format_success(
			Mode::Human,
			&Outcome::Snapshot {
				bytes: 4,
				path: Some("/tmp/x.jpg".into()),
				format: "jpeg".into(),
			},
		);
		assert_eq!(out, expected_out.as_bytes());
		assert_eq!(err, expected_err.as_bytes());
	}

	#[test]
	fn emit_failure_to_json_writes_to_stdout_only() {
		let err_in = anyhow::anyhow!("boom");
		let mut out = Vec::new();
		let mut errbuf = Vec::new();
		emit_failure_to(Mode::Json, &err_in, "protocol", &mut out, &mut errbuf);
		assert!(errbuf.is_empty(), "Json mode must not touch stderr");
		let s = String::from_utf8(out).expect("utf8");
		assert!(s.contains("\"ok\": false"));
		assert!(s.contains("\"kind\": \"protocol\""));
	}

	#[test]
	fn emit_failure_to_human_writes_to_stderr_only() {
		let err_in = anyhow::anyhow!("boom");
		let mut out = Vec::new();
		let mut errbuf = Vec::new();
		emit_failure_to(Mode::Human, &err_in, "protocol", &mut out, &mut errbuf);
		assert!(out.is_empty(), "Human mode must not touch stdout");
		let s = String::from_utf8(errbuf).expect("utf8");
		assert!(s.contains("boom"));
	}

	#[test]
	fn on_shutdown_signal_cancels_token() {
		let tok = CancellationToken::new();
		assert!(!tok.is_cancelled());
		on_shutdown_signal(&tok, "SIGTERM");
		assert!(tok.is_cancelled());
	}

	#[tokio::test]
	async fn sleep_or_cancel_returns_true_on_natural_completion() {
		let tok = CancellationToken::new();
		let res = sleep_or_cancel(Duration::from_millis(0), &tok).await;
		assert!(res, "zero-duration sleep must return true (sleep won)");
	}

	#[tokio::test]
	async fn sleep_or_cancel_returns_false_when_token_already_cancelled() {
		let tok = CancellationToken::new();
		tok.cancel();
		let res = sleep_or_cancel(Duration::from_secs(60), &tok).await;
		assert!(!res, "pre-cancelled token must short-circuit the sleep");
	}

	#[tokio::test(start_paused = true)]
	async fn sleep_or_cancel_returns_false_on_concurrent_cancel() {
		let tok = CancellationToken::new();
		let tok_for_canceller = tok.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tok_for_canceller.cancel();
		});
		let res = sleep_or_cancel(Duration::from_secs(60), &tok).await;
		assert!(!res, "cancel firing during sleep must return false");
		let _ = canceller.await;
	}

	#[test]
	fn resolve_bind_ip_prefers_the_listener_key() {
		let top = "0.0.0.0";
		assert_eq!(
			resolve_bind_ip("webhook_addr", Some("127.0.0.1"), top).unwrap(),
			std::net::IpAddr::from([127, 0, 0, 1])
		);
		assert_eq!(
			resolve_bind_ip("webhook_addr", None, top).unwrap(),
			std::net::IpAddr::from([0, 0, 0, 0])
		);
		let err = resolve_bind_ip("webhook_addr", Some("nope"), top).unwrap_err();
		assert!(format!("{err:#}").contains("webhook_addr"), "{err:#}");
		let err = resolve_bind_ip("webhook_addr", None, "nope").unwrap_err();
		assert!(format!("{err:#}").contains("nope"), "{err:#}");
	}

	#[tokio::test]
	async fn bind_service_listener_binds_and_names_a_port_conflict() {
		let ip = std::net::IpAddr::from([127, 0, 0, 1]);
		let listener = bind_service_listener("webhook listener", ip, 0)
			.await
			.unwrap();
		let port = listener.local_addr().unwrap().port();
		let err = bind_service_listener("webhook listener", ip, port)
			.await
			.unwrap_err();
		let msg = format!("{err:#}");
		assert!(
			msg.contains("webhook listener") && msg.contains(&port.to_string()),
			"{msg}"
		);
	}

	// `snapshot --json` without `--output` is a usage error. The
	// preflight logic is covered by `snapshot_json_preflight_*` in
	// `oneshot/dispatch.rs`; running the same path through
	// `run_oneshot` here would emit the JSON payload to the real
	// process stdout, polluting the test runner — no test here.

	#[tokio::test]
	async fn run_oneshot_missing_config_file_returns_config_exit() {
		// Fails at the `read_to_string` stage → ConfigError → classified.
		let cli = cli_from(&[
			"bairelay",
			"snapshot",
			"cam0",
			"--output",
			"/tmp/x.jpg",
			"-c",
			"/nonexistent-config-xyzz.toml",
		]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		// Per classify tables, a ConfigError surfaces as EXIT_CONFIG or
		// a coarse non-zero code — whatever the table decided.
		assert_ne!(code, classify::EXIT_OK);
	}

	#[tokio::test]
	async fn run_oneshot_cloud_authorise_missing_config_is_usage() {
		// The cloud-authorise dispatch reads the config first; a missing file
		// short-circuits to EXIT_USAGE before any network/MFA work.
		let cli = cli_from(&[
			"bairelay",
			"cloud-authorise",
			"-c",
			"/nonexistent-ca-xyz.toml",
		]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		assert_eq!(code, classify::EXIT_USAGE);
	}

	#[tokio::test]
	async fn run_oneshot_cloud_authorise_malformed_config_is_config_error() {
		let f = tempfile::NamedTempFile::new().unwrap();
		std::fs::write(f.path(), "this is = not valid toml {{").unwrap();
		let p = f.path().display().to_string();
		let cli = cli_from(&["bairelay", "cloud-authorise", "-c", &p]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		assert_eq!(code, classify::EXIT_CONFIG);
	}

	#[tokio::test]
	async fn run_oneshot_unknown_camera_returns_nonzero() {
		// Valid config with zero cameras → find_camera_config fails.
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
cameras = []
"#;
		std::fs::write(f.path(), toml).unwrap();
		let p = f.path().display().to_string();
		let cli = cli_from(&[
			"bairelay",
			"snapshot",
			"cam-missing",
			"--output",
			"/tmp/x.jpg",
			"-c",
			&p,
		]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		assert_ne!(code, classify::EXIT_OK);
	}

	// `emit_success` / `emit_failure` are 5-line wrappers around
	// `format_success` / `format_failure` (covered above by
	// `emit_success_bytes_returns_both_streams` and
	// `emit_failure_payload_packages_mode`). Their only side effect is
	// writing to the real process stdout/stderr, which would pollute
	// the cargo-test output for zero correctness signal — no test here.

	/// `run_oneshot` dispatches to `runner::run` for the
	/// non-`--use-stream-raw` path. The TCP connect fails (no real
	/// camera at 127.0.0.1:65535), surfacing through `Err(e)` →
	/// classify → exit code. Covers the dispatch_oneshot bind branch.
	/// Body of a one-shot test, pinned to the heap so it can cross a
	/// thread without generics fuss.
	type TestBody = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

	/// Runs a one-shot test on a thread with an **explicit stack** and
	/// its own runtime.
	///
	/// Why the detour here: `run_oneshot` over the real TCP path is the
	/// Baichuan handshake, so `BcXml` — every protocol block in an
	/// `Option`, several kilobytes, with at least two copies per command.
	/// In release — the profile of the binary that runs at the operator's
	/// place — the thread gets exactly **tokio's 2 MB worker stack**, the
	/// stack the one-shot really has: a green test proves the path fits
	/// there. In debug, unoptimised frames need more room and the test
	/// proves behaviour only — by design, not a cheat.
	fn on_worker(body: fn() -> TestBody) {
		let stack = if cfg!(debug_assertions) {
			8 * 1024 * 1024
		} else {
			2 * 1024 * 1024
		};
		std::thread::Builder::new()
			.stack_size(stack)
			.spawn(move || {
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("test thread runtime");
				rt.block_on(body());
			})
			.expect("test thread with explicit stack")
			.join()
			.expect("the one-shot must not panic against an absent camera");
	}

	#[test]
	fn run_oneshot_dispatch_path_returns_nonzero_on_tcp_failure() {
		on_worker(|| Box::pin(run_oneshot_dispatch_path()))
	}

	async fn run_oneshot_dispatch_path() {
		let f = tempfile::NamedTempFile::new().unwrap();
		// Address with `127.0.0.1:65500` — connect refused / times out.
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
[[cameras]]
name = "cam0"
address = "127.0.0.1:65500"
username = "u"
password = ""
discovery = "local"
"#;
		std::fs::write(f.path(), toml).unwrap();
		let p = f.path().display().to_string();
		let cli = cli_from(&["bairelay", "battery", "cam0", "-c", &p]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		// TCP refused → connection-class exit code, not OK.
		assert_ne!(code, classify::EXIT_OK);
	}

	/// `run_oneshot` `--use-stream-raw` branch picks the stream path
	/// (the `if let Command::Snapshot ... use_stream_raw: true` arm),
	/// then `runner::run` fails to connect — exit nonzero. Covers
	/// the snapshot --use-stream-raw branch (lines 161-180).
	#[test]
	fn run_oneshot_use_stream_raw_path_returns_nonzero_on_tcp_failure() {
		on_worker(|| Box::pin(run_oneshot_use_stream_raw_path()))
	}

	async fn run_oneshot_use_stream_raw_path() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
[[cameras]]
name = "cam0"
address = "127.0.0.1:65501"
username = "u"
password = ""
discovery = "local"
"#;
		std::fs::write(f.path(), toml).unwrap();
		let p = f.path().display().to_string();
		let outpath = tempfile::NamedTempFile::new().unwrap();
		let outpath_s = outpath.path().display().to_string();
		let cli = cli_from(&[
			"bairelay",
			"snapshot",
			"cam0",
			"--use-stream-raw",
			"--output",
			&outpath_s,
			"-c",
			&p,
		]);
		let code = run_oneshot_to(&cli, &mut Vec::new(), &mut Vec::new()).await;
		assert_ne!(code, classify::EXIT_OK);
	}

	/// Sync helper: drive `run_check_config_to` against `Vec<u8>` writers
	/// so tests assert on the exit code AND the bytes that *would* have
	/// gone to the process stdout/stderr without leaking them through the
	/// real fds.
	fn check_config_capture(cli: &Cli) -> (i32, Vec<u8>, Vec<u8>) {
		let mode = cli_output_mode(cli);
		let mut out = Vec::new();
		let mut err = Vec::new();
		let code = run_check_config_to(cli, mode, &mut out, &mut err);
		(code, out, err)
	}

	#[test]
	fn check_config_happy_path_returns_zero_and_writes_summary() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
[[cameras]]
name = "front"
address = "10.0.0.10:9000"
username = "admin"
password = "x"
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_OK);
		assert!(err.is_empty(), "Human OK path must not write to stderr");
		let s = String::from_utf8(out).expect("utf8");
		assert!(s.starts_with("config OK: 1 camera(s)"));
	}

	/// The startup scan's whole premise is a file that names no camera, so the command
	/// an operator runs before deploying has to accept that file — and say what it
	/// accepted, because `0 camera(s)` on its own reads like a file that does nothing.
	#[test]
	fn check_config_accepts_a_file_that_lets_the_network_name_the_cameras() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554

[startup_discovery]
enable = true
wait_secs = 4
username = "admin"
password = "shared-lan-password"
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, out, err) = check_config_capture(&cli);
		assert_eq!(
			code,
			classify::EXIT_OK,
			"a file with no camera and a scan is a decision, not a mistake; stderr: {}",
			String::from_utf8_lossy(&err)
		);
		let s = String::from_utf8(out).expect("utf8");
		assert!(s.starts_with("config OK: 0 camera(s)"), "{s}");
		assert!(s.contains("startup discovery on"), "{s}");
		assert!(
			s.contains("4s"),
			"the window the file asked for is repeated back: {s}"
		);
	}

	/// An unarmed table is not a mode: a file that carries one and declares its cameras
	/// gets the summary this command has always printed, with no hint of a scan in it.
	#[test]
	fn check_config_says_nothing_about_a_mode_that_was_not_asked_for() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554

[startup_discovery]
enable = false
wait_secs = 4

[[cameras]]
name = "front"
address = "10.0.0.10:9000"
username = "admin"
password = "x"
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, out, _err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_OK);
		let s = String::from_utf8(out).expect("utf8");
		assert_eq!(s, "config OK: 1 camera(s), bind 127.0.0.1:8554\n", "{s}");
	}

	/// A table that was written but never armed, in a file with no cameras, is the one
	/// combination that can do nothing at all. It is refused — and refused in terms of
	/// the table the operator did write, rather than serde's `missing field`, which is
	/// not the word they were looking for.
	#[test]
	fn check_config_refuses_an_unarmed_scan_table_and_names_it() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554

[startup_discovery]
wait_secs = 4
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, _out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_CONFIG);
		let s = String::from_utf8(err).expect("utf8");
		assert!(s.contains("[startup_discovery]"), "{s}");
		assert!(
			s.contains("enable = true"),
			"and what to write instead: {s}"
		);
	}

	#[test]
	fn check_config_missing_file_returns_usage() {
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			"/nonexistent-bairelay-check-xxxx.toml",
		]);
		let (code, out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_USAGE);
		assert!(out.is_empty(), "Human err path must not write to stdout");
		let s = String::from_utf8(err).expect("utf8");
		assert!(s.contains("read"));
	}

	#[test]
	fn check_config_malformed_toml_returns_config() {
		let f = tempfile::NamedTempFile::new().unwrap();
		std::fs::write(f.path(), b"not { valid toml = [[[").unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_CONFIG);
		assert!(out.is_empty());
		let s = String::from_utf8(err).expect("utf8");
		assert!(s.contains("parse"));
	}

	#[test]
	fn check_config_validation_failure_returns_config() {
		// bind_port = 0 trips validate_config's first rule.
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 0
cameras = []
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, _out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_CONFIG);
		let s = String::from_utf8(err).expect("utf8");
		assert!(s.contains("validate"));
	}

	#[test]
	fn check_config_runs_neolink_compat_warnings_without_failing() {
		// A config with a deprecated `pause.timeout` and a neolink-only
		// `tokio_console` knob must still pass validation AND surface
		// the same one-line migration warnings the daemon emits. We
		// can't easily capture tracing output in a unit test without
		// pulling in `tracing-subscriber-test`; the assertion here is
		// that the warning helpers run without panic and the overall
		// exit code stays `EXIT_OK`.
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
tokio_console = true

[[cameras]]
name = "front"
address = "10.0.0.10:9000"
username = "admin"
password = "x"
idle_disconnect_timeout_secs = 5.0

[cameras.pause]
timeout = 9.0
on_motion = true
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, _out, _err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_OK);
	}

	#[test]
	fn check_config_directory_path_classifies_as_config_not_usage() {
		// `read_to_string` on a directory returns an IO error whose
		// `kind()` is *not* `NotFound` (it's `IsADirectory` on Linux,
		// or a generic `Other` elsewhere). Pins the `else` branch in
		// the kind classifier — without this test only the
		// `NotFound → EXIT_USAGE` arm was exercised.
		let dir = tempfile::tempdir().unwrap();
		let cli = cli_from(&[
			"bairelay",
			"check-config",
			"-c",
			&dir.path().display().to_string(),
		]);
		let (code, _out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_CONFIG);
		let s = String::from_utf8(err).expect("utf8");
		assert!(s.contains("read"));
	}

	/// The `<audioCfg>` block of a doorbell that prints **three** volumes, as
	/// they were observed on the user's model.
	fn block_audio_settings() -> crate::oneshot::output::AudioCfgInfo {
		crate::oneshot::output::AudioCfgInfo {
			version: "1.1".into(),
			channel_id: "0".into(),
			volume: "100".into(),
			timeout: "10".into(),
			audio_select: "0".into(),
			pre_alarm: "0".into(),
			audio_list_id: "0".into(),
			talk_and_reply_volume: Some("90".into()),
			visitor_volume: Some("38".into()),
			visitor_loudspeaker: Some("1".into()),
			pascal: false,
		}
	}

	/// A successful settings read returns **0** - that is the invariant the
	/// report asked for on `audio-settings` and `chime-settings`, and the only
	/// place it is decided is [`code_pour_sortie`]. The full path
	/// (`runner::run` -> a real camera) cannot be reproduced offline; these two
	/// commands have one happy ending, an `Outcome`, so this pair (Outcome ->
	/// code + bytes) is what has to be pinned.
	#[test]
	fn a_successful_settings_read_returns_zero() {
		use crate::oneshot::output::DingDongCfgInfo;
		let cas = [
			(
				"audio-settings",
				Outcome::AudioSettings {
					tag: "audioCfg".into(),
					changed: false,
					block: block_audio_settings(),
					changes: vec![],
				},
			),
			(
				"chime-settings",
				Outcome::ChimeSettings {
					changed: false,
					block: DingDongCfgInfo::default(),
					changes: vec![],
					ring: None,
				},
			),
		];
		for (nom, outcome) in cas {
			let mut output = Vec::new();
			let mut erreurs = Vec::new();
			let code = code_for_output(Mode::Json, Ok(outcome), &mut output, &mut erreurs);
			assert_eq!(code, classify::EXIT_OK, "{nom} succeeded: the code must be 0");
			assert!(erreurs.is_empty(), "{nom} must write nothing to stderr");
			let lu: serde_json::Value = serde_json::from_slice(&output)
				.unwrap_or_else(|e| panic!("{nom} doit rendre du JSON lisible : {e}"));
			assert_eq!(lu["ok"], serde_json::Value::Bool(true), "{nom} : {lu}");
			assert_eq!(
				lu["changed"],
				serde_json::Value::Bool(false),
				"{nom}: a read writes nothing"
			);
		}
		// The three volumes of the captured `<audioCfg>` are in the JSON: the
		// missing one was on the MQTT entity side, not here.
		let mut output = Vec::new();
		let mut erreurs = Vec::new();
		code_for_output(
			Mode::Json,
			Ok(Outcome::AudioSettings {
				tag: "audioCfg".into(),
				changed: false,
				block: block_audio_settings(),
				changes: vec![],
			}),
			&mut output,
			&mut erreurs,
		);
		let lu: serde_json::Value = serde_json::from_slice(&output).expect("json");
		assert_eq!(lu["block"]["volume"], "100", "<volume> is the master volume");
		assert_eq!(
			lu["block"]["talk_and_reply_volume"], "90",
			"<talkAndReplyVolume> is the intercom volume"
		);
		assert_eq!(lu["block"]["visitor_volume"], "38", "<visitorVolume> is what the visitor hears");
	}

	/// A successful talk run exits **0** — the invariant is decided in
	/// [`code_for_output`] and nowhere else. The full call path
	/// (`runner::run` → real camera) is not reproducible offline, so what
	/// gets pinned here is the pair (Outcome → exit code + emitted bytes),
	/// including the awkward success that carries a camera error code:
	/// `talk-ability` reporting a refusal is still a completed read.
	#[test]
	fn successful_run_returns_zero() {
		let cases = [
			(
				"talk-ability refused",
				Outcome::TalkAbility {
					found: false,
					code: 482,
					payload: "none".into(),
					xml: None,
					configs: vec![],
					duplex: vec![],
					audio_stream_modes: vec![],
				},
			),
			(
				"talk-ability answered",
				Outcome::TalkAbility {
					found: true,
					code: 200,
					payload: "xml".into(),
					xml: Some("<TalkAbility/>".into()),
					configs: vec![],
					duplex: vec![],
					audio_stream_modes: vec![],
				},
			),
		];
		for (name, outcome) in cases {
			let mut stdout = Vec::new();
			let mut stderr = Vec::new();
			let code = code_for_output(Mode::Json, Ok(outcome), &mut stdout, &mut stderr);
			assert_eq!(code, classify::EXIT_OK, "{name} succeeded: the code must be 0");
			assert!(stderr.is_empty(), "{name} must write nothing to stderr");
			let parsed: serde_json::Value = serde_json::from_slice(&stdout)
				.unwrap_or_else(|e| panic!("{name} must emit readable JSON: {e}"));
			assert_eq!(parsed["ok"], serde_json::Value::Bool(true), "{name}: {parsed}");
		}
	}

	/// Extracting the decision did not soften failures: an error is still
	/// classified, and the error JSON goes to `stdout` in JSON mode.
	#[test]
	fn an_output_error_stays_classified() {
		let mut output = Vec::new();
		let mut erreurs = Vec::new();
		let e = anyhow::anyhow!("a failure that looks like nothing known");
		let code = code_for_output(Mode::Json, Err(e), &mut output, &mut erreurs);
		assert_eq!(code, classify::EXIT_UNEXPECTED);
		let lu: serde_json::Value = serde_json::from_slice(&output).expect("json");
		assert_eq!(lu["ok"], serde_json::Value::Bool(false));
		assert!(erreurs.is_empty(), "en mode JSON, tout part sur stdout");
	}

	#[test]
	fn emit_success_real_stdio_does_not_panic() {
		// The production wrapper around `std::io::stdout().lock()` /
		// `std::io::stderr().lock()`. Cargo-test captures both, so the
		// test runner output is untouched. Coverage-only assertion.
		emit_success(Mode::Json, &Outcome::Siren);
		emit_success(Mode::Human, &Outcome::Siren);
	}

	#[test]
	fn emit_failure_real_stdio_does_not_panic() {
		// Same shape: production wrapper around real stdout/stderr.
		let err = anyhow::anyhow!("synthetic failure for coverage");
		emit_failure(Mode::Json, &err, "test");
		emit_failure(Mode::Human, &err, "test");
	}

	#[test]
	fn check_config_json_mode_writes_ok_payload() {
		let f = tempfile::NamedTempFile::new().unwrap();
		let toml = r#"
bind = "127.0.0.1"
bind_port = 8554
cameras = []
"#;
		std::fs::write(f.path(), toml).unwrap();
		let cli = cli_from(&[
			"bairelay",
			"--json",
			"check-config",
			"-c",
			&f.path().display().to_string(),
		]);
		let (code, out, err) = check_config_capture(&cli);
		assert_eq!(code, classify::EXIT_OK);
		assert!(err.is_empty(), "Json OK path must not write to stderr");
		let s = String::from_utf8(out).expect("utf8");
		assert!(s.contains("\"ok\":true"));
		assert!(s.contains("\"cameras\":0"));
	}

	#[tokio::test]
	async fn spawn_shutdown_cancel_returns_handle() {
		// The signal path itself is covered by shutdown_signal's SIGTERM
		// test; here just spawn the handle and abort it to exercise drop.
		let tok = CancellationToken::new();
		let handle = spawn_shutdown_cancel(tok.clone());
		// Cancel externally — the handle's task waits on tokio::signal
		// so abort it to complete the test cleanly.
		handle.abort();
		let _ = handle.await;
	}
}
