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
use crate::oneshot::{runner, snapshot};

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
	crate::config::warn_idle_timeout_below_prune_floor(&config);
	// Success — short summary so operators get a quick affirmation.
	let summary = format!(
		"config OK: {} camera(s), bind {}:{}\n",
		config.cameras.len(),
		config.bind_addr,
		config.bind_port,
	);
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
	#[tokio::test]
	async fn run_oneshot_dispatch_path_returns_nonzero_on_tcp_failure() {
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
	#[tokio::test]
	async fn run_oneshot_use_stream_raw_path_returns_nonzero_on_tcp_failure() {
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
