use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Custom help template: clap 4 doesn't support `help_heading` on
/// Subcommand enum variants, so the Commands block is hand-rolled
/// here to split "Service modes" from "Camera commands". Keep in
/// sync with the `Command` enum below when adding / renaming
/// subcommands — `cargo test` will fail fast if the wording drifts.
const HELP_TEMPLATE: &str = "\
{name} {version} — {about-with-newline}
{usage-heading} {usage}

Service modes:
  mqtt           Run the MQTT bridge only
  rtsp           Run the RTSP server only
  mqtt-rtsp      Run both the MQTT bridge and RTSP server

Camera commands:
  reboot         Reboot one camera
  snapshot       Capture a JPEG (or H.264/265 with --use-stream-raw) (alias: image)
  battery        Print battery status
  floodlight     Query or toggle the floodlight (held 30 s on set)
  pir            Query or set the PIR sensor
  status-light   Query or toggle the blue status LED
  ptz            Pan / tilt / zoom / preset control
  presets        List PTZ presets (shorthand for `ptz preset`)
  services       Query or configure camera network services (bare form: list all)
  users          List or manage camera user accounts
  set-time       Set the camera clock to the host's current local time
  version        Print firmware + model info
  siren          Trigger the siren once
  abilities      Dump the camera's abilityInfo XML + parsed permissions
  webhook        Read the camera's HTTP webhook configuration
  audio-settings Read (or with options, rewrite) the audioCfg volumes
  chime-settings Read (or with options, rewrite) the paired-chime settings
  talk-ability   Probe <TalkAbility> ungated; print the raw reply
  talk           Send a PCM file through the camera loudspeaker (one shot)
  audio-bridge   Bridge stdin PCM to the camera loudspeaker (go2rtc backchannel)

Other:
  check-config   Validate the config file and exit (no camera connection)
  audio-check    Verify the talk ADPCM encoder against a PCM file (offline)
  cloud-authorise Clear cloud login verification (MFA) for this host (~30 days)
  help           Print help for the given subcommand

Options:
{options}\
";

/// RTSP relay for Reolink Baichuan cameras.
#[derive(Debug, Parser)]
#[command(
	name = "bairelay",
	version = env!("CARGO_PKG_VERSION"),
	about,
	help_template = HELP_TEMPLATE,
	arg_required_else_help = true,
)]
pub struct Cli {
	/// Path to the configuration file. Accepted anywhere on the command
	/// line for neolink compatibility (`bairelay -c cfg.toml <cmd>`
	/// works; so does `bairelay <cmd> -c cfg.toml`).
	#[arg(
		short = 'c',
		long = "config",
		global = true,
		default_value = "config.toml"
	)]
	pub config: PathBuf,

	/// Emit machine-readable JSON on stdout instead of the human summary.
	/// One-shot subcommands only; ignored by `mqtt` / `rtsp` / `mqtt-rtsp`.
	#[arg(long, global = true)]
	pub json: bool,

	/// Increase log verbosity. -v info→debug, -vv debug→trace +
	/// bairelay_neolink_core=debug, -vvv trace. RUST_LOG wins if set.
	#[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, global = true)]
	pub verbose: u8,

	#[command(subcommand)]
	pub command: Command,
}

/// Floodlight on/off literal for the `floodlight` subcommand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum FloodlightState {
	On,
	Off,
}

/// PTZ direction used by `ptz control`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PtzDirection {
	Up,
	Down,
	Left,
	Right,
	Stop,
}

/// Network service name used by `services`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ServiceName {
	Baichuan,
	Http,
	Https,
	Rtmp,
	Rtsp,
	Onvif,
}

/// User permission level used by `users add`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum UserTypeArg {
	User,
	Administrator,
}

/// Subcommands under `ptz`.
#[derive(Debug, Subcommand)]
pub enum PtzCommand {
	#[command(about = "Move to a stored preset. Omit <id> to list presets instead")]
	Preset { preset_id: Option<u8> },
	/// Save the camera's current position as a new preset.
	Assign { preset_id: u8, name: String },
	/// Perform a directional move.
	Control {
		amount: u32,
		direction: PtzDirection,
		speed: Option<u32>,
	},
	/// Set the zoom level (0.0–1.0 range on most cameras).
	Zoom { amount: f32 },
}

/// Subcommands under `services <service>`.
///
/// `port: u16` (not `u32`) so clap rejects values outside `0..=65535` at
/// CLI parse time. Port `0` is rejected later in the dispatch path —
/// allowing it through clap keeps the error message close to the call
/// site instead of buried in clap's per-arg formatting.
#[derive(Debug, Subcommand)]
pub enum ServiceAction {
	/// Print current state.
	Get,
	/// Enable the service without changing the port.
	On,
	/// Disable the service without changing the port.
	Off,
	/// Change the port; leave enable flag untouched.
	Port { port: u16 },
	/// Set port and enable flag together.
	Set { port: u16, enabled: OnOff },
}

/// On / off literal used by `services set` and future toggles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OnOff {
	On,
	Off,
}

/// Subcommands under `users`.
///
/// `password` is intentionally `Option<String>` on `Add` and
/// `Password` so the operator can omit it and bairelay will prompt
/// (TTY, no-echo) or read from stdin. Passing it positionally still
/// works for backward compat but exposes the value to `ps auxww` /
/// shell history; the prompt path is the safer default.
#[derive(Debug, Subcommand)]
pub enum UserAction {
	/// List all configured users.
	List,
	#[command(
		about = "Create a new user. Omit <password> to be prompted (TTY) or to read from stdin (when piped)"
	)]
	Add {
		name: String,
		user_type: UserTypeArg,
		password: Option<String>,
	},
	#[command(
		about = "Change the password of an existing user. Omit <password> to be prompted (TTY) or to read from stdin (when piped)"
	)]
	Password {
		name: String,
		password: Option<String>,
	},
	/// Delete a user.
	Delete { name: String },
}

/// Arguments common to every one-shot subcommand.
#[derive(Debug, clap::Args)]
pub struct OneShotArgs {
	#[arg(help = "Camera name (must match a [[cameras]] entry in the config)")]
	pub camera: String,
}

#[derive(Debug, Subcommand)]
pub enum Command {
	/// Run the MQTT bridge only.
	Mqtt,

	/// Run the RTSP server only.
	Rtsp {
		#[arg(
			long = "dump-bcmedia",
			value_name = "DIR",
			help = "Dump raw BcMedia packets to <dir>/<camera>-<stream>.bcmedia for fixture capture and replay testing. Opt-in; no effect on live streaming. See docs/testing.md for the capture playbook"
		)]
		dump_bcmedia: Option<PathBuf>,
	},

	/// Run both the MQTT bridge and RTSP server.
	MqttRtsp {
		#[arg(
			long = "dump-bcmedia",
			value_name = "DIR",
			help = "Dump raw BcMedia packets to <dir>/<camera>-<stream>.bcmedia for fixture capture and replay testing. Opt-in; no effect on live streaming. See docs/testing.md for the capture playbook"
		)]
		dump_bcmedia: Option<PathBuf>,
	},

	/// Reboot one camera.
	Reboot(OneShotArgs),

	/// Capture one JPEG snapshot (or a raw H.264/H.265 bitstream with `--use-stream-raw`).
	#[command(alias = "image")]
	Snapshot {
		#[command(flatten)]
		common: OneShotArgs,
		/// Write the output here instead of stdout. `--file-path` is a
		/// neolink-compat alias.
		#[arg(short = 'f', long = "output", visible_alias = "file-path")]
		output: Option<PathBuf>,
		/// Neolink-compat no-op. In neolink, `--use-stream` decodes the
		/// live video stream into a JPEG via gstreamer for cameras that
		/// lack the `snap` ability. Bairelay's scope is battery cameras,
		/// which all support `snap`, so this flag delegates to the
		/// standard snapshot path and prints a one-line note to stderr.
		/// Use `--use-stream-raw` if you actually want the raw NAL dump.
		#[arg(long = "use-stream", conflicts_with = "use_stream_raw")]
		use_stream: bool,
		/// Pull from the live video stream (first I-frame) and write a
		/// raw H.264 / H.265 Annex-B bitstream. Decode with
		/// `ffmpeg -i <file> -vframes 1 out.jpg`.
		#[arg(long = "use-stream-raw")]
		use_stream_raw: bool,
	},

	/// Print battery status.
	Battery(OneShotArgs),

	/// Toggle the floodlight on or off (held 30 s). Omit the state to read.
	Floodlight {
		#[command(flatten)]
		common: OneShotArgs,
		/// Omit to read current state; pass on/off to change it.
		#[arg(value_enum)]
		state: Option<FloodlightState>,
	},

	/// List PTZ presets.
	Presets(OneShotArgs),

	/// Set the camera clock to the host's current local time.
	SetTime(OneShotArgs),

	/// Print firmware + model info.
	Version(OneShotArgs),

	/// Trigger the siren once.
	Siren(OneShotArgs),

	/// Dump the camera's `abilityInfo` XML and the flat list of
	/// permission triples it carries. Read-only; safe to run on a
	/// production camera. Used to capture ground-truth ability strings
	/// for `MissingAbility` gate decisions.
	Abilities(OneShotArgs),

	/// Read the camera's HTTP webhook configuration (enable, url,
	/// verify_cert). Reports "unsupported" (exit 0) when the firmware
	/// answers with an empty body or a persistent error reply, after
	/// retrying the wake-time 400. Read-only.
	Webhook(OneShotArgs),

	/// Read the camera's `<audioCfg>` block: the loudspeaker `volume` and, on
	/// a doorbell, `talkAndReplyVolume` (how loud the answer you speak through
	/// the app comes out), `visitorVolume` (the pre-set greeting clips) and
	/// `visitorLoudspeaker`. Baichuan cmd 264 reads it, 265 writes it, and it
	/// is the only place the intercom volume exists.
	///
	/// With no option this is read-only. Each write option rewrites one tag of
	/// the block the camera just printed: the command reads the block, patches
	/// only the tags you passed, and sends the **whole block** back — a block
	/// that omits tags the camera sent is rejected, or silently ignored, by
	/// this firmware. A tag the camera didn't print is never invented, and
	/// asking to change one it doesn't have is an error rather than a silent
	/// addition. The block is read again after the write and printed, so the
	/// before / after you see is what the camera kept.
	///
	/// Tags whose meaning is undocumented (`audioSelect`, `audioListId`,
	/// `preAlarm`, `timeout`) are printed and echoed back untouched.
	AudioSettings {
		#[command(flatten)]
		common: OneShotArgs,
		/// volume — the loudspeaker volume, 0-100.
		#[arg(long = "volume", value_name = "N")]
		volume: Option<u8>,
		/// talkAndReplyVolume — the intercom volume, 0-100.
		#[arg(long = "talk", value_name = "N")]
		talk: Option<u8>,
		/// visitorVolume — how loud the pre-set message clips play, 0-100.
		#[arg(long = "visitor-volume", value_name = "N")]
		visitor_volume: Option<u8>,
		/// visitorLoudspeaker — whether the loudspeaker is live for a visitor
		/// (0 or 1).
		#[arg(long = "loudspeaker", value_name = "0|1")]
		loudspeaker: Option<u8>,
		/// Let the camera's advertised abilities veto the command before it is
		/// sent. Off by default: this doorbell's `abilityInfo` under-reports,
		/// so the answer to cmd 264 is the only proof of support that counts.
		#[arg(long = "gate")]
		gate: bool,
	},

	/// Read the paired chime (ding-dong): the paired list
	/// (cmd 484), the per-chime volume and LED (485), the mute window (609),
	/// the per-event melodies (486) and the hardwired block (483) — the
	/// doorbell answers all of these for the chime, which has no address of
	/// its own.
	///
	/// The camera must be REACHABLE and the chime PAIRED for anything to
	/// answer: each part the camera doesn't have refuses its question, and the
	/// report names which parts were refused — that is how "no wireless chime"
	/// tells itself apart from a broken read. On a wired install the 483 read
	/// itself can make the chime sound.
	///
	/// With no option this is read-only. Each write option patches one tag of
	/// the block the camera just printed and rewrites the whole received
	/// block — a tag the camera didn't print is never invented. Only parts
	/// whose text actually changed are sent: re-writing the silent-mode block
	/// would re-arm the mute countdown, so an unchanged value costs no
	/// command. The family is read again after any write and printed, so the
	/// before / after you see is what the camera kept.
	ChimeSettings {
		#[command(flatten)]
		common: OneShotArgs,
		/// volLevel — the chime volume, 0-4 (the documented range; the true
		/// ceiling is undocumented).
		#[arg(long = "volume", value_name = "N")]
		volume: Option<u8>,
		/// ledState — the chime status LED (0 or 1).
		#[arg(long = "led", value_name = "0|1")]
		led: Option<u8>,
		/// musicId — the melody for every event whose block printed one
		/// (integer; no melody list is documented, so read 486 first).
		#[arg(long = "melody", value_name = "ID")]
		melody: Option<u32>,
		/// Mute the chime for N (unit undocumented; the reference client sends
		/// seconds). 0 unmutes.
		#[arg(long = "silent", value_name = "N")]
		silent: Option<u32>,
		/// Ring the chime once after the read (and after the write, if any):
		/// the live test — hearing it is the only proof of life the protocol
		/// cannot give. It makes noise, on purpose, because you asked.
		#[arg(long = "ring")]
		ring: bool,
	},

	/// Ask the camera for its `<TalkAbility>` element (msg id 10) and print
	/// the answer verbatim, **bypassing** the `two_way_audio` ability gate
	/// that `talk_ability()` applies. Read-only: only the capability read is
	/// sent — no talk config (201), no audio (202), no reset (11). Reports
	/// the reply code unedited (200 ok, 405 unknown command on this family,
	/// 482 elsewhere), so an AbilityInfo that under-reports can be checked
	/// against what the box actually answers. Needs the concrete session, so
	/// it runs on the same route as `snapshot --use-stream-raw`.
	TalkAbility(OneShotArgs),

	/// Speak through the camera's loudspeaker once: validate a PCM file,
	/// encode it into the ADPCM geometry the camera's own `<TalkAbility>`
	/// answer asks for, and pace it out at real speed (one 64 ms block per
	/// block — sending it all at once overruns the firmware).
	///
	/// Accepts a 16-bit signed PCM WAV at 16000 Hz mono, or headerless bytes
	/// with `--raw`. Any other format is refused, naming what was found in
	/// the file instead of silently re-encoding it. Needs
	/// `two_way_audio = true` on the camera when its `abilityInfo` fails to
	/// advertise the microphone (see README).
	Talk {
		#[command(flatten)]
		common: OneShotArgs,
		/// Audio file to send (PCM WAV, or headerless with --raw).
		#[arg(long = "file", value_name = "PATH")]
		file: PathBuf,
		/// The file is headerless signed 16-bit LE PCM at 16000 Hz mono.
		#[arg(long = "raw")]
		raw: bool,
		/// What the 4 header bytes of each ADPCM block carry: `sample`
		/// (default) puts that block's first sample there, so the loudspeaker
		/// hears every sample exactly once; `repeat` puts the previous
		/// block's last sample there instead, which costs one duplicated
		/// sample per block (every 64 ms) and is only here to A/B the two on
		/// the hardware. Compare them offline with `audio-check --compare`.
		#[arg(long = "header", value_name = "HEADER", default_value = "sample")]
		header: String,
	},

	/// Live audio bridge: read signed 16-bit LE PCM at 16000 Hz mono from
	/// **stdin** and speak it through the camera until stdin closes. This is
	/// the Home Assistant / go2rtc backchannel entry point (`exec` source
	/// with `#backchannel=1#audio=s16le/16000`); the camera's microphone
	/// comes back on the RTSP stream go2rtc already pulls, so nothing here
	/// writes to stdout.
	///
	/// A talk session is opened at the first byte and closed on silence or
	/// EOF, because a killed process can never send the reset (msg 11) — see
	/// the module docs. EOF is the normal end of a call and exits 0.
	AudioBridge {
		#[command(flatten)]
		common: OneShotArgs,
		/// Silence, in seconds, that ends the current talk session.
		#[arg(long = "idle", value_name = "SEC", default_value_t = 2.0)]
		idle: f64,
		/// Block-header convention, same meaning (and same default) as
		/// `talk --header`; the live bridge shares the encoder, so it must be
		/// able to speak either layout.
		#[arg(long = "header", value_name = "HEADER", default_value = "sample")]
		header: String,
		/// Format of the bytes arriving on stdin: `s16le/16000` for raw PCM, or
		/// `pcmu/8000` / `pcma/8000` for G.711. go2rtc pipes the payload the
		/// browser actually negotiated and transcodes nothing, and a browser can
		/// only negotiate what it offered — G.711 at 8 kHz, never 16-bit PCM at
		/// 16 kHz. Whatever go2rtc declares in `#audio=` must be repeated here.
		#[arg(long = "input", value_name = "FORMAT", default_value = "s16le/16000")]
		input: String,
		/// Input gain, applied to the decoded samples before the ADPCM encoder.
		/// A browser microphone typically arrives well below full scale and the
		/// doorbell's amplifier is small, so the bridge is audibly quiet with a
		/// factor of 1. This is the only loudness knob that is not a camera
		/// setting; above ~4 the ADPCM encoder starts to clip on loud peaks.
		#[arg(long = "gain", value_name = "FACTOR", default_value = "1")]
		gain: f32,
	},

	/// Check the talk audio path **locally**: encode a file with the very
	/// encoder `talk` uses, decode it back with the very decoder the RTSP
	/// side uses for the camera's own audio, and print what came out. No
	/// camera, no network, no config file — this measures the encoder, the
	/// block geometry and the timer, which is everything about the crackle
	/// that can be measured without a loudspeaker.
	///
	/// Reports sample counts in and out (samples lost, invented or played
	/// twice), the error against the input, and whether that error jumps at
	/// the block seams (`--compare` runs both header conventions side by
	/// side). `--bench N` instead benches this machine's 64 ms timer.
	AudioCheck {
		/// Audio file to check (PCM WAV, or headerless with --raw).
		#[arg(long = "file", value_name = "PATH")]
		file: Option<PathBuf>,
		/// The file is headerless signed 16-bit LE PCM at 16000 Hz mono.
		#[arg(long = "raw")]
		raw: bool,
		/// Header convention to measure (`sample` or `repeat`).
		#[arg(long = "header", value_name = "HEADER")]
		header: Option<String>,
		/// Run both conventions and print the comparison.
		#[arg(long = "compare")]
		compare: bool,
		/// `lengthPerEncoder` to assume (the doorbell answers 1024).
		#[arg(long = "block-length", value_name = "N", default_value_t = 1024)]
		block_length: u16,
		/// Skip the audio check and bench the block timer instead: N sleeps of
		/// 64 ms, relative (what `talk()` does) against absolute deadlines.
		#[arg(long = "bench", value_name = "N")]
		bench: Option<usize>,
	},


	/// Query or set the PIR sensor.
	Pir {
		#[command(flatten)]
		common: OneShotArgs,
		/// Omit to read current state; pass on/off to change it.
		#[arg(value_enum)]
		state: Option<OnOff>,
	},

	/// Query or toggle the blue status LED.
	#[command(name = "status-light")]
	StatusLight {
		#[command(flatten)]
		common: OneShotArgs,
		/// Omit to read current state; pass on/off to change it.
		#[arg(value_enum)]
		state: Option<OnOff>,
	},

	/// Pan / tilt / zoom / preset control. With no sub-command, lists presets.
	Ptz {
		#[command(flatten)]
		common: OneShotArgs,
		#[command(subcommand)]
		cmd: Option<PtzCommand>,
	},

	/// Query or configure camera network services. No service lists
	/// all six; no action defaults to `get`.
	Services {
		#[command(flatten)]
		common: OneShotArgs,
		service: Option<ServiceName>,
		#[command(subcommand)]
		action: Option<ServiceAction>,
	},

	/// List or manage camera user accounts. No action defaults to `list`.
	Users {
		#[command(flatten)]
		common: OneShotArgs,
		#[command(subcommand)]
		action: Option<UserAction>,
	},

	/// Validate the configuration file and exit. No camera connection.
	/// Exits with code 0 on success, 3 on parse / validation failure, 2
	/// on missing file. Useful in CI / Ansible / pre-deploy hooks.
	#[command(name = "check-config")]
	CheckConfig,

	/// Interactively clear Reolink's login verification (MFA) for this host
	/// and store the ~30-day trust token beside the config, so cloud cameras
	/// connect headlessly from an IP Reolink would otherwise challenge. Run
	/// once per host (re-run when the token lapses).
	#[command(name = "cloud-authorise")]
	CloudAuthorise {
		/// Verification method: `email` (a code is emailed), `totp` (read from
		/// your authenticator app), or `backup_code` (one of your saved codes).
		#[arg(long, default_value = "email")]
		method: String,
	},

	/// Render Home Assistant Supervisor options + a TOML overlay into
	/// a complete bairelay config file. Internal subcommand used by the
	/// HA add-on entrypoint shim; operators do not invoke this directly.
	#[command(hide = true)]
	RenderHassioConfig {
		#[arg(long = "options-json", value_name = "PATH")]
		options_json: PathBuf,
		#[arg(long = "overlay", value_name = "PATH")]
		overlay: Option<PathBuf>,
		#[arg(long = "mqtt-host")]
		mqtt_host: Option<String>,
		#[arg(long = "mqtt-port")]
		mqtt_port: Option<u16>,
		#[arg(long = "mqtt-user")]
		mqtt_user: Option<String>,
		#[arg(long = "mqtt-pass")]
		mqtt_pass: Option<String>,
		#[arg(long = "mqtt-ssl", default_value = "false")]
		mqtt_ssl: bool,
		#[arg(long = "output", value_name = "PATH")]
		output: Option<PathBuf>,
	},
}

impl Cli {
	/// Returns the config path. Now a single global arg, so this just
	/// forwards `self.config`.
	pub fn config_path(&self) -> &std::path::Path {
		&self.config
	}

	/// Returns the `--dump-bcmedia` directory, if set. Only the RTSP-bearing
	/// subcommands accept the flag; every other mode returns `None`.
	pub fn dump_bcmedia_path(&self) -> Option<&std::path::Path> {
		match &self.command {
			Command::Rtsp { dump_bcmedia, .. } => dump_bcmedia.as_deref(),
			Command::MqttRtsp { dump_bcmedia, .. } => dump_bcmedia.as_deref(),
			Command::Mqtt
			| Command::Reboot(_)
			| Command::Snapshot { .. }
			| Command::Battery(_)
			| Command::Floodlight { .. }
			| Command::Presets(_)
			| Command::SetTime(_)
			| Command::Version(_)
			| Command::Siren(_)
			| Command::Abilities(_)
			| Command::Webhook(_)
			| Command::AudioSettings { .. }
			| Command::ChimeSettings { .. }
			| Command::TalkAbility(_)
			| Command::Talk { .. }
			| Command::AudioBridge { .. }
			| Command::Pir { .. }
			| Command::StatusLight { .. }
			| Command::Ptz { .. }
			| Command::Services { .. }
			| Command::Users { .. }
			| Command::CheckConfig
			| Command::CloudAuthorise { .. }
			| Command::RenderHassioConfig { .. }
			| Command::AudioCheck { .. } => None,
		}
	}

	/// Returns true for the `check-config` subcommand. Routed through
	/// the one-shot pipeline (so it exits via `std::process::exit`) but
	/// short-circuits before anything that needs a camera.
	pub fn is_check_config(&self) -> bool {
		matches!(self.command, Command::CheckConfig)
	}

	/// Returns true for the `cloud-authorise` subcommand — a config-level,
	/// no-camera interactive command (like `check-config`), routed through the
	/// one-shot pipeline but short-circuited before any camera connection.
	pub fn is_cloud_authorise(&self) -> bool {
		matches!(self.command, Command::CloudAuthorise { .. })
	}

	/// Returns true if the selected mode includes MQTT.
	pub fn wants_mqtt(&self) -> bool {
		matches!(&self.command, Command::Mqtt | Command::MqttRtsp { .. })
	}

	/// Returns true if the selected mode includes RTSP.
	pub fn wants_rtsp(&self) -> bool {
		matches!(
			&self.command,
			Command::Rtsp { .. } | Command::MqttRtsp { .. }
		)
	}

	/// Returns true for one-shot control subcommands (reboot, snapshot, …)
	/// and `check-config`. The three long-running service modes return false.
	pub fn is_oneshot(&self) -> bool {
		!matches!(
			self.command,
			Command::Mqtt | Command::Rtsp { .. } | Command::MqttRtsp { .. }
		)
	}

	/// Returns the target camera name for one-shot subcommands; `None` for
	/// the long-running service modes and for `check-config`.
	pub fn camera_name(&self) -> Option<&str> {
		match &self.command {
			Command::Mqtt
			| Command::Rtsp { .. }
			| Command::MqttRtsp { .. }
			| Command::CheckConfig
			| Command::CloudAuthorise { .. }
			// Local-only check: no camera, so no camera to name.
			| Command::AudioCheck { .. }
			| Command::RenderHassioConfig { .. } => None,
			Command::Reboot(a)
			| Command::Battery(a)
			| Command::Presets(a)
			| Command::SetTime(a)
			| Command::Version(a)
			| Command::Siren(a)
			| Command::Abilities(a)
			| Command::Webhook(a)
			| Command::TalkAbility(a) => Some(&a.camera),
			Command::Snapshot { common, .. }
			| Command::AudioSettings { common, .. }
			| Command::ChimeSettings { common, .. }
			| Command::Talk { common, .. }
			| Command::AudioBridge { common, .. }
			| Command::Floodlight { common, .. }
			| Command::Pir { common, .. }
			| Command::StatusLight { common, .. }
			| Command::Ptz { common, .. }
			| Command::Services { common, .. }
			| Command::Users { common, .. } => Some(&common.camera),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use clap::Parser;

	#[test]
	fn parse_mqtt_only_mode() {
		let cli = Cli::try_parse_from(["bairelay", "mqtt", "-c", "foo.toml"]).unwrap();
		assert!(cli.wants_mqtt());
		assert!(!cli.wants_rtsp());
		assert_eq!(cli.config_path(), std::path::Path::new("foo.toml"));
		assert_eq!(cli.dump_bcmedia_path(), None);
	}

	#[test]
	fn parse_rtsp_only_mode_with_dump() {
		let cli = Cli::try_parse_from([
			"bairelay",
			"rtsp",
			"-c",
			"bar.toml",
			"--dump-bcmedia",
			"/tmp/dump",
		])
		.unwrap();
		assert!(!cli.wants_mqtt());
		assert!(cli.wants_rtsp());
		assert_eq!(cli.config_path(), std::path::Path::new("bar.toml"));
		assert_eq!(
			cli.dump_bcmedia_path(),
			Some(std::path::Path::new("/tmp/dump"))
		);
	}

	#[test]
	fn parse_mqtt_rtsp_combined() {
		let cli = Cli::try_parse_from(["bairelay", "mqtt-rtsp"]).unwrap();
		assert!(cli.wants_mqtt());
		assert!(cli.wants_rtsp());
		assert_eq!(cli.config_path(), std::path::Path::new("config.toml"));
		assert_eq!(cli.dump_bcmedia_path(), None);
	}

	#[test]
	fn dump_bcmedia_path_is_none_for_mqtt_only() {
		let cli = Cli::try_parse_from(["bairelay", "mqtt"]).unwrap();
		assert!(cli.dump_bcmedia_path().is_none());
	}

	#[test]
	fn parse_reboot_subcommand() {
		// Global -c accepted after the subcommand as before...
		let cli = Cli::try_parse_from(["bairelay", "reboot", "-c", "c.toml", "driveway"]).unwrap();
		let Command::Reboot(args) = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(args.camera, "driveway");
		assert_eq!(cli.config_path(), std::path::Path::new("c.toml"));

		// ...and, for neolink drop-in compat, also before.
		let cli = Cli::try_parse_from(["bairelay", "-c", "c.toml", "reboot", "driveway"]).unwrap();
		assert_eq!(cli.config_path(), std::path::Path::new("c.toml"));
		let Command::Reboot(args) = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(args.camera, "driveway");
	}

	#[test]
	fn parse_image_is_alias_for_snapshot() {
		// Neolink calls this `image`; we keep `snapshot` as the
		// canonical name but accept `image` too for parity.
		let cli = Cli::try_parse_from(["bairelay", "image", "driveway", "--output", "/tmp/x.jpg"])
			.unwrap();
		let Command::Snapshot {
			common,
			output,
			use_stream,
			use_stream_raw,
		} = &cli.command
		else {
			panic!("wrong variant — expected Snapshot")
		};
		assert_eq!(common.camera, "driveway");
		assert_eq!(output.as_deref(), Some(std::path::Path::new("/tmp/x.jpg")));
		assert!(!use_stream);
		assert!(!use_stream_raw);
	}

	#[test]
	fn parse_snapshot_with_output() {
		// Neolink-compat: `-c` accepted BEFORE the subcommand.
		let cli = Cli::try_parse_from([
			"bairelay",
			"-c",
			"c.toml",
			"snapshot",
			"driveway",
			"--output",
			"/tmp/s.jpg",
		])
		.unwrap();
		let Command::Snapshot { common, output, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "driveway");
		assert_eq!(output.as_deref(), Some(std::path::Path::new("/tmp/s.jpg")));
		assert_eq!(cli.config_path(), std::path::Path::new("c.toml"));
	}

	#[test]
	fn parse_snapshot_file_path_alias() {
		// `-f` and `--file-path` are neolink-compat aliases for `--output`.
		for arg in ["-f", "--file-path"] {
			let cli = Cli::try_parse_from(["bairelay", "snapshot", "driveway", arg, "/tmp/x.jpg"])
				.unwrap();
			let Command::Snapshot { output, .. } = &cli.command else {
				panic!("wrong variant")
			};
			assert_eq!(
				output.as_deref(),
				Some(std::path::Path::new("/tmp/x.jpg")),
				"flag {} should parse as --output",
				arg
			);
		}
	}

	#[test]
	fn parse_snapshot_use_stream_is_noop_flag() {
		// --use-stream is a neolink-compat no-op; it parses but the
		// dispatch layer just logs a note and delegates to snap.
		let cli = Cli::try_parse_from([
			"bairelay",
			"snapshot",
			"driveway",
			"--use-stream",
			"-f",
			"/tmp/x.jpg",
		])
		.unwrap();
		let Command::Snapshot {
			use_stream,
			use_stream_raw,
			..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert!(use_stream);
		assert!(!use_stream_raw);
	}

	#[test]
	fn parse_snapshot_use_stream_raw_emits_nal_bytes() {
		let cli = Cli::try_parse_from([
			"bairelay",
			"snapshot",
			"driveway",
			"--use-stream-raw",
			"-f",
			"/tmp/x.h265",
		])
		.unwrap();
		let Command::Snapshot {
			output,
			use_stream,
			use_stream_raw,
			..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert!(!use_stream);
		assert!(use_stream_raw);
		assert_eq!(output.as_deref(), Some(std::path::Path::new("/tmp/x.h265")));
	}

	#[test]
	fn parse_snapshot_conflicting_stream_flags_errors() {
		// clap's conflicts_with refuses both flags at once.
		let err = Cli::try_parse_from([
			"bairelay",
			"snapshot",
			"driveway",
			"--use-stream",
			"--use-stream-raw",
			"-f",
			"/tmp/x",
		])
		.unwrap_err();
		assert!(
			err.to_string().contains("use-stream"),
			"expected conflict hint, got: {}",
			err
		);
	}

	#[test]
	fn parse_battery_json_and_verbose() {
		let cli =
			Cli::try_parse_from(["bairelay", "-vv", "--json", "battery", "driveway"]).unwrap();
		assert!(cli.json);
		assert_eq!(cli.verbose, 2);
		let Command::Battery(args) = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(args.camera, "driveway");
	}

	#[test]
	fn parse_floodlight_read_and_set() {
		// No-arg form now reads instead of erroring.
		let cli = Cli::try_parse_from(["bairelay", "floodlight", "driveway"]).unwrap();
		let Command::Floodlight { common, state } = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "driveway");
		assert!(state.is_none());

		let cli = Cli::try_parse_from(["bairelay", "floodlight", "driveway", "on"]).unwrap();
		let Command::Floodlight { state, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(matches!(state, Some(FloodlightState::On)));
	}

	#[test]
	fn parse_all_oneshots_default_config() {
		let cases: &[&[&str]] = &[
			&["reboot", "driveway"],
			&["snapshot", "driveway"],
			&["battery", "driveway"],
			&["floodlight", "driveway", "on"],
			&["presets", "driveway"],
			&["set-time", "driveway"],
			&["version", "driveway"],
			&["siren", "driveway"],
			&["abilities", "driveway"],
		];
		for args in cases {
			let mut full = vec!["bairelay"];
			full.extend_from_slice(args);
			let cli = Cli::try_parse_from(full).unwrap();
			assert_eq!(
				cli.config_path(),
				std::path::Path::new("config.toml"),
				"default config for {:?}",
				args
			);
		}
	}

	#[test]
	fn is_oneshot_true_for_new_variants_only() {
		let cli = Cli::try_parse_from(["bairelay", "reboot", "driveway"]).unwrap();
		assert!(cli.is_oneshot());
		let cli = Cli::try_parse_from(["bairelay", "mqtt-rtsp"]).unwrap();
		assert!(!cli.is_oneshot());
	}

	/// The two talk senders need their own flags, a camera name (that is what
	/// routes them to the concrete-`BcCamera` intercept), and a required
	/// `--file` so a forgotten flag cannot become a silent no-op.
	#[test]
	fn parse_talk_and_audio_bridge() {
		let cli = Cli::try_parse_from([
			"bairelay",
			"talk",
			"doorbell",
			"--file",
			"test.wav",
			"--raw",
		])
		.unwrap();
		let Command::Talk {
			common,
			file,
			raw,
			header,
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "doorbell");
		assert_eq!(*file, PathBuf::from("test.wav"));
		assert!(*raw);
		assert_eq!(header, "sample", "the default header is the measured wire layout");
		assert!(cli.is_oneshot());
		assert_eq!(cli.camera_name(), Some("doorbell"));
		assert!(cli.dump_bcmedia_path().is_none());

		assert!(
			Cli::try_parse_from(["bairelay", "talk", "doorbell"]).is_err(),
			"talk without --file must be a usage error, not a silent no-op"
		);

		let cli = Cli::try_parse_from(["bairelay", "audio-bridge", "doorbell"]).unwrap();
		let Command::AudioBridge {
			common,
			idle,
			header,
			input,
			gain,
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "doorbell");
		assert_eq!(
			*idle, 2.0,
			"a short default gap is what limits an orphaned session"
		);
		assert_eq!(header, "sample");
		assert_eq!(
			input, "s16le/16000",
			"the bridge keeps the raw-PCM default; go2rtc has to ask for G.711 out loud"
		);
		assert_eq!(
			*gain, 1.0,
			"no silent rescaling by default: loudness is decided in go2rtc.yaml, not here"
		);
		let cloned = crate::cli_convert::clone_command(&cli.command);
		assert!(matches!(cloned, Command::AudioBridge { .. }));

		// The exact spelling `go2rtc.yaml` passes must reach a usable format,
		// otherwise the two halves of the intercom disagree in silence.
		let cli = Cli::try_parse_from([
			"bairelay",
			"audio-bridge",
			"doorbell",
			"--input",
			"pcmu/8000",
		])
		.unwrap();
		let Command::AudioBridge { input, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(input, "pcmu/8000");
		assert_eq!(
			crate::oneshot::input::Input::parse(input).unwrap(),
			crate::oneshot::input::Input::Pcmu8k
		);

		let cli =
			Cli::try_parse_from(["bairelay", "audio-bridge", "doorbell", "--idle", "0.5"])
				.unwrap();
		let Command::AudioBridge { idle, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(*idle, 0.5);
	}

	#[test]
	fn parse_pir_read_and_set() {
		let cli = Cli::try_parse_from(["bairelay", "pir", "driveway"]).unwrap();
		let Command::Pir { common, state } = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "driveway");
		assert!(state.is_none());

		let cli = Cli::try_parse_from(["bairelay", "pir", "driveway", "on"]).unwrap();
		let Command::Pir { state, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(matches!(state, Some(OnOff::On)));
	}

	#[test]
	fn parse_webhook_takes_camera_only() {
		let cli = Cli::try_parse_from(["bairelay", "webhook", "driveway"]).unwrap();
		let Command::Webhook(a) = &cli.command else {
			panic!("wrong variant")
		};
		assert_eq!(a.camera, "driveway");
		assert!(Cli::try_parse_from(["bairelay", "webhook", "driveway", "on"]).is_err());
	}

	#[test]
	fn parse_status_light_read_and_set() {
		let cli = Cli::try_parse_from(["bairelay", "status-light", "driveway"]).unwrap();
		assert!(matches!(cli.command, Command::StatusLight { .. }));

		let cli = Cli::try_parse_from(["bairelay", "status-light", "driveway", "off"]).unwrap();
		let Command::StatusLight { state, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(matches!(state, Some(OnOff::Off)));
	}

	#[test]
	fn parse_ptz_tree() {
		// Bare form defaults to listing presets (cmd is None → dispatch
		// layer rewrites to PtzCommand::Preset { preset_id: None }).
		let cli = Cli::try_parse_from(["bairelay", "ptz", "driveway"]).unwrap();
		let Command::Ptz { cmd, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(cmd.is_none());

		let cli = Cli::try_parse_from(["bairelay", "ptz", "driveway", "preset", "3"]).unwrap();
		let Command::Ptz { cmd, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(matches!(
			cmd,
			Some(PtzCommand::Preset { preset_id: Some(3) })
		));

		let cli =
			Cli::try_parse_from(["bairelay", "ptz", "driveway", "assign", "2", "home"]).unwrap();
		let Command::Ptz { cmd, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(PtzCommand::Assign { preset_id, name }) = cmd else {
			panic!("wrong ptz cmd")
		};
		assert_eq!(*preset_id, 2);
		assert_eq!(name, "home");

		let cli =
			Cli::try_parse_from(["bairelay", "ptz", "driveway", "control", "32", "left"]).unwrap();
		let Command::Ptz { cmd, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(PtzCommand::Control {
			amount, direction, ..
		}) = cmd
		else {
			panic!("wrong ptz cmd")
		};
		assert_eq!(*amount, 32);
		assert!(matches!(direction, PtzDirection::Left));

		let cli = Cli::try_parse_from(["bairelay", "ptz", "driveway", "zoom", "0.5"]).unwrap();
		let Command::Ptz { cmd, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(PtzCommand::Zoom { amount }) = cmd else {
			panic!("wrong ptz cmd")
		};
		assert!((*amount - 0.5).abs() < 1e-6);
	}

	#[test]
	fn parse_services_tree() {
		// Bare `services <cam>` parses to service = None AND action
		// = None (dispatch lists all six).
		let cli = Cli::try_parse_from(["bairelay", "services", "driveway"]).unwrap();
		let Command::Services {
			service, action, ..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert!(service.is_none());
		assert!(action.is_none());

		// `services <cam> <svc>` with no action → service = Some, action = None.
		let cli = Cli::try_parse_from(["bairelay", "services", "driveway", "http"]).unwrap();
		let Command::Services {
			service, action, ..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert!(matches!(service, Some(ServiceName::Http)));
		assert!(action.is_none());

		let cli = Cli::try_parse_from(["bairelay", "services", "driveway", "http", "get"]).unwrap();
		let Command::Services {
			service, action, ..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert!(matches!(service, Some(ServiceName::Http)));
		assert!(matches!(action, Some(ServiceAction::Get)));

		let cli = Cli::try_parse_from([
			"bairelay", "services", "driveway", "rtsp", "set", "554", "on",
		])
		.unwrap();
		let Command::Services { action, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(ServiceAction::Set { port, enabled }) = action else {
			panic!("wrong service action")
		};
		assert_eq!(*port, 554);
		assert!(matches!(enabled, OnOff::On));
	}

	#[test]
	fn parse_users_tree() {
		// Bare `users <cam>` parses to action = None (dispatch treats
		// it as `list`).
		let cli = Cli::try_parse_from(["bairelay", "users", "driveway"]).unwrap();
		let Command::Users { action, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(action.is_none());

		let cli = Cli::try_parse_from(["bairelay", "users", "driveway", "list"]).unwrap();
		let Command::Users { action, .. } = &cli.command else {
			panic!("wrong variant")
		};
		assert!(matches!(action, Some(UserAction::List)));

		// New positional shape: <name> <user_type> [<password>].
		// Operator-supplied positional password still works (legacy /
		// drop-in compat), but the role moves to the second slot so
		// the trailing password can be optional.
		let cli = Cli::try_parse_from([
			"bairelay", "users", "driveway", "add", "bob", "user", "p4ss",
		])
		.unwrap();
		let Command::Users { action, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(UserAction::Add {
			name,
			password,
			user_type,
		}) = action
		else {
			panic!("wrong user action")
		};
		assert_eq!(name, "bob");
		assert_eq!(password.as_deref(), Some("p4ss"));
		assert!(matches!(user_type, UserTypeArg::User));

		// Password omitted: parses to None and the prompt/stdin path
		// fires later in `clone_user_action`.
		let cli =
			Cli::try_parse_from(["bairelay", "users", "driveway", "add", "bob", "user"]).unwrap();
		let Command::Users { action, .. } = &cli.command else {
			panic!("wrong variant")
		};
		let Some(UserAction::Add { name, password, .. }) = action else {
			panic!("wrong user action")
		};
		assert_eq!(name, "bob");
		assert!(
			password.is_none(),
			"omitted password must parse to None; got {password:?}"
		);
	}

	/// `audio-settings` is the read/write face of `<audioCfg>`: the bare form
	/// must be a plain read, and every option must reach the one-shot, with the
	/// ability gate **off** unless `--gate` is passed (this doorbell's
	/// `abilityInfo` under-reports, so gating by default would refuse a command
	/// the camera answers).
	#[test]
	fn parse_audio_settings() {
		let cli = Cli::try_parse_from(["bairelay", "-c", "c.toml", "audio-settings", "Doorbell"])
			.unwrap();
		let Command::AudioSettings {
			common,
			volume,
			talk,
			visitor_volume,
			loudspeaker,
			gate,
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "Doorbell");
		assert_eq!(volume, &None);
		assert_eq!(talk, &None);
		assert_eq!(visitor_volume, &None);
		assert_eq!(loudspeaker, &None);
		assert!(!*gate, "the gate is opt-in");
		assert!(cli.is_oneshot());
		assert_eq!(cli.camera_name(), Some("Doorbell"));
		assert!(cli.dump_bcmedia_path().is_none());

		let cli = Cli::try_parse_from([
			"bairelay",
			"audio-settings",
			"Doorbell",
			"--volume",
			"80",
			"--talk",
			"90",
			"--visitor-volume",
			"60",
			"--loudspeaker",
			"0",
			"--gate",
		])
		.unwrap();
		let Command::AudioSettings {
			volume,
			talk,
			visitor_volume,
			loudspeaker,
			gate,
			..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(volume, &Some(80));
		assert_eq!(
			talk,
			&Some(90),
			"the intercom volume is the setting this command exists for"
		);
		assert_eq!(visitor_volume, &Some(60));
		assert_eq!(
			*loudspeaker,
			Some(0),
			"a switch set to 0 must not read as 'not passed'"
		);
		assert!(*gate);

		let cloned = crate::cli_convert::clone_command(&cli.command);
		let Command::AudioSettings { talk, .. } = &cloned else {
			panic!("clone_command must preserve the variant")
		};
		assert_eq!(talk, &Some(90));
	}

	/// `chime-settings` must parse as a bare read (every option `None`,
	/// no write intent) and as a full-option write.
	#[test]
	fn parse_chime_settings() {
		let cli = Cli::try_parse_from(["bairelay", "-c", "c.toml", "chime-settings", "Doorbell"])
			.unwrap();
		let Command::ChimeSettings {
			common,
			volume,
			led,
			melody,
			silent,
			ring,
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(common.camera, "Doorbell");
		assert_eq!(volume, &None);
		assert_eq!(led, &None);
		assert_eq!(melody, &None);
		assert_eq!(silent, &None);
		assert!(!*ring, "a command that makes noise must never be the default");
		assert!(cli.is_oneshot());
		assert_eq!(cli.camera_name(), Some("Doorbell"));
		assert!(cli.dump_bcmedia_path().is_none());

		let cli = Cli::try_parse_from([
			"bairelay",
			"chime-settings",
			"Doorbell",
			"--volume",
			"4",
			"--led",
			"0",
			"--melody",
			"2",
			"--silent",
			"0",
			"--ring",
		])
		.unwrap();
		let Command::ChimeSettings {
			volume,
			led,
			melody,
			silent,
			ring,
			..
		} = &cli.command
		else {
			panic!("wrong variant")
		};
		assert_eq!(volume, &Some(4));
		assert_eq!(
			*led,
			Some(0),
			"a switch set to 0 must not read as 'not passed'"
		);
		assert_eq!(melody, &Some(2));
		assert_eq!(
			*silent,
			Some(0),
			"silent 0 means unmute: a value, not an absence"
		);
		assert!(*ring);

		let cloned = crate::cli_convert::clone_command(&cli.command);
		let Command::ChimeSettings { melody, ring, .. } = &cloned else {
			panic!("clone_command must preserve the variant")
		};
		assert_eq!(melody, &Some(2));
		assert!(*ring);
	}

	#[test]
	fn help_template_lists_all_subcommands_grouped() {
		// The hand-rolled help_template must stay in sync with the
		// Command enum. If someone adds a new camera command without
		// adding its line here, the help output drifts silently; this
		// test pins both the group headings and every subcommand name.
		let mut buf = Vec::new();
		use clap::CommandFactory;
		Cli::command().write_help(&mut buf).unwrap();
		let help = String::from_utf8(buf).unwrap();

		assert!(help.contains("Service modes:"));
		assert!(help.contains("Camera commands:"));
		for cmd in [
			"mqtt",
			"rtsp",
			"mqtt-rtsp",
			"reboot",
			"snapshot",
			"battery",
			"floodlight",
			"pir",
			"status-light",
			"ptz",
			"presets",
			"services",
			"users",
			"set-time",
			"version",
			"siren",
			"abilities",
			"webhook",
			"audio-settings",
			"chime-settings",
			"talk-ability",
			"talk",
			"audio-bridge",
			"audio-check",
		] {
			assert!(
				help.contains(cmd),
				"help missing subcommand `{}`:\n{}",
				cmd,
				help
			);
		}
	}
}
