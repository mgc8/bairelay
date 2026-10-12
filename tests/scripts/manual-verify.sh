#!/usr/bin/env bash
# Manual-verification harness for .
#
# Starts bairelay under `mqtt-rtsp`, waits for the RTSP listener to come up,
# then runs each installed RTSP client against each configured camera for a
# short playback window. Also exercises the MQTT bridge in parallel to catch
# any regression introduced by the RTSP wiring.
#
# Outputs: one line per client/camera/transport with PASS / FAIL / SKIP,
# full logs under `./tests/logs/manual-verify/`.
#
# Usage:
#     tests/scripts/manual-verify.sh [--config config.toml] [--duration 15] [--rtsp-port 8554]
#     tests/scripts/manual-verify.sh [--go2rtc PATH] [--go2rtc-port 28554]
#     tests/scripts/manual-verify.sh [--tls] [--tls-port 8555]
#     tests/scripts/manual-verify.sh --help
#
# --tls flag:
#     Auto-runs tests/scripts/gen-test-certs.sh, drops a TLS-enabled wrapper
#     config (operator's $CONFIG + certificate / tls_bind_port appended), and
#     flips probe URLs to rtsps://$RTSP_HOST:$TLS_PORT/<cam>. Each client tool
#     gets the appropriate CA-trust flag pointed at tests/test-certs/ca.pem.
#     UDP probes are skipped under --tls (RTSP-over-TLS implies TCP-interleaved
#     RTP). Logs land in tests/logs/manual-verify-tls/ so plain and TLS runs do
#     not clobber each other. vlc is SKIPped under --tls because its CA-trust
#     knob for self-signed roots is not portable in a script. The
#     go2rtc relay stage is also SKIPped under --tls (go2rtc would need its own
#     rtsps client config; out of scope here).
#
# Environment:
#     RUST_LOG   defaults to "bairelay=debug,bairelay_rtsp=debug,info"
#     NO_BUILD   if set, skips the cargo build step
#     NO_MQTT    if set, skips the MQTT regression test
#     NO_GO2RTC  if set, skips the go2rtc relay stage
#     SKIP_BATTERY_SLEEP  if set, skip the 35 s "wait for camera to sleep" checkpoint
#
# Exit codes:
#     0  all tests that ran passed (skipped tests do not fail the run)
#     1  one or more tests failed
#     2  setup error (config missing, bairelay failed to start, etc.)

set -u
set -o pipefail

# ── Argument parsing ────────────────────────────────────────────────────

CONFIG="config.toml"
DURATION=15
RTSP_PORT=8554
TLS_PORT=8555
TLS=0
GO2RTC_BIN="tests/scripts/go2rtc/go2rtc"
GO2RTC_PORT=28554

while [ $# -gt 0 ]; do
	case "$1" in
		--config)        CONFIG="$2"; shift 2 ;;
		--duration)      DURATION="$2"; shift 2 ;;
		--rtsp-port)     RTSP_PORT="$2"; shift 2 ;;
		--tls)           TLS=1; shift 1 ;;
		--tls-port)      TLS_PORT="$2"; shift 2 ;;
		--go2rtc)        GO2RTC_BIN="$2"; shift 2 ;;
		--go2rtc-port)   GO2RTC_PORT="$2"; shift 2 ;;
		-h|--help)
			grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'
			exit 0 ;;
		*) echo "unknown arg: $1" >&2; exit 2 ;;
	esac
done

if [ ! -f "$CONFIG" ]; then
	echo "error: config file not found: $CONFIG" >&2
	exit 2
fi

# GNU coreutils `timeout` is required for the bounded probe runs. It is
# present on Linux out of the box; on macOS it ships as `gtimeout` from
# `brew install coreutils` (or as `timeout` if the gnubin path is on
# $PATH). Resolve once here so every call site uses the same binary.
if command -v timeout >/dev/null 2>&1; then
	TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
	TIMEOUT_BIN="gtimeout"
else
	echo "error: GNU coreutils 'timeout' not found." >&2
	echo "  Linux: install via your package manager (coreutils)." >&2
	echo "  macOS: brew install coreutils  (provides gtimeout)." >&2
	exit 2
fi

if [ "$TLS" -eq 1 ]; then
	OUT_DIR="tests/logs/manual-verify-tls"
	RTSP_SCHEME="rtsps"
	EFFECTIVE_PORT="$TLS_PORT"
else
	OUT_DIR="tests/logs/manual-verify"
	RTSP_SCHEME="rtsp"
	EFFECTIVE_PORT="$RTSP_PORT"
fi
mkdir -p "$OUT_DIR"
: > "$OUT_DIR/summary.txt"

RUST_LOG="${RUST_LOG:-bairelay=debug,bairelay_rtsp=debug,info}"
export RUST_LOG

# ── TLS setup ────────────────────────────────────────────────
#
# When --tls is set:
#   * Generate the test cert tree if missing (idempotent).
#   * Resolve absolute paths to ca.pem and server-bundle.pem so the wrapper
#     config and the client CA-verify flags are robust to the operator's CWD.
#   * The wrapper config itself is built later, after the original $CONFIG
#     has been parsed for bind addr / camera names — see "TLS wrapper
#     config" below.

CA_PEM=""
SERVER_BUNDLE=""
CONFIG_TO_USE="$CONFIG"
if [ "$TLS" -eq 1 ]; then
	echo "TLS mode: ensuring test certs are present..."
	if ! tests/scripts/gen-test-certs.sh > "$OUT_DIR/gen-test-certs.log" 2>&1; then
		echo "error: gen-test-certs.sh failed; see $OUT_DIR/gen-test-certs.log" >&2
		exit 2
	fi
	# Resolve to absolute paths — bairelay's config parser is happier with
	# them, and the client CA-trust flags must work regardless of where the
	# tools are invoked from.
	CA_PEM="$(cd "$(dirname tests/test-certs/ca.pem)" && pwd)/$(basename tests/test-certs/ca.pem)"
	SERVER_BUNDLE="$(cd "$(dirname tests/test-certs/server-bundle.pem)" && pwd)/$(basename tests/test-certs/server-bundle.pem)"
	if [ ! -f "$CA_PEM" ] || [ ! -f "$SERVER_BUNDLE" ]; then
		echo "error: expected cert files missing after gen-test-certs.sh" >&2
		exit 2
	fi
fi

# ── Discover config values (non-secret) ─────────────────────────────────

RTSP_BIND=$(awk '/^\[mqtt\]/ {in_m=1} /^\[/ && !/^\[mqtt\]/ {in_m=0} !in_m && /^bind *= *"/ { gsub(/^bind *= *"|"$/, ""); print; exit }' "$CONFIG")
if [ -z "$RTSP_BIND" ] || [ "$RTSP_BIND" = "0.0.0.0" ]; then
	RTSP_HOST="127.0.0.1"
else
	RTSP_HOST="$RTSP_BIND"
fi

CAMERAS=$(awk '/^\[\[cameras\]\]/ { in_c=1; next } /^\[/ && !/^\[\[cameras\]\]/ { in_c=0 } in_c && /^name *= *"/ { gsub(/^name *= *"|"$/, ""); print }' "$CONFIG")
if [ -z "$CAMERAS" ]; then
	echo "error: no cameras found in $CONFIG" >&2
	exit 2
fi

MQTT_BROKER=$(awk '/^\[mqtt\]/ {in_m=1; next} /^\[/ && !/^\[mqtt\]/ {in_m=0} in_m && /^broker_addr *= *"/ { gsub(/^broker_addr *= *"|"$/, ""); print; exit }' "$CONFIG")
# Credentials for MQTT broker. Parsed as a quoted 2-element TOML array:
#     credentials = ["user", "pass"]
# Kept in shell variables and passed via -u/-P; never echoed.
MQTT_CREDS_RAW=$(awk '/^\[mqtt\]/ {in_m=1; next} /^\[/ && !/^\[mqtt\]/ {in_m=0} in_m && /^credentials *=/' "$CONFIG")
MQTT_USER=""
MQTT_PASS=""
if [ -n "$MQTT_CREDS_RAW" ]; then
	# Extract the two quoted strings without shelling out twice.
	MQTT_USER=$(printf '%s' "$MQTT_CREDS_RAW" | awk -F '"' '{print $2}')
	MQTT_PASS=$(printf '%s' "$MQTT_CREDS_RAW" | awk -F '"' '{print $4}')
fi
# : mqtt.topic_prefix (default "bairelay"); matches
# src/config.rs's default_topic_prefix.
MQTT_PREFIX=$(awk '/^\[mqtt\]/ {in_m=1; next} /^\[/ && !/^\[mqtt\]/ {in_m=0} in_m && /^topic_prefix *= *"/ { gsub(/^topic_prefix *= *"|"$/, ""); print; exit }' "$CONFIG")
if [ -z "$MQTT_PREFIX" ]; then
	MQTT_PREFIX="bairelay"
fi

echo "config:   $CONFIG"
if [ "$TLS" -eq 1 ]; then
	echo "tls:      enabled (rtsps://$RTSP_HOST:$TLS_PORT, ca=$CA_PEM)"
else
	echo "rtsp:     rtsp://$RTSP_HOST:$RTSP_PORT"
fi
echo "cameras:  $(echo "$CAMERAS" | tr '\n' ' ')"
if [ -n "$MQTT_USER" ]; then
	echo "mqtt:     ${MQTT_BROKER:-<none>} (auth as user '$MQTT_USER')"
else
	echo "mqtt:     ${MQTT_BROKER:-<none>}"
fi
echo "duration: ${DURATION}s per client"
echo "log dir:  $OUT_DIR"
echo "---"

# ── TLS wrapper config ──────────────────────────────────────────────────
#
# `certificate` and `tls_bind_port` are TOP-LEVEL config keys. TOML scalars
# placed AFTER any `[section]` header parse as members of that section, so
# we have to insert the TLS lines BEFORE the first table-header line in the
# operator's config (typically `[mqtt]` or `[[cameras]]`). Bairelay treats
# `certificate` as the trigger to spawn the TLS listener in parallel with
# the plain one.

if [ "$TLS" -eq 1 ]; then
	CONFIG_TO_USE="$OUT_DIR/config-tls.toml"
	awk -v cert="$SERVER_BUNDLE" -v port="$TLS_PORT" '
		BEGIN { inserted = 0 }
		/^[[:space:]]*\[/ && !inserted {
			print "# Inserted by manual-verify.sh --tls"
			print "certificate = \"" cert "\""
			print "tls_bind_port = " port
			print ""
			inserted = 1
		}
		{ print }
		END {
			if (!inserted) {
				print ""
				print "# Inserted by manual-verify.sh --tls"
				print "certificate = \"" cert "\""
				print "tls_bind_port = " port
			}
		}
	' "$CONFIG" > "$CONFIG_TO_USE"
	echo "tls config wrapper written to $CONFIG_TO_USE"
fi

# ── Build ───────────────────────────────────────────────────────────────

if [ -z "${NO_BUILD:-}" ]; then
	echo "building release binary..."
	if ! cargo build --release --bin bairelay > "$OUT_DIR/build.log" 2>&1; then
		echo "error: cargo build failed; see $OUT_DIR/build.log" >&2
		exit 2
	fi
fi

BAIRELAY_BIN="target/release/bairelay"
if [ ! -x "$BAIRELAY_BIN" ]; then
	echo "error: $BAIRELAY_BIN not found or not executable" >&2
	exit 2
fi

# ── Client discovery ────────────────────────────────────────────────────

have() { command -v "$1" >/dev/null 2>&1; }

VLC_BIN=""
if have vlc; then
	VLC_BIN="vlc"
elif [ -x "/Applications/VLC.app/Contents/MacOS/VLC" ]; then
	VLC_BIN="/Applications/VLC.app/Contents/MacOS/VLC"
fi

# ── Test runner ─────────────────────────────────────────────────────────

PASS=0
FAIL=0
SKIP=0

record() {
	# $1 = status (PASS|FAIL|SKIP), $2 = label, $3 = detail (optional)
	printf '%-4s %s  %s\n' "$1" "$2" "${3:-}" | tee -a "$OUT_DIR/summary.txt"
	case "$1" in
		PASS) PASS=$((PASS+1)) ;;
		FAIL) FAIL=$((FAIL+1)) ;;
		SKIP) SKIP=$((SKIP+1)) ;;
	esac
}

# Run a single RTSP client against one URL. Writes client stderr/stdout to a log.
# $1 = client name (ffprobe|ffmpeg|vlc|mpv)
# $2 = test label (used in filenames)
# $3 = transport ("tcp" or "udp")
# $4 = rtsp URL
run_client() {
	local client="$1" label="$2" transport="$3" url="$4"
	# Include transport in the filename so consecutive runs over tcp and
	# udp for the same camera don't overwrite each other — the early
	# live-verify misread UDP DTS warnings as TCP warnings
	# because of the previous filename collision.
	local log="$OUT_DIR/${client}-${transport}-${label}.log"
	local t="$DURATION"
	# TLS flags for ffmpeg/ffprobe: none. The rtsps demuxer exposes no
	# tls_verify / ca_file options (ffmpeg 7.x errors "Option not found";
	# ffprobe ignores them), so these probes check handshake + stream
	# only. Cert-chain and client-auth verification live in
	# crates/rtsp/tests/tls_handshake.rs.
	local ff_tls_args=()
	case "$client" in
		ffprobe)
			# ffprobe -show_streams with a bounded timeout. TCP transport = reliable.
			"$TIMEOUT_BIN" --foreground -k 5s "$((t + 5))" ffprobe \
				-v info -hide_banner \
				-rtsp_transport "$transport" \
				"${ff_tls_args[@]}" \
				-i "$url" \
				-show_streams -show_format \
				> "$log" 2>&1
			local rc=$?
			if [ "$rc" -eq 0 ] && grep -q 'codec_type=video' "$log"; then
				record PASS "ffprobe/$transport/$label" "video stream detected"
			else
				record FAIL "ffprobe/$transport/$label" "rc=$rc, see $log"
			fi
			;;
		ffmpeg)
			"$TIMEOUT_BIN" --foreground -k 5s "$((t + 5))" ffmpeg \
				-hide_banner -loglevel info \
				-rtsp_transport "$transport" \
				"${ff_tls_args[@]}" \
				-i "$url" \
				-t "$t" -f null - \
				> "$log" 2>&1
			local rc=$?
			# ffmpeg exits 0 after -t duration; 124 if timeout(1) killed it
			# before ffmpeg finished. Either outcome is acceptable provided
			# ffmpeg managed to pull >0 frames (i.e., the server and the
			# stream actually worked).
			local frames
			frames=$(grep -oE 'frame= *[0-9]+' "$log" | tail -1 | grep -oE '[0-9]+')
			if { [ "$rc" -eq 0 ] || [ "$rc" -eq 124 ]; } && [ -n "$frames" ] && [ "$frames" -gt 0 ]; then
				record PASS "ffmpeg/$transport/$label" "frames=$frames rc=$rc"
			else
				record FAIL "ffmpeg/$transport/$label" "rc=$rc frames=${frames:-0}, see $log"
			fi
			;;
		vlc)
			if [ "$TLS" -eq 1 ]; then
				# VLC's rtsps:// support has no portable CA-trust knob for a
				# self-signed root, so we SKIP under --tls instead of pretending.
				record SKIP "vlc/$transport/$label" "vlc has no portable rtsps:// CA-trust knob"
				return
			fi
			if [ -z "$VLC_BIN" ]; then
				record SKIP "vlc/$transport/$label" "vlc not installed"
				return
			fi
			local rtsp_caching=500
			# VLC's exit code on --play-and-exit is 0 on success.
			"$TIMEOUT_BIN" --foreground -k 5s "$((t + 10))" "$VLC_BIN" \
				--intf dummy --no-video-title-show \
				--rtsp-caching="$rtsp_caching" \
				--run-time="$t" \
				--play-and-exit \
				"$url" \
				> "$log" 2>&1
			local rc=$?
			# VLC sometimes returns non-zero on clean shutdown via signal; accept timeout's 124 if the log shows any media was played.
			if [ "$rc" -eq 0 ] || ( [ "$rc" -eq 124 ] && grep -qi 'demux\|live555\|rtsp' "$log" ); then
				record PASS "vlc/$transport/$label" "rc=$rc"
			else
				record FAIL "vlc/$transport/$label" "rc=$rc, see $log"
			fi
			;;
		mpv)
			# mpv's TLS knobs cover rtsps:// the same way they cover https://.
			local mpv_tls_args=()
			if [ "$TLS" -eq 1 ]; then
				mpv_tls_args=(--tls-verify=yes --tls-ca-file="$CA_PEM")
			fi
			"$TIMEOUT_BIN" --foreground -k 5s "$((t + 5))" mpv \
				--no-config --msg-level=all=info \
				--vo=null --ao=null \
				--length="$t" \
				--rtsp-transport="$transport" \
				"${mpv_tls_args[@]}" \
				"$url" \
				> "$log" 2>&1
			local rc=$?
			if [ "$rc" -eq 0 ] || [ "$rc" -eq 124 ]; then
				# mpv with --vo=null --ao=null doesn't print AV progress lines,
				# but it DOES print a VO: line when the decoder locks onto the
				# stream. That's the reliable marker of a working connection.
				if grep -qE '^VO: |Video --vid|AV:|V: ' "$log"; then
					record PASS "mpv/$transport/$label" "rc=$rc"
				else
					record FAIL "mpv/$transport/$label" "rc=$rc, no VO line, see $log"
				fi
			else
				record FAIL "mpv/$transport/$label" "rc=$rc, see $log"
			fi
			;;
		*)
			record SKIP "$client/$transport/$label" "unknown client"
			;;
	esac
}

# ── Launch bairelay ─────────────────────────────────────────────────────

SERVER_LOG="$OUT_DIR/bairelay.log"

# Refuse to start if something already listens on the probe port (e.g. a
# deployed bairelay service): the readiness poll below would see that
# listener and every probe would silently test the other instance.
if nc -z -w 1 "$RTSP_HOST" "$EFFECTIVE_PORT" 2>/dev/null; then
	echo "error: $RTSP_HOST:$EFFECTIVE_PORT is already in use by another process" >&2
	echo "       (a deployed bairelay service?) — stop it and re-run" >&2
	exit 2
fi

echo "starting bairelay..."
"$BAIRELAY_BIN" mqtt-rtsp -c "$CONFIG_TO_USE" > "$SERVER_LOG" 2>&1 &
BAIRELAY_PID=$!

cleanup() {
	if kill -0 "$BAIRELAY_PID" 2>/dev/null; then
		echo "stopping bairelay (pid $BAIRELAY_PID)..."
		kill -INT "$BAIRELAY_PID" 2>/dev/null || true
		# Give it up to 10 s to shut down cleanly.
		for _ in $(seq 1 20); do
			sleep 0.5
			if ! kill -0 "$BAIRELAY_PID" 2>/dev/null; then
				break
			fi
		done
		if kill -0 "$BAIRELAY_PID" 2>/dev/null; then
			echo "bairelay did not exit in 10 s, sending KILL"
			kill -KILL "$BAIRELAY_PID" 2>/dev/null || true
		fi
	fi
}
trap cleanup EXIT INT TERM

# Wait for the RTSP listener to be ready — poll up to 30 s. Under --tls the
# probe targets the TLS listener on $TLS_PORT so we know the cert load + bind
# completed; the plain listener can come up first and we still want to wait.
echo "waiting for RTSP listener on $RTSP_HOST:$EFFECTIVE_PORT..."
READY=0
for _ in $(seq 1 60); do
	# Liveness first: a port answering means nothing if our process died.
	if ! kill -0 "$BAIRELAY_PID" 2>/dev/null; then
		echo "error: bairelay exited during startup, see $SERVER_LOG" >&2
		tail -20 "$SERVER_LOG" >&2
		exit 2
	fi
	if grep -q 'RTSP server listening\|RTSP server started' "$SERVER_LOG" 2>/dev/null; then
		READY=1
		break
	fi
	# Also try a TCP connect to be extra sure.
	if nc -z -w 1 "$RTSP_HOST" "$EFFECTIVE_PORT" 2>/dev/null; then
		READY=1
		break
	fi
	sleep 0.5
done
if [ "$READY" -ne 1 ]; then
	echo "error: RTSP listener did not come up in 30 s" >&2
	tail -30 "$SERVER_LOG" >&2
	exit 2
fi
echo "RTSP listener ready."

# Wait for `startup_wake` to finish warming all cameras. While it is still
# running it holds StreamSources that would otherwise be torn down out
# from under a client connecting at the same time. Poll the bairelay log
# for the completion marker with a generous deadline.
echo "waiting for startup wake cycle to finish..."
for _ in $(seq 1 120); do
	if grep -q 'Startup wake cycle complete' "$SERVER_LOG" 2>/dev/null; then
		break
	fi
	sleep 0.5
done
if grep -q 'Startup wake cycle complete' "$SERVER_LOG" 2>/dev/null; then
	echo "startup wake complete."
else
	echo "(startup wake did not complete in 60 s; proceeding anyway)"
fi
echo ""

# ── Run tests ───────────────────────────────────────────────────────────
#
# bairelay's startup-wake cycle (awaited above) leaves every battery
# camera awake. Each RTSP probe acquires its own wake lock for the
# duration of the connection, so consecutive probes keep the camera
# awake without any explicit MQTT wakeup. After the last probe, the
# camera's grace timer fires (30 s default) and it sleeps.

for CAM in $CAMERAS; do
	URL="$RTSP_SCHEME://$RTSP_HOST:$EFFECTIVE_PORT/$CAM"
	echo "=== camera: $CAM ==="
	# No explicit MQTT wakeup: bairelay's startup-wake cycle (awaited
	# above) warmed every camera, and each probe's RTSP connection
	# holds its own wake lock for the duration of the probe.

	# Per-client TCP probes. Under --tls we run only the clients that
	# accept a self-signed CA via a portable command-line flag — vlc
	# doesn't, so it is excluded from the matrix entirely (not even
	# listed as SKIP) instead of cluttering the output.
	run_client ffprobe "$CAM" tcp "$URL"
	run_client ffmpeg  "$CAM" tcp "$URL"
	run_client mpv     "$CAM" tcp "$URL"
	if [ "$TLS" -eq 0 ]; then
		run_client vlc "$CAM" tcp "$URL"
	fi

	# UDP transport probes (ffmpeg only — others follow the same RTP path).
	# Excluded entirely under --tls: RTSP-over-TLS implies TCP-interleaved
	# RTP, so UDP probes are not part of the TLS matrix.
	if [ "$TLS" -eq 0 ]; then
		run_client ffmpeg  "$CAM" udp "$URL"
		run_client ffprobe "$CAM" udp "$URL"
	fi

	echo ""
done

# ── Multi-client fanout test (camera 1) ─────────────────────────────────

FIRST_CAM=$(echo "$CAMERAS" | head -1)
FANOUT_URL="$RTSP_SCHEME://$RTSP_HOST:$EFFECTIVE_PORT/$FIRST_CAM"
echo "=== multi-client fanout: 2× ffmpeg on $FIRST_CAM ==="
# No TLS flags: see the ff_tls_args note in run_client.
FANOUT_TLS_ARGS=()
# Run the fanout block in the parent shell (NOT a `(...)` subshell):
# `record` increments PASS / FAIL counters, and a subshell-local
# increment is invisible to the line-`totals:` print at the end of the
# script — every fanout FAIL would be silently dropped from the totals.
# Local variables here (FAN_A, FAN_B, RC_A, RC_B) are reused below
# only by reference and don't need scoping.
#
# Pass criterion mirrors the single-client `ffmpeg/$transport` test
# (line ~300): `rc ∈ {0, 124}` AND parsed `frame=N` > 0. The 124 case
# is the harness's outer `timeout` hard-kill — expected when ffmpeg
# is still streaming when the duration window closes — and is only
# spuriously a FAIL signal when `-eq 0` is checked literally. What
# fanout actually validates is "both clients pulled frames in
# parallel"; that's the assertion the criterion below pins.
# `-loglevel info` (not `error`) is required for ffmpeg to print the
# `frame= ...` progress lines we parse below — the same loglevel the
# single-client `ffmpeg/$transport` test uses.
"$TIMEOUT_BIN" --foreground -k 5s $((DURATION + 5)) ffmpeg \
	-hide_banner -loglevel info -rtsp_transport tcp \
	"${FANOUT_TLS_ARGS[@]}" -i "$FANOUT_URL" \
	-t "$DURATION" -f null - > "$OUT_DIR/fanout-a.log" 2>&1 &
FAN_A=$!
sleep 0.5
"$TIMEOUT_BIN" --foreground -k 5s $((DURATION + 5)) ffmpeg \
	-hide_banner -loglevel info -rtsp_transport tcp \
	"${FANOUT_TLS_ARGS[@]}" -i "$FANOUT_URL" \
	-t "$DURATION" -f null - > "$OUT_DIR/fanout-b.log" 2>&1 &
FAN_B=$!
wait "$FAN_A"; RC_A=$?
wait "$FAN_B"; RC_B=$?
FRAMES_A=$(grep -oE 'frame= *[0-9]+' "$OUT_DIR/fanout-a.log" | tail -1 | grep -oE '[0-9]+')
FRAMES_B=$(grep -oE 'frame= *[0-9]+' "$OUT_DIR/fanout-b.log" | tail -1 | grep -oE '[0-9]+')
if { [ "$RC_A" -eq 0 ] || [ "$RC_A" -eq 124 ]; } \
	&& { [ "$RC_B" -eq 0 ] || [ "$RC_B" -eq 124 ]; } \
	&& [ -n "$FRAMES_A" ] && [ "$FRAMES_A" -gt 0 ] \
	&& [ -n "$FRAMES_B" ] && [ "$FRAMES_B" -gt 0 ]; then
	record PASS "fanout/tcp/$FIRST_CAM" \
		"a=frames=$FRAMES_A rc=$RC_A b=frames=$FRAMES_B rc=$RC_B"
else
	record FAIL "fanout/tcp/$FIRST_CAM" \
		"a=frames=${FRAMES_A:-0} rc=$RC_A b=frames=${FRAMES_B:-0} rc=$RC_B"
fi
echo ""

# ── MQTT regression: LED toggle while the next stream runs ──────────────

if [ -z "${NO_MQTT:-}" ] && [ -n "$MQTT_BROKER" ] && have mosquitto_pub && have mosquitto_sub; then
	echo "=== MQTT regression: query/battery round-trip on $FIRST_CAM ==="
	# Uses query/battery rather than a control topic because every Reolink
	# camera answers battery queries — control/led is refused on Argus
	# battery cameras ("Missing ability: ledState"). This check's purpose
	# is to prove the MQTT bridge still round-trips while RTSP is running,
	# not to assert LED capability.
	MQTT_AUTH=()
	if [ -n "$MQTT_USER" ]; then
		MQTT_AUTH=(-u "$MQTT_USER" -P "$MQTT_PASS")
	fi

	# Subscribe to status/battery (the camera's response) before publishing.
	ACK_LOG="$OUT_DIR/mqtt-ack.log"
	"$TIMEOUT_BIN" --foreground -k 5s 15 mosquitto_sub -h "$MQTT_BROKER" \
		"${MQTT_AUTH[@]}" \
		-t "$MQTT_PREFIX/$FIRST_CAM/status/battery" \
		> "$ACK_LOG" 2>&1 &
	SUB_PID=$!
	sleep 0.5

	# Keep the camera awake during the round-trip with a brief ffprobe.
	# Reuse the same TLS flags as the fanout stage when --tls is set.
	"$TIMEOUT_BIN" --foreground -k 5s 12 ffprobe -v error -rtsp_transport tcp \
		"${FANOUT_TLS_ARGS[@]}" -i "$FANOUT_URL" -show_streams \
		> "$OUT_DIR/mqtt-keepawake.log" 2>&1 &
	KEEP_PID=$!
	sleep 1.5

	# Publish the query. Empty payload — the command is identified by the
	# topic alone.
	mosquitto_pub -h "$MQTT_BROKER" \
		"${MQTT_AUTH[@]}" \
		-t "$MQTT_PREFIX/$FIRST_CAM/query/battery" -m "" \
		> "$OUT_DIR/mqtt-pub.log" 2>&1
	PUB_RC=$?

	# Wait up to 10 s for the camera's response to appear.
	for _ in $(seq 1 20); do
		if [ -s "$ACK_LOG" ]; then
			break
		fi
		sleep 0.5
	done
	kill "$SUB_PID" 2>/dev/null || true
	wait "$KEEP_PID" 2>/dev/null || true

	# Response is an XML blob with battery fields. Any non-empty content
	# means the MQTT bridge completed the round-trip.
	if [ "$PUB_RC" -eq 0 ] && [ -s "$ACK_LOG" ]; then
		BYTES=$(wc -c < "$ACK_LOG" | tr -d ' ')
		record PASS "mqtt/query-battery/$FIRST_CAM" "response=${BYTES}B"
	else
		record FAIL "mqtt/query-battery/$FIRST_CAM" "pub_rc=$PUB_RC bytes=$(wc -c < "$ACK_LOG" 2>/dev/null || echo 0)"
	fi
	echo ""
fi

# ── go2rtc relay stage ──────────────────────────────────────────────────
#
# Start go2rtc with a generated config that ingests from bairelay over TCP
# interleaved, then reserve-serves on $GO2RTC_PORT. Probe through go2rtc with
# ffprobe; any PASS proves the server is compatible with go2rtc's RTSP client
# (used in practice by many Home Assistant / Scrypted / Frigate deployments).

if [ "$TLS" -eq 1 ]; then
	# go2rtc relay stage is excluded entirely from the --tls matrix:
	# go2rtc would need its own rtsps:// client config + CA trust, which
	# is not portable to script. Not even listed as SKIP.
	:
elif [ -z "${NO_GO2RTC:-}" ] && [ -x "$GO2RTC_BIN" ]; then
	echo "=== go2rtc relay test ==="
	GO2RTC_DIR="$OUT_DIR/go2rtc"
	mkdir -p "$GO2RTC_DIR"

	# Write a minimal config. Disable HTTP/webrtc/hass listeners so we don't
	# fight with anything on the machine; bind RTSP on $GO2RTC_PORT only.
	{
		echo "log:"
		echo "  level: info"
		echo ""
		echo "api:"
		echo "  listen: ''"
		echo ""
		echo "webrtc:"
		echo "  listen: ''"
		echo ""
		echo "hass:"
		echo "  config: ''"
		echo ""
		echo "rtsp:"
		echo "  listen: 127.0.0.1:$GO2RTC_PORT"
		echo ""
		echo "streams:"
		for CAM in $CAMERAS; do
			echo "  ${CAM}: rtsp://$RTSP_HOST:$RTSP_PORT/$CAM"
		done
	} > "$GO2RTC_DIR/go2rtc.yaml"

	GO2RTC_LOG="$GO2RTC_DIR/go2rtc.log"
	"$GO2RTC_BIN" -c "$GO2RTC_DIR/go2rtc.yaml" > "$GO2RTC_LOG" 2>&1 &
	GO2RTC_PID=$!

	# Ensure we always clean up go2rtc, even on early exit.
	stop_go2rtc() {
		if [ -n "${GO2RTC_PID:-}" ] && kill -0 "$GO2RTC_PID" 2>/dev/null; then
			kill -INT "$GO2RTC_PID" 2>/dev/null || true
			for _ in $(seq 1 10); do
				sleep 0.3
				if ! kill -0 "$GO2RTC_PID" 2>/dev/null; then return; fi
			done
			kill -KILL "$GO2RTC_PID" 2>/dev/null || true
		fi
	}
	# Chain stop_go2rtc into the existing cleanup.
	trap 'stop_go2rtc; cleanup' EXIT INT TERM

	READY=0
	for _ in $(seq 1 40); do
		if nc -z -w 1 127.0.0.1 "$GO2RTC_PORT" 2>/dev/null; then
			READY=1
			break
		fi
		if ! kill -0 "$GO2RTC_PID" 2>/dev/null; then
			record FAIL "go2rtc/startup" "exited during startup, see $GO2RTC_LOG"
			READY=-1
			break
		fi
		sleep 0.25
	done

	if [ "$READY" -eq 1 ]; then
		# Probe each camera through go2rtc. 10 s each to keep it tight.
		for CAM in $CAMERAS; do
			RELAY_URL="rtsp://127.0.0.1:$GO2RTC_PORT/$CAM"
			LOG="$GO2RTC_DIR/ffprobe-${CAM}.log"
			"$TIMEOUT_BIN" --foreground -k 5s 20 ffprobe \
				-v info -hide_banner \
				-rtsp_transport tcp \
				-i "$RELAY_URL" \
				-show_streams -show_format \
				> "$LOG" 2>&1
			rc=$?
			if [ "$rc" -eq 0 ] && grep -q 'codec_type=video' "$LOG"; then
				record PASS "go2rtc-relay/$CAM" "video seen via go2rtc"
			else
				record FAIL "go2rtc-relay/$CAM" "rc=$rc, see $LOG (go2rtc log: $GO2RTC_LOG)"
			fi
		done
	elif [ "$READY" -eq 0 ]; then
		record FAIL "go2rtc/startup" "listener on :$GO2RTC_PORT not ready in 10 s"
	fi

	stop_go2rtc
	trap cleanup EXIT INT TERM
	echo ""
elif [ -z "${NO_GO2RTC:-}" ]; then
	record SKIP "go2rtc-relay" "binary not found at $GO2RTC_BIN"
fi

# ── Battery sleep observation ───────────────────────────────────────────

if [ -z "${SKIP_BATTERY_SLEEP:-}" ]; then
	# All probes have finished — last wake lock dropped here. The camera
	# disconnects after grace (30 s default) + watchdog jitter (sweep
	# every 30 s).
	OBSERVE_SECS=60
	echo "=== battery sleep observation: waiting ${OBSERVE_SECS} s (30 s grace + 30 s watchdog slack) ==="
	BEFORE=$(wc -l < "$SERVER_LOG")
	sleep "$OBSERVE_SECS"
	# Look for shutdown markers in the server log for the battery cameras.
	TAIL=$(tail -n "+$BEFORE" "$SERVER_LOG")
	SLEEP_HITS=$(printf '%s\n' "$TAIL" | grep -cE 'Grace period expired|Disconnected|Disconnecting' || true)
	if [ "$SLEEP_HITS" -gt 0 ]; then
		record PASS "battery-sleep/grace-period" "$SLEEP_HITS disconnect events in ${OBSERVE_SECS} s"
	else
		record FAIL "battery-sleep/grace-period" "no disconnect events observed; see $SERVER_LOG"
	fi
	echo ""
fi

# ── Summary ─────────────────────────────────────────────────────────────

echo "======================================================================"
echo "summary"
echo "======================================================================"
cat "$OUT_DIR/summary.txt"
echo "---"
printf "totals: %d passed, %d failed, %d skipped\n" "$PASS" "$FAIL" "$SKIP"
echo "logs: $OUT_DIR/"

if [ "$FAIL" -gt 0 ]; then
	exit 1
fi
exit 0
