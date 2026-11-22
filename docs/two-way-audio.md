# Two-way audio (the live talk path)

Everything on this branch that touches the doorbell's loudspeaker while
someone is talking: the session, the microphone capture, the encoder, the
bridge to Home Assistant, the capability probe, and the offline encoder
check. Camera *settings* that happen to be audio-shaped (volume, visitor
loudspeaker, chime family) are not here — see “Known limitations”.

The talk path is a **client** feature: the Baichuan protocol has no audio
file upload anywhere. The talk session (msg id 202, closed by msg id 11)
is the only audio upload surface, so live voice has to be streamed into
it. There is accordingly **no MQTT service and no RTSP audio ingest**;
the live path is CLI/exec only (go2rtc `exec:` URL, see below).

## The commands

| Command | What it does | Needs a camera? |
|---|---|---|
| `bairelay talk <camera> --file <PATH>` | one-shot: send one 16 kHz mono PCM/WAV file out of the loudspeaker | yes |
| `bairelay audio-bridge <camera>` | live bridge: PCM (or G.711) on stdin → paced talk session, one session per speech burst | yes |
| `bairelay talk-ability <camera>` | ungated `<TalkAbility>` probe (msg id 10) — reads the negotiation table, sends no audio | yes |
| `bairelay audio-check` | offline encoder round trip against this project's own decoder; `--compare` both header conventions, `--bench <N>` the 64 ms block timer | no (dispatched before the config file is read) |

`talk` flags: `--raw` (headerless s16le, skips the WAV refusal),
`--header sample|repeat` (block header convention, see below).
`audio-bridge` flags: `--idle <SECONDS>` (default 2.0 — silence that ends
the session), `--header sample|repeat`, `--input <FORMAT>` (default
`s16le/16000`; accepts the go2rtc spellings `pcmu/8000`, `pcma/8000`, …),
`--gain <FACTOR>` (default 1).
All flag and format errors are **usage errors before the camera is
woken** (exit 2) — a battery doorbell must not pay a wake window for a
typo.

Exit codes follow the usual table: `0` for any completed run (including
EOF before a single byte on the bridge: `0 talk session(s)`, exit 0),
`2` for usage errors, `6` for `MissingAbility` when the gate refuses,
`130` after a graceful Ctrl+C on the bridge.

## Where each piece lives

| Concern | Where |
|---------|-------|
| Wire framing, gate checks, `talk` / `talk_stream` / `talk_stop`, ungated probe | `crates/core/src/bc_protocol/talk.rs` |
| Probe result (`TalkAbilityProbe`), payload description | `crates/core/src/bc_protocol/talk.rs` |
| List-shaped `<TalkAbility>` XML (doorbell answers with lists) | `crates/core/src/bc/xml.rs` (`TalkAbility`, `DuplexList`, `AudioStreamModeList`, `AudioConfigList`) |
| Ability gate override (`declare_ability`, `ability_assertions`) | `crates/core/src/bc_protocol.rs` |
| ADPCM decode **and** encode (DVI 4-bit, block geometry from `lengthPerEncoder`) | `crates/rtsp/src/transcode/adpcm.rs` |
| Input validation (RIFF walk, format refusal) | `src/oneshot/pcm.rs` |
| G.711 expand + integer-ratio resampling of stdin | `src/oneshot/input.rs` |
| Negotiation (msg 10 → `TalkConfig`, block geometry), shared by both senders | `src/oneshot/talk.rs::negotiate` |
| One-shot send, wall-clock pacing | `src/oneshot/talk.rs` |
| Live bridge: per-burst sessions, stdin thread, queue cap, heartbeat log | `src/oneshot/audio_bridge.rs` |
| Per-camera flag | `src/config.rs::CameraConfig::two_way_audio` |
| Op deadline for a paced send / a long-running bridge | `src/oneshot/runner.rs::{run_with_op_timeout, run_graceful}` |
| Concrete-`BcCamera` intercept (no `talk*` surface on `CameraDriver`) | `src/run_support.rs` |
| Offline encoder check and the `--header` convention parser | `src/audio_control.rs` |

## Why the gate needs an override, and how far the override reaches

`talk`, `talk_stream` and `talk_stop` call `has_ability_rw("two_way_audio")`.
Measured on the battery doorbell of this fork: `<TalkAbility>` (msg 10)
answers `200` with `version 1.1`, `duplex FDX`,
`audioStreamMode followVideoStream` + `mixAudioStream` and one
`<audioConfig>` — `priority 0, adpcm, 16000 Hz, 16 bits,
lengthPerEncoder 1024, mono` — while `abilityInfo` lists 56 abilities and
**none** of them is `two_way_audio` (nor `Talk`, nor `audioTalk`). The
camera can speak; its inventory lies. That is a firmware
under-declaration, and `bairelay talk-ability` is the read-only probe
that demonstrates it without sending audio.

`BcCamera::declare_ability(name)` adds the name to a **separate** set,
`ability_assertions`, consulted first by `has_ability`. Deliberately *not*
a write into the `abilities` map: `ability_advertised()` reads the raw
map, so with `two_way_audio = true` in force
`bairelay abilities <camera>` still prints the camera's own inventory —
measured on this doorbell: the same `abilities (56):` table, still no
`two_way_audio` row — even though the gate is open. An override that also
satisfied the diagnostic would hide the mismatch an operator needs to
see, and a future `set_ability` writer could then silently disagree with
itself.

`two_way_audio = true` in `[[cameras]]` is the only thing that calls
`declare_ability`, the default is `false`, and no other caller of the
gate changes behaviour. **Config:**

| Key | Scope | Default | Meaning |
|---|---|---|---|
| `two_way_audio` | `[[camera]]` | `false` | declare the talk ability past the `abilityInfo` gate for **this camera only** (firmware under-declaration, verified with `talk-ability`). Not part of the HA add-on's per-camera option set — config file only. |

No other config key is needed by the live path; everything else is a
command-line flag.

## Block geometry, and what is measured versus inferred

`talk.rs` computes `block_size = length_per_encoder / 2` and
`full_block_size = block_size + 4`, so one block is 512 payload bytes
plus a 4-byte header (predictor `i16` LE, step index `u8`, one reserved
byte), i.e. **516 bytes = 1024 nibble samples = 64 ms at 16 kHz**, and
1025 real samples per block once the header sample is counted (so one
block consumes exactly 2050 input bytes).
`talk()` packs `BLOCK_PER_PAYLOAD = 4` blocks into one msg 202 payload
and paces by sleeping out each payload's playback duration;
`talk_stream()` sends one block per payload, paced on the camera's ack.
The header sample is emitted by the decoder as the block's *first*
sample, so a sender that ignores it duplicates one sample per block
(0.1 %); `talk()`'s own sample arithmetic counts that sample, which is
consistent with the camera doing the same. The `--header` flag chooses
the convention: `sample` (default — the header carries this block's
first sample) or `repeat` (the header replays the previous sample, the
layout older senders used). `audio-check --compare` prints both side by
side, offline.

Verified on hardware: the camera answers msg 10 with
`lengthPerEncoder = 1024`, `duplex FDX`, and a 2 s clip goes out as 32
blocks in ~2.5 s of wall time on both senders. **And: this firmware
never answers a msg 202.** The first bridge run wrote all 32 blocks and
then hung: `talk_stream()` had pushed every queued block and was sitting
on `sub.recv()` for the ack of the last payload, so the flush and
`talk_stop()` living *after* its loop were never reached. `talk()` never
had that problem because it paces by the clock. `talk_stream()` now
waits for the ack only *up to the playback time already scheduled for
what has been sent* and paces by the clock when the window closes — an
acking camera stays in charge of the pace, a silent one can still be
paced; receive errors still propagate. After the change the same run
closes its own session and the wall time drops from 5.0 s to 2.5 s.

Still inferred, not measured: that `lengthPerEncoder` counts nibble
samples rather than bytes (inferred from the decoder, which the encoder
mirrors nibble-for-nibble), and whether the duplicated header sample is
audible. Whether any of it was *heard* is only answerable at the
loudspeaker.

## Why an encoder had to be written

`crates/rtsp/src/transcode/adpcm.rs` only ever *decoded* — the RTSP side
needs camera → listener audio, and nothing upstream produced camera-bound
ADPCM. `AdpcmEncoder` keeps a decoder as its **reconstruction
reference**: each nibble is chosen by simulating what the decoder would
produce for the candidate step, so drift cannot accumulate between
encoder and decoder. Two details that only show up in the waveform:

- the block header's predictor must be the *last reconstructed sample of
  the previous block*, not the first sample of this one;
- the padding nibble at the end of a stream cannot be `0x00`: a zero
  nibble *adds* `step >> 3`, so a run of zeros slides the reconstruction
  upward and clicks. The encoder pads with the cancelling pair
  (`-step/8` then `+step/8`), which keeps the output at the last value
  and lets the step index decay to silence.

`audio-check` runs the whole file through encoder → decoder and reports
RMS/max error, seam behaviour per convention, and the timer bench,
without a camera or a network.

## The live bridge (`audio-bridge`)

**Sessions are per speech burst.** Opening a talk session costs one TCP
wake window; a session left open across silence is an orphan the camera
holds open on its own timer, and while it holds it the doorbell is deaf
to its own microphone and the talk slot is taken. The bridge therefore
opens a session at the first byte of a burst and closes it (msg 11)
after `--idle` seconds of silence (default 2 s), at EOF, or on Ctrl+C.
Measured: two bursts on one stdin produce `2 talk session(s)`; a new
process started right after a clean close is accepted immediately.

**Stdin is a blocking OS thread** feeding an unbounded channel; the
async pump drains it, converts (G.711 expand / resample —
`src/oneshot/input.rs`) and applies `--gain`, and encodes into whole
ADPCM blocks carrying the encoder state across chunk boundaries (a split
sample survives; a 4096-byte read is 128 ms of audio). If the camera
stops draining, more than `MAX_QUEUED_SECONDS = 10` seconds of audio
queued means the burst is closed rather than replayed minutes later —
an intercom is live or it is useless. At session open the backlog made
while the camera woke (opening a session takes ~3 s) is trimmed to the
last `PRE_ROLL = 0.3` s, on a sample boundary: nobody is listening to
the words spoken before the loudspeaker could answer.

**Heartbeat.** Once per second the bridge logs blocks produced, blocks
still queued at the camera and bytes read. A talk session that stops
says nothing and closes nothing — it falls silent — and without this
line “audio cuts out after ten seconds” is indistinguishable from “the
microphone stopped delivering”.

**Op deadline.** The one-shot `talk` gets
`max(60 s, audio + 40 s)`; the bridge runs under a 7-day budget because
go2rtc may keep one process across a whole day of calls.

## Home Assistant / go2rtc integration

go2rtc (HA add-on) writes *whatever it received from the browser* to the
backchannel handler — byte for byte, no transcoding (`pkg/pcm/
backchannel.go` does `wr.Write(packet.Payload)`). A browser negotiates
what it offered in the SDP: Opus 48 kHz or G.711 8 kHz. So declare the
backchannel with what will actually arrive and tell the bridge the same
thing:

```yaml
streams:
  front_door_push: "exec:bairelay audio-bridge front_door --input pcmu/8000#backchannel=1#audio=pcmu/8000"
```

(With `--input s16le/16000` — the default — pipe real 16 kHz s16le from
something that transcoded first, e.g. `ffmpeg -f alaw -ar 8000 -i -
-ar 16000 -f s16le`. Raw 16 kHz PCM is not a codec a browser ever
sends.) A rate mismatch is refused before the camera is contacted, with
a message naming both sides. The camera's microphone comes back on the
RTSP stream go2rtc already pulls — no second path needed.

The Home Assistant add-on's per-camera option set does not carry
`two_way_audio` yet; set it in the config file the add-on mounts.

## Battery doorbell behaviour

The doorbell sleeps; every talk run pays one wake window. The code
protects it in three ways: argument and file validation happen **before
the connection** (a bad WAV is a usage error, exit 1, and the camera
never wakes); the op deadline scales with the file (`talk`) or is a
standing 7-day budget (bridge); and the bridge releases sessions on
silence so an orphaned session never makes the doorbell deaf while it
should be sleeping. `talk` against a sleeping camera with a valid config
behaves like every other one-shot command: the runner's wake-then-
connect loop owns the timing.

## Known limitations

- **Audio settings are on the other branch.** Reading or writing the
  camera's `<audioCfg>` block (general volume, intercom volume, visitor
  loudspeaker) and the chime family belongs to `feature/settings-parameters`
  and its `audio-settings` / `chime-settings` commands. The live path
  neither reads nor writes them: `talk` and `audio-bridge` play at
  whatever volume the camera is set to.
- No MQTT service, no RTSP audio ingest, no `bairelay talk` alias for a
  live stream: the protocol has no upload surface beyond msg 202.
- This firmware never acks msg 202; pacing is by the clock on it. Other
  firmware may ack and stay in charge of the pace (`talk_stream`
  handles both, see above).
- Opus input is deliberately refused (`--input opus/48000` is a usage
  error): decoding Opus needs a codec library this branch refuses to
  carry. Let the browser fall back to G.711 (offer two m-lines) or
  transcode before the pipe.
- The duplicated header sample under `--header sample` is unmeasured by
  ear; `audio-check --compare` quantifies it offline instead.
- `two_way_audio` is not an HA add-on option (config file only).

## Command names

One spelling, in English, like everything else the project publishes:

```
bairelay -c /config/bairelay/bridge.toml audio-bridge --input pcmu/8000 --gain 3 Doorbell
```

`audio-bridge` and `--input` are the only spellings the parser accepts for the
subcommand and for the stdin format. The French names an unrename build of this
program answered to — `pont-audio`, `--entree` — are not kept as aliases.
A second spelling printed by `--help` is a second spelling that two people in
one installation have to reconcile, and a published branch should not carry a
compatibility hinge for a build nobody has. Typing one of them now produces
clap's own error, which names the subcommand or the argument it did not
recognise, and for the argument it points at `--input`.

`snapshot --output`'s `visible_alias = "file-path"` is the one remaining visible
alias in the CLI. It is not a translation but a neolink-compatibility spelling
of the same word, and it stays until someone with an installation that uses it
says otherwise.
