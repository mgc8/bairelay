# Contributing to Bairelay

Thanks for your interest. This file is the orientation page — it covers how to get a local build going, what gates a change has to clear, and where to read next. The deeper docs in `docs/` are the source of truth for everything past that.

## TL;DR

```bash
git clone https://github.com/mgc8/bairelay
cd bairelay
cargo build
cargo test
```

That's it for the read-only contributor experience. No GStreamer, no OpenSSL, no system MQTT library — `cargo build` produces a single static binary.

## Prerequisites

- A recent stable Rust toolchain (1.74+). Install via [rustup](https://rustup.rs/).
- A C toolchain for `aws-lc-rs` (the rustls crypto provider):
  - **Linux**: `build-essential` (Debian / Ubuntu) or `base-devel` (Arch) or the equivalent in your distro.
  - **macOS**: Xcode Command Line Tools (`xcode-select --install`).
- `git`.

Linux and macOS are first-class platforms; both are exercised in CI on every push. Windows is build-only — no CI, no live-verify, at least at the moment; contributions welcome.

## Workspace shape

```
bairelay/
├── src/                # binary: CLI, config, orchestrator, lifecycle
├── crates/
│   ├── core/           # bairelay_neolink_core: Baichuan protocol (vendored)
│   ├── rtsp/           # bairelay_rtsp: pure-Rust RTSP server + RTP packetisers
│   ├── mqtt/           # bairelay_mqtt: MQTT bridge + HA discovery
│   └── wake-server/    # bairelay_wake_server: local BcUdp wake server
├── fuzz/               # cargo-fuzz harness (excluded from the workspace)
├── docs/               # specification, architecture, build history, etc.
└── tests/              # integration tests, fixtures, scripts
```

`docs/architecture.md` has the full crate-responsibility breakdown and the runtime patterns (per-camera task tree, wake lock, watchdog, per-kind RTSP dispatch).

## What has to pass before a PR merges

Three commands. CI runs them on Linux + macOS for every push:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

Coverage is also gated. `tarpaulin.toml` pins `fail-under = 87` against the full workspace; the current baseline is ~90%. New code is expected to keep that headroom:

```bash
cargo tarpaulin
```

Pure-logic modules aim for ≥95%; command modules / pollers ≥90%; I/O-adjacent modules ≥85% via test seams. See `docs/implementation.md` § Coverage policy for documented exceptions.

## Coding standards

- **Tabs, not spaces** — enforced by `rustfmt.toml`'s `hard_tabs = true`.
- **DRY** — three similar lines is the threshold to refactor; two is fine.
- **No half-finished implementations** — don't add error handling, fallbacks, or feature flags for scenarios that can't happen. Trust internal code; only validate at system boundaries.
- **Default to no comments** — well-named identifiers say *what*; only add a comment when the *why* is non-obvious (a hidden constraint, a workaround, a subtle invariant). Don't reference the current task or PR.
- **Portability**: Linux + macOS are primary; any OS-specific code goes in a separate file/module.
- **Definition of done**: code review + lint clean + tests + relevant docs updated. No exceptions.

## Reproducible build expectations

This project aims to be buildable reproducibly.

Release builds should not depend on:

- the current time, except through `SOURCE_DATE_EPOCH`;
- the current username, hostname, home directory, or absolute checkout path;
- the presence of a `.git` directory;
- network access during the build;
- nondeterministic filesystem ordering.

For binary releases, build with:

```bash
cargo build --release --locked
```

Before releases, check with `reprotest` where available.

## Commit style

Subject ≤72 chars, followed by a blank line, then a number of bullet points detailing the changes, **each one line ≤72 chars** — never wrap a bullet onto a second line. The diff documents *what*; the body documents *why* and any non-obvious *how*.

Good:

```
session_task: split video and audio dispatch loops

- single broadcast::Receiver let 4K HEVC IDRs starve audio writes
- video_dispatch_loop owns the original receiver; audio resubscribes
- TCP-interleaved write mutex still holds for one $-framed packet
- audio bursting fix needs the pacer change too (see #...)
```

## Test rigs

Three layers, in order of cost:

1. **Unit + integration tests** — `cargo test`. Hermetic, >2000 tests, runs in seconds.
2. **Fixture replay** (`tests/fixture_replay.rs`) — replays captured `BcMedia` packets through the real `RtspServer`. Drop `.bcmedia` files into `tests/fixtures/` to exercise; without fixtures the test is a no-op pass. See `docs/testing.md` § BcMedia fixture pipeline.
3. **Live-verify on real camera hardware** — `tests/scripts/manual-verify.sh` (RTSP/MQTT compat matrix) and `tests/scripts/ha-verify.sh` (Home Assistant ingestion). On-demand only; not wired into `cargo test`.

Live-verify is the load-bearing one for any change touching the RTSP path, MQTT bridge, or wake-server protocol. **If you don't have hardware, say so in the PR**; a maintainer will run the rig before merge.

`docs/testing.md` has the full setup walkthrough for both rigs, on Linux and macOS.

## Fuzzing

`fuzz/` is a detached cargo project (libfuzzer-sys, nightly-only). Its targets (listed in `fuzz/README.md`) cover the parsers + state machines facing untrusted bytes. Day-to-day:

```bash
scripts/fuzz.sh                    # all targets, 10s each
scripts/fuzz.sh aac_parse_adts     # one target only
FUZZ_TIME=600 scripts/fuzz.sh      # longer window
```

Setup (rustup nightly + `cargo install cargo-fuzz`) and direct `cargo fuzz` invocations are in `fuzz/README.md`. New parsers / state machines should land with a fuzz target; new bug fixes against fuzz crashes should land with the reproducer pinned as a regression test.

## Where to read next

In rough order of "I want to understand the system":

1. **`README.md`** — what bairelay is, who it's for, how to run it.
2. **`docs/architecture.md`** — workspace structure, crate responsibilities, runtime patterns (the *how-it's-shaped*).
3. **`docs/implementation.md`** — API gotchas, design decisions, test infrastructure (the *what-bites-you*).
4. **`docs/testing.md`** — fixture pipeline, HA rig, manual-verify, live-verify discipline.
5. **`docs/cloud-interception.md`** — wire-level protocol reference for the wake server + push listener.

## Scope

Bairelay's exclusive design target is **Reolink battery cameras** (e.g. Argus). Always-on Reolinks work incidentally; doorbells / NVRs / non-Reolink hardware are explicitly out of scope. Changes that broaden scope past battery cameras are unlikely to be accepted.

## Reporting issues

Use the GitHub issue tracker. The templates under `.github/ISSUE_TEMPLATE/` cover bug reports and feature requests. For protocol-level bugs, a pcap capture of the camera ↔ bairelay traffic is worth a thousand words — but **scrub camera UIDs, IPs, and credentials** before sharing.

## License

By contributing, you agree your contribution is licensed under AGPL-3.0-or-later, the same as the rest of the project. See `LICENSE`.
