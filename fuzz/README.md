# bairelay-fuzz

`cargo-fuzz` harness for the parsers facing untrusted bytes.

## Why a separate crate

`fuzz/` is excluded from the parent workspace (`exclude = ["fuzz"]` in
the root `Cargo.toml`). It has its own `[workspace]` table and pulls
`libfuzzer-sys`, which only builds on nightly. `cargo test` never
compiles this crate.

## Targets

| Binary | Drives | Surface |
|---|---|---|
| `wake_server_decode_discovery` | `bairelay_wake_server::packet::decode_discovery` | Wake-server UDP entry on ports 9999 + 58200, LAN-exposed. |
| `bcudp_deserialize` | `neolink_core::bcudp::BcUdp::deserialize` | Every camera/relay UDP frame (Discovery, Ack, Data). |
| `bc_deserialize` | `neolink_core::fuzz_api::parse_bc` (wraps `Bc::deserialize`) | Every TCP Baichuan modern message. |
| `bcxml_try_parse` | `neolink_core::fuzz_api::parse_bc_xml` (wraps `BcXml::try_parse`) | Largest serde struct surface in the project. |
| `aac_parse_adts` | `bairelay_rtsp::codec::aac::parse_adts` | ADTS audio frame header; RTP timestamp slope depends on it. |
| `nal_split_decode` | `bairelay_rtsp::codec::nal::{split_annex_b, is_decodable_nal}` | Per-NAL hot path. |
| `udp_flow_state` | `neolink_core::fuzz_api::flow_state_drive_arbitrary` (drives `UdpFlowState`) | BcUdp reorder / ack state machine; bounded-state invariants. |
| `adpcm_decode_block` | `bairelay_rtsp::transcode::adpcm::AdpcmDecoder::decode_block` | Camera ADPCM audio blocks. |
| `dns_parse_a_response` | `bairelay::dns::parse_a_response` | Upstream resolver replies for push forwarding. |

The `neolink_core` targets reach `pub(crate)` parsers via the
`fuzz_api` module that the `fuzz-api` Cargo feature exposes. Production
builds never compile that module.

## Setup

You need **rustup** (Homebrew's `rust` package is stable-only and can't
build this harness). One-off:

```
brew install rustup-init && rustup-init    # if you don't have rustup yet
rustup toolchain install nightly
cargo install cargo-fuzz
```

`fuzz/rust-toolchain.toml` pins nightly for this directory; rustup
walks up from the cwd to find it, so commands must run with cwd at
`fuzz/` or below. From the repo root the pin is invisible.

## Running

The wrapper at `scripts/fuzz.sh` (run from the repo root) is the
day-to-day driver:

```
scripts/fuzz.sh                       # all targets, 10s each
scripts/fuzz.sh aac_parse_adts        # one target only
FUZZ_TIME=600 scripts/fuzz.sh         # ten-minute window per target
```

Verbose libfuzzer output goes to `fuzz/logs/<target>.log`; stdout
shows one line per target:

```
aac_parse_adts                   OK    (29459512 execs, 2678137/s)
bc_deserialize                   OK    (605172 execs, 55015/s)
bcudp_deserialize                CRASH — see fuzz/logs/bcudp_deserialize.log
    thread '<unnamed>' panicked at crates/core/src/bcudp/de.rs:172:5: ...
    Test unit written to ./artifacts/bcudp_deserialize/crash-1a2b3c
```

Exit code is non-zero iff any target crashed. Per-target exec rate
varies 50× across the set: pure-byte parsers (`aac_parse_adts`,
`nal_split_decode`) hit millions of execs/second; allocating parsers
(`bcxml_try_parse`) sit at tens of thousands. Budget per-target
`FUZZ_TIME` accordingly when chasing real bugs (10 s only verifies the
harness wires up).

For raw access (corpus minimisation, coverage reports, formatting a
crash input):

```
cd fuzz
cargo fuzz run <target> [-- libfuzzer-flags]
cargo fuzz tmin <target> artifacts/<target>/<crash>
cargo fuzz fmt <target>  artifacts/<target>/<crash>
cargo fuzz coverage <target>
```

## Interpreting hits

| Signal | Meaning | Action |
|---|---|---|
| Panic | Logic bug in the parser. | Reproduce, fix the underlying check, add a regression test in the parser's host crate. |
| Hang / timeout | Quadratic or worse scaling on adversarial input. | Cap input length or add early-exit; don't just bump `-timeout`. |
| ASAN / UBSAN | Memory error. **Treat as security incident.** | Same as panic, but escalate. |
| OOM | Unbounded allocation. | Cap parsed sizes (BcUdp already caps `payload_size` at 64 KiB; check whether the failing parser does the same). |

Hits land in `fuzz/artifacts/<target>/`; reload one with
`cargo fuzz run <target> artifacts/<target>/<file>` to confirm the
crash deterministically before opening a fix. `fuzz/corpus/<target>/`
and `fuzz/logs/` are gitignored.

## Adding a new target

1. New `fuzz_targets/<name>.rs` with a `fuzz_target!(|data: &[u8]| { … })` body.
2. New `[[bin]]` block in `Cargo.toml`.
3. If the target needs an otherwise-private parser, expose it through
   `neolink_core::fuzz_api` (or whichever crate hosts it). Keep the
   shim trivial — fuzz harnesses must not rebuild context the
   production code path doesn't already build.
