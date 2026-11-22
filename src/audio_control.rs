//! `bairelay audio-check` — a **camera-free, network-free** round trip
//! through the talk path: encode a file with the same [`AdpcmEncoder`] `talk`
//! and `audio-bridge` use, decode the result with the same [`AdpcmDecoder`] the
//! RTSP side uses for the camera's own audio, and report what came back.
//!
//! This exists because the one thing nobody can measure from this machine is
//! what the loudspeaker sounds like. Everything that can be measured without
//! it is measured here:
//!
//! - does the encoder lose or invent samples, and does the count close over a
//!   whole melody;
//! - is the error at a block seam bigger than the error anywhere else, and do
//!   the error jumps all land on the same remainder (that is what a periodic
//!   click looks like in numbers);
//! - does the header sample of each block come back bit-exact, or is it a
//!   replay of the sample before it (which is what `--header repeat` does);
//! - how much jitter this OS puts into a 64 ms nap, relative versus absolute
//!   (`--bench`), which is how a sender decides whether to pace by naps or by
//!   deadlines.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bairelay_rtsp::transcode::adpcm::{AdpcmDecoder, AdpcmEncoder, HeaderConvention};

use crate::oneshot::errors::UsageError;
use crate::oneshot::pcm::read_pcm_file;

/// Wire header every ADPCM block carries (predictor LE i16, step index, spare).
const BLOCK_HEADER_BYTES: usize = 4;

/// One measured statistic set, for one header convention.
#[derive(Debug)]
pub struct Report {
	pub convention: HeaderConvention,
	pub length_per_encoder: u16,
	/// Input samples handed to the encoder.
	pub in_samples: usize,
	pub blocks: usize,
	pub wire_bytes: usize,
	/// Samples the decoder emitted, replays and padding included: what the
	/// loudspeaker would actually play.
	pub out_samples: usize,
	/// Samples compared against the input one-for-one.
	pub aligned_samples: usize,
	/// `out_samples - in_samples`: padded tail plus any seam replay.
	pub surplus: usize,
	/// Samples the framing carries per block (1025 by default, 1024 under
	/// `repeat`), i.e. the period a seam artifact would repeat at.
	pub block_samples: usize,
	pub rmse: f64,
	pub mean_abs: f64,
	pub max_abs: i64,
	pub max_at: usize,
	/// Root-mean-square error taken at seam positions only.
	pub seam_rmse: f64,
	/// ...and taken everywhere else, for comparison.
	pub body_rmse: f64,
	pub seam_count: usize,
	/// Seams whose decoded sample equals the input exactly (only possible when
	/// the convention spends the header on a real sample).
	pub seam_exact: usize,
	/// Seams whose header sample equals the last sample the previous block
	/// emitted: a replay, i.e. a held waveform sample, i.e. a click.
	pub seam_replays: usize,
	/// Samples **held** in the stream the loudspeaker gets, landing exactly on a
	/// block boundary (see [`count_holds`]): a value played twice where the
	/// waveform wanted to move. This is the artifact the ear hears, counted on
	/// the played stream rather than as an error against the input, and it can
	/// only be invented by the block framing.
	pub seam_holds: usize,
	/// Holds that land anywhere else: a real recording has those by itself (top
	/// of a peak, a flat stretch), so they are context, not a verdict.
	pub other_holds: usize,
	/// The same test at seam positions on the input file: the floor, which for a
	/// tone is zero because a moving waveform never repeats a value exactly.
	pub input_holds: usize,
	/// Mean and max of the slope a hold swallows, in 16-bit counts — the
	/// amplitude of the click.
	pub hold_slope_mean: f64,
	pub hold_slope_max: i64,
	/// The last few samples the loudspeaker gets, pad included: what the stream
	/// leaves the speaker sitting at when the transmission stops. A tail parked
	/// away from zero is a click the moment the stream ends.
	pub tail: Vec<i16>,
	/// The largest errors: (sample index, index modulo the block length, |err|).
	pub worst: Vec<(usize, usize, i64)>,
	/// Largest sample-to-sample *jumps* in the error: (index, index modulo block
	/// length, |jump|). Periodicity shows up here as a modulo column that is
	/// always the same number.
	pub jumps: Vec<(usize, usize, i64)>,
}

impl Report {
	/// `seam_rmse / body_rmse`: a seam that is hurting the listener is markedly
	/// worse than the body of a block.
	pub fn seam_ratio(&self) -> f64 {
		if self.body_rmse > 0.0 {
			self.seam_rmse / self.body_rmse
		} else {
			0.0
		}
	}
}

/// Encode `samples` the way the talk uplink does, then decode the wire back and
/// measure the difference.
pub fn round_trip(
	samples: &[i16],
	length_per_encoder: u16,
	convention: HeaderConvention,
) -> Result<(Vec<u8>, Report)> {
	let data_bytes = usize::from(length_per_encoder) / 2;
	if data_bytes < 1 {
		return Err(UsageError::new(format!(
			"--block-length must be at least 2 (it is divided by two to get the data bytes per \
			 block); got {length_per_encoder}"
		))
		.into());
	}
	let full_block = data_bytes + BLOCK_HEADER_BYTES;

	let mut encoder = AdpcmEncoder::with_convention(data_bytes, convention);
	let block_samples = encoder.samples_in_block();
	let mut wire: Vec<u8> = Vec::with_capacity(samples.len() + samples.len() / 2 + full_block);
	encoder.encode(samples, &mut wire);
	encoder.finish(&mut wire);

	let blocks = wire.len() / full_block;
	anyhow::ensure!(
		wire.len() % full_block == 0,
		"encoder produced {} bytes, not a whole number of {full_block}-byte blocks",
		wire.len()
	);

	// Decode the way the camera does: one block at a time, each block reseeding
	// the decoder from its own 4-byte header.
	//
	// `out` is everything the decoder emits. `aligned` is what gets compared to
	// the input: under the default convention every emitted sample is one the
	// encoder put there (the header carries a real sample), so nothing is
	// dropped; under `repeat` the header is the previous reconstruction,
	// which the decoder replays, so that sample is dropped from every block —
	// including block 0, whose header is the encoder's initial zero state and
	// was never an input sample either. With that drop, `aligned` is our own
	// samples in order and the seams sit exactly on multiples of `block_samples`.
	let mut decoder = AdpcmDecoder::new();
	let mut out: Vec<i16> = Vec::with_capacity(samples.len() + 2 * full_block);
	let mut aligned: Vec<i16> = Vec::with_capacity(samples.len() + 2 * full_block);
	let skip = match convention {
		HeaderConvention::SampleInHeader => 0,
		HeaderConvention::RepeatPrevious => 1,
	};
	let mut seam_replays = 0usize;
	let mut previous_block_last: Option<i16> = None;
	for (b, chunk) in wire.chunks(full_block).enumerate() {
		let decoded = decoder
			.decode_block(chunk)
			.with_context(|| format!("the project's own decoder rejected block {b}"))?;
		if b > 0 {
			if let Some(last) = previous_block_last {
				if decoded[0] == last {
					seam_replays += 1;
				}
			}
		}
		previous_block_last = decoded.last().copied();
		out.extend_from_slice(&decoded);
		aligned.extend_from_slice(&decoded[skip.min(decoded.len())..]);
	}

	let n = samples.len().min(aligned.len());

	// The artifact a listener actually hears is not an error figure: it is the
	// loudspeaker being *held* on one value for a sample while the waveform
	// asked to move. Count that directly on `out`, the stream the camera gets,
	// and only where the signal was moving — a repeated zero inside a silence
	// is nothing. `input_holds` runs the same test on the file itself, so the
	// reader can see the floor a clean stream sits at.
	let period = if blocks > 0 { out.len() / blocks } else { 0 };
	let (seam_holds, other_holds, hold_slope_mean, hold_slope_max) =
		count_holds(&out, n, period);
	let (input_holds, _, _, _) = count_holds(samples, samples.len(), period);

	let mut sum_sq = 0.0f64;
	let mut sum_abs = 0i64;
	let mut max_abs = 0i64;
	let mut max_at = 0usize;
	let mut seam_sq = 0.0f64;
	let mut seam_n = 0usize;
	let mut body_sq = 0.0f64;
	let mut body_n = 0usize;
	let mut seam_exact = 0usize;
	let mut errs: Vec<i64> = Vec::with_capacity(n);

	for i in 0..n {
		let err = i64::from(aligned[i]) - i64::from(samples[i]);
		let abs = err.abs();
		errs.push(abs);
		sum_sq += (err as f64) * (err as f64);
		sum_abs += abs;
		if abs > max_abs {
			max_abs = abs;
			max_at = i;
		}
		if block_samples > 1 && i > 0 && i % block_samples == 0 {
			seam_sq += (err as f64) * (err as f64);
			seam_n += 1;
			if aligned[i] == samples[i] {
				seam_exact += 1;
			}
		} else {
			body_sq += (err as f64) * (err as f64);
			body_n += 1;
		}
	}

	let mut worst: Vec<(usize, usize, i64)> = (0..n)
		.map(|i| (i, i % block_samples.max(1), errs[i]))
		.collect();
	worst.sort_by_key(|e| std::cmp::Reverse(e.2));
	worst.truncate(8);

	let mut jumps: Vec<(usize, usize, i64)> = (1..n)
		.map(|i| (i, i % block_samples.max(1), (errs[i] - errs[i - 1]).abs()))
		.collect();
	jumps.sort_by_key(|e| std::cmp::Reverse(e.2));
	jumps.truncate(8);

	let report = Report {
		convention,
		length_per_encoder,
		in_samples: samples.len(),
		blocks,
		wire_bytes: wire.len(),
		out_samples: out.len(),
		aligned_samples: aligned.len(),
		surplus: out.len().saturating_sub(samples.len()),
		block_samples,
		rmse: if n > 0 { (sum_sq / n as f64).sqrt() } else { 0.0 },
		mean_abs: if n > 0 {
			sum_abs as f64 / n as f64
		} else {
			0.0
		},
		max_abs,
		max_at,
		seam_rmse: if seam_n > 0 {
			(seam_sq / seam_n as f64).sqrt()
		} else {
			0.0
		},
		body_rmse: if body_n > 0 {
			(body_sq / body_n as f64).sqrt()
		} else {
			0.0
		},
		seam_count: seam_n,
		seam_exact,
		seam_replays,
		seam_holds,
		other_holds,
		input_holds,
		hold_slope_mean,
		hold_slope_max,
		tail: out.iter().rev().take(8).rev().copied().collect(),
		worst,
		jumps,
	};
	Ok((wire, report))
}

/// Count samples that were **held** in a stream: one value played twice where
/// the waveform asked to move, which is what a click is made of.
///
/// Returns `(at_seam, elsewhere, mean lost slope, max lost slope)`, where
/// `at_seam` counts holds landing on a block boundary (`i % period == 0`) — a
/// seam artifact, the thing a block convention can invent — and `elsewhere`
/// counts the rest, which a real recording produces by itself (a sample at the
/// top of a peak, a zero crossing in a silent gap). Splitting them matters: the
/// block boundaries sit every 1025 samples whatever the convention, because the
/// decoder always emits header-plus-1024.
///
/// The lost slope is what the sample would have moved,
/// `(stream[i + 1] - stream[i - 1]) / 2`, in 16-bit counts, so it reads as the
/// amplitude of the click. A 16-count dead band (about −46 dBFS) skips the flat
/// spots at a peak, where holding a sample is inaudible and would only inflate
/// the count.
fn count_holds(stream: &[i16], upto: usize, period: usize) -> (usize, usize, f64, i64) {
	const MOVING: i64 = 16;
	let end = upto.min(stream.len().saturating_sub(2));
	let mut at_seam = 0usize;
	let mut elsewhere = 0usize;
	let mut sum = 0i64;
	let mut max = 0i64;
	for i in 2..end {
		if stream[i] != stream[i - 1] {
			continue;
		}
		let before = i64::from(stream[i - 1]) - i64::from(stream[i - 2]);
		let after = i64::from(stream[i + 1]) - i64::from(stream[i]);
		if before.abs() < MOVING && after.abs() < MOVING {
			continue;
		}
		if period > 1 && i % period == 0 {
			at_seam += 1;
		} else {
			elsewhere += 1;
		}
		let lost = ((i64::from(stream[i + 1]) - i64::from(stream[i - 1])) / 2).abs();
		sum += lost;
		max = max.max(lost);
	}
	let mean = if at_seam + elsewhere > 0 {
		sum as f64 / (at_seam + elsewhere) as f64
	} else {
		0.0
	};
	(at_seam, elsewhere, mean, max)
}

/// Measured behaviour of this OS's timers at the talk block cadence.
#[derive(Debug)]
pub struct Jitter {
	pub step_ms: u64,
	pub steps: usize,
	pub expected_ms: f64,
	pub relative_total_ms: f64,
	pub absolute_total_ms: f64,
	pub relative_mean_overshoot_ms: f64,
	pub relative_max_overshoot_ms: f64,
	pub absolute_mean_late_ms: f64,
	pub absolute_max_late_ms: f64,
	pub relative_late_steps: usize,
	pub absolute_late_steps: usize,
	/// Total of the third strategy — the schedule `talk()`/`talk_stream()` now
	/// run, with a re-anchor allowed only when more than one payload behind —
	/// **excluding** the [`STALL_MS`] producer stall injected mid-run.
	pub scheduled_total_ms: f64,
	/// How many times that schedule had to re-anchor (1 is the expected answer:
	/// the injected stall, and nothing else).
	pub scheduled_reanchors: usize,
	/// Longest run of payloads sent without waiting: the burst the camera's
	/// jitter buffer has to swallow after a hiccup.
	pub scheduled_burst_max: usize,
	/// Steps that finished more than 1 ms after the ideal schedule.
	pub scheduled_late_steps: usize,
}

/// A producer stall injected into the third pacing strategy, to see how the
/// schedule reacts to something the OS clock cannot explain: the microphone (or
/// the encoder, or the network) stops delivering for a moment.
const STALL_MS: u64 = 300;

/// Nap `steps` times `step_ms` two different ways: the way `talk()` does it (a
/// fresh relative sleep per payload, so every overshoot is inherited by the
/// next payload) and the way a deadline pacer does it (`t0 + n * step`, which
/// cannot accumulate). No network, no camera: this measures the OS.
pub async fn measure_jitter(step_ms: u64, steps: usize) -> Jitter {
	let step = Duration::from_millis(step_ms);
	let expected_ms = step_ms as f64 * steps as f64;

	let start = Instant::now();
	let mut previous = start;
	let mut overshoot: Vec<f64> = Vec::with_capacity(steps);
	for _ in 0..steps {
		tokio::time::sleep(step).await;
		let now = Instant::now();
		overshoot.push((now - previous).as_secs_f64() * 1000.0 - step_ms as f64);
		previous = now;
	}
	let relative_total_ms = start.elapsed().as_secs_f64() * 1000.0;
	let relative_max = overshoot
		.iter()
		.fold(0.0f64, |acc, v| if *v > acc { *v } else { acc });
	let relative_mean = mean(&overshoot);
	let relative_late = overshoot.iter().filter(|v| **v > 1.0).count();

	// Absolute schedule: one origin, every deadline computed from it.
	let base = tokio::time::Instant::now();
	let mut late: Vec<f64> = Vec::with_capacity(steps);
	let mut absolute_max = 0.0f64;
	for n in 1..=steps as u32 {
		let deadline = base + step * n;
		tokio::time::sleep_until(deadline).await;
		let now = tokio::time::Instant::now();
		let late_ms = now.saturating_duration_since(deadline).as_secs_f64() * 1000.0;
		late.push(late_ms);
		if late_ms > absolute_max {
			absolute_max = late_ms;
		}
	}
	let absolute_total_ms = tokio::time::Instant::now()
		.saturating_duration_since(base)
		.as_secs_f64()
		* 1000.0;

	// Third strategy: the schedule the senders actually run now. Same absolute
	// deadlines as above, but a re-anchor is allowed when the producer is more
	// than one payload behind — which is what a real stall needs, since audio
	// that arrived late cannot be played on time any more. The question this
	// answers is how much of the stream that sacrifice costs: one payload, or
	// the rest of the file.
	let stall = Duration::from_millis(STALL_MS);
	let stall_at = steps / 2;
	let sched_base = tokio::time::Instant::now();
	let mut deadline = sched_base;
	let mut reanchors = 0usize;
	let mut burst = 0usize;
	let mut burst_max = 0usize;
	let mut sched_late: Vec<f64> = Vec::with_capacity(steps);
	for n in 0..steps {
		if n == stall_at {
			// The microphone stopped delivering for a moment.
			tokio::time::sleep(stall).await;
		}
		let now = tokio::time::Instant::now();
		if now.saturating_duration_since(deadline) > step {
			deadline = now + step;
			reanchors += 1;
		} else {
			deadline += step;
		}
		let ideal = sched_base + step * (n as u32 + 1);
		if deadline > now {
			tokio::time::sleep_until(deadline).await;
			burst = 0;
		} else {
			// Sent without waiting: the camera gets this one back-to-back.
			burst += 1;
			burst_max = burst_max.max(burst);
		}
		sched_late.push(
			tokio::time::Instant::now()
				.saturating_duration_since(ideal)
				.as_secs_f64()
				* 1000.0,
		);
	}
	let scheduled_total_ms = tokio::time::Instant::now()
		.saturating_duration_since(sched_base)
		.as_secs_f64()
		* 1000.0
		- STALL_MS as f64;

	Jitter {
		step_ms,
		steps,
		expected_ms,
		relative_total_ms,
		absolute_total_ms,
		relative_mean_overshoot_ms: relative_mean,
		relative_max_overshoot_ms: relative_max,
		absolute_mean_late_ms: mean(&late),
		absolute_max_late_ms: absolute_max,
		relative_late_steps: relative_late,
		absolute_late_steps: late.iter().filter(|v| **v > 1.0).count(),
		scheduled_total_ms,
		scheduled_reanchors: reanchors,
		scheduled_burst_max: burst_max,
		scheduled_late_steps: sched_late.iter().filter(|v| **v > 1.0).count(),
	}
}

fn mean(values: &[f64]) -> f64 {
	if values.is_empty() {
		0.0
	} else {
		values.iter().sum::<f64>() / values.len() as f64
	}
}

/// Parse the `--header` value.
pub fn parse_convention(value: &str) -> Result<HeaderConvention> {
	match value.trim().to_ascii_lowercase().as_str() {
		"sample" => Ok(HeaderConvention::SampleInHeader),
		"repeat" => Ok(HeaderConvention::RepeatPrevious),
		other => Err(UsageError::new(format!(
			"--header must be `sample` (the block header carries that block's first sample, so \
			 nothing is heard twice) or `repeat` (the header replays the previous sample, one \
			 duplicated sample per block); got `{other}`"
		))
		.into()),
	}
}

pub fn convention_label(convention: HeaderConvention) -> &'static str {
	match convention {
		HeaderConvention::SampleInHeader => "sample",
		HeaderConvention::RepeatPrevious => "repeat",
	}
}

/// Read the file, run the round trip for one or both conventions, print it.
pub fn run(
	path: &Path,
	raw: bool,
	length_per_encoder: u16,
	convention: Option<HeaderConvention>,
	both: bool,
) -> Result<()> {
	let input = read_pcm_file(path, raw)?;
	println!(
		"audio-check: {} ({}) — {} samples, {:.3} s of audio",
		path.display(),
		input.origin.label(),
		input.samples.len(),
		input.samples.len() as f64 / 16000.0
	);
	if input.dropped_bytes > 0 {
		println!(
			"  note: the reader dropped {} trailing byte(s) that could not form a sample",
			input.dropped_bytes
		);
	}
	let mut list = Vec::new();
	if both {
		list.push(HeaderConvention::SampleInHeader);
		list.push(HeaderConvention::RepeatPrevious);
	} else if let Some(c) = convention {
		list.push(c);
	} else {
		list.push(HeaderConvention::SampleInHeader);
	}
	let mut reports = Vec::new();
	for c in list {
		let (_wire, r) = round_trip(&input.samples, length_per_encoder, c)?;
		print_report(&r);
		reports.push(r);
	}
	if let (Some(a), Some(b)) = (reports.first(), reports.get(1)) {
		println!(
			"  compare: {} seam-held samples under sample vs {} under repeat \
			 (the file itself holds {} at those positions); slope swallowed per hold {:.0} vs \
			 {:.0} counts; seam/body error ratio {:.2} vs {:.2}; {} samples played vs {} for \
			 {} in",
			a.seam_holds,
			b.seam_holds,
			a.input_holds,
			a.hold_slope_mean,
			b.hold_slope_mean,
			a.seam_ratio(),
			b.seam_ratio(),
			a.out_samples,
			b.out_samples,
			a.in_samples
		);
	}
	Ok(())
}

fn print_report(r: &Report) {
	println!(
		"  header {}: {} block(s) of {} B = {} B on the wire",
		convention_label(r.convention),
		r.blocks,
		BLOCK_HEADER_BYTES + usize::from(r.length_per_encoder) / 2,
		r.wire_bytes
	);
	println!(
		"  samples: {} in, {} decoded back (+{} = padded tail and seam replays), \
		 {} carried per block",
		r.in_samples, r.out_samples, r.surplus, r.block_samples
	);
	println!(
		"  error against the input over {} samples: rms {:.1}, mean |e| {:.1}, max {} at sample {}",
		r.in_samples.min(r.aligned_samples),
		r.rmse,
		r.mean_abs,
		r.max_abs,
		r.max_at
	);
	println!(
		"  seams (sample % {} == 0): {} of them, rms {:.1} there vs {:.1} elsewhere -> ratio {:.2}",
		r.block_samples,
		r.seam_count,
		r.seam_rmse,
		r.body_rmse,
		r.seam_ratio()
	);
	println!(
		"  seams bit-exact against the input: {}; seams whose header replays the previous \
		 sample: {}",
		r.seam_exact, r.seam_replays
	);
	println!(
		"  samples HELD on a block boundary in the played stream: {} ({} held anywhere else; \
		 the input file itself holds {}); mean slope swallowed {:.0} counts, max {:.0}",
		r.seam_holds, r.other_holds, r.input_holds, r.hold_slope_mean, r.hold_slope_max
	);
	println!("  tail of the played stream (last 8 samples, pad included): {:?}", r.tail);
	println!("  worst errors (sample, sample % block, |error|):");
	for (i, m, e) in &r.worst {
		println!("    {i:>8}  {m:>5}  {e:>6}");
	}
	println!("  biggest error jumps (sample, sample % block, |jump|):");
	for (i, m, j) in &r.jumps {
		println!("    {i:>8}  {m:>5}  {j:>6}");
	}
	if r.seam_holds == 0 {
		println!(
			"  verdict: no sample is played twice at a block boundary ({} held elsewhere, all of \
			 them places the file itself is flat) — this convention cannot click at {:.1} times a \
			 second",
			r.other_holds,
			16000.0 / r.block_samples as f64
		);
	} else {
		println!(
			"  verdict: {} block boundaries HOLD the waveform (one every {} samples, i.e. every \
			 {:.1} ms, {:.1} times a second, mean {:.0} counts of slope swallowed): that train is \
			 the crackle",
			r.seam_holds,
			r.block_samples,
			r.block_samples as f64 * 1000.0 / 16000.0,
			16000.0 / r.block_samples as f64,
			r.hold_slope_mean
		);
	}
}

/// `--bench`: print the measured timer behaviour.
pub async fn run_jitter(steps: usize, step_ms: u64) {
	println!(
		"audio-check: pacing bench, {steps} sleeps of {step_ms} ms, no camera and no network"
	);
	let j = measure_jitter(step_ms, steps).await;
	println!(
		"  relative sleeps (how talk() paced before the fix: a fresh {step_ms} ms sleep per \
		 payload, so every overshoot is handed to the next one): {:.1} ms total against {:.1} ms \
		 expected (+{:.1} ms), mean overshoot {:.2} ms, worst {:.2} ms, {} sleeps over by > 1 ms",
		j.relative_total_ms,
		j.expected_ms,
		j.relative_total_ms - j.expected_ms,
		j.relative_mean_overshoot_ms,
		j.relative_max_overshoot_ms,
		j.relative_late_steps
	);
	println!(
		"  absolute deadlines (t0 + n x {step_ms} ms): {:.1} ms total (+{:.1} ms), mean lateness \
		 {:.2} ms, worst {:.2} ms, {} deadlines missed by > 1 ms",
		j.absolute_total_ms,
		j.absolute_total_ms - j.expected_ms,
		j.absolute_mean_late_ms,
		j.absolute_max_late_ms,
		j.absolute_late_steps
	);
	println!(
		"  scheduled (what talk()/talk_stream() do now: deadline + re-anchor only when more than \
		 one payload behind): {:.1} ms total (+{:.1} ms, the {} ms producer stall excluded), {} \
		 re-anchors, worst back-to-back burst {} payload(s), {} steps over the ideal by > 1 ms",
		j.scheduled_total_ms,
		j.scheduled_total_ms - j.expected_ms,
		STALL_MS,
		j.scheduled_reanchors,
		j.scheduled_burst_max,
		j.scheduled_late_steps
	);
	println!(
		"  so over a {:.2} s stream the sender itself drifts {:.1} ms relative, {:.1} ms absolute, \
		 {:.1} ms scheduled (a pure deadline cannot survive a real stall; the schedule gives up \
		 exactly {} payload(s) of it)",
		j.expected_ms / 1000.0,
		j.relative_total_ms - j.expected_ms,
		j.absolute_total_ms - j.expected_ms,
		j.scheduled_total_ms - j.expected_ms,
		j.scheduled_reanchors
	);
}

/// Entry point used by `main`, before any config file or camera handling.
pub async fn command(
	file: Option<PathBuf>,
	raw: bool,
	header: Option<String>,
	block_length: u16,
	bench: Option<usize>,
	compare: bool,
) -> Result<()> {
	if let Some(steps) = bench {
		run_jitter(steps.clamp(1, 100_000), 64).await;
		return Ok(());
	}
	let path = file.ok_or_else(|| {
		anyhow::Error::from(UsageError::new(
			"audio-check needs --file <PATH> (a 16-bit signed PCM WAV at 16000 Hz mono, \
			 or headerless PCM with --raw) — or --bench <N> to bench the 64 ms block timer \
			 instead",
		))
	})?;
	let convention = match header.as_deref() {
		Some(v) => Some(parse_convention(v)?),
		None => None,
	};
	run(&path, raw, block_length, convention, compare)
}
