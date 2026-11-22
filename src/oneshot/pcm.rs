//! PCM input for the talk one-shots.
//!
//! The camera's talk path is fixed by its own `<TalkAbility>` answer: signed
//! 16-bit little-endian, mono, 16000 Hz. Nothing in this protocol resamples or
//! down-mixes for you, so the loader's job is to accept *exactly* that and,
//! when it is handed anything else, say what it actually found instead of
//! feeding the camera garbage that plays back as noise.

use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use crate::oneshot::errors::UsageError;

/// The only sample rate the talk path is driven at.
pub const TALK_SAMPLE_RATE: u32 = 16_000;

/// Where the samples came from, for the report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcmOrigin {
	/// Parsed out of a `.wav` container.
	Wav,
	/// Headerless samples, read as-is (`--raw`).
	Raw,
}

impl PcmOrigin {
	pub fn label(&self) -> &'static str {
		match self {
			PcmOrigin::Wav => "wav",
			PcmOrigin::Raw => "raw",
		}
	}
}

/// PCM accepted for transmission, plus what the loader had to accept to get it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PcmInput {
	/// Signed 16-bit little-endian samples, mono, 16 kHz.
	pub samples: Vec<i16>,
	pub origin: PcmOrigin,
	/// A trailing odd byte (raw) or odd sample (wav) was dropped because it
	/// could not form a complete sample.
	pub dropped_bytes: usize,
}

impl PcmInput {
	/// Number of samples.
	pub fn len(&self) -> usize {
		self.samples.len()
	}

	/// `true` when there is nothing to send.
	pub fn is_empty(&self) -> bool {
		self.samples.is_empty()
	}

	/// Duration of the clip in seconds at the talk sample rate.
	pub fn duration_secs(&self) -> f64 {
		self.samples.len() as f64 / f64::from(TALK_SAMPLE_RATE)
	}
}

/// Read `path` as PCM for transmission.
///
/// * `raw = false` — parse a RIFF/WAVE container and require PCM 16-bit
///   signed, mono, 16000 Hz. Any other shape is refused with a message naming
///   the values actually found in the `fmt ` chunk.
/// * `raw = false` and the file *looks* like a headerless dump (no RIFF magic)
///   — the refusal says so and points at `--raw`, because that is almost
///   always what the operator has.
/// * `raw = true` — no container parsing at all: every byte is a sample pair.
///
/// Every refusal here is a [`UsageError`], i.e. exit code 2: the input file is
/// the operator's, so it is a wrong-argument failure, never a protocol or
/// transport one. This is what lets `run_support` validate the file *before*
/// connecting — a camera must not be woken to be handed a refusal.
pub fn read_pcm_file(path: &Path, raw: bool) -> Result<PcmInput> {
	let bytes = fs::read(path).map_err(|e| {
		anyhow::Error::new(UsageError::new(format!(
			"could not read {}: {e}",
			path.display()
		)))
	})?;
	if raw {
		return Ok(from_raw(&bytes));
	}
	parse_wav(&bytes)
		.map_err(|e| anyhow::Error::new(UsageError::new(format!("{e:#}"))))
		.with_context(|| describe_for_message(path))
}

/// Interpret a fully-buffered stream (a pipe) as headerless PCM. Used by
/// `audio-bridge`, whose input is raw s16le by contract.
pub fn read_pcm_bytes(bytes: &[u8]) -> PcmInput {
	from_raw(bytes)
}

fn describe_for_message(path: &Path) -> String {
	format!("{} is not usable as talk audio", path.display())
}

/// Interpret every byte pair as one signed LE sample, dropping an odd tail.
fn from_raw(bytes: &[u8]) -> PcmInput {
	let pairs = bytes.len() / 2;
	let dropped_bytes = bytes.len() - pairs * 2;
	let mut samples = Vec::with_capacity(pairs);
	for pair in bytes.chunks_exact(2) {
		samples.push(i16::from_le_bytes([pair[0], pair[1]]));
	}
	PcmInput {
		samples,
		origin: PcmOrigin::Raw,
		dropped_bytes,
	}
}

/// What a `fmt ` chunk said, before we decide whether it is usable.
#[derive(Debug, Clone, Copy)]
struct FmtChunk {
	audio_format: u16,
	channels: u16,
	sample_rate: u32,
	bits: u16,
}

impl FmtChunk {
	/// `None` when the file is exactly what the talk path needs, otherwise a
	/// polite refusal naming every value found and what is required instead.
	///
	/// Wording follows the rest of the CLI (English prose, French flag names)
	/// and lists *all* the problems at once, so one re-encode is enough.
	fn reject_reason(&self) -> Option<String> {
		let format_name = match self.audio_format {
			1 => "PCM".to_string(),
			3 => "IEEE float".to_string(),
			6 => "mu-law".to_string(),
			7 => "A-law".to_string(),
			0xfffe => "EXTENSIBLE".to_string(),
			other => format!("format tag {other}"),
		};
		let mut problems: Vec<String> = Vec::new();
		if self.audio_format != 1 {
			problems.push(format!("codec is {format_name}, needs integer PCM"));
		}
		if self.bits != 16 {
			problems.push(format!("depth is {} bits, needs 16", self.bits));
		}
		if self.channels != 1 {
			problems.push(format!("channel count is {}, needs 1 (mono)", self.channels));
		}
		if self.sample_rate != TALK_SAMPLE_RATE {
			problems.push(format!(
				"sample rate is {} Hz, needs {} Hz",
				self.sample_rate, TALK_SAMPLE_RATE
			));
		}
		if problems.is_empty() {
			return None;
		}
		let channel_word = if self.channels == 1 { "channel" } else { "channels" };
		Some(format!(
			"found {} {channel_word} / {} Hz / {} bits / {}; {}; the talk path is fixed at 16000 Hz mono 16-bit signed PCM — re-encode the file (for example: ffmpeg -i INPUT -ar 16000 -ac 1 -sample_fmt s16 OUTPUT.wav)",
			self.channels,
			self.sample_rate,
			self.bits,
			format_name,
			problems.join("; "),
		))
	}
}

/// Minimal RIFF/WAVE reader: only `fmt ` and `data` are interpreted, every
/// other chunk is skipped, and a truncated file is tolerated as long as both
/// chunks were fully seen.
fn parse_wav(bytes: &[u8]) -> Result<PcmInput> {
	if bytes.len() < 12 {
		return Err(anyhow!(
			"file is too short ({} bytes) to hold a RIFF/WAVE header; if this is \
			 headerless PCM (s16le / 16000 Hz / mono), re-run with --raw",
			bytes.len()
		));
	}
	if &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
		let riff = ascii_escape(&bytes[0..4]);
		let wave = ascii_escape(&bytes[8..12]);
		return Err(anyhow!(
			"bytes 0..4 are \"{riff}\" and 8..12 are \"{wave}\", so this is not a \
			 RIFF/WAVE file; if it is headerless PCM (s16le / 16000 Hz / mono), \
			 re-run with --raw"
		));
	}

	let mut fmt: Option<FmtChunk> = None;
	let mut data: Option<&[u8]> = None;
	let mut pos = 12usize;
	while pos + 8 <= bytes.len() {
		let id = &bytes[pos..pos + 4];
		let size = u32::from_le_bytes([
			bytes[pos + 4],
			bytes[pos + 5],
			bytes[pos + 6],
			bytes[pos + 7],
		]) as usize;
		let body_start = pos + 8;
		if body_start >= bytes.len() {
			break;
		}
		// A chunk claiming more bytes than the file holds is truncated; take
		// what is there rather than failing on a writer that lied about sizes.
		let body_end = body_start.saturating_add(size).min(bytes.len());
		let body = &bytes[body_start..body_end];
		match id {
			b"fmt " if body.len() >= 16 => {
				fmt = Some(FmtChunk {
					audio_format: u16::from_le_bytes([body[0], body[1]]),
					channels: u16::from_le_bytes([body[2], body[3]]),
					sample_rate: u32::from_le_bytes([body[4], body[5], body[6], body[7]]),
					bits: u16::from_le_bytes([body[14], body[15]]),
				});
			}
			b"data" => {
				if data.is_none() {
					data = Some(body);
				}
			}
			_ => {}
		}
		// Chunks are word-aligned: an odd size is followed by one pad byte.
		pos = body_end + usize::from(size % 2 == 1);
	}

	let fmt = fmt
		.ok_or_else(|| anyhow!("no usable \"fmt \" chunk in this WAV (header truncated?)"))?;
	if let Some(reason) = fmt.reject_reason() {
		return Err(anyhow!(reason));
	}
	let data = data.ok_or_else(|| anyhow!("WAV has a usable \"fmt \" chunk but no \"data\" chunk"))?;

	let pairs = data.len() / 2;
	let dropped_bytes = data.len() - pairs * 2;
	let mut samples = Vec::with_capacity(pairs);
	for pair in data.chunks_exact(2) {
		samples.push(i16::from_le_bytes([pair[0], pair[1]]));
	}
	Ok(PcmInput {
		samples,
		origin: PcmOrigin::Wav,
		dropped_bytes,
	})
}

/// Render four bytes as printable ASCII, non-printables as `\\xNN`, so a
/// refusal message can quote a wrong magic without injecting control codes.
fn ascii_escape(bytes: &[u8]) -> String {
	bytes
		.iter()
		.map(|&b| {
			if b.is_ascii_graphic() || b == b' ' {
				char::from(b).to_string()
			} else {
				format!("\\x{b:02X}")
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn wav_header(fmt: u16, channels: u16, rate: u32, bits: u16, data_len: u32) -> Vec<u8> {
		let mut out = Vec::new();
		out.extend_from_slice(b"RIFF");
		out.extend_from_slice(&(36 + data_len).to_le_bytes());
		out.extend_from_slice(b"WAVE");
		out.extend_from_slice(b"fmt ");
		out.extend_from_slice(&16u32.to_le_bytes());
		out.extend_from_slice(&fmt.to_le_bytes());
		out.extend_from_slice(&channels.to_le_bytes());
		out.extend_from_slice(&rate.to_le_bytes());
		let byte_rate = rate * channels as u32 * (bits as u32 / 8);
		out.extend_from_slice(&byte_rate.to_le_bytes());
		out.extend_from_slice(&(channels * (bits as u16 / 8)).to_le_bytes());
		out.extend_from_slice(&bits.to_le_bytes());
		out.extend_from_slice(b"data");
		out.extend_from_slice(&data_len.to_le_bytes());
		out
	}

	#[test]
	fn accepts_a_conformant_wav() {
		let mut bytes = wav_header(1, 1, 16_000, 16, 4);
		bytes.extend_from_slice(&[0x00, 0x00, 0x2c, 0x01]);
		let input = parse_wav(&bytes).expect("must accept");
		assert_eq!(input.origin, PcmOrigin::Wav);
		assert_eq!(input.samples, vec![0, 300]);
		assert_eq!(input.dropped_bytes, 0);
	}

	#[test]
	fn rejects_44100_naming_what_it_found() {
		let bytes = wav_header(1, 1, 44_100, 16, 0);
		let err = parse_wav(&bytes).expect_err("44.1 kHz must be refused");
		let msg = err.to_string();
		assert!(msg.contains("44100"), "message was: {msg}");
		assert!(msg.contains("needs 16000 Hz"), "message was: {msg}");
	}

	#[test]
	fn rejects_stereo_and_float_and_8_bit() {
		for (fmt, channels, rate, bits, needle) in [
			(1u16, 2u16, 16_000u32, 16u16, "channel count is 2"),
			(3, 1, 16_000, 32, "IEEE float"),
			(7, 1, 8_000, 8, "A-law"),
		] {
			let bytes = wav_header(fmt, channels, rate, bits, 0);
			let msg = parse_wav(&bytes).expect_err("must refuse").to_string();
			assert!(msg.contains(needle), "{needle} missing from: {msg}");
		}
	}

	#[test]
	fn headerless_input_points_at_the_raw_flag() {
		// Long enough to have a RIFF field worth quoting, but not a WAV.
		let mut bytes = vec![0x34, 0x12, 0x78, 0x56];
		bytes.extend_from_slice(&[0u8; 16]);
		let msg = parse_wav(&bytes).expect_err("must refuse").to_string();
		assert!(msg.contains("--raw"), "message was: {msg}");
		// Non-text bytes of the magic are quoted as hex; 0x34 is '4' and
		// stays readable, 0x12 must not leak raw.
		assert!(msg.contains("\\x12"), "magic must be quoted: {msg}");
	}

	#[test]
	fn raw_input_keeps_every_pair_and_reports_the_odd_tail() {
		let input = from_raw(&[0x00, 0x00, 0xff, 0x7f, 0x11]);
		assert_eq!(input.origin, PcmOrigin::Raw);
		assert_eq!(input.samples, vec![0, i16::MAX]);
		assert_eq!(input.dropped_bytes, 1);
		assert!((input.duration_secs() - 2.0 / 16_000.0).abs() < 1e-12);
	}

	#[test]
	fn skips_unknown_chunks_and_honours_word_padding() {
		// `LIST` with an odd size, sitting between `fmt ` and `data`: the
		// reader must skip it plus its pad byte and still find the samples.
		let mut bytes = Vec::new();
		bytes.extend_from_slice(b"RIFF");
		bytes.extend_from_slice(&0u32.to_le_bytes());
		bytes.extend_from_slice(b"WAVE");
		bytes.extend_from_slice(b"fmt ");
		bytes.extend_from_slice(&16u32.to_le_bytes());
		bytes.extend_from_slice(&1u16.to_le_bytes()); // PCM
		bytes.extend_from_slice(&1u16.to_le_bytes()); // mono
		bytes.extend_from_slice(&16_000u32.to_le_bytes());
		bytes.extend_from_slice(&32_000u32.to_le_bytes()); // byte rate
		bytes.extend_from_slice(&2u16.to_le_bytes()); // block align
		bytes.extend_from_slice(&16u16.to_le_bytes()); // bits
		bytes.extend_from_slice(b"LIST");
		bytes.extend_from_slice(&3u32.to_le_bytes());
		bytes.extend_from_slice(b"abc");
		bytes.push(0x00); // word-alignment pad
		bytes.extend_from_slice(b"data");
		bytes.extend_from_slice(&2u32.to_le_bytes());
		bytes.extend_from_slice(&[0x34, 0x12]);

		let input = parse_wav(&bytes).expect("LIST must be skipped");
		assert_eq!(input.samples, vec![0x1234]);
	}
}
