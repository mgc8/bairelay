//! Minimal WAV reader for the talk path: RIFF/WAVE, 16-bit integer PCM
//! (`WAVE_FORMAT_PCM` or `WAVE_FORMAT_EXTENSIBLE` with the PCM
//! sub-format), any channel count (downmixed to mono by averaging),
//! plus a linear resampler and a volume gain. No external codec.

use anyhow::{bail, Context, Result};

use super::TalkError;

/// Decoded mono PCM.
#[derive(Debug, Clone, PartialEq)]
pub struct MonoPcm {
	pub sample_rate: u32,
	pub samples: Vec<i16>,
}

const WAVE_FORMAT_PCM: u16 = 0x0001;
const WAVE_FORMAT_EXTENSIBLE: u16 = 0xFFFE;

/// Highest sample rate accepted from a WAV file.
pub const MAX_SAMPLE_RATE: u32 = 48_000;
/// Most channels accepted from a WAV file (downmixed to mono).
pub const MAX_CHANNELS: u16 = 2;
/// Largest WAV file read: [`super::MAX_CLIP`] at [`MAX_SAMPLE_RATE`],
/// [`MAX_CHANNELS`], 16 bit, plus 1 MiB for headers and metadata chunks.
pub const MAX_WAV_BYTES: u64 =
	super::MAX_CLIP.as_secs() * MAX_SAMPLE_RATE as u64 * MAX_CHANNELS as u64 * 2 + (1 << 20);

/// Read and decode a WAV file from disk. Only a regular file is opened
/// (a FIFO or device would block or never end), and at most
/// [`MAX_WAV_BYTES`] are read. Blocking; call from a blocking thread.
pub fn read_wav_file(path: &std::path::Path) -> Result<MonoPcm, TalkError> {
	use std::io::Read;
	let meta = std::fs::metadata(path).map_err(|_| TalkError::NotFound)?;
	if !meta.is_file() {
		return Err(TalkError::NotAFile);
	}
	let file = std::fs::File::open(path).map_err(|_| TalkError::NotFound)?;
	// Re-check on the open handle: the path may have been swapped.
	if !file.metadata().map(|m| m.is_file()).unwrap_or(false) {
		return Err(TalkError::NotAFile);
	}
	let mut bytes = Vec::new();
	file.take(MAX_WAV_BYTES + 1)
		.read_to_end(&mut bytes)
		.map_err(|e| TalkError::Decode(anyhow::anyhow!("read: {e}")))?;
	if bytes.len() as u64 > MAX_WAV_BYTES {
		return Err(TalkError::Decode(anyhow::anyhow!(
			"file is larger than {MAX_WAV_BYTES} bytes"
		)));
	}
	parse_wav(&bytes).map_err(TalkError::Decode)
}

fn le_u16(b: &[u8], at: usize) -> u16 {
	u16::from_le_bytes([b[at], b[at + 1]])
}

fn le_u32(b: &[u8], at: usize) -> u32 {
	u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// Parse a WAV byte buffer into mono 16-bit PCM.
pub fn parse_wav(bytes: &[u8]) -> Result<MonoPcm> {
	if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
		bail!("not a RIFF/WAVE file");
	}
	let mut pos = 12;
	let mut fmt: Option<(u16, u32, u16)> = None; // (channels, rate, bits)
	let mut data: Option<&[u8]> = None;
	while pos + 8 <= bytes.len() {
		let id = &bytes[pos..pos + 4];
		let len = le_u32(bytes, pos + 4) as usize;
		let body_start = pos + 8;
		// Tolerate a data chunk whose declared size runs past EOF (some
		// streaming writers leave it at 0xFFFFFFFF).
		let body_end = body_start.saturating_add(len).min(bytes.len());
		let body = &bytes[body_start..body_end];
		match id {
			b"fmt " => {
				if body.len() < 16 {
					bail!("fmt chunk too short ({} bytes)", body.len());
				}
				let mut tag = le_u16(body, 0);
				let channels = le_u16(body, 2);
				let rate = le_u32(body, 4);
				let bits = le_u16(body, 14);
				if tag == WAVE_FORMAT_EXTENSIBLE {
					if body.len() < 26 {
						bail!("WAVE_FORMAT_EXTENSIBLE fmt chunk too short");
					}
					// First two bytes of the sub-format GUID carry the tag.
					tag = le_u16(body, 24);
				}
				if tag != WAVE_FORMAT_PCM {
					bail!("unsupported WAV format tag {tag:#06x}; need 16-bit integer PCM");
				}
				fmt = Some((channels, rate, bits));
			}
			b"data" => {
				data = Some(body);
			}
			_ => {}
		}
		if data.is_some() && fmt.is_some() {
			break;
		}
		// Chunks are word-aligned.
		pos = body_start.saturating_add(len).saturating_add(len & 1);
	}
	let (channels, rate, bits) = fmt.context("missing fmt chunk")?;
	let data = data.context("missing data chunk")?;
	if bits != 16 {
		bail!("unsupported sample size {bits} bits; need 16-bit PCM");
	}
	if channels == 0 {
		bail!("WAV declares 0 channels");
	}
	if rate == 0 {
		bail!("WAV declares a sample rate of 0");
	}
	if rate > MAX_SAMPLE_RATE {
		bail!("sample rate {rate} Hz is above the {MAX_SAMPLE_RATE} Hz limit");
	}
	if channels > MAX_CHANNELS {
		bail!("{channels} channels; at most {MAX_CHANNELS} are accepted");
	}
	let ch = usize::from(channels);
	let frame = 2 * ch;
	let samples = data
		.chunks_exact(frame)
		.map(|f| {
			let sum: i32 = f
				.chunks_exact(2)
				.map(|s| i32::from(i16::from_le_bytes([s[0], s[1]])))
				.sum();
			(sum / ch as i32) as i16
		})
		.collect();
	Ok(MonoPcm {
		sample_rate: rate,
		samples,
	})
}

/// Linear-interpolation resampler. Returns the input unchanged when the
/// rates match.
pub fn resample_linear(samples: &[i16], from: u32, to: u32) -> Vec<i16> {
	if from == to || samples.is_empty() {
		return samples.to_vec();
	}
	let out_len = ((samples.len() as u64 * u64::from(to)) / u64::from(from)) as usize;
	let ratio = f64::from(from) / f64::from(to);
	let last = samples.len() - 1;
	(0..out_len)
		.map(|i| {
			let x = i as f64 * ratio;
			let i0 = (x.floor() as usize).min(last);
			let i1 = (i0 + 1).min(last);
			let frac = x - i0 as f64;
			let y = f64::from(samples[i0]) * (1.0 - frac) + f64::from(samples[i1]) * frac;
			y.round() as i16
		})
		.collect()
}

/// Multiply by `volume`, saturating at the i16 range.
pub fn apply_volume(samples: &mut [i16], volume: f32) {
	if (volume - 1.0).abs() < f32::EPSILON {
		return;
	}
	for s in samples.iter_mut() {
		let v = (f32::from(*s) * volume).round();
		*s = v.clamp(f32::from(i16::MIN), f32::from(i16::MAX)) as i16;
	}
}

#[cfg(test)]
pub(crate) mod tests {
	use super::*;

	/// Build a canonical 16-bit PCM WAV for tests.
	pub(crate) fn make_wav(rate: u32, channels: u16, interleaved: &[i16]) -> Vec<u8> {
		let data_len = (interleaved.len() * 2) as u32;
		let mut v = Vec::new();
		v.extend_from_slice(b"RIFF");
		v.extend_from_slice(&(36 + data_len).to_le_bytes());
		v.extend_from_slice(b"WAVE");
		v.extend_from_slice(b"fmt ");
		v.extend_from_slice(&16u32.to_le_bytes());
		v.extend_from_slice(&WAVE_FORMAT_PCM.to_le_bytes());
		v.extend_from_slice(&channels.to_le_bytes());
		v.extend_from_slice(&rate.to_le_bytes());
		v.extend_from_slice(&(rate * u32::from(channels) * 2).to_le_bytes());
		v.extend_from_slice(&(channels * 2).to_le_bytes());
		v.extend_from_slice(&16u16.to_le_bytes());
		v.extend_from_slice(b"data");
		v.extend_from_slice(&data_len.to_le_bytes());
		for s in interleaved {
			v.extend_from_slice(&s.to_le_bytes());
		}
		v
	}

	#[test]
	fn parses_mono_pcm() {
		let wav = make_wav(16000, 1, &[1, -2, 300, i16::MIN]);
		let pcm = parse_wav(&wav).unwrap();
		assert_eq!(pcm.sample_rate, 16000);
		assert_eq!(pcm.samples, vec![1, -2, 300, i16::MIN]);
	}

	#[test]
	fn downmixes_stereo_and_skips_unknown_chunks() {
		let mut wav = make_wav(8000, 2, &[100, 300, -100, -300]);
		// Splice an odd-length LIST chunk (with pad byte) before fmt.
		let mut list = b"LIST".to_vec();
		list.extend_from_slice(&3u32.to_le_bytes());
		list.extend_from_slice(b"abc\0");
		wav.splice(12..12, list);
		let pcm = parse_wav(&wav).unwrap();
		assert_eq!(pcm.sample_rate, 8000);
		assert_eq!(pcm.samples, vec![200, -200]);
	}

	#[test]
	fn rejects_non_pcm_and_wrong_depth() {
		assert!(parse_wav(b"not a wav file at all").is_err());
		let mut wav = make_wav(16000, 1, &[0; 4]);
		wav[20] = 3; // IEEE float tag
		assert!(format!("{:#}", parse_wav(&wav).unwrap_err()).contains("format tag"));
		let mut wav = make_wav(16000, 1, &[0; 4]);
		wav[34] = 8; // 8-bit
		assert!(format!("{:#}", parse_wav(&wav).unwrap_err()).contains("8 bits"));
	}

	#[test]
	fn resample_lengths_and_identity() {
		let s: Vec<i16> = (0..800).map(|i| i as i16).collect();
		assert_eq!(resample_linear(&s, 16000, 16000), s);
		assert_eq!(resample_linear(&s, 8000, 16000).len(), 1600);
		assert_eq!(resample_linear(&s, 48000, 16000).len(), 266);
		// A ramp stays a ramp under linear interpolation.
		let up = resample_linear(&s, 8000, 16000);
		assert_eq!(up[10], 5);
		assert_eq!(up[11], 6); // 5.5 rounds half away from zero
	}

	#[test]
	fn rejects_high_rate_and_many_channels() {
		let wav = make_wav(96_000, 1, &[0; 4]);
		assert!(format!("{:#}", parse_wav(&wav).unwrap_err()).contains("96000"));
		let wav = make_wav(16_000, 6, &[0; 6]);
		assert!(format!("{:#}", parse_wav(&wav).unwrap_err()).contains("6 channels"));
	}

	#[test]
	fn read_rejects_directory_fifo_and_missing() {
		let dir = tempfile::tempdir().unwrap();
		assert!(matches!(
			read_wav_file(dir.path()),
			Err(TalkError::NotAFile)
		));
		let fifo = dir.path().join("pipe.wav");
		let st = std::process::Command::new("mkfifo")
			.arg(&fifo)
			.status()
			.expect("run mkfifo");
		assert!(st.success());
		// Must return at once, never open (and block on) the FIFO.
		assert!(matches!(read_wav_file(&fifo), Err(TalkError::NotAFile)));
		assert!(matches!(
			read_wav_file(&dir.path().join("missing.wav")),
			Err(TalkError::NotFound)
		));
		let good = dir.path().join("ok.wav");
		std::fs::write(&good, make_wav(16000, 1, &[1, 2, 3])).unwrap();
		assert_eq!(read_wav_file(&good).unwrap().samples, vec![1, 2, 3]);
	}

	#[test]
	fn volume_scales_and_saturates() {
		let mut s = vec![1000, -1000, 30000];
		apply_volume(&mut s, 2.0);
		assert_eq!(s, vec![2000, -2000, i16::MAX]);
	}
}
