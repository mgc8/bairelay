//! Input formats accepted by the live intercom bridge (`audio-bridge`).
//!
//! go2rtc writes whatever it *received* into the bridge's stdin, byte for byte:
//! `pkg/pcm/backchannel.go` installs a handler that does `wr.Write(packet.Payload)`
//! and does no transcoding whatsoever. The payload is therefore the codec the
//! browser actually negotiated, and a browser can only negotiate what it offered
//! in the SDP — Opus at 48 kHz, or G.711 at 8 kHz. Raw 16-bit PCM at 16 kHz is
//! never on that list, so a backchannel declared as `s16le/16000` simply is not
//! matched (go2rtc's [`Media::MatchMedia`] also refuses a `sendrecv` m-line
//! outright, which is why the intercom page now offers two audio lines).
//!
//! So the widening has to happen here: G.711 expand is a 256-entry table, and
//! 8 kHz → the camera's talk rate is an integer-ratio linear interpolator. Both
//! fit in this file and neither needs a codec library.
//!
//! [`Media::MatchMedia`]: https://github.com/AlexxIT/go2rtc/blob/master/pkg/core/media.go

use anyhow::{Result, anyhow};

/// A raw input format the bridge knows how to read from stdin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Input {
	/// Signed 16-bit little-endian PCM at `hz` — the format the bridge was
	/// originally written for, and the one `sox`/`ffmpeg` will happily pipe.
	S16Le { hz: u32 },
	/// G.711 μ-law, 8 kHz, one byte per sample (WebRTC payload type 0).
	Pcmu8k,
	/// G.711 A-law, 8 kHz, one byte per sample (WebRTC payload type 8).
	Pcma8k,
}

impl Input {
	/// Parse an `--input` value. Accepts the same spellings go2rtc's
	/// `ParseCodecString` accepts for these codecs, plus the bare names, so the
	/// string in `go2rtc.yaml` and the one on the command line can be identical.
	pub fn parse(text: &str) -> Result<Self> {
		let lower = text.trim().to_ascii_lowercase();
		let (name, hz) = match lower.split_once('/') {
			Some((name, hz)) => (name, hz.trim().parse::<u32>().ok()),
			None => (lower.as_str(), None),
		};
		let input = match name {
			"pcm_s16le" | "s16le" | "pcml" | "s16" | "pcm" => Self::S16Le { hz: hz.unwrap_or(16_000) },
			"pcm_mulaw" | "mulaw" | "pcmu" | "g711u" | "ulaw" => {
				if matches!(hz, Some(h) if h != 8_000) {
					return Err(anyhow!(
						"--input {text}: G.711 mu-law is 8000 Hz only, that is what a browser \
						 negotiates (payload type 0); got {hz:?}"
					));
				}
				Self::Pcmu8k
			}
			"pcm_alaw" | "alaw" | "pcma" | "g711a" => {
				if matches!(hz, Some(h) if h != 8_000) {
					return Err(anyhow!(
						"--input {text}: G.711 A-law is 8000 Hz only, that is what a browser \
						 negotiates (payload type 8); got {hz:?}"
					));
				}
				Self::Pcma8k
			}
			_ => {
				return Err(anyhow!(
					"--input {text}: not understood. Use s16le/16000 (raw PCM), pcmu/8000 or \
					 pcma/8000 (what a browser sends over WebRTC)."
				));
			}
		};
		Ok(input)
	}

	/// Sample rate of this input format in hertz.
	pub fn hz(self) -> u32 {
		match self {
			Self::S16Le { hz } => hz,
			Self::Pcmu8k | Self::Pcma8k => 8_000,
		}
	}

	/// Human-readable form, for the startup log line.
	pub fn describe(self) -> String {
		match self {
			Self::S16Le { hz } => format!("s16le/{hz}"),
			Self::Pcmu8k => "pcmu/8000 (G.711 mu-law)".to_string(),
			Self::Pcma8k => "pcma/8000 (G.711 A-law)".to_string(),
		}
	}

	/// Build the converter that turns this format into little-endian s16 samples
	/// at `target_hz`, which is what the ADPCM encoder downstream eats.
	pub fn converter(self, target_hz: u32) -> Result<Converter> {
		match self {
			Self::S16Le { hz } if hz == target_hz => Ok(Converter {
				input: self,
				target_hz,
				ratio: 1,
				previous: 0,
				started: false,
			}),
			Self::S16Le { hz } => Err(anyhow!(
				"stdin is {hz} Hz but the camera's talk session negotiated {target_hz} Hz; \
				 pass --input with the rate the pipe really carries, or let the browser send \
				 pcmu/8000 and the bridge will resample"
			)),
			Self::Pcmu8k | Self::Pcma8k => {
				// The interpolator uses an integer ratio. Any talk rate that is
				// not a whole multiple of 8 kHz (Reolink reports 8000 or 16000)
				// gets the nearest ratio and a warning in the log rather than a
				// silent wrong pitch.
				let ratio = ((target_hz as f64) / 8_000.0).round() as i32;
				Ok(Converter {
					input: self,
					target_hz,
					ratio: ratio.max(1),
					previous: 0,
					started: false,
				})
			}
		}
	}
}

/// Turn raw stdin bytes into the s16le stream the talk encoder expects.
///
/// State is one sample (the interpolation anchor), so a chunk boundary can fall
/// anywhere without a click: G.711 is byte-aligned and the interpolator carries
/// its anchor across chunks.
#[derive(Debug, Clone)]
pub struct Converter {
	input: Input,
	target_hz: u32,
	ratio: i32,
	previous: i16,
	started: bool,
}

impl Converter {
	/// True when conversion is the identity (raw PCM at the camera's rate).
	pub fn is_identity(&self) -> bool {
		matches!(self.input, Input::S16Le { .. })
	}

	/// One-line description of the conversion, for the log.
	pub fn describe(&self) -> String {
		if self.is_identity() {
			format!("{} -> s16le/{}", self.input.describe(), self.target_hz)
		} else if self.ratio == 1 {
			format!(
				"{} -> s16le/{} (resampling membrane 1:1)",
				self.input.describe(),
				self.target_hz
			)
		} else {
			format!(
				"{} -> s16le/{} (resampling x{})",
				self.input.describe(),
				self.target_hz,
				self.ratio
			)
		}
	}

	/// Convert one stdin chunk. Returns `ratio` samples per input sample for
	/// the G.711 formats, and passes raw PCM through untouched.
	pub fn convert(&mut self, raw: &[u8]) -> Vec<u8> {
		if self.is_identity() {
			return raw.to_vec();
		}
		let ratio = usize::try_from(self.ratio).unwrap_or(1);
		let mut out = Vec::with_capacity(raw.len() * 2 * ratio);
		for &byte in raw {
			let sample = match self.input {
				Input::Pcmu8k => mulaw(byte),
				Input::Pcma8k => alaw(byte),
				// `is_identity()` returned false, so this arm is unreachable.
				Input::S16Le { .. } => 0,
			};
			// Linear ramp from the previous sample to this one, ending exactly
			// on this one: `ratio` output samples per input sample.
			let start = if self.started { self.previous } else { sample };
			let delta = i32::from(sample) - i32::from(start);
			for k in 1..=self.ratio {
				let value = i32::from(start) + (delta * k) / self.ratio;
				let value = i16::try_from(value).unwrap_or(if value < 0 { i16::MIN } else { i16::MAX });
				out.extend_from_slice(&value.to_le_bytes());
			}
			self.previous = sample;
			self.started = true;
		}
		out
	}
}

/// G.711 μ-law expand, ITU-T G.711. `sample = ((mantissa * 8 + 132) << exponent) - 132`,
/// with the sign taken from bit 7 of the complemented code (`0xFF` is +0,
/// `0x7F` is −0). Built once at compile time.
const fn table_mulaw() -> [i16; 256] {
	let mut table = [0i16; 256];
	let mut i = 0usize;
	while i < 256 {
		let code = !(i as u8);
		let exponent = ((code >> 4) & 0x07) as i32;
		let mantissa = (code & 0x0F) as i32;
		let mut value = ((mantissa * 8 + 132) << exponent) - 132;
		if code & 0x80 != 0 {
			value = -value;
		}
		table[i] = if value > 32767 {
			i16::MAX
		} else if value < -32768 {
			i16::MIN
		} else {
			value as i16
		};
		i += 1;
	}
	table
}

/// G.711 A-law expand, ITU-T G.711: complement with `0x55`, then segment
/// arithmetic. Bit 7 of the complemented code set means **positive** here — the
/// opposite convention to μ-law, which is why the two tables are not each
/// other's negation. The segment-0 origin is kept as the reference decoder
/// keeps it (the zero codewords expand to ±8, i.e. −72 dBFS: silence).
const fn table_alaw() -> [i16; 256] {
	let mut table = [0i16; 256];
	let mut i = 0usize;
	while i < 256 {
		let code = (i as u8) ^ 0x55;
		let segment = ((code >> 4) & 0x07) as i32;
		let mantissa = (code & 0x0F) as i32;
		let mut value = mantissa << 4;
		if segment == 0 {
			value += 8;
		} else if segment == 1 {
			value += 0x108;
		} else {
			// Origin of the segment is 0x108 and nothing else: adding the
			// segment-0 offset here too pushes the top of segment 7 to 32768,
			// which clamps and breaks the sign symmetry of the table.
			value = (value + 0x108) << (segment - 1);
		}
		if code & 0x80 == 0 {
			value = -value;
		}
		table[i] = if value > 32767 {
			i16::MAX
		} else if value < -32768 {
			i16::MIN
		} else {
			value as i16
		};
		i += 1;
	}
	table
}

static TABLE_MULAW: [i16; 256] = table_mulaw();
static TABLE_ALAW: [i16; 256] = table_alaw();

/// Expand one μ-law byte to a 16-bit sample.
pub fn mulaw(byte: u8) -> i16 {
	TABLE_MULAW[usize::from(byte)]
}

/// Expand one A-law byte to a 16-bit sample.
pub fn alaw(byte: u8) -> i16 {
	TABLE_ALAW[usize::from(byte)]
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn mu_law_zeroes_are_silent() {
		// 0xFF is positive zero and 0x7F is negative zero; both must land on 0,
		// otherwise G.711 "silence" would bias the talk stream.
		assert_eq!(mulaw(0xFF), 0);
		assert_eq!(mulaw(0x7F), 0);
	}

	#[test]
	fn mu_law_full_scale_matches_the_standard() {
		// The transmitted byte is the one's complement, so after complementing,
		// bit 7 set means negative: 0x00 = -32124, 0x80 = +32124. The two zeros
		// pinned above (0xFF = +0, 0x7F = -0) only exist under this convention.
		assert_eq!(mulaw(0x00), -32_124);
		assert_eq!(mulaw(0x80), 32_124);
	}

	#[test]
	fn a_law_zero_and_full_scale_match_the_standard() {
		// A-law is transmitted XOR 0x55 and, unlike μ-law, bit 7 set after the
		// XOR means positive. Segment 7 mantissa 15 lands on 32256, comfortably
		// inside i16 — a table that clamps is a table with the wrong origin.
		assert_eq!(alaw(0xD5), 8); // +0: segment 0, mantissa 0
		assert_eq!(alaw(0x55), -8); // -0
		// 0x2A ^ 0x55 = 0x7F, sign bit clear → negative full scale; 0xAA
		// ^ 0x55 = 0xFF, sign bit set → positive full scale.
		assert_eq!(alaw(0x2A), -32_256);
		assert_eq!(alaw(0xAA), 32_256);
		assert_eq!(alaw(0x2A), -alaw(0xAA));
	}

	#[test]
	fn signs_are_symmetric_for_both_tables() {
		for i in 0..128u16 {
			let a = u8::try_from(i).unwrap();
			let b = u8::try_from(i | 0x80).unwrap();
			assert_eq!(mulaw(a), -mulaw(b), "mu-law byte {i:02X}");
			assert_eq!(alaw(a), -alaw(b), "a-law byte {i:02X}");
		}
	}

	#[test]
	fn eighty_kilohertz_widens_to_sixteen() {
		let mut conv = Input::Pcmu8k.converter(16_000).unwrap();
		assert_eq!(conv.ratio, 2);
		// Ten μ-law samples of full positive scale (0x80) then the same of
		// silence: the converter must double the sample count and never exceed
		// the source.
		let raw = vec![0x80u8; 10];
		let out = conv.convert(&raw);
		assert_eq!(out.len(), 10 * 2 * 2);
		let pics: Vec<i16> = out
			.chunks_exact(2)
			.map(|p| i16::from_le_bytes([p[0], p[1]]))
			.collect();
		assert_eq!(pics.last().copied(), Some(32_124));
		assert!(pics.iter().all(|v| *v >= 0 && *v <= 32_124));
	}

	#[test]
	fn the_interpolator_carries_its_anchor_across_chunks() {
		let mut conv = Input::Pcmu8k.converter(16_000).unwrap();
		let first_chunk = conv.convert(&[0x80]);
		let second_chunk = conv.convert(&[0xFF]);
		let first: Vec<i16> = first_chunk
			.chunks_exact(2)
			.map(|p| i16::from_le_bytes([p[0], p[1]]))
			.collect();
		let second: Vec<i16> = second_chunk
			.chunks_exact(2)
			.map(|p| i16::from_le_bytes([p[0], p[1]]))
			.collect();
		// First output of the second chunk must be the ramp *down* from +32124,
		// not a jump straight to zero: that jump would be a click at every read.
		assert!(second[0] > 15_000, "{first:?} then {second:?}");
	}

	#[test]
	fn raw_pcm_at_the_camera_rate_passes_through() {
		let mut conv = Input::S16Le { hz: 16_000 }.converter(16_000).unwrap();
		let raw = vec![1u8, 2, 3, 4];
		assert_eq!(conv.convert(&raw), raw);
		assert!(conv.is_identity());
	}

	#[test]
	fn raw_pcm_at_the_wrong_rate_is_refused_before_the_camera_hears_anything() {
		assert!(Input::S16Le { hz: 48_000 }.converter(16_000).is_err());
	}

	#[test]
	fn spellings_go2rtc_writes_are_accepted() {
		for text in ["s16le", "s16le/16000", "pcm_s16le/16000", "pcml/16000"] {
			assert!(matches!(Input::parse(text), Ok(Input::S16Le { .. })), "{text}");
		}
		assert_eq!(Input::parse("pcmu/8000").unwrap(), Input::Pcmu8k);
		assert_eq!(Input::parse("G711U").unwrap(), Input::Pcmu8k);
		assert_eq!(Input::parse("pcm_alaw/8000").unwrap(), Input::Pcma8k);
		assert!(Input::parse("opus/48000").is_err());
		assert!(Input::parse("pcmu/16000").is_err());
		// Not a codec name anyone writes, and a typo here must not quietly
		// become "raw PCM at 16 kHz" — that is exactly the silent-mono failure
		// this whole module exists to kill.
		assert!(Input::parse("S16L").is_err());
		assert!(Input::parse("").is_err());
	}
}
