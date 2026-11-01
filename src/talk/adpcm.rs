//! Pure-Rust IMA ADPCM encoder in the DVI4 block layout that the
//! neolink talk client produced with GStreamer
//! (`adpcmenc blockalign=<n> layout=dvi`, neolink `src/talk/gst.rs:152`).
//!
//! One mono block of `block_align` bytes is:
//!
//! | bytes      | content                                          |
//! |------------|--------------------------------------------------|
//! | 0..2       | first sample of the block, `i16` little-endian   |
//! | 2          | step index (0..=88) at the start of the block    |
//! | 3          | reserved, always 0                               |
//! | 4..        | `2 * (block_align - 4)` 4-bit codes, low nibble first |
//!
//! So a block carries `2 * (block_align - 4) + 1` samples. neolink sets
//! `block_align = length_per_encoder / 2 + 4` (neolink
//! `src/talk/mod.rs:98`): 516 bytes and 1025 samples for the Reolink
//! default `length_per_encoder = 1024`. The core `BcCamera::talk`
//! chunks its input by the same `length_per_encoder / 2 + 4`.
//!
//! The predictor value restarts from the raw header sample at each
//! block; the step index carries over from the previous block.

/// IMA ADPCM step size table (89 entries).
pub(crate) const STEP_TABLE: [i32; 89] = [
	7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50, 55, 60, 66,
	73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449,
	494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552, 1707, 1878, 2066, 2272,
	2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493,
	10442, 11487, 12635, 13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767,
];

/// IMA ADPCM step index adjustment, indexed by the 4-bit code.
pub(crate) const INDEX_TABLE: [i32; 16] = [-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8];

/// Size of the per-block header in bytes (mono).
pub const BLOCK_HEADER_LEN: usize = 4;

/// Block size in bytes for a camera `length_per_encoder` value, as
/// neolink computes it (`length_per_encoder / 2 + 4`).
pub fn block_align_for(length_per_encoder: u16) -> usize {
	usize::from(length_per_encoder / 2) + BLOCK_HEADER_LEN
}

/// Number of PCM samples that one block of `block_align` bytes carries.
pub fn samples_per_block(block_align: usize) -> usize {
	(block_align - BLOCK_HEADER_LEN) * 2 + 1
}

/// Encoder state for one channel.
struct ImaState {
	predictor: i32,
	step_index: i32,
}

impl ImaState {
	/// Encode one sample to a 4-bit code and advance the state the same
	/// way a decoder does, so encoder and decoder stay in step.
	fn encode(&mut self, sample: i16) -> u8 {
		let step = STEP_TABLE[self.step_index as usize];
		let mut diff = i32::from(sample) - self.predictor;
		let mut code: u8 = 0;
		if diff < 0 {
			code = 8;
			diff = -diff;
		}
		let mut vpdiff = step >> 3;
		let mut s = step;
		let mut mask: u8 = 4;
		while mask != 0 {
			if diff >= s {
				code |= mask;
				diff -= s;
				vpdiff += s;
			}
			s >>= 1;
			mask >>= 1;
		}
		if code & 8 != 0 {
			self.predictor -= vpdiff;
		} else {
			self.predictor += vpdiff;
		}
		self.predictor = self
			.predictor
			.clamp(i32::from(i16::MIN), i32::from(i16::MAX));
		self.step_index = (self.step_index + INDEX_TABLE[code as usize]).clamp(0, 88);
		code
	}
}

/// Encode mono 16-bit PCM into DVI4 IMA ADPCM blocks of `block_align`
/// bytes each. The last block is padded with silence, so the output
/// length is always a multiple of `block_align`. Empty input gives an
/// empty output.
///
/// # Panics
///
/// Panics if `block_align` is not larger than the 4-byte header.
pub fn encode_dvi4(pcm: &[i16], block_align: usize) -> Vec<u8> {
	assert!(
		block_align > BLOCK_HEADER_LEN,
		"block_align must exceed the {BLOCK_HEADER_LEN}-byte header"
	);
	let spb = samples_per_block(block_align);
	let blocks = pcm.len().div_ceil(spb);
	let mut out = Vec::with_capacity(blocks * block_align);
	let mut step_index = 0i32;
	let mut padded = vec![0i16; spb];
	for chunk in pcm.chunks(spb) {
		let block: &[i16] = if chunk.len() == spb {
			chunk
		} else {
			padded[..chunk.len()].copy_from_slice(chunk);
			padded[chunk.len()..].fill(0);
			&padded
		};
		let first = block[0];
		out.extend_from_slice(&first.to_le_bytes());
		out.push(step_index as u8);
		out.push(0);
		let mut state = ImaState {
			predictor: i32::from(first),
			step_index,
		};
		for pair in block[1..].chunks_exact(2) {
			let lo = state.encode(pair[0]);
			let hi = state.encode(pair[1]);
			out.push(lo | (hi << 4));
		}
		step_index = state.step_index;
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Reference IMA ADPCM decoder for the DVI4 block layout, written
	/// from the IMA spec independent of the encoder's state struct.
	fn decode_dvi4(data: &[u8], block_align: usize) -> Vec<i16> {
		assert_eq!(data.len() % block_align, 0, "partial block");
		let mut out = Vec::new();
		for block in data.chunks_exact(block_align) {
			let mut predictor = i32::from(i16::from_le_bytes([block[0], block[1]]));
			let mut index = i32::from(block[2]);
			assert!(index <= 88, "step index out of range: {index}");
			assert_eq!(block[3], 0, "reserved header byte must be 0");
			out.push(predictor as i16);
			for &byte in &block[BLOCK_HEADER_LEN..] {
				for code in [byte & 0x0f, byte >> 4] {
					let step = STEP_TABLE[index as usize];
					let mut diff = step >> 3;
					if code & 4 != 0 {
						diff += step;
					}
					if code & 2 != 0 {
						diff += step >> 1;
					}
					if code & 1 != 0 {
						diff += step >> 2;
					}
					if code & 8 != 0 {
						predictor -= diff;
					} else {
						predictor += diff;
					}
					predictor = predictor.clamp(-32768, 32767);
					index = (index + INDEX_TABLE[code as usize]).clamp(0, 88);
					out.push(predictor as i16);
				}
			}
		}
		out
	}

	fn sine(n: usize, freq: f64, rate: f64, amp: f64) -> Vec<i16> {
		(0..n)
			.map(|i| (amp * (2.0 * std::f64::consts::PI * freq * i as f64 / rate).sin()) as i16)
			.collect()
	}

	#[test]
	fn reolink_default_block_geometry() {
		// length_per_encoder 1024 → 516-byte blocks of 1025 samples.
		assert_eq!(block_align_for(1024), 516);
		assert_eq!(samples_per_block(516), 1025);
	}

	#[test]
	fn output_is_whole_blocks_and_pads_the_tail() {
		let ba = block_align_for(1024);
		assert!(encode_dvi4(&[], ba).is_empty());
		assert_eq!(encode_dvi4(&[1; 1], ba).len(), ba);
		assert_eq!(encode_dvi4(&[1; 1025], ba).len(), ba);
		assert_eq!(encode_dvi4(&[1; 1026], ba).len(), 2 * ba);
		assert_eq!(encode_dvi4(&[1; 3 * 1025], ba).len(), 3 * ba);
	}

	#[test]
	fn block_header_layout() {
		let ba = block_align_for(1024);
		let pcm = sine(3 * 1025, 440.0, 16000.0, 12000.0);
		let enc = encode_dvi4(&pcm, ba);
		// Block 0: raw first sample, step index 0, reserved 0.
		assert_eq!(i16::from_le_bytes([enc[0], enc[1]]), pcm[0]);
		assert_eq!(enc[2], 0);
		assert_eq!(enc[3], 0);
		// Later blocks: raw first sample of that block; the step index
		// is carried over (non-zero after a loud sine) and in range.
		for b in 1..3 {
			let h = &enc[b * ba..b * ba + 4];
			assert_eq!(i16::from_le_bytes([h[0], h[1]]), pcm[b * 1025]);
			assert!(h[2] > 0 && h[2] <= 88, "step index {}", h[2]);
			assert_eq!(h[3], 0);
		}
	}

	#[test]
	fn first_code_is_low_nibble() {
		// One block: header sample 0, then a large positive step followed
		// by silence-ish. The first code must be in the low nibble and be
		// positive with magnitude bits set; the second (back to 0) negative.
		let ba = 8; // 4 header bytes + 4 data bytes = 9 samples
		let mut pcm = vec![0i16; samples_per_block(ba)];
		pcm[1] = 30000;
		let enc = encode_dvi4(&pcm, ba);
		assert_eq!(enc.len(), ba);
		let first = enc[4] & 0x0f;
		let second = enc[4] >> 4;
		assert_eq!(first, 0x7, "max positive code expected in low nibble");
		assert_eq!(second & 8, 8, "second code steps back down");
	}

	#[test]
	fn round_trip_matches_reference_decoder_state() {
		// The reference decoder must reproduce, sample for sample, the
		// predictor track the encoder followed while it encoded.
		let ba = block_align_for(1024);
		let spb = samples_per_block(ba);
		let pcm = sine(4500, 523.25, 16000.0, 9000.0);
		let enc = encode_dvi4(&pcm, ba);
		let dec = decode_dvi4(&enc, ba);
		assert_eq!(dec.len(), enc.len() / ba * spb);

		let mut padded = pcm.clone();
		padded.resize(dec.len(), 0);
		let mut step_index = 0;
		for (b, block) in padded.chunks(spb).enumerate() {
			let mut st = ImaState {
				predictor: i32::from(block[0]),
				step_index,
			};
			let mut track = vec![block[0]];
			for &s in &block[1..] {
				st.encode(s);
				track.push(st.predictor as i16);
			}
			assert_eq!(&dec[b * spb..(b + 1) * spb], &track[..], "block {b}");
			step_index = st.step_index;
		}
		// Padded tail decodes to near silence.
		let tail = &dec[pcm.len() + 200..];
		assert!(
			tail.iter().all(|s| s.unsigned_abs() < 64),
			"tail not silent"
		);
	}

	#[test]
	fn sine_decodes_with_small_error() {
		let ba = block_align_for(1024);
		let pcm = sine(16000, 440.0, 16000.0, 16000.0);
		let enc = encode_dvi4(&pcm, ba);
		let dec = decode_dvi4(&enc, ba);
		// Skip the first 64 samples while the step size adapts.
		let mut err2 = 0f64;
		let mut sig2 = 0f64;
		let mut max_err = 0i32;
		for (&a, &b) in pcm.iter().zip(&dec).skip(64) {
			let e = i32::from(a) - i32::from(b);
			max_err = max_err.max(e.abs());
			err2 += f64::from(e).powi(2);
			sig2 += f64::from(a).powi(2);
		}
		let snr_db = 10.0 * (sig2 / err2).log10();
		assert!(snr_db > 25.0, "SNR too low: {snr_db:.1} dB");
		assert!(max_err < 2000, "max error too large: {max_err}");
	}

	#[test]
	#[should_panic(expected = "block_align must exceed")]
	fn rejects_header_only_block() {
		let _ = encode_dvi4(&[0; 4], 4);
	}
}
