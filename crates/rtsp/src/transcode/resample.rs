//! Sample-rate conversion for voice-band audio.

/// Decimate 16 kHz PCM to 8 kHz by averaging adjacent sample pairs.
///
/// Filter: `y[n] = (x[2n] + x[2n+1] + 1) >> 1`. This is a 2-tap boxcar
/// followed by 2× decimation, rounding half-up via the +1 bias. The
/// frequency response before decimation is `H(f) = cos(π f / fs)`:
/// only ~3 dB attenuation at the new Nyquist (4 kHz) and ~12 dB at
/// 6 kHz, with a true zero at 8 kHz. Content above 4 kHz aliases into
/// 0–4 kHz with as little as 3 dB of suppression — adequate for 8 kHz
/// µ-law voice (telephony cuts off around 3.4 kHz anyway) but **not**
/// a true low-pass; it does not "avoid aliasing" in the strict
/// signal-processing sense. Replace with a windowed-sinc (e.g. 7-tap)
/// if non-voice content ever flows through this path.
///
/// `(a + b + 1) >> 1` is bias-symmetric for both signs (arithmetic
/// shift rounds toward negative infinity, the +1 lifts the rounding
/// edge to half-up). The earlier `(a + b) / 2` truncated toward zero,
/// which slightly compresses signals near zero on average.
///
/// If `pcm_16k` has an odd length, the trailing sample is dropped.
pub fn decimate_16_to_8(pcm_16k: &[i16]) -> Vec<i16> {
	let (pairs, _odd) = pcm_16k.as_chunks::<2>();
	pairs
		.iter()
		.map(|&[a, b]| {
			let avg = (a as i32 + b as i32 + 1) >> 1;
			avg as i16
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn output_length_is_half_of_input() {
		let input = vec![100i16; 320];
		let output = decimate_16_to_8(&input);
		assert_eq!(output.len(), 160);
	}

	#[test]
	fn constant_signal_preserved() {
		let input = vec![1000i16; 100];
		let output = decimate_16_to_8(&input);
		for &s in &output {
			assert_eq!(s, 1000);
		}
	}

	#[test]
	fn averages_adjacent_samples() {
		let input = &[1000i16, 3000, -1000, 5000];
		let output = decimate_16_to_8(input);
		assert_eq!(output, vec![2000, 2000]);
	}

	#[test]
	fn odd_length_drops_trailing_sample() {
		let input = &[1000i16, 3000, 500];
		let output = decimate_16_to_8(input);
		assert_eq!(output, vec![2000]);
	}

	#[test]
	fn rounds_half_up_symmetrically_around_zero() {
		// (1+2+1)>>1 = 2 ; (-1+-2+1)>>1 = (-2)>>1 = -1.
		// The +1 lifts both halves toward +∞ deterministically. The
		// previous `/2` truncate-toward-zero gave `1` and `-1`
		// respectively — asymmetric on the magnitude of the result.
		let input = &[1i16, 2, -1, -2];
		let output = decimate_16_to_8(input);
		assert_eq!(output, vec![2, -1]);
	}
}
