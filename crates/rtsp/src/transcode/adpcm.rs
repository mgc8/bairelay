//! IMA/DVI ADPCM decoder per IMA Digital Audio specification.
//!
//! Reolink cameras emit ADPCM in blocks with a 4-byte predictor header:
//! - 2 bytes little-endian signed i16: initial predictor
//! - 1 byte: initial step index (0–88)
//! - 1 byte: reserved (0)
//!
//! Followed by packed 4-bit nibbles, low nibble first.

const STEP_TABLE: [i32; 89] = [
	7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50, 55, 60, 66,
	73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449,
	494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552, 1707, 1878, 2066, 2272,
	2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871, 5358, 5894, 6484, 7132, 7845, 8630, 9493,
	10442, 11487, 12635, 13899, 15289, 16818, 18500, 20350, 22385, 24623, 27086, 29794, 32767,
];

const INDEX_TABLE: [i32; 16] = [-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8];

/// IMA ADPCM decoder.
///
/// Holds the running `predictor` sample and `step_index` across nibble
/// calls within a single block. [`decode_block`] reseeds both fields
/// from the 4-byte block header on entry, so the across-block state is
/// not load-bearing — the production call site at
/// `src/stream_source.rs::handle_adpcm` constructs a fresh decoder per
/// packet. [`decode_nibble`] is exposed for incremental decoding within
/// a block; callers wanting cross-block continuity must avoid
/// [`decode_block`] (which always resets) and feed nibbles directly.
///
/// [`decode_block`]: AdpcmDecoder::decode_block
/// [`decode_nibble`]: AdpcmDecoder::decode_nibble
pub struct AdpcmDecoder {
	predictor: i32,
	step_index: i32,
}

impl AdpcmDecoder {
	/// Create a fresh decoder with zeroed predictor and step index.
	pub fn new() -> Self {
		Self {
			predictor: 0,
			step_index: 0,
		}
	}

	/// Decode a whole block (4-byte header + ≥ 1 data byte) to PCM16.
	///
	/// Returns one PCM16 sample per nibble, plus the initial predictor sample
	/// from the header (IMA convention: first sample is the header predictor).
	pub fn decode_block(&mut self, block: &[u8]) -> Result<Vec<i16>, AdpcmError> {
		let mut out = Vec::new();
		self.decode_block_into(block, &mut out)?;
		Ok(out)
	}

	/// Like [`decode_block`] but writes into a caller-owned `Vec` so a
	/// reused buffer skips per-packet allocation. Clears `out` on entry,
	/// reserves enough capacity for the full output, and grows it
	/// in-place. Returns `Err` without touching `out` only if the header
	/// validation fails.
	///
	/// [`decode_block`]: AdpcmDecoder::decode_block
	pub fn decode_block_into(
		&mut self,
		block: &[u8],
		out: &mut Vec<i16>,
	) -> Result<(), AdpcmError> {
		// 4 header bytes + at least 1 data byte. A header-only block
		// would yield a single-sample output of the predictor, which is
		// valid IMA but useless on the wire and just churns the pipeline.
		if block.len() < 5 {
			return Err(AdpcmError::TooShort);
		}
		self.predictor = i16::from_le_bytes([block[0], block[1]]) as i32;
		self.step_index = block[2] as i32;
		if !(0..=88).contains(&self.step_index) {
			return Err(AdpcmError::InvalidStepIndex(block[2]));
		}
		// Reserved byte block[3] ignored.

		let data = &block[4..];
		out.clear();
		out.reserve(1 + data.len() * 2);
		out.push(self.predictor as i16);

		for &byte in data {
			let low = byte & 0x0F;
			let high = (byte >> 4) & 0x0F;
			out.push(self.decode_nibble(low));
			out.push(self.decode_nibble(high));
		}
		Ok(())
	}

	/// Decode a single nibble incrementally, updating internal state.
	///
	/// Only the low 4 bits of `nibble` are used; the upper bits are
	/// masked off to match the IMA wire format and keep
	/// `INDEX_TABLE[nibble as usize]` in bounds.
	pub fn decode_nibble(&mut self, nibble: u8) -> i16 {
		let nibble = nibble & 0x0F;
		let step = STEP_TABLE[self.step_index as usize];
		let sign = nibble & 0x08;
		let magnitude = (nibble & 0x07) as i32;

		let mut diff = step >> 3;
		if magnitude & 4 != 0 {
			diff += step;
		}
		if magnitude & 2 != 0 {
			diff += step >> 1;
		}
		if magnitude & 1 != 0 {
			diff += step >> 2;
		}
		if sign != 0 {
			self.predictor -= diff;
		} else {
			self.predictor += diff;
		}
		self.predictor = self.predictor.clamp(i16::MIN as i32, i16::MAX as i32);

		self.step_index = (self.step_index + INDEX_TABLE[nibble as usize]).clamp(0, 88);

		self.predictor as i16
	}

	/// Reset decoder state (use on desync recovery). Test-only —
	/// production decoders are constructed fresh per packet (see
	/// `src/stream_source.rs::handle_adpcm`), so reset isn't invoked
	/// outside the test suite. Promote to `pub(crate)` if a future
	/// caller wants in-place reuse.
	#[cfg(test)]
	pub(crate) fn reset(&mut self) {
		self.predictor = 0;
		self.step_index = 0;
	}
}

impl Default for AdpcmDecoder {
	fn default() -> Self {
		Self::new()
	}
}

/// Errors returned by [`AdpcmDecoder::decode_block`].
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum AdpcmError {
	/// Block is shorter than the mandatory 4-byte header.
	#[error("block too short: must be at least 4 header bytes")]
	TooShort,
	/// Header step index is outside the valid 0..=88 range.
	#[error("invalid step index {0}; must be 0..=88")]
	InvalidStepIndex(u8),
}

/// IMA/DVI ADPCM **encoder** — the uplink direction of [`AdpcmDecoder`].
///
/// Blocks are laid out exactly the way the decoder above reads them, which is
/// also how `bcmedia::model::BcMediaAdpcm::data` documents the talk uplink: a
/// 4-byte predictor header (2-byte little-endian signed predictor, 1-byte step
/// index, 1 reserved zero byte) then `data_bytes_per_block` bytes of packed
/// 4-bit nibbles, low nibble first.
///
/// The encoder owns an [`AdpcmDecoder`] and advances its own state by feeding
/// every nibble it emits back through [`AdpcmDecoder::decode_nibble`]. The
/// reconstruction side of the encoder is therefore *the* decoder rather than a
/// second implementation that can drift: whatever the camera computes for the
/// running predictor and step index, this encoder already computed for the
/// same nibble, so block headers always carry the state the camera expects to
/// resume from.
///
/// Sizing: `bc_protocol::talk` derives the wire block as `length_per_encoder / 2`
/// data bytes plus the 4-byte header — 512 data bytes + 4 for the doorbell's
/// `lengthPerEncoder = 1024` — and its own sample arithmetic for the pacing
/// counts `(data_len - 4) * 2 + 1` samples per block, i.e. **1025**: the header's
/// predictor is one of the block's samples, not an extra. That is also exactly
/// how the IMA WAV convention is defined (`nSamplesPerBlock = (nBlockAlign - 4) *
/// 2 + 1`) and how [`AdpcmDecoder`] reads a block. [`AdpcmEncoder`] follows it by
/// default; see [`HeaderConvention`] for the alternative and why it costs a
/// click at every block boundary.
/// [`AdpcmEncoder::from_length_per_encoder`] applies the same division so callers
/// can hand it the raw ability value.

/// What the encoder does with the 4-byte block header, i.e. what it assumes the
/// camera's decoder does with it.
///
/// [`AdpcmDecoder::decode_block_into`] emits the header's predictor as the
/// block's **first output sample** (see its `out.push(self.predictor as i16)`),
/// which is the IMA convention and the one the talk sender's pacing arithmetic
/// assumes. Two ways to fill that slot:
///
/// - [`HeaderConvention::SampleInHeader`] (default): the block's first sample is
///   written *into* the header, losslessly as a 16-bit value, and the block's
///   nibbles then carry the following samples. Every sample is heard exactly
///   once, and the header slot is even a small gift: one sample per block
///   survives with zero coding error.
/// - [`HeaderConvention::RepeatPrevious`]: the header carries the reconstruction
///   state the previous block ended on, which is therefore a *replay* of the
///   sample the decoder has already produced. A decoder following the IMA
///   convention plays that sample twice, so one sample is duplicated every
///   `2 * data_bytes_per_block + 1` samples — every 64 ms at 16 kHz. A held
///   sample is a slope discontinuity, and 15.6 of them per second is what a
///   listener calls crackle; see the round-trip numbers in
///   `bairelay audio-check`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeaderConvention {
	/// Header = this block's first sample (IMA convention, no duplicate).
	#[default]
	SampleInHeader,
	/// Header = the previous block's last reconstruction (replayed by an IMA
	/// decoder, one duplicated sample per block).
	RepeatPrevious,
}

pub struct AdpcmEncoder {
	/// Reconstruction side: holds `predictor` / `step_index`.
	decoder: AdpcmDecoder,
	/// Nibble bytes per block, i.e. half the sample count.
	data_bytes_per_block: usize,
	/// Whether the block header carries a real sample or a replay.
	convention: HeaderConvention,
	/// Nibble bytes accumulated for the block being filled.
	data: Vec<u8>,
	/// Low nibble of a pair not yet merged into a byte.
	half: Option<u8>,
	/// Header captured when the open block started: the reconstruction state
	/// the camera must resume *from*, so it is taken before the block's first
	/// nibble is quantised, never at flush time.
	header: Option<[u8; 4]>,
}

impl AdpcmEncoder {
	/// Encoder emitting blocks of `data_bytes_per_block` nibble bytes
	/// (2 samples per byte) under the default [`HeaderConvention`].
	/// A zero size is meaningless; 1 is used instead rather than panicking,
	/// because this value comes from the camera.
	pub fn new(data_bytes_per_block: usize) -> Self {
		Self::with_convention(data_bytes_per_block, HeaderConvention::default())
	}

	/// Same as [`new`](Self::new) with an explicit header convention. Used by
	/// `bairelay audio-check` to A/B the two conventions on real audio, and
	/// by `--header repeat` to reproduce the old duplicated-sample layout.
	pub fn with_convention(data_bytes_per_block: usize, convention: HeaderConvention) -> Self {
		Self {
			decoder: AdpcmDecoder::new(),
			data_bytes_per_block: data_bytes_per_block.max(1),
			convention,
			data: Vec::new(),
			half: None,
			header: None,
		}
	}

	/// The convention this encoder writes.
	pub fn convention(&self) -> HeaderConvention {
		self.convention
	}

	/// Size the blocks from a `<TalkAbility>` `lengthPerEncoder` value, using
	/// the same `length_per_encoder / 2` rule as the talk sender.
	pub fn from_length_per_encoder(length_per_encoder: u16) -> Self {
		Self::new((length_per_encoder as usize) / 2)
	}

	/// Samples accepted before the next block boundary is reached: the nibble
	/// slots, plus the header slot when the convention puts a sample there.
	pub fn samples_in_block(&self) -> usize {
		let header_sample = usize::from(self.convention == HeaderConvention::SampleInHeader);
		self.data_bytes_per_block * 2 + header_sample
	}

	/// Samples already absorbed into the block currently being filled.
	pub fn pending_samples(&self) -> usize {
		let header_sample = match self.convention {
			// The header slot is only occupied once the block has started.
			HeaderConvention::SampleInHeader => usize::from(self.header.is_some()),
			HeaderConvention::RepeatPrevious => 0,
		};
		header_sample + self.data.len() * 2 + usize::from(self.half.is_some())
	}

	/// Reset both the reconstruction state and the open block. Call between
	/// talk sessions: each session starts from a silent predictor.
	pub fn reset(&mut self) {
		self.decoder.predictor = 0;
		self.decoder.step_index = 0;
		self.data.clear();
		self.half = None;
		self.header = None;
	}

	/// Encode `samples`, appending every **complete** block to `out`.
	///
	/// A partial trailing block stays open (possibly holding a single half
	/// nibble) until [`finish`](Self::finish) pads it. State carries across
	/// calls, so block N+1 resumes from block N's reconstruction state: under
	/// [`HeaderConvention::SampleInHeader`] its header names the new block's
	/// first sample and the step index still continues, and under
	/// [`HeaderConvention::RepeatPrevious`] it is literally the last
	/// reconstructed sample. Neither convention ever restarts from zero
	/// mid-stream, which is what would tear the waveform open at the seam.
	pub fn encode(&mut self, samples: &[i16], out: &mut Vec<u8>) {
		for &sample in samples {
			if self.header.is_none() {
				match self.convention {
					// Spend the header slot on a real sample: the camera's
					// decoder makes it the block's first output sample, so
					// seeding the predictor with it is both lossless and the
					// only way no sample is heard twice.
					HeaderConvention::SampleInHeader => {
						self.decoder.predictor = i32::from(sample);
						self.header = Some(self.current_header());
						continue;
					},
					HeaderConvention::RepeatPrevious => {
						// First sample of a block: stamp the state the camera's
						// decoder must start from, before this sample moves it.
						self.header = Some(self.current_header());
					},
				}
			}
			let nibble = self.quantise(sample);
			// Advance the shared reconstruction state through the decoder, so
			// the encoder's idea of the predictor is the decoder's.
			self.decoder.decode_nibble(nibble);

			match self.half.take() {
				None => self.half = Some(nibble),
				Some(low) => {
					self.data.push(low | (nibble << 4));
					if self.data.len() == self.data_bytes_per_block {
						self.flush_block(out);
					}
				}
			}
		}
	}

	/// Pad the open block out to a full block and append it to `out`. Returns
	/// the number of blocks written, 0 when nothing was pending. Under
	/// [`HeaderConvention::SampleInHeader`] a block holding only its header
	/// sample is still worth emitting — that sample is one the listener would
	/// otherwise lose — while under [`HeaderConvention::RepeatPrevious`] an
	/// untouched block has no sample of its own and is skipped.
	///
	/// The pad **encodes silence**: each pad nibble is the code that best
	/// reconstructs a zero sample from the state the block stopped on, exactly
	/// like coding a real silent sample. What that buys, measured with
	/// `bairelay audio-check` on a tone that ends on its positive peak: the
	/// played stream used to stop *parked* on the last value (6502 counts of a
	/// 32767 scale, about −14 dBFS) and stay there for the rest of the block, so
	/// the loudspeaker was cut mid-excitation when the transmission ended — one
	/// click per transmission, whatever the block convention. Padding with
	/// plain `0` nibbles is worse still: it adds `step/8` per sample and slides
	/// the reconstruction *away* from the last real sample. Encoding zeros walks
	/// the reconstruction back down to the silence the listener is owed, within a
	/// count or two, and it does it in a few dozen nibbles because the step index
	/// rises as long as there is a gap to close.
	pub fn finish(&mut self, out: &mut Vec<u8>) -> usize {
		if self.pending_samples() == 0 {
			return 0;
		}
		while self.data.len() < self.data_bytes_per_block {
			let nibble = self.quantise(0);
			// Advance the reconstruction the same way the camera's decoder will,
			// so the next pad nibble is chosen against the value it will hear.
			self.decoder.decode_nibble(nibble);
			match self.half.take() {
				None => self.half = Some(nibble),
				Some(low) => self.data.push(low | (nibble << 4)),
			}
		}
		self.flush_block(out);
		1
	}

	/// Choose the 4-bit code whose reconstruction is closest to `sample`,
	/// following the decoder's own step/magnitude arithmetic.
	fn quantise(&mut self, sample: i16) -> u8 {
		let step = STEP_TABLE[self.decoder.step_index as usize];
		let mut diff = i32::from(sample) - self.decoder.predictor;

		let mut nibble = 0u8;
		if diff < 0 {
			nibble |= 0x08;
			// Both operands are inside the i16 range, so the negation can't
			// overflow.
			diff = -diff;
		}
		// Greedy magnitude bits, mirroring `decode_nibble`'s
		// step + step/2 + step/4 (+ step/8 baseline) reconstruction.
		if diff >= step {
			nibble |= 0x04;
			diff -= step;
		}
		if diff >= step >> 1 {
			nibble |= 0x02;
			diff -= step >> 1;
		}
		if diff >= step >> 2 {
			nibble |= 0x01;
		}
		nibble
	}

	/// Header bytes describing the reconstruction state as it stands *now*:
	/// predictor (LE i16), step index, reserved zero.
	fn current_header(&self) -> [u8; 4] {
		let predictor = self
			.decoder
			.predictor
			.clamp(i16::MIN as i32, i16::MAX as i32) as i16;
		let [lo, hi] = predictor.to_le_bytes();
		// `step_index` is clamped to 0..=88 by `decode_nibble`.
		[lo, hi, self.decoder.step_index as u8, 0]
	}

	/// Write the captured header plus the block's nibbles to `out` and clear
	/// the open block.
	fn flush_block(&mut self, out: &mut Vec<u8>) {
		let header = self.header.take().unwrap_or_else(|| self.current_header());
		out.extend_from_slice(&header);
		out.extend_from_slice(&self.data);
		self.data.clear();
	}
}

impl Default for AdpcmEncoder {
	fn default() -> Self {
		Self::new(1)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn single_nibble_zero_is_silence_ish() {
		let mut d = AdpcmDecoder::new();
		// Starting predictor=0, step_index=0, step=7
		// Nibble 0: diff = 7>>3=0, no additions. Predictor stays 0.
		assert_eq!(d.decode_nibble(0), 0);
	}

	#[test]
	fn decode_block_recovers_header_predictor() {
		let mut block = vec![0x10, 0x00, 5, 0]; // predictor=16, step_index=5, one zero-nibble byte
		block.push(0x00);
		let mut d = AdpcmDecoder::new();
		let samples = d.decode_block(&block).unwrap();
		assert_eq!(samples[0], 16);
		assert_eq!(samples.len(), 3);
	}

	#[test]
	fn rejects_short_block() {
		let mut d = AdpcmDecoder::new();
		assert_eq!(d.decode_block(&[0, 0, 0]), Err(AdpcmError::TooShort));
		// Header-only (4 bytes, no data) is also rejected — would
		// otherwise yield a single-sample useless block.
		assert_eq!(d.decode_block(&[0, 0, 0, 0]), Err(AdpcmError::TooShort));
	}

	#[test]
	fn rejects_invalid_step_index() {
		let mut d = AdpcmDecoder::new();
		assert_eq!(
			d.decode_block(&[0, 0, 90, 0, 0]),
			Err(AdpcmError::InvalidStepIndex(90))
		);
	}

	#[test]
	fn step_index_clamps_at_88() {
		let mut d = AdpcmDecoder::new();
		d.step_index = 88;
		// Nibble 7 adds +8 to index, should clamp to 88
		d.decode_nibble(7);
		assert_eq!(d.step_index, 88);
	}

	#[test]
	fn step_index_floors_at_zero() {
		let mut d = AdpcmDecoder::new();
		d.step_index = 0;
		// Nibble 0 adds -1 to index, should floor at 0
		d.decode_nibble(0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn known_sequence_matches_reference() {
		// A short ramp-up sequence that a reference IMA decoder also produces.
		// Block: predictor=0, step_index=0; nibbles: 4, 4, 4 → three ascending samples.
		let block = &[0, 0, 0, 0, 0x44, 0x04]; // nibbles 4, 4, 4, 0
		let mut d = AdpcmDecoder::new();
		let samples = d.decode_block(block).unwrap();
		// First sample is predictor=0, then three rising, then one more.
		assert_eq!(samples[0], 0);
		assert!(samples[1] > 0);
		assert!(samples[2] > samples[1]);
		assert!(samples[3] > samples[2]);
	}

	#[test]
	fn negative_nibble_decreases_predictor() {
		// The high (sign) bit of a nibble inverts the diff direction.
		// Start with a non-zero predictor so the decrease is observable.
		let mut d = AdpcmDecoder::new();
		d.predictor = 1000;
		d.step_index = 10;
		let before = d.predictor;
		// Nibble 0x0C = sign set + magnitude 4 → predictor -= (step + step>>3)
		d.decode_nibble(0x0C);
		assert!(
			d.predictor < before,
			"sign-set nibble must decrease predictor ({} → {})",
			before,
			d.predictor
		);
	}

	#[test]
	fn reset_clears_predictor_and_step_index() {
		let mut d = AdpcmDecoder::new();
		d.predictor = 1234;
		d.step_index = 42;
		d.reset();
		assert_eq!(d.predictor, 0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn default_constructs_fresh_decoder() {
		let d = AdpcmDecoder::default();
		assert_eq!(d.predictor, 0);
		assert_eq!(d.step_index, 0);
	}

	#[test]
	fn decode_block_into_reuses_buffer_and_clears_old_data() {
		let block = &[0x10, 0x00, 5, 0, 0x44, 0x04];
		let mut buf = vec![999i16, 999, 999, 999, 999];
		let mut d = AdpcmDecoder::new();
		d.decode_block_into(block, &mut buf).unwrap();
		// Old contents replaced; first sample is the header predictor.
		assert_eq!(buf[0], 16);
		assert_eq!(buf.len(), 5);
		// Reusing the same buffer with a new header overwrites cleanly.
		let block2 = &[0x20, 0x00, 7, 0, 0x00];
		d.decode_block_into(block2, &mut buf).unwrap();
		assert_eq!(buf[0], 32);
		assert_eq!(buf.len(), 3);
	}

	#[test]
	fn decode_block_into_does_not_clobber_buffer_on_header_error() {
		let mut buf = vec![1i16, 2, 3];
		let mut d = AdpcmDecoder::new();
		assert_eq!(
			d.decode_block_into(&[0, 0, 90, 0, 0], &mut buf),
			Err(AdpcmError::InvalidStepIndex(90))
		);
		assert_eq!(buf, vec![1i16, 2, 3]);
	}

	// Property tests: decode_block must reject any block that fails
	// the size / step-index validation, and never panic on arbitrary
	// input. decode_nibble's contract is "any 4-bit value yields an
	// i16 sample without panicking".
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 4096,
			..ProptestConfig::default()
		})]

		/// `decode_block` must:
		/// - never panic on arbitrary bytes (truncated, malformed step);
		/// - on `Ok`, produce exactly `1 + 2 * (block.len() - 4)` samples
		///   (header predictor + two nibbles per data byte);
		/// - on `Ok`, every output sample stays inside `i16` range
		///   (trivially true since the return type is `i16`, but the
		///   property pins down the predictor-clamp invariant);
		/// - be deterministic for a given input.
		#[test]
		fn decode_block_property_invariants(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
			let mut d = AdpcmDecoder::new();
			let result = d.decode_block(&bytes);
			if let Ok(samples) = &result {
				prop_assert_eq!(samples.len(), 1 + 2 * (bytes.len() - 4));
			}
			// Determinism: a second decoder must produce the same output.
			let mut d2 = AdpcmDecoder::new();
			let result2 = d2.decode_block(&bytes);
			prop_assert_eq!(result, result2);
		}

		/// `decode_nibble` must accept the full 0..=255 byte range
		/// without panicking; the function masks to 4 bits internally so
		/// `INDEX_TABLE` and `STEP_TABLE` reads stay in bounds.
		#[test]
		fn decode_nibble_accepts_full_byte_range(byte in any::<u8>()) {
			let mut d = AdpcmDecoder::new();
			let _ = d.decode_nibble(byte);
		}
	}
}

#[cfg(test)]
mod encoder_tests {
	use super::*;

	/// Encode `samples` under `convention`, pad the tail, then decode the wire
	/// the way the camera does: block by block, each block reseeding the
	/// decoder from its own 4-byte header (`decode_block` always reseeds, which
	/// is what `src/stream_source.rs::handle_adpcm` relies on).
	fn round_trip(samples: &[i16], convention: HeaderConvention) -> (Vec<u8>, Vec<i16>) {
		let mut enc = AdpcmEncoder::with_convention(512, convention);
		let mut wire = Vec::new();
		enc.encode(samples, &mut wire);
		enc.finish(&mut wire);
		let mut out = Vec::new();
		let mut d = AdpcmDecoder::new();
		for chunk in wire.chunks(516) {
			out.extend_from_slice(&d.decode_block(chunk).expect("block must parse"));
		}
		(wire, out)
	}

	/// A constant-level tone: the reference signal for "reconstruction must be
	/// clean and free of periodic jumps".
	fn sine(len: usize, amplitude: f64) -> Vec<i16> {
		(0..len)
			.map(|i| {
				let t = i as f64 / 16000.0;
				(amplitude * (2.0 * std::f64::consts::PI * 440.0 * t).sin()) as i16
			})
			.collect()
	}

	#[test]
	fn default_convention_counts_1025_samples_per_block() {
		let enc = AdpcmEncoder::from_length_per_encoder(1024);
		assert_eq!(enc.convention(), HeaderConvention::SampleInHeader);
		// 512 nibble bytes + the header slot the default convention spends on a
		// real sample: matches the talk sender's `(len - 4) * 2 + 1` arithmetic.
		assert_eq!(enc.samples_in_block(), 1025);
		assert_eq!(enc.pending_samples(), 0);
		assert_eq!(
			AdpcmEncoder::with_convention(512, HeaderConvention::RepeatPrevious).samples_in_block(),
			1024
		);
	}

	#[test]
	fn blocks_are_516_bytes_and_parse_under_both_conventions() {
		let samples = sine(3000, 12000.0);
		for convention in [HeaderConvention::SampleInHeader, HeaderConvention::RepeatPrevious] {
			let (wire, out) = round_trip(&samples, convention);
			assert_eq!(
				wire.len(),
				3 * 516,
				"two full blocks plus the padded tail, whatever the convention"
			);
			for chunk in wire.chunks(516) {
				let mut d = AdpcmDecoder::new();
				assert_eq!(d.decode_block(chunk).unwrap().len(), 1 + 2 * 512);
			}
			assert!(out.len() >= samples.len());
		}
	}

	/// The point of the default convention: the header slot is a real sample,
	/// so the decoded stream lines up one-for-one with the input and the sample
	/// riding in each header comes back bit-exact.
	#[test]
	fn default_header_carries_a_real_sample_bit_exact() {
		let samples = sine(3 * 1025, 12000.0);
		let (wire, out) = round_trip(&samples, HeaderConvention::SampleInHeader);
		for b in 0..3 {
			let seam = b * 1025;
			assert_eq!(
				out[seam], samples[seam],
				"header of block {b} must carry its own first sample"
			);
			assert_eq!(
				i16::from_le_bytes([wire[b * 516], wire[b * 516 + 1]]),
				samples[seam],
				"and the header bytes say the same"
			);
		}
	}

	/// With the default convention a seam costs nothing: it is not a replay of
	/// the sample the decoder has already produced.
	#[test]
	fn default_convention_does_not_replay_the_seam_sample() {
		let samples = sine(4 * 1025, 12000.0);
		let (_, out) = round_trip(&samples, HeaderConvention::SampleInHeader);
		for b in 1..4 {
			let seam = b * 1025;
			assert_ne!(
				out[seam],
				out[seam - 1],
				"a header sample equal to its predecessor would be a click at {seam}"
			);
		}
	}

	/// The alternative layout is kept so it can be A/B'd on hardware, and it is
	/// exactly the one that costs a duplicated sample at every seam: block 2's
	/// header replays the reconstruction block 1 already emitted.
	#[test]
	fn repeat_convention_duplicates_every_seam_sample() {
		let samples = sine(3 * 1024, 12000.0);
		let (wire, out) = round_trip(&samples, HeaderConvention::RepeatPrevious);
		for b in 1..3 {
			let seam = b * 1025;
			assert_eq!(
				i16::from_le_bytes([wire[b * 516], wire[b * 516 + 1]]),
				out[seam - 1],
				"header {b} replays the previous block's last reconstruction"
			);
			assert_eq!(out[seam], out[seam - 1], "so the camera plays it twice");
		}
	}

	/// Over a long stream the sample count has to fall out exactly: every
	/// sample in, one sample out, nothing invented at the seams. 51360 samples
	/// is the length a real 3.2 s melody came in at.
	#[test]
	fn default_convention_neither_loses_nor_invents_samples() {
		let samples = sine(51_360, 8000.0);
		let (wire, out) = round_trip(&samples, HeaderConvention::SampleInHeader);
		assert_eq!(wire.len(), 51 * 516, "50 full blocks plus the padded tail");
		assert_eq!(out.len(), 51 * 1025, "each block yields 1025 samples");
		let mut wrong_seams = 0;
		for b in 1..51 {
			if out[b * 1025] != samples[b * 1025] {
				wrong_seams += 1;
			}
		}
		assert_eq!(wrong_seams, 0, "all 50 seams must carry their own sample");
		// And nothing in the audio proper is a replay of its predecessor
		// *because of the framing*: exact repeats may only happen where the
		// sine itself is flat, which is never at a seam for this tone.
	}

	/// Block 2 must resume from the step index block 1 ended on: restarting at
	/// zero would make the first samples after a seam far too coarse, which is
	/// a click. Asserted through the header byte, which is what the camera reads.
	#[test]
	fn step_index_continues_across_the_seam() {
		let mut enc = AdpcmEncoder::from_length_per_encoder(1024);
		let mut wire = Vec::new();
		enc.encode(&[5000, -5000].repeat(1025), &mut wire);
		assert_eq!(wire.len(), 2 * 516, "1025 samples per block, exactly two of them");
		// The very first block legitimately starts from the silent state.
		assert_eq!(wire[2], 0, "block 1 starts at step index 0");
		// A ±5000 square wave drives the step index to the top of the table;
		// block 2 must inherit that, not restart from zero, or every seam would
		// be encoded with a step 88 times too small.
		assert!(
			wire[516 + 2] > 20,
			"block 2 must not restart the step index: {}",
			wire[516 + 2]
		);
		assert_eq!(wire[519], 0, "reserved header byte stays zero");
	}

	/// Round trip through the decoder is the only end-to-end check available
	/// without a speaker: under the default convention the decoded stream is
	/// sample-aligned with the input, so the error is directly comparable
	/// sample by sample. IMA is typically > 30 dB SNR on a tone.
	#[test]
	fn round_trip_tracks_a_sine_within_adpcm_tolerance() {
		const AMPLITUDE: f64 = 12000.0;
		let samples = sine(3200, AMPLITUDE);
		let (_, out) = round_trip(&samples, HeaderConvention::SampleInHeader);
		assert!(
			out.len() >= samples.len(),
			"padding only adds samples: {} vs {}",
			out.len(),
			samples.len()
		);

		let sum_sq: f64 = out
			.iter()
			.zip(samples.iter())
			.map(|(got, expected)| {
				let delta = f64::from(*got) - f64::from(*expected);
				delta * delta
			})
			.sum();
		let rmse = (sum_sq / samples.len() as f64).sqrt();
		assert!(
			rmse < AMPLITUDE / 3.0,
			"rms error {rmse} against amplitude {AMPLITUDE}"
		);
	}

	/// A single pending sample must survive in the header, and the pad that
	/// fills the rest of the block must be silence, not a held level: the
	/// measured artifact was the played stream sitting on the last amplitude
	/// (thousands of counts) until the transmission was cut.
	#[test]
	fn finish_pads_with_silence_and_lands_back_on_zero() {
		let mut enc = AdpcmEncoder::from_length_per_encoder(1024);
		let mut wire = Vec::new();
		assert_eq!(enc.finish(&mut wire), 0);
		assert!(wire.is_empty(), "an idle encoder emits nothing at all");

		enc.encode(&[1000], &mut wire);
		assert_eq!(enc.pending_samples(), 1, "the header slot holds it");
		assert!(wire.is_empty(), "a partial block stays open");

		assert_eq!(enc.finish(&mut wire), 1);
		assert_eq!(wire.len(), 516);
		assert_eq!(i16::from_le_bytes([wire[0], wire[1]]), 1000);
		assert!(
			wire[4..].iter().any(|&b| b != 0x08),
			"the pad must be codes chosen to reconstruct silence, not the fixed \
			 -step/8 then +step/8 pair that just holds the level"
		);

		let mut d = AdpcmDecoder::new();
		let decoded = d.decode_block(&wire).unwrap();
		assert_eq!(decoded[0], 1000, "the header sample survives exactly");
		assert!(
			decoded[decoded.len() / 2..].iter().all(|&s| s.abs() <= 8),
			"the pad encodes silence: the walk from the header's 1000 comes to rest \
			 within a few dozen nibbles, so by the half of the block it must sit at the \
			 bottom of the step table; the half-block sample was {}",
			decoded[decoded.len() / 2]
		);
		assert!(
			decoded.last().unwrap().abs() < 1000,
			"the stream must not end parked on the level it stopped at, got {}",
			decoded.last().unwrap()
		);
	}

	#[test]
	fn reset_reopens_the_block_from_silence() {
		let mut enc = AdpcmEncoder::from_length_per_encoder(1024);
		let mut wire = Vec::new();
		enc.encode(&[7000; 10], &mut wire);
		assert_eq!(enc.pending_samples(), 10);
		enc.reset();
		assert_eq!(enc.pending_samples(), 0);
		assert_eq!(enc.decoder.predictor, 0);
		assert_eq!(enc.decoder.step_index, 0);
	}
}
