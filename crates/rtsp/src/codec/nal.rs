//! Annex-B NAL unit splitting and codec detection.

use super::VideoCodec;

/// Split an Annex-B byte stream into NAL unit bodies.
///
/// Start codes are `0x000001` or `0x00000001`. The returned slices contain
/// the NAL unit body (without start codes, including the NAL header byte(s)).
pub fn split_annex_b(stream: &[u8]) -> Vec<&[u8]> {
	let mut nals = Vec::new();
	let mut i = 0;
	let mut nal_start: Option<usize> = None;

	while i + 2 < stream.len() {
		// Look for 0x000001 or 0x00000001
		let three_byte = stream[i] == 0 && stream[i + 1] == 0 && stream[i + 2] == 1;
		let four_byte = i + 3 < stream.len()
			&& stream[i] == 0
			&& stream[i + 1] == 0
			&& stream[i + 2] == 0
			&& stream[i + 3] == 1;
		if four_byte {
			if let Some(start) = nal_start {
				if i > start {
					nals.push(&stream[start..i]);
				}
			}
			nal_start = Some(i + 4);
			i += 4;
		} else if three_byte {
			if let Some(start) = nal_start {
				if i > start {
					nals.push(&stream[start..i]);
				}
			}
			nal_start = Some(i + 3);
			i += 3;
		} else {
			i += 1;
		}
	}
	if let Some(start) = nal_start {
		if start < stream.len() {
			nals.push(&stream[start..]);
		}
	}
	nals
}

/// H.264 NAL unit types we recognise.
///
/// Marker struct plus a `from_header_byte` helper and named constants for
/// the NAL types we act on in later code. Kept symmetric with
/// `H265NalType`.
pub struct H264NalType;

impl H264NalType {
	/// H.264 NAL header is 1 byte. Type is the low 5 bits.
	pub fn from_header_byte(b: u8) -> u8 {
		b & 0x1F
	}

	/// Coded slice of a non-IDR picture (P or B frame body).
	pub const NON_IDR_SLICE: u8 = 1;
	/// Coded slice of an IDR picture (keyframe).
	pub const IDR_SLICE: u8 = 5;
	/// Supplemental Enhancement Information — ignored by bairelay.
	pub const SEI: u8 = 6;
	/// Sequence Parameter Set.
	pub const SPS: u8 = 7;
	/// Picture Parameter Set.
	pub const PPS: u8 = 8;
	/// Access Unit Delimiter.
	pub const AUD: u8 = 9;
}

/// H.265 NAL unit types we recognise.
pub struct H265NalType;

impl H265NalType {
	/// H.265 NAL header is 2 bytes. Type is bits 1–6 of the first byte.
	pub fn from_header_byte(b: u8) -> u8 {
		(b >> 1) & 0x3F
	}

	/// Video Parameter Set.
	pub const VPS: u8 = 32;
	/// Sequence Parameter Set.
	pub const SPS: u8 = 33;
	/// Picture Parameter Set.
	pub const PPS: u8 = 34;
	/// Access Unit Delimiter.
	pub const AUD: u8 = 35;
	// IRAP slices 19–23 are keyframes.
	/// Broken Link Access slice with leading pictures (IRAP keyframe).
	pub const BLA_W_LP: u8 = 16;
	/// IDR slice with Random Access Decodable Leading pictures (keyframe).
	pub const IDR_W_RADL: u8 = 19;
	/// IDR slice with no leading pictures (keyframe).
	pub const IDR_N_LP: u8 = 20;
	/// Clean Random Access slice (keyframe, may have leading pictures).
	pub const CRA: u8 = 21;
}

/// Returns true when `nal` is a standard, single-layer NAL unit that
/// downstream decoders (ffmpeg, mpv, VLC, gstreamer, HA's stream
/// component) are expected to handle.
///
/// Reolink firmware emits NAL types outside the standard whitelist as
/// proprietary metadata (HEVC type 62 is the one we've seen on Argus).
/// ffmpeg's RTP-HEVC depacketizer rejects these with `Unsupported (HEVC)
/// NAL type (62)` per RFC 7798 §4.4 and may also log
/// `Multi-layer HEVC coding is not implemented` when `nuh_layer_id != 0`.
/// Forwarding them disrupts the depacketizer enough to manifest as
/// `Could not find ref with POC N` / `Skipping invalid undecodable NALU`
/// in mpv and as visible spinning / breakup in HA's stream component.
/// The official Reolink app's proprietary decoder ignores them silently.
///
/// Whitelist (drop everything else, plus empty NALs):
/// - **H.264**: nal_unit_type in 1..=13 (VCL slices 1..=5, SEI 6,
///   SPS 7, PPS 8, AUD 9, EOS 10, EOB 11, FD 12, SPS-ext 13). Drops
///   type 0 (undefined) and 14..=31 (auxiliary / extension / reserved).
/// - **H.265**: nal_unit_type in {0..=9, 16..=21, 32..=40} and
///   nuh_layer_id == 0. Keeps standard VCL trailing/TSA/STSA/RADL/RASL
///   (0..=9), IRAP keyframes (16..=21), VPS/SPS/PPS/AUD/EOS/EOB/FD/SEI
///   (32..=40). Drops reserved 10..=15 and 22..=31, reserved non-VCL
///   41..=47, and unspecified 48..=63 (which is where Reolink's
///   proprietary type-62 NALs land).
pub fn is_decodable_nal(nal: &[u8], codec: VideoCodec) -> bool {
	if nal.is_empty() {
		tracing::trace!(?codec, "is_decodable_nal: dropping empty NAL");
		return false;
	}
	match codec {
		VideoCodec::H264 => {
			let ty = H264NalType::from_header_byte(nal[0]);
			let ok = (1..=13).contains(&ty);
			if !ok {
				// Per-NAL hot path: trace level only, filtered out in
				// production. Surfaces under `RUST_LOG=trace` so a
				// future-firmware NAL-type shift can be diagnosed
				// without bisecting the codebase.
				tracing::trace!(
					codec = "h264",
					nal_type = ty,
					len = nal.len(),
					"is_decodable_nal: dropping H.264 NAL outside type range 1..=13"
				);
			}
			ok
		}
		VideoCodec::H265 => {
			if nal.len() < 2 {
				tracing::trace!(
					codec = "h265",
					len = nal.len(),
					"is_decodable_nal: dropping short H.265 NAL (need ≥2 bytes for layer/tid)"
				);
				return false;
			}
			let ty = H265NalType::from_header_byte(nal[0]);
			let type_ok = matches!(ty, 0..=9 | 16..=21 | 32..=40);
			if !type_ok {
				tracing::trace!(
					codec = "h265",
					nal_type = ty,
					len = nal.len(),
					"is_decodable_nal: dropping H.265 NAL outside type ranges 0..=9 | 16..=21 | 32..=40"
				);
				return false;
			}
			// nuh_layer_id is 6 bits split across the two-byte NAL header:
			// bit5 is byte0[0] (lsb of byte 0), bits4..0 are byte1[7..3].
			let layer_id = ((nal[0] & 0x01) << 5) | (nal[1] >> 3);
			if layer_id != 0 {
				tracing::trace!(
					codec = "h265",
					nal_type = ty,
					layer_id,
					"is_decodable_nal: dropping multi-layer H.265 NAL (nuh_layer_id != 0)"
				);
			}
			layer_id == 0
		}
	}
}

/// Detect video codec from the first NAL's header.
///
/// Uses heuristics: H.264 parameter-set NAL types 7/8 and IDR 5 fall in
/// 0–31; H.265 parameter-set types 32/33/34 exceed 31. For ambiguous
/// values (1–21), we inspect the second byte: H.265 NAL headers have a
/// `nuh_layer_id`/`nuh_temporal_id_plus1` byte following, with the
/// low 3 bits always `>= 1` (temporal_id is 0..=6, plus1 >= 1); H.264
/// has no such constraint. When uncertain we default to H.264.
pub fn detect_codec(nal: &[u8]) -> Option<VideoCodec> {
	if nal.is_empty() {
		return None;
	}
	let byte0 = nal[0];
	// H.264 forbidden-zero bit must be 0; same for H.265. Both share that.
	if byte0 & 0x80 != 0 {
		return None;
	}
	let h264_type = H264NalType::from_header_byte(byte0);
	let h265_type = H265NalType::from_header_byte(byte0);

	// Strong H.264 indicators
	if matches!(h264_type, 7 | 8) && h265_type < 32 {
		return Some(VideoCodec::H264);
	}
	// Strong H.265 indicators: any H.265 non-VCL type (VPS=32, SPS=33,
	// PPS=34, AUD=35, EOS=36, EOB=37, FD=38, SEI prefix=39, SEI suffix=40).
	// Require the H.265 2-byte header's nuh_temporal_id_plus1 >= 1 to
	// distinguish from H.264 slice bytes in the same range (e.g. 0x41 is
	// an H.264 non-IDR slice but also maps to H.265 type 32 by naive
	// extraction). Restricting to 32..=34 misclassifies an H.265 access
	// unit led by AUD (byte 0x46) as H.264 because its h264_type is 6
	// (SEI), which doesn't trigger any strong-indicator branch.
	if matches!(h265_type, 32..=40) && nal.len() >= 2 && (nal[1] & 0x07) >= 1 {
		return Some(VideoCodec::H265);
	}
	// IDR slices: H.264 = 5, H.265 = 19–21.
	if h264_type == 5 && !(19..=21).contains(&h265_type) {
		return Some(VideoCodec::H264);
	}
	if (19..=21).contains(&h265_type) && nal.len() >= 2 {
		// H.265 has a second header byte; layer_id+temporal_id_plus1.
		let tid_plus1 = nal[1] & 0x07;
		if tid_plus1 >= 1 {
			return Some(VideoCodec::H265);
		}
	}
	// Fallback: assume H.264.
	Some(VideoCodec::H264)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn split_single_nal_three_byte_start() {
		let stream = &[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f];
		let nals = split_annex_b(stream);
		assert_eq!(nals.len(), 1);
		assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1f]);
	}

	#[test]
	fn split_single_nal_four_byte_start() {
		let stream = &[0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f];
		let nals = split_annex_b(stream);
		assert_eq!(nals.len(), 1);
		assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1f]);
	}

	#[test]
	fn split_two_nals_mixed_start_codes() {
		// 4-byte, SPS, 3-byte, PPS
		let stream = &[
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, 0x00, 0x00, 0x01, 0x68, 0xce, 0x38,
			0x80,
		];
		let nals = split_annex_b(stream);
		assert_eq!(nals.len(), 2);
		assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1f]);
		assert_eq!(nals[1], &[0x68, 0xce, 0x38, 0x80]);
	}

	#[test]
	fn split_empty() {
		assert!(split_annex_b(&[]).is_empty());
	}

	#[test]
	fn h264_nal_type_extraction() {
		// SPS (type 7)
		assert_eq!(H264NalType::from_header_byte(0x67), 7);
		// PPS (type 8)
		assert_eq!(H264NalType::from_header_byte(0x68), 8);
		// IDR (type 5)
		assert_eq!(H264NalType::from_header_byte(0x65), 5);
		// Non-IDR (type 1)
		assert_eq!(H264NalType::from_header_byte(0x41), 1);
	}

	#[test]
	fn h265_nal_type_extraction() {
		// VPS (type 32): byte = 0x40
		assert_eq!(H265NalType::from_header_byte(0x40), 32);
		// SPS (type 33): byte = 0x42
		assert_eq!(H265NalType::from_header_byte(0x42), 33);
		// PPS (type 34): byte = 0x44
		assert_eq!(H265NalType::from_header_byte(0x44), 34);
		// IDR_W_RADL (type 19): byte = 0x26
		assert_eq!(H265NalType::from_header_byte(0x26), 19);
	}

	#[test]
	fn detect_h264_from_sps() {
		assert_eq!(
			detect_codec(&[0x67, 0x42, 0x00, 0x1f]),
			Some(VideoCodec::H264)
		);
	}

	#[test]
	fn detect_h265_from_vps() {
		assert_eq!(
			detect_codec(&[0x40, 0x01, 0x0c, 0x01]),
			Some(VideoCodec::H265)
		);
	}

	#[test]
	fn detect_h265_from_idr_with_layer_byte() {
		// IDR_W_RADL (19) with layer_id=0, temporal_id_plus1=1 → second byte 0x01
		assert_eq!(detect_codec(&[0x26, 0x01]), Some(VideoCodec::H265));
	}

	#[test]
	fn detect_rejects_forbidden_zero_bit() {
		assert_eq!(detect_codec(&[0x80]), None);
	}

	#[test]
	fn split_skips_empty_nal_for_back_to_back_start_codes() {
		// Two start codes with nothing between → must NOT emit an empty NAL.
		let stream = &[0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x67, 0x42];
		let nals = split_annex_b(stream);
		assert_eq!(nals.len(), 1);
		assert_eq!(nals[0], &[0x67, 0x42]);
	}

	#[test]
	fn split_no_start_code_returns_empty() {
		let stream = &[0xAA, 0xBB, 0xCC];
		assert!(split_annex_b(stream).is_empty());
	}

	#[test]
	fn split_tail_only_start_code_returns_empty() {
		// A start code at the very end with no body should not emit a NAL.
		let stream = &[0x00, 0x00, 0x01];
		assert!(split_annex_b(stream).is_empty());
	}

	#[test]
	fn detect_does_not_misclassify_h264_slice_as_h265() {
		// 0x41 is H.264 non-IDR slice (nal_ref_idc=2, type=1). Its naive H.265
		// mapping is (0x41 >> 1) & 0x3F = 32, which would collide with H.265
		// VPS. Without the 2-byte layer check, detect_codec used to return H.265;
		// we now require nal[1] & 0x07 >= 1 for H.265 detection, so a second
		// byte whose low 3 bits are 0 (e.g. 0x98) falls through to H.264.
		assert_eq!(
			detect_codec(&[0x41, 0x98, 0x00, 0x00]),
			Some(VideoCodec::H264)
		);
	}

	#[test]
	fn detect_h264_nal_type_constants_match_raw_values() {
		// Documents that constants match the standard values.
		assert_eq!(H264NalType::NON_IDR_SLICE, 1);
		assert_eq!(H264NalType::IDR_SLICE, 5);
		assert_eq!(H264NalType::SEI, 6);
		assert_eq!(H264NalType::SPS, 7);
		assert_eq!(H264NalType::PPS, 8);
		assert_eq!(H264NalType::AUD, 9);
	}

	#[test]
	fn detect_codec_rejects_empty_slice() {
		assert_eq!(detect_codec(&[]), None);
	}

	#[test]
	fn detect_codec_rejects_forbidden_zero_bit() {
		// Any NAL header byte with the top bit set must be rejected —
		// both H.264 and H.265 require `forbidden_zero_bit == 0`.
		assert_eq!(detect_codec(&[0x80, 0x00]), None);
		assert_eq!(detect_codec(&[0xFF]), None);
	}

	#[test]
	fn detect_codec_h264_idr_slice() {
		// 0x65 = nal_ref_idc=3, type=5 (IDR). Naive H.265 type = 18,
		// so the `h265_type in 19..=21` branch does not apply and the
		// `h264_type == 5` branch returns H.264.
		assert_eq!(detect_codec(&[0x65, 0x00]), Some(VideoCodec::H264));
	}

	#[test]
	fn detect_codec_h265_idr_slice_with_valid_second_byte() {
		// Pick a header whose H.265 naive type is in 19..=21 AND whose
		// second byte has low 3 bits >= 1 so the H.265 branch fires.
		// 0x26 >> 1 = 19 → H.265 IDR_W_RADL; second byte 0x01 → tid+1 = 1.
		assert_eq!(detect_codec(&[0x26, 0x01]), Some(VideoCodec::H265));
	}

	#[test]
	fn detect_codec_falls_through_to_h264_on_ambiguous() {
		// 0x01 = type 1 on both codecs; no strong indicator → H.264
		// default path.
		assert_eq!(detect_codec(&[0x01, 0x00]), Some(VideoCodec::H264));
	}

	#[test]
	fn detect_codec_h264_sps_with_low_nal_ref_idc() {
		// 0x07: nal_ref_idc=0, type=7 (SPS). Naive H.265 type = 3 which
		// is < 32, so the `matches!(h264_type, 7 | 8)` strong-indicator
		// branch fires and returns H.264.
		assert_eq!(detect_codec(&[0x07, 0x00]), Some(VideoCodec::H264));
	}

	#[test]
	fn detect_codec_h265_aud_led_access_unit() {
		// AUD = H.265 type 35; byte0 = (35 << 1) = 0x46. Naive H.264
		// type = 6 (SEI), which does not match the strong-H.264-indicator
		// branch (matches 7 | 8). The strong-H.265-indicator branch must
		// span the full non-VCL range (32..=40) so an AUD-led access unit
		// classifies as H.265 instead of falling through to the default.
		assert_eq!(detect_codec(&[0x46, 0x01]), Some(VideoCodec::H265));
	}

	#[test]
	fn detect_codec_h265_sei_prefix_led_access_unit() {
		// H.265 SEI prefix = type 39; byte0 = 0x4E. h264_type = 14
		// (Prefix NAL — extension), no strong H.264 match.
		assert_eq!(detect_codec(&[0x4E, 0x01]), Some(VideoCodec::H265));
	}

	#[test]
	fn detect_codec_h264_pps_with_low_nal_ref_idc() {
		// 0x08: nal_ref_idc=0, type=8 (PPS). Naive H.265 type = 4 < 32.
		assert_eq!(detect_codec(&[0x08, 0x00]), Some(VideoCodec::H264));
	}

	#[test]
	fn is_decodable_nal_drops_empty() {
		assert!(!is_decodable_nal(&[], VideoCodec::H264));
		assert!(!is_decodable_nal(&[], VideoCodec::H265));
	}

	#[test]
	fn is_decodable_nal_h264_accepts_standard_types() {
		// Non-IDR slice (1), IDR slice (5), SEI (6), SPS (7), PPS (8), AUD (9).
		assert!(is_decodable_nal(&[0x41, 0x00], VideoCodec::H264));
		assert!(is_decodable_nal(&[0x65, 0x00], VideoCodec::H264));
		assert!(is_decodable_nal(&[0x06, 0x00], VideoCodec::H264));
		assert!(is_decodable_nal(&[0x67, 0x00], VideoCodec::H264));
		assert!(is_decodable_nal(&[0x68, 0x00], VideoCodec::H264));
		assert!(is_decodable_nal(&[0x09, 0x00], VideoCodec::H264));
	}

	#[test]
	fn is_decodable_nal_h264_drops_undefined_and_extension_types() {
		// Type 0 (undefined).
		assert!(!is_decodable_nal(&[0x00, 0x00], VideoCodec::H264));
		// Type 14 (prefix NAL, MVC/SVC extension) — drop.
		assert!(!is_decodable_nal(&[0x0E, 0x00], VideoCodec::H264));
		// Type 24 (STAP-A — RTP packetization, never in Annex-B) — drop.
		assert!(!is_decodable_nal(&[0x18, 0x00], VideoCodec::H264));
		// Type 31 — drop.
		assert!(!is_decodable_nal(&[0x1F, 0x00], VideoCodec::H264));
	}

	#[test]
	fn is_decodable_nal_h265_accepts_vcl_irap_and_non_vcl() {
		// TRAIL_N (0), TRAIL_R (1), all standard VCL up to 9 (RASL_R).
		assert!(is_decodable_nal(&[0x00, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x02, 0x01], VideoCodec::H265));
		// IDR_W_RADL (19), IDR_N_LP (20), CRA (21), BLA_W_LP (16).
		assert!(is_decodable_nal(&[0x26, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x28, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x2A, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x20, 0x01], VideoCodec::H265));
		// VPS (32), SPS (33), PPS (34), AUD (35), SEI prefix (39),
		// SEI suffix (40).
		assert!(is_decodable_nal(&[0x40, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x42, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x44, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x46, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x4E, 0x01], VideoCodec::H265));
		assert!(is_decodable_nal(&[0x50, 0x01], VideoCodec::H265));
	}

	#[test]
	fn is_decodable_nal_h265_drops_unspec62_argus_payload() {
		// Type 62 = UNSPEC62, the Reolink Argus proprietary metadata NAL
		// that ffmpeg logs as `Unsupported (HEVC) NAL type (62)`.
		// byte0 = F(0) | type(62=0b111110) | layer_id_msb(0) = 0b01111100 = 0x7C.
		assert!(!is_decodable_nal(&[0x7C, 0x01], VideoCodec::H265));
		// Same type with layer_id_msb=1 (byte0 = 0x7D) — also dropped.
		assert!(!is_decodable_nal(&[0x7D, 0x09], VideoCodec::H265));
	}

	#[test]
	fn is_decodable_nal_h265_drops_reserved_ranges() {
		// Reserved VCL 10..=15.
		assert!(!is_decodable_nal(&[0x14, 0x01], VideoCodec::H265)); // type 10
		assert!(!is_decodable_nal(&[0x1E, 0x01], VideoCodec::H265)); // type 15
																	 // Reserved VCL 22..=31.
		assert!(!is_decodable_nal(&[0x2C, 0x01], VideoCodec::H265)); // type 22
		assert!(!is_decodable_nal(&[0x3E, 0x01], VideoCodec::H265)); // type 31
																	 // Reserved non-VCL 41..=47.
		assert!(!is_decodable_nal(&[0x52, 0x01], VideoCodec::H265)); // type 41
		assert!(!is_decodable_nal(&[0x5E, 0x01], VideoCodec::H265)); // type 47
																	 // Unspecified 48..=63 (48=AP, 49=FU in RTP, but in Annex-B
																	 // they're proprietary application data — drop them all).
		assert!(!is_decodable_nal(&[0x60, 0x01], VideoCodec::H265)); // type 48
		assert!(!is_decodable_nal(&[0x7E, 0x01], VideoCodec::H265)); // type 63
	}

	#[test]
	fn is_decodable_nal_h265_drops_every_unspec_type_48_through_63() {
		// Sweep the entire UNSPEC range — Reolink's proprietary
		// metadata sits at 62, but new firmwares could ship 48..=63
		// values we'd want to drop on principle. Layer = 0, tid+1 = 1.
		for ty in 48u8..=63 {
			let byte0 = ty << 1; // forbidden_zero=0, layer_msb=0
			let byte1 = 0x01; // layer_lsb=0, tid+1=1
			assert!(
				!is_decodable_nal(&[byte0, byte1], VideoCodec::H265),
				"H.265 UNSPEC type {ty} must be rejected"
			);
		}
	}

	#[test]
	fn is_decodable_nal_h265_drops_every_reserved_vcl_22_through_31() {
		for ty in 22u8..=31 {
			let byte0 = ty << 1;
			let byte1 = 0x01;
			assert!(
				!is_decodable_nal(&[byte0, byte1], VideoCodec::H265),
				"H.265 reserved VCL type {ty} must be rejected"
			);
		}
	}

	#[test]
	fn is_decodable_nal_h265_drops_every_reserved_non_vcl_41_through_47() {
		for ty in 41u8..=47 {
			let byte0 = ty << 1;
			let byte1 = 0x01;
			assert!(
				!is_decodable_nal(&[byte0, byte1], VideoCodec::H265),
				"H.265 reserved non-VCL type {ty} must be rejected"
			);
		}
	}

	#[test]
	fn is_decodable_nal_h265_drops_multi_layer() {
		// Standard IDR_W_RADL (type 19) with nuh_layer_id == 1 — multi-
		// layer extension, dropped per the layer_id == 0 guard.
		// byte0 = type<<1 | layer_msb = 0x26 | 0x01 = 0x27.
		// byte1 = layer_lsb(00000) << 3 | tid_plus1(1) = 0x01.
		// layer_id = (0x27 & 0x01)<<5 | (0x01>>3) = 0x20 | 0 = 32 ≠ 0.
		assert!(!is_decodable_nal(&[0x27, 0x01], VideoCodec::H265));
		// Standard SPS with non-zero layer (e.g. layer 1).
		// byte0 = 0x42 (SPS), byte1 = 0x09 = layer_lsb(00001) << 3 | tid+1(1).
		// layer_id = 0<<5 | 1 = 1.
		assert!(!is_decodable_nal(&[0x42, 0x09], VideoCodec::H265));
	}

	#[test]
	fn is_decodable_nal_h265_requires_two_byte_header() {
		// One-byte H.265 NAL has no layer/TID byte; reject conservatively.
		assert!(!is_decodable_nal(&[0x40], VideoCodec::H265));
	}

	#[test]
	fn split_two_nals_with_four_byte_start_codes() {
		// Covers the four_byte branch where nal_start is already Some —
		// two consecutive 0x00000001 start codes with content between.
		let stream = &[
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x00, 0x00, 0x01, 0x68, 0xce,
		];
		let nals = split_annex_b(stream);
		assert_eq!(nals.len(), 2);
		assert_eq!(nals[0], &[0x67, 0x42]);
		assert_eq!(nals[1], &[0x68, 0xce]);
	}

	// Property tests: detect_codec / is_decodable_nal / split_annex_b
	// must accept any byte slice without panicking. Camera firmware
	// variation + lossy upstreams produce odd NAL shapes; ffmpeg
	// downstream must not see a server panic.
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 256,
			..ProptestConfig::default()
		})]

		#[test]
		fn detect_codec_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
			let _ = detect_codec(&bytes);
		}

		#[test]
		fn is_decodable_nal_never_panics_h264(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
			let _ = is_decodable_nal(&bytes, VideoCodec::H264);
		}

		#[test]
		fn is_decodable_nal_never_panics_h265(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
			let _ = is_decodable_nal(&bytes, VideoCodec::H265);
		}

		#[test]
		fn split_annex_b_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
			let _ = split_annex_b(&bytes);
		}
	}
}
