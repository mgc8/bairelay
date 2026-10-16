use super::model::*;
use crate::Error;
use bytes::{Buf, BytesMut};
use nom::{
	bytes::streaming::take,
	combinator::*,
	error::{context, ContextError, ErrorKind, ParseError},
	number::streaming::*,
	Err,
};

type IResult<I, O, E = nom::error::VerboseError<I>> = Result<(I, O), nom::Err<E>>;

/// Build a typed nom error for the dispatch fall-through arms below.
/// Replaces `unreachable!()` so a future copy-paste drift between
/// `verify` predicates and `match` arms surfaces as a parse error
/// rather than a panic.
fn make_dispatch_error<I, E>(input: I, ctx: &'static str) -> nom::Err<E>
where
	I: Copy,
	E: ContextError<I> + ParseError<I>,
{
	Err::Failure(E::add_context(
		input,
		ctx,
		E::from_error_kind(input, ErrorKind::Switch),
	))
}

// PAD_SIZE: Media packets use 8 byte padding
const PAD_SIZE: u32 = 8;

impl BcMedia {
	/// Parse a single [`BcMedia`] variant from the front of `buf`.
	///
	/// Consumes one on-wire Baichuan media frame: a little-endian
	/// 32-bit magic header, the per-variant fixed header, the
	/// payload, and any zero-byte padding used to round the payload
	/// up to an 8-byte boundary. Each variant of [`BcMedia`] has its
	/// own magic header; consult the variant's description for the
	/// wire-format details. On success `buf` is advanced past the
	/// parsed frame (i.e. by the total wire length of that frame).
	///
	/// If `buf` does not yet contain a complete frame the call returns
	/// `Err(Error::NomIncomplete(..))`; `buf` is left untouched and
	/// the caller should buffer more bytes and retry. Other `Err`
	/// variants indicate a malformed or unrecognised frame.
	///
	/// # Known wire-format caveat (ADPCM)
	///
	/// Replayers should feed raw camera-captured ADPCM bytes into this
	/// function, not bytes produced by [`BcMedia::serialize`]. The
	/// serializer pads based on `data.len() % 8` while this parser
	/// pads based on `(data.len() + 4) % 8` (the 4-byte sub-header is
	/// counted in the wire `payload_size`), so a loop that reads from
	/// a concatenation of our own serialized ADPCM frames will desync.
	/// Tracked for future upstream alignment.
	///
	/// # Examples
	///
	/// Streaming-loop over a framed byte buffer:
	///
	/// ```no_run
	/// use bytes::BytesMut;
	/// use bairelay_neolink_core::bcmedia::model::BcMedia;
	/// use bairelay_neolink_core::Error;
	///
	/// # fn main() -> Result<(), Error> {
	/// let mut buf = BytesMut::new();
	/// // ... fill `buf` from a file or socket ...
	/// loop {
	///     match BcMedia::deserialize(&mut buf) {
	///         Ok(_frame) => { /* handle the frame */ }
	///         Err(Error::NomIncomplete(_)) => break, // need more bytes
	///         Err(e) => return Err(e),
	///     }
	/// }
	/// # Ok(()) }
	/// ```
	pub fn deserialize(buf: &mut BytesMut) -> Result<BcMedia, Error> {
		let (result, len) = match consumed(bcmedia)(buf) {
			Ok((_, (parsed_buff, result))) => Ok((result, parsed_buff.len())),
			Err(e) => Err(e),
		}?;
		buf.advance(len);
		Ok(result)
	}
}

fn bcmedia(buf: &[u8]) -> IResult<&[u8], BcMedia> {
	let (buf, magic) = context(
		"Failed to match any known bcmedia",
		verify(le_u32, |x| {
			matches!(
				*x,
				MAGIC_HEADER_BCMEDIA_INFO_V1
					| MAGIC_HEADER_BCMEDIA_INFO_V2
					| MAGIC_HEADER_BCMEDIA_IFRAME..=MAGIC_HEADER_BCMEDIA_IFRAME_LAST
					| MAGIC_HEADER_BCMEDIA_PFRAME..=MAGIC_HEADER_BCMEDIA_PFRAME_LAST
					| MAGIC_HEADER_BCMEDIA_AAC
					| MAGIC_HEADER_BCMEDIA_ADPCM
			)
		}),
	)(buf)?;

	match magic {
		MAGIC_HEADER_BCMEDIA_INFO_V1 => {
			let (buf, payload) = bcmedia_info_v1(buf)?;
			Ok((buf, BcMedia::InfoV1(payload)))
		}
		MAGIC_HEADER_BCMEDIA_INFO_V2 => {
			let (buf, payload) = bcmedia_info_v2(buf)?;
			Ok((buf, BcMedia::InfoV2(payload)))
		}
		MAGIC_HEADER_BCMEDIA_IFRAME..=MAGIC_HEADER_BCMEDIA_IFRAME_LAST => {
			let (buf, payload) = bcmedia_iframe(buf)?;
			Ok((buf, BcMedia::Iframe(payload)))
		}
		MAGIC_HEADER_BCMEDIA_PFRAME..=MAGIC_HEADER_BCMEDIA_PFRAME_LAST => {
			let (buf, payload) = bcmedia_pframe(buf)?;
			Ok((buf, BcMedia::Pframe(payload)))
		}
		MAGIC_HEADER_BCMEDIA_AAC => {
			let (buf, payload) = bcmedia_aac(buf)?;
			Ok((buf, BcMedia::Aac(payload)))
		}
		MAGIC_HEADER_BCMEDIA_ADPCM => {
			let (buf, payload) = bcmedia_adpcm(buf)?;
			Ok((buf, BcMedia::Adpcm(payload)))
		}
		_ => Err(make_dispatch_error(
			buf,
			"BcMedia magic dispatch mismatch (verify and match diverged)",
		)),
	}
}

fn bcmedia_info_v1(buf: &[u8]) -> IResult<&[u8], BcMediaInfoV1> {
	let (buf, _header_size) = context(
		"Header size mismatch in BCMedia InfoV1",
		verify(le_u32, |x| *x == 32),
	)(buf)?;
	let (buf, video_width) = le_u32(buf)?;
	let (buf, video_height) = le_u32(buf)?;
	let (buf, _unknown) = le_u8(buf)?;
	let (buf, fps) = le_u8(buf)?;
	let (buf, start_year) = le_u8(buf)?;
	let (buf, start_month) = le_u8(buf)?;
	let (buf, start_day) = le_u8(buf)?;
	let (buf, start_hour) = le_u8(buf)?;
	let (buf, start_min) = le_u8(buf)?;
	let (buf, start_seconds) = le_u8(buf)?;
	let (buf, end_year) = le_u8(buf)?;
	let (buf, end_month) = le_u8(buf)?;
	let (buf, end_day) = le_u8(buf)?;
	let (buf, end_hour) = le_u8(buf)?;
	let (buf, end_min) = le_u8(buf)?;
	let (buf, end_seconds) = le_u8(buf)?;
	let (buf, _unknown_b) = le_u16(buf)?;

	Ok((
		buf,
		BcMediaInfoV1 {
			// header_size,
			video_width,
			video_height,
			fps,
			start_year,
			start_month,
			start_day,
			start_hour,
			start_min,
			start_seconds,
			end_year,
			end_month,
			end_day,
			end_hour,
			end_min,
			end_seconds,
		},
	))
}

fn bcmedia_info_v2(buf: &[u8]) -> IResult<&[u8], BcMediaInfoV2> {
	let (buf, _header_size) = context(
		"Failed to match headersize in BCMedia Info V2",
		verify(le_u32, |x| *x == 32),
	)(buf)?;
	let (buf, video_width) = le_u32(buf)?;
	let (buf, video_height) = le_u32(buf)?;
	let (buf, _unknown) = le_u8(buf)?;
	let (buf, fps) = le_u8(buf)?;
	let (buf, start_year) = le_u8(buf)?;
	let (buf, start_month) = le_u8(buf)?;
	let (buf, start_day) = le_u8(buf)?;
	let (buf, start_hour) = le_u8(buf)?;
	let (buf, start_min) = le_u8(buf)?;
	let (buf, start_seconds) = le_u8(buf)?;
	let (buf, end_year) = le_u8(buf)?;
	let (buf, end_month) = le_u8(buf)?;
	let (buf, end_day) = le_u8(buf)?;
	let (buf, end_hour) = le_u8(buf)?;
	let (buf, end_min) = le_u8(buf)?;
	let (buf, end_seconds) = le_u8(buf)?;
	let (buf, _unknown_b) = le_u16(buf)?;

	Ok((
		buf,
		BcMediaInfoV2 {
			// header_size,
			video_width,
			video_height,
			fps,
			start_year,
			start_month,
			start_day,
			start_hour,
			start_min,
			start_seconds,
			end_year,
			end_month,
			end_day,
			end_hour,
			end_min,
			end_seconds,
		},
	))
}

fn take4(buf: &[u8]) -> IResult<&[u8], &str> {
	map_res(nom::bytes::streaming::take(4usize), |r| {
		std::str::from_utf8(r)
	})(buf)
}

fn bcmedia_iframe(buf: &[u8]) -> IResult<&[u8], BcMediaIframe> {
	let (buf, video_type_str) = context(
		"Video Type is unrecognised in IFrame",
		verify(take4, |x| matches!(x, "H264" | "H265")),
	)(buf)?;
	let (buf, payload_size) = le_u32(buf)?;
	let (buf, additional_header_size) = le_u32(buf)?;
	let (buf, microseconds) = le_u32(buf)?;
	let (buf, _unknown_b) = le_u32(buf)?;
	let (buf, time) = if additional_header_size >= 4 {
		let (buf, time_value) = le_u32(buf)?;
		(buf, Some(time_value))
	} else {
		(buf, None)
	};
	let (buf, _unknown_remained) = if additional_header_size > 4 {
		let remainder = additional_header_size - 4;
		let (buf, unknown_remained) = take(remainder)(buf)?;
		(buf, Some(unknown_remained))
	} else {
		(buf, None)
	};

	let (buf, data_slice) = take(payload_size)(buf)?;
	let pad_size = match payload_size % PAD_SIZE {
		0 => 0,
		n => PAD_SIZE - n,
	};
	let (buf, _padding) = take(pad_size)(buf)?;
	// `take(payload_size)` already guarantees `data_slice.len() ==
	// payload_size`. The check is dead today; demoted from `assert_eq!`
	// to `debug_assert_eq!` so a future combinator swap (streaming →
	// non-streaming or custom `take`) doesn't turn the invariant into
	// a panic vector reachable from network input.
	debug_assert_eq!(payload_size as usize, data_slice.len());

	let video_type = match video_type_str {
		"H264" => VideoType::H264,
		"H265" => VideoType::H265,
		_ => {
			return Err(make_dispatch_error(
				buf,
				"BcMedia video_type_str mismatch (verify and match diverged)",
			));
		}
	};

	Ok((
		buf,
		BcMediaIframe {
			video_type,
			// payload_size,
			microseconds,
			time,
			data: data_slice.to_vec(),
		},
	))
}

fn bcmedia_pframe(buf: &[u8]) -> IResult<&[u8], BcMediaPframe> {
	let (buf, video_type_str) = context(
		"Video Type is unrecognised in PFrame",
		verify(take4, |x| matches!(x, "H264" | "H265")),
	)(buf)?;
	let (buf, payload_size) = le_u32(buf)?;
	let (buf, additional_header_size) = le_u32(buf)?;
	let (buf, microseconds) = le_u32(buf)?;
	let (buf, _unknown_b) = le_u32(buf)?;
	let (buf, _additional_header) = take(additional_header_size)(buf)?;
	let (buf, data_slice) = take(payload_size)(buf)?;
	let pad_size = match payload_size % PAD_SIZE {
		0 => 0,
		n => PAD_SIZE - n,
	};
	let (buf, _padding) = take(pad_size)(buf)?;
	// `take(payload_size)` already guarantees `data_slice.len() ==
	// payload_size`. The check is dead today; demoted from `assert_eq!`
	// to `debug_assert_eq!` so a future combinator swap (streaming →
	// non-streaming or custom `take`) doesn't turn the invariant into
	// a panic vector reachable from network input.
	debug_assert_eq!(payload_size as usize, data_slice.len());

	let video_type = match video_type_str {
		"H264" => VideoType::H264,
		"H265" => VideoType::H265,
		_ => {
			return Err(make_dispatch_error(
				buf,
				"BcMedia video_type_str mismatch (verify and match diverged)",
			));
		}
	};

	Ok((
		buf,
		BcMediaPframe {
			video_type,
			// payload_size,
			microseconds,
			data: data_slice.to_vec(),
		},
	))
}

fn bcmedia_aac(buf: &[u8]) -> IResult<&[u8], BcMediaAac> {
	let (buf, payload_size) = le_u16(buf)?;
	let (buf, _payload_size_b) = le_u16(buf)?;
	let (buf, data_slice) = take(payload_size)(buf)?;
	let pad_size = match payload_size as u32 % PAD_SIZE {
		0 => 0,
		n => PAD_SIZE - n,
	};
	let (buf, _padding) = take(pad_size)(buf)?;

	Ok((
		buf,
		BcMediaAac {
			// payload_size,
			data: data_slice.to_vec(),
		},
	))
}

fn bcmedia_adpcm(buf: &[u8]) -> IResult<&[u8], BcMediaAdpcm> {
	const SUB_HEADER_SIZE: u16 = 4;

	// `payload_size` is a 4-byte sub-header (magic + half_block_size)
	// followed by the ADPCM payload. A peer sending `payload_size < 4`
	// underflows the u16 subtraction below — debug-build panic, release
	// wraps to ~65 KiB which then drives `take()` into Incomplete and
	// the bcmedia codec's strict mode bubbles the error. Reject
	// explicitly with a typed parse error.
	let (buf, payload_size) = context(
		"ADPCM payload_size shorter than sub-header",
		verify(le_u16, |&n| n >= SUB_HEADER_SIZE),
	)(buf)?;
	let (buf, _payload_size_b) = le_u16(buf)?;
	let (buf, _magic) = context(
		"ADPCM data magic value is invalid",
		verify(le_u16, |x| *x == MAGIC_HEADER_BCMEDIA_ADPCM_DATA),
	)(buf)?;
	// On some camera this value is just 2
	// On other cameras is half the block size without the header
	let (buf, _half_block_size) = le_u16(buf)?;
	let block_size = payload_size - SUB_HEADER_SIZE;
	let (buf, data_slice) = take(block_size)(buf)?;
	let pad_size = match payload_size as u32 % PAD_SIZE {
		0 => 0,
		n => PAD_SIZE - n,
	};
	let (buf, _padding) = take(pad_size)(buf)?;

	Ok((
		buf,
		BcMediaAdpcm {
			// payload_size,
			// block_size,
			data: data_slice.to_vec(),
		},
	))
}

#[cfg(test)]
mod tests {
	use super::Error;
	use crate::bcmedia::model::*;
	use bytes::BytesMut;
	use log::*;
	use std::io::ErrorKind;

	fn init() {
		crate::test_logs::init();
	}

	#[test]
	// This method will test the decoding on swann cameras output
	//
	// Crucially this contains adpcm
	fn test_swan_deser() {
		init();

		let sample = [
			include_bytes!("samples/video_stream_swan_00.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_01.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_02.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_03.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_04.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_05.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_06.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_07.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_08.raw").as_ref(),
			include_bytes!("samples/video_stream_swan_09.raw").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);

		// Should derealise all of this
		loop {
			let e = BcMedia::deserialize(&mut buf);
			match e {
				Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
					// Reach end of files
					break;
				}
				Err(Error::NomIncomplete(_)) if buf.is_empty() => {
					// EOF still (but parser looking for next magic)
					break;
				}
				Err(e) => {
					error!("{:?}", e);
					panic!();
				}
				Ok(_) => {}
			}
		}
	}

	#[test]
	// This method will test the decoding of argus2 cameras output
	//
	// This packet has an extended iframe
	fn test_argus2_iframe_extended() {
		init();

		let sample = [
			include_bytes!("samples/argus2_iframe_0.raw").as_ref(),
			include_bytes!("samples/argus2_iframe_1.raw").as_ref(),
			include_bytes!("samples/argus2_iframe_2.raw").as_ref(),
			include_bytes!("samples/argus2_iframe_3.raw").as_ref(),
			include_bytes!("samples/argus2_iframe_4.raw").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);
		// Should derealise all of this
		loop {
			let e = BcMedia::deserialize(&mut buf);
			match e {
				Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
					// Reach end of files
					break;
				}
				Err(Error::NomIncomplete(_)) if buf.is_empty() => {
					// EOF still (but parser looking for next magic)
					break;
				}
				Err(e) => {
					error!("{:?}", e);
					panic!();
				}
				Ok(_) => {}
			}
		}
	}

	#[test]
	// This method will test the decoding of argus2 cameras output
	//
	// This packet has an extended pframe
	fn test_argus2_pframe_extended() {
		init();

		let sample = [
			include_bytes!("samples/argus2_pframe_0.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_1.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_2.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_3.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_4.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_5.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_6.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_7.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_8.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_9.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_10.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_11.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_12.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_13.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_14.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_15.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_16.raw").as_ref(),
			include_bytes!("samples/argus2_pframe_17.raw").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);

		// Should derealise all of this
		loop {
			let e = BcMedia::deserialize(&mut buf);
			match e {
				Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
					// Reach end of files
					break;
				}
				Err(Error::NomIncomplete(_)) if buf.is_empty() => {
					// EOF still (but parser looking for next magic)
					break;
				}
				Err(e) => {
					error!("{:?}", e);
					panic!();
				}
				Ok(_) => {}
			}
		}
	}

	#[test]
	// Tests the decoding of an info v1
	fn test_info_v1() {
		init();

		let sample = include_bytes!("samples/info_v1.raw");

		let mut buf = BytesMut::from(&sample[..]);

		let e = BcMedia::deserialize(&mut buf);
		assert!(matches!(
			e,
			Ok(BcMedia::InfoV1(BcMediaInfoV1 {
				video_width: 2560,
				video_height: 1440,
				fps: 30,
				start_year: 121,
				start_month: 8,
				start_day: 4,
				start_hour: 23,
				start_min: 23,
				start_seconds: 52,
				end_year: 121,
				end_month: 8,
				end_day: 4,
				end_hour: 23,
				end_min: 23,
				end_seconds: 52,
			}))
		));
	}

	#[test]
	fn test_iframe() {
		init();

		let sample = [
			include_bytes!("samples/iframe_0.raw").as_ref(),
			include_bytes!("samples/iframe_1.raw").as_ref(),
			include_bytes!("samples/iframe_2.raw").as_ref(),
			include_bytes!("samples/iframe_3.raw").as_ref(),
			include_bytes!("samples/iframe_4.raw").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);

		let e = BcMedia::deserialize(&mut buf);
		if let Ok(BcMedia::Iframe(BcMediaIframe {
			video_type: VideoType::H264,
			microseconds: 3557705112,
			time: Some(1628085232),
			data: d,
		})) = e
		{
			assert_eq!(d.len(), 192881);
		} else {
			panic!();
		}
	}

	#[test]
	fn test_pframe() {
		init();

		let sample = [
			include_bytes!("samples/pframe_0.raw").as_ref(),
			include_bytes!("samples/pframe_1.raw").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);

		let e = BcMedia::deserialize(&mut buf);
		if let Ok(BcMedia::Pframe(BcMediaPframe {
			video_type: VideoType::H264,
			microseconds: 3557767112,
			data: d,
		})) = e
		{
			assert_eq!(d.len(), 45108);
		} else {
			panic!();
		}
	}

	#[test]
	fn test_adpcm() {
		init();

		let sample = include_bytes!("samples/adpcm_0.raw");
		let mut buf = BytesMut::from(&sample[..]);

		let e = BcMedia::deserialize(&mut buf);
		if let Ok(BcMedia::Adpcm(BcMediaAdpcm { data: d })) = e {
			assert_eq!(d.len(), 244);
		} else {
			panic!();
		}
	}

	// Property tests: exercise the deserialiser with arbitrary byte
	// soup. The contract under test is "deserialize never panics and
	// never hangs — it returns Ok or Err for every input". Camera
	// firmware drift, lossy upstreams, and hostile peers can all
	// produce unexpected bytes; the parser must absorb them safely.
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			// Untrusted-input attack surface — every camera packet flows
			// through this parser. 1024 cases per property still runs in
			// well under a second on a modern host.
			cases: 1024,
			..ProptestConfig::default()
		})]

		#[test]
		fn deserialize_never_panics_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
			let mut buf = BytesMut::from(&bytes[..]);
			// We don't care which arm hits — only that no panic / no
			// infinite loop. nom's streaming combinators surface
			// "need more bytes" as Err; truly malformed input also
			// surfaces as Err. Everything else parses to Ok.
			let _ = BcMedia::deserialize(&mut buf);
		}

		#[test]
		fn deserialize_with_valid_magic_prefix_never_panics(
			magic_idx in 0u8..6,
			tail in proptest::collection::vec(any::<u8>(), 0..2048),
		) {
			// Bias the input toward "looks like a real header": pick
			// one of the 6 valid magics, prepend it, then random tail.
			// This walks the parser deeper into the per-variant
			// branches than uniform random would.
			const MAGICS: [u32; 6] = [
				MAGIC_HEADER_BCMEDIA_INFO_V1,
				MAGIC_HEADER_BCMEDIA_INFO_V2,
				MAGIC_HEADER_BCMEDIA_IFRAME,
				MAGIC_HEADER_BCMEDIA_PFRAME,
				MAGIC_HEADER_BCMEDIA_AAC,
				MAGIC_HEADER_BCMEDIA_ADPCM,
			];
			let mut bytes = MAGICS[magic_idx as usize].to_le_bytes().to_vec();
			bytes.extend_from_slice(&tail);
			let mut buf = BytesMut::from(&bytes[..]);
			let _ = BcMedia::deserialize(&mut buf);
		}
	}

	#[test]
	// Hostile peer sends an ADPCM frame with `payload_size < SUB_HEADER_SIZE`.
	// Pre-fix the u16 subtraction underflowed (debug panic / release wrap
	// to ~65 KiB which then drove `take()` into Incomplete). Now the
	// `verify` rejects at parse time.
	fn adpcm_payload_size_below_sub_header_rejected() {
		// MAGIC_HEADER_BCMEDIA_ADPCM (4 bytes) + payload_size = 3 (u16)
		// + payload_size_b (u16) — short of legal minimum.
		let mut buf = Vec::new();
		buf.extend_from_slice(&MAGIC_HEADER_BCMEDIA_ADPCM.to_le_bytes());
		buf.extend_from_slice(&3u16.to_le_bytes()); // payload_size = 3 (< 4)
		buf.extend_from_slice(&0u16.to_le_bytes()); // payload_size_b
		buf.extend_from_slice(&MAGIC_HEADER_BCMEDIA_ADPCM_DATA.to_le_bytes()); // magic
		buf.extend_from_slice(&0u16.to_le_bytes()); // half_block_size
		let mut bm = BytesMut::from(&buf[..]);
		let result = BcMedia::deserialize(&mut bm);
		assert!(
			result.is_err(),
			"ADPCM payload_size below sub-header must reject, got {result:?}"
		);
	}
}
