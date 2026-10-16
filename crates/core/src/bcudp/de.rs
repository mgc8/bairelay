use super::{crc::calc_crc, model::*, xml::*, xml_crypto::decrypt};
use crate::Error;
use bytes::{Buf, BytesMut};
use nom::{
	bytes::streaming::take,
	combinator::*,
	error::{context as error_context, ContextError, ErrorKind, ParseError},
	number::streaming::*,
	Err,
};

/// Upper bound on any BcUdp variant's `payload_size` field. UDP itself
/// is bounded by IPv4's 64 KiB datagram limit, so any wire field above
/// this is by definition crafted. Without the cap, `take(payload_size)`
/// returns `Incomplete` and the BcUdp framer keeps growing its read
/// buffer toward 4 GiB before failing — a single hostile UDP packet on
/// the wake server's public ports drives memory exhaustion.
const MAX_BCUDP_PAYLOAD: u32 = 65_535;

type IResult<I, O, E = nom::error::VerboseError<I>> = Result<(I, O), nom::Err<E>>;

fn make_error<I, E>(input: I, ctx: &'static str, kind: ErrorKind) -> E
where
	I: std::marker::Copy,
	E: ContextError<I> + ParseError<I>,
{
	E::add_context(input, ctx, E::from_error_kind(input, kind))
}

impl BcUdp {
	/// Parse a single BcUdp packet from `buf`, advancing past the parsed bytes.
	pub fn deserialize(buf: &mut BytesMut) -> Result<BcUdp, Error> {
		const TYPICAL_HEADER: usize = 20;
		let (result, len) = match consumed(bcudp)(buf) {
			Ok((_, (parsed_buff, result))) => Ok((result, parsed_buff.len())),
			Err(e) => Err(e),
		}?;
		buf.advance(len);
		buf.reserve(len + TYPICAL_HEADER); // Preallocate for future buffer calls
		Ok(result)
	}
}

fn bcudp(buf: &[u8]) -> IResult<&[u8], BcUdp> {
	let (buf, magic) = error_context(
		"Magic is invalid",
		verify(le_u32, |x| {
			matches!(
				*x,
				MAGIC_HEADER_UDP_NEGO | MAGIC_HEADER_UDP_ACK | MAGIC_HEADER_UDP_DATA
			)
		}),
	)(buf)?;

	match magic {
		MAGIC_HEADER_UDP_NEGO => {
			let (buf, payload) = udp_disc(buf)?;
			Ok((buf, BcUdp::Discovery(payload)))
		}
		MAGIC_HEADER_UDP_ACK => {
			let (buf, payload) = udp_ack(buf)?;
			Ok((buf, BcUdp::Ack(payload)))
		}
		MAGIC_HEADER_UDP_DATA => {
			let (buf, payload) = udp_data(buf)?;
			Ok((buf, BcUdp::Data(payload)))
		}
		_ => Err(Err::Failure(make_error(
			buf,
			"BcUdp magic dispatch mismatch (verify and match diverged)",
			ErrorKind::Switch,
		))),
	}
}

fn udp_disc(buf: &[u8]) -> IResult<&[u8], UdpDiscovery> {
	let (buf, payload_size) = error_context(
		"DISC: Missing payload size or exceeds cap",
		verify(le_u32, |&n| n <= MAX_BCUDP_PAYLOAD),
	)(buf)?;
	let (buf, _unknown_a) = error_context(
		"DISC: Unable to verify UnknowA",
		verify(le_u32, |&x| x == 1),
	)(buf)?;
	let (buf, tid) = error_context("DISC: Missing TID", le_u32)(buf)?;
	let (buf, checksum) = error_context("DISC: Missing checksum", le_u32)(buf)?;
	let (buf, enc_data_slice) = take(payload_size)(buf)?;

	let actual_checksum = calc_crc(enc_data_slice);
	if checksum != actual_checksum {
		// Bad checksum — could be a corrupted in-transit packet, an
		// unrelated UDP packet that happened to share our magic header,
		// or a hostile probe on the wake-server's public ports. Reject
		// without panicking; the wake-server tasks would otherwise die
		// and a single crafted packet would be a service-killing DoS.
		// Logged at debug only — public UDP ports see junk routinely.
		log::debug!(
			"BcUdp Discovery: CRC mismatch (got {:#x}, expected {:#x}); dropping",
			checksum,
			actual_checksum,
		);
		return Err(Err::Failure(make_error(
			buf,
			"DISC: CRC mismatch",
			ErrorKind::Verify,
		)));
	}

	let decrypted_payload = decrypt(tid, enc_data_slice);
	let payload = UdpXml::try_parse(decrypted_payload.as_slice()).map_err(|e| {
		// Unknown XML variants and parse errors arrive routinely on a
		// public UDP port (other Reolink products on the LAN, scanners,
		// firmware variants we have not catalogued). Log at debug only;
		// `error!` here flooded operator logs with one block per heartbeat
		// during live-verify.
		log::debug!(
			"BcUdp Discovery: unable to decode UDPXml; payload={:?} err={:?}",
			std::str::from_utf8(&decrypted_payload),
			e,
		);
		Err::Error(make_error(
			buf,
			"DISC: Unable to decode UDPXml",
			ErrorKind::MapRes,
		))
	})?;

	let data = UdpDiscovery { tid, payload };
	Ok((buf, data))
}

fn udp_ack(buf: &[u8]) -> IResult<&[u8], UdpAck> {
	let (buf, connection_id) = error_context("ACK: Missing connect ID", le_i32)(buf)?;
	let (buf, _unknown_a) =
		error_context("ACK: Unable to verify UnknowA", verify(le_u32, |&x| x == 0))(buf)?;
	let (buf, group_id) = error_context(
		"ACK: Unable to verify UnknowB",
		verify(le_u32, |&x| x == 0 || x == 0xffffffff),
	)(buf)?;
	let (buf, packet_id) = error_context("Missing packet_id", le_u32)(buf)?; // This is the point at which the camera has contigious
																		  // packets to
	let (buf, maybe_latency) = error_context("ACK: Missing Maybe Latency", le_u32)(buf)?;
	let (buf, payload_size) = error_context(
		"ACK: Missing payload_size or exceeds cap",
		verify(le_u32, |&n| n <= MAX_BCUDP_PAYLOAD),
	)(buf)?;
	let (buf, payload) = if payload_size > 0 {
		let (buf, t_payload) = take(payload_size)(buf)?; // It is a binary payload of
												   // `00 01 01 01 01 00 01`
												   // This is a truth map of missing packets
												   // since last contigious packet_id up
												   // to the last packet we sent and it received
		(buf, t_payload.to_vec())
	} else {
		(buf, vec![])
	};

	let data = UdpAck {
		connection_id,
		packet_id,
		group_id,
		maybe_latency,
		payload,
	};
	Ok((buf, data))
}

fn udp_data(buf: &[u8]) -> IResult<&[u8], UdpData> {
	let (buf, connection_id) = error_context("DATA: Missing connection_id", le_i32)(buf)?;
	let (buf, _unknown_a) =
		error_context("DATA: Unable to verify UnownA", verify(le_u32, |&x| x == 0))(buf)?;
	let (buf, packet_id) = error_context("DATA: Missing packet_id", le_u32)(buf)?;
	let (buf, payload_size) = error_context(
		"DATA: Missing payload_size or exceeds cap",
		verify(le_u32, |&n| n <= MAX_BCUDP_PAYLOAD),
	)(buf)?;
	let (buf, payload) = take(payload_size)(buf)?;

	let data = UdpData {
		connection_id,
		packet_id,
		payload: payload.to_vec(),
	};
	Ok((buf, data))
}

#[cfg(test)]
mod tests {
	use super::Error;
	use crate::bcudp::model::*;
	use crate::bcudp::xml::*;
	use assert_matches::assert_matches;
	use bytes::BytesMut;
	use log::*;
	use std::io::ErrorKind;

	fn init() {
		crate::test_logs::init();
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a discovery xml
	fn test_nego_disconnect() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_disc.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Discovery(UdpDiscovery {
				tid: 96,
				payload: UdpXml::C2dDisc(C2dDisc {
					cid: 82000,
					did: 80,
				}),
			}))
		);
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Camera Transmission xml
	fn test_nego_cam_transmission() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_camt.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Discovery(UdpDiscovery {
				tid: 113,
				payload: UdpXml::D2cT(D2cT {
						sid: 62098713,
						conn: conn_str,
						cid: 82001,
						did: 96,
					}),
			})) if &conn_str == "local"
		);
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Client Transmission xml
	fn test_nego_client_transmission() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_clientt.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Discovery(UdpDiscovery {
				tid: 1101,
				payload: UdpXml::C2dT(C2dT {
						sid: 62098713,
						conn: conn_str,
						cid: 82001,
						mtu: 1350,
					}),
			})) if &conn_str == "local"
		);
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Camera CFM xml
	fn test_nego_cfm() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_camcfm.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Discovery(UdpDiscovery {
				tid: 1101,
				payload: UdpXml::D2cCfm(D2cCfm {
						sid: 62098713,
						conn: conn_str,
						rsp: 0,
						cid: 82001,
						did: 96,
						time_r: Some(0),
					}),
			})) if &conn_str == "local"
		);
	}

	#[test]
	// Tests the decoding of an acknoledge packet
	fn test_ack() {
		init();

		let sample = include_bytes!("samples/udp_ack.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Ack(UdpAck {
				connection_id: 80,
				packet_id: 2439,
				..
			}))
		);
	}

	#[test]
	// Tests the decoding of an data packet
	fn test_data() {
		init();

		let sample = include_bytes!("samples/udp_data.bin");

		let e = BcUdp::deserialize(&mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(BcUdp::Data(UdpData {
				connection_id: 82000,
				packet_id: 2439,
				payload: payload_data
			})) if payload_data.len() == 1176
		);
	}

	#[test]
	// Craft a BcUdp Discovery packet with a CRC that doesn't match the
	// payload bytes. Must reject as `NomError` rather than panic.
	// Pre-fix this hit `assert_eq!(checksum, actual_checksum)` and
	// crashed the wake-server task — a single hostile UDP packet was
	// enough to kill both listeners.
	fn test_disc_bad_crc_returns_err_no_panic() {
		use crate::bcudp::xml_crypto::encrypt;

		let tid: u32 = 123;
		let plaintext = b"<P><dummy/></P>";
		let encrypted = encrypt(tid, plaintext);
		let bad_checksum: u32 = 0xDEADBEEF; // intentionally not the real CRC

		let mut wire = Vec::new();
		wire.extend_from_slice(&MAGIC_HEADER_UDP_NEGO.to_le_bytes());
		wire.extend_from_slice(&(encrypted.len() as u32).to_le_bytes());
		wire.extend_from_slice(&1u32.to_le_bytes()); // unknown_a
		wire.extend_from_slice(&tid.to_le_bytes());
		wire.extend_from_slice(&bad_checksum.to_le_bytes());
		wire.extend_from_slice(&encrypted);

		let mut buf = BytesMut::from(wire.as_slice());
		let result = BcUdp::deserialize(&mut buf);
		assert!(
			matches!(result, Err(Error::NomError(_))),
			"expected NomError for bad CRC, got {result:?}"
		);
	}

	#[test]
	// Craft a BcUdp Discovery packet whose decrypted payload is not
	// valid XML. The parser's map_res-error arm (`make_error` +
	// "DISC: Unable to decode UDPXml") must fire.
	fn test_disc_bad_xml_returns_err() {
		use crate::bcudp::crc::calc_crc;
		use crate::bcudp::xml_crypto::encrypt;

		let tid: u32 = 123;
		// Plaintext that is not XML at all — parser should reject.
		let plaintext = b"<<<not-xml-garbage>>>";
		let encrypted = encrypt(tid, plaintext);
		let checksum = calc_crc(&encrypted);

		let mut wire = Vec::new();
		wire.extend_from_slice(&MAGIC_HEADER_UDP_NEGO.to_le_bytes());
		wire.extend_from_slice(&(encrypted.len() as u32).to_le_bytes());
		wire.extend_from_slice(&1u32.to_le_bytes()); // unknown_a
		wire.extend_from_slice(&tid.to_le_bytes());
		wire.extend_from_slice(&checksum.to_le_bytes());
		wire.extend_from_slice(&encrypted);

		let mut buf = BytesMut::from(wire.as_slice());
		let result = BcUdp::deserialize(&mut buf);
		assert!(
			matches!(result, Err(Error::NomError(_))),
			"expected NomError for bad XML, got {result:?}"
		);
	}

	#[test]
	// Tests the decoding of multiple packets
	fn test_multi_packets() {
		init();

		let sample = [
			include_bytes!("samples/udp_multi_0.bin").as_ref(),
			include_bytes!("samples/udp_multi_1.bin").as_ref(),
			include_bytes!("samples/udp_multi_2.bin").as_ref(),
			include_bytes!("samples/udp_multi_3.bin").as_ref(),
			include_bytes!("samples/udp_multi_4.bin").as_ref(),
			include_bytes!("samples/udp_multi_5.bin").as_ref(),
			include_bytes!("samples/udp_multi_6.bin").as_ref(),
			include_bytes!("samples/udp_multi_7.bin").as_ref(),
			include_bytes!("samples/udp_multi_8.bin").as_ref(),
			include_bytes!("samples/udp_multi_9.bin").as_ref(),
		]
		.concat();

		let mut buf = BytesMut::from(&sample[..]);
		// Should derealise all of this
		loop {
			let e = BcUdp::deserialize(&mut buf);
			match e {
				Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
					// Reach end of files
					break;
				}
				Err(Error::NomIncomplete(_)) if buf.is_empty() => {
					// Reach end of files
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
	// Hostile peer sends a Discovery packet with `payload_size = u32::MAX`.
	// Pre-fix the BcUdp framer's `take(payload_size)` returned Incomplete
	// in a loop and the read buffer grew toward 4 GiB. Now the cap on
	// payload_size rejects up front.
	fn disc_payload_size_above_cap_rejected() {
		let mut wire = Vec::new();
		wire.extend_from_slice(&MAGIC_HEADER_UDP_NEGO.to_le_bytes());
		wire.extend_from_slice(&u32::MAX.to_le_bytes()); // payload_size = 4 GiB - 1
		wire.extend_from_slice(&1u32.to_le_bytes()); // unknown_a
		wire.extend_from_slice(&0u32.to_le_bytes()); // tid
		wire.extend_from_slice(&0u32.to_le_bytes()); // checksum
		let mut buf = BytesMut::from(wire.as_slice());
		let result = BcUdp::deserialize(&mut buf);
		assert!(
			matches!(result, Err(Error::NomError(_))),
			"payload_size above MAX_BCUDP_PAYLOAD must reject, got {result:?}"
		);
	}

	// Property test: the BcUdp parser must absorb any UDP datagram the
	// public ports (9999, 58200) can receive without panicking. The wake
	// server's listeners are exposed to LAN scans, unrelated Reolink
	// products, and any hostile peer that knows the magic bytes.
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 1024,
			..ProptestConfig::default()
		})]

		#[test]
		fn bcudp_deserialize_never_panics_on_arbitrary_bytes(
			bytes in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			let mut buf = BytesMut::from(&bytes[..]);
			let _ = BcUdp::deserialize(&mut buf);
		}

		#[test]
		fn bcudp_deserialize_with_valid_magic_prefix_never_panics(
			magic_idx in 0u8..3,
			tail in proptest::collection::vec(any::<u8>(), 0..2048),
		) {
			const MAGICS: [u32; 3] = [
				MAGIC_HEADER_UDP_NEGO,
				MAGIC_HEADER_UDP_ACK,
				MAGIC_HEADER_UDP_DATA,
			];
			let mut bytes = MAGICS[magic_idx as usize].to_le_bytes().to_vec();
			bytes.extend_from_slice(&tail);
			let mut buf = BytesMut::from(&bytes[..]);
			let _ = BcUdp::deserialize(&mut buf);
		}
	}
}
