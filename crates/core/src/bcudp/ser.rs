use super::{crc::calc_crc, model::*, xml_crypto::encrypt};
use crate::Error;
use cookie_factory::bytes::*;
use cookie_factory::sequence::tuple;
use cookie_factory::SerializeFn;
use cookie_factory::{combinator::*, gen};
use log::error;
use std::io::Write;

/// Cast `len` to the wire's `u32` length field, returning an error if
/// the payload exceeded `u32::MAX`. Real BcUdp payloads are bounded by
/// the OS UDP MTU (~65 KiB max datagram), so this never fires in
/// practice — the check guards against malformed / synthetic inputs
/// where `len() as u32` would silently truncate the high bits and
/// produce a wire-format mismatch the receiver can't detect.
fn len_as_u32(len: usize) -> Result<u32, Error> {
	u32::try_from(len).map_err(|_| {
		error!("BcUdp payload length {len} exceeds u32::MAX — refusing to truncate");
		// Mirrors the encoding-error shape `serialize()` already
		// emits via `cookie_factory::GenError` for the XML pre-encode.
		Error::from(cookie_factory::GenError::CustomError(5))
	})
}

impl BcUdp {
	/// Serialize this BcUdp packet to `buf`, returning the writer.
	pub fn serialize<W: Write>(&self, buf: W) -> Result<W, Error> {
		let (buf, _) = match &self {
			BcUdp::Discovery(payload) => {
				// Pre-serialize XML eagerly so a `quick_xml` error
				// surfaces as a typed `Error` (via `cookie_factory::GenError`)
				// instead of panicking inside the cookie-factory closure.
				let xml_bytes = payload.payload.serialize(vec![]).map_err(|e| {
					error!("UdpXml payload serialize failed: {e}");
					cookie_factory::GenError::CustomError(4)
				})?;
				let xml_payload = encrypt(payload.tid, &xml_bytes);
				let xml_len = len_as_u32(xml_payload.len())?;
				gen(bcudp_disc(payload, &xml_payload, xml_len), buf)?
			}
			BcUdp::Ack(payload) => {
				let binary_payload = &payload.payload;
				let bin_len = len_as_u32(binary_payload.len())?;
				gen(bcudp_ack(payload, binary_payload, bin_len), buf)?
			}
			BcUdp::Data(payload) => {
				let binary_payload = &payload.payload;
				let bin_len = len_as_u32(binary_payload.len())?;
				gen(bcudp_data(payload, binary_payload, bin_len), buf)?
			}
		};

		Ok(buf)
	}
}

fn bcudp_disc<'a, W: 'a + Write>(
	payload: &'a UdpDiscovery,
	xml_payload: &'a [u8],
	xml_len: u32,
) -> impl SerializeFn<W> + 'a {
	let checksum = calc_crc(xml_payload);
	tuple((
		le_u32(MAGIC_HEADER_UDP_NEGO),
		le_u32(xml_len),
		le_u32(1),
		le_u32(payload.tid),
		le_u32(checksum),
		slice(xml_payload),
	))
}

fn bcudp_ack<'a, W: 'a + Write>(
	payload: &'a UdpAck,
	binary_payload: &'a [u8],
	bin_len: u32,
) -> impl SerializeFn<W> + 'a {
	tuple((
		le_u32(MAGIC_HEADER_UDP_ACK),
		le_i32(payload.connection_id),
		le_u32(0),
		le_u32(payload.group_id),
		le_u32(payload.packet_id),
		le_u32(payload.maybe_latency),
		le_u32(bin_len),
		slice(binary_payload),
	))
}

fn bcudp_data<'a, W: 'a + Write>(
	payload: &'a UdpData,
	binary_payload: &'a [u8],
	bin_len: u32,
) -> impl SerializeFn<W> + 'a {
	tuple((
		le_u32(MAGIC_HEADER_UDP_DATA),
		le_i32(payload.connection_id),
		le_u32(0),
		le_u32(payload.packet_id),
		le_u32(bin_len),
		slice(binary_payload),
	))
}

#[cfg(test)]
mod tests {
	use super::len_as_u32;
	use crate::bcudp::model::*;
	use bytes::BytesMut;

	#[test]
	fn len_as_u32_passes_in_range_values() {
		assert_eq!(len_as_u32(0).unwrap(), 0);
		assert_eq!(len_as_u32(1500).unwrap(), 1500); // typical UDP MTU
		assert_eq!(len_as_u32(u32::MAX as usize).unwrap(), u32::MAX);
	}

	#[test]
	#[cfg(target_pointer_width = "64")]
	fn len_as_u32_rejects_out_of_range() {
		// Only meaningful on 64-bit targets where `usize > u32`.
		let too_big = (u32::MAX as usize) + 1;
		assert!(len_as_u32(too_big).is_err());
	}

	fn init() {
		crate::test_logs::init();
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a discovery xml
	fn test_nego_disconnect() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_disc.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf: Vec<u8> = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		// Raw samples don't quite match exactly
		// because the serde for xml puts spaces and new lines in different places
		// then the raw data from the camera so we skip this last assert
		//assert_eq!(&sample[..], ser_buf.as_slice());
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Camera Transmission xml
	fn test_nego_cam_transmission() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_camt.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		// Raw samples don't quite match exactly
		// because the serde for xml puts spaces and new lines in different places
		// then the raw data from the camera so we skip this last assert
		//assert_eq!(&sample[..], ser_buf.as_slice());
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Client Transmission xml
	fn test_nego_client_transmission() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_clientt.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		// Raw samples don't quite match exactly
		// because the serde for xml puts spaces and new lines in different places
		// then the raw data from the camera so we skip this last assert
		//assert_eq!(&sample[..], ser_buf.as_slice());
	}

	#[test]
	// Tests the decoding of a UdpDiscovery with a Camera CFM xml
	fn test_nego_cfm() {
		init();

		let sample = include_bytes!("samples/udp_negotiate_camcfm.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		// Raw samples don't quite match exactly
		// because the serde for xml puts spaces and new lines in different places
		// then the raw data from the camera so we skip this last assert
		//assert_eq!(&sample[..], ser_buf.as_slice());
	}

	#[test]
	// Tests the decoding of an acknoledge packet
	fn test_ack() {
		init();

		let sample = include_bytes!("samples/udp_ack.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		assert_eq!(&sample[..], ser_buf.as_slice());
	}

	#[test]
	// Tests the decoding of an data packet
	fn test_data() {
		init();

		let sample = include_bytes!("samples/udp_data.bin");

		let msg = BcUdp::deserialize(&mut BytesMut::from(&sample[..])).unwrap();
		let ser_buf = msg.serialize(vec![]).unwrap();
		let msg2 = BcUdp::deserialize(&mut BytesMut::from(ser_buf.as_slice())).unwrap();
		assert_eq!(msg, msg2);
		assert_eq!(&sample[..], ser_buf.as_slice());
	}
}
