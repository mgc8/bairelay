//! Thin helpers wrapping `bairelay_neolink_core::bcudp` framing for the wake server.

use bairelay_neolink_core::bcudp::{
	model::{BcUdp, UdpDiscovery},
	xml::UdpXml,
};
use bytes::BytesMut;
use rand::Rng;

use crate::WakeServerError;

/// Hard cap on the UID field length we accept on any inbound BcUdp
/// payload. Real Argus UIDs are 16-char base + 4-char firmware suffix
/// (= 20 chars). 64 is a generous upper bound that still rejects the
/// memory-amplification vector where a hostile peer floods D2M_Q with
/// kilobyte-long UIDs to bloat the registry / anchor map.
const MAX_UID_LEN: usize = 64;

/// Decode a UDP datagram into a typed `UdpXml`. Anything that isn't a
/// `Discovery` packet (i.e. ack / data — those carry Baichuan stream
/// payload, not what the wake server speaks) is reported as
/// `WakeServerError::UnexpectedPacketKind` with a label identifying which
/// kind appeared.
///
/// Variants that carry a UID field are bounds-checked at
/// `MAX_UID_LEN`. The XOR cipher in `bcudp::xml_crypto` is symmetric
/// over the full byte domain, so without this guard a peer can stuff
/// any printable bytes into a `<uid>...</uid>` element of arbitrary
/// size, get them deserialized into a String, and have them stored
/// indefinitely in the registry / session-anchors map.
pub fn decode_discovery(buf: &[u8]) -> Result<(u32, UdpXml), WakeServerError> {
	let mut bm = BytesMut::from(buf);
	let pkt = BcUdp::deserialize(&mut bm)?;
	match pkt {
		BcUdp::Discovery(UdpDiscovery { tid, payload }) => {
			validate_uid_bounds(&payload)?;
			Ok((tid, payload))
		}
		BcUdp::Ack(_) => Err(WakeServerError::UnexpectedPacketKind { kind: "Ack" }),
		BcUdp::Data(_) => Err(WakeServerError::UnexpectedPacketKind { kind: "Data" }),
	}
}

/// The UID a payload carries, for the packet types that name a camera.
///
/// The wake server reads this to bound the field before storing it. The binary's
/// startup discovery scan reads the same field off whatever a camera sends it, so
/// the two cannot disagree about which packets name a camera — a list each caller
/// kept for itself would be a list one of them forgot to update.
pub fn payload_uid(payload: &UdpXml) -> Option<&str> {
	match payload {
		UdpXml::C2mQ(q) => Some(q.uid.as_str()),
		UdpXml::D2mQ(q) => Some(q.uid.as_str()),
		UdpXml::C2rC(c) => Some(c.uid.as_str()),
		UdpXml::D2rHb(hb) => Some(hb.uid.as_str()),
		UdpXml::D2rR(r) => Some(r.uid.as_str()),
		_ => None,
	}
}

/// Reject UDP payloads whose UID field exceeds `MAX_UID_LEN`. Returns
/// `Err(WakeServerError::UnexpectedPacketKind)` (with a "uid-too-long"
/// label) so the caller's existing log path fires uniformly with
/// other bad-packet rejections.
fn validate_uid_bounds(payload: &UdpXml) -> Result<(), WakeServerError> {
	let uid = payload_uid(payload);
	if let Some(u) = uid {
		if u.len() > MAX_UID_LEN {
			return Err(WakeServerError::UnexpectedPacketKind {
				kind: "uid-too-long",
			});
		}
	}
	Ok(())
}

/// Encode a `UdpXml` payload into a complete BcUdp Discovery datagram.
pub fn encode_discovery(tid: u32, payload: UdpXml) -> Result<Vec<u8>, WakeServerError> {
	let pkt = BcUdp::Discovery(UdpDiscovery { tid, payload });
	let buf = Vec::<u8>::new();
	let buf = pkt.serialize(buf)?;
	Ok(buf)
}

/// Mint a random session ID for `R2D_C` / `R2C_C_R`.
pub fn random_sid() -> u32 {
	rand::thread_rng().gen()
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bcudp::xml::{C2mQ, IpPort, M2cQr};

	#[test]
	fn encode_decode_round_trip_preserves_payload() {
		let payload = UdpXml::C2mQ(C2mQ {
			uid: "TESTUID".into(),
			os: "MAC".into(),
		});
		let bytes = encode_discovery(0xdeadbeef, payload.clone()).unwrap();
		let (tid, decoded) = decode_discovery(&bytes).unwrap();
		assert_eq!(tid, 0xdeadbeef);
		assert_eq!(decoded, payload);
	}

	#[test]
	fn decode_rejects_random_garbage() {
		assert!(decode_discovery(&[0u8; 4]).is_err());
		assert!(decode_discovery(&[0xff; 32]).is_err());
	}

	#[test]
	fn decode_rejects_ack_packet_with_kind_label() {
		use bairelay_neolink_core::bcudp::model::{BcUdp, UdpAck};
		let ack = BcUdp::Ack(UdpAck::empty(7));
		let bytes = ack.serialize(Vec::new()).expect("serialize ack");
		let err = decode_discovery(&bytes).expect_err("decode should reject ack");
		match err {
			WakeServerError::UnexpectedPacketKind { kind } => assert_eq!(kind, "Ack"),
			other => panic!("expected UnexpectedPacketKind, got {other:?}"),
		}
	}

	#[test]
	fn decode_rejects_data_packet_with_kind_label() {
		use bairelay_neolink_core::bcudp::model::{BcUdp, UdpData};
		let data = BcUdp::Data(UdpData {
			connection_id: 7,
			packet_id: 1,
			payload: vec![0xab, 0xcd],
		});
		let bytes = data.serialize(Vec::new()).expect("serialize data");
		let err = decode_discovery(&bytes).expect_err("decode should reject data");
		match err {
			WakeServerError::UnexpectedPacketKind { kind } => assert_eq!(kind, "Data"),
			other => panic!("expected UnexpectedPacketKind, got {other:?}"),
		}
	}

	#[test]
	fn encode_discovery_round_trips_m2c_q_r() {
		let payload = UdpXml::M2cQr(M2cQr {
			reg: Some(IpPort {
				ip: "127.0.0.1".into(),
				port: 58200,
			}),
			relay: Some(IpPort {
				ip: "127.0.0.1".into(),
				port: 58200,
			}),
			log: Some(IpPort {
				ip: "127.0.0.1".into(),
				port: 58200,
			}),
			t: Some(IpPort {
				ip: "127.0.0.1".into(),
				port: 58200,
			}),
		});
		let bytes = encode_discovery(0x42, payload.clone()).unwrap();
		let (tid, decoded) = decode_discovery(&bytes).unwrap();
		assert_eq!(tid, 0x42);
		assert_eq!(decoded, payload);
	}
}
