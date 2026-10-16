#![warn(unused_crate_dependencies)]
#![warn(missing_docs)]
//! # Neolink-Core
//!
//! Neolink-Core is a rust library for interacting with reolink and family cameras.
//!
//! Most high level camera controls are in the [`bc_protocol`] module
//!
//! A camera can be initialised with
//!
//! ```no_run
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use bairelay_neolink_core::bc_protocol::{BcCamera, BcCameraOpt, DiscoveryMethods, ConnectionProtocol, Credentials};
//! let options = BcCameraOpt {
//!     name: "CamName".to_string(),
//!     channel_id: 0,
//!     addrs: ["192.168.1.1".parse().unwrap()].to_vec(),
//!     port: Some(9000),
//!     uid: Some("CAMUID".to_string()),
//!     protocol: ConnectionProtocol::TcpUdp,
//!     discovery: DiscoveryMethods::Relay,
//!     credentials: Credentials {
//!         username: "username".to_string(),
//!         password: Some("password".to_string()),
//!     },
//!     cloud_account: None,
//!     cloud_password: None,
//!     cloud_mfa_trust_token: None,
//!     cloud_refresh_token: None,
//!     debug: false,
//!     max_discovery_retries: 10,
//! };
//! let mut camera = BcCamera::new(&options).await.unwrap();
//! # })
//! ```
//!
//! After that login can be conducted with
//!
//! ```no_run
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # use bairelay_neolink_core::bc_protocol::{BcCamera, BcCameraOpt, DiscoveryMethods, ConnectionProtocol, Credentials};
//! # let options = BcCameraOpt {
//! #    name: "CamName".to_string(),
//! #    channel_id: 0,
//! #    addrs: ["192.168.1.1".parse().unwrap()].to_vec(),
//! #    port: Some(9000),
//! #    uid: Some("CAMUID".to_string()),
//! #    protocol: ConnectionProtocol::TcpUdp,
//! #    discovery: DiscoveryMethods::Relay,
//! #    credentials: Credentials {
//! #        username: "username".to_string(),
//! #        password: Some("password".to_string()),
//! #    },
//! #    cloud_account: None,
//! #    cloud_password: None,
//! #    cloud_mfa_trust_token: None,
//! #    cloud_refresh_token: None,
//! #    debug: false,
//! #    max_discovery_retries: 10,
//! # };
//! # let mut camera = BcCamera::new(&options).await.unwrap();
//! camera.login().await;
//! # })
//! ```
//! For further commands see the [`bc_protocol::BcCamera`] struct.
//!

/// Contains low level BC structures and formats
pub mod bc;
/// Contains high level interfaces for the camera
pub mod bc_protocol;
/// Contains low level structures and formats for the media substream
pub mod bcmedia;
///  Contains low level structures and formats for the udpstream
pub mod bcudp;
/// Cloud bundle minting for account ("cloud") cameras (apis.reolink.com).
pub mod cloud;
/// Log-safe renderings of identifiers (camera UIDs) for info-level logs.
pub mod redact;

#[cfg(test)]
mod xml_corpus_tests;

/// This is the top level error structure of the library
///
/// Most commands will either return their `Ok(result)` or this `Err(Error)`
pub use bc_protocol::Error;

pub(crate) use bc_protocol::{Credentials, Result};

pub(crate) type NomErrorType<'a> = nom::error::VerboseError<&'a [u8]>;

/// Thin public shims around otherwise `pub(crate)` parsers so the
/// out-of-tree fuzz harness can drive them. Gated on the `fuzz-api`
/// Cargo feature.
#[cfg(feature = "fuzz-api")]
pub mod fuzz_api {
	use bytes::BytesMut;

	/// Drive `Bc::deserialize` on arbitrary input under the
	/// `Unencrypted` codec context.
	pub fn parse_bc(input: &[u8]) -> Result<crate::bc::model::Bc, crate::Error> {
		let ctx = crate::bc::model::BcContext::new_with_encryption(
			crate::bc::crypto::EncryptionProtocol::Unencrypted,
		);
		let mut buf = BytesMut::from(input);
		crate::bc::model::Bc::deserialize(&ctx, &mut buf)
	}

	/// Drive `BcXml::try_parse` on arbitrary input.
	pub fn parse_bc_xml(input: &[u8]) -> Result<crate::bc::xml::BcXml, quick_xml::de::DeError> {
		crate::bc::xml::BcXml::try_parse(input)
	}

	pub use crate::bc_protocol::connection::udpsource::{UdpFlowState, REORDER_CAP};
	pub use crate::bcudp::model::{UdpAck, UdpData};

	/// Drive a sequence of `UdpFlowState` operations parsed from
	/// arbitrary bytes. Asserts the bounded-state invariants
	/// (REORDER_CAP cap on `received`, no panic on u32::MAX corners)
	/// survive every input.
	///
	/// Encoding: each op consumes 1 tag byte + payload:
	///
	/// - tag % 4 == 0: handle_data — 5 bytes (4 = packet_id LE, 1 =
	///   payload-length-cap byte). The synthetic UdpData payload is
	///   filled with the tag value to keep the fuzz corpus dense.
	/// - tag % 4 == 1: handle_ack — 5 bytes (4 = ack.packet_id LE,
	///   1 = ack.payload length, capped at remaining input).
	/// - tag % 4 == 2: enqueue_send — 1 byte (length of synthetic
	///   payload, capped at remaining input).
	/// - tag % 4 == 3: drain_contiguous + build_send_ack — no params.
	///
	/// Input length is clamped to 8 KiB so the harness drives at most
	/// ~1 k operations per iteration — `sent` is unbounded by design
	/// (the camera-side ack stream shrinks it in production but the
	/// fuzzer doesn't model that), so a multi-MB input would OOM the
	/// process with synthetic enqueue_send growth that has nothing to
	/// do with real bug-finding.
	pub fn flow_state_drive_arbitrary(data: &[u8]) {
		let data = &data[..data.len().min(8 * 1024)];
		let mut s = UdpFlowState::new(0xABCD, 0x1234);
		let mut p = 0;
		while p < data.len() {
			let tag = data[p];
			p += 1;
			match tag % 4 {
				0 => {
					if data.len() - p < 5 {
						break;
					}
					let packet_id =
						u32::from_le_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
					let payload_len = data[p + 4] as usize;
					p += 5;
					let _ = s.handle_data(UdpData {
						connection_id: 0xABCD,
						packet_id,
						payload: vec![tag; payload_len.min(64)],
					});
				}
				1 => {
					if data.len() - p < 5 {
						break;
					}
					let pkt = u32::from_le_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
					let plen = data[p + 4] as usize;
					p += 5;
					let payload_len = plen.min(data.len().saturating_sub(p));
					let payload: Vec<u8> = data[p..p + payload_len].to_vec();
					p += payload_len;
					s.handle_ack(UdpAck {
						connection_id: 0x1234,
						packet_id: pkt,
						group_id: 0,
						maybe_latency: 0,
						payload,
					});
				}
				2 => {
					if data.len() - p < 1 {
						break;
					}
					let n = data[p] as usize;
					p += 1;
					let take = n.min(data.len().saturating_sub(p)).min(4096);
					let _ = s.enqueue_send(&data[p..p + take]);
					p += take;
				}
				_ => {
					let _ = s.drain_contiguous();
					let _ = s.build_send_ack();
				}
			}
			// Bounded-state invariants — these are the load-bearing
			// claims the production code makes. Crash → fuzzer flag.
			assert!(s.received_len() <= REORDER_CAP);
		}
	}
}

/// Offline decoder primitives for captured BcUdp sessions. Drives
/// Bc / BcUdp parsing + AES-CFB decryption against a pcap recorded with
/// tcpdump. Consumed by `tests/scripts/decode-bc-pcap`. Gated on the
/// `pcap-decode-api` Cargo feature so production builds never compile
/// this surface.
///
/// Stream model: a captured BcUdp session has two directions
/// (client→camera and camera→client). Each direction reassembles its own
/// sequence of `UdpData` packets by `packet_id` into a Bc TCP-like byte
/// stream, then drives `Bc::deserialize` against it. Both directions
/// share a single `BcContext` whose `EncryptionProtocol` is updated when
/// the camera's login reply (msg_id=1, `response_code >> 8 == 0xdd`)
/// surfaces — same negotiation logic the production codex runs, lifted
/// into `Session::feed_datagram`.
#[cfg(feature = "pcap-decode-api")]
pub mod pcap_decode_api {
	use bytes::BytesMut;
	use std::collections::BTreeMap;

	use crate::bc::crypto::EncryptionProtocol;
	use crate::bc::model::{Bc, BcBody, BcContext, BcMeta, ModernMsg};
	use crate::bc::xml::{BcPayloads, BcXml, Encryption};
	use crate::bcudp::model::BcUdp;

	pub use crate::bc::model::{Bc as BcMessage, BcMeta as BcMessageMeta};
	pub use crate::bc::xml::BcXml as DecodedXml;
	pub use crate::bc_protocol::Credentials;
	pub use crate::bcudp::model::{UdpAck, UdpData, UdpDiscovery};
	pub use crate::Error;

	/// Source of a UDP datagram in a captured session.
	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub enum Direction {
		/// Client to camera (operator software → camera).
		ClientToCamera,
		/// Camera to client (camera → operator software).
		CameraToClient,
	}

	/// Per-direction reassembly state: pending out-of-order `UdpData`
	/// packets keyed by `packet_id`, plus the contiguous Bc byte stream
	/// drained from them.
	struct DirState {
		next_packet_id: Option<u32>,
		pending: BTreeMap<u32, Vec<u8>>,
		bc_buf: BytesMut,
	}

	impl DirState {
		fn new() -> Self {
			Self {
				next_packet_id: None,
				pending: BTreeMap::new(),
				bc_buf: BytesMut::new(),
			}
		}

		fn feed(&mut self, data: UdpData) {
			let id = data.packet_id;
			// First packet seen sets the baseline. Production cameras
			// don't always start at 0 (depends on the connection negotiation
			// instant); align the cursor to whatever the first observed
			// packet_id is so we don't stall waiting for "missing" earlier
			// packets that simply weren't captured.
			let next = *self.next_packet_id.get_or_insert(id);
			if id < next {
				return; // late duplicate / retransmit, ignore
			}
			self.pending.insert(id, data.payload);
			while let Some(payload) = self.pending.remove(&self.next_packet_id.unwrap()) {
				self.bc_buf.extend_from_slice(&payload);
				let cur = self.next_packet_id.unwrap();
				self.next_packet_id = Some(cur.wrapping_add(1));
			}
		}
	}

	/// One captured BcUdp session. Holds the shared encryption state +
	/// per-direction reassembly buffers.
	pub struct Session {
		ctx: BcContext,
		c2d: DirState,
		d2c: DirState,
	}

	/// Result of feeding one datagram: zero or more decoded Bc messages
	/// that became complete after the new bytes arrived.
	#[derive(Debug)]
	pub struct DecodedMessage {
		/// Direction the message travelled.
		pub direction: Direction,
		/// The decoded Bc message (header + decrypted XML / binary body).
		pub bc: BcMessage,
		/// Best-effort plaintext view of a binary payload when the camera
		/// is in `FullAes` mode and the underlying decoder returned the
		/// raw wire bytes (no `<encryptLen>` was present, so the
		/// production codec couldn't tell whether the bytes were
		/// already plaintext or still ciphertext). Some Bc messages —
		/// notably control replies like `MSG_ID_GET_DST` — encrypt the
		/// payload on the wire even without an `<encryptLen>` marker;
		/// others — notably the high-throughput stream chunks
		/// (`MSG_ID_VIDEO`) — leave the payload plaintext on the wire
		/// even in `FullAes` mode. Production code never needed to tell
		/// these apart because bairelay's own commands always include
		/// `<encryptLen>` when relevant; offline decoders facing
		/// arbitrary captured client traffic do.
		///
		/// The tool consuming this struct prints the raw bytes from
		/// `bc.body` as a hexdump and additionally checks
		/// `manually_decrypted_binary` for an XML / UTF-8 view —
		/// whichever is meaningful is what the operator wants to see.
		/// `None` when the Bc body is not `Binary` or the context isn't
		/// `FullAes`.
		pub manually_decrypted_binary: Option<Vec<u8>>,
	}

	impl Session {
		/// Construct a session decoder for the given camera credentials.
		/// The `Credentials` value is used to derive the AES key once the
		/// login response selects an AES variant.
		pub fn new(creds: Credentials) -> Self {
			let mut ctx = BcContext::new(creds);
			// Enable BcCodex's plaintext-payload trace prints so the
			// caller's `log` subscriber can surface raw decrypted XML —
			// including fields the `BcXml` struct doesn't model, which
			// serde silently drops on parse. This is the only way to
			// see e.g. `<Dst>` blocks inside an unknown msg_id reply.
			ctx.debug_on();
			Self {
				ctx,
				c2d: DirState::new(),
				d2c: DirState::new(),
			}
		}

		/// Feed one captured UDP datagram payload. Discovery and Ack
		/// packets are recognised but not surfaced (they don't carry Bc
		/// messages). Data packets reassemble per-direction; for every
		/// complete Bc message that becomes decodable from the new bytes,
		/// `on_msg` is called in arrival order — once per message,
		/// before the next is decoded. The callback shape is critical
		/// for tools that capture bairelay_neolink_core's `log::trace!` output
		/// to attach raw decrypted payloads to specific messages: the
		/// trace channel is a shared global, so the caller must drain
		/// it between successive decodes.
		pub fn feed_datagram<F>(
			&mut self,
			direction: Direction,
			datagram: &[u8],
			on_msg: F,
		) -> Result<(), Error>
		where
			F: FnMut(DecodedMessage),
		{
			let mut buf = BytesMut::from(datagram);
			let bcudp = match BcUdp::deserialize(&mut buf) {
				Ok(b) => b,
				Err(Error::NomIncomplete(_)) => return Ok(()),
				Err(e) => return Err(e),
			};

			match bcudp {
				BcUdp::Data(data) => {
					match direction {
						Direction::ClientToCamera => self.c2d.feed(data),
						Direction::CameraToClient => self.d2c.feed(data),
					}
					return self.drain(direction, on_msg);
				}
				BcUdp::Discovery(_) | BcUdp::Ack(_) => {}
			}
			Ok(())
		}

		/// Shared Bc-decode loop over one direction's reassembled buffer,
		/// mirroring BcCodex's login-negotiation + binary bookkeeping.
		fn drain<F>(&mut self, direction: Direction, mut on_msg: F) -> Result<(), Error>
		where
			F: FnMut(DecodedMessage),
		{
			let dir_state = match direction {
				Direction::ClientToCamera => &mut self.c2d,
				Direction::CameraToClient => &mut self.d2c,
			};
			loop {
				match Bc::deserialize(&self.ctx, &mut dir_state.bc_buf) {
					Ok(bc) => {
						// Mirror the BcCodex login-response
						// negotiation logic — without this the
						// follow-on messages don't decrypt.
						if let Bc {
							meta:
								BcMeta {
									msg_id: 1,
									response_code,
									..
								},
							body:
								BcBody::ModernMsg(ModernMsg {
									payload:
										Some(BcPayloads::BcXml(BcXml {
											encryption: Some(Encryption { ref nonce, .. }),
											..
										})),
									..
								}),
						} = bc
						{
							if response_code >> 8 == 0xdd {
								let kind = (response_code & 0xff) as u8;
								let new_proto = match kind {
									0x00 => EncryptionProtocol::Unencrypted,
									0x01 => EncryptionProtocol::BCEncrypt,
									0x02 => EncryptionProtocol::aes(
										self.ctx.credentials.make_aeskey(nonce),
									),
									0x12 => EncryptionProtocol::full_aes(
										self.ctx.credentials.make_aeskey(nonce),
									),
									other => {
										return Err(Error::UnknownEncryption(other as usize));
									}
								};
								self.ctx.set_encrypted(new_proto);
							}
						}

						// Mirror BcCodex's binary-mode bookkeeping
						// so streaming msg_nums (3 / 4) don't get
						// mis-parsed as XML on subsequent packets.
						if let BcBody::ModernMsg(ModernMsg {
							extension:
								Some(crate::bc::xml::Extension {
									binary_data: Some(on_off),
									..
								}),
							..
						}) = bc.body
						{
							if on_off == 0 {
								self.ctx.binary_off(bc.meta.msg_num);
							} else {
								self.ctx.binary_on(bc.meta.msg_num);
							}
						}

						// Compute a "would-be plaintext" view for binary
						// payloads when the session is in FullAes —
						// see DecodedMessage's field doc for why.
						let manually_decrypted_binary =
							match (&self.ctx.encryption_protocol, &bc.body) {
								(
									EncryptionProtocol::FullAes { .. },
									BcBody::ModernMsg(ModernMsg {
										payload: Some(BcPayloads::Binary(bytes)),
										..
									}),
								) => Some(
									self.ctx
										.encryption_protocol
										.decrypt(bc.meta.channel_id as u32, bytes.as_slice()),
								),
								_ => None,
							};
						on_msg(DecodedMessage {
							direction,
							bc,
							manually_decrypted_binary,
						});
					}
					Err(Error::NomIncomplete(_)) => break,
					Err(e) => return Err(e),
				}
			}
			Ok(())
		}

		/// Append raw TCP payload bytes for one direction and decode any
		/// complete Bc frames. Baichuan-over-TCP carries Bc frames back to
		/// back with no BcUdp wrapper, so bytes go straight into the buffer.
		pub fn feed_tcp_payload<F>(
			&mut self,
			direction: Direction,
			payload: &[u8],
			on_msg: F,
		) -> Result<(), Error>
		where
			F: FnMut(DecodedMessage),
		{
			match direction {
				Direction::ClientToCamera => self.c2d.bc_buf.extend_from_slice(payload),
				Direction::CameraToClient => self.d2c.bc_buf.extend_from_slice(payload),
			}
			self.drain(direction, on_msg)
		}
	}
}
