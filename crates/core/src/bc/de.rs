use super::model::*;
use crate::Error;
use bytes::{Buf, BytesMut};
use log::*;
use nom::{
	bytes::streaming::take, combinator::*, error::context as error_context, number::streaming::*,
	sequence::*, Parser,
};

type IResult<I, O, E = nom::error::VerboseError<I>> = Result<(I, O), nom::Err<E>>;

impl Bc {
	/// Returns Ok(deserialized data, the amount of data consumed)
	/// Can then use this as the amount that should be remove from a buffer
	pub(crate) fn deserialize(context: &BcContext, buf: &mut BytesMut) -> Result<Bc, Error> {
		let parser = BcParser { context };
		let (result, amount) = match consumed(parser)(buf) {
			Ok((_, (parsed_buff, result))) => Ok((result, parsed_buff.len())),
			Err(e) => Err(Error::from(e)),
		}?;

		buf.advance(amount);
		Ok(result)
	}
}

struct BcParser<'a> {
	context: &'a BcContext,
}

impl<'a> Parser<&'a [u8], Bc, nom::error::VerboseError<&'a [u8]>> for BcParser<'a> {
	fn parse(&mut self, buf: &'a [u8]) -> IResult<&'a [u8], Bc> {
		bc_msg(self.context, buf)
	}
}

fn bc_msg<'a>(context: &BcContext, buf: &'a [u8]) -> IResult<&'a [u8], Bc> {
	let (buf, header) = bc_header(buf)?;
	let (buf, body) = bc_body(context, &header, buf)?;

	let bc = Bc {
		meta: header.to_meta(),
		body,
	};

	Ok((buf, bc))
}

fn bc_body<'a>(context: &BcContext, header: &BcHeader, buf: &'a [u8]) -> IResult<&'a [u8], BcBody> {
	if header.is_modern() {
		let (buf, body) = bc_modern_msg(context, header, buf)?;
		Ok((buf, BcBody::ModernMsg(body)))
	} else {
		let (buf, body) = match header.msg_id {
			MSG_ID_LOGIN => bc_legacy_login_msg(buf)?,
			_ => (buf, LegacyMsg::UnknownMsg),
		};
		Ok((buf, BcBody::LegacyMsg(body)))
	}
}

fn hex32<'a>() -> impl FnMut(&'a [u8]) -> IResult<&'a [u8], String> {
	map_res(take(32usize), |slice: &'a [u8]| {
		String::from_utf8(slice.to_vec())
	})
}

fn bc_legacy_login_msg(buf: &'_ [u8]) -> IResult<&'_ [u8], LegacyMsg> {
	let (buf, username) = hex32()(buf)?;
	let (buf, password) = hex32()(buf)?;

	Ok((buf, LegacyMsg::LoginMsg { username, password }))
}

fn bc_modern_msg<'a>(
	context: &BcContext,
	header: &BcHeader,
	buf: &'a [u8],
) -> IResult<&'a [u8], ModernMsg> {
	use nom::{
		error::{ContextError, ErrorKind, ParseError},
		Err,
	};

	fn make_error<I, E>(input: I, ctx: &'static str, kind: ErrorKind) -> E
	where
		I: std::marker::Copy,
		E: ParseError<I> + ContextError<I>,
	{
		E::add_context(input, ctx, E::from_error_kind(input, kind))
	}

	// If missing payload_offset treat all as payload
	let ext_len = header.payload_offset.unwrap_or_default();

	// Validate the wire fields against each other before subtracting.
	// `body_len` and `payload_offset` come straight from the on-wire
	// header — a hostile peer (or a corrupted relay packet) can set
	// `payload_offset > body_len`, which underflows in debug builds and
	// wraps to ~4 GiB in release. The streaming `take()` then returns
	// `Incomplete` and the codec keeps growing its read buffer toward
	// 4 GiB before failing — practical OOM DoS on a memory-constrained
	// host. Reject explicitly with a typed parse error so the camera
	// task drops the packet instead of expanding to fill RAM.
	if ext_len > header.body_len {
		return Err(Err::Failure(make_error(
			buf,
			"payload_offset > body_len",
			ErrorKind::Verify,
		)));
	}
	let (buf, ext_buf) = take(ext_len)(buf)?;
	let payload_len = header.body_len - ext_len;
	let (buf, payload_buf) = take(payload_len)(buf)?;

	let decrypted;
	let processed_ext_buf = match context.get_encrypted() {
		EncryptionProtocol::Unencrypted => ext_buf,
		encryption_protocol => {
			decrypted = encryption_protocol.decrypt(header.channel_id as u32, ext_buf);
			&decrypted
		}
	};

	let mut in_binary = false;
	let mut encrypted_len = None;
	// Now we'll take the buffer that Nom gave a ref to and parse it.
	let extension = if ext_len > 0 {
		// `BcContext.debug` previously dumped the decrypted Extension
		// XML straight to stdout via `println!`. Login replies and
		// every authenticated control message flow through here, so
		// the spew leaked credentials / tokens to whatever process
		// stdout routed to (cron mail, journald, supervisor logs).
		// Routed through `log::trace!` so it stays opt-in via
		// `RUST_LOG=trace` and never lands on stdout.
		if context.debug {
			log::trace!(
				"Extension Txt: {:?}",
				String::from_utf8(processed_ext_buf.to_vec()).unwrap_or("Not Text".to_string())
			);
		}
		// Apply the XML parse function, but throw away the reference to decrypted in the Ok and
		// Err case. This error-error-error thing is the same idiom Nom uses internally.
		let parsed = Extension::try_parse(processed_ext_buf).map_err(|e| {
			// The buffer, and the parse error that can quote it, are
			// logged only when debugging: either can hold credentials.
			log::error!(
				"Unable to parse extension XML ({} bytes)",
				processed_ext_buf.len()
			);
			if context.debug {
				log::trace!("Extension buffer: {:?}", processed_ext_buf);
				log::trace!("Extension XML error: {e:?}");
			}
			Err::Error(make_error(
				buf,
				"Unable to parse Extension XML",
				ErrorKind::MapRes,
			))
		})?;
		if let Extension {
			binary_data: Some(1),
			encrypt_len,
			..
		} = parsed
		{
			// In binary so tell the current context that we need to treat the payload as binary
			in_binary = true;
			encrypted_len = encrypt_len;
		}
		Some(parsed)
	} else {
		None
	};

	// Now to handle the payload block
	// This block can either be xml or binary depending on what the message expects.
	// For our purposes we use try_parse and if all xml based parsers fail we treat
	// As binary
	let payload;
	if payload_len > 0 {
		// Extract remainder of message as binary, if it exists
		const UNENCRYPTED: EncryptionProtocol = EncryptionProtocol::Unencrypted;
		const BC_ENCRYPTED: EncryptionProtocol = EncryptionProtocol::BCEncrypt;
		let encryption_protocol = match header {
			BcHeader {
				msg_id: 1,
				response_code,
				..
			} if (response_code >> 8) & 0xff == 0xdd => {
				// 0xdd means we are setting the encryption method
				// Durig login, the max encryption is BcEncrypt since
				// the nonce has not been exchanged yet
				match response_code & 0xff {
					0x00 => &UNENCRYPTED,
					_ => &BC_ENCRYPTED,
				}
			}
			BcHeader { msg_id: 1, .. } => {
				match &context.get_encrypted() {
					EncryptionProtocol::Aes { .. } | EncryptionProtocol::FullAes { .. } => {
						// During login max is BcEncrypt
						&BC_ENCRYPTED
					}
					n => *n,
				}
			}
			_ => context.get_encrypted(),
		};

		let processed_payload_buf =
			encryption_protocol.decrypt(header.channel_id as u32, payload_buf);
		if context.in_bin_mode.contains(&(header.msg_num)) || in_binary {
			payload = match (context.get_encrypted(), encrypted_len) {
				(EncryptionProtocol::FullAes { .. }, Some(encrypted_len)) => {
					// `encrypted_len` is parsed from the camera's
					// `<Extension>` XML — fully attacker-controlled.
					// Without clamping, a hostile peer setting
					// `<encryptLen>4294967295</encryptLen>` (or any value
					// past the actual buffer) drives `[..n]` into a slice
					// OOB panic that kills the camera task. Clamp to the
					// real buffer length; legitimate camera firmware
					// always emits `encryptLen <= ciphertext.len()`.
					let take = (encrypted_len as usize).min(processed_payload_buf.len());
					Some(BcPayloads::Binary(processed_payload_buf[..take].to_vec()))
				}
				_ => Some(BcPayloads::Binary(payload_buf.to_vec())),
			};
		} else {
			// Same credential-leak concern as the Extension dump
			// above: login replies + every authenticated control
			// message flow through here. Demoted to `log::trace!`.
			if context.debug {
				log::trace!(
					"Payload Txt: {:?}",
					String::from_utf8(processed_payload_buf.to_vec())
						.unwrap_or("Not Text".to_string())
				);
			}
			let xml = BcXml::try_parse(processed_payload_buf.as_slice()).map_err(|e| {
				// The payload, and the parse error that can quote it, are
				// logged only when debugging: either can hold credentials
				// or a webhook URL.
				error!(
					"Unable to parse payload XML: msg_id {} ({} bytes)",
					header.msg_id,
					processed_payload_buf.len()
				);
				if context.debug {
					log::trace!("Payload XML error: {e:?}");
				}
				Err::Error(make_error(
					buf,
					"Unable to parse Payload XML",
					ErrorKind::MapRes,
				))
			})?;
			payload = Some(BcPayloads::BcXml(xml));
		}
	} else {
		payload = None;
	}

	Ok((buf, ModernMsg { extension, payload }))
}

/// Upper bound on a single Baichuan TCP message body. Real-world
/// messages are well under this: snapshots are ~3 MiB at 4K, XML
/// payloads are kilobytes, multi-MiB I-frames flow through the bcmedia
/// codec (a separate framing) not bc. The cap exists so a hostile peer
/// (compromised camera, on-path attacker on plain RTSP, P2P relay
/// impersonator) cannot send a header declaring `body_len = 4 GiB` and
/// drive the `tokio_util::Framed` read buffer to OOM before any payload
/// validation runs.
const MAX_BC_BODY_LEN: u32 = 8 * 1024 * 1024;

fn bc_header(buf: &[u8]) -> IResult<&[u8], BcHeader> {
	let (buf, _magic) = error_context(
		"Magic invalid",
		verify(le_u32, |x| *x == MAGIC_HEADER || *x == MAGIC_HEADER_REV),
	)(buf)?;
	let (buf, msg_id) = error_context("MsgID missing", le_u32)(buf)?;
	let (buf, body_len) = error_context(
		"BodyLen missing or exceeds cap",
		verify(le_u32, |&n| n <= MAX_BC_BODY_LEN),
	)(buf)?;
	let (buf, channel_id) = error_context("ChannelID missing", le_u8)(buf)?;
	let (buf, stream_type) = error_context("StreamType missing", le_u8)(buf)?;
	let (buf, msg_num) = error_context("MsgNum missing", le_u16)(buf)?;
	let (buf, (response_code, class)) =
		error_context("ResponseCode missing", tuple((le_u16, le_u16)))(buf)?;

	let (buf, payload_offset) = error_context(
		"Payload Offset is missing",
		cond(has_payload_offset(class), le_u32),
	)(buf)?;

	Ok((
		buf,
		BcHeader {
			body_len,
			msg_id,
			channel_id,
			stream_type,
			msg_num,
			response_code,
			class,
			payload_offset,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc::xml::*;
	use assert_matches::assert_matches;

	fn init() {
		crate::test_logs::init();
	}

	#[test]
	fn test_bc_modern_login() {
		init();

		let sample = include_bytes!("samples/model_sample_modern_login.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let (buf, header) = bc_header(&sample[..]).unwrap();
		let (_, body) = bc_body(&context, &header, buf).unwrap();
		assert_eq!(header.msg_id, 1);
		assert_eq!(header.body_len, 145);
		assert_eq!(header.channel_id, 0);
		assert_eq!(header.stream_type, 0);
		assert_eq!(header.payload_offset, None);
		assert_eq!(header.response_code, 0xdd01);
		assert_eq!(header.class, 0x6614);
		match body {
			BcBody::ModernMsg(ModernMsg {
				payload:
					Some(BcPayloads::BcXml(BcXml {
						encryption: Some(encryption),
						..
					})),
				..
			}) => assert_eq!(encryption.nonce, "9E6D1FCB9E69846D"),
			_ => panic!(),
		}
	}

	#[test]
	// This is an 0xdd03 encryption from an Argus 2
	fn test_03_enc_login() {
		init();

		let sample = include_bytes!("samples/battery_enc.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let (buf, header) = bc_header(&sample[..]).unwrap();
		let (_, body) = bc_body(&context, &header, buf).unwrap();
		assert_eq!(header.msg_id, 1);
		assert_eq!(header.body_len, 175);
		assert_eq!(header.channel_id, 0);
		assert_eq!(header.stream_type, 0);
		assert_eq!(header.payload_offset, None);
		assert_eq!(header.response_code, 0xdd03);
		assert_eq!(header.class, 0x6614);
		match body {
			BcBody::ModernMsg(ModernMsg {
				payload:
					Some(BcPayloads::BcXml(BcXml {
						encryption: Some(encryption),
						..
					})),
				..
			}) => assert_eq!(encryption.nonce, "0-AhnEZyUg6eKrJFIWgXPF"),
			_ => panic!(),
		}
	}

	#[test]
	fn test_bc_legacy_login() {
		init();

		let sample = include_bytes!("samples/model_sample_legacy_login.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let (buf, header) = bc_header(&sample[..]).unwrap();
		let (_, body) = bc_body(&context, &header, buf).unwrap();
		assert_eq!(header.msg_id, 1);
		assert_eq!(header.body_len, 1836);
		assert_eq!(header.channel_id, 0);
		assert_eq!(header.stream_type, 0);
		assert_eq!(header.payload_offset, None);
		assert_eq!(header.response_code, 0xdc01);
		assert_eq!(header.class, 0x6514);
		match body {
			BcBody::LegacyMsg(LegacyMsg::LoginMsg { username, password }) => {
				assert_eq!(username, "21232F297A57A5A743894A0E4A801FC\0");
				assert_eq!(password, EMPTY_LEGACY_PASSWORD);
			}
			_ => panic!(),
		}
	}

	#[test]
	fn test_bc_modern_login_failed() {
		init();

		let sample = include_bytes!("samples/modern_login_failed.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let (buf, header) = bc_header(&sample[..]).unwrap();
		let (_, body) = bc_body(&context, &header, buf).unwrap();
		assert_eq!(header.msg_id, 1);
		assert_eq!(header.body_len, 0);
		assert_eq!(header.channel_id, 0);
		assert_eq!(header.stream_type, 0);
		assert_eq!(header.payload_offset, Some(0x0));
		assert_eq!(header.response_code, 0x190); // 400
		assert_eq!(header.class, 0x0000);
		match body {
			BcBody::ModernMsg(ModernMsg {
				extension: None,
				payload: None,
			}) => {}
			_ => panic!(),
		}
	}

	#[test]
	fn test_bc_modern_login_success() {
		init();

		let sample = include_bytes!("samples/modern_login_success.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let (buf, header) = bc_header(&sample[..]).unwrap();
		let (_, body) = bc_body(&context, &header, buf).unwrap();
		assert_eq!(header.msg_id, 1);
		assert_eq!(header.body_len, 2949);
		assert_eq!(header.channel_id, 0);
		assert_eq!(header.stream_type, 0);
		assert_eq!(header.payload_offset, Some(0x0));
		assert_eq!(header.response_code, 0xc8); // 200
		assert_eq!(header.class, 0x0000);

		// Previously, we were not handling payload_offset == 0 (no bin offset) correctly.
		// Test that we decoded XML and no binary.
		match body {
			BcBody::ModernMsg(ModernMsg {
				extension: None,
				payload: Some(_),
			}) => {}
			_ => panic!(),
		}
	}

	#[test]
	fn test_bc_binary_mode() {
		init();

		let sample1 = include_bytes!("samples/modern_video_start1.bin");
		let sample2 = include_bytes!("samples/modern_video_start2.bin");

		let mut context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let msg1 = Bc::deserialize(&context, &mut BytesMut::from(&sample1[..])).unwrap();
		match msg1.body {
			BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					binary_data: Some(1),
					..
				}),
				payload: Some(BcPayloads::Binary(bin)),
			}) => {
				assert_eq!(bin.len(), 32);
			}
			_ => panic!(),
		}

		context.in_bin_mode.insert(msg1.meta.msg_num);
		let msg2 = Bc::deserialize(&context, &mut BytesMut::from(&sample2[..])).unwrap();
		match msg2.body {
			BcBody::ModernMsg(ModernMsg {
				extension: None,
				payload: Some(BcPayloads::Binary(bin)),
			}) => {
				assert_eq!(bin.len(), 30344);
			}
			_ => panic!(),
		}
	}

	#[test]
	// B800 seems to have a different header to the E1 and swann cameras
	// the stream_type and message_num do not seem to set in the official clients
	//
	// They also have extra streams
	fn test_bc_b800_externstream() {
		init();

		let sample = include_bytes!("samples/xml_externstream_b800.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let e = Bc::deserialize(&context, &mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(Bc {
				meta:
					BcMeta {
						msg_id: 3,
						channel_id: 0x8c,
						stream_type: 0,
						response_code: 0,
						msg_num: 0,
						class: 0x6414,
					},
				body:
					BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload:
							Some(BcPayloads::BcXml(BcXml {
								preview:
									Some(Preview {
										version,
										channel_id: 0,
										handle: 1024,
										stream_type,
									}),
								..
							})),
					}),
			}) if version == "1.1" && stream_type == Some("externStream".to_string())
		);
	}

	#[test]
	// B800 seems to have a different header to the E1 and swann cameras
	// the stream_type and message_num do not seem to set in the official clients
	//
	// They also have extra streams
	fn test_bc_b800_substream() {
		init();

		let sample = include_bytes!("samples/xml_substream_b800.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let e = Bc::deserialize(&context, &mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(Bc {
				meta:
					BcMeta {
						msg_id: 3,
						channel_id: 143,
						stream_type: 0,
						response_code: 0,
						msg_num: 0,
						class: 0x6414,
					},
				body:
					BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload:
							Some(BcPayloads::BcXml(BcXml {
								preview:
									Some(Preview {
										version,
										channel_id: 0,
										handle: 256,
										stream_type,
									}),
								..
							})),
					}),
			}) if version == "1.1" && stream_type == Some("subStream".to_string())
		);
	}

	#[test]
	// B800 seems to have a different header to the E1 and swann cameras
	// the stream_type and message_num do not seem to set in the official clients
	//
	// They also have extra streams
	fn test_bc_b800_mainstream() {
		init();

		let sample = include_bytes!("samples/xml_mainstream_b800.bin");

		let context = BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt);

		let e = Bc::deserialize(&context, &mut BytesMut::from(&sample[..]));
		assert_matches!(
			e,
			Ok(Bc {
				meta:
					BcMeta {
						msg_id: 3,
						channel_id: 138,
						stream_type: 0,
						response_code: 0,
						msg_num: 0,
						class: 0x6414,
					},
				body:
					BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload:
							Some(BcPayloads::BcXml(BcXml {
								preview:
									Some(Preview {
										version,
										channel_id: 0,
										handle: 0,
										stream_type,
									}),
								..
							})),
					}),
			}) if version == "1.1" && stream_type == Some("mainStream".to_string())
		);
	}

	/// Modern-class header with a payload that's not valid BcXml — the
	/// `BcXml::try_parse` branch in `bc_modern_msg` surfaces a parse
	/// error (Err::Error wrapped via map_err) rather than panicking.
	#[test]
	fn modern_msg_with_unparseable_payload_xml_returns_err() {
		init();
		// Hand-craft a 0x6414 (modern, has payload_offset) header with
		// 5 bytes of pure non-XML payload following.
		let payload = b"abcde";
		let mut buf = vec![];
		buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
		buf.extend_from_slice(&1u32.to_le_bytes()); // msg_id
		buf.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // body_len
		buf.push(0); // channel_id
		buf.push(0); // stream_type
		buf.extend_from_slice(&0u16.to_le_bytes()); // msg_num
		buf.extend_from_slice(&200u16.to_le_bytes()); // response_code
		buf.extend_from_slice(&0x6414u16.to_le_bytes()); // class
		buf.extend_from_slice(&0u32.to_le_bytes()); // payload_offset = 0
		buf.extend_from_slice(payload);

		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let e = Bc::deserialize(&context, &mut BytesMut::from(&buf[..]));
		assert!(e.is_err(), "should refuse non-XML payload");
	}

	/// Modern-class header with non-XML extension bytes — surfaces the
	/// `Extension::try_parse` Err branch (lines 122-126).
	#[test]
	fn modern_msg_with_unparseable_extension_xml_returns_err() {
		init();
		let ext = b"badext";
		let body = ext;
		let mut buf = vec![];
		buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
		buf.extend_from_slice(&1u32.to_le_bytes());
		buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
		buf.push(0);
		buf.push(0);
		buf.extend_from_slice(&0u16.to_le_bytes());
		buf.extend_from_slice(&200u16.to_le_bytes());
		buf.extend_from_slice(&0x6414u16.to_le_bytes());
		buf.extend_from_slice(&(ext.len() as u32).to_le_bytes()); // payload_offset = ext_len
		buf.extend_from_slice(body);

		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let e = Bc::deserialize(&context, &mut BytesMut::from(&buf[..]));
		assert!(e.is_err(), "should refuse non-XML extension");
	}

	/// Modern message with a binary_data=1 extension and an actual
	/// binary payload — drives the `in_binary` branch in
	/// `bc_modern_msg`.
	#[test]
	fn modern_msg_binary_extension_carries_binary_payload() {
		init();

		// Build via serialize (so the extension is well-formed XML).
		let bc = Bc::new(
			BcMeta {
				msg_id: 109,
				channel_id: 0,
				stream_type: 0,
				response_code: 200,
				msg_num: 7,
				class: 0x6414,
			},
			Some(Extension {
				binary_data: Some(1),
				..Default::default()
			}),
			Some(BcPayloads::Binary(vec![0xAA, 0xBB, 0xCC, 0xDD])),
		);
		let bytes = bc
			.serialize(vec![], &EncryptionProtocol::Unencrypted)
			.unwrap();

		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let decoded =
			Bc::deserialize(&context, &mut BytesMut::from(bytes.as_slice())).expect("decode ok");
		match decoded.body {
			BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					binary_data: Some(1),
					..
				}),
				payload: Some(BcPayloads::Binary(b)),
			}) if b == vec![0xAA, 0xBB, 0xCC, 0xDD] => {}
			_ => panic!("expected binary payload"),
		}
	}

	/// Buffer containing a valid header with `body_len = 0` but a
	/// payload-bearing class (no extension, no payload) — should yield
	/// `payload: None` cleanly via the `payload_len == 0` else arm
	/// (line 217-219).
	#[test]
	fn modern_msg_zero_body_returns_no_payload() {
		init();
		let mut buf = vec![];
		buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
		buf.extend_from_slice(&29u32.to_le_bytes()); // some msg_id
		buf.extend_from_slice(&0u32.to_le_bytes()); // body_len = 0
		buf.push(0);
		buf.push(0);
		buf.extend_from_slice(&0u16.to_le_bytes());
		buf.extend_from_slice(&200u16.to_le_bytes());
		buf.extend_from_slice(&0x6414u16.to_le_bytes());
		buf.extend_from_slice(&0u32.to_le_bytes()); // payload_offset = 0

		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let bc =
			Bc::deserialize(&context, &mut BytesMut::from(&buf[..])).expect("zero-body decodes");
		match bc.body {
			BcBody::ModernMsg(ModernMsg {
				extension: None,
				payload: None,
			}) => {}
			_ => panic!("expected empty ModernMsg"),
		}
	}

	#[test]
	// Hostile peer crafts a header with `body_len > MAX_BC_BODY_LEN`.
	// Pre-fix this would size a multi-GiB read buffer; now `verify`
	// rejects at the header parse step with a typed error.
	fn body_len_above_cap_rejected() {
		let mut buf = Vec::new();
		buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
		buf.extend_from_slice(&1u32.to_le_bytes()); // msg_id
		buf.extend_from_slice(&u32::MAX.to_le_bytes()); // body_len = 4 GiB - 1
		buf.push(0); // channel_id
		buf.push(0); // stream_type
		buf.extend_from_slice(&0u16.to_le_bytes()); // msg_num
		buf.extend_from_slice(&0u16.to_le_bytes()); // response_code
		buf.extend_from_slice(&0u16.to_le_bytes()); // class
		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let result = Bc::deserialize(&context, &mut BytesMut::from(&buf[..]));
		assert!(
			result.is_err(),
			"body_len above MAX_BC_BODY_LEN must be rejected, got {result:?}"
		);
	}

	#[test]
	// Hostile peer crafts a modern header where `payload_offset > body_len`.
	// Pre-fix this drove a u32 underflow (debug panic / release wrap to
	// ~4 GiB) inside `bc_modern_msg`; now the validation gates the
	// subtraction.
	fn payload_offset_greater_than_body_len_rejected() {
		// class = 0x6414 carries an extension (has_payload_offset → true).
		// body_len = 10, payload_offset = 100 → 100 > 10 must reject.
		let mut buf = Vec::new();
		buf.extend_from_slice(&MAGIC_HEADER.to_le_bytes());
		buf.extend_from_slice(&26u32.to_le_bytes()); // msg_id (any modern)
		buf.extend_from_slice(&10u32.to_le_bytes()); // body_len = 10
		buf.push(0); // channel_id
		buf.push(0); // stream_type
		buf.extend_from_slice(&0u16.to_le_bytes()); // msg_num
		buf.extend_from_slice(&0u16.to_le_bytes()); // response_code
		buf.extend_from_slice(&0x6414u16.to_le_bytes()); // class with extension
		buf.extend_from_slice(&100u32.to_le_bytes()); // payload_offset = 100 > body_len
		buf.extend_from_slice(&[0u8; 10][..]); // body bytes
		let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		let result = Bc::deserialize(&context, &mut BytesMut::from(&buf[..]));
		assert!(
			result.is_err(),
			"payload_offset > body_len must reject without panic, got {result:?}"
		);
	}

	// Property test: the modern Baichuan TCP parser must absorb any byte
	// sequence the network can deliver without panicking. Camera firmware
	// drift, lossy upstreams, on-path attackers, and compromised relay
	// peers all produce unexpected bytes — every one must surface as
	// `Ok` or `Err`, never panic / never hang.
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 1024,
			..ProptestConfig::default()
		})]

		#[test]
		fn bc_deserialize_never_panics_on_arbitrary_bytes(
			bytes in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
			let mut buf = BytesMut::from(&bytes[..]);
			let _ = Bc::deserialize(&context, &mut buf);
		}

		#[test]
		fn bc_deserialize_with_valid_magic_prefix_never_panics(
			use_rev in any::<bool>(),
			tail in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			// Bias toward "looks like a real header": prepend a valid
			// magic so the fuzzer walks deeper than uniform random into
			// the body / extension / payload branches.
			let magic = if use_rev { MAGIC_HEADER_REV } else { MAGIC_HEADER };
			let mut bytes = magic.to_le_bytes().to_vec();
			bytes.extend_from_slice(&tail);
			let context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
			let mut buf = BytesMut::from(&bytes[..]);
			let _ = Bc::deserialize(&context, &mut buf);
		}
	}

	/// A message with a modern header whose body is `body`, as a
	/// payload with no extension.
	fn modern_msg_bytes(body: &[u8]) -> Vec<u8> {
		let bc = Bc {
			meta: BcMeta {
				msg_id: 806,
				channel_id: 0,
				stream_type: 0,
				response_code: 200,
				msg_num: 7,
				class: 0x0000,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: None,
				payload: Some(BcPayloads::Binary(body.to_vec())),
			}),
		};
		bc.serialize(vec![], &EncryptionProtocol::Unencrypted)
			.expect("serialize ok")
	}

	/// Bytes 20..24 of a modern header: the payload offset, which is the
	/// extension's length.
	const PAYLOAD_OFFSET: std::ops::Range<usize> = 20..24;

	/// Whether `logs` shows `SECRET`, as text or as a byte dump.
	fn shows_secret(logs: &str) -> bool {
		logs.contains("SECRET")
			|| logs.contains(&format!("{:?}", b"SECRET").replace(['[', ']'], ""))
	}

	/// Deserialize `bytes` with debugging `debug`, returning whether it
	/// failed and what it logged.
	fn deserialize_logged(bytes: &[u8], debug: bool) -> (bool, String) {
		let mut context = BcContext::new_with_encryption(EncryptionProtocol::Unencrypted);
		if debug {
			context.debug_on();
		}
		let (result, logs) =
			crate::test_logs::capture(|| Bc::deserialize(&context, &mut BytesMut::from(bytes)));
		(result.is_err(), logs)
	}

	/// A payload that is not valid XML fails the message; the payload is
	/// logged only when debugging.
	#[test]
	fn malformed_payload_xml_is_an_error() {
		let bytes = modern_msg_bytes(b"<HaCfg><url>http://example.com/hook/SECRET</ur");
		assert_eq!(&bytes[PAYLOAD_OFFSET], &[0; 4]);
		for debug in [false, true] {
			let (failed, logs) = deserialize_logged(&bytes, debug);
			assert!(failed, "debug={debug}");
			assert!(logs.contains("Unable to parse payload XML"), "{logs}");
			assert_eq!(shows_secret(&logs), debug, "debug={debug}: {logs}");
		}
	}

	/// An extension that is not valid XML fails the message; the buffer
	/// is logged only when debugging.
	#[test]
	fn malformed_extension_xml_is_an_error() {
		let body = b"<Extension><userName>SECRET</user";
		let mut bytes = modern_msg_bytes(body);
		bytes[PAYLOAD_OFFSET].copy_from_slice(&(body.len() as u32).to_le_bytes());
		for debug in [false, true] {
			let (failed, logs) = deserialize_logged(&bytes, debug);
			assert!(failed, "debug={debug}");
			assert!(logs.contains("Unable to parse extension XML"), "{logs}");
			assert_eq!(shows_secret(&logs), debug, "debug={debug}: {logs}");
		}
	}
}
