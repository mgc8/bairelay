//! The camera's event announcement packet (`0x31`).
//!
//! A battery camera pushes this datagram by itself when something happens: a
//! button press ends, a recording ends, the camera is torn off the wall. It
//! arrives on the registration port, unasked, and it carries the uid in the form
//! the camera actually puts on the wire, which is shorter than the uid the
//! configuration carries.
//!
//! This module only decodes and verifies. It decides nothing and publishes
//! nothing: `decode_announcement` answers "is this an announcement, and what
//! does it say", and the caller decides what a heard announcement is worth. That
//! split is deliberate. The port that receives this packet must stay able to
//! hand it to someone else without having already acted on it, and a decoder
//! that also woke cameras or wrote to MQTT would make that impossible.
//!
//! A frame of another type is *not* consumed here: discovery (`0x3a`), ack
//! (`0x20`) and data (`0x10`) each come back as [`crate::bcudp::announcement::AnnouncementError::Magic`] so
//! the caller can still route them to the decoder that handles them.

use std::fmt;

use serde::Deserialize;

use super::{crc::calc_crc, xml_crypto::decrypt};
use crate::redact::UidTail;

/// Full magic of an announcement: packet type `0x31` plus the BcUdp prefix.
const MAGIC: u32 = 0x2a87_cf31;
/// Header bytes, before the uid field.
const HEADER: usize = 20;
/// Fixed width of the uid field on the network.
const UID_WIDTH: usize = 32;
/// The CRC sits in the four bytes between the uid and the body.
const CRC_WIDTH: usize = 4;
/// Where the body starts: below this size nothing can be an announcement.
const BODY_START: usize = HEADER + UID_WIDTH + CRC_WIDTH;
/// Reserved word at offset 12, observed constant.
const RESERVED_WORD: u32 = 0xffff_ffff;
/// Reserved word at offset 16, observed zero.
const PADDING_WORD: u32 = 0;

/// Body length cap, in bytes. Announcements observed on the wire are 11, 63 and
/// 64 bytes.
/// The length field comes off the network: without a cap, a crafted packet would
/// make this code allocate and decrypt a 4 GiB payload for nothing.
pub const MAX_BODY: usize = 256;

/// Why a frame is not a usable announcement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AnnouncementError {
	/// Frame shorter than header, uid and CRC together.
	#[error("not an announcement: {0} bytes, less than the {BODY_START}-byte frame")]
	TooShort(usize),
	/// Some other magic: a discovery `0x3a`, an ack `0x20`, a data `0x10`, or a
	/// packet that is not Baichuan at all. Those are handled elsewhere, and this
	/// decoder does not consume them.
	#[error("not an announcement: magic {0:#010x}")]
	Magic(u32),
	/// Useless claimed length: zero, above the cap, or larger than what the frame
	/// really holds.
	#[error("announcement body of {claimed} bytes does not fit (cap {MAX_BODY}, {present} bytes on the wire)")]
	Length {
		/// Body length as the header claims it.
		claimed: usize,
		/// Bytes actually present after header, uid and CRC.
		present: usize,
	},
	/// The word at offset 12 is not the one observed on the network.
	#[error("announcement reserved word at offset 12 is {0:#010x}, not ffffffff")]
	Reserved(u32),
	/// The word at offset 16 is not zero.
	#[error("announcement reserved word at offset 16 is {0:#010x}, not zero")]
	Padding(u32),
	/// Empty uid field, or one holding something other than printable ASCII.
	#[error("announcement uid is empty or not printable ascii")]
	Uid,
	/// CRC disagrees: damaged frame, or a crafted one.
	#[error("announcement crc mismatch (frame {frame_bytes:#010x}, computed {computed:#010x})")]
	Crc {
		/// CRC read from the header.
		frame_bytes: u32,
		/// CRC recomputed over the encrypted body.
		computed: u32,
	},
	/// Decoded body that is not text: wrong seed, or a sender that does not
	/// speak this dialect.
	#[error("announcement body is not valid utf-8")]
	Encoding,
	/// Textual body, but not the expected JSON object.
	#[error("announcement body is not the expected json: {0}")]
	Json(String),
}

/// What the announcement says. These are the four fields observed on the wire; a
/// firmware that adds a fifth must not cost us the event, so unknown fields are
/// ignored and all but `event` are optional: `{"event":3}` on its own is a
/// complete announcement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Payload {
	/// Event type: 3 activity ended, 4 activity ended with a duration.
	pub event: u32,
	/// Duration of the activity that just ended, in milliseconds.
	#[serde(default)]
	pub duration: Option<u64>,
	/// Running total since the camera woke, in milliseconds.
	#[serde(default)]
	pub totalduration: Option<u64>,
	/// What caused it: 3 wake on recording, 5 tamper, 19 button press. Values
	/// observed here, not a Reolink table.
	#[serde(default)]
	pub reason: Option<u32>,
}

/// A decoded and verified announcement.
///
/// `Debug` masks the uid: an announcement sometimes ends up in a log, and a full
/// uid must never get there. The whole value is on [`Announcement::uid`], to be
/// used only to match the frame against a configured camera.
#[derive(Clone, PartialEq, Eq)]
pub struct Announcement {
	/// Encryption seed of the packet: handy to spot a retransmission or compare
	/// two frames of a capture.
	tid: u32,
	uid: String,
	payload: Payload,
	json: String,
}

impl Announcement {
	/// The uid in its **short form**, the way the camera writes it on the
	/// network: 16 characters, without the firmware suffix the configured uid
	/// carries. The bridge matches this against a camera by prefix.
	pub fn uid(&self) -> &str {
		&self.uid
	}

	/// The uid in loggable form: masked, only the tail visible.
	pub fn masked_uid(&self) -> UidTail<'_> {
		UidTail(&self.uid)
	}

	/// Packet seed (`tid`).
	pub fn tid(&self) -> u32 {
		self.tid
	}

	/// The deserialized payload.
	pub fn payload(&self) -> &Payload {
		&self.payload
	}

	/// Shortcut to [`Payload::event`].
	pub fn event(&self) -> u32 {
		self.payload.event
	}

	/// Shortcut to [`Payload::reason`].
	pub fn reason(&self) -> Option<u32> {
		self.payload.reason
	}

	/// Shortcut to [`Payload::duration`].
	pub fn duration(&self) -> Option<u64> {
		self.payload.duration
	}

	/// Running duration, shortcut to [`Payload::totalduration`].
	pub fn total_duration(&self) -> Option<u64> {
		self.payload.totalduration
	}

	/// The plain JSON, as decoded: this is what gets published for diagnostics
	/// and re-read when a value surprises. It carries no identifier; the uid
	/// stays in its own field.
	pub fn json(&self) -> &str {
		&self.json
	}
}

impl fmt::Debug for Announcement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Announcement")
			.field("tid", &self.tid)
			// Deliberate: a full uid must not be able to enter a log through a
			// plain `{:?}`.
			.field("uid", &self.masked_uid().to_string())
			.field("payload", &self.payload)
			.field("json", &self.json)
			.finish()
	}
}

impl fmt::Display for Announcement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"event={} reason={:?} duration={:?}ms uid={}",
			self.event(),
			self.reason(),
			self.duration(),
			self.masked_uid()
		)
	}
}

/// Decodes an event announcement (`0x2a87cf31`): field by field, CRC verified
/// over the encrypted body, body decrypted with the same table and seed as
/// discovery, then deserialized as JSON.
///
/// A frame that is not of this type yields [`AnnouncementError::Magic`], in
/// particular a discovery, ack or data packet, which is handled elsewhere and
/// which this function does not consume.
pub fn decode_announcement(buf: &[u8]) -> Result<Announcement, AnnouncementError> {
	// The magic is judged first: a frame that is not of type `0x31` is not an
	// announcement, short or not, and the port has to be able to hand it back to
	// the other decoders with that reason.
	if buf.len() < 4 {
		return Err(AnnouncementError::TooShort(buf.len()));
	}
	let magic = little(buf, 0);
	if magic != MAGIC {
		return Err(AnnouncementError::Magic(magic));
	}
	if buf.len() < BODY_START {
		return Err(AnnouncementError::TooShort(buf.len()));
	}
	let claimed = little(buf, 4) as usize;
	let present = buf.len().saturating_sub(BODY_START);
	if claimed == 0 || claimed > MAX_BODY || claimed > present {
		return Err(AnnouncementError::Length { claimed, present });
	}
	let tid = little(buf, 8);
	let reserved = little(buf, 12);
	if reserved != RESERVED_WORD {
		return Err(AnnouncementError::Reserved(reserved));
	}
	let padding = little(buf, 16);
	if padding != PADDING_WORD {
		return Err(AnnouncementError::Padding(padding));
	}
	let uid = decode_uid(&buf[HEADER..HEADER + UID_WIDTH])?;
	let body = &buf[BODY_START..BODY_START + claimed];
	let frame_bytes = little(buf, HEADER + UID_WIDTH);
	let computed = calc_crc(body);
	if frame_bytes != computed {
		return Err(AnnouncementError::Crc { frame_bytes, computed });
	}
	// Same table, same seed as discovery: nothing is duplicated here.
	let plain = decrypt(tid, body);
	let json = String::from_utf8(plain).map_err(|_| AnnouncementError::Encoding)?;
	let payload: Payload =
		serde_json::from_str(&json).map_err(|e| AnnouncementError::Json(e.to_string()))?;
	Ok(Announcement {
		tid,
		uid,
		payload,
		json,
	})
}

/// Reads the uid field: up to the first zeroes, bounded by the field's 32 bytes,
/// printable ASCII only. A non-printable value is not a uid: it is noise, or a
/// crafted frame.
fn decode_uid(field: &[u8]) -> Result<String, AnnouncementError> {
	let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
	let raw = &field[..end];
	if raw.is_empty() || !raw.iter().all(|b| b.is_ascii_graphic() || *b == b' ') {
		return Err(AnnouncementError::Uid);
	}
	String::from_utf8(raw.to_vec()).map_err(|_| AnnouncementError::Uid)
}

/// Little-endian word at `offset`. The caller already bounded the frame.
fn little(buf: &[u8], offset: usize) -> u32 {
	u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Eight captures held inline: these are the proof of the decoder, so it must
	/// not depend on a file somewhere else.
	/// `(label, hex, uid, event, reason, duration)`
	const CAPTURES: &[(&str, &str, &str, u32, Option<u32>, Option<u64>)] = &[
		// Woken by the bridge.
		// // Captured from the wire on the operator's camera; the uid field is a synthetic
		// // stand-in of the same width, the bytes around it are the capture as it arrived.
		("A", "31cf872a0b0000008c010000ffffffff000000003935323730303053594e54485445535400000000000000000000000000000000b539c75fac1f48697cef1878ed1c6a", "9527000SYNTHTEST", 3, None, None),
		// Recording end following the press.
		("B", "31cf872a3f0000008d010000ffffffff000000003935323730303053594e54485445535400000000000000000000000000000000fc3a1132a31f48697fef1878e21b3b1a831103e3cc7250e847c3f39c75d048af10a19d63b951496a68e01833b7413502d55546b488230aaa478ba3c4328c1fa108e68f", "9527000SYNTHTEST", 4, Some(3), Some(59_439)),
		// After the press.
		("C", "31cf872a0b0000008f010000ffffffff000000003935323730303053594e5448544553540000000000000000000000000000000046c8b3cea11f486979ef1878e01c6a", "9527000SYNTHTEST", 3, None, None),
		// Press release: the reason we care about.
		("D", "31cf872a4000000090010000ffffffff000000003935323730303053594e54485445535400000000000000000000000000000000129eb05ca01f486978ef1878e11b3b1a8e1103e3cf7250e84ac3f79d7cd442af17a19d63ba51496a6fe01833b4413502d85548b6822e07aa4a8ba3c4378c1fa10fe4cb6a", "9527000SYNTHTEST", 4, Some(19), Some(18_873)),
		// End of tamper removal.
		("E", "31cf872a3f00000093010000ffffffff000000003935323730303053594e544854455354000000000000000000000000000000008b4f4b3ba51f486945ef1878e41b3b1a891103e3ca7250e849c3f79477da40af1aa19d63bf51496a52e01833b1413502df5641b48e2f06aa498ba3c4348c1fa102e08f", "9527000SYNTHTEST", 4, Some(5), Some(11_091)),
		// Press release, second capture.
		("F", "31cf872a4000000096010000ffffffff000000003935323730303053594e544854455354000000000000000000000000000000001f2924649a1f486946ef1878db1b3b1a941103e3b57250e84cc3f79c78db40af19a19d638051496a51e018338e413502c25643b7f2280faa4c8ba3c4398c1fa101e4cb6a", "9527000SYNTHTEST", 4, Some(19), Some(19_281)),
		// Tamper.
		("T", "31cf872a0b00000092010000ffffffff000000003935323730303053594e54485445535400000000000000000000000000000000ddd12fd3a61f48697aef1878e71c6a", "9527000SYNTHTEST", 3, None, None),
		// Press only, camera left asleep.
		("K", "31cf872a0b00000095010000ffffffff000000003935323730303053594e544854455354000000000000000000000000000000009765d6d59b1f486947ef1878da1c6a", "9527000SYNTHTEST", 3, None, None),
	];

	fn hex(s: &str) -> Vec<u8> {
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
			.collect()
	}

	/// Builds an announcement from scratch: the body is encrypted with the
	/// crate's own table and the CRC laid on top. Used for the rejections,
	fn build_frame(tid: u32, uid: &[u8], plain: &[u8]) -> Vec<u8> {
		let body = super::super::xml_crypto::encrypt(tid, plain);
		let mut frame = Vec::with_capacity(BODY_START + body.len());
		frame.extend_from_slice(&MAGIC.to_le_bytes());
		frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
		frame.extend_from_slice(&tid.to_le_bytes());
		frame.extend_from_slice(&RESERVED_WORD.to_le_bytes());
		frame.extend_from_slice(&PADDING_WORD.to_le_bytes());
		let mut champ = vec![0u8; UID_WIDTH];
		champ[..uid.len()].copy_from_slice(uid);
		frame.extend_from_slice(&champ);
		frame.extend_from_slice(&calc_crc(&body).to_le_bytes());
		frame.extend_from_slice(&body);
		frame
	}

	/// Capture D, a press release, used as the base for the rejections.
	fn base() -> Vec<u8> {
		hex(CAPTURES[3].1)
	}

	#[test]
	fn all_eight_captures_decode() {
		for (label, frame_bytes, uid, event, reason, duration) in CAPTURES {
			let frame = hex(frame_bytes);
			let claimed = decode_announcement(&frame).unwrap_or_else(|e| panic!("{label} rejected: {e}"));
			assert_eq!(claimed.uid(), *uid, "{label}: uid");
			assert_eq!(claimed.event(), *event, "{label}: event");
			assert_eq!(claimed.reason(), *reason, "{label}: reason");
			assert_eq!(claimed.duration(), *duration, "{label}: duration");
			// The field length really is the one the header carries.
			assert_eq!(claimed.json().len(), little(&frame, 4) as usize, "{label}");
		}
	}

	#[test]
	fn a_discovery_frame_is_never_taken_for_an_announcement() {
		// The binary judges announcements **before** discovery, so that order is only
		// safe while this decoder really refuses a discovery frame - otherwise a button
		// press and a heartbeat trade places and the press ends up in the "invalid
		// packet" line. Two arms, both pinned: the same envelope carrying a discovery
		// body, and a frame that was never an announcement to begin with.
		let discovery = b"<P2P><D2R_HB><uid>9527000SYNTHTEST</uid><token>123</token>\
			<needrsp>1</needrsp></D2R_HB></P2P>";
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", discovery);
		let err = decode_announcement(&frame).expect_err("a discovery body is not an announcement");
		assert!(
			matches!(err, AnnouncementError::Json(_)),
			"a discovery body reaches the JSON stage and is refused there: {err:?}"
		);

		let mut wrong_magic = base();
		wrong_magic[0] ^= 0x01;
		let err = decode_announcement(&wrong_magic).expect_err("not an announcement at all");
		assert!(matches!(err, AnnouncementError::Magic(_)), "the first arm is the magic: {err:?}");
	}

	#[test]
	fn the_captures_carry_the_short_uid_form() {
		// 16 visible characters, then zeroes up to 32: the firmware suffix of
		// the 20-character configured uid is not on the network.
		let frame = base();
		// The uid lives in the clear header field only: the encrypted body is keyed by `tid`,
		// and the decoded JSON below provably does not carry the uid (see the not-contains
		// assertions further down), so substituting the field rewrites no ciphertext.
		assert_eq!(&frame[20..36], b"9527000SYNTHTEST");
		assert!(frame[36..52].iter().all(|b| *b == 0));
		let claimed = decode_announcement(&frame).expect("decode");
		assert_eq!(claimed.uid().len(), 16);
		assert_eq!(claimed.tid(), 0x0190);
	}

	#[test]
	fn the_plain_json_is_exposed_without_the_uid() {
		let claimed = decode_announcement(&base()).expect("decode");
		assert_eq!(
			claimed.json(),
			r#"{"event":4,"duration":18873,"totalduration":2194958,"reason":19}"#
		);
		assert!(!claimed.json().contains("9527000SYNTHTEST"));
	}

	#[test]
	fn un_crc_faux_est_rejete() {
		let mut frame = base();
		frame[56] ^= 0x01;
		assert_eq!(
			decode_announcement(&frame),
			Err(AnnouncementError::Crc {
				frame_bytes: 0x5cb0_9e12,
				computed: calc_crc(&frame[56..]),
			})
		);
	}

	#[test]
	fn other_bcudp_packets_are_not_consumed() {
		// Real frames from the corpus: discovery (`0x3a`), ack (`0x20`), data
		// (`0x10`). Each must return `Magic`, so the port keeps handling them
		// somewhere else.
		let discovery: &[u8] = include_bytes!("samples/udp_negotiate_disc.bin");
		let ack: &[u8] = include_bytes!("samples/udp_ack.bin");
		let data: &[u8] = include_bytes!("samples/udp_data.bin");
		assert_eq!(decode_announcement(discovery), Err(AnnouncementError::Magic(0x2a87_cf3a)));
		assert_eq!(decode_announcement(ack), Err(AnnouncementError::Magic(0x2a87_cf20)));
		assert_eq!(decode_announcement(data), Err(AnnouncementError::Magic(0x2a87_cf10)));
	}

	#[test]
	fn a_non_printable_uid_is_rejected() {
		let mut frame = base();
		frame[20..52].fill(0xff);
		assert_eq!(decode_announcement(&frame), Err(AnnouncementError::Uid));
	}

	#[test]
	fn an_empty_uid_is_rejected() {
		let mut frame = base();
		frame[20..52].fill(0);
		assert_eq!(decode_announcement(&frame), Err(AnnouncementError::Uid));
	}

	#[test]
	fn a_uid_field_is_cut_at_its_32_bytes() {
		let mut frame = base();
		frame[20..52].fill(b'X');
		let claimed = decode_announcement(&frame).expect("decode");
		assert_eq!(claimed.uid().len(), 32);
	}

	#[test]
	fn a_body_that_is_not_json_is_rejected_without_panicking() {
		// Body with a correct CRC, decryptable, but not JSON.
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", b"this is not json at all");
		let err = decode_announcement(&frame).expect_err("must reject");
		assert!(matches!(err, AnnouncementError::Json(_)), "{err}");
		// Body that is not even text.
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", &[0xffu8; 12]);
		assert_eq!(decode_announcement(&frame), Err(AnnouncementError::Encoding));
		// A JSON object without `event` is not an announcement either.
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", br#"{"autre":1}"#);
		assert!(matches!(decode_announcement(&frame), Err(AnnouncementError::Json(_))));
	}

	#[test]
	fn an_oversized_body_is_rejected() {
		// 300 bytes: above our cap, but still within what a UDP datagram can
		// carry. The decoder must not even read it.
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", &[b'x'; 300]);
		assert_eq!(
			decode_announcement(&frame),
			Err(AnnouncementError::Length {
				claimed: 300,
				present: 300
			})
		);
		// And a lying length, larger than the frame.
		let mut frame = base();
		frame[4..8].copy_from_slice(&4096u32.to_le_bytes());
		assert_eq!(
			decode_announcement(&frame),
			Err(AnnouncementError::Length {
				claimed: 4096,
				present: 64
			})
		);
		// The cap itself passes: 256 bytes decrypt.
		let prefix = r#"{"event":4,"x":""#;
		let suffix = r#""}"#;
		let plain = format!(
			"{prefix}{}{suffix}",
			"y".repeat(MAX_BODY - prefix.len() - suffix.len())
		);
		assert_eq!(plain.len(), MAX_BODY);
		let frame = build_frame(0x1234, b"9527000SYNTHTEST", plain.as_bytes());
		assert_eq!(decode_announcement(&frame).expect("256 bytes pass").event(), 4);
	}

	#[test]
	fn the_reserved_words_are_checked() {
		let mut frame = base();
		frame[12..16].copy_from_slice(&0u32.to_le_bytes());
		assert_eq!(decode_announcement(&frame), Err(AnnouncementError::Reserved(0)));
		let mut frame = base();
		frame[16..20].copy_from_slice(&1u32.to_le_bytes());
		assert_eq!(decode_announcement(&frame), Err(AnnouncementError::Padding(1)));
	}

	#[test]
	fn a_truncated_frame_is_rejected() {
		assert_eq!(decode_announcement(&[]), Err(AnnouncementError::TooShort(0)));
		assert_eq!(decode_announcement(&[0x31, 0xcf, 0x87]), Err(AnnouncementError::TooShort(3)));
		// Right magic, but truncated before the header ends.
		let mut frame = base();
		frame.truncate(BODY_START - 1);
		assert_eq!(
			decode_announcement(&frame),
			Err(AnnouncementError::TooShort(BODY_START - 1))
		);
	}

	#[test]
	fn the_debug_shows_only_the_uid_tail() {
		let claimed = decode_announcement(&base()).expect("decode");
		let rendu = format!("{claimed:?}");
		assert!(
			!rendu.contains("9527000SYNTHTEST"),
			"a full uid leaked into Debug: {rendu}"
		);
		assert!(rendu.contains("**********THTEST"), "{rendu}");
	}

	#[test]
	fn the_display_shows_only_the_uid_tail() {
		let claimed = decode_announcement(&base()).expect("decode");
		assert_eq!(
			claimed.to_string(),
			"event=4 reason=Some(19) duration=Some(18873)ms uid=**********THTEST"
		);
	}
}
