//! RTCP Sender Report builder per RFC 3550 §6.4.1.
//!
//! Sender Reports carry an NTP timestamp + RTP timestamp correspondence
//! that lets receivers synchronise media and compute drift. **Periodic
//! SR is intentionally disabled in production** (Phase 3D — every SR
//! receipt re-anchored mpv/ffmpeg's decode clock and surfaced as a
//! brief A-V hitch every `SR_INTERVAL`). Receivers fall back to
//! RTP-arrival-time sync, which at our sub-second live-camera latency
//! is indistinguishable. The helpers below remain for any future
//! SR-emitting context (e.g. a recording sink that needs precise
//! NTP↔RTP). The disabled `sr_ticker` arm in
//! `crate::server::session_task::run` is the single point of toggle.
//!
//! **Why NTP and RTP are both caller-supplied.** RFC 3550 §6.4.1 is
//! explicit: the NTP and RTP timestamps in an SR MUST correspond to the
//! same instant ("Rather, it MUST be calculated from the corresponding
//! NTP timestamp using the relationship between the RTP timestamp
//! counter and real time as maintained by periodically checking the
//! wallclock time at a sampling instant.").
//!
//! Earlier revisions sampled `ntp_now()` inside this function while the
//! caller passed `rtp_timestamp = last_rtp_ts` — the RTP timestamp of
//! the last dispatched packet, which lagged the SR fire instant by up to
//! one frame period (≤ 64 ms for AAC-LC at 16 kHz). ffmpeg uses the
//! SR's (NTP, RTP) pair to calibrate its decode clock; inconsistent
//! pairs surface as `non monotonically increasing dts` warnings every SR
//! interval. The post-Phase-2E fix takes a different tack: the session
//! task records `last_rtp_ts` alongside a monotonic `Instant`, and at
//! SR fire time extrapolates forward to "now" using the track's clock
//! rate — so the SR reports the (NTP, RTP) pair that would correspond
//! to the same "now" a next-arriving frame would extend. See
//! `docs/implementation.md` for the investigation log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// RTCP packet type: Sender Report.
pub const PT_SENDER_REPORT: u8 = 200;

/// Build a 28-byte minimal Sender Report (no RR blocks).
///
/// - `ssrc` — the stream's RTP SSRC.
/// - `ntp_sec` / `ntp_frac` — the NTP wall-clock instant that corresponds
///   to `rtp_timestamp`. Callers MUST sample both at the same instant
///   (see module docs).
/// - `rtp_timestamp` — the RTP timestamp corresponding to that instant
///   (in the media's clock rate).
/// - `packet_count` / `octet_count` — totals since stream start.
pub fn build_sender_report(
	ssrc: u32,
	ntp_sec: u32,
	ntp_frac: u32,
	rtp_timestamp: u32,
	packet_count: u32,
	octet_count: u32,
) -> [u8; 28] {
	let mut buf = [0u8; 28];
	// Byte 0: V=2, P=0, RC=0 → 0x80
	buf[0] = 0x80;
	// Byte 1: PT = 200
	buf[1] = PT_SENDER_REPORT;
	// Bytes 2-3: length in 32-bit words minus 1 → (28/4)-1 = 6
	buf[2..4].copy_from_slice(&6u16.to_be_bytes());
	// Bytes 4-7: SSRC
	buf[4..8].copy_from_slice(&ssrc.to_be_bytes());
	// Bytes 8-11: NTP seconds
	buf[8..12].copy_from_slice(&ntp_sec.to_be_bytes());
	// Bytes 12-15: NTP fraction
	buf[12..16].copy_from_slice(&ntp_frac.to_be_bytes());
	// Bytes 16-19: RTP timestamp
	buf[16..20].copy_from_slice(&rtp_timestamp.to_be_bytes());
	// Bytes 20-23: packet count
	buf[20..24].copy_from_slice(&packet_count.to_be_bytes());
	// Bytes 24-27: octet count
	buf[24..28].copy_from_slice(&octet_count.to_be_bytes());
	buf
}

/// Recommended interval between Sender Reports.
pub const SR_INTERVAL: Duration = Duration::from_secs(5);

/// Subtract a wall-clock `Duration` from an NTP `(sec, frac)` pair.
///
/// Used by the session task to translate "NTP at now" into "NTP at the
/// instant we last dispatched a packet" without re-sampling
/// `SystemTime::now()`. Pairing the SR's NTP with the actual dispatch
/// instant (instead of "now" + an extrapolated RTP) keeps (NTP, RTP)
/// referring to the *same* instant per RFC 3550 §6.4.1, with zero
/// extrapolation error — the receiver's slope between successive SRs
/// is then exactly the per-packet emission slope, which the audio
/// pacer holds at `clock_rate`.
pub fn ntp_minus(ntp_sec: u32, ntp_frac: u32, dt: Duration) -> (u32, u32) {
	let dt_sec = dt.as_secs() as u32;
	// Convert dt's sub-second part to NTP fraction units (Q32 of a second).
	let dt_frac = ((dt.subsec_nanos() as u64 * (1u64 << 32)) / 1_000_000_000) as u32;
	let (frac, borrow) = ntp_frac.overflowing_sub(dt_frac);
	let sec = ntp_sec.wrapping_sub(dt_sec);
	let sec = if borrow { sec.wrapping_sub(1) } else { sec };
	(sec, frac)
}

/// Wall-clock sampled as `(ntp_seconds, ntp_fraction)`. Callers sample
/// this at the same instant they capture a frame's RTP timestamp so the
/// pair can be passed into [`build_sender_report`] preserving the RFC
/// 3550 §6.4.1 "same instant" invariant.
pub fn ntp_now() -> (u32, u32) {
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default();
	// NTP epoch = 1900-01-01; Unix epoch = 1970-01-01. Offset = 2_208_988_800 s.
	const NTP_OFFSET: u64 = 2_208_988_800;
	let secs = now.as_secs() + NTP_OFFSET;
	let frac = ((now.subsec_nanos() as u64 * (1u64 << 32)) / 1_000_000_000) as u32;
	(secs as u32, frac)
}

// ------------------------------------------------------------------
// Inbound RTCP: recognising a client's request for a key frame
// ------------------------------------------------------------------

/// RTCP packet type: Receiver Report (RFC 3550 §6.4.2). Only ever inbound
/// here — this server does not emit reports — the constant exists so
/// [`find_keyframe_request`] can name what it steps over.
pub const PT_RECEIVER_REPORT: u8 = 201;

/// RTCP packet type: transport-layer feedback, RTPFB (RFC 4585 §6.1). Its
/// only registered FMT today is 1 = Generic NACK (RFC 4585 §6.2.1), which
/// asks for specific sequence numbers back. This server keeps no
/// retransmission ring buffer, so a NACK is not a key frame request and is
/// ignored like any other unrecognised packet.
pub const PT_RTPFB: u8 = 205;

/// RTCP packet type: payload-specific feedback, PSFB (RFC 4585 §6.1). Both
/// requests acted on below are PSFB packets told apart by their FMT field.
pub const PT_PSFB: u8 = 206;

/// PSFB FMT 1 — Picture Loss Indication (RFC 4585 §6.3.1): 12 octets,
/// "I lost packets, my decoder needs a new reference picture". Every WebRTC
/// stack sends these on loss, which is how a phone watching go2rtc tells us
/// its picture is dead.
pub const PSFB_FMT_PLI: u8 = 1;

/// PSFB FMT 4 — Full Intra Request command (RFC 5104 §4.3.1). Its `length`
/// is 2 + 2·N words, N being the number of FCI entries (§4.3.1.1), so the
/// single-entry message a client actually sends is 20 octets: common
/// feedback header, sender SSRC, media-source SSRC (unused in FIR, set to
/// zero per §4.3.1.2 — the target lives in the FCI), then the FCI entry
/// naming the media sender, its command sequence number and 3 reserved
/// octets. RFC 5104's IANA section registered FMT 4, which RFC 4585 §6.1 had
/// left unassigned, so the pair to remember is PT 206 with FMT 4 — not PT 205
/// (the transport-layer class, Generic NACK), and not the historic PT 192
/// that RFC 2032 used for H.261/H.263 intra requests.
pub const PSFB_FMT_FIR: u8 = 4;

/// Common RTCP header size: V/P/RC-or-FMT, PT, length (RFC 3550 §6.1).
const RTCP_HEADER_BYTES: usize = 4;

/// RTCP fields are 32-bit aligned: a packet's `length` counts 32-bit words
/// minus one, so every packet size is a multiple of this.
const RTCP_WORD_BYTES: usize = 4;

/// Value of the version field in every conformant RTCP packet.
const RTCP_VERSION: u8 = 2;

/// A client's request for a fresh key frame, as recognised in inbound RTCP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyframeRequest {
	/// PLI — loss-driven "my decoder lost its reference" (RFC 4585 §6.3.1).
	Pli,
	/// FIR — explicit "send me a decoder refresh point" (RFC 5104 §4.3.1).
	Fir,
}

impl KeyframeRequest {
	/// Lowercase label for log lines.
	pub fn label(self) -> &'static str {
		match self {
			Self::Pli => "pli",
			Self::Fir => "fir",
		}
	}
}

/// Scan one inbound RTCP compound packet for a key frame request.
///
/// RTCP is compound: one UDP datagram, or one `$`-framed interleaved packet
/// on the control connection, carries several RTCP packets back to back
/// (RFC 3550 §6.1). This walks the buffer using each packet's own `length`
/// field — which already covers padding, so the P bit needs no special
/// handling — and returns the first PLI or FIR it finds. Everything else
/// (SR, RR, SDES, BYE, NACK, SLI, XR, APP, …) is stepped over in silence:
/// a receiver report arrives about once a second per session and must not
/// reach the journal.
///
/// Deliberately NOT checked: the two SSRC fields. A PLI's media source ID
/// ought to name our video SSRC, but an intermediary — Home Assistant's
/// go2rtc in the deployment this exists for — may forward the request with
/// SSRC values of its own (RFC 5104 §4.3.1.4 even tells a translator to
/// issue its own FIR rather than forward one). Rejecting on a mismatch would
/// silently reproduce the frozen-picture bug; accepting an unsolicited
/// request costs at most one rate-limited burst replay, see
/// [`crate::server::registry::KeyframeGate`].
///
/// Malformed input (a header cut short, a `length` running past the buffer,
/// a version field that is not 2) stops the walk: guessing at a boundary we
/// cannot see would risk reading a media payload as control data.
pub fn find_keyframe_request(buf: &[u8]) -> Option<KeyframeRequest> {
	let mut off = 0usize;
	while buf.len() - off >= RTCP_HEADER_BYTES {
		let first = buf[off];
		let version = first >> 6;
		let fmt = first & 0x1f;
		let pt = buf[off + 1];
		let words = u16::from_be_bytes([buf[off + 2], buf[off + 3]]) as usize;
		// `length` excludes the common header, so the packet occupies
		// (length + 1) words in total.
		let packet_bytes = (words + 1) * RTCP_WORD_BYTES;
		if buf.len() - off < packet_bytes {
			return None;
		}
		if version == RTCP_VERSION && pt == PT_PSFB {
			match fmt {
				PSFB_FMT_PLI => return Some(KeyframeRequest::Pli),
				PSFB_FMT_FIR => return Some(KeyframeRequest::Fir),
				_ => {}
			}
		}
		off += packet_bytes;
	}
	None
}

// ------------------------------------------------------------------
// Inbound RTCP: making a silent RTCP path readable
// ------------------------------------------------------------------

/// RTCP packet type: Source Description, SDES (RFC 3550 §6.1). Named so the
/// arrival line can say `SDES` instead of a bare 202.
pub const PT_SOURCE_DESCRIPTION: u8 = 202;

/// RTCP packet type: BYE (RFC 3550 §6.1) — a source is leaving.
pub const PT_END_OF_SESSION: u8 = 203;

/// RTCP packet type: application-defined, APP (RFC 3550 §6.1).
pub const PT_APP: u8 = 204;

/// RTCP packet type: extended reports, XR (RFC 3611 §4.1).
pub const PT_EXT_REPORT: u8 = 207;

/// RTPFB FMT 1 — Generic NACK (RFC 4585 §6.2.1). Named for the arrival line
/// only: with no retransmission buffer here a NACK stays as unrecognised to
/// [`find_keyframe_request`] as any other packet.
pub const RTPFB_FMT_NACK: u8 = 1;

/// PSFB FMT 2 — Slice Loss Indication (RFC 4585 §6.3.2). Named for the arrival
/// line, and likewise no key frame request.
pub const PSFB_FMT_SLI: u8 = 2;

/// What an inbound RTCP packet says it is, as far as the first two octets of
/// the common header reveal: the FMT/RC nibble and the payload type
/// (RFC 3550 §6.1).
///
/// Deliberately not the walk [`find_keyframe_request`] does. This reads the
/// head of a received buffer to *describe* what landed, so it has to work on
/// junk as well as on well-formed packets, while the parser that *acts* on the
/// content stays exactly as strict as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtcpClass {
	/// FMT on a feedback message, RC on a report: the low 5 bits of octet 0.
	pub fmt: u8,
	/// The payload type octet.
	pub pt: u8,
}

impl RtcpClass {
	/// Class of the first packet in a received buffer, or `None` when the buffer
	/// is too short to hold a payload type octet. A runt datagram on the RTCP
	/// port is still an arrival, so it is still counted — it simply has no class
	/// to name.
	///
	/// The version field is not checked here on purpose: a buffer failing it is
	/// exactly the traffic this trace exists to make visible.
	pub fn first_of(buf: &[u8]) -> Option<Self> {
		(buf.len() >= 2).then(|| Self { fmt: buf[0] & 0x1f, pt: buf[1] })
	}

	/// `200(SR)`, `206(PLI)` — or, for an octet this module does not know,
	/// `other<250>`. The number is always rendered; a name is added only when it
	/// is earned, so the journal never invents a meaning for a byte.
	pub fn label(&self) -> String {
		match rtcp_packet_name(self.pt, self.fmt) {
			Some(name) => format!("{}({name})", self.pt),
			None => format!("other<{}>", self.pt),
		}
	}
}

/// Conventional name of an RTCP packet class, from its payload type plus the
/// FMT nibble that separates the types sharing one payload type.
///
/// PSFB (206) is what forces the second argument: PLI, SLI and FIR are one and
/// the same payload type, and FMT is the only thing that tells them apart.
pub fn rtcp_packet_name(pt: u8, fmt: u8) -> Option<&'static str> {
	match pt {
		PT_SENDER_REPORT => Some("SR"),
		PT_RECEIVER_REPORT => Some("RR"),
		PT_SOURCE_DESCRIPTION => Some("SDES"),
		PT_END_OF_SESSION => Some("BYE"),
		PT_APP => Some("APP"),
		PT_RTPFB if fmt == RTPFB_FMT_NACK => Some("NACK"),
		PT_RTPFB => Some("RTPFB"),
		PT_PSFB if fmt == PSFB_FMT_PLI => Some("PLI"),
		PT_PSFB if fmt == PSFB_FMT_SLI => Some("SLI"),
		PT_PSFB if fmt == PSFB_FMT_FIR => Some("FIR"),
		PT_PSFB => Some("PSFB"),
		PT_EXT_REPORT => Some("XR"),
		_ => None,
	}
}

/// Render the fields of the one `keyframe-rtcp` line a session gets: the
/// transport the first packet arrived on, that packet's class, its length in
/// octets, and how many RTCP arrivals the session has counted by now.
///
/// Pure, so the exact journal text is pinned by a test instead of by someone
/// grepping a live log. The `keyframe-rtcp` keyword and the `session=` field
/// come from the caller, which is where the other `keyframe-*` lines get
/// theirs.
pub fn describe_first_rtcp_packet(
	transport: &str,
	class: Option<RtcpClass>,
	len: usize,
	total: u64,
) -> String {
	let class_label = match class {
		Some(class) => class.label(),
		None => "none".to_string(),
	};
	format!(
		"stage=first transport={transport} pt={class_label} len={len} rtcp_total={total}"
	)
}

/// Per-session RTCP arrival counter behind that line.
///
/// Lives on [`crate::server::registry::KeyframeGate`], one per video session:
/// both inbound paths — the `$`-framed RTCP channel on TCP, and the UDP RTCP
/// reader — hand every packet they read to the gate. Counting there therefore
/// covers both transports, cannot double-count a session, and adds no plumbing
/// of its own.
#[derive(Default)]
pub struct InboundRtcpTrace {
	/// Arrivals so far, whatever they carried: one per buffer handed in, not one
	/// per RTCP packet inside a compound buffer.
	arrived: AtomicU64,
}

impl InboundRtcpTrace {
	/// Record one inbound RTCP arrival on `transport`, and return the line to
	/// log — on the first arrival only.
	///
	/// `None` from the second packet onwards: a streaming client sends a receiver
	/// report about once a second, and one line per report is exactly the journal
	/// noise this crate refuses everywhere else. Returning the string rather than
	/// logging it keeps the once-only rule testable without a subscriber, and
	/// leaves the level and the prefix with the caller.
	pub fn note(&self, transport: &str, packet: &[u8]) -> Option<String> {
		let arrived = self.arrived.fetch_add(1, Ordering::Relaxed).saturating_add(1);
		if arrived != 1 {
			return None;
		}
		Some(describe_first_rtcp_packet(
			transport,
			RtcpClass::first_of(packet),
			packet.len(),
			arrived,
		))
	}

	/// Arrivals so far. The line above prints this number as `rtcp_total`; it
	/// keeps counting after that line, so a session-end summary can print the
	/// real total later without touching this counter again.
	pub fn arrived(&self) -> u64 {
		self.arrived.load(Ordering::Relaxed)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	// ============ Inbound RTCP: key frame request recognition ============

	/// Hand-built PLI (RFC 4585 §6.3.1, 12 octets): header with V=2, FMT=1,
	/// PT=206, length=2, then sender SSRC and media source SSRC.
	fn pli_packet(sender: u32, media_ssrc: u32) -> Vec<u8> {
		let mut p = vec![0x81, PT_PSFB, 0x00, 0x02];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&media_ssrc.to_be_bytes());
		p
	}

	/// Hand-built single-FCI FIR (RFC 5104 §4.3.1.1, 20 octets, length=4):
	/// header with V=2, FMT=4, PT=206; sender SSRC; the media-source SSRC
	/// that FIR leaves zero; then the FCI entry naming `target`, its command
	/// sequence number and three reserved octets.
	fn fir_packet(sender: u32, target: u32, seq: u8) -> Vec<u8> {
		let mut p = vec![0x84, PT_PSFB, 0x00, 0x04];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&0u32.to_be_bytes());
		p.extend_from_slice(&target.to_be_bytes());
		p.push(seq);
		p.extend_from_slice(&[0x00, 0x00, 0x00]);
		p
	}

	/// Hand-built RR with one report block (RFC 3550 §6.4.2, 32 octets).
	/// Note its first octet is 0x81 — byte for byte the same as a PLI's,
	/// because RC=1 and FMT=1 both occupy the same 5 bits. PT alone tells
	/// them apart, which is exactly the distinction that must hold.
	fn rr_packet(sender: u32, reported: u32) -> Vec<u8> {
		let mut p = vec![0x81, PT_RECEIVER_REPORT, 0x00, 0x07];
		p.extend_from_slice(&sender.to_be_bytes());
		p.extend_from_slice(&reported.to_be_bytes());
		// fraction lost, cumulative loss, extended seq, jitter, LSR, DLSR:
		// the receiver has nothing to report.
		p.extend_from_slice(&[0u8; 20]);
		p
	}

	#[test]
	fn pli_is_recognised_as_keyframe_request() {
		let p = pli_packet(0x1122_3344, 0xdead_beef);
		assert_eq!(p.len(), 12);
		assert_eq!(find_keyframe_request(&p), Some(KeyframeRequest::Pli));
	}

	#[test]
	fn fir_is_recognised_as_keyframe_request() {
		let p = fir_packet(0x1122_3344, 0xdead_beef, 7);
		assert_eq!(p.len(), 20);
		assert_eq!(u16::from_be_bytes([p[2], p[3]]), 4, "length = 2 + 2*1");
		assert_eq!(find_keyframe_request(&p), Some(KeyframeRequest::Fir));
	}

	#[test]
	fn receiver_report_is_not_a_keyframe_request() {
		let rr = rr_packet(1, 2);
		assert_eq!(rr[0], 0x81, "same first octet as a PLI by construction");
		assert_eq!(rr.len(), 32);
		assert_eq!(find_keyframe_request(&rr), None);
	}

	#[test]
	fn sender_report_is_not_a_keyframe_request() {
		// Feed the parser this module's own SR builder: 28 octets, PT 200.
		let sr = build_sender_report(0xaabb_ccdd, 1, 2, 3, 4, 5).to_vec();
		assert_eq!(sr.len(), 28);
		assert_eq!(find_keyframe_request(&sr), None);
	}

	#[test]
	fn compound_packet_walks_sr_and_rr_before_finding_pli() {
		// One datagram, three packets: the request is last, so a parser that
		// only looked at the head of the buffer would miss it.
		let mut compound = build_sender_report(1, 2, 3, 4, 5, 6).to_vec();
		compound.extend_from_slice(&rr_packet(1, 2));
		compound.extend_from_slice(&pli_packet(1, 2));
		assert_eq!(compound.len(), 28 + 32 + 12);
		assert_eq!(
			find_keyframe_request(&compound),
			Some(KeyframeRequest::Pli),
			"the walk must reach the third packet"
		);
	}

	#[test]
	fn nack_and_sli_are_not_keyframe_requests() {
		// RTPFB (205) FMT 1 = Generic NACK: a retransmission ask we cannot
		// serve (no send buffer), and not a key frame request. PSFB FMT 2 =
		// SLI: slice loss. Both must be stepped over.
		let nack = vec![0x81, PT_RTPFB, 0x00, 0x03, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0];
		assert_eq!(nack.len(), 16);
		assert_eq!(find_keyframe_request(&nack), None);
		let sli = vec![0x82, PT_PSFB, 0x00, 0x02, 0, 0, 0, 1, 0, 0, 0, 2];
		assert_eq!(find_keyframe_request(&sli), None);
	}

	#[test]
	fn padding_is_skipped_by_the_length_field() {
		// RFC 3550 §6.1: with P set the last octet counts padding bytes, and
		// `length` already includes them. A padded RR followed by a PLI must
		// still yield the PLI.
		let mut padded = rr_packet(1, 2);
		padded[0] |= 0x20;
		padded.extend_from_slice(&[0u8; 7]);
		padded.push(8);
		padded[2..4].copy_from_slice(&9u16.to_be_bytes());
		assert_eq!(padded.len(), 40);
		let mut compound = padded;
		compound.extend_from_slice(&pli_packet(1, 2));
		assert_eq!(find_keyframe_request(&compound), Some(KeyframeRequest::Pli));
	}

	#[test]
	fn malformed_input_is_ignored_and_never_panics() {
		assert_eq!(find_keyframe_request(&[]), None);
		assert_eq!(find_keyframe_request(&[0x81, PT_PSFB]), None);
		// Length claims 64 words; only 8 octets are there.
		assert_eq!(find_keyframe_request(&[0x81, PT_PSFB, 0x00, 0x40, 0, 0, 0, 1]), None);
		let mut wrong_version = pli_packet(1, 2);
		wrong_version[0] &= 0x3f;
		assert_eq!(find_keyframe_request(&wrong_version), None, "V must be 2");
	}


	// ============ Inbound RTCP: the once-per-session arrival line ============

	/// This module's own SR builder, as a received buffer: 28 octets, PT 200.
	fn sr_bytes() -> Vec<u8> {
		build_sender_report(1, 2, 3, 4, 5, 6).to_vec()
	}

	#[test]
	fn sender_report_and_receiver_report_are_named_in_the_line() {
		// The two packets a streaming client actually sends, and the only evidence
		// the operator can get that the RTCP path carries anything at all. Both must
		// be named, not merely counted: `pt=201` alone makes a reader look things up.
		let sr = sr_bytes();
		let line = describe_first_rtcp_packet("udp", RtcpClass::first_of(&sr), sr.len(), 1);
		assert_eq!(line, "stage=first transport=udp pt=200(SR) len=28 rtcp_total=1");
		let rr = rr_packet(1, 2);
		let line = describe_first_rtcp_packet(
			"tcp-interleaved",
			RtcpClass::first_of(&rr),
			rr.len(),
			1,
		);
		assert_eq!(
			line,
			"stage=first transport=tcp-interleaved pt=201(RR) len=32 rtcp_total=1"
		);
	}

	#[test]
	fn pli_and_fir_are_named_from_fmt_and_not_from_pt_alone() {
		// PLI and FIR share PT 206; only the FMT nibble separates them. The line has
		// to say which one arrived — telling those two apart is the whole question.
		let pli = pli_packet(1, 2);
		let fir = fir_packet(1, 2, 7);
		let pli_class = RtcpClass::first_of(&pli).expect("12 octets");
		let fir_class = RtcpClass::first_of(&fir).expect("20 octets");
		assert_eq!(pli_class.pt, fir_class.pt, "PLI and FIR are both PSFB");
		assert_eq!(pli_class.fmt, PSFB_FMT_PLI);
		assert_eq!(fir_class.fmt, PSFB_FMT_FIR);
		assert_eq!(pli_class.label(), "206(PLI)");
		assert_eq!(fir_class.label(), "206(FIR)");
	}

	#[test]
	fn nack_is_named_while_other_rtpfb_falls_back_to_the_class() {
		let nack = [0x81, PT_RTPFB, 0x00, 0x03, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0];
		assert_eq!(
			RtcpClass::first_of(&nack).expect("16 octets").label(),
			"205(NACK)"
		);
		let other_rtpfb = [0x83, PT_RTPFB, 0x00, 0x02, 0, 0, 0, 1];
		assert_eq!(
			RtcpClass::first_of(&other_rtpfb).expect("8 octets").label(),
			"205(RTPFB)"
		);
	}

	#[test]
	fn unknown_payload_type_renders_other_and_never_panics() {
		// Unassigned octets, including the historic H.261/H.263 intra-request PT
		// that FIR does not use. The line must carry the number and admit that it
		// knows no class: an invented name would send the operator hunting one.
		for pt in [250u8, 0, 1, 127, 192, 255] {
			let buf = [0x80, pt, 0x00, 0x02, 0, 0, 0, 1];
			let class = RtcpClass::first_of(&buf).expect("8 octets");
			assert_eq!(rtcp_packet_name(pt, 0), None, "pt {pt} must have no name");
			assert_eq!(class.label(), format!("other<{pt}>"));
			let line = describe_first_rtcp_packet("udp", Some(class), buf.len(), 7);
			assert!(line.contains(&format!("pt=other<{pt}>")), "{line}");
		}
	}

	#[test]
	fn runt_buffer_is_counted_without_a_class() {
		// Fewer than two octets: nothing to name, but the arrival still happened and
		// still has to show up. An unnamed arrival and no arrival at all are the two
		// facts this trace exists to tell apart.
		assert_eq!(RtcpClass::first_of(&[]), None);
		assert_eq!(RtcpClass::first_of(&[0x81]), None);
		let line = describe_first_rtcp_packet("udp", None, 1, 1);
		assert_eq!(line, "stage=first transport=udp pt=none len=1 rtcp_total=1");
	}

	#[test]
	fn trace_describes_only_the_first_arrival() {
		let trace = InboundRtcpTrace::default();
		assert_eq!(trace.arrived(), 0, "a silent session has counted nothing");
		let first = trace
			.note("udp", &sr_bytes())
			.expect("the first arrival gets a line");
		assert!(first.contains("pt=200(SR)"), "{first}");
		// Receiver reports every second from here on: no further line, while the
		// count keeps moving for whoever prints a session total later.
		let rr = rr_packet(1, 2);
		for _ in 0..5 {
			assert!(
				trace.note("udp", &rr).is_none(),
				"one line per session, not per packet"
			);
		}
		assert_eq!(trace.arrived(), 6, "the counter keeps counting");
	}

	#[test]
	fn trace_names_the_transport_it_was_handed() {
		// Both transports reach the same gate, so the line has to say which one the
		// client used: a browser behind go2rtc over TCP looks nothing like an
		// ffmpeg client that negotiated UDP.
		let tcp = InboundRtcpTrace::default();
		let udp = InboundRtcpTrace::default();
		let sr = sr_bytes();
		let line = tcp.note("tcp-interleaved", &sr).expect("first");
		assert!(line.contains("transport=tcp-interleaved"), "{line}");
		let line = udp.note("udp", &sr).expect("first");
		assert!(line.contains("transport=udp"), "{line}");
	}

	#[test]
	fn sender_report_is_28_bytes_version_2_pt_200() {
		let sr = build_sender_report(0xDEADBEEF, 0x1234_5678, 0x9ABC_DEF0, 90_000, 100, 14_400);
		assert_eq!(sr.len(), 28);
		assert_eq!(sr[0], 0x80);
		assert_eq!(sr[1], 200);
		// Length = 6 words
		assert_eq!(u16::from_be_bytes([sr[2], sr[3]]), 6);
	}

	#[test]
	fn sender_report_fields_roundtrip() {
		let sr = build_sender_report(0xAABBCCDD, 0x1111_2222, 0x3333_4444, 123_456, 42, 8_400);
		assert_eq!(u32::from_be_bytes([sr[4], sr[5], sr[6], sr[7]]), 0xAABBCCDD);
		// NTP seconds and fraction must be the caller-supplied values,
		// not sampled inside build_sender_report — this is the whole
		// point of the signature change. See module docs.
		assert_eq!(
			u32::from_be_bytes([sr[8], sr[9], sr[10], sr[11]]),
			0x1111_2222,
			"NTP seconds must come from caller, not from now()"
		);
		assert_eq!(
			u32::from_be_bytes([sr[12], sr[13], sr[14], sr[15]]),
			0x3333_4444,
			"NTP fraction must come from caller, not from now()"
		);
		assert_eq!(
			u32::from_be_bytes([sr[16], sr[17], sr[18], sr[19]]),
			123_456
		);
		assert_eq!(u32::from_be_bytes([sr[20], sr[21], sr[22], sr[23]]), 42);
		assert_eq!(u32::from_be_bytes([sr[24], sr[25], sr[26], sr[27]]), 8_400);
	}

	#[test]
	fn ntp_now_seconds_are_after_ntp_epoch() {
		let (secs, _) = ntp_now();
		// Sanity: 2026 is well past 1900+2_208_988_800 = 2070 NTP-sec ≈ 2^31.
		assert!(secs > 2_208_988_800);
	}

	#[test]
	fn ntp_minus_subtracts_whole_seconds() {
		// Whole-second delta with no sub-second component → sec drops by
		// the duration, frac unchanged.
		let (sec, frac) = ntp_minus(1000, 0x8000_0000, Duration::from_secs(3));
		assert_eq!(sec, 997);
		assert_eq!(frac, 0x8000_0000);
	}

	#[test]
	fn ntp_minus_subtracts_subsecond_without_borrow() {
		// 500 ms = 0.5 in NTP fraction = 0x8000_0000. Subtracting from
		// 0xC000_0000 leaves 0x4000_0000, no borrow into seconds.
		let (sec, frac) = ntp_minus(1000, 0xC000_0000, Duration::from_millis(500));
		assert_eq!(sec, 1000);
		assert_eq!(frac, 0x4000_0000);
	}

	#[test]
	fn ntp_minus_borrows_into_seconds_when_frac_underflows() {
		// 500 ms subtracted from 0x4000_0000 (0.25) underflows → frac
		// wraps high (0xC000_0000) and sec borrows one extra.
		let (sec, frac) = ntp_minus(1000, 0x4000_0000, Duration::from_millis(500));
		assert_eq!(sec, 999);
		assert_eq!(frac, 0xC000_0000);
	}

	#[test]
	fn ntp_minus_handles_combined_seconds_and_subsecond() {
		// 2.75 s = 2 s + 0xC000_0000 frac. Subtract from
		// (1000, 0x4000_0000) → frac borrows once (becomes 0x8000_0000),
		// seconds decrement by 2 + 1 = 3 → 997.
		let (sec, frac) = ntp_minus(1000, 0x4000_0000, Duration::from_millis(2750));
		assert_eq!(sec, 997);
		assert_eq!(frac, 0x8000_0000);
	}
}
