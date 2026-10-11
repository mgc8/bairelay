//! Minimal DNS A-record lookup against one chosen resolver.
//!
//! The push listener's upstream forwarding needs `pushx.reolink.com`'s
//! real address while the LAN resolver deliberately points that name at
//! bairelay, so it can't go through the system resolver. One UDP query,
//! IPv4 answers only — enough to reach a Cloudflare-fronted host.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;

const TYPE_A: u16 = 1;
const CLASS_IN: u16 = 1;
const ATTEMPTS: usize = 2;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DnsError {
	#[error("not a valid DNS name")]
	InvalidName,
	#[error("malformed DNS response")]
	Malformed,
	#[error("DNS response doesn't answer our query")]
	Mismatch,
	#[error("DNS response truncated")]
	Truncated,
	#[error("resolver returned rcode {0}")]
	Rcode(u8),
}

/// Encode a recursive `A IN` query for `name` with transaction `id`.
pub fn encode_query(id: u16, name: &str) -> Result<Vec<u8>, DnsError> {
	let mut msg = Vec::with_capacity(18 + name.len());
	msg.extend_from_slice(&id.to_be_bytes());
	// Flags: RD. Counts: one question, nothing else.
	msg.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
	for label in name.trim_end_matches('.').split('.') {
		if label.is_empty() || label.len() > 63 {
			return Err(DnsError::InvalidName);
		}
		msg.push(label.len() as u8);
		msg.extend_from_slice(label.as_bytes());
	}
	msg.push(0);
	if msg.len() - 12 > 255 {
		return Err(DnsError::InvalidName);
	}
	msg.extend_from_slice(&TYPE_A.to_be_bytes());
	msg.extend_from_slice(&CLASS_IN.to_be_bytes());
	Ok(msg)
}

/// The IPv4 addresses answering query `id`. Other answer records — the
/// CNAME chain a recursive resolver includes — are skipped.
pub fn parse_a_response(id: u16, msg: &[u8]) -> Result<Vec<Ipv4Addr>, DnsError> {
	let mut r = Reader { msg, pos: 0 };
	if r.u16()? != id {
		return Err(DnsError::Mismatch);
	}
	let flags = r.u16()?;
	if flags & 0x8000 == 0 {
		return Err(DnsError::Mismatch);
	}
	if flags & 0x0200 != 0 {
		return Err(DnsError::Truncated);
	}
	let rcode = (flags & 0x000F) as u8;
	if rcode != 0 {
		return Err(DnsError::Rcode(rcode));
	}
	let questions = r.u16()?;
	let answers = r.u16()?;
	r.take(4)?;
	for _ in 0..questions {
		r.skip_name()?;
		r.take(4)?;
	}
	let mut addrs = Vec::new();
	for _ in 0..answers {
		r.skip_name()?;
		let rtype = r.u16()?;
		let class = r.u16()?;
		r.take(4)?;
		let len = r.u16()? as usize;
		let data = r.take(len)?;
		if let (TYPE_A, CLASS_IN, &[a, b, c, d]) = (rtype, class, data) {
			addrs.push(Ipv4Addr::new(a, b, c, d));
		}
	}
	Ok(addrs)
}

struct Reader<'a> {
	msg: &'a [u8],
	pos: usize,
}

impl<'a> Reader<'a> {
	fn take(&mut self, n: usize) -> Result<&'a [u8], DnsError> {
		let end = self.pos.checked_add(n).ok_or(DnsError::Malformed)?;
		let bytes = self.msg.get(self.pos..end).ok_or(DnsError::Malformed)?;
		self.pos = end;
		Ok(bytes)
	}

	fn u16(&mut self) -> Result<u16, DnsError> {
		let b = self.take(2)?;
		Ok(u16::from_be_bytes([b[0], b[1]]))
	}

	/// Step over a name without following compression pointers, so a
	/// pointer loop can't spin: every label consumes input.
	fn skip_name(&mut self) -> Result<(), DnsError> {
		loop {
			let len = self.take(1)?[0];
			match len & 0xC0 {
				0x00 if len == 0 => return Ok(()),
				0x00 => {
					self.take(len as usize)?;
				}
				0xC0 => {
					self.take(1)?;
					return Ok(());
				}
				_ => return Err(DnsError::Malformed),
			}
		}
	}
}

/// Look up `name`'s IPv4 addresses at `resolver`, waiting up to
/// `timeout` per attempt.
pub async fn lookup_a(
	resolver: SocketAddr,
	name: &str,
	timeout: Duration,
) -> io::Result<Vec<Ipv4Addr>> {
	let id: u16 = rand::random();
	let query =
		encode_query(id, name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
	let bind: SocketAddr = if resolver.is_ipv4() {
		(Ipv4Addr::UNSPECIFIED, 0).into()
	} else {
		(Ipv6Addr::UNSPECIFIED, 0).into()
	};
	let sock = UdpSocket::bind(bind).await?;
	// Connected, so the kernel drops datagrams from anyone else.
	sock.connect(resolver).await?;
	let mut buf = [0u8; 1232];
	for _ in 0..ATTEMPTS {
		sock.send(&query).await?;
		if let Ok(answer) = tokio::time::timeout(timeout, recv_answer(&sock, id, &mut buf)).await {
			return answer;
		}
	}
	Err(io::Error::new(
		io::ErrorKind::TimedOut,
		format!("no answer from resolver {resolver}"),
	))
}

async fn recv_answer(sock: &UdpSocket, id: u16, buf: &mut [u8]) -> io::Result<Vec<Ipv4Addr>> {
	loop {
		let n = sock.recv(buf).await?;
		match parse_a_response(id, &buf[..n]) {
			// A late reply to an earlier attempt, or a stray datagram.
			Err(DnsError::Mismatch) => continue,
			result => return result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
		}
	}
}

#[cfg(test)]
pub(crate) mod test_support {
	use super::*;

	/// Answer `query` the way a recursive resolver would: the question
	/// echoed, a CNAME, then one `A` record per address — all names
	/// compressed back to the question.
	pub(crate) fn answer(query: &[u8], addrs: &[Ipv4Addr]) -> Vec<u8> {
		let mut msg = query[..2].to_vec();
		msg.extend_from_slice(&[0x81, 0x80, 0, 1]);
		msg.extend_from_slice(&(1 + addrs.len() as u16).to_be_bytes());
		msg.extend_from_slice(&[0, 0, 0, 0]);
		msg.extend_from_slice(&query[12..]);
		// CNAME -> "cdn" + pointer to the question name.
		msg.extend_from_slice(&[0xC0, 0x0C, 0, 5, 0, 1, 0, 0, 0, 60, 0, 6]);
		msg.extend_from_slice(&[3, b'c', b'd', b'n', 0xC0, 0x0C]);
		for addr in addrs {
			msg.extend_from_slice(&[0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4]);
			msg.extend_from_slice(&addr.octets());
		}
		msg
	}

	/// A UDP resolver on loopback that answers every query with `addrs`.
	pub(crate) async fn fake_resolver(addrs: Vec<Ipv4Addr>) -> SocketAddr {
		let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let addr = sock.local_addr().unwrap();
		tokio::spawn(async move {
			let mut buf = [0u8; 512];
			while let Ok((n, peer)) = sock.recv_from(&mut buf).await {
				let _ = sock.send_to(&answer(&buf[..n], &addrs), peer).await;
			}
		});
		addr
	}
}

#[cfg(test)]
mod tests {
	use super::test_support::{answer, fake_resolver};
	use super::*;

	const PUSHX: &str = "pushx.reolink.com";

	#[test]
	fn encode_query_wire_format() {
		let q = encode_query(0xBEEF, PUSHX).unwrap();
		let mut expected = vec![0xBE, 0xEF, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
		expected.extend_from_slice(b"\x05pushx\x07reolink\x03com\x00");
		expected.extend_from_slice(&[0, 1, 0, 1]);
		assert_eq!(q, expected);
		assert_eq!(encode_query(0xBEEF, "pushx.reolink.com.").unwrap(), q);
	}

	#[test]
	fn encode_query_rejects_bad_names() {
		let long_label = "x".repeat(64);
		let long_name = ["abc"; 70].join(".");
		for name in ["", "a..b", long_label.as_str(), long_name.as_str()] {
			assert_eq!(
				encode_query(1, name),
				Err(DnsError::InvalidName),
				"{name:?}"
			);
		}
	}

	#[test]
	fn parse_collects_a_records_past_a_cname() {
		let q = encode_query(7, PUSHX).unwrap();
		let addrs = [Ipv4Addr::new(104, 18, 1, 1), Ipv4Addr::new(104, 18, 2, 2)];
		assert_eq!(parse_a_response(7, &answer(&q, &addrs)), Ok(addrs.to_vec()));
	}

	#[test]
	fn parse_rejects_responses_to_other_queries() {
		let q = encode_query(7, PUSHX).unwrap();
		let a = answer(&q, &[Ipv4Addr::LOCALHOST]);
		assert_eq!(parse_a_response(8, &a), Err(DnsError::Mismatch));
		// Our own query echoed back: QR clear.
		assert_eq!(parse_a_response(7, &q), Err(DnsError::Mismatch));
	}

	#[test]
	fn parse_surfaces_truncation_and_rcode() {
		let q = encode_query(7, PUSHX).unwrap();
		let mut a = answer(&q, &[Ipv4Addr::LOCALHOST]);
		a[2] |= 0x02;
		assert_eq!(parse_a_response(7, &a), Err(DnsError::Truncated));
		let mut a = answer(&q, &[]);
		a[3] = 0x83; // NXDOMAIN
		assert_eq!(parse_a_response(7, &a), Err(DnsError::Rcode(3)));
	}

	#[test]
	fn parse_rejects_malformed_input() {
		let q = encode_query(7, PUSHX).unwrap();
		let a = answer(&q, &[Ipv4Addr::LOCALHOST]);
		for cut in 0..a.len() {
			assert!(parse_a_response(7, &a[..cut]).is_err(), "cut at {cut}");
		}
		// Label type 0b01 is reserved.
		let mut bad = a.clone();
		bad[12] = 0x45;
		assert_eq!(parse_a_response(7, &bad), Err(DnsError::Malformed));
	}

	#[tokio::test]
	async fn lookup_a_round_trips_through_a_resolver() {
		let addrs = vec![Ipv4Addr::new(203, 0, 113, 9)];
		let resolver = fake_resolver(addrs.clone()).await;
		let got = lookup_a(resolver, PUSHX, Duration::from_secs(2))
			.await
			.unwrap();
		assert_eq!(got, addrs);
	}

	#[tokio::test]
	async fn lookup_a_times_out_against_a_silent_resolver() {
		let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let err = lookup_a(
			silent.local_addr().unwrap(),
			PUSHX,
			Duration::from_millis(50),
		)
		.await
		.unwrap_err();
		assert_eq!(err.kind(), io::ErrorKind::TimedOut);
	}
}
