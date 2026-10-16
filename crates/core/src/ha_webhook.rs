//! Parsers for the HTTP webhook a camera's `<HaCfg>` setting points at.
//!
//! A camera with an armed webhook POSTs a small JSON body to a URL the
//! relay serves. [`parse_http_request`](crate::ha_webhook::parse_http_request)
//! parses one HTTP/1.x request from the bytes a listener has read so far,
//! [`parse_webhook_body`](crate::ha_webhook::parse_webhook_body) turns its
//! body into a [`WebhookEvent`](crate::ha_webhook::WebhookEvent), and
//! [`alarm_events_from_xml`](crate::ha_webhook::alarm_events_from_xml)
//! turns the XML of a forwarded cmd 33 push into the input of
//! [`classify_alarm_events`](crate::bc_protocol::classify_alarm_events).
//!
//! [`random_hex_token`](crate::ha_webhook::random_hex_token) makes the per-camera token in that URL's path.
//!
//! Every parser here is pure and panic-free on any input. Each input
//! is size-bounded by a named limit before any parsing work.

use crate::bc::xml::{parse_lossy_utf8, AlarmEvent, AlarmEventList, BcXml};
use rand::RngCore;
use serde_json::{Map, Value};

/// Largest accepted header block in bytes: the request line, every
/// header line and the blank line ending them.
pub const MAX_HEADER_BYTES: usize = 8 * 1024;
/// Largest accepted request body in bytes.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// Longest [`WakeReason::Other`] text kept, in bytes.
pub const MAX_REASON_LEN: usize = 32;
/// `cmd` value of a forwarded `<AlarmEventList>` push.
pub const ALARM_CMD: u64 = 33;
/// Length of a [`random_hex_token`] in hex digits.
pub const TOKEN_HEX_LEN: usize = 32;

/// A fresh 128-bit token as [`TOKEN_HEX_LEN`] lowercase hex digits: 16
/// bytes from the operating system's random source (`OsRng`).
pub fn random_hex_token() -> String {
	use std::fmt::Write as _;
	let mut bytes = [0u8; TOKEN_HEX_LEN / 2];
	rand::rngs::OsRng.fill_bytes(&mut bytes);
	bytes
		.iter()
		.fold(String::with_capacity(TOKEN_HEX_LEN), |mut token, b| {
			let _ = write!(token, "{b:02x}");
			token
		})
}

/// One parsed HTTP request, borrowing from the read buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequest<'a> {
	/// Request method, e.g. `POST`.
	pub method: &'a str,
	/// Request target as sent, e.g. `/hook/<token>`.
	pub path: &'a str,
	/// Exactly `Content-Length` bytes of body; empty for a method other
	/// than `POST` that carries no `Content-Length`.
	pub body: &'a [u8],
}

/// Why [`parse_http_request`] produced no request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRequestError {
	/// The header block or the body is not complete yet; read more bytes.
	Incomplete {
		/// The whole request's length once the header block is
		/// complete (`None` before): no parse succeeds on fewer bytes.
		needed: Option<usize>,
	},
	/// The header block exceeds [`MAX_HEADER_BYTES`].
	HeaderTooLarge,
	/// `Content-Length` exceeds [`MAX_BODY_BYTES`].
	BodyTooLarge,
	/// The request line is not `<method> <path> HTTP/1.1` (or `HTTP/1.0`).
	BadRequestLine,
	/// A header line is not `<name>: <value>` with a token name and a
	/// value free of control characters.
	BadHeader,
	/// A `POST` carries no `Content-Length`.
	MissingContentLength,
	/// `Content-Length` appears more than once.
	DuplicateContentLength,
	/// `Content-Length` is not a plain decimal number.
	BadContentLength,
	/// The request carries `Transfer-Encoding` (e.g. `chunked`).
	TransferEncoding,
}

/// Parse one HTTP/1.x request from the start of `buf`.
///
/// Returns [`HttpRequestError::Incomplete`] while `buf` holds a prefix of
/// an acceptable request, so a listener calls it again after more bytes
/// arrive: after each read while the header block is incomplete, and
/// once the request's `needed` length is buffered after that.
/// A `POST` requires `Content-Length`; `Transfer-Encoding`, a duplicate
/// or non-numeric `Content-Length`, a header block over
/// [`MAX_HEADER_BYTES`] and a body over [`MAX_BODY_BYTES`] are rejected.
/// Lines end in CRLF; a bare CR or LF inside a line is rejected. Bytes
/// after the body are ignored: a listener answers one request per
/// connection.
pub fn parse_http_request(buf: &[u8]) -> Result<HttpRequest<'_>, HttpRequestError> {
	use HttpRequestError::*;

	// The blank line must end within the first MAX_HEADER_BYTES bytes.
	let scan = &buf[..buf.len().min(MAX_HEADER_BYTES)];
	let Some(head_len) = find(scan, HEADER_END) else {
		return Err(if buf.len() >= MAX_HEADER_BYTES {
			HeaderTooLarge
		} else {
			Incomplete { needed: None }
		});
	};
	let body_start = head_len + HEADER_END.len();

	let mut lines = CrlfLines(&buf[..head_len]);
	let (method, path) = parse_request_line(lines.next().unwrap_or_default())?;

	let mut content_length: Option<u64> = None;
	for line in lines {
		let (name, value) = parse_header_line(line)?;
		if name.eq_ignore_ascii_case(b"transfer-encoding") {
			return Err(TransferEncoding);
		}
		if name.eq_ignore_ascii_case(b"content-length") {
			if content_length.is_some() {
				return Err(DuplicateContentLength);
			}
			content_length = Some(parse_content_length(value)?);
		}
	}

	let body_len = match content_length {
		Some(n) => n,
		None if method == "POST" => return Err(MissingContentLength),
		None => 0,
	};
	if body_len > MAX_BODY_BYTES as u64 {
		return Err(BodyTooLarge);
	}
	// Both terms are bounded by the limits above, so the sum cannot overflow.
	let body_end = body_start + body_len as usize;
	let body = buf.get(body_start..body_end).ok_or(Incomplete {
		needed: Some(body_end),
	})?;
	Ok(HttpRequest { method, path, body })
}

/// The blank line ending an HTTP header block.
pub const HEADER_END: &[u8] = b"\r\n\r\n";
/// An HTTP line ending.
const CRLF: &[u8] = b"\r\n";

/// Position of the first occurrence of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
	hay.windows(needle.len()).position(|w| w == needle)
}

/// Iterator over the CRLF-separated lines of a header block (which
/// excludes the final blank line).
struct CrlfLines<'a>(&'a [u8]);

impl<'a> Iterator for CrlfLines<'a> {
	type Item = &'a [u8];

	fn next(&mut self) -> Option<&'a [u8]> {
		if self.0.is_empty() {
			return None;
		}
		match find(self.0, CRLF) {
			Some(i) => {
				let line = &self.0[..i];
				self.0 = &self.0[i + CRLF.len()..];
				Some(line)
			}
			None => Some(std::mem::take(&mut self.0)),
		}
	}
}

/// An RFC 9110 `tchar`: the characters of a method or header name.
fn is_tchar(b: u8) -> bool {
	b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// A non-empty run of `tchar`s.
fn is_token(s: &[u8]) -> bool {
	!s.is_empty() && s.iter().copied().all(is_tchar)
}

/// Split `<method> <path> HTTP/1.x` into method and path.
fn parse_request_line(line: &[u8]) -> Result<(&str, &str), HttpRequestError> {
	let bad = HttpRequestError::BadRequestLine;
	let mut parts = line.split(|&b| b == b' ');
	let (Some(method), Some(path), Some(version), None) =
		(parts.next(), parts.next(), parts.next(), parts.next())
	else {
		return Err(bad);
	};
	let path_ok = path.first() == Some(&b'/') && path.iter().all(u8::is_ascii_graphic);
	let version_ok = version == b"HTTP/1.1" || version == b"HTTP/1.0";
	if !is_token(method) || !path_ok || !version_ok {
		return Err(bad);
	}
	// Both are ASCII after the checks above.
	match (std::str::from_utf8(method), std::str::from_utf8(path)) {
		(Ok(method), Ok(path)) => Ok((method, path)),
		_ => Err(bad),
	}
}

/// Split `<name>: <value>` into the name and the OWS-trimmed value.
fn parse_header_line(line: &[u8]) -> Result<(&[u8], &[u8]), HttpRequestError> {
	let bad = HttpRequestError::BadHeader;
	let colon = line.iter().position(|&b| b == b':').ok_or(bad)?;
	let (name, value) = (&line[..colon], &line[colon + 1..]);
	let is_ctl = |b: u8| (b < 0x20 && b != b'\t') || b == 0x7f;
	if !is_token(name) || value.iter().copied().any(is_ctl) {
		return Err(bad);
	}
	Ok((name, value.trim_ascii()))
}

/// Parse a `Content-Length` value. A numeral too large for `u64` is
/// reported as [`HttpRequestError::BodyTooLarge`].
fn parse_content_length(value: &[u8]) -> Result<u64, HttpRequestError> {
	if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
		return Err(HttpRequestError::BadContentLength);
	}
	value.iter().try_fold(0u64, |acc, &d| {
		acc.checked_mul(10)
			.and_then(|n| n.checked_add(u64::from(d - b'0')))
			.ok_or(HttpRequestError::BodyTooLarge)
	})
}

/// The reason a `wake` webhook reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeReason {
	/// The doorbell button was pressed.
	Doorbell,
	/// The PIR sensor detected motion.
	Pir,
	/// Any other reason, lowercased and reduced to `[a-z0-9_-]`, at most
	/// [`MAX_REASON_LEN`] bytes; empty when the body names none.
	Other(String),
}

/// One webhook event a camera reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookEvent {
	/// The camera woke up.
	Wake {
		/// Why it woke.
		reason: WakeReason,
	},
	/// The camera is going to sleep.
	Sleep,
	/// A test request (e.g. from the Reolink app).
	Test,
	/// A forwarded cmd 33 push; `xml` is its `<AlarmEventList>` payload.
	Alarm {
		/// The forwarded XML, for [`alarm_events_from_xml`].
		xml: String,
	},
}

/// A webhook body [`parse_webhook_body`] rejects as malformed: not a
/// JSON object within [`MAX_BODY_BYTES`], or a cmd 33 forward without
/// a string `xml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedBody;

/// Parse a webhook body into a [`WebhookEvent`].
///
/// Accepts the wake/sleep/test envelope (`event`, `reason`) under a
/// `data` object or at the top level, and the command forward
/// `{"cmd":33,"xml":"…"}` with `cmd` as a number or a numeric string.
/// A JSON object naming no known event or command is `Ok(None)`; see
/// [`MalformedBody`] for the errors.
pub fn parse_webhook_body(body: &[u8]) -> Result<Option<WebhookEvent>, MalformedBody> {
	if body.len() > MAX_BODY_BYTES {
		return Err(MalformedBody);
	}
	// serde_json caps nesting depth (128), so deep input is an error.
	let value: Value = serde_json::from_slice(body).map_err(|_| MalformedBody)?;
	let top = value.as_object().ok_or(MalformedBody)?;

	if let Some(cmd) = top.get("cmd") {
		if cmd_number(cmd) != Some(ALARM_CMD) {
			return Ok(None);
		}
		let xml = top
			.get("xml")
			.and_then(Value::as_str)
			.ok_or(MalformedBody)?;
		return Ok(Some(WebhookEvent::Alarm {
			xml: xml.to_owned(),
		}));
	}

	let envelope = top.get("data").and_then(Value::as_object).unwrap_or(top);
	let Some(event) = envelope.get("event").and_then(Value::as_str) else {
		return Ok(None);
	};
	let event = event.trim();
	Ok(if event.eq_ignore_ascii_case("wake") {
		Some(WebhookEvent::Wake {
			reason: wake_reason(envelope),
		})
	} else if event.eq_ignore_ascii_case("sleep") {
		Some(WebhookEvent::Sleep)
	} else if event.eq_ignore_ascii_case("test") {
		Some(WebhookEvent::Test)
	} else {
		None
	})
}

/// A `cmd` value as a non-negative integer: a JSON integer or a string
/// holding one.
fn cmd_number(cmd: &Value) -> Option<u64> {
	match cmd {
		Value::Number(n) => n.as_u64(),
		Value::String(s) => s.trim().parse().ok(),
		_ => None,
	}
}

/// The `reason` of a wake envelope.
fn wake_reason(envelope: &Map<String, Value>) -> WakeReason {
	let reason = envelope
		.get("reason")
		.and_then(Value::as_str)
		.unwrap_or_default()
		.trim();
	if reason.eq_ignore_ascii_case("doorbell") {
		WakeReason::Doorbell
	} else if reason.eq_ignore_ascii_case("pir") {
		WakeReason::Pir
	} else {
		let clean = reason
			.chars()
			.map(|c| c.to_ascii_lowercase())
			.filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
			.take(MAX_REASON_LEN)
			.collect();
		WakeReason::Other(clean)
	}
}

/// Parse the XML of a forwarded cmd 33 push into the events
/// [`classify_alarm_events`](crate::bc_protocol::classify_alarm_events)
/// takes.
///
/// Accepts `<AlarmEventList>` as the root or inside a `<body>` wrapper.
/// Returns `None` when the XML does not parse or exceeds
/// [`MAX_BODY_BYTES`].
pub fn alarm_events_from_xml(xml: &str) -> Option<Vec<AlarmEvent>> {
	if xml.len() > MAX_BODY_BYTES {
		return None;
	}
	let bytes = xml.as_bytes();
	// The deserializer ignores the root element's name: a `<body>`
	// document fills `BcXml`, a bare list fills `AlarmEventList`.
	if let Ok(BcXml {
		alarm_event_list: Some(list),
		..
	}) = BcXml::try_parse(bytes)
	{
		return Some(list.alarm_events);
	}
	parse_lossy_utf8::<AlarmEventList>(bytes)
		.ok()
		.map(|list| list.alarm_events)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc_protocol::{classify_alarm_events, MotionStatus};
	use proptest::prelude::*;
	use std::time::Instant;
	use HttpRequestError::*;

	/// A `POST` to `/hook/abc` with `body` and a matching `Content-Length`.
	fn post(body: &str) -> Vec<u8> {
		format!(
			"POST /hook/abc HTTP/1.1\r\nHost: 192.0.2.1:8780\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
			body.len()
		)
		.into_bytes()
	}

	/// A request with `head` (request line and headers, CRLF-joined
	/// without the blank line) followed by the blank line and `body`.
	fn raw(head: &str, body: &str) -> Vec<u8> {
		format!("{head}\r\n\r\n{body}").into_bytes()
	}

	#[test]
	fn random_tokens_are_128_bit_lowercase_hex_and_distinct() {
		let tokens: Vec<String> = (0..64).map(|_| random_hex_token()).collect();
		for t in &tokens {
			assert_eq!(t.len(), TOKEN_HEX_LEN, "{t}");
			assert!(
				t.bytes()
					.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
				"{t}"
			);
		}
		let unique: std::collections::HashSet<&String> = tokens.iter().collect();
		assert_eq!(unique.len(), tokens.len());
	}

	#[test]
	fn parses_a_post_with_its_body() {
		let buf = post(r#"{"data":{"event":"wake"}}"#);
		let req = parse_http_request(&buf).expect("valid request");
		assert_eq!(req.method, "POST");
		assert_eq!(req.path, "/hook/abc");
		assert_eq!(req.body, br#"{"data":{"event":"wake"}}"#);
	}

	#[test]
	fn accepted_request_table() {
		// (request bytes, method, path, body)
		let cases: &[(Vec<u8>, &str, &str, &[u8])] = &[
			(
				raw("POST /h HTTP/1.0\r\nContent-Length: 2", "{}"),
				"POST",
				"/h",
				b"{}",
			),
			// Header names are case-insensitive; the value may carry OWS.
			(
				raw("POST /h HTTP/1.1\r\ncontent-length: \t2 ", "{}"),
				"POST",
				"/h",
				b"{}",
			),
			(
				raw("POST /h HTTP/1.1\r\nCONTENT-LENGTH:2", "{}"),
				"POST",
				"/h",
				b"{}",
			),
			// Zero-length body.
			(
				raw("POST /h HTTP/1.1\r\nContent-Length: 0", ""),
				"POST",
				"/h",
				b"",
			),
			// Leading zeros are still a decimal number.
			(
				raw("POST /h HTTP/1.1\r\nContent-Length: 002", "{}"),
				"POST",
				"/h",
				b"{}",
			),
			// Bytes after the body are ignored.
			(
				raw(
					"POST /h HTTP/1.1\r\nContent-Length: 2",
					"{}\r\nGET / HTTP/1.1",
				),
				"POST",
				"/h",
				b"{}",
			),
			// Another method without Content-Length has an empty body, so
			// the listener can answer it with 405.
			(raw("GET /h HTTP/1.1\r\nHost: x", ""), "GET", "/h", b""),
			(raw("GET /h HTTP/1.1", ""), "GET", "/h", b""),
			// Query strings and percent escapes are part of the path.
			(
				raw("POST /h?a=1&b=%20 HTTP/1.1\r\nContent-Length: 0", ""),
				"POST",
				"/h?a=1&b=%20",
				b"",
			),
			// Header values may hold obs-text and tabs.
			(
				raw(
					"POST /h HTTP/1.1\r\nUser-Agent: cam\t\u{e9}\r\nContent-Length: 0",
					"",
				),
				"POST",
				"/h",
				b"",
			),
		];
		for (buf, method, path, body) in cases {
			let req = parse_http_request(buf)
				.unwrap_or_else(|e| panic!("{:?}: {e:?}", String::from_utf8_lossy(buf)));
			assert_eq!(req.method, *method, "{:?}", String::from_utf8_lossy(buf));
			assert_eq!(req.path, *path, "{:?}", String::from_utf8_lossy(buf));
			assert_eq!(req.body, *body, "{:?}", String::from_utf8_lossy(buf));
		}
	}

	#[test]
	fn rejected_request_table() {
		let cases: &[(&[u8], HttpRequestError)] = &[
			// Request line.
			(
				b"POST /h HTTP/2.0\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h http/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST h HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST  /h HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h  HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(b"POST /h\r\nContent-Length: 0\r\n\r\n", BadRequestLine),
			(
				b"POST /a b HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(b" /h HTTP/1.1\r\nContent-Length: 0\r\n\r\n", BadRequestLine),
			(
				b"PO(ST /h HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h\x01 HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h\xff HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h HTTP/1.1 \r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"\r\nPOST /h HTTP/1.1\r\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(b"\r\n\r\n", BadRequestLine),
			// Bare LF / CR inside a line.
			(
				b"POST /h HTTP/1.1\nContent-Length: 0\r\n\r\n",
				BadRequestLine,
			),
			(
				b"POST /h HTTP/1.1\r\nX: a\nb\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			(
				b"POST /h HTTP/1.1\r\nX: a\rb\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			// Header lines.
			(
				b"POST /h HTTP/1.1\r\nNoColon\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			(
				b"POST /h HTTP/1.1\r\n: v\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			(b"POST /h HTTP/1.1\r\nContent-Length : 0\r\n\r\n", BadHeader),
			(
				b"POST /h HTTP/1.1\r\nX: a\r\n folded\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			(
				b"POST /h HTTP/1.1\r\nX: a\x00b\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			(
				b"POST /h HTTP/1.1\r\nX: a\x7fb\r\nContent-Length: 0\r\n\r\n",
				BadHeader,
			),
			// Content-Length.
			(b"POST /h HTTP/1.1\r\nHost: x\r\n\r\n", MissingContentLength),
			(b"POST /h HTTP/1.1\r\n\r\n", MissingContentLength),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
				DuplicateContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 2\r\ncontent-length: 3\r\n\r\n{}",
				DuplicateContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: abc\r\n\r\n",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length:\r\n\r\n",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: +2\r\n\r\n{}",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 1 1\r\n\r\n",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 2,2\r\n\r\n{}",
				BadContentLength,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 0x2\r\n\r\n{}",
				BadContentLength,
			),
			// Transfer-Encoding, alone or with Content-Length.
			(
				b"POST /h HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
				TransferEncoding,
			),
			(
				b"POST /h HTTP/1.1\r\nContent-Length: 2\r\ntransfer-encoding: chunked\r\n\r\n{}",
				TransferEncoding,
			),
			(
				b"POST /h HTTP/1.1\r\nTransfer-Encoding: gzip\r\nContent-Length: 2\r\n\r\n{}",
				TransferEncoding,
			),
		];
		for (buf, want) in cases {
			assert_eq!(
				parse_http_request(buf),
				Err(*want),
				"{:?}",
				String::from_utf8_lossy(buf)
			);
		}
	}

	#[test]
	fn every_strict_prefix_of_a_request_is_incomplete() {
		let buf = post(r#"{"data":{"event":"sleep"}}"#);
		let body_start = find(&buf, HEADER_END).expect("blank line") + HEADER_END.len();
		for n in 0..buf.len() {
			// The total is known once the header block is complete.
			let needed = (n >= body_start).then_some(buf.len());
			assert_eq!(
				parse_http_request(&buf[..n]),
				Err(Incomplete { needed }),
				"prefix of {n} bytes"
			);
		}
		assert!(parse_http_request(&buf).is_ok());
	}

	#[test]
	fn body_too_large_is_reported_before_the_body_arrives() {
		let head = format!(
			"POST /h HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
			MAX_BODY_BYTES + 1
		);
		assert_eq!(parse_http_request(head.as_bytes()), Err(BodyTooLarge));
		// A numeral that overflows u64 is too large, not malformed.
		let huge = "POST /h HTTP/1.1\r\nContent-Length: 99999999999999999999999999\r\n\r\n";
		assert_eq!(parse_http_request(huge.as_bytes()), Err(BodyTooLarge));
	}

	#[test]
	fn body_at_the_limit_is_accepted() {
		let body = "x".repeat(MAX_BODY_BYTES);
		let buf = post(&body);
		let req = parse_http_request(&buf).expect("body at the limit");
		assert_eq!(req.body.len(), MAX_BODY_BYTES);
		// One byte short of the declared length is still incomplete.
		assert_eq!(
			parse_http_request(&buf[..buf.len() - 1]),
			Err(Incomplete {
				needed: Some(buf.len())
			})
		);
	}

	/// A request whose header block (request line through the blank
	/// line) is exactly `len` bytes, padded with one `X-Pad` header.
	fn request_with_header_block(len: usize) -> Vec<u8> {
		let fixed = "POST /h HTTP/1.1\r\nContent-Length: 0\r\nX-Pad: \r\n\r\n".len();
		let pad = "p".repeat(len - fixed);
		format!("POST /h HTTP/1.1\r\nContent-Length: 0\r\nX-Pad: {pad}\r\n\r\n").into_bytes()
	}

	#[test]
	fn header_block_limit() {
		let at = request_with_header_block(MAX_HEADER_BYTES);
		assert_eq!(at.len(), MAX_HEADER_BYTES);
		assert!(parse_http_request(&at).is_ok(), "block at the limit");

		let over = request_with_header_block(MAX_HEADER_BYTES + 1);
		assert_eq!(parse_http_request(&over), Err(HeaderTooLarge));

		// An unterminated block is incomplete until it reaches the limit.
		let open = vec![b'a'; MAX_HEADER_BYTES - 1];
		assert_eq!(parse_http_request(&open), Err(Incomplete { needed: None }));
		let full = vec![b'a'; MAX_HEADER_BYTES];
		assert_eq!(parse_http_request(&full), Err(HeaderTooLarge));
	}

	proptest! {
		#[test]
		fn http_parser_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..2048)) {
			let _ = parse_http_request(&buf);
		}

		#[test]
		fn http_parser_never_panics_after_a_valid_head(tail in proptest::collection::vec(any::<u8>(), 0..512)) {
			let mut buf = b"POST /h HTTP/1.1\r\n".to_vec();
			buf.extend_from_slice(&tail);
			let _ = parse_http_request(&buf);
		}

		/// A reported total exceeds the bytes given, stays within the
		/// limits, and completes the request whatever the body holds.
		#[test]
		fn incomplete_total_is_exact(
			head_len in 0usize..200,
			body in proptest::collection::vec(any::<u8>(), 0..300),
			cut in any::<proptest::sample::Index>(),
		) {
			let mut buf = format!("POST /h HTTP/1.1\r\nX-Pad: {}\r\nContent-Length: {}\r\n\r\n", "p".repeat(head_len), body.len()).into_bytes();
			buf.extend_from_slice(&body);
			let prefix = &buf[..cut.index(buf.len() + 1)];
			if let Err(Incomplete { needed: Some(n) }) = parse_http_request(prefix) {
				prop_assert!(n > prefix.len());
				prop_assert!(n <= MAX_HEADER_BYTES + MAX_BODY_BYTES);
				prop_assert_eq!(n, buf.len());
				let mut padded = prefix.to_vec();
				padded.resize(n, 0);
				prop_assert!(parse_http_request(&padded).is_ok());
			}
		}

		#[test]
		fn body_parser_never_panics(buf in proptest::collection::vec(any::<u8>(), 0..2048)) {
			if let Ok(Some(WebhookEvent::Alarm { xml })) = parse_webhook_body(&buf) {
				let _ = alarm_events_from_xml(&xml);
			}
		}

		#[test]
		fn other_reason_is_always_bounded_and_clean(reason in ".{0,200}") {
			let body = serde_json::json!({"event": "wake", "reason": reason}).to_string();
			match parse_webhook_body(body.as_bytes()) {
				Ok(Some(WebhookEvent::Wake { reason: WakeReason::Other(s) })) => {
					prop_assert!(s.len() <= MAX_REASON_LEN);
					prop_assert!(s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-'));
				}
				Ok(Some(WebhookEvent::Wake { .. })) => {}
				other => prop_assert!(false, "unexpected {:?}", other),
			}
		}
	}

	type Parsed = Result<Option<WebhookEvent>, MalformedBody>;

	fn wake(reason: WakeReason) -> Parsed {
		Ok(Some(WebhookEvent::Wake { reason }))
	}

	fn other(s: &str) -> Parsed {
		wake(WakeReason::Other(s.to_string()))
	}

	const SLEEP: Parsed = Ok(Some(WebhookEvent::Sleep));
	const TEST: Parsed = Ok(Some(WebhookEvent::Test));
	const IGNORED: Parsed = Ok(None);
	const MALFORMED: Parsed = Err(MalformedBody);

	#[test]
	fn webhook_body_table() {
		let alarm = |xml: &str| {
			Ok(Some(WebhookEvent::Alarm {
				xml: xml.to_string(),
			}))
		};
		let cases: &[(&str, Parsed)] = &[
			// Envelope under `data`.
			(
				r#"{"data":{"event":"wake","reason":"doorbell"}}"#,
				wake(WakeReason::Doorbell),
			),
			(
				r#"{"data":{"event":"wake","reason":"pir"}}"#,
				wake(WakeReason::Pir),
			),
			(
				r#"{"data":{"event":"wake","reason":"network"}}"#,
				other("network"),
			),
			(r#"{"data":{"event":"sleep"}}"#, SLEEP),
			(r#"{"data":{"event":"test"}}"#, TEST),
			// Envelope at the top level.
			(
				r#"{"event":"wake","reason":"doorbell"}"#,
				wake(WakeReason::Doorbell),
			),
			(r#"{"event":"wake","reason":"pir"}"#, wake(WakeReason::Pir)),
			(r#"{"event":"sleep","reason":"other"}"#, SLEEP),
			(r#"{"event":"test"}"#, TEST),
			// Extra fields, case and whitespace are tolerated.
			(
				r#"{"uid":"x","data":{"event":" Wake ","reason":"DoorBell","ts":1}}"#,
				wake(WakeReason::Doorbell),
			),
			(r#"{"event":"SLEEP"}"#, SLEEP),
			// A wake reason is reduced to a short clean token.
			(r#"{"event":"wake"}"#, other("")),
			(r#"{"event":"wake","reason":7}"#, other("")),
			(
				r#"{"event":"wake","reason":"Bad Reason!\n<b>"}"#,
				other("badreasonb"),
			),
			(
				r#"{"event":"wake","reason":"low_battery-2"}"#,
				other("low_battery-2"),
			),
			// A non-object `data` leaves the top level as the envelope.
			(r#"{"data":"x","event":"sleep"}"#, SLEEP),
			// A JSON object naming no known event is ignored.
			(r#"{"data":{"event":"reboot"}}"#, IGNORED),
			(r#"{"data":{"reason":"pir"}}"#, IGNORED),
			(r#"{"data":{"event":1}}"#, IGNORED),
			(r#"{"event":null}"#, IGNORED),
			(r#"{}"#, IGNORED),
			// Command forward: numeric and string `cmd`.
			(
				r#"{"uid":"u","cmd":33,"xml":"<AlarmEventList/>","ext_xml":""}"#,
				alarm("<AlarmEventList/>"),
			),
			(r#"{"cmd":"33","xml":"<a/>"}"#, alarm("<a/>")),
			(r#"{"cmd":" 33 ","xml":"<a/>"}"#, alarm("<a/>")),
			(r#"{"cmd":33,"xml":""}"#, alarm("")),
			// Any other command is ignored.
			(r#"{"cmd":34,"xml":"<a/>"}"#, IGNORED),
			(r#"{"cmd":"34","xml":"<a/>"}"#, IGNORED),
			(r#"{"cmd":-33,"xml":"<a/>"}"#, IGNORED),
			(r#"{"cmd":33.5,"xml":"<a/>"}"#, IGNORED),
			(r#"{"cmd":"abc","xml":"<a/>"}"#, IGNORED),
			(r#"{"cmd":null,"xml":"<a/>"}"#, IGNORED),
			// A `cmd` field makes the body a command forward only.
			(r#"{"cmd":34,"event":"sleep"}"#, IGNORED),
			// A cmd 33 forward without string XML is malformed.
			(r#"{"cmd":33}"#, MALFORMED),
			(r#"{"cmd":33,"xml":5}"#, MALFORMED),
			// Not a JSON object.
			("", MALFORMED),
			("not json", MALFORMED),
			("[1,2]", MALFORMED),
			("\"wake\"", MALFORMED),
			(r#"{"event":"wake""#, MALFORMED),
		];
		for (body, want) in cases {
			assert_eq!(&parse_webhook_body(body.as_bytes()), want, "{body}");
		}
	}

	#[test]
	fn webhook_body_rejects_invalid_utf8_and_deep_nesting() {
		assert_eq!(parse_webhook_body(b"{\"event\":\"wake\xff\"}"), MALFORMED);
		let deep = format!("{}{}", "[".repeat(10_000), "]".repeat(10_000));
		assert_eq!(parse_webhook_body(deep.as_bytes()), MALFORMED);
	}

	#[test]
	fn webhook_body_size_limit() {
		let envelope = r#"{"event":"sleep"}"#;
		let at = format!("{envelope}{}", " ".repeat(MAX_BODY_BYTES - envelope.len()));
		assert_eq!(at.len(), MAX_BODY_BYTES);
		assert_eq!(parse_webhook_body(at.as_bytes()), SLEEP);
		let over = format!("{at} ");
		assert_eq!(parse_webhook_body(over.as_bytes()), MALFORMED);
	}

	#[test]
	fn wake_reason_truncates_to_the_limit() {
		let long = "a".repeat(MAX_REASON_LEN * 3);
		let body = format!(r#"{{"event":"wake","reason":"{long}"}}"#);
		assert_eq!(
			parse_webhook_body(body.as_bytes()),
			other(&"a".repeat(MAX_REASON_LEN))
		);
	}

	const ALARM_LIST: &str = "<AlarmEventList version=\"1.1\">\
		<AlarmEvent version=\"1.1\"><channelId>0</channelId>\
		<status>visitor,MD</status><AItype>people</AItype>\
		<recording>0</recording><timeStamp>0</timeStamp></AlarmEvent>\
		</AlarmEventList>";

	#[test]
	fn alarm_xml_parses_bare_declared_and_wrapped_lists() {
		let declared = format!("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>{ALARM_LIST}");
		let wrapped =
			format!("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<body>{ALARM_LIST}</body>");
		for xml in [ALARM_LIST.to_string(), declared, wrapped] {
			let events = alarm_events_from_xml(&xml).unwrap_or_else(|| panic!("{xml}"));
			assert_eq!(events.len(), 1, "{xml}");
			assert_eq!(events[0].channel_id, 0);
			assert_eq!(events[0].status, "visitor,MD");
			assert_eq!(events[0].ai_type.as_deref(), Some("people"));
		}
	}

	#[test]
	fn alarm_xml_feeds_the_classifier() {
		let events = alarm_events_from_xml(ALARM_LIST).expect("parses");
		let update = classify_alarm_events(&events, 0, Instant::now());
		assert!(matches!(update.motion, MotionStatus::Start(_)));
		assert_eq!(update.visitor, Some(true));
		assert!(update.objects.expect("objects").person);
	}

	#[test]
	fn alarm_xml_from_a_webhook_body() {
		let body = serde_json::json!({"cmd": 33, "xml": ALARM_LIST}).to_string();
		let Ok(Some(WebhookEvent::Alarm { xml })) = parse_webhook_body(body.as_bytes()) else {
			panic!("cmd 33 body");
		};
		assert_eq!(alarm_events_from_xml(&xml).expect("parses").len(), 1);
	}

	#[test]
	fn alarm_xml_rejects_garbage_and_oversize() {
		assert_eq!(alarm_events_from_xml("not xml <"), None);
		assert_eq!(alarm_events_from_xml("<AlarmEventList><AlarmEvent>"), None);
		let over = format!("{ALARM_LIST}{}", " ".repeat(MAX_BODY_BYTES));
		assert_eq!(alarm_events_from_xml(&over), None);
	}

	#[test]
	fn alarm_xml_without_events_is_empty() {
		assert_eq!(alarm_events_from_xml("<AlarmEventList/>"), Some(vec![]));
	}
}
