#![no_main]

use bairelay_neolink_core::fuzz_api::{
	parse_http_request, HttpRequestError, MAX_BODY_BYTES, MAX_HEADER_BYTES,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	// A known total exceeds the input, stays within the limits, and
	// completes the request whatever the missing bytes hold.
	if let Err(HttpRequestError::Incomplete { needed: Some(n) }) = parse_http_request(data) {
		assert!(n > data.len());
		assert!(n <= MAX_HEADER_BYTES + MAX_BODY_BYTES);
		let mut padded = data.to_vec();
		padded.resize(n, 0);
		assert!(parse_http_request(&padded).is_ok());
	}
});
