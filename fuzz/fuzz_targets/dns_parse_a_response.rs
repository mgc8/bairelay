#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	// Take the transaction id from the input so mutations get past the
	// id check and into the record walk.
	let id = data.get(..2).map_or(0, |b| u16::from_be_bytes([b[0], b[1]]));
	let _ = bairelay::dns::parse_a_response(id, data);
});
