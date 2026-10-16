#![no_main]

use bairelay_neolink_core::fuzz_api::{alarm_events_from_xml, parse_webhook_body, WebhookEvent};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
	if let Ok(Some(WebhookEvent::Alarm { xml })) = parse_webhook_body(data) {
		let _ = alarm_events_from_xml(&xml);
	}
});
