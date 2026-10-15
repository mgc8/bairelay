//! Test helpers for exercising publish plumbing without a real broker.
//!
//! `mock_client()` returns a [`SharedMqttClient`] paired with a
//! [`MockHandle`]. Every `publish` / `publish_retained` call on the
//! client is mirrored into the handle's capture buffer, so downstream
//! tests can assert on topic + payload + retained flag without spinning
//! up an MQTT broker.
//!
//! The wrapped `AsyncClient` is the same stub shape as
//! [`SharedMqttClient::for_test_stub`]: its event loop is leaked, so
//! `publish()` returns `Ok(())` after queuing on the rumqttc request
//! channel. Tests that do not await broker I/O stay synchronous.
//!
//! Only compiled under the `test-util` feature.

use std::sync::{Arc, Mutex};

use rumqttc::{AsyncClient, MqttOptions};

use crate::client::{CaptureSink, SharedMqttClient};

/// Observable side of a [`mock_client`] pair.
///
/// Clone-cheap (`Arc`-backed); cloning shares the same capture buffer
/// so tests can hand one side to the unit under test and keep another
/// for assertions.
#[derive(Clone)]
pub struct MockHandle {
	buf: CaptureSink,
}

/// Acquire the capture buffer's `MutexGuard`, recovering from poison
/// rather than re-panicking — a panic in one test holding this lock
/// would otherwise cascade to every subsequent test. Mirrors
/// `lock_recover` in stream_source.rs (commit b394b95).
fn capture_lock(buf: &CaptureSink) -> std::sync::MutexGuard<'_, Vec<crate::client::CaptureRow>> {
	buf.lock().unwrap_or_else(|p| p.into_inner())
}

impl MockHandle {
	/// All (topic, payload, retained) publish tuples observed so far,
	/// in arrival order. Each call returns a fresh `Vec` so callers
	/// can inspect without holding the lock.
	pub fn published(&self) -> Vec<(String, Vec<u8>, bool)> {
		capture_lock(&self.buf).clone()
	}

	/// Topics of all observed publishes, in arrival order. Handy when
	/// a test only cares *which* topics were hit, not the payloads.
	pub fn published_topics(&self) -> Vec<String> {
		capture_lock(&self.buf)
			.iter()
			.map(|(t, _, _)| t.clone())
			.collect()
	}

	/// `(topic, payload)` pairs for observed publishes, in arrival
	/// order. Retained flag is dropped; use [`Self::published`] if you
	/// need it.
	pub fn published_payloads(&self) -> Vec<(String, Vec<u8>)> {
		capture_lock(&self.buf)
			.iter()
			.map(|(t, p, _)| (t.clone(), p.clone()))
			.collect()
	}

	/// Count of observed publishes. Cheap shortcut for "nothing was
	/// published yet" / "publishes are flowing" assertions.
	pub fn count(&self) -> usize {
		capture_lock(&self.buf).len()
	}
}

/// Build a [`SharedMqttClient`] whose publishes are recorded into the
/// returned [`MockHandle`]. The client's wrapped `AsyncClient` never
/// connects to a broker — its event loop is leaked the same way
/// [`SharedMqttClient::for_test_stub`] handles it — so `publish*`
/// calls return `Ok(())` without broker I/O.
pub fn mock_client() -> (SharedMqttClient, MockHandle) {
	capturing_client(1024, true)
}

/// Like [`mock_client`], but every publish fails, as it does once the
/// client's event loop is gone. Attempts are still recorded.
pub fn failing_client() -> (SharedMqttClient, MockHandle) {
	capturing_client(1024, false)
}

/// Like [`mock_client`], but nothing drains the request queue: the
/// first `capacity` publishes succeed and every later one blocks, as
/// with a broker that has stopped responding. Attempts are still
/// recorded, blocked ones included.
pub fn wedged_client(capacity: usize) -> (SharedMqttClient, MockHandle) {
	capturing_client(capacity, true)
}

fn capturing_client(capacity: usize, keep_event_loop: bool) -> (SharedMqttClient, MockHandle) {
	let buf = Arc::new(Mutex::new(Vec::new()));
	let (client, event_loop) =
		AsyncClient::new(MqttOptions::new("mock-client", "127.0.0.1", 1883), capacity);
	// The event loop owns the request channel's receiver: leaked, the
	// channel stays open; dropped, every publish fails.
	if keep_event_loop {
		Box::leak(Box::new(event_loop));
	}
	let shared = SharedMqttClient::__test_new_with_capture(client, Arc::clone(&buf));
	(shared, MockHandle { buf })
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn mock_client_captures_retained_and_non_retained_publishes() {
		let (client, mock) = mock_client();
		client
			.publish_retained("bairelay/cam/status/motion", b"on")
			.await
			.unwrap();
		client
			.publish("bairelay/cam/control/reboot/reply", b"OK")
			.await
			.unwrap();

		let rows = mock.published();
		assert_eq!(rows.len(), 2);
		assert_eq!(rows[0].0, "bairelay/cam/status/motion");
		assert_eq!(rows[0].1, b"on".to_vec());
		assert!(rows[0].2, "first publish should be retained");
		assert_eq!(rows[1].0, "bairelay/cam/control/reboot/reply");
		assert_eq!(rows[1].1, b"OK".to_vec());
		assert!(!rows[1].2, "second publish should not be retained");
	}

	#[tokio::test]
	async fn failing_client_records_and_fails_every_publish() {
		let (client, mock) = failing_client();
		assert!(client.publish_retained("bairelay/a", b"1").await.is_err());
		assert!(client.publish("bairelay/b", b"2").await.is_err());
		assert_eq!(
			mock.published_topics(),
			["bairelay/a".to_string(), "bairelay/b".to_string()]
		);
	}

	#[tokio::test]
	async fn wedged_client_blocks_once_its_queue_is_full() {
		let (client, mock) = wedged_client(1);
		client.publish("bairelay/a", b"1").await.unwrap();
		// Nothing will ever drain the queue, so one pending poll means
		// blocked for good.
		let blocked = tokio::select! {
			biased;
			_ = client.publish("bairelay/b", b"2") => false,
			_ = std::future::ready(()) => true,
		};
		assert!(blocked, "second publish must block");
		assert_eq!(mock.count(), 2, "the blocked attempt is still recorded");
	}

	#[tokio::test]
	async fn mock_handle_views_agree() {
		let (client, mock) = mock_client();
		client.publish_retained("bairelay/a", b"1").await.unwrap();
		client.publish("bairelay/b", b"2").await.unwrap();

		assert_eq!(mock.count(), 2);
		assert_eq!(
			mock.published_topics(),
			vec!["bairelay/a".to_string(), "bairelay/b".to_string()]
		);
		assert_eq!(
			mock.published_payloads(),
			vec![
				("bairelay/a".to_string(), b"1".to_vec()),
				("bairelay/b".to_string(), b"2".to_vec()),
			]
		);
	}
}
