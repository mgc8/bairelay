//! Read the camera in the second after it announces that it has woken.
//!
//! Why this module exists. The UDP announcement the camera pushes on its own
//! says two things, but not at the same moment: `event` 3 reports that it is
//! waking - a hundred milliseconds after the button - and `event` 4 with `reason`
//! 19 only confirms the press **eighteen seconds later**, at the end of the
//! recording ([`crate::camera_tasks::report_doorbell_press`] publishes that second moment). Eighteen
//! seconds is too long for a doorbell.
//!
//! What the read does, and only that: on `event` 3 it takes the camera's wake
//! lock and holds it. That lock is what tells the connect loop to go and reach
//! the camera - the same gesture as the read burst ([`crate::burst`]). Once a
//! session exists, the bridge's alarm listener
//! ([`crate::camera_tasks::motion_listener`]) subscribes to message 31 by itself
//! and reads the visitor state of every push: if the camera holds that state, the
//! press goes out through the path already written, deduplicated.
//!
//! What this module does not do, and that is the contract:
//!
//! - **it does not open a subscription of its own.** One alarm subscription
//!   already holds the session's push channel; opening a second one on the same
//!   message would be inventing a protocol the camera never wrote. The existing
//!   listener therefore does the reading and the publishing.
//! - **it publishes nothing itself**: neither motion nor press. A wake is not a
//!   motion event, and the press belongs to [`crate::camera_tasks`].
//! - **it does not fire twice.** One read at a time per camera, and a minimum
//! interval between two *paid* ones: in ordinary operation, motion detection
//! wakes the camera for every passing vehicle.
//!
//! The lock, not the session. This module cannot promise a session: it can only
//! hold the door open and wait. Attempt timings taken on a live installation say
//! it better than any theoretical setting - 604 ms,
//! 2 413 ms, 604 ms, and once **nothing at all after 20 s**, the camera
//! answering the wake twelve seconds after the attempt had been given up. Hence
//! the policy: hold a bounded window, watch the session arrive without depending
//! on it, and **do not spend the interval when nothing was obtained** - a
//! attempt that paid nothing must not forbid the next one, which may be the
//! successful one.
//!
//! Cost, stated so that it is measured rather than forgotten: a successful read
//! buys a TCP session, and a session lengthens the camera's activity - 249 s
//! observed against 19 s without us. Every step is logged with `tracing::debug!`
//! and durations in milliseconds, because it is the container log that gets read:
//! this bridge keeps no separate notebook.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::camera::CameraHandle;

/// Cadence of the probe that checks whether the session has arrived.
const POLL_STEP: Duration = Duration::from_millis(250);

/// How long the lock stays held **after** the session appeared: that is the
/// window the alarm subscription and the first push need. A session that
/// arrives late must not be released the instant it shows up.
const HOLD_AFTER_SESSION: Duration = Duration::from_secs(8);

/// One read at a time, per camera.
///
/// The guard lives in the task that consumes announcements, one per configured
/// camera - never global: two cameras have no reason to punish each other.
/// l'autre.
pub(crate) struct Guard {
	/// A task already holds this camera's lock. Shared with the task that was
	/// started, which gives it back when it finishes - including if it is cancelled.
	in_flight: Arc<AtomicBool>,
	/// When the last read **that obtained a session** ran. A failed attempt does
	/// not write it: it cost nothing, so it must forbid
	/// nothing.
	last_paid: Mutex<Option<Instant>>,
}

impl Guard {
	pub(crate) fn new() -> Self {
		Self {
			in_flight: Arc::new(AtomicBool::new(false)),
			last_paid: Mutex::new(None),
		}
	}

	/// Reserve the right to start: false if a read is already running, or if the
	/// last **paid** one is too recent. The reservation is taken here, synchronously,
	/// because two announcements a millisecond apart must not buy two
	/// sessions.
	///
	/// A refusal consumes nothing: neither the timestamp nor the flag.
	fn reserve(&self, interval: Duration, now: Instant) -> bool {
		if self.in_flight.swap(true, Ordering::SeqCst) {
			return false;
		}
		let last = self
			.last_paid
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		if let Some(at) = *last {
			if now - at < interval {
				// The flag was not taken: give it back exactly as it was.
				drop(last);
				self.in_flight.store(false, Ordering::SeqCst);
				return false;
			}
		}
		true
	}

	/// The session arrived: this read paid for a session, so it earns its interval
	/// before the next one.
	fn mark_paid(&self, at: Instant) {
		*self.last_paid
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(at);
	}

	/// The task finished: it gives the flag back. Called from the task's `Drop`,
	/// so it also runs when the task is cancelled in the middle of an `await`.
	fn release(&self) {
		self.in_flight.store(false, Ordering::SeqCst);
	}
}

/// Start a read if the guard allows it. True when the task was started - the
/// bridge log then carries the lines saying when the session arrived, or that
/// it never arrived.
///
/// `hold` bounds the waiting window: the lock is given back at the latest at
/// that point, unless the session arrives so late that [`HOLD_AFTER_SESSION`]
/// would be squeezed out - in which case the window is extended by that much,
/// `hold + HOLD_AFTER_SESSION`.
pub(crate) fn try_once(
	guard: &Arc<Guard>,
	handle: Arc<CameraHandle>,
	hold: Duration,
	interval: Duration,
	cancel: &CancellationToken,
) -> bool {
	if !guard.reserve(interval, Instant::now()) {
		tracing::debug!("poll: no start cam={} (one is running, or the last session is younger than {} s)",
			handle.name(),
			interval.as_secs());
		return false;
	}
	let cancel = cancel.clone();
	tokio::spawn(run_task(Arc::clone(guard), handle, hold, cancel));
	true
}

/// The body of the read, once the guard is reserved.
async fn run_task(guard: Arc<Guard>, handle: Arc<CameraHandle>, hold: Duration, cancel: CancellationToken) {
	/// The flag is given back whatever happens: a task cancelled in the middle of
	/// an `await` must not lock the camera forever.
	struct Release(Arc<Guard>);
	impl Drop for Release {
		fn drop(&mut self) {
			self.0.release();
		}
	}
	let _release = Release(Arc::clone(&guard));

	let cam = handle.name().to_owned();
	let start = tokio::time::Instant::now();

	// The whole gesture: hold the lock. It is the lock that brings the session up,
	// and `idle_disconnect` releases the camera as soon as we let go.
	//
	// The log carries the count of held locks, not just ours: a lock taken while
	// one already exists means a session is open, or being opened, somewhere else.
	// That is exactly what we want to see when a session lasts four minutes
	// instead of thirty seconds.
	let wake_guard = handle.wake_lock().acquire();
	tracing::debug!("poll: wake lock taken cam={cam} locks={}",
		handle.wake_lock().count());

	let ceiling = start + hold + HOLD_AFTER_SESSION;
	let mut deadline = start + hold;
	let mut seen = None;
	loop {
		let now = tokio::time::Instant::now();
		if now >= deadline {
			break;
		}
		if seen.is_none() && handle.bc_camera_concrete().is_some() {
			let at = now;
			seen = Some(at);
			guard.mark_paid(at);
			// The session just arrived: give it time for the subscription.
			deadline = (at + HOLD_AFTER_SESSION).min(ceiling);
			tracing::debug!("poll: session established after {} ms cam={} — the alarm listener holds the lane, lock kept for {} more s",
				elapsed_ms(start, at),
				cam,
				(deadline - at).as_secs());
		}
		let remaining = deadline - tokio::time::Instant::now();
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = handle.cancel_token().cancelled() => break,
			_ = tokio::time::sleep(remaining.min(POLL_STEP)) => {}
		}
	}

	// The lock is released before the log line: the count that follows is the one
	// that remains, so the one that says whether the camera will really sleep.
	drop(wake_guard);
	let locks_left = handle.wake_lock().count();
	match seen {
		Some(at) => tracing::debug!("poll: lock released after {} ms cam={} (session seen at {} ms) locks={}",
			elapsed_ms(start, tokio::time::Instant::now()),
			cam,
			elapsed_ms(start, at),
			locks_left),
		None => tracing::debug!("poll: no session after {} ms cam={} locks={} — nothing polled, the slow path keeps the lane"
			, elapsed_ms(start, tokio::time::Instant::now()), cam, locks_left),
	}
}

/// Milliseconds between two instants, for the log lines.
fn elapsed_ms(start: Instant, now: Instant) -> u128 {
	now.saturating_duration_since(start).as_millis()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_first_request_always_passes() {
		let guard = Guard::new();
		assert!(guard.reserve(Duration::from_secs(30), Instant::now()));
	}

	#[test]
	fn only_one_poll_at_a_time() {
		// The second request lands while the first still holds the lock: refused,
		// without even looking at the interval.
		let guard = Guard::new();
		let now = Instant::now();
		assert!(guard.reserve(Duration::from_secs(30), now));
		assert!(
			!guard.reserve(Duration::from_secs(0), now),
			"a poll in flight blocks the next one, zero interval or not"
		);
	}

	#[test]
	fn the_interval_only_punishes_a_won_session() {
		// The heart of the policy: an attempt that got no session writes nothing,
		// so the next wake announcement starts again.
		let guard = Guard::new();
		let now = Instant::now();
		assert!(guard.reserve(Duration::from_secs(30), now));
		guard.release();
		assert!(
			guard.reserve(Duration::from_secs(30), now + Duration::from_secs(2)),
			"nothing was paid for, so nothing is forbidden"
		);
		guard.release();
		// Here the session arrived: the interval applies.
		guard.mark_paid(now + Duration::from_secs(4));
		assert!(!guard.reserve(Duration::from_secs(30), now + Duration::from_secs(10)));
		assert!(guard.reserve(Duration::from_secs(30), now + Duration::from_secs(35)));
	}

	#[test]
	fn an_interval_refusal_costs_nothing() {
		// A refusal must not push back the date of the next allowed attempt:
		// otherwise a noise announcement would make a good one be missed.
		let guard = Guard::new();
		let now = Instant::now();
		guard.mark_paid(now);
		assert!(!guard.reserve(Duration::from_secs(30), now + Duration::from_secs(1)));
		assert!(guard.reserve(Duration::from_secs(30), now + Duration::from_secs(31)));
	}
}
