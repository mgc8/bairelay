//! The unit tests' `log` logger: `env_logger` output, plus per-thread
//! capture so a test can assert on what it logged.
//!
//! `log` takes one logger per process and every unit test in this crate
//! shares the process, so each test module's `init` installs this one.
//! A capture collects the records its own thread logs, at every level,
//! while other tests run alongside.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, Once, OnceLock, PoisonError};
use std::thread::{self, ThreadId};

use log::{LevelFilter, Log, Metadata, Record};

/// `env_logger`, with the lines each capturing thread has logged.
pub(crate) struct TestLogger {
	inner: env_logger::Logger,
	captures: Mutex<HashMap<ThreadId, Vec<String>>>,
}

impl TestLogger {
	fn captures(&self) -> MutexGuard<'_, HashMap<ThreadId, Vec<String>>> {
		self.captures.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// Every level while any thread captures, else `env_logger`'s own
	/// filter. Called with the capture map locked, so it matches the
	/// map.
	fn set_max_level(&self, captures: &HashMap<ThreadId, Vec<String>>) {
		log::set_max_level(if captures.is_empty() {
			self.inner.filter()
		} else {
			LevelFilter::Trace
		});
	}
}

impl Log for TestLogger {
	fn enabled(&self, metadata: &Metadata<'_>) -> bool {
		self.inner.enabled(metadata) || self.captures().contains_key(&thread::current().id())
	}

	fn log(&self, record: &Record<'_>) {
		if let Some(lines) = self.captures().get_mut(&thread::current().id()) {
			lines.push(format!("{} {}", record.level(), record.args()));
		}
		// `env_logger` applies its own filter.
		self.inner.log(record);
	}

	fn flush(&self) {
		self.inner.flush();
	}
}

static LOGGER: OnceLock<TestLogger> = OnceLock::new();
static INSTALL: Once = Once::new();

/// Install the logger once per process: `RUST_LOG`, else `info`, to
/// the test harness's captured output.
pub(crate) fn init() -> &'static TestLogger {
	INSTALL.call_once(|| {
		let logger = LOGGER.get_or_init(|| TestLogger {
			inner: env_logger::Builder::from_env(
				env_logger::Env::default().default_filter_or("info"),
			)
			.is_test(true)
			.build(),
			captures: Mutex::default(),
		});
		if log::set_logger(logger).is_ok() {
			logger.set_max_level(&logger.captures());
		}
	});
	LOGGER.get().expect("installed above")
}

/// Removes its thread's capture, even when the captured code panics.
struct Capture {
	logger: &'static TestLogger,
	thread: ThreadId,
}

impl Drop for Capture {
	fn drop(&mut self) {
		let mut captures = self.logger.captures();
		captures.remove(&self.thread);
		self.logger.set_max_level(&captures);
	}
}

/// Run `f` and return its result with every line this thread logged
/// meanwhile, one `<LEVEL> <message>` per line.
pub(crate) fn capture<R>(f: impl FnOnce() -> R) -> (R, String) {
	let logger = init();
	let thread = thread::current().id();
	{
		let mut captures = logger.captures();
		captures.insert(thread, Vec::new());
		logger.set_max_level(&captures);
	}
	let capture = Capture { logger, thread };
	let result = f();
	let lines = logger
		.captures()
		.get_mut(&thread)
		.map(std::mem::take)
		.unwrap_or_default();
	drop(capture);
	(result, lines.join("\n"))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn capture_collects_this_threads_records_at_every_level() {
		let ((), logs) = capture(|| {
			log::trace!("trace line");
			log::error!("error line");
			std::thread::scope(|s| {
				s.spawn(|| log::error!("other thread"));
			});
		});
		assert_eq!(logs, "TRACE trace line\nERROR error line");
		let ((), after) = capture(|| {});
		assert!(after.is_empty(), "{after}");
	}
}
