use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, error::TryRecvError, Receiver};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use crate::bc_protocol::connection::mock::{reply_200_empty, reply_err_code, MockConnection};

/// Object classes the camera attributed an active alarm to, parsed
/// from the comma-separated `AItype` of an `AlarmEvent`.
///
/// `other` is the camera's own `other` token (battery cameras report
/// PIR-triggered motion this way) and is also set when no person,
/// vehicle or animal is reported, so every active alarm carries at
/// least one class.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiDetections {
	/// `people`
	pub person: bool,
	/// `vehicle`
	pub vehicle: bool,
	/// `dog_cat`
	pub animal: bool,
	/// `other`, or no recognised class
	pub other: bool,
}

impl AiDetections {
	/// Classify an active alarm from its `AItype` field
	pub fn from_ai_type(ai_type: Option<&str>) -> Self {
		let mut detections = Self::default();
		for token in ai_type.unwrap_or_default().split(',') {
			match token.trim() {
				"people" => detections.person = true,
				"vehicle" => detections.vehicle = true,
				"dog_cat" => detections.animal = true,
				"other" => detections.other = true,
				_ => {}
			}
		}
		if !(detections.person || detections.vehicle || detections.animal) {
			detections.other = true;
		}
		detections
	}
}

/// Motion Status that the callback can send
#[derive(Clone, Copy, Debug)]
pub enum MotionStatus {
	/// Sent when motion is first detected, and again whenever the
	/// camera re-reports the alarm (e.g. its AI classification changes)
	Start(Instant, AiDetections),
	/// Sent when motion stops
	Stop(Instant),
	/// Sent when an Alarm about something other than motion was received
	NoChange(Instant),
}

/// A handle on current motion related events comming from the camera
///
/// When this object is dropped the motion events are stopped
pub struct MotionData {
	handle: JoinSet<Result<()>>,
	cancel: CancellationToken,
	rx: Receiver<Result<MotionStatus>>,
	last_update: MotionStatus,
	/// Cached at construction so the `Drop` impl can spawn its
	/// detached cancel-and-join task without calling
	/// `tokio::runtime::Handle::current()` — that call panics if Drop
	/// runs from a thread with no current runtime, which can happen
	/// if a `MotionData` ever crosses a non-tokio drop site.
	rt_handle: tokio::runtime::Handle,
}

impl MotionData {
	/// Get if motion has been detected. Returns None if
	/// no motion data has yet been received from the camera
	///
	/// An error is raised if the motion connection to the camera is dropped
	pub fn motion_detected(&mut self) -> Result<Option<bool>> {
		self.consume_motion_events()?;
		Ok(match &self.last_update {
			MotionStatus::Start(..) => Some(true),
			MotionStatus::Stop(_) => Some(false),
			MotionStatus::NoChange(_) => None,
		})
	}

	/// Get if motion has been detected within given duration. Returns None if
	/// no motion data has yet been received from the camera
	///
	/// An error is raised if the motion connection to the camera is dropped
	pub fn motion_detected_within(&mut self, duration: Duration) -> Result<Option<bool>> {
		self.consume_motion_events()?;
		Ok(match &self.last_update {
			MotionStatus::Start(..) => Some(true),
			MotionStatus::Stop(time) => Some((Instant::now() - *time) < duration),
			MotionStatus::NoChange(_) => None,
		})
	}

	/// Consume the motion events diretly
	///
	/// An error is raised if the motion connection to the camera is dropped
	pub fn consume_motion_events(&mut self) -> Result<Vec<MotionStatus>> {
		let mut results: Vec<MotionStatus> = vec![];
		loop {
			match self.rx.try_recv() {
				Ok(motion) => results.push(motion?),
				Err(TryRecvError::Empty) => break,
				Err(e) => return Err(Error::from(e)),
			}
		}
		if let Some(last) = results.last() {
			self.last_update = *last;
		}
		Ok(results)
	}

	/// Await a new motion event
	///
	///
	pub async fn next_motion(&mut self) -> Result<MotionStatus> {
		let motions = self.consume_motion_events()?;
		if let Some(last) = motions.last() {
			Ok(*last)
		} else if let Some(motion) = self.rx.recv().await {
			let motion = motion?;
			self.last_update = motion;
			Ok(motion)
		} else {
			Err(Error::Other("Motion dropped"))
		}
	}

	/// Wait for the motion to stop
	///
	/// It must be stopped for at least the given duration
	pub async fn await_stop(&mut self, duration: Duration) -> Result<()> {
		let motions = self.consume_motion_events()?;
		let mut last_motion = motions.last().copied();
		loop {
			if let Some(MotionStatus::Stop(time)) = last_motion {
				// In stop state
				if duration.is_zero() || (Instant::now() - time) > duration {
					return Ok(());
				} else {
					// Schedule a sleep or wait for motion to start.
					// `saturating_sub` covers the TOCTOU between the
					// check above and this subtraction — under heavy
					// scheduler load `Instant::now() - time` can flip
					// past `duration` and naive subtraction underflows.
					let remaining_sleep = duration.saturating_sub(Instant::now() - time);
					let result = tokio::select! {
						_ = tokio::time::sleep(remaining_sleep) => {None},
						v = async {
							loop {
								match self.next_motion().await {
									n @ Ok(MotionStatus::Start(..)) => {return n;},
									n @ Err(_) => {return n;},
									_ => {continue;}
								}
							}
						} => {Some(v)}
					};
					if let Some(v) = result {
						v?;
					} else {
						return Ok(());
					}
				}
			}
			last_motion = Some(self.next_motion().await?);
		}
	}

	/// Wait for the motion to start
	///
	/// The motion must have a minimum duration as given
	pub async fn await_start(&mut self, duration: Duration) -> Result<()> {
		let motions = self.consume_motion_events()?;
		let mut last_motion = motions.last().copied();
		loop {
			if let Some(MotionStatus::Start(time, _)) = last_motion {
				// In start state
				if duration.is_zero() || (Instant::now() - time) > duration {
					return Ok(());
				} else {
					// `saturating_sub` mirrors `await_stop` above:
					// guards the TOCTOU on the duration computation.
					let remaining_sleep = duration.saturating_sub(Instant::now() - time);
					let result = tokio::select! {
						_ = tokio::time::sleep(remaining_sleep) => {None},
						v = async {
							loop {
								match self.next_motion().await {
									n @ Ok(MotionStatus::Stop(_)) => {return n;},
									n @ Err(_) => {return n;},
									_ => {continue;}
								}
							}
						} => {Some(v)}
					};
					if let Some(v) = result {
						v?;
					} else {
						return Ok(());
					}
				}
			}
			last_motion = Some(self.next_motion().await?);
		}
	}
}

impl BcCamera {
	/// This message tells the camera to send the motion events to us
	/// Which are the received on msgid 33
	async fn start_motion_query(&self) -> Result<u16> {
		self.has_ability_rw("motion").await?;
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_MOTION_REQUEST, msg_num).await?;
		let msg = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_MOTION_REQUEST,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				..Default::default()
			}),
		};

		sub.send(msg).await?;

		let msg = sub.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(msg_num)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the request to start motion",
			})
		}
	}

	/// This returns a data structure which can be used to
	/// query motion events
	pub async fn listen_on_motion(&self) -> Result<MotionData> {
		self.start_motion_query().await?;

		let connection = self.get_connection();

		// After start_motion_query (MSG_ID 31) the camera sends motion messages
		// when whenever motion is detected.
		let (tx, rx) = channel(20);

		let mut set = JoinSet::new();
		let channel_id = self.channel_id;
		let cancel = CancellationToken::new();
		let thread_cancel = cancel.clone();
		set.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					let mut sub = connection.subscribe_to_id(MSG_ID_MOTION).await?;

					loop {
						tokio::task::yield_now().await;
						let msg = sub.recv().await;
						let status = match msg {
							Ok(motion_msg) => {
								if let BcBody::ModernMsg(ModernMsg {
									payload:
										Some(BcPayloads::BcXml(BcXml {
											alarm_event_list: Some(alarm_event_list),
											..
										})),
									..
								}) = motion_msg.body
								{
									let mut result = MotionStatus::NoChange(Instant::now());
									for alarm_event in &alarm_event_list.alarm_events {
										if alarm_event.channel_id == channel_id {
											if alarm_event.status != "none"
												|| alarm_event
													.ai_type
													.as_ref()
													.map(|ai_type| ai_type != "none")
													.unwrap_or(false)
											{
												result = MotionStatus::Start(
													Instant::now(),
													AiDetections::from_ai_type(
														alarm_event.ai_type.as_deref(),
													),
												);
												break;
											} else {
												result = MotionStatus::Stop(Instant::now());
												break;
											}
										}
									}
									Ok(result)
								} else {
									Ok(MotionStatus::NoChange(Instant::now()))
								}
							}
							// On connection drop we stop
							Err(e) => Err(e),
						};

						if tx.send(status).await.is_err() {
							// Motion receiver has been dropped
							break;
						}
					}
					Ok(())
				} => v,
			}
		});

		Ok(MotionData {
			handle: set,
			cancel,
			rx,
			last_update: MotionStatus::NoChange(Instant::now()),
			rt_handle: tokio::runtime::Handle::current(),
		})
	}
}

impl MotionData {
	/// Construct a `MotionData` wired to a caller-supplied channel.
	///
	/// Gated on the `test-util` feature. Tests drive the returned handle
	/// by sending `MotionStatus` values on the matching `Sender` — no
	/// camera, no `JoinSet` task. The internal cancel token is unused
	/// (no background task to cancel) but is kept so `Drop` stays a
	/// no-op cancel.
	#[cfg(any(test, feature = "test-util"))]
	pub fn test_new(rx: tokio::sync::mpsc::Receiver<Result<MotionStatus>>) -> Self {
		Self {
			handle: JoinSet::new(),
			cancel: CancellationToken::new(),
			rx,
			last_update: MotionStatus::NoChange(Instant::now()),
			rt_handle: tokio::runtime::Handle::current(),
		}
	}
}

impl Drop for MotionData {
	fn drop(&mut self) {
		log::trace!("Drop MotionData");
		self.cancel.cancel();
		let mut handle = std::mem::take(&mut self.handle);
		// Use the cached runtime handle rather than `Handle::current()`
		// so Drop is safe on any thread.
		self.rt_handle.spawn(async move {
			while handle.join_next().await.is_some() {}
			log::trace!("Dropped MotionData");
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn listen_on_motion_happy_path_returns_handle() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let md = cam.listen_on_motion().await.ok();
		assert!(md.is_some(), "listen_on_motion should succeed");
		// Drop MotionData; its Drop cancels the background task.
		drop(md);
	}

	#[tokio::test]
	async fn listen_on_motion_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let err = cam.listen_on_motion().await.err().expect("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn listen_on_motion_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.listen_on_motion().await.err().expect("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	// ---- AItype classification ----

	#[test]
	fn ai_detections_maps_each_wire_class() {
		for (ai_type, expected) in [
			(
				"people",
				AiDetections {
					person: true,
					..Default::default()
				},
			),
			(
				"vehicle",
				AiDetections {
					vehicle: true,
					..Default::default()
				},
			),
			(
				"dog_cat",
				AiDetections {
					animal: true,
					..Default::default()
				},
			),
			(
				"other",
				AiDetections {
					other: true,
					..Default::default()
				},
			),
		] {
			assert_eq!(
				AiDetections::from_ai_type(Some(ai_type)),
				expected,
				"AItype {ai_type:?}"
			);
		}
	}

	#[test]
	fn ai_detections_splits_comma_separated_list() {
		assert_eq!(
			AiDetections::from_ai_type(Some("people, dog_cat")),
			AiDetections {
				person: true,
				animal: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn ai_detections_keeps_explicit_other_alongside_a_class() {
		assert_eq!(
			AiDetections::from_ai_type(Some("vehicle,other")),
			AiDetections {
				vehicle: true,
				other: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn ai_detections_unclassified_alarm_is_other() {
		// `MD` with `AItype` absent, `none`, or a class we don't map
		// (`face`, `package`) is still motion — it lands in `other`.
		let other_only = AiDetections {
			other: true,
			..Default::default()
		};
		for ai_type in [None, Some("none"), Some(""), Some("face"), Some("package")] {
			assert_eq!(
				AiDetections::from_ai_type(ai_type),
				other_only,
				"AItype {ai_type:?}"
			);
		}
	}

	// ---- MotionData state-machine tests driven by `test_new` ----

	#[tokio::test]
	async fn motion_detected_none_until_event() {
		let (_tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// No events produced yet → `last_update` is NoChange → returns
		// None.
		assert_eq!(md.motion_detected().expect("ok"), None);
	}

	#[tokio::test]
	async fn motion_detected_after_start_event_is_true() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		// Yield so the channel is drained on try_recv.
		tokio::task::yield_now().await;
		assert_eq!(md.motion_detected().expect("ok"), Some(true));
	}

	#[tokio::test]
	async fn motion_detected_after_stop_event_is_false() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		tokio::task::yield_now().await;
		assert_eq!(md.motion_detected().expect("ok"), Some(false));
	}

	#[tokio::test]
	async fn motion_detected_within_reports_based_on_stop_time() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Stop "1 hour ago": (now - stop_time) > 10ms → returns Some(false).
		tx.send(Ok(MotionStatus::Stop(
			Instant::now() - Duration::from_secs(3600),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		assert_eq!(
			md.motion_detected_within(Duration::from_millis(10))
				.expect("ok"),
			Some(false)
		);
	}

	#[tokio::test]
	async fn motion_detected_within_returns_true_inside_window() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		tokio::task::yield_now().await;
		assert_eq!(
			md.motion_detected_within(Duration::from_secs(60))
				.expect("ok"),
			Some(true)
		);
	}

	#[tokio::test]
	async fn motion_detected_within_start_is_some_true() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		assert_eq!(
			md.motion_detected_within(Duration::from_secs(60))
				.expect("ok"),
			Some(true)
		);
	}

	#[tokio::test]
	async fn consume_motion_events_propagates_err() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Err(Error::Other("boom"))).await.unwrap();
		tokio::task::yield_now().await;
		assert!(md.consume_motion_events().is_err());
	}

	#[tokio::test]
	async fn next_motion_consumes_buffered_event() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		let t = Instant::now();
		tx.send(Ok(MotionStatus::Start(t, AiDetections::default())))
			.await
			.unwrap();
		tokio::task::yield_now().await;
		let status = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::Start(..)));
	}

	#[tokio::test]
	async fn next_motion_awaits_fresh_event() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Nothing buffered; spawn producer.
		let h = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.send(Ok(MotionStatus::Stop(Instant::now())))
				.await
				.unwrap();
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::Stop(_)));
		h.await.unwrap();
	}

	#[tokio::test]
	async fn next_motion_returns_err_when_channel_closed() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		drop(tx);
		// With no sender alive, try_recv yields Disconnected inside
		// consume_motion_events → DroppedConnectionTry. If the branch
		// ever changes to hit `rx.recv().await` → None, the Other arm
		// also counts.
		let err = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(
			err,
			Error::DroppedConnectionTry(_) | Error::Other(_)
		));
	}

	#[tokio::test]
	async fn await_stop_zero_duration_returns_immediately() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(Duration::from_millis(200), md.await_stop(Duration::ZERO))
			.await
			.expect("did not hang")
			.expect("ok");
	}

	#[tokio::test]
	async fn await_start_zero_duration_returns_immediately() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(Duration::from_millis(200), md.await_start(Duration::ZERO))
			.await
			.expect("did not hang")
			.expect("ok");
	}

	#[tokio::test]
	async fn await_stop_waits_then_returns() {
		// Start stopped-long-enough: (now - stop_time) > duration triggers
		// the fast-path "already satisfied" exit through the `else` arm.
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(
			Instant::now() - Duration::from_secs(3600),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(
			Duration::from_millis(200),
			md.await_stop(Duration::from_millis(10)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	#[tokio::test]
	async fn await_start_waits_then_returns() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now() - Duration::from_secs(3600),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(
			Duration::from_millis(200),
			md.await_start(Duration::from_millis(10)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	#[tokio::test]
	async fn await_stop_sleep_satisfies_after_recent_stop() {
		// Stop is "now" → (now - now) < duration → schedules sleep for
		// the remaining window. Sleep wins the select since no Start
		// event arrives; loop returns Ok.
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(
			Duration::from_millis(200),
			md.await_stop(Duration::from_millis(20)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	#[tokio::test]
	async fn await_start_sleep_satisfies_after_recent_start() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		tokio::time::timeout(
			Duration::from_millis(200),
			md.await_start(Duration::from_millis(20)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	#[tokio::test]
	async fn await_stop_transitions_from_nochange() {
		// Initial NoChange → falls through to `next_motion().await` and
		// loops until a Stop arrives with enough age.
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tokio::spawn(async move {
			tx.send(Ok(MotionStatus::Stop(
				Instant::now() - Duration::from_secs(3600),
			)))
			.await
			.unwrap();
		});
		tokio::time::timeout(Duration::from_millis(300), md.await_stop(Duration::ZERO))
			.await
			.expect("did not hang")
			.expect("ok");
	}

	#[tokio::test]
	async fn await_stop_does_not_panic_on_aged_stop_within_duration() {
		// Regression: `duration - (Instant::now() - time)` panicked
		// on Duration underflow if `Instant::now() - time` flipped past
		// `duration` between the if-check and the subtraction.
		// `saturating_sub` returns ZERO and the sleep arm fires instantly.
		// Build the setup: stop ~5 ms ago, request a 4 ms window — the
		// inner check `(now - time) > duration` may be false at first but
		// flip true between the check and the sub on a busy scheduler.
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Stop(
			Instant::now() - Duration::from_millis(5),
		)))
		.await
		.unwrap();
		tokio::task::yield_now().await;
		// Whether the if-arm or the else-with-saturating-sub fires, the
		// call must complete without panic.
		tokio::time::timeout(
			Duration::from_millis(200),
			md.await_stop(Duration::from_millis(4)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	#[tokio::test]
	async fn await_start_transitions_from_nochange() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tokio::spawn(async move {
			tx.send(Ok(MotionStatus::Start(
				Instant::now() - Duration::from_secs(3600),
				AiDetections::default(),
			)))
			.await
			.unwrap();
		});
		tokio::time::timeout(Duration::from_millis(300), md.await_start(Duration::ZERO))
			.await
			.expect("did not hang")
			.expect("ok");
	}

	// ---- Inner listen_on_motion task body tests ----
	// These drive the MSG_ID_MOTION subscription loop by pushing frames
	// via the mock injector. The loop lives inside a spawned task inside
	// `listen_on_motion`; waiting on `md.next_motion()` drains it.

	fn push_motion_start(
		injector: &crate::bc_protocol::connection::mock::MockInjector,
		channel_id: u8,
	) -> tokio::task::JoinHandle<()> {
		let injector = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				BcXml {
					alarm_event_list: Some(AlarmEventList {
						version: "1".into(),
						alarm_events: vec![AlarmEvent {
							version: "1".into(),
							channel_id,
							status: "MD".into(),
							recording: 0,
							timeStamp: 0,
							ai_type: None,
						}],
					}),
					..Default::default()
				},
			);
			injector.push(push).await;
		})
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_start_on_motion_frame() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let mut md = cam.listen_on_motion().await.expect("ok");

		// Give the subscription time to register.
		tokio::time::sleep(Duration::from_millis(30)).await;
		let _ = push_motion_start(&injector, 0).await;

		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		// Plain `MD` with no `AItype` is unclassified motion.
		let MotionStatus::Start(_, detections) = status else {
			panic!("expected Start, got {status:?}");
		};
		assert_eq!(
			detections,
			AiDetections {
				other: true,
				..Default::default()
			}
		);
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_stop_on_none_status_frame() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let mut md = cam.listen_on_motion().await.expect("ok");

		tokio::time::sleep(Duration::from_millis(30)).await;
		let injector_c = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id: 0,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				BcXml {
					alarm_event_list: Some(AlarmEventList {
						version: "1".into(),
						alarm_events: vec![AlarmEvent {
							version: "1".into(),
							channel_id: 0,
							status: "none".into(),
							recording: 0,
							timeStamp: 0,
							ai_type: Some("none".into()),
						}],
					}),
					..Default::default()
				},
			);
			injector_c.push(push).await;
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::Stop(_)));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_start_on_ai_type_not_none() {
		// status=="none" but ai_type != "none" → fall into the "motion
		// detected" branch.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let mut md = cam.listen_on_motion().await.expect("ok");

		tokio::time::sleep(Duration::from_millis(30)).await;
		let injector_c = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id: 0,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				BcXml {
					alarm_event_list: Some(AlarmEventList {
						version: "1".into(),
						alarm_events: vec![AlarmEvent {
							version: "1".into(),
							channel_id: 0,
							status: "none".into(),
							recording: 0,
							timeStamp: 0,
							ai_type: Some("people".into()),
						}],
					}),
					..Default::default()
				},
			);
			injector_c.push(push).await;
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		let MotionStatus::Start(_, detections) = status else {
			panic!("expected Start, got {status:?}");
		};
		assert_eq!(
			detections,
			AiDetections {
				person: true,
				..Default::default()
			}
		);
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_returns_nochange_on_empty_xml() {
		// Frame without alarm_event_list → NoChange path.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let mut md = cam.listen_on_motion().await.expect("ok");

		tokio::time::sleep(Duration::from_millis(30)).await;
		let injector_c = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id: 0,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				BcXml::default(),
			);
			injector_c.push(push).await;
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::NoChange(_)));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_skips_other_channel_ids() {
		// Alarm event for a different channel_id → falls through to
		// NoChange.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let mut md = cam.listen_on_motion().await.expect("ok");

		tokio::time::sleep(Duration::from_millis(30)).await;
		let injector_c = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id: 0,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				BcXml {
					alarm_event_list: Some(AlarmEventList {
						version: "1".into(),
						alarm_events: vec![AlarmEvent {
							version: "1".into(),
							channel_id: 7, // not our channel (0)
							status: "MD".into(),
							recording: 0,
							timeStamp: 0,
							ai_type: None,
						}],
					}),
					..Default::default()
				},
			);
			injector_c.push(push).await;
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::NoChange(_)));
	}

	/// `next_motion` falls through to `rx.recv().await` returning `None`
	/// (sender dropped while we were already past the buffered drain),
	/// surfacing `Other("Motion dropped")`.
	#[tokio::test]
	async fn next_motion_returns_other_when_sender_dropped_after_drain() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Spawn a task that closes the sender after a short delay so
		// `next_motion` enters its inner `rx.recv().await`, which then
		// returns `None` and surfaces `Other("Motion dropped")`.
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(20)).await;
			drop(tx);
		});
		let err = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(err, Error::Other("Motion dropped")));
	}

	/// `await_stop` schedules the sleep for a recent stop, but a Start
	/// arrives before the sleep elapses → the select's async branch
	/// wins, propagating the Start back into the outer loop. The next
	/// iteration re-enters `next_motion` for a fresh stop.
	#[tokio::test]
	async fn await_stop_resets_when_motion_starts_during_sleep() {
		let (tx, rx) = channel::<Result<MotionStatus>>(8);
		let mut md = MotionData::test_new(rx);
		// Initial Stop just now → schedules a 50 ms sleep.
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		let tx_c = tx.clone();
		tokio::spawn(async move {
			// Start arrives 10 ms in → wins the select; outer loop loops.
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx_c.send(Ok(MotionStatus::Start(
				Instant::now(),
				AiDetections::default(),
			)))
			.await
			.unwrap();
			// Then Stop "long ago" → second iteration's sleep is ZERO so
			// fast-path returns Ok immediately.
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx_c.send(Ok(MotionStatus::Stop(
				Instant::now() - Duration::from_secs(60),
			)))
			.await
			.unwrap();
		});
		tokio::time::timeout(
			Duration::from_millis(500),
			md.await_stop(Duration::from_millis(50)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	/// Inverse: `await_start` schedules a sleep for a recent Start, but
	/// a Stop arrives → loop continues and a subsequent Start with
	/// enough age completes via the fast-path.
	#[tokio::test]
	async fn await_start_resets_when_motion_stops_during_sleep() {
		let (tx, rx) = channel::<Result<MotionStatus>>(8);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		let tx_c = tx.clone();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx_c.send(Ok(MotionStatus::Stop(Instant::now())))
				.await
				.unwrap();
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx_c.send(Ok(MotionStatus::Start(
				Instant::now() - Duration::from_secs(60),
				AiDetections::default(),
			)))
			.await
			.unwrap();
		});
		tokio::time::timeout(
			Duration::from_millis(500),
			md.await_start(Duration::from_millis(50)),
		)
		.await
		.expect("did not hang")
		.expect("ok");
	}

	/// `await_stop` propagates an error returned by the inner
	/// `next_motion()` call when the channel disconnects mid-wait.
	#[tokio::test]
	async fn await_stop_propagates_inner_err() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Recent Stop → enters sleep branch, then we push an Err which
		// the inner loop only rejects on Start/Err — it returns Err.
		tx.send(Ok(MotionStatus::Stop(Instant::now())))
			.await
			.unwrap();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.send(Err(Error::Other("boom"))).await.unwrap();
		});
		let err = tokio::time::timeout(
			Duration::from_millis(500),
			md.await_stop(Duration::from_millis(200)),
		)
		.await
		.expect("did not hang")
		.expect_err("should fail");
		assert!(matches!(err, Error::Other(_)));
	}

	#[tokio::test]
	async fn await_start_propagates_inner_err() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::Start(
			Instant::now(),
			AiDetections::default(),
		)))
		.await
		.unwrap();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.send(Err(Error::Other("boom"))).await.unwrap();
		});
		let err = tokio::time::timeout(
			Duration::from_millis(500),
			md.await_start(Duration::from_millis(200)),
		)
		.await
		.expect("did not hang")
		.expect_err("should fail");
		assert!(matches!(err, Error::Other(_)));
	}
}
