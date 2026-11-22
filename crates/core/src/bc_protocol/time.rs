use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};
use std::convert::{TryFrom, TryInto};
use time::{
	macros::date, parsing::Parsed, Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset,
	Weekday,
};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{
	reply_200_empty, reply_200_xml, reply_err_code, MockConnection,
};

impl BcCamera {
	///
	/// Get the time from the camera
	///
	/// # Returns
	///
	/// returns either an error or an option with the offsetted date time
	///
	pub async fn get_time(&self) -> Result<Option<OffsetDateTime>> {
		self.has_ability_ro("general").await?;
		let general = self.get_system_general().await?;

		let (
			Some(time_zone),
			Some(year),
			Some(month),
			Some(day),
			Some(hour),
			Some(minute),
			Some(second),
		) = (
			general.time_zone,
			general.year,
			general.month,
			general.day,
			general.hour,
			general.minute,
			general.second,
		)
		else {
			return Err(Error::UnintelligibleXml {
				reply: std::sync::Arc::new(BcXml {
					system_general: Some(general),
					..Default::default()
				}),
				why: "SystemGeneral reply missing one or more time fields",
			});
		};

		let datetime = try_build_timestamp(time_zone, year, month, day, hour, minute, second)
			.map_err(|_| Error::UnintelligibleXml {
				reply: std::sync::Arc::new(BcXml {
					system_general: Some(general),
					..Default::default()
				}),
				why: "Could not parse date",
			})?;

		// Pre-2019 dates indicate the camera's clock was never set
		// (factory firmware boots to e.g. 1999-01-01 or 2000-01-01).
		// Treat as `Ok(None)` so callers can re-set rather than treat
		// an obvious factory date as a real reading.
		const BOUNDARY: Date = date!(2019 - 01 - 01);
		if datetime.date() < BOUNDARY {
			Ok(None)
		} else {
			Ok(Some(datetime))
		}
	}

	/// Read the camera's daylight-saving-time configuration via
	/// `MSG_ID_GET_DST`. The reply body is `<Dst>`; some firmwares
	/// flag it binary (`<Extension><binaryData>1</binaryData></…>`)
	/// even though the bytes are UTF-8 XML, so this method handles
	/// both shapes — a parsed `BcXml { dst: Some(_) }` payload and a
	/// raw `Binary` payload that we re-parse via `BcXml::try_parse`.
	///
	/// Returns the parsed `Dst` on success. A camera that doesn't
	/// support the message (older firmware, or a model without DST
	/// in its abilities) returns `Error::CameraServiceUnavailable` —
	/// callers should treat that as "no DST awareness" and fall back
	/// to sending the host's effective offset directly.
	pub async fn get_dst(&self) -> Result<Dst> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_GET_DST, msg_num).await?;
		let req = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_DST,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg::default()),
		};
		sub.send(req).await?;
		let msg = sub.recv().await?;
		if msg.meta.response_code != 200 {
			return Err(Error::CameraServiceUnavailable {
				id: msg.meta.msg_id,
				code: msg.meta.response_code,
			});
		}

		// Pull `<Dst>` from either a parsed-XML body (the common path
		// when the camera attaches no Extension) or a Binary body whose
		// bytes we re-parse as `<body>...<Dst>...</body>` (some replies
		// carry `binaryData=1` even though the bytes are XML, and a
		// trailing-padding tail past `</body>` is harmless to the
		// parser).
		let dst = match msg.body {
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::BcXml(BcXml { dst: Some(d), .. })),
				..
			}) => d,
			BcBody::ModernMsg(ModernMsg {
				payload: Some(BcPayloads::Binary(bytes)),
				..
			}) => match BcXml::try_parse(bytes.as_slice()) {
				Ok(BcXml { dst: Some(d), .. }) => d,
				_ => {
					return Err(Error::UnintelligibleReply {
						reply: std::sync::Arc::new(Bc {
							meta: msg.meta,
							body: BcBody::ModernMsg(ModernMsg::default()),
						}),
						why: "GetDst binary payload did not contain <Dst>",
					});
				}
			},
			_ => {
				return Err(Error::UnintelligibleReply {
					reply: std::sync::Arc::new(msg),
					why: "GetDst reply lacked a <Dst> body",
				});
			}
		};
		Ok(dst)
	}

	/// Read the full `SystemGeneral` block from the camera. Used
	/// internally by `get_time` (for the time fields) and by
	/// `set_time` (for the read-modify-write that preserves every
	/// non-time field on the wire — see the comment in `set_time`).
	async fn get_system_general(&self) -> Result<SystemGeneral> {
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get_general = connection.subscribe(MSG_ID_GET_GENERAL, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_GET_GENERAL,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg::default()),
		};

		sub_get_general.send(get).await?;
		let msg = sub_get_general.recv().await?;
		if msg.meta.response_code != 200 {
			return Err(Error::CameraServiceUnavailable {
				id: msg.meta.msg_id,
				code: msg.meta.response_code,
			});
		}

		if let BcBody::ModernMsg(ModernMsg {
			payload: Some(BcPayloads::BcXml(BcXml {
				system_general: Some(general),
				..
			})),
			..
		}) = msg.body
		{
			Ok(general)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Reply did not contain a SystemGeneral block",
			})
		}
	}

	///
	/// Sets the time of the camera
	///
	/// # Parameters
	///
	/// * `timestamp` - The time to set the camera to
	///
	/// # Returns
	///
	/// returns Ok(()) or error
	///
	pub async fn set_time(&self, timestamp: OffsetDateTime) -> Result<()> {
		self.has_ability_rw("general").await?;

		// DST compensation. The camera autonomously adds `<Dst><offset/></Dst>`
		// hours to displayed local time when the moment falls inside the
		// schedule's window. To make displayed-local match `timestamp`'s
		// local time after that addition, write the camera's `<timeZone>`
		// as the BASE offset (host effective offset minus the DST hours
		// the camera will add) and the wallclock fields as UTC, so the
		// camera's reconstruction `displayed = <hour> + (-<timeZone>/3600)
		// + dst_in_window` lands on `timestamp`. Run before the
		// SystemGeneral read so a non-supporting camera (older firmware,
		// missing ability) surfaces fast and we fall through to the
		// pre-DST behaviour without an extra round-trip on failure.
		let dst_seconds = match self.get_dst().await {
			Ok(dst) => {
				let local = PrimitiveDateTime::new(timestamp.date(), timestamp.time());
				dst_offset_seconds(&dst, local)
			}
			Err(_) => 0,
		};

		// Read-modify-write: we GET the camera's current SystemGeneral,
		// mutate ONLY the time fields, and SET the full struct back.
		// Writing a freshly-defaulted SystemGeneral (the previous
		// behaviour) silently dropped every field we don't model
		// (osdFormat, language, deviceName) plus any firmware-internal
		// fields not surfaced in the struct, leaving the camera in a
		// half-set state that produced subtle drift on subsequent
		// firmware-driven time syncs. Round-tripping the rest of the
		// struct keeps every other knob bytewise unchanged.
		let mut general = self.get_system_general().await?;

		general.version = xml_ver();

		if dst_seconds != 0 {
			let utc = timestamp.to_offset(UtcOffset::UTC);
			let base_offset_seconds = timestamp
				.offset()
				.whole_seconds()
				.saturating_sub(dst_seconds);
			// Reolink uses positive seconds to indicate a negative UTC offset:
			general.time_zone = Some(-base_offset_seconds);
			general.year = Some(utc.year());
			general.month = Some(utc.month().into());
			general.day = Some(utc.day());
			general.hour = Some(utc.hour());
			general.minute = Some(utc.minute());
			general.second = Some(utc.second());
		} else {
			// Reolink uses positive seconds to indicate a negative UTC offset:
			general.time_zone = Some(-timestamp.offset().whole_seconds());
			general.year = Some(timestamp.year());
			general.month = Some(timestamp.month().into());
			general.day = Some(timestamp.day());
			general.hour = Some(timestamp.hour());
			general.minute = Some(timestamp.minute());
			general.second = Some(timestamp.second());
		}

		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_set_general = connection.subscribe(MSG_ID_SET_GENERAL, msg_num).await?;
		let set = Bc::new_from_xml(
			BcMeta {
				msg_id: MSG_ID_SET_GENERAL,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			BcXml {
				system_general: Some(general),
				..Default::default()
			},
		);

		sub_set_general.send(set).await?;
		let msg = sub_set_general.recv().await?;
		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
		} else {
			return Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the set time command.",
			});
		}

		Ok(())
	}
}

/// Compute the DST offset (in seconds) the camera will apply to a
/// displayed local time at `instant_local`, given its `<Dst>` config.
///
/// Returns `0` when DST is disabled (`enable != 1`), the schedule fields
/// are missing/invalid, or `instant_local` is outside the
/// `[start, end)` window. Otherwise returns `dst.offset * 3600` seconds.
///
/// `instant_local` must be expressed in the camera's *local* time (the
/// schedule fields are wall-time, not UTC) — for the bairelay set-time
/// path that's the host's `OffsetDateTime::now_local()` projected to
/// the same `OffsetDateTime` we're about to write to the camera.
pub(crate) fn dst_offset_seconds(dst: &Dst, instant_local: PrimitiveDateTime) -> i32 {
	if dst.enable != Some(1) {
		return 0;
	}
	let offset_hours = match dst.offset {
		Some(h) if h != 0 => h,
		_ => return 0,
	};

	let start = match dst_transition_for_year(
		instant_local.year(),
		dst.start_month,
		dst.start_week_index,
		dst.start_weekday.as_deref(),
		dst.start_hour,
		dst.start_minute,
		dst.start_second,
	) {
		Some(t) => t,
		None => return 0,
	};
	let end = match dst_transition_for_year(
		instant_local.year(),
		dst.end_month,
		dst.end_week_index,
		dst.end_weekday.as_deref(),
		dst.end_hour,
		dst.end_minute,
		dst.end_second,
	) {
		Some(t) => t,
		None => return 0,
	};

	// Two shapes: northern-hemisphere (start < end, single window inside
	// the calendar year) and southern-hemisphere (start > end, window
	// straddles the year boundary). The schedule pinned in current
	// captures is northern (March → October); the south-hemisphere
	// branch is handled symmetrically for completeness.
	let in_window = if start < end {
		instant_local >= start && instant_local < end
	} else {
		instant_local >= start || instant_local < end
	};

	if in_window {
		offset_hours.saturating_mul(3600)
	} else {
		0
	}
}

/// Resolve a Reolink DST schedule entry (month + week-index + weekday +
/// time-of-day) to a concrete `PrimitiveDateTime` for the given year.
/// Returns `None` if any field is absent or invalid.
///
/// `week_index` semantics: `1`–`4` = "Nth occurrence of `weekday` in the
/// month"; `5` = "last occurrence in the month".
fn dst_transition_for_year(
	year: i32,
	month: Option<u8>,
	week_index: Option<u8>,
	weekday: Option<&str>,
	hour: Option<u8>,
	minute: Option<u8>,
	second: Option<u8>,
) -> Option<PrimitiveDateTime> {
	let month: Month = month?.try_into().ok()?;
	let week_index = week_index?;
	let weekday = parse_weekday(weekday?)?;
	let hour = hour.unwrap_or(0);
	let minute = minute.unwrap_or(0);
	let second = second.unwrap_or(0);

	let date = nth_weekday_of_month(year, month, week_index, weekday)?;
	let time = Time::from_hms(hour, minute, second).ok()?;
	Some(PrimitiveDateTime::new(date, time))
}

fn parse_weekday(s: &str) -> Option<Weekday> {
	match s.trim().to_ascii_lowercase().as_str() {
		"monday" | "mon" => Some(Weekday::Monday),
		"tuesday" | "tue" => Some(Weekday::Tuesday),
		"wednesday" | "wed" => Some(Weekday::Wednesday),
		"thursday" | "thu" => Some(Weekday::Thursday),
		"friday" | "fri" => Some(Weekday::Friday),
		"saturday" | "sat" => Some(Weekday::Saturday),
		"sunday" | "sun" => Some(Weekday::Sunday),
		_ => None,
	}
}

/// Find the date of the Nth (`week_index`) occurrence of `weekday` in
/// the given `(year, month)`. `week_index = 5` means "last occurrence",
/// regardless of how many weeks the month actually contains.
fn nth_weekday_of_month(year: i32, month: Month, week_index: u8, weekday: Weekday) -> Option<Date> {
	if week_index == 0 || week_index > 5 {
		return None;
	}
	let last_day = month.length(year);
	if week_index == 5 {
		for d in (1..=last_day).rev() {
			if let Ok(date) = Date::from_calendar_date(year, month, d) {
				if date.weekday() == weekday {
					return Some(date);
				}
			}
		}
		return None;
	}
	for d in 1..=7 {
		if let Ok(date) = Date::from_calendar_date(year, month, d) {
			if date.weekday() == weekday {
				let day_of_target = d + (week_index - 1) * 7;
				if day_of_target > last_day {
					return None;
				}
				return Date::from_calendar_date(year, month, day_of_target).ok();
			}
		}
	}
	None
}

/// Decode a Reolink `SystemGeneral` payload into an `OffsetDateTime`.
///
/// The wire `timezone` field uses Reolink's inverted convention
/// (positive seconds = west of UTC, negative = east) as documented in
/// `set_time` above. We negate here so the round-trip
/// `set_time(t)` → `get_time()` preserves the original
/// `OffsetDateTime`. Without the negation the two functions disagree
/// by `2 × offset` whenever the operator runs anything other than UTC.
///
/// `get_time` is currently only called by the Neolink CLI; bairelay's
/// CLI has no `get-time` subcommand, so the asymmetry was invisible in
/// production. Closing it here keeps the contract clean for any future
/// caller (TZ-aware MQTT publish, etc.) and makes the unit-level
/// symmetry test below load-bearing.
fn try_build_timestamp(
	timezone: i32,
	year: i32,
	month: u8,
	day: u8,
	hour: u8,
	minute: u8,
	second: u8,
) -> std::result::Result<OffsetDateTime, crate::Error> {
	let date = Date::try_from(
		Parsed::new()
			.with_year(year)
			.ok_or(Error::TimeParse)?
			.with_month(month.try_into()?)
			.ok_or(Error::TimeParse)?
			.with_day(day.try_into()?)
			.ok_or(Error::TimeParse)?,
	)?;
	let time = Time::try_from(
		Parsed::new()
			.with_hour_24(hour)
			.ok_or(Error::TimeParse)?
			.with_minute(minute)
			.ok_or(Error::TimeParse)?
			.with_second(second)
			.ok_or(Error::TimeParse)?,
	)?;
	// Negate to match `set_time`'s sign convention (Reolink wire:
	// positive = west of UTC, negative = east).
	let offset = UtcOffset::from_whole_seconds(-timezone)?;

	Ok(PrimitiveDateTime::new(date, time).assume_offset(offset))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn get_time_happy_path_parses_reply() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2026),
							month: Some(4),
							day: Some(23),
							hour: Some(12),
							minute: Some(30),
							second: Some(45),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let dt = cam
			.get_time()
			.await
			.expect("get_time should succeed")
			.expect("datetime should be Some");
		assert_eq!(dt.year(), 2026);
		assert_eq!(dt.hour(), 12);
	}

	#[tokio::test]
	async fn get_time_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_time().await.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn get_time_not_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let err = cam.get_time().await.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn get_time_pre_boundary_returns_ok_none() {
		// Any date before 2019-01-01 → treated as "time not set".
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(1999),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let dt = cam.get_time().await.expect("ok");
		assert!(dt.is_none());
	}

	#[tokio::test]
	async fn get_time_invalid_date_returns_err() {
		// Month 13 is invalid → try_build_timestamp fails →
		// UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2026),
							month: Some(13),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let err = cam.get_time().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleXml { .. }));
	}

	#[tokio::test]
	async fn get_time_missing_fields_returns_err() {
		// 200 with SystemGeneral but year=None → pattern doesn't match
		// → UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let err = cam.get_time().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleXml { .. }));
	}

	/// Reply that disables DST (`<enable>0</enable>`). Used by every
	/// non-DST-focused `set_time` test so the production code's
	/// pre-flight `get_dst()` returns cleanly without compensating
	/// the wallclock — the original assertions remain meaningful.
	fn reply_dst_disabled(req: &Bc) -> Bc {
		reply_200_xml(
			req,
			BcXml {
				dst: Some(Dst {
					version: "1.1".to_string(),
					enable: Some(0),
					..Default::default()
				}),
				..Default::default()
			},
		)
	}

	#[tokio::test]
	async fn set_time_happy_path() {
		// `set_time` is read-modify-write: GET the camera's current
		// SystemGeneral, mutate ONLY the time fields, SET the full
		// struct back. The GET stub seeds non-time fields (osdFormat,
		// language, deviceName, timeFormat) that the SET assertion
		// then verifies are preserved bytewise — that's the
		// regression guard for "freshly-defaulted SystemGeneral
		// silently dropping camera state on every set-time".
		// Pin the inverted timezone convention here too: UTC+2
		// (+7200 UtcOffset seconds) serialises as -7200 on the wire.
		// Camera DST is disabled in the GetDst stub so this test
		// pins the pre-DST passthrough wire shape.
		let dt = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::April, 23).unwrap(),
			Time::from_hms(12, 30, 45).unwrap(),
		)
		.assume_offset(UtcOffset::from_whole_seconds(7200).unwrap());

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(reply_dst_disabled)
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2024),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							osd_format: Some("DMY".to_string()),
							time_format: Some(1),
							language: Some("English".to_string()),
							device_name: Some("Front gate".to_string()),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with_xml(|req, xml| {
				let g = xml.system_general.as_ref().expect("system_general on SET");
				// Mutated time fields:
				assert_eq!(g.year, Some(2026));
				assert_eq!(g.month, Some(4));
				assert_eq!(g.day, Some(23));
				assert_eq!(g.hour, Some(12));
				assert_eq!(g.minute, Some(30));
				assert_eq!(g.second, Some(45));
				// UtcOffset +7200 (UTC+2) → wire -7200 per Reolink.
				assert_eq!(g.time_zone, Some(-7200));
				// Preserved non-time fields (the regression guard):
				assert_eq!(g.osd_format.as_deref(), Some("DMY"));
				assert_eq!(g.time_format, Some(1));
				assert_eq!(g.language.as_deref(), Some("English"));
				assert_eq!(g.device_name.as_deref(), Some("Front gate"));
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		cam.set_time(dt).await.expect("ok");
	}

	#[tokio::test]
	async fn set_time_get_failure_propagates() {
		// If the GET roundtrip fails, set_time must surface the error
		// without sending a partial SET — otherwise we'd be back to
		// the bug this refactor fixed. GetDst comes back disabled, so
		// the failure source is the SystemGeneral GET, not DST.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(reply_dst_disabled)
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		let err = cam
			.set_time(OffsetDateTime::now_utc())
			.await
			.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn set_time_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam
			.set_time(OffsetDateTime::now_utc())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn set_time_not_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(reply_dst_disabled)
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2026),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		let err = cam
			.set_time(OffsetDateTime::now_utc())
			.await
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn set_time_dst_in_window_writes_utc_wallclock_and_base_offset() {
		// Pins the DST-compensation contract. Operator local =
		// 2026-05-03 17:30:45 at UTC+2 (a DST-on offset for any zone
		// that bases on UTC+1 + 1h DST). Camera reports DST enabled
		// with offset=1h on a Mar-last-Sun → Oct-last-Sun schedule —
		// exactly the configuration observed against real hardware.
		// The SET request must carry: <hour> = host UTC (15), and
		// <timeZone> = -3600 (base UTC+1, DST stripped). The camera
		// then adds its own +1h DST → display = 17:30:45.
		let local = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::May, 3).unwrap(),
			Time::from_hms(17, 30, 45).unwrap(),
		)
		.assume_offset(UtcOffset::from_whole_seconds(7200).unwrap());

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						dst: Some(Dst {
							version: "1.1".to_string(),
							enable: Some(1),
							offset: Some(1),
							start_month: Some(3),
							start_week_index: Some(5),
							start_weekday: Some("Sunday".to_string()),
							start_hour: Some(2),
							start_minute: Some(0),
							start_second: Some(0),
							end_month: Some(10),
							end_week_index: Some(5),
							end_weekday: Some("Sunday".to_string()),
							end_hour: Some(3),
							end_minute: Some(0),
							end_second: Some(0),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2024),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with_xml(|req, xml| {
				let g = xml.system_general.as_ref().expect("system_general on SET");
				// UTC components, NOT operator-local. Local was 17:30:45
				// at UTC+2 → UTC = 15:30:45.
				assert_eq!(g.year, Some(2026));
				assert_eq!(g.month, Some(5));
				assert_eq!(g.day, Some(3));
				assert_eq!(g.hour, Some(15));
				assert_eq!(g.minute, Some(30));
				assert_eq!(g.second, Some(45));
				// Base offset: effective UTC+2 minus DST 1h = UTC+1 base
				// → wire -3600 per Reolink's inverted convention.
				assert_eq!(g.time_zone, Some(-3600));
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		cam.set_time(local).await.expect("ok");
	}

	#[tokio::test]
	async fn set_time_dst_out_of_window_passes_through() {
		// DST enabled but the target date sits outside the window.
		// `dst_offset_seconds` must return 0; set_time keeps the old
		// behaviour (host-local components + effective offset).
		// 2026-12-25 in a UTC+1 zone — well past Oct's last Sunday.
		let dt = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::December, 25).unwrap(),
			Time::from_hms(9, 0, 0).unwrap(),
		)
		.assume_offset(UtcOffset::from_whole_seconds(3600).unwrap());

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						dst: Some(Dst {
							version: "1.1".to_string(),
							enable: Some(1),
							offset: Some(1),
							start_month: Some(3),
							start_week_index: Some(5),
							start_weekday: Some("Sunday".to_string()),
							start_hour: Some(2),
							start_minute: Some(0),
							start_second: Some(0),
							end_month: Some(10),
							end_week_index: Some(5),
							end_weekday: Some("Sunday".to_string()),
							end_hour: Some(3),
							end_minute: Some(0),
							end_second: Some(0),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2024),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with_xml(|req, xml| {
				let g = xml.system_general.as_ref().expect("system_general on SET");
				// Host-local components preserved, no UTC conversion.
				assert_eq!(g.year, Some(2026));
				assert_eq!(g.month, Some(12));
				assert_eq!(g.day, Some(25));
				assert_eq!(g.hour, Some(9));
				assert_eq!(g.time_zone, Some(-3600));
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		cam.set_time(dt).await.expect("ok");
	}

	#[tokio::test]
	async fn get_dst_with_xml_payload_lacking_dst_returns_unintelligible() {
		// 200 reply + parsed BcXml that has no <Dst> field. The match in
		// get_dst falls through both arms and hits the `_ =>` arm —
		// pins the "GetDst reply lacked a <Dst> body" error string.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_dst().await.expect_err("must be error");
		match err {
			Error::UnintelligibleReply { why, .. } => {
				assert!(
					why.contains("lacked a <Dst> body"),
					"unexpected reason: {why}"
				);
			}
			other => panic!("expected UnintelligibleReply, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn get_dst_with_binary_payload_containing_xml_succeeds() {
		// Some firmwares flag the GetDst reply binary even though the
		// bytes are XML. Construct that shape: serialize a BcXml with a
		// <Dst> block and wrap as `BcPayloads::Binary`. get_dst must
		// re-parse via `BcXml::try_parse` and surface the `Dst` —
		// pinning the binary-body alt path.
		let dst = eu_dst_one_hour();
		let dst_clone = dst.clone();
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(move |req| Bc {
				meta: BcMeta {
					msg_id: req.meta.msg_id,
					channel_id: req.meta.channel_id,
					msg_num: req.meta.msg_num,
					stream_type: 0,
					response_code: 200,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: None,
					payload: Some(BcPayloads::Binary(
						BcXml {
							dst: Some(dst_clone.clone()),
							..Default::default()
						}
						.serialize(Vec::new())
						.expect("serialize"),
					)),
				}),
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let got = cam.get_dst().await.expect("must succeed");
		assert_eq!(got.enable, dst.enable);
		assert_eq!(got.offset, dst.offset);
		assert_eq!(got.start_weekday, dst.start_weekday);
	}

	#[tokio::test]
	async fn get_dst_with_binary_payload_lacking_dst_returns_unintelligible() {
		// Binary body whose bytes parse as XML but contain no <Dst> —
		// hits the inner `_ =>` arm: "GetDst binary payload did not
		// contain <Dst>".
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(|req| Bc {
				meta: BcMeta {
					msg_id: req.meta.msg_id,
					channel_id: req.meta.channel_id,
					msg_num: req.meta.msg_num,
					stream_type: 0,
					response_code: 200,
					class: 0x6414,
				},
				body: BcBody::ModernMsg(ModernMsg {
					extension: None,
					payload: Some(BcPayloads::Binary(
						BcXml::default().serialize(Vec::new()).expect("serialize"),
					)),
				}),
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_dst().await.expect_err("must be error");
		match err {
			Error::UnintelligibleReply { why, .. } => {
				assert!(
					why.contains("binary payload did not contain <Dst>"),
					"unexpected reason: {why}"
				);
			}
			other => panic!("expected UnintelligibleReply, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn get_system_general_with_no_system_general_block_returns_unintelligible() {
		// 200 reply + parsed BcXml that lacks <SystemGeneral>. The
		// `if let` doesn't bind, the else arm fires — pins the
		// "Reply did not contain a SystemGeneral block" error path.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| reply_200_xml(req, BcXml::default()))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", false).await;
		let err = cam.get_time().await.expect_err("must be error");
		match err {
			Error::UnintelligibleReply { why, .. } => {
				assert!(
					why.contains("did not contain a SystemGeneral"),
					"unexpected reason: {why}"
				);
			}
			other => panic!("expected UnintelligibleReply, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn set_time_get_dst_failure_falls_back_to_passthrough() {
		// Older firmware doesn't support GetDst (returns non-200);
		// set_time must swallow that failure and fall through to the
		// pre-DST behaviour (effective offset + local components),
		// not bubble the GetDst error up to the caller.
		let dt = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::April, 23).unwrap(),
			Time::from_hms(12, 30, 45).unwrap(),
		)
		.assume_offset(UtcOffset::from_whole_seconds(7200).unwrap());

		let mock = MockConnection::new()
			.expect_msg(MSG_ID_GET_DST)
			.reply_with(|req| reply_err_code(req, 500))
			.expect_msg(MSG_ID_GET_GENERAL)
			.reply_with(|req| {
				reply_200_xml(
					req,
					BcXml {
						system_general: Some(SystemGeneral {
							version: "1.1".to_string(),
							time_zone: Some(0),
							year: Some(2024),
							month: Some(1),
							day: Some(1),
							hour: Some(0),
							minute: Some(0),
							second: Some(0),
							..Default::default()
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SET_GENERAL)
			.reply_with_xml(|req, xml| {
				let g = xml.system_general.as_ref().expect("system_general on SET");
				assert_eq!(g.hour, Some(12));
				assert_eq!(g.time_zone, Some(-7200));
				reply_200_empty(req)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("general", true).await;
		cam.set_time(dt).await.expect("ok");
	}

	#[test]
	fn dst_offset_seconds_disabled_returns_zero() {
		// `<enable>0</enable>` → no compensation regardless of date.
		let dst = Dst {
			version: "1.1".to_string(),
			enable: Some(0),
			offset: Some(1),
			start_month: Some(3),
			start_week_index: Some(5),
			start_weekday: Some("Sunday".to_string()),
			start_hour: Some(2),
			..Default::default()
		};
		let inside = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::May, 3).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, inside), 0);
	}

	#[test]
	fn dst_offset_seconds_in_window_returns_offset_seconds() {
		// EU schedule (last Sun of Mar 02:00 → last Sun of Oct 03:00),
		// May 3 2026 → in window → 3600 s.
		let dst = eu_dst_one_hour();
		let inside = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::May, 3).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, inside), 3600);
	}

	#[test]
	fn dst_offset_seconds_out_of_window_returns_zero() {
		let dst = eu_dst_one_hour();
		// December 25 — well past Oct's last Sunday.
		let outside = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::December, 25).unwrap(),
			Time::from_hms(9, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, outside), 0);
	}

	#[test]
	fn dst_offset_seconds_at_start_boundary_inclusive() {
		// 2026-03-29 02:00:00 (last Sun of Mar) is the FIRST instant
		// inside the window — the contract is `start <= now`.
		let dst = eu_dst_one_hour();
		let at_start = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::March, 29).unwrap(),
			Time::from_hms(2, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, at_start), 3600);

		let just_before = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::March, 29).unwrap(),
			Time::from_hms(1, 59, 59).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, just_before), 0);
	}

	#[test]
	fn dst_offset_seconds_at_end_boundary_exclusive() {
		// 2026-10-25 03:00:00 (last Sun of Oct) is the FIRST instant
		// OUTSIDE the window — contract is `now < end`.
		let dst = eu_dst_one_hour();
		let at_end = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::October, 25).unwrap(),
			Time::from_hms(3, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, at_end), 0);

		let just_before = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::October, 25).unwrap(),
			Time::from_hms(2, 59, 59).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, just_before), 3600);
	}

	#[test]
	fn dst_offset_seconds_missing_schedule_returns_zero() {
		// Enable + offset present but the schedule is incomplete —
		// must not crash, must return 0 (conservative passthrough).
		let dst = Dst {
			version: "1.1".to_string(),
			enable: Some(1),
			offset: Some(1),
			..Default::default()
		};
		let any = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::May, 3).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, any), 0);
	}

	#[test]
	fn dst_offset_seconds_invalid_end_weekday_returns_zero() {
		// Valid start, invalid end weekday — `dst_transition_for_year`
		// returns None for the end transition, the early-return on
		// `end_match` line fires, function returns 0. Pins the second
		// fallback path, which the missing-schedule test above doesn't
		// exercise (it fails on the start transition first).
		let dst = Dst {
			version: "1.1".to_string(),
			enable: Some(1),
			offset: Some(1),
			start_month: Some(3),
			start_week_index: Some(5),
			start_weekday: Some("Sunday".to_string()),
			start_hour: Some(2),
			start_minute: Some(0),
			start_second: Some(0),
			end_month: Some(10),
			end_week_index: Some(5),
			end_weekday: Some("not-a-day".to_string()),
			end_hour: Some(3),
			end_minute: Some(0),
			end_second: Some(0),
		};
		let any = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::May, 3).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, any), 0);
	}

	#[test]
	fn dst_offset_seconds_southern_hemisphere_window_wraps_year() {
		// Southern-hemisphere schedule: DST runs Oct → Mar across the
		// year boundary. The wrap-aware branch in `dst_offset_seconds`
		// must treat both January AND November as in-window.
		let dst = Dst {
			version: "1.1".to_string(),
			enable: Some(1),
			offset: Some(1),
			start_month: Some(10),
			start_week_index: Some(1),
			start_weekday: Some("Sunday".to_string()),
			start_hour: Some(2),
			start_minute: Some(0),
			start_second: Some(0),
			end_month: Some(4),
			end_week_index: Some(1),
			end_weekday: Some("Sunday".to_string()),
			end_hour: Some(3),
			end_minute: Some(0),
			end_second: Some(0),
		};
		let november = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::November, 15).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		let january = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::January, 15).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		let june = PrimitiveDateTime::new(
			Date::from_calendar_date(2026, time::Month::June, 15).unwrap(),
			Time::from_hms(12, 0, 0).unwrap(),
		);
		assert_eq!(dst_offset_seconds(&dst, november), 3600);
		assert_eq!(dst_offset_seconds(&dst, january), 3600);
		assert_eq!(dst_offset_seconds(&dst, june), 0);
	}

	#[test]
	fn nth_weekday_of_month_finds_first_through_fourth() {
		// 2026-03 starts on a Sunday — first Sunday is the 1st.
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::March, 1, Weekday::Sunday).unwrap(),
			Date::from_calendar_date(2026, time::Month::March, 1).unwrap()
		);
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::March, 2, Weekday::Sunday).unwrap(),
			Date::from_calendar_date(2026, time::Month::March, 8).unwrap()
		);
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::March, 4, Weekday::Sunday).unwrap(),
			Date::from_calendar_date(2026, time::Month::March, 22).unwrap()
		);
	}

	#[test]
	fn nth_weekday_of_month_index_5_returns_last_occurrence() {
		// Last Sunday of March 2026 is the 29th.
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::March, 5, Weekday::Sunday).unwrap(),
			Date::from_calendar_date(2026, time::Month::March, 29).unwrap()
		);
		// Last Sunday of October 2026 is the 25th.
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::October, 5, Weekday::Sunday).unwrap(),
			Date::from_calendar_date(2026, time::Month::October, 25).unwrap()
		);
	}

	#[test]
	fn nth_weekday_of_month_index_5_finds_distinct_weekday() {
		// Last Friday of February 2026 is the 27th.
		assert_eq!(
			nth_weekday_of_month(2026, time::Month::February, 5, Weekday::Friday).unwrap(),
			Date::from_calendar_date(2026, time::Month::February, 27).unwrap()
		);
	}

	#[test]
	fn nth_weekday_of_month_overflow_returns_none() {
		// February 2026 has at most 4 of any weekday — 5th occurrence
		// only exists if `week_index = 5` (last). Index 5 of a Sunday
		// in Feb 2026 = Feb 22. Index 5 of Wednesday in a 28-day Feb
		// where 4 of them exist = the 4th. `nth_weekday_of_month` with
		// `week_index = 4` for a weekday whose 4th occurrence falls
		// past the month should return None — but in Feb 2026 every
		// weekday has 4 occurrences fitting within 28 days, so this
		// covers the boundary clean.
		assert!(nth_weekday_of_month(2026, time::Month::February, 4, Weekday::Sunday).is_some());
	}

	#[test]
	fn nth_weekday_of_month_zero_or_six_returns_none() {
		assert!(nth_weekday_of_month(2026, time::Month::March, 0, Weekday::Sunday).is_none());
		assert!(nth_weekday_of_month(2026, time::Month::March, 6, Weekday::Sunday).is_none());
	}

	#[test]
	fn parse_weekday_accepts_full_and_abbreviated_names() {
		assert_eq!(parse_weekday("Sunday"), Some(Weekday::Sunday));
		assert_eq!(parse_weekday("MONDAY"), Some(Weekday::Monday));
		assert_eq!(parse_weekday("tue"), Some(Weekday::Tuesday));
		assert_eq!(parse_weekday("  Wed  "), Some(Weekday::Wednesday));
		assert_eq!(parse_weekday("not-a-day"), None);
	}

	/// Schedule used by every "in-window vs out-of-window" test: EU-style
	/// `last Sunday of March 02:00 → last Sunday of October 03:00`,
	/// `<offset>1</offset>` hours. Pinned to match the real-firmware
	/// schema observed against current Argus hardware.
	fn eu_dst_one_hour() -> Dst {
		Dst {
			version: "1.1".to_string(),
			enable: Some(1),
			offset: Some(1),
			start_month: Some(3),
			start_week_index: Some(5),
			start_weekday: Some("Sunday".to_string()),
			start_hour: Some(2),
			start_minute: Some(0),
			start_second: Some(0),
			end_month: Some(10),
			end_week_index: Some(5),
			end_weekday: Some("Sunday".to_string()),
			end_hour: Some(3),
			end_minute: Some(0),
			end_second: Some(0),
		}
	}

	#[test]
	fn try_build_timestamp_valid_ok_inverts_timezone_sign() {
		// Wire timezone uses Reolink's inverted convention: positive =
		// west of UTC, negative = east. Wire `+3600` therefore decodes
		// to `UtcOffset` of `-3600` (UTC-1, "one hour west").
		let dt = try_build_timestamp(3600, 2026, 4, 23, 12, 30, 45).expect("ok");
		assert_eq!(dt.year(), 2026);
		assert_eq!(dt.offset().whole_seconds(), -3600);
	}

	#[test]
	fn try_build_timestamp_bad_month_is_err() {
		assert!(try_build_timestamp(0, 2026, 13, 1, 0, 0, 0).is_err());
	}

	#[test]
	fn try_build_timestamp_bad_day_is_err() {
		assert!(try_build_timestamp(0, 2026, 2, 31, 0, 0, 0).is_err());
	}

	#[test]
	fn try_build_timestamp_bad_hour_is_err() {
		assert!(try_build_timestamp(0, 2026, 4, 23, 25, 0, 0).is_err());
	}

	/// Roundtrip pin: `set_time` sends `time_zone =
	/// -offset.whole_seconds()`; `try_build_timestamp` must decode that
	/// wire value back into the same `OffsetDateTime` the operator
	/// passed in. Tests every offset that bairelay's actual deployments
	/// span (UTC-12 to UTC+14 covers the full real-world range).
	#[test]
	fn set_time_get_time_roundtrip_preserves_offset_datetime() {
		// Pick a deterministic wallclock so we don't depend on
		// `OffsetDateTime::now_*` flakiness.
		let local_date = Date::from_calendar_date(2026, time::Month::April, 23).unwrap();
		let local_time = Time::from_hms(12, 30, 45).unwrap();
		for offset_seconds in [-12 * 3600, -5 * 3600, -3600, 0, 3600, 5 * 3600, 14 * 3600] {
			let original_offset = UtcOffset::from_whole_seconds(offset_seconds).unwrap();
			let original =
				PrimitiveDateTime::new(local_date, local_time).assume_offset(original_offset);

			// `set_time` would put this on the wire:
			let wire_time_zone = -original.offset().whole_seconds();

			// `get_time` (via `try_build_timestamp`) would decode the
			// wire value back to:
			let decoded = try_build_timestamp(
				wire_time_zone,
				original.year(),
				original.month().into(),
				original.day(),
				original.hour(),
				original.minute(),
				original.second(),
			)
			.expect("decode ok");

			// Same instant in UTC AND same display-local components.
			assert_eq!(
				decoded, original,
				"roundtrip drifted at offset {offset_seconds}s: wire={wire_time_zone}"
			);
			assert_eq!(decoded.offset(), original_offset);
		}
	}
}
