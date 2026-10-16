//! `CameraDriver` trait — the subset of `BcCamera` methods the binary
//! calls from non-stream code paths. Lets downstream test code substitute
//! a `FakeCamera` without a live camera session.
//!
//! Dyn-compatible: no generics, no `impl Trait`, no `Self`-returning
//! methods. Holds only plain `async fn`s (via `async-trait`) so consumers
//! can hold `Arc<dyn CameraDriver>`.

use async_trait::async_trait;
use std::net::IpAddr;
use time::OffsetDateTime;
use tokio::sync::mpsc::Receiver;

use crate::bc::xml::{
	AbilityInfo, BatteryInfo, FloodlightStatusList, HaCfg, HttpPort, HttpsPort, LedState, LinkType,
	OnvifPort, PtzPreset, RfAlarmCfg, RtmpPort, RtspPort, ServerPort, Support, UserList,
	VersionInfo,
};

use super::{BcCamera, Direction, Error, LightState, MotionData, Result};

/// The subset of `BcCamera` operations the binary relies on from non-
/// stream code paths (motion / battery / floodlight / PIR pollers, MQTT
/// control dispatch, snapshot warm-up).
///
/// Method names and signatures mirror `BcCamera`'s inherent `impl` so a
/// forwarding blanket impl reads as one line per method.
#[async_trait]
pub trait CameraDriver: Send + Sync {
	/// Subscribe to motion events; returns a [`MotionData`] stream.
	async fn listen_on_motion(&self) -> Result<MotionData>;
	/// Request the camera's current battery status.
	async fn battery_info(&self) -> Result<BatteryInfo>;
	/// Request the camera's current PIR configuration.
	async fn get_pirstate(&self) -> Result<RfAlarmCfg>;
	/// Enable or disable the PIR sensor.
	async fn pir_set(&self, state: bool) -> Result<()>;

	/// Subscribe to floodlight status updates.
	async fn listen_on_floodlight(&self) -> Result<Receiver<FloodlightStatusList>>;
	/// Query whether the camera's floodlight tasks are enabled.
	async fn is_floodlight_tasks_enabled(&self) -> Result<bool>;
	/// Enable or disable the floodlight tasks engine.
	async fn floodlight_tasks_enable(&self, state: bool) -> Result<()>;
	/// Manually trigger the floodlight for `duration` seconds.
	async fn set_floodlight_manual(&self, state: bool, duration: u16) -> Result<()>;

	/// Send a PTZ movement command.
	async fn send_ptz(&self, direction: Direction, amount: f32) -> Result<()>;
	/// Create or overwrite a named PTZ preset.
	async fn set_ptz_preset(&self, preset_id: u8, name: String) -> Result<()>;
	/// Move the camera to a saved PTZ preset.
	async fn moveto_ptz_preset(&self, preset_id: u8) -> Result<()>;
	/// Move the camera's zoom to an absolute position.
	async fn zoom_to(&self, zoom_pos: u32) -> Result<()>;

	/// Request a JPEG snapshot from the camera.
	async fn get_snapshot(&self) -> Result<Vec<u8>>;

	/// Turn the status LED (small indicator light) on or off.
	async fn led_light_set(&self, state: bool) -> Result<()>;
	/// Set the IR night-vision LED mode.
	async fn irled_light_set(&self, state: LightState) -> Result<()>;

	/// Reboot the camera.
	async fn reboot(&self) -> Result<()>;
	/// Trigger the built-in siren.
	async fn siren(&self) -> Result<()>;

	/// Read `<LedState>`: IR night-vision mode (`state`) and status LED
	/// (`lightState`) — two unrelated features in one reply. Values for
	/// hardware the camera lacks are placeholders.
	async fn get_ledstate(&self) -> Result<LedState>;

	/// Read the HTTP webhook configuration (`<HaCfg>`). `None` for an empty
	/// or `<HaCfg>`-less 200 reply; an error reply is an `Err` (the
	/// binary's `ha_cfg_support` maps it to "unsupported").
	async fn get_ha_cfg(&self) -> Result<Option<HaCfg>>;

	/// Write the HTTP webhook configuration, then read it back. `None`
	/// when the read-back finds no `<HaCfg>`.
	async fn set_ha_cfg(&self, cfg: &HaCfg) -> Result<Option<HaCfg>>;

	/// Whether the login-time `abilityInfo` lists `name` (any
	/// permission); `None` when that report was never received. Cached;
	/// no camera round trip.
	async fn ability_advertised(&self, name: &str) -> Option<bool>;

	/// The IP of the camera this session is connected to, when the
	/// session reaches the camera itself (direct TCP or local discovery);
	/// `None` through remote, map or relay discovery.
	fn camera_ip(&self) -> Option<IpAddr>;

	/// Read the camera's PTZ preset list.
	async fn get_ptz_preset(&self) -> Result<PtzPreset>;

	/// Read the camera model / firmware / hardware version block.
	async fn version(&self) -> Result<VersionInfo>;

	/// Set the camera's onboard clock to `timestamp`.
	async fn set_time(&self, timestamp: OffsetDateTime) -> Result<()>;

	/// List the camera's configured user accounts.
	async fn get_users(&self) -> Result<UserList>;
	/// Add a new user account.
	async fn add_user(&self, user_name: String, password: String, user_level: u8) -> Result<()>;
	/// Change the password on an existing user account.
	async fn modify_user(&self, user_name: String, password: String) -> Result<()>;
	/// Remove a user account.
	async fn delete_user(&self, user_name: String) -> Result<()>;

	/// Read the Baichuan / server-port service config.
	async fn get_serverport(&self) -> Result<ServerPort>;
	/// Update the Baichuan / server-port service config.
	async fn set_serverport(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;
	/// Read the HTTP service config.
	async fn get_http(&self) -> Result<HttpPort>;
	/// Update the HTTP service config.
	async fn set_http(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;
	/// Read the HTTPS service config.
	async fn get_https(&self) -> Result<HttpsPort>;
	/// Update the HTTPS service config.
	async fn set_https(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;
	/// Read the RTSP service config.
	async fn get_rtsp(&self) -> Result<RtspPort>;
	/// Update the RTSP service config.
	async fn set_rtsp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;
	/// Read the RTMP service config.
	async fn get_rtmp(&self) -> Result<RtmpPort>;
	/// Update the RTMP service config.
	async fn set_rtmp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;
	/// Read the ONVIF service config.
	async fn get_onvif(&self) -> Result<OnvifPort>;
	/// Update the ONVIF service config.
	async fn set_onvif(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()>;

	/// Request the camera's link-type info. Used by the keepalive
	/// loop as a cheap "are you still there?" probe (matches neolink's
	/// choice — see `CameraHandle::keepalive_loop`).
	async fn get_linktype(&self) -> Result<LinkType>;

	/// Lenient liveness probe wrapping [`Self::get_linktype`]. The
	/// keepalive loop calls this instead of `get_linktype` directly so
	/// the "camera is reachable but doesn't speak link-type" case lives
	/// at the protocol layer where we can match on `Error` variants
	/// instead of stringifying them.
	///
	/// Some Reolink firmwares answer the link-type query with an XML
	/// payload that doesn't carry a `link_type` element. `BcCamera`
	/// surfaces that as [`Error::UnintelligibleReply`]. The TCP ACK
	/// proves the camera is alive; the missing element is harmless,
	/// so we map it to `Ok(())`. Every other error path is forwarded
	/// untouched.
	async fn keepalive_probe(&self) -> Result<()> {
		match self.get_linktype().await {
			Ok(_) => Ok(()),
			Err(Error::UnintelligibleReply { .. }) => Ok(()),
			Err(e) => Err(e),
		}
	}

	/// Request the camera's capability report. Used post-connect to
	/// populate `CameraCapabilities` (PTZ support, etc.). Not exposed
	/// as a sub-command today; lives on the trait purely so the
	/// session-task lifecycle in `CameraHandle::run` can be driven by
	/// a `FakeCamera` in unit tests.
	async fn get_support(&self) -> Result<Support>;

	/// Fetch the camera's `abilityInfo` XML report — the per-user
	/// permission map keyed by module (system / network / alarm / image
	/// / video / security / replay / PTZ / IO / streaming). Used by the
	/// `abilities` one-shot command to capture ground-truth ability
	/// strings for `MissingAbility` gate decisions.
	async fn get_abilityinfo(&self) -> Result<AbilityInfo>;
}

#[async_trait]
impl CameraDriver for BcCamera {
	async fn listen_on_motion(&self) -> Result<MotionData> {
		BcCamera::listen_on_motion(self).await
	}
	async fn battery_info(&self) -> Result<BatteryInfo> {
		BcCamera::battery_info(self).await
	}
	async fn get_pirstate(&self) -> Result<RfAlarmCfg> {
		BcCamera::get_pirstate(self).await
	}
	async fn pir_set(&self, state: bool) -> Result<()> {
		BcCamera::pir_set(self, state).await
	}

	async fn listen_on_floodlight(&self) -> Result<Receiver<FloodlightStatusList>> {
		BcCamera::listen_on_floodlight(self).await
	}
	async fn is_floodlight_tasks_enabled(&self) -> Result<bool> {
		BcCamera::is_floodlight_tasks_enabled(self).await
	}
	async fn floodlight_tasks_enable(&self, state: bool) -> Result<()> {
		BcCamera::floodlight_tasks_enable(self, state).await
	}
	async fn set_floodlight_manual(&self, state: bool, duration: u16) -> Result<()> {
		BcCamera::set_floodlight_manual(self, state, duration).await
	}

	async fn send_ptz(&self, direction: Direction, amount: f32) -> Result<()> {
		BcCamera::send_ptz(self, direction, amount).await
	}
	async fn set_ptz_preset(&self, preset_id: u8, name: String) -> Result<()> {
		BcCamera::set_ptz_preset(self, preset_id, name).await
	}
	async fn moveto_ptz_preset(&self, preset_id: u8) -> Result<()> {
		BcCamera::moveto_ptz_preset(self, preset_id).await
	}
	async fn zoom_to(&self, zoom_pos: u32) -> Result<()> {
		BcCamera::zoom_to(self, zoom_pos).await
	}

	async fn get_snapshot(&self) -> Result<Vec<u8>> {
		BcCamera::get_snapshot(self).await
	}

	async fn led_light_set(&self, state: bool) -> Result<()> {
		BcCamera::led_light_set(self, state).await
	}
	async fn irled_light_set(&self, state: LightState) -> Result<()> {
		BcCamera::irled_light_set(self, state).await
	}

	async fn reboot(&self) -> Result<()> {
		BcCamera::reboot(self).await
	}
	async fn siren(&self) -> Result<()> {
		BcCamera::siren(self).await
	}

	async fn get_ledstate(&self) -> Result<LedState> {
		BcCamera::get_ledstate(self).await
	}

	async fn get_ha_cfg(&self) -> Result<Option<HaCfg>> {
		BcCamera::get_ha_cfg(self).await
	}

	async fn set_ha_cfg(&self, cfg: &HaCfg) -> Result<Option<HaCfg>> {
		BcCamera::set_ha_cfg(self, cfg).await
	}

	async fn ability_advertised(&self, name: &str) -> Option<bool> {
		BcCamera::ability_advertised(self, name).await
	}

	fn camera_ip(&self) -> Option<IpAddr> {
		BcCamera::camera_ip(self)
	}

	async fn get_ptz_preset(&self) -> Result<PtzPreset> {
		BcCamera::get_ptz_preset(self).await
	}

	async fn version(&self) -> Result<VersionInfo> {
		BcCamera::version(self).await
	}

	async fn set_time(&self, timestamp: OffsetDateTime) -> Result<()> {
		BcCamera::set_time(self, timestamp).await
	}

	async fn get_users(&self) -> Result<UserList> {
		BcCamera::get_users(self).await
	}
	async fn add_user(&self, user_name: String, password: String, user_level: u8) -> Result<()> {
		BcCamera::add_user(self, user_name, password, user_level).await
	}
	async fn modify_user(&self, user_name: String, password: String) -> Result<()> {
		BcCamera::modify_user(self, user_name, password).await
	}
	async fn delete_user(&self, user_name: String) -> Result<()> {
		BcCamera::delete_user(self, user_name).await
	}

	async fn get_serverport(&self) -> Result<ServerPort> {
		BcCamera::get_serverport(self).await
	}
	async fn set_serverport(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_serverport(self, set_on, set_port).await
	}
	async fn get_http(&self) -> Result<HttpPort> {
		BcCamera::get_http(self).await
	}
	async fn set_http(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_http(self, set_on, set_port).await
	}
	async fn get_https(&self) -> Result<HttpsPort> {
		BcCamera::get_https(self).await
	}
	async fn set_https(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_https(self, set_on, set_port).await
	}
	async fn get_rtsp(&self) -> Result<RtspPort> {
		BcCamera::get_rtsp(self).await
	}
	async fn set_rtsp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_rtsp(self, set_on, set_port).await
	}
	async fn get_rtmp(&self) -> Result<RtmpPort> {
		BcCamera::get_rtmp(self).await
	}
	async fn set_rtmp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_rtmp(self, set_on, set_port).await
	}
	async fn get_onvif(&self) -> Result<OnvifPort> {
		BcCamera::get_onvif(self).await
	}
	async fn set_onvif(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		BcCamera::set_onvif(self, set_on, set_port).await
	}

	async fn get_linktype(&self) -> Result<LinkType> {
		BcCamera::get_linktype(self).await
	}

	async fn get_support(&self) -> Result<Support> {
		BcCamera::get_support(self).await
	}

	async fn get_abilityinfo(&self) -> Result<AbilityInfo> {
		BcCamera::get_abilityinfo(self).await
	}
}

#[cfg(test)]
mod tests {
	//! Cover every forwarding line in the `CameraDriver for BcCamera`
	//! blanket impl. Each method is invoked through `&dyn CameraDriver`
	//! against a `MockConnection`-backed `BcCamera`. We don't care about
	//! the success / failure outcome — only that the forwarding wrapper
	//! executes once per method. Errors are accepted (most methods fail
	//! ability gates or get an err_code 500 reply) because the goal is
	//! to drive the one-line `BcCamera::xxx(self).await` body, not to
	//! re-test the wrapped command's happy path.
	use super::*;
	use crate::bc_protocol::connection::mock::{reply_err_code, MockConnection};
	use crate::bc_protocol::BcCamera;
	use std::time::Duration;

	/// Helper: build a BcCamera whose connection accepts every msg_id
	/// and replies with err_code 500. Only the forwarding line in the
	/// trait impl matters; the inner `BcCamera::xxx` wrapper exits via
	/// `Error::CameraServiceUnavailable` after that one call.
	async fn err_camera() -> BcCamera {
		// Build with no expects — `MockConnection` ignores unknown
		// MSG_IDs (sender just drops the request, so the recv pends).
		// To keep tests bounded use a global timeout per call.
		let mock = MockConnection::new().build().await;
		BcCamera::from_mock_connection(mock).await
	}

	/// Wrap each method-under-test in a hard 200 ms timeout so a missing
	/// reply on a method that takes ability-check then send doesn't
	/// hang.
	async fn run<F, T>(fut: F) -> std::result::Result<T, &'static str>
	where
		F: std::future::Future<Output = T>,
	{
		tokio::time::timeout(Duration::from_millis(200), fut)
			.await
			.map_err(|_| "timeout")
	}

	#[tokio::test]
	async fn forwarding_blanket_impl_drives_every_method() {
		use crate::bc::model::*;

		// Use ONE camera per method so cross-method state can't bleed.
		// We accept that some calls return Ok and some Err — only the
		// fact the wrapper line runs matters for coverage.

		// listen_on_motion: hits ability check → Err.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.listen_on_motion()).await;
		}
		// battery_info: Stream-XML reply with err_code 500.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_BATTERY_INFO)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.battery_info()).await;
		}
		// get_pirstate: needs MSG_ID_GET_PIR_ALARM.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_GET_PIR_ALARM)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			cam.test_set_ability("rfAlarm", true).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_pirstate()).await;
		}
		// pir_set: ability + start_pir_alarm + set_pir_alarm. Just hit
		// the ability gate (no ability → Err) but the trait line runs.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.pir_set(true)).await;
		}
		// listen_on_floodlight: ability check fails → Err.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.listen_on_floodlight()).await;
		}
		// is_floodlight_tasks_enabled: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.is_floodlight_tasks_enabled()).await;
		}
		// floodlight_tasks_enable: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.floodlight_tasks_enable(true)).await;
		}
		// set_floodlight_manual: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.set_floodlight_manual(true, 30)).await;
		}
		// send_ptz: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.send_ptz(Direction::Stop, 1.0)).await;
		}
		// set_ptz_preset: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.set_ptz_preset(1, "x".to_string())).await;
		}
		// moveto_ptz_preset: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.moveto_ptz_preset(1)).await;
		}
		// zoom_to: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.zoom_to(100)).await;
		}
		// get_snapshot: err_code 500.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_SNAP)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_snapshot()).await;
		}
		// led_light_set: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.led_light_set(true)).await;
		}
		// irled_light_set: no reply → timeout.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.irled_light_set(LightState::Off)).await;
		}
		// reboot: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.reboot()).await;
		}
		// siren: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.siren()).await;
		}
		// get_ledstate: no reply → timeout.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_ledstate()).await;
		}
		// get_ha_cfg / set_ha_cfg: no reply → timeout.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_ha_cfg()).await;
			let _ = run(dr.set_ha_cfg(&HaCfg::default())).await;
		}
		// ability_advertised: empty cache → unknown; populated → yes/no.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			assert_eq!(dr.ability_advertised("ledState").await, None);
			cam.test_set_ability("ledState", false).await;
			assert_eq!(dr.ability_advertised("ledState").await, Some(true));
			assert_eq!(dr.ability_advertised("floodlight").await, Some(false));
		}
		// camera_ip: a mock connection has no camera address.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			assert_eq!(dr.camera_ip(), None);
		}
		// get_ptz_preset: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_ptz_preset()).await;
		}
		// version: err_code 500.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_VERSION)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.version()).await;
		}
		// set_time: ability fail.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let now = time::OffsetDateTime::now_utc();
			let _ = run(dr.set_time(now)).await;
		}
		// get_users: err_code 500.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_UPDATE_USER_LIST)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_users()).await;
		}
		// add_user / modify_user / delete_user: send-only commands —
		// even with empty mock the send-then-recv recv blocks, so 200 ms
		// timeout exits.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.add_user("u".to_string(), "p".to_string(), 1)).await;
			let _ = run(dr.modify_user("u".to_string(), "p".to_string())).await;
			let _ = run(dr.delete_user("u".to_string())).await;
		}
		// services: 12 forwarding methods. Each `get_*` calls
		// MSG_ID_GET_SERVICE_PORTS; each `set_*` flows get → set. With
		// no expectation set, recv pends → 200 ms timeout.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_serverport()).await;
			let _ = run(dr.set_serverport(Some(true), None)).await;
			let _ = run(dr.get_http()).await;
			let _ = run(dr.set_http(Some(true), None)).await;
			let _ = run(dr.get_https()).await;
			let _ = run(dr.set_https(Some(true), None)).await;
			let _ = run(dr.get_rtsp()).await;
			let _ = run(dr.set_rtsp(Some(true), None)).await;
			let _ = run(dr.get_rtmp()).await;
			let _ = run(dr.set_rtmp(Some(true), None)).await;
			let _ = run(dr.get_onvif()).await;
			let _ = run(dr.set_onvif(Some(true), None)).await;
		}
		// get_linktype: err_code 500.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_linktype()).await;
		}
		// get_support: err_code 500.
		{
			let cam = err_camera().await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_support()).await;
		}
		// get_abilityinfo: err_code 500.
		{
			let mock = MockConnection::new()
				.expect_msg(MSG_ID_ABILITY_INFO)
				.reply_with(|req| reply_err_code(req, 500))
				.build()
				.await;
			let cam = BcCamera::from_mock_connection(mock).await;
			let dr: &dyn CameraDriver = &cam;
			let _ = run(dr.get_abilityinfo()).await;
		}
	}
}
