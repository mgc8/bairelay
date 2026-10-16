//! `FakeCamera` — a `CameraDriver` backed by closures the test sets
//! per-method. Unset read/stream methods panic with a clear diagnostic
//! so tests fail fast rather than silently return bogus data. Side-
//! effect setters (`pir_set`, `reboot`, etc.) record their args /
//! invocation counts in [`FakeCalls`] for assertion.
//!
//! Gated on `#[cfg(any(test, feature = "test-util"))]` — the module is
//! not compiled for production builds.

// Individual builder methods and `FakeCalls` fields are driven on-demand
// per test; let the normal dead-code lint fire once all methods are used
// so accidentally unused scaffolding surfaces naturally.

use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::mpsc::Receiver;

use crate::bc::xml::{
	AbilityInfo, BatteryInfo, FloodlightStatusList, HaCfg, HttpPort, HttpsPort, LedState, LinkType,
	OnvifPort, PtzPreset, RfAlarmCfg, RtmpPort, RtspPort, ServerPort, Support, UserList,
	VersionInfo,
};

use super::camera_driver::CameraDriver;
use super::{Direction, Error, LightState, MotionData, Result};

type BoxFn<T> = Box<dyn Fn() -> Result<T> + Send + Sync>;

/// Call log populated by side-effect methods on [`FakeCamera`]. Tests
/// read these fields after exercising the code under test to confirm
/// the right camera calls were made with the right arguments.
#[derive(Default)]
pub struct FakeCalls {
	/// Count of `reboot` invocations.
	pub reboot: Mutex<u32>,
	/// Count of `siren` invocations.
	pub siren: Mutex<u32>,
	/// Each `pir_set(state)` call appended as `state`.
	pub pir_set: Mutex<Vec<bool>>,
	/// Each `floodlight_tasks_enable(state)` call appended as `state`.
	pub floodlight_tasks_enable: Mutex<Vec<bool>>,
	/// Each `set_floodlight_manual(state, duration)` call appended.
	pub set_floodlight_manual: Mutex<Vec<(bool, u16)>>,
	/// Each `send_ptz(direction, amount)` call appended.
	pub send_ptz: Mutex<Vec<(Direction, f32)>>,
	/// Each `set_ptz_preset(id, name)` call appended.
	pub set_ptz_preset: Mutex<Vec<(u8, String)>>,
	/// Each `moveto_ptz_preset(id)` call appended.
	pub moveto_ptz_preset: Mutex<Vec<u8>>,
	/// Each `zoom_to(pos)` call appended.
	pub zoom_to: Mutex<Vec<u32>>,
	/// Each `led_light_set(state)` call appended.
	pub led_light_set: Mutex<Vec<bool>>,
	/// Each `irled_light_set(state)` call appended.
	pub irled_light_set: Mutex<Vec<LightState>>,
	/// Each `set_time(ts)` call appended.
	pub set_time: Mutex<Vec<OffsetDateTime>>,
	/// Each `add_user(name, password, level)` call appended.
	pub add_user: Mutex<Vec<(String, String, u8)>>,
	/// Each `modify_user(name, password)` call appended.
	pub modify_user: Mutex<Vec<(String, String)>>,
	/// Each `delete_user(name)` call appended.
	pub delete_user: Mutex<Vec<String>>,
	/// Each `set_serverport(on, port)` call appended.
	pub set_serverport: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_http(on, port)` call appended.
	pub set_http: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_https(on, port)` call appended.
	pub set_https: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_rtsp(on, port)` call appended.
	pub set_rtsp: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_rtmp(on, port)` call appended.
	pub set_rtmp: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_onvif(on, port)` call appended.
	pub set_onvif: Mutex<Vec<(Option<bool>, Option<u32>)>>,
	/// Each `set_ha_cfg(cfg)` call appended.
	pub set_ha_cfg: Mutex<Vec<HaCfg>>,
}

/// Internal state backing a [`FakeCamera`]. Read-method closures are
/// `Option<BoxFn<_>>`; stream-returning methods hold pre-built values
/// in a `Mutex<Option<_>>` so the first call takes ownership.
///
/// `*_pending` flags request the matching async method to never
/// resolve (returns `pending()`); used by mqtt_dispatch tests to
/// drive the 30 s `tokio::time::timeout` warn-then-OK branch under
/// `tokio::time::pause()`.
#[derive(Default)]
struct FakeState {
	battery_info: Option<BoxFn<BatteryInfo>>,
	battery_info_pending: bool,
	get_pirstate: Option<BoxFn<RfAlarmCfg>>,
	get_pirstate_pending: bool,
	get_ledstate_pending: bool,
	led_writes_fail: bool,
	abilities: Option<Vec<String>>,
	camera_ip: Option<IpAddr>,
	is_floodlight_tasks_enabled: Option<BoxFn<bool>>,
	get_snapshot: Option<BoxFn<Vec<u8>>>,
	get_snapshot_pending: bool,
	send_ptz_pending: bool,
	siren_pending: bool,
	is_floodlight_tasks_enabled_pending: bool,
	get_ptz_preset_pending: bool,
	get_ledstate: Option<BoxFn<LedState>>,
	get_ha_cfg: Option<BoxFn<Option<HaCfg>>>,
	get_ha_cfg_pending: bool,
	set_ha_cfg_pending: bool,
	get_ptz_preset: Option<BoxFn<PtzPreset>>,
	version: Option<BoxFn<VersionInfo>>,
	get_users: Option<BoxFn<UserList>>,
	get_serverport: Option<BoxFn<ServerPort>>,
	get_http: Option<BoxFn<HttpPort>>,
	get_https: Option<BoxFn<HttpsPort>>,
	get_rtsp: Option<BoxFn<RtspPort>>,
	get_rtmp: Option<BoxFn<RtmpPort>>,
	get_onvif: Option<BoxFn<OnvifPort>>,
	get_linktype: Option<BoxFn<LinkType>>,
	get_support: Option<BoxFn<Support>>,
	get_abilityinfo: Option<BoxFn<AbilityInfo>>,
	/// One [`MotionData`] per `listen_on_motion` call, in order.
	motion_stream: Mutex<VecDeque<MotionData>>,
	floodlight_stream: Mutex<Option<Receiver<FloodlightStatusList>>>,
	/// When `Some`, `listen_on_motion` returns `Err(f())` on every call
	/// rather than consuming a `motion_stream` value.
	motion_stream_error: Option<Box<dyn Fn() -> Error + Send + Sync>>,
	/// When `Some`, `listen_on_floodlight` returns `Err(f())` on every
	/// call rather than consuming the `floodlight_stream` value.
	floodlight_stream_error: Option<Box<dyn Fn() -> Error + Send + Sync>>,
	calls: FakeCalls,
}

/// A [`CameraDriver`] implementation driven entirely by closures and
/// pre-built stream values configured on the [`FakeCameraBuilder`].
///
/// Access the call log via [`FakeCamera::calls`] after the code under
/// test has run. An unconfigured read or stream method panics with a
/// `"FakeCamera: <method> not configured for this test"` message —
/// silent defaults are deliberately not supported because they hide
/// test-setup bugs.
pub struct FakeCamera {
	inner: Arc<FakeState>,
}

impl FakeCamera {
	/// Access the call log recorded by side-effect methods.
	pub fn calls(&self) -> &FakeCalls {
		&self.inner.calls
	}

	fn led_write_result(&self) -> Result<()> {
		if self.inner.led_writes_fail {
			Err(Error::Other("FakeCamera: scripted LED write failure"))
		} else {
			Ok(())
		}
	}
}

/// Builder for a [`FakeCamera`]. Use `with_<method>(...)` to install a
/// closure per read method, or `with_<method>_stream(...)` to install
/// a pre-built value for a stream-returning method. Call [`build`] to
/// produce an `Arc<FakeCamera>` usable as `Arc<dyn CameraDriver>`.
///
/// [`build`]: FakeCameraBuilder::build
#[derive(Default)]
pub struct FakeCameraBuilder {
	state: FakeState,
}

impl FakeCameraBuilder {
	/// Start a new builder with every method unconfigured.
	pub fn new() -> Self {
		Self::default()
	}

	/// Install the closure invoked on [`CameraDriver::battery_info`].
	pub fn with_battery_info<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<BatteryInfo> + Send + Sync + 'static,
	{
		self.state.battery_info = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_pirstate`].
	pub fn with_pirstate<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<RfAlarmCfg> + Send + Sync + 'static,
	{
		self.state.get_pirstate = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on
	/// [`CameraDriver::is_floodlight_tasks_enabled`].
	pub fn with_is_floodlight_tasks_enabled<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<bool> + Send + Sync + 'static,
	{
		self.state.is_floodlight_tasks_enabled = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_snapshot`].
	pub fn with_snapshot<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<Vec<u8>> + Send + Sync + 'static,
	{
		self.state.get_snapshot = Some(Box::new(f));
		self
	}

	/// Queue a [`MotionData`] handle for
	/// [`CameraDriver::listen_on_motion`]. Each call returns the next
	/// queued handle, in the order queued; a call with none left panics.
	pub fn with_motion_stream(mut self, data: MotionData) -> Self {
		self.state
			.motion_stream
			.get_mut()
			.expect("motion_stream lock")
			.push_back(data);
		self
	}

	/// Pre-seed the floodlight receiver returned by
	/// [`CameraDriver::listen_on_floodlight`]. Consumed on first call;
	/// second call panics.
	pub fn with_floodlight_stream(mut self, rx: Receiver<FloodlightStatusList>) -> Self {
		self.state.floodlight_stream = Mutex::new(Some(rx));
		self
	}

	/// Configure [`CameraDriver::listen_on_motion`] to return `Err(f())`
	/// on every call. Used by retry-loop tests that need the listener to
	/// observe a subscribe failure without panicking.
	pub fn with_motion_stream_error<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Error + Send + Sync + 'static,
	{
		self.state.motion_stream_error = Some(Box::new(f));
		self
	}

	/// Configure [`CameraDriver::listen_on_floodlight`] to return
	/// `Err(f())` on every call. Used by retry-loop tests that need the
	/// listener to observe a subscribe failure without panicking.
	pub fn with_floodlight_stream_error<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Error + Send + Sync + 'static,
	{
		self.state.floodlight_stream_error = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_ledstate`].
	pub fn with_ledstate<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<LedState> + Send + Sync + 'static,
	{
		self.state.get_ledstate = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_ha_cfg`] and as
	/// the read-back of [`CameraDriver::set_ha_cfg`].
	pub fn with_ha_cfg<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<Option<HaCfg>> + Send + Sync + 'static,
	{
		self.state.get_ha_cfg = Some(Box::new(f));
		self
	}

	/// Make `get_ha_cfg` await `pending()` forever.
	pub fn with_ha_cfg_pending(mut self) -> Self {
		self.state.get_ha_cfg_pending = true;
		self
	}

	/// Make `set_ha_cfg` record the call, then await `pending()` forever.
	pub fn with_set_ha_cfg_pending(mut self) -> Self {
		self.state.set_ha_cfg_pending = true;
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_ptz_preset`].
	pub fn with_ptz_preset<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<PtzPreset> + Send + Sync + 'static,
	{
		self.state.get_ptz_preset = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::version`].
	pub fn with_version<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<VersionInfo> + Send + Sync + 'static,
	{
		self.state.version = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_users`].
	pub fn with_users<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<UserList> + Send + Sync + 'static,
	{
		self.state.get_users = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_serverport`].
	pub fn with_serverport<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<ServerPort> + Send + Sync + 'static,
	{
		self.state.get_serverport = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_http`].
	pub fn with_http<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<HttpPort> + Send + Sync + 'static,
	{
		self.state.get_http = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_https`].
	pub fn with_https<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<HttpsPort> + Send + Sync + 'static,
	{
		self.state.get_https = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_rtsp`].
	pub fn with_rtsp<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<RtspPort> + Send + Sync + 'static,
	{
		self.state.get_rtsp = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_rtmp`].
	pub fn with_rtmp<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<RtmpPort> + Send + Sync + 'static,
	{
		self.state.get_rtmp = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_onvif`].
	pub fn with_onvif<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<OnvifPort> + Send + Sync + 'static,
	{
		self.state.get_onvif = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_linktype`].
	pub fn with_linktype<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<LinkType> + Send + Sync + 'static,
	{
		self.state.get_linktype = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_support`].
	pub fn with_support<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<Support> + Send + Sync + 'static,
	{
		self.state.get_support = Some(Box::new(f));
		self
	}

	/// Install the closure invoked on [`CameraDriver::get_abilityinfo`].
	pub fn with_abilityinfo<F>(mut self, f: F) -> Self
	where
		F: Fn() -> Result<AbilityInfo> + Send + Sync + 'static,
	{
		self.state.get_abilityinfo = Some(Box::new(f));
		self
	}

	/// Make `battery_info` await `pending()` forever — used by 30 s
	/// command-timeout tests under `tokio::time::pause()`. Mutually
	/// exclusive with `with_battery_info(...)`; the pending flag
	/// short-circuits before the closure runs.
	pub fn with_battery_info_pending(mut self) -> Self {
		self.state.battery_info_pending = true;
		self
	}

	/// Make `get_pirstate` await `pending()` forever.
	pub fn with_pirstate_pending(mut self) -> Self {
		self.state.get_pirstate_pending = true;
		self
	}

	/// Abilities [`CameraDriver::ability_advertised`] reports. Default:
	/// no report received (every lookup is `None`).
	pub fn with_abilities(mut self, names: &[&str]) -> Self {
		self.state.abilities = Some(names.iter().map(|n| n.to_string()).collect());
		self
	}

	/// The address [`CameraDriver::camera_ip`] reports. Default: `None`
	/// (a session through remote, map or relay discovery).
	pub fn with_camera_ip(mut self, ip: IpAddr) -> Self {
		self.state.camera_ip = Some(ip);
		self
	}

	/// Make `led_light_set` / `irled_light_set` record the call, then
	/// return an error (both are infallible by default).
	pub fn with_led_writes_failing(mut self) -> Self {
		self.state.led_writes_fail = true;
		self
	}

	/// Make `get_ledstate` await `pending()` forever.
	pub fn with_ledstate_pending(mut self) -> Self {
		self.state.get_ledstate_pending = true;
		self
	}

	/// Make `get_snapshot` await `pending()` forever.
	pub fn with_snapshot_pending(mut self) -> Self {
		self.state.get_snapshot_pending = true;
		self
	}

	/// Make `send_ptz` await `pending()` forever — both directional and
	/// stop calls hang. The PTZ dispatcher's outer `timeout(30s)` pulls
	/// us out, so a paused virtual clock + `advance(31s)` is required.
	pub fn with_send_ptz_pending(mut self) -> Self {
		self.state.send_ptz_pending = true;
		self
	}

	/// Make `siren()` await `pending()` forever.
	pub fn with_siren_pending(mut self) -> Self {
		self.state.siren_pending = true;
		self
	}

	/// Make `is_floodlight_tasks_enabled()` await `pending()` forever.
	pub fn with_is_floodlight_tasks_enabled_pending(mut self) -> Self {
		self.state.is_floodlight_tasks_enabled_pending = true;
		self
	}

	/// Make `get_ptz_preset()` await `pending()` forever.
	pub fn with_ptz_preset_pending(mut self) -> Self {
		self.state.get_ptz_preset_pending = true;
		self
	}

	/// Finalise the fake and hand back a shared handle.
	pub fn build(self) -> Arc<FakeCamera> {
		Arc::new(FakeCamera {
			inner: Arc::new(self.state),
		})
	}
}

fn unset(method: &'static str) -> ! {
	panic!("FakeCamera: {} not configured for this test", method)
}

#[async_trait]
impl CameraDriver for FakeCamera {
	async fn listen_on_motion(&self) -> Result<MotionData> {
		if let Some(f) = self.inner.motion_stream_error.as_ref() {
			return Err(f());
		}
		match self.inner.motion_stream.lock().unwrap().pop_front() {
			Some(data) => Ok(data),
			None => unset("listen_on_motion (stream not configured, or already consumed)"),
		}
	}

	async fn battery_info(&self) -> Result<BatteryInfo> {
		if self.inner.battery_info_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.battery_info.as_ref() {
			Some(f) => f(),
			None => unset("battery_info"),
		}
	}

	async fn get_pirstate(&self) -> Result<RfAlarmCfg> {
		if self.inner.get_pirstate_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.get_pirstate.as_ref() {
			Some(f) => f(),
			None => unset("get_pirstate"),
		}
	}

	async fn pir_set(&self, state: bool) -> Result<()> {
		self.inner.calls.pir_set.lock().unwrap().push(state);
		Ok(())
	}

	async fn listen_on_floodlight(&self) -> Result<Receiver<FloodlightStatusList>> {
		if let Some(f) = self.inner.floodlight_stream_error.as_ref() {
			return Err(f());
		}
		match self.inner.floodlight_stream.lock().unwrap().take() {
			Some(rx) => Ok(rx),
			None => unset("listen_on_floodlight (stream not configured, or already consumed)"),
		}
	}

	async fn is_floodlight_tasks_enabled(&self) -> Result<bool> {
		if self.inner.is_floodlight_tasks_enabled_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.is_floodlight_tasks_enabled.as_ref() {
			Some(f) => f(),
			None => unset("is_floodlight_tasks_enabled"),
		}
	}

	async fn floodlight_tasks_enable(&self, state: bool) -> Result<()> {
		self.inner
			.calls
			.floodlight_tasks_enable
			.lock()
			.unwrap()
			.push(state);
		Ok(())
	}

	async fn set_floodlight_manual(&self, state: bool, duration: u16) -> Result<()> {
		self.inner
			.calls
			.set_floodlight_manual
			.lock()
			.unwrap()
			.push((state, duration));
		Ok(())
	}

	async fn send_ptz(&self, direction: Direction, amount: f32) -> Result<()> {
		self.inner
			.calls
			.send_ptz
			.lock()
			.unwrap()
			.push((direction, amount));
		if self.inner.send_ptz_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		Ok(())
	}

	async fn set_ptz_preset(&self, preset_id: u8, name: String) -> Result<()> {
		self.inner
			.calls
			.set_ptz_preset
			.lock()
			.unwrap()
			.push((preset_id, name));
		Ok(())
	}

	async fn moveto_ptz_preset(&self, preset_id: u8) -> Result<()> {
		self.inner
			.calls
			.moveto_ptz_preset
			.lock()
			.unwrap()
			.push(preset_id);
		Ok(())
	}

	async fn zoom_to(&self, zoom_pos: u32) -> Result<()> {
		self.inner.calls.zoom_to.lock().unwrap().push(zoom_pos);
		Ok(())
	}

	async fn get_snapshot(&self) -> Result<Vec<u8>> {
		if self.inner.get_snapshot_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.get_snapshot.as_ref() {
			Some(f) => f(),
			None => unset("get_snapshot"),
		}
	}

	async fn led_light_set(&self, state: bool) -> Result<()> {
		self.inner.calls.led_light_set.lock().unwrap().push(state);
		self.led_write_result()
	}

	async fn irled_light_set(&self, state: LightState) -> Result<()> {
		self.inner.calls.irled_light_set.lock().unwrap().push(state);
		self.led_write_result()
	}

	async fn reboot(&self) -> Result<()> {
		*self.inner.calls.reboot.lock().unwrap() += 1;
		Ok(())
	}

	async fn siren(&self) -> Result<()> {
		*self.inner.calls.siren.lock().unwrap() += 1;
		if self.inner.siren_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		Ok(())
	}

	async fn ability_advertised(&self, name: &str) -> Option<bool> {
		let names = self.inner.abilities.as_ref()?;
		Some(names.iter().any(|a| a == name))
	}

	fn camera_ip(&self) -> Option<IpAddr> {
		self.inner.camera_ip
	}

	async fn get_ledstate(&self) -> Result<LedState> {
		if self.inner.get_ledstate_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.get_ledstate.as_ref() {
			Some(f) => f(),
			None => unset("get_ledstate"),
		}
	}

	async fn get_ha_cfg(&self) -> Result<Option<HaCfg>> {
		if self.inner.get_ha_cfg_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.get_ha_cfg.as_ref() {
			Some(f) => f(),
			None => unset("get_ha_cfg"),
		}
	}

	async fn set_ha_cfg(&self, cfg: &HaCfg) -> Result<Option<HaCfg>> {
		self.inner
			.calls
			.set_ha_cfg
			.lock()
			.unwrap()
			.push(cfg.clone());
		if self.inner.set_ha_cfg_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		self.get_ha_cfg().await
	}

	async fn get_ptz_preset(&self) -> Result<PtzPreset> {
		if self.inner.get_ptz_preset_pending {
			std::future::pending::<()>().await;
			unreachable!()
		}
		match self.inner.get_ptz_preset.as_ref() {
			Some(f) => f(),
			None => unset("get_ptz_preset"),
		}
	}

	async fn version(&self) -> Result<VersionInfo> {
		match self.inner.version.as_ref() {
			Some(f) => f(),
			None => unset("version"),
		}
	}

	async fn set_time(&self, timestamp: OffsetDateTime) -> Result<()> {
		self.inner.calls.set_time.lock().unwrap().push(timestamp);
		Ok(())
	}

	async fn get_users(&self) -> Result<UserList> {
		match self.inner.get_users.as_ref() {
			Some(f) => f(),
			None => unset("get_users"),
		}
	}
	async fn add_user(&self, user_name: String, password: String, user_level: u8) -> Result<()> {
		self.inner
			.calls
			.add_user
			.lock()
			.unwrap()
			.push((user_name, password, user_level));
		Ok(())
	}
	async fn modify_user(&self, user_name: String, password: String) -> Result<()> {
		self.inner
			.calls
			.modify_user
			.lock()
			.unwrap()
			.push((user_name, password));
		Ok(())
	}
	async fn delete_user(&self, user_name: String) -> Result<()> {
		self.inner.calls.delete_user.lock().unwrap().push(user_name);
		Ok(())
	}

	async fn get_serverport(&self) -> Result<ServerPort> {
		match self.inner.get_serverport.as_ref() {
			Some(f) => f(),
			None => unset("get_serverport"),
		}
	}
	async fn set_serverport(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_serverport
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}
	async fn get_http(&self) -> Result<HttpPort> {
		match self.inner.get_http.as_ref() {
			Some(f) => f(),
			None => unset("get_http"),
		}
	}
	async fn set_http(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_http
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}
	async fn get_https(&self) -> Result<HttpsPort> {
		match self.inner.get_https.as_ref() {
			Some(f) => f(),
			None => unset("get_https"),
		}
	}
	async fn set_https(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_https
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}
	async fn get_rtsp(&self) -> Result<RtspPort> {
		match self.inner.get_rtsp.as_ref() {
			Some(f) => f(),
			None => unset("get_rtsp"),
		}
	}
	async fn set_rtsp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_rtsp
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}
	async fn get_rtmp(&self) -> Result<RtmpPort> {
		match self.inner.get_rtmp.as_ref() {
			Some(f) => f(),
			None => unset("get_rtmp"),
		}
	}
	async fn set_rtmp(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_rtmp
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}
	async fn get_onvif(&self) -> Result<OnvifPort> {
		match self.inner.get_onvif.as_ref() {
			Some(f) => f(),
			None => unset("get_onvif"),
		}
	}
	async fn set_onvif(&self, set_on: Option<bool>, set_port: Option<u32>) -> Result<()> {
		self.inner
			.calls
			.set_onvif
			.lock()
			.unwrap()
			.push((set_on, set_port));
		Ok(())
	}

	async fn get_linktype(&self) -> Result<LinkType> {
		match self.inner.get_linktype.as_ref() {
			Some(f) => f(),
			None => unset("get_linktype"),
		}
	}

	async fn get_support(&self) -> Result<Support> {
		match self.inner.get_support.as_ref() {
			Some(f) => f(),
			None => unset("get_support"),
		}
	}

	async fn get_abilityinfo(&self) -> Result<AbilityInfo> {
		match self.inner.get_abilityinfo.as_ref() {
			Some(f) => f(),
			None => unset("get_abilityinfo"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc::xml::BatteryInfo;

	/// End-to-end smoke test: a builder-scripted read value and a
	/// recorded setter call both come back through the trait object.
	#[tokio::test]
	async fn fake_camera_scaffolding_compiles_and_drives_through_trait() {
		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Ok(BatteryInfo {
					battery_percent: 42,
					..Default::default()
				})
			})
			.with_snapshot(|| Ok(b"jpeg-bytes".to_vec()))
			.build();

		// Hold as Arc<dyn CameraDriver> to prove dyn-compatibility.
		let driver: Arc<dyn CameraDriver> = fake.clone();

		let info = driver.battery_info().await.unwrap();
		assert_eq!(info.battery_percent, 42);

		let snap = driver.get_snapshot().await.unwrap();
		assert_eq!(snap, b"jpeg-bytes");

		driver.pir_set(true).await.unwrap();
		driver.reboot().await.unwrap();
		driver.reboot().await.unwrap();

		assert_eq!(*fake.calls().pir_set.lock().unwrap(), vec![true]);
		assert_eq!(*fake.calls().reboot.lock().unwrap(), 2);
	}

	#[tokio::test]
	#[should_panic(expected = "FakeCamera: battery_info not configured")]
	async fn unconfigured_read_method_panics_with_clear_message() {
		let fake = FakeCameraBuilder::new().build();
		let _ = fake.battery_info().await;
	}
}
