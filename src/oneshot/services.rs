use anyhow::{Context, Result};
use bairelay_neolink_core::bc_protocol::CameraDriver;

use super::errors::UsageError;
use super::output::{Outcome, ServiceEntry};

/// Which camera network service to inspect or configure.
#[derive(Debug, Clone, Copy)]
pub enum Service {
	Baichuan,
	Http,
	Https,
	Rtmp,
	Rtsp,
	Onvif,
}

impl Service {
	fn label(self) -> &'static str {
		match self {
			Service::Baichuan => "baichuan",
			Service::Http => "http",
			Service::Https => "https",
			Service::Rtmp => "rtmp",
			Service::Rtsp => "rtsp",
			Service::Onvif => "onvif",
		}
	}
}

/// What to do with the service: read, enable, disable, set port, or both.
///
/// Port is `u16` (matching the CLI surface in `cli.rs::ServiceAction`).
/// `0` is rejected at dispatch time as `UsageError` (operator typo
/// guard); the cast to `u32` happens at the `CameraDriver` boundary
/// since bairelay_neolink_core's `set_http(set_on, set_port: Option<u32>)` etc.
/// take a wider type.
#[derive(Debug, Clone)]
pub enum Action {
	Get,
	On,
	Off,
	Port(u16),
	Set { port: u16, enabled: bool },
}

pub async fn run(cam: &dyn CameraDriver, service: Service, action: Action) -> Result<Outcome> {
	// Apply the mutation first (if any), then always read back so the
	// returned Outcome reflects the camera's current state.
	match &action {
		Action::Get => {}
		Action::On => apply_toggle(cam, service, Some(true), None).await?,
		Action::Off => apply_toggle(cam, service, Some(false), None).await?,
		Action::Port(p) => apply_toggle(cam, service, None, Some(*p)).await?,
		Action::Set { port, enabled } => {
			apply_toggle(cam, service, Some(*enabled), Some(*port)).await?
		}
	}

	let (port, enabled) = read(cam, service).await?;
	Ok(Outcome::Service {
		service: service.label().into(),
		port,
		enabled,
	})
}

/// Per-RPC timeout for `run_all`'s six sequential reads. The outer
/// `runner::OP_TIMEOUT = 30 s` covers the whole call; without an
/// inner cap a single hung RPC consumes the entire budget and the
/// operator gets zero partial results. 8 s × 6 = 48 s worst case,
/// 2 s above the outer cap — runner returns ConnectionTimeout
/// before the last RPC fires, which is fine: the operator sees a
/// useful "5 of 6 services answered" output.
const PER_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

/// Read every service in one shot. Used by `bairelay services <cam>`
/// with no positional service argument. Each failed service becomes an
/// entry with `enabled = None` rather than failing the whole call —
/// older firmwares don't expose all six on every channel, and any
/// individual RPC that exceeds `PER_RPC_TIMEOUT` degrades to the
/// same "unknown" entry instead of starving the remaining reads.
pub async fn run_all(cam: &dyn CameraDriver) -> Result<Outcome> {
	const ALL: [Service; 6] = [
		Service::Baichuan,
		Service::Http,
		Service::Https,
		Service::Rtmp,
		Service::Rtsp,
		Service::Onvif,
	];
	let mut services = Vec::with_capacity(ALL.len());
	for svc in ALL {
		match tokio::time::timeout(PER_RPC_TIMEOUT, read(cam, svc)).await {
			Ok(Ok((port, enabled))) => services.push(ServiceEntry {
				name: svc.label().into(),
				port,
				enabled,
			}),
			Ok(Err(_)) | Err(_) => services.push(ServiceEntry {
				name: svc.label().into(),
				port: 0,
				enabled: None,
			}),
		}
	}
	Ok(Outcome::ServiceList { services })
}

async fn apply_toggle(
	cam: &dyn CameraDriver,
	service: Service,
	set_on: Option<bool>,
	set_port: Option<u16>,
) -> Result<()> {
	if matches!(set_port, Some(0)) {
		return Err(UsageError::new(format!("port 0 is not valid for {}", service.label())).into());
	}
	let set_port = set_port.map(u32::from);
	match service {
		Service::Baichuan => cam
			.set_serverport(set_on, set_port)
			.await
			.context("set_serverport failed")?,
		Service::Http => cam
			.set_http(set_on, set_port)
			.await
			.context("set_http failed")?,
		Service::Https => cam
			.set_https(set_on, set_port)
			.await
			.context("set_https failed")?,
		Service::Rtmp => cam
			.set_rtmp(set_on, set_port)
			.await
			.context("set_rtmp failed")?,
		Service::Rtsp => cam
			.set_rtsp(set_on, set_port)
			.await
			.context("set_rtsp failed")?,
		Service::Onvif => cam
			.set_onvif(set_on, set_port)
			.await
			.context("set_onvif failed")?,
	}
	Ok(())
}

async fn read(cam: &dyn CameraDriver, service: Service) -> Result<(u32, Option<bool>)> {
	let (port, enable) = match service {
		Service::Baichuan => {
			let s = cam
				.get_serverport()
				.await
				.context("get_serverport failed")?;
			(s.port, s.enable)
		}
		Service::Http => {
			let s = cam.get_http().await.context("get_http failed")?;
			(s.port, s.enable)
		}
		Service::Https => {
			let s = cam.get_https().await.context("get_https failed")?;
			(s.port, s.enable)
		}
		Service::Rtmp => {
			let s = cam.get_rtmp().await.context("get_rtmp failed")?;
			(s.port, s.enable)
		}
		Service::Rtsp => {
			let s = cam.get_rtsp().await.context("get_rtsp failed")?;
			(s.port, s.enable)
		}
		Service::Onvif => {
			let s = cam.get_onvif().await.context("get_onvif failed")?;
			(s.port, s.enable)
		}
	};
	Ok((port, enable.map(|e| e != 0)))
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::{
		HttpPort, HttpsPort, OnvifPort, RtmpPort, RtspPort, ServerPort,
	};
	use bairelay_neolink_core::bc_protocol::{Error, FakeCameraBuilder};

	fn all_enabled_fake() -> std::sync::Arc<bairelay_neolink_core::bc_protocol::FakeCamera> {
		FakeCameraBuilder::new()
			.with_serverport(|| {
				Ok(ServerPort {
					port: 9000,
					enable: Some(1),
					..Default::default()
				})
			})
			.with_http(|| {
				Ok(HttpPort {
					port: 80,
					enable: Some(1),
					..Default::default()
				})
			})
			.with_https(|| {
				Ok(HttpsPort {
					port: 443,
					enable: Some(0),
					..Default::default()
				})
			})
			.with_rtsp(|| {
				Ok(RtspPort {
					port: 554,
					enable: Some(1),
					..Default::default()
				})
			})
			.with_rtmp(|| {
				Ok(RtmpPort {
					port: 1935,
					enable: Some(1),
					..Default::default()
				})
			})
			.with_onvif(|| {
				Ok(OnvifPort {
					port: 8000,
					enable: Some(1),
					..Default::default()
				})
			})
			.build()
	}

	#[tokio::test]
	async fn service_label_all_variants() {
		assert_eq!(Service::Baichuan.label(), "baichuan");
		assert_eq!(Service::Http.label(), "http");
		assert_eq!(Service::Https.label(), "https");
		assert_eq!(Service::Rtmp.label(), "rtmp");
		assert_eq!(Service::Rtsp.label(), "rtsp");
		assert_eq!(Service::Onvif.label(), "onvif");
	}

	#[tokio::test]
	async fn run_get_http_returns_current_state() {
		let fake = all_enabled_fake();
		let outcome = run(&*fake, Service::Http, Action::Get).await.unwrap();
		assert_eq!(
			outcome,
			Outcome::Service {
				service: "http".into(),
				port: 80,
				enabled: Some(true),
			}
		);
	}

	#[tokio::test]
	async fn run_on_baichuan_records_set_call() {
		let fake = all_enabled_fake();
		let _ = run(&*fake, Service::Baichuan, Action::On).await.unwrap();
		assert_eq!(
			*fake.calls().set_serverport.lock().unwrap(),
			vec![(Some(true), None)]
		);
	}

	#[tokio::test]
	async fn run_off_https_records_set_call() {
		let fake = all_enabled_fake();
		let _ = run(&*fake, Service::Https, Action::Off).await.unwrap();
		assert_eq!(
			*fake.calls().set_https.lock().unwrap(),
			vec![(Some(false), None)]
		);
	}

	#[tokio::test]
	async fn run_port_rtsp_records_set_call() {
		let fake = all_enabled_fake();
		let _ = run(&*fake, Service::Rtsp, Action::Port(8554))
			.await
			.unwrap();
		assert_eq!(
			*fake.calls().set_rtsp.lock().unwrap(),
			vec![(None, Some(8554))]
		);
	}

	#[tokio::test]
	async fn run_set_rtmp_records_both_args() {
		let fake = all_enabled_fake();
		let _ = run(
			&*fake,
			Service::Rtmp,
			Action::Set {
				port: 1935,
				enabled: false,
			},
		)
		.await
		.unwrap();
		assert_eq!(
			*fake.calls().set_rtmp.lock().unwrap(),
			vec![(Some(false), Some(1935))]
		);
	}

	#[tokio::test]
	async fn run_onvif_set_records_call() {
		let fake = all_enabled_fake();
		let _ = run(&*fake, Service::Onvif, Action::On).await.unwrap();
		assert_eq!(
			*fake.calls().set_onvif.lock().unwrap(),
			vec![(Some(true), None)]
		);
	}

	#[tokio::test]
	async fn run_http_set_records_call() {
		let fake = all_enabled_fake();
		let _ = run(&*fake, Service::Http, Action::On).await.unwrap();
		assert_eq!(
			*fake.calls().set_http.lock().unwrap(),
			vec![(Some(true), None)]
		);
	}

	#[tokio::test]
	async fn run_all_lists_all_six_services() {
		let fake = all_enabled_fake();
		let outcome = run_all(&*fake).await.unwrap();
		let Outcome::ServiceList { services } = outcome else {
			panic!("wrong variant");
		};
		assert_eq!(services.len(), 6);
		let names: Vec<_> = services.iter().map(|s| s.name.as_str()).collect();
		assert_eq!(
			names,
			vec!["baichuan", "http", "https", "rtmp", "rtsp", "onvif"]
		);
	}

	#[tokio::test]
	async fn run_all_failing_service_becomes_unknown_entry() {
		let fake = FakeCameraBuilder::new()
			.with_serverport(|| Err(Error::Other("bc down")))
			.with_http(|| {
				Ok(HttpPort {
					port: 80,
					enable: Some(1),
					..Default::default()
				})
			})
			.with_https(|| Err(Error::Other("none")))
			.with_rtsp(|| {
				Ok(RtspPort {
					port: 554,
					enable: None,
					..Default::default()
				})
			})
			.with_rtmp(|| Err(Error::Other("none")))
			.with_onvif(|| Err(Error::Other("none")))
			.build();
		let outcome = run_all(&*fake).await.unwrap();
		let Outcome::ServiceList { services } = outcome else {
			panic!("wrong variant");
		};
		// baichuan failed
		assert_eq!(services[0].name, "baichuan");
		assert_eq!(services[0].port, 0);
		assert!(services[0].enabled.is_none());
		// http ok
		assert_eq!(services[1].name, "http");
		assert_eq!(services[1].port, 80);
		assert_eq!(services[1].enabled, Some(true));
		// rtsp port returned but enable=None
		assert_eq!(services[4].name, "rtsp");
		assert_eq!(services[4].port, 554);
		assert!(services[4].enabled.is_none());
	}

	#[tokio::test]
	async fn run_port_zero_is_usage_error() {
		// Operator typo guard: `bairelay services cam http port 0` would
		// otherwise reach the camera and surface as an opaque
		// camera-side rejection. Map to UsageError → EXIT_USAGE = 2 so
		// CI scripts can branch cleanly.
		let fake = all_enabled_fake();
		let err = run(&*fake, Service::Http, Action::Port(0))
			.await
			.expect_err("port 0 must be rejected");
		assert!(format!("{err:#}").contains("port 0 is not valid"));
		// Camera was never asked.
		assert!(fake.calls().set_http.lock().unwrap().is_empty());
	}

	#[tokio::test]
	async fn run_set_with_port_zero_is_usage_error() {
		let fake = all_enabled_fake();
		let err = run(
			&*fake,
			Service::Rtsp,
			Action::Set {
				port: 0,
				enabled: true,
			},
		)
		.await
		.expect_err("port 0 must be rejected");
		assert!(format!("{err:#}").contains("port 0 is not valid"));
		assert!(fake.calls().set_rtsp.lock().unwrap().is_empty());
	}
}
