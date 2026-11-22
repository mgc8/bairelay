//! Integration tests for the wake-server crate. All sockets bind to
//! `127.0.0.1:0` so the OS picks an ephemeral port; tests parallelise
//! cleanly without colliding on the well-known 9999 / 58200.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use bairelay_neolink_core::bcudp::xml::{C2mQ, IpPort, M2cQr, UdpXml};
use bairelay_wake_server::config::RuntimeConfig;
use bairelay_wake_server::packet::{decode_discovery, encode_discovery};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

async fn recv_one(sock: &UdpSocket) -> (Vec<u8>, SocketAddr) {
	let mut buf = vec![0u8; 4096];
	let (n, src) = tokio::time::timeout(Duration::from_millis(500), sock.recv_from(&mut buf))
		.await
		.expect("recv timeout")
		.expect("recv ok");
	buf.truncate(n);
	(buf, src)
}

#[tokio::test]
async fn middleman_replies_to_c2m_q_with_register_addr() {
	let cancel = CancellationToken::new();

	// Bind both sockets first so we know the ephemeral ports for assertions.
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let middleman_addr = middleman.local_addr().unwrap();
	let register_addr = register.local_addr().unwrap();

	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: middleman_addr.port(),
		register_port: register_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};

	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Fake client.
	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let q = encode_discovery(
		0xaabbccdd,
		UdpXml::C2mQ(C2mQ {
			uid: "TESTUID".into(),
			os: "MAC".into(),
		}),
	)
	.unwrap();
	client.send_to(&q, middleman_addr).await.unwrap();

	let (reply, _src) = recv_one(&client).await;
	let (tid, payload) = decode_discovery(&reply).unwrap();
	assert_eq!(tid, 0xaabbccdd, "tid echoed");
	match payload {
		UdpXml::M2cQr(M2cQr {
			reg: Some(IpPort { ip, port }),
			..
		}) => {
			assert_eq!(ip, "127.0.0.1");
			assert_eq!(port, register_addr.port());
		}
		other => panic!("expected M2cQr with reg.port = register port, got {other:?}"),
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

use bairelay_neolink_core::bcudp::xml::{
	C2dHb, C2rC, C2rCfm, D2rCr, D2rDisc, D2rHb, R2cCr, R2cT, R2dC, R2dDcr, R2dHbr,
};

#[tokio::test]
async fn heartbeat_records_and_replies() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let hb = encode_discovery(
		0x42,
		UdpXml::D2rHb(D2rHb {
			uid: "UIDX".into(),
			dev: None,
			needrsp: Some(1),
			token: 999,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();

	let (reply, _) = recv_one(&cam).await;
	let (tid, payload) = decode_discovery(&reply).unwrap();
	assert_eq!(tid, 0x42);
	match payload {
		UdpXml::R2dHbr(R2dHbr { rsp: 0, timer, .. }) => {
			assert_eq!(timer.hb, 20000);
		}
		other => panic!("expected R2dHbr, got {other:?}"),
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn heartbeat_without_needrsp_records_silently() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let hb = encode_discovery(
		0x99,
		UdpXml::D2rHb(D2rHb {
			uid: "QUIET".into(),
			dev: None,
			needrsp: None,
			token: 1,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();

	// Nothing should come back.
	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(100), cam.recv_from(&mut buf)).await;
	assert!(res.is_err(), "expected no reply when needrsp is absent");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn c2r_c_for_known_uid_emits_burst_and_replies() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Camera registers via heartbeat so its address is known.
	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let cam_addr = cam.local_addr().unwrap();
	let hb = encode_discovery(
		0x1,
		UdpXml::D2rHb(D2rHb {
			uid: "CAM1".into(),
			dev: None,
			needrsp: Some(1),
			token: 7,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();
	let _ = recv_one(&cam).await; // R2D_HB_R

	// Client requests wake.
	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let c2r = encode_discovery(
		0xabc,
		UdpXml::C2rC(C2rC {
			uid: "CAM1".into(),
			cli: IpPort {
				ip: "127.0.0.1".into(),
				port: 12345,
			},
			relay: IpPort {
				ip: "127.0.0.1".into(),
				port: reg_addr.port(),
			},
			cid: 42,
			debug: false,
			family: 4,
			os: "MAC".into(),
			revision: None,
		}),
	)
	.unwrap();
	client.send_to(&c2r, reg_addr).await.unwrap();

	// Two replies are expected at the client side: R2C_C_R then R2C_T.
	let (r1, _) = recv_one(&client).await;
	let (_, p1) = decode_discovery(&r1).unwrap();
	let sid = match p1 {
		UdpXml::R2cCr(R2cCr {
			rsp: 0,
			sid: Some(s),
			dev: Some(d),
			..
		}) => {
			assert_eq!(d.port, cam_addr.port());
			s
		}
		other => panic!("expected R2cCr, got {other:?}"),
	};

	let (r2, _) = recv_one(&client).await;
	let (_, p2) = decode_discovery(&r2).unwrap();
	match p2 {
		UdpXml::R2cT(R2cT {
			sid: s, cid: 42, ..
		}) => assert_eq!(s, sid),
		other => panic!("expected R2cT, got {other:?}"),
	}

	// Camera-side: 10 R2D_C wake packets within ~1.1 s, at >= 80 ms gaps.
	let mut wake_ts = Vec::new();
	for _ in 0..10 {
		let mut buf = vec![0u8; 4096];
		let (n, _) = tokio::time::timeout(Duration::from_millis(2000), cam.recv_from(&mut buf))
			.await
			.unwrap()
			.unwrap();
		let now = std::time::Instant::now();
		wake_ts.push(now);
		let (_, payload) = decode_discovery(&buf[..n]).unwrap();
		match payload {
			UdpXml::R2dC(R2dC {
				sid: s, cid: 42, ..
			}) => assert_eq!(s, sid),
			other => panic!("expected R2dC, got {other:?}"),
		}
	}
	for w in wake_ts.windows(2) {
		let gap = w[1].duration_since(w[0]);
		assert!(
			gap >= Duration::from_millis(80),
			"wake gap too short: {:?}",
			gap
		);
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn c2r_c_for_unknown_uid_replies_with_rsp_neg1() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let c2r = encode_discovery(
		0xabc,
		UdpXml::C2rC(C2rC {
			uid: "GHOST".into(),
			cli: IpPort {
				ip: "127.0.0.1".into(),
				port: 12345,
			},
			relay: IpPort {
				ip: "127.0.0.1".into(),
				port: reg_addr.port(),
			},
			cid: 42,
			debug: false,
			family: 4,
			os: "MAC".into(),
			revision: None,
		}),
	)
	.unwrap();
	client.send_to(&c2r, reg_addr).await.unwrap();

	let (r, _) = recv_one(&client).await;
	let (_, p) = decode_discovery(&r).unwrap();
	match p {
		UdpXml::R2cCr(R2cCr {
			rsp: -1,
			sid: None,
			dev: None,
			..
		}) => {}
		other => panic!("expected rsp=-1 R2cCr, got {other:?}"),
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn d2r_disc_is_acked_with_matching_sid() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(0xfeed, UdpXml::D2rDisc(D2rDisc { sid: 12345 })).unwrap();
	cam.send_to(&pkt, reg_addr).await.unwrap();

	let (r, _) = recv_one(&cam).await;
	let (tid, payload) = decode_discovery(&r).unwrap();
	assert_eq!(tid, 0xfeed);
	match payload {
		UdpXml::R2dDcr(R2dDcr { sid: 12345, rsp: 0 }) => {}
		other => panic!("expected R2dDcr{{sid=12345, rsp=0}}, got {other:?}"),
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test(start_paused = true)]
async fn stale_uid_reads_as_unknown() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 1000,
		stale_after_ms: 2000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Camera registers (no needrsp: avoids awaiting a reply under paused
	// virtual time).
	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let hb = encode_discovery(
		0x1,
		UdpXml::D2rHb(D2rHb {
			uid: "STALE".into(),
			dev: None,
			needrsp: None,
			token: 1,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();
	// Yield so the register loop has a real chance to record the
	// heartbeat before we advance virtual time past the TTL.
	for _ in 0..20 {
		tokio::task::yield_now().await;
	}

	// Advance virtual time past the TTL.
	tokio::time::advance(Duration::from_secs(10)).await;

	// Client tries to wake.
	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let c2r = encode_discovery(
		0x2,
		UdpXml::C2rC(C2rC {
			uid: "STALE".into(),
			cli: IpPort {
				ip: "127.0.0.1".into(),
				port: 1,
			},
			relay: IpPort {
				ip: "127.0.0.1".into(),
				port: reg_addr.port(),
			},
			cid: 1,
			debug: false,
			family: 4,
			os: "MAC".into(),
			revision: None,
		}),
	)
	.unwrap();
	client.send_to(&c2r, reg_addr).await.unwrap();

	// Under paused virtual time, `tokio::time::timeout` auto-advances and
	// fires before real UDP I/O completes. Use a yield-driven poll instead.
	let mut buf = vec![0u8; 4096];
	let mut got = None;
	for _ in 0..200 {
		if let Ok((n, _)) = client.try_recv_from(&mut buf) {
			got = Some(n);
			break;
		}
		tokio::task::yield_now().await;
	}
	let n = got.expect("R2cCr reply did not arrive within 200 yields");
	let (_, p) = decode_discovery(&buf[..n]).unwrap();
	assert!(matches!(p, UdpXml::R2cCr(R2cCr { rsp: -1, .. })));

	// No wake packet should reach the (stale) camera socket.
	let mut buf2 = vec![0u8; 4096];
	let mut wake_seen = false;
	for _ in 0..50 {
		if cam.try_recv_from(&mut buf2).is_ok() {
			wake_seen = true;
			break;
		}
		tokio::task::yield_now().await;
	}
	assert!(!wake_seen, "stale entry should not produce a wake burst");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn loopback_self_call_succeeds() {
	// The "bairelay reaches its own wake server" guard. Both sockets
	// bound to 127.0.0.1; one process opens a client socket and queries
	// C2M_Q; the M2C_Q_R response must arrive at the configured register
	// ephemeral port.
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let q = encode_discovery(
		0x55,
		UdpXml::C2mQ(C2mQ {
			uid: "SELF".into(),
			os: "MAC".into(),
		}),
	)
	.unwrap();
	client.send_to(&q, mid_addr).await.unwrap();
	let (r, _) = recv_one(&client).await;
	let (_, p) = decode_discovery(&r).unwrap();
	match p {
		UdpXml::M2cQr(M2cQr {
			reg: Some(IpPort { ref ip, port }),
			..
		}) => {
			assert_eq!(ip, "127.0.0.1");
			assert_eq!(port, reg_addr.port());
		}
		other => panic!("expected M2cQr, got {other:?}"),
	}

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn bad_packets_do_not_crash_listener() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let attacker = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	for junk in [
		&[0u8; 4][..],
		&[0xff; 32][..],
		&[0x3a, 0xcf, 0x87, 0x2a, 0xff, 0xff, 0xff, 0xff][..], // bad payload size
	] {
		attacker.send_to(junk, mid_addr).await.unwrap();
		attacker.send_to(junk, reg_addr).await.unwrap();
	}

	// Listener still alive: a valid C2M_Q after the junk still gets a reply.
	let q = encode_discovery(
		0x77,
		UdpXml::C2mQ(C2mQ {
			uid: "AFTERJUNK".into(),
			os: "MAC".into(),
		}),
	)
	.unwrap();
	attacker.send_to(&q, mid_addr).await.unwrap();
	let (r, _) = recv_one(&attacker).await;
	let (tid, p) = decode_discovery(&r).unwrap();
	assert_eq!(tid, 0x77);
	assert!(matches!(p, UdpXml::M2cQr(_)));

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn run_binds_configured_ports_and_cancels_cleanly() {
	// Capture two ephemeral ports by binding then dropping; then ask the
	// production `run()` driver to bind those same ports itself.
	let middleman_probe = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register_probe = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_port = middleman_probe.local_addr().unwrap().port();
	let reg_port = register_probe.local_addr().unwrap().port();
	drop(middleman_probe);
	drop(register_probe);

	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_port,
		register_port: reg_port,
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let cancel = CancellationToken::new();
	let handle = tokio::spawn(bairelay_wake_server::run(
		cfg,
		bairelay_wake_server::make_registry(),
		cancel.clone(),
	));

	// Give the spawned task a chance to bind both sockets before we cancel.
	tokio::time::sleep(Duration::from_millis(50)).await;
	cancel.cancel();
	let res = tokio::time::timeout(Duration::from_millis(500), handle)
		.await
		.expect("run did not exit within 500 ms");
	let inner = res.expect("task panicked");
	inner.expect("inner Result should be Ok");
}

#[tokio::test]
async fn run_returns_bind_error_on_register_port_collision() {
	// Hold the register port (second bind attempt inside `run()`) so the
	// register-side bind path errors. Middleman gets a fresh port so the
	// first bind succeeds and the failure is provably the register one.
	let mid_probe = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_port = mid_probe.local_addr().unwrap().port();
	drop(mid_probe);

	let occupier = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let reg_port = occupier.local_addr().unwrap().port();

	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_port,
		register_port: reg_port,
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let cancel = CancellationToken::new();
	let res = bairelay_wake_server::run(cfg, bairelay_wake_server::make_registry(), cancel).await;
	match res {
		Err(bairelay_wake_server::WakeServerError::Bind { addr, .. }) => {
			assert_eq!(addr.port(), reg_port);
		}
		other => panic!("expected Bind error on register port collision, got {other:?}"),
	}
	drop(occupier);
}

#[tokio::test]
async fn cancel_during_wake_burst_aborts_remaining_packets() {
	// Trigger a wake burst, then cancel before all 10 packets have been
	// emitted. Exercises the cancel branch inside the burst loop.
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Camera registers so its UID is fresh.
	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let hb = encode_discovery(
		0x1,
		UdpXml::D2rHb(D2rHb {
			uid: "BURSTCAM".into(),
			dev: None,
			needrsp: Some(1),
			token: 7,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();
	let _ = recv_one(&cam).await;

	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let c2r = encode_discovery(
		0xabc,
		UdpXml::C2rC(C2rC {
			uid: "BURSTCAM".into(),
			cli: IpPort {
				ip: "127.0.0.1".into(),
				port: 12345,
			},
			relay: IpPort {
				ip: "127.0.0.1".into(),
				port: reg_addr.port(),
			},
			cid: 1,
			debug: false,
			family: 4,
			os: "MAC".into(),
			revision: None,
		}),
	)
	.unwrap();
	client.send_to(&c2r, reg_addr).await.unwrap();

	// Drain first wake packet so we're sure the burst started.
	let mut buf = vec![0u8; 4096];
	let _ = tokio::time::timeout(Duration::from_millis(500), cam.recv_from(&mut buf))
		.await
		.expect("first wake packet")
		.expect("recv ok");

	// Cancel during the inter-packet sleep so the burst loop hits the
	// `cancelled()` branch.
	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn run_returns_bind_error_on_port_collision() {
	// Hold the middleman port open so `run()` cannot bind to it.
	let occupier = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let occupied_port = occupier.local_addr().unwrap().port();

	// Capture a fresh port for the register socket.
	let reg_probe = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let reg_port = reg_probe.local_addr().unwrap().port();
	drop(reg_probe);

	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: occupied_port,
		register_port: reg_port,
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let cancel = CancellationToken::new();
	let res = bairelay_wake_server::run(cfg, bairelay_wake_server::make_registry(), cancel).await;
	match res {
		Err(bairelay_wake_server::WakeServerError::Bind { addr, .. }) => {
			assert_eq!(addr.port(), occupied_port);
		}
		other => panic!("expected Bind error on port collision, got {other:?}"),
	}
	drop(occupier);
}

#[tokio::test]
async fn register_logs_d2r_c_r_without_replying() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(
		0xd2c,
		UdpXml::D2rCr(D2rCr {
			sid: 1,
			dev: None,
			rsp: 0,
		}),
	)
	.unwrap();
	cam.send_to(&pkt, reg_addr).await.unwrap();

	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(100), cam.recv_from(&mut buf)).await;
	assert!(res.is_err(), "register should not reply to D2R_C_R");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn register_logs_c2r_cfm_without_replying() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(
		0xcf_u32,
		UdpXml::C2rCfm(C2rCfm {
			sid: 1,
			conn: "local".into(),
			rsp: 0,
			cid: 1,
			did: 1,
		}),
	)
	.unwrap();
	client.send_to(&pkt, reg_addr).await.unwrap();

	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(100), client.recv_from(&mut buf)).await;
	assert!(res.is_err(), "register should not reply to C2R_CFM");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn register_drops_unhandled_xml_variant() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let peer = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(0x111, UdpXml::C2dHb(C2dHb { cid: 1, did: 2 })).unwrap();
	peer.send_to(&pkt, reg_addr).await.unwrap();

	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(100), peer.recv_from(&mut buf)).await;
	assert!(res.is_err(), "register should drop unhandled variants");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn middleman_drops_non_c2m_q_xml() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let peer = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(0x222, UdpXml::C2dHb(C2dHb { cid: 1, did: 2 })).unwrap();
	peer.send_to(&pkt, mid_addr).await.unwrap();

	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(100), peer.recv_from(&mut buf)).await;
	assert!(res.is_err(), "middleman should drop non-C2M_Q payloads");

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn c2r_c_for_unknown_uid_does_not_leak_other_uids_address() {
	// Defence-in-depth: register UID-X via heartbeat, then send C2R_C
	// for a different UID from the *same* source IP. The reply must
	// be rsp=-1 with sid/dev unset — registry lookup keys on UID, not
	// on source addr, so the wrong-UID query must not surface UID-X's
	// recorded address. Pin this so a future "speed up by indexing on
	// IP" optimisation can't quietly cross-leak.
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Camera registers UID "ALICE" via heartbeat from its own socket.
	let cam = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let hb = encode_discovery(
		0x1,
		UdpXml::D2rHb(D2rHb {
			uid: "ALICE".into(),
			dev: None,
			needrsp: Some(1),
			token: 7,
		}),
	)
	.unwrap();
	cam.send_to(&hb, reg_addr).await.unwrap();
	let _ = recv_one(&cam).await; // drain R2D_HB_R

	// Client (different socket, but same loopback IP) asks to wake UID
	// "BOB" — never registered. The reply must be rsp=-1 with no
	// surfaced sid/dev for ALICE.
	let client = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let c2r = encode_discovery(
		0xbeef,
		UdpXml::C2rC(C2rC {
			uid: "BOB".into(),
			cli: IpPort {
				ip: "127.0.0.1".into(),
				port: 12345,
			},
			relay: IpPort {
				ip: "127.0.0.1".into(),
				port: reg_addr.port(),
			},
			cid: 99,
			debug: false,
			family: 4,
			os: "MAC".into(),
			revision: None,
		}),
	)
	.unwrap();
	client.send_to(&c2r, reg_addr).await.unwrap();

	let (r, _) = recv_one(&client).await;
	let (_, p) = decode_discovery(&r).unwrap();
	match p {
		UdpXml::R2cCr(R2cCr {
			rsp: -1,
			sid: None,
			dev: None,
			..
		}) => {}
		other => panic!("expected rsp=-1 with no sid/dev, got {other:?}"),
	}

	// Camera must NOT have received a wake burst for BOB's request.
	let mut buf = vec![0u8; 4096];
	let res = tokio::time::timeout(Duration::from_millis(200), cam.recv_from(&mut buf)).await;
	assert!(
		res.is_err(),
		"camera must not get a wake burst for an unrelated UID query"
	);

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

/// Bad-CRC regression: a hostile UDP packet whose CRC doesn't match
/// the payload bytes must not panic the listener. Pre-fix,
/// `assert_eq!(checksum, actual_checksum)` in
/// `crates/core/src/bcudp/de.rs:75` killed the spawned task on any
/// crafted packet, taking both wake-server listeners with it via the
/// JoinHandle race in `run_with_sockets`. Single packet → service
/// dead.
#[tokio::test]
async fn bad_crc_packet_does_not_crash_listener() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	// Craft a discovery packet with a deliberately wrong CRC by taking
	// a valid one and flipping the checksum bytes (offset 16..20:
	// magic[0..4] | payload_size[4..8] | unknown_a[8..12] | tid[12..16]
	// | checksum[16..20] | payload).
	let attacker = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let valid = encode_discovery(
		0x12345678,
		UdpXml::C2mQ(C2mQ {
			uid: "BADCRC".into(),
			os: "MAC".into(),
		}),
	)
	.unwrap();
	let mut wire = valid.clone();
	wire[16..20].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
	attacker.send_to(&wire, mid_addr).await.unwrap();
	attacker.send_to(&wire, reg_addr).await.unwrap();

	// Listener still alive: a follow-up valid C2M_Q gets a reply.
	let q = encode_discovery(
		0x77,
		UdpXml::C2mQ(C2mQ {
			uid: "AFTERBADCRC".into(),
			os: "MAC".into(),
		}),
	)
	.unwrap();
	attacker.send_to(&q, mid_addr).await.unwrap();
	let (r, _) = recv_one(&attacker).await;
	let (rtid, p) = decode_discovery(&r).unwrap();
	assert_eq!(rtid, 0x77);
	assert!(matches!(p, UdpXml::M2cQr(_)));

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

/// Oversized-UID regression: an inbound payload with a kilobyte-long
/// `<uid>...</uid>` element must be rejected by `decode_discovery` so
/// the registry / anchor maps can't be bloated by a hostile peer
/// stuffing oversized UIDs.
#[tokio::test]
async fn oversized_uid_in_d2m_q_is_rejected() {
	use bairelay_neolink_core::bcudp::xml::D2mQ;

	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let attacker = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let huge_uid: String = "A".repeat(2048);
	let pkt = encode_discovery(
		0x99,
		UdpXml::D2mQ(D2mQ {
			uid: huge_uid,
			revision: None,
		}),
	)
	.unwrap();
	attacker.send_to(&pkt, mid_addr).await.unwrap();

	// No reply within a short window — the server should drop the
	// oversized-UID packet at decode time.
	let mut buf = vec![0u8; 4096];
	let recv = tokio::time::timeout(Duration::from_millis(150), attacker.recv_from(&mut buf)).await;
	assert!(
		recv.is_err(),
		"server replied to oversized-UID D2M_Q (should have been dropped)",
	);

	// Listener still alive — a normal-sized UID still works.
	let normal = encode_discovery(
		0x9a,
		UdpXml::D2mQ(D2mQ {
			uid: "NORMAL".into(),
			revision: None,
		}),
	)
	.unwrap();
	attacker.send_to(&normal, mid_addr).await.unwrap();
	let (r, _) = recv_one(&attacker).await;
	let (_tid, p) = decode_discovery(&r).unwrap();
	assert!(matches!(p, UdpXml::M2dQr(_)));

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

/// Anchor-required regression: a `D2R_R` for a UID that has not gone
/// through `M2D_Q_R` first must be silently dropped — pre-fix the
/// server replied with a synthesised random `ac`, leaking session
/// state to anyone who could send a `D2R_R` to the wake port.
#[tokio::test]
async fn d2r_r_with_no_anchor_is_dropped() {
	use bairelay_neolink_core::bcudp::xml::D2rR;

	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let mid_addr = middleman.local_addr().unwrap();
	let reg_addr = register.local_addr().unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: mid_addr.port(),
		register_port: reg_addr.port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let server_handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));

	let attacker = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let pkt = encode_discovery(
		0x55,
		UdpXml::D2rR(D2rR {
			uid: "GHOST".into(),
			token: 0x1111_2222_3333_4444,
			revision: None,
		}),
	)
	.unwrap();
	attacker.send_to(&pkt, reg_addr).await.unwrap();

	let mut buf = vec![0u8; 4096];
	let recv = tokio::time::timeout(Duration::from_millis(150), attacker.recv_from(&mut buf)).await;
	assert!(
		recv.is_err(),
		"server replied to D2R_R for unanchored UID (should have been dropped)",
	);

	cancel.cancel();
	let _ = tokio::time::timeout(Duration::from_millis(500), server_handle).await;
}

#[tokio::test]
async fn cancel_returns_promptly() {
	let cancel = CancellationToken::new();
	let middleman = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let register = UdpSocket::bind((LOOPBACK, 0)).await.unwrap();
	let cfg = RuntimeConfig {
		bind: LOOPBACK,
		middleman_port: middleman.local_addr().unwrap().port(),
		register_port: register.local_addr().unwrap().port(),
		hb_advice_ms: 20000,
		stale_after_ms: 80000,
		declared: Default::default(),
	};
	let handle = tokio::spawn(bairelay_wake_server::run_with_sockets(
		cfg,
		bairelay_wake_server::make_registry(),
		middleman,
		register,
		cancel.clone(),
	));
	cancel.cancel();
	let res = tokio::time::timeout(Duration::from_millis(500), handle).await;
	let join = res.expect("did not exit within 500 ms");
	let inner = join.expect("task panicked");
	inner.expect("inner Result should be Ok");
}
