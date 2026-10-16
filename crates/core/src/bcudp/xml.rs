use serde::{Deserialize, Serialize};
use std::io::Write;

/// The top level of the UDP xml is P2P
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
#[serde(rename = "P2P")]
pub enum UdpXml {
	/// C2D_S xml Discovery of any client
	#[serde(rename = "C2D_S")]
	C2dS(C2dS),
	/// C2D_S xml Discovery of client with a UID
	#[serde(rename = "C2D_C")]
	C2dC(C2dC),
	/// D2C_C_C xml Reply from discovery
	#[serde(rename = "D2C_C_R")]
	D2cCr(D2cCr),
	/// D2C_T xml
	#[serde(rename = "D2C_T")]
	D2cT(D2cT),
	/// C2D_T xml
	#[serde(rename = "C2D_T")]
	C2dT(C2dT),
	/// D2C_CFM xml
	#[serde(rename = "D2C_CFM")]
	D2cCfm(D2cCfm),
	/// C2D_DISC xml Disconnect
	#[serde(rename = "C2D_DISC")]
	C2dDisc(C2dDisc),
	/// D2C_DISC xml Disconnect
	#[serde(rename = "D2C_DISC")]
	D2cDisc(D2cDisc),
	/// R2C_DISC xml Disconnect
	#[serde(rename = "R2C_DISC")]
	R2cDisc(R2cDisc),
	/// C2M_Q xml client to middle man query
	#[serde(rename = "C2M_Q")]
	C2mQ(C2mQ),
	/// M2C_Q_R xml middle man to client query reply
	#[serde(rename = "M2C_Q_R")]
	M2cQr(M2cQr),
	/// C2R_C xml client to register connect
	#[serde(rename = "C2R_C")]
	C2rC(C2rC),
	/// R2C_T xml register to clinet with device ID etc
	#[serde(rename = "R2C_T")]
	R2cT(R2cT),
	/// R2C_T xml register to clinet with device ID etc handled over dmap ONLY
	#[serde(rename = "R2C_C_R")]
	R2cCr(R2cCr),
	/// C2R_CFM xml client to register CFM
	#[serde(rename = "C2R_CFM")]
	C2rCfm(C2rCfm),
	/// C2D_A xml client to device accept
	#[serde(rename = "C2D_A")]
	C2dA(C2dA),
	/// C2D_HB xml client to device heartbeat. This is the keep alive
	#[serde(rename = "C2D_HB")]
	C2dHb(C2dHb),
	/// C2D_HB xml client to device heartbeat. This is the keep alive
	#[serde(rename = "C2R_HB")]
	C2rHb(C2rHb),
	/// D2D_HB xml client to device heartbeat. This is the keep alive
	#[serde(rename = "D2C_HB")]
	D2cHb(D2cHb),
	/// D2R_HB xml device-to-register heartbeat (battery camera lifecycle)
	#[serde(rename = "D2R_HB")]
	D2rHb(D2rHb),
	/// R2D_HB_R xml register-to-device heartbeat reply
	#[serde(rename = "R2D_HB_R")]
	R2dHbr(R2dHbr),
	/// R2D_C xml register-to-device wake packet
	#[serde(rename = "R2D_C")]
	R2dC(R2dC),
	/// D2R_C_R xml device-to-register wake ack
	#[serde(rename = "D2R_C_R")]
	D2rCr(D2rCr),
	/// D2R_DISC xml device-to-register disconnect stats
	#[serde(rename = "D2R_DISC")]
	D2rDisc(D2rDisc),
	/// R2D_DC_R xml register-to-device disconnect ack
	#[serde(rename = "R2D_DC_R")]
	R2dDcr(R2dDcr),
	/// D2M_Q xml device-to-middleman query:
	/// battery cameras send this on boot before the heartbeat handshake.
	/// Argus emits the long-form UID with a 4-char firmware suffix.
	#[serde(rename = "D2M_Q")]
	D2mQ(D2mQ),
	/// M2D_Q_R xml middleman-to-device query reply: tells the camera the
	/// register / relay / log / t addresses (mirrors `M2C_Q_R` for clients).
	#[serde(rename = "M2D_Q_R")]
	M2dQr(M2dQr),
	/// D2R_R xml device-to-register registration request: camera
	/// echoes the M2D_Q_R `<token>` back to the register port to
	/// anchor the session.
	#[serde(rename = "D2R_R")]
	D2rR(D2rR),
	/// R2D_R_R xml register-to-device registration ack: cloud observed
	/// to send `<rsp>-4</rsp>` (apparently informational, not fatal) +
	/// the same `<ac>` issued during `M2D_Q_R`. After this exchange the
	/// camera transitions to `D2R_HB` heartbeats.
	#[serde(rename = "R2D_R_R")]
	R2dRr(R2dRr),
}

/// The top level holder for P2P we auto add/remove this at serde
#[derive(PartialEq, Eq, Debug, Deserialize, Serialize, Clone)]
struct P2P {
	#[serde(rename = "$value")]
	xml: UdpXml,
}

impl UdpXml {
	pub(crate) fn try_parse(s: &[u8]) -> Result<Self, quick_xml::de::DeError> {
		crate::bc::xml::parse_lossy_utf8::<P2P>(s).map(|i| i.xml)
	}
	pub(crate) fn serialize<W: Write>(&self, mut w: W) -> Result<W, quick_xml::SeError> {
		let mut writer = quick_xml::writer::Writer::new(&mut w);
		// No header on a UdpXml
		// writer.write_event(quick_xml::events::Event::Decl(
		//     quick_xml::events::BytesDecl::new("1.0", Some("UTF-8"), None),
		// ))?;
		writer.create_element("P2P").write_inner_content(|writer| {
			writer
				.write_serializable("", &self)
				.map_err(std::io::Error::other)
		})?;

		Ok(w)
	}
}

/// C2D_S xml
///
/// The camera will send binary data to port 3000
/// to whoever it gets this message from
///
/// It should be broadcasted to port 2015
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dS {
	/// The destination to reply to
	pub to: PortList,
}

/// Port list xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct PortList {
	/// Port to open udp connections with
	pub port: u32,
}

/// C2D_C xml
///
/// This will start a connection with any camera that has this UID
/// It should be broadcasted to port 2018
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dC {
	/// UID of the camera the client wants to connect with
	pub uid: String,
	/// Cli contains the udp port to communicate on
	pub cli: ClientList,
	/// The cid is the client ID
	pub cid: i32,
	/// Maximum transmission size,
	pub mtu: u32,
	/// Debug mode. Purpose unknown
	pub debug: bool,
	/// Os of the machine known values are `"MAC"`, `"WIN"`
	#[serde(rename = "p")]
	pub os: String,
	/// Login/protocol version. `3` signals the sigV3 handshake — the camera
	/// then issues the login `nc` (nonce) in its `D2C_C_R` reply. Omitted
	/// (serialized as nothing via `skip`) for legacy connects.
	#[serde(default, skip_serializing_if = "is_zero_u32_xml")]
	pub lver: u32,
}

/// serde predicate: skip `lver` when 0 (legacy connect emits no `<lver>`).
fn is_zero_u32_xml(v: &u32) -> bool {
	*v == 0
}

/// Client List xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct ClientList {
	/// Port to start udp communication with
	pub port: u32,
}

/// D2C_C_R xml
///
/// This will start a connection with any camera that has this UID
/// It should be broadcasted to port 2018
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2cCr {
	/// Called timer but not sure what it is a timer of
	pub timer: Timer,
	/// Unknown seems to be 0 on success and -3 on fail
	pub rsp: i32,
	/// Client ID
	pub cid: i32,
	/// Camera ID
	pub did: i32,
	/// sigV3 payload line — the camera's ECDHE offer delivered in the P2P
	/// handshake (cloud-bound cameras): `V=1;C=...,P2=v3,P3=X25519,
	/// P4=<camera pubkey b64>,P5=<sign b64>,P6=<iterations>;`. Absent on
	/// non-sigV3 cameras.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pl: Option<String>,
	/// sigV3 login nonce delivered in the P2P handshake (cloud-bound
	/// cameras). The sigV3 login is keyed by THIS nonce, not by a Bc
	/// `Encryption`-reply nonce. Absent on non-sigV3 cameras.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub nc: Option<i64>,
}

/// Timer provided by D2C_C_R
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct Timer {
	/// Unknown
	def: u32,
	/// Unknown
	hb: u32,
	/// Unknown
	hbt: u32,
}

/// C2D_DISC xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dDisc {
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// D2C_DISC xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2cDisc {
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// R2C_DISC xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2cDisc {
	/// The sid
	pub sid: u32,
}

/// D2C_T xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2cT {
	/// The camera SID
	pub sid: u32,
	/// Type of connection observed values are `"local"` `"relay"`, `"map"`
	pub conn: String,
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// C2D_T xml
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dT {
	/// The camera SID
	pub sid: u32,
	/// Type of connection observed values are `"local"`
	pub conn: String,
	/// The client connection ID
	pub cid: i32,
	/// Maximum size in bytes of a transmission
	pub mtu: u32,
}

/// C2M_Q xml
///
/// This is from client to a reolink middle man server
///
/// It should be sent to a reolink p2p sever on port 9999
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2mQ {
	/// UID to look up
	pub uid: String,
	/// Os of the machine known values are `"MAC"`, `"WIN"`
	#[serde(rename = "p")]
	pub os: String,
}

/// D2M_Q xml
///
/// Battery camera asks the middleman where to register. Sent on boot
/// (before D2R_HB heartbeats). Argus firmware emits the long-form UID
/// (config UID + a 4-character firmware suffix). The `<r>` field is a
/// firmware revision (observed value: `2`).
///
/// surfaced this from a real Argus during live-verify; the
/// reference wake-server only handled `C2M_Q` (clients) and missed it.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2mQ {
	/// Long-form camera UID (config UID + 4-char firmware suffix).
	pub uid: String,
	/// Firmware revision; observed value is `2` on current Argus firmware.
	#[serde(rename = "r", skip_serializing_if = "Option::is_none")]
	pub revision: Option<i32>,
}

/// M2D_Q_R xml
///
/// Middleman reply to `D2M_Q`. Verified shape from a real Reolink cloud
/// capture:
///
/// ```xml
/// <P2P><M2D_Q_R>
///   <reg><ip>...</ip><port>58200</port></reg>
///   <log><ip>...</ip><port>57850</port></log>
///   <timer/>
///   <retry/>
///   <rsp>0</rsp>
///   <token>1773137273</token>
///   <ac>1130209852</ac>
/// </M2D_Q_R></P2P>
/// ```
///
/// Note: this is **not** the same shape as `M2C_Q_R` (the client reply).
/// Cameras get `reg` + `log` plus a session token / ac code; clients get
/// `reg`/`relay`/`log`/`t`. Operationally we put our register address in
/// both `reg` and `log` (we have no separate log server) and emit empty
/// `<timer/>` + `<retry/>` markers for byte-shape parity with the cloud.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct M2dQr {
	/// Register server address; `0` on success.
	pub reg: IpPort,
	/// Log / telemetry server address. Cameras send diagnostic blobs here
	/// (port 57850 in the real cloud). We point this at our register
	/// address so any reach-out lands somewhere known.
	pub log: IpPort,
	/// Empty `<timer/>` element; cloud emits self-closing.
	#[serde(default)]
	pub timer: EmptyTag,
	/// Empty `<retry/>` element; cloud emits self-closing.
	#[serde(default)]
	pub retry: EmptyTag,
	/// Response code; `0` on success.
	pub rsp: i32,
	/// Session token assigned by the cloud (and echoed in subsequent
	/// `D2R_HB` heartbeats from the camera).
	pub token: u64,
	/// Access code; purpose unknown but the cloud always emits one.
	pub ac: u32,
}

/// Marker struct for `<timer/>` / `<retry/>`-style empty self-closing
/// elements that Reolink uses as protocol stubs.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct EmptyTag {}

/// D2R_R xml — camera-to-register registration request.
///
/// Sent immediately after the camera receives `M2D_Q_R`. The `<token>`
/// is the value our `M2D_Q_R` issued; the camera echoes it back so the
/// register-port loop can correlate and reply. Verified against a real
/// Reolink cloud capture.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2rR {
	/// Long-form camera UID (matches the `<uid>` from `D2M_Q`).
	pub uid: String,
	/// Echoed `<token>` from the matching `M2D_Q_R` reply.
	pub token: u64,
	/// Firmware revision (same value the camera supplied in `D2M_Q`).
	#[serde(rename = "r", skip_serializing_if = "Option::is_none")]
	pub revision: Option<i32>,
}

/// R2D_R_R xml — register-to-device registration ack.
///
/// Verified shape from a real Reolink cloud capture: the cloud emits
/// `<rsp>-4</rsp>` and echoes the `<ac>` value it issued during the
/// preceding `M2D_Q_R`. Argus firmware accepts `rsp = -4` here as a
/// "registered, proceed to heartbeats" signal — it is not a fatal
/// error code.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2dRr {
	/// Response code; cloud sends `-4` (informational).
	pub rsp: i32,
	/// Echoed `<ac>` from the `M2D_Q_R` reply that bootstrapped this
	/// session. The camera anchors to this value.
	pub ac: u32,
}

/// M2C_Q_R xml
///
/// This is from middle man reolink server to client
///
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct M2cQr {
	/// The register server location
	#[serde(skip_serializing_if = "Option::is_none")]
	pub reg: Option<IpPort>,
	/// The relay server location
	#[serde(skip_serializing_if = "Option::is_none")]
	pub relay: Option<IpPort>,
	/// The log server location
	#[serde(skip_serializing_if = "Option::is_none")]
	pub log: Option<IpPort>,
	/// The camera location
	#[serde(skip_serializing_if = "Option::is_none")]
	pub t: Option<IpPort>,
}

/// Used as part of M2C_Q_R to provide the host and port
///
/// of the register, relay and log servers
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct IpPort {
	/// Ip of the service
	pub ip: String,
	/// Port of the service
	pub port: u16,
}

impl std::convert::TryFrom<IpPort> for std::net::SocketAddr {
	type Error = crate::Error;

	fn try_from(src: IpPort) -> Result<Self, Self::Error> {
		Ok(src
			.ip
			.parse::<std::net::IpAddr>()
			.map(|ip| std::net::SocketAddr::new(ip, src.port))?)
	}
}

/// C2R_C xml
///
/// This is from client to the register reolink server
///
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2rC {
	/// The UID to register connecition request with
	pub uid: String,
	/// The location of the client
	pub cli: IpPort,
	/// The location of the relay server
	pub relay: IpPort,
	/// The client id
	pub cid: i32,
	/// Debug setting. Unknown purpose observed values are `0`
	pub debug: bool,
	/// Inet family. Observed values `4`
	pub family: u8,
	/// Os of the machine known values are `"MAC"`, `"WIN"`
	#[serde(rename = "p")]
	pub os: String,
	/// The revision. Known values None and 3
	#[serde(rename = "r", skip_serializing_if = "Option::is_none")]
	pub revision: Option<i32>,
}

/// R2C_T xml
///
/// This is from register reolink server to clinet with device ip and did etc
///
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2cT {
	/// The location of the camera
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dmap: Option<IpPort>,
	/// The location of the camera
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dev: Option<IpPort>,
	/// The client id
	pub cid: i32,
	/// The camera SID
	pub sid: u32,
}

/// R2C_C_R xml
///
/// This is from register reolink server to clinet with device ip and did etc
/// during a relay
///
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2cCr {
	/// Dev camera location (actual local ip)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dev: Option<IpPort>,
	/// Dmap camera location
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dmap: Option<IpPort>,
	/// The location of the relay
	#[serde(skip_serializing_if = "Option::is_none")]
	pub relay: Option<IpPort>,
	/// The location of the relayt (not sure what the t is for)
	#[serde(skip_serializing_if = "Option::is_none")]
	pub relayt: Option<IpPort>,
	/// The nat type. Known values `"NULL"`
	pub nat: String,
	/// The camera SID, missing when rsp is `-3`
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sid: Option<u32>,
	/// rsp. Known values `0`, `-3, seems to be 0 on success and -3 on fail`
	pub rsp: i32,
	/// ac. Known values. `127536491`
	pub ac: u32,
}

/// D2C_CFM xml
///
/// Device to client, with connection started from middle man server
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2cCfm {
	/// The camera SID
	pub sid: u32,
	/// Type of connection observed values are `"local"`
	pub conn: String,
	/// Unknown known values are `0`, `-3, seems to be 0 on success and -3 on fail`
	pub rsp: i32,
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
	/// The time but only value that has been observed is `0
	pub time_r: Option<u32>,
}

/// C2R_CFM xml
///
/// Client to register
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2rCfm {
	/// The camera SID
	pub sid: u32,
	/// Type of connection observed values are `"local"`
	pub conn: String,
	/// Unknown known values are  `0`, `-3, seems to be 0 on success and -3 on fail`
	pub rsp: i32,
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// C2D_A xml
///
/// Client to device accept.
/// Sent it reply to a D2C_T
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dA {
	/// The camera SID
	pub sid: u32,
	/// Type of connection observed values are `"local"`
	pub conn: String,
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
	/// Maximum size in bytes of a transmission
	pub mtu: u32,
}

/// C2D_HB xml
///
/// Client to device heart beat.
/// Seems to act as a keep alive
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2dHb {
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// C2R_HB xml
///
/// Client to device heart beat.
/// Seems to act as a keep alive
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct C2rHb {
	/// The connection ID
	pub sid: u32,
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

/// D2C_HB xml
///
/// Device to client heart beat.
/// Seems to act as a keep alive
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2cHb {
	/// The client connection ID
	pub cid: i32,
	/// The camera connection ID
	pub did: i32,
}

#[test]
fn ip_port_try_into_socket_addr_ok_ipv4() {
	use std::convert::TryFrom;
	let ip = IpPort {
		ip: "10.0.0.1".into(),
		port: 9999,
	};
	let addr = std::net::SocketAddr::try_from(ip).expect("ipv4 parses");
	assert_eq!(addr.port(), 9999);
	assert_eq!(addr.ip().to_string(), "10.0.0.1");
}

#[test]
fn ip_port_try_into_socket_addr_ok_ipv6() {
	use std::convert::TryFrom;
	let ip = IpPort {
		ip: "::1".into(),
		port: 5000,
	};
	let addr = std::net::SocketAddr::try_from(ip).expect("ipv6 parses");
	assert_eq!(addr.port(), 5000);
}

#[test]
fn ip_port_try_into_socket_addr_err_on_bad_ip() {
	use std::convert::TryFrom;
	let ip = IpPort {
		ip: "not-an-ip".into(),
		port: 1,
	};
	let result = std::net::SocketAddr::try_from(ip);
	assert!(result.is_err(), "invalid IP should fail to parse");
}

/// D2R_HB xml — Camera-to-Register heartbeat
///
/// Battery cameras emit this every ~20 seconds (cadence echoed by the
/// register server in `R2D_HB_R`). The short form omits `<dev>`.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2rHb {
	/// Camera's unique identifier.
	pub uid: String,
	/// Camera's local LAN address as it sees itself; absent on the short form.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dev: Option<IpPort>,
	/// `Some(1)` when the camera wants an `R2D_HB_R` response; absent otherwise.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub needrsp: Option<u8>,
	/// Opaque session token; persists across heartbeats. Stored as `u64`
	/// to match the reference and accommodate any future widening.
	pub token: u64,
}

/// `<timer>` element nested inside `R2D_HB_R`.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct HbTimer {
	/// Heartbeat interval in milliseconds (20000 = 20 s on Argus).
	pub hb: u32,
}

/// R2D_HB_R xml — Register-to-Device heartbeat reply.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2dHbr {
	/// `0` on success.
	pub rsp: i32,
	/// Server's unix-epoch timestamp.
	pub time_t: u64,
	/// Cadence the camera should follow.
	pub timer: HbTimer,
}

/// R2D_C xml — Register-to-Device wake packet (sent 10× at 100 ms by the server).
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2dC {
	/// Client's local (LAN) address.
	pub cli: IpPort,
	/// Client's NAT-mapped (public) address as the server observed it.
	pub cmap: IpPort,
	/// Relay address for fallback (we send our own register address).
	pub relay: IpPort,
	/// Session ID minted by the server.
	pub sid: u32,
	/// Client ID echoed back from `C2R_C`.
	pub cid: i32,
}

/// D2R_C_R xml — camera's wake acknowledgement.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2rCr {
	/// Echoes the `sid` from `R2D_C`.
	pub sid: u32,
	/// Camera's address; optional because firmwares vary.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub dev: Option<IpPort>,
	/// `0` on success.
	pub rsp: i32,
}

/// D2R_DISC xml — camera-side connection statistics after a session ends.
///
/// Cameras pack `<time>`, `<send>`, `<recv>` and similar diagnostics here.
/// We only need `sid` to acknowledge; everything else is discarded by
/// quick-xml because the struct does not name those fields.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct D2rDisc {
	/// Session ID being torn down.
	pub sid: u32,
}

/// R2D_DC_R xml — server's ack of a `D2R_DISC` payload.
#[derive(PartialEq, Eq, Default, Debug, Deserialize, Serialize, Clone)]
pub struct R2dDcr {
	/// Echoed session ID from `D2R_DISC`.
	pub sid: u32,
	/// `0` on success.
	pub rsp: i32,
}

/// This test was added because of issues with Argus3 during discovery
#[test]
fn test_d2c_c_r_deser() {
	let sample = indoc::indoc!(
		r#"
        <P2P>
        <D2C_C_R>
        <timer>
        <def>3000</def>
        <hb>20000</hb>
        <hbt>60000</hbt>
        </timer>
        <rsp>0</rsp>
        <cid>-376737975</cid>
        <did>49</did>
        </D2C_C_R>
        </P2P>
        "#
	);
	let b: UdpXml = UdpXml::try_parse(sample.as_bytes()).unwrap();

	assert_matches::assert_matches!(
		b,
		UdpXml::D2cCr(D2cCr {
			timer: Timer {
				def: 3000,
				hb: 20000,
				hbt: 60000,
			},
			rsp: 0,
			cid: -376737975,
			did: 49,
			pl: None,
			nc: None,
		})
	);
}

#[test]
fn test_d2r_hb_long_form() {
	let xml = indoc::indoc!(
		r#"
        <P2P>
        <D2R_HB>
        <uid>9527000TESTCAMERA</uid>
        <dev>
        <ip>10.0.0.91</ip>
        <port>10177</port>
        </dev>
        <needrsp>1</needrsp>
        <token>1770434238</token>
        </D2R_HB>
        </P2P>
    "#
	);
	let parsed = UdpXml::try_parse(xml.as_bytes()).expect("parse");
	assert_matches::assert_matches!(parsed,
		UdpXml::D2rHb(D2rHb { ref uid, dev: Some(ref d), needrsp: Some(1), token: 1770434238 })
		if uid == "9527000TESTCAMERA" && d.ip == "10.0.0.91" && d.port == 10177
	);
}

#[test]
fn test_d2r_hb_short_form() {
	let xml =
		b"<P2P><D2R_HB><uid>9527000TEST</uid><token>123</token><needrsp>1</needrsp></D2R_HB></P2P>";
	let parsed = UdpXml::try_parse(&xml[..]).expect("parse");
	assert_matches::assert_matches!(parsed,
		UdpXml::D2rHb(D2rHb { ref uid, dev: None, needrsp: Some(1), token: 123 })
		if uid == "9527000TEST"
	);
}

#[test]
fn test_r2d_hb_r_serialize() {
	let msg = UdpXml::R2dHbr(R2dHbr {
		rsp: 0,
		time_t: 1772993748,
		timer: HbTimer { hb: 20000 },
	});
	let buf = msg.serialize(Vec::new()).expect("serialize");
	let s = String::from_utf8(buf).expect("utf8");
	assert!(s.contains("<R2D_HB_R>"), "got: {s}");
	assert!(s.contains("<hb>20000</hb>"), "got: {s}");
	assert!(s.contains("<time_t>1772993748</time_t>"), "got: {s}");
}

#[test]
fn test_r2d_hb_r_roundtrip() {
	let msg = UdpXml::R2dHbr(R2dHbr {
		rsp: 0,
		time_t: 1,
		timer: HbTimer { hb: 20000 },
	});
	let buf = msg.serialize(Vec::new()).expect("serialize");
	let parsed = UdpXml::try_parse(buf.as_slice()).expect("parse");
	assert_eq!(parsed, msg);
}

#[test]
fn test_r2d_c_roundtrip() {
	let msg = UdpXml::R2dC(R2dC {
		cli: IpPort {
			ip: "10.0.0.170".into(),
			port: 10739,
		},
		cmap: IpPort {
			ip: "192.0.2.35".into(),
			port: 10739,
		},
		relay: IpPort {
			ip: "10.0.0.1".into(),
			port: 58200,
		},
		sid: 95196080,
		cid: 330001,
	});
	let buf = msg.serialize(Vec::new()).expect("serialize");
	let parsed = UdpXml::try_parse(buf.as_slice()).expect("parse");
	assert_eq!(parsed, msg);
}

#[test]
fn test_d2r_c_r_parse() {
	let xml = indoc::indoc!(
		r#"
        <P2P>
        <D2R_C_R>
        <sid>95196080</sid>
        <dev>
        <ip>10.0.0.91</ip>
        <port>10177</port>
        </dev>
        <rsp>0</rsp>
        </D2R_C_R>
        </P2P>
    "#
	);
	let parsed = UdpXml::try_parse(xml.as_bytes()).expect("parse");
	assert_matches::assert_matches!(
		parsed,
		UdpXml::D2rCr(D2rCr {
			sid: 95196080,
			dev: Some(_),
			rsp: 0
		})
	);
}

#[test]
fn test_d2r_disc_parse_with_stats() {
	let xml = r#"<P2P><D2R_DISC><sid>95196080</sid><time><query>0</query><setup>0</setup><conn>7493</conn></time><conn>local</conn><rsp>0</rsp><did>743</did><send><spd>0</spd><cnt>0</cnt><cntr>0</cntr><size>0</size><sizer>0</sizer><lc>0</lc><rc>0</rc><sizew>0</sizew><itvl>0</itvl></send><recv><spd>0</spd><cnt>0</cnt><cntr>0</cntr><size>0</size><sizer>0</sizer><lc>0</lc><rc>0</rc><sizew>0</sizew><itvl>0</itvl></recv></D2R_DISC></P2P>"#;
	let parsed = UdpXml::try_parse(xml.as_bytes()).expect("parse");
	assert_matches::assert_matches!(parsed, UdpXml::D2rDisc(D2rDisc { sid: 95196080 }));
}

#[test]
fn test_r2d_dc_r_roundtrip() {
	let msg = UdpXml::R2dDcr(R2dDcr {
		sid: 95196080,
		rsp: 0,
	});
	let buf = msg.serialize(Vec::new()).expect("serialize");
	let parsed = UdpXml::try_parse(buf.as_slice()).expect("parse");
	assert_eq!(parsed, msg);
}

/// Pin our `M2D_Q_R` serialiser against the literal element shape a real
/// Reolink cloud emits (verified against a captured live pcap during
/// live-verify). Field order, element shape, and self-closing
/// `<timer/>` / `<retry/>` markers must all match or cameras silently
/// re-query in a 20 s loop without ever proceeding to `D2R_HB`.
#[test]
fn m2dqr_serialises_byte_for_byte_with_real_cloud() {
	// Use TEST-NET-3 (RFC 5737) addresses + obviously-fictional values so
	// no real camera or cloud-host details land in source.
	let r = UdpXml::M2dQr(M2dQr {
		reg: IpPort {
			ip: "203.0.113.140".into(),
			port: 58200,
		},
		log: IpPort {
			ip: "203.0.113.140".into(),
			port: 57850,
		},
		timer: EmptyTag::default(),
		retry: EmptyTag::default(),
		rsp: 0,
		token: 1,
		ac: 2,
	});
	let buf = r.serialize(Vec::new()).expect("ok");
	let cloud_shape = "<P2P><M2D_Q_R><reg><ip>203.0.113.140</ip><port>58200</port></reg><log><ip>203.0.113.140</ip><port>57850</port></log><timer/><retry/><rsp>0</rsp><token>1</token><ac>2</ac></M2D_Q_R></P2P>";
	assert_eq!(String::from_utf8(buf).unwrap(), cloud_shape);
}

#[test]
fn d2mq_parses_real_cloud_request() {
	// Argus battery cameras send this on boot; UID is the long form
	// (config UID + 4-char firmware suffix). Test value here is
	// fictional.
	let xml = "<P2P><D2M_Q><uid>9527000TESTCAM00ABCD</uid><r>2</r></D2M_Q></P2P>";
	let parsed = UdpXml::try_parse(xml.as_bytes()).expect("parse");
	assert_matches::assert_matches!(parsed,
		UdpXml::D2mQ(D2mQ { ref uid, revision: Some(2) })
		if uid == "9527000TESTCAM00ABCD"
	);
}
