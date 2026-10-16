//! HTTP listener for the camera's HaCfg webhook.
//!
//! A camera with `webhook = true` gets a random per-process token, and
//! its armed HaCfg webhook POSTs JSON to `/hook/<token>` on this
//! listener: a wake reason while it sleeps on battery, or a forwarded
//! cmd 33 alarm on wired power. A request is accepted only on a known
//! token from that camera's own IP, either the address bairelay last
//! reached it on or its configured literal IP.
//!
//! - `wake` + `doorbell`: `status/doorbell` `press`, plus a wake-lock
//!   hold so the connect loop reaches the camera while it is awake.
//! - `wake` + `pir`: a motion edge, the same as a push-listener edge.
//! - any other `wake`: a wake-lock hold.
//! - `sleep`, `test`, or a well-formed body naming nothing known:
//!   logged only.
//! - a forwarded alarm: classified with a per-camera press edge that
//!   sees every alarm. While the camera has a live session, whose own
//!   cmd 33 stream is authoritative, nothing is published. Otherwise
//!   motion, AI objects and presses are published as the session
//!   publishes them; a motion start also takes the motion edge's hold
//!   and fallback `off`, and a press a wake-lock hold.
//!
//! A `doorbell` or `pir` wake and a push-listener edge less than
//! [`crate::status_cache::WAKE_DEDUPE_WINDOW`] apart are one wake: the
//! first report takes the wake-lock hold (its own listener's length),
//! and a PIR wake or push edge its motion `on`; the second adds
//! neither. A `doorbell` wake still publishes its press. Any other
//! `wake` stands apart: it takes its own hold, and a push edge beside
//! it is a full motion edge.
//!
//! Presses from every source share one dedupe window
//! ([`crate::status_cache::DOORBELL_PRESS_DEDUPE_WINDOW`]). A camera
//! with `[cameras.mqtt] enable_motion = false` gets its wake-lock holds
//! but no motion, AI or press publishes.
//!
//! A connection from a peer that is no opted-in camera's recorded or
//! configured IP is closed at accept. Each other connection carries one
//! request: at most [`MAX_CONNECTIONS`] are served at once,
//! [`MAX_CONNECTIONS_PER_PEER`] of them from one peer IP; each has
//! [`READ_DEADLINE`] to deliver its request,
//! and gets one fixed response with `Connection: close`. A response
//! never carries request data.
//!
//! Wake-lock holds last `[webhook] motion_wake_hold_secs`.
//!
//! [`WebhookTarget`] builds the URL each session arms its camera with
//! (`camera_tasks::arm_webhook`).

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use bairelay_mqtt::{SharedMqttClient, StatusPublisher};
use bairelay_neolink_core::bc::xml::AlarmEvent;
use bairelay_neolink_core::bc_protocol::{classify_alarm_events, MotionStatus, VisitorEdge};
use bairelay_neolink_core::ha_webhook::{
	alarm_events_from_xml, parse_http_request, parse_webhook_body, HttpRequestError, WakeReason,
	WebhookEvent, HEADER_END, MAX_BODY_BYTES, MAX_HEADER_BYTES, TOKEN_HEX_LEN,
};
use bairelay_wake_server::route::advertise_ip;

use crate::camera::CameraHandle;
use crate::camera_tasks::{report_alarm_update, report_doorbell_press, AiBaseline, AlarmReport};
use crate::config::{parse_webhook_advertise, WebhookAdvertise, WebhookConfig};
use crate::status_cache::WakeSource;
use crate::stream_source::MutexPoisonRecover as _;
use crate::wake_events::{claim_wake, event_mqtt, motion_edge, motion_hold, spawn_wake_hold};

/// Most connections served at once; a connection over the cap is
/// closed unserved.
pub const MAX_CONNECTIONS: usize = 16;
/// Most connections served at once from one peer IP, so one client
/// can't hold every slot.
pub const MAX_CONNECTIONS_PER_PEER: usize = 4;
/// Time a client has to deliver one whole request.
pub const READ_DEADLINE: Duration = Duration::from_secs(10);
/// Upper bound on writing the response and closing the socket.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// Upper bound on acting on one accepted event.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause after a failed `accept`, so a persistent error (e.g. no free
/// file descriptors) can't spin the loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
/// Bytes read from the socket per call.
const READ_CHUNK: usize = 4096;
/// Request path prefix; the per-camera token
/// ([`random_hex_token`](bairelay_neolink_core::ha_webhook::random_hex_token))
/// follows it.
pub(crate) const HOOK_PATH_PREFIX: &str = "/hook/";

/// The request path a camera POSTs to: `/hook/<token>`.
pub fn hook_path(token: &str) -> String {
	format!("{HOOK_PATH_PREFIX}{token}")
}

/// Whether `path` is exactly a hook path: `/hook/` and a token of
/// [`TOKEN_HEX_LEN`] lowercase hex digits.
pub(crate) fn is_hook_path(path: &str) -> bool {
	path.strip_prefix(HOOK_PATH_PREFIX).is_some_and(|token| {
		token.len() == TOKEN_HEX_LEN
			&& token
				.bytes()
				.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
	})
}

/// Where cameras reach the listener, resolved once from `[webhook]`:
/// the `webhook_advertise` override when set, else the bind address;
/// and how long one webhook event holds a camera's wake lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookTarget {
	advertise: Option<WebhookAdvertise>,
	bind_ip: IpAddr,
	port: u16,
	hold: Duration,
}

impl WebhookTarget {
	/// Resolve the target from the `[webhook]` block and the top-level
	/// `bind`, the same way `main.rs` resolves the listener's bind IP.
	pub fn from_config(cfg: &WebhookConfig, top_level_bind: &str) -> anyhow::Result<Self> {
		let bind_ip = crate::run_support::resolve_bind_ip(
			"webhook_addr",
			cfg.webhook_addr.as_deref(),
			top_level_bind,
		)?;
		let advertise = cfg
			.webhook_advertise
			.as_deref()
			.map(|raw| {
				parse_webhook_advertise(raw)
					.map_err(|e| anyhow::anyhow!("Invalid webhook_advertise {raw}: {e}"))
			})
			.transpose()?;
		let hold = Duration::try_from_secs_f64(cfg.motion_wake_hold_secs).map_err(|e| {
			anyhow::anyhow!(
				"Invalid [webhook] motion_wake_hold_secs {}: {e}",
				cfg.motion_wake_hold_secs
			)
		})?;
		Ok(Self {
			advertise,
			bind_ip,
			port: cfg.webhook_port,
			hold,
		})
	}

	/// `[webhook] motion_wake_hold_secs`: how long one webhook event
	/// holds the camera's wake lock.
	pub fn hold(&self) -> Duration {
		self.hold
	}

	/// The URL a camera at `camera_ip` POSTs to with `token`:
	/// `http://<host>:<port>/hook/<token>`, or why there is none.
	pub fn url(&self, camera_ip: Option<IpAddr>, token: &str) -> Result<String, NoWebhookHost> {
		let authority = match &self.advertise {
			Some(advertise) => advertise.to_string(),
			None => {
				let camera_ip = camera_ip.map(|ip| ip.to_canonical());
				let host = match camera_ip {
					Some(camera) => advertise_ip(self.bind_ip, SocketAddr::new(camera, self.port)),
					None if !self.bind_ip.is_unspecified() => self.bind_ip,
					None => return Err(NoWebhookHost::UnknownCamera),
				};
				// A failed route probe falls back to loopback (and stays
				// cached), as does a loopback bind: no use to a camera
				// elsewhere, and writing it would replace a working URL.
				if host.is_loopback() && !camera_ip.is_some_and(|ip| ip.is_loopback()) {
					return Err(NoWebhookHost::LoopbackOnly);
				}
				SocketAddr::new(host, self.port).to_string()
			}
		};
		Ok(format!("http://{authority}{}", hook_path(token)))
	}
}

/// Why [`WebhookTarget::url`] has no URL for a camera.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoWebhookHost {
	/// The listener binds a wildcard and the camera's IP is unknown.
	UnknownCamera,
	/// The only host is loopback, which a camera off this host can't
	/// reach.
	LoopbackOnly,
}

impl std::fmt::Display for NoWebhookHost {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(match self {
			Self::UnknownCamera => {
				"the camera's address is unknown and the listener binds a wildcard; set [webhook] webhook_advertise or webhook_addr"
			}
			Self::LoopbackOnly => {
				"the only listener address is loopback, which the camera can't reach; set [webhook] webhook_advertise or webhook_addr"
			}
		})
	}
}

/// The listener's answer to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
	Ok,
	BadRequest,
	Forbidden,
	NotFound,
	MethodNotAllowed,
	PayloadTooLarge,
}

impl Status {
	/// The whole fixed response for this status.
	fn response(self) -> &'static str {
		macro_rules! response {
			($line:literal $(, $extra:literal)?) => {
				concat!(
					"HTTP/1.1 ", $line, "\r\n",
					$($extra, "\r\n",)?
					"Connection: close\r\nContent-Length: 0\r\n\r\n"
				)
			};
		}
		match self {
			Status::Ok => response!("200 OK"),
			Status::BadRequest => response!("400 Bad Request"),
			Status::Forbidden => response!("403 Forbidden"),
			Status::NotFound => response!("404 Not Found"),
			Status::MethodNotAllowed => response!("405 Method Not Allowed", "Allow: POST"),
			Status::PayloadTooLarge => response!("413 Payload Too Large"),
		}
	}
}

impl From<HttpRequestError> for Status {
	fn from(e: HttpRequestError) -> Self {
		match e {
			HttpRequestError::HeaderTooLarge | HttpRequestError::BodyTooLarge => {
				Status::PayloadTooLarge
			}
			_ => Status::BadRequest,
		}
	}
}

/// What an accepted request asks for.
enum Action {
	Wake(WakeReason),
	Sleep,
	Test,
	Alarm(Vec<AlarmEvent>),
	/// A well-formed body naming no event or command bairelay acts on.
	Unrecognised,
}

/// Per-camera state of the forwarded-alarm stream: press detection and
/// what it has published. AI objects diff against the status cache,
/// which holds what the last session or alarm published.
struct AlarmStream {
	edge: VisitorEdge,
	report: AlarmReport,
}

impl Default for AlarmStream {
	fn default() -> Self {
		Self {
			edge: VisitorEdge::default(),
			report: AlarmReport {
				ai_baseline: AiBaseline::Cache,
				..AlarmReport::default()
			},
		}
	}
}

/// State every connection shares.
struct Shared {
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	alarms: HashMap<String, Mutex<AlarmStream>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	/// How long one event holds the camera's wake lock.
	hold: Duration,
}

impl Shared {
	fn new(
		cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
		mqtt: Option<SharedMqttClient>,
		topic_prefix: String,
		hold: Duration,
	) -> Self {
		let alarms = cameras
			.values()
			.filter(|h| h.webhook_token().is_some())
			.map(|h| (h.name().to_string(), Mutex::default()))
			.collect();
		Self {
			cameras,
			alarms,
			mqtt,
			topic_prefix,
			hold,
		}
	}

	/// Whether `peer` is an opted-in camera's recorded or configured IP;
	/// any other peer is closed at accept.
	fn is_known_peer(&self, peer: IpAddr) -> bool {
		let peer = peer.to_canonical();
		self.cameras
			.values()
			.any(|h| h.webhook_token().is_some() && is_camera_peer(h, peer))
	}

	/// The opted-in camera whose token is `token`.
	fn camera_for_token(&self, token: &str) -> Option<&Arc<CameraHandle>> {
		self.cameras.values().find(|h| {
			h.webhook_token()
				.is_some_and(|t| ct_eq(t.as_bytes(), token.as_bytes()))
		})
	}
}

/// Connection admission: at most [`MAX_CONNECTIONS`] in all and
/// [`MAX_CONNECTIONS_PER_PEER`] per peer IP.
struct Admission {
	slots: Arc<Semaphore>,
	per_peer: Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

/// One admitted connection; dropping it frees its slots.
struct Admitted {
	_slot: OwnedSemaphorePermit,
	peer: IpAddr,
	per_peer: Arc<std::sync::Mutex<HashMap<IpAddr, usize>>>,
}

impl Admission {
	fn new() -> Self {
		Self {
			slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
			per_peer: Arc::default(),
		}
	}

	/// Admit a connection from `peer`, or `None` when a cap is reached.
	/// The total slot is taken first and the per-peer count only grows
	/// on admission, so a rejected peer never leaves a map entry; a slot
	/// taken for a peer at its cap is freed on return.
	fn admit(&self, peer: IpAddr) -> Option<Admitted> {
		let peer = peer.to_canonical();
		let slot = Arc::clone(&self.slots).try_acquire_owned().ok()?;
		let mut per_peer = self.per_peer.lock_recover();
		let count = per_peer.get(&peer).copied().unwrap_or(0);
		if count >= MAX_CONNECTIONS_PER_PEER {
			return None;
		}
		per_peer.insert(peer, count + 1);
		Some(Admitted {
			_slot: slot,
			peer,
			per_peer: Arc::clone(&self.per_peer),
		})
	}
}

impl Drop for Admitted {
	fn drop(&mut self) {
		let mut per_peer = self.per_peer.lock_recover();
		if let Some(count) = per_peer.get_mut(&self.peer) {
			*count -= 1;
			if *count == 0 {
				per_peer.remove(&self.peer);
			}
		}
	}
}

/// Equality whose time depends only on the lengths.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
	std::hint::black_box(diff) == 0
}

/// Whether `peer` is the camera: the address bairelay last reached it
/// on, or its configured literal IP. Neither known matches nothing.
fn is_camera_peer(handle: &CameraHandle, peer: IpAddr) -> bool {
	[handle.camera_ip(), handle.configured_ip()]
		.into_iter()
		.flatten()
		.any(|ip| ip.to_canonical() == peer)
}

/// Serve the webhook on an already-bound listener until `cancel` fires.
/// The binary binds in `main.rs` so a bind error halts startup before
/// any "started" log line. Cancel closes every open connection. `hold`
/// is how long one event holds a camera's wake lock.
pub async fn run_with_listener(
	listener: TcpListener,
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	hold: Duration,
	cancel: CancellationToken,
) {
	let shared = Arc::new(Shared::new(cameras, mqtt, topic_prefix, hold));
	let admission = Admission::new();
	// Dropping the set on return aborts every connection still open.
	let mut connections = JoinSet::new();

	loop {
		tokio::select! {
			_ = cancel.cancelled() => {
				tracing::debug!("Webhook listener cancelled");
				return;
			}
			Some(_) = connections.join_next(), if !connections.is_empty() => {}
			accept = listener.accept() => match accept {
				Ok((sock, peer)) => {
					if !shared.is_known_peer(peer.ip()) {
						tracing::debug!(peer_ip = %peer.ip().to_canonical(), "webhook connection from no opted-in camera's address; closing");
						continue;
					}
					let Some(admitted) = admission.admit(peer.ip()) else {
						tracing::debug!(peer_ip = %peer.ip(), "webhook connection cap reached; closing");
						continue;
					};
					let shared = Arc::clone(&shared);
					let cancel = cancel.clone();
					connections.spawn(async move {
						let _admitted = admitted;
						serve(sock, peer.ip(), &shared, &cancel).await;
					});
				}
				Err(e) => {
					tracing::warn!(error = %e, "webhook listener accept failed");
					if !crate::run_support::sleep_or_cancel(ACCEPT_ERROR_BACKOFF, &cancel).await {
						return;
					}
				}
			},
		}
	}
}

/// Serve one connection from `peer`: read one request within
/// [`READ_DEADLINE`], answer it, close, then act on an accepted event.
async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
	mut sock: S,
	peer: IpAddr,
	shared: &Shared,
	cancel: &CancellationToken,
) {
	let peer_ip = peer.to_canonical();
	let request = match tokio::time::timeout(READ_DEADLINE, read_request(&mut sock)).await {
		Ok(request) => request,
		Err(_) => {
			tracing::debug!(peer_ip = %peer_ip, "webhook request incomplete at the read deadline");
			return;
		}
	};
	let routed = request.and_then(|buf| route(&buf, peer_ip, shared));
	let status = match &routed {
		Ok(_) => Status::Ok,
		Err(status) => {
			tracing::debug!(peer_ip = %peer_ip, ?status, "webhook request rejected");
			*status
		}
	};
	let _ = tokio::time::timeout(WRITE_TIMEOUT, async {
		sock.write_all(status.response().as_bytes()).await?;
		sock.shutdown().await
	})
	.await;
	drop(sock);

	if let Ok((handle, action)) = routed {
		tokio::select! {
			_ = cancel.cancelled() => {}
			_ = tokio::time::timeout(DISPATCH_TIMEOUT, dispatch(handle, action, shared, cancel)) => {}
		}
	}
}

/// Read until the bytes hold one complete request or a reason to
/// reject it. The buffer stays within the parser's header and body
/// limits plus one read; bytes after a complete request are ignored.
async fn read_request<S: AsyncRead + Unpin>(sock: &mut S) -> Result<Vec<u8>, Status> {
	let mut request = RequestBuf::default();
	let mut chunk = [0u8; READ_CHUNK];
	loop {
		let n = sock
			.read(&mut chunk)
			.await
			.map_err(|_| Status::BadRequest)?;
		if n == 0 {
			return Err(Status::BadRequest);
		}
		if let Some(done) = request.push(&chunk[..n]) {
			return done.map(|()| request.buf);
		}
	}
}

/// The bytes of one request as they arrive.
#[derive(Debug, Default)]
struct RequestBuf {
	buf: Vec<u8>,
	/// The whole request's length, known once its header block parses;
	/// the parser runs again only when that many bytes are buffered.
	/// Before that it runs only when a read may have completed the
	/// header block or the block reaches its limit.
	needed: Option<usize>,
	/// Parser runs, for tests.
	#[cfg(test)]
	parses: usize,
}

impl RequestBuf {
	/// Append `chunk`: `None` while more bytes are needed, else `Ok`
	/// for a complete request or the status that rejects it.
	fn push(&mut self, chunk: &[u8]) -> Option<Result<(), Status>> {
		// The new bytes, plus the three before them in case the blank
		// line straddles two reads.
		let fresh = self.buf.len().saturating_sub(HEADER_END.len() - 1);
		self.buf.extend_from_slice(chunk);
		let ready = match self.needed {
			Some(n) => self.buf.len() >= n,
			None => {
				self.buf.len() >= MAX_HEADER_BYTES
					|| self.buf[fresh..]
						.windows(HEADER_END.len())
						.any(|w| w == HEADER_END)
			}
		};
		if !ready {
			return None;
		}
		#[cfg(test)]
		{
			self.parses += 1;
		}
		match parse_http_request(&self.buf) {
			Ok(_) => Some(Ok(())),
			// The parser rejects an oversize header block or body
			// itself; this ceiling is the backstop.
			Err(HttpRequestError::Incomplete { .. })
				if self.buf.len() > MAX_HEADER_BYTES + MAX_BODY_BYTES =>
			{
				Some(Err(Status::PayloadTooLarge))
			}
			Err(HttpRequestError::Incomplete { needed }) => {
				self.needed = needed;
				None
			}
			Err(e) => Some(Err(e.into())),
		}
	}
}

/// Check one complete request against the method, the token, the
/// camera's address and the body, in that order.
fn route(
	buf: &[u8],
	peer_ip: IpAddr,
	shared: &Shared,
) -> Result<(Arc<CameraHandle>, Action), Status> {
	let request = parse_http_request(buf)?;
	if request.method != "POST" {
		return Err(Status::MethodNotAllowed);
	}
	let handle = request
		.path
		.strip_prefix(HOOK_PATH_PREFIX)
		.and_then(|token| shared.camera_for_token(token))
		.ok_or(Status::NotFound)?;
	if !is_camera_peer(handle, peer_ip) {
		tracing::debug!(camera = %handle.name(), peer_ip = %peer_ip, "webhook POST from an address other than the camera's");
		return Err(Status::Forbidden);
	}
	let event = parse_webhook_body(request.body).map_err(|_| Status::BadRequest)?;
	let action = match event {
		Some(WebhookEvent::Wake { reason }) => Action::Wake(reason),
		Some(WebhookEvent::Sleep) => Action::Sleep,
		Some(WebhookEvent::Test) => Action::Test,
		Some(WebhookEvent::Alarm { xml }) => {
			Action::Alarm(alarm_events_from_xml(&xml).ok_or(Status::BadRequest)?)
		}
		None => Action::Unrecognised,
	};
	Ok((Arc::clone(handle), action))
}

/// Act on one accepted event; see the module docs for the mapping.
async fn dispatch(
	handle: Arc<CameraHandle>,
	action: Action,
	shared: &Shared,
	cancel: &CancellationToken,
) {
	let hold = shared.hold;
	let mqtt = event_mqtt(&handle, shared.mqtt.as_ref());
	let publisher = mqtt.map(|m| StatusPublisher::new(m, &shared.topic_prefix, handle.name()));
	match action {
		Action::Wake(WakeReason::Doorbell) => {
			tracing::info!(camera = %handle.name(), "Webhook: doorbell wake");
			if claim_wake(&handle, WakeSource::Webhook) {
				spawn_wake_hold(Arc::clone(&handle), hold, cancel);
			}
			// A press is news even when a push edge already woke the camera.
			if let Some(publisher) = &publisher {
				report_doorbell_press(handle.name(), publisher, &handle.status_cache()).await;
			}
		}
		Action::Wake(WakeReason::Pir) => {
			if !claim_wake(&handle, WakeSource::Webhook) {
				tracing::info!(camera = %handle.name(), "Webhook: PIR wake (already reported by a push edge)");
				return;
			}
			tracing::info!(camera = %handle.name(), "Webhook: PIR wake (motion edge)");
			motion_edge(
				Arc::clone(&handle),
				shared.mqtt.as_ref(),
				&shared.topic_prefix,
				hold,
				cancel,
			);
		}
		Action::Wake(WakeReason::Other(reason)) => {
			// Names neither a press nor motion, so it takes no wake
			// claim: a push edge beside it stays a full motion edge.
			tracing::info!(camera = %handle.name(), reason = %reason, "Webhook: wake");
			spawn_wake_hold(Arc::clone(&handle), hold, cancel);
		}
		Action::Sleep => tracing::debug!(camera = %handle.name(), "Webhook: sleep"),
		Action::Test => tracing::debug!(camera = %handle.name(), "Webhook: test"),
		Action::Unrecognised => {
			tracing::debug!(camera = %handle.name(), "Webhook: unrecognised event ignored");
		}
		Action::Alarm(events) => {
			let Some(stream) = shared.alarms.get(handle.name()) else {
				return;
			};
			let update = classify_alarm_events(
				&events,
				handle.config().channel_id,
				std::time::Instant::now(),
			);
			let mut stream = stream.lock().await;
			// The edge sees every forwarded alarm, so a visitor state
			// that clears during a session doesn't swallow the next press.
			let update = stream.edge.apply(update);
			if handle.state().is_connected() {
				// The session's own cmd 33 stream is authoritative, and
				// motion is unknown again once it ends.
				stream.report.motion_on = None;
				tracing::debug!(camera = %handle.name(), "Webhook: alarm ignored; the live session reports alarms");
				return;
			}
			if let Some(publisher) = &publisher {
				let caps = handle.capabilities().unwrap_or_default().into();
				report_alarm_update(
					handle.name(),
					publisher,
					&handle.status_cache(),
					caps,
					&mut stream.report,
					&update,
				)
				.await;
			}
			if matches!(update.motion, MotionStatus::Start(_)) {
				motion_hold(
					Arc::clone(&handle),
					shared.mqtt.as_ref(),
					&shared.topic_prefix,
					hold,
					cancel,
				);
			} else if update.doorbell_press {
				spawn_wake_hold(Arc::clone(&handle), hold, cancel);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;
	use crate::config::CameraConfig;
	use bairelay_mqtt::test_support::MockHandle;
	use std::net::IpAddr;
	use std::time::Duration;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpStream;

	/// Upper bound on any single client exchange in these tests.
	const CLIENT_WAIT: Duration = Duration::from_secs(30);

	const WAKE_DOORBELL: &str = r#"{"data":{"event":"wake","reason":"doorbell"}}"#;
	const WAKE_PIR: &str = r#"{"data":{"event":"wake","reason":"pir"}}"#;
	const WAKE_NETWORK: &str = r#"{"event":"wake","reason":"network"}"#;

	/// Default wake hold in these tests.
	const TEST_HOLD: Duration = Duration::from_secs(1);

	struct Rig {
		port: u16,
		handle: Arc<CameraHandle>,
		mock: MockHandle,
		cancel: CancellationToken,
		server: tokio::task::JoinHandle<()>,
	}

	impl Rig {
		fn token(&self) -> String {
			self.handle.webhook_token().expect("opted in").to_string()
		}

		fn hook_path(&self) -> String {
			hook_path(&self.token())
		}

		async fn post(&self, body: &str) -> String {
			exchange(self.port, &post_request(&self.hook_path(), body)).await
		}

		fn payloads(&self, suffix: &str) -> Vec<Vec<u8>> {
			payloads(&self.mock, suffix)
		}

		async fn stop(self) {
			self.cancel.cancel();
			tokio::time::timeout(Duration::from_secs(1), self.server)
				.await
				.expect("listener stops within 1 s")
				.expect("listener task panic-free");
		}
	}

	/// An opted-in `cam1` at 127.0.0.1.
	fn webhook_camera() -> CameraConfig {
		let mut cfg = minimal_camera_config("cam1");
		cfg.address = Some("127.0.0.1:9000".into());
		cfg.webhook = true;
		cfg
	}

	struct RigOpts {
		cfg: CameraConfig,
		with_mqtt: bool,
	}

	impl Default for RigOpts {
		fn default() -> Self {
			Self {
				cfg: webhook_camera(),
				with_mqtt: true,
			}
		}
	}

	async fn rig_with(cfg: CameraConfig) -> Rig {
		rig_opts(RigOpts {
			cfg,
			..Default::default()
		})
		.await
	}

	async fn rig_opts(opts: RigOpts) -> Rig {
		let handle = Arc::new(CameraHandle::new(opts.cfg, CancellationToken::new(), None));
		let mut cameras = HashMap::new();
		cameras.insert(handle.name().to_string(), Arc::clone(&handle));
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let cancel = CancellationToken::new();
		let server = tokio::spawn(run_with_listener(
			listener,
			Arc::new(cameras),
			opts.with_mqtt.then_some(mqtt),
			"bairelay".into(),
			TEST_HOLD,
			cancel.clone(),
		));
		Rig {
			port,
			handle,
			mock,
			cancel,
			server,
		}
	}

	async fn rig() -> Rig {
		rig_with(webhook_camera()).await
	}

	/// Payloads published to `bairelay/cam1/<suffix>`, in order.
	fn payloads(mock: &MockHandle, suffix: &str) -> Vec<Vec<u8>> {
		let topic = format!("bairelay/cam1/{suffix}");
		mock.published()
			.into_iter()
			.filter(|(t, _, _)| *t == topic)
			.map(|(_, p, _)| p)
			.collect()
	}

	const LOOPBACK: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

	/// The listener's connection handling without sockets, for tests on
	/// paused time: each request runs `serve` over an in-memory duplex.
	struct Local {
		shared: Shared,
		handle: Arc<CameraHandle>,
		mqtt: SharedMqttClient,
		mock: MockHandle,
		cancel: CancellationToken,
	}

	impl Local {
		fn new(cfg: CameraConfig) -> Self {
			let handle = Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None));
			let cameras = HashMap::from([(handle.name().to_string(), Arc::clone(&handle))]);
			let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
			let shared = Shared::new(
				Arc::new(cameras),
				Some(mqtt.clone()),
				"bairelay".into(),
				TEST_HOLD,
			);
			Self {
				shared,
				handle,
				mqtt,
				mock,
				cancel: CancellationToken::new(),
			}
		}

		/// Serve `raw` from `peer` and return the whole response.
		async fn send_from(&self, peer: IpAddr, raw: &[u8]) -> String {
			let (mut client, server_end) = tokio::io::duplex(READ_CHUNK);
			let serving = serve(server_end, peer, &self.shared, &self.cancel);
			let client_side = async {
				client.write_all(raw).await.unwrap();
				let mut out = Vec::new();
				client.read_to_end(&mut out).await.unwrap();
				String::from_utf8(out).unwrap()
			};
			let ((), out) = tokio::join!(serving, client_side);
			out
		}

		/// POST `body` to the camera's hook from the camera's address.
		async fn post(&self, body: &str) -> u16 {
			let token = self.handle.webhook_token().expect("opted in");
			status(
				&self
					.send_from(LOOPBACK, &post_request(&hook_path(token), body))
					.await,
			)
		}

		fn payloads(&self, suffix: &str) -> Vec<Vec<u8>> {
			payloads(&self.mock, suffix)
		}

		fn presses(&self) -> usize {
			self.payloads("status/doorbell").len()
		}

		/// A push-listener edge for the camera, with the test hold.
		fn push(&self) -> bool {
			crate::wake_events::push_edge(
				Arc::clone(&self.handle),
				Some(&self.mqtt),
				"bairelay",
				TEST_HOLD,
				&self.cancel,
			)
		}

		fn motion(&self) -> Vec<Vec<u8>> {
			self.payloads("status/motion")
		}

		fn holds(&self) -> usize {
			self.handle.wake_lock().count()
		}
	}

	fn post_request(path: &str, body: &str) -> Vec<u8> {
		format!(
			"POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
			body.len()
		)
		.into_bytes()
	}

	/// Send `raw`, then read the whole response until the server closes.
	async fn try_exchange(port: u16, raw: &[u8]) -> std::io::Result<String> {
		tokio::time::timeout(CLIENT_WAIT, async {
			let mut sock = TcpStream::connect(("127.0.0.1", port)).await?;
			sock.write_all(raw).await?;
			let mut out = Vec::new();
			sock.read_to_end(&mut out).await?;
			Ok(String::from_utf8(out).expect("ASCII response"))
		})
		.await
		.expect("server answers and closes")
	}

	async fn exchange(port: u16, raw: &[u8]) -> String {
		try_exchange(port, raw).await.expect("exchange")
	}

	fn status(response: &str) -> u16 {
		response
			.strip_prefix("HTTP/1.1 ")
			.and_then(|r| r.get(..3))
			.and_then(|c| c.parse().ok())
			.unwrap_or_else(|| panic!("not an HTTP response: {response:?}"))
	}

	async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		while !cond() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for {what}"
			);
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	}

	fn alarm_body(status: &str, ai_type: &str) -> String {
		let xml = format!(
			"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\" ?><AlarmEventList version=\\\"1.1\\\"><AlarmEvent version=\\\"1.1\\\"><channelId>0</channelId><status>{status}</status><AItype>{ai_type}</AItype><recording>0</recording><timeStamp>0</timeStamp></AlarmEvent></AlarmEventList>"
		);
		format!(r#"{{"uid":"UID0","cmd":33,"xml":"{xml}"}}"#)
	}

	// ── Tokens ──

	#[test]
	fn hook_paths_carry_the_token() {
		use bairelay_neolink_core::ha_webhook::random_hex_token;
		let token = random_hex_token();
		assert_eq!(hook_path(&token), format!("/hook/{token}"));
		assert!(is_hook_path(&hook_path(&token)));
	}

	// ── Webhook target ──

	fn webhook_block(addr: Option<&str>, advertise: Option<&str>) -> WebhookConfig {
		WebhookConfig {
			enable: true,
			webhook_addr: addr.map(str::to_string),
			webhook_port: 8780,
			webhook_advertise: advertise.map(str::to_string),
			..WebhookConfig::default()
		}
	}

	fn target(addr: Option<&str>, advertise: Option<&str>, bind: &str) -> WebhookTarget {
		WebhookTarget::from_config(&webhook_block(addr, advertise), bind).unwrap()
	}

	const CAM: Option<IpAddr> = Some(IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 10)));

	fn url_ok(url: &str) -> Result<String, NoWebhookHost> {
		Ok(url.to_string())
	}

	#[test]
	fn target_url_prefers_the_advertise_override() {
		let t = target(Some("192.0.2.5"), Some("example.com:9443"), "0.0.0.0");
		for cam in [CAM, None] {
			assert_eq!(
				t.url(cam, "abc"),
				url_ok("http://example.com:9443/hook/abc")
			);
		}
		let v6 = target(None, Some("[2001:db8::5]:8780"), "0.0.0.0");
		assert_eq!(
			v6.url(None, "abc"),
			url_ok("http://[2001:db8::5]:8780/hook/abc")
		);
	}

	#[test]
	fn target_url_uses_a_concrete_bind_address() {
		let own = target(Some("192.0.2.5"), None, "0.0.0.0");
		assert_eq!(
			own.url(None, "abc"),
			url_ok("http://192.0.2.5:8780/hook/abc")
		);
		let top = target(None, None, "192.0.2.6");
		assert_eq!(
			top.url(CAM, "abc"),
			url_ok("http://192.0.2.6:8780/hook/abc")
		);
		let v6 = target(Some("2001:db8::5"), None, "0.0.0.0");
		assert_eq!(
			v6.url(None, "abc"),
			url_ok("http://[2001:db8::5]:8780/hook/abc")
		);
	}

	#[test]
	fn target_url_on_a_wildcard_bind_uses_the_route_to_the_camera() {
		let t = target(None, None, "0.0.0.0");
		assert_eq!(t.url(None, "abc"), Err(NoWebhookHost::UnknownCamera));
		assert_eq!(
			t.url(Some(LOOPBACK), "abc"),
			url_ok("http://127.0.0.1:8780/hook/abc")
		);
	}

	/// A loopback host is only given to a camera on loopback itself.
	#[test]
	fn target_url_never_gives_loopback_to_an_off_host_camera() {
		for bind in ["127.0.0.1", "::1"] {
			let t = target(Some(bind), None, "0.0.0.0");
			assert_eq!(
				t.url(CAM, "abc"),
				Err(NoWebhookHost::LoopbackOnly),
				"{bind}"
			);
			assert_eq!(
				t.url(None, "abc"),
				Err(NoWebhookHost::LoopbackOnly),
				"{bind}"
			);
		}
		let t = target(Some("127.0.0.1"), None, "0.0.0.0");
		assert_eq!(
			t.url(Some(LOOPBACK), "abc"),
			url_ok("http://127.0.0.1:8780/hook/abc")
		);
		let mapped = Some(IpAddr::V6(std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped()));
		assert_eq!(
			t.url(mapped, "abc"),
			url_ok("http://127.0.0.1:8780/hook/abc")
		);
	}

	#[test]
	fn no_webhook_host_names_the_fix() {
		for reason in [NoWebhookHost::UnknownCamera, NoWebhookHost::LoopbackOnly] {
			assert!(reason.to_string().contains("webhook_advertise"), "{reason}");
		}
	}

	#[test]
	fn target_carries_the_webhook_hold() {
		let mut block = webhook_block(None, None);
		block.motion_wake_hold_secs = 12.5;
		let t = WebhookTarget::from_config(&block, "0.0.0.0").unwrap();
		assert_eq!(t.hold(), Duration::from_millis(12_500));
		block.motion_wake_hold_secs = -1.0;
		let err = WebhookTarget::from_config(&block, "0.0.0.0").unwrap_err();
		assert!(
			format!("{err:#}").contains("motion_wake_hold_secs"),
			"{err:#}"
		);
	}

	#[test]
	fn target_rejects_unparseable_addresses() {
		let err =
			WebhookTarget::from_config(&webhook_block(Some("nope"), None), "0.0.0.0").unwrap_err();
		assert!(format!("{err:#}").contains("webhook_addr"), "{err:#}");
		let err =
			WebhookTarget::from_config(&webhook_block(None, Some("nope")), "0.0.0.0").unwrap_err();
		assert!(format!("{err:#}").contains("webhook_advertise"), "{err:#}");
	}

	#[test]
	fn ct_eq_compares_bytes_and_lengths() {
		assert!(ct_eq(b"0123abcd", b"0123abcd"));
		assert!(ct_eq(b"", b""));
		assert!(!ct_eq(b"0123abcd", b"0123abce"));
		assert!(!ct_eq(b"0123abcd", b"1123abcd"));
		assert!(!ct_eq(b"0123abcd", b"0123abc"));
		assert!(!ct_eq(b"0123abc", b"0123abcd"));
	}

	#[test]
	fn only_opted_in_cameras_get_a_token() {
		let opted = CameraHandle::new(webhook_camera(), CancellationToken::new(), None);
		let plain = CameraHandle::new(
			minimal_camera_config("cam2"),
			CancellationToken::new(),
			None,
		);
		assert_eq!(opted.webhook_token().map(str::len), Some(32));
		assert_eq!(plain.webhook_token(), None);
		let again = CameraHandle::new(webhook_camera(), CancellationToken::new(), None);
		assert_ne!(opted.webhook_token(), again.webhook_token());
	}

	// ── Mapping ──

	#[tokio::test]
	async fn doorbell_wake_publishes_press_and_holds_the_wake_lock() {
		let rig = rig().await;
		let resp = rig.post(WAKE_DOORBELL).await;
		assert_eq!(status(&resp), 200);
		wait_for("press", || {
			rig.payloads("status/doorbell") == [b"press".to_vec()]
		})
		.await;
		wait_for("wake lock", || rig.handle.wake_lock().count() == 1).await;
		assert!(rig.payloads("status/motion").is_empty());
		wait_for("hold release", || rig.handle.wake_lock().count() == 0).await;
		rig.stop().await;
	}

	#[tokio::test]
	async fn without_mqtt_every_wake_still_holds_the_wake_lock() {
		let rig = rig_opts(RigOpts {
			with_mqtt: false,
			..Default::default()
		})
		.await;
		// Each wake adds its own hold while the earlier ones last.
		for (i, body) in [WAKE_DOORBELL, WAKE_PIR, WAKE_NETWORK]
			.into_iter()
			.enumerate()
		{
			assert_eq!(status(&rig.post(body).await), 200, "{body}");
			wait_for(body, || rig.handle.wake_lock().count() > i).await;
		}
		rig.stop().await;
	}

	#[tokio::test]
	async fn pir_wake_is_a_motion_edge_with_fallback_off() {
		let rig = rig().await;
		assert_eq!(status(&rig.post(WAKE_PIR).await), 200);
		wait_for("motion on", || {
			rig.payloads("status/motion") == [b"on".to_vec()]
		})
		.await;
		assert_eq!(rig.handle.wake_lock().count(), 1);
		wait_for("fallback off", || {
			rig.payloads("status/motion") == [b"on".to_vec(), b"off".to_vec()]
		})
		.await;
		wait_for("hold release", || rig.handle.wake_lock().count() == 0).await;
		assert!(rig.payloads("status/doorbell").is_empty());
		rig.stop().await;
	}

	#[tokio::test(start_paused = true)]
	async fn pir_wake_shares_newest_edge_wins_with_push_edges() {
		// With hold H: webhook PIR A at t=0, a push edge P at t=H/2,
		// webhook PIR B at t=H. Only B clears, at t=2H.
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_PIR).await, 200);
		tokio::time::sleep(TEST_HOLD / 2).await;
		crate::wake_events::motion_edge(
			Arc::clone(&local.handle),
			Some(&local.mqtt),
			"bairelay",
			TEST_HOLD,
			&local.cancel,
		);
		tokio::time::sleep(TEST_HOLD / 2).await;
		assert_eq!(local.post(WAKE_PIR).await, 200);
		// t=7H/4: past A's and P's windows, inside B's.
		tokio::time::sleep(TEST_HOLD * 3 / 4).await;
		assert_eq!(local.motion(), vec![b"on".to_vec(); 3]);
		tokio::time::sleep(TEST_HOLD / 2).await;
		assert_eq!(
			local.motion(),
			[
				b"on".to_vec(),
				b"on".to_vec(),
				b"on".to_vec(),
				b"off".to_vec()
			]
		);
	}

	#[tokio::test]
	async fn other_wake_holds_the_wake_lock_only() {
		let rig = rig().await;
		assert_eq!(status(&rig.post(WAKE_NETWORK).await), 200);
		wait_for("wake lock", || rig.handle.wake_lock().count() == 1).await;
		wait_for("hold release", || rig.handle.wake_lock().count() == 0).await;
		assert!(rig.mock.published().is_empty());
		rig.stop().await;
	}

	#[tokio::test(start_paused = true)]
	async fn sleep_test_and_unrecognised_events_change_nothing() {
		let local = Local::new(webhook_camera());
		for body in [
			r#"{"data":{"event":"sleep"}}"#,
			r#"{"data":{"event":"test"}}"#,
			r#"{"data":{"event":"dance"}}"#,
			r#"{"cmd":34,"xml":"<a/>"}"#,
		] {
			assert_eq!(local.post(body).await, 200, "{body}");
		}
		// `serve` returned after dispatch; anything it spawned has run.
		tokio::time::sleep(SETTLE).await;
		assert!(local.mock.published().is_empty());
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test]
	async fn alarm_publishes_motion_objects_and_one_press_per_visitor_edge() {
		let rig = rig().await;
		assert_eq!(status(&rig.post(&alarm_body("MD", "people")).await), 200);
		wait_for("motion on", || {
			rig.payloads("status/motion") == [b"on".to_vec()]
		})
		.await;
		wait_for("person on", || {
			rig.payloads("status/ai/person").last() == Some(&b"on".to_vec())
		})
		.await;
		assert_eq!(
			rig.handle.wake_lock().count(),
			1,
			"motion start takes the wake hold"
		);

		assert_eq!(status(&rig.post(&alarm_body("visitor", "none")).await), 200);
		wait_for("press", || {
			rig.payloads("status/doorbell") == [b"press".to_vec()]
		})
		.await;
		assert_eq!(status(&rig.post(&alarm_body("visitor", "none")).await), 200);
		assert_eq!(status(&rig.post(&alarm_body("none", "none")).await), 200);
		wait_for("motion off", || {
			rig.payloads("status/motion").last() == Some(&b"off".to_vec())
		})
		.await;
		assert_eq!(
			rig.payloads("status/doorbell"),
			[b"press".to_vec()],
			"a held visitor state is one press"
		);
		assert_eq!(
			rig.payloads("status/ai/person").last(),
			Some(&b"off".to_vec())
		);
		rig.stop().await;
	}

	#[tokio::test(start_paused = true)]
	async fn alarm_is_ignored_while_the_camera_has_a_live_session() {
		let local = Local::new(webhook_camera());
		let fake = bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build();
		local.handle.set_driver_for_test(fake);
		assert!(local.handle.state().is_connected());
		assert_eq!(local.post(&alarm_body("MD,visitor", "people")).await, 200);
		// `serve` returned after dispatch; anything it spawned has run.
		tokio::time::sleep(SETTLE).await;
		assert!(local.mock.published().is_empty());
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn alarm_edge_tracks_visitor_while_a_session_is_live() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(&alarm_body("visitor", "none")).await, 200);
		assert_eq!(local.presses(), 1);
		// A session comes up; the forwarded "visitor cleared" is ignored
		// for publishing but still moves the edge.
		local.handle.set_driver_for_test(
			bairelay_neolink_core::bc_protocol::FakeCameraBuilder::new().build(),
		);
		assert_eq!(local.post(&alarm_body("none", "none")).await, 200);
		local
			.handle
			.set_state_for_test(crate::camera::CameraState::Disconnected);
		// Past the press dedupe window, a new visitor state is a new press.
		tokio::time::sleep(Duration::from_secs(11)).await;
		assert_eq!(local.post(&alarm_body("visitor", "none")).await, 200);
		assert_eq!(local.presses(), 2);
	}

	#[tokio::test(start_paused = true)]
	async fn alarm_start_without_stop_clears_motion_after_the_hold() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(&alarm_body("MD", "none")).await, 200);
		assert_eq!(local.payloads("status/motion"), [b"on".to_vec()]);
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert_eq!(local.handle.wake_lock().count(), 1);
		tokio::time::sleep(TEST_HOLD).await;
		assert_eq!(
			local.payloads("status/motion"),
			[b"on".to_vec(), b"off".to_vec()]
		);
		assert_eq!(local.handle.wake_lock().count(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn alarm_start_shares_newest_edge_wins_with_push_edges() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(&alarm_body("MD", "none")).await, 200);
		tokio::time::sleep(TEST_HOLD / 2).await;
		crate::wake_events::motion_edge(
			Arc::clone(&local.handle),
			Some(&local.mqtt),
			"bairelay",
			TEST_HOLD,
			&local.cancel,
		);
		// Past the alarm's window, inside the push edge's.
		tokio::time::sleep(TEST_HOLD * 3 / 4).await;
		assert!(!local.payloads("status/motion").contains(&b"off".to_vec()));
		tokio::time::sleep(TEST_HOLD / 2).await;
		assert_eq!(
			local.payloads("status/motion").last(),
			Some(&b"off".to_vec())
		);
	}

	// ── Doorbell press dedupe ──

	#[tokio::test(start_paused = true)]
	async fn doorbell_wake_then_session_visitor_is_one_press() {
		use bairelay_neolink_core::bc_protocol::AlarmUpdate;
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		assert_eq!(local.presses(), 1);
		// The session that the wake brings up reports the same press.
		let publisher = StatusPublisher::new(&local.mqtt, "bairelay", "cam1");
		let press = AlarmUpdate {
			visitor: Some(true),
			doorbell_press: true,
			..AlarmUpdate::no_change(std::time::Instant::now())
		};
		tokio::time::sleep(Duration::from_secs(3)).await;
		report_alarm_update(
			"cam1",
			&publisher,
			&local.handle.status_cache(),
			Default::default(),
			&mut AlarmReport::default(),
			&press,
		)
		.await;
		assert_eq!(local.presses(), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn alarm_visitor_then_doorbell_wake_is_one_press() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(&alarm_body("visitor", "none")).await, 200);
		tokio::time::sleep(Duration::from_secs(2)).await;
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		assert_eq!(local.presses(), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn doorbell_wakes_dedupe_inside_the_window_only() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		tokio::time::sleep(Duration::from_secs(5)).await;
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		assert_eq!(local.presses(), 1, "inside the window");
		tokio::time::sleep(Duration::from_secs(6)).await;
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		assert_eq!(local.presses(), 2, "outside the window");
	}

	// ── One wake, two reports: push edge and webhook ──

	/// Lets spawned holds and publishes run on paused time.
	const SETTLE: Duration = Duration::from_millis(10);

	#[tokio::test(start_paused = true)]
	async fn push_edge_then_pir_wake_is_one_motion_on_and_one_hold() {
		let logs = crate::test_logs::capture_at(tracing::Level::INFO);
		let local = Local::new(webhook_camera());
		assert!(local.push());
		tokio::time::sleep(Duration::from_millis(200)).await;
		assert_eq!(local.post(WAKE_PIR).await, 200);
		tokio::time::sleep(SETTLE).await;
		let text = logs.text();
		assert!(
			text.contains("Webhook: PIR wake (already reported by a push edge)"),
			"{text}"
		);
		assert!(!text.contains("PIR wake (motion edge)"), "{text}");
		assert_eq!(local.motion(), [b"on".to_vec()]);
		assert_eq!(local.holds(), 1);
		tokio::time::sleep(TEST_HOLD).await;
		assert_eq!(local.motion(), [b"on".to_vec(), b"off".to_vec()]);
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn pir_wake_then_push_edge_is_one_motion_on_and_one_hold() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_PIR).await, 200);
		tokio::time::sleep(Duration::from_millis(200)).await;
		assert!(!local.push());
		tokio::time::sleep(SETTLE).await;
		assert_eq!(local.motion(), [b"on".to_vec()]);
		assert_eq!(local.holds(), 1);
		tokio::time::sleep(TEST_HOLD).await;
		assert_eq!(local.motion(), [b"on".to_vec(), b"off".to_vec()]);
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn push_edge_then_doorbell_wake_still_publishes_the_press() {
		let local = Local::new(webhook_camera());
		assert!(local.push());
		tokio::time::sleep(Duration::from_millis(200)).await;
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		tokio::time::sleep(SETTLE).await;
		assert_eq!(local.presses(), 1);
		assert_eq!(local.motion(), [b"on".to_vec()]);
		assert_eq!(local.holds(), 1, "the push edge's hold covers the press");
	}

	#[tokio::test(start_paused = true)]
	async fn doorbell_wake_then_push_edge_publishes_no_motion() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_DOORBELL).await, 200);
		tokio::time::sleep(Duration::from_millis(200)).await;
		assert!(!local.push());
		tokio::time::sleep(SETTLE).await;
		assert_eq!(local.presses(), 1);
		assert_eq!(local.holds(), 1);
		tokio::time::sleep(TEST_HOLD).await;
		assert!(local.motion().is_empty());
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn other_wakes_take_no_wake_claim() {
		// A wake that names neither a press nor PIR says nothing about
		// motion, so a push edge after it is a full motion edge.
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_NETWORK).await, 200);
		assert!(local.push());
		tokio::time::sleep(SETTLE).await;
		assert_eq!(local.holds(), 2);
		assert_eq!(local.motion(), [b"on".to_vec()]);
		// And a push edge first leaves the other wake its own hold.
		tokio::time::sleep(crate::status_cache::WAKE_DEDUPE_WINDOW).await;
		assert!(local.push());
		assert_eq!(local.post(WAKE_NETWORK).await, 200);
		tokio::time::sleep(SETTLE).await;
		assert_eq!(local.holds(), 2);
	}

	#[tokio::test(start_paused = true)]
	async fn a_push_edge_after_the_window_is_a_new_wake() {
		let local = Local::new(webhook_camera());
		assert_eq!(local.post(WAKE_PIR).await, 200);
		tokio::time::sleep(crate::status_cache::WAKE_DEDUPE_WINDOW).await;
		assert!(local.push());
		tokio::time::sleep(SETTLE).await;
		assert_eq!(
			local.motion(),
			[b"on".to_vec(), b"off".to_vec(), b"on".to_vec()]
		);
		assert_eq!(local.holds(), 1);
	}

	// ── enable_motion ──

	#[tokio::test(start_paused = true)]
	async fn enable_motion_off_holds_wakes_but_publishes_nothing() {
		let mut cfg = webhook_camera();
		cfg.mqtt.enable_motion = false;
		let local = Local::new(cfg);
		for body in [
			WAKE_DOORBELL.to_string(),
			WAKE_PIR.to_string(),
			alarm_body("MD,visitor", "people"),
		] {
			assert_eq!(local.post(&body).await, 200, "{body}");
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert!(local.handle.wake_lock().count() >= 3);
		tokio::time::sleep(TEST_HOLD * 2).await;
		assert!(
			local.mock.published().is_empty(),
			"{:?}",
			local.mock.published_topics()
		);
		assert_eq!(local.handle.wake_lock().count(), 0);
	}

	// ── Status codes ──

	#[tokio::test]
	async fn malformed_requests_get_400() {
		let rig = rig().await;
		let path = rig.hook_path();
		let cases: Vec<Vec<u8>> = vec![
			b"POST\r\n\r\n".to_vec(),
			format!("POST {path} HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").into_bytes(),
			format!("POST {path} HTTP/1.1\r\n\r\n").into_bytes(),
			post_request(&path, "not json"),
			post_request(&path, r#"{"cmd":33,"xml":"<garbage"}"#),
		];
		for raw in cases {
			let resp = exchange(rig.port, &raw).await;
			assert_eq!(status(&resp), 400, "{:?}", String::from_utf8_lossy(&raw));
		}
		assert!(rig.mock.published().is_empty());
		rig.stop().await;
	}

	#[tokio::test]
	async fn eof_before_a_complete_request_gets_400() {
		let rig = rig().await;
		let mut sock = TcpStream::connect(("127.0.0.1", rig.port)).await.unwrap();
		sock.write_all(format!("POST {} HTTP/1.1\r\n", rig.hook_path()).as_bytes())
			.await
			.unwrap();
		sock.shutdown().await.unwrap();
		let mut out = String::new();
		tokio::time::timeout(CLIENT_WAIT, sock.read_to_string(&mut out))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(status(&out), 400);
		rig.stop().await;
	}

	#[tokio::test]
	async fn non_post_methods_get_405() {
		let rig = rig().await;
		let path = rig.hook_path();
		for method in ["GET", "PUT", "HEAD", "post"] {
			let raw = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n\r\n");
			let resp = exchange(rig.port, raw.as_bytes()).await;
			assert_eq!(status(&resp), 405, "{method}");
			assert!(resp.contains("\r\nAllow: POST\r\n"), "{resp:?}");
		}
		rig.stop().await;
	}

	#[tokio::test]
	async fn unknown_paths_and_tokens_get_404() {
		let rig = rig().await;
		let token = rig.token();
		let mut wrong = token.clone().into_bytes();
		wrong[0] = if wrong[0] == b'0' { b'1' } else { b'0' };
		let wrong = String::from_utf8(wrong).unwrap();
		for path in [
			"/".to_string(),
			"/hook".to_string(),
			"/hook/".to_string(),
			format!("/hook/{wrong}"),
			format!("/hook/{token}/"),
			format!("/hook/{token}x"),
			format!("/hook/{}", &token[..31]),
			format!("/hook/{token}?a=1"),
			format!("/other/{token}"),
		] {
			let resp = exchange(rig.port, &post_request(&path, WAKE_DOORBELL)).await;
			assert_eq!(status(&resp), 404, "{path}");
		}
		assert!(rig.mock.published().is_empty());
		rig.stop().await;
	}

	/// A request that reaches routing from a peer other than the token's
	/// camera (e.g. another opted-in camera) is refused.
	#[tokio::test(start_paused = true)]
	async fn a_peer_other_than_the_camera_gets_403() {
		let mut cfg = webhook_camera();
		cfg.address = Some("192.0.2.10:9000".into());
		let local = Local::new(cfg);
		assert_eq!(local.post(WAKE_DOORBELL).await, 403);
		tokio::time::sleep(TEST_HOLD * 2).await;
		assert!(local.mock.published().is_empty());
		assert_eq!(local.holds(), 0);
	}

	#[tokio::test(start_paused = true)]
	async fn an_unknown_camera_address_gets_403() {
		let mut cfg = webhook_camera();
		cfg.address = Some("cam1.example.com:9000".into());
		let local = Local::new(cfg);
		assert_eq!(local.handle.camera_ip(), None);
		assert_eq!(local.post(WAKE_DOORBELL).await, 403);
	}

	/// A peer that is no opted-in camera is closed at accept, long
	/// before the read deadline a served connection gets.
	#[tokio::test]
	async fn a_stranger_is_closed_before_its_request_is_read() {
		let mut cfg = webhook_camera();
		cfg.address = Some("192.0.2.10:9000".into());
		let rig = rig_with(cfg).await;
		let mut sock = TcpStream::connect(("127.0.0.1", rig.port)).await.unwrap();
		let mut out = Vec::new();
		let read = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut out))
			.await
			.expect("a stranger is closed at once");
		assert!(read.is_err() || out.is_empty(), "closed unserved: {out:?}");
		assert!(rig.mock.published().is_empty());
		rig.stop().await;
	}

	#[test]
	fn known_peers_are_opted_in_cameras_recorded_or_configured_ips() {
		let camera = |name: &str, address: &str, webhook: bool| {
			let mut cfg = minimal_camera_config(name);
			cfg.address = Some(address.into());
			cfg.webhook = webhook;
			Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None))
		};
		let literal = camera("cam1", "192.0.2.10:9000", true);
		let plain = camera("cam2", "192.0.2.20:9000", false);
		let named = camera("cam3", "cam3.example.com:9000", true);
		named.set_camera_ip_for_test(IpAddr::from([192, 0, 2, 30]));
		let cameras = [literal, plain, named]
			.into_iter()
			.map(|h| (h.name().to_string(), h))
			.collect();
		let shared = Shared::new(Arc::new(cameras), None, "bairelay".into(), TEST_HOLD);
		let v4 = |n: u8| IpAddr::from([192, 0, 2, n]);
		assert!(shared.is_known_peer(v4(10)));
		assert!(shared.is_known_peer(v4(30)));
		let mapped = IpAddr::V6(std::net::Ipv4Addr::new(192, 0, 2, 10).to_ipv6_mapped());
		assert!(shared.is_known_peer(mapped));
		assert!(!shared.is_known_peer(v4(20)), "not opted in");
		assert!(!shared.is_known_peer(v4(99)));
	}

	#[tokio::test]
	async fn the_recorded_camera_address_is_accepted() {
		let mut cfg = webhook_camera();
		cfg.address = Some("192.0.2.10:9000".into());
		let rig = rig_with(cfg).await;
		rig.handle
			.set_camera_ip_for_test(IpAddr::from([127, 0, 0, 1]));
		assert_eq!(status(&rig.post(WAKE_DOORBELL).await), 200);
		rig.stop().await;
	}

	#[tokio::test(start_paused = true)]
	async fn an_ipv4_mapped_camera_peer_is_accepted() {
		let local = Local::new(webhook_camera());
		let mapped = IpAddr::V6(std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped());
		let token = local.handle.webhook_token().unwrap().to_string();
		let resp = local
			.send_from(mapped, &post_request(&hook_path(&token), WAKE_NETWORK))
			.await;
		assert_eq!(status(&resp), 200);
	}

	/// A reader that hands out the given chunks one read at a time.
	struct Chunks(std::collections::VecDeque<Vec<u8>>);

	impl AsyncRead for Chunks {
		fn poll_read(
			mut self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
			buf: &mut tokio::io::ReadBuf<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			if let Some(mut chunk) = self.0.pop_front() {
				let n = chunk.len().min(buf.remaining());
				buf.put_slice(&chunk[..n]);
				if n < chunk.len() {
					self.0.push_front(chunk.split_off(n));
				}
			}
			std::task::Poll::Ready(Ok(()))
		}
	}

	#[tokio::test]
	async fn a_maximum_size_request_with_trailing_bytes_is_read_whole() {
		use bairelay_neolink_core::ha_webhook::{MAX_BODY_BYTES, MAX_HEADER_BYTES};
		let envelope = r#"{"event":"sleep"}"#;
		let body = format!("{envelope}{}", " ".repeat(MAX_BODY_BYTES - envelope.len()));
		let head = format!("POST /hook/x HTTP/1.1\r\nContent-Length: {MAX_BODY_BYTES}\r\nX-Pad: ");
		// Pad the header block to just under its limit.
		let pad = MAX_HEADER_BYTES - 200 - head.len();
		let mut request = format!("{head}{}\r\n\r\n", "a".repeat(pad)).into_bytes();
		request.extend_from_slice(body.as_bytes());
		request.extend_from_slice(&[b'x'; READ_CHUNK]);
		// A short first read leaves every later read straddling the
		// end of the request.
		let rest = request.split_off(100);
		let mut reader = Chunks([request, rest].into());
		let buf = read_request(&mut reader).await.expect("a whole request");
		let parsed = parse_http_request(&buf).expect("parses");
		assert_eq!(parsed.body.len(), MAX_BODY_BYTES);
	}

	/// A body dripped one byte per read is parsed again only once its
	/// whole length is buffered.
	#[test]
	fn a_dripped_body_is_parsed_once_its_length_is_buffered() {
		let raw = post_request(
			"/hook/x",
			&format!(r#"{{"event":"sleep","pad":"{}"}}"#, "p".repeat(1000)),
		);
		let body_start = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
		let mut request = RequestBuf::default();
		assert_eq!(request.push(&raw[..body_start]), None);
		assert_eq!(request.parses, 1);
		for byte in &raw[body_start..raw.len() - 1] {
			assert_eq!(request.push(std::slice::from_ref(byte)), None);
		}
		assert_eq!(request.parses, 1, "no parse before the body is whole");
		assert_eq!(request.push(&raw[raw.len() - 1..]), Some(Ok(())));
		assert_eq!(request.parses, 2);
		assert_eq!(request.buf, raw);
	}

	/// A header block dripped one byte per read is parsed once its
	/// blank line arrives, then once the body is whole.
	#[test]
	fn a_dripped_header_is_parsed_at_its_blank_line() {
		let raw = post_request("/hook/x", WAKE_DOORBELL);
		let mut request = RequestBuf::default();
		for byte in &raw[..raw.len() - 1] {
			assert_eq!(request.push(std::slice::from_ref(byte)), None);
		}
		assert_eq!(request.parses, 1, "one parse, at the blank line");
		assert_eq!(request.push(&raw[raw.len() - 1..]), Some(Ok(())));
		assert_eq!(request.parses, 2);
		assert_eq!(request.buf, raw);
	}

	/// The blank line split across reads is still found, and a header
	/// block that never ends is refused at the limit.
	#[test]
	fn the_header_end_is_found_across_reads_and_bounded() {
		let raw = b"GET /hook/x HTTP/1.1\r\nHost: x\r\n\r\n";
		for split in 1..4 {
			let mut request = RequestBuf::default();
			let at = raw.len() - split;
			assert_eq!(request.push(&raw[..at]), None);
			assert_eq!(request.push(&raw[at..]), Some(Ok(())), "split {split}");
		}
		let mut request = RequestBuf::default();
		let mut result = None;
		for _ in 0..MAX_HEADER_BYTES {
			result = request.push(b"a");
			if result.is_some() {
				break;
			}
		}
		assert_eq!(result, Some(Err(Status::PayloadTooLarge)));
		assert_eq!(request.buf.len(), MAX_HEADER_BYTES);
		assert_eq!(request.parses, 1);
	}

	#[tokio::test]
	async fn a_request_read_one_byte_at_a_time_is_read_whole() {
		let raw = post_request("/hook/x", WAKE_DOORBELL);
		let mut reader = Chunks(raw.iter().map(|b| vec![*b]).collect());
		assert_eq!(read_request(&mut reader).await, Ok(raw));
	}

	#[tokio::test]
	async fn oversize_requests_get_413() {
		use bairelay_neolink_core::ha_webhook::{MAX_BODY_BYTES, MAX_HEADER_BYTES};
		let rig = rig().await;
		let path = rig.hook_path();
		let body_too_large = format!(
			"POST {path} HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
			MAX_BODY_BYTES + 1
		);
		assert_eq!(
			status(&exchange(rig.port, body_too_large.as_bytes()).await),
			413
		);
		// Exactly the header limit with no blank line, so the server
		// reads every byte before it answers.
		let mut header_too_large = format!("POST {path} HTTP/1.1\r\nX-Pad: ").into_bytes();
		header_too_large.resize(MAX_HEADER_BYTES, b'a');
		assert_eq!(status(&exchange(rig.port, &header_too_large).await), 413);
		rig.stop().await;
	}

	#[tokio::test]
	async fn responses_are_fixed_and_never_echo_the_request() {
		let rig = rig().await;
		let marker = "zz-echo-marker-zz";
		for raw in [
			post_request(&format!("/hook/{marker}"), marker),
			format!("GET /{marker} HTTP/1.1\r\n\r\n").into_bytes(),
			post_request(
				&rig.hook_path(),
				&format!(r#"{{"event":"wake","reason":"{marker}"}}"#),
			),
		] {
			let resp = exchange(rig.port, &raw).await;
			assert!(!resp.contains(marker), "{resp:?}");
			assert!(resp.contains("\r\nConnection: close\r\n"), "{resp:?}");
			assert!(resp.ends_with("\r\nContent-Length: 0\r\n\r\n"), "{resp:?}");
		}
		rig.stop().await;
	}

	// ── Limits and shutdown ──

	#[tokio::test(start_paused = true)]
	async fn a_slow_client_is_cut_off_at_the_read_deadline() {
		let local = Local::new(webhook_camera());
		let (mut client, server_end) = tokio::io::duplex(1024);
		let started = tokio::time::Instant::now();
		let serving = serve(server_end, LOOPBACK, &local.shared, &local.cancel);
		let client_side = async {
			client.write_all(b"POST /hook/").await.unwrap();
			let mut out = Vec::new();
			client.read_to_end(&mut out).await.unwrap();
			(out, started.elapsed())
		};
		let ((), (out, waited)) =
			tokio::time::timeout(CLIENT_WAIT, async { tokio::join!(serving, client_side) })
				.await
				.expect("server closes at its deadline");
		assert!(out.is_empty(), "no response after the deadline: {out:?}");
		assert_eq!(waited, READ_DEADLINE);
	}

	impl Admission {
		/// Per-peer counts, sorted, for assertions.
		fn counts(&self) -> Vec<(IpAddr, usize)> {
			let mut counts: Vec<_> = self
				.per_peer
				.lock_recover()
				.iter()
				.map(|(ip, n)| (*ip, *n))
				.collect();
			counts.sort();
			counts
		}
	}

	#[test]
	fn admission_rejections_leave_the_peer_map_unchanged() {
		let admission = Admission::new();
		let peer = |n: u8| IpAddr::from([192, 0, 2, n]);
		let held: Vec<_> = (0..MAX_CONNECTIONS as u8)
			.map(|n| admission.admit(peer(n % 4)).expect("a slot is free"))
			.collect();
		let before = admission.counts();
		assert_eq!(before.len(), 4);
		// Rejected at the total cap: fresh peers leave no entry.
		for n in 100..120 {
			assert!(admission.admit(peer(n)).is_none());
		}
		assert_eq!(admission.counts(), before, "no entry for a rejected peer");
		drop(held);
		assert!(admission.counts().is_empty(), "drops remove zero entries");

		// Rejected at the per-peer cap: the count stays at the cap, and
		// the slot it tried is freed again.
		let same: Vec<_> = (0..MAX_CONNECTIONS_PER_PEER)
			.map(|_| admission.admit(peer(1)).unwrap())
			.collect();
		let before = admission.counts();
		assert!(admission.admit(peer(1)).is_none());
		assert_eq!(admission.counts(), before);
		assert_eq!(
			admission.slots.available_permits(),
			MAX_CONNECTIONS - MAX_CONNECTIONS_PER_PEER
		);

		// Each drop decrements; the last removes the entry.
		let mut same = same;
		drop(same.pop());
		assert_eq!(
			admission.counts(),
			[(peer(1), MAX_CONNECTIONS_PER_PEER - 1)]
		);
		drop(same);
		assert!(admission.counts().is_empty());
		assert_eq!(admission.slots.available_permits(), MAX_CONNECTIONS);
	}

	#[test]
	fn admission_caps_connections_in_all_and_per_peer() {
		let admission = Admission::new();
		let peer = |n: u8| IpAddr::from([192, 0, 2, n]);
		let mut held = Vec::new();
		for n in 0..MAX_CONNECTIONS as u8 {
			held.push(admission.admit(peer(n)).expect("a slot is free"));
		}
		assert!(admission.admit(peer(200)).is_none(), "every slot is held");
		drop(held.pop());
		assert!(
			admission.admit(peer(200)).is_some(),
			"a freed slot is reusable"
		);

		let admission = Admission::new();
		let same: Vec<_> = (0..MAX_CONNECTIONS_PER_PEER)
			.map(|_| admission.admit(peer(1)).expect("under the per-peer cap"))
			.collect();
		assert!(admission.admit(peer(1)).is_none(), "per-peer cap");
		assert!(admission.admit(peer(2)).is_some(), "other peers unaffected");
		drop(same);
		assert!(admission.admit(peer(1)).is_some(), "released on drop");
	}

	#[test]
	fn admission_counts_an_ipv4_mapped_peer_as_its_ipv4_address() {
		let admission = Admission::new();
		let v4 = std::net::Ipv4Addr::new(192, 0, 2, 1);
		let _held: Vec<_> = (0..MAX_CONNECTIONS_PER_PEER)
			.map(|_| admission.admit(IpAddr::V4(v4)).unwrap())
			.collect();
		assert!(admission.admit(IpAddr::V6(v4.to_ipv6_mapped())).is_none());
	}

	#[tokio::test]
	async fn connections_over_the_per_peer_cap_are_closed_unserved() {
		let rig = rig().await;
		let mut idle = Vec::new();
		for _ in 0..MAX_CONNECTIONS_PER_PEER {
			idle.push(TcpStream::connect(("127.0.0.1", rig.port)).await.unwrap());
		}
		// The accept loop takes connections in order, so this one finds
		// every slot held and is closed without a response.
		let mut over = TcpStream::connect(("127.0.0.1", rig.port)).await.unwrap();
		let mut out = Vec::new();
		// Well inside the read deadline a served connection would get.
		let read = tokio::time::timeout(Duration::from_secs(2), over.read_to_end(&mut out))
			.await
			.expect("over-cap connection is closed at once");
		assert!(read.is_err() || out.is_empty(), "closed unserved: {out:?}");

		drop(idle.pop());
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let resp = try_exchange(rig.port, &post_request(&rig.hook_path(), WAKE_DOORBELL)).await;
			if let Some(resp) = resp.ok().filter(|r| !r.is_empty()) {
				assert_eq!(status(&resp), 200);
				break;
			}
			assert!(tokio::time::Instant::now() < deadline, "a slot frees up");
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		rig.stop().await;
	}

	#[tokio::test]
	async fn cancel_stops_the_listener_and_open_connections_promptly() {
		let rig = rig().await;
		let mut sock = TcpStream::connect(("127.0.0.1", rig.port)).await.unwrap();
		sock.write_all(b"POST /hook/").await.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		let started = std::time::Instant::now();
		let port = rig.port;
		rig.stop().await;
		assert!(started.elapsed() < Duration::from_secs(1));
		let mut out = Vec::new();
		tokio::time::timeout(Duration::from_secs(1), sock.read_to_end(&mut out))
			.await
			.expect("open connection closes on cancel")
			.ok();
		assert!(
			TcpStream::connect(("127.0.0.1", port)).await.is_err(),
			"listener is closed"
		);
	}
}
