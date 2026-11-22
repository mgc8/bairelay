use super::bc::model::{Bc, BcXml};
use crate::NomErrorType;
use thiserror::Error;

/// This is the primary error type of the library
#[derive(Debug, Error, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
	/// Underlying IO errors
	#[error("IO Error: {:?}", _0)]
	Io(#[from] std::sync::Arc<std::io::Error>),

	/// Raised when fails to parse time from the camera
	#[error("Error in time coversion: {:?}", _0)]
	TimeRange(#[from] time::error::ComponentRange),

	/// Raised when fails to parse time from the camera
	#[error("Error in time parsing")]
	TimeParse,

	/// Raised when fails to parse time from the camera
	#[error("Error in try from NonZeroInt")]
	TryFromInt(#[from] std::num::TryFromIntError),

	/// /// Raised when fails to parse time from the camera
	#[error("Error in time conversion")]
	TimeTryFrom(#[from] time::error::TryFromParsed),

	/// Raised when a Bc reply was not understood
	#[error("unexpected camera reply: {why}")]
	UnintelligibleReply {
		/// The Bc packet that was not understood
		reply: std::sync::Arc<Bc>,
		/// The message attached to the error
		why: &'static str,
	},

	/// Raised when a BcXml reply was not understood
	#[error("unexpected camera reply: {why}")]
	UnintelligibleXml {
		/// The Bc packet that was not understood
		reply: std::sync::Arc<BcXml>,
		/// The message attached to the error
		why: &'static str,
	},

	/// Raised when the camera responds with a status code over than OK
	#[error("Camera responded with Service Unavailable: Msg of type {id} returned code {code}")]
	CameraServiceUnavailable {
		/// The message ID
		id: u32,
		/// The return code this is usually
		/// 200 for OK
		/// 400 for not yet ready
		/// 500 for camera cannot comply or understand
		code: u16,
	},

	/// Refusal of a **settings read**, together with what the camera wrote in
	/// answer.
	///
	/// Twin of [`Error::CameraServiceUnavailable`] extended with `detail`: the text of
	/// the block received in answer to the refusal, where the twin returns only the
	/// code. It is not one more field on the twin - its thirty-two construction sites
	/// mostly have no XML block at hand (stream, login, PTZ, snapshot), and imposing
	/// `detail: String::new()` everywhere to serve one family of commands would pay the
	/// orchestra for one instrument.
	/// soliste.
	///
	/// The `detail` is **already bounded** (see `cut` on the settings side); it is
	/// never published as it arrives, `publishable_reason` handles that higher up.
	#[error(
		"Camera refused msg {id} with code {code}: {detail}"
	)]
	CameraRefused {
		/// The message ID
		id: u32,
		/// The return code — same scale as [`Self::CameraServiceUnavailable`]
		code: u16,
		/// What the camera answered, one line, bounded
		detail: String,
	},

	/// Raised when the camera responds with a status code over than OK during login
	#[error("Camera responded with Err during login")]
	CameraLoginFail,

	/// Raised when a connection is dropped.
	#[error("Dropped connection")]
	DroppedConnection,

	/// Raised when a connection is dropped during a tokio mpsc TryRecv event
	#[error("Dropped connection (TryRecv)")]
	DroppedConnectionTry(#[from] tokio::sync::mpsc::error::TryRecvError),

	/// Raised when a connection is dropped during a TryRecv event
	#[error("Dropped connection (Broadcast TryRecv)")]
	BroadcastDroppedConnectionTry(#[from] tokio::sync::broadcast::error::TryRecvError),

	/// Raised when a stream thread has finished
	#[error("End of Stream")]
	StreamFinished,

	/// Raised when a connection requests shutdown
	#[error("Connection shutting down")]
	ConnectionShutdown,

	/// Raised when a discovery attempt fails to get a reply
	#[error("No reply to discovery packet")]
	DiscoveryIgnored,

	/// Raised when there is no reply to a UDP packet
	#[error("BcUDP packet timeout")]
	BcUdpTimeout,

	/// Raised when a BcUdp incomming connection is dropped
	#[error("BcUDP receiver dropped: {0:?}")]
	BcUdpDropReceiver(BcUdpDropReceiverKind),

	/// Raised when a BcUdp outgoing connection is dropped
	#[error("BcUDP sender dropped")]
	BcUdpDropSender,

	/// Raised when a BcUdp outgoing connection is dropped
	#[error("BcUDPPayload inner protocol was dropped")]
	BcUdpPayloadDroppedInner,

	/// Raised when BcUdp sender fails to reconnect in time
	#[error("BcUDP reconnect timeout")]
	BcUdpReconnectTimeout,

	/// Raised when a connection is dropped during a TryRecv event
	#[error("Send Error")]
	TokioBcSendError,

	/// Raised when the TIMEOUT is reach
	#[error("Timeout")]
	Timeout(#[from] std::sync::Arc<tokio::time::error::Elapsed>),

	/// Raised when a timeout fails in a non standard way such as timeout during shutdown
	#[error("TimeoutError")]
	TimeoutError(#[from] tokio::time::error::Error),

	/// Raised when connection is dropped because the timeout is reach
	#[error("Dropped connection (Timeout)")]
	TimeoutDisconnected,

	/// Raised when a camera cannot be connected to ay any of the given addresses
	#[error("Cannot contact camera at given address")]
	CannotInitCamera,

	/// Raised when failed to login to the camera
	#[error("Credential error")]
	AuthFailed,

	/// Raised when the given camera url could not be resolved
	#[error("Failed to translate camera address")]
	AddrResolutionError,

	/// Raised non adpcm data is sent to the talk command
	#[error("Talk data is not ADPCM")]
	UnknownTalkEncoding,

	/// `add_user` was asked to create a user that already exists. The
	/// camera would reject the eventual SET, but catching it client-
	/// side gives callers a typed handle instead of `Error::Other`.
	#[error("user '{user_name}' already exists on the camera")]
	UserAlreadyExists {
		/// Name of the user the caller tried to add.
		user_name: String,
	},

	/// `modify_user` / `delete_user` was asked to operate on a user
	/// that doesn't exist on the camera.
	#[error("user '{user_name}' not found on the camera")]
	UserNotFound {
		/// Name of the user the caller tried to modify or delete.
		user_name: String,
	},

	/// Raised when dicovery times out waiting for a reply
	#[error("Timed out while waiting for camera reply")]
	DiscoveryTimeout,

	/// Raised when every P2P relay refused or failed a registration
	/// round. Carries a per-relay summary so the caller can name the
	/// unreachable servers in one log line.
	#[error("no relay accepted registration — {0}")]
	DiscoveryNoRelay(String),

	/// Raised during a (de)seralisation error
	#[error("Cookie GenError")]
	GenError(#[from] std::sync::Arc<cookie_factory::GenError>),

	/// Raised when a connection is subscribed to more than once for msg_num
	#[error("Simultaneous subscription, {msg_num:?}")]
	SimultaneousSubscription {
		/// The message number that was subscribed to
		msg_num: Option<u16>,
	},

	/// Raised when a connection is subscribed to more than once for msg_id
	#[error("Simultaneous subscription, {msg_id}")]
	SimultaneousSubscriptionId {
		/// The message number that was subscribed to
		msg_id: u32,
	},

	/// Raised when a new encyrption byte is observed
	#[error("Unknown encryption: {0:x?}")]
	UnknownEncryption(usize),

	/// Raised when the camera cannot be found
	#[error("Camera Not Findable")]
	ConnectionUnavailable,

	/// Raised when the subscription id dropped too soon
	#[error("Dropped Subscriber")]
	DroppedSubscriber,

	/// Raised when a unknown connection ID attempts to connect with us over UDP
	#[error("Connection with unknown connectionID: {0:?}")]
	UnknownConnectionId(i32),

	/// Raised when a unknown SocketAddr attempts to connect with us over UDP
	#[error("Connection from unknown source: {0:?}")]
	UnknownSource(std::net::SocketAddr),

	/// Raised when the IP/Hostname cannot be understood
	#[error("Could not parse as IP")]
	AddrParseError(#[from] std::net::AddrParseError),

	/// Raised when a relay connection is not possible
	/// usually happens if the camera has not contacted reolink yet
	#[error("Cannot perform relay connection with this camera")]
	NoDmap,

	/// Raised when a dev connection is not possible
	/// usually happens if the camera has not contacted reolink yet
	#[error("Cannot perform lookup with this camera against reolink servers")]
	NoDev,

	/// Raised when a discovery fails to be accepted by the register
	#[error("Register refuses to accept us")]
	RegisterError,

	/// Raised when a the relay terminates the connection by sending a R2C_DISC
	#[error("Relay terminated the connection")]
	RelayTerminate,

	/// Raised when a the camera terminates the connection by sending a D2C_DISC
	#[error("Camera terminated the connection")]
	CameraTerminate,

	/// Raised when the stream is not enough to complete a message
	#[error("Nom Parsing incomplete: {0}")]
	NomIncomplete(usize),

	/// Raised when a stream cannot be decoded
	#[error("Nom Parsing error: {0}")]
	NomError(String),

	/// Raised when a camera/user lacks an ability
	#[error("camera does not support '{name}': requested {requested} permission, has {actual}")]
	MissingAbility {
		/// Name of the ability
		name: String,
		/// Requested permission (read/write)
		requested: String,
		/// Actual permission (read/write/none)
		actual: String,
	},

	/// Raised when a thread panics
	#[error("Thread panicked")]
	JoinError(#[from] std::sync::Arc<tokio::task::JoinError>),

	/// A generic catch all error
	#[error("Other error: {0}")]
	Other(&'static str),

	/// The caller asked for something the protocol forbids — a tag the
	/// camera never printed, an id it never listed. Nothing went on the
	/// wire. Not a camera fault and not a network fault: the caller fixes
	/// it by not asking. (The chime family raises this from code that must
	/// phrase the refusal per tag, so the text is dynamic; `Other` is
	/// `&'static str` and cannot carry that.)
	#[error("Usage error: {0}")]
	Usage(String),

	/// Cloud ("account camera") bundle minting against apis.reolink.com failed.
	#[error("Cloud bundle mint failed: {0}")]
	Cloud(String),
}

#[derive(Debug, Clone)]
pub enum BcUdpDropReceiverKind {
	NoneReceived,
	SendFailed(String),
}

impl From<std::io::Error> for Error {
	fn from(k: std::io::Error) -> Self {
		// Check for other error that is already an Error of this type
		if k.get_ref()
			.is_some_and(|e| e.downcast_ref::<Error>().is_some())
		{
			*k.into_inner().unwrap().downcast::<Error>().unwrap()
		} else {
			Error::Io(std::sync::Arc::new(k))
		}
	}
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
	fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
		Error::TokioBcSendError
	}
}

impl<T> From<tokio_util::sync::PollSendError<T>> for Error {
	fn from(_: tokio_util::sync::PollSendError<T>) -> Self {
		Error::TokioBcSendError
	}
}

impl From<cookie_factory::GenError> for Error {
	fn from(k: cookie_factory::GenError) -> Self {
		Error::GenError(std::sync::Arc::new(k))
	}
}

impl From<tokio::task::JoinError> for Error {
	fn from(k: tokio::task::JoinError) -> Self {
		Error::JoinError(std::sync::Arc::new(k))
	}
}

impl From<tokio::time::error::Elapsed> for Error {
	fn from(k: tokio::time::error::Elapsed) -> Self {
		Error::Timeout(std::sync::Arc::new(k))
	}
}

impl<'a> From<nom::Err<NomErrorType<'a>>> for Error {
	fn from(k: nom::Err<NomErrorType<'a>>) -> Self {
		match k {
			nom::Err::Error(e) => Error::NomError(format!("Nom Error: {:X?}", e)),
			nom::Err::Failure(e) => Error::NomError(format!("Nom Error: {:X?}", e)),
			nom::Err::Incomplete(nom::Needed::Size(amount)) => Error::NomIncomplete(amount.get()),
			nom::Err::Incomplete(nom::Needed::Unknown) => Error::NomIncomplete(1),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc::model::{Bc, BcMeta};
	use crate::bc::xml::BcXml;
	use std::sync::Arc;

	fn bare_bc() -> Bc {
		Bc::new_from_meta(BcMeta {
			msg_id: 0,
			channel_id: 0,
			stream_type: 0,
			response_code: 0,
			msg_num: 0,
			class: 0,
		})
	}

	#[test]
	fn unintelligible_reply_display_surfaces_why() {
		// Regression: the #[error] attribute used to be literally
		// "Communication error", which swallowed the `why` field and
		// produced a useless message on non-PTZ cameras.
		let err = Error::UnintelligibleReply {
			reply: Arc::new(bare_bc()),
			why: "the camera did not return a valid PtzPreset xml",
		};
		assert_eq!(
			format!("{err}"),
			"unexpected camera reply: the camera did not return a valid PtzPreset xml"
		);
	}

	#[test]
	fn unintelligible_xml_display_surfaces_why() {
		let err = Error::UnintelligibleXml {
			reply: Arc::new(BcXml::default()),
			why: "missing expected field",
		};
		assert_eq!(
			format!("{err}"),
			"unexpected camera reply: missing expected field"
		);
	}

	#[test]
	fn missing_ability_display_is_operator_friendly() {
		// Regression: the #[error] attribute used to read like an
		// internal protocol bug ("Missing ability: ledState with read
		// permission has only none"); the common case is actually
		// "this model does not expose that feature", which is why the
		// CLI routes it to a dedicated EXIT_UNSUPPORTED.
		let err = Error::MissingAbility {
			name: "ledState".into(),
			requested: "read".into(),
			actual: "none".into(),
		};
		assert_eq!(
			format!("{err}"),
			"camera does not support 'ledState': requested read permission, has none"
		);
	}

	#[test]
	fn display_simple_variants() {
		// Unit-style variants have fixed Display strings — lock them down
		// so changes are intentional.
		assert_eq!(format!("{}", Error::TimeParse), "Error in time parsing");
		assert_eq!(
			format!("{}", Error::CameraLoginFail),
			"Camera responded with Err during login"
		);
		assert_eq!(
			format!("{}", Error::DroppedConnection),
			"Dropped connection"
		);
		assert_eq!(format!("{}", Error::StreamFinished), "End of Stream");
		assert_eq!(
			format!("{}", Error::ConnectionShutdown),
			"Connection shutting down"
		);
		assert_eq!(
			format!("{}", Error::DiscoveryIgnored),
			"No reply to discovery packet"
		);
		assert_eq!(format!("{}", Error::BcUdpTimeout), "BcUDP packet timeout");
		assert_eq!(
			format!("{}", Error::BcUdpDropSender),
			"BcUDP sender dropped"
		);
		assert_eq!(
			format!("{}", Error::BcUdpPayloadDroppedInner),
			"BcUDPPayload inner protocol was dropped"
		);
		assert_eq!(
			format!("{}", Error::BcUdpReconnectTimeout),
			"BcUDP reconnect timeout"
		);
		assert_eq!(format!("{}", Error::TokioBcSendError), "Send Error");
		assert_eq!(
			format!("{}", Error::TimeoutDisconnected),
			"Dropped connection (Timeout)"
		);
		assert_eq!(
			format!("{}", Error::CannotInitCamera),
			"Cannot contact camera at given address"
		);
		assert_eq!(format!("{}", Error::AuthFailed), "Credential error");
		assert_eq!(
			format!("{}", Error::AddrResolutionError),
			"Failed to translate camera address"
		);
		assert_eq!(
			format!("{}", Error::UnknownTalkEncoding),
			"Talk data is not ADPCM"
		);
		assert_eq!(
			format!("{}", Error::DiscoveryTimeout),
			"Timed out while waiting for camera reply"
		);
		assert_eq!(
			format!(
				"{}",
				Error::DiscoveryNoRelay("p2p.reolink.com (query failed: Timeout)".to_string())
			),
			"no relay accepted registration — p2p.reolink.com (query failed: Timeout)"
		);
		assert_eq!(
			format!("{}", Error::ConnectionUnavailable),
			"Camera Not Findable"
		);
		assert_eq!(
			format!("{}", Error::DroppedSubscriber),
			"Dropped Subscriber"
		);
		assert_eq!(
			format!("{}", Error::NoDmap),
			"Cannot perform relay connection with this camera"
		);
		assert_eq!(
			format!("{}", Error::NoDev),
			"Cannot perform lookup with this camera against reolink servers"
		);
		assert_eq!(
			format!("{}", Error::RegisterError),
			"Register refuses to accept us"
		);
		assert_eq!(
			format!("{}", Error::RelayTerminate),
			"Relay terminated the connection"
		);
		assert_eq!(
			format!("{}", Error::CameraTerminate),
			"Camera terminated the connection"
		);
	}

	#[test]
	fn display_data_variants() {
		assert_eq!(
			format!("{}", Error::CameraServiceUnavailable { id: 33, code: 400 }),
			"Camera responded with Service Unavailable: Msg of type 33 returned code 400"
		);
		assert_eq!(
			format!("{}", Error::SimultaneousSubscription { msg_num: Some(7) }),
			"Simultaneous subscription, Some(7)"
		);
		assert_eq!(
			format!("{}", Error::SimultaneousSubscription { msg_num: None }),
			"Simultaneous subscription, None"
		);
		assert_eq!(
			format!("{}", Error::SimultaneousSubscriptionId { msg_id: 12 }),
			"Simultaneous subscription, 12"
		);
		assert_eq!(
			format!("{}", Error::UnknownEncryption(0xab)),
			"Unknown encryption: ab"
		);
		assert_eq!(
			format!("{}", Error::UnknownConnectionId(-5)),
			"Connection with unknown connectionID: -5"
		);
		let addr: std::net::SocketAddr = "192.168.1.1:9000".parse().unwrap();
		assert_eq!(
			format!("{}", Error::UnknownSource(addr)),
			format!("Connection from unknown source: {:?}", addr)
		);
		assert_eq!(
			format!("{}", Error::NomIncomplete(16)),
			"Nom Parsing incomplete: 16"
		);
		assert_eq!(
			format!("{}", Error::NomError("xyz".into())),
			"Nom Parsing error: xyz"
		);
		assert_eq!(format!("{}", Error::Other("boom")), "Other error: boom");
		assert_eq!(
			format!(
				"{}",
				Error::BcUdpDropReceiver(BcUdpDropReceiverKind::NoneReceived)
			),
			"BcUDP receiver dropped: NoneReceived"
		);
		assert_eq!(
			format!(
				"{}",
				Error::BcUdpDropReceiver(BcUdpDropReceiverKind::SendFailed("send failed".into()))
			),
			"BcUDP receiver dropped: SendFailed(\"send failed\")"
		);
	}

	#[test]
	fn display_wrapped_variants() {
		let io: std::io::Error = std::io::Error::other("bad");
		let io_err: Error = io.into();
		assert!(format!("{}", io_err).starts_with("IO Error:"));

		let int_err: std::num::TryFromIntError = u8::try_from(-1_i32).unwrap_err();
		let err: Error = int_err.into();
		assert_eq!(format!("{}", err), "Error in try from NonZeroInt");

		let gen: Error = cookie_factory::GenError::BufferTooSmall(1).into();
		assert_eq!(format!("{}", gen), "Cookie GenError");

		let (_tx, mut rx) = tokio::sync::mpsc::channel::<u8>(1);
		rx.close();
		let send_err_result: Error = tokio::sync::mpsc::error::TryRecvError::Disconnected.into();
		assert!(format!("{}", send_err_result).starts_with("Dropped connection"));

		let bcast_err: Error = tokio::sync::broadcast::error::TryRecvError::Empty.into();
		assert!(format!("{}", bcast_err).starts_with("Dropped connection"));

		// Addr parse error
		let parse_err: Result<std::net::IpAddr, _> = "not-an-ip".parse();
		let addr_err: Error = parse_err.unwrap_err().into();
		assert_eq!(format!("{}", addr_err), "Could not parse as IP");
	}

	#[test]
	fn io_error_unwraps_nested_error() {
		// The From<io::Error> impl unwraps a nested Error back out.
		let inner = Error::AuthFailed;
		let io_err = std::io::Error::other(inner);
		let out: Error = io_err.into();
		match out {
			Error::AuthFailed => {}
			other => panic!("expected AuthFailed, got {:?}", other),
		}
	}

	#[test]
	fn mpsc_send_error_flattens_to_send_error() {
		let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
		drop(rx);
		let err = tx.try_send(1).unwrap_err();
		// Manually convert through SendError-shaped path
		let send_err: tokio::sync::mpsc::error::SendError<u8> =
			tokio::sync::mpsc::error::SendError(2);
		let _ = err;
		let converted: Error = send_err.into();
		assert!(matches!(converted, Error::TokioBcSendError));
	}

	#[test]
	fn name_error_conversion_branches() {
		use nom::Needed;
		// Incomplete(Size) → NomIncomplete(n)
		let e: Error = nom::Err::Incomplete::<NomErrorType>(Needed::new(42)).into();
		assert!(matches!(e, Error::NomIncomplete(42)));
		// Incomplete(Unknown) → NomIncomplete(1)
		let e: Error = nom::Err::Incomplete::<NomErrorType>(Needed::Unknown).into();
		assert!(matches!(e, Error::NomIncomplete(1)));
		// Error(_) → NomError
		let raw: &[u8] = &[];
		let e: Error = nom::Err::Error::<NomErrorType>(nom::error::VerboseError {
			errors: vec![(
				raw,
				nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Tag),
			)],
		})
		.into();
		assert!(matches!(e, Error::NomError(_)));
		// Failure(_) → NomError
		let e: Error = nom::Err::Failure::<NomErrorType>(nom::error::VerboseError {
			errors: vec![(
				raw,
				nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Tag),
			)],
		})
		.into();
		assert!(matches!(e, Error::NomError(_)));
	}

	#[tokio::test]
	async fn join_error_and_elapsed_conversions() {
		// JoinError (from a panicking task)
		let handle: tokio::task::JoinHandle<()> = tokio::spawn(async { panic!("test panic") });
		let je = handle.await.unwrap_err();
		let e: Error = je.into();
		assert!(matches!(e, Error::JoinError(_)));

		// Elapsed (from tokio::time::timeout on a never-ending future)
		let el = tokio::time::timeout(
			std::time::Duration::from_millis(1),
			std::future::pending::<()>(),
		)
		.await
		.unwrap_err();
		let e: Error = el.into();
		assert!(matches!(e, Error::Timeout(_)));
	}

	#[tokio::test]
	async fn poll_send_error_flattens() {
		// PollSendError → TokioBcSendError
		// Close the receiver, then poll_reserve returns a PollSendError.
		use futures::future::poll_fn;
		let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
		drop(rx);
		let mut ps = tokio_util::sync::PollSender::new(tx);
		let pse = poll_fn(|cx| ps.poll_reserve(cx))
			.await
			.expect_err("reserve should fail after rx drop");
		let e: Error = pse.into();
		assert!(matches!(e, Error::TokioBcSendError));
	}
}
