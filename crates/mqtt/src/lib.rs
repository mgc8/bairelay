pub mod client;
pub mod control;
pub mod discovery;
pub mod error;
pub mod status;
pub mod test_support;
pub mod topics;

pub use client::{connect, ClientAuth, MqttConfig, MqttEventLoop, SharedMqttClient};
pub use control::{parse_control_message, ControlCommand};
pub use discovery::{CameraEnableFlags, DiscoveryPublisher};
pub use error::MqttError;
pub use status::StatusPublisher;

// Re-export rumqttc types needed by the event loop consumer + tests.
pub use rumqttc::{ConnAck, ConnectReturnCode, Event, Outgoing, Packet, Publish, QoS};
