//! `coqui-transport-web`'s WebSocket as a provider of the `transport` kind, id `websocket`.
//!
//! Endpoints are `ws://HOST:PORT/PATH`. A configuration is data, so this provider carries no TLS: it
//! reaches and serves plain WebSocket on loopback, and beyond it only when `dial.plain_beyond_loopback` or
//! `listen.plain_beyond_loopback` says so, which `non_defaults` then lists. A listener's peer is the
//! address its connection came from. A message is one WebSocket message, text by default (`frames`).

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use core::net::{IpAddr, SocketAddr};
use core::time::Duration;

use coqui_builder::{Describe, Partial};
use coqui_transport::{Channel, Connector as _, Failure};
use coqui_transport_web::websocket::{
    Connector, Dialer, DialerSettings, Listener, ListenerSettings, ServerTls,
};
use url::Url;

use crate::driver::Accepted;
use crate::guatiao;
use crate::{Acceptor, Driver, DriverError, Refusal, TransportInfo};

/// The settings of the WebSocket transport: one group for each thing it does.
#[derive(Clone, Debug, Default, PartialEq, Eq, Describe)]
#[map(deny_unknown_fields)]
pub struct WebSocketSettings {
    /// Making a connection: the wire, the limits and whether plain WebSocket may leave loopback.
    #[map(default = DialerSettings::default())]
    pub dial: DialerSettings,
    /// Listening: the wire, the limits, the origins it admits and whether plain WebSocket may leave loopback.
    #[map(default = ListenerSettings::default())]
    pub listen: ListenerSettings,
}

/// The WebSocket transport as a [`Driver`].
#[derive(Debug, Clone, Copy)]
pub struct WebSocketDriver;

fn endpoint_url(endpoint: &str) -> Result<Url, DriverError> {
    let url = Url::parse(endpoint)
        .map_err(|_| DriverError::new(Refusal::Endpoint, "the endpoint is not a URL"))?;
    if url.scheme() != "ws" {
        return Err(DriverError::new(
            Refusal::Endpoint,
            "the endpoint is not a ws:// URL: this provider carries no TLS",
        ));
    }
    Ok(url)
}

impl Driver for WebSocketDriver {
    type Settings = WebSocketSettings;

    fn describe(settings: &WebSocketSettings) -> TransportInfo {
        TransportInfo::new("WebSocket", "ws://HOST:PORT/PATH, plain WebSocket").max_message_bytes(
            settings
                .dial
                .limits
                .max_message
                .min(settings.listen.limits.max_message) as u64,
        )
    }

    fn dial(
        settings: &WebSocketSettings,
        endpoint: &str,
        timeout: Duration,
    ) -> Result<Box<dyn Channel>, DriverError> {
        let url = endpoint_url(endpoint)?;
        let dialer: Dialer = Dialer::builder()
            .fill(Partial::from(settings.dial))
            .build()
            .map_err(|error| DriverError::new(Refusal::Failed, error.to_string()))?;
        let channel = Connector::new(dialer, url).connect(timeout)?;
        Ok(Box::new(channel))
    }

    fn listen(
        settings: &WebSocketSettings,
        endpoint: &str,
    ) -> Result<Box<dyn Acceptor>, DriverError> {
        let url = endpoint_url(endpoint)?;
        let host = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .ok_or_else(|| {
                DriverError::new(Refusal::Endpoint, "the endpoint's host is not an address")
            })?;
        let port = url.port().unwrap_or(80);
        let listener = Listener::builder(SocketAddr::new(host, port), ServerTls::Plain)
            .fill(Partial::from(settings.listen.clone()))
            .build()
            .map_err(|error| match error {
                coqui_builder::builder::BuildError::Failed(failure) => DriverError::from(failure),
                other => DriverError::new(Refusal::Failed, other.to_string()),
            })?;
        Ok(Box::new(Accepting { listener }))
    }
}

struct Accepting {
    listener: Listener,
}

impl Acceptor for Accepting {
    fn address(&self) -> String {
        match self.listener.local_addr() {
            Ok(address) => format!("ws://{address}"),
            Err(_) => String::new(),
        }
    }

    fn accept(&self, timeout: Duration) -> Result<Option<Accepted>, DriverError> {
        let pending = match self.listener.next(Some(timeout)) {
            Ok(pending) => pending,
            Err(Failure::Timeout) => return Ok(None),
            Err(failure) => return Err(failure.into()),
        };
        let peer = pending.peer_addr().to_string();
        // One peer's failed handshake is that peer's, not the listener's.
        match pending.handshake() {
            Ok(channel) => Ok(Some((Box::new(channel), peer))),
            Err(_) => Ok(None),
        }
    }
}

crate::transport_provider! {
    /// `coqui-transport-web`'s WebSocket as a provider of the `transport` kind, id `websocket`.
    pub struct WebSocketProvider for WebSocketDriver, id = "websocket";
}

guatiao::local_providers!(
    id = "coqui-transport-websocket",
    version = env!("CARGO_PKG_VERSION"),
    providers = [WebSocketProvider]
);

/// Offers the `websocket` provider to `registry`, as a library linked into this binary named
/// `coqui-transport-websocket`. A second call is skipped, not an error.
///
/// # Errors
///
/// When the registry cannot take the library.
pub fn register(
    registry: &mut guatiao::library::Registry,
) -> Result<(), guatiao::library::LoadError> {
    registry
        .register_local("coqui-transport-websocket", library)
        .map(drop)
}
