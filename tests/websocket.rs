//! The WebSocket transport as a built-in provider, over loopback.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

use coqui_transport_guatiao::contract::{self, Contract};
use coqui_transport_guatiao::guatiao::library::Registry;
use coqui_transport_guatiao::guatiao::{Map, Status, Value};
use coqui_transport_guatiao::websocket::{self, WebSocketSettings};
use coqui_transport_guatiao::{DialRequest, ListenRequest, Transport, TransportAcceptor, settings};

fn registry() -> Registry {
    let mut registry = Registry::new("websocket-tests", "1.0");
    websocket::register(&mut registry).expect("the built-in registers");
    registry
}

fn websocket(registry: &Registry, settings: Map) -> impl Transport + use<> {
    registry
        .offer::<dyn Transport>("websocket")
        .expect("the built-in is offered by its id")
        .expect("its table is the kind")
        .instantiate(&Value::from(settings))
        .expect("the settings fit the schema")
}

#[test]
fn the_websocket_is_found_by_its_id_and_keeps_the_kinds_contract() {
    let registry = registry();
    let transport = websocket(&registry, Map::new());
    assert_eq!(transport.describe().expect("described").title, "WebSocket");
    let suite = Contract::new("the websocket", "ws://127.0.0.1:0/", "ws://127.0.0.1:0/")
        .unreadable("not a url")
        .unreachable("ws://127.0.0.1:1/");
    if let Err(failures) = suite.run(&transport) {
        panic!("{failures}");
    }
}

#[test]
fn a_listener_reports_the_port_it_got_and_names_where_each_peer_came_from() {
    let registry = registry();
    let transport = websocket(&registry, Map::new());
    let (listener, told) = contract::listen(&transport, "ws://127.0.0.1:0/").expect("listens");
    let address = listener.address().expect("answers");
    assert!(
        address.starts_with("ws://127.0.0.1:") && !address.ends_with(":0"),
        "the system's port: {address}"
    );
    let (_client, _heard) = contract::dial(&transport, DialRequest::new(address)).expect("dials");
    let peer = contract::next_peer(&told);
    assert!(peer.address.starts_with("127.0.0.1:"), "{}", peer.address);
    listener.close(0, "");
}

#[test]
fn the_schema_is_the_websocket_settings_and_what_differs_is_listed() {
    let registry = registry();
    let offer = registry
        .offer::<dyn Transport>("websocket")
        .expect("offered")
        .expect("the kind");
    assert_eq!(
        offer.config_schema().expect("a schema"),
        &settings::schema::<WebSocketSettings>().expect("described")
    );
    let mut listen = Map::new();
    listen.set("plain_beyond_loopback", true);
    let mut map = Map::new();
    map.set("listen", Value::from(listen));
    let transport = websocket(&registry, map);
    let listed = transport.non_defaults().expect("answered");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].setting, "listen.plain_beyond_loopback");
}

#[test]
fn tls_and_beyond_loopback_are_refused_unless_said() {
    let registry = registry();
    let transport = websocket(&registry, Map::new());
    let (sink, _heard) = contract::sink();
    let error = transport
        .dial(DialRequest::new("wss://127.0.0.1:1/"), sink)
        .expect_err("no TLS here");
    assert_eq!(error.status, Status::GUATIAO_ERR_BAD_VALUE);
    let (sink, _heard) = contract::sink();
    let error = transport
        .dial(DialRequest::new("ws://192.0.2.1:1/").timeout_ms(200), sink)
        .expect_err("plain beyond loopback");
    assert_eq!(error.status, Status::GUATIAO_ERR_GONE);
    let (sender, _receiver) = std::sync::mpsc::channel();
    let error = transport
        .listen(
            ListenRequest::new("ws://0.0.0.0:0/"),
            contract::Accepts(sender).into_object(),
        )
        .map(|_| ())
        .expect_err("plain beyond loopback");
    assert_eq!(error.status, Status::GUATIAO_ERR_GONE);
}
