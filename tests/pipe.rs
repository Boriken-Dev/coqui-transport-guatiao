//! The pipe transport as a built-in provider: found in a registry by its id, held to the kind's contract, and
//! configured through its settings.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

use std::path::Path;

use coqui_transport_guatiao::contract::{self, Contract, Heard};
use coqui_transport_guatiao::guatiao::library::Registry;
use coqui_transport_guatiao::guatiao::{Map, Status, Value};
use coqui_transport_guatiao::pipe::{self, PipeSettings};
use coqui_transport_guatiao::{DialRequest, Transport, settings};

fn registry() -> Registry {
    let mut registry = Registry::new("pipe-tests", "1.0");
    pipe::register(&mut registry).expect("the built-in registers");
    registry
}

fn pipe(registry: &Registry, settings: Map) -> impl Transport + use<> {
    registry
        .offer::<dyn Transport>("pipe")
        .expect("the built-in is offered by its id")
        .expect("its table is the kind")
        .instantiate(&Value::from(settings))
        .expect("the settings fit the schema")
}

/// An endpoint no other test or process holds.
fn endpoint(directory: &Path, name: &str) -> String {
    if cfg!(windows) {
        format!("pipe:coqui-tg-{}-{name}", std::process::id())
    } else {
        format!("unix:{}/{name}.sock", directory.display())
    }
}

#[test]
fn the_pipe_is_found_by_its_id_and_keeps_the_kinds_contract() {
    let registry = registry();
    let directory = tempfile::tempdir().expect("a directory");
    let transport = pipe(&registry, Map::new());
    let info = transport.describe().expect("described");
    assert_eq!(info.title, "Pipe");
    let suite = Contract::new(
        "the pipe",
        endpoint(directory.path(), "one"),
        endpoint(directory.path(), "two"),
    )
    .unreachable(endpoint(directory.path(), "nobody"));
    if let Err(failures) = suite.run(&transport) {
        panic!("{failures}");
    }
}

#[test]
fn a_second_registration_is_skipped_and_the_offer_stays() {
    let mut registry = registry();
    pipe::register(&mut registry).expect("skipped, not refused");
    assert_eq!(registry.offers::<dyn Transport>().count(), 1);
}

#[test]
fn the_schema_is_the_pipe_settings_and_a_misspelling_is_refused() {
    let registry = registry();
    let offer = registry
        .offer::<dyn Transport>("pipe")
        .expect("offered")
        .expect("the kind");
    assert_eq!(
        offer.config_schema().expect("a schema"),
        &settings::schema::<PipeSettings>().expect("described")
    );
    let mut channel = Map::new();
    channel.set("max_mesage", 5_u64);
    let mut misspelled = Map::new();
    misspelled.set("channel", Value::from(channel));
    assert!(offer.instantiate(&Value::from(misspelled)).is_err());
}

#[test]
fn a_configured_pipe_reports_what_differs_and_applies_it() {
    let registry = registry();
    let directory = tempfile::tempdir().expect("a directory");
    let mut channel = Map::new();
    channel.set("max_message", 64_u64);
    let mut map = Map::new();
    map.set("channel", Value::from(channel));
    let transport = pipe(&registry, map);
    let listed = transport.non_defaults().expect("answered");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].setting, "channel.max_message");
    assert_eq!(
        transport.describe().expect("described").max_message_bytes,
        64
    );

    // The bound is the channel's own: a message over it is refused to the sender, and the channel goes on.
    let (listener, told) =
        contract::listen(&transport, &endpoint(directory.path(), "bounded")).expect("listens");
    let (client, _from) = contract::dial(
        &transport,
        DialRequest::new(listener.address().expect("answers")),
    )
    .expect("dials");
    let server = contract::next_peer(&told);
    let error = client.send(&[0; 65]).expect_err("over the bound");
    assert_eq!(error.status, Status::GUATIAO_ERR_BAD_VALUE);
    client.send(&[7; 64]).expect("at the bound");
    assert_eq!(contract::next(&server.heard), Heard::Message(vec![7; 64]));
    client.close(0, "");
    listener.close(0, "");
}

#[test]
fn a_name_the_first_listener_holds_is_not_shared() {
    let registry = registry();
    let directory = tempfile::tempdir().expect("a directory");
    let transport = pipe(&registry, Map::new());
    let address = endpoint(directory.path(), "taken");
    let (first, _told) = contract::listen(&transport, &address).expect("listens");
    let error = contract::listen(&transport, &address)
        .map(|_| ())
        .expect_err("taken");
    assert_eq!(error.status, Status::GUATIAO_ERR_GONE);
    first.close(0, "");
}
