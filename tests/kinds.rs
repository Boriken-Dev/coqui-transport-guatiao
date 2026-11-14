//! The contract other programs are built against: the kinds' hashes and description, and the models' keys and
//! schemas. A provider built against another table is refused at load, and one that sends a key a request model
//! does not declare is refused, so each of these changes only in a commit that means to break providers.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

use std::path::PathBuf;

use coqui_transport_guatiao::guatiao::Schema;
use coqui_transport_guatiao::{
    ChannelSinkVtable, ChannelVtable, DialRequest, ListenRequest, NonDefault,
    TransportAcceptorVtable, TransportInfo, TransportListenerVtable, TransportVtable,
};
use guatiao_serde::Serializable;
use serde_json::Value as Json;

fn file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn pretty(value: &Json) -> String {
    let mut text = serde_json::to_string_pretty(value).expect("written");
    text.push('\n');
    text
}

/// Compares `fresh` with the committed file, writing it first when asked to.
fn committed(path: &str, fresh: &str) {
    let path = file(path);
    if std::env::var_os("COQUI_TRANSPORT_GUATIAO_WRITE_KINDS").is_some() {
        std::fs::write(&path, fresh).expect("the file is written");
    }
    let committed = std::fs::read_to_string(&path).expect("the file is committed");
    assert!(committed == fresh, "regenerate {}", path.display());
}

/// The kinds a provider in another language needs: `transport`, and every object kind its slots name.
fn rendered() -> String {
    let document = format!(
        "{{\"kinds\":[{},{},{},{},{}]}}",
        TransportVtable::description(),
        TransportListenerVtable::description(),
        TransportAcceptorVtable::description(),
        ChannelVtable::description(),
        ChannelSinkVtable::description()
    );
    pretty(&serde_json::from_str(&document).expect("a kind's description is JSON"))
}

/// Regenerate with
/// `COQUI_TRANSPORT_GUATIAO_WRITE_KINDS=1 cargo test --test kinds`.
#[test]
fn the_committed_description_is_what_this_build_describes() {
    committed("include/coqui_transport_guatiao.kinds.json", &rendered());
}

fn schema_of<T: Schema>() -> Json {
    let value = T::schema().expect("a model describes itself");
    serde_json::to_value(Serializable::from(&value)).expect("a schema is JSON")
}

fn schemas() -> Vec<(&'static str, Json)> {
    vec![
        ("TransportInfo", schema_of::<TransportInfo>()),
        ("DialRequest", schema_of::<DialRequest>()),
        ("ListenRequest", schema_of::<ListenRequest>()),
        ("NonDefault", schema_of::<NonDefault>()),
    ]
}

#[test]
fn the_committed_schemas_are_what_this_build_describes() {
    let document: serde_json::Map<String, Json> = schemas()
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), schema))
        .collect();
    committed(
        "include/coqui_transport_guatiao.schemas.json",
        &pretty(&Json::Object(document)),
    );
}

/// Adding, removing or renaming a key of a model breaks providers already built. This names every key, and
/// which are required, so changing one is a decision.
#[test]
fn every_models_keys_are_pinned() {
    let pinned: [(&str, &[&str], &[&str]); 4] = [
        (
            "TransportInfo",
            &["dials", "endpoint", "listens", "max_message_bytes", "title"],
            &["dials", "endpoint", "listens", "max_message_bytes", "title"],
        ),
        ("DialRequest", &["endpoint", "timeout_ms"], &["endpoint"]),
        ("ListenRequest", &["endpoint"], &["endpoint"]),
        (
            "NonDefault",
            &["default", "setting", "value"],
            &["default", "setting", "value"],
        ),
    ];
    let schemas = schemas();
    for (name, keys, required) in pinned {
        let schema = &schemas
            .iter()
            .find(|(model, _)| *model == name)
            .expect("a model is listed")
            .1;
        let mut found: Vec<&str> = schema["properties"]
            .as_object()
            .expect("a model has properties")
            .keys()
            .map(String::as_str)
            .collect();
        found.sort_unstable();
        assert_eq!(found, keys, "{name}'s keys");
        let mut needed: Vec<&str> = schema["required"]
            .as_array()
            .map(|list| list.iter().filter_map(Json::as_str).collect())
            .unwrap_or_default();
        needed.sort_unstable();
        assert_eq!(needed, required, "{name}'s required keys");
    }
}

/// A library built against another table is refused at load: change these only to break every provider built
/// before.
#[test]
fn the_kinds_floor_hashes_are_pinned() {
    assert_eq!(
        [
            TransportVtable::FLOOR_HASH,
            TransportListenerVtable::FLOOR_HASH,
            TransportAcceptorVtable::FLOOR_HASH,
        ],
        [2_030_380_456, 2_298_448_771, 2_525_300_295]
    );
}
