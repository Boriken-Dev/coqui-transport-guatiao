//! A transport in a native library, loaded through the registry from its file: the provider's id, the kind's
//! table and the schema all cross the C ABI, and so do the connections.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

use std::path::{Path, PathBuf};

use coqui_transport_guatiao::Transport;
use coqui_transport_guatiao::contract::Contract;
use coqui_transport_guatiao::guatiao::library::Registry;
use coqui_transport_guatiao::guatiao::{Map, Value};

fn library() -> PathBuf {
    let tests = std::env::current_exe().expect("the test knows where it is");
    let profile = tests
        .parent()
        .and_then(Path::parent)
        .expect("a test binary is in target/<profile>/deps");
    profile.join("examples").join(format!(
        "{}pipe_library{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

fn endpoint(directory: &Path, name: &str) -> String {
    if cfg!(windows) {
        format!("pipe:coqui-tg-lib-{}-{name}", std::process::id())
    } else {
        format!("unix:{}/{name}.sock", directory.display())
    }
}

#[test]
fn a_library_offers_its_transport_to_a_host_that_only_has_the_file() {
    let mut registry = Registry::new("library-tests", "1.0");
    registry
        .load_file(&library())
        .expect("the library loads once cargo built it (`cargo build --examples --features pipe`)");
    let offer = registry
        .offer::<dyn Transport>("pipe")
        .expect("the library offers the id the provider declares")
        .expect("its table is this crate's kind");
    assert!(offer.config_schema().is_some());
    let transport = offer
        .instantiate(&Value::from(Map::new()))
        .expect("an instance from the library");
    let directory = tempfile::tempdir().expect("a directory");
    let suite = Contract::new(
        "the pipe library",
        endpoint(directory.path(), "one"),
        endpoint(directory.path(), "two"),
    )
    .unreachable(endpoint(directory.path(), "nobody"));
    if let Err(failures) = suite.run(&transport) {
        panic!("{failures}");
    }
}
