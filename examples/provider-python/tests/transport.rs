//! The `transport` provider written in Python, `transport/memory_transport.py`, loaded in this process through
//! guatiao's source of Python libraries and called as the Rust-declared kind: it holds to the contract every
//! provider of the kind is held to, and its settings are checked against the schema the source derives.
//!
//! Where build.rs found no Python the test left is ignored, and a run counts it as not run.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

#[cfg(not(python_ready))]
#[test]
#[ignore = "NOT RUN: build.rs found no Python to embed, and said why in a warning"]
fn the_python_transport_needs_a_python() {
    panic!("build.rs found no Python to embed: set COQUI_TRANSPORT_TEST_PYTHON to one");
}

#[cfg(python_ready)]
mod support;

#[cfg(python_ready)]
mod with_python {
    use std::path::PathBuf;
    use std::process::Command;

    use coqui_transport_guatiao::Transport;
    use coqui_transport_guatiao::contract::Contract;
    use coqui_transport_guatiao::guatiao::library::Registry;
    use coqui_transport_guatiao::guatiao::{List, Map, Value};

    use super::support::{guatiao, manifest_dir, python, source};

    /// The repository's `python/` directory, where the kinds' module is.
    fn kinds() -> PathBuf {
        manifest_dir().join("../../python")
    }

    fn texts(items: &[PathBuf]) -> List {
        let mut list = List::new();
        for item in items {
            list.push(item.to_string_lossy().as_ref());
        }
        list
    }

    /// The registry stays alive with the test: what it offered points into the source's library.
    fn loaded() -> Registry {
        let mut config = Map::new();
        config.set("dirs", texts(&[manifest_dir().join("transport")]));
        config.set(
            "paths",
            texts(&[guatiao().join("bindings/python/guatiao/src"), kinds()]),
        );
        if cfg!(windows) {
            // Where the source finds python3.dll, and through it the interpreter.
            config.set(
                "import_dirs",
                texts(&[PathBuf::from(env!("COQUI_TRANSPORT_TEST_PYTHON_PREFIX"))]),
            );
        }
        let mut registry = Registry::new("coqui-transport-guatiao-python-tests", "1.0");
        registry
            .add_source_file(source(), &config)
            .unwrap_or_else(|e| panic!("the source was refused: {e}"));
        let changed = registry.refresh();
        assert!(changed.refused.is_empty(), "{changed:?}");
        registry
    }

    const ID: &str = "memory_transport_memory";

    fn instance(registry: &Registry, settings: Map) -> impl Transport + use<> {
        registry
            .offer::<dyn Transport>(ID)
            .expect("the source offers it")
            .expect("its table is the Rust-declared kind")
            .instantiate(&Value::from(settings))
            .expect("the settings fit the schema")
    }

    #[test]
    fn the_python_transport_keeps_the_kinds_contract() {
        let registry = loaded();
        let transport = instance(&registry, Map::new());
        let info = transport.describe().expect("described");
        assert_eq!(info.title, "Memory, in Python");
        assert_eq!(info.max_message_bytes, 1024);
        let suite = Contract::new("the Python memory transport", "memory:one", "memory:two")
            .unreadable("loop:elsewhere")
            .unreachable("memory:nobody");
        if let Err(failures) = suite.run(&transport) {
            panic!("{failures}");
        }
        assert_eq!(transport.non_defaults().expect("answered"), Vec::new());
    }

    #[test]
    fn its_settings_are_checked_against_the_schema_and_reported_when_they_differ() {
        let registry = loaded();
        let offer = registry
            .offer::<dyn Transport>(ID)
            .expect("the source offers it")
            .expect("its table is the Rust-declared kind");
        assert!(
            offer.config_schema().is_some(),
            "the source derives a schema"
        );
        let mut map = Map::new();
        map.set("scheme", "mem");
        map.set("max_message", 200_u64);
        let transport = instance(&registry, map);
        assert_eq!(
            transport.describe().expect("described").max_message_bytes,
            200
        );
        let listed = transport.non_defaults().expect("answered");
        let names: Vec<&str> = listed.iter().map(|n| n.setting.as_str()).collect();
        assert_eq!(names, ["max_message", "scheme"]);
        let suite = Contract::new("the configured Python transport", "mem:one", "mem:two")
            .unreadable("memory:one")
            .unreachable("mem:nobody");
        if let Err(failures) = suite.run(&transport) {
            panic!("{failures}");
        }
        let mut wrong = Map::new();
        wrong.set("max_mesage", 64_u64);
        assert!(
            offer.instantiate(&Value::from(wrong)).is_err(),
            "a key the settings do not declare was accepted"
        );
    }

    #[test]
    fn the_committed_python_kinds_are_what_the_description_writes() {
        let checked = Command::new(python())
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHONPATH", guatiao().join("bindings/python/guatiao/src"))
            .arg(kinds().join("write_kinds.py"))
            .arg("--check")
            .output()
            .expect("Python runs");
        assert!(
            checked.status.success(),
            "{}",
            String::from_utf8_lossy(&checked.stderr)
        );
    }
}
