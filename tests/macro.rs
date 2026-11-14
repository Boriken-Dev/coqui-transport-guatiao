//! A transport written in this file, with no guatiao in it, made a provider by the macro, offered through a
//! registry and used by a host: settings, schema, non-defaults, dial, listen and the channel rules.
#![allow(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core,
    reason = "a test names what it uses from std"
)]

use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use coqui_builder::Describe;
use coqui_transport::frame::LengthPrefix;
use coqui_transport::memory;
use coqui_transport::{Channel as Messages, StreamChannel};
use coqui_transport_guatiao::contract::{self, Contract};
use coqui_transport_guatiao::guatiao::library::Registry;
use coqui_transport_guatiao::guatiao::{Map, Value};
use coqui_transport_guatiao::{
    Acceptor, Driver, DriverError, Refusal, Transport, TransportAcceptor, TransportInfo, settings,
    transport_provider,
};

/// How a loopback is configured.
#[derive(Clone, Debug, PartialEq, Eq, Describe)]
#[map(deny_unknown_fields)]
struct LoopSettings {
    /// The largest message, in bytes.
    #[map(default = 1024)]
    max_message: usize,
    /// What the endpoint says it is.
    #[map(default = "loop".to_owned())]
    scheme: String,
}

impl Default for LoopSettings {
    fn default() -> Self {
        Self {
            max_message: 1024,
            scheme: "loop".to_owned(),
        }
    }
}

type Pending = Sender<Box<dyn Messages>>;

/// The listeners by endpoint: the only thing a dialler and a listener of this transport share.
static LISTENERS: Mutex<BTreeMap<String, Pending>> = Mutex::new(BTreeMap::new());

struct Loop;

fn channel_on(stream: memory::Stream, max: usize) -> Box<dyn Messages> {
    let codec = LengthPrefix::u32_be();
    let channel = coqui_transport::StreamChannelBuilder::new()
        .codec(codec)
        .stream(stream)
        .max_message(max)
        .build()
        .expect("the channel starts");
    Box::new(channel)
}

impl Driver for Loop {
    type Settings = LoopSettings;

    fn describe(settings: &LoopSettings) -> TransportInfo {
        TransportInfo::new("Loop", "loop:NAME").max_message_bytes(settings.max_message as u64)
    }

    fn dial(
        settings: &LoopSettings,
        endpoint: &str,
        _timeout: Duration,
    ) -> Result<Box<dyn Messages>, DriverError> {
        if !endpoint.starts_with(&format!("{}:", settings.scheme)) {
            return Err(DriverError::new(Refusal::Endpoint, "not a loop endpoint"));
        }
        let sender = LISTENERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(endpoint)
            .cloned()
            .ok_or_else(|| DriverError::new(Refusal::Unreachable, "nobody listens there"))?;
        let (ours, theirs) = memory::pipe(1 << 16);
        sender
            .send(channel_on(theirs, settings.max_message))
            .map_err(|_| DriverError::new(Refusal::Unreachable, "the listener ended"))?;
        Ok(channel_on(ours, settings.max_message))
    }

    fn listen(settings: &LoopSettings, endpoint: &str) -> Result<Box<dyn Acceptor>, DriverError> {
        if !endpoint.starts_with(&format!("{}:", settings.scheme)) {
            return Err(DriverError::new(Refusal::Endpoint, "not a loop endpoint"));
        }
        let (sender, receiver) = channel();
        let mut listeners = LISTENERS.lock().unwrap_or_else(PoisonError::into_inner);
        if listeners.contains_key(endpoint) {
            return Err(DriverError::new(Refusal::Unreachable, "taken"));
        }
        listeners.insert(endpoint.to_owned(), sender);
        Ok(Box::new(Accepting {
            endpoint: endpoint.to_owned(),
            receiver: Mutex::new(receiver),
        }))
    }
}

struct Accepting {
    endpoint: String,
    receiver: Mutex<Receiver<Box<dyn Messages>>>,
}

impl Drop for Accepting {
    fn drop(&mut self) {
        LISTENERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.endpoint);
    }
}

impl Acceptor for Accepting {
    fn address(&self) -> String {
        self.endpoint.clone()
    }

    fn accept(
        &self,
        timeout: Duration,
    ) -> Result<Option<(Box<dyn Messages>, String)>, DriverError> {
        let receiver = self.receiver.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(receiver
            .recv_timeout(timeout)
            .ok()
            .map(|channel| (channel, "memory".to_owned())))
    }
}

transport_provider! {
    /// The loopback transport.
    struct LoopProvider for Loop, id = "loop";
}

coqui_transport_guatiao::guatiao::local_providers!(
    id = "loop-tests",
    version = "1.0.0",
    providers = [LoopProvider]
);

fn registry() -> Registry {
    let mut registry = Registry::new("loop-tests", "1.0");
    registry
        .register_local("loop-tests", library)
        .expect("registers")
        .loaded()
        .expect("accepted");
    registry
}

fn instance(registry: &Registry, settings: Map) -> impl Transport + use<> {
    registry
        .offer::<dyn Transport>("loop")
        .expect("offered")
        .expect("the kind's table")
        .instantiate(&Value::from(settings))
        .expect("the settings fit the schema")
}

#[test]
fn a_provider_made_by_the_macro_dials_and_listens_through_the_kind() {
    let registry = registry();
    let transport = instance(&registry, Map::new());
    let info = transport.describe().expect("described");
    assert_eq!(
        (info.title.as_str(), info.max_message_bytes),
        ("Loop", 1024)
    );
    let suite = Contract::new("the loop", "loop:one", "loop:two")
        .unreadable("pipe:elsewhere")
        .unreachable("loop:nobody");
    if let Err(failures) = suite.run(&transport) {
        panic!("{failures}");
    }
}

#[test]
fn the_configuration_is_the_settings_and_is_checked_against_their_schema() {
    let registry = registry();
    let offer = registry
        .offer::<dyn Transport>("loop")
        .expect("offered")
        .expect("the kind's table");
    let schema = offer.config_schema().expect("a schema is published");
    assert_eq!(
        schema,
        &settings::schema::<LoopSettings>().expect("a group describes itself")
    );
    let mut misspelled = Map::new();
    misspelled.set("max_mesage", 5_u64);
    assert!(offer.instantiate(&Value::from(misspelled)).is_err());
    let mut wrong = Map::new();
    wrong.set("max_message", "a lot");
    assert!(offer.instantiate(&Value::from(wrong)).is_err());
}

#[test]
fn non_defaults_are_generated_from_the_settings() {
    let registry = registry();
    assert_eq!(
        instance(&registry, Map::new())
            .non_defaults()
            .expect("answered"),
        Vec::new()
    );
    let mut map = Map::new();
    map.set("max_message", 4096_u64);
    let transport = instance(&registry, map);
    let listed = transport.non_defaults().expect("answered");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].setting, "max_message");
    assert_eq!(
        (listed[0].value.as_str(), listed[0].default.as_str()),
        ("usize(4096)", "usize(1024)")
    );
    assert_eq!(
        transport.describe().expect("described").max_message_bytes,
        4096
    );
}

#[test]
fn a_refusal_carries_the_status_of_its_kind() {
    use coqui_transport_guatiao::DialRequest;
    use coqui_transport_guatiao::guatiao::Status;
    let registry = registry();
    let transport = instance(&registry, Map::new());
    let (object, _heard) = contract::sink();
    let error = transport
        .dial(DialRequest::new("loop:nobody"), object)
        .expect_err("nothing listens");
    assert_eq!(error.status, Status::GUATIAO_ERR_GONE);
    let (sender, _receiver) = channel();
    let error = transport
        .listen(
            coqui_transport_guatiao::ListenRequest::new("pipe:elsewhere"),
            contract::Accepts(sender).into_object(),
        )
        .map(|_| ())
        .expect_err("not a loop endpoint");
    assert_eq!(error.status, Status::GUATIAO_ERR_BAD_VALUE);
}

#[test]
fn a_stream_channel_is_what_the_driver_returns() {
    // The driver's own type is a plain coqui-transport channel: nothing in it names guatiao.
    let (a, b) = memory::pipe(4096);
    let ours = StreamChannel::start(a, LengthPrefix::u32_be()).expect("starts");
    let theirs = StreamChannel::start(b, LengthPrefix::u32_be()).expect("starts");
    ours.send(b"x").expect("sent");
    assert_eq!(
        theirs.receive(Some(Duration::from_secs(5))),
        Ok(Some(b"x".to_vec()))
    );
}

#[test]
fn settings_round_trip_through_a_guatiao_value() {
    let changed = LoopSettings {
        max_message: 7,
        scheme: "x".to_owned(),
    };
    let value = settings::encode(&changed).expect("written");
    assert_eq!(
        settings::decode::<LoopSettings>(&value).expect("read"),
        changed
    );
    assert_eq!(
        settings::decode::<LoopSettings>(&Value::from(Map::new())).expect("every key is optional"),
        LoopSettings::default()
    );
    let mut unknown = Map::new();
    unknown.set("max_mesage", 1_u64);
    assert!(settings::decode::<LoopSettings>(&Value::from(unknown)).is_err());
}
