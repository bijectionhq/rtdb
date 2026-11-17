use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::{ConnectConfig, PluginRegistry, Predicate, PredicateFactory, SinkRecord, Transform, TransformChain, TransformFactory};
use crate::connect::date_environment::test_environment;
use rtdb_types::Result;

const RECORDING_PREDICATE: &str = "xtdb.kafka.connectsrc.RecordingPredicate";
const RECORDING_TRANSFORM: &str = "xtdb.kafka.connectsrc.RecordingTransform";

struct RecordingFactory {
    events: Arc<Mutex<Vec<String>>>,
}

struct RecordingPlugin {
    name: String,
    events: Arc<Mutex<Vec<String>>>,
}

impl RecordingFactory {
    fn instance(&self, role: &str, options: &BTreeMap<String, String>) -> RecordingPlugin {
        let name = options.get("name").map_or(role, String::as_str);
        self.events.lock().unwrap().push(format!("open {name}"));
        RecordingPlugin { name: name.into(), events: Arc::clone(&self.events) }
    }
}

impl PredicateFactory for RecordingFactory {
    fn open(&self, options: &BTreeMap<String, String>) -> Result<Box<dyn Predicate>> {
        Ok(Box::new(self.instance("predicate", options)))
    }
}

impl TransformFactory for RecordingFactory {
    fn open(&self, options: &BTreeMap<String, String>) -> Result<Box<dyn Transform>> {
        Ok(Box::new(self.instance("transform", options)))
    }
}

impl Predicate for RecordingPlugin {
    fn test(&mut self, _: &SinkRecord) -> Result<bool> { Ok(true) }
}

impl Transform for RecordingPlugin {
    fn apply(&mut self, record: SinkRecord) -> Result<Option<SinkRecord>> { Ok(Some(record)) }
}

impl Drop for RecordingPlugin {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(format!("drop {}", self.name));
    }
}

fn recording_registry() -> (PluginRegistry, Arc<Mutex<Vec<String>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(RecordingFactory { events: Arc::clone(&events) });
    let registry = PluginRegistry::default()
        .with_predicate(RECORDING_PREDICATE.into(), factory.clone()).unwrap()
        .with_transform(RECORDING_TRANSFORM.into(), factory).unwrap();
    (registry, events)
}

fn observed(events: &Mutex<Vec<String>>) -> Vec<String> {
    events.lock().unwrap().clone()
}

fn open(registry: &PluginRegistry, options: &[(&str, &str)]) -> Result<TransformChain> {
    let mut properties = BTreeMap::from([
        ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
        ("value.converter".into(), "org.apache.kafka.connect.json.JsonConverter".into()),
    ]);
    for &(key, value) in options {
        properties.insert(key.into(), value.into());
    }
    registry.chain_with_environment(&ConnectConfig::parse(&properties)?, test_environment("en", "US", "UTC"))
}

#[test]
fn distinct_referenced_predicates_open_in_first_use_order_before_any_transform() {
    let (registry, events) = recording_registry();
    let _chain = open(&registry, &[
        ("transforms", "first,second,third,fourth"),
        ("transforms.first.type", RECORDING_TRANSFORM), ("transforms.first.name", "first"),
        ("transforms.second.type", RECORDING_TRANSFORM), ("transforms.second.name", "second"),
        ("transforms.second.predicate", "z"),
        ("transforms.third.type", RECORDING_TRANSFORM), ("transforms.third.name", "third"),
        ("transforms.third.predicate", "a"),
        ("transforms.fourth.type", RECORDING_TRANSFORM), ("transforms.fourth.name", "fourth"),
        ("transforms.fourth.predicate", "z"),
        ("predicates", "a,unused,z"),
        ("predicates.a.type", RECORDING_PREDICATE), ("predicates.a.name", "a"),
        ("predicates.unused.type", "com.example.DoesNotExist"),
        ("predicates.z.type", RECORDING_PREDICATE), ("predicates.z.name", "z"),
    ]).unwrap();
    assert_eq!(observed(&events), ["open z", "open a", "open first", "open second", "open third", "open fourth"]);
}

#[test]
fn a_later_steps_missing_predicate_preempts_the_first_transforms_invalid_configuration() {
    let result = open(&PluginRegistry::default(), &[
        ("transforms", "first,second"),
        ("transforms.first.type", "org.apache.kafka.connect.transforms.RegexRouter"),
        ("transforms.second.type", "org.apache.kafka.connect.transforms.Filter"),
        ("transforms.second.predicate", "missing"),
        ("predicates", "missing"),
        ("predicates.missing.type", "com.example.DoesNotExist"),
    ]);
    let error = result.err().expect("missing predicate class");
    assert_eq!(error.code, "xtdb.kafka-connect-source/plugin-class-not-found");
    assert!(error.message.as_deref().is_some_and(|message| message.contains("com.example.DoesNotExist")));
}

#[test]
fn closes_already_opened_transforms_when_a_later_one_fails_to_open() {
    let (registry, events) = recording_registry();
    let result = open(&registry, &[
        ("transforms", "ok,bad"),
        ("transforms.ok.type", RECORDING_TRANSFORM),
        ("transforms.bad.type", "com.example.DoesNotExist"),
    ]);
    assert_eq!(result.err().expect("missing transform class").code, "xtdb.kafka-connect-source/plugin-class-not-found");
    assert_eq!(observed(&events), ["open transform", "drop transform"]);
}

#[test]
fn opens_a_shared_predicate_once_and_closes_it_once() {
    let (registry, events) = recording_registry();
    let chain = open(&registry, &[
        ("transforms", "a,b"),
        ("transforms.a.type", RECORDING_TRANSFORM), ("transforms.a.predicate", "p"),
        ("transforms.b.type", RECORDING_TRANSFORM), ("transforms.b.predicate", "p"),
        ("predicates", "p"), ("predicates.p.type", RECORDING_PREDICATE),
    ]).unwrap();
    assert_eq!(observed(&events), ["open predicate", "open transform", "open transform"]);
    drop(chain);
    assert_eq!(observed(&events).iter().filter(|event| event.as_str() == "drop predicate").count(), 1);
}
