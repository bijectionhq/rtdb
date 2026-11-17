use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::{ConnectConfig, PluginRegistry, Predicate, PredicateFactory, SinkRecord, Transform, TransformChain, TransformFactory};
use crate::connect::{ConnectData, date_environment::test_environment};
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
    fn test(&mut self, _: &SinkRecord) -> Result<bool> {
        self.events.lock().unwrap().push(format!("test {}", self.name));
        Ok(true)
    }
}

impl Transform for RecordingPlugin {
    fn apply(&mut self, record: SinkRecord) -> Result<Option<SinkRecord>> {
        self.events.lock().unwrap().push(format!("apply {}", self.name));
        Ok(Some(record))
    }
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

fn open_four_steps(registry: &PluginRegistry, fourth_transform: &str, last_predicate: &str) -> Result<TransformChain> {
    open(registry, &[
        ("transforms", "first,second,third,fourth"),
        ("transforms.first.type", RECORDING_TRANSFORM), ("transforms.first.name", "first"),
        ("transforms.first.predicate", "a"),
        ("transforms.second.type", RECORDING_TRANSFORM), ("transforms.second.name", "second"),
        ("transforms.second.predicate", "z"), ("transforms.second.negate", "true"),
        ("transforms.third.type", RECORDING_TRANSFORM), ("transforms.third.name", "third"),
        ("transforms.third.predicate", "a"),
        ("transforms.fourth.type", fourth_transform), ("transforms.fourth.name", "fourth"),
        ("transforms.fourth.predicate", "m"),
        ("predicates", "unused,m,z,a"),
        ("predicates.unused.type", "com.example.DoesNotExist"),
        ("predicates.m.type", last_predicate), ("predicates.m.name", "m"),
        ("predicates.z.type", RECORDING_PREDICATE), ("predicates.z.name", "z"),
        ("predicates.a.type", RECORDING_PREDICATE), ("predicates.a.name", "a"),
    ])
}

#[test]
fn a_completed_chain_uses_shared_predicate_slots_and_drops_transforms_before_predicates() {
    let (registry, events) = recording_registry();
    let mut chain = open_four_steps(&registry, RECORDING_TRANSFORM, RECORDING_PREDICATE).unwrap();
    let record = SinkRecord::new("events".into(), 0, 0, ConnectData::null(), ConnectData::null());
    assert!(chain.apply(record).unwrap().is_some());
    drop(chain);
    assert_eq!(observed(&events), [
        "open a", "open z", "open m", "open first", "open second", "open third", "open fourth",
        "test a", "apply first", "test z", "test a", "apply third", "test m", "apply fourth",
        "drop first", "drop second", "drop third", "drop fourth", "drop a", "drop z", "drop m",
    ]);
}

#[test]
fn transform_open_failure_drops_successful_transforms_then_predicates_in_reverse_opening_order() {
    let (registry, events) = recording_registry();
    let error = open_four_steps(&registry, "com.example.MissingTransform", RECORDING_PREDICATE)
        .err().expect("missing transform class");
    assert_eq!(error.code, "xtdb.kafka-connect-source/plugin-class-not-found");
    assert!(error.message.as_deref().is_some_and(|message| message.contains("com.example.MissingTransform")));
    assert_eq!(observed(&events), [
        "open a", "open z", "open m", "open first", "open second", "open third",
        "drop third", "drop second", "drop first", "drop m", "drop z", "drop a",
    ]);
}

#[test]
fn predicate_open_failure_drops_preceding_predicates_in_reverse_opening_order() {
    let (registry, events) = recording_registry();
    let error = open_four_steps(&registry, RECORDING_TRANSFORM, "com.example.MissingPredicate")
        .err().expect("missing predicate class");
    assert_eq!(error.code, "xtdb.kafka-connect-source/plugin-class-not-found");
    assert!(error.message.as_deref().is_some_and(|message| message.contains("com.example.MissingPredicate")));
    assert_eq!(observed(&events), ["open a", "open z", "drop z", "drop a"]);
}

#[test]
fn remaining_allocation_counts_new_and_retained_charges_without_counting_the_same_owner_twice() {
    let memory = rtdb_columnar::MemoryBudget::new(64);
    let mut context = super::ConversionContext::with_limit(Arc::clone(&memory), 32);
    assert_eq!(context.allocation_remaining(), 32);
    let owned = context.reserve(8).unwrap();
    assert_eq!(context.allocation_remaining(), 24);
    context.retain(Arc::clone(&owned)).unwrap();
    assert_eq!(context.allocation_remaining(), 24);
    let retained = memory.reserve(4).unwrap();
    context.retain(Arc::clone(&retained)).unwrap();
    assert_eq!(context.allocation_remaining(), 20);
    assert!(context.reserve(21).is_err());
    assert_eq!(context.allocation_remaining(), 20);
    drop(context);
    assert_eq!(memory.used_bytes(), 12);
    drop(owned);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn a_remaining_record_ceiling_does_not_authorize_an_exhausted_shared_budget() {
    let memory = rtdb_columnar::MemoryBudget::new(16);
    let held = memory.reserve(14).unwrap();
    let mut context = super::ConversionContext::with_limit(Arc::clone(&memory), 32);
    assert_eq!(context.allocation_remaining(), 32);
    assert!(context.reserve(3).is_err());
    assert_eq!(context.allocation_remaining(), 32);
    let admitted = context.reserve(2).unwrap();
    assert_eq!(context.allocation_remaining(), 30);
    assert_eq!(memory.used_bytes(), 16);
    drop(context);
    drop(admitted);
    drop(held);
    assert_eq!(memory.used_bytes(), 0);
}
