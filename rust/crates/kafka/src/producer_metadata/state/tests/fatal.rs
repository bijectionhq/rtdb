use std::cell::Cell;

use super::*;

fn error(producer: &mut ProducerMetadata, code: &'static str) -> MetadataError {
    producer.prepare_fatal_error(128, || Ok(Anomaly::unsupported(code, "Source metadata error"))).unwrap()
}

fn with_topic_errors(memory: Arc<MemoryBudget>) -> ProducerMetadata {
    let mut producer = producer(memory);
    producer.add_topics(["invalid", "secret"], 0, &mut NoIdentityWitness).unwrap();
    let topics = [
        Topic { error: 17, ..topic(b"invalid", 1, &[]) },
        Topic { error: 29, ..topic(b"secret", 2, &[]) },
    ];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    producer
}

#[test]
fn ordinary_fatal_is_consumed_once_and_the_result_retains_its_payload() {
    let memory = MemoryBudget::new(1 << 20);
    let mut producer = producer(memory.clone());
    let admitted = error(&mut producer, "fixture/request");
    producer.record_fatal_error(FatalKind::Request, admitted.clone()).unwrap();
    let observed = producer.take_fatal_error().unwrap().unwrap();
    assert!(std::ptr::eq(observed.cause(), admitted.cause()));
    assert!(producer.take_fatal_error().unwrap().is_none());
    drop(admitted);
    drop(producer);
    assert!(memory.used_bytes() > 0);
    assert_eq!(observed.cause().code, "fixture/request");
    drop(observed);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn waiter_observes_the_admitted_fatal_before_its_newer_version() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    let fatal = error(&mut producer, "fixture/request");
    producer.record_fatal_error(FatalKind::Request, fatal.clone()).unwrap();
    let mut wait = MetadataWait::new(-1, 0, || Ok(10)).unwrap();
    let MetadataWaitObservation::Fatal(observed) = producer.poll_wait(&mut wait, || panic!("fatal precedes clock")).unwrap() else {
        panic!("expected retained fatal");
    };
    assert!(std::ptr::eq(observed.cause(), fatal.cause()));
    assert!(matches!(
        producer.poll_wait(&mut wait, || panic!("newer version precedes clock")).unwrap(),
        MetadataWaitObservation::Step(WaitStep::Updated)
    ));
}

#[test]
fn permanent_bootstrap_fatal_shadows_the_request_slot_without_clearing_topic_errors() {
    let mut producer = with_topic_errors(MemoryBudget::new(1 << 20));
    let ordinary = error(&mut producer, "fixture/request");
    let bootstrap = error(&mut producer, "fixture/bootstrap");
    producer.record_fatal_error(FatalKind::Request, ordinary).unwrap();
    producer.record_fatal_error(FatalKind::Bootstrap, bootstrap.clone()).unwrap();
    let first = producer.take_fatal_error().unwrap().unwrap();
    let Some(MetadataTopicError::Fatal(again)) = producer.take_error_for_topic("secret").unwrap() else {
        panic!("expected retained bootstrap failure");
    };
    assert!(std::ptr::eq(first.cause(), bootstrap.cause()));
    assert!(std::ptr::eq(again.cause(), bootstrap.cause()));
    assert_eq!(producer.fatal.as_ref().unwrap().cause().code, "fixture/request");
    assert_eq!(producer.invalid.len(), 1);
    assert_eq!(producer.unauthorized.len(), 1);
}

#[test]
fn ordinary_fatal_on_topic_demand_clears_both_recoverable_sets() {
    let mut producer = with_topic_errors(MemoryBudget::new(1 << 20));
    let fatal = error(&mut producer, "fixture/request");
    producer.record_fatal_error(FatalKind::Request, fatal.clone()).unwrap();
    let Some(MetadataTopicError::Fatal(observed)) = producer.take_error_for_topic("secret").unwrap() else {
        panic!("ordinary fatal should precede the topic error");
    };
    assert!(std::ptr::eq(observed.cause(), fatal.cause()));
    assert!(producer.take_error_for_topic("invalid").unwrap().is_none());
    assert!(producer.take_error_for_topic("secret").unwrap().is_none());
    assert_eq!(producer.topic_error("secret"), Some(29));
}

#[test]
fn fatal_payload_admission_precedes_the_source_builder() {
    let memory = MemoryBudget::new(1 << 20);
    let mut producer = producer(memory.clone());
    let full = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let called = Cell::new(false);
    let result = producer.prepare_fatal_error(128, || {
        called.set(true);
        Ok(Anomaly::unsupported("fixture/request", "Source metadata error"))
    });
    assert!(matches!(result, Err(Failure::Native(_))));
    assert!(!called.get());
    assert!(matches!(producer.take_fatal_error(), Err(Failure::CapabilityFailed)));
    drop(full);
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn recording_a_foreign_budget_error_keeps_its_origin_reservation_until_last_release() {
    let original_budget = MemoryBudget::new(1 << 20);
    let mut original = producer(original_budget.clone());
    let error = error(&mut original, "fixture/request");
    drop(original);
    let mut target = producer(MemoryBudget::new(1 << 20));
    let rejected = target.record_fatal_error(FatalKind::Request, error.clone());
    assert!(matches!(rejected, Err(Failure::Native(cause)) if cause.code == "rtdb/kafka-fatal-error-budget-domain"));
    assert!(target.fatal.is_none());
    assert!(original_budget.used_bytes() > 0);
    drop(error);
    assert_eq!(original_budget.used_bytes(), 0);
}
