use super::*;
use super::super::test_support::*;

#[test]
fn reference_contains_add_and_target_get_preserve_separate_identity_observations() {
    let mut context=context();let source=record("R",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();
    let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();cycles.register_record(&source,CycleReference{schema:17,optional:false},&mut evaluation).unwrap();evaluation.authority.witness.events.clear();
    assert_eq!(cycles.reference(&source,true,&mut evaluation).unwrap(),Some(CycleReference{schema:17,optional:true}));
    assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(source.id()),Event::Identity(source.id()),Event::Identity(source.id()),Event::Type(Type::Record),Event::Identity(source.id())]);
    evaluation.authority.witness.events.clear();assert_eq!(cycles.reference(&source,false,&mut evaluation).unwrap(),Some(CycleReference{schema:17,optional:false}));assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(source.id()),Event::Identity(source.id())]);
    evaluation.authority.witness.events.clear();cycles.finish(&source,&mut evaluation).unwrap();assert!(evaluation.authority.witness.events.is_empty());
}

#[test]
fn structurally_equal_distinct_references_collapse_at_actual_hash_set_insertion() {
    let mut context=context();let left=record("R",&mut context);let right=record("R",&mut context);assert!(!left.same_object(&right));let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();
    cycles.register_record(&left,CycleReference{schema:1,optional:false},&mut evaluation).unwrap();cycles.reference(&left,false,&mut evaluation).unwrap();cycles.register_record(&right,CycleReference{schema:2,optional:false},&mut evaluation).unwrap();
    assert_eq!(cycles.reference(&right,true,&mut evaluation).unwrap(),Some(CycleReference{schema:2,optional:true}));assert_eq!(cycles.detected.len(),1);assert_eq!(cycles.records.entries().unwrap().count(),2);
    cycles.finish(&right,&mut evaluation).unwrap();assert_eq!(cycles.records.entries().unwrap().count(),2);let (retained,_)=cycles.detected.try_iter().unwrap().next().unwrap();assert!(retained.same_object(&left));
}

#[test]
fn a_missed_identity_never_demands_structural_hash_or_equality() {
    let mut context=context();let registered=record("R",&mut context);let distinct=record("R",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();cycles.register_record(&registered,CycleReference{schema:1,optional:false},&mut evaluation).unwrap();evaluation.authority.witness.events.clear();evaluation.authority.witness.fail=Some(Type::Record);
    assert_eq!(cycles.reference(&distinct,true,&mut evaluation).unwrap(),None);assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(distinct.id())]);assert_eq!(cycles.detected.len(),0);
}

#[test]
fn undetected_finish_demands_contains_then_remove_and_cached_nonrecords_skip_seeding() {
    let mut context=context();let source=record("R",&mut context);let scalar=scalar_schema(Type::Int,&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();
    cycles.seed_cached(&scalar,CycleReference{schema:3,optional:false},&mut evaluation).unwrap();assert!(evaluation.authority.witness.events.is_empty());cycles.seed_cached(&source,CycleReference{schema:7,optional:false},&mut evaluation).unwrap();evaluation.authority.witness.events.clear();cycles.finish(&source,&mut evaluation).unwrap();assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(source.id()),Event::Identity(source.id())]);assert!(cycles.records.is_empty());assert_eq!(cycles.reference(&source,true,&mut evaluation).unwrap(),None);
}

#[test]
fn detected_set_hash_failure_precedes_target_get_and_poisoned_context_rejects_reuse() {
    let mut context=context();let source=record("R",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();cycles.register_record(&source,CycleReference{schema:1,optional:false},&mut evaluation).unwrap();evaluation.authority.witness.events.clear();evaluation.authority.witness.fail=Some(Type::Record);
    assert!(cycles.reference(&source,true,&mut evaluation).unwrap_err().code.ends_with("schema-witness-test"));assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(source.id()),Event::Identity(source.id()),Event::Identity(source.id()),Event::Type(Type::Record)]);assert!(cycles.failed);assert_eq!(cycles.records.entries().unwrap().count(),1);assert!(cycles.reference(&source,false,&mut evaluation).is_err());
}

#[test]
fn retained_cycle_keys_keep_canonical_owners_alive_after_input_scope_drop() {
    let budget=rtdb_columnar::MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&budget));let source=record("R",&mut context);let weak=Arc::downgrade(&source.owner);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();let mut cycles=CycleContext::new(&mut evaluation).unwrap();cycles.seed_cached(&source,CycleReference{schema:1,optional:false},&mut evaluation).unwrap();drop(evaluation);drop(source);drop(context);assert!(weak.upgrade().is_some());assert!(budget.used_bytes()>0);
    let mut denied=ConversionContext::with_limit(Arc::clone(&budget),0);assert!(cycles.retain(&mut denied).is_err());let mut next=ConversionContext::new(Arc::clone(&budget));cycles.retain(&mut next).unwrap();drop(cycles);assert!(weak.upgrade().is_none());drop(next);drop(denied);drop(seen);assert_eq!(budget.used_bytes(),0);
}

#[test]
fn cycle_retention_walks_each_actual_arena_once_across_both_source_collections() {
    let budget = rtdb_columnar::MemoryBudget::new(usize::MAX);
    let mut construction = ConversionContext::new(Arc::clone(&budget));
    let mut builder = SchemasBuilder::new(&mut construction).unwrap();
    let scalar = builder.primitive(Type::Int, &mut construction).unwrap();
    let default = node(JsonValue::I32(1), &mut construction);
    let mut records = Vec::new();
    for value in ["R", "A", "B"] {
        let id = builder.record(name(value, &mut construction), false, &mut construction).unwrap();
        let field = Field::new(text("x", &mut construction), scalar, Some(default.clone()), FieldOrder::Ascending).unwrap();
        builder.fields_with(id, 4096, &mut construction, || Ok(vec![field])).unwrap();
        records.push(id);
    }
    let source = builder.finish(records[0], &mut construction).unwrap();
    let mut state = RecursionState::new();
    let mut observed = Observed::default();
    let mut evaluation = Evaluation::new(&mut state, &mut construction, &mut observed).unwrap();
    let mut cycles = CycleContext::new(&mut evaluation).unwrap();
    for (index, id) in records.iter().enumerate() {
        let record = source.at(*id).unwrap();
        cycles.register_record(&record, CycleReference { schema: index, optional: false }, &mut evaluation).unwrap();
        cycles.reference(&record, false, &mut evaluation).unwrap();
    }
    drop(evaluation);
    let mut expected = ConversionContext::new(Arc::clone(&budget));
    for lease in cycles.records.leases().iter().chain(cycles.detected.leases()) { expected.retain(Arc::clone(lease)).unwrap(); }
    source.retain(&mut expected).unwrap();
    let needed = usize::MAX - expected.allocation_remaining();
    let mut exact = ConversionContext::with_limit(Arc::clone(&budget), needed);
    cycles.retain(&mut exact).unwrap();
    assert_eq!(exact.allocation_remaining(), 0);
    assert_eq!(cycles.records.entries().unwrap().count(), 3);
    assert_eq!(cycles.detected.len(), 3);
}
