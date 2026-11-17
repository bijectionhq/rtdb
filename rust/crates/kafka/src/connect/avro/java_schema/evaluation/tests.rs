use super::*;
use super::super::test_support::*;

#[test]
fn a_computed_minimum_hash_remains_the_source_uninitialized_sentinel() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let id=builder.primitive(Type::Int,&mut context).unwrap();let maximum=node(JsonValue::I32(i32::MAX),&mut context);builder.properties(id,properties(vec![("",maximum)],&mut context)).unwrap();let source=builder.finish(id,&mut context).unwrap();let mut seen=RecursionState::new();let mut observed=Observed{type_value:1,..Observed::default()};let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();
    assert_eq!(evaluation.hash_code(&source).unwrap(),i32::MIN);assert_eq!(evaluation.hash_code(&source).unwrap(),i32::MIN);assert_eq!(evaluation.authority.witness.events,vec![Event::Type(Type::Int),Event::Type(Type::Int)]);assert_eq!(source.view().node().hash.load(Ordering::Relaxed),NO_HASHCODE);
}

#[test]
fn compute_hash_bypasses_cached_child_public_hashes() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let child=builder.primitive(Type::Int,&mut context).unwrap();let array=builder.array(child,&mut context).unwrap();let source=builder.finish(array,&mut context).unwrap();let child=source.at(child).unwrap();let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();
    assert_eq!(evaluation.hash_code(&child).unwrap(),7);evaluation.authority.witness.events.clear();assert_eq!(evaluation.compute_hash(&source).unwrap(),14);assert_eq!(evaluation.authority.witness.events,vec![Event::Type(Type::Array),Event::Type(Type::Int)]);evaluation.authority.witness.events.clear();assert_eq!(evaluation.hash_code(&child).unwrap(),7);assert!(evaluation.authority.witness.events.is_empty());assert_eq!(source.view().node().hash.load(Ordering::Relaxed),NO_HASHCODE);
}

#[test]
fn record_seen_membership_spans_the_whole_top_level_compute_operation() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let root=builder.record(name("R",&mut context),false,&mut context).unwrap();let child=builder.record(name("C",&mut context),false,&mut context).unwrap();builder.fields_with(child,0,&mut context,||Ok(Vec::new())).unwrap();let fields=vec![Field::new(text("a",&mut context),child,None,FieldOrder::Ascending).unwrap(),Field::new(text("b",&mut context),child,None,FieldOrder::Ascending).unwrap()];builder.fields_with(root,4096,&mut context,||Ok(fields)).unwrap();let source=builder.finish(root,&mut context).unwrap();let child=source.at(child).unwrap();let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();
    assert_eq!(evaluation.hash_code(&child).unwrap(),75);evaluation.authority.witness.events.clear();assert_eq!(evaluation.compute_hash(&source).unwrap(),6480);assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(source.id()),Event::Identity(source.id()),Event::Type(Type::Record),Event::Identity(child.id()),Event::Identity(child.id()),Event::Type(Type::Record),Event::Identity(child.id())]);assert!(matches!(&evaluation.state.hash,Lazy::Ready(table) if table.is_empty()));
}

#[test]
fn recursive_records_use_real_identity_and_seen_pairs_without_caching_compute_results() {
    fn recursive(context:&mut ConversionContext)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.record(name("R",context),false,context).unwrap();let field=Field::new(text("next",context),id,None,FieldOrder::Ascending).unwrap();builder.fields_with(id,4096,context,||Ok(vec![field])).unwrap();builder.finish(id,context).unwrap()}
    let mut context=context();let left=recursive(&mut context);let right=recursive(&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).unwrap());assert!(evaluation.compute_hash(&left).is_ok());assert_eq!(left.view().node().hash.load(Ordering::Relaxed),NO_HASHCODE);assert!(matches!(&evaluation.state.equal,Lazy::Ready(table) if table.len()==0));assert!(matches!(&evaluation.state.hash,Lazy::Ready(table) if table.is_empty()));
}

#[test]
fn warm_cleared_pair_sets_hash_empty_contains_while_cold_sets_skip_it() {
    let mut context=context();let left=record("R",&mut context);let right=record("R",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).unwrap());assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(left.id()),Event::Identity(right.id())]);evaluation.authority.witness.events.clear();assert!(evaluation.equal(&left,&right).unwrap());assert_eq!(evaluation.authority.witness.events,vec![Event::Identity(left.id()),Event::Identity(right.id()),Event::Identity(left.id()),Event::Identity(right.id())]);
}

#[test]
fn child_hash_failure_runs_outer_record_finally_and_preserves_successful_child_memos() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let root=builder.record(name("R",&mut context),false,&mut context).unwrap();let child=builder.primitive(Type::Int,&mut context).unwrap();let field=Field::new(text("x",&mut context),child,None,FieldOrder::Ascending).unwrap();builder.fields_with(root,4096,&mut context,||Ok(vec![field])).unwrap();let source=builder.finish(root,&mut context).unwrap();let child=source.at(child).unwrap();let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert_eq!(evaluation.hash_code(&child).unwrap(),7);evaluation.authority.witness.fail=Some(Type::Int);
    assert!(evaluation.hash_code(&source).unwrap_err().code.ends_with("schema-witness-test"));assert_eq!(child.view().node().hash.load(Ordering::Relaxed),7);assert_eq!(source.view().node().hash.load(Ordering::Relaxed),NO_HASHCODE);assert!(matches!(&evaluation.state.hash,Lazy::Ready(table) if table.is_empty()));evaluation.authority.witness.fail=None;assert!(evaluation.hash_code(&source).is_ok());
}

#[test]
fn warm_seen_storage_is_retained_before_reuse_and_type_mismatch_keeps_it_empty() {
    let mut context=context();let left=record("R",&mut context);let right=record("R",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).unwrap());let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);drop(evaluation);assert!(Evaluation::new(&mut seen,&mut denied,&mut observed).is_err());
    let mut next=super::super::test_support::context();let mut evaluation=Evaluation::new(&mut seen,&mut next,&mut observed).unwrap();assert!(!evaluation.equal(&left,&scalar_schema(Type::Int,&mut context)).unwrap());assert!(evaluation.equal(&left,&right).unwrap());assert!(matches!(&evaluation.state.equal,Lazy::Ready(table) if table.len()==0));
}

#[test]
fn schema_identity_fastpaths_and_mismatched_cached_hashes_avoid_seen_collections() {
    let mut context=context();let left=record("R",&mut context);let right=record("S",&mut context);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();evaluation.hash_code(&left).unwrap();evaluation.hash_code(&right).unwrap();evaluation.authority.witness.events.clear();assert!(!evaluation.equal(&left,&right).unwrap());assert!(evaluation.equal(&left,&left).unwrap());assert!(evaluation.authority.witness.events.is_empty());assert!(matches!(&evaluation.state.equal,Lazy::Cold));
}

#[test]
fn union_lockable_lists_visit_record_prefix_before_reporting_different_lengths() {
    fn union(context:&mut ConversionContext,extra:bool)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let record=builder.record(name("R",context),false,context).unwrap();builder.fields_with(record,0,context,||Ok(Vec::new())).unwrap();let mut variants=vec![record];if extra {variants.push(builder.primitive(Type::Int,context).unwrap());}let id=builder.union_with(4096,context,||Ok(variants)).unwrap();builder.finish(id,context).unwrap()}
    let mut construction=context();let left=union(&mut construction,false);let right=union(&mut construction,true);let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut denied,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).is_err());assert_eq!(evaluation.authority.witness.events.len(),2);assert!(evaluation.authority.witness.events.iter().all(|event|matches!(event,Event::Identity(_))));assert!(evaluation.state.failed);
}

#[test]
fn native_schema_traversal_boundary_is_an_explicit_resource_limit_and_identity_can_skip_it() {
    fn arrays(count: usize, context: &mut ConversionContext) -> SchemaHandle {
        let mut builder = SchemasBuilder::new(context).unwrap();
        let mut id = builder.primitive(Type::Int, context).unwrap();
        for _ in 0..count { id = builder.array(id, context).unwrap(); }
        builder.finish(id, context).unwrap()
    }
    let mut context = context();
    let edge = arrays(NATIVE_MAX_TRAVERSAL_DEPTH, &mut context);
    let edge_copy = arrays(NATIVE_MAX_TRAVERSAL_DEPTH, &mut context);
    let over = arrays(NATIVE_MAX_TRAVERSAL_DEPTH + 1, &mut context);
    let over_copy = arrays(NATIVE_MAX_TRAVERSAL_DEPTH + 1, &mut context);
    let mut state = RecursionState::new();
    let mut observed = Observed::default();
    let mut evaluation = Evaluation::new(&mut state, &mut context, &mut observed).unwrap();
    assert!(evaluation.compute_hash(&edge).is_ok());
    assert!(evaluation.equal(&edge, &edge_copy).unwrap());
    assert!(evaluation.hash_code(&over).unwrap_err().code.ends_with("avro-native-traversal-limit"));
    assert!(evaluation.equal(&over, &over_copy).unwrap_err().code.ends_with("avro-native-traversal-limit"));
    assert!(evaluation.equal(&over, &over).unwrap());
    assert_eq!(over.view().node().hash.load(Ordering::Relaxed), NO_HASHCODE);
}
