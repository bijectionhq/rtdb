use super::*;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};

struct Source { root: Schema, definitions: BTreeMap<String, Schema> }
impl Source {
    fn key(&self) -> RecordIdentity<'_> { RecordIdentity::canonical(&self.root, &self.definitions).unwrap().unwrap() }
}
fn record(name: &str) -> Source {
    let root = Schema::parse_str(&format!(r#"{{"type":"record","name":"{name}","fields":[]}}"#)).unwrap();
    let mut definitions = BTreeMap::new(); super::super::collect_names(&root, &mut definitions); Source { root, definitions }
}
fn identity_equal(left: &Schema, right: &Schema) -> Result<bool> { Ok(std::ptr::eq(left, right)) }
fn reference(schema: usize, optional: bool) -> CycleReference<usize> { CycleReference { schema, optional } }
fn admitted(budget: &Arc<MemoryBudget>, leases: &mut Vec<Arc<MemoryReservation>>, bytes: usize) -> Result<()> { leases.push(budget.reserve(bytes)?); Ok(()) }

#[test]
fn cycle_reference_uses_record_identity_and_the_requested_optionality() {
    let first = record("R"); let equal_copy = record("R");
    let budget = MemoryBudget::new(usize::MAX); let mut leases = Vec::new();
    let mut context = ToConnectContext::new();
    context.register_record(first.key(), reference(7, false), &mut |bytes| admitted(&budget, &mut leases, bytes)).unwrap();
    assert_eq!(context.reference(equal_copy.key(), true, identity_equal, &mut |_| panic!("unregistered identity must not allocate")).unwrap(), None);
    assert_eq!(context.reference(first.key(), true, identity_equal, &mut |bytes| admitted(&budget, &mut leases, bytes)).unwrap(), Some(reference(7, true)));
    assert_eq!(context.reference(first.key(), false, identity_equal, &mut |_| panic!("already detected identity must not allocate")).unwrap(), Some(reference(7, false)));
    assert_eq!(context.detected.len(), 1);
    drop(context); assert!(budget.used_bytes() > 0); drop(leases); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn a_completed_record_is_removed_until_a_reference_detects_a_cycle() {
    let source = record("R"); let mut context = ToConnectContext::new();
    context.register_record(source.key(), reference(3, false), &mut |_| Ok(())).unwrap();
    context.finish(source.key(), |_, _| panic!("no detected records need equality")).unwrap();
    assert_eq!(context.reference(source.key(), true, identity_equal, &mut |_| panic!("removed record must not allocate")).unwrap(), None);
    context.register_record(source.key(), reference(8, false), &mut |_| Ok(())).unwrap();
    assert_eq!(context.reference(source.key(), true, identity_equal, &mut |_| Ok(())).unwrap(), Some(reference(8, true)));
    let mut comparisons = 0;
    context.finish(source.key(), |left, right| { comparisons += 1; Ok(std::ptr::eq(left, right)) }).unwrap();
    assert_eq!(comparisons, 1); assert_eq!(context.records.len(), 1);
    assert_eq!(context.reference(source.key(), false, identity_equal, &mut |_| Ok(())).unwrap(), Some(reference(8, false)));
}

#[test]
fn detected_cycle_membership_uses_the_required_structural_comparator() {
    let first = record("R"); let equal_copy = record("R");
    let mut context = ToConnectContext::new();
    context.register_record(first.key(), reference(1, false), &mut |_| Ok(())).unwrap();
    context.reference(first.key(), false, identity_equal, &mut |_| Ok(())).unwrap();
    context.register_record(equal_copy.key(), reference(2, false), &mut |_| Ok(())).unwrap();
    let mut saw_distinct = false;
    context.finish(equal_copy.key(), |left, right| { saw_distinct = !std::ptr::eq(left, right); Ok(true) }).unwrap();
    assert!(saw_distinct); assert_eq!(context.records.len(), 2);
    context.finish(equal_copy.key(), |_, _| Ok(false)).unwrap();
    assert_eq!(context.records.len(), 1);
    assert_eq!(context.reference(equal_copy.key(), true, identity_equal, &mut |_| panic!("removed identity must not allocate")).unwrap(), None);
}

#[test]
fn a_cached_root_seeds_the_same_cycle_table_as_an_in_progress_record() {
    let source = record("R"); let mut context = ToConnectContext::new();
    context.seed_cached(None, reference(1, false), &mut |_| panic!("only cached records are seeded")).unwrap();
    assert!(context.records.is_empty());
    context.seed_cached(Some(source.key()), reference(41, false), &mut |_| Ok(())).unwrap();
    assert_eq!(context.reference(source.key(), true, identity_equal, &mut |_| Ok(())).unwrap(), Some(reference(41, true)));
    context.register_record(source.key(), reference(42, false), &mut |_| panic!("replacement must retain table storage")).unwrap();
    assert_eq!(context.reference(source.key(), false, identity_equal, &mut |_| panic!("identity was already detected")).unwrap(), Some(reference(42, false)));
}

#[test]
fn rejected_admission_does_not_publish_a_record_or_detected_cycle() {
    let source = record("R"); let mut context = ToConnectContext::new(); let mut calls = 0;
    let result = context.register_record(source.key(), reference(1, false), &mut |_| { calls += 1; Err(error("admission-test", "reject records")) });
    assert!(result.unwrap_err().code.ends_with("admission-test")); assert_eq!(calls, 1); assert!(context.records.is_empty());
    context.register_record(source.key(), reference(1, false), &mut |_| Ok(())).unwrap();
    let result = context.reference(source.key(), true, identity_equal, &mut |_| Err(error("admission-test", "reject detected cycles")));
    assert!(result.unwrap_err().code.ends_with("admission-test")); assert!(context.detected.is_empty()); assert_eq!(context.records.len(), 1);
    assert_eq!(context.reference(source.key(), true, identity_equal, &mut |_| Ok(())).unwrap(), Some(reference(1, true)));
}

#[test]
fn structural_comparison_failures_propagate_without_dropping_record_identity() {
    let source = record("R"); let mut context = ToConnectContext::new();
    context.register_record(source.key(), reference(1, false), &mut |_| Ok(())).unwrap();
    context.reference(source.key(), false, identity_equal, &mut |_| Ok(())).unwrap();
    let result = context.finish(source.key(), |_, _| Err(error("equality-test", "reject comparison")));
    assert!(result.unwrap_err().code.ends_with("equality-test")); assert_eq!(context.records.len(), 1);
    assert_eq!(context.reference(source.key(), false, identity_equal, &mut |_| Ok(())).unwrap(), Some(reference(1, false)));
}

#[test]
fn record_and_detection_vector_growth_is_admitted_before_each_allocation() {
    let sources = (0..17).map(|index| record(&format!("R{index}"))).collect::<Vec<_>>();
    let mut context = ToConnectContext::new(); let mut record_growth = Vec::new(); let mut detected_growth = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        context.register_record(source.key(), reference(index, false), &mut |bytes| { record_growth.push(bytes); Ok(()) }).unwrap();
        assert_eq!(context.reference(source.key(), true, identity_equal, &mut |bytes| { detected_growth.push(bytes); Ok(()) }).unwrap(), Some(reference(index, true)));
    }
    assert_eq!(record_growth.len(), 4); assert_eq!(detected_growth.len(), 4);
    for growth in [&record_growth, &detected_growth] { assert!(growth.windows(2).all(|pair| pair[0] < pair[1])); }
    assert_eq!(context.records.len(), 17); assert_eq!(context.detected.len(), 17);
    assert!(RecordIdentity::canonical(&Schema::Int, &BTreeMap::new()).unwrap().is_none());
}

#[test]
fn equal_distinct_record_references_collapse_at_insertion_before_admission() {
    let first=record("R");let equal_copy=record("R");let mut context=ToConnectContext::new();
    context.register_record(first.key(),reference(1,false),&mut |_| Ok(())).unwrap();
    context.reference(first.key(),false,identity_equal,&mut |_| Ok(())).unwrap();
    context.register_record(equal_copy.key(),reference(2,false),&mut |_| Ok(())).unwrap();
    let mut comparisons=0;
    let result=context.reference(equal_copy.key(),true,|left,right| {comparisons+=1;assert!(!std::ptr::eq(left,right));Ok(true)},&mut |_| panic!("equal detection must not allocate"));
    assert_eq!(result.unwrap(),Some(reference(2,true)));assert_eq!(comparisons,1);assert_eq!(context.detected.len(),1);
    context.finish(equal_copy.key(),|_,_| Ok(true)).unwrap();assert_eq!(context.records.len(),2);
}

#[test]
fn reference_comparison_failure_leaves_both_tables_unchanged() {
    let first=record("R");let equal_copy=record("R");let mut context=ToConnectContext::new();
    context.register_record(first.key(),reference(1,false),&mut |_| Ok(())).unwrap();
    context.reference(first.key(),false,identity_equal,&mut |_| Ok(())).unwrap();
    context.register_record(equal_copy.key(),reference(2,false),&mut |_| Ok(())).unwrap();
    let result=context.reference(equal_copy.key(),true,|_,_| Err(error("equality-test","reject insertion comparison")),&mut |_| panic!("comparison failure must precede admission"));
    assert!(result.unwrap_err().code.ends_with("equality-test"));assert_eq!(context.detected.len(),1);assert_eq!(context.records.len(),2);
    assert_eq!(context.reference(equal_copy.key(),true,|_,_| Ok(true),&mut |_| panic!("equal detection must not allocate")).unwrap(),Some(reference(2,true)));
}

#[test]
fn a_root_clone_and_its_resolved_self_reference_share_the_canonical_identity() {
    let root=Schema::parse_str(r#"{"type":"record","name":"ns.Node","aliases":["OldNode"],"fields":[{"name":"next","type":["null","ns.Node"],"default":null}]}"#).unwrap();
    let mut definitions=BTreeMap::new();super::super::collect_names(&root,&mut definitions);
    let Schema::Record(record)=&root else {panic!("expected record")};let Schema::Union(union)=&record.fields[0].schema else {panic!("expected union")};
    let actual_ref=&union.variants()[1];let resolved=super::super::resolve(actual_ref,&definitions).unwrap();assert!(!std::ptr::eq(&root,resolved));
    let root_key=RecordIdentity::canonical(&root,&definitions).unwrap().unwrap();let ref_key=RecordIdentity::canonical(actual_ref,&definitions).unwrap().unwrap();
    assert!(std::ptr::eq(root_key.source,ref_key.source));assert!(std::ptr::eq(ref_key.source,resolved));
    let mut context=ToConnectContext::new();context.register_record(root_key,reference(19,false),&mut |_| Ok(())).unwrap();
    assert_eq!(context.reference(ref_key,true,identity_equal,&mut |_| Ok(())).unwrap(),Some(reference(19,true)));
    context.finish(root_key,identity_equal).unwrap();assert_eq!(context.records.len(),1);
    assert!(RecordIdentity::canonical(&root,&BTreeMap::new()).is_err());
}
