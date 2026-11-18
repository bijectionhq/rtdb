use super::*;
use super::hashing::Comparable;
use rtdb_columnar::MemoryBudget;

fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(usize::MAX)) }
fn owner(context: &mut ConversionContext) -> SchemaHandle {
    let mut builder = SchemaObjectsBuilder::new(context).unwrap();
    let root = builder.scalar(Scalar::String, context).unwrap();
    let root = builder.build(root, context).unwrap();
    builder.finish(root, context).unwrap()
}
fn exact(units: &[u16], context: &mut ConversionContext) -> JavaValue {
    JavaValue::string(JavaText::from_utf16(units), context).unwrap()
}
struct StringWitness;
impl IdentityWitness for StringWitness {
    fn type_hash(&mut self, _: SchemaType, _: &mut ConversionContext) -> Result<i32> { Ok(7) }
    fn builder_hash(&mut self, _: ObjectView<'_>, _: &mut ConversionContext) -> Result<i32> { Err(invalid("Unexpected builder hash")) }
    fn byte_array_hash(&mut self, _: &Arc<ByteStorage>, _: &mut ConversionContext) -> Result<i32> { Err(invalid("Unexpected binary hash")) }
    fn key_hash(&mut self, _: &JavaKey, _: &mut ConversionContext) -> Result<i32> { Err(invalid("Unexpected identity tie")) }
    fn pair_hash(&mut self, _: KeyIdentity, _: &mut ConversionContext) -> Result<i32> { Err(invalid("Unexpected pair hash")) }
}

#[test]
fn exact_string_values_keep_unmatched_units_and_their_lease_after_the_context() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let value = exact(&[0xd800, 65, 0xdc00], &mut context);
    drop(context);
    assert!(value.string_text().unwrap().units().eq([0xd800, 65, 0xdc00]));
    assert!(budget.used_bytes() > 0);
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(value.retain(&mut denied, 0).unwrap_err().code.ends_with("batch-too-large"));
    drop(value);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn both_string_representations_share_equality_hash_comparable_and_validation() {
    let mut context = context();
    let arena = owner(&mut context);
    let represented = exact(&[65, 0xd800, 0xdc00], &mut context);
    let legacy = JavaValue::String("A𐀀".into());
    let high = exact(&[0xd800], &mut context);
    let bmp = JavaValue::String("\u{e000}".into());
    let mut evaluation = Evaluation { context: &mut context, witness: &mut MissingIdentityWitness };
    assert!(evaluation.values_equal(&represented, arena.owner(), &legacy, arena.owner(), false).unwrap());
    assert!(evaluation.values_equal(&legacy, arena.owner(), &represented, arena.owner(), false).unwrap());
    assert_eq!(evaluation.value_hash(&represented, arena.owner()).unwrap(), 1832961);
    assert_eq!(evaluation.value_hash(&legacy, arena.owner()).unwrap(), 1832961);
    assert_eq!(evaluation.value_hash(&high, arena.owner()).unwrap(), 55296);
    assert_eq!(hashing::compare(Comparable::String, &represented, &legacy, evaluation.context).unwrap(), 0);
    assert_eq!(hashing::compare(Comparable::String, &high, &bmp, evaluation.context).unwrap(), -2048);
    assert!(matches!(evaluation.validate_value(&represented, arena.owner(), arena.view()).unwrap(), super::super::default_input::DefaultAttempt::Value(())));
}

fn metadata(context: &mut ConversionContext, parameter_value: &[u16]) -> SchemaHandle {
    let mut builder = SchemaObjectsBuilder::new(context).unwrap();
    let child = builder.scalar(Scalar::String, context).unwrap();
    let child = builder.build(child, context).unwrap();
    let root = builder.record(context).unwrap();
    builder.field_text(root, JavaText::from_utf16(&[0xd800]), child, context).unwrap();
    let before_duplicate = context.allocation_remaining();
    assert!(builder.field_text(root, JavaText::from_utf16(&[0xd800]), child, context).is_err());
    assert_eq!(context.allocation_remaining(), before_duplicate);
    builder.name_text(root, Some(JavaText::from_utf16(&[0xdc00])), context).unwrap();
    builder.doc_text(root, Some(JavaText::from_utf16(&[0xd800])), context).unwrap();
    builder.parameter_text(root, JavaText::from_utf16(&[0xdc00]), JavaText::from_utf16(parameter_value), context).unwrap();
    let built = builder.build(root, context).unwrap();
    builder.finish(built, context).unwrap()
}

#[test]
fn field_metadata_and_parameter_units_participate_in_schema_relations_and_hashes() {
    let mut context = context();
    let left = metadata(&mut context, &[0xd800]);
    let equal = metadata(&mut context, &[0xd800]);
    let different = metadata(&mut context, &[0xfffd]);
    assert!(left.view().ordinary_equals(equal.view(), &mut context).unwrap());
    assert!(!left.view().ordinary_equals(different.view(), &mut context).unwrap());
    let body = left.view().body().unwrap();
    assert!(body.body.metadata.name.as_ref().unwrap().view().units().eq([0xdc00]));
    assert!(body.body.metadata.doc.as_ref().unwrap().view().units().eq([0xd800]));
    let Kind::Struct(fields) = &body.body.kind else { panic!("expected struct") };
    assert!(fields[0].name.view().units().eq([0xd800]));
    let (key, value) = body.body.metadata.parameters.as_ref().unwrap().iter().next().unwrap();
    assert!(key.view().units().eq([0xdc00]));
    assert!(value.view().units().eq([0xd800]));
    let mut evaluation = Evaluation { context: &mut context, witness: &mut StringWitness };
    assert_eq!(evaluation.schema_hash(left.view()).unwrap(), -597570660);
}

#[test]
fn exact_and_legacy_equal_string_keys_replace_values_and_keep_the_first_key() {
    let mut context = context();
    let arena = owner(&mut context);
    let payload = context.reserve(65536).unwrap();
    let first = JavaKey::new(JavaValue::String("A".into()), arena.owner(), Arc::clone(&payload), &mut context).unwrap();
    let first_id = first.identity();
    let represented = exact(&[65], &mut context);
    let replacement = JavaKey::new(represented, arena.owner(), Arc::clone(&payload), &mut context).unwrap();
    let mut evaluation = Evaluation { context: &mut context, witness: &mut MissingIdentityWitness };
    let mut map = JavaMapBuilder::new(None, arena.owner(), Arc::clone(&payload)).unwrap();
    assert!(map.put(first, JavaValue::I32(1), arena.owner(), &mut evaluation).unwrap().is_none());
    assert!(matches!(map.put(replacement, JavaValue::I32(2), arena.owner(), &mut evaluation).unwrap(), Some(previous) if matches!(previous.value(), JavaValue::I32(1))));
    let map = map.finish(&mut evaluation).unwrap();
    assert_eq!(map.len(), 1);
    assert_eq!(map.entries().next().unwrap().0.identity(), first_id);
    assert!(matches!(map.iter().next().unwrap().1, JavaValue::I32(2)));
}

#[test]
fn exact_string_collision_trees_need_no_invented_identity_observation() {
    let mut context = context();
    let arena = owner(&mut context);
    let payload = context.reserve(65536).unwrap();
    let mut keys = Vec::new();
    for index in 0..16 {
        let mut units = vec![0xd800];
        for bit in 0..4 { units.extend(if index & (1 << bit) == 0 { [65, 97] } else { [66, 66] }); }
        let value = exact(&units, &mut context);
        keys.push(JavaKey::new(value, arena.owner(), Arc::clone(&payload), &mut context).unwrap());
    }
    let mut evaluation = Evaluation { context: &mut context, witness: &mut MissingIdentityWitness };
    let mut map = JavaMapBuilder::new(Some(64), arena.owner(), Arc::clone(&payload)).unwrap();
    let first_hash = evaluation.value_hash(keys[0].value(), arena.owner()).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(evaluation.value_hash(key.value(), arena.owner()).unwrap(), first_hash);
        assert!(map.put(key.clone(), JavaValue::I32(index as i32), arena.owner(), &mut evaluation).unwrap().is_none());
    }
    let map = map.finish(&mut evaluation).unwrap();
    assert_eq!(map.len(), 16);
    for (index, key) in keys.iter().enumerate() {
        assert!(matches!(map.get(key, arena.owner().arena, arena.owner().arena, &mut evaluation, 0).unwrap(), Some(JavaValue::I32(value)) if *value == index as i32));
    }
}

#[test]
fn imported_schema_families_retain_their_actual_metadata_and_field_text() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let source = metadata(&mut context, &[0xd800]);
    let weak = Arc::downgrade(&source.arena);
    let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap();
    let imported = builder.import(&source, &mut context).unwrap();
    let family = builder.finish(imported, &mut context).unwrap();
    drop(source);
    drop(context);
    assert!(weak.upgrade().is_some());
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(family.retain(&mut denied).is_err());
    let mut fresh = ConversionContext::new(Arc::clone(&budget));
    family.retain(&mut fresh).unwrap();
    assert!(family.view().body().unwrap().body.metadata.name.as_ref().unwrap().view().units().eq([0xdc00]));
    drop(family);
    assert!(weak.upgrade().is_none());
    assert!(budget.used_bytes() > 0);
    drop(fresh);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn map_contents_retention_counts_exact_text_after_the_unchanged_storage_scope() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let arena = owner(&mut context);
    let payload = context.reserve(65536).unwrap();
    let key_text = exact(&[0xd800], &mut context);
    let value = exact(&[0xdc00], &mut context);
    let key = JavaKey::new(key_text, arena.owner(), Arc::clone(&payload), &mut context).unwrap();
    let mut evaluation = Evaluation { context: &mut context, witness: &mut MissingIdentityWitness };
    let mut builder = JavaMapBuilder::new(None, arena.owner(), Arc::clone(&payload)).unwrap();
    builder.put(key, value, arena.owner(), &mut evaluation).unwrap();
    let map = builder.finish(&mut evaluation).unwrap();
    drop(evaluation);
    drop(payload);
    drop(arena);
    drop(context);
    let mut storage = ConversionContext::new(Arc::clone(&budget));
    map.retain(&mut storage).unwrap();
    let storage_bytes = storage.allocation_limit() - storage.allocation_remaining();
    drop(storage);
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), storage_bytes);
    assert!(map.retain_contents(&mut denied).unwrap_err().code.ends_with("batch-too-large"));
    assert_eq!(denied.allocation_remaining(), 0);
    drop(denied);
    let before = budget.used_bytes();
    let mut fresh = ConversionContext::new(Arc::clone(&budget));
    map.retain_contents(&mut fresh).unwrap();
    let remaining = fresh.allocation_remaining();
    map.retain_contents(&mut fresh).unwrap();
    assert_eq!(fresh.allocation_remaining(), remaining);
    assert_eq!(budget.used_bytes(), before);
    assert!(map.iter().next().unwrap().0.string_text().unwrap().units().eq([0xd800]));
    assert!(map.iter().next().unwrap().1.string_text().unwrap().units().eq([0xdc00]));
    drop(map);
    assert!(budget.used_bytes() > 0);
    drop(fresh);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn replaced_exact_string_values_keep_their_text_owner_and_recount_it_after_map_drop() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let arena = owner(&mut context);
    let payload = context.reserve(4096).unwrap();
    let payload_bytes = payload.bytes();
    let first = JavaKey::new(JavaValue::I64(1), arena.owner(), Arc::clone(&payload), &mut context).unwrap();
    let replacement = JavaKey::new(JavaValue::I64(1), arena.owner(), Arc::clone(&payload), &mut context).unwrap();
    let first_value = exact(&[0xd800], &mut context);
    let next_value = exact(&[0xdc00], &mut context);
    let mut evaluation = Evaluation { context: &mut context, witness: &mut MissingIdentityWitness };
    let mut builder = JavaMapBuilder::new(None, arena.owner(), Arc::clone(&payload)).unwrap();
    builder.put(first, first_value, arena.owner(), &mut evaluation).unwrap();
    let previous = builder.put(replacement, JavaValue::Array(vec![next_value]), arena.owner(), &mut evaluation).unwrap().unwrap();
    let map = builder.finish(&mut evaluation).unwrap();
    drop(evaluation);
    drop(payload);
    drop(arena);
    drop(context);
    drop(map);
    assert!(previous.value().string_text().unwrap().units().eq([0xd800]));
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), payload_bytes);
    assert!(previous.retain(&mut denied).unwrap_err().code.ends_with("batch-too-large"));
    assert_eq!(denied.allocation_remaining(), 0);
    drop(denied);
    let mut fresh = ConversionContext::new(Arc::clone(&budget));
    previous.retain(&mut fresh).unwrap();
    drop(previous);
    assert!(budget.used_bytes() > 0);
    drop(fresh);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn exact_text_admission_failure_leaves_builder_metadata_unassigned() {
    let budget = MemoryBudget::new(65536);
    let mut context = ConversionContext::with_limit(Arc::clone(&budget), 65536);
    let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap();
    let root = builder.scalar(Scalar::String, &mut context).unwrap();
    context.reserve(context.allocation_remaining()).unwrap();
    assert!(builder.name_text(root, Some(JavaText::from_utf16(&[0xd800])), &mut context).is_err());
    assert!(builder.view(root).unwrap().body().unwrap().body.metadata.name.is_none());
    assert!(JavaValue::string(JavaText::from_utf16(&[0xdc00]), &mut context).is_err());
    assert_eq!(context.allocation_remaining(), 0);
}

#[test]
fn installed_exact_string_defaults_keep_their_units_and_charge_in_cached_schemas() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap();
    let root = builder.scalar(Scalar::String, &mut context).unwrap();
    let mut name = "org.apache.kafka.connect.data.Time".encode_utf16().collect::<Vec<_>>();
    name.push(0xd800);
    builder.name_text(root, Some(JavaText::from_utf16(&name)), &mut context).unwrap();
    let value = exact(&[0xd800, 0xdc00, 0xd800], &mut context);
    assert!(matches!(builder.default_with(root, 4096, &mut context, || Ok(value)).unwrap(), super::super::default_input::DefaultAttempt::Value(())));
    let built = builder.build(root, &mut context).unwrap();
    let schema = builder.finish(built, &mut context).unwrap();
    drop(context);
    let default = schema.view().body().unwrap().body.metadata.default.as_ref().unwrap();
    assert!(default.string_text().unwrap().units().eq([0xd800, 0xdc00, 0xd800]));
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(schema.retain(&mut denied).is_err());
    drop(denied);
    let mut fresh = ConversionContext::new(Arc::clone(&budget));
    schema.retain(&mut fresh).unwrap();
    assert!(budget.used_bytes() > 0);
    drop(schema);
    assert!(budget.used_bytes() > 0);
    drop(fresh);
    assert_eq!(budget.used_bytes(), 0);
}
