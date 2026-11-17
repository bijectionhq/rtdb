use super::*;
use super::super::default_input::DefaultAttempt;
use rtdb_columnar::MemoryBudget;

fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(usize::MAX)) }
fn accepted(value: Result<DefaultAttempt<()>>) { assert!(matches!(value.unwrap(), DefaultAttempt::Value(()))); }
fn empty_record(default: bool, context: &mut ConversionContext) -> (SchemaHandle, ObjectId) {
    let mut builder = SchemaObjectsBuilder::new(context).unwrap(); let object = builder.record(context).unwrap();
    if default { accepted(builder.default_with(object, 4096, context, || Ok(JavaValue::Struct { schema: object, values: Vec::new() }))); }
    let built = builder.build(object, context).unwrap(); (builder.finish(built, context).unwrap(), object)
}

#[test]
fn builder_identity_and_built_schema_class_remain_distinct() {
    let mut context = context(); let (schema, builder) = empty_record(false, &mut context);
    let original = schema.at(builder).unwrap();
    assert!(!original.ordinary_equals(schema.view(), &mut context).unwrap());
    assert!(!schema.view().ordinary_equals(original, &mut context).unwrap());
    assert!(original.property_equals(schema.view(), &mut context).unwrap());
    assert!(schema.view().property_equals(original, &mut context).unwrap());
}

#[test]
fn equal_built_records_do_not_make_independent_builders_equal() {
    let mut context = context(); let (left, left_builder) = empty_record(false, &mut context); let (right, right_builder) = empty_record(false, &mut context);
    assert!(left.view().ordinary_equals(right.view(), &mut context).unwrap());
    assert!(left.view().property_equals(right.view(), &mut context).unwrap());
    assert!(!left.at(left_builder).unwrap().ordinary_equals(right.at(right_builder).unwrap(), &mut context).unwrap());
}

#[test]
fn equal_payload_struct_defaults_retain_their_creating_builder_identity() {
    let mut context = context(); let (left, left_builder) = empty_record(true, &mut context); let (right, _) = empty_record(true, &mut context);
    assert!(!left.view().ordinary_equals(right.view(), &mut context).unwrap());
    assert!(!left.view().property_equals(right.view(), &mut context).unwrap());
    let body = left.view().body().unwrap(); let value = body.body.metadata.default.as_ref().unwrap();
    assert!(matches!(equality::validate(value, body.arena, left.at(left_builder).unwrap(), &mut context, 0).unwrap(), DefaultAttempt::Value(())));
    assert!(matches!(equality::validate(value, body.arena, left.view(), &mut context, 0).unwrap(), DefaultAttempt::Invalid(_)));
}

#[test]
fn wrapper_optionality_is_a_captured_override_and_ordinary_wrapper_equality_keeps_class() {
    let mut context = context(); let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap(); let object = builder.record(&mut context).unwrap();
    let required = builder.wrapper(object, false, &mut context).unwrap(); let optional = builder.wrapper(object, true, &mut context).unwrap(); let equal_required = builder.wrapper(object, false, &mut context).unwrap();
    builder.optional(object, true).unwrap(); let built = builder.build(object, &mut context).unwrap(); let schema = builder.finish(built, &mut context).unwrap();
    let required = schema.at(required).unwrap(); let optional = schema.at(optional).unwrap();
    assert!(!required.body().unwrap().optional); assert!(optional.body().unwrap().optional); assert!(schema.view().body().unwrap().optional);
    assert!(required.ordinary_equals(schema.at(equal_required).unwrap(), &mut context).unwrap());
    assert!(!required.ordinary_equals(optional, &mut context).unwrap()); assert!(!optional.ordinary_equals(schema.view(), &mut context).unwrap());
    assert!(optional.property_equals(schema.view(), &mut context).unwrap()); assert!(!required.property_equals(schema.view(), &mut context).unwrap());
}

fn recursive(context: &mut ConversionContext) -> SchemaHandle {
    let mut builder = SchemaObjectsBuilder::new(context).unwrap(); let object = builder.record(context).unwrap(); builder.name(object, Some("Node"), context).unwrap();
    let next = builder.wrapper(object, true, context).unwrap(); builder.field(object, "next", next, context).unwrap(); let built = builder.build(object, context).unwrap(); builder.finish(built, context).unwrap()
}
#[test]
fn recursive_property_equality_uses_admitted_pairs_without_erasing_builder_identity() {
    let mut context = context(); let left = recursive(&mut context); let right = recursive(&mut context);
    assert!(!left.view().ordinary_equals(right.view(), &mut context).unwrap());
    let before = context.allocation_remaining(); assert!(left.view().property_equals(right.view(), &mut context).unwrap()); assert!(context.allocation_remaining() < before);
    assert!(left.view().ordinary_equals(left.view(), &mut context).unwrap());
}

#[test]
fn imported_cached_objects_retain_exact_identity_and_their_original_leases() {
    let budget = MemoryBudget::new(usize::MAX); let mut first_context = ConversionContext::new(Arc::clone(&budget)); let (cached, _) = empty_record(true, &mut first_context);
    let owner = Arc::downgrade(&cached.arena); drop(first_context); assert!(budget.used_bytes() > 0);
    let mut second_context = ConversionContext::new(Arc::clone(&budget)); let mut builder = SchemaObjectsBuilder::new(&mut second_context).unwrap();
    let imported = builder.import(&cached, &mut second_context).unwrap(); assert_eq!(builder.import(&cached, &mut second_context).unwrap(), imported);
    let published = builder.finish(imported, &mut second_context).unwrap(); assert!(published.view().ordinary_equals(cached.view(), &mut second_context).unwrap());
    drop(cached); drop(second_context); assert!(owner.upgrade().is_some()); assert!(budget.used_bytes() > 0);
    drop(published); assert!(owner.upgrade().is_none()); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn rejected_admission_never_constructs_a_default_or_publishes_an_object() {
    let budget = MemoryBudget::new(65536); let mut context = ConversionContext::with_limit(Arc::clone(&budget), 65536); let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap();
    let object = builder.record(&mut context).unwrap(); context.reserve(context.allocation_remaining()).unwrap(); let mut constructed = false;
    assert!(builder.default_with(object, 1, &mut context, || { constructed = true; Ok(JavaValue::Null) }).is_err()); assert!(!constructed);
    let count = builder.arena.objects.len(); assert!(builder.record(&mut context).is_err()); assert_eq!(builder.arena.objects.len(), count);
    drop(context); assert!(budget.used_bytes() > 0); drop(builder); assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn region_ids_reject_foreign_objects_before_storing_an_edge() {
    let mut context = context(); let mut left = SchemaObjectsBuilder::new(&mut context).unwrap(); let mut right = SchemaObjectsBuilder::new(&mut context).unwrap();
    let local = left.record(&mut context).unwrap(); let foreign = right.record(&mut context).unwrap(); let count = left.arena.objects.len();
    assert!(left.field(local, "foreign", foreign, &mut context).is_err()); assert!(left.wrapper(foreign, false, &mut context).is_err()); assert_eq!(left.arena.objects.len(), count);
    let built = left.build(local, &mut context).unwrap(); let schema = left.finish(built, &mut context).unwrap(); assert!(schema.at(foreign).is_err());
}

#[test]
fn publication_requires_completed_builders_and_completed_builders_reject_mutation() {
    let mut context = context(); let mut incomplete = SchemaObjectsBuilder::new(&mut context).unwrap(); let root = incomplete.record(&mut context).unwrap(); assert!(incomplete.finish(root, &mut context).is_err());
    let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap(); let root = builder.record(&mut context).unwrap(); let built = builder.build(root, &mut context).unwrap();
    assert!(builder.optional(root, true).is_err()); assert!(builder.name(root, Some("late"), &mut context).is_err()); assert!(builder.build(root, &mut context).is_err());
    assert!(builder.finish(built, &mut context).is_ok());
}

fn scalar_default(kind: Scalar, context: &mut ConversionContext, value: impl FnOnce() -> JavaValue) -> SchemaHandle {
    let mut builder = SchemaObjectsBuilder::new(context).unwrap(); let object = builder.scalar(kind, context).unwrap(); accepted(builder.default_with(object, 16384, context, || Ok(value())));
    let built = builder.build(object, context).unwrap(); builder.finish(built, context).unwrap()
}
#[test]
fn scalar_default_equality_keeps_nan_equivalence_and_signed_zero_distinct() {
    let mut context = context();
    let left = scalar_default(Scalar::F64, &mut context, || JavaValue::F64(f64::from_bits(0x7ff8_0000_0000_0001))); let right = scalar_default(Scalar::F64, &mut context, || JavaValue::F64(f64::from_bits(0x7ff8_0000_0000_0002)));
    assert!(left.view().ordinary_equals(right.view(), &mut context).unwrap());
    let left = scalar_default(Scalar::F64, &mut context, || JavaValue::F64(0.0)); let right = scalar_default(Scalar::F64, &mut context, || JavaValue::F64(-0.0));
    assert!(!left.view().ordinary_equals(right.view(), &mut context).unwrap());
}

#[test]
fn default_validation_preserves_exact_primitive_and_logical_classes() {
    let mut context = context(); let mut builder = SchemaObjectsBuilder::new(&mut context).unwrap(); let byte = builder.scalar(Scalar::I8, &mut context).unwrap();
    assert!(matches!(builder.default_with(byte, 4096, &mut context, || Ok(JavaValue::I32(1))).unwrap(), DefaultAttempt::Invalid(_)));
    accepted(builder.default_with(byte, 4096, &mut context, || Ok(JavaValue::I8(1))));
    let date = builder.scalar(Scalar::I32, &mut context).unwrap(); builder.name(date, Some("org.apache.kafka.connect.data.Time"), &mut context).unwrap();
    assert!(matches!(builder.default_with(date, 4096, &mut context, || Ok(JavaValue::I32(1))).unwrap(), DefaultAttempt::Invalid(_)));
    accepted(builder.default_with(date, 4096, &mut context, || Ok(JavaValue::Date(i64::MAX))));
    builder.build(byte, &mut context).unwrap(); let root = builder.build(date, &mut context).unwrap(); assert!(builder.finish(root, &mut context).is_ok());
}

fn record_array_default(context: &mut ConversionContext) -> SchemaHandle {
    let mut builder=SchemaObjectsBuilder::new(context).unwrap();let record=builder.record(context).unwrap();let record=builder.build(record,context).unwrap();
    let array=builder.array(record,context).unwrap();accepted(builder.default_with(array,16384,context,|| Ok(JavaValue::Array(vec![JavaValue::Struct{schema:record,values:Vec::new()}]))));
    let built=builder.build(array,context).unwrap();builder.finish(built,context).unwrap()
}
#[test]
fn nested_struct_values_can_match_independent_built_schemas_without_erasing_builder_identity() {
    let mut context=context();let left=record_array_default(&mut context);let right=record_array_default(&mut context);
    assert!(left.view().ordinary_equals(right.view(),&mut context).unwrap());assert!(left.view().property_equals(right.view(),&mut context).unwrap());
}

#[test]
fn map_validation_checks_each_key_before_an_unchecked_logical_value_cast_and_skips_empty_maps() {
    let mut context=context();
    for empty in [true,false] {
        let mut builder=SchemaObjectsBuilder::new(&mut context).unwrap();let key=builder.scalar(Scalar::String,&mut context).unwrap();builder.name(key,Some("org.apache.kafka.connect.data.Date"),&mut context).unwrap();let key=builder.build(key,&mut context).unwrap();
        let element=builder.record(&mut context).unwrap();builder.name(element,Some("org.apache.kafka.connect.data.Date"),&mut context).unwrap();let element=builder.build(element,&mut context).unwrap();let map=builder.map(key,element,&mut context).unwrap();
        let result=builder.default_with(map,16384,&mut context,|| Ok(JavaValue::Map(if empty {Vec::new()} else {vec![(JavaValue::String("key".into()),JavaValue::Date(0))]}))).unwrap();
        assert_eq!(matches!(result,DefaultAttempt::Value(())),empty);let built=builder.build(map,&mut context).unwrap();assert!(builder.finish(built,&mut context).is_ok());
    }
}

fn decimal_default(coefficient:&[u8],scale:i32,context:&mut ConversionContext)->SchemaHandle {
    let mut builder=SchemaObjectsBuilder::new(context).unwrap();let object=builder.scalar(Scalar::Bytes,context).unwrap();builder.name(object,Some("org.apache.kafka.connect.data.Decimal"),context).unwrap();
    accepted(builder.default_with(object,16384,context,|| Ok(JavaValue::Decimal(BigDecimal::new(coefficient.to_vec(),scale)?))));let built=builder.build(object,context).unwrap();builder.finish(built,context).unwrap()
}
#[test]
fn decimal_defaults_compare_coefficient_values_and_scale_independently_of_storage_width() {
    let mut context=context();
    for (left,right) in [(&[0,1][..],&[1][..]),(&[255,255][..],&[255][..])] {
        let left=decimal_default(left,2,&mut context);let right=decimal_default(right,2,&mut context);assert!(left.view().ordinary_equals(right.view(),&mut context).unwrap());
    }
    let left=decimal_default(&[1],2,&mut context);let right=decimal_default(&[1],3,&mut context);assert!(!left.view().ordinary_equals(right.view(),&mut context).unwrap());
    let left=scalar_default(Scalar::F32,&mut context,|| JavaValue::F32(f32::from_bits(0x7fc0_0001)));let right=scalar_default(Scalar::F32,&mut context,|| JavaValue::F32(f32::from_bits(0x7fc0_0002)));assert!(left.view().ordinary_equals(right.view(),&mut context).unwrap());
}

fn decorated(order:&[&str],version:i32,context:&mut ConversionContext)->SchemaHandle {
    let mut builder=SchemaObjectsBuilder::new(context).unwrap();let scalar=builder.scalar(Scalar::I64,context).unwrap();let scalar=builder.build(scalar,context).unwrap();let root=builder.record(context).unwrap();
    builder.name(root,Some("R"),context).unwrap();builder.version(root,Some(version)).unwrap();builder.doc(root,Some("record"),context).unwrap();builder.parameter(root,"key","value",context).unwrap();
    for name in order {builder.field(root,name,scalar,context).unwrap();}let built=builder.build(root,context).unwrap();builder.finish(built,context).unwrap()
}
#[test]
fn schema_metadata_and_field_position_participate_in_both_built_relations() {
    let mut context=context();let left=decorated(&["a","b"],1,&mut context);let equal=decorated(&["a","b"],1,&mut context);let reordered=decorated(&["b","a"],1,&mut context);let versioned=decorated(&["a","b"],2,&mut context);
    assert!(left.view().ordinary_equals(equal.view(),&mut context).unwrap());assert!(left.view().property_equals(equal.view(),&mut context).unwrap());
    for right in [&reordered,&versioned] {assert!(!left.view().ordinary_equals(right.view(),&mut context).unwrap());assert!(!left.view().property_equals(right.view(),&mut context).unwrap());}
}

#[test]
fn null_default_validation_and_struct_unset_slots_use_their_actual_schema_stage() {
    let mut context=context();let mut builder=SchemaObjectsBuilder::new(&mut context).unwrap();let child=builder.scalar(Scalar::I32,&mut context).unwrap();
    assert!(matches!(builder.default_with(child,4096,&mut context,|| Ok(JavaValue::Null)).unwrap(),DefaultAttempt::Invalid(_)));
    accepted(builder.default_with(child,4096,&mut context,|| Ok(JavaValue::I32(7))));let child=builder.build(child,&mut context).unwrap();
    let root=builder.record(&mut context).unwrap();builder.field(root,"child",child,&mut context).unwrap();accepted(builder.default_with(root,16384,&mut context,|| Ok(JavaValue::Struct{schema:root,values:vec![JavaValue::Null]})));
    let optional=builder.scalar(Scalar::I32,&mut context).unwrap();builder.optional(optional,true).unwrap();accepted(builder.default_with(optional,4096,&mut context,|| Ok(JavaValue::Null)));builder.build(optional,&mut context).unwrap();
    let built=builder.build(root,&mut context).unwrap();let schema=builder.finish(built,&mut context).unwrap();assert!(schema.at(optional).unwrap().body().unwrap().body.metadata.default.is_none());
}

#[derive(Clone,Copy)]
enum BinaryShape { Direct, List, Map, StructField, StructListField }
fn binary_default(buffer:bool,shape:BinaryShape,context:&mut ConversionContext)->SchemaHandle {
    let mut builder=SchemaObjectsBuilder::new(context).unwrap();let value=if buffer {builder.byte_buffer(&[1,2,3],context).unwrap()} else {builder.byte_array(&[1,2,3],context).unwrap()};
    let bytes=builder.scalar(Scalar::Bytes,context).unwrap();let mut record_id=None;
    let root=match shape {
        BinaryShape::Direct=>bytes,
        BinaryShape::List=>{let bytes=builder.build(bytes,context).unwrap();builder.array(bytes,context).unwrap()},
        BinaryShape::Map=>{let bytes=builder.build(bytes,context).unwrap();let key=builder.scalar(Scalar::String,context).unwrap();let key=builder.build(key,context).unwrap();builder.map(key,bytes,context).unwrap()},
        BinaryShape::StructField|BinaryShape::StructListField=>{
            let mut field=builder.build(bytes,context).unwrap();
            if matches!(shape,BinaryShape::StructListField) {let array=builder.array(field,context).unwrap();field=builder.build(array,context).unwrap();}
            let record=builder.record(context).unwrap();builder.field(record,"bytes",field,context).unwrap();let record=builder.build(record,context).unwrap();record_id=Some(record);builder.array(record,context).unwrap()
        }
    };
    accepted(builder.default_with(root,16384,context,|| Ok(match shape {
        BinaryShape::Direct=>value,
        BinaryShape::List=>JavaValue::Array(vec![value]),
        BinaryShape::Map=>JavaValue::Map(vec![(JavaValue::String("key".into()),value)]),
        BinaryShape::StructField|BinaryShape::StructListField=>{
            let value=if matches!(shape,BinaryShape::StructListField) {JavaValue::Array(vec![value])} else {value};
            JavaValue::Array(vec![JavaValue::Struct{schema:record_id.unwrap(),values:vec![value]}])
        }
    })));let built=builder.build(root,context).unwrap();builder.finish(built,context).unwrap()
}
#[test]
fn byte_array_deep_equality_stops_at_list_and_map_equals_but_struct_fields_are_deep() {
    let mut context=context();
    for (shape,equal) in [(BinaryShape::Direct,true),(BinaryShape::List,false),(BinaryShape::Map,false),(BinaryShape::StructField,true),(BinaryShape::StructListField,false)] {
        let left=binary_default(false,shape,&mut context);let right=binary_default(false,shape,&mut context);
        assert_eq!(left.view().ordinary_equals(right.view(),&mut context).unwrap(),equal);assert_eq!(left.view().property_equals(right.view(),&mut context).unwrap(),equal);
    }
}
#[test]
fn byte_buffer_content_equality_applies_through_list_map_and_struct_values() {
    let mut context=context();
    for shape in [BinaryShape::Direct,BinaryShape::List,BinaryShape::Map,BinaryShape::StructField,BinaryShape::StructListField] {
        let left=binary_default(true,shape,&mut context);let right=binary_default(true,shape,&mut context);assert!(left.view().ordinary_equals(right.view(),&mut context).unwrap());
    }
}
#[test]
fn byte_buffer_layout_preserves_backing_identity_and_remaining_range_without_a_payload_clone() {
    let budget=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&budget));let mut builder=SchemaObjectsBuilder::new(&mut context).unwrap();
    let JavaValue::ByteArray(storage)=builder.byte_array(&[9,1,2,8],&mut context).unwrap() else {panic!("expected byte array")};
    let window=ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Heap{offset:1,read_only:false},2,0,2).unwrap();
    assert_eq!(window.remaining(),&[1,2]);assert!(Arc::ptr_eq(window.backing_array().unwrap(),&storage));assert_eq!(window.backing_array().unwrap().bytes(),&[9,1,2,8]);
    assert_eq!((window.capacity(),window.position(),window.limit()),(2,0,2));assert!(!window.read_only());
    let shifted=ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Heap{offset:0,read_only:true},4,1,3).unwrap();assert_eq!(window.remaining(),shifted.remaining());assert!(shifted.backing_array().is_err());assert!(shifted.read_only());
    let direct=ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Direct{read_only:false},4,1,3).unwrap();assert_eq!(window.remaining(),direct.remaining());assert!(direct.backing_array().is_err());
    let marked=ByteBuffer::state(Arc::clone(&storage),BufferBacking::Heap{offset:0,read_only:false},4,1,3,Some(1),BufferOrder::LittleEndian).unwrap();assert_eq!(marked.mark(),Some(1));assert_eq!(marked.order(),BufferOrder::LittleEndian);assert_eq!(marked.remaining(),window.remaining());
    assert!(ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Heap{offset:usize::MAX,read_only:false},2,0,2).is_err());assert!(ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Direct{read_only:false},4,3,2).is_err());
    assert!(ByteBuffer::state(Arc::clone(&storage),BufferBacking::Direct{read_only:false},4,1,3,Some(2),BufferOrder::BigEndian).is_err());
    drop(builder);drop(context);assert!(budget.used_bytes()>0);drop(storage);drop(window);drop(shifted);drop(direct);drop(marked);assert_eq!(budget.used_bytes(),0);
}
#[test]
fn source_bigdecimal_defaults_remain_admitted_beyond_the_later_native_writer_domain() {
    let budget=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&budget));let mut builder=SchemaObjectsBuilder::new(&mut context).unwrap();let object=builder.scalar(Scalar::Bytes,&mut context).unwrap();builder.name(object,Some("org.apache.kafka.connect.data.Decimal"),&mut context).unwrap();
    accepted(builder.default_with(object,16384,&mut context,|| Ok(JavaValue::Decimal(BigDecimal::new(vec![1;128],7)?))));let built=builder.build(object,&mut context).unwrap();let schema=builder.finish(built,&mut context).unwrap();drop(context);
    let body=schema.view().body().unwrap();let Some(JavaValue::Decimal(decimal))=&body.body.metadata.default else {panic!("expected source BigDecimal")};assert_eq!(decimal.coefficient().len(),128);assert_eq!(decimal.scale(),7);assert!(budget.used_bytes()>0);drop(schema);assert_eq!(budget.used_bytes(),0);
}

fn buffer_default(buffer:ByteBuffer,context:&mut ConversionContext)->SchemaHandle {
    let mut builder=SchemaObjectsBuilder::new(context).unwrap();let object=builder.scalar(Scalar::Bytes,context).unwrap();accepted(builder.default_with(object,4096,context,|| Ok(JavaValue::ByteBuffer(buffer))));let built=builder.build(object,context).unwrap();builder.finish(built,context).unwrap()
}
#[test]
fn byte_buffer_defaults_compare_remaining_content_across_layouts_and_retain_external_backing_charges() {
    let budget=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&budget));let mut source=SchemaObjectsBuilder::new(&mut context).unwrap();
    let JavaValue::ByteArray(storage)=source.byte_array(&[9,1,2,8],&mut context).unwrap() else {panic!("expected byte array")};
    let left=buffer_default(ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Heap{offset:1,read_only:false},2,0,2).unwrap(),&mut context);
    let right=buffer_default(ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Direct{read_only:true},4,1,3).unwrap(),&mut context);
    let different=buffer_default(ByteBuffer::layout(Arc::clone(&storage),BufferBacking::Heap{offset:0,read_only:false},4,0,2).unwrap(),&mut context);
    assert!(left.view().ordinary_equals(right.view(),&mut context).unwrap());assert!(!left.view().ordinary_equals(different.view(),&mut context).unwrap());
    drop(source);drop(storage);drop(context);assert!(budget.used_bytes()>0);
    let own=left.arena.charges.iter().map(|charge|charge.bytes()).sum::<usize>();let scratch=capacity(&Vec::<RegionId>::new()).unwrap().unwrap().1;
    let mut limited=ConversionContext::with_limit(Arc::clone(&budget),own+scratch);assert!(left.retain(&mut limited).unwrap_err().code.ends_with("batch-too-large"));drop(limited);
    let mut fresh=ConversionContext::new(Arc::clone(&budget));let before=fresh.allocation_remaining();left.retain(&mut fresh).unwrap();assert!(fresh.allocation_remaining()<before);drop(fresh);
    drop(left);drop(right);drop(different);assert_eq!(budget.used_bytes(),0);
}

#[test]
fn a_later_declared_logical_name_does_not_revalidate_an_installed_primitive_default() {
    let mut context=context();let mut builder=SchemaObjectsBuilder::new(&mut context).unwrap();let object=builder.scalar(Scalar::I32,&mut context).unwrap();
    accepted(builder.default_with(object,4096,&mut context,|| Ok(JavaValue::I32(86400001))));builder.name(object,Some("org.apache.kafka.connect.data.Time"),&mut context).unwrap();
    let built=builder.build(object,&mut context).unwrap();let schema=builder.finish(built,&mut context).unwrap();let body=schema.view().body().unwrap();let value=body.body.metadata.default.as_ref().unwrap();assert!(matches!(value,JavaValue::I32(86400001)));
    assert!(matches!(equality::validate(value,body.arena,schema.view(),&mut context,0).unwrap(),DefaultAttempt::Invalid(_)));
}

#[test]
fn escaped_ids_do_not_become_valid_after_their_original_owner_drops() {
    let mut context=context();
    let stale={let mut previous=SchemaObjectsBuilder::new(&mut context).unwrap();previous.record(&mut context).unwrap()};
    let mut current=SchemaObjectsBuilder::new(&mut context).unwrap();let root=current.record(&mut context).unwrap();
    assert_eq!(stale.index,root.index);assert_ne!(stale.region,root.region);assert!(current.field(root,"stale",stale,&mut context).is_err());assert!(current.wrapper(stale,false,&mut context).is_err());
    let built=current.build(root,&mut context).unwrap();let schema=current.finish(built,&mut context).unwrap();assert!(schema.at(stale).is_err());
}
#[test]
fn the_region_identity_allocator_refuses_exhaustion_without_wrapping_or_reusing_ids() {
    let counter=AtomicU64::new(u64::MAX-1);assert_eq!(take_region_id(&counter).unwrap(),RegionId(u64::MAX-1));
    assert!(take_region_id(&counter).is_err());assert!(take_region_id(&counter).is_err());assert_eq!(counter.load(Ordering::Relaxed),u64::MAX);
}
