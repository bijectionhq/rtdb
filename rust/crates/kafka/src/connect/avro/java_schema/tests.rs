use super::*;
use test_support::*;

#[test]
fn names_preserve_namespace_utf16_and_dotted_override_without_validating_namespace_chars() {
    let mut context=context();let namespace=[0xd800,46,63];let named=Name::new(Some(JavaText::from_utf8("R")),Some(JavaText::from_utf16(&namespace)),&mut context).unwrap();assert_eq!(named.full().unwrap().view().units().collect::<Vec<_>>(),vec![0xd800,46,63,46,82]);
    let dotted=Name::new(Some(JavaText::from_utf8("other.R")),Some(JavaText::from_utf16(&namespace)),&mut context).unwrap();assert_eq!(dotted.full().unwrap().view().units().collect::<Vec<_>>(),"other.R".encode_utf16().collect::<Vec<_>>());let empty=Name::new(Some(JavaText::from_utf8(".R")),Some(JavaText::from_utf8("ignored")),&mut context).unwrap();assert_eq!(empty.full().unwrap().view().units().collect::<Vec<_>>(),vec![82]);assert!(Name::new(Some(JavaText::from_utf16(&[0xd800])),None,&mut context).is_err());assert!(Name::new(None,Some(JavaText::from_utf16(&namespace)),&mut context).unwrap().full().is_none());
}

#[test]
fn canonical_references_reuse_named_objects_and_primitive_lookups_create_new_objects() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let record=builder.record(name("ns.R",&mut context),false,&mut context).unwrap();assert_eq!(builder.reference(JavaText::from_utf8("R"),Some(JavaText::from_utf8("ns")),&mut context).unwrap(),record);assert_eq!(builder.reference(JavaText::from_utf8("ns.R"),Some(JavaText::from_utf8("ignored")),&mut context).unwrap(),record);let first=builder.reference(JavaText::from_utf8("int"),None,&mut context).unwrap();let second=builder.reference(JavaText::from_utf8("int"),None,&mut context).unwrap();assert_ne!(first,second);builder.fields_with(record,0,&mut context,||Ok(Vec::new())).unwrap();let source=builder.finish(record,&mut context).unwrap();assert!(source.at(record).unwrap().same_object(&source));assert!(!source.at(first).unwrap().same_object(&source.at(second).unwrap()));
}

#[test]
fn actual_native_root_clones_and_recursive_refs_select_one_canonical_definition() {
    let native=apache_avro::Schema::parse_str(r#"{"type":"record","name":"ns.R","fields":[{"name":"next","type":["null","ns.R"]}]}"#).unwrap();let mut definitions=BTreeMap::new();super::super::collect_names(&native,&mut definitions);let apache_avro::Schema::Record(record)=&native else {panic!()};let apache_avro::Schema::Union(union)=&record.fields[0].schema else {panic!()};let reference=&union.variants()[1];let resolved=super::super::resolve(reference,&definitions).unwrap();assert!(!std::ptr::eq(&native,resolved));
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let id=builder.record(name("ns.R",&mut context),false,&mut context).unwrap();let field=Field::new(text("next",&mut context),id,None,FieldOrder::Ascending).unwrap();builder.fields_with(id,4096,&mut context,||Ok(vec![field])).unwrap();let source=builder.finish(id,&mut context).unwrap();assert!(source.canonical_native_record(&native,&definitions).unwrap().unwrap().same_object(&source));assert!(source.canonical_native_record(reference,&definitions).unwrap().unwrap().same_object(&source));assert!(source.canonical_native_record(resolved,&definitions).unwrap().unwrap().same_object(&source));assert!(source.canonical_native_record(&apache_avro::Schema::Int,&definitions).unwrap().is_none());
}

#[test]
fn foreign_and_expired_region_ids_cannot_be_reinterpreted_in_new_owners() {
    let mut context=context();let foreign=scalar_schema(Type::Int,&mut context);let id=foreign.id();drop(foreign);let mut builder=SchemasBuilder::new(&mut context).unwrap();let local=builder.primitive(Type::Int,&mut context).unwrap();assert_ne!(id,local);assert!(builder.array(id,&mut context).is_err());let source=builder.finish(local,&mut context).unwrap();assert!(source.at(id).is_err());
}

#[test]
fn incomplete_records_and_invalid_union_names_cannot_be_published() {
    let mut context=context();let mut incomplete=SchemasBuilder::new(&mut context).unwrap();let id=incomplete.record(name("R",&mut context),false,&mut context).unwrap();assert!(incomplete.finish(id,&mut context).is_err());let mut builder=SchemasBuilder::new(&mut context).unwrap();let anonymous=builder.record(Name::new(None,None,&mut context).unwrap(),false,&mut context).unwrap();assert!(builder.union_with(4096,&mut context,||Ok(vec![anonymous])).is_err());assert!(builder.record(name("int",&mut context),false,&mut context).is_err());let int=builder.primitive(Type::Int,&mut context).unwrap();let array=builder.array(int,&mut context).unwrap();let named_array=builder.record(name("array",&mut context),false,&mut context).unwrap();assert!(builder.union_with(4096,&mut context,||Ok(vec![array,named_array])).is_err());let empty=builder.union_with(0,&mut context,||Ok(Vec::new())).unwrap();assert!(matches!(builder.owner.nodes[empty.index].kind,Kind::Union(ref branches) if branches.is_empty()));
}

#[test]
fn enum_symbols_validate_before_duplicate_or_default_membership_checks() {
    let mut context=context();let mut builder=SchemasBuilder::new(&mut context).unwrap();let invalid_symbol=text("1bad",&mut context);let missing=text("missing",&mut context);assert_eq!(builder.enumeration_with(name("E",&mut context),Some(missing),4096,&mut context,||Ok(vec![invalid_symbol])).unwrap_err().message.as_deref(),Some("Invalid Avro enum symbol"));
    let valid=text("OK",&mut context);assert!(builder.enumeration_with(name("E",&mut context),None,4096,&mut context,||Ok(vec![valid.clone(),valid])).is_err());
}

#[test]
fn schema_docs_aliases_error_flags_and_enum_defaults_do_not_enter_source_equality() {
    fn annotated(context:&mut ConversionContext,error_flag:bool)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.record(name("R",context),error_flag,context).unwrap();builder.fields_with(id,0,context,||Ok(Vec::new())).unwrap();let alias=name(if error_flag {"Old"} else {"Other"},context);builder.annotations_with(id,Some(text(if error_flag {"first"} else {"second"},context)),4096,context,||Ok(vec![alias])).unwrap();builder.finish(id,context).unwrap()}
    fn enumeration(context:&mut ConversionContext,default:bool)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let symbol=text("OK",context);let id=builder.enumeration_with(name("E",context),default.then(||symbol.clone()),4096,context,||Ok(vec![symbol])).unwrap();builder.finish(id,context).unwrap()}
    let mut context=context();let left=annotated(&mut context,false);let right=annotated(&mut context,true);let first_enum=enumeration(&mut context,false);let second_enum=enumeration(&mut context,true);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).unwrap());assert_eq!(evaluation.hash_code(&left).unwrap(),evaluation.hash_code(&right).unwrap());assert!(evaluation.equal(&first_enum,&second_enum).unwrap());assert_eq!(evaluation.hash_code(&first_enum).unwrap(),evaluation.hash_code(&second_enum).unwrap());
}

#[test]
fn field_default_nan_rule_is_distinct_from_raw_json_node_class_equality() {
    fn with_default(context:&mut ConversionContext,value:JsonValue,order:FieldOrder)->SchemaHandle {let mut builder=SchemasBuilder::new(context).unwrap();let id=builder.record(name("R",context),false,context).unwrap();let scalar=builder.primitive(Type::Double,context).unwrap();let default=node(value,context);let field=Field::new(text("x",context),scalar,Some(default),order).unwrap();builder.fields_with(id,4096,context,||Ok(vec![field])).unwrap();builder.finish(id,context).unwrap()}
    let mut context=context();let left=with_default(&mut context,JsonValue::F32(f32::NAN),FieldOrder::Ascending);let right=with_default(&mut context,JsonValue::F64(f64::NAN),FieldOrder::Ascending);let string=text("NaN",&mut context);let text_default=with_default(&mut context,JsonValue::Text(string),FieldOrder::Ascending);let descending=with_default(&mut context,JsonValue::F64(f64::NAN),FieldOrder::Descending);let mut seen=RecursionState::new();let mut observed=Observed::default();let mut evaluation=Evaluation::new(&mut seen,&mut context,&mut observed).unwrap();assert!(evaluation.equal(&left,&right).unwrap());assert!(!evaluation.equal(&left,&text_default).unwrap());assert!(!evaluation.equal(&left,&descending).unwrap());assert_eq!(evaluation.hash_code(&left).unwrap(),evaluation.hash_code(&descending).unwrap());
}

#[test]
fn schema_handles_keep_text_and_numeric_defaults_charged_after_build_context_drop() {
    let budget=rtdb_columnar::MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&budget));let mut builder=SchemasBuilder::new(&mut context).unwrap();let id=builder.record(name("R",&mut context),false,&mut context).unwrap();let default=JsonNode::integer_decimal(JavaText::from_utf8("12345678901234567890123456789012345678901234567890123456789012345678901234567890"),&mut context).unwrap();let field=Field::new(text("x",&mut context),id,Some(default),FieldOrder::Ascending).unwrap();builder.fields_with(id,4096,&mut context,||Ok(vec![field])).unwrap();let owner=builder.finish(id,&mut context).unwrap();drop(context);assert!(budget.used_bytes()>0);let mut denied=ConversionContext::with_limit(Arc::clone(&budget),0);assert!(owner.retain(&mut denied).is_err());let mut accepted=ConversionContext::new(Arc::clone(&budget));owner.retain(&mut accepted).unwrap();let after=budget.used_bytes();owner.retain(&mut accepted).unwrap();assert!(budget.used_bytes()>=after);drop(owner);drop(accepted);drop(denied);assert_eq!(budget.used_bytes(),0);
}

#[test]
fn fixed_sizes_observe_default_effective_and_vm_caps_without_allocating_the_declared_payload() {
    let mut context = context();
    let mut builder = SchemasBuilder::new(&mut context).unwrap();
    let largest = builder.fixed(name("Largest", &mut context), MAX_ARRAY_VM_LIMIT, FixedBytesLimit::DEFAULT, &mut context).unwrap();
    assert!(matches!(&builder.owner.nodes[largest.index].kind, Kind::Fixed { size, .. } if *size == i32::MAX - 8));
    assert!(builder.fixed(name("Empty", &mut context), 0, FixedBytesLimit::effective(0), &mut context).is_ok());
    assert!(builder.fixed(name("Limited", &mut context), 1, FixedBytesLimit::effective(0), &mut context).unwrap_err().code.ends_with("avro-schema-system-limit"));
    assert!(builder.fixed(name("UnsignedWrapped", &mut context), 0, FixedBytesLimit::effective(-1), &mut context).unwrap_err().code.ends_with("avro-schema-system-limit"));
    assert!(builder.fixed(name("VmCap", &mut context), MAX_ARRAY_VM_LIMIT + 1, FixedBytesLimit::effective(i32::MAX), &mut context).unwrap_err().code.ends_with("avro-schema-unsupported"));
}

#[test]
fn fixed_named_validation_precedes_negative_vm_and_effective_size_failures() {
    let mut context = context();
    let mut builder = SchemasBuilder::new(&mut context).unwrap();
    assert!(builder.fixed(name("int", &mut context), -1, FixedBytesLimit::effective(-1), &mut context).unwrap_err().code.ends_with("avro-java-schema"));
    assert!(builder.fixed(name("Negative", &mut context), -1, FixedBytesLimit::effective(-1), &mut context).unwrap_err().code.ends_with("avro-schema-unchecked"));
    assert!(builder.fixed(name("VmFirst", &mut context), MAX_ARRAY_VM_LIMIT + 1, FixedBytesLimit::effective(-1), &mut context).unwrap_err().code.ends_with("avro-schema-unsupported"));
    assert!(builder.fixed(name("Configured", &mut context), MAX_ARRAY_VM_LIMIT, FixedBytesLimit::effective(MAX_ARRAY_VM_LIMIT - 1), &mut context).unwrap_err().code.ends_with("avro-schema-system-limit"));
}
