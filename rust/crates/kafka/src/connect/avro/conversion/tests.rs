use super::*;
use super::super::super::ConnectField;
use rtdb_columnar::MemoryBudget;
use serde_json::json;

fn plan(text: &str) -> AvroDatumPlan { AvroDatumPlan::parse(text, &[], &BTreeMap::new()).unwrap() }
fn context() -> ConversionContext { ConversionContext::new(MemoryBudget::new(usize::MAX)) }
fn field(name: &str, schema: Arc<ConnectSchema>) -> ConnectField { ConnectField { name: name.into(), schema } }
fn output(plan: &AvroDatumPlan, value: &Avro, destination: &ConnectSchema, scrub: bool, context: &mut ConversionContext) -> Result<Value> { direct(&plan.root, value, plan, destination.borrowed(), scrub, context) }
fn demand(plan: &AvroDatumPlan, value: &Avro, destination: &ConnectSchema, scrub: bool) -> usize {
    let mut context = context();
    let mut walk = Walk::<false> { context: &mut context, demand: 0 };
    assert!(walk.value(&plan.root, value, plan, destination.borrowed(), scrub, 0).unwrap().is_none());
    walk.demand
}

#[test]
fn repeated_scrubbed_fields_borrow_one_actual_child_and_keep_raw_output_names() {
    let plan = plan(r#"{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"bytes"}]}"#);
    let child = Arc::new(ConnectSchema::new(SchemaKind::Bytes));
    let destination = ConnectSchema::new(SchemaKind::Struct(vec![field("é", Arc::clone(&child)), field("_C3_A9", child)]));
    let value = Avro::Record(vec![("_C3_A9".into(), Avro::Bytes(vec![7;8192]))]);
    let source = value.clone();
    let mut context = context();
    let Value::Struct(fields) = output(&plan, &value, &destination, true, &mut context).unwrap() else { panic!("expected Struct") };
    let Value::Binary(first) = &fields["é"] else { panic!("expected bytes") };
    let Value::Binary(second) = &fields["_C3_A9"] else { panic!("expected bytes") };
    assert_eq!(first, second);
    assert_ne!(first.as_ptr(), second.as_ptr());
    assert_eq!(value, source);
}
#[test]
fn disabled_scrubbing_and_raw_field_aliases_do_not_substitute_a_missing_lookup() {
    let plan = plan(r#"{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"int","aliases":["alias"]}]}"#);
    let value = Avro::Record(vec![("_C3_A9".into(), Avro::Int(1))]);
    for (name, scrub) in [("é",false),("alias",false),("alias",true)] {
        let destination = ConnectSchema::new(SchemaKind::Struct(vec![field(name, Arc::new(ConnectSchema::new(SchemaKind::I32)))]));
        assert!(output(&plan, &value, &destination, scrub, &mut context()).is_err());
    }
}
#[test]
fn a_collision_reads_the_selected_child_actual_type_before_any_output_reservation() {
    let plan = plan(r#"{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"string"}]}"#);
    let value = Avro::Record(vec![("_C3_A9".into(), Avro::String("wrong physical type".into()))]);
    let destination = ConnectSchema::new(SchemaKind::Struct(vec![field("é", Arc::new(ConnectSchema::new(SchemaKind::I32)))]));
    let budget = MemoryBudget::new(usize::MAX); let mut context = ConversionContext::new(Arc::clone(&budget));
    assert!(output(&plan, &value, &destination, true, &mut context).is_err());
    assert_eq!(budget.used_bytes(),0);
}
fn collision_family(depth: usize) -> (AvroDatumPlan, Avro, ConnectSchema) {
    let mut schema = json!("bytes");
    let mut value = Avro::Bytes(vec![9;1024]);
    let mut destination = Arc::new(ConnectSchema::new(SchemaKind::Bytes));
    for index in 0..depth {
        schema = json!({"type":"record","name":format!("R{index}"),"fields":[{"name":"_C3_A9","type":schema}]});
        value = Avro::Record(vec![("_C3_A9".into(), value)]);
        destination = Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![field("é",Arc::clone(&destination)),field("_C3_A9",destination)])));
    }
    (plan(&schema.to_string()),value,Arc::try_unwrap(destination).unwrap())
}
fn leaves(value: &Value) -> usize { match value { Value::Struct(fields) => fields.values().map(leaves).sum(), Value::Binary(_) => 1, _ => panic!("unexpected collision output") } }
#[test]
fn nested_lookup_multiplicity_has_exact_and_one_byte_below_admission_limits() {
    for depth in [0,1,4,8] {
        let (plan,value,destination) = collision_family(depth);
        let bytes = demand(&plan,&value,&destination,true);
        assert!(bytes >= (1usize << depth) * 1024);
        for (limit,success) in [(bytes,true),(bytes-1,false)] {
            let budget = MemoryBudget::new(usize::MAX);
            let mut context = ConversionContext::with_limit(Arc::clone(&budget),limit);
            let result = output(&plan,&value,&destination,true,&mut context);
            assert_eq!(result.is_ok(),success);
            if let Ok(value) = result { assert_eq!(leaves(&value),1 << depth); assert_eq!(budget.used_bytes(),bytes); }
            else { assert_eq!(budget.used_bytes(),0); }
            drop(context); assert_eq!(budget.used_bytes(),0);
        }
    }
}
#[test]
fn remaining_limit_includes_still_live_prior_input_and_cache_owners() {
    let (plan,value,destination) = collision_family(4);
    let bytes = demand(&plan,&value,&destination,true);
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::with_limit(Arc::clone(&budget),bytes + 511);
    context.reserve(512).unwrap();
    assert!(output(&plan,&value,&destination,true,&mut context).is_err());
    assert_eq!(budget.used_bytes(),512);
    drop(context); assert_eq!(budget.used_bytes(),0);
}
#[test]
fn nullable_and_singleton_actual_unions_do_not_create_an_intermediate_avro_tree() {
    let plan = plan(r#"["null","int"]"#);
    let mut destination = ConnectSchema::new(SchemaKind::I8); destination.optional=true;
    assert_eq!(output(&plan,&Avro::Union(1,Box::new(Avro::Int(257))),&destination,true,&mut context()).unwrap(),Value::I8(1));
    assert_eq!(output(&plan,&Avro::Union(0,Box::new(Avro::Null)),&destination,true,&mut context()).unwrap(),Value::Null);
}
#[test]
fn map_replay_demands_only_the_last_physical_duplicate_value() {
    let plan = plan(r#"{"type":"map","values":"int"}"#);
    let destination = ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::String)),Arc::new(ConnectSchema::new(SchemaKind::I32))));
    let value = Avro::Map(AvroMap::new(2,vec![(MapKey::Utf8(b"a".to_vec()),Avro::String("ignored".into())),(MapKey::Utf8(b"a".to_vec()),Avro::Int(3))],false));
    assert_eq!(output(&plan,&value,&destination,true,&mut context()).unwrap(),Value::Map { entries:vec![(Value::Utf8("a".into()),Value::I32(3))],sorted:false });
}
#[test]
fn malformed_distinct_keys_collide_only_after_each_visible_value_is_converted() {
    let plan = plan(r#"{"type":"map","values":"int"}"#);
    let destination = ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::String)),Arc::new(ConnectSchema::new(SchemaKind::I32))));
    let entries = vec![(MapKey::Utf8(vec![0x80]),Avro::Int(1)),(MapKey::Utf8(vec![0x81]),Avro::Int(2))];
    assert_eq!(output(&plan,&Avro::Map(AvroMap::new(2,entries.clone(),false)),&destination,true,&mut context()).unwrap(),Value::Map { entries:vec![(Value::Utf8("�".into()),Value::I32(1))],sorted:false });
    let mut entries = entries; entries[0].1 = Avro::String("visible invalid value".into());
    assert!(output(&plan,&Avro::Map(AvroMap::new(2,entries,false)),&destination,true,&mut context()).is_err());
}
#[test]
fn map_replay_is_admitted_before_scratch_and_leases_release_with_context() {
    let plan = plan(r#"{"type":"map","values":"int"}"#);
    let destination = ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::String)),Arc::new(ConnectSchema::new(SchemaKind::I32))));
    let value = Avro::Map(AvroMap::new(1,vec![(MapKey::Utf8(vec![0x80;8192]),Avro::Int(1))],false));
    let budget = MemoryBudget::new(usize::MAX); let mut context = ConversionContext::with_limit(Arc::clone(&budget),65536);
    assert!(output(&plan,&value,&destination,true,&mut context).is_err()); assert_eq!(budget.used_bytes(),0);
    drop(context);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let value = output(&plan,&value,&destination,true,&mut context).unwrap();
    assert!(budget.used_bytes() >= value.retained_bytes().unwrap());
    drop(value); assert!(budget.used_bytes()>0); drop(context); assert_eq!(budget.used_bytes(),0);
}
#[test]
fn optional_string_map_entries_lookup_raw_key_and_value_while_nested_records_scrub() {
    let plan = plan(r#"{"type":"array","items":{"type":"record","name":"Entry","fields":[{"name":"key","type":"string"},{"name":"value","type":{"type":"record","name":"R","fields":[{"name":"_C3_A9","type":"int"}]}}]}}"#);
    let record = ConnectSchema::new(SchemaKind::Struct(vec![field("é",Arc::new(ConnectSchema::new(SchemaKind::I32)))]));
    let mut key = ConnectSchema::new(SchemaKind::String); key.optional=true;
    let destination = ConnectSchema::new(SchemaKind::Map(Arc::new(key),Arc::new(record)));
    let value = Avro::Array(vec![Avro::Record(vec![("key".into(),Avro::String("é".into())),("value".into(),Avro::Record(vec![("_C3_A9".into(),Avro::Int(3))]))])]);
    assert_eq!(output(&plan,&value,&destination,true,&mut context()).unwrap(),Value::Map { entries:vec![(Value::Utf8("é".into()),Value::Struct(BTreeMap::from([("é".into(),Value::I32(3))])))],sorted:false });
}
#[test]
fn logical_dates_and_narrow_ints_are_materialized_without_a_second_coercion_tree() {
    let plan = plan(r#""int""#);
    let mut date = ConnectSchema::new(SchemaKind::I32); date.name=Some("org.apache.kafka.connect.data.Date".into());
    assert_eq!(output(&plan,&Avro::Int(2),&date,true,&mut context()).unwrap(),super::super::super::data::date_value(172800000));
    date.name=Some("org.apache.kafka.connect.data.Time".into());
    assert!(output(&plan,&Avro::Int(-1),&date,true,&mut context()).is_err());
    for (kind,expected) in [(SchemaKind::I8,Value::I8(-1)),(SchemaKind::I16,Value::I16(255))] {
        assert_eq!(output(&plan,&Avro::Int(255),&ConnectSchema::new(kind),true,&mut context()).unwrap(),expected);
    }
}
#[test]
fn utf8_demand_and_materialization_share_replacement_boundaries() {
    for (bytes,expected) in [(&b"a\0b"[..],"a\0b"),(&[0x80,0xff][..],"��"),(&[0xed,0xa0,0x80][..],"�"),(&[0xed,0xa0,b'x'][..],"�x"),(&[0xe2,0x82][..],"�")] {
        assert_eq!(utf8_len(bytes).unwrap(),expected.len()); assert_eq!(utf8(bytes).unwrap(),expected); assert_eq!(utf8(bytes).unwrap(),datum::java_utf8(bytes));
    }
}
#[test]
fn decimal_precision_uses_bounded_borrowed_magnitude_before_output_allocation() {
    let fixtures: &[(&[u8],i32,bool)] = &[
        (&[0], 32, false),
        (&[1], 32, false),
        (&[255], 32, true),
        (&[4, 238, 45, 109, 65, 91, 133, 172, 239, 128, 255, 255, 255, 255], 32, false),
        (&[251, 17, 210, 146, 190, 164, 122, 83, 16, 127, 0, 0, 0, 1], 32, true),
        (&[4, 238, 45, 109, 65, 91, 133, 172, 239, 129, 0, 0, 0, 0], 64, false),
        (&[251, 17, 210, 146, 190, 164, 122, 83, 16, 127, 0, 0, 0, 0], 64, true),
        (&[24, 79, 3, 233, 63, 249, 244, 218, 167, 151, 237, 110, 56, 237, 100, 191, 106, 31, 0, 255, 255, 255, 255, 255, 255, 255, 255], 64, false),
        (&[231, 176, 252, 22, 192, 6, 11, 37, 88, 104, 18, 145, 199, 18, 155, 64, 149, 224, 255, 0, 0, 0, 0, 0, 0, 0, 1], 64, true),
    ];
    for (bytes,precision,negative) in fixtures {
        let width=decimal_width(bytes).unwrap(); assert_eq!((width*2) as i32,*precision);
        let Value::Decimal(value)=decimal(bytes,i32::MIN,width).unwrap() else { panic!("expected Decimal") };
        assert_eq!(value.precision,*precision); assert_eq!(value.scale,i32::MIN); assert_eq!(value.unscaled[0]&128!=0,*negative);
        assert_eq!(super::super::decimal_value_at_scale(bytes.to_vec(),i32::MIN).unwrap(),Value::Decimal(value));
    }
    assert!(decimal_width(&[0x7f;8192]).is_err()); assert!(decimal_width(&[0x80;8192]).is_err()); assert!(decimal_width(&[]).is_err());
    for scale in ["-2147483648","+2147483647","٠١٢"] { assert!(decimal_scale(scale).is_ok()); }
    for scale in ["2147483648","-2147483649","","+","𝟙"] { assert!(decimal_scale(scale).is_err()); }
}
#[test]
fn direct_kernel_rejects_unselected_connect_unions_and_checked_size_overflow() {
    let plan=plan(r#""int""#); let mut destination=ConnectSchema::new(SchemaKind::Struct(vec![])); destination.name=Some("io.confluent.connect.avro.Union".into());
    assert!(output(&plan,&Avro::Int(1),&destination,true,&mut context()).is_err());
    let mut scalar=ConnectSchema::new(SchemaKind::I32); scalar.name=destination.name.clone(); scalar.parameters.insert("org.apache.kafka.connect.data.Union".into(),"marker".into());
    assert_eq!(output(&plan,&Avro::Int(1),&scalar,true,&mut context()).unwrap(),Value::I32(1));
    assert!(add(usize::MAX,1).is_err()); assert!(mul(usize::MAX,2).is_err());
}

#[test]
fn map_entries_replay_string_keys_and_reject_unimplemented_key_domains() {
    let plan=plan(r#"{"type":"array","items":{"type":"record","name":"Entry","fields":[{"name":"key","type":["null","string"]},{"name":"value","type":"int"}]}}"#);
    let entry=|key:Avro,value:Avro| Avro::Record(vec![("key".into(),key),("value".into(),value)]);
    let key=|text:&str| Avro::Union(1,Box::new(Avro::String(text.into())));
    let mut key_schema=ConnectSchema::new(SchemaKind::String); key_schema.optional=true;
    let value_schema=Arc::new(ConnectSchema::new(SchemaKind::I32));
    let destination=ConnectSchema::new(SchemaKind::Map(Arc::new(key_schema),Arc::clone(&value_schema)));
    let value=Avro::Array(vec![entry(key("z"),Avro::Int(1)),entry(key("a"),Avro::Int(2)),entry(key("z"),Avro::Int(3))]);
    assert_eq!(output(&plan,&value,&destination,true,&mut context()).unwrap(),Value::Map { entries:vec![(Value::Utf8("a".into()),Value::I32(2)),(Value::Utf8("z".into()),Value::I32(3))],sorted:false });
    let overwritten_invalid=Avro::Array(vec![entry(key("a"),Avro::String("visited".into())),entry(key("a"),Avro::Int(3))]);
    assert!(output(&plan,&overwritten_invalid,&destination,true,&mut context()).is_err());
    let required=ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::String)),Arc::clone(&value_schema)));
    assert!(output(&plan,&value,&required,true,&mut context()).is_err());
    let null=Avro::Array(vec![entry(Avro::Union(0,Box::new(Avro::Null)),Avro::Int(1))]);
    assert!(output(&plan,&null,&destination,true,&mut context()).unwrap_err().to_string().contains("non-null String keys"));
    let other=ConnectSchema::new(SchemaKind::Map(Arc::new(ConnectSchema::new(SchemaKind::I32)),value_schema));
    assert!(output(&plan,&value,&other,true,&mut context()).unwrap_err().to_string().contains("optional String keys only"));
}

#[test]
fn borrowed_children_fail_before_parent_logical_representation_and_empty_decimal_before_scale() {
    let plan=plan(r#"{"type":"record","name":"Container","fields":[{"name":"time","type":"int"}]}"#);
    let mut time=ConnectSchema::new(SchemaKind::I32); time.name=Some("org.apache.kafka.connect.data.Time".into());
    for parent in ["Date","Decimal"] {
        let mut destination=ConnectSchema::new(SchemaKind::Struct(vec![field("time",Arc::new(time.clone()))])); destination.name=Some(format!("org.apache.kafka.connect.data.{parent}"));
        for (value,expected) in [(-1,"Time is outside one day"),(0,if parent=="Date" { "Logical date value has an incompatible primitive representation" } else { "Logical Decimal value has an incompatible primitive representation" })] {
            let value=Avro::Record(vec![("time".into(),Avro::Int(value))]);
            let budget=MemoryBudget::new(usize::MAX); let mut context=ConversionContext::new(Arc::clone(&budget));
            assert_eq!(output(&plan,&value,&destination,true,&mut context).unwrap_err().message.as_deref(),Some(expected));
            assert_eq!(budget.used_bytes(),0);
        }
    }
    let bytes=AvroDatumPlan::parse(r#""bytes""#,&[],&BTreeMap::new()).unwrap();
    let mut decimal=ConnectSchema::new(SchemaKind::Bytes); decimal.name=Some("org.apache.kafka.connect.data.Decimal".into());
    assert_eq!(output(&bytes,&Avro::Bytes(vec![]),&decimal,true,&mut context()).unwrap_err().message.as_deref(),Some("Decimal coefficient bytes are empty"));
    assert_eq!(output(&bytes,&Avro::Bytes(vec![1]),&decimal,true,&mut context()).unwrap_err().message.as_deref(),Some("Decimal schema has no scale"));
    decimal.parameters.insert("scale".into(),"bad".into());
    assert_eq!(output(&bytes,&Avro::Bytes(vec![]),&decimal,true,&mut context()).unwrap_err().message.as_deref(),Some("Decimal coefficient bytes are empty"));
    assert_eq!(output(&bytes,&Avro::Bytes(vec![0x7f;8192]),&decimal,true,&mut context()).unwrap_err().message.as_deref(),Some("Decimal scale is not a signed 32-bit integer"));
}
