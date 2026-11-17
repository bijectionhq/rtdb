use super::*;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use super::super::{ConnectSchema, SchemaKind, default_input::FieldObject};
use super::super::super::ConnectField;
use serde_json::json;

struct Output { value:Value, _leases:Vec<Arc<MemoryReservation>> }
fn raw(text:&str)->DefaultJson {DefaultJson::from_str(text).unwrap()}
fn parsed(text:&str)->Schema {Schema::parse_str_with_list_with_name_policy(text,Vec::<&str>::new(),&super::super::names::JavaNames).unwrap().0}
fn names(schema:&Schema)->BTreeMap<String,Schema> {let mut names=BTreeMap::new();super::super::collect_names(schema,&mut names);names}
fn prepared<'a>(value:&'a DefaultJson,schema:&'a Schema,names:&'a BTreeMap<String,Schema>)->FieldObject<'a> {match FieldObject::prepare(value,schema,names).unwrap() {DefaultAttempt::Value(input)=>input,DefaultAttempt::Invalid(cause)=>panic!("{cause}")}}
fn field(name:&str,schema:Arc<ConnectSchema>)->ConnectField {ConnectField{name:name.into(),schema}}
fn run(input:ScalarInput<'_>,destination:&ConnectSchema,path:ConversionPath,logical_name:Option<&str>,names:&BTreeMap<String,Schema>,scrub:bool,ceiling:usize,budget:&Arc<MemoryBudget>)->Result<DefaultAttempt<Output>> {
    let mut leases=Vec::new();
    let result=convert(input,destination.borrowed(),path,logical_name,names,scrub,ceiling,&mut |bytes| {leases.push(budget.reserve(bytes)?);Ok(())})?;
    Ok(match result {DefaultAttempt::Value(value)=>DefaultAttempt::Value(Output{value,_leases:leases}),DefaultAttempt::Invalid(cause)=>DefaultAttempt::Invalid(cause)})
}
fn value(result:Result<DefaultAttempt<Output>>)->Output {match result.unwrap() {DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}")}}
fn required_string()->Arc<ConnectSchema> {Arc::new(ConnectSchema::new(SchemaKind::String))}

#[test]
fn field_object_scrubs_each_lookup_while_jsonnode_keeps_raw_record_keys() {
    let schema=parsed(r#"{"type":"record","name":"R","fields":[{"name":"é","type":"string"},{"name":"_C3_A9","type":"string"}]}"#);let names=names(&schema);let source=raw(r#"{"é":"first","_C3_A9":"selected"}"#);
    let destination=ConnectSchema::new(SchemaKind::Struct(vec![field("é",required_string()),field("_C3_A9",required_string())]));
    let budget=MemoryBudget::new(usize::MAX);
    let field=value(run(ScalarInput::FieldObject(prepared(&source,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,true,usize::MAX,&budget));
    assert_eq!(field.value,Value::Struct(BTreeMap::from([("é".into(),Value::Utf8("selected".into())),("_C3_A9".into(),Value::Utf8("selected".into()))])));
    let node=value(run(ScalarInput::JsonNode(JsonNode::new(Some(&source),&schema)),&destination,ConversionPath::Wrapper,None,&names,true,usize::MAX,&budget));
    assert_eq!(node.value,Value::Struct(BTreeMap::from([("é".into(),Value::Utf8("first".into())),("_C3_A9".into(),Value::Utf8("selected".into()))])));
    drop(field);drop(node);assert_eq!(budget.used_bytes(),0);
}
#[test]
fn absent_field_object_keys_reconvert_a_native_default_but_present_null_does_not_fall_back() {
    let schema=parsed(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}"#);let names=names(&schema);
    let mut child=ConnectSchema::new(SchemaKind::String);child.default=Some(Value::Utf8("fallback".into()));
    let destination=ConnectSchema::new(SchemaKind::Struct(vec![field("a",Arc::new(child))]));let budget=MemoryBudget::new(usize::MAX);
    let absent=raw("{}");let present=raw(r#"{"a":null}"#);
    let output=value(run(ScalarInput::FieldObject(prepared(&absent,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget));
    assert_eq!(output.value,Value::Struct(BTreeMap::from([("a".into(),Value::Utf8("fallback".into()))])));
    assert!(matches!(run(ScalarInput::FieldObject(prepared(&present,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap(),DefaultAttempt::Invalid(_)));
    assert!(matches!(run(ScalarInput::JsonNode(JsonNode::new(Some(&absent),&schema)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn native_byte_and_logical_date_fallback_classes_are_not_cloned_as_avro_integers() {
    let schema=parsed(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#);let names=names(&schema);let source=raw("{}");let budget=MemoryBudget::new(usize::MAX);
    for mut child in [ConnectSchema::new(SchemaKind::I8),ConnectSchema::new(SchemaKind::I32)] {
        if matches!(child.kind,SchemaKind::I8) {child.default=Some(Value::I8(1));} else {child.name=Some("org.apache.kafka.connect.data.Date".into());child.default=Some(super::super::super::data::date_value(0));}
        let destination=ConnectSchema::new(SchemaKind::Struct(vec![field("a",Arc::new(child))]));
        assert!(matches!(run(ScalarInput::FieldObject(prepared(&source,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap(),DefaultAttempt::Invalid(_)));
    }
}
fn family(depth:usize)->(Schema,BTreeMap<String,Schema>,DefaultJson,ConnectSchema) {
    let mut schema=json!("bytes");let mut source=json!("x".repeat(1024));let mut destination=Arc::new(ConnectSchema::new(SchemaKind::Bytes));
    for index in 0..depth {
        let name=format!("R{index}");
        let alias=if index==0 {json!("bytes")} else {json!(format!("R{}",index-1))};
        schema=json!({"type":"record","name":name,"fields":[{"name":"é","type":schema},{"name":"_C3_A9","type":alias}]});
        source=json!({"_C3_A9":source});
        destination=Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![field("é",Arc::clone(&destination)),field("_C3_A9",destination)])));
    }
    let schema=parsed(&schema.to_string());let definitions=names(&schema);(schema,definitions,raw(&source.to_string()),Arc::try_unwrap(destination).unwrap())
}
fn leaves(value:&Value)->usize {match value {Value::Binary(_)=>1,Value::Struct(fields)=>fields.values().map(leaves).sum(),_=>panic!("unexpected default collision value")}}
fn measured(input:ScalarInput<'_>,destination:&ConnectSchema,names:&BTreeMap<String,Schema>,scrub:bool)->usize {
    let mut scratch=0usize;let mut admit=|bytes| {scratch=add(scratch,bytes)?;Ok(())};
    let mut walk=Walk::<_,false>{admit:&mut admit,names,demand:0,remaining:usize::MAX,scrub};
    assert!(matches!(walk.value(input,destination.borrowed(),ConversionPath::Wrapper,None,0).unwrap(),DefaultAttempt::Value(Converted{value:None,validation:DefaultAttempt::Value(())})));
    let demand=walk.demand;drop(walk);drop(admit);add(scratch,demand).unwrap()
}
#[test]
fn default_collision_multiplicity_is_measured_before_materialization() {
    for depth in [0,1,4,8] {
        let (schema,names,source,destination)=family(depth);let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));let bytes=measured(input,&destination,&names,true);
        assert!(bytes>=1024*(1usize<<depth));
        for (limit,success) in [(bytes,true),(bytes-1,false)] {
            let budget=MemoryBudget::new(limit);
            let result=run(input,&destination,ConversionPath::Wrapper,None,&names,true,limit,&budget);
            assert_eq!(result.is_ok(),success);
            if let Ok(DefaultAttempt::Value(output))=result {assert_eq!(leaves(&output.value),1<<depth);assert_eq!(budget.used_bytes(),bytes);drop(output);}else{assert!(!success);}
            assert_eq!(budget.used_bytes(),0);
        }
    }
}
#[test]
fn default_resource_failures_propagate_outside_invalid_suppression() {
    let (schema,names,source,destination)=family(4);let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));let mut calls=0;
    let result=convert(input,destination.borrowed(),ConversionPath::Wrapper,None,&names,true,usize::MAX,&mut |_| {calls+=1;Err(error("admission-test","reject output"))});
    assert!(result.unwrap_err().code.ends_with("admission-test"));assert_eq!(calls,1);
    let result=convert(input,destination.borrowed(),ConversionPath::Wrapper,None,&names,true,1,&mut |_| {panic!("limit failure must precede reservation")});
    assert!(result.unwrap_err().code.ends_with("batch-too-large"));
}
#[test]
fn default_remaining_ceiling_keeps_prior_input_and_cache_charges_live() {
    let (schema,names,source,destination)=family(4);let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));let bytes=measured(input,&destination,&names,true);
    let budget=MemoryBudget::new(bytes+511);let prior=budget.reserve(512).unwrap();
    assert!(run(input,&destination,ConversionPath::Wrapper,None,&names,true,bytes-1,&budget).is_err());assert_eq!(budget.used_bytes(),512);
    drop(prior);assert_eq!(budget.used_bytes(),0);
}
#[test]
fn ordinary_arrays_and_string_maps_use_origin_specific_child_classes_and_java_order() {
    let array=parsed(r#"{"type":"array","items":"long"}"#);let array_names=names(&array);let source=raw("[1,2]");let destination=ConnectSchema::new(SchemaKind::Array(Arc::new(ConnectSchema::new(SchemaKind::I64))));let budget=MemoryBudget::new(usize::MAX);
    let output=value(run(ScalarInput::FieldObject(prepared(&source,&array,&array_names)),&destination,ConversionPath::Wrapper,None,&array_names,false,usize::MAX,&budget));assert_eq!(output.value,Value::List(vec![Value::I64(1),Value::I64(2)]));drop(output);
    let map=parsed(r#"{"type":"map","values":"long"}"#);let map_names=names(&map);let source=raw(r#"{"z":1,"a":2}"#);let destination=ConnectSchema::new(SchemaKind::Map(required_string(),Arc::new(ConnectSchema::new(SchemaKind::I64))));
    for input in [ScalarInput::FieldObject(prepared(&source,&map,&map_names)),ScalarInput::JsonNode(JsonNode::new(Some(&source),&map))] {
        let output=value(run(input,&destination,ConversionPath::Wrapper,None,&map_names,false,usize::MAX,&budget));assert_eq!(output.value,Value::Map{entries:vec![(Value::Utf8("a".into()),Value::I64(2)),(Value::Utf8("z".into()),Value::I64(1))],sorted:false});drop(output);
    }
    assert_eq!(budget.used_bytes(),0);
}
#[test]
fn map_struct_child_return_bypasses_parent_logical_conversion_but_wrapper_does_not() {
    let schema=parsed(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#);let names=names(&schema);let source=raw(r#"{"a":1}"#);let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));
    let mut destination=ConnectSchema::new(SchemaKind::Struct(vec![field("a",Arc::new(ConnectSchema::new(SchemaKind::I32)))]));destination.name=Some("org.apache.kafka.connect.data.Date".into());let budget=MemoryBudget::new(usize::MAX);
    let mut admit=|_| Ok(());let mut walk=Walk::<_,false>{admit:&mut admit,names:&names,demand:0,remaining:usize::MAX,scrub:false};
    let converted=match walk.value(input,destination.borrowed(),ConversionPath::MapChild,destination.name.as_deref(),0).unwrap() {DefaultAttempt::Value(value)=>value,DefaultAttempt::Invalid(cause)=>panic!("{cause}")};
    assert!(converted.value.is_none());assert!(matches!(converted.validation,DefaultAttempt::Invalid(_)));
    assert!(matches!(walk.value(input,destination.borrowed(),ConversionPath::Wrapper,destination.name.as_deref(),0).unwrap(),DefaultAttempt::Invalid(_)));
    assert_eq!(budget.used_bytes(),0);
}
#[test]
fn builder_time_logical_name_is_separate_from_the_already_published_child_name() {
    let schema=parsed(r#""int""#);let names=BTreeMap::new();let source=raw("86400001");let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));let mut destination=ConnectSchema::new(SchemaKind::I32);destination.name=Some("org.apache.kafka.connect.data.Time".into());let budget=MemoryBudget::new(usize::MAX);
    let output=value(run(input,&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget));assert_eq!(output.value,Value::I32(86400001));drop(output);
    assert!(matches!(run(input,&destination,ConversionPath::Wrapper,destination.name.as_deref(),&names,false,usize::MAX,&budget).unwrap(),DefaultAttempt::Invalid(_)));
}
#[test]
fn missing_jsonnode_does_not_demand_its_source_reference_or_union() {
    let schema=parsed("[]");let names=BTreeMap::new();let mut destination=ConnectSchema::new(SchemaKind::Array(required_string()));destination.optional=true;let budget=MemoryBudget::new(usize::MAX);
    let output=value(run(ScalarInput::JsonNode(JsonNode::new(None,&schema)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget));assert_eq!(output.value,Value::Null);
}

#[test]
fn array_conversion_finishes_later_children_before_deferred_destination_validation() {
    let schema=parsed(r#"{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"time","type":"int"}]}}"#);let names=names(&schema);let source=raw(r#"[{"time":0},{"time":-1}]"#);
    let mut time=ConnectSchema::new(SchemaKind::I32);time.name=Some("org.apache.kafka.connect.data.Time".into());
    let mut record=ConnectSchema::new(SchemaKind::Struct(vec![field("time",Arc::new(time))]));record.name=Some("org.apache.kafka.connect.data.Date".into());
    let destination=ConnectSchema::new(SchemaKind::Array(Arc::new(record)));let budget=MemoryBudget::new(usize::MAX);
    let field_result=run(ScalarInput::FieldObject(prepared(&source,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap();
    let DefaultAttempt::Invalid(cause)=field_result else {panic!("expected invalid default")};
    assert_eq!(cause.message.as_deref(),Some("Time default is outside one day"));
    let node_result=run(ScalarInput::JsonNode(JsonNode::new(Some(&source),&schema)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap();
    let DefaultAttempt::Invalid(cause)=node_result else {panic!("expected invalid default")};
    assert_eq!(cause.message.as_deref(),Some("Logical default has an incompatible container representation"));
    assert_eq!(budget.used_bytes(),0);
}
#[test]
fn actual_jsonnode_null_string_elements_keep_accessor_results_for_source_reconciliation() {
    let schema=parsed(r#"{"type":"array","items":"string"}"#);let names=BTreeMap::new();let source=raw("[null]");let destination=ConnectSchema::new(SchemaKind::Array(required_string()));let budget=MemoryBudget::new(usize::MAX);
    let output=value(run(ScalarInput::JsonNode(JsonNode::new(Some(&source),&schema)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget));
    assert_eq!(output.value,Value::List(vec![Value::Utf8("null".into())]));
}

#[test]
fn string_map_replay_has_an_admitted_exact_limit_with_both_passes_live() {
    let schema=parsed(r#"{"type":"map","values":"string"}"#);let names=names(&schema);let source=raw(r#"{"z":"last","a":"first"}"#);
    let destination=ConnectSchema::new(SchemaKind::Map(required_string(),required_string()));let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));
    let bytes=measured(input,&destination,&names,false);
    for (limit,success) in [(bytes,true),(bytes-1,false)] {
        let budget=MemoryBudget::new(limit);let result=run(input,&destination,ConversionPath::Wrapper,None,&names,false,limit,&budget);
        assert_eq!(result.is_ok(),success);
        if let Ok(DefaultAttempt::Value(output))=result {
            assert_eq!(output.value,Value::Map{entries:vec![(Value::Utf8("a".into()),Value::Utf8("first".into())),(Value::Utf8("z".into()),Value::Utf8("last".into()))],sorted:false});
            assert_eq!(budget.used_bytes(),bytes);drop(output);
        } else {assert!(!success);}
        assert_eq!(budget.used_bytes(),0);
    }
}

#[test]
fn explicit_null_sentinels_defer_validation_but_java_null_fallthrough_fails_at_entry() {
    let schema=parsed(r#"{"type":"array","items":"int"}"#);let names=names(&schema);
    let mut time=ConnectSchema::new(SchemaKind::I32);time.name=Some("org.apache.kafka.connect.data.Time".into());
    let destination=ConnectSchema::new(SchemaKind::Array(Arc::new(time)));let budget=MemoryBudget::new(usize::MAX);
    for (text,message) in [("[null,-1]","Time default is outside one day"),("[9223372036854775808,-1]","Null input targets a required schema")] {
        let source=raw(text);let input=ScalarInput::FieldObject(prepared(&source,&schema,&names));
        let result=run(input,&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap();
        let DefaultAttempt::Invalid(cause)=result else {panic!("expected invalid default")};assert_eq!(cause.message.as_deref(),Some(message));
        assert_eq!(budget.used_bytes(),0);
    }
}

#[test]
fn retained_logical_string_keys_validate_after_all_conversions_and_before_their_values() {
    let schema=parsed(r#"{"type":"map","values":{"type":"record","name":"R","fields":[{"name":"time","type":"int"}]}}"#);let names=names(&schema);
    let mut time=ConnectSchema::new(SchemaKind::I32);time.name=Some("org.apache.kafka.connect.data.Time".into());
    let mut record=ConnectSchema::new(SchemaKind::Struct(vec![field("time",Arc::new(time))]));record.name=Some("org.apache.kafka.connect.data.Date".into());
    let record=Arc::new(record);let budget=MemoryBudget::new(usize::MAX);
    for logical in ["Date","Time","Timestamp","Decimal"] {
        let mut key=ConnectSchema::new(SchemaKind::String);key.name=Some(format!("org.apache.kafka.connect.data.{logical}"));
        let destination=ConnectSchema::new(SchemaKind::Map(Arc::new(key),Arc::clone(&record)));
        let empty=raw("{}");let output=value(run(ScalarInput::FieldObject(prepared(&empty,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget));
        assert_eq!(output.value,Value::Map{entries:Vec::new(),sorted:false});drop(output);
        for (text,message) in [(r#"{"a":{"time":0}}"#,"Default value does not match its destination class"),(r#"{"a":{"time":0},"z":{"time":-1}}"#,"Time default is outside one day")] {
            let source=raw(text);let result=run(ScalarInput::FieldObject(prepared(&source,&schema,&names)),&destination,ConversionPath::Wrapper,None,&names,false,usize::MAX,&budget).unwrap();
            let DefaultAttempt::Invalid(cause)=result else {panic!("expected invalid default")};assert_eq!(cause.message.as_deref(),Some(message));
        }
    }
    assert_eq!(budget.used_bytes(),0);
}
