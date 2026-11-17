use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn keyword(value: &str) -> T { T::Keyword(value.into()) }
fn anomaly(message: &str,data: Vec<(T,T)>) -> T { tag("xt/error",T::Array(vec![keyword("incorrect"),T::Null,text(message),T::Map(data)])) }
fn selected(value: T,json: bool,context: &EvalContext<'_>) -> Result<T> {
    let object = T::Map(vec![(keyword("chosen"),value)]);
    let bytes = if json { encode_json(&object)? } else { encode_msgpack(&object)? };
    let source = Expr::Literal(V::Transit(bytes));
    let expr = Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() };
    let V::Transit(bytes) = evaluate(&expr,&Schema::default(),&[],context)? else { panic!("Transit") };
    decode_msgpack(&bytes)
}
fn set_values(values: Vec<T>,json: bool) -> Vec<T> {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let T::Tagged { representation,.. } = selected(tag("set",T::Array(values)),json,&EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap() else { panic!("set") };
    let T::Array(values) = *representation else { panic!("members") };values
}

#[test]
fn shared_nullable_anomalies_round_trip_through_object_throwables() {
    use rtdb_format::transit::{decode_anomaly,encode_anomaly};
    use rtdb_types::{Anomaly,AnomalyCategory};
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        for json in [false,true] {
            let mut values = Vec::new();
            for message in [None,Some(String::new()),Some("detail".into())] {
                let error = Anomaly::new_nullable(AnomalyCategory::Incorrect,"test/error",message.clone());
                let wire = decode_msgpack(&encode_anomaly(&error).unwrap()).unwrap();
                let object_wire = selected(wire.clone(),json,&context).unwrap();
                assert_eq!(decode_anomaly(&encode_msgpack(&object_wire).unwrap()).unwrap(),error);
                assert_eq!(object_wire,wire);
                values.push(wire);
            }
            let members = set_values(vec![values[0].clone(),values[1].clone(),values[0].clone(),values[1].clone()],json);
            assert_eq!(members.len(),2);
            assert!(members.contains(&values[0]));assert!(members.contains(&values[1]));
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn anomaly_keys_compare_normalized_message_data_and_structural_causes() {
    for json in [false,true] {
        let direct = anomaly("message",vec![(keyword("x"),T::Integer(1))]);
        let legacy = tag("xtdb/runtime-err",T::Array(vec![T::Null,text("message"),T::Map(vec![(keyword("x"),T::Integer(1))])]));
        assert_eq!(set_values(vec![direct.clone(),legacy],json),[direct]);
        let cause = anomaly("cause",vec![]);
        let outer = anomaly("outer",vec![(keyword("xtdb.error/cause"),cause)]);
        assert_eq!(set_values(vec![outer.clone(),outer],json),[anomaly("outer",vec![])]);
        let left = anomaly("outer",vec![(keyword("xtdb.error/cause"),anomaly("left",vec![]))]);
        let right = anomaly("outer",vec![(keyword("xtdb.error/cause"),anomaly("right",vec![]))]);
        let values = set_values(vec![left,right],json);
        assert_eq!(values,[anomaly("outer",vec![]),anomaly("outer",vec![])]);
        let legacy = tag("xtdb/illegal-arg",T::Array(vec![T::Null,text("outer"),T::Map(vec![(keyword("xtdb.error/cause"),anomaly("ignored",vec![]))]),anomaly("actual",vec![])]));
        let modern = anomaly("outer",vec![(keyword("xtdb.error/cause"),anomaly("actual",vec![]))]);
        assert_eq!(set_values(vec![legacy,modern],json),[anomaly("outer",vec![])]);
    }
}

#[test]
fn exception_info_and_exception_info_causes_retain_object_identity() {
    for json in [false,true] {
        let info = tag("xtdb/exception-info",T::Array(vec![text("same"),T::Map(vec![])]));
        assert_eq!(set_values(vec![info.clone(),info.clone()],json).len(),2);
        let outer = anomaly("outer",vec![(keyword("xtdb.error/cause"),info)]);
        assert_eq!(set_values(vec![outer.clone(),outer],json).len(),2);
    }
}

#[test]
fn anomaly_data_preserves_present_null_and_false_error_codes_until_writing() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    for json in [false,true] {
        let absent = anomaly("message",vec![]);
        for value in [T::Null,T::Bool(false)] {
            let present = anomaly("message",vec![(keyword("xtdb.error/code"),value.clone())]);
            let entries = T::Map(vec![(absent.clone(),T::Integer(1)),(present,T::Integer(2))]);
            let T::Map(entries) = selected(entries,json,&EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap() else { panic!("map") };
            assert_eq!(entries.len(),2);
            assert_eq!(entries[0],(absent.clone(),T::Integer(1)));
            assert_eq!(entries[1],(tag("xt/error",T::Array(vec![keyword("incorrect"),value,text("message"),T::Map(vec![])])),T::Integer(2)));
        }
    }
}

#[test]
fn anomaly_data_uses_clojure_equivalence_and_java_map_hashing() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let left = anomaly("message",vec![(keyword("x"),tag("f",text("1.0")))]);
    let right = anomaly("message",vec![(keyword("x"),tag("f",text("1.00")))]);
    let T::Map(entries) = selected(T::Map(vec![(left.clone(),T::Integer(1)),(right.clone(),T::Integer(2))]),false,&EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap() else { panic!("map") };
    assert_eq!(entries,[(left.clone(),T::Integer(2))]);
    assert_eq!(set_values(vec![left,right],false).len(),2);
}

#[test]
fn malformed_retained_causes_release_object_allocations() {
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let invalid = anomaly("outer",vec![(keyword("xtdb.error/cause"),T::Bool(false))]);
        assert!(selected(invalid,false,&context).is_err());
    }
    assert_eq!(memory.used_bytes(),0);
}
