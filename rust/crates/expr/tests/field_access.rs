use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{BinaryOp as B,Expr,Schema,field_type};
use rtdb_types::{LogicalType as L,ScalarType as S,Value as V};

fn field(expr: Expr,name: &str) -> Expr { Expr::Field { data_type: field_type(&expr.data_type(),name),expr: Box::new(expr),name: name.into() } }
fn selected(value: V,name: &str) -> V {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap(); evaluate(&field(Expr::Literal(value),name),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap() }
fn transit(value: &T) -> V { V::Transit(encode_msgpack(value).unwrap()) }
fn unpack(value: V) -> T { let V::Transit(bytes) = value else { panic!("Transit") };decode_msgpack(&bytes).unwrap() }
fn tagged(tag: &str,representation: T) -> T { T::Tagged { tag: tag.into(),representation: Box::new(representation) } }

#[test]
fn transit_fields_use_keyword_then_string_lookup_and_suppress_false_values() {
    let value = T::Map(vec![
        (T::Keyword("a".into()),T::Integer(1)),(T::String("a".into()),T::Integer(2)),
        (T::Keyword("b".into()),T::Bool(false)),(T::String("b".into()),T::Integer(3)),
        (T::Keyword("c".into()),T::Null),(T::String("c".into()),T::Bool(false)),
        (T::String("d".into()),T::String(String::new())),
    ]);
    for encoded in [encode_msgpack(&value).unwrap(),encode_json(&value).unwrap()] {
        let value = V::Transit(encoded);
        assert_eq!(unpack(selected(value.clone(),"a")),T::Integer(1));
        assert_eq!(unpack(selected(value.clone(),"b")),T::Integer(3));
        assert_eq!(selected(value.clone(),"c"),V::Null);
        assert_eq!(selected(value.clone(),"absent"),V::Null);
        assert_eq!(unpack(selected(value,"d")),T::String(String::new()));
    }
    assert_eq!(selected(transit(&T::Array(vec![T::Integer(1)])),"a"),V::Null);
    assert_eq!(selected(V::I64(1),"a"),V::Null);
    assert_eq!(selected(V::Struct(BTreeMap::from([("a".into(),V::Bool(false))])),"a"),V::Bool(false));
    assert_eq!(field_type(&L::Scalar(S::Transit),"a"),L::Scalar(S::Transit).maybe());
}

#[test]
fn stored_error_fields_reconstruct_exception_data_and_anomaly_metadata() {
    let cause = tagged("xt/error",T::Array(vec![T::Keyword("incorrect".into())]));
    let data = T::Map(vec![(T::Keyword("detail".into()),T::Integer(7)),(T::Keyword("xtdb.error/cause".into()),cause.clone())]);
    for (tag,representation) in [
        ("xt/error",T::Array(vec![T::Keyword("conflict".into()),T::Keyword("test/code".into()),T::String("message".into()),data.clone()])),
        ("xtdb/illegal-arg",T::Array(vec![T::Keyword("test/code".into()),T::String("message".into()),data.clone(),T::Null])),
        ("xtdb/runtime-err",T::Array(vec![T::Keyword("test/code".into()),T::String("message".into()),data.clone(),T::Null])),
    ] {
        let value = transit(&T::Tagged { tag: tag.into(),representation: Box::new(representation) });
        assert_eq!(unpack(selected(value.clone(),"detail")),T::Integer(7));
        assert_eq!(unpack(selected(value.clone(),"xtdb.error/code")),T::Keyword("test/code".into()));
        assert_eq!(unpack(selected(value.clone(),"cognitect.anomalies/category")),T::Keyword(format!("cognitect.anomalies/{}",if tag == "xt/error" { "conflict" } else { "incorrect" })));
        assert_eq!(selected(value,"xtdb.error/cause"),V::Null);
    }
    let value = transit(&T::Tagged { tag: "xtdb/exception-info".into(),representation: Box::new(T::Array(vec![T::String("message".into()),data])) });
    assert_eq!(unpack(selected(value,"xtdb.error/cause")),tagged("xt/error",T::Array(vec![T::Keyword("incorrect".into()),T::Null,T::Null,T::Map(vec![])])));
}

#[test]
fn transit_handler_reconstruction_precedes_selection_and_preserves_nested_object_tags() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let legacy = tagged("xtdb/runtime-err",T::Array(vec![T::Null,T::String("message".into()),T::Map(vec![(T::Keyword("xtdb.error/code".into()),T::Keyword("data/code".into()))])]));
    let record = tagged("xtdb/tx-key",T::Map(vec![(T::Keyword("tx-id".into()),T::Integer(7))]));
    assert_eq!(unpack(selected(transit(&record),"tx-id")),T::Integer(7));
    assert_eq!(selected(transit(&record),"system-time"),V::Null);
    let value = transit(&T::Map(vec![(T::Keyword("error".into()),legacy),(T::Keyword("record".into()),record)]));
    assert_eq!(unpack(selected(value.clone(),"error")),tagged("xt/error",T::Array(vec![T::Keyword("incorrect".into()),T::Keyword("data/code".into()),T::String("message".into()),T::Map(vec![])])));
    assert_eq!(unpack(selected(value,"record")),tagged("xtdb/tx-key",T::Map(vec![(T::Keyword("tx-id".into()),T::Integer(7)),(T::Keyword("system-time".into()),T::Null)])));
    let invalid = tagged("xt/error",T::Array(vec![T::Keyword("unknown".into())]));
    for object in [T::Map(vec![(T::Keyword("chosen".into()),T::Integer(1)),(T::Keyword("other".into()),invalid.clone())]),T::Array(vec![invalid])] {
        assert!(evaluate(&field(Expr::Literal(transit(&object)),"chosen"),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err());
    }
}

#[test]
fn stored_error_optional_parts_and_effective_cause_follow_constructor_admission() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    for (tag,representation) in [
        ("xt/error",T::Array(vec![T::Keyword("incorrect".into())])),
        ("xtdb/runtime-err",T::Null),("xtdb/illegal-arg",T::Array(vec![])),
        ("xtdb/illegal-arg",T::Array(vec![T::Null,T::Null,T::Bool(false),T::Bool(false)])),
        ("xtdb/exception-info",T::Array(vec![])),("xtdb/exception-info",T::Null),
    ] { assert_eq!(selected(transit(&tagged(tag,representation)),"missing"),V::Null); }
    for bad_cause in [T::Bool(false),T::String("invalid cause".into())] {
        let data = T::Map(vec![(T::Keyword("xtdb.error/cause".into()),bad_cause)]);
        for object in [
            tagged("xt/error",T::Array(vec![T::Keyword("incorrect".into()),T::Null,T::Null,data.clone()])),
            tagged("xtdb/runtime-err",T::Array(vec![T::Null,T::Null,data.clone(),T::Bool(false)])),
        ] { assert!(evaluate(&field(Expr::Literal(transit(&object)),"missing"),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err()); }
        assert_eq!(selected(transit(&tagged("xtdb/exception-info",T::Array(vec![T::Null,data]))),"missing"),V::Null);
    }
    for object in [
        tagged("xt/error",T::Array(vec![T::Keyword("incorrect".into()),T::Null,T::Null,T::Bool(false)])),
        tagged("xtdb/exception-info",T::Array(vec![T::Null,T::Bool(false)])),
        tagged("xtdb/runtime-err",T::Array(vec![T::Null,T::Integer(1)])),
        tagged("xtdb/tx-key",T::Bool(false)),
    ] { assert!(evaluate(&field(Expr::Literal(transit(&object)),"missing"),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err()); }
    let record = tagged("xtdb/tx-key",T::Array(vec![T::Array(vec![T::Keyword("tx-id".into()),T::Integer(9)])]));
    assert_eq!(unpack(selected(transit(&record),"tx-id")),T::Integer(9));
}

#[test]
fn transit_lists_and_record_map_construction_keep_sequential_and_map_entry_rules() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    for tag in ["xtdb/runtime-err","xtdb/illegal-arg","xtdb/exception-info"] {
        let parts = if tag == "xtdb/exception-info" { vec![T::Null,T::Map(vec![])] } else { vec![T::Null,T::Null,T::Map(vec![])] };
        assert_eq!(selected(transit(&tagged(tag,tagged("list",T::Array(parts)))),"missing"),V::Null);
    }
    let entry = T::Array(vec![T::Keyword("tx-id".into()),T::Integer(3)]);
    for container in [T::Array(vec![entry.clone()]),tagged("list",T::Array(vec![entry.clone()])),tagged("set",T::Array(vec![entry]))] {
        assert_eq!(unpack(selected(transit(&tagged("xtdb/tx-key",container)),"tx-id")),T::Integer(3));
    }
    let parts = T::Array(vec![T::Null,T::String(String::new()),T::Bytes(vec![]),tagged("list",T::Array(vec![])),T::Map(vec![(T::Keyword("tx-id".into()),T::Integer(4))])]);
    assert_eq!(unpack(selected(transit(&tagged("xtdb/tx-key",parts)),"tx-id")),T::Integer(4));
    for entry in [T::Array(vec![]),tagged("list",T::Array(vec![T::Array(vec![T::Keyword("tx-id".into()),T::Integer(3)])])),T::String("a".into())] {
        let value = transit(&tagged("xtdb/tx-key",T::Array(vec![entry])));
        assert!(evaluate(&field(Expr::Literal(value),"missing"),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err());
    }
}

#[tokio::test]
async fn field_access_discards_scalar_payloads_but_forces_controls_struct_construction_and_boxes() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let divide = Expr::binary(B::Divide,Expr::Literal(V::I64(1)),Expr::Literal(V::I64(0)));
    let expression = field(divide.clone(),"a");
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(),V::Null);
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(),V::Null);
    let controlled = Expr::Case { branches: vec![(Expr::binary(B::Eq,divide.clone(),Expr::Literal(V::I64(0))),Expr::Literal(V::I64(1)))],otherwise: Box::new(Expr::Literal(V::I64(2))) };
    let boxed = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),divide.clone())],otherwise: Box::new(Expr::Literal(V::Null)) };
    for source in [controlled,boxed,Expr::Struct(vec![("present".into(),divide)])] {
        assert_eq!(evaluate_async(&field(source,"absent"),&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}

#[test]
fn generic_field_calls_keep_absent_metadata_and_transit_budget_contracts() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let context = EvalContext { allocation: Some(&default_allocation),..Default::default() };
    assert_eq!(scalar_call("get_field",&[V::Struct(BTreeMap::from([("".into(),V::I64(9))]))],&context).unwrap(),V::I64(9));
    assert_eq!(scalar_call("get-field",&[V::I64(1)],&context).unwrap(),V::Null);
    let nil_key = transit(&T::Map(vec![(T::Null,T::Integer(8))]));
    assert_eq!(unpack(scalar_call("get_field",&[nil_key],&context).unwrap()),T::Integer(8));
    assert!(scalar_call("get_field",&[transit(&T::Map(vec![]))],&context).is_err());
    for args in [vec![],vec![V::Null,V::Null]] { assert!(scalar_call("get_field",&args,&context).is_err()); }
    let value = transit(&T::Map(vec![(T::Keyword("a".into()),T::String("x".repeat(8192)))]));
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    assert!(evaluate(&field(Expr::Literal(value),"a"),&Schema::default(),&[],&context).is_err());
    drop(scope);
    assert_eq!(memory.used_bytes(),0);
}
