use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call,sql_eq,strict_eq};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{BinaryOp,Expr,Schema,field_type};
use rtdb_types::{DecimalValue,Value as V};

fn tagged(tag: &str,representation: T) -> T { T::Tagged { tag: tag.into(),representation: Box::new(representation) } }
fn stored(value: &T,json: bool) -> V { V::Transit(if json { encode_json(value).unwrap() } else { encode_msgpack(value).unwrap() }) }
fn field(value: V) -> Expr {
    let source = Expr::Literal(value);
    Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() }
}

#[tokio::test]
async fn reached_transit_requires_allocation_before_decoding_with_a_default_context() {
    for value in [V::Transit(vec![0xc1]),stored(&T::Null,false)] {
        let context = EvalContext::default();
        let expression = field(value.clone());
        assert_eq!(scalar_call("===",&[value,V::Null],&context).unwrap_err().code,"rtdb.expr/missing-allocation-scope");
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap_err().code,"rtdb.expr/missing-allocation-scope");
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&context).await.unwrap_err().code,"rtdb.expr/missing-allocation-scope");
    }
}

#[tokio::test]
async fn outer_null_and_unequal_prefixes_leave_invalid_transit_unread() {
    let invalid = V::Transit(vec![0xc1]);
    let memory = MemoryBudget::new(4096);
    let allocation = EvaluationScope::new(&memory).unwrap();
    assert_eq!(sql_eq(&invalid,&V::Null,&allocation).unwrap(),None);
    let left = V::list(vec![V::I64(0),invalid.clone()]);
    let right = V::list(vec![V::I64(1),invalid.clone()]);
    assert_eq!(sql_eq(&left,&right,&allocation).unwrap(),Some(false));
    assert!(!strict_eq(&left,&right,&allocation).unwrap());
    let expression = Expr::binary(BinaryOp::Eq,Expr::Literal(invalid),Expr::Literal(V::Null));
    assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext::default()).unwrap(),V::Null);
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    drop(allocation);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn left_registered_failure_precedes_right_decode_admission() {
    for json in [false,true] {
        let invalid = stored(&tagged("i8",T::Integer(128)),json);
        let large = V::Transit(vec![0;2048]);
        let memory = MemoryBudget::new(1 << 20);
        {
            let allocation = EvaluationScope::new(&memory).unwrap();
            assert_eq!(sql_eq(&invalid,&large,&allocation).unwrap_err().code,"xtdb.expression/invalid-transit-object");
        }
        assert_eq!(memory.used_bytes(),0);
        {
            let allocation = EvaluationScope::new(&memory).unwrap();
            assert_eq!(sql_eq(&large,&invalid,&allocation).unwrap_err().code,"rtdb/memory-budget-exceeded");
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn decimal_peer_conversion_is_admitted_after_the_transit_read() {
    let value = stored(&T::Integer(1),false);
    let decimal = V::Decimal(DecimalValue::new(vec![0;32],76,0,256).unwrap());
    let memory = MemoryBudget::new(1 << 20);
    let required = {
        let allocation = EvaluationScope::new(&memory).unwrap();
        assert_eq!(sql_eq(&value,&V::I64(1),&allocation).unwrap(),Some(true));
        memory.used_bytes()
    };
    assert_eq!(memory.used_bytes(),0);
    let limited = MemoryBudget::new(required);
    {
        let allocation = EvaluationScope::new(&limited).unwrap();
        assert_eq!(sql_eq(&value,&decimal,&allocation).unwrap_err().code,"rtdb/memory-budget-exceeded");
    }
    assert_eq!(limited.used_bytes(),0);
}

#[test]
fn derived_table_text_and_canonical_output_keep_the_callers_reservations() {
    let table = tagged("xt/table",T::Map(vec![(T::Keyword("table-name".into()),T::Array(vec![T::String("a\\\"\n".into())]))]));
    let canonical = tagged("xt/table",T::Map(vec![(T::Keyword("schema-name".into()),T::String("public".into())),(T::Keyword("table-name".into()),T::String(r#"["a\\\"\n"]"#.into()))]));
    for json in [false,true] {
        let object = stored(&T::Map(vec![(T::Keyword("chosen".into()),table.clone())]),json);
        let expression = field(object);
        let memory = MemoryBudget::new(1 << 20);
        let required = {
            let allocation = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&allocation),..Default::default() };
            let V::Transit(bytes) = evaluate(&expression,&Schema::default(),&[],&context).unwrap() else { panic!("Transit") };
            assert_eq!(decode_msgpack(&bytes).unwrap(),canonical);
            memory.used_bytes()
        };
        assert_eq!(memory.used_bytes(),0);
        let limited = MemoryBudget::new(required - 1);
        {
            let allocation = EvaluationScope::new(&limited).unwrap();
            let context = EvalContext { allocation: Some(&allocation),..Default::default() };
            assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap_err().code,"rtdb/memory-budget-exceeded");
        }
        assert_eq!(limited.used_bytes(),0);
    }
}

#[test]
fn equality_shape_admission_uses_the_public_callers_scope() {
    let value = V::list(vec![V::Set(vec![])]);
    let memory = MemoryBudget::new(4096);
    {
        let allocation = EvaluationScope::new(&memory).unwrap();
        assert_eq!(sql_eq(&value,&value,&allocation).unwrap_err().code,"rtdb/memory-budget-exceeded");
    }
    assert_eq!(memory.used_bytes(),0);
    let allocation = EvaluationScope::new(&MemoryBudget::new(1 << 20)).unwrap();
    assert_eq!(sql_eq(&value,&value,&allocation).unwrap_err().code,"xtdb.expression/set-equality");
}
