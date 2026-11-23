use rtdb_types::StructFields;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, equality_allocation_bound, evaluate, evaluate_async, function_type, scalar_call, sql_eq, strict_eq};
use rtdb_format::transit::{TransitValue as T, encode_json, encode_msgpack};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::Value as V;

fn tag(name: &str, value: T) -> T { T::Tagged { tag: name.into(), representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn stored(value: &T, json: bool) -> V { V::Transit(if json { encode_json(value).unwrap() } else { encode_msgpack(value).unwrap() }) }
fn call(name: &str, left: V, right: V) -> Expr {
    let args = vec![Expr::Literal(left), Expr::Literal(right)];
    let return_type = function_type(name, &args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(), args, return_type }
}

#[tokio::test]
async fn transit_equality_compares_registered_objects_across_wire_encodings() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let cases = [
        (tag("i8", T::Integer(7)), T::Integer(7)),
        (tag("u", text("1-1-1-1-1")), tag("u", T::Array(vec![T::Integer(0x0000000100010001), T::Integer(0x0001000000000001)]))),
        (tag("r", text("HTTP://Example.com/%aa")), tag("r", text("http://example.COM/%AA"))),
        (tag("list", T::Array(vec![T::Integer(7), T::Null])), T::Array(vec![T::Integer(7), T::Null])),
        (T::Map(vec![(text("a"), T::Integer(1)), (text("b"), T::Integer(2))]), T::Map(vec![(text("b"), T::Integer(2)), (text("a"), T::Integer(1))])),
        (tag("set", T::Array(vec![T::Integer(2), T::Integer(1), T::Integer(1)])), tag("set", T::Array(vec![T::Integer(1), T::Integer(2)]))),
    ];
    for (left, right) in cases {
        for (left_json, right_json) in [(false, false), (false, true), (true, false), (true, true)] {
            let (left, right) = (stored(&left, left_json), stored(&right, right_json));
            assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap(), Some(true));
            assert!(strict_eq(&left, &right, &equality_scope()).unwrap());
            for expression in [
                Expr::binary(BinaryOp::Eq, Expr::Literal(left.clone()), Expr::Literal(right.clone())),
                call("===", left.clone(), right.clone()),
                call("null_eq", left.clone(), right.clone()),
            ] {
                assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Bool(true));
                assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(), V::Bool(true));
            }
        }
    }
}

#[test]
fn separately_read_identity_objects_are_unequal_even_with_identical_bytes() {
    for object in [
        T::Bytes(vec![1, 2]),
        tag("xtdb/exception-info", T::Array(vec![text("failure"), T::Map(vec![])])),
        tag("link", T::Map(vec![])),
    ] {
        let value = stored(&object, false);
        assert_eq!(sql_eq(&value, &value, &equality_scope()).unwrap(), Some(false));
        assert!(!strict_eq(&value, &value, &equality_scope()).unwrap());
    }
    let value = stored(&tag("xtdb/byte-array", text("0x0102")), false);
    assert_eq!(sql_eq(&value, &value, &equality_scope()).unwrap(), Some(true));
}

#[test]
fn transit_children_use_decoded_equality_inside_native_lists_and_structs() {
    let left = stored(&tag("i8", T::Integer(7)), false);
    let right = stored(&T::Integer(7), true);
    for (left, right) in [
        (V::List(vec![left.clone()]), V::List(vec![right.clone()])),
        (V::Struct(StructFields::from([("value".into(), left)])), V::Struct(StructFields::from([("value".into(), right)]))),
    ] {
        assert_eq!(equality_allocation_bound(&left, &right).unwrap(), 0);
        assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap(), Some(true));
        assert!(strict_eq(&left, &right, &equality_scope()).unwrap());
    }
}

#[tokio::test]
async fn reached_transit_comparisons_construct_the_complete_registered_graph() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let malformed = stored(&T::Map(vec![(text("valid"), T::Integer(1)), (text("invalid"), tag("i8", T::Integer(128)))]), false);
    assert!(sql_eq(&malformed, &malformed, &equality_scope()).is_err());
    assert!(strict_eq(&malformed, &malformed, &equality_scope()).is_err());
    for expression in [
        Expr::binary(BinaryOp::Eq, Expr::Literal(malformed.clone()), Expr::Literal(malformed.clone())),
        call("===", malformed.clone(), malformed.clone()),
    ] {
        assert!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err());
        assert!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.is_err());
    }
    let expression = Expr::binary(BinaryOp::Eq, Expr::Literal(malformed), Expr::Literal(V::Null));
    assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Null);
    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(), V::Null);
}

#[test]
fn scalar_transit_comparisons_admit_decode_scratch_before_parsing() {
    let value = V::Transit(vec![0; 2048]);
    for name in ["===", "null_eq", "nullif", "distinct_from", "set"] {
        let memory = MemoryBudget::new(1 << 20);
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let failure = scalar_call(name, &[value.clone(), value.clone()], &context).unwrap_err();
        assert_eq!(failure.code, "rtdb/memory-budget-exceeded", "{name}");
        drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn transit_scalars_compare_the_native_emitted_value_and_numeric_category() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let decimal = rtdb_types::DecimalValue::new(123i128.to_be_bytes().to_vec(), 38, 2, 128).unwrap();
    for (object, native, expected) in [
        (T::Integer(7), V::I16(7), true),
        (T::Integer(7), V::F64(7.0), false),
        (T::Float(7.0), V::F32(7.0), true),
        (tag("f", text("1.230")), V::Decimal(decimal), true),
        (T::Bool(true), V::Bool(true), true),
        (T::Integer(7), V::DateDay(7), true),
        (T::Integer(1), V::DateMilli(86_400_000), true),
        (T::Integer(86_400_000), V::DateMilli(86_400_000), false),
        (T::Integer(-2), V::DateMilli(-86_400_001), true),
        (T::Integer(-1), V::DateMilli(-1), true),
        (T::Integer(7), V::Duration { value: 7, unit: rtdb_types::TimeUnit::Second }, true),
        (text("ab"), V::Utf8("ab".into()), false),
        (tag("xtdb/byte-array", text("0x6162")), V::Utf8("ab".into()), true),
        (T::Bytes(vec![0x61, 0x62]), V::Binary(vec![0x61, 0x62]), false),
        (tag("xtdb/byte-array", text("0x6162")), V::Binary(vec![0x61, 0x62]), true),
        (tag("xtdb/byte-array", text("0x6162")), V::Binary(vec![0x61, 0x63]), false),
        (tag("xtdb/byte-array", text("0x6162")), V::FixedSizeBinary(vec![0x61, 0x62]), true),
        (tag("xtdb/byte-array", text("0x6162")), V::FixedSizeBinary(vec![0x61, 0x62, 0x00]), false),
        (tag("xtdb/byte-array", text("0x6162")), V::Keyword(rtdb_types::Keyword::parse("ab").unwrap()), true),
        (tag("xtdb/byte-array", text("0x00000000000000000000000000000000")), V::Uuid([0; 16]), true),
    ] {
        for json in [false, true] {
            let transit = stored(&object, json);
            for (left, right) in [(transit.clone(), native.clone()), (native.clone(), transit)] {
                assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap(), Some(expected));
                assert_eq!(strict_eq(&left, &right, &equality_scope()).unwrap(), expected);
                for expression in [
                    Expr::binary(BinaryOp::Eq, Expr::Literal(left.clone()), Expr::Literal(right.clone())),
                    call("===", left.clone(), right.clone()),
                ] {
                    assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Bool(expected));
                    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(), V::Bool(expected));
                }
            }
        }
    }
}

#[tokio::test]
async fn strict_transit_null_reads_objects_while_sql_outer_null_discards_them() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    for json in [false, true] {
        let null = stored(&T::Null, json);
        let invalid = stored(&tag("i8", T::Integer(128)), json);
        for (left, right) in [(null.clone(), V::Null), (V::Null, null)] {
            assert!(strict_eq(&left, &right, &equality_scope()).unwrap());
            assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap(), None);
            assert_eq!(scalar_call("===", &[left.clone(), right.clone()], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Bool(true));
            let expression = call("===", left, right);
            assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Bool(true));
            assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(), V::Bool(true));
        }
        for native in [V::Null, V::I64(7)] {
            for (left, right) in [(invalid.clone(), native.clone()), (native, invalid.clone())] {
                assert!(strict_eq(&left, &right, &equality_scope()).is_err());
                let expression = call("===", left, right);
                assert!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).is_err());
                assert!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.is_err());
            }
        }
    }
}

#[tokio::test]
async fn transit_intervals_compare_native_object_fields() {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    for (object, native, expected) in [
        (tag("xtdb/interval", text("P1Y")), V::IntervalYearMonth(12), true),
        (tag("xtdb/interval", text("P2DT0.001S")), V::IntervalDayTime { days: 2, milliseconds: 1 }, true),
        (tag("xtdb/interval", text("P1Y2DT0.000000003S")), V::IntervalMonthDayNano { months: 12, days: 2, nanoseconds: 3 }, true),
        (tag("xtdb/interval", text("P1Y2DT0.000000003S")), V::IntervalMonthDayMicro { months: 12, days: 2, nanoseconds: 3 }, true),
        (tag("xtdb/interval", text("P1D")), V::IntervalDayTime { days: 0, milliseconds: 86_400_000 }, false),
        (tag("time/period", text("P1Y")), V::IntervalYearMonth(12), false),
    ] {
        for json in [false, true] {
            let transit = stored(&object, json);
            for (left, right) in [(transit.clone(), native.clone()), (native.clone(), transit)] {
                assert_eq!(sql_eq(&left, &right, &equality_scope()).unwrap(), Some(expected));
                assert_eq!(strict_eq(&left, &right, &equality_scope()).unwrap(), expected);
                for expression in [
                    Expr::binary(BinaryOp::Eq, Expr::Literal(left.clone()), Expr::Literal(right.clone())),
                    call("===", left.clone(), right.clone()),
                ] {
                    assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).unwrap(), V::Bool(expected));
                    assert_eq!(evaluate_async(&expression, &Schema::default(), &[], &EvalContext { allocation: Some(&default_allocation),..Default::default() }).await.unwrap(), V::Bool(expected));
                }
            }
        }
    }
}

#[test]
fn strict_null_operand_still_admits_the_transit_decode_charge() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    let failure = scalar_call("===", &[V::Null, V::Transit(vec![0; 2048])], &context).unwrap_err();
    assert_eq!(failure.code, "rtdb/memory-budget-exceeded");
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
