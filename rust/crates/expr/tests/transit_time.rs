use rtdb_expr::{EvalContext,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn selected(value: T,json: bool) -> Result<V> {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let object = T::Map(vec![(T::Keyword("chosen".into()),value)]);
    let bytes = if json { encode_json(&object)? } else { encode_msgpack(&object)? };
    let source = Expr::Literal(V::Transit(bytes));
    let expr = Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() };
    evaluate(&expr,&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() })
}

#[test]
fn selected_registered_times_are_reencoded_from_semantic_objects() {
    for json in [false,true] {
        for (input,output) in [
            (tag("time/period",text("P1Y12M1W")),tag("time/period",text("P1Y12M7D"))),
            (tag("time/date-time",text("2023-02-31T24:00")),tag("time/date-time",text("2023-03-01T00:00"))),
            (tag("time/instant",text("2024-01-02T03:04+02")),tag("time/instant",text("2024-01-02T01:04:00Z"))),
            (tag("time/offset-date-time",text("2024-01-02T03:04+02[Europe/Paris]")),tag("time/offset-date-time",text("2024-01-02T02:04+01:00"))),
            (tag("xtdb/interval",text("P12M")),tag("xtdb/interval",text("P1Y"))),
            (tag("m",text("-0x1")),tag("m",T::Integer(-1))),
            (tag("t",text("1970-01-01T00:00:00.123999+12:00")),tag("m",T::Integer(123))),
            (tag("t",text("---01")),tag("m",T::Integer(0))),
            (tag("time/duration",text("P1Dt")),tag("time/duration",text("PT24H"))),
            (tag("xtdb/tstz-range",text("")),tag("xtdb/tstz-range",T::Array(vec![T::Null,T::Null]))),
            (tag("xtdb/tstz-range",T::Bytes(vec![])),tag("xtdb/tstz-range",T::Array(vec![T::Null,T::Null]))),
            (tag("xtdb/tstz-range",T::Array(vec![tag("time/instant",text("2024-01-01Z"))])),tag("xtdb/tstz-range",T::Array(vec![tag("time/zoned-date-time",text("2024-01-01T00:00Z[UTC]")),T::Null]))),
        ] {
            let V::Transit(bytes) = selected(input,json).unwrap() else { panic!("Transit") };
            assert_eq!(decode_msgpack(&bytes).unwrap(),output);
        }
        assert_eq!(selected(tag("xtdb/interval",text("invalid")),json).unwrap(),V::Null);
        assert!(selected(tag("time/date",text("2023-02-29")),json).is_err());
        assert!(selected(tag("time/duration",text("PT1ſ")),json).is_err());
        assert!(selected(tag("xtdb/tstz-range",text("x")),json).is_err());
        assert!(selected(tag("xtdb/tstz-range",T::Bytes(vec![0])),json).is_err());
        assert!(selected(tag("xtdb/interval",text("P2147483647Y")),json).is_err());
    }
}

#[test]
fn temporal_map_keys_compare_by_registered_object_kind_and_value() {
    let same = tag("time/instant",text("2024-01-02T03:04+02"));
    let equal = tag("time/instant",text("2024-01-02T01:04Z"));
    let distinct = tag("time/offset-date-time",text("2024-01-02T01:04Z"));
    let value = T::Map(vec![(same,T::Integer(1)),(equal,T::Integer(2)),(distinct,T::Integer(3))]);
    let V::Transit(bytes) = selected(value,false).unwrap() else { panic!("Transit") };
    let T::Map(entries) = decode_msgpack(&bytes).unwrap() else { panic!("map") };
    assert_eq!(entries.len(),2);
    assert!(entries.contains(&(tag("time/instant",text("2024-01-02T01:04:00Z")),T::Integer(2))));
    assert!(entries.contains(&(tag("time/offset-date-time",text("2024-01-02T01:04Z")),T::Integer(3))));
}
