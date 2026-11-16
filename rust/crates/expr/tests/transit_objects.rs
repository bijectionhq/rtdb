use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn expression(bytes: Vec<u8>) -> Expr { let expr = Expr::Literal(V::Transit(bytes));Expr::Field { data_type: field_type(&expr.data_type(),"chosen"),expr: Box::new(expr),name: "chosen".into() } }
fn object(value: T) -> T { T::Map(vec![(T::Keyword("chosen".into()),value)]) }
fn selected(value: T) -> Result<T> {
    let value = evaluate(&expression(encode_msgpack(&object(value))?),&Schema::default(),&[],&EvalContext::default())?;
    let V::Transit(bytes) = value else { panic!("Transit") };decode_msgpack(&bytes)
}

#[test]
fn numeric_handlers_keep_checked_casts_and_write_default_scalar_forms() {
    for (value,expected) in [
        (tag("i8",T::Integer(7)),T::Integer(7)),(tag("i16",T::Float(-128.9)),T::Integer(-128)),
        (tag("i32",tag("c",text("A"))),T::Integer(65)),(tag("i64",T::Float(f64::NAN)),T::Integer(0)),
        (tag("i64",tag("f",text("9223372036854775808"))),T::Integer(i64::MAX)),
        (tag("f32",T::Integer(16_777_217)),T::Float(16_777_216.0)),
        (tag("f64",tag("ratio",T::Array(vec![T::Integer(2),T::Integer(3)]))),T::Float(0.6666666666666667)),
        (tag("n",text("+００１２")),tag("n",text("12"))),
        (tag("f",text("+001.2300")),tag("f",text("1.2300"))),
        (tag("f",text("1e2147483647")),tag("f",text("1E+2147483647"))),
        (tag("ratio",T::Array(vec![T::Integer(2),T::Integer(2)])),T::Integer(1)),
        (tag("ratio",T::Array(vec![tag("n",text("2")),T::Integer(2)])),tag("n",text("1"))),
        (tag("ratio",T::Array(vec![T::Integer(i64::MIN),T::Integer(i64::MIN)])),tag("n",text("1"))),
        (
            tag("ratio",T::Array(vec![T::Integer(2),T::Integer(-4)])),
            tag("ratio",T::Array(vec![tag("n",text("-1")),tag("n",text("2"))])),
        ),
        (
            tag("ratio",T::Array(vec![tag("f",text("1.0")),tag("f",text("2.0"))])),
            tag("f",text("0.5")),
        ),
    ] { assert_eq!(selected(value).unwrap(),expected); }
    for value in [tag("i8",T::Integer(128)),tag("i16",T::Integer(32_768)),tag("i32",T::Float(2_147_483_648.0)),tag("i64",tag("n",text("9223372036854775808"))),tag("f32",T::Float(f64::INFINITY)),tag("f64",text("1")),tag("ratio",T::Array(vec![tag("f",text("1")),T::Integer(3)]))] {
        assert!(selected(value).is_err());
    }
}

#[test]
fn registered_scalar_objects_validate_and_canonicalize_their_writer_forms() {
    for (value,expected) in [
        (tag("u",text("1-1-1-1-1")),tag("u",T::Array(vec![T::Integer(0x0000000100010001),T::Integer(0x0001000000000001)]))),
        (tag("u",T::Array(vec![T::Integer(-1),T::Integer(0),T::Null])),tag("u",T::Array(vec![T::Integer(-1),T::Integer(0)]))),
        (tag("c",text("AB")),tag("c",text("A"))),
        (tag("c",text("😀")),tag("c",text("?"))),
        (tag("r",text("HTTP://Example.com/%aa")),tag("r",text("HTTP://Example.com/%aa"))),
        (tag("xtdb/byte-array",text("xxA0fF")),tag("xtdb/byte-array",text("0xa0ff"))),
        (tag("xtdb/key-fn",tag("xtdb/key-fn",T::Keyword("snake-case-string".into()))),tag("xtdb/key-fn",T::Keyword("snake-case-string".into()))),
        (tag("xtdb/clj-form",tag("i8",T::Integer(7))),tag("xtdb/clj-form",T::Integer(7))),
        (tag("unregistered/example",tag("i8",T::Integer(7))),tag("unregistered/example",T::Integer(7))),
    ] { assert_eq!(selected(value).unwrap(),expected); }
}

#[tokio::test]
async fn registered_children_are_constructed_before_selection_and_unknown_wrapping() {
    for malformed in [
        tag("r",text("http://[")),tag("u",text("bad")),tag("i8",T::Integer(128)),tag("f",text("1e2147483649")),
        tag("xtdb/byte-array",text("0x0")),tag("xtdb/key-fn",T::Keyword("unknown".into())),tag("xtdb/clj-form",T::Null),
        tag("xtdb/period-duration",T::Array(vec![text("P1D"),text("PT1S")])),tag("ratio",T::Array(vec![T::Integer(1),T::Integer(0)])),
    ] {
        let value = T::Map(vec![(T::Keyword("chosen".into()),T::Integer(1)),(T::Keyword("ignored".into()),tag("unknown/wrapper",malformed))]);
        for encoded in [encode_msgpack(&value).unwrap(),encode_json(&value).unwrap()] {
            let expr = expression(encoded);
            assert!(evaluate(&expr,&Schema::default(),&[],&EvalContext::default()).is_err());
            assert!(evaluate_async(&expr,&Schema::default(),&[],&EvalContext::default()).await.is_err());
        }
    }
    assert!(selected(tag("u",T::Array(vec![T::Integer(0),T::Integer(0),tag("i8",T::Integer(128))]))).is_err());
}

#[test]
fn record_entry_keys_keep_decoded_identity_and_numeric_wrapper_classes_until_writing() {
    let pair = |key,value|T::Array(vec![key,T::Integer(value)]);
    for keys in [
        [T::Bytes(vec![1]),T::Bytes(vec![1])],
        [tag("xtdb/exception-info",T::Array(vec![])),tag("xtdb/exception-info",T::Array(vec![]))],
        [tag("unknown/key",tag("i8",T::Integer(1))),tag("unknown/key",T::Integer(1))],
    ] {
        let value = selected(tag("xtdb/tx-key",T::Array(vec![pair(keys[0].clone(),1),pair(keys[1].clone(),2)]))).unwrap();
        let T::Tagged { representation,.. } = value else { panic!("record") };let T::Map(entries) = *representation else { panic!("map") };
        assert_eq!(entries.len(),4);
    }
    let value = selected(tag("xtdb/tx-key",T::Array(vec![pair(tag("i8",T::Integer(1)),1),pair(T::Integer(1),2)]))).unwrap();
    let T::Tagged { representation,.. } = value else { panic!("record") };let T::Map(entries) = *representation else { panic!("map") };
    assert_eq!(entries.len(),3);
    assert!(entries.contains(&(T::Integer(1),T::Integer(2))));
}

#[test]
fn registered_object_failures_release_the_call_reservation() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    let value = object(tag("ratio",T::Array(vec![tag("n",text("123456789012345678901234567890")),T::Integer(0)])));
    assert!(evaluate(&expression(encode_msgpack(&value).unwrap()),&Schema::default(),&[],&context).is_err());
    drop(scope);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn selected_float_objects_keep_single_precision_payloads_and_map_keys() {
    for (value,expected) in [
        (tag("f32",T::Float(1.5)),vec![0x92,0xa3,b'~',b'#',b'\'',0xca,0x3f,0xc0,0,0]),
        (T::Array(vec![tag("f32",T::Float(1.5))]),vec![0x91,0xca,0x3f,0xc0,0,0]),
        (T::Map(vec![(tag("f32",T::Float(1.5)),T::Integer(2))]),vec![0x81,0xca,0x3f,0xc0,0,0,2]),
    ] {
        let result = evaluate(&expression(encode_msgpack(&object(value)).unwrap()),&Schema::default(),&[],&EvalContext::default()).unwrap();
        assert_eq!(result,V::Transit(expected));
    }
}

#[test]
fn record_entry_replacement_retains_the_original_equal_decimal_key() {
    let pairs = T::Array(vec![T::Array(vec![tag("f",text("1.0")),T::Integer(1)]),T::Array(vec![tag("f",text("1.00")),T::Integer(2)])]);
    let T::Tagged { representation,.. } = selected(tag("xtdb/tx-key",pairs)).unwrap() else { panic!("record") };
    let T::Map(entries) = *representation else { panic!("map") };
    assert!(entries.contains(&(tag("f",text("1.0")),T::Integer(2))));
    assert!(!entries.iter().any(|(key,_)|key == &tag("f",text("1.00"))));
}

#[test]
fn cached_byte_array_and_nan_keys_retain_identity_through_record_construction() {
    for key in [T::Bytes(vec![1]),T::Float(f64::NAN)] {
        let value = object(tag("xtdb/tx-key",T::Array(vec![T::Map(vec![(key.clone(),T::Integer(1))]),T::Map(vec![(key,T::Integer(2))])])));
        let result = evaluate(&expression(encode_msgpack(&value).unwrap()),&Schema::default(),&[],&EvalContext::default()).unwrap();
        let V::Transit(bytes) = result else { panic!("Transit") };
        let T::Tagged { representation,.. } = decode_msgpack(&bytes).unwrap() else { panic!("record") };
        let T::Map(entries) = *representation else { panic!("map") };
        assert_eq!(entries.len(),3);
        assert!(entries.iter().any(|(key,value)|!matches!(key,T::Keyword(_)) && *value == T::Integer(2)));
    }
    for bytes in [
        br#"["^ ","~:chosen",["~#xtdb/tx-key",[{"~bAQ==":1},{"^2":2}]]]"#.as_slice(),
        br#"["^ ","~:chosen",["~#xtdb/tx-key",[{"~zNaN":1},{"^2":2}]]]"#.as_slice(),
        br#"["^ ","~:chosen",["~#xtdb/tx-key",[{"~zNaN":1},[["~#ratio",["^2",0]],2]]]]"#.as_slice(),
    ] {
        let result = evaluate(&expression(bytes.to_vec()),&Schema::default(),&[],&EvalContext::default()).unwrap();
        let V::Transit(bytes) = result else { panic!("Transit") };
        let T::Tagged { representation,.. } = decode_msgpack(&bytes).unwrap() else { panic!("record") };
        let T::Map(entries) = *representation else { panic!("map") };
        assert_eq!(entries.len(),3);
        assert!(entries.iter().any(|(key,value)|!matches!(key,T::Keyword(_)) && *value == T::Integer(2)));
    }
}
