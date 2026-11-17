use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,sql_eq,strict_eq};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{BinaryOp,Expr,Schema,field_type};
use rtdb_types::{AnomalyCategory,Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn keyword(value: &str) -> T { T::Keyword(value.into()) }
fn text(value: &str) -> T { T::String(value.into()) }
fn ty(value: T) -> T { tag("xtdb/type",value) }
fn vector(head: &str,tail: Vec<T>) -> T { T::Array(std::iter::once(keyword(head)).chain(tail).collect()) }
fn set(values: Vec<T>) -> T { tag("set",T::Array(values)) }
fn stored(value: &T,json: bool) -> V { V::Transit(if json { encode_json(value).unwrap() } else { encode_msgpack(value).unwrap() }) }
fn field(object: T,json: bool,scope: &EvaluationScope) -> Result<T> {
    let source = Expr::Literal(stored(&object,json));
    let expr = Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() };
    let V::Transit(bytes) = evaluate(&expr,&Schema::default(),&[],&EvalContext { allocation: Some(scope),..Default::default() })? else { panic!("Transit") };decode_msgpack(&bytes)
}
fn selected(value: T,json: bool) -> Result<T> {
    let scope = EvaluationScope::new(&MemoryBudget::new(64 << 20)).unwrap();field(T::Map(vec![(keyword("chosen"),value)]),json,&scope)
}
fn table(value: T) -> T { tag("xt/table",T::Map(vec![(keyword("table-name"),value)])) }
fn named(value: &str) -> T { tag("xt/table",T::Map(vec![(keyword("schema-name"),text("public")),(keyword("table-name"),text(value))])) }
fn equal(left: T,right: T,json: bool) -> bool {
    let scope = EvaluationScope::new(&MemoryBudget::new(64 << 20)).unwrap();
    let (left,right) = (stored(&left,json),stored(&right,!json));
    let result = sql_eq(&left,&right,&scope).unwrap().unwrap();assert_eq!(strict_eq(&left,&right,&scope).unwrap(),result);result
}

#[test]
fn registered_vector_types_preserve_kinds_and_canonical_arrow_fields() {
    for json in [false,true] {
        for name in ["null","nothing","bool","i8","i16","i32","i64","f32","f64","utf8","varbinary","instant","list","struct","set","union","sparse-union","keyword","transit","tstz-range","uuid","uri","oid","regclass","regproc"] {
            assert_eq!(selected(ty(keyword(name)),json).unwrap(),ty(keyword(name)),"{name}");
        }
        for (source,expected) in [
            (keyword("iid"),vector("fixed-size-binary",vec![T::Integer(16)])),
            (vector("timestamp-tz",vec![keyword("micro"),text("UTC")]),keyword("instant")),
            (vector("timestamp-tz",vec![keyword("nano"),T::Null]),vector("timestamp-local",vec![keyword("nano")])),
            (vector("fixed-size-binary",vec![T::Integer(-3)]),vector("fixed-size-binary",vec![T::Integer(-3)])),
            (vector("decimal",vec![T::Integer(-1),T::Integer(-8),tag("c",text("\u{80}"))]),vector("decimal",vec![T::Integer(-1),T::Integer(-8),T::Integer(128)])),
            (vector("time-local",vec![keyword("second"),text("ignored")]),vector("time-local",vec![keyword("second")])),
            (vector("timestamp-tz",vec![keyword("micro"),text("not/a/zone")]),vector("timestamp-tz",vec![keyword("micro"),text("not/a/zone")])),
        ] { assert_eq!(selected(ty(source),json).unwrap(),ty(expected)); }
        assert!(!equal(ty(keyword("struct")),ty(vector("struct",vec![])),json));
        assert!(!equal(ty(keyword("list")),ty(vector("list",vec![keyword("null")])),json));
        assert!(!equal(ty(keyword("i64")),ty(vector("?",vec![keyword("i64")])),json));
        assert!(equal(ty(keyword("iid")),ty(vector("fixed-size-binary",vec![T::Integer(16)])),json));
        assert!(equal(ty(ty(keyword("i64"))),ty(keyword("i64")),json));
    }
}

#[test]
fn vector_type_nullable_tail_and_set_factories_keep_their_distinct_admission() {
    for json in [false,true] {
        for (source,expected) in [
            (set(vec![]),keyword("null")),(set(vec![keyword("null")]),keyword("null")),
            (set(vec![keyword("null"),keyword("i64")]),vector("?",vec![keyword("i64")])),
            (vector("?",vec![keyword("?"),keyword("i64")]),vector("?",vec![keyword("i64")])),
            (vector("?",vec![keyword("null"),keyword("nothing")]),keyword("null")),
            (set(vec![vector("ree",vec![vector("list",vec![vector("ree-i16",vec![keyword("i64")])])])]),vector("list",vec![keyword("i64")])),
        ] { assert_eq!(selected(ty(source),json).unwrap(),ty(expected)); }
        for source in [keyword("no-such-type"),vector("?",vec![]),vector("?",vec![vector("list",vec![keyword("i64")])]),vector("?",vec![keyword("nothing")]),set(vec![keyword("nothing")]),set(vec![vector("?",vec![keyword("i64")])]),set(vec![set(vec![keyword("i64"),keyword("utf8")])])] {
            assert!(selected(ty(source),json).is_err());
        }
        for name in ["null","nothing"] {
            for (encoding,width) in [("ree-i16","I16"),("ree","I32"),("ree-i64","I64")] {
                let error = selected(ty(vector(encoding,vec![keyword(name)])),json).unwrap_err();
                assert_eq!(error.category,AnomalyCategory::Unsupported);assert_eq!(error.code,"");
                assert_eq!(error.message,format!("{} can't be laid out as RunEnd(width={width})",if name == "null" { "Null" } else { "Nothing" }));
            }
        }
        assert_eq!(selected(ty(set(vec![keyword("nothing"),T::Bool(false)])),json).unwrap_err().code,"invalid-type");
        assert_eq!(selected(ty(vector("fixed-size-list",vec![T::Bool(false)])),json).unwrap_err().code,"xtdb.expression/invalid-transit-object");
        assert!(equal(
            ty(vector("?",vec![keyword("ree"),ty(set(vec![keyword("bool"),keyword("utf8")]))])),
            ty(vector("ree",vec![set(vec![keyword("null"),keyword("bool"),keyword("utf8")])])),json));
    }
}

#[test]
fn flat_nullable_prefixes_do_not_recurse_on_the_rust_stack() {
    for json in [false,true] {
        for has_base in [true,false] {
            let mut prefix = vec![keyword("?");65_536];if has_base { prefix.push(keyword("bool")); }
            let object = T::Map(vec![(keyword("chosen"),ty(T::Array(prefix)))]);
            let V::Transit(bytes) = stored(&object,json) else { panic!("Transit") };
            let count = bytes.len();let raw_bound = count.checked_mul(count).and_then(|n|n.checked_mul(16)).and_then(|n|n.checked_add(count.checked_mul(64)?)).and_then(|n|n.checked_add(4096)).unwrap();
            let memory = MemoryBudget::new(raw_bound.checked_add(1 << 20).unwrap());
            let scope = EvaluationScope::new(&memory).unwrap();
            let result = field(object,json,&scope);
            if has_base { assert_eq!(result.unwrap(),ty(vector("?",vec![keyword("bool")]))); }
            else { assert_eq!(result.unwrap_err().code,"xtdb.expression/invalid-transit-object"); }
            drop(scope);assert_eq!(memory.used_bytes(),0);
        }
    }
}

#[test]
fn vector_type_readers_validate_constructor_operands_and_ignored_registered_values() {
    for json in [false,true] {
        for source in [
            vector("decimal",vec![tag("c",text("1")),T::Integer(0),T::Integer(128)]),
            vector("decimal",vec![T::Integer(1),T::Integer(0)]),vector("decimal",vec![T::Integer(1),T::Integer(0),T::Null]),
            vector("fixed-size-binary",vec![T::Integer(i64::from(i32::MAX) + 1)]),
            vector("date",vec![keyword("nano")]),vector("duration",vec![T::Null]),vector("time-local",vec![text("micro")]),
            vector("timestamp-tz",vec![keyword("micro"),T::Bool(false)]),
            vector("bool",vec![tag("i8",T::Integer(128))]),
        ] { assert_eq!(selected(ty(source),json).unwrap_err().code,"xtdb.expression/invalid-transit-object"); }
        let options = tag("list",T::Array(vec![keyword("sorted?"),T::Bool(true),T::Bool(false)]));
        assert_eq!(selected(ty(vector("map",vec![options,T::Bool(false)])),json).unwrap_err().code,"xtdb.expression/invalid-transit-object");
        assert_eq!(selected(ty(vector("map",vec![T::Null,T::Bool(false)])),json).unwrap_err().code,"invalid-type");
        let scope = EvaluationScope::new(&MemoryBudget::new(1 << 20)).unwrap();
        assert!(field(T::Map(vec![(keyword("chosen"),T::Integer(1)),(keyword("unused"),ty(vector("bool",vec![tag("r",text("bad uri"))])))]),json,&scope).is_err());
    }
}

#[test]
fn vector_type_invalid_spec_context_preserves_canonical_objects_without_semantic_rereading() {
    for json in [false,true] {
        let spec = tag("list",T::Array(vec![
            ty(set(vec![keyword("null"),keyword("struct")])),tag("f32",T::Float(1.25)),
            T::Map(vec![(keyword("x"),tag("unknown",T::Null))]),
        ]));
        let expected = tag("list",T::Array(vec![
            ty(vector("?",vec![keyword("struct")])),T::Float32(1.25),T::Map(vec![(keyword("x"),tag("unknown",T::Null))]),
        ]));
        let scope = EvaluationScope::new(&MemoryBudget::new(64 << 20)).unwrap();
        let error = field(T::Map(vec![(keyword("chosen"),ty(spec))]),json,&scope).unwrap_err();
        assert_eq!(error.category,AnomalyCategory::Incorrect);assert_eq!(error.code,"invalid-type");assert_eq!(error.message,"Invalid type");
        assert_eq!(rtdb_format::transit::from_json_value(&error.context["type-spec"]).unwrap(),expected);
        let wire = tag("xt/error",T::Array(vec![keyword("incorrect"),keyword("invalid-type"),text("Invalid type"),T::Map(vec![(keyword("type-spec"),expected)])]));
        assert_eq!(rtdb_format::transit::encode_anomaly(&error).unwrap(),encode_msgpack(&wire).unwrap());
    }
}

#[test]
fn vector_type_structs_stringify_keys_before_reading_values_and_keep_last_values() {
    for json in [false,true] {
        let entries = vec![
            T::Array(vec![T::Null,keyword("bool")]),T::Array(vec![text(""),keyword("i8")]),
            tag("list",T::Array(vec![T::Bool(false),keyword("i16"),text("ignored")])),T::Array(vec![text("false"),keyword("i32")]),
            T::Array(vec![keyword("x"),keyword("i64")]),
        ];
        let expected = ty(vector("struct",vec![T::Map(vec![(text(""),keyword("i8")),(text("false"),keyword("i32")),(text(":x"),keyword("i64"))])]));
        for source in [T::Array(entries.clone()),tag("list",T::Array(entries))] { assert_eq!(selected(ty(vector("struct",vec![source])),json).unwrap(),expected); }
        for source in [T::Null,text(""),T::Bytes(vec![]),T::Array(vec![]),tag("list",T::Array(vec![])),set(vec![]),tag("xtdb/path",text("/"))] {
            assert_eq!(selected(ty(vector("struct",vec![source])),json).unwrap(),ty(vector("struct",vec![T::Map(vec![])])));
        }
        for source in [text("x"),T::Bytes(vec![1]),tag("xtdb/path",text("")),T::Array(vec![T::Map(vec![])]),T::Array(vec![set(vec![])]),T::Array(vec![text("ab")]),T::Array(vec![T::Bytes(vec![1,2])])] {
            assert!(selected(ty(vector("struct",vec![source])),json).is_err());
        }
        let error = selected(ty(vector("struct",vec![T::Array(vec![T::Array(vec![tag("unknown",T::Null),T::Bool(false)])])])),json).unwrap_err();
        assert_eq!(error.code,"xtdb.expression/invalid-transit-object");
    }
}

#[test]
fn vector_type_map_options_use_map_destructuring_and_clojure_truthiness() {
    for json in [false,true] {
        for (options,sorted) in [(T::Null,false),(T::Bool(true),false),(T::Map(vec![(keyword("sorted?"),T::Integer(0))]),true),(set(vec![keyword("sorted?")]),true),(tag("list",T::Array(vec![keyword("sorted?"),T::Bool(false)])),false)] {
            assert_eq!(selected(ty(vector("map",vec![options,keyword("i64")])),json).unwrap(),ty(vector("map",vec![T::Map(vec![(keyword("sorted?"),T::Bool(sorted))]),keyword("i64")])));
        }
        assert_eq!(selected(ty(vector("map",vec![keyword("i64")])),json).unwrap(),ty(vector("map",vec![T::Map(vec![(keyword("sorted?"),T::Bool(false))]),keyword("i64")])));
    }
}

#[test]
fn vector_type_wire_nullable_rendering_preserves_child_encoding_but_text_removes_it() {
    let source = vector("ree-i64",vec![vector("?",vec![keyword("list"),vector("ree-i16",vec![keyword("i64")])])]);
    for json in [false,true] {
        assert_eq!(selected(ty(source.clone()),json).unwrap(),ty(source.clone()));
        assert_eq!(selected(table(ty(source.clone())),json).unwrap(),named("[:ree I64 [:? [<List> <Int(64, true)>]]]"));
        assert_eq!(selected(table(T::Array(vec![ty(source.clone())])),json).unwrap(),named("[#xt/type [:ree-i64 [:? :list [:ree-i16 :i64]]]]"));
    }
}

#[test]
fn vector_type_scalar_and_nullable_container_writes_can_change_kind_on_reread() {
    for json in [false,true] {
        let scalar_struct = ty(set(vec![keyword("null"),keyword("struct")]));
        let canonical = ty(vector("?",vec![keyword("struct")]));
        assert_eq!(selected(scalar_struct.clone(),json).unwrap(),canonical);
        assert!(!equal(scalar_struct,canonical.clone(),json));
        assert_eq!(selected(canonical,json).unwrap(),ty(vector("?",vec![keyword("struct"),T::Map(vec![])])));
        let scalar_list = ty(set(vec![keyword("null"),keyword("list")]));
        let canonical = selected(scalar_list,json).unwrap();assert_eq!(canonical,ty(vector("?",vec![keyword("list")])));
        assert_eq!(selected(canonical,json).unwrap_err().code,"invalid-type");
    }
}

#[test]
fn vector_type_struct_and_union_equality_ignore_order_and_hash_equal_keys_together() {
    let left = ty(vector("struct",vec![T::Map(vec![(text("a"),keyword("i64")),(text("b"),keyword("bool"))])]));
    let right = ty(vector("struct",vec![T::Map(vec![(text("b"),keyword("bool")),(text("a"),keyword("i64"))])]));
    for json in [false,true] {
        assert!(equal(left.clone(),right.clone(),json));
        assert_eq!(selected(T::Map(vec![(left.clone(),T::Integer(1)),(right.clone(),T::Integer(2))]),json).unwrap(),T::Map(vec![(left.clone(),T::Integer(2))]));
        assert_eq!(selected(set(vec![left.clone(),right.clone()]),json).unwrap(),set(vec![left.clone()]));
        let mut entries = (0..8).map(|value|(T::Integer(value),T::Integer(value))).collect::<Vec<_>>();
        entries.push((left.clone(),T::Integer(1)));entries.push((right.clone(),T::Integer(2)));
        let T::Map(entries) = selected(T::Map(entries),json).unwrap() else { panic!("map") };
        assert_eq!(entries.len(),9);assert!(entries.contains(&(left.clone(),T::Integer(2))));
        assert!(equal(ty(set(vec![keyword("i64"),keyword("utf8"),keyword("null")])),ty(set(vec![keyword("null"),keyword("utf8"),keyword("i64")])),json));
        assert!(!equal(ty(set(vec![keyword("i64"),keyword("utf8")])),ty(set(vec![keyword("i64"),keyword("bool")])),json));
        let union = ty(set(vec![ty(keyword("bool")),ty(keyword("utf8"))]));
        assert_eq!(selected(table(union.clone()),json).unwrap(),named("Poly(children=[<Bool>, <Utf8>], encoding=Plain)"));
        assert_eq!(selected(table(ty(vector("ree-i16",vec![union]))),json).unwrap(),named("Poly(children=[<Bool>, <Utf8>], encoding=RunEnd(width=I16))"));
    }
}

#[test]
fn vector_type_text_uses_arrow_and_kotlin_object_forms() {
    for json in [false,true] {
        for (source,expected) in [
            (keyword("null"),"Null"),(keyword("nothing"),"Nothing"),(keyword("struct"),"<Struct>"),
            (vector("struct",vec![T::Map(vec![(text("a"),keyword("bool")),(text("b"),keyword("i32"))])]),"{a: <Bool>, b: <Int(32, true)>}"),
            (vector("decimal",vec![T::Integer(10),T::Integer(-2),T::Integer(64)]),"<Decimal(10, -2, 64)>"),
            (vector("timestamp-tz",vec![keyword("nano"),text("some-zone")]),"<Timestamp(NANOSECOND, some-zone)>"),
            (vector("time-local",vec![keyword("second")]),"<Time(SECOND, 32)>"),(keyword("union"),"<Union(Dense, null)>"),
            (keyword("keyword"),"<ExtensionType(xt/clj-keyword, Utf8)>"),(keyword("uuid"),"<ExtensionType(uuid, FixedSizeBinary(16))>"),
            (vector("interval",vec![keyword("month-day-micro")]),"<ExtensionType(xt/intervalDMMType, Interval(MONTH_DAY_NANO))>"),
            (keyword("tstz-range"),"[<ExtensionType(xt/tstz-range, FixedSizeList(2))> <Timestamp(MICROSECOND, UTC)>]"),
        ] { assert_eq!(selected(table(ty(source)),json).unwrap(),named(expected)); }
    }
}

fn escaped_type(depth: usize) -> T {
    let mut value = ty(keyword("bool"));
    for _ in 0..depth { value = ty(vector("struct",vec![T::Map(vec![(T::Array(vec![value]),keyword("i64"))])])); }value
}

#[test]
fn vector_type_derived_names_reserve_before_growth_and_release_with_the_scope() {
    for json in [false,true] {
        assert_eq!(selected(escaped_type(1),json).unwrap(),ty(vector("struct",vec![T::Map(vec![(text("[#xt/type :bool]"),keyword("i64"))])])));
        let value = escaped_type(20);let object = T::Map(vec![(keyword("chosen"),T::Integer(1)),(keyword("unused"),value.clone())]);
        let bytes = match stored(&object,json) { V::Transit(bytes) => bytes,_ => unreachable!() };
        let raw = bytes.len() * bytes.len() * 16 + bytes.len() * 64 + 4096;
        let memory = MemoryBudget::new(raw + (1 << 20));
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            assert!(field(object,json,&scope).is_err());
            assert!(memory.used_bytes() <= memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(),0);
        let memory = MemoryBudget::new(64 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            assert!(field(T::Map(vec![(keyword("chosen"),escaped_type(5))]),json,&scope).is_ok());
            assert!(memory.used_bytes() > 0);
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn vector_type_equality_demands_registered_graphs_only_after_nullable_control() {
    for json in [false,true] {
        let invalid = stored(&ty(vector("ree",vec![keyword("null")])),json);
        let scope = EvaluationScope::new(&MemoryBudget::new(1 << 20)).unwrap();
        assert_eq!(sql_eq(&invalid,&invalid,&scope).unwrap_err().category,AnomalyCategory::Unsupported);
        let dead = Expr::binary(BinaryOp::Eq,Expr::Literal(invalid),Expr::Literal(V::Null));
        assert_eq!(evaluate(&dead,&Schema::default(),&[],&EvalContext::default()).unwrap(),V::Null);
    }
}

#[test]
fn vector_type_selected_and_equality_paths_reject_unadmitted_derived_names() {
    for json in [false,true] {
        let value = escaped_type(20);
        for select in [false,true] {
            let wire = if select { T::Map(vec![(keyword("chosen"),value.clone())]) } else { value.clone() };
            let source = stored(&wire,json);
            let V::Transit(bytes) = &source else { unreachable!() };
            let raw = bytes.len() * bytes.len() * 16 + bytes.len() * 64 + 4096;
            let memory = MemoryBudget::new(raw + (1 << 20));
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                if select { assert!(field(wire,json,&scope).is_err()); }
                else { assert!(sql_eq(&source,&source,&scope).is_err()); }
                assert!(memory.used_bytes() >= raw);
            }
            assert_eq!(memory.used_bytes(),0);
        }
    }
}
