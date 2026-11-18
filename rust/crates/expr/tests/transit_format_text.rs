use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call};
use rtdb_format::transit::{TransitValue as T,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,function_type_with_args};
use rtdb_types::Value as V;

fn text(value: &str) -> V { V::Utf8(value.into()) }
fn tag(name: &str,representation: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(representation) } }
fn info(message: T,data: T) -> T { tag("xtdb/exception-info",T::Array(vec![message,data])) }
fn stored(value: &T) -> V { V::Transit(encode_msgpack(value).unwrap()) }

#[test]
fn format_resolves_bounded_exception_info_messages_with_tracked_and_standalone_contexts() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for (message,expected) in [
            (T::Null,"clojure.lang.ExceptionInfo: null {}"),
            (T::String(String::new()),"clojure.lang.ExceptionInfo:  {}"),
            (T::String("quote ' \"\nλ😀".into()),"clojure.lang.ExceptionInfo: quote ' \"\nλ😀 {}"),
        ] {
            for data in [T::Null,T::Map(vec![])] {
                let args = [text("%s"),stored(&info(message.clone(),data))];
                for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
                    assert_eq!(scalar_call("format",&args,&context).unwrap(),text(expected));
                }
            }
        }
        assert!(memory.used_bytes() > 4096);
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn semantic_exception_info_arguments_keep_format_quoting_width_and_position() {
    let value = stored(&info(T::String("x".into()),T::Map(vec![])));
    let literal = stored(&info(T::String("O'Reilly".into()),T::Null));
    let identifier = stored(&info(T::String("said \"hi\"".into()),T::Map(vec![])));
    for (args,expected) in [
        (vec![text("%L"),literal],"'clojure.lang.ExceptionInfo: O''Reilly {}'"),
        (vec![text("%I"),identifier],"\"clojure.lang.ExceptionInfo: said \"\"hi\"\" {}\""),
        (vec![text("|%40s|"),value.clone()],"|        clojure.lang.ExceptionInfo: x {}|"),
        (vec![text("|%*s|"),V::I64(-36),value.clone()],"|clojure.lang.ExceptionInfo: x {}    |"),
        (vec![text("%2$s/%1$s"),value,text("tail")],"tail/clojure.lang.ExceptionInfo: x {}"),
    ] {
        assert_eq!(scalar_call("format",&args,&EvalContext::default()).unwrap(),text(expected));
    }
}

#[tokio::test]
async fn sync_and_async_expressions_reach_the_same_semantic_format_argument() {
    let args = vec![Expr::Literal(text("%s")),Expr::Literal(stored(&info(T::String("evaluated".into()),T::Null)))];
    let return_type = function_type_with_args("format",&args).unwrap();
    let expression = Expr::Call { name: "format".into(),args,return_type };
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
            assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap(),text("clojure.lang.ExceptionInfo: evaluated {}"));
            assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&context).await.unwrap(),text("clojure.lang.ExceptionInfo: evaluated {}"));
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn format_keeps_first_argument_checks_and_resolves_unused_arguments_before_directives() {
    let invalid = V::Transit(vec![0xc1]);
    let error = scalar_call("format",&[V::Null,invalid.clone()],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"xtdb.error/unknown");
    assert_eq!(error.message.as_deref(),Some("Parameter specified as non-null is null: method xtdb.expression.PgFormat.pgFormat, parameter formatStr"));
    let error = scalar_call("format",&[V::I64(1),invalid.clone()],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"xtdb.expression/type-error");
    assert_eq!(error.message.as_deref(),Some("First argument to `format` must be a string"));
    let selected = stored(&info(T::String("first".into()),T::Null));
    for source in ["literal","%0$s"] {
        let error = scalar_call("format",&[text(source),selected.clone(),invalid.clone()],&EvalContext::default()).unwrap_err();
        assert_eq!(error.code,"rtdb.format/invalid-transit");
        assert_eq!(error.message.as_deref(),Some("MessagePack tag 0xc1 is not a Transit value"));
    }
    let json = V::Transit(encode_json(&info(T::Null,T::Null)).unwrap());
    let error = scalar_call("format",&[text("%s"),json],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"rtdb.format/invalid-transit");
    assert_eq!(error.message.as_deref(),Some("Trailing bytes after Transit value"));
}

#[test]
fn unselected_registered_shapes_keep_successful_raw_format_rendering() {
    let date = tag("m",T::String("0".into()));
    let nested = info(T::String("nested".into()),T::Map(vec![]));
    for value in [date,info(T::Null,T::Map(vec![(T::String("nested".into()),nested)]))] {
        let args = [text("%s"),stored(&value)];
        let memory = MemoryBudget::new(8 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
                assert!(matches!(scalar_call("format",&args,&context).unwrap(),V::Utf8(_)));
            }
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
fn public_format_denies_constructor_charge_after_the_existing_transit_prefix() {
    let args = [text("%s"),stored(&info(T::String("message".into()),T::Map(vec![])))];
    let retained = args.iter().map(|value|value.retained_bytes().unwrap()).sum::<usize>();
    let V::Transit(bytes) = &args[1] else { panic!("Transit") };
    let generic = 16 * (256 + retained);
    let string = 16 * (1024 + retained);
    let raw = 64 * args[1].retained_bytes().unwrap() + 4096 + 16 * bytes.len() * bytes.len();
    let prefix = 4096 + generic + 256 + string + 256 + raw + 256;
    let memory = MemoryBudget::new(prefix);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert_eq!(scalar_call("format",&args,&context).unwrap_err().code,"rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(),prefix);assert_eq!(memory.peak_bytes(),prefix);
    }
    assert_eq!(memory.used_bytes(),0);
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let output = scalar_call("format",&args,&context).unwrap();
        assert_eq!(output,text("clojure.lang.ExceptionInfo: message {}"));
        assert!(memory.used_bytes() > prefix);
        let retained = memory.used_bytes();drop(output);
        assert_eq!(memory.used_bytes(),retained);
    }
    assert_eq!(memory.used_bytes(),0);
}
