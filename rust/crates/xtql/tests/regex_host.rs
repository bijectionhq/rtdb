use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate_async};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{Result,Value};
use rtdb_xtql::{CompileOptions,compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
async fn project(expression: &str) -> Result<Value> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).await
}

#[tokio::test]
async fn host_patterns_use_lexical_controls_and_strict_string_functions() {
    for pattern in [
        r#"(str "a")"#,
        r#"(clojure.core/str nil \a)"#,
        r#"(let [x "b" x "a"] x)"#,
        r#"(let* [x "a" y (str x "b")] (subs y 0 1))"#,
        r#"(let [f str] (f "a"))"#,
        r#"(let [str identity] (str "a"))"#,
        r#"(let [/ str] (/ "a"))"#,
        r#"(let [if str] (if true "a" "b"))"#,
        r#"(if false (subs "a" -1) "a")"#,
        r#"(do (identity "ignored") "a")"#,
        r#"(or nil false (and true "a"))"#,
        r#"(when (not nil) "a")"#,
        r#"(when-not false "a")"#,
        r#"(name :ns/a)"#,
        r#"(namespace 'a/b)"#,
        r#"(str 'a)"#,
        r#"(if (boolean []) "a" "b")"#,
        r#"(if (and {} #{}) "a" "b")"#,
        r#"(subs "😀a" 2)"#,
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{pattern}");
    }
    for (source,pattern) in [("true",r#"(str true)"#),("12",r#"(str 12)"#),("1.0",r#"(str 1.0)"#),(":ns/a",r#"(str :ns/a)"#)] {
        assert_eq!(project(&format!(r#"(regexp-replace "{source}" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    assert_eq!(project(r#"(regexp-replace "😀a" (subs "😀a" 0 2) "x")"#).await.unwrap(),Value::Utf8("xa".into()));
}

#[tokio::test]
async fn host_compilation_visits_dead_branches_before_any_pattern_binding_executes() {
    for expression in [
        r#"(regexp-replace "a" (if true "a" (if)) "x")"#,
        r#"(regexp-replace (regexp-replace "a" (if) "x") "[" "x")"#,
        r#"(regexp-replace "a" (if true "a" (boolean)) "x")"#,
    ] { assert_eq!(project(expression).await.unwrap_err().code,"xtdb.error/unknown","{expression}"); }
    assert_eq!(project(r#"(regexp-replace "a" (if true "a" (identity)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
    assert_eq!(project(r#"(regexp-replace "a" (let [f boolean] (if true "a" (f))) "x")"#).await.unwrap(),Value::Utf8("x".into()));
}

#[tokio::test]
async fn host_strict_arguments_and_selected_controls_retain_failures() {
    for pattern in [r#"(str "a" (subs "x" -1))"#,r#"(do (subs "x" -1) "a")"#,r#"(let [unused (subs "x" -1)] "a")"#,r#"(if true nil "a")"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code,"xtdb.error/unknown","{pattern}");
    }
    assert_eq!(project(r#"(regexp-replace nil (if) "x")"#).await.unwrap(),Value::Null);
    assert_eq!(project(r#"(coalesce "kept" (regexp-replace "a" (if) "x"))"#).await.unwrap(),Value::Utf8("kept".into()));
}

#[tokio::test]
async fn host_numeric_objects_keep_exact_text_and_boxed_substring_indexes() {
    for (source,pattern) in [
        ("9223372036854775808",r#"(str 9223372036854775808N)"#),
        ("12345678901234567890123456789012345678901234567890123456789012345678901234567890",r#"(str 12345678901234567890123456789012345678901234567890123456789012345678901234567890M)"#),
        ("1.00",r#"(str "^" 1.00M "$")"#),
        ("3.0",r#"(str (double 3N))"#),
        ("1.25",r#"(str (float 1.25))"#),
        ("0",r#"(str (int ##NaN))"#),
        ("127",r#"(let [f byte] (str (f 127.9)))"#),
        ("32767",r#"(str (short (identity 32767.9)))"#),
        ("2147483647",r#"(let [f int] (str (f 2147483647.9)))"#),
        ("2147483647",r#"(str (int (float 2147483648.0)))"#),
        ("0",r#"(let [f byte] (str (f ##NaN)))"#),
        ("4611686568183201792",r#"(str (long (float 4611686293305294849)))"#),
        ("4611686018427387904",r#"(let [f float] (str (long (f 4611686293305294849))))"#),
        ("a",r#"(subs "😀a" 2N 3.9M)"#),
        ("a",r#"(subs "a" ##NaN 1.9)"#),
        ("a",r#"(subs "a" (int \u0000) (int \u0001))"#),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{source}" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{pattern}");
    }
}

#[tokio::test]
async fn host_inline_casts_preserve_primitive_selection_and_compile_time_arity() {
    for pattern in [
        r#"(str (byte 127.9))"#,r#"(let [n 127.9] (str (byte n)))"#,r#"(str (short 32767.9))"#,
        r#"(str (int (if true 2147483647.9 0.0)))"#,r#"(str (byte ##NaN))"#,
        r#"(let [f int] (str (f (float 2147483648.0))))"#,r#"(str (long 9223372036854775808N))"#,
        r#"(if true "a" (int))"#,r#"(if true "a" (long 1 2))"#,
        r#"(subs "a" \u0000 \u0001)"#,
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code,"xtdb.error/unknown","{pattern}");
    }
    for pattern in [r#"(let [f int] (if true "a" (f)))"#,r#"(let [int identity] (if true "a" (int)))"#,r#"(if true "a" (byte 127.9))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
}

#[tokio::test]
async fn host_arithmetic_keeps_numeric_categories_scale_and_primitive_overloads() {
    for (source,pattern) in [
        ("0",r#"(str (+))"#),("1",r#"(str (*))"#),("",r#"(str (+ nil))"#),
        ("6",r#"(str (+ 1 2 3))"#),("24",r#"(str (* 2 3 4))"#),
        ("5",r#"(str (- 10 3 2))"#),("-3",r#"(str (- 3))"#),
        ("1/2",r#"(str (/ 2))"#),("2",r#"(str (/ 12 3 2))"#),
        ("1/4",r#"(str (* (/ 1 2) (/ 1 2)))"#),("1",r#"(str (+ (/ 1 2) (/ 1 2)))"#),
        ("3.50",r#"(str (+ 1.20M 2.3M))"#),("2.760",r#"(str (* 1.20M 2.3M))"#),
        ("1.70",r#"(str (+ 1.20M (/ 1 2)))"#),("1.00",r#"(str (/ 2.00M 2))"#),
        ("9223372036854775808",r#"(str (+' 9223372036854775807 1))"#),
        ("9223372036854775808",r#"(str (-' -9223372036854775808))"#),
        ("9223372036854775808",r#"(str (*' -9223372036854775808 -1))"#),
        ("9223372036854775808",r#"(str (inc' 9223372036854775807))"#),
        ("-9223372036854775809",r#"(str (dec' -9223372036854775808))"#),
        ("0",r#"(str (- -9223372036854775808 -9223372036854775808))"#),
        ("0",r#"(str (- (+ -9223372036854775808 0) -9223372036854775808))"#),
        ("Infinity",r#"(str (/ 1.0 0.0))"#),("Infinity",r#"(str (/ 1.0 (identity 0)))"#),
        ("Infinity",r#"(str (/ (identity 1) 0.0))"#),("Infinity",r#"(str (/ (float 1) 0))"#),
        ("NaN",r#"(let [f /] (str (f ##NaN "not numeric")))"#),
        ("NaN",r#"(let [f /] (str (f "not numeric" ##NaN)))"#),
        ("3",r#"(let [f +] (str (f 1 2)))"#),("3",r#"(let [+ str] (+ "3"))"#),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{source}" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{pattern}");
    }
}

#[tokio::test]
async fn host_direct_arithmetic_runs_each_nested_operation_before_later_arguments() {
    let direct = project(r#"(regexp-replace "a" (str (+ 9223372036854775807 1 (subs "x" -1))) "x")"#).await.unwrap_err();
    assert_eq!(direct.message,"long overflow");
    let indirect = project(r#"(regexp-replace "a" (let [f +] (str (f 9223372036854775807 1 (subs "x" -1)))) "x")"#).await.unwrap_err();
    assert_eq!(indirect.message,"Host substring indexes are out of range");
    let boxed = project(r#"(regexp-replace "a" (let [f -] (str (f -9223372036854775808 -9223372036854775808))) "x")"#).await.unwrap_err();
    assert_eq!(boxed.message,"long overflow");
    let promoted = project(r#"(regexp-replace "a" (str (- (+' -9223372036854775808 0) -9223372036854775808)) "x")"#).await.unwrap_err();
    assert_eq!(promoted.message,"long overflow");
    for pattern in [r#"(let [f /] (str (f 1.0 0.0)))"#,r#"(str (/ (identity 1.0) 0))"#,r#"(str (/ 1 0))"#,
        r#"(str (/ (identity "not numeric") 0))"#,r#"(let [f /] (str (f "not numeric" 0)))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().message,"Divide by zero","{pattern}");
    }
}

#[tokio::test]
async fn host_arithmetic_preserves_inline_arity_and_selected_runtime_errors() {
    for pattern in [r#"(if true "a" (inc))"#,r#"(if true "a" (dec' 1 2))"#,r#"(str (+ 9223372036854775807 1))"#,r#"(str (/ 1M 3M))"#,r#"(str (+ 1M (/ 1 3)))"#,r#"(str (/ ##NaN "not numeric"))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code,"xtdb.error/unknown","{pattern}");
    }
    for pattern in [r#"(if true "a" (-))"#,r#"(if true "a" (/))"#,r#"(if true "a" (+ 9223372036854775807 1))"#,r#"(let [f inc] (if true "a" (f)))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{pattern}");
    }
}

#[tokio::test]
async fn host_exact_decimal_failures_preserve_jvm_arithmetic_messages() {
    for (form,message) in [
        ("(/ 1M 3M)","Non-terminating decimal expansion; no exact representable decimal result."),
        ("(+ 1M (/ 1 3))","Non-terminating decimal expansion; no exact representable decimal result."),
        ("(* 1M (/ 1 3))","Non-terminating decimal expansion; no exact representable decimal result."),
        ("(* 1E-2147483647M 1.0M)","Underflow"),
        ("(* 1E+2147483647M 1E+2M)","Overflow"),
    ] {
        let error = project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err();
        assert_eq!(error.code,"xtdb.error/unknown");
        assert_eq!(error.message,message,"{form}");
    }
}

#[tokio::test]
async fn host_numeric_comparisons_cross_categories_and_keep_nan_unordered() {
    for (form,expected) in [
        ("(< 1 2)",true),("(<= 2 2)",true),("(> 3 2)",true),("(>= 2 2)",true),
        ("(== 1 1.0 1M 1N)",true),("(== (/ 1 2) 0.5M)",true),
        ("(< 9007199254740992N 9007199254740993N)",true),
        ("(== 9007199254740993N 9007199254740992.0)",true),
        ("(< 1E-2147483647M 1M)",true),("(> -1E-2147483647M -1M)",true),
        ("(== 1.200M 1.2M)",true),("(< -1.21M -1.200M)",true),
        ("(zero? -0.0)",true),("(pos? 1N)",true),("(neg? (/ -1 3))",true),
        ("(zero? ##NaN)",false),("(pos? ##NaN)",false),("(neg? ##NaN)",false),
        ("(< ##NaN 1)",false),("(<= 1 ##NaN)",false),("(> ##NaN ##NaN)",false),
        ("(>= ##NaN 1)",false),("(== ##NaN ##NaN)",false),
        ("(let [f <] (f 1 2))",true),("(clojure.core/pos? 1)",true),
    ] {
        let expected = if expected { "true" } else { "false" };
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str {form}) "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
    assert_eq!(project(r#"(regexp-replace "a" (str (== (/ 1 3) 0.3M)) "x")"#).await.unwrap_err().message,"Non-terminating decimal expansion; no exact representable decimal result.");
}

#[tokio::test]
async fn host_comparison_chains_evaluate_arguments_before_short_circuiting_numeric_pairs() {
    for (form,expected) in [(r#"(< "not numeric")"#,true),("(== nil)",true),(r#"(< 3 2 "not numeric")"#,false),(r#"(== 1 2 "not numeric")"#,false)] {
        let expected = if expected { "true" } else { "false" };
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str {form}) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    for form in [r#"(< 3 2 (subs "x" -1))"#,r#"(let [f <] (f 3 2 (subs "x" -1)))"#,r#"(== 1 2 (subs "x" -1))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err().message,"Host substring indexes are out of range");
    }
    for pattern in [r#"(if true "a" (<))"#,r#"(if true "a" (==))"#,r#"(let [f zero?] (if true "a" (f)))"#,r#"(let [< str] (< "a"))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    for pattern in [r#"(if true "a" (zero?))"#,r#"(if true "a" (pos? 1 2))"#,r#"(if true "a" (neg?))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code,"xtdb.error/unknown");
    }
}

#[tokio::test]
async fn host_primitive_boolean_metadata_preserves_unresolved_static_calls() {
    for form in [
        "(byte (if true 127.9 (+ 1.0 (boolean nil))))",
        "(byte (if true 127.9 (+ 1.0 (zero? 0))))",
        "(byte (if true 127.9 (+ 1.0 (< 1 2))))",
        "(byte (if true 127.9 (+ 1.0 (zero? true))))",
        "(let [b (boolean nil)] (byte (if true 127.9 (+ 1.0 b))))",
        "(byte (if true 127.9 (+ 1.0 (if true (boolean nil) (pos? 1)))))",
        "(byte (if true 127.9 (+ 1.0 (do 0 (boolean nil)))))",
        "(byte (if true 127.9 (+ 1.0 (and (boolean nil) (pos? 1)))))",
        "(byte (if true 127.9 (+ 1.0 (or (boolean nil) (pos? 1)))))",
        "(byte (if true 127.9 (+ 1.0 (boolean (identity 1)))))",
        "(byte (if true 127.9 (double (boolean nil))))",
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "127" (str {form}) "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
    for form in [
        "(byte (if true 127.9 (+ 1.0 true)))",
        "(byte (if true 127.9 (+ 1.0 'false)))",
        "(byte (if true 127.9 (+ 1.0 (boolean 1))))",
        "(byte (if true 127.9 (+ 1.0 (zero? (boolean nil)))))",
        "(let [f boolean] (byte (if true 127.9 (+ 1.0 (f nil)))))",
        "(byte (if true 127.9 (+ 1.0 (not nil))))",
        "(byte (if true 127.9 (double true)))",
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err().message,"Host numeric cast is out of range","{form}");
    }
    assert_eq!(project(r#"(regexp-replace "NaN" (str (/ (boolean nil) ##NaN)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
    assert_eq!(project(r#"(regexp-replace "a" (str (/ false ##NaN)) "x")"#).await.unwrap_err().message,"Host value is not a Number");
}
