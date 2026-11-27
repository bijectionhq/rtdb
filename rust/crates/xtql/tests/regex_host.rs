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
async fn regex_patterns_use_portable_reader_literals_and_allow_list_calls() {
    for expression in [
        r#"(regexp-replace "a" "\141" "x")"#,
        r#"(regexp-replace "a" (str \o141) "x")"#,
        r#"(regexp-replace "a" (str (subs "a!" 0 2/2)) "x")"#,
        r#"(regexp-replace "1/3" (str (/ 1 3)) "x")"#,
        r#"(regexp-replace "[" (java.util.regex.Pattern/quote "[") "x")"#,
    ] {
        assert_eq!(project(expression).await.unwrap(), Value::Utf8("x".into()), "{expression}");
    }
    for expression in [
        r#"(regexp-replace "a" (java.lang.System/getProperty "user.name") "x")"#,
        r#"(regexp-replace "a" (str/join ["a"]) "x")"#,
        r#"(regexp-replace "a" (Pattern/quote "a") "x")"#,
    ] {
        assert_eq!(project(expression).await.unwrap_err().code, "rtdb.expr/regex-host-form", "{expression}");
    }
}

#[tokio::test]
async fn raw_ratios_reach_host_numeric_and_quoted_pattern_forms() {
    for (source, pattern) in [
        ("-5/3", "(str -010/06)"),
        ("a", r#"(if (ratio? 6/8) "a" "b")"#),
        ("a", r#"(if (and (integer? 6/3) (> 9007199254740993/9007199254740992 1)) "a" "b")"#),
        ("1/2", "(str (+ 1/6 1/3))"),
        ("-3", "(str (long -7/2))"),
        ("1/2", "(str (apply + '(1/6 1/3)))"),
        ("[3/4 -5/3]", "(java.util.regex.Pattern/quote (str '[+06/08 -10/6]))"),
        ("3/4:5/3", r#"(clojure.string/join ":" [(identity 6/8) '10/6])"#),
    ] {
        let expression = format!(r#"(regexp-replace "{source}" {pattern} "x")"#);
        assert_eq!(project(&expression).await.unwrap(), Value::Utf8("x".into()), "{pattern}");
    }
}

#[tokio::test]
async fn raw_ratios_preserve_regex_host_demand_and_admission() {
    let direct = project(r#"(regexp-replace "a" 2/3 "x")"#).await.unwrap_err();
    assert_eq!(direct.message.as_deref(), Some("REGEXP_REPLACE host pattern did not produce a String"));

    for pattern in ["(arbitrary/call 2/3)", "(str #arbitrary/object 2/3)"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code, "rtdb.expr/regex-host-form", "{pattern}");
    }
    assert_eq!(project(r#"(regexp-replace nil (arbitrary/call 2/3) "x")"#).await.unwrap(), Value::Null);
    assert_eq!(project(r#"(regexp-replace nil (str 2/0) "x")"#).await.unwrap_err().code, "xtql/edn-reader");
    assert_eq!(project(r#"(regexp-replace "a" #=(str 2/3) "x")"#).await.unwrap_err().code, "xtql/edn-reader");
    assert_eq!(project("2/3").await.unwrap_err().code, "xtql/invalid-literal");
}

#[tokio::test]
async fn conditional_host_bindings_scope_only_the_selected_body() {
    for (source, pattern) in [
        ("a", r#"(let [p "a"] (if-let [p (str p "b")] (subs p 0 1) "b"))"#),
        ("a", r#"(let [p "a"] (if-let [p false] (subs "x" -1) (let [inner p] inner)))"#),
        ("a", r#"(let [p "b"] (when-let [p "a"] (identity "ignored") p))"#),
        ("a", r#"(or (if-let [p nil] (subs "x" -1)) "a")"#),
        ("a", r#"(or (when-let [p false] (subs "x" -1) p) "a")"#),
        ("a", r#"(or (clojure.core/when-let [p true]) "a")"#),
        ("a", r#"(if-let [p ""] (str p "a") "b")"#),
        ("1", r#"(if-let [n 0] (str (inc n)) "b")"#),
        ("126", r#"(if-let [n 126.9] (str (byte n)) "b")"#),
        ("a", r#"(let [if-let str] (if-let "a"))"#),
        ("a", r#"(let [if-let str] (clojure.core/if-let [p "a"] p "b"))"#),
        ("a", r#"(if-let [when-let str] (when-let "a") "b")"#),
    ] {
        let expression = format!(r#"(regexp-replace "{source}" {pattern} "x")"#);
        assert_eq!(project(&expression).await.unwrap(), Value::Utf8("x".into()), "{pattern}");
    }
    for pattern in [
        r#"(if-let [p (subs "x" -1)] p "a")"#,
        r#"(when-let [p "a"] (subs "x" -1) p)"#,
    ] {
        let expression = format!(r#"(regexp-replace "a" {pattern} "x")"#);
        assert_eq!(project(&expression).await.unwrap_err().message.as_deref(), Some("Host substring indexes are out of range"));
    }
    for pattern in [
        r#"(if-let [n 127.9] (str (byte n)) "a")"#,
        r#"(when-let [n 127.9] (str (byte n)))"#,
    ] {
        let expression = format!(r#"(regexp-replace "a" {pattern} "x")"#);
        assert_eq!(project(&expression).await.unwrap_err().message.as_deref(), Some("Host numeric cast is out of range"));
    }
}

#[tokio::test]
async fn conditional_host_bindings_compile_vector_shape_and_both_branch_scopes() {
    for (pattern, message) in [
        (r#"(if true "a" (if-let [p "a"]))"#, "Host form has the wrong number of arguments"),
        (r#"(if-let [p "a"] p "b" "c")"#, "Host form has the wrong number of arguments"),
        (r#"(when-let)"#, "Host form has the wrong number of arguments"),
        (r#"(if-let (p "a") "a")"#, "Host conditional binding requires a binding vector"),
        (r#"(when-let [p "a" q "b"] p)"#, "Host conditional binding requires one binding pair"),
        (r#"(when-let [ns/p "a"] "a")"#, "Host let cannot bind a qualified symbol"),
        (r#"(if-let [p "a"] p p)"#, "Host symbol or namespace is not implemented"),
        (r#"(if-let [p p] "a" "b")"#, "Host symbol or namespace is not implemented"),
    ] {
        let expression = format!(r#"(regexp-replace "a" {pattern} "x")"#);
        assert_eq!(project(&expression).await.unwrap_err().message.as_deref(), Some(message), "{pattern}");
    }
    let error = project(r#"(regexp-replace "a" (when-let [[p] ["a"]] p) "x")"#).await.unwrap_err();
    assert_eq!(error.code, "rtdb.expr/regex-host-form");
}

#[tokio::test]
async fn conditional_host_expansion_counts_toward_the_instruction_depth_limit() {
    let nested = |depth| {
        let mut pattern = r#""a""#.to_owned();
        for _ in 0..depth { pattern = format!("(if-let [p true] {pattern})"); }
        format!(r#"(regexp-replace "a" {pattern} "x")"#)
    };
    assert_eq!(project(&nested(63)).await.unwrap(), Value::Utf8("x".into()));
    assert_eq!(project(&nested(64)).await.unwrap_err().code, "rtdb.expr/regex-host-limit");
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
    assert_eq!(direct.message.as_deref(),Some("long overflow"));
    let indirect = project(r#"(regexp-replace "a" (let [f +] (str (f 9223372036854775807 1 (subs "x" -1)))) "x")"#).await.unwrap_err();
    assert_eq!(indirect.message.as_deref(),Some("Host substring indexes are out of range"));
    let boxed = project(r#"(regexp-replace "a" (let [f -] (str (f -9223372036854775808 -9223372036854775808))) "x")"#).await.unwrap_err();
    assert_eq!(boxed.message.as_deref(),Some("long overflow"));
    let promoted = project(r#"(regexp-replace "a" (str (- (+' -9223372036854775808 0) -9223372036854775808)) "x")"#).await.unwrap_err();
    assert_eq!(promoted.message.as_deref(),Some("long overflow"));
    for pattern in [r#"(let [f /] (str (f 1.0 0.0)))"#,r#"(str (/ (identity 1.0) 0))"#,r#"(str (/ 1 0))"#,
        r#"(str (/ (identity "not numeric") 0))"#,r#"(let [f /] (str (f "not numeric" 0)))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().message.as_deref(),Some("Divide by zero"),"{pattern}");
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
        assert_eq!(error.message.as_deref(),Some(message),"{form}");
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
    assert_eq!(project(r#"(regexp-replace "a" (str (== (/ 1 3) 0.3M)) "x")"#).await.unwrap_err().message.as_deref(),Some("Non-terminating decimal expansion; no exact representable decimal result."));
}

#[tokio::test]
async fn host_comparison_chains_evaluate_arguments_before_short_circuiting_numeric_pairs() {
    for (form,expected) in [(r#"(< "not numeric")"#,true),("(== nil)",true),(r#"(< 3 2 "not numeric")"#,false),(r#"(== 1 2 "not numeric")"#,false)] {
        let expected = if expected { "true" } else { "false" };
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str {form}) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    for form in [r#"(< 3 2 (subs "x" -1))"#,r#"(let [f <] (f 3 2 (subs "x" -1)))"#,r#"(== 1 2 (subs "x" -1))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
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
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host numeric cast is out of range"),"{form}");
    }
    assert_eq!(project(r#"(regexp-replace "NaN" (str (/ (boolean nil) ##NaN)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
    assert_eq!(project(r#"(regexp-replace "a" (str (/ false ##NaN)) "x")"#).await.unwrap_err().message.as_deref(),Some("Host value is not a Number"));
}

#[tokio::test]
async fn host_integral_division_preserves_categories_scales_and_floating_roundoff() {
    for (form,expected) in [
        ("(quot -7 3)","-2"),("(rem -7 3)","-1"),("(mod -7 3)","2"),("(mod 7 -3)","-2"),
        ("(quot -9223372036854775808 -1)","-9223372036854775808"),("(rem -9223372036854775808 -1)","0"),
        ("(quot -9223372036854775808N -1)","9223372036854775808"),("(mod -9223372036854775808 -1)","0"),
        ("(quot (/ 7 2) 1)","3"),("(rem (/ 7 2) 1)","1/2"),("(mod (/ -7 2) 1)","1/2"),
        ("(quot 2.00M 0.5M)","4.0"),("(quot 1.00M 3M)","0.00"),("(rem 5.00M 2M)","1.00"),
        ("(quot 1E+3M 4M)","2.5E+2"),("(rem 1E+3M 4M)","0E+1"),("(mod -7.00M 3M)","2.00"),
        ("(quot -0.0 1.0)","0.0"),("(rem -0.0 1.0)","-0.0"),("(mod -0.0 1.0)","-0.0"),
        ("(rem -0.0 -1.0)","0.0"),("(mod -0.0 -1.0)","0.0"),
        ("(rem 1.0 0.1)","0.0"),("(rem 1.0 ##Inf)","NaN"),("(quot 1.0 ##Inf)","0.0"),
        ("(mod 1.0 ##Inf)","NaN"),("(quot 1E100 1.0)","1.0E100"),("(rem 1E100 1.0)","0.0"),
        ("(quot (float 7.5) (short 2))","3.0"),("(clojure.core/rem 7 3)","1"),
        ("(let [f quot] (f -7 3))","-2"),("(let [f rem] (f -7 3))","-1"),("(let [f mod] (f -7 3))","2"),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str "\\Q" {form} "\\E") "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
}

#[tokio::test]
async fn host_integral_division_admits_boxed_rhs_before_lhs_without_nan_shortcuts() {
    for operation in ["quot","rem"] {
        for (arguments,message) in [
            ("1 0","/ by zero"),("1.0 0","Divide by zero"),("##NaN 0.0","Divide by zero"),
            (r#"(identity "not numeric") 0"#,"Divide by zero"),(r#""not numeric" 0.0"#,"Divide by zero"),
            ("(boolean nil) 0","Divide by zero"),("(zero? 1) 0.0","Divide by zero"),
            ("##NaN 1","Infinite or NaN"),("1 ##NaN","Infinite or NaN"),("##Inf 1.0","Infinite or NaN"),
            (r#""not numeric" ##NaN"#,"Host value is not a Number"),(r#"##NaN "not numeric""#,"Host value is not a Number"),
            (r#"(subs "x" -1) 0"#,"Host substring indexes are out of range"),
        ] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some(message),"{operation} {arguments}");
        }
        for arguments in ["1 0",r#""not numeric" 0"#,"##NaN 0"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (str (f {arguments}))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Divide by zero"));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} 1M (/ 1 3))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Non-terminating decimal expansion; no exact representable decimal result."));
    }
    assert_eq!(project(r#"(regexp-replace "a" (str (mod 1 0)) "x")"#).await.unwrap_err().message.as_deref(),Some("Divide by zero"));
}

#[tokio::test]
async fn host_integral_inline_arity_and_mixed_result_metadata_remain_distinct() {
    for operation in ["quot","rem"] {
        for arguments in ["","1","1 2 3"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({operation} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (if true "a" (f))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (f (subs "x" -1))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [{operation} str] ({operation} "a")) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        for arguments in ["2.0 (identity 1)","(identity 2) 1.0","2.0 (boolean nil)","2.0 (pos? 1)"] {
            assert_eq!(project(&format!(r#"(regexp-replace "127" (str (byte (if true 127.9 ({operation} {arguments})))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (byte (if true 127.9 ({operation} 2.0 1)))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host numeric cast is out of range"));
        assert_eq!(project(&format!(r#"(regexp-replace "0" (str (- (if true -9223372036854775808 ({operation} 2 1)) -9223372036854775808)) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (- (if true -9223372036854775808 ({operation} 2 (identity 1))) -9223372036854775808)) "x")"#)).await.unwrap_err().message.as_deref(),Some("long overflow"));
    }
    assert_eq!(project(r#"(regexp-replace "a" (if true "a" (mod)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
    assert_eq!(project(r#"(regexp-replace "a" (mod (subs "x" -1)) "x")"#).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    assert_eq!(project(r#"(regexp-replace "a" (str (- (if true -9223372036854775808 (mod 2 1)) -9223372036854775808)) "x")"#).await.unwrap_err().message.as_deref(),Some("long overflow"));
    assert_eq!(project(r#"(regexp-replace "Infinity" (str (/ (quot 2.0 1) 0)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
    assert_eq!(project(r#"(regexp-replace "a" (str (/ (quot 2.0 (identity 1)) 0)) "x")"#).await.unwrap_err().message.as_deref(),Some("Divide by zero"));
}

#[tokio::test]
async fn host_bit_operations_keep_long_results_and_mask_signed_long_counts() {
    for (form,expected) in [
        ("(bit-not (int 0))","-1"),("(bit-and (byte -1) 255)","255"),
        ("(bit-and 15 7 3)","3"),("(bit-or 1 2 4)","7"),("(bit-xor 15 7 3)","11"),("(bit-and-not 15 3 4)","8"),
        ("(bit-shift-left 1 4294967296)","1"),("(bit-shift-left 1 -1)","-9223372036854775808"),
        ("(bit-shift-left 9223372036854775807 1)","-2"),("(bit-shift-right -2 1)","-1"),
        ("(unsigned-bit-shift-right -1 1)","9223372036854775807"),("(unsigned-bit-shift-right -1 0)","-1"),
        ("(unsigned-bit-shift-right -1 -1)","1"),("(unsigned-bit-shift-right -1 4294967296)","-1"),
        ("(bit-clear -1 -1)","9223372036854775807"),("(bit-set 0 -1)","-9223372036854775808"),
        ("(bit-flip -9223372036854775808 63)","0"),("(bit-test -9223372036854775808 -1)","true"),
        ("(bit-test 1 64)","true"),("(bit-test 1 -1)","false"),("(bit-set 0 4294967296)","1"),
        ("(bit-clear -1 -9223372036854775808)","-2"),("(bit-flip 1 4294967296)","0"),
        ("(clojure.core/bit-and 12 10)","8"),("(let [f bit-xor] (f 15 7 3))","11"),
        ("(let [f bit-not] (f 0))","-1"),("(let [f bit-test] (f 1 64))","true"),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str "\\Q" {form} "\\E") "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
}

#[tokio::test]
async fn host_bit_admission_rejects_other_numeric_categories_before_the_next_operand() {
    for (value,class) in [("1N","clojure.lang.BigInt"),("1M","java.math.BigDecimal"),("(/ 1 2)","clojure.lang.Ratio"),("(float 1)","java.lang.Float"),("1.0","java.lang.Double")] {
        let message = format!("bit operation not supported for: class {class}");
        for form in [format!("(bit-not {value})"),format!(r#"(bit-and {value} "not numeric")"#),format!("(bit-shift-left 1 {value})"),format!("(bit-test 1 {value})")] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str {form}) "x")"#)).await.unwrap_err().message.as_deref(),Some(message.as_str()),"{form}");
        }
    }
    for value in [r#"\a"#,"true","nil",r#""not numeric""#,"[]",":a"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (bit-not {value})) "x")"#)).await.unwrap_err().code,"xtdb.error/unknown");
    }
    for operation in ["bit-and","bit-or","bit-xor","bit-and-not"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} 1N 1 (subs "x" -1))) "x")"#)).await.unwrap_err().message.as_deref(),Some("bit operation not supported for: class clojure.lang.BigInt"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (str (f 1N 1 (subs "x" -1)))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} 1N (subs "x" -1))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    }
    assert_eq!(project(r#"(regexp-replace "a" (str (bit-and "not numeric" 1N)) "x")"#).await.unwrap_err().code,"xtdb.error/unknown");
    assert_eq!(project(r#"(regexp-replace "a" (str (bit-set 1N (subs "x" -1))) "x")"#).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
}

#[tokio::test]
async fn host_bit_inline_arity_distinguishes_fixed_variadic_and_ordinary_calls() {
    for (operation,arguments) in [("bit-not",""),("bit-not","1 2"),("bit-shift-left","1"),("bit-shift-right",""),("unsigned-bit-shift-right","1 2 3")] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({operation} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
    }
    for operation in ["bit-and","bit-or","bit-xor","bit-and-not","bit-clear","bit-set","bit-flip","bit-test"] {
        for arguments in ["","1N"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({operation} {arguments})) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
        }
    }
    for operation in ["bit-not","bit-shift-left","bit-test"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (if true "a" (f))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [{operation} str] ({operation} "a")) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    assert_eq!(project(r#"(regexp-replace "a" (let [f bit-not] (f 1 (subs "x" -1))) "x")"#).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    assert_eq!(project(r#"(regexp-replace "a" (if true "a" (bit-set 1 2 3)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
}

#[tokio::test]
async fn host_bit_metadata_preserves_unresolved_calls_and_resolved_outer_calls() {
    for form in [
        "(bit-and 1.0 1)","(bit-not 1.0)","(bit-and (float 1) 1)","(bit-shift-left 1 1.0)",
        "(bit-and 1 (boolean nil))","(bit-not (pos? 1))","(let [b (boolean nil)] (bit-not b))",
        "(bit-not (if true (boolean nil) (pos? 1)))","(bit-not (do 0 (boolean nil)))",
        "(bit-not (and (boolean nil) (pos? 1)))","(bit-not (or (boolean nil) (pos? 1)))",
        "(bit-clear 1 0)","(bit-set 0 1)","(bit-flip 1 0)","(bit-test 1 0)","(let [f bit-not] (f 1))",
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (- (if true -9223372036854775808 {form}) -9223372036854775808)) "x")"#)).await.unwrap_err().message.as_deref(),Some("long overflow"),"{form}");
    }
    for form in [
        "(bit-and 1 1)","(bit-not (byte 0))","(bit-and (identity 1.0) 1)","(bit-and true 1)",
        "(bit-and 1.0 1 1)","(bit-not (bit-not 1.0))","(bit-not (boolean 1))","(bit-not (pos? (boolean nil)))",
        "(bit-not (identity true))","(bit-shift-left 1 (identity 1.0))",
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "0" (str (- (if true -9223372036854775808 {form}) -9223372036854775808)) "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
    assert_eq!(project(r#"(regexp-replace "a" (str (byte (if true 127.9 (+ 1.0 (bit-test 1 0))))) "x")"#).await.unwrap_err().message.as_deref(),Some("Host numeric cast is out of range"));
}

#[tokio::test]
async fn host_extrema_preserve_selected_categories_scales_and_primitive_zero_rules() {
    for (form,expected) in [
        (r#"(max "a")"#,"a"),(r#"(str (min nil) "a")"#,"a"),
        ("(min 1.0M 1.00M)","1.00"),("(max 1.00M 1.0M)","1.0"),
        ("(bit-not (min 1N 1))","-2"),("(bit-not (max 1N 1))","-2"),
        ("(max 0.0 -0.0)","0.0"),("(let [f max] (f 0.0 -0.0))","-0.0"),
        ("(min -0.0 0.0)","-0.0"),("(let [f min] (f -0.0 0.0))","0.0"),
        ("(max (float 1.2) 1)","1.2000000476837158"),("(let [f max] (f (float 1.2) 1))","1.2"),
        ("(max 9007199254740992.0 9007199254740993)","9007199254740993"),
        ("(min 9007199254740992.0 9007199254740993)","9007199254740993"),
        ("(clojure.core/min 3 2 1)","1"),("(max 1 2 3)","3"),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str "\\Q" {form} "\\E") "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
    for operation in ["min","max"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (bit-not ({operation} 1 1N))) "x")"#)).await.unwrap_err().message.as_deref(),Some("bit operation not supported for: class clojure.lang.BigInt"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} 1M (/ 1 3))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Non-terminating decimal expansion; no exact representable decimal result."));
    }
}

#[tokio::test]
async fn host_extrema_check_nan_before_numeric_admission_but_after_argument_payloads() {
    for operation in ["min","max"] {
        for arguments in [r#"##NaN "not numeric""#,r#""not numeric" ##NaN"#,"##NaN (boolean nil)","false ##NaN"] {
            assert_eq!(project(&format!(r#"(regexp-replace "NaN" (str ({operation} {arguments})) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        }
        for (form,class) in [
            (format!("({operation} (float ##NaN) 1)"),"java.lang.Double"),
            (format!("(let [f {operation}] (f (float ##NaN) 1))"),"java.lang.Float"),
        ] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str (bit-not {form})) "x")"#)).await.unwrap_err().message.as_deref().expect("present diagnostic message"),format!("bit operation not supported for: class {class}"));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} ##NaN (subs "x" -1))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({operation} "not numeric" 1 (subs "x" -1))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host value is not a Number"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {operation}] (str (f "not numeric" 1 (subs "x" -1)))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    }
}

#[tokio::test]
async fn host_extrema_recompute_each_nested_result_metadata_and_keep_ordinary_arity() {
    for operation in ["min","max"] {
        for arguments in ["1 2","1 2 3"] {
            assert_eq!(project(&format!(r#"(regexp-replace "0" (str (- (if true -9223372036854775808 ({operation} {arguments})) -9223372036854775808)) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        }
        for arguments in ["1 2.0","1 2.0 3","1 true","1 (boolean nil)","1 (identity 2)"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str (- (if true -9223372036854775808 ({operation} {arguments})) -9223372036854775808)) "x")"#)).await.unwrap_err().message.as_deref(),Some("long overflow"));
        }
        for arguments in ["1.0 2.0","1.0 (float 2)"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str (byte (if true 127.9 ({operation} {arguments})))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host numeric cast is out of range"));
        }
        for arguments in ["1.0 2.0 3","1.0 true","1.0 (boolean nil)"] {
            assert_eq!(project(&format!(r#"(regexp-replace "127" (str (byte (if true 127.9 ({operation} {arguments})))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({operation})) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        assert_eq!(project(&format!(r#"(regexp-replace "a" ({operation}) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [{operation} str] ({operation} "a")) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
}

#[tokio::test]
async fn host_absolute_value_preserves_category_scale_and_inline_metadata() {
    for (form,expected) in [
        ("(abs -9223372036854775808)","-9223372036854775808"),("(abs -9223372036854775808N)","9223372036854775808"),
        ("(abs (byte -128))","128"),("(abs (float -1.2))","1.2000000476837158"),
        ("(let [f abs] (f (float -1.2)))","1.2000000476837158"),("(abs -0.0)","0.0"),
        ("(abs ##-Inf)","Infinity"),("(abs ##NaN)","NaN"),("(abs -1.20M)","1.20"),
        ("(abs (/ -2 3))","2/3"),("(clojure.core/abs -3)","3"),
        ("(bit-not (abs (byte -1)))","-2"),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str "\\Q" {form} "\\E") "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{form}");
    }
    for arguments in ["","1 2"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" (abs {arguments})) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (abs {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
    }
    assert_eq!(project(r#"(regexp-replace "a" (abs 1 (subs "x" -1)) "x")"#).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    for form in ["(abs 1.0)","(abs (float 1))"] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str (byte (if true 127.9 {form}))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host numeric cast is out of range"));
    }
    for form in ["(abs (identity 1.0))","(abs (boolean nil))","(abs true)","(let [f abs] (f 1.0))"] {
        assert_eq!(project(&format!(r#"(regexp-replace "127" (str (byte (if true 127.9 {form}))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
}

const CLASSIFICATIONS: [&str;11] = ["number?","integer?","int?","double?","float?","ratio?","decimal?","rational?","pos-int?","neg-int?","nat-int?"];

#[tokio::test]
async fn host_classifications_distinguish_numeric_categories_without_coercion() {
    let cases: &[(&str,&[&str])] = &[
        ("(byte 1)",&["number?","integer?","int?","rational?","pos-int?","nat-int?"]),
        ("(short -1)",&["number?","integer?","int?","rational?","neg-int?"]),
        ("(int 0)",&["number?","integer?","int?","rational?","nat-int?"]),
        ("(long 1)",&["number?","integer?","int?","rational?","pos-int?","nat-int?"]),
        ("-9223372036854775808",&["number?","integer?","int?","rational?","neg-int?"]),
        ("9223372036854775807",&["number?","integer?","int?","rational?","pos-int?","nat-int?"]),
        ("1N",&["number?","integer?","rational?"]),
        ("'1N",&["number?","integer?","rational?"]),
        ("0N",&["number?","integer?","rational?"]),
        ("-1N",&["number?","integer?","rational?"]),
        ("9223372036854775808",&["number?","integer?","rational?"]),
        ("(float 1)",&["number?","float?"]),
        ("(float ##NaN)",&["number?","float?"]),
        ("1.0",&["number?","double?","float?"]),
        ("-0.0",&["number?","double?","float?"]),
        ("##NaN",&["number?","double?","float?"]),
        ("##Inf",&["number?","double?","float?"]),
        ("1.00M",&["number?","decimal?","rational?"]),
        ("'1.00M",&["number?","decimal?","rational?"]),
        ("1E-2147483647M",&["number?","decimal?","rational?"]),
        ("(/ 7 2)",&["number?","ratio?","rational?"]),
        ("(/ 1 3)",&["number?","ratio?","rational?"]),
        ("(/ 4 2)",&["number?","integer?","int?","rational?","pos-int?","nat-int?"]),
        ("(/ 4N 2)",&["number?","integer?","rational?"]),
    ];
    for (argument,accepted) in cases {
        for predicate in CLASSIFICATIONS {
            let expected = accepted.contains(&predicate);
            assert_eq!(project(&format!(r#"(regexp-replace "{expected}" (str ({predicate} {argument})) "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{predicate} {argument}");
        }
    }
}

#[tokio::test]
async fn host_classifications_return_false_for_nonnumbers_without_reading_quoted_children() {
    for predicate in CLASSIFICATIONS {
        for argument in ["nil","(if false 1)","false","true",r#""""#,r#""1""#,r#"(str "1")"#,r"\a",":a","'a","[]","{}","#{}","()","'()","'[1]","'{:a 1}","'#{1}","'(1)","str","(integer? 1)",r#"'[(subs "x" -1)]"#] {
            assert_eq!(project(&format!(r#"(regexp-replace "false" (str ({predicate} {argument})) "x")"#)).await.unwrap(),Value::Utf8("x".into()),"{predicate} {argument}");
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({predicate} [(subs "x" -1)])) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    }
}

#[tokio::test]
async fn host_classifications_keep_ordinary_call_arity_and_boxed_result_metadata() {
    for predicate in CLASSIFICATIONS {
        for arguments in ["","1 2"] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({predicate} {arguments})) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({predicate} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host form has the wrong number of arguments"));
        }
        for arguments in [r#"(subs "x" -1) 0"#,r#"0 (subs "x" -1)"#] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str ({predicate} {arguments})) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
            assert_eq!(project(&format!(r#"(regexp-replace "a" (let [f {predicate}] (str (f {arguments}))) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
        }
        for result in [format!("({predicate} 1)"),format!("(let [v ({predicate} 1)] v)"),format!("(let [f {predicate}] (f 1))")] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" (str (/ {result} ##NaN)) "x")"#)).await.unwrap_err().message.as_deref(),Some("Host value is not a Number"));
        }
        for call in [format!("(clojure.core/{predicate} nil)"),format!("(let [f {predicate}] (f nil))")] {
            assert_eq!(project(&format!(r#"(regexp-replace "false" (str {call}) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (let [{predicate} str] ({predicate} "a")) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
    assert_eq!(project(r#"(regexp-replace "NaN" (str (/ (zero? 1) ##NaN)) "x")"#).await.unwrap(),Value::Utf8("x".into()));
}

#[tokio::test]
async fn host_non_ifn_values_fail_before_argument_evaluation() {
    for function in ["true","false","(number? 1)","(zero? 1)","1","1.0","1N","1M","(/ 1 2)","(byte 1)","(float 1)",r#""x""#,r#"(str "x")"#,r"\x","()","'(1)","'#(identity %)"] {
        for body in [format!(r#"({function} (subs "x" -1))"#),format!(r#"(let [f {function}] (f (subs "x" -1)))"#)] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" {body} "x")"#)).await.unwrap_err().message.as_deref(),Some("Host value is not callable"),"{body}");
        }
        assert_eq!(project(&format!(r#"(regexp-replace "a" (if true "a" ({function} (subs "x" -1))) "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
}

#[tokio::test]
async fn host_null_and_ifn_values_evaluate_arguments_before_invocation() {
    for function in ["nil","(if false 1)","str","[]","'[1]","{}","'{:a 1}","#{}","'#{1}",":a","'a",r#"'[(subs "x" -1)]"#] {
        for body in [format!(r#"({function} (subs "x" -1))"#),format!(r#"(let [f {function}] (f (subs "x" -1)))"#)] {
            assert_eq!(project(&format!(r#"(regexp-replace "a" {body} "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"),"{body}");
        }
    }
    for body in [r#"(identity (subs "x" -1) 0)"#,r#"(number? 0 (subs "x" -1))"#,r#"((subs "x" -1) (/ 1 0))"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {body} "x")"#)).await.unwrap_err().message.as_deref(),Some("Host substring indexes are out of range"));
    }
    assert_eq!(project(r#"(regexp-replace "a" ([(/ 1 0)] (subs "x" -1)) "x")"#).await.unwrap_err().message.as_deref(),Some("Divide by zero"));
    for body in [r#"(str "a")"#,r#"(identity "a")"#,r#"(let [f str] (f "a"))"#,r#"(if (number? 1) "a" "b")"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {body} "x")"#)).await.unwrap(),Value::Utf8("x".into()));
    }
}

#[tokio::test]
async fn host_string_library_forms_compute_patterns() {
    for (source,pattern,expected) in [
        ("a.b.c",r#"(java.util.regex.Pattern/quote ".")"#,"axbxc"),
        ("a1b",r#"(format "%s%d" "a" 1)"#,"xb"),
        ("abc",r#"(apply str ["a" "b"])"#,"xc"),
        ("a|b",r#"(java.util.regex.Pattern/quote (clojure.string/join "|" ["a" "b"]))"#,"x"),
        ("AB",r#"(clojure.string/upper-case "ab")"#,"x"),
        ("ab",r#"(clojure.string/lower-case "AB")"#,"x"),
        ("a.b",r#"(clojure.string/replace "a-b" "-" "\\.")"#,"x"),
        ("ab",r#"(clojure.string/trim " ab ")"#,"x"),
        (r#"[1 "a"]"#,r#"(java.util.regex.Pattern/quote (str [1 "a"]))"#,"x"),
    ] {
        assert_eq!(project(&format!(r#"(regexp-replace {source:?} {pattern} "x")"#)).await.unwrap(),Value::Utf8(expected.into()),"{pattern}");
    }
    for pattern in [r#"(Pattern/quote ".")"#,r#"(System/getProperty "user.dir")"#,r#"(str/join ["a"])"#] {
        assert_eq!(project(&format!(r#"(regexp-replace "a" {pattern} "x")"#)).await.unwrap_err().code,"rtdb.expr/regex-host-form","{pattern}");
    }
}
