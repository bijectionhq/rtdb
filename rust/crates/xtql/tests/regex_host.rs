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
