use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate_async};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{Result,Value as V};
use rtdb_xtql::{CompileOptions,compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(expression: &str) -> Result<V> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).await
}

#[tokio::test]
async fn raw_replacement_fields_preserve_missing_extra_and_seqable_metadata() {
    for (source,expected) in [
        (r#"(regexp-replace)"#,V::Null),
        (r#"(regexp-replace nil unresolved nil 1 nil false missing)"#,V::Null),
        (r#"(regexp-replace "A" "a" "x" nil nil [\i])"#,V::Utf8("x".into())),
        (r#"(regexp-replace "A" "a" "x" false false #{\i})"#,V::Utf8("x".into())),
        (r#"(regexp-replace "A" "a" "x" nil nil (str "i"))"#,V::Utf8("A".into())),
        (r#"(regexp-replace "A" "a" "x" nil nil {\i true})"#,V::Utf8("A".into())),
        (r#"(regexp-replace "a" "a" "x" nil nil nil missing (/ 1 0))"#,V::Utf8("x".into())),
        (r#"(coalesce "kept" (regexp-replace "a" unresolved nil 1 nil false))"#,V::Utf8("kept".into())),
        (r#"(coalesce "kept" (regexp-replace 1 "a" "x"))"#,V::Utf8("kept".into())),
    ] { assert_eq!(project(source).await.unwrap(),expected,"{source}"); }
}

#[tokio::test]
async fn replacement_metadata_errors_precede_source_payloads_and_control_selection() {
    for source in [
        r#"(regexp-replace (cast (/ 1 0) #xt/type :utf8) "[" "x")"#,
        r#"(if true "ok" (regexp-replace "a" "[" "x"))"#,
        r#"(cardinality [(regexp-replace "a" "[" "x")])"#,
    ] { assert_eq!(project(source).await.unwrap_err().code,"xtdb.expression/illegal-argument-exception","{source}"); }
    for source in [r#"(regexp-replace "a" "a" "x" 0)"#,r#"(regexp-replace "a" "a" "x" nil [])"#] {
        assert_eq!(project(source).await.unwrap_err().code,"xtdb.expression/unsupported");
    }
    assert_eq!(project(r#"(regexp-replace "a" "a")"#).await.unwrap_err().code,"xtdb.error/unknown");
    assert_eq!(project(r#"(regexp-replace "a" "a" "x" nil nil false)"#).await.unwrap_err().code,"xtdb.error/unknown");
}

#[test]
fn host_pattern_forms_remain_owned_raw_metadata_without_variable_dependencies() {
    let source = r#"(-> (rel [{}] []) (return {:v (regexp-replace "a" (str "a") "x")}))"#;
    let Statement::Query(relation) = compile(source,&EmptyCatalog,&CompileOptions::default()).unwrap().statement else { panic!() };
    let RelationNode::Project { expressions,.. } = relation.node() else { panic!() };
    let rtdb_plan::Expr::RegexReplace { recipe,.. } = &expressions[0].expr else { panic!() };
    assert!(raw_list_begins_with_str(recipe.pattern.view()));
    assert_eq!(expressions[0].expr.parameter_count(),0);
}

fn raw_list_begins_with_str(form: rtdb_plan::RegexFormView<'_>) -> bool {
    match form.kind() {
        rtdb_plan::RegexFormKind::List(values) => matches!(values.first(), Some(first) if matches!(first.kind(), rtdb_plan::RegexFormKind::Symbol(name) if name == "str")),
        _ => false,
    }
}
