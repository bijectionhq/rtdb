use std::collections::BTreeSet;
use rtdb_expr::{EvalContext, evaluate_async};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{Result, Value as V};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
async fn project(expression: &str) -> Result<V> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default())?.statement else { panic!("query") };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!("projection") };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).await
}

#[tokio::test]
async fn xtql_generic_comparison_calls_keep_magnitudes_null_order_and_type_admission() {
    for (source,expected) in [
        (r#"(compare "ab" "cb")"#,V::I32(-512)),
        ("(compare 9007199254740993 9007199254740992)",V::I32(1)),
        ("(compare (/ 1 0) nil)",V::Null),
        ("(compare-nulls-first (/ 1 0) nil)",V::I32(1)),
        ("(compare_nulls_last nil (/ 1 0))",V::I32(1)),
        ("(compare-nulls-last nil nil)",V::I32(0)),
    ] { assert_eq!(project(source).await.unwrap(),expected,"{source}"); }
    for source in ["(compare 1)","(compare [] [])",r#"(compare "x" 1)"#,"(compare-nulls-first 1 2 3)"] { assert!(project(source).await.is_err(),"{source}"); }
}
