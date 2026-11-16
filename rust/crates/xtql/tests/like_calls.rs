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
async fn xtql_generic_like_and_ilike_share_native_pattern_and_null_contracts() {
    for (source,expected) in [
        (r#"(like "a.b" "a.b")"#,V::Bool(true)),
        (r#"(like "axb" "a.b")"#,V::Bool(false)),
        (r#"(ilike "ı" "I")"#,V::Bool(true)),
        ("(like (cast (/ 1 0) #xt/type :utf8) nil)",V::Null),
        ("(ilike nil (cast (/ 1 0) #xt/type :utf8))",V::Null),
    ] { assert_eq!(project(source).await.unwrap(),expected,"{source}"); }
    for source in [r#"(like 1 "%")"#,r#"(ilike "a")"#,r#"(like "a" "a" "escape")"#] { assert!(project(source).await.is_err(),"{source}"); }
}
