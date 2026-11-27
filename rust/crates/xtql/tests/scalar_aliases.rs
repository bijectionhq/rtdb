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
async fn xtql_shared_scalar_aliases_preserve_type_selection_and_nested_unknowns() {
    for (source,expected) in [
        ("(is-true true)",V::Bool(true)),("(is_false false)",V::Bool(true)),
        ("(is-true (/ 1 0))",V::Bool(false)),("(distinct-from (/ 1 0) nil)",V::Bool(true)),
        ("(distinct_from [nil] [nil])",V::Null),
        ("(_meta_double 12)",V::F64(12.0)),("(_meta-double (cast (/ 1 0) #xt/type :utf8))",V::Null),
        ("(log 2 8)",V::F64(3.0)),
    ] { assert_eq!(project(source).await.unwrap(),expected,"{source}"); }
    for source in ["(is-true)","(is-false nil nil)","(distinct-from 1)","(_meta_double 1 2)","(log 100)","(log nil)"] { assert!(project(source).await.is_err(),"{source}"); }
}
