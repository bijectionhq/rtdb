use std::{collections::BTreeSet};
use rtdb_expr::{EvalContext, evaluate_async};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, Result, Value};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(expression: &str) -> Result<(LogicalType, Value)> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!() };
    let expression = &expressions[0].expr;
    Ok((expression.data_type(), evaluate_async(expression, input.schema(), &[], &EvalContext::default()).await?))
}


#[tokio::test(start_paused = true)]
async fn xtql_between_macros_retain_once_binding_and_symmetry() {
    let start = tokio::time::Instant::now();
    assert_eq!(project("(between-symmetric (coalesce (nullif (sleep (cast 5 #xt/type [:duration :milli])) nil) 7) 10 5)").await.unwrap().1,Value::Bool(true));
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
    assert_eq!(project("(between 7 10 5)").await.unwrap().1,Value::Bool(false));
    assert_eq!(project("(between-symmetric 7 nil 5)").await.unwrap().1,Value::Null);
    assert!(project("(between 1 2)").await.is_err());
    assert!(project("(between-symmetric 1 2 3 4)").await.is_err());
}
