use std::{collections::BTreeSet, time::Duration};
use rtdb_expr::{EvalContext, evaluate_async};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(expression: &str) -> (LogicalType, Value) {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!() };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!() };
    let expression = &expressions[0].expr;
    (expression.data_type(), evaluate_async(expression, input.schema(), &[], &EvalContext::default()).await.unwrap())
}

#[tokio::test(start_paused = true)]
async fn expression_random_and_duration_sleep_have_reference_types_and_timing() {
    let (ty, value) = project("(random)").await;
    assert_eq!(ty, LogicalType::Scalar(ScalarType::F64));
    let Value::F64(value) = value else { panic!() };
    assert!((0.0..1.0).contains(&value));
    let start = tokio::time::Instant::now();
    assert_eq!(project("(sleep (cast 0.125 #xt/type [:duration :milli]))").await, (LogicalType::Null, Value::Null));
    assert_eq!(tokio::time::Instant::now() - start, Duration::from_millis(125));
    assert_eq!(project("(sleep nil)").await, (LogicalType::Null, Value::Null));
}
