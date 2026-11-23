use std::{collections::BTreeSet};
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

#[tokio::test]
async fn xtql_series_uses_explicit_end_inclusion_and_lazy_integer_reads() {
    for (source,expected) in [
        ("(generate-series 1 4 1)",Value::list(vec![Value::I64(1),Value::I64(2),Value::I64(3)])),
        ("(generate_series 1 4 1 true)",Value::list(vec![Value::I64(1),Value::I64(2),Value::I64(3),Value::I64(4)])),
        ("(generate-series 3 0 -1)",Value::list(vec![Value::I64(3),Value::I64(2),Value::I64(1)])),
        ("(cardinality (generate-series 0 2147483647 1))",Value::I32(i32::MAX)),
        ("(nth (generate-series 0 2147483647 1) 2147483646)",Value::I64(2147483646)),
        ("(generate-series nil 4 0)",Value::Null),
    ] { assert_eq!(project(source).await.1,expected,"{source}"); }
    assert_eq!(project("(generate-series 0 4 1)").await.0,LogicalType::list(LogicalType::Scalar(ScalarType::I64)));
}
