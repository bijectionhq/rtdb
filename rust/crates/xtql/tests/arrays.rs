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
async fn xtql_array_functions_keep_the_shared_type_and_reader_contract() {
    for (source, expected) in [
        ("(array-upper [1 2] 1)", Value::I32(2)),
        ("(array-lower [] 1)", Value::I32(1)),
        ("(array_length [] 1)", Value::I32(0)),
        ("(trim-array [42 43] 1)", Value::list(vec![Value::I64(42)])),
        ("(trim_array [42] 1)", Value::list(vec![])),
        ("(cardinality (trim-array [1 2] -1))", Value::I32(3)),
        ("(nth (trim-array [1 2] -1) 0)", Value::I64(1)),
        ("(array-lower (trim-array [] -2147483648) 1)", Value::I32(1)),
        ("(nth (trim-array [] -2147483648) -1)", Value::Null),
        ("(trim-array nil 1)", Value::Null),
    ] { assert_eq!(project(source).await.1, expected, "{source}"); }
    assert_eq!(project("(array-length [1 2] 1)").await.0, LogicalType::Scalar(ScalarType::I32));
    assert_eq!(project("(trim-array [42 43] 1)").await.0, LogicalType::list(LogicalType::Scalar(ScalarType::I64)));
}
