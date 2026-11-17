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

#[tokio::test]
async fn xtql_registered_math_calls_keep_the_shared_numeric_results() {
    for (source, expected) in [("(log10 100)",Value::F64(2.0)),("(sinh 0)",Value::F64(0.0)),("(cosh 0)",Value::F64(1.0)),("(tanh 0)",Value::F64(0.0)),("(double 7)",Value::F64(7.0)),("(mod -5 3)",Value::I64(1))] {
        assert_eq!(project(source).await.unwrap().1,expected);
    }
}

#[tokio::test(start_paused = true)]
async fn xtql_mod_discards_payloads_at_either_null_argument_and_preserves_selection_demands() {
    let start = tokio::time::Instant::now();
    for source in [
        "(mod nil (/ 1 0))", "(mod (/ 1 0) nil)",
        "(mod (sleep (cast \"PT0.06S\" #xt/type [:duration :milli])) (/ 1 0))",
        "(mod (/ 1 0) (sleep (cast \"PT0.06S\" #xt/type [:duration :milli])))",
    ] {
        assert_eq!(project(source).await.unwrap(), (LogicalType::Null,Value::Null), "{source}");
    }
    assert_eq!(tokio::time::Instant::now(), start);
    for source in ["(mod (if (= (/ 1 0) 0) 1 2) nil)", "(mod (if true (/ 1 0) nil) nil)"] {
        assert_eq!(project(source).await.unwrap_err().code, "xtdb.expression/division-by-zero", "{source}");
    }
}

#[tokio::test]
async fn xtql_mod_requires_exactly_two_arguments() {
    for source in ["(mod)","(mod 1)","(mod 1 2 3)"] { assert!(project(source).await.is_err(),"{source}"); }
}
