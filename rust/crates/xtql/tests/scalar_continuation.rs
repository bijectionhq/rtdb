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
async fn xtql_scalar_calls_preserve_type_selection_and_if_truth() {
    let start = tokio::time::Instant::now();
    for source in [
        "(+ (/ 1 0) nil)", "(+ nil (/ 1 0))", "(< (/ 1 0) nil)", "(<> (/ 1 0) nil)",
        "(power (/ 1 0) nil)", "(power nil (/ 1 0))", "(log (/ 1 0) nil)",
        "(substring (cast (/ 1 0) #xt/type :utf8) nil)", "(string-to-array nil (cast (/ 1 0) #xt/type :utf8))",
        "(cast (sleep (cast \"PT0.005S\" #xt/type [:duration :milli])) #xt/type :i64)",
    ] { assert_eq!(project(source).await.unwrap().1,Value::Null,"{source}"); }
    for (source,expected) in [
        ("(nil? (/ 1 0))",Value::Bool(false)),("(true? (/ 1 0))",Value::Bool(false)),("(boolean (/ 1 0))",Value::Bool(true)),
        ("(if (/ 1 0) 1 2)",Value::I64(1)),("(if (true? (/ 1 0)) 1 2)",Value::I64(2)),
        ("(and 0 true)",Value::Bool(true)),("(or false 0)",Value::Bool(true)),
        ("(coalesce (sleep (cast \"PT0.005S\" #xt/type [:duration :milli])) 7)",Value::I64(7)),
    ] { assert_eq!(project(source).await.unwrap().1,expected,"{source}"); }
    assert_eq!(tokio::time::Instant::now(),start);
    for source in ["(power (if (= (/ 1 0) 0) 1 2) nil)","(power (if true (/ 1 0) nil) nil)","(greatest (/ 1 0) nil)","(nullif (/ 1 0) nil)","(and (/ 1 0) false)"] {
        assert_eq!(project(source).await.unwrap_err().code,"xtdb.expression/division-by-zero","{source}");
    }
    assert_eq!(project("(nullif (sleep (cast \"PT0.005S\" #xt/type [:duration :milli])) (/ 1 0))").await.unwrap().1,Value::Null);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_millis(5));
}
