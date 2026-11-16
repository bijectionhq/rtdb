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
async fn patch_fields_preserve_continuation_controls_and_lazy_lists() {
    let start = tokio::time::Instant::now();
    assert_eq!(project("(. (_patch (sleep (cast 5 #xt/type [:duration :milli])) {:v 7}) v)").await.unwrap().1,Value::I64(7));
    assert_eq!(tokio::time::Instant::now(),start);
    assert_eq!(project("(. (_patch {:v 7 :unused [(/ 1 0)]} {:v nil}) v)").await.unwrap().1,Value::I64(7));
    assert_eq!(project("(. (_patch {:v (/ 1 0)} {:v 7}) v)").await.unwrap_err().code,"xtdb.expression/division-by-zero");
}
