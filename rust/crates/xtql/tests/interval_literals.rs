use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate_async};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{Result,Value as V};
use rtdb_xtql::{CompileOptions,compile};

struct Empty;
impl Catalog for Empty {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(source: &str) -> Result<V> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {source}}}))");
    let Statement::Query(relation) = compile(&source,&Empty,&CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).await
}

#[tokio::test]
async fn the_zero_interval_spelling_reads_as_zero() {
    for source in ["\"P\"","\"PT\"","\"-P\""] {
        assert_eq!(project(&format!("#xt/interval {source}")).await.unwrap(),V::IntervalYearMonth(0),"{source}");
    }
    let sum = project("(+ #xt/interval \"PT-23H-59M-59.001S\" #xt/interval \"P\")").await.unwrap();
    assert_eq!(sum,V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: -86_399_001_000_000 });
    assert!(project("#xt/duration \"PT\"").await.is_err());
}

#[tokio::test]
async fn a_period_duration_past_interval_range_raises_the_expression_overflow() {
    let error = project("#xt/period-duration [\"P200000000Y\" \"PT0S\"]").await.unwrap_err();
    assert_eq!(error.code,"xtdb.expression/overflow-error");
    assert_eq!(error.message.as_deref(),Some("data exception - overflow error"));
}
