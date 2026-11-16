use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate_async};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{LogicalType as L,Result,ScalarType as S,TimeUnit as U,Value as V};
use rtdb_xtql::{CompileOptions,compile};

struct Empty;
impl Catalog for Empty {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
async fn project(source: &str,zone: &str) -> (L,V) {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {source}}}))");
    let Statement::Query(relation) = compile(&source,&Empty,&CompileOptions { default_timezone: zone.into(),..Default::default() }).unwrap().statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    let expression = &expressions[0].expr;
    (expression.data_type(),evaluate_async(expression,input.schema(),&[],&EvalContext { timezone: zone,..Default::default() }).await.unwrap())
}

#[tokio::test]
async fn xtql_temporal_functions_share_scalar_units_and_lazy_bin_readers() {
    assert_eq!(project("(extract \"WEEK\" #xt/date \"2021-01-01\")","UTC").await.1,V::I32(53));
    assert_eq!(project("(date-trunc \"MONTH\" #xt/date \"2024-03-19\")","UTC").await.1,V::DateDay(19783));
    assert_eq!(project("(age #xt/date \"2024-01-01\" #xt/date \"2023-01-01\")","UTC").await.1,V::IntervalMonthDayMicro { months: 12,days: 0,nanoseconds: 0 });
    assert_eq!(project("(cardinality (range-bins #xt/interval \"PT15M\" #xt/zoned-date-time \"2020-01-01T00:13Z\" #xt/zoned-date-time \"2020-01-01T00:38Z\"))","UTC").await.1,V::I32(3));
    assert_eq!(project("(date-bin 15 -10 0)","UTC").await.1,V::I64(0));
    assert_eq!(project("(current-timezone)","Europe/Paris").await.1,V::Utf8("Europe/Paris".into()));
    assert_eq!(project("(current-timestamp 0)","Europe/Paris").await,(L::Scalar(S::Timestamp { unit: U::Second,timezone: Some("Europe/Paris".into()) }),V::Timestamp { value: 0,unit: U::Second,timezone: Some("Europe/Paris".into()) }));
    assert_eq!(project("(localtimestamp 0)","Europe/Paris").await.1,V::Timestamp { value: 3600,unit: U::Second,timezone: None });
}

#[test]
fn xtql_temporal_fields_keep_case_and_literal_requirements() {
    for expression in ["(extract \"year\" #xt/date \"2024-01-01\")","(current-time 1.5)","(date-trunc \"DAY\" #xt/date \"2024-01-01\" \"UTC\")"] {
        assert!(compile(&format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))"),&Empty,&CompileOptions::default()).is_err(),"{expression}");
    }
}

#[tokio::test]
async fn xtql_clock_uses_the_exact_execution_instant() {
    let Statement::Query(relation) = compile("(-> (rel [{}] []) (return {:v (current-timestamp 9)}))",&Empty,&CompileOptions::default()).unwrap().statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    let context = EvalContext { current_time: rtdb_types::Instant::new(0,999).unwrap(),..Default::default() };
    assert_eq!(evaluate_async(&expressions[0].expr,input.schema(),&[],&context).await.unwrap(),V::Timestamp { value: 999,unit: U::Nanosecond,timezone: Some("UTC".into()) });
}
