use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate};
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
fn project(expression: &str) -> Result<V> {
    let prepared = compile(&format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))"),&Empty,&CompileOptions::default())?;
    let Statement::Query(relation) = prepared.statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    evaluate(&expressions[0].expr,input.schema(),&[],&EvalContext::default())
}
#[test]
fn xtql_interval_constructor_calls_preserve_literal_metadata_and_unused_payloads() {
    assert_eq!(project("(single-field-interval 3 \"YEAR\" 2 (/ 1 0))").unwrap(),V::IntervalYearMonth(36));
    assert_eq!(project("(single_field_interval \"1.000000001\" \"SECOND\" 2 0)").unwrap(),V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 1_000_000_001 });
    assert_eq!(project("(multi-field-interval \"1 2:3:4.123456789\" \"DAY\" 2 \"SECOND\" 9)").unwrap(),V::IntervalMonthDayNano { months: 0,days: 1,nanoseconds: 7_384_123_456_789 });
    assert_eq!(project("(single-field-interval (/ 1 0) \"DAY\" 2 nil)").unwrap(),V::Null);
    assert_eq!(project("(single-field-interval 3 \"DAY\" 0 6)").unwrap_err().code,"xtdb.expression/invalid-interval-precision");
    assert_eq!(project("(single-field-interval 3 \"SECOND\" 2 (+ 1 1))").unwrap_err().code,"xtdb.expression/non-literal-interval-metadata");
}
