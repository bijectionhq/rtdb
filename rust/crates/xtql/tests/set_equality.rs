use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{Result,Value};
use rtdb_xtql::{CompileOptions,compile};

struct Empty;
impl Catalog for Empty {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(vec![]) }
}
fn project(expression: &str) -> Result<Value> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&Empty,&CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    evaluate(&expressions[0].expr,input.schema(),&[],&EvalContext::default())
}
#[test]
fn xtql_set_equality_rejects_nested_and_unselected_type_pairs() {
    for expression in ["(= #{} #{})","(<> #{} #{})","(= [#{}] [#{} 1])","(= {:a #{}} {:a #{}})","(if false (= #{} #{}) true)","(distinct-from #{} #{})"] {
        assert!(project(expression).is_err(),"{expression}");
    }
    assert_eq!(project("(= {:a #{}} {:b #{}})").unwrap(),Value::Bool(false));
    assert_eq!(project("(= #{} nil)").unwrap(),Value::Null);
    assert_eq!(project("(=== #{} #{})").unwrap(),Value::Bool(false));
}
