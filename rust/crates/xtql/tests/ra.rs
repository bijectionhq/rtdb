use std::collections::{BTreeMap, BTreeSet};

use rtdb_plan::{Catalog, Expr, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, MemoryBudget, Result, ScalarType, Value};
use rtdb_xtql::{CompileOptions, compile_ra_in};

struct EmptyCatalog;

impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

#[test]
fn logical_relations_keep_case_sensitive_columns_and_normalize_parameter_names() {
    let plan = "[:project {:projections [A a {copied ?ap_n}]} [:select {:predicate (> A ?ap_n)} [:table {:rows [{A 3 a 1} {A 0}]}]]]";
    let memory = MemoryBudget::new(1 << 20);
    let integer = LogicalType::Scalar(ScalarType::I64);
    let options = CompileOptions { named_parameters: BTreeMap::from([("ap-n".into(), (0, integer.clone()))]), ..Default::default() };
    let prepared = compile_ra_in(plan, &EmptyCatalog, &options, &memory).unwrap();
    assert_eq!(prepared.parameter_types, BTreeMap::from([(0, integer.clone())]));
    let Statement::Query(output) = prepared.statement else { panic!("Expected query") };
    let RelationNode::Project { input, expressions } = output.node() else { panic!("Expected projection") };
    assert_eq!(expressions.iter().map(|expression| expression.field.name.as_str()).collect::<Vec<_>>(), ["A", "a", "copied"]);
    assert_ne!(expressions[0].expr, expressions[1].expr);
    assert_eq!(expressions[1].field.data_type, integer.clone().maybe());
    assert_eq!(expressions[2].expr, Expr::Parameter { index: 0, data_type: integer });
    let RelationNode::Filter { input, .. } = input.node() else { panic!("Expected selection") };
    let RelationNode::Values { rows } = input.node() else { panic!("Expected table") };
    assert_eq!(rows[1], [Expr::Literal(Value::I64(0)), Expr::Literal(Value::Null)]);
}

#[test]
fn logical_table_keeps_zero_column_cardinality() {
    let memory = MemoryBudget::new(1 << 20);
    let prepared = compile_ra_in("[:table {:rows [{} {} {}]}]", &EmptyCatalog, &CompileOptions::default(), &memory).unwrap();
    let Statement::Query(output) = prepared.statement else { panic!("Expected query") };
    let RelationNode::Values { rows } = output.node() else { panic!("Expected table") };
    assert!(output.schema().is_empty());
    assert_eq!(rows, &[vec![], vec![], vec![]]);
}
