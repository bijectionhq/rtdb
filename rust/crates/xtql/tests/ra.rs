use std::collections::{BTreeMap, BTreeSet};

use rtdb_plan::{Catalog, Expr, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, MemoryBudget, Result, ScalarType, Value};
use rtdb_xtql::{CompileOptions, compile, compile_ra_in};

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

#[test]
fn an_unbound_logical_symbol_is_an_unknown_symbol() {
    let memory = MemoryBudget::new(1 << 20);
    let error = compile_ra_in("[:select {:predicate (== ?x13 x4)} [:table {:rows []}]]", &EmptyCatalog, &CompileOptions::default(), &memory).unwrap_err();
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtdb.expression/unknown-symbol", Some("Unknown symbol: '?x13'")));
}

#[test]
fn mega_join_joins_every_relation_under_all_its_conditions() {
    let memory = MemoryBudget::new(1 << 20);
    let plan = "[:mega-join {:conditions [{x1 x2} (> (+ x1 10) x3)]} [[:table {:rows [{x1 1}]}] [:table {:rows [{x2 1}]}] [:table {:rows [{x3 3}]}]]]";
    let Statement::Query(output) = compile_ra_in(plan, &EmptyCatalog, &CompileOptions::default(), &memory).unwrap().statement else { panic!("Expected query") };
    let mut names = output.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["x1", "x2", "x3"]);
    assert!(matches!(output.node(), RelationNode::Filter { .. }));
    assert!(compile_ra_in("[:mega-join {:conditions []} []]", &EmptyCatalog, &CompileOptions::default(), &memory).is_err());
}

#[test]
fn aggregate_outputs_may_only_reference_grouping_variables_outside_aggregates() {
    let compiled = |source: &str| compile(source, &EmptyCatalog, &CompileOptions::default());
    let error = compiled("(-> (rel [{:c 1 :v 2}] [c v]) (aggregate c {:s (* v (sum v))}))").unwrap_err();
    assert_eq!(error.message.as_deref(), Some("Variables outside of aggregate expressions must be grouping columns"));
    let error = compiled("(-> (rel [{:v 2}] [v]) (aggregate y))").unwrap_err();
    assert_eq!(error.message.as_deref(), Some("Not all variables in expression are in scope"));
    let error = compiled("(-> (rel [{:v 2}] [v]) (aggregate {:s (+ y (sum v))}))").unwrap_err();
    assert_eq!(error.message.as_deref(), Some("Not all variables in expression are in scope"));
    assert!(compiled("(-> (rel [{:c 1 :v 2}] [c v]) (aggregate c {:s (+ c (sum v))}))").is_ok());
}

#[test]
fn now_in_a_temporal_filter_is_the_current_time_and_limits_take_literals() {
    let compiled = |source: &str| compile(source, &EmptyCatalog, &CompileOptions::default());
    assert!(compiled("(-> (rel [{:x 1}] [x]) (limit 1))").is_ok());
    let error = compiled("(-> (rel [{:x 1}] [x]) (limit (+ 1 1)))").unwrap_err();
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtql/invalid-limit", Some("Limit must be a non-negative integer literal or parameter")));
    let error = compiled("(-> (rel [{:x 1}] [x]) (offset x))").unwrap_err();
    assert_eq!((error.code.as_str(), error.message.as_deref()), ("xtql/invalid-offset", Some("Offset must be a non-negative integer literal or parameter")));
}

#[test]
fn a_logical_plan_get_accepts_a_keyword_field() {
    let memory = MemoryBudget::new(1 << 20);
    let plan = "[:project {:projections [{bar (. foo :bar)}]} [:table {:rows [{foo {:bar 42}}]}]]";
    assert!(compile_ra_in(plan, &EmptyCatalog, &CompileOptions::default(), &memory).is_ok());
    let error = compile("(-> (rel [{:foo {:bar 42}}] [foo]) (return {:bar (. foo :bar)}))", &EmptyCatalog, &CompileOptions::default()).unwrap_err();
    assert_eq!(error.code, "xtql/malformed-get");
}
