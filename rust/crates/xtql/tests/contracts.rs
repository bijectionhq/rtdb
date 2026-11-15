use rtdb_plan::{ApplyKind, BinaryOp, Catalog, Expr as PlanExpr, QualifiedTable, Relation, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, Result, ScalarType, TableRef, Value};
use rtdb_xtql::{CompileOptions, EdnKind, Expr, Query, compile, parse_query, read};
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

struct FixtureCatalog;

impl Catalog for FixtureCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok(Some(TableBinding { name: name.clone(), oid: None, columns: BTreeMap::from([
            ("_id".into(), LogicalType::Scalar(ScalarType::Keyword)),
            ("first_name".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("last_name".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("age".into(), LogicalType::Scalar(ScalarType::I64)),
        ]) }))
    }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        Ok(self.table(&QualifiedTable { database: database.into(), table: TableRef::new("public", "docs") })?.into_iter().collect())
    }
}

fn plan(source: &str) -> Arc<Relation> {
    let prepared = compile(source, &FixtureCatalog, &CompileOptions::default()).unwrap();
    match prepared.statement { Statement::Query(relation) => relation, _ => panic!("Expected a query") }
}

fn find<'a>(relation: &'a Relation, predicate: &impl Fn(&RelationNode) -> bool) -> Option<&'a Relation> {
    if predicate(relation.node()) { return Some(relation); }
    for child in relation.children() { if let Some(found) = find(child, predicate) { return Some(found); } }
    None
}

#[test]
fn edn_keywords_symbols_strings_and_source_spans_remain_distinct() {
    let source = "[:name name \"name\" ; comment\n #_42 nil]";
    let value = read(source).unwrap();
    assert_eq!(value.span.start, 0);
    assert_eq!(value.span.end, source.len());
    let EdnKind::Vector(values) = value.kind else { panic!("Expected vector") };
    assert_eq!(values.len(), 4);
    assert!(matches!(&values[0].kind, EdnKind::Keyword(name) if name == "name"));
    assert!(matches!(&values[1].kind, EdnKind::Symbol(name) if name == "name"));
    assert!(matches!(&values[2].kind, EdnKind::String(name) if name == "name"));
    assert!(matches!(values[3].kind, EdnKind::Nil));
}

#[test]
fn edn_rejects_duplicate_keys_reader_evaluation_and_invalid_radices() {
    for source in ["{:a 1 :a 2}", "#{1 1}", "#=(System/exit 0)", "37r123", "1r1", "[] []", "{:a}"] {
        assert!(read(source).is_err(), "{source}");
    }
    assert_eq!(read("\"\\uD83D\\uDE00\"").unwrap().kind, EdnKind::String("😀".into()));
}

#[test]
fn xtdb_xtql_test_parse_from_keeps_parameter_expressions_distinct_from_shorthand() {
    let query = parse_query(&read("(fn [a] (from :foo [a {:other a}]))").unwrap()).unwrap();
    let Query::Function { body, .. } = query else { panic!("Expected function") };
    let Query::From(from) = *body else { panic!("Expected from") };
    assert!(matches!(&from.bindings[0].expression, Expr::LogicVariable(name) if name == "a"));
    assert!(matches!(&from.bindings[1].expression, Expr::Variable(name) if name == "a"));
}

#[test]
fn xtdb_xtql_test_parse_from_rejects_missing_bindings_and_invalid_options() {
    for source in ["(from :foo)", "(from :foo {})", "(from :foo {:bind [a] :bar x})", "(from :foo [{bar x}])"] {
        assert!(parse_query(&read(source).unwrap()).is_err(), "{source}");
    }
}

#[test]
fn xtdb_xtql_plan_test_from_normalizes_aliases_and_preserves_duplicate_projections() {
    let relation = plan("(from :docs [{:xt/id e} {:first-name name} {:first-name also-name}])");
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["also_name", "e", "name"]);
    assert_ne!(relation.schema().fields()[0].id, relation.schema().fields()[2].id);
}

#[test]
fn xtdb_xtql_plan_test_from_unification_adds_a_value_predicate_after_the_scan() {
    let relation = plan("(from :docs [{:first-name name :last-name name}])");
    let filtered = find(&relation, &|node| matches!(node, RelationNode::Filter { predicate: PlanExpr::Binary { op: BinaryOp::Eq, .. }, .. })).unwrap();
    assert!(find(filtered, &|node| matches!(node, RelationNode::Scan(_))).is_some());
    assert_eq!(relation.schema().len(), 1);
}

#[test]
fn xtdb_xtql_plan_test_from_star_is_rejected_in_unify() {
    let error = compile("(unify (from :docs [*]))", &FixtureCatalog, &CompileOptions::default()).unwrap_err();
    assert_eq!(error.code, "xtql/invalid-from");
}

#[test]
fn redundant_from_star_bindings_do_not_filter_null_cells_through_self_equality() {
    let relation = plan("(from :docs [* first-name {:first-name first-name}])");
    assert!(find(&relation, &|node| matches!(node, RelationNode::Filter { .. })).is_none());
}

#[test]
fn xtdb_xtql_plan_test_unify_schedules_bindings_before_their_dependants() {
    let relation = plan("(unify (where (= answer 42)) (with {answer (+ base 1)}) (with {base 41}))");
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["answer", "base"]);
    assert!(find(&relation, &|node| matches!(node, RelationNode::Filter { .. })).is_some());
    assert_eq!(compile("(unify (with {a b} {b a}))", &FixtureCatalog, &CompileOptions::default()).unwrap_err().code, "no-available-clauses");
}

#[test]
fn xtdb_xtql_plan_test_rel_preserves_rows_and_missing_column_types() {
    let relation = plan("(rel [{:a 12 :b \"foo\"} {:a 1 :c \"bar\"}] [a b c])");
    let fields = relation.schema().fields();
    assert_eq!(fields[0].data_type, LogicalType::Scalar(ScalarType::I64));
    assert_eq!(fields[1].data_type, LogicalType::Scalar(ScalarType::Utf8).maybe());
    assert_eq!(fields[2].data_type, LogicalType::Scalar(ScalarType::Utf8).maybe());
    assert!(find(&relation, &|node| matches!(node, RelationNode::Set { all: true, .. })).is_some());
}

#[test]
fn xtdb_xtql_plan_test_left_join_filters_unifying_variables_inside_the_dependent_plan() {
    let relation = plan("(unify (from :docs [first-name]) (left-join (rel [{:first-name \"Ivan\" :age 21}] [first-name age]) [first-name age]))");
    let apply = find(&relation, &|node| matches!(node, RelationNode::Apply { kind: ApplyKind::Left, .. })).unwrap();
    let RelationNode::Apply { dependent, arguments, .. } = apply.node() else { panic!("Expected apply") };
    assert_eq!(arguments.len(), 1);
    assert!(matches!(dependent.node(), RelationNode::Filter { predicate: PlanExpr::Binary { op: BinaryOp::Eq, .. }, .. }));
    assert!(relation.schema().fields().iter().find(|field| field.name == "age").unwrap().data_type.nullable());
}

#[test]
fn xtdb_xtql_plan_test_nested_subquery_parameters_do_not_inflate_external_arity() {
    let prepared = compile("(fn [wanted] (-> (rel [{:a wanted}] [a]) (with {:result (q [(fn [inner] (rel [{:result inner}] [result])) a])})))", &FixtureCatalog,
        &CompileOptions { parameter_types: vec![LogicalType::Scalar(ScalarType::I64)], ..Default::default() }).unwrap();
    assert_eq!(prepared.parameter_types.len(), 1);
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    assert_eq!(relation.parameter_count(), 1);
    assert!(find(&relation, &|node| matches!(node, RelationNode::Apply { kind: ApplyKind::Scalar, .. })).is_some());
}

#[test]
fn xtdb_xtql_plan_test_pull_many_retains_nullable_empty_result_semantics() {
    let relation = plan("(unify (with {orders (pull* (rel [] []))}))");
    let apply = find(&relation, &|node| matches!(node, RelationNode::Apply { kind: ApplyKind::PullMany, .. })).unwrap();
    let RelationNode::Apply { output: Some(output), .. } = apply.node() else { panic!("Expected scalar apply") };
    assert!(output.data_type.nullable());
    assert!(output.data_type.legs().iter().any(|leg| matches!(leg, LogicalType::List { .. })));
}

#[test]
fn scalar_subqueries_reject_multiple_output_columns_during_binding() {
    assert_eq!(compile("(unify (with {x (q (rel [{:a 1 :b 2}] [a b]))}))", &FixtureCatalog, &CompileOptions::default()).unwrap_err().code, "xtql/invalid-scalar-subquery");
}

#[test]
fn xtdb_xtql_plan_test_aggregate_expressions_preserve_group_and_aggregate_scopes() {
    let relation = plan("(-> (from :docs [first-name age]) (aggregate first-name {:total (+ (sum age) 1)} {:count (row-count)}))");
    let aggregate = find(&relation, &|node| matches!(node, RelationNode::Aggregate { .. })).unwrap();
    let RelationNode::Aggregate { groups, aggregates, .. } = aggregate.node() else { panic!("Expected aggregate") };
    assert_eq!(groups.len(), 1);
    assert_eq!(aggregates.len(), 2);
    assert!(compile("(-> (from :docs [age]) (aggregate {:x (sum (sum age))}))", &FixtureCatalog, &CompileOptions::default()).is_err());
    assert!(compile("(-> (from :docs [age first-name]) (aggregate {:x (+ age (sum age))}))", &FixtureCatalog, &CompileOptions::default()).is_err());
}

#[test]
fn xtdb_xtql_test_parse_expr_preserves_jvm_literal_widths_and_tag_payloads() {
    let query = parse_query(&read("(rel [{:decimal 12.30M :date #xt/date \"2020-01-01\" :instant #inst \"2020\" :duration #xt/duration \"PT3H1M35.23S\" :bytes #bytes \"80ff00\"}] [])").unwrap()).unwrap();
    let Query::Rel { input: rtdb_xtql::RelationInput::Documents(documents), .. } = query else { panic!("Expected relation") };
    let values: BTreeMap<_, _> = documents[0].iter().map(|(name, expr)| match expr { Expr::Literal(value) => (name.as_str(), value), _ => panic!("Expected literal") }).collect();
    assert!(matches!(values["decimal"], Value::Decimal(value) if value.precision == 32 && value.bit_width == 128 && value.scale == 2));
    assert_eq!(values["date"], &Value::DateDay(18262));
    assert_eq!(values["bytes"], &Value::Binary(vec![128, 255, 0]));
}

#[test]
fn anonymous_functions_bind_percent_parameters_without_reader_evaluation() {
    let prepared = compile("#(rel [{:name %}] [name])", &FixtureCatalog,
        &CompileOptions { parameter_types: vec![LogicalType::Scalar(ScalarType::Utf8)], ..Default::default() }).unwrap();
    assert_eq!(prepared.parameter_types.len(), 1);
}

#[test]
fn temporal_period_projection_keeps_hidden_endpoints_out_of_the_result() {
    let relation = plan("(from :docs [{:xt/valid-time period}])");
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["period"]);
    assert!(find(&relation, &|node| matches!(node, RelationNode::Project { expressions, .. } if expressions.iter().any(|value| matches!(&value.expr, PlanExpr::Call { name, .. } if name == "period")))).is_some());
}
