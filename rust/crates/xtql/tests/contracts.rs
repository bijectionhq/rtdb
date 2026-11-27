use rtdb_types::StructFields;
use rtdb_plan::{ApplyKind, BinaryOp, Binder, Catalog, Expr as PlanExpr, QualifiedTable, Relation, RelationNode, Statement, TableBinding, TemporalFilter};
use rtdb_types::{LogicalType, Result, ScalarType, TableRef, Value};
use rtdb_xtql::{CompileOptions, EdnKind, Expr, Query, compile, compile_query, parse_query, read};
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

struct FixtureCatalog;

impl Catalog for FixtureCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok(Some(TableBinding { name: name.clone(), oid: None, columns: StructFields::from([
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
fn parser_diagnostics_identify_the_kind_of_binding_or_option() {
    for (source, code, message) in [
        ("(from :docs [{age x}])", "xtql/malformed-bind-spec", "Attribute in bind spec must be keyword"),
        ("(from :docs :all-time)", "xtql/malformed-table-opts", "Malformed table opts"),
        ("(from :docs {:bind nil})", "xtql/missing-bind", "Missing bind"),
        ("(unify (with {:age 1}))", "xtql/malformed-var-spec", "Attribute in var spec must be symbol"),
        ("(unify (with (+ 1 1)))", "xtql/malformed-var-spec", "Var specs must be pairs of bindings"),
        ("(unify (join (not-a-query) {:x x}))", "xtql/malformed-bind", "Malformed bind"),
        ("(-> (rel [{}] []) (with {age 1}))", "xtql/malformed-col-spec", "Attribute in col spec must be keyword"),
        ("(-> (rel [{}] []) (with (+ 1 1)))", "xtql/malformed-col-spec", "Short form of col spec must be a symbol"),
        ("(-> (rel [{}] []) (without age))", "xtql/malformed-without", "Columns must be keywords in without"),
        ("(-> (rel [{}] []) (limit 1 2))", "xtql/limit", "Limit can only take a single value"),
        ("(-> (rel [{}] []) (offset))", "xtql/offset", "Offset can only take a single value"),
    ] {
        let error = parse_query(&read(source).unwrap()).unwrap_err();
        assert_eq!(error.code, code, "{source}");
        assert_eq!(error.message.as_deref(), Some(message), "{source}");
    }
}

#[test]
fn from_nil_options_and_join_nil_use_their_omitted_defaults() {
    let relation = plan("(from {:table :docs :db nil} {:bind [age] :for-valid-time nil :for-system-time nil})");
    let scan = find(&relation, &|node| matches!(node, RelationNode::Scan(_))).unwrap();
    let RelationNode::Scan(scan) = scan.node() else { panic!("Expected scan") };
    assert_eq!(scan.table.name.database, "xtdb");
    assert!(matches!(&scan.valid_time, TemporalFilter::Default));
    assert!(matches!(&scan.system_time, TemporalFilter::Default));

    let join = plan("(unify (with {x 1}) (join (rel [{}] []) nil))");
    assert_eq!(join.schema().fields()[0].name, "x");
    assert_eq!(join.schema().len(), 1);
}

#[test]
fn expression_map_keys_use_the_reference_string_suffix() {
    let query = parse_query(&read(r#"(rel [{:doc {"Xname" age foo (+ age 1) :name 2}}] [doc])"#).unwrap()).unwrap();
    let Query::Rel { input: rtdb_xtql::RelationInput::Documents(documents), .. } = query else { panic!("Expected relation") };
    let Expr::Map(fields) = &documents[0][0].1 else { panic!("Expected expression map") };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0], ("name".to_owned(), Expr::Literal(Value::I64(2))));
    assert!(matches!(&fields[1], (name, Expr::Call { arguments, .. })
        if name == "oo" && arguments[0] == Expr::Variable("age".into())));
}

#[test]
fn xtdb_xtql_plan_test_from_normalizes_aliases_and_preserves_duplicate_projections() {
    let relation = plan("(from :docs [{:xt/id e} {:first-name name} {:first-name also-name}])");
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["e", "name", "also_name"]);
    assert_ne!(relation.schema().fields()[1].id, relation.schema().fields()[2].id);
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
fn embedded_compilation_uses_callers_column_and_apply_parameter_allocators() {
    let mut binder = rtdb_plan::Binder::default();
    let before = binder.field("before",None,LogicalType::Scalar(ScalarType::I64)).unwrap();
    let mut next_parameter = 100;
    let query = rtdb_xtql::parse_query(&rtdb_xtql::read("(fn [wanted] (-> (rel [{:a wanted}] [a]) (with {:result (q [(fn [inner] (rel [{:result inner}] [result])) a])})))").unwrap()).unwrap();
    let relation = rtdb_xtql::compile_query(&query,&FixtureCatalog,&mut binder,&mut next_parameter,&[(7,LogicalType::Scalar(ScalarType::I64))],"UTC").unwrap();
    assert_eq!(relation.parameter_count(),8);
    assert_eq!(next_parameter,101);
    assert!(relation.schema().fields().iter().all(|field| field.id.0>before.id.0));
    let after = binder.field("after",None,LogicalType::Scalar(ScalarType::I64)).unwrap();
    assert!(relation.schema().fields().iter().all(|field| field.id.0<after.id.0));
    assert_eq!(binder.parameter_types().keys().copied().collect::<Vec<_>>(),[7]);
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
fn xtdb_xtql_plan_test_rel_accepts_an_anonymous_relation_parameter() {
    let integer = LogicalType::Scalar(ScalarType::I64);
    let documents = LogicalType::list(LogicalType::Struct(StructFields::from([("age".into(), integer.clone())])));
    let options = CompileOptions { parameter_types: vec![documents.clone()], ..Default::default() };
    for source in ["#(rel % [age])", "#(rel %01 [age])"] {
        let prepared = compile(source, &FixtureCatalog, &options).unwrap();
        let Statement::Query(relation) = prepared.statement else { panic!("Expected a query") };
        assert_eq!(prepared.parameter_types.get(&0), Some(&documents));
        assert_eq!(relation.schema().fields()[0].data_type, integer);
        assert!(find(&relation, &|node| matches!(node, RelationNode::Unnest { expr: PlanExpr::Parameter { index: 0, .. }, .. })).is_some());
    }
}

#[test]
fn function_parameter_symbols_resolve_independently_of_column_normalization() {
    let options = CompileOptions {
        parameter_types: vec![LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8)],
        ..Default::default()
    };
    let source = "(fn [x-y x_y] (-> (rel [{}] []) (return {:a x-y :b x_y})))";
    let Statement::Query(relation) = compile(source, &FixtureCatalog, &options).unwrap().statement else { panic!("Expected a query") };
    let RelationNode::Project { expressions, .. } = relation.node() else { panic!("Expected projection") };
    assert!(matches!(&expressions[0].expr, PlanExpr::Parameter { index: 0, .. }));
    assert!(matches!(&expressions[1].expr, PlanExpr::Parameter { index: 1, .. }));

    let single_parameter = CompileOptions { parameter_types: options.parameter_types[..1].to_vec(), ..Default::default() };
    let source = "(fn [Age] (-> (rel [{:age 23}] [age]) (return {:column age :parameter Age})))";
    let Statement::Query(relation) = compile(source, &FixtureCatalog, &single_parameter).unwrap().statement else { panic!("Expected a query") };
    let RelationNode::Project { expressions, .. } = relation.node() else { panic!("Expected projection") };
    assert!(matches!(&expressions[0].expr, PlanExpr::Column { .. }));
    assert!(matches!(&expressions[1].expr, PlanExpr::Parameter { index: 0, .. }));
}

#[test]
fn inline_query_functions_preserve_explicit_parameter_positions() {
    let form = read("(fn [x-y x_y] (-> (rel [{}] []) (return {:a x-y :b x_y})))").unwrap();
    let query = parse_query(&form).unwrap();
    let integer = LogicalType::Scalar(ScalarType::I64);
    let mut next_parameter = 8;
    let relation = compile_query(&query, &FixtureCatalog, &mut Binder::default(), &mut next_parameter, &[(7, integer.clone()), (4, integer)], "UTC").unwrap();
    let RelationNode::Project { expressions, .. } = relation.node() else { panic!("Expected projection") };
    assert!(matches!(&expressions[0].expr, PlanExpr::Parameter { index: 7, .. }));
    assert!(matches!(&expressions[1].expr, PlanExpr::Parameter { index: 4, .. }));
}

#[test]
fn scan_bound_subqueries_report_the_context_before_binding_their_query() {
    for source in [
        "(from :docs {:bind [age] :for-valid-time (at (q (rel [{}] [])))})",
        "(from :docs {:bind [age] :for-system-time (to (if true (q (rel [{}] [])) nil))})",
    ] {
        let error = compile(source, &FixtureCatalog, &CompileOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtql/invalid-expression");
        assert!(error.message.as_deref().unwrap().contains("Temporal scan filters cannot contain relational subqueries"));
    }
    let relation = plan("(fn [bound] (from :docs {:bind [age] :for-valid-time (at bound)}))");
    let scan = find(&relation, &|node| matches!(node, RelationNode::Scan(_))).unwrap();
    let RelationNode::Scan(scan) = scan.node() else { panic!("Expected scan") };
    assert!(matches!(&scan.valid_time, TemporalFilter::At(PlanExpr::Parameter { index: 0, .. })));
}

#[test]
fn ordered_set_percentiles_point_to_sql_even_with_null_arguments() {
    for function in ["percentile-cont", "percentile_disc"] {
        let source = format!("(-> (rel [{{}}] []) (aggregate {{:v ({function} nil)}}))");
        let error = compile(&source, &FixtureCatalog, &CompileOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtql/invalid-expression");
        assert!(error.message.as_deref().unwrap().contains("WITHIN GROUP"));
    }
}

#[test]
fn temporal_period_projection_keeps_hidden_endpoints_out_of_the_result() {
    let relation = plan("(from :docs [{:xt/valid-time period}])");
    assert_eq!(relation.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["period"]);
    assert!(find(&relation, &|node| matches!(node, RelationNode::Project { expressions, .. } if expressions.iter().any(|value| matches!(&value.expr, PlanExpr::Call { name, .. } if name == "period")))).is_some());
}

#[test]
fn temporal_scan_bounds_admit_null_upper_endpoints() {
    let source = "(from :docs {:bind [{:xt/valid-from vf, :xt/valid-to vt, :xt/system-from sf, :xt/system-to st}]
                               :for-valid-time :all-time :for-system-time :all-time})";
    let result = plan(source);
    let timestamp = LogicalType::Scalar(ScalarType::Timestamp {
        unit: rtdb_types::TimeUnit::Microsecond,
        timezone: Some("UTC".into()),
    });
    let fields: BTreeMap<_, _> = result.schema().fields().iter().map(|field| (field.name.as_str(), &field.data_type)).collect();
    assert_eq!(fields["vf"], &timestamp);
    assert_eq!(fields["sf"], &timestamp);
    let nullable = timestamp.maybe();
    assert_eq!(fields["vt"], &nullable);
    assert_eq!(fields["st"], &nullable);
}

#[test]
fn xtdb_api_test_explain_plan_2383_from_projects_columns_in_binding_order() {
    let prepared = compile("(fn [id] (from :people [{:xt/id id} last_name age xt/valid-from first_name]))", &FixtureCatalog,
        &CompileOptions { parameter_types: vec![LogicalType::Scalar(ScalarType::Keyword)], ..Default::default() }).unwrap();
    let Statement::Query(relation) = prepared.statement else { panic!("Expected query") };
    let names: Vec<_> = relation.schema().fields().iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["last_name", "age", "_valid_from", "first_name"]);
}
