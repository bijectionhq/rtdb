use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};

use rtdb_plan::*;
use rtdb_sql::{PrepareOptions, parse, prepare};
use rtdb_types::{LogicalType, Result, ScalarType, TableRef, Value};

struct Fixture;

#[test]
fn period_diagnostics_use_the_cte_name_even_when_referenced_through_an_alias() {
    for with in ["WITH", "WITH MATERIALIZED"] {
        for period in ["SYSTEM_TIME", "VALID_TIME"] {
            let sql = format!("{with} q AS (SELECT 1 AS v) SELECT other.v FROM q FOR {period} ALL AS other");
            let error = prepare(&sql, &Fixture, &PrepareOptions::default()).unwrap_err();
            assert_eq!(error.code, "xtdb/sql-error", "{sql}");
            assert_eq!(error.message.as_deref(), Some("Errors planning SQL statement:\n  - Period specifications not allowed on CTE reference: q"), "{sql}");
            assert_eq!(error.context["errors"][0]["table-name"], rtdb_types::transit_value::to_json_value(
                &rtdb_types::transit_value::TransitValue::Symbol("q".into())
            ).unwrap(), "{sql}");
        }
    }
}

#[test]
fn table_periods_have_no_local_or_outer_column_scope() {
    for (source, column) in [
        ("SELECT * FROM absent.docs FOR SYSTEM_TIME AS OF foo", "foo"),
        ("SELECT * FROM foo FOR VALID_TIME BETWEEN source.region AND other", "region"),
        ("SELECT * FROM (VALUES (CURRENT_TIMESTAMP)) q(at_time), LATERAL (SELECT * FROM foo FOR SYSTEM_TIME AS OF q.at_time) t", "at_time"),
        ("SELECT * FROM foo FOR VALID_TIME AS OF CASE WHEN 1 IN (2, bad) THEN CURRENT_TIMESTAMP ELSE CURRENT_TIMESTAMP END", "bad"),
    ] {
        let error = prepare(source, &Fixture, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb.sql/no-column-refs-allowed", "{source}");
        assert_eq!(error.message.as_deref(), Some(format!("No column reference allowed in table period specification: {column}").as_str()), "{source}");
    }
    assert!(prepare("SELECT * FROM foo FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP", &Fixture, &PrepareOptions::default()).is_ok());
}

#[test]
fn insert_values_without_a_projection_reports_both_planning_diagnostics() {
    let sql = "INSERT INTO foo VALUES (1, unresolved_function())";
    let error = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert_eq!(error.message.as_deref(), Some("Errors planning SQL statement:\n  - INSERT with VALUES needs column name list\n  - INSERT does not contain mandatory _id column"));
    assert_eq!(error.context["errors"], serde_json::json!([{}, {}]));
    assert!(rtdb_sql::prepare_static(sql, &Fixture, &PrepareOptions::default()).unwrap().is_none());
    let select = prepare("INSERT INTO foo SELECT 1 AS v", &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(select.context["errors"], serde_json::json!([{}]));
    assert_eq!(select.message.as_deref(), Some("Errors planning SQL statement:\n  - INSERT does not contain mandatory _id column"));
}

#[test]
fn invalid_values_projection_remains_available_for_resolution() {
    for source in [
        "INSERT INTO foo (_id, v) VALUES (1)",
        "INSERT INTO foo (_id, v) VALUES (unbound_name)",
        "INSERT INTO foo (_id, v) VALUES (1, 2), (3)",
    ] {
        assert!(rtdb_sql::prepare_static(source, &Fixture, &PrepareOptions::default()).unwrap().is_none());
        let error = prepare(source, &Fixture, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.context["errors"], serde_json::json!([{"expected": 2, "given": 1}]), "{source}");
    }
    let source = "INSERT INTO foo (_id, v) VALUES (1), (2,3,4), (5,6)";
    let error = prepare(source, &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.context["errors"], serde_json::json!([
        {"expected": 2, "given": 1},
        {"expected": 2, "given": 3},
    ]));
}

#[test]
fn table_projection_failures_report_dependent_columns_in_order() {
    let sql = "INSERT INTO foo (_id) SELECT _id FROM UNNEST([1,2,3]) x (_id, foo)";
    let error = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert_eq!(error.message.as_deref(), Some(
        "Errors planning SQL statement:\n  - Table projection mismatch for x (_id,foo)\n  - Column not found: _id",
    ));
    assert_eq!(error.context["errors"], serde_json::json!([
        {"table-alias": "x", "projections": ["_id", "foo"]},
        {"chain": ["_id"]},
    ]));

    let qualified = prepare("SELECT x._id FROM UNNEST([1,2,3]) x (_id, foo)", &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(qualified.context["errors"][1]["chain"], serde_json::json!(["_id", "x"]));

    let sql = "INSERT INTO foo (_id) FROM generate_series (1,4) xs (x, foo)";
    let error = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.message.as_deref(), Some(concat!(
        "Errors planning SQL statement:\n  - Table projection mismatch for xs (x,foo)",
        "\n  - Column count mismatch: expected 1, given 0",
        "\n  - INSERT does not contain mandatory _id column",
    )));
    assert_eq!(error.context["errors"], serde_json::json!([
        {"table-alias": "xs", "projections": ["x", "foo"]},
        {"expected": 1, "given": 0},
        {},
    ]));
}

#[test]
fn invalid_interval_default_reports_its_source_occurrence_once() {
    let error = prepare("SETTING DEFAULT VALID_TIME AS OF INTERVAL 'bad' SELECT * FROM foo",&Fixture,&PrepareOptions::default()).unwrap_err();
    assert_eq!(error.message.as_deref(),Some("Errors planning SQL statement:\n  - Cannot parse interval: bad - failed with message null"));
    assert_eq!(serde_json::to_value(&error.context).unwrap(),serde_json::json!({"errors":[{"i-str":"bad","msg":null}]}));
}

#[test]
fn update_interval_diagnostics_admit_assignments_before_the_predicate() {
    let error = prepare("UPDATE foo SET v=INTERVAL 'set-bad' WHERE INTERVAL 'where-bad' IS NULL",&Fixture,&PrepareOptions::default()).unwrap_err();
    assert_eq!(error.message.as_deref(),Some("Errors planning SQL statement:\n  - Cannot parse interval: set-bad - failed with message null\n  - Cannot parse interval: where-bad - failed with message null"));
    assert_eq!(serde_json::to_value(&error.context).unwrap(),serde_json::json!({"errors":[{"i-str":"set-bad","msg":null},{"i-str":"where-bad","msg":null}]}));
}

#[test]
fn table_interval_diagnostics_admit_valid_time_before_system_time() {
    let error = prepare("SELECT 1 FROM foo FOR SYSTEM_TIME AS OF INTERVAL 'system-bad' FOR VALID_TIME AS OF INTERVAL 'valid-bad'",&Fixture,&PrepareOptions::default()).unwrap_err();
    assert_eq!(error.message.as_deref(),Some("Errors planning SQL statement:\n  - Cannot parse interval: valid-bad - failed with message null\n  - Cannot parse interval: system-bad - failed with message null"));
    assert_eq!(serde_json::to_value(&error.context).unwrap(),serde_json::json!({"errors":[{"i-str":"valid-bad","msg":null},{"i-str":"system-bad","msg":null}]}));
}

#[test]
fn parsed_parameter_count_precedes_dml_table_binding() {
    let parsed = parse("UPDATE absent SET value = $3 WHERE _id = ?").unwrap();
    assert_eq!(parsed[0].kind(), rtdb_sql::StatementKind::Dml);
    assert_eq!(parsed[0].parameter_count(), 3);
    assert_eq!(parse("SELECT '?' AS literal, $2").unwrap()[0].parameter_count(), 2);
}

#[test]
fn temporal_projection_resolves_hidden_columns_without_expanding_wildcards() {
    assert_eq!(query("SELECT * FROM foo").schema().len(), 4);
    assert_eq!(query("SELECT _valid_from, _valid_to, _system_from, _system_to FROM foo FOR ALL SYSTEM_TIME").schema().len(), 4);
    assert_eq!(query("SELECT f._iid, f._valid_from, (SELECT f._system_from) AS captured FROM foo AS f(a,b,c,d)").schema().len(), 3);
    assert_eq!(query("SELECT f.* FROM foo AS f(a,b,c,d) WHERE f._valid_from<=CURRENT_TIMESTAMP").schema().len(), 4);
    assert_eq!(query("SELECT f.* FROM foo AS f(a, b, c, d)").schema().len(), 4);
    assert_eq!(query("SELECT * FROM UNNEST($1) AS items(value)").schema().len(), 1);
}

#[test]
fn sql_to_static_ops_non_dml_5856_and_dynamic_expressions_remain_unexpanded() {
    for sql in ["SELECT 1", "CREATE TABLE foo (_id, v)", "GRANT reader TO alice", "INSERT INTO foo (_id, v) VALUES (1, 1 + 2)",
        "INSERT INTO foo (_id, v) VALUES (1, '9'::DECIMAL(1,0))", "INSERT INTO foo (_id, v) VALUES (1, ?::DECIMAL)",
        "INSERT INTO foo (_id, v) VALUES (1, ARRAY[?]::DECIMAL(1,0)[])"] {
        assert!(rtdb_sql::prepare_static(sql, &Fixture, &PrepareOptions::default()).unwrap().is_none());
    }
    assert!(rtdb_sql::prepare_static("INSERT INTO foo (_id, v) VALUES (1, 12.34)", &Fixture, &PrepareOptions::default()).unwrap().is_some());
    assert!(rtdb_sql::prepare_static("INSERT INTO foo (", &Fixture, &PrepareOptions::default()).is_err());
}

#[test]
fn patch_records_null_valid_time_4448_binds_gap_operator_and_merge() {
    let mutation = rtdb_sql::prepare_tx("PATCH INTO foo FOR PORTION OF VALID_TIME FROM NULL TO NULL RECORDS {_id: 1, price: 42}", &Fixture, &PrepareOptions::default()).unwrap();
    let rtdb_sql::Mutation::Put { input, .. } = mutation else { panic!("Expected put plan") };
    fn contains_gap(input: &Relation) -> bool { matches!(input.node(), RelationNode::PatchGaps { .. }) || input.children().iter().any(|child| contains_gap(child)) }
    assert!(contains_gap(&input));
    assert_eq!(input.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["_iid", "_valid_from", "_valid_to", "doc"]);
    let mut has_patch = false;
    for expr in input.expressions() { expr.visit(&mut |expr| { if matches!(expr, Expr::Call { name, .. } if name == "_patch") { has_patch = true; } }); }
    assert!(has_patch);
}

#[test]
fn create_or_alter_table_and_role_membership_are_transaction_operations() {
    for sql in ["CREATE TABLE empty_table", "CREATE OR ALTER TABLE empty_table (_id, v)", "GRANT reader TO alice", "REVOKE reader FROM alice"] {
        assert!(matches!(prepare(sql, &Fixture, &PrepareOptions::default()).unwrap().statement, Statement::Dml(_)));
    }
}

#[test]
fn typed_declarations_bind_native_columns_and_allow_untyped_neighbors() {
    let sql = r#"CREATE TABLE docs (_id TEXT, "ExactName" BIGINT, effective TIMESTAMPTZ, tags TEXT[], dynamic)"#;
    let statement = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap().statement;
    let Statement::Dml(rtdb_plan::Dml::CreateTable { columns, .. }) = statement else {
        panic!("Expected a table declaration");
    };
    use rtdb_types::TimeUnit;
    assert_eq!(columns, vec![
        ("_id".into(), LogicalType::Scalar(ScalarType::Utf8).maybe()),
        ("ExactName".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("effective".into(), LogicalType::Scalar(ScalarType::Timestamp { timezone: Some("UTC".into()), unit: TimeUnit::Microsecond }).maybe()),
        ("tags".into(), LogicalType::list(LogicalType::Scalar(ScalarType::Utf8)).maybe()),
        ("dynamic".into(), LogicalType::Nothing),
    ]);
    for sql in [
        "CREATE TABLE docs (v BIGINT, V TEXT)",
        "CREATE TABLE docs (_valid_from TIMESTAMPTZ)",
        "CREATE TABLE docs (v TEXT NOT NULL)",
        "CREATE TABLE docs (v imaginary_type)",
    ] {
        assert!(prepare(sql, &Fixture, &PrepareOptions::default()).is_err(), "{sql}");
    }
}

impl Catalog for Fixture {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into(), "archive".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok((name.table.schema == "public").then(|| TableBinding { name: name.clone(), oid: Some(if name.database == "xtdb" { 16384 } else { 20000 }), columns: StructFields::from([
            ("_id".into(), LogicalType::Scalar(ScalarType::I64)),
            ("region".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("price".into(), LogicalType::Scalar(ScalarType::I64)),
            ("ExactName".into(), LogicalType::Scalar(ScalarType::Bool)),
        ]) }))
    }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

fn query(sql: &str) -> std::sync::Arc<Relation> {
    let Statement::Query(query) = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    query
}

fn parameters(relation: &Relation) -> Vec<usize> {
    let mut parameters = Vec::new();
    for expression in relation.expressions() { expression.visit(&mut |expr| { if let Expr::Parameter { index, .. } = expr { parameters.push(*index); } }); }
    for child in relation.children() { parameters.extend(self::parameters(child)); }
    parameters
}

fn find_scan(relation: &Relation) -> &Scan {
    if let RelationNode::Scan(scan) = relation.node() { return scan; }
    find_scan(relation.children()[0])
}

#[test]
fn anonymous_params_bound_in_lexical_order_5988() {
    let projected = query("SELECT ? AS p1, ? AS p2, region FROM docs WHERE region = ?");
    assert_eq!(parameters(&projected), [0, 1, 2]);
    let grouped = query("SELECT ? AS p1, ? AS p2 FROM docs GROUP BY region HAVING region > ?");
    assert_eq!(parameters(&grouped), [0, 1, 2]);
    let nested = query("SELECT ? AS p1, s.x FROM (SELECT ? AS x FROM docs) s");
    assert_eq!(parameters(&nested), [0, 1]);
    let expression = query("SELECT ? || ? AS p1 FROM docs WHERE region = ?");
    assert_eq!(parameters(&expression), [0, 1, 2]);
}

#[test]
fn from_where_param_order_bug_4305() {
    let relation = query("SELECT _id FROM foo FOR VALID_TIME AS OF ? WHERE _id = ?");
    assert!(matches!(find_scan(&relation).valid_time, TemporalFilter::At(Expr::Parameter { index: 0, .. })));
    assert_eq!(parameters(&relation), [1, 0]);
}

#[test]
fn dynamic_temporal_filters_3068_bind_each_dimension() {
    for (sql, expected) in [
        ("SELECT price FROM foo FOR VALID_TIME FROM ? TO ?", "from-to"),
        ("SELECT price FROM foo FOR VALID_TIME BETWEEN ? AND ?", "between"),
    ] {
        let relation = query(sql);
        let filter = &find_scan(&relation).valid_time;
        match (expected, filter) {
            ("from-to", TemporalFilter::FromTo { from: Expr::Parameter { index: 0, .. }, to: Expr::Parameter { index: 1, .. } }) => {}
            ("between", TemporalFilter::Between { from: Expr::Parameter { index: 0, .. }, to: Expr::Parameter { index: 1, .. } }) => {}
            _ => panic!("Temporal parameter identity changed"),
        }
    }
    let relation = query("SELECT price FROM foo FOR ALL SYSTEM_TIME FOR VALID_TIME ONLY FROM ? TO ?");
    let scan = find_scan(&relation);
    assert_eq!(scan.system_time, TemporalFilter::All);
    assert!(scan.clamp_valid_time);
}

#[test]
fn insert_plan_keeps_column_names_and_lexical_parameters() {
    let prepared = prepare("INSERT INTO users (_id, region, price) VALUES (?, ?, ?)", &Fixture, &PrepareOptions::default()).unwrap();
    let Statement::Dml(Dml::Insert { table, input }) = prepared.statement else { panic!("Expected insert") };
    assert_eq!(table.table, TableRef::new("public", "users"));
    assert_eq!(input.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["_id", "region", "price"]);
    assert_eq!(parameters(&input), [0, 1, 2]);
}

#[test]
fn temporal_dml_targets_keep_aliases_and_extent() {
    let prepared = prepare("UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' AS u SET region = 'eu' WHERE u._id = ?", &Fixture, &PrepareOptions::default()).unwrap();
    let Statement::Dml(Dml::Update { input, valid_time, assignments, .. }) = prepared.statement else { panic!("Expected update") };
    assert!(matches!(valid_time, TemporalFilter::FromTo { .. }));
    assert_eq!(assignments[0], ("region".into(), Expr::Literal(Value::Utf8("eu".into()))));
    assert_eq!(parameters(&input), [0]);
    assert!(matches!(prepare("ERASE FROM users AS u WHERE u._id = ?", &Fixture, &PrepareOptions::default()).unwrap().statement, Statement::Dml(Dml::Erase { .. })));
}

#[test]
fn update_subqueries_preserve_target_fields_without_persisting_apply_helpers() {
    let mutation = rtdb_sql::prepare_tx("UPDATE foo AS f SET price = (SELECT f.price + 1) WHERE EXISTS (SELECT 1 WHERE f._id = 1)", &Fixture, &PrepareOptions::default()).unwrap();
    let rtdb_sql::Mutation::Put { input, .. } = mutation else { panic!("expected put mutation") };
    let RelationNode::Project { expressions, .. } = input.node() else { panic!("expected canonical projection") };
    let Expr::RowCopy(fields) = &expressions.iter().find(|expression| expression.field.name == "doc").unwrap().expr else { panic!("expected document") };
    assert_eq!(fields.iter().map(|field| field.name.as_str()).collect::<BTreeSet<_>>(), BTreeSet::from(["_id", "ExactName", "price", "region"]));
    assert!(rtdb_sql::prepare_static("INSERT INTO foo (_id, price) VALUES (1, (SELECT 2))", &Fixture, &PrepareOptions::default()).unwrap().is_none());
}

#[test]
fn update_evaluates_each_set_expression_once_per_row() {
    fn count_calls(relation: &Relation, target: &str) -> usize {
        let mut count = 0;
        for expr in relation.expressions() {
            expr.visit(&mut |expr| {
                if matches!(expr, Expr::Call { name, .. } if name == target) {
                    count += 1;
                }
            });
        }
        count + relation.children().into_iter().map(|child| count_calls(child, target)).sum::<usize>()
    }
    let mutation = rtdb_sql::prepare_tx("UPDATE foo SET price = RANDOM() WHERE _id = 1", &Fixture, &PrepareOptions::default()).unwrap();
    let rtdb_sql::Mutation::Put { input, .. } = mutation else { panic!("expected put mutation") };
    assert_eq!(count_calls(&input, "random"), 1, "SET RHS must be evaluated once before change detection and projection");
}

#[test]
fn database_oid_and_quoted_identifier_are_bound_from_catalog() {
    let relation = query("SELECT \"ExactName\" FROM archive.public.docs");
    assert_eq!(find_scan(&relation).table.name.database, "archive");
    assert_eq!(find_scan(&relation).table.oid, Some(20000));
    assert!(prepare("SELECT exactname FROM docs", &Fixture, &PrepareOptions::default()).is_err());
}

#[test]
fn comments_and_string_semicolons_preserve_statement_spans() {
    let source = "/* one /* nested */ */ SELECT ';' AS x;\nSELECT $2 AS y";
    let parsed = parse(source).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(&source[parsed[0].span.start..parsed[0].span.start + 6], "SELECT");
    let prepared = prepare("SELECT $2 AS y, ? AS x", &Fixture, &PrepareOptions::default()).unwrap();
    assert_eq!(prepared.parameter_types.keys().copied().collect::<Vec<_>>(), [0, 1]);
    let error = prepare("SELECT missing FROM docs", &Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error.context.get("position"), Some(&serde_json::json!(8)));
}

#[test]
fn aggregate_projection_has_group_then_aggregate_input() {
    let relation = query("SELECT region, sum(price) AS total FROM docs GROUP BY region HAVING count(*) > 0 ORDER BY total DESC NULLS LAST");
    let mut ordered = relation.as_ref();
    while let RelationNode::Project { input, .. } | RelationNode::Rename { input, .. } = ordered.node() { ordered = input; }
    let RelationNode::Sort { input, order_by } = ordered.node() else { panic!("Expected sort") };
    assert!(order_by[0].descending);
    assert!(!order_by[0].nulls_first);
    assert_eq!(relation.schema().fields()[1].name, "total");
    let mut grouped = input.as_ref();
    while !matches!(grouped.node(), RelationNode::Aggregate { .. }) {
        let children = grouped.children();
        assert_eq!(children.len(), 1);
        grouped = children[0];
    }
    let RelationNode::Aggregate { groups, aggregates, .. } = grouped.node() else { unreachable!() };
    assert_eq!(groups.len(), 1);
    assert_eq!(aggregates.len(), 2);
}

#[test]
fn promoting_sum_and_minmax_use_the_argument_computation_type() {
    let integer = LogicalType::Scalar(ScalarType::I32);
    let mixed = LogicalType::join_all([&integer, &LogicalType::Scalar(ScalarType::F64)]);
    let duration = LogicalType::join_all([
        &LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Second)),
        &LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Nanosecond)),
    ]);
    for (sql, argument, expected) in [
        ("SELECT sum($1) FROM docs", integer.clone(), integer.clone().maybe()),
        ("SELECT sum($1) FROM docs", mixed.clone(), LogicalType::Scalar(ScalarType::F64).maybe()),
        ("SELECT min($1) FROM docs", mixed.clone(), LogicalType::Scalar(ScalarType::F64).maybe()),
        ("SELECT max($1) FROM docs", mixed, LogicalType::Scalar(ScalarType::F64).maybe()),
        ("SELECT avg($1) FROM docs", integer, LogicalType::Scalar(ScalarType::F64).maybe()),
        ("SELECT avg($1) FROM docs", duration, LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Nanosecond)).maybe()),
    ] {
        let options = PrepareOptions { parameter_types: BTreeMap::from([(0, argument)]), ..Default::default() };
        let Statement::Query(query) = prepare(sql, &Fixture, &options).unwrap().statement else { panic!("Expected query") };
        assert_eq!(query.schema().fields()[0].data_type, expected);
    }
}

#[test]
fn minmax_planning_preserves_argument_types_for_factory_emission() {
    for argument in [
        LogicalType::join_all([&LogicalType::Scalar(ScalarType::I64), &LogicalType::Scalar(ScalarType::Utf8)]),
        LogicalType::Scalar(ScalarType::Bool),
        LogicalType::list(LogicalType::Scalar(ScalarType::I64)),
    ] {
        let options = PrepareOptions { parameter_types: BTreeMap::from([(0, argument.clone())]), ..Default::default() };
        let plan = prepare("SELECT MIN($1) AS m FROM docs", &Fixture, &options).unwrap();
        let Statement::Query(plan) = plan.statement else { panic!("expected query") };
        assert_eq!(plan.schema().fields()[0].name, "m");
        assert_eq!(plan.schema().fields()[0].data_type, argument.maybe());
    }
}

#[test]
fn static_insert_values_keep_declared_column_renames() {
    let plan = rtdb_sql::prepare_static("INSERT INTO foo (_id, v) VALUES (1, 7)", &Fixture, &PrepareOptions::default()).unwrap().unwrap();
    assert!(matches!(plan.input.node(), RelationNode::Rename { .. }));
    assert_eq!(plan.input.schema().fields().iter().map(|field|field.name.as_str()).collect::<Vec<_>>(), ["_id", "v"]);
    assert!(rtdb_sql::prepare_static("INSERT INTO foo (_id, v) SELECT _id, v FROM foo", &Fixture, &PrepareOptions::default()).unwrap().is_none());
}

#[test]
fn temporal_literal_plan_errors_remain_sql_in_the_source_transaction() {
    let options = PrepareOptions::default();
    for (constructor, input, key) in [
        ("DATE", "2030- 01-01", "d-str"),
        ("TIME", "bad", "t-str"),
        ("TIMESTAMP", "bad", "ts-str"),
        ("INTERVAL", "bad", "i-str"),
    ] {
        let source = format!("INSERT INTO foo (_id, v) VALUES (1, {constructor} '{input}')");
        assert!(rtdb_sql::prepare_static(&source, &Fixture, &options).unwrap().is_none());
        let error = rtdb_sql::prepare_tx(&source, &Fixture, &options).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert!(error.message.as_deref().unwrap().starts_with(&format!("Errors planning SQL statement:\n  - Cannot parse {}: ", constructor.to_ascii_lowercase())));
        assert_eq!(error.context["errors"][0][key], input);
    }
}

#[test]
fn repeated_submissions_prepare_the_same_static_source_or_error_each_time() {
    let options = PrepareOptions::default();
    for sql in [
        "UPDATE foo SET v = v + 1 WHERE _id = $1 /* repeated static probe */",
        "DELETE FROM foo WHERE _id = 1 /* repeated static probe */",
        "INSERT INTO foo (_id, v) SELECT _id, v FROM foo /* repeated static probe */",
    ] {
        for _ in 0..3 {
            assert!(rtdb_sql::prepare_static(sql, &Fixture, &options).unwrap().is_none(), "{sql}");
        }
    }
    for _ in 0..3 {
        assert!(rtdb_sql::prepare_static("INSERT INTO foo (_id, v) VALUES (1, 7) /* repeated static probe */", &Fixture, &options).unwrap().is_some());
    }
    for sql in [
        "UPDATE foo SET /* repeated static probe */",
        "UPDATE foo SET v = 1 WHERE _id = 1; UPDATE foo SET v = 2 WHERE _id = 2 /* repeated static probe */",
    ] {
        let first = rtdb_sql::prepare_static(sql, &Fixture, &options).unwrap_err();
        for _ in 0..2 {
            let again = rtdb_sql::prepare_static(sql, &Fixture, &options).unwrap_err();
            assert_eq!((&again.code, &again.message), (&first.code, &first.message), "{sql}");
        }
    }
}
