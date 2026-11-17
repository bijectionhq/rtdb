use std::collections::{BTreeMap, BTreeSet};

use rtdb_plan::*;
use rtdb_sql::{PrepareOptions, parse, prepare};
use rtdb_types::{LogicalType, Result, ScalarType, TableRef, Value};

struct Fixture;

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

impl Catalog for Fixture {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into(), "archive".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok((name.table.schema == "public").then(|| TableBinding { name: name.clone(), oid: Some(if name.database == "xtdb" { 16384 } else { 20000 }), columns: BTreeMap::from([
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
fn minmax_rejects_incomparable_and_unsupported_argument_types_340() {
    for argument in [
        LogicalType::join_all([&LogicalType::Scalar(ScalarType::I64), &LogicalType::Scalar(ScalarType::Utf8)]),
        LogicalType::Scalar(ScalarType::Bool),
        LogicalType::list(LogicalType::Scalar(ScalarType::I64)),
    ] {
        let options = PrepareOptions { parameter_types: BTreeMap::from([(0, argument)]), ..Default::default() };
        assert!(prepare("SELECT min($1) FROM docs", &Fixture, &options).is_err());
    }
}

#[test]
fn static_insert_values_keep_declared_column_renames() {
    let plan = rtdb_sql::prepare_static("INSERT INTO foo (_id, v) VALUES (1, 7)", &Fixture, &PrepareOptions::default()).unwrap().unwrap();
    assert!(matches!(plan.input.node(), RelationNode::Rename { .. }));
    assert_eq!(plan.input.schema().fields().iter().map(|field|field.name.as_str()).collect::<Vec<_>>(), ["_id", "v"]);
    assert!(rtdb_sql::prepare_static("INSERT INTO foo (_id, v) SELECT _id, v FROM foo", &Fixture, &PrepareOptions::default()).unwrap().is_none());
}
