use rtdb_types::StructFields;
use std::collections::BTreeSet;

use rtdb_plan::*;
use rtdb_sql::{PrepareOptions, parse, prepare, prepare_in};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, MemoryBudget, Result, ScalarType};

struct Fixture;

impl Catalog for Fixture {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok((name.table.schema == "public").then(|| TableBinding { name: name.clone(), oid: Some(16384), columns: StructFields::from([
            ("_id".into(), LogicalType::Scalar(ScalarType::I64)),
            ("a".into(), LogicalType::Scalar(ScalarType::I64)),
        ]) }))
    }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

fn rejection(sql: &str) -> Anomaly {
    prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_err()
}

fn parse_error(report: &str) -> Anomaly {
    Anomaly::incorrect("xtdb/sql-error", format!("Errors parsing SQL statement:\n  - {report}"))
}

#[test]
fn constructs_outside_the_reference_grammar_report_the_listener_text() {
    for (sql, report) in [
        ("SELECT DISTINCT ON (a) a FROM docs", "line 1:19 mismatched input '(' expecting {<EOF>, ';'}"),
        ("SELECT CAST(a AS BLOB) FROM docs", "line 1:17 no viable alternative at input 'CAST(aASBLOB'"),
        ("SELECT ARRAY_AGG(DISTINCT a) FROM docs", "line 1:17 no viable alternative at input 'ARRAY_AGG(DISTINCT'"),
        ("SELECT foo.ROW_NUMBER() OVER (ORDER BY a) FROM docs", "line 1:11 no viable alternative at input 'foo.ROW_NUMBER'"),
        ("EXPLAIN VERBOSE SELECT 1", "line 1:8 extraneous input 'VERBOSE' expecting {'(', ANALYZE, 'FROM', 'RECORDS', 'SELECT', 'SETTING', 'VALUES', 'WITH', 'XTQL'}"),
    ] {
        assert_eq!(rejection(sql), parse_error(report), "{sql}");
    }
    let memory = MemoryBudget::new(64 << 20);
    assert_eq!(prepare_in("SELECT TRIM(a, 'x') FROM docs", &Fixture, &PrepareOptions::default(), &memory).unwrap_err(),
        parse_error("line 1:13 no viable alternative at input 'TRIM(a,'"));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn listener_positions_count_from_the_start_of_the_batch() {
    let statements = parse("SELECT 1;\n  SELECT CAST(a AS BLOB) FROM docs").unwrap();
    let error = statements.into_iter().nth(1).unwrap().bind(&Fixture, &PrepareOptions::default()).unwrap_err();
    assert_eq!(error, parse_error("line 2:19 no viable alternative at input 'CAST(aASBLOB'"));
}

#[test]
fn constructs_outside_the_reference_grammar_are_incorrect_sql_errors() {
    for sql in [
        "SELECT a FROM docs LIMIT 1, 2",
        "SELECT a FROM docs FETCH FIRST 10 ROWS WITH TIES",
        "SELECT a FROM docs GROUP BY ROLLUP (a)",
        "SELECT a FROM docs MINUS SELECT a FROM docs",
        "SELECT TOP 1 a FROM docs",
        "SELECT * EXCEPT (a) FROM docs",
        "SELECT ABS(a) OVER (ORDER BY a) FROM docs",
        "SELECT SUM(a) WITHIN GROUP (ORDER BY a) FROM docs",
        "SELECT TRY_CAST(a AS INT) FROM docs",
        "SELECT a FROM docs AS x LEFT SEMI JOIN docs AS y ON TRUE",
        "SELECT * FROM UNNEST([1], [2]) AS t(x)",
        "INSERT INTO docs (_id) VALUES (1) ON CONFLICT DO NOTHING",
        "UPDATE docs SET a = 1 FROM docs AS d2",
        "DELETE FROM docs USING docs AS d2",
        "CREATE VIEW v AS SELECT 1",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
        assert!(error.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{sql}");
    }
}

#[test]
fn reference_visitor_rejections_stay_unsupported() {
    for (sql, code, message) in [
        ("SELECT ROW_NUMBER() OVER w FROM docs WINDOW w AS (ORDER BY a)", "xtdb/sql-unsupported", "TODO: Window names currently not supported!"),
        ("SELECT ROW_NUMBER() OVER (ORDER BY a ROWS UNBOUNDED PRECEDING) FROM docs", "xtdb/sql-unsupported", "TODO: Window frames currently not supported!"),
        ("SELECT ROW_NUMBER() OVER (ORDER BY a), ROW_NUMBER() OVER (ORDER BY _id) FROM docs", "xtdb/sql-unsupported", "TODO: only one window function supported at the moment!"),
        ("SELECT LEAD(a, 1, 0) OVER (ORDER BY a) FROM docs", "xtdb.sql/lead-lag-default-expr", "LEAD/LAG default expressions are not currently supported"),
        ("WITH RECURSIVE q AS (SELECT 1 AS x) SELECT * FROM q", "xtdb.sql/recursive-ctes", "Recursive CTEs are not supported yet"),
    ] {
        assert_eq!(rejection(sql), Anomaly::unsupported(code, message), "{sql}");
    }
    assert_eq!(rejection("SELECT NTILE(2) OVER (ORDER BY a) FROM docs").category, AnomalyCategory::Unsupported);
}

#[test]
fn standard_isolation_levels_are_accepted_and_ignored() {
    for sql in ["START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "START TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ"] {
        let prepared = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap();
        assert!(matches!(prepared.statement, Statement::Begin { .. }), "{sql}");
    }
}

#[test]
fn quoted_identifiers_after_a_date_keyword_and_out_of_range_integers_are_sql_errors() {
    let date = rejection(r#"INSERT INTO docs (_id, a) VALUES (1, DATE "2020-01-01")"#);
    assert_eq!((date.category, date.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"));
    let integer = rejection("INSERT INTO docs (_id, a) VALUES (1, 9223372036854775808)");
    assert_eq!((integer.category, integer.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"));
    assert!(integer.message.as_deref().unwrap().contains("Cannot parse integer: 9223372036854775808"), "{integer:?}");
}

#[test]
fn reserved_words_name_tables_only_when_quoted() {
    for sql in ["SELECT FROM WHERE", "SELECT * FROM where", "SELECT * FROM docs, order", "INSERT INTO order (_id) VALUES (1)", "DELETE FROM where", "UPDATE where SET a = 1"] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
        assert!(error.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{sql}");
    }
    for sql in [r#"SELECT * FROM "where""#, r#"INSERT INTO "order" (_id) VALUES (1)"#, "SELECT * FROM time", "SELECT * FROM public.docs"] {
        prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn hex_quote_literals_are_outside_the_reference_grammar() {
    for sql in ["SELECT X'00'", "INSERT INTO docs (_id, a) VALUES (1, x'DEADBEEF')", "SELECT X'0'"] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
        assert!(error.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{sql}");
    }
    prepare("SELECT X('00')", &Fixture, &PrepareOptions::default()).unwrap();
}

#[test]
fn an_integer_grouping_element_is_a_constant_expression_not_an_output_position() {
    prepare("SELECT COUNT(*) FROM docs GROUP BY 1", &Fixture, &PrepareOptions::default()).unwrap();
    let error = rejection("SELECT a, COUNT(*) FROM docs GROUP BY 1");
    assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"));
}

#[test]
fn joins_without_specification_and_global_joins_are_grammar_rejections() {
    for sql in [
        "SELECT * FROM docs JOIN docs AS d2",
        "SELECT * FROM docs INNER JOIN docs AS d2",
        "SELECT * FROM docs LEFT JOIN docs AS d2",
        "SELECT * FROM docs RIGHT JOIN docs AS d2",
        "SELECT * FROM docs FULL OUTER JOIN docs AS d2",
        "SELECT * FROM docs GLOBAL JOIN docs AS d2 ON docs._id = d2._id",
        "SELECT * FROM docs GLOBAL CROSS JOIN docs AS d2",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
        assert!(error.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{sql}");
    }
    prepare("SELECT * FROM docs CROSS JOIN docs AS d2", &Fixture, &PrepareOptions::default()).unwrap();
    prepare("SELECT * FROM docs NATURAL JOIN docs AS d2", &Fixture, &PrepareOptions::default()).unwrap();
}

#[test]
fn clickhouse_query_modifiers_cte_without_as_and_bare_from_values_are_rejected() {
    for sql in [
        "SELECT 1 FORMAT JSON",
        "SELECT a FROM docs ORDER BY a WITH FILL STEP 1",
        "SELECT a FROM docs ORDER BY a INTERPOLATE (a AS 1)",
        "SELECT ROW_NUMBER() OVER (ORDER BY a WITH FILL) FROM docs",
        "WITH c (SELECT 1 AS a) SELECT * FROM c",
        "SELECT * FROM VALUES (1, 2) AS t(x, y)",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
    }
    prepare("SELECT * FROM (VALUES (1, 2)) AS t(x, y)", &Fixture, &PrepareOptions::default()).unwrap();
}

#[test]
fn non_grammar_query_bodies_and_table_factors_are_rejected() {
    for sql in [
        "(WITH c AS (SELECT 1 AS a) SELECT * FROM c)",
        "VALUE (1, 2)",
        "RECORDS ({a: 1})",
        "RECORDS COALESCE({a: 1}, {a: 2})",
        "INSERT INTO docs RECORDS ({_id: 1})",
        "SELECT * FROM (SELECT 1 AS a) AS t TABLESAMPLE BERNOULLI (50)",
        "SELECT * FROM (docs JOIN docs AS d2 ON docs._id = d2._id) AS j",
        "SELECT * FROM docs AS d(_id INT, a INT)",
        "SELECT * FROM (SELECT 1) AS t(x INT)",
        "SELECT * FROM UNNEST([1]) AS u(x INT)",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
        assert!(error.message.as_deref().unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{sql}");
    }
    prepare("RECORDS {a: 1}", &Fixture, &PrepareOptions::default()).unwrap();
    prepare("RECORDS OBJECT(a: 1)", &Fixture, &PrepareOptions::default()).unwrap();
    prepare("INSERT INTO docs RECORDS {_id: 1}", &Fixture, &PrepareOptions::default()).unwrap();
}

#[test]
fn non_grammar_dml_ddl_and_transaction_forms_are_rejected() {
    for sql in [
        "INSERT docs (_id) VALUES (1)",
        "INSERT docs RECORDS {_id: 1}",
        "INSERT INTO TABLE docs (_id) VALUES (1)",
        "INSERT LOW_PRIORITY INTO docs (_id) VALUES (1)",
        "INSERT INTO docs (_id) PARTITION (p = 1) VALUES (1)",
        "INSERT INTO docs (_id) VALUES (1) AS new_row",
        "PATCH docs RECORDS {_id: 1}",
        "PATCH INTO docs FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' RECORDS {_id: 1}",
        "UPDATE OR IGNORE docs SET a = 1",
        "DELETE docs",
        "ERASE docs",
        "UPDATE xtdb.public.docs SET a = 1",
        "DELETE FROM xtdb.public.docs",
        "ERASE FROM xtdb.public.docs",
        "CREATE TABLE docs ()",
        "END",
        "BEGIN WORK",
        "BEGIN TRAN",
        "BEGIN DEFERRED",
        "START TRANSACTION READ ONLY, READ WRITE",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
    }
}

#[test]
fn setting_before_explain_and_setting_from_without_to_are_rejected() {
    for sql in [
        "SETTING DEFAULT VALID_TIME ALL EXPLAIN SELECT 1",
        "SETTING CLOCK_TIME = TIMESTAMP '2020-01-01T00:00:00Z' EXPLAIN ANALYZE SELECT 1",
        "SETTING DEFAULT VALID_TIME FROM TIMESTAMP '2020-01-01T00:00:00Z' SELECT * FROM docs",
        "SETTING DEFAULT SYSTEM_TIME FROM TIMESTAMP '2020-01-01T00:00:00Z' SELECT * FROM docs",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
    }
    prepare("EXPLAIN SETTING DEFAULT VALID_TIME ALL SELECT * FROM docs", &Fixture, &PrepareOptions::default()).unwrap();
}

#[test]
fn non_grammar_string_numeric_and_typed_literals_are_rejected() {
    for sql in [
        "SELECT N'hello'",
        "SELECT U&'\\0041'",
        "SELECT .5",
        "SELECT 1.",
        "SELECT 1L + 2",
        "SELECT INT '1'",
        "SELECT BOOL 'true'",
        "SELECT VARCHAR 'a'",
        "SELECT TIMESTAMP(3) '2020-01-01T00:00:00'",
        "SELECT TIME(3) '12:00:00'",
        "SELECT TIME WITH TIME ZONE '12:00:00Z'",
    ] {
        let error = rejection(sql);
        assert_eq!((error.category, error.code.as_str()), (AnomalyCategory::Incorrect, "xtdb/sql-error"), "{sql}");
    }
    for sql in [
        "SELECT DATE '2020-01-01'",
        "SELECT TIME '12:00:00'",
        "SELECT TIMESTAMP WITH TIME ZONE '2020-01-01T00:00:00Z'",
        "SELECT UUID '00000000-0000-0000-0000-000000000000'",
        "SELECT DURATION 'PT1S'",
        "SELECT KEYWORD 'foo'",
        "SELECT URI 'https://example.com'",
    ] {
        prepare(sql, &Fixture, &PrepareOptions::default()).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
