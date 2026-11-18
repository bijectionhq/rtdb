use std::collections::BTreeSet;

use rtdb_plan::{Catalog, QualifiedTable, TableBinding};
use rtdb_sql::{PrepareOptions, parse, parse_control, prepare, split_statements};
use rtdb_types::Result;

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

#[test]
fn select_column_labels_follow_expression_provenance_and_sublist_positions() {
    for (source, labels) in [
        (
            "SELECT ARRAY_AGG(v), COUNT(*), SUM(v), SUM(v) FROM (VALUES (1),(2)) t(v)",
            vec!["_column_1", "_column_2", "_column_3", "_column_4"],
        ),
        (
            "SELECT ABS(v), ABS(v), ((v)), +v AS identity, (-v) FROM (VALUES (-2)) t(v)",
            vec!["_column_1", "_column_2", "v", "identity", "_column_5"],
        ),
        ("SELECT +((v)) FROM (VALUES (1)) t(v)", vec!["v"]),
        (
            r#"SELECT (v).nested, ((v).nested)."Mixed Case", v FROM (VALUES ({nested: {"Mixed Case": 4}})) AS t(v)"#,
            vec!["nested", "Mixed Case", "v"],
        ),
        (
            "SELECT p.*, ABS(v), ABS(v) FROM (VALUES (1)) t(v), (VALUES (7,8)) p(x,y)",
            vec!["x", "y", "_column_2", "_column_3"],
        ),
        (
            "SELECT *, ABS(v), ABS(v) FROM (VALUES (1)) t(v)",
            vec!["v", "_column_1", "_column_2"],
        ),
        ("SELECT q._column_1 FROM (SELECT ABS(1)) q", vec!["_column_1"]),
        ("SELECT xt.VERSION()", vec!["version"]),
        ("SELECT pg_catalog.VERSION()", vec!["version"]),
        (
            "SELECT version(), snapshot_token(), current_database(), current_user, current_schema, CURRENT_TIMESTAMP",
            vec!["version", "_column_2", "_column_3", "_column_4", "_column_5", "_column_6"],
        ),
        ("SELECT SNAPSHOT_TOKEN", vec!["_column_1"]),
        (
            "SELECT COUNT(*), SNAPSHOT_TOKEN FROM (VALUES (1)) t(v) GROUP BY SNAPSHOT_TOKEN",
            vec!["_column_1", "_column_2"],
        ),
        (
            "SELECT COUNT(*), pg_catalog.CURRENT_USER FROM (VALUES (1)) t(v) GROUP BY pg_catalog.CURRENT_USER",
            vec!["_column_1", "_column_2"],
        ),
        (
            "SELECT COUNT(*), USER FROM (VALUES (1)) t(v) GROUP BY USER",
            vec!["_column_1", "_column_2"],
        ),
        (
            "SELECT COUNT(*), USER FROM (VALUES ('alice')) t(user) GROUP BY USER",
            vec!["_column_1", "user"],
        ),
    ] {
        let prepared = prepare(source, &EmptyCatalog, &PrepareOptions::default()).unwrap();
        let rtdb_plan::Statement::Query(relation) = prepared.statement else { panic!("Expected a query") };
        let actual: Vec<_> = relation.schema().fields().iter().map(|field| field.name.as_str()).collect();
        assert_eq!(actual, labels, "{source}");
    }
}

#[test]
fn identifier_keywords_distinguish_columns_constructors_and_explicit_aliases() {
    for sql in [
        "SELECT 1 AS default",
        "SELECT 1 \"default\"",
        "SELECT interval, q.interval AS qualified, interval + 1 FROM (VALUES (2)) q(interval)",
        "SELECT record, object FROM (VALUES (2, 3)) q(record, object)",
        "SELECT INTERVAL '3' DAY",
    ] {
        prepare(sql, &EmptyCatalog, &PrepareOptions::default()).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert!(prepare("SELECT 1 default", &EmptyCatalog, &PrepareOptions::default()).is_err());
    let sql = "SELECT RECORD() AS r, OBJECT(a: 1, b: RECORD(c: 2)) AS o";
    let rtdb_plan::Statement::Query(relation) = prepare(sql, &EmptyCatalog, &PrepareOptions::default()).unwrap().statement else { panic!() };
    assert_eq!(relation.schema().fields()[0].data_type, rtdb_types::LogicalType::Struct(Default::default()));
    let rtdb_types::LogicalType::Struct(fields) = &relation.schema().fields()[1].data_type else { panic!() };
    assert_eq!(fields.keys().collect::<Vec<_>>(), ["a", "b"]);
}

#[test]
fn implicit_and_explicit_labels_share_duplicate_checks() {
    for source in [
        "SELECT (v), v FROM (VALUES (1)) t(v)",
        "SELECT ARRAY_AGG(v), 12 AS _column_1 FROM (VALUES (1)) t(v)",
        "SELECT VERSION(), pg_catalog.VERSION()",
    ] {
        let error = prepare(source, &EmptyCatalog, &PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error", "{source}: {error}");
        assert!(error.message.as_deref().is_some_and(|message| message.starts_with("Duplicate column projection: ")), "{source}: {error}");
    }
}

#[test]
fn identifiers_retain_pinned_quoted_and_regular_spellings() {
    for source in ["SELECT 1 AS a1_$", "SELECT 1 AS _"] {
        assert!(prepare(source, &EmptyCatalog, &PrepareOptions::default()).is_ok(), "{source}");
    }
    for (source, name) in [
        ("SELECT 1 AS \"é@#\"", "é@#"),
        ("SELECT 1 AS `é@#`", "é@#"),
        ("SELECT 1 AS \"a\"\"b\"", "a\"\"b"),
        ("SELECT 1 AS `a``b`", "a``b"),
    ] {
        let rtdb_plan::Statement::Query(relation) = prepare(source, &EmptyCatalog, &PrepareOptions::default()).unwrap().statement else { panic!() };
        assert_eq!(relation.schema().fields()[0].name, name);
        assert_eq!(split_statements(source).unwrap().len(), 1);
    }
}

#[test]
fn invalid_identifiers_and_characters_are_rejected_at_each_public_lexical_entry() {
    for source in ["SELECT 1 AS \"a/b\"", "SELECT 1 AS `a/b`", "SELECT 1 AS é", "SELECT 1 AS a雪", "SELECT 1 AS @a", "SELECT 1 AS a@b", "SELECT a <@ b", "SELECT a ^@ b"] {
        assert_eq!(parse(source).unwrap_err().code, "xtdb/sql-error", "{source}");
        assert_eq!(parse_control(source, &PrepareOptions::default()).unwrap_err().code, "xtdb/sql-error", "{source}");
        assert_eq!(split_statements(source).unwrap_err().code, "xtdb/sql-error", "{source}");
    }
    assert!(parse_control("PREPARE \"a/b\" AS SELECT 1", &PrepareOptions::default()).is_err());
}

#[test]
fn identifier_rules_leave_string_and_comment_contents_unchanged() {
    for source in ["SELECT 'é/@#' AS v", "SELECT $$a/b$$ AS v", "SELECT /* é/@# */ 1 AS v", "SELECT 1 -- é/@#\n AS v"] {
        assert!(prepare(source, &EmptyCatalog, &PrepareOptions::default()).is_ok(), "{source}");
        assert_eq!(split_statements(source).unwrap().len(), 1);
    }
}

#[test]
fn hash_operator_does_not_join_its_neighboring_identifiers() {
    let options = PrepareOptions::default();
    for (source, spaced) in [
        ("SELECT 7#3 AS v", "SELECT 7 # 3 AS v"),
        ("SELECT a#b AS v FROM (VALUES (7,3)) AS t(a,b)", "SELECT a # b AS v FROM (VALUES (7,3)) AS t(a,b)"),
        ("SELECT a#>ARRAY['b'] AS v FROM (VALUES (1)) AS t(a)", "SELECT a #> ARRAY['b'] AS v FROM (VALUES (1)) AS t(a)"),
    ] {
        assert_eq!(prepare(source, &EmptyCatalog, &options).unwrap().statement, prepare(spaced, &EmptyCatalog, &options).unwrap().statement, "{source}");
    }
}

#[test]
fn first_invalid_identifier_precedes_a_later_tokenizer_error() {
    let source = "SELECT 1 AS \"bad/name\", 'unterminated";
    let errors = [parse(source).unwrap_err(), parse_control(source, &PrepareOptions::default()).unwrap_err(), split_statements(source).unwrap_err()];
    for error in errors {
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(error.context["position"], serde_json::json!(source.find('"').unwrap() + 1));
    }
}

#[test]
fn delimited_identifier_recognition_uses_the_failing_source_token() {
    use rtdb_sql::split_statements_in;
    use rtdb_types::MemoryBudget;

    for (source, line, column, bad) in [
        ("SELECT 1 AS \"zip/code\"", 1, 12, "\"zip/"),
        ("SELECT 'é';\n  SELECT `zip/code`", 2, 9, "`zip/"),
        ("SELECT 'é', \"a\"\"bé/c\"", 1, 15, "\"bé/"),
        ("SELECT \"zip/code", 1, 7, "\"zip/"),
    ] {
        let memory = MemoryBudget::new(64 << 10);
        let errors = [parse(source).unwrap_err(), split_statements(source).unwrap_err(),
            split_statements_in(source, &memory).err().expect("invalid SQL")];
        let start = source.find(bad).unwrap();
        for error in errors {
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(error.message, Some(format!("Errors parsing SQL statement:\n  - line {line}:{column} token recognition error at: '{bad}'")));
            assert_eq!(error.context["position"], serde_json::json!(start + 1));
            assert_eq!(error.context["span"], serde_json::json!({"start": start, "end": start + bad.len()}));
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn invalid_combined_operator_reports_its_first_invalid_character_before_a_later_lexical_error() {
    for (source, first) in [("SELECT a <@ b, 'unterminated", '@'), ("SELECT a ^@ b, 'unterminated", '^')] {
        let errors = [parse(source).unwrap_err(), parse_control(source, &PrepareOptions::default()).unwrap_err(), split_statements(source).unwrap_err()];
        for error in errors {
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(error.context["position"], serde_json::json!(source.find(first).unwrap() + 1));
        }
    }
}

#[test]
fn quoted_query_names_and_control_names_retain_doubled_delimiters() {
    for (name, expected) in [(r#""a""b""#, "a\"\"b"), ("`a``b`", "a``b"), (r#""a""""b""#, "a\"\"\"\"b")] {
        for source in [
            format!("SELECT {name} FROM (SELECT 1 AS {name}) AS t"),
            format!("WITH {name} AS (SELECT 1 AS {name}) SELECT {name} FROM {name}"),
        ] {
            let rtdb_plan::Statement::Query(relation) = prepare(&source, &EmptyCatalog, &PrepareOptions::default()).unwrap().statement else { panic!() };
            assert_eq!(relation.schema().fields()[0].name, expected);
            let spans = split_statements(&source).unwrap();
            assert_eq!(spans.len(), 1);
            assert_eq!(&source[spans[0].start..spans[0].end], source);
        }
        let rtdb_plan::ControlStatement::AttachDatabase { name: actual, .. } = parse_control(&format!("ATTACH DATABASE {name}"), &PrepareOptions::default()).unwrap().unwrap() else { panic!() };
        assert_eq!(actual, expected);
        let rtdb_plan::ControlStatement::CopyIn { table, .. } = parse_control(&format!("COPY {name} FROM STDIN WITH ()"), &PrepareOptions::default()).unwrap().unwrap() else { panic!() };
        assert_eq!(table[0].text, expected);
        assert!(table[0].quoted);
    }
}
