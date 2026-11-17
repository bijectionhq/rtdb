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
fn identifiers_retain_pinned_quoted_and_regular_spellings() {
    for source in ["SELECT 1 AS a1_$", "SELECT 1 AS _"] {
        assert!(prepare(source, &EmptyCatalog, &PrepareOptions::default()).is_ok(), "{source}");
    }
    for (source, name) in [
        ("SELECT 1 AS \"é@#\"", "é@#"),
        ("SELECT 1 AS `é@#`", "é@#"),
        ("SELECT 1 AS \"a\"\"b\"", "a\"b"),
        ("SELECT 1 AS `a``b`", "a`b"),
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
fn invalid_combined_operator_reports_its_first_invalid_character_before_a_later_lexical_error() {
    for (source, first) in [("SELECT a <@ b, 'unterminated", '@'), ("SELECT a ^@ b, 'unterminated", '^')] {
        let errors = [parse(source).unwrap_err(), parse_control(source, &PrepareOptions::default()).unwrap_err(), split_statements(source).unwrap_err()];
        for error in errors {
            assert_eq!(error.code, "xtdb/sql-error");
            assert_eq!(error.context["position"], serde_json::json!(source.find(first).unwrap() + 1));
        }
    }
}
