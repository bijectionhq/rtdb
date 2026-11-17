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
fn comments_do_not_contribute_query_tokens_or_parameters() {
    let options = PrepareOptions::default();
    let expected = prepare("SELECT 2 AS v", &EmptyCatalog, &options).unwrap();
    for source in [
        "SELECT /*! $99; XTQL $$invalid$$ */ 2 AS v",
        "SELECT /*+ ?; nonsense */ 2 AS v",
        "/*!40101 SELECT $99; */ SELECT 2 AS v",
        "SELECT /* /*! nested */ ignored */ 2 AS v",
        "SELECT /* 'unterminated ? $7 ; */ 2 AS v",
        "SELECT --+ $88; SELECT 3\n2 AS v",
    ] {
        let statements = parse(source).unwrap();
        assert_eq!(statements.len(), 1, "{source}");
        assert_eq!(statements[0].parameter_count(), 0, "{source}");
        assert_eq!(prepare(source, &EmptyCatalog, &options).unwrap().statement, expected.statement, "{source}");
    }
    assert_eq!(parse("SELECT /*! ? $99 */ ?, /*+ $88 */ ?").unwrap()[0].parameter_count(), 2);
}

#[test]
fn comment_only_input_has_no_query_control_or_protocol_statement() {
    for source in ["/*! SELECT $99; */", "/*+ BEGIN; */", "/* /*! COMMIT */ SELECT ?; */", "--+ ROLLBACK\n"] {
        assert!(parse(source).unwrap().is_empty(), "{source}");
        assert!(parse_control(source, &PrepareOptions::default()).unwrap().is_none(), "{source}");
        assert!(split_statements(source).unwrap().is_empty(), "{source}");
    }
}

#[test]
fn control_commands_ignore_executable_comment_spelling() {
    let options = PrepareOptions::default();
    for (source, plain) in [
        ("/*! BEGIN READ WRITE; */ BEGIN READ ONLY /*+ $99 ? */", "BEGIN READ ONLY"),
        ("COMMIT /*! ; ROLLBACK */", "COMMIT"),
        ("SET TIME ZONE /*! $100; */ 'UTC'", "SET TIME ZONE 'UTC'"),
        ("/*+ PREPARE hidden AS SELECT ? */ ROLLBACK", "ROLLBACK"),
    ] {
        assert_eq!(parse_control(source, &options).unwrap(), parse_control(plain, &options).unwrap(), "{source}");
    }
}

#[test]
fn comment_semicolons_and_multiline_text_preserve_real_statement_spans() {
    let source = "/*! SELECT $99;\n */ SELECT /*+ ; ? */ 1; /* /*! COMMIT; */ */ COMMIT;";
    let spans = split_statements(source).unwrap();
    let statements: Vec<_> = spans.iter().map(|span| &source[span.start..span.end]).collect();
    assert_eq!(statements, ["SELECT /*+ ; ? */ 1", "COMMIT"]);
    let parsed = parse(source).unwrap();
    assert_eq!(parsed.len(), 2);
    for (statement, span) in parsed.iter().zip(&spans) {
        assert_eq!(statement.span, *span);
        assert_eq!(statement.parameter_count(), 0);
    }
}

#[test]
fn comment_delimiters_in_strings_and_quoted_identifiers_are_literal() {
    let options = PrepareOptions::default();
    for source in ["SELECT '/*! ?; */' AS v", "SELECT $$/*+ ?; */$$ AS v", "SELECT 1 AS \"-- ?;\""] {
        assert_eq!(parse(source).unwrap()[0].parameter_count(), 0);
        let spans = split_statements(source).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(&source[spans[0].start..spans[0].end], source);
        assert!(prepare(source, &EmptyCatalog, &options).is_ok(), "{source}");
    }
}

#[test]
fn nested_hint_spelling_is_consumed_as_one_nested_comment() {
    let source = format!("{} ignored ?; {} SELECT 1", "/*!".repeat(1024), "*/".repeat(1024));
    let parsed = parse(&source).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].parameter_count(), 0);
    assert_eq!(&source[parsed[0].span.start..parsed[0].span.end], "SELECT 1");
    assert_eq!(split_statements(&source).unwrap().len(), 1);
}

#[test]
fn unterminated_comments_fail_at_every_public_lexical_entry() {
    let source = "SELECT 1 /*! unfinished";
    assert_eq!(parse(source).unwrap_err().code, "xtdb/sql-error");
    assert_eq!(split_statements(source).unwrap_err().code, "xtdb/sql-error");
    assert_eq!(parse_control(source, &PrepareOptions::default()).unwrap_err().code, "xtdb/sql-error");
}
