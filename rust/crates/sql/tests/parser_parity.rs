use rtdb_sql::{parse, parse_statement_batch};
use rtdb_types::MemoryBudget;

#[test]
fn fetch_requires_direction_row_keyword_and_completion() {
    for sql in [
        "SELECT 1 FETCH FIRST 1 ROWS",
        "SELECT 1 FETCH FIRST 1 ONLY",
        "SELECT 1 FETCH 1 ROWS ONLY",
        "SELECT 1 FETCH FIRST ROW",
        r#"SELECT 1 FETCH "FIRST" 1 ROWS ONLY"#,
        r#"SELECT 1 FETCH FIRST 1 "ROWS" ONLY"#,
        r#"SELECT 1 FETCH FIRST 1 ROWS "ONLY""#,
    ] {
        assert_eq!(parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    for (sql, parameter_count) in [
        ("SELECT 1 FETCH FIRST ROW ONLY", 0),
        ("SELECT 1 OFFSET 2 ROWS fetch next 1 row only", 0),
        ("SELECT 1 FETCH NEXT ? ROWS ONLY OFFSET 1", 1),
        ("SELECT 1 FETCH FIRST $3 ROWS ONLY", 3),
        (r#"SELECT 1 AS "FETCH""#, 0),
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(statement[0].parameter_count(), parameter_count, "{sql}");
    }
}

#[test]
fn a_wrapped_table_reference_keeps_aliases_inside_its_parentheses() {
    for sql in [
        "SELECT * FROM (docs AS d) d2",
        r#"SELECT * FROM (docs AS d) AS "where""#,
        r#"SELECT * FROM ((SELECT 1) AS d) AS "where""#,
    ] {
        assert_eq!(parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    for sql in [
        r#"SELECT * FROM (docs AS "where")"#,
        "SELECT * FROM (docs AS d JOIN other AS o ON TRUE)",
        r#"SELECT * FROM (SELECT 1) AS "where""#,
        "SELECT * FROM (VALUES (1)) q(n), (q.n) AS t(v)",
    ] {
        parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn line_feed_requires_an_escaped_or_dollar_quoted_string() {
    let memory = MemoryBudget::new(1 << 20);
    for sql in ["SELECT 'a\nb'", "SELECT 'a\r\nb'", "SELECT 'it''s\na'"] {
        let error = parse(sql).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(parse_statement_batch(sql, &memory).unwrap_err(), error);
    }
    for sql in [
        "SELECT E'a\nb'",
        r"SELECT E'a\nb'",
        "SELECT $$a\nb$$",
        "SELECT $tag$a\nb$tag$",
        "SELECT 'a\rb'",
        "SELECT 1 AS \"a\nb\"",
        "SELECT 1 AS `a\nb`",
    ] {
        parse(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        parse_statement_batch(sql, &memory).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    }
}

#[test]
fn parse_allows_only_one_delimiter_between_statements_and_after_the_batch() {
    for sql in [";", "; SELECT 1", "SELECT 1;;", "SELECT 1; /* gap */ ; SELECT 2"] {
        assert_eq!(parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    assert_eq!(parse("SELECT ';'; SELECT 2; /* end */").unwrap().len(), 2);
    assert_eq!(parse("/* nothing */").unwrap().len(), 0);
}

#[test]
fn direct_ddl_names_observe_the_reference_identifier_tier() {
    for sql in [
        "CREATE TABLE docs (where)",
        "CREATE TABLE docs (where INT)",
        "GRANT where TO demo",
        "GRANT demo TO from",
        "REVOKE demo FROM where",
    ] {
        assert_eq!(parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    for sql in [
        r#"CREATE TABLE docs ("where", `from` TEXT, time)"#,
        r#"GRANT "where" TO `from`"#,
        r#"REVOKE "where" FROM `from`"#,
        "GRANT time TO role",
    ] {
        parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn the_transaction_statement_head_does_not_change_identifier_or_case_contexts() {
    for sql in ["ABORT", "abort WORK", "END", "END TRANSACTION"] {
        assert_eq!(parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    for sql in [r#"SELECT 1 AS "ABORT", 2 AS "END""#, "SELECT CASE WHEN TRUE THEN 1 END AS end"] {
        parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
