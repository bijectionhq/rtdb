use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_index::PartitionState;
use rtdb_plan::Statement;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{LogicalType, Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap()
}
async fn query(source: &str) -> Result<Vec<Vec<V>>> {
    let context = context();
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default())?.statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() { rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?); }
    }
    Ok(rows)
}
fn text(value: &str) -> V { V::Utf8(value.into()) }

#[test]
fn character_length_forms_select_the_pinned_scalar_and_projection_label() {
    let context = context();
    let options = PrepareOptions::default();
    for spelling in ["CHARACTER_LENGTH", "CHAR_LENGTH"] {
        for (clause, expected) in [
            ("", "character_length"),
            (" USING CHARACTERS", "character_length"),
            (" USING OCTETS", "octet_length"),
        ] {
            let sql = format!("SELECT {spelling}(foo.a{clause}) FROM (VALUES ('é😀')) foo(a)");
            let Statement::Query(relation) = prepare(&sql, &context.catalog, &options).unwrap().statement else {
                panic!("query");
            };
            let expressions = relation.expressions();
            let [rtdb_plan::Expr::Call { name, args, .. }] = expressions.as_slice() else {
                panic!("scalar projection");
            };
            assert_eq!(name, expected, "{sql}");
            assert!(matches!(args.as_slice(), [rtdb_plan::Expr::Column { .. }]), "{sql}");
            assert_eq!(relation.schema().fields()[0].name, "_column_1", "{sql}");
        }
    }
}

#[tokio::test]
async fn character_length_units_count_unicode_and_preserve_nulls_and_binary_octets() {
    let rows = query("SELECT CHAR_LENGTH('aé😀' USING CHARACTERS), CHARACTER_LENGTH('aé😀' USING OCTETS), \
        CHARACTER_LENGTH(NULL USING CHARACTERS), CHAR_LENGTH(NULL USING OCTETS), CHAR_LENGTH(X('00FF') USING OCTETS), \
        CHAR_LENGTH(CAST(CHARACTER_LENGTH('é😀' USING OCTETS) AS TEXT) USING CHARACTERS)")
        .await.unwrap();
    assert_eq!(rows, [vec![V::I32(3), V::I32(7), V::Null, V::Null, V::I32(2), V::I32(1)]]);
    let error = query("SELECT CHAR_LENGTH(X('00FF') USING CHARACTERS)").await.unwrap_err();
    assert_eq!(error.code, "xtdb.expression/function-type-mismatch");
}

#[test]
fn character_length_units_preserve_syntax_and_later_binding_errors() {
    let context = context();
    let options = PrepareOptions::default();
    for spelling in ["CHAR_LENGTH", "CHARACTER_LENGTH"] {
        for wrong_unit in ["BYTES", "\"OCTETS\"", "$1"] {
            let sql = format!("SELECT {spelling}('x' USING {wrong_unit})");
            let error = rtdb_sql::parse(&sql).unwrap_err();
            assert_eq!(error.code, "xtdb/sql-error");
            assert!(error.message.as_deref().is_some_and(|message| message.contains("CHARACTERS or OCTETS")), "{sql}: {error}");
        }
        for name in [format!("pg_catalog.{spelling}"), format!("\"{spelling}\"")] {
            let sql = format!("SELECT {name}('x')");
            assert!(rtdb_sql::parse(&sql).is_ok(), "{sql}");
            let error = prepare(&sql, &context.catalog, &options).unwrap_err();
            assert!(error.message.as_deref().is_some_and(|message| message.contains("Unknown function:")), "{sql}: {error}");
            for units in ["CHARACTERS", "OCTETS"] {
                let sql = format!("SELECT {name}('x' USING {units})");
                assert_eq!(rtdb_sql::parse(&sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
            }
        }
        for argument in ["DISTINCT 'x'", "'x', 'y'", "'x' AS alias", "*"] {
            let sql = format!("SELECT {spelling}({argument} USING OCTETS)");
            assert!(rtdb_sql::parse(&sql).is_err(), "{sql}");
        }
        for null_treatment in ["IGNORE NULLS", "RESPECT NULLS"] {
            for unit in ["CHARACTERS", "OCTETS"] {
                let sql = format!("SELECT {spelling}('x' USING {unit}) {null_treatment}");
                assert!(rtdb_sql::parse(&sql).is_err(), "{sql}");
            }
            let sql = format!("SELECT {spelling}('x') {null_treatment}");
            assert!(prepare(&sql, &context.catalog, &options).is_ok(), "{sql}");
        }
        for unit in ["octets", "Characters"] {
            let sql = format!("SELECT {spelling}('x' USING {unit})");
            assert!(rtdb_sql::parse(&sql).is_ok(), "{sql}");
            let error = prepare(&sql, &context.catalog, &options).unwrap_err();
            assert_eq!(error.code, "xtdb.error/illegal-arg");
            assert_eq!(error.message, Some(format!("No matching clause: {unit}")));
        }
        let sql = format!("SELECT {spelling}(CHAR_LENGTH('x' USING Characters) USING octets)");
        let error = prepare(&sql, &context.catalog, &options).unwrap_err();
        assert_eq!(error.code, "xtdb.error/illegal-arg");
        assert_eq!(error.message.as_deref(), Some("No matching clause: Characters"));
    }
}

#[tokio::test]
async fn sql_concat_stringifies_arguments_and_treats_null_as_empty_text() {
    assert_eq!(query("SELECT CONCAT('a',NULL),CONCAT('a',1) AS test_output_2,CONCAT(1) AS test_output_3,CONCAT(NULL) AS test_output_4,CONCAT(X('0102'),TRUE) AS test_output_5").await.unwrap(),
        [vec![text("a"), text("a1"), text("1"), text(""), text("\\x0102true")]]);
    assert_eq!(query("SELECT 'a'||NULL,X('01')||X('02')").await.unwrap(), [vec![V::Null, V::Binary(vec![1, 2])]]);
    for sql in ["SELECT CONCAT()", "SELECT STR()"] { assert!(prepare(sql, &context().catalog, &PrepareOptions::default()).is_err(), "{sql}"); }
}

#[tokio::test]
async fn stringutil_sql_overlay_position_units_and_binary_functions() {
    assert_eq!(query("SELECT POSITION('é' IN 'a😀éz'),POSITION('é' IN 'a😀éz' USING OCTETS) AS test_output_2,POSITION('é' IN 'a😀éz' USING CHARACTERS) AS test_output_3").await.unwrap(), [vec![V::I32(3), V::I32(6), V::I32(3)]]);
    assert_eq!(query("SELECT OVERLAY('abcdef' PLACING 'zz' FROM 2),OVERLAY('abcdef' PLACING 'zz' FROM 2 FOR 3) AS test_output_2,SUBSTRING(X('010203') FROM 2 FOR 1),TRIM(X('00') FROM X('000100'))").await.unwrap(), [vec![text("azzdef"), text("azzef"), V::Binary(vec![2]), V::Binary(vec![1])]]);
    assert_eq!(query("SELECT OVERLAY('abcdef' PLACING LOWER('ZZ') FROM 2 FOR 1),X('01')||X('02')").await.unwrap(), [vec![text("azzcdef"), V::Binary(vec![1, 2])]]);
}

#[tokio::test]
async fn substring_and_overlay_parse_units_without_changing_character_positions() {
    assert_eq!(query("SELECT SUBSTRING('a😀éz' FROM 2 FOR 2 USING OCTETS) AS sub_octets, SUBSTRING('a😀éz' FROM 2 USING Characters) AS sub_chars").await.unwrap(),
        [vec![text("😀é"), text("😀éz")]]);
    assert_eq!(query("SELECT OVERLAY('a😀éz' PLACING 'Q' FROM 3 FOR 1 USING OCTETS) AS overlay_octets, OVERLAY('a😀éz' PLACING 'Q' FROM 3 USING octets) AS overlay_default").await.unwrap(),
        [vec![text("a😀Qz"), text("a😀Qz")]]);
    assert_eq!(query("SELECT SUBSTRING(NULL FROM 1 USING OCTETS) AS sub_null, SUBSTRING(X('010203') FROM 2 FOR 1 USING CHARACTERS) AS sub_bytes").await.unwrap(),
        [vec![V::Null, V::Binary(vec![2])]]);
}

#[test]
fn substring_and_overlay_units_require_a_complete_standard_operand_list() {
    for sql in [
        "SELECT SUBSTRING('abc' USING CHARACTERS)",
        "SELECT SUBSTRING('abc', 2 USING OCTETS)",
        "SELECT SUBSTR('abc' FROM 2 USING OCTETS)",
        "SELECT SUBSTRING('abc' FROM 2, 1 USING OCTETS)",
        "SELECT SUBSTRING('abc' FROM 2 USING other)",
        r#"SELECT OVERLAY('abc' PLACING 'x' FROM 2 USING "OCTETS")"#,
    ] {
        assert_eq!(rtdb_sql::parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    let parsed = rtdb_sql::parse(
        "SELECT SUBSTRING(? FROM ? FOR ? USING OCTETS), OVERLAY(? PLACING ? FROM ? FOR ? USING CHARACTERS)",
    ).unwrap();
    assert_eq!(parsed[0].parameter_count(), 7);

    let generic = sqlparser::dialect::GenericDialect {};
    for sql in [
        "SELECT SUBSTRING('abc' FROM 2 USING OCTETS)",
        "SELECT OVERLAY('abc' PLACING 'x' FROM 2 USING OCTETS)",
    ] {
        assert!(sqlparser::parser::Parser::parse_sql(&generic, sql).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn substring_bare_length_preserves_start_expression_precedence_and_units() {
    let rows = query("SELECT SUBSTRING('abcdef' FROM 2 3), SUBSTRING('abcdef' FROM 1 + 1 2 * 1), \
        SUBSTRING('abcdef' FROM 2 (1 + 2)), SUBSTRING('abcdef' FROM ABS (2) (1 + 1)), \
        SUBSTRING('abcdef' FROM ABS (2)), SUBSTRING('abcdef' FROM 2 - 1), \
        SUBSTRING('é😀x' FROM 2 1 USING OCTETS)")
        .await.unwrap();
    assert_eq!(
        rows,
        [vec![text("bcd"), text("bc"), text("bcd"), text("bc"), text("bcdef"), text("abcdef"), text("😀")]],
    );
}

#[test]
fn substring_bare_length_requires_a_separate_operand_after_explicit_from() {
    for sql in [
        "SELECT SUBSTRING('abc' FROM 2 1, 3)",
        "SELECT SUBSTRING('abc' FROM 2 1 3)",
        "SELECT SUBSTRING('abc', 2 1)",
        "SELECT SUBSTRING('abc' FROM 2 FROM)",
        "SELECT SUBSTR('abc' FROM 2 1)",
        "SELECT OVERLAY('abc' PLACING 'x' FROM 2 1)",
    ] {
        assert_eq!(rtdb_sql::parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    let generic = sqlparser::dialect::GenericDialect {};
    assert!(sqlparser::parser::Parser::parse_sql(&generic, "SELECT SUBSTRING('abc' FROM 2 1)").is_err());
    let parametric = "SELECT HISTOGRAM(0.5, 0.6)(x, y)";
    assert!(sqlparser::parser::Parser::parse_sql(&generic, parametric).is_ok());
    assert_eq!(rtdb_sql::parse(parametric).unwrap_err().code, "xtdb/sql-error");
    for sql in [
        "SELECT SUBSTRING('abc' FROM 2 FOR 1)",
        "SELECT SUBSTRING('abc' FROM 2)",
        "SELECT SUBSTRING('abc', 2, 1)",
    ] {
        assert!(sqlparser::parser::Parser::parse_sql(&generic, sql).is_ok(), "{sql}");
        assert!(rtdb_sql::parse(sql).is_ok(), "{sql}");
    }
}

#[test]
fn substring_bare_length_gates_numeric_spelling_before_parsing_the_expression() {
    for length in ["1L", ".1", "1."] {
        let bare = format!("SELECT SUBSTRING('abc' FROM 2 {length})");
        assert_eq!(rtdb_sql::parse(&bare).unwrap_err().code, "xtdb/sql-error", "{bare}");
        let explicit = format!("SELECT SUBSTRING('abc' FROM 2 FOR {length})");
        assert!(rtdb_sql::parse(&explicit).is_ok(), "{explicit}");
    }
}

#[tokio::test]
async fn substring_without_from_preserves_operand_precedence_and_units() {
    let rows = query("SELECT SUBSTRING('abcdef' 2), SUBSTRING('abcdef' 2 FOR 2), \
        SUBSTRING('abcdef' 1 + 1 2 * 1), SUBSTRING(LOWER ('ABCDEF') (2) (1 + 1)), \
        SUBSTRING(LOWER ('ABCDEF')), SUBSTRING('abcdef' 2 - 1), \
        SUBSTRING('a😀éz' 2 2 USING octets), SUBSTRING(X('010203') 2 USING CHARACTERS), \
        SUBSTRING(NULL 2 FOR 1)")
        .await.unwrap();
    assert_eq!(
        rows,
        [vec![
            text("bcdef"), text("bc"), text("bc"), text("bc"), text("abcdef"), text("abcdef"),
            text("😀é"), V::Binary(vec![2, 3]), V::Null,
        ]],
    );
}

#[test]
fn substring_without_from_requires_a_separate_start_and_excludes_commas() {
    for sql in [
        "SELECT SUBSTRING('abc' 2, 1)",
        "SELECT SUBSTRING('abc' 2, 1 USING OCTETS)",
        "SELECT SUBSTRING('abc' 2 FOR 1, 3)",
        "SELECT SUBSTRING('abc' 2 1 3)",
        "SELECT SUBSTRING('abc' 1L)",
        "SELECT SUBSTRING('abc' .1)",
        "SELECT SUBSTRING('abc' 1.)",
        "SELECT SUBSTRING('abc' 2 USING other)",
        "SELECT SUBSTR('abc' 2)",
        "SELECT OVERLAY('abc' PLACING 'x' 2)",
    ] {
        assert_eq!(rtdb_sql::parse(sql).unwrap_err().code, "xtdb/sql-error", "{sql}");
    }
    for sql in [
        "SELECT SUBSTRING('abc')",
        "SELECT SUBSTRING('abc' FOR 2)",
        "SELECT SUBSTRING('abc' FROM 2, 1)",
        "SELECT SUBSTRING('abc', 2 FOR 1)",
    ] {
        assert!(rtdb_sql::parse(sql).is_ok(), "{sql}");
    }
    let generic = sqlparser::dialect::GenericDialect {};
    for sql in [
        "SELECT SUBSTRING('abc' 2)",
        "SELECT SUBSTRING('abc' 2 FOR 1)",
        "SELECT SUBSTRING('abc' 2 1)",
    ] {
        assert!(sqlparser::parser::Parser::parse_sql(&generic, sql).is_err(), "{sql}");
    }
}

#[tokio::test]
async fn string_null_policy_is_preserved_when_virtual_columns_narrow_at_execution() {
    let context = context();
    let source = "SELECT STR(_valid_from),STRING_TO_ARRAY('ab',CAST(_valid_from AS TEXT)),FORMAT('%s',_valid_from) FROM pg_catalog.pg_namespace LIMIT 1";
    let Statement::Query(relation) = prepare(source, &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("query") };
    let mut cursor = PreparedQuery::new(relation).unwrap().execute(context, vec![]).await.unwrap();
    assert!(cursor.runtime_schema().fields().iter().all(|field| field.data_type != LogicalType::Null));
    let batch = cursor.next_batch().await.unwrap().unwrap();
    let row = batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| read_value(field, array.as_ref(), 0).unwrap()).collect::<Vec<_>>();
    assert_eq!(row, [text(""), V::list(vec![text("a"), text("b")]), text("")]);
}

#[tokio::test]
async fn regexp_replace_literals_and_unsupported_start_are_admitted_before_rows() {
    assert_eq!(query("SELECT REGEXP_REPLACE('a b a','a','x')").await.unwrap(), [vec![text("x b x")]]);
    let error = query("SELECT REGEXP_REPLACE('a','a','x',1)").await.unwrap_err();
    assert_eq!(error.code, "xtdb.expression/unsupported");
}

#[tokio::test]
async fn regexp_replace_reports_source_exception_category_only_after_a_match() {
    for (replacement, category, code, message) in [
        ("$", rtdb_types::AnomalyCategory::Incorrect, "xtdb.expression/illegal-argument-exception", "Illegal group reference: group index is missing"),
        ("${1}", rtdb_types::AnomalyCategory::Incorrect, "xtdb.expression/illegal-argument-exception", "capturing group name {1} starts with digit character"),
        ("$x$9", rtdb_types::AnomalyCategory::Incorrect, "xtdb.expression/illegal-argument-exception", "Illegal group reference"),
        ("$9$", rtdb_types::AnomalyCategory::Fault, "xtdb.error/unknown", "No group 9"),
    ] {
        let error = query(&format!("SELECT REGEXP_REPLACE('xx','x','{replacement}')")).await.unwrap_err();
        assert_eq!(error.category, category);
        assert_eq!(error.code, code);
        assert_eq!(error.message.as_deref(), Some(message));
        assert_eq!(query(&format!("SELECT REGEXP_REPLACE('kept','x','{replacement}')")).await.unwrap(), [vec![text("kept")]]);
        assert_eq!(query(&format!("SELECT REGEXP_REPLACE(NULL,'x','{replacement}')")).await.unwrap(), [vec![V::Null]]);
    }
}

#[tokio::test]
async fn regclass_and_regproc_text_casts_resolve_captured_catalog_and_search_path() {
    let oid = rtdb_types::pg_table_oid(None, &rtdb_types::TableRef::new("pg_catalog", "pg_class"));
    assert_eq!(query("SELECT CAST('pg_class' AS REGCLASS),CAST(CAST('pg_catalog.pg_class' AS REGCLASS) AS TEXT) AS test_output_2,STR(CAST('pg_class' AS REGCLASS))").await.unwrap(), [vec![V::RegClass(oid), text("pg_class"), text("pg_class")]]);
    assert_eq!(query("SELECT CAST(CAST(42 AS REGCLASS) AS TEXT),CAST(CAST('pg_catalog.array_in' AS REGPROC) AS TEXT) AS test_output_2,CAST(CAST('array_in' AS REGPROC) AS INTEGER) AS test_output_3").await.unwrap(), [vec![text("42"), text("array_in"), V::I32(rtdb_types::pg_name_oid("array_in"))]]);
    assert_eq!(query("SELECT CAST('does_not_exist' AS REGCLASS)").await.unwrap_err().code, "xtdb.expression.pg/unknown-relation");
    assert_eq!(query("SELECT CAST('does_not_exist' AS REGPROC)").await.unwrap_err().code, "xtdb.expression.pg/unknown-proc");
}

#[tokio::test]
async fn position_full_predicate_needles_reach_string_type_admission() {
    for source in ["SELECT POSITION(TRUE=TRUE IN 'x')","SELECT POSITION(TRUE IN (TRUE) IN ('x'))","SELECT POSITION(FALSE OR TRUE IN 'x')"] {
        assert_eq!(query(source).await.unwrap_err().code,"xtdb.expression/function-type-mismatch","{source}");
    }
    assert_eq!(query("SELECT POSITION(CASE WHEN TRUE IN (TRUE) THEN 'b' ELSE 'a' END IN ('abc')),POSITION('b' IN CASE WHEN TRUE THEN 'abc' ELSE 'x' END USING OCTETS) AS test_output_2").await.unwrap(),[vec![V::I32(2),V::I32(2)]]);
}

#[test]
fn position_retry_exhaustion_is_a_public_sql_error() {
    for case_haystack in [false,true] {
        let mut source = "'x'".to_string();
        for _ in 0..16 {
            source = if case_haystack { format!("POSITION(CAST({source} AS VARCHAR) IN CASE WHEN 'x' IN ('x') THEN 'x' ELSE 'x' END)") }
                else { format!("POSITION({source} IN 'x' IN ('x'))") };
        }
        let error = rtdb_sql::parse(&format!("SELECT {source}")).unwrap_err();
        assert_eq!(error.code,"xtdb/sql-error");
        assert!(error.message.as_deref().is_some_and(|message| message.contains("POSITION cumulative parsing work limit exceeded")),"{error:?}");
    }
}

#[tokio::test]
async fn contiguous_x_paren_quote_is_a_binary_literal_a_spaced_one_is_a_call_and_x_quote_is_rejected() {
    assert_eq!(query("SELECT X('00f0') AS v, x('5C') AS w, X('') AS e").await.unwrap(), [vec![V::Binary(vec![0x00, 0xf0]), V::Binary(vec![0x5c]), V::Binary(vec![])]]);
    let odd = query("SELECT X('0') AS v").await.unwrap_err();
    assert_eq!(odd.message.as_deref(), Some("Cannot parse binary string expr: X('0') - failed with message Odd number of characters."));
    for call in ["SELECT X ('00') AS v", "SELECT X('GG') AS v", "SELECT \"X\"('00') AS v"] {
        assert!(query(call).await.unwrap_err().message.unwrap().starts_with("Unknown function: "), "{call}");
    }
    for quoted in ["SELECT X'00f0' AS v", "SELECT x'5C' AS w", "SELECT X'0' AS v", "SELECT X'' AS e"] {
        let error = query(quoted).await.unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error", "{quoted}");
        assert!(error.message.unwrap().starts_with("Errors parsing SQL statement:\n  - "), "{quoted}");
    }
}

#[tokio::test]
async fn struct_literal_keys_named_like_odbc_escapes_stay_struct_keys() {
    let rows = query("SELECT {d: 1} AS a, [{t: 1, v: 2}] AS b, {ts: TIMESTAMP '2020-01-01Z'} AS c").await.unwrap();
    let [a, b, c] = rows[0].as_slice() else { panic!("{rows:?}") };
    assert_eq!(a, &V::Struct(StructFields::from([("d".into(), V::I64(1))])));
    assert_eq!(b, &V::list(vec![V::Struct(StructFields::from([("t".into(), V::I64(1)), ("v".into(), V::I64(2))]))]));
    assert!(matches!(c, V::Struct(fields) if matches!(fields.get("ts"), Some(V::Timestamp { .. }))), "{c:?}");
}
