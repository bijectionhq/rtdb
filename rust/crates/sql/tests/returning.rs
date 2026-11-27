use rtdb_types::StructFields;
use std::collections::BTreeSet;
use rtdb_plan::{Catalog, QualifiedTable, Statement, TableBinding};
use rtdb_sql::{PrepareOptions, parse, prepare, prepare_static};
use rtdb_types::{LogicalType, Result, ScalarType};

struct Fixture;
impl Catalog for Fixture {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok(Some(TableBinding { name: name.clone(), oid: Some(16384), columns: StructFields::from([
            ("_id".into(), LogicalType::Scalar(ScalarType::I64)),
            ("foo".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ]) }))
    }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

#[test]
fn returning_select_lists_do_not_bind_names_functions_or_errors_into_mutations() {
    for mutation in ["INSERT INTO docs (_id,foo) VALUES (1,'bar')", "INSERT INTO docs RECORDS {_id:1,foo:'bar'}", "UPDATE docs SET foo='toto' WHERE _id=1", "DELETE FROM docs WHERE _id=1"] {
        let expected = prepare(mutation, &Fixture, &PrepareOptions::default()).unwrap();
        assert!(matches!(expected.statement, Statement::Dml(_)));
        for returning in ["* EXCLUDE foo", "_id+1,foo AS bar", "missing,unknown_function(1),1/0", "SUBSTR(NULL)", "DATE 'not-a-date',TIMESTAMP 'not-a-time',CAST(1 AS DECIMAL(100))", "missing IS DISTINCT FROM other", "missing=ALL(ARRAY[1,2])", "percentile_cont(0.5) WITHIN GROUP (ORDER BY missing)", "missing AS DEFAULT", "*,_valid_time AS requested_period", "schema.missing.*", "* RENAME (missing._id AS first,other._id second)", "(SELECT missing FROM absent FOR ALL VALID_TIME)", "(XTQL 'not a query')", "(XTQL ('(fn [x] (rel x [x]))',$8,$9))", "", ",", "missing,,", ",*"] {
            let sql = format!("{mutation} RETURNING {returning}");
            let actual = prepare(&sql, &Fixture, &PrepareOptions::default()).unwrap();
            assert_eq!(actual.statement, expected.statement, "{sql}");
            assert_eq!(actual.parameter_types, expected.parameter_types, "{sql}");
            assert_eq!(actual.source, sql);
            assert_eq!(parse(&sql).unwrap()[0].span.end, sql.trim_end().len());
        }
    }
}

#[test]
fn only_mutation_prefix_parameters_count_and_keep_their_original_indices() {
    for (prefix, count) in [
        ("INSERT INTO docs (_id,foo) VALUES (?,?)", 2),
        ("INSERT INTO docs RECORDS {_id:?,foo:?}", 2),
        ("UPDATE docs SET foo=$3 WHERE _id=?", 3),
        ("DELETE FROM docs WHERE _id=?", 1),
        ("INSERT INTO docs (_id,foo) XTQL '(fn [id] (rel [{:_id id :foo \"bar\"}] [_id foo]))'", 1),
    ] {
        let sql = format!("{prefix} RETURNING $100,?,(XTQL '(fn [a b] (rel a [x]))')");
        assert_eq!(parse(&sql).unwrap()[0].parameter_count(), count);
        let expected = prepare(prefix, &Fixture, &PrepareOptions::default()).unwrap();
        let actual = prepare(&sql, &Fixture, &PrepareOptions::default()).unwrap();
        assert_eq!(actual.statement, expected.statement); assert_eq!(actual.parameter_types, expected.parameter_types);
    }
    assert_eq!(parse("DELETE FROM docs RETURNING ?,$99").unwrap()[0].parameter_count(), 0);
    assert_eq!(parse("INSERT INTO docs (_id) SELECT 1 RETURNING (XTQL '(fn [a] (rel a [x]))')").unwrap()[0].parameter_count(), 0);
    let sql = "INSERT INTO docs (_id,foo) VALUES (1,'RETURNING ?') RETURNING \"RETURNING\"";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(), 0);
}

#[test]
fn returning_keeps_literal_insert_and_records_static_expansion_eligible() {
    for prefix in ["INSERT INTO docs (_id,foo) VALUES (1,'bar')", "INSERT INTO docs RECORDS {_id:1,foo:'bar'}"] {
        let expected = prepare_static(prefix, &Fixture, &PrepareOptions::default()).unwrap().unwrap();
        let actual = prepare_static(&format!("{prefix} RETURNING missing,unknown_function(1),1/0,?"), &Fixture, &PrepareOptions::default()).unwrap().unwrap();
        assert_eq!(actual.table, expected.table); assert_eq!(actual.input, expected.input);
    }
}

#[test]
fn returning_requires_select_list_syntax_and_is_rejected_by_erase_and_patch() {
    for returning in ["x FROM docs", "x WHERE TRUE", "x ORDER BY x", "x LIMIT 1", "x UNION SELECT 1", "x FOR ALL VALID_TIME", "DISTINCT x", "x DEFAULT", "x AS 'text'", "x+", "f(", "* EXCLUDE ()", "* RENAME x", "* EXCLUDE t.x", "t.* RENAME t.x AS y", "x,*,y", ",*,y", "*,*", "x,*,", "* EXCEPT (x)", "* REPLACE (1 AS x)", "(XTQL ('bad',1))"] {
        let sql = format!("INSERT INTO docs (_id) VALUES (1) RETURNING {returning}");
        assert!(parse(&sql).is_err(), "{sql}");
    }
    for sql in ["ERASE FROM docs RETURNING *", "PATCH INTO docs RECORDS {_id:1} RETURNING *"] { assert!(parse(sql).is_err(), "{sql}"); }
    let sql = "INSERT INTO docs (_id) VALUES (1) RETURNING x WHERE TRUE";
    let error = parse(sql).unwrap_err();
    assert_eq!(error.context["span"]["start"], serde_json::json!(sql.find("WHERE").unwrap()));
}

#[test]
fn ignored_returning_expressions_still_require_sql_syntax() {
    for suffix in [
        "CAST(missing AS BLOB)",
        "TRY_CAST(missing AS INT)",
        "CAST(missing AS INT(10))",
        "missing::BLOB",
        "TIMESTAMP(3) '2000-01-01'",
        "unknown_function(CAST(missing AS BLOB))",
        "(SELECT CAST(missing AS BLOB) FROM absent)",
        "(SELECT _id FROM absent FOR VALID_TIME AS OF CAST(missing AS BLOB))",
        "(SELECT _id FROM absent FOR VALID_TIME FROM DATE '2000-01-01')",
        "(SELECT _id FOR ALL VALID_TIME FROM absent)",
        "(SELECT _id FROM absent AS z FOR ALL VALID_TIME)",
        "(SELECT x FROM absent AS l JOIN absent AS r)",
        "(SELECT x FROM absent AS l JOIN absent AS r USING (l.x))",
        "(SELECT x FROM (VALUES (1)) AS v(x INT))",
        "(SELECT TOP 1 x FROM absent)",
        "(SELECT DISTINCT ON (x) x FROM absent)",
        "(SELECT x FROM absent FETCH FIRST 1 ROWS WITH TIES)",
        "(RECORDS ({a: 1}))",
        "1 <=> 2",
        "SUBSTRING('abc')",
        "TRIM('abc', 'a')",
        "COALESCE()",
        "COUNT(ALL *)",
        "ARRAY_AGG(DISTINCT missing)",
        "ABS('abc' USING OCTETS)",
        "unknown_function(missing) OVER ()",
        "LAG(missing, ?) OVER ()",
    ] {
        let sql = format!("INSERT INTO docs (_id) VALUES (1) RETURNING {suffix}");
        let error = parse(&sql).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error", "{sql}");
        assert!(prepare_static(&sql, &Fixture, &PrepareOptions::default()).is_err(), "{sql}");
    }

    let sql = "DELETE FROM docs RETURNING CAST(missing AS INT), missing::TEXT, \
        COUNT(*), LAG(missing, 1) OVER (), CARDINALITY(ARRAY[missing]), SUBSTRING('abc' FROM 1), \
        ABS(9223372036854775808), (SELECT a FROM absent FOR VALID_TIME FROM ? TO $7), \
        (SELECT missing FROM absent AS l LEFT JOIN absent AS r USING (\"where\"))";
    let prepared = prepare(sql, &Fixture, &PrepareOptions::default()).unwrap();
    assert!(prepared.parameter_types.is_empty());
    assert!(matches!(prepared.statement, Statement::Dml(_)));
}
