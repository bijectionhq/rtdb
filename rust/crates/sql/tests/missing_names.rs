use std::collections::{BTreeMap, BTreeSet};
use rtdb_plan::{Catalog, QualifiedTable, Statement, TableBinding};
use rtdb_sql::{PrepareOptions, prepare};
use rtdb_types::{LogicalType, Result, ScalarType};

struct CatalogFixture;
impl Catalog for CatalogFixture {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok((name.database == "xtdb" && name.table.schema == "public" && name.table.table == "docs").then(|| TableBinding {
            name:name.clone(),oid:Some(16384),columns:BTreeMap::from([("_id".into(),LogicalType::Scalar(ScalarType::I64))]),
        }))
    }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

#[test]
fn missing_tables_fail_before_an_empty_sentinel_plan_can_escape() {
    for sql in ["SELECT * FROM missing", "SELECT 1 FROM missing", "SELECT absent FROM missing", "SELECT COUNT(*) FROM missing", "SELECT * FROM public.missing"] {
        let error = prepare(sql,&CatalogFixture,&PrepareOptions::default()).unwrap_err();
        assert_eq!(error.code,"xtdb/table-not-found","{sql}");
    }
}

#[test]
fn unresolved_columns_are_errors_in_projection_filters_groups_having_and_window_scopes() {
    for sql in [
        "SELECT x FROM docs", "SELECT d.x FROM docs AS d", "SELECT _id AS s FROM docs WHERE x>1",
        "SELECT SUM(_id) AS s FROM docs GROUP BY x", "SELECT SUM(_id) AS s FROM docs HAVING SUM(x)>1",
        "SELECT _id FROM docs ORDER BY x", "SELECT ROW_NUMBER() OVER(PARTITION BY x) FROM docs",
        "SELECT ROW_NUMBER() OVER(ORDER BY x) FROM docs", "SELECT LEAD(x) OVER() FROM docs",
    ] { assert!(prepare(sql,&CatalogFixture,&PrepareOptions::default()).is_err(),"{sql}"); }
}

#[test]
fn a_catalog_declared_table_binds_without_requiring_a_data_row() {
    let Statement::Query(relation) = prepare("SELECT _id FROM docs",&CatalogFixture,&PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    assert_eq!(relation.schema().fields()[0].name,"_id");
    assert_eq!(relation.dependencies()[0].oid,Some(16384));
}
