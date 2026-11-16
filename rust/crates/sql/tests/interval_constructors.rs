use std::collections::BTreeSet;
use rtdb_plan::{Catalog,QualifiedTable,TableBinding};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_types::Result;

struct Empty;
impl Catalog for Empty {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
#[test]
fn sql_interval_grammar_does_not_admit_internal_constructor_names() {
    for expression in ["single_field_interval(1,'DAY',2,6)","multi_field_interval('1:02','HOUR',2,'MINUTE',6)"] {
        for source in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            assert!(prepare(&source,&Empty,&PrepareOptions::default()).is_err(),"{source}");
        }
    }
    prepare("SELECT INTERVAL '1' DAY, INTERVAL '1:02' HOUR TO MINUTE",&Empty,&PrepareOptions::default()).unwrap();
}
