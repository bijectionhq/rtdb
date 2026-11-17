use std::collections::BTreeSet;
use rtdb_plan::{Catalog,PatchEndpoint,QualifiedTable,Relation,RelationNode,TableBinding};
use rtdb_sql::{Mutation,PrepareOptions,prepare_static,prepare_tx};
use rtdb_types::Result;

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(vec![]) }
}
fn check(input: &Relation) -> usize {
    let here = if let RelationNode::PatchGaps { from,to,.. } = input.node() {
        PatchEndpoint::from_expr(from).unwrap();PatchEndpoint::from_expr(to).unwrap();1
    } else { 0 };
    here + input.children().iter().map(|child|check(child)).sum::<usize>()
}

#[test]
fn patch_lowering_preserves_direct_endpoint_forms_and_rejects_scalar_operators() {
    for clause in ["","FOR VALID_TIME FROM NULL TO NULL","FOR VALID_TIME FROM FALSE TO FALSE","FOR VALID_TIME FROM CURRENT_TIMESTAMP","FOR VALID_TIME FROM NOW()","FOR VALID_TIME FROM CURRENT_TIMESTAMP() TO NOW()","FOR VALID_TIME FROM DATE '2020-01-01' TO TIMESTAMP '2021-01-01Z'","FOR VALID_TIME FROM $1 TO $2"] {
        let sql = format!("PATCH INTO docs {clause} RECORDS {{_id: 1, v: 2}}");
        let Mutation::Put { input,.. } = prepare_tx(&sql,&EmptyCatalog,&PrepareOptions::default()).unwrap_or_else(|error| panic!("{sql}: {error:?}")) else { panic!() };
        assert_eq!(check(&input),1,"{sql}");
    }
    for bound in ["CURRENT_TIMESTAMP(6)","CURRENT_DATE","COALESCE($1, TIMESTAMP '2020-01-01Z')","CAST('2020-01-01Z' AS TIMESTAMP)","CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE)","CAST(NOW() AS TIMESTAMP)","'2020-01-01Z'","1","TIMESTAMP '2020-01-01Z' + INTERVAL '1' DAY"] {
        let sql = format!("PATCH INTO docs FOR VALID_TIME FROM {bound} RECORDS {{_id: 1, v: 2}}");
        assert_eq!(prepare_tx(&sql,&EmptyCatalog,&PrepareOptions::default()).unwrap_err().code,"rtdb.plan/patch-endpoint","{sql}");
    }
}

#[test]
fn static_patch_retains_its_distinct_constant_expansion_path() {
    for bound in ["'2020-01-01Z'","CAST('2020-01-01Z' AS TIMESTAMP)","$1","NULL","FALSE"] {
        let sql = format!("PATCH INTO docs FOR VALID_TIME FROM {bound} RECORDS {{_id: 1, v: 2}}");
        assert!(prepare_static(&sql,&EmptyCatalog,&PrepareOptions::default()).unwrap().is_some(),"{sql}");
    }
}

#[test]
fn patch_all_valid_time_is_rejected_before_static_or_dynamic_planning() {
    for clause in ["FOR ALL VALID_TIME", "FOR VALID_TIME ALL"] {
        let sql = format!("PATCH INTO docs {clause} RECORDS {{_id: 1}}");
        assert_eq!(rtdb_sql::parse(&sql).unwrap_err().code,"xtdb/sql-error");
        assert!(prepare_static(&sql,&EmptyCatalog,&PrepareOptions::default()).is_err());
        assert!(prepare_tx(&sql,&EmptyCatalog,&PrepareOptions::default()).is_err());
    }
    assert!(rtdb_sql::parse("SELECT * FROM docs FOR ALL VALID_TIME").is_ok());
}
