use std::{collections::BTreeSet};
use rtdb_expr::{EvalContext, evaluate_async};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType, Result, Value};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(expression: &str) -> Result<(LogicalType, Value)> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default())?.statement else { panic!() };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!() };
    let expression = &expressions[0].expr;
    Ok((expression.data_type(), evaluate_async(expression, input.schema(), &[], &EvalContext::default()).await?))
}

#[tokio::test]
async fn xtql_description_and_access_method_property_functions_return_null() {
    for source in ["(col-description 12345 1)", "(pg-indexam-has-property 403 \"can_order\")", "(col_description nil nil)"] {
        assert_eq!(project(source).await.unwrap(), (LogicalType::Null, Value::Null));
    }
}

#[tokio::test(start_paused = true)]
async fn xtql_null_catalogue_calls_discard_payloads_but_keep_control_and_union_demands() {
    let start = tokio::time::Instant::now();
    for source in [
        "(col-description (/ 1 0) 1)", "(pg-indexam-has-property (/ 1 0) \"anything\")",
        "(col-description nil (/ 1 0))", "(col-description (/ 1 0) nil)",
        "(pg-indexam-has-property nil (/ 1 0))", "(pg-indexam-has-property (/ 1 0) nil)",
        "(col-description (sleep (cast 60.0 #xt/type [:duration :milli])) 1)",
        "(pg-indexam-has-property 1 (sleep (cast 60.0 #xt/type [:duration :milli])))",
    ] {
        assert_eq!(project(source).await.unwrap(), (LogicalType::Null,Value::Null), "{source}");
    }
    assert_eq!(tokio::time::Instant::now(), start);
    for source in [
        "(col-description (if (= (/ 1 0) 0) 1 2) 1)",
        "(col-description (if true (/ 1 0) nil) 1)",
        "(pg-indexam-has-property (if true (/ 1 0) nil) \"anything\")",
    ] {
        assert_eq!(project(source).await.unwrap_err().code, "xtdb.expression/division-by-zero", "{source}");
    }
}
