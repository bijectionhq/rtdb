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
async fn nth_uses_signed_integer_indexes_and_constant_null_nonlists() {
    assert_eq!(project("(nth [4 5] (cast 1 #xt/type :i16))").await.unwrap().1,Value::I64(5));
    assert_eq!(project("(nth [4 5] nil)").await.unwrap().1,Value::Null);
    assert_eq!(project("(nth nil 1.0)").await.unwrap().1,Value::Null);
    assert_eq!(project("(character-length (nth 7 0))").await.unwrap().1,Value::Null);
    for source in ["(nth [4 5] 1.0)","(nth 7 1.0)","(nth [4 5] true)","(nth [4 5] \"1\")"] {
        assert_eq!(project(source).await.unwrap_err().code,"xtdb.expression/function-type-mismatch","{source}");
    }
}
