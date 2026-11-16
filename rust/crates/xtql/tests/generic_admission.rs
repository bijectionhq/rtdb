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
async fn generic_names_follow_registered_methods_and_strict_null_specializations() {
    for name in ["trunc","atan2","degrees","radians","uuid","uri","substr","char-length","bit-length","boolean?","number?","integer?","float?","string?","keyword?","uuid?","list?","set?","map?","unregistered-fn"] {
        assert_eq!(project(&format!("({name} 1)")).await.unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
        assert_eq!(project(&format!("({name} nil)")).await.unwrap().1,Value::Null,"{name}");
        assert_eq!(project(&format!("({name} (/ 1 0) nil)")).await.unwrap().1,Value::Null,"{name}");
    }
    assert_eq!(project("(character-length \"abc\")").await.unwrap().1,Value::I32(3));
}
