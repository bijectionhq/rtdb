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
async fn regex_search_uses_three_arguments_and_literal_flag_metadata() {
    for (source,expected) in [
        (r#"(like-regex "A" "a" "i")"#,Value::Bool(true)),
        (r#"(like-regex "A" "a" (str "i"))"#,Value::Bool(false)),
        (r#"(like-regex "A" "a" (cast (/ 1 0) #xt/type :utf8))"#,Value::Bool(false)),
        (r#"(like-regex nil "[")"#,Value::Null),
        (r#"(like-regex (cast (/ 1 0) #xt/type :utf8) "[" nil)"#,Value::Null),
    ] { assert_eq!(project(source).await.unwrap().1,expected,"{source}"); }
    assert_eq!(project(r#"(like-regex (cast (/ 1 0) #xt/type :utf8) "[" "")"#).await.unwrap_err().code,"xtdb.expression/illegal-argument-exception");
    assert_eq!(project(r#"(like-regex "A" "[" (if (= (/ 1 0) 0) "i" ""))"#).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    assert!(project(r#"(like-regex "a" "a")"#).await.is_err());
    assert!(project(r#"(like-regex "a" "a" 1)"#).await.is_err());
}
