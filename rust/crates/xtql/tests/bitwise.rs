use std::collections::BTreeSet;
use rtdb_expr::{EvalContext, evaluate_async};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{Result, Value as V};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
async fn project(expression: &str) -> Result<V> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default())?.statement else { panic!("query") };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!("projection") };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).await
}

#[tokio::test]
async fn xtql_bitwise_functions_share_signed_scalar_dispatch_and_null_demand() {
    for (source,expected) in [
        ("(bit-not 0)",V::I64(-1)),("(bit-and 6 3)",V::I64(2)),("(bit-or 6 3)",V::I64(7)),
        ("(bit-xor 6 3)",V::I64(5)),("(bit-shift-left 1 -1)",V::I64(i64::MIN)),("(bit-shift-right -4 1)",V::I64(-2)),
        ("(bit-and (/ 1 0) nil)",V::Null),("(bit-shift-left (cast 127 #xt/type :i8) 1)",V::I8(-2)),
    ] { assert_eq!(project(source).await.unwrap(),expected,"{source}"); }
    for source in ["(bit-not)","(bit-and 1)","(bit-or 1 2 3)","(bit-xor true 1)","(bit-shift-right 1.0 2)"] { assert!(project(source).await.is_err(),"{source}"); }
    assert_eq!(project("(bit-shift-left (cast 1 #xt/type :i8) 2147483648)").await.unwrap_err().code,"xtdb.expression/overflow-error");
}
