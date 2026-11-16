use std::collections::BTreeSet;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{LogicalType as L, Result, ScalarType as S, Value as V};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
fn project(expression: &str) -> Result<(L,V)> {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default())?.statement else { panic!("query") };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!("projection") };
    let expression = &expressions[0].expr;
    Ok((expression.data_type(),evaluate(expression,input.schema(),&[],&EvalContext::default())?))
}

#[test]
fn xtql_uri_literals_casts_and_extractors_preserve_the_java_component_contract() {
    let uri = "https://user%3Apass@EXAMPLE.com:80/path%20name?q=%ED%A0%80+#";
    for (name,expected) in [("uri-scheme",V::Utf8("https".into())),("uri-user-info",V::Utf8("user:pass".into())),("uri-host",V::Utf8("EXAMPLE.com".into())),("uri-port",V::I32(80)),("uri-path",V::Utf8("/path name".into())),("uri-query",V::Utf8("q=�+".into())),("uri-fragment",V::Utf8("".into()))] {
        let (ty,value) = project(&format!("({name} #xt/uri {uri:?})")).unwrap();
        assert_eq!(value,expected);
        assert_eq!(ty,L::Scalar(if name == "uri-port" { S::I32 } else { S::Utf8 }).maybe());
        assert_eq!(project(&format!("({name} nil)")).unwrap().1,V::Null);
        assert!(project(&format!("({name} {uri:?})")).is_err());
    }
    assert_eq!(project(&format!("(cast (cast {uri:?} #xt/type :uri) #xt/type :utf8)")).unwrap().1,V::Utf8(uri.into()));
    for expression in ["#xt/uri \"bad space\"", "(cast \"bad space\" #xt/type :uri)", "(uri-host #xt/uri \"http://[bad]\")", "(uri-path)", "(uri-path nil nil)"] { assert!(project(expression).is_err(),"{expression}"); }
}

#[test]
fn xtql_uri_equality_and_nested_ascii_rendering_follow_the_object_contract() {
    assert_eq!(project("(= #xt/uri \"HTTP://EXAMPLE.com/%ab\" #xt/uri \"http://example.com/%AB\")").unwrap().1,V::Bool(true));
    assert_eq!(project("(str [#xt/uri \"/é\"])").unwrap().1,V::Utf8("[#xt/uri \"/%C3%A9\"]".into()));
    assert!(project("(< #xt/uri \"a\" #xt/uri \"b\")").is_err());
}
