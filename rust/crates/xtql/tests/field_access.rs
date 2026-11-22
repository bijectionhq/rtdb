use rtdb_types::StructFields;
use std::collections::BTreeSet;
use rtdb_expr::{EvalContext,evaluate_async};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_msgpack};
use rtdb_plan::{Catalog,QualifiedTable,RelationNode,Statement,TableBinding};
use rtdb_types::{Result,Value as V};
use rtdb_xtql::{CompileOptions,compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

async fn project(expression: &str,argument: V,scope: &rtdb_expr::EvaluationScope) -> Result<V> {
    let source = format!("(fn [x] (-> (rel [{{}}] []) (return {{:v {expression}}})))");
    let options = CompileOptions { parameter_types: vec![argument.logical_type()],..Default::default() };
    let Statement::Query(relation) = compile(&source,&EmptyCatalog,&options)?.statement else { panic!("query") };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!("projection") };
    evaluate_async(&expressions[0].expr,input.schema(),&[],&EvalContext { parameters: rtdb_expr::Parameters::Values(&[argument]),allocation: Some(scope),..Default::default() }).await
}

#[tokio::test]
async fn xtql_named_fields_and_generic_calls_preserve_their_distinct_field_metadata() {
    let memory = rtdb_types::MemoryBudget::new(1 << 24);
    let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    for (source,expected) in [
        ("(. {:a false} a)",V::Bool(false)),("(. (/ 1 0) absent)",V::Null),
        ("(get-field (/ 1 0))",V::Null),
    ] { assert_eq!(project(source,V::Null,&scope).await.unwrap(),expected,"{source}"); }
    assert_eq!(project("(. {:a (/ 1 0)} absent)",V::Null,&scope).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(project("(get_field x)",V::Struct(StructFields::from([("".into(),V::I64(4))])),&scope).await.unwrap(),V::I64(4));
    assert_eq!(project("(. {:Foo-Bar 7} Foo-Bar)",V::Null,&scope).await.unwrap(),V::Null);
    assert_eq!(project("(. {:Foo-Bar 7} foo_bar)",V::Null,&scope).await.unwrap(),V::I64(7));
    for source in ["(get-field)","(get-field x x)"] { assert!(project(source,V::Null,&scope).await.is_err(),"{source}"); }
    for source in ["(. nil a)","(. false a)","(. x :a)"] { assert!(project(source,V::Null,&scope).await.is_err(),"{source}"); }
    let object = T::Map(vec![(T::Keyword("a".into()),T::Bool(false)),(T::String("a".into()),T::Integer(5)),(T::Null,T::Integer(6)),(T::Keyword("tx-id".into()),T::Integer(7))]);
    for (source,expected) in [("(. x a)",5),("(get_field x)",6),("(. x tx-id)",7)] {
        let V::Transit(bytes) = project(source,V::Transit(encode_msgpack(&object).unwrap()),&scope).await.unwrap() else { panic!("Transit") };
        assert_eq!(decode_msgpack(&bytes).unwrap(),T::Integer(expected));
    }
}

#[tokio::test]
async fn xtql_transit_fields_write_registered_scalar_objects_after_construction() {
    let memory = rtdb_types::MemoryBudget::new(1 << 24);
    let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
    let tag = |name: &str,value|T::Tagged { tag: name.into(),representation: Box::new(value) };
    let object = T::Map(vec![(T::Keyword("a".into()),tag("f32",T::Integer(16_777_217)))]);
    let V::Transit(bytes) = project("(. x a)",V::Transit(encode_msgpack(&object).unwrap()),&scope).await.unwrap() else { panic!("Transit") };
    assert_eq!(decode_msgpack(&bytes).unwrap(),T::Float(16_777_216.0));
    let object = T::Map(vec![(T::Keyword("a".into()),T::Integer(7)),(T::Keyword("ignored".into()),tag("r",T::String("http://[".into())))]);
    assert!(project("(. x a)",V::Transit(encode_msgpack(&object).unwrap()),&scope).await.is_err());
}
