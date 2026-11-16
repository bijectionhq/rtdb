use std::collections::BTreeSet;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{DecimalValue, LogicalType as T, Result, ScalarType as S, Value as V};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

fn project(expression: &str) -> (T, V) {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
    let expression = &expressions[0].expr;
    (expression.data_type(), evaluate(expression, input.schema(), &[], &EvalContext::default()).unwrap())
}

fn decimal(coefficient: i128, precision: u8, scale: i8) -> V {
    V::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap())
}

#[test]
fn expression_test_cast_list_elementwise_and_pg_array_text() {
    for (expression, expected) in [
        ("(cast [1 nil 3] #xt/type [:list :i64])", V::List(vec![V::I64(1), V::Null, V::I64(3)])),
        ("(cast [1 2] #xt/type [:list :utf8])", V::List(vec![V::Utf8("1".into()), V::Utf8("2".into())])),
        ("(cast [[1.0 2.0] [3.0]] #xt/type [:list [:list :i64]])", V::List(vec![V::List(vec![V::I64(1), V::I64(2)]), V::List(vec![V::I64(3)])])),
        ("(cast \"{1,NULL}\" #xt/type [:list :i64])", V::List(vec![V::I64(1), V::Null])),
        ("(cast [] #xt/type [:list :i64])", V::List(vec![])),
        ("(cast nil #xt/type [:list :i64])", V::Null),
    ] { assert_eq!(project(expression).1, expected, "{expression}"); }
}

#[test]
fn expression_test_cast_list_applies_element_cast_opts() {
    assert_eq!(project("(cast (cast [1.5] #xt/type [:list [:decimal 64 9 256]]) #xt/type [:list [:decimal 64 9 256]] {:precision 10 :scale 2})"),
        (T::list(T::Scalar(S::Decimal { precision: 10, scale: 2, bit_width: 128 })), V::List(vec![decimal(150, 10, 2)])));
    assert_eq!(project("(cast [[1.245M nil] []] #xt/type [:list [:list [:decimal 64 9 256]]] {:precision 5 :scale 2})").1,
        V::List(vec![V::List(vec![decimal(124, 5, 2), V::Null]), V::List(vec![])]));
}

#[test]
fn unspecified_decimal_cast_preserves_literal_scale_and_scale_only_options() {
    for expression in ["(cast 1.25M #xt/type [:decimal 64 9 256])", "(cast 1.25M #xt/type [:decimal 64 9 256] {:scale 0})", "(cast 1.25M #xt/type [:decimal 64 9 256] {:scale :ignored})"] {
        assert_eq!(project(expression), (T::Scalar(S::Decimal { precision: 32, scale: 2, bit_width: 128 }), decimal(125, 32, 2)));
    }
    assert_eq!(project("(cast 125 #xt/type [:decimal 64 9 256] {:precision 3 :scale -1})").1, decimal(12, 3, -1));
}

#[test]
fn casts_retain_unification_dependencies_and_aggregate_rewriting() {
    for source in [
        "(unify (with {:v (cast x #xt/type [:decimal 64 9 256])}) (rel [{:x 1.25M}] [x]))",
        "(-> (rel [{:x 1} {:x 2}] [x]) (aggregate {:total (cast (sum x) #xt/type [:decimal 64 9 256] {:precision 5 :scale 2})}))",
    ] {
        assert!(compile(source, &EmptyCatalog, &CompileOptions::default()).is_ok(), "{source}");
    }
}
