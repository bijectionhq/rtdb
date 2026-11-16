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

fn decimal(coefficient: i128, precision: i32, scale: i32) -> V {
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

#[test]
fn decimal_literals_casts_and_round_keep_signed32_scale() {
    for (expression, coefficient, scale) in [
        ("1E-2147483647M", 1, i32::MAX),
        ("1E+2147483648M", 1, i32::MIN),
        ("(cast 1E-1000M #xt/type [:decimal 64 9 256] {:precision 32 :scale 1000})", 1, 1000),
        ("(cast 0 #xt/type [:decimal 32 -2147483648 128] {:precision 32 :scale -2147483648})", 0, i32::MIN),
        ("(round 1.25E-1000M 1001)", 13, 1001),
    ] {
        assert_eq!(project(expression), (T::Scalar(S::Decimal { precision: 32, scale, bit_width: 128 }), decimal(coefficient, 32, scale)), "{expression}");
    }
    assert_eq!(project("(cast [1E-1000M 2E-1000M] #xt/type [:list [:decimal 64 9 256]] {:precision 32 :scale 1000})").1,
        V::List(vec![decimal(1, 32, 1000), decimal(2, 32, 1000)]));
}

#[test]
fn java_unicode_decimal_string_casts_keep_clojure_ascii_literal_grammar() {
    assert_eq!(project("(cast \"١२.３४\" #xt/type [:decimal 32 2 128] {:precision 32 :scale 2})").1, decimal(1234, 32, 2));
    assert_eq!(project("(cast \"١E+٢١٤٧٤٨٣٦٤٨\" #xt/type [:decimal 32 -2147483648 128] {:precision 32 :scale -2147483648})").1, decimal(1, 32, i32::MIN));
    for source in ["١M", "+१M", "1.٢M", "1e२M", "1M.0", "1eM", "1_0M"] {
        assert!(rtdb_xtql::read(source).is_err(), "{source}");
    }
    assert!(matches!(rtdb_xtql::read("𝟙M").unwrap().kind, rtdb_xtql::EdnKind::Symbol(_)));
    for source in ["1M", "+1.M", "1.e2M", "1E+2147483648M"] {
        assert!(matches!(rtdb_xtql::read(source).unwrap().kind, rtdb_xtql::EdnKind::Decimal(_)), "{source}");
    }
}

#[test]
fn decimal_inference_keeps_32_and_64_digit_classes_and_signed_cast_metadata() {
    for (digits, precision, bit_width) in [(32, 32, 128), (33, 64, 256), (64, 64, 256)] {
        let expression = format!("{}M", "9".repeat(digits));
        let (logical, value) = project(&expression);
        assert_eq!(logical, T::Scalar(S::Decimal { precision, scale: 0, bit_width }));
        let V::Decimal(value) = value else { panic!("Expected decimal") };
        assert_eq!((value.precision, value.scale, value.bit_width), (precision, 0, bit_width));
    }
    let source = format!("(-> (rel [{{}}] []) (return {{:v {}M}}))", "9".repeat(65));
    assert!(compile(&source, &EmptyCatalog, &CompileOptions::default()).is_err());
    for precision in [i32::MIN, -1, 0] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v (cast 0 #xt/type [:decimal 2147483647 -2147483648 128] {{:precision {precision} :scale 0}})}}))");
        let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
        let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
        assert_eq!(expressions[0].expr.data_type(), T::Scalar(S::Decimal { precision, scale: 0, bit_width: 128 }));
        assert!(evaluate(&expressions[0].expr, input.schema(), &[], &EvalContext::default()).is_err());
        let null_source = source.replace("(cast 0 ", "(cast nil ");
        let Statement::Query(relation) = compile(&null_source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
        let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
        assert_eq!(evaluate(&expressions[0].expr, input.schema(), &[], &EvalContext::default()).unwrap(), V::Null);
    }
}
