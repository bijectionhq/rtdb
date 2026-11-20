use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_static_cell};
use rtdb_plan::{ColumnId, Expr, Field, Schema};
use rtdb_types::{CivilDate, DateUnit, LogicalType, MemoryBudget, ScalarType, Value};

#[test]
fn static_cells_match_expression_values_and_date_errors() {
    let memory = MemoryBudget::new(1 << 24);
    let scope = EvaluationScope::new(&memory).unwrap();
    let parameters = [Value::I64(7), Value::Utf8("test".into()), Value::Null,
                      Value::calendar_date(CivilDate::MAX, DateUnit::Day)];
    let context = EvalContext { parameters: &parameters, allocation: Some(&scope), ..Default::default() };
    let schema = Schema::new(vec![Field {
        id: ColumnId(1), name: "day".into(), qualifier: None, qualified_only: false, unnamed: false,
        data_type: LogicalType::Scalar(ScalarType::DateDay),
    }]).unwrap();
    let row = [Value::DateDay(19_000)];
    let mut expressions: Vec<_> = parameters.iter().enumerate().map(|(index, value)| {
        Expr::Parameter { index, data_type: value.logical_type() }
    }).collect();
    expressions.extend([
        Expr::Literal(Value::F64(1.5)),
        Expr::Literal(Value::List(vec![Value::I64(1)])),
        schema.fields()[0].expression(),
        Expr::Parameter { index: parameters.len(), data_type: LogicalType::Nothing },
    ]);
    for expression in expressions {
        let expected = evaluate(&expression, &schema, &row, &context);
        let actual = evaluate_static_cell(&expression, &schema, &row, &context);
        assert_eq!(actual, expected, "{expression:?}");
    }
}

#[test]
fn static_cell_rejects_a_parameter_schema_with_the_wrong_width() {
    let parameters = [Value::I64(42)];
    let context = EvalContext { parameters: &parameters, parameter_types: Some(&[]), ..Default::default() };
    let expression = Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::I64) };
    let schema = Schema::default();
    assert!(evaluate_static_cell(&expression, &schema, &[], &context).is_err());
}
