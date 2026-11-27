use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::{MemoryBudget, OwnedBatch};
use rtdb_expr::{ArrowParameterCell, EvalContext, ParameterLookup, Parameters};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, Schema};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;

use crate::{
    CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions,
    columnar::{Column, ColumnBatch, SelectedColumn, admitted_predicate_selection, functions::Predicate},
    context::{Execution, OuterRows},
    parameters::BoundParameters,
};

fn input(scalar: ScalarType) -> Field {
    Field {
        id: ColumnId(0), name: "x".into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: LogicalType::Scalar(scalar).maybe(),
    }
}

fn column(field: &Field, array: ArrayRef) -> Column {
    Column::Arrow {
        array, field: Arc::new(rtdb_columnar::to_field(&field.name, &field.data_type).unwrap()),
        logical: Some(field.data_type.clone()),
    }
}

fn between(field: &Field, low: Expr, high: Expr) -> Expr {
    Expr::Between {
        operand: Box::new(field.expression()), low: Box::new(low), high: Box::new(high), symmetric: false,
    }
}

fn check(
    predicate: &Predicate, expr: &Expr, schema: &Schema, batch: &ColumnBatch, context: &EvalContext<'_>,
    expected: &[Option<bool>],
) {
    assert_eq!(batch.len, expected.len());
    let selection = predicate.selection(batch, context.parameters, batch.reservation.budget(), || Ok(()))
        .unwrap().unwrap();
    for (row, truth) in expected.iter().enumerate() {
        assert_eq!(selection.contains(row), *truth == Some(true), "row {row}");
    }
    assert_eq!(predicate.truth(batch, context.parameters).unwrap().unwrap(), expected);
    let scalar = (0..batch.len).map(|row| {
        let value = batch.columns[0].value(row).unwrap();
        match rtdb_expr::evaluate(expr, schema, &[value], context).unwrap() {
            Value::Bool(value) => Some(value),
            Value::Null => None,
            other => panic!("expected SQL truth, got {other:?}"),
        }
    }).collect::<Vec<_>>();
    assert_eq!(scalar, expected);
}

#[test]
fn prepared_arrow_bounds_can_change_without_rebinding_the_range_layout() {
    let memory = MemoryBudget::new(1 << 20);
    let field = input(ScalarType::I64);
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let base = 9_007_199_254_740_992_i64;
    let values = Arc::new(Int64Array::from(vec![None, Some(base), Some(base + 1), Some(base + 2)]));
    let batch = ColumnBatch::settled(vec![column(&field, values)], 4, memory.reserve(0).unwrap()).unwrap();
    let bound = |index| Expr::Parameter { index, data_type: field.data_type.clone() };
    let range = Expr::binary(
        BinaryOp::And,
        Expr::binary(BinaryOp::Gt, bound(1), field.expression()),
        Expr::binary(BinaryOp::GtEq, field.expression(), bound(0)),
    );
    let compiled = Predicate::bind(&range, &schema).unwrap();
    let arguments = RecordBatch::try_from_iter([
        ("low", Arc::new(Int64Array::from(vec![Some(base + 1), Some(base + 2), None])) as ArrayRef),
        ("high", Arc::new(Int64Array::from(vec![Some(base + 2), Some(base + 1), Some(base + 2)])) as ArrayRef),
    ]).unwrap();
    for (row, expected) in [
        [None, Some(false), Some(true), Some(false)],
        [None, Some(false), Some(false), Some(false)],
        [None, None, None, Some(false)],
    ].into_iter().enumerate() {
        let args = OwnedBatch::new(arguments.clone(), &memory).unwrap();
        let parameters = BoundParameters::from_arrow(args, row, vec![field.data_type.clone(); 2], &memory).unwrap();
        let context = EvalContext {
            parameters: parameters.evaluator(), parameter_types: Some(parameters.types()), ..EvalContext::default()
        };
        check(&compiled, &range, &schema, &batch, &context, &expected);
    }
}

#[test]
fn selected_float_ranges_keep_signed_zero_order_and_unknown_separate() {
    let memory = MemoryBudget::new(1 << 20);
    let field = input(ScalarType::F64);
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let arrow = Arc::new(ArrowField::new("x", DataType::Float64, true));
    let selected = SelectedColumn::new(
        vec![
            Some((Arc::new(Float64Array::from(vec![Some(0.0), Some(-0.0), Some(f64::NAN), None])),
                Arc::clone(&arrow), Some(field.data_type.clone()), None)),
            None,
        ],
        Arc::new(vec![(0, 1), (0, 0), (0, 2), (0, 3), (1, 0)]),
    );
    let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 5, memory.reserve(0).unwrap()).unwrap();
    let zero = |value| Expr::Literal(Value::F64(value));
    for (low, high, expected) in [
        (zero(-0.0), zero(-0.0), [Some(true), Some(false), Some(false), None, None]),
        (zero(0.0), zero(-0.0), [Some(false), Some(false), Some(false), None, None]),
        (Expr::Literal(Value::Null), zero(-0.0), [None, Some(false), Some(false), None, None]),
        (zero(-0.0), zero(f64::NAN), [Some(false), Some(false), Some(false), None, None]),
    ] {
        let range = between(&field, low, high);
        let compiled = Predicate::bind(&range, &schema).unwrap();
        check(&compiled, &range, &schema, &batch, &EvalContext::default(), &expected);
    }
}

#[test]
fn a_bound_error_requires_a_reached_comparison_operand() {
    struct FailingBound;
    impl ParameterLookup for FailingBound {
        fn width(&self) -> usize { 1 }
        fn value(&self, _: usize) -> Result<Option<&Value>> {
            Err(Anomaly::fault("test/reached-bound", "Reached an unreadable parameter"))
        }
        fn known_value(&self, _: usize) -> Option<&Value> { None }
        fn arrow_cell(&self, _: usize) -> Option<ArrowParameterCell<'_>> { None }
    }

    let memory = MemoryBudget::new(1 << 20);
    let field = input(ScalarType::I64);
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let parameter = Expr::Parameter { index: 0, data_type: field.data_type.clone() };
    let parameters = Parameters::Lookup(&FailingBound);
    let values = Arc::new(Int64Array::from(vec![None, Some(0)]));
    let batch = ColumnBatch::settled(vec![column(&field, values)], 2, memory.reserve(0).unwrap()).unwrap();
    let short_circuited = between(&field, Expr::Literal(Value::I64(1)), parameter.clone());
    let unknown = between(&field, Expr::Literal(Value::Null), parameter.clone());
    let bound_before_column = Expr::binary(
        BinaryOp::And,
        Expr::binary(BinaryOp::Gt, parameter, field.expression()),
        Expr::binary(BinaryOp::GtEq, field.expression(), Expr::Literal(Value::I64(1))),
    );
    let null = ColumnBatch::settled(
        vec![column(&field, Arc::new(Int64Array::from(vec![None])))], 1, memory.reserve(0).unwrap(),
    ).unwrap();
    let selection = |expr, batch| Predicate::bind(expr, &schema).unwrap().selection(batch, parameters, &memory, || Ok(()));
    assert!(selection(&short_circuited, &batch).unwrap().unwrap().is_empty());
    assert!(selection(&unknown, &null).unwrap().unwrap().is_empty());
    assert_eq!(selection(&unknown, &batch).err().unwrap().code, "test/reached-bound");
    assert_eq!(selection(&bound_before_column, &null).err().unwrap().code, "test/reached-bound");
}

#[test]
fn a_range_selection_fits_with_only_bitmap_space_left() {
    let memory = MemoryBudget::new(1 << 20);
    let field = input(ScalarType::I64);
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let len = 4096;
    let values = Arc::new(Int64Array::from_iter_values(0..len as i64));
    let batch = ColumnBatch::settled(vec![column(&field, values)], len, memory.reserve(0).unwrap()).unwrap();
    let predicate = Expr::between(field.expression(), Expr::Literal(Value::I64(127)), Expr::Literal(Value::I64(129)), false);
    let compiled = Predicate::bind(&predicate, &schema).unwrap();
    let _remaining = memory.reserve(memory.limit_bytes() - memory.used_bytes() - 1024).unwrap();
    let selection = compiled.selection(&batch, Parameters::Values(&[]), &memory, || Ok(())).unwrap().unwrap();
    assert_eq!((0..len).filter(|&row| selection.contains(row)).collect::<Vec<_>>(), [127, 128, 129]);
}

#[tokio::test]
async fn a_declined_physical_layout_keeps_scalar_range_selection() {
    let memory = MemoryBudget::new(1 << 20);
    let database = DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(
        catalog, Arc::clone(&memory), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new(),
    ).unwrap();
    let execution = Execution {
        parameters: Arc::new(BoundParameters::infer(vec![], &memory).unwrap()),
        outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
    };
    let field = input(ScalarType::I64);
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let batch = ColumnBatch::settled(vec![Column::Arrow {
        array: Arc::new(UInt64Array::from(vec![None, Some(u64::MAX), Some(0), Some(1)])),
        field: Arc::new(ArrowField::new("x", DataType::UInt64, true)),
        logical: None,
    }], 4, memory.reserve(0).unwrap()).unwrap();
    let predicate = between(&field, Expr::Literal(Value::I64(-1)), Expr::Literal(Value::I64(0)));
    let compiled = Predicate::bind(&predicate, &schema).unwrap();
    assert!(compiled.selection(&batch, Parameters::Values(&[]), &memory, || Ok(())).unwrap().is_none());
    let selection = admitted_predicate_selection(&batch, &predicate, &schema, &execution, Some(&compiled), None).await.unwrap();
    assert_eq!((0..batch.len).filter(|&row| selection.contains(row)).collect::<Vec<_>>(), [1, 2]);
}
