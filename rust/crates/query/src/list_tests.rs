use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, OrdinalityScope, Relation, RelationNode, Schema};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::{Execution, OuterRows}, columnar::{BatchCursor, BoxBatchCursor, ColumnBatch}, operators::build_column_pages, parameters::BoundParameters, runtime_schema::EmittedRelation};

fn execution(batch_rows: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 27), Timestamp(0), "UTC", QueryOptions { batch_rows, list_batch_rows: 3, ..Default::default() }, &CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![], &context.memory).unwrap()), outer: OuterRows::empty(&context.memory).unwrap(), context, bindings: None, analysis: None }
}
fn list(expr: Expr) -> Arc<Relation> {
    let types = expr.data_type().legs().into_iter().map(|ty| match ty { LogicalType::List { element, .. } => *element, _ => LogicalType::Null }).collect::<Vec<_>>();
    let element = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::join_all(&types) };
    Arc::new(Relation::new(Schema::new(vec![element.clone()]).unwrap(), RelationNode::List { expr, element, page_rows: None }).unwrap())
}
fn series(start: i64, end: i64, step: i64, inclusive: bool) -> Expr {
    let args = vec![Expr::Literal(Value::I64(start)), Expr::Literal(Value::I64(end)), Expr::Literal(Value::I64(step)), Expr::Literal(Value::Bool(inclusive))];
    Expr::Call { name: "generate_series".into(), return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(), args }
}
async fn open(relation: Arc<Relation>, execution: &Execution) -> Result<BoxBatchCursor> {
    let runtime = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, execution.parameters.types(), &execution.context.memory)?;
    Ok(build_column_pages(runtime, execution.clone()).await?.unwrap())
}
fn rows(batch: &ColumnBatch) -> Vec<Vec<Value>> {
    (0..batch.len).map(|row| batch.columns.iter().map(|column| column.value(row).unwrap()).collect()).collect()
}
fn numbers(values: &[i64]) -> Vec<Vec<Value>> { values.iter().map(|value| vec![Value::I64(*value)]).collect() }

#[test]
fn direct_unnest_borrows_variable_and_fixed_child_ranges_with_their_original_custody() {
    use arrow_array::{Array, ArrayRef, FixedSizeListArray, Int64Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::{Column, SelectedColumn};

    let execution = execution(1);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let variable_values: ArrayRef = Arc::new(Int64Array::from(vec![Some(70), Some(11), None, Some(99)]));
    let fixed_values: ArrayRef = Arc::new(Int64Array::from(vec![90, 91, 33, 44]));
    let variable = ListArray::new(Arc::clone(&item), OffsetBuffer::new(vec![0_i32, 1, 3, 3, 4].into()),
        Arc::clone(&variable_values), Some(NullBuffer::from(vec![true, true, true, false])));
    let fixed = FixedSizeListArray::new(item, 2, Arc::clone(&fixed_values), None);
    let vector = |array: ArrayRef| {
        let field = Arc::new(ArrowField::new("xs", array.data_type().clone(), array.is_nullable()));
        let ty = rtdb_columnar::from_field(&field).unwrap();
        Some((array, field, Some(ty), None))
    };
    let list = Column::Selected(SelectedColumn::new(vec![vector(Arc::new(variable)), vector(Arc::new(fixed))],
        Arc::new(vec![(0, 1), (0, 2), (0, 3), (1, 1)])));
    let (parent, _charge) = crate::columnar::pack_evaluated_column(&[10, 20, 30, 40].map(Value::I64), memory).unwrap();
    let input = ColumnBatch::settled(vec![parent, list], 4, memory.reserve(8192).unwrap()).unwrap();
    drop(_charge);
    let output = input.expand_list(1, Some("n"), true, context).unwrap().unwrap();
    drop(input);

    let Column::Selected(elements) = &output.columns[2] else { panic!("Expected borrowed child spans"); };
    for (output_rows, source, child_rows) in [(0..2, variable_values, 1..3), (4..6, fixed_values, 2..4)] {
        let ((array, _, _), span) = elements.fragment_source(output_rows).unwrap();
        assert!(Arc::ptr_eq(&array, &source));
        assert_eq!(span, child_rows);
    }
    assert_eq!(output.working_set_bytes(memory).unwrap(), memory.used_bytes() - baseline);
    let selected = output.select(&[true, true, true, true, false, true], memory).unwrap();
    let selected = selected.slice(1, 4, memory).unwrap();
    let actual = rows(&selected).into_iter().map(|row| [row[0].clone(), row[2].clone(), row[3].clone()]).collect::<Vec<_>>();
    assert_eq!(actual, [
        [Value::I64(10), Value::Null, Value::I32(2)],
        [Value::I64(20), Value::Null, Value::Null],
        [Value::I64(30), Value::Null, Value::Null],
        [Value::I64(40), Value::I64(44), Value::I32(2)],
    ]);
    drop(selected);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn filtered_unnest_borrows_selected_arrow_spans_and_keeps_original_ordinals() {
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::Column;
    use rtdb_plan::UnaryOp;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(11), None, Some(13), Some(14), Some(21), Some(51)]));
    let lists = ListArray::new(item, OffsetBuffer::new(vec![0_i32, 4, 5, 5, 6].into()),
        Arc::clone(&values), Some(NullBuffer::from(vec![true, true, true, false])));
    let list_field = Arc::new(ArrowField::new("xs", lists.data_type().clone(), true));
    let int = LogicalType::Scalar(ScalarType::I64);
    let field = |id, name: &str, data_type| Field {
        id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
    };
    let list = field(0, "xs", rtdb_columnar::from_field(&list_field).unwrap());
    let element = field(1, "v", int.clone().maybe());
    let ordinal = field(2, "n", int);
    let predicate = Expr::binary(BinaryOp::Or, Expr::unary(UnaryOp::IsNull, element.expression()),
        Expr::binary(BinaryOp::Eq, ordinal.expression(), Expr::Literal(Value::I64(4))));
    let schema = Schema::new(vec![list, element, ordinal]).unwrap();
    let charge = memory.reserve(lists.get_array_memory_size() + 1024).unwrap();
    let input = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(lists), field: list_field, logical: None }], 4, charge).unwrap();
    let (output, _) = input.expand_filtered_list(0, &schema, &predicate, Some("n"), &execution).await.unwrap().unwrap();
    drop(input);

    let Column::Selected(elements) = &output.columns[1] else { panic!("Expected borrowed child spans"); };
    for (result, input) in [(0..1, 1..2), (1..2, 3..4)] {
        let ((array, _, _), span) = elements.fragment_source(result).unwrap();
        assert!(Arc::ptr_eq(&array, &values));
        assert_eq!(span, input);
    }
    let actual = rows(&output).into_iter().map(|row| [row[1].clone(), row[2].clone()]).collect::<Vec<_>>();
    assert_eq!(actual, [
        [Value::Null, Value::I64(2)], [Value::I64(14), Value::I64(4)],
        [Value::Null, Value::Null], [Value::Null, Value::Null], [Value::Null, Value::Null],
    ]);
    drop(output);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn wide_filtered_unnest_bounds_predicate_scratch_and_keeps_matches_across_chunks() {
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::Column;
    use rtdb_plan::UnaryOp;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let width = 32_768;
    let first_width = width - 1;
    let mut items = vec![Some(0); 2 * width + 1];
    items[first_width + 1024] = Some(7);
    items[first_width + 1025] = None;
    items[first_width + width - 1] = Some(9);
    items[2 * width - 1] = Some(10);
    items[2 * width] = Some(8);
    let values: ArrayRef = Arc::new(Int64Array::from(items));
    let item = Arc::new(ArrowField::new("item", DataType::Int64, true));
    let offsets = [0, 0, first_width, first_width + width, first_width + width, 2 * width + 1]
        .map(|offset| i32::try_from(offset).unwrap());
    let lists = ListArray::new(item, OffsetBuffer::new(offsets.to_vec().into()), Arc::clone(&values),
        Some(NullBuffer::from(vec![true, true, true, false, true])));
    let list_field = Arc::new(ArrowField::new("xs", lists.data_type().clone(), true));
    let field = |id, name: &str, data_type| Field {
        id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
    };
    let int = LogicalType::Scalar(ScalarType::I64);
    let element = field(2, "v", int.clone().maybe());
    let predicate = Expr::binary(BinaryOp::Or, Expr::unary(UnaryOp::IsNull, element.expression()),
        Expr::binary(BinaryOp::GtEq, element.expression(), Expr::Literal(Value::I64(7))));
    let schema = Schema::new(vec![field(0, "xs", rtdb_columnar::from_field(&list_field).unwrap()),
        field(1, "id", int.clone()), element, field(3, "n", int.clone())]).unwrap();
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let charge = memory.reserve(lists.get_array_memory_size() + ids.get_array_memory_size() + 1024).unwrap();
    let input = ColumnBatch::settled(vec![
        Column::Arrow { array: Arc::new(lists), field: list_field, logical: None },
        Column::Arrow { array: Arc::new(ids), field: Arc::new(ArrowField::new("id", DataType::Int64, false)), logical: Some(int) },
    ], 5, charge).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - (64 << 10)).unwrap();
    let (output, counts) = input.expand_filtered_list(0, &schema, &predicate, Some("n"), &execution).await.unwrap().unwrap();
    drop(input);

    assert_eq!((counts.candidates, counts.survivors), (65_537, 5));
    let Column::Selected(elements) = &output.columns[2] else { panic!("Expected borrowed child spans"); };
    let ((array, _, _), span) = elements.fragment_source(2..4).unwrap();
    assert!(Arc::ptr_eq(&array, &values));
    assert_eq!(span, first_width + 1024..first_width + 1026);
    let actual = (0..output.len).map(|row| [1, 2, 3].map(|column| output.columns[column].value(row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(actual, [
        [Value::I64(1), Value::Null, Value::Null], [Value::I64(2), Value::Null, Value::Null],
        [Value::I64(3), Value::I64(7), Value::I64(1025)], [Value::I64(3), Value::Null, Value::I64(1026)],
        [Value::I64(3), Value::I64(9), Value::I64(32_768)], [Value::I64(4), Value::Null, Value::Null],
        [Value::I64(5), Value::I64(10), Value::I64(1)], [Value::I64(5), Value::I64(8), Value::I64(2)],
    ]);
    assert!(memory.used_bytes() - pressure.bytes() >= values.get_array_memory_size());
    drop((output, pressure));
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn filtered_unnest_borrows_sliced_dense_union_list_and_element_spans() {
    use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, NullArray, UnionArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field as ArrowField, UnionFields};
    use crate::columnar::Column;
    use rtdb_plan::UnaryOp;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let boundary = 1_i64 << 53;
    let item_legs = UnionFields::try_new([5, 9, 12], [
        ArrowField::new("i32", DataType::Int32, false),
        ArrowField::new("i64", DataType::Int64, false),
        ArrowField::new("null", DataType::Null, true),
    ]).unwrap();
    let items: ArrayRef = Arc::new(UnionArray::try_new(item_legs, vec![5, 5, 9, 12, 9, 5, 5].into(),
        Some(vec![0, 1, 0, 0, 1, 2, 3].into()), vec![
            Arc::new(Int32Array::from(vec![90, -2, 7, 5])),
            Arc::new(Int64Array::from(vec![boundary, boundary + 1])),
            Arc::new(NullArray::new(1)),
        ]).unwrap());
    let item = Arc::new(ArrowField::new("item", items.data_type().clone(), true));
    let ty = rtdb_columnar::from_field(&item).unwrap();
    let lists = ListArray::new(item, OffsetBuffer::new(vec![0_i32, 1, 5, 5, 6, 7].into()),
        Arc::clone(&items), Some(NullBuffer::from(vec![true, true, true, false, true]))).slice(1, 4);
    let list_legs = UnionFields::try_new([3, 7, 15], [
        ArrowField::new("list", lists.data_type().clone(), true),
        ArrowField::new("bool", DataType::Boolean, false),
        ArrowField::new("null", DataType::Null, true),
    ]).unwrap();
    let lists = UnionArray::try_new(list_legs, vec![7, 3, 3, 15, 3, 7, 3, 3].into(),
        Some(vec![0, 0, 1, 0, 2, 0, 3, 0].into()), vec![
            Arc::new(lists), Arc::new(BooleanArray::from(vec![true])), Arc::new(NullArray::new(1)),
        ]).unwrap().slice(1, 7);
    let list_field = Arc::new(ArrowField::new("xs", lists.data_type().clone(), false));
    let field = |id, name: &str, data_type| Field {
        id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
    };
    let list = field(0, "xs", rtdb_columnar::from_field(&list_field).unwrap());
    let element = field(1, "v", ty);
    let ordinal = field(2, "n", LogicalType::Scalar(ScalarType::I64));
    let predicate = Expr::binary(BinaryOp::Or,
        Expr::unary(UnaryOp::IsNull, element.expression()),
        Expr::binary(BinaryOp::Or,
            Expr::binary(BinaryOp::Eq, element.expression(), Expr::Literal(Value::I64(-2))),
            Expr::binary(BinaryOp::Gt, element.expression(), Expr::Literal(Value::I64(boundary)))));
    let schema = Schema::new(vec![list, element, ordinal]).unwrap();
    let charge = memory.reserve(lists.get_array_memory_size() + 1024).unwrap();
    let input = ColumnBatch::settled(vec![Column::Arrow {
        array: Arc::new(lists), field: list_field, logical: None,
    }], 7, charge).unwrap();
    let owner = Arc::downgrade(input.column_reservation(0));
    input.validate_readable(&execution.context).unwrap();
    let (output, _) = input.expand_filtered_list(0, &schema, &predicate, Some("n"), &execution).await.unwrap().unwrap();
    drop(input);

    let Column::Selected(elements) = &output.columns[1] else { panic!("Expected borrowed child spans"); };
    for (result, input) in [(0..1, 1..2), (1..3, 3..5), (8..9, 1..2), (9..11, 3..5)] {
        let ((array, _, _), span) = elements.fragment_source(result).unwrap();
        assert!(Arc::ptr_eq(&array, &items));
        assert_eq!(span, input);
    }
    let element = output.columns[1].funded_arrow_cell(0, output.column_reservation(1)).unwrap().unwrap();
    assert!(Arc::ptr_eq(element.owner, &owner.upgrade().unwrap()));
    let actual = rows(&output).into_iter().map(|row| [row[1].clone(), row[2].clone()]).collect::<Vec<_>>();
    let matched = [[Value::I32(-2), Value::I64(1)], [Value::Null, Value::I64(3)], [Value::I64(boundary + 1), Value::I64(4)]];
    let expected = matched.iter().cloned().chain(std::iter::repeat_n([Value::Null, Value::Null], 5))
        .chain(matched.iter().cloned()).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    drop(output);
    assert!(owner.upgrade().is_none());
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn filtered_unnest_declines_an_ambiguous_union_and_keeps_reached_validation() {
    use arrow_array::{Array, BooleanArray, Int64Array, ListArray, UnionArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field as ArrowField, UnionFields};
    use crate::columnar::Column;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let legs = UnionFields::try_new([0, 1], [
        ArrowField::new("int", DataType::Int64, false),
        ArrowField::new("i64", DataType::Boolean, false),
    ]).unwrap();
    let items = UnionArray::try_new(legs, vec![0, 1].into(), Some(vec![0, 0].into()), vec![
        Arc::new(Int64Array::from(vec![1])), Arc::new(BooleanArray::from(vec![true])),
    ]).unwrap();
    let item = Arc::new(ArrowField::new("item", items.data_type().clone(), false));
    let ty = rtdb_columnar::from_field(&item).unwrap();
    let lists = ListArray::new(item, OffsetBuffer::new(vec![0_i32, 1, 2].into()), Arc::new(items), None);
    let field = Arc::new(ArrowField::new("xs", lists.data_type().clone(), false));
    let schema = Schema::new(vec![
        Field { id: ColumnId(0), name: "xs".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::list(ty.clone()) },
        Field { id: ColumnId(1), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty },
    ]).unwrap();
    let charge = memory.reserve(lists.get_array_memory_size() + 1024).unwrap();
    let input = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(lists), field, logical: None }], 2, charge).unwrap();
    let error = input.validate_readable(&execution.context).unwrap_err();
    assert_eq!(error.code.to_string(), "rtdb/array-layout");
    let reached = input.slice(0, 1, memory).unwrap();
    reached.validate_readable(&execution.context).unwrap();
    let predicate = Expr::Literal(Value::Bool(false));
    assert!(reached.expand_filtered_list(0, &schema, &predicate, None, &execution).await.unwrap().is_none());
}

#[test]
fn computed_unnest_keeps_compatible_child_spans_and_gathers_mixed_children_in_order() {
    use crate::columnar::{Column, UnnestRows, pack_evaluated_column};

    let execution = execution(1);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    for first in [[Value::I64(7), Value::I64(8)], [Value::I64(7), Value::Bool(true)]] {
        let batch = |values: &[Value]| {
            let (column, custody) = pack_evaluated_column(values, memory).unwrap();
            let mut batch = ColumnBatch::settled(Vec::new(), values.len(), memory.reserve(256).unwrap()).unwrap();
            batch.append_column(column, custody, memory).unwrap();
            batch
        };
        let parent = batch(&[Value::I64(1), Value::I64(2), Value::I64(3)]);
        let children = [batch(&first), batch(&[Value::I64(9)])];
        let mut runs = UnnestRows::new(memory).unwrap();
        runs.push(0, Some((1, 0..2)), true, memory).unwrap();
        runs.push(1, None, true, memory).unwrap();
        runs.push(2, Some((2, 0..1)), true, memory).unwrap();
        let output = parent.expand_evaluated_lists(runs, &children, Some("n"), true, context).unwrap().unwrap();
        if let Column::Arrow { array, .. } = &children[0].columns[0] {
            let Column::Selected(elements) = &output.columns[1] else { panic!("Expected borrowed child spans"); };
            let ((source, _, _), span) = elements.fragment_source(0..2).unwrap();
            assert!(Arc::ptr_eq(&source, array));
            assert_eq!(span, 0..2);
        }
        drop((parent, children));
        assert_eq!(rows(&output), [
            vec![Value::I64(1), first[0].clone(), Value::I32(1)],
            vec![Value::I64(1), first[1].clone(), Value::I32(2)],
            vec![Value::I64(2), Value::Null, Value::Null],
            vec![Value::I64(3), Value::I64(9), Value::I32(1)],
        ]);
        drop(output);
        assert_eq!(memory.used_bytes(), baseline);
    }
}

#[test]
fn unnest_descriptors_scale_with_the_number_of_lists() {
    use arrow_array::{Array, Int64Array, ListArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field as ArrowField};
    use crate::columnar::Column;

    let execution = execution(1);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    let count = 16_384_i32;
    let values = Arc::new(Int64Array::from_iter_values(0..i64::from(count)));
    let item = Arc::new(ArrowField::new("item", DataType::Int64, false));
    let array = Arc::new(ListArray::new(item, OffsetBuffer::new(vec![0, count].into()), values, None));
    let field = Arc::new(ArrowField::new("xs", array.data_type().clone(), false));
    let logical = Some(rtdb_columnar::from_field(&field).unwrap());
    let charge = memory.reserve(array.get_array_memory_size() + 1024).unwrap();
    let input = ColumnBatch::settled(vec![Column::Arrow { array, field, logical }], 1, charge).unwrap();
    let before = memory.used_bytes();
    let output = input.expand_list(0, None, false, context).unwrap().unwrap();
    assert_eq!(output.len, count as usize);
    assert!(memory.used_bytes() - before < 16 * 1024);
    drop(input);
    assert_eq!(output.columns[1].value(count as usize - 1).unwrap(), Value::I64(i64::from(count) - 1));
    drop(output);
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn unnest_checks_ordinal_and_output_width_overflow_from_group_lengths() {
    let memory = MemoryBudget::new(4096);
    let mut runs = crate::columnar::UnnestRows::new(&memory).unwrap();
    let error = runs.push(0, Some((1, 0..(i32::MAX as usize + 1))), true, &memory).unwrap_err();
    assert_eq!(error.code, "rtdb.query/ordinality-overflow");
    runs.push(0, Some((1, 0..usize::MAX)), false, &memory).unwrap();
    assert_eq!(runs.push(1, None, false, &memory).unwrap_err().code, "rtdb.query/column-size");
}

#[tokio::test]
async fn list_pages_preserve_the_reference_batch_boundaries_independently_of_client_batches() {
    for batch_rows in [1, 2048] {
        for (end, inclusive, expected) in [
            (3, false, vec![vec![1,2]]), (4, false, vec![vec![1,2,3]]),
            (7, false, vec![vec![1,2,3],vec![4,5,6]]), (6, false, vec![vec![1,2,3],vec![4,5]]),
            (1, false, vec![]), (1, true, vec![vec![1]]),
        ] {
            let execution = execution(batch_rows);
            let baseline = execution.context.memory.used_bytes();
            let mut cursor = open(list(series(1, end, 1, inclusive)), &execution).await.unwrap();
            let mut pages = Vec::new();
            while let Some(page) = cursor.next_batch().await.unwrap() { pages.push(rows(&page)); }
            assert_eq!(pages, expected.iter().map(|page| numbers(page)).collect::<Vec<_>>());
            drop(cursor);
            assert_eq!(execution.context.memory.used_bytes(), baseline);
        }
    }
}

#[tokio::test]
async fn page_ordinality_has_i64_cells_and_restarts_for_each_semantic_page() {
    let execution = execution(1);
    let input = list(series(1, 8, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Page).unwrap());
    let mut cursor = open(relation, &execution).await.unwrap();
    for expected in [vec![(1,1),(2,2),(3,3)],vec![(4,1),(5,2),(6,3)],vec![(7,1)]] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(rows(&page), expected.into_iter().map(|(a,n)| vec![Value::I64(a),Value::I64(n)]).collect::<Vec<_>>());
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn relation_ordinality_includes_rows_in_pages_skipped_by_the_consumer() {
    let execution = execution(1);
    let input = list(series(1, 8, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Relation).unwrap());
    let mut cursor = open(relation, &execution).await.unwrap();
    assert_eq!(cursor.skip_complete_pages(4).unwrap(), 3);
    for expected in [&[4, 5, 6][..], &[7][..]] {
        let page = cursor.next_batch().await.unwrap().unwrap();
        let expected = expected.iter().map(|value| vec![Value::I64(*value), Value::I64(*value)]).collect::<Vec<_>>();
        assert_eq!(rows(&page), expected);
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
}

fn division_by_zero() -> Expr { Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0))) }

#[tokio::test]
async fn list_cells_are_demanded_only_in_complete_emitted_pages() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    for good in [2,3] {
        let mut expressions = (1..=good).map(|value| Expr::Literal(Value::I64(value))).collect::<Vec<_>>();
        expressions.push(division_by_zero());
        let mut cursor = open(list(Expr::List(expressions)), &execution).await.unwrap();
        if good == 3 {
            let page = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(rows(&page), numbers(&[1,2,3]));
        }
        assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
        drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
    let mut cursor = open(list(series(1, 2_000_000_000, 1, false)), &execution).await.unwrap();
    drop(cursor.next_batch().await.unwrap().unwrap());
    execution.context.cancellation.cancel();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb/query-cancelled");
    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn integer_list_fast_forward_preserves_full_and_partial_pages() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let mut cursor = open(list(series(8, 1, -1, true)), &execution).await.unwrap();

    assert_eq!(cursor.skip_complete_pages(4).unwrap(), 3);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(rows(&page), numbers(&[5, 4, 3]));
    drop(page);
    assert_eq!(cursor.skip_complete_pages(1).unwrap(), 0);
    assert_eq!(cursor.skip_complete_pages(2).unwrap(), 2);
    assert!(cursor.next_batch().await.unwrap().is_none());

    drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn fast_forward_preserves_errors_in_lists_and_computed_projections() {
    let execution = execution(1);
    let mut lazy = open(list(Expr::List(vec![division_by_zero()])), &execution).await.unwrap();
    assert_eq!(lazy.skip_complete_pages(3).unwrap(), 0);
    assert_eq!(lazy.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");

    let input = list(series(0, 9, 1, false));
    let column = input.schema().fields()[0].clone();
    let original = column.expression();
    let computed = Arc::new(input.project(vec![rtdb_plan::NamedExpr {
        field: column,
        expr: Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), original),
    }]).unwrap());
    let limited = Arc::new(computed.limit(Some(Expr::Literal(Value::I64(3))), Some(Expr::Literal(Value::I64(1)))).unwrap());
    let mut cursor = open(limited, &execution).await.unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn list_descriptor_errors_happen_while_the_cursor_opens() {
    let execution = execution(1);
    let args = vec![division_by_zero(), Expr::Literal(Value::I64(5)), Expr::Literal(Value::I64(1)), Expr::Literal(Value::Bool(false))];
    let expression = Expr::Call { name: "generate_series".into(), return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(), args };
    assert_eq!(open(list(expression), &execution).await.err().unwrap().code, "xtdb.expression/division-by-zero");
}

#[tokio::test]
async fn emitted_definition_survives_its_caller_and_output_survives_the_cursor() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let relation = list(series(1, 4, 1, false));
    let definition = Arc::downgrade(&relation);
    let emitted = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, execution.parameters.types(), &execution.context.memory).unwrap();
    drop(relation);
    assert!(definition.upgrade().is_some());
    let mut cursor = build_column_pages(emitted, execution.clone()).await.unwrap().unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert!(definition.upgrade().is_none());
    assert_eq!(rows(&page), numbers(&[1,2,3]));
    drop(page);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn generated_list_page_handoff_fits_one_payload_grant() {
    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let count = 16_384usize;
    let page_rows = std::num::NonZeroUsize::new(count);
    let mut cursor = execution.open_list(series(0, count as i64, 1, false), page_rows).await.unwrap();
    let headroom = count * std::mem::size_of::<i64>() + 8192;
    let budget_hold = memory.reserve(memory.available_bytes() - headroom).unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(page.len, count);
    assert_eq!(page.columns[0].value(count - 1).unwrap(), Value::I64(count as i64 - 1));
    drop((page, cursor, budget_hold));
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn packed_list_slice_keeps_parent_backing_charged_after_reader_drop() {
    use arrow_array::{Array, Float32Array};
    use crate::columnar::Column;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let parent = Float32Array::from(vec![-0.0; 16_384]);
    let parent_capacity = parent.get_buffer_memory_size();
    let parent_address = parent.values().inner().data_ptr().as_ptr() as usize;
    let expression = Expr::Literal(Value::List(rtdb_types::ListValue::from_f32(parent)));
    let mut reader = execution.open_list(expression, None).await.unwrap();
    let page = reader.next_batch().await.unwrap().unwrap();
    assert_eq!(page.len, 3);
    drop(reader);

    let page = page.slice(1, 1, memory).unwrap();
    let Column::Arrow { array, .. } = &page.columns[0] else { panic!("expected packed float slice"); };
    let values = array.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(values.values().inner().data_ptr().as_ptr() as usize, parent_address);
    assert_eq!(values.value(0).to_bits(), (-0.0_f32).to_bits());
    assert!(memory.used_bytes() >= baseline + parent_capacity);
    drop(page);
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn emissions_of_one_definition_retain_their_own_complete_parameter_fields() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    let declared = LogicalType::Scalar(ScalarType::I64);
    let field = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: declared.clone() };
    let relation = Arc::new(Arc::new(Relation::unit()).project(vec![rtdb_plan::NamedExpr { field, expr: Expr::Parameter { index: 0, data_type: declared.clone() } }]).unwrap());
    {
        let narrow = LogicalType::Scalar(ScalarType::I32).maybe();
        let first = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, std::slice::from_ref(&narrow), &execution.context.memory).unwrap();
        let second = EmittedRelation::derive(Arc::clone(&relation), &execution.context.catalog, std::slice::from_ref(&declared), &execution.context.memory).unwrap();
        assert!(Arc::ptr_eq(first.relation(), second.relation()));
        assert_eq!(first.schema().fields()[0].data_type, narrow);
        assert_eq!(second.schema().fields()[0].data_type, declared);
        assert_eq!(relation.schema().fields()[0].data_type, declared);
        let output = first.output();
        drop(first);
        assert_eq!(output.schema().fields()[0].data_type, narrow);
    }
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn list_batches_keep_arrow_ordinals_after_skipping_semantic_pages() {
    let execution = execution(1);
    let input = list(series(1, 2_000_000_000, 1, false));
    let field = Field { id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let relation = Arc::new(input.with_ordinality(field, OrdinalityScope::Relation).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
    assert_eq!(batches.skip_complete_pages(1_500_000_001).unwrap(), 1_500_000_000);
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 3);
    for column in &batch.columns {
        let crate::columnar::Column::Arrow { array, .. } = column else { panic!("expected Arrow series and ordinality"); };
        let values = array.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(values.values().as_ref(), [1_500_000_001, 1_500_000_002, 1_500_000_003]);
    }
}
