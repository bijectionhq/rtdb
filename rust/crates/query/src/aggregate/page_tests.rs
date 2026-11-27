use super::*;
use crate::pages::PageBuilder;
use std::{collections::{BTreeMap,VecDeque},sync::atomic::{AtomicUsize,Ordering}};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp,ColumnId,Expr,Field,Relation,RelationNode,SortExpr};
use rtdb_storage::{MemoryStore,cache::BufferPool};
use rtdb_types::{Anomaly,TimeUnit,Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{CatalogSnapshot,DatabaseSnapshot,QueryContext,QueryOptions,context::OuterRows,parameters::BoundParameters,runtime_schema::EmittedRelation};

struct Input { pages: VecDeque<Vec<Vec<Value>>>,fail_after: Option<usize>,polls: Arc<AtomicUsize>,memory: Arc<MemoryBudget>, width: usize }
#[async_trait]
impl BatchCursor for Input {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let poll = self.polls.fetch_add(1,Ordering::SeqCst);
        if self.fail_after == Some(poll) { return Err(Anomaly::fault("test/later-page","Later input page failed")); }
        let Some(rows) = self.pages.pop_front() else { return Ok(None); };
        if rows.is_empty() { return ColumnBatch::empty_with_width(self.width, &self.memory).map(Some); }
        let mut page = PageBuilder::new(rows.len(),&self.memory)?;
        for row in rows { page.push(OwnedRow::new(row,&self.memory)?)?; }
        ColumnBatch::from_page(&page.finish(), &self.memory).map(Some)
    }
}
fn execution(batch_rows: usize) -> Execution {
    execution_with_options(QueryOptions { batch_rows,..Default::default() })
}
fn execution_with_options(options: QueryOptions) -> Execution {
    execution_with_budget(options, 1 << 27)
}
fn execution_with_budget(options: QueryOptions, bytes: usize) -> Execution {
    let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    let context = QueryContext::new(catalog,MemoryBudget::new(bytes),Timestamp(0),"UTC",options,&CancellationToken::new()).unwrap();
    Execution { parameters: Arc::new(BoundParameters::infer(vec![],&context.memory).unwrap()),outer: OuterRows::empty(&context.memory).unwrap(),context,bindings: None,analysis: None }
}
fn field(id: u32,ty: LogicalType) -> Field { Field { id: ColumnId(id),name: format!("c{id}"),qualifier: None,qualified_only: false,unnamed: false,data_type: ty } }
fn schema() -> Schema { Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64)),field(1,LogicalType::Scalar(ScalarType::I64))]).unwrap() }
fn count(id: u32,filter: Option<Expr>) -> AggregateExpr {
    AggregateExpr { field: field(id,LogicalType::Scalar(ScalarType::I64)),function: AggregateFunction::CountStar,argument: None,distinct: false,filter,order_by: vec![] }
}
fn divide_filter() -> Expr {
    Expr::binary(BinaryOp::Eq,Expr::binary(BinaryOp::Divide,Expr::Literal(Value::I64(1)),Expr::Literal(Value::I64(0))),Expr::Literal(Value::I64(1)))
}
fn bad_regex() -> Expr {
    Expr::binary(BinaryOp::Eq,Expr::regex_replace(Expr::Literal(Value::Utf8("a".into())),rtdb_plan::RegexReplacement { pattern: raw_literal(Value::Utf8("[".into())),replacement: raw_literal(Value::Utf8("x".into())),..Default::default() }),Expr::Literal(Value::Utf8("x".into())))
}
fn cursor(execution: &Execution,schema: Schema,pages: Vec<Vec<Vec<Value>>>,groups: Vec<NamedExpr>,aggregates: Vec<AggregateExpr>,fail_after: Option<usize>) -> (AggregateCursor,Arc<AtomicUsize>) {
    let input = Arc::new(Relation::new(schema.clone(),RelationNode::Values { rows: vec![] }).unwrap());
    let output = Schema::new(groups.iter().map(|group|group.field.clone()).chain(aggregates.iter().map(|aggregate|aggregate.field.clone())).collect()).unwrap();
    let relation = Relation::new(output,RelationNode::Aggregate { input,groups: groups.clone(),aggregates: aggregates.clone() }).unwrap();
    let runtime = EmittedRelation::derive(Arc::new(relation),&execution.context.catalog,execution.parameters.types(),&execution.context.memory).unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let input = Box::new(Input { pages: pages.into(),fail_after,polls: Arc::clone(&polls),memory: Arc::clone(&execution.context.memory), width: schema.len() });
    (AggregateCursor::from_batches(input, false,schema,groups,aggregates,runtime,execution.clone()).unwrap(),polls)
}
fn batch_row(batch: &ColumnBatch, row: usize) -> Vec<Value> {
    batch.columns.iter().map(|column| column.value(row).unwrap()).collect()
}
fn row(group: i64,value: i64) -> Vec<Value> { vec![Value::I64(group),Value::I64(value)] }

struct Batches(Option<ColumnBatch>);
#[async_trait]
impl BatchCursor for Batches {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.take()) }
}

#[tokio::test]
async fn empty_arrow_groups_validate_aggregate_order_types() {
    let execution = execution(8);
    let mut fields = schema().fields().to_vec();
    let document = field(2, LogicalType::Struct(Default::default()));
    fields.push(document.clone());
    let schema = Schema::new(fields).unwrap();
    let group = NamedExpr { field: field(3, LogicalType::Scalar(ScalarType::I64)), expr: schema.fields()[0].expression() };
    let argument = schema.fields()[1].expression();
    let aggregate = AggregateExpr {
        field: field(4, LogicalType::list(argument.data_type())), function: AggregateFunction::ArrayAgg,
        argument: Some(argument), distinct: false, filter: None,
        order_by: vec![SortExpr { expr: document.expression(), descending: false, nulls_first: false }],
    };
    let child = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let output = Schema::new(vec![group.field.clone(), aggregate.field.clone()]).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input: child, groups: vec![group.clone()], aggregates: vec![aggregate.clone()],
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(None)), false, schema, vec![group], vec![aggregate], runtime, execution).unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/function-type-mismatch");
}

#[tokio::test]
async fn grouping_wide_arrow_lists_reuses_the_input_reservation() {
    use arrow_array::RecordBatch;
    use rtdb_columnar::{OwnedBatch, copy_array, to_field};
    let execution = execution(64);
    let memory = &execution.context.memory;
    let key = Value::list(vec![Value::Utf8("x".repeat(1 << 14))]);
    let ty = key.logical_type();
    let input_field = field(0, ty.clone());
    let schema = Schema::new(vec![input_field.clone()]).unwrap();
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let group = NamedExpr { field: field(1, ty.clone()), expr: input_field.expression() };
    let aggregate = count(2, None);
    let output = Schema::new(vec![group.field.clone(), aggregate.field.clone()]).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input, groups: vec![group.clone()], aggregates: vec![aggregate.clone()],
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let keys = copy_array(&ty, &vec![key.clone(); 64]).unwrap();
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![to_field(&input_field.name, &ty).unwrap()]));
    let batch = RecordBatch::try_new(arrow_schema, vec![keys]).unwrap();
    let batch = ColumnBatch::from_owned(OwnedBatch::new(batch, memory).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(batch))), false, schema,
        vec![group], vec![aggregate], runtime, execution.clone()).unwrap();
    let _pressure = memory.reserve(memory.limit_bytes() - memory.used_bytes() - (1 << 19)).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(result.len, 1);
    assert_eq!(result.columns[0].value(0).unwrap(), key);
    assert_eq!(result.columns[1].value(0).unwrap(), Value::I64(64));
}

#[tokio::test]
async fn grouping_materialized_keys_only_packs_representatives() {
    let execution = execution(64);
    let memory = &execution.context.memory;
    let key_type = LogicalType::Scalar(ScalarType::Utf8).maybe();
    let schema = Schema::new(vec![field(0, LogicalType::Scalar(ScalarType::I64)), field(1, key_type.clone())]).unwrap();
    let group = NamedExpr { field: field(2, key_type), expr: schema.fields()[1].expression() };
    let aggregate = count(3, None);
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let output = Schema::new(vec![group.field.clone(), aggregate.field.clone()]).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input, groups: vec![group.clone()], aggregates: vec![aggregate.clone()],
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let key = Value::Utf8("x".repeat(1 << 14));
    let mut keys = vec![key.clone(); 64];
    keys[0] = Value::Null;
    keys[63] = Value::Null;
    let batch = ColumnBatch::settled(
        vec![Column::Values((0..64).map(Value::I64).collect()), Column::Values(keys)], 64, memory.reserve(1 << 21).unwrap(),
    ).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(batch))), false, schema,
        vec![group], vec![aggregate], runtime, execution.clone()).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - (1 << 19)).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(result.len, 2);
    assert_eq!(batch_row(&result, 0), [Value::Null, Value::I64(2)]);
    assert_eq!(batch_row(&result, 1), [key, Value::I64(62)]);
    drop((pressure, cursor));
    assert!(memory.used_bytes() < 1 << 19);
}

#[tokio::test]
async fn global_reductions_emit_arrow_vectors_for_nullable_and_empty_inputs() {
    use arrow_array::{Float64Array, RecordBatch};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

    let execution = execution(1);
    let memory = &execution.context.memory;
    let float = LogicalType::Scalar(ScalarType::F64).maybe();
    let schema = Schema::new(vec![field(0, float.clone())]).unwrap();
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let argument = schema.fields()[0].expression();
    let aggregates = vec![count(1, None),
        AggregateExpr { function: AggregateFunction::Count, argument: Some(argument.clone()), ..count(2, None) },
        AggregateExpr { field: field(3, float.clone()), function: AggregateFunction::Sum, argument: Some(argument.clone()), ..count(3, None) },
        AggregateExpr { field: field(4, float), function: AggregateFunction::Average, argument: Some(argument), ..count(4, None) },
    ];
    let output = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate { input, groups: vec![], aggregates: aggregates.clone() }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    assert!(AggregateCursor::positions(&schema, &[], &aggregates).is_some());
    for (values, expected) in [
        (vec![Some(2.0), None, Some(3.0)], [Value::I64(3), Value::I64(2), Value::F64(5.0), Value::F64(2.5)]),
        (vec![], [Value::I64(0), Value::I64(0), Value::Null, Value::Null]),
    ] {
        let physical = Arc::new(ArrowSchema::new(vec![ArrowField::new("c0", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(physical, vec![Arc::new(Float64Array::from(values))]).unwrap();
        let batch = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(batch, memory).unwrap()).unwrap();
        let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(batch))), false, schema.clone(), vec![],
            aggregates.clone(), Arc::clone(&runtime), execution.clone()).unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(result.len, 1);
        for (column, value) in result.columns.iter().zip(expected) {
            assert!(matches!(column, Column::Arrow { .. }));
            assert_eq!(column.value(0).unwrap(), value);
        }
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn selected_global_reductions_fit_without_materializing_row_masks() {
    use arrow_array::{Float64Array, NullArray};
    use crate::columnar::SelectedColumn;

    let execution = execution(1024);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let len = 32_768;
    let float = LogicalType::Scalar(ScalarType::F64).maybe();
    let null_field = Arc::new(rtdb_columnar::to_field("c0", &LogicalType::Null).unwrap());
    let float_field = Arc::new(rtdb_columnar::to_field("c0", &float).unwrap());
    let schema = Schema::new(vec![field(0, float.clone())]).unwrap();
    let argument = schema.fields()[0].expression();
    let aggregates = vec![
        count(1, None),
        AggregateExpr { function: AggregateFunction::Count, argument: Some(argument.clone()), ..count(2, None) },
        AggregateExpr { field: field(3, float.clone()), function: AggregateFunction::Sum, argument: Some(argument.clone()), ..count(3, None) },
        AggregateExpr { field: field(4, float.clone()), function: AggregateFunction::Average, argument: Some(argument), ..count(4, None) },
    ];
    let output = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input: Arc::new(Relation::empty(schema.clone())), groups: vec![], aggregates: aggregates.clone(),
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();

    let selected = SelectedColumn::new(vec![
        Some((Arc::new(Float64Array::from(vec![Some(-0.0), None])), float_field, Some(float), None)),
        Some((Arc::new(NullArray::new(1)), null_field, Some(LogicalType::Null), None)),
        None,
    ], Arc::new([(0, 0), (0, 1), (1, 0), (2, 0)].into_iter().cycle().take(len).collect()));
    let input = ColumnBatch::settled(vec![Column::Selected(selected)], len, memory.reserve(0).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(input))), false,
        schema, vec![], aggregates, runtime, execution.clone()).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - 16 * 1024).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(result.columns[0].value(0).unwrap(), Value::I64(len as i64));
    assert_eq!(result.columns[1].value(0).unwrap(), Value::I64(len as i64 / 4));
    for column in &result.columns[2..] {
        let Value::F64(value) = column.value(0).unwrap() else { panic!("Expected a double result"); };
        assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
    }
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop((result, cursor, pressure));
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn grouped_floating_reductions_keep_input_order_across_selected_and_value_pages() {
    use arrow_array::{Float64Array, Int64Array};
    use crate::columnar::SelectedColumn;

    struct Pages(VecDeque<ColumnBatch>);
    #[async_trait]
    impl BatchCursor for Pages {
        async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
    }

    let execution = execution(8);
    let memory = &execution.context.memory;
    let integer = LogicalType::Scalar(ScalarType::I64);
    let float = LogicalType::Scalar(ScalarType::F64).maybe();
    let schema = Schema::new(vec![field(0, integer.clone()), field(1, float.clone())]).unwrap();
    let group = NamedExpr { field: field(2, integer.clone()), expr: schema.fields()[0].expression() };
    let argument = schema.fields()[1].expression();
    let aggregates = vec![
        count(3, None),
        AggregateExpr { function: AggregateFunction::Count, argument: Some(argument.clone()), ..count(4, None) },
        AggregateExpr { field: field(5, float.clone()), function: AggregateFunction::Sum, argument: Some(argument.clone()), ..count(5, None) },
        AggregateExpr { field: field(6, float.clone()), function: AggregateFunction::Average, argument: Some(argument), ..count(6, None) },
    ];
    let output = Schema::new(std::iter::once(group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect()).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input: Arc::new(Relation::empty(schema.clone())), groups: vec![group.clone()], aggregates: aggregates.clone(),
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let selected = SelectedColumn::new(vec![
        Some((Arc::new(Float64Array::from(vec![Some(1e16), Some(1.0), Some(-1e16), None])),
            Arc::new(rtdb_columnar::to_field("c1", &float).unwrap()), Some(float), None)),
        None,
    ], Arc::new(vec![(0, 0), (0, 3), (0, 2), (1, 0), (0, 1)]));
    let first = ColumnBatch::settled(vec![
        Column::Arrow { array: Arc::new(Int64Array::from(vec![9, 1, 9, 7, 9])),
            field: Arc::new(rtdb_columnar::to_field("c0", &integer).unwrap()), logical: Some(integer) },
        Column::Selected(selected),
    ], 5, memory.reserve(0).unwrap()).unwrap();
    let second = ColumnBatch::settled(vec![
        Column::Values(vec![Value::I64(9), Value::I64(1), Value::I64(7)]),
        Column::Values(vec![Value::F64(2.0), Value::F64(8.0), Value::Null]),
    ], 3, memory.reserve(0).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Pages([first, second].into())), false,
        schema, vec![group], aggregates, runtime, execution.clone()).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(result.len, 3);
    assert_eq!(batch_row(&result, 0), vec![Value::I64(9), Value::I64(4), Value::I64(4), Value::F64(3.0), Value::F64(0.75)]);
    assert_eq!(batch_row(&result, 1), vec![Value::I64(1), Value::I64(2), Value::I64(1), Value::F64(8.0), Value::F64(8.0)]);
    assert_eq!(batch_row(&result, 2), vec![Value::I64(7), Value::I64(2), Value::I64(0), Value::Null, Value::Null]);
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn selected_decimal_and_text_reductions_keep_exact_values_and_nulls() {
    use arrow_array::StringArray;
    use crate::columnar::SelectedColumn;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 2, scale: 1, bit_width: 128 }).maybe();
    let text = LogicalType::Scalar(ScalarType::Utf8).maybe();
    let schema = Schema::new(vec![field(0, decimal.clone()), field(1, text.clone())]).unwrap();
    let aggregate = |id, function, position: usize, output| AggregateExpr {
        field: field(id, output), function, argument: Some(schema.fields()[position].expression()),
        distinct: false, filter: None, order_by: vec![],
    };
    let aggregates = vec![
        aggregate(2, AggregateFunction::Sum, 0, decimal.clone()),
        aggregate(3, AggregateFunction::Average, 0, LogicalType::Scalar(ScalarType::F64).maybe()),
        aggregate(4, AggregateFunction::Min, 1, text.clone()),
        aggregate(5, AggregateFunction::Max, 1, text.clone()),
    ];
    let output = Schema::new(aggregates.iter().map(|agg| agg.field.clone()).collect()).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input: Arc::new(Relation::empty(schema.clone())), groups: vec![], aggregates: aggregates.clone(),
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let number = |coefficient| Value::Decimal(decimal128_value(coefficient, 2, 1).unwrap());
    let decimals = rtdb_columnar::copy_array(&decimal, &[number(15), number(25), Value::Null]).unwrap();
    let strings = Arc::new(StringArray::from(vec![Some("é"), Some("b"), None]));
    let picks = Arc::new(vec![(1, 0), (0, 1), (0, 2), (0, 0)]);
    let selected = |array: arrow_array::ArrayRef, name: &str, ty: LogicalType| {
        let field = Arc::new(rtdb_columnar::to_field(name, &ty).unwrap());
        Column::Selected(SelectedColumn::new(vec![Some((array, field, Some(ty), None)), None], Arc::clone(&picks)))
    };
    let input = ColumnBatch::settled(vec![selected(decimals, "c0", decimal), selected(strings, "c1", text)],
        picks.len(), memory.reserve(4096).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(input))), false,
        schema, vec![], aggregates, runtime, execution.clone()).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(batch_row(&result, 0),
        vec![number(40), Value::F64(2.0), Value::Utf8("b".into()), Value::Utf8("é".into())]);
}

#[tokio::test]
async fn global_counts_fit_without_a_group_ordinal_for_each_row() {
    for filtered in [false, true] {
        let execution = execution(1024);
        let memory = &execution.context.memory;
        let baseline = memory.used_bytes();
        let len = 32_768;
        let logical = LogicalType::Scalar(ScalarType::Bool);
        let schema = Schema::new(vec![field(0, logical.clone())]).unwrap();
        let filter = filtered.then(|| Expr::binary(
            BinaryOp::Eq, schema.fields()[0].expression(), Expr::Literal(Value::Bool(true)),
        ));
        let aggregate = count(1, filter);
        let output = Schema::new(vec![aggregate.field.clone()]).unwrap();
        let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
            input: Arc::new(Relation::empty(schema.clone())), groups: vec![], aggregates: vec![aggregate.clone()],
        }).unwrap());
        let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
        let array = Arc::new(arrow_array::BooleanArray::from((0..len).map(|row| row % 2 == 0).collect::<Vec<_>>()));
        let field = Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap());
        let input = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(logical) }],
            len, memory.reserve(0).unwrap()).unwrap();
        let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(input))), false,
            schema, vec![], vec![aggregate], runtime, execution.clone()).unwrap();
        let pressure = memory.reserve(memory.available_bytes() - 128 * 1024).unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        let expected = if filtered { len / 2 } else { len };
        assert_eq!(result.columns[0].value(0).unwrap(), Value::I64(expected as i64));
        drop((result, cursor, pressure));
        assert_eq!(memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn grouped_count_excludes_null_runs() {
    use arrow_array::{Float64Array, Int16Array, Int64Array, RecordBatch, RunArray, types::Int16Type};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_types::RunEndWidth;

    let execution = execution(1);
    let memory = &execution.context.memory;
    let key = LogicalType::Scalar(ScalarType::F64);
    let runs = LogicalType::RunEndEncoded {
        width: RunEndWidth::I16,
        value: Box::new(LogicalType::Scalar(ScalarType::I64).maybe()),
    };
    let schema = Schema::new(vec![field(0, key.clone()), field(1, runs.clone())]).unwrap();
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let groups = vec![NamedExpr { field: field(2, key.clone()), expr: schema.fields()[0].expression() }];
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        argument: Some(schema.fields()[1].expression()),
        ..count(3, None)
    }];
    let output = Schema::new(vec![groups[0].field.clone(), aggregates[0].field.clone()]).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate { input, groups: groups.clone(), aggregates: aggregates.clone() }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let physical = Arc::new(ArrowSchema::new(vec![
        rtdb_columnar::to_field("c0", &key).unwrap(),
        rtdb_columnar::to_field("c1", &runs).unwrap(),
    ]));
    let values = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![1, 3]), &Int64Array::from(vec![Some(7), None]),
    ).unwrap();
    let batch = RecordBatch::try_new(physical, vec![Arc::new(Float64Array::from(vec![1.0; 3])), Arc::new(values)]).unwrap();
    let batch = ColumnBatch::from_owned(rtdb_columnar::OwnedBatch::new(batch, memory).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(
        Box::new(Batches(Some(batch))), false, schema, groups, aggregates, runtime, execution.clone(),
    ).unwrap();
    let result = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(result.len, 1);
    assert_eq!(result.columns[0].value(0).unwrap(), Value::F64(1.0));
    assert_eq!(result.columns[1].value(0).unwrap(), Value::I64(1));
}

#[test]
fn count_does_not_use_the_parent_bitmap_for_nested_cells_that_can_fail_to_read() {
    use arrow_array::{ListArray, StringArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::DataType;

    let logical = LogicalType::list(LogicalType::Scalar(ScalarType::Uri));
    let field = Arc::new(rtdb_columnar::to_field("c0", &logical).unwrap());
    let DataType::List(child) = field.data_type() else { panic!("Expected list"); };
    let array = Arc::new(ListArray::new(
        Arc::clone(child), OffsetBuffer::new(vec![0, 1].into()),
        Arc::new(StringArray::from(vec!["%"])), None,
    ));
    let column = Column::Arrow { array, field, logical: Some(logical) };
    assert!(column.value(0).is_err());
    assert!(!accepts_typed_column(&column, count_layout));
}

#[test]
fn aggregate_argument_positions_follow_first_use_after_group_keys() {
    let ty = LogicalType::Scalar(ScalarType::I64);
    let schema = Schema::new((0..3).map(|id| field(id, ty.clone())).collect()).unwrap();
    let argument = |position: usize| schema.fields()[position].expression();
    let groups = vec![NamedExpr { field: field(3, ty.clone()), expr: argument(0) }];
    let order_by = [2, 2, 0].into_iter().map(|position| SortExpr {
        expr: argument(position), descending: false, nulls_first: false,
    }).collect();
    let array = AggregateExpr {
        field: field(4, LogicalType::list(ty)), function: AggregateFunction::ArrayAgg,
        argument: Some(argument(0)), order_by, ..count(4, None)
    };
    let count_arg = |id, position| AggregateExpr {
        function: AggregateFunction::Count, argument: Some(argument(position)), ..count(id, None)
    };
    assert_eq!(AggregateCursor::positions(&schema, &groups, &[array, count_arg(5, 1), count_arg(6, 2)]),
        Some(vec![0, 2, 0, 1]));
}

#[test]
fn aggregate_preflight_reads_selected_cells_in_page_order() {
    use arrow_array::{ArrayRef, Int64Array, NullArray, StringArray, UnionArray};
    use arrow_schema::{Field as ArrowField, UnionFields};
    use crate::columnar::SelectedColumn;

    let execution = execution(8);
    let context = &execution.context;
    let uri = rtdb_columnar::to_field("other", &LogicalType::Scalar(ScalarType::Uri)).unwrap();
    let union_fields = UnionFields::try_new([0, 2], [ArrowField::new("uri", DataType::Utf8, false), uri]).unwrap();
    let union: ArrayRef = Arc::new(UnionArray::try_new(union_fields, vec![0_i8, 0].into(), Some(vec![0_i32, 1].into()), vec![
        Arc::new(StringArray::from(vec!["bad uri", "https://example.com/"])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
    ]).unwrap());
    let union_field = Arc::new(ArrowField::new("arg", union.data_type().clone(), false));
    let uri_error = rtdb_columnar::read_value(&union_field, union.as_ref(), 0).unwrap_err().code;
    let page = |cells: Vec<(usize, usize)>| {
        let len = cells.len();
        let selected = SelectedColumn::new(vec![
            Some((Arc::new(NullArray::new(1)), Arc::new(ArrowField::new("arg", DataType::Null, false)), None, None)),
            Some((Arc::clone(&union), Arc::clone(&union_field), None, None)),
            Some((Arc::new(Int64Array::from(vec![7])), Arc::new(ArrowField::new("arg", DataType::Int64, false)),
                Some(LogicalType::Scalar(ScalarType::I64)), None)),
            None,
        ], Arc::new(cells));
        ColumnBatch::settled(vec![Column::Selected(selected)], len, context.memory.reserve(4096).unwrap()).unwrap()
    };
    let valid = [(3, 0), (1, 1), (2, 0), (1, 1), (2, 0)];
    let batch = page(valid.to_vec());
    let baseline = context.memory.used_bytes();
    assert!(!accepts_typed_column(&batch.columns[0], count_layout));
    assert!(Rows::columns(&batch, &[0], false, context).is_ok());
    assert_eq!(context.memory.used_bytes(), baseline);

    for (tail, expected) in [([(1, 0), (0, 0)], uri_error.as_str()), ([(0, 0), (1, 0)], "xtdb/zero-leg-read")] {
        let batch = page(valid.into_iter().chain(tail).collect());
        assert_eq!(Rows::columns(&batch, &[0], false, context).err().unwrap().code, expected);
    }
}

#[tokio::test]
async fn fallback_argument_preflight_precedes_aggregate_filters() {
    use arrow_array::StringArray;

    let execution = execution(2);
    let memory = &execution.context.memory;
    let uri = LogicalType::Scalar(ScalarType::Uri);
    let schema = Schema::new(vec![field(0, uri.clone())]).unwrap();
    let aggregates = vec![count(1, Some(divide_filter())), AggregateExpr {
        function: AggregateFunction::Count, argument: Some(schema.fields()[0].expression()), ..count(2, None)
    }];
    let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
    let output = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
    let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
        input, groups: vec![], aggregates: aggregates.clone(),
    }).unwrap());
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
    let field = Arc::new(rtdb_columnar::to_field("c0", &uri).unwrap());
    let array = Arc::new(StringArray::from(vec!["https://example.com/", "bad uri"]));
    let expected = rtdb_columnar::read_value(&field, array.as_ref(), 1).unwrap_err().code;
    let column = Column::Arrow { array, field, logical: None };
    let batch = ColumnBatch::settled(vec![column], 2, memory.reserve(4096).unwrap()).unwrap();
    let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(batch))), false, schema,
        vec![], aggregates, runtime, execution.clone()).unwrap();
    assert_eq!(cursor.next_batch().await.err().unwrap().code, expected);
}

#[tokio::test]
async fn percentile_collects_arrow_positions_before_sorting_the_group() {
    for object_sort in [rtdb_types::ObjectSort::TimSort, rtdb_types::ObjectSort::LegacyMergeSort] {
        let execution = execution_with_options(QueryOptions { object_sort, batch_rows: 1, ..Default::default() });
        let baseline = execution.context.memory.used_bytes();
        let ty = LogicalType::Scalar(ScalarType::I64);
        let schema = Schema::new(vec![field(0, ty.clone())]).unwrap();
        let argument = schema.fields()[0].expression();
        let percentile = |id, kind, descending, ty| AggregateExpr {
            field: field(id, ty),
            function: AggregateFunction::Percentile { kind, fraction: PercentileFraction::Literal(0.25), descending },
            argument: Some(argument.clone()), distinct: false, filter: None, order_by: vec![],
        };
        let aggregates = vec![
            percentile(1, PercentileKind::Continuous, false, LogicalType::Scalar(ScalarType::F64).maybe()),
            percentile(2, PercentileKind::Discrete, true, ty.maybe()),
        ];
        let input = Arc::new(Relation::empty(schema.clone()));
        let output = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect()).unwrap();
        let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
            input, groups: vec![], aggregates: aggregates.clone(),
        }).unwrap());
        let memory = &execution.context.memory;
        let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
        let arrow = Arc::new(rtdb_columnar::to_field("value", &LogicalType::Scalar(ScalarType::I64)).unwrap());
        let array = Arc::new(arrow_array::Int64Array::from_iter_values((0..=102_400).rev()));
        let batch = || ColumnBatch::settled(vec![Column::Arrow {
            array: array.clone(), field: arrow.clone(), logical: None,
        }], 102_401, memory.reserve(1024 * 1024).unwrap()).unwrap();

        struct FailAfterBatch(Option<ColumnBatch>);
        #[async_trait]
        impl BatchCursor for FailAfterBatch {
            async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
                self.0.take().map(Some).ok_or_else(|| Anomaly::fault("test/later-page", "Later input page failed"))
            }
        }
        let mut failed = AggregateCursor::from_batches(Box::new(FailAfterBatch(Some(batch()))), false,
            schema.clone(), vec![], aggregates.clone(), runtime.clone(), execution.clone()).unwrap();
        assert_eq!(failed.next_batch().await.err().unwrap().code, "test/later-page");
        drop(failed);
        let mut sorted = AggregateCursor::from_batches(Box::new(Batches(Some(batch()))), false,
            schema, vec![], aggregates, runtime.clone(), execution.clone()).unwrap();
        let result = sorted.next_batch().await.unwrap().unwrap();
        assert!(result.columns.iter().all(|column| !matches!(column, Column::Values(_))));
        assert_eq!(result.columns[0].value(0).unwrap(), Value::F64(25_600.0));
        assert_eq!(result.columns[1].value(0).unwrap(), Value::I64(76_800));
        assert!(sorted.next_batch().await.unwrap().is_none());
        drop((sorted, result, runtime));
        assert_eq!(execution.context.memory.used_bytes(), baseline);
        assert_eq!(execution.context.spill_bytes(), 0);
    }
}

#[tokio::test]
async fn spilled_percentiles_use_original_positions_across_mixed_runs() {
    for object_sort in [rtdb_types::ObjectSort::TimSort, rtdb_types::ObjectSort::LegacyMergeSort] {
        let execution = execution_with_options(QueryOptions { object_sort, batch_rows: 64, sort_memory_bytes: 2048, ..Default::default() });
        let baseline = execution.context.memory.used_bytes();
        let ty = LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::F64)).maybe();
        let schema = Schema::new(vec![field(0, ty.clone())]).unwrap();
        let argument = schema.fields()[0].expression();
        let percentile = |id, kind, descending, fraction, ty| AggregateExpr {
            field: field(id, ty),
            function: AggregateFunction::Percentile { kind, fraction: PercentileFraction::Literal(fraction), descending },
            argument: Some(argument.clone()), distinct: false, filter: None, order_by: vec![],
        };
        let aggregates = vec![
            percentile(1, PercentileKind::Continuous, false, 0.5, LogicalType::Scalar(ScalarType::F64).maybe()),
            percentile(2, PercentileKind::Discrete, true, 0.75, ty),
        ];
        let pages = (0..10).map(|page| {
            (0..30).flat_map(|row| {
                let lower = if row % 2 == 0 { Value::I64(10) } else { Value::F64(10.0) };
                let upper = if page % 2 == 0 { Value::F64(30.0) } else { Value::I64(30) };
                [vec![lower], vec![upper], vec![Value::Null]]
            }).collect()
        }).collect();
        let (mut cursor, _) = cursor(&execution, schema, pages, vec![], aggregates, None);
        let result = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(result.columns[0].value(0).unwrap(), Value::F64(20.0));
        assert!(matches!(result.columns[1].value(0).unwrap(), Value::F64(value) if value == 10.0));
        assert!(execution.context.spill_peak_bytes() > 0);
        drop((cursor, result));
        assert_eq!(execution.context.spill_bytes(), 0);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn typed_percentile_ranks_keep_decimal_peers_and_copy_nullable_winners() {
    for object_sort in [rtdb_types::ObjectSort::TimSort, rtdb_types::ObjectSort::LegacyMergeSort] {
        let execution = execution_with_options(QueryOptions { object_sort, ..Default::default() });
        let baseline = execution.context.memory.used_bytes();
        let int = LogicalType::Scalar(ScalarType::I64);
        let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 20, scale: 0, bit_width: 128 }).maybe();
        let schema = Schema::new(vec![field(0, int.clone()), field(1, decimal.clone())]).unwrap();
        let groups = vec![NamedExpr { field: field(2, int), expr: schema.fields()[0].expression() }];
        let percentile = |id, kind, ty| AggregateExpr {
            field: field(id, ty),
            function: AggregateFunction::Percentile { kind, fraction: PercentileFraction::Literal(0.5), descending: true },
            argument: Some(schema.fields()[1].expression()),
            distinct: false,
            filter: None,
            order_by: vec![],
        };
        let aggregates = vec![
            percentile(3, PercentileKind::Discrete, decimal),
            percentile(4, PercentileKind::Continuous, LogicalType::Scalar(ScalarType::F64).maybe()),
        ];
        let decimal = |value: i128| Value::Decimal(
            rtdb_types::DecimalValue::new(value.to_be_bytes().to_vec(), 20, 0, 128).unwrap()
        );
        let first = decimal(9_007_199_254_740_992);
        let second = decimal(9_007_199_254_740_993);
        let pages = vec![
            vec![vec![Value::I64(1), first.clone()], vec![Value::I64(2), Value::Null]],
            vec![vec![Value::I64(1), second], vec![Value::I64(2), Value::Null]],
        ];
        let (mut cursor, _) = cursor(&execution, schema, pages, groups, aggregates, None);
        let result = cursor.next_batch().await.unwrap().unwrap();
        let Column::Arrow { array, field, .. } = &result.columns[1] else { panic!("Expected one nullable decimal array"); };
        assert_eq!(array.data_type(), &DataType::Decimal128(20, 0));
        assert!(field.is_nullable());
        assert_eq!(batch_row(&result, 0), [Value::I64(1), first, Value::F64(9_007_199_254_740_992.0)]);
        assert_eq!(batch_row(&result, 1), [Value::I64(2), Value::Null, Value::Null]);
        drop((cursor, result));
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn typed_percentile_ranks_validate_only_the_discrete_decimal_winner() {
    let execution = execution(8);
    let baseline = execution.context.memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::Decimal { precision: 2, scale: 0, bit_width: 128 });
    let decimal = |value: i128| Value::Decimal(
        rtdb_types::DecimalValue::new(value.to_be_bytes().to_vec(), 2, 0, 128).unwrap()
    );
    let percentile = |kind, fraction| primitive_cursor(
        &execution,
        AggregateFunction::Percentile { kind, fraction: PercentileFraction::Literal(fraction), descending: false },
        ty.clone(), ty.clone(), vec![vec![decimal(100), decimal(10)]], None,
    );

    assert_eq!(primitive_result(percentile(PercentileKind::Discrete, 0.0)).await, decimal(10));
    assert_eq!(primitive_result(percentile(PercentileKind::Continuous, 1.0)).await, Value::F64(100.0));
    let mut discrete = percentile(PercentileKind::Discrete, 1.0);
    assert_eq!(discrete.next_batch().await.err().unwrap().code, "rtdb/decimal-precision");
    drop(discrete);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn typed_percentile_ranks_keep_date_peers_and_reach_mixed_timestamp_overflow() {
    let execution = execution(8);
    let baseline = execution.context.memory.used_bytes();
    let function = AggregateFunction::Percentile {
        kind: PercentileKind::Discrete, fraction: PercentileFraction::Literal(0.5), descending: false,
    };
    let ty = LogicalType::Scalar(ScalarType::DateMilli);
    let mut percentile = primitive_cursor(&execution, function.clone(), ty.clone(), ty,
        vec![vec![Value::DateMilli(999)], vec![Value::DateMilli(1)]], None);
    let result = percentile.next_batch().await.unwrap().unwrap();
    assert_eq!(result.columns[0].value(0).unwrap(), Value::DateMilli(999));
    drop((percentile, result));

    let timestamp = |value, unit| Value::Timestamp { value, unit, timezone: Some("America/New_York".into()) };
    let start = timestamp(i64::MAX, TimeUnit::Second);
    let end = timestamp(0, TimeUnit::Nanosecond);
    let mixed = start.logical_type().join(&end.logical_type());
    let mut percentile = primitive_cursor(&execution, function, mixed.clone(), mixed, vec![vec![start], vec![end]], None);
    assert_eq!(percentile.next_batch().await.err().unwrap().code, "xtdb.expression/overflow-error");
    drop(percentile);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[test]
fn typed_percentile_ranks_copy_timestamp_results_with_independent_backing() {
    let memory = MemoryBudget::new(1 << 20);
    let ty = LogicalType::Scalar(ScalarType::Timestamp {
        unit: TimeUnit::Microsecond, timezone: Some("America/New_York".into()),
    });
    let field = rtdb_columnar::to_field("value", &ty).unwrap();
    let values = arrow_array::TimestampMicrosecondArray::from(vec![i64::MIN, i64::MAX]).with_timezone("America/New_York");
    let original = values.values().as_ptr();
    let batch = ColumnBatch::settled(vec![Column::Arrow {
        array: Arc::new(values), field: Arc::new(field.clone()), logical: Some(ty.clone()),
    }], 2, memory.reserve(0).unwrap()).unwrap();
    let source = Arc::downgrade(batch.column_reservation(0));
    let (column, charge) = copy_ranked_cells(&[batch], &[Some((0, 1)), None], field, &memory).unwrap();
    assert!(source.upgrade().is_none());
    let Column::Arrow { array, logical, .. } = &column else { panic!("Expected a timestamp array"); };
    assert_eq!(logical.as_ref(), Some(&ty.maybe()));
    assert_ne!(array.as_primitive::<TimestampMicrosecondType>().values().as_ptr(), original);
    assert_eq!(column.value(0).unwrap(), Value::Timestamp {
        value: i64::MAX, unit: TimeUnit::Microsecond, timezone: Some("America/New_York".into()),
    });
    assert_eq!(column.value(1).unwrap(), Value::Null);
    drop((column, charge));
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn aggregate_columns_complete_their_page_masks_before_the_next_column() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let schema = schema();
        let filter = Expr::Case { branches: vec![(Expr::binary(BinaryOp::Eq,schema.fields()[0].expression(),Expr::Literal(Value::I64(1))),Expr::Literal(Value::Bool(true)))],otherwise: Box::new(divide_filter()) };
        let groups = vec![NamedExpr { field: field(2,LogicalType::Scalar(ScalarType::I64)),expr: schema.fields()[0].expression() }];
        let (mut cursor,_) = cursor(&execution,schema,vec![vec![row(1,1),row(2,2)]],groups,vec![count(3,Some(filter)),count(4,Some(bad_regex()))],None);
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
        assert!(cursor.next_batch().await.unwrap().is_none());drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn aggregate_page_failure_precedes_the_next_input_pull() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let (mut cursor,polls) = cursor(&execution,schema(),vec![vec![row(1,1)]],vec![],vec![count(2,Some(divide_filter()))],Some(1));
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");
    assert_eq!(polls.load(Ordering::SeqCst),1);drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn complete_filter_mask_precedes_the_inner_accumulator_factory() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let schema = Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64)),field(1,LogicalType::Scalar(ScalarType::I8))]).unwrap();
    let filter = Expr::Case { branches: vec![(Expr::binary(BinaryOp::Eq,schema.fields()[0].expression(),Expr::Literal(Value::I64(1))),Expr::Literal(Value::Bool(true)))],otherwise: Box::new(divide_filter()) };
    let aggregate = AggregateExpr { field: field(2,LogicalType::Scalar(ScalarType::I8).maybe()),function: AggregateFunction::Sum,argument: Some(schema.fields()[1].expression()),distinct: false,filter: Some(filter),order_by: vec![] };
    let (mut cursor,_) = cursor(&execution,schema,vec![vec![vec![Value::I64(1),Value::I8(1)],vec![Value::I64(2),Value::I8(2)]]],vec![],vec![aggregate],None);
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/division-by-zero");drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn empty_semantic_pages_prepare_filters_and_absent_input_preserves_global_count() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let (mut empty,_) = cursor(&execution,schema(),vec![vec![]],vec![],vec![count(2,Some(bad_regex()))],None);
    assert_eq!(empty.next_batch().await.err().unwrap().code,"xtdb.expression/illegal-argument-exception");drop(empty);
    let (mut absent,_) = cursor(&execution,schema(),vec![],vec![],vec![count(2,Some(bad_regex()))],None);
    let page = absent.next_batch().await.unwrap().unwrap();assert_eq!(batch_row(&page, 0),[Value::I64(0)]);
    assert!(absent.next_batch().await.unwrap().is_none());drop(absent);drop(page);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn grouping_full_hash_avoids_unreachable_cross_unit_equality() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let first = Value::Timestamp { value: i64::MAX,unit: TimeUnit::Second,timezone: Some("UTC".into()) };
    let second = Value::Timestamp { value: 0,unit: TimeUnit::Millisecond,timezone: Some("UTC".into()) };
    let ty = first.logical_type().join(&second.logical_type());
    let schema = Schema::new(vec![field(0,ty.clone())]).unwrap();
    let groups = vec![NamedExpr { field: field(1,ty),expr: schema.fields()[0].expression() }];
    let (mut cursor,_) = cursor(&execution,schema,vec![vec![vec![first.clone()],vec![second.clone()]]],groups,vec![count(2,None)],None);
    let page = cursor.next_batch().await.unwrap().unwrap();drop(cursor);
    assert_eq!(batch_row(&page, 0),[first,Value::I64(1)]);
    assert_eq!(batch_row(&page, 1),[second,Value::I64(1)]);
    drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn aggregate_output_is_complete_and_retains_distinct_state_across_pages() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let schema = schema();let argument = schema.fields()[1].expression();
        let groups = vec![NamedExpr { field: field(2,LogicalType::Scalar(ScalarType::I64)),expr: schema.fields()[0].expression() }];
        let mut array = AggregateExpr { field: field(3,LogicalType::list(LogicalType::Scalar(ScalarType::I64))),function: AggregateFunction::ArrayAgg,argument: Some(argument.clone()),distinct: true,filter: None,order_by: vec![SortExpr { expr: argument.clone(),descending: true,nulls_first: false }] };
        array.filter = Some(Expr::binary(BinaryOp::NotEq,argument.clone(),Expr::Literal(Value::I64(0))));
        let sum = AggregateExpr { field: field(4,LogicalType::Scalar(ScalarType::I64).maybe()),function: AggregateFunction::Sum,argument: Some(argument),distinct: true,filter: None,order_by: vec![] };
        let (mut cursor,_) = cursor(&execution,schema,vec![vec![row(1,2),row(2,5)],vec![],vec![row(1,1),row(1,2),row(2,0)]],groups,vec![array,sum],None);
        let page = cursor.next_batch().await.unwrap().unwrap();assert_eq!(page.len,2);
        assert!(cursor.next_batch().await.unwrap().is_none());drop(cursor);assert_eq!(execution.context.spill_bytes(),0);
        assert_eq!(batch_row(&page, 0),[Value::I64(1),Value::list(vec![Value::I64(2),Value::I64(1)]),Value::I64(3)]);
        assert_eq!(batch_row(&page, 1),[Value::I64(2),Value::list(vec![Value::I64(5)]),Value::I64(5)]);
        drop(page);assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn ordered_array_agg_lends_small_backing_and_detaches_wide_sort_keys() {
    for key_width in [0, 16 * 1024] {
        let execution = execution(1024);
        let memory = &execution.context.memory;
        let baseline = memory.used_bytes();
        let ty = LogicalType::Scalar(ScalarType::I64);
        let key_ty = LogicalType::Scalar(ScalarType::Utf8);
        let schema = Schema::new(vec![field(0, ty.clone()), field(1, key_ty.clone())]).unwrap();
        let aggregate = AggregateExpr {
            field: field(2, LogicalType::list(ty.clone()).maybe()),
            function: AggregateFunction::ArrayAgg,
            argument: Some(schema.fields()[0].expression()),
            distinct: false,
            filter: None,
            order_by: vec![SortExpr { expr: schema.fields()[1].expression(), descending: false, nulls_first: false }],
        };
        let output = Schema::new(vec![aggregate.field.clone()]).unwrap();
        let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
            input: Arc::new(Relation::empty(schema.clone())), groups: vec![], aggregates: vec![aggregate.clone()],
        }).unwrap());
        let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], memory).unwrap();
        let values: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![30, 10, 20]));
        let tail = "x".repeat(key_width);
        let key = Arc::new(arrow_array::StringArray::from(vec![format!("a{tail}"), format!("b{tail}"), format!("c{tail}")]));
        let input = ColumnBatch::settled(vec![
            Column::Arrow { array: Arc::clone(&values), field: Arc::new(rtdb_columnar::to_field("c0", &ty).unwrap()), logical: Some(ty) },
            Column::Arrow { array: key, field: Arc::new(rtdb_columnar::to_field("c1", &key_ty).unwrap()), logical: Some(key_ty) },
        ], 3, memory.reserve(0).unwrap()).unwrap();
        let mut cursor = AggregateCursor::from_batches(Box::new(Batches(Some(input))), false, schema,
            vec![], vec![aggregate], runtime, execution.clone()).unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        drop(cursor);
        let Column::Arrow { array, .. } = &result.columns[0] else { panic!("Expected an Arrow list"); };
        let lists = array.as_list::<i32>();
        assert_eq!(lists.value_offsets(), &[0, 3]);
        assert_eq!(Arc::ptr_eq(lists.values(), &values), key_width == 0);
        assert!(memory.used_bytes() - baseline < 16 * 1024);
        assert_eq!(result.columns[0].value(0).unwrap(), Value::list(vec![Value::I64(30), Value::I64(10), Value::I64(20)]));
        drop((result, values));
        assert_eq!(memory.used_bytes(), baseline);
    }
}

#[tokio::test]
/// XTDB sorts an ordered `ARRAY_AGG`'s rows in memory, so a sort share far smaller than they are spills nothing.
async fn ordered_array_agg_past_its_sort_share_stays_in_memory_and_keeps_mixed_children_and_empty_group_offsets() {
    let execution = execution_with_options(QueryOptions { batch_rows: 4, sort_memory_bytes: 1024, ..Default::default() });
    let baseline = execution.context.memory.used_bytes();
    let int = LogicalType::Scalar(ScalarType::I64);
    let arg = int.join(&LogicalType::Scalar(ScalarType::Utf8)).maybe();
    let schema = Schema::new(vec![field(0, int.clone()), field(1, arg.clone()), field(2, int.clone())]).unwrap();
    let groups = vec![NamedExpr { field: field(3, int), expr: schema.fields()[0].expression() }];
    let key = schema.fields()[2].expression();
    let aggregate = AggregateExpr {
        field: field(4, LogicalType::list(arg).maybe()),
        function: AggregateFunction::ArrayAgg,
        argument: Some(schema.fields()[1].expression()),
        distinct: false,
        filter: Some(Expr::binary(BinaryOp::NotEq, key.clone(), Expr::Literal(Value::I64(2)))),
        order_by: vec![SortExpr { expr: key, descending: false, nulls_first: false }],
    };
    let row = |group, argument, key| vec![Value::I64(group), argument, Value::I64(key)];
    let pages = vec![
        vec![row(1, Value::I64(30), 3), row(2, Value::I64(20), 2), row(1, Value::Null, 1)],
        vec![row(1, Value::Utf8("skip".into()), 2), row(1, Value::Utf8("later tie".into()), 3)],
    ];
    let (mut cursor, _) = cursor(&execution, schema, pages, groups, vec![aggregate], None);
    let result = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert_eq!(batch_row(&result, 0), [Value::I64(1), Value::list(vec![Value::Null, Value::I64(30), Value::Utf8("later tie".into())])]);
    assert_eq!(batch_row(&result, 1), [Value::I64(2), Value::Null]);
    let Column::Arrow { array, .. } = &result.columns[1] else { panic!("Expected an Arrow list"); };
    assert_eq!(array.as_list::<i32>().value_offsets(), &[0, 3, 3]);
    assert_eq!(execution.context.spill_peak_bytes(), 0);
    assert_eq!(execution.context.spill_bytes(), 0);
    drop(result);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn array_order_admission_includes_filtered_and_empty_input() {
    for pages in [vec![vec![vec![Value::I64(1),Value::Set(vec![Value::I64(1)])]]],vec![]] {
        let execution = execution(1);let baseline = execution.context.memory.used_bytes();
        let schema = Schema::new(vec![field(0,LogicalType::Scalar(ScalarType::I64)),field(1,LogicalType::List { kind: rtdb_types::ListKind::Set,element: Box::new(LogicalType::Scalar(ScalarType::I64)) })]).unwrap();
        let aggregate = AggregateExpr { field: field(2,LogicalType::list(LogicalType::Scalar(ScalarType::I64)).maybe()),function: AggregateFunction::ArrayAgg,argument: Some(schema.fields()[0].expression()),distinct: false,filter: Some(Expr::Literal(Value::Bool(false))),order_by: vec![SortExpr { expr: schema.fields()[1].expression(),descending: false,nulls_first: false }] };
        let (mut cursor,_) = cursor(&execution,schema,pages,vec![],vec![aggregate],None);
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"xtdb.expression/function-type-mismatch");drop(cursor);
        assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

fn primitive_cursor(execution: &Execution,function: AggregateFunction,declared: LogicalType,input: LogicalType,pages: Vec<Vec<Value>>,filter: Option<Expr>) -> AggregateCursor {
    let schema = Schema::new(vec![field(0,input.clone())]).unwrap();
    let argument = field(0,declared).expression();
    let mut aggregate = AggregateExpr { field: field(1,LogicalType::Null),function,argument: Some(argument),distinct: false,filter,order_by: vec![] };
    aggregate.field.data_type = AggregateTypes::derive(&aggregate,input,true,EmissionMode::Execution).unwrap().output_type().clone();
    cursor(execution,schema,pages.into_iter().map(|page|page.into_iter().map(|value|vec![value]).collect()).collect(),vec![],vec![aggregate],None).0
}
async fn primitive_result(mut cursor: AggregateCursor) -> Value {
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    page.columns[0].value(0).unwrap()
}

#[tokio::test]
async fn boolean_reducer_admits_unsigned_vector_kinds_before_applying_the_filter() {
    let execution = execution(1);
    let baseline = execution.context.memory.used_bytes();
    for (input, value) in [
        (LogicalType::Scalar(ScalarType::U32), Value::U32(0)),
        (LogicalType::Scalar(ScalarType::U64), Value::U64(0)),
    ] {
        for function in [AggregateFunction::Every, AggregateFunction::Any] {
            for pages in [vec![vec![]], vec![vec![value.clone()]]] {
                let filtered = primitive_cursor(&execution, function.clone(), input.clone(), input.clone(), pages, Some(Expr::Literal(Value::Bool(false))));
                assert_eq!(primitive_result(filtered).await, Value::Null);
            }
            let mixed = input.join(&LogicalType::Scalar(ScalarType::Bool));
            let initialized = primitive_cursor(&execution, function.clone(), mixed.clone(), mixed.clone(), vec![vec![Value::Bool(function == AggregateFunction::Every), value.clone()]], None);
            assert_eq!(primitive_result(initialized).await, Value::Bool(true));
            let mut first = primitive_cursor(&execution, function, mixed.clone(), mixed, vec![vec![value.clone(), Value::Bool(true)]], None);
            assert_eq!(first.next_batch().await.err().unwrap().code, "rtdb.query/boolean-setter");
            drop(first);
            assert_eq!(execution.context.memory.used_bytes(), baseline);
            assert_eq!(execution.context.spill_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn sum_factory_admits_complete_vectors_before_reading_the_mask() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let integer = LogicalType::Scalar(ScalarType::I64);
    for (declared,input,value,code) in [
        (integer.clone(),integer.join(&LogicalType::Scalar(ScalarType::Utf8)),Value::I64(3),"rtdb.query/sum-input"),
        (LogicalType::Scalar(ScalarType::I32),LogicalType::Scalar(ScalarType::F64),Value::F64(3.0),"rtdb.query/sum-input"),
        (LogicalType::Scalar(ScalarType::Decimal { precision: 32,scale: 0,bit_width: 128 }),integer.clone(),Value::I64(3),"rtdb.query/decimal-sum-input"),
        (integer.clone(),LogicalType::RunEndEncoded { width: rtdb_types::RunEndWidth::I32,value: Box::new(integer.clone()) },Value::I64(3),"rtdb.query/sum-input"),
        (LogicalType::Scalar(ScalarType::Duration(TimeUnit::Second)),LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond)),Value::Duration { value: 1,unit: TimeUnit::Millisecond },"rtdb.query/sum-input"),
    ] {
        for pages in [vec![vec![value]],vec![vec![]]] {
            let mut cursor = primitive_cursor(&execution,AggregateFunction::Sum,declared.clone(),input.clone(),pages,Some(Expr::Literal(Value::Bool(false))));
            assert_eq!(cursor.next_batch().await.err().unwrap().code,code);drop(cursor);
            assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
        }
        assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,declared,input,vec![],None)).await,Value::Null);
        assert_eq!(execution.context.memory.used_bytes(),baseline);
    }
}

#[tokio::test]
async fn sum_uses_vector_narrowing_and_float_accumulator_width() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,LogicalType::Scalar(ScalarType::I32),LogicalType::Scalar(ScalarType::I64),vec![vec![Value::I64(i64::MAX),Value::I64(2)]],None)).await,Value::I32(1));
    let near_midpoint = (1i64 << 54) + (1i64 << 30) + 1;
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,LogicalType::Scalar(ScalarType::F32),LogicalType::Scalar(ScalarType::I64),vec![vec![Value::I64(near_midpoint)]],None)).await,Value::F32(((1u64 << 54) + (1u64 << 31)) as f32));
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,LogicalType::Scalar(ScalarType::F32),LogicalType::Scalar(ScalarType::F32),vec![vec![Value::F32(16_777_216.0),Value::F32(1.0)],vec![Value::F32(-16_777_216.0)]],None)).await,Value::F32(0.0));
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[test]
fn primitive_group_sums_wrap_within_the_reserved_state() {
    let execution = execution(32);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(32).unwrap();
    let pressure = memory.reserve(memory.available_bytes()).unwrap();
    let output = LogicalType::Scalar(ScalarType::I64);

    for state in &mut states.values {
        state.add_numeric(NumericCell::Integer(i64::MAX), &output).unwrap();
        state.add_numeric(NumericCell::Integer(1), &output).unwrap();
        assert!(matches!(state.accumulator, ScalarState::I64(i64::MIN)));
    }

    drop((pressure, states));
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn decimal_and_duration_group_sums_fit_in_the_reserved_state() {
    let execution = execution(1024);
    let memory = &execution.context.memory;
    let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 1, scale: 0, bit_width: 128 });
    let duration = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond));
    let decimal_sum = SumFactory::new(&decimal, &decimal).unwrap();
    let duration_sum = SumFactory::new(&duration, &duration).unwrap();
    let nine = Value::Decimal(decimal128_value(9, 1, 0).unwrap());
    let millis = Value::Duration { value: i64::MAX, unit: TimeUnit::Millisecond };
    let mut decimals = ScalarGroups::new(&execution).unwrap();
    let mut durations = ScalarGroups::new(&execution).unwrap();
    decimals.ensure(1024).unwrap();
    durations.ensure(1024).unwrap();
    let _pressure = memory.reserve(memory.available_bytes()).unwrap();
    for (decimal, duration) in decimals.values.iter_mut().zip(&mut durations.values) {
        decimal_sum.add(decimal, &Value::Null, 0, memory).unwrap();
        for index in [1, 2] {
            decimal_sum.add(decimal, &nine, index, memory).unwrap();
            duration_sum.add(duration, &millis, index, memory).unwrap();
        }
        assert!(matches!(decimal.accumulator, ScalarState::Decimal128(bytes) if i128::from_ne_bytes(bytes) == 18));
        assert!(matches!(duration.accumulator, ScalarState::Duration(-2, TimeUnit::Millisecond)));
    }
}

#[test]
fn decimal_sum_switches_to_scale_alignment_and_reports_physical_overflow() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 4, scale: 2, bit_width: 128 });
    let other = LogicalType::Scalar(ScalarType::Decimal { precision: 4, scale: 1, bit_width: 128 });
    let input = decimal.join(&other);
    let sum = SumFactory::new(&input, &decimal).unwrap();
    let number = |coefficient, scale| Value::Decimal(decimal128_value(coefficient, 4, scale).unwrap());
    let mut mixed = ScalarAccumulator::default();
    sum.add(&mut mixed, &number(5, 2), 0, memory).unwrap();
    sum.add(&mut mixed, &number(2, 1), 1, memory).unwrap();
    assert_eq!(mixed.accumulator.retained(), Some(&number(25, 2)));

    let mut wide = ScalarAccumulator::default();
    sum.add(&mut wide, &number(i128::MAX, 2), 0, memory).unwrap();
    assert_eq!(sum.add(&mut wide, &number(1, 2), 1, memory).unwrap_err().code, "xtdb.expression/overflow-error");
}

#[tokio::test]
async fn duration_sum_wraps_conversion_and_distinguishes_null_vector_slots() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let second = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Second));
    let milli = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond));
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,milli.clone(),second.clone(),vec![vec![Value::Duration { value: i64::MAX,unit: TimeUnit::Second }],vec![Value::Duration { value: 1,unit: TimeUnit::Second }]],None)).await,Value::Duration { value: 0,unit: TimeUnit::Millisecond });
    let mut cursor = primitive_cursor(&execution,AggregateFunction::Sum,milli.clone(),milli.clone().maybe(),vec![vec![Value::Duration { value: 5,unit: TimeUnit::Millisecond },Value::Null]],None);
    let error = cursor.next_batch().await.err().unwrap();assert_eq!(error.code,"xtdb.error/unknown");assert_eq!(error.message.as_deref(),Some("null at index 1"));drop(cursor);
    for input in [LogicalType::Null,second.join(&milli).maybe()] {
        assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,milli.clone(),input,vec![vec![Value::Null]],None)).await,Value::Null);
    }
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Sum,milli.clone(),milli.maybe(),vec![vec![Value::Null]],Some(Expr::Literal(Value::Bool(false))))).await,Value::Null);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn average_uses_vector_sums_and_counts_nonnull_cells_after_each_sum_pass() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let double = LogicalType::Scalar(ScalarType::F64);
    let value = primitive_result(primitive_cursor(&execution,AggregateFunction::Average,double.clone(),double.maybe(),vec![vec![Value::Null,Value::F64(-0.0)]],None)).await;
    let Value::F64(value) = value else { panic!("Expected double average"); };assert_eq!(value.to_bits(),(-0.0f64).to_bits());
    let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 32,scale: 1,bit_width: 128 }).maybe();
    let decimal_value = |coefficient: i128|Value::Decimal(rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(),32,1,128).unwrap());
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Average,decimal.clone(),decimal,vec![vec![Value::Null,decimal_value(10)],vec![decimal_value(20)]],None)).await,Value::F64(1.5));
    let duration = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond));
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Average,duration.clone(),LogicalType::Null,vec![vec![Value::Null]],None)).await,Value::Null);
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Average,duration.clone(),duration.clone(),vec![vec![Value::Duration { value: 10,unit: TimeUnit::Millisecond }],vec![Value::Duration { value: 15,unit: TimeUnit::Millisecond }]],None)).await,Value::Duration { value: 12,unit: TimeUnit::Millisecond });
    let union = duration.clone().join(&LogicalType::Scalar(ScalarType::Duration(TimeUnit::Second))).maybe();
    assert_eq!(primitive_result(primitive_cursor(&execution,AggregateFunction::Average,duration.clone(),union,vec![vec![Value::Null,Value::Duration { value: 1,unit: TimeUnit::Second }],vec![Value::Duration { value: 500,unit: TimeUnit::Millisecond }]],None)).await,Value::Duration { value: 750,unit: TimeUnit::Millisecond });
    for pages in [vec![vec![]],vec![vec![Value::Utf8("excluded".into())]]] {
        let mut cursor = primitive_cursor(&execution,AggregateFunction::Average,LogicalType::Scalar(ScalarType::I64),LogicalType::Scalar(ScalarType::Utf8),pages,Some(Expr::Literal(Value::Bool(false))));
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/sum-input");drop(cursor);
    }
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[test]
fn failing_duration_average_sum_does_not_start_the_count_pass() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let input = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond)).maybe();
    let argument = field(0,input.clone()).expression();
    let aggregate = AggregateExpr { field: field(1,input.clone()),function: AggregateFunction::Average,argument: Some(argument),distinct: false,filter: None,order_by: vec![] };
    let types = AggregateTypes::derive(&aggregate,input,true,EmissionMode::Execution).unwrap();
    let mut page = PageBuilder::new(2,&execution.context.memory).unwrap();
    for value in [Value::Duration { value: 10,unit: TimeUnit::Millisecond },Value::Null] { page.push(OwnedRow::new(vec![value],&execution.context.memory).unwrap()).unwrap(); }
    let page = ColumnBatch::from_page(&page.finish(), &execution.context.memory).unwrap();let scope = execution.scope().unwrap();
    let mut states = ScalarGroups::new(&execution).unwrap();
    assert_eq!(states.aggregate(Rows { batch: &page, projected: false, memory: &execution.context.memory },GroupMapping::Global(2),&[true,true],1,Some(0),&aggregate,&types,&scope,&execution).unwrap_err().code,"xtdb.error/unknown");
    assert_eq!(states.values[0].count,0);
    assert!(matches!(states.values[0].accumulator, ScalarState::Duration(10, TimeUnit::Millisecond)));
    drop(states);drop(scope);drop(page);
    assert_eq!(execution.context.memory.used_bytes(),baseline);
}

#[tokio::test]
async fn variance_admits_only_concrete_square_vector_factories_even_without_selected_rows() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    let integer = LogicalType::Scalar(ScalarType::I64);
    for input in [LogicalType::Scalar(ScalarType::I8),LogicalType::Scalar(ScalarType::F32),LogicalType::Scalar(ScalarType::Decimal { precision: 32,scale: 0,bit_width: 128 }),LogicalType::Null,integer.join(&LogicalType::Scalar(ScalarType::F64)),LogicalType::RunEndEncoded { width: rtdb_types::RunEndWidth::I32,value: Box::new(integer.clone()) }] {
        for function in [AggregateFunction::VariancePopulation,AggregateFunction::VarianceSample,AggregateFunction::StddevPopulation,AggregateFunction::StddevSample] {
            let mut cursor = primitive_cursor(&execution,function.clone(),input.clone(),input.clone(),vec![vec![]],Some(Expr::Literal(Value::Bool(false))));
            assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/variance-input");drop(cursor);
            assert_eq!(primitive_result(primitive_cursor(&execution,function,input.clone(),input.clone(),vec![],None)).await,Value::Null);
        }
    }
    let ty = LogicalType::Scalar(ScalarType::F32);
    let mut cursor = primitive_cursor(&execution,AggregateFunction::VariancePopulation,ty.clone(),ty,vec![vec![Value::F32(1.0)]],Some(Expr::Literal(Value::Bool(false))));
    assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/variance-input");drop(cursor);
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

#[tokio::test]
async fn variance_preserves_full_column_square_values_and_nullable_group_counts() {
    for batch_rows in [1,1024] {
        let execution = execution(batch_rows);let baseline = execution.context.memory.used_bytes();
        let ty = LogicalType::Scalar(ScalarType::I64).maybe();
        for (function,expected) in [(AggregateFunction::VariancePopulation,Value::F64(2.0 / 3.0)),(AggregateFunction::VarianceSample,Value::F64(1.0)),(AggregateFunction::StddevSample,Value::F64(1.0))] {
            assert_eq!(primitive_result(primitive_cursor(&execution,function,ty.clone(),ty.clone(),vec![vec![Value::I64(1),Value::Null],vec![],vec![Value::I64(2),Value::I64(3)]],None)).await,expected);
        }
        let page = {
            let mut page = PageBuilder::new(3,&execution.context.memory).unwrap();
            for value in [Value::I64(3),Value::Null,Value::I64(4)] { page.push(OwnedRow::new(vec![value],&execution.context.memory).unwrap()).unwrap(); }
            ColumnBatch::from_page(&page.finish(), &execution.context.memory).unwrap()
        };
        let scope = execution.scope().unwrap();
        assert_eq!(square_column(Rows { batch: &page, projected: false, memory: &execution.context.memory },Some(0),&ty,&scope,&execution).unwrap(),[Some(9.0),None,Some(16.0)]);
        drop(scope);drop(page);
        assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn boolean_reducer_separates_emitted_setter_admission_from_selected_first_value() {
    let execution = execution(1);let baseline = execution.context.memory.used_bytes();
    for function in [AggregateFunction::Every,AggregateFunction::Any] {
        for input in [LogicalType::Scalar(ScalarType::I8),LogicalType::Scalar(ScalarType::Utf8),LogicalType::Scalar(ScalarType::Decimal { precision: 32,scale: 0,bit_width: 128 })] {
            let mut cursor = primitive_cursor(&execution,function.clone(),input.clone(),input.clone(),vec![vec![]],Some(Expr::Literal(Value::Bool(false))));
            assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/boolean-setter");drop(cursor);
            assert_eq!(primitive_result(primitive_cursor(&execution,function.clone(),input.clone(),input,vec![],None)).await,Value::Null);
        }
        let ty = LogicalType::Scalar(ScalarType::Bool).join(&LogicalType::Scalar(ScalarType::I64));
        assert_eq!(primitive_result(primitive_cursor(&execution,function.clone(),ty.clone(),ty.clone(),vec![vec![Value::Bool(true)],vec![Value::I64(0)]],None)).await,Value::Bool(true));
        let mut cursor = primitive_cursor(&execution,function.clone(),ty.clone(),ty,vec![vec![Value::I64(0),Value::Bool(true)]],None);
        assert_eq!(cursor.next_batch().await.err().unwrap().code,"rtdb.query/boolean-setter");drop(cursor);
        let ty = LogicalType::Scalar(ScalarType::I64);
        assert_eq!(primitive_result(primitive_cursor(&execution,function,ty.clone(),ty,vec![vec![Value::I64(0)]],Some(Expr::Literal(Value::Bool(false))))).await,Value::Null);
    }
    assert_eq!(execution.context.memory.used_bytes(),baseline);assert_eq!(execution.context.spill_bytes(),0);
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }

#[tokio::test]
async fn arrow_wave4_numeric_aggregates_match_scalar_rows_and_float_bits() {
    let execution = execution(8);
    let context = &execution.context;
    let baseline = context.memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::F64).maybe();
    let input_field = field(0, ty.clone());
    let schema = Schema::new(vec![input_field.clone()]).unwrap();
    for values in [
        vec![Value::F64(-0.0), Value::Null, Value::F64(0.0), Value::F64(-0.0)],
        vec![Value::F64(f64::from_bits(0x7ff8_0000_0000_0001)), Value::F64(1.0), Value::F64(f64::from_bits(0x7ff8_0000_0000_0002))],
        vec![Value::Null, Value::Null],
    ] {
        for (function, distinct) in [
            (AggregateFunction::Min, false), (AggregateFunction::Max, false),
            (AggregateFunction::Sum, true), (AggregateFunction::Average, true),
            (AggregateFunction::VariancePopulation, false), (AggregateFunction::Count, true),
            (AggregateFunction::Percentile {
                kind: PercentileKind::Continuous, fraction: PercentileFraction::Literal(0.5), descending: false,
            }, false),
        ] {
            let mut aggregate = AggregateExpr {
                field: field(1, LogicalType::Null), function, argument: Some(input_field.expression()),
                distinct, filter: None, order_by: vec![],
            };
            aggregate.field.data_type = AggregateTypes::derive(&aggregate, ty.clone(), true, EmissionMode::Execution).unwrap().output_type().clone();
            let input = Arc::new(Relation::new(schema.clone(), RelationNode::Values { rows: vec![] }).unwrap());
            let output = Schema::new(vec![aggregate.field.clone()]).unwrap();
            let relation = Arc::new(Relation::new(output, RelationNode::Aggregate {
                input, groups: vec![], aggregates: vec![aggregate.clone()],
            }).unwrap());
            let runtime = EmittedRelation::derive(relation, &context.catalog, &[], &context.memory).unwrap();
            let batch = ColumnBatch::settled(vec![Column::Arrow {
                array: rtdb_columnar::copy_array(&ty, &values).unwrap(),
                field: Arc::new(rtdb_columnar::to_field(&input_field.name, &ty).unwrap()),
                logical: Some(ty.clone()),
            }], values.len(), context.memory.reserve(4096).unwrap()).unwrap();
            let mut vector = AggregateCursor::from_batches(Box::new(Batches(Some(batch))), false, schema.clone(), vec![], vec![aggregate.clone()], runtime, execution.clone()).unwrap();
            let expected = primitive_result(cursor(&execution, schema.clone(),
                vec![values.iter().cloned().map(|value| vec![value]).collect()], vec![], vec![aggregate], None).0).await;
            let output = vector.next_batch().await.unwrap().unwrap();
            let actual = output.columns[0].value(0).unwrap();
            match (expected, actual) {
                (Value::F64(expected), Value::F64(actual)) => assert_eq!(expected.to_bits(), actual.to_bits()),
                (expected, actual) => assert_eq!(expected, actual),
            }
        }
    }
    assert_eq!(context.memory.used_bytes(), baseline);
}

/// Nullable groups and arguments, with many groups, repeated arguments and rows of a group spread over every page.
fn spread_pages() -> (Schema, Vec<Vec<Vec<Value>>>) {
    let ty = LogicalType::Scalar(ScalarType::I64).maybe();
    let schema = Schema::new(vec![field(0, ty.clone()), field(1, ty)]).unwrap();
    let pages = (0..40_i64).map(|page| (0..50_i64).map(|row| {
        let n = page * 50 + row;
        let group = if n % 23 == 0 { Value::Null } else { Value::I64((n * 7919) % 613) };
        vec![group, if n % 7 == 0 { Value::Null } else { Value::I64(n % 5) }]
    }).collect()).collect();
    (schema, pages)
}

fn spread_aggregates(schema: &Schema) -> (Vec<NamedExpr>, Vec<AggregateExpr>) {
    let argument = schema.fields()[1].expression();
    let nullable = LogicalType::Scalar(ScalarType::I64).maybe();
    let groups = vec![NamedExpr { field: field(2, nullable.clone()), expr: schema.fields()[0].expression() }];
    let aggregates = vec![
        count(3, None),
        AggregateExpr { function: AggregateFunction::Count, argument: Some(argument.clone()), ..count(4, None) },
        AggregateExpr { field: field(5, nullable.clone()), function: AggregateFunction::Sum, argument: Some(argument.clone()), distinct: false, filter: None, order_by: vec![] },
        AggregateExpr { field: field(6, nullable), function: AggregateFunction::Sum, argument: Some(argument.clone()), distinct: true, filter: None, order_by: vec![] },
        AggregateExpr { field: field(7, LogicalType::list(argument.data_type())), function: AggregateFunction::ArrayAgg, argument: Some(argument), distinct: false, filter: None, order_by: vec![] },
    ];
    (groups, aggregates)
}

/// Every group's output row by its key, which identifies it whatever order the groups arrive in.
async fn drain_groups(cursor: &mut AggregateCursor) -> BTreeMap<String, Vec<Value>> {
    let mut groups = BTreeMap::new();
    while let Some(page) = cursor.next_batch().await.unwrap() {
        for row in 0..page.len {
            let values = batch_row(&page, row);
            assert!(groups.insert(format!("{:?}", values[0]), values).is_none(), "a group is emitted once");
        }
    }
    groups
}

#[tokio::test]
async fn grouping_holds_every_group_in_memory_and_never_spills() {
    let (schema, pages) = spread_pages();
    let (groups, aggregates) = spread_aggregates(&schema);
    let execution = execution_with_options(QueryOptions { batch_rows: 50, ..Default::default() });
    let baseline = execution.context.memory.used_bytes();
    let (mut grouped, _) = cursor(&execution, schema, pages, groups, aggregates, None);
    assert_eq!(drain_groups(&mut grouped).await.len(), 614);
    assert_eq!(execution.context.spill_peak_bytes(), 0);
    drop(grouped);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

/// XTDB's group-by keeps its groups in memory and fails the query when its allocator refuses them; so does this.
#[tokio::test]
async fn grouping_past_the_query_budget_fails_rather_than_spilling() {
    let (schema, pages) = spread_pages();
    let (groups, aggregates) = spread_aggregates(&schema);
    let reference = execution_with_options(QueryOptions { batch_rows: 50, ..Default::default() });
    let (mut grouped, _) = cursor(&reference, schema.clone(), pages.clone(), groups.clone(), aggregates.clone(), None);
    drain_groups(&mut grouped).await;
    drop(grouped);
    let peak = reference.context.memory.peak_bytes();

    let execution = execution_with_budget(QueryOptions { batch_rows: 50, ..Default::default() }, peak - 1);
    let (mut grouped, _) = cursor(&execution, schema, pages, groups, aggregates, None);
    let error = loop {
        match grouped.next_batch().await {
            Ok(Some(_)) => (),
            Ok(None) => panic!("the grouping fits a budget below its peak"),
            Err(error) => break error,
        }
    };
    assert!(error.is(rtdb_types::AnomalyKind::ResourceExhausted), "{error:?}");
    assert_eq!(execution.context.spill_peak_bytes(), 0);
}
