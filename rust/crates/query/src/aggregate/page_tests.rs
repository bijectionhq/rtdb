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

struct Pages(VecDeque<ColumnBatch>);
#[async_trait]
impl BatchCursor for Pages {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.pop_front()) }
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
async fn grouped_count_excludes_null_runs_with_or_without_a_filter() {
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
    for filter in [None, Some(Expr::Literal(Value::Bool(true)))] {
        let aggregates = vec![AggregateExpr {
            function: AggregateFunction::Count,
            argument: Some(schema.fields()[1].expression()),
            ..count(3, filter)
        }];
        let output = Schema::new(vec![groups[0].field.clone(), aggregates[0].field.clone()]).unwrap();
        let relation = Arc::new(Relation::new(output, RelationNode::Aggregate { input: Arc::clone(&input), groups: groups.clone(), aggregates: aggregates.clone() }).unwrap());
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
            Box::new(Batches(Some(batch))), false, schema.clone(), groups.clone(), aggregates, runtime, execution.clone(),
        ).unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(result.len, 1);
        assert_eq!(result.columns[0].value(0).unwrap(), Value::F64(1.0));
        assert_eq!(result.columns[1].value(0).unwrap(), Value::I64(1));
    }
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
    assert!(!accepts_masked_count(&batch.columns[0]));
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
async fn mixed_numeric_percentiles_rank_in_double_order_and_return_the_exact_winner() {
    let execution = execution(8);
    let baseline = execution.context.memory.used_bytes();
    let winner = Value::Decimal(decimal128_value(9_007_199_254_740_993, 20, 0).unwrap());
    let peer = Value::I64(9_007_199_254_740_992);
    let ty = winner.logical_type().join(&peer.logical_type());
    let function = AggregateFunction::Percentile {
        kind: PercentileKind::Discrete, fraction: PercentileFraction::Literal(0.0), descending: false,
    };
    let mut cursor = primitive_cursor(&execution, function, ty.clone(), ty, vec![vec![winner.clone()], vec![peer]], None);
    let page = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert!(matches!(&page.columns[0], Column::Selected(_)));
    assert_eq!(page.columns[0].value(0).unwrap(), winner);
    drop(page);
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn mixed_discrete_percentiles_validate_only_the_selected_decimal_winner() {
    let execution = execution(8);
    let decimal = Value::Decimal(decimal128_value(100, 2, 0).unwrap());
    let integer = Value::I64(1);
    let ty = decimal.logical_type().join(&integer.logical_type());
    let make_cursor = |fraction| primitive_cursor(
        &execution,
        AggregateFunction::Percentile { kind: PercentileKind::Discrete, fraction: PercentileFraction::Literal(fraction), descending: false },
        ty.clone(), ty.clone(), vec![vec![decimal.clone()], vec![integer.clone()]], None,
    );
    assert_eq!(primitive_result(make_cursor(0.0)).await, integer);
    let mut cursor = make_cursor(1.0);
    assert_eq!(cursor.next_batch().await.err().unwrap().code, "rtdb/decimal-precision");
}

#[tokio::test]
async fn discrete_keyword_percentile_reuses_the_winners_arrow_backing() {
    let execution = execution(8);
    let baseline = execution.context.memory.used_bytes();
    let keyword = |value| Value::Keyword(rtdb_types::Keyword::from_name(value));
    let winner = keyword("a");
    let ty = winner.logical_type();
    let function = AggregateFunction::Percentile {
        kind: PercentileKind::Discrete, fraction: PercentileFraction::Literal(0.0), descending: false,
    };
    let mut cursor = primitive_cursor(&execution, function, ty.clone(), ty, vec![vec![keyword("z"), winner.clone()]], None);
    let page = cursor.next_batch().await.unwrap().unwrap();
    drop(cursor);
    assert!(matches!(&page.columns[0], Column::Selected(_)));
    assert_eq!(page.columns[0].value(0).unwrap(), winner);
    drop(page);
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

#[tokio::test]
async fn discrete_percentiles_keep_month_day_peer_fields_and_nullable_layouts() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let days = i32::MAX;
    let wrapped = i64::from(days).wrapping_mul(86_400_000_000_000).wrapping_sub(3);
    for micro in [false, true] {
        let interval = |months, days, nanoseconds| if micro {
            Value::IntervalMonthDayMicro { months, days, nanoseconds }
        } else {
            Value::IntervalMonthDayNano { months, days, nanoseconds }
        };
        let winner = interval(-1, days, -3);
        let peer = interval(0, 0, wrapped);
        let singleton = interval(1, 0, 0);
        let ty = winner.logical_type();
        let int = LogicalType::Scalar(ScalarType::I64);
        let schema = Schema::new(vec![field(0, int.clone()), field(1, ty.clone().maybe())]).unwrap();
        let groups = vec![NamedExpr { field: field(2, int), expr: schema.fields()[0].expression() }];
        let function = AggregateFunction::Percentile {
            kind: PercentileKind::Discrete, fraction: PercentileFraction::Literal(0.5), descending: true,
        };
        let aggregate = AggregateExpr {
            field: field(3, ty.clone().maybe()), function: function.clone(),
            argument: Some(schema.fields()[1].expression()), distinct: false, filter: None, order_by: vec![],
        };
        let pages = vec![
            vec![vec![Value::I64(1), winner.clone()], vec![Value::I64(2), Value::Null]],
            vec![vec![Value::I64(1), peer.clone()], vec![Value::I64(3), singleton.clone()]],
        ];
        let (mut grouped, _) = cursor(&execution, schema, pages, groups, vec![aggregate], None);
        let result = grouped.next_batch().await.unwrap().unwrap();
        drop(grouped);
        let column = &result.columns[1];
        let Some(Some((array, field, logical, _))) = column.arrow_cell(0) else {
            panic!("Expected a selected Arrow interval winner");
        };
        let expected = rtdb_columnar::to_field(field.name(), &ty).unwrap();
        assert_eq!(field, &expected);
        assert_eq!(array.data_type(), expected.data_type());
        assert_eq!(logical, Some(&ty));
        assert!(matches!(column.arrow_cell(1), Some(None)));
        assert_eq!(batch_row(&result, 0), [Value::I64(1), winner]);
        assert_eq!(batch_row(&result, 1), [Value::I64(2), Value::Null]);
        assert_eq!(batch_row(&result, 2), [Value::I64(3), singleton.clone()]);
        drop(result);

        let mut compared = primitive_cursor(&execution, function, ty.clone(), ty,
            vec![vec![peer, singleton]], None);
        assert_eq!(compared.next_batch().await.err().unwrap().code,
            "xtdb.expression/cannot-compare-mdn-interval-with-months");
        drop(compared);
    }
    assert_eq!(memory.used_bytes(), baseline);
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
async fn aggregate_filter_bitmap_precedes_distinct_and_keeps_unknown_equalities() {
    let execution = execution(8);
    let value = LogicalType::Scalar(ScalarType::I64).maybe();
    let truth = LogicalType::Scalar(ScalarType::Bool).maybe();
    let schema = Schema::new(vec![field(0, value.clone()), field(1, truth)]).unwrap();
    let filter = schema.fields()[1].expression();
    let collect = AggregateExpr {
        field: field(2, LogicalType::list(value).maybe()),
        function: AggregateFunction::ArrayAgg,
        argument: Some(schema.fields()[0].expression()),
        distinct: true,
        filter: Some(filter),
        order_by: vec![],
    };
    let pages = vec![
        vec![vec![Value::I64(9), Value::Null], vec![Value::Null, Value::Bool(true)], vec![Value::I64(3), Value::Bool(true)]],
        vec![vec![Value::I64(9), Value::Bool(true)], vec![Value::I64(3), Value::Bool(true)],
            vec![Value::Null, Value::Bool(true)], vec![Value::I64(4), Value::Bool(false)]],
    ];
    let (mut cursor, _) = cursor(&execution, schema, pages, vec![], vec![collect], None);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(page.columns[0].value(0).unwrap(), Value::list(vec![Value::Null, Value::I64(3), Value::I64(9), Value::Null]));
}

#[tokio::test]
async fn fixed_aggregate_results_keep_declared_layout_nulls_and_page_custody() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let integer = LogicalType::Scalar(ScalarType::I64);
    let duration = |value| Value::Duration { value, unit: TimeUnit::Millisecond };
    let decimal = |coefficient, precision, scale| Value::Decimal(decimal128_value(coefficient, precision, scale).unwrap());
    let timestamp = |value, unit, timezone: &str| Value::Timestamp {
        value, unit, timezone: Some(timezone.into()),
    };
    for (function, left, right, expected) in [
        (AggregateFunction::Sum, Value::I32(i32::MAX), Value::I32(1), Value::I32(i32::MIN)),
        (AggregateFunction::Sum, Value::I64(i64::MAX), Value::I64(1), Value::I64(i64::MIN)),
        (AggregateFunction::Sum, Value::F32(-0.0), Value::F32(-0.0), Value::F32(-0.0)),
        (AggregateFunction::Average, duration(-10), duration(-15), duration(-12)),
        (AggregateFunction::Sum, decimal(9, 1, 0), decimal(9, 1, 0), decimal(18, 1, 0)),
        (AggregateFunction::Sum, decimal(1, 20, 0), decimal(2, 20, 2), decimal(102, 22, 2)),
        (AggregateFunction::Max, decimal(9_007_199_254_740_992, 2, 0), decimal(9_007_199_254_740_993, 2, 0),
            decimal(9_007_199_254_740_993, 2, 0)),
        (AggregateFunction::Every, Value::Bool(true), Value::Bool(false), Value::Bool(false)),
        (AggregateFunction::Any, Value::Bool(false), Value::Bool(true), Value::Bool(true)),
        (AggregateFunction::VariancePopulation, Value::F64(2.0), Value::F64(4.0), Value::F64(1.0)),
        (AggregateFunction::Max, Value::I16(-257), Value::I16(1300), Value::I16(1300)),
        (AggregateFunction::Min,
            Value::Time { value: 19, unit: TimeUnit::Millisecond, bit_width: 32 },
            Value::Time { value: 11, unit: TimeUnit::Millisecond, bit_width: 32 },
            Value::Time { value: 11, unit: TimeUnit::Millisecond, bit_width: 32 }),
        (AggregateFunction::Min,
            timestamp(1_700_000_000_000_000, TimeUnit::Microsecond, "America/New_York"),
            timestamp(1_700_000_000_000_000_001, TimeUnit::Nanosecond, "UTC"),
            timestamp(1_700_000_000_000_000_000, TimeUnit::Nanosecond, "Z")),
    ] {
        let ty = left.logical_type().join(&right.logical_type());
        let result_type = expected.logical_type().maybe();
        let schema = Schema::new(vec![field(0, integer.clone()), field(1, ty.clone())]).unwrap();
        let group = NamedExpr { field: field(2, integer.clone()), expr: schema.fields()[0].expression() };
        let aggregate = AggregateExpr {
            field: field(3, result_type.clone()), function: function.clone(),
            argument: Some(schema.fields()[1].expression()), distinct: false,
            filter: Some(Expr::binary(BinaryOp::NotEq, schema.fields()[0].expression(), Expr::Literal(Value::I64(1)))),
            order_by: vec![],
        };
        let pages = vec![
            vec![vec![Value::I64(2), left.clone()], vec![Value::I64(1), left]],
            vec![vec![Value::I64(2), right]],
        ];
        let (mut cursor, _) = cursor(&execution, schema, pages, vec![group], vec![aggregate], None);
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);
        let Column::Arrow { array, field, logical } = &page.columns[1] else { panic!("Expected an Arrow result"); };
        assert_eq!(field.as_ref(), &rtdb_columnar::to_field("c3", &result_type).unwrap());
        assert_eq!(logical.as_ref(), Some(&result_type));
        assert_eq!(array.data_type(), field.data_type());
        assert_eq!(page.columns[0].value(0).unwrap(), Value::I64(2));
        assert_eq!(page.columns[0].value(1).unwrap(), Value::I64(1));
        let actual = page.columns[1].value(0).unwrap();
        assert_eq!(actual, expected);
        if let (Value::F32(actual), Value::F32(expected)) = (actual, expected) {
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
        assert!(array.is_null(1));
        assert!(page.column_reservation(1).bytes() >= array.get_array_memory_size());
        drop(page);
        assert_eq!(memory.used_bytes(), baseline);

        let mut empty = primitive_cursor(&execution, function, ty.clone(), ty, vec![], None);
        let page = empty.next_batch().await.unwrap().unwrap();
        let Column::Arrow { array, field, .. } = &page.columns[0] else { panic!("Expected an Arrow result"); };
        assert_eq!(field.as_ref(), &rtdb_columnar::to_field("c1", &result_type).unwrap());
        assert_eq!(array.data_type(), field.data_type());
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        drop((page, empty));
        assert_eq!(memory.used_bytes(), baseline);
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

#[test]
fn aggregate_input_inline_extrema_and_boolean_fit_in_reserved_group_state() {
    use crate::columnar::SelectedColumn;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let mut inputs = Vec::new();
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        for timezone in [None, Some("America/New_York".to_owned())] {
            let value = |value| Value::Timestamp { value, unit, timezone: timezone.clone() };
            inputs.push((value(i64::MIN), value(i64::MAX)));
        }
        let bit_width = if unit < TimeUnit::Microsecond { 32 } else { 64 };
        inputs.push((Value::Time { value: 1, unit, bit_width }, Value::Time { value: 2, unit, bit_width }));
        inputs.push((Value::Duration { value: -9, unit }, Value::Duration { value: 9, unit }));
    }
    inputs.push((Value::Bool(false), Value::Bool(true)));
    inputs.extend([
        (Value::I8(-2), Value::I8(3)),
        (Value::I16(-234), Value::I16(901)),
        (Value::I32(-132_999), Value::I32(267_444)),
        (Value::I64(i64::MIN), Value::I64(i64::MAX)),
        (Value::F32(-9.5), Value::F32(0.0)),
        (Value::F64(-1.5), Value::F64(19.25)),
        (Value::DateDay(i32::MIN), Value::DateDay(i32::MAX)),
        (Value::DateMilli(-1), Value::DateMilli(4)),
        (Value::Decimal(decimal128_value(9_007_199_254_740_992, 20, 2).unwrap()),
            Value::Decimal(decimal128_value(9_007_199_254_740_993, 20, 2).unwrap())),
    ]);
    for (lesser, greater) in inputs {
        let input = lesser.logical_type().maybe();
        let functions = if matches!(lesser, Value::Bool(_)) {
            [AggregateFunction::Every, AggregateFunction::Any]
        } else {
            [AggregateFunction::Min, AggregateFunction::Max]
        };
        let arrow_field = Arc::new(rtdb_columnar::to_field("c0", &input).unwrap());
        let array = rtdb_columnar::copy_array(&input, &[greater.clone(), Value::Null, lesser.clone()]).unwrap();
        let picks = Arc::new(vec![(0, 2), (1, 0), (0, 0), (0, 1), (0, 2)]);
        let selected = SelectedColumn::new(vec![Some((array, arrow_field, Some(input.clone()), None)), None], picks);
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 5, memory.reserve(4096).unwrap()).unwrap();
        let page = Rows::columns(&batch, &[0], true, &execution.context).unwrap();
        let scope = execution.scope().unwrap();
        for (function, expected) in functions.into_iter().zip([lesser.clone(), greater.clone()]) {
            let aggregate = AggregateExpr {
                field: field(1, input.clone()), function,
                argument: Some(field(0, input.clone()).expression()), distinct: false, filter: None, order_by: vec![],
            };
            let types = AggregateTypes::derive(&aggregate, input.clone(), false, EmissionMode::Execution).unwrap();
            let mut states = ScalarGroups::new(&execution).unwrap();
            states.ensure(3).unwrap();
            let pressure = memory.reserve(memory.available_bytes()).unwrap();
            states.aggregate(page, GroupMapping::Grouped(&[0, 1, 0, 1, 2]), |row| row != 4,
                3, Some(0), &aggregate, &types, &scope, &execution).unwrap();
            drop(pressure);
            assert_eq!(states.values[0].finish(&aggregate, types.output_type(), &scope).unwrap(), expected);
            for state in &states.values[1..] {
                assert_eq!(state.finish(&aggregate, types.output_type(), &scope).unwrap(), Value::Null);
            }
        }
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn numeric_winner_keeps_nan_bits_and_signed_zero_across_input_routes() {
    let execution = execution(8);
    let output = LogicalType::Scalar(ScalarType::F64).maybe();
    let first_nan = f64::from_bits(0x7ff8_0000_0000_1000);
    let next_nan = f64::from_bits(0xfff8_0000_0000_2000);
    for (minimum, typed, generic, expected) in [
        (false, first_nan, next_nan, first_nan),
        (false, -0.0, 0.0, 0.0),
        (true, 0.0, -0.0, -0.0),
        (true, first_nan, 44.0, 44.0),
    ] {
        let mut state = ScalarAccumulator::default();
        state.numeric_extremum(NumericCell::Float64(typed), &output, minimum).unwrap();
        state.scalar_extremum(&Value::F64(generic), &output, minimum, &execution, &execution.context.memory).unwrap();
        let ScalarState::F64(winner) = state.accumulator else { panic!("Expected an inline numeric winner"); };
        assert_eq!(winner.to_bits(), expected.to_bits());
    }
}

#[test]
fn signed_extrema_widen_arrow_operands_and_validate_reached_narrowing() {
    use crate::columnar::SelectedColumn;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let narrow = LogicalType::Scalar(ScalarType::I16);
    let wide = LogicalType::Scalar(ScalarType::I64).maybe();
    let input = narrow.join(&wide);
    let source = |ty, values: &[Value]| {
        let field = Arc::new(rtdb_columnar::to_field("c0", &ty).unwrap());
        let array = rtdb_columnar::copy_array(&ty, values).unwrap();
        Some((array, field, Some(ty), None))
    };
    let selected = SelectedColumn::new(vec![
        source(narrow.clone(), &[Value::I16(i16::MAX), Value::I16(-9)]),
        source(wide.clone(), &[Value::I64(i64::MIN), Value::Null]),
    ], Arc::new(vec![(0, 0), (1, 0), (0, 1), (1, 1)]));
    let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 4, memory.reserve(4096).unwrap()).unwrap();
    let page = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
    let aggregate = |function, ty: LogicalType| AggregateExpr {
        field: field(1, ty.clone().maybe()), function, argument: Some(field(0, ty).expression()),
        distinct: false, filter: None, order_by: vec![],
    };
    let scope = execution.scope().unwrap();
    for (function, expected) in [(AggregateFunction::Min, i64::MIN), (AggregateFunction::Max, i64::from(i16::MAX))] {
        let extrema = aggregate(function, input.clone());
        let types = AggregateTypes::derive(&extrema, input.clone(), false, EmissionMode::Execution).unwrap();
        assert_eq!(types.output_type(), &wide);
        let mut states = ScalarGroups::new(&execution).unwrap();
        states.ensure(3).unwrap();
        let pressure = memory.reserve(memory.available_bytes()).unwrap();
        states.aggregate(page, GroupMapping::Grouped(&[0, 0, 1, 2]), |_| true,
            3, Some(0), &extrema, &types, &scope, &execution).unwrap();
        drop(pressure);
        let (column, _charge) = fixed_scalar_results(&states, &extrema.function, &field(1, wide.clone()), &execution.context)
            .unwrap().unwrap();
        assert_eq!(column.value(0).unwrap(), Value::I64(expected));
        assert_eq!(column.value(1).unwrap(), Value::I64(-9));
        assert_eq!(column.value(2).unwrap(), Value::Null);
    }

    let maximum = aggregate(AggregateFunction::Max, narrow);
    let types = AggregateTypes::derive(&maximum, input, true, EmissionMode::Execution).unwrap();
    let mut narrowed = ScalarGroups::new(&execution).unwrap();
    let error = narrowed.aggregate(page, GroupMapping::Global(4), |_| true, 1,
        Some(0), &maximum, &types, &scope, &execution).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/overflow-error");
    drop((narrowed, scope, batch));
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn date64_extremum_keeps_the_first_tick_on_a_calendar_day() {
    let execution = execution(8);
    let output = field(0, LogicalType::Scalar(ScalarType::DateMilli).maybe());
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(2).unwrap();
    states.values[0].date_extremum(-1, true, true);
    states.values[0].scalar_extremum(&Value::DateMilli(-3), &output.data_type, true, &execution, &execution.context.memory).unwrap();
    let (column, _charge) = fixed_scalar_results(&states, &AggregateFunction::Min, &output, &execution.context).unwrap().unwrap();
    assert_eq!(column.value(0).unwrap(), Value::DateMilli(-1));
    assert_eq!(column.value(1).unwrap(), Value::Null);
}

#[test]
fn time32_extremum_checks_winning_ticks_at_publication() {
    let execution = execution(8);
    let output = field(0, LogicalType::Scalar(ScalarType::Time { unit: TimeUnit::Millisecond, bit_width: 32 }).maybe());
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    states.values[0].temporal_extremum(i64::from(i32::MAX) + 1, false).unwrap();
    let error = fixed_scalar_results(&states, &AggregateFunction::Max, &output, &execution.context).err().unwrap();
    assert_eq!(error.code, "rtdb/time-overflow");
    assert!(error.is(rtdb_types::AnomalyKind::NumericValueOutOfRange));
}

#[test]
fn variance_and_average_read_nullable_numeric_runs() {
    use arrow_array::{Int16Array, Int64Array, RunArray};
    let execution = execution(8);
    let memory = &execution.context.memory;
    let input = LogicalType::Scalar(ScalarType::I64).maybe();
    let array = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 3, 5]), &Int64Array::from(vec![Some(5), None, Some(2)]),
    ).unwrap();
    let field = Arc::new(ArrowField::new("c0", array.data_type().clone(), false));
    let batch = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(array), field, logical: None }], 5, memory.reserve(4096).unwrap()).unwrap();
    let rows = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
    let scope = execution.scope().unwrap();
    let column = NumericColumn::bind(&batch.columns[0], None).expect("A scalar run is admitted");
    assert!(column.get(2).unwrap().is_none());
    assert_eq!(square_column(rows, Some(0), &input, &scope, &execution).unwrap(), vec![Some(25.0), Some(25.0), None, Some(4.0), Some(4.0)]);
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    let mask = [true, true, true, false, true];
    let output = LogicalType::Scalar(ScalarType::F64);
    let factory = SumFactory::new(&input, &output).unwrap();
    states.sum_page(rows, GroupMapping::Global(5), |row| mask[row], Some(0), &factory, &execution).unwrap();
    states.count_page(rows, GroupMapping::Global(5), |row| mask[row], Some(0), &execution).unwrap();
    assert_eq!(states.values[0].average_double(), Some(4.0));
}

#[test]
fn variance_and_count_read_active_numeric_union_cells() {
    use arrow_array::{Int64Array, NullArray, UInt64Array, UnionArray};
    use arrow_schema::UnionFields;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let fields = UnionFields::try_new([3, 6, 9], [
        ArrowField::new("i64", DataType::Int64, true),
        ArrowField::new("u64", DataType::UInt64, false),
        ArrowField::new("null", DataType::Null, true),
    ]).unwrap();
    let array = UnionArray::try_new(fields, vec![3_i8, 6, 9, 3].into(), Some(vec![0_i32, 0, 0, 1].into()), vec![
        Arc::new(Int64Array::from(vec![Some(-4), None])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(NullArray::new(1)),
    ]).unwrap();
    let len = array.len();
    let field = Arc::new(ArrowField::new("c0", array.data_type().clone(), false));
    let input = rtdb_columnar::execution_field_type(&field).unwrap();
    assert_eq!(input, LogicalType::Scalar(ScalarType::I64).maybe());
    let batch = ColumnBatch::settled(vec![Column::Arrow { array: Arc::new(array), field, logical: None }],
        len, memory.reserve(4096).unwrap()).unwrap();
    let page = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
    let scope = execution.scope().unwrap();
    let scratch = len * size_of::<Option<f64>>() + 256;
    let pressure = memory.reserve(memory.available_bytes() - scratch).unwrap();
    let squares = square_column(page, Some(0), &input, &scope, &execution).unwrap();
    assert_eq!(squares, [Some(16.0), Some(1.0), None, None]);
    let typed = page.arrow_column(Some(0)).filter(|column| accepts_masked_count(column));
    assert!(typed.is_some());
    let present = [true, true, false, false];
    for (row, expected) in present.into_iter().enumerate() {
        assert_eq!(count_is_present(page, typed, row, Some(0)).unwrap(), expected);
    }
    drop((squares, pressure, scope));

    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    let output = LogicalType::Scalar(ScalarType::F64);
    let factory = SumFactory::new(&input, &output).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
    let selected = |row| row != 0;
    states.sum_page(page, GroupMapping::Global(len), selected, Some(0), &factory, &execution).unwrap();
    states.count_page(page, GroupMapping::Global(len), selected, Some(0), &execution).unwrap();
    assert_eq!(states.values[0].average_double(), Some(-1.0));
    drop((pressure, states, batch));
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn byte_extremum_result_owns_the_winners_and_keeps_empty_groups_null() {
    let execution = execution(8);
    let context = &execution.context;
    let baseline = context.memory.used_bytes();
    for winner in [
        Value::Utf8(String::new()), Value::Binary(Vec::new()),
        Value::Utf8("winner".repeat(1024)), Value::Binary(vec![0xff; 8192]),
        Value::Keyword(rtdb_types::Keyword::from_name("")),
        Value::Keyword(rtdb_types::Keyword::from_name(":ns/é".repeat(1024))),
    ] {
        let field = field(0, winner.logical_type().maybe());
        let (column, charge) = {
            let mut states = ScalarGroups::new(&execution).unwrap();
            states.ensure(2).unwrap();
            states.values[1].accumulator = ScalarState::Retained(OwnedRow::new(vec![winner.clone()], &context.memory).unwrap());
            byte_scalar_results(&states, &AggregateFunction::Max, &field, context).unwrap().unwrap()
        };
        let batch = ColumnBatch::settled(vec![], 2, context.memory.reserve(256).unwrap()).unwrap();
        let mut batch = batch;
        batch.append_column(column, charge, &context.memory).unwrap();
        assert_eq!(batch.columns[0].value(0).unwrap(), Value::Null);
        assert_eq!(batch.columns[0].value(1).unwrap(), winner);
        drop(batch);
        assert_eq!(context.memory.used_bytes(), baseline);
    }
}

#[test]
fn byte_extrema_without_winners_emit_nullable_arrow_null() {
    let execution = execution(8);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    for (ty, len) in [(ScalarType::Utf8, 3), (ScalarType::Binary, 3), (ScalarType::Keyword, 3), (ScalarType::Keyword, 0)] {
        let field = field(0, LogicalType::Scalar(ty).maybe());
        let mut states = ScalarGroups::new(&execution).unwrap();
        states.ensure(len).unwrap();
        let (expected, reference_charge) = crate::columnar::pack_evaluated_column(&vec![Value::Null; len], memory).unwrap();
        let Column::Arrow { field: expected_field, .. } = expected else { panic!("Expected nullable Arrow Null"); };
        let pressure = memory.reserve(memory.available_bytes() - 512).unwrap();
        let (result, charge) = byte_scalar_results(&states, &AggregateFunction::Min, &field, context).unwrap().unwrap();
        drop((pressure, states));
        let Column::Arrow { array, field, logical } = &result else { panic!("Expected nullable Arrow Null"); };
        assert_eq!(field, &expected_field);
        assert_eq!(array.data_type(), &DataType::Null);
        assert_eq!(logical.as_ref(), Some(&LogicalType::Null));
        assert_eq!(array.len(), len);
        for row in 0..len {
            assert_eq!(result.value(row).unwrap(), Value::Null);
        }
        drop((result, charge, expected_field, reference_charge));
        assert_eq!(memory.used_bytes(), baseline);
    }
}

#[test]
fn aggregate_input_duration_sum_preserves_the_reached_null_index_and_wrapping() {
    use crate::columnar::SelectedColumn;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    {
        let input = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Second)).maybe();
        let output = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond)).maybe();
        let aggregate = AggregateExpr {
            field: field(1, output.clone()), function: AggregateFunction::Average,
            argument: Some(field(0, output.clone()).expression()), distinct: false, filter: None, order_by: vec![],
        };
        let types = AggregateTypes::derive(&aggregate, input.clone(), true, EmissionMode::Execution).unwrap();
        let arrow_field = Arc::new(rtdb_columnar::to_field("c0", &input).unwrap());
        let array = Arc::new(arrow_array::DurationSecondArray::from(vec![i64::MAX]));
        let millis_field = Arc::new(rtdb_columnar::to_field("c0", &output).unwrap());
        let millis = Arc::new(arrow_array::DurationMillisecondArray::from(vec![3000]));
        let selected = SelectedColumn::new(vec![Some((array, arrow_field, None, None)), None, Some((millis, millis_field, None, None))],
            Arc::new(vec![(0, 0), (1, 0), (2, 0)]));
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 3, memory.reserve(4096).unwrap()).unwrap();
        let page = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
        let scope = execution.scope().unwrap();
        for selected in [false, true] {
            let mut states = ScalarGroups::new(&execution).unwrap();
            states.ensure(1).unwrap();
            let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
            let result = states.aggregate(page, GroupMapping::Global(3), |row| row != 1 || selected, 1,
                Some(0), &aggregate, &types, &scope, &execution);
            drop(pressure);
            let state = &states.values[0];
            if selected {
                let error = result.unwrap_err();
                assert_eq!(error.code, "xtdb.error/unknown");
                assert_eq!(error.message.as_deref(), Some("null at index 1"));
                assert_eq!(state.count, 0);
                assert!(matches!(state.accumulator, ScalarState::Duration(-1000, TimeUnit::Millisecond)));
            } else {
                result.unwrap();
                assert_eq!(state.finish(&aggregate, &output, &scope).unwrap(),
                    Value::Duration { value: 1000, unit: TimeUnit::Millisecond });
            }
        }
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn decimal_average_reads_the_annotated_scale_before_rounding_to_double() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let decimal = decimal128_value(-123, 20, 400).unwrap();
    let expected = rtdb_expr::decimal_to_f64(&decimal);
    let values = [Value::Decimal(decimal), Value::Null];
    let input = values[0].logical_type().maybe();
    let arrow_field = Arc::new(rtdb_columnar::to_field("c0", &input).unwrap());
    let array = rtdb_columnar::copy_array(&input, &values).unwrap();
    let batch = ColumnBatch::settled(vec![Column::Arrow { array, field: arrow_field, logical: Some(input.clone()) }],
        2, memory.reserve(4096).unwrap()).unwrap();
    let rows = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    let output = LogicalType::Scalar(ScalarType::F64);
    let factory = SumFactory::new(&input, &output).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
    states.sum_page(rows, GroupMapping::Global(2), |_| true, Some(0), &factory, &execution).unwrap();
    drop(pressure);
    states.count_page(rows, GroupMapping::Global(2), |_| true, Some(0), &execution).unwrap();
    assert_eq!(states.values[0].average_double().unwrap().to_bits(), expected.to_bits());
}

#[test]
fn decimal_sum_to_f32_rounds_once_at_the_logical_scale() {
    use arrow_buffer::i256;
    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let midpoint = (1i128 << 54) + (1i128 << 30);
    let negative = i256::from_i128(-midpoint - 1).wrapping_mul(i256::from_i128(10).wrapping_pow(40));
    let upper = ((1u64 << 54) + (1u64 << 31)) as f32;
    for (decimal, expected) in [
        (decimal128_value(midpoint + 1, 20, 0).unwrap(), upper),
        (rtdb_types::DecimalValue::new(negative.to_be_bytes().to_vec(), 60, 40, 256).unwrap(), -upper),
        (decimal128_value(-123, 20, 400).unwrap(), -0.0),
    ] {
        let scalar = vector_float(&Value::Decimal(decimal.clone())).unwrap();
        assert_eq!(scalar.to_bits(), expected.to_bits());
        let values = [Value::Decimal(decimal), Value::Null, Value::Null];
        let input = values[0].logical_type().maybe();
        let field = Arc::new(rtdb_columnar::to_field("c0", &input).unwrap());
        let array = rtdb_columnar::copy_array(&input, &values).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(input.clone()) }],
            values.len(), memory.reserve(4096).unwrap()).unwrap();
        let rows = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
        let mut states = ScalarGroups::new(&execution).unwrap();
        states.ensure(2).unwrap();
        let output = LogicalType::Scalar(ScalarType::F32);
        let factory = SumFactory::new(&input, &output).unwrap();
        let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
        states.sum_page(rows, GroupMapping::Grouped(&[0, 0, 1]), |_| true, Some(0), &factory, &execution).unwrap();
        drop(pressure);
        assert!(matches!(states.values[0].accumulator, ScalarState::F32(value) if value.to_bits() == expected.to_bits()));
        assert!(matches!(states.values[1].accumulator, ScalarState::Empty));
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn mixed_decimal_float_sum_keeps_the_binary32_accumulation_order() {
    let execution = execution(8);
    let midpoint = (1i128 << 54) + (1i128 << 30);
    let positive = Value::Decimal(decimal128_value(midpoint + 1, 20, 0).unwrap());
    let negative = Value::F32(-(((1u64 << 54) + (1u64 << 31)) as f32));
    let input = positive.logical_type().join(&negative.logical_type()).maybe();
    let cursor = primitive_cursor(&execution, AggregateFunction::Sum, input.clone(), input,
        vec![vec![negative, Value::Null, positive, Value::F32(1.0)]], None);
    assert_eq!(primitive_result(cursor).await, Value::F32(1.0));
}

#[tokio::test]
async fn mixed_numeric_average_preserves_input_order_and_counts_nonnull_values() {
    let execution = execution(8);
    let decimal = Value::Decimal(decimal128_value(-100_000_000_000_000_000, 20, 1).unwrap());
    let integer = Value::I64(10_000_000_000_000_000);
    let ty = decimal.logical_type().join(&integer.logical_type()).maybe();
    let result = primitive_cursor(&execution, AggregateFunction::Average, ty.clone(), ty,
        vec![vec![integer, decimal, Value::I64(3), Value::Null]], None);
    assert_eq!(primitive_result(result).await, Value::F64(1.0));
}

#[test]
fn duration_average_skips_a_null_in_an_admitted_union() {
    use crate::columnar::SelectedColumn;
    let execution = execution(8);
    let memory = &execution.context.memory;
    let seconds = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Second));
    let millis = LogicalType::Scalar(ScalarType::Duration(TimeUnit::Millisecond));
    let input = seconds.join(&millis).maybe();
    let sources = vec![
        Some((
            Arc::new(arrow_array::DurationSecondArray::from(vec![2])) as arrow_array::ArrayRef,
            Arc::new(rtdb_columnar::to_field("c0", &seconds).unwrap()), None, None,
        )),
        Some((
            Arc::new(arrow_array::DurationMillisecondArray::from(vec![9])) as arrow_array::ArrayRef,
            Arc::new(rtdb_columnar::to_field("c0", &millis).unwrap()), None, None,
        )),
        None,
    ];
    let selected = SelectedColumn::new(sources, Arc::new(vec![(0, 0), (2, 0), (1, 0)]));
    let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 3, memory.reserve(4096).unwrap()).unwrap();
    let rows = Rows::columns(&batch, &[0], false, &execution.context).unwrap();
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    let factory = SumFactory::new(&input, &millis).unwrap();
    let pressure = memory.reserve(memory.available_bytes() - 4096).unwrap();
    states.sum_page(rows, GroupMapping::Global(3), |_| true, Some(0), &factory, &execution).unwrap();
    states.count_page(rows, GroupMapping::Global(3), |_| true, Some(0), &execution).unwrap();
    drop(pressure);
    assert!(matches!(states.values[0].accumulator, ScalarState::Duration(2009, TimeUnit::Millisecond)));
    assert_eq!(states.values[0].count, 2);
}

#[tokio::test]
async fn aggregate_input_timestamp_extremum_keeps_units_and_casts_each_reached_value() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let timestamp = |value, unit, zone: &str| Value::Timestamp { value, unit, timezone: Some(zone.to_owned()) };
    let initial = timestamp(10, TimeUnit::Microsecond, "Z");
    let coarse = timestamp(-2, TimeUnit::Second, "America/New_York");
    let overflow = timestamp(i64::MAX, TimeUnit::Second, "America/New_York");
    let declared = initial.logical_type().join(&coarse.logical_type()).maybe();
    for (later, arrow) in [(coarse.clone(), false), (overflow.clone(), false), (coarse, true), (overflow.clone(), true)] {
        let ty = initial.logical_type();
        let first = ColumnBatch::settled(vec![Column::Arrow {
            array: rtdb_columnar::copy_array(&ty, std::slice::from_ref(&initial)).unwrap(),
            field: Arc::new(rtdb_columnar::to_field("c0", &ty).unwrap()), logical: Some(ty),
        }], 1, memory.reserve(4096).unwrap()).unwrap();
        let is_overflow = later == overflow;
        let column = if arrow {
            let ty = later.logical_type();
            Column::Arrow {
                array: rtdb_columnar::copy_array(&ty, std::slice::from_ref(&later)).unwrap(),
                field: Arc::new(rtdb_columnar::to_field("c0", &ty).unwrap()), logical: Some(ty),
            }
        } else {
            Column::Values(vec![later])
        };
        let second = ColumnBatch::settled(vec![column], 1, memory.reserve(4096).unwrap()).unwrap();
        let mut cursor = primitive_cursor(&execution, AggregateFunction::Min, declared.clone(), declared.clone(), vec![], None);
        cursor.input = Some(AggregateInput { batches: Box::new(Pages([first, second].into())), projected: true });
        if is_overflow {
            assert_eq!(cursor.next_batch().await.err().unwrap().code, "xtdb.expression/overflow-error");
        } else {
            assert_eq!(primitive_result(cursor).await, timestamp(-2_000_000, TimeUnit::Microsecond, "Z"));
        }
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn aggregate_input_binary_extrema_compare_unsigned_bytes_across_pages() {
    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::Binary).maybe();
    for (function, expected) in [(AggregateFunction::Min, vec![0x7f]), (AggregateFunction::Max, vec![0xff, 0x01])] {
        let first = ColumnBatch::settled(vec![Column::Values(vec![Value::Binary(vec![0xff])])],
            1, memory.reserve(4096).unwrap()).unwrap();
        let values = [Value::Binary(vec![0x7f]), Value::Null, Value::Binary(vec![0xff, 0x01])];
        let second = ColumnBatch::settled(vec![Column::Arrow {
            array: rtdb_columnar::copy_array(&ty, &values).unwrap(),
            field: Arc::new(rtdb_columnar::to_field("c0", &ty).unwrap()), logical: Some(ty.clone()),
        }], values.len(), memory.reserve(4096).unwrap()).unwrap();
        let mut cursor = primitive_cursor(&execution, function, ty.clone(), ty.clone(), vec![], None);
        cursor.input = Some(AggregateInput { batches: Box::new(Pages([first, second].into())), projected: false });
        assert_eq!(primitive_result(cursor).await, Value::Binary(expected));
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[test]
fn keyword_extrema_fund_winners_and_borrow_losing_and_equal_names() {
    use crate::columnar::SelectedColumn;
    use arrow_array::StringArray;

    let execution = execution(8);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    let ty = LogicalType::Scalar(ScalarType::Keyword).maybe();
    let loser = "m".repeat(32 * 1024);
    let scope = execution.scope().unwrap();
    for (function, winner, replacement) in [
        (AggregateFunction::Min, ":ns/key", "!/ns"),
        (AggregateFunction::Max, "é/ns", "ω/ns"),
    ] {
        let source_field = Arc::new(rtdb_columnar::to_field("arg", &ty).unwrap());
        let array = Arc::new(StringArray::from(vec![Some(winner), Some(loser.as_str()), Some(winner), None, Some(replacement)]));
        let selected = SelectedColumn::new(vec![Some((array, source_field, Some(ty.clone()), None)), None],
            Arc::new(vec![(0, 0), (0, 1), (1, 0), (0, 2), (0, 3), (0, 4)]));
        let batch = ColumnBatch::settled(vec![Column::Selected(selected)], 6, memory.reserve(1 << 17).unwrap()).unwrap();
        let page = Rows::columns(&batch, &[0], false, context).unwrap();
        let aggregate = AggregateExpr {
            field: field(1, ty.clone()), function, argument: Some(field(0, ty.clone()).expression()),
            distinct: false, filter: None, order_by: vec![],
        };
        let types = AggregateTypes::derive(&aggregate, ty.clone(), false, EmissionMode::Execution).unwrap();
        let mut states = ScalarGroups::new(&execution).unwrap();
        states.ensure(2).unwrap();
        let pressure = memory.reserve(memory.available_bytes()).unwrap();
        let refused = memory.reserve(1).err().unwrap().code;
        let groups = GroupMapping::Grouped(&[0, 0, 1, 0, 1, 0]);
        assert_eq!(states.aggregate(page, groups, |_| true, 2, Some(0), &aggregate, &types, &scope, &execution)
            .unwrap_err().code, refused);
        assert!(matches!(states.values[0].accumulator, ScalarState::Empty));
        drop(pressure);

        let winner = Value::Keyword(rtdb_types::Keyword::from_name(winner));
        let winner_charge = winner.retained_bytes().unwrap() + 256;
        let pressure = memory.reserve(memory.available_bytes() - winner_charge).unwrap();
        states.aggregate(page, groups, |row| row != 5, 2, Some(0), &aggregate, &types, &scope, &execution).unwrap();
        assert_eq!(memory.available_bytes(), 0);
        assert_eq!(states.aggregate(page, groups, |row| row == 5, 2, Some(0), &aggregate, &types, &scope, &execution)
            .unwrap_err().code, refused);
        drop((pressure, batch));

        let (output, charge) = byte_scalar_results(&states, &aggregate.function, &aggregate.field, context).unwrap().unwrap();
        drop(states);
        let (reference, reference_charge) = crate::columnar::pack_evaluated_column(&[winner.clone(), Value::Null], memory).unwrap();
        let (Column::Arrow { array, field, logical }, Column::Arrow { field: expected, logical: expected_type, .. }) =
            (&output, &reference) else { panic!("Expected Arrow keyword results"); };
        assert_eq!(field, expected);
        assert_eq!(logical, expected_type);
        assert_eq!(array.as_string::<i32>().value(0).as_bytes(), match &winner {
            Value::Keyword(winner) => winner.as_str().as_bytes(), _ => unreachable!(),
        });
        assert_eq!(output.value(0).unwrap(), winner);
        assert_eq!(output.value(1).unwrap(), Value::Null);
        drop((output, charge, reference, reference_charge));
    }
    drop(scope);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn keyword_extrema_keep_casted_and_tagged_spellings_across_pages() {
    use arrow_array::StringArray;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let baseline = memory.used_bytes();
    let keyword = LogicalType::Scalar(ScalarType::Keyword);
    let page = |ty: &LogicalType, text| {
        let array = Arc::new(StringArray::from(vec![text]));
        let field = Arc::new(rtdb_columnar::to_field("arg", ty).unwrap());
        ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }], 1, memory.reserve(4096).unwrap()).unwrap()
    };
    for (function, expected) in [(AggregateFunction::Min, ":ns/z"), (AggregateFunction::Max, "ns/a")] {
        let mut cursor = primitive_cursor(&execution, function, keyword.clone(), keyword.clone(), vec![], None);
        let untagged = page(&LogicalType::Scalar(ScalarType::Utf8), ":ns/a");
        let tagged = page(&keyword, ":ns/z");
        cursor.input = Some(AggregateInput { batches: Box::new(Pages([untagged, tagged].into())), projected: false });

        let result = cursor.next_batch().await.unwrap().unwrap();
        assert!(cursor.next_batch().await.unwrap().is_none());
        let Column::Arrow { array, field, logical } = &result.columns[0] else { panic!("Expected an Arrow keyword"); };
        assert_eq!(field.as_ref(), &rtdb_columnar::to_field("value", &keyword).unwrap());
        assert_eq!(logical.as_ref(), Some(&keyword));
        assert_eq!(array.as_string::<i32>().value(0), expected);
        assert_eq!(result.columns[0].value(0).unwrap(), Value::Keyword(rtdb_types::Keyword::from_name(expected)));
    }
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn uri_extrema_compare_raw_spellings_and_validate_losing_inputs() {
    use arrow_array::StringArray;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let ty = LogicalType::Scalar(ScalarType::Uri);
    let valid = Value::Uri("https://example.test/./z".into());
    let other = Value::Uri("https://example.test/a".into());
    assert_eq!(compare_in_timezone(&valid, &other, false, "UTC").unwrap_err().code, "xtdb.expression/uri-comparator");
    assert_eq!(primitive_result(primitive_cursor(&execution, AggregateFunction::Min, ty.clone(), ty.clone(),
        vec![vec![other, valid.clone()]], None)).await, valid);

    for (function, invalid) in [(AggregateFunction::Min, "z invalid"), (AggregateFunction::Max, " invalid")] {
        for filter in [None, Some(Expr::Literal(Value::Bool(false)))] {
            let field = Arc::new(rtdb_columnar::to_field("arg", &ty).unwrap());
            let array = Arc::new(StringArray::from(vec!["https://a/", invalid]));
            let expected = rtdb_columnar::read_value(&field, array.as_ref(), 1).unwrap_err();
            let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: Some(ty.clone()) }],
                2, memory.reserve(4096).unwrap()).unwrap();
            let mut cursor = primitive_cursor(&execution, function.clone(), ty.clone(), ty.clone(), vec![], filter);
            cursor.input = Some(AggregateInput { batches: Box::new(Batches(Some(batch))), projected: false });
            let error = cursor.next_batch().await.err().unwrap();
            assert_eq!(error.code, expected.code);
            assert_eq!(error.message, expected.message);
        }
    }
}

#[test]
fn uri_extrema_fund_replacements_and_borrow_losing_and_equal_spellings() {
    use arrow_array::StringArray;

    let execution = execution(8);
    let context = &execution.context;
    let memory = &context.memory;
    let baseline = memory.used_bytes();
    let scope = execution.scope().unwrap();
    let ty = LogicalType::Scalar(ScalarType::Uri).maybe();
    let loser = format!("https://example.test/{}", "m".repeat(32 * 1024));
    for (function, winner, replacement) in [
        (AggregateFunction::Min, "HTTP://EXAMPLE.test/%ab", "A:late"),
        (AggregateFunction::Max, "https://example.test/ω", "z:late"),
    ] {
        let array = Arc::new(StringArray::from(vec![Some(winner), Some(loser.as_str()), None, Some(winner), Some(replacement)]));
        let field_type = Arc::new(rtdb_columnar::to_field("arg", &ty).unwrap());
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field: field_type, logical: Some(ty.clone()) }],
            5, memory.reserve(1 << 17).unwrap()).unwrap();
        let page = Rows::columns(&batch, &[0], false, context).unwrap();
        let aggregate = AggregateExpr {
            field: field(1, ty.clone()), function, argument: Some(field(0, ty.clone()).expression()),
            distinct: false, filter: None, order_by: vec![],
        };
        let types = AggregateTypes::derive(&aggregate, ty.clone(), false, EmissionMode::Execution).unwrap();
        let mut states = ScalarGroups::new(&execution).unwrap();
        states.ensure(1).unwrap();
        let winner = Value::Uri(winner.to_owned());
        let winner_charge = winner.retained_bytes().unwrap() + 256;
        let pressure = memory.reserve(memory.available_bytes() - winner_charge).unwrap();
        let groups = GroupMapping::Global(batch.len);
        states.aggregate(page, groups, |row| row < 4, 1, Some(0), &aggregate, &types, &scope, &execution).unwrap();
        assert_eq!(memory.available_bytes(), 0);
        let refused = memory.reserve(1).err().unwrap().code;
        assert_eq!(states.aggregate(page, groups, |row| row == 4, 1, Some(0), &aggregate, &types, &scope, &execution)
            .unwrap_err().code, refused);
        drop((pressure, batch));
        assert_eq!(states.values[0].accumulator.retained(), Some(&winner));
    }
    drop(scope);
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn uri_extrema_keep_utf8_casts_and_all_null_result_layout() {
    use arrow_array::StringArray;

    let execution = execution(8);
    let memory = &execution.context.memory;
    let ty = LogicalType::Scalar(ScalarType::Uri).maybe();
    let page = |ty: &LogicalType, text| {
        let array = Arc::new(StringArray::from(vec![text]));
        let field = Arc::new(rtdb_columnar::to_field("arg", ty).unwrap());
        ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }], 1, memory.reserve(4096).unwrap()).unwrap()
    };
    let plain = LogicalType::Scalar(ScalarType::Utf8);
    let mut mixed = primitive_cursor(&execution, AggregateFunction::Min, ty.clone(), ty.clone(), vec![], None);
    mixed.input = Some(AggregateInput { batches: Box::new(Pages([
        page(&ty, Some("https://example.test/z")),
        page(&plain, Some("HTTP://EXAMPLE.test/%ab")),
    ].into())), projected: false });
    let result = mixed.next_batch().await.unwrap().unwrap();
    let Column::Arrow { field, logical, .. } = &result.columns[0] else { panic!("Expected tagged Arrow URI"); };
    assert_eq!(logical.as_ref(), Some(&LogicalType::Scalar(ScalarType::Uri)));
    assert!(!field.is_nullable());
    assert_eq!(result.columns[0].value(0).unwrap(), Value::Uri("HTTP://EXAMPLE.test/%ab".into()));

    let mut nulls = primitive_cursor(&execution, AggregateFunction::Max, ty.clone(), ty.clone(), vec![], None);
    nulls.input = Some(AggregateInput { batches: Box::new(Batches(Some(page(&ty, None)))), projected: false });
    let result = nulls.next_batch().await.unwrap().unwrap();
    let Column::Arrow { field, logical, .. } = &result.columns[0] else { panic!("Expected Arrow Null"); };
    assert_eq!(field.as_ref(), &rtdb_columnar::to_field("value", &LogicalType::Null).unwrap());
    assert_eq!(logical.as_ref(), Some(&LogicalType::Null));
    assert_eq!(result.columns[0].value(0).unwrap(), Value::Null);
}

#[tokio::test]
async fn grouped_uri_extrema_publish_tagged_winners_and_nulls_together() {
    let execution = execution(8);
    let ty = LogicalType::Scalar(ScalarType::Uri).maybe();
    let int = LogicalType::Scalar(ScalarType::I64);
    let schema = Schema::new(vec![field(0, int.clone()), field(1, ty.clone())]).unwrap();
    let groups = vec![NamedExpr { field: field(2, int), expr: schema.fields()[0].expression() }];
    let aggregates = [AggregateFunction::Min, AggregateFunction::Max].into_iter().enumerate()
        .map(|(index, function)| AggregateExpr {
            field: field(3 + index as u32, ty.clone()), function,
            argument: Some(schema.fields()[1].expression()), distinct: false, filter: None, order_by: vec![],
        }).collect();
    let min = "HTTP://EXAMPLE.test/%ab";
    let max = "https://example.test/ω";
    let pages = vec![
        vec![vec![Value::I64(1), Value::Uri(max.into())], vec![Value::I64(2), Value::Null]],
        vec![vec![Value::I64(1), Value::Uri(min.into())], vec![Value::I64(3), Value::Uri(max.into())]],
    ];
    let (mut cursor, _) = cursor(&execution, schema, pages, groups, aggregates, None);
    let result = cursor.next_batch().await.unwrap().unwrap();
    let expected_field = rtdb_columnar::to_field("value", &ty).unwrap();
    for (column, first) in result.columns[1..].iter().zip([min, max]) {
        let Column::Arrow { array, field, logical } = column else { panic!("Expected a tagged Arrow URI column"); };
        assert_eq!(field.as_ref(), &expected_field);
        assert_eq!(logical.as_ref(), Some(&ty));
        assert_eq!(array.as_string::<i32>().iter().collect::<Vec<_>>(), [Some(first), None, Some(max)]);
    }
}

#[test]
fn uri_extrema_validate_unchecked_winners_at_publication() {
    let execution = execution(8);
    let mut states = ScalarGroups::new(&execution).unwrap();
    states.ensure(1).unwrap();
    let text = "bad uri";
    states.values[0].byte_extremum(text.as_bytes(), true, &states.state, || Value::Uri(text.into())).unwrap();
    let expected = rtdb_types::UriParts::parse(text).unwrap_err();
    let field = field(0, LogicalType::Scalar(ScalarType::Uri));
    let error = byte_scalar_results(&states, &AggregateFunction::Min, &field, &execution.context).err().unwrap();
    assert_eq!(error.code, expected.code);
    assert_eq!(error.message, expected.message);
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
    assert_eq!(states.aggregate(Rows { batch: &page, projected: false, memory: &execution.context.memory },GroupMapping::Global(2),|_| true,1,Some(0),&aggregate,&types,&scope,&execution).unwrap_err().code,"xtdb.error/unknown");
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
