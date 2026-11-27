use crate::{
    CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions,
    context::{Execution, OuterRows},
    operators::build_column_pages,
    columnar::BoxBatchCursor,
    parameters::BoundParameters,
    runtime_schema::{EmittedRelation, RuntimeSchema},
};
use rtdb_columnar::MemoryBudget;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{LogicalType, Result, ScalarType, Timestamp, Value};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

fn execution(batch_rows: usize, arguments: Vec<Value>) -> Execution {
    execution_with_budget(batch_rows, arguments, MemoryBudget::new(1 << 27))
}

fn execution_with_budget(batch_rows: usize, arguments: Vec<Value>, memory: Arc<MemoryBudget>) -> Execution {
    let database = DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    };
    let catalog =
        CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(
        catalog,
        memory,
        Timestamp(0),
        "UTC",
        QueryOptions {
            batch_rows,
            list_batch_rows: 3,
            ..Default::default()
        },
        &CancellationToken::new(),
    )
    .unwrap();
    Execution {
        parameters: Arc::new(BoundParameters::infer(arguments, &context.memory).unwrap()),
        outer: OuterRows::empty(&context.memory).unwrap(),
        context,
        bindings: None,
        analysis: None,
    }
}

fn column_table(name: &str, expr: Expr) -> Arc<Relation> {
    let types = expr
        .data_type()
        .legs()
        .into_iter()
        .map(|ty| match ty {
            LogicalType::List { element, .. } => *element,
            _ => LogicalType::Null,
        })
        .collect::<Vec<_>>();
    let element = Field {
        id: ColumnId(0),
        name: name.into(),
        qualifier: None,
        qualified_only: false,
        unnamed: false,
        data_type: LogicalType::join_all(&types),
    };
    Arc::new(
        Relation::new(
            Schema::new(vec![element.clone()]).unwrap(),
            RelationNode::ColumnTable { expr, element },
        )
        .unwrap(),
    )
}

async fn open(
    relation: Arc<Relation>,
    execution: &Execution,
) -> Result<(BoxBatchCursor, Arc<RuntimeSchema>)> {
    let emitted = EmittedRelation::derive(
        relation,
        &execution.context.catalog,
        execution.parameters.types(),
        &execution.context.memory,
    )?;
    let output = emitted.output();
    Ok((build_column_pages(emitted, execution.clone()).await?.unwrap(), output))
}

#[tokio::test]
async fn captured_arrow_list_lends_one_semantic_page_and_retains_the_source_charge() {
    use arrow_array::{Array, RecordBatch, cast::AsArray};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};

    use crate::{columnar::{Column, ColumnBatch}, context::ApplyArguments};

    let execution = execution(1, vec![]);
    let source_memory = MemoryBudget::new(1 << 20);
    let ty = LogicalType::list(LogicalType::Scalar(ScalarType::I64).maybe());
    let outer = Field {
        id: ColumnId(1), name: "items".into(), data_type: ty.clone(),
        qualifier: None, qualified_only: false, unnamed: false,
    };
    let items = [Value::I64(1), Value::Null, Value::I64(3), Value::I64(4), Value::I64(5)];
    let array = build_array(&ty, &[Value::list(vec![Value::I64(20)]), Value::list(items.to_vec())]).unwrap();
    let list = array.as_list::<i32>();
    let child = list.values().slice(1, items.len());
    let child_ptr = child.as_primitive::<arrow_array::types::Int64Type>().values().as_ptr();
    drop(child);
    let schema = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(schema, vec![array]).unwrap();
    let source = Arc::new(ColumnBatch::from_owned(OwnedBatch::new(record, &source_memory).unwrap()).unwrap());
    let schema = Schema::new(vec![outer.clone()]).unwrap();
    let schema = RuntimeSchema::fixed(&schema, &execution.context.memory).unwrap();
    let arguments = ApplyArguments::new(schema, vec![(0, outer.expression())]);
    let bound = arguments.bind(&execution, source, 1).await.unwrap();
    let mut cursor = crate::column_table::open_arrow(&bound, 0).unwrap().unwrap();
    drop(bound);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let [Column::Arrow { array, .. }] = page.columns.as_slice() else { panic!("Expected an Arrow slice"); };
    assert_eq!(array.len(), items.len());
    assert_eq!(array.as_primitive::<arrow_array::types::Int64Type>().values().as_ptr(), child_ptr);
    assert_eq!((0..page.len).map(|row| page.columns[0].value(row).unwrap()).collect::<Vec<_>>(), items);
    assert!(source_memory.used_bytes() > 0);
    drop(page);
    assert_eq!(source_memory.used_bytes(), 0);
}

#[tokio::test]
async fn computed_capture_lends_the_reached_float_list_with_its_source_charge() {
    use arrow_array::{Float32Array, RecordBatch, cast::AsArray};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};
    use crate::{columnar::{Column, ColumnBatch}, context::ApplyArguments};

    let execution = execution(1, vec![]);
    let source_memory = MemoryBudget::new(1 << 20);
    let element = LogicalType::Scalar(ScalarType::F32).maybe();
    let ty = LogicalType::list(element.clone());
    let field = Field {
        id: ColumnId(1), name: "items".into(), data_type: ty.clone(),
        qualifier: None, qualified_only: false, unnamed: false,
    };
    let items = vec![Value::F32(-0.0), Value::Null, Value::F32(f32::from_bits(0x7fc0_1234))];
    let array = build_array(&ty, &[Value::list(items)]).unwrap();
    let child = array.as_list::<i32>().values().as_any().downcast_ref::<Float32Array>().unwrap();
    let address = child.values().as_ptr();
    let schema = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(schema, vec![array]).unwrap();
    let source = Arc::new(ColumnBatch::from_owned(OwnedBatch::new(record, &source_memory).unwrap()).unwrap());
    let expression = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), field.expression())],
        otherwise: Box::new(Expr::Cast {
            expr: Box::new(Expr::Literal(Value::Utf8("invalid array".into()))),
            target: rtdb_plan::CastTarget::fixed(ty),
        }),
    };
    let schema = Schema::new(vec![field]).unwrap();
    let schema = RuntimeSchema::fixed(&schema, &execution.context.memory).unwrap();
    let arguments = ApplyArguments::new(schema, vec![(0, expression)]);
    let bound = arguments.bind(&execution, source, 0).await.unwrap();
    let mut cursor = crate::column_table::open_arrow(&bound, 0).unwrap().unwrap();
    drop(bound);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let [Column::Arrow { array, logical: Some(logical), .. }] = page.columns.as_slice() else {
        panic!("Expected the reached Arrow child and its logical type");
    };
    assert_eq!(logical, &element);
    let child = array.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(child.values().as_ptr(), address);
    assert_eq!(child.iter().map(|value| value.map(f32::to_bits)).collect::<Vec<_>>(), [
        Some((-0.0_f32).to_bits()), None, Some(0x7fc0_1234),
    ]);
    assert!(source_memory.used_bytes() > 0);
    drop(page);
    assert_eq!(source_memory.used_bytes(), 0);
}

#[tokio::test]
async fn computed_capture_lends_nested_list_children_with_their_parent_validity() {
    use arrow_array::{Array, RecordBatch, cast::AsArray};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};
    use crate::{columnar::{Column, ColumnBatch}, context::ApplyArguments};

    let execution = execution(1, vec![]);
    let source_memory = MemoryBudget::new(1 << 20);
    let element = LogicalType::list(LogicalType::Scalar(ScalarType::Utf8).maybe()).maybe();
    let ty = LogicalType::list(element.clone());
    let values = vec![
        Value::Null,
        Value::list(vec![]),
        Value::list(vec![Value::Utf8("nested".into()), Value::Null]),
    ];
    let array = build_array(&ty, &[
        Value::list(vec![Value::list(vec![Value::Utf8("unreached".into())])]),
        Value::list(values.clone()),
    ]).unwrap();
    let leaf = array.as_list::<i32>().values().as_list::<i32>().values().as_string::<i32>();
    let address = leaf.value(1).as_ptr();
    let physical = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(physical, vec![array]).unwrap();
    let source = Arc::new(ColumnBatch::from_owned(OwnedBatch::new(record, &source_memory).unwrap()).unwrap());
    let field = Field {
        id: ColumnId(0), name: "items".into(), data_type: ty.clone(),
        qualifier: None, qualified_only: false, unnamed: false,
    };
    let expression = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), field.expression())],
        otherwise: Box::new(Expr::Cast {
            expr: Box::new(Expr::Literal(Value::Utf8("invalid array".into()))),
            target: rtdb_plan::CastTarget::fixed(ty),
        }),
    };
    let schema = RuntimeSchema::fixed(&Schema::new(vec![field]).unwrap(), &execution.context.memory).unwrap();
    let arguments = ApplyArguments::new(schema, vec![(0, expression)]);
    let bound = arguments.bind(&execution, source, 1).await.unwrap();
    assert!(bound.parameters.captured_arrow_cell(0).is_some());
    let mut cursor = crate::column_table::open_arrow(&bound, 0).unwrap().unwrap();
    drop(bound);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let [Column::Arrow { array, logical: Some(logical), .. }] = page.columns.as_slice() else {
        panic!("Expected the nested child slice");
    };
    assert_eq!(logical, &element);
    let lists = array.as_list::<i32>();
    assert!(lists.is_null(0));
    assert!(lists.value(1).is_empty());
    let child = lists.value(2);
    assert_eq!(child.as_string::<i32>().value(0).as_ptr(), address);
    assert_eq!((0..page.len).map(|row| page.columns[0].value(row).unwrap()).collect::<Vec<_>>(), values);
    assert!(source_memory.used_bytes() > 0);
    drop((child, page));
    assert_eq!(source_memory.used_bytes(), 0);
}

#[tokio::test]
async fn computed_column_table_lends_an_arrow_parameter_through_the_list_reader() {
    use arrow_array::{RecordBatch, cast::AsArray};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};
    use crate::columnar::Column;

    let mut execution = execution(1, vec![]);
    let source_memory = MemoryBudget::new(1 << 20);
    let element = LogicalType::Scalar(ScalarType::Binary).maybe();
    let ty = LogicalType::list(element.clone());
    let items = vec![Value::Binary(vec![1, 2]), Value::Null, Value::Binary(vec![3])];
    let array = build_array(&ty, &[Value::list(items.clone())]).unwrap();
    let address = array.as_list::<i32>().values().as_binary::<i32>().value(0).as_ptr();
    let schema = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(schema, vec![array]).unwrap();
    let reservation = source_memory.reserve(record.get_array_memory_size()).unwrap();
    let arguments = OwnedBatch::with_reservation(record, reservation).unwrap();
    execution.parameters = Arc::new(BoundParameters::from_arrow(arguments, 0, vec![ty.clone()], &execution.context.memory).unwrap());
    let expr = Expr::Case {
        branches: vec![(Expr::Literal(Value::Bool(true)), Expr::Parameter { index: 0, data_type: ty })],
        otherwise: Box::new(Expr::Literal(Value::Null)),
    };
    let mut cursor = crate::column_table::open_columns(&execution, expr).await.unwrap();
    drop(execution);
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    drop(cursor);

    let [Column::Arrow { array, logical: Some(logical), .. }] = page.columns.as_slice() else {
        panic!("Expected a typed binary child");
    };
    assert_eq!(page.len, items.len());
    assert_eq!(logical, &element);
    assert_eq!(array.as_binary::<i32>().value(0).as_ptr(), address);
    assert_eq!((0..page.len).map(|row| page.columns[0].value(row).unwrap()).collect::<Vec<_>>(), items);
    assert!(source_memory.used_bytes() > 0);
    drop(page);
    assert_eq!(source_memory.used_bytes(), 0);
}

#[tokio::test]
async fn binary_list_cast_keeps_null_elements_and_one_column_table_page() {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};

    let mut execution = execution(2, vec![]);
    let memory = Arc::clone(&execution.context.memory);
    let baseline = memory.used_bytes();
    let ty = LogicalType::list(LogicalType::Scalar(ScalarType::Binary).maybe());
    let items = vec![Value::Binary(vec![1, 255]), Value::Null, Value::Binary(Vec::new())];
    let array = build_array(&ty, &[Value::list(items.clone())]).unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(schema, vec![array]).unwrap();
    let input = OwnedBatch::new(record, &memory).unwrap();
    let parameters = BoundParameters::from_arrow(input, 0, vec![ty.clone()], &memory).unwrap();
    let original = std::mem::replace(&mut execution.parameters, Arc::new(parameters));
    let expr = Expr::Cast {
        expr: Box::new(Expr::Parameter { index: 0, data_type: ty }),
        target: rtdb_plan::CastTarget::fixed(LogicalType::list(LogicalType::Scalar(ScalarType::Binary))),
    };
    let mut cursor = crate::column_table::open_columns(&execution, expr).await.unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert!(cursor.next_batch().await.unwrap().is_none());
    assert_eq!(page.len, items.len());
    assert_eq!((0..page.len).map(|row| page.columns[0].value(row).unwrap()).collect::<Vec<_>>(), items);
    drop((page, cursor));
    execution.parameters = original;
    assert_eq!(memory.used_bytes(), baseline);
}

#[tokio::test]
async fn captured_fixed_list_keeps_its_numeric_union_tags_and_real_null() {
    use arrow_array::{RecordBatch, cast::AsArray};
    use arrow_schema::Schema as ArrowSchema;
    use rtdb_columnar::{OwnedBatch, build_array, to_field};
    use rtdb_types::ListKind;
    use crate::{columnar::{Column, ColumnBatch}, context::ApplyArguments};

    let execution = execution(1, vec![]);
    let memory = &execution.context.memory;
    let element = LogicalType::from_legs([
        LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::F32), LogicalType::Null,
    ]);
    let ty = LogicalType::List { kind: ListKind::FixedSizeList(3), element: Box::new(element.clone()) }.maybe();
    let field = Field {
        id: ColumnId(1), name: "items".into(), data_type: ty.clone(),
        qualifier: None, qualified_only: false, unnamed: false,
    };
    let items = vec![Value::F32(-0.0), Value::Null, Value::I64(19)];
    let array = build_array(&ty, &[Value::Null, Value::FixedSizeList(items.clone().into())]).unwrap();
    let list = array.as_fixed_size_list();
    let tags = list.values().as_union().type_ids()[3..6].to_vec();
    let schema = Arc::new(ArrowSchema::new(vec![to_field("items", &ty).unwrap()]));
    let record = RecordBatch::try_new(schema, vec![array]).unwrap();
    let source = Arc::new(ColumnBatch::from_owned(OwnedBatch::new(record, memory).unwrap()).unwrap());
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let schema = RuntimeSchema::fixed(&schema, memory).unwrap();
    let arguments = ApplyArguments::new(schema, vec![(0, field.expression())]);
    let bound = arguments.bind(&execution, source, 1).await.unwrap();

    let mut cursor = crate::column_table::open_arrow(&bound, 0).unwrap().unwrap();
    let page = cursor.next_batch().await.unwrap().unwrap();
    let [Column::Arrow { array, logical: Some(logical), .. }] = page.columns.as_slice() else {
        panic!("Expected a typed union child");
    };
    assert_eq!(logical, &element);
    assert_eq!(array.as_union().type_ids().as_ref(), tags.as_slice());
    assert_eq!((0..page.len).map(|row| page.columns[0].value(row).unwrap()).collect::<Vec<_>>(), items);
    assert!(matches!(page.columns[0].value(0).unwrap(), Value::F32(value) if value.to_bits() == (-0.0_f32).to_bits()));
}

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Assertion>,
}
#[derive(Deserialize)]
struct Assertion {
    id: String,
    column: String,
    parameter: bool,
    input: Value,
    expected: Expected,
}
#[derive(Deserialize)]
struct Expected {
    data_type: LogicalType,
    rows: Vec<Vec<Value>>,
}

#[tokio::test]
async fn column_tables_preserve_the_four_original_single_column_assertions() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../../../conformance/fixtures/table-column.json"
    ))
    .unwrap();
    assert_eq!(fixture.cases.len(), 4);
    for case in fixture.cases {
        let arguments = if case.parameter {
            vec![case.input.clone()]
        } else {
            vec![]
        };
        let expression = if case.parameter {
            Expr::Parameter {
                index: 0,
                data_type: case.input.logical_type(),
            }
        } else {
            Expr::Literal(case.input)
        };
        let execution = execution(1, arguments);
        let baseline = execution.context.memory.used_bytes();
        let (mut cursor, types) = open(column_table(&case.column, expression), &execution)
            .await
            .unwrap();
        assert_eq!(types.schema().fields()[0].name, case.column, "{}", case.id);
        assert_eq!(
            types.schema().fields()[0].data_type,
            case.expected.data_type,
            "{}",
            case.id
        );
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_batch().await.unwrap() {
            rows.extend((0..page.len).map(|row| vec![page.columns[0].value(row).unwrap()]));
        }
        assert_eq!(rows, case.expected.rows, "{}", case.id);
        drop(cursor);
        drop(types);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn arrow_column_table_slices_only_the_bound_list_and_adds_page_ordinals() {
    use arrow_array::{Array, Int64Array, ListArray, RecordBatch, types::Int64Type};
    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};

    let execution = execution(1, vec![]);
    let context = &execution.context;
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(100)]),
        Some(vec![Some(3), None, Some(5), Some(8)]),
        None,
        Some(vec![]),
    ]);
    let parameter_field = ArrowField::new("args", list.data_type().clone(), true);
    let parameter_type = rtdb_columnar::from_field(&parameter_field).unwrap();
    let record = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![parameter_field])), vec![Arc::new(list)]).unwrap();
    let batch = rtdb_columnar::OwnedBatch::new(record, &context.memory).unwrap();
    let base = column_table("v", Expr::Parameter { index: 0, data_type: parameter_type.clone() });
    let n = Field {
        id: ColumnId(1), name: "n".into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64),
    };
    let relation = base.with_ordinality(n, rtdb_plan::OrdinalityScope::Page).unwrap();
    let prepared = crate::PreparedQuery::new(Arc::new(relation)).unwrap();
    let mut result = prepared.execute_arrow_columns(
        Arc::clone(context), batch.clone(), 1, std::slice::from_ref(&parameter_type), None,
    ).await.unwrap();
    for (position, value) in [Some(3), None, Some(5), Some(8)].into_iter().enumerate() {
        let page = result.next_batch().await.unwrap().unwrap();
        let output = page.batch().column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(output.iter().collect::<Vec<_>>(), [value]);
        let ordinals = page.batch().column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&ordinals.values()[..], [position as i64 + 1]);
    }
    assert!(result.next_batch().await.unwrap().is_none());
    let one_batch = execution_with_budget(1024, vec![], Arc::clone(&context.memory));
    let mut one_result = prepared.execute_arrow_columns(
        one_batch.context, batch.clone(), 1, std::slice::from_ref(&parameter_type), None,
    ).await.unwrap();
    let page = one_result.next_batch().await.unwrap().unwrap();
    let output = page.batch().column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let list = batch.batch().column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let input = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(output.values().as_ptr(), input.values()[1..].as_ptr());
    for row in [2, 3] {
        let mut result = prepared.execute_arrow_columns(
            Arc::clone(context), batch.clone(), row, std::slice::from_ref(&parameter_type), None,
        ).await.unwrap();
        assert!(result.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn arrow_column_table_leaves_room_for_a_tiny_bound_slice_beside_a_large_unused_row() {
    use arrow_array::{Array, ListArray, RecordBatch, StringArray, builder::{ListBuilder, StringBuilder}};
    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};

    let execution = execution_with_budget(1, vec![], MemoryBudget::new(48 << 20));
    let context = &execution.context;
    let mut lists = ListBuilder::new(StringBuilder::with_capacity(2, (20 << 20) + 1));
    lists.values().append_value("x");
    lists.append(true);
    lists.values().append_value("z".repeat(20 << 20));
    lists.append(true);
    let list: ListArray = lists.finish();
    let field = ArrowField::new("args", list.data_type().clone(), true);
    let parameter_type = rtdb_columnar::from_field(&field).unwrap();
    let record = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![Arc::new(list)]).unwrap();
    let bound = rtdb_columnar::OwnedBatch::new(record, &context.memory).unwrap();
    let base = column_table("v", Expr::Parameter { index: 0, data_type: parameter_type.clone() });
    let projections = ["a", "b"].into_iter().enumerate().map(|(index, name)| {
        let input = &base.schema().fields()[0];
        rtdb_plan::NamedExpr {
            field: Field { id: ColumnId(index as u32 + 1), name: name.into(), ..input.clone() },
            expr: input.expression(),
        }
    }).collect();
    let prepared = crate::PreparedQuery::new(Arc::new(base.project(projections).unwrap())).unwrap();
    let mut result = prepared.execute_arrow_columns(
        Arc::clone(context), bound.clone(), 0, std::slice::from_ref(&parameter_type), None,
    ).await.unwrap();
    let page = result.next_batch().await.unwrap().unwrap();
    assert_eq!(page.batch().num_columns(), 2);
    for array in page.batch().columns() {
        let values = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "x");
        assert!(values.get_buffer_memory_size() < 4096);
    }
    assert!(result.next_batch().await.unwrap().is_none());
}

fn series(end: i64) -> Expr {
    let args = [1, end, 1]
        .into_iter()
        .map(|value| Expr::Literal(Value::I64(value)))
        .chain([Expr::Literal(Value::Bool(false))])
        .collect::<Vec<_>>();
    Expr::Call {
        name: "generate_series".into(),
        return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(),
        args,
    }
}

fn division_by_zero() -> Expr {
    Expr::binary(
        BinaryOp::Divide,
        Expr::Literal(Value::I64(1)),
        Expr::Literal(Value::I64(0)),
    )
}

#[tokio::test]
async fn column_tables_publish_one_complete_page_including_empty_and_undemanded_scalar_inputs() {
    for batch_rows in [1, 2048] {
        for (expr, expected_len) in [
            (series(8), 7),
            (Expr::List(vec![]), 0),
            (Expr::Literal(Value::Null), 0),
            (division_by_zero(), 0),
        ] {
            let execution = execution(batch_rows, vec![]);
            let baseline = execution.context.memory.used_bytes();
            let (mut cursor, types) = open(column_table("v", expr), &execution).await.unwrap();
            let page = cursor.next_batch().await.unwrap().unwrap();
            assert_eq!(page.len, expected_len);
            if expected_len != 0 {
                assert!(matches!(&page.columns[0], crate::columnar::Column::Arrow { logical: Some(ty), .. }
                    if *ty == LogicalType::Scalar(ScalarType::I64)));
            }
            assert!(cursor.next_batch().await.unwrap().is_none());
            drop(cursor);
            drop(types);
            drop(page);
            assert_eq!(execution.context.memory.used_bytes(), baseline);
        }
    }
}

#[tokio::test]
async fn column_table_ordinality_is_one_i64_sequence_across_internal_reader_slices() {
    let execution = execution(1, vec![]);
    let input = column_table("v", series(8));
    let field = Field {
        id: ColumnId(1),
        name: "n".into(),
        qualifier: None,
        qualified_only: false,
        unnamed: false,
        data_type: LogicalType::Scalar(ScalarType::I64),
    };
    let relation = Arc::new(
        Relation::new(
            input
                .schema()
                .concat(&Schema::new(vec![field.clone()]).unwrap())
                .unwrap(),
            RelationNode::Ordinality { input, field, scope: rtdb_plan::OrdinalityScope::Page },
        )
        .unwrap(),
    );
    let (mut cursor, types) = open(relation, &execution).await.unwrap();
    assert_eq!(
        types.schema().fields()[1].data_type,
        LogicalType::Scalar(ScalarType::I64)
    );
    let page = cursor.next_batch().await.unwrap().unwrap();
    assert_eq!(
        (0..page.len)
            .map(|row| page.columns.iter().map(|column| column.value(row).unwrap()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        (1..8)
            .map(|value| vec![Value::I64(value), Value::I64(value)])
            .collect::<Vec<_>>()
    );
    assert!(cursor.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn column_table_element_failure_prevents_opening_and_releases_completed_prefixes() {
    let execution = execution(1, vec![]);
    let baseline = execution.context.memory.used_bytes();
    let expressions = (0..6)
        .map(|value| Expr::Literal(Value::I64(value)))
        .chain([division_by_zero()])
        .collect();
    assert!(
        open(column_table("v", Expr::List(expressions)), &execution)
            .await
            .is_err()
    );
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}

#[tokio::test]
async fn cancelled_or_dropped_column_table_opening_releases_its_unpublished_page_and_reader() {
    for cancel in [false, true] {
        let execution = execution(1, vec![]);
        let baseline = execution.context.memory.used_bytes();
        let mut opening = Box::pin(open(column_table("v", series(2_000_000_000)), &execution));
        assert!(matches!(
            futures::poll!(&mut opening),
            std::task::Poll::Pending
        ));
        assert!(execution.context.memory.used_bytes() > baseline);
        if cancel {
            execution.context.cancellation.cancel();
            assert_eq!(opening.await.err().unwrap().code, "xtdb/query-cancelled");
        } else {
            drop(opening);
        }
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn a_computed_column_table_keeps_one_batch_across_list_pages() {
    let execution = execution(1, vec![]);
    let expression = Expr::List((1..=7).map(|value| Expr::Literal(Value::I64(value))).collect());
    let relation = column_table("value", expression);
    let runtime = EmittedRelation::derive(relation, &execution.context.catalog, &[], &execution.context.memory).unwrap();
    let mut batches = crate::operators::build_columns(runtime, execution).await.unwrap().unwrap();
    let batch = batches.next_batch().await.unwrap().unwrap();
    assert_eq!(batch.len, 7);
    for row in 0..batch.len {
        assert_eq!(batch.columns[0].value(row).unwrap(), Value::I64(row as i64 + 1));
    }
    assert!(batches.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn computed_column_table_moves_one_span_and_assembles_multiple_float_spans() {
    use arrow_array::{Array, Float32Array};
    use crate::columnar::Column;

    for count in [3, 6] {
        let execution = execution(1, vec![]);
        let nan = f32::from_bits(0x7fc0_1234);
        let cells = [Some(-0.0_f32), None, Some(nan), None, Some(0.0), Some(nan)];
        let expected = cells[..count].iter().map(|cell| cell.map(f32::to_bits)).collect::<Vec<_>>();
        let array = Float32Array::from(cells[..count].to_vec());
        let source = array.values().inner().data_ptr().as_ptr() as usize;
        let expression = Expr::Literal(Value::List(rtdb_types::ListValue::from_f32(array)));
        let mut cursor = crate::column_table::open_columns(&execution, expression).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.len, count);
        let Column::Arrow { array, logical: Some(logical), .. } = &page.columns[0] else { panic!("expected a typed float vector"); };
        assert_eq!(*logical, LogicalType::Scalar(ScalarType::F32).maybe());
        let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(array.iter().map(|value| value.map(f32::to_bits)).collect::<Vec<_>>(), expected);
        if count == 3 {
            assert_eq!(array.values().inner().data_ptr().as_ptr() as usize, source);
        }
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn computed_column_table_keeps_mixed_float_widths_and_null_legs_in_order() {
    let f32_bits = 0x7fc0_1234;
    let f64_bits = 0x7ff8_0000_0000_1234;
    let cells = vec![
        Value::F32(f32::from_bits(f32_bits)),
        Value::Null,
        Value::F64(-0.0),
        Value::F64(f64::from_bits(f64_bits)),
        Value::F32(-0.0),
        Value::Null,
    ];
    for expression in [
        Expr::Literal(Value::list(cells.clone())),
        Expr::List(cells.into_iter().map(Expr::Literal).collect()),
    ] {
        let execution = execution(1, vec![]);
        let memory = &execution.context.memory;
        let baseline = memory.used_bytes();
        let mut cursor = crate::column_table::open_columns(&execution, expression).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(page.len, 6);
        assert!(matches!(page.columns[0].value(0).unwrap(), Value::F32(value) if value.to_bits() == f32_bits));
        assert!(cursor.next_batch().await.unwrap().is_none());
        drop(cursor);

        let page = page.slice(1, 5, memory).unwrap();
        let cell = |row| page.columns[0].value(row).unwrap();
        assert_eq!(cell(0), Value::Null);
        assert!(matches!(cell(1), Value::F64(value) if value.to_bits() == (-0.0_f64).to_bits()));
        assert!(matches!(cell(2), Value::F64(value) if value.to_bits() == f64_bits));
        assert!(matches!(cell(3), Value::F32(value) if value.to_bits() == (-0.0_f32).to_bits()));
        assert_eq!(cell(4), Value::Null);
        drop(page);
        assert_eq!(memory.used_bytes(), baseline);
    }
}

#[tokio::test]
async fn computed_column_table_cancellation_during_assembly_releases_all_pages() {
    let execution = execution(1, vec![]);
    let baseline = execution.context.memory.used_bytes();
    let mut opening = Box::pin(crate::column_table::open_columns(&execution, series(8)));
    for _ in 0..4 {
        assert!(matches!(futures::poll!(&mut opening), std::task::Poll::Pending));
    }
    execution.context.cancellation.cancel();
    assert_eq!(opening.await.err().unwrap().code, "xtdb/query-cancelled");
    assert_eq!(execution.context.memory.used_bytes(), baseline);
}
