use crate::{
    CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions,
    context::{Execution, OuterRows},
    operators::build_pages,
    pages::BoxPageCursor,
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
    let database = DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    };
    let catalog =
        CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(
        catalog,
        MemoryBudget::new(1 << 27),
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
) -> Result<(BoxPageCursor, Arc<RuntimeSchema>)> {
    let emitted = EmittedRelation::derive(
        relation,
        &execution.context.catalog,
        execution.parameters.types(),
        &execution.context.memory,
    )?;
    let output = emitted.output();
    Ok((build_pages(emitted, execution.clone()).await?, output))
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
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values.clone()));
        }
        assert_eq!(rows, case.expected.rows, "{}", case.id);
        drop(cursor);
        drop(types);
        assert_eq!(execution.context.memory.used_bytes(), baseline);
    }
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
            let page = cursor.next_page().await.unwrap().unwrap();
            assert_eq!(page.len(), expected_len);
            assert!(cursor.next_page().await.unwrap().is_none());
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
            RelationNode::PageOrdinality { input, field },
        )
        .unwrap(),
    );
    let (mut cursor, types) = open(relation, &execution).await.unwrap();
    assert_eq!(
        types.schema().fields()[1].data_type,
        LogicalType::Scalar(ScalarType::I64)
    );
    let page = cursor.next_page().await.unwrap().unwrap();
    assert_eq!(
        page.rows()
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        (1..8)
            .map(|value| vec![Value::I64(value), Value::I64(value)])
            .collect::<Vec<_>>()
    );
    assert!(cursor.next_page().await.unwrap().is_none());
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
