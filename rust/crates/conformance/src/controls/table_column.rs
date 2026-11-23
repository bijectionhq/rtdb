use super::{Error, ObservationReceipt, returned};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Anomaly, LogicalType, Timestamp, Value};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
pub(super) struct Input {
    id: String,
    column: String,
    parameter: bool,
    input: Value,
}

fn validate_input(value: &Value) -> Result<(), Error> {
    let scalar = |value: &Value| matches!(value, Value::Null | Value::I64(_) | Value::F64(_));
    if scalar(value) || matches!(value, Value::List(elements) if elements.iter().all(|value| scalar(value.as_ref()))) {
        Ok(())
    } else {
        Err(Anomaly::unsupported(
            "rtdb.conformance/column-table-fixture-type",
            "Column-table fixtures support null, I64, F64 and lists of those scalar values",
        )
        .into())
    }
}

pub(super) async fn observe(cases: Vec<Input>) -> Result<Vec<ObservationReceipt>, Error> {
    let database = DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
    let context = QueryContext::new(
        catalog,
        MemoryBudget::new(1 << 27),
        Timestamp(0),
        "UTC",
        QueryOptions::default(),
        &CancellationToken::new(),
    )?;
    let mut observations = Vec::with_capacity(cases.len());
    for case in cases {
        validate_input(&case.input)?;
        let (expr, arguments) = if case.parameter {
            (
                Expr::Parameter {
                    index: 0,
                    data_type: case.input.logical_type(),
                },
                vec![case.input],
            )
        } else {
            (Expr::Literal(case.input), vec![])
        };
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
            name: case.column,
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: LogicalType::join_all(&types),
        };
        let relation = Arc::new(Relation::new(
            Schema::new(vec![element.clone()])?,
            RelationNode::ColumnTable { expr, element },
        )?);
        let mut cursor = PreparedQuery::new(relation)?
            .execute(Arc::clone(&context), arguments)
            .await?;
        let field = &cursor.runtime_schema().fields()[0];
        let column = field.name.clone();
        let data_type = field.data_type.clone();
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() {
                rows.push(
                    batch
                        .batch()
                        .schema()
                        .fields()
                        .iter()
                        .zip(batch.batch().columns())
                        .map(|(field, values)| read_value(field, values.as_ref(), row))
                        .collect::<rtdb_types::Result<Vec<_>>>()?,
                );
            }
        }
        observations.push(returned(
            case.id,
            json!({(column): {"data_type": data_type, "rows": rows}}),
        ));
    }
    Ok(observations)
}
