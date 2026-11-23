use std::sync::Arc;

use rtdb_columnar::OwnedBatch;
use rtdb_plan::{Catalog, QualifiedTable};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, Result, TableRef, TimeUnit, Timestamp, Value,
};

use crate::{
    QueryContext, RuntimeSchema, owned_relation::open_owned_table_point, owned_row_revision,
};

pub struct HostedControlPoint {
    pub row: Option<(OwnedBatch, Timestamp)>,
    pub schema: Arc<RuntimeSchema>,
    pub receipt_tx_id: Option<i64>,
}

/// Exhaust one exact ordinary control cell over its installed epoch interval.
/// A new table is not an absence until the installed catalog covers it.
pub async fn read_hosted_control_point(
    context: Arc<QueryContext>,
    table: TableRef,
    id: &EntityId,
    include_payload: bool,
) -> Result<HostedControlPoint> {
    if table.schema != "public"
        || !table.table.starts_with("bj_")
        || table.table.contains('-')
        || !matches!(id, EntityId::String(text) if !text.is_empty() && text.len() <= 4096)
    {
        return Err(Anomaly::incorrect(
            "rtdb.hosted/control",
            "Invalid hosted control point",
        ));
    }
    let catalog = QualifiedTable {
        database: context.catalog.default_database().into(),
        table: table.clone(),
    };
    let table_binding = context.catalog.table(&catalog)?.ok_or_else(unavailable)?;
    let mut columns = vec!["_valid_from", "_valid_to"];
    let stamped_receipt = include_payload && table.table == rtdb_types::HOSTED_ROOT_RECEIPTS_TABLE
        && table_binding.columns.contains_key(rtdb_types::HOSTED_RECEIPT_TX_ID);
    if stamped_receipt {
        columns.push(rtdb_types::HOSTED_RECEIPT_TX_ID);
    }
    if include_payload {
        for name in table_binding
            .columns
            .keys()
            .filter(|name| !name.starts_with('_'))
        {
            if columns.len() >= 129 {
                return Err(unavailable());
            }
            columns.push(name.as_str());
        }
    }
    let valid = Interval::new(Timestamp(0), Timestamp::MAX)?;
    let mut cursor = open_owned_table_point(
        context.clone(),
        table,
        id.iid(),
        Some(id),
        Some(valid),
        Some(&columns),
        false,
    )
    .await?
    .ok_or_else(unavailable)?;
    let schema = cursor.retained_runtime_schema();
    let mut row = None;
    let mut receipt_tx_id = None;
    while let Some(batch) = cursor.next_batch().await? {
        let data = batch.batch();
        if data.num_rows() == 0 {
            continue;
        }
        if data.num_rows() != 1 || row.is_some() {
            return Err(unavailable());
        }
        let column = |name| {
            let (slot, field) = data
                .schema_ref()
                .column_with_name(name)
                .ok_or_else(unavailable)?;
            rtdb_columnar::read_value(field, data.column(slot).as_ref(), 0)
        };
        if !matches!(
            column("_valid_from")?,
            Value::Timestamp {
                value: 0,
                unit: TimeUnit::Microsecond,
                ..
            }
        ) || column("_valid_to")? != Value::Null
        {
            return Err(unavailable());
        }
        let revision = owned_row_revision(data, 0, id, &context.memory)?;
        if stamped_receipt {
            receipt_tx_id = match column(rtdb_types::HOSTED_RECEIPT_TX_ID)? {
                Value::I64(id) if id >= 0 => Some(id),
                Value::Null | Value::Undefined => None,
                _ => return Err(unavailable()),
            };
        }
        row = Some((batch, revision));
    }
    context.check_cancelled()?;
    Ok(HostedControlPoint { row, schema, receipt_tx_id })
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.hosted/control-scope",
        "The installed hosted control does not cover one complete epoch slice",
    )
}
