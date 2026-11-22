use std::sync::Arc;

use rtdb_plan::{Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, TemporalFilter};
use rtdb_types::{Anomaly, EntityId, Interval, OwnedRelation, Result, TableRef, TimeUnit, Timestamp, Value};

use crate::{PreparedQuery, QueryContext, QueryCursor};

/// Opens a single admitted IID read without granting SQL access to the owned namespace.
pub async fn open_owned_point(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
    valid: Option<Interval>,
    columns: Option<&[&str]>,
) -> Result<Option<QueryCursor>> {
    if owner.is_empty() {
        return Err(Anomaly::incorrect("rtdb.owned/no-owner", "Owned relations require an authenticated principal"));
    }
    let name = QualifiedTable {
        database: context.catalog.default_database().to_owned(),
        table: relation.table(owner),
    };
    let context = context.for_owned_relation(name.clone(), Some(id.clone()));
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let mut fields: Vec<&str> = columns.map_or_else(|| table.columns.keys().map(String::as_str).collect(), |names| names.to_vec());
    for required in ["_id", "_system_from"] {
        if !fields.contains(&required) { fields.push(required); }
    }
    let columns = fields.into_iter().enumerate().map(|(index, name)| Field {
        id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: table.scan_column_type(name),
    }).collect();
    let timestamp = |time: Timestamp| Expr::Literal(Value::Timestamp {
        value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
    });
    let valid_time = valid.map_or(TemporalFilter::Default, |valid| TemporalFilter::FromTo {
        from: timestamp(valid.lower()), to: timestamp(valid.upper()),
    });
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time, system_time: TemporalFilter::Default, clamp_valid_time: false,
        iids: Some([id.iid()].into()), predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = PreparedQuery::new_in(scan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}

/// Interprets the winning revision only after checking the complete stored identity.
pub fn owned_row_revision(
    batch: &arrow_array::RecordBatch,
    row: usize,
    expected_id: &rtdb_types::EntityId,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> Result<Timestamp> {
    let schema = batch.schema_ref();
    let (id_column, id_field) = schema.column_with_name("_id")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned read omitted its identity column"))?;
    let id = batch.column(id_column).as_ref();
    let bound = rtdb_columnar::estimate_read_value_bytes(id_field, id, row)?;
    let _scratch = memory.reserve(bound.checked_add(4096)
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-identity", "Owned identity allocation overflow"))?)?;
    if &rtdb_columnar::read_value(id_field, id, row)?.entity_id()? != expected_id {
        return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.owned/iid-collision", "Owned row locator resolves to a different complete identity"));
    }
    let (revision_column, revision_field) = schema.column_with_name("_system_from")
        .ok_or_else(|| Anomaly::fault("rtdb.owned/row-revision", "Owned read omitted its revision column"))?;
    match rtdb_columnar::read_value(revision_field, batch.column(revision_column).as_ref(), row)? {
        Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Ok(Timestamp(value)),
        _ => Err(Anomaly::fault("rtdb.owned/row-revision", "Owned row revision is not a microsecond timestamp")),
    }
}

/// Reads the fixed protected path catalog shape for engine maintenance and resolution.
pub async fn open_owned_path_catalog(
    context: Arc<QueryContext>,
    catalog: &TableRef,
    path: Option<&str>,
) -> Result<Option<QueryCursor>> {
    if !catalog.is_owned_path_catalog() {
        return Err(Anomaly::incorrect("rtdb.owned/path-catalog", "Expected an owned path catalog"));
    }
    let name = QualifiedTable { database: context.catalog.default_database().to_owned(), table: catalog.clone() };
    let context = context.for_owned_relation(name.clone(), None);
    let Some(table) = context.catalog.table(&name)? else { return Ok(None); };
    let columns = ["_id", "schema_name", "table_name", "columns", "version"]
        .into_iter().enumerate().map(|(index, name)| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false,
            unnamed: false, data_type: table.scan_column_type(name),
        }).collect();
    let scan = Arc::new(Relation::scan(Scan {
        table, columns, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
        clamp_valid_time: false, iids: path.map(|name| [EntityId::String(name.into()).iid()].into()),
        predicates: Vec::new(), matches: Vec::new(), top_k: None, ordered_top_k: None,
    })?);
    let query = PreparedQuery::new_in(scan, &context.memory)?;
    query.execute(context, Vec::new()).await.map(Some)
}
