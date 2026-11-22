use std::{fmt::Write, sync::Arc};

use rtdb_columnar::OwnedBatch;
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, OwnedRelation, Result, StructFields, Timestamp,
    Value,
};

use crate::{
    QueryContext, QueryCursor, open_owned_point, open_owned_unique_point, owned_row_revision,
};

pub const OWNED_UNIQUE_KEY_MAX: usize = 4096;
const DECLARATION_ID: &str = "!path";

pub struct OwnedUniqueRule {
    pub path: OwnedRelation,
    pub column: String,
    pub through_events: i64,
}

impl OwnedUniqueRule {
    pub fn document(&self) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Utf8(DECLARATION_ID.into())),
            ("path".into(), Value::Utf8(self.path.name().into())),
            ("column".into(), Value::Utf8(self.column.clone())),
            ("through_events".into(), Value::I64(self.through_events)),
        ])
    }
}

pub fn owned_unique_key_id(key: &[u8]) -> EntityId {
    let mut name = String::with_capacity(1 + key.len() * 2);
    name.push('k');
    for byte in key {
        write!(&mut name, "{byte:02x}").expect("writing to a string cannot fail");
    }
    EntityId::String(name)
}

pub async fn read_owned_unique_rule(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
) -> Result<OwnedUniqueRule> {
    let id = EntityId::String(DECLARATION_ID.into());
    let cursor = open_owned_unique_point(
        context.clone(),
        owner,
        relation,
        &id,
        &["path", "column", "through_events"],
    )
    .await?;
    let batch = single(cursor).await?.ok_or_else(uncovered)?;
    owned_row_revision(batch.batch(), 0, &id, &context.memory)?;
    let (Value::Utf8(path), Value::Utf8(column), Value::I64(through_events)) = (
        cell(&batch, "path", &context)?,
        cell(&batch, "column", &context)?,
        cell(&batch, "through_events", &context)?,
    ) else {
        return Err(uncovered());
    };
    if through_events < 0 || column.is_empty() || column.len() > 96 || column.starts_with('_') {
        return Err(uncovered());
    }
    let path = OwnedRelation::new(path).map_err(|_| uncovered())?;
    Ok(OwnedUniqueRule {
        path,
        column,
        through_events,
    })
}

pub struct OwnedUniqueHit {
    pub row: OwnedBatch,
    pub revision: Timestamp,
}

/// Checks an occupancy at the caller's already admitted system and valid cut.
pub async fn lookup_owned_unique(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    rule: &OwnedUniqueRule,
    key: &[u8],
    valid: Timestamp,
) -> Result<Option<OwnedUniqueHit>> {
    if !(1..=OWNED_UNIQUE_KEY_MAX).contains(&key.len()) {
        return Err(Anomaly::incorrect(
            "rtdb.owned/unique-key",
            "A unique lookup needs a complete Binary key of at most 4096 bytes",
        ));
    }
    let _key = context.memory.reserve(key.len() * 4 + 4096)?;
    let id = owned_unique_key_id(key);
    let cursor =
        open_owned_unique_point(context.clone(), owner, relation, &id, &["holder"]).await?;
    let Some(row) = single(cursor).await? else {
        return Ok(None);
    };
    owned_row_revision(row.batch(), 0, &id, &context.memory)?;
    let holder = cell(&row, "holder", &context)?
        .entity_id()
        .map_err(|_| uncovered())?;
    let valid = Interval::at(valid)?;
    let cursor =
        open_owned_point(context.clone(), owner, relation, &holder, Some(valid), None).await?;
    let row = single(cursor).await?.ok_or_else(uncovered)?;
    let revision = owned_row_revision(row.batch(), 0, &holder, &context.memory)?;
    match cell(&row, &rule.column, &context)? {
        Value::Binary(actual) if actual.as_slice() == key => {
            Ok(Some(OwnedUniqueHit { row, revision }))
        }
        _ => Err(uncovered()),
    }
}

async fn single(cursor: Option<QueryCursor>) -> Result<Option<OwnedBatch>> {
    let Some(mut cursor) = cursor else {
        return Ok(None);
    };
    let mut result = None;
    while let Some(batch) = cursor.next_batch().await? {
        if batch.batch().num_rows() == 0 {
            continue;
        }
        if batch.batch().num_rows() != 1 || result.replace(batch).is_some() {
            return Err(uncovered());
        }
    }
    Ok(result)
}

fn cell(batch: &OwnedBatch, column: &str, context: &QueryContext) -> Result<Value> {
    let batch = batch.batch();
    let (slot, field) = batch
        .schema_ref()
        .column_with_name(column)
        .ok_or_else(uncovered)?;
    let array = batch.column(slot).as_ref();
    let bytes = rtdb_columnar::estimate_read_value_bytes(field, array, 0)?;
    let _read = context.memory.reserve(bytes.saturating_add(4096))?;
    rtdb_columnar::read_value(field, array, 0)
}

fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/unique-uncovered",
        "The owned unique path cannot prove its complete occupancy",
    )
}
