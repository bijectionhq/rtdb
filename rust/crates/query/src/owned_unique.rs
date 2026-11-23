use std::{collections::BTreeMap, fmt::Write, sync::Arc};

use rtdb_columnar::OwnedBatch;
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, OwnedRelation, Result, StructFields, Timestamp,
    Value,
};

use crate::{
    QueryContext, QueryCursor, open_owned_point, open_owned_unique_point, owned_row_revision,
};

pub const OWNED_UNIQUE_KEY_MAX: usize = 4096;
pub const OWNED_UNIQUE_PATHS_MAX: usize = 16;
pub const OWNED_UNIQUE_POLICY_ID: &str = "!path";

#[derive(Default)]
pub struct OwnedUniquePolicy {
    pub paths: BTreeMap<OwnedRelation, String>,
    pub through_events: i64,
}

impl OwnedUniquePolicy {
    pub fn document(&self) -> StructFields<Value> {
        let paths = self
            .paths
            .iter()
            .map(|(path, column)| {
                Value::Struct(StructFields::from([
                    ("path".into(), Value::Utf8(path.name().into())),
                    ("column".into(), Value::Utf8(column.clone())),
                ]))
            })
            .collect();
        StructFields::from([
            ("_id".into(), Value::Utf8(OWNED_UNIQUE_POLICY_ID.into())),
            ("paths".into(), Value::list(paths)),
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

pub fn owned_unique_path_key(path: &OwnedRelation, key: &[u8]) -> EntityId {
    let name = path.name().as_bytes();
    let mut bytes = Vec::with_capacity(1 + name.len() + key.len());
    bytes.push(name.len() as u8);
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(key);
    owned_unique_key_id(&bytes)
}

pub async fn read_owned_unique_policy(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
) -> Result<OwnedUniquePolicy> {
    let id = EntityId::String(OWNED_UNIQUE_POLICY_ID.into());
    let cursor = open_owned_unique_point(
        context.clone(),
        owner,
        relation,
        &id,
        &["paths", "through_events"],
    )
    .await?;
    let batch = single(cursor).await?.ok_or_else(uncovered)?;
    owned_row_revision(batch.batch(), 0, &id, &context.memory)?;
    let (Value::List(paths), Value::I64(through_events)) = (
        cell(&batch, "paths", &context)?,
        cell(&batch, "through_events", &context)?,
    ) else {
        return Err(uncovered());
    };
    if through_events < 0 || !(1..=OWNED_UNIQUE_PATHS_MAX).contains(&paths.len()) {
        return Err(uncovered());
    }
    let mut installed = BTreeMap::new();
    for path in paths.iter() {
        let Value::Struct(path) = path.as_ref() else {
            return Err(uncovered());
        };
        let (Some(Value::Utf8(name)), Some(Value::Utf8(column))) =
            (path.get("path"), path.get("column"))
        else {
            return Err(uncovered());
        };
        let name = OwnedRelation::new(name.clone()).map_err(|_| uncovered())?;
        if path.len() != 2
            || column.is_empty()
            || column.len() > 96
            || column.starts_with('_')
            || installed
                .last_key_value()
                .is_some_and(|(prior, _)| prior >= &name)
        {
            return Err(uncovered());
        }
        installed.insert(name, column.clone());
    }
    Ok(OwnedUniquePolicy {
        paths: installed,
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
    path: &OwnedRelation,
    column: &str,
    key: &[u8],
    valid: Timestamp,
) -> Result<Option<OwnedUniqueHit>> {
    if !(1..=OWNED_UNIQUE_KEY_MAX).contains(&key.len()) {
        return Err(Anomaly::incorrect(
            "rtdb.owned/unique-key",
            "A unique lookup needs a complete Binary key of at most 4096 bytes",
        ));
    }
    let _key = context
        .memory
        .reserve((key.len() + path.name().len()) * 4 + 4096)?;
    let id = owned_unique_path_key(path, key);
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
    match cell(&row, column, &context)? {
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
