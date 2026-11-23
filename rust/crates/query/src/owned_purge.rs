//! Ordered purge floors use complete typed identities. A digest is a bounded
//! bucket locator, never an identity or permission to purge another handle.
use std::{collections::BTreeMap, sync::Arc};

use rtdb_plan::Catalog;
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, StructFields, Timestamp, Value,
    OWNED_CHANGE_GROUP_BYTES,
};

use crate::{QueryContext, owned_relation::open_owned_table_point, owned_row_revision};

const MAX_BUCKET: usize = 64;
const COVERAGE: EntityId = EntityId::Uuid([0; 16]);

pub struct OwnedPurgeBucket {
    id: EntityId,
    entries: BTreeMap<Vec<u8>, Timestamp>,
}

impl OwnedPurgeBucket {
    pub fn advance(&mut self, identity: &EntityId, at: Timestamp) -> Result<StructFields<Value>> {
        let handle = complete_handle(identity)?;
        if !self.entries.contains_key(&handle) && self.entries.len() == MAX_BUCKET {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.owned/purge-bucket-full",
                "The exact purge bucket exceeds its bound",
            ));
        }
        if bucket_id(identity) != self.id
            || self
                .entries
                .get(&handle)
                .is_some_and(|previous| *previous >= at)
        {
            return Err(uncovered());
        }
        self.entries.insert(handle, at);
        Ok(StructFields::from([
            ("_id".into(), self.id.as_value()),
            (
                "floors".into(),
                Value::list(
                    self.entries
                        .iter()
                        .map(|(handle, at)| {
                            Value::Struct(StructFields::from([
                                ("handle".into(), Value::Binary(handle.clone())),
                                ("at".into(), Value::I64(at.0)),
                            ]))
                        })
                        .collect(),
                ),
            ),
        ]))
    }
}

pub fn purge_coverage_document(through_events: i64) -> StructFields<Value> {
    StructFields::from([
        ("_id".into(), COVERAGE.as_value()),
        ("through_events".into(), Value::I64(through_events)),
    ])
}

pub async fn owned_purge_floor(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
) -> Result<Option<Timestamp>> {
    let bucket = read_owned_purge_bucket(context, owner, relation, id).await?;
    if bucket.entries.is_empty() || exceeds_handle_bound(id) {
        return Ok(None);
    }
    Ok(bucket.entries.get(&id.handle_bytes()).copied())
}

pub async fn read_owned_purge_bucket(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    identity: &EntityId,
) -> Result<OwnedPurgeBucket> {
    if owner.is_empty() || relation.world().is_some() || relation.is_world_catalog() {
        return Err(uncovered());
    }
    let database = context
        .catalog
        .database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(uncovered());
    };
    if snapshot.contains_table(&OwnedRelation::obsolete_purge_table(owner)) {
        return Err(uncovered());
    }
    let through = snapshot.relation_event_frontier(&relation.purge_table(owner))?;
    let id = bucket_id(identity);
    let mut entries = BTreeMap::new();
    if through == 0 {
        if snapshot.contains_table(&relation.purge_floor_table(owner)) {
            return Err(uncovered());
        }
        return Ok(OwnedPurgeBucket { id, entries });
    }
    let coverage = cell(
        context.clone(),
        owner,
        relation,
        &COVERAGE,
        "through_events",
    )
    .await?;
    match coverage {
        Some((Value::I64(count), _)) if count == through => {}
        _ => return Err(uncovered()),
    }
    if let Some((floors, revision)) = cell(context, owner, relation, &id, "floors").await? {
        let Value::List(floors) = floors else {
            return Err(uncovered());
        };
        if floors.is_empty() || floors.len() > MAX_BUCKET {
            return Err(uncovered());
        }
        for floor in floors.iter() {
            let Value::Struct(entry) = floor.as_ref() else {
                return Err(uncovered());
            };
            let (Some(Value::Binary(handle)), Some(Value::I64(time))) =
                (entry.get("handle"), entry.get("at"))
            else {
                return Err(uncovered());
            };
            if entry.len() != 2 {
                return Err(uncovered());
            }
            if handle.len() > OWNED_CHANGE_GROUP_BYTES
                || Timestamp(*time) > revision
                || entries
                    .last_key_value()
                    .is_some_and(|(previous, _)| previous >= handle)
            {
                return Err(uncovered());
            }
            let exact = EntityId::from_handle_bytes(handle).map_err(|_| uncovered())?;
            if bucket_id(&exact) != id {
                return Err(uncovered());
            }
            entries.insert(handle.clone(), Timestamp(*time));
        }
    }
    Ok(OwnedPurgeBucket { id, entries })
}

async fn cell(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
    column: &str,
) -> Result<Option<(Value, Timestamp)>> {
    let Some(mut cursor) = open_owned_table_point(
        context.clone(),
        relation.purge_floor_table(owner),
        id.iid(),
        Some(id),
        None,
        Some(&[column]),
        false,
    )
    .await?
    else {
        return Ok(None);
    };
    let mut found = None;
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        if batch.num_rows() == 0 {
            continue;
        }
        if found.is_some() || batch.num_rows() != 1 {
            return Err(uncovered());
        }
        let revision = owned_row_revision(batch, 0, id, &context.memory)?;
        let (position, field) = batch
            .schema_ref()
            .column_with_name(column)
            .ok_or_else(uncovered)?;
        let array = batch.column(position).as_ref();
        let size = rtdb_columnar::estimate_read_value_bytes(field, array, 0)?;
        let _scratch = context.memory.reserve(size.saturating_add(4096))?;
        found = Some((rtdb_columnar::read_value(field, array, 0)?, revision));
    }
    Ok(found)
}

fn complete_handle(id: &EntityId) -> Result<Vec<u8>> {
    if exceeds_handle_bound(id) {
        return Err(Anomaly::incorrect(
            "rtdb.owned/purge-handle-bound",
            "The complete purge handle exceeds its bound",
        ));
    }
    Ok(id.handle_bytes())
}

fn exceeds_handle_bound(id: &EntityId) -> bool {
    matches!(id, EntityId::String(text) if text.len() >= OWNED_CHANGE_GROUP_BYTES)
        || matches!(id, EntityId::Keyword(text) if text.as_str().len() >= OWNED_CHANGE_GROUP_BYTES)
}

fn bucket_id(id: &EntityId) -> EntityId {
    let mut locator = id.iid().0;
    // Keep the control entry outside the bucket address space.
    locator[0] |= 0x80;
    EntityId::Uuid(locator)
}

fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/purge-history-reset",
        "The relation cannot prove its complete purge floors",
    )
}
