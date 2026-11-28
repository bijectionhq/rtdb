//! Ordered purge floors use complete typed identities. A digest is a bounded
//! bucket locator, never an identity or permission to purge another handle.
//! A floor names the purging transaction by its position, with its system
//! time.
use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Array, ArrayRef, ListArray, StructArray, cast::AsArray};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{EntityIdRef, MemoryBudget};
use rtdb_plan::Catalog;
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, StructFields, Timestamp,
    TransactionKey, TxId, Value, OWNED_CHANGE_GROUP_BYTES,
};

use crate::{QueryContext, owned_relation::open_owned_table_point, owned_row_revision};

const MAX_BUCKET: usize = 64;
const COVERAGE: EntityId = EntityId::Uuid([0; 16]);

pub struct OwnedPurgeBucket {
    id: EntityId,
    entries: Floors,
}

enum BucketCell {
    Arrow(CanonicalFloors),
    Decoded(Value),
}

enum Floors {
    Decoded(BTreeMap<Vec<u8>, TransactionKey>),
    Arrow(CanonicalFloors),
}

struct CanonicalFloors {
    item: Arc<Field>,
    entries: ArrayRef,
}

impl CanonicalFloors {
    fn select(field: &Field, array: &ArrayRef) -> Option<Self> {
        let DataType::List(item) = field.data_type() else { return None; };
        if !field.metadata().is_empty() || item.as_ref() != &Field::new(
            "$data$",
            DataType::Struct(vec![
                Field::new("handle", DataType::Binary, false),
                Field::new("at", DataType::Int64, false),
                Field::new("tx", DataType::Int64, false),
            ].into()),
            false,
        ) || array.data_type() != field.data_type() || array.is_null(0) {
            return None;
        }
        let entries = array.as_any().downcast_ref::<ListArray>()?.value(0);
        Some(Self { item: Arc::clone(item), entries })
    }

    fn entry(&self, row: usize) -> Option<(&[u8], TransactionKey)> {
        let entries = self.entries.as_struct();
        let [handles, times, txs] = entries.columns() else { return None; };
        if entries.is_null(row) || handles.is_null(row) || times.is_null(row) || txs.is_null(row) {
            return None;
        }
        Some((handles.as_binary::<i32>().value(row), TransactionKey {
            tx_id: TxId(txs.as_primitive::<arrow_array::types::Int64Type>().value(row)),
            system_time: Timestamp(times.as_primitive::<arrow_array::types::Int64Type>().value(row)),
        }))
    }

    fn document(&self, id: &EntityId, handle: &[u8], at: TransactionKey, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
        let position = (0..self.entries.len()).find(|&row| self.handle(row) >= handle).unwrap_or(self.entries.len());
        let replaces = position < self.entries.len() && self.handle(position) == handle;
        let rows = self.entries.len() + usize::from(!replaces);
        let needed = (0..self.entries.len()).try_fold(16_384usize, |bytes, row| {
            Ok::<_, Anomaly>(bytes.saturating_add(rtdb_columnar::estimate_event_copy_cell_bytes(&self.item, self.entries.as_ref(), row)?))
        })?.saturating_add(handle.len().saturating_mul(2));
        let mut reservation = memory.reserve(needed)?;
        let changed = Arc::new(StructArray::try_new(
            self.entries.as_struct().fields().clone(),
            vec![
                Arc::new(arrow_array::BinaryArray::from_iter_values([handle])),
                Arc::new(arrow_array::Int64Array::from(vec![at.system_time.0])),
                Arc::new(arrow_array::Int64Array::from(vec![at.tx_id.0])),
            ],
            None,
        ).map_err(rtdb_columnar::arrow_error)?) as ArrayRef;
        let positions = (0..position).map(|row| Some((0, row)))
            .chain([Some((1, 0))])
            .chain((position + usize::from(replaces)..self.entries.len()).map(|row| Some((0, row))))
            .collect::<Vec<_>>();
        let entries = rtdb_columnar::copy_event_cells(&[self.entries.as_ref(), changed.as_ref()], &positions)?;
        let floors = Arc::new(ListArray::try_new(
            Arc::clone(&self.item), arrow_buffer::OffsetBuffer::from_lengths([rows]), entries, None,
        ).map_err(rtdb_columnar::arrow_error)?) as ArrayRef;
        let id = id.as_value();
        let id_type = id.logical_type();
        let identity = rtdb_columnar::build_array(&id_type, &[id])?;
        let fields = vec![
            rtdb_columnar::to_field("_id", &id_type)?,
            Field::new("floors", floors.data_type().clone(), false),
        ];
        let document = rtdb_columnar::build_struct_array(fields.into(), &[identity, floors], 1, None)?;
        Arc::get_mut(&mut reservation).expect("Purge floor construction owns its reservation")
            .shrink_to(document.get_array_memory_size())?;
        rtdb_columnar::retain_array(document, &reservation)
    }

    fn handle(&self, row: usize) -> &[u8] {
        self.entries.as_struct().column(0).as_binary::<i32>().value(row)
    }
}

impl Floors {
    fn len(&self) -> usize {
        match self { Self::Arrow(floors) => floors.entries.len(), Self::Decoded(floors) => floors.len() }
    }

    fn get(&self, handle: &[u8]) -> Option<TransactionKey> {
        match self {
            Self::Decoded(floors) => floors.get(handle).copied(),
            Self::Arrow(floors) => (0..floors.entries.len()).find(|&row| floors.handle(row) == handle)
                .and_then(|row| floors.entry(row).map(|(_, at)| at)),
        }
    }
}

pub struct OwnedPurgeUpdate {
    bucket: OwnedPurgeBucket,
    handle: Vec<u8>,
    at: TransactionKey,
}

impl OwnedPurgeUpdate {
    pub fn id(&self) -> &EntityId { &self.bucket.id }

    pub fn arrow_document(&self, memory: &Arc<MemoryBudget>) -> Result<Option<ArrayRef>> {
        match &self.bucket.entries {
            Floors::Decoded(_) => Ok(None),
            Floors::Arrow(floors) => floors.document(&self.bucket.id, &self.handle, self.at, memory).map(Some),
        }
    }

    pub fn document(self) -> StructFields<Value> {
        let mut entries = match self.bucket.entries {
            Floors::Decoded(entries) => entries,
            Floors::Arrow(floors) => (0..floors.entries.len()).map(|row| {
                let (handle, at) = floors.entry(row).expect("Validated purge floor");
                (handle.to_vec(), at)
            }).collect(),
        };
        entries.insert(self.handle, self.at);
        StructFields::from([
            ("_id".into(), self.bucket.id.as_value()),
            (
                "floors".into(),
                Value::list(entries.into_iter().map(|(handle, at)| {
                    Value::Struct(StructFields::from([
                        ("handle".into(), Value::Binary(handle)),
                        ("at".into(), Value::I64(at.system_time.0)),
                        ("tx".into(), Value::I64(at.tx_id.0)),
                    ]))
                }).collect()),
            ),
        ])
    }
}

impl OwnedPurgeBucket {
    pub fn advance(self, identity: &EntityId, at: TransactionKey) -> Result<OwnedPurgeUpdate> {
        let handle = complete_handle(identity)?;
        let previous = self.entries.get(&handle);
        if previous.is_none() && self.entries.len() == MAX_BUCKET {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.owned/purge-bucket-full",
                "The exact purge bucket exceeds its bound",
            ));
        }
        if bucket_id(identity.into()) != self.id
            || previous.is_some_and(|previous| previous.tx_id >= at.tx_id || previous.system_time >= at.system_time)
        {
            return Err(uncovered());
        }
        Ok(OwnedPurgeUpdate { bucket: self, handle, at })
    }

    fn floor(&self, id: EntityIdRef<'_>) -> Option<Timestamp> {
        if self.entries.len() == 0 || exceeds_handle_bound(id) {
            return None;
        }
        let mut integer = [0; 8];
        let (tag, payload) = id.handle_parts(&mut integer);
        let mut handle = Vec::with_capacity(1 + payload.len());
        handle.push(tag);
        handle.extend_from_slice(payload);
        self.entries.get(&handle).map(|floor| floor.system_time)
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
    owned_purge_floor_for(context, owner, relation, id.into()).await
}

#[doc(hidden)]
pub async fn owned_purge_floor_for(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: EntityIdRef<'_>,
) -> Result<Option<Timestamp>> {
    Ok(read_owned_purge_bucket_for(context, owner, relation, id).await?.floor(id))
}

pub async fn read_owned_purge_bucket(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    identity: &EntityId,
) -> Result<OwnedPurgeBucket> {
    read_owned_purge_bucket_for(context, owner, relation, identity.into()).await
}

async fn read_owned_purge_bucket_for(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    identity: EntityIdRef<'_>,
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
    let empty = || Floors::Decoded(BTreeMap::new());
    if through == 0 {
        if snapshot.contains_table(&relation.purge_floor_table(owner)) {
            return Err(uncovered());
        }
        return Ok(OwnedPurgeBucket { id, entries: empty() });
    }
    let coverage = cell(
        context.clone(),
        owner,
        relation,
        &COVERAGE,
        "through_events",
        |field, array| rtdb_columnar::read_value(field, array.as_ref(), 0),
    )
    .await?;
    match coverage {
        Some((Value::I64(count), _)) if count == through => {}
        _ => return Err(uncovered()),
    }
    let entries = match cell(context, owner, relation, &id, "floors", |field, array| {
        if let Some(floors) = CanonicalFloors::select(field, array) {
            Ok(BucketCell::Arrow(floors))
        } else {
            rtdb_columnar::read_value(field, array.as_ref(), 0).map(BucketCell::Decoded)
        }
    }).await? {
        None => empty(),
        Some((BucketCell::Arrow(floors), revision)) => {
            if !(1..=MAX_BUCKET).contains(&floors.entries.len()) {
                return Err(uncovered());
            }
            let mut previous = None;
            for row in 0..floors.entries.len() {
                let (handle, at) = floors.entry(row).ok_or_else(uncovered)?;
                validate_floor(&id, previous, handle, at, revision)?;
                previous = Some(handle);
            }
            Floors::Arrow(floors)
        }
        Some((BucketCell::Decoded(Value::List(floors)), revision)) => {
            if !(1..=MAX_BUCKET).contains(&floors.len()) {
                return Err(uncovered());
            }
            let mut entries: BTreeMap<Vec<u8>, TransactionKey> = BTreeMap::new();
            for floor in floors.iter() {
                let Value::Struct(entry) = floor.as_ref() else {
                    return Err(uncovered());
                };
                let (Some(Value::Binary(handle)), Some(Value::I64(time)), Some(Value::I64(tx))) =
                    (entry.get("handle"), entry.get("at"), entry.get("tx"))
                else {
                    return Err(uncovered());
                };
                if entry.len() != 3 {
                    return Err(uncovered());
                }
                let at = TransactionKey { tx_id: TxId(*tx), system_time: Timestamp(*time) };
                validate_floor(&id, entries.last_key_value().map(|(previous, _)| previous.as_slice()), handle, at, revision)?;
                entries.insert(handle.clone(), at);
            }
            Floors::Decoded(entries)
        }
        Some((BucketCell::Decoded(_), _)) => return Err(uncovered()),
    };
    Ok(OwnedPurgeBucket { id, entries })
}

fn validate_floor(id: &EntityId, previous: Option<&[u8]>, handle: &[u8], at: TransactionKey, revision: Timestamp) -> Result<()> {
    if handle.len() > OWNED_CHANGE_GROUP_BYTES || at.tx_id.0 < 0 || at.system_time > revision
        || previous.is_some_and(|prior| prior >= handle)
    {
        return Err(uncovered());
    }
    let exact = EntityIdRef::from_handle_bytes(handle).map_err(|_| uncovered())?;
    if bucket_id(exact) != *id {
        return Err(uncovered());
    }
    Ok(())
}

async fn cell<T>(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    id: &EntityId,
    column: &str,
    read: impl Fn(&Field, &ArrayRef) -> Result<T>,
) -> Result<Option<(T, Timestamp)>> {
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
        let array = batch.column(position);
        let size = rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0)?;
        let _scratch = context.memory.reserve(size.saturating_add(4096))?;
        found = Some((read(field, array)?, revision));
    }
    Ok(found)
}

fn complete_handle(id: &EntityId) -> Result<Vec<u8>> {
    if exceeds_handle_bound(id.into()) {
        return Err(Anomaly::incorrect(
            "rtdb.owned/purge-handle-bound",
            "The complete purge handle exceeds its bound",
        ).with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded));
    }
    Ok(id.handle_bytes())
}

fn exceeds_handle_bound(id: EntityIdRef<'_>) -> bool {
    id.owned_payload_bytes() >= OWNED_CHANGE_GROUP_BYTES
}

fn bucket_id(id: EntityIdRef<'_>) -> EntityId {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ordered(tx: i64, time: i64) -> TransactionKey {
        TransactionKey { tx_id: TxId(tx), system_time: Timestamp(time) }
    }

    fn canonical_bucket(
        id: EntityId, rows: impl IntoIterator<Item = (EntityId, TransactionKey)>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<OwnedPurgeBucket> {
        let floors = Value::list(rows.into_iter().map(|(handle, at)| Value::Struct(StructFields::from([
            ("handle".into(), Value::Binary(handle.handle_bytes())),
            ("at".into(), Value::I64(at.system_time.0)),
            ("tx".into(), Value::I64(at.tx_id.0)),
        ]))).collect());
        let ty = floors.logical_type();
        let field = rtdb_columnar::to_field("floors", &ty)?;
        let array = rtdb_columnar::build_array(&ty, &[floors.clone(), floors])?;
        let array = rtdb_columnar::retain_array(array.slice(1, 1), &memory.reserve(1 << 16)?)?;
        let source = CanonicalFloors::select(&field, &array).unwrap();
        Ok(OwnedPurgeBucket { id, entries: Floors::Arrow(source) })
    }

    #[test]
    fn a_canonical_bucket_keeps_colliding_handles_in_order_when_inserting_or_advancing_a_floor() -> Result<()> {
        let identity = EntityId::String("same-digest".into());
        let collision = EntityId::Uuid(identity.iid().0);
        let id = bucket_id((&identity).into());
        let memory = MemoryBudget::new(1 << 20);
        for replaces in [false, true] {
            let mut rows = Vec::new();
            if replaces { rows.push((identity.clone(), ordered(1, 10))); }
            rows.push((collision.clone(), ordered(2, 20)));
            let bucket = canonical_bucket(id.clone(), rows, &memory)?;
            assert_eq!(bucket.floor((&collision).into()), Some(Timestamp(20)));
            assert_eq!(bucket.floor((&identity).into()), replaces.then_some(Timestamp(10)));
            let update = bucket.advance(&identity, ordered(3, 30))?;
            let positioned = update.arrow_document(&memory)?.expect("canonical bucket");
            let wanted = Value::Struct(update.document());
            let wanted_type = wanted.logical_type();
            let field = rtdb_columnar::to_field("put", &wanted_type)?;
            let baseline = rtdb_columnar::build_array(&wanted_type, std::slice::from_ref(&wanted))?;
            assert_eq!(positioned.to_data(), baseline.to_data());
            assert_eq!(rtdb_columnar::read_value(&field, positioned.as_ref(), 0)?, wanted);
            assert!(memory.used_bytes() > 0);
            drop(positioned);
            assert_eq!(memory.used_bytes(), 0);
        }
        Ok(())
    }

    #[test]
    fn a_canonical_bucket_requires_both_floor_coordinates_to_advance() -> Result<()> {
        let identity = EntityId::Binary(vec![1, 2, 3]);
        let id = bucket_id((&identity).into());
        let memory = MemoryBudget::new(1 << 20);
        for attempted in [ordered(1, 21), ordered(2, 20)] {
            let bucket = canonical_bucket(id.clone(), [(identity.clone(), ordered(1, 20))], &memory)?;
            assert!(matches!(bucket.advance(&identity, attempted), Err(error) if error == uncovered()));
        }
        Ok(())
    }

    #[test]
    fn borrowed_purge_floor_distinguishes_complete_ids_at_the_same_bucket() {
        let identity = EntityId::String("same-digest".into());
        let collision = EntityId::Uuid(identity.iid().0);
        let id = bucket_id((&identity).into());
        assert_eq!(id, bucket_id((&collision).into()));

        let ordered = |tx, time| TransactionKey { tx_id: TxId(tx), system_time: Timestamp(time) };
        let mut entries = BTreeMap::from([(collision.handle_bytes(), ordered(1, 10))]);
        let bucket = OwnedPurgeBucket { id: id.clone(), entries: Floors::Decoded(entries.clone()) };
        assert_eq!(bucket.floor((&identity).into()), None);
        assert_eq!(bucket.floor((&collision).into()), Some(Timestamp(10)));

        entries.insert(identity.handle_bytes(), ordered(2, 20));
        let bucket = OwnedPurgeBucket { id, entries: Floors::Decoded(entries) };
        assert_eq!(bucket.floor((&identity).into()), Some(Timestamp(20)));
        assert_eq!(bucket.floor((&collision).into()), Some(Timestamp(10)));
    }
}
