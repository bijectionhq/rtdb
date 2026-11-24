//! Exact cosine nearest neighbours over clear owned Root relations at a held cut.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{Float64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_query::QueryContext;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, Timestamp, Value};

use crate::OwnedSearchBatch;

pub const MAX_NEAREST_K: usize = 256;
pub const MAX_NEAREST_DIMENSIONS: usize = 4096;
pub const MAX_NEAREST_FILTERS: usize = 8;
pub const MAX_NEAREST_FILTER_VALUES: usize = 64;
pub const MAX_NEAREST_FILTER_BYTES: usize = 1024;
pub const MAX_NEAREST_SHADOWS: usize = 64;
const MAX_NEAREST_CANDIDATES: usize = 100_000;

/// One selected partition: its relation, stored vector column and filter columns in request order.
#[derive(Clone, Debug)]
pub struct NearestPartition {
    pub relation: OwnedRelation,
    pub vector: String,
    pub filters: Vec<String>,
}

/// A validated probe, its filters and the size of the ranked window.
#[derive(Clone, Debug)]
pub struct NearestQuery {
    probe: Vec<f32>,
    norm: f64,
    filters: Vec<BTreeSet<Vec<u8>>>,
    k: usize,
}

impl NearestQuery {
    pub fn new(probe: Vec<f32>, filters: Vec<BTreeSet<Vec<u8>>>, k: usize) -> Result<Self> {
        if !(1..=MAX_NEAREST_DIMENSIONS).contains(&probe.len()) || !(1..=MAX_NEAREST_K).contains(&k)
            || filters.len() > MAX_NEAREST_FILTERS
            || filters.iter().any(|values| !(1..=MAX_NEAREST_FILTER_VALUES).contains(&values.len())
                || values.iter().any(|value| value.len() > MAX_NEAREST_FILTER_BYTES))
            || probe.iter().any(|value| !value.is_finite())
        {
            return Err(Anomaly::incorrect("rtdb.owned/nearest-limit", "Owned nearest exceeds its probe, filter or result bound"));
        }
        let norm = probe.iter().map(|&value| f64::from(value) * f64::from(value)).sum::<f64>().sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return Err(Anomaly::incorrect("rtdb.owned/nearest-probe", "Owned nearest requires a nonzero probe"));
        }
        Ok(Self { probe, norm, filters, k })
    }

    pub fn dimensions(&self) -> usize { self.probe.len() }
    pub fn filter_count(&self) -> usize { self.filters.len() }

    fn admits<'a>(&self, cells: impl ExactSizeIterator<Item = Option<&'a [u8]>>) -> bool {
        cells.len() == self.filters.len()
            && cells.zip(&self.filters).all(|(cell, values)| cell.is_some_and(|cell| values.contains(cell)))
    }

    /// The cosine distance, or `None` for a zero-norm vector.
    fn distance(&self, vector: &[f64]) -> Result<Option<f64>> {
        if vector.len() != self.probe.len() || vector.iter().any(|value| !value.is_finite()) {
            return Err(Anomaly::incorrect("rtdb.owned/nearest-dimension", "A stored vector does not have the probe's finite dimensions"));
        }
        let mut dot = 0.0;
        let mut norm = 0.0;
        for (&left, &right) in self.probe.iter().zip(vector) {
            dot += f64::from(left) * right;
            norm += right * right;
        }
        let norm = norm.sqrt();
        if norm == 0.0 { return Ok(None); }
        let distance = 1.0 - dot / (self.norm * norm);
        if !distance.is_finite() { return Ok(None); }
        Ok(Some(distance))
    }
}

/// Exact staged shadows. Each shadow masks the stored identity in its partition.
pub struct HeldVectorOverlay {
    rows: Vec<(usize, EntityId, Option<Vec<f64>>, Vec<Option<Vec<u8>>>)>,
    shadows: BTreeMap<usize, BTreeSet<EntityId>>,
    _retained: Arc<MemoryReservation>,
}

impl HeldVectorOverlay {
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { rows: Vec::new(), shadows: BTreeMap::new(), _retained: memory.reserve(4096)? })
    }

    pub fn shadow(&mut self, partition: usize, id: EntityId, vector: Option<Vec<f32>>, filters: Vec<Option<Vec<u8>>>) -> Result<()> {
        let bytes = identity_charge(&id)
            .saturating_add(vector.as_ref().map_or(0, |vector| vector.len() * 8))
            .saturating_add(filters.iter().flatten().map(Vec::len).sum::<usize>())
            .saturating_add(256);
        let retained = Arc::get_mut(&mut self._retained).expect("exclusive overlay reservation");
        retained.grow_to(retained.bytes().saturating_add(bytes))?;
        if self.rows.len() == MAX_NEAREST_SHADOWS || !self.shadows.entry(partition).or_default().insert(id.clone()) {
            return Err(overlay_invalid());
        }
        let vector = vector.map(|vector| vector.into_iter().map(f64::from).collect());
        self.rows.push((partition, id, vector, filters));
        Ok(())
    }

    fn shadowed(&self, partition: usize) -> Option<&BTreeSet<EntityId>> {
        self.shadows.get(&partition)
    }
}

struct NearestHit {
    partition: usize,
    id: EntityId,
    distance: f64,
}

fn nearest_order(left: &NearestHit, partition: usize, id: EntityIdRef<'_>, distance: f64) -> std::cmp::Ordering {
    left.distance.total_cmp(&distance)
        .then_with(|| EntityIdRef::from(&left.id).cmp(&id))
        .then_with(|| left.partition.cmp(&partition))
}

/// The first `k` candidates in ascending distance, then typed identity, then partition.
struct NearestWindow {
    hits: Vec<NearestHit>,
    memory: Arc<MemoryReservation>,
    k: usize,
    total: usize,
    bytes: usize,
}

impl NearestWindow {
    fn new(memory: &Arc<MemoryBudget>, k: usize) -> Result<Self> {
        let memory = memory.reserve(4096 + (k + 1) * size_of::<NearestHit>())?;
        Ok(Self { hits: Vec::with_capacity(k + 1), memory, k, total: 0, bytes: 4096 })
    }

    fn admit(&mut self, partition: usize, id: EntityIdRef<'_>, distance: f64) -> Result<()> {
        self.total += 1;
        if self.total > MAX_NEAREST_CANDIDATES {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/nearest-scan-limit", "Owned nearest exceeded its candidate budget"));
        }
        let at = self.hits.partition_point(|hit| nearest_order(hit, partition, id, distance).is_lt());
        if at >= self.k { return Ok(()); }
        if self.hits.len() == self.k {
            let evicted = self.hits.pop().expect("A full nearest window has a last hit");
            self.bytes -= identity_charge(&evicted.id).saturating_add(128);
        }
        let id = id.into_owned();
        self.bytes = self.bytes.saturating_add(identity_charge(&id).saturating_add(128));
        self.hits.insert(at, NearestHit { partition, id, distance });
        let reservation = Arc::get_mut(&mut self.memory).expect("nearest owns its reservation");
        reservation.grow_to(self.bytes.max(reservation.bytes()))
    }

    fn finish(self) -> Result<OwnedSearchBatch> {
        let bound = self.hits.iter().try_fold(4096usize, |size, hit| {
            size.checked_add(identity_charge(&hit.id).saturating_mul(4).saturating_add(128))
                .ok_or_else(|| Anomaly::fault("rtdb.search/nearest-output", "Nearest output size overflow"))
        })?;
        let reservation = self.memory.budget().reserve(bound)?;
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type())?,
            Field::new("distance", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ]));
        let ids = rtdb_columnar::entity_id_array(self.hits.iter().map(|hit| EntityIdRef::from(&hit.id)))?;
        let distance = Arc::new(Float64Array::from_iter_values(self.hits.iter().map(|hit| hit.distance)));
        let partition = Arc::new(UInt64Array::from_iter_values(self.hits.iter().map(|hit| hit.partition as u64)));
        let output = RecordBatch::try_new(schema, vec![ids, distance, partition])
            .map_err(|error| Anomaly::fault("rtdb.search/nearest-output", error.to_string()))?;
        Ok(OwnedSearchBatch { hits: OwnedBatch::with_reservation(output, reservation)?, total: self.total })
    }
}

/// Rank the visible vectors of the selected partitions and the staged overlay by exact cosine distance.
pub async fn owned_nearest_batch(
    context: Arc<QueryContext>, owner: &str, partitions: &[NearestPartition], query: &NearestQuery,
    valid_at: Option<Timestamp>, overlay: Option<HeldVectorOverlay>,
) -> Result<OwnedSearchBatch> {
    let distinct: BTreeSet<_> = partitions.iter().map(|partition| &partition.relation).collect();
    if !(1..=32).contains(&partitions.len()) || distinct.len() != partitions.len()
        || partitions.iter().any(|partition| partition.filters.len() != query.filter_count())
    {
        return Err(Anomaly::incorrect("rtdb.owned/nearest-relations", "An owned nearest relation must be selected once with every filter column"));
    }
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Err(uncovered()); };
    let present = context.current_time.as_micros()?;
    if snapshot.system_time_cap() != Some(present) { return Err(uncovered()); }
    let valid = valid_at.unwrap_or(present);
    let mut window = NearestWindow::new(&context.memory, query.k)?;
    for (ordinal, partition) in partitions.iter().enumerate() {
        let shadows = overlay.as_ref().and_then(|overlay| overlay.shadowed(ordinal));
        let mut columns = vec!["_id", partition.vector.as_str()];
        columns.extend(partition.filters.iter().map(String::as_str));
        let Some(mut cursor) = rtdb_query::open_owned_root_projection(
            Arc::clone(&context), owner, &partition.relation, valid, &columns,
        ).await? else { continue; };
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let (id_column, id_field) = schema.column_with_name("_id").ok_or_else(uncovered)?;
            let (vector_column, vector_field) = schema.column_with_name(&partition.vector).ok_or_else(uncovered)?;
            let filters = partition.filters.iter()
                .map(|name| schema.column_with_name(name).ok_or_else(uncovered))
                .collect::<Result<Vec<_>>>()?;
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let mut cells = Vec::with_capacity(filters.len());
                for &(column, field) in &filters {
                    cells.push(rtdb_columnar::binary_cell(field, batch.column(column).as_ref(), row)?);
                }
                if !query.admits(cells.into_iter()) { continue; }
                let vectors = batch.column(vector_column).as_ref();
                let bytes = rtdb_columnar::estimate_read_value_bytes(vector_field, vectors, row)?;
                let _cell = context.memory.reserve(bytes.saturating_mul(2))?;
                let Some(vector) = read_vector(vector_field, vectors, row)? else { continue; };
                let Some(distance) = query.distance(&vector)? else { continue; };
                let ids = batch.column(id_column).as_ref();
                let id = rtdb_columnar::read_entity_id(id_field, ids, row)?;
                if shadows.is_some_and(|shadows| shadows.iter().any(|shadow| EntityIdRef::from(shadow) == id)) { continue; }
                window.admit(ordinal, id, distance)?;
            }
        }
    }
    if let Some(overlay) = overlay {
        for (ordinal, id, vector, filters) in &overlay.rows {
            if *ordinal >= partitions.len() { return Err(overlay_invalid()); }
            let Some(vector) = vector else { continue; };
            if !query.admits(filters.iter().map(Option::as_deref)) { continue; }
            let Some(distance) = query.distance(vector)? else { continue; };
            window.admit(*ordinal, EntityIdRef::from(id), distance)?;
        }
    }
    context.check_cancelled()?;
    window.finish()
}

/// Read one stored vector cell as f64 components. A missing or null cell has no vector.
fn read_vector(field: &Field, array: &dyn arrow_array::Array, row: usize) -> Result<Option<Vec<f64>>> {
    if array.is_null(row) { return Ok(None); }
    let value = rtdb_columnar::read_value(field, array, row)?;
    let list = match &value {
        Value::Null | Value::Undefined => return Ok(None),
        Value::List(list) | Value::FixedSizeList(list) => list.view(),
        _ => return Err(not_a_vector()),
    };
    let mut vector = Vec::with_capacity(list.len());
    for index in 0..list.len() {
        let component = match list.float_component(index) {
            Some(component) => component,
            None => match list.get(index).as_deref() {
                Some(Value::F32(value)) => f64::from(*value),
                Some(Value::F64(value)) => *value,
                Some(Value::I8(value)) => f64::from(*value),
                Some(Value::I16(value)) => f64::from(*value),
                Some(Value::I32(value)) => f64::from(*value),
                Some(Value::I64(value)) => *value as f64,
                _ => return Err(not_a_vector()),
            },
        };
        vector.push(component);
    }
    Ok(Some(vector))
}

fn identity_charge(id: &EntityId) -> usize {
    size_of::<Value>() + EntityIdRef::from(id).owned_payload_bytes()
}

fn not_a_vector() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/nearest-dimension", "A stored vector cell is not a list of numbers")
}

fn overlay_invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/nearest-overlay", "Invalid staged nearest overlay")
}

fn uncovered() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/nearest-uncovered", "The held nearest read cannot cover its cut")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(filters: Vec<BTreeSet<Vec<u8>>>, k: usize) -> NearestQuery {
        NearestQuery::new(vec![1.0, 0.0], filters, k).unwrap()
    }

    #[test]
    fn nearest_window_orders_by_distance_then_identity_then_partition() {
        let memory = MemoryBudget::new(1 << 20);
        let mut window = NearestWindow::new(&memory, 3).unwrap();
        let ids = [EntityId::Integer(2), EntityId::Integer(1), EntityId::Integer(1), EntityId::Integer(0)];
        for ((partition, id), distance) in [1, 1, 0, 0].into_iter().zip(&ids).zip([0.5, 0.5, 0.5, 0.9]) {
            window.admit(partition, EntityIdRef::from(id), distance).unwrap();
        }
        let order = window.hits.iter().map(|hit| (hit.partition, hit.id.clone())).collect::<Vec<_>>();
        assert_eq!(order, vec![(0, EntityId::Integer(1)), (1, EntityId::Integer(1)), (1, EntityId::Integer(2))]);
        assert_eq!(window.total, 4);
    }

    #[test]
    fn nearest_query_measures_cosine_distance_and_skips_zero_vectors() {
        let nearest = query(Vec::new(), 1);
        assert_eq!(nearest.distance(&[3.0, 0.0]).unwrap(), Some(0.0));
        assert_eq!(nearest.distance(&[0.0, 2.0]).unwrap(), Some(1.0));
        assert_eq!(nearest.distance(&[-1.0, 0.0]).unwrap(), Some(2.0));
        assert_eq!(nearest.distance(&[0.0, 0.0]).unwrap(), None);
        assert_eq!(nearest.distance(&[1.0]).unwrap_err().code, "rtdb.owned/nearest-dimension");
        assert!(NearestQuery::new(vec![0.0, 0.0], Vec::new(), 1).is_err());
        assert!(NearestQuery::new(vec![f32::NAN], Vec::new(), 1).is_err());
        assert!(NearestQuery::new(vec![1.0], Vec::new(), MAX_NEAREST_K + 1).is_err());
    }

    #[test]
    fn nearest_filters_require_every_column_to_match_one_value() {
        let nearest = query(vec![BTreeSet::from([b"a".to_vec(), b"b".to_vec()]), BTreeSet::from([b"x".to_vec()])], 1);
        assert!(nearest.admits([Some(&b"b"[..]), Some(&b"x"[..])].into_iter()));
        assert!(!nearest.admits([Some(&b"c"[..]), Some(&b"x"[..])].into_iter()));
        assert!(!nearest.admits([Some(&b"a"[..]), None].into_iter()));
    }

    #[test]
    fn nearest_overlay_refuses_a_repeated_shadow() {
        let memory = MemoryBudget::new(1 << 20);
        let mut overlay = HeldVectorOverlay::new(&memory).unwrap();
        overlay.shadow(0, EntityId::String("a".into()), Some(vec![1.0]), Vec::new()).unwrap();
        overlay.shadow(1, EntityId::String("a".into()), None, Vec::new()).unwrap();
        assert!(overlay.shadow(0, EntityId::String("a".into()), None, Vec::new()).is_err());
    }
}
