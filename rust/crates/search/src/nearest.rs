//! Exact cosine nearest neighbours over clear owned Root relations at a held cut.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{Array, Float32Array, Float64Array, RecordBatch, UInt64Array};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_query::QueryContext;
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, ListValue, NumericCell, NumericSpan, OwnedRelation, Result,
    TemporalBounds, Timestamp, Value,
};

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
    approximate: bool,
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
        Ok(Self { probe, norm, filters, k, approximate: false })
    }

    /// Opts into the approximate route for partitions with a published vector path (vector.allium: ApproximateRead).
    pub fn approximate(mut self) -> Self { self.approximate = true; self }
    pub fn exact(mut self) -> Self { self.approximate = false; self }

    pub fn dimensions(&self) -> usize { self.probe.len() }
    pub fn filter_count(&self) -> usize { self.filters.len() }
    pub fn k(&self) -> usize { self.k }

    fn admits<'a>(&self, cells: impl ExactSizeIterator<Item = Option<&'a [u8]>>) -> bool {
        cells.len() == self.filters.len()
            && cells.zip(&self.filters).all(|(cell, values)| cell.is_some_and(|cell| values.contains(cell)))
    }

    /// The cosine distance, or `None` for a zero-norm vector.
    fn distance(&self, vector: impl ExactSizeIterator<Item = f64>) -> Distance {
        if vector.len() != self.probe.len() {
            return Err(InvalidComponents);
        }
        let mut dot = 0.0;
        let mut norm = 0.0;
        for (&left, right) in self.probe.iter().zip(vector) {
            if !right.is_finite() { return Err(InvalidComponents); }
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

type Distance = std::result::Result<Option<f64>, InvalidComponents>;

#[derive(Clone, Copy, Debug)]
struct InvalidComponents;

impl From<InvalidComponents> for Anomaly {
    fn from(_: InvalidComponents) -> Self {
        Self::incorrect("rtdb.owned/nearest-dimension", "A stored vector does not have the probe's finite dimensions")
    }
}

enum ShadowVector {
    Staged { vector: Option<ScalarBuffer<f32>>, filters: Vec<Option<Buffer>> },
    World(Distance),
}

impl ShadowVector {
    fn distance(&self, query: &NearestQuery) -> Distance {
        match self {
            Self::Staged { vector: Some(vector), filters } if query.admits(filters.iter().map(Option::as_deref)) => {
                query.distance(vector.iter().copied().map(f64::from))
            }
            Self::Staged { .. } => Ok(None),
            Self::World(distance) => *distance,
        }
    }
}

/// Exact staged shadows. Each shadow masks the stored identity in its partition.
pub struct HeldVectorOverlay {
    rows: Vec<(usize, EntityId, ShadowVector)>,
    shadows: BTreeMap<usize, BTreeSet<EntityId>>,
    _retained: Arc<MemoryReservation>,
}

impl HeldVectorOverlay {
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { rows: Vec::new(), shadows: BTreeMap::new(), _retained: memory.reserve(4096)? })
    }

    /// The slices come from the admitted staged batch and retain its Arrow allocation charge.
    pub fn shadow_staged(
        &mut self, partition: usize, id: EntityId, vector: Option<ScalarBuffer<f32>>, filters: Vec<Option<Buffer>>,
    ) -> Result<()> {
        self.reserve_shadow(&id, vector.as_ref().map_or(0, |vector| vector.len()),
            filters.iter().flatten().map(|filter| filter.len()).sum())?;
        if self.rows.len() == MAX_NEAREST_SHADOWS { return Err(overlay_invalid()); }
        self.register_shadow(partition, &id)?;
        self.rows.push((partition, id, ShadowVector::Staged { vector, filters }));
        Ok(())
    }

    fn shadowed(&self, partition: usize) -> Option<&BTreeSet<EntityId>> {
        self.shadows.get(&partition)
    }

    pub fn contains_shadow(&self, partition: usize, id: &EntityId) -> bool {
        self.shadows.get(&partition).is_some_and(|ids| ids.contains(id))
    }

    /// Shadow an identity with one stored world row, or with its absence, for the query being ranked.
    pub fn shadow_row(
        &mut self, partition: usize, selected: &NearestPartition, id: EntityId, row: Option<&RecordBatch>, query: &NearestQuery,
    ) -> Result<()> {
        let row = row.filter(|row| row.num_rows() != 0);
        let vector = row.and_then(|row| ArrowCell::named(row, &selected.vector, 0))
            .map(read_vector).transpose()?.flatten();
        let filters = selected.filters.iter().map(|name| match row.and_then(|row| ArrowCell::named(row, name, 0)) {
            Some(cell) => rtdb_columnar::binary_cell(cell.field, cell.array, cell.row),
            None => Ok(None),
        }).collect::<Result<Vec<_>>>()?;
        self.reserve_shadow(&id, vector.as_ref().map_or(0, VectorCell::len),
            filters.iter().flatten().map(|filter| filter.len()).sum())?;
        self.register_shadow(partition, &id)?;
        // Retain the score or its refusal until overlay ranking so the source batch can be released here.
        let distance = match vector {
            Some(vector) if query.admits(filters.into_iter()) => vector.distance(query),
            _ => Ok(None),
        };
        self.rows.push((partition, id, ShadowVector::World(distance)));
        Ok(())
    }

    pub fn shadow_opened_projection(
        &mut self, partition: usize, id: EntityId, vector: impl ExactSizeIterator<Item = f32>,
        filters: &[Option<&[u8]>], query: &NearestQuery,
    ) -> Result<()> {
        self.reserve_shadow(&id, vector.len(), filters.iter().flatten().map(|key| key.len()).sum())?;
        self.register_shadow(partition, &id)?;
        let distance = if query.admits(filters.iter().copied()) {
            query.distance(vector.map(f64::from))
        } else {
            Ok(None)
        };
        self.rows.push((partition, id, ShadowVector::World(distance)));
        Ok(())
    }

    fn reserve_shadow(&mut self, id: &EntityId, components: usize, filter_bytes: usize) -> Result<()> {
        // These charges also govern admission for shadows backed by an already charged batch.
        let bytes = identity_charge(id)
            .saturating_add(components * 8)
            .saturating_add(filter_bytes)
            .saturating_add(256);
        let retained = Arc::get_mut(&mut self._retained).expect("exclusive overlay reservation");
        retained.grow_to(retained.bytes().saturating_add(bytes))
    }

    fn register_shadow(&mut self, partition: usize, id: &EntityId) -> Result<()> {
        if !self.shadows.entry(partition).or_default().insert(id.clone()) {
            return Err(overlay_invalid());
        }
        Ok(())
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

    /// Admits another window's hits and counts its candidates.
    fn absorb(&mut self, other: NearestWindow) -> Result<()> {
        self.total = self.total.saturating_add(other.total).saturating_sub(other.hits.len());
        for hit in &other.hits {
            self.admit(hit.partition, EntityIdRef::from(&hit.id), hit.distance)?;
        }
        Ok(())
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
/// An approximate query ranks only the shortlist of each partition with an approximate vector path; it reports whether any did.
pub async fn owned_nearest_batch(
    context: Arc<QueryContext>, owner: &str, partitions: &[NearestPartition], query: &NearestQuery,
    valid_at: Option<Timestamp>, overlay: Option<HeldVectorOverlay>,
) -> Result<(OwnedSearchBatch, bool)> {
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
    let mut approximated = false;
    for (ordinal, partition) in partitions.iter().enumerate() {
        let shadows = overlay.as_ref().and_then(|overlay| overlay.shadowed(ordinal));
        let mut columns = vec!["_id", partition.vector.as_str()];
        columns.extend(partition.filters.iter().map(String::as_str));
        let sealed = crate::rank_family::needs_rank_family(&context, owner, &partition.relation).await?;
        if sealed { columns.push("_system_from"); }
        if query.approximate && !sealed {
            let bounds = TemporalBounds { valid_time: Interval::at(valid)?, system_time: Interval::at(present)? };
            let cut = rtdb_vector::Cut { snapshot, storage: &database.storage, memory: &context.memory, cancellation: &context.cancellation, bounds };
            match approximate_partition(&context, owner, partition, query, cut, valid, &columns, shadows, ordinal, &mut window).await {
                Ok(true) => { approximated = true; continue; }
                Ok(false) => {}
                Err(error) if rtdb_vector::declined(&error) => {}
                Err(error) => return Err(error),
            }
        }
        let Some(cursor) = rtdb_query::open_owned_root_projection(
            Arc::clone(&context), owner, &partition.relation, valid, &columns,
        ).await? else { continue; };
        rank_rows(&context, cursor, partition, query, shadows, ordinal, &mut window, sealed.then_some((owner, valid))).await?;
    }
    if let Some(overlay) = overlay {
        for (ordinal, id, vector) in &overlay.rows {
            if *ordinal >= partitions.len() { return Err(overlay_invalid()); }
            let Some(distance) = vector.distance(query)? else { continue; };
            window.admit(*ordinal, EntityIdRef::from(id), distance)?;
        }
    }
    context.check_cancelled()?;
    Ok((window.finish()?, approximated))
}

/// Ranks one partition's shortlist from its approximate vector path, widening it until `k` candidates pass the filters
/// or every row has been considered. False where the partition has no such path or the shortlist exceeds its budget,
/// so the caller ranks the partition exactly; nothing reaches the window until the shortlist settles.
#[allow(clippy::too_many_arguments)]
async fn approximate_partition(
    context: &Arc<QueryContext>, owner: &str, partition: &NearestPartition, query: &NearestQuery, cut: rtdb_vector::Cut<'_>,
    valid: Timestamp, columns: &[&str], shadows: Option<&BTreeSet<EntityId>>, ordinal: usize, window: &mut NearestWindow,
) -> Result<bool> {
    let table = partition.relation.table(owner);
    let Some(definition) = rtdb_vector::owned_definition(Arc::clone(context), &table, &partition.vector).await? else { return Ok(false) };
    let Some(probe) = rtdb_vector::unit_probe(&query.probe) else { return Ok(false) };
    let Some(mut shortlist) = rtdb_vector::Shortlist::open(cut, &definition, probe, query.k, None, None, rtdb_vector::Budget::default()).await? else {
        return Ok(false);
    };
    loop {
        let Some(rtdb_vector::Round { candidates, complete }) = shortlist.round(cut, None).await? else { return Ok(false) };
        let mut settled = NearestWindow::new(&context.memory, query.k)?;
        if !candidates.is_empty() && let Some(cursor) = rtdb_query::open_owned_root_projection_of(
            Arc::clone(context), owner, &partition.relation, valid, columns, candidates,
        ).await? {
            rank_rows(context, cursor, partition, query, shadows, ordinal, &mut settled, None).await?;
        }
        if settled.total >= query.k || complete {
            window.absorb(settled)?;
            return Ok(true);
        }
        if !shortlist.widen() { return Ok(false); }
    }
}

/// Admits the projected rows that pass the filters into the window by exact distance.
async fn rank_rows(
    context: &Arc<QueryContext>, mut cursor: rtdb_query::QueryCursor, partition: &NearestPartition, query: &NearestQuery,
    shadows: Option<&BTreeSet<EntityId>>, ordinal: usize, window: &mut NearestWindow,
    sealed: Option<(&str, Timestamp)>,
) -> Result<()> {
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
            let read_id = || rtdb_columnar::read_entity_id(id_field, batch.column(id_column).as_ref(), row);
            let is_shadowed = |id| shadows.is_some_and(|shadows| shadows.iter().any(|shadow| EntityIdRef::from(shadow) == id));
            let admitted_id = if let Some((owner, valid)) = sealed {
                let protected = crate::rank_family::RootProjectionId::parse(read_id()?)?;
                let id = protected.canonical();
                if is_shadowed(id) { continue; }
                protected.require(context, owner, &partition.relation, valid, batch, row).await?;
                Some(id)
            } else { None };
            let mut cells = [None; MAX_NEAREST_FILTERS];
            for (cell, &(column, field)) in cells.iter_mut().zip(&filters) {
                *cell = rtdb_columnar::binary_cell(field, batch.column(column).as_ref(), row)?;
            }
            if !query.admits(cells[..filters.len()].iter().copied()) { continue; }
            let vectors = batch.column(vector_column).as_ref();
            // Keep the same admission and refusal point for borrowed and decoded cells.
            let bytes = rtdb_columnar::estimate_read_value_bytes(vector_field, vectors, row)?;
            let _cell = context.memory.reserve(bytes.saturating_mul(2))?;
            let Some(vector) = read_vector(ArrowCell { field: vector_field, array: vectors, row })? else { continue; };
            let Some(distance) = vector.distance(query)? else { continue; };
            let id = match admitted_id {
                Some(id) => id,
                None => {
                    let id = read_id()?;
                    if is_shadowed(id) { continue; }
                    id
                }
            };
            window.admit(ordinal, id, distance)?;
        }
    }
    Ok(())
}

enum VectorCell<'a> {
    F32(&'a [f32]),
    F64(&'a [f64]),
    Numeric(NumericSpan<'a>),
    Decoded(Vec<f64>),
}

impl VectorCell<'_> {
    fn len(&self) -> usize {
        match self {
            Self::F32(values) => values.len(),
            Self::F64(values) => values.len(),
            Self::Numeric(values) => values.len(),
            Self::Decoded(values) => values.len(),
        }
    }

    fn distance(self, query: &NearestQuery) -> Distance {
        match self {
            Self::F32(values) => query.distance(values.iter().copied().map(f64::from)),
            Self::F64(values) => query.distance(values.iter().copied()),
            Self::Numeric(values) => query.distance(values.iter().map(|value| match value {
                NumericCell::F32(value) => f64::from(value),
                NumericCell::F64(value) => value,
                NumericCell::I8(value) => f64::from(value),
                NumericCell::I16(value) => f64::from(value),
                NumericCell::I32(value) => f64::from(value),
                NumericCell::I64(value) => value as f64,
                _ => unreachable!("admitted vector component"),
            })),
            Self::Decoded(values) => query.distance(values.into_iter()),
        }
    }
}

fn plain_vector_list(cell: ArrowCell<'_>) -> Option<ArrowCell<'_>> {
    let cell = match cell.field.data_type() {
        DataType::Union(..) | DataType::RunEndEncoded(..) => {
            let (selected, logical) = cell.logical_position(None).ok()?;
            if rtdb_columnar::from_field(selected.field).ok()? != logical { return None; }
            selected
        }
        _ => cell,
    };
    if cell.field.extension_type_name().is_some() { return None; }
    match cell.field.data_type() {
        DataType::List(_) => {}
        DataType::FixedSizeList(_, width) => {
            // `sequence_range` gets the fixed-size list offset through Arrow's i32 accessor.
            i32::try_from(cell.row.checked_mul(usize::try_from(*width).ok()?)?).ok()?;
        }
        _ => return None,
    }
    Some(cell)
}

fn packed_vector_element(field: &Field) -> bool {
    let signed = |ty: &DataType| matches!(ty, DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64);
    ListValue::supports_numeric_arrow(field) && match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, child)| {
            let ty = child.data_type();
            signed(ty) || matches!(ty, DataType::Float32 | DataType::Float64 | DataType::Null)
        }),
        ty => signed(ty),
    }
}

fn read_vector(cell: ArrowCell<'_>) -> Result<Option<VectorCell<'_>>> {
    let ArrowCell { field, array, row } = cell;
    if array.is_null(row) { return Ok(None); }
    if let Some(list) = plain_vector_list(cell)
        && let Some((element, values, range)) = list.sequence_range()
    {
        let has_primitive_null = || {
            values.nulls().is_some_and(|nulls| range.clone().any(|index| nulls.is_null(index)))
        };
        if element.metadata().is_empty() && matches!(element.data_type(), DataType::Float32 | DataType::Float64)
            && let Some(vector) = values.as_any().downcast_ref::<Float32Array>()
                .map(|array| VectorCell::F32(&array.values()[range.clone()]))
                .or_else(|| values.as_any().downcast_ref::<Float64Array>()
                    .map(|array| VectorCell::F64(&array.values()[range.clone()])))
        {
            if has_primitive_null() { return Err(not_a_vector()); }
            return Ok(Some(vector));
        }
        if packed_vector_element(element)
            && rtdb_columnar::decimal_field_parameters(list.field).is_ok()
            && let Some(span) = list.numeric_list_span_range(0..range.len())
        {
            let has_null = if matches!(element.data_type(), DataType::Union(..)) {
                span.iter().any(|value| matches!(value, NumericCell::Null))
            } else {
                has_primitive_null()
            };
            if has_null { return Err(not_a_vector()); }
            return Ok(Some(VectorCell::Numeric(span)));
        }
    }
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
    Ok(Some(VectorCell::Decoded(vector)))
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
    use arrow_array::{
        BinaryArray, FixedSizeListArray, Int64Array, ListArray, NullArray, UnionArray,
        types::{Float32Type, UInt64Type},
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::UnionFields;

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
        assert_eq!(nearest.distance([3.0, 0.0].into_iter()).unwrap(), Some(0.0));
        assert_eq!(nearest.distance([0.0, 2.0].into_iter()).unwrap(), Some(1.0));
        assert_eq!(nearest.distance([-1.0, 0.0].into_iter()).unwrap(), Some(2.0));
        assert_eq!(nearest.distance([0.0, 0.0].into_iter()).unwrap(), None);
        assert!(nearest.distance([1.0].into_iter()).is_err());
        assert!(NearestQuery::new(vec![0.0, 0.0], Vec::new(), 1).is_err());
        assert!(NearestQuery::new(vec![f32::NAN], Vec::new(), 1).is_err());
        assert!(NearestQuery::new(vec![1.0], Vec::new(), MAX_NEAREST_K + 1).is_err());
    }

    #[test]
    fn nearest_scores_sliced_float_lists_with_the_original_component_order() {
        let floats = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some([Some(9.0), Some(7.0)]),
            Some([Some(-0.3), Some(0.125)]),
        ]).slice(1, 1);
        let doubles = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float64, false)), 2,
            Arc::new(Float64Array::from(vec![9.0, 7.0, -0.3, 0.125])), None,
        ).slice(1, 1);
        let nearest = NearestQuery::new(vec![0.2, -0.9], Vec::new(), 1).unwrap();
        for (array, expected) in [
            (&floats as &dyn Array, [f64::from(-0.3f32), 0.125]),
            (&doubles as &dyn Array, [-0.3, 0.125]),
        ] {
            let field = Field::new("vector", array.data_type().clone(), false);
            let cell = read_vector(ArrowCell { field: &field, array, row: 0 }).unwrap().unwrap();
            assert!(matches!(&cell, VectorCell::F32(_) | VectorCell::F64(_)));
            let mut dot = 0.0;
            let mut norm = 0.0;
            for (&probe, component) in nearest.probe.iter().zip(expected) {
                dot += f64::from(probe) * component;
                norm += component * component;
            }
            let expected = 1.0 - dot / (nearest.norm * norm.sqrt());
            assert_eq!(cell.distance(&nearest).unwrap().unwrap().to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn nearest_refuses_a_null_component_before_dimension_and_finiteness_checks() {
        let array = ListArray::from_iter_primitive::<Float32Type, _, _>([Some([Some(f32::NAN), None, Some(1.0)])]);
        let field = Field::new("vector", array.data_type().clone(), false);
        let cell = ArrowCell { field: &field, array: &array, row: 0 };
        assert_eq!(read_vector(cell).err(), Some(not_a_vector()));
    }

    #[test]
    fn nearest_borrows_sliced_signed_and_mixed_lists_in_component_order() {
        let large = (1_i64 << 53) + (1 << 30) + 1;
        let integers = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, false)), 3,
            Arc::new(Int64Array::from(vec![9, 7, 6, large, -large, 125])), None,
        ).slice(1, 1);
        let fields = UnionFields::try_new([7, 4, 2, 5], [
            Field::new("i64", DataType::Int64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("null", DataType::Null, true),
        ]).unwrap();
        let mixed = UnionArray::try_new(
            fields, vec![4, 7, 4, 2, 4, 5, 4, 7, 2].into(), Some(vec![0, 0, 1, 0, 2, 0, 2, 0, 0].into()),
            vec![
                Arc::new(Int64Array::from(vec![large])),
                Arc::new(Float32Array::from(vec![9.0, -0.3, f32::NAN])),
                Arc::new(Float64Array::from(vec![0.125])),
                Arc::new(NullArray::new(1)),
            ],
        ).unwrap();
        let mixed = ListArray::try_new(
            Arc::new(Field::new("item", mixed.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 4, 6, 9].into()), Arc::new(mixed), None,
        ).unwrap().slice(1, 3);
        let nearest = NearestQuery::new(vec![0.2, -0.9, 0.5], Vec::new(), 1).unwrap();
        for (array, expected) in [
            (&integers as &dyn Array, [large as f64, (-large) as f64, 125.0]),
            (&mixed as &dyn Array, [large as f64, f64::from(-0.3_f32), 0.125]),
        ] {
            let field = Field::new("vector", array.data_type().clone(), false);
            let cell = read_vector(ArrowCell { field: &field, array, row: 0 }).unwrap().unwrap();
            assert!(matches!(&cell, VectorCell::Numeric(_)));
            let expected = nearest.distance(expected.into_iter()).unwrap().unwrap();
            assert_eq!(cell.distance(&nearest).unwrap().unwrap().to_bits(), expected.to_bits());
        }

        let field = Field::new("vector", mixed.data_type().clone(), false);
        let malformed = ArrowCell { field: &field, array: &mixed, row: 1 };
        assert_eq!(read_vector(malformed).err(), Some(not_a_vector()));
        let nonfinite = read_vector(ArrowCell { row: 2, ..malformed }).unwrap().unwrap();
        assert!(nonfinite.distance(&nearest).is_err());
    }

    #[test]
    fn nearest_refuses_unsigned_components_even_when_the_numeric_span_opens() {
        let array = ListArray::from_iter_primitive::<UInt64Type, _, _>([Some([Some(u64::MAX), Some(1)])]);
        let field = Field::new("vector", array.data_type().clone(), false);
        let cell = ArrowCell { field: &field, array: &array, row: 0 };
        assert!(cell.numeric_list_span().is_some());
        assert_eq!(read_vector(cell).err(), Some(not_a_vector()));
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
        overlay.shadow_staged(0, EntityId::String("a".into()), Some(vec![1.0].into()), Vec::new()).unwrap();
        overlay.shadow_staged(1, EntityId::String("a".into()), None, Vec::new()).unwrap();
        assert!(overlay.shadow_staged(0, EntityId::String("a".into()), None, Vec::new()).is_err());
    }

    #[test]
    fn nearest_staged_spans_keep_the_admitted_arrow_charge_until_ranking() {
        use arrow_array::ArrayRef;

        let memory = MemoryBudget::new(1 << 20);
        let vector = Arc::new(Float32Array::from(vec![-9.0, 3.0, 0.0])) as ArrayRef;
        let filter = Arc::new(BinaryArray::from(vec![b"no".as_slice(), b"yes".as_slice(), b"no".as_slice()])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("vector", vector), ("filter", filter)]).unwrap();
        let source = OwnedBatch::new(batch, &memory).unwrap();
        let charge = Arc::downgrade(source.reservation());
        let vector = source.batch().column(0).as_any().downcast_ref::<Float32Array>().unwrap();
        let filter = source.batch().column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut overlay = HeldVectorOverlay::new(&memory).unwrap();
        overlay.shadow_staged(0, EntityId::Integer(1), Some(vector.values().slice(1, 2)),
            vec![Some(filter.values().slice_with_length(2, 3))]).unwrap();
        drop(source);

        let nearest = query(vec![BTreeSet::from([b"yes".to_vec()])], 1);
        assert!(charge.upgrade().is_some());
        assert_eq!(overlay.rows[0].2.distance(&nearest).unwrap(), Some(0.0));
        drop(overlay);
        assert!(charge.upgrade().is_none());
    }

    #[test]
    fn nearest_world_shadow_defers_numeric_refusal_until_ranking_after_filters() {
        let memory = MemoryBudget::new(1 << 20);
        let mut overlay = HeldVectorOverlay::new(&memory).unwrap();
        let nearest = query(vec![BTreeSet::from([b"yes".to_vec()])], 1);
        let selected = NearestPartition {
            relation: OwnedRelation::new("facts").unwrap(), vector: "vector".into(), filters: vec!["label".into()],
        };
        let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>([
            Some([Some(f32::NAN), Some(2.0)]), Some([Some(f32::NAN), Some(2.0)]),
        ]);
        let labels = BinaryArray::from(vec![b"no".as_slice(), b"yes".as_slice()]);
        let batch = RecordBatch::try_from_iter([
            ("vector", Arc::new(vectors) as arrow_array::ArrayRef),
            ("label", Arc::new(labels) as arrow_array::ArrayRef),
        ]).unwrap();
        let filtered = EntityId::String("filtered".into());
        let refused = EntityId::String("refused".into());
        overlay.shadow_row(0, &selected, filtered.clone(), Some(&batch.slice(0, 1)), &nearest).unwrap();
        overlay.shadow_row(0, &selected, refused.clone(), Some(&batch.slice(1, 1)), &nearest).unwrap();
        drop(batch);

        assert!(overlay.contains_shadow(0, &filtered));
        assert!(overlay.contains_shadow(0, &refused));
        assert_eq!(overlay.rows[0].2.distance(&nearest).unwrap(), None);
        let error = Anomaly::from(overlay.rows[1].2.distance(&nearest).unwrap_err());
        assert_eq!(error.code, "rtdb.owned/nearest-dimension");
    }
}
