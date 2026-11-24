//! Exact cosine nearest neighbours over clear owned Root relations at a held cut.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{Array, Float32Array, Float64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, UnionMode};
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_plan::Catalog;
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
        self.finish_distance(dot, norm)
    }

    fn finish_distance(&self, dot: f64, norm: f64) -> Distance {
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

/// Each shadow masks the stored identity in its partition, even when it has no score.
pub struct HeldVectorOverlay {
    rows: Vec<(usize, EntityId, Distance)>,
    shadows: BTreeMap<usize, BTreeSet<EntityId>>,
    _retained: Arc<MemoryReservation>,
}

impl HeldVectorOverlay {
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { rows: Vec::new(), shadows: BTreeMap::new(), _retained: memory.reserve(4096)? })
    }

    /// Score admitted staged cells while borrowed. A component refusal surfaces during ranking.
    pub fn shadow_staged<'a>(
        &mut self, partition: usize, id: EntityIdRef<'_>, vector: Option<&[f32]>,
        filters: impl ExactSizeIterator<Item = Option<&'a [u8]>>, query: &NearestQuery,
    ) -> Result<()> {
        self.reserve_shadow(id, id.owned_payload_bytes())?;
        if self.rows.len() == MAX_NEAREST_SHADOWS { return Err(overlay_invalid()); }
        let id = id.into_owned();
        self.register_shadow(partition, &id)?;
        let distance = match vector {
            Some(vector) if query.admits(filters) => query.distance(vector.iter().copied().map(f64::from)),
            _ => Ok(None),
        };
        self.rows.push((partition, id, distance));
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
            .map(|cell| read_vector(cell, query)).transpose()?.flatten();
        let filters = selected.filters.iter().map(|name| match row.and_then(|row| ArrowCell::named(row, name, 0)) {
            Some(cell) => rtdb_columnar::binary_cell(cell.field, cell.array, cell.row),
            None => Ok(None),
        }).collect::<Result<Vec<_>>>()?;
        let payload_bytes = (vector.as_ref().map_or(0, VectorCell::len) * 8)
            .saturating_add(filters.iter().flatten().map(|filter| filter.len()).sum());
        self.reserve_shadow(EntityIdRef::from(&id), payload_bytes)?;
        self.register_shadow(partition, &id)?;
        let distance = match vector {
            Some(vector) if query.admits(filters.into_iter()) => vector.distance(query),
            _ => Ok(None),
        };
        self.rows.push((partition, id, distance));
        Ok(())
    }

    pub fn shadow_opened_projection(
        &mut self, partition: usize, id: EntityId, vector: impl ExactSizeIterator<Item = f32>,
        filters: &[Option<&[u8]>], query: &NearestQuery,
    ) -> Result<()> {
        let payload_bytes = (vector.len() * 8)
            .saturating_add(filters.iter().flatten().map(|key| key.len()).sum());
        self.reserve_shadow(EntityIdRef::from(&id), payload_bytes)?;
        self.register_shadow(partition, &id)?;
        let distance = if query.admits(filters.iter().copied()) {
            query.distance(vector.map(f64::from))
        } else {
            Ok(None)
        };
        self.rows.push((partition, id, distance));
        Ok(())
    }

    fn reserve_shadow(&mut self, id: EntityIdRef<'_>, additional_bytes: usize) -> Result<()> {
        let bytes = identity_charge(id)
            .saturating_add(additional_bytes)
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
        let total = self.total_after(1)?;
        let at = self.position(partition, id, distance);
        if at < self.k {
            let full = self.hits.len() == self.k;
            let removed = if full {
                let last = self.hits.last().expect("a full window has a last hit");
                identity_charge(EntityIdRef::from(&last.id)) + 128
            } else { 0 };
            let bytes = (self.bytes - removed)
                .saturating_add(identity_charge(id) + 128);
            self.reserve_hits(bytes)?;
            if full { drop(self.hits.pop()); }
            self.hits.insert(at, NearestHit { partition, id: id.into_owned(), distance });
            self.bytes = bytes;
        }
        self.total = total;
        Ok(())
    }

    fn total_after(&self, added: usize) -> Result<usize> {
        let total = self.total.saturating_add(added);
        if total > MAX_NEAREST_CANDIDATES {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/nearest-scan-limit", "Owned nearest exceeded its candidate budget"));
        }
        Ok(total)
    }

    fn position(&self, partition: usize, id: EntityIdRef<'_>, distance: f64) -> usize {
        self.hits.partition_point(|hit| nearest_order(hit, partition, id, distance).is_lt())
    }

    fn reserve_hits(&mut self, bytes: usize) -> Result<()> {
        let reservation = Arc::get_mut(&mut self.memory).expect("nearest owns its reservation");
        reservation.grow_to(bytes.max(reservation.bytes()))
    }

    /// Admits another window's hits and counts its candidates.
    fn absorb(&mut self, other: NearestWindow) -> Result<()> {
        let total = self.total_after(other.total)?;
        let (mut kept, mut incoming) = (0, 0);
        let mut bytes = 4096usize;
        for _ in 0..self.k {
            let left = self.hits.get(kept);
            let right = other.hits.get(incoming);
            let winner = if let Some(right) = right
                && left.is_none_or(|left| !nearest_order(left, right.partition, EntityIdRef::from(&right.id), right.distance).is_lt())
            {
                incoming += 1;
                right
            } else if let Some(left) = left {
                kept += 1;
                left
            } else {
                break;
            };
            bytes = bytes.saturating_add(identity_charge(EntityIdRef::from(&winner.id)).saturating_add(128));
        }
        // A refusal here lets the caller rank the partition exactly into the original window.
        self.reserve_hits(bytes)?;

        self.hits.truncate(kept);
        let NearestWindow { memory: _source_memory, hits, .. } = other;
        for hit in hits.into_iter().take(incoming) {
            let at = self.position(hit.partition, EntityIdRef::from(&hit.id), hit.distance);
            self.hits.insert(at, hit);
        }
        self.total = total;
        self.bytes = bytes;
        Ok(())
    }

    fn finish(self) -> Result<OwnedSearchBatch> {
        let bound = self.hits.iter().try_fold(4096usize, |size, hit| {
            size.checked_add(identity_charge(EntityIdRef::from(&hit.id)).saturating_mul(4).saturating_add(128))
                .ok_or_else(|| Anomaly::fault("rtdb.search/nearest-output", "Nearest output size overflow"))
        })?;
        let reservation = self.memory.budget().reserve(bound)?;
        let ids = rtdb_columnar::entity_id_array(self.hits.iter().map(|hit| EntityIdRef::from(&hit.id)))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", ids.data_type().clone(), false),
            Field::new("distance", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ]));
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
        for (ordinal, id, distance) in &overlay.rows {
            if *ordinal >= partitions.len() { return Err(overlay_invalid()); }
            let Some(distance) = (*distance)? else { continue; };
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
            Arc::clone(context), owner, &partition.relation, valid, columns, candidates.into_iter().collect(),
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
            let read_id = || rtdb_columnar::read_owned_entity_id(id_field, batch.column(id_column).as_ref(), row);
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
            let Some(vector) = read_vector(ArrowCell { field: vector_field, array: vectors, row }, query)? else { continue; };
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
    Decoded { length: usize, distance: Distance },
}

impl VectorCell<'_> {
    fn len(&self) -> usize {
        match self {
            Self::F32(values) => values.len(),
            Self::F64(values) => values.len(),
            Self::Numeric(values) => values.len(),
            Self::Decoded { length, .. } => *length,
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
            Self::Decoded { distance, .. } => distance,
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

fn read_vector<'a>(cell: ArrowCell<'a>, query: &NearestQuery) -> Result<Option<VectorCell<'a>>> {
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
        Value::List(list) | Value::FixedSizeList(list) => list,
        _ => return Err(not_a_vector()),
    };
    Ok(Some(VectorCell::Decoded { length: list.len(), distance: score_decoded_list(list, query)? }))
}

fn score_decoded_list(list: &ListValue, query: &NearestQuery) -> Result<Distance> {
    let list = list.view();
    let mut valid = list.len() == query.dimensions();
    let mut dot = 0.0;
    let mut norm = 0.0;
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
        // A malformed cell is a decode error even if dimension or finiteness has already failed.
        valid &= component.is_finite();
        if valid {
            dot += f64::from(query.probe[index]) * component;
            norm += component * component;
        }
    }
    Ok(if valid { query.finish_distance(dot, norm) } else { Err(InvalidComponents) })
}

fn identity_charge(id: EntityIdRef<'_>) -> usize {
    size_of::<Value>() + id.owned_payload_bytes()
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
    fn nearest_window_moves_partition_ids_and_projects_the_mixed_ranking() {
        let memory = MemoryBudget::new(1 << 20);
        let mut window = NearestWindow::new(&memory, 5).unwrap();
        let mut settled = NearestWindow::new(&memory, 5).unwrap();
        let text = EntityIdRef::String("places/東京");
        let keyword = EntityIdRef::Keyword("places/東京");
        let integer = EntityIdRef::Integer(-3);
        let uuid = EntityIdRef::Uuid([7; 16]);
        let farther = 0.5_f64.next_up();
        window.admit(0, text, 0.5).unwrap();
        window.admit(0, EntityIdRef::String("evicted"), 2.0).unwrap();
        for (id, distance) in [
            (keyword, 0.5), (uuid, farther), (text, 0.5), (integer, 0.5),
            (EntityIdRef::String("outside the merged window"), 1.0),
            (EntityIdRef::String("outside the partition window"), 2.0),
        ] {
            settled.admit(1, id, distance).unwrap();
        }
        let payload = |hit: &NearestHit| match EntityIdRef::from(&hit.id) {
            EntityIdRef::String(value) | EntityIdRef::Keyword(value) => value.as_ptr(),
            _ => unreachable!(),
        };
        let source_payloads = [payload(&settled.hits[0]), payload(&settled.hits[1])];

        window.absorb(settled).unwrap();
        assert_eq!([payload(&window.hits[1]), payload(&window.hits[2])], source_payloads);

        let page = window.finish().unwrap();
        assert_eq!(page.total, 8);
        let batch = page.hits.batch();
        assert_eq!(batch.num_rows(), 5);
        let field = batch.schema_ref().field(0);
        assert_eq!(field, &rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type()).unwrap());
        let ids = batch.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
        let distances = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let partitions = batch.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        for (row, (id, distance, partition, offset)) in [
            (text, 0.5, 0, 0), (text, 0.5, 1, 1), (keyword, 0.5, 1, 0),
            (integer, 0.5, 1, 0), (uuid, farther, 1, 0),
        ].into_iter().enumerate() {
            assert_eq!(rtdb_columnar::read_owned_entity_id(field, ids, row).unwrap(), id);
            assert_eq!(ids.value_offset(row), offset);
            assert_eq!(distances.value(row).to_bits(), distance.to_bits());
            assert_eq!(partitions.value(row), partition);
        }
    }

    #[test]
    fn refused_nearest_window_merge_leaves_the_original_window_for_exact_ranking() {
        let window_charge = 4096 + 2 * size_of::<NearestHit>();
        let original_bytes = 4096 + size_of::<Value>() + 128;
        let original_charge = window_charge.max(original_bytes);
        let text = "id".repeat(1024);
        let hit_charge = 4096 + size_of::<Value>() + text.len() + 128;
        let memory = MemoryBudget::new(original_charge + hit_charge);
        let mut window = NearestWindow::new(&memory, 1).unwrap();
        window.admit(0, EntityIdRef::Integer(1), 0.75).unwrap();
        let mut settled = NearestWindow::new(&memory, 1).unwrap();
        settled.admit(1, EntityIdRef::String(&text), 0.5).unwrap();

        assert_eq!(window.absorb(settled).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), original_charge);
        assert_eq!(window.bytes, original_bytes);
        assert_eq!(window.total, 1);
        assert_eq!(window.hits[0].id, EntityId::Integer(1));

        window.admit(1, EntityIdRef::String(&text), 0.5).unwrap();
        assert_eq!(window.total, 2);
        assert_eq!(window.hits[0].id, EntityId::String(text));
        assert_eq!(memory.used_bytes(), hit_charge);
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
            let cell = read_vector(ArrowCell { field: &field, array, row: 0 }, &nearest).unwrap().unwrap();
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
        assert_eq!(read_vector(cell, &query(Vec::new(), 1)).err(), Some(not_a_vector()));
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
            let cell = read_vector(ArrowCell { field: &field, array, row: 0 }, &nearest).unwrap().unwrap();
            assert!(matches!(&cell, VectorCell::Numeric(_)));
            let expected = nearest.distance(expected.into_iter()).unwrap().unwrap();
            assert_eq!(cell.distance(&nearest).unwrap().unwrap().to_bits(), expected.to_bits());
        }

        let field = Field::new("vector", mixed.data_type().clone(), false);
        let malformed = ArrowCell { field: &field, array: &mixed, row: 1 };
        assert_eq!(read_vector(malformed, &nearest).err(), Some(not_a_vector()));
        let nonfinite = read_vector(ArrowCell { row: 2, ..malformed }, &nearest).unwrap().unwrap();
        assert!(nonfinite.distance(&nearest).is_err());
    }

    #[test]
    fn nearest_refuses_unsigned_components_even_when_the_numeric_span_opens() {
        let array = ListArray::from_iter_primitive::<UInt64Type, _, _>([Some([Some(u64::MAX), Some(1)])]);
        let field = Field::new("vector", array.data_type().clone(), false);
        let cell = ArrowCell { field: &field, array: &array, row: 0 };
        assert!(cell.numeric_list_span().is_some());
        assert_eq!(read_vector(cell, &query(Vec::new(), 1)).err(), Some(not_a_vector()));
    }

    #[test]
    fn nearest_scores_decoded_lists_without_changing_refusal_precedence() {
        let nearest = NearestQuery::new(vec![0.25, -0.5], Vec::new(), 1).unwrap();
        let list = |values: Vec<Value>| ListValue::try_from_cells(values.into_iter().map(Ok)).unwrap();
        let decoded = list(vec![Value::I64(7), Value::F32(-0.25)]);
        let expected = nearest.distance([7.0, -0.25].into_iter()).unwrap().unwrap();
        let actual = score_decoded_list(&decoded, &nearest).unwrap().unwrap().unwrap();
        assert_eq!(actual.to_bits(), expected.to_bits());

        let invalid = list(vec![Value::F64(f64::NAN), Value::I64(7)]);
        assert!(score_decoded_list(&invalid, &nearest).unwrap().is_err());
        let malformed = list(vec![Value::F64(f64::NAN), Value::I64(7), Value::Null]);
        assert_eq!(score_decoded_list(&malformed, &nearest).err(), Some(not_a_vector()));
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
        let nearest = query(Vec::new(), 1);
        let mut overlay = HeldVectorOverlay::new(&memory).unwrap();
        let id = EntityIdRef::String("a");
        overlay.shadow_staged(0, id, Some(&[1.0, 0.0]), std::iter::empty(), &nearest).unwrap();
        overlay.shadow_staged(1, id, None, std::iter::empty(), &nearest).unwrap();
        assert!(overlay.shadow_staged(0, id, None, std::iter::empty(), &nearest).is_err());
    }

    #[test]
    fn nearest_staged_shadows_release_arrow_and_keep_scores_and_refusals() {
        use arrow_array::ArrayRef;

        let memory = MemoryBudget::new(1 << 20);
        let nearest = query(vec![BTreeSet::from([b"yes".to_vec()])], 1);
        let vector = Arc::new(Float32Array::from(vec![-9.0, 3.0, 0.0, f32::NAN, 2.0])) as ArrayRef;
        let filter = Arc::new(BinaryArray::from(vec![
            b"no".as_slice(), b"yes".as_slice(), b"no".as_slice(), b"no".as_slice(), b"no".as_slice(),
        ])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("vector", vector), ("filter", filter)]).unwrap();
        let source = OwnedBatch::new(batch, &memory).unwrap();
        let charge = Arc::downgrade(source.reservation());
        let vector = source.batch().column(0).as_any().downcast_ref::<Float32Array>().unwrap();
        let filter = source.batch().column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut overlay = HeldVectorOverlay::new(&memory).unwrap();
        let initial_bytes = overlay._retained.bytes();
        let invalid = &vector.values()[3..5];
        for (id, vector, filter) in [
            (1, Some(&vector.values()[1..3]), Some(filter.value(1))),
            (2, Some(invalid), Some(filter.value(0))),
            (3, None, Some(filter.value(1))),
            (4, Some(invalid), Some(filter.value(1))),
        ] {
            overlay.shadow_staged(0, EntityIdRef::Integer(id), vector, [filter].into_iter(), &nearest).unwrap();
        }
        drop(source);

        assert!(charge.upgrade().is_none());
        assert_eq!(overlay._retained.bytes() - initial_bytes, 4 * (size_of::<Value>() + 256));
        assert_eq!(overlay.rows[0].2.unwrap(), Some(0.0));
        assert!(overlay.contains_shadow(0, &EntityId::Integer(2)));
        assert_eq!(overlay.rows[1].2.unwrap(), None);
        assert!(overlay.contains_shadow(0, &EntityId::Integer(3)));
        assert_eq!(overlay.rows[2].2.unwrap(), None);
        let error = Anomaly::from(overlay.rows[3].2.unwrap_err());
        assert_eq!(error.code, "rtdb.owned/nearest-dimension");
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
        assert_eq!(overlay.rows[0].2.unwrap(), None);
        let error = Anomaly::from(overlay.rows[1].2.unwrap_err());
        assert_eq!(error.code, "rtdb.owned/nearest-dimension");
    }
}
