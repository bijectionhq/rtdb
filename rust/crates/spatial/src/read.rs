use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use geometry::{Geometry, GeometryWork, KeyRange, predicates::prepared::PreparedGeometry};
use rtdb_columnar::{
    BatchBuilder, EntityIdRef, IidView, MemoryBudget, MemoryReservation, OwnedBatch, binary_cell,
    copy_selected_cells, estimate_copy_cell_bytes, read_entity_id,
};
use rtdb_format::data::EventBatch;
use rtdb_plan::Catalog;
use rtdb_query::QueryContext;
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Iid, OwnedRelation, Result, SpatialGeometryEncoding, Timestamp};

use crate::{
    Predicate, Projection, definition,
    kernel::{self, Location},
    postings::{self, Partition},
};

const MAX_CANDIDATES: usize = 65_536;
const MAX_REFINED_ROWS: usize = 65_536;
const MAX_UNINDEXED_BYTES: u64 = 64 << 20;
const MAX_UNINDEXED_ROWS: usize = 65_536;
const MAX_INDEXED_BYTES: u64 = 256 << 20;

pub struct Request<'a> {
    pub partition: Vec<Cow<'a, [u8]>>,
    pub region: Cow<'a, [u8]>,
    pub predicate: Predicate,
    pub valid_at: Timestamp,
    pub page: PageOrder<'a>,
    pub limit: usize,
}

#[derive(Clone, Copy)]
pub enum PageOrder<'a> {
    Identity { after: Option<EntityIdRef<'a>> },
    NearestOnce,
}

#[derive(Clone, Copy)]
pub struct NearestPosition<'a> {
    pub distance_m: f64,
    pub id: EntityIdRef<'a>,
}

pub(super) struct SelectedHit {
    pub(super) source: Arc<OwnedBatch>,
    pub(super) row: usize,
    pub(super) distance_m: Option<f64>,
}

pub struct Page {
    pub more: bool,
    pub next_distance_m: Option<f64>,
    pub(super) rows: Option<OwnedBatch>,
    pub(super) includes_documents: bool,
    pub(super) distances: Float64Array,
    pub(super) _distance_reservation: Arc<MemoryReservation>,
}

impl Page {
    pub fn distance_column(&self) -> Float64Array {
        self.distances.clone()
    }

    pub fn documents(&self) -> Option<&OwnedBatch> {
        self.rows.as_ref().filter(|_| self.includes_documents)
    }

    pub fn rows(&self) -> Option<&OwnedBatch> {
        self.rows.as_ref()
    }
}

pub(super) fn gather_selected(
    hits: &[SelectedHit],
    indices: &[usize],
    memory: &Arc<MemoryBudget>,
) -> Result<Option<OwnedBatch>> {
    let Some(first) = hits.first() else { return Ok(None); };
    let schema = first.source.batch().schema_ref();
    let mut sources: Vec<&OwnedBatch> = Vec::new();
    let mut positions = Vec::with_capacity(hits.len());
    for hit in hits {
        let source = hit.source.as_ref();
        if source.batch().schema_ref() != schema {
            return Err(Anomaly::fault(
                "rtdb.spatial/projection",
                "Spatial document page schemas differ",
            ));
        }
        let index = sources
            .iter()
            .position(|other| std::ptr::eq(*other, source))
            .unwrap_or_else(|| {
                let index = sources.len();
                sources.push(source);
                index
            });
        positions.push((index, hit.row));
    }
    let mut output = BatchBuilder::new(4096 + hits.len() * 64 + indices.len() * 128, memory)?;
    let columns = indices
        .iter()
        .map(|&index| {
            let field = schema.field(index);
            let arrays = sources
                .iter()
                .map(|source| source.batch().column(index).as_ref())
                .collect::<Vec<_>>();
            let bytes = positions
                .iter()
                .try_fold(4096usize, |bytes, &(source, row)| {
                    estimate_copy_cell_bytes(field, arrays[source], row)?
                        .checked_mul(4)
                        .and_then(|cell| bytes.checked_add(cell))
                        .ok_or_else(postings::work_limit)
                })?;
            output.column(bytes, || copy_selected_cells(&arrays, &positions))
        })
        .collect::<Result<Vec<_>>>()?;
    let projection_error = |error: arrow_schema::ArrowError| {
        Anomaly::fault("rtdb.spatial/projection", error.to_string())
    };
    let schema = if indices.iter().copied().eq(0..schema.fields().len()) {
        Arc::clone(schema)
    } else {
        Arc::new(schema.project(indices).map_err(projection_error)?)
    };
    let batch = RecordBatch::try_new(schema, columns).map_err(projection_error)?;
    output.finish(batch).map(Some)
}

pub(super) fn empty_projected_documents(
    projection: Projection<'_>,
    schema: Option<(&rtdb_plan::Schema, &[usize])>,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<OwnedBatch>> {
    if !matches!(projection, Projection::SelectedDocuments(_)) {
        return Ok(None);
    }
    let schema = match schema {
        Some((schema, columns)) => Schema::new(columns.iter().map(|&column| {
            let field = schema.fields().get(column).ok_or_else(inconsistent_projection)?;
            rtdb_columnar::to_field(&field.name, &field.data_type)
        }).collect::<Result<Vec<_>>>()?),
        None => Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type())?,
            Field::new("_system_from", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ]),
    };
    let scratch = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
    let arrays = RecordBatch::new_empty(Arc::new(schema));
    OwnedBatch::with_reservation(arrays, scratch).map(Some)
}

pub async fn path_ready(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
) -> Result<bool> {
    let definition = definition::held_definition(Arc::clone(&context), owner, relation, path).await?;
    definition_ready(context, &definition).await
}

pub async fn require_path_ready(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    columns: &[String], encoding: SpatialGeometryEncoding,
) -> Result<()> {
    let refused = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/spatial-not-ready",
        "The spatial path changed or lacks coverage at the ordered cut");
    let definition = definition::held_definition(Arc::clone(&context), owner, relation, path).await
        .map_err(|error| if error.code == "rtdb.owned/spatial-path" { refused() } else { error })?;
    if columns.split_first().is_none_or(|(geometry, partition)| {
        geometry != &definition.geometry || partition != definition.partition.as_slice()
    }) || definition.encoding != encoding || !definition_ready(context, &definition).await? {
        return Err(refused());
    }
    Ok(())
}

async fn definition_ready(context: Arc<QueryContext>, definition: &definition::Definition) -> Result<bool> {
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else { return Ok(false); };
    let Some(table) = snapshot.table_entry(&definition.table) else { return Ok(true); };
    let artefact = definition.artefact();
    let mut uncovered_bytes = 0u64;
    let mut uncovered = Vec::new();
    for trie in snapshot.trie_entries(&definition.table) {
        context.check_cancelled()?;
        let available = trie.artefact(&artefact.kind, &artefact.id, artefact.version).is_some_and(|record| {
            record.file_size.and_then(|size| u64::try_from(size).ok())
                .is_some_and(|size| (1..=postings::MAX_FILE_BYTES).contains(&size))
                && record.stats.as_ref().is_some_and(|digest| digest.len() == 32)
        });
        if available { continue; }
        if rtdb_maintenance::wants(&artefact, &trie.key, trie.data_file_size, snapshot.trie_catalog().file_size_target()) {
            return Ok(false);
        }
        let size = u64::try_from(trie.data_file_size).map_err(|_| postings::work_limit())?;
        uncovered_bytes = uncovered_bytes.saturating_add(size);
        if uncovered_bytes > MAX_UNINDEXED_BYTES { return Ok(false); }
        uncovered.push(trie);
    }

    let mut unindexed_rows = snapshot.iter_segments(&definition.table)
        .flat_map(|segment| segment.batches())
        .fold(0usize, |count, batch| count.saturating_add(batch.view().len()));
    if unindexed_rows > MAX_UNINDEXED_ROWS { return Ok(false); }
    for trie in uncovered {
        context.check_cancelled()?;
        if let Some(rows) = trie.metadata.as_ref().and_then(|metadata| metadata.row_count)
            .and_then(|rows| usize::try_from(rows).ok())
        {
            unindexed_rows = unindexed_rows.saturating_add(rows);
            if unindexed_rows > MAX_UNINDEXED_ROWS { return Ok(false); }
            continue;
        }
        let Some(rows) = rtdb_query::trie_row_count_up_to(
            &context, Arc::clone(&database.storage), &table.slug, &trie.key,
            MAX_UNINDEXED_ROWS - unindexed_rows,
        ).await? else { return Ok(false); };
        unindexed_rows += rows;
    }
    Ok(true)
}

pub async fn read_held(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
    request: Request<'_>,
    projection: Projection<'_>,
) -> Result<Page> {
    let region = PreparedRegion::new(&request.region, request.predicate, &context.memory)?;
    read_prepared(context, owner, relation, path, &region, request, projection).await
}

pub struct PreparedRegion {
    predicate: Predicate,
    location: Option<RegionGeometry>,
    ranges: Vec<KeyRange>,
    steps: usize,
    _reservation: Arc<MemoryReservation>,
}

enum RegionGeometry {
    Prepared(PreparedGeometry),
    Plain(Geometry),
}

impl RegionGeometry {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Prepared(geometry) => geometry.retained_bytes(),
            Self::Plain(geometry) => geometry.retained_bytes(),
        }
    }

    fn evaluate(&self, predicate: Predicate, candidate: &Location, work: &mut GeometryWork) -> Result<Option<Option<f64>>> {
        match self {
            Self::Prepared(geometry) => predicate.evaluate(candidate, geometry, Some(geometry), work),
            Self::Plain(geometry) => predicate.evaluate(candidate, geometry, None, work),
        }
    }
}

impl PreparedRegion {
    pub fn new(region_bytes: &[u8], predicate: Predicate, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut reservation = memory.reserve(kernel::WORK_BYTES)?;
        let mut work = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
        let region = if matches!(predicate, Predicate::All) {
            if !region_bytes.is_empty() {
                return Err(kernel::invalid(
                    "An unrestricted spatial read takes no region",
                ));
            }
            None
        } else {
            let region = Location::decode(region_bytes, &mut work)?;
            predicate.validate_region(&region)?;
            Some(region)
        };
        let (region, ranges) = match region {
            Some(region) if !region.geometry.is_empty() && predicate.cover_predicate().is_some() => {
                work.scoped(|work| {
                    let geometry = predicate.prepare_geometry(&region.geometry, work)?;
                    let ranges = predicate.cover(&geometry, work)?;
                    Ok::<_, Anomaly>((Some(RegionGeometry::Prepared(geometry)), ranges))
                })?
            }
            Some(region) => {
                // An uncovered predicate only spends topology work when it reaches a candidate.
                let geometry = if region.geometry.is_empty()
                    || matches!(predicate, Predicate::Classification(crate::Classification::Unknown))
                {
                    RegionGeometry::Plain(region.geometry)
                } else {
                    let mut preparation = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
                    match predicate.prepare_geometry(&region.geometry, &mut preparation) {
                        Ok(geometry) => RegionGeometry::Prepared(geometry),
                        Err(_) => RegionGeometry::Plain(region.geometry),
                    }
                };
                (Some(geometry), None)
            }
            None => (None, None),
        };
        let ranges = ranges.unwrap_or_else(|| vec![KeyRange { start: i64::MIN, end: i64::MAX }]);
        let bytes = region.as_ref().map_or(0, RegionGeometry::retained_bytes)
            .checked_add(ranges.capacity() * std::mem::size_of::<KeyRange>())
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(postings::work_limit)?;
        let lease = Arc::get_mut(&mut reservation).expect("geometry construction owns the reservation");
        if bytes > lease.bytes() {
            lease.grow_to(bytes)?;
        } else {
            lease.shrink_to(bytes)?;
        }
        Ok(Self { predicate, location: region, ranges, steps: work.usage().steps, _reservation: reservation })
    }
}

pub async fn read_prepared(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
    region: &PreparedRegion,
    request: Request<'_>,
    projection: Projection<'_>,
) -> Result<Page> {
    let nearest = matches!(region.predicate, Predicate::Nearest { .. });
    let valid_order = match request.page {
        PageOrder::Identity { .. } => !nearest,
        PageOrder::NearestOnce => nearest,
    };
    if !(1..=128).contains(&request.limit) || !valid_order {
        return Err(kernel::invalid("Invalid spatial page size or continuation"));
    }
    let capacity = request.limit;
    let _selection = context
        .memory
        .reserve(capacity * std::mem::size_of::<SelectedHit>())?;
    let mut selected = Vec::<SelectedHit>::with_capacity(capacity);
    let mut more = false;
    let (projection_layout, _) =
        visit_matches(Arc::clone(&context), owner, relation, path, region, &request, projection, false, |source, row, id, _, distance_m| {
                let index = selection_position(&selected, id, distance_m)?;
                if index < capacity {
                    if selected.len() == capacity {
                        selected.pop();
                        more = true;
                    }
                    selected.insert(index, SelectedHit { source: Arc::clone(source), row, distance_m });
                } else {
                    more = true;
                }
                Ok(())
        }).await?;
    if matches!(request.page, PageOrder::NearestOnce) {
        more = false;
    }
    context.check_cancelled()?;
    let rows = if selected.is_empty() {
        empty_projected_documents(
            projection, projection_layout.as_ref().map(|(schema, columns)| (schema, columns.as_slice())), &context.memory,
        )?
    } else {
        let identities = [0];
        let columns = if projection.has_documents() {
            projection_layout.as_ref().ok_or_else(inconsistent_projection)?.1.as_slice()
        } else {
            &identities
        };
        gather_selected(&selected, columns, &context.memory)?
    };
    let reservation = context.memory.reserve(4096 + selected.len() * 16)?;
    let distances = Float64Array::from_iter(selected.iter().map(|hit| hit.distance_m));
    Ok(Page {
        more,
        next_distance_m: None,
        rows,
        includes_documents: projection.has_documents(),
        distances,
        _distance_reservation: reservation,
    })
}

fn inconsistent_projection() -> Anomaly {
    Anomaly::fault("rtdb.spatial/projection", "Spatial matches have no projection")
}

pub struct PreparedRange {
    matches: Vec<(EntityId, Iid)>,
    relation: OwnedRelation,
    valid_at: Timestamp,
    columns: DocumentColumns,
    _reservation: Arc<MemoryReservation>,
}

impl PreparedRange {
    pub fn relation(&self) -> &OwnedRelation { &self.relation }

    pub async fn prepare(
        context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
        region: &PreparedRegion, request: &Request<'_>,
    ) -> Result<Self> {
        if !(1..=128).contains(&request.limit) || !matches!(request.page, PageOrder::Identity { .. })
            || matches!(region.predicate, Predicate::Nearest { .. })
        {
            return Err(kernel::invalid("Invalid spatial page size or continuation"));
        }
        let mut matches = Vec::new();
        let mut reservation = context.memory.reserve(0)?;
        let mut payload_bytes = 0usize;
        let (_, columns) = visit_matches(Arc::clone(&context), owner, relation, path, region, request, Projection::Identities, true,
            |_, _, id, iid, _| {
                let capacity = if matches.len() == matches.capacity() {
                    (matches.capacity() * 2).max(8)
                } else {
                    matches.capacity()
                };
                payload_bytes = payload_bytes.checked_add(id.owned_payload_bytes()).ok_or_else(postings::work_limit)?;
                let required = payload_bytes.checked_add(capacity * std::mem::size_of::<(EntityId, Iid)>())
                    .ok_or_else(postings::work_limit)?;
                Arc::get_mut(&mut reservation).expect("matches own their reservation").grow_to(required)?;
                if capacity > matches.capacity() {
                    matches.reserve_exact(capacity - matches.len());
                }
                matches.push((id.into_owned(), iid.ok_or_else(inconsistent_projection)?));
                Ok(())
            },
        ).await?;
        context.check_cancelled()?;
        matches.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        context.check_cancelled()?;
        Ok(Self { matches, relation: relation.clone(), valid_at: request.valid_at, columns,
            _reservation: reservation })
    }

    pub async fn page(
        &self, context: Arc<QueryContext>, owner: &str, after: Option<EntityIdRef<'_>>,
        limit: usize, projection: Projection<'_>,
    ) -> Result<Page> {
        if !(1..=128).contains(&limit) {
            return Err(kernel::invalid("Invalid spatial page size or continuation"));
        }
        let start = after.map_or(0, |after| self.matches.partition_point(|entry| {
            EntityIdRef::from(&entry.0) <= after
        }));
        let end = (start + limit).min(self.matches.len());
        let _selection = context.memory.reserve(std::mem::size_of::<(EntityIdRef<'_>, Iid)>() * (end - start))?;
        let selection = self.matches[start..end].iter().map(|(id, iid)| {
            (EntityIdRef::from(id), *iid)
        }).collect::<Vec<_>>();
        super::nearest::gather_range(context, owner, &self.relation, self.valid_at, projection,
            &self.columns, &selection, end < self.matches.len()).await
    }
}

pub(super) struct DocumentColumns {
    pub(super) geometry: String,
    pub(super) partition: Vec<String>,
}

pub(super) async fn visit_matches(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
    region: &PreparedRegion,
    request: &Request<'_>,
    projection: Projection<'_>,
    include_iid: bool,
    mut visit: impl FnMut(&Arc<OwnedBatch>, usize, EntityIdRef<'_>, Option<Iid>, Option<f64>) -> Result<()>,
) -> Result<(Option<(rtdb_plan::Schema, Vec<usize>)>, DocumentColumns)> {
    let definition =
        definition::held_definition(Arc::clone(&context), owner, relation, path).await?;
    if request.partition.len() > definition.partition.len() {
        return Err(kernel::invalid(
            "Spatial read binds more than its declared equality partition",
        ));
    }
    let _geometry_memory = context.memory.reserve(kernel::WORK_BYTES)?;
    let mut work = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
    work.charge(region.steps).map_err(kernel::geometry_error)?;
    let partition = (request.partition.len() == definition.partition.len())
        .then(|| Partition::of(request.partition.iter().map(|part| part.as_ref())));
    let mut candidates = Candidates {
        ids: BTreeSet::new(),
        allowance: context.memory.reserve(0)?,
        unindexed_rows: 0,
        unindexed_bytes: 0,
        indexed_bytes: 0,
    };
    let database = context
        .catalog
        .database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(postings::work_limit());
    };
    let reference = definition.artefact_id();
    for trie in snapshot.trie_entries(&definition.table) {
        context.check_cancelled()?;
        let table = snapshot
            .table_entry(&definition.table)
            .ok_or_else(postings::work_limit)?;
        if let Some(record) = trie.artefact(definition::KIND, &reference, definition.generation)
            && let (Some(size), Some(digest)) = (
                record
                    .file_size
                    .and_then(|size| u64::try_from(size).ok())
                    .filter(|size| (1..=postings::MAX_FILE_BYTES).contains(size)),
                record.stats.as_deref(),
            )
        {
            charge_bytes(&mut candidates.indexed_bytes, size, MAX_INDEXED_BYTES)?;
            let key = ObjectKey::new(table.slug.recorded_artefact_key(&trie.key, record)?)?;
            let length = usize::try_from(size).map_err(|_| postings::work_limit())?;
            match database
                .storage
                .get_exact_cached_or(&key, length, &context.memory)
                .await
            {
                Ok(bytes) => {
                    let check = || context.check_cancelled();
                    check()?;
                    let computed = bytes.sha256(|bytes| postings::sha256(bytes, &check))?;
                    match postings::verify_digest(computed, digest).and_then(|()| postings::candidates(
                        bytes,
                        partition,
                        &region.ranges,
                        &context.memory,
                        check,
                        |iid| candidates.insert(iid),
                    )) {
                        Ok(()) => continue,
                        Err(error) if error.code == "rtdb.spatial/postings" => { database.storage.evict(&key); }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => {
                    let error = Anomaly::from(error);
                    if matches!(
                        error.category,
                        AnomalyCategory::Forbidden | AnomalyCategory::Busy
                    ) || error.code == "rtdb.storage/read-lease-expired"
                    {
                        return Err(error);
                    }
                }
            }
        }
        let size = u64::try_from(trie.data_file_size).map_err(|_| postings::work_limit())?;
        charge_bytes(&mut candidates.unindexed_bytes, size, MAX_UNINDEXED_BYTES)?;
        rtdb_query::visit_trie_iids(
            &context,
            Arc::clone(&database.storage),
            &table.slug,
            &trie.key,
            |iid| candidates.insert_unindexed(iid),
        )
        .await?;
    }
    for segment in snapshot.iter_segments(&definition.table) {
        for batch in segment.batches() {
            candidates.extend_live(batch.view(), &context)?;
        }
    }
    let geometry = (region.location.is_some() || matches!(projection, Projection::Documents))
        .then_some(definition.geometry.as_str());
    let partition = definition.partition[..request.partition.len()].to_vec();
    let cursor = rtdb_query::open_owned_spatial_scan(
        Arc::clone(&context),
        owner,
        relation,
        geometry,
        &partition,
        request.valid_at,
        candidates.ids,
        projection,
        include_iid,
    )
    .await?;
    let partition_start = 1 + usize::from(geometry.is_some());
    let columns = DocumentColumns { geometry: definition.geometry, partition };
    let Some(rtdb_query::OwnedSpatialScan { mut cursor, document_columns }) = cursor else {
        return Ok((None, columns));
    };
    let mut refined = 0;
    while let Some(source) = cursor.next_batch().await? {
        let source = Arc::new(source);
        let batch = source.batch();
        let iids = if include_iid {
            let column = batch.column_by_name("_iid").ok_or_else(inconsistent_projection)?;
            Some(IidView::new(column.as_ref())?)
        } else {
            None
        };
        for row in 0..batch.num_rows() {
            context.check_cancelled_at(row)?;
            if !partition_matches(batch, row, partition_start, &request.partition)? {
                continue;
            }
            refined += 1;
            if refined > MAX_REFINED_ROWS {
                return Err(postings::work_limit());
            }
            let id = read_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
            if let PageOrder::Identity { after: Some(after) } = request.page
                && id <= after
            {
                continue;
            }
            let exact = if let Some(location_filter) = &region.location {
                let Some(bytes) =
                    binary_cell(batch.schema_ref().field(1), batch.column(1).as_ref(), row)?
                else {
                    continue;
                };
                work.scoped(|work| {
                    let Some(location) = Location::decode_stored(bytes, definition.encoding, work)? else {
                        return Ok(None);
                    };
                    location_filter.evaluate(region.predicate, &location, work)
                })?
            } else {
                Some(None)
            };
            if let Some(distance_m) = exact {
                visit(&source, row, id, iids.as_ref().map(|iids| iids.get(row)).transpose()?, distance_m)?;
            }
        }
    }
    Ok((Some((cursor.runtime_schema().clone(), document_columns)), columns))
}

fn partition_matches(
    batch: &RecordBatch,
    row: usize,
    start: usize,
    expected: &[Cow<'_, [u8]>],
) -> Result<bool> {
    for (index, expected) in (start..).zip(expected) {
        let value = binary_cell(
            batch.schema_ref().field(index),
            batch.column(index).as_ref(),
            row,
        )?;
        if value != Some(expected.as_ref()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn selection_position(selected: &[SelectedHit], id: EntityIdRef<'_>, distance: Option<f64>) -> Result<usize> {
    let mut lower = 0;
    let mut upper = selected.len();
    while lower < upper {
        let mid = lower + (upper - lower) / 2;
        let hit = &selected[mid];
        let batch = hit.source.batch();
        let previous = read_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), hit.row)?;
        let order = match (hit.distance_m, distance) {
            (Some(left), Some(right)) => left.total_cmp(&right).then_with(|| previous.cmp(&id)),
            _ => previous.cmp(&id),
        };
        if order.is_lt() { lower = mid + 1; } else { upper = mid; }
    }
    Ok(lower)
}

struct Candidates {
    ids: BTreeSet<Iid>,
    allowance: Arc<MemoryReservation>,
    unindexed_rows: usize,
    unindexed_bytes: u64,
    indexed_bytes: u64,
}

impl Candidates {
    fn insert(&mut self, iid: Iid) -> Result<()> {
        if self.ids.contains(&iid) {
            return Ok(());
        }
        if self.ids.len() == MAX_CANDIDATES {
            return Err(postings::work_limit());
        }
        Arc::get_mut(&mut self.allowance)
            .expect("read owns its reservation")
            .grow_to((self.ids.len() + 1) * 96)?;
        self.ids.insert(iid);
        Ok(())
    }

    fn insert_unindexed(&mut self, iid: Iid) -> Result<()> {
        if self.unindexed_rows == MAX_UNINDEXED_ROWS {
            return Err(postings::work_limit());
        }
        self.unindexed_rows += 1;
        self.insert(iid)
    }

    fn extend_live(&mut self, events: &EventBatch, context: &QueryContext) -> Result<()> {
        let iids = events.iids()?;
        for row in 0..iids.len() {
            context.check_cancelled_at(row)?;
            self.insert_unindexed(iids.get(row)?)?;
        }
        Ok(())
    }
}

fn charge_bytes(spent: &mut u64, size: u64, limit: u64) -> Result<()> {
    *spent = spent
        .checked_add(size)
        .filter(|bytes| *bytes <= limit)
        .ok_or_else(postings::work_limit)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::{entity_id_array, entity_id_type, to_field};

    #[test]
    fn spatial_selection_orders_ties_by_the_typed_arrow_identity() -> Result<()> {
        let memory = MemoryBudget::new(64 << 10);
        let ids = entity_id_array([EntityIdRef::String("same"), EntityIdRef::Integer(3), EntityIdRef::Uuid([0; 16])])?;
        let schema = Arc::new(Schema::new(vec![to_field("_id", &entity_id_type())?]));
        let batch = RecordBatch::try_new(schema, vec![ids]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let selected = [2.0, 2.0, 3.0].into_iter().enumerate().map(|(row, distance_m)| SelectedHit {
            source: Arc::clone(&source), row, distance_m: Some(distance_m),
        }).collect::<Vec<_>>();

        assert_eq!(selection_position(&selected, EntityIdRef::Keyword("same"), Some(2.0))?, 1);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(1.0))?, 0);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(2.0))?, 2);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(4.0))?, 3);
        Ok(())
    }
}
