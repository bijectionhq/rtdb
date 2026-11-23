use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use geometry::{GeometryWork, KeyRange};
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

pub struct Hit {
    pub id: EntityId,
    pub distance_m: Option<f64>,
    pub(super) _reservation: Arc<MemoryReservation>,
}

pub(super) struct SelectedHit {
    pub(super) hit: Hit,
    pub(super) source: Arc<OwnedBatch>,
    pub(super) row: usize,
}

pub struct Page {
    pub hits: Vec<Hit>,
    pub more: bool,
    pub next_distance_m: Option<f64>,
    pub(super) rows: Option<OwnedBatch>,
    pub(super) includes_documents: bool,
    pub(super) ranked_distances: Option<Float64Array>,
    pub(super) _reservation: Arc<MemoryReservation>,
}

impl Page {
    pub fn distance_column(&self) -> Float64Array {
        self.ranked_distances.clone()
            .unwrap_or_else(|| Float64Array::from_iter(self.hits.iter().map(|hit| hit.distance_m)))
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
        let result = rtdb_query::visit_trie_iids(
            &context, Arc::clone(&database.storage), &table.slug, &trie.key, |_| {
                unindexed_rows += 1;
                if unindexed_rows > MAX_UNINDEXED_ROWS { Err(postings::work_limit()) } else { Ok(()) }
            },
        ).await;
        if unindexed_rows > MAX_UNINDEXED_ROWS { return Ok(false); }
        result?;
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
    location: Option<Location>,
    ranges: Vec<KeyRange>,
    steps: usize,
    _reservation: Arc<MemoryReservation>,
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
        let ranges = match &region {
            Some(region) => work.scoped(|work| predicate.cover(region, work))?,
            None => None,
        }
        .unwrap_or_else(|| {
            vec![KeyRange {
                start: i64::MIN,
                end: i64::MAX,
            }]
        });
        let bytes = region.as_ref().map_or(0, Location::retained_bytes)
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
    let reservation = context
        .memory
        .reserve(capacity * std::mem::size_of::<Hit>())?;
    let _selection = context
        .memory
        .reserve(capacity * std::mem::size_of::<SelectedHit>())?;
    let mut selected = Vec::<SelectedHit>::with_capacity(capacity);
    let mut more = false;
    let projection_layout =
        visit_matches(Arc::clone(&context), owner, relation, path, region, &request, projection, false, |source, row, id, _, distance_m| {
                let index = selected
                    .partition_point(|previous| order(&previous.hit, id, distance_m).is_lt());
                if index < capacity {
                    if selected.len() == capacity {
                        selected.pop();
                        more = true;
                    }
                    let reservation = context.memory.reserve(id.owned_payload_bytes().saturating_add(128))?;
                    let hit = Hit {
                        id: id.into_owned(),
                        distance_m,
                        _reservation: reservation,
                    };
                    selected.insert(index, SelectedHit { hit, source: Arc::clone(source), row });
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
    let hits = selected.into_iter().map(|selected| selected.hit).collect();
    Ok(Page {
        hits,
        more,
        next_distance_m: None,
        rows,
        includes_documents: projection.has_documents(),
        ranked_distances: None,
        _reservation: reservation,
    })
}

fn inconsistent_projection() -> Anomaly {
    Anomaly::fault("rtdb.spatial/projection", "Spatial matches have no projection")
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
) -> Result<Option<(rtdb_plan::Schema, Vec<usize>)>> {
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
                .get_exact_or(&key, length, &context.memory)
                .await
            {
                Ok(bytes) => {
                    match postings::candidates(
                        bytes,
                        digest,
                        partition,
                        &region.ranges,
                        &context.memory,
                        || context.check_cancelled(),
                        |iid| candidates.insert(iid),
                    ) {
                        Ok(()) => continue,
                        Err(error) if error.code == "rtdb.spatial/postings" => (),
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
    let cursor = rtdb_query::open_owned_spatial_scan(
        Arc::clone(&context),
        owner,
        relation,
        geometry,
        &definition.partition[..request.partition.len()],
        request.valid_at,
        candidates.ids,
        projection,
        include_iid,
    )
    .await?;
    let Some(rtdb_query::OwnedSpatialScan { mut cursor, document_columns }) = cursor else {
        return Ok(None);
    };
    let mut refined = 0;
    let partition_start = 1 + usize::from(geometry.is_some());
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
                    region.predicate.evaluate(&location, location_filter, work)
                })?
            } else {
                Some(None)
            };
            if let Some(distance_m) = exact {
                visit(&source, row, id, iids.as_ref().map(|iids| iids.get(row)).transpose()?, distance_m)?;
            }
        }
    }
    Ok(Some((cursor.runtime_schema().clone(), document_columns)))
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

fn order(hit: &Hit, id: EntityIdRef<'_>, distance: Option<f64>) -> std::cmp::Ordering {
    match (hit.distance_m, distance) {
        (Some(left), Some(right)) => compare_nearest(left, EntityIdRef::from(&hit.id), right, id),
        _ => EntityIdRef::from(&hit.id).cmp(&id),
    }
}

fn compare_nearest(left: f64, left_id: EntityIdRef<'_>, right: f64, right_id: EntityIdRef<'_>) -> std::cmp::Ordering {
    left.total_cmp(&right).then_with(|| left_id.cmp(&right_id))
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
        for row in 0..events.len() {
            context.check_cancelled_at(row)?;
            self.insert_unindexed(events.iid(row)?)?;
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
