use std::{borrow::Cow, collections::BTreeSet, ops::Range, sync::Arc};

use arrow_array::{Float64Array, RecordBatch, StructArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use geometry::{Geometry, GeometryWork, KeyRange, predicates::prepared::PreparedGeometry};
use rtdb_columnar::{
    BatchBuilder, EntityIdLocator, EntityIdRef, IidView, MemoryBudget, MemoryReservation, OwnedBatch, binary_cell, selected_cell,
    copy_selected_cells, estimate_copy_cell_bytes, read_owned_entity_id, visible_buffer_bytes,
};
use rtdb_format::data::EventBatch;
use rtdb_plan::Catalog;
use rtdb_query::{CandidateIids, QueryContext};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, Iid, OwnedRelation, Result, SpatialGeometryEncoding, Timestamp};

use crate::{
    Predicate, Projection, definition,
    kernel::{self, Location},
    postings::{self, Partition},
    range_matches::{MatchBuilder, RetainedMatches, Retention},
};

const MAX_CANDIDATES: usize = 65_536;
const MAX_REFINED_ROWS: usize = 65_536;
const MAX_UNINDEXED_BYTES: u64 = 64 << 20;
const MAX_UNINDEXED_ROWS: usize = 65_536;
const MAX_INDEXED_BYTES: u64 = 256 << 20;
const MAX_UNUSED_SLICE_BYTES: usize = 16 << 10;

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
    pub(super) probe: Option<Arc<crate::probe::DeferredPage>>,
    pub(super) row: usize,
    pub(super) distance_m: Option<f64>,
}

struct PageHit {
    id: EntityIdLocator,
    hit: SelectedHit,
}

pub struct Page {
    pub more: bool,
    pub next_distance_m: Option<f64>,
    pub(super) rows: Option<OwnedBatch>,
    pub(super) includes_documents: bool,
    pub(super) distances: Float64Array,
    pub(super) _distance_reservation: Arc<MemoryReservation>,
    pub(super) probe: Option<crate::probe::Evidence>,
}

impl Page {
    pub fn omitted_sizes(&self) -> Option<&StructArray> {
        self.probe.as_ref()?.omitted.as_ref()
    }

    pub fn certifies_point_intersects(&self) -> bool {
        self.probe.as_ref().is_some_and(|probe| probe.point_intersects)
    }

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
    let mut output = BatchBuilder::new(4096 + hits.len() * 64 + indices.len() * 128, memory)?;
    let projection_error = |error: arrow_schema::ArrowError| {
        Anomaly::fault("rtdb.spatial/projection", error.to_string())
    };
    let projected = if indices.iter().copied().eq(0..schema.fields().len()) {
        Arc::clone(schema)
    } else {
        Arc::new(schema.project(indices).map_err(projection_error)?)
    };
    if let Some(batch) = slice_selected(hits, indices, &projected) {
        return Ok(Some(batch));
    }

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
    let batch = RecordBatch::try_new(projected, columns).map_err(projection_error)?;
    output.finish(batch).map(Some)
}

fn slice_selected(
    hits: &[SelectedHit],
    indices: &[usize],
    schema: &Arc<Schema>,
) -> Option<OwnedBatch> {
    let first = hits.first()?;
    let source = first.source.batch();
    let end = first.row.checked_add(hits.len())?;
    if end > source.num_rows()
        || !hits.iter().zip(first.row..end).all(|(hit, row)| {
            Arc::ptr_eq(&hit.source, &first.source) && hit.row == row
        })
    {
        return None;
    }

    let columns = indices
        .iter()
        .map(|&index| source.column(index).slice(first.row, hits.len()))
        .collect::<Vec<_>>();
    let reservation = first.source.reservation();
    if reservation.bytes() > MAX_UNUSED_SLICE_BYTES
        && reservation.bytes().saturating_sub(visible_buffer_bytes(&columns)?) > MAX_UNUSED_SLICE_BYTES
    {
        return None;
    }
    let batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;
    OwnedBatch::with_reservation(batch, Arc::clone(reservation)).ok()
}

pub(super) fn empty_projected_documents(
    projection: Projection<'_>,
    schema: Option<(&rtdb_plan::Schema, &[usize])>,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<OwnedBatch>> {
    if !matches!(projection, Projection::SelectedDocuments(_)) {
        return Ok(None);
    }
    let logical_bound = match schema {
        Some((schema, columns)) => columns.iter().try_fold(4096usize, |bytes, &column| {
            let field = schema.fields().get(column).ok_or_else(inconsistent_projection)?;
            field.data_type.retained_bytes()?
                .checked_add(field.name.len())
                .and_then(|size| size.checked_mul(32))
                .and_then(|size| bytes.checked_add(size))
                .ok_or_else(postings::work_limit)
        })?,
        None => 16 << 10,
    };
    let mut reservation = memory.reserve(logical_bound)?;
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
    let bound = rtdb_format::ipc::schema_translation_bound(&schema)?.max(logical_bound);
    Arc::get_mut(&mut reservation).expect("empty projection owns its reservation").grow_to(bound)?;
    let arrays = RecordBatch::new_empty(Arc::new(schema));
    OwnedBatch::with_reservation(arrays, reservation).map(Some)
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
        let available = trie.artefact(&artefact.kind, &artefact.id, artefact.version)
            .is_some_and(|record| postings::Root::from_record(record).is_ok());
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
    point_geometry: bool,
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
        const METADATA_BYTES: usize = 512;
        const FULL_RANGE: KeyRange = KeyRange { start: i64::MIN, end: i64::MAX };

        if matches!(predicate, Predicate::All) {
            if !region_bytes.is_empty() {
                return Err(kernel::invalid(
                    "An unrestricted spatial read takes no region",
                ));
            }
            let reservation = memory.reserve(METADATA_BYTES + std::mem::size_of::<KeyRange>())?;
            return Ok(Self {
                predicate,
                location: None,
                point_geometry: false,
                ranges: vec![FULL_RANGE],
                steps: 0,
                _reservation: reservation,
            });
        }

        let mut reservation = memory.reserve(kernel::WORK_BYTES)?;
        let mut work = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
        let region = Location::decode(region_bytes, &mut work)?;
        predicate.validate_region(&region)?;
        let point_geometry = region.is_point_geometry();
        let (geometry, ranges) = if !region.geometry.is_empty() && predicate.cover_predicate().is_some() {
            work.scoped(|work| {
                let geometry = predicate.prepare_geometry(&region.geometry, work)?;
                let ranges = predicate.cover(&geometry, work)?;
                Ok::<_, Anomaly>((RegionGeometry::Prepared(geometry), ranges))
            })?
        } else {
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
            (geometry, None)
        };
        let ranges = ranges.unwrap_or_else(|| vec![FULL_RANGE]);
        let bytes = geometry.retained_bytes()
            .checked_add(ranges.capacity() * std::mem::size_of::<KeyRange>())
            .and_then(|bytes| bytes.checked_add(METADATA_BYTES)).ok_or_else(postings::work_limit)?;
        let lease = Arc::get_mut(&mut reservation).expect("geometry construction owns the reservation");
        if bytes > lease.bytes() {
            lease.grow_to(bytes)?;
        } else {
            lease.shrink_to(bytes)?;
        }
        Ok(Self { predicate, location: Some(geometry), point_geometry, ranges, steps: work.usage().steps, _reservation: reservation })
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
    read_page(context, owner, relation, path, region, request, projection, None).await
}

pub async fn read_probe(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    region: &PreparedRegion, request: Request<'_>, columns: &[&str], probe: crate::Probe<'_>,
) -> Result<Page> {
    read_page(context, owner, relation, path, region, request, Projection::SelectedDocuments(columns), Some(probe)).await
}

async fn read_page(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    region: &PreparedRegion, request: Request<'_>, projection: Projection<'_>, probe: Option<crate::Probe<'_>>,
) -> Result<Page> {
    let result = read_page_capturing(
        Arc::clone(&context), owner, relation, path, region, &request, projection, probe, true, &mut None,
    ).await;
    let result = match result {
        Err(error) if probe.is_some_and(|probe| !probe.fields.is_empty()) && error.code == "rtdb/memory-budget-exceeded" =>
            read_page_capturing(context, owner, relation, path, region, &request, projection, probe, false, &mut None).await,
        result => result,
    };
    result.map(|(page, _)| page)
}

pub async fn open_probe(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    region: &PreparedRegion, request: Request<'_>, columns: &[&str], probe: crate::Probe<'_>,
) -> Result<(Page, Option<PreparedProbe>)> {
    let mut retained = if matches!(request.page, PageOrder::Identity { after: None }) {
        MatchBuilder::new(&context.memory, Retention::Probe).ok()
    } else {
        None
    };
    let projection = Projection::SelectedDocuments(columns);
    let (page, columns) = match read_page_capturing(
        Arc::clone(&context), owner, relation, path, region, &request, projection, Some(probe), true, &mut retained,
    ).await {
        Err(error) if error.code == "rtdb/memory-budget-exceeded"
            && (retained.take().is_some() || !probe.fields.is_empty()) =>
        {
            return read_page_capturing(context, owner, relation, path, region, &request, projection, Some(probe), false, &mut None)
                .await.map(|(page, _)| (page, None));
        }
        result => result?,
    };
    let matches = if let Some(retained) = retained {
        let matches = retained.finish(|| context.check_cancelled())?;
        (!matches.has_ties()).then(|| PreparedProbe {
            range: PreparedRange {
                matches, relation: relation.clone(), valid_at: request.valid_at, columns,
            },
            certified_point_column: probe.point_column
                .filter(|_| page.certifies_point_intersects()).map(str::to_owned),
        })
    } else {
        None
    };
    context.check_cancelled()?;
    Ok((page, matches))
}

async fn read_page_capturing(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation,
    region: &PreparedRegion, request: &Request<'_>, projection: Projection<'_>, probe: Option<crate::Probe<'_>>, defer: bool,
    retained: &mut Option<MatchBuilder>,
) -> Result<(Page, DocumentColumns)> {
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
        .reserve(capacity * (size_of::<PageHit>() + size_of::<SelectedHit>()))?;
    let mut selected = Vec::<PageHit>::with_capacity(capacity);
    let mut more = false;
    let mut point_intersects = probe.is_some_and(|probe| probe.certify_point && probe.point_column.is_some());
    let (projection_layout, document_columns) = visit_matches(
        Arc::clone(&context), owner, relation, path, region, request, projection, retained.is_some(),
        probe, defer, |source, fallback, row, id, iid, distance_m, certified| {
                point_intersects &= certified;
                if let Some(matches) = retained
                    && !iid.is_some_and(|iid| matches.push(id, iid).is_ok())
                {
                    *retained = None;
                }
                let index = selection_position(&selected, id, distance_m);
                if index < capacity {
                    let batch = source.batch();
                    let id = EntityIdLocator::read_owned(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
                    if selected.len() == capacity {
                        selected.pop();
                        more = true;
                    }
                    selected.insert(index, PageHit {
                        id, hit: SelectedHit { source: Arc::clone(source), probe: fallback.cloned(), row, distance_m },
                    });
                } else {
                    more = true;
                }
                Ok(())
        }).await?;
    let selected: Vec<_> = selected.into_iter().map(|entry| entry.hit).collect();
    if matches!(request.page, PageOrder::NearestOnce) {
        more = false;
    }
    context.check_cancelled()?;
    if !more { *retained = None; }
    let mut evidence = probe.map(|_| crate::probe::Evidence::new(point_intersects && !selected.is_empty()));
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
        if let (Some(probe), Some(evidence)) = (probe, evidence.as_mut()) {
            let summarized = match crate::probe::summarize(&selected, columns, probe, evidence, &context.memory) {
                Err(_) if retained.take().is_some() =>
                    crate::probe::summarize(&selected, columns, probe, evidence, &context.memory),
                result => result,
            };
            crate::probe::gather(&selected, summarized.as_deref().unwrap_or(columns), &context.memory)?
        } else {
            gather_selected(&selected, columns, &context.memory)?
        }
    };
    let reservation = context.memory.reserve(4096 + selected.len() * 16)?;
    let distances = Float64Array::from_iter(selected.iter().map(|hit| hit.distance_m));
    Ok((Page {
        more,
        next_distance_m: None,
        rows,
        includes_documents: projection.has_documents(),
        distances,
        _distance_reservation: reservation,
        probe: evidence,
    }, document_columns))
}

pub async fn gather_document(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation,
    valid_at: Timestamp, id: EntityIdRef<'_>, revision: Timestamp, columns: &[&str],
) -> Result<Page> {
    let projection = Projection::SelectedDocuments(columns);
    let scan = rtdb_query::open_owned_spatial_scan(
        Arc::clone(&context), owner, relation, None, &[], valid_at, [id.iid()].into(), projection, false,
    ).await?;
    let missing = || Anomaly::fault("rtdb.spatial/gather", "The held spatial document changed identity or revision");
    let rtdb_query::OwnedSpatialScan { mut cursor, document_columns } = scan.ok_or_else(missing)?;
    let mut selected = None;
    while let Some(source) = cursor.next_batch().await? {
        let source = Arc::new(source);
        let batch = source.batch();
        for row in 0..batch.num_rows() {
            context.check_cancelled_at(row)?;
            if selected.is_some() || read_owned_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)? != id {
                return Err(missing());
            }
            let (index, field) = batch.schema_ref().column_with_name("_system_from").ok_or_else(missing)?;
            let (_, array, row_index) =
                selected_cell(field, batch.column(index).as_ref(), row)?.ok_or_else(missing)?;
            let system_from = array.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(missing)?;
            if system_from.value(row_index) != revision.0 {
                return Err(missing());
            }
            selected = Some(SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: None });
        }
    }
    let hit = selected.ok_or_else(missing)?;
    let rows = gather_selected(&[hit], &document_columns, &context.memory)?;
    let reservation = context.memory.reserve(4096)?;
    Ok(Page {
        more: false,
        next_distance_m: None,
        rows,
        includes_documents: true,
        distances: Float64Array::from(vec![None]),
        _distance_reservation: reservation,
        probe: None,
    })
}

fn inconsistent_projection() -> Anomaly {
    Anomaly::fault("rtdb.spatial/projection", "Spatial matches have no projection")
}

pub struct PreparedProbe {
    range: PreparedRange,
    certified_point_column: Option<String>,
}

impl PreparedProbe {
    pub fn relation(&self) -> &OwnedRelation { self.range.relation() }

    pub async fn page(
        &self, context: Arc<QueryContext>, owner: &str, after: EntityIdRef<'_>,
        limit: usize, columns: &[&str], mut probe: crate::Probe<'_>,
    ) -> Result<Page> {
        probe.certify_point &= self.certified_point_column.as_deref()
            .is_some_and(|column| probe.point_column == Some(column));
        self.range.project_page(context, owner, Some(after), limit, Projection::SelectedDocuments(columns), Some(probe)).await
    }
}

pub struct PreparedRange {
    matches: RetainedMatches,
    relation: OwnedRelation,
    valid_at: Timestamp,
    columns: DocumentColumns,
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
        let mut matches = MatchBuilder::new(&context.memory, Retention::Range)?;
        let (_, columns) = visit_matches(Arc::clone(&context), owner, relation, path, region, request, Projection::Identities, true, None, false,
            |_, _, _, id, iid, _, _| matches.push(id, iid.ok_or_else(inconsistent_projection)?),
        ).await?;
        let matches = matches.finish(|| context.check_cancelled())?;
        Ok(Self { matches, relation: relation.clone(), valid_at: request.valid_at, columns })
    }

    pub async fn page(
        &self, context: Arc<QueryContext>, owner: &str, after: Option<EntityIdRef<'_>>,
        limit: usize, projection: Projection<'_>,
    ) -> Result<Page> {
        self.project_page(context, owner, after, limit, projection, None).await
    }

    async fn project_page(
        &self, context: Arc<QueryContext>, owner: &str, after: Option<EntityIdRef<'_>>,
        limit: usize, projection: Projection<'_>, probe: Option<crate::Probe<'_>>,
    ) -> Result<Page> {
        if !(1..=128).contains(&limit) {
            return Err(kernel::invalid("Invalid spatial page size or continuation"));
        }
        let start = after.map_or(0, |after| self.matches.seek_after(after));
        let end = (start + limit).min(self.matches.len());
        let _selection = context.memory.reserve(std::mem::size_of::<(EntityIdRef<'_>, Iid)>() * (end - start))?;
        let selection = (start..end)
            .map(|position| self.matches.get(position))
            .collect::<Result<Vec<_>>>()?;
        let result = super::nearest::gather_range(
            Arc::clone(&context), owner, &self.relation, self.valid_at, projection,
            &self.columns, &selection, end < self.matches.len(), probe, true,
        ).await;
        match result {
            Err(error) if probe.is_some_and(|probe| !probe.fields.is_empty()) && error.code == "rtdb/memory-budget-exceeded" =>
                super::nearest::gather_range(context, owner, &self.relation, self.valid_at, projection,
                    &self.columns, &selection, end < self.matches.len(), probe, false).await,
            result => result,
        }
    }
}

pub(super) struct DocumentColumns {
    pub(super) geometry: String,
    pub(super) refined_geometry: bool,
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
    probe: Option<crate::Probe<'_>>,
    defer: bool,
    mut visit: impl FnMut(&Arc<OwnedBatch>, Option<&Arc<crate::probe::DeferredPage>>, usize, EntityIdRef<'_>, Option<Iid>, Option<f64>, bool) -> Result<()>,
) -> Result<(Option<(rtdb_plan::Schema, Vec<usize>)>, DocumentColumns)> {
    let definition =
        definition::held_definition(Arc::clone(&context), owner, relation, path).await?;
    if request.partition.len() > definition.partition.len() {
        return Err(kernel::invalid(
            "Spatial read binds more than its declared equality partition",
        ));
    }
    let certify_points = probe.filter(|probe| probe.certify_point).and_then(|probe| probe.point_column) == Some(definition.geometry.as_str())
        && definition.encoding == SpatialGeometryEncoding::BijectionGeometry
        && request.partition.len() == definition.partition.len()
        && matches!(region.predicate, Predicate::Topology(geometry::predicates::TopologyPredicate::Intersects))
        && region.point_geometry;
    let mut work = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
    work.charge(region.steps).map_err(kernel::geometry_error)?;
    let partition = (request.partition.len() == definition.partition.len())
        .then(|| Partition::of(request.partition.iter().map(|part| part.as_ref())));
    let mut candidates = Candidates::new(&context.memory)?;
    let mut indexed_bytes = 0;
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
            && let Ok(root) = postings::Root::from_record(record)
        {
            let key = ObjectKey::new(table.slug.recorded_artefact_key(&trie.key, record)?)?;
            let storage = &database.storage;
            let result = async {
                let size = root.range().end;
                let whole = if storage.object_store().supports_scan_ranges(&key) {
                    if storage.scan_length(&key).await.map_err(posting_read_error)? != size {
                        return Err(postings::corrupt("Spatial posting object length differs from its catalog receipt"));
                    }
                    None
                } else {
                    charge_bytes(&mut indexed_bytes, size, MAX_INDEXED_BYTES)?;
                    Some(storage.get_exact_cached_or(&key, size as usize, &context.memory).await.map_err(posting_read_error)?)
                };
                let whole = whole.as_ref();
                let key = &key;
                let memory = &context.memory;
                postings::candidates(
                    root,
                    partition,
                    &region.ranges,
                    &context.memory,
                    || context.check_cancelled(),
                    |range: Range<u64>| {
                        let charge = if whole.is_none() {
                            charge_bytes(&mut indexed_bytes, range.end - range.start, MAX_INDEXED_BYTES)
                        } else {
                            Ok(())
                        };
                        async move {
                            charge?;
                            let lease = if let Some(whole) = whole {
                                whole.slice(range.start as usize..range.end as usize).map_err(posting_read_error)?
                            } else {
                                storage.scan_range(key, range, memory).await.map_err(posting_read_error)?
                            };
                            Ok(Bytes::from_owner(lease))
                        }
                    },
                    |iid| candidates.insert(iid),
                ).await
            }.await;
            match result {
                Ok(()) => continue,
                Err(error) if error.code == postings::CORRUPT => { storage.evict(&key); }
                Err(error) => return Err(error),
            }
        }
        context.check_cancelled()?;
        let size = u64::try_from(trie.data_file_size).map_err(|_| postings::work_limit())?;
        charge_bytes(&mut candidates.unindexed_bytes, size, MAX_UNINDEXED_BYTES)?;
        let rows = rtdb_query::visit_trie_iids(
            &context,
            Arc::clone(&database.storage),
            &table.slug,
            &trie.key,
            MAX_UNINDEXED_ROWS - candidates.unindexed_rows,
            |iid| candidates.insert(iid),
        )
        .await?
        .ok_or_else(postings::work_limit)?;
        candidates.unindexed_rows += rows;
    }
    for segment in snapshot.iter_segments(&definition.table) {
        for batch in segment.batches() {
            candidates.extend_live(batch.view(), &context)?;
        }
    }
    let geometry = (region.location.is_some() || matches!(projection, Projection::Documents))
        .then_some(definition.geometry.as_str());
    let partition = definition.partition[..request.partition.len()].to_vec();
    let iids = candidates.finish()?;
    let cursor = rtdb_query::open_owned_spatial_refinement(
        Arc::clone(&context),
        owner,
        relation,
        geometry,
        &partition,
        request.valid_at,
        iids,
        projection,
        include_iid,
    )
    .await?;
    let partition_start = 1 + usize::from(geometry.is_some());
    let refined_geometry = geometry.is_some();
    let columns = DocumentColumns { geometry: definition.geometry, refined_geometry, partition };
    let Some(rtdb_query::OwnedSpatialScan { mut cursor, document_columns }) = cursor else {
        return Ok((None, columns));
    };
    let mut refined = 0;
    let geometry = refined_geometry.then_some(columns.geometry.as_str());
    let early_probe = probe.filter(|_| defer);
    while let Some((source, fallback)) = crate::probe::next_batch(&mut cursor, early_probe, geometry, &columns.partition).await? {
        let source = Arc::new(source);
        let batch = source.batch();
        let iids = if include_iid {
            let column = batch.column_by_name("_iid").ok_or_else(inconsistent_projection)?;
            Some(IidView::new(column.as_ref())?)
        } else {
            None
        };
        let mut geometry_workspace = None;
        for row in 0..batch.num_rows() {
            context.check_cancelled_at(row)?;
            if !partition_matches(batch, row, partition_start, &request.partition)? {
                continue;
            }
            refined += 1;
            if refined > MAX_REFINED_ROWS {
                return Err(postings::work_limit());
            }
            let id = read_owned_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
            if let PageOrder::Identity { after: Some(after) } = request.page
                && id <= after
            {
                continue;
            }
            let mut certified = false;
            let exact = if let Some(location_filter) = &region.location {
                let Some(bytes) =
                    binary_cell(batch.schema_ref().field(1), batch.column(1).as_ref(), row)?
                else {
                    continue;
                };
                if geometry_workspace.is_none() {
                    geometry_workspace = Some(context.memory.reserve(kernel::WORK_BYTES)?);
                }
                work.scoped(|work| {
                    let Some(location) = Location::decode_stored(bytes, definition.encoding, work)? else {
                        return Ok(None);
                    };
                    certified = certify_points && location.is_point_geometry();
                    location_filter.evaluate(region.predicate, &location, work)
                })?
            } else {
                Some(None)
            };
            if let Some(distance_m) = exact {
                visit(&source, fallback.as_ref(), row, id, iids.as_ref().map(|iids| iids.get(row)).transpose()?, distance_m, certified)?;
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

fn selection_position(selected: &[PageHit], id: EntityIdRef<'_>, distance: Option<f64>) -> usize {
    let mut integer = [0; 8];
    let key = id.handle_parts(&mut integer);
    let mut lower = 0;
    let mut upper = selected.len();
    while lower < upper {
        let mid = lower + (upper - lower) / 2;
        let hit = &selected[mid];
        let order = match (hit.hit.distance_m, distance) {
            (Some(left), Some(right)) => left.total_cmp(&right).then_with(|| hit.id.cmp_parts(key)),
            _ => hit.id.cmp_parts(key),
        };
        if order.is_lt() { lower = mid + 1; } else { upper = mid; }
    }
    lower
}

struct Candidates {
    ids: BTreeSet<Iid>,
    allowance: Arc<MemoryReservation>,
    unindexed_rows: usize,
    unindexed_bytes: u64,
}

impl Candidates {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self {
            ids: BTreeSet::new(),
            allowance: memory.reserve(0)?,
            unindexed_rows: 0,
            unindexed_bytes: 0,
        })
    }

    fn finish(self) -> Result<CandidateIids> {
        let Self { ids, allowance, .. } = self;
        let iids = CandidateIids::from_set(ids, allowance.budget())?;
        drop(allowance);
        Ok(iids)
    }

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

fn posting_read_error(error: rtdb_storage::Error) -> Anomaly {
    let error = Anomaly::from(error);
    if matches!(error.category, AnomalyCategory::Forbidden | AnomalyCategory::Busy | AnomalyCategory::Interrupted)
        || error.code == "rtdb.storage/read-lease-expired"
    {
        error
    } else {
        postings::corrupt(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryArray, ListArray, NullArray, StringArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use rtdb_columnar::{build_struct_array, entity_id_array, entity_id_type, read_value, to_field};
    use rtdb_types::{LogicalType, Value};

    #[test]
    fn spatial_candidate_handoff_releases_the_tree_allowance_at_the_cap() -> Result<()> {
        let memory = MemoryBudget::new(8 << 20);
        let mut candidates = Candidates::new(&memory)?;
        let iid = |index: usize| Iid((index as u128).to_be_bytes());
        for index in (0..MAX_CANDIDATES).rev() {
            candidates.insert_unindexed(iid(index))?;
        }
        candidates.insert(iid(0))?;
        assert_eq!(candidates.insert(iid(MAX_CANDIDATES)).unwrap_err().code, postings::work_limit().code);
        assert_eq!(candidates.insert_unindexed(iid(0)).unwrap_err().code, postings::work_limit().code);
        let tree_bytes = memory.used_bytes();

        let retained = candidates.finish()?;
        assert!(retained.iter().eq((0..MAX_CANDIDATES).map(iid)));
        let arrow_bytes = memory.used_bytes();
        assert!(arrow_bytes >= MAX_CANDIDATES * 16);
        assert!(arrow_bytes < tree_bytes);
        let shared = retained.clone();
        drop(retained);
        assert_eq!(shared.len(), MAX_CANDIDATES);
        assert_eq!(memory.used_bytes(), arrow_bytes);
        drop(shared);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn spatial_selection_orders_ties_by_the_typed_arrow_identity() -> Result<()> {
        let memory = MemoryBudget::new(64 << 10);
        let ids = entity_id_array([EntityIdRef::String("same"), EntityIdRef::Integer(3), EntityIdRef::Uuid([0; 16])])?;
        let schema = Arc::new(Schema::new(vec![to_field("_id", &entity_id_type())?]));
        let batch = RecordBatch::try_new(schema, vec![ids]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let batch = source.batch();
        let selected = [2.0, 2.0, 3.0].into_iter().enumerate().map(|(row, distance_m)| Ok(PageHit {
            id: EntityIdLocator::read_owned(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?,
            hit: SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: Some(distance_m) },
        })).collect::<Result<Vec<_>>>()?;

        assert_eq!(selection_position(&selected, EntityIdRef::Keyword("same"), Some(2.0)), 1);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(1.0)), 0);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(2.0)), 2);
        assert_eq!(selection_position(&selected, EntityIdRef::Integer(9), Some(4.0)), 3);
        Ok(())
    }

    #[test]
    fn spatial_gather_slices_contiguous_projected_rows_under_the_original_charge() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let ids = entity_id_array([
            EntityIdRef::Integer(0),
            EntityIdRef::Keyword("first"),
            EntityIdRef::String("second"),
            EntityIdRef::Integer(3),
        ])?;
        let winner = vec![42; 32 << 10];
        let payloads = BinaryArray::from(vec![&b"before"[..], winner.as_slice(), b"next", b"after"]);
        let winner_ptr = payloads.value(1).as_ptr();
        let schema = Arc::new(Schema::new(vec![
            to_field("_id", &entity_id_type())?,
            Field::new("geometry", DataType::Utf8, false),
            Field::new("_system_from", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("payload", DataType::Binary, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            ids,
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(TimestampMicrosecondArray::from(vec![100, 200, 300, 400]).with_timezone("UTC")),
            Arc::new(payloads),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("present"), Some("d")])),
        ]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let hit = |row| SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: None };
        let projection = [0, 3, 4, 2];
        let output = gather_selected(&[hit(1), hit(2)], &projection, &memory)?.unwrap();
        assert!(Arc::ptr_eq(output.reservation(), source.reservation()));
        let batch = output.batch();
        let ids = batch.column(0).as_ref();
        let id_field = batch.schema_ref().field(0);
        assert_eq!(read_owned_entity_id(id_field, ids, 0)?, EntityIdRef::Keyword("first"));
        assert_eq!(read_owned_entity_id(id_field, ids, 1)?, EntityIdRef::String("second"));
        assert!(batch.column_by_name("geometry").is_none());
        let payloads = batch.column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(payloads.value(0).as_ptr(), winner_ptr);
        assert_eq!(payloads.value(1), b"next");
        let notes = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(notes.iter().collect::<Vec<_>>(), [None, Some("present")]);
        let revisions = batch.column(3).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(revisions.values().as_ref(), &[200, 300]);

        let copied = gather_selected(&[hit(3), hit(1), hit(3)], &projection, &memory)?.unwrap();
        assert!(!Arc::ptr_eq(copied.reservation(), source.reservation()));
        let payloads = copied.batch().column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(payloads.value(0), b"after");
        assert_eq!(payloads.value(1), winner.as_slice());
        assert_eq!(payloads.value(2), b"after");
        drop(copied);

        let retained = Arc::clone(batch.column(1));
        let charged = source.reservation().bytes();
        drop(source);
        drop(output);
        assert_eq!(memory.used_bytes(), charged);
        drop(retained);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn spatial_gather_retains_compact_list_and_struct_buffers() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let payload = vec![42; 32 << 10];
        let values = BinaryArray::from(vec![payload.as_slice()]);
        let payload_ptr = values.value(0).as_ptr();
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::Binary, false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 1, 1])),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let fields = vec![
            Field::new("list", lists.data_type().clone(), true),
            to_field("missing", &LogicalType::Nothing)?,
            to_field("explicit_null", &LogicalType::Null)?,
        ];
        let document = build_struct_array(fields.into(), &[
            Arc::new(lists), Arc::new(NullArray::new(3)), Arc::new(NullArray::new(3)),
        ], 3, None)?;
        let schema = Arc::new(Schema::new(vec![Field::new("document", document.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![document]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let charged = source.reservation().bytes();
        assert!(charged > MAX_UNUSED_SLICE_BYTES);
        let hits = [0, 1, 2].map(|row| SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: None });
        let output = gather_selected(&hits, &[0], &memory)?.unwrap();
        assert!(Arc::ptr_eq(output.reservation(), source.reservation()));

        let document = output.batch().column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let lists = document.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let retained = lists.value(0);
        let values = retained.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(values.value(0).as_ptr(), payload_ptr);
        assert!(lists.is_null(1));
        assert!(!lists.is_null(2));
        assert_eq!(lists.value_length(2), 0);
        let field = output.batch().schema_ref().field(0);
        let Value::Struct(cells) = read_value(field, document, 1)? else { panic!("Expected a document"); };
        assert_eq!(cells.get("missing"), Some(&Value::Undefined));
        assert_eq!(cells.get("explicit_null"), Some(&Value::Null));

        drop(hits);
        drop(source);
        drop(output);
        assert_eq!(memory.used_bytes(), charged);
        drop(retained);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn spatial_gather_detaches_list_children_outside_the_visible_extent() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let unused = vec![42; 256 << 10];
        let values = BinaryArray::from(vec![unused.as_slice(), b"a", b"b"]);
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::Binary, false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
            Arc::new(values),
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("list", lists.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(lists)]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let hit = |row| SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: None };
        let slice = gather_selected(&[hit(1), hit(2)], &[0], &memory)?.unwrap();
        let full = gather_selected(&[hit(0), hit(1), hit(2)], &[0], &memory)?.unwrap();
        assert!(!Arc::ptr_eq(slice.reservation(), source.reservation()));
        assert!(!Arc::ptr_eq(full.reservation(), source.reservation()));
        drop(source);
        assert_eq!(memory.used_bytes(), slice.reservation().bytes() + full.reservation().bytes());
        assert!(memory.used_bytes() < unused.len());

        let slice_lists = slice.batch().column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let full_lists = full.batch().column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(full_lists.is_null(0));
        assert_eq!(&full_lists.value_offsets()[1..], slice_lists.value_offsets());
        let values = slice_lists.values().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(&b"a"[..]), Some(&b"b"[..])]);
        Ok(())
    }

    #[test]
    fn spatial_gather_copies_a_contiguous_slice_that_would_pin_a_large_unselected_value() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let unused = vec![42; 256 << 10];
        let schema = Arc::new(Schema::new(vec![Field::new("payload", DataType::Binary, false)]));
        let payloads = BinaryArray::from(vec![unused.as_slice(), b"a", b"b"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(payloads)]).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let hits = [1, 2].map(|row| SelectedHit { source: Arc::clone(&source), probe: None, row, distance_m: None });
        let output = gather_selected(&hits, &[0], &memory)?.unwrap();
        assert!(!Arc::ptr_eq(output.reservation(), source.reservation()));
        drop(hits);
        drop(source);
        assert_eq!(memory.used_bytes(), output.reservation().bytes());
        assert!(memory.used_bytes() < unused.len());
        let payloads = output.batch().column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(payloads.value(0), b"a");
        assert_eq!(payloads.value(1), b"b");
        Ok(())
    }
}
