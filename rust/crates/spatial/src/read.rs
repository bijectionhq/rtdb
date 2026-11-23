use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use arrow_array::RecordBatch;
use geometry::{GeometryWork, KeyRange};
use rtdb_columnar::{
    BatchBuilder, EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch, binary_cell,
    copy_selected_cells, estimate_copy_cell_bytes, read_entity_id,
};
use rtdb_format::data::EventBatch;
use rtdb_plan::Catalog;
use rtdb_query::QueryContext;
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, Iid, OwnedRelation, Result, Timestamp};

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
    pub after: Option<EntityIdRef<'a>>,
    pub limit: usize,
}

pub struct Hit {
    pub id: EntityId,
    pub distance_m: Option<f64>,
    _reservation: Arc<MemoryReservation>,
}

struct SelectedHit {
    hit: Hit,
    document: Option<(Arc<OwnedBatch>, usize)>,
}

pub struct Page {
    pub hits: Vec<Hit>,
    pub more: bool,
    documents: Option<OwnedBatch>,
    _reservation: Arc<MemoryReservation>,
}

impl Page {
    pub fn documents(&self) -> Option<&OwnedBatch> {
        self.documents.as_ref()
    }
}

fn gather_documents(
    hits: &[SelectedHit],
    projection: Projection<'_>,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<OwnedBatch>> {
    let Some((first, _)) = hits.first().and_then(|hit| hit.document.as_ref()) else {
        return Ok(None);
    };
    let schema = first.batch().schema_ref();
    let mut sources: Vec<&OwnedBatch> = Vec::new();
    let mut positions = Vec::with_capacity(hits.len());
    for hit in hits {
        let (source, row) = hit
            .document
            .as_ref()
            .expect("one spatial projection per read");
        let source = source.as_ref();
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
        positions.push((index, *row));
    }
    let indices: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| projection.includes(field.name()).then_some(index))
        .collect();
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
    let schema = if indices.len() == schema.fields().len() {
        Arc::clone(schema)
    } else {
        Arc::new(schema.project(&indices).map_err(projection_error)?)
    };
    let batch = RecordBatch::try_new(schema, columns).map_err(projection_error)?;
    output.finish(batch).map(Some)
}

pub async fn read_held(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
    request: Request<'_>,
    projection: Projection<'_>,
) -> Result<Page> {
    if !(1..=128).contains(&request.limit)
        || (matches!(request.predicate, Predicate::Nearest { .. }) && request.after.is_some())
    {
        return Err(kernel::invalid(
            "Invalid spatial page size or nearest continuation",
        ));
    }
    let reservation = context
        .memory
        .reserve(request.limit * std::mem::size_of::<Hit>())?;
    let definition =
        definition::held_definition(Arc::clone(&context), owner, relation, path).await?;
    if request.partition.len() > definition.partition.len() {
        return Err(kernel::invalid(
            "Spatial read binds more than its declared equality partition",
        ));
    }
    let _geometry_memory = context.memory.reserve(kernel::WORK_BYTES)?;
    let mut work = GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
    let region = if matches!(request.predicate, Predicate::All) {
        if !request.region.is_empty() {
            return Err(kernel::invalid(
                "An unrestricted spatial read takes no region",
            ));
        }
        None
    } else {
        let region = Location::decode(&request.region, &mut work)?;
        request.predicate.validate_region(&region)?;
        Some(region)
    };
    let ranges = match &region {
        Some(region) => work.scoped(|work| request.predicate.cover(region, work))?,
        None => None,
    }
    .unwrap_or_else(|| {
        vec![KeyRange {
            start: i64::MIN,
            end: i64::MAX,
        }]
    });
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
                        &ranges,
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
    let geometry = (region.is_some() || matches!(projection, Projection::Documents))
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
    )
    .await?;
    let Some(mut cursor) = cursor else {
        return Ok(Page {
            hits: Vec::new(),
            more: false,
            documents: None,
            _reservation: reservation,
        });
    };
    let _selection = context
        .memory
        .reserve(request.limit * std::mem::size_of::<SelectedHit>())?;
    let mut selected = Vec::<SelectedHit>::with_capacity(request.limit);
    let mut more = false;
    let mut refined = 0;
    let partition_start = 1 + usize::from(geometry.is_some());
    while let Some(source) = cursor.next_batch().await? {
        let source = Arc::new(source);
        let batch = source.batch();
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
            if request.after.is_some_and(|after| id <= after) {
                continue;
            }
            let exact = if let Some(region) = &region {
                let Some(bytes) =
                    binary_cell(batch.schema_ref().field(1), batch.column(1).as_ref(), row)?
                else {
                    continue;
                };
                work.scoped(|work| {
                    let location = Location::decode(bytes, work)?;
                    request.predicate.evaluate(&location, region, work)
                })?
            } else {
                Some(None)
            };
            if let Some(distance_m) = exact {
                let index = selected
                    .partition_point(|previous| order(&previous.hit, id, distance_m).is_lt());
                if index < request.limit {
                    if selected.len() == request.limit {
                        selected.pop();
                        more = true;
                    }
                    let text_bytes = match id {
                        EntityIdRef::String(text) | EntityIdRef::Keyword(text) => text.len(),
                        _ => 0,
                    };
                    let reservation = context.memory.reserve(text_bytes.saturating_add(128))?;
                    let document = projection
                        .has_documents()
                        .then(|| (Arc::clone(&source), row));
                    let hit = Hit {
                        id: id.into_owned(),
                        distance_m,
                        _reservation: reservation,
                    };
                    selected.insert(index, SelectedHit { hit, document });
                } else {
                    more = true;
                }
            }
        }
    }
    if matches!(request.predicate, Predicate::Nearest { .. }) {
        more = false;
    }
    context.check_cancelled()?;
    let documents = gather_documents(&selected, projection, &context.memory)?;
    let hits = selected.into_iter().map(|selected| selected.hit).collect();
    Ok(Page {
        hits,
        more,
        documents,
        _reservation: reservation,
    })
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
        (Some(left), Some(right)) => left
            .total_cmp(&right)
            .then_with(|| EntityIdRef::from(&hit.id).cmp(&id)),
        _ => EntityIdRef::from(&hit.id).cmp(&id),
    }
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
