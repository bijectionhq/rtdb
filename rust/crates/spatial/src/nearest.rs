use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, RecordBatch, UInt32Array,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{
    EntityIdRef, IidView, MemoryBudget, MemoryReservation, OwnedBatch, read_owned_entity_id, retain_buffer,
};
use rtdb_query::{OwnedSpatialScan, QueryContext, open_owned_spatial_scan};
use rtdb_types::{Anomaly, Iid, OwnedRelation, Result, Timestamp};

use crate::{
    NearestPosition, Page, PageOrder, Predicate, PreparedRegion, Projection, Request, kernel,
    postings::work_limit,
    read::{DocumentColumns, SelectedHit, empty_projected_documents, gather_selected, visit_matches},
};

pub struct PreparedNearest {
    ranking: RankedMatches,
    relation: OwnedRelation,
    valid_at: Timestamp,
}

impl PreparedNearest {
    pub fn relation(&self) -> &OwnedRelation {
        &self.relation
    }

    pub async fn prepare(
        context: Arc<QueryContext>,
        owner: &str,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        request: Request<'_>,
        after_radius: Option<f64>,
    ) -> Result<Self> {
        let invalid = || kernel::invalid("A prepared ranking requires an initial nearest request");
        let Predicate::Nearest { radius_m, .. } = request.predicate else { return Err(invalid()); };
        if !matches!(request.page, PageOrder::NearestOnce) || !(1..=128).contains(&request.limit) {
            return Err(invalid());
        }
        if after_radius.is_some_and(|after| !after.is_finite() || after < 0.0 || radius_m.is_some_and(|upper| after > upper)) {
            return Err(kernel::invalid("Invalid spatial rank window"));
        }
        let region = PreparedRegion::new(&request.region, request.predicate, &context.memory)?;
        let mut ranking = RankingBuilder::new(&context.memory)?;
        visit_matches(
            Arc::clone(&context),
            owner,
            relation,
            path,
            &region,
            &request,
            Projection::Identities,
            true,
            None,
            false,
            |_, _, _, id, iid, distance, _| {
                let distance = distance.ok_or_else(inconsistent_ranking)?;
                if after_radius.is_some_and(|after| distance <= after) { return Ok(()); }
                ranking.push(id, iid.ok_or_else(inconsistent_ranking)?, distance)
            },
        )
        .await?;
        let ranking = ranking.finish(|| context.check_cancelled())?;
        Ok(Self {
            ranking,
            relation: relation.clone(),
            valid_at: request.valid_at,
        })
    }

    pub async fn page(
        &self,
        context: Arc<QueryContext>,
        owner: &str,
        after: Option<NearestPosition<'_>>,
        limit: usize,
        projection: Projection<'_>,
    ) -> Result<Page> {
        if !(1..=128).contains(&limit) {
            return Err(kernel::invalid("Invalid spatial page size"));
        }
        let batch = self.ranking.batch.batch();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(inconsistent_ranking)?;
        let distances = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(inconsistent_ranking)?;
        let row_at = |rank| self.ranking.row(rank);
        let start = seek(keys, distances, row_at, after)?;
        let end = keys.len().min(start + limit);
        let more = end < keys.len();
        let next_distance_m = more.then(|| distances.value(row_at(end)));
        let _selection = context.memory.reserve(4096 + (end - start) * 256)?;
        let positions = (start..end)
            .map(|rank| Ok((EntityIdRef::from_handle_bytes(keys.value(row_at(rank)))?, rank)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        if positions.len() != end - start {
            return Err(inconsistent_ranking());
        }
        let stored_iids = IidView::new(batch.column(2).as_ref())?;
        let iids = (start..end)
            .map(|rank| stored_iids.get(row_at(rank)))
            .collect::<Result<_>>()?;
        let cursor = open_owned_spatial_scan(
            Arc::clone(&context),
            owner,
            &self.relation,
            None,
            &[],
            self.valid_at,
            iids,
            projection,
            false,
        )
        .await?;
        let mut selected = (start..end).map(|_| None).collect::<Vec<_>>();
        let mut layout = None;
        if let Some(OwnedSpatialScan {
            mut cursor,
            document_columns,
        }) = cursor
        {
            while let Some(source) = cursor.next_batch().await? {
                let source = Arc::new(source);
                let batch = source.batch();
                for row in 0..batch.num_rows() {
                    context.check_cancelled_at(row)?;
                    let id =
                        read_owned_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
                    let Some(&rank) = positions.get(&id) else {
                        continue;
                    };
                    let destination = &mut selected[rank - start];
                    if destination.is_some() {
                        return Err(inconsistent_ranking());
                    }
                    *destination = Some(SelectedHit {
                        source: Arc::clone(&source),
                        probe: None,
                        row,
                        distance_m: Some(distances.value(row_at(rank))),
                    });
                }
            }
            layout = Some((cursor.runtime_schema().clone(), document_columns));
        }
        let selected = selected
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(inconsistent_ranking)?;
        let rows = if selected.is_empty() {
            empty_projected_documents(
                projection,
                layout
                    .as_ref()
                    .map(|(schema, columns)| (schema, columns.as_slice())),
                &context.memory,
            )?
        } else {
            let ids = [0];
            let columns = if projection.has_documents() {
                layout
                    .as_ref()
                    .ok_or_else(inconsistent_ranking)?
                    .1
                    .as_slice()
            } else {
                &ids
            };
            gather_selected(&selected, columns, &context.memory)?
        };
        let count = end - start;
        let distance_bytes = count * size_of::<f64>();
        let (distances, distance_reservation) =
            if self.ranking.order.is_none() && distances.get_buffer_memory_size() <= distance_bytes {
                (
                    distances.slice(start, count),
                    Arc::clone(self.ranking.batch.reservation()),
                )
            } else {
                let reservation = context
                    .memory
                    .reserve(size_of::<Float64Array>() + distance_bytes)?;
                let values = Buffer::from_vec((start..end).map(|rank| distances.value(row_at(rank))).collect::<Vec<_>>());
                let distances = Float64Array::new(retain_buffer(&values, &reservation).into(), None);
                (distances, reservation)
            };
        Ok(Page {
            more,
            next_distance_m,
            rows,
            includes_documents: projection.has_documents(),
            distances,
            _distance_reservation: distance_reservation,
            probe: None,
        })
    }
}

pub(super) async fn gather_range(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    valid_at: Timestamp,
    projection: Projection<'_>,
    document_columns: &DocumentColumns,
    selection: &[(EntityIdRef<'_>, Iid)],
    more: bool,
    probe: Option<crate::Probe<'_>>,
    defer: bool,
) -> Result<Page> {
    let reservation = context.memory.reserve(4096 + selection.len() * 256)?;
    let positions = selection.iter().enumerate().map(|(rank, (id, _))| (*id, rank))
        .collect::<BTreeMap<_, _>>();
    if positions.len() != selection.len() { return Err(inconsistent_ranking()); }
    let iids = selection.iter().map(|(_, iid)| *iid).collect();
    let full = matches!(projection, Projection::Documents);
    let geometry = (full || probe.is_some() && document_columns.refined_geometry)
        .then_some(document_columns.geometry.as_str());
    let partition = if full || probe.is_some() { document_columns.partition.as_slice() } else { &[] };
    let cursor = open_owned_spatial_scan(
        Arc::clone(&context), owner, relation, geometry, partition, valid_at, iids, projection, false,
    ).await?;
    let mut selected = (0..selection.len()).map(|_| None).collect::<Vec<_>>();
    let mut layout = None;
    if let Some(OwnedSpatialScan { mut cursor, document_columns }) = cursor {
        let early_probe = probe.filter(|_| defer);
        while let Some((source, fallback)) = crate::probe::next_batch(&mut cursor, early_probe, geometry, partition).await? {
            let source = Arc::new(source);
            let batch = source.batch();
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let id = read_owned_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
                let Some(&rank) = positions.get(&id) else { continue; };
                let destination = &mut selected[rank];
                if destination.is_some() { return Err(inconsistent_ranking()); }
                *destination = Some(SelectedHit {
                    distance_m: None,
                    source: Arc::clone(&source),
                    probe: fallback.clone(),
                    row,
                });
            }
        }
        layout = Some((cursor.runtime_schema().clone(), document_columns));
    }
    let selected = selected.into_iter().collect::<Option<Vec<_>>>().ok_or_else(inconsistent_ranking)?;
    let mut evidence = probe.map(|probe| crate::probe::Evidence::new(
        probe.certify_point && probe.point_column.is_some() && !selected.is_empty(),
    ));
    let rows = if selected.is_empty() {
        empty_projected_documents(
            projection, layout.as_ref().map(|(schema, columns)| (schema, columns.as_slice())),
            &context.memory,
        )?
    } else {
        let ids = [0];
        let columns = if projection.has_documents() {
            layout.as_ref().ok_or_else(inconsistent_ranking)?.1.as_slice()
        } else {
            &ids
        };
        if let (Some(probe), Some(evidence)) = (probe, evidence.as_mut()) {
            let columns = crate::probe::summarize(&selected, columns, probe, evidence, &context.memory)?;
            crate::probe::gather(&selected, &columns, &context.memory)?
        } else {
            gather_selected(&selected, columns, &context.memory)?
        }
    };
    let distance_reservation = context.memory.reserve(4096 + selection.len() * 16)?;
    let distances = Float64Array::new_null(selection.len());
    drop(reservation);
    Ok(Page {
        more,
        next_distance_m: None,
        rows,
        includes_documents: projection.has_documents(),
        distances,
        _distance_reservation: distance_reservation,
        probe: evidence,
    })
}

fn seek(
    keys: &BinaryArray,
    distances: &Float64Array,
    row_at: impl Fn(usize) -> usize,
    after: Option<NearestPosition<'_>>,
) -> Result<usize> {
    let Some(after) = after else {
        return Ok(0);
    };
    if !after.distance_m.is_finite() || after.distance_m < 0.0 {
        return Err(kernel::invalid("Invalid nearest continuation distance"));
    }
    let mut integer = [0; 8];
    let bound = after.id.handle_parts(&mut integer);
    let mut lower = 0;
    let mut upper = keys.len();
    while lower < upper {
        let mid = lower + (upper - lower) / 2;
        let row = row_at(mid);
        let order = distances.value(row).total_cmp(&after.distance_m);
        let order = if order.is_eq() {
            let key = keys.value(row);
            (key[0], &key[1..]).cmp(&bound)
        } else {
            order
        };
        match order {
            std::cmp::Ordering::Less => lower = mid + 1,
            std::cmp::Ordering::Greater => upper = mid,
            std::cmp::Ordering::Equal => return Ok(mid + 1),
        }
    }
    Err(kernel::invalid(
        "Nearest continuation does not name a ranked hit",
    ))
}

const RANKING_OVERHEAD: usize = 16 << 10;
const IID_BYTES: usize = 16;
const RANKING_ROW_BYTES: usize = size_of::<i32>() + size_of::<f64>() + IID_BYTES;

#[derive(Debug)]
struct RankedMatches {
    order: Option<UInt32Array>,
    batch: OwnedBatch,
}

impl RankedMatches {
    fn row(&self, rank: usize) -> usize {
        self.order.as_ref().map_or(rank, |order| order.value(rank) as usize)
    }
}

struct RankingBuilder {
    keys: Vec<u8>,
    offsets: Vec<i32>,
    distances: Vec<f64>,
    iids: Vec<u8>,
    work_key_bytes: usize,
    memory: Arc<MemoryReservation>,
    ordered: bool,
}

impl RankingBuilder {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        let memory = memory.reserve(RANKING_OVERHEAD)?;
        Ok(Self {
            keys: Vec::new(),
            offsets: vec![0],
            distances: Vec::new(),
            iids: Vec::new(),
            work_key_bytes: 0,
            memory,
            ordered: true,
        })
    }

    fn push(&mut self, id: EntityIdRef<'_>, iid: Iid, distance: f64) -> Result<()> {
        if !distance.is_finite() || distance < 0.0 {
            return Err(inconsistent_ranking());
        }
        let work_key_bytes = self
            .work_key_bytes
            .checked_add(id.owned_payload_bytes().max(16))
            .and_then(|bytes| bytes.checked_add(1))
            .filter(|&bytes| i32::try_from(bytes).is_ok())
            .ok_or_else(work_limit)?;

        let mut integer = [0; 8];
        let (tag, payload) = id.handle_parts(&mut integer);
        let end = self
            .keys
            .len()
            .checked_add(1)
            .and_then(|len| len.checked_add(payload.len()))
            .ok_or_else(work_limit)?;
        let offset = i32::try_from(end).map_err(|_| work_limit())?;
        let next_count = self.offsets.len();
        let ordered = self.ordered && self.distances.last().is_none_or(|previous| {
            previous.total_cmp(&distance).then_with(|| {
                let key = &self.keys[self.offsets[next_count - 2] as usize..];
                (key[0], &key[1..]).cmp(&(tag, payload))
            }).is_lt()
        });
        let rows_capacity = next_count
            .checked_next_power_of_two()
            .ok_or_else(work_limit)?;
        let order_bytes = if ordered { 0 } else { next_count.checked_mul(size_of::<u32>()).ok_or_else(work_limit)? };
        let fixed = rows_capacity
            .checked_mul(RANKING_ROW_BYTES)
            .and_then(|bytes| bytes.checked_add(order_bytes))
            .and_then(|bytes| bytes.checked_add(RANKING_OVERHEAD + 2 * size_of::<i32>()))
            .ok_or_else(work_limit)?;
        let current = self.keys.capacity();
        let minimum = current.max(end);
        let limit = (usize::MAX - fixed).min(i32::MAX as usize);
        if minimum > limit {
            return Err(work_limit());
        }
        let preferred = if end > current {
            end.max(current.saturating_mul(2)).min(limit)
        } else {
            current
        };
        let memory = Arc::get_mut(&mut self.memory).expect("ranking owns its reservation");
        let mut keys_capacity = preferred;
        if fixed + preferred > memory.bytes()
            && let Err(error) = memory.grow_to(fixed + preferred)
        {
            if preferred == minimum || error.code != "rtdb/memory-budget-exceeded" {
                return Err(error);
            }
            memory.grow_to(fixed + minimum)?;
            keys_capacity = minimum;
        }

        self.keys.reserve_exact(keys_capacity - self.keys.len());
        self.offsets
            .reserve_exact(rows_capacity + 1 - self.offsets.len());
        self.distances
            .reserve_exact(rows_capacity - self.distances.len());
        self.iids
            .reserve_exact(rows_capacity * IID_BYTES - self.iids.len());
        self.keys.push(tag);
        self.keys.extend_from_slice(payload);
        self.offsets.push(offset);
        self.distances.push(distance);
        self.iids.extend_from_slice(iid.as_bytes());
        self.work_key_bytes = work_key_bytes;
        self.ordered = ordered;
        Ok(())
    }

    fn finish(self, check: impl Fn() -> Result<()>) -> Result<RankedMatches> {
        check()?;
        let Self {
            // Local bindings drop in reverse order, so the buffers release before their charge.
            mut memory,
            mut keys,
            mut offsets,
            mut distances,
            mut iids,
            ordered,
            ..
        } = self;
        let count = u32::try_from(distances.len()).map_err(|_| work_limit())?;
        let order = if ordered {
            let compact = [
                (keys.capacity(), keys.len(), 1),
                (offsets.capacity(), offsets.len(), size_of::<i32>()),
                (distances.capacity(), distances.len(), size_of::<f64>()),
                (iids.capacity(), iids.len(), 1),
            ].into_iter().filter(|(capacity, len, _)| capacity > len)
                .map(|(_, len, width)| len * width).max().unwrap_or(0);
            let _compact = memory.budget().reserve(compact)?;
            keys = keys.into_boxed_slice().into_vec();
            offsets = offsets.into_boxed_slice().into_vec();
            distances = distances.into_boxed_slice().into_vec();
            iids = iids.into_boxed_slice().into_vec();
            None
        } else {
            let mut positions = (0..count).collect::<Vec<_>>();
            let key_at = |row: usize| &keys[offsets[row] as usize..offsets[row + 1] as usize];
            positions.sort_unstable_by(|&left, &right| {
                let (left, right) = (left as usize, right as usize);
                distances[left]
                    .total_cmp(&distances[right])
                    .then_with(|| key_at(left).cmp(key_at(right)))
            });
            Some(UInt32Array::from(positions))
        };
        check()?;
        let keys = BinaryArray::try_new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from_vec(keys),
            None,
        )
        .map_err(ranking_arrow_error)?;
        let distances = Float64Array::new(distances.into(), None);
        let iids = FixedSizeBinaryArray::try_new(IID_BYTES as i32, Buffer::from_vec(iids), None)
            .map_err(ranking_arrow_error)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_entity_key", DataType::Binary, false),
            Field::new("distance_m", DataType::Float64, false),
            Field::new("_iid", DataType::FixedSizeBinary(IID_BYTES as i32), false),
        ]));
        let columns: Vec<ArrayRef> = vec![Arc::new(keys), Arc::new(distances), Arc::new(iids)];
        let batch = RecordBatch::try_new(schema, columns).map_err(ranking_arrow_error)?;
        let retained = batch.get_array_memory_size() + order.as_ref().map_or(0, |order| order.get_array_memory_size());
        Arc::get_mut(&mut memory)
            .expect("ranking owns its reservation")
            .shrink_to(retained)?;
        Ok(RankedMatches { order, batch: OwnedBatch::with_reservation(batch, memory)? })
    }
}

fn ranking_arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    Anomaly::fault("rtdb.spatial/ranking", error.to_string())
}

fn inconsistent_ranking() -> Anomaly {
    Anomaly::fault(
        "rtdb.spatial/ranking",
        "A ranked spatial hit does not match its held document",
    )
}

#[cfg(test)]
#[test]
fn rank_continuation_uses_the_exact_typed_identity_among_equal_distances() {
    let memory = MemoryBudget::new(1 << 20);
    let mut builder = RankingBuilder::new(&memory).unwrap();
    let later = f64::from_bits(1.0_f64.to_bits() + 1);
    for (row, (id, distance)) in [
        (EntityIdRef::Integer(1), 1.0),
        (EntityIdRef::String("same"), 1.0),
        (EntityIdRef::Integer(-3), 1.0),
        (EntityIdRef::Keyword("same"), 1.0),
        (EntityIdRef::String("later"), later),
        (EntityIdRef::Uuid([0; 16]), 1.0),
    ]
    .into_iter()
    .enumerate()
    {
        builder.push(id, Iid([row as u8; 16]), distance).unwrap();
    }
    let source = builder.keys.as_ptr();
    let ranking = builder.finish(|| Ok(())).unwrap();
    assert!(ranking.order.is_some());
    let keys = ranking.batch
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(keys.value_data().as_ptr(), source);
    let distances = ranking.batch
        .batch()
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let iids = IidView::new(ranking.batch.batch().column(2).as_ref()).unwrap();
    for (rank, (id, source_row)) in [
        (EntityIdRef::String("same"), 1),
        (EntityIdRef::Keyword("same"), 3),
        (EntityIdRef::Integer(-3), 2),
        (EntityIdRef::Integer(1), 0),
        (EntityIdRef::Uuid([0; 16]), 5),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            EntityIdRef::from_handle_bytes(keys.value(ranking.row(rank))).unwrap(),
            id
        );
        assert_eq!(iids.get(ranking.row(rank)).unwrap(), Iid([source_row; 16]));
        assert_eq!(
            seek(
                keys,
                distances,
                |rank| ranking.row(rank),
                Some(NearestPosition {
                    id,
                    distance_m: 1.0
                })
            )
            .unwrap(),
            rank + 1
        );
        assert!(
            seek(
                keys,
                distances,
                |rank| ranking.row(rank),
                Some(NearestPosition {
                    id,
                    distance_m: later
                })
            )
            .is_err()
        );
    }
    assert_eq!(distances.value(ranking.row(5)).to_bits(), later.to_bits());
}

#[cfg(test)]
#[test]
fn already_ranked_rows_retain_compact_buffers_until_the_last_slice_drops() -> Result<()> {
    let memory = MemoryBudget::new(1 << 20);
    for count in [2, 3] {
        let mut builder = RankingBuilder::new(&memory)?;
        for row in 0..count {
            builder.push(EntityIdRef::Integer(row as i64), Iid([row as u8; 16]), 1.0)?;
        }
        let source = builder.keys.as_ptr();
        let ranking = builder.finish(|| Ok(()))?;
        assert!(ranking.order.is_none());
        let batch = ranking.batch.batch();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let distances = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let iids = IidView::new(batch.column(2).as_ref())?;
        if count == 2 {
            assert_eq!(keys.value_data().as_ptr(), source);
        }
        for row in 0..count {
            assert_eq!(
                EntityIdRef::from_handle_bytes(keys.value(row))?,
                EntityIdRef::Integer(row as i64)
            );
            assert_eq!(iids.get(row)?, Iid([row as u8; 16]));
        }
        let retained = size_of::<BinaryArray>()
            + size_of::<Float64Array>()
            + size_of::<FixedSizeBinaryArray>()
            + count * RANKING_ROW_BYTES
            + size_of::<i32>()
            + keys.value_data().len();
        assert_eq!(memory.used_bytes(), retained);
        let slice = distances.slice(1, 1);
        drop(ranking);
        assert_eq!(slice.value(0), 1.0);
        assert_eq!(memory.used_bytes(), retained);
        drop(slice);
        assert_eq!(memory.used_bytes(), 0);
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn rank_key_growth_falls_back_to_exact_need_and_refuses_without_appending() -> Result<()> {
    let limit = RANKING_OVERHEAD
        + 2 * size_of::<i32>()
        + 2 * (RANKING_ROW_BYTES + size_of::<u32>())
        + 8;
    let memory = MemoryBudget::new(limit);
    let mut builder = RankingBuilder::new(&memory)?;
    builder.push(EntityIdRef::String("abcde"), Iid::MIN, 2.0)?;
    builder.push(EntityIdRef::String("z"), Iid::MAX, 1.0)?;
    assert_eq!(memory.used_bytes(), limit);
    assert_eq!(
        builder
            .push(EntityIdRef::String("zz"), Iid::MIN, 3.0)
            .unwrap_err()
            .code,
        "rtdb/memory-budget-exceeded",
    );
    let ranking = builder.finish(|| Ok(()))?;
    assert_eq!(ranking.batch.batch().num_rows(), 2);
    assert_eq!(
        IidView::new(ranking.batch.batch().column(2).as_ref())?.get(ranking.row(0))?,
        Iid::MAX
    );
    drop(ranking);
    assert_eq!(memory.used_bytes(), 0);
    Ok(())
}

#[cfg(test)]
#[test]
fn rank_cancelled_after_sort_releases_its_buffer_reservation() -> Result<()> {
    let memory = MemoryBudget::new(1 << 20);
    let mut builder = RankingBuilder::new(&memory)?;
    builder.push(EntityIdRef::Integer(1), Iid::MIN, 1.0)?;
    let checked = std::cell::Cell::new(false);
    let error = builder
        .finish(|| {
            if checked.replace(true) {
                assert!(memory.used_bytes() > 0);
                Err(crate::postings::cancelled())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(error.code, crate::postings::cancelled().code);
    assert_eq!(memory.used_bytes(), 0);
    Ok(())
}
