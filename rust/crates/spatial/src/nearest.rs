use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, UInt32Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, Float64Builder},
};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{
    EntityIdRef, IidView, MemoryBudget, MemoryReservation, OwnedBatch, read_entity_id,
};
use rtdb_query::{OwnedSpatialScan, QueryContext, open_owned_spatial_scan};
use rtdb_types::{Anomaly, Iid, OwnedRelation, Result, Timestamp};

use crate::{
    NearestPosition, Page, PageOrder, Predicate, PreparedRegion, Projection, Request, kernel,
    postings::work_limit,
    read::{SelectedHit, empty_projected_documents, gather_selected, visit_matches},
};

pub struct PreparedNearest {
    ranking: OwnedBatch,
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
            |_, _, id, iid, distance| {
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
        let batch = self.ranking.batch();
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
        let start = seek(keys, distances, after)?;
        let end = keys.len().min(start + limit);
        let more = end < keys.len();
        let next_distance_m = more.then(|| distances.value(end));
        let _selection = context.memory.reserve(4096 + (end - start) * 256)?;
        let positions = (start..end)
            .map(|row| Ok((EntityIdRef::from_handle_bytes(keys.value(row))?, row)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        if positions.len() != end - start {
            return Err(inconsistent_ranking());
        }
        let stored_iids = IidView::new(batch.column(2).as_ref())?;
        let iids = (start..end)
            .map(|row| stored_iids.get(row))
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
                        read_entity_id(batch.schema_ref().field(0), batch.column(0).as_ref(), row)?;
                    let Some(&rank) = positions.get(&id) else {
                        continue;
                    };
                    let destination = &mut selected[rank - start];
                    if destination.is_some() {
                        return Err(inconsistent_ranking());
                    }
                    *destination = Some(SelectedHit {
                        source: Arc::clone(&source),
                        row,
                        distance_m: Some(distances.value(rank)),
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
        Ok(Page {
            more,
            next_distance_m,
            rows,
            includes_documents: projection.has_documents(),
            distances: distances.slice(start, end - start),
            _distance_reservation: Arc::clone(self.ranking.reservation()),
        })
    }
}

fn seek(
    keys: &BinaryArray,
    distances: &Float64Array,
    after: Option<NearestPosition<'_>>,
) -> Result<usize> {
    let Some(after) = after else {
        return Ok(0);
    };
    if !after.distance_m.is_finite() || after.distance_m < 0.0 {
        return Err(kernel::invalid("Invalid nearest continuation distance"));
    }
    let mut lower = 0;
    let mut upper = keys.len();
    while lower < upper {
        let mid = lower + (upper - lower) / 2;
        let order = distances.value(mid).total_cmp(&after.distance_m);
        let order = if order.is_eq() {
            EntityIdRef::from_handle_bytes(keys.value(mid))?.cmp(&after.id)
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

struct RankingBuilder {
    keys: BinaryBuilder,
    distances: Float64Builder,
    iids: FixedSizeBinaryBuilder,
    scratch: Vec<u8>,
    key_bytes: usize,
    len: usize,
    memory: Arc<MemoryReservation>,
}

impl RankingBuilder {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        let memory = memory.reserve(16 << 10)?;
        Ok(Self {
            keys: BinaryBuilder::with_capacity(0, 0),
            distances: Float64Builder::with_capacity(0),
            iids: FixedSizeBinaryBuilder::with_capacity(0, 16),
            scratch: Vec::new(),
            key_bytes: 0,
            len: 0,
            memory,
        })
    }

    fn push(&mut self, id: EntityIdRef<'_>, iid: Iid, distance: f64) -> Result<()> {
        if !distance.is_finite() || distance < 0.0 {
            return Err(inconsistent_ranking());
        }
        self.key_bytes = self
            .key_bytes
            .checked_add(id.owned_payload_bytes().max(16) + 1)
            .filter(|&bytes| i32::try_from(bytes).is_ok())
            .ok_or_else(work_limit)?;
        self.len += 1;
        let required = self
            .key_bytes
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(self.len * 128 + (16 << 10)))
            .ok_or_else(work_limit)?;
        Arc::get_mut(&mut self.memory)
            .expect("ranking owns its reservation")
            .grow_to(required)?;
        self.scratch.clear();
        match id {
            EntityIdRef::String(value) => {
                self.scratch.push(0);
                self.scratch.extend_from_slice(value.as_bytes());
            }
            EntityIdRef::Keyword(value) => {
                self.scratch.push(1);
                self.scratch.extend_from_slice(value.as_bytes());
            }
            EntityIdRef::Integer(value) => {
                self.scratch.push(2);
                self.scratch
                    .extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
            }
            EntityIdRef::Uuid(value) => {
                self.scratch.push(3);
                self.scratch.extend_from_slice(&value);
            }
        }
        self.keys.append_value(&self.scratch);
        self.distances.append_value(distance);
        self.iids.append_value(iid.0).map_err(ranking_arrow_error)
    }

    fn finish(mut self, check: impl Fn() -> Result<()>) -> Result<OwnedBatch> {
        check()?;
        let keys = self.keys.finish();
        let distances = self.distances.finish();
        let iids = self.iids.finish();
        let count = u32::try_from(self.len).map_err(|_| work_limit())?;
        let mut positions = (0..count).collect::<Vec<_>>();
        // Complete key bytes sort in the same order as the typed identities.
        positions.sort_unstable_by(|&left, &right| {
            let (left, right) = (left as usize, right as usize);
            distances
                .value(left)
                .total_cmp(&distances.value(right))
                .then_with(|| keys.value(left).cmp(keys.value(right)))
        });
        check()?;
        let order = UInt32Array::from(positions);
        let schema = Arc::new(Schema::new(vec![
            Field::new("_entity_key", DataType::Binary, false),
            Field::new("distance_m", DataType::Float64, false),
            Field::new("_iid", DataType::FixedSizeBinary(16), false),
        ]));
        let ordered = |values: &dyn Array| {
            arrow_select::take::take(values, &order, None).map_err(ranking_arrow_error)
        };
        let columns = vec![ordered(&keys)?, ordered(&distances)?, ordered(&iids)?];
        let batch = RecordBatch::try_new(schema, columns).map_err(ranking_arrow_error)?;
        drop((
            keys,
            distances,
            iids,
            order,
            self.keys,
            self.distances,
            self.iids,
            self.scratch,
        ));
        Arc::get_mut(&mut self.memory)
            .expect("ranking owns its reservation")
            .shrink_to(batch.get_array_memory_size())?;
        OwnedBatch::with_reservation(batch, self.memory)
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
    for (id, distance) in [
        (EntityIdRef::Integer(1), 1.0),
        (EntityIdRef::String("same"), 1.0),
        (EntityIdRef::Integer(-3), 1.0),
        (EntityIdRef::Keyword("same"), 1.0),
        (EntityIdRef::String("later"), 4.0),
    ] {
        builder.push(id, id.iid(), distance).unwrap();
    }
    let ranking = builder.finish(|| Ok(())).unwrap();
    let keys = ranking
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let distances = ranking
        .batch()
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for (rank, id) in [
        EntityIdRef::String("same"),
        EntityIdRef::Keyword("same"),
        EntityIdRef::Integer(-3),
        EntityIdRef::Integer(1),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            EntityIdRef::from_handle_bytes(keys.value(rank)).unwrap(),
            id
        );
        assert_eq!(
            seek(
                keys,
                distances,
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
                Some(NearestPosition {
                    id,
                    distance_m: 2.0
                })
            )
            .is_err()
        );
    }
    assert_eq!(distances.value(4), 4.0);
}
