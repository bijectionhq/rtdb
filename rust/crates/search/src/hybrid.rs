//! Reciprocal rank fusion of exact rankings taken at one held cut.

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Float64Array, RecordBatch, UInt64Array, cast::AsArray, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{EntityIdRef, MemoryBudget, OwnedBatch};
use rtdb_types::{Anomaly, EntityId, Result};

use crate::OwnedSearchBatch;

/// The constant k of reciprocal rank fusion: the hit at rank r of a ranking contributes 1 / (k + r).
pub const RANK_CONSTANT: f64 = 60.0;

/// Fuses ranked windows of hits by reciprocal rank fusion and keeps the first `limit` fused hits.
/// A hit's rank is its 1-based row in its window, and a window must hold each (partition, identity) once.
/// The fused score adds the contributions in window order, so it is the same binary64 sum on every node.
/// Equal scores order by typed identity, then partition ordinal; `total` counts the distinct fused hits.
pub fn fuse_ranked(windows: &[&OwnedSearchBatch], limit: usize, memory: &Arc<MemoryBudget>) -> Result<OwnedSearchBatch> {
    let mut scores = BTreeMap::<(usize, EntityId), f64>::new();
    let mut bytes = 4096usize;
    let mut scratch = memory.reserve(bytes)?;
    for window in windows {
        let batch = window.hits.batch();
        let ids = batch.column(0).as_ref();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let mut seen = std::collections::BTreeSet::new();
        for row in 0..batch.num_rows() {
            let id = rtdb_columnar::read_entity_id(batch.schema_ref().field(0), ids, row)?;
            let partition = usize::try_from(partitions.value(row)).map_err(|_| invalid())?;
            let id = id.into_owned();
            if !seen.insert((partition, id.clone())) { return Err(invalid()); }
            let contribution = 1.0 / (RANK_CONSTANT + (row + 1) as f64);
            match scores.entry((partition, id)) {
                std::collections::btree_map::Entry::Occupied(mut entry) => *entry.get_mut() += contribution,
                std::collections::btree_map::Entry::Vacant(entry) => {
                    bytes = bytes.saturating_add(hit_bytes(&entry.key().1));
                    Arc::get_mut(&mut scratch).expect("fusion owns its scratch").grow_to(bytes)?;
                    entry.insert(contribution);
                }
            }
        }
    }
    let total = scores.len();
    let mut fused: Vec<_> = scores.into_iter().map(|((partition, id), score)| (partition, id, score)).collect();
    fused.sort_by(|left, right| right.2.total_cmp(&left.2)
        .then_with(|| EntityIdRef::from(&left.1).cmp(&EntityIdRef::from(&right.1)))
        .then_with(|| left.0.cmp(&right.0)));
    fused.truncate(limit);
    let bound = fused.iter().fold(4096usize, |size, (_, id, _)| size.saturating_add(hit_bytes(id).saturating_mul(4)));
    let reservation = memory.reserve(bound)?;
    let schema = Arc::new(Schema::new(vec![
        rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type())?,
        Field::new("score", DataType::Float64, false),
        Field::new("partition", DataType::UInt64, false),
    ]));
    let ids = rtdb_columnar::entity_id_array(fused.iter().map(|(_, id, _)| EntityIdRef::from(id)))?;
    let score = Arc::new(Float64Array::from_iter_values(fused.iter().map(|(_, _, score)| *score)));
    let partition = Arc::new(UInt64Array::from_iter_values(fused.iter().map(|(partition, _, _)| *partition as u64)));
    let output = RecordBatch::try_new(schema, vec![ids, score, partition])
        .map_err(|error| Anomaly::fault("rtdb.search/hybrid-output", error.to_string()))?;
    Ok(OwnedSearchBatch { hits: OwnedBatch::with_reservation(output, reservation)?, total })
}

fn hit_bytes(id: &EntityId) -> usize {
    size_of::<rtdb_types::Value>().saturating_add(EntityIdRef::from(id).owned_payload_bytes()).saturating_add(128)
}

fn invalid() -> Anomaly {
    Anomaly::fault("rtdb.search/hybrid-input", "A fused ranking holds a hit twice or an invalid partition")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(hits: &[(u64, &str)], memory: &Arc<MemoryBudget>) -> OwnedSearchBatch {
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type()).unwrap(),
            Field::new("score", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ]));
        let ids: Vec<EntityId> = hits.iter().map(|(_, id)| EntityId::String((*id).into())).collect();
        let batch = RecordBatch::try_new(schema, vec![
            rtdb_columnar::entity_id_array(ids.iter().map(EntityIdRef::from)).unwrap(),
            Arc::new(Float64Array::from(vec![0.0; hits.len()])),
            Arc::new(UInt64Array::from_iter_values(hits.iter().map(|(partition, _)| *partition))),
        ]).unwrap();
        OwnedSearchBatch { hits: OwnedBatch::new(batch, memory).unwrap(), total: hits.len() }
    }

    #[test]
    fn fusion_adds_reciprocal_ranks_and_breaks_ties_by_identity_then_partition() {
        let memory = MemoryBudget::new(1 << 20);
        let text = window(&[(0, "b"), (0, "a"), (1, "c")], &memory);
        let nearest = window(&[(0, "a"), (1, "b"), (0, "b")], &memory);
        let fused = fuse_ranked(&[&text, &nearest], 3, &memory).unwrap();
        assert_eq!(fused.total, 4);
        let page = fused.into_page().unwrap();
        let hits: Vec<_> = page.hits.iter().map(|hit| (hit.partition, hit.id.clone(), hit.score)).collect();
        let rrf = |rank: f64| 1.0 / (RANK_CONSTANT + rank);
        assert_eq!(hits, [
            (0, EntityId::String("a".into()), rrf(2.0) + rrf(1.0)),
            (0, EntityId::String("b".into()), rrf(1.0) + rrf(3.0)),
            (1, EntityId::String("b".into()), rrf(2.0)),
        ]);
        assert!(fuse_ranked(&[&window(&[(0, "a"), (0, "a")], &memory)], 1, &memory).is_err());
    }
}
