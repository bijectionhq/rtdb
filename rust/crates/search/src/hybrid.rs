//! Reciprocal rank fusion of exact rankings taken at one held cut.

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, Float64Array, RecordBatch, UInt32Array, UInt64Array, cast::AsArray, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{EntityIdRef, MemoryBudget, OwnedBatch};
use rtdb_types::{Anomaly, Result};

use crate::OwnedSearchBatch;

/// The constant k of reciprocal rank fusion: the hit at rank r of a ranking contributes 1 / (k + r).
pub const RANK_CONSTANT: f64 = 60.0;

/// Fuses ranked windows of hits by reciprocal rank fusion and keeps the first `limit` fused hits.
/// A hit's rank is its 1-based row in its window, and a window must hold each (partition, identity) once.
/// The fused score adds the contributions in window order, so it is the same binary64 sum on every node.
/// Equal scores order by typed identity, then partition ordinal; `total` counts the distinct fused hits.
/// Each window's name labels a nullable UInt32 column with the hit's rank in that window.
pub fn fuse_ranked(windows: &[(&str, &OwnedSearchBatch)], limit: usize, memory: &Arc<MemoryBudget>) -> Result<OwnedSearchBatch> {
    let mut scores = BTreeMap::<(usize, EntityIdRef<'_>), (f64, Vec<Option<u32>>)>::new();
    let mut bytes = 4096usize;
    let mut scratch = memory.reserve(bytes)?;
    for (ordinal, (_, window)) in windows.iter().enumerate() {
        let batch = window.hits.batch();
        let ids = batch.column(0).as_ref();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        for row in 0..batch.num_rows() {
            let id = rtdb_columnar::read_owned_entity_id(batch.schema_ref().field(0), ids, row)?;
            let partition = usize::try_from(partitions.value(row)).map_err(|_| invalid())?;
            let contribution = 1.0 / (RANK_CONSTANT + (row + 1) as f64);
            let rank = u32::try_from(row + 1).map_err(|_| invalid())?;
            let (score, ranks) = match scores.entry((partition, id)) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    bytes = bytes.saturating_add(hit_bytes(id));
                    Arc::get_mut(&mut scratch).expect("fusion owns its scratch").grow_to(bytes)?;
                    entry.insert((0.0, vec![None; windows.len()]))
                }
            };
            if ranks[ordinal].replace(rank).is_some() { return Err(invalid()); }
            *score += contribution;
        }
    }
    let total = scores.len();
    let mut fused: Vec<_> = scores.into_iter().map(|((partition, id), (score, ranks))| (partition, id, score, ranks)).collect();
    fused.sort_by(|left, right| right.2.total_cmp(&left.2)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.0.cmp(&right.0)));
    fused.truncate(limit);
    let bound = fused.iter().fold(4096usize, |size, (_, id, _, _)| {
        size.saturating_add(hit_bytes(*id).saturating_mul(4)).saturating_add(8 * windows.len())
    });
    let reservation = memory.reserve(bound)?;
    let mut fields = vec![
        rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type())?,
        Field::new("score", DataType::Float64, false),
        Field::new("partition", DataType::UInt64, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        rtdb_columnar::entity_id_array(fused.iter().map(|(_, id, _, _)| *id))?,
        Arc::new(Float64Array::from_iter_values(fused.iter().map(|(_, _, score, _)| *score))),
        Arc::new(UInt64Array::from_iter_values(fused.iter().map(|(partition, _, _, _)| *partition as u64))),
    ];
    for (ordinal, (name, _)) in windows.iter().enumerate() {
        fields.push(Field::new(*name, DataType::UInt32, true));
        columns.push(Arc::new(UInt32Array::from_iter(fused.iter().map(|(_, _, _, ranks)| ranks[ordinal]))));
    }
    let output = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| Anomaly::fault("rtdb.search/hybrid-output", error.to_string()))?;
    Ok(OwnedSearchBatch { hits: OwnedBatch::with_reservation(output, reservation)?, total })
}

fn hit_bytes(id: EntityIdRef<'_>) -> usize {
    size_of::<rtdb_types::Value>().saturating_add(id.owned_payload_bytes()).saturating_add(128)
}

fn invalid() -> Anomaly {
    Anomaly::fault("rtdb.search/hybrid-input", "A fused ranking holds a hit twice or an invalid partition")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::EntityId;

    fn window(hits: &[(u64, EntityIdRef<'_>)], memory: &Arc<MemoryBudget>) -> OwnedSearchBatch {
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type()).unwrap(),
            Field::new("score", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            rtdb_columnar::entity_id_array(hits.iter().map(|(_, id)| *id)).unwrap(),
            Arc::new(Float64Array::from(vec![0.0; hits.len()])),
            Arc::new(UInt64Array::from_iter_values(hits.iter().map(|(partition, _)| *partition))),
        ]).unwrap();
        OwnedSearchBatch { hits: OwnedBatch::new(batch, memory).unwrap(), total: hits.len() }
    }

    #[test]
    fn fusion_adds_reciprocal_ranks_and_breaks_ties_by_identity_then_partition() {
        let memory = MemoryBudget::new(1 << 20);
        let text = window(&[(0, EntityIdRef::String("b")), (0, EntityIdRef::String("a")), (1, EntityIdRef::String("c"))], &memory);
        let nearest = window(&[(0, EntityIdRef::String("a")), (1, EntityIdRef::String("b")), (0, EntityIdRef::String("b"))], &memory);
        let fused = fuse_ranked(&[("text_rank", &text), ("nearest_rank", &nearest)], 3, &memory).unwrap();
        assert_eq!(fused.total, 4);
        let ranks = |name: &str| fused.hits.batch().column_by_name(name).unwrap().as_primitive::<arrow_array::types::UInt32Type>().iter().collect::<Vec<_>>();
        assert_eq!(ranks("text_rank"), [Some(2), Some(1), None]);
        assert_eq!(ranks("nearest_rank"), [Some(1), Some(3), Some(2)]);
        let page = fused.into_page().unwrap();
        let hits: Vec<_> = page.hits.iter().map(|hit| (hit.partition, hit.id.clone(), hit.score)).collect();
        let rrf = |rank: f64| 1.0 / (RANK_CONSTANT + rank);
        assert_eq!(hits, [
            (0, EntityId::String("a".into()), rrf(2.0) + rrf(1.0)),
            (0, EntityId::String("b".into()), rrf(1.0) + rrf(3.0)),
            (1, EntityId::String("b".into()), rrf(2.0)),
        ]);
        assert!(fuse_ranked(&[("rank", &window(&[(0, EntityIdRef::String("a")), (0, EntityIdRef::String("a"))], &memory))], 1, &memory).is_err());
    }

    #[test]
    fn fusion_uses_the_complete_typed_identity_and_partition_for_duplicates() {
        use EntityIdRef::{Keyword, String};

        let memory = MemoryBudget::new(1 << 20);
        let first = window(&[(0, String("a")), (0, Keyword("a")), (1, String("a"))], &memory);
        let second = window(&[(1, String("a")), (0, Keyword("a")), (0, String("a"))], &memory);
        let fused = fuse_ranked(&[("first_rank", &first), ("second_rank", &second)], 3, &memory).unwrap();
        assert_eq!(fused.total, 3);
        let ids = fused.into_page().unwrap().hits.into_iter().map(|hit| (hit.partition, hit.id)).collect::<Vec<_>>();
        assert_eq!(ids, [
            (0, String("a").into_owned()), (1, String("a").into_owned()), (0, Keyword("a").into_owned()),
        ]);

        let duplicate = window(&[(1, String("a")), (0, Keyword("a")), (1, String("a"))], &memory);
        let error = fuse_ranked(&[("first_rank", &first), ("second_rank", &duplicate)], 1, &memory).err().unwrap();
        assert_eq!(error, invalid());
    }
}
