//! Covered seek pages across a live writer and its frozen predecessor.
use std::collections::BTreeMap;

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray};
use rtdb_node::{OwnedRangePage, OwnedRangeRequest};

use super::*;

type Key = (Vec<u8>, String);

fn keys(page: &OwnedRangePage, path: &OwnedRelation) -> Result<Vec<Key>> {
    let Some(batch) = &page.batch else { return Ok(Vec::new()); };
    let batch = batch.batch();
    let path = batch.column_by_name(path.name()).and_then(|values| values.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(OwnedWorld::unavailable)?;
    let ids = batch.column_by_name("_id").and_then(|values| values.as_any().downcast_ref::<StringArray>())
        .ok_or_else(OwnedWorld::unavailable)?;
    if path.null_count() != 0 || ids.null_count() != 0 {
        return Err(OwnedWorld::unavailable());
    }
    Ok(path.iter().flatten().zip(ids.iter().flatten())
        .map(|(key, id)| (key.to_vec(), id.to_owned())).collect())
}

fn order(descending: bool, left: &Key, right: &Key) -> std::cmp::Ordering {
    if descending { right.cmp(left) } else { left.cmp(right) }
}

fn covered_edge<'a>(live: &'a [Key], frozen: &'a [Key], limit: usize, descending: bool) -> Option<&'a Key> {
    [live, frozen].into_iter().filter(|keys| keys.len() == limit).filter_map(|keys| keys.last())
        .min_by(|left, right| order(descending, left, right))
}

impl HeldCheckpointWorld {
    pub(super) async fn merged_overlay_range(
        &self, relation: &OwnedRelation, path: &OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, local: Option<&rtdb_node::OwnedWorldAcceptedCut<'_>>,
    ) -> Result<OwnedRangePage> {
        let unavailable = OwnedWorld::unavailable;
        if !(1..=128).contains(&request.limit) {
            return Err(unavailable());
        }
        let mut cursor = request.after.cloned();
        let mut _cursor_charge = None;
        let mut output = Vec::<Key>::new();
        let mut retained = vec![self.memory.reserve(request.limit * 128)?];
        let mut bytes = 0usize;
        for _ in 0..32 {
            let seek = || OwnedRangeRequest {
                lower: request.lower, upper: request.upper, after: cursor.as_ref(),
                descending: request.descending, limit: request.limit,
            };
            let (frozen, _) = self.ancestor_overlay_range(relation, path.clone(), seek(), valid, None).await?;
            let live = match local {
                Some(cut) => cut.overlay_range(path, seek(), valid).await?,
                None => self.open.snapshot.range_world_overlay(self.open.record.id, relation, path, seek(), valid).await?,
            };
            let schema = live.schema.clone();
            if schema.fields() != frozen.schema.fields() || schema.fields().len() != 2
                || schema.field_with_name(path.name()).is_err() || schema.field_with_name("_id").is_err()
            {
                return Err(unavailable());
            }
            let key_bytes = [&live, &frozen].into_iter().filter_map(|page| page.batch.as_ref())
                .map(|batch| batch.batch().get_array_memory_size()).sum::<usize>();
            let _keys = self.memory.reserve(key_bytes.saturating_add(request.limit * 256))?;
            let live = keys(&live, path)?;
            let frozen = keys(&frozen, path)?;
            let coverage = covered_edge(&live, &frozen, request.limit, request.descending).cloned();

            // A local tombstone displaces a frozen key even when the local
            // order has no entry at that key.
            let mut candidates = BTreeMap::new();
            candidates.extend(frozen.iter().map(|key| (key, false)));
            candidates.extend(live.iter().map(|key| (key, true)));
            let mut candidates = candidates.into_iter().collect::<Vec<_>>();
            if request.descending {
                candidates.reverse();
            }
            for (key, live) in candidates {
                if coverage.as_ref().is_some_and(|edge| order(request.descending, key, edge).is_gt()) {
                    break;
                }
                if !live {
                    let id = EntityId::String(key.1.clone());
                    let shadows = match local {
                        Some(cut) => cut.shadow(&id, valid).await?.row.is_some(),
                        None => self.open.snapshot.read_world_shadow(self.open.record.id, relation, &id, valid, false)
                            .await?.row.is_some(),
                    };
                    if shadows { continue; }
                }
                let size = key.0.len().checked_add(key.1.len()).ok_or_else(unavailable)?;
                bytes = bytes.checked_add(size).filter(|bytes| *bytes <= 8 << 20).ok_or_else(unavailable)?;
                retained.push(self.memory.reserve(size)?);
                output.push(key.clone());
                if output.len() == request.limit { break; }
            }
            if output.len() == request.limit || coverage.is_none() {
                let charge = self.memory.reserve(bytes.saturating_mul(3).saturating_add(request.limit * 128 + 4096))?;
                let batch = if output.is_empty() {
                    None
                } else {
                    let paths = Arc::new(BinaryArray::from_iter_values(output.iter().map(|key| key.0.as_slice())));
                    let ids = Arc::new(StringArray::from_iter_values(output.iter().map(|key| key.1.as_str())));
                    let columns = schema.fields().iter().map(|field| -> arrow_array::ArrayRef {
                        if field.name() == "_id" { ids.clone() } else { paths.clone() }
                    }).collect::<Vec<_>>();
                    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| unavailable())?;
                    Some(OwnedBatch::with_reservation(batch, charge)?)
                };
                self.check_relation(relation)?;
                return Ok(OwnedRangePage { schema, batch });
            }
            let (key, id) = coverage.ok_or_else(unavailable)?;
            _cursor_charge = Some(self.memory.reserve(key.len().saturating_add(id.len()).saturating_add(256))?);
            cursor = Some(StructFields::from([(path.name().to_owned(), Value::Binary(key)), ("_id".into(), Value::Utf8(id))]));
        }
        Err(unavailable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_writers_must_cover_the_returned_seek_prefix() {
        let key = |n: u8| (vec![n], n.to_string());
        let live = [key(1), key(3)];
        let frozen = [key(2), key(4)];
        assert_eq!(covered_edge(&live, &frozen, 2, false), Some(&live[1]));
        let live = [key(4), key(2)];
        let frozen = [key(3), key(1)];
        assert_eq!(covered_edge(&live, &frozen, 2, true), Some(&live[1]));
        assert_eq!(covered_edge(&live[..1], &frozen, 2, true), Some(&frozen[1]));
        assert_eq!(covered_edge(&live[..1], &frozen[..1], 2, true), None);
    }
}
