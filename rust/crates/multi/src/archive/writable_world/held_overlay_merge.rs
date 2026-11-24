//! Covered seek pages over ordered writer shadows.
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

fn covered_edge<'a>(
    pages: impl IntoIterator<Item = &'a [Key]>, limit: usize, descending: bool,
) -> Option<&'a Key> {
    pages.into_iter().filter(|keys| keys.len() == limit).filter_map(|keys| keys.last())
        .min_by(|left, right| order(descending, left, right))
}

/// Newest first. The held source's archived aggregate is always the last layer.
enum Layer<'a> {
    Live(&'a HeldCheckpointWorld, &'a OwnedRelation, Option<&'a rtdb_node::OwnedWorldAcceptedCut<'a>>),
    Archived(&'a HeldCheckpointWorld, &'a OwnedRelation),
    Writer(&'a InheritedRelation<'a>),
}

impl Layer<'_> {
    async fn seek(&self, path: &OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp) -> Result<OwnedRangePage> {
        match self {
            Self::Live(_, _, Some(cut)) => cut.overlay_range(path, request, valid).await,
            Self::Live(held, relation, None) =>
                held.open.snapshot.range_world_overlay(held.open.record.id, relation, path, request, valid).await,
            Self::Archived(held, relation) =>
                held.ancestor_overlay_range(relation, path.clone(), request, valid, None).await.map(|(page, _)| page),
            Self::Writer(writer) => writer.own_range(path, request, valid).await,
        }
    }

    async fn shadows(&self, id: &EntityId, valid: Timestamp) -> Result<bool> {
        match self {
            Self::Live(_, _, Some(cut)) => Ok(cut.shadow(id, valid).await?.row.is_some()),
            Self::Live(held, relation, None) => Ok(held.open.snapshot.read_world_shadow(
                held.open.record.id, relation, id, valid, false,
            ).await?.row.is_some()),
            Self::Writer(writer) => Ok(writer.shadow(id, Interval::at(valid)?).await?.is_some()),
            Self::Archived(..) => Err(OwnedWorld::unavailable()),
        }
    }
}

impl HeldCheckpointWorld {
    pub(super) async fn merged_overlay_range<'a>(
        &'a self, relation: &'a OwnedRelation, path: &OwnedRelation, request: OwnedRangeRequest<'_>,
        valid: Timestamp, local: Option<&'a rtdb_node::OwnedWorldAcceptedCut<'a>>,
    ) -> Result<OwnedRangePage> {
        let layers = [Layer::Live(self, relation, local), Layer::Archived(self, relation)];
        let page = merge_range(&self.memory, &layers, path, request, valid).await?;
        self.check_relation(relation)?;
        Ok(page)
    }
}

impl InheritedRelation<'_> {
    pub(super) async fn overlay_range(
        &self, path: &OwnedRelation, request: OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<OwnedRangePage> {
        let layers = self.writers().map(Layer::Writer).collect::<Vec<_>>();
        merge_range(self.memory, &layers, path, request, valid).await
    }
}

async fn merge_range(
    memory: &Arc<MemoryBudget>, layers: &[Layer<'_>], path: &OwnedRelation,
    request: OwnedRangeRequest<'_>, valid: Timestamp,
) -> Result<OwnedRangePage> {
    let unavailable = OwnedWorld::unavailable;
    if !(1..=128).contains(&request.limit) {
        return Err(unavailable());
    }
    let mut cursor = request.after.cloned();
    let mut _cursor_charge = None;
    let mut output = Vec::<Key>::new();
    let mut retained = vec![memory.reserve(request.limit * 128)?];
    let mut bytes = 0usize;
    for _ in 0..32 {
        let seek = || OwnedRangeRequest {
            lower: request.lower, upper: request.upper, after: cursor.as_ref(),
            descending: request.descending, limit: request.limit,
        };
        let mut pages = Vec::with_capacity(layers.len());
        for layer in layers {
            pages.push(layer.seek(path, seek(), valid).await?);
        }
        let schema = Arc::clone(&pages.first().ok_or_else(unavailable)?.schema);
        if schema.fields().len() != 2 || schema.field_with_name(path.name()).is_err()
            || schema.field_with_name("_id").is_err()
            || pages.iter().any(|page| schema.fields() != page.schema.fields())
        {
            return Err(unavailable());
        }
        let key_bytes = pages.iter().filter_map(|page| page.batch.as_ref())
            .map(|batch| batch.batch().get_array_memory_size()).sum::<usize>();
        let _keys = memory.reserve(key_bytes.saturating_add(layers.len().saturating_mul(request.limit * 128)))?;
        let pages = pages.iter().map(|page| keys(page, path)).collect::<Result<Vec<_>>>()?;
        let coverage = covered_edge(pages.iter().map(Vec::as_slice), request.limit, request.descending).cloned();

        let mut candidates = BTreeMap::new();
        for (index, keys) in pages.iter().enumerate().rev() {
            candidates.extend(keys.iter().map(|key| (key, index)));
        }
        let mut candidates = candidates.into_iter().collect::<Vec<_>>();
        if request.descending { candidates.reverse(); }
        for (key, origin) in candidates {
            if coverage.as_ref().is_some_and(|edge| order(request.descending, key, edge).is_gt()) {
                break;
            }
            let id = EntityId::String(key.1.clone());
            let mut masked = false;
            for newer in &layers[..origin] {
                if newer.shadows(&id, valid).await? {
                    masked = true;
                    break;
                }
            }
            if masked { continue; }
            let size = key.0.len().checked_add(key.1.len()).ok_or_else(unavailable)?;
            bytes = bytes.checked_add(size).filter(|bytes| *bytes <= 8 << 20).ok_or_else(unavailable)?;
            retained.push(memory.reserve(size)?);
            output.push(key.clone());
            if output.len() == request.limit { break; }
        }
        if output.len() == request.limit || coverage.is_none() {
            let charge = memory.reserve(bytes.saturating_mul(3).saturating_add(request.limit * 128 + 4096))?;
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
            return Ok(OwnedRangePage { schema, batch });
        }
        let (key, id) = coverage.ok_or_else(unavailable)?;
        _cursor_charge = Some(memory.reserve(key.len().saturating_add(id.len()).saturating_add(256))?);
        cursor = Some(StructFields::from([(path.name().to_owned(), Value::Binary(key)), ("_id".into(), Value::Utf8(id))]));
    }
    Err(unavailable())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_writers_must_cover_the_returned_seek_prefix() {
        let key = |n: u8| (vec![n], n.to_string());
        let live = [key(1), key(3)];
        let frozen = [key(2), key(4)];
        assert_eq!(covered_edge([live.as_slice(), frozen.as_slice()], 2, false), Some(&live[1]));
        let live = [key(4), key(2)];
        let frozen = [key(3), key(1)];
        assert_eq!(covered_edge([live.as_slice(), frozen.as_slice()], 2, true), Some(&live[1]));
        assert_eq!(covered_edge([&live[..1], frozen.as_slice()], 2, true), Some(&frozen[1]));
        assert_eq!(covered_edge([&live[..1], &frozen[..1]], 2, true), None);
    }
}
