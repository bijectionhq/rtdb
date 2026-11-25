use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_columnar::ArrowCell;
use rtdb_types::{EntityId, OwnedRelation, Result, RootRowCarrier, SEALED_SEARCH_UNIT_ROWS, Timestamp,
    is_rank_row_address, sealed_search_inventory_row};

use crate::{TxOp, hosted_control::{self, replay_rank::{MARKERS, rank_base}}, source::Operation};

use super::OpenTransaction;

const MAX_UNIT_ROWS: usize = 2048;

pub(super) struct ReplayRankErases {
    keys: BTreeMap<String, BTreeSet<Vec<u8>>>,
    _charge: Arc<rtdb_columnar::MemoryReservation>,
}

impl ReplayRankErases {
    pub(super) fn take(&mut self, relation: &str, key: &[u8]) -> bool {
        self.keys.get_mut(relation).is_some_and(|keys| keys.remove(key))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.keys.values().all(BTreeSet::is_empty)
    }
}

impl OpenTransaction<'_> {
    pub(super) async fn check_replayed_rank_erasure(
        &mut self, owner: &str, operations: &[Operation],
    ) -> Result<()> {
        let invalid = || rtdb_types::Anomaly::incorrect(
            "rtdb.hosted/rank-erasure", "The held Original rank erase lacks exact ordered custody",
        );
        let _scratch = self.context.memory.reserve(operations.len().saturating_mul(160))?;
        let mut erases = BTreeMap::<&str, BTreeSet<&[u8]>>::new();
        let mut markers = BTreeSet::new();
        let mut fences = BTreeSet::new();
        let mut retirements = BTreeSet::new();
        let mut count = 0;
        for operation in operations {
            if let Operation::Public(TxOp::OwnedErase { relation, id }) = operation
                && rank_base(relation.name()).is_some()
            {
                let EntityId::Binary(key) = id else { return Err(invalid()); };
                if !is_rank_row_address(key) || !erases.entry(relation.name()).or_default().insert(key)
                    || count == MAX_UNIT_ROWS
                {
                    return Err(invalid());
                }
                count += 1;
            }
            let Some(write) = hosted_control::owned_document_write(operation) else { continue; };
            if write.relation.name() == MARKERS {
                if !hosted_control::family_search_erase_marker(&write) { return Err(invalid()); }
                for row in write.documents.rows() {
                    if !markers.insert(row.string_id().ok_or_else(invalid)?) { return Err(invalid()); }
                }
            } else if write.relation.name() == "fact_unit_fences" && write.create_only
                && write.from == Some(Timestamp::MIN) && write.to == Some(Timestamp::MAX)
            {
                for row in write.documents.rows() {
                    if row.binary("unit_phase", invalid)? == Some(b"BFU1\x01") {
                        fences.insert(row.string_id().ok_or_else(invalid)?);
                    }
                }
            } else if write.create_only {
                for row in write.documents.rows() {
                    if let Some(key) = row.binary_id().and_then(|id| id.strip_prefix(&[0]))
                        && RootRowCarrier::parse(key).is_some() && row.only_id()
                    {
                        retirements.insert((write.relation.name(), key, write.from, write.to));
                    }
                }
            }
        }
        if !markers.into_iter().eq(erases.keys().copied()) { return Err(invalid()); }
        let replay = self.hosted_replay.as_ref().ok_or_else(invalid)?;
        let alias = replay.rank().and_then(|rank| rank.shared_erase_alias());
        if count == 0 {
            return if alias.is_none() { Ok(()) } else { Err(invalid()) };
        }
        if replay.rank().is_none() { return Err(invalid()); }
        let mut shared = BTreeMap::<&str, BTreeSet<&[u8]>>::new();
        for (relation, keys) in &erases {
            let base = rank_base(relation).ok_or_else(invalid)?;
            for &key in keys {
                let _fence = self.context.memory.reserve(2 * key.len() + 4)?;
                if let Some(row) = RootRowCarrier::parse(key)
                    && fences.contains(row.fence_name().as_str())
                    && retirements.iter().any(|&(journal, address, from, to)| {
                        journal == base && address == key && to == Some(Timestamp::MAX)
                            && from.is_some_and(|from| Timestamp(0) < from && from <= replay.source_time())
                    })
                {
                    continue;
                }
                shared.entry(relation).or_default().insert(key);
            }
        }
        if let Some(alias) = alias {
            if shared.values().map(BTreeSet::len).sum::<usize>() != count { return Err(invalid()); }
            if !fences.iter().any(|name| parse_alias(name) == Some(alias)) {
                let name = alias.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
                let fence = self.root_point(owner, &OwnedRelation::new("fact_unit_fences")?,
                    &EntityId::String(name), Timestamp::MIN, invalid).await?;
                if ArrowCell::named(fence.batch(), "unit_phase", 0).and_then(ArrowCell::binary) != Some(b"BFU1\x01") {
                    return Err(invalid());
                }
            }
            let ordered = self.tx_key().system_time;
            let context = self.context.query_context(
                self.buffer.base_snapshot().clone(), ordered, &self.options.default_tz, self.trace_parent,
            )?;
            let mut inventory = rtdb_query::open_owned_root_prefix_projection(
                Arc::clone(&context), owner, &OwnedRelation::new(SEALED_SEARCH_UNIT_ROWS)?, ordered,
                &["_id"], rtdb_columnar::EntityIdRef::Binary(&alias),
            ).await?.ok_or_else(invalid)?;
            let mut rows = 0;
            while let Some(held) = inventory.next_batch().await? {
                let batch = held.batch();
                let (index, field) = batch.schema_ref().column_with_name("_id").ok_or_else(invalid)?;
                for row in 0..batch.num_rows() {
                    context.check_cancelled_at(row)?;
                    let rtdb_columnar::EntityIdRef::Binary(key) =
                        rtdb_columnar::read_owned_entity_id(field, batch.column(index).as_ref(), row)?
                    else { return Err(invalid()); };
                    let (relation, key) = sealed_search_inventory_row(&alias, key).ok_or_else(invalid)?;
                    rows += 1;
                    if rows > MAX_UNIT_ROWS || !shared.get_mut(relation).is_some_and(|keys| keys.remove(key)) {
                        return Err(invalid());
                    }
                }
            }
            if shared.values().any(|keys| !keys.is_empty()) { return Err(invalid()); }
        } else if !shared.is_empty() {
            return Err(invalid());
        }
        let bytes = erases.iter().flat_map(|(relation, keys)| keys.iter().map(|key| relation.len() + key.len()))
            .sum::<usize>();
        let charge = self.context.memory.reserve(bytes.saturating_add(count * 128))?;
        self.replay_rank_erases = Some(ReplayRankErases {
            keys: erases.into_iter().map(|(relation, keys)| {
                (relation.to_owned(), keys.into_iter().map(<[u8]>::to_vec).collect())
            }).collect(),
            _charge: charge,
        });
        Ok(())
    }
}

fn parse_alias(alias: &str) -> Option<[u8; 32]> {
    if alias.len() != 64 || !alias.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')) {
        return None;
    }
    let mut bytes = [0; 32];
    for (offset, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&alias[2 * offset..2 * offset + 2], 16).ok()?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_rank_erase_consumes_only_its_exact_relation_and_binary_key() -> Result<()> {
        let first = b"opaque\0\xff";
        let second = b"opaque\0\xfe";
        let mut erases = ReplayRankErases {
            keys: BTreeMap::from([("rank_a".to_owned(), BTreeSet::from([first.to_vec()]))]),
            _charge: rtdb_columnar::MemoryBudget::new(4096).reserve(512)?,
        };
        assert!(!erases.take("rank_b", first));
        assert!(!erases.take("rank_a", second));
        assert!(erases.take("rank_a", first));
        assert!(!erases.take("rank_a", first));
        assert!(erases.is_empty());
        assert_eq!(parse_alias(&"0f".repeat(32)), Some([15; 32]));
        assert!(parse_alias(&"0F".repeat(32)).is_none());
        Ok(())
    }
}
