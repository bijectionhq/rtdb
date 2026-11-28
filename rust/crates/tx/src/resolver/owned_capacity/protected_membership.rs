//! The protected decision reads only installed scoped membership metadata.

use std::sync::Arc;

use rtdb_columnar::{EntityIdRef, read_owned_entity_id};
use rtdb_query::QueryContext;
use rtdb_types::{EntityId, Interval, OwnedRelation, Result, ROOT_ROW_CARRIERS, RootRowCarrier, SHARED_COLUMN_CARRIERS, SharedColumnCarrierPrefix, Timestamp, Value};

use super::{occupancy_value_at, overflow, uncovered};

pub(super) const PROTECTED_MEMBERSHIP: &str = "protected_membership";
const FENCES: &str = "fact_unit_fences";
const UNTIL: &str = "membership_until";

pub(super) enum ProtectedProjection<'a> {
    Rows(&'a [u8; 16]),
    SharedUnit { carriers: SharedColumnCarrierPrefix<'a>, fence: &'a str },
}

fn lowercase_hex(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl<'a> ProtectedProjection<'a> {
    pub(super) fn decode(encoded: &'a [u8]) -> Result<Self> {
        let (&kind, encoded) = encoded.split_first().ok_or_else(uncovered)?;
        match kind {
            3 => Ok(Self::Rows(encoded.try_into().map_err(|_| uncovered())?)),
            2 => {
                let (carriers, fence) = encoded.split_at_checked(SharedColumnCarrierPrefix::WIDTH).ok_or_else(uncovered)?;
                let carriers = SharedColumnCarrierPrefix::parse(carriers).ok_or_else(uncovered)?;
                let fence = std::str::from_utf8(fence).map_err(|_| uncovered())?;
                if fence.len() != 64 || !lowercase_hex(fence) { return Err(uncovered()); }
                Ok(Self::SharedUnit { carriers, fence })
            }
            _ => Err(uncovered()),
        }
    }

    pub(super) async fn count(&self, context: Arc<QueryContext>, owner: &str, journal: &OwnedRelation, ordered: Timestamp) -> Result<i64> {
        let fences = OwnedRelation::new(FENCES)?;
        match self {
            Self::Rows(table) => {
                let carriers = OwnedRelation::new(ROOT_ROW_CARRIERS)?;
                let id_prefix = RootRowCarrier::table_prefix(table);
                let mut count = 0i64;
                if let Some(mut cursor) = rtdb_query::open_owned_root_prefix_projection(
                    Arc::clone(&context), owner, &carriers, ordered, &["_id"], EntityIdRef::Binary(&id_prefix),
                ).await? {
                    while let Some(batch) = cursor.next_batch().await? {
                        let rows = batch.batch();
                        let (column, field) = rows.schema_ref().column_with_name("_id").ok_or_else(uncovered)?;
                        for row in 0..rows.num_rows() {
                            context.check_cancelled_at(row)?;
                            let EntityIdRef::Binary(id) = read_owned_entity_id(field, rows.column(column).as_ref(), row)
                                .map_err(|_| uncovered())? else { return Err(uncovered()); };
                            let carrier = RootRowCarrier::parse(id).filter(|_| id.starts_with(&id_prefix))
                                .ok_or_else(uncovered)?;
                            let _keys = context.memory.reserve(3 * id.len() + 256)?;
                            if !point_exists(&context, owner, &fences, &EntityId::String(carrier.fence_name()), ordered).await?
                                && position_survives(&context, owner, journal, &EntityId::Binary(carrier.assertion_key()), ordered).await?
                            {
                                count = count.checked_add(1).ok_or_else(overflow)?;
                            }
                        }
                    }
                }
                Ok(count)
            }
            Self::SharedUnit { carriers: prefix, fence } => {
                let carriers = OwnedRelation::new(SHARED_COLUMN_CARRIERS)?;
                if point_exists(&context, owner, &fences, &EntityId::String((*fence).to_owned()), ordered).await? {
                    return Ok(0);
                }
                let Some(mut cursor) = rtdb_query::open_owned_root_prefix_projection(
                    Arc::clone(&context), owner, &carriers, ordered, &["_id"], EntityIdRef::Binary(prefix.as_bytes()),
                ).await? else { return Ok(0); };
                let mut count = 0i64;
                while let Some(batch) = cursor.next_batch().await? {
                    let batch = batch.batch();
                    let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(uncovered)?;
                    for row in 0..batch.num_rows() {
                        context.check_cancelled_at(row)?;
                        let id = read_owned_entity_id(field, batch.column(column).as_ref(), row).map_err(|_| uncovered())?;
                        if !matches!(id, EntityIdRef::Binary(id) if prefix.contains(id)) { return Err(uncovered()); }
                        count = count.checked_add(1).ok_or_else(overflow)?;
                    }
                }
                Ok(count)
            }
        }
    }
}

async fn point_exists(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, ordered: Timestamp,
) -> Result<bool> {
    let upper = Timestamp(ordered.0.checked_add(1).ok_or_else(overflow)?);
    let Some(mut cursor) = rtdb_query::open_owned_point(
        Arc::clone(context), owner, relation, id, Some(Interval::new(ordered, upper)?), Some(&["_id"]),
    ).await? else { return Ok(false); };
    while let Some(batch) = cursor.next_batch().await? {
        if batch.batch().num_rows() != 0 { return Ok(true); }
    }
    Ok(false)
}

async fn position_survives(
    context: &Arc<QueryContext>, owner: &str, relation: &OwnedRelation, id: &EntityId, ordered: Timestamp,
) -> Result<bool> {
    let upper = Timestamp(ordered.0.checked_add(1).ok_or_else(overflow)?);
    let mut cursor = rtdb_query::open_owned_point(
        Arc::clone(context), owner, relation, id, Some(Interval::new(ordered, upper)?), Some(&["_id", UNTIL]),
    ).await?.ok_or_else(uncovered)?;
    let mut membership = None;
    while let Some(batch) = cursor.next_batch().await? {
        let rows = batch.batch();
        for row in 0..rows.num_rows() {
            let Value::Binary(until) = occupancy_value_at(&context.memory, rows, row, UNTIL, uncovered)?
                else { return Err(uncovered()); };
            let until = i64::from_be_bytes(until.try_into().map_err(|_| uncovered())?);
            if until <= 0 || membership.replace(ordered.0 < until).is_some() { return Err(uncovered()); }
        }
    }
    membership.ok_or_else(uncovered)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rtdb_columnar::MemoryBudget;
    use rtdb_index::PartitionState;
    use rtdb_query::QueryOptions;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{EntityId, OwnedIdentityScan, OwnedRelation, OwnedSchema, Result,
        SHARED_COLUMN_CARRIERS, SharedColumnCarrierPrefix, StructFields, Timestamp, TransactionKey, TxId, Value};
    use tokio_util::sync::CancellationToken;

    use crate::{Documents, SubmitOptions, TxOp, source::Operation};
    use crate::resolver::{OpenTransaction, ResolveContext, TransactionAuthorization};
    use super::{FENCES, PROTECTED_MEMBERSHIP, ProtectedProjection};

    fn scope() -> [u8; SharedColumnCarrierPrefix::WIDTH] {
        let mut prefix = [0xff; SharedColumnCarrierPrefix::WIDTH];
        prefix[..6].copy_from_slice(b"BFCP2\0");
        prefix
    }

    fn operand(prefix: &[u8], fence: &str) -> Vec<u8> {
        [&[2], prefix, fence.as_bytes()].concat()
    }

    #[test]
    fn shared_membership_accepts_only_the_raw_scoped_operand() {
        let prefix = scope();
        let fence = "a1".repeat(32);
        let raw = operand(&prefix, &fence);
        assert!(matches!(ProtectedProjection::decode(&raw),
            Ok(ProtectedProjection::SharedUnit { carriers, fence: id })
                if carriers.as_bytes() == prefix.as_slice() && id == fence.as_str()));
        assert!(ProtectedProjection::decode(&raw[..raw.len() - 1]).is_err());
        assert!(ProtectedProjection::decode(&[raw.as_slice(), b"0"].concat()).is_err());
        assert!(ProtectedProjection::decode(&operand(&prefix, &fence.to_ascii_uppercase())).is_err());
        let hex = prefix.iter().map(|byte| format!("{byte:02x}")).collect::<Vec<_>>().concat();
        assert!(ProtectedProjection::decode(&operand(hex.as_bytes(), &fence)).is_err());
        assert!(ProtectedProjection::decode(&[&[1], hex.as_bytes(), &[0], fence.as_bytes()].concat()).is_err());
    }

    struct FixtureWriter;
    impl TransactionAuthorization for FixtureWriter {
        fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn binary_pages_and_final_count_cover_the_same_scope_and_the_ordered_unit_fence() -> Result<()> {
        let memory = MemoryBudget::new(64 << 20);
        let context = ResolveContext {
            footprint_grants: Default::default(), decision_clock: None, write_event_origins: false, placed_receipt_retirement: false,
            owned_witness_scope: None, world_constraints: None, database: "app".into(),
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
            memory: Arc::clone(&memory), query_options: QueryOptions::default(), scan_accelerators: Default::default(),
            cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(FixtureWriter),
            execution_observer: None, tracer: None,
        };
        let owner = "owner";
        let options = SubmitOptions { user: Some(owner.into()), ..Default::default() };
        let state = PartitionState::empty(Default::default())?;
        let root = OwnedRelation::new(SHARED_COLUMN_CARRIERS)?;
        let overlay = OwnedRelation::new("fact_unit_column_payloads")?;
        let fence = "ab".repeat(32);
        let journal = OwnedRelation::new("fact_fixture")?;
        let key = |id, time| TransactionKey { tx_id: TxId(id), system_time: Timestamp(time) };
        let prefix = scope();
        let payload = |prefix: &[u8], last| {
            let mut id = prefix.to_vec();
            id.resize(SharedColumnCarrierPrefix::ID_WIDTH, 0);
            *id.last_mut().unwrap() = last;
            id
        };
        let [low, high] = [payload(&prefix, 0x7f), payload(&prefix, 0x80)];
        let mut next_scope = [0; SharedColumnCarrierPrefix::WIDTH];
        next_scope[..6].copy_from_slice(b"BFCP2\x01");
        let adjacent = payload(&next_scope, 0);
        let schema = OwnedSchema::from_json(br#"{"version":2,"fields":[{"name":"sealed_payload","type":{"kind":"opaque_binary"},"optional":false,"nullable":false}]}"#)?;
        let mut install = OpenTransaction::new(state.snapshot(), key(1, 100), &options, &context)?;
        install.declare_owned_relation(&root, &schema).await?;
        install.declare_owned_relation(&overlay, &schema).await?;
        state.apply(&install.buffer.stage()?)?;

        let row = |id| StructFields::from([("_id".into(), id), ("sealed_payload".into(), Value::Binary(vec![9]))]);
        let mut candidate = OpenTransaction::new(state.snapshot(), key(2, 200), &options, &context)?;
        let marker = Documents::new(vec![row(Value::Utf8("world-column/fixture".into()))], &memory)?;
        assert!(candidate.write_owned(&root, &marker, None, None, None, true).await.is_err());
        candidate.write_owned(&overlay, &marker, None, None, None, true).await?;
        let keys = [high.clone(), adjacent, low.clone()];
        let rows = Documents::new(keys.iter().map(|id| row(Value::Binary(id.clone()))).collect(), &memory)?;
        candidate.write_owned(&root, &rows, None, None, None, true).await?;
        let at = Timestamp(200);
        let held = context.query_context(candidate.snapshot()?, at, "UTC", None)?;
        let within = Some(rtdb_columnar::EntityIdRef::Binary(&prefix));
        let first = rtdb_query::owned_identity_candidates_in_prefix(
            Arc::clone(&held), owner, &root, at, at, &OwnedIdentityScan::binary(None), 1, within,
        ).await?;
        let next = rtdb_query::owned_identity_candidates_in_prefix(
            Arc::clone(&held), owner, &root, at, at, &OwnedIdentityScan::binary(Some(&low)), 2, within,
        ).await?;
        let binary_end = rtdb_query::owned_identity_candidates_in_prefix(
            Arc::clone(&held), owner, &root, at, at, &OwnedIdentityScan::binary(Some(&high)), 2, within,
        ).await?;
        assert!(binary_end.keys.is_empty());
        let enumerated: Vec<_> = first.keys.into_iter().chain(next.keys).collect();
        let mut expected: Vec<_> = keys.into_iter().filter(|id| id.starts_with(&prefix)).map(EntityId::Binary).collect();
        expected.sort();
        assert_eq!(enumerated, expected);

        let text = rtdb_query::owned_identity_candidates_in_prefix(
            Arc::clone(&held), owner, &overlay, at, at, &OwnedIdentityScan::text(Some("world-column/fixtur")), 2,
            Some(rtdb_columnar::EntityIdRef::String("world-column/")),
        ).await?;
        assert_eq!(text.keys, [EntityId::String("world-column/fixture".into())]);
        let text_end = rtdb_query::owned_identity_candidates_in_prefix(
            held, owner, &overlay, at, at, &OwnedIdentityScan::text(Some("world-column/fixture")), 2,
            Some(rtdb_columnar::EntityIdRef::String("world-column/")),
        ).await?;
        assert!(text_end.keys.is_empty());

        let bound = |at_most| Operation::Public(TxOp::OwnedCheckCapacity {
            relation: journal.clone(), column: PROTECTED_MEMBERSHIP.into(), prefix: operand(&prefix, &fence), at_most,
        });
        candidate.check_final_constraints(&[bound(2)]).await?;
        assert_eq!(candidate.check_final_constraints(&[bound(1)]).await.unwrap_err().code, "rtdb.owned/count-bound-exceeded");
        let marker = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::Utf8(fence.clone())),
        ])], &memory)?;
        candidate.write_owned(&OwnedRelation::new(FENCES)?, &marker, None, Some(Timestamp::MIN), None, true).await?;
        candidate.check_final_constraints(&[bound(0)]).await?;
        Ok(())
    }

    #[test]
    fn protected_row_membership_admits_only_the_raw_table_operand() {
        let table = [17; 16];
        let raw = [&[3][..], table.as_slice()].concat();
        assert!(matches!(ProtectedProjection::decode(&raw), Ok(ProtectedProjection::Rows(id)) if id == &table));
        assert!(ProtectedProjection::decode(&raw[..16]).is_err());
        assert!(ProtectedProjection::decode(&[raw.as_slice(), &[0]].concat()).is_err());
        let hex = "11".repeat(16);
        assert!(ProtectedProjection::decode(&[&[0], hex.as_bytes()].concat()).is_err());
    }
}
