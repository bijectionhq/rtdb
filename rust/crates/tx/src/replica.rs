use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::StoredOperation, envelope::decode_replica_outcome_attempt, generated::xtdb::log::proto::ResolvedTx};
use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
use rtdb_types::{Anomaly, BlockIndex, EventOp, Interval, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttempt, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

use crate::{RetainedAnomaly, TransactionResult};

pub(crate) mod worlds;

const MAX_RETIRED_OUTCOMES: usize = 128;

pub struct ReplayedTransaction {
    pub writes: StagedWrite,
    pub result: TransactionResult,
    pub placed_attempt: Option<PlacedAttempt>,
}

/// Decodes the complete transaction for atomic application by the caller.
pub fn decode_replica(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<ReplayedTransaction> {
    let placed = if let Some(placed) = message.placed_attempt.as_deref() {
        let (attempt, retirement) = decode_replica_outcome_attempt(placed).map_err(|_| invalid("Invalid placed attempt identity"))?;
        let position = Some(attempt.position().0);
        if message.src_msg_id != position || message.tx_id != position
            || !message.table_data.contains_key(&format!("xt/{PLACED_ATTEMPT_OUTCOME_TABLE}"))
        {
            return Err(invalid("Placed attempt and transaction outcome disagree"));
        }
        Some((attempt, retirement))
    } else {
        if message.table_data.contains_key(&format!("xt/{PLACED_ATTEMPT_OUTCOME_TABLE}")) {
            return Err(invalid("Outcome table has no placed attempt authority"));
        }
        None
    };
    let key = TransactionKey { tx_id: TxId(message.tx_id.unwrap_or_default()), system_time: Timestamp(message.system_time_micros.unwrap_or_default()) };
    let result = if message.committed.unwrap_or_default() {
        TransactionResult::Committed { tx_key: key }
    } else {
        let error = message.error.as_deref().filter(|error| !error.is_empty()).map(|error| decode_error(error, memory)).transpose()?;
        TransactionResult::Aborted { tx_key: key, error }
    };
    let mut tables = BTreeMap::new();
    let mut owned_owner = None;
    let mut owned_scopes = BTreeSet::new();
    for (name, bytes) in &message.table_data {
        let (schema, physical_name) = name.split_once('/').ok_or_else(|| invalid("Replica table must use schema/table"))?;
        if schema.is_empty() || physical_name.is_empty() || physical_name.contains('/') { return Err(invalid("Invalid replica table name")); }
        let table = TableRef::new(schema, physical_name);
        if TableRef::owned_schema(schema) {
            if !table.supported_owned_name() {
                return Err(invalid("Unknown owned relation policy or invalid owned relation name"));
            }
            let owner = &physical_name[..64];
            if owned_owner.is_some_and(|seen| seen != owner) {
                return Err(invalid("Resolved transaction writes more than one owned principal"));
            }
            owned_owner = Some(owner);
            owned_scopes.insert(table.owned_world_scope());
        }
        let pages = crate::ipc::decode(bytes, memory)?;
        if pages.len() != 1 { return Err(invalid("Resolved transaction table must contain exactly one record batch")); }
        let batch = IndexBatch::from_owned(pages.into_iter().next().ok_or_else(|| invalid("Replica table has no batch"))?)?;
        let owned_data = table.is_owned_event_data();
        if !batch.is_empty() && batch.view().has_entity_keys() != owned_data {
            return Err(invalid("Resolved transaction has the wrong entity-key layout"));
        }
        if owned_data {
            for index in 0..batch.len() {
                let encoded = batch.view().entity_key(index)?.ok_or_else(|| invalid("Owned event has no complete key"))?;
                let _key = memory.reserve(encoded.len().saturating_mul(2))?;
                let id = rtdb_types::EntityId::from_handle_bytes(encoded)?;
                if id.iid() != batch.header(index)?.iid || id.handle_bytes().as_slice() != encoded {
                    return Err(invalid("Resolved owned event locality differs from its complete key"));
                }
            }
        }
        tables.insert(table, TableWrite::new(StructFields::new(), Some(batch)));
    }
    let outcome = placed.map(|(attempt, retirement)| read_placed_outcome(&tables, attempt, retirement, key, memory)).transpose()?;
    let proof = outcome.as_ref().map(|(proof, _)| proof);
    let world_effects = proof.and_then(|proof| proof.get(worlds::WORLD_EFFECTS));
    if owned_owner.is_some() {
        if !result.committed() || message.db_op.is_some() {
            return Err(invalid("Resolved owned transaction mixes storage scopes"));
        }
        let controls: Vec<_> = tables
            .iter()
            .filter(|(table, _)| {
                !TableRef::owned_schema(&table.schema)
                    && !(table.schema == "xt"
                        && matches!(
                            table.table.as_str(),
                            "txs" | "idempotency_receipts" | PLACED_ATTEMPT_OUTCOME_TABLE
                        ))
            })
            .collect();
        match world_effects {
            Some(Value::List(effects)) => {
                let hosted = worlds::verify(&tables, effects, key, memory)?;
                let has_controls = !controls.is_empty()
                    || proof.is_some_and(|proof| proof.contains_key(crate::hosted_control::CONTROL_TABLES));
                match hosted {
                    worlds::HostedEffects::CheckOnly if has_controls =>
                        return Err(invalid("Resolved world effects may only check hosted controls")),
                    worlds::HostedEffects::ProposalClose if !(4..=7).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved Proposal close lacks its bounded custody and receipt effects")),
                    worlds::HostedEffects::ProposalLanding if controls.iter()
                        .filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>() != 3 =>
                        return Err(invalid("Resolved Proposal landing requires its replay head, receipt and proof")),
                    worlds::HostedEffects::RequiredReplacements if controls.is_empty() =>
                        return Err(invalid("Resolved control-only retained fork has no hosted replacements")),
                    worlds::HostedEffects::RetainedRelease if !(1..=2).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved retained release differs from its bounded control replacements")),
                    worlds::HostedEffects::ProposalHold if !(1..=6).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved Proposal hold differs from its bounded control replacements")),
                    worlds::HostedEffects::ProposalOverlay if !(1..=6).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved Proposal settlement differs from its bounded control replacements")),
                    worlds::HostedEffects::RetainedWrite if !(1..=5).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved world write differs from its bounded control replacements")),
                    _ => {}
                }
                if has_controls {
                    let proof = proof.ok_or_else(|| invalid("Resolved hosted controls require a placed attempt"))?;
                    verify_hosted_controls(proof, &controls, key)?;
                }
            }
            Some(Value::Undefined | Value::Null) | None => {
                if owned_scopes.len() != 1 {
                    return Err(invalid("Cross-scope owned effects have no placed world inventory"));
                }
                if !controls.is_empty() {
                    let proof = proof.filter(|_| owned_scopes.contains(&(false, None))).ok_or_else(|| {
                        invalid("Resolved hosted controls require one placed owned root")
                    })?;
                    verify_hosted_controls(proof, &controls, key)?;
                }
            }
            Some(_) => return Err(invalid("Invalid placed world inventory carrier")),
        }
    } else if matches!(world_effects, Some(value) if !matches!(value, Value::Undefined | Value::Null)) {
        return Err(invalid("Placed world inventory has no owned effects"));
    }
    Ok(ReplayedTransaction { writes: StagedWrite::new(key, predecessor, block, tables)?, result, placed_attempt: placed.map(|(attempt, _)| attempt) })
}

fn read_placed_outcome(
    tables: &BTreeMap<TableRef, TableWrite>, attempt: PlacedAttempt, retirement: bool, key: TransactionKey,
    memory: &Arc<MemoryBudget>,
) -> Result<(StructFields<Value>, Arc<rtdb_columnar::MemoryReservation>)> {
    let mismatch = || invalid("Resolved owned effects differ from their ordered placed attempt");
    let outcome = tables
        .get(&TableRef::new("xt", PLACED_ATTEMPT_OUTCOME_TABLE))
        .and_then(TableWrite::batch)
        .filter(|batch| if retirement { (1..=MAX_RETIRED_OUTCOMES + 1).contains(&batch.len()) } else { batch.len() == 1 })
        .ok_or_else(mismatch)?;
    let decode = memory.reserve(
        outcome
            .arrow()
            .get_array_memory_size()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(mismatch)?,
    )?;
    let mut proof = None;
    let mut retired = BTreeSet::new();
    let expected = Value::Utf8(attempt.outcome_key()).entity_id()?.iid();
    for row in 0..outcome.len() {
        let header = outcome.header(row)?;
        if header.system_from != key.system_time || header.commit_position != 0 { return Err(mismatch()); }
        if retirement && header.op == EventOp::Erase
            && header.valid_time == Interval::ALL && header.iid != expected && retired.insert(header.iid)
        {
            continue;
        }
        if header.op != EventOp::Put || header.iid != expected || proof.is_some() { return Err(mismatch()); }
        let event = outcome.view().read_event(row)?;
        let StoredOperation::Put(row) = event.operation else { return Err(mismatch()); };
        proof = Some(row);
    }
    let proof = proof.ok_or_else(mismatch)?;
    if !matches!(proof.get("_id"), Some(Value::Utf8(id)) if id == &attempt.outcome_key())
        || proof.get("tx_id") != Some(&Value::I64(key.tx_id.0))
        || !matches!(proof.get("tx_system_time"), Some(Value::Timestamp { value, unit: rtdb_types::TimeUnit::Microsecond, .. }) if *value == key.system_time.0)
    {
        return Err(mismatch());
    }
    Ok((proof, decode))
}

fn verify_hosted_controls(
    proof: &StructFields<Value>, controls: &[(&TableRef, &TableWrite)], key: TransactionKey,
) -> Result<()> {
    use crate::hosted_control::{CONTROL_TABLES, admits_name};

    let mismatch = || invalid("Resolved hosted controls differ from their ordered attempt inventory");
    let Some(Value::List(names)) = proof.get(CONTROL_TABLES) else {
        return Err(mismatch());
    };
    if names.len() != controls.len() || names.len() > 32 {
        return Err(mismatch());
    }
    let mut expected = BTreeSet::new();
    for name in names {
        let Value::Utf8(name) = name else {
            return Err(mismatch());
        };
        if !admits_name(name) || !expected.insert(name.as_str()) {
            return Err(mismatch());
        }
    }
    let valid = Interval::new(Timestamp(0), Timestamp::MAX)?;
    for (table, rows) in controls {
        if table.schema != "public" || !expected.remove(table.table.as_str()) {
            return Err(mismatch());
        }
        let batch = rows
            .batch()
            .filter(|batch| !batch.is_empty())
            .ok_or_else(mismatch)?;
        for index in 0..batch.len() {
            let row = batch.header(index)?;
            if row.op != EventOp::Put
                || row.system_from != key.system_time
                || row.valid_time != valid
            {
                return Err(mismatch());
            }
        }
    }
    if !expected.is_empty() {
        return Err(mismatch());
    }
    Ok(())
}

fn decode_error(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<RetainedAnomaly> {
    let _reservation = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("Transit error allocation overflow"))?)?;
    RetainedAnomaly::from_transit(bytes, memory)
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.tx/invalid-replica-transaction", message) }
