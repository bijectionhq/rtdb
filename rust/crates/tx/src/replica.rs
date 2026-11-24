use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{EncodedEvent, StoredOperation}, envelope::decode_replica_outcome_attempt, generated::xtdb::log::proto::ResolvedTx};
use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
use rtdb_types::{Anomaly, BlockIndex, EventOp, Interval, PLACED_ATTEMPT_OUTCOME_TABLE, PlacedAttempt, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

use crate::{RetainedAnomaly, TransactionResult};

pub(crate) mod worlds;

pub(crate) const ADOPTED_HISTORY_METADATA: &str = "rtdb:adopted-history";

const MAX_RETIRED_OUTCOMES: usize = 128;

pub struct ReplayedTransaction {
    pub writes: StagedWrite,
    pub result: TransactionResult,
    pub placed_attempt: Option<PlacedAttempt>,
}

/// Decodes the complete transaction for atomic application by the caller.
pub fn decode_replica(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<ReplayedTransaction> {
    decode(message, predecessor, block, memory, false)
}

/// The held-world writer calls this after authenticating the original journal
/// frame. Validate all its effects, then keep the original business table
/// bytes and omit retry state, including the placed lookup witness.
pub fn project_held_replica(message: &mut ResolvedTx, predecessor: TransactionKey, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<()> {
    decode_replica(message, Some(predecessor), block, memory)?;
    message.table_data.retain(|name, _| !receipt_table(name));
    message.placed_attempt = None;
    message.placed_slot_terminal = None;
    // The strict acceptance witness is bound to that same placed attempt.
    // Original journal admission checked it before this projection.
    message.acceptance_stage = None;
    Ok(())
}

/// The authenticated held-world manifest attests projection of frames that
/// passed the full replica decoder. Placement-dependent checks ran before the
/// receipt witness was removed. This decoder is private to archived replay.
pub fn decode_held_replica(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>) -> Result<ReplayedTransaction> {
    if message.placed_attempt.is_some() || message.placed_slot_terminal.is_some() || message.table_data.keys().any(|name| receipt_table(name)) {
        return Err(invalid("Held business replay contains expiring transaction metadata"));
    }
    decode(message, predecessor, block, memory, true)
}

fn receipt_table(name: &str) -> bool {
    name.split_once('/').is_some_and(|(schema, table)| TableRef::new(schema, table).stores_expiring_transaction_metadata())
}

fn decode(message: &ResolvedTx, predecessor: Option<TransactionKey>, block: BlockIndex, memory: &Arc<MemoryBudget>, held: bool) -> Result<ReplayedTransaction> {
    if let Some(wire) = message.placed_slot_terminal.as_deref() {
        let terminal = rtdb_types::PlacedTerminal::decode(wire)?;
        if message.placed_attempt.is_none() || (terminal.fenced && (
            message.committed != Some(false) || message.db_op.is_some() || message.acceptance_stage.is_some()
            || message.external_source_token.is_some() || message.search_delta.is_some()
            || message.table_data.keys().any(|table| table != "xt/txs" && table != "xt/placed_attempt_outcomes")
        )) {
            return Err(invalid("A placed receiver fence cannot publish effects"));
        }
    }
    let placed = if let Some(placed) = message.placed_attempt.as_deref() {
        let (attempt, retirement) = decode_replica_outcome_attempt(placed).map_err(|_| invalid("Invalid placed attempt identity"))?;
        let position = Some(attempt.position().0);
        if message.src_msg_id != position || message.tx_id != position || message.committed.is_none()
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
        let page = pages.into_iter().next().ok_or_else(|| invalid("Replica table has no batch"))?;
        let schema = page.batch().schema_ref();
        let adopted = match schema.metadata().get(ADOPTED_HISTORY_METADATA).map(String::as_str) {
            None => false,
            Some("1") if result.committed() && message.db_op.is_none() && (held || placed.is_some()) => true,
            Some(_) => return Err(invalid("An adopted history requires a committed placed decision")),
        };
        let declarations = rtdb_format::catalog::schema_declarations(schema)?;
        if !declarations.is_empty() && message.committed != Some(true) {
            return Err(invalid("An uncommitted transaction cannot declare columns"));
        }
        let mut clean_schema = rtdb_format::catalog::without_schema_declarations(schema);
        if adopted {
            if !declarations.is_empty() { return Err(invalid("An adopted history cannot declare columns")); }
            let clean = clean_schema.unwrap_or_else(|| schema.as_ref().clone());
            let mut metadata = clean.metadata().clone();
            metadata.remove(ADOPTED_HISTORY_METADATA);
            clean_schema = Some(clean.with_metadata(metadata));
        }
        let page = match clean_schema {
            Some(schema) => {
                let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(page.batch().num_rows()));
                let rows = arrow_array::RecordBatch::try_new_with_options(Arc::new(schema), page.batch().columns().to_vec(), &options)
                    .map_err(|error| invalid(error.to_string()))?;
                rtdb_columnar::OwnedBatch::with_reservation(rows, Arc::clone(page.reservation()))?
            }
            None => page,
        };
        let batch = IndexBatch::from_owned(page)?;
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
                if matches!(&id, rtdb_types::EntityId::Binary(_)) {
                    let event = batch.view().borrow_event(index)?;
                    if event.header().op == EventOp::Put && event.put_identity()? != Some(rtdb_columnar::EntityIdRef::from(&id)) {
                        return Err(invalid("Resolved owned put differs from its complete identity"));
                    }
                }
            }
        }
        let write = TableWrite::new(StructFields::new(), Some(batch)).with_declarations(declarations);
        tables.insert(table, if adopted { write.as_adopted_history() } else { write });
    }
    check_adoption_publication(&tables, key, memory)?;
    if held {
        return Ok(ReplayedTransaction { writes: StagedWrite::new(key, predecessor, block, tables)?, result, placed_attempt: None });
    }
    let outcome = placed.map(|(attempt, retirement)| read_placed_outcome(&tables, attempt, retirement, &result, memory)).transpose()?;
    let proof = outcome.as_ref().map(|(proof, _)| proof);
    let world_effects = proof.and_then(|proof| proof.get(worlds::WORLD_EFFECTS));
    let archive_replay = match proof.and_then(|proof| proof.get(crate::hosted_control::RESOLVED_ARCHIVE_REPLAY)) {
        Some(Value::Bool(true)) => true,
        None | Some(Value::Null | Value::Undefined) => false,
        _ => return Err(invalid("Invalid resolved archive replay marker")),
    };
    if owned_owner.is_some() || archive_replay {
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
        match (archive_replay, world_effects) {
            (true, None | Some(Value::Undefined | Value::Null)) => {
                let proof = proof.ok_or_else(|| invalid("Resolved archive replay has no placed attempt"))?;
                verify_hosted_controls(proof, &controls, key, memory)?;
                verify_archive_replay(&tables, key, memory)?;
            }
            (true, Some(_)) => return Err(invalid("Resolved archive replay mixes live and copied world effects")),
            (false, Some(Value::List(effects))) => {
                let effects = effects.scalar_values().ok_or_else(|| invalid("Resolved world effects have an invalid layout"))?;
                let family_fence = controls.iter().any(|(table, _)| {
                    table.schema == "public" && table.table == rtdb_types::ASSERTION_FAMILY_FENCES
                });
                let hosted = worlds::verify(&tables, effects, key, memory, family_fence)?;
                let ordinary_control_rows = || controls.iter()
                    .filter(|(table, _)| table.table != rtdb_types::ASSERTION_FAMILY_FENCES)
                    .filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>();
                let has_controls = !controls.is_empty()
                    || proof.is_some_and(|proof| proof.contains_key(crate::hosted_control::CONTROL_TABLES));
                match hosted {
                    worlds::HostedEffects::CheckOnly if has_controls && !(family_fence && controls.len() == 1) =>
                        return Err(invalid("Resolved world effects may only check hosted controls")),
                    worlds::HostedEffects::ProposalClose if !(4..=7).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved Proposal close lacks its bounded custody and receipt effects")),
                    worlds::HostedEffects::ProposalLanding => verify_proposal_landing_controls(&controls, memory)?,
                    worlds::HostedEffects::RequiredReplacements if controls.is_empty() =>
                        return Err(invalid("Resolved control-only retained fork has no hosted replacements")),
                    worlds::HostedEffects::RetainedRelease if !(1..=2).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved retained release differs from its bounded control replacements")),
                    worlds::HostedEffects::ProposalHold if !(1..=6).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved Proposal hold differs from its bounded control replacements")),
                    worlds::HostedEffects::ProposalOverlay if !(1..=6).contains(&ordinary_control_rows()) => return Err(invalid("Resolved Proposal settlement differs from its bounded control replacements")),
                    worlds::HostedEffects::RetainedWrite if !(1..=5).contains(
                        &controls.iter().filter_map(|(_, write)| write.batch()).map(|batch| batch.len()).sum::<usize>()
                    ) => return Err(invalid("Resolved world write differs from its bounded control replacements")),
                    _ => {}
                }
                if has_controls {
                    let proof = proof.ok_or_else(|| invalid("Resolved hosted controls require a placed attempt"))?;
                    verify_hosted_controls(proof, &controls, key, memory)?;
                }
            }
            (false, Some(Value::Undefined | Value::Null) | None) => {
                if owned_scopes.len() != 1 {
                    return Err(invalid("Cross-scope owned effects have no placed world inventory"));
                }
                if !controls.is_empty() {
                    let proof = proof.filter(|_| owned_scopes.contains(&(false, None))).ok_or_else(|| {
                        invalid("Resolved hosted controls require one placed owned root")
                    })?;
                    verify_hosted_controls(proof, &controls, key, memory)?;
                }
            }
            (false, Some(_)) => return Err(invalid("Invalid placed world inventory carrier")),
        }
    } else if matches!(world_effects, Some(value) if !matches!(value, Value::Undefined | Value::Null)) {
        return Err(invalid("Placed world inventory has no owned effects"));
    }
    Ok(ReplayedTransaction { writes: StagedWrite::new(key, predecessor, block, tables)?, result, placed_attempt: placed.map(|(attempt, _)| attempt) })
}

fn check_adoption_publication(tables: &BTreeMap<TableRef, TableWrite>, key: TransactionKey, memory: &Arc<MemoryBudget>) -> Result<()> {
    let Some((table, _)) = tables.iter().find(|(table, write)| write.is_adopted_history() && table.is_owned_event_data())
    else { return Ok(()); };
    let invalid = || invalid("Adopted history has no atomic Root publication and retirement");
    let (owner, destination) = table.table.split_once('-').ok_or_else(invalid)?;
    let role = |suffix| TableRef::new(&table.schema, format!("{owner}-{suffix}"));
    let adoption = tables.get(&role(".adopted-relations")).and_then(TableWrite::batch).ok_or_else(invalid)?;
    let attempts = tables.get(&role(rtdb_types::OWNED_ATTEMPT_RELATION)).and_then(TableWrite::batch).ok_or_else(invalid)?;
    let publications = tables.get(&role(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)).and_then(TableWrite::batch).ok_or_else(invalid)?;
    let retirement = tables.get(&role(".retired-relations")).and_then(TableWrite::batch).ok_or_else(invalid)?;
    let bytes = [adoption, publications, retirement].into_iter().try_fold(0usize, |bytes, batch| {
        bytes.checked_add(batch.arrow().get_array_memory_size()).ok_or_else(invalid)
    })?;
    if bytes > 256 * 1024 || attempts.is_empty()
        || adoption.len() != 1 || !adoption.view().header(0).is_ok_and(|header| header.system_from == key.system_time)
        || [TableRef::new("public", "bj_fact_root_owner"), TableRef::new("public", "bj_relation_declarations")]
            .iter().any(|table| tables.get(table).is_none_or(|write| write.row_count() == 0))
    {
        return Err(invalid());
    }
    let _scratch = memory.reserve(bytes * 8 + 4096)?;
    let StoredOperation::Put(row) = adoption.view().read_event(0)?.operation else { return Err(invalid()); };
    let Some(Value::Utf8(source)) = row.get("predecessor") else { return Err(invalid()); };
    if source == destination || !rtdb_types::OwnedRelation::new(source.as_str()).is_ok_and(|relation| relation.is_root_data())
        || !matches!(row.get("_id"), Some(Value::Utf8(id)) if id == destination)
        || row.get("activation_tx") != Some(&Value::I64(key.tx_id.0))
        || row.get("activation_micros") != Some(&Value::I64(key.system_time.0))
        || tables.contains_key(&role(source))
        || tables.contains_key(&role(&format!(".changes-{source}")))
    {
        return Err(invalid());
    }
    let head = format!("fact_partition_head:{destination}");
    let mut heads = 0;
    for index in 0..publications.len() {
        if let StoredOperation::Put(row) = publications.view().read_event(index)?.operation
            && matches!(row.get("_id"), Some(Value::Utf8(id)) if id == &head)
            && publications.header(index)?.valid_time == Interval::ALL
        {
            heads += 1;
        }
    }
    if heads != 1 { return Err(invalid()); }
    for index in 0..retirement.len() {
        if let StoredOperation::Put(row) = retirement.view().read_event(index)?.operation
            && matches!(row.get("_id"), Some(Value::Utf8(id)) if id == source)
            && rtdb_types::OwnedRetirement::decode(&row).is_ok_and(|(_, retired)| retired.retired_at == key.system_time)
        {
            return Ok(());
        }
    }
    Err(invalid())
}

fn verify_archive_replay(
    tables: &BTreeMap<TableRef, TableWrite>, key: TransactionKey, memory: &Arc<MemoryBudget>,
) -> Result<()> {
    use crate::hosted_control::{HostedReplay, NATIVE_FRONTIER, RESTORE};

    let mismatch = || invalid("Resolved archive effects differ from their held replay inventory");
    let hold = tables.get(&TableRef::new("public", RESTORE))
        .and_then(TableWrite::batch).ok_or_else(mismatch)?;
    let _charge = memory.reserve(hold.arrow().get_array_memory_size().checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(mismatch)?)?;
    let binding = |id: &str| -> Result<Option<Vec<u8>>> {
        let iid = rtdb_types::EntityId::String(id.to_owned()).iid();
        let mut body = None;
        for index in 0..hold.len() {
            if hold.header(index)?.iid != iid {
                continue;
            }
            let StoredOperation::Put(row) = hold.view().read_event(index)?.operation else {
                return Err(mismatch());
            };
            if !matches!(row.get("_id"), Some(Value::Utf8(key)) if key == id) {
                continue;
            }
            let Some(Value::Binary(bytes)) = row.get("binding") else { return Err(mismatch()); };
            if body.replace(bytes.to_vec()).is_some() {
                return Err(mismatch());
            }
        }
        Ok(body)
    };
    let frontier = binding(NATIVE_FRONTIER)?.ok_or_else(mismatch)?;
    let replay = HostedReplay::decode_resolved(&frontier, key.system_time)?;
    if binding(&replay.transaction_locator())?.as_deref() != Some(frontier.as_slice())
        || binding(&HostedReplay::destination_locator(key.tx_id))?.as_deref()
            != Some(replay.original_receipt_key().as_slice())
    {
        return Err(mismatch());
    }
    let mut positions: BTreeSet<_> = replay.commit_positions().iter().copied().collect();
    if let Some(omissions) = binding(&replay.world_omissions_locator())? {
        if omissions.is_empty() || omissions.len() / 20 > positions.len() {
            return Err(mismatch());
        }
        let (entries, remainder) = omissions.as_chunks::<20>();
        if !remainder.is_empty() {
            return Err(mismatch());
        }
        for entry in entries {
            let (ordinal, world) = entry.split_at(4);
            let ordinal = u32::from_be_bytes(ordinal.try_into().map_err(|_| mismatch())?);
            if world == [0; 16] || !positions.remove(&ordinal) {
                return Err(mismatch());
            }
            replay.require_assertion_line(ordinal, world.try_into().map_err(|_| mismatch())?)?;
        }
    }
    for (table, write) in tables {
        let topology = table.is_owned_world_topology();
        if !table.is_owned_event_data() && !topology {
            continue;
        }
        if let Some(batch) = write.batch() {
            for index in 0..batch.len() {
                let event = batch.header(index)?;
                if event.system_from != key.system_time {
                    return Err(mismatch());
                }
                if event.commit_position != 0 {
                    if !positions.remove(&event.commit_position) {
                        return Err(mismatch());
                    }
                    let line = if topology {
                        let id = batch.view().borrow_event(index)?.put_identity()?;
                        if event.valid_time != Interval::ALL || event.iid.0 == [0; 16]
                            || id != Some(rtdb_columnar::EntityIdRef::Uuid(event.iid.0))
                        {
                            return Err(mismatch());
                        }
                        event.iid.0
                    } else {
                        table.owned_world_scope().1.map_or([0; 16], |world| world.0)
                    };
                    replay.require_assertion_line(event.commit_position, line)?;
                }
            }
        }
    }
    if !positions.is_empty() {
        return Err(mismatch());
    }
    Ok(())
}

fn read_placed_outcome(
    tables: &BTreeMap<TableRef, TableWrite>, attempt: PlacedAttempt, retirement: bool, result: &TransactionResult,
    memory: &Arc<MemoryBudget>,
) -> Result<(StructFields<Value>, Arc<rtdb_columnar::MemoryReservation>)> {
    let key = result.tx_key();
    let mismatch = || invalid("Resolved transaction differs from its ordered placed attempt");
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
        || proof.get("committed") != Some(&Value::Bool(result.committed()))
        || proof.get("tx_id") != Some(&Value::I64(key.tx_id.0))
        || !matches!(proof.get("tx_system_time"), Some(Value::Timestamp { value, unit: rtdb_types::TimeUnit::Microsecond, .. }) if *value == key.system_time.0)
    {
        return Err(mismatch());
    }
    Ok((proof, decode))
}

fn verify_proposal_landing_controls(
    controls: &[(&TableRef, &TableWrite)], memory: &Arc<MemoryBudget>,
) -> Result<()> {
    use crate::hosted_control::{MAX_PROPOSAL_LAND_TASKS, proposal_landing_replacements};

    let mismatch = || invalid("Resolved Proposal landing differs from its exact control inventory");
    let [(table, write)] = controls else { return Err(mismatch()); };
    if table.schema != "public" || table.table != "bj_fact_root_admission" {
        return Err(mismatch());
    }
    let batch = write.batch().filter(|batch| batch.len() <= MAX_PROPOSAL_LAND_TASKS + 8).ok_or_else(mismatch)?;
    let bytes = batch.arrow().get_array_memory_size().checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(mismatch)?;
    let _charge = memory.reserve(bytes)?;
    let mut ids = Vec::new();
    for index in 0..batch.len() {
        let StoredOperation::Put(mut row) = batch.view().read_event(index)?.operation else { return Err(mismatch()); };
        let Some(Value::Utf8(id)) = row.remove("_id") else { return Err(mismatch()); };
        ids.push(id);
    }
    if !proposal_landing_replacements(ids.iter().map(String::as_str)) {
        return Err(mismatch());
    }
    Ok(())
}

fn verify_hosted_controls(
    proof: &StructFields<Value>, controls: &[(&TableRef, &TableWrite)], key: TransactionKey,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    use crate::hosted_control::{CONTROL_TABLES, admits_name};

    let mismatch = || invalid("Resolved hosted controls differ from their ordered attempt inventory");
    let Some(Value::List(names)) = proof.get(CONTROL_TABLES) else {
        return Err(mismatch());
    };
    if names.is_empty() || names.len() != controls.len() || names.len() > 32 {
        return Err(mismatch());
    }
    let mut expected = BTreeSet::new();
    for index in 0..names.len() {
        let name = names.view().utf8(index).ok_or_else(mismatch)?;
        if !admits_name(name) || !expected.insert(name) {
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
        if table.table == rtdb_types::ASSERTION_FAMILY_FENCES {
            use rtdb_types::AssertionFamilyFenceMarker;

            if batch.len() != 1 { return Err(mismatch()); }
            let bound = batch.arrow().get_array_memory_size().saturating_mul(4).saturating_add(8192);
            let _charge = memory.reserve(bound)?;
            let StoredOperation::Put(row) = batch.view().read_event(0)?.operation
            else { return Err(mismatch()); };
            let (Some(Value::Utf8(family)), Some(Value::Binary(body))) = (row.get("_id"), row.get("body"))
            else { return Err(mismatch()); };
            if row.len() != 2 || AssertionFamilyFenceMarker::family_id(family).is_err()
                || AssertionFamilyFenceMarker::decode(body).is_err()
            {
                return Err(mismatch());
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::data::StoredEvent;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_types::{EntityId, Iid, OwnedRelation};

    #[test]
    fn replica_and_checkpoint_preserve_only_explicit_declarations_as_provenance() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let state = PartitionState::empty(Default::default())?;
        let table = TableRef::new("public", "orders");
        let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory))?;
        transaction.declare_table(table.clone(), ["declared".to_owned()])?;
        transaction.record_sql_declaration(&table, &[("declared".into(), rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Utf8))])?;
        transaction.put(table.clone(), StructFields::from([
            ("_id".into(), Value::Utf8("one".into())),
            ("row_only".into(), Value::I64(7)),
        ]), Interval::ALL)?;
        let staged = transaction.stage()?;
        let write = &staged.tables()[&table];
        let batch = write.batch().unwrap().arrow();
        let schema = rtdb_format::catalog::schema_with_declarations(batch.schema_ref().as_ref().clone(), write.declared_columns())?;
        let batch = batch.clone().with_schema(Arc::new(schema)).unwrap();
        let bytes = crate::ipc::encode(batch.schema_ref(), [&batch], &memory)?;
        let mut frame = ResolvedTx {
            tx_id: Some(key.tx_id.0),
            src_msg_id: Some(key.tx_id.0),
            system_time_micros: Some(key.system_time.0),
            committed: Some(true),
            ..Default::default()
        };
        frame.table_data.insert(table.schema_and_table(), bytes.into_parts().0);
        let replayed = decode_replica(&frame, None, staged.block_index(), &memory)?;
        state.apply(&replayed.writes)?;
        let snapshot = state.snapshot();
        assert!(snapshot.column_types(&table).contains_key("row_only"));
        assert_eq!(&snapshot.declared_columns(&table), write.declared_columns());
        let prepared = state.prepare_block()?;
        let blocks = prepared.table_blocks(&BTreeMap::new(), key.system_time)?;
        let reopened = rtdb_format::catalog::TableBlock::decode(&blocks[&table].encode())?;
        assert_eq!(&reopened.declared_columns()?, write.declared_columns());
        Ok(())
    }

    #[test]
    fn held_projection_preserves_the_exact_business_batch() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let state = PartitionState::empty(Default::default())?;
        let previous = TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) };
        let key = TransactionKey { tx_id: TxId(2), system_time: Timestamp(20) };
        let mut transaction = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory))?;
        let business = TableRef::new("public", "orders");
        let receipt = TableRef::new("xt", "idempotency_receipts");
        for (table, id) in [(&business, "order"), (&receipt, "retry")] {
            transaction.put(table.clone(), StructFields::from([
                ("_id".into(), Value::Utf8(id.into())),
            ]), Interval::ALL)?;
        }
        let staged = transaction.stage()?;
        let mut frame = ResolvedTx {
            tx_id: Some(key.tx_id.0),
            src_msg_id: Some(key.tx_id.0),
            system_time_micros: Some(key.system_time.0),
            committed: Some(true),
            ..Default::default()
        };
        for (table, write) in staged.tables() {
            let batch = write.batch().unwrap().arrow();
            let bytes = crate::ipc::encode(batch.schema().as_ref(), [batch], &memory)?;
            frame.table_data.insert(table.schema_and_table(), bytes.into_bytes());
        }
        let original = frame.table_data[&business.schema_and_table()].clone();
        assert!(decode_held_replica(&frame, Some(previous), staged.block_index(), &memory).is_err());

        project_held_replica(&mut frame, previous, staged.block_index(), &memory)?;
        assert_eq!(frame.table_data.len(), 1);
        assert_eq!(frame.table_data[&business.schema_and_table()], original);
        let replayed = decode_held_replica(&frame, Some(previous), staged.block_index(), &memory)?;
        assert_eq!(replayed.result.tx_key(), key);
        assert!(replayed.writes.tables().contains_key(&business));
        assert!(!replayed.writes.tables().contains_key(&receipt));
        Ok(())
    }

    #[test]
    fn archive_replay_accounts_for_copied_and_omitted_world_ordinals() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let state = PartitionState::empty(Default::default())?;
        let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(5) };
        let receipt = serde_json::json!({
            "binding": vec![1; 32], "archive": vec![2; 32], "original_authority": vec![3; 16],
            "captured": {"tx_id": 3, "system_time": 4},
            "handoff": null, "through": 4000, "previous": null,
            "source": {"tx_id": 3, "system_time": 4}, "logical_at": 4000,
            "fingerprint": vec![4; 32], "commit_positions": [1, 2, 4],
            "assertion_lines": {"lines": [vec![1; 16], vec![2; 16]], "by_position": [0, 1, 0]},
        }).to_string().into_bytes();
        let hold = TableRef::new("public", crate::hosted_control::RESTORE);
        let original = EntityId::Binary(vec![0, 255, 0]);
        for (copied, copied_line, omitted, omitted_line, clock_line, clock_id, source_id, valid) in [
            (1u32, 1, 2u32, 2, 1, 1, Some(3i64), true),
            (3, 1, 2, 2, 1, 1, Some(3), false),
            (2, 2, 2, 2, 1, 1, Some(3), false),
            (1, 2, 2, 2, 1, 1, Some(3), false),
            (1, 1, 2, 1, 1, 1, Some(3), false),
            (1, 1, 2, 2, 2, 2, Some(3), false),
            (1, 1, 2, 2, 1, 2, Some(3), false),
            (1, 1, 2, 2, 1, 1, Some(2), false),
            (1, 1, 2, 2, 1, 1, None, false),
        ] {
            let overlay = OwnedRelation::new("locations")?.in_world(Iid([copied_line; 16]))?.table("owner");
            let mut transaction = TransactionBuffer::new(state.snapshot(), key, Arc::clone(&memory))?;
            transaction.retain_archived_owned_positions(vec![copied, 4])?;
            transaction.append_archived_world_effect(&overlay, StoredEvent { position: None,
                origin: None,
                commit_position: 0,
                entity_key: Some(original.handle_bytes().into()),
                iid: original.iid(),
                system_from: key.system_time,
                valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), original.as_value())])),
                recency: None,
            }, std::num::NonZeroU32::new(copied).unwrap())?;
            let world = Iid([clock_line; 16]);
            transaction.append_archived_world_effect(&OwnedRelation::world_clock_table("owner"), StoredEvent {
                position: None, origin: None, commit_position: 0, entity_key: None,
                iid: world, system_from: key.system_time, valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), Value::Uuid([clock_id; 16])), ("clock".into(), Value::I64(30)),
                ])),
                recency: None,
            }, std::num::NonZeroU32::new(4).unwrap())?;
            let mut omission = omitted.to_be_bytes().to_vec();
            omission.extend_from_slice(&[omitted_line; 16]);
            for (id, body) in [
                (crate::hosted_control::NATIVE_FRONTIER, receipt.clone()),
                ("native-transaction/3", receipt.clone()),
                ("native-world-omissions/3", omission),
            ] {
                transaction.put(hold.clone(), StructFields::from([
                    ("_id".into(), Value::Utf8(id.into())),
                    ("binding".into(), Value::Binary(body)),
                ]), Interval::new(Timestamp(0), Timestamp::MAX)?)?;
            }
            if let Some(source_id) = source_id {
                transaction.put(hold.clone(), StructFields::from([
                    ("_id".into(), Value::Utf8("native-destination/0".into())),
                    ("binding".into(), Value::Binary(source_id.to_be_bytes().to_vec())),
                ]), Interval::new(Timestamp(0), Timestamp::MAX)?)?;
            }
            let staged = transaction.stage()?;
            let shadow = staged.tables()[&overlay].batch().unwrap().view().borrow_event(0)?;
            assert_eq!(shadow.header().commit_position, copied);
            assert_eq!(shadow.physical_entity_key(), Some(original.handle_bytes().as_slice()));
            assert_eq!(shadow.put_identity()?, Some(rtdb_columnar::EntityIdRef::from(&original)));
            assert_eq!(verify_archive_replay(staged.tables(), key, &memory).is_ok(), valid);
        }
        Ok(())
    }
}
