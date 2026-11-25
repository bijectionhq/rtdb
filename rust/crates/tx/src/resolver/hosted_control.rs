use std::collections::BTreeSet;
use rtdb_types::{EntityId, OwnedRelation, SharedColumnCarrierPrefix, SHARED_COLUMN_CARRIERS};
use sha2::{Digest, Sha256};

use crate::HostedControlWrite;

use super::*;
use super::control_order::HostedStamp;
use super::owned_flight_decision::{ControlDecision, check_controls};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FundedSeat {
    until: u64,
    offer: [u8; 32],
}

fn funded_booking_missing() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/funded-booking",
        "The ordered mutation seat differs from the original funded booking")
}

fn invalid_family_fence() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/assertion-family-fence-marker", "Invalid ordered family fence marker")
}

const SHARED_COLUMN_RETIREMENTS: &str = "fact_unit_column_retirements";
const MAX_SEALED_UNIT_ROWS: usize = 2048;

fn invalid_shared_retirement() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/shared-column-retirement", "Invalid ordered shared-column retirement")
}

struct SharedRetirement<'a> {
    unit: &'a [u8],
    member: &'a [u8; 32],
    accepted: Timestamp,
    fence: Timestamp,
    ciphertext: &'a [u8; 32],
}

impl<'a> SharedRetirement<'a> {
    fn decode(body: &'a [u8], carrier: &[u8]) -> Option<Self> {
        SharedColumnCarrierPrefix::of_identity(carrier)?;
        let body = body.strip_prefix(b"BJSC2\x02")?;
        let (scope, body) = body.split_at_checked(48)?;
        if carrier.get(5..53)? != scope { return None; }
        let (member, body) = body.split_first_chunk::<32>()?;
        let (accepted, body) = body.split_first_chunk::<8>()?;
        let (fence, body) = body.split_first_chunk::<8>()?;
        let (ciphertext, body) = body.split_first_chunk::<32>()?;
        let (length, saved) = body.split_first_chunk::<4>()?;
        if usize::try_from(u32::from_be_bytes(*length)).ok()? != carrier.len() || saved != carrier {
            return None;
        }
        let revision = |bytes| {
            let nanos = u64::from_be_bytes(bytes);
            (nanos % 1000 == 0).then(|| Timestamp((nanos / 1000) as i64))
        };
        let accepted = revision(*accepted)?;
        let fence = revision(*fence)?;
        (accepted < fence).then_some(Self { unit: &scope[16..], member, accepted, fence, ciphertext })
    }
}

fn hex_identity(bytes: &[u8]) -> String {
    use core::fmt::Write as _;
    let mut id = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        write!(id, "{byte:02x}").expect("Writing to a String cannot fail");
    }
    id
}

#[cfg(test)]
mod shared_retirement_tests {
    use super::{SharedColumnCarrierPrefix, SharedRetirement, Timestamp};

    #[test]
    fn retirement_receipt_names_the_complete_carrier_and_ordered_unit_scope() {
        let mut carrier = vec![7; SharedColumnCarrierPrefix::ID_WIDTH];
        carrier[..5].copy_from_slice(b"BFCP2");
        let mut receipt = b"BJSC2\x02".to_vec();
        receipt.extend_from_slice(&carrier[5..53]);
        receipt.extend_from_slice(&[2; 32]);
        receipt.extend_from_slice(&17000u64.to_be_bytes());
        receipt.extend_from_slice(&19000u64.to_be_bytes());
        receipt.extend_from_slice(&[3; 32]);
        receipt.extend_from_slice(&(carrier.len() as u32).to_be_bytes());
        receipt.extend_from_slice(&carrier);
        let admitted = SharedRetirement::decode(&receipt, &carrier).unwrap();
        assert_eq!(admitted.unit, &[7; 32]);
        assert_eq!(admitted.member, &[2; 32]);
        assert_eq!((admitted.accepted, admitted.fence), (Timestamp(17), Timestamp(19)));

        let mut foreign = carrier.clone();
        foreign[5 + 16] ^= 1;
        assert!(SharedRetirement::decode(&receipt, &foreign).is_none());
        foreign = carrier.clone();
        *foreign.last_mut().unwrap() ^= 1;
        assert!(SharedRetirement::decode(&receipt, &foreign).is_none());
        assert!(SharedRetirement::decode(&receipt[..receipt.len() - 1], &carrier).is_none());

        let mut later_acceptance = receipt.clone();
        let offset = b"BJSC2\x02".len() + 48 + 32;
        later_acceptance[offset..offset + 8].copy_from_slice(&21000u64.to_be_bytes());
        assert!(SharedRetirement::decode(&later_acceptance, &carrier).is_none());
        receipt[5] = 1;
        assert!(SharedRetirement::decode(&receipt, &carrier).is_none());
    }
}

fn family_retirement(operation: &Operation) -> Result<Option<(Option<Iid>, EntityId, [u8; 32])>> {
    use crate::hosted_control::OwnedDocumentRow;

    let Some(write) = crate::hosted_control::owned_document_write(operation) else { return Ok(None); };
    let world = write.relation.world();
    if write.relation.root_relation().as_ref().unwrap_or(write.relation).name() != "fact_unit_fences" {
        return Ok(None);
    }
    let inventory = |phase: Option<&[u8]>, inventory: Option<&[u8]>| {
        if phase != Some(b"BFU1\x01".as_slice()) { return None; }
        inventory.and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
    };
    let mut retirement = None;
    for row in write.documents.rows() {
        let digest = match row {
            OwnedDocumentRow::Values(values) => {
                let binary = |name| match values.get(name) {
                    Some(Value::Binary(bytes)) => Some(bytes.as_slice()),
                    _ => None,
                };
                inventory(binary("unit_phase"), binary("assertion_vault_inventory"))
            }
            OwnedDocumentRow::Arrow(page, index) => {
                let cell = |name| {
                    if page.legacy_owned_presence().is_some() {
                        return page.legacy_owned_cell(name, index);
                    }
                    let array = page.array();
                    Ok(array.fields().iter().zip(array.columns())
                        .find(|(field, _)| field.name() == name)
                        .map(|(field, array)| ArrowCell { field, array: array.as_ref(), row: index }))
                };
                inventory(cell("unit_phase")?.and_then(ArrowCell::binary),
                    cell("assertion_vault_inventory")?.and_then(ArrowCell::binary))
            }
        };
        if let Some(digest) = digest {
            let all_time = write.from == Some(Timestamp::MIN)
                && (write.to == Some(Timestamp::MAX) || world.is_none() && write.to.is_none());
            if !all_time || retirement.replace((world, row.id()?, digest)).is_some() {
                return Err(invalid_family_fence());
            }
        }
    }
    Ok(retirement)
}

impl OpenTransaction<'_> {
    pub(super) async fn check_shared_column_retirement(
        &self, owner: &str, operations: &[Operation], replay: bool,
    ) -> Result<()> {
        if replay { return Ok(()); }
        let Some((carriers, id)) = operations.iter().find_map(|operation| match operation {
            Operation::Public(TxOp::OwnedErase { relation, id }) if relation.name() == SHARED_COLUMN_CARRIERS =>
                Some((relation, id)),
            _ => None,
        }) else { return Ok(()); };
        let invalid = invalid_shared_retirement;
        let EntityId::Binary(carrier) = id else { return Err(invalid()); };
        let mut authority = false;
        let mut erases = 0;
        let mut selection = None;
        let mut marker = None;
        for operation in operations {
            match operation {
                Operation::Public(TxOp::OwnedDeadline { .. }) => (),
                Operation::Public(TxOp::HostedControl(control)) if control.is_check() => {
                    authority |= control.name() == crate::hosted_control::INSTALLATION_AUTHORITY_RELATION
                        && control.expected().is_some()
                        && matches!(control.id(), EntityId::String(id) if id == crate::hosted_control::INSTALLATION_AUTHORITY_ID);
                },
                Operation::Public(TxOp::OwnedCheck { relation, id: checked, valid_at, expected: Some(expected) })
                    if relation == carriers && checked == id && selection.is_none() => {
                    selection = Some((*valid_at, expected.history));
                },
                Operation::Public(TxOp::OwnedErase { relation, id: erased }) if relation == carriers && erased == id => erases += 1,
                _ => {
                    let write = crate::hosted_control::owned_document_write(operation).ok_or_else(invalid)?;
                    if marker.is_some() || write.relation.name() != SHARED_COLUMN_RETIREMENTS || !write.create_only
                        || write.from != Some(Timestamp::MIN) || !matches!(write.to, None | Some(Timestamp::MAX))
                    {
                        return Err(invalid());
                    }
                    let mut rows = write.documents.rows();
                    let row = rows.next().ok_or_else(invalid)?;
                    if rows.next().is_some() { return Err(invalid()); }
                    marker = Some((row.string_id().ok_or_else(invalid)?,
                        row.binary("retirement_receipt", invalid)?.ok_or_else(invalid)?));
                },
            }
        }
        if !authority || erases != 1 { return Err(invalid()); }
        let (marker_id, body) = marker.ok_or_else(invalid)?;
        let receipt = SharedRetirement::decode(body, carrier).ok_or_else(invalid)?;
        let member_id = hex_identity(receipt.member);
        let (at, stamp) = selection.ok_or_else(invalid)?;
        if stamp != receipt.accepted || marker_id != member_id { return Err(invalid()); }

        let payload = self.root_point(owner, carriers, id, at, invalid).await?;
        let ciphertext = ArrowCell::named(payload.batch(), "sealed_payload", 0)
            .and_then(ArrowCell::binary).ok_or_else(invalid)?;
        if &*Sha256::digest(ciphertext) != receipt.ciphertext
            || rtdb_query::owned_row_revision(payload.batch(), 0, id, &self.context.memory)? != receipt.accepted
        {
            return Err(invalid());
        }
        drop(payload);
        let valid = self.decision_time();
        let fence_id = EntityId::String(hex_identity(receipt.unit));
        let fence = self.root_point(owner, &OwnedRelation::new("fact_unit_fences")?, &fence_id, valid, invalid).await?;
        if ArrowCell::named(fence.batch(), "unit_phase", 0).and_then(ArrowCell::binary) != Some(b"BFU1\x01")
            || rtdb_query::owned_row_revision(fence.batch(), 0, &fence_id, &self.context.memory)? != receipt.fence
        {
            return Err(invalid());
        }
        drop(fence);
        let member_id = EntityId::String(member_id);
        let member = self.root_point(
            owner, &OwnedRelation::new("fact_column_memberships")?, &member_id, valid, invalid,
        ).await?;
        let locator = ArrowCell::named(member.batch(), "column_locator", 0).and_then(ArrowCell::binary).ok_or_else(invalid)?;
        if locator.strip_prefix(b"BJSC2\x00").and_then(|body| body.strip_prefix(carrier.as_slice())).is_none_or(<[u8]>::is_empty)
            || &*Sha256::digest(locator) != receipt.member
            || rtdb_query::owned_row_revision(member.batch(), 0, &member_id, &self.context.memory)? != receipt.accepted
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) async fn check_shared_search_erasure(
        &mut self, owner: &str, operations: &[Operation], replay: bool,
    ) -> Result<()> {
        use rtdb_types::{RootRowCarrier, SEALED_SEARCH_UNIT_ROWS, is_rank_row_address, sealed_search_inventory_row};

        if replay {
            return self.check_replayed_rank_erasure(owner, operations).await;
        }
        if operations.iter().any(|operation| matches!(operation,
            Operation::Public(TxOp::HostedControl(control)) if control.is_assertion_family_marker()
        )) || !operations.iter().any(|operation| matches!(operation,
            Operation::Public(TxOp::OwnedErase { relation, .. }) if crate::hosted_control::family_root_erase(relation)
        )) {
            return Ok(());
        }
        let invalid = || Anomaly::incorrect("rtdb.owned/shared-search-retirement", "Invalid ordered shared search retirement");
        let _scratch = self.context.memory.reserve(operations.len().saturating_mul(96))?;
        let mut erases = BTreeMap::<&str, BTreeSet<&[u8]>>::new();
        let mut count = 0;
        let mut markers = BTreeSet::new();
        let mut unit = None;
        let mut creates = None;
        let mut row_checks = BTreeSet::new();
        let mut row_fences = BTreeSet::new();
        let mut row_retirements = BTreeSet::new();
        let mut witness = 0;
        let mut receipt = false;
        for operation in operations {
            match operation {
                Operation::Public(TxOp::HostedControl(control)) => {
                    receipt |= control.name() == rtdb_types::HOSTED_ROOT_RECEIPTS_TABLE && !control.is_check();
                },
                Operation::Public(TxOp::OwnedErase { relation, id: EntityId::Binary(id) })
                    if crate::hosted_control::family_root_erase(relation) => {
                    if !erases.entry(relation.name()).or_default().insert(id)
                        || !is_rank_row_address(id) || count == MAX_SEALED_UNIT_ROWS
                    {
                        return Err(invalid());
                    }
                    count += 1;
                },
                Operation::Public(TxOp::OwnedErase { .. }) => return Err(invalid()),
                Operation::Public(TxOp::OwnedCheckRange { relation, .. }) if relation.name() == SEALED_SEARCH_UNIT_ROWS =>
                    witness += 1,
                Operation::Public(TxOp::OwnedCheck { relation, id: EntityId::String(alias), expected, valid_at })
                    if relation.name() == "fact_unit_fences" => {
                    if alias.starts_with("row/") {
                        if expected.is_some() || !row_checks.insert(alias.as_str()) { return Err(invalid()); }
                        continue;
                    }
                    if unit.replace((alias.as_str(), expected.is_some(), *valid_at)).is_some()
                        || alias.len() != 64 || !alias.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                    {
                        return Err(invalid());
                    }
                },
                _ => {
                    let Some(write) = crate::hosted_control::owned_document_write(operation) else { continue; };
                    if write.relation.name() == "fact_sealed_search_erasures" {
                        if !crate::hosted_control::family_search_erase_marker(&write) { return Err(invalid()); }
                        for row in write.documents.rows() {
                            markers.insert(row.string_id().ok_or_else(invalid)?);
                        }
                    } else if write.relation.name() == "fact_unit_fences" {
                        let mut rows = write.documents.rows();
                        let row = rows.next().ok_or_else(invalid)?;
                        if !write.create_only || write.from != Some(Timestamp::MIN)
                            || !matches!(write.to, None | Some(Timestamp::MAX)) || rows.next().is_some()
                            || row.binary("unit_phase", invalid)? != Some(b"BFU1\x01")
                        {
                            return Err(invalid());
                        }
                        let name = row.string_id().ok_or_else(invalid)?;
                        if name.starts_with("row/") {
                            if !row_fences.insert(name) { return Err(invalid()); }
                        } else if creates.replace(name).is_some() { return Err(invalid()); }
                    } else if write.relation.is_root_data() && write.relation.name().starts_with("fact_")
                        && write.create_only && write.from.is_none() && write.to.is_none()
                    {
                        for row in write.documents.rows() {
                            if let Some(address) = row.binary_id().and_then(|id| id.strip_prefix(&[0]))
                                && RootRowCarrier::parse(address).is_some()
                                && (!row.only_id() || !row_retirements.insert((write.relation.name(), address)))
                            { return Err(invalid()); }
                        }
                    }
                },
            }
        }
        if !receipt || !markers.into_iter().eq(erases.keys().copied()) {
            return Err(invalid());
        }
        if unit.is_none() && creates.is_none() && witness == 0 {
            for (relation, keys) in &erases {
                let journal = relation.strip_suffix("_search").ok_or_else(invalid)?;
                for &key in keys {
                    let _name = self.context.memory.reserve(2 * key.len() + 4)?;
                    let fence = RootRowCarrier::parse(key).ok_or_else(invalid)?.fence_name();
                    if !row_checks.contains(fence.as_str()) || !row_fences.contains(fence.as_str())
                        || !row_retirements.contains(&(journal, key))
                    { return Err(invalid()); }
                }
            }
            self.search_erasure_admitted = true;
            return Ok(());
        }
        if !row_checks.is_empty() || !row_fences.is_empty() || witness != 1 { return Err(invalid()); }
        let (alias, existing, valid) = unit.ok_or_else(invalid)?;
        if existing {
            if creates.is_some() { return Err(invalid()); }
            let fence = self.root_point(owner, &OwnedRelation::new("fact_unit_fences")?,
                &EntityId::String(alias.to_owned()), valid, invalid).await?;
            if ArrowCell::named(fence.batch(), "unit_phase", 0).and_then(ArrowCell::binary) != Some(b"BFU1\x01") {
                return Err(invalid());
            }
        } else if creates != Some(alias) {
            return Err(invalid());
        }
        let mut prefix = [0; 32];
        for (offset, byte) in prefix.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&alias[2 * offset..2 * offset + 2], 16).map_err(|_| invalid())?;
        }
        let ordered = self.tx_key().system_time;
        let context = self.context.query_context(
            self.buffer.base_snapshot().clone(), ordered, &self.options.default_tz, self.trace_parent,
        )?;
        let mut inventory = rtdb_query::open_owned_root_prefix_projection(
            Arc::clone(&context), owner, &OwnedRelation::new(SEALED_SEARCH_UNIT_ROWS)?, ordered,
            &["_id"], rtdb_columnar::EntityIdRef::Binary(&prefix),
        ).await?.ok_or_else(invalid)?;
        while let Some(held) = inventory.next_batch().await? {
            let batch = held.batch();
            let (index, field) = batch.schema_ref().column_with_name("_id").ok_or_else(invalid)?;
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let rtdb_columnar::EntityIdRef::Binary(key) =
                    rtdb_columnar::read_owned_entity_id(field, batch.column(index).as_ref(), row)?
                else { return Err(invalid()); };
                let (relation, id) = sealed_search_inventory_row(&prefix, key).ok_or_else(invalid)?;
                if !erases.get_mut(relation).is_some_and(|ids| ids.remove(id)) { return Err(invalid()); }
            }
        }
        if erases.values().any(|ids| !ids.is_empty()) { return Err(invalid()); }
        self.search_erasure_admitted = true;
        Ok(())
    }

    pub(super) async fn claim_assertion_family_fences(
        &mut self, owner: &str, operations: &[Operation], replay: bool,
    ) -> Result<()> {
        use rtdb_types::AssertionFamilyFenceMarker;

        let mut controls = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::HostedControl(control)) if control.is_assertion_family_marker() =>
                Some(control),
            _ => None,
        });
        let invalid = invalid_family_fence;
        let Some(control) = controls.next() else {
            if !replay {
                for operation in operations {
                    if family_retirement(operation)?.is_some() { return Err(invalid()); }
                }
            }
            return Ok(());
        };
        let attempt = self.placed_attempt.filter(|attempt| Some(attempt.scope()) == self.context.owned_witness_scope)
            .ok_or_else(invalid)?;
        if replay || controls.next().is_some() { return Err(invalid()); }
        let archive = self.context.world_constraints.as_ref().ok_or_else(invalid)?;
        let mut root_erases = BTreeSet::new();
        let mut root_relations = BTreeSet::new();
        let mut search_markers = BTreeSet::new();
        for operation in operations {
            if let Operation::Public(TxOp::OwnedErase { relation, id }) = operation {
                if !crate::hosted_control::family_root_erase(relation) || !root_erases.insert((relation, id)) {
                    return Err(invalid());
                }
                root_relations.insert(relation.name());
            }
            if let Some(write) = crate::hosted_control::owned_document_write(operation)
                && write.relation.is_root_data() && write.relation.name() == "fact_sealed_search_erasures"
            {
                if !crate::hosted_control::family_search_erase_marker(&write) { return Err(invalid()); }
                for row in write.documents.rows() {
                    if !search_markers.insert(row.string_id().ok_or_else(invalid)?) { return Err(invalid()); }
                }
            }
        }
        if root_relations != search_markers {
            return Err(invalid());
        }
        let EntityId::String(id) = control.id() else { return Err(invalid()); };
        let family = AssertionFamilyFenceMarker::family_id(id).map_err(|_| invalid())?;
        let row = control.replacement().filter(|row| row.len() == 2).ok_or_else(invalid)?;
        let Some(Value::Binary(body)) = row.get("body") else { return Err(invalid()); };
        if control.expected().is_some() {
            return Err(invalid());
        }
        let marker = AssertionFamilyFenceMarker::decode(body).map_err(|_| invalid())?;
        let mut retirement = None;
        for operation in operations {
            if let Some((world, id, digest)) = family_retirement(operation)?
                && (digest != marker.inventory || retirement.replace((world, id)).is_some())
            {
                return Err(invalid());
            }
        }
        let (retirement_world, retirement_id) = retirement.ok_or_else(invalid)?;
        let rows = match retirement_world {
            Some(world) => self.assertion_fence_world_winner(
                owner, operations, world, &retirement_id, marker.winner,
            ).await?,
            None => self.assertion_fence_root_winner(owner, &retirement_id, marker.winner).await?,
        };
        let carrier = ArrowCell::named(rows.batch(), "sealed_payload", 0)
            .and_then(ArrowCell::binary).ok_or_else(invalid)?;
        let (winner, root_carrier) = match (retirement_world, &retirement_id) {
            (Some(world), EntityId::String(key)) => (Some(super::AssertionFenceWorldWinner { world, key, carrier }), None),
            (None, EntityId::String(_)) => (None, Some(carrier)),
            _ => return Err(invalid()),
        };
        archive.claim_assertion_family_fence(super::AssertionFenceClaim {
            database: &self.context.database, attempt, durable_predecessor: self.buffer.base_snapshot().tx_basis(),
            family, marker, winner, root_carrier, root_erases: &root_erases,
        }).await?;
        self.search_erasure_admitted = true;
        Ok(())
    }

    async fn assertion_fence_root_winner(
        &self, owner: &str, id: &EntityId, winner: rtdb_types::Position,
    ) -> Result<OwnedBatch> {
        let invalid = invalid_family_fence;
        let EntityId::String(fence) = id else { return Err(invalid()); };
        let address = fence.strip_prefix("row/").ok_or_else(invalid)?;
        let _key = self.context.memory.reserve(address.len() + 128)?;
        let id = EntityId::Binary(rtdb_types::RootRowCarrier::decode_named_address(address).ok_or_else(invalid)?);
        let relation = OwnedRelation::new(rtdb_types::ROOT_ROW_CARRIERS)?;
        let at = self.tx_key().system_time;
        let rows = self.root_point(owner, &relation, &id, at, invalid).await?;
        let revision = rtdb_query::owned_row_revision(rows.batch(), 0, &id, &self.context.memory)?;
        if u64::try_from(revision.0).ok() != Some(winner.publication) {
            return Err(invalid());
        }
        Ok(rows)
    }

    pub(super) async fn root_point(
        &self, owner: &str, relation: &OwnedRelation, id: &EntityId, at: Timestamp,
        invalid: fn() -> Anomaly,
    ) -> Result<OwnedBatch> {
        let context = self.context.query_context(
            self.buffer.base_snapshot().clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        let mut cursor = rtdb_query::open_owned_assertion_point(
            context, owner, relation, id, Some(Interval::at(at)?), false,
        ).await?.ok_or_else(invalid)?;
        let mut selected = None;
        while let Some(rows) = cursor.next_batch().await? {
            match rows.batch().num_rows() {
                0 => continue,
                1 if selected.is_none() => selected = Some(rows),
                _ => return Err(invalid()),
            }
        }
        selected.ok_or_else(invalid)
    }

    async fn assertion_fence_world_winner(
        &self, owner: &str, operations: &[Operation], world: Iid, id: &EntityId, winner: rtdb_types::Position,
    ) -> Result<OwnedBatch> {
        let invalid = invalid_family_fence;
        let (record, _) = self.owned_worlds.get(&world).ok_or_else(invalid)?;
        let relation = OwnedRelation::new("fact_unit_column_payloads")?.in_world(world)?;
        if winner.line_generation != world.0 || record.dropped || !record.mode.is_writable()
            || !operations.iter().any(|operation| matches!(operation,
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation: checked, pin, .. })
                    if checked == &relation && *pin == record.pin))
            || !operations.iter().any(|operation| matches!(operation,
                Operation::Public(TxOp::OwnedPublishWorld { world: published, drop_world: false, .. })
                    if *published == world))
        {
            return Err(invalid());
        }
        let mut witnesses = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: selected, valid_at, expected: Some(expected) })
                if checked == &relation && selected == id => Some((*valid_at, *expected)),
            _ => None,
        });
        let (valid_at, expected) = witnesses.next().ok_or_else(invalid)?;
        if witnesses.next().is_some() {
            return Err(invalid());
        }
        let context = self.context.query_context(
            self.buffer.base_snapshot().clone(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        let mut cursor = rtdb_query::open_owned_assertion_point(
            context, owner, &relation, id, Some(Interval::at(valid_at)?), false,
        ).await?.ok_or_else(invalid)?;
        let mut winner_row = None;
        while let Some(rows) = cursor.next_batch().await? {
            match rows.batch().num_rows() {
                0 => continue,
                1 if winner_row.is_none() => winner_row = Some(rows),
                _ => return Err(invalid()),
            }
        }
        let rows = winner_row.ok_or_else(invalid)?;
        let row = rows.batch();
        let history = rtdb_query::owned_row_revision(row, 0, id, &self.context.memory)?;
        let position = rtdb_query::owned_row_event_position(row, 0)?;
        let ordinal = rtdb_query::owned_row_assertion_ordinal(row, 0)?.ok_or_else(invalid)?;
        if u64::try_from(history.0).ok() != Some(winner.publication) || u64::from(ordinal.get()) != winner.ordinal.get()
            || expected.history != history || expected.position != position
        {
            return Err(invalid());
        }
        Ok(rows)
    }

    pub(super) async fn check_hosted_controls(
        &mut self,
        operations: &[Operation],
        source_scope: Option<rtdb_types::PlacedAttemptScope>,
        replay: Option<crate::hosted_control::HostedReplay>,
    ) -> Result<()> {
        let controls = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::HostedControl(control)) => Some(control),
            _ => None,
        });
        let family_fence = controls.clone().any(HostedControlWrite::is_assertion_family_marker);
        if controls.clone().next().is_none() {
            if operations.iter().any(|operation| matches!(operation,
                Operation::Public(TxOp::OwnedForkWorld { .. } | TxOp::OwnedPublishRoot { .. } | TxOp::OwnedPublishWorld { .. })
            )) {
                return Err(crate::hosted_control::authority_required());
            }
            return if self.clock_slot.is_some_and(|slot| slot.funded) {
                Err(funded_booking_missing())
            } else {
                Ok(())
            };
        }
        let funded = self.clock_slot.filter(|slot| slot.funded).map(|slot| {
            let mut terminals = controls.clone().filter(|control| {
                control.name() == rtdb_types::HOSTED_ROOT_RECEIPTS_TABLE && !control.is_check()
            });
            let terminal = terminals.next().filter(|control| control.expected().is_none())
                .and_then(|control| control.replacement()).ok_or_else(funded_booking_missing)?;
            let Some(Value::Binary(evidence)) = terminal.get("funded_clock") else {
                return Err(funded_booking_missing());
            };
            let (reading, booking) = evidence.split_first_chunk::<8>().ok_or_else(funded_booking_missing)?;
            let booking: [u8; 32] = booking.try_into().map_err(|_| funded_booking_missing())?;
            if terminals.next().is_some() || booking == [0; 32] {
                return Err(funded_booking_missing());
            }
            Ok((slot.reading, Timestamp(i64::from_be_bytes(*reading)), booking))
        }).transpose()?;
        let mut funded_seat_seen = false;
        let readmission = crate::hosted_control::world_readmission_control(controls.clone())?;
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.hosted/control-scope",
                "Hosted controls require an admitted placed commit",
            )
        };
        let Some(scope) =
            source_scope.filter(|scope| Some(*scope) == self.context.owned_witness_scope)
        else {
            return Err(unavailable());
        };
        let permits = |operation: &Operation| {
            matches!(
                operation,
                Operation::Public(
                    TxOp::OwnedDeadline { .. }
                        | TxOp::OwnedCheck { .. }
                        | TxOp::OwnedForkWorld { .. }
                        | TxOp::OwnedCheckWorldScope { clock: None, .. }
                        | TxOp::OwnedCheckRange { .. }
                        | TxOp::OwnedCheckCapacity { .. }
                        | TxOp::OwnedDeclareCapacity { .. }
                        | TxOp::OwnedRequireSpatialPath { .. }
                        | TxOp::OwnedRetireRelation { .. } | TxOp::OwnedAdoptRelation { .. }
                        | TxOp::OwnedInsertDocs { .. }
                        | TxOp::OwnedReplaceDocs { .. }
                        | TxOp::OwnedArrowDocs { .. }
                        | TxOp::OwnedDelete { .. }
                        | TxOp::HostedControl(_)
                ) | Operation::OwnedWriteIids { .. } | Operation::OwnedArrowIids { .. }
            ) || ((family_fence || replay.is_some()) && matches!(operation, Operation::Public(TxOp::OwnedErase { .. })))
                || matches!(operation, Operation::Public(TxOp::OwnedErase { relation, .. })
                    if relation.name() == SHARED_COLUMN_CARRIERS || crate::hosted_control::family_root_erase(relation))
                || (replay.is_some() && matches!(operation, Operation::Public(TxOp::OwnedReplayResidue { .. })))
        };
        if !operations
            .iter()
            .any(|operation| matches!(operation, Operation::Public(TxOp::OwnedDeadline { .. })))
        {
            return Err(unavailable());
        }
        let validating_world = !self.owned_worlds.is_empty();
        let hosted_world_reads = validating_world && crate::hosted_control::hosted_root_world_reads(operations);
        if operations.iter().any(|operation| matches!(operation, Operation::Public(TxOp::OwnedDeclareCapacity { .. })))
            && (validating_world || !controls.clone().any(|control| {
                control.name() == "bj_installation" && control.is_check() && control.expected().is_some()
                    && matches!(control.id(), EntityId::String(id) if id == "authority")
            }))
        {
            return Err(unavailable());
        }
        if readmission.is_some() && (replay.is_some() || validating_world || operations.iter().any(|operation| {
            !matches!(operation, Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::HostedControl(_)))
        })) {
            return Err(unavailable());
        }
        if let Some(replay) = &replay {
            replay.check_order(self.tx_key().system_time)?;
            if validating_world || operations.iter().any(|operation|
                operation.direct_archive_relation().is_none() && !matches!(operation,
                    Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheck { .. } | TxOp::HostedControl(_))
                )
            ) {
                return Err(unavailable());
            }
            let direct = operations.iter().filter(|operation| operation.direct_archive_relation().is_some());
            let rank_count = direct.clone().filter(|operation| operation.direct_archive_relation()
                .is_some_and(|relation| crate::hosted_control::replay_rank::is_rank_relation(relation.name()))).count();
            if replay.rank().map_or(0, |rank| rank.count()) != rank_count
                || direct.clone().count() != replay.direct_effect_count()
                || direct.filter_map(Operation::owned_documents).any(|(_, rows)| rows != 1)
            {
                return Err(unavailable());
            }
        }
        let publishing_world = operations.iter().any(|op| matches!(op, Operation::Public(TxOp::OwnedPublishWorld { .. })));
        let retained_release = crate::hosted_control::retained_release_world(operations).is_some();
        let proposal_overlay = crate::hosted_control::hosted_proposal_overlay(operations).is_some();
        let proposal_land = crate::hosted_control::hosted_proposal_land(operations).is_some();
        if validating_world && (
            (!retained_release && !proposal_overlay && !proposal_land && !hosted_world_reads
                && controls.clone().any(|control| !control.is_check() && !control.is_assertion_family_marker()))
            || !operations.iter().any(|operation| matches!(operation, Operation::Public(TxOp::OwnedCheckWorldScope { .. })))
        ) {
            return Err(unavailable());
        }
        let mut has_native_effect = false;
        for operation in operations {
            if !hosted_world_reads {
                if validating_world {
                    if !matches!(operation, Operation::Public(
                        TxOp::OwnedDeadline { .. } | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckWorldScope { .. }
                            | TxOp::OwnedCheckWorldPurge { .. } | TxOp::OwnedCheckRange { .. }
                            | TxOp::OwnedCheckCapacity { .. } | TxOp::HostedControl(_)
                    )) && !(publishing_world && matches!(operation,
                        Operation::Public(TxOp::OwnedPublishWorld { .. } | TxOp::OwnedPublishRoot { .. }
                            | TxOp::OwnedInsertDocs { .. } | TxOp::OwnedReplaceDocs { .. } | TxOp::OwnedArrowDocs { .. } | TxOp::OwnedDelete { .. })
                            | Operation::OwnedWriteIids { .. } | Operation::OwnedArrowIids { .. }
                    )) && !(family_fence && publishing_world && matches!(operation,
                        Operation::Public(TxOp::OwnedErase { .. })
                    )) {
                        return Err(unavailable());
                    }
                } else if !permits(operation) || operation.owned_relation().is_some_and(|relation|
                    relation.world().is_some() || relation.is_world_catalog()
                ) {
                    return Err(unavailable());
                }
            }
            has_native_effect |= matches!(operation, Operation::Public(TxOp::OwnedDelete { .. } | TxOp::OwnedErase { .. } | TxOp::OwnedReplayResidue { .. } | TxOp::OwnedDeclareCapacity { .. } | TxOp::OwnedRetireRelation { .. } | TxOp::OwnedAdoptRelation { .. }))
                || operation.owned_documents().is_some_and(|(_, rows)| rows > 0);
        }
        let fork_with_controls = operations.iter().any(|operation| matches!(
            operation, Operation::Public(TxOp::OwnedForkWorld { root: crate::OwnedForkRoot::Controls | crate::OwnedForkRoot::ProposalControls, .. })
        ));
        // An exact native replay transaction can consist entirely of source
        // effects whose Original key was withdrawn. Its clock still advances.
        if !has_native_effect && replay.is_none() && readmission.is_none()
            && !fork_with_controls && !retained_release && !proposal_overlay
            && controls.clone().any(|control| !control.is_check())
        {
            return Err(unavailable());
        }

        let base = self.buffer.base_snapshot();
        let context = self.context.query_context(
            base.clone(),
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let (_, authority) = crate::hosted_control::installed_authority_control(controls.clone())?;
        if authority.scope() != scope { return Err(unavailable()); }
        let stale = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.hosted/stale-authority",
            "The installation authority changed before ordering");
        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(&context), authority.table(), authority.id(), true,
        ).await?;
        let (row, revision) = point.row.ok_or_else(stale)?;
        let authority_current = if let Some(retired) = crate::hosted_control::RetiredFamilyFence::from_control(authority)? {
            if !family_fence || validating_world || replay.is_some() || funded.is_some()
                || authority.expected() != Some(revision)
            {
                return Err(unavailable());
            }
            let mut retirement = None;
            for operation in operations {
                if let Some((world, EntityId::String(id), _)) = family_retirement(operation)? {
                    if world.is_some() || retirement.replace(id).is_some() { return Err(unavailable()); }
                }
            }
            retired.require_batch(operations, &retirement.ok_or_else(unavailable)?)?;
            retired.current(&context, &row).await?
        } else {
            let proof = authority.installed_authority()?;
            if !proof.matches_revision(Some(revision)) { return Err(stale()); }
            let row = row.batch();
            let field = |name| {
                let (index, field) = row.schema_ref().column_with_name(name).ok_or_else(stale)?;
                rtdb_columnar::read_value(field, row.column(index).as_ref(), 0)
            };
            let (Value::Utf8(kind), Value::Binary(incarnation), Value::Utf8(writer)) =
                (field("kind")?, field("incarnation")?, field("writer")?)
            else { return Err(stale()); };
            proof.matches(&kind, &incarnation, &writer)
        };
        if !authority_current {
            return Err(stale());
        }

        let mut locators = BTreeSet::new();
        let mut selected_tables = BTreeSet::new();
        let mut charges = Vec::new();
        // Each lookup uses the context captured from this one ordered base.
        let mut stamps = Vec::new();
        let mut rows = 0usize;
        for control in controls.clone() {
            self.context.check_cancelled()?;
            let table = control.table();
            rows += 1;
            selected_tables.insert(control.name());
            if rows > 4096 || selected_tables.len() > 32 {
                return Err(unavailable());
            }
            if control.scope() != scope || !base.contains_table(&table) {
                return Err(unavailable());
            }
            let shape = || Anomaly::incorrect(
                "rtdb.hosted/control-shape",
                "Hosted control fields must match the installed ordinary column types",
            );
            let accepts = |kind: Option<&LogicalType>, observed: &LogicalType| {
                kind.is_some_and(|kind| !matches!(kind, LogicalType::Nothing | LogicalType::Null)
                    && *observed != LogicalType::Nothing && kind.join(observed) == *kind)
            };
            if let Some(documents) = control.arrow_replacement() {
                let array = documents.array();
                let existing = base.column_types_for(&table, documents.columns().iter().cloned());
                for (field, array) in array.fields().iter().zip(array.columns()) {
                    let cell = ArrowCell { field, array: array.as_ref(), row: 0 };
                    if !accepts(existing.get(field.name()), &cell.observed_type()?) { return Err(shape()); }
                }
            } else if let Some(document) = control.replacement() {
                let fields = document.keys().cloned();
                let existing = base.column_types_for(&table, fields);
                if document.iter().any(|(name, value)| !accepts(existing.get(name), &value.logical_type())) {
                    return Err(shape());
                }
            }
            charges.push(self.context.memory.reserve(table.table.len() + 512)?);
            if !locators.insert((table.clone(), control.id().iid())) {
                return Err(Anomaly::incorrect(
                    "rtdb.hosted/control-repeat",
                    "A hosted control row must appear once per commit",
                ));
            }
            if std::ptr::eq(control, authority) { continue; }
            let booked_seat = funded.filter(|_| control.name() == "bj_fact_root_admission"
                && matches!(control.id(), EntityId::String(id) if id == "funded_mutation_seat:v1"));
            let point = rtdb_query::read_hosted_control_point(
                Arc::clone(&context),
                table,
                control.id(),
                booked_seat.is_some() || replay.as_ref().is_some_and(|replay| replay.requires_base_payload(control)),
            )
            .await?;
            if let Some(replay) = &replay {
                replay.check_base(control, point.row.as_ref().map(|(row, _)| row.batch()))?;
            }
            if let Some((offered, terminal, booking)) = booked_seat {
                let (row, _) = point.row.as_ref().ok_or_else(funded_booking_missing)?;
                let row = row.batch();
                let (column, field) = row.schema_ref().column_with_name("admission").ok_or_else(funded_booking_missing)?;
                let Value::Binary(body) = rtdb_columnar::read_value(field, row.column(column).as_ref(), 0)? else {
                    return Err(funded_booking_missing());
                };
                let seat: FundedSeat = serde_json::from_slice(&body).map_err(|_| funded_booking_missing())?;
                let end = i64::try_from(seat.until).map_err(|_| funded_booking_missing())?;
                if end % 1_000 != 0 || !crate::system_time::original_funded_seat(
                    offered.0, terminal.0, end / 1_000,
                    control.is_check() && control.expected().is_some(), booking == seat.offer,
                ) {
                    return Err(funded_booking_missing());
                }
                funded_seat_seen = true;
            }
            stamps.push(HostedStamp {
                observed: point.row.map(|(_, revision)| revision.0),
                expected: control.expected().map(|time| time.0),
            });
        }
        match check_controls(authority_current, &stamps) {
            ControlDecision::Current => {}
            ControlDecision::StaleAuthority => return Err(stale()),
            ControlDecision::StaleControl => return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.hosted/stale-control",
                "An observed hosted control changed before ordering",
            )),
        }
        if funded.is_some() && !funded_seat_seen {
            return Err(funded_booking_missing());
        }
        if let Some(replay) = &replay {
            self.restore_owned_lifetime_floors(replay, &context).await?;
            self.restore_owned_worlds(replay, &context).await?;
            self.buffer.put(
                TableRef::new("public", crate::hosted_control::RESTORE),
                StructFields::from([
                    ("_id".into(), Value::Utf8(crate::hosted_control::HostedReplay::destination_locator(self.tx_key().tx_id))),
                    ("binding".into(), Value::Binary(replay.original_receipt_key().to_vec())),
                ]),
                Interval::new(Timestamp(0), Timestamp::MAX)?,
            )?;
            for (relation, rule) in replay.capacity_declarations() {
                let relation = OwnedRelation::new(relation)?;
                self.declare_owned_capacity(&relation, &rule.column, 0, rule.cap, &rule.when).await?;
            }
        }
        if let Some(readmission) = readmission {
            self.readmit_replayed_world(&context, readmission, controls).await?;
        }
        self.hosted_replay = replay;
        Ok(())
    }

    async fn restore_owned_lifetime_floors(
        &mut self,
        replay: &crate::hosted_control::HostedReplay,
        context: &Arc<rtdb_query::QueryContext>,
    ) -> Result<()> {
        use rtdb_types::{OwnedLifetimeFloor, OwnedRelation};
        let owner = self.options.user.as_deref().filter(|owner| !owner.is_empty()).ok_or_else(no_owned_principal)?;
        let ordered = self.tx_key().system_time;
        for original in replay.lifetimes() {
            let relation = OwnedRelation::new(original.relation.as_str())?;
            let duration = rtdb_query::read_owned_relation_schema(Arc::clone(context), owner, &relation)
                .await?.and_then(|(schema, _)| schema.lifetime_micros()).ok_or_else(lifetime_uncovered)?;
            let archived = original.floor();
            archived.check(duration, replay.source_time())?;
            let before = rtdb_query::read_owned_lifetime_floor(Arc::clone(context), owner, &relation).await?;
            before.check(duration, ordered)?;
            if archived.swept_present_through < before.swept_present_through {
                return Err(lifetime_uncovered());
            }
            let next = OwnedLifetimeFloor {
                duration,
                expired_created_through: before.expired_created_through.max(archived.expired_created_through),
                swept_present_through: archived.swept_present_through,
            };
            if next != before {
                self.put_owned_lifetime_floor(owner, &relation, next)?;
            }
        }
        Ok(())
    }
}
