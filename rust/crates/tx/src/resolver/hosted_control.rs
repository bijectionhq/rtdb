use std::collections::BTreeSet;
use rtdb_types::{EntityId, OwnedRelation};

use super::*;
use super::control_order::{HostedStamp, hosted_stamps_current};

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

fn family_retirement(operation: &Operation, digest: Option<&[u8; 32]>) -> Result<Option<Option<Iid>>> {
    let relation = match operation.owned_relation() {
        Some(relation) if relation.root_relation().as_ref().unwrap_or(relation).name() == "fact_unit_fences" => relation,
        _ => return Ok(None),
    };
    let inventory_matches = |inventory: &[u8]| inventory.len() == 32
        && digest.is_none_or(|digest| inventory == digest.as_slice());
    let values_match = |rows: &crate::Documents| rows.rows().iter().filter(|row| {
        matches!(row.get("unit_phase"), Some(Value::Binary(phase)) if phase == b"BFU1\x01")
            && matches!(row.get("assertion_vault_inventory"), Some(Value::Binary(inventory)) if inventory_matches(inventory))
    }).count();
    let arrow_matches = |rows: &crate::ArrowDocuments| {
        let mut matches = 0;
        for row in 0..rows.row_count() {
            let cell = |name: &str| {
                if rows.legacy_owned_presence().is_some() {
                    return rows.legacy_owned_cell(name, row);
                }
                let array = rows.array();
                Ok(array.fields().iter().zip(array.columns())
                    .find(|(field, _)| field.name() == name)
                    .map(|(field, array)| rtdb_columnar::ArrowCell { field, array: array.as_ref(), row }))
            };
            if cell("unit_phase")?.and_then(|cell| cell.binary()) == Some(b"BFU1\x01".as_slice())
                && cell("assertion_vault_inventory")?.and_then(|cell| cell.binary()).is_some_and(inventory_matches)
            {
                matches += 1;
            }
        }
        Ok::<_, Anomaly>(matches)
    };
    let count = match operation {
        Operation::Public(TxOp::OwnedInsertDocs { documents, .. } | TxOp::OwnedReplaceDocs { documents, .. })
        | Operation::OwnedWriteIids { documents, .. } => values_match(documents),
        Operation::Public(TxOp::OwnedArrowDocs { documents, .. })
        | Operation::OwnedArrowIids { documents, .. } => arrow_matches(documents)?,
        _ => 0,
    };
    match count {
        0 => Ok(None),
        1 => Ok(Some(relation.world())),
        _ => Err(Anomaly::incorrect("rtdb.owned/assertion-family-fence-marker", "A family fence requires one digest-bound retirement")),
    }
}

impl OpenTransaction<'_> {
    pub(super) async fn claim_assertion_family_fences(
        &self, operations: &[Operation], replay: bool,
    ) -> Result<()> {
        use rtdb_types::AssertionFamilyFenceMarker;

        let mut controls = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::HostedControl(control)) if control.is_assertion_family_marker() =>
                Some(control),
            _ => None,
        });
        let invalid = || Anomaly::incorrect("rtdb.owned/assertion-family-fence-marker", "Invalid ordered family fence marker");
        let Some(control) = controls.next() else {
            if !replay {
                for operation in operations {
                    if family_retirement(operation, None)?.is_some() { return Err(invalid()); }
                }
            }
            return Ok(());
        };
        let attempt = self.placed_attempt.filter(|attempt| Some(attempt.scope()) == self.context.owned_witness_scope)
            .ok_or_else(invalid)?;
        if replay || controls.next().is_some() { return Err(invalid()); }
        let archive = self.context.world_constraints.as_ref().ok_or_else(invalid)?;
        let mut root_erases = BTreeSet::new();
        for operation in operations {
            if let Operation::Public(TxOp::OwnedErase { relation, id }) = operation
                && (!relation.is_root_data() || !relation.name().ends_with("_search") || !root_erases.insert((relation, id)))
            {
                return Err(invalid());
            }
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
            if let Some(line) = family_retirement(operation, Some(&marker.inventory))? {
                if retirement.replace(line).is_some() { return Err(invalid()); }
            }
        }
        let retirement_world = retirement.ok_or_else(invalid)?;
        archive.claim_assertion_family_fence(super::AssertionFenceClaim {
            database: &self.context.database, attempt, durable_predecessor: self.buffer.base_snapshot().tx_basis(),
            family, marker, retirement_world, root_erases: &root_erases,
        }).await
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
                        | TxOp::OwnedRetireRelation { .. }
                        | TxOp::OwnedInsertDocs { .. }
                        | TxOp::OwnedReplaceDocs { .. }
                        | TxOp::OwnedArrowDocs { .. }
                        | TxOp::OwnedDelete { .. }
                        | TxOp::HostedControl(_)
                ) | Operation::OwnedWriteIids { .. } | Operation::OwnedArrowIids { .. }
            ) || ((family_fence || replay.is_some()) && matches!(operation, Operation::Public(TxOp::OwnedErase { .. })))
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
            if direct.clone().count() != replay.direct_effect_count()
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
            has_native_effect |= matches!(operation, Operation::Public(TxOp::OwnedDelete { .. } | TxOp::OwnedErase { .. } | TxOp::OwnedReplayResidue { .. } | TxOp::OwnedDeclareCapacity { .. } | TxOp::OwnedRetireRelation { .. }))
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
        let proof = authority.installed_authority()?;
        let stale = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.hosted/stale-authority",
            "The installation authority changed before ordering");
        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(&context), authority.table(), authority.id(), true,
        ).await?;
        let (row, revision) = point.row.ok_or_else(stale)?;
        if !proof.matches_revision(Some(revision)) { return Err(stale()); }
        let row = row.batch();
        let field = |name| {
            let (index, field) = row.schema_ref().column_with_name(name).ok_or_else(stale)?;
            rtdb_columnar::read_value(field, row.column(index).as_ref(), 0)
        };
        let (Value::Utf8(kind), Value::Binary(incarnation), Value::Utf8(writer)) =
            (field("kind")?, field("incarnation")?, field("writer")?)
        else { return Err(stale()); };
        if !proof.matches(&kind, &incarnation, &writer) {
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
        if !hosted_stamps_current(&stamps) {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.hosted/stale-control",
                "An observed hosted control changed before ordering",
            ));
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
