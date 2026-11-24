//! Install Original world facts atomically with the held replay frontier.

use std::{
    collections::BTreeMap,
    sync::Arc,
};

use arrow_array::{Array, BinaryArray};
use rtdb_format::data::{StoredEvent, StoredOperation};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedChangeKind, OwnedRelation, OwnedWorld, Result, StructFields,
    TableRef, Timestamp, TransactionKey, Value, WorldClockMode, WorldShadow,
};

use super::{OpenTransaction, no_owned_principal, overflow, validate_owned_schema};
use crate::hosted_control::{
    HostedControlWrite, HostedReplay, ReplayWorlds, ReplayedWorldParent, WorldReadmission,
    replayed_archive_cut, replayed_parent_logical_time, replayed_world_parent,
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ReplayedRegistration {
    binding: [u8; 32],
    pin: [u8; 16],
    source: TransactionKey,
    logical: Timestamp,
}

impl ReplayedRegistration {
    fn precedes(
        &self, binding: [u8; 32], current: TransactionKey, link: &rtdb_types::OwnedWorldForkLink,
    ) -> bool {
        self.binding == binding && self.pin == link.pin && self.logical == link.registered
            && self.source.tx_id < current.tx_id && self.source.system_time < current.system_time
    }
}

/// The registration receipt identifies a writer. A separate clock proof
/// identifies the exact publication selected from that writer.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ReplayedHeldPublication {
    binding: [u8; 32],
    pin: [u8; 16],
    root: TransactionKey,
    source: TransactionKey,
    reading: Timestamp,
}

impl ReplayedHeldPublication {
    fn binds(&self, binding: [u8; 32], ancestor: &rtdb_types::OwnedWorldAncestor) -> bool {
        self.binding == binding && self.pin == ancestor.pin
            && self.root == ancestor.source_root && self.reading == ancestor.logical
            && self.source == ancestor.publication && self.source.at_or_before(ancestor.basis)
    }
}

fn held_publication_locator(world: Iid, source: rtdb_types::TxId) -> String {
    format!("native-world-publication/{world}/{}", source.0)
}

fn registration_locator(world: Iid) -> String {
    format!("native-world-registration/{world}")
}

impl OpenTransaction<'_> {
    pub(super) async fn readmit_replayed_world<'a>(
        &mut self,
        context: &Arc<rtdb_query::QueryContext>,
        admission: WorldReadmission,
        controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
    ) -> Result<()> {
        let unavailable = OwnedWorld::unavailable;
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let id = Iid(admission.world);
        let (mut world, _reservation) =
            rtdb_query::read_owned_world(Arc::clone(context), owner, id)
                .await?
                .ok_or_else(unavailable)?;
        let (active, _) = replay_control(context, "restore").await?;
        let (archive, _) = replay_control(context, "archive").await?;
        let (frontier, _) = replay_control(context, "native-frontier.v1").await?;
        let source_cut = replayed_archive_cut(&frontier, admission.binding, &archive)?;
        if active != admission.binding
            || source_cut != admission.source_cut
            || world.dropped
            || world.pin.0 != admission.pin
            || world.until <= source_cut
            || world.basis != admission.original_parent
            || world.until != admission.original_until
            || world.basis.system_time > source_cut
        {
            return Err(unavailable());
        }
        let (proof, _) =
            replay_control(context, &ReplayedWorldParent::locator(id, world.pin)).await?;
        let proven = replayed_world_parent(&proof, admission.binding, &world)?;
        let locator = format!("native-transaction/{}", world.basis.tx_id.0);
        let (parent, restored) = replay_control(context, &locator).await?;
        let logical =
            replayed_parent_logical_time(&parent, admission.binding, &archive, world.basis)?;
        let ordered = self.tx_key();
        let remaining = world
            .until
            .0
            .checked_sub(source_cut.0)
            .ok_or_else(unavailable)?;
        let lease_start = Timestamp(
            admission
                .until
                .0
                .checked_sub(remaining)
                .ok_or_else(unavailable)?,
        );
        let root_clock = world.root_clock();
        if admission.basis != proven
            || restored != admission.basis
            || admission.basis.tx_id >= ordered.tx_id
            || root_clock > world.basis.system_time
            || (world.mode == WorldClockMode::Proposal && root_clock != logical)
            || lease_start < proven.system_time
            || lease_start > ordered.system_time
            || admission.until <= ordered.system_time
        {
            return Err(unavailable());
        }
        admission.require_retained_ancestors(&world)?;
        for layer in &admission.ancestors {
            let ancestor = &layer.retained;
            let original = layer.original;
            let locator = format!("native-transaction/{}", original.tx_id.0);
            let (receipt, destination) = replay_control(context, &locator).await?;
            replayed_parent_logical_time(&receipt, admission.binding, &archive, original)?;
            if ancestor.basis != destination
                || ancestor.basis.tx_id >= ordered.tx_id
                || destination.system_time > lease_start
                || ancestor.publication.tx_id <= admission.basis.tx_id
                || !ancestor.publication.at_or_before(destination)
            {
                return Err(unavailable());
            }
            let selected = layer.selected;
            let locator = format!("native-transaction/{}", selected.tx_id.0);
            let (receipt, selected_destination) = replay_control(context, &locator).await?;
            replayed_parent_logical_time(&receipt, admission.binding, &archive, selected)?;
            let (source, clock, _charge) = rtdb_query::read_held_world_publication(
                context, owner, Iid(ancestor.world), ancestor.publication,
            ).await?;
            if selected_destination != ancestor.publication || clock != ancestor.logical
                || source.pin.0 != ancestor.pin || source.basis != ancestor.source_root
            {
                return Err(unavailable());
            }
        }
        admission.require_application(&world, controls)?;
        world.ancestors = admission.ancestors.into_iter().map(|layer| layer.retained).collect();
        world.capture = Iid(admission.capture);
        world.suffix = admission.suffix.map(Iid);
        world.manifest = admission.manifest;
        world.basis = admission.basis;
        world.parent_publication = Some(root_clock);
        world.until = admission.until;
        let record = world.record();
        OwnedWorld::from_record(&record)?;
        self.buffer.put(
            OwnedRelation::world_catalog().table(owner),
            record,
            Interval::ALL,
        )
    }

    pub(super) async fn restore_owned_worlds(
        &mut self,
        replay: &HostedReplay,
        context: &Arc<rtdb_query::QueryContext>,
    ) -> Result<()> {
        let body = replay.world_body();
        if body.is_empty() {
            return Ok(());
        }
        let _charge = self.context.memory.reserve(
            body.len()
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(overflow)?,
        )?;
        let effects = ReplayWorlds::decode(body)?;
        let options = self.options;
        let owner = options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let base = self.buffer.base_snapshot();
        let at = replay.source_time();
        let logical = replay.logical_time();

        for registration in effects.registrations {
            let world = registration.world;
            let previous =
                rtdb_query::read_owned_world(Arc::clone(context), owner, world.id).await?;
            if world.basis.system_time > at || world.until <= logical
                || world.ancestors.iter().any(|ancestor| ancestor.basis.system_time >= at)
            {
                return Err(OwnedWorld::unavailable());
            }
            if let Some(readmission) = registration.readmission.as_ref() {
                let (previous, _charge) = previous.ok_or_else(OwnedWorld::unavailable)?;
                readmission.require_relocation(&previous, &world, logical)?;
                self.record_replayed_world_relocation(replay, world.id, readmission)?;
            } else {
                match previous {
                    None if world.pin == world.id && !world.dropped => {
                        require_replayed_fork_parent(context, owner, &world, replay).await?;
                    }
                    Some((previous, _))
                        if !previous.dropped && previous.until > logical
                            && ((world.dropped && previous.same_fork(&world))
                                || previous.can_advance_to(&world)
                                || previous.can_renew_to(&world)
                                || previous.can_hold_proposal(&world)
                                || previous.can_shorten_proposal(&world)) =>
                    {
                        ()
                    }
                    _ => return Err(OwnedWorld::unavailable()),
                }
            }
            if registration.bind_parent {
                if world.dropped { return Err(OwnedWorld::unavailable()); }
                self.record_replayed_world_parent(replay, context, &world)
                    .await?;
            }
            self.record_replayed_registration(replay, &world)?;
            self.stage_replayed_topology(
                replay, OwnedRelation::world_catalog().table(owner), world.id,
                registration.position, world.record(),
            )?;
        }

        let mut active = BTreeMap::new();
        if !effects.clocks.is_empty() {
            let snapshot = self.snapshot()?;
            let updated = self
                .context
                .query_context(
                    snapshot,
                    self.tx_key().system_time,
                    &self.options.default_tz,
                    self.trace_parent,
                )?
                .with_accelerators(Arc::from([]));
            for publication in effects.clocks {
                let (id, clock) = (publication.world, publication.reading);
                let (world, charge) = rtdb_query::read_owned_world(Arc::clone(&updated), owner, id)
                    .await?
                    .ok_or_else(OwnedWorld::unavailable)?;
                let previous =
                    rtdb_query::read_owned_world_clock(Arc::clone(context), owner, id).await?;
                let clock_admitted = match world.mode {
                    WorldClockMode::Proposal => clock == logical,
                    WorldClockMode::Held { .. } => world.admits_clock_publication(clock, self.tx_key().system_time),
                    WorldClockMode::Retained => false,
                };
                if world.dropped
                    || logical >= world.until
                    || !clock_admitted
                    || clock <= previous.unwrap_or_else(|| world.initial_clock())
                {
                    return Err(OwnedWorld::unavailable());
                }
                self.stage_replayed_topology(
                    replay, OwnedRelation::world_clock_table(owner), id, publication.position,
                    StructFields::from([
                        ("_id".into(), Value::Uuid(id.0)),
                        ("clock".into(), Value::I64(clock.0)),
                    ]),
                )?;
                if matches!(world.mode, WorldClockMode::Held { .. }) {
                    self.record_replayed_held_publication(replay, &world, clock)?;
                }
                active.insert(id, (world, charge, clock));
            }
        }

        if !effects.omitted_shadows.is_empty() {
            let mut omissions = Vec::with_capacity(effects.omitted_shadows.len() * 20);
            for (position, world) in effects.omitted_shadows {
                replay.require_assertion_line(position.get(), world.0)?;
                self.buffer.omit_archived_world_effect(position)?;
                omissions.extend_from_slice(&position.get().to_be_bytes());
                omissions.extend_from_slice(&world.0);
            }
            self.buffer.put(
                TableRef::new("public", crate::hosted_control::RESTORE),
                StructFields::from([
                    ("_id".into(), Value::Utf8(replay.world_omissions_locator())),
                    ("binding".into(), Value::Binary(omissions)),
                ]),
                Interval::new(Timestamp(0), Timestamp::MAX)?,
            )?;
        }
        let mut identities = BTreeMap::new();
        for shadow in effects.shadows {
            let (world, _, clock) = active
                .get(&shadow.world)
                .ok_or_else(OwnedWorld::unavailable)?;
            let root = shadow.relation;
            let relation = root.in_world(Iid(shadow.world.0))?;
            let schema = world
                .schemas
                .relation(&root)
                .ok_or_else(OwnedWorld::unavailable)?;
            if schema.lifetime_micros().is_some()
                || base.contains_table(&root.unique_table(owner))
                || base.contains_table(&root.capacity_table(owner))
            {
                return Err(OwnedWorld::unavailable());
            }
            let identity = (relation.clone(), shadow.id.clone(), shadow.valid.lower(), shadow.valid.upper());
            let positioned = shadow.position.is_some();
            if identities.insert(identity, positioned).is_some_and(|prior| !prior || !positioned) {
                return Err(OwnedWorld::unavailable());
            }
            schema.require_replayed_period(shadow.valid, *clock, rtdb_types::EventOp::Put)?;
            if matches!(world.mode, WorldClockMode::Held { .. })
                && schema.has_acceptance_validity()
                && shadow.valid.lower() != *clock
            {
                return Err(OwnedWorld::unavailable());
            }
            let (image, change) = if let Some(mut fields) = shadow.fields {
                fields.insert("_id".into(), shadow.id.as_value());
                validate_owned_schema(schema, &fields)?;
                (WorldShadow::Present(fields), OwnedChangeKind::Put)
            } else {
                (WorldShadow::Deleted, OwnedChangeKind::Delete)
            };
            let table = relation.table(owner);
            let event = StoredEvent { position: None,
                origin: None,
                commit_position: 0,
                entity_key: Some(shadow.id.handle_bytes().into()),
                iid: shadow.id.iid(),
                system_from: self.tx_key().system_time,
                valid_time: shadow.valid,
                operation: StoredOperation::Put(image.record(&shadow.id)?),
                recency: None,
            };
            if let Some(position) = shadow.position {
                replay.require_assertion_line(position.get(), shadow.world.0)?;
                self.buffer.append_archived_world_effect(&table, event, position)?;
            } else {
                self.buffer.append_unpositioned_replay(&table, event)?;
            }
            self.record_owned_change(&relation, shadow.id, change, shadow.valid, shadow.position, Ok(None))?;
        }
        Ok(())
    }

    fn stage_replayed_topology(
        &mut self, replay: &HostedReplay, table: TableRef, world: Iid,
        position: Option<std::num::NonZeroU32>, record: StructFields<Value>,
    ) -> Result<()> {
        let Some(position) = position else { return self.buffer.put(table, record, Interval::ALL); };
        replay.require_assertion_line(position.get(), world.0)?;
        self.buffer.append_archived_world_effect(&table, StoredEvent {
            position: None,
            origin: None,
            commit_position: 0,
            entity_key: None,
            iid: world,
            system_from: self.tx_key().system_time,
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(record),
            recency: None,
        }, position)
    }

    fn record_replayed_registration(&mut self, replay: &HostedReplay, world: &OwnedWorld) -> Result<()> {
        let receipt = ReplayedRegistration {
            binding: replay.holds_binding(),
            pin: world.pin.0,
            source: replay.source_position(),
            logical: replay.logical_time(),
        };
        let body = serde_json::to_vec(&receipt).map_err(|_| OwnedWorld::unavailable())?;
        self.buffer.put(
            TableRef::new("public", crate::hosted_control::RESTORE),
            StructFields::from([
                ("_id".into(), Value::Utf8(registration_locator(world.id))),
                ("binding".into(), Value::Binary(body)),
            ]),
            Interval::new(Timestamp(0), Timestamp::MAX)?,
        )
    }

    fn record_replayed_held_publication(
        &mut self,
        replay: &HostedReplay,
        world: &OwnedWorld,
        reading: Timestamp,
    ) -> Result<()> {
        let source = replay.source_position();
        let proof = ReplayedHeldPublication {
            binding: replay.holds_binding(),
            pin: world.pin.0,
            root: world.basis,
            source,
            reading,
        };
        let body = serde_json::to_vec(&proof).map_err(|_| OwnedWorld::unavailable())?;
        self.buffer.put(
            TableRef::new("public", crate::hosted_control::RESTORE),
            StructFields::from([
                ("_id".into(), Value::Utf8(held_publication_locator(world.id, source.tx_id))),
                ("binding".into(), Value::Binary(body)),
            ]),
            Interval::new(Timestamp(0), Timestamp::MAX)?,
        )
    }

    fn record_replayed_world_relocation(
        &mut self,
        replay: &HostedReplay,
        world: Iid,
        original: &WorldReadmission,
    ) -> Result<()> {
        #[derive(serde::Serialize)]
        struct Relocation<'a> {
            binding: [u8; 32],
            original: &'a WorldReadmission,
        }
        let body = serde_json::to_vec(&Relocation {
            binding: replay.holds_binding(),
            original,
        })
        .map_err(|_| OwnedWorld::unavailable())?;
        self.buffer.put(
            TableRef::new("public", "bj_rtdb_restore_hold"),
            StructFields::from([
                ("_id".into(), Value::Utf8(replay.relocation_locator(world))),
                ("binding".into(), Value::Binary(body)),
            ]),
            Interval::new(Timestamp(0), Timestamp::MAX)?,
        )
    }

    async fn record_replayed_world_parent(
        &mut self,
        replay: &HostedReplay,
        context: &Arc<rtdb_query::QueryContext>,
        world: &OwnedWorld,
    ) -> Result<()> {
        let table = TableRef::new("public", "bj_rtdb_restore_hold");
        let original = EntityId::String(format!("native-transaction/{}", world.basis.tx_id.0));
        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(context),
            table.clone(),
            &original,
            true,
        )
        .await?;
        let (row, system_time) = point.row.ok_or_else(OwnedWorld::unavailable)?;
        let tx_id = rtdb_types::TxId(point.receipt_tx_id.ok_or_else(OwnedWorld::unavailable)?);
        let destination = TransactionKey { tx_id, system_time };
        let bytes = row
            .batch()
            .column_by_name("binding")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .filter(|column| column.is_valid(0))
            .ok_or_else(OwnedWorld::unavailable)?;
        if destination.system_time <= replay.source_time()
            || destination.tx_id >= self.tx_key().tx_id || !destination.at_or_before(self.tx_key())
            || !replay.matches_parent_receipt(bytes.value(0), world.basis)
        {
            return Err(OwnedWorld::unavailable());
        }
        let parent = ReplayedWorldParent {
            binding: replay.holds_binding(),
            world: world.id.0,
            pin: world.pin.0,
            original: world.basis,
            destination,
        };
        let locator = ReplayedWorldParent::locator(world.id, world.pin);
        let body = serde_json::to_vec(&parent).map_err(|_| OwnedWorld::unavailable())?;
        self.buffer.put(
            table,
            StructFields::from([
                ("_id".into(), Value::Utf8(locator)),
                ("binding".into(), Value::Binary(body)),
            ]),
            Interval::new(Timestamp(0), Timestamp::MAX)?,
        )
    }
}

/// Every copied child must extend its parent's earlier accepted prefix. The
/// replay installer retains the source registrations used by this check,
/// including intermediate aliases that have expired at the capture end.
async fn require_replayed_fork_parent(
    context: &Arc<rtdb_query::QueryContext>,
    owner: &str,
    world: &OwnedWorld,
    replay: &HostedReplay,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let Some(link) = world.fork_lineage.last() else { return Ok(()); };
    let logical = replay.logical_time();
    let (source, destination, _reservation) =
        rtdb_query::read_owned_world_with_revision(Arc::clone(context), owner, Iid(link.world))
            .await?
            .ok_or_else(unavailable)?;
    let admits_child = if !world.ancestors.is_empty() {
        source.can_retain_overlay(world)
    } else {
        source.can_retain_parent(world)
    };
    let (receipt, registered_at) = replay_control(context, &registration_locator(source.id)).await?;
    let registration: ReplayedRegistration = serde_json::from_slice(&receipt).map_err(|_| unavailable())?;
    let locator = format!("native-transaction/{}", registration.source.tx_id.0);
    let (accepted, accepted_at) = replay_control(context, &locator).await?;
    if !admits_child
        || source.pin.0 != link.pin
        || !registration.precedes(replay.holds_binding(), replay.source_position(), link)
        || destination != registered_at.system_time || registered_at != accepted_at
        || !replay.matches_parent_receipt(&accepted, registration.source)
        || link.registered >= logical
        || logical >= source.until
        || world.fork_lineage != source.retained_child_lineage(world, link.registered)?
    {
        return Err(unavailable());
    }
    for ancestor in &world.ancestors {
        require_replayed_held_publication(context, owner, replay, ancestor).await?;
    }
    Ok(())
}

async fn require_replayed_held_publication(
    context: &Arc<rtdb_query::QueryContext>,
    owner: &str,
    replay: &HostedReplay,
    ancestor: &rtdb_types::OwnedWorldAncestor,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let id = Iid(ancestor.world);
    let (body, selected_destination) =
        replay_control(context, &held_publication_locator(id, ancestor.publication.tx_id)).await?;
    let proof: ReplayedHeldPublication = serde_json::from_slice(&body).map_err(|_| unavailable())?;
    let selected = proof.source;
    if !proof.binds(replay.holds_binding(), ancestor) {
        return Err(unavailable());
    }
    if selected != ancestor.basis {
        let locator = format!("native-transaction/{}", ancestor.basis.tx_id.0);
        let (media_receipt, media_destination) = replay_control(context, &locator).await?;
        if media_destination.tx_id <= selected_destination.tx_id || !selected_destination.at_or_before(media_destination)
            || !replay.matches_parent_receipt(&media_receipt, ancestor.basis) {
            return Err(unavailable());
        }
    }
    let locator = format!("native-transaction/{}", selected.tx_id.0);
    let (receipt, destination) = replay_control(context, &locator).await?;
    let (record, reading, _charge) =
        rtdb_query::read_held_world_publication(context, owner, id, destination).await?;
    if destination != selected_destination
        || !replay.matches_parent_receipt(&receipt, selected)
        || record.pin.0 != proof.pin || record.basis != proof.root || reading != proof.reading
    {
        return Err(unavailable());
    }
    Ok(())
}

async fn replay_control(
    context: &Arc<rtdb_query::QueryContext>,
    locator: &str,
) -> Result<(Vec<u8>, TransactionKey)> {
    let point = rtdb_query::read_hosted_control_point(
        Arc::clone(context),
        TableRef::new("public", "bj_rtdb_restore_hold"),
        &EntityId::String(locator.to_owned()),
        true,
    )
    .await?;
    let (row, system_time) = point.row.ok_or_else(OwnedWorld::unavailable)?;
    let tx_id = rtdb_types::TxId(point.receipt_tx_id.ok_or_else(OwnedWorld::unavailable)?);
    let body = row
        .batch()
        .column_by_name("binding")
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .filter(|column| column.is_valid(0) && column.value(0).len() <= 4096)
        .ok_or_else(OwnedWorld::unavailable)?;
    Ok((body.value(0).to_vec(), TransactionKey { tx_id, system_time }))
}

#[cfg(test)]
mod tests {
    use super::{ReplayedHeldPublication, ReplayedRegistration};
    use rtdb_types::{OwnedWorldAncestor, OwnedWorldForkLink, Timestamp, TransactionKey, TxId};

    #[test]
    fn replayed_held_publication_binds_the_selected_cut_within_its_media() {
        let source = TransactionKey { tx_id: TxId(8), system_time: Timestamp(90) };
        let root = TransactionKey { tx_id: TxId(5), system_time: Timestamp(60) };
        let proof = ReplayedHeldPublication {
            binding: [1; 32], pin: [2; 16], root, source, reading: Timestamp(61),
        };
        let mut ancestor = OwnedWorldAncestor {
            world: [3; 16], pin: proof.pin, capture: [4; 16], suffix: [5; 16], manifest: String::new(),
            basis: source, publication: source, source_root: root, logical: proof.reading,
        };
        assert!(proof.binds([1; 32], &ancestor));
        ancestor.basis.tx_id = TxId(9);
        assert!(proof.binds([1; 32], &ancestor));
        ancestor.publication.tx_id = TxId(7);
        assert!(!proof.binds([1; 32], &ancestor));
        ancestor.publication = source;
        ancestor.basis.system_time = Timestamp(100);
        assert!(proof.binds([1; 32], &ancestor));
        ancestor.logical = Timestamp(62);
        assert!(!proof.binds([1; 32], &ancestor));
        ancestor.logical = proof.reading;
        ancestor.source_root = source;
        assert!(!proof.binds([1; 32], &ancestor));
    }

    #[test]
    fn replayed_registration_rejects_an_unrelated_pin_or_original_cut() {
        let source = TransactionKey { tx_id: TxId(8), system_time: Timestamp(90) };
        let current = TransactionKey { tx_id: TxId(9), system_time: Timestamp(100) };
        let observed = ReplayedRegistration {
            binding: [1; 32], pin: [2; 16], source, logical: Timestamp(60),
        };
        let mut link = OwnedWorldForkLink {
            world: [3; 16], pin: [2; 16], registered: Timestamp(60),
            system: Timestamp(55), logical: Timestamp(41),
        };
        assert!(observed.precedes([1; 32], current, &link));
        assert!(!observed.precedes([1; 32], source, &link));
        link.pin = [4; 16];
        assert!(!observed.precedes([1; 32], current, &link));
        link.pin = [2; 16];
        link.registered = Timestamp(61);
        assert!(!observed.precedes([1; 32], current, &link));
    }
}
