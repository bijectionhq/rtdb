//! Install Original world facts atomically with the held replay frontier.

use std::{
    collections::{BTreeMap, BTreeSet},
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
        if admission.basis.system_time != proven
            || restored != proven
            || admission.basis.tx_id >= ordered.tx_id
            || root_clock > world.basis.system_time
            || (world.mode == WorldClockMode::Proposal && root_clock != logical)
            || lease_start < proven
            || lease_start > ordered.system_time
            || admission.until <= ordered.system_time
        {
            return Err(unavailable());
        }
        let ancestor = admission.retained_ancestor(&world)?.cloned();
        if let Some(ancestor) = &ancestor {
            let original = world.ancestor.as_ref().ok_or_else(unavailable)?.basis;
            let locator = format!("native-transaction/{}", original.tx_id.0);
            let (receipt, destination) = replay_control(context, &locator).await?;
            replayed_parent_logical_time(&receipt, admission.binding, &archive, original)?;
            if ancestor.basis.system_time != destination
                || ancestor.basis.tx_id >= ordered.tx_id
                || destination > lease_start
                || ancestor.system <= admission.basis.system_time
                || ancestor.system > destination
            {
                return Err(unavailable());
            }
            let selected = context.with_owned_system_cut(ancestor.system, &context.cancellation)?;
            let clock = rtdb_query::read_owned_world_clock_revision(
                selected, owner, Iid(ancestor.world),
            ).await?;
            if clock != Some((ancestor.logical, ancestor.system)) {
                return Err(unavailable());
            }
        }
        admission.require_application(&world, controls)?;
        world.ancestor = ancestor;
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
                || world.ancestor.as_ref().is_some_and(|ancestor| ancestor.basis.system_time >= at)
            {
                return Err(OwnedWorld::unavailable());
            }
            if let Some(readmission) = registration.readmission.as_ref() {
                let (previous, _charge) = previous.ok_or_else(OwnedWorld::unavailable)?;
                readmission.require_relocation(&previous, &world, logical)?;
                self.record_replayed_world_relocation(replay, world.id, readmission)?;
            } else {
                match previous {
                    None if world.pin == world.id && !world.dropped => (),
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
            self.buffer.put(
                OwnedRelation::world_catalog().table(owner),
                world.record(),
                Interval::ALL,
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
            for (id, clock) in effects.clocks {
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
                self.buffer.put(
                    OwnedRelation::world_clock_table(owner),
                    StructFields::from([
                        ("_id".into(), Value::Uuid(id.0)),
                        ("clock".into(), Value::I64(clock.0)),
                    ]),
                    Interval::ALL,
                )?;
                active.insert(id, (world, charge, clock));
            }
        }

        let mut identities = BTreeSet::new();
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
                || !identities.insert((
                    relation.clone(),
                    shadow.id.clone(),
                    shadow.valid.lower(),
                    shadow.valid.upper(),
                ))
            {
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
            let position = self.buffer.append_to(
                &relation.table(owner),
                StoredEvent {
                    origin: None,
                    commit_position: 0,
                    entity_key: Some(shadow.id.handle_bytes().into()),
                    iid: shadow.id.iid(),
                    system_from: self.tx_key().system_time,
                    valid_time: shadow.valid,
                    operation: StoredOperation::Put(image.record(&shadow.id)?),
                    recency: None,
                },
            )?;
            self.record_owned_change(&relation, shadow.id, change, shadow.valid, position, Ok(None))?;
        }
        Ok(())
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
        let (row, destination) = point.row.ok_or_else(OwnedWorld::unavailable)?;
        let bytes = row
            .batch()
            .column_by_name("binding")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .filter(|column| column.is_valid(0))
            .ok_or_else(OwnedWorld::unavailable)?;
        if destination <= replay.source_time()
            || destination >= self.tx_key().system_time
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

async fn replay_control(
    context: &Arc<rtdb_query::QueryContext>,
    locator: &str,
) -> Result<(Vec<u8>, Timestamp)> {
    let (row, revision) = rtdb_query::read_hosted_control_point(
        Arc::clone(context),
        TableRef::new("public", "bj_rtdb_restore_hold"),
        &EntityId::String(locator.to_owned()),
        true,
    )
    .await?
    .row
    .ok_or_else(OwnedWorld::unavailable)?;
    let body = row
        .batch()
        .column_by_name("binding")
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .filter(|column| column.is_valid(0) && column.value(0).len() <= 4096)
        .ok_or_else(OwnedWorld::unavailable)?;
    Ok((body.value(0).to_vec(), revision))
}
