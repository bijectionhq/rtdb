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
use crate::hosted_control::{HostedReplay, ReplayWorlds, ReplayedWorldParent};

impl OpenTransaction<'_> {
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

        for world in effects.registrations {
            let previous =
                rtdb_query::read_owned_world(Arc::clone(context), owner, world.id).await?;
            if world.basis.system_time > at || world.dropped || world.until <= at {
                return Err(OwnedWorld::unavailable());
            }
            match previous {
                None if world.pin == world.id => (),
                Some((previous, _))
                    if previous.until > at
                        && (previous.can_advance_to(&world)
                            || previous.can_renew_to(&world)
                            || previous.can_hold_proposal(&world)
                            || previous.can_shorten_proposal(&world)) =>
                {
                    ()
                }
                _ => return Err(OwnedWorld::unavailable()),
            }
            self.record_replayed_world_parent(replay, context, &world)
                .await?;
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
                    WorldClockMode::Held { .. } => clock <= logical,
                    WorldClockMode::Retained => false,
                };
                if world.dropped
                    || at >= world.until
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
                active.insert(id, (world, charge));
            }
        }

        let mut identities = BTreeSet::new();
        for shadow in effects.shadows {
            let (world, _) = active
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
            if world.mode == WorldClockMode::Proposal {
                schema.require_replayed_period(shadow.valid, logical, rtdb_types::EventOp::Put)?;
            } else {
                if schema.requires_bounded_period() {
                    return Err(OwnedWorld::unavailable());
                }
                schema.require_write_validity(true)?;
            }
            let (image, change) = if let Some(mut fields) = shadow.fields {
                fields.insert("_id".into(), shadow.id.as_value());
                validate_owned_schema(schema, &fields)?;
                (WorldShadow::Present(fields), OwnedChangeKind::Put)
            } else {
                (WorldShadow::Deleted, OwnedChangeKind::Delete)
            };
            self.buffer.append(
                relation.table(owner),
                StoredEvent {
                    entity_key: Some(shadow.id.handle_bytes().into()),
                    iid: shadow.id.iid(),
                    system_from: self.tx_key().system_time,
                    valid_time: shadow.valid,
                    operation: StoredOperation::Put(image.record(&shadow.id)?),
                    recency: None,
                },
            )?;
            self.record_owned_change(&relation, shadow.id, change, shadow.valid)?;
        }
        Ok(())
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
        let Some((row, destination)) = point.row else {
            // A retained world may have been forked between two recorded
            // Original publications. Its parent still needs a covering proof.
            return Ok(());
        };
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
