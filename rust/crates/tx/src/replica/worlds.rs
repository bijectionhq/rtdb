use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_format::data::StoredOperation;
use rtdb_index::{IndexBatch, TableWrite};
use rtdb_types::{EventOp, Iid, Interval, OWNED_ATTEMPT_RELATION, OWNED_PUBLIC_ROOT_VERSIONS_RELATION, OWNED_ROOT_PUBLICATION_RELATION, OwnedRelation, OwnedWorld, Result, StructFields, TableRef, Timestamp, TransactionKey, Value};

use crate::{TxOp, source::Operation};
use super::invalid;

pub(crate) const WORLD_EFFECTS: &str = "owned_world_effects";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind { Root, Overlay, TimedOverlay, ProposalOverlay, ProposalSettlement, ProposalLanding, ProposalClose, Clock, Fork, RootHeldFork, ControlsHeldFork, RootRetainedFork, RootRetainedAdvance, RootRetainedRenewal, RootRetainedSibling, ControlsRetainedFork, ProposalControlsFork, ProposalRootFork, ProposalRootHold, ProposalRootShorten, Advance, Drop, ControlsRetainedDrop, RootRetainedDrop }

const ROOT: Iid = Iid([0; 16]);

impl Kind {
    fn writes_data(self) -> bool {
        matches!(self, Self::Root | Self::Overlay | Self::TimedOverlay | Self::ProposalOverlay | Self::RootHeldFork | Self::RootRetainedFork | Self::RootRetainedAdvance | Self::RootRetainedRenewal | Self::RootRetainedSibling | Self::RootRetainedDrop | Self::ProposalRootFork | Self::ProposalRootHold | Self::ProposalRootShorten | Self::ProposalLanding | Self::ProposalClose)
    }

    fn relation_world(self, registered_world: Iid) -> Iid {
        if matches!(self, Self::RootHeldFork | Self::RootRetainedFork | Self::RootRetainedAdvance | Self::RootRetainedRenewal | Self::RootRetainedSibling | Self::RootRetainedDrop | Self::ProposalRootFork | Self::ProposalRootHold | Self::ProposalRootShorten | Self::ProposalLanding | Self::ProposalClose) { ROOT } else { registered_world }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Overlay => "overlay",
            Self::TimedOverlay => "timed-overlay",
            Self::ProposalOverlay => "proposal-overlay",
            Self::ProposalSettlement => "proposal-settlement",
            Self::ProposalClose => "proposal-close",
            Self::ProposalLanding => "proposal-landing",
            Self::Clock => "clock",
            Self::Fork => "fork",
            Self::RootHeldFork => "root-held-fork",
            Self::ControlsHeldFork => "controls-held-fork",
            Self::RootRetainedFork => "root-retained-fork",
            Self::RootRetainedAdvance => "root-retained-advance",
            Self::RootRetainedRenewal => "root-retained-renewal",
            Self::RootRetainedSibling => "root-retained-sibling",
            Self::ControlsRetainedFork => "controls-retained-fork",
            Self::ProposalControlsFork => "controls-proposal-fork",
            Self::ProposalRootShorten => "root-proposal-shorten",
            Self::ProposalRootHold => "root-proposal-hold",
            Self::ProposalRootFork => "root-proposal-fork",
            Self::Advance => "advance",
            Self::Drop => "drop",
            Self::ControlsRetainedDrop => "controls-retained-drop",
            Self::RootRetainedDrop => "root-retained-drop",
        }
    }

    fn parse(name: &str) -> Result<Self> {
        match name {
            "root" => Ok(Self::Root),
            "overlay" => Ok(Self::Overlay),
            "timed-overlay" => Ok(Self::TimedOverlay),
            "proposal-overlay" => Ok(Self::ProposalOverlay),
            "proposal-settlement" => Ok(Self::ProposalSettlement),
            "proposal-close" => Ok(Self::ProposalClose),
            "proposal-landing" => Ok(Self::ProposalLanding),
            "clock" => Ok(Self::Clock),
            "fork" => Ok(Self::Fork),
            "root-held-fork" => Ok(Self::RootHeldFork),
            "controls-held-fork" => Ok(Self::ControlsHeldFork),
            "root-retained-fork" => Ok(Self::RootRetainedFork),
            "root-retained-advance" => Ok(Self::RootRetainedAdvance),
            "root-retained-renewal" => Ok(Self::RootRetainedRenewal),
            "root-retained-sibling" => Ok(Self::RootRetainedSibling),
            "controls-retained-fork" => Ok(Self::ControlsRetainedFork),
            "controls-proposal-fork" => Ok(Self::ProposalControlsFork),
            "root-proposal-shorten" => Ok(Self::ProposalRootShorten),
            "root-proposal-hold" => Ok(Self::ProposalRootHold),
            "root-proposal-fork" => Ok(Self::ProposalRootFork),
            "advance" => Ok(Self::Advance),
            "drop" => Ok(Self::Drop),
            "controls-retained-drop" => Ok(Self::ControlsRetainedDrop),
            "root-retained-drop" => Ok(Self::RootRetainedDrop),
            _ => Err(mismatch()),
        }
    }
}

pub(crate) fn source_inventory(operations: &[Operation]) -> Result<Vec<Value>> {
    use crate::hosted_control::ProposalLandingEffect;

    let mut selected = BTreeMap::<Iid, BTreeSet<&str>>::new();
    let mut readings = BTreeMap::new();
    let proposal_land = crate::hosted_control::hosted_proposal_land(operations);
    for operation in operations {
        if let Operation::Public(TxOp::OwnedPublishWorld { world, advance_to: Some(at), .. }) = operation {
            readings.insert(*world, *at);
        }
        if crate::hosted_control::root_partition_frontier_rows(operation).is_some()
            || proposal_land.is_some_and(|landing| landing.effect != ProposalLandingEffect::Close)
                && crate::hosted_control::proposal_landing_frontier_rows(operation).is_some()
        {
            continue;
        }
        if let Some((relation, rows)) = operation.owned_documents() && rows > 0 {
            selected.entry(relation.world().unwrap_or(ROOT)).or_default().insert(relation.name());
        } else if let Operation::Public(TxOp::OwnedDelete { relation, .. } | TxOp::OwnedErase { relation, .. }) = operation {
            selected.entry(ROOT).or_default().insert(relation.name());
        }
    }
    let retained_release = crate::hosted_control::retained_release_world(operations);
    let proposal_overlay = crate::hosted_control::hosted_proposal_overlay(operations);
    operations.iter().filter_map(|operation| match operation {
        Operation::Public(TxOp::OwnedForkWorld { record, previous, attempt, fingerprint, root, .. }) => Some(
            match record.get("_id") {
                Some(Value::Uuid(world)) => {
                    let held = matches!(record.get("mode"), Some(Value::Utf8(mode)) if mode == "held");
                    let kind = match root {
                        crate::OwnedForkRoot::Writes if held => Kind::RootHeldFork,
                        crate::OwnedForkRoot::Controls if held => Kind::ControlsHeldFork,
                        crate::OwnedForkRoot::Writes => Kind::RootRetainedFork,
                        crate::OwnedForkRoot::WritesAdvance => Kind::RootRetainedAdvance,
                        crate::OwnedForkRoot::WritesRenewal => Kind::RootRetainedRenewal,
                        crate::OwnedForkRoot::WritesSibling => Kind::RootRetainedSibling,
                        crate::OwnedForkRoot::Controls => Kind::ControlsRetainedFork,
                        crate::OwnedForkRoot::ProposalControls => Kind::ProposalControlsFork,
                        crate::OwnedForkRoot::ProposalShorten => Kind::ProposalRootShorten,
                        crate::OwnedForkRoot::ProposalHold => Kind::ProposalRootHold,
                        crate::OwnedForkRoot::ProposalWrites => Kind::ProposalRootFork,
                        crate::OwnedForkRoot::None if previous.is_some() => Kind::Advance,
                        crate::OwnedForkRoot::None => Kind::Fork,
                    };
                    Ok((Iid(*world), kind, attempt, fingerprint))
                }
                _ => Err(mismatch()),
            }),
        Operation::Public(TxOp::OwnedPublishRoot { attempt, fingerprint }) =>
            Some(Ok((ROOT, Kind::Root, attempt, fingerprint))),
        Operation::Public(TxOp::OwnedPublishWorld { world, attempt, fingerprint, advance_to, drop_world, settlement_pin, delete_root, .. }) => {
            let kind = if let Some(landing) = proposal_land && landing.world == *world {
                if landing.effect == ProposalLandingEffect::Close { Kind::ProposalClose } else { Kind::ProposalLanding }
            }
                else if delete_root.is_some() { Kind::RootRetainedDrop }
                else if retained_release == Some(*world) { Kind::ControlsRetainedDrop }
                else if proposal_overlay == Some(*world) {
                    if settlement_pin.is_some() { Kind::ProposalSettlement } else { Kind::ProposalOverlay }
                }
                else if *drop_world { Kind::Drop }
                else if advance_to.is_some() { Kind::Clock }
                else { Kind::Overlay };
            Some(Ok((*world, kind, attempt, fingerprint)))
        }
        _ => None,
    }).map(|effect| {
        let (world, kind, attempt, fingerprint) = effect?;
        let relations = selected.remove(&kind.relation_world(world)).unwrap_or_default();
        let kind = if kind == Kind::Clock && !relations.is_empty() { Kind::TimedOverlay } else { kind };
        if relations.is_empty() == kind.writes_data() { return Err(mismatch()); }
        let mut effect = StructFields::from([
            ("world".into(), Value::Uuid(world.0)),
            ("kind".into(), Value::Utf8(kind.name().to_owned())),
            ("attempt".into(), Value::Utf8(attempt.clone())),
            ("fingerprint".into(), Value::Binary(fingerprint.to_vec())),
            ("relations".into(), Value::list(relations.into_iter().map(|name| Value::Utf8(name.to_owned())).collect())),
        ]);
        if kind == Kind::TimedOverlay {
            effect.insert("reading".into(), Value::I64(readings.get(&world).ok_or_else(mismatch)?.0));
        }
        Ok(Value::Struct(effect))
    }).collect()
}

pub(super) fn verify(
    tables: &BTreeMap<TableRef, TableWrite>, effects: &[Value], key: TransactionKey,
    memory: &Arc<MemoryBudget>, family_fence: bool,
) -> Result<HostedEffects> {
    if !(1..=9).contains(&effects.len()) { return Err(mismatch()); }
    let mut worlds = BTreeMap::new();
    let mut timed_readings = BTreeMap::new();
    let mut attempts = BTreeMap::new();
    let mut expected_data = BTreeSet::new();
    let mut common_fingerprint: Option<&[u8]> = None;
    let mut hosted = HostedEffects::CheckOnly;
    let mut root_retained_fork = false;
    let mut root_holder_replace = false;
    for effect in effects {
        let Value::Struct(fields) = effect else { return Err(mismatch()); };
        let (Some(Value::Uuid(world)), Some(Value::Utf8(kind)), Some(Value::Utf8(attempt)), Some(Value::Binary(fingerprint)), Some(Value::List(relations))) =
            (fields.get("world"), fields.get("kind"), fields.get("attempt"), fields.get("fingerprint"), fields.get("relations"))
        else { return Err(mismatch()); };
        let kind = Kind::parse(kind)?;
        let world = Iid(*world);
        if kind == Kind::TimedOverlay {
            let Some(Value::I64(reading)) = fields.get("reading") else { return Err(mismatch()); };
            timed_readings.insert(world, *reading);
        }
        if fields.len() != 5 + usize::from(kind == Kind::TimedOverlay) || relations.is_empty() == kind.writes_data()
            || (world == ROOT) != (kind == Kind::Root) || fingerprint.len() != 32
            || attempt.len() != 22 || !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || (effects.len() > 1 && kind == Kind::ProposalOverlay && !(family_fence && effects.len() == 2))
            || (effects.len() > 1 && matches!(kind, Kind::ProposalSettlement | Kind::Fork | Kind::RootHeldFork | Kind::ControlsHeldFork | Kind::RootRetainedFork | Kind::RootRetainedAdvance | Kind::RootRetainedRenewal | Kind::RootRetainedSibling | Kind::ControlsRetainedFork | Kind::ProposalControlsFork | Kind::ProposalRootFork | Kind::ProposalRootHold | Kind::ProposalRootShorten | Kind::Advance | Kind::ControlsRetainedDrop | Kind::RootRetainedDrop | Kind::ProposalLanding | Kind::ProposalClose))
            || (matches!(kind, Kind::RootRetainedDrop | Kind::ProposalClose | Kind::RootRetainedAdvance | Kind::RootRetainedRenewal | Kind::RootRetainedSibling | Kind::ProposalRootHold | Kind::ProposalRootShorten) && relations.len() != 1)
            || common_fingerprint.is_some_and(|first| first != fingerprint.as_slice())
            || worlds.insert(world, kind).is_some()
            || attempts.insert(attempt.as_str(), fingerprint.as_slice()).is_some()
        {
            return Err(mismatch());
        }
        if kind == Kind::RootRetainedFork {
            root_retained_fork = true;
            hosted = HostedEffects::OptionalReplacements;
        } else if matches!(kind, Kind::RootRetainedAdvance | Kind::RootRetainedRenewal | Kind::RootRetainedSibling) {
            root_retained_fork = true;
            root_holder_replace = true;
            hosted = HostedEffects::RetainedWrite;
        } else if matches!(kind, Kind::ProposalRootHold | Kind::ProposalRootShorten) {
            root_retained_fork = true;
            root_holder_replace = kind == Kind::ProposalRootHold;
            hosted = HostedEffects::ProposalHold;
        } else if matches!(kind, Kind::RootHeldFork | Kind::ProposalRootFork) {
            root_retained_fork = true;
            hosted = HostedEffects::RequiredReplacements;
        } else if matches!(kind, Kind::ControlsHeldFork | Kind::ControlsRetainedFork | Kind::ProposalControlsFork) {
            hosted = HostedEffects::RequiredReplacements;
        } else if matches!(kind, Kind::ProposalOverlay | Kind::ProposalSettlement) {
            hosted = HostedEffects::ProposalOverlay;
        } else if kind == Kind::ControlsRetainedDrop {
            hosted = HostedEffects::RetainedRelease;
        } else if kind == Kind::ProposalClose {
            hosted = HostedEffects::ProposalClose;
        } else if kind == Kind::ProposalLanding {
            hosted = HostedEffects::ProposalLanding;
        } else if kind == Kind::RootRetainedDrop {
            hosted = HostedEffects::RetainedWrite;
        }
        if relations.is_empty() { return Err(mismatch()); }
        for index in 0..relations.len() {
            let name = relations.view().utf8(index).ok_or_else(mismatch)?;
            let relation = OwnedRelation::from_source(name.to_owned()).map_err(|_| mismatch())?;
            if relation.is_world_catalog() || matches!(name, OWNED_ATTEMPT_RELATION | OWNED_ROOT_PUBLICATION_RELATION)
                || relation.world().unwrap_or(ROOT) != kind.relation_world(world)
                || !expected_data.insert(name) || expected_data.len() > 4096
            {
                return Err(mismatch());
            }
        }
        common_fingerprint = Some(fingerprint);
    }
    let separate_root = worlds.contains_key(&ROOT);
    if separate_root && worlds.len() == 1 || worlds.len() - usize::from(separate_root) > 8
        || worlds.len() > 1 && matches!(hosted, HostedEffects::ProposalOverlay) && !separate_root
    {
        return Err(mismatch());
    }
    let delete_root = worlds.values().any(|kind| matches!(kind, Kind::RootRetainedDrop | Kind::ProposalRootShorten | Kind::ProposalClose));
    let landed = matches!(hosted, HostedEffects::ProposalLanding);
    let has_root = separate_root || root_retained_fork || delete_root || landed;
    if has_root {
        expected_data.insert(OWNED_ROOT_PUBLICATION_RELATION);
    }
    let mut root_head = false;
    let mut public_version_seen = false;
    let mut publication_frontiers = BTreeSet::new();
    let mut landed_rows = 0usize;
    let mut landed_positions = BTreeSet::new();
    let mut data = BTreeSet::new();
    let mut journals = BTreeSet::new();
    let mut overlays = BTreeSet::new();
    let mut clocks = BTreeSet::new();
    let mut registrations = BTreeSet::new();
    let controlled_root = separate_root && matches!(hosted, HostedEffects::ProposalOverlay);
    let mut root_erases = BTreeSet::new();
    let mut search_erase_markers = BTreeSet::new();
    let mut purge_markers = BTreeSet::new();
    let mut purge_floors = BTreeSet::new();
    for (table, write) in tables {
        let Some(name) = table.owned_storage_name() else { continue; };
        let batch = write.batch().filter(|batch| !batch.is_empty()).ok_or_else(mismatch)?;
        if name == OWNED_ATTEMPT_RELATION {
            records(batch, key, memory, |row| {
                let (Some(Value::Utf8(id)), Some(Value::Binary(fingerprint))) = (row.get("_id"), row.get("fingerprint"))
                else { return Err(mismatch()); };
                if row.len() != 2 || attempts.remove(id.as_str()) != Some(fingerprint.as_slice()) {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if table.is_owned_world_clock_table() {
            records(batch, key, memory, |row| {
                let (Some(Value::Uuid(world)), Some(Value::I64(clock))) = (row.get("_id"), row.get("clock"))
                else { return Err(mismatch()); };
                let world = Iid(*world);
                let clock_kind = worlds.get(&world);
                if row.len() != 2 || !(Timestamp::MIN.0 < *clock && *clock < Timestamp::MAX.0)
                    || !matches!(clock_kind, Some(Kind::Overlay | Kind::TimedOverlay | Kind::ProposalOverlay | Kind::Clock))
                    || (clock_kind == Some(&Kind::ProposalOverlay) && *clock > key.system_time.0)
                    || timed_readings.get(&world).is_some_and(|reading| clock != reading)
                    || !clocks.insert(world)
                {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if table.owned_world_scope().0 {
            records(batch, key, memory, |row| {
                let record = OwnedWorld::from_record(row).map_err(|_| mismatch())?;
                let matches_kind = match worlds.get(&record.id) {
                    Some(Kind::Drop) => record.dropped,
                    Some(Kind::ProposalLanding | Kind::ProposalClose) => record.dropped
                        && record.mode == rtdb_types::WorldClockMode::Proposal
                        && record.parent_publication == Some(record.basis.system_time),
                    Some(Kind::ControlsRetainedDrop | Kind::RootRetainedDrop) => record.dropped
                        && record.mode == rtdb_types::WorldClockMode::Retained,
                    Some(Kind::Fork) => !record.dropped && record.id == record.pin,
                    Some(Kind::RootHeldFork | Kind::ControlsHeldFork) => !record.dropped
                        && record.id == record.pin && record.is_held_root_fork(),
                    Some(Kind::RootRetainedFork | Kind::ControlsRetainedFork) => !record.dropped && record.id == record.pin
                        && record.mode == rtdb_types::WorldClockMode::Retained,
                    Some(Kind::ProposalRootHold | Kind::ProposalRootShorten) => !record.dropped && record.id != record.pin
                        && record.mode == rtdb_types::WorldClockMode::Proposal
                        && record.parent_publication == Some(record.basis.system_time),
                    Some(Kind::ProposalControlsFork | Kind::ProposalRootFork) => !record.dropped && record.id == record.pin
                        && record.mode == rtdb_types::WorldClockMode::Proposal
                        && record.parent_publication == Some(record.basis.system_time),
                    Some(Kind::RootRetainedSibling) => !record.dropped && record.id == record.pin
                        && record.mode == rtdb_types::WorldClockMode::Retained
                        && record.parent_publication == Some(record.basis.system_time),
                    Some(Kind::RootRetainedAdvance | Kind::RootRetainedRenewal) => !record.dropped && record.id != record.pin
                        && record.mode == rtdb_types::WorldClockMode::Retained
                        && record.parent_publication == Some(record.basis.system_time),
                    Some(Kind::Advance) => !record.dropped && !record.mode.is_writable() && record.id != record.pin,
                    _ => false,
                };
                if record.basis.system_time >= key.system_time || record.until <= key.system_time
                    || !matches_kind || !registrations.insert(record.id)
                {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if let Some(relation) = table.owned_lifetime_relation() {
            if !has_root || !expected_data.contains(relation) { return Err(mismatch()); }
            let relation = OwnedRelation::new(relation).map_err(|_| mismatch())?;
            if batch.len() != 1 { return Err(mismatch()); }
            records(batch, key, memory, |row| {
                let floor = rtdb_types::OwnedLifetimeFloor::decode(row, &relation).map_err(|_| mismatch())?;
                floor.check(floor.duration, key.system_time).map_err(|_| mismatch())?;
                if floor.expired_created_through != rtdb_types::OwnedLifetimeFloor::at(floor.duration, key.system_time)
                    .map_err(|_| mismatch())?.expired_created_through {
                    return Err(mismatch());
                }
                Ok(())
            })?;
        } else if let Some((relation, floor)) = table.owned_purge_relation() {
            let selected = OwnedRelation::new(relation).map_err(|_| mismatch())?;
            if !family_fence || !separate_root || !expected_data.contains(relation)
                || !crate::hosted_control::family_root_erase(&selected)
            {
                return Err(mismatch());
            }
            if floor {
                purge_floors.insert(relation);
                for index in 0..batch.len() {
                    let header = batch.header(index)?;
                    if header.op != EventOp::Put || header.system_from != key.system_time
                        || header.valid_time != Interval::ALL
                    {
                        return Err(mismatch());
                    }
                }
            } else {
                purge_markers.insert(relation);
                if batch.len() != 1 { return Err(mismatch()); }
                records(batch, key, memory, |row| {
                    if row.len() != 1 || row.get("_id") != Some(&Value::I64(key.tx_id.0)) {
                        return Err(mismatch());
                    }
                    Ok(())
                })?;
            }
        } else if let Some(relation) = table.owned_root_constraint_relation() {
            if !has_root || !expected_data.contains(relation) || relation == OWNED_ROOT_PUBLICATION_RELATION
                || OwnedRelation::new(relation).is_err()
            {
                return Err(mismatch());
            }
            event_headers(batch, key, true, false)?;
        } else {
            let relation = table.owned_effect_relation_name().ok_or_else(mismatch)?;
            let is_data = name == relation;
            let world = OwnedRelation::from_source(relation.to_owned()).map_err(|_| mismatch())?.world();
            if let Some(world) = world {
                if !matches!(worlds.get(&world), Some(Kind::Overlay | Kind::TimedOverlay | Kind::ProposalOverlay)) { return Err(mismatch()); }
                if is_data { overlays.insert(world); }
            }
            if is_data {
                if relation == OWNED_PUBLIC_ROOT_VERSIONS_RELATION {
                    if !has_root || !expected_data.contains(relation) || public_version_seen || batch.len() != 1 {
                        return Err(mismatch());
                    }
                    records(batch, key, memory, |row| {
                        if row.len() != 1 || !matches!(row.get("_id"), Some(Value::Utf8(id)) if id == "head") {
                            return Err(mismatch());
                        }
                        Ok(())
                    })?;
                    public_version_seen = true;
                }
                if relation == OWNED_ROOT_PUBLICATION_RELATION {
                    if !has_root { return Err(mismatch()); }
                    let limit = if landed { crate::MAX_PROPOSAL_LAND_ROWS + 1 } else { 1 };
                    records_with_limit(batch, key, memory, limit, |row| {
                        let Some(Value::Utf8(id)) = row.get("_id") else { return Err(mismatch()); };
                        if row.len() != 1 {
                            return Err(mismatch());
                        }
                        if id == "head" && !root_head {
                            root_head = true;
                        } else if !landed || !crate::hosted_control::public_fact_frontier(id)
                            || !publication_frontiers.insert(id.clone())
                        {
                            return Err(mismatch());
                        }
                        Ok(())
                    })?;
                }
                if family_fence && separate_root && relation == "fact_sealed_search_erasures" {
                    records_with_limit(batch, key, memory, 4096, |row| {
                        if row.len() != 1 || !matches!(row.get("_id"), Some(Value::Utf8(id))
                            if search_erase_markers.insert(id.clone()))
                        {
                            return Err(mismatch());
                        }
                        Ok(())
                    })?;
                }
                data.insert(relation);
            } else if table.is_owned_change_journal() {
                journals.insert(relation);
            }
            let allows_delete = is_data && world.is_none()
                && !matches!(relation, OWNED_ROOT_PUBLICATION_RELATION | OWNED_PUBLIC_ROOT_VERSIONS_RELATION);
            if delete_root && allows_delete && (batch.len() != 1 || batch.header(0)?.op != EventOp::Delete) {
                return Err(mismatch());
            }
            if root_holder_replace && allows_delete && (batch.len() != 1 || batch.header(0)?.op != EventOp::Put) {
                return Err(mismatch());
            }
            if landed && allows_delete {
                landed_rows = landed_rows.saturating_add(batch.len());
                if landed_rows > crate::MAX_PROPOSAL_LAND_ROWS { return Err(mismatch()); }
                for index in 0..batch.len() {
                    if !landed_positions.insert(batch.header(index)?.commit_position) {
                        return Err(mismatch());
                    }
                }
            }
            let allows_erase = family_fence && separate_root && allows_delete && relation.ends_with("_search");
            if controlled_root && is_data && world.is_none()
                && !matches!(relation, OWNED_ROOT_PUBLICATION_RELATION | "fact_sealed_search_erasures")
            {
                for index in 0..batch.len() {
                    if !allows_erase || batch.header(index)?.op != EventOp::Erase { return Err(mismatch()); }
                }
            }
            if event_headers(batch, key, allows_delete && !root_holder_replace && !landed, allows_erase)? {
                root_erases.insert(relation);
            }
        }
    }
    if root_erases != purge_markers || root_erases != purge_floors
        || !root_erases.iter().copied().eq(search_erase_markers.iter().map(String::as_str))
        || (controlled_root && root_erases.is_empty())
        || (landed && (landed_rows == 0 || publication_frontiers.len() > landed_rows
        || !landed_positions.into_iter().eq(1..=landed_rows as u32)))
        || !attempts.is_empty() || root_head != has_root || data != expected_data
        || !journals.remove(OWNED_ATTEMPT_RELATION) || journals != expected_data
        || worlds.iter().any(|(world, kind)| match kind {
            Kind::Root | Kind::ProposalSettlement => false,
            Kind::Overlay | Kind::TimedOverlay | Kind::ProposalOverlay => !overlays.contains(world) || !clocks.contains(world),
            Kind::Clock => !clocks.contains(world),
            Kind::ProposalLanding | Kind::ProposalClose | Kind::Fork | Kind::RootHeldFork | Kind::ControlsHeldFork | Kind::RootRetainedFork | Kind::RootRetainedAdvance | Kind::RootRetainedRenewal | Kind::RootRetainedSibling | Kind::ControlsRetainedFork | Kind::ProposalControlsFork | Kind::ProposalRootFork | Kind::ProposalRootHold | Kind::ProposalRootShorten | Kind::Advance | Kind::Drop | Kind::ControlsRetainedDrop | Kind::RootRetainedDrop => !registrations.contains(world),
        })
    {
        return Err(mismatch());
    }
    Ok(hosted)
}

pub(super) enum HostedEffects {
    CheckOnly,
    OptionalReplacements,
    RequiredReplacements,
    RetainedRelease,
    ProposalOverlay,
    ProposalLanding,
    ProposalClose,
    RetainedWrite,
    ProposalHold,
}

fn event_headers(batch: &IndexBatch, key: TransactionKey, allows_delete: bool, allows_erase: bool) -> Result<bool> {
    let mut erased = false;
    for index in 0..batch.len() {
        let header = batch.header(index)?;
        if header.system_from != key.system_time
            || !(header.op == EventOp::Put || allows_delete && header.op == EventOp::Delete
                || allows_erase && header.op == EventOp::Erase && header.valid_time == Interval::ALL)
        {
            return Err(mismatch());
        }
        erased |= header.op == EventOp::Erase;
    }
    Ok(erased)
}

fn records(
    batch: &IndexBatch, key: TransactionKey, memory: &Arc<MemoryBudget>,
    inspect: impl FnMut(&StructFields<Value>) -> Result<()>,
) -> Result<()> {
    records_with_limit(batch, key, memory, 9, inspect)
}

fn records_with_limit(
    batch: &IndexBatch, key: TransactionKey, memory: &Arc<MemoryBudget>, maximum: usize,
    mut inspect: impl FnMut(&StructFields<Value>) -> Result<()>,
) -> Result<()> {
    if batch.len() > maximum { return Err(mismatch()); }
    let bound = batch.arrow().get_array_memory_size().checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(mismatch)?;
    let _decode = memory.reserve(bound)?;
    for index in 0..batch.len() {
        let event = batch.view().read_event(index)?;
        let StoredOperation::Put(row) = event.operation else { return Err(mismatch()); };
        if event.system_from != key.system_time || event.valid_time != Interval::ALL { return Err(mismatch()); }
        inspect(&row)?;
    }
    Ok(())
}

fn mismatch() -> rtdb_types::Anomaly {
    invalid("Resolved world effects disagree with their ordered placed inventory")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{TxId, WorldClockMode, WorldSchemas};

    #[test]
    fn held_birth_has_its_own_control_and_registration_inventory() -> Result<()> {
        let child = OwnedWorld {
            id: Iid([1; 16]), pin: Iid([1; 16]), capture: Iid([2; 16]), suffix: None,
            ancestors: Vec::new(), fork_lineage: Vec::new(), manifest: "a".repeat(43),
            basis: TransactionKey { tx_id: TxId(10), system_time: Timestamp(100) },
            parent_publication: Some(Timestamp(100)), mode: WorldClockMode::Held { start: Timestamp(100) },
            until: Timestamp(300), schemas: WorldSchemas::new(BTreeMap::new())?, dropped: false,
        };
        let inventory = source_inventory(&[Operation::Public(TxOp::OwnedForkWorld {
            record: child.record(), previous: None, sibling: None, overlay_source: None,
            attempt: "1".repeat(22), fingerprint: [2; 32], root: crate::OwnedForkRoot::Controls,
        })])?;
        let [Value::Struct(effect)] = inventory.as_slice() else { panic!("Expected one world effect"); };
        assert!(matches!(effect.get("kind"), Some(Value::Utf8(kind)) if kind == "controls-held-fork"));
        Ok(())
    }
    #[test]
    fn timed_overlay_inventory_binds_facts_and_exact_reading() -> Result<()> {
        let world = Iid([1; 16]);
        let relation = OwnedRelation::new("orders")?.in_world(world)?;
        let memory = MemoryBudget::new(1 << 20);
        let documents = crate::Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(1)),
        ])], &memory)?;
        let inventory = source_inventory(&[
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation, documents, valid_from: None, valid_to: None,
            }),
            Operation::Public(TxOp::OwnedPublishWorld {
                world, parent_pin: world, attempt: "1".repeat(22), fingerprint: [2; 32],
                expected: None, advance_to: Some(Timestamp(400)), drop_world: false,
                settlement_pin: None, land_root: None, delete_root: None,
            }),
        ])?;
        let [Value::Struct(effect)] = inventory.as_slice() else { panic!("Expected one world effect"); };
        assert!(matches!(effect.get("kind"), Some(Value::Utf8(kind)) if kind == "timed-overlay"));
        assert!(matches!(effect.get("reading"), Some(Value::I64(400))));
        assert!(matches!(effect.get("relations"), Some(Value::List(relations)) if relations.len() == 1));
        Ok(())
    }

}
