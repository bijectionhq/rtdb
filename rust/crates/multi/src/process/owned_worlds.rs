use std::{fmt, sync::Arc};

use futures::future::BoxFuture;
use rtdb_directory::{Directory, HostedTask};
use rtdb_flight_sql::{OwnedWorldHost, OwnedWorldRead, PreparedReplayParent, RetainedReplayWorld, WorldCall, WorldIdentityPage, WorldPointResult, WorldPointRow};
use rtdb_node::{NodeOptions, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, OwnedWorldWitness};
use rtdb_types::{EntityId, Interval, OwnedRelation, OwnedWorld, Result, Timestamp, TransactionKey, WorldClockMode};
use uuid::Uuid;

use crate::{
    archive::{CheckpointReadResources, HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world, commit_checkpoint_world_batch, finish_checkpoint_replay_parents, validate_checkpoint_world, fork_checkpoint_world_selected, prepare_checkpoint_replay_parent, retain_replayed_checkpoint_world},
    capability::attachment_storage,
    deployment::CellStorage,
    keys::KeyService,
};

pub(super) struct PlacedWorldHost {
    pub(super) directory: Arc<dyn Directory>,
    pub(super) agent: Arc<super::FenceSlot>,
    pub(super) capture_source: Option<CellStorage>,
    pub(super) archive: CellStorage,
    pub(super) keys: Arc<dyn KeyService>,
    pub(super) hosted_task: Option<HostedTask>,
    pub(super) options: NodeOptions,
}

impl fmt::Debug for PlacedWorldHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("PlacedWorldHost").finish_non_exhaustive()
    }
}

impl PlacedWorldHost {
    fn deployment(&self, call: &WorldCall<'_>) -> Result<Uuid> {
        let storage = attachment_storage(call.attachment).map_err(|_| OwnedWorld::unavailable())?;
        let epoch = storage.writer_epoch.ok_or_else(OwnedWorld::unavailable)?;
        if storage.compactor || storage.snapshot_block.is_some()
            || call.session.state().user().is_none_or(str::is_empty)
            || call.session.state().database() != call.attachment.name()
            || call.session.placed_history()?.is_none_or(|scope| scope.writer_epoch() != epoch)
        {
            return Err(OwnedWorld::unavailable());
        }
        Uuid::parse_str(&storage.deployment).map_err(|_| OwnedWorld::unavailable())
    }
}

impl OwnedWorldHost for PlacedWorldHost {
    fn prepare_replay_parent<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], original: Option<TransactionKey>,
    ) -> BoxFuture<'a, Result<Option<PreparedReplayParent>>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let source = self.capture_source.as_ref().ok_or_else(OwnedWorld::unavailable)?;
            let agent = self.agent.0.get().and_then(std::sync::Weak::upgrade)
                .ok_or_else(OwnedWorld::unavailable)?;
            prepare_checkpoint_replay_parent(
                Arc::clone(&self.directory), &self.archive, source, &agent, self.keys.as_ref(),
                self.hosted_task.as_ref(), call.session, deployment, binding, original, None,
                CheckpointReadResources {
                    node: &self.options, memory: call.node.memory(),
                    cache: Arc::clone(call.node.cache_budget()), cancellation: call.cancellation,
                },
            ).await
        })
    }

    fn finish_replay_parents<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            finish_checkpoint_replay_parents(
                self.directory.as_ref(), call.session, deployment, binding,
            ).await
        })
    }

    fn retain_replayed_world<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], world: Uuid,
    ) -> BoxFuture<'a, Result<RetainedReplayWorld>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let source = self.capture_source.as_ref().ok_or_else(OwnedWorld::unavailable)?;
            let agent = self.agent.0.get().and_then(std::sync::Weak::upgrade)
                .ok_or_else(OwnedWorld::unavailable)?;
            retain_replayed_checkpoint_world(
                Arc::clone(&self.directory), &self.archive, source, &agent, self.keys.as_ref(),
                self.hosted_task.as_ref(), call.session, deployment, binding, world,
                CheckpointReadResources {
                    node: &self.options, memory: call.node.memory(),
                    cache: Arc::clone(call.node.cache_budget()), cancellation: call.cancellation,
                },
            ).await
        })
    }

    fn open_world_read<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<Box<dyn OwnedWorldRead>>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let archive = WorldArchive {
                directory: Arc::clone(&self.directory), storage: self.archive.clone(),
                keys: Arc::clone(&self.keys), hosted_task: self.hosted_task.clone(), options: self.options.clone(),
            };
            let held = open_held_checkpoint_world(
                archive, call.session, deployment, world, Arc::clone(call.node.memory()), Arc::clone(call.node.cache_budget()),
            ).await?;
            Ok(Box::new(held) as Box<dyn OwnedWorldRead>)
        })
    }

    fn fork_world_checked<'a>(
        &'a self, call: WorldCall<'a>, capture: Uuid, suffix: Option<Uuid>, until: Timestamp, fork: rtdb_node::OwnedWorldFork,
    ) -> BoxFuture<'a, Result<(Uuid, TransactionKey)>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let resources = CheckpointReadResources {
                node: &self.options, memory: call.node.memory(),
                cache: Arc::clone(call.node.cache_budget()), cancellation: call.cancellation,
            };
            let agent = self.agent.0.get().and_then(std::sync::Weak::upgrade);
            fork_checkpoint_world_selected(Arc::clone(&self.directory), &self.archive,
                self.capture_source.as_ref(), agent.as_deref(), self.keys.as_ref(),
                self.hosted_task.as_ref(), call.session, deployment, capture, suffix, until, resources, fork).await
        })
    }

    fn validate_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid, checks: Vec<OwnedWorldWitness>, controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp) -> BoxFuture<'a, Result<TransactionKey>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            validate_checkpoint_world(self.directory.as_ref(), &self.archive, call.session, deployment, world, checks, controls, deadline).await
        })
    }

    fn commit_world_batch<'a>(
        &'a self, call: WorldCall<'a>, sections: Vec<rtdb_node::OwnedWorldSection>,
        root: rtdb_node::OwnedRootSection, fingerprint: [u8; 32],
        controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
    ) -> BoxFuture<'a, Result<TransactionKey>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            commit_checkpoint_world_batch(self.directory.as_ref(), &self.archive, call.session, deployment,
                sections, root, fingerprint, controls, deadline).await
        })
    }

}

impl OwnedWorldRead for HeldCheckpointWorld {
    fn present(&self) -> Timestamp { HeldCheckpointWorld::present(self).system_time }

    fn application_clock(&self) -> Option<Timestamp> { HeldCheckpointWorld::application_clock(self) }
    fn initial_clock(&self) -> Timestamp { HeldCheckpointWorld::initial_clock(self) }
    fn clock_mode(&self) -> WorldClockMode { HeldCheckpointWorld::clock_mode(self) }

    fn parent_basis(&self) -> TransactionKey { HeldCheckpointWorld::parent_basis(self) }

    fn ranked_search<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize,
        limit: usize, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::ranked_search(self, partitions, query, offset, limit, valid))
    }
    fn range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::range(self, relation, path, request, valid))
    }
    fn count<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::count(self, relation, path, request))
    }
    fn measure<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(OwnedRangeCount, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::measure(self, relation, path, request))
    }
    fn overlay_range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::overlay_range(self, relation, path, request, valid))
    }
    fn overlay_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<OwnedRangePage>> {
        Box::pin(HeldCheckpointWorld::overlay_range_at(self, relation, path, request, valid, system))
    }
    fn parent_pin(&self) -> rtdb_types::Iid { HeldCheckpointWorld::parent_pin(self) }
    fn until(&self) -> Timestamp { HeldCheckpointWorld::until(self) }
    fn evidence_until(&self) -> Timestamp { HeldCheckpointWorld::evidence_until(self) }

    fn check_relation(&self, relation: &OwnedRelation) -> Result<()> {
        HeldCheckpointWorld::check_relation(self, relation)
    }

    fn check_parent_point(&self, relation: &OwnedRelation, until: Option<Timestamp>) -> Result<()> {
        HeldCheckpointWorld::check_parent_point(self, relation, until)
    }

    fn check(&self) -> Result<()> { HeldCheckpointWorld::check(self) }

    fn close(&self) { HeldCheckpointWorld::close(self); }

    fn changes<'a>(
        &'a self, relations: &'a [OwnedRelation], after: Timestamp, through: Timestamp, limit: usize, descending: bool,
    ) -> BoxFuture<'a, Result<rtdb_node::OwnedMergedChanges>> {
        Box::pin(HeldCheckpointWorld::changes(self, relations, after, through, limit, descending))
    }

    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, parent_only: bool,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::valid_during(self, relation, id, valid, parent_only))
    }

    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::identity_page(self, relation, valid, after, limit).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn overlay_identities<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::overlay_identities(self, relation, valid, after, limit).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn overlay_identities_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::overlay_identities_at(self, relation, valid, after, limit, system).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn version<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId,
        from: Timestamp, through: Timestamp, group: Timestamp,
    ) -> BoxFuture<'a, Result<(Timestamp, Vec<(Interval, WorldPointResult)>)>> {
        Box::pin(async move {
            let (clock, slices) = HeldCheckpointWorld::version(self, relation, id, from, through, group).await?;
            Ok((clock, slices.into_iter().map(|(valid, fact, shadowed)| {
                (valid, path_touch_result(group, (Some(fact), shadowed)))
            }).collect()))
        })
    }

    fn path_touch<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>> {
        Box::pin(async move {
            if before.next()? != group { return Err(rtdb_types::OwnedWorld::unavailable()); }
            let [prior, accepted] = HeldCheckpointWorld::path_touch(self, relation, path, id, group, valid, images).await?;
            Ok([path_touch_result(before, prior), path_touch_result(group, accepted)])
        })
    }

    fn select_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<(Timestamp, Timestamp)>>> {
        Box::pin(HeldCheckpointWorld::select_accepted_scope(self, relations, requested))
    }

    fn preflight_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], system: Timestamp,
    ) -> BoxFuture<'a, Result<Timestamp>> {
        Box::pin(HeldCheckpointWorld::preflight_accepted_scope(self, relations, system))
    }

    fn accepted_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
        system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { HeldCheckpointWorld::accepted_point(self, relation, id, valid, system).await.map(|image| path_touch_result(system, image)) })
    }

    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.point(relation, id, valid_at, observed).await.map(point_result) })
    }

    fn read_parent_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.parent_point(relation, id, valid_at, observed).await.map(point_result) })
    }
}

fn path_touch_result(present: Timestamp, (fact, shadowed): (Option<crate::archive::WorldFact>, bool)) -> WorldPointResult {
    let row = fact.map(|fact| {
        let (fields, revision, reservation) = fact.into_parts();
        WorldPointRow { fields, revision, reservation }
    });
    WorldPointResult { present, row, until: None, shadowed, witness: None }
}

fn point_result(read: crate::archive::WorldPoint) -> WorldPointResult {
    let row = read.fact.map(|fact| {
        let (fields, revision, reservation) = fact.into_parts();
        WorldPointRow { fields, revision, reservation }
    });
    WorldPointResult { present: read.present.system_time, row, until: read.until, shadowed: read.shadowed, witness: read.witness }
}
