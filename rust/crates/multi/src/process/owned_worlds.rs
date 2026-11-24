use std::{fmt, sync::Arc};

use futures::future::BoxFuture;
use rtdb_directory::{Directory, HostedTask};
use rtdb_flight_sql::{OwnedWorldHost, OwnedWorldRead, PreparedReplayParent, RetainedReplayWorld, WorldCall, WorldIdentityPage, WorldPointResult};
use rtdb_node::{OwnedChangeEdge, NodeOptions, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, OwnedWorldWitness};
use rtdb_types::{EntityId, Interval, OwnedRelation, OwnedWorld, Result, Timestamp, TransactionKey, WorldClockMode};
use uuid::Uuid;

use crate::{
    archive::{CheckpointReadResources, HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world, commit_checkpoint_world_batch, finish_checkpoint_replay_parents, fork_checkpoint_world_selected, prepare_checkpoint_replay_parent, retain_replayed_checkpoint_world},
    capability::attachment_storage,
    deployment::CellStorage,
    keys::KeyService,
};

mod assertion_families;

pub(super) struct PlacedWorldHost {
    pub(super) directory: Arc<dyn Directory>,
    pub(super) agent: Arc<super::FenceSlot>,
    pub(super) capture_source: Option<CellStorage>,
    pub(super) archive: CellStorage,
    pub(super) keys: Arc<dyn KeyService>,
    pub(super) hosted_task: Option<HostedTask>,
    pub(super) options: NodeOptions,
    pub(super) cell_secret: Option<Arc<crate::capability::CellSecret>>,
    pub(super) family_vault_namespace: [u8; 32],
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

    fn family_custody_secret(&self, record: &rtdb_directory::DeploymentRecord) -> Result<&crate::capability::CellSecret> {
        if record.storage_namespaces.as_ref().map(|namespaces| namespaces.keys) != Some(self.family_vault_namespace) {
            return Err(OwnedWorld::unavailable());
        }
        self.cell_secret.as_deref().ok_or_else(OwnedWorld::unavailable)
    }
}

impl OwnedWorldHost for PlacedWorldHost {
    fn prepare_world_overlay<'a>(
        &'a self, call: WorldCall<'a>, cut: rtdb_node::OwnedForkCut, child: Uuid, retain_until: Timestamp, writable: bool,
    ) -> BoxFuture<'a, Result<rtdb_flight_sql::PreparedWorldOverlay>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let source = self.capture_source.as_ref().ok_or_else(OwnedWorld::unavailable)?;
            let agent = self.agent.agent.get().and_then(std::sync::Weak::upgrade)
                .ok_or_else(OwnedWorld::unavailable)?;
            crate::archive::prepare_checkpoint_world_overlay(
                Arc::clone(&self.directory), &self.archive, source, &agent, self.keys.as_ref(),
                self.hosted_task.as_ref(), call.session, deployment, cut, child, retain_until, writable,
                CheckpointReadResources {
                    node: &self.options, memory: call.node.memory(),
                    cache: Arc::clone(call.node.cache_budget()), cancellation: call.cancellation,
                },
            ).await
        })
    }

    fn prepare_replay_parent<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], original: Option<TransactionKey>,
    ) -> BoxFuture<'a, Result<Option<PreparedReplayParent>>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let source = self.capture_source.as_ref().ok_or_else(OwnedWorld::unavailable)?;
            let agent = self.agent.agent.get().and_then(std::sync::Weak::upgrade)
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
            let agent = self.agent.agent.get().and_then(std::sync::Weak::upgrade)
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
            let agent = self.agent.agent.get().and_then(std::sync::Weak::upgrade);
            fork_checkpoint_world_selected(Arc::clone(&self.directory), &self.archive,
                self.capture_source.as_ref(), agent.as_deref(), self.keys.as_ref(),
                self.hosted_task.as_ref(), call.session, deployment, capture, suffix, until, resources, fork).await
        })
    }

    fn commit_world_batch<'a>(
        &'a self, call: WorldCall<'a>, sections: Vec<rtdb_node::OwnedWorldSection>,
        root: rtdb_node::OwnedRootSection, observed: rtdb_node::OwnedWorldObservations, fingerprint: [u8; 32],
        controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
    ) -> BoxFuture<'a, Result<TransactionKey>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            commit_checkpoint_world_batch(self.directory.as_ref(), &self.archive, call.session, deployment,
                sections, root, observed, fingerprint, controls, deadline).await
        })
    }

    fn root_rank_authority(&self, call: WorldCall<'_>) -> Result<Arc<dyn rtdb_storage::sealing::RootRankAuthority>> {
        let deployment = self.deployment(&call)?;
        assertion_families::root_rank_authority(
            call.session, deployment, Arc::clone(&self.directory), Arc::clone(&self.keys), call.cancellation.clone(),
        )
    }

    fn assertion_family<'a>(
        &'a self, call: WorldCall<'a>, request: rtdb_flight_sql::FamilyRequest,
    ) -> BoxFuture<'a, Result<rtdb_flight_sql::FamilyReply>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let record = self.directory.deployment(&deployment.to_string()).await?
                .ok_or_else(OwnedWorld::unavailable)?;
            if let rtdb_flight_sql::FamilyRequest::Retired(request) = request {
                let secret = self.family_custody_secret(&record)?;
                return assertion_families::serve_retired(Arc::clone(&self.keys), secret, call.session, &record, request).await;
            }
            assertion_families::serve(self.keys.as_ref(), call.session, deployment, &record, request).await
        })
    }

    fn retired_family_fence<'a>(
        &'a self, call: WorldCall<'a>, request: &'a rtdb_flight_sql::RetiredFamilyRequest,
    ) -> BoxFuture<'a, Result<bytes::Bytes>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let record = self.directory.deployment(&deployment.to_string()).await?.ok_or_else(OwnedWorld::unavailable)?;
            let secret = self.family_custody_secret(&record)?;
            assertion_families::authorize_retired_fence(secret, call.session, &record, request).await
        })
    }
}

impl OwnedWorldRead for HeldCheckpointWorld {
    fn retained_child_cut<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, publication: TransactionKey,
    ) -> BoxFuture<'a, Result<rtdb_node::OwnedForkCut>> {
        Box::pin(HeldCheckpointWorld::retained_child_cut(self, scope, publication))
    }

    fn present(&self) -> Timestamp { HeldCheckpointWorld::present(self).system_time }
    fn ancestors(&self) -> Vec<(rtdb_types::Iid, Timestamp)> { HeldCheckpointWorld::ancestors(self) }
    fn ancestor(&self) -> Option<(rtdb_types::Iid, Timestamp)> { HeldCheckpointWorld::ancestor(self) }

    fn application_publication(&self) -> Option<(Timestamp, Timestamp)> {
        HeldCheckpointWorld::application_publication(self)
    }
    fn initial_clock(&self) -> Timestamp { HeldCheckpointWorld::initial_clock(self) }
    fn clock_mode(&self) -> WorldClockMode { HeldCheckpointWorld::clock_mode(self) }

    fn parent_basis(&self) -> TransactionKey { HeldCheckpointWorld::parent_basis(self) }

    fn ranked_search<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize,
        limit: usize, valid: Timestamp, staged: Option<rtdb_node::TextShadows<'a>>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::ranked_search(self, partitions, query, offset, limit, valid, staged))
    }
    fn ranked_search_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::ranked_search_at(self, partitions, query, offset, limit, valid, system, parent_only))
    }
    fn nearest<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, staged: Option<rtdb_node::nearest::HeldVectorOverlay>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::nearest(self, partitions, query, valid, staged))
    }
    fn nearest_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::nearest_at(self, partitions, query, valid, system, parent_only))
    }
    fn nearest_parent_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::nearest_parent_at(self, partitions, query, valid, system))
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
    fn shared_column_census<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, valid: Timestamp, root_prefix: &'a str, world_prefix: &'a str,
    ) -> BoxFuture<'a, Result<(WorldIdentityPage, OwnedWorldWitness)>> {
        Box::pin(async move {
            let (page, witness) = HeldCheckpointWorld::shared_column_census(self, scope, valid, root_prefix, world_prefix).await?;
            Ok((WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations }, witness))
        })
    }
    fn parent_identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::parent_identity_page_at(self, relation, valid, after, limit, system).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }
    fn parent_schedule_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::parent_schedule_at(self, relation, id, valid, system))
    }
    fn ranked_search_parent_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::ranked_search_parent_at(self, partitions, query, offset, limit, valid, system))
    }
    fn parent_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::parent_range_at(self, relation, path, request, valid, system))
    }
    fn parent_count_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>, system: Timestamp,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::parent_count_at(self, relation, path, request, system))
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
        &'a self, relations: &'a [OwnedRelation], after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, descending: bool,
    ) -> BoxFuture<'a, Result<rtdb_node::OwnedMergedChanges>> {
        Box::pin(HeldCheckpointWorld::changes(self, relations, after, through, limit, descending))
    }

    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, parent_only: bool,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)>> {
        Box::pin(HeldCheckpointWorld::valid_during(self, relation, id, valid, parent_only))
    }

    fn valid_during_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<Vec<(Interval, Timestamp)>>> {
        Box::pin(HeldCheckpointWorld::valid_during_at(self, relation, id, valid, system))
    }

    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::identity_page(self, relation, valid, after, limit).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
        system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::identity_page_at(self, relation, valid, after, limit, system, parent_only).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn overlay_identities<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::overlay_identities(self, relation, valid, after, limit).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn overlay_identities_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::overlay_identities_at(self, relation, valid, after, limit, system).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn parent_touch_images_at<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>> {
        Box::pin(async move {
            let [prior, accepted] = HeldCheckpointWorld::parent_touch_images_at(
                self, relation, path, id, selected, before, group, valid, images,
            ).await?;
            Ok([path_touch_result(before, (prior, false)), path_touch_result(group, (accepted, false))])
        })
    }

    fn parent_changes_at<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::parent_changes_at(self, relation, selected, after, through, limit, reverse))
    }

    fn ancestor_changes<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::ancestor_changes(self, relation, selected, after, through, limit, reverse))
    }

    fn parent_versions_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, selected: Timestamp,
        request: rtdb_node::OwnedVersionRequest,
    ) -> BoxFuture<'a, Result<(rtdb_flight_sql::WorldParentVersions, OwnedWorldWitness)>> {
        Box::pin(HeldCheckpointWorld::parent_versions_at(self, relation, id, selected, request))
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
    ) -> BoxFuture<'a, Result<Option<(TransactionKey, Timestamp)>>> {
        Box::pin(HeldCheckpointWorld::select_accepted_scope(self, relations, requested))
    }

    fn select_parent_as_of<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<Timestamp>>> {
        Box::pin(HeldCheckpointWorld::select_parent_as_of(self, relations, requested))
    }

    fn parent_point_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp, system: Timestamp,
        original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move {
            let fact = HeldCheckpointWorld::parent_point_at(self, relation, id, valid, system, original_scope).await?;
            Ok(path_touch_result(system, (fact, false)))
        })
    }

    fn preflight_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], system: Timestamp,
    ) -> BoxFuture<'a, Result<(TransactionKey, Timestamp)>> {
        Box::pin(HeldCheckpointWorld::preflight_accepted_scope(self, relations, system))
    }

    fn accepted_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
        system: Timestamp, original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move {
            let (fact, shadowed, frozen_winner) = HeldCheckpointWorld::accepted_point(self, relation, id, valid, system, original_scope).await?;
            Ok(WorldPointResult { frozen_winner, ..path_touch_result(system, (fact, shadowed)) })
        })
    }

    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.point(relation, id, valid_at, observed).await.map(point_result) })
    }

    fn read_ancestor_root_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.ancestor_root_point(relation, id, valid).await.map(point_result) })
    }

    fn read_parent_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.parent_point(relation, id, valid_at, observed).await.map(point_result) })
    }
}

fn path_touch_result(present: Timestamp, (fact, shadowed): (Option<crate::archive::WorldFact>, bool)) -> WorldPointResult {
    let row = fact.map(crate::archive::WorldFact::into_point_row);
    WorldPointResult { present, row, until: None, shadowed, witness: None, frozen_winner: None }
}

fn point_result(read: crate::archive::WorldPoint) -> WorldPointResult {
    let row = read.fact.map(crate::archive::WorldFact::into_point_row);
    WorldPointResult { present: read.present.system_time, row, until: read.until, shadowed: read.shadowed, witness: read.witness, frozen_winner: None }
}
