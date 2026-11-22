use std::{fmt, sync::Arc};

use futures::future::BoxFuture;
use rtdb_directory::{Directory, HostedTask};
use rtdb_flight_sql::{OwnedWorldHost, OwnedWorldRead, WorldCall, WorldIdentityPage, WorldPoint, WorldPointResult, WorldPointRow};
use rtdb_node::{NodeOptions, OwnedWorldPointWitness, OwnedWorldWrite};
use rtdb_types::{EntityId, Interval, OwnedRelation, OwnedWorld, Result, Timestamp, TransactionKey};
use uuid::Uuid;

use crate::{
    archive::{CheckpointReadResources, HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world, commit_checkpoint_world_checked, drop_checkpoint_world, fork_checkpoint_world_at, read_checkpoint_world_point},
    capability::attachment_storage,
    deployment::CellStorage,
    keys::KeyService,
};

pub(super) struct PlacedWorldHost {
    pub(super) directory: Arc<dyn Directory>,
    pub(super) storage: CellStorage,
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
    fn read_point<'a>(&'a self, call: WorldCall<'a>, point: WorldPoint<'a>) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let resources = CheckpointReadResources {
                node: &self.options,
                memory: call.node.memory(),
                cache: Arc::clone(call.node.cache_budget()),
                cancellation: call.cancellation,
            };
            let read = read_checkpoint_world_point(
                self.directory.as_ref(), &self.storage, self.keys.as_ref(), self.hosted_task.as_ref(),
                call.session, deployment, point.world, point.relation, point.id, point.valid_at, resources,
            ).await?;
            Ok(point_result(read))
        })
    }

    fn open_world_read<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<Box<dyn OwnedWorldRead>>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let archive = WorldArchive {
                directory: Arc::clone(&self.directory), storage: self.storage.clone(),
                keys: Arc::clone(&self.keys), hosted_task: self.hosted_task.clone(), options: self.options.clone(),
            };
            let held = open_held_checkpoint_world(
                archive, call.session, deployment, world, Arc::clone(call.node.memory()), Arc::clone(call.node.cache_budget()),
            ).await?;
            Ok(Box::new(held) as Box<dyn OwnedWorldRead>)
        })
    }

    fn fork_world<'a>(&'a self, call: WorldCall<'a>, capture: Uuid, suffix: Option<Uuid>, until: Timestamp) -> BoxFuture<'a, Result<Uuid>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            let resources = CheckpointReadResources {
                node: &self.options,
                memory: call.node.memory(),
                cache: Arc::clone(call.node.cache_budget()),
                cancellation: call.cancellation,
            };
            fork_checkpoint_world_at(self.directory.as_ref(), &self.storage, self.keys.as_ref(), self.hosted_task.as_ref(),
                call.session, deployment, capture, suffix, until, resources).await
        })
    }

    fn commit_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid, writes: Vec<OwnedWorldWrite>, checks: Vec<OwnedWorldPointWitness>) -> BoxFuture<'a, Result<TransactionKey>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            commit_checkpoint_world_checked(self.directory.as_ref(), &self.storage, call.session, deployment, world, writes, checks).await
        })
    }

    fn drop_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            drop_checkpoint_world(self.directory.as_ref(), call.session, deployment, world).await
        })
    }
}

impl OwnedWorldRead for HeldCheckpointWorld {
    fn present(&self) -> Timestamp { HeldCheckpointWorld::present(self).system_time }

    fn parent_basis(&self) -> TransactionKey { HeldCheckpointWorld::parent_basis(self) }

    fn check_relation(&self, relation: &OwnedRelation) -> Result<()> {
        HeldCheckpointWorld::check_relation(self, relation)
    }

    fn check(&self) -> Result<()> { HeldCheckpointWorld::check(self) }

    fn close(&self) { HeldCheckpointWorld::close(self); }

    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval,
    ) -> BoxFuture<'a, Result<Vec<(Interval, Timestamp)>>> {
        Box::pin(HeldCheckpointWorld::valid_during(self, relation, id, valid))
    }

    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>> {
        Box::pin(async move {
            let page = HeldCheckpointWorld::identity_page(self, relation, valid, after, limit).await?;
            Ok(WorldIdentityPage { keys: page.keys, more: page.more, reservations: page.reservations })
        })
    }

    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>> {
        Box::pin(async move { self.point(relation, id, valid_at, observed).await.map(point_result) })
    }
}

fn point_result(read: crate::archive::WorldPoint) -> WorldPointResult {
    let row = read.fact.map(|fact| {
        let (fields, revision, reservation) = fact.into_parts();
        WorldPointRow { fields, revision, reservation }
    });
    WorldPointResult { present: read.present.system_time, row, shadowed: read.shadowed, witness: read.witness }
}
