use std::{fmt, sync::Arc};

use futures::future::BoxFuture;
use rtdb_directory::{Directory, HostedTask};
use rtdb_flight_sql::{OwnedWorldHost, WorldCall, WorldPoint, WorldPointResult, WorldPointRow};
use rtdb_node::NodeOptions;
use rtdb_types::{OwnedWorld, Result};
use uuid::Uuid;

use crate::{
    archive::{CheckpointReadResources, drop_checkpoint_world, read_checkpoint_world_point},
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
            let row = read.fact.map(|fact| {
                let (fields, revision, reservation) = fact.into_parts();
                WorldPointRow { fields, revision, reservation }
            });
            Ok(WorldPointResult { present: read.present.system_time, row })
        })
    }

    fn drop_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let deployment = self.deployment(&call)?;
            drop_checkpoint_world(self.directory.as_ref(), call.session, deployment, world).await
        })
    }
}
