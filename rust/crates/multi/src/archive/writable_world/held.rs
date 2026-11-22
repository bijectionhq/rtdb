use std::time::Duration;

use rtdb_node::NodeOptions;
use rtdb_storage::cache::CacheBudget;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::archive::read_world::{read_checkpoint_point_in_context, with_checkpoint_context};

#[derive(Clone)]
pub(crate) struct WorldArchive {
    pub directory: Arc<dyn Directory>,
    pub storage: CellStorage,
    pub keys: Arc<dyn KeyService>,
    pub hosted_task: Option<HostedTask>,
    pub options: NodeOptions,
}

pub(crate) struct HeldCheckpointWorld {
    open: OpenWorld,
    memory: Arc<MemoryBudget>,
    requests: mpsc::Sender<ArchivePoint>,
    cancellation: CancellationToken,
}

struct ArchivePoint {
    relation: OwnedRelation,
    id: EntityId,
    valid: Interval,
    reply: oneshot::Sender<Result<Option<WorldFact>>>,
}

impl HeldCheckpointWorld {
    pub fn present(&self) -> TransactionKey { self.open.snapshot.present() }

    pub fn check(&self) -> Result<()> {
        self.open.check()?;
        if self.cancellation.is_cancelled() || self.requests.is_closed() {
            return Err(OwnedWorld::unavailable());
        }
        Ok(())
    }

    pub fn close(&self) {
        self.cancellation.cancel();
        self.open.snapshot.close();
    }

    pub async fn point(&self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp) -> Result<WorldPoint> {
        self.check()?;
        let valid = Interval::at(valid_at)?;
        let fact = if let Some(fact) = self.open.read_local(relation, id, valid_at, &self.memory).await? {
            fact
        } else {
            let (reply, result) = oneshot::channel();
            self.cancellation.run_until_cancelled(self.requests.send(ArchivePoint {
                relation: relation.clone(), id: id.clone(), valid, reply,
            })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
            self.cancellation.run_until_cancelled(result).await
                .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??
        };
        self.check()?;
        Ok(WorldPoint { present: self.present(), fact })
    }
}

impl Drop for HeldCheckpointWorld {
    fn drop(&mut self) { self.close(); }
}

pub(crate) async fn open_held_checkpoint_world(
    archive: WorldArchive, session: &mut NativeSession, deployment: Uuid, world: Uuid,
    memory: Arc<MemoryBudget>, cache: Arc<CacheBudget>,
) -> Result<HeldCheckpointWorld> {
    let open = open_world(archive.directory.as_ref(), &archive.storage, session, deployment, world).await?;
    let parent = open.parent(deployment);
    let owner = open.retained.owner.clone();
    let remaining = open.retained.until.0.saturating_sub(now().0).max(0) as u64;
    let lifetime = Duration::from_micros(remaining).min(Duration::from_secs(60));
    let deadline = tokio::time::Instant::now() + lifetime;
    let cancellation = session.state().cancellation().child_token();
    // On RPC cancellation the worker still completes the storage-region drain.
    let opening = cancellation.clone().drop_guard();
    let worker = cancellation.clone();
    let backing_memory = Arc::clone(&memory);
    let (requests, mut pending) = mpsc::channel::<ArchivePoint>(1);
    let (ready, opened) = oneshot::channel();
    tokio::spawn(async move {
        let timer = worker.clone();
        let expire = async {
            if timer.run_until_cancelled(tokio::time::sleep_until(deadline)).await.is_some() {
                timer.cancel();
            }
        };
        let serve = async {
            let mut ready = Some(ready);
            let outcome = with_checkpoint_context(
                archive.directory.as_ref(), &archive.storage, archive.keys.as_ref(), archive.hosted_task.as_ref(),
                &parent,
                CheckpointReadResources { node: &archive.options, memory: &backing_memory, cache, cancellation: &worker },
                |context| {
                    let opened = ready.take().expect("one archive open");
                    let scope = &worker;
                    let memory = &backing_memory;
                    async move {
                        if scope.is_cancelled() || tokio::time::Instant::now() >= deadline
                            || opened.send(Ok(())).is_err()
                        {
                            return Ok(());
                        }
                        loop {
                            let ArchivePoint { relation, id, valid, mut reply } = tokio::select! {
                                _ = scope.cancelled() => break,
                                point = pending.recv() => match point { Some(point) => point, None => break },
                            };
                            let read = async {
                                let batches = read_checkpoint_point_in_context(Arc::clone(&context), CheckpointPoint {
                                    owner: &owner, relation: &relation, id: &id, valid: Some(valid),
                                }).await?;
                                decode_point(batches.unwrap_or_default(), &id, memory)
                            };
                            let response = tokio::select! {
                                _ = scope.cancelled() => break,
                                _ = reply.closed() => continue,
                                result = read => result,
                            };
                            let _ = reply.send(response);
                        }
                        Ok(())
                    }
                },
            ).await;
            if let Some(ready) = ready {
                let _ = ready.send(outcome);
            }
            worker.cancel();
        };
        tokio::join!(expire, serve);
    });
    opened.await.map_err(|_| OwnedWorld::unavailable())??;
    let held = HeldCheckpointWorld { open, memory, requests, cancellation };
    held.check()?;
    let _ = opening.disarm();
    Ok(held)
}
