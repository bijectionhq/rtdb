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
    requests: mpsc::Sender<ArchiveRead>,
    cancellation: CancellationToken,
}

struct ArchivePoint {
    relation: OwnedRelation,
    id: EntityId,
    valid: Interval,
    reply: oneshot::Sender<Result<Option<WorldFact>>>,
}

enum ArchiveRead {
    Point(ArchivePoint),
    Identities {
        relation: OwnedRelation,
        at: Timestamp,
        after: Option<String>,
        fetch: usize,
        reply: oneshot::Sender<Result<rtdb_query::OwnedIdentityCandidates>>,
    },
}

pub(crate) struct WorldIdentityPage {
    pub keys: Vec<String>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
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
        let (local, witness) = self.open.read_local(relation, id, valid_at, &self.memory, true).await?;
        let fact = if let Some(fact) = local {
            fact
        } else {
            self.open.snapshot.check_point_purge_floor(relation, id, self.open.retained.basis.system_time).await?;
            let (reply, result) = oneshot::channel();
            self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Point(ArchivePoint {
                relation: relation.clone(), id: id.clone(), valid, reply,
            }))).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
            self.cancellation.run_until_cancelled(result).await
                .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??
        };
        self.check()?;
        Ok(WorldPoint { present: self.present(), fact, witness })
    }
    pub async fn identity_page(
        &self, relation: &OwnedRelation, valid: Timestamp, after: Option<&str>, limit: usize,
    ) -> Result<WorldIdentityPage> {
        self.check()?;
        Interval::at(valid)?;
        if !(1..=128).contains(&limit) { return Err(OwnedWorld::unavailable()); }
        const FETCH: usize = 129;
        let mut cursor = after.map(|key| (key.to_owned(), None));
        let mut keys = Vec::with_capacity(limit);
        let mut bytes = limit * 128;
        let mut more = false;
        let mut complete = false;
        let mut reservations = vec![self.memory.reserve(bytes)?];
        for _ in 0..32 {
            let local = self.open.snapshot.world_identity_candidates(
                self.open.record.id, relation, valid, cursor.as_ref().map(|(key, _)| key.as_str()), FETCH,
            ).await?;
            let (reply, result) = oneshot::channel();
            self.cancellation.run_until_cancelled(self.requests.send(ArchiveRead::Identities {
                relation: relation.clone(), at: valid, after: cursor.as_ref().map(|(key, _)| key.clone()), fetch: FETCH, reply,
            })).await.ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())?;
            let parent = self.cancellation.run_until_cancelled(result).await
                .ok_or_else(OwnedWorld::unavailable)?.map_err(|_| OwnedWorld::unavailable())??;

            // A full input page can have unseen keys beyond its last key.
            let through = [
                (local.keys.len() == FETCH).then(|| local.keys.last()).flatten(),
                (parent.keys.len() == FETCH).then(|| parent.keys.last()).flatten(),
            ].into_iter().flatten().min();
            let candidates: std::collections::BTreeSet<_> = parent.keys.iter().chain(&local.keys).collect();
            for key in candidates.into_iter().take_while(|key| through.is_none_or(|through| *key <= through)) {
                let id = EntityId::String(key.clone());
                let (shadow, _) = self.open.read_local(relation, &id, valid, &self.memory, false).await?;
                let visible = match shadow {
                    Some(fact) => fact.is_some(),
                    None => {
                        if local.keys.binary_search(key).is_ok() || parent.keys.binary_search(key).is_err() {
                            return Err(OwnedWorld::unavailable());
                        }
                        self.open.snapshot.check_point_purge_floor(relation, &id, self.open.retained.basis.system_time).await?;
                        true
                    }
                };
                if visible {
                    if keys.len() == limit {
                        more = true;
                        break;
                    }
                    bytes = bytes.checked_add(key.len()).filter(|size| *size <= (8 << 20)).ok_or_else(OwnedWorld::unavailable)?;
                    reservations.push(self.memory.reserve(key.len())?);
                    keys.push(key.clone());
                }
            }
            if more || through.is_none() {
                complete = true;
                break;
            }
            if let Some(through) = through {
                let charge = self.memory.reserve(through.len())?;
                cursor = Some((through.clone(), Some(charge)));
            }
        }
        if !complete { return Err(OwnedWorld::unavailable()); }
        self.check()?;
        Ok(WorldIdentityPage { keys, more, reservations })
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
    let parent_time = parent.basis.system_time;
    let remaining = open.retained.until.0.saturating_sub(now().0).max(0) as u64;
    let lifetime = Duration::from_micros(remaining).min(Duration::from_secs(60));
    let deadline = tokio::time::Instant::now() + lifetime;
    let cancellation = session.state().cancellation().child_token();
    // On RPC cancellation the worker still completes the storage-region drain.
    let opening = cancellation.clone().drop_guard();
    let worker = cancellation.clone();
    let backing_memory = Arc::clone(&memory);
    let (requests, mut pending) = mpsc::channel::<ArchiveRead>(1);
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
                            let request = tokio::select! {
                                _ = scope.cancelled() => break,
                                point = pending.recv() => match point { Some(point) => point, None => break },
                            };
                            match request {
                                ArchiveRead::Point(ArchivePoint { relation, id, valid, mut reply }) => {
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
                                ArchiveRead::Identities { relation, at, after, fetch, mut reply } => {
                                    let response = tokio::select! {
                                        _ = scope.cancelled() => break,
                                        _ = reply.closed() => continue,
                                        result = rtdb_query::owned_identity_candidates(
                                            Arc::clone(&context), &owner, &relation, parent_time,
                                            at, after.as_deref(), fetch,
                                        ) => result,
                                    };
                                    let _ = reply.send(response);
                                }
                            }
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
