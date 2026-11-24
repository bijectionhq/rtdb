use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{Error as ProviderError, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, Result};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{IoRegion, ObjectKey, io_region::IoAdmission, local_catalog::{CatalogHold, covers, same_storage, well_formed}};

const MAX_LEDGER_BYTES: usize = 1024 * 1024;
type CatalogHolds = BTreeMap<Uuid, CatalogHold>;

#[derive(Default)]
struct Ledger {
    sequence: u64,
    floor: Option<CatalogCut>,
    holds: CatalogHolds,
}

pub(super) struct CatalogLedger {
    client: Arc<dyn ProviderStore>,
    key: ObjectKey,
    path: Path,
    reads: Arc<IoRegion>,
}

impl CatalogLedger {
    pub fn new(client: Arc<dyn ProviderStore>, key: ObjectKey, path: Path) -> Arc<Self> {
        Arc::new(Self { client, key, path, reads: IoRegion::new() })
    }

    pub async fn retain(self: &Arc<Self>, cut: Option<CatalogCut>, paths: Option<Arc<[String]>>, io: &IoRegion)
        -> Result<Arc<dyn Send + Sync>> {
        let claim = CatalogHold::new(cut, paths.as_deref());
        let admission: IoAdmission = io.admit_read(self.clone(), self.key.clone())?;
        let ledger = Arc::clone(self);
        let (answer, result) = oneshot::channel();
        let (permit, drained) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let ticket = Uuid::new_v4();
            let mut uncertain = false;
            let registered = ledger.update(|state| {
                if cut.is_some_and(|cut| !well_formed(cut))
                    || state.floor.is_some_and(|floor| !cut.is_some_and(|cut| covers(cut, floor)))
                {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-current",
                        "The captured catalog precedes the remote collection floor or belongs to another journal"));
                }
                state.holds.insert(ticket, claim.clone());
                uncertain = true;
                Ok((true, ()))
            }).await;
            let opened = registered.is_ok();
            let delivered = answer.send(registered.map(|()| Arc::new(permit) as Arc<dyn Send + Sync>)).is_ok();
            drop(admission);
            if opened && delivered { let _ = drained.await; }
            if opened || uncertain {
                // Even an absent ticket needs a CAS after a lost registration reply.
                while ledger.update(|state| Ok((state.holds.remove(&ticket).is_some() || !opened, ()))).await.is_err() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
        result.await.map_err(|_| unavailable("Remote catalog admission ended"))?
    }

    pub async fn advance(self: &Arc<Self>, cut: CatalogCut) -> Result<bool> {
        Ok(self.close_admission(cut, &[None]).await?[0])
    }

    pub async fn close_admission(self: &Arc<Self>, cut: CatalogCut, paths: &[Option<&str>]) -> Result<Vec<bool>> {
        if !well_formed(cut) || cut.through.after_offset(cut.replica_epoch) < 0 {
            return Err(unavailable("Invalid remote catalog floor"));
        }
        self.update(|state| {
            let closed = state.floor.is_some_and(|prior| covers(prior, cut));
            if !closed && state.floor.is_some_and(|prior| {
                !covers(cut, prior) && !(same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch)
            }) {
                return Err(unavailable("The remote catalog floor belongs to another storage root or a newer journal"));
            }
            let advance = !closed && (paths.iter().any(Option::is_some)
                || state.holds.values().all(|held| !held.retains(cut, None)));
            if advance {
                state.floor = Some(cut);
            }
            let allowed = paths.iter().map(|path| {
                (closed || advance) && state.holds.values().all(|held| !held.retains(cut, *path))
            }).collect();
            Ok((advance, allowed))
        }).await
    }

    async fn read(self: &Arc<Self>) -> Result<(Ledger, PutMode)> {
        self.reads.read_async(self.clone(), self.key.clone(), |ledger, key| async move {
            Ok(ledger.read_ledger(&key).await)
        }).await?
    }

    async fn read_ledger(&self, key: &ObjectKey) -> Result<(Ledger, PutMode)> {
        let response = match self.client.get(&self.path).await {
            Err(ProviderError::NotFound { .. }) => return Ok((Ledger::default(), PutMode::Create)),
            result => result.map_err(|error| super::provider_error(key, error))?,
        };
        if response.meta.size > MAX_LEDGER_BYTES as u64 {
            return Err(unavailable("The remote catalog ledger exceeds its size limit"));
        }
        let version = UpdateVersion { e_tag: response.meta.e_tag.clone(), version: response.meta.version.clone() };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(unavailable("The remote catalog ledger has no conditional update identity"));
        }
        let bytes = response.bytes().await.map_err(|error| super::provider_error(key, error))?;
        let (sequence, floor, holds): (u64, Option<CatalogCut>, CatalogHolds) =
            serde_json::from_slice(&bytes).map_err(unavailable)?;
        if sequence == 0 || floor.is_some_and(|floor| !well_formed(floor))
            || holds.values().any(|held| held.cut.is_some_and(|cut| !well_formed(cut)))
        {
            return Err(unavailable("Malformed remote catalog ledger"));
        }
        Ok((Ledger { sequence, floor, holds }, PutMode::Update(version)))
    }

    async fn update<T>(self: &Arc<Self>, mut change: impl FnMut(&mut Ledger) -> Result<(bool, T)>) -> Result<T> {
        for _ in 0..8 {
            let (mut state, mode) = self.read().await?;
            let (changed, answer) = change(&mut state)?;
            if !changed { return Ok(answer); }
            let sequence = state.sequence.checked_add(1).ok_or_else(|| unavailable("Remote catalog ledger sequence exhausted"))?;
            let bytes = serde_json::to_vec(&(sequence, state.floor, state.holds)).map_err(unavailable)?;
            if bytes.len() > MAX_LEDGER_BYTES { return Err(unavailable("The remote catalog ledger has no remaining hold capacity")); }
            let options = PutOptions { mode, ..Default::default() };
            match self.client.put_opts(&self.path, Bytes::from(bytes).into(), options).await {
                Ok(_) => return Ok(answer),
                Err(ProviderError::Precondition { .. } | ProviderError::AlreadyExists { .. }) => continue,
                Err(error) => return Err(super::provider_error(&self.key, error).into()),
            }
        }
        Err(unavailable("The remote catalog ledger kept changing"))
    }
}

fn unavailable(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/remote-catalog-ledger", message.to_string())
}
