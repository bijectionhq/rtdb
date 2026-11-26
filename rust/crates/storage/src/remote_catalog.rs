use std::{collections::BTreeMap, path::Path as VaultPath, sync::Arc};

use bytes::Bytes;
use object_store::{Error as ProviderError, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, Result};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{IoRegion, ObjectKey, classic_catalog_drain::{CatalogReadWitness, ClassicCatalogDrain, selected_vault},
    io_region::IoAdmission, local_catalog::{CatalogHold, covers, same_storage, well_formed},
    release::{Backoff, Settled, settle}};

const MAX_LEDGER_BYTES: usize = 1024 * 1024;
type CatalogHolds = BTreeMap<Uuid, HeldCatalog>;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HeldCatalog {
    catalog: CatalogHold,
    owner: Option<ClassicCatalogDrain>,
}

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
        let catalog = CatalogHold::new(cut, paths.as_deref());
        let admission: IoAdmission = io.admit_read(self.clone(), self.key.clone())?;
        let vault = selected_vault();
        let ledger = Arc::clone(self);
        let (answer, result) = oneshot::channel();
        let (permit, drained) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let ticket = Uuid::new_v4();
            let witness = match vault {
                Some(vault) => match ledger.reads.run(move || CatalogReadWitness::reserve(&vault, ticket)).await {
                    Ok(witness) => witness.map(Arc::new),
                    Err(error) => { let _ = answer.send(Err(error.into())); return; }
                },
                None => None,
            };
            let claim = HeldCatalog { catalog, owner: witness.as_ref().map(|witness| witness.identity) };
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
            let delivered = answer.send(registered.map(|()| {
                Arc::new((permit, witness.clone())) as Arc<dyn Send + Sync>
            })).is_ok();
            drop(admission);
            if opened && delivered { let _ = drained.await; }
            if !(opened || uncertain) {
                if let Some(witness) = witness {
                    let _ = ledger.reads.run(move || witness.forget()).await;
                }
                return;
            }
            let ledger = &ledger;
            // Even an absent ticket needs a CAS after a lost registration reply.
            let release = move || async move {
                ledger.update(move |state| Ok((state.holds.remove(&ticket).is_some() || !opened, ()))).await
            };
            match witness {
                // The witness stays in the vault, unlocked, until the ledger no
                // longer names the ticket: `drained_holds` clears it otherwise.
                Some(witness) => match release().await {
                    Ok(()) => { let _ = ledger.reads.run(move || witness.forget()).await; }
                    Err(error) => eprintln!("Remote catalog hold {ticket} is left to drained-hold reconciliation: {}", error.code),
                },
                None => {
                    let settled = settle(Backoff::RELEASE, release, |attempt, error, delay| eprintln!(
                        "Remote catalog hold {ticket} release attempt {attempt} failed, retrying in {delay:?}: {}", error.code)).await;
                    if let Settled::Refused(error) = settled {
                        eprintln!("Remote catalog hold {ticket} cannot be released: {}", error.code);
                    }
                }
            }
        });
        result.await.map_err(|_| unavailable("Remote catalog admission ended"))?
    }

    /// Admits another reader of a catalog whose ticket this process holds, as `retain` would admit it, without registering.
    pub async fn readmit(self: &Arc<Self>, cut: Option<CatalogCut>) -> Result<()> {
        let (state, _) = self.read().await?;
        if cut.is_some_and(|cut| !well_formed(cut)) || state.floor.is_some_and(|floor| !cut.is_some_and(|cut| covers(cut, floor))) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-current",
                "The captured catalog precedes the remote collection floor or belongs to another journal"));
        }
        Ok(())
    }

    pub async fn advance(self: &Arc<Self>, cut: CatalogCut, vault: Option<Arc<VaultPath>>) -> Result<bool> {
        Ok(self.close_admission(cut, &[None], vault).await?[0])
    }

    pub async fn close_admission(self: &Arc<Self>, cut: CatalogCut, paths: &[Option<&str>], vault: Option<Arc<VaultPath>>) -> Result<Vec<bool>> {
        if !well_formed(cut) || cut.through.after_offset(cut.replica_epoch) < 0 {
            return Err(unavailable("Invalid remote catalog floor"));
        }
        let drained = self.drained_holds(vault).await?;
        self.update(|state| {
            let removed = clear_drained(&mut state.holds, &drained);
            let closed = state.floor.is_some_and(|prior| covers(prior, cut));
            if !closed && state.floor.is_some_and(|prior| {
                !covers(cut, prior) && !(same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch)
            }) {
                return Err(unavailable("The remote catalog floor belongs to another storage root or a newer journal"));
            }
            let advance = !closed && (paths.iter().any(Option::is_some)
                || state.holds.values().all(|held| !held.catalog.retains(cut, None)));
            if advance {
                state.floor = Some(cut);
            }
            let allowed = paths.iter().map(|path| {
                (closed || advance) && state.holds.values().all(|held| !held.catalog.retains(cut, *path))
            }).collect();
            Ok((advance || removed, allowed))
        }).await
    }

    async fn drained_holds(self: &Arc<Self>, vault: Option<Arc<VaultPath>>) -> Result<Vec<(Uuid, ClassicCatalogDrain)>> {
        let Some(vault) = vault else { return Ok(Vec::new()); };
        let (state, _) = self.read().await?;
        let owners = state.holds.into_iter().filter_map(|(ticket, held)| held.owner.map(|witness| (ticket, witness)));
        Ok(self.reads.run(move || {
            let mut drained = Vec::new();
            for (ticket, witness) in owners {
                if CatalogReadWitness::drained(&vault, ticket, witness)? { drained.push((ticket, witness)); }
            }
            Ok(drained)
        }).await?)
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
            || holds.values().any(|held| held.catalog.cut.is_some_and(|cut| !well_formed(cut)))
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

fn clear_drained(holds: &mut CatalogHolds, drained: &[(Uuid, ClassicCatalogDrain)]) -> bool {
    let before = holds.len();
    for &(ticket, witness) in drained {
        if holds.get(&ticket).is_some_and(|hold| hold.owner == Some(witness)) {
            holds.remove(&ticket);
        }
    }
    holds.len() != before
}

fn unavailable(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/remote-catalog-ledger", message.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs::File, time::Duration};

    use fs2::FileExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::classic_catalog_drain::within_vault;

    #[tokio::test]
    async fn a_hold_whose_release_fails_is_left_drained_for_reconciliation() {
        let vault = tempfile::tempdir().unwrap();
        let vault_path: Arc<VaultPath> = Arc::from(vault.path());
        let client: Arc<dyn ProviderStore> = Arc::new(InMemory::new());
        let path = Path::from("catalog-ledger");
        let ledger = CatalogLedger::new(Arc::clone(&client), ObjectKey::new("catalog-ledger").unwrap(), path.clone());
        let io = IoRegion::new();
        let permit = within_vault(Some(Arc::clone(&vault_path)), ledger.retain(None, None, &io)).await.unwrap();
        assert_eq!(ledger.read().await.unwrap().0.holds.len(), 1);
        let registered = client.get(&path).await.unwrap().bytes().await.unwrap();

        client.put_opts(&path, Bytes::from_static(b"not a ledger").into(), PutOptions::default()).await.unwrap();
        drop(permit);
        let witness = std::fs::read_dir(vault.path().join("catalog-readers")).unwrap().next().unwrap().unwrap().path();
        tokio::time::timeout(Duration::from_secs(5), async {
            while File::open(&witness).unwrap().try_lock_exclusive().is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();

        client.put_opts(&path, registered.into(), PutOptions::default()).await.unwrap();
        assert!(witness.exists());
        assert_eq!(ledger.drained_holds(Some(vault_path)).await.unwrap().len(), 1);
    }
}
