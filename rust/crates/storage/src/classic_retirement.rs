use std::{fs::{File, OpenOptions}, path::{Path, PathBuf}, sync::Arc};

use fs2::FileExt;

use bytes::Bytes;
use rtdb_format::{catalog::BlockCatalog, keys::{block_key, is_receipt_l0_file, skipped_tx_position}, trie_retirement::InstalledRetirement};
use rtdb_types::{Anomaly, PhysicalObjectVersion, Result, SourcePosition};

use crate::{Error, IoRegion, LocalRepairAnchor, LocalStore, ObjectKey, ObjectStore,
    log::{LogOffset, local::{prefix_end, settle_prefix}}, sealing::ObjectCipher};


struct TrieDeletionGrant<'a> {
    store: &'a dyn ObjectStore,
    files: Vec<(ObjectKey, Option<PhysicalObjectVersion>)>,
    recovery_gate: Option<Arc<dyn Send + Sync>>,
}

impl<'a> TrieDeletionGrant<'a> {
    async fn authorize(store: &'a dyn ObjectStore, receipt: &ObjectKey, contents: Bytes) -> Result<Option<Self>> {
        if !store.defers_trie_releases() {
            return Err(Anomaly::unsupported("rtdb.storage/trie-release", "This store has no coordinated trie retirement"));
        }
        let installed = InstalledRetirement::decode(receipt.as_str(), &contents)?;
        if store.get_fresh_exact(receipt, contents.len()).await? != contents {
            return Err(Anomaly::incorrect("rtdb.storage/trie-release", "The durable trie retirement receipt differs"));
        }
        let mut selected: Vec<_> = installed.files().iter().collect();
        selected.sort_by_key(|file| file.release_order());
        let files = selected.into_iter().map(|file| {
            let key = ObjectKey::new(&file.key)?;
            let version = file.version.as_ref().filter(|_| file.matches_publication).cloned();
            Ok((key, version))
        }).collect::<crate::Result<Vec<_>>>()?;

        let recovery_gate = if let Some(local) = store.local_maintenance_store() {
            let Some(gate) = local.try_lock_recovery_deletion().await? else { return Ok(None); };
            let cut = installed.cut();
            if files.iter().any(|(file, _)| is_receipt_l0_file(file.as_str()))
                && !local.retained_repair_anchor().await?.is_some_and(|anchor|
                    anchor.boundary.epoch() == cut.replica_epoch && anchor.boundary >= cut.through)
            {
                return Ok(None);
            }
            Some(gate)
        } else { None };

        // The current conservative predicate uses the installed cut. Admission
        // and this grant can evolve independently once holds carry reachability.
        if !store.advance_catalog_floor(installed.cut()).await? { return Ok(None); }
        for (file, version) in &files {
            if !store.classic_trie_retirement_ready(file, version.as_ref()).await? { return Ok(None); }
        }
        Ok(Some(Self { store, files, recovery_gate }))
    }

    async fn erase(self, keys: Option<&dyn ObjectCipher>) -> Result<()> {
        let erase = async {
            for (file, version) in self.files {
                if let Some(keys) = keys {
                    keys.withdraw(&file).await?;
                }
                self.store.settle_classic_trie(&file, version.as_ref()).await?;
            }
            Ok(())
        };
        match self.recovery_gate {
            Some(gate) => IoRegion::retaining(gate, erase).await,
            None => erase.await,
        }
    }
}

struct ClassicLogDeletionGrant {
    source: (PathBuf, File, u64),
    replica: (PathBuf, File, u64),
    source_through: SourcePosition,
    maximum: usize,
    _recovery_gate: Arc<dyn Send + Sync>,
}

impl ClassicLogDeletionGrant {
    async fn authorize(store: &LocalStore, anchor: LocalRepairAnchor, replica: &Path,
        maximum: usize,
    ) -> Result<Self> {
        let unavailable = || Anomaly::incorrect("rtdb.storage/log-retirement", "Retiring a local log requires its exact retained checkpoint and exclusive custody");
        let gate = store.try_lock_recovery_deletion().await?.ok_or_else(unavailable)?;
        if store.retained_repair_anchor().await? != Some(anchor) || replica.file_name().and_then(|name| name.to_str()) != Some("REPLICA_LOG") {
            return Err(unavailable());
        }
        crate::sealing::ClassicKeyVault::require_unsealed_log(replica)?;
        crate::sealing::ClassicKeyVault::require_unsealed_log(&replica.with_file_name("LOG"))?;
        let root = ObjectKey::new(block_key(anchor.block)?)?;
        let catalog = BlockCatalog::decode(&store.get_fresh(&root).await?)?;
        if catalog.publication_sha256() != Some(anchor.sha256)
            || catalog.boundary_replica_position() != Some(anchor.boundary)
        {
            return Err(unavailable());
        }
        let source_through = catalog.latest_source_position()
            .filter(|source| source.after_offset(anchor.boundary.epoch()) >= 0).ok_or_else(unavailable)?;
        let replica = replica.to_path_buf();
        store.io_region().run(move || {
            let source = replica.with_file_name("LOG");
            let open = |path: &Path| OpenOptions::new().read(true).write(true).open(path)
                .map_err(|error| Error::io(path, error));
            let mut source_file = open(&source)?;
            FileExt::try_lock_exclusive(&source_file).map_err(|error| Error::io(&source, error))?;
            let mut replica_file = open(&replica)?;
            FileExt::try_lock_exclusive(&replica_file).map_err(|error| Error::io(&replica, error))?;
            let source_end = prefix_end(&mut source_file, &source, maximum,
                LogOffset(source_through.after_offset(anchor.boundary.epoch())), true)?;
            let replica_end = prefix_end(&mut replica_file, &replica, maximum,
                LogOffset(anchor.boundary.offset()), false)?;
            Ok(Self { source: (source, source_file, source_end), replica: (replica, replica_file, replica_end),
                source_through, maximum, _recovery_gate: gate })
        }).await.map_err(Into::into)
    }

    async fn settle(mut self, store: &LocalStore) -> Result<()> {
        let through = self.source_through;
        IoRegion::retaining(Arc::clone(&self._recovery_gate), async {
            let locked = store.io_region().run(move || {
                let (path, file, end) = &mut self.source;
                settle_prefix(file, path, self.maximum, *end)?;
                let (path, file, end) = &mut self.replica;
                settle_prefix(file, path, self.maximum, *end)?;
                Ok(self)
            }).await?;
            for object in store.list(Some(&ObjectKey::new("skipped-txs")?)).await? {
                if skipped_tx_position(object.key.as_str()).is_some_and(|position|
                    position.epoch() == through.epoch() && position <= through)
                {
                    store.delete(&object.key).await?;
                }
            }
            drop(locked);
            Ok(())
        }).await
    }
}

pub(crate) async fn retire_log_prefixes(store: &LocalStore, anchor: LocalRepairAnchor,
    replica: &Path, maximum: usize,
) -> Result<()> {
    ClassicLogDeletionGrant::authorize(store, anchor, replica, maximum).await?.settle(store).await
}

pub(crate) async fn retire(store: &dyn ObjectStore, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
    retire_with_keys(store, receipt, contents, None).await
}

pub(crate) async fn retire_with_keys(
    store: &dyn ObjectStore, receipt: &ObjectKey, contents: Bytes, keys: Option<&dyn ObjectCipher>,
) -> Result<bool> {
    let Some(grant) = TrieDeletionGrant::authorize(store, receipt, contents).await? else { return Ok(false); };
    grant.erase(keys).await?;
    Ok(true)
}
