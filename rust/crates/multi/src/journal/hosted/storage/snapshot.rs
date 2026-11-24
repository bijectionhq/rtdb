use super::*;

pub(super) struct Source {
    pub(super) placement: Arc<super::super::Placement>,
    pub(super) keys: Arc<dyn KeyService>,
    pub(super) io: Arc<IoRegion>,
    pub(super) tasks: tokio_util::task::TaskTracker,
}

impl Source {
    pub(super) async fn retain(
        &self, cut: rtdb_types::CatalogCut, base: rtdb_types::CatalogCheckpoint,
    ) -> Result<Arc<dyn Send + Sync>> {
        let custody = self.placement.hold_query_custody(&self.tasks).await?;
        let journal = &self.placement.worker;
        let epoch = journal.log_epoch();
        let genesis = ReplicaPosition::from_epoch_offset(epoch, -1)?;
        if cut.replica_epoch != epoch || cut.storage_epoch != rtdb_types::StorageEpoch(0)
            || cut.storage_version != rtdb_format::keys::CURRENT_STORAGE_VERSION
            || base.boundary < genesis || cut.through < base.boundary
            || (base.boundary > genesis && base.boundary.epoch() != epoch)
            || (cut.through > genesis && cut.through.epoch() != epoch)
        {
            return Err(invalid("The catalog belongs to another journal generation"));
        }
        let from = (base.boundary.0 - genesis.0) as u64;
        let through = (cut.through.0 - genesis.0) as u64;
        let route = journal.route().await?;
        if from < route.erasure_floor { return Err(Error::ErasedUnderView.into()); }
        let request = journal.request(&route, ordered_logkeeper::ordered::Command::ProtectCheckpoint {
            root: base.root, from, through,
        })?;
        let held = crate::journal::Held::acquire(journal, request, &self.tasks).await?;
        let scope = held.scope();
        if scope.checkpoint.root != base.root || scope.checkpoint.cut != scope.start
            || scope.start.next_lsn != from || scope.through.next_lsn != through
        {
            return Err(invalid("The protected catalog has a different checkpoint or suffix"));
        }
        Ok(Arc::new((held, custody)))
    }

    pub(super) async fn open(
        &self, files: Arc<crate::journal::log::SnapshotFiles>, objects: ReplicaObjects,
        allocation: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ObjectStore>> {
        if !Arc::ptr_eq(&files.journal, &self.placement.worker)
            || files.journal.placement() != objects.installed
        {
            return Err(invalid("The exact snapshot belongs to another placement"));
        }
        let scope = ReaderScope {
            journal: Arc::clone(&files.journal), protection: Arc::clone(&files.held),
            epoch: files.epoch, lease: ServingLease::default(),
        };
        let deadline = scope.serving().await?;
        let held = scope.protection.scope();
        let checkpoint = if genesis(held.checkpoint) {
            None
        } else {
            let opened = with_read_lease(deadline, |_| IoRegion::retaining(Arc::clone(&scope.protection) as Arc<dyn Send + Sync>, async {
                let provider = self.placement.checkpoint_custody(Arc::clone(&self.io), Arc::clone(&self.keys)).await?;
                provider.open(held.checkpoint).await
            })).await?;
            Some(opened.store)
        };
        Ok(Arc::new(SnapshotStore {
            checkpoint, objects, files, scope, allocation,
            placement: Arc::clone(&self.placement), tasks: self.tasks.clone(),
        }))
    }
}

/// Serves the original checkpoint and the immutable files named by its suffix.
struct SnapshotStore {
    checkpoint: Option<Arc<crate::journal::custody::ExactStore>>,
    objects: ReplicaObjects,
    files: Arc<crate::journal::log::SnapshotFiles>,
    scope: ReaderScope,
    allocation: Arc<dyn ObjectStore>,
    placement: Arc<super::super::Placement>,
    tasks: tokio_util::task::TaskTracker,
}

impl SnapshotStore {
    async fn deadline(&self) -> rtdb_storage::Result<Instant> {
        self.scope.serving().await.map_err(|error| Error::Closed(error.to_string()))
    }

    async fn select(&self, key: &ObjectKey, deadline: Instant) -> rtdb_storage::Result<Arc<dyn ObjectStore>> {
        if let Some(checkpoint) = &self.checkpoint && checkpoint.contains(key) {
            return Ok(Arc::clone(checkpoint) as Arc<dyn ObjectStore>);
        }
        let epoch = self.files.origin(|epoch| {
            let prefix = source_prefix(&self.objects.storage_prefix, self.objects.installed.generation, epoch, &self.objects.root);
            let physical = ObjectKey::new(format!("{prefix}/{key}"))?;
            self.objects.bucket.s3_publication_address(&physical).map(|(namespace, name)| {
                crate::store::exact_retirement::installed_object_name(&namespace, &name)
                    .map_err(|error| Error::Task(error.to_string()))
            }).transpose()
        })?.ok_or_else(|| Error::MissingObject(key.to_string()))?;
        self.objects.open_writer_with_grant(epoch, Some(deadline)).await
    }

    async fn read<T, F, Fut>(&self, key: &ObjectKey, read: F) -> rtdb_storage::Result<T>
    where
        F: FnOnce(Arc<dyn ObjectStore>) -> Fut,
        Fut: std::future::Future<Output = rtdb_storage::Result<T>>,
    {
        with_read_lease(self.deadline().await?, |deadline| IoRegion::retaining(
            Arc::clone(&self.scope.protection) as Arc<dyn Send + Sync>,
            async move { read(self.select(key, deadline).await?).await },
        )).await
    }
}

fn read_only() -> Error { Error::Closed("An exact snapshot store is read-only".into()) }

#[async_trait]
impl ObjectStore for SnapshotStore {
    async fn catalog_delivery_lease(&self) -> Result<Option<Arc<ReadLease>>> {
        Ok(Some(Arc::new(ReadLease::until(self.scope.serving().await?))))
    }

    async fn retain_catalog(&self, _cut: Option<rtdb_types::CatalogCut>) -> Result<Option<Arc<dyn Send + Sync>>> {
        let custody = self.placement.hold_query_custody(&self.tasks).await?;
        self.scope.serving().await?;
        Ok(Some(Arc::new((Arc::clone(&self.files), custody))))
    }

    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        self.read(key, |store| async move { store.head_fresh(key).await }).await
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store| async move { store.get_fresh(key).await }).await
    }

    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store| async move { store.get_fresh_exact(key, length).await }).await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store| async move { store.get_range(key, range).await }).await
    }

    fn supports_scan_ranges(&self, _key: &ObjectKey) -> bool { false }

    fn whole_object_allocation(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        self.allocation.whole_object_allocation(key, length)
    }

    async fn get_reserved_object(&self, key: &ObjectKey, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        self.read(key, |store| async move { store.get_reserved_object(key, reservation).await }).await
    }

    async fn list(&self, _: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { Err(read_only()) }
    async fn put(&self, _: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> { Err(read_only()) }
    async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> { Err(read_only()) }
    async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> { Err(read_only()) }
}
