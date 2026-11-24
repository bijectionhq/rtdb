use super::*;
use catalog::ManagedTransfer;

impl MultiRuntime {
    /// Installs an original authority before handing this runtime to the server.
    pub fn with_managed_checkpoint_source(
        mut self,
        incarnation: Uuid,
        source: Arc<dyn ManagedCheckpointSource>,
    ) -> Result<Self> {
        let sources = Arc::make_mut(&mut self.managed_sources);
        if incarnation.is_nil() || sources.contains_key(&incarnation) {
            return Err(reservation_conflict());
        }
        sources.insert(incarnation, source);
        Ok(self)
    }

    pub(super) fn managed_source(&self, incarnation: Uuid) -> Result<&dyn ManagedCheckpointSource> {
        self.managed_sources
            .get(&incarnation)
            .map(Arc::as_ref)
            .ok_or_else(source_unavailable)
    }

    pub(super) async fn drive_managed_transfers(&self, stop: &CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stop.run_until_cancelled(async {
            loop {
                interval.tick().await;
                if !self.catalog.is_writer().await {
                    continue;
                }
                let rows = match self.catalog.read().await {
                    Ok(rows) => rows,
                    Err(error) if retryable(&error) => continue,
                    Err(error) => return Err(error),
                };
                for (name, entry) in rows {
                    if let Status::TransferPending(pending) = entry.status
                        && matches!(pending.export, TransferExport::Managed { .. })
                        && self.managed_sources.contains_key(&pending.incarnation)
                    {
                        let result = self
                            .finish_managed_transfer(&name, entry.database, pending.request, stop)
                            .await;
                        if let Err(error) = result
                            && !retryable(&error)
                            && !matches!(
                                error.category,
                                AnomalyCategory::Incorrect
                                    | AnomalyCategory::NotFound
                                    | AnomalyCategory::Forbidden
                                    | AnomalyCategory::Unsupported
                            )
                        {
                            return Err(error);
                        }
                    }
                }
            }
        })
        .await
        .unwrap_or(Ok(()))
    }

    pub(super) async fn finish_managed_transfer(
        &self,
        name: &str,
        database: Uuid,
        request: [u8; 32],
        cancellation: &CancellationToken,
    ) -> Result<Basis> {
        let target = ManagedCheckpointTarget {
            incarnation: *database.as_bytes(),
            request,
        };
        let mut selected = self.managed_entry(name, database).await?;
        if let Status::TransferPending(pending) = &selected.status
            && let TransferExport::Managed {
                pin,
                basis,
                manifest: None,
            } = pending.export
        {
            if pending.request != request {
                return Err(reservation_conflict());
            }
            self.managed_source(pending.incarnation)?;
            let identity = self
                .publish_managed_pin(
                    &pending.source,
                    pending.incarnation,
                    name,
                    target,
                    pin,
                    basis,
                    cancellation,
                )
                .await?;
            selected = self
                .catalog
                .bind_managed_transfer(name, target, identity)
                .await?;
        }
        let transfer = selected.managed_transfer()?;
        let reservation = transfer.reservation;
        if reservation.target != target {
            return Err(reservation_conflict());
        }
        if let Status::ManagedTransferReady { basis, .. } = selected.status {
            return Ok(basis);
        }

        let basis = cancellation
            .run_until_cancelled(async {
                if let Some(peer) = &self.peer
                    && let Some(basis) = peer.forward_managed_install(name, reservation).await?
                {
                    return Ok(basis);
                }
                self.install_peer_managed_transfer(name, reservation).await
            })
            .await
            .ok_or_else(transfer_interrupted)??;

        cancellation
            .run_until_cancelled(async {
                let source = &transfer.source;
                if let Some(peer) = &self.peer
                    && peer
                        .forward_managed_release(source, name, reservation)
                        .await?
                {
                    return Ok(());
                }
                self.release_peer_managed_transfer(source, name, reservation)
                    .await
            })
            .await
            .ok_or_else(transfer_interrupted)??;

        self.catalog
            .mark_managed_transfer_ready(name, transfer, basis)
            .await?;
        Ok(basis)
    }

    async fn publish_managed_pin(
        &self,
        source: &str,
        incarnation: Uuid,
        destination: &str,
        target: ManagedCheckpointTarget,
        pin: Uuid,
        basis: Basis,
        cancellation: &CancellationToken,
    ) -> Result<PinnedCheckpointIdentity> {
        cancellation
            .run_until_cancelled(async {
                if let Some(peer) = &self.peer
                    && let Some(identity) = peer
                        .forward_managed_pin(source, destination, target, pin, basis)
                        .await?
                {
                    return Ok(identity);
                }
                self.pin_peer_managed_transfer(source, incarnation, destination, target, pin, basis)
                    .await
            })
            .await
            .ok_or_else(transfer_interrupted)?
    }

    pub(super) async fn pin_peer_managed_transfer(
        &self,
        source: &str,
        incarnation: Uuid,
        destination: &str,
        target: ManagedCheckpointTarget,
        pin: Uuid,
        basis: Basis,
    ) -> Result<PinnedCheckpointIdentity> {
        let runtime = self.clone();
        let (source, destination) = (source.to_owned(), destination.to_owned());
        self.managed_operations
            .spawn(async move {
                runtime
                    .pin_managed_transfer(&source, incarnation, &destination, target, pin, basis)
                    .await
            })
            .await
            .map_err(|_| transfer_interrupted())?
    }

    async fn pin_managed_transfer(
        &self,
        source: &str,
        incarnation: Uuid,
        destination: &str,
        target: ManagedCheckpointTarget,
        pin: Uuid,
        basis: Basis,
    ) -> Result<PinnedCheckpointIdentity> {
        if basis.incarnation() != *incarnation.as_bytes() {
            return Err(reservation_conflict());
        }
        self.managed_source(incarnation)?;
        let writer = self.ready_driver(source, incarnation).await?;
        writer
            .pin_managed_checkpoint(pin, basis, target, async {
                let selected = self
                    .managed_entry(destination, Uuid::from_bytes(target.incarnation))
                    .await?;
                match selected.status {
                    Status::TransferPending(catalog::Transfer {
                        request,
                        source: original,
                        incarnation: prior_incarnation,
                        export:
                            TransferExport::Managed {
                                pin: prior_pin,
                                basis: prior_basis,
                                manifest: None,
                            },
                    }) if request == target.request
                        && original == source
                        && prior_incarnation == incarnation
                        && prior_pin == pin
                        && prior_basis == basis =>
                    {
                        Ok(())
                    }
                    _ => Err(reservation_conflict()),
                }
            })
            .await
    }

    pub(super) async fn install_peer_managed_transfer(
        &self,
        name: &str,
        reservation: ManagedCheckpointReservation,
    ) -> Result<Basis> {
        let runtime = self.clone();
        let name = name.to_owned();
        // A timed-out peer or admin reply must not cancel the sole copy worker.
        self.managed_operations
            .spawn(async move {
                runtime
                    .confirm_managed_installation(&name, reservation)
                    .await
            })
            .await
            .map_err(|_| transfer_interrupted())?
    }

    async fn confirm_managed_installation(
        &self,
        name: &str,
        reservation: ManagedCheckpointReservation,
    ) -> Result<Basis> {
        let database = Uuid::from_bytes(reservation.target.incarnation);
        let selected = self.managed_entry(name, database).await?;
        let transfer = selected.managed_transfer()?;
        if transfer.reservation != reservation {
            return Err(reservation_conflict());
        }
        let destination = self.ensure_driver(name, database).await?;
        destination.require_managed_checkpoint_writer().await?;
        if let Status::ManagedTransferReady { basis, .. } = selected.status {
            return Ok(basis);
        }

        let cancellation = CancellationToken::new();
        let source_id = Uuid::from_bytes(reservation.source.basis.incarnation());
        let provider = self
            .managed_source(source_id)?
            .bind(reservation, &cancellation)?;
        if destination
            .restored_checkpoint_basis(reservation.source.manifest_sha256)
            .await?
            .is_none()
        {
            let source = self.ready_driver(&transfer.source, source_id).await?;
            let export = source.open_managed_checkpoint(reservation).await?;
            provider.install(&export, &destination).await?;
        }
        let basis = provider.recover_installation(&destination).await?;
        if basis
            != Basis::new(
                reservation.target.incarnation,
                reservation.source.basis.position(),
            )
        {
            return Err(reservation_conflict());
        }
        destination.require_managed_checkpoint_writer().await?;
        Ok(basis)
    }

    pub(super) async fn release_peer_managed_transfer(
        &self,
        source: &str,
        destination: &str,
        reservation: ManagedCheckpointReservation,
    ) -> Result<()> {
        let runtime = self.clone();
        let (source, destination) = (source.to_owned(), destination.to_owned());
        self.managed_operations
            .spawn(async move {
                runtime
                    .release_managed_transfer(&source, &destination, reservation)
                    .await
            })
            .await
            .map_err(|_| transfer_interrupted())?
    }

    async fn release_managed_transfer(
        &self,
        source: &str,
        destination: &str,
        reservation: ManagedCheckpointReservation,
    ) -> Result<()> {
        let selected = self
            .managed_entry(
                destination,
                Uuid::from_bytes(reservation.target.incarnation),
            )
            .await?;
        let transfer = selected.managed_transfer()?;
        if transfer.source != source || transfer.reservation != reservation {
            return Err(reservation_conflict());
        }
        let source_id = Uuid::from_bytes(reservation.source.basis.incarnation());
        let writer = self.ready_driver(source, source_id).await?;
        writer.require_managed_checkpoint_writer().await?;
        let cancellation = CancellationToken::new();
        self.managed_source(source_id)?
            .bind(reservation, &cancellation)?
            .release_source(&writer)
            .await
    }

    pub(super) async fn check_managed_read(
        &self,
        name: &str,
        incarnation: [u8; 16],
        read: ManagedCheckpointRead<'_>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let database = Uuid::from_bytes(incarnation);
        let selected = self.managed_entry(name, database).await?;
        let Status::ManagedTransferReady { transfer, .. } = selected.status else {
            return Err(reservation_conflict());
        };
        let reservation = transfer.reservation;
        let source_id = Uuid::from_bytes(reservation.source.basis.incarnation());
        let destination = self.ready_driver(name, database).await?;
        self.managed_source(source_id)?
            .bind(reservation, cancellation)?
            .require_read(&destination, read)
            .await
    }

    async fn managed_entry(&self, name: &str, database: Uuid) -> Result<catalog::Deployment> {
        self.catalog
            .read()
            .await?
            .remove(name)
            .filter(|entry| entry.database == database)
            .ok_or_else(reservation_conflict)
    }
}

pub(super) fn reservation_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/managed-checkpoint-reservation",
        "The managed transfer does not match its ordered catalog reservation",
    )
}

fn source_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/managed-checkpoint-original",
        "The source incarnation has no mounted original checkpoint custody authority",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use rtdb_db::PinnedExport;
    use rtdb_node::ManagedCheckpointProvider;
    use rtdb_storage::sealing::PlaintextView;
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Transact};
    use rtdb_types::{EntityId, TableRef, Value, rtdb_v1::MAIN_BRANCH};

    use super::*;

    #[derive(Default)]
    struct ProviderState {
        copies: AtomicUsize,
        confirm: AtomicBool,
        drained: AtomicBool,
        lose_release_reply: AtomicBool,
    }

    struct Provider<'a> {
        state: &'a ProviderState,
        reservation: ManagedCheckpointReservation,
    }

    impl ManagedCheckpointSource for ProviderState {
        fn bind<'a>(
            &'a self,
            reservation: ManagedCheckpointReservation,
            _: &'a CancellationToken,
        ) -> Result<Box<dyn ManagedCheckpointProvider + 'a>> {
            Ok(Box::new(Provider {
                state: self,
                reservation,
            }))
        }
    }

    fn unavailable(code: &'static str) -> Anomaly {
        Anomaly::new(
            AnomalyCategory::Unavailable,
            code,
            "The fixture has withheld the exact evidence",
        )
    }

    #[async_trait::async_trait]
    impl ManagedCheckpointProvider for Provider<'_> {
        async fn install(
            &self,
            export: &PinnedExport,
            destination: &MultiServing,
        ) -> Result<Basis> {
            destination
                .prepare_managed_checkpoint(self.reservation)
                .await?;
            let cut = destination
                .restore_managed_checkpoint(export, self.reservation)
                .await?;
            self.state.copies.fetch_add(1, Ordering::SeqCst);
            Ok(cut.basis)
        }

        async fn recover_installation(&self, destination: &MultiServing) -> Result<Basis> {
            if !self.state.confirm.load(Ordering::SeqCst) {
                return Err(unavailable("rtdb.test/managed-copy-unconfirmed"));
            }
            Ok(destination
                .managed_checkpoint_view(self.reservation)
                .await?
                .0)
        }

        async fn require_read(
            &self,
            destination: &Arc<MultiServing>,
            read: ManagedCheckpointRead<'_>,
        ) -> Result<()> {
            let (_, _, generation) = destination
                .managed_checkpoint_view(self.reservation)
                .await?;
            if read.generation != generation
                || read.branch != MAIN_BRANCH
                || read.table != &TableRef::new("public", "docs")
                || read.id != &EntityId::Integer(1)
                || read.carrier != b"fixture"
            {
                return Err(unavailable("rtdb.test/managed-read-slot"));
            }
            Ok(())
        }

        async fn release_source(&self, source: &MultiServing) -> Result<()> {
            if !self.state.drained.load(Ordering::SeqCst) {
                return Err(unavailable("rtdb.test/managed-copy-undrained"));
            }
            source.release_managed_checkpoint(self.reservation).await?;
            if self.state.lose_release_reply.swap(false, Ordering::SeqCst) {
                return Err(unavailable("rtdb.test/managed-release-reply-lost"));
            }
            Ok(())
        }
    }

    #[test]
    fn managed_catalog_waits_for_confirmation_and_drains_and_recovers_a_lost_unpin_reply() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(managed_catalog_fixture())
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn managed_catalog_fixture() {
        let data = tempfile::tempdir().unwrap();
        let limit = 256 << 20;
        let mut config =
            NodeConfiguration::from_yaml_with_environment("", &|_| None, false).unwrap();
        config.flight_sql = None;
        config.health = None;
        let memory = MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
        let runtime = MultiRuntime::development(data.path(), &mut config, Arc::clone(&memory))
            .await
            .unwrap();
        let unmounted = runtime.clone();
        let source_id = runtime.bootstrap["xtdb"];
        let state = Arc::new(ProviderState::default());
        let runtime = runtime
            .with_managed_checkpoint_source(source_id, state.clone())
            .unwrap();
        let (handle, server) =
            crate::ServerDriver::open_with_multi(config, limit, memory, &runtime)
                .await
                .unwrap();
        let server = tokio::spawn(server.run());
        let node = handle.node();
        runtime.tick(node).await.unwrap();
        let cancellation = CancellationToken::new();
        let source = runtime.ready_driver("xtdb", source_id).await.unwrap();
        let table = TableRef::new("public", "docs");
        let id = EntityId::Integer(1);
        let transaction = Transact {
            key: vec![27; 16],
            require: vec![],
            window: Default::default(),
            guards: vec![],
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table: table.clone(),
                if_absent: false,
                rows: vec![Row {
                    id: id.clone(),
                    valid: None,
                    custody_group: None,
                    protected_columns: vec![],
                    custody_payload: None,
                    cells: [(
                        "value".into(),
                        Cell::Value(Value::Binary(b"fixture".to_vec())),
                    )]
                    .into_iter()
                    .collect(),
                }],
            }],
            otherwise: vec![],
            requires: vec![],
            custody_request_digest: None,
            request_memory: None,
        };
        let receipt = source.transact_local(transaction, None).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        let basis = receipt.basis;
        let pin = Uuid::new_v4();
        let request = || DeploymentTransfer::ManagedFork { pin, basis };
        let rejected = unmounted
            .transfer("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(rejected.code, "rtdb/managed-checkpoint-original");
        assert!(!runtime.catalog.read().await.unwrap().contains_key("copy"));
        let pending = runtime
            .transfer("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(pending.code, "rtdb/no-writer");

        let destination_id = runtime.catalog.deployment_id("copy");
        let destination = runtime.ensure_driver("copy", destination_id).await.unwrap();
        destination
            .tick(&BTreeSet::from([runtime.backends.node]))
            .await
            .unwrap();
        let bound = runtime.managed_entry("copy", destination_id).await.unwrap();
        let reservation = bound.managed_transfer().unwrap().reservation;
        assert_eq!(bound.written, reservation.reserved_at);
        let mut changed = reservation;
        changed.reserved_at = Basis::new(
            reservation.reserved_at.incarnation(),
            reservation.reserved_at.position() + 1,
        );
        let error = runtime
            .install_peer_managed_transfer("copy", changed)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb/managed-checkpoint-reservation");
        let error = runtime
            .release_peer_managed_transfer("xtdb", "copy", changed)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb/managed-checkpoint-reservation");

        let error = runtime
            .transfer("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb.test/managed-copy-unconfirmed");
        assert_eq!(state.copies.load(Ordering::SeqCst), 1);
        assert!(source.open_managed_checkpoint(reservation).await.is_ok());
        assert!(node.catalog().database("copy").is_err());

        state.confirm.store(true, Ordering::SeqCst);
        let error = runtime
            .transfer("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb.test/managed-copy-undrained");
        assert!(source.open_managed_checkpoint(reservation).await.is_ok());
        assert!(node.catalog().database("copy").is_err());

        state.drained.store(true, Ordering::SeqCst);
        state.lose_release_reply.store(true, Ordering::SeqCst);
        let error = runtime
            .transfer("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb.test/managed-release-reply-lost");
        let error = source
            .open_managed_checkpoint(reservation)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, "rtdb/unknown-pin");
        let error = runtime
            .pin_peer_managed_transfer("xtdb", source_id, "copy", reservation.target, pin, basis)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb/managed-checkpoint-reservation");
        assert!(node.catalog().database("copy").is_err());

        let installed = node
            .transfer_managed_deployment("xtdb", "copy", request(), &cancellation)
            .await
            .unwrap();
        assert_eq!(
            installed,
            Basis::new(*destination_id.as_bytes(), basis.position())
        );
        assert_eq!(state.copies.load(Ordering::SeqCst), 1);
        let ready = runtime.managed_entry("copy", destination_id).await.unwrap();
        assert!(matches!(ready.status, Status::ManagedTransferReady { .. }));
        assert_eq!(ready.managed_transfer().unwrap().reservation, reservation);
        assert_ne!(ready.written, reservation.reserved_at);

        let (_, _, generation) = destination
            .managed_checkpoint_view(reservation)
            .await
            .unwrap();
        let read = |carrier: &'static [u8]| ManagedCheckpointRead {
            generation,
            branch: MAIN_BRANCH,
            table: &table,
            id: &id,
            carrier,
        };
        let error = node
            .check_managed_checkpoint_read("copy", read(b"fixture"), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.code, "rtdb/managed-checkpoint-read");
        let plaintext = PlaintextView::new(Arc::clone(node.memory()));
        plaintext
            .reading(async {
                let error = unmounted
                    .check_managed_read(
                        "copy",
                        *destination_id.as_bytes(),
                        read(b"fixture"),
                        &cancellation,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.code, "rtdb/managed-checkpoint-original");
                let error = node
                    .check_managed_checkpoint_read("copy", read(b"changed"), &cancellation)
                    .await
                    .unwrap_err();
                assert_eq!(error.code, "rtdb.test/managed-read-slot");
                node.check_managed_checkpoint_read("copy", read(b"fixture"), &cancellation)
                    .await
                    .unwrap();
            })
            .await;
        assert!(plaintext.grant().await.is_ok());
        node.detach_local("copy".to_owned()).await.unwrap();
        assert_eq!(
            plaintext.grant().await.err().unwrap().code,
            "xtdb/no-such-db"
        );

        drop(destination);
        drop(source);
        drop(unmounted);
        handle.shutdown();
        server.await.unwrap().unwrap();
        drop(handle);
        runtime.close().await;
    }
}
