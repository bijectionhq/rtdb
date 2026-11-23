use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, ReplayCustody, StorageRegion};
use rtdb_columnar::MemoryBudget;
use rtdb_format::{envelope::ReplicaFrame, keys::CURRENT_STORAGE_VERSION};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, Result,
    log::{LogOffset, LogRecord, ReplicaObjectCustody, ReplicaPayload, ReplicaPayloadBroker}};
use rtdb_types::{Anomaly, StorageEpoch};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::deployment::{CellStorage, LogkeeperLogConfig};
use crate::keys::KeyService;
use crate::hosted_recovery::replay_start;

pub(crate) struct RecoveryHolds {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub custody: ReplayCustody,
    pub storage: CellStorage,
    pub tasks: TaskTracker,
}

#[derive(Clone)]
pub(crate) struct RecoveryAdmission {
    holds: Arc<RecoveryHolds>,
    deployment: String,
    stream: String,
    log_epoch: i32,
    writer_epoch: u64,
}

pub(crate) struct ReplayHold {
    pub through: LogOffset,
    broker: Arc<DirectoryPayloadBroker>,
    _release: oneshot::Sender<()>,
}

impl ReplayHold {
    pub fn retain_record(self: &Arc<Self>, record: &mut LogRecord) -> Result<()> {
        record.replica_objects = ReplicaObjectCustody::from_brokered_delivery(
            self.broker.clone(), record.metadata.offset, &record.payload, CURRENT_STORAGE_VERSION, StorageEpoch(0), self.clone(),
        )?;
        record.payload = self.retain_delivery(std::mem::take(&mut record.payload));
        Ok(())
    }

    pub fn retain_delivery(self: &Arc<Self>, payload: Bytes) -> Bytes {
        struct Delivery {
            payload: Bytes,
            _hold: Arc<ReplayHold>,
        }

        impl AsRef<[u8]> for Delivery {
            fn as_ref(&self) -> &[u8] { &self.payload }
        }
        Bytes::from_owner(Delivery { payload, _hold: Arc::clone(self) })
    }
}

impl RecoveryAdmission {
    pub fn new(holds: Arc<RecoveryHolds>, config: &LogkeeperLogConfig) -> Self {
        Self { holds, deployment: config.deployment.clone(), stream: config.replica_stream.clone(), log_epoch: config.log_epoch, writer_epoch: config.epoch }
    }

    pub async fn hold(&self, after: LogOffset, through: LogOffset) -> Result<Option<ReplayHold>> {
        if after >= through {
            self.check_start(after).await?;
            return Ok(None);
        }
        let id = Uuid::new_v4();
        let (release, closed) = oneshot::channel();
        let (ready, admitted) = oneshot::channel();
        let worker = self.clone();
        // The worker must resolve a submitted CAS even if the cursor opener is cancelled.
        self.holds.tasks.spawn(async move {
            let witness = if worker.holds.custody == ReplayCustody::Local {
                match worker.holds.keys.hold_local_storage_region(id) {
                    Ok(file) => file,
                    Err(error) => { let _ = ready.send(Err(worker.directory_error(error))); return; }
                }
            } else { None };
            let mut attempted = false;
            let outcome = worker.register(id, after, through, &mut attempted).await;
            let opened = outcome.is_ok();
            if ready.send(outcome).is_ok() && opened {
                let _ = closed.await;
            }
            if attempted {
                loop {
                    match crate::store::release_region(worker.holds.directory.as_ref(), &worker.deployment, id, !opened).await {
                        Ok(()) => {
                            crate::residence_trace::record("finish-recovery", || serde_json::json!({
                                "installation": worker.deployment, "stream": worker.stream, "epoch": worker.log_epoch, "hold": id,
                            }));
                            break;
                        }
                        Err(error) => eprintln!("Recovery hold {id} for {} is pending release: {}", worker.deployment, error.code),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            if witness.is_some() {
                drop(witness);
                if let Err(error) = worker.holds.keys.forget_local_storage_region(id) {
                    eprintln!("Could not remove recovery hold witness {id}: {}", error.code);
                }
            }
        });
        let residence = admitted.await.map_err(|_| Error::Closed(self.stream.clone()))??;
        let broker = Arc::new(DirectoryPayloadBroker {
            admission: self.clone(),
            residence,
            ticket: id,
        });
        Ok(Some(ReplayHold { through, broker, _release: release }))
    }

    pub(crate) async fn retain_offloaded(&self, records: &mut [LogRecord]) -> Result<()> {
        let mut offloaded = Vec::new();
        for record in records {
            if ReplicaFrame::has_external_payload(&record.payload).map_err(|error| self.directory_error(error))? {
                offloaded.push(record);
            }
        }
        let Some((first, last)) = offloaded.first().zip(offloaded.last()) else { return Ok(()); };
        let after = first.metadata.offset.0.checked_sub(1)
            .ok_or_else(|| Error::Task("Invalid first offloaded replica offset".into()))?;
        let hold = self.hold(LogOffset(after), last.metadata.offset).await?
            .map(Arc::new).ok_or_else(|| Error::Task("The offloaded replica delivery has no replay hold".into()))?;
        for record in offloaded {
            hold.retain_record(record)?;
        }
        Ok(())
    }

    pub(crate) async fn confirms_checkpoint(&self, block: i64, through: LogOffset, root: [u8; 32]) -> Result<bool> {
        let (record, _) = self.checked_record(through).await?;
        if record.epoch != self.writer_epoch || record.holder.is_none() { return Ok(false); }
        let (Some(checkpoint), Some(manifest)) = (record.checkpoint, record.recovery_manifest_sha256) else { return Ok(false); };
        if checkpoint.block != block || checkpoint.boundary != through.0 || checkpoint.root_sha256 != Some(root) {
            return Ok(false);
        }
        crate::residence_trace::record("hosted-recovery-closure", || serde_json::json!({
            "lineage": record.lineage, "installation": record.id, "incarnation": record.incarnation,
            "epoch": record.log_epoch, "stream": self.stream, "revision": record.version,
            "block": block, "through": through.0, "root": root, "manifest": manifest,
        }));
        Ok(true)
    }

    pub(crate) async fn check_start(&self, after: LogOffset) -> Result<()> {
        self.checked_record(after).await.map(|_| ())
    }

    async fn checked_record(&self, after: LogOffset) -> Result<(DeploymentRecord, i64)> {
        let record = self.holds.directory.deployment(&self.deployment).await.map_err(|error| self.directory_error(error))?;
        let record = record.filter(|record| {
            record.log_epoch == self.log_epoch && record.stream("replica") == self.stream
                && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        }).ok_or_else(|| Error::Closed(self.stream.clone()))?;
        let first_required = replay_start(&record, after.0)?;
        Ok((record, first_required))
    }

    async fn register(&self, id: Uuid, after: LogOffset, through: LogOffset, attempted: &mut bool) -> Result<String> {
        for _ in 0..8 {
            let (mut record, first_required) = self.checked_record(after).await?;
            let version = record.version;
            record.storage_regions.insert(id, StorageRegion::ReplicaReplay { first_required, through: through.0, custody: self.holds.custody.clone() });
            *attempted = true;
            if let Cas::Applied(stored) = self.holds.directory.replace_deployment(record, version).await.map_err(|error| self.directory_error(error))? {
                crate::residence_trace::record("admit-recovery", || serde_json::json!({
                    "lineage": stored.lineage, "installation": stored.id, "incarnation": stored.incarnation, "revision": stored.version,
                    "stream": self.stream, "epoch": self.log_epoch, "hold": id, "residence": stored.storage_prefix(),
                    "first_required": first_required, "through": through.0,
                }));
                return Ok(stored.storage_prefix());
            }
        }
        Err(Error::Remote { key: self.stream.clone(), message: "The directory kept changing while registering a replay hold".into() })
    }

    fn directory_error(&self, error: Anomaly) -> Error {
        Error::Remote { key: self.stream.clone(), message: error.to_string() }
    }
}

struct DirectoryPayloadBroker {
    admission: RecoveryAdmission,
    residence: String,
    ticket: Uuid,
}

#[async_trait::async_trait]
impl ReplicaPayloadBroker for DirectoryPayloadBroker {
    async fn read(&self, custody: &ReplicaObjectCustody, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload> {
        let owner = &self.admission;
        let position = custody.position().0;
        let closed = || Error::Closed(owner.stream.clone());
        let record = owner.holds.directory.deployment(&owner.deployment).await.map_err(|error| owner.directory_error(error))?
            .filter(|record| {
                record.stream("replica") == owner.stream && record.log_epoch == owner.log_epoch
                    && record.storage_prefix() == self.residence
                    && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                    && matches!(record.storage_regions.get(&self.ticket),
                        Some(StorageRegion::ReplicaReplay { first_required, through, .. })
                            if *first_required >= 0 && (*first_required..=*through).contains(&position))
            })
            .ok_or_else(closed)?;

        let io = IoRegion::new();
        let bucket = owner.holds.storage.bucket(io).await.map_err(|error| owner.directory_error(error))?;
        if record.storage_namespaces.is_some() {
            let objects = crate::deployment::existing_storage_namespace(bucket.as_ref()).await
                .map_err(|error| owner.directory_error(error))?.ok_or_else(closed)?;
            let keys = owner.holds.keys.namespace_id().await.map_err(|error| owner.directory_error(error))?;
            crate::deployment::require_storage_namespaces(&record, rtdb_directory::StorageNamespaces { objects, keys })
                .map_err(|error| owner.directory_error(error))?;
        }
        let key_id = record.key_id();
        let material = owner.holds.keys.existing(&key_id).await.map_err(|error| owner.directory_error(error))?;
        let root = crate::archive::checkpoint_storage_root().map_err(|error| owner.directory_error(error))?;
        let prefix = format!("{}/{root}", self.residence);
        let files = Arc::new(crate::reference::PrefixedStore::new(bucket, prefix.clone()));
        let marker = ObjectKey::new(crate::store::ROOT_MARKER)?;
        if files.get_fresh_exact(&marker, record.id.len()).await?.as_ref() != record.id.as_bytes() {
            return Err(closed());
        }
        let store = crate::store::EncryptingStore::new(files, &material).map_err(|error| owner.directory_error(error))?;
        let detached = record.checkpoint_transfer_activation.as_ref().is_some_and(|activation| {
            let terminal = &activation.replica.bootstrap.terminal;
            let stage = &terminal.fence.destination;
            stage.intent.destination.to_string() == record.id
                && stage.source_log_epoch == record.log_epoch
                && position > stage.checkpoint.boundary && position <= terminal.replica_through
        });
        let payload = if detached {
            custody.read_transferred_copy(&store, memory).await?
        } else {
            custody.read_from(&store, memory).await?
        };
        crate::residence_trace::record("read-retained-payload", || serde_json::json!({
            "lineage": record.lineage, "installation": record.id, "incarnation": record.incarnation,
            "revision": record.version, "stream": owner.stream, "epoch": record.log_epoch,
            "hold": self.ticket, "residence": self.residence, "position": position,
            "key": format!("{prefix}/{}", custody.object()),
            "publication": if detached { None } else { custody.publication() },
        }));
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_payload_read_spends_its_directory_replay_hold() {
        use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto::NoOp};
        use rtdb_types::{BlockIndex, Term, TermSequence};

        let root = tempfile::tempdir().unwrap();
        let vault = tempfile::tempdir().unwrap();
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.lifecycle = Lifecycle::Live;
        let record = directory.create_deployment(record).await.unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(),
            keys: Arc::new(crate::keys::LocalKeyService::new(vault.path()).unwrap()),
            custody: ReplayCustody::Local,
            storage: CellStorage::Local { root: root.path().to_owned() },
            tasks: TaskTracker::new(),
        });
        let admission = RecoveryAdmission {
            holds: holds.clone(), deployment: record.id.clone(), stream: record.stream("replica"),
            log_epoch: record.log_epoch, writer_epoch: record.epoch,
        };
        let io = IoRegion::new();
        let bucket = holds.storage.bucket(io.clone()).await.unwrap();
        let root = crate::archive::checkpoint_storage_root().unwrap();
        let files = Arc::new(crate::reference::PrefixedStore::new(bucket, format!("{}/{root}", record.storage_prefix())));
        files.put(&ObjectKey::new(crate::store::ROOT_MARKER).unwrap(), Bytes::copy_from_slice(record.id.as_bytes())).await.unwrap();
        let media_key = holds.keys.key(&record.key_id()).await.unwrap();
        let store = crate::store::EncryptingStore::new(files, &media_key).unwrap();
        let envelope = ReplicaEnvelope {
            term: Term(1), term_sequence: Some(TermSequence(0)),
            message: ReplicaBody::NoOp(NoOp::default()),
        };
        let payload = Bytes::from(envelope.encode());
        let epoch = StorageEpoch(0);
        let frame = envelope.offloaded(CURRENT_STORAGE_VERSION, epoch, BlockIndex(0), Uuid::new_v4().to_string(), &payload);
        let name = ObjectKey::new(frame.oversized_payload(CURRENT_STORAGE_VERSION, epoch).unwrap().unwrap()).unwrap();
        store.put(&name, payload.clone()).await.unwrap();
        let hold = Arc::new(admission.hold(LogOffset(0), LogOffset(2)).await.unwrap().unwrap());
        let proof = ReplicaObjectCustody::from_brokered_delivery(
            hold.broker.clone(), LogOffset(2), &frame.encode(), CURRENT_STORAGE_VERSION, epoch, hold.clone(),
        ).unwrap().unwrap();
        let memory = MemoryBudget::new(1 << 20);
        assert_eq!(proof.redeem(&store, &memory).await.unwrap().as_ref(), payload.as_ref());

        crate::store::release_region(directory.as_ref(), &record.id, hold.broker.ticket, false).await.unwrap();
        assert!(matches!(proof.redeem(&store, &memory).await, Err(Error::Closed(_))));
        drop((proof, hold));
        holds.tasks.close();
        holds.tasks.wait().await;
        io.close_and_wait().await;
    }

    #[tokio::test]
    async fn a_delivered_reference_keeps_its_replay_hold_through_provider_drain() {
        let (release, closed) = oneshot::channel();
        let root = tempfile::tempdir().unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: rtdb_directory::MemoryDirectory::new(),
            keys: Arc::new(crate::keys::LocalKeyService::new(root.path()).unwrap()),
            custody: ReplayCustody::Local,
            storage: CellStorage::Local { root: root.path().to_owned() },
            tasks: TaskTracker::new(),
        });
        let broker = Arc::new(DirectoryPayloadBroker {
            admission: RecoveryAdmission { holds, deployment: "d".into(), stream: "d/1/replica".into(), log_epoch: 0, writer_epoch: 1 },
            residence: "d/1".into(),
            ticket: Uuid::new_v4(),
        });
        let hold = Arc::new(ReplayHold { through: LogOffset(3), broker, _release: release });
        let delivery = hold.retain_delivery(Bytes::from_static(b"last record"));
        drop(hold);

        let (entered, started) = oneshot::channel();
        let (resume, gate) = std::sync::mpsc::channel();
        let region = rtdb_storage::IoRegion::new();
        let provider = Arc::clone(&region);
        let read = tokio::spawn(async move {
            rtdb_storage::IoRegion::retaining(Arc::new(delivery), provider.run_blocking(move || {
                let _ = entered.send(());
                gate.recv().unwrap();
                Ok(())
            })).await
        });
        started.await.unwrap();
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());

        let mut closed = closed;
        let pending = closed.try_recv();
        resume.send(()).unwrap();
        assert!(matches!(pending, Err(oneshot::error::TryRecvError::Empty)));
        region.close_and_wait().await;
        assert!(matches!(closed.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
    }
}
