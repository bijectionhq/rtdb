use std::sync::Arc;

use bytes::Bytes;
use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, ReplayCustody, StorageRegion};
use rtdb_columnar::MemoryBudget;
use rtdb_format::{envelope::ReplicaFrame, keys::CURRENT_STORAGE_VERSION};
use rtdb_storage::{Error, ObjectKey, Result,
    log::{LogOffset, LogRecord, ReplicaObjectCustody, ReplicaPayload, ReplicaPayloadBroker}, release::Settled};
use rtdb_types::{Anomaly, PhysicalObjectVersion, StorageEpoch};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::deployment::LogkeeperLogConfig;
use crate::hold_release::release_hold;
use crate::keys::KeyService;
use crate::hosted_recovery::replay_start;
use crate::replay_broker::{Client, History};

pub(crate) struct RecoveryHolds {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub custody: ReplayCustody,
    pub broker: Option<Client>,
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
    client: Option<Client>,
    history: History,
    ticket: Uuid,
    _release: oneshot::Sender<()>,
}

impl ReplayHold {
    pub fn retain_record(self: &Arc<Self>, record: &mut LogRecord) -> Result<()> {
        if ReplicaFrame::has_external_payload(&record.payload).map_err(broker_error)? {
            let broker = Arc::new(DirectoryPayloadBroker {
                client: self.client.clone(),
                history: self.history,
                ticket: self.ticket,
                frame: record.payload.clone(),
            });
            record.replica_objects = ReplicaObjectCustody::from_brokered_delivery(
                broker, record.metadata.offset, &record.payload, CURRENT_STORAGE_VERSION, StorageEpoch(0), self.clone(),
            )?;
        }
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

    pub(crate) async fn check_handoff_dispatch(&self, cutoff: rtdb_types::TimedJournalCutoff) -> Result<()> {
        let record = self.holds.directory.deployment(&self.deployment).await.map_err(broker_error)?
            .filter(|record| record.epoch == self.writer_epoch && record.stream("replica") == self.stream)
            .ok_or(Error::GuardedAppendCutoff)?;
        if let Some(handoff) = record.writer_handoffs.last().filter(|entry| entry.pending()
            && entry.plan.identity.predecessor_epoch() == self.writer_epoch)
            && !crate::agent::writer_handoff_decision::original_cutoff_before_fence(
                cutoff.before().0, handoff.plan.maximum_clock_error_ns, handoff.plan.identity.switch_at_ns())
        {
            return Err(Error::GuardedAppendCutoff);
        }
        Ok(())
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
            let released = if attempted {
                let (directory, deployment) = (&worker.holds.directory, &worker.deployment);
                let settled = release_hold(format!("recovery hold {id} for {deployment}"),
                    move || crate::store::release_region(directory.as_ref(), deployment, id, !opened)).await;
                if let Settled::Released = settled {
                    crate::residence_trace::record("finish-recovery", || serde_json::json!({
                        "installation": worker.deployment, "stream": worker.stream, "epoch": worker.log_epoch, "hold": id,
                    }));
                }
                settled.is_released()
            } else { true };
            if witness.is_some() {
                drop(witness);
                if released && let Err(error) = worker.holds.keys.forget_local_storage_region(id) {
                    tracing::warn!("Could not remove recovery hold witness {id}: {}", error.code);
                }
            }
        });
        let history = admitted.await.map_err(|_| Error::Closed(self.stream.clone()))??;
        Ok(Some(ReplayHold {
            through, client: self.holds.broker.clone(), history, ticket: id, _release: release,
        }))
    }

    pub async fn publish(&self, key: &ObjectKey, body: Bytes) -> Result<Option<PhysicalObjectVersion>> {
        let client = self.holds.broker.as_ref().ok_or_else(no_broker)?;
        let record = self.current_record().await?;
        let history = History::of(&record).map_err(broker_error)?;
        client.publish(history, self.writer_epoch, key, body).await.map_err(broker_error)
    }

    pub(crate) async fn admit_detached_append(&self, payload: &[u8]) -> Result<()> {
        let client = self.holds.broker.as_ref().ok_or_else(no_broker)?;
        let record = self.current_record().await?;
        client.admit_append(History::of(&record).map_err(broker_error)?, self.writer_epoch, payload).await.map_err(broker_error)
    }

    pub(crate) fn observe_detached_commit(&self, position: LogOffset, frame: Bytes) {
        let Some(broker) = self.holds.broker.clone() else { return; };
        let admission = self.clone();
        self.holds.tasks.spawn(async move {
            if let Ok(record) = admission.current_record().await
                && let Ok(history) = History::of(&record)
            {
                let _ = broker.observe_append(history, position.0, &frame).await;
            }
        });
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
        let record = self.current_record().await?;
        let first_required = replay_start(&record, after.0)?;
        Ok((record, first_required))
    }

    async fn current_record(&self) -> Result<DeploymentRecord> {
        let record = self.holds.directory.deployment(&self.deployment).await.map_err(|error| self.directory_error(error))?;
        record.filter(|record| {
            record.log_epoch == self.log_epoch && record.stream("replica") == self.stream
                && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        }).ok_or_else(|| Error::Closed(self.stream.clone()))
    }

    async fn register(&self, id: Uuid, after: LogOffset, through: LogOffset, attempted: &mut bool) -> Result<History> {
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
                return History::of(&stored).map_err(broker_error);
            }
        }
        Err(Error::Remote { key: self.stream.clone(), message: "The directory kept changing while registering a replay hold".into() })
    }

    fn directory_error(&self, error: Anomaly) -> Error {
        Error::Remote { key: self.stream.clone(), message: error.to_string() }
    }
}

struct DirectoryPayloadBroker {
    client: Option<Client>,
    history: History,
    ticket: Uuid,
    frame: Bytes,
}

#[async_trait::async_trait]
impl ReplicaPayloadBroker for DirectoryPayloadBroker {
    async fn read(&self, custody: &ReplicaObjectCustody, memory: &Arc<MemoryBudget>) -> Result<ReplicaPayload> {
        let client = self.client.as_ref().ok_or_else(no_broker)?;
        client.read(self.history, self.ticket, custody.position().0, &self.frame, memory).await.map_err(broker_error)
    }
}

fn no_broker() -> Error {
    Error::Closed("The detached replay broker is not installed".into())
}

fn broker_error(error: Anomaly) -> Error {
    match error.code.as_str() {
        "rtdb/erased-under-view" => Error::ErasedUnderView,
        "rtdb.multi/replay-broker-refused" => Error::Closed("The detached replay route closed".into()),
        _ => Error::Remote { key: "detached replay broker".into(), message: error.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_payload_read_spends_its_directory_replay_hold() {
        crate::residence_trace::install_from_env();
        use crate::{deployment::CellStorage, keys::LocalKeyService, lease::LeaseOptions,
            logs::JournalUnits, process, replay_broker::Client};
        use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, keys::oversized_key, log::proto::NoOp};
        use rtdb_storage::{IoRegion, MemoryStore, log::Log};
        use rtdb_types::{BlockIndex, Term, TermSequence, Timestamp};
        use tokio_util::sync::CancellationToken;

        let data = tempfile::tempdir().unwrap();
        let directory = rtdb_directory::MemoryDirectory::new();
        let journal = process::start_logkeeper(directory.clone(), process::LogkeeperSettings {
            authority: None, advertise_host: None, listen: "127.0.0.1:0".parse().unwrap(),
            data_dir: data.path().join("journal"), fsync: true, cache_bytes: 1 << 20,
            availability_zone: "local".into(), lease: LeaseOptions::default(),
        }).await.unwrap();
        let mut record = crate::admin::create_deployment(directory.as_ref(), "shop", "acme", 1).await.unwrap();
        record.holder = Some(rtdb_directory::MemberId("writer".into()));
        record.epoch = 1;
        let version = record.version;
        let Cas::Applied(record) = directory.replace_deployment(record, version).await.unwrap() else {
            panic!("the writer should be admitted");
        };
        let stream = crate::admin::administration_stream(directory.as_ref(), &record, process::MAXIMUM_LOG_MESSAGE_BYTES).await.unwrap();
        let shutdown = CancellationToken::new();
        let writer = rtdb_logkeeper::open_writer(stream, record.epoch, shutdown.clone()).await.unwrap();
        let tasks = TaskTracker::new();
        let serving = CellStorage::Local { root: data.path().join("serving") };
        let detached = CellStorage::Local { root: data.path().join("detached") };
        let keys = Arc::new(LocalKeyService::new(data.path().join("journal-keys")).unwrap());
        let broker = Client::standalone(directory.clone(), detached, &serving,
            Arc::new(LocalKeyService::new(data.path().join("detached-keys")).unwrap()), tasks.clone()).unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(), keys: keys.clone(), custody: ReplayCustody::Local, broker: Some(broker), tasks: tasks.clone(),
        });
        let admission = RecoveryAdmission {
            holds, deployment: record.id.clone(), stream: record.stream("replica"),
            log_epoch: record.log_epoch, writer_epoch: record.epoch,
        };
        let body = ReplicaEnvelope {
            term: Term(1), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(NoOp::default()),
        };
        let payload = Bytes::from(body.encode());
        let allocation = Uuid::new_v4().to_string();
        let key = ObjectKey::new(oversized_key(BlockIndex(0), &allocation).unwrap()).unwrap();
        assert!(admission.publish(&key, payload.clone()).await.unwrap().is_none());
        let reference = Bytes::from(body.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(record.log_epoch),
            BlockIndex(0), allocation, &payload).encode());
        let io = IoRegion::new();
        keys.key(&record.key_id()).await.unwrap();
        let journal_keys = JournalUnits::open(keys.as_ref(), &record.key_id(), "replica", io.clone()).await.unwrap();
        let sealed = journal_keys.seed(0, reference.clone()).await.unwrap();
        let committed = writer.enqueue(sealed, Timestamp(0)).await.unwrap().durable().await.unwrap();
        let hold = Arc::new(admission.hold(LogOffset(-1), committed.offset).await.unwrap().unwrap());
        let mut delivery = LogRecord::new(committed, reference);
        hold.retain_record(&mut delivery).unwrap();
        let returned = delivery.replica_objects.as_ref().unwrap().redeem(&MemoryStore::default(), &MemoryBudget::new(1 << 20)).await.unwrap();
        assert_eq!(returned.as_ref(), payload.as_ref());
        drop((delivery, hold));
        tasks.close();
        tasks.wait().await;
        io.close_and_wait().await;
        shutdown.cancel();
        journal.stop().await.unwrap();
    }

    #[tokio::test]
    async fn a_directory_payload_cannot_fall_back_to_ordinary_serving_storage() {
        use rtdb_format::{envelope::{ReplicaBody, ReplicaEnvelope}, log::proto::NoOp};
        use rtdb_storage::{MemoryStore, ObjectStore, log::LogMetadata};
        use rtdb_types::{BlockIndex, Term, TermSequence, Timestamp};

        let vault = tempfile::tempdir().unwrap();
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let holds = Arc::new(RecoveryHolds {
            directory: directory.clone(),
            keys: Arc::new(crate::keys::LocalKeyService::new(vault.path()).unwrap()),
            custody: ReplayCustody::Local,
            broker: None,
            tasks: TaskTracker::new(),
        });
        let admission = RecoveryAdmission {
            holds: holds.clone(), deployment: record.id.clone(), stream: record.stream("replica"),
            log_epoch: record.log_epoch, writer_epoch: record.epoch,
        };
        let envelope = ReplicaEnvelope {
            term: Term(1), term_sequence: Some(TermSequence(0)),
            message: ReplicaBody::NoOp(NoOp::default()),
        };
        let payload = Bytes::from(envelope.encode());
        let epoch = StorageEpoch(0);
        let frame = envelope.offloaded(CURRENT_STORAGE_VERSION, epoch, BlockIndex(0), Uuid::new_v4().to_string(), &payload);
        let name = ObjectKey::new(frame.oversized_payload(CURRENT_STORAGE_VERSION, epoch).unwrap().unwrap()).unwrap();
        let store = MemoryStore::default();
        store.put(&name, payload.clone()).await.unwrap();
        let hold = Arc::new(admission.hold(LogOffset(0), LogOffset(2)).await.unwrap().unwrap());
        let mut delivery = LogRecord::new(LogMetadata { offset: LogOffset(2), timestamp: Timestamp(0) }, frame.encode().into());
        hold.retain_record(&mut delivery).unwrap();
        let proof = delivery.replica_objects.as_ref().unwrap();
        let memory = MemoryBudget::new(1 << 20);

        assert!(matches!(proof.redeem(&store, &memory).await, Err(Error::Closed(_))));
        assert!(matches!(admission.publish(&name, payload).await, Err(Error::Closed(_))));
        drop((delivery, hold));
        holds.tasks.close();
        holds.tasks.wait().await;
    }

    #[tokio::test]
    async fn a_delivered_reference_keeps_its_replay_hold_through_provider_drain() {
        let (release, closed) = oneshot::channel();
        let hold = Arc::new(ReplayHold {
            through: LogOffset(3),
            client: None,
            history: History { deployment: Uuid::new_v4(), incarnation: 1, log_epoch: 0, storage_generation: None },
            ticket: Uuid::new_v4(),
            _release: release,
        });
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
