use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, ReplayCustody, StorageRegion};
use rtdb_storage::{Error, Result, log::LogOffset};
use rtdb_types::Anomaly;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::deployment::LogkeeperLogConfig;
use crate::keys::KeyService;
use crate::hosted_recovery::replay_start;

pub(crate) struct RecoveryHolds {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub custody: ReplayCustody,
    pub tasks: TaskTracker,
}

#[derive(Clone)]
pub(crate) struct RecoveryAdmission {
    holds: Arc<RecoveryHolds>,
    deployment: String,
    stream: String,
    log_epoch: i32,
}

pub(crate) struct ReplayHold {
    pub through: LogOffset,
    _release: oneshot::Sender<()>,
}

impl RecoveryAdmission {
    pub fn new(holds: Arc<RecoveryHolds>, config: &LogkeeperLogConfig) -> Self {
        Self { holds, deployment: config.deployment.clone(), stream: config.replica_stream.clone(), log_epoch: config.log_epoch }
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
            let outcome = worker.register(id, after, &mut attempted).await;
            let opened = outcome.is_ok();
            if ready.send(outcome).is_ok() && opened {
                let _ = closed.await;
            }
            if attempted {
                loop {
                    match crate::store::release_region(worker.holds.directory.as_ref(), &worker.deployment, id, !opened).await {
                        Ok(()) => break,
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
        admitted.await.map_err(|_| Error::Closed(self.stream.clone()))??;
        Ok(Some(ReplayHold { through, _release: release }))
    }

    async fn check_start(&self, after: LogOffset) -> Result<()> {
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

    async fn register(&self, id: Uuid, after: LogOffset, attempted: &mut bool) -> Result<()> {
        for _ in 0..8 {
            let (mut record, first_required) = self.checked_record(after).await?;
            let version = record.version;
            record.storage_regions.insert(id, StorageRegion::ReplicaReplay { first_required, custody: self.holds.custody.clone() });
            *attempted = true;
            if let Cas::Applied(_) = self.holds.directory.replace_deployment(record, version).await.map_err(|error| self.directory_error(error))? {
                return Ok(());
            }
        }
        Err(Error::Remote { key: self.stream.clone(), message: "The directory kept changing while registering a replay hold".into() })
    }

    fn directory_error(&self, error: Anomaly) -> Error {
        Error::Remote { key: self.stream.clone(), message: error.to_string() }
    }
}
