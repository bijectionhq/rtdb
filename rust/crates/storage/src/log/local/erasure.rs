use serde::{Deserialize, Serialize};

use super::*;
use crate::log::{CheckpointKeyRetirement, CheckpointPrefix};

const BATCH: usize = 128;

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Intent {
    retirement: CheckpointPrefix,
    before: u64,
    settled: u64,
}

impl LocalLog {
    /// Checks the exact durable boundary while leaving all existing keys readable.
    pub async fn validate_erasure_prefix(&self, retirement: CheckpointPrefix) -> Result<()> {
        self.with_retirement_file(retirement, |_, _, _| Ok(()))
            .await
    }

    /// Closes key admission through the exact ordered checkpoint cut and settles one batch.
    /// The caller retains independent recovery and has durably closed prior recovery admission.
    pub async fn settle_erasure_prefix(&self, retirement: CheckpointPrefix) -> Result<bool> {
        let maximum = physical_maximum(self.maximum_message_bytes, true)?;
        self.with_retirement_file(retirement, move |path, keys, (mut file, before)| {
            keys.with_local_prefix_retirement(|intent_path| {
                let mut intent = prepare(intent_path, retirement, before)?;
                for _ in 0..BATCH {
                    if intent.settled == before {
                        break;
                    }
                    let header =
                        read_record_header_at(&mut file, path, maximum, intent.settled, before)?;
                    let next =
                        intent.settled + HEADER_BYTES + header.payload_size as u64 + TRAILER_BYTES;
                    if !header.sealed {
                        return Err(corrupt(
                            path,
                            intent.settled,
                            "unsealed frame in the erasure prefix",
                        ));
                    }
                    if !header.retired {
                        let mut sealed = vec![0; header.payload_size];
                        file.read_exact(&mut sealed)
                            .map_err(|error| Error::io(path, error))?;
                        read_record_trailer(&mut file, path, header.metadata.offset)?;
                        keys.retire_record(
                            header.metadata.offset.0,
                            header.metadata.timestamp.0,
                            &sealed,
                        )?;
                    }
                    intent.settled = next;
                }
                write_intent(intent_path, intent)?;
                Ok(intent.settled == before)
            })
        })
        .await
    }

    async fn with_retirement_file<T: Send + 'static>(
        &self,
        retirement: CheckpointPrefix,
        work: impl FnOnce(&Path, &ClassicObjectKeys, (JournalFile, u64)) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let latest = self.progress.latest()?;
        if retirement.through < 0 || retirement.through > latest.0 {
            return Err(invalid());
        }
        let keys = self.keys.clone().ok_or_else(invalid)?;
        let maximum = physical_maximum(self.maximum_message_bytes, true)?;
        let lineage = self.lineage;
        self.reads
            .read(self.path.clone(), retirement, move |path, retirement| {
                let mut file = open_at(path, lineage, 0)?;
                let before = prefix_end(
                    &mut file,
                    path,
                    maximum,
                    LogOffset(retirement.through),
                    true,
                )?;
                work(path, keys.as_ref(), (file, before))
            })
            .await
    }
}

#[async_trait]
impl CheckpointKeyRetirement for LocalLog {
    async fn settled_local_checkpoint_prefix(&self) -> rtdb_types::Result<Option<(CheckpointPrefix, LogOffset)>> {
        let keys = self.keys.clone().ok_or_else(invalid)?;
        Ok(self.reads.run_blocking(move || keys.with_local_prefix_retirement(|path| {
            let Some(intent) = read_intent(path)? else { return Ok(None); };
            if intent.settled != intent.before { return Err(invalid()); }
            Ok(Some((intent.retirement, LogOffset(intent.before as i64))))
        })).await?)
    }

    async fn validate_checkpoint_prefix(&self, cut: CheckpointPrefix) -> rtdb_types::Result<()> {
        Ok(self.validate_erasure_prefix(cut).await?)
    }

    async fn settle_checkpoint_prefix(&self, cut: CheckpointPrefix) -> rtdb_types::Result<bool> {
        Ok(self.settle_erasure_prefix(cut).await?)
    }
}

pub(crate) fn read_floor(path: &Path) -> Result<i64> {
    Ok(read_intent(path)?.map_or(0, |intent| intent.before as i64))
}

fn prepare(path: &Path, retirement: CheckpointPrefix, before: u64) -> Result<Intent> {
    let settled = match read_intent(path)? {
        Some(prior) if prior.retirement == retirement && prior.before == before => {
            crate::sync_directory(path.parent().ok_or_else(invalid)?)
                .map_err(|error| Error::io(path, error))?;
            return Ok(prior);
        }
        Some(prior) if prior.settled == prior.before && prior.before < before => prior.before,
        Some(_) => return Err(invalid()),
        None => 0,
    };
    let intent = Intent {
        retirement,
        before,
        settled,
    };
    write_intent(path, intent)?;
    Ok(intent)
}

fn read_intent(path: &Path) -> Result<Option<Intent>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(path, error)),
    };
    let intent: Intent = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if intent.retirement.through < 0
        || intent.retirement.through as u64 >= intent.before
        || intent.before >= 1 << 48
        || intent.settled > intent.before
    {
        return Err(invalid());
    }
    Ok(Some(intent))
}

fn write_intent(path: &Path, intent: Intent) -> Result<()> {
    let dir = path.parent().ok_or_else(invalid)?;
    let mut file = tempfile::NamedTempFile::new_in(dir).map_err(|error| Error::io(path, error))?;
    serde_json::to_writer(&mut file, &intent).map_err(|_| invalid())?;
    crate::sync_file(file.as_file()).map_err(|error| Error::io(path, error))?;
    file.persist(path)
        .map_err(|error| Error::io(path, error.error))?;
    crate::sync_directory(dir).map_err(|error| Error::io(dir, error))
}

fn invalid() -> Error {
    Error::Task("The sealed Local log has no exact checkpoint retirement cut".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_live_sealed_log_reopens_after_the_inclusive_erasure_cut() -> rtdb_types::Result<()>
    {
        let files = tempfile::tempdir().unwrap();
        let keys = crate::sealing::private_tempdir();
        let vault = ClassicKeyVault::open(keys.path())?;
        let path = files.path().join("REPLICA_LOG");
        let io = IoRegion::new();
        let open = |enroll| {
            let slots = vault.log_keys(&path, enroll, Arc::clone(&io))?;
            Ok::<_, rtdb_types::Anomaly>(LocalLog::open_replica(
                &path,
                LocalLogOptions {
                    maximum_message_bytes: 1024,
                    queue_capacity: 2,
                },
                Arc::new(SuppliedTimestamp),
                Arc::clone(&io),
                Some(slots),
            )?)
        };
        let (log, driver) = open(true)?;
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(driver.run(shutdown.clone()));
        let old = Bytes::from_static(b"old");
        let live = Bytes::from_static(b"surviving suffix");
        let through = log
            .enqueue(old, Timestamp(1))
            .await?
            .durable()
            .await?
            .offset;
        let suffix = log
            .enqueue(live.clone(), Timestamp(2))
            .await?
            .durable()
            .await?
            .offset;
        let mut stale = log.tail(LogOffset(-1)).await?;
        let cut = CheckpointPrefix {
            through: through.0,
            checkpoint_sha256: [3; 32],
            unreadable_by: Timestamp(8),
        };
        log.validate_erasure_prefix(cut).await?;
        assert!(log.settle_erasure_prefix(cut).await?);
        assert!(matches!(stale.poll(Duration::ZERO).await,
            Err(Error::UnavailableLogRange { requested, first_retained }) if requested == through.0 && first_retained == suffix.0));
        assert!(matches!(
            log.tail(LogOffset(-1)).await,
            Err(Error::UnavailableLogRange { .. })
        ));
        let before = fs::read(&path).unwrap();
        assert!(log.settle_erasure_prefix(cut).await?);
        assert_eq!(fs::read(&path).unwrap(), before);
        shutdown.cancel();
        writer.await.unwrap()?;
        let (reopened, _driver) = open(false)?;
        let rows = reopened.tail(through).await?.poll(Duration::ZERO).await?;
        assert_eq!(rows[0].metadata.offset, suffix);
        assert_eq!(rows[0].payload, live);
        assert!(
            reopened
                .settle_erasure_prefix(CheckpointPrefix {
                    unreadable_by: Timestamp(9),
                    ..cut
                })
                .await
                .is_err()
        );
        Ok(())
    }
}
