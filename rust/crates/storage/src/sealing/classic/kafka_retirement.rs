use rtdb_types::Timestamp;

use super::*;

const BATCH: i64 = 128;
const INTENT: &str = "native-prefix-retirement";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaPrefixRetirement {
    pub before: i64,
    pub checkpoint_sha256: [u8; 32],
    pub unreadable_by: Timestamp,
}

#[derive(Clone, Copy)]
struct Intent {
    retirement: KafkaPrefixRetirement,
    settled_before: i64,
}

impl Intent {
    fn read(path: &Path) -> crate::Result<Option<Self>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io(path)(error)),
        };
        let bytes: [u8; 57] = bytes.try_into().map_err(|_| invalid_intent())?;
        if bytes[0] != 1 {
            return Err(invalid_intent());
        }
        let before = i64::from_be_bytes(bytes[1..9].try_into().expect("fixed field"));
        let settled_before = i64::from_be_bytes(bytes[9..17].try_into().expect("fixed field"));
        let unreadable_by = Timestamp(i64::from_be_bytes(
            bytes[17..25].try_into().expect("fixed field"),
        ));
        let checkpoint_sha256 = bytes[25..].try_into().expect("fixed field");
        if before <= 0 || !(0..=before).contains(&settled_before) {
            return Err(invalid_intent());
        }
        Ok(Some(Self {
            retirement: KafkaPrefixRetirement {
                before,
                checkpoint_sha256,
                unreadable_by,
            },
            settled_before,
        }))
    }

    fn write(self, path: &Path) -> crate::Result<()> {
        let mut bytes = [0; 57];
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&self.retirement.before.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.settled_before.to_be_bytes());
        bytes[17..25].copy_from_slice(&self.retirement.unreadable_by.0.to_be_bytes());
        bytes[25..].copy_from_slice(&self.retirement.checkpoint_sha256);
        write_kafka_marker(path, &bytes, true)
    }
}

impl KafkaFrameKeys {
    /// Returns the exclusive key admission floor established by ordered native retirement.
    pub fn kafka_erasure_floor(&self) -> crate::Result<i64> {
        Ok(Intent::read(&self.0.path.join(INTENT))?.map_or(0, |intent| intent.retirement.before))
    }

    pub fn kafka_classic_retirement_floor(&self) -> crate::Result<i64> {
        kafka_retired_before(&self.0.path.join("retired-before"))
    }

    pub async fn has_kafka_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
    ) -> crate::Result<bool> {
        let path = self.0.path.join(INTENT);
        self.0
            .region
            .run_blocking(move || {
                Ok(Intent::read(&path)?.is_some_and(|intent| intent.retirement == retirement))
            })
            .await
    }

    /// Closes new decryptions below a checkpoint-authorized cut and retains its receipt deadline.
    /// The caller must first order recovery from the exact checkpoint on this topic lineage.
    /// `broker_end` is the exclusive end of the same broker partition's observed committed prefix.
    pub async fn arm_kafka_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
        broker_end: i64,
    ) -> crate::Result<()> {
        let path = Arc::clone(&self.0.path);
        self.0
            .region
            .run_blocking(move || {
                let classic = path.join("retired-before");
                let _gate = lock(&classic)?;
                let intent = prepare(
                    &path,
                    kafka_retired_before(&classic)?,
                    retirement,
                    broker_end,
                )?;
                intent.write(&path.join(INTENT))
            })
            .await
    }

    /// Validates every retiring slot and its placement relative to the observed suffix.
    /// Staging slot identities does not close decryptions before the floor is armed.
    pub async fn validate_kafka_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
        broker_end: i64,
    ) -> crate::Result<()> {
        let path = Arc::clone(&self.0.path);
        self.0
            .region
            .run_blocking(move || {
                let classic = path.join("retired-before");
                let _gate = lock(&classic)?;
                prepare(
                    &path,
                    kafka_retired_before(&classic)?,
                    retirement,
                    broker_end,
                )?;
                Ok(())
            })
            .await
    }

    /// Withdraws up to one bounded batch of slots from the exact durable intent.
    /// The broker-retention owner keeps its own floor and position census.
    pub async fn settle_kafka_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
    ) -> crate::Result<bool> {
        let keys = ClassicObjectKeys {
            path: Arc::clone(&self.0.path),
            region: Arc::clone(&self.0.region),
        };
        self.0
            .region
            .run_blocking(move || {
                let classic_floor = keys.path.join("retired-before");
                let _gate = lock(&classic_floor)?;
                let intent_path = keys.path.join(INTENT);
                let mut intent = Intent::read(&intent_path)?
                    .filter(|intent| intent.retirement == retirement)
                    .ok_or_else(invalid_intent)?;
                let classic_floor = kafka_retired_before(&classic_floor)?;
                intent.settled_before = intent
                    .settled_before
                    .max(classic_floor.min(retirement.before));
                let census = keys.path.join("positions");
                let stop = retirement
                    .before
                    .min(intent.settled_before.saturating_add(BATCH));
                for offset in intent.settled_before..stop {
                    let publication = publication_at(&census, offset)?;
                    withdraw_slot(&keys.name(&ObjectKey::new(format!("frames/{publication}"))?))?;
                }
                intent.settled_before = stop;
                intent.write(&intent_path)?;
                Ok(stop == retirement.before)
            })
            .await
    }

    pub(super) fn check_retiring_slot(&self, publication: Uuid, floor: i64) -> crate::Result<()> {
        if floor == 0 {
            return Ok(());
        }
        let marker = retirement_slot(&self.0.path.join("positions"), publication);
        if retirement_offset(&marker)?.is_some_and(|offset| offset < floor) {
            return Err(Error::ErasedUnderView);
        }
        Ok(())
    }
}

fn prepare(
    path: &Path,
    classic_floor: i64,
    retirement: KafkaPrefixRetirement,
    broker_end: i64,
) -> crate::Result<Intent> {
    if retirement.before <= 0 || retirement.before > broker_end {
        return Err(invalid_intent());
    }
    let mut settled_before = classic_floor.min(retirement.before);
    if let Some(prior) = Intent::read(&path.join(INTENT))? {
        if prior.retirement == retirement {
            return Ok(prior);
        }
        if prior.retirement.before >= retirement.before
            || prior.settled_before < prior.retirement.before
        {
            return Err(invalid_intent());
        }
        settled_before = settled_before.max(prior.settled_before);
    }
    if settled_before > retirement.before {
        return Err(invalid_intent());
    }
    let census = path.join("positions");
    for offset in settled_before..retirement.before {
        let marker = retirement_slot(&census, publication_at(&census, offset)?);
        match retirement_offset(&marker)? {
            Some(prior) if prior == offset => (),
            Some(_) => return Err(aliased_publication()),
            None => write_kafka_marker(&marker, &offset.to_be_bytes(), false)?,
        }
    }
    for offset in retirement.before..broker_end {
        let marker = retirement_slot(&census, publication_at(&census, offset)?);
        if retirement_offset(&marker)?.is_some_and(|prior| prior < retirement.before) {
            return Err(aliased_publication());
        }
    }
    Ok(Intent {
        retirement,
        settled_before,
    })
}

fn retirement_slot(census: &Path, publication: Uuid) -> PathBuf {
    census.join(format!("retiring-{publication}"))
}

fn retirement_offset(marker: &Path) -> crate::Result<Option<i64>> {
    match fs::read(marker) {
        Ok(bytes) => {
            let offset = i64::from_be_bytes(bytes.try_into().map_err(|_| invalid_intent())?);
            if offset < 0 {
                return Err(invalid_intent());
            }
            Ok(Some(offset))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(marker)(error)),
    }
}

pub(super) fn publication_at(census: &Path, offset: i64) -> crate::Result<Uuid> {
    let path = census.join(offset.to_string());
    let bytes = fs::read(&path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            Error::Task(format!(
                "Kafka offset {offset} has no exact sealed publication"
            ))
        } else {
            io(&path)(error)
        }
    })?;
    let bytes: [u8; 48] = bytes.try_into().map_err(|_| invalid_intent())?;
    let publication = Uuid::from_bytes(bytes[..16].try_into().expect("fixed field"));
    if publication.is_nil() {
        return Err(invalid_intent());
    }
    Ok(publication)
}

fn invalid_intent() -> Error {
    Error::Task("Invalid Kafka checkpoint retirement intent".into())
}

fn aliased_publication() -> Error {
    Error::Task("A retiring Kafka key slot is shared by multiple offsets".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cut(before: i64) -> KafkaPrefixRetirement {
        KafkaPrefixRetirement {
            before,
            checkpoint_sha256: [2; 32],
            unreadable_by: Timestamp(100),
        }
    }

    #[tokio::test]
    async fn an_armed_prefix_denies_replay_across_restart_and_retains_the_suffix() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = ClassicKeyVault::open(directory.path())?;
        let topic = Uuid::new_v4().into_bytes();
        let keys = vault.kafka_log_keys(topic, true, IoRegion::new())?;
        let earlier = keys.seal_kafka_record(Bytes::from_static(b"old")).await?;
        let later = keys.seal_kafka_record(Bytes::from_static(b"live")).await?;
        keys.record_kafka_publication(0, &earlier)?;
        assert!(keys.arm_kafka_prefix_retirement(cut(1), 2).await.is_err());
        assert_eq!(keys.kafka_erasure_floor()?, 0);
        keys.record_kafka_publication(1, &later)?;
        keys.validate_kafka_prefix_retirement(cut(1), 2).await?;
        assert_eq!(keys.kafka_erasure_floor()?, 0);
        assert_eq!(
            keys.open_kafka_publication(0, earlier.clone()).await?,
            Bytes::from_static(b"old")
        );
        keys.arm_kafka_prefix_retirement(cut(1), 2).await?;
        let keys = vault.kafka_log_keys(topic, false, IoRegion::new())?;
        assert_eq!(keys.kafka_erasure_floor()?, 1);
        assert!(keys.has_kafka_prefix_retirement(cut(1)).await?);
        assert!(
            !keys
                .has_kafka_prefix_retirement(KafkaPrefixRetirement {
                    unreadable_by: Timestamp(101),
                    ..cut(1)
                })
                .await?
        );
        assert!(matches!(
            keys.open_kafka_publication(0, earlier.clone()).await,
            Err(Error::UnavailableLogRange {
                requested: 0,
                first_retained: 1
            })
        ));
        assert!(matches!(
            keys.open_kafka_publication(2, earlier.clone()).await,
            Err(Error::ErasedUnderView)
        ));
        assert!(matches!(
            keys.record_kafka_publication(2, &earlier),
            Err(Error::ErasedUnderView)
        ));
        keys.record_kafka_publication(0, &earlier)?;
        assert_eq!(
            keys.open_kafka_publication(1, later).await?,
            Bytes::from_static(b"live")
        );
        assert!(
            keys.arm_kafka_prefix_retirement(
                KafkaPrefixRetirement {
                    unreadable_by: Timestamp(101),
                    ..cut(1)
                },
                2
            )
            .await
            .is_err()
        );
        assert!(keys.settle_kafka_prefix_retirement(cut(1)).await?);
        assert!(keys.settle_kafka_prefix_retirement(cut(1)).await?);
        assert!(matches!(
            keys.open_kafka_record(&earlier),
            Err(Error::ErasedUnderView)
        ));
        assert!(keys.retire_kafka_before(0).await?);
        assert!(keys.retire_kafka_before(1).await?);
        Ok(())
    }

    #[tokio::test]
    async fn a_slot_aliased_by_the_preserved_suffix_prevents_retirement() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = ClassicKeyVault::open(directory.path())?;
        let keys = vault.kafka_log_keys(Uuid::new_v4().into_bytes(), true, IoRegion::new())?;
        let publication = keys
            .seal_kafka_record(Bytes::from_static(b"shared"))
            .await?;
        keys.record_kafka_publication(0, &publication)?;
        keys.record_kafka_publication(1, &publication)?;
        assert!(keys.arm_kafka_prefix_retirement(cut(1), 2).await.is_err());
        assert_eq!(keys.kafka_erasure_floor()?, 0);
        Ok(())
    }
}
