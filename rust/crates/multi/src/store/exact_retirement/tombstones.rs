use rtdb_directory::{Directory, LineageChange};
use rtdb_s3::{NeverVersionedObject, NeverVersionedRetirement};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, Result, S3TombstoneTarget};
use uuid::Uuid;

pub(in crate::store) struct TombstoneJournal<'a> {
    directory: &'a dyn Directory,
    lineage: &'a str,
    token: Uuid,
}

impl<'a> TombstoneJournal<'a> {
    pub(in crate::store) fn new(directory: &'a dyn Directory, lineage: &'a str, token: Uuid) -> Self {
        Self { directory, lineage, token }
    }

    pub(super) fn token(&self) -> Uuid { self.token }

    pub(super) async fn upload(&self, key: &ObjectKey) -> Result<Option<String>> {
        let state = self.directory.lineage_state(self.lineage).await?;
        Ok(state.tombstone_upload(self.token, key.as_str())?.map(str::to_owned))
    }

    pub(super) async fn admit(&self, key: &ObjectKey, upload: &str) -> Result<()> {
        self.directory.change_lineage(self.lineage, LineageChange::AdmitTombstoneUpload {
            retirement: self.token, key: key.to_string(), upload: upload.to_owned(),
        }).await.map(drop)
    }

    pub(super) async fn settle(&self, key: &ObjectKey, upload: &str) -> Result<()> {
        self.directory.change_lineage(self.lineage, LineageChange::SettleTombstoneUpload {
            retirement: self.token, key: key.to_string(), upload: upload.to_owned(),
        }).await.map(drop)
    }
}

pub(in crate::store) async fn retire(provider: &NeverVersionedRetirement<'_>, journal: &TombstoneJournal<'_>,
    key: &ObjectKey, target: &S3TombstoneTarget) -> Result<()> {
    let retained = journal.upload(key).await?;
    let observed = provider.observe(key).await?;
    let should_write = match observed {
        NeverVersionedObject::Retired(token) if token == journal.token() => false,
        NeverVersionedObject::Open(current) if current == *target => true,
        _ => {
            if let Some(upload) = retained {
                provider.drain(key, target, &upload).await?;
                journal.settle(key, &upload).await?;
            }
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/tombstone-target-changed", "The current object differs from the retired publication"));
        }
    };
    let upload = match retained {
        Some(upload) => Some(upload),
        None if should_write => {
            let upload = provider.reserve(key, target, journal.token()).await?;
            if let Err(error) = journal.admit(key, &upload).await {
                provider.drain(key, target, &upload).await?;
                return Err(error);
            }
            Some(upload)
        }
        None => None,
    };
    let mut completion = Ok(());
    if let Some(upload) = upload {
        if should_write { completion = provider.complete(key, target, &upload).await; }
        provider.drain(key, target, &upload).await?;
        journal.settle(key, &upload).await?;
    }
    if provider.confirms(key, journal.token()).await? { return Ok(()); }
    completion?;
    Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/tombstone-pending", "The retired key has no confirmed tombstone"))
}
