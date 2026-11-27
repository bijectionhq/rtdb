use serde::{Deserialize, Serialize};
use uuid::Uuid;

use rtdb_storage::{Error, ObjectKey, remote::PublicationJournal};

use super::{ByteStream, ProvideErrorMetadata, Result, VersionedInventory, strong_entity_tag, unavailable};

const ATTEMPTS: usize = 12;
const MAX_RECORD_BYTES: usize = 8192;

#[derive(Clone)]
pub struct StagingUploads {
    provider: VersionedInventory,
    control: String,
    candidate: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    owner: Option<Uuid>,
    active: Option<Upload>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Upload {
    key: String,
    attempt: Uuid,
    id: String,
}

struct StagingWriter {
    scope: StagingUploads,
    owner: Uuid,
}

impl VersionedInventory {
    pub fn staging_uploads(&self, authority: Uuid, candidate: Uuid) -> Result<StagingUploads> {
        if authority.is_nil() || candidate.is_nil() || self.terminal_allocation.is_some() {
            return Err(invalid());
        }
        let key = ObjectKey::new(format!("recovery-custody/{authority}/{candidate}/staging-uploads"))?;
        Ok(StagingUploads {
            provider: self.clone(),
            control: self.object_path(&key)?.to_string(),
            candidate,
        })
    }
}

impl StagingUploads {
    pub async fn begin(&self) -> Result<Option<std::sync::Arc<dyn PublicationJournal>>> {
        let owner = Uuid::new_v4();
        if !self.change_owner(Some(owner)).await? {
            return Ok(None);
        }
        Ok(Some(std::sync::Arc::new(StagingWriter { scope: self.clone(), owner })))
    }

    pub async fn close(&self) -> Result<()> {
        self.change_owner(None).await.map(drop)
    }

    pub async fn require_closed(&self) -> Result<()> {
        if matches!(self.read().await?, Some((Ledger { owner: None, active: None }, _))) {
            return Ok(());
        }
        Err(invalid())
    }

    pub async fn purge_abandoned(&self) -> Result<()> {
        self.require_closed().await?;
        self.provider.purge(&ObjectKey::new(format!("db/{}", self.candidate))?).await?;
        self.provider.purge(&ObjectKey::new(format!("~rtdb-v1/{}/keys", self.candidate.simple()))?).await
    }

    async fn change_owner(&self, owner: Option<Uuid>) -> Result<bool> {
        self.provider.require_versioned().await?;
        for _ in 0..ATTEMPTS {
            let (active, tag) = match self.read().await? {
                Some((observed, tag)) => {
                    if observed.owner.is_none() {
                        if owner.is_none() {
                            self.drain(observed.active).await?;
                        }
                        return Ok(false);
                    }
                    (observed.active, Some(tag))
                }
                None => (None, None),
            };
            let next = Ledger { owner, active };
            if self.write(&next, tag.as_deref()).await? {
                self.drain(next.active).await?;
                return Ok(true);
            }
        }
        Err(contention())
    }

    async fn drain(&self, active: Option<Upload>) -> Result<()> {
        if let Some(upload) = active {
            let key = self.validate(&upload)?;
            self.provider.abort_publication(&key, &upload.id).await?;
            self.clear(&upload).await?;
        }
        Ok(())
    }

    async fn admit(&self, owner: Uuid, upload: Upload) -> Result<()> {
        self.validate(&upload)?;
        for _ in 0..ATTEMPTS {
            let (mut state, tag) = self.read().await?.ok_or_else(invalid)?;
            if state.owner != Some(owner) {
                return Err(invalid());
            }
            if let Some(active) = state.active {
                return if active == upload { Ok(()) } else { Err(contention()) };
            }
            state.active = Some(upload.clone());
            if self.write(&state, Some(&tag)).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    async fn clear(&self, upload: &Upload) -> Result<()> {
        self.validate(upload)?;
        for _ in 0..ATTEMPTS {
            let Some((mut state, tag)) = self.read().await? else { return Ok(()); };
            if state.active.as_ref() != Some(upload) {
                return Ok(());
            }
            state.active = None;
            if self.write(&state, Some(&tag)).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    fn validate(&self, upload: &Upload) -> Result<ObjectKey> {
        let key = ObjectKey::new(upload.key.as_str())?;
        let data = format!("db/{}/", self.candidate);
        let tokens = format!("~rtdb-v1/{}/keys/", self.candidate.simple());
        if !(upload.key.starts_with(&data) || upload.key.starts_with(&tokens))
            || upload.attempt.is_nil() || upload.id.is_empty() || upload.id.len() > 2048
        {
            return Err(invalid());
        }
        Ok(key)
    }

    async fn read(&self) -> Result<Option<(Ledger, String)>> {
        let reply = self.provider.client.get_object()
            .bucket(&self.provider.namespace.bucket).key(&self.control)
            .set_request_payer(self.provider.request_payer.clone()).send().await;
        let mut body = match reply {
            Ok(body) => body,
            Err(error) if error.as_service_error().is_some_and(|error| error.is_no_such_key()) => return Ok(None),
            Err(error) => return Err(unavailable(format!("Reading the staging upload ledger failed: {error}"))),
        };
        let tag = body.e_tag().filter(|tag| strong_entity_tag(tag)).ok_or_else(invalid)?.to_owned();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.body.next().await {
            let chunk = chunk.map_err(|error| unavailable(format!("Reading the staging upload record failed: {error}")))?;
            if chunk.len() > MAX_RECORD_BYTES - bytes.len() {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        let state: Ledger = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if state.owner.is_some_and(|owner| owner.is_nil()) {
            return Err(invalid());
        }
        if let Some(upload) = &state.active {
            self.validate(upload)?;
        }
        Ok(Some((state, tag)))
    }

    async fn write(&self, state: &Ledger, tag: Option<&str>) -> Result<bool> {
        let encoded = serde_json::to_vec(state).map_err(|_| invalid())?;
        if encoded.len() > MAX_RECORD_BYTES { return Err(invalid()); }
        let put = self.provider.client.put_object()
            .bucket(&self.provider.namespace.bucket).key(&self.control)
            .set_request_payer(self.provider.request_payer.clone()).body(ByteStream::from(encoded));
        let reply = match tag {
            Some(tag) => put.if_match(tag),
            None => put.if_none_match("*"),
        }.send().await;
        match reply {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().and_then(|error| error.code()).is_some_and(|code|
                matches!(code, "PreconditionFailed" | "ConditionalRequestConflict")) => Ok(false),
            Err(error) => Err(unavailable(format!("Updating the staging upload ledger failed: {error}"))),
        }
    }
}

#[async_trait::async_trait]
impl PublicationJournal for StagingWriter {
    async fn admit(&self, key: &ObjectKey, attempt: Uuid, upload: &str) -> rtdb_storage::Result<()> {
        self.scope.admit(self.owner, Upload { key: key.to_string(), attempt, id: upload.to_owned() }).await
            .map_err(|error| Error::Closed(error.to_string()))
    }

    async fn settle(&self, key: &ObjectKey, attempt: Uuid, upload: &str, _: bool) -> rtdb_storage::Result<()> {
        self.scope.clear(&Upload { key: key.to_string(), attempt, id: upload.to_owned() }).await
            .map_err(|error| Error::Closed(error.to_string()))
    }
}

fn invalid() -> rtdb_types::Anomaly {
    unavailable("The staging upload scope is closed or its provider record is invalid")
}

fn contention() -> rtdb_types::Anomaly {
    unavailable("The staging upload scope is busy")
}
