use std::{fmt, fs::File, io::Read, path::Path, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_directory::{ClassicMigrationSource, DeploymentRecord, Directory, Lifecycle, MemberId, classic_flight, now_ms};
use rtdb_flight_sql::{ClassicRetainedAdmission, ClassicRetainedRead};
use rtdb_format::database::proto::database_config;
use rtdb_node::Database;
use rtdb_storage::sealing::DeliveryDeadline;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TransactionKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{archive, capability::attachment_storage, deployment::LogkeeperLogConfig};

pub(crate) struct ReceiverExclusion {
    directory: Arc<dyn Directory>,
    retained_key: Option<[u8; 32]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedScope {
    name: String,
    authority: Uuid,
    capture: Uuid,
    source_manifest_sha256: String,
    holder: MemberId,
    source: ClassicMigrationSource,
    snapshot: TransactionKey,
    begin_sha256: [u8; 32],
    original_catalog_sha256: [u8; 32],
    custody_sha256: [u8; 32],
    application_selection_sha256: [u8; 32],
    retained_path_sha256: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RetainedGrant {
    scope: RetainedScope,
    principal: String,
    action: String,
    body_sha256: [u8; 32],
    expires_at_ms: i64,
}

impl ReceiverExclusion {
    pub(crate) fn new(directory: Arc<dyn Directory>, retained_key: Option<&Path>) -> Result<Self> {
        let retained_key = retained_key.map(read_retained_key).transpose()?;
        Ok(Self { directory, retained_key })
    }
}

pub(crate) fn read_retained_key(path: &Path) -> Result<[u8; 32]> {
    let invalid = || Anomaly::incorrect("rtdb.multi/classic-retained-key",
        "The installed Classic retained Flight verification key is invalid");
    let mut bytes = Vec::new();
    File::open(path).and_then(|file| file.take(256).read_to_end(&mut bytes)).map_err(|_| invalid())?;
    let key: [u8; 32] = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if bytes.len() == 256 || key == [0; 32] { return Err(invalid()); }
    Ok(key)
}

pub(crate) const RETAINED_PROOF_HEADER: &str = "x-rtdb-classic-retained-proof";

pub(crate) fn verify_forwarded_retained(
    key: &[u8; 32], original: &DeploymentRecord, proof: &str, action: &str, body: &[u8],
) -> Result<()> {
    let grant = verify_grant(key, proof, action, archive::sha256(body))?;
    grant.scope.require_current(original)?;
    grant.delivery_deadline()?;
    Ok(())
}

fn verify_grant(key: &[u8; 32], proof: &str, action: &str, body_sha256: [u8; 32]) -> Result<RetainedGrant> {
    if proof.len() > 4608 { return Err(refused()); }
    let (token, signature) = proof.split_once('.').filter(|(_, signature)| signature.len() == 86)
        .ok_or_else(refused)?;
    let token = URL_SAFE_NO_PAD.decode(token).map_err(|_| refused())?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| refused())?;
    let grant: RetainedGrant = serde_json::from_slice(&token).map_err(|_| refused())?;
    UnparsedPublicKey::new(&ED25519, key).verify(
        &archive::encode(&("rtdb.multi/classic-retained-flight-read/v1", &grant))?, &signature,
    ).map_err(|_| refused())?;
    let now = now_ms();
    let scope = &grant.scope;
    if grant.action != action || grant.body_sha256 != body_sha256
        || !(now < grant.expires_at_ms && grant.expires_at_ms <= now.saturating_add(30_000))
        || scope.authority.is_nil() || scope.capture.is_nil()
        || scope.begin_sha256 == [0; 32] || scope.original_catalog_sha256 == [0; 32]
        || scope.custody_sha256 == [0; 32] || scope.application_selection_sha256 == [0; 32]
        || scope.retained_path_sha256 == [0; 32]
    {
        return Err(refused());
    }
    Ok(grant)
}

impl RetainedGrant {
    fn delivery_deadline(&self) -> Result<DeliveryDeadline> {
        u64::try_from(self.expires_at_ms).ok().and_then(|millis| millis.checked_mul(1_000_000))
            .and_then(DeliveryDeadline::from_wire_nanos).ok_or_else(refused)
    }
}

impl RetainedScope {
    fn require_current(&self, record: &DeploymentRecord) -> Result<()> {
        if record.id != self.authority.to_string() || record.name != self.name || record.holder.as_ref() != Some(&self.holder)
            || record.incarnation != self.source.incarnation || record.epoch != self.source.writer_epoch
            || record.log_epoch != self.source.log_epoch || record.lifecycle != Lifecycle::Live
            || record.move_from.is_some() || record.checkpoint_transfer_fence.is_some()
            || record.checkpoint_capture_retirements.contains_key(&self.capture)
            || !record.checkpoint_captures.get(&self.capture).is_some_and(|capture| {
                capture.incarnation == record.incarnation && capture.log_epoch == record.log_epoch
                    && capture.media_bound && capture.remote_disposal.is_none()
                    && capture.manifest_sha256.as_deref() == Some(self.source_manifest_sha256.as_str())
            })
        {
            return Err(refused());
        }
        if classic_flight::commission(record).is_some_and(|commission| {
            commission.intent.capture != self.capture || commission.intent.source_manifest_sha256 != self.source_manifest_sha256
                || commission.begin_sha256 != self.begin_sha256 || commission.original_catalog_sha256 != self.original_catalog_sha256
                || commission.retained_path_sha256 != self.retained_path_sha256
        }) {
            return Err(refused());
        }
        Ok(())
    }
}

impl fmt::Debug for ReceiverExclusion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("ClassicFlightExclusion") }
}

#[async_trait::async_trait]
impl rtdb_flight_sql::ClassicFlightExclusion for ReceiverExclusion {
    async fn excludes_unclassified(&self, attachment: &Database) -> Result<bool> {
        let Some(database_config::Storage::RemoteStorage(remote)) = &attachment.configuration().storage else {
            return Ok(false);
        };
        if !remote.object_store.as_ref().is_some_and(|store| {
            store.type_url.rsplit('/').next() == Some(crate::store::CELL_STORAGE_TYPE)
        }) {
            return Ok(false);
        }
        let storage = attachment_storage(attachment)?;
        let record = self.directory.deployment(&storage.deployment).await?
            .filter(|record| record.name == attachment.name())
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/classic-flight-order",
                "The Classic Flight receiver has no exact Original directory record"))?;
        Ok(classic_flight::excludes_unclassified(&record))
    }

    async fn verify_retained(&self, attachment: &Database, request: ClassicRetainedRead<'_>) -> Result<ClassicRetainedAdmission> {
        let key = self.retained_key.as_ref().ok_or_else(refused)?;
        let grant = verify_grant(key, request.proof, request.action, request.body_sha256)?;
        let scope = &grant.scope;
        if grant.principal != request.principal || scope.name != attachment.name() { return Err(refused()); }
        let storage = attachment_storage(attachment).map_err(|_| refused())?;
        let authority = scope.authority.to_string();
        let Some(database_config::Log::OtherLog(log)) = &attachment.configuration().log else { return Err(refused()); };
        let log: LogkeeperLogConfig = serde_json::from_slice(&log.value).map_err(|_| refused())?;
        let record = self.directory.deployment(&authority).await?.ok_or_else(refused)?;
        scope.require_current(&record)?;
        if storage.deployment != authority || storage.writer_epoch != Some(scope.source.writer_epoch)
            || storage.compactor || storage.snapshot_block.is_some()
            || log.deployment != authority || log.epoch != scope.source.writer_epoch
            || log.log_epoch != scope.source.log_epoch || log.replica_stream != record.stream("replica")
            || record.storage_prefix() != storage.prefix
        {
            return Err(refused());
        }
        let deadline = grant.delivery_deadline()?;
        let snapshot = scope.snapshot;
        let scope_sha256 = archive::sha256(&archive::encode(&("rtdb.multi/classic-retained-flight-scope/v1", &grant.principal, scope))?);
        Ok(ClassicRetainedAdmission { scope_sha256, snapshot, deadline })
    }
}

fn refused() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/classic-retained-flight",
        "The requested Flight read did not match its exact Classic retained authority")
}
