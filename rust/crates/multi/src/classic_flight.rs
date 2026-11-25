use std::{fmt, fs::File, io::Read, path::Path, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_directory::{ClassicMigrationSource, Directory, Lifecycle, MemberId, classic_flight, now_ms};
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
        let retained_key = retained_key.map(|path| {
            let invalid = || Anomaly::incorrect("rtdb.multi/classic-retained-key",
                "The installed Classic retained Flight verification key is invalid");
            let mut bytes = Vec::new();
            File::open(path).and_then(|file| file.take(256).read_to_end(&mut bytes)).map_err(|_| invalid())?;
            let key: [u8; 32] = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if bytes.len() == 256 || key == [0; 32] { return Err(invalid()); }
            Ok(key)
        }).transpose()?;
        Ok(Self { directory, retained_key })
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
        let (token, signature) = request.proof.split_once('.').filter(|(_, signature)| signature.len() == 86)
            .ok_or_else(refused)?;
        let token = URL_SAFE_NO_PAD.decode(token).map_err(|_| refused())?;
        let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| refused())?;
        let grant: RetainedGrant = serde_json::from_slice(&token).map_err(|_| refused())?;
        UnparsedPublicKey::new(&ED25519, key).verify(
            &archive::encode(&("rtdb.multi/classic-retained-flight-read/v1", &grant))?, &signature,
        ).map_err(|_| refused())?;
        let RetainedGrant { scope, principal, action, body_sha256, expires_at_ms } = grant;
        let now = now_ms();
        if principal != request.principal || action != request.action || body_sha256 != request.body_sha256
            || !(now < expires_at_ms && expires_at_ms <= now.saturating_add(30_000))
            || scope.authority.is_nil() || scope.capture.is_nil() || scope.name != attachment.name()
            || scope.begin_sha256 == [0; 32] || scope.original_catalog_sha256 == [0; 32]
            || scope.custody_sha256 == [0; 32] || scope.application_selection_sha256 == [0; 32]
            || scope.retained_path_sha256 == [0; 32]
        {
            return Err(refused());
        }
        let storage = attachment_storage(attachment).map_err(|_| refused())?;
        let authority = scope.authority.to_string();
        let Some(database_config::Log::OtherLog(log)) = &attachment.configuration().log else { return Err(refused()); };
        let log: LogkeeperLogConfig = serde_json::from_slice(&log.value).map_err(|_| refused())?;
        let record = self.directory.deployment(&authority).await?.ok_or_else(refused)?;
        if storage.deployment != authority || storage.writer_epoch != Some(scope.source.writer_epoch)
            || storage.compactor || storage.snapshot_block.is_some()
            || log.deployment != authority || log.epoch != scope.source.writer_epoch
            || log.log_epoch != scope.source.log_epoch || log.replica_stream != record.stream("replica")
            || record.name != scope.name || record.holder.as_ref() != Some(&scope.holder)
            || record.incarnation != scope.source.incarnation || record.epoch != scope.source.writer_epoch
            || record.log_epoch != scope.source.log_epoch || record.storage_prefix() != storage.prefix
            || record.lifecycle != Lifecycle::Live || record.move_from.is_some() || record.checkpoint_transfer_fence.is_some()
            || record.checkpoint_capture_retirements.contains_key(&scope.capture)
            || !record.checkpoint_captures.get(&scope.capture).is_some_and(|capture| {
                capture.incarnation == record.incarnation && capture.log_epoch == record.log_epoch
                    && capture.media_bound && capture.remote_disposal.is_none()
                    && capture.manifest_sha256.as_deref() == Some(scope.source_manifest_sha256.as_str())
            })
        {
            return Err(refused());
        }
        if classic_flight::commission(&record).is_some_and(|commission| {
            commission.intent.capture != scope.capture || commission.intent.source_manifest_sha256 != scope.source_manifest_sha256
                || commission.begin_sha256 != scope.begin_sha256 || commission.original_catalog_sha256 != scope.original_catalog_sha256
                || commission.retained_path_sha256 != scope.retained_path_sha256
        }) {
            return Err(refused());
        }
        let deadline = u64::try_from(expires_at_ms).ok().and_then(|millis| millis.checked_mul(1_000_000))
            .and_then(DeliveryDeadline::from_wire_nanos).ok_or_else(refused)?;
        let snapshot = scope.snapshot;
        let scope_sha256 = archive::sha256(&archive::encode(&("rtdb.multi/classic-retained-flight-scope/v1", &principal, scope))?);
        Ok(ClassicRetainedAdmission { scope_sha256, snapshot, deadline })
    }
}

fn refused() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/classic-retained-flight",
        "The requested Flight read did not match its exact Classic retained authority")
}
