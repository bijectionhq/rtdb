use bytes::Bytes;
use journal_contract::Lsn;
use rtdb_crypt::RecoveryKeyHold;
use rtdb_types::S3ObjectNamespace;

use crate::multi::activation::{
    ActivationAuthority, Binding, journal_custody::KeeperJournal,
    operator::attestation::{Intent, KeyEvidence},
    recovery::{LossPolicy, Operation, Status},
};

use super::*;
use super::lineage::LineageObject;

#[derive(Clone, Debug)]
pub(super) struct Stage {
    archive: PathBuf,
    source_key_vault: PathBuf,
    operation: Operation,
    destination: HostedDatabase,
}

struct Candidate {
    incarnation: Uuid,
    cell: Uuid,
    scope: ActivationScope,
    intent: Intent,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageReceipt {
    source_incarnation: Uuid,
    source_lsn: u64,
    source_position: u64,
    destination_incarnation: Uuid,
    destination_cell: Uuid,
    destination_position: u64,
    pub(super) manifest_sha256: String,
    pub(super) intent: Intent,
    pub(super) key_hold: RecoveryKeyHold,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) object_namespace: Option<S3ObjectNamespace>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StagingOrder {
    pub(super) intent: Intent,
    pub(super) key_hold: RecoveryKeyHold,
    pub(super) destination_journal_sha256: [u8; 32],
    pub(super) vault_identity: Uuid,
    #[serde(default, skip_serializing_if = "untracked_uploads")]
    pub(super) tracked_uploads: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) lineage: Option<LineageObject>,
}

fn untracked_uploads(tracked: &bool) -> bool { !*tracked }

fn staging_key(authority: Uuid, candidate: Uuid) -> Result<rtdb_storage::ObjectKey> {
    Ok(rtdb_storage::ObjectKey::new(format!("recovery-custody/{authority}/{candidate}/staging"))?)
}

pub(super) async fn read_staging_order(
    store: &dyn ObjectStore, authority: Uuid, candidate: Uuid,
) -> Result<Option<StagingOrder>> {
    super::held_objects::read_provider_record(store, &staging_key(authority, candidate)?, 4096).await
}

impl StageReceipt {
    pub(super) async fn require_pending(&self, authority: &ActivationAuthority, source_lsn: Lsn) -> Result<()> {
        let operation = Operation { predecessor: self.intent.expected, operation_id: self.intent.operation_id };
        let candidate = Candidate::pending(authority, &self.intent.database, operation, source_lsn).await?;
        if candidate.intent != self.intent || authority.connection_digest()? != self.key_hold.authority_sha256 {
            return Err(stage_source());
        }
        Ok(())
    }
}

impl Stage {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let archive = backup_field(&mut fields, "--archive")?.into();
        let source_key_vault = backup_field(&mut fields, "--source-key-vault")?.into();
        let operation_id = backup_uuid(&backup_field(&mut fields, "--operation-id")?)?;
        let incarnation = backup_uuid(&backup_field(&mut fields, "--expected-incarnation")?)?;
        let revision = backup_field(&mut fields, "--expected-revision")?.parse::<u64>()
            .ok().filter(|revision| *revision != 0).ok_or_else(arguments_error)?;
        let destination = HostedDatabase::parse(&mut fields)?;
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        let operation = Operation { predecessor: Binding { incarnation, revision }, operation_id };
        Ok(Self { archive, source_key_vault, operation, destination })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let authority = activation::installed(&self.destination.journal_url, true).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb/backup-stage-authority", "Configure the independent activation journal reader"))?;
        self.run_with_authority(&authority).await
    }

    pub(super) async fn run_with_authority(self, authority: &ActivationAuthority) -> Result<Value> {
        let source_keys = Arc::new(FileKms::open(&self.source_key_vault, || SystemClock.now().0)?);
        let archive = Bytes::from(read_archive(&self.archive)?);
        let backup = PortableBackup::verify(&archive, source_keys).await?;
        let manifest = backup.manifest_sha256();
        let proof = backup.lineage_proof_bytes(&archive)?.map(|proof| archive.slice_ref(proof));
        let source_lsn = backup.source_lsn();
        if backup.source_basis().incarnation() != *self.operation.predecessor.incarnation.as_bytes() {
            return Err(stage_source());
        }
        let lineage = authority.supports_lineage_for(&self.destination.name).await?;
        if backup.authenticated_cut().is_some() != lineage {
            return Err(stage_source());
        }
        let candidate = Candidate::pending(authority, &self.destination.name, self.operation, source_lsn).await?;
        let evidence = KeyEvidence {
            generation: 1,
            authority_sha256: authority.connection_digest()?,
        };
        let hold = candidate.intent.archive_key_hold(&backup, evidence)?;
        let root = rtdb_storage::ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.destination.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let credentials = crate::cli::journal_credentials(&self.destination.journal_tls)?;
        let JournalBackend { journal, batch, placement } =
            connect_stream_journal(&self.destination.journal_url, &credentials).await?;
        let placement = placement.ok_or_else(stage_source)?;
        let keys = Arc::new(FileKms::open(&self.destination.key_vault, || SystemClock.now().0)?);
        let mut order = StagingOrder {
            intent: candidate.intent.clone(), key_hold: hold,
            destination_journal_sha256: KeeperJournal::connect(&self.destination.journal_url, &credentials)?.connection_sha256,
            vault_identity: keys.enroll_recovery_vault().await?,
            tracked_uploads: true,
            lineage: proof.as_deref().map(LineageObject::describe),
        };
        let io = IoRegion::new();
        let mut writer_io = IoRegion::new();
        let installed = async {
            let store = self.destination.objects.open(&root, &remotes, Arc::clone(&io)).await?;
            if let Some(prior) = read_staging_order(store.as_ref(), hold.authority, candidate.incarnation).await? {
                order.tracked_uploads = prior.tracked_uploads;
                if prior.lineage.is_none() { order.lineage = None; }
            }
            let encoded = serde_json::to_vec(&order).map_err(|_| stage_source())?;
            store.put(&staging_key(hold.authority, candidate.incarnation)?, Bytes::from(encoded)).await?;
            Candidate::pending(authority, &self.destination.name, self.operation, source_lsn).await?;
            super::held_objects::require_staging_open(store.as_ref(), hold).await?;
            let uploads = order.tracked_uploads.then(|| inventory.staging_uploads(hold.authority, candidate.incarnation)).transpose()?;
            let (store, can_write): (Arc<dyn ObjectStore>, bool) = if let Some(uploads) = &uploads {
                match uploads.begin().await? {
                    Some(writer) => {
                        Arc::get_mut(&mut writer_io).expect("the staging region is private").track_publications(writer);
                        (self.destination.objects.open_cell(&root, &remotes, Arc::clone(&writer_io)).await?, true)
                    }
                    None => (store, false),
                }
            } else {
                (store, true)
            };
            if let Some(lineage) = order.lineage {
                if can_write {
                    let bytes = proof.as_ref().ok_or_else(stage_source)?.clone();
                    store.put(&lineage.key(candidate.incarnation, manifest)?, bytes).await?;
                }
                lineage.read(candidate.incarnation, manifest, store.as_ref(), None).await?;
            }
            let mut database = MultiDatabase::new(
                MultiOptions {
                    database: candidate.incarnation,
                    node: Ipv4Addr::LOCALHOST.into(),
                    lease_micros: WRITER_LEASE_MICROS,
                    drift_micros: CLOCK_DRIFT_MICROS,
                },
                journal.clone(), store, keys.clone(), Arc::new(|| SystemClock.now()),
            )?.with_immutable_journal()
                .with_authenticated_journal(placement.journal, lineage)
                .with_batch_appender(batch).with_activation_scope(candidate.scope)?
                .with_manifest_budget(MemoryBudget::new(PortableBackup::MAX_BYTES));
            match journal.head(database.journal_stream()).await {
                Ok(Lsn(0)) | Err(JournalError::NotFound) if can_write => database.create().await?,
                Ok(Lsn(0)) | Err(JournalError::NotFound) => return Err(stage_closed()),
                Ok(_) => (),
                Err(error) => return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/journal-unavailable", error.to_string())),
            }
            let installed = async {
                if database.installed_backup().await?.is_none() {
                    if !can_write { return Err(stage_closed()); }
                    acquire_writer(&mut database).await?;
                }
                database.install_portable_backup_holding_key(backup, hold).await
            }.await;
            let stopped = database.shutdown_writer().await;
            let installed = installed?;
            stopped?;
            if candidate.intent.installed_key_hold(&installed, evidence)? != hold {
                return Err(stage_source());
            }
            keys.require_recovery_key(hold).await?;
            if let Some(uploads) = uploads {
                uploads.close().await?;
            }
            Candidate::pending(authority, &self.destination.name, self.operation, source_lsn).await?;
            Ok::<_, Anomaly>(installed)
        }.await;
        writer_io.close_and_wait().await;
        io.close_and_wait().await;
        let installed = installed?;
        serde_json::to_value(StageReceipt {
            source_incarnation: installed.source_incarnation(),
            source_lsn: installed.source_lsn().0,
            source_position: installed.source_basis().position(),
            destination_incarnation: candidate.incarnation,
            destination_cell: candidate.cell,
            destination_position: installed.destination_basis().position(),
            manifest_sha256: digest_hex(&installed.manifest_sha256()),
            intent: candidate.intent,
            key_hold: hold,
            object_namespace: Some(inventory.namespace().clone()),
        }).map_err(|error| Anomaly::fault("rtdb/backup-stage-receipt", error.to_string()))
    }

}

impl Candidate {
    async fn pending(authority: &ActivationAuthority, name: &str, operation: Operation, source_lsn: Lsn) -> Result<Self> {
        let Status::Pending { intent, candidate: incarnation, sealed_segment_end, installation: None, .. } =
            authority.recovery_status(name, operation).await?
        else {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-stage-intent", "Staging requires a pending recovery before installation is recorded"));
        };
        if intent.loss_policy == LossPolicy::Lossless && sealed_segment_end != Some(source_lsn) {
            return Err(stage_source());
        }
        let cell = intent.destination_cell.ok_or_else(|| Anomaly::incorrect(
            "rtdb/backup-stage-cell", "Hosted staging requires a recovery intent with a destination cell",
        ))?;
        let intent = Intent {
            authority_id: authority.identity(),
            database: name.to_owned(),
            expected: operation.predecessor,
            operation_id: operation.operation_id,
            cell: Some(cell),
        };
        let scope = intent.scope()?;
        Ok(Candidate { incarnation, cell, scope, intent })
    }
}

async fn acquire_writer(database: &mut MultiDatabase) -> Result<()> {
    let live = BTreeSet::from([Ipv4Addr::LOCALHOST.into()]);
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if matches!(database.tick(&live).await?, Role::Writer(_)) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.map_err(|_| Anomaly::new(
        AnomalyCategory::Busy, "rtdb/backup-writer", "The staged Multi writer lease did not become available",
    ))?
}

fn stage_source() -> Anomaly {
    Anomaly::incorrect("rtdb/backup-stage-source", "The archive does not match the recovery's predecessor or sealed final cut")
}

fn stage_closed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-stage-closed", "The original provider has closed staging uploads for this candidate")
}
