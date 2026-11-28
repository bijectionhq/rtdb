#[cfg(unix)]
use crate::multi::activation::operator::attestation::Intent;
use crate::multi::activation::operator::attestation::KeyEvidence;

use super::*;
use super::lineage::{HeldLineage, LineageObject};
use rtdb_crypt::RecoveryKeyHold;
use rtdb_db::BackupObjectReadback;
use rtdb_storage::ObjectVersionHold;
use rtdb_types::PhysicalObjectVersion;

mod acquire;
pub(super) use acquire::Acquirer;
pub(super) use acquire::require_staging_open;
pub(super) use acquire::read_published_lineage;
#[cfg(unix)]
pub(super) use acquire::Abandon;

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Readback {
    pub(super) incarnation: Uuid,
    pub(super) archive_sha256: String,
    journal_head: u64,
    pub(super) checkpoint: String,
    pub(super) held_versions: Vec<PhysicalObjectVersion>,
    pub(super) object_holds_verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) key_hold: Option<RecoveryKeyHold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) lineage: Option<HeldLineage>,
}

impl Readback {
    fn confirmed(incarnation: Uuid, checked: BackupObjectReadback, key_hold: Option<RecoveryKeyHold>, lineage: Option<HeldLineage>) -> Self {
        Self {
            incarnation,
            archive_sha256: digest_hex(&checked.installation.manifest_sha256()),
            journal_head: checked.journal_head.0,
            checkpoint: checked.checkpoint.to_string(),
            held_versions: checked.versions,
            object_holds_verified: true,
            key_hold,
            lineage,
        }
    }

    pub(super) fn digest(&self) -> Result<[u8; 32]> {
        let bytes = serde_json::to_vec(self).map_err(|error| Anomaly::fault("rtdb/backup-object-readback", error.to_string()))?;
        let mut digest = Sha256::new();
        digest.update(b"rtdb.hosted-recovery-readback/v1\0");
        digest.update(bytes);
        Ok(digest.finalize().into())
    }

    fn pinned_versions(&self) -> Vec<PhysicalObjectVersion> {
        self.held_versions.iter().chain(self.lineage.iter().map(|proof| &proof.version)).cloned().collect()
    }
}

struct VerifiedReadback {
    report: Readback,
    #[cfg(unix)]
    installation: rtdb_db::InstalledBackup,
    #[cfg(unix)]
    intent: Option<Intent>,
    #[cfg(unix)]
    journal_anchor: Option<journal_contract::CheckpointAnchor>,
}

struct StagedBackup {
    archive_sha256: [u8; 32],
    receipt: Option<staged::StageReceipt>,
}

impl StagedBackup {
    async fn staging_order(&self, store: &dyn ObjectStore) -> Result<Option<staged::StagingOrder>> {
        let Some(receipt) = &self.receipt else { return Ok(None); };
        let hold = receipt.key_hold;
        let order = staged::read_staging_order(store, hold.authority, hold.generation.database).await?;
        if order.as_ref().is_some_and(|order| order.key_hold != hold || order.intent != receipt.intent) {
            return Err(invalid_readback());
        }
        Ok(order)
    }

    async fn held_lineage(
        &self, incarnation: Uuid, lineage: Option<LineageObject>, store: &dyn ObjectStore, holds: Option<&dyn ObjectVersionHold>,
    ) -> Result<Option<HeldLineage>> {
        let Some(lineage) = lineage else { return Ok(None); };
        let (proof, _) = lineage.read(incarnation, self.archive_sha256, store, holds).await?;
        Ok(Some(proof))
    }
}

#[derive(Clone, Copy)]
enum ReadSource {
    SavedVersions,
    #[cfg(unix)]
    ServingNames,
}

pub(super) fn read_receipt<T: serde::de::DeserializeOwned>(path: &Path, max_bytes: u64) -> Result<T> {
    let file = File::open(path).map_err(io_error)?;
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(invalid_readback());
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1).read_to_end(&mut bytes).map_err(io_error)?;
    if bytes.len() as u64 > max_bytes {
        return Err(invalid_readback());
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid_readback())
}

pub(super) async fn read_provider_record<T: serde::de::DeserializeOwned>(
    store: &dyn ObjectStore, key: &rtdb_storage::ObjectKey, limit: usize,
) -> Result<Option<T>> {
    let size = match store.head_fresh(key).await {
        Ok(metadata) => usize::try_from(metadata.size).ok().filter(|size| *size <= limit).ok_or_else(invalid_readback)?,
        Err(rtdb_storage::Error::MissingObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bytes = store.get_fresh_exact(key, size).await?;
    serde_json::from_slice(&bytes).map(Some).map_err(|_| invalid_readback())
}

#[derive(Clone, Debug)]
pub(super) struct Verifier {
    archive: PathBuf,
    readback: Option<PathBuf>,
    stage_receipt: Option<PathBuf>,
    incarnation: Uuid,
    authority: Uuid,
    scope: ActivationScope,
    destination: HostedDatabase,
}

impl Verifier {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let archive = backup_field(&mut fields, "--archive")?.into();
        let incarnation = backup_uuid(&backup_field(&mut fields, "--incarnation")?)?;
        let authority = backup_uuid(&backup_field(&mut fields, "--authority-id")?)?;
        let destination = HostedDatabase::parse(&mut fields)?;
        let readback = fields.remove("--readback").map(PathBuf::from);
        let stage_receipt = fields.remove("--stage-receipt").map(PathBuf::from);
        let cell = fields.remove("--cell-id").as_deref().map(backup_uuid).transpose()?;
        let scope = ActivationScope::for_cell(authority, &destination.name, cell)?;
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self { archive, readback, stage_receipt, incarnation, authority, scope, destination })
    }

    pub(super) async fn run(self) -> Result<Value> {
        serde_json::to_value(self.verify(ReadSource::SavedVersions).await?.report)
            .map_err(|error| Anomaly::fault("rtdb/backup-object-readback", error.to_string()))
    }

    fn staged_backup(&self) -> Result<StagedBackup> {
        let archive_sha256 = Sha256::digest(read_archive(&self.archive)?).into();
        let receipt = self.stage_receipt.as_deref()
            .map(|path| read_receipt::<staged::StageReceipt>(path, 4096)).transpose()?;
        if let Some(receipt) = &receipt {
            if receipt.intent.authority_id != self.authority || receipt.intent.scope()? != self.scope
                || receipt.intent.candidate()? != self.incarnation || receipt.manifest_sha256 != digest_hex(&archive_sha256)
            {
                return Err(invalid_readback());
            }
        }
        Ok(StagedBackup { archive_sha256, receipt })
    }

    async fn verify(self, source: ReadSource) -> Result<VerifiedReadback> {
        let staged = self.staged_backup()?;
        let requested = self.readback.as_deref().map(|path| read_receipt::<Readback>(path, 32 << 20)).transpose()?;
        if requested.as_ref().is_some_and(|readback| {
            readback.incarnation != self.incarnation || readback.archive_sha256 != digest_hex(&staged.archive_sha256)
                || !readback.object_holds_verified || readback.held_versions.is_empty()
                || readback.key_hold != staged.receipt.as_ref().map(|receipt| receipt.key_hold)
        }) {
            return Err(invalid_readback());
        }
        let root = rtdb_storage::ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.destination.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let io = IoRegion::new();
        let result = async {
            let provider = self.destination.objects.open(&root, &remotes, Arc::clone(&io)).await?;
            let lineage = staged.staging_order(provider.as_ref()).await?.and_then(|order| order.lineage);
            let store = match (&requested, source) {
                (Some(readback), ReadSource::SavedVersions) => inventory.pinned_read_store(&readback.pinned_versions())?,
                _ => provider,
            };
            #[cfg(unix)]
            if matches!(source, ReadSource::ServingNames)
                && let (Some(receipt), Some(readback)) = (&staged.receipt, &requested)
            {
                acquire::require_attestation_plan(store.as_ref(), receipt, readback).await?;
            }
            let held_lineage = staged.held_lineage(self.incarnation, lineage, store.as_ref(), Some(&inventory)).await?;
            let (checked, key_hold) = self.inspect(&staged, store, Some(&inventory)).await?;
            Ok::<_, Anomaly>((checked, key_hold, held_lineage))
        }.await;
        io.close_and_wait().await;
        let (checked, key_hold, held_lineage) = result?;
        #[cfg(unix)]
        let installation = checked.installation;
        #[cfg(unix)]
        let journal_anchor = checked.journal_anchor;
        let current = Readback::confirmed(self.incarnation, checked, key_hold, held_lineage);
        if requested.is_some_and(|previous| previous != current) {
            return Err(invalid_readback());
        }
        Ok(VerifiedReadback {
            report: current,
            #[cfg(unix)]
            installation,
            #[cfg(unix)]
            intent: staged.receipt.map(|receipt| receipt.intent),
            #[cfg(unix)]
            journal_anchor,
        })
    }

    async fn inspect(
        &self, staged: &StagedBackup, store: Arc<dyn ObjectStore>, holds: Option<&dyn ObjectVersionHold>,
    ) -> Result<(BackupObjectReadback, Option<RecoveryKeyHold>)> {
        let credentials = crate::cli::journal_credentials(&self.destination.journal_tls)?;
        let JournalBackend { journal, batch, .. } = connect_stream_journal(&self.destination.journal_url, &credentials).await?;
        let keys = Arc::new(FileKms::open(&self.destination.key_vault, || SystemClock.now().0)?);
        let mut database = MultiDatabase::new(
            MultiOptions {
                database: self.incarnation,
                node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS,
                drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal, store, keys.clone(), Arc::new(|| SystemClock.now()),
        )?.with_immutable_journal().with_batch_appender(batch).with_activation_scope(self.scope)?;
        let checked = match holds {
            Some(holds) => database.check_installed_backup_object_holds(staged.archive_sha256, holds).await?,
            None => database.observe_installed_backup_object_versions(staged.archive_sha256).await?,
        };
        let key_hold = if let Some(receipt) = &staged.receipt {
            let key = KeyEvidence { generation: checked.generation.number, authority_sha256: receipt.key_hold.authority_sha256 };
            let hold = receipt.intent.installed_key_hold(&checked.installation, key)?;
            if hold != receipt.key_hold || hold.generation != checked.generation {
                return Err(invalid_readback());
            }
            keys.require_recovery_key(hold).await?;
            Some(hold)
        } else { None };
        Ok((checked, key_hold))
    }
}

#[cfg(unix)]
#[derive(Clone, Debug)]
pub(super) struct Attester {
    verifier: Verifier,
    signing_key: PathBuf,
    journal_custodian_tls: PathBuf,
}

#[cfg(unix)]
impl Attester {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let signing_key = backup_field(&mut fields, "--attestation-signing-key")?.into();
        let journal_custodian_tls = PathBuf::from(backup_field(&mut fields, "--journal-custodian-tls-dir")?);
        let verifier = Verifier::parse(fields)?;
        if !journal_custodian_tls.is_absolute() || verifier.readback.is_none() || verifier.stage_receipt.is_none() {
            return Err(arguments_error());
        }
        Ok(Self { verifier, signing_key, journal_custodian_tls })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let authority = activation::installed(&self.verifier.destination.journal_url, true).await?
            .ok_or_else(|| Anomaly::incorrect("rtdb/backup-attestation-authority", "Configure the independent activation journal reader"))?;
        self.run_with_authority(&authority).await
    }

    pub(super) async fn run_with_authority(self, authority: &activation::ActivationAuthority) -> Result<Value> {
        use crate::multi::activation::{
            journal_custody::KeeperJournal,
            operator::attestation::{Attestation, signing_key},
        };

        let candidate = self.verifier.incarnation;
        let destination = &self.verifier.destination;
        let reader = KeeperJournal::connect(
            &destination.journal_url, &crate::cli::journal_credentials(&destination.journal_tls)?,
        )?;
        let custodian = KeeperJournal::connect(
            &destination.journal_url, &crate::cli::journal_credentials(&self.journal_custodian_tls)?,
        )?;
        let checked = self.verifier.verify(ReadSource::ServingNames).await?;
        let (intent, hold) = checked.intent.zip(checked.report.key_hold).ok_or_else(invalid_readback)?;
        let anchor = checked.journal_anchor.ok_or_else(invalid_readback)?;
        let head = journal_contract::Lsn(checked.report.journal_head);
        let key = signing_key(&self.signing_key)?;
        let mut proof = Attestation::sign_hosted_installation(
            authority, intent, &checked.installation, hold, checked.report.digest()?, &key,
        ).await?;
        let selected = reader.selected_for_readback(candidate, head, anchor).await?;
        let retained = proof.bind_journal(selected, &key)?;
        let document = serde_json::to_value(proof)
            .map_err(|error| Anomaly::fault("rtdb/backup-attestation", error.to_string()))?;
        custodian.retain(candidate, retained).await?;
        Ok(document)
    }
}

fn invalid_readback() -> Anomaly {
    Anomaly::incorrect("rtdb/backup-object-readback", "The object readback must match the verified staged backup")
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    use super::*;
    use rtdb_storage::ObjectKey;
    use crate::multi::activation::{
        ActivationAuthority, ActivationOptions, journal_custody::KeeperJournal, operator::attestation::Attestation,
        recovery::{Intent, LossPolicy, Operation, Status},
    };
    use ring::{rand::SystemRandom, signature::{Ed25519KeyPair, KeyPair}};
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, Config, JournalConfig, testing::LocalCluster};

    const DATABASE: &str = "hosted-documents";
    const TRANSACTION_KEY: [u8; 16] = [1; 16];

    struct RestartedKeepers {
        stop: CancellationToken,
        tasks: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
    }

    impl RestartedKeepers {
        async fn with_roles(keepers: &mut LocalCluster, reader: &str, custodian: Option<&str>) -> Self {
            let configs = keepers.keepers().keys().map(|id| {
                let mut config = Config::read(&keepers.write_config(*id).unwrap()).unwrap();
                config.readers.push(reader.into());
                config.administrators.extend(custodian.map(str::to_owned));
                config
            }).collect::<Vec<_>>();
            keepers.shutdown().await.unwrap();
            let stop = CancellationToken::new();
            let mut tasks = Vec::new();
            for config in configs {
                let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
                tasks.push(tokio::spawn(config.serve(listener, stop.child_token())));
            }
            Self { stop, tasks }
        }

        async fn shutdown(self) {
            self.stop.cancel();
            for task in self.tasks {
                task.await.unwrap().unwrap();
            }
        }
    }

    fn keeper_access(
        home: &Path, name: &str, keepers: &LocalCluster, placement: &JournalConfig, principal: &str,
    ) -> (String, PathBuf) {
        let journal = home.join(format!("{name}.json"));
        fs::write(&journal, serde_json::to_vec(&json!({
            "keepers": placement.keepers, "catalog": placement.catalog, "voters": placement.voters,
            "catalog_voters": placement.catalog_voters, "quota_bytes": placement.quota_bytes,
            "max_record_bytes": placement.max_record_bytes,
        })).unwrap()).unwrap();
        let tls = home.join(format!("{name}-tls"));
        fs::create_dir(&tls).unwrap();
        let credentials = keepers.credentials(principal).unwrap();
        for (source, name) in [(credentials.certificate, "journal-certificate.pem"),
            (credentials.private_key, "journal-key.pem"), (credentials.ca, "journal-ca.pem")]
        {
            let file = tls.join(name);
            fs::copy(source, &file).unwrap();
            fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
        }
        (format!("{CONSENSUS_SCHEME}{}", journal.display()), tls)
    }

    struct CommissionedSource {
        keepers: RestartedKeepers,
        _keeper_files: LocalCluster,
        journal: Arc<rtdb_stream_journal::ConsensusJournal>,
        stream: StreamId,
        head: journal_contract::Lsn,
        keys: Arc<FileKms>,
        export_arguments: Vec<String>,
    }

    impl CommissionedSource {
        async fn shutdown(self) {
            self.keepers.shutdown().await;
        }

        async fn export(&self, archive: &Path) -> Result<()> {
            let options = MultiBackupOptions::parse([
                "export-hosted".into(), "--archive".into(), archive.display().to_string(),
            ].into_iter().chain(self.export_arguments.iter().cloned()))?;
            let exported = options.run(rtdb_storage::AppleSync::Full).await?;
            let verified = PortableBackup::verify(&fs::read(archive).unwrap(), self.keys.clone()).await.unwrap();
            assert_eq!(exported["manifest_sha256"], digest_hex(&verified.manifest_sha256()));
            assert_eq!(verified.source_basis().incarnation(), self.stream.0.to_be_bytes());
            assert_eq!(verified.source_lsn(), self.head);
            assert_eq!(self.journal.head(self.stream).await.unwrap(), self.head);
            assert_eq!(verified.source_receipt(&TRANSACTION_KEY).await?.outcome, rtdb_tx::transact::Outcome::Then);
            Ok(())
        }
    }

    async fn commissioned_source(
        home: &Path, authority: &Arc<ActivationAuthority>, source: Uuid, config: &rtdb_s3::S3Config,
        vault: &Path, authenticated: bool,
    ) -> CommissionedSource {
        const WRITER: &str = "urn:logkeeper:client:backup-source-writer";
        const READER: &str = "urn:logkeeper:client:backup-source-reader";
        let mut keepers = if authenticated { LocalCluster::start_authenticated(&[WRITER]).await }
            else { LocalCluster::start_independent_catalog(&[WRITER]).await }.unwrap();
        let placement = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(WRITER, &placement).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let (journal_url, tls) = keeper_access(home, "source-reader", &keepers, &placement, READER);

        FileKms::create_directory(vault).unwrap();
        let keys = Arc::new(FileKms::open(vault, || SystemClock.now().0).unwrap());
        let io = IoRegion::new();
        let root = ObjectKey::new("multi").unwrap();
        let store = config.open(&root, &BTreeMap::new(), io.clone()).await.unwrap();
        let node = Ipv4Addr::LOCALHOST.into();
        let mut database = MultiDatabase::new(MultiOptions {
            database: source, node, lease_micros: WRITER_LEASE_MICROS, drift_micros: CLOCK_DRIFT_MICROS,
        }, journal.clone(), store, keys.clone(), Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal()
            .with_activation_scope(authority.scope(DATABASE).unwrap()).unwrap()
            .with_serving_authority(authority.serving_authority(DATABASE, source).unwrap());
        if authenticated {
            database = database.with_authenticated_journal(journal.clone(), true);
        }
        database.create().await.unwrap();
        assert!(matches!(database.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        use rtdb_tx::transact::{Cell, Op, Row, Transact};
        use rtdb_types::{EntityId, StructFields, TableRef, Value, rtdb_v1::MAIN_BRANCH};

        database.transact(&Transact {
            key: TRANSACTION_KEY.to_vec(),
            require: Vec::new(),
            window: Default::default(),
            guards: Vec::new(),
            requires: Vec::new(),
            otherwise: Vec::new(),
            custody_request_digest: None,
            request_memory: None,
            ops: vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table: TableRef::new("public", "documents"),
                if_absent: false,
                rows: vec![Row {
                    id: EntityId::Integer(1),
                    valid: None,
                    cells: StructFields::from([("value".into(), Cell::Value(Value::Utf8("portable".into())))]),
                    custody_group: None,
                    protected_columns: Vec::new(),
                    custody_payload: None,
                }],
            }],
        }).await.unwrap();
        let attachment = database.write_file(b"portable object").await.unwrap();
        database.checkpoint(vec![attachment]).await.unwrap();
        database.shutdown_writer().await.unwrap();
        let stream = database.journal_stream();
        let head = journal.head(stream).await.unwrap();
        drop(database);
        io.close_and_wait().await;
        let serving = RestartedKeepers::with_roles(&mut keepers, READER, None).await;
        let export_arguments = vec![
            "--database".into(), DATABASE.into(),
            "--incarnation".into(), source.to_string(), "--authority-id".into(), authority.identity().to_string(),
            "--journal-url".into(), journal_url,
            "--tls-dir".into(), tls.display().to_string(), "--key-vault".into(), vault.display().to_string(),
            "--bucket".into(), config.bucket.clone(), "--region".into(), config.region.clone().unwrap(),
            "--s3-endpoint".into(), config.endpoint.clone().unwrap(),
        ];
        CommissionedSource { keepers: serving, _keeper_files: keepers, journal, stream, head, keys, export_arguments }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a disposable loopback S3 bucket with Object Lock and the AWS CLI"]
    async fn staged_backup_checks_exact_provider_holds_on_its_complete_object_closure() {
        check_provider_holds(false).await;
    }

    #[test]
    #[ignore = "requires a disposable loopback S3 service with Object Lock and the AWS CLI"]
    fn published_provider_lineage_survives_source_and_temporary_custody_release() {
        std::thread::Builder::new().stack_size(16 << 20).spawn(|| {
            tokio::runtime::Builder::new_multi_thread().worker_threads(4)
                .thread_stack_size(16 << 20).enable_all().build().unwrap()
                .block_on(provider_lineage_survives_source_and_release());
        }).unwrap().join().unwrap();
    }

    async fn provider_lineage_survives_source_and_release() {
        check_provider_holds(true).await;
    }

    async fn check_provider_holds(authenticated: bool) {
        let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
        let url = url::Url::parse(&endpoint).unwrap();
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        let bucket = if authenticated {
            format!("rtdb-backup-hold-test-l-{}", Uuid::new_v4().simple())
        } else {
            let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
            assert!(bucket.starts_with("rtdb-backup-hold-test-"));
            bucket
        };
        let aws_call = |bucket: &str, operation: &str, fields: &[&str]| {
            std::process::Command::new("aws")
                .args(["--no-cli-pager", "--endpoint-url", &endpoint, "--region", "us-east-1",
                    "s3api", operation, "--bucket", bucket])
                .args(fields).output().unwrap()
        };
        let aws_bucket = |bucket: &str, operation: &str, fields: &[&str]| {
            let result = aws_call(bucket, operation, fields);
            assert!(result.status.success(), "{operation}: {}", String::from_utf8_lossy(&result.stderr));
            if result.stdout.is_empty() { Value::Null } else { serde_json::from_slice(&result.stdout).unwrap() }
        };
        let aws = |operation: &str, fields: &[&str]| aws_bucket(&bucket, operation, fields);
        if authenticated {
            aws("create-bucket", &["--object-lock-enabled-for-bucket"]);
        }
        let source_bucket = format!("{bucket}-source");
        aws_bucket(&source_bucket, "create-bucket", &["--object-lock-enabled-for-bucket"]);

        const PRINCIPAL: &str = "urn:logkeeper:client:backup-object-holds";
        const READER: &str = "urn:logkeeper:client:backup-destination-reader";
        const CUSTODIAN: &str = "urn:logkeeper:client:backup-checkpoint-custodian";
        let mut keepers = if authenticated { LocalCluster::start_authenticated(&[PRINCIPAL]).await }
            else { LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await }.unwrap();
        let placement = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(PRINCIPAL, &placement).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let home = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir().unwrap();
        let (journal_url, tls) = keeper_access(home.path(), "destination-writer", &keepers, &placement, PRINCIPAL);
        let (_, reader_tls) = keeper_access(home.path(), "destination-reader", &keepers, &placement, READER);
        let (_, custodian_tls) = keeper_access(home.path(), "destination-custodian", &keepers, &placement, CUSTODIAN);
        let mut destination_keepers = RestartedKeepers::with_roles(&mut keepers, READER, Some(CUSTODIAN)).await;
        let reader = KeeperJournal::connect(&journal_url, &crate::cli::journal_credentials(&reader_tls).unwrap()).unwrap();
        let options = |database| MultiOptions {
            database, node: Ipv4Addr::LOCALHOST.into(), lease_micros: WRITER_LEASE_MICROS,
            drift_micros: CLOCK_DRIFT_MICROS,
        };
        let live = BTreeSet::from([Ipv4Addr::LOCALHOST.into()]);
        let authority = Uuid::new_v4();
        let archive_path = home.path().join("archive");
        let source_vault = home.path().join("source");
        let mut source = rtdb_s3::S3Config::new(&source_bucket);
        source.endpoint = Some(endpoint.clone());
        source.region = Some("us-east-1".into());
        let mut s3 = rtdb_s3::S3Config::new(&bucket);
        s3.endpoint = Some(endpoint.clone());
        s3.region = Some("us-east-1".into());
        let io = IoRegion::new();
        let root = rtdb_storage::ObjectKey::new("multi").unwrap();
        let store = s3.open(&root, &BTreeMap::new(), Arc::clone(&io)).await.unwrap();
        let inventory = s3.versioned_inventory(&root, &BTreeMap::new()).await.unwrap();
        let vault = home.path().join("destination");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let cell = Uuid::new_v4();
        const AUTHORITY_PRINCIPAL: &str = "urn:logkeeper:client:backup-activation";
        const AUTHORITY_READER: &str = "urn:logkeeper:client:backup-activation-reader";
        let mut authority_keepers = if authenticated { LocalCluster::start_authenticated(&[AUTHORITY_PRINCIPAL]).await }
            else { LocalCluster::start_independent_catalog(&[AUTHORITY_PRINCIPAL]).await }.unwrap();
        let authority_placement = authority_keepers.journal_config(64 << 20, 4 << 20);
        let authority_journal = Arc::new(authority_keepers.journal(AUTHORITY_PRINCIPAL, &authority_placement).unwrap());
        authority_journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let (authority_url, authority_tls) = keeper_access(home.path(), "authority", &authority_keepers, &authority_placement, AUTHORITY_PRINCIPAL);
        let (_, authority_reader_tls) = keeper_access(home.path(), "authority-reader", &authority_keepers, &authority_placement, AUTHORITY_READER);
        let activated_keepers = RestartedKeepers::with_roles(&mut authority_keepers, AUTHORITY_READER, None).await;
        let authority_reader = ActivationAuthority::connect_reader(
            &authority_url, crate::cli::journal_credentials(&authority_reader_tls).unwrap(), authority,
        ).await.unwrap();
        let activation = ActivationAuthority::new(authority_journal.clone(), authority).unwrap();
        let source_id = Uuid::new_v4();
        let predecessor = if authenticated {
            ActivationOptions::parse([
                "bootstrap".into(), DATABASE.into(), "--incarnation".into(), source_id.to_string(),
                "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
                "--authority-tls-dir".into(), authority_tls.display().to_string(),
            ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full).await.unwrap();
            authority_reader.binding(DATABASE).await.unwrap()
        } else {
            activation.bootstrap(DATABASE, source_id).await.unwrap()
        };
        let source = commissioned_source(home.path(), &authority_reader, source_id, &source, &source_vault, authenticated).await;
        source.export(&archive_path).await.unwrap();
        let source_root = if authenticated {
            let root = source.journal.catalog_history().await.unwrap().genesis.root();
            assert_ne!(root, authority_journal.catalog_history().await.unwrap().genesis.root());
            assert_ne!(root, journal.catalog_history().await.unwrap().genesis.root());
            Some(root)
        } else { None };
        let source_end = source.head;
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signing_key = home.path().join("attestor.pkcs8");
        fs::write(&signing_key, pkcs8.as_ref()).unwrap();
        fs::set_permissions(&signing_key, fs::Permissions::from_mode(0o600)).unwrap();
        let attestor_public_key =
            Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap().public_key().as_ref().to_vec();
        let broker_config = home.path().join("broker.json");
        let socket = home.path().join("authority.sock");
        fs::write(&broker_config, serde_json::to_vec(&json!({
            "authority_id": authority, "authority_url": authority_url, "authority_tls_dir": authority_tls,
            "operator_uids": [fs::metadata(home.path()).unwrap().uid()],
            "destination_attestors": {(cell.to_string()): attestor_public_key},
            "destination_journal_readers": {(cell.to_string()): {
                "journal_url": journal_url, "journal_tls_dir": reader_tls, "require_object_custody": true,
                "object_namespace": inventory.namespace(),
            }},
        })).unwrap()).unwrap();
        fs::set_permissions(&broker_config, fs::Permissions::from_mode(0o600)).unwrap();
        let broker = ActivationOptions::parse([
            "serve".into(), "--authority-socket".into(), socket.display().to_string(),
            "--broker-config".into(), broker_config.display().to_string(),
        ].into_iter()).unwrap();
        let broker = tokio::spawn(broker.run(rtdb_storage::AppleSync::Full));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !socket.exists() && !broker.is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        if broker.is_finished() {
            panic!("broker startup failed: {:?}", broker.await);
        }
        let recover = |action: &str, fields: Vec<String>| ActivationOptions::parse([
            action.into(), DATABASE.into(), "--authority-socket".into(), socket.display().to_string(),
            "--expected-incarnation".into(), source_id.to_string(),
            "--expected-revision".into(), predecessor.revision.to_string(),
        ].into_iter().chain(fields)).unwrap().run(rtdb_storage::AppleSync::Full);
        let operation_id = Uuid::new_v4();
        let intent = Intent {
            operation: Operation { predecessor, operation_id },
            destination_cell: Some(cell),
            loss_policy: LossPolicy::AcceptUnknownTail,
        };
        let stage_archive = |archive: &Path, operation_id: Uuid, journal_tls: &Path| {
            let opts = MultiBackupOptions::parse([
                "stage".into(), "--archive".into(), archive.display().to_string(),
                "--database".into(), DATABASE.into(),
                "--operation-id".into(), operation_id.to_string(),
                "--expected-incarnation".into(), source_id.to_string(),
                "--expected-revision".into(), predecessor.revision.to_string(),
                "--source-key-vault".into(), source_vault.display().to_string(),
                "--key-vault".into(), vault.display().to_string(),
                "--journal-url".into(), journal_url.clone(), "--tls-dir".into(), journal_tls.display().to_string(),
                "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
                "--s3-endpoint".into(), endpoint.clone(),
            ].into_iter()).unwrap();
            let BackupCommand::Stage(stage) = opts.0 else { panic!("expected a stage command"); };
            stage.run_with_authority(&authority_reader)
        };
        let stage_with_credentials = |operation_id, journal_tls: &Path| stage_archive(&archive_path, operation_id, journal_tls);
        let stage = |operation_id| stage_with_credentials(operation_id, &tls);
        let lossless_id = Uuid::new_v4();
        let unsealed = Intent {
            operation: Operation { predecessor, operation_id: lossless_id },
            loss_policy: LossPolicy::Lossless,
            ..intent
        };
        let Status::Pending { candidate: unsealed_candidate, .. } =
            activation.begin_recovery(DATABASE, unsealed, None).await.unwrap()
        else { panic!("expected a pending lossless recovery"); };
        assert_eq!(stage_archive(&archive_path, lossless_id, &tls).await.unwrap_err().code,
            "rtdb/backup-stage-source");
        assert!(matches!(journal.head(StreamId(unsealed_candidate.as_u128())).await, Err(JournalError::NotFound)));
        let Status::Pending { candidate: incarnation, .. } =
            activation.begin_recovery(DATABASE, intent, Some(lossless_id)).await.unwrap()
        else { panic!("expected a pending recovery"); };
        recover("freeze-recovery", vec!["--operation-id".into(), operation_id.to_string()]).await.unwrap();
        source.shutdown().await;
        let archive_digest: [u8; 32] = Sha256::digest(fs::read(&archive_path).unwrap()).into();
        let stream = StreamId(incarnation.as_u128());
        let activation_stream = activation.commissioning_stream_id(DATABASE).unwrap();
        let activation_head = authority_journal.head(activation_stream).await.unwrap();
        assert!(stage(Uuid::new_v4()).await.is_err());
        assert!(matches!(journal.head(stream).await, Err(JournalError::NotFound)));
        assert!(store.list(None).await.unwrap().is_empty());
        let receipt = stage(operation_id).await.unwrap();
        let head = journal.head(stream).await.unwrap();
        assert_eq!(receipt["destination_incarnation"], incarnation.to_string());
        assert_eq!(receipt["object_namespace"], json!(inventory.namespace()));
        let key_hold: RecoveryKeyHold = serde_json::from_value(receipt["key_hold"].clone()).unwrap();
        assert_eq!(key_hold.authority, authority);
        assert_eq!(key_hold.operation, operation_id);
        assert_eq!(key_hold.generation, Generation { database: incarnation, number: 1 });
        assert_eq!(key_hold.authority_sha256, authority_reader.connection_digest().unwrap());
        keys.require_recovery_key(key_hold).await.unwrap();
        assert!(keys.retire(key_hold.generation, SystemClock.now().0).await.is_err());
        assert_eq!(stage(operation_id).await.unwrap(), receipt);
        assert_eq!(journal.head(stream).await.unwrap(), head);
        assert_eq!(authority_journal.head(activation_stream).await.unwrap(), activation_head);

        let scope = ActivationScope::for_cell(authority, DATABASE, Some(cell)).unwrap();
        let mut staged = MultiDatabase::new(options(incarnation), journal.clone(),
            store.clone(), keys, Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal().with_activation_scope(scope).unwrap();
        let proof_path = home.path().join("installation-proof.json");
        let release = || ActivationOptions::parse([
            "release-recovery-key".into(), DATABASE.into(),
            "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
            "--authority-tls-dir".into(), authority_tls.display().to_string(),
            "--installation-proof".into(), proof_path.display().to_string(),
            "--key-vault".into(), vault.display().to_string(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        let relocated = staged.installed_backup_file_relocations().await.unwrap();
        let [relocated] = relocated.as_slice() else { panic!("expected one relocated file"); };
        let file = rtdb_storage::ObjectKey::new(relocated.destination_file.to_string()).unwrap();
        drop(staged);

        let receipt_path = home.path().join("stage-receipt.json");
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let wrong_receipt_path = home.path().join("wrong-stage-receipt.json");
        let mut substituted = receipt.clone();
        substituted["key_hold"]["operation"] = json!(Uuid::new_v4());
        fs::write(&wrong_receipt_path, serde_json::to_vec(&substituted).unwrap()).unwrap();
        let wrong_namespace_path = home.path().join("wrong-namespace-stage-receipt.json");
        let mut wrong_namespace = receipt.clone();
        wrong_namespace["object_namespace"]["bucket"] = json!("other-staging-allocation");
        fs::write(&wrong_namespace_path, serde_json::to_vec(&wrong_namespace).unwrap()).unwrap();
        let hosted_options = |operation: &str, destination: Uuid, readback: Option<&Path>, receipt: &Path, key: Option<&Path>| MultiBackupOptions::parse([
            operation.to_owned(), "--archive".into(), archive_path.display().to_string(),
            "--database".into(), DATABASE.into(),
            "--incarnation".into(), destination.to_string(), "--authority-id".into(), authority.to_string(),
            "--stage-receipt".into(), receipt.display().to_string(),
            "--cell-id".into(), cell.to_string(), "--key-vault".into(), vault.display().to_string(),
            "--journal-url".into(), journal_url.clone(), "--tls-dir".into(), reader_tls.display().to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter().chain(readback.into_iter().flat_map(|path| [
            "--readback".into(), path.display().to_string(),
        ])).chain(key.into_iter().flat_map(|path| [
            "--attestation-signing-key".into(), path.display().to_string(),
            "--journal-custodian-tls-dir".into(), custodian_tls.display().to_string(),
        ]))).unwrap();
        let verify = |readback: Option<&Path>, receipt: &Path|
            hosted_options("verify-object-holds", incarnation, readback, receipt, None).run(rtdb_storage::AppleSync::Full);
        let acquire = |destination: Uuid, receipt: &Path| {
            let BackupCommand::AcquireObjects(acquirer) = hosted_options("acquire-object-holds", destination, None, receipt, None).0
            else { panic!("expected hosted acquisition"); };
            acquirer.run_with_authority(&authority_reader)
        };
        let attest = |readback: &Path| {
            let options = hosted_options("attest", incarnation, Some(readback), &receipt_path, Some(&signing_key));
            let BackupCommand::AttestObjects(attester) = options.0 else { panic!("expected hosted attestation"); };
            attester.run_with_authority(&authority_reader)
        };
        let objects: Vec<_> = store.list(None).await.unwrap().into_iter()
            .filter(|entry| !entry.key.as_str().starts_with("recovery-custody/")).collect();
        assert_eq!(objects.len(), 4 + usize::from(authenticated));
        let token = &objects.iter().find(|entry| entry.key.as_str().starts_with("~rtdb-v1/")).unwrap().key;
        assert!(verify(None, &receipt_path).await.is_err());
        let hold = |version: &rtdb_types::PhysicalObjectVersion| {
            aws("put-object-legal-hold", &["--key", &version.key,
                "--version-id", &version.version, "--legal-hold", "Status=ON"]);
        };
        let observe_hold = |version: &PhysicalObjectVersion| {
            aws("get-object-legal-hold", &["--key", &version.key, "--version-id", &version.version])
        };
        for entry in objects.iter().filter(|entry| entry.key != file && &entry.key != token) {
            hold(&store.observe_head(&entry.key).await.unwrap().version.unwrap());
        }
        assert!(verify(None, &receipt_path).await.is_err());
        let physical = store.observe_head(&file).await.unwrap().version.unwrap();
        hold(&physical);
        assert!(verify(None, &receipt_path).await.is_err());
        assert!(acquire(incarnation, &wrong_receipt_path).await.is_err());
        assert!(acquire(incarnation, &wrong_namespace_path).await.is_err());
        assert!(verify(None, &receipt_path).await.is_err());
        hold(&store.observe_head(token).await.unwrap().version.unwrap());
        let manual = home.path().join("manual-readback.json");
        let report = verify(None, &receipt_path).await.unwrap();
        fs::write(&manual, serde_json::to_vec(&report).unwrap()).unwrap();
        assert_eq!(attest(&manual).await.unwrap_err().code, "rtdb/backup-object-acquisition");
        let result = acquire(incarnation, &receipt_path).await.unwrap();
        assert_eq!(verify(None, &receipt_path).await.unwrap(), result);
        assert_eq!(result["object_holds_verified"], true);
        assert_eq!(result["key_hold"], receipt["key_hold"]);
        assert_eq!(result["incarnation"], incarnation.to_string());
        assert_eq!(result["archive_sha256"], digest_hex(&archive_digest));
        assert_eq!(result["journal_head"], head.0);
        assert_eq!(result["held_versions"].as_array().unwrap().len(), 4);
        assert!(result["held_versions"].as_array().unwrap().contains(&json!(physical)));
        let plan = ObjectKey::new(format!("recovery-custody/{authority}/{incarnation}/acquisition")).unwrap();
        let plan_before = store.get_fresh(&plan).await.unwrap();
        let retry_bytes = home.path().join("retry-file-bytes");
        fs::write(&retry_bytes, store.get_fresh(&file).await.unwrap()).unwrap();
        aws("put-object", &["--key", &physical.key, "--body", retry_bytes.to_str().unwrap()]);
        let newer = store.observe_head(&file).await.unwrap().version.unwrap();
        assert_ne!(physical, newer);
        for version in [&physical, &newer] {
            aws("put-object-legal-hold", &["--key", &version.key,
                "--version-id", &version.version, "--legal-hold", "Status=OFF"]);
        }
        assert_eq!(acquire(incarnation, &receipt_path).await.unwrap(), result);
        assert_eq!(store.get_fresh(&plan).await.unwrap(), plan_before);
        assert_eq!(observe_hold(&physical)["LegalHold"]["Status"], "ON");
        assert_eq!(observe_hold(&newer)["LegalHold"]["Status"], "OFF");
        aws("delete-object", &["--key", &newer.key, "--version-id", &newer.version]);

        let readback = home.path().join("readback.json");
        fs::write(&readback, serde_json::to_vec(&result).unwrap()).unwrap();
        let BackupCommand::AttestObjects(mut unauthorized) = hosted_options("attest", incarnation, Some(&readback), &receipt_path, Some(&signing_key)).0
        else { panic!("expected a hosted attestor"); };
        unauthorized.journal_custodian_tls = reader_tls.clone();
        assert!(unauthorized.run_with_authority(&authority_reader).await.is_err());
        assert!(reader.journal.checkpoint_custody(stream).await.unwrap().is_none());
        let proof_json = attest(&readback).await.unwrap();
        let proof: Attestation = serde_json::from_value(proof_json.clone()).unwrap();
        assert_eq!(proof.held_key().unwrap().1, key_hold);
        let (.., retained) = proof.held_journal().unwrap();
        let anchor = retained.hold.anchor;
        let mut different_record = anchor;
        different_record.record_sha256[0] ^= 1;
        assert!(reader.selected_for_readback(incarnation, head, different_record).await.is_err());
        assert!(reader.selected_for_readback(incarnation, journal_contract::Lsn(head.0 + 1), anchor).await.is_err());
        assert_eq!(
            reader.journal.checkpoint_custody(stream).await.unwrap().unwrap(),
            journal_contract::CheckpointCustody { hold: retained.hold, released: false },
        );
        let verified_readback: Readback = serde_json::from_value(result.clone()).unwrap();
        assert_eq!(proof_json["evidence"]["objects_sha256"], json!(verified_readback.digest().unwrap()));
        fs::write(&proof_path, serde_json::to_vec(&proof_json).unwrap()).unwrap();
        assert!(release().await.is_err());

        let record = |operation: Uuid, path: &Path| ActivationOptions::parse([
            "record-install".into(), DATABASE.into(),
            "--authority-socket".into(), socket.display().to_string(),
            "--operation-id".into(), operation.to_string(),
            "--expected-incarnation".into(), predecessor.incarnation.to_string(),
            "--expected-revision".into(), predecessor.revision.to_string(),
            "--cell-id".into(), cell.to_string(),
            "--manifest-sha256".into(), result["archive_sha256"].as_str().unwrap().into(),
            "--installation-proof".into(), path.display().to_string(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        let altered_proof = home.path().join("altered-proof.json");
        let mut altered = proof_json.clone();
        altered["evidence"]["objects_sha256"][0] = json!(1 ^ verified_readback.digest().unwrap()[0]);
        fs::write(&altered_proof, serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(record(operation_id, &altered_proof).await.is_err());
        let recorded = record(operation_id, &proof_path).await.unwrap();
        assert_eq!(recorded["installation"]["objects_sha256"], json!(verified_readback.digest().unwrap()));
        assert_eq!(record(operation_id, &proof_path).await.unwrap(), recorded);
        assert_eq!(attest(&readback).await.unwrap(), proof_json);
        let release_objects = |readback: &Path, proof: &Path| MultiBackupOptions::parse([
            "release-object-holds".into(), "--readback".into(), readback.display().to_string(),
            "--installation-proof".into(), proof.display().to_string(),
            "--authority-socket".into(), socket.display().to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        assert_eq!(
            release_objects(&readback, &proof_path).await.unwrap_err().code,
            "rtdb/activation-object-custody",
        );

        let wrong_readback = home.path().join("wrong-readback.json");
        let mut wrong = result.clone();
        wrong["journal_head"] = json!(head.0 + 1);
        fs::write(&wrong_readback, serde_json::to_vec(&wrong).unwrap()).unwrap();
        assert!(verify(Some(&wrong_readback), &receipt_path).await.is_err());
        assert!(attest(&wrong_readback).await.is_err());

        aws("put-object-legal-hold", &["--key", &physical.key,
            "--version-id", &physical.version, "--legal-hold", "Status=OFF"]);
        assert!(verify(Some(&readback), &receipt_path).await.is_err());
        assert!(attest(&readback).await.is_err());
        hold(&physical);

        let same_bytes = home.path().join("same-bytes");
        fs::write(&same_bytes, store.get_fresh(&file).await.unwrap()).unwrap();
        aws("put-object", &["--key", &physical.key, "--body", same_bytes.to_str().unwrap()]);
        let replacement = store.observe_head(&file).await.unwrap().version.unwrap();
        assert_ne!(physical, replacement);
        assert!(verify(None, &receipt_path).await.is_err());
        hold(&replacement);
        let new = verify(None, &receipt_path).await.unwrap();
        assert_eq!(new["checkpoint"], result["checkpoint"]);
        assert_eq!(new["archive_sha256"], result["archive_sha256"]);
        let verified = new["held_versions"].as_array().unwrap();
        assert!(!verified.contains(&json!(physical)));
        assert!(verified.contains(&json!(replacement)));
        assert_eq!(verify(Some(&readback), &receipt_path).await.unwrap(), result);
        assert!(attest(&readback).await.is_err());

        let new_readback = home.path().join("replacement-readback.json");
        fs::write(&new_readback, serde_json::to_vec(&new).unwrap()).unwrap();
        let checkpoint = rtdb_storage::ObjectKey::new(result["checkpoint"].as_str().unwrap()).unwrap();
        let checkpoint_version = store.observe_head(&checkpoint).await.unwrap().version.unwrap();
        let deleted = aws("delete-object", &["--key", &checkpoint_version.key]);
        let marker = deleted["VersionId"].as_str().unwrap();
        assert!(verify(None, &receipt_path).await.is_err());
        assert_eq!(verify(Some(&new_readback), &receipt_path).await.unwrap(), new);
        assert!(attest(&new_readback).await.is_err());
        aws("delete-object", &["--key", &checkpoint_version.key, "--version-id", marker]);
        assert_eq!(verify(None, &receipt_path).await.unwrap(), new);

        let replacement_intent = Intent {
            operation: Operation { predecessor, operation_id: Uuid::new_v4() },
            ..intent
        };
        activation.begin_recovery(DATABASE, replacement_intent, Some(operation_id)).await.unwrap();
        assert!(stage(operation_id).await.is_err());
        assert!(acquire(incarnation, &receipt_path).await.is_err());
        assert!(attest(&new_readback).await.is_err());
        assert!(release_objects(&new_readback, &proof_path).await.is_err());
        let altered_signature = home.path().join("altered-signature.json");
        let mut altered = proof_json.clone();
        let first_byte = altered["signature"][0].as_u64().unwrap();
        altered["signature"][0] = json!(first_byte ^ 1);
        fs::write(&altered_signature, serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(release_objects(&readback, &altered_signature).await.is_err());
        assert_eq!(verify(Some(&readback), &receipt_path).await.unwrap(), result);
        let settled = release_objects(&readback, &proof_path).await.unwrap();
        assert_eq!(settled["incarnation"], incarnation.to_string());
        assert_eq!(settled["operation_id"], operation_id.to_string());
        assert_eq!(settled["released_versions"], verified_readback.held_versions.len());
        for target in &verified_readback.held_versions {
            assert_eq!(observe_hold(target)["LegalHold"]["Status"], "OFF");
        }
        assert_eq!(observe_hold(&replacement)["LegalHold"]["Status"], "ON");
        release().await.unwrap();
        assert!(verify(Some(&readback), &receipt_path).await.is_err());
        assert_eq!(journal.head(stream).await.unwrap(), head);
        let abandon_from = |selection: Vec<String>, key_vault: &Path| MultiBackupOptions::parse([
            "abandon-object-acquisition".into(),
            "--authority-socket".into(), socket.display().to_string(),
            "--journal-url".into(), journal_url.clone(),
            "--journal-custodian-tls-dir".into(), custodian_tls.display().to_string(),
            "--key-vault".into(), key_vault.display().to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter().chain(selection)).unwrap().run(rtdb_storage::AppleSync::Full);
        let abandon = |receipt: &Path| abandon_from(vec![
            "--stage-receipt".into(), receipt.display().to_string(),
        ], &vault);
        let discover = |candidate: Uuid, key_vault: &Path| abandon_from(vec![
            "--authority-id".into(), authority.to_string(), "--incarnation".into(), candidate.to_string(),
        ], key_vault);
        let settling_operation = replacement_intent.operation;
        let Status::Pending { candidate: abandoned, .. } = activation.recovery_status(DATABASE, settling_operation).await.unwrap()
        else { panic!("expected the staged recovery"); };
        let abandoned_receipt = home.path().join("abandoned-stage.json");
        fs::write(&abandoned_receipt, serde_json::to_vec(&stage(settling_operation.operation_id).await.unwrap()).unwrap()).unwrap();
        let older_key = store.list(None).await.unwrap().into_iter().find(|entry| {
            entry.key.as_str().parse::<rtdb_format::journal::ObjectId>().is_ok_and(|id| {
                id.database == abandoned && id.kind == rtdb_format::journal::ObjectKind::File
            })
        }).unwrap().key;
        let older = store.observe_head(&older_key).await.unwrap().version.unwrap();
        fs::write(&retry_bytes, store.get_fresh(&older_key).await.unwrap()).unwrap();
        aws("put-object", &["--key", &older.key, "--body", retry_bytes.to_str().unwrap()]);
        let document = acquire(abandoned, &abandoned_receipt).await.unwrap();
        let plan: Readback = serde_json::from_value(document).unwrap();
        let stage_evidence: staged::StageReceipt = read_receipt(&abandoned_receipt, 4096).unwrap();
        let journal_stream = journal_contract::StreamId(abandoned.as_u128());
        assert!(reader.journal.checkpoint_custody(journal_stream).await.unwrap().is_none());
        assert!(abandon(&abandoned_receipt).await.is_err());
        let version = plan.held_versions.iter().find(|version| version.key == format!("multi/{}", plan.checkpoint)).unwrap();
        assert_eq!(observe_hold(version)["LegalHold"]["Status"], "ON");
        let checkpoint_key = ObjectKey::new(&plan.checkpoint).unwrap();
        fs::write(&retry_bytes, store.get_fresh(&checkpoint_key).await.unwrap()).unwrap();
        aws("put-object", &["--key", &version.key, "--body", retry_bytes.to_str().unwrap()]);
        let unrelated = store.observe_head(&checkpoint_key).await.unwrap().version.unwrap();
        hold(&unrelated);
        aws("put-object-legal-hold", &["--key", &version.key, "--version-id", &version.version, "--legal-hold", "Status=OFF"]);

        let mut serving_intent = Intent {
            operation: Operation { predecessor, operation_id: Uuid::new_v4() },
            ..replacement_intent
        };
        activation.begin_recovery(DATABASE, serving_intent, Some(settling_operation.operation_id)).await.unwrap();
        let mut unrelated_namespace = stage_evidence.object_namespace.unwrap();
        unrelated_namespace.bucket = "uncommissioned-staging".into();
        let substituted = crate::multi::activation::operator::authorize_abandoned_acquisition(
            &socket, stage_evidence.intent, stage_evidence.key_hold, journal_contract::Lsn(plan.journal_head), None, Some(unrelated_namespace),
        ).await.unwrap_err();
        assert_eq!(substituted.code, "rtdb/activation-custody-allocation");
        assert!(reader.journal.checkpoint_custody(journal_stream).await.unwrap().is_none());
        fs::remove_file(&abandoned_receipt).unwrap();
        let empty_vault = home.path().join("empty-custody-vault");
        FileKms::create_directory(&empty_vault).unwrap();
        assert!(discover(abandoned, &empty_vault).await.is_err());
        assert!(reader.journal.checkpoint_custody(journal_stream).await.unwrap().is_none());
        let planned_versions = plan.pinned_versions();
        for version in &planned_versions {
            let listed = aws("list-object-versions", &["--prefix", &version.key]);
            assert!(listed["Versions"].as_array().unwrap().iter().any(|entry| {
                entry["Key"] == version.key && entry["VersionId"] == version.version
            }));
        }
        let original_keys = FileKms::open(&vault, || SystemClock.now().0).unwrap();
        original_keys.require_recovery_key(plan.key_hold.unwrap()).await.unwrap();
        let abandoned_result = discover(abandoned, &vault).await.unwrap();
        assert_eq!(abandoned_result["retired_versions"], planned_versions.len());
        assert_eq!(discover(abandoned, &vault).await.unwrap(), abandoned_result);
        assert_eq!(observe_hold(&unrelated)["LegalHold"]["Status"], "ON");
        for version in &planned_versions {
            let listed = aws("list-object-versions", &["--prefix", &version.key]);
            assert!(listed["Versions"].as_array().into_iter().flatten().all(|entry| entry["VersionId"] != version.version));
        }
        let listed = aws("list-object-versions", &["--prefix", &older.key]);
        assert!(listed["Versions"].as_array().unwrap().iter().any(|entry| {
            entry["VersionId"] == older.version && entry["IsLatest"] == false
        }));
        assert!(inventory.acquire_legal_hold(&checkpoint_key, version).await.is_err());
        let released = reader.journal.checkpoint_custody(journal_stream).await.unwrap().unwrap();
        assert!(released.released);
        let custodian = KeeperJournal::connect(&journal_url, &crate::cli::journal_credentials(&custodian_tls).unwrap()).unwrap();
        let retained = crate::multi::activation::journal_custody::RetainedJournal {
            connection_sha256: custodian.connection_sha256, hold: released.hold,
        };
        assert!(custodian.retain(abandoned, retained).await.is_err());
        assert!(original_keys.require_recovery_key(plan.key_hold.unwrap()).await.is_err());

        let interrupted_stages = [&reader_tls, &tls].into_iter().filter(|path| !authenticated || *path == &tls);
        for journal_tls in interrupted_stages {
            let operation = serving_intent.operation;
            let Status::Pending { candidate, .. } = activation.recovery_status(DATABASE, operation).await.unwrap()
            else { panic!("expected a staging intent"); };
            let stream = StreamId(candidate.as_u128());
            let receipt = stage_with_credentials(operation.operation_id, journal_tls).await;
            let order = staged::read_staging_order(store.as_ref(), authority, candidate).await.unwrap().unwrap();
            let partial = if journal_tls == &reader_tls {
                assert!(receipt.is_err());
                assert!(matches!(journal.head(stream).await, Err(JournalError::NotFound)));
                let uploads = inventory.staging_uploads(authority, candidate).unwrap();
                let writer = uploads.begin().await.unwrap().unwrap();
                let key = ObjectKey::new(format!("db/{candidate}/stalled")).unwrap();
                let physical = format!("multi/{key}");
                let reservation = aws("create-multipart-upload", &["--key", &physical]);
                let upload = reservation["UploadId"].as_str().unwrap().to_owned();
                writer.admit(&key, Uuid::new_v4(), &upload).await.unwrap();
                aws("upload-part", &["--key", &physical, "--upload-id", &upload,
                    "--part-number", "1", "--body", retry_bytes.to_str().unwrap()]);
                Some((uploads, writer, key, physical, upload))
            } else {
                assert!(receipt.is_ok());
                original_keys.require_recovery_key(order.key_hold).await.unwrap();
                None
            };
            assert!(discover(candidate, &vault).await.is_err());
            serving_intent.operation.operation_id = Uuid::new_v4();
            activation.begin_recovery(DATABASE, serving_intent, Some(operation.operation_id)).await.unwrap();
            assert!(discover(candidate, &empty_vault).await.is_err());
            assert!(!original_keys.generation_destroyed(order.key_hold.generation).await.unwrap());
            if authenticated {
                journal.require_authenticated(stream).await.unwrap();
                destination_keepers.shutdown().await;
                destination_keepers = RestartedKeepers::with_roles(&mut keepers, READER, Some(CUSTODIAN)).await;
                let reopened = FileKms::open(&vault, || SystemClock.now().0).unwrap();
                reopened.require_recovery_key(order.key_hold).await.unwrap();
            }
            let retired = discover(candidate, &vault).await.unwrap();
            assert_eq!(retired["journal_retired"], true);
            assert_eq!(retired["generation_destroyed"], true);
            assert_eq!(retired["objects_purged"], true);
            assert_eq!(discover(candidate, &vault).await.unwrap(), retired);
            assert!(matches!(journal.head(stream).await, Err(JournalError::Deleted)));
            assert!(journal.create_immutable(stream).await.is_err());
            if authenticated {
                assert!(journal.create_authenticated(stream).await.is_err());
            }
            assert!(original_keys.create(order.key_hold.generation).await.is_err());
            assert!(inventory.is_empty(&ObjectKey::new(format!("db/{candidate}")).unwrap()).await.unwrap());
            assert!(inventory.is_empty(&ObjectKey::new(format!("~rtdb-v1/{}/keys", candidate.simple())).unwrap()).await.unwrap());
            if let Some((uploads, writer, key, physical, upload)) = partial {
                let parts = aws_call(&bucket, "list-parts", &["--key", &physical, "--upload-id", &upload]);
                assert!(!parts.status.success());
                assert!(String::from_utf8_lossy(&parts.stderr).contains("NoSuchUpload"));
                assert!(writer.admit(&key, Uuid::new_v4(), &upload).await.is_err());
                assert!(uploads.begin().await.unwrap().is_none());
            }
        }

        let operation = serving_intent.operation;
        let Status::Pending { candidate, .. } = activation.recovery_status(DATABASE, operation).await.unwrap()
        else { panic!("expected the replacement recovery"); };
        let next_receipt = stage(operation.operation_id).await.unwrap();
        let receipt = home.path().join("serving-stage.json");
        fs::write(&receipt, serde_json::to_vec(&next_receipt).unwrap()).unwrap();
        let report = acquire(candidate, &receipt).await.unwrap();
        let serving_readback = home.path().join("serving-readback.json");
        fs::write(&serving_readback, serde_json::to_vec(&report).unwrap()).unwrap();
        let BackupCommand::AttestObjects(attester) = hosted_options("attest", candidate, Some(&serving_readback), &receipt, Some(&signing_key)).0
        else { panic!("expected a hosted attestor"); };
        let document = attester.run_with_authority(&authority_reader).await.unwrap();
        let serving_proof = home.path().join("serving-proof.json");
        fs::write(&serving_proof, serde_json::to_vec(&document).unwrap()).unwrap();
        let recorded = record(operation.operation_id, &serving_proof).await.unwrap();
        let serving_report: Readback = serde_json::from_value(report).unwrap();
        assert_eq!(recorded["installation"]["objects_sha256"], json!(serving_report.digest().unwrap()));
        let serving_authority = authority_reader.serving_authority_for_cell(DATABASE, candidate, Some(cell)).unwrap();
        assert_eq!(serving_authority.acquire().await.unwrap_err().code, "rtdb/activation-fenced");

        let held_inclusion = |root: rtdb_stream_journal::authenticated::CatalogRoot| MultiBackupOptions::parse([
            "lineage-inclusion-held".into(), "--database".into(), DATABASE.into(),
            "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
            "--authority-tls-dir".into(), authority_reader_tls.display().to_string(),
            "--expected-incarnation".into(), predecessor.incarnation.to_string(),
            "--expected-revision".into(), predecessor.revision.to_string(),
            "--catalog-root".into(), digest_hex(&root.0), "--position".into(), source_end.0.to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        let expected_inclusion = if let Some(root) = source_root {
            assert_eq!(held_inclusion(root).await.unwrap_err().code, "rtdb/held-lineage-proof-unavailable");
            Some(MultiBackupOptions::parse([
                "lineage-inclusion".into(), "--archive".into(), archive_path.display().to_string(),
                "--catalog-root".into(), digest_hex(&root.0), "--position".into(), source_end.0.to_string(),
            ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full).await.unwrap())
        } else { None };
        let published = recover("publish-recovery", vec!["--operation-id".into(), operation.operation_id.to_string()]).await.unwrap();
        assert_eq!(published["binding"]["incarnation"], candidate.to_string());
        serving_authority.acquire().await.unwrap().check().unwrap();
        if let Some((root, expected)) = source_root.zip(expected_inclusion.as_ref()) {
            fs::remove_file(&archive_path).unwrap();
            fs::remove_dir_all(&source_vault).unwrap();
            let source_objects = aws_bucket(&source_bucket, "list-object-versions", &[]);
            assert!(!source_objects["Versions"].as_array().unwrap().is_empty());
            for kind in ["Versions", "DeleteMarkers"] {
                for entry in source_objects[kind].as_array().into_iter().flatten() {
                    aws_bucket(&source_bucket, "delete-object", &[
                        "--key", entry["Key"].as_str().unwrap(), "--version-id", entry["VersionId"].as_str().unwrap(),
                    ]);
                }
            }
            aws_bucket(&source_bucket, "delete-bucket", &[]);
            assert_eq!(&held_inclusion(root).await.unwrap(), expected);
        }
        let serving_keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let mut serving = MultiDatabase::new(
            options(candidate), journal.clone(), store.clone(), serving_keys.clone(), Arc::new(|| SystemClock.now()),
        ).unwrap().with_immutable_journal().with_activation_scope(scope).unwrap()
            .with_serving_authority(serving_authority);
        let installed = serving.installed_backup().await.unwrap().unwrap();
        let serving_stream = serving.journal_stream();
        let installed_head = journal.head(serving_stream).await.unwrap();
        let terminal = serving.tx_status(&TRANSACTION_KEY).await.unwrap();
        assert!(terminal.replayed && terminal.decision_clock.is_none());
        assert_eq!(terminal.outcome, rtdb_tx::transact::Outcome::Then);
        assert_eq!(journal.head(serving_stream).await.unwrap(), installed_head);
        let proof: Attestation = serde_json::from_value(document).unwrap();
        let (installed_intent, held) = proof.held_key().unwrap();
        let key_evidence = KeyEvidence { generation: held.generation.number, authority_sha256: held.authority_sha256 };
        let signer = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let key_only = Attestation::sign_installation(
            installed_intent.clone(), installed.manifest_sha256(), &installed, Some(key_evidence), &signer,
        ).unwrap();
        let key_only_proof = home.path().join("key-only-proof.json");
        fs::write(&key_only_proof, serde_json::to_vec(&key_only).unwrap()).unwrap();
        let release_pending = || release_objects(&serving_readback, &serving_proof);
        assert!(abandon(&receipt).await.is_err());
        let release_key = |proof: &Path| ActivationOptions::parse([
            "release-recovery-key".into(), DATABASE.into(),
            "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
            "--authority-tls-dir".into(), authority_tls.display().to_string(),
            "--installation-proof".into(), proof.display().to_string(),
            "--key-vault".into(), vault.display().to_string(),
            "--journal-url".into(), journal_url.clone(), "--journal-reader-tls-dir".into(), reader_tls.display().to_string(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        assert_eq!(release_pending().await.unwrap_err().code, "rtdb/activation-custody-replacement");
        assert_eq!(release_key(&serving_proof).await.unwrap_err().code, "rtdb/activation-custody-replacement");
        assert_eq!(release_key(&key_only_proof).await.unwrap_err().code, "rtdb/activation-conflict");
        serving_keys.require_recovery_key(held).await.unwrap();
        let release_journal = |proof: &Path| ActivationOptions::parse([
            "release-recovery-journal".into(), DATABASE.into(),
            "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
            "--authority-tls-dir".into(), authority_tls.display().to_string(),
            "--installation-proof".into(), proof.display().to_string(),
            "--journal-url".into(), journal_url.clone(),
            "--journal-custodian-tls-dir".into(), custodian_tls.display().to_string(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        let mut substituted = serde_json::to_value(&proof).unwrap();
        let digest = &mut substituted["evidence"]["objects_sha256"][0];
        *digest = json!(digest.as_u64().unwrap() ^ 1);
        let mismatched = home.path().join("wrong-serving-closure.json");
        fs::write(&mismatched, serde_json::to_vec(&substituted).unwrap()).unwrap();
        assert_eq!(release_journal(&mismatched).await.unwrap_err().code, "rtdb/activation-conflict");
        assert!(!reader.journal.checkpoint_custody(journal_contract::StreamId(candidate.as_u128()))
            .await.unwrap().unwrap().released);
        release_journal(&serving_proof).await.unwrap();
        assert_eq!(release_pending().await.unwrap_err().code, "rtdb/activation-custody-replacement");
        assert_eq!(release_key(&serving_proof).await.unwrap_err().code, "rtdb/activation-custody-replacement");
        assert!(matches!(serving.tick(&live).await.unwrap(), Role::Writer(_)));
        serving.checkpoint(Vec::new()).await.unwrap();
        serving.shutdown_writer().await.unwrap();
        let previous_checkpoint = rtdb_storage::ObjectKey::new(&serving_report.checkpoint).unwrap();
        let previous_version = store.observe_head(&previous_checkpoint).await.unwrap().version.unwrap();
        aws("put-object-legal-hold", &["--key", &previous_version.key,
            "--version-id", &previous_version.version, "--legal-hold", "Status=OFF"]);
        aws("delete-object", &["--key", &previous_version.key, "--version-id", &previous_version.version]);
        let released = release_pending().await.unwrap();
        assert_eq!(released["incarnation"], candidate.to_string());
        assert_eq!(release_pending().await.unwrap(), released);
        let released_key = release_key(&serving_proof).await.unwrap();
        assert_eq!(released_key["released"]["operation"], operation.operation_id.to_string());
        assert_eq!(release_key(&serving_proof).await.unwrap(), released_key);
        for version in serving_report.held_versions.iter().filter(|version| **version != previous_version) {
            assert_eq!(observe_hold(version)["LegalHold"]["Status"], "OFF");
        }
        if let Some((root, expected)) = source_root.zip(expected_inclusion.as_ref()) {
            let lineage = serving_report.lineage.as_ref().unwrap();
            assert_eq!(observe_hold(&lineage.version)["LegalHold"]["Status"], "ON");
            assert_eq!(&held_inclusion(root).await.unwrap(), expected);

            let key = lineage.object.key(candidate, lineage.manifest).unwrap();
            let mut altered = store.get_fresh(&key).await.unwrap().to_vec();
            altered[0] ^= 1;
            fs::write(&retry_bytes, altered).unwrap();
            aws("put-object", &["--key", &lineage.version.key, "--body", retry_bytes.to_str().unwrap()]);
            let altered_version = store.observe_head(&key).await.unwrap().version.unwrap();
            assert_ne!(altered_version, lineage.version);
            assert_eq!(&held_inclusion(root).await.unwrap(), expected);

            let acquisition = ObjectKey::new(format!("recovery-custody/{authority}/{candidate}/acquisition")).unwrap();
            let mut altered_plan: Value = serde_json::from_slice(&store.get_fresh(&acquisition).await.unwrap()).unwrap();
            altered_plan["lineage"]["version"] = json!(&altered_version);
            fs::write(&retry_bytes, serde_json::to_vec(&altered_plan).unwrap()).unwrap();
            let plan_key = format!("multi/{acquisition}");
            let forged = aws("put-object", &["--key", &plan_key, "--body", retry_bytes.to_str().unwrap()]);
            assert_eq!(held_inclusion(root).await.unwrap_err().code, "rtdb/held-lineage-proof");
            aws("delete-object", &["--key", &plan_key, "--version-id", forged["VersionId"].as_str().unwrap()]);
            aws("delete-object", &["--key", &altered_version.key, "--version-id", &altered_version.version]);
            assert_eq!(&held_inclusion(root).await.unwrap(), expected);
        }
        assert_eq!(serving.installed_backup().await.unwrap().unwrap().manifest_sha256(), archive_digest);
        let replacement_head = journal.head(serving_stream).await.unwrap();
        assert!(replacement_head > installed_head);
        assert_eq!(serving.tx_status(&TRANSACTION_KEY).await.unwrap(), terminal);
        assert_eq!(journal.head(serving_stream).await.unwrap(), replacement_head);
        assert_eq!(discover(abandoned, &vault).await.unwrap(), abandoned_result);
        broker.abort();
        assert!(broker.await.unwrap_err().is_cancelled());
        let completion = format!("multi/recovery-custody/{authority}/{incarnation}/released");
        aws("delete-object", &["--key", &completion]);
        assert_eq!(release_objects(&readback, &proof_path).await.unwrap(), settled);
        aws("delete-object", &["--key", &physical.key, "--version-id", &physical.version]);
        assert_eq!(release_objects(&readback, &proof_path).await.unwrap(), settled);
        io.close_and_wait().await;
        destination_keepers.shutdown().await;
        activated_keepers.shutdown().await;
    }
}
