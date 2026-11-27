use crate::multi::activation::operator::attestation::KeyEvidence;

use super::*;
use rtdb_crypt::RecoveryKeyHold;
use rtdb_types::PhysicalObjectVersion;

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Readback {
    incarnation: Uuid,
    archive_sha256: String,
    journal_head: u64,
    checkpoint: String,
    held_versions: Vec<PhysicalObjectVersion>,
    object_holds_verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_hold: Option<RecoveryKeyHold>,
}

fn read_receipt<T: serde::de::DeserializeOwned>(path: &Path, max_bytes: u64) -> Result<T> {
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

#[derive(Clone, Debug)]
pub(super) struct Verifier {
    archive: PathBuf,
    readback: Option<PathBuf>,
    stage_receipt: Option<PathBuf>,
    incarnation: Uuid,
    authority: Uuid,
    scope: ActivationScope,
    destination: HostedDestination,
}

impl Verifier {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let archive = backup_field(&mut fields, "--archive")?.into();
        let incarnation = backup_uuid(&backup_field(&mut fields, "--incarnation")?)?;
        let authority = backup_uuid(&backup_field(&mut fields, "--authority-id")?)?;
        let destination = HostedDestination::parse(&mut fields)?;
        let readback = fields.remove("--readback").map(PathBuf::from);
        let stage_receipt = fields.remove("--stage-receipt").map(PathBuf::from);
        let cell = fields.remove("--cell-id").as_deref().map(backup_uuid).transpose()?;
        let scope = ActivationScope::for_cell(authority, DEVELOPMENT_DATABASE, cell)?;
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self { archive, readback, stage_receipt, incarnation, authority, scope, destination })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let digest = Sha256::digest(read_archive(&self.archive)?).into();
        let receipt = self.stage_receipt.as_deref()
            .map(|path| read_receipt::<staged::StageReceipt>(path, 4096)).transpose()?;
        if let Some(receipt) = &receipt {
            if receipt.intent.authority_id != self.authority || receipt.intent.scope()? != self.scope
                || receipt.intent.candidate()? != self.incarnation || receipt.manifest_sha256 != digest_hex(&digest)
            {
                return Err(invalid_readback());
            }
        }
        let requested = self.readback.as_deref().map(|path| read_receipt::<Readback>(path, 32 << 20)).transpose()?;
        if requested.as_ref().is_some_and(|readback| {
            readback.incarnation != self.incarnation || readback.archive_sha256 != digest_hex(&digest)
                || !readback.object_holds_verified || readback.held_versions.is_empty()
                || readback.key_hold != receipt.as_ref().map(|receipt| receipt.key_hold)
        }) {
            return Err(invalid_readback());
        }
        let credentials = crate::cli::journal_credentials(&self.destination.journal_tls)?;
        let JournalBackend { journal, batch, .. } = connect_stream_journal(&self.destination.journal_url, &credentials).await?;
        let keys = Arc::new(FileKms::open(&self.destination.key_vault, || SystemClock.now().0)?);
        let root = rtdb_storage::ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.destination.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let io = IoRegion::new();
        let store = match &requested {
            Some(readback) => inventory.pinned_read_store(&readback.held_versions)?,
            None => self.destination.objects.open(&root, &remotes, Arc::clone(&io)).await?,
        };
        let mut database = MultiDatabase::new(
            MultiOptions {
                database: self.incarnation,
                node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS,
                drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal, store, keys.clone(), Arc::new(|| SystemClock.now()),
        )?.with_immutable_journal().with_batch_appender(batch).with_activation_scope(self.scope)?;
        let checked = database.check_installed_backup_object_holds(digest, &inventory).await;
        io.close_and_wait().await;
        let checked = checked?;
        let key_hold = if let Some(receipt) = receipt {
            let key = KeyEvidence { generation: checked.generation.number, authority_sha256: receipt.key_hold.authority_sha256 };
            let hold = receipt.intent.installed_key_hold(&checked.installation, key)?;
            if hold != receipt.key_hold || hold.generation != checked.generation {
                return Err(invalid_readback());
            }
            keys.require_recovery_key(hold).await?;
            Some(hold)
        } else { None };
        let current = Readback {
            incarnation: self.incarnation,
            archive_sha256: digest_hex(&checked.installation.manifest_sha256()),
            journal_head: checked.journal_head.0,
            checkpoint: checked.checkpoint.to_string(),
            held_versions: checked.versions,
            object_holds_verified: true,
            key_hold,
        };
        if requested.is_some_and(|previous| previous != current) {
            return Err(invalid_readback());
        }
        serde_json::to_value(current)
            .map_err(|error| Anomaly::fault("rtdb/backup-object-readback", error.to_string()))
    }
}

fn invalid_readback() -> Anomaly {
    Anomaly::incorrect("rtdb/backup-object-readback", "The object readback must match the verified staged backup")
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::multi::activation::{
        ActivationAuthority, ActivationOptions, operator::attestation::Attestation,
        recovery::{Intent, LossPolicy, Operation, Status},
    };
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a disposable loopback S3 bucket with Object Lock and the AWS CLI"]
    async fn staged_backup_checks_exact_provider_holds_on_its_complete_object_closure() {
        let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
        let url = url::Url::parse(&endpoint).unwrap();
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
        assert!(bucket.starts_with("rtdb-backup-hold-test-"));
        let aws = |operation: &str, fields: &[&str]| {
            let result = std::process::Command::new("aws")
                .args(["--no-cli-pager", "--endpoint-url", &endpoint, "--region", "us-east-1",
                    "s3api", operation, "--bucket", &bucket])
                .args(fields).output().unwrap();
            assert!(result.status.success(), "{operation}: {}", String::from_utf8_lossy(&result.stderr));
        };

        const PRINCIPAL: &str = "urn:logkeeper:client:backup-object-holds";
        let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
        let placement = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(PRINCIPAL, &placement).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let home = tempfile::tempdir().unwrap();
        let journal_config = home.path().join("journal.json");
        fs::write(&journal_config, serde_json::to_vec(&json!({
            "keepers": placement.keepers, "catalog": placement.catalog, "voters": placement.voters,
            "catalog_voters": placement.catalog_voters, "quota_bytes": placement.quota_bytes,
            "max_record_bytes": placement.max_record_bytes,
        })).unwrap()).unwrap();
        let journal_url = format!("{CONSENSUS_SCHEME}{}", journal_config.display());
        let tls = home.path().join("tls");
        fs::create_dir(&tls).unwrap();
        let credentials = keepers.credentials(PRINCIPAL).unwrap();
        for (source, name) in [(credentials.certificate, "journal-certificate.pem"),
            (credentials.private_key, "journal-key.pem"), (credentials.ca, "journal-ca.pem")]
        {
            let file = tls.join(name);
            fs::copy(source, &file).unwrap();
            fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let options = |database| MultiOptions {
            database, node: Ipv4Addr::LOCALHOST.into(), lease_micros: WRITER_LEASE_MICROS,
            drift_micros: CLOCK_DRIFT_MICROS,
        };
        let live = BTreeSet::from([Ipv4Addr::LOCALHOST.into()]);
        let source_vault = home.path().join("source");
        FileKms::create_directory(&source_vault).unwrap();
        let source_keys = Arc::new(FileKms::open(&source_vault, || SystemClock.now().0).unwrap());
        let source_id = Uuid::new_v4();
        let mut source = MultiDatabase::new(options(source_id), journal.clone(),
            Arc::new(rtdb_storage::MemoryStore::default()), source_keys, Arc::new(|| SystemClock.now()))
            .unwrap().with_immutable_journal();
        source.create().await.unwrap();
        assert!(matches!(source.tick(&live).await.unwrap(), Role::Writer(_)));
        let attachment = source.write_file(b"portable object").await.unwrap();
        source.checkpoint(vec![attachment]).await.unwrap();
        let archive = source.portable_backup().await.unwrap().into_bytes();
        let archive_path = home.path().join("archive");
        fs::write(&archive_path, &archive).unwrap();
        let mut s3 = rtdb_s3::S3Config::new(&bucket);
        s3.endpoint = Some(endpoint.clone());
        s3.region = Some("us-east-1".into());
        let io = IoRegion::new();
        let root = rtdb_storage::ObjectKey::new("multi").unwrap();
        let store = s3.open(&root, &BTreeMap::new(), Arc::clone(&io)).await.unwrap();
        let vault = home.path().join("destination");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let authority = Uuid::new_v4();
        let cell = Uuid::new_v4();
        const AUTHORITY_PRINCIPAL: &str = "urn:logkeeper:client:backup-activation";
        let mut authority_keepers = LocalCluster::start_replicated_catalog(&[AUTHORITY_PRINCIPAL]).await.unwrap();
        let authority_placement = authority_keepers.journal_config(64 << 20, 4 << 20);
        let authority_journal = Arc::new(authority_keepers.journal(AUTHORITY_PRINCIPAL, &authority_placement).unwrap());
        authority_journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let authority_config = home.path().join("authority.json");
        fs::write(&authority_config, serde_json::to_vec(&json!({
            "keepers": authority_placement.keepers, "catalog": authority_placement.catalog,
            "voters": authority_placement.voters, "catalog_voters": authority_placement.catalog_voters,
            "quota_bytes": authority_placement.quota_bytes, "max_record_bytes": authority_placement.max_record_bytes,
        })).unwrap()).unwrap();
        let authority_url = format!("{CONSENSUS_SCHEME}{}", authority_config.display());
        let authority_tls = home.path().join("authority-tls");
        fs::create_dir(&authority_tls).unwrap();
        let credentials = authority_keepers.credentials(AUTHORITY_PRINCIPAL).unwrap();
        for (source, name) in [(credentials.certificate, "journal-certificate.pem"),
            (credentials.private_key, "journal-key.pem"), (credentials.ca, "journal-ca.pem")]
        {
            let file = authority_tls.join(name);
            fs::copy(source, &file).unwrap();
            fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let authority_reader = ActivationAuthority::connect_reader(
            &authority_url, crate::cli::journal_credentials(&authority_tls).unwrap(), authority,
        ).await.unwrap();
        let activation = ActivationAuthority::new(authority_journal.clone(), authority).unwrap();
        let predecessor = activation.bootstrap(DEVELOPMENT_DATABASE, source_id).await.unwrap();
        let operation_id = Uuid::new_v4();
        let intent = Intent {
            operation: Operation { predecessor, operation_id },
            destination_cell: Some(cell),
            loss_policy: LossPolicy::AcceptUnknownTail,
        };
        let stage = |operation_id: Uuid| {
            let opts = MultiBackupOptions::parse([
                "stage".into(), "--archive".into(), archive_path.display().to_string(),
                "--operation-id".into(), operation_id.to_string(),
                "--expected-incarnation".into(), source_id.to_string(),
                "--expected-revision".into(), predecessor.revision.to_string(),
                "--source-key-vault".into(), source_vault.display().to_string(),
                "--key-vault".into(), vault.display().to_string(),
                "--journal-url".into(), journal_url.clone(), "--tls-dir".into(), tls.display().to_string(),
                "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
                "--s3-endpoint".into(), endpoint.clone(),
            ].into_iter()).unwrap();
            let BackupCommand::Stage(stage) = opts.0 else { panic!("expected a stage command"); };
            stage.run_with_authority(&authority_reader)
        };
        let lossless_id = Uuid::new_v4();
        let unsealed = Intent {
            operation: Operation { predecessor, operation_id: lossless_id },
            loss_policy: LossPolicy::Lossless,
            ..intent
        };
        let Status::Pending { candidate: unsealed_candidate, .. } =
            activation.begin_recovery(DEVELOPMENT_DATABASE, unsealed, None).await.unwrap()
        else { panic!("expected a pending lossless recovery"); };
        assert!(stage(lossless_id).await.is_err());
        assert!(matches!(journal.head(StreamId(unsealed_candidate.as_u128())).await, Err(JournalError::NotFound)));
        let Status::Pending { candidate: incarnation, .. } =
            activation.begin_recovery(DEVELOPMENT_DATABASE, intent, Some(lossless_id)).await.unwrap()
        else { panic!("expected a pending recovery"); };
        let stream = StreamId(incarnation.as_u128());
        let activation_stream = activation.commissioning_stream_id(DEVELOPMENT_DATABASE).unwrap();
        let activation_head = authority_journal.head(activation_stream).await.unwrap();
        assert!(stage(Uuid::new_v4()).await.is_err());
        assert!(matches!(journal.head(stream).await, Err(JournalError::NotFound)));
        assert!(store.list(None).await.unwrap().is_empty());
        let receipt = stage(operation_id).await.unwrap();
        let head = journal.head(stream).await.unwrap();
        assert_eq!(receipt["destination_incarnation"], incarnation.to_string());
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

        let scope = ActivationScope::for_cell(authority, DEVELOPMENT_DATABASE, Some(cell)).unwrap();
        let mut staged = MultiDatabase::new(options(incarnation), journal.clone(),
            store.clone(), keys, Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal().with_activation_scope(scope).unwrap();
        let installed = staged.installed_backup().await.unwrap().unwrap();
        let stage_receipt: staged::StageReceipt = serde_json::from_value(receipt.clone()).unwrap();
        let signing_key = ring::signature::Ed25519KeyPair::from_seed_unchecked(&[47; 32]).unwrap();
        let proof = Attestation::sign_installation(
            stage_receipt.intent, installed.manifest_sha256(), &installed,
            Some(KeyEvidence { generation: key_hold.generation.number, authority_sha256: key_hold.authority_sha256 }),
            &signing_key,
        ).unwrap();
        assert_eq!(proof.held_key().unwrap().1, key_hold);
        let proof_path = home.path().join("installation-proof.json");
        fs::write(&proof_path, serde_json::to_vec(&proof).unwrap()).unwrap();
        let release = || ActivationOptions::parse([
            "release-recovery-key".into(), DEVELOPMENT_DATABASE.into(),
            "--authority-id".into(), authority.to_string(), "--authority-url".into(), authority_url.clone(),
            "--authority-tls-dir".into(), authority_tls.display().to_string(),
            "--installation-proof".into(), proof_path.display().to_string(),
            "--key-vault".into(), vault.display().to_string(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        assert!(release().await.is_err());
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
        let verify = |readback: Option<&Path>, receipt: &Path| MultiBackupOptions::parse([
            "verify-object-holds".to_owned(), "--archive".into(), archive_path.display().to_string(),
            "--incarnation".into(), incarnation.to_string(), "--authority-id".into(), authority.to_string(),
            "--stage-receipt".into(), receipt.display().to_string(),
            "--cell-id".into(), cell.to_string(), "--key-vault".into(), vault.display().to_string(),
            "--journal-url".into(), journal_url.clone(), "--tls-dir".into(), tls.display().to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter().chain(readback.into_iter().flat_map(|path| [
            "--readback".into(), path.display().to_string(),
        ]))).unwrap().run(rtdb_storage::AppleSync::Full);
        let objects = store.list(None).await.unwrap();
        assert_eq!(objects.len(), 4);
        let token = &objects.iter().find(|entry| entry.key.as_str().starts_with("~rtdb-v1/")).unwrap().key;
        assert!(verify(None, &receipt_path).await.is_err());
        let hold = |version: &rtdb_types::PhysicalObjectVersion| {
            aws("put-object-legal-hold", &["--key", &version.key,
                "--version-id", &version.version, "--legal-hold", "Status=ON"]);
        };
        for entry in objects.iter().filter(|entry| entry.key != file && &entry.key != token) {
            hold(&store.observe_head(&entry.key).await.unwrap().version.unwrap());
        }
        assert!(verify(None, &receipt_path).await.is_err());
        let physical = store.observe_head(&file).await.unwrap().version.unwrap();
        hold(&physical);
        assert!(verify(None, &receipt_path).await.is_err());
        hold(&store.observe_head(token).await.unwrap().version.unwrap());
        assert!(verify(None, &wrong_receipt_path).await.is_err());
        let result = verify(None, &receipt_path).await.unwrap();
        assert_eq!(result["object_holds_verified"], true);
        assert_eq!(result["key_hold"], receipt["key_hold"]);
        assert_eq!(result["incarnation"], incarnation.to_string());
        assert_eq!(result["archive_sha256"], digest_hex(&Sha256::digest(&archive).into()));
        assert_eq!(result["journal_head"], head.0);
        assert_eq!(result["held_versions"].as_array().unwrap().len(), 4);
        assert!(result["held_versions"].as_array().unwrap().contains(&json!(physical)));

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

        let readback = home.path().join("readback.json");
        fs::write(&readback, serde_json::to_vec(&result).unwrap()).unwrap();
        let checkpoint = rtdb_storage::ObjectKey::new(result["checkpoint"].as_str().unwrap()).unwrap();
        let checkpoint_version = store.observe_head(&checkpoint).await.unwrap().version.unwrap();
        aws("delete-object", &["--key", &checkpoint_version.key]);
        assert!(verify(None, &receipt_path).await.is_err());
        assert_eq!(verify(Some(&readback), &receipt_path).await.unwrap(), result);

        let wrong_readback = home.path().join("wrong-readback.json");
        let mut wrong = result.clone();
        wrong["journal_head"] = json!(head.0 + 1);
        fs::write(&wrong_readback, serde_json::to_vec(&wrong).unwrap()).unwrap();
        assert!(verify(Some(&wrong_readback), &receipt_path).await.is_err());

        aws("put-object-legal-hold", &["--key", &physical.key,
            "--version-id", &physical.version, "--legal-hold", "Status=OFF"]);
        assert!(verify(Some(&readback), &receipt_path).await.is_err());
        hold(&physical);
        let replacement_intent = Intent {
            operation: Operation { predecessor, operation_id: Uuid::new_v4() },
            ..intent
        };
        activation.begin_recovery(DEVELOPMENT_DATABASE, replacement_intent, Some(operation_id)).await.unwrap();
        assert!(stage(operation_id).await.is_err());
        release().await.unwrap();
        assert!(verify(Some(&readback), &receipt_path).await.is_err());
        assert_eq!(journal.head(stream).await.unwrap(), head);
        io.close_and_wait().await;
        keepers.shutdown().await.unwrap();
        authority_keepers.shutdown().await.unwrap();
    }
}
