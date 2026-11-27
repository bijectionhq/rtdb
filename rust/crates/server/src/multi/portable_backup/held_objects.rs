use super::*;

#[derive(Clone, Debug)]
pub(super) struct Verifier {
    archive: PathBuf,
    incarnation: Uuid,
    scope: ActivationScope,
    journal_url: String,
    journal_tls: PathBuf,
    key_vault: PathBuf,
    objects: rtdb_s3::S3Config,
}

impl Verifier {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let mut take = |name: &str| fields.remove(name).ok_or_else(arguments_error);
        let archive = take("--archive")?.into();
        let incarnation = uuid(&take("--incarnation")?)?;
        let authority = uuid(&take("--authority-id")?)?;
        let journal_url = take("--journal-url")?;
        if !journal_url.starts_with(CONSENSUS_SCHEME) {
            return Err(arguments_error());
        }
        let journal_tls = take("--tls-dir")?.into();
        let key_vault = take("--key-vault")?.into();
        let mut objects = rtdb_s3::S3Config::new(take("--bucket")?);
        objects.region = fields.remove("--region");
        objects.endpoint = fields.remove("--s3-endpoint");
        let cell = fields.remove("--cell-id").as_deref().map(uuid).transpose()?;
        let scope = ActivationScope::for_cell(authority, DEVELOPMENT_DATABASE, cell)?;
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self { archive, incarnation, scope, journal_url, journal_tls, key_vault, objects })
    }

    pub(super) async fn run(self) -> Result<Value> {
        let digest = Sha256::digest(read_archive(&self.archive)?).into();
        let credentials = crate::cli::journal_credentials(&self.journal_tls)?;
        let JournalBackend { journal, batch, .. } = connect_stream_journal(&self.journal_url, &credentials).await?;
        let keys = Arc::new(FileKms::open(&self.key_vault, || SystemClock.now().0)?);
        let root = rtdb_storage::ObjectKey::new("multi")?;
        let remotes = BTreeMap::new();
        let inventory = self.objects.versioned_inventory(&root, &remotes).await?;
        inventory.require_versioned().await?;
        let io = IoRegion::new();
        let store = self.objects.open(&root, &remotes, Arc::clone(&io)).await?;
        let mut database = MultiDatabase::new(
            MultiOptions {
                database: self.incarnation,
                node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS,
                drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal, store, keys, Arc::new(|| SystemClock.now()),
        )?.with_immutable_journal().with_batch_appender(batch).with_activation_scope(self.scope)?;
        let checked = database.check_installed_backup_object_holds(digest, &inventory).await;
        io.close_and_wait().await;
        let checked = checked?;
        Ok(json!({
            "incarnation": self.incarnation,
            "archive_sha256": digest_hex(&checked.installation.manifest_sha256()),
            "journal_head": checked.journal_head.0,
            "checkpoint": checked.checkpoint.to_string(),
            "held_versions": checked.versions,
            "object_holds_verified": true,
        }))
    }
}

fn uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).ok().filter(|id| !id.is_nil()).ok_or_else(arguments_error)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
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
        let mut source = MultiDatabase::new(options(Uuid::new_v4()), journal.clone(),
            Arc::new(rtdb_storage::MemoryStore::default()), source_keys.clone(), Arc::new(|| SystemClock.now()))
            .unwrap().with_immutable_journal();
        source.create().await.unwrap();
        assert!(matches!(source.tick(&live).await.unwrap(), Role::Writer(_)));
        let attachment = source.write_file(b"portable object").await.unwrap();
        source.checkpoint(vec![attachment]).await.unwrap();
        let archive = source.portable_backup().await.unwrap().into_bytes();
        let archive_path = home.path().join("archive");
        fs::write(&archive_path, &archive).unwrap();
        let backup = PortableBackup::verify(&archive, source_keys).await.unwrap();

        let mut s3 = rtdb_s3::S3Config::new(&bucket);
        s3.endpoint = Some(endpoint.clone());
        s3.region = Some("us-east-1".into());
        let io = IoRegion::new();
        let root = rtdb_storage::ObjectKey::new("multi").unwrap();
        let store = s3.open(&root, &BTreeMap::new(), Arc::clone(&io)).await.unwrap();
        let vault = home.path().join("destination");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let incarnation = Uuid::new_v4();
        let authority = Uuid::new_v4();
        let cell = Uuid::new_v4();
        let scope = ActivationScope::for_cell(authority, DEVELOPMENT_DATABASE, Some(cell)).unwrap();
        let mut staged = MultiDatabase::new(options(incarnation), journal.clone(),
            store.clone(), keys, Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal().with_activation_scope(scope).unwrap();
        staged.create().await.unwrap();
        assert!(matches!(staged.tick(&live).await.unwrap(), Role::Writer(_)));
        staged.install_portable_backup(backup).await.unwrap();
        let relocated = staged.installed_backup_file_relocations().await.unwrap();
        let [relocated] = relocated.as_slice() else { panic!("expected one relocated file"); };
        let file = rtdb_storage::ObjectKey::new(relocated.destination_file.to_string()).unwrap();
        drop(staged);

        let verify = || MultiBackupOptions::parse([
            "verify-object-holds".to_owned(), "--archive".into(), archive_path.display().to_string(),
            "--incarnation".into(), incarnation.to_string(), "--authority-id".into(), authority.to_string(),
            "--cell-id".into(), cell.to_string(), "--key-vault".into(), vault.display().to_string(),
            "--journal-url".into(), journal_url.clone(), "--tls-dir".into(), tls.display().to_string(),
            "--bucket".into(), bucket.clone(), "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter()).unwrap().run(rtdb_storage::AppleSync::Full);
        let stream = StreamId(incarnation.as_u128());
        let head = journal.head(stream).await.unwrap();
        let objects = store.list(None).await.unwrap();
        assert_eq!(objects.len(), 4);
        let token = &objects.iter().find(|entry| entry.key.as_str().starts_with("~rtdb-v1/")).unwrap().key;
        assert!(verify().await.is_err());
        let hold = |version: &rtdb_types::PhysicalObjectVersion| {
            aws("put-object-legal-hold", &["--key", &version.key,
                "--version-id", &version.version, "--legal-hold", "Status=ON"]);
        };
        for entry in objects.iter().filter(|entry| entry.key != file && &entry.key != token) {
            hold(&store.observe_head(&entry.key).await.unwrap().version.unwrap());
        }
        assert!(verify().await.is_err());
        let physical = store.observe_head(&file).await.unwrap().version.unwrap();
        hold(&physical);
        assert!(verify().await.is_err());
        hold(&store.observe_head(token).await.unwrap().version.unwrap());
        let result = verify().await.unwrap();
        assert_eq!(result["object_holds_verified"], true);
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
        assert!(verify().await.is_err());
        hold(&replacement);
        let new = verify().await.unwrap();
        assert_eq!(new["checkpoint"], result["checkpoint"]);
        assert_eq!(new["archive_sha256"], result["archive_sha256"]);
        let verified = new["held_versions"].as_array().unwrap();
        assert!(!verified.contains(&json!(physical)));
        assert!(verified.contains(&json!(replacement)));
        assert_eq!(journal.head(stream).await.unwrap(), head);
        io.close_and_wait().await;
        keepers.shutdown().await.unwrap();
    }
}
