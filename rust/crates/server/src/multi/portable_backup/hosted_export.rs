use super::*;

#[derive(Clone, Debug)]
pub(super) struct Export {
    archive: PathBuf,
    incarnation: Uuid,
    scope: ActivationScope,
    source: HostedDatabase,
}

impl Export {
    pub(super) fn parse(mut fields: BTreeMap<String, String>) -> Result<Self> {
        let archive = backup_field(&mut fields, "--archive")?.into();
        let incarnation = backup_uuid(&backup_field(&mut fields, "--incarnation")?)?;
        let authority = backup_uuid(&backup_field(&mut fields, "--authority-id")?)?;
        let cell = fields
            .remove("--cell-id")
            .as_deref()
            .map(backup_uuid)
            .transpose()?;
        let source = HostedDatabase::parse(&mut fields)?;
        let scope = ActivationScope::for_cell(authority, &source.name, cell)?;
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self {
            archive,
            incarnation,
            scope,
            source,
        })
    }

    pub(super) async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        if self.archive.try_exists().map_err(io_error)? {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/backup-exists",
                "The portable backup path is already occupied",
            ));
        }
        let credentials = crate::cli::journal_credentials(&self.source.journal_tls)?;
        let JournalBackend { journal, batch, placement } =
            connect_stream_journal(&self.source.journal_url, &credentials).await?;
        let placement = placement.ok_or_else(backup_source)?;
        let keys = Arc::new(FileKms::open(&self.source.key_vault, || {
            SystemClock.now().0
        })?);
        let io = IoRegion::new();
        let backup = async {
            let root = rtdb_storage::ObjectKey::new("multi")?;
            let store = self
                .source
                .objects
                .open(&root, &BTreeMap::new(), Arc::clone(&io))
                .await?;
            let mut database = MultiDatabase::new(
                MultiOptions {
                    database: self.incarnation,
                    node: Ipv4Addr::LOCALHOST.into(),
                    lease_micros: WRITER_LEASE_MICROS,
                    drift_micros: CLOCK_DRIFT_MICROS,
                },
                journal,
                store,
                keys,
                Arc::new(|| SystemClock.now()),
            )?
            .with_immutable_journal()
            .with_authenticated_journal(placement.journal, false)
            .with_activation_scope(self.scope)?
            .with_batch_appender(batch)
            .with_manifest_budget(MemoryBudget::new(PortableBackup::MAX_BYTES));
            database.portable_backup().await
        }
        .await;
        io.close_and_wait().await;
        export_archive(&self.archive, backup?, apple_sync)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, Config, testing::LocalCluster};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a disposable S3-compatible loopback bucket"]
    async fn hosted_export_reads_the_commissioned_cut_with_a_read_only_principal() {
        let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
        assert_eq!(url::Url::parse(&endpoint).unwrap().host_str(), Some("127.0.0.1"));
        let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
        assert!(bucket.starts_with("rtdb-backup-hold-test-"));
        let mut config = rtdb_s3::S3Config::new(&bucket);
        config.endpoint = Some(endpoint.clone());
        config.region = Some("us-east-1".into());
        let root = rtdb_storage::ObjectKey::new("multi").unwrap();
        let io = IoRegion::new();
        let store = config.open(&root, &BTreeMap::new(), io.clone()).await.unwrap();

        const WRITER: &str = "urn:logkeeper:client:hosted-export-writer";
        const READER: &str = "urn:logkeeper:client:hosted-export-reader";
        let mut keepers = LocalCluster::start_replicated_catalog(&[WRITER]).await.unwrap();
        let placement = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(WRITER, &placement).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let home = tempfile::tempdir().unwrap();
        let vault = home.path().join("keys");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let (authority, cell, incarnation) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let name = "hosted-records";
        let scope = ActivationScope::for_cell(authority, name, Some(cell)).unwrap();
        let node = Ipv4Addr::LOCALHOST.into();
        let mut database = MultiDatabase::new(MultiOptions {
            database: incarnation, node, lease_micros: WRITER_LEASE_MICROS, drift_micros: CLOCK_DRIFT_MICROS,
        }, journal.clone(), store, keys.clone(), Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal().with_activation_scope(scope).unwrap();
        database.create().await.unwrap();
        assert!(matches!(database.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        let attachment = database.write_file(b"hosted source attachment").await.unwrap();
        database.checkpoint(vec![attachment]).await.unwrap();
        database.shutdown_writer().await.unwrap();
        let stream = database.journal_stream();
        let head = journal.head(stream).await.unwrap();
        drop(database);
        io.close_and_wait().await;

        let credentials = keepers.credentials(READER).unwrap();
        let tls = home.path().join("reader");
        fs::create_dir(&tls).unwrap();
        for (source, name) in [
            (credentials.certificate, "journal-certificate.pem"),
            (credentials.private_key, "journal-key.pem"),
            (credentials.ca, "journal-ca.pem"),
        ] {
            let file = tls.join(name);
            fs::copy(source, &file).unwrap();
            fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let configs = keepers.keepers().keys().map(|id| {
            let mut config = Config::read(&keepers.write_config(*id).unwrap()).unwrap();
            config.readers.push(READER.into());
            config
        }).collect::<Vec<_>>();
        keepers.shutdown().await.unwrap();
        let stop = CancellationToken::new();
        let mut tasks = Vec::new();
        for config in configs {
            let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
            tasks.push(tokio::spawn(config.serve(listener, stop.child_token())));
        }
        let journal_config = home.path().join("journal.json");
        fs::write(&journal_config, serde_json::to_vec(&json!({
            "keepers": placement.keepers, "catalog": placement.catalog, "voters": placement.voters,
            "catalog_voters": placement.catalog_voters, "quota_bytes": placement.quota_bytes,
            "max_record_bytes": placement.max_record_bytes,
        })).unwrap()).unwrap();
        let url = format!("{CONSENSUS_SCHEME}{}", journal_config.display());
        let archive = home.path().join("archive");
        let command = |authority: Uuid, name: &str| MultiBackupOptions::parse([
            "export-hosted".to_owned(),
            "--archive".into(), archive.display().to_string(),
            "--incarnation".into(), incarnation.to_string(),
            "--database".into(), name.to_owned(),
            "--authority-id".into(), authority.to_string(),
            "--cell-id".into(), cell.to_string(),
            "--journal-url".into(), url.clone(),
            "--tls-dir".into(), tls.display().to_string(),
            "--key-vault".into(), vault.display().to_string(),
            "--bucket".into(), bucket.clone(),
            "--region".into(), "us-east-1".into(),
            "--s3-endpoint".into(), endpoint.clone(),
        ].into_iter()).unwrap();
        let sync = rtdb_storage::AppleSync::Full;
        assert_eq!(command(Uuid::new_v4(), name).run(sync).await.unwrap_err().code, "rtdb/activation-scope");
        assert_eq!(command(authority, "another-database").run(sync).await.unwrap_err().code, "rtdb/activation-scope");
        assert!(!archive.exists());
        let report = command(authority, name).run(sync).await.unwrap();
        let original = PortableBackup::verify(&fs::read(&archive).unwrap(), keys).await.unwrap();
        assert_eq!(report["manifest_sha256"], digest_hex(&original.manifest_sha256()));
        assert_eq!(original.source_basis().incarnation(), *incarnation.as_bytes());
        assert_eq!(original.source_lsn(), head);
        assert_eq!(journal.head(stream).await.unwrap(), head);
        assert_eq!(command(authority, name).run(sync).await.unwrap_err().code, "rtdb/backup-exists");
        stop.cancel();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }
}
