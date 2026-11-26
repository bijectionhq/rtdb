use bytes::Bytes;
use rtdb_format::trie_retirement::{RetirementFile, RetirementIntent, intent_digest, journal_directory};
use rtdb_storage::{Error, LocalStore, ObjectKey, ObjectStore};
use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, StorageVersion, TableRef};

#[tokio::test]
async fn a_local_trie_release_waits_for_recovery_but_never_for_a_reader() -> rtdb_types::Result<()> {
    let location = tempfile::tempdir().unwrap();
    let open = || LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0));
    let reader = open();
    let collector = open();
    let selected = CatalogCut {
        storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
        through: ReplicaPosition(1),
    };
    let installed = CatalogCut { through: ReplicaPosition(2), ..selected };
    let file = ObjectKey::new("tables/orders/data/l01-rc-b00.arrow")?;
    collector.put(&file, Bytes::from_static(b"retired data")).await?;
    let plan = serde_json::to_vec(&RetirementIntent {
        selected, table: TableRef::new("public", "orders"), tries: vec!["l01-rc-b00".into()],
        files: vec![RetirementFile { key: file.to_string(), version: None, matches_publication: false, terminal_publication: None }],
        after_reset: None,
    }).unwrap();
    let receipt = ObjectKey::new(format!("{}/installed/{}", journal_directory(selected), intent_digest(&plan)))?;
    let bytes = Bytes::from([installed.through.0.to_be_bytes().as_slice(), &plan].concat());
    collector.put(&receipt, bytes.clone()).await?;

    let mut forged = bytes.to_vec();
    forged[7] = 3;
    assert_eq!(collector.release_trie_files(&receipt, forged.into()).await.unwrap_err().code, "rtdb.storage/trie-release");

    let recovery = reader.hold_recovery_open().await?;
    let old_catalog = ObjectKey::new("blocks/b00.binpb")?;
    collector.put(&old_catalog, Bytes::from_static(b"protected checkpoint metadata")).await?;
    assert!(!collector.release_trie_files(&receipt, bytes.clone()).await?, "recovery custody still defers the release");
    collector.collect_recovery_file(&old_catalog).await?;
    assert_eq!(reader.get(&old_catalog).await?, Bytes::from_static(b"protected checkpoint metadata"));
    assert!(reader.head(&file).await.is_ok());
    drop(recovery);

    collector.collect_recovery_file(&old_catalog).await?;
    assert!(matches!(reader.head(&old_catalog).await, Err(Error::MissingObject(_))));
    // The retirement was selected from tries superseded before the GC window, so no reader of an older catalog holds it.
    assert!(collector.release_trie_files(&receipt, bytes.clone()).await?);
    assert!(matches!(collector.head(&file).await, Err(Error::MissingObject(_))));
    assert!(collector.release_trie_files(&receipt, bytes).await?, "a repeated release is idempotent");
    assert!(reader.require_erasure_catalog(Some(selected)).await.is_ok(), "a retirement installs no floor");
    assert!(collector.list(None).await?.iter().all(|object| !object.key.as_str().contains("catalog-ledger")));
    assert_eq!(reader_tickets(location.path()), 0);
    Ok(())
}

#[tokio::test]
async fn a_newer_local_journal_retires_without_waiting_for_the_prior_journal() -> rtdb_types::Result<()> {
    let location = tempfile::tempdir().unwrap();
    let open = |epoch| LocalStore::open(location.path()).unwrap().with_catalog_ledger(epoch);
    let old = CatalogCut {
        storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
        through: ReplicaPosition(1),
    };
    let replica_epoch = StorageEpoch(1);
    let current = CatalogCut {
        replica_epoch, through: ReplicaPosition::from_epoch_offset(replica_epoch, 1)?, ..old
    };
    let predecessor = open(old.replica_epoch);
    assert!(predecessor.advance_catalog_floor(old).await?);
    let successor = open(replica_epoch);
    assert!(successor.advance_catalog_floor(current).await?);
    assert!(predecessor.advance_catalog_floor(current).await.is_err(), "a ledger refuses a cut of another journal");
    Ok(())
}

#[tokio::test]
async fn malformed_repair_anchor_prevents_collection_after_reopen() -> rtdb_types::Result<()> {
    let location = tempfile::tempdir().unwrap();
    let store = LocalStore::open(location.path())?;
    let file = ObjectKey::new("oversized/b00-payload.binpb")?;
    store.put(&file, Bytes::from_static(b"replica bytes")).await?;
    store.install_repair_anchor(rtdb_storage::LocalRepairAnchor {
        block: rtdb_types::BlockIndex(1), boundary: ReplicaPosition(7), sha256: [1; 32],
        primary_inventory_sha256: [2; 32],
    }).await?;
    drop(store);
    std::fs::write(location.path().join(".tmp/catalog-ledger/repair-anchor"), b"{").unwrap();

    let reopened = LocalStore::open(location.path())?;
    assert_eq!(reopened.collect_recovery_file(&file).await.unwrap_err().code, "rtdb.storage/catalog-ledger");
    assert_eq!(reopened.get(&file).await?, Bytes::from_static(b"replica bytes"));
    Ok(())
}

fn reader_tickets(root: &std::path::Path) -> usize {
    std::fs::read_dir(root.join(".tmp/catalog-ledger")).map_or(0, |entries| {
        entries.filter(|entry| entry.as_ref().is_ok_and(|entry| entry.file_name().to_string_lossy().starts_with("reader-"))).count()
    })
}

#[tokio::test]
async fn reading_a_catalog_registers_nothing_and_never_blocks_the_floor() -> rtdb_types::Result<()> {
    let location = tempfile::tempdir().unwrap();
    let open = || LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0));
    let reader: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(open());
    let collector = open();
    let pool = rtdb_storage::cache::BufferPool::new(reader, 1 << 20);
    let selected = CatalogCut {
        storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
        through: ReplicaPosition(1),
    };
    let installed = CatalogCut { through: ReplicaPosition(2), ..selected };
    for _ in 0..100 {
        assert!(pool.admit_catalog(Some(selected), None).await?.is_none(), "local storage grants no per-read custody");
    }
    assert_eq!(reader_tickets(location.path()), 0, "no reader ticket is written on the read path");
    assert!(collector.advance_catalog_floor(installed).await?, "an older reader never holds the floor back");
    assert!(pool.admit_catalog(Some(selected), None).await?.is_none(), "a read older than a retirement is not refused up front");
    Ok(())
}
