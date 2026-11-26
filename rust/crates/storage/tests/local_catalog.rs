use bytes::Bytes;
use rtdb_format::trie_retirement::{RetirementFile, RetirementIntent, intent_digest, journal_directory};
use rtdb_storage::{Error, LocalStore, ObjectKey, ObjectStore};
use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, StorageVersion, TableRef};

#[tokio::test]
async fn a_local_trie_release_waits_for_held_catalogs_and_refuses_stale_reopens() -> rtdb_types::Result<()> {
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

    let old = reader.retain_catalog(Some(selected)).await?.unwrap();
    let current = reader.retain_catalog(Some(installed)).await?.unwrap();
    assert!(!collector.release_trie_files(&receipt, bytes.clone()).await?);
    assert!(collector.head(&file).await.is_ok());

    drop(old);
    let mut forged = bytes.to_vec();
    forged[7] = 3;
    assert_eq!(collector.release_trie_files(&receipt, forged.into()).await.unwrap_err().code, "rtdb.storage/trie-release");
    assert_eq!(reader.retain_catalog(Some(selected)).await.err().unwrap().code, "rtdb.storage/catalog-not-current");

    let recovery = reader.hold_recovery_open().await?;
    let old_catalog = ObjectKey::new("blocks/b00.binpb")?;
    collector.put(&old_catalog, Bytes::from_static(b"protected checkpoint metadata")).await?;
    assert!(!collector.release_trie_files(&receipt, bytes.clone()).await?);
    collector.collect_recovery_file(&old_catalog).await?;
    assert_eq!(reader.get(&old_catalog).await?, Bytes::from_static(b"protected checkpoint metadata"));
    assert!(reader.head(&file).await.is_ok());
    drop(recovery);

    collector.collect_recovery_file(&old_catalog).await?;
    assert!(matches!(reader.head(&old_catalog).await, Err(Error::MissingObject(_))));
    assert!(!collector.release_trie_files(&receipt, bytes.clone()).await?, "a hold without a file inventory keeps every exact file");
    assert!(reader.head(&file).await.is_ok());
    drop(current);
    assert!(collector.release_trie_files(&receipt, bytes.clone()).await?);
    let new_reader = open();
    assert_eq!(new_reader.retain_catalog(Some(selected)).await.err().unwrap().code, "rtdb.storage/catalog-not-current");
    assert_eq!(new_reader.retain_catalog(None).await.err().unwrap().code, "rtdb.storage/catalog-not-current");
    assert!(new_reader.retain_catalog(Some(installed)).await?.is_some());
    assert!(collector.release_trie_files(&receipt, bytes).await?);
    assert!(matches!(collector.head(&file).await, Err(Error::MissingObject(_))));
    assert!(collector.list(None).await?.iter().all(|object| !object.key.as_str().contains("catalog-ledger")));
    Ok(())
}

#[tokio::test]
async fn a_newer_local_journal_keeps_prior_holds_and_can_install_its_own_floor() -> rtdb_types::Result<()> {
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
    let old_hold = predecessor.retain_catalog(Some(old)).await?.unwrap();

    let successor = open(replica_epoch);
    let checkpoint = successor.retain_catalog(None).await?.unwrap();
    assert!(successor.retain_catalog(Some(old)).await.is_err());
    assert!(!successor.advance_catalog_floor(current).await?);
    drop(old_hold);
    assert!(!successor.advance_catalog_floor(current).await?);
    drop(checkpoint);
    assert!(successor.advance_catalog_floor(current).await?);

    assert!(predecessor.retain_catalog(Some(old)).await.is_err());
    assert!(predecessor.advance_catalog_floor(old).await.is_err());
    assert!(successor.retain_catalog(Some(current)).await?.is_some());
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
async fn reads_of_one_catalog_share_a_storage_hold_that_lapses_after_its_readers() -> rtdb_types::Result<()> {
    let location = tempfile::tempdir().unwrap();
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
    let pool = rtdb_storage::cache::BufferPool::new(store, 1 << 20);
    let cut = CatalogCut {
        storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0),
        through: ReplicaPosition(1),
    };

    let first = pool.hold_catalog(Some(cut)).await?;
    let concurrent = pool.hold_catalog(Some(cut)).await?;
    assert!(std::sync::Arc::ptr_eq(&first, &concurrent));
    assert_eq!(reader_tickets(location.path()), 1);
    drop((first, concurrent));

    let consecutive = pool.hold_catalog(Some(cut)).await?;
    assert_eq!(reader_tickets(location.path()), 1, "a read right after the last one reuses its hold");
    let newer = pool.hold_catalog(Some(CatalogCut { storage_version: StorageVersion(7), ..cut })).await?;
    assert!(!std::sync::Arc::ptr_eq(&consecutive, &newer));
    assert_eq!(reader_tickets(location.path()), 2);
    drop((consecutive, newer));

    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    assert_eq!(reader_tickets(location.path()), 0, "an idle pool keeps no catalog held");
    Ok(())
}
