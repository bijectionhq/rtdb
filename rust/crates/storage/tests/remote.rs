#![cfg(feature = "remote")]

use bytes::Bytes;
use rtdb_storage::{Error, ObjectKey, ObjectStore, remote::RemoteStore};
use std::sync::Arc;

#[tokio::test]
async fn fresh_exact_reads_accept_only_a_complete_object_of_the_published_length() {
    let provider = Arc::new(object_store::memory::InMemory::new());
    let store = RemoteStore::new(provider, None);
    let key = ObjectKey::new("ordered-index/page").unwrap();
    store.put(&key, Bytes::from_static(b"page")).await.unwrap();
    assert_eq!(store.get_fresh_exact(&key, 4).await.unwrap(), Bytes::from_static(b"page"));
    assert!(store.get_fresh_exact(&key, 3).await.is_err());
    assert!(store.get_fresh_exact(&key, 5).await.is_err());
    store.delete(&key).await.unwrap();
    assert!(matches!(store.get_fresh_exact(&key, 4).await, Err(Error::MissingObject(_))));
}

#[tokio::test]
async fn remote_namespaces_share_a_provider_without_aliasing() {
    let provider = Arc::new(object_store::memory::InMemory::new());
    let first = RemoteStore::new(provider.clone(), Some(ObjectKey::new("db/parts/0/v06").unwrap()));
    let second = RemoteStore::new(provider, Some(ObjectKey::new("db/parts/1/v06").unwrap()));
    let key = ObjectKey::new("blocks/b00.binpb").unwrap();
    first.put(&key, Bytes::from_static(b"first")).await.unwrap();
    second.put(&key, Bytes::from_static(b"second")).await.unwrap();
    assert_eq!(first.get(&key).await.unwrap().as_ref(), b"first");
    assert_eq!(second.get(&key).await.unwrap().as_ref(), b"second");
    assert_eq!(first.get_range(&key, 1..4).await.unwrap().as_ref(), b"irs");
    assert_eq!(first.list(None).await.unwrap().len(), 1);
    first.put(&key, Bytes::from_static(b"first")).await.unwrap();
    assert!(matches!(first.put(&key, Bytes::from_static(b"changed")).await, Err(Error::ConflictingObject(_))));
    first.delete(&key).await.unwrap();
    first.delete(&key).await.unwrap();
    assert_eq!(second.get(&key).await.unwrap().as_ref(), b"second");
}

#[tokio::test]
async fn remote_collection_advances_the_floor_without_waiting_for_any_reader() {
    use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, StorageVersion};

    let provider = Arc::new(object_store::memory::InMemory::new());
    let root = ObjectKey::new("db/v06").unwrap();
    let reader = RemoteStore::new(provider.clone(), Some(root.clone())).with_catalog_ledger().unwrap();
    let collector = RemoteStore::new(provider, Some(root)).with_catalog_ledger().unwrap();
    let older = CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0),
        replica_epoch: StorageEpoch(0), through: ReplicaPosition(1) };
    let newer = CatalogCut { through: ReplicaPosition(2), ..older };
    assert!(reader.admit_catalog(Some(older), None).await.unwrap().is_none(), "an object store grants no per-read custody");
    assert!(collector.advance_catalog_floor(newer).await.unwrap(), "a reader of an older catalog never holds the floor back");
    assert!(reader.admit_catalog(Some(older), None).await.unwrap().is_none());
    assert!(collector.list(None).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_encoded_namespace_lists_its_own_keys() {
    let provider = Arc::new(object_store::memory::InMemory::new());
    let inside = RemoteStore::new(provider.clone(), Some(ObjectKey::new("cell 1/~org/%data").unwrap()));
    let neighbor = RemoteStore::new(provider, Some(ObjectKey::new("cell 1/~org/%data-2").unwrap()));
    let key = ObjectKey::new("tries/~packs/block #1").unwrap();
    inside.put(&key, Bytes::from_static(b"inside")).await.unwrap();
    neighbor.put(&key, Bytes::from_static(b"neighbor")).await.unwrap();

    let root = inside.list(None).await.unwrap();
    let subtree = inside.list(Some(&ObjectKey::new("tries").unwrap())).await.unwrap();
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].key, key);
    assert_eq!(subtree, root);
    assert_eq!(inside.get(&root[0].key).await.unwrap(), Bytes::from_static(b"inside"));
    inside.delete(&root[0].key).await.unwrap();
    assert!(inside.list(None).await.unwrap().is_empty());
    assert_eq!(neighbor.get(&key).await.unwrap(), Bytes::from_static(b"neighbor"));
}

#[tokio::test]
async fn a_provider_object_at_the_namespace_root_is_not_a_relative_object() {
    let provider = Arc::new(object_store::memory::InMemory::new());
    let root = RemoteStore::new(provider.clone(), None);
    root.put(&ObjectKey::new("partition").unwrap(), Bytes::new()).await.unwrap();
    root.put(&ObjectKey::new("partition-two/object").unwrap(), Bytes::from_static(b"other")).await.unwrap();
    let namespace = RemoteStore::new(provider, Some(ObjectKey::new("partition").unwrap()));
    namespace.put(&ObjectKey::new("inside").unwrap(), Bytes::from_static(b"present")).await.unwrap();
    let listed = namespace.list(None).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key.as_str(), "inside");
}

#[tokio::test]
async fn multipart_file_publication_uses_an_immutable_destination_and_hides_staging() {
    use object_store::ObjectStoreExt as _;
    let provider = Arc::new(object_store::memory::InMemory::new());
    let store = RemoteStore::new(provider.clone(), Some(ObjectKey::new("db").unwrap()));
    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("data.arrow");
    let key = ObjectKey::new("tables/a/data/trie.arrow").unwrap();
    let mut data = vec![17u8; 5 * 1024 * 1024 + 31];
    data[5 * 1024 * 1024] = 29;
    std::fs::write(&path, &data).unwrap();
    store.put_file(&key, &path).await.unwrap();
    store.put_file(&key, &path).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap().as_ref(), data.as_slice());
    data[5 * 1024 * 1024] = 30;
    std::fs::write(&path, &data).unwrap();
    assert!(matches!(store.put_file(&key, &path).await, Err(Error::ConflictingObject(_))));
    let orphan = object_store::path::Path::from("db/.tmp/interrupted-upload");
    provider.put(&orphan, Bytes::from_static(b"staging").into()).await.unwrap();
    assert_eq!(store.list(None).await.unwrap().len(), 1);
    assert!(provider.head(&orphan).await.is_ok());
    let empty = ObjectKey::new("empty").unwrap();
    std::fs::write(&path, []).unwrap();
    store.put_file(&empty, &path).await.unwrap();
    assert_eq!(store.head(&empty).await.unwrap().size, 0);
}

#[tokio::test]
async fn remote_copy_preserves_immutable_bytes_across_explicit_prefixes() {
    let provider = Arc::new(object_store::memory::InMemory::new());
    let source = RemoteStore::new(provider.clone(), Some(ObjectKey::new("source").unwrap()));
    let destination = RemoteStore::new(provider, Some(ObjectKey::new("outside").unwrap()));
    let key = ObjectKey::new("original.arrow").unwrap();
    let target = ObjectKey::new("subdir/copy.arrow").unwrap();
    let mut data = vec![7u8; 2 * 1024 * 1024 + 17];
    source.put(&key, Bytes::copy_from_slice(&data)).await.unwrap();
    source.copy(&key, &key).await.unwrap();
    source.copy_to(&key, &destination, &target).await.unwrap();
    source.copy_to(&key, &destination, &target).await.unwrap();
    assert_eq!(destination.get(&target).await.unwrap().as_ref(), data.as_slice());
    assert_eq!(source.list(None).await.unwrap().len(), 1);
    data[1024 * 1024] = 8;
    let different = ObjectKey::new("different.arrow").unwrap();
    source.put(&different, Bytes::from(data)).await.unwrap();
    assert!(matches!(source.copy_to(&different, &destination, &target).await, Err(Error::ConflictingObject(_))));
    assert_eq!(destination.get_range(&target, 1024 * 1024..1024 * 1024 + 1).await.unwrap().as_ref(), &[7]);
    assert!(matches!(source.copy_to(&ObjectKey::new("missing").unwrap(), &destination, &target).await, Err(Error::MissingObject(_))));
    let empty = ObjectKey::new("empty").unwrap();
    source.put(&empty, Bytes::new()).await.unwrap();
    source.copy_to(&empty, &destination, &empty).await.unwrap();
    source.copy_to(&empty, &destination, &empty).await.unwrap();
    assert_eq!(destination.head(&empty).await.unwrap().size, 0);
    let other = RemoteStore::new(Arc::new(object_store::memory::InMemory::new()), None);
    assert!(matches!(source.copy_to(&key, &other, &target).await, Err(Error::Remote { .. })));
    assert!(other.list(None).await.unwrap().is_empty());
}
