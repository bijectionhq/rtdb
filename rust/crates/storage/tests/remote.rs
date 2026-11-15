#![cfg(feature = "remote")]

use bytes::Bytes;
use rtdb_storage::{Error, ObjectKey, ObjectStore, remote::RemoteStore};
use std::sync::Arc;

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
    use object_store::{ObjectStore as _, ObjectStoreExt as _};
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
