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
