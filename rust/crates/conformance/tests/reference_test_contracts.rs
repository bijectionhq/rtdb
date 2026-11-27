use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{
    block::proto as block_pb,
    catalog::BlockCatalog,
    envelope::{ReplicaBody, ReplicaEnvelope},
    keys::{CURRENT_STORAGE_VERSION, TrieKey, block_key},
    log::proto::TriesDeleted,
};
use rtdb_index::{PartitionState, PartitionValue, TableCatalog, TrieCatalog};
use rtdb_maintenance::{BlockCollector, CollectionObserver, CycleWorker, GarbageConfig};
use rtdb_replication::PartitionLogs;
use rtdb_storage::{
    Error, LocalStore, MemoryStore, ObjectKey, ObjectStore,
    cache::BufferPool, disk_cache::DiskCache, log::MemoryLog,
};
use rtdb_types::{Anomaly, BlockIndex, StorageEpoch, TableRef, Term, TermSequence, Timestamp};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

fn record(case: &str, observation: Value) {
    println!("\nVE-11 {}", json!({
        "case": case,
        "revision": env!("RTDB_CONFORMANCE_REVISION"),
        "source_dirty": env!("RTDB_CONFORMANCE_DIRTY") != "false",
        "observation": observation,
    }));
}

struct FailOnCollectionError;

impl CollectionObserver for FailOnCollectionError {
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) {
        panic!("block collection failed for {table:?}: {error}");
    }
}

fn classic_logs(store: Arc<LocalStore>, source: Arc<MemoryLog>, replica: Arc<MemoryLog>) -> Arc<PartitionLogs> {
    let memory = MemoryBudget::new(4 << 20);
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
    PartitionLogs::new(source, replica, StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION,
        StorageEpoch(0), store, cache, memory).unwrap()
}

async fn listed_keys(store: &LocalStore, prefix: &str) -> Vec<ObjectKey> {
    let mut keys: Vec<_> = store.list(Some(&ObjectKey::new(prefix).unwrap())).await.unwrap()
        .into_iter().map(|object| object.key).collect();
    keys.sort();
    keys
}

#[tokio::test]
async fn classic_block_collection_preserves_published_replica_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalStore::open(directory.path()).unwrap());
    let source = Arc::new(MemoryLog::new(256));
    let replica = Arc::new(MemoryLog::new(256));
    let writer = classic_logs(store.clone(), source.clone(), replica.clone());
    let blocks: Vec<_> = (1..=10).map(|index| ObjectKey::new(block_key(BlockIndex(index)).unwrap()).unwrap()).collect();
    for (index, key) in (1..=10).zip(&blocks) {
        let block = BlockCatalog::from_proto(block_pb::Block {
            block_index: Some(index),
            latest_completed_tx: Some(block_pb::TxKey { tx_id: Some(index), system_time: Some(index), ..Default::default() }),
            ..Default::default()
        }).unwrap();
        store.put(key, Bytes::from(block.encode())).await.unwrap();
    }
    assert_eq!(listed_keys(&store, "blocks").await, blocks);

    let mut published = BTreeMap::new();
    for index in [6, 9] {
        let envelope = ReplicaEnvelope {
            term: Term(1),
            term_sequence: Some(TermSequence(index)),
            message: ReplicaBody::TriesDeleted(TriesDeleted {
                table_name: Some("public/foo".into()),
                trie_keys: (0..100).map(|value| TrieKey::l0(BlockIndex(value)).unwrap().to_string()).collect(),
            }),
        };
        let receipt = writer.enqueue_replica(&envelope, BlockIndex(index), Timestamp(index)).await.unwrap();
        published.insert(receipt.durable().await.unwrap().position, (index, envelope));
    }

    let mut tail = writer.replica_tail(writer.replica_floor().unwrap()).await.unwrap();
    let records = tail.poll(Duration::from_secs(1)).await.unwrap();
    assert_eq!(records.len(), published.len());
    let expected_keys: BTreeMap<_, _> = records.iter().map(|raw| {
        let pointer = writer.decode_replica(raw.clone()).unwrap().unwrap();
        let (index, _) = &published[&pointer.metadata.position];
        assert!(matches!(&pointer.envelope.message, ReplicaBody::OversizedMessage(reference)
            if reference.block_index == Some(*index) && reference.payload_sha256.is_some()));
        let key = pointer.envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap();
        (*index, ObjectKey::new(key).unwrap())
    }).collect();
    drop((tail, writer));
    let mut both_keys = expected_keys.values().cloned().collect::<Vec<_>>();
    both_keys.sort();
    assert_eq!(listed_keys(&store, "oversized").await, both_keys);

    let latest = BlockCatalog::decode(&store.get(blocks.last().unwrap()).await.unwrap()).unwrap();
    let tables = TableCatalog::load(Some(latest), &BTreeMap::new()).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(
        TrieCatalog::default(), tables, Default::default(),
    ).unwrap()));
    let collector = BlockCollector::new(store.clone(), state, Arc::new(FailOnCollectionError),
        GarbageConfig { blocks_to_keep: 3, ..Default::default() }).unwrap();
    collector.collect(&CancellationToken::new()).await.unwrap();
    drop(collector);
    store.close_and_wait().await;
    drop(store);

    let reopened = Arc::new(LocalStore::open(directory.path()).unwrap());
    let reader = classic_logs(reopened.clone(), source, replica);
    let remaining_blocks = listed_keys(&reopened, "blocks").await;
    let remaining_payloads = listed_keys(&reopened, "oversized").await;
    let mut tail = reader.replica_tail(reader.replica_floor().unwrap()).await.unwrap();
    let mut replay = BTreeMap::new();
    for raw in tail.poll(Duration::from_secs(1)).await.unwrap() {
        let pointer = reader.decode_replica(raw).unwrap().unwrap();
        let (index, original) = &published[&pointer.metadata.position];
        let outcome = match reader.resolve_oversized(pointer).await {
            Ok(resolved) if resolved.envelope.as_ref() == original => "exact".to_owned(),
            Ok(_) => "decoded a different envelope".to_owned(),
            Err(error) => error.to_string(),
        };
        replay.insert(*index, outcome);
    }
    record("offloaded-retention", json!({
        "published_payloads": expected_keys.iter().map(|(index, key)| (index, key.as_str())).collect::<BTreeMap<_, _>>(),
        "remaining_payloads": remaining_payloads.iter().map(ObjectKey::as_str).collect::<Vec<_>>(),
        "remaining_blocks": remaining_blocks.iter().map(ObjectKey::as_str).collect::<Vec<_>>(),
        "replay": replay,
    }));
    drop((tail, reader));
    reopened.close_and_wait().await;

    assert_eq!(remaining_blocks, blocks[7..]);
    assert_eq!(remaining_payloads, both_keys);
    assert_eq!(replay.get(&6).map(String::as_str), Some("exact"));
    assert_eq!(replay.get(&9).map(String::as_str), Some("exact"));
}

async fn cache_inputs() -> (Arc<MemoryStore>, [ObjectKey; 3]) {
    let store = Arc::new(MemoryStore::default());
    let keys = ["a", "b", "c"].map(|name| ObjectKey::new(name).unwrap());
    for (key, body) in keys.iter().zip([b"aaaa", b"bbbb", b"cccc"]) {
        store.put(key, Bytes::from_static(body)).await.unwrap();
    }
    (store, keys)
}

fn disk_keys<'a>(root: &Path, keys: &'a [ObjectKey]) -> Vec<&'a str> {
    keys.iter().map(ObjectKey::as_str)
        .filter(|key| root.join(key).try_exists().unwrap()).collect()
}

#[tokio::test]
async fn native_disk_cache_trims_released_files_and_uses_idle_recency() {
    let directory = tempfile::tempdir().unwrap();
    let (store, keys) = cache_inputs().await;
    let [a, b, c] = &keys;
    let cache = DiskCache::open(directory.path(), 10, MemoryBudget::new(4 << 20)).unwrap();
    let first = cache.get(a, store.as_ref(), a).await.unwrap();
    let second = cache.get(b, store.as_ref(), b).await.unwrap();
    let third = cache.get(c, store.as_ref(), c).await.unwrap();
    for (lease, body) in [(&first, b"aaaa"), (&second, b"bbbb"), (&third, b"cccc")] {
        assert_eq!(std::fs::read(lease.path()).unwrap().as_slice(), body);
    }
    let mut pinned = vec![cache.stats().pinned_bytes];
    let admitted = disk_keys(directory.path(), &keys);
    drop(first);
    pinned.push(cache.stats().pinned_bytes);
    let after_first_release = disk_keys(directory.path(), &keys);
    drop(second);
    pinned.push(cache.stats().pinned_bytes);
    drop(third);
    let final_stats = cache.stats();
    pinned.push(final_stats.pinned_bytes);
    let retained = disk_keys(directory.path(), &keys);
    let evicted: Vec<_> = admitted.iter().copied().filter(|key| !retained.contains(key)).collect();

    let misses = cache.traffic().misses;
    let hit = cache.get(b, store.as_ref(), b).await.unwrap();
    let hit_misses = cache.traffic().misses - misses;
    drop(hit);
    let reloaded = cache.get(a, store.as_ref(), a).await.unwrap();
    let reloaded_bytes = std::fs::read(reloaded.path()).unwrap();
    let reload_misses = cache.traffic().misses - misses - hit_misses;
    let after_reload = disk_keys(directory.path(), &keys);
    drop(reloaded);
    cache.close_and_wait().await;

    record("disk-cache", json!({
        "admitted": admitted, "pinned_bytes": pinned, "after_first_release": after_first_release,
        "retained": retained, "evicted": evicted, "idle_bytes": final_stats.evictable_bytes,
        "after_reload": after_reload, "hit_misses": hit_misses, "reload_misses": reload_misses,
    }));
    assert_eq!(admitted, ["a", "b", "c"]);
    assert_eq!(pinned, [12, 8, 4, 0]);
    assert_eq!(after_first_release, ["b", "c"]);
    assert_eq!(retained, ["b", "c"]);
    assert_eq!(evicted, ["a"]);
    assert_eq!(final_stats.evictable_bytes, 8);
    assert_eq!((hit_misses, reload_misses), (0, 1));
    assert_eq!(reloaded_bytes.as_slice(), b"aaaa");
    assert_eq!(after_reload, ["a", "b"]);
}

#[tokio::test]
async fn native_memory_cache_enforces_its_ceiling_and_evicts_the_oldest_idle_object() {
    let (store, keys) = cache_inputs().await;
    let [a, b, c] = &keys;
    let pool = BufferPool::new(store, 10);
    let first = pool.get(a).await.unwrap();
    let second = pool.get(b).await.unwrap();
    let denied = pool.get(c).await;
    let denial = match &denied {
        Err(Error::MemoryLimit { requested, limit, retained }) => json!({
            "kind": "memory_limit", "requested": requested, "limit": limit, "retained": retained,
        }),
        Err(error) => json!({"kind": "unexpected_error", "detail": error.to_string()}),
        Ok(_) => Value::Null,
    };
    let admitted_three_while_pinned = denied.is_ok();
    drop(denied);
    let residents = || keys.iter().filter(|key| pool.holds(key)).map(ObjectKey::as_str).collect::<Vec<_>>();
    let after_denial = residents();
    let retained_after_denial = pool.retained_bytes();

    let slice = first.slice(1..2).unwrap();
    drop(first);
    drop(second);
    let recovered = pool.get(c).await.unwrap();
    let while_slice_pinned = residents();
    assert_eq!(recovered.as_slice(), b"cccc");
    assert_eq!(slice.as_slice(), b"a");
    drop(recovered);
    drop(slice);
    assert_eq!(pool.get(a).await.unwrap().as_slice(), b"aaaa");
    assert_eq!(pool.get(b).await.unwrap().as_slice(), b"bbbb");
    let after_reload = residents();
    let final_bytes = pool.retained_bytes();

    record("memory-cache", json!({
        "admitted_three_while_pinned": admitted_three_while_pinned, "admission_error": denial,
        "after_denial": after_denial, "retained_after_denial": retained_after_denial,
        "while_slice_pinned": while_slice_pinned, "after_reload": after_reload, "final_bytes": final_bytes,
    }));
    assert_eq!(denial, json!({"kind": "memory_limit", "requested": 4, "limit": 10, "retained": 8}));
    assert_eq!(after_denial, ["a", "b"]);
    assert_eq!(retained_after_denial, 8);
    assert_eq!(while_slice_pinned, ["a", "c"]);
    assert_eq!(after_reload, ["a", "b"]);
    assert_eq!(final_bytes, 8);
}
