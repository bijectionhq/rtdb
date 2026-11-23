use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{
    block::proto as block,
    catalog::{BlockCatalog, TableBlock, TableEntry},
    envelope::{ReplicaBody, ReplicaEnvelope},
    keys::{CURRENT_STORAGE_VERSION, TableSlug, TrieKey, block_key, oversized_key},
    log::proto,
};
use rtdb_replication::{ReplayBase, ReplayDependencySources, checkpoint_dependencies, tail_dependencies};
use rtdb_storage::ObjectKey;
use rtdb_types::{BlockIndex, Result, StorageEpoch, TableRef, Term, TermSequence};

struct Sources {
    payload_key: ObjectKey,
    payload: Bytes,
    catalog: BlockCatalog,
    tables: BTreeMap<TableRef, TableBlock>,
    catalog_reads: AtomicUsize,
}

#[async_trait]
impl ReplayDependencySources for Sources {
    async fn oversized_payload(&self, key: &ObjectKey) -> Result<Bytes> {
        assert_eq!(*key, self.payload_key);
        Ok(self.payload.clone())
    }

    async fn adopted_checkpoint(&self, block: BlockIndex) -> Result<(BlockCatalog, BTreeMap<TableRef, TableBlock>)> {
        assert_eq!(block, self.catalog.block_index());
        self.catalog_reads.fetch_add(1, Ordering::Relaxed);
        Ok((self.catalog.clone(), self.tables.clone()))
    }
}

#[tokio::test]
async fn checkpointless_suffix_keeps_the_offloaded_body_and_adopts_block_zero_once() {
    let storage_version = CURRENT_STORAGE_VERSION;
    let storage_epoch = StorageEpoch(0);
    let block = BlockIndex(0);
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::recorded("public$docs~3".to_owned()).unwrap();
    let entry = TableEntry { oid: 16_384, table: table.clone(), slug: slug.clone() };
    let mut catalog = BlockCatalog::from_proto(block::Block {
        block_index: Some(block.0),
        tables: vec![entry.to_proto()],
        ..Default::default()
    }).unwrap();
    catalog.encode_for_publication();
    let search_run = proto::SearchRun {
        definition_id: Some("examplef16".into()),
        definition_version: Some(3),
        blocks: vec![0],
        digest: Some(vec![5; 32]),
        file_size: Some(100),
        locator: Some(vec![1]),
        source_rows: Some(1),
        ..Default::default()
    };
    let metadata = TableBlock::from_parts(Default::default(), &Default::default(), 0, Vec::new()).unwrap()
        .with_search_runs(vec![search_run.clone()], Vec::new());
    let expected_checkpoint = BTreeSet::from([
        ObjectKey::new(block_key(block).unwrap()).unwrap(),
        ObjectKey::new(slug.block_key(block).unwrap()).unwrap(),
        ObjectKey::new(search_run.object_key(&slug).unwrap()).unwrap(),
    ]);

    let upload = ReplicaBody::BlockUploaded(proto::BlockUploaded {
        block_index: Some(block.0),
        storage_version: Some(storage_version.0),
        storage_epoch: Some(storage_epoch.0),
        catalog_sha256: Some(catalog.publication_sha256().unwrap().to_vec()),
        ..Default::default()
    });
    let trie = TrieKey::l0(BlockIndex(1)).unwrap();
    let artefact = proto::ArtefactRecord {
        kind: Some("search".into()),
        definition_id: Some("body".into()),
        definition_version: Some(1),
        ..Default::default()
    };
    let additions = ReplicaBody::TriesAdded(proto::TriesAdded {
        storage_version: Some(storage_version.0),
        storage_epoch: Some(storage_epoch.0),
        tries: vec![proto::TrieDetails {
            table_name: Some(table.schema_and_table()),
            trie_key: Some(trie.to_string()),
            trie_state: Some(proto::TrieState::Live as i32),
            artefacts: vec![artefact.clone()],
            ..Default::default()
        }],
        ..Default::default()
    });
    let envelope = |sequence, message| ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence)), message };
    let added = envelope(0, additions);
    let payload = added.encode();
    let payload_key = ObjectKey::new(oversized_key(block, "added").unwrap()).unwrap();
    let proxy = added.offloaded(storage_version, storage_epoch, block, "added".into(), &payload);
    let sources = Sources {
        payload_key: payload_key.clone(),
        payload: payload.into(),
        catalog,
        tables: BTreeMap::from([(table, metadata)]),
        catalog_reads: AtomicUsize::new(0),
    };

    assert_eq!(checkpoint_dependencies(&sources.catalog, &sources.tables).unwrap(), expected_checkpoint);
    let expected_tail = expected_checkpoint.into_iter().chain([
        payload_key,
        ObjectKey::new(slug.data_key(&trie)).unwrap(),
        ObjectKey::new(slug.metadata_key(&trie)).unwrap(),
        ObjectKey::new(slug.recorded_artefact_key(&trie, &artefact).unwrap()).unwrap(),
    ]).collect();
    let base = ReplayBase { storage_version, storage_epoch, checkpoint: None };
    let suffix = [proxy, envelope(1, upload.clone()), envelope(2, upload)];
    assert_eq!(tail_dependencies(base, suffix, &sources).await.unwrap(), expected_tail);
    assert_eq!(sources.catalog_reads.load(Ordering::Relaxed), 1);
}
