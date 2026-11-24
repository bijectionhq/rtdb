use rtdb_columnar::MemoryBudget;
use rtdb_format::{block::proto, catalog::{BlockCatalog, TableBlock, TableEntry}};
use rtdb_types::TableRef;

#[test]
fn catalog_clones_share_the_graph_and_its_allocation_charge() {
    let original = BlockCatalog::from_proto(proto::Block { block_index: Some(0), ..Default::default() }).unwrap();
    let memory = MemoryBudget::new(4096);
    let catalog = BlockCatalog::decode_reserved(&original.encode(), memory.reserve(1024).unwrap()).unwrap();
    assert_eq!(catalog, original);
    let retained = catalog.clone();
    assert!(std::ptr::eq(catalog.as_proto(), retained.as_proto()));
    drop(catalog);
    assert_eq!(memory.used_bytes(), 1024);
    let derived_owner = retained.retain_charge().unwrap();
    drop(retained);
    assert_eq!(memory.used_bytes(), 1024);
    drop(derived_owner);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn table_block_clones_keep_the_decoded_schema_charged() {
    let memory = MemoryBudget::new(4096);
    let original = TableBlock::from_parts(Default::default(), &Default::default(), 0, Vec::new()).unwrap();
    let block = TableBlock::decode_reserved(&original.encode(), memory.reserve(1024).unwrap()).unwrap();
    let retained = block.clone();
    assert_eq!(retained, original);
    assert!(std::ptr::eq(block.as_proto(), retained.as_proto()));
    drop(block);
    assert_eq!(memory.used_bytes(), 1024);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn replay_checkpoint_lists_active_search_roots_alongside_primary_closure() {
    use rtdb_format::{catalog::CheckpointContents, log::proto::{RetiredSearchRun, SearchRun}};

    let entry = TableEntry::new(16_384, TableRef::new("public", "example")).unwrap();
    let slug = &entry.slug;
    let run = |id| SearchRun { definition_id: Some("examplef16".into()), definition_version: Some(3), blocks: vec![0],
        digest: Some(vec![id; 32]), file_size: Some(100), locator: Some(vec![1]), source_rows: Some(1), ..Default::default() };
    let active = run(5);
    let retired = RetiredSearchRun { run: Some(run(6)), superseded_at: Some(8), ..Default::default() };
    let block = TableBlock::from_parts(Default::default(), &Default::default(), 0, Vec::new()).unwrap()
        .with_search_runs(vec![active.clone()], vec![retired.clone()]);
    let block = TableBlock::decode(&block.encode()).unwrap();
    assert_eq!(block.as_proto().search_runs, [active.clone()]);
    assert_eq!(block.as_proto().retired_search_runs, [retired]);
    let replay = block.checkpoint_trie_files(&entry, CheckpointContents::Replay).unwrap();
    assert_eq!(replay.into_iter().collect::<Vec<_>>(), [active.object_key(slug).unwrap()]);
    assert!(block.checkpoint_trie_files(&entry, CheckpointContents::Primary).unwrap().is_empty());
}

#[test]
fn a_table_block_manifest_covers_exactly_the_recorded_oids() {
    let table = TableEntry::new(16_384, TableRef::new("public", "example")).unwrap();
    let legacy = proto::Block { tables: vec![table.to_proto()], ..Default::default() };
    assert_eq!(BlockCatalog::from_proto(legacy.clone()).unwrap().table_block_digest(table.oid), None);
    let root = |entries: Vec<(u32, Vec<u8>)>| proto::Block {
        table_block_manifest: Some(proto::TableBlockManifest { sha256_by_oid: entries.into_iter().collect() }),
        ..legacy.clone()
    };
    let verified = BlockCatalog::from_proto(root(vec![(table.oid, vec![7; 32])])).unwrap();
    assert_eq!(verified.table_block_digest(table.oid), Some([7; 32].as_slice()));
    for malformed in [
        vec![],
        vec![(table.oid + 1, vec![7; 32])],
        vec![(table.oid, vec![7; 31])],
        vec![(table.oid, vec![7; 32]), (table.oid + 1, vec![9; 32])],
    ] {
        assert!(BlockCatalog::from_proto(root(malformed)).is_err(), "the manifest must cover exactly the table OIDs with SHA-256 digests");
    }
}

#[test]
fn replay_closure_preserves_l0_inputs_and_usable_artefacts() {
    use std::collections::BTreeSet;
    use prost::Message;
    use rtdb_format::{catalog::CheckpointContents, keys::TrieKey, log::proto::{ArtefactRecord, ArtefactScope, TrieDetails, TrieState}};
    use rtdb_types::BlockIndex;

    let entry = TableEntry::new(16_384, TableRef::new("public", "example")).unwrap();
    let slug = &entry.slug;
    let l0 = TrieKey::l0(BlockIndex(1)).unwrap();
    let retired = TrieKey::new(1, None, Vec::new(), BlockIndex(2)).unwrap();
    let live = TrieKey::new(2, None, vec![0], BlockIndex(3)).unwrap();
    let nascent = TrieKey::new(2, None, vec![1], BlockIndex(3)).unwrap();
    let group = ArtefactRecord { kind: Some("search".into()), definition_id: Some("group".into()), definition_version: Some(4), scope: Some(ArtefactScope::PartGroup as i32), ..Default::default() };
    let own = ArtefactRecord { definition_id: Some("own".into()), scope: None, ..group.clone() };
    let detail = |trie: &TrieKey, state: TrieState, artefacts| TrieDetails { trie_key: Some(trie.to_string()), trie_state: Some(state as i32), artefacts, ..Default::default() };
    let block = TableBlock::decode(&proto::TableBlock {
        tries: vec![detail(&retired, TrieState::Live, vec![own.clone()])],
        partitions: vec![proto::Partition { tries: vec![
            detail(&l0, TrieState::Garbage, vec![own.clone()]),
            detail(&retired, TrieState::Garbage, vec![own.clone()]),
            detail(&live, TrieState::Live, vec![group.clone(), own.clone()]),
            detail(&nascent, TrieState::Nascent, vec![group.clone()]),
        ], ..Default::default() }],
        ..Default::default()
    }.encode_to_vec()).unwrap();
    let primary = [slug.data_key(&live), slug.metadata_key(&live), slug.data_key(&nascent), slug.metadata_key(&nascent)];
    assert_eq!(block.checkpoint_trie_files(&entry, CheckpointContents::Primary).unwrap(), BTreeSet::from(primary.clone()));
    let mut replay = BTreeSet::from(primary);
    replay.extend([slug.data_key(&l0), slug.metadata_key(&l0), slug.recorded_artefact_key(&live, &own).unwrap(),
        slug.recorded_artefact_key(&live, &group).unwrap()]);
    assert_eq!(block.checkpoint_trie_files(&entry, CheckpointContents::Replay).unwrap(), replay,
        "replay closure must include retained L0 and one copy per usable artefact");

    let receipt = TableEntry::new(16_385, TableRef::new("xt", "bounded_retry_receipts")).unwrap();
    let receipt_files = block.checkpoint_trie_files(&receipt, CheckpointContents::Replay).unwrap();
    assert!(!receipt_files.contains(&receipt.slug.data_key(&l0)));
    assert!(!receipt_files.contains(&receipt.slug.metadata_key(&l0)));
    assert!(receipt_files.contains(&receipt.slug.data_key(&live)));
    assert!(receipt_files.contains(&receipt.slug.data_key(&nascent)));
}
