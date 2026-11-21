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
