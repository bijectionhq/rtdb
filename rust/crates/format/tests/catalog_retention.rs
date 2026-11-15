use rtdb_columnar::MemoryBudget;
use rtdb_format::{block::proto, catalog::{BlockCatalog, TableBlock}};

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
    let original = TableBlock::from_parts(Default::default(), 0, Vec::new()).unwrap();
    let block = TableBlock::decode_reserved(&original.encode(), memory.reserve(1024).unwrap()).unwrap();
    let retained = block.clone();
    assert_eq!(retained, original);
    assert!(std::ptr::eq(block.as_proto(), retained.as_proto()));
    drop(block);
    assert_eq!(memory.used_bytes(), 1024);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}
