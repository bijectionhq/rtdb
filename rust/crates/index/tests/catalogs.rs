use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::{block::proto as pb, catalog::{BlockCatalog, TableBlock, TableEntry}, keys::TableSlug};
use rtdb_index::{CatalogTrie, Shard, TableCatalog, TrieCatalog, TrieState, merge_column_types};
use rtdb_types::{BlockIndex, LogicalType, ScalarType, TableRef, Timestamp};

fn table() -> TableRef { TableRef::new("public", "foo") }
fn notification(key: &str, size: i64) -> CatalogTrie { CatalogTrie::notification(key.parse().unwrap(), size, None).unwrap() }
fn apply(messages: &[(&str, i64)]) -> TrieCatalog {
    TrieCatalog::new(20).unwrap().with_tries(table(), messages.iter().map(|(key, size)| notification(key, *size)), Timestamp(100)).unwrap()
}
fn current(catalog: &TrieCatalog) -> BTreeSet<String> { catalog.current_tries(&table()).iter().map(|trie| trie.key.to_string()).collect() }
fn names(keys: &[&str]) -> BTreeSet<String> { keys.iter().map(|key| (*key).to_owned()).collect() }

#[test]
fn historical_l1_waits_for_current_file_and_covered_l0_leaves_catalog() {
    let initial = apply(&[("l00-rc-b00", 10), ("l00-rc-b01", 10), ("l00-rc-b02", 10), ("l01-r20200101-b01", 5), ("l01-r20200102-b01", 5)]);
    assert_eq!(current(&initial), names(&["l00-rc-b00", "l00-rc-b01", "l00-rc-b02"]));
    let next = initial.with_tries(table(), [notification("l01-rc-b01", 15)], Timestamp(100)).unwrap();
    assert_eq!(current(&next), names(&["l00-rc-b02", "l01-r20200101-b01", "l01-r20200102-b01", "l01-rc-b01"]));
    assert!(next.all_tries(&table()).iter().all(|trie| trie.key.to_string() != "l00-rc-b00"));
    assert!(next.garbage_tries(&table(), Timestamp::MAX).is_empty());
    assert_eq!(next.l0_max_block_index(&table()), BlockIndex(2));
}

#[test]
fn levelled_current_files_supersede_only_partial_files() {
    let catalog = apply(&[("l01-rc-b00", 10), ("l01-r20200101-b01", 15), ("l01-rc-b01", 5), ("l01-rc-b02", 20), ("l01-rc-b03", 5)]);
    assert_eq!(current(&catalog), names(&["l01-r20200101-b01", "l01-rc-b02", "l01-rc-b03"]));
    assert_eq!(catalog.garbage_tries(&table(), Timestamp(100)).len(), 2);
    assert!(catalog.garbage_tries(&table(), Timestamp(99)).is_empty());
}

#[test]
fn four_iid_parts_promote_together_and_garbage_deletion_keeps_watermark_4946() {
    let initial = apply(&[("l01-rc-b00", 2), ("l02-rc-p0-b00", 10), ("l02-rc-p3-b00", 10)]);
    assert_eq!(current(&initial), names(&["l01-rc-b00"]));
    let complete = initial.with_tries(table(), [notification("l02-rc-p1-b00", 10), notification("l02-rc-p2-b00", 10)], Timestamp(101)).unwrap();
    assert_eq!(current(&complete), names(&["l02-rc-p0-b00", "l02-rc-p1-b00", "l02-rc-p2-b00", "l02-rc-p3-b00"]));
    let garbage = complete.garbage_tries(&table(), Timestamp(101)).iter().map(|trie| trie.key.clone()).collect();
    let deleted = complete.without_tries(&table(), &garbage);
    let replayed = deleted.with_tries(table(), [notification("l01-rc-b00", 50)], Timestamp(102)).unwrap();
    assert_eq!(current(&deleted), current(&replayed));
    assert_eq!(replayed.max_block_index(&table(), &Shard { level: 1, recency: None, part: vec![] }), BlockIndex(0));
}

#[test]
fn historical_l2_supersession_is_scoped_by_recency() {
    let catalog = apply(&[("l01-r20200101-b01", 5), ("l01-r20200102-b01", 5), ("l01-rc-b01", 15), ("l01-r20200102-b00", 5), ("l02-r20200101-b01", 5), ("l02-r20200102-b00", 5)]);
    assert_eq!(current(&catalog), names(&["l02-r20200102-b00", "l02-r20200101-b01", "l01-r20200102-b01", "l01-rc-b01"]));
}

#[test]
fn l1_current_before_historical_and_replayed_l0_are_idempotent_4301_4545() {
    let catalog = apply(&[("l00-rc-b00", 10), ("l01-rc-b00", 10), ("l01-r20200101-b00", 10), ("l00-rc-b00", 0)]);
    assert_eq!(current(&catalog), names(&["l01-rc-b00", "l01-r20200101-b00"]));
}

#[test]
fn a_checkpoint_preserves_pending_historical_promotion_for_each_block() {
    let initial = apply(&[
        ("l00-rc-b00", 10),
        ("l00-rc-b01", 10),
        ("l01-r20200101-b00", 5),
        ("l01-r20200102-b00", 5),
        ("l01-r20200101-b01", 5),
    ]);
    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, initial.partitions(&table()).unwrap()).unwrap();
    let block = TableBlock::decode(&block.encode()).unwrap();
    let loaded = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(101)).unwrap();
    for mut catalog in [initial, loaded] {
        assert_eq!(current(&catalog), names(&["l00-rc-b00", "l00-rc-b01"]));
        catalog = catalog.with_tries(table(), [notification("l01-rc-b00", 20)], Timestamp(102)).unwrap();
        assert_eq!(current(&catalog), names(&[
            "l00-rc-b01", "l01-rc-b00", "l01-r20200101-b00", "l01-r20200102-b00",
        ]));
        catalog = catalog.with_tries(table(), [notification("l01-rc-b01", 20)], Timestamp(103)).unwrap();
        assert_eq!(current(&catalog), names(&[
            "l01-rc-b00", "l01-rc-b01", "l01-r20200101-b00", "l01-r20200101-b01", "l01-r20200102-b00",
        ]));
    }
}

#[test]
fn persisted_partitions_repair_covered_l0_and_drop_old_l0_garbage_5395() {
    let mut live_l0 = notification("l00-rc-b00", 10); live_l0.state = TrieState::Live;
    let mut garbage_l0 = notification("l00-rc-b01", 10); garbage_l0.state = TrieState::Garbage { as_of: Timestamp(100) };
    let mut l1 = notification("l01-rc-b00", 10); l1.state = TrieState::Live;
    let partitions = vec![
        pb::Partition { level: Some(0), part: Some(vec![]), tries: vec![live_l0.to_proto(&table()), garbage_l0.to_proto(&table())], max_block_index: Some(1), ..Default::default() },
        pb::Partition { level: Some(1), part: Some(vec![]), tries: vec![l1.to_proto(&table())], max_block_index: Some(0), ..Default::default() },
    ];
    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, partitions).unwrap();
    let catalog = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(101)).unwrap();
    assert_eq!(current(&catalog), names(&["l01-rc-b00"]));
    assert_eq!(catalog.all_tries(&table()).len(), 1);
    assert_eq!(catalog.l0_max_block_index(&table()), BlockIndex(1));
}

#[test]
fn partition_roundtrip_preserves_states_watermarks_and_omits_table_name() {
    let catalog = apply(&[("l00-rc-b00", 10), ("l01-rc-b00", 10), ("l01-rc-b01", 10), ("l02-rc-p0-b01", 10)]);
    let partitions = catalog.partitions(&table()).unwrap();
    assert!(partitions.iter().flat_map(|partition| &partition.tries).all(|trie| trie.table_name.is_none()));
    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, partitions).unwrap();
    let loaded = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(200)).unwrap();
    assert_eq!(catalog.all_tries(&table()), loaded.all_tries(&table()));
    assert_eq!(loaded.l0_max_block_index(&table()), BlockIndex(0));
}

#[test]
fn wide_recency_partitions_roundtrip_and_reject_microsecond_overflow() {
    use rtdb_types::CivilDate;
    let catalog = apply(&[("l02-r2800000103-b00", 10)]);
    let mut partitions = catalog.partitions(&table()).unwrap();
    let midnight = CivilDate::from_ymd(280_000, 1, 3).unwrap().epoch_day() * 86_400_000_000;
    assert_eq!(partitions[0].recency, Some(midnight));
    partitions[0].recency = Some(midnight + 86_400_000_000 - 1);
    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, partitions).unwrap();
    let loaded = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(200)).unwrap();
    assert_eq!(catalog.all_tries(&table()), loaded.all_tries(&table()));
    assert_eq!(loaded.partitions(&table()).unwrap()[0].recency, Some(midnight));
    assert!(apply(&[("l02-r9999999991231-b00", 10)]).partitions(&table()).is_err());
}

#[test]
fn reset_rebuilds_l0_from_supplied_objects_and_clears_compaction() {
    let catalog = apply(&[("l01-rc-b01", 10), ("l02-rc-p0-b01", 10)]);
    let reset = catalog.reset_to_l0(BTreeMap::from([(table(), vec![notification("l00-rc-b00", 10), notification("l00-rc-b01", 10)])])).unwrap();
    assert_eq!(current(&reset), names(&["l00-rc-b00", "l00-rc-b01"]));
    assert_eq!(reset.all_tries(&table()).len(), 2);
}

#[test]
fn absent_columns_widen_only_when_the_other_side_has_put_data_5911() {
    let old = StructFields::from([("a".into(), LogicalType::Scalar(ScalarType::I64))]);
    let declared = StructFields::from([("b".into(), LogicalType::Nothing)]);
    let merged = merge_column_types(&old, &declared);
    assert_eq!(merged["a"], LogicalType::Scalar(ScalarType::I64));
    assert_eq!(merged["b"], LogicalType::Null);
    assert_eq!(merge_column_types(&old, &StructFields::new()), old);
    let rows = StructFields::from([("b".into(), LogicalType::Scalar(ScalarType::Utf8))]);
    assert_eq!(merge_column_types(&old, &rows)["a"], LogicalType::Scalar(ScalarType::I64).maybe());
}

#[test]
fn table_oid_allocation_is_sorted_and_recorded_slugs_stay_frozen() {
    let alpha = TableRef::new("public", "alpha");
    let zeta = TableRef::new("public", "zeta");
    let custom = TableEntry { oid: 20_000, table: alpha.clone(), slug: TableSlug::recorded("historical-location").unwrap() };
    let block = BlockCatalog::from_proto(pb::Block { block_index: Some(0), tables: vec![custom.to_proto()], ..Default::default() }).unwrap();
    let metadata = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, vec![]).unwrap();
    let catalog = TableCatalog::load(Some(block), &BTreeMap::from([(alpha.clone(), metadata)])).unwrap();
    let next = catalog.with_entries_for([zeta.clone(), table()]).unwrap();
    assert_eq!(next.entry(&alpha).unwrap().slug.as_str(), "historical-location");
    assert_eq!(next.entry(&table()).unwrap().oid, 20_001);
    assert_eq!(next.entry(&zeta).unwrap().oid, 20_002);
}

#[test]
fn a_newer_nascent_sibling_cannot_complete_an_older_group() {
    let catalog = apply(&[
        ("l01-rc-b00", 2),
        ("l02-rc-p2-b01", 10),
        ("l02-rc-p0-b00", 10),
        ("l02-rc-p1-b00", 10),
        ("l02-rc-p3-b00", 10),
        ("l02-rc-p2-b00", 10),
    ]);
    let keys = |catalog: &TrieCatalog| {
        catalog.current_tries(&table()).iter().map(|trie| trie.key.to_string()).collect::<Vec<_>>()
    };
    assert_eq!(keys(&catalog), ["l01-rc-b00"], "the parent covers every bucket until a complete group can replace it");

    let complete = catalog.with_tries(table(), [
        notification("l02-rc-p0-b01", 10),
        notification("l02-rc-p1-b01", 10),
        notification("l02-rc-p3-b01", 10),
    ], Timestamp(101)).unwrap();
    assert_eq!(keys(&complete), [
        "l02-rc-p0-b01", "l02-rc-p1-b01", "l02-rc-p2-b01", "l02-rc-p3-b01",
    ]);
}
