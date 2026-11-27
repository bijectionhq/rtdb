use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::{block::proto as pb, catalog::{BlockCatalog, TableBlock, TableEntry}, keys::TableSlug};
use rtdb_index::{CatalogTrie, Shard, TableCatalog, TableMeta, TrieCatalog, TrieState, merge_column_types};
use rtdb_types::{BlockIndex, LogicalType, ScalarType, TableRef, Timestamp};

fn table() -> TableRef { TableRef::new("public", "foo") }
fn notification(key: &str, size: i64) -> CatalogTrie { CatalogTrie::notification(key.parse().unwrap(), size, None).unwrap() }
fn apply(messages: &[(&str, i64)]) -> TrieCatalog {
    TrieCatalog::new(20).unwrap().with_tries(table(), messages.iter().map(|(key, size)| notification(key, *size)), Timestamp(100)).unwrap()
}
fn current(catalog: &TrieCatalog) -> BTreeSet<String> { catalog.current_tries(&table()).iter().map(|trie| trie.key.to_string()).collect() }
fn names(keys: &[&str]) -> BTreeSet<String> { keys.iter().map(|key| (*key).to_owned()).collect() }

#[test]
fn historical_l1_waits_for_current_file_and_covered_l0_stays_in_source_inventory() {
    let initial = apply(&[("l00-rc-b00", 10), ("l00-rc-b01", 10), ("l00-rc-b02", 10), ("l01-r20200101-b01", 5), ("l01-r20200102-b01", 5)]);
    assert_eq!(current(&initial), names(&["l00-rc-b00", "l00-rc-b01", "l00-rc-b02"]));
    let next = initial.with_tries(table(), [notification("l01-rc-b01", 15)], Timestamp(100)).unwrap();
    assert_eq!(current(&next), names(&["l00-rc-b02", "l01-r20200101-b01", "l01-r20200102-b01", "l01-rc-b01"]));
    let blocks: Vec<_> = next.l0_source(&table(), BlockIndex(-1), BlockIndex(2))
        .map(|trie| (trie.key.block_index(), trie.state)).collect();
    assert_eq!(blocks, [
        (BlockIndex(0), TrieState::Garbage { as_of: Timestamp(100) }),
        (BlockIndex(1), TrieState::Garbage { as_of: Timestamp(100) }),
        (BlockIndex(2), TrieState::Live),
    ]);
    assert!(next.garbage_tries(&table(), Timestamp::MAX).is_empty());
    assert_eq!(next.l0_max_block_index(&table()), BlockIndex(2));
}

#[test]
fn publishing_tries_shares_other_tables_and_preserves_the_prior_catalog() {
    let changed = table();
    let untouched = TableRef::new("public", "unchanged");
    let l0 = notification("l00-rc-b00", 10);
    let before = apply(&[("l00-rc-b00", 10)])
        .with_tries(untouched.clone(), [l0.clone()], Timestamp(100)).unwrap();
    let after = before.with_tries(changed.clone(), [notification("l01-rc-b00", 15)], Timestamp(101)).unwrap();

    assert!(std::ptr::eq(before.trie(&untouched, &l0.key).unwrap(), after.trie(&untouched, &l0.key).unwrap()));
    assert_eq!(before.trie(&changed, &l0.key).unwrap().state, TrieState::Live);
    assert_eq!(after.trie(&changed, &l0.key).unwrap().state, TrieState::Garbage { as_of: Timestamp(101) });
    assert_eq!(before.partitions(&untouched).unwrap(), after.partitions(&untouched).unwrap());
}

#[test]
fn a_retired_table_supersedes_every_trie_with_no_successor_and_keeps_its_retirement_across_checkpoints() {
    let initial = apply(&[("l00-rc-b00", 10), ("l00-rc-b01", 10), ("l01-rc-b00", 15), ("l02-rc-p0-b01", 10)]);
    assert!(!current(&initial).is_empty());
    let retired = initial.with_retired_tables([table()], Timestamp(200));
    assert!(current(&retired).is_empty());
    assert!(retired.live_and_nascent_tries(&table()).is_empty());
    assert_eq!(retired.retired_as_of(&table()), Some(Timestamp(200)));
    assert_eq!(retired.garbage_tries(&table(), Timestamp(200)).iter().map(|trie| trie.key.to_string()).collect::<BTreeSet<_>>(),
        names(&["l01-rc-b00", "l02-rc-p0-b01"]));
    assert!(retired.garbage_tries(&table(), Timestamp(199)).is_empty());

    let again = retired.with_retired_tables([table()], Timestamp(300));
    assert_eq!(again.retired_as_of(&table()), Some(Timestamp(200)));
    let arrived = again.with_tries(table(), [notification("l01-rc-b01", 15), notification("l00-rc-b02", 10)], Timestamp(400)).unwrap();
    assert!(current(&arrived).is_empty());
    assert!(arrived.iter_tries(&table()).all(|trie| matches!(trie.state, TrieState::Garbage { .. })));

    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, arrived.partitions(&table()).unwrap()).unwrap();
    let block = TableBlock::decode(&arrived.with_retirement(&table(), block).encode()).unwrap();
    let loaded = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(500)).unwrap();
    assert_eq!(loaded.retired_as_of(&table()), Some(Timestamp(200)));
    let late = loaded.with_tries(table(), [notification("l01-rc-b03", 15)], Timestamp(600)).unwrap();
    assert!(current(&late).is_empty());

    let unseen = TableRef::new("public", "unflushed");
    let early = TrieCatalog::new(20).unwrap().with_retired_tables([unseen.clone()], Timestamp(10));
    let flushed = early.with_tries(unseen.clone(), [notification("l00-rc-b00", 10)], Timestamp(20)).unwrap();
    assert!(flushed.current_tries(&unseen).is_empty());
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
    let initial = apply(&[
        ("l01-rc-b00", 2), ("l02-rc-p0-b00", 10), ("l02-rc-p1-b00", 10), ("l02-rc-p2-b00", 10),
    ]);
    assert_eq!(current(&initial), names(&["l01-rc-b00"]));
    let complete = initial.with_tries(table(), [notification("l02-rc-p3-b00", 10)], Timestamp(101)).unwrap();
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
fn persisted_partitions_keep_covered_l0_for_exact_replay_5395() {
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
    let source: Vec<_> = catalog.l0_source(&table(), BlockIndex(-1), BlockIndex(1))
        .map(|trie| (trie.key.block_index(), trie.state)).collect();
    assert_eq!(source, [
        (BlockIndex(0), TrieState::Garbage { as_of: Timestamp(101) }),
        (BlockIndex(1), TrieState::Garbage { as_of: Timestamp(100) }),
    ]);
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
fn reset_relocates_the_compacted_primary_from_a_checkpoint() {
    let before = apply(&[("l00-rc-b00", 10), ("l01-rc-b00", 10)]);
    let primary = apply(&[("l01-rc-b00", 10)]);
    let (reset, files_epoch) = before.reset_from_primary(primary, BlockIndex(1), Timestamp(101)).unwrap();
    assert_eq!(current(&reset), BTreeSet::from([notification("l01-rc-b00", 10).key.with_epoch(files_epoch).to_string()]));
    assert_eq!(reset.naming_epoch(), 1);
    assert_eq!(reset.l0_source(&table(), BlockIndex(-1), BlockIndex(0)).count(), 1);
}

#[test]
fn a_delayed_notice_removes_or_adopts_only_its_named_incarnation() {
    use rtdb_index::{PartitionState, PartitionValue};
    use rtdb_trie::TrieConfig;

    let l0 = notification("l00-rc-b00", 10);
    let old = notification("l01-rc-b00", 10);
    let current = |name: &str| {
        let mut trie = notification(name, 10);
        trie.key = trie.key.with_epoch(1);
        trie
    };
    let table = table();
    let checkpoint = BlockCatalog::from_proto(pb::Block { block_index: Some(0), naming_epoch: Some(1), ..Default::default() }).unwrap();
    let tables = TableCatalog::load(Some(checkpoint), &BTreeMap::new()).unwrap();
    let catalog = TrieCatalog::new(20).unwrap().with_tries(table.clone(), [l0], Timestamp(10)).unwrap();
    let state = PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap());
    let add = |tries| state.add_tries(&BTreeMap::from([(table.clone(), tries)]), Timestamp(10)).unwrap();
    add(vec![old.clone()]);
    assert!(state.value().trie_catalog().trie(&table, &old.key).is_none(), "a delayed publication from before reset must not be adopted");
    let first = current("l01-rc-b00");
    let next = current("l01-rc-b01");
    add(vec![first.clone(), next]);
    assert!(matches!(state.value().trie_catalog().trie(&table, &first.key).unwrap().state, TrieState::Garbage { .. }));
    state.delete_tries(&BTreeMap::from([(table.clone(), BTreeSet::from([old.key]))]));
    assert!(state.value().trie_catalog().trie(&table, &first.key).is_some(), "a delayed retirement must preserve the replacement");
    state.delete_tries(&BTreeMap::from([(table.clone(), BTreeSet::from([first.key.clone()]))]));
    assert!(state.value().trie_catalog().trie(&table, &first.key).is_none(), "retirement of the exact garbage key must succeed");
}

#[test]
fn a_follower_installs_the_naming_epoch_and_exact_catalog_of_a_reset_checkpoint() {
    use rtdb_index::{PartitionState, PartitionValue};
    use rtdb_trie::TrieConfig;

    let table = table();
    let entry = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap().entry(&table).unwrap().clone();
    let old = apply(&[("l00-rc-b00", 10), ("l01-rc-b00", 10)]);
    let table_block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, old.partitions(&table).unwrap()).unwrap();
    let checkpoint = BlockCatalog::from_proto(pb::Block {
        block_index: Some(0),
        tables: vec![entry.to_proto()],
        latest_completed_tx: Some(pb::TxKey { tx_id: Some(1), system_time: Some(10) }),
        ..Default::default()
    }).unwrap();
    let target = checkpoint.latest_completed_tx().unwrap();
    let tables = TableCatalog::load(Some(checkpoint), &BTreeMap::from([(table.clone(), table_block)])).unwrap();
    let leader = PartitionState::new(PartitionValue::loaded(old.clone(), tables.clone(), TrieConfig::default()).unwrap());
    let follower = PartitionState::new(PartitionValue::loaded(old.clone(), tables, TrieConfig::default()).unwrap());
    let l0 = notification("l00-rc-b00", 10);
    let (reset, _) = old.reset_from_primary(apply(&[("l00-rc-b00", 10)]), BlockIndex(1), Timestamp(10)).unwrap();
    leader.install_reset_primary(reset, target).unwrap();

    let leader_prepared = leader.prepare_block().unwrap();
    let no_new_tries = BTreeMap::new();
    let block = BlockCatalog::from_proto(pb::Block {
        block_index: Some(leader_prepared.block_index().0),
        tables: vec![entry.to_proto()],
        naming_epoch: Some(1),
        latest_completed_tx: leader.value().table_catalog().block().unwrap().as_proto().latest_completed_tx.clone(),
        ..Default::default()
    }).unwrap();
    let table_blocks = leader_prepared.table_blocks(&no_new_tries, Timestamp(101)).unwrap();
    follower.adopt_block(follower.prepare_block().unwrap(), block, &table_blocks, &no_new_tries, Timestamp(101)).unwrap();

    let installed = follower.value();
    assert_eq!(installed.trie_catalog().naming_epoch(), 1);
    assert_eq!(current(installed.trie_catalog()), BTreeSet::from([l0.key.to_string()]));
    let mut new = notification("l01-rc-b00", 10);
    new.key = new.key.with_epoch(1);
    follower.add_tries(&BTreeMap::from([(table.clone(), vec![new.clone()])]), Timestamp(102)).unwrap();
    assert_eq!(current(follower.value().trie_catalog()), BTreeSet::from([new.key.to_string()]));
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
fn unicode_suffix_replay_refuses_changed_oids_and_explicit_restart_preserves_them() {
    let bmp = TableRef::new("public", "tＡ");
    let supplementary = TableRef::new("public", "t𐐀");
    let existing = TableRef::new("public", "renamed");
    let anchor = TableEntry { table: existing.clone(), oid: 20_000, slug: TableSlug::recorded("historical-location").unwrap() };
    let base = BlockCatalog::from_proto(pb::Block { block_index: Some(0), tables: vec![anchor.to_proto()], ..Default::default() }).unwrap();
    let metadata = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, vec![]).unwrap();
    let predecessor = TableCatalog::load(Some(base), &BTreeMap::from([(existing.clone(), metadata.clone())])).unwrap();
    let replayed = predecessor.with_entries_for([supplementary.clone(), bmp.clone()]).unwrap();
    assert_eq!(replayed.entry(&bmp).unwrap().oid, 20_001);
    assert_eq!(replayed.entry(&supplementary).unwrap().oid, 20_002);

    let pinned_supplementary = TableEntry::new(20_001, supplementary.clone()).unwrap();
    let pinned_bmp = TableEntry::new(20_002, bmp.clone()).unwrap();
    let successor = BlockCatalog::from_proto(pb::Block {
        block_index: Some(1), tables: vec![anchor.to_proto(), pinned_supplementary.to_proto(), pinned_bmp.to_proto()], ..Default::default()
    }).unwrap();
    let delta = BTreeMap::from([(bmp.clone(), TableMeta::default()), (supplementary.clone(), TableMeta::default())]);
    let error = replayed.adopt(successor.clone(), &delta).unwrap_err();
    assert_eq!(error.message.as_deref(), Some("Block changes a previously allocated table OID or slug"));
    assert_eq!(replayed.entry(&existing), Some(&anchor));

    let restarted = TableCatalog::load(Some(BlockCatalog::decode(&successor.encode()).unwrap()), &BTreeMap::from([
        (existing.clone(), metadata.clone()), (bmp.clone(), metadata.clone()), (supplementary.clone(), metadata),
    ])).unwrap();
    let added = TableRef::new("public", "a");
    let later = restarted.with_entries_for([added.clone(), existing.clone(), bmp.clone(), supplementary.clone()]).unwrap();
    assert_eq!(later.entry(&existing), Some(&anchor));
    assert_eq!(later.entry(&bmp), Some(&pinned_bmp));
    assert_eq!(later.entry(&supplementary), Some(&pinned_supplementary));
    assert_eq!(later.entry(&added).unwrap().oid, 20_003);
}

#[test]
fn catalog_allocates_the_first_free_suffix_for_colliding_table_slugs() {
    let original = TableRef::new("public", "a$b");
    let suffix_owner = TableRef::new("public", "a$b~1");
    let dotted = TableRef::new("public", "a.b");
    let catalog = TableCatalog::default().with_entries_for([suffix_owner.clone(), original.clone()]).unwrap();
    let next = catalog.with_entries_for([dotted.clone(), dotted.clone()]).unwrap();
    assert_eq!(next.entry(&original), catalog.entry(&original));
    assert_eq!(next.entry(&suffix_owner), catalog.entry(&suffix_owner));
    assert_eq!(next.entry(&dotted).unwrap().slug.as_str(), "public$a$b~2");
    assert_eq!(next.entries().count(), 3);
}

fn artefact(definition_id: &str, version: i64, stats: u8) -> rtdb_format::log::proto::ArtefactRecord {
    rtdb_format::log::proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some(definition_id.into()), definition_version: Some(version), stats: Some(vec![stats]), scope: None, file_size: None, published_version: None, pack_members: Vec::new() }
}

#[test]
fn a_trie_keeps_the_first_artefact_per_definition_version_and_unknown_tries_take_none() {
    let catalog = apply(&[("l01-rc-b00", 30)]);
    let key = "l01-rc-b00".parse().unwrap();
    let recorded = catalog.with_artefacts(&table(), [
        (key, artefact("body", 1, 1)),
        ("l01-rc-b00".parse().unwrap(), artefact("body", 1, 2)),
        ("l01-rc-b00".parse().unwrap(), artefact("body", 2, 3)),
        ("l01-rc-b07".parse().unwrap(), artefact("body", 1, 4)),
    ]);
    let tries = recorded.all_tries(&table());
    let [trie] = tries.as_slice() else { panic!("one trie expected, got {tries:?}") };
    assert_eq!(trie.artefacts, vec![artefact("body", 1, 1), artefact("body", 2, 3)]);
    assert_eq!(trie.artefact("search", "body", 2), Some(&artefact("body", 2, 3)));
    assert!(trie.artefact("search", "title", 1).is_none());
    assert!(recorded.with_artefacts(&TableRef::new("public", "other"), [("l01-rc-b00".parse().unwrap(), artefact("body", 1, 1))]).all_tries(&TableRef::new("public", "other")).is_empty());
}

#[test]
fn removing_an_artefact_drops_its_record_whatever_its_stats_and_leaves_the_rest() {
    let key: rtdb_format::keys::TrieKey = "l01-rc-b00".parse().unwrap();
    let recorded = apply(&[("l01-rc-b00", 30)]).with_artefacts(&table(), [(key.clone(), artefact("body", 1, 1)), (key.clone(), artefact("body", 2, 2))]);
    let removed = recorded.without_artefacts(&table(), [(key.clone(), artefact("body", 1, 7)), (key.clone(), artefact("title", 1, 1)), ("l01-rc-b07".parse().unwrap(), artefact("body", 2, 2))]);
    assert_eq!(removed.all_tries(&table())[0].artefacts, vec![artefact("body", 2, 2)]);
    assert_eq!(removed.without_artefacts(&table(), [(key, artefact("body", 1, 1))]).all_tries(&table()), removed.all_tries(&table()), "removing an absent record changes nothing");
}

#[test]
fn artefact_records_survive_the_block_file_roundtrip() {
    let catalog = apply(&[("l01-rc-b00", 30), ("l01-rc-b01", 30)]).with_artefacts(&table(), [("l01-rc-b01".parse().unwrap(), artefact("body", 1, 9))]);
    let block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, catalog.partitions(&table()).unwrap()).unwrap();
    let loaded = TrieCatalog::load(&BTreeMap::from([(table(), block)]), 20, Timestamp(200)).unwrap();
    assert_eq!(catalog.all_tries(&table()), loaded.all_tries(&table()));
    assert!(loaded.all_tries(&table()).iter().any(|trie| trie.artefact("search", "body", 1).is_some()));
}

#[test]
fn a_partition_groups_members_are_looked_up_by_key_whatever_their_state() {
    let complete = apply(&[("l01-rc-b00", 2), ("l02-rc-p0-b00", 10), ("l02-rc-p1-b00", 10), ("l02-rc-p2-b00", 10), ("l02-rc-p3-b00", 10)]);
    let member: rtdb_format::keys::TrieKey = "l02-rc-p2-b00".parse().unwrap();
    let group = member.part_group().unwrap();
    let members = complete.group_members(&table(), &group);
    assert_eq!(members.iter().map(|trie| trie.key.to_string()).collect::<Vec<_>>(), ["l02-rc-p0-b00", "l02-rc-p1-b00", "l02-rc-p2-b00", "l02-rc-p3-b00"]);
    assert!(members.iter().all(|trie| trie.state == TrieState::Live));
    assert_eq!(complete.trie(&table(), &member).map(|trie| trie.data_file_size), Some(10));
    assert!(complete.trie(&table(), &"l02-rc-p2-b01".parse().unwrap()).is_none());
    let partial = apply(&[("l02-rc-p0-b00", 10), ("l02-rc-p3-b00", 10)]);
    assert_eq!(partial.group_members(&table(), &group).iter().map(|trie| (trie.key.to_string(), trie.state)).collect::<Vec<_>>(),
        [("l02-rc-p0-b00".to_string(), TrieState::Nascent), ("l02-rc-p3-b00".to_string(), TrieState::Nascent)]);
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

#[test]
fn checkpoint_replay_preserves_overlapping_group_publication() {
    for (parent, output) in [
        ("l01-rc-b00", "l02-rc"),
        ("l02-r20200101-b00", "l03-r20200101"),
    ] {
        let initial = apply(&[(parent, 20)]);
        for order in (0..256u16).filter(|order| order.count_ones() == 4) {
            let mut next_bucket = [0; 2];
            let notices: Vec<_> = (0..8).map(|position| {
                let block = usize::from((order >> position) & 1);
                let bucket = next_bucket[block];
                next_bucket[block] += 1;
                notification(&format!("{output}-p{bucket}-b0{block}"), 10)
            }).collect();

            let mut uninterrupted = vec![initial.clone()];
            for notice in &notices {
                let next = uninterrupted.last().unwrap()
                    .with_tries(table(), [notice.clone()], Timestamp(102)).unwrap();
                uninterrupted.push(next);
            }
            for (cut, expected) in uninterrupted.iter().enumerate() {
                let checkpoint = TableBlock::from_parts(
                    BTreeMap::new(), &Default::default(), 0, expected.partitions(&table()).unwrap(),
                ).unwrap();
                let decoded = TableBlock::decode(&checkpoint.encode()).unwrap();
                let mut resumed = TrieCatalog::load(
                    &BTreeMap::from([(table(), decoded)]), 20, Timestamp(101),
                ).unwrap();
                assert_eq!(resumed.partitions(&table()).unwrap(), expected.partitions(&table()).unwrap());
                for (index, notice) in notices.iter().enumerate().skip(cut) {
                    resumed = resumed.with_tries(table(), [notice.clone()], Timestamp(102)).unwrap();
                    assert_eq!(
                        resumed.partitions(&table()).unwrap(), uninterrupted[index + 1].partitions(&table()).unwrap(),
                        "{output}: order {order:08b}, checkpoint {cut}, notice {index}",
                    );
                }
            }
        }
    }
}
