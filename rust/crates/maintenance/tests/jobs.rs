use rtdb_format::keys::TrieKey;
use rtdb_index::{CatalogTrie, TrieCatalog, TrieState};
use rtdb_maintenance::{available_jobs, RecencyMode};
use rtdb_types::{TableRef, Timestamp};

fn catalog(keys: &[(&str, i64)]) -> TrieCatalog {
    let table = TableRef::new("public", "foo");
    let mut catalog = TrieCatalog::new(16).unwrap();
    for (key, size) in keys {
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: *size, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(100)).unwrap();
    }
    catalog
}

#[test]
fn l0_compaction_takes_the_oldest_file_and_the_partial_l1() {
    let jobs = available_jobs(&catalog(&[("l00-rc-b00", 10), ("l00-rc-b01", 5), ("l00-rc-b02", 5), ("l01-rc-b00", 10)])).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].inputs.iter().map(ToString::to_string).collect::<Vec<_>>(), ["l01-rc-b00", "l00-rc-b01"]);
    assert_eq!(jobs[0].output.to_string(), "l01-rc-b01");
    assert_eq!(jobs[0].recency_mode, RecencyMode::Partition);
}

#[test]
fn only_live_inputs_are_compacted_and_existing_nascent_outputs_are_skipped_4930() {
    let jobs = available_jobs(&catalog(&[("l01-rc-b00", 20), ("l01-rc-b01", 20), ("l01-rc-b02", 20), ("l01-rc-b03", 20), ("l02-rc-p0-b03", 20)])).unwrap();
    assert_eq!(jobs.iter().map(|job| job.output.to_string()).collect::<Vec<_>>(), ["l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03"]);
    assert!(jobs.iter().all(|job| job.inputs.len() == 4 && job.part.len() == 1));
}

#[test]
fn historical_leveling_fills_a_partial_or_waits_for_four_inputs() {
    let mut keys = vec![("l01-r20200101-b00", 2), ("l01-r20200101-b01", 2), ("l01-r20200101-b02", 2), ("l01-r20200101-b03", 2),
        ("l01-rc-b00", 0), ("l01-rc-b01", 0), ("l01-rc-b02", 0)];
    assert!(available_jobs(&catalog(&keys)).unwrap().is_empty());
    keys.push(("l01-rc-b03", 0));
    let jobs = available_jobs(&catalog(&keys)).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].output.to_string(), "l02-r20200101-b03");
    keys.push(("l02-r20200101-b01", 14));
    let jobs = available_jobs(&catalog(&keys)).unwrap();
    assert_eq!(jobs[0].output.to_string(), "l02-r20200101-b02");
    assert_eq!(jobs[0].inputs, ["l02-r20200101-b01".parse::<TrieKey>().unwrap(), "l01-r20200101-b02".parse().unwrap()]);
}

#[test]
fn outputs_are_named_in_the_catalogs_epoch_so_a_reset_never_reuses_a_name() {
    let before = catalog(&[("l00-rc-b00", 10), ("l00-rc-b01", 10)]);
    let (after, _) = before.reset_from_primary(before.clone(), rtdb_types::BlockIndex(2), Timestamp(101)).unwrap();
    let planned = |catalog: &TrieCatalog| available_jobs(catalog).unwrap().into_iter().map(|job| job.output).collect::<Vec<_>>();
    let (old, new) = (planned(&before), planned(&after));
    assert!(!old.is_empty());
    assert!(old.iter().all(|key| key.epoch() == 0) && new.iter().all(|key| key.epoch() == 1));
    assert!(new.iter().all(|key| !old.contains(key)), "{new:?} reuses a name planned before the reset");
    assert_eq!(new.iter().map(|key| key.clone().with_epoch(0)).collect::<Vec<_>>(), old, "only the epoch differs");
}
