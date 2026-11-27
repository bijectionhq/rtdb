use rtdb_crypt::Generation;
use rtdb_format::journal::{Checkpoint, Manifest};

use super::*;
use crate::objects::key;

const DATABASE: Uuid = Uuid::from_u128(99);
const PIN: Uuid = Uuid::from_u128(100);

fn file(epoch: u64, seq: u64) -> ObjectId {
    ObjectId {
        database: DATABASE,
        kind: ObjectKind::File,
        epoch,
        seq,
        digest: [0x35; 32],
    }
}

fn manifest(epoch: u64, seq: u64, files: Vec<ObjectId>, pins: Vec<Pin>) -> VerifiedManifest {
    let id = ObjectId {
        kind: ObjectKind::Manifest,
        ..file(epoch, seq)
    };
    let manifest = Manifest {
        files,
        pins,
        payload: Vec::new(),
    };
    VerifiedManifest {
        id,
        manifest,
        generation: Generation {
            database: DATABASE,
            number: 8,
        },
    }
}

fn checkpoint(manifest: &VerifiedManifest, through: u64) -> Record {
    Record::Checkpoint(Checkpoint {
        manifest: manifest.id,
        through,
        key_generation: manifest.generation.number,
        restore: false,
        recovery_epoch: 0,
    })
}

#[test]
fn gc_c1_delayed_upload_and_delete_cannot_reach_a_successor_publication() {
    let zombie = file(1, 0);
    let unrelated = file(1, 1);
    let live = file(1, 2);
    let replacement = file(2, 0);
    let recovery = manifest(1, 3, vec![live], vec![]);
    let new_image = manifest(2, 1, vec![replacement, live], vec![]);
    let history = [checkpoint(&recovery, 5)];
    let at_seal = Reachability::from_records(DATABASE, 2, &history, [&recovery]).unwrap();
    let zombie_key = key(zombie).unwrap();
    let unrelated_key = key(unrelated).unwrap();
    let replacement_key = key(replacement).unwrap();
    let live_key = key(live).unwrap();

    for upload_delay in 0..8 {
        for scan_delay in 0..8 {
            for delete_delay in scan_delay..8 {
                let mut listed = vec![&unrelated_key, &replacement_key, &live_key];
                if upload_delay <= scan_delay {
                    listed.push(&zombie_key);
                }
                let deletion = at_seal.plan_gc(listed);
                assert!(deletion.keys().contains(&unrelated_key));
                assert_eq!(
                    deletion.keys().contains(&zombie_key),
                    upload_delay <= scan_delay
                );
                assert!(
                    at_seal
                        .validate_publication([new_image.id, replacement, live])
                        .is_ok()
                );
                assert!(at_seal.validate_publication([zombie]).is_err());

                let successor = Reachability::from_records(
                    DATABASE,
                    3,
                    &[checkpoint(&new_image, 8)],
                    [&new_image],
                )
                .unwrap();
                for key in deletion.keys() {
                    assert!(
                        !successor.contains(key.as_str().parse().unwrap()),
                        "delete delay {delete_delay}"
                    );
                }
                let next_scan = successor.plan_gc([&zombie_key, &replacement_key, &live_key]);
                assert_eq!(next_scan.keys(), &[zombie_key.clone()]);
            }
        }
    }
}

#[test]
fn gc_c2_pin_survives_checkpoint_trim_and_delayed_export_until_release() {
    let exported = file(1, 0);
    let unrelated = file(1, 1);
    let original = manifest(1, 2, vec![exported], vec![]);
    let pin = Pin {
        id: PIN,
        manifest: original.id,
        until: Some(2),
    };
    let pinned = manifest(2, 0, vec![], vec![pin.clone()]);
    let released = manifest(2, 1, vec![], vec![]);
    let history = [
        checkpoint(&original, 1),
        Record::Pin(pin),
        checkpoint(&pinned, 3),
        Record::Unpin { id: PIN },
        checkpoint(&released, 5),
    ];
    let manifests = [&original, &pinned, &released];
    let exported_key = key(exported).unwrap();
    let original_key = key(original.id).unwrap();
    let unrelated_key = key(unrelated).unwrap();

    for trim_delay in 0..8 {
        for export_delay in 0..8 {
            let plan = Reachability::from_records(DATABASE, 3, &history[..2], manifests)
                .unwrap()
                .plan_gc([&exported_key, &original_key, &unrelated_key]);
            assert_eq!(plan.keys(), &[unrelated_key.clone()]);
            let retained = if trim_delay <= export_delay {
                &history[2..3]
            } else {
                &history[..3]
            };
            let during_export =
                Reachability::from_records(DATABASE, 3, retained, manifests).unwrap();
            assert_eq!(during_export.pins().count(), 1);
            assert!(
                during_export
                    .plan_gc([&exported_key, &original_key])
                    .keys()
                    .is_empty()
            );
        }
    }
    let release_committed =
        Reachability::from_records(DATABASE, 3, &history[2..4], manifests).unwrap();
    assert_eq!(release_committed.pins().count(), 0);
    assert!(
        release_committed
            .plan_gc([&exported_key, &original_key])
            .keys()
            .is_empty()
    );
    let after_trim = Reachability::from_records(DATABASE, 3, &history[4..], manifests).unwrap();
    assert_eq!(
        after_trim
            .plan_gc([&exported_key, &original_key])
            .keys()
            .len(),
        2
    );
}

#[test]
fn gc_c3_trim_and_stale_lists_preserve_every_retained_recovery_input() {
    let old = file(1, 0);
    let retained_tx = file(1, 1);
    let unrelated = file(1, 2);
    let current = file(2, 0);
    let original = manifest(1, 3, vec![old], vec![]);
    let replacement = manifest(2, 1, vec![current], vec![]);
    let history = [
        checkpoint(&original, 0),
        Record::Tx {
            payload: vec![0],
            objects: vec![retained_tx],
        },
        checkpoint(&replacement, 2),
    ];
    let manifests = [&original, &replacement];
    let ids = [
        old,
        retained_tx,
        unrelated,
        original.id,
        current,
        replacement.id,
    ];
    let keys: Vec<_> = ids.into_iter().map(|id| key(id).unwrap()).collect();

    for listing in 0..1_u32 << ids.len() {
        let listed = keys
            .iter()
            .enumerate()
            .filter(|(i, _)| listing & (1 << i) != 0)
            .map(|(_, key)| key);
        for floor in [0, 2] {
            let capture =
                Reachability::from_records(DATABASE, 3, &history[floor..], manifests).unwrap();
            let plan = capture.plan_gc(listed.clone());
            let expected = if floor == 0 {
                vec![unrelated]
            } else {
                vec![old, retained_tx, unrelated, original.id]
            };
            for (i, id) in ids.into_iter().enumerate() {
                assert_eq!(
                    plan.keys().contains(&keys[i]),
                    expected.contains(&id) && listing & (1 << i) != 0
                );
            }
            assert!(capture.contains(replacement.id));
            assert!(capture.contains(current));
        }
    }
    assert!(Reachability::from_records(DATABASE, 3, &history, [&replacement]).is_err());
}

#[test]
fn gc_c4_in_database_branch_keeps_history_without_reviving_an_old_checkpoint() {
    let old = file(1, 0);
    let history = file(2, 0);
    let orphaned_image = manifest(1, 1, vec![old], vec![]);
    let current_image = manifest(2, 1, vec![history], vec![]);
    let retained = [
        checkpoint(&orphaned_image, 2),
        checkpoint(&current_image, 4),
    ];
    let manifests = [&orphaned_image, &current_image];
    let mut foreign = old;
    foreign.database = Uuid::from_u128(42);
    let listed =
        [old, orphaned_image.id, history, current_image.id, foreign].map(|id| key(id).unwrap());

    for trim_tick in 0..8 {
        for branch_tick in 0..8 {
            let floor = usize::from(trim_tick <= branch_tick);
            let reachability =
                Reachability::from_records(DATABASE, 3, &retained[floor..], manifests).unwrap();
            let plan = reachability.plan_gc(&listed);
            assert_eq!(plan.keys().len(), 2 * floor);
            assert_eq!(plan.keys().contains(&listed[0]), floor == 1);
            assert_eq!(plan.keys().contains(&listed[1]), floor == 1);
            assert!(
                reachability
                    .validate_publication([file(3, 0), history])
                    .is_ok()
            );
            assert_eq!(
                reachability
                    .validate_publication([orphaned_image.id, old])
                    .is_err(),
                floor == 1,
            );
            assert!(reachability.validate_publication([foreign]).is_err());
        }
    }
}

#[test]
fn gc_default_leader_preserves_replica_checkpoint_pin_and_compaction_roots() {
    let checkpoint = manifest(1, 0, vec![], vec![]).id;
    let block = file(1, 1);
    let replay = file(1, 2);
    let export = file(1, 3);
    let export_manifest = manifest(1, 4, vec![], vec![]).id;
    let compaction_input = file(1, 5);
    let abandoned_output = file(1, 6);
    let new_output = file(2, 0);
    let ids = [
        checkpoint,
        block,
        replay,
        export,
        export_manifest,
        compaction_input,
        abandoned_output,
        new_output,
    ];
    let keys = ids.map(|id| key(id).unwrap());
    let leader = Reachability::from_roots(DATABASE, 2, ids[..6].iter().copied()).unwrap();

    for upload_tick in 0..4 {
        for scan_tick in 0..4 {
            let inventory = keys
                .iter()
                .enumerate()
                .filter_map(|(i, key)| (i != 6 || upload_tick <= scan_tick).then_some(key));
            let plan = leader.plan_gc(inventory);
            assert_eq!(plan.keys().contains(&keys[6]), upload_tick <= scan_tick);
            assert!(plan.keys().iter().all(|key| key == &keys[6]));
            assert!(leader.validate_publication([abandoned_output]).is_err());
            assert!(
                leader
                    .validate_publication([new_output, compaction_input])
                    .is_ok()
            );

            for release_tick in 0..4 {
                for delete_tick in scan_tick..4 {
                    let roots = if release_tick <= delete_tick {
                        vec![block, new_output]
                    } else {
                        ids[..6].iter().copied().chain([new_output]).collect()
                    };
                    let successor = Reachability::from_roots(DATABASE, 3, roots).unwrap();
                    assert!(
                        plan.keys()
                            .iter()
                            .all(|key| !successor.contains(key.as_str().parse().unwrap()))
                    );
                    assert!(successor.contains(new_output));
                    assert!(successor.contains(block));
                }
            }
        }
    }
}

#[test]
fn gc_timed_decisions_preserve_dependencies_of_the_retained_tx() {
    let pending = file(1, 0);
    let key = key(pending).unwrap();
    let tx = Record::Tx {
        payload: vec![],
        objects: vec![pending],
    };
    for decision in [Record::Settle { tx_lsn: 4 }, Record::Refuse { tx_lsn: 4 }] {
        let retained = [tx.clone(), decision];
        let roots = Reachability::from_records(DATABASE, 2, &retained, []).unwrap();
        assert!(roots.plan_gc([&key]).keys().is_empty());
    }
}

#[tokio::test]
async fn gc_executes_idempotent_exact_key_deletes() {
    let store = crate::MemoryStore::default();
    let old = key(file(1, 0)).unwrap();
    let current = key(file(2, 0)).unwrap();
    for key in [&old, &current] {
        store
            .put(key, bytes::Bytes::from_static(b"object"))
            .await
            .unwrap();
    }
    let reached = Reachability::from_records(DATABASE, 2, &[], []).unwrap();
    let listed = store.list(None).await.unwrap();
    let plan = reached.plan_gc(listed.iter().map(|record| &record.key));
    plan.execute(&store).await.unwrap();
    plan.execute(&store).await.unwrap();
    assert!(store.get(&old).await.is_err());
    assert_eq!(store.get(&current).await.unwrap(), b"object"[..]);
}
