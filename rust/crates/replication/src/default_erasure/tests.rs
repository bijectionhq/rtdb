use std::io::Cursor;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{
    catalog::BlockCatalog,
    data::EventBatch,
    ipc::ArrowFile,
    keys::{CURRENT_STORAGE_VERSION, TrieKey},
};
use rtdb_index::{
    PartitionState, TransactionBuffer,
    branches::{BranchCut, BranchEffect, BranchEvent, BranchIndex, RowImage},
};
use rtdb_storage::MemoryStore;
use rtdb_trie::TrieConfig;
use rtdb_tx::transact::{Outcome, Receipt};
use rtdb_types::{
    BlockIndex, EntityId, EventOp, Interval, SourcePosition, StorageEpoch, StructFields, TableRef, TermSequence,
    TransactionKey, TxId, Value, rtdb_v1::MAIN_BRANCH,
};

use super::*;

#[test]
fn an_ordered_retirement_keeps_the_deadline_when_reopened() -> Result<()> {
    let cut = ErasureCut {
        basis: Basis::new([3; 16], 9),
        source: SourcePosition(31),
        replica: ReplicaPosition(41),
        term: Term(6),
    };
    let selected = SelectedRewrite {
        evidence: RewriteEvidence {
            cut,
            boundary: ReplicaPosition(42),
            members_sha256: [4; 32],
            inventory_sha256: Some([8; 32]),
            prior_generation: Some(1),
        },
        checkpoint: PublishedCheckpoint {
            block: BlockIndex(8),
            boundary: ReplicaPosition(42),
            root_sha256: Some([5; 32]),
        },
        generation: 2,
        uploaded: Some(ReplicaPosition(43)),
    };
    let initial = ErasureRetirement::observe(selected, Timestamp(1000), 200)?;
    let replayed = ErasureRetirement::decode(&initial.message()?)?.unwrap();
    assert_eq!(replayed.unreadable_by, Timestamp(1200));
    replayed.require_selected(selected, ReplicaPosition(44))?;
    let completed = ErasureCompleted {
        intent: replayed,
        ordered_intent: ReplicaPosition(44),
        read_closure: [10; 32],
    };
    let reopened = ErasureCompleted::decode(&completed.message()?)?.unwrap();
    assert_eq!(reopened, completed);
    reopened.require_order(initial, ReplicaPosition(44), ReplicaPosition(45))?;
    assert!(
        reopened
            .require_order(initial, ReplicaPosition(44), ReplicaPosition(44))
            .is_err()
    );
    assert!(
        reopened
            .require_order(initial, ReplicaPosition(43), ReplicaPosition(45))
            .is_err()
    );
    let retirement = reopened.intent.read_cut()?;
    assert_eq!((retirement.predecessor, retirement.successor), (1, 2));
    assert_eq!(retirement.unreadable_by, Timestamp(1200));
    assert!(
        replayed
            .require_selected(selected, ReplicaPosition(43))
            .is_err()
    );
    let foreign = SelectedRewrite {
        checkpoint: PublishedCheckpoint {
            root_sha256: Some([6; 32]),
            ..selected.checkpoint
        },
        ..selected
    };
    assert!(
        replayed
            .require_selected(foreign, ReplicaPosition(44))
            .is_err()
    );
    let frame = selected.evidence.frame(b"prefix", b"[state]")?;
    assert_eq!(
        RewriteEvidence::read(&frame[6..])?,
        (Some(selected.evidence), b"[state]".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn predecessor_destruction_preserves_the_successor_across_vault_restart() -> Result<()> {
    use crate::NativeCheckpointAuthority;
    use rtdb_crypt::{FileKms, Generation, Kms};

    let root = tempfile::tempdir().unwrap();
    let vault = root.path().join("vault");
    FileKms::create_directory(&vault)?;
    let incarnation = [3; 16];
    let keys = Arc::new(FileKms::open(&vault, || 50)?);
    let database = uuid::Uuid::from_bytes(incarnation);
    let predecessor = Generation {
        database,
        number: 1,
    };
    let successor = Generation {
        database,
        number: 2,
    };
    keys.create(predecessor).await?;
    keys.create(successor).await?;
    let authority = NativeServingAuthority {
        incarnation,
        checkpoints: Some(NativeCheckpointAuthority {
            kms: keys,
            generation: 1,
        }),
    };
    let cut = ErasureCut {
        basis: Basis::new(incarnation, 9),
        source: SourcePosition(31),
        replica: ReplicaPosition(41),
        term: Term(6),
    };
    let intent = ErasureRetirement {
        evidence: RewriteEvidence {
            cut,
            boundary: ReplicaPosition(42),
            members_sha256: [4; 32],
            inventory_sha256: Some([8; 32]),
            prior_generation: Some(1),
        },
        block: BlockIndex(8),
        root_sha256: [5; 32],
        upload: ReplicaPosition(43),
        observed_at: Timestamp(30),
        unreadable_by: Timestamp(50),
    };
    assert!(
        !intent
            .destroy_predecessor(&authority, Timestamp(49))
            .await?
    );
    assert!(
        intent
            .destroy_predecessor(&authority, Timestamp(50))
            .await?
    );
    let reopened = Arc::new(FileKms::open(&vault, || 50)?);
    assert!(reopened.generation_destroyed(predecessor).await?);
    reopened.check_readable(successor).await?;
    let authority = NativeServingAuthority {
        incarnation,
        checkpoints: Some(NativeCheckpointAuthority {
            kms: reopened,
            generation: 2,
        }),
    };
    assert!(
        intent
            .destroy_predecessor(&authority, Timestamp(51))
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn a_native_erase_rewrites_classic_and_native_rows_into_independent_files() -> Result<()> {
    let partition = PartitionState::empty(TrieConfig::default())?;
    let memory = MemoryBudget::new(64 << 20);
    let table = TableRef::new("public", "mixed");
    let erased = EntityId::Integer(1);
    let retained = EntityId::Integer(2);
    let tx = |position| TransactionKey {
        tx_id: TxId(position),
        system_time: Timestamp(position),
    };
    let mut classic = TransactionBuffer::new(partition.snapshot(), tx(1), Arc::clone(&memory))?;
    classic.put(
        table.clone(),
        StructFields::from([
            ("_id".into(), Value::I64(1)),
            ("body".into(), Value::Utf8("erase".into())),
        ]),
        Interval::ALL,
    )?;
    partition.apply(&classic.stage()?)?;
    let incarnation = [7; 16];
    let prior = BranchIndex::new(incarnation);
    let write = BranchEffect::Write {
        branch: MAIN_BRANCH.into(),
        table: table.clone(),
        event: BranchEvent {
            id: retained.clone(),
            valid: Interval::ALL,
            cut: BranchCut {
                basis: Basis::new(incarnation, 2),
                system_time: Timestamp(2),
            },
            image: Some(Arc::new(RowImage {
                cells: StructFields::from([
                    ("_id".into(), Value::I64(2)),
                    ("body".into(), Value::Utf8("retain".into())),
                ]),
                expires_at: None,
                custody_group: None,
                custody_payload: None,
            })),
            written: Default::default(),
        },
    };
    let receipt = |position: u64| Receipt {
        key: vec![position as u8; 16],
        basis: Basis::new(incarnation, position),
        system_time: Timestamp(position as i64),
        outcome: Outcome::Then,
        replayed: false,
        erasure: None,
        decision_clock: None, store_reading: None,
    };
    let write = serving::classic::stage_source(
        partition.snapshot(), &prior, SourcePosition(2), &receipt(2), &[write], &memory,
    )?;
    partition.apply(&write)?;
    let erase = BranchEffect::Erase {
        branch: None,
        table: table.clone(),
        id: erased.clone(),
    };
    let write = serving::classic::stage_source(
        partition.snapshot(), &prior, SourcePosition(3), &receipt(3), &[erase], &memory,
    )?;
    let targets = erasure_targets(&write, &memory)?;
    partition.apply(&write)?;
    let snapshot = partition.snapshot();
    let store = MemoryStore::default();
    let fence = TermFence::from_boundary(Term(1), Some(TermSequence(3)))?;
    let held: Arc<dyn Send + Sync> = Arc::new(());
    let boundary = ReplicaPosition(8);
    let survivor = SurvivorInput {
        snapshot: &snapshot,
        targets: &targets,
        cut: CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION,
            storage_epoch: StorageEpoch(0),
            replica_epoch: StorageEpoch(0),
            through: boundary,
        },
        source_position: SourcePosition(3),
        inherited_erasure: None,
        custody: &held,
        store: &store,
        memory: &memory,
        fence: &fence,
        external_token: None,
    }
    .build(16 << 20, &CancellationToken::new())
    .await?;
    let catalog = &survivor.catalog;
    assert_eq!(catalog.latest_source_position(), Some(SourcePosition(3)));
    assert_eq!(catalog.boundary_replica_position(), Some(boundary));
    assert_eq!(catalog.naming_epoch(), 1);
    let entry = catalog
        .table_entries()?
        .into_iter()
        .find(|entry| entry.table == table)
        .unwrap();
    let key = ObjectKey::new(entry.slug.data_key(&TrieKey::l0(catalog.block_index())?))?;
    let mut file = ArrowFile::from_reader(Cursor::new(survivor.store().get(&key).await?), None)?;
    let mut rows = Vec::new();
    for page in 0..file.page_count() {
        let batch = EventBatch::new(file.read_page(page)?)?;
        for row in 0..batch.len() {
            let header = batch.header(row)?;
            rows.push((header.iid, header.op));
        }
    }
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(erased.iid(), EventOp::Erase)));
    assert!(rows.contains(&(retained.iid(), EventOp::Put)));
    let root = ObjectKey::new(block_key(catalog.block_index())?)?;
    assert_eq!(
        BlockCatalog::decode(&survivor.store().get(&root).await?)?,
        *catalog
    );
    Ok(())
}
