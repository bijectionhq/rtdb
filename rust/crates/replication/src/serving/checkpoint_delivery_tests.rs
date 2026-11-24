use std::sync::atomic::{AtomicI64, Ordering};

use rtdb_crypt::LocalKms;
use rtdb_storage::MemoryStore;
use rtdb_tx::transact::Outcome;

use super::*;

#[tokio::test]
async fn a_reopened_view_checks_the_actual_checkpoint_generation_at_delivery() -> Result<()> {
    let incarnation = [7; 16];
    let database = Uuid::from_bytes(incarnation);
    let time = Arc::new(AtomicI64::new(0));
    let kms: Arc<dyn Kms> = Arc::new(LocalKms::new({
        let time = Arc::clone(&time);
        move || time.load(Ordering::SeqCst)
    }));
    let selected = Generation {
        database,
        number: 1,
    };
    let current = Generation {
        database,
        number: 2,
    };
    kms.create(selected).await?;
    kms.create(current).await?;
    let authority = NativeServingAuthority {
        incarnation,
        checkpoints: Some(NativeCheckpointAuthority {
            kms: Arc::clone(&kms),
            generation: current.number,
        }),
    };
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
    let (mut serving, _) = NativeServing::open(authority.clone(), Arc::clone(&store), None, false)?;
    let image = serving.checkpoint(Term(1), false)?.image;
    let stored = WriterObjects::new(database, 1)
        .file(kms.as_ref(), selected.number, &image)
        .await?;
    stored.put(store.as_ref()).await?;
    let key = stored.id().to_string();
    let checkpoint = authority
        .read_checkpoint(store.as_ref(), None, Some(&key))
        .await?;
    let (_reopened, view) =
        NativeServing::open(authority.clone(), store, checkpoint.as_ref(), false)?;
    let generation = view.borrow().key_generation.unwrap();
    assert_eq!(generation, selected.number);
    kms.retire(selected, 10).await?;
    authority.check_checkpoint_generation(generation).await?;
    time.store(10, Ordering::SeqCst);
    assert_eq!(
        authority
            .check_checkpoint_generation(generation)
            .await
            .unwrap_err()
            .code,
        "rtdb/erased-under-view"
    );
    authority
        .check_checkpoint_generation(current.number)
        .await?;
    Ok(())
}

#[test]
fn pending_erasure_receipts_cannot_be_delivered() -> Result<()> {
    let index = BranchIndex::new([4; 16]);
    let cut = index.head();
    let receipt = Receipt {
        key: vec![2; 16],
        basis: cut.basis,
        system_time: cut.system_time,
        outcome: Outcome::Then,
        replayed: false,
        erasure: Some(Erasure::Pending),
    };
    assert_eq!(
        project_receipt(&index, receipt.clone(), None)
            .unwrap_err()
            .code,
        "rtdb/erasure-pending"
    );
    let armed = Erasure::Armed {
        unreadable_by: Timestamp(20),
    };
    let receipt = Receipt {
        erasure: Some(armed),
        ..receipt
    };
    assert_eq!(project_receipt(&index, receipt, None)?.erasure, Some(armed));
    Ok(())
}
