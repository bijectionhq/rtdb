use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use super::*;

fn generation(number: u64) -> Generation {
    Generation {
        database: Uuid::from_u128(17),
        number,
    }
}

#[tokio::test]
async fn envelopes_authenticate_database_purpose_position_and_bytes() {
    let kms = LocalKms::new(|| 0);
    let generation = generation(1);
    kms.create(generation).await.unwrap();
    let other_database = Uuid::from_u128(18);
    kms.create(Generation {
        database: other_database,
        ..generation
    })
    .await
    .unwrap();
    let context = Context::Journal { lsn: 7 };
    let bytes = seal(&kms, generation, context, b"secret").await.unwrap();

    assert_ne!(
        bytes,
        seal(&kms, generation, context, b"secret").await.unwrap()
    );
    assert_eq!(
        &*open(&kms, generation.database, context, &bytes)
            .await
            .unwrap()
            .bytes,
        b"secret"
    );
    for context in [
        Context::Journal { lsn: 6 },
        Context::File { epoch: 0, seq: 7 },
    ] {
        assert!(
            open(&kms, generation.database, context, &bytes)
                .await
                .is_err()
        );
    }
    assert!(open(&kms, other_database, context, &bytes).await.is_err());

    for i in 0..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[i] ^= 1;
        assert!(
            open(&kms, generation.database, context, &corrupt)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn deadline_is_exclusive_and_destroyed_generation_cannot_reopen() {
    let now = Arc::new(AtomicI64::new(0));
    let kms = LocalKms::new({
        let now = Arc::clone(&now);
        move || now.load(Ordering::SeqCst)
    });
    let old = generation(1);
    let clean = generation(2);
    kms.create(old).await.unwrap();
    kms.create(clean).await.unwrap();
    let context = Context::Journal { lsn: 7 };
    let before = seal(&kms, old, context, b"erased").await.unwrap();
    let after = seal(&kms, clean, context, b"kept").await.unwrap();
    kms.retire(old, 10).await.unwrap();
    kms.retire(old, 10).await.unwrap();
    assert!(kms.retire(old, 11).await.is_err());
    assert!(seal(&kms, old, context, b"late").await.is_err());

    now.store(9, Ordering::SeqCst);
    assert!(kms.destroy(old).await.is_err());
    assert!(open(&kms, old.database, context, &before).await.is_ok());
    now.store(10, Ordering::SeqCst);
    let Err(error) = open(&kms, old.database, context, &before).await else {
        panic!("generation opened at its deadline");
    };
    assert_eq!(error, erased_under_view());
    kms.destroy(old).await.unwrap();
    kms.destroy(old).await.unwrap();
    assert_eq!(kms.create(old).await.unwrap_err(), erased_under_view());
    assert_eq!(
        &*open(&kms, clean.database, context, &after)
            .await
            .unwrap()
            .bytes,
        b"kept"
    );
}
