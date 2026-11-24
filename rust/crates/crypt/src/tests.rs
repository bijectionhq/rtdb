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
async fn custody_values_bind_the_member_and_remain_revoked_after_vault_reopen() {
    let directory = tempfile::tempdir().unwrap();
    FileKms::create_directory(directory.path()).unwrap();
    let incarnation = Uuid::from_u128(19);
    let group = [3; 16];
    let other_group = [4; 16];
    let binding = [5; 32];
    let other_binding = [6; 32];
    let vault = CustodyVault::new(incarnation, Arc::new(FileKms::open(directory.path(), || 100).unwrap()));
    let another_incarnation = CustodyVault::new(Uuid::from_u128(20), Arc::new(FileKms::open(directory.path(), || 100).unwrap()));
    vault.create(group).await.unwrap();
    vault.create(other_group).await.unwrap();
    another_incarnation.create(group).await.unwrap();
    let original = vault.seal(group, binding, b"original").await.unwrap();
    let copy = vault.seal(group, other_binding, b"copy").await.unwrap();
    let unaffected = vault.seal(other_group, binding, b"unaffected").await.unwrap();
    assert_eq!(&*vault.open(group, other_binding, &copy).await.unwrap().bytes, b"copy");
    assert!(vault.open(group, binding, &copy).await.is_err());
    assert!(vault.open(other_group, binding, &original).await.is_err());
    assert!(another_incarnation.open(group, binding, &original).await.is_err());
    vault.retire(group, 100).await.unwrap();
    vault.destroy(group).await.unwrap();
    drop(vault);

    let reopened = CustodyVault::new(incarnation, Arc::new(FileKms::open(directory.path(), || 101).unwrap()));
    assert_eq!(reopened.check_readable(group).await.unwrap_err(), erased_under_view());
    assert!(reopened.open(group, binding, &original).await.is_err());
    assert!(reopened.open(group, other_binding, &copy).await.is_err());
    assert!(reopened.create(group).await.is_err());
    assert_eq!(&*reopened.open(other_group, binding, &unaffected).await.unwrap().bytes, b"unaffected");
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
