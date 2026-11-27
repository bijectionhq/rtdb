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

fn recovery_hold(generation: Generation) -> RecoveryKeyHold {
    RecoveryKeyHold {
        authority: Uuid::from_u128(23),
        authority_sha256: [27; 32],
        operation: Uuid::from_u128(24),
        generation,
        installation_sha256: [25; 32],
    }
}

#[tokio::test]
async fn recovery_key_hold_survives_reopen_and_remembers_exact_release() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vault");
    FileKms::create_directory(&path).unwrap();
    let generation = generation(1);
    let hold = recovery_hold(generation);
    let foreign = RecoveryKeyHold { installation_sha256: [26; 32], ..hold };
    let kms = FileKms::open(&path, || 0).unwrap();
    kms.create(generation).await.unwrap();
    let context = Context::Journal { lsn: 7 };
    let sealed = seal(&kms, generation, context, b"successor").await.unwrap();
    assert!(kms.require_recovery_key(hold).await.is_err());
    kms.retain_recovery_key(hold).await.unwrap();
    kms.retain_recovery_key(hold).await.unwrap();
    assert!(kms.retain_recovery_key(foreign).await.is_err());
    assert!(kms.require_recovery_key(foreign).await.is_err());
    assert!(kms.release_recovery_key(foreign).await.is_err());
    assert!(kms.retire(generation, 1).await.is_err());
    drop(kms);

    let kms = FileKms::open(&path, || 2).unwrap();
    kms.require_recovery_key(hold).await.unwrap();
    assert!(kms.destroy(generation).await.is_err());
    assert_eq!(&*open(&kms, generation.database, context, &sealed).await.unwrap().bytes, b"successor");
    kms.release_recovery_key(hold).await.unwrap();
    assert!(kms.require_recovery_key(hold).await.is_err());
    kms.retire(generation, 2).await.unwrap();
    kms.destroy(generation).await.unwrap();
    drop(kms);

    let kms = FileKms::open(&path, || 3).unwrap();
    kms.release_recovery_key(hold).await.unwrap();
    assert!(kms.release_recovery_key(foreign).await.is_err());
    assert!(kms.retain_recovery_key(hold).await.is_err());
    assert!(kms.generation_destroyed(generation).await.unwrap());
}

#[tokio::test]
async fn recovery_key_hold_and_retirement_cannot_both_win_admission() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vault");
    FileKms::create_directory(&path).unwrap();
    let generation = generation(1);
    let hold = recovery_hold(generation);
    let kms = FileKms::open(&path, || 0).unwrap();
    kms.create(generation).await.unwrap();
    let peer = FileKms::open(&path, || 0).unwrap();
    let (held, retired) = tokio::join!(
        kms.retain_recovery_key(hold),
        peer.retire(generation, 10),
    );
    assert_ne!(held.is_ok(), retired.is_ok());
    if retired.is_ok() {
        assert!(kms.retain_recovery_key(hold).await.is_err());
        let reopened = FileKms::open(&path, || 10).unwrap();
        assert_eq!(reopened.check_readable(generation).await.unwrap_err(), erased_under_view());
    } else {
        peer.require_recovery_key(hold).await.unwrap();
        assert!(peer.retire(generation, 10).await.is_err());
    }
}

#[tokio::test]
async fn custody_values_bind_the_member_and_remain_revoked_after_vault_reopen() {
    let root = tempfile::tempdir().unwrap();
    let vault_path = root.path().join("vault");
    FileKms::create_directory(&vault_path).unwrap();
    let incarnation = Uuid::from_u128(19);
    let group = [3; 16];
    let other_group = [4; 16];
    let binding = [5; 32];
    let other_binding = [6; 32];
    let vault = CustodyVault::new(incarnation, Arc::new(FileKms::open(&vault_path, || 100).unwrap()));
    let another_incarnation = CustodyVault::new(Uuid::from_u128(20), Arc::new(FileKms::open(&vault_path, || 100).unwrap()));
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

    let reopened = CustodyVault::new(incarnation, Arc::new(FileKms::open(&vault_path, || 101).unwrap()));
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
async fn backup_envelopes_have_their_own_purpose() {
    let kms = LocalKms::new(|| 0);
    let generation = generation(1);
    kms.create(generation).await.unwrap();

    let binding = [7; 32];
    let context = Context::PortableBackup { header_digest: binding };
    let envelope = seal(&kms, generation, context, b"checkpoint").await.unwrap();
    assert!(open(&kms, generation.database, context, &envelope).await.is_ok());
    assert!(open(&kms, generation.database, Context::CustodyValue { binding }, &envelope).await.is_err());
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
