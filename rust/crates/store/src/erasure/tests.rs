use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use rtdb_crypt::{LocalKms, erased_under_view};
use rtdb_types::AnomalyCategory;
use uuid::Uuid;

use super::*;
use crate::{MemoryStore, read_file, read_manifest};

#[derive(Default)]
struct Journal {
    appends: Mutex<Vec<(u64, Bytes)>>,
    trimmed: Mutex<Option<u64>>,
    lose_rotation_reply: AtomicBool,
}

impl Journal {
    async fn append(&self, lsn: u64, ciphertext: Bytes) -> Result<i64> {
        let mut appends = self.appends.lock().unwrap();
        if let Some((_, existing)) = appends.iter().find(|(position, _)| *position == lsn) {
            assert_eq!(existing, &ciphertext);
            return Ok(100);
        }
        appends.push((lsn, ciphertext));
        if lsn == 11 && self.lose_rotation_reply.swap(false, Ordering::SeqCst) {
            return Err(Anomaly::new(
                AnomalyCategory::Interrupted,
                "test/unknown",
                "Reply lost",
            ));
        }
        Ok(100)
    }

    async fn trim(&self, through: u64) -> Result<()> {
        assert_eq!(self.appends.lock().unwrap().len(), 2);
        *self.trimmed.lock().unwrap() = Some(through);
        Ok(())
    }
}

#[tokio::test]
async fn erasure_retries_exact_rotation_bytes_and_keeps_clean_checkpoint_recoverable() {
    let database = Uuid::from_u128(99);
    let now = Arc::new(AtomicI64::new(100));
    let kms = LocalKms::new({
        let now = Arc::clone(&now);
        move || now.load(Ordering::SeqCst)
    });
    let store = MemoryStore::default();
    let journal = Journal::default();
    let mut writer = WriterObjects::new(database, 2);
    let old = Generation {
        database,
        number: 1,
    };
    kms.create(old).await.unwrap();
    let old_file = writer.file(&kms, 1, b"erase me").await.unwrap();
    old_file.put(&store).await.unwrap();
    let old_context = Context::Journal { lsn: 9 };
    let old_record = rtdb_crypt::seal(&kms, old, old_context, b"erase me")
        .await
        .unwrap();

    let plan = ErasurePlan::new(old, 10, 8, 2).unwrap();
    assert_eq!(
        (
            plan.checkpoint_lsn(),
            plan.rotation_lsn(),
            plan.trim_through()
        ),
        (10, 11, 9)
    );
    let prepared = plan
        .prepare(
            &mut writer,
            &kms,
            &[Bytes::from_static(b"keep me")],
            vec![],
            vec![],
            vec![42],
        )
        .await
        .unwrap();
    journal.lose_rotation_reply.store(true, Ordering::SeqCst);
    assert_eq!(
        prepared
            .execute(
                &store,
                &kms,
                |lsn, bytes| journal.append(lsn, bytes),
                |through| journal.trim(through)
            )
            .await
            .unwrap_err()
            .category,
        AnomalyCategory::Interrupted
    );
    assert_eq!(*journal.trimmed.lock().unwrap(), None);
    let retirement = prepared
        .execute(
            &store,
            &kms,
            |lsn, bytes| journal.append(lsn, bytes),
            |through| journal.trim(through),
        )
        .await
        .unwrap();
    assert_eq!(*journal.trimmed.lock().unwrap(), Some(9));
    assert_eq!(retirement.unreadable_by(), 110);

    let appends = journal.appends.lock().unwrap().clone();
    let mut decoded = Vec::new();
    for (lsn, bytes) in appends {
        let opened = rtdb_crypt::open(&kms, database, Context::Journal { lsn }, &bytes)
            .await
            .unwrap();
        assert_eq!(opened.generation, plan.next_generation());
        decoded.push(Record::decode(&opened.bytes).unwrap());
    }
    assert_eq!(
        decoded,
        [
            Record::Checkpoint(prepared.checkpoint().clone()),
            Record::KeyRotate {
                generation: 2,
                at: 10
            },
        ]
    );

    now.store(109, Ordering::SeqCst);
    assert_eq!(
        &*read_file(&store, &kms, old_file.id()).await.unwrap().bytes,
        b"erase me"
    );
    assert!(retirement.destroy(&kms).await.is_err());
    now.store(110, Ordering::SeqCst);
    assert!(
        matches!(read_file(&store, &kms, old_file.id()).await, Err(err) if err == erased_under_view())
    );
    assert!(
        matches!(rtdb_crypt::open(&kms, database, old_context, &old_record).await, Err(err) if err == erased_under_view())
    );
    retirement.destroy(&kms).await.unwrap();
    plan.finish_rotation(&kms, 100, |through| journal.trim(through))
        .await
        .unwrap()
        .destroy(&kms)
        .await
        .unwrap();

    let clean = read_manifest(&store, &kms, prepared.checkpoint().manifest)
        .await
        .unwrap();
    assert_eq!(clean.contents().payload, [42]);
    assert_eq!(clean.generation, plan.next_generation());
    let clean_file = read_file(&store, &kms, clean.contents().files[0])
        .await
        .unwrap();
    assert_eq!(clean_file.generation, plan.next_generation());
    assert_eq!(&*clean_file.bytes, b"keep me");
    assert_eq!(store.list(None).await.unwrap().len(), 3);
}

#[test]
fn erasure_plan_rejects_position_generation_and_deadline_overflow() {
    let old = Generation {
        database: Uuid::nil(),
        number: 1,
    };
    assert!(ErasurePlan::new(old, 0, 0, 0).is_err());
    assert!(ErasurePlan::new(old, u64::MAX, 0, 0).is_err());
    assert!(
        ErasurePlan::new(
            Generation {
                number: u64::MAX,
                ..old
            },
            1,
            0,
            0
        )
        .is_err()
    );
    assert!(ErasurePlan::new(old, 1, 1, u64::MAX).is_err());
    assert!(
        ErasurePlan::new(old, 1, 1, 1)
            .unwrap()
            .retirement(i64::MAX)
            .is_err()
    );
}
