use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use journal_contract::{Journal, WriterSessionId};
use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_node::{Clock, SystemClock};
use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};

use super::*;
use crate::multi::activation::{
    VerifiedSuccessor,
    funded::Enrollment,
    recovery::{Intent, LossPolicy, Operation, SourceCut, Status},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciles_funded_bookings_with_original_commitments_and_a_lossless_seal() {
    const PRINCIPAL: &str = "urn:logkeeper:client:funded-reconciliation";
    const DATABASE: &str = "funded-reconciliation";
    let mut control = LocalCluster::start_independent_catalog(&[PRINCIPAL])
        .await
        .unwrap();
    let mut source = LocalCluster::start_timed(&[PRINCIPAL]).await.unwrap();
    control
        .finalize_version(PRINCIPAL, CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    source
        .finalize_version(PRINCIPAL, CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    let control_journal = control
        .journal(PRINCIPAL, &control.journal_config(8 << 20, 1 << 20))
        .unwrap();
    let source_journal = source
        .journal(PRINCIPAL, &source.journal_config(8 << 20, 1 << 20))
        .unwrap()
        .with_timed_original(source.timed_verifier(), Some(4096));
    let identity = Uuid::new_v4();
    let mut authority = ActivationAuthority::new(Arc::new(control_journal), identity).unwrap();
    let elapsed = AtomicU64::new(0);
    Arc::get_mut(&mut authority).unwrap().clock =
        Arc::new(move || Some(Duration::from_secs(elapsed.fetch_add(8, Ordering::SeqCst))));
    let predecessor = authority.bootstrap(DATABASE, Uuid::new_v4()).await.unwrap();
    let stream = StreamId(predecessor.incarnation.as_u128());
    source_journal.create_immutable(stream).await.unwrap();
    let writer = source_journal
        .seal_session(stream, Epoch(0), WriterSessionId(1))
        .await
        .unwrap();
    source_journal
        .append_session_batch(writer.fence, Lsn(1), vec![Bytes::from_static(b"base")])
        .await
        .unwrap();

    let issuer = Ed25519KeyPair::from_seed_unchecked(&[55; 32]).unwrap();
    let receiver = [4; 32];
    let enrollment = Enrollment {
        issuer: Uuid::new_v4(),
        receiver_certificate_sha256: receiver,
        verification_key: issuer.public_key().as_ref().try_into().unwrap(),
    };
    let scope = authority
        .commission_funded(
            DATABASE,
            predecessor,
            enrollment,
            Some(JournalTrust {
                catalog: source.catalog(),
                keepers: source
                    .timed_verification_keys()
                    .into_iter()
                    .map(|(identity, verification_key)| TrustedKeeper {
                        identity,
                        verification_key,
                    })
                    .collect(),
            }),
        )
        .await
        .unwrap();
    let accept_payload = Bytes::from_static(b"accepted appointment activation");
    let original = TimedOriginal {
        stream,
        epoch: writer.fence.writer_epoch,
        at: Lsn(2),
        payload_sha256: Sha256::digest(&accept_payload).into(),
        exclusive_journal_cutoff_micros: SystemClock.now().0 + 60_000_000,
        original_receiver_certificate_sha256: receiver,
    };
    source_journal
        .append_session_timed_original(writer.fence, original, accept_payload)
        .await
        .unwrap();
    let commitment = source_journal
        .attest_timed_original(original)
        .await
        .unwrap()
        .encode()
        .to_vec();
    let activate = Authorization {
        stage: Stage::Activate,
        writer_epoch: original.epoch,
        at: original.at,
        payload_sha256: original.payload_sha256,
        decision_before_micros: original.exclusive_journal_cutoff_micros - 1,
        journal_before_micros: original.exclusive_journal_cutoff_micros,
    };
    let consume = Authorization {
        stage: Stage::Consume,
        at: Lsn(3),
        payload_sha256: [8; 32],
        ..activate.clone()
    };
    let mut prefix = [0; 32];
    let mut resolutions = Vec::new();
    for index in 1..=33_u8 {
        let resolved = index == 1;
        let booking = Booking {
            id: Uuid::new_v4(),
            receipt: ReceiptKey {
                namespace: None,
                key: vec![index; 16],
            },
            owner: [6; 32],
            predecessor: Basis::new(*predecessor.incarnation.as_bytes(), 0),
            intent_sha256: [9; 32],
            kind: if resolved {
                Kind::Appointment
            } else {
                Kind::Transaction
            },
            authorizations: if resolved {
                vec![activate.clone(), consume.clone()]
            } else {
                Vec::new()
            },
        };
        prefix = next_prefix(prefix, &booking);
        let stages = if resolved {
            vec![
                Evidence::Committed {
                    commitment: commitment.clone(),
                },
                Evidence::ExcludedBySeal,
            ]
        } else {
            Vec::new()
        };
        resolutions.push((
            prefix,
            Resolution {
                scope_sha256: scope.scope_sha256,
                index: u64::from(index),
                booking,
                disposition: Disposition::Refused,
                stages,
                issuer_signature: Vec::new(),
            },
        ));
    }
    let mut closure = Closure {
        scope_sha256: scope.scope_sha256,
        bookings: resolutions.len() as u64,
        inventory_sha256: prefix,
        signature: Vec::new(),
    };
    closure.signature = issuer.sign(&closure.statement()).as_ref().to_vec();
    for (prefix, resolution) in &mut resolutions {
        resolution.issuer_signature = issuer
            .sign(&resolution.statement(&closure, *prefix))
            .as_ref()
            .to_vec();
    }
    let operation = Operation {
        predecessor,
        operation_id: Uuid::new_v4(),
    };
    let status = authority
        .begin_recovery(
            DATABASE,
            Intent {
                operation,
                destination_cell: None,
                loss_policy: LossPolicy::Lossless,
            },
            None,
        )
        .await
        .unwrap();
    let Status::Pending { candidate, .. } = status else {
        panic!("Expected a pending recovery")
    };
    authority
        .freeze_recovery(DATABASE, operation)
        .await
        .unwrap();
    authority
        .record_funded_closure(DATABASE, predecessor, closure.clone())
        .await
        .unwrap();
    let first = resolutions[0].1.clone();
    assert!(
        authority
            .record_funded_outcome(DATABASE, predecessor, first.clone())
            .await
            .is_err()
    );
    assert!(
        authority
            .seal_recovery_source(
                DATABASE,
                operation,
                &source_journal,
                None,
                Some(Uuid::new_v4())
            )
            .await
            .is_err()
    );
    authority
        .seal_recovery_source(
            DATABASE,
            operation,
            &source_journal,
            None,
            Some(source.catalog()),
        )
        .await
        .unwrap();
    source.shutdown().await.unwrap();

    let (_, state) = authority
        .read(authority.stream_id(DATABASE).unwrap())
        .await
        .unwrap();
    assert_eq!(
        authority
            .funded_settlements(&state.unwrap(), Lsn(2))
            .await
            .unwrap_err()
            .code,
        "rtdb/activation-funded-reconciliation-required"
    );
    assert!(
        authority
            .record_funded_outcome(DATABASE, predecessor, resolutions[32].1.clone())
            .await
            .is_err()
    );
    let mut forged = first.clone();
    if let Evidence::Committed { commitment } = &mut forged.stages[0] {
        *commitment.last_mut().unwrap() ^= 1;
    }
    forged.issuer_signature = issuer
        .sign(&forged.statement(&closure, resolutions[0].0))
        .as_ref()
        .to_vec();
    assert!(
        authority
            .record_funded_outcome(DATABASE, predecessor, forged)
            .await
            .is_err()
    );
    let verified = authority
        .record_funded_outcome(DATABASE, predecessor, first.clone())
        .await
        .unwrap();
    assert_eq!(verified.accepted_through, original.at);
    assert_eq!(
        verified.exclusion_fence,
        Some((Epoch(original.epoch.0 + 1), original.at))
    );
    assert_eq!(
        authority
            .record_funded_outcome(DATABASE, predecessor, first)
            .await
            .unwrap(),
        verified
    );
    for (_, resolution) in &resolutions[1..32] {
        authority
            .record_funded_outcome(DATABASE, predecessor, resolution.clone())
            .await
            .unwrap();
    }
    let mut wrong_inventory = resolutions[32].1.clone();
    wrong_inventory.booking.intent_sha256 = [10; 32];
    wrong_inventory.issuer_signature = issuer
        .sign(&wrong_inventory.statement(
            &closure,
            next_prefix(resolutions[31].0, &wrong_inventory.booking),
        ))
        .as_ref()
        .to_vec();
    assert!(
        authority
            .record_funded_outcome(DATABASE, predecessor, wrong_inventory)
            .await
            .is_err()
    );
    authority
        .record_funded_outcome(DATABASE, predecessor, resolutions[32].1.clone())
        .await
        .unwrap();
    let (_, state) = authority
        .read(authority.stream_id(DATABASE).unwrap())
        .await
        .unwrap();
    assert_eq!(
        authority
            .funded_settlements(&state.unwrap(), Lsn(1))
            .await
            .unwrap_err()
            .code,
        "rtdb/activation-funded-source-cut"
    );

    let (_, settlements) = authority
        .funded_backup_settlements(DATABASE, predecessor)
        .await
        .unwrap();
    let funded_inventory_sha256 =
        funded_settlement_digest(*predecessor.incarnation.as_bytes(), &settlements).unwrap();
    let installed = VerifiedSuccessor {
        predecessor: predecessor.incarnation,
        successor: candidate,
        manifest_digest: [20; 32],
        activation_scope: Some(rtdb_db::ActivationScope::new(identity, DATABASE).unwrap()),
        destination_cell: None,
        key_hold: None,
        journal_hold: None,
        objects_sha256: None,
        funded_inventory_sha256: None,
        source_cut: SourceCut {
            lsn: 2,
            position: 1,
            system_time: 1,
        },
    };
    authority
        .record_recovery_installation(DATABASE, operation, installed)
        .await
        .unwrap();
    assert_eq!(
        authority
            .publish_recovery(DATABASE, operation)
            .await
            .unwrap_err()
            .code,
        "rtdb/activation-funded-installation"
    );
    let replacement = Operation {
        predecessor,
        operation_id: Uuid::new_v4(),
    };
    let status = authority
        .begin_recovery(
            DATABASE,
            Intent {
                operation: replacement,
                destination_cell: None,
                loss_policy: LossPolicy::Lossless,
            },
            Some(operation.operation_id),
        )
        .await
        .unwrap();
    let Status::Pending { candidate, .. } = status else {
        panic!("Expected a pending replacement")
    };
    authority
        .record_recovery_installation(
            DATABASE,
            replacement,
            VerifiedSuccessor {
                successor: candidate,
                funded_inventory_sha256,
                ..installed
            },
        )
        .await
        .unwrap();
    authority
        .publish_recovery(DATABASE, replacement)
        .await
        .unwrap();
    assert_eq!(
        authority
            .funded_outcome(DATABASE, predecessor, scope.scope_sha256, 1)
            .await
            .unwrap(),
        Some(verified)
    );
    control.shutdown().await.unwrap();
}
