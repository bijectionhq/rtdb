use std::sync::Mutex;

use bytes::Bytes;
use ordered_logkeeper::ordered::{
    AppendClass, Custody, Hold, Limits, Refusal, State, Window,
    replication::{Ballot, RotationSlots, SlotImage, SlotPair, VoterSlots},
    service::wire::Signature,
};
use rtdb_storage::IoRegion;

use super::{decision::OPERATIONAL_TARGET_MS, *};
use crate::keys::LocalKeyService;

const RETRY_POSITIONS: u64 = 64;
const CUSTODY_ROSTER: [DrainParticipant; 3] = [
    DrainParticipant {
        kind: CustodyWork::Plaintext,
        identity: [1; 32],
    },
    DrainParticipant {
        kind: CustodyWork::Egress,
        identity: [2; 32],
    },
    DrainParticipant {
        kind: CustodyWork::Copy,
        identity: [3; 32],
    },
];

struct Journal {
    state: State,
    preparing: Option<CheckpointPreparation>,
    lose_rotation_reply: bool,
    append_before_switch: bool,
}

struct CustodyFixture {
    journal: Mutex<Journal>,
    attempt: Mutex<Option<ErasureProgress>>,
    keys: LocalKeyService,
    stale_slot: Option<usize>,
    duplicate_slot: bool,
    lose_commit_reply: bool,
    revoke_early: bool,
    use_boundary: bool,
}

struct Fixture {
    custody: CustodyFixture,
    erasure: Erasure,
    view: Hold,
    io: std::sync::Arc<IoRegion>,
    _directory: tempfile::TempDir,
}

impl Journal {
    fn apply(&mut self, principal: Principal, command: Command) -> Receipt {
        apply(&mut self.state, principal, command)
    }

    fn append(&mut self, grant: Grant, payload: Bytes) -> std::result::Result<Outcome, Refusal> {
        let expected_next = self.state.head().next_lsn;
        self.apply(
            Principal::Worker(grant.enrollment),
            Command::Append {
                class: AppendClass::Data,
                grant,
                expected_next,
                timestamp: 1,
                payload,
                references: None,
            },
        )
        .outcome
    }
}

impl Fixture {
    async fn new() -> Result<Self> {
        let placement = Placement {
            deployment: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            group: Uuid::new_v4(),
            generation: 1,
        };
        let genesis = Checkpoint {
            cut: Cut {
                next_lsn: 0,
                history: [1; 32],
            },
            root: [0; 32],
            manifest: [0; 32],
            custody: Custody {
                allocation: Uuid::new_v4(),
                revision: 1,
                provider: [0; 32],
            },
        };
        let mut state = State::new(
            placement,
            Limits {
                enrolled_workers: 1,
                replay_holds: 4,
                retry_positions: RETRY_POSITIONS,
                retained_data_bytes: 32_768,
                maintenance_bytes: 4_096,
                serving_lease: 30_000,
                clock_drift: 1_000,
            },
            genesis,
            Window {
                allocation: Uuid::new_v4(),
                serial: 1,
                bytes: 28_672,
            },
        )
        .unwrap();
        let Ok(Outcome::Enrolled(enrollment)) = apply(
            &mut state,
            Principal::Administrator,
            Command::Enroll {
                worker: Uuid::new_v4(),
                expected: None,
            },
        )
        .outcome
        else {
            panic!("enrollment failed");
        };
        let worker = Principal::Worker(enrollment);
        let expected = state.writer_state();
        let Ok(Outcome::Granted {
            grant: incumbent, ..
        }) = apply(
            &mut state,
            worker,
            Command::Grant {
                expected,
                candidate: enrollment,
            },
        )
        .outcome
        else {
            panic!("grant failed");
        };
        let mut journal = Journal {
            state,
            preparing: None,
            lose_rotation_reply: false,
            append_before_switch: false,
        };
        let directory = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(directory.path())?;
        let io = IoRegion::new();
        let old = JournalKey::new(placement, incumbent.key_generation);
        keys.provision_journal_key(old).await?;
        for plaintext in [
            b"erased history".as_slice(),
            b"chosen native boundary",
            b"chosen native upload",
        ] {
            let encrypted = keys
                .seal_journal_payload(old, &io, Bytes::from_static(plaintext))
                .await?;
            assert!(matches!(
                journal.append(incumbent, encrypted),
                Ok(Outcome::Appended(_))
            ));
        }
        let Ok(Outcome::Barrier { read: view, .. }) =
            journal.apply(worker, Command::Barrier).outcome
        else {
            panic!("view admission failed");
        };
        Ok(Self {
            custody: CustodyFixture {
                journal: Mutex::new(journal),
                attempt: Mutex::new(None),
                keys,
                stale_slot: None,
                duplicate_slot: false,
                lose_commit_reply: false,
                revoke_early: false,
                use_boundary: false,
            },
            erasure: Erasure {
                placement,
                operation: Uuid::new_v4(),
                custody_domain: Uuid::new_v4(),
                incumbent,
                timing: ErasureTiming::OperationalTarget,
            },
            view,
            io,
            _directory: directory,
        })
    }

    async fn commit(&self) -> Result<ErasureCommit> {
        coordinate(
            &self.custody,
            &self.custody.keys,
            &self.custody,
            self.erasure,
            None,
        )
        .await
    }

    async fn open_held_view(&self) -> Result<Bytes> {
        let row = {
            let journal = self.custody.journal.lock().unwrap();
            let worker = Principal::Worker(self.view.owner);
            journal
                .state
                .held_replay(worker, self.view)
                .map_err(refused)?
                .records()
                .next()
                .unwrap()
                .clone()
        };
        let key = JournalKey::new(self.erasure.placement, row.grant.key_generation);
        Ok(key.open(&self.custody.keys, &self.io, row.payload).await?)
    }
}

#[async_trait]
impl ErasureJournal for CustodyFixture {
    async fn selection(&self) -> Result<Selection> {
        let journal = self.journal.lock().unwrap();
        Ok(Selection {
            grant: journal.state.writer_state().grant,
            head: journal.state.head(),
            checkpoint: journal.state.checkpoint(),
            preparing: journal.preparing,
        })
    }

    async fn reopen(&self, _: PreparedErasure) -> Result<()> {
        let mut journal = self.journal.lock().unwrap();
        if std::mem::take(&mut journal.append_before_switch) {
            let incumbent = journal.preparing.unwrap().source;
            assert_eq!(
                journal.append(incumbent, Bytes::from_static(b"old suffix")),
                Err(Refusal::Unavailable)
            );
        }
        Ok(())
    }

    async fn execute(&self, request: &Request) -> Result<Decision> {
        let mut journal = self.journal.lock().unwrap();
        let position = journal.state.position() + 1;
        let time = journal.state.time();
        let receipt = journal
            .state
            .apply(position, time, request.clone())
            .unwrap();
        let rotation = match receipt.outcome {
            Ok(Outcome::Granted { grant, recovery }) => {
                journal.preparing = None;
                // This fixture supplies structural evidence. The installed journal
                // verifies the transport issuer before returning the actual decision.
                let mut images = [
                    SlotImage {
                        generation: 2,
                        digest: [2; 32],
                    },
                    SlotImage {
                        generation: 3,
                        digest: [3; 32],
                    },
                ];
                if let Some(index) = self.stale_slot {
                    images[index].digest = [0; 32];
                }
                if self.duplicate_slot {
                    images[1] = images[0];
                }
                Some(SignedRotationSlots {
                    evidence: RotationSlots {
                        group: [1; 32],
                        cohort_size: 3,
                        ballot: Ballot {
                            round: 1,
                            nonce: Uuid::from_u128(1),
                        },
                        prefix: [6; 32],
                        position,
                        checkpoint: recovery.checkpoint,
                        key_generation: grant.key_generation,
                        voters: [1, 2]
                            .map(|id| VoterSlots {
                                voter: Uuid::from_u128(id),
                                slots: SlotPair {
                                    selected: 1,
                                    images,
                                },
                            })
                            .into(),
                    },
                    signature: Signature {
                        issuer: [5; 32],
                        proof: [7; 64],
                    },
                })
            }
            _ => None,
        };
        if std::mem::take(&mut journal.lose_rotation_reply) {
            return Err(lost_reply());
        }
        Ok(Decision {
            receipt,
            barrier: None,
            rotation,
        })
    }
}

#[async_trait]
impl ErasureCustody for CustodyFixture {
    async fn prepare(&self, erasure: Erasure) -> Result<ErasureProgress> {
        let mut attempt = self.attempt.lock().unwrap();
        if let Some(progress) = *attempt {
            return Ok(progress);
        }
        let mut journal = self.journal.lock().unwrap();
        let base = journal.state.checkpoint();
        let head = journal.state.head();
        let cut = if self.use_boundary {
            journal.state.record(head.next_lsn - 2).unwrap().result.cut
        } else {
            head
        };
        let checkpoint = Checkpoint {
            cut,
            root: [2; 32],
            manifest: [3; 32],
            custody: Custody {
                allocation: Uuid::new_v4(),
                revision: 2,
                provider: [4; 32],
            },
        };
        let rotation = RequestId::fresh(journal.state.position(), RETRY_POSITIONS).unwrap();
        let preparation = CheckpointPreparation {
            erasure: true,
            checkpoint: cut,
            root: checkpoint.root,
            source: erasure.incumbent,
            base,
            custody: checkpoint.custody,
            usable_bytes: 64,
            descriptor: binding(erasure, rotation).descriptor()?,
        };
        if !self.use_boundary {
            assert_eq!(
                journal
                    .apply(
                        Principal::Custodian,
                        Command::PrepareCheckpoint {
                            preparation,
                            references: None
                        }
                    )
                    .outcome,
                Ok(Outcome::CheckpointPrepared(preparation))
            );
            journal.preparing = Some(preparation);
        }
        let prepared = PreparedErasure {
            erasure,
            checkpoint,
            rotation,
        };
        *attempt = Some(ErasureProgress::Prepared(prepared));
        Ok(ErasureProgress::Prepared(prepared))
    }

    async fn commit_and_arm(&self, ready: CleanRecoverySlots) -> Result<ErasureCommit> {
        let prepared = ready.prepared();
        let old_key = JournalKey::new(
            prepared.erasure.placement,
            prepared.erasure.incumbent.key_generation,
        );
        let now = rtdb_directory::now_ms();
        let due = now + OPERATIONAL_TARGET_MS as i64;
        if self.revoke_early {
            self.keys.withdraw_archive_key(&old_key.id()).await?;
        } else {
            assert!(!self.keys.schedule_archive_key(&old_key.id(), due).await?);
        }
        let commit = ErasureCommit {
            prepared,
            committed_at_ms: now as u64,
            deadline: ErasureDeadline::Target { at_ms: due as u64 },
            retiring_inventory: [10; 32],
            draining_inventory: drain_inventory(CUSTODY_ROSTER)?,
            switch_evidence: ready.binding(),
        };
        *self.attempt.lock().unwrap() = Some(ErasureProgress::Committed(commit));
        if self.lose_commit_reply {
            return Err(lost_reply());
        }
        Ok(commit)
    }
}

fn lost_reply() -> Anomaly {
    Anomaly::fault(
        "test/lost-reply",
        "Simulated loss after applying the decision",
    )
}

fn apply(state: &mut State, principal: Principal, command: Command) -> Receipt {
    let request = Request {
        placement: state.placement(),
        principal,
        id: RequestId::fresh(state.position(), RETRY_POSITIONS).unwrap(),
        command,
    };
    state
        .apply(state.position() + 1, state.time(), request)
        .unwrap()
}

#[tokio::test]
async fn exact_head_rotation_survives_lost_rotation_and_commit_replies() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture
        .custody
        .journal
        .get_mut()
        .unwrap()
        .lose_rotation_reply = true;
    fixture.custody.lose_commit_reply = true;
    assert_eq!(fixture.commit().await.unwrap_err().code, "test/lost-reply");
    assert_eq!(fixture.commit().await.unwrap_err().code, "test/lost-reply");
    let commit = fixture.commit().await?;
    assert_eq!(fixture.commit().await?, commit);
    let journal = fixture.custody.journal.lock().unwrap();
    assert_eq!(journal.state.head().next_lsn, 3);
    assert_eq!(journal.state.head(), commit.prepared.checkpoint.cut);
    assert_eq!(journal.state.checkpoint(), commit.prepared.checkpoint);
    assert_eq!(
        journal.state.writer_state().grant.unwrap().key_generation,
        2
    );
    Ok(())
}

#[tokio::test]
async fn an_ordinary_boundary_cut_is_refused_and_preparation_fences_appends() -> Result<()> {
    let mut boundary = Fixture::new().await?;
    boundary.custody.use_boundary = true;
    let failure = boundary.commit().await.unwrap_err();
    assert!(
        failure
            .message
            .as_deref()
            .is_some_and(|message| message.contains("outside the quiesced journal head"))
    );
    let journal = boundary.custody.journal.get_mut().unwrap();
    assert_eq!(journal.state.checkpoint().cut.next_lsn, 0);
    assert_eq!(
        journal.state.writer_state().grant,
        Some(boundary.erasure.incumbent)
    );

    let mut racing = Fixture::new().await?;
    racing
        .custody
        .journal
        .get_mut()
        .unwrap()
        .append_before_switch = true;
    let commit = racing.commit().await?;
    let journal = racing.custody.journal.get_mut().unwrap();
    assert_eq!(journal.state.checkpoint(), commit.prepared.checkpoint);
    assert_eq!(journal.state.head(), commit.prepared.checkpoint.cut);
    assert_eq!(journal.state.head().next_lsn, 3);
    assert_eq!(
        journal.state.writer_state().grant,
        Some(Grant {
            epoch: racing.erasure.incumbent.epoch + 1,
            key_generation: racing.erasure.incumbent.key_generation + 1,
            ..racing.erasure.incumbent
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_different_rotation_identity_cannot_adopt_the_ordered_survivor() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let ErasureProgress::Prepared(mut prepared) = fixture.custody.prepare(fixture.erasure).await?
    else {
        panic!("expected a new erasure attempt");
    };
    prepared.rotation.nonce = Uuid::new_v4();
    *fixture.custody.attempt.get_mut().unwrap() = Some(ErasureProgress::Prepared(prepared));
    assert_eq!(
        fixture.commit().await.unwrap_err().code,
        "rtdb.multi/erasure-custody"
    );
    let journal = fixture.custody.journal.get_mut().unwrap();
    assert_eq!(
        journal.state.writer_state().grant,
        Some(fixture.erasure.incumbent)
    );
    Ok(())
}

#[tokio::test]
async fn either_old_selectable_slot_blocks_commit_and_leaves_the_view_readable() -> Result<()> {
    for slot in 0..2 {
        let mut fixture = Fixture::new().await?;
        fixture.custody.stale_slot = Some(slot);
        let failure = fixture.commit().await.unwrap_err();
        assert!(
            failure
                .message
                .as_deref()
                .is_some_and(|message| message.contains("Both recovery slots"))
        );
        assert_eq!(
            fixture.open_held_view().await?,
            Bytes::from_static(b"erased history")
        );
        assert!(matches!(
            fixture.custody.attempt.get_mut().unwrap(),
            Some(ErasureProgress::Prepared(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn clean_slot_gate_rejects_a_short_quorum_and_stale_generation() -> Result<()> {
    let fixture = Fixture::new().await?;
    let ErasureProgress::Prepared(prepared) = fixture.custody.prepare(fixture.erasure).await? else {
        panic!("expected a new erasure attempt");
    };
    let request = Request {
        id: prepared.rotation,
        placement: prepared.erasure.placement,
        principal: Principal::Custodian,
        command: Command::PublishAndRotate {
            grant: prepared.erasure.incumbent,
            checkpoint: prepared.checkpoint,
        },
    };
    let decision = fixture.custody.execute(&request).await?;
    let receipt = decision.receipt;
    let evidence = decision.rotation.unwrap().evidence;
    assert!(evidence.covers(receipt));
    assert!(covers_clean_rotation(&evidence, receipt));

    let mut short_quorum = evidence.clone();
    short_quorum.voters.pop();
    assert!(!short_quorum.covers(receipt));
    assert!(!covers_clean_rotation(&short_quorum, receipt));

    let mut stale_generation = evidence;
    stale_generation.key_generation -= 1;
    assert!(!stale_generation.covers(receipt));
    assert!(!covers_clean_rotation(&stale_generation, receipt));
    Ok(())
}

#[tokio::test]
async fn one_slot_attested_twice_cannot_authorize_commit() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.custody.duplicate_slot = true;
    assert_eq!(
        fixture.commit().await.unwrap_err().code,
        "rtdb.multi/erasure-custody"
    );
    assert_eq!(
        fixture.open_held_view().await?,
        Bytes::from_static(b"erased history")
    );
    Ok(())
}

#[tokio::test]
async fn old_generation_appends_are_fenced_and_the_successor_suffix_opens() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let commit = fixture.commit().await?;
    let journal = fixture.custody.journal.get_mut().unwrap();
    let new = journal.state.writer_state().grant.unwrap();
    let old = fixture.erasure.incumbent;
    let plaintext = Bytes::from_static(b"surviving new-generation data");
    let keys = &fixture.custody.keys;
    let old_key = JournalKey::new(fixture.erasure.placement, old.key_generation);
    let new_key = JournalKey::new(fixture.erasure.placement, new.key_generation);
    let old_frame = keys
        .seal_journal_payload(old_key, &fixture.io, plaintext.clone())
        .await?;
    assert_eq!(journal.append(old, old_frame.clone()), Err(Refusal::Stale));
    let forged = Grant {
        key_generation: old.key_generation,
        ..new
    };
    assert_eq!(journal.append(forged, old_frame), Err(Refusal::Stale));
    let frame = keys
        .seal_journal_payload(new_key, &fixture.io, plaintext.clone())
        .await?;
    assert!(matches!(
        journal.append(new, frame),
        Ok(Outcome::Appended(_))
    ));
    let row = journal
        .state
        .record(commit.prepared.checkpoint.cut.next_lsn)
        .unwrap();
    assert_eq!(row.grant, new);
    assert_eq!(
        new_key.open(keys, &fixture.io, row.payload.clone()).await?,
        plaintext
    );
    Ok(())
}

#[tokio::test]
async fn arming_preserves_the_held_view_and_early_withdrawal_breaks_it() -> Result<()> {
    let fixture = Fixture::new().await?;
    let commit = fixture.commit().await?;
    assert!((rtdb_directory::now_ms() as u64) < commit.deadline.at_ms());
    assert_eq!(
        fixture.open_held_view().await?,
        Bytes::from_static(b"erased history")
    );
    let old = JournalKey::new(
        fixture.erasure.placement,
        fixture.erasure.incumbent.key_generation,
    );
    let premature = fixture
        .custody
        .keys
        .withdraw_archive_key(&old.id())
        .await
        .unwrap_err();
    assert_eq!(premature.category, rtdb_types::AnomalyCategory::Unavailable);

    let mut weakened = Fixture::new().await?;
    weakened.custody.revoke_early = true;
    let commit = weakened.commit().await?;
    assert!((rtdb_directory::now_ms() as u64) < commit.deadline.at_ms());
    assert_eq!(
        weakened.open_held_view().await.unwrap_err().code,
        "rtdb/erased-under-view"
    );
    Ok(())
}

#[tokio::test]
async fn custody_evidence_binds_the_inventory_switch_and_destruction_order() -> Result<()> {
    use ring::signature::{Ed25519KeyPair, KeyPair};

    let mut fixture = Fixture::new().await?;
    let signer = Ed25519KeyPair::from_seed_unchecked(&[4; 32]).unwrap();
    let authority = CustodyEvidence::new(signer.public_key().as_ref().try_into().unwrap())?;
    let sign = |bytes: &[u8]| signer.sign(bytes).as_ref().try_into().unwrap();
    let operational = fixture.commit().await?;
    let mut commit = operational;
    let committed = commit.committed_at_ms;
    let by_ms = committed + 10;
    commit.prepared.erasure.timing = ErasureTiming::Fixed { before_ms: by_ms };
    let mut reservation = DeadlineReservation {
        by_ms,
        retiring_inventory: commit.retiring_inventory,
        draining_inventory: commit.draining_inventory,
        signature: [0; 64],
    };
    reservation.signature = sign(&reservation.signing_bytes(commit.prepared));
    authority.verify_reservation(commit.prepared, reservation)?;
    assert!(
        authority
            .verify_reservation(
                commit.prepared,
                DeadlineReservation {
                    retiring_inventory: [9; 32],
                    ..reservation
                }
            )
            .is_err()
    );

    commit.deadline = ErasureDeadline::Enforced {
        by_ms,
        signature: [0; 64],
    };
    commit.deadline = ErasureDeadline::Enforced {
        by_ms,
        signature: sign(&commit.signing_bytes()),
    };
    validate_commit(
        commit.prepared,
        commit,
        Some(commit.switch_evidence),
        Some(&authority),
    )?;
    let other_switch = ErasureCommit {
        switch_evidence: [9; 32],
        ..commit
    };
    assert!(validate_commit(other_switch.prepared, other_switch, None, Some(&authority)).is_err());

    let drain = CustodyDrain {
        closed_at_ms: committed + 1,
        joined: CUSTODY_ROSTER
            .into_iter()
            .map(|participant| DrainedWork {
                participant,
                observed_at_ms: committed + 2,
                observation: [16; 32],
            })
            .collect(),
        signature: [0; 64],
    };
    let mut destruction = DestructionEvidence {
        drain,
        destroyed_at_ms: by_ms + 1,
        disposition: [12; 32],
        journal_key_disposition: [22; 32],
        checkpoint_disposition: [9; 32],
        serving_disposition: [10; 32],
        signature: [0; 64],
    };
    let sign_drain = |receipt: &mut DestructionEvidence| {
        receipt.drain.signature = sign(&receipt.drain.signing_bytes(&commit));
        receipt.signature = sign(&receipt.signing_bytes(&commit));
    };
    sign_drain(&mut destruction);
    authority.verify_destruction(commit, &destruction)?;
    assert_eq!(destruction.fixed_deadline_met(&commit), Some(false));
    assert_eq!(destruction.fixed_deadline_met(&operational), None);
    let mut on_deadline = destruction.clone();
    on_deadline.destroyed_at_ms = by_ms;
    sign_drain(&mut on_deadline);
    authority.verify_destruction(commit, &on_deadline)?;
    assert_eq!(on_deadline.fixed_deadline_met(&commit), Some(true));
    assert!(
        authority
            .verify_destruction(other_switch, &destruction)
            .is_err()
    );
    let mut incomplete = destruction.clone();
    incomplete.drain.joined.pop();
    sign_drain(&mut incomplete);
    assert!(authority.verify_destruction(commit, &incomplete).is_err());

    destruction.drain.joined[0].observed_at_ms = destruction.destroyed_at_ms + 1;
    sign_drain(&mut destruction);
    assert!(authority.verify_destruction(commit, &destruction).is_err());

    let journal = fixture.custody.journal.get_mut().unwrap();
    let head = journal.state.head();
    fixture.erasure.timing = commit.prepared.erasure.timing;
    assert_eq!(
        fixture.commit().await.unwrap_err().code,
        "rtdb.multi/fixed-erasure-authority"
    );
    assert_eq!(
        fixture.custody.journal.get_mut().unwrap().state.head(),
        head
    );
    Ok(())
}

#[tokio::test]
async fn drain_journal_reopens_exact_joined_observations() -> Result<()> {
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use std::sync::Arc;

    let fixture = Fixture::new().await?;
    let mut commit = fixture.commit().await?;
    let worker = Ed25519KeyPair::from_seed_unchecked(&[5; 32]).unwrap();
    let other_worker = Ed25519KeyPair::from_seed_unchecked(&[6; 32]).unwrap();
    let verification_key = worker.public_key().as_ref().try_into().unwrap();
    let roster = [
        DrainRegistration { kind: CustodyWork::Plaintext, duty: [8; 32], verification_key },
        DrainRegistration { kind: CustodyWork::Copy, duty: [9; 32], verification_key },
    ];
    let participants = roster.map(|duty| duty.participant().unwrap());
    commit.draining_inventory = drain_inventory(participants)?;
    let closed = commit.committed_at_ms + 1;
    let volume = tempfile::tempdir().unwrap();
    let vault = tempfile::tempdir().unwrap();
    let custody = || {
        CheckpointCustody::new(
            Arc::new(rtdb_storage::MemoryStore::default()),
            rtdb_storage::ObjectKey::new("protected").unwrap(),
            fixture.erasure.placement,
            rtdb_types::StorageEpoch(0),
            commit.prepared.checkpoint.custody.provider,
            Arc::new(LocalKeyService::new(vault.path()).unwrap()),
            IoRegion::new(),
        ).on_local_volume(volume.path())
    };
    let provider = custody()?;
    let journal = ErasureDrainJournal::close(&provider, commit, closed, roster.to_vec()).await?;
    let work = participants.map(|participant| DrainedWork {
        participant,
        observed_at_ms: closed + 1,
        observation: [10; 32],
    });
    let signed = |work, signer: &Ed25519KeyPair| SignedDrainJoin {
        work,
        signature: signer.sign(&SignedDrainJoin::signing_bytes(&commit, closed, work)).as_ref().to_vec(),
    };
    assert!(journal.record(&signed(work[0], &other_worker)).await.is_err());
    journal.record(&signed(work[0], &worker)).await?;
    assert!(journal.joined().await?.is_none());
    drop(journal);
    drop(provider);

    let reopened = custody()?;
    let journal = ErasureDrainJournal::reopen(&reopened, commit).await?.unwrap();
    assert!(journal.joined().await?.is_none());
    journal.record(&signed(work[1], &worker)).await?;
    journal.record(&signed(work[1], &worker)).await?;
    assert_eq!(journal.joined().await?, Some(work.to_vec()));
    let other = DrainedWork { observation: [11; 32], ..work[1] };
    assert!(journal.record(&signed(other, &worker)).await.is_err());
    Ok(())
}
