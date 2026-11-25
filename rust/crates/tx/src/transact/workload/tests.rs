use super::*;
use rtdb_types::rtdb_v1::ClaimSealer;

const INCARNATION: [u8; 16] = [7; 16];
const PLACEMENT: [u8; 32] = [1; 32];
const CLAIM: [u8; 32] = [2; 32];
const WRITE: [u8; 32] = [3; 32];
const CLOSE: [u8; 32] = [4; 32];
const ABSENT: [u8; 32] = [5; 32];
const FEATURES: &[&str] = &[TIMED_TRANSACT, TRANSACT_REQUIRES];

struct Writer {
    pipeline: TransactPipeline,
    point: Point,
    now: Timestamp,
}

impl Writer {
    fn new() -> Self {
        Self {
            pipeline: TransactPipeline::new(TransactState::new(INCARNATION), 1, 1).unwrap(),
            point: Point::new(
                "public.installation_authority",
                "authority".into(),
                Timestamp(0),
            )
            .unwrap(),
            now: Timestamp(10),
        }
    }

    fn restart(&mut self) {
        let state = self.pipeline.durable();
        let cut = state.index().head();
        let checkpoint = state.encode_checkpoint().unwrap();
        let restored = TransactState::decode_checkpoint(&checkpoint, cut).unwrap();
        self.pipeline = TransactPipeline::new(restored, cut.basis.position() + 1, 1).unwrap();
    }

    fn turn(&mut self, namespace: [u8; 32], operation: Operation) -> Result<Reply> {
        let request = Request {
            point: self.point.clone(),
            namespace,
            expires_at: Timestamp(500),
            operation,
        };
        let (transaction, admission) = match request.prepare(self.pipeline.durable(), self.now)? {
            Prepared::Answer(answer) => return Ok(answer),
            Prepared::Append {
                transaction,
                admission,
            } => (transaction, admission),
        };
        let claims = ClaimSealer::new(INCARNATION, [0; 32]);
        let decision =
            self.pipeline
                .decide_workload(&transaction, self.now, FEATURES, &claims, &admission)?;
        let Decision::Append { position, .. } = decision else {
            panic!("expected append")
        };
        let mut receipts = self.pipeline.acknowledge(position)?;
        if admission.cutoff().is_some() {
            assert!(receipts.is_empty());
            request.check_effect(self.pipeline.durable(), self.now)?;
            let terminal = self.pipeline.terminal(position, self.now)?;
            receipts = self.pipeline.acknowledge(terminal.position)?;
        }
        self.now = self.now.next()?;
        assert_eq!(receipts.len(), 1);
        Ok(Reply::Receipt(receipts.pop().unwrap()))
    }
}

fn placement(epoch: u64, assignment: u8) -> Placement {
    Placement {
        epoch,
        assignment: Some(Holder {
            assignment_id: [assignment; 16],
            holder: format!("holder-{assignment}"),
        }),
    }
}

#[test]
fn imported_placement_accepts_only_the_exact_initial_fence_or_a_later_epoch() {
    let fence = |epoch, position, incarnation, marker| Fence {
        placement: placement(epoch, marker),
        applied: Applied {
            namespace: [marker; 32],
            key: vec![marker; 16],
            basis: Basis::new(incarnation, position).to_bytes(),
            system_time_micros: i64::try_from(position).unwrap(),
        },
    };
    let imported = fence(2, 10, INCARNATION, 1);
    let later = fence(3, 13, INCARNATION, 2);
    assert!(imported.at_or_after_import(&imported));
    assert!(later.at_or_after_import(&imported));
    assert!(!fence(1, 9, INCARNATION, 3).at_or_after_import(&imported));
    assert!(!fence(2, 10, INCARNATION, 3).at_or_after_import(&imported));
    assert!(!fence(2, 11, INCARNATION, 3).at_or_after_import(&imported));
    assert!(!fence(3, 13, [4; 16], 3).at_or_after_import(&imported));

    let predecessor = Predecessor {
        fence: later.clone(),
        writer: later.applied.clone(),
        close: later.applied.clone(),
        closure: [2; 16],
    };
    let advance = Authority::AdvancePlacement {
        installation_intent: [3; 16],
        previous: Previous::Closed { predecessor },
        next: placement(4, 3),
    };
    assert!(advance.permits_imported_workload(&imported));
    let empty = Authority::AdvancePlacement {
        installation_intent: [3; 16],
        previous: Previous::Empty,
        next: placement(4, 3),
    };
    assert!(!empty.permits_imported_workload(&imported));
}

fn applied(reply: Reply, namespace: [u8; 32]) -> Applied {
    let Reply::Receipt(receipt) = reply else {
        panic!("expected receipt")
    };
    assert!(matches!(receipt.outcome, Outcome::Then));
    Applied {
        namespace,
        key: receipt.key,
        basis: receipt.basis.to_bytes(),
        system_time_micros: receipt.system_time.0,
    }
}

fn submit(authority: Authority, marker: u8, transaction: Option<Transact>) -> Operation {
    Operation::Submit {
        key: vec![marker; 16],
        authority,
        transaction,
        digest: [marker; 32],
    }
}

fn begin(writer: &mut Writer) -> (Fence, Applied) {
    let first = placement(1, 11);
    let reply = writer
        .turn(
            PLACEMENT,
            submit(
                Authority::AdvancePlacement {
                    installation_intent: [6; 16],
                    previous: Previous::Empty,
                    next: first.clone(),
                },
                10,
                None,
            ),
        )
        .unwrap();
    let fence = Fence {
        placement: first,
        applied: applied(reply, PLACEMENT),
    };
    let claim = writer
        .turn(
            CLAIM,
            submit(
                Authority::ClaimWriter {
                    fence: fence.clone(),
                },
                11,
                None,
            ),
        )
        .unwrap();
    (fence, applied(claim, CLAIM))
}

#[test]
fn funded_appointment_checks_the_live_namespaced_claim_and_exact_receipt_point() -> Result<()> {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let receipt = Point::new("public.receipts", "receipt".into(), Timestamp(0))?;
    let scope = FundedAppointmentScope {
        fence: fence.clone(), writer: writer.clone(), key: [6; 32], receipt: receipt.clone(),
        intent_sha256: [7; 32], funding_policy_sha256: [8; 32],
    };
    let context = ManagedAppointment::new(journal.point.clone(), scope.clone(), [9; 32], Timestamp(100))?;
    let state = journal.pipeline.durable();
    context.check_current(state, journal.now)?;
    assert!(context.check_current(state, Timestamp(100)).is_err());

    let mut other = scope;
    other.writer.namespace = WRITE;
    let wrong_claim = ManagedAppointment::new(journal.point.clone(), other, [10; 32], Timestamp(100))?;
    assert!(wrong_claim.check_current(state, journal.now).is_err());
    let absent = |id| Guard::Version {
        branch: MAIN_BRANCH.into(), table: receipt.table.clone(), id,
        valid_at: Some(receipt.valid_at), expect: Version::Absent,
    };
    let command = AppointmentCommand::Prepare { predecessor: state.index().head().basis, booking: [11; 16], intent: [7; 32] };
    let prepare = Transact::prepare_funded_appointment(vec![6; 32], absent(receipt.id.clone()), Timestamp(90))?;
    context.check_request(state, &prepare, command)?;
    let wrong_receipt = Transact::prepare_funded_appointment(vec![6; 32], absent(EntityId::Binary(b"receipt".to_vec())), Timestamp(90))?;
    assert!(context.check_request(state, &wrong_receipt, command).is_err());

    journal.turn(CLOSE, submit(Authority::CloseWriter { fence, writer, closure: [12; 16] }, 12, None))?;
    assert!(context.check_current(journal.pipeline.durable(), journal.now).is_err());
    Ok(())
}

#[test]
fn imported_settlement_preserves_receipts_at_the_import_floor() {
    let mut journal = Writer::new();
    let (_, claim) = begin(&mut journal);
    let after = Basis::from_bytes(&claim.basis).unwrap();
    let existing = Operation::ImportedSettle {
        key: claim.key,
        family: Family::ClaimWriter,
        digest: [11; 32],
        after,
    };
    assert_eq!(
        journal.turn(CLAIM, existing).err().unwrap().code,
        "rtdb/unknown-transaction"
    );

    let settle = || Operation::ImportedSettle {
        key: vec![21; 16],
        family: Family::Write,
        digest: [21; 32],
        after,
    };
    assert!(matches!(
        journal.turn(WRITE, settle()).unwrap(),
        Reply::Receipt(receipt) if receipt.basis > after && matches!(receipt.outcome, Outcome::Refused { .. })
    ));
    journal.restart();
    assert!(matches!(
        journal.turn(WRITE, settle()).unwrap(),
        Reply::Receipt(receipt) if receipt.replayed && receipt.basis > after
    ));
}

fn write(marker: u8, table: TableRef, id: EntityId) -> Transact {
    let mut transaction = empty_transaction(vec![marker; 16]);
    transaction.ops.push(Op::Put {
        branch: MAIN_BRANCH.into(),
        table,
        if_absent: false,
        rows: vec![Row {
            id,
            valid: None,
            cells: StructFields::from_iter([("value".into(), text_cell("data".into()))]),
            custody_group: None,
            custody_payload: None,
            protected_columns: Vec::new(),
        }],
    });
    transaction
}

#[test]
fn commissioned_fork_co_orders_the_registry_and_owner_under_the_current_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let writer_basis = Basis::from_bytes(&writer.basis).unwrap();
    let line = LineProvision {
        branch: "authored".into(),
        registry: TableRef::new("public", "line_registry"),
        generation: [13; 16],
        owner: Point::new("public.admissions", "owner".into(), Timestamp(0)).unwrap(),
    };
    let key = vec![12; 16];
    let expires = Timestamp(300);
    let authority = Authority::ProvisionBranch {
        fence: fence.clone(), writer: writer.clone(), line: line.clone(), request_digest: [12; 32],
    };
    let mut request = write(12, line.owner.table.clone(), line.owner.id.clone());
    let Op::Put { rows, .. } = &mut request.ops[0] else { panic!("expected owner put") };
    rows[0].valid = Some(Interval::ALL.into());
    request.window.before = Some(Timestamp(200));
    request.requires.push(Guard::Version {
        branch: MAIN_BRANCH.into(), table: journal.point.table.clone(), id: journal.point.id.clone(),
        valid_at: Some(journal.point.valid_at), expect: Version::Written(writer_basis),
    });
    request.ops.push(Op::Fork {
        child: line.branch.clone(), from: (MAIN_BRANCH.into(), writer_basis),
        writable: true, expires_at: Some(expires),
    });
    let row = Row {
        id: EntityId::String(line.branch.clone()), valid: Some(Interval::ALL.into()),
        cells: StructFields::from_iter([
            ("created_basis".into(), Cell::TxBasis),
            ("creation_key".into(), Cell::Value(Value::Binary(key.clone()))),
            ("line_generation".into(), binary_cell(line.generation)),
            ("proposal_owner".into(), text_cell("owner".into())),
            ("expires".into(), Cell::Value(Value::I64(expires.0))),
            ("writable".into(), Cell::Value(Value::Bool(true))),
        ]),
        custody_group: None, protected_columns: Vec::new(), custody_payload: None,
    };
    request.ops.push(Op::Put {
        branch: MAIN_BRANCH.into(), table: line.registry.clone(), rows: vec![row], if_absent: true,
    });

    let ordinary = Authority::Write { fence: fence.clone(), writer: writer.clone() };
    assert!(journal.turn(WRITE, submit(ordinary, 12, Some(request.clone()))).is_err());
    let mut mismatched = request.clone();
    let Op::Put { rows, .. } = &mut mismatched.ops[2] else { panic!("expected registry put") };
    rows[0].cells.insert("creation_key".into(), binary_cell([9; 16]));
    assert!(journal.turn(WRITE, submit(authority.clone(), 12, Some(mismatched))).is_err());
    let mut no_owner = request.clone();
    no_owner.ops.remove(0);
    assert!(journal.turn(WRITE, submit(authority.clone(), 12, Some(no_owner))).is_err());

    let receipt = applied(journal.turn(WRITE, submit(authority, 12, Some(request))).unwrap(), WRITE);
    let created = Basis::from_bytes(&receipt.basis).unwrap();
    let state = journal.pipeline.durable();
    assert!(fence.covers_imported_family_line(&fence, &writer, created));
    assert!(!fence.covers_imported_family_line(&fence, &writer, writer_basis));
    assert_eq!(state.original_line_creation_receipt(&key, created).unwrap().basis, created);
    assert!(state.original_custody_receipt(&key, created).is_err());
    assert!(state.index().lineage(created).iter().any(|branch| branch.branch == line.branch && branch.created == created));
    let view = state.index().view(MAIN_BRANCH, At::Exactly(created), Layer::Local, Timestamp(receipt.system_time_micros)).unwrap();
    let registry = view.row(&line.registry, &EntityId::String(line.branch), Timestamp(0)).unwrap();
    assert_eq!(registry.written_basis(), Some(created));
    assert_eq!(registry.image_at(Timestamp(0)).unwrap().cells.get("created_basis"), Some(&Value::Binary(receipt.basis.to_vec())));
    assert_eq!(view.row(&line.owner.table, &line.owner.id, line.owner.valid_at).unwrap().written_basis(), Some(created));
}

#[test]
fn original_family_claim_scope_commits_to_the_session_grant_and_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let mut authority = Authority::OriginalFamilyWrite {
        fence, writer, branch: "authored".into(), created: Basis::new(INCARNATION, 9),
        group: [4; 16], session: [5; 32], request_digest: [6; 32],
    };
    let namespace = [7; 32];
    let binding = authority.original_family_claim_scope(namespace).unwrap().binding;
    assert!(authority.assignment_binding().unwrap().is_none());
    assert!(authority.original_family_claim_scope([0; 32]).is_err());
    assert_ne!(authority.original_family_claim_scope([8; 32]).unwrap().binding, binding);
    let Authority::OriginalFamilyWrite { session, .. } = &mut authority else { unreachable!() };
    *session = [9; 32];
    assert_ne!(authority.original_family_claim_scope(namespace).unwrap().binding, binding);
    let changed_session = authority.original_family_claim_scope(namespace).unwrap().binding;
    let Authority::OriginalFamilyWrite { writer, .. } = &mut authority else { unreachable!() };
    writer.key = vec![10; 16];
    assert_ne!(authority.original_family_claim_scope(namespace).unwrap().binding, changed_session);
}

#[test]
fn exact_close_and_successor_fence_outlive_checkpoint_and_reject_the_former_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let auth = Authority::Write {
        fence: fence.clone(),
        writer: writer.clone(),
    };
    let data = |key| write(key, TableRef::new("public", "facts"), EntityId::Integer(1));
    let reply = journal
        .turn(WRITE, submit(auth.clone(), 12, Some(data(12))))
        .unwrap();
    let written = applied(reply, WRITE);
    let recovered = journal
        .turn(
            WRITE,
            Operation::Settle {
                key: written.key.clone(),
                family: Family::Write,
                digest: [12; 32],
            },
        )
        .unwrap();
    assert!(
        matches!(recovered, Reply::Receipt(receipt) if receipt.replayed && matches!(receipt.outcome, Outcome::Then))
    );
    let close = journal
        .turn(
            CLOSE,
            submit(
                Authority::CloseWriter {
                    fence: fence.clone(),
                    writer: writer.clone(),
                    closure: [13; 16],
                },
                13,
                None,
            ),
        )
        .unwrap();
    let predecessor = Predecessor {
        fence,
        writer,
        close: applied(close, CLOSE),
        closure: [13; 16],
    };
    journal.restart();

    let mut inexact = predecessor.clone();
    inexact.writer.system_time_micros += 1;
    assert_eq!(
        journal
            .turn(ABSENT, Operation::Predecessor(inexact))
            .err()
            .unwrap()
            .code,
        "rtdb/managed-workload-fence"
    );
    let evidence = journal
        .turn(ABSENT, Operation::Predecessor(predecessor.clone()))
        .unwrap();
    assert!(
        matches!(evidence, Reply::Predecessor(proof) if proof.close.applied == predecessor.close)
    );
    journal
        .turn(
            PLACEMENT,
            submit(
                Authority::AdvancePlacement {
                    installation_intent: [6; 16],
                    previous: Previous::Closed { predecessor },
                    next: placement(3, 22),
                },
                14,
                None,
            ),
        )
        .unwrap();

    assert_eq!(
        journal
            .turn(WRITE, submit(auth.clone(), 15, Some(data(15))))
            .err()
            .unwrap()
            .code,
        "rtdb/managed-workload-fence"
    );
    let replay = journal
        .turn(WRITE, submit(auth, 12, Some(data(12))))
        .unwrap();
    assert!(
        matches!(replay, Reply::Receipt(receipt) if receipt.replayed && receipt.basis.to_bytes() == written.basis)
    );
    for (namespace, family) in [(ABSENT, Family::Write), (WRITE, Family::ClaimWriter)] {
        assert_eq!(
            journal
                .turn(
                    namespace,
                    Operation::Status {
                        key: written.key.clone(),
                        family
                    }
                )
                .err()
                .unwrap()
                .code,
            "rtdb/unknown-transaction"
        );
    }
    assert!(
        matches!(journal.turn(WRITE, Operation::Status { key: written.key, family: Family::Write }).unwrap(),
        Reply::Receipt(receipt) if receipt.replayed)
    );
}

#[test]
fn both_arms_are_checked_and_settlement_prevents_the_same_attempt_after_restart() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let authority = Authority::Write { fence, writer };
    let mut tx = write(20, TableRef::new("public", "facts"), EntityId::Integer(1));
    tx.otherwise = write(20, journal.point.table.clone(), journal.point.id.clone()).ops;
    assert_eq!(
        journal
            .turn(WRITE, submit(authority.clone(), 20, Some(tx)))
            .err()
            .unwrap()
            .code,
        "rtdb/managed-workload-authority"
    );
    let key = vec![21; 16];
    let settle = || Operation::Settle {
        key: key.clone(),
        family: Family::Write,
        digest: [21; 32],
    };
    assert!(
        matches!(journal.turn(WRITE, settle()).unwrap(), Reply::Receipt(receipt)
        if matches!(&receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/managed-workload-attempt-settled"))
    );
    journal.restart();

    let tx = write(21, TableRef::new("public", "facts"), EntityId::Integer(1));
    let replay = journal
        .turn(WRITE, submit(authority.clone(), 21, Some(tx.clone())))
        .unwrap();
    assert!(
        matches!(replay, Reply::Receipt(receipt) if receipt.replayed && matches!(receipt.outcome, Outcome::Refused { .. }))
    );
    let changed = Operation::Submit {
        key,
        authority,
        transaction: Some(tx),
        digest: [22; 32],
    };
    assert_eq!(
        journal.turn(WRITE, changed).err().unwrap().code,
        "rtdb/idempotency-key"
    );
}

#[test]
fn original_acknowledgement_at_the_grant_cutoff_refuses_the_timed_effect() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let transaction = write(20, TableRef::new("public", "facts"), EntityId::Integer(1));
    let request = Request {
        point: journal.point.clone(),
        namespace: WRITE,
        expires_at: Timestamp(100),
        operation: submit(Authority::Write { fence, writer }, 20, Some(transaction)),
    };
    let Prepared::Append {
        transaction,
        admission,
    } = request
        .prepare(journal.pipeline.durable(), journal.now)
        .unwrap()
    else {
        panic!("expected pending write")
    };
    let decision = journal
        .pipeline
        .decide_workload(
            &transaction,
            journal.now,
            FEATURES,
            &ClaimSealer::new(INCARNATION, [0; 32]),
            &admission,
        )
        .unwrap();
    let Decision::Append { position, record } = decision else {
        panic!("expected append")
    };
    let TransactionRecord::Tx(record) = TransactionRecord::decode(&record).unwrap() else {
        panic!("expected transaction record")
    };
    let fingerprint = transaction.logical_sha256().unwrap();
    assert_eq!(record.admission(), (transaction.key.as_slice(), journal.now, fingerprint));
    assert_eq!(record.workload_digest, Some([20; 32]));
    assert_eq!(record.canonical_request().unwrap().get(), transaction.canonical_request().unwrap().get());
    assert!(journal.pipeline.acknowledge(position).unwrap().is_empty());
    assert!(
        request
            .check_effect(journal.pipeline.durable(), Timestamp(100))
            .is_err()
    );
    let terminal = journal.pipeline.terminal(position, Timestamp(100)).unwrap();
    let receipts = journal.pipeline.acknowledge(terminal.position).unwrap();
    assert!(matches!(receipts[0].outcome, Outcome::Refused { .. }));
    let view = journal
        .pipeline
        .durable()
        .index()
        .view(MAIN_BRANCH, At::Latest, Layer::Local, Timestamp(100))
        .unwrap();
    assert!(
        view.row(
            &TableRef::new("public", "facts"),
            &EntityId::Integer(1),
            Timestamp(100)
        )
        .is_none()
    );
    journal.restart();
    let stored = journal.pipeline.durable().workload_receipt(WRITE, &transaction.key).unwrap();
    assert_eq!(stored.fingerprint, fingerprint);
    assert!(stored.matches_workload_digest([20; 32]));
    assert!(!stored.matches_workload_digest([21; 32]));
    assert!(matches!(request.prepare(journal.pipeline.durable(), journal.now).unwrap(),
        Prepared::Answer(Reply::Receipt(receipt)) if receipt.replayed && matches!(receipt.outcome, Outcome::Refused { .. })));
}

#[test]
fn original_valid_point_alone_does_not_authorize_a_displaced_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let claims = ClaimSealer::new(INCARNATION, [0; 32]);
    let mut overlay = write(15, journal.point.table.clone(), journal.point.id.clone());
    let Op::Put { rows, .. } = &mut overlay.ops[0] else {
        panic!("expected put")
    };
    rows[0].valid = Some(
        ValidTime::new(
            ValidTimeBound::Instant(Timestamp(20)),
            ValidTimeBound::Instant(Timestamp::MAX),
        )
        .unwrap(),
    );
    let decision = journal
        .pipeline
        .decide(&overlay, journal.now, FEATURES, &claims)
        .unwrap();
    let Decision::Append { position, .. } = decision else {
        panic!("expected append")
    };
    journal.pipeline.acknowledge(position).unwrap();
    journal.now = Timestamp(30);

    let state = journal.pipeline.durable();
    let view = state
        .index()
        .view(MAIN_BRANCH, At::Latest, Layer::Local, journal.now)
        .unwrap();
    let old = view
        .row(
            &journal.point.table,
            &journal.point.id,
            journal.point.valid_at,
        )
        .unwrap();
    assert_eq!(old.cut.basis.to_bytes(), writer.basis);
    let transaction = write(16, TableRef::new("public", "facts"), EntityId::Integer(1));
    let request = submit(Authority::Write { fence, writer }, 16, Some(transaction));
    assert_eq!(
        journal.turn(WRITE, request).err().unwrap().code,
        "rtdb/managed-workload-fence"
    );
}

#[test]
fn empty_closure_orders_vacancy_and_prevents_a_delayed_claim() {
    let mut journal = Writer::new();
    let abandoned = placement(1, 11);
    let initial = journal
        .turn(
            PLACEMENT,
            submit(
                Authority::AdvancePlacement {
                    installation_intent: [6; 16],
                    previous: Previous::Empty,
                    next: abandoned.clone(),
                },
                10,
                None,
            ),
        )
        .unwrap();
    let fence = Fence {
        placement: abandoned,
        applied: applied(initial, PLACEMENT),
    };
    let closure = Authority::CloseEmptyAssignment {
        fence: fence.clone(),
        closure: [13; 16],
        vacant_epoch: 2,
    };
    let applied_close = applied(
        journal.turn(CLOSE, submit(closure, 13, None)).unwrap(),
        CLOSE,
    );
    journal.restart();

    let delayed = submit(Authority::ClaimWriter { fence }, 11, None);
    assert_eq!(
        journal.turn(CLAIM, delayed).err().unwrap().code,
        "rtdb/managed-workload-fence"
    );
    let vacancy = Fence {
        placement: Placement {
            epoch: 2,
            assignment: None,
        },
        applied: applied_close,
    };
    assert!(
        matches!(journal.turn(ABSENT, Operation::Observe(vacancy.clone())).unwrap(),
        Reply::Placement(evidence) if evidence.placement == vacancy.placement)
    );
    let successor = submit(
        Authority::AdvancePlacement {
            installation_intent: [6; 16],
            previous: Previous::Vacant { fence: vacancy },
            next: placement(3, 22),
        },
        14,
        None,
    );
    applied(journal.turn(PLACEMENT, successor).unwrap(), PLACEMENT);
}

#[test]
fn guarded_close_checks_cold_prerequisites_with_the_original_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let check = |key: u8, expect| {
        let mut tx = empty_transaction(vec![key; 16]);
        tx.requires.push(Guard::Version {
            branch: MAIN_BRANCH.into(),
            table: TableRef::new("public", "installation_authority"),
            id: EntityId::String("authority".into()),
            valid_at: Some(Timestamp(0)),
            expect,
        });
        tx
    };
    let grant = |key| Authority::CloseWriterGuarded {
        fence: fence.clone(),
        writer: writer.clone(),
        closure: [key; 16],
        request_digest: [key; 32],
    };
    let rejected = submit(grant(12), 12, Some(check(12, Version::Absent)));
    let Reply::Receipt(refused) = journal.turn(CLOSE, rejected).unwrap() else {
        panic!("expected receipt")
    };
    assert!(
        matches!(refused.outcome, Outcome::Refused { error } if error.code == REQUIREMENT_FAILED)
    );
    let closed = submit(grant(13), 13, Some(check(13, Version::Present)));
    let close = applied(journal.turn(CLOSE, closed).unwrap(), CLOSE);
    let predecessor = Predecessor {
        fence,
        writer,
        close,
        closure: [13; 16],
    };
    assert!(matches!(
        journal
            .turn(ABSENT, Operation::Predecessor(predecessor))
            .unwrap(),
        Reply::Predecessor(_)
    ));
}

#[test]
fn custody_metadata_checks_the_named_groups_and_current_writer() {
    let mut journal = Writer::new();
    let (fence, writer) = begin(&mut journal);
    let group = [21; 16];
    let mut transaction = empty_transaction(vec![22; 16]);
    transaction.ops.push(Op::CreateCustodyGroup { group });
    let decision = journal
        .pipeline
        .decide(
            &transaction,
            journal.now,
            &[ROW_CUSTODY],
            &ClaimSealer::new(INCARNATION, [0; 32]),
        )
        .unwrap();
    let Decision::Append { position, .. } = decision else {
        panic!("expected append")
    };
    journal.pipeline.acknowledge(position).unwrap();
    journal.now = journal.now.next().unwrap();

    let read = |groups| Operation::ReadCustody {
        fence: fence.clone(),
        writer: writer.clone(),
        groups,
    };
    assert_eq!(
        journal
            .turn(ABSENT, read(vec![group, group]))
            .err()
            .unwrap()
            .code,
        "rtdb/managed-workload-authority"
    );
    assert_eq!(
        journal
            .turn(ABSENT, read(vec![[20; 16]]))
            .err()
            .unwrap()
            .code,
        "rtdb/custody-group-unknown"
    );
    assert!(
        matches!(journal.turn(ABSENT, read(vec![group])).unwrap(), Reply::ReadCut(cut)
        if cut.basis.position() == position)
    );
    let close = Authority::CloseWriter {
        fence: fence.clone(),
        writer: writer.clone(),
        closure: [23; 16],
    };
    applied(journal.turn(CLOSE, submit(close, 23, None)).unwrap(), CLOSE);
    assert_eq!(
        journal.turn(ABSENT, read(vec![group])).err().unwrap().code,
        "rtdb/managed-workload-fence"
    );
}
