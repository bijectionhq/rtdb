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
    let Decision::Append { position, .. } = decision else {
        panic!("expected append")
    };
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
