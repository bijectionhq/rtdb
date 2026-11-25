use super::*;
use rtdb_types::rtdb_v1::ClaimSealer;

const OWNER: [u8; 32] = [2; 32];
const INTENT: [u8; 32] = [3; 32];
const BOOKING: [u8; 16] = [4; 16];
const READING: Timestamp = Timestamp(1_000_000);

fn absent(table: &TableRef, id: EntityId) -> Guard {
    Guard::Version {
        branch: MAIN_BRANCH.into(), table: table.clone(), id,
        valid_at: Some(Timestamp(0)), expect: Version::Absent,
    }
}

fn written(table: &TableRef, id: EntityId, basis: Basis) -> Guard {
    Guard::Version {
        branch: MAIN_BRANCH.into(), table: table.clone(), id,
        valid_at: Some(Timestamp(0)), expect: Version::Written(basis),
    }
}

fn put(table: &TableRef, id: EntityId) -> Op {
    Op::Put {
        branch: MAIN_BRANCH.into(), table: table.clone(), if_absent: true,
        rows: vec![Row {
            id, valid: Some(Interval::ALL.into()), cells: StructFields::from([("recorded_at".into(), Cell::TxTime)]),
            custody_group: None, protected_columns: vec![], custody_payload: None,
        }],
    }
}

fn sample(sequence: u64, instant: i64, decision_before: i64, journal_before: i64) -> Option<AppointmentSample> {
    Some(AppointmentSample {
        observation: DecisionObservation { domain: [7; 16], epoch: 1, sequence, instant: Timestamp(instant) },
        decision_before: Timestamp(decision_before), journal_before: Timestamp(journal_before), certificate: [8; 32],
    })
}

fn append(pipeline: &mut TransactPipeline, decision: Decision) -> (Receipt, Option<TimedJournalCutoff>) {
    let Decision::Append { position, record } = decision else { panic!("expected ordered append"); };
    let cutoff = TransactionRecord::funded_journal_cutoff(&record).unwrap();
    (pipeline.acknowledge(position).unwrap().pop().unwrap(), cutoff)
}

#[test]
fn appointment_orders_before_guest_and_binds_only_one_terminal() -> Result<()> {
    let incarnation = [1; 16];
    let claims = ClaimSealer::new(incarnation, [9; 32]);
    let state = TransactState::new(incarnation);
    let predecessor = state.index().head().basis;
    let mut pipeline = TransactPipeline::new(state, 1, 4)?;
    let receipts = TableRef::new("public", "receipts");
    let controls = TableRef::new("public", "controls");
    let receipt_id = EntityId::Integer(1);
    let seat_id = EntityId::Integer(2);
    let claim_id = EntityId::Integer(3);
    let prepare = Transact::prepare_funded_appointment(
        vec![1; 16], absent(&receipts, receipt_id.clone()), Timestamp(1_950_000),
    )?;
    let prepare_command = AppointmentCommand::Prepare { predecessor, booking: BOOKING, intent: INTENT };
    let decide = |pipeline: &mut TransactPipeline, request: &Transact, command, sample| {
        let input = FundingInput::Appointment { owner: OWNER, command, sample };
        pipeline.decide_funded(request, Timestamp(2_000_000), &[TRANSACT_REQUIRES], &claims, None, None, input, usize::MAX)
    };
    let prepared = decide(&mut pipeline, &prepare, prepare_command, sample(1, 100_000, 1_800_000, 1_900_000))?;
    let (prepared, proof) = append(&mut pipeline, prepared);
    assert!(proof.is_none());
    assert_eq!(prepared.decision_clock.unwrap().reading(), READING);
    assert_eq!(pipeline.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");

    let mut foreign = prepare.clone();
    foreign.key = vec![5; 16];
    assert!(decide(&mut pipeline, &foreign, prepare_command, sample(1, 100_000, 1_800_000, 1_900_000)).is_err());

    let mut activate = prepare.clone();
    activate.window = Window::default();
    activate.requires.push(absent(&controls, seat_id.clone()));
    activate.requires.push(absent(&controls, claim_id.clone()));
    activate.ops.extend([put(&controls, seat_id.clone()), put(&controls, claim_id.clone())]);
    let activate_command = AppointmentCommand::Activate { booking: BOOKING };
    let mut consume = prepare.clone();
    consume.window = Window::default();
    consume.ops.push(put(&receipts, receipt_id.clone()));
    let consume_command = AppointmentCommand::Consume { booking: BOOKING, intent: INTENT };
    assert!(decide(&mut pipeline, &consume, consume_command, sample(3, 1_200_000, 1_800_000, 1_900_000)).is_err());

    let activation = decide(&mut pipeline, &activate, activate_command, sample(2, 400_000, 800_000, 900_000))?;
    let (activated, proof) = append(&mut pipeline, activation);
    assert_eq!(proof.unwrap().before(), Timestamp(900_000));
    assert_eq!(activated.decision_clock.unwrap().booking(), BOOKING);
    assert_eq!(pipeline.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");
    consume.requires.extend([written(&controls, seat_id, activated.basis), written(&controls, claim_id, activated.basis)]);

    let expired = decide(&mut pipeline, &consume, consume_command, sample(3, 1_800_000, 1_800_000, 1_900_000));
    assert!(expired.is_err());
    assert_eq!(pipeline.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");
    let mut wrong_row = consume.clone();
    wrong_row.ops = vec![put(&receipts, EntityId::String("1".into()))];
    assert!(decide(&mut pipeline, &wrong_row, consume_command, sample(3, 1_200_000, 1_800_000, 1_900_000)).is_err());
    let accepted = decide(&mut pipeline, &consume, consume_command, sample(3, 1_200_000, 1_800_000, 1_900_000))?;
    assert_eq!(pipeline.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");
    let (terminal, proof) = append(&mut pipeline, accepted);
    assert_eq!(proof.unwrap(), TimedJournalCutoff::new(Timestamp(1_900_000), [8; 32])?);
    assert_eq!(terminal.decision_clock.unwrap().appointment_cut().unwrap().basis, activated.basis);
    let state = pipeline.durable();
    let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(0))?;
    let row = view.row(&receipts, &receipt_id, Timestamp(0)).unwrap();
    assert!(matches!(row.image.as_ref().unwrap().cells.get("recorded_at"),
        Some(Value::Timestamp { value, .. }) if *value == READING.0));

    let prior_cut = state.index().exact(activated.basis)?;
    let prior = TransactState::decode_checkpoint(&state.encode_checkpoint_at(activated.basis)?, prior_cut)?;
    assert_eq!(prior.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");
    let state = TransactState::decode_checkpoint(&state.encode_checkpoint()?, state.index().head())?;
    let mut recovered = TransactPipeline::new(state, 4, 4)?;
    for (request, command, expected) in [
        (&prepare, prepare_command, prepared),
        (&activate, activate_command, activated),
        (&consume, consume_command, terminal),
    ] {
        let Decision::Answer(recovered_receipt) = decide(&mut recovered, request, command, None)?
            else { panic!("expected original receipt"); };
        assert_eq!(recovered_receipt, replay(&expected));
    }
    consume.window.before = Some(Timestamp(1_600_000));
    assert!(decide(&mut recovered, &consume, consume_command, None).is_err());
    Ok(())
}

#[test]
fn an_ordered_exclusion_blocks_a_later_consume_without_binding_a_terminal() -> Result<()> {
    let incarnation = [1; 16];
    let claims = ClaimSealer::new(incarnation, [9; 32]);
    let state = TransactState::new(incarnation);
    let predecessor = state.index().head().basis;
    let mut pipeline = TransactPipeline::new(state, 1, 4)?;
    let table = TableRef::new("public", "receipts");
    let receipt_id = EntityId::Integer(1);
    let seat_id = EntityId::Integer(2);
    let claim_id = EntityId::Integer(3);
    let prepare = Transact::prepare_funded_appointment(
        vec![1; 16], absent(&table, receipt_id.clone()), Timestamp(1_950_000),
    )?;
    let decide = |pipeline: &mut TransactPipeline, request: &Transact, command, sample| {
        pipeline.decide_funded(request, Timestamp(2_000_000), &[TRANSACT_REQUIRES], &claims, None, None,
            FundingInput::Appointment { owner: OWNER, command, sample }, usize::MAX)
    };
    let ordered = decide(&mut pipeline, &prepare, AppointmentCommand::Prepare { predecessor, booking: BOOKING, intent: INTENT },
        sample(1, 100_000, 1_800_000, 1_900_000))?;
    append(&mut pipeline, ordered);
    let mut activate = prepare.clone();
    activate.window = Window::default();
    activate.requires.push(absent(&table, seat_id.clone()));
    activate.requires.push(absent(&table, claim_id.clone()));
    activate.ops.extend([put(&table, seat_id.clone()), put(&table, claim_id.clone())]);
    let ordered = decide(&mut pipeline, &activate, AppointmentCommand::Activate { booking: BOOKING },
        sample(2, 400_000, 800_000, 900_000))?;
    let (activated, _) = append(&mut pipeline, ordered);

    let mut exclusion = prepare.clone();
    exclusion.key = vec![6; 16];
    exclusion.require.clear();
    exclusion.window = Window::default();
    exclusion.ops.push(put(&table, receipt_id.clone()));
    let ordered = pipeline.decide(&exclusion, Timestamp(2_000_000), &[TRANSACT_REQUIRES], &claims)?;
    append(&mut pipeline, ordered);
    let mut consume = prepare.clone();
    consume.window = Window::default();
    consume.ops.push(put(&table, receipt_id));
    consume.requires.extend([written(&table, seat_id, activated.basis), written(&table, claim_id, activated.basis)]);
    assert!(decide(&mut pipeline, &consume, AppointmentCommand::Consume { booking: BOOKING, intent: INTENT },
        sample(3, 1_200_000, 1_800_000, 1_900_000)).is_err());
    assert_eq!(pipeline.tx_status(&prepare.key).unwrap_err().code, "rtdb/transaction-pending");
    Ok(())
}
