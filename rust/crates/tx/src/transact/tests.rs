use super::*;
use rtdb_types::rtdb_v1::{ClaimEvidence, ClaimFootprint, ClaimRegion, ClaimScope, ClaimSealer};

const INCARNATION: [u8; 16] = [1; 16];
const DEFAULT: &[&str] = &[LINEARIZABLE, TRANSACT_REQUIRES];
const MULTI: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES];

fn claims() -> ClaimSealer { ClaimSealer::new(INCARNATION, [2; 32]) }
fn table() -> TableRef { TableRef::new("public", "docs") }
fn basis(position: u64) -> Basis { Basis::new(INCARNATION, position) }

fn request(key: u8, ops: Vec<Op>) -> Transact {
    Transact { key: vec![key; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), ops, otherwise: Vec::new(), requires: Vec::new(), custody_request_digest: None, request_memory: None }
}

fn put(branch: &str, id: i64, key: &str) -> Op {
    let row = Row { id: EntityId::Integer(id), valid: None, cells: StructFields::from([("key".into(), Cell::Value(Value::Utf8(key.into())))]), custody_group: None, protected_columns: Vec::new(), custody_payload: None };
    Op::Put { branch: branch.into(), table: table(), rows: vec![row], if_absent: false }
}

fn fork(child: &str, position: u64) -> Op {
    Op::Fork { child: child.into(), from: (MAIN_BRANCH.into(), basis(position)), writable: true, expires_at: None }
}

fn plan(state: &TransactState, request: &Transact, at: u64, now: i64, features: &[&str]) -> PreparedAppend {
    let Resolution::Append(prepared) = state.resolve(request, at, Timestamp(now), features, &claims()).unwrap() else {
        panic!("expected append");
    };
    assert_eq!(prepared.position(), at);
    prepared
}

fn apply(state: &mut TransactState, at: u64, record: &TransactionRecord) -> Option<Receipt> {
    state.apply(at, TransactionRecord::decode(&record.encode().unwrap()).unwrap()).unwrap()
}

fn commit(state: &mut TransactState, at: u64, request: &Transact) -> Receipt {
    let prepared = plan(state, request, at, at as i64 * 10, DEFAULT);
    apply(state, at, prepared.record()).unwrap()
}

fn visible(state: &TransactState, branch: &str, layer: Layer, at: i64) -> Vec<i64> {
    let view = state.index().view(branch, At::Latest, layer, Timestamp(at)).unwrap();
    view.ids(&table()).into_iter().filter_map(|id| match id {
        EntityId::Integer(n) if view.row(&table(), id, Timestamp(at)).is_some() => Some(*n),
        _ => None,
    }).collect()
}

#[test]
fn put_and_delete_resolve_relative_intervals_at_the_receipt_time() {
    let mut state = TransactState::new(INCARNATION);
    let interval = |from, to| Some(ValidTime::new(
        ValidTimeBound::TransactionMicros(from), ValidTimeBound::TransactionMicros(to),
    ).unwrap());
    let mut put = put(MAIN_BRANCH, 1, "relative");
    let Op::Put { rows, .. } = &mut put else { unreachable!() };
    rows[0].valid = interval(-5, 5);
    let first = commit(&mut state, 1, &request(1, vec![put]));
    assert_eq!(first.system_time, Timestamp(10));
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 4).is_empty());
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 5), [1]);
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 14), [1]);
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 15).is_empty());
    let delete = Op::Delete {
        branch: MAIN_BRANCH.into(), table: table(), ids: vec![EntityId::Integer(1)], valid: interval(-10, 5),
    };
    assert_eq!(commit(&mut state, 2, &request(2, vec![delete])).system_time, Timestamp(20));
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 9), [1]);
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 10).is_empty());
}

#[test]
fn custody_revocation_masks_prior_and_inherited_members_after_checkpoint() {
    let group = [8; 16];
    let features = [LINEARIZABLE, ROW_CUSTODY];
    let protected = |branch, id| {
        let Op::Put { branch, table, mut rows, if_absent } = put(branch, id, "protected") else { unreachable!() };
        rows[0].custody_group = Some(group);
        Op::Put { branch, table, rows, if_absent }
    };
    let mut state = TransactState::new(INCARNATION);
    let mut initial_inventory = None;
    for (position, ops) in [
        (1, vec![Op::CreateCustodyGroup { group }, protected(MAIN_BRANCH, 1), put(MAIN_BRANCH, 4, "ordinary")]),
        (2, vec![fork("child", 1), protected("child", 2), protected(MAIN_BRANCH, 3)]),
    ] {
        let prepared = plan(&state, &request(position as u8, ops), position, position as i64 * 10, &features);
        assert_eq!(apply(&mut state, position, prepared.record()).unwrap().outcome, Outcome::Then);
        if position == 1 { initial_inventory = state.index().custody_inventory(group); }
    }
    assert_eq!(visible(&state, "child", Layer::Merged, 25), [1, 2, 4]);
    let stale = request(3, vec![Op::RevokeCustodyGroup { group, inventory_sha256: initial_inventory.unwrap().sha256 }]);
    let prepared = plan(&state, &stale, 3, 30, &features);
    let refusal = apply(&mut state, 3, prepared.record()).unwrap();
    assert!(matches!(refusal.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));
    assert!(state.index().custody_group_live(group).unwrap());
    let inventory = state.index().custody_inventory(group).unwrap();
    assert_eq!(inventory.placements, 3);
    let revoke = request(4, vec![Op::RevokeCustodyGroup { group, inventory_sha256: inventory.sha256 }]);
    let prepared = plan(&state, &revoke, 4, 40, &features);
    let first = apply(&mut state, 4, prepared.record()).unwrap();
    let notices = state.index().custody_revocation_writes(group, state.index().head()).unwrap();
    assert_eq!(notices.iter().filter(|write| write.branch == "child")
        .map(|write| (&write.id, write.erased_written_basis)).collect::<Vec<_>>(),
        [(&EntityId::Integer(2), Some(basis(2))), (&EntityId::Integer(1), Some(basis(1)))]);
    assert!(notices.iter().all(|write| write.valid.is_some()));
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    assert_eq!(state.index().custody_revocation(group), Some(basis(4)));
    for (branch, cut) in [(MAIN_BRANCH, basis(1)), ("child", basis(2))] {
        let view = state.index().view(branch, At::Exactly(cut), Layer::Merged, Timestamp(25)).unwrap();
        for member in 1..=3 {
            assert!(view.row(&table(), &EntityId::Integer(member), Timestamp(25)).is_none());
        }
        assert!(view.row(&table(), &EntityId::Integer(4), Timestamp(25)).is_some());
    }
    let Resolution::Answer(replay) = state.resolve(&revoke, 5, Timestamp(50), &features, &claims()).unwrap() else {
        panic!("revocation retry must return its original receipt");
    };
    assert_eq!(replay.basis, first.basis);
    assert!(replay.replayed);
    let late = request(5, vec![protected("child", 5)]);
    let prepared = plan(&state, &late, 5, 50, &features);
    let refused = apply(&mut state, 5, prepared.record()).unwrap();
    assert!(matches!(refused.outcome, Outcome::Refused { error } if error.code == "rtdb/erased-under-view"));
}

fn labelled_put(branch: &str, id: i64, label: u8, valid: Interval) -> Op {
    let Op::Put { branch, table, mut rows, if_absent } = put(branch, id, "value") else { unreachable!() };
    rows[0].valid = Some(valid.into());
    rows[0].cells.insert("label".into(), Cell::Value(Value::Binary(vec![label])));
    Op::Put { branch, table, rows, if_absent }
}

fn declare_visibility() -> Op {
    Op::Declare { table: table(), change: Declaration::Visibility(Visibility { column: "label".into() }) }
}

#[test]
fn projected_custody_keeps_issued_bases_and_masks_the_revoked_versions() {
    let public = [21; 16];
    let hidden = [22; 16];
    let scoped = LabelScope { read: BTreeSet::from([vec![1]]), write: Default::default() };
    let features = [LINEARIZABLE, ROW_CUSTODY];
    let mut state = TransactState::new(INCARNATION);
    let protected = |id, label, group| {
        let Op::Put { branch, table, mut rows, if_absent } =
            labelled_put(MAIN_BRANCH, id, label, Interval::ALL) else { unreachable!() };
        rows[0].custody_group = Some(group);
        Op::Put { branch, table, rows, if_absent }
    };
    let write = |state: &mut TransactState, position, ops| {
        let request = request(position as u8, ops);
        let prepared = plan(state, &request, position, position as i64 * 10, &features);
        assert_eq!(apply(state, position, prepared.record()).unwrap().outcome, Outcome::Then);
    };
    write(&mut state, 1, vec![
        declare_visibility(), Op::CreateCustodyGroup { group: public },
        Op::CreateCustodyGroup { group: hidden },
        protected(1, 1, public), protected(2, 2, hidden),
        labelled_put(MAIN_BRANCH, 3, 1, Interval::ALL),
    ]);
    let projected = state.index().project_main_visibility(&scoped).unwrap();
    let first = projected.index().head();
    assert_eq!(projected.source_basis(first.basis).unwrap(), basis(1));
    let hidden_inventory = state.index().custody_inventory(hidden).unwrap().sha256;
    write(&mut state, 2, vec![Op::RevokeCustodyGroup { group: hidden, inventory_sha256: hidden_inventory }]);
    let projected = state.index().project_main_visibility(&scoped).unwrap();
    assert_eq!(projected.index().head(), first);

    let public_inventory = state.index().custody_inventory(public).unwrap().sha256;
    write(&mut state, 3, vec![Op::RevokeCustodyGroup { group: public, inventory_sha256: public_inventory }]);
    let projected = state.index().project_main_visibility(&scoped).unwrap();
    assert_eq!(projected.source_basis(first.basis).unwrap(), basis(1));
    let old = projected.index().view(MAIN_BRANCH, At::Exactly(first.basis), Layer::Merged, Timestamp(0)).unwrap();
    assert!(old.row(&table(), &EntityId::Integer(1), Timestamp(0)).is_none());
    assert!(old.row(&table(), &EntityId::Integer(2), Timestamp(0)).is_none());
    assert!(old.row(&table(), &EntityId::Integer(3), Timestamp(0)).is_some());
    assert!(old.custody_inventories().next().is_none());
    assert_eq!(projected.changes().last().unwrap().rows[0].id, EntityId::Integer(1));
    assert!(projected.changes().last().unwrap().rows.iter().any(|row| {
        row.id == EntityId::Integer(1) && row.erased_written_basis == Some(first.basis)
    }));
}

#[test]
fn row_visibility_masks_inherited_and_older_versions_only_where_the_hidden_version_wins() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL)]));
    commit(&mut state, 2, &request(2, vec![fork("child", 1)]));
    let private_period = Interval::new(Timestamp(30), Timestamp(40)).unwrap();
    commit(&mut state, 3, &request(3, vec![labelled_put("child", 1, 2, private_period)]));
    let labels = BTreeSet::from([vec![1]]);
    let id = EntityId::Integer(1);
    let view = state.index().view("child", At::Latest, Layer::Merged, Timestamp(50)).unwrap();
    assert!(view.visible_row(&table(), &id, Timestamp(29), &labels).is_some());
    assert!(view.visible_row(&table(), &id, Timestamp(30), &labels).is_none());
    assert!(view.visible_row(&table(), &id, Timestamp(40), &labels).is_some());
    let historical = state.index().view("child", At::Exactly(basis(2)), Layer::Merged, Timestamp(50)).unwrap();
    assert!(historical.visible_row(&table(), &id, Timestamp(30), &labels).is_some());
}

#[test]
fn row_visibility_checks_incoming_and_affected_labels() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 2, Interval::ALL)]));
    let write = LabelScope { read: BTreeSet::from([vec![2]]), write: BTreeSet::from([vec![1]]) };
    let delete = |id| Op::Delete { branch: MAIN_BRANCH.into(), table: table(), ids: vec![EntityId::Integer(id)], valid: Some(Interval::ALL.into()) };
    let erase = Op::Erase { branch: None, table: table(), ids: vec![EntityId::Integer(1)] };
    for op in [labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL), labelled_put(MAIN_BRANCH, 2, 2, Interval::ALL),
        delete(1), delete(2), erase] {
        let mut candidate = Candidate::new(state.index().clone(), Some(&write));
        let error = candidate.execute(&[op]).unwrap_err();
        assert_eq!(error.code, "rtdb/write-scope");
        assert_eq!(error.category, AnomalyCategory::Forbidden);
        assert!(candidate.effects.is_empty());
    }
    let mut candidate = Candidate::new(state.index().clone(), Some(&write));
    candidate.execute(&[labelled_put(MAIN_BRANCH, 2, 1, Interval::ALL), delete(2)]).unwrap();
    assert_eq!(candidate.effects.len(), 2);
}

#[test]
fn row_visibility_declarations_and_single_label_are_ordered_and_recoverable() {
    let mut state = TransactState::new(INCARNATION);
    let single = Op::Declare { table: table(), change: Declaration::SingleLabelPerTx };
    commit(&mut state, 1, &request(1, vec![declare_visibility(), single]));
    let receipt = commit(&mut state, 2, &request(2, vec![put(MAIN_BRANCH, 1, "unlabelled")]));
    assert!(matches!(&receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/visibility-label"));
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    let mixed = request(3, vec![labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL), labelled_put(MAIN_BRANCH, 2, 2, Interval::ALL)]);
    let receipt = commit(&mut state, 3, &mixed);
    assert!(matches!(&receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/mixed-visibility-labels"));
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 30).is_empty());
    let same = request(4, vec![labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL), labelled_put(MAIN_BRANCH, 2, 1, Interval::ALL)]);
    assert_eq!(commit(&mut state, 4, &same).outcome, Outcome::Then);
}

fn evidence(position: u64, regions: impl IntoIterator<Item = ClaimRegion>) -> ClaimEvidence {
    ClaimEvidence {
        scope: ClaimScope { branch: MAIN_BRANCH.into(), basis: basis(position), layer: Layer::Merged },
        footprint: ClaimFootprint::from_regions(regions),
    }
}

fn claimed_row(id: i64) -> ClaimRegion {
    ClaimRegion::Row { table: table(), iid: EntityId::Integer(id).iid() }
}

#[test]
fn claim_guards_detect_writes_to_rows_and_both_sides_of_index_moves() {
    struct Verified(ClaimEvidence);
    impl ClaimVerifier for Verified {
        fn verify(&self, _: &Claim) -> Result<ClaimEvidence> { Ok(self.0.clone()) }
    }

    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![
        Op::Declare { table: table(), change: Declaration::Index(IndexDefinition { name: "by_key".into(), column: "key".into(), additional_columns: Vec::new() }) },
        put(MAIN_BRANCH, 1, "b"), put(MAIN_BRANCH, 2, "z"),
    ]));
    let row = Verified(evidence(1, [claimed_row(1)]));
    let range = Verified(evidence(1, [ClaimRegion::IndexRange {
        table: table(), index: "by_key".into(), lower: Some(b"a".to_vec()), upper: Some(b"c".to_vec()),
    }]));
    let mut validation = request(5, Vec::new());
    validation.guards.push(Guard::Unchanged(Claim::from_bytes(Vec::new())));
    assert_eq!(validation.claims().count(), 1);
    let outcome = |state: &TransactState, claim: &Verified| {
        let Resolution::Answer(receipt) = state.resolve(&validation, 7, Timestamp(70), DEFAULT, claim).unwrap() else {
            panic!("claim validation must answer at the applied cut");
        };
        receipt.outcome
    };

    commit(&mut state, 2, &request(2, vec![put(MAIN_BRANCH, 2, "x")]));
    assert_eq!(outcome(&state, &row), Outcome::Then);
    assert_eq!(outcome(&state, &range), Outcome::Then);

    let mut entering = TransactState::decode_checkpoint(&state.encode_checkpoint().unwrap(), state.index().head()).unwrap();
    commit(&mut entering, 3, &request(3, vec![put(MAIN_BRANCH, 2, "a")]));
    assert_eq!(outcome(&entering, &row), Outcome::Then);
    assert_eq!(outcome(&entering, &range), Outcome::Otherwise { failed: vec![0] });

    commit(&mut state, 3, &request(4, vec![put(MAIN_BRANCH, 1, "y")]));
    commit(&mut state, 4, &request(5, vec![put(MAIN_BRANCH, 1, "b")]));
    assert_eq!(outcome(&state, &row), Outcome::Otherwise { failed: vec![0] });
    assert_eq!(outcome(&state, &range), Outcome::Otherwise { failed: vec![0] });
}

#[test]
fn claim_footprint_coarsening_never_accepts_rejected_precise_regions() {
    let mut initial = TransactState::new(INCARNATION);
    commit(&mut initial, 1, &request(1, vec![put(MAIN_BRANCH, 0, "initial")]));
    let rows = (0..17).map(claimed_row).collect::<Vec<_>>();
    let table_claim = evidence(1, rows.iter().cloned());
    assert_eq!(table_claim.footprint.regions().unwrap().iter().collect::<Vec<_>>(), [&ClaimRegion::Table(table())]);
    assert_eq!(table_claim, evidence(1, rows.iter().rev().cloned()));

    let tables = std::iter::once(ClaimRegion::Table(table())).chain((0..32).map(|i| {
        ClaimRegion::Table(TableRef::new("public", format!("table_{i}")))
    })).collect::<Vec<_>>();
    let branch_claim = evidence(1, tables.iter().cloned());
    assert!(branch_claim.footprint.regions().is_none());
    assert_eq!(branch_claim, evidence(1, tables.iter().rev().cloned()));

    let checkpoint = initial.encode_checkpoint().unwrap();
    for id in -1..18 {
        let mut state = TransactState::decode_checkpoint(&checkpoint, initial.index().head()).unwrap();
        let ops = if id < 0 { vec![fork("preview", 1)] } else { vec![put(MAIN_BRANCH, id, "changed")] };
        commit(&mut state, 2, &request(2, ops));
        let unchanged = |claim: &ClaimEvidence| state.index().claim_unchanged(claim, Timestamp(21)).unwrap();
        assert!(!unchanged(&table_claim) || rows.iter().all(|part| unchanged(&evidence(1, [part.clone()]))));
        assert!(!unchanged(&branch_claim) || tables.iter().all(|part| unchanged(&evidence(1, [part.clone()]))));
    }
}

#[test]
fn timed_writes_publish_only_at_the_terminal_position() {
    let mut state = TransactState::new(INCARNATION);
    let mut timed = put(MAIN_BRANCH, 1, "a");
    if let Op::Put { rows, .. } = &mut timed {
        rows[0].cells.extend([("at".into(), Cell::TxTime), ("basis".into(), Cell::TxBasis)]);
    }
    let mut timed = request(1, vec![timed]);
    timed.window = Window { not_before: Some(Timestamp(10)), before: Some(Timestamp(20)) };
    assert!(matches!(state.resolve(&timed, 1, Timestamp(10), DEFAULT, &claims()),
        Err(error) if error.category == AnomalyCategory::Unsupported));
    assert!(matches!(state.resolve(&timed, 1, Timestamp(9), MULTI, &claims()),
        Err(error) if error.category == AnomalyCategory::Busy));

    let tx = plan(&state, &timed, 1, 10, MULTI);
    assert_eq!(apply(&mut state, 1, tx.record()), None);
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 10).is_empty());
    assert_eq!(state.tx_status(&timed.key).unwrap_err().code, "rtdb/transaction-pending");
    assert!(matches!(tx.after_original_ack(Timestamp(20), 2), Some(TransactionRecord::Refuse { tx_lsn: 1 })));

    let settle = tx.after_original_ack(Timestamp(19), 2).unwrap();
    assert!(matches!(settle, TransactionRecord::Settle { tx_lsn: 1 }));
    let receipt = apply(&mut state, 2, &settle).unwrap();
    assert_eq!(receipt.basis, basis(2));
    assert_eq!(receipt.system_time, Timestamp(10));
    assert!(state.index().cut(At::Exactly(basis(1))).is_err());
    let view = state.index().view(MAIN_BRANCH, At::Exactly(receipt.basis), Layer::Merged, Timestamp(19)).unwrap();
    let row = view.row(&table(), &EntityId::Integer(1), Timestamp(19)).unwrap();
    let cells = &row.image_at(Timestamp(19)).unwrap().cells;
    assert_eq!(cells["basis"], Value::Binary(receipt.basis.to_bytes().to_vec()));
    assert!(matches!(cells["at"], Value::Timestamp { value: 10, .. }));

    let mut follower = TransactState::new(INCARNATION);
    apply(&mut follower, 1, tx.record());
    assert_eq!(apply(&mut follower, 2, &settle), Some(receipt));
}

#[test]
fn replay_orders_refusal_when_a_timed_write_has_no_terminal_record() {
    let mut state = TransactState::new(INCARNATION);
    let mut timed = request(1, vec![put(MAIN_BRANCH, 1, "a")]);
    timed.window.before = Some(Timestamp(20));
    let tx = plan(&state, &timed, 1, 10, MULTI);
    apply(&mut state, 1, tx.record());
    let refusal = state.recover_pending().unwrap();
    let receipt = apply(&mut state, 3, &refusal).unwrap();
    assert_eq!(receipt.basis, basis(3));
    assert!(matches!(&receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/timed-transaction-refused"));

    let mut restarted = TransactState::new(INCARNATION);
    assert!(restarted.tx_status(&timed.key).is_err());
    apply(&mut restarted, 1, tx.record());
    apply(&mut restarted, 3, &refusal);
    assert!(visible(&restarted, MAIN_BRANCH, Layer::Merged, 30).is_empty());
    let Resolution::Answer(replay) = restarted.resolve(&timed, 4, Timestamp(99), MULTI, &claims()).unwrap() else {
        panic!("expected replay");
    };
    assert_eq!(replay, restarted.tx_status(&timed.key).unwrap());
    assert!(replay.replayed);
    assert_eq!(replay.basis, receipt.basis);
    timed.ops.clear();
    assert!(matches!(restarted.resolve(&timed, 4, Timestamp(99), MULTI, &claims()),
        Err(error) if error.code == "rtdb/idempotency-key"));
}

#[test]
fn otherwise_discards_the_candidate_and_validation_never_appends() {
    let mut state = TransactState::new(INCARNATION);
    let index = IndexDefinition { name: "by_key".into(), column: "key".into(), additional_columns: Vec::new() };
    commit(&mut state, 1, &request(1, vec![
        Op::Declare { table: table(), change: Declaration::Index(index) },
        put(MAIN_BRANCH, 1, "a"),
    ]));
    let limit = CountRange { table: table(), index: "by_key".into(), lower: Some(b"a".to_vec()), upper: Some(b"b".to_vec()), valid_at: None, n: 1 };
    let mut guarded = request(2, vec![put(MAIN_BRANCH, 2, "a")]);
    guarded.otherwise.push(put(MAIN_BRANCH, 3, "b"));
    guarded.guards.extend([
        Guard::Version { branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(1), valid_at: None, expect: Version::Written(basis(1)) },
        Guard::CountAtMost { branch: MAIN_BRANCH.into(), range: limit.clone(), when: When::Before },
        Guard::CountAtMost { branch: MAIN_BRANCH.into(), range: limit, when: When::After },
    ]);
    assert_eq!(commit(&mut state, 2, &guarded).outcome, Outcome::Otherwise { failed: vec![2] });
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 20), [1, 3]);

    let mut validation = request(3, Vec::new());
    validation.guards.push(Guard::Version { branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(2), valid_at: None, expect: Version::Present });
    assert!(matches!(state.resolve(&validation, 3, Timestamp(21), &[], &claims()),
        Err(error) if error.category == AnomalyCategory::Unsupported));
    let result = state.resolve(&validation, 3, Timestamp(21), DEFAULT, &claims()).unwrap();
    assert!(matches!(result, Resolution::Answer(receipt)
        if receipt.basis == basis(2) && receipt.outcome == (Outcome::Otherwise { failed: vec![0] })));
    assert!(state.tx_status(&validation.key).is_err());
}

#[test]
fn requires_count_validates_the_original_state_without_appending() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![
        Op::Declare { table: table(), change: Declaration::Index(IndexDefinition { name: "by_key".into(), column: "key".into() }) },
        put(MAIN_BRANCH, 1, "a"),
    ]));
    let range = CountRange { table: table(), index: "by_key".into(), lower: None, upper: None, valid_at: None, n: 0 };
    let mut validation = request(2, vec![]);
    for (when, code) in [(When::Before, REQUIREMENT_FAILED), (When::After, "rtdb/invalid-requirement")] {
        validation.requires = vec![Guard::CountAtMost { branch: MAIN_BRANCH.into(), range: range.clone(), when }];
        let Resolution::Answer(receipt) = state.resolve(&validation, 2, Timestamp(20), DEFAULT, &claims()).unwrap()
            else { panic!("expected validation receipt"); };
        assert_eq!(receipt.basis, basis(1));
        assert!(matches!(receipt.outcome, Outcome::Refused { error } if error.code == code));
        assert_eq!(state.tx_status(&validation.key).unwrap_err().code, "rtdb/unknown-transaction");
    }
}

#[test]
fn branch_tombstones_hold_the_fork_cut_and_merge_rejects_overlap() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![put(MAIN_BRANCH, 1, "a"), put(MAIN_BRANCH, 2, "b")]));
    commit(&mut state, 2, &request(2, vec![fork("child", 1)]));
    let claim = claims().seal(&ClaimScope { branch: "child".into(), basis: basis(2), layer: Layer::Merged }).unwrap();
    commit(&mut state, 3, &request(3, vec![put(MAIN_BRANCH, 3, "c")]));
    assert!(state.index().is_unchanged("child", basis(2), Layer::Merged, Timestamp(31)).unwrap());
    commit(&mut state, 4, &request(4, vec![
        put("child", 4, "d"),
        Op::Delete { branch: "child".into(), table: table(), ids: vec![EntityId::Integer(1)], valid: None },
    ]));
    assert_eq!(visible(&state, "child", Layer::Merged, 40), [2, 4]);
    assert_eq!(visible(&state, "child", Layer::Local, 40), [4]);
    assert_eq!(visible(&state, "child", Layer::Merged, 20), [1, 2]);

    let mut validation = request(5, Vec::new());
    validation.guards.push(Guard::Unchanged(claim));
    assert!(matches!(state.resolve(&validation, 5, Timestamp(50), DEFAULT, &claims()).unwrap(),
        Resolution::Answer(receipt) if receipt.outcome == (Outcome::Otherwise { failed: vec![0] })));
    assert_eq!(commit(&mut state, 5, &request(6, vec![Op::Merge { from: "child".into(), into: MAIN_BRANCH.into() }])).outcome, Outcome::Then);
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 50), [2, 3, 4]);

    commit(&mut state, 6, &request(7, vec![fork("competing", 5)]));
    commit(&mut state, 7, &request(8, vec![put("competing", 2, "draft")]));
    commit(&mut state, 8, &request(9, vec![put(MAIN_BRANCH, 2, "published")]));
    assert!(matches!(commit(&mut state, 9, &request(10, vec![Op::Merge { from: "competing".into(), into: MAIN_BRANCH.into() }])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/merge-conflict"));
}

#[test]
fn declarations_and_branch_lifetimes_are_ordered_with_writes() {
    let mut state = TransactState::new(INCARNATION);
    let index = IndexDefinition { name: "key".into(), column: "key".into(), additional_columns: Vec::new() };
    let range = CountRange { table: table(), index: "key".into(), lower: None, upper: None, valid_at: None, n: 1 };
    commit(&mut state, 1, &request(1, vec![
        Op::Declare { table: table(), change: Declaration::Ttl(Some(30)) },
        Op::Declare { table: table(), change: Declaration::Index(index) },
        Op::Declare { table: table(), change: Declaration::Constraint { name: "one".into(), range: Some(range) } },
        put(MAIN_BRANCH, 1, "a"),
    ]));
    assert!(matches!(commit(&mut state, 2, &request(2, vec![put(MAIN_BRANCH, 2, "b")])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/count-constraint"));
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 39), [1]);
    assert!(visible(&state, MAIN_BRANCH, Layer::Merged, 40).is_empty());

    let branch = "retained".to_owned();
    commit(&mut state, 3, &request(3, vec![Op::Fork {
        child: branch.clone(), from: (MAIN_BRANCH.into(), basis(1)), writable: false, expires_at: Some(Timestamp(45)),
    }]));
    commit(&mut state, 4, &request(4, vec![Op::Renew { branch: branch.clone(), expires_at: Timestamp(75) }]));
    assert!(state.index().view(&branch, At::Latest, Layer::Merged, Timestamp(50)).is_ok());
    commit(&mut state, 5, &request(5, vec![Op::DropBranch { branch: branch.clone() }]));
    assert!(state.index().view(&branch, At::Exactly(basis(4)), Layer::Merged, Timestamp(50)).is_err());
    assert_eq!(state.index().lineage(basis(5)).into_iter().find(|row| row.branch == branch).unwrap().dropped, Some(basis(5)));

    commit(&mut state, 6, &request(6, vec![Op::Declare { table: table(), change: Declaration::Drop }]));
    commit(&mut state, 7, &request(7, vec![
        Op::Declare { table: table(), change: Declaration::Schema(StructFields::new()) },
        put(MAIN_BRANCH, 3, "c"),
    ]));
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 70), [3]);
}

#[test]
fn checkpoint_retains_receipts_ancestry_and_mask_without_erased_values() {
    let mut state = TransactState::new(INCARNATION);
    let write = request(1, vec![put(MAIN_BRANCH, 1, "secret"), put(MAIN_BRANCH, 2, "keep")]);
    commit(&mut state, 1, &write);
    commit(&mut state, 2, &request(2, vec![fork("child", 1)]));
    commit(&mut state, 3, &request(3, vec![Op::Erase {
        branch: None, table: table(), ids: vec![EntityId::Integer(1)],
    }]));
    let encoded = state.encode_checkpoint().unwrap();
    assert!(!std::str::from_utf8(&encoded).unwrap().contains("secret"));
    let restored = TransactState::decode_checkpoint(&encoded, state.index().head()).unwrap();
    assert_eq!(visible(&restored, "child", Layer::Merged, 30), [2]);
    assert_eq!(restored.tx_status(&write.key), state.tx_status(&write.key));
    assert!(restored.index().view("child", At::Exactly(basis(2)), Layer::Merged, Timestamp(30)).is_err());
}

#[tokio::test]
async fn sql_aggregate_and_lineage_share_the_selected_branch_snapshot() {
    use rtdb_columnar::MemoryBudget;
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
    use rtdb_sql::PrepareOptions;
    use rtdb_storage::{cache::BufferPool, object_store::MemoryStore};
    use tokio_util::sync::CancellationToken;

    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![put(MAIN_BRANCH, 1, "remove"), put(MAIN_BRANCH, 2, "inherit")]));
    commit(&mut state, 2, &request(2, vec![fork("child", 1)]));
    commit(&mut state, 3, &request(3, vec![put(MAIN_BRANCH, 3, "outside-cut")]));
    commit(&mut state, 4, &request(4, vec![
        put("child", 4, "local"),
        Op::Delete { branch: "child".into(), table: table(), ids: vec![EntityId::Integer(1)], valid: None },
    ]));

    let memory = MemoryBudget::new(1 << 24);
    let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
    let cancel = CancellationToken::new();
    for (layer, count) in [(Layer::Merged, 2), (Layer::Local, 1)] {
        let view = state.index().view("child", At::Latest, layer, Timestamp(40)).unwrap();
        let snapshot = rtdb_query::branch_snapshot(&view, &memory).unwrap();
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&storage) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        for (sql, expected) in [
            ("SELECT count(*) FROM docs", count),
            ("SELECT count(*) FROM xt.branch_lineage WHERE branch = 'child' AND base_branch = 'main'", 1),
        ] {
            let prepared = rtdb_sql::prepare_in(sql, &catalog, &PrepareOptions::default(), &memory).unwrap();
            let context = QueryContext::new(catalog.clone(), Arc::clone(&memory), Timestamp(40), "UTC", QueryOptions::default(), &cancel).unwrap();
            let mut cursor = PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(context, Vec::new()).await.unwrap();
            let result = cursor.next_batch().await.unwrap().unwrap();
            let count = result.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
            assert_eq!(count.values().as_ref(), [expected]);
        }
    }
}

#[test]
fn row_visibility_projection_ignores_generated_hidden_histories() {
    fn assert_same(public: &TransactState, mixed: &TransactState, scope: &LabelScope) {
        let a = public.index().project_main_visibility(scope).unwrap();
        let b = mixed.index().project_main_visibility(scope).unwrap();
        assert_eq!(a.index().head(), b.index().head());
        assert_eq!(a.changes(), b.changes());
        for group in a.changes() {
            let basis = group.cut.basis;
            let view = |index: &BranchIndex| -> Vec<_> {
                let view = index.view(MAIN_BRANCH, At::Exactly(basis), Layer::Merged, Timestamp(0)).unwrap();
                view.ids(&table()).into_iter().filter_map(|id| {
                    let event = view.row(&table(), id, Timestamp(0))?;
                    Some((id.clone(), event.cut, event.image_at(Timestamp(0))?.clone()))
                }).collect()
            };
            assert_eq!(view(a.index()), view(b.index()));
        }
    }

    let scope = LabelScope { read: BTreeSet::from([vec![1]]), ..LabelScope::default() };
    for seed in 0u64..32 {
        let mut pseudorandom = seed;
        let mut public = TransactState::new(INCARNATION);
        let mut mixed = TransactState::new(INCARNATION);
        let declare = request(0, vec![declare_visibility()]);
        commit(&mut public, 1, &declare);
        commit(&mut mixed, 1, &declare);
        let mut public_position = 1;
        let mut mixed_position = 1;
        for step in 1u8..=12 {
            pseudorandom = pseudorandom.wrapping_mul(6364136223846793005).wrapping_add(1);
            for offset in 0..(pseudorandom >> 63) + 1 {
                mixed_position += 1;
                let private = if offset == 0 {
                    let at = Timestamp(40 + (pseudorandom % 13) as i64);
                    let valid = Interval::new(at, at.next().unwrap()).unwrap();
                    labelled_put(MAIN_BRANCH, (pseudorandom % 3) as i64, 2, valid)
                } else {
                    labelled_put(MAIN_BRANCH, 100 + step as i64, 2, Interval::ALL)
                };
                commit(&mut mixed, mixed_position, &request(100 + mixed_position as u8, vec![private]));
            }
            assert_same(&public, &mixed, &scope);

            let valid = Interval::new(Timestamp(-20), Timestamp(20)).unwrap();
            let authorized = request(step, vec![labelled_put(MAIN_BRANCH, (pseudorandom % 3) as i64, 1, valid)]);
            public_position += 1;
            mixed_position += 1;
            commit(&mut public, public_position, &authorized);
            commit(&mut mixed, mixed_position, &authorized);
            assert_same(&public, &mixed, &scope);
        }
        let checkpoint = mixed.encode_checkpoint().unwrap();
        let recovered = TransactState::decode_checkpoint(&checkpoint, mixed.index().head()).unwrap();
        assert_same(&public, &recovered, &scope);
    }
}

#[test]
fn row_visibility_projection_reports_a_hidden_winner_as_only_a_visible_removal() {
    let mut state = TransactState::new(INCARNATION);
    let scope = LabelScope { read: BTreeSet::from([vec![1]]), ..LabelScope::default() };
    let valid = Interval::new(Timestamp(10), Timestamp(20)).unwrap();
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, valid)]));
    commit(&mut state, 2, &request(2, vec![labelled_put(MAIN_BRANCH, 1, 2, Interval::ALL)]));
    let projected = state.index().project_main_visibility(&scope).unwrap();
    let [opened, masked] = projected.changes() else { panic!("expected a visible put and removal"); };
    assert_eq!(opened.cut.system_time, Timestamp(1));
    assert_eq!(masked.cut.system_time, Timestamp(2));
    let [row] = masked.rows.as_slice() else { panic!("expected one masked interval"); };
    assert_eq!(row.valid, valid);
    assert!(row.before.is_some());
    assert!(row.after.is_none());
    let view = projected.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(15)).unwrap();
    assert!(view.row(&table(), &EntityId::Integer(1), Timestamp(15)).is_none());
    assert!(projected.index().exact(state.index().head().basis).is_err());
    let other = LabelScope { read: BTreeSet::from([vec![2]]), ..LabelScope::default() };
    let other = state.index().project_main_visibility(&other).unwrap();
    assert!(other.index().exact(masked.cut.basis).is_err());
}

#[tokio::test]
async fn row_visibility_supplies_only_projected_versions_to_sql_aggregates() {
    use rtdb_columnar::MemoryBudget;
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
    use rtdb_sql::PrepareOptions;
    use rtdb_storage::{cache::BufferPool, object_store::MemoryStore};
    use tokio_util::sync::CancellationToken;

    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL),
        labelled_put(MAIN_BRANCH, 2, 2, Interval::ALL)]));
    let masked = Interval::new(Timestamp(10), Timestamp(20)).unwrap();
    commit(&mut state, 2, &request(2, vec![labelled_put(MAIN_BRANCH, 1, 2, masked)]));
    let scope = LabelScope { read: BTreeSet::from([vec![1]]), ..LabelScope::default() };
    let projected = state.index().project_main_visibility(&scope).unwrap();
    let memory = MemoryBudget::new(1 << 24);
    let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
    let cancel = CancellationToken::new();
    for (group, expected) in projected.changes().iter().zip([1, 0]) {
        let now = Timestamp(15);
        let view = projected.index().view(MAIN_BRANCH, At::Exactly(group.cut.basis), Layer::Merged, now).unwrap();
        let snapshot = rtdb_query::branch_snapshot(&view, &memory).unwrap();
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&storage) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let prepared = rtdb_sql::prepare_in("SELECT count(*) FROM docs", &catalog, &PrepareOptions::default(), &memory).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), now, "UTC", QueryOptions::default(), &cancel).unwrap();
        let mut cursor = PreparedQuery::from_prepared(prepared, &memory).unwrap().execute(context, Vec::new()).await.unwrap();
        let result = cursor.next_batch().await.unwrap().unwrap();
        let count = result.batch().column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(count.values().as_ref(), [expected]);
    }
}

#[test]
fn row_visibility_scopes_otherwise_effects_and_receipts() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(2, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 2, Interval::ALL)]));
    let scope = LabelScope { read: BTreeSet::from([vec![1]]), write: BTreeSet::from([vec![1]]) };
    let authority = ScopedTransact { labels: &scope, receipt_namespace: [3; 32] };
    let other = ScopedTransact { receipt_namespace: [4; 32], ..authority };

    let mut guarded = request(2, vec![labelled_put(MAIN_BRANCH, 2, 1, Interval::ALL)]);
    guarded.guards.push(Guard::Version {
        branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(1), valid_at: None, expect: Version::Absent,
    });
    let resolve = |state: &TransactState, request: &Transact, position, authority| {
        let Resolution::Append(prepared) = state.resolve_scoped(
            request, position, Timestamp(40), MULTI, &claims(), authority,
        ).unwrap() else { panic!("expected append"); };
        prepared
    };
    let prepared = resolve(&state, &guarded, 2, authority);
    let receipt = apply(&mut state, 2, prepared.record()).unwrap();
    assert_eq!(receipt.outcome, Outcome::Otherwise { failed: vec![0] });
    assert_eq!(state.tx_status(&guarded.key).unwrap().basis, basis(1));
    assert_eq!(state.tx_status_scoped(&guarded.key, authority).unwrap().basis, basis(2));
    assert_eq!(state.tx_status_scoped(&guarded.key, other).unwrap_err().code, "rtdb/unknown-transaction");

    guarded.otherwise.push(labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL));
    let prepared = resolve(&state, &guarded, 3, other);
    let receipt = apply(&mut state, 3, prepared.record()).unwrap();
    assert!(matches!(&receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/write-scope"));

    let before = state.index().head();
    let checkpoint = state.encode_checkpoint().unwrap();
    let recovered = TransactState::decode_checkpoint(&checkpoint, before).unwrap();
    assert_eq!(recovered.tx_status(&guarded.key).unwrap().basis, basis(1));
    assert_eq!(recovered.tx_status_scoped(&guarded.key, authority).unwrap().basis, basis(2));
    assert_eq!(recovered.tx_status_scoped(&guarded.key, other).unwrap().basis, basis(3));
}

#[test]
fn row_visibility_scoped_claims_check_hidden_effects_without_returning_them() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL)]));
    let scope = LabelScope { read: BTreeSet::from([vec![1]]), ..LabelScope::default() };
    let caller = ScopedTransact { labels: &scope, receipt_namespace: [2; 32] };
    let evidence = ClaimScope { branch: MAIN_BRANCH.into(), basis: basis(1), layer: Layer::Merged };
    let claim = claims().seal_scoped(&evidence, &scope).unwrap();
    let mut validation = request(3, vec![]);
    validation.guards.push(Guard::Unchanged(claim));
    let validate = |state: &TransactState, request: &Transact, authority: ScopedTransact<'_>| {
        let sealer = claims();
        let verifier = sealer.scoped_verifier(authority.labels);
        let Resolution::Answer(receipt) = state.resolve_scoped(request, 3, Timestamp(30), DEFAULT, &verifier, authority).unwrap()
            else { panic!("expected validation receipt"); };
        state.project_receipt(receipt, authority.labels).unwrap()
    };
    let first = validate(&state, &validation, caller);
    assert_eq!(first.outcome, Outcome::Then);
    commit(&mut state, 2, &request(2, vec![labelled_put(MAIN_BRANCH, 2, 2, Interval::ALL)]));
    let second = validate(&state, &validation, caller);
    assert_eq!(second.outcome, Outcome::Otherwise { failed: vec![0] });
    assert_eq!(first.basis, second.basis);
    assert_eq!(first.system_time, second.system_time);

    let foreign = LabelScope::default();
    let wrong = validate(&state, &validation, ScopedTransact { labels: &foreign, ..caller });
    assert!(matches!(wrong.outcome, Outcome::Refused { error } if error.code == "rtdb/invalid-claim"));
    validation.guards[0] = Guard::Unchanged(claims().seal(&evidence).unwrap());
    assert!(matches!(validate(&state, &validation, caller).outcome, Outcome::Refused { error } if error.code == "rtdb/invalid-claim"));
}

#[test]
fn custody_reservation_activation_and_release_share_the_revocation_order() {
    use std::num::NonZeroU64;
    use rtdb_index::branches::{CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponsePart};

    let group = [8; 16];
    let features = [LINEARIZABLE, ROW_CUSTODY];
    let mut state = TransactState::new(INCARNATION);
    let write = |state: &mut TransactState, at, request: &Transact| {
        let prepared = plan(state, request, at, at as i64 * 10, &features);
        apply(state, at, prepared.record()).unwrap()
    };
    let place = |branch: &str, stage: CustodyPlacementId, reservation, valid, carrier_sha256, link_sha256: Option<[u8; 32]>| {
        Op::PlaceCustodyRow {
            branch: branch.into(), table: table(),
            row: Row {
                id: EntityId::Integer(1), valid: Some(ValidTime::Literal(valid)), cells: StructFields::new(),
                custody_group: Some(group), custody_payload: Some(CustodyPayload { binding: [3; 32], envelope: vec![stage[0]] }),
                protected_columns: if link_sha256.is_some() { vec!["carrier".into(), "link".into()] } else { vec!["carrier".into()] },
            },
            placement: CustodyPlacement {
                placement_id: stage, reservation, carrier_column: "carrier".into(), link_column: link_sha256.map(|_| "link".into()),
                commitments: Some(CustodyPlacementCommitments { carrier_sha256, link_sha256 }),
            },
        }
    };
    let valid = Interval::new(Timestamp(0), Timestamp(1_000)).unwrap();
    let original_id = [11; 16];
    assert_eq!(write(&mut state, 1, &request(1, vec![Op::CreateCustodyGroup { group },
        place(MAIN_BRANCH, original_id, None, valid, [7; 32], None)])).outcome, Outcome::Then);
    let occurrence_at = |state: &TransactState, stage| {
        let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(30)).unwrap();
        view.custody_placements().find(|entry| entry.placement_id == stage).unwrap().occurrence.unwrap().clone()
    };
    let original = occurrence_at(&state, original_id).commitment(group);
    let plan_before_reserving = state.index().custody_inventory(group).unwrap().sha256;
    let reservation = CustodyReservation {
        original, source: original, destination: [12; 16], link: [13; 16],
        target: CustodyTarget { branch: "copy".into(), table: table(), id: EntityId::Integer(1), valid },
        review_sha256: [15; 32], carrier_sha256: [16; 32], link_sha256: [17; 32],
    };
    let reserved = reservation.commitment(INCARNATION, group);
    assert_eq!(write(&mut state, 2, &request(2, vec![fork("copy", 1), Op::ReserveCustodyPlacement {
        group, reservation: reservation.clone(),
    }])).outcome, Outcome::Then);
    let pending_inventory = state.index().custody_inventory(group).unwrap();
    assert_eq!(pending_inventory.placements, 2);
    assert_ne!(pending_inventory.sha256, plan_before_reserving);
    let stale = |inventory_sha256| Op::RevokeCustodyGroup { group, inventory_sha256 };
    assert!(matches!(write(&mut state, 3, &request(3, vec![stale(plan_before_reserving)])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));
    let activate = |carrier| place("copy", reservation.destination, Some(reserved), valid, carrier, Some(reservation.link_sha256));
    assert!(matches!(write(&mut state, 4, &request(4, vec![activate([18; 32])])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
    assert_eq!(state.index().custody_inventory(group).unwrap().sha256, pending_inventory.sha256);
    let activation = request(5, vec![activate(reservation.carrier_sha256)]);
    let mut fence_first = state.clone();
    assert_eq!(write(&mut fence_first, 5, &request(14, vec![stale(pending_inventory.sha256)])).outcome, Outcome::Then);
    assert!(matches!(write(&mut fence_first, 6, &activation).outcome,
        Outcome::Refused { error } if error.code == "rtdb/erased-under-view"));
    let fenced = fence_first.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(60)).unwrap();
    assert!(fenced.custody_placements().find(|entry| entry.placement_id == reservation.destination).unwrap().occurrence.is_none());
    let first = write(&mut state, 5, &activation);
    assert_eq!(first.outcome, Outcome::Then);
    let active = occurrence_at(&state, reservation.destination);
    assert_eq!((active.written, active.ordinal, active.reservation), (first.basis, 1, Some(reserved)));
    assert!(matches!(write(&mut state, 6, &request(6, vec![activate(reservation.carrier_sha256)])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
    assert!(matches!(write(&mut state, 7, &request(7, vec![stale(pending_inventory.sha256)])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));

    let registration = CustodyReleaseRegistration {
        authority: CustodyCarrierAuthority {
            incarnation: INCARNATION, source_history: [15; 32], installation: [16; 16], registry: [17; 32], original_writer: [18; 32],
        },
        group, original, occurrence: active.commitment(group), expires_at: Timestamp(120),
        part: CustodyResponsePart {
            transport_history: [19; 16], producer: [20; 16], consumer: [21; 16], response: [22; 16], ordinal: NonZeroU64::MIN,
        },
    };
    let release = |phase| Op::CustodyRelease(CustodyRelease { registration, phase });
    let stage = request(8, vec![release(CustodyReleasePhase::Private)]);
    assert_eq!(commit(&mut state, 8, &stage).outcome, Outcome::Then);
    assert!(state.tx_status(&stage.key).is_err());
    assert_eq!(commit(&mut state, 9, &request(9, vec![release(CustodyReleasePhase::Promoted)])).outcome, Outcome::Then);
    let inventory = state.index().custody_inventory(group).unwrap();
    let revoke = request(10, vec![stale(inventory.sha256)]);
    let revocation = write(&mut state, 10, &revoke);
    assert_eq!(revocation.outcome, Outcome::Then);
    assert!(!state.index().custody_releases_drained(group));
    assert!(matches!(commit(&mut state, 11, &request(11, vec![release(CustodyReleasePhase::Cancelled)])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-release"));
    let terminal = CustodyReleasePhase::Disposed { evidence_sha256: [23; 32] };
    let dispose = request(12, vec![release(terminal)]);
    let disposed = commit(&mut state, 12, &dispose);
    assert_eq!(disposed.outcome, Outcome::Then);
    let checkpoint = state.encode_checkpoint().unwrap();
    let state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    assert_eq!(state.index().custody_release_phase(registration).unwrap(), Some(terminal));
    assert!(state.index().custody_releases_drained(group));
    assert!(state.index().custody_release_phase(CustodyReleaseRegistration { expires_at: Timestamp(121), ..registration }).is_err());
    assert_eq!(state.index().custody_release_phase(CustodyReleaseRegistration { occurrence: original, ..registration }).unwrap(), None);
    for (request, prior) in [(activation, first), (revoke, revocation), (dispose, disposed)] {
        assert!(matches!(state.resolve(&request, 13, Timestamp(130), &features, &claims()).unwrap(),
            Resolution::Answer(replay) if replay.replayed && replay.basis == prior.basis && replay.outcome == prior.outcome));
    }
}
