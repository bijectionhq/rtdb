use super::*;
use rtdb_types::rtdb_v1::{ClaimEvidence, ClaimFootprint, ClaimRegion, ClaimScope, ClaimSealer};

const INCARNATION: [u8; 16] = [1; 16];
const DEFAULT: &[&str] = &[LINEARIZABLE];
const MULTI: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT];

fn claims() -> ClaimSealer { ClaimSealer::new(INCARNATION, [2; 32]) }
fn table() -> TableRef { TableRef::new("public", "docs") }
fn basis(position: u64) -> Basis { Basis::new(INCARNATION, position) }

fn request(key: u8, ops: Vec<Op>) -> Transact {
    Transact { key: vec![key; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), ops, otherwise: Vec::new() }
}

fn put(branch: &str, id: i64, key: &str) -> Op {
    let row = Row { id: EntityId::Integer(id), valid: None, cells: StructFields::from([("key".into(), Cell::Value(Value::Utf8(key.into())))]) };
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
        Op::Declare { table: table(), change: Declaration::Index(IndexDefinition { name: "by_key".into(), column: "key".into() }) },
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
    let index = IndexDefinition { name: "by_key".into(), column: "key".into() };
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
    let index = IndexDefinition { name: "key".into(), column: "key".into() };
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
