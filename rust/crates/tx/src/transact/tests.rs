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

fn store_append(key: u8, since: u64, reads: Vec<KeyRange>, writes: Vec<StoreWrite>, window: Window) -> Transact {
    store_append_with_bounds(key, since, reads, writes, Vec::new(), window)
}

fn store_append_with_bounds(
    key: u8, since: u64, reads: Vec<KeyRange>, writes: Vec<StoreWrite>, bounds: Vec<StoreBound>, window: Window,
) -> Transact {
    Transact {
        key: vec![key; 16], require: Vec::new(), window: Window::default(),
        guards: reads.into_iter().map(|range| Guard::Unwritten { range, since: basis(since) }).collect(),
        ops: vec![Op::Store(StoreAppend { writes, bounds, derives: Vec::new(), window })], otherwise: Vec::new(), requires: Vec::new(),
        custody_request_digest: None, request_memory: None,
    }
}

fn refused(state: &TransactState, request: &Transact, at: u64, now: i64) -> String {
    match state.resolve(request, at, Timestamp(now), DEFAULT, &claims()) {
        Err(error) => error.code,
        Ok(_) => panic!("expected a refusal"),
    }
}

#[test]
fn a_store_append_is_judged_and_stamped_at_its_clock_sample() {
    use rtdb_index::branches::store::{Projection, StoreRead};
    let mut state = TransactState::new(INCARNATION);
    let partition = vec![0u8, 1, b'p'];
    let stamped = StoreWrite {
        key: [partition.as_slice(), b"e", &[0; 16]].concat(), value: Some(vec![0; 16]), key_stamp: Some(4), value_stamp: Some(0),
    };
    let other = StoreWrite { key: [partition.as_slice(), b"o"].concat(), value: Some(b"v".to_vec()), key_stamp: None, value_stamp: None };
    let everything = KeyRange { start: partition.clone(), end: None };

    // Committed at its own sample; both placeholders hold the stamp.
    let committed = plan(&state, &store_append(1, 0, Vec::new(), vec![stamped.clone()], Window::default()), 1, 100, DEFAULT);
    let receipt = apply(&mut state, 1, committed.record()).unwrap();
    assert_eq!((receipt.basis, receipt.system_time, receipt.outcome), (basis(1), Timestamp(100), Outcome::Then));
    let stamp = StoreStamp { micros: 100, version: 1 }.encode();
    let read = StoreRead { range: everything.clone(), after: None, limit: 10, projection: Projection::Values, filter: None, reverse: false };
    let page = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(100)).unwrap().store_page(&read).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(&page.entries[0].key[4..], stamp.as_slice());
    assert_eq!(page.entries[0].value.as_deref(), Some(stamp.as_slice()));

    // Ties at the head's time commit in version order; a regressed sample faults.
    assert_eq!(refused(&state, &store_append(2, 1, Vec::new(), vec![other.clone()], Window::default()), 2, 99), STORE_CLOCK_REGRESSED);
    let tied = plan(&state, &store_append(2, 1, Vec::new(), vec![other.clone()], Window::default()), 2, 100, DEFAULT);
    let tied_receipt = apply(&mut state, 2, tied.record()).unwrap();
    assert_eq!((tied_receipt.basis, tied_receipt.system_time, tied_receipt.outcome), (basis(2), Timestamp(100), Outcome::Then));
    // Outside the window nothing is decided.
    let closed = Window { not_before: None, before: Some(Timestamp(150)) };
    let unopened = Window { not_before: Some(Timestamp(300)), before: None };
    assert_eq!(refused(&state, &store_append(3, 2, Vec::new(), vec![other.clone()], closed), 3, 200), STORE_OUTSIDE_LATE);
    assert_eq!(refused(&state, &store_append(4, 2, Vec::new(), vec![other.clone()], unopened), 3, 200), STORE_OUTSIDE_EARLY);
    // A misplaced placeholder is the request's error.
    let inside = StoreWrite { key_stamp: Some(1), ..stamped.clone() };
    assert_eq!(refused(&state, &store_append(5, 2, Vec::new(), vec![inside], Window::default()), 3, 200), "rtdb.store/invalid-stamp-placeholder");

    // The reads are judged before the window: a conflict is recorded.
    let conflicting = plan(&state, &store_append(6, 0, vec![everything.clone()], vec![other.clone()], closed), 3, 200, DEFAULT);
    let receipt = apply(&mut state, 3, conflicting.record()).unwrap();
    assert_eq!(receipt.outcome, Outcome::Otherwise { failed: vec![0] });
    // A write-free append validates through its own record, which writes nothing.
    let validated = plan(&state, &store_append(7, 3, vec![everything], Vec::new(), Window::default()), 4, 300, DEFAULT);
    let receipt = apply(&mut state, 4, validated.record()).unwrap();
    assert_eq!((receipt.basis, receipt.outcome), (basis(4), Outcome::Then));
}

/// The SHA-256 of Bijection's `crates/fact_store/src/persistence/judge.rs`.
/// Bijection's `fact_store` pins the same digest of its own copy
/// (`persistence::tests::the_store_judge_matches_rtdbs_vendored_copy`), so a
/// change on either side alone fails a test. Change the judge in Bijection,
/// copy it to `vendor/bijection-store-judge/src/judge.rs` and update both pins
/// in the paired commits.
const BIJECTION_STORE_JUDGE_SHA256: &str = "5010d6fb02ae6aaff5ae37417ed816d2cc09b0f3dd5973ab127754e3b6ea92e3";

#[test]
fn the_vendored_store_judge_is_bijections_pinned_copy() {
    use sha2::{Digest, Sha256};
    let source = include_bytes!("../../../../vendor/bijection-store-judge/src/judge.rs");
    let digest = Sha256::digest(source).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    assert_eq!(digest, BIJECTION_STORE_JUDGE_SHA256, "vendor/bijection-store-judge drifted from Bijection's judge.rs");
}

#[test]
fn a_store_derive_diffs_retires_and_purges_atomically() {
    use rtdb_index::branches::store::{
        Projection, StoreRead,
        store_derive::{Derive, DeriveEntry, DeriveInterval, DeriveLevel, DeriveMode, EntryKind},
    };
    let mut state = TransactState::new(INCARNATION);
    // Bijection's stored key: `partition ‖ family ‖ segment(world) ‖ …`, the
    // manifest in the Row family (16), its markers in the Index family (17).
    let place = |family: u8, inner: &[u8]| {
        let world: Vec<u8> = [0u8; 16].iter().flat_map(|_| [0u8, 0xff]).chain([0u8, 1]).collect();
        [vec![0u8, 1, b'p', family], world, inner.to_vec()].concat()
    };
    let manifest_prefix = place(16, b"row1\x04");
    let idx_a = place(17, b"idx/a");
    let idx_b = place(17, b"idx/b");
    let level = vec![DeriveLevel { prefix: manifest_prefix.clone(), through: None }];
    let marker = |id: &Vec<u8>| DeriveEntry {
        kind: EntryKind::Marker,
        identity: id.clone(),
        key_stamp: None,
        key_instants: Vec::new(),
        value_instants: Vec::new(),
        enter_value: vec![1],
        leave_value: Vec::new(),
    };
    let make = |key: u8, since: u64, reads: Vec<KeyRange>, derive: Derive| Transact {
        key: vec![key; 16],
        require: Vec::new(),
        window: Window::default(),
        guards: reads.into_iter().map(|range| Guard::Unwritten { range, since: basis(since) }).collect(),
        ops: vec![Op::Store(StoreAppend { writes: Vec::new(), bounds: Vec::new(), derives: vec![derive.encode()], window: Window::default() })],
        otherwise: Vec::new(),
        requires: Vec::new(),
        custody_request_digest: None,
        request_memory: None,
    };
    let d_a = Derive {
        manifest_prefix: manifest_prefix.clone(),
        levels: level.clone(),
        mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(i64::MAX), entries: vec![marker(&idx_a)] },
    };
    let d_b = Derive {
        manifest_prefix: manifest_prefix.clone(),
        levels: level,
        mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(i64::MAX), entries: vec![marker(&idx_b)] },
    };
    let d_purge = Derive { manifest_prefix: manifest_prefix.clone(), levels: Vec::new(), mode: DeriveMode::Purge };

    // 1. Enter idx_a.
    let c1 = plan(&state, &make(1, 0, Vec::new(), d_a.clone()), 1, 100, DEFAULT);
    apply(&mut state, 1, c1.record()).unwrap();
    // 2. Identical re-put writes no new key in idx_a; reader of idx_a since 1 validates.
    let c2 = plan(&state, &make(2, 1, Vec::new(), d_a), 2, 200, DEFAULT);
    apply(&mut state, 2, c2.record()).unwrap();
    let range_a = KeyRange { start: idx_a.clone(), end: Some([idx_a[..idx_a.len() - 1].to_vec(), vec![b'b']].concat()) };
    let val = plan(&state, &store_append(3, 1, vec![range_a.clone()], Vec::new(), Window::default()), 3, 300, DEFAULT);
    assert_eq!(apply(&mut state, 3, val.record()).unwrap().outcome, Outcome::Then);
    // 3. Move to idx_b writes leave in idx_a; reader of idx_a since 2 conflicts.
    let c4 = plan(&state, &make(4, 3, Vec::new(), d_b), 4, 400, DEFAULT);
    apply(&mut state, 4, c4.record()).unwrap();
    let stale = plan(&state, &store_append(5, 2, vec![range_a.clone()], Vec::new(), Window::default()), 5, 500, DEFAULT);
    assert_eq!(apply(&mut state, 5, stale.record()).unwrap().outcome, Outcome::Otherwise { failed: vec![0] });
    // 4. Purge deletes all markers and manifests.
    let c6 = plan(&state, &make(6, 5, Vec::new(), d_purge), 6, 600, DEFAULT);
    apply(&mut state, 6, c6.record()).unwrap();
    let read_a = StoreRead { range: range_a, after: None, limit: 16, projection: Projection::Keys, filter: None, reverse: false };
    assert!(state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(600)).unwrap().store_page(&read_a).unwrap().entries.is_empty());
}

#[test]
fn a_store_derive_claim_is_judged_on_its_filled_key_at_ordering() {
    use rtdb_index::branches::store::store_derive::{Derive, DeriveEntry, DeriveInterval, DeriveLevel, DeriveMode, EntryKind, KeyInstant};
    let mut state = TransactState::new(INCARNATION);
    let place = |family: u8, inner: &[u8]| {
        let world: Vec<u8> = [0u8; 16].iter().flat_map(|_| [0u8, 0xff]).chain([0u8, 1]).collect();
        [vec![0u8, 1, b'p', family], world, inner.to_vec()].concat()
    };
    // A claim of the stamp's micros plus 7: eight zero bytes the resolver
    // fills, then a fixed claim of the value it filled to.
    let template = [place(32, b"claim/"), vec![0; 8]].concat();
    let offset = u32::try_from(template.len() - 8).unwrap();
    let claim = |row: &[u8], identity: Vec<u8>, key_instants: Vec<KeyInstant>| {
        let manifest_prefix = place(16, row);
        Derive {
            levels: vec![DeriveLevel { prefix: manifest_prefix.clone(), through: None }],
            manifest_prefix,
            mode: DeriveMode::Replace {
                interval: DeriveInterval::from_stamp(i64::MAX),
                entries: vec![DeriveEntry {
                    kind: EntryKind::Claim,
                    identity,
                    key_stamp: None,
                    key_instants,
                    value_instants: Vec::new(),
                    enter_value: 1i64.to_be_bytes().to_vec(),
                    leave_value: (-1i64).to_be_bytes().to_vec(),
                }],
            },
        }
    };
    let make = |key: u8, derive: Derive| Transact {
        key: vec![key; 16],
        require: Vec::new(),
        window: Window::default(),
        guards: Vec::new(),
        ops: vec![Op::Store(StoreAppend { writes: Vec::new(), bounds: Vec::new(), derives: vec![derive.encode()], window: Window::default() })],
        otherwise: Vec::new(),
        requires: Vec::new(),
        custody_request_digest: None,
        request_memory: None,
    };
    let first = plan(&state, &make(1, claim(b"row1\x04", template.clone(), vec![KeyInstant { offset, escapes: 0, plus_micros: 7 }])), 1, 100, DEFAULT);
    assert_eq!(apply(&mut state, 1, first.record()).unwrap().outcome, Outcome::Then);
    let filled = [place(32, b"claim/"), ((107u64) ^ (1 << 63)).to_be_bytes().to_vec()].concat();
    let second = plan(&state, &make(2, claim(b"row2\x04", filled.clone(), Vec::new())), 2, 200, DEFAULT);
    match apply(&mut state, 2, second.record()).unwrap().outcome {
        Outcome::Refused { error } => {
            assert_eq!(error.code, STORE_CLAIM);
            assert_eq!(error.context.get("derive").and_then(|value| value.as_u64()), Some(0));
        },
        other => panic!("a second claimant of the filled value is refused, got {other:?}"),
    }
    let other = [place(32, b"claim/"), ((108u64) ^ (1 << 63)).to_be_bytes().to_vec()].concat();
    let third = plan(&state, &make(3, claim(b"row3\x04", other, Vec::new())), 3, 300, DEFAULT);
    assert_eq!(apply(&mut state, 3, third.record()).unwrap().outcome, Outcome::Then);
}

#[test]
fn a_store_derive_close_retracts_a_claim_at_ordering() {
    use rtdb_index::branches::store::store_derive::{Derive, DeriveEntry, DeriveInterval, DeriveLevel, DeriveMode, EntryKind};
    let mut state = TransactState::new(INCARNATION);
    let place = |family: u8, inner: &[u8]| {
        let world: Vec<u8> = [0u8; 16].iter().flat_map(|_| [0u8, 0xff]).chain([0u8, 1]).collect();
        [vec![0u8, 1, b'p', family], world, inner.to_vec()].concat()
    };
    let value = place(32, b"claim/x");
    let derive = |row: &[u8], mode: DeriveMode| {
        let manifest_prefix = place(16, row);
        Derive { levels: vec![DeriveLevel { prefix: manifest_prefix.clone(), through: None }], manifest_prefix, mode }
    };
    let claim = |row: &[u8]| derive(row, DeriveMode::Replace {
        interval: DeriveInterval::from_stamp(i64::MAX),
        entries: vec![DeriveEntry {
            kind: EntryKind::Claim,
            identity: value.clone(),
            key_stamp: None,
            key_instants: Vec::new(),
            value_instants: Vec::new(),
            enter_value: 1i64.to_be_bytes().to_vec(),
            leave_value: (-1i64).to_be_bytes().to_vec(),
        }],
    });
    let make = |key: u8, derive: Derive| Transact {
        key: vec![key; 16],
        require: Vec::new(),
        window: Window::default(),
        guards: Vec::new(),
        ops: vec![Op::Store(StoreAppend { writes: Vec::new(), bounds: Vec::new(), derives: vec![derive.encode()], window: Window::default() })],
        otherwise: Vec::new(),
        requires: Vec::new(),
        custody_request_digest: None,
        request_memory: None,
    };
    let first = plan(&state, &make(1, claim(b"row1\x04")), 1, 100, DEFAULT);
    assert_eq!(apply(&mut state, 1, first.record()).unwrap().outcome, Outcome::Then);
    let refused = plan(&state, &make(2, claim(b"row2\x04")), 2, 200, DEFAULT);
    assert!(matches!(apply(&mut state, 2, refused.record()).unwrap().outcome, Outcome::Refused { .. }), "a second claimant is refused");
    // Bijection aggregates-close: the first row's unit closes, and the
    // resolver retracts its claim at the closing stamp, reading nothing for
    // the writer, so the value is free from the closure on.
    let closed = plan(&state, &make(3, derive(b"row1\x04", DeriveMode::Close)), 3, 300, DEFAULT);
    assert_eq!(apply(&mut state, 3, closed.record()).unwrap().outcome, Outcome::Then);
    let again = plan(&state, &make(4, claim(b"row2\x04")), 4, 400, DEFAULT);
    assert_eq!(apply(&mut state, 4, again.record()).unwrap().outcome, Outcome::Then, "the closed row's claim no longer counts");
}

#[test]
fn a_store_append_records_its_validation_and_conflict_as_receipts() {
    use rtdb_index::branches::store::{Projection, StoreRead};
    let mut state = TransactState::new(INCARNATION);
    let partition = vec![0u8, 1, b'p'];
    let everything = KeyRange { start: partition.clone(), end: None };
    let write = StoreWrite { key: [partition.as_slice(), b"k"].concat(), value: Some(b"v".to_vec()), key_stamp: None, value_stamp: None };
    let entries = |state: &TransactState, at: i64| {
        let read = StoreRead { range: everything.clone(), after: None, limit: 10, projection: Projection::Keys, filter: None, reverse: false };
        state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(at)).unwrap().store_page(&read).unwrap().entries.len()
    };

    // A write-free append is recorded at its own position with no effects: that
    // position is the version it validated through.
    let validation = store_append(1, 0, vec![everything.clone()], Vec::new(), Window::default());
    let validated = plan(&state, &validation, 1, 100, DEFAULT);
    let receipt = apply(&mut state, 1, validated.record()).unwrap();
    assert_eq!((receipt.basis, receipt.system_time, receipt.outcome), (basis(1), Timestamp(100), Outcome::Then));
    assert_eq!(entries(&state, 100), 0, "a validation writes nothing");

    // A later write into its reads would conflict the same request judged afresh;
    // the retry replays the first outcome instead.
    let committed = plan(&state, &store_append(2, 1, Vec::new(), vec![write], Window::default()), 2, 200, DEFAULT);
    apply(&mut state, 2, committed.record()).unwrap();
    let Resolution::Answer(replayed) = state.resolve(&validation, 3, Timestamp(300), DEFAULT, &claims()).unwrap() else {
        panic!("a retried validation replays its receipt");
    };
    assert!(replayed.replayed);
    assert_eq!((replayed.basis, replayed.outcome), (basis(1), Outcome::Then));
    // The key belongs to its first request.
    let reused = store_append(1, 0, Vec::new(), Vec::new(), Window::default());
    assert_eq!(refused(&state, &reused, 3, 300), "rtdb/idempotency-key");

    // A write-free append that conflicts is recorded too, and its retry replays the
    // conflict.
    let stale = store_append(3, 1, vec![everything.clone()], Vec::new(), Window::default());
    let conflicted = plan(&state, &stale, 3, 300, DEFAULT);
    let receipt = apply(&mut state, 3, conflicted.record()).unwrap();
    assert_eq!((receipt.basis, receipt.outcome), (basis(3), Outcome::Otherwise { failed: vec![0] }));
    let Resolution::Answer(replayed) = state.resolve(&stale, 4, Timestamp(400), DEFAULT, &claims()).unwrap() else {
        panic!("a retried conflict replays its receipt");
    };
    assert_eq!((replayed.basis, replayed.outcome), (basis(3), Outcome::Otherwise { failed: vec![0] }));

    // Outside its window a validation is no receipt: its retry is judged again.
    let closing = Window { not_before: None, before: Some(Timestamp(450)) };
    let late = store_append(4, 3, vec![everything], Vec::new(), closing);
    assert_eq!(refused(&state, &late, 4, 450), STORE_OUTSIDE_LATE);
    let validated = plan(&state, &late, 4, 400, DEFAULT);
    let receipt = apply(&mut state, 4, validated.record()).unwrap();
    assert_eq!((receipt.basis, receipt.outcome), (basis(4), Outcome::Then));
}

#[test]
fn a_store_append_bound_updates_without_conflicts_and_refuses_out_of_range() {
    use rtdb_index::branches::store::{Projection, StoreRead};
    let mut state = TransactState::new(INCARNATION);
    let host = vec![0u8, 4, b'h', b'o', b's', b't'];
    let counter = [host.as_slice(), b"shared/places"].concat();
    let payload = StoreWrite { key: [host.as_slice(), b"side"].concat(), value: Some(b"ok".to_vec()), key_stamp: None, value_stamp: None };

    // Invalid bound (min > max) is a request error before ordering.
    let invalid = store_append_with_bounds(1, 0, Vec::new(), Vec::new(), vec![StoreBound { key: counter.clone(), delta: 1, min: 5, max: 4 }], Window::default());
    assert_eq!(refused(&state, &invalid, 1, 100), "rtdb.store/invalid-bound");

    // Two concurrent appends from since=0 both succeed on the same counter without conflicting.
    let b1 = store_append_with_bounds(2, 0, Vec::new(), Vec::new(), vec![StoreBound { key: counter.clone(), delta: 3, min: 0, max: 5 }], Window::default());
    let p1 = plan(&state, &b1, 1, 100, DEFAULT);
    assert_eq!(apply(&mut state, 1, p1.record()).unwrap().outcome, Outcome::Then);

    let b2 = store_append_with_bounds(3, 0, Vec::new(), vec![payload.clone()], vec![StoreBound { key: counter.clone(), delta: 2, min: 0, max: 5 }], Window::default());
    let p2 = plan(&state, &b2, 2, 200, DEFAULT);
    assert_eq!(apply(&mut state, 2, p2.record()).unwrap().outcome, Outcome::Then);

    // Exceeding max refuses the whole append (writes nothing) and records a receipt.
    let over = store_append_with_bounds(4, 0, Vec::new(), vec![StoreWrite { key: [host.as_slice(), b"never"].concat(), value: Some(b"x".to_vec()), key_stamp: None, value_stamp: None }], vec![StoreBound { key: counter.clone(), delta: 1, min: 0, max: 5 }], Window::default());
    let p3 = plan(&state, &over, 3, 300, DEFAULT);
    let receipt = apply(&mut state, 3, p3.record()).unwrap();
    match &receipt.outcome {
        Outcome::Refused { error } => {
            assert_eq!(error.code, STORE_BOUND);
            assert_eq!(error.context.get("bound").and_then(|v| v.as_u64()), Some(0));
        }
        other => panic!("expected bound refusal, got {other:?}"),
    }

    // Retry replays the refusal receipt even after a decrement frees headroom.
    let refund = store_append_with_bounds(5, 3, Vec::new(), Vec::new(), vec![StoreBound { key: counter.clone(), delta: -2, min: 0, max: 5 }], Window::default());
    let p4 = plan(&state, &refund, 4, 400, DEFAULT);
    assert_eq!(apply(&mut state, 4, p4.record()).unwrap().outcome, Outcome::Then);
    let Resolution::Answer(replayed) = state.resolve(&over, 5, Timestamp(500), DEFAULT, &claims()).unwrap() else {
        panic!("a retried bound refusal replays its receipt");
    };
    assert!(replayed.replayed);
    assert!(matches!(replayed.outcome, Outcome::Refused { ref error } if error.code == STORE_BOUND));

    // Counter value is 3 + 2 - 2 = 3, and the refused write never landed.
    let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(500)).unwrap();
    let page = view.store_page(&StoreRead {
        range: KeyRange { start: host, end: None }, after: None, limit: 10, projection: Projection::Values, filter: None, reverse: false,
    }).unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(view.store_value(&counter).unwrap(), Some(3i64.to_be_bytes().as_slice()));
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
    assert_eq!(state.encode_checkpoint_at(basis(2)).unwrap_err().code, "rtdb/erased-under-view");
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
fn projected_hold_reopens_same_publication_after_checkpoint_and_hidden_write() {
    use rtdb_index::branches::ScopedHold;

    let mut state = TransactState::new(INCARNATION);
    let labels = LabelScope { read: BTreeSet::from([vec![1]]), ..Default::default() };
    let owner = [9; 32];
    let id = [8; 16];
    let authority = ScopedTransact { labels: &labels, receipt_namespace: owner };
    commit(&mut state, 1, &request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL)]));
    let projected = state.index().project_main_visibility(&labels).unwrap().index().head().basis;
    let hold = ScopedHold {
        id, owner, basis: projected, valid_at: Timestamp(50), expires_at: Timestamp(120),
        publication_table: table(), publication_id: EntityId::Integer(1), publication_written: projected,
    };
    let request_hold = Transact::scoped_hold(ScopedHoldCommand::Retain(hold.clone()));
    assert!(state.resolve(&request_hold, 2, Timestamp(20), &[], &claims()).is_err());
    let Resolution::Append(append) = state.resolve_scoped(&request_hold, 2, Timestamp(20), &[], &claims(), authority).unwrap()
        else { panic!("the authorized hold must order"); };
    assert_eq!(apply(&mut state, 2, append.record()).unwrap().outcome, Outcome::Then);
    commit(&mut state, 3, &request(3, vec![labelled_put(MAIN_BRANCH, 2, 2, Interval::ALL)]));
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    assert_eq!(state.index().open_scoped_hold(id, owner, &labels, Timestamp(119)).unwrap(), &hold);
    assert!(state.index().open_scoped_hold(id, [6; 32], &labels, Timestamp(119)).is_err());
    assert!(state.index().open_scoped_hold(id, owner, &labels, Timestamp(120)).is_err());
    let release = Transact::scoped_hold(ScopedHoldCommand::Release { id, owner });
    let Resolution::Append(append) = state.resolve_scoped(&release, 4, Timestamp(40), &[], &claims(), authority).unwrap()
        else { panic!("the authorized release must order"); };
    assert_eq!(apply(&mut state, 4, append.record()).unwrap().outcome, Outcome::Then);
    assert!(state.index().open_scoped_hold(id, owner, &labels, Timestamp(40)).is_err());
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
        let error = candidate.execute(&[op], &[1; 16], None).unwrap_err();
        assert_eq!(error.code, "rtdb/write-scope");
        assert_eq!(error.category, AnomalyCategory::Forbidden);
        assert!(candidate.effects.is_empty());
    }
    let mut candidate = Candidate::new(state.index().clone(), Some(&write));
    candidate.execute(&[labelled_put(MAIN_BRANCH, 2, 1, Interval::ALL), delete(2)], &[1; 16], None).unwrap();
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
    let mut validation = request(9, Vec::new());
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
        Op::Declare { table: table(), change: Declaration::Index(IndexDefinition {
            name: "by_key".into(), column: "key".into(), additional_columns: Vec::new(),
        }) },
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
fn grouped_count_rejects_only_a_qualifying_bucket_over_capacity() {
    let mut state = TransactState::new(INCARNATION);
    let rule = GroupedCount {
        column: "key".into(), prefix_bytes: Some(1), equals: BTreeMap::from([("active".into(), vec![1])]),
        valid_at: None, n: 1,
    };
    let grouped = |id, key: &[u8], active| {
        let mut op = put(MAIN_BRANCH, id, "value");
        let Op::Put { rows, .. } = &mut op else { unreachable!() };
        rows[0].cells.insert("key".into(), Cell::Value(Value::Binary(key.to_vec())));
        rows[0].cells.insert("active".into(), Cell::Value(Value::Binary(vec![active])));
        op
    };
    assert_eq!(commit(&mut state, 1, &request(1, vec![Op::Declare {
        table: table(), change: Declaration::GroupedConstraint { name: "per-prefix".into(), rule },
    }])).outcome, Outcome::Then);
    assert_eq!(commit(&mut state, 2, &request(2, vec![
        grouped(1, b"a1", 1), grouped(2, b"b1", 1), grouped(3, b"a2", 0),
    ])).outcome, Outcome::Then);
    assert!(matches!(commit(&mut state, 3, &request(3, vec![grouped(4, b"a3", 1)])).outcome,
        Outcome::Refused { error } if error.code == "rtdb/grouped-count-constraint"));
    assert_eq!(commit(&mut state, 4, &request(4, vec![
        Op::Delete { branch: MAIN_BRANCH.into(), table: table(), ids: vec![EntityId::Integer(1)], valid: None },
        grouped(4, b"a3", 1),
    ])).outcome, Outcome::Then);
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 40), [2, 3, 4]);
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
        Op::Declare { table: table(), change: Declaration::Schema(docs_schema()) },
        put(MAIN_BRANCH, 3, "c"),
    ]));
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 70), [3]);
}

fn utf8() -> LogicalType { LogicalType::Scalar(rtdb_types::ScalarType::Utf8) }
fn int64() -> LogicalType { LogicalType::Scalar(rtdb_types::ScalarType::I64) }
fn schema(fields: &[(&str, LogicalType)]) -> StructFields<LogicalType> {
    fields.iter().map(|(name, ty)| ((*name).to_owned(), ty.clone())).collect()
}
fn docs_schema() -> StructFields<LogicalType> { schema(&[("_id", int64()), ("key", utf8())]) }
fn declare_schema(fields: StructFields<LogicalType>) -> Op { Op::Declare { table: table(), change: Declaration::Schema(fields) } }
fn put_cells(id: EntityId, cells: &[(&str, Value)]) -> Op {
    let cells = cells.iter().map(|(name, value)| ((*name).to_owned(), Cell::Value(value.clone()))).collect();
    let row = Row { id, valid: None, cells, custody_group: None, protected_columns: Vec::new(), custody_payload: None };
    Op::Put { branch: MAIN_BRANCH.into(), table: table(), rows: vec![row], if_absent: false }
}
fn refusal(receipt: Receipt) -> String {
    match receipt.outcome { Outcome::Refused { error } => error.code, outcome => panic!("expected a refusal, got {outcome:?}") }
}

#[test]
fn a_schema_declaration_names_a_non_null_key_and_leaves_temporal_columns_to_rtdb() {
    let mut state = TransactState::new(INCARNATION);
    for (position, fields, code) in [
        (1, schema(&[("key", utf8())]), "rtdb/schema-key"),
        (2, schema(&[("_id", int64().maybe()), ("key", utf8())]), "rtdb/schema-key"),
        (3, schema(&[("_id", LogicalType::Scalar(rtdb_types::ScalarType::F64))]), "rtdb/schema-key"),
        (4, schema(&[("_id", int64()), ("_valid_from", int64())]), "rtdb/reserved-field"),
        (5, schema(&[("_id", int64()), ("_system_to", int64())]), "rtdb/reserved-field"),
    ] {
        assert_eq!(refusal(commit(&mut state, position, &request(position as u8, vec![declare_schema(fields)]))), code);
    }
    assert!(state.index().definition(&table(), state.index().head().basis).is_none());
    commit(&mut state, 6, &request(6, vec![declare_schema(docs_schema())]));
    assert_eq!(refusal(commit(&mut state, 7, &request(7, vec![declare_schema(schema(&[("_id", utf8())]))]))), "rtdb/schema-key-change");
    assert_eq!(refusal(commit(&mut state, 8, &request(8, vec![declare_schema(schema(&[("_id", int64()), ("key", utf8().maybe())]))]))),
        "rtdb/schema-type-change");
    assert_eq!(commit(&mut state, 9, &request(9, vec![declare_schema(schema(&[("_id", int64()), ("n", int64())]))])).outcome, Outcome::Then);
    let definition = state.index().definition(&table(), state.index().head().basis).unwrap();
    assert_eq!(definition.schema.columns(), schema(&[("_id", int64()), ("key", utf8()), ("n", int64())]));
}

#[test]
fn closed_declared_tables_refuse_undeclared_columns_and_other_types_across_a_checkpoint() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_schema(docs_schema())]));
    let encoded = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&encoded, state.index().head()).unwrap();
    assert!(state.index().definition(&table(), basis(1)).unwrap().schema.is_declared());
    for (position, op, code) in [
        (2, put_cells(EntityId::Integer(1), &[("key", Value::Utf8("a".into())), ("extra", Value::I64(1))]), "rtdb/undeclared-column"),
        (3, put_cells(EntityId::Integer(1), &[("key", Value::I64(1))]), "rtdb/schema-type"),
        (4, put_cells(EntityId::String("one".into()), &[("key", Value::Utf8("a".into()))]), "rtdb/schema-type"),
    ] {
        assert_eq!(refusal(commit(&mut state, position, &request(position as u8, vec![op]))), code);
    }
    assert_eq!(commit(&mut state, 5, &request(5, vec![put_cells(EntityId::Integer(1), &[("key", Value::Null)]), put(MAIN_BRANCH, 2, "b")])).outcome,
        Outcome::Then);
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 50), [1, 2]);
    let encoded = state.encode_checkpoint().unwrap();
    let restored = TransactState::decode_checkpoint(&encoded, state.index().head()).unwrap();
    assert_eq!(restored.index().definition(&table(), basis(5)), state.index().definition(&table(), basis(5)));
}

#[test]
fn undeclared_tables_accept_new_columns_and_a_first_declaration_must_admit_their_rows() {
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![put(MAIN_BRANCH, 1, "a"), put_cells(EntityId::Integer(2), &[("other", Value::I64(2))])]));
    assert_eq!(visible(&state, MAIN_BRANCH, Layer::Merged, 10), [1, 2]);
    assert_eq!(refusal(commit(&mut state, 2, &request(2, vec![declare_schema(docs_schema())]))), "rtdb/schema-history");
    commit(&mut state, 3, &request(3, vec![declare_schema(schema(&[("_id", int64()), ("key", utf8()), ("other", int64())]))]));
    assert!(state.index().definition(&table(), basis(3)).unwrap().schema.is_declared());
}

#[tokio::test]
async fn an_empty_declared_table_resolves_its_key_and_temporal_columns() {
    use rtdb_columnar::MemoryBudget;
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot};
    use rtdb_sql::PrepareOptions;
    use rtdb_storage::{cache::BufferPool, object_store::MemoryStore};

    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![declare_schema(docs_schema())]));
    let memory = MemoryBudget::new(1 << 24);
    let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
    let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(10)).unwrap();
    let snapshot = rtdb_query::branch_snapshot(&view, &memory).unwrap();
    let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    rtdb_sql::prepare_in("SELECT _id, key, _valid_from, _valid_to, _system_from, _system_to FROM docs", &catalog,
        &PrepareOptions::default(), &memory).unwrap();
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
    assert_eq!(state.encode_checkpoint_at(basis(2)).unwrap_err().code, "rtdb/erased-under-view");
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
fn derived_custody_placement_uses_the_earlier_original_in_the_accepting_arm() {
    let group = [8; 16];
    let original_id = [9; 16];
    let derived_id = [10; 16];
    let inline = DerivedCustodyReservation {
        original_placement_id: original_id, link: [11; 16], review_sha256: [12; 32],
        carrier_sha256: [13; 32], link_sha256: [14; 32],
    };
    let original = custody_carrier(group, 1, original_id);
    let mut derived = custody_carrier(group, 2, derived_id);
    let Op::PlaceCustodyRow { row, placement, .. } = &mut derived else { unreachable!() };
    row.protected_columns.push("link".into());
    placement.link_column = Some("link".into());
    placement.commitments = Some(CustodyPlacementCommitments {
        carrier_sha256: inline.carrier_sha256, link_sha256: Some(inline.link_sha256),
    });
    placement.derived = Some(inline);

    let mut state = TransactState::new(INCARNATION);
    let create = Op::CreateCustodyGroup { group };
    for (at, ops) in [
        (1, vec![create.clone(), derived.clone(), original.clone()]),
        (2, vec![create.clone(), original.clone(), derived.clone(), derived.clone()]),
    ] {
        let receipt = commit_custody(&mut state, at, &request(at as u8, ops));
        assert!(matches!(receipt.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
        assert!(state.index().custody_inventory(group).is_none());
    }
    let publish = request(3, vec![create, put(MAIN_BRANCH, 99, "before"), original, derived.clone()]);
    let receipt = commit_custody(&mut state, 3, &publish);
    assert_eq!(receipt.outcome, Outcome::Then);
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, receipt.system_time).unwrap();
    let entries = view.custody_placements().collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    let original = entries.iter().find(|entry| entry.placement_id == original_id).unwrap();
    let derived_entry = entries.iter().find(|entry| entry.placement_id == derived_id).unwrap();
    let occurrence = derived_entry.occurrence.unwrap();
    assert_eq!(original.occurrence.unwrap().ordinal, 2);
    assert_eq!((derived_entry.enrolled, occurrence.written, occurrence.ordinal), (receipt.basis, receipt.basis, 3));
    assert_eq!(occurrence.receipt_key, receipt.key);
    assert_eq!(occurrence.reservation, derived_entry.reservation);
    assert!(occurrence.reservation.is_some());
    let (origin, read) = state.index().custody_read_occurrence(receipt.basis, group, occurrence.commitment(group)).unwrap();
    assert_eq!(origin, original.occurrence.unwrap().commitment(group));
    assert_eq!(read, occurrence);
    let replay = state.tx_status(&publish.key).unwrap();
    assert_eq!(replay, Receipt { replayed: true, ..receipt });

    let Op::PlaceCustodyRow { placement, .. } = &mut derived else { unreachable!() };
    placement.placement_id = [15; 16];
    placement.derived.as_mut().unwrap().link = [16; 16];
    let late = commit_custody(&mut state, 4, &request(4, vec![derived]));
    assert!(matches!(late.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
    let inventory_sha256 = state.index().custody_inventory(group).unwrap().sha256;
    let revoke = request(5, vec![Op::RevokeCustodyGroup { group, inventory_sha256 }]);
    assert_eq!(commit_custody(&mut state, 5, &revoke).outcome, Outcome::Then);
    assert!(state.index().custody_read_occurrence(basis(3), group, origin).is_err());
    let retried = state.resolve(&publish, 6, Timestamp(60), &[ROW_CUSTODY], &claims()).unwrap();
    assert!(matches!(retried, Resolution::Answer(receipt) if receipt == replay));
}

#[test]
fn moved_custody_placement_compares_relative_validity_at_its_own_ordered_clock() {
    let group = [8; 16];
    let mut state = TransactState::new(INCARNATION);
    let root = custody_carrier(group, 1, [9; 16]);
    assert_eq!(commit_custody(&mut state, 1, &request(1, vec![Op::CreateCustodyGroup { group }, root])).outcome, Outcome::Then);
    let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(10)).unwrap();
    let original = view.custody_placements().next().unwrap().occurrence.unwrap().commitment(group);
    let valid = ValidTime::new(ValidTimeBound::TransactionMicros(0), ValidTimeBound::Instant(Timestamp::MAX)).unwrap();
    let reservation = CustodyReservation {
        original, source: original, destination: [10; 16], link: [11; 16],
        target: CustodyTarget { branch: "copy".into(), table: table(), id: EntityId::Integer(2), valid },
        review_sha256: [12; 32], carrier_sha256: [13; 32], link_sha256: [14; 32],
    };
    let digest = reservation.commitment(INCARNATION, group);
    let reserve = Op::ReserveCustodyPlacement { group, reservation: reservation.clone() };
    let mut moved = custody_carrier(group, 2, reservation.destination);
    let Op::PlaceCustodyRow { branch, row, placement, .. } = &mut moved else { unreachable!() };
    *branch = "copy".into();
    row.valid = Some(valid);
    row.protected_columns.push("link".into());
    placement.reservation = Some(digest);
    placement.link_column = Some("link".into());
    placement.commitments = Some(CustodyPlacementCommitments {
        carrier_sha256: reservation.carrier_sha256, link_sha256: Some(reservation.link_sha256),
    });
    assert_eq!(commit_custody(&mut state, 2, &request(2, vec![fork("copy", 1)])).outcome, Outcome::Then);
    let early = commit_custody(&mut state, 3, &request(3, vec![reserve.clone(), moved.clone()]));
    assert!(matches!(early.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
    assert_eq!(commit_custody(&mut state, 4, &request(4, vec![reserve])).outcome, Outcome::Then);
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    let mut stale = moved.clone();
    let Op::PlaceCustodyRow { row, .. } = &mut stale else { unreachable!() };
    row.valid = Some(valid.resolve(Timestamp(40)).unwrap().into());
    let refused = commit_custody(&mut state, 5, &request(5, vec![stale]));
    assert!(matches!(refused.outcome, Outcome::Refused { error } if error.code == "rtdb/custody-placement"));
    let applied = commit_custody(&mut state, 6, &request(6, vec![moved]));
    assert_eq!(applied.outcome, Outcome::Then);
    let view = state.index().view("copy", At::Latest, Layer::Local, applied.system_time).unwrap();
    let enrolled = view.custody_placements().find(|entry| entry.placement_id == reservation.destination).unwrap();
    let occurrence = enrolled.occurrence.unwrap();
    assert_eq!(enrolled.enrolled, basis(4));
    assert_eq!(occurrence.target.valid.lower(), applied.system_time);
    assert_eq!(occurrence.target.valid.upper(), Timestamp::MAX);
    assert_eq!(occurrence.receipt_key, applied.key);
}

fn custody_carrier(group: CustodyGroupId, id: i64, placement_id: CustodyPlacementId) -> Op {
    Op::PlaceCustodyRow {
        branch: MAIN_BRANCH.into(), table: table(),
        row: Row {
            id: EntityId::Integer(id), valid: Some(Interval::ALL.into()), cells: StructFields::new(),
            custody_group: Some(group), protected_columns: vec!["carrier".into()],
            custody_payload: Some(CustodyPayload { binding: [3; 32], envelope: placement_id.to_vec() }),
        },
        placement: CustodyPlacement {
            placement_id, reservation: None, derived: None, carrier_column: "carrier".into(), link_column: None,
            commitments: Some(CustodyPlacementCommitments { carrier_sha256: [7; 32], link_sha256: None }),
        },
    }
}

#[test]
fn reviewed_custody_landing_orders_the_exact_reservation_and_keeps_ancestor_fences() {
    use rtdb_index::branches::{CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponsePart};
    use std::num::NonZeroU64;

    let group = [8; 16];
    let features = [ROW_CUSTODY, TRANSACT_REQUIRES];
    let transact = |state: &mut TransactState, at, request: &Transact| {
        let prepared = plan(state, request, at, at as i64 * 10, &features);
        apply(state, at, prepared.record()).unwrap()
    };
    let mut state = TransactState::new(INCARNATION);
    commit(&mut state, 1, &request(1, vec![put(MAIN_BRANCH, 99, "writer")]));
    commit(&mut state, 2, &request(2, vec![fork("author", 1), fork("other", 1)]));
    let mut original = custody_carrier(group, 1, [9; 16]);
    let Op::PlaceCustodyRow { branch, .. } = &mut original else { unreachable!() };
    *branch = "author".into();
    transact(&mut state, 3, &request(3, vec![Op::CreateCustodyGroup { group }, original]));
    let child = |child: &str, parent: &str, at| Op::Fork {
        child: child.into(), from: (parent.into(), basis(at)), writable: true, expires_at: None,
    };
    commit(&mut state, 4, &request(4, vec![child("review", "author", 3)]));
    commit(&mut state, 5, &request(5, vec![child("retained", "review", 4)]));
    commit(&mut state, 6, &request(6, vec![Op::DropBranch { branch: "author".into() }]));
    let original = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(60)).unwrap()
        .custody_placements().next().unwrap().occurrence.unwrap().commitment(group);
    let (source, _, _) = state.index().custody_review_at("review", basis(6), Timestamp(30), group, original).unwrap();
    assert!(state.index().custody_review_at("other", basis(6), Timestamp(30), group, original).is_err());
    let reservation = CustodyReservation {
        original, source: original, destination: [10; 16], link: [11; 16],
        target: CustodyTarget { branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(2), valid: Interval::ALL.into() },
        review_sha256: [12; 32], carrier_sha256: [13; 32], link_sha256: [14; 32],
    };
    let writer = Guard::Version {
        branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(99),
        valid_at: Some(Timestamp(10)), expect: Version::Written(basis(1)),
    };
    let reviewed_request = |key, op| {
        let mut request = request(key, vec![op]);
        request.require.push(REVIEWED_CUSTODY_PLACEMENT.into());
        request.requires.push(writer.clone());
        request
    };
    let mut reserve = reviewed_request(7, Op::ReserveReviewedCustodyPlacement {
        group, reservation: reservation.clone(), source: source.clone(), read_proof: vec![1],
    });
    reserve.requires.push(Guard::Unchanged(claims().seal(&ClaimScope {
        branch: source.branch.clone(), basis: source.basis, layer: Layer::Merged,
    }).unwrap()));
    let reserved = transact(&mut state, 7, &reserve);
    assert_eq!(reserved.outcome, Outcome::Then);
    let placement_commitment = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(70)).unwrap()
        .custody_placements().find(|entry| entry.placement_id == reservation.destination).unwrap().reservation.unwrap();
    let Op::PlaceCustodyRow { table, mut row, mut placement, .. } = custody_carrier(group, 2, reservation.destination)
        else { unreachable!() };
    row.protected_columns.push("link".into());
    placement.reservation = Some(placement_commitment);
    placement.link_column = Some("link".into());
    placement.commitments = Some(CustodyPlacementCommitments {
        carrier_sha256: reservation.carrier_sha256, link_sha256: Some(reservation.link_sha256),
    });
    let bypass = request(8, vec![Op::PlaceCustodyRow {
        branch: MAIN_BRANCH.into(), table: table.clone(), row: row.clone(), placement: placement.clone(),
    }]);
    assert!(matches!(transact(&mut state, 8, &bypass).outcome, Outcome::Refused { .. }));
    let landing = |key, reserve_key| reviewed_request(key, Op::LandCustodyBranch {
        branch: "review".into(), placements: vec![ReviewedCustodyPlacement {
            table: table.clone(), row: row.clone(), placement: placement.clone(), reserved_at: reserved.basis, reserve_key,
        }],
    });
    assert!(matches!(transact(&mut state, 9, &landing(9, vec![99; 16])).outcome, Outcome::Refused { .. }));
    let revoke = request(11, vec![Op::RevokeCustodyGroup {
        group, inventory_sha256: state.index().custody_inventory(group).unwrap().sha256,
    }]);
    let accept = landing(10, reserved.key.clone());
    let mut fenced_first = state.clone();
    assert_eq!(transact(&mut fenced_first, 11, &revoke).outcome, Outcome::Then);
    assert!(matches!(transact(&mut fenced_first, 12, &accept).outcome, Outcome::Refused { .. }));
    assert!(fenced_first.index().active_names(Timestamp(120)).any(|branch| branch == "review"));

    let applied = transact(&mut state, 10, &accept);
    assert_eq!(applied.outcome, Outcome::Then);
    assert!(matches!(transact(&mut state, 11, &revoke).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-inventory-changed"));
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    let root = state.index().view(MAIN_BRANCH, At::Exactly(applied.basis), Layer::Merged, Timestamp(110)).unwrap();
    let entry = root.custody_placements().find(|entry| entry.placement_id == reservation.destination).unwrap();
    assert_eq!(entry.reviewed.unwrap().0.receipt_key, reserved.key);
    assert_eq!(entry.occurrence.unwrap().receipt_key, applied.key);
    assert_eq!(entry.occurrence.unwrap().written, applied.basis);
    assert!(root.lineage().iter().any(|entry| entry.branch == "review" && entry.dropped == Some(applied.basis)));
    assert_eq!(state.tx_status(&reserved.key).unwrap(), Receipt { replayed: true, ..reserved });
    assert_eq!(state.tx_status(&applied.key).unwrap(), Receipt { replayed: true, ..applied });
    assert!(visible(&state, "retained", Layer::Merged, 110).contains(&1));
    let registration = CustodyReleaseRegistration {
        authority: CustodyCarrierAuthority {
            incarnation: INCARNATION, source_history: [15; 32], installation: [16; 16], registry: [17; 32], original_writer: [18; 32],
        },
        group, original, occurrence: original, carrier_receipt_sha256: [19; 32], expires_at: Timestamp(200),
        part: CustodyResponsePart {
            transport_history: [20; 16], producer: [21; 16], consumer: [22; 16], response: [23; 16], ordinal: NonZeroU64::MIN,
        },
    };
    let release = |phase| TrustedCustodyRelease {
        release: CustodyRelease { registration, phase },
        writer: CustodyWriterFence { table: table.clone(), id: EntityId::Integer(99), valid_at: Timestamp(10), written: basis(1) },
    }.into_request();
    assert_eq!(commit(&mut state, 12, &release(CustodyReleasePhase::Private)).outcome, Outcome::Then);
    assert_eq!(commit(&mut state, 13, &release(CustodyReleasePhase::Promoted)).outcome, Outcome::Then);
    let revoke = request(14, vec![Op::RevokeCustodyGroup {
        group, inventory_sha256: state.index().custody_inventory(group).unwrap().sha256,
    }]);
    assert_eq!(transact(&mut state, 14, &revoke).outcome, Outcome::Then);
    assert!(!visible(&state, "retained", Layer::Merged, 140).contains(&1));
    assert!(!visible(&state, MAIN_BRANCH, Layer::Merged, 140).contains(&2));
    assert!(!state.index().custody_releases_drained(group));
    let dispose = release(CustodyReleasePhase::Disposed { evidence_sha256: [24; 32] });
    assert_eq!(commit(&mut state, 15, &dispose).outcome, Outcome::Then);
    assert!(state.index().custody_releases_drained(group));
    assert!(matches!(state.resolve(&accept, 16, Timestamp(160), &features, &claims()).unwrap(),
        Resolution::Answer(receipt) if receipt.replayed && receipt.basis == basis(10)));
}

fn commit_custody(state: &mut TransactState, at: u64, request: &Transact) -> Receipt {
    let prepared = plan(state, request, at, at as i64 * 10, &[ROW_CUSTODY]);
    apply(state, at, prepared.record()).unwrap()
}

#[test]
fn custody_read_contributor_follows_the_enrolled_write_through_branch_inheritance() {
    let group = [8; 16];
    let mut state = TransactState::new(INCARNATION);
    let carrier = custody_carrier(group, 1, [9; 16]);
    let Op::PlaceCustodyRow { row, .. } = &carrier else { unreachable!() };
    let duplicate = Op::Put { branch: MAIN_BRANCH.into(), table: table(), rows: vec![row.clone()], if_absent: false };
    assert_eq!(commit_custody(&mut state, 1, &request(1, vec![
        Op::CreateCustodyGroup { group }, carrier, duplicate,
    ])).outcome, Outcome::Then);
    assert_eq!(commit_custody(&mut state, 2, &request(2, vec![fork("child", 1)])).outcome, Outcome::Then);
    let checkpoint = state.encode_checkpoint().unwrap();
    let state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    let view = state.index().view("child", At::Latest, Layer::Merged, Timestamp(20)).unwrap();
    let table = table();
    let history = view.history(&table, &EntityId::Integer(1));
    assert_eq!(history.len(), 2);
    let entry = view.custody_placements().next().unwrap();
    let occurrence = entry.occurrence.unwrap();
    let commitment = occurrence.commitment(group);
    assert_eq!(view.custody_event_contributor(&table, history[0]), Some((group, commitment)));
    assert_eq!(view.custody_event_contributor(&table, history[1]), None);
    let (original, retained) = state.index().custody_read_occurrence(view.cut().basis, group, commitment).unwrap();
    assert_eq!(original, commitment);
    assert_eq!(retained.target.branch, MAIN_BRANCH);
    assert_eq!(retained.ordinal, 1);
    assert_eq!(state.tx_status(&retained.receipt_key).unwrap().basis, basis(1));
}

#[test]
fn custody_reservation_activation_and_release_share_the_revocation_order() {
    use std::num::NonZeroU64;
    use rtdb_index::branches::{CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponsePart};

    let group = [8; 16];
    let features = [LINEARIZABLE, ROW_CUSTODY, TRANSACT_REQUIRES];
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
                placement_id: stage, reservation, derived: None, carrier_column: "carrier".into(), link_column: link_sha256.map(|_| "link".into()),
                commitments: Some(CustodyPlacementCommitments { carrier_sha256, link_sha256 }),
            },
        }
    };
    let valid = Interval::new(Timestamp(0), Timestamp(1_000)).unwrap();
    let original_id = [11; 16];
    assert_eq!(write(&mut state, 1, &request(1, vec![Op::CreateCustodyGroup { group }, put(MAIN_BRANCH, 99, "writer"),
        place(MAIN_BRANCH, original_id, None, valid, [7; 32], None)])).outcome, Outcome::Then);
    let occurrence_at = |state: &TransactState, stage| {
        let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(30)).unwrap();
        view.custody_placements().find(|entry| entry.placement_id == stage).unwrap().occurrence.unwrap().clone()
    };
    let original = occurrence_at(&state, original_id).commitment(group);
    let plan_before_reserving = state.index().custody_inventory(group).unwrap().sha256;
    let reservation = CustodyReservation {
        original, source: original, destination: [12; 16], link: [13; 16],
        target: CustodyTarget { branch: "copy".into(), table: table(), id: EntityId::Integer(1), valid: valid.into() },
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
        group, original, occurrence: active.commitment(group), carrier_receipt_sha256: [31; 32], expires_at: Timestamp(120),
        part: CustodyResponsePart {
            transport_history: [19; 16], producer: [20; 16], consumer: [21; 16], response: [22; 16], ordinal: NonZeroU64::MIN,
        },
    };
    assert!(state.index().check_custody_carrier_receipt(registration, first.basis, &first.key).is_ok());
    assert!(state.index().check_custody_carrier_receipt(registration, first.basis, &[31; 16]).is_err());
    assert!(state.index().check_custody_carrier_receipt(registration, basis(1), &[1; 16]).is_err());
    let release = |phase| TrustedCustodyRelease {
        release: CustodyRelease { registration, phase },
        writer: CustodyWriterFence { table: table(), id: EntityId::Integer(99), valid_at: Timestamp(20), written: basis(1) },
    }.into_request();
    let stage = release(CustodyReleasePhase::Private);
    let mut displaced = state.clone();
    let mut replacement = put(MAIN_BRANCH, 99, "replacement");
    let Op::Put { rows, .. } = &mut replacement else { unreachable!() };
    rows[0].valid = Some(Interval::new(Timestamp(80), Timestamp::MAX).unwrap().into());
    assert_eq!(commit(&mut displaced, 8, &request(16, vec![replacement])).outcome, Outcome::Then);
    let view = displaced.index().view(MAIN_BRANCH, At::Latest, Layer::Local, Timestamp(90)).unwrap();
    assert_eq!(view.row(&table(), &EntityId::Integer(99), Timestamp(20)).unwrap().cut.basis, basis(1));
    assert!(matches!(commit(&mut displaced, 9, &stage).outcome,
        Outcome::Refused { error } if error.code == REQUIREMENT_FAILED));
    assert!(displaced.index().custody_release_phase(registration).unwrap().is_none());
    assert_eq!(commit(&mut state, 8, &stage).outcome, Outcome::Then);
    assert!(state.tx_status(&stage.key).is_err());
    assert_eq!(commit(&mut state, 9, &release(CustodyReleasePhase::Promoted)).outcome, Outcome::Then);
    let inventory = state.index().custody_inventory(group).unwrap();
    let revoke = request(10, vec![stale(inventory.sha256)]);
    let revocation = write(&mut state, 10, &revoke);
    assert_eq!(revocation.outcome, Outcome::Then);
    assert!(!state.index().custody_releases_drained(group));
    assert!(matches!(commit(&mut state, 11, &release(CustodyReleasePhase::Cancelled)).outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-release"));
    let terminal = CustodyReleasePhase::Disposed { evidence_sha256: [23; 32] };
    let dispose = release(terminal);
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

#[test]
fn custody_admission_orders_the_whole_part_and_preserves_its_drain_after_restart() {
    use std::num::NonZeroU64;
    use rtdb_index::branches::{CustodyCarrierAuthority, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponsePart};

    let features = [LINEARIZABLE, ROW_CUSTODY, TRANSACT_REQUIRES];
    let write = |state: &mut TransactState, at, request: &Transact| {
        let prepared = plan(state, request, at, at as i64 * 10, &features);
        apply(state, at, prepared.record()).unwrap()
    };
    let writer = CustodyWriterFence { table: table(), id: EntityId::Integer(99), valid_at: Timestamp(10), written: basis(1) };
    let authority = CustodyCarrierAuthority {
        incarnation: INCARNATION, source_history: [1; 32], installation: [2; 16], registry: [3; 32], original_writer: [4; 32],
    };
    let part = CustodyResponsePart {
        transport_history: [5; 16], producer: [6; 16], consumer: [7; 16], response: [8; 16], ordinal: NonZeroU64::MIN,
    };
    let phase = |registration, phase| TrustedCustodyRelease { release: CustodyRelease { registration, phase }, writer: writer.clone() }.into_request();
    let mut state = TransactState::new(INCARNATION);
    assert_eq!(write(&mut state, 1, &request(1, vec![put(MAIN_BRANCH, 99, "writer")])).outcome, Outcome::Then);
    let mut registrations = Vec::new();
    for (at, group) in [(2, [11; 16]), (3, [12; 16])] {
        let placement_id = [at as u8; 16];
        let placement = Op::PlaceCustodyRow {
            branch: MAIN_BRANCH.into(), table: table(),
            row: Row { id: EntityId::Integer(at as i64), valid: Some(Interval::ALL.into()), cells: StructFields::new(),
                custody_group: Some(group), custody_payload: Some(CustodyPayload { binding: [13; 32], envelope: vec![at as u8] }),
                protected_columns: vec!["carrier".into()] },
            placement: CustodyPlacement { placement_id, reservation: None, derived: None, carrier_column: "carrier".into(),
                link_column: None, commitments: Some(CustodyPlacementCommitments { carrier_sha256: [14; 32], link_sha256: None }) },
        };
        let carrier = write(&mut state, at, &request(at as u8, vec![Op::CreateCustodyGroup { group }, placement]));
        assert_eq!(carrier.outcome, Outcome::Then);
        let view = state.index().view(MAIN_BRANCH, At::Latest, Layer::Merged, carrier.system_time).unwrap();
        let original = view.custody_placements().find(|entry| entry.placement_id == placement_id).unwrap().occurrence.unwrap().commitment(group);
        registrations.push(CustodyReleaseRegistration { authority, group, original, occurrence: original, part,
            carrier_receipt_sha256: [at as u8; 32], expires_at: Timestamp(1_000) });
    }
    let admission = CustodyResponseAdmission { registrations: registrations.clone(), frame_sha256: [15; 32],
        read_proofs_sha256: Some([16; 32]), selected_basis: basis(3), key_generation: 7 };
    let admit = |admission| TrustedCustodyCommand::Admit(TrustedCustodyAdmission { admission, writer: writer.clone() }).into_request().unwrap();
    let exact = admit(admission.clone());
    assert!(Transact::decode_canonical_request(exact.canonical_request().unwrap().as_bytes()).is_err());
    for (at, registration) in [(4, registrations[0]), (6, registrations[1])] {
        assert_eq!(write(&mut state, at, &phase(registration, CustodyReleasePhase::Private)).outcome, Outcome::Then);
        let mut private = state.clone();
        assert!(matches!(write(&mut private, at + 1, &exact).outcome, Outcome::Refused { .. }));
        assert_eq!(write(&mut state, at + 1, &phase(registration, CustodyReleasePhase::Promoted)).outcome, Outcome::Then);
    }
    let revoke = |state: &TransactState, key, group| {
        let inventory_sha256 = state.index().custody_inventory(group).unwrap().sha256;
        request(key, vec![Op::RevokeCustodyGroup { group, inventory_sha256 }])
    };
    let mut fence_first = state.clone();
    let close = revoke(&fence_first, 20, registrations[1].group);
    assert_eq!(write(&mut fence_first, 8, &close).outcome, Outcome::Then);
    assert!(matches!(write(&mut fence_first, 9, &exact).outcome, Outcome::Refused { .. }));

    let terminal = CustodyReleasePhase::Disposed { evidence_sha256: [17; 32] };
    let mut disposed_first = state.clone();
    assert_eq!(write(&mut disposed_first, 8, &phase(registrations[1], terminal)).outcome, Outcome::Then);
    assert!(matches!(write(&mut disposed_first, 9, &exact).outcome, Outcome::Refused { .. }));
    let expired = plan(&state, &exact, 8, 1_000, &features);
    assert!(matches!(apply(&mut state.clone(), 8, expired.record()).unwrap().outcome,
        Outcome::Refused { error } if error.code == "rtdb/custody-release-cutoff"));

    let accepted = write(&mut state, 8, &exact);
    assert_eq!(accepted.outcome, Outcome::Then);
    assert!(!accepted.replayed);
    assert!(state.tx_status(&exact.key).is_err());
    for (at, registration) in [(9, registrations[0]), (10, registrations[1])] {
        let close = revoke(&state, at as u8, registration.group);
        assert_eq!(write(&mut state, at, &close).outcome, Outcome::Then);
        assert!(!state.index().custody_releases_drained(registration.group));
    }
    let checkpoint = state.encode_checkpoint().unwrap();
    let mut state = TransactState::decode_checkpoint(&checkpoint, state.index().head()).unwrap();
    assert!(matches!(state.resolve(&exact, 11, Timestamp(110), &features, &claims()).unwrap(),
        Resolution::Answer(receipt) if receipt.replayed && receipt.basis == accepted.basis));
    let changed_frame = admit(CustodyResponseAdmission { frame_sha256: [18; 32], ..admission.clone() });
    let changed_inventory = admit(CustodyResponseAdmission { registrations: vec![registrations[1]], ..admission });
    for changed in [changed_frame, changed_inventory] {
        assert_eq!(changed.key, exact.key);
        assert_eq!(state.resolve(&changed, 11, Timestamp(110), &features, &claims()).err().unwrap().code, "rtdb/idempotency-key");
    }
    for (at, registration) in [(11, registrations[0]), (12, registrations[1])] {
        assert!(!state.index().custody_releases_drained(registration.group));
        assert_eq!(write(&mut state, at, &phase(registration, terminal)).outcome, Outcome::Then);
        assert_eq!(state.index().custody_release_phase(registration).unwrap(), Some(terminal));
        assert!(state.index().custody_releases_drained(registration.group));
    }
}

#[test]
fn classic_receipt_import_preserves_fingerprints_and_separates_equal_keys_by_namespace() {
    let mut original = TransactState::new(INCARNATION);
    let shared_key = request(1, vec![declare_visibility(), labelled_put(MAIN_BRANCH, 1, 1, Interval::ALL)]);
    let plain = commit(&mut original, 1, &shared_key);
    let other = commit(&mut original, 2, &request(2, vec![labelled_put(MAIN_BRANCH, 3, 1, Interval::ALL)]));
    let labels = LabelScope { read: BTreeSet::from([vec![1]]), write: BTreeSet::from([vec![1]]) };
    let scoped = ScopedTransact { labels: &labels, receipt_namespace: [8; 32] };
    let scoped_request = request(1, vec![labelled_put(MAIN_BRANCH, 2, 1, Interval::ALL)]);
    let Resolution::Append(append) = original.resolve_scoped(&scoped_request, 3, Timestamp(30), MULTI, &claims(), scoped).unwrap() else {
        panic!("expected a scoped receipt");
    };
    let scoped_receipt = apply(&mut original, 3, append.record()).unwrap();
    for (namespace, receipt) in [(None, &plain), (None, &other), (Some(scoped.receipt_namespace), &scoped_receipt)] {
        let position = receipt.basis.position() as i64;
        original.install_receipt_position(namespace, receipt, InstalledReceipt {
            source: SourcePosition(position), replica: ReplicaPosition(position * 2), term: Term(1),
        }).unwrap();
    }

    let mut candidate = TransactState::new(INCARNATION);
    let mut index = candidate.index().clone();
    for position in 1..=3 {
        index.advance_classic(BranchCut { basis: basis(position + 100), system_time: Timestamp(position as i64 * 10) }).unwrap();
    }
    candidate.replace_applied_index(index).unwrap();
    assert!(candidate.import_classic_receipts(&original, |_| Some(basis(101))).is_err());
    assert!(!candidate.has_receipts());
    assert_eq!(candidate.import_classic_receipts(&original, |position| Some(basis(position.source.0 as u64 + 100))).unwrap(), 3);
    for namespace in [None, Some(scoped.receipt_namespace)] {
        let from = &original.receipts[&namespace][&shared_key.key];
        let installed = &candidate.receipts[&namespace][&shared_key.key];
        assert_eq!(installed.fingerprint, from.fingerprint);
        assert_eq!(installed.receipt.outcome, from.receipt.outcome);
        assert_eq!(installed.receipt.basis, basis(from.receipt.basis.position() + 100));
        assert!(installed.installed.is_none());
        assert!(from.installed.is_some());
    }
}

fn every_shape() -> Transact {
    let cells = StructFields::from([
        ("z".into(), Cell::Value(Value::Struct(StructFields::from([
            ("b".into(), Value::F64(-0.0)), ("a".into(), Value::List(vec![Value::F32(1.5), Value::Null, Value::Utf8("x".into())].into())),
        ])))),
        ("a".into(), Cell::Value(Value::Map { entries: vec![(Value::Keyword(rtdb_types::Keyword::from_name("k")), Value::Binary(vec![1, 2]))], sorted: false })),
        ("d".into(), Cell::Value(Value::calendar_date(rtdb_types::CivilDate::parse_iso("+12000-01-02").unwrap(), rtdb_types::DateUnit::Day))),
        ("t".into(), Cell::TxTime),
    ]);
    let relative = ValidTime::new(ValidTimeBound::TransactionMicros(-5), ValidTimeBound::Instant(Timestamp::MAX)).unwrap();
    let row = Row {
        id: EntityId::Binary(vec![9; 3]), valid: Some(relative), cells, custody_group: Some([4; 16]),
        protected_columns: vec!["secret".into()], custody_payload: Some(CustodyPayload { binding: [5; 32], envelope: vec![6; 4] }),
    };
    Transact {
        key: vec![1; 16], require: vec![TRANSACT_REQUIRES.into(), LINEARIZABLE.into()],
        window: Window { not_before: Some(Timestamp(3)), before: None },
        guards: vec![Guard::Version { branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Uuid([7; 16]), valid_at: None, expect: Version::Written(basis(2)) }],
        ops: vec![
            Op::Put { branch: MAIN_BRANCH.into(), table: table(), rows: vec![row], if_absent: true },
            Op::Delete { branch: MAIN_BRANCH.into(), table: table(), ids: vec![EntityId::Integer(-1)], valid: Some(ValidTime::Literal(Interval::from_micros(1, 2).unwrap())) },
            Op::Declare { table: table(), change: Declaration::Index(IndexDefinition { name: "by".into(), column: "a".into(), additional_columns: Vec::new() }) },
            Op::Store(StoreAppend { writes: vec![StoreWrite { key: vec![0; 16], value: None, key_stamp: Some(0), value_stamp: None }], bounds: vec![StoreBound { key: vec![1; 16], delta: -1, min: 0, max: 9 }], derives: Vec::new(), window: Window::default() }),
        ],
        otherwise: vec![Op::DropBranch { branch: "child".into() }],
        requires: Vec::new(), custody_request_digest: None, request_memory: None,
    }
}

#[test]
fn a_canonical_request_round_trips_every_field_shape_and_encodes_deterministically() {
    let request = every_shape();
    let canonical = request.canonical_request().unwrap();
    assert_eq!(request.canonical_request().unwrap(), canonical);
    let decoded = canonical.decode().unwrap();
    assert_eq!(decoded.canonical_request().unwrap(), canonical);
    assert_eq!(decoded.require, [LINEARIZABLE, TRANSACT_REQUIRES]);
    let Op::Put { rows, .. } = &decoded.ops[0] else { panic!("expected a Put"); };
    assert_eq!(rows[0].cells.keys().collect::<Vec<_>>(), ["a", "d", "t", "z"]);
    assert_eq!(rows[0].valid, request.ops.iter().find_map(|op| match op { Op::Put { rows, .. } => rows[0].valid, _ => None }));
    assert!(matches!(decoded.ops[3], Op::Store(_)));
}

#[test]
fn a_request_that_decodes_but_is_not_canonical_is_refused() {
    let mut request = request(1, Vec::new());
    request.require = vec!["a".into()];
    let canonical = request.canonical_request().unwrap();
    let bytes = canonical.as_bytes();
    // The key's length and bytes, then the one feature: its count, its length, "a".
    assert_eq!(&bytes[17..20], [1, 1, b'a']);
    let duplicated = [&bytes[..17], &[2, 1, b'a', 1, b'a'], &bytes[20..]].concat();
    let error = Transact::decode_canonical_request(&duplicated).unwrap_err();
    assert_eq!(error.message.as_deref(), Some("The logged transaction request is not canonical"));
    assert!(Transact::decode_canonical_request(&[bytes, &[0]].concat()).is_err());
    assert!(Transact::decode_canonical_request(serde_json::to_string(&request).unwrap().as_bytes()).is_err());
}

#[test]
fn a_transaction_record_round_trips_with_its_refusal_context_and_encodes_deterministically() {
    let request = every_shape();
    let error = Anomaly::incorrect("rtdb/test", "refused").with_context("detail", serde_json::json!({"n": 1, "s": [true, null]}));
    let record = TransactionRecord::Tx(ResolvedTransact {
        fingerprint: request.logical_sha256().unwrap(),
        receipt: Receipt {
            key: request.key.clone(), basis: basis(4), system_time: Timestamp(40), outcome: Outcome::Refused { error: error.clone() },
            replayed: false, erasure: Some(Erasure::Armed { unreadable_by: Timestamp(50) }), decision_clock: None,
        },
        effects: Vec::new(), before: None, receipt_namespace: Some([3; 32]), admitted_at: Timestamp(39),
        workload: None, workload_digest: None, request: Some(request.canonical_request().unwrap()),
    });
    let bytes = record.encode().unwrap();
    assert_eq!(record.encode().unwrap(), bytes);
    assert_eq!(bytes[0], RECORD_VERSION);
    let TransactionRecord::Tx(decoded) = TransactionRecord::decode(&bytes).unwrap() else { panic!("expected a Tx") };
    assert_eq!(decoded.receipt.outcome, Outcome::Refused { error });
    assert_eq!(decoded.canonical_request(), Some(&request.canonical_request().unwrap()));
    assert_eq!(TransactionRecord::Tx(decoded).encode().unwrap(), bytes);
    for terminal in [TransactionRecord::Settle { tx_lsn: 300 }, TransactionRecord::Refuse { tx_lsn: 1 }] {
        let bytes = terminal.encode().unwrap();
        assert_eq!(TransactionRecord::decode(&bytes).unwrap().encode().unwrap(), bytes);
    }
    let json = serde_json::to_vec(&(2u8, TransactionRecord::Settle { tx_lsn: 1 })).unwrap();
    assert_eq!(TransactionRecord::decode(&json).unwrap_err().code, "rtdb/transaction-version");
}
