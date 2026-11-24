//! Decision-path timing (W14, P6). Run with
//! `cargo test -p rtdb-tx --release transact::perf -- --ignored --nocapture`.
//!
//! Commits distinct-key single-row puts and reports, as the table and the history grow:
//! the time per decision (`resolve`) and per apply on the sequential fold, and the time
//! per commit through [`TransactPipeline`] (decide in groups, acknowledge each group,
//! no re-apply). The write path's target is a flat curve at 50 µs or less per commit.
use std::time::{Duration, Instant};

use rtdb_types::rtdb_v1::ClaimSealer;

use super::*;

const INCARNATION: [u8; 16] = [7; 16];

fn key(n: u64) -> Vec<u8> {
    let mut key = vec![0u8; 16];
    key[..8].copy_from_slice(&n.to_be_bytes());
    key
}

fn put(n: u64) -> Transact {
    let row = Row {
        id: EntityId::Integer(n as i64),
        valid: None,
        cells: StructFields::from([("v".into(), Cell::Value(Value::Utf8(format!("row-{n}"))))]),
    };
    Transact {
        key: key(n),
        require: Vec::new(),
        window: Window::default(),
        guards: Vec::new(),
        ops: vec![Op::Put { branch: MAIN_BRANCH.into(), table: TableRef::new("public", "bench"), rows: vec![row], if_absent: false }],
        otherwise: Vec::new(),
    }
}

/// Mean (resolve, apply) time over `sample` commits made after `prefix` commits.
fn measure(state: &mut TransactState, from: u64, sample: u64, claims: &ClaimSealer) -> (Duration, Duration) {
    let (mut resolving, mut applying) = (Duration::ZERO, Duration::ZERO);
    for n in from..from + sample {
        let request = put(n);
        let started = Instant::now();
        let Resolution::Append(prepared) = state.resolve(&request, n, Timestamp(n as i64 * 10), &[LINEARIZABLE], claims).unwrap() else {
            panic!("expected an append");
        };
        resolving += started.elapsed();
        let record = prepared.record;
        let started = Instant::now();
        state.apply(n, record).unwrap().expect("an untimed commit publishes its receipt");
        applying += started.elapsed();
    }
    (resolving / sample as u32, applying / sample as u32)
}

const SIZES: [u64; 5] = [1_000, 5_000, 20_000, 50_000, 100_000];

#[test]
#[ignore = "timing; run explicitly in release mode"]
fn pipeline_commit_cost_as_the_table_grows() {
    const GROUP: u64 = 64;
    let claims = ClaimSealer::new(INCARNATION, [3; 32]);
    let mut pipeline = TransactPipeline::new(TransactState::new(INCARNATION), 1, GROUP as usize).unwrap();
    let mut committed = 0u64;
    let mut commit_group = |pipeline: &mut TransactPipeline, from: u64| {
        let requests = (from..from + GROUP).map(put).collect::<Vec<_>>();
        let decisions = pipeline.decide_batch(&requests, Timestamp(from as i64 * 10), &[LINEARIZABLE], &claims);
        assert!(decisions.iter().all(|decision| matches!(decision, Ok(Decision::Append { .. }))));
        let released = pipeline.acknowledge(pipeline.next_position() - 1).unwrap();
        assert_eq!(released.len(), GROUP as usize);
    };
    println!("rows_before_sample pipeline_us_per_commit");
    for target in SIZES {
        while committed < target {
            commit_group(&mut pipeline, committed + 1);
            committed += GROUP;
        }
        let started = Instant::now();
        let groups = 8;
        for _ in 0..groups {
            commit_group(&mut pipeline, committed + 1);
            committed += GROUP;
        }
        let per_commit = started.elapsed() / (groups * GROUP) as u32;
        println!("{target} {:.1}", per_commit.as_secs_f64() * 1e6);
    }
}

#[test]
#[ignore = "timing; run explicitly in release mode"]
fn decision_cost_as_the_table_grows() {
    let claims = ClaimSealer::new(INCARNATION, [3; 32]);
    let mut state = TransactState::new(INCARNATION);
    let mut committed = 0u64;
    println!("rows_before_sample resolve_us apply_us");
    for target in SIZES {
        while committed < target {
            committed += 1;
            let Resolution::Append(prepared) = state
                .resolve(&put(committed), committed, Timestamp(committed as i64 * 10), &[LINEARIZABLE], &claims)
                .unwrap() else { panic!("expected an append") };
            state.apply(committed, prepared.record).unwrap();
        }
        let (resolve, apply) = measure(&mut state, committed + 1, 200, &claims);
        committed += 200;
        println!("{target} {:.1} {:.1}", resolve.as_secs_f64() * 1e6, apply.as_secs_f64() * 1e6);
    }
}
