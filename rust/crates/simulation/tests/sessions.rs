use rtdb_simulation::Violation;
use rtdb_simulation::sessions::{Bug, Config, Summary, run_with};

fn env(name: &str, default: u64) -> u64 { std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default) }

fn config(seed: u64, steps: usize, bug: Option<Bug>) -> Config { Config { bug, ..Config::new(seed, steps) } }

/// Panics with the first violation's seed and trace.
fn sweep(seeds: std::ops::Range<u64>, steps: usize) -> Summary {
    let mut total = Summary::default();
    for seed in seeds {
        let summary = run_with(config(seed, steps, None)).unwrap_or_else(|violation| panic!("{violation}"));
        total.steps += summary.steps;
        total.sessions += summary.sessions;
        total.backends += summary.backends;
        total.succeeded += summary.succeeded;
        total.unknown += summary.unknown;
        total.failed += summary.failed;
        total.faults += summary.faults;
        total.greeting_faults += summary.greeting_faults;
        total.prepared_reads += summary.prepared_reads;
        total.resent += summary.resent;
    }
    total
}

fn first_violation(seeds: std::ops::Range<u64>, steps: usize, bug: Bug) -> Option<Violation> {
    seeds.into_iter().find_map(|seed| run_with(config(seed, steps, Some(bug))).err())
}

#[test]
fn a_session_seed_replays_exactly() {
    for seed in 0..20 { assert_eq!(run_with(Config::new(seed, 200)).unwrap(), run_with(Config::new(seed, 200)).unwrap()); }
    let violation = first_violation(0..200, 200, Bug::NodeIgnoresKeys).expect("the broken variant is caught");
    let replayed = run_with(config(violation.seed, 200, Some(Bug::NodeIgnoresKeys))).unwrap_err();
    assert_eq!((replayed.step, &replayed.message, &replayed.trace), (violation.step, &violation.message, &violation.trace));
}

#[test]
fn sessions_over_failing_backends_run_every_command_at_most_once() {
    let total = sweep(0..300, 200);
    eprintln!("{total:?}");
    assert!(total.greeting_faults > 10 && total.prepared_reads > 100, "{total:?}");
    assert!(total.resent > 1_000 && total.unknown > 1_000 && total.failed > 100, "the sweep resends, loses and refuses commands: {total:?}");
}

#[test]
fn a_backend_that_ignores_keys_is_caught() {
    let violation = first_violation(0..300, 200, Bug::NodeIgnoresKeys).expect("the checker catches a resent command run twice");
    eprintln!("{violation}");
    assert!(violation.to_string().contains(&format!("seed {}", violation.seed)));
}

#[test]
fn a_backend_answering_a_repeat_unlike_the_original_is_caught() {
    let violation = first_violation(0..300, 200, Bug::RepeatAnsweredDifferently).expect("the checker catches a reply that is not the command's own");
    eprintln!("{violation}");
}

/// `RTDB_SIM_SEEDS=10000 RTDB_SIM_STEPS=1000 cargo test -p rtdb-simulation --test sessions -- --ignored`
#[test]
#[ignore]
fn many_seeds_run_every_command_at_most_once() {
    let (seeds, steps) = (env("RTDB_SIM_SEEDS", 3_000), env("RTDB_SIM_STEPS", 500) as usize);
    let first = env("RTDB_SIM_FIRST_SEED", 0);
    eprintln!("{:?}", sweep(first..first + seeds, steps));
}
