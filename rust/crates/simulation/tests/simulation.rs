use rtdb_simulation::{Bug, Config, Summary, Violation, run, run_with};

fn env(name: &str, default: u64) -> u64 { std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default) }

fn config(seed: u64, steps: usize, configure: &impl Fn(&mut Config)) -> Config {
    let mut config = Config::new(seed, steps);
    configure(&mut config);
    config
}

/// Panics with the first violation's seed and trace.
fn sweep(seeds: std::ops::Range<u64>, steps: usize, configure: impl Fn(&mut Config)) -> Summary {
    let mut total = Summary::default();
    for seed in seeds {
        let summary = run_with(config(seed, steps, &configure)).unwrap_or_else(|violation| panic!("{violation}"));
        total.steps += summary.steps;
        total.committed += summary.committed;
        total.acknowledged += summary.acknowledged;
        total.claims += summary.claims;
        total.opened += summary.opened;
        total.faults += summary.faults;
        total.stale_appends += summary.stale_appends;
    }
    total
}

fn first_violation(seeds: std::ops::Range<u64>, steps: usize, configure: impl Fn(&mut Config)) -> Option<Violation> {
    seeds.into_iter().find_map(|seed| run_with(config(seed, steps, &configure)).err())
}

#[test]
fn a_seed_replays_exactly() {
    assert_eq!(run(7, 2_000).unwrap(), run(7, 2_000).unwrap());
    let broken = |config: &mut Config| config.bug = Some(Bug::AppendIgnoresPromise);
    let violation = first_violation(0..100, 2_000, broken).expect("the broken variant is caught");
    let replayed = run_with(config(violation.seed, 2_000, &broken)).unwrap_err();
    assert_eq!((replayed.step, &replayed.message, &replayed.trace), (violation.step, &violation.message, &violation.trace));
}

#[test]
fn two_thousand_seeds_of_two_thousand_steps_keep_the_single_writer_safe() {
    let total = sweep(0..2_000, 2_000, |_| ());
    assert!(total.acknowledged > 10_000 && total.opened > 4_000, "the sweep commits and fails over: {total:?}");
}

#[test]
fn requests_a_server_decides_after_newer_ones_cannot_undo_a_commit() {
    let total = sweep(10_000..11_000, 2_000, |config| config.stale_redelivery = true);
    assert!(total.stale_appends > 0 && total.acknowledged > 0, "the sweep must exercise stale writes and commit data: {total:?}");
}

#[test]
fn a_server_accepting_appends_without_the_promise_is_caught() {
    let violation = first_violation(0..2_000, 2_000, |config| config.bug = Some(Bug::AppendIgnoresPromise)).expect("the checker catches a server that ignores its promise");
    assert!(violation.to_string().contains(&format!("seed {}", violation.seed)));
}

#[test]
fn recovery_by_last_entry_epoch_is_caught() {
    first_violation(0..2_000, 2_000, |config| config.bug = Some(Bug::WinnerByLastEntryEpoch)).expect("the checker catches recovery that ignores the synced epoch");
}

/// `RTDB_SIM_SEEDS=100000 RTDB_SIM_STEPS=10000 cargo test -p rtdb-simulation --release -- --ignored`
#[test]
#[ignore]
fn many_seeds_keep_the_single_writer_safe() {
    let (seeds, steps) = (env("RTDB_SIM_SEEDS", 20_000), env("RTDB_SIM_STEPS", 5_000) as usize);
    let first = env("RTDB_SIM_FIRST_SEED", 0);
    let total = sweep(first..first + seeds, steps, |config| config.stale_redelivery = config.seed % 2 == 1);
    eprintln!("{total:?}");
}
