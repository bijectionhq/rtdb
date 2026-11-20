use rtdb_simulation::Violation;
use rtdb_simulation::storage::{Bug, Config, Summary, run_with};

fn env(name: &str, default: u64) -> u64 { std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default) }

fn config(seed: u64, steps: usize, bug: Option<Bug>) -> Config { Config { bug, ..Config::new(seed, steps) } }

/// Panics with the first violation's seed and trace.
fn sweep(seeds: std::ops::Range<u64>, steps: usize) -> Summary {
    let mut total = Summary::default();
    for seed in seeds {
        let summary = run_with(config(seed, steps, None)).unwrap_or_else(|violation| panic!("{violation}"));
        total.steps += summary.steps;
        total.writes += summary.writes;
        total.flushes += summary.flushes;
        total.compactions += summary.compactions;
        total.releases += summary.releases;
        total.forks += summary.forks;
        total.past_forks += summary.past_forks;
        total.reaper_passes += summary.reaper_passes;
        total.failed_pack_reads += summary.failed_pack_reads;
        total.cleaned_moves += summary.cleaned_moves;
        total.erased += summary.erased;
        total.deletions += summary.deletions;
        total.moves += summary.moves;
        total.restarts += summary.restarts;
        total.left_behind += summary.left_behind;
    }
    total
}

fn first_violation(seeds: std::ops::Range<u64>, steps: usize, bug: Bug) -> Option<Violation> {
    seeds.into_iter().find_map(|seed| run_with(config(seed, steps, Some(bug))).err())
}

#[test]
fn a_storage_seed_replays_exactly() {
    assert_eq!(run_with(Config::new(7, 300)).unwrap(), run_with(Config::new(7, 300)).unwrap());
    let violation = first_violation(0..300, 200, Bug::ReaperSkipsCopy).expect("the broken variant is caught");
    let replayed = run_with(config(violation.seed, 200, Some(Bug::ReaperSkipsCopy))).unwrap_err();
    assert_eq!((replayed.step, &replayed.message, &replayed.trace), (violation.step, &violation.message, &violation.trace));
}

#[test]
fn forks_reaping_deletion_and_moves_keep_every_deployments_objects() {
    let total = sweep(0..300, 200);
    eprintln!("{total:?}");
    assert!(total.failed_pack_reads > 10 && total.cleaned_moves > 10, "{total:?}");
    assert!(total.past_forks > 100 && total.erased > 1_000 && total.deletions > 50 && total.moves > 50, "the sweep forks, reaps, deletes and moves: {total:?}");
}

#[test]
fn a_reaper_that_skips_the_copy_before_erasing_is_caught() {
    let violation = first_violation(0..300, 200, Bug::ReaperSkipsCopy).expect("the checker catches a reaper erasing what a fork references");
    eprintln!("{violation}");
    assert!(violation.to_string().contains(&format!("seed {}", violation.seed)));
}

#[test]
fn a_deletion_that_skips_the_copy_before_erasing_is_caught() {
    let violation = first_violation(0..300, 200, Bug::DeletionSkipsCopy).expect("the checker catches a deletion erasing what a fork references");
    eprintln!("{violation}");
}

/// `RTDB_SIM_SEEDS=10000 RTDB_SIM_STEPS=1000 cargo test -p rtdb-simulation --release --test storage -- --ignored`
#[test]
#[ignore]
fn many_seeds_keep_every_deployments_objects() {
    let (seeds, steps) = (env("RTDB_SIM_SEEDS", 5_000), env("RTDB_SIM_STEPS", 600) as usize);
    let first = env("RTDB_SIM_FIRST_SEED", 0);
    eprintln!("{:?}", sweep(first..first + seeds, steps));
}

/// Replays one seed: `RTDB_SIM_SEED=72 cargo test -p rtdb-simulation --test storage one_seed -- --ignored`
#[test]
#[ignore]
fn one_seed() {
    let (seed, steps) = (env("RTDB_SIM_SEED", 0), env("RTDB_SIM_STEPS", 300) as usize);
    if let Err(violation) = run_with(Config::new(seed, steps)) { panic!("{violation}"); }
}
