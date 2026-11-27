//! A deterministic simulation of an RTDB multi cell's single-writer protocol (`allium/rtdb-multi.allium`: `Claim`, `SealPriorLoglets`, `SealQuorumReached`, `RecoverTail`, `OpenHolding`, `ResolveAndAppend`, `ApplyOnCommit`, `DeposeOnRefusedAppend`, `Park`, `ReclaimOnDemand`).
//!
//! Members claim one deployment through the real `MemoryDirectory` and `policy::claim`/`park`, then seal, recover and append against three log servers, each running the real logkeeper per-stream state machine behind the real server's durability rule.
//! One seed fixes every choice: which message is delivered next, which connection breaks, which member or server crashes, which links are cut, when leases lapse.
//! There are no threads, sockets or clocks; a run is a pure function of its seed.
//!
//! After every step the checker holds the protocol to `OneEffectiveWriterPerEpoch`, `NoTwoWritersHaveEffect`, `NothingCommittedIsLost`, `SealIsAPromise`, `SystemTimeRises` and `CheckpointNeverAheadOfLog`.
//!
//! Two more simulations drive the rest of a cell's real code from a seed: `storage`, deployments' objects through forks, the reaper, deletion and cell moves, and `sessions`, gateway sessions over backends that die or refuse.

mod check;
mod rng;
mod server;
pub mod sessions;
pub mod storage;
mod world;
mod writer;

/// A deliberately broken variant, to show the checker finds what it is meant to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// A server accepts an append at the next lsn without checking the writer's epoch against its promise.
    AppendIgnoresPromise,
    /// Recovery takes the seal reply whose last entry has the greatest epoch, rather than the greatest synced epoch.
    WinnerByLastEntryEpoch,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub steps: usize,
    pub bug: Option<Bug>,
    /// Whether a request whose connection broke, or whose sender crashed, may still be decided by its server later, after newer requests.
    pub stale_redelivery: bool,
}

impl Config {
    pub fn new(seed: u64, steps: usize) -> Self { Self { seed, steps, bug: None, stale_redelivery: false } }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    pub steps: usize,
    /// Distinct lsns some writer reported committed.
    pub committed: usize,
    /// Commits acknowledged to a client.
    pub acknowledged: usize,
    /// Claims the directory applied.
    pub claims: usize,
    /// Writers that recovered and opened; every one after the first is a failover or a reactivation.
    pub opened: usize,
    pub faults: usize,
    /// Append requests decided after their original connection or process was gone.
    pub stale_appends: usize,
}

/// A safety property failed: the seed replays it, and the trace shows the steps leading up to it.
#[derive(Clone, Debug)]
pub struct Violation {
    pub seed: u64,
    pub step: usize,
    pub message: String,
    pub trace: Vec<String>,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "seed {} violated at step {}: {}", self.seed, self.step, self.message)?;
        writeln!(f, "the last {} events:", self.trace.len())?;
        for event in &self.trace { writeln!(f, "  {event}")?; }
        Ok(())
    }
}

impl std::error::Error for Violation {}

pub fn run(seed: u64, steps: usize) -> Result<Summary, Violation> { run_with(Config::new(seed, steps)) }

pub fn run_with(config: Config) -> Result<Summary, Violation> { world::World::new(config).run() }

/// Runs the storage-lifecycle simulation (`storage`), printing the seed and the last events if it finds a violation.
pub fn run_storage(seed: u64, steps: usize) -> Result<storage::Summary, Violation> {
    storage::run_with(storage::Config::new(seed, steps)).inspect_err(|violation| eprintln!("{violation}"))
}

/// Runs the gateway-session simulation (`sessions`), printing the seed and the last events if it finds a violation.
pub fn run_sessions(seed: u64, steps: usize) -> Result<sessions::Summary, Violation> {
    sessions::run_with(sessions::Config::new(seed, steps)).inspect_err(|violation| eprintln!("{violation}"))
}
