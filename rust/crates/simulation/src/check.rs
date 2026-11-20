//! The safety properties, checked as the simulation goes.
//!
//! `committed` is the one chain every writer has reported committed: each opened writer's recovered history (all of it is committed once synced on a write quorum) and every entry its pipeline committed.

use std::collections::BTreeMap;

use rtdb_logkeeper::simulation::{Entry, StreamState};

fn same(left: &Entry, right: &Entry) -> bool { left.epoch == right.epoch && left.payload == right.payload && left.timestamp == right.timestamp }

fn show(entry: &Entry) -> String { format!("{}@{}#{}", entry.lsn, entry.epoch, u64::from_be_bytes(entry.payload[..8].try_into().unwrap_or([0; 8]))) }

pub(crate) struct Snapshot {
    pub server: usize,
    pub trim: i64,
    pub entries: BTreeMap<i64, Entry>,
}

#[derive(Default)]
pub(crate) struct Checker {
    /// Each committed entry, with the epoch of the writer that first reported it committed.
    committed: BTreeMap<i64, (Entry, u64)>,
    /// Per opened epoch, the trim point and last lsn of its recovered history.
    opened: BTreeMap<u64, (i64, i64)>,
    /// Per epoch, the trim point and entries of each server of its seal quorum when it sealed.
    seal_quorums: BTreeMap<u64, Vec<Snapshot>>,
    /// The one process whose requests at each epoch a server has durably accepted.
    epoch_writers: BTreeMap<u64, usize>,
    /// The member each epoch was claimed for.
    claims: BTreeMap<u64, String>,
    pub checkpoint: i64,
}

impl Checker {
    pub fn new() -> Self { Self { checkpoint: -1, ..Self::default() } }

    pub fn last_committed(&self) -> i64 { self.committed.keys().next_back().copied().unwrap_or(-1) }

    pub fn committed_count(&self) -> usize { self.committed.len() }

    /// `EveryEpochHasOneClaimant` and `EpochsOnlyRise`, as the directory applied them.
    pub fn claimed(&mut self, epoch: u64, member: &str) -> Result<(), String> {
        if let Some((&latest, _)) = self.claims.iter().next_back() && epoch <= latest {
            return Err(format!("OneEffectiveWriterPerEpoch: epoch {epoch} claimed by {member} after epoch {latest} was claimed"));
        }
        self.claims.insert(epoch, member.to_owned());
        Ok(())
    }

    /// At most one process has any request at an epoch durably accepted.
    pub fn accepted(&mut self, epoch: u64, process: usize) -> Result<(), String> {
        match self.epoch_writers.get(&epoch) {
            Some(&writer) if writer != process => Err(format!("OneEffectiveWriterPerEpoch: processes {writer} and {process} both had writes accepted at epoch {epoch}")),
            Some(_) => Ok(()),
            None => { self.epoch_writers.insert(epoch, process); Ok(()) }
        }
    }

    pub fn seal_quorum(&mut self, epoch: u64, snapshots: Vec<Snapshot>) { self.seal_quorums.insert(epoch, snapshots); }

    fn commit_one(&mut self, entry: &Entry, epoch: u64, by: &str) -> Result<(), String> {
        if let Some((existing, _)) = self.committed.get(&entry.lsn) {
            if !same(existing, entry) { return Err(format!("NoTwoWritersHaveEffect: {by} committed {} where {} was already committed", show(entry), show(existing))); }
            return Ok(());
        }
        if let Some((_, (before, _))) = self.committed.range(..entry.lsn).next_back() && (before.epoch > entry.epoch || before.timestamp >= entry.timestamp) {
            return Err(format!("{by} committed {} (time {}) after {} (time {}): epochs never fall and system time rises with lsn", show(entry), entry.timestamp, show(before), before.timestamp));
        }
        if let Some((_, (after, _))) = self.committed.range(entry.lsn + 1..).next() && (after.epoch < entry.epoch || after.timestamp <= entry.timestamp) {
            return Err(format!("{by} committed {} (time {}) before {} (time {}): epochs never fall and system time rises with lsn", show(entry), entry.timestamp, show(after), after.timestamp));
        }
        self.committed.insert(entry.lsn, (entry.clone(), epoch));
        Ok(())
    }

    /// A writer at `epoch` recovered `history` (trim, last) and synced it on a write quorum.
    pub fn opened(&mut self, epoch: u64, history: &[Entry], trim: i64, last: i64) -> Result<(), String> {
        let lsns: Vec<i64> = history.iter().map(|entry| entry.lsn).collect();
        if lsns != (trim + 1..=last).collect::<Vec<_>>() { return Err(format!("epoch {epoch} recovered a history that is not dense from {} to {last}: {lsns:?}", trim + 1)); }
        if trim > self.checkpoint { return Err(format!("TrimFollowsPublication: epoch {epoch} recovered a history trimmed through {trim}, beyond the checkpoint {}", self.checkpoint)); }
        if let Some((entry, by)) = self.committed.range(last + 1..).map(|(_, committed)| committed).find(|(_, by)| *by < epoch) {
            return Err(format!("NothingCommittedIsLost: epoch {epoch} recovered through {last}, without {} committed at epoch {by}", show(entry)));
        }
        for pair in history.windows(2) {
            if pair[1].epoch < pair[0].epoch { return Err(format!("epoch {epoch} recovered {} after {}", show(&pair[1]), show(&pair[0]))); }
        }
        for entry in history { self.commit_one(entry, epoch, &format!("epoch {epoch}'s recovery"))?; }
        self.opened.insert(epoch, (trim, last));
        Ok(())
    }

    /// The pipeline of the writer at `epoch` committed `entries`.
    pub fn pipeline_committed(&mut self, epoch: u64, entries: &[Entry]) -> Result<(), String> {
        for entry in entries {
            self.commit_one(entry, epoch, &format!("epoch {epoch}'s pipeline"))?;
            for (&later, &(trim, last)) in self.opened.range(epoch + 1..) {
                if entry.lsn > trim && entry.lsn > last {
                    return Err(format!("NothingCommittedIsLost: epoch {epoch} committed {} after epoch {later} opened with a history ending at {last}", show(entry)));
                }
            }
            // SealIsAPromise: an entry a deposed writer commits was held by some server of every later seal quorum when it sealed.
            for (&later, snapshots) in self.seal_quorums.range(epoch + 1..) {
                let witnessed = snapshots.iter().any(|snapshot| entry.lsn <= snapshot.trim || snapshot.entries.get(&entry.lsn).is_some_and(|held| same(held, entry)));
                if !witnessed {
                    let servers: Vec<usize> = snapshots.iter().map(|snapshot| snapshot.server).collect();
                    return Err(format!("SealIsAPromise: epoch {epoch} committed {} though no server of epoch {later}'s seal quorum {servers:?} held it when sealed", show(entry)));
                }
            }
        }
        Ok(())
    }

    /// `CheckpointNeverAheadOfLog`.
    pub fn checkpointed(&mut self, lsn: i64) -> Result<(), String> {
        if lsn > self.last_committed() { return Err(format!("CheckpointNeverAheadOfLog: checkpoint {lsn} beyond the last committed lsn {}", self.last_committed())); }
        self.checkpoint = self.checkpoint.max(lsn);
        Ok(())
    }

    /// A server's state after a change from `from`: epochs never fall along its entries, nothing above the checkpoint is trimmed, and a server synced at e holds every committed entry of an epoch up to e where it holds that lsn.
    pub fn server(&self, server: usize, state: &StreamState, from: i64) -> Result<(), String> {
        if state.trim > self.checkpoint { return Err(format!("TrimFollowsPublication: server {server} trimmed through {}, beyond the checkpoint {}", state.trim, self.checkpoint)); }
        let start = from.saturating_sub(1).max(state.trim + 1);
        let skip = (start - state.trim - 1).max(0) as usize;
        let mut previous: Option<(i64, u64)> = None;
        for (held, lsn) in state.entries.iter().skip(skip).zip(start..) {
            if let Some((before, epoch)) = previous && held.epoch < epoch {
                return Err(format!("server {server} holds {lsn}@{} after {before}@{epoch}", held.epoch));
            }
            previous = Some((lsn, held.epoch));
        }
        if let Some(synced) = state.synced && start <= state.last() {
            for (&lsn, (entry, _)) in self.committed.range(start..=state.last()) {
                if entry.epoch > synced || lsn <= state.trim { continue; }
                let held = &state.entries[(lsn - state.trim - 1) as usize];
                if held.epoch != entry.epoch || held.payload != entry.payload {
                    return Err(format!("NothingCommittedIsLost: server {server}, synced at {synced}, holds {lsn}@{} where {} is committed", held.epoch, show(entry)));
                }
            }
        }
        Ok(())
    }
}
