//! Pure decisions of recovery: which sealed server's history wins, and where another server diverges from it.
//!
//! A server is synced at epoch e once e's writer has copied its whole recovered history onto it; only then does it accept e's appends.
//! The winner is the reply greatest by (synced epoch, last lsn), as in Viewstamped Replication's view change: a commit at e reached a quorum of servers synced at e, every seal quorum meets that quorum, and a server synced later holds a history that already contains the commit.
//! Two holders agreeing on an entry's epoch at an lsn agree on the entry and everything before it, because one writer creates at most one entry per lsn and copies only whole prefixes.

use crate::protocol::{EpochRun, Sealed};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Holding {
    pub trim: i64,
    pub last: i64,
    pub runs: Vec<EpochRun>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairPlan {
    /// The server holds entries below the history's trim point that cannot be compared, so it drops them first.
    pub trim_through: Option<i64>,
    /// The first lsn at which the server's entries differ from the history, or one past the shorter of the two.
    pub from: i64,
}

impl From<&Sealed> for Holding {
    fn from(sealed: &Sealed) -> Self { Self { trim: sealed.trim_lsn, last: sealed.last_lsn, runs: sealed.runs.clone() } }
}

impl Holding {
    fn epoch_at(&self, lsn: i64) -> Option<u64> {
        if lsn <= self.trim || lsn > self.last { return None; }
        self.runs.iter().rev().find(|run| run.start_lsn <= lsn).map(|run| run.epoch)
    }

    pub fn push(&mut self, lsn: i64, epoch: u64) {
        debug_assert_eq!(lsn, self.last + 1);
        if self.runs.last().is_none_or(|run| run.epoch != epoch) { self.runs.push(EpochRun { start_lsn: lsn, epoch }); }
        self.last = lsn;
    }

    fn trimmed_through(&self, through: i64) -> Self {
        Self { trim: through, last: self.last.max(through), runs: self.runs.clone() }
    }
}

pub fn winner(replies: &[Sealed]) -> usize {
    (0..replies.len()).max_by_key(|&index| (replies[index].synced, replies[index].last_lsn)).expect("a quorum of replies")
}

pub fn plan_repair(server: &Holding, history: &Holding) -> Result<RepairPlan, String> {
    let (server, trim_through) = if server.trim < history.trim {
        (server.trimmed_through(history.trim), Some(history.trim))
    } else {
        (server.clone(), None)
    };
    if server.trim > history.last {
        return Err(format!("a server trimmed through {} beyond the recovered history's last lsn {}", server.trim, history.last));
    }
    let start = server.trim + 1;
    let end = server.last.min(history.last);
    let mut boundaries: Vec<i64> = std::iter::once(start)
        .chain(server.runs.iter().chain(&history.runs).map(|run| run.start_lsn).filter(|lsn| (start..=end).contains(lsn)))
        .collect();
    boundaries.sort_unstable();
    let from = boundaries.into_iter().filter(|lsn| *lsn <= end)
        .find(|lsn| server.epoch_at(*lsn) != history.epoch_at(*lsn))
        .unwrap_or(end + 1);
    Ok(RepairPlan { trim_through, from })
}

#[allow(unexpected_cfgs)]
#[cfg(kani)]
mod proofs;

#[cfg(test)]
mod tests {
    use super::*;

    fn holding(trim: i64, last: i64, runs: &[(i64, u64)]) -> Holding {
        Holding { trim, last, runs: runs.iter().map(|&(start_lsn, epoch)| EpochRun { start_lsn, epoch }).collect() }
    }

    fn sealed(synced: u64, last_lsn: i64) -> Sealed { Sealed { synced, last_lsn, ..Default::default() } }

    #[test]
    fn the_winner_is_the_latest_synced_then_the_longest() {
        assert_eq!(winner(&[sealed(1, 9), sealed(2, 3), sealed(2, 4)]), 2);
        assert_eq!(winner(&[sealed(0, 9), sealed(1, 0)]), 1, "a longer log a writer never synced loses");
    }

    #[test]
    fn a_server_partly_copied_by_a_dead_recovery_still_wins_over_a_staler_one() {
        use crate::protocol::{Entry, ResponseBody};
        use crate::server::state::StreamState;
        let entry = |lsn: i64| Entry { lsn, epoch: 1, timestamp: lsn, payload: Default::default() };
        let sealed = |state: &mut StreamState, epoch: u64| match state.seal(epoch).reply { ResponseBody::Sealed(sealed) => sealed, other => panic!("{other:?}") };
        let synced_at_1 = |lsns: std::ops::RangeInclusive<i64>| {
            let mut state = StreamState::default();
            state.seal(1);
            state.overwrite(1, 0, Vec::new(), true, -1);
            state.append(1, 0, lsns.map(entry).collect(), -1);
            state
        };
        // Epoch 1 committed lsns 0..=3 on `a` and `c`; `c` also holds 4 and 5, and `b` lags at 0.
        let (mut a, mut b, mut c) = (synced_at_1(0..=3), synced_at_1(0..=0), synced_at_1(0..=5));
        let (from_a, from_c) = (sealed(&mut a, 2), sealed(&mut c, 2));
        assert_eq!(winner(&[from_a.clone(), from_c.clone()]), 1);
        let plan = plan_repair(&Holding::from(&from_a), &Holding::from(&from_c)).unwrap();
        // Epoch 2 copies one chunk of `c`'s history onto `a`, then dies.
        a.overwrite(2, plan.from, vec![entry(4)], false, -1);
        let replies = [sealed(&mut a, 3), sealed(&mut b, 3)];
        let history = Holding::from(&replies[winner(&replies)]);
        assert!(history.last >= 3, "epoch 3 recovers everything committed, not only through {}", history.last);
    }

    #[test]
    fn an_identical_or_shorter_server_continues_from_its_end() {
        let history = holding(-1, 5, &[(0, 1), (3, 2)]);
        assert_eq!(plan_repair(&history, &history), Ok(RepairPlan { trim_through: None, from: 6 }));
        assert_eq!(plan_repair(&holding(-1, 3, &[(0, 1), (3, 2)]), &history), Ok(RepairPlan { trim_through: None, from: 4 }));
        assert_eq!(plan_repair(&holding(-1, -1, &[]), &history), Ok(RepairPlan { trim_through: None, from: 0 }));
    }

    #[test]
    fn a_divergent_suffix_is_overwritten_from_its_first_differing_epoch() {
        let history = holding(-1, 5, &[(0, 1), (3, 2)]);
        assert_eq!(plan_repair(&holding(-1, 4, &[(0, 1)]), &history).unwrap().from, 3, "a minority suffix of epoch 1");
        assert_eq!(plan_repair(&holding(-1, 8, &[(0, 1), (3, 2)]), &history).unwrap().from, 6, "entries beyond the history are dropped");
        assert_eq!(plan_repair(&holding(-1, 5, &[(0, 1), (4, 2)]), &history).unwrap().from, 3);
    }

    #[test]
    fn a_server_holding_entries_below_the_history_trim_drops_them_first() {
        let history = holding(10, 12, &[(11, 3)]);
        assert_eq!(plan_repair(&holding(-1, 4, &[(0, 1)]), &history), Ok(RepairPlan { trim_through: Some(10), from: 11 }));
        assert_eq!(plan_repair(&holding(-1, 11, &[(0, 3)]), &history), Ok(RepairPlan { trim_through: Some(10), from: 12 }));
        assert_eq!(plan_repair(&holding(11, 12, &[(12, 3)]), &history), Ok(RepairPlan { trim_through: None, from: 13 }), "a server trimmed further compares above its own trim");
    }
}
