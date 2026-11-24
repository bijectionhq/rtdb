/// How far the approximate route probes and how many candidates it keeps for exact reranking (vector.allium: ProbePlan).
/// The recall target sets the starting fraction of each generation's clusters and the shortlist per requested row.
/// A filter that leaves too few candidates widens both, doubling, until it is satisfied, every cluster is probed,
/// or the shortlist would exceed the candidate budget and the read takes the exact route.
pub(crate) struct Plan { fraction: f64, target: usize }

const MIN_PROBES: usize = 8;
const MIN_TARGET: usize = 64;

impl Plan {
    pub(crate) fn new(recall: f64, limit: usize) -> Self {
        let (fraction, overfetch) = if recall >= 0.99 { (0.25, 8) } else if recall >= 0.95 { (0.125, 4) } else if recall >= 0.9 { (0.0625, 4) } else { (0.03125, 2) };
        Self { fraction, target: limit.saturating_mul(overfetch).max(MIN_TARGET) }
    }

    /// The clusters to have probed in a generation of `clusters`.
    pub(crate) fn probes(&self, clusters: usize) -> usize {
        ((clusters as f64 * self.fraction).ceil() as usize).max(MIN_PROBES).min(clusters)
    }

    pub(crate) fn target(&self) -> usize { self.target }

    /// Doubles the probe fraction and the shortlist; false once the shortlist would exceed `max_candidates`.
    pub(crate) fn widen(&mut self, max_candidates: usize) -> bool {
        let target = self.target.saturating_mul(2);
        if target > max_candidates { return false; }
        self.target = target;
        self.fraction = (self.fraction * 2.0).min(1.0);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::Plan;

    #[test]
    fn a_higher_recall_target_probes_more_and_keeps_more() {
        let (low, high) = (Plan::new(0.8, 10), Plan::new(0.99, 10));
        assert!(low.probes(1_000) < high.probes(1_000));
        assert!(low.target() < high.target());
    }

    #[test]
    fn widening_stops_at_the_candidate_budget() {
        let mut plan = Plan::new(0.95, 10);
        let mut rounds = 0;
        while plan.widen(1_000) { rounds += 1; }
        assert!(plan.target() <= 1_000);
        assert_eq!(plan.probes(64), 64);
        assert!(rounds > 0);
    }
}
