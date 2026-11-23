use std::collections::BTreeSet;

use rtdb_query::ScanRequest;
use rtdb_types::{Iid, Result};
use serde_json::{Value as Json, json};

use super::super::{Narrowing, NarrowingWork, SearchAccelerator};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy<'a> {
    Current,
    Baseline,
    Tree14,
    Exhaustive,
    FilterFirst(&'a BTreeSet<Iid>),
    SkipRetry,
    Estimate,
}

impl SearchAccelerator {
    /// Runs the normal SQL accelerator with a measurement policy.
    /// FilterFirst requires the caller's exact matching filter IIDs at the request's two point instants and captured basis.
    pub async fn workload_candidates(
        &self, request: &ScanRequest<'_>, policy: Policy<'_>,
    ) -> Result<(Option<BTreeSet<Iid>>, Json)> {
        self.workload_narrow(request, policy).await.map(|(iids, _, report)| (iids, report))
    }

    pub async fn workload_selection(
        &self, request: &ScanRequest<'_>, policy: Policy<'_>,
    ) -> Result<(Option<rtdb_query::ScanSelection>, Json)> {
        let (iids, scores, report) = self.workload_narrow(request, policy).await?;
        let selected = iids.map(|iids| Ok::<_, rtdb_types::Anomaly>(rtdb_query::ScanSelection {
            iids: rtdb_query::CandidateIids::from_set(iids, request.memory)?, scores,
        })).transpose()?;
        Ok((selected, report))
    }

    async fn workload_narrow(
        &self, request: &ScanRequest<'_>, policy: Policy<'_>,
    ) -> Result<(Option<BTreeSet<Iid>>, Option<rtdb_query::CandidateScores>, Json)> {
        let options = match policy {
            Policy::Current => Narrowing::default(),
            Policy::Baseline => Narrowing { baseline: true, ..Default::default() },
            Policy::Tree14 => Narrowing { omit_packs: true, ..Default::default() },
            Policy::Exhaustive => Narrowing { is_oracle: true, ..Default::default() },
            Policy::FilterFirst(scope) if request.bounds.valid_time.is_point() && request.bounds.system_time.is_point() => {
                Narrowing { scope: Some(scope), ..Default::default() }
            }
            Policy::FilterFirst(_) => Narrowing::default(),
            Policy::SkipRetry => Narrowing { baseline: true, skip_retry: true, ..Default::default() },
            Policy::Estimate => Narrowing { baseline: true, estimate_first: true, ..Default::default() },
        };
        let mut work = NarrowingWork::default();
        let picked = self.narrow(request, options, &mut work).await?;
        let route = if work.ranked { "ranked" }
        else if options.scope.is_some() && picked.is_some() { "filter-scope" }
        else if picked.is_some() { "match-narrowing" }
        else { "scan" };
        let page_reads = work.tree_io.by_kind().map(|(kind, reads)| {
            (kind.to_owned(), json!({"requests": reads.requests, "bytes": reads.bytes}))
        }).collect::<serde_json::Map<_, _>>();
        let report = json!({
            "route": route, "tried_ranking": work.tried_ranking, "rank_decline": work.decline,
            "match_decline": work.plain_decline, "score_evaluations": work.score_evaluations,
            "postings_visited": work.postings_visited, "verification_rounds": work.verification_rounds,
            "verifier_iids": work.verifier_rows, "unranked_events": work.plain_events,
            "estimated_postings": work.estimated_postings, "candidate_iids": picked.as_ref().map(BTreeSet::len),
            "filter_scope": work.filter_scope, "retry_redundant": work.retry_redundant,
            "tree": {"opened_roots": work.tree_roots, "expansions": work.tree_expansions, "decline": work.tree_decline,
                "range_requests": work.tree_io.requests, "range_bytes": work.tree_io.bytes, "page_cache_hits": work.tree_io.cache_hits,
                "requests_by_kind": page_reads, "uncovered_live_rows": work.uncovered_live_rows, "retained_rows": work.retained_rank_rows,
                "packed_resolved": work.packed_resolved, "deferred_payloads": work.deferred_payloads, "shared_cutoff_prunes": work.shared_cutoff_prunes},
        });
        Ok((picked, work.scores, report))
    }
}
