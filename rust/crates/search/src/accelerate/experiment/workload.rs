use rtdb_query::{CandidateIids, ScanRequest, ScanSelection};
use rtdb_types::Result;
use serde_json::{Value as Json, json};

use super::super::{Narrowing, NarrowingWork, SearchAccelerator};

#[derive(Clone, Copy)]
pub enum Policy<'a> {
    Current,
    Baseline,
    Exhaustive,
    FilterFirst(&'a CandidateIids),
    SkipRetry,
    Estimate,
}

impl SearchAccelerator {
    /// Runs the normal SQL accelerator with a measurement policy.
    /// FilterFirst requires the caller's exact matching filter IIDs at the request's two point instants and captured basis.
    pub async fn workload_candidates(
        &self, request: &ScanRequest<'_>, policy: Policy<'_>,
    ) -> Result<(Option<ScanSelection>, Json)> {
        let options = match policy {
            Policy::Current => Narrowing::default(),
            Policy::Baseline => Narrowing { baseline: true, ..Default::default() },
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
        let report = json!({
            "route": route, "tried_ranking": work.tried_ranking, "rank_decline": work.decline,
            "match_decline": work.plain_decline, "score_evaluations": work.score_evaluations,
            "postings_visited": work.postings_visited, "verification_rounds": work.verification_rounds,
            "verifier_iids": work.verifier_rows, "unranked_events": work.plain_events,
            "estimated_postings": work.estimated_postings, "candidate_iids": picked.as_ref().map(CandidateIids::len),
            "filter_scope": work.filter_scope, "retry_redundant": work.retry_redundant,
        });
        Ok((picked.map(|iids| ScanSelection { iids, scores: work.scores }), report))
    }
}
