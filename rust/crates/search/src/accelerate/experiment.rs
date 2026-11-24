//! Point-query experiments over the same trie files and score as the native SQL accelerator.

pub mod workload;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtdb_query::ScanRequest;
use rtdb_text::Query;
use rtdb_types::{EntityId, Iid, Result};

use super::{Cutter, Found, Judged, SearchAccelerator, TrieFiles, by_file};
use crate::file::{Ranked, experiment::{Bound, WindowSummary}};

/// The baseline prefers higher score alone and retains all score ties through ranking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ranking { ScoreOnly, ScoreAndId }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fallback { Visibility, IndexCoverage, IdCoverage, ScoreDomain, CandidateLimit }

#[derive(Debug, Default)]
pub struct Work {
    pub files: usize,
    pub windows: usize,
    pub scored: usize,
    pub hidden: usize,
    pub preparation: Duration,
    pub summary_bytes: usize,
}

/// An index-scored hit carrying a canonical integer ID for a point fetch at the captured basis.
#[derive(Debug)]
pub struct Hit {
    pub iid: Iid,
    pub id: i64,
    pub score: f64,
}

/// A benchmark's immutable ID domain and reusable bounds preparation, owned by the benchmark driving it.
pub struct PointRanking {
    ids: BTreeMap<Iid, i64>,
    summaries: BTreeMap<(String, String), WindowSummary>,
}

impl PointRanking {
    pub fn new(ids: impl IntoIterator<Item = (Iid, i64)>) -> Option<Self> {
        let mut canonical = BTreeMap::new();
        for (iid, id) in ids {
            if EntityId::Integer(id).iid() != iid || canonical.insert(iid, id).is_some_and(|old| old != id) {
                return None;
            }
        }
        Some(Self { ids: canonical, summaries: BTreeMap::new() })
    }

    /// Ranks a single exact MATCH at valid/system points ordered by score descending then integer SQL ID ascending.
    /// The caller fetches fields at the same points or falls back to the ordinary SQL path.
    pub async fn rank(
        &mut self, accelerator: &SearchAccelerator, request: &ScanRequest<'_>, column: &str,
        matching: &Query, scoring: &str, limit: usize, ranking: Ranking,
    ) -> Result<(std::result::Result<Vec<Hit>, Fallback>, Work)> {
        let mut work = Work::default();
        let table = &request.scan.table.name.table;
        if !request.bounds.valid_time.is_point() || !request.bounds.system_time.is_point()
            || request.iid_scope().is_some() || request.snapshot.has_overlays()
            || request.snapshot.iter_segments(table).any(|segment| segment.row_count() > 0) {
            return Ok((Err(Fallback::Visibility), work));
        }
        if limit == 0 { return Ok((Ok(Vec::new()), work)); }
        let Some(definition) = accelerator.definition(request, column).await? else { return Ok((Err(Fallback::IndexCoverage), work)) };
        let scoring = Query::parse(scoring);
        let TrieFiles::Complete(Found { files, skipped }) = accelerator.trie_files(request, &definition, column, &[matching, &scoring], super::LoadSearch::Match, None).await?
            else { return Ok((Err(Fallback::IndexCoverage), work)) };
        let scoring = Arc::new(rtdb_text::Scorer::new(scoring, None));
        let Some(rank_scorer) = scoring.for_ranking() else { return Ok((Err(Fallback::ScoreDomain), work)); };
        let cutter = Cutter::new(&files, &skipped, request);
        let mut cursors = Vec::new();
        let tie_bounds = ranking == Ranking::ScoreAndId;
        for group in by_file(&files) {
            let mut cursor = group.file.ranked(column, matching, &rank_scorer, request.bounds)?;
            let summary = if tie_bounds {
                let start = Instant::now();
                let name = (&files[*group.members.values().next().unwrap()].identity, column);
                let key = (name.0.clone(), name.1.to_string());
                let summary = match self.summaries.entry(key) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let Some(summary) = WindowSummary::new(&group.file, &self.ids) else {
                            return Ok((Err(Fallback::IdCoverage), work));
                        };
                        entry.insert(summary)
                    }
                };
                summary.prepare(&group.file, column, scoring.query())?;
                work.preparation += start.elapsed();
                work.summary_bytes += summary.retained_bytes();
                cursor.with_tie_bounds(summary);
                Some((name.0.clone(), name.1.to_string()))
            } else { None };
            cursors.push((group.members, cursor, summary));
        }
        work.files = cursors.len();
        let mut top = Vec::<Hit>::new();
        while let Some((index, bound)) = best(&cursors, &self.summaries) {
            if let Some(kth) = top.get(limit - 1) {
                let kth = Bound { score: kth.score, id: kth.id };
                if kth.compare(bound).is_gt() { break; }
            }
            let (members, cursor, _) = &mut cursors[index];
            let (windows, hits) = cursor.expand_experiment()?;
            work.windows += windows;
            work.scored += hits.len();
            if work.scored > accelerator.max_candidates { return Ok((Err(Fallback::CandidateLimit), work)); }
            for (doc, score) in hits {
                let Some(&own) = members.get(&doc.member) else { continue };
                match cutter.judge(own, doc)? {
                    Judged::Hidden => { work.hidden += 1; continue; }
                    Judged::Visible { exact: false } => return Ok((Err(Fallback::Visibility), work)),
                    Judged::Visible { exact: true } => {}
                }
                let iid = Iid(doc.iid);
                let Some(&id) = self.ids.get(&iid) else { return Ok((Err(Fallback::IdCoverage), work)) };
                top.push(Hit { iid, id, score });
            }
            top.sort_unstable_by(|left, right| right.score.total_cmp(&left.score).then_with(|| left.id.cmp(&right.id)));
            top.truncate(limit);
        }
        Ok((Ok(top), work))
    }
}

type Cursor = (BTreeMap<u32, usize>, Ranked, Option<(String, String)>);

fn best(cursors: &[Cursor], summaries: &BTreeMap<(String, String), WindowSummary>) -> Option<(usize, Bound)> {
    cursors.iter().enumerate().filter_map(|(index, (_, cursor, summary))| {
        cursor.experimental_bound(summary.as_ref().map(|key| &summaries[key])).map(|bound| (index, bound))
    }).max_by(|left, right| left.1.compare(right.1))
}
