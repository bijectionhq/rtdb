use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use rtdb_text::Query;
use rtdb_types::{Iid, Result};
use super::{DocMatch, Ranked, SearchFile};
use crate::rank_windows::window_of;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Bound {
    pub score: f64,
    pub id: i64,
}

impl Bound {
    pub fn compare(self, other: Self) -> Ordering {
        self.score.total_cmp(&other.score).then_with(|| other.id.cmp(&self.id))
    }
}

type TermShapes = Vec<Vec<(u32, u32)>>;

/// One minimum SQL ID per document window plus the distinct (frequency, length) pairs of each prepared term.
pub(crate) struct WindowSummary {
    minimum_ids: Vec<i64>,
    terms: BTreeMap<String, TermShapes>,
}

impl WindowSummary {
    pub(crate) fn new(file: &SearchFile, ids: &BTreeMap<Iid, i64>) -> Option<Self> {
        let count = file.manifest.documents.checked_sub(1).map_or(0, |doc| window_of(doc) as usize + 1);
        let mut minimum_ids = vec![i64::MAX; count];
        for doc in 0..file.manifest.documents {
            let id = *ids.get(&Iid(file.manifest.doc_iid(doc)))?;
            let minimum = &mut minimum_ids[window_of(doc) as usize];
            *minimum = (*minimum).min(id);
        }
        Some(Self { minimum_ids, terms: BTreeMap::new() })
    }

    pub(crate) fn prepare(&mut self, file: &SearchFile, column: &str, scoring: &Query) -> Result<()> {
        for term in scoring.terms() {
            if self.terms.contains_key(term) { continue; }
            let mut windows = vec![BTreeSet::new(); self.minimum_ids.len()];
            if let Some(postings) = file.postings(column, term)? {
                for &doc in postings.documents() {
                    windows[window_of(doc) as usize].insert((postings.frequency(doc), file.stored_length(column, doc)?));
                }
            }
            self.terms.insert(term.clone(), windows.into_iter().map(|pairs| pairs.into_iter().collect()).collect());
        }
        Ok(())
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.minimum_ids.capacity() * size_of::<i64>() + self.terms.iter().map(|(term, windows)| {
            term.capacity() + windows.capacity() * size_of::<Vec<(u32, u32)>>()
                + windows.iter().map(|window| window.capacity() * size_of::<(u32, u32)>()).sum::<usize>()
        }).sum::<usize>()
    }
}

impl Ranked {
    pub(crate) fn with_tie_bounds(&mut self, summary: &WindowSummary) {
        let Some(windows) = &mut self.0 else { return };
        for (score, _) in &mut windows.windows { *score = 0.0; }
        for (index, term) in windows.scorer.scoring.query().terms().iter().enumerate() {
            let by_window = &summary.terms[term];
            let mut scores = BTreeMap::new();
            for (score, window) in &mut windows.windows {
                let best = by_window[*window as usize].iter().map(|&(frequency, length)| {
                    *scores.entry((frequency, length)).or_insert_with(||
                        windows.scorer.scoring.term_score(index, frequency, length))
                }).fold(0.0f64, f64::max);
                *score += best;
            }
        }
        // Each bound sums per-term maxima in the same order and with the same floating-point operations as an exact score.
        windows.windows.sort_unstable_by(|left, right| right.0.total_cmp(&left.0)
            .then_with(|| summary.minimum_ids[left.1 as usize].cmp(&summary.minimum_ids[right.1 as usize]))
            .then_with(|| left.1.cmp(&right.1)));
    }

    pub(crate) fn experimental_bound(&self, summary: Option<&WindowSummary>) -> Option<Bound> {
        let windows = self.0.as_ref()?;
        let &(score, window) = windows.windows.get(windows.next)?;
        let id = summary.map_or(i64::MIN, |summary| summary.minimum_ids[window as usize]);
        Some(Bound { score, id })
    }

    pub(crate) fn expand_experiment(&mut self) -> Result<(usize, Vec<(DocMatch, f64)>)> {
        let Some(windows) = &mut self.0 else { return Ok((0, Vec::new())) };
        let before = windows.next;
        let hits = windows.expand(None, &mut 0);
        Ok((windows.next - before, hits.iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Bound, WindowSummary};
    use crate::{SearchFile, SearchRow, build_search_file};
    use rtdb_text::{Query, Scorer, tokens};
    use rtdb_types::{EntityId, EventOp, Iid, Interval, TemporalBounds, Timestamp};

    #[test]
    fn exact_bounds_cover_every_score_and_order_uniform_windows_by_sql_id() {
        let columns = ["body".to_string()];
        let ids: std::collections::BTreeMap<_, _> = (-512..512).map(|id| (EntityId::Integer(id).iid(), id)).collect();
        let rows: Vec<_> = ids.keys().map(|iid| SearchRow {
            position: None,
            identity: None, iid: iid.0,
            system_from: 0,
            valid_from: 0,
            valid_to: 20,
            op: EventOp::Put,
            values: vec![Some(tokens("invoice alpha alpha beta"))],
        }).collect();
        let file = Arc::new(SearchFile::open(build_search_file(&columns, &rows, "ties").unwrap(), &columns).unwrap());
        let matching = Query::parse("invoice");
        let scoring = Arc::new(Scorer::new(Query::parse("beta missing alpha"), None));
        let bounds = TemporalBounds { valid_time: Interval::at(Timestamp(1)).unwrap(), system_time: Interval::at(Timestamp(1)).unwrap() };
        let mut summary = WindowSummary::new(&file, &ids).unwrap();
        summary.prepare(&file, "body", scoring.query()).unwrap();
        let rank_scorer = scoring.for_ranking().unwrap();
        let mut ranked = file.ranked("body", &matching, &rank_scorer, bounds).unwrap();
        ranked.with_tie_bounds(&summary);
        assert_eq!(ranked.experimental_bound(Some(&summary)).unwrap().id, -512);
        let mut total = 0;
        while let Some(bound) = ranked.experimental_bound(Some(&summary)) {
            let (_, hits) = ranked.expand_experiment().unwrap();
            for (hit, score) in hits {
                assert_eq!(score.to_bits(), bound.score.to_bits());
                assert!(!Bound { score, id: ids[&Iid(hit.iid)] }.compare(bound).is_gt());
                total += 1;
            }
        }
        assert_eq!(total, 1024);
    }
}
