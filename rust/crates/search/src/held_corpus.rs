use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_query::QueryContext;
use rtdb_text::{Corpus, Query, Scorer};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result};

use crate::{OwnedSearchHit, OwnedSearchPage, accelerate::owned_rank_order};

/// Resolve a declared single-column search path on its admitted archive cut.
pub async fn owned_text_column(
    context: Arc<QueryContext>,
    owner: &str,
    relation: &OwnedRelation,
    path: &OwnedRelation,
) -> Result<String> {
    let definition = crate::definitions::owned_definition(context, owner, relation, path)
        .await?
        .ok_or_else(|| {
            Anomaly::incorrect(
                "rtdb.owned/search-path",
                "The retained relation has no declared text search path with this name",
            )
        })?;
    let [column]: [String; 1] = definition.columns.try_into().map_err(|_| corpus_limit())?;
    Ok(column)
}

const MAX_IDENTITIES: usize = 4096;
const MAX_TEXT_BYTES: usize = 8 << 20;

struct Match {
    id: EntityId,
    partition: usize,
    length: u32,
    frequencies: Vec<u32>,
}

/// Accumulate exact statistics after the held world owner resolves visibility.
/// Only match frequencies are retained. Scoring waits for the complete corpus.
pub struct HeldTextCorpus {
    memory: Arc<MemoryBudget>,
    retained: Arc<MemoryReservation>,
    query: Query,
    corpus: Corpus,
    matches: Vec<Match>,
    identities: usize,
    text_bytes: usize,
}

impl HeldTextCorpus {
    pub fn new(memory: Arc<MemoryBudget>, text: &str) -> Result<Self> {
        if text.len() > 4096 {
            return Err(corpus_limit());
        }
        let retained = memory.reserve(4096 + 128 * text.len())?;
        let query = Query::parse(text);
        Ok(Self {
            memory,
            retained,
            query,
            corpus: Corpus::default(),
            matches: Vec::new(),
            identities: 0,
            text_bytes: 0,
        })
    }

    pub fn observe(&mut self, partition: usize, id: EntityId, text: Option<&str>) -> Result<()> {
        self.identities += 1;
        if self.identities > MAX_IDENTITIES {
            return Err(corpus_limit());
        }
        let Some(text) = text else {
            return Ok(());
        };
        self.text_bytes = self
            .text_bytes
            .checked_add(text.len())
            .filter(|total| *total <= MAX_TEXT_BYTES)
            .ok_or_else(corpus_limit)?;
        let _tokens = self.memory.reserve(4096 + 64 * text.len())?;
        let tokens = rtdb_text::tokens(text);
        self.corpus.documents += 1;
        self.corpus.tokens += tokens.len() as u64;
        let frequencies = self
            .query
            .terms()
            .iter()
            .map(|term| tokens.iter().filter(|token| *token == term).count() as u32)
            .collect::<Vec<_>>();
        for (term, &count) in self.query.terms().iter().zip(&frequencies) {
            if count > 0 {
                *self.corpus.frequencies.entry(term.clone()).or_default() += 1;
            }
        }
        if self.query.matches(&tokens) {
            let charge = id.as_value().retained_bytes()? + 256 + 4 * frequencies.capacity();
            let retained = Arc::get_mut(&mut self.retained).expect("exclusive corpus reservation");
            retained.grow_to(retained.bytes().saturating_add(charge))?;
            self.matches.push(Match {
                id,
                partition,
                length: rtdb_text::stored_length(tokens.len()),
                frequencies,
            });
        }
        Ok(())
    }

    pub fn finish(self, offset: usize, limit: usize) -> Result<OwnedSearchPage> {
        if offset > 4096 || !(1..=128).contains(&limit) {
            return Err(corpus_limit());
        }
        let scorer = Scorer::new(self.query, Some(&self.corpus));
        let total = self.matches.len();
        let _scratch = self
            .memory
            .reserve(total * std::mem::size_of::<OwnedSearchHit>())?;
        let mut ranked = self
            .matches
            .into_iter()
            .map(|matched| OwnedSearchHit {
                partition: matched.partition,
                id: matched.id,
                score: scorer.score(&matched.frequencies, matched.length),
            })
            .collect::<Vec<_>>();
        if ranked.iter().any(|hit| !hit.score.is_finite()) {
            return Err(corpus_limit());
        }
        ranked.sort_unstable_by(owned_rank_order);
        let selected = ranked.into_iter().skip(offset).take(limit);
        let mut reservation = self.memory.reserve(4096)?;
        let mut hits = Vec::with_capacity(selected.len());
        for hit in selected {
            let reservation = Arc::get_mut(&mut reservation).expect("exclusive rank reservation");
            let bytes = reservation
                .bytes()
                .saturating_add(hit.id.as_value().retained_bytes()? + 128);
            reservation.grow_to(bytes)?;
            hits.push(hit);
        }
        Ok(OwnedSearchPage {
            hits,
            total,
            _reservation: reservation,
        })
    }
}

fn corpus_limit() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/search-held-corpus-limit",
        "The exact held world search corpus exceeds its supported bounds",
    )
}
