use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_query::QueryContext;
use rtdb_text::{Corpus, Query, Scorer};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, Timestamp, Value};

use crate::{OwnedSearchHit, OwnedSearchPage, accelerate::OwnedRankWindow};

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
    shadows: BTreeMap<usize, BTreeSet<EntityId>>,
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
            shadows: BTreeMap::new(),
            identities: 0,
            text_bytes: 0,
        })
    }

    fn observe(&mut self, partition: usize, id: EntityId, text: Option<&str>) -> Result<()> {
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

    /// Register an exact typed overlay shadow, even if it contributes no text.
    pub fn shadow(&mut self, partition: usize, id: EntityId, text: Option<&str>) -> Result<()> {
        self.observe(partition, id.clone(), text)?;
        let charge = id.as_value().retained_bytes()? + 192;
        let retained = Arc::get_mut(&mut self.retained).expect("exclusive corpus reservation");
        retained.grow_to(retained.bytes().saturating_add(charge))?;
        if !self.shadows.entry(partition).or_default().insert(id) {
            return Err(corpus_limit());
        }
        Ok(())
    }

    pub fn contains_shadow(&self, partition: usize, id: &EntityId) -> bool {
        self.shadows.get(&partition).is_some_and(|ids| ids.contains(id))
    }

    pub(crate) async fn read_root_partition(
        &mut self,
        context: &Arc<QueryContext>,
        owner: &str,
        relation: &OwnedRelation,
        path: &OwnedRelation,
        partition: usize,
        valid: Timestamp,
    ) -> Result<()> {
        let column = owned_text_column(Arc::clone(context), owner, relation, path).await?;
        let database = context.catalog.database(context.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else { return Err(corpus_limit()); };
        if snapshot.system_time_cap() != Some(context.current_time.as_micros()?) {
            return Err(corpus_limit());
        }
        let table = relation.table(owner);
        if !snapshot.contains_table(&table) {
            return Ok(());
        }
        let types = snapshot.column_types_for(&table, [column.clone()]);
        if !types.get(&column).is_some_and(rtdb_types::LogicalType::is_clear_search_text) {
            return Err(Anomaly::incorrect("rtdb.owned/search-column", "Owned search path no longer describes only clear text"));
        }
        let Some(mut cursor) = rtdb_query::open_owned_root_projection(
            Arc::clone(context), owner, relation, valid, &["_id", &column],
        ).await? else { return Err(corpus_limit()); };
        let shadows = self.shadows.remove(&partition).unwrap_or_default();
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let (id_column, id_field) = schema.column_with_name("_id").ok_or_else(corpus_limit)?;
            let (text_column, text_field) = schema.column_with_name(&column).ok_or_else(corpus_limit)?;
            let ids = batch.column(id_column).as_ref();
            let text = batch.column(text_column).as_ref();
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let id_bytes = rtdb_columnar::estimate_read_value_bytes(id_field, ids, row)?;
                let _identity = self.memory.reserve(id_bytes.saturating_mul(2))?;
                let id = rtdb_columnar::read_value(id_field, ids, row)?.entity_id()?;
                if shadows.contains(&id) { continue; }
                let bytes = rtdb_columnar::estimate_read_value_bytes(text_field, text, row)?;
                let _text = self.memory.reserve(bytes)?;
                let value = rtdb_columnar::read_value(text_field, text, row)?;
                let value = match &value {
                    Value::Utf8(text) => Some(text.as_str()),
                    Value::Null | Value::Undefined => None,
                    _ => return Err(corpus_limit()),
                };
                self.shadow(partition, id, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn shadowed(&self, partition: usize) -> Option<&BTreeSet<EntityId>> {
        self.shadows.get(&partition)
    }

    pub(crate) fn statistics(&self) -> &Corpus {
        &self.corpus
    }

    pub(crate) fn require_query(&self, query: &Query) -> Result<()> {
        if query != &self.query {
            return Err(corpus_limit());
        }
        Ok(())
    }

    pub(crate) fn rank(self, scorer: &Scorer, window: &mut OwnedRankWindow) -> Result<()> {
        for matched in self.matches {
            window.admit_candidate()?;
            let score = scorer.score(&matched.frequencies, matched.length);
            if !score.is_finite() {
                return Err(corpus_limit());
            }
            window.insert(OwnedSearchHit {
                partition: matched.partition,
                id: matched.id,
                score,
            })?;
        }
        Ok(())
    }

    pub fn finish(self, offset: usize, limit: usize) -> Result<OwnedSearchPage> {
        if offset > 4096 || !(1..=128).contains(&limit) {
            return Err(corpus_limit());
        }
        let scorer = Scorer::new(self.query.clone(), Some(&self.corpus));
        let mut window = OwnedRankWindow::new(&self.memory, offset, limit)?;
        self.rank(&scorer, &mut window)?;
        Ok(window.finish())
    }
}

fn corpus_limit() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/search-held-corpus-limit",
        "The exact held world search corpus exceeds its supported bounds",
    )
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_shadows_replace_committed_text_before_statistics() {
        let mut corpus = HeldTextCorpus::new(MemoryBudget::new(1 << 20), "apples").unwrap();
        let replaced = EntityId::String("replaced".into());
        let deleted = EntityId::String("deleted".into());
        let unchanged = EntityId::String("unchanged".into());
        corpus.shadow(0, replaced.clone(), Some("pears")).unwrap();
        corpus.shadow(0, deleted.clone(), None).unwrap();
        assert!(!corpus.contains_shadow(1, &replaced));
        for id in [replaced, deleted, unchanged.clone()] {
            if !corpus.contains_shadow(0, &id) {
                corpus.shadow(0, id, Some("apples")).unwrap();
            }
        }
        assert_eq!(corpus.statistics().documents, 2);
        let page = corpus.finish(0, 10).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.hits[0].id, unchanged);
    }
}
