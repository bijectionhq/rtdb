use std::{borrow::Borrow, collections::BTreeMap, sync::Arc};

use arrow_array::{Array, BinaryArray, StringArray, UInt64Array};
use rtdb_columnar::{EntityIdRef, MemoryBudget, MemoryReservation};
use rtdb_plan::Catalog;
use rtdb_query::QueryContext;
use rtdb_text::{Corpus, Query, Scorer};
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, OwnedRelation, Result, Timestamp, Value};

use crate::{OwnedSearchBatch, OwnedSearchPage, accelerate::{OwnedRankWindow, RankedIdentity}};

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

/// The analyzer of the declared search paths a held search ranks: every partition's path must use the same one.
pub async fn owned_search_analyzer(
    context: Arc<QueryContext>,
    owner: &str,
    partitions: &[(OwnedRelation, OwnedRelation)],
) -> Result<rtdb_text::Analyzer> {
    let mut analyzer = None;
    for (relation, path) in partitions {
        let definition = crate::definitions::owned_definition(Arc::clone(&context), owner, relation, path)
            .await?
            .ok_or_else(|| Anomaly::incorrect(
                "rtdb.owned/search-path",
                "Owned relation has no declared clear-text search path with this name",
            ))?;
        if *analyzer.get_or_insert(definition.analyzer) != definition.analyzer {
            return Err(Anomaly::incorrect(
                "rtdb.owned/search-analyzers",
                "A held search ranks search paths that use one analyzer",
            ));
        }
    }
    analyzer.ok_or_else(corpus_limit)
}

const MAX_IDENTITIES: usize = 4096;
const MAX_TEXT_BYTES: usize = 8 << 20;

struct Match {
    length: u32,
    frequencies: Vec<u32>,
}

/// Staged text shadows borrow the request batch through analysis.
pub struct TextShadows<'a> {
    partitions: &'a UInt64Array,
    ids: TextShadowIds<'a>,
    text: &'a StringArray,
}

#[derive(Clone, Copy)]
enum TextShadowIds<'a> {
    Text(&'a StringArray),
    Binary(&'a BinaryArray),
}

impl<'a> TextShadowIds<'a> {
    fn get(self, row: usize) -> Option<EntityIdRef<'a>> {
        match self {
            Self::Text(ids) => ids.is_valid(row).then(|| EntityIdRef::String(ids.value(row))),
            Self::Binary(ids) => ids.is_valid(row).then(|| EntityIdRef::Binary(ids.value(row))),
        }
    }
}

impl<'a> TextShadows<'a> {
    pub fn try_new(
        partitions: &'a UInt64Array,
        ids: &'a dyn Array,
        text: &'a StringArray,
        admit_identity: impl Fn(usize, EntityIdRef<'a>) -> bool,
    ) -> Option<Self> {
        if partitions.len() != ids.len() || ids.len() != text.len() {
            return None;
        }
        let ids = if let Some(ids) = ids.as_any().downcast_ref::<StringArray>() {
            TextShadowIds::Text(ids)
        } else {
            TextShadowIds::Binary(ids.as_any().downcast_ref::<BinaryArray>()?)
        };
        for row in 0..partitions.len() {
            if partitions.is_null(row)
                || !admit_identity(usize::try_from(partitions.value(row)).ok()?, ids.get(row)?)
            {
                return None;
            }
        }
        Some(Self { partitions, ids, text })
    }

    fn rows(&self) -> impl Iterator<Item = (usize, EntityIdRef<'a>, Option<&'a str>)> {
        let Self { partitions, ids, text } = *self;
        (0..partitions.len()).map(move |row| {
            (
                partitions.value(row) as usize,
                ids.get(row).expect("admitted staged shadow identity"),
                text.is_valid(row).then(|| text.value(row)),
            )
        })
    }

    /// The corpus of `text` under the paths' analyzer, holding these shadows.
    pub fn into_corpus(
        self,
        memory: Arc<MemoryBudget>,
        analyzer: rtdb_text::Analyzer,
        text: &str,
        check: impl Fn() -> Result<()>,
    ) -> Result<HeldTextCorpus> {
        check()?;
        let mut corpus = HeldTextCorpus::new(memory, analyzer, text)?;
        for (partition, id, text) in self.rows() {
            let matched = corpus.observe(id, text)?;
            if corpus.shadows.entry(partition).or_default().insert(ShadowKey(id.into_owned()), matched).is_some() {
                return Err(corpus_limit());
            }
            check()?;
        }
        Ok(corpus)
    }
}

struct ShadowKey(EntityId);

trait ShadowIdentity {
    fn key(&self) -> EntityIdRef<'_>;
}

impl ShadowIdentity for ShadowKey {
    fn key(&self) -> EntityIdRef<'_> { EntityIdRef::from(&self.0) }
}

impl ShadowIdentity for EntityIdRef<'_> {
    fn key(&self) -> EntityIdRef<'_> { *self }
}

impl<'a> Borrow<dyn ShadowIdentity + 'a> for ShadowKey {
    fn borrow(&self) -> &(dyn ShadowIdentity + 'a) { self }
}

impl Eq for dyn ShadowIdentity + '_ {}
impl PartialEq for dyn ShadowIdentity + '_ {
    fn eq(&self, other: &Self) -> bool { self.key() == other.key() }
}
impl Ord for dyn ShadowIdentity + '_ {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.key().cmp(&other.key()) }
}
impl PartialOrd for dyn ShadowIdentity + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Eq for ShadowKey {}
impl PartialEq for ShadowKey {
    fn eq(&self, other: &Self) -> bool { self.key() == other.key() }
}
impl Ord for ShadowKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.key().cmp(&other.key()) }
}
impl PartialOrd for ShadowKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

/// Accumulate exact statistics after the held world owner resolves visibility.
/// Only match frequencies are retained. Scoring waits for the complete corpus.
pub struct HeldTextCorpus {
    memory: Arc<MemoryBudget>,
    query: Query,
    corpus: Corpus,
    shadows: BTreeMap<usize, BTreeMap<ShadowKey, Option<Match>>>,
    roots: BTreeMap<usize, BTreeMap<EntityId, Option<Match>>>,
    identities: usize,
    text_bytes: usize,
    retained: Arc<MemoryReservation>,
}

impl HeldTextCorpus {
    /// A corpus for `text` tokenised by the analyzer of the searched paths (owned_search_analyzer).
    pub fn new(memory: Arc<MemoryBudget>, analyzer: rtdb_text::Analyzer, text: &str) -> Result<Self> {
        if text.len() > 4096 {
            return Err(corpus_limit());
        }
        let retained = memory.reserve(4096 + 128 * text.len())?;
        let query = Query::analyzed(analyzer, text);
        Ok(Self {
            memory,
            query,
            corpus: Corpus::default(),
            shadows: BTreeMap::new(),
            roots: BTreeMap::new(),
            identities: 0,
            text_bytes: 0,
            retained,
        })
    }

    fn observe(&mut self, id: EntityIdRef<'_>, text: Option<&str>) -> Result<Option<Match>> {
        if self.identities >= MAX_IDENTITIES {
            return Err(corpus_limit());
        }
        let entry_charge = identity_charge(id) + 192;
        let retained = Arc::get_mut(&mut self.retained).expect("exclusive corpus reservation");
        let Some(text) = text else {
            retained.grow_to(retained.bytes().saturating_add(entry_charge))?;
            self.identities += 1;
            return Ok(None);
        };
        let text_bytes = self
            .text_bytes
            .checked_add(text.len())
            .filter(|total| *total <= MAX_TEXT_BYTES)
            .ok_or_else(corpus_limit)?;
        let _tokens = self.memory.reserve(4096 + 64 * text.len())?;
        let (tokens, frequencies, matched) = self.query.text_terms(text, true);
        let match_charge = if matched {
            256 + 4 * frequencies.capacity()
        } else {
            0
        };
        retained.grow_to(retained.bytes().saturating_add(entry_charge + match_charge))?;
        self.identities += 1;
        self.text_bytes = text_bytes;
        self.corpus.documents += 1;
        self.corpus.tokens += tokens.len() as u64;
        for (term, &count) in self.query.terms().iter().zip(&frequencies) {
            if count > 0 {
                *self.corpus.frequencies.entry(term.clone()).or_default() += 1;
            }
        }
        Ok(matched.then(|| Match { length: rtdb_text::stored_length(tokens.len()), frequencies }))
    }

    /// Register an exact typed overlay shadow, even if it contributes no text.
    pub fn shadow(&mut self, partition: usize, id: EntityId, text: Option<&str>) -> Result<()> {
        if self.contains_shadow(partition, &id) {
            return Err(corpus_limit());
        }
        let matched = self.observe(EntityIdRef::from(&id), text)?;
        self.shadows.entry(partition).or_default().insert(ShadowKey(id), matched);
        Ok(())
    }

    pub fn contains_shadow(&self, partition: usize, id: &EntityId) -> bool {
        self.contains_shadow_key(partition, EntityIdRef::from(id))
    }

    pub(crate) fn contains_shadow_key(&self, partition: usize, id: EntityIdRef<'_>) -> bool {
        let key: &dyn ShadowIdentity = &id;
        self.shadows.get(&partition).is_some_and(|rows| rows.contains_key(key))
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
        let sealed = crate::rank_family::needs_rank_family(context, owner, relation).await?;
        let column = owned_text_column(Arc::clone(context), owner, relation, path).await?;
        let database = context.catalog.database(context.catalog.default_database())?;
        let [snapshot] = database.partitions.as_slice() else { return Err(corpus_limit()); };
        if snapshot.system_time_cap() != Some(context.current_time.as_micros()?) {
            return Err(corpus_limit());
        }
        let table = relation.table(owner);
        if !crate::definitions::has_owned_search_text(snapshot, &table, &column)? {
            return Ok(());
        }
        let mut columns = vec!["_id", &column];
        if sealed { columns.push("_system_from"); }
        let Some(mut cursor) = rtdb_query::open_owned_root_projection(
            Arc::clone(context), owner, relation, valid, &columns,
        ).await? else { return Err(corpus_limit()); };
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let (id_column, id_field) = schema.column_with_name("_id").ok_or_else(corpus_limit)?;
            let (text_column, text_field) = schema.column_with_name(&column).ok_or_else(corpus_limit)?;
            let ids = batch.column(id_column).as_ref();
            let text = batch.column(text_column).as_ref();
            let logical = rtdb_columnar::from_field(text_field)?;
            for row in 0..batch.num_rows() {
                context.check_cancelled_at(row)?;
                let id_bytes = rtdb_columnar::estimate_read_value_bytes(id_field, ids, row)?;
                let _identity = self.memory.reserve(id_bytes.saturating_mul(2))?;
                let physical = rtdb_columnar::read_owned_entity_id(id_field, ids, row)?;
                let protected = sealed.then(|| crate::rank_family::RootProjectionId::parse(physical)).transpose()?;
                let id = protected.as_ref().map_or(physical, |id| id.canonical()).into_owned();
                if self.contains_shadow(partition, &id) { continue; }
                if self.roots.get(&partition).is_some_and(|rows| rows.contains_key(&id)) {
                    return Err(if sealed { crate::rank_family::unavailable() } else { corpus_limit() });
                }
                if let Some(protected) = protected {
                    protected.require(context, owner, relation, valid, batch, row).await?;
                }
                let value = match rtdb_columnar::owned_text_cell(&logical, text_field, text, row)? {
                    rtdb_columnar::TextCell::Utf8(text) => Some(text),
                    rtdb_columnar::TextCell::Missing => None,
                    rtdb_columnar::TextCell::Other => return Err(corpus_limit()),
                };
                let matched = self.observe(EntityIdRef::from(&id), value)?;
                self.roots.entry(partition).or_default().insert(id, matched);
            }
        }
        Ok(())
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

    pub(crate) fn rank(self, scorer: &Scorer, window: &mut OwnedRankWindow<'_>) -> Result<()> {
        let mut retained = self.retained;
        let shadows = self.shadows.into_iter().flat_map(|(partition, rows)| {
            rows.into_iter().map(move |(id, matched)| (partition, id.0, matched))
        });
        let roots = self.roots.into_iter().flat_map(|(partition, rows)| {
            rows.into_iter().map(move |(id, matched)| (partition, id, matched))
        });
        let matches = shadows.chain(roots).filter_map(|(partition, id, matched)| {
            matched.map(|matched| (partition, id, matched))
        });
        for (partition, id, matched) in matches {
            window.admit_candidate()?;
            let score = scorer.score(&matched.frequencies, matched.length);
            if !score.is_finite() {
                return Err(corpus_limit());
            }
            if let Some(at) = window.position(partition, EntityIdRef::from(&id), score) {
                let charge = Arc::get_mut(&mut retained).expect("exclusive corpus reservation")
                    .split_off(identity_charge(EntityIdRef::from(&id)))?;
                window.insert_at(at, partition, RankedIdentity::Held(id, charge), score);
            }
        }
        Ok(())
    }

    pub fn finish(self, offset: usize, limit: usize) -> Result<OwnedSearchPage> {
        self.finish_batch(offset, limit)?.into_page()
    }

    pub fn finish_batch(self, offset: usize, limit: usize) -> Result<OwnedSearchBatch> {
        if offset > 4096 || !(1..=128).contains(&limit) {
            return Err(corpus_limit());
        }
        let scorer = Scorer::new(self.query.clone(), Some(&self.corpus));
        let mut window = OwnedRankWindow::new(&self.memory, offset, limit)?;
        self.rank(&scorer, &mut window)?;
        window.finish_batch()
    }
}

fn identity_charge(id: EntityIdRef<'_>) -> usize {
    // Keep the charge for a compact copy and the original Value-sized allowance.
    std::mem::size_of::<Value>() + id.owned_payload_bytes()
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
    fn arrow_search_held_corpus_counts_phrase_misses_and_evicts_the_last_hit() {
        let memory = MemoryBudget::new(1 << 24);
        let query = r#""a b""#;
        let mut corpus = HeldTextCorpus::new(memory, rtdb_text::Analyzer::Simple, query).unwrap();
        for (id, text) in [(5, "a b"), (1, "b a"), (2, "a b"), (0, "a b a b")] {
            corpus.shadow(0, EntityId::Integer(id), Some(text)).unwrap();
        }
        assert_eq!(corpus.statistics().tokens, 10);
        assert_eq!(corpus.statistics().frequencies["a"], 4);
        let scorer = Scorer::new(Query::parse(query), Some(corpus.statistics()));
        let page = corpus.finish(1, 1).unwrap();
        assert_eq!(page.total, 3);
        assert_eq!(page.hits[0].id, EntityId::Integer(2));
        assert_eq!(page.hits[0].score.to_bits(), scorer.bm25_arrow(&rtdb_text::Analyzer::Simple.analyze("a b")).to_bits());
    }

    #[test]
    fn held_corpus_charges_each_typed_identity_once() {
        let mut text = String::with_capacity(1024);
        text.push_str("places/東京");
        let ids = [
            EntityId::String(text.clone()),
            EntityId::Keyword(rtdb_types::Keyword::from_name(text)),
            EntityId::Integer(i64::MIN),
            EntityId::Uuid([3; 16]),
        ];
        let mut corpus = HeldTextCorpus::new(MemoryBudget::new(1 << 20), rtdb_text::Analyzer::Simple, "apples").unwrap();
        for (partition, id) in ids.into_iter().enumerate() {
            let expected = id.as_value().retained_bytes().unwrap();
            let before = corpus.retained.bytes();
            let key = id.clone();
            corpus.shadow(partition, id, Some("apples")).unwrap();
            let matched = corpus.shadows[&partition][&key].as_ref().unwrap();
            assert_eq!(corpus.retained.bytes() - before, expected + 192 + 256 + 4 * matched.frequencies.capacity());
        }
        let before = corpus.retained.bytes();
        let id = EntityId::String("deleted".into());
        let expected = id.as_value().retained_bytes().unwrap();
        corpus.shadow(0, id, None).unwrap();
        assert_eq!(corpus.retained.bytes() - before, expected + 192);
    }

    #[test]
    fn held_shadows_reject_duplicate_typed_keys_and_rank_ties_by_key_then_partition() {
        let mut corpus = HeldTextCorpus::new(MemoryBudget::new(1 << 20), rtdb_text::Analyzer::Simple, "apples").unwrap();
        let string = EntityId::String("fruit/東京".into());
        let keyword = EntityId::Keyword(rtdb_types::Keyword::from_name("fruit/東京"));
        corpus.shadow(1, string.clone(), Some("apples")).unwrap();
        corpus.shadow(0, keyword.clone(), Some("apples")).unwrap();
        assert!(!corpus.contains_shadow_key(0, EntityIdRef::from(&string)));
        corpus.shadow(0, string.clone(), Some("apples")).unwrap();
        let before = corpus.retained.bytes();
        let error = corpus.shadow(0, string.clone(), Some("apples apples")).unwrap_err();
        assert_eq!(error.code, "rtdb.owned/search-held-corpus-limit");
        assert_eq!(corpus.retained.bytes(), before);
        assert_eq!(corpus.statistics().documents, 3);
        assert_eq!(corpus.statistics().tokens, 3);
        let binary = [0, 0xff, 0];
        corpus.shadow(0, EntityId::Binary(binary.to_vec()), None).unwrap();
        assert!(corpus.contains_shadow_key(0, EntityIdRef::Binary(&binary)));
        assert!(!corpus.contains_shadow_key(1, EntityIdRef::Binary(&binary)));
        let page = corpus.finish(0, 3).unwrap();
        assert_eq!(page.total, 3);
        assert_eq!(page.hits.into_iter().map(|hit| (hit.id, hit.partition)).collect::<Vec<_>>(),
            [(string.clone(), 0), (string, 1), (keyword, 0)]);
    }

    #[test]
    fn held_root_match_charges_one_typed_identity_and_ranks_it() {
        let mut corpus = HeldTextCorpus::new(
            MemoryBudget::new(1 << 20),
            rtdb_text::Analyzer::Simple,
            "apples",
        ).unwrap();
        let id = EntityId::String("places/東京".repeat(32));
        let key = EntityIdRef::from(&id);
        let before = corpus.retained.bytes();
        let matched = corpus.observe(key, Some("apples")).unwrap().unwrap();
        assert_eq!(
            corpus.retained.bytes() - before,
            identity_charge(key) + 192 + 256 + 4 * matched.frequencies.capacity(),
        );
        corpus.roots.entry(0).or_default().insert(id.clone(), Some(matched));
        let page = corpus.finish(0, 1).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.hits[0].id, id);
    }

    #[test]
    fn staged_arrow_shadows_borrow_text_and_replace_committed_documents() {
        let partitions = UInt64Array::from(vec![0, 0, 1, 1]);
        let ids = StringArray::from(vec!["replaced", "deleted", "empty", "staged"]);
        let text = StringArray::from(vec![Some("pears"), None, Some(""), Some("apples")]);
        let shadows = TextShadows::try_new(&partitions, &ids, &text, |_, _| true).unwrap();
        {
            let mut rows = shadows.rows();
            assert!(std::ptr::eq(rows.next().unwrap().2.unwrap(), text.value(0)));
            assert_eq!(rows.next().unwrap().2, None);
            assert_eq!(rows.next().unwrap().2, Some(""));
        }
        let mut corpus = shadows.into_corpus(
            MemoryBudget::new(1 << 20), rtdb_text::Analyzer::Simple, "apples", || Ok(()),
        ).unwrap();
        let replaced = EntityId::String("replaced".into());
        let deleted = EntityId::String("deleted".into());
        let unchanged = EntityId::String("unchanged".into());
        assert!(corpus.contains_shadow(0, &replaced));
        assert!(corpus.contains_shadow(0, &deleted));
        assert!(corpus.contains_shadow(1, &EntityId::String("empty".into())));
        assert!(corpus.contains_shadow_key(1, EntityIdRef::String("staged")));
        assert!(!corpus.contains_shadow(1, &replaced));
        for id in [replaced, deleted, unchanged.clone()] {
            if !corpus.contains_shadow(0, &id) {
                corpus.shadow(0, id, Some("apples")).unwrap();
            }
        }
        assert_eq!(corpus.statistics().documents, 4);
        let page = corpus.finish(0, 10).unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.hits[0].id, EntityId::String("staged".into()));
        assert_eq!(page.hits[0].partition, 1);
        assert_eq!(page.hits[1].id, unchanged);
    }
}
