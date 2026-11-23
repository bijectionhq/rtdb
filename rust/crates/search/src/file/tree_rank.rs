use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray, cast::AsArray, types::{Int64Type, UInt32Type}};
use rtdb_columnar::EntityIdRef;
use rtdb_text::{Query, RankScorer, stored_length};
use rtdb_types::{Iid, Result, TemporalBounds};

use super::{corrupt, meets, layout::{Table, table}, paged::{self, Block, Frequencies}, postings::Impact, rank_tree::{self, RankLeaf, RankNode, WindowIds},
    tree_file::{DOCUMENT, DOCUMENT_ROWS, POSTING, TreeFile}};
use crate::{member_bounds::{MemberStart, document_member}, rank_cutoff::RankKey, rank_windows::document_range};

struct Term {
    node: RankNode,
    inline: Option<Arc<RankLeaf>>,
    frequency: u32,
}

type RankBlock = (Arc<Block>, Impact, WindowIds);

impl Term {
    fn read(page: Option<arrow_array::RecordBatch>, column: u32, text: &str) -> Result<Option<Self>> {
        let Some(page) = page else { return Ok(None); };
        let terms = table(&page, Table::Terms);
        let columns = terms.column(0).as_primitive::<UInt32Type>();
        let names = terms.column(1).as_string::<i32>();
        let Some(row) = (0..terms.len()).find(|&row| columns.value(row) == column && names.value(row) == text) else { return Ok(None); };
        let directory = table(&page, Table::RankNodes);
        let node = rank_tree::node_at(&directory, row)?;
        let inline = if node.page.is_none() {
            let blocks = table(&page, Table::Blocks);
            let ids = table(&page, Table::ImpactIds);
            Some(Arc::new(RankLeaf::read(blocks.column(0).as_list::<i64>(), ids.column(0).as_list::<i64>(), row)?))
        } else { None };
        Ok(Some(Self { node, inline, frequency: terms.column(4).as_primitive::<UInt32Type>().value(row) }))
    }

    async fn leaf(&self, file: &TreeFile, node: RankNode) -> Result<Arc<RankLeaf>> {
        if let Some(leaf) = &self.inline { return Ok(Arc::clone(leaf)); }
        let (blocks, ids) = rank_tree::leaf_arrays(file.reader(), node).await?;
        Ok(Arc::new(RankLeaf::read(blocks.column(0).as_list::<i64>(), ids.column(0).as_list::<i64>(), 0)?))
    }

    async fn windows(&self, file: &TreeFile, first: u32, last: u32) -> Result<BTreeMap<u32, RankBlock>> {
        let mut windows = BTreeMap::new();
        for node in rank_tree::leaves(file.reader(), self.node, first, last).await? {
            let leaf = self.leaf(file, node).await?;
            append_windows(&mut windows, &leaf, first, last);
        }
        Ok(windows)
    }
}

fn append_windows(windows: &mut BTreeMap<u32, RankBlock>, leaf: &RankLeaf, first: u32, last: u32) {
    let blocks = leaf.postings.blocks.iter().cloned().map(Arc::new).collect::<Vec<_>>();
    for (&(window, block, impact), &ids) in leaf.postings.windows.iter().zip(&leaf.ids) {
        if (first..=last).contains(&window) {
            windows.insert(window, (Arc::clone(&blocks[block]), impact, ids));
        }
    }
}

enum Job {
    Node(RankNode),
    Windows(Vec<Option<Arc<Block>>>),
}

struct Work {
    key: RankKey,
    first: u32,
    job: Job,
}

impl Ord for Work {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.compare(other.key).then_with(|| other.first.cmp(&self.first))
    }
}
impl PartialOrd for Work { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl PartialEq for Work { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Work {}

pub(crate) struct TreeRanked {
    file: Arc<TreeFile>,
    terms: Vec<Option<Term>>,
    leading: Option<usize>,
    matching: Vec<usize>,
    scoring: Vec<usize>,
    phrases: Vec<Vec<usize>>,
    scorer: RankScorer,
    ready: BinaryHeap<Work>,
    members: Vec<MemberStart>,
    documents: u32,
    bounds: TemporalBounds,
    order: TreeRankOrder,
    checked_frequencies: BTreeSet<(usize, usize)>,
    checked_positions: BTreeSet<(usize, usize)>,
    sparse: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TreeRankOrder { Score, Integer, Uuid }

pub(crate) struct TreeCandidate {
    pub iid: Iid,
    pub member: u32,
    pub document: u32,
    pub key: RankKey,
    pub revision: Option<super::TextRevision>,
    pub rectangles: Vec<[i64; 4]>,
}

impl TreeRanked {
    pub async fn open(file: Arc<TreeFile>, column: usize, matching: &Query, scoring: &RankScorer,
        bounds: TemporalBounds, order: TreeRankOrder) -> Result<Self> {
        let term_ids: BTreeMap<_, _> = matching.terms().iter().chain(scoring.scorer().query().terms())
            .collect::<BTreeSet<_>>().into_iter().enumerate().map(|(index, term)| (term.as_str(), index)).collect();
        let names = term_ids.keys().map(|term| term.to_string()).collect::<Vec<_>>();
        let pages = file.term_pages(column as u32, &names).await?;
        let terms = pages.into_iter().zip(names).map(|(page, term)| Term::read(page, column as u32, &term)).collect::<Result<Vec<_>>>()?;
        let phrases: Vec<Vec<usize>> = matching.phrases().iter()
            .map(|phrase| phrase.iter().map(|term| term_ids[term.as_str()]).collect()).collect();
        let matching: Vec<_> = matching.terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let score_terms: Vec<_> = scoring.scorer().query().terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let pages = table(file.root(), Table::Pages);
        if pages.len() != 2 { return Err(corrupt("Search root omits its document count")); }
        let documents = pages.column(0).as_primitive::<UInt32Type>().value(1);
        let members = table(file.root(), Table::Members);
        let pages = members.column(0).as_primitive::<UInt32Type>();
        let starts = members.column(1).as_primitive::<UInt32Type>();
        if members.is_empty() || starts.value(0) != 0 || starts.values().windows(2).any(|ids| ids[0] > ids[1])
            || starts.values().last().is_some_and(|&id| id > documents) {
            return Err(corrupt("Search root member positions are invalid"));
        }
        let members = pages.values().iter().zip(starts.values()).map(|(&page, &doc)| MemberStart { page, doc }).collect();
        let leading = if matching.iter().all(|&index| terms[index].is_some()) {
            matching.iter().copied().min_by_key(|&index| terms[index].as_ref().expect("matching term is present").frequency)
        } else { None };
        let sparse = file.format() == super::LOGICAL_FORMAT_VERSION && (matching.len() > 1 || !phrases.is_empty())
            && leading.is_some_and(|index| terms[index].as_ref().is_some_and(|term| term.frequency <= 1024));
        let mut rank = Self { file, terms, leading, matching, scoring: score_terms, phrases, scorer: scoring.clone(),
            ready: BinaryHeap::new(), members, documents, bounds, order,
            checked_frequencies: BTreeSet::new(), checked_positions: BTreeSet::new(), sparse };
        if let Some(leading) = leading {
            let node = rank.terms[leading].as_ref().expect("leading term is present").node;
            let floors = rank.uuid_floors(std::iter::once(node.first)).await?;
            rank.push_node(leading, node, floors.get(&node.first).copied())?;
        }
        Ok(rank)
    }

    pub fn bound(&self) -> Option<RankKey> { self.ready.peek().map(|work| work.key) }

    fn term_score(&self, index: usize, score: f64, frequency: u32, length: u32) -> f64 {
        if self.scorer.scorer().uses_unit_term_scores() { score }
        else { self.scorer.term_bound(index, frequency, length) }
    }

    fn push_node(&mut self, leading: usize, node: RankNode, floor: Option<Iid>) -> Result<()> {
        if document_range(node.last).0 >= self.documents {
            return Err(corrupt("Search window exceeds its document directory"));
        }
        let mut score = 0.0;
        for (index, &term) in self.scoring.iter().enumerate() {
            if let Some(term_node) = self.terms[term].as_ref().map(|term| term.node) {
                let summary = if term == leading { node } else { term_node };
                score += self.term_score(index, summary.unit_score, summary.frequency, summary.length);
            }
        }
        let integer = if self.scoring.as_slice() == [leading] && self.scorer.scorer().uses_unit_term_scores() {
            node.peak.or(node.minimum)
        } else { node.minimum };
        self.ready.push(Work { key: self.key(score, integer, floor), first: node.first, job: Job::Node(node) });
        Ok(())
    }

    async fn open_node(&mut self, node: RankNode) -> Result<()> {
        let leading = self.leading.expect("only matching sources enter the rank frontier");
        if node.height != 0 {
            let children = rank_tree::children(self.file.reader(), node).await?;
            let floors = self.uuid_floors(children.iter().map(|child| child.first)).await?;
            for child in children { self.push_node(leading, child, floors.get(&child.first).copied())?; }
            return Ok(());
        }
        let leaf = self.terms[leading].as_ref().expect("leading term is present").leaf(&self.file, node).await?;
        let mut windows = Vec::with_capacity(self.terms.len());
        for (index, term) in self.terms.iter().enumerate() {
            let bound = if index == leading {
                let mut windows = BTreeMap::new();
                append_windows(&mut windows, &leaf, node.first, node.last);
                windows
            } else if let Some(term) = term {
                term.windows(&self.file, node.first, node.last).await?
            } else { BTreeMap::new() };
            windows.push(bound);
        }
        let matching = &self.matching;
        let matching_windows = || windows[leading].iter()
            .filter(|&(&window, _)| matching.iter().all(|&term| windows[term].contains_key(&window)));
        let floors = self.uuid_floors(matching_windows().map(|(&window, _)| window)).await?;
        for (&window, (_, _, ids)) in matching_windows() {
            let mut score = 0.0;
            for (index, &term) in self.scoring.iter().enumerate() {
                if let Some((_, impact, _)) = windows[term].get(&window) {
                    score += self.term_score(index, impact.max_unit_score, impact.frequency, impact.length);
                }
            }
            let integer = match self.scoring.as_slice() {
                [term] if self.scorer.scorer().uses_unit_term_scores() && score > 0.0 =>
                    windows[*term].get(&window).and_then(|(_, _, ids)| ids.peak).or(ids.minimum),
                _ => ids.minimum,
            };
            let blocks = windows.iter().map(|term| term.get(&window).map(|(block, _, _)| Arc::clone(block))).collect();
            self.ready.push(Work { key: self.key(score, integer, floors.get(&window).copied()), first: window, job: Job::Windows(blocks) });
        }
        Ok(())
    }

    fn key(&self, score: f64, integer: Option<i64>, iid: Option<Iid>) -> RankKey {
        match self.order {
            TreeRankOrder::Score => RankKey::integer(score, None),
            TreeRankOrder::Integer => RankKey::integer(score, integer),
            TreeRankOrder::Uuid => iid.map_or_else(|| RankKey::integer(score, None), |iid| RankKey::uuid(score, iid.0)),
        }
    }

    async fn uuid_floors(&self, windows: impl Iterator<Item = u32>) -> Result<BTreeMap<u32, Iid>> {
        if self.order != TreeRankOrder::Uuid { return Ok(BTreeMap::new()); }
        let documents = windows.map(|window| (window, document_range(window).0)).collect::<Vec<_>>();
        let pages = self.pages(DOCUMENT, documents.iter().map(|&(_, doc)| document_page(doc)).collect()).await?;
        // A published tree writes all documents in nondecreasing IID order, including across members.
        // The first document of the first window bounds every posting that can enter the work item.
        documents.into_iter().map(|(window, doc)| {
            let expected = document_page(doc);
            let batch = &pages[&expected];
            let starts = table(batch, Table::Pages);
            let start = starts.column(0).as_primitive::<UInt32Type>();
            if doc >= self.documents || starts.len() != 1 || start.is_null(0) || start.value(0) as usize != expected {
                return Err(corrupt("Search UUID floor has an invalid document page"));
            }
            Ok((window, Document::iid_at(&table(batch, Table::Runs), (doc - start.value(0)) as usize)?))
        }).collect()
    }

    async fn pages(&self, kind: u8, pages: BTreeSet<usize>) -> Result<BTreeMap<usize, RecordBatch>> {
        self.file.find_ordinals(kind, pages).await
    }

    pub async fn expand(&mut self, scope: Option<&BTreeSet<Iid>>, visited: &mut usize) -> Result<Vec<TreeCandidate>> {
        if let Some(Work { job: Job::Node(node), .. }) = self.ready.peek() {
            let node = *node;
            self.ready.pop();
            if self.sparse {
                for leaf in rank_tree::leaves(self.file.reader(), node, node.first, node.last).await? {
                    self.open_node(leaf).await?;
                }
            } else {
                self.open_node(node).await?;
            }
            return Ok(Vec::new());
        }
        let mut windows = BTreeMap::new();
        let batch = if self.order == TreeRankOrder::Uuid { 1 } else if self.sparse { 16 } else { 4 };
        while windows.len() < batch && self.ready.peek().is_some_and(|work| matches!(work.job, Job::Windows(_))) {
            let Some(Work { first, job: Job::Windows(blocks), .. }) = self.ready.pop() else { unreachable!() };
            windows.insert(first, blocks);
        }
        let key = |block: &Block| (block.page, block.slot);
        let blocks = windows.values().flatten().flatten().map(|block| (key(block), block.as_ref())).collect::<BTreeMap<_, _>>();
        let frequency_pages = self.pages(POSTING, blocks.keys().map(|&(page, _)| page).collect()).await?;
        let mut frequency = BTreeMap::new();
        for (&address, block) in &blocks {
            let checked = was_checked(&mut self.checked_frequencies, address);
            let postings = Frequencies::checked_batch(&frequency_pages[&block.page], block, self.documents, checked)?;
            frequency.insert(address, postings);
        }
        let in_window = |term: usize, window| windows[&window][term].as_ref().map(|block| &frequency[&key(block)]);
        let mut nominated = Vec::new();
        for window in windows.keys().copied() {
            let (start, end) = document_range(window);
            let lists: Vec<_> = self.matching.iter().map(|&term| {
                let posting = in_window(term, window).expect("matching terms occur in this window");
                (posting, posting.window(start, end))
            }).collect();
            let Some((lead, rows)) = lists.iter().min_by_key(|entry| entry.1.len()) else { continue };
            for row in rows.clone() {
                *visited += 1;
                let doc = lead.document(row);
                if lists.iter().all(|(posting, _)| posting.index(doc).is_some()) { nominated.push((window, doc)); }
            }
        }
        if !self.phrases.is_empty() {
            let phrases: BTreeSet<_> = self.phrases.iter().flatten().copied().collect();
            let position_keys: BTreeSet<_> = nominated.iter().flat_map(|&(window, _)| {
                let blocks = &windows[&window];
                phrases.iter().filter_map(move |&term| blocks[term].as_ref().map(|block| key(block)))
            }).collect();
            let position_pages = self.pages(POSTING, position_keys.iter().map(|&(page, _)| page + 1).collect()).await?;
            let mut positions = BTreeMap::new();
            for address in position_keys {
                let block = blocks[&address];
                let checked = was_checked(&mut self.checked_positions, address);
                positions.insert(address, frequency[&address].checked_positions(&position_pages[&(block.page + 1)], block, checked)?);
            }
            nominated.retain(|&(window, doc)| {
                let occurrences = |term: usize| {
                    let block = windows[&window][term].as_ref().expect("phrase terms occur in this window");
                    let address = key(block);
                    let row = frequency[&address].index(doc).expect("phrase terms occur in this document");
                    paged::position_values(&positions[&address], row)
                };
                self.phrases.iter().all(|phrase| occurrences(phrase[0]).iter().any(|&first| {
                    phrase.iter().skip(1).enumerate().all(|(offset, &term)| {
                        first.checked_add(offset as u32 + 1).is_some_and(|at| occurrences(term).binary_search(&at).is_ok())
                    })
                }))
            });
        }
        let records = self.pages(DOCUMENT, nominated.iter().map(|&(_, doc)| document_page(doc)).collect()).await?;
        let mut docs = BTreeMap::new();
        for (&requested, page) in &records {
            let starts = table(page, Table::Pages);
            let start = starts.column(0).as_primitive::<UInt32Type>();
            if starts.len() != 1 || start.is_null(0) || start.value(0) as usize > requested {
                return Err(corrupt("Search document directory has an invalid page start"));
            }
            let revisions = (self.file.format() == super::LOGICAL_FORMAT_VERSION).then(|| table(page, Table::TokenSequences));
            docs.insert(requested, (start.value(0), table(page, Table::Runs), revisions));
        }
        let mut candidates = Vec::new();
        let mut frequencies = Vec::with_capacity(self.scoring.len());
        for (window, doc) in nominated {
            let start = document_page(doc);
            let (first, runs, revisions) = &docs[&start];
            let record = Document::at(runs, (doc - first) as usize)?;
            if scope.is_some_and(|scope| !scope.contains(&record.iid))
                || !record.visibility.iter().any(|&rect| meets(rect, self.bounds)) { continue; }
            frequencies.clear();
            frequencies.extend(self.scoring.iter().map(|&term| in_window(term, window).map_or(0, |posting| posting.frequency(doc))));
            let score = self.scorer.scorer().score(&frequencies, stored_length(record.length as usize));
            let revision = revisions.as_ref().map(|sequences| {
                let row = (doc - first) as usize;
                if row >= sequences.len() { return Err(corrupt("Ranked document has no text revision")); }
                sequences.column(1).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search revision layout")
                    .value(row).try_into().map_err(|_| corrupt("Ranked document has an invalid text revision"))
            }).transpose()?;
            candidates.push(TreeCandidate { iid: record.iid, member: document_member(&self.members, doc) as u32, document: doc,
                key: self.key(score, record.id, Some(record.iid)), rectangles: record.visibility, revision });
        }
        Ok(candidates)
    }
}

fn document_page(doc: u32) -> usize { doc as usize / DOCUMENT_ROWS * DOCUMENT_ROWS }

fn was_checked(checked: &mut BTreeSet<(usize, usize)>, address: (usize, usize)) -> bool {
    const MAX_VALIDATED_SLICES: usize = 1024;
    if checked.len() < MAX_VALIDATED_SLICES { !checked.insert(address) }
    else { checked.contains(&address) }
}

struct Document { iid: Iid, id: Option<i64>, length: u32, visibility: Vec<[i64; 4]> }

impl Document {
    fn iid_at(runs: &StructArray, row: usize) -> Result<Iid> {
        let iids = runs.column(0).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search IID layout");
        if row >= runs.len() || iids.is_null(row) { return Err(corrupt("Search document exceeds its page or has no IID")); }
        Ok(Iid(iids.value(row).try_into().expect("fixed-width search IID")))
    }

    fn at(runs: &StructArray, row: usize) -> Result<Self> {
        let iid = Self::iid_at(runs, row)?;
        let identities = runs.column(2).as_binary::<i32>();
        let id = identities.is_valid(row).then(|| EntityIdRef::from_handle_bytes(identities.value(row)).ok()).flatten()
            .and_then(|id| match id { EntityIdRef::Integer(id) => Some(id), _ => None });
        let all = runs.column(4).as_list::<i64>().value(row);
        let all = all.as_struct();
        let bounds: [_; 4] = std::array::from_fn(|column| all.column(column).as_primitive::<Int64Type>());
        let visibility = (0..all.len()).map(|row| bounds.map(|column| column.value(row))).collect();
        Ok(Self { iid, id, length: runs.column(1).as_primitive::<UInt32Type>().value(row), visibility })
    }
}
