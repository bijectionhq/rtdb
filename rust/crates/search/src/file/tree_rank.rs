use futures::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray, cast::AsArray, types::{Int64Type, UInt32Type}};
use rtdb_columnar::EntityIdRef;
use rtdb_text::{Query, RankScorer, stored_length};
use rtdb_types::{Iid, MemoryBudget, MemoryReservation, Result, TemporalBounds};

use super::{corrupt, meets, layout::{PageRef, Table, table}, paged::{self, Frequencies, PostingBlock},
    rank_tree::{self, RankLeaf, RankNode, RankWindow}, tree_file::{DOCUMENT, DOCUMENT_ROWS, POSTING, TreeFile}};
use crate::{member_bounds::{MemberStart, document_member}, rank_cutoff::RankKey, rank_windows::document_range};

struct Term {
    node: RankNode,
    inline: Option<Arc<RankLeaf>>,
    frequency: u32,
}

impl Term {
    fn read(page: Option<arrow_array::RecordBatch>, column: u32, text: &str, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
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
            Some(Arc::new(RankLeaf::read(blocks.column(0).as_list::<i64>(), ids.column(0).as_list::<i64>(), row, memory)?))
        } else { None };
        Ok(Some(Self { node, inline, frequency: terms.column(4).as_primitive::<UInt32Type>().value(row) }))
    }
}

struct LeafCache {
    entries: VecDeque<(PageRef, Arc<RankLeaf>)>,
    memory: Arc<MemoryBudget>,
}

impl LeafCache {
    async fn read(&mut self, file: &TreeFile, node: RankNode, reuse: bool) -> Result<Arc<RankLeaf>> {
        const MAX_CACHED_LEAVES: usize = 16;
        let page = node.page.ok_or_else(|| corrupt("Search rank leaf is inline"))?;
        if let Some(index) = self.entries.iter().position(|(cached, _)| *cached == page) {
            let entry = self.entries.remove(index).expect("cached rank leaf");
            let leaf = Arc::clone(&entry.1);
            self.entries.push_back(entry);
            return Ok(leaf);
        }
        let (blocks, ids) = rank_tree::leaf_arrays(file.reader(), node).await?;
        if reuse && self.entries.len() == MAX_CACHED_LEAVES { self.entries.pop_front(); }
        let blocks = blocks.column(0).as_list::<i64>();
        let ids = ids.column(0).as_list::<i64>();
        let leaf = self.admit(|memory| RankLeaf::read(blocks, ids, 0, memory))?;
        let leaf = Arc::new(leaf);
        if reuse { self.entries.push_back((page, Arc::clone(&leaf))); }
        Ok(leaf)
    }

    fn admit<T>(&mut self, allocate: impl Fn(&Arc<MemoryBudget>) -> Result<T>) -> Result<T> {
        match allocate(&self.memory) {
            Err(error) if error.code == "rtdb/memory-budget-exceeded" && !self.entries.is_empty() => {
                self.entries.clear();
                allocate(&self.memory)
            }
            result => result,
        }
    }
}

struct WindowRef {
    leaf: Arc<RankLeaf>,
    index: usize,
}

impl WindowRef {
    fn entry(&self) -> &RankWindow { &self.leaf.windows[self.index] }

    fn block(&self) -> BlockRef { BlockRef { leaf: Arc::clone(&self.leaf), block: self.entry().block } }
}

fn append_windows(windows: &mut BTreeMap<u32, WindowRef>, leaf: &Arc<RankLeaf>, leading: &[RankWindow]) {
    let first = leaf.windows[0].window;
    let last = leaf.windows.last().expect("a rank leaf contains a window").window;
    let start = leading.partition_point(|entry| entry.window < first);
    for entry in leading[start..].iter().take_while(|entry| entry.window <= last) {
        if let Ok(index) = leaf.windows.binary_search_by_key(&entry.window, |entry| entry.window) {
            windows.insert(entry.window, WindowRef { leaf: Arc::clone(leaf), index });
        }
    }
}

struct BlockRef {
    leaf: Arc<RankLeaf>,
    block: usize,
}

impl BlockRef {
    fn posting(&self) -> PostingBlock { self.leaf.blocks[self.block].posting }

    fn windows(&self) -> impl Iterator<Item = u32> + '_ {
        self.leaf.windows[self.leaf.blocks[self.block].windows.clone()].iter().map(|entry| entry.window)
    }
}

struct WindowWork {
    blocks: Vec<Option<BlockRef>>,
    _reservation: Arc<MemoryReservation>,
}

enum Job {
    Node(RankNode),
    Window(WindowWork),
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
    ready_charge: Arc<MemoryReservation>,
    _plan: Arc<MemoryReservation>,
    leaves: LeafCache,
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
}

impl TreeRanked {
    pub async fn open(file: Arc<TreeFile>, column: usize, matching: &Query, scoring: &RankScorer,
        bounds: TemporalBounds, order: TreeRankOrder, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let term_bytes = matching.terms().iter().chain(scoring.scorer().query().terms())
            .fold(4096usize, |bytes, term| bytes.saturating_add(256).saturating_add(term.len().saturating_mul(4)));
        let phrase_bytes = matching.phrases().iter()
            .fold(0usize, |bytes, phrase| bytes.saturating_add(128).saturating_add(phrase.len().saturating_mul(size_of::<usize>())));
        let members = table(file.root(), Table::Members);
        let plan = memory.reserve(term_bytes.saturating_add(phrase_bytes)
            .saturating_add(members.len().saturating_mul(size_of::<MemberStart>())))?;
        let term_ids: BTreeMap<_, _> = matching.terms().iter().chain(scoring.scorer().query().terms())
            .collect::<BTreeSet<_>>().into_iter().enumerate().map(|(index, term)| (term.as_str(), index)).collect();
        let names = term_ids.keys().map(|term| term.to_string()).collect::<Vec<_>>();
        let pages = file.term_pages(column as u32, &names).await?;
        let terms = pages.into_iter().zip(names).map(|(page, term)| Term::read(page, column as u32, &term, memory)).collect::<Result<Vec<_>>>()?;
        let phrases: Vec<Vec<usize>> = matching.phrases().iter()
            .map(|phrase| phrase.iter().map(|term| term_ids[term.as_str()]).collect()).collect();
        let matching: Vec<_> = matching.terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let score_terms: Vec<_> = scoring.scorer().query().terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let pages = table(file.root(), Table::Pages);
        if pages.len() != 2 { return Err(corrupt("Search root omits its document count")); }
        let documents = pages.column(0).as_primitive::<UInt32Type>().value(1);
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
            ready: BinaryHeap::new(), ready_charge: memory.reserve(0)?, _plan: plan, leaves: LeafCache { entries: VecDeque::new(), memory: Arc::clone(memory) }, members, documents, bounds, order,
            checked_frequencies: BTreeSet::new(), checked_positions: BTreeSet::new(), sparse };
        if let Some(leading) = leading {
            let node = rank.terms[leading].as_ref().expect("leading term is present").node;
            let (floors, _scratch) = rank.uuid_floors(std::iter::once(node.first), 1).await?;
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
        Self::admit_frontier(&mut self.ready, &mut self.ready_charge, 1)?;
        self.ready.push(Work { key: self.key(score, integer, floor), first: node.first, job: Job::Node(node) });
        Ok(())
    }

    async fn open_node(&mut self, node: RankNode) -> Result<()> {
        let leading = self.leading.expect("only matching sources enter the rank frontier");
        if node.height != 0 {
            let _children = self.leaves.admit(|memory| memory.reserve(4096))?;
            let children = rank_tree::children(self.file.reader(), node).await?;
            let (floors, _scratch) = self.uuid_floors(children.iter().map(|child| child.first), children.len()).await?;
            for child in children { self.push_node(leading, child, floors.get(&child.first).copied())?; }
            return Ok(());
        }
        let term = self.terms[leading].as_ref().expect("leading term is present");
        let leading_leaf = if let Some(inline) = &term.inline { Arc::clone(inline) }
            else { self.leaves.read(&self.file, node, false).await? };
        let start = leading_leaf.windows.partition_point(|entry| entry.window < node.first);
        let end = leading_leaf.windows.partition_point(|entry| entry.window <= node.last);
        let leading_windows = &leading_leaf.windows[start..end];
        let scratch_bytes = rank_tree::TRAVERSAL_BYTES.saturating_add(
            self.terms.len().saturating_mul(128usize.saturating_add(leading_windows.len().saturating_mul(256))));
        let _intersection = self.leaves.admit(|memory| memory.reserve(scratch_bytes))?;
        let mut windows = Vec::with_capacity(self.terms.len());
        for (index, term) in self.terms.iter().enumerate() {
            let mut selected = BTreeMap::new();
            if index != leading && let Some(term) = term {
                let leaves = rank_tree::leaf_stream(self.file.reader(), term.node, node.first, node.last);
                futures::pin_mut!(leaves);
                while let Some(leaf) = leaves.try_next().await? {
                    let leaf = if let Some(inline) = &term.inline { Arc::clone(inline) }
                        else { self.leaves.read(&self.file, leaf, true).await? };
                    append_windows(&mut selected, &leaf, leading_windows);
                }
            }
            windows.push(selected);
        }
        let matching = &self.matching;
        let matching_windows = || leading_windows.iter()
            .filter(|entry| matching.iter().all(|&term| term == leading || windows[term].contains_key(&entry.window)));
        let count = matching_windows().count();
        if count == 0 { return Ok(()); }
        let (floors, _scratch) = self.uuid_floors(matching_windows().map(|entry| entry.window), count).await?;
        Self::admit_frontier(&mut self.ready, &mut self.ready_charge, count)?;
        let bytes = 256usize.saturating_add(count.saturating_mul(self.terms.len()).saturating_mul(size_of::<Option<BlockRef>>()));
        let reservation = self.leaves.admit(|memory| memory.reserve(bytes))?;
        for lead in matching_windows() {
            let window = lead.window;
            let entry = |term: usize| if term == leading { Some(lead) }
                else { windows[term].get(&window).map(WindowRef::entry) };
            let mut score = 0.0;
            for (index, &term) in self.scoring.iter().enumerate() {
                if let Some(entry) = entry(term) {
                    score += self.term_score(index, entry.unit_score, entry.frequency, entry.length);
                }
            }
            let integer = match self.scoring.as_slice() {
                [term] if self.scorer.scorer().uses_unit_term_scores() && score > 0.0 =>
                    entry(*term).and_then(|entry| entry.peak).or(lead.minimum),
                _ => lead.minimum,
            };
            let blocks = windows.iter().enumerate().map(|(term, windows)| {
                if term == leading { Some(BlockRef { leaf: Arc::clone(&leading_leaf), block: lead.block }) }
                else { windows.get(&window).map(WindowRef::block) }
            }).collect();
            self.ready.push(Work { key: self.key(score, integer, floors.get(&window).copied()), first: window,
                job: Job::Window(WindowWork { blocks, _reservation: Arc::clone(&reservation) }) });
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

    fn admit_frontier(ready: &mut BinaryHeap<Work>, charge: &mut Arc<MemoryReservation>, extra: usize) -> Result<()> {
        let required = ready.len().saturating_add(extra);
        if required <= ready.capacity() { return Ok(()); }
        let capacity = required.max(ready.capacity().saturating_mul(2)).max(8);
        let charge = Arc::get_mut(charge).expect("rank frontier owns its reservation");
        charge.grow_to(capacity.saturating_add(ready.capacity()).saturating_mul(size_of::<Work>()))?;
        ready.try_reserve_exact(capacity - ready.len()).map_err(super::fault)?;
        charge.shrink_to(ready.capacity().saturating_mul(size_of::<Work>()))
    }

    async fn uuid_floors(&self, windows: impl Iterator<Item = u32>, count: usize)
        -> Result<(BTreeMap<u32, Iid>, Option<Arc<MemoryReservation>>)> {
        if self.order != TreeRankOrder::Uuid { return Ok((BTreeMap::new(), None)); }
        let scratch = self.leaves.memory.reserve(4096usize.saturating_add(count.saturating_mul(256)))?;
        let documents = windows.map(|window| (window, document_range(window).0)).collect::<Vec<_>>();
        let pages = self.pages(DOCUMENT, documents.iter().map(|&(_, doc)| document_page(doc)).collect()).await?;
        // A published tree writes all documents in nondecreasing IID order, including across members.
        // The first document of the first window bounds every posting that can enter the work item.
        let floors = documents.into_iter().map(|(window, doc)| {
            let expected = document_page(doc);
            let batch = &pages[&expected];
            let starts = table(batch, Table::Pages);
            let start = starts.column(0).as_primitive::<UInt32Type>();
            if doc >= self.documents || starts.len() != 1 || start.is_null(0) || start.value(0) as usize != expected {
                return Err(corrupt("Search UUID floor has an invalid document page"));
            }
            Ok((window, Document::iid_at(&table(batch, Table::Runs), (doc - start.value(0)) as usize)?))
        }).collect::<Result<_>>()?;
        Ok((floors, Some(scratch)))
    }

    async fn pages(&self, kind: u8, pages: BTreeSet<usize>) -> Result<BTreeMap<usize, RecordBatch>> {
        self.file.find_ordinals(kind, pages).await
    }

    pub async fn expand(&mut self, scope: Option<rtdb_query::IidScope<'_>>, visited: &mut usize) -> Result<Vec<TreeCandidate>> {
        if let Some(Work { job: Job::Node(node), .. }) = self.ready.peek() {
            let node = *node;
            self.ready.pop();
            if self.sparse {
                let _traversal = self.leaves.admit(|memory| memory.reserve(rank_tree::TRAVERSAL_BYTES))?;
                let file = Arc::clone(&self.file);
                let leaves = rank_tree::leaf_stream(file.reader(), node, node.first, node.last);
                futures::pin_mut!(leaves);
                while let Some(leaf) = leaves.try_next().await? { self.open_node(leaf).await?; }
            } else {
                self.open_node(node).await?;
            }
            if self.sparse || self.ready.is_empty() { self.leaves.entries.clear(); }
            return Ok(Vec::new());
        }
        let mut windows = BTreeMap::new();
        let batch = if self.order == TreeRankOrder::Uuid { 1 } else if self.sparse { 16 } else { 4 };
        while windows.len() < batch && self.ready.peek().is_some_and(|work| matches!(work.job, Job::Window(_))) {
            let Some(Work { first, job: Job::Window(window), .. }) = self.ready.pop() else { unreachable!() };
            windows.insert(first, window);
        }
        if self.ready.is_empty() { self.leaves.entries.clear(); }
        let key = |block: &BlockRef| {
            let posting = block.posting();
            (posting.page, posting.slot)
        };
        let blocks = windows.values().flat_map(|window| window.blocks.iter().flatten())
            .map(|block| (key(block), block)).collect::<BTreeMap<_, _>>();
        let frequency_pages = self.pages(POSTING, blocks.keys().map(|&(page, _)| page).collect()).await?;
        let mut frequency = BTreeMap::new();
        for (&address, block) in &blocks {
            let checked = was_checked(&mut self.checked_frequencies, address);
            let posting = block.posting();
            let postings = Frequencies::checked_batch(&frequency_pages[&posting.page], posting, block.windows(), self.documents, checked)?;
            frequency.insert(address, postings);
        }
        let in_window = |term: usize, window| windows[&window].blocks[term].as_ref().map(|block| &frequency[&key(block)]);
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
                let blocks = &windows[&window].blocks;
                phrases.iter().filter_map(move |&term| blocks[term].as_ref().map(|block| key(block)))
            }).collect();
            let position_pages = self.pages(POSTING, position_keys.iter().map(|&(page, _)| page + 1).collect()).await?;
            let mut positions = BTreeMap::new();
            for address in position_keys {
                let (page, slot) = address;
                let checked = was_checked(&mut self.checked_positions, address);
                positions.insert(address, frequency[&address].checked_positions(&position_pages[&(page + 1)], slot, checked)?);
            }
            nominated.retain(|&(window, doc)| {
                let occurrences = |term: usize| {
                    let block = windows[&window].blocks[term].as_ref().expect("phrase terms occur in this window");
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
                key: self.key(score, record.id, Some(record.iid)), revision });
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
