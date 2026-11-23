use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, LargeListArray, StructArray, UInt32Array, cast::AsArray, types::{Float64Type, UInt32Type}};
use rtdb_text::{Query, RankScorer, Scorer, stored_length};
use rtdb_types::{Iid, Result, TemporalBounds};

use super::{ScoredRuns, SearchFile, corrupt, fault, layout::{self, Table, item_of, list}, postings::{Impact, PostingBuilder, PostingValues, Postings, POSTING_TERMS}};
use crate::{rank_cutoff::RankKey, rank_windows::{document_range, window_of}};

const TARGET_POSTINGS: usize = 2048;
pub(super) const FIRST_POSTING_PAGE: usize = 2;

#[derive(Clone)]
pub(super) struct Block {
    pub page: usize,
    pub slot: usize,
    pub count: usize,
    pub impacts: Vec<Impact>,
}

pub(super) struct PostingPlan {
    terms: Vec<Vec<Block>>,
    groups: Vec<Vec<PostingBuilder>>,
}

impl PostingPlan {
    pub fn new(terms: impl IntoIterator<Item = PostingBuilder>) -> Self {
        let mut plan = Self { terms: Vec::new(), groups: Vec::new() };
        let mut small = Vec::new();
        for term in terms {
            let mut blocks = Vec::new();
            if term.len() <= TARGET_POSTINGS {
                if small.len() == POSTING_TERMS {
                    plan.groups.push(std::mem::take(&mut small));
                }
                blocks.push(plan.block(small.len(), &term));
                small.push(term);
            } else {
                if !small.is_empty() { plan.groups.push(std::mem::take(&mut small)); }
                let mut first = 0;
                let mut first_position = 0;
                let mut first_impact = 0;
                while first < term.len() {
                    let candidate = (first + TARGET_POSTINGS).min(term.len()) - 1;
                    let last_window = window_of(term.documents[candidate]);
                    let end = candidate + 1 + term.documents[candidate + 1..]
                        .partition_point(|&doc| window_of(doc) == last_window);
                    let end_position = first_position + term.position_counts[first..end].iter().sum::<usize>();
                    let end_impact = first_impact + term.windows[first_impact..].partition_point(|impact| impact.window <= last_window);
                    let chunk = PostingBuilder {
                        documents: term.documents[first..end].to_vec(),
                        position_counts: term.position_counts[first..end].to_vec(),
                        positions: term.positions[first_position..end_position].to_vec(),
                        windows: term.windows[first_impact..end_impact].to_vec(),
                    };
                    blocks.push(plan.block(0, &chunk));
                    plan.groups.push(vec![chunk]);
                    first = end;
                    first_position = end_position;
                    first_impact = end_impact;
                }
            }
            plan.terms.push(blocks);
        }
        if !small.is_empty() { plan.groups.push(small); }
        plan
    }

    fn block(&self, slot: usize, term: &PostingBuilder) -> Block {
        Block { page: FIRST_POSTING_PAGE + self.groups.len() * 2, slot, count: term.len(), impacts: term.windows.clone() }
    }

    pub fn directory(&self) -> Result<StructArray> {
        let block_item = item_of(&Table::Blocks.fields()[0]);
        let arrow_schema::DataType::Struct(block_fields) = block_item.data_type() else { unreachable!() };
        let impact_item = item_of(&block_fields[3]);
        let arrow_schema::DataType::Struct(impact_fields) = impact_item.data_type() else { unreachable!() };
        let blocks: Vec<_> = self.terms.iter().flatten().collect();
        let impact_values = || blocks.iter().flat_map(|block| &block.impacts);
        let impacts = StructArray::try_new(impact_fields.clone(), vec![
            Arc::new(UInt32Array::from_iter_values(impact_values().map(|impact| impact.window))),
            Arc::new(UInt32Array::from_iter_values(impact_values().map(|impact| impact.frequency))),
            Arc::new(UInt32Array::from_iter_values(impact_values().map(|impact| impact.length))),
            Arc::new(BooleanArray::from_iter(impact_values().map(|impact| Some(impact.same_frequency)))),
            Arc::new(BooleanArray::from_iter(impact_values().map(|impact| Some(impact.same_length)))),
            Arc::new(Float64Array::from_iter_values(impact_values().map(|impact| impact.max_unit_score))),
        ], None).map_err(fault)?;
        let column = |value: fn(&Block) -> usize| {
            blocks.iter().map(|block| u32::try_from(value(block)).map_err(fault)).collect::<Result<Vec<_>>>()
                .map(|values| Arc::new(UInt32Array::from(values)) as arrow_array::ArrayRef)
        };
        let blocks_table = StructArray::try_new(block_fields.clone(), vec![
            column(|block| block.page)?, column(|block| block.slot)?, column(|block| block.count)?,
            list(Arc::new(impacts), blocks.iter().map(|block| block.impacts.len()), impact_item)?,
        ], None).map_err(fault)?;
        Table::Blocks.array(vec![list(Arc::new(blocks_table), self.terms.iter().map(Vec::len), block_item)?])
    }

    pub fn ids(&self, minima: &[Option<i64>]) -> Result<StructArray> {
        let item = item_of(&Table::ImpactIds.fields()[0]);
        let arrow_schema::DataType::Struct(fields) = item.data_type() else { unreachable!() };
        let impacts = || self.terms.iter().flatten().flat_map(|block| block.impacts.iter());
        let array = StructArray::try_new(fields.clone(), vec![
            Arc::new(UInt32Array::from_iter_values(impacts().map(|impact| impact.window))),
            Arc::new(Int64Array::from_iter(impacts().map(|impact| minima[impact.window as usize]))),
            Arc::new(Int64Array::from_iter(impacts().map(|impact| impact.max_unit_id))),
        ], None).map_err(fault)?;
        Table::ImpactIds.array(vec![list(Arc::new(array), self.terms.iter()
            .map(|blocks| blocks.iter().map(|block| block.impacts.len()).sum()), item)?])
    }

    pub fn write(self, writer: &mut layout::Writer<impl std::io::Write>) -> Result<Vec<layout::PageRef>> {
        self.emit(|table, data| writer.page([(table, data)]))
    }

    pub fn emit<T>(self, mut page: impl FnMut(Table, StructArray) -> Result<T>) -> Result<Vec<T>> {
        let mut pages = Vec::new();
        for group in self.groups {
            let counts: Vec<_> = group.iter().map(PostingBuilder::len).collect();
            let mut documents = Vec::new();
            let mut position_counts = Vec::new();
            let mut positions = Vec::new();
            for term in group {
                documents.extend(term.documents);
                position_counts.extend(term.position_counts);
                positions.extend(term.positions);
            }
            let columns = Table::Frequencies.fields();
            let frequencies = Table::Frequencies.array(vec![
                list(Arc::new(UInt32Array::from(documents)), counts.iter().copied(), item_of(&columns[0]))?,
                list(Arc::new(UInt32Array::from_iter_values(position_counts.iter().map(|&count| count as u32))),
                    counts.iter().copied(), item_of(&columns[1]))?,
            ])?;
            let outer = item_of(&Table::Positions.fields()[0]);
            let positions = list(Arc::new(UInt32Array::from(positions)), position_counts, item_of(&outer))?;
            let positions = Table::Positions.array(vec![list(positions, counts, outer)?])?;
            pages.push(page(Table::Frequencies, frequencies)?);
            pages.push(page(Table::Positions, positions)?);
        }
        Ok(pages)
    }
}

pub(super) fn blocks(directory: &LargeListArray, term: usize) -> Vec<Block> {
    let rows = directory.value(term);
    let rows = rows.as_struct();
    let column = |index| rows.column(index).as_primitive::<UInt32Type>();
    let impacts = rows.column(3).as_list::<i64>();
    (0..rows.len()).map(|row| {
        let impact = impacts.value(row);
        let impact = impact.as_struct();
        let impact_column = |index| impact.column(index).as_primitive::<UInt32Type>();
        Block {
            page: column(0).value(row) as usize, slot: column(1).value(row) as usize, count: column(2).value(row) as usize,
            impacts: (0..impact.len()).map(|row| Impact {
                window: impact_column(0).value(row), frequency: impact_column(1).value(row),
                length: impact_column(2).value(row), same_frequency: impact.column(3).as_boolean().value(row),
                same_length: impact.column(4).as_boolean().value(row),
                max_unit_score: impact.column(5).as_primitive::<Float64Type>().value(row),
                max_unit_id: None,
            }).collect(),
        }
    }).collect()
}

pub(super) struct Frequencies {
    docs: UInt32Array,
    counts: UInt32Array,
}

impl Frequencies {
    pub fn read(reader: &layout::Reader, block: &Block, runs: u32) -> Result<Self> {
        Self::from_batch(&reader.page(block.page)?, block, runs)
    }

    pub fn from_batch(batch: &arrow_array::RecordBatch, block: &Block, runs: u32) -> Result<Self> {
        Self::checked_batch(batch, block, runs, false)
    }

    pub fn checked_batch(batch: &arrow_array::RecordBatch, block: &Block, runs: u32, prior_check: bool) -> Result<Self> {
        let table = layout::table(batch, Table::Frequencies);
        if block.slot >= table.len() { return Err(corrupt("Search block has no frequency slot")); }
        let column = |index: usize| table.column(index).as_list::<i64>().value(block.slot).as_primitive::<UInt32Type>().clone();
        let docs = column(0);
        let counts = column(1);
        if !prior_check && (docs.len() != block.count || counts.len() != block.count
            || docs.null_count() != 0 || counts.null_count() != 0
            || docs.values().windows(2).any(|pair| pair[0] >= pair[1]) || counts.values().contains(&0)
            || docs.values().last().is_some_and(|&doc| doc >= runs)) {
            return Err(corrupt("Search frequencies disagree with their directory"));
        }
        if !prior_check && !docs.values().chunk_by(|left, right| window_of(*left) == window_of(*right))
            .map(|docs| window_of(docs[0])).eq(block.impacts.iter().map(|impact| impact.window)) {
            return Err(corrupt("Search documents disagree with their scoring windows"));
        }
        Ok(Self { docs, counts })
    }

    pub fn window(&self, start: u32, end: u32) -> Range<usize> {
        self.docs.values().partition_point(|&doc| doc < start)..self.docs.values().partition_point(|&doc| doc < end)
    }

    pub fn document(&self, row: usize) -> u32 { self.docs.value(row) }

    pub fn index(&self, doc: u32) -> Option<usize> { self.docs.values().binary_search(&doc).ok() }

    pub fn frequency(&self, doc: u32) -> u32 { self.index(doc).map_or(0, |row| self.counts.value(row)) }

    pub fn positions(&self, reader: &layout::Reader, block: &Block) -> Result<LargeListArray> {
        self.positions_from_batch(&reader.page(block.page + 1)?, block)
    }

    pub fn positions_from_batch(&self, batch: &arrow_array::RecordBatch, block: &Block) -> Result<LargeListArray> {
        self.checked_positions(batch, block, false)
    }

    pub fn checked_positions(&self, batch: &arrow_array::RecordBatch, block: &Block, prior_check: bool) -> Result<LargeListArray> {
        let table = layout::table(batch, Table::Positions);
        if block.slot >= table.len() { return Err(corrupt("Search block has no position slot")); }
        let outer = table.column(0).as_list::<i64>();
        let positions = outer.value(block.slot);
        let positions = positions.as_list::<i64>();
        if !prior_check && (positions.len() != self.docs.len() || positions.null_count() != 0
            || !positions.value_offsets().windows(2).zip(self.counts.values()).all(|(range, &count)| range[1] - range[0] == i64::from(count))) {
            return Err(corrupt("Search positions disagree with their frequencies"));
        }
        Ok(positions.clone())
    }
}

pub(super) fn position_values(array: &LargeListArray, row: usize) -> &[u32] {
    let offsets = array.value_offsets();
    let values = array.values().as_primitive::<UInt32Type>().values();
    &values[offsets[row] as usize..offsets[row + 1] as usize]
}

pub(super) fn postings(reader: &layout::Reader, blocks: &[Block], runs: u32, with_positions: bool) -> Result<Postings> {
    let positions_item = item_of(&item_of(&Table::Positions.fields()[0]));
    let mut docs = Vec::new();
    let mut counts = Vec::new();
    let mut positions = Vec::new();
    for block in blocks {
        let frequencies = Frequencies::read(reader, block, runs)?;
        docs.extend_from_slice(frequencies.docs.values());
        counts.extend_from_slice(frequencies.counts.values());
        if with_positions {
            let page = frequencies.positions(reader, block)?;
            positions.extend_from_slice(page.values().as_primitive::<UInt32Type>().values()
                .get(page.value_offsets()[0] as usize..page.value_offsets()[page.len()] as usize)
                .expect("Arrow list offsets are validated"));
        }
    }
    let values = if with_positions {
        let positions = list(Arc::new(UInt32Array::from(positions)), counts.into_iter().map(|count| count as usize), positions_item)?;
        PostingValues::Positions(positions.as_list::<i64>().clone())
    } else { PostingValues::Frequencies(UInt32Array::from(counts)) };
    Ok(Postings::from_pages(UInt32Array::from(docs), values))
}

pub(super) struct TermBlocks {
    pub blocks: Vec<Block>,
    pub windows: Vec<(u32, usize, Impact)>,
}

impl TermBlocks {
    pub fn new(blocks: Vec<Block>) -> Self {
        let windows = blocks.iter().enumerate().flat_map(|(index, block)| {
            block.impacts.iter().map(move |&impact| (impact.window, index, impact))
        }).collect();
        Self { blocks, windows }
    }

    pub fn window(&self, window: u32) -> Option<(usize, Impact)> {
        let index = self.windows.binary_search_by_key(&window, |entry| entry.0).ok()?;
        Some((self.windows[index].1, self.windows[index].2))
    }
}

pub(crate) struct PagedRanked {
    file: Arc<SearchFile>,
    scorer: Arc<Scorer>,
    terms: Vec<TermBlocks>,
    required: Vec<usize>,
    scored: Vec<usize>,
    phrases: Vec<Vec<usize>>,
    bounds: TemporalBounds,
    windows: Vec<(RankKey, u32)>,
    next: usize,
}

impl PagedRanked {
    pub(super) fn new(file: Arc<SearchFile>, column: usize, matching: &Query, scoring: &RankScorer,
        bounds: TemporalBounds, ascending_id: bool) -> Self {
        let term_ids: BTreeMap<_, _> = matching.terms().iter().chain(scoring.scorer().query().terms())
            .collect::<BTreeSet<_>>().into_iter().enumerate().map(|(index, term)| (term.as_str(), index)).collect();
        let terms: Vec<_> = term_ids.keys().map(|term| TermBlocks::new(file.manifest.term(column, term)
            .and_then(|term| file.manifest.blocks(&term)).unwrap_or_default())).collect();
        let required: Vec<_> = matching.terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let scored: Vec<_> = scoring.scorer().query().terms().iter().map(|term| term_ids[term.as_str()]).collect();
        let phrases = matching.phrases().iter().map(|phrase| phrase.iter().map(|term| term_ids[term.as_str()]).collect()).collect();
        let unit_scores = scoring.scorer().uses_unit_term_scores();
        let mut windows = Vec::new();
        if let Some(&lead) = required.iter().min_by_key(|&&term| terms[term].windows.len()) {
            for &(window, _, _) in &terms[lead].windows {
                if required.iter().any(|&term| terms[term].window(window).is_none()) { continue; }
                let score = scored.iter().enumerate().filter_map(|(index, &term)| {
                    let (_, impact) = terms[term].window(window)?;
                    if unit_scores { return Some(impact.max_unit_score); }
                    let exact = impact.same_frequency && (impact.same_length || !scoring.scorer().uses_length_normalization());
                    Some(if exact { scoring.scorer().term_score(index, impact.frequency, impact.length) }
                        else { scoring.term_bound(index, impact.frequency, impact.length) })
                }).fold(0.0, |score, bound| score + bound);
                let id = ascending_id.then(|| file.manifest.window_id(window)).flatten();
                windows.push((RankKey::integer(score, id), window));
            }
        }
        windows.sort_unstable_by(|left, right| right.0.compare(left.0).then_with(|| left.1.cmp(&right.1)));
        Self { file, scorer: Arc::clone(scoring.scorer()), terms, required, scored, phrases, bounds, windows, next: 0 }
    }

    pub(crate) fn bound(&self) -> Option<RankKey> { self.windows.get(self.next).map(|&(key, _)| key) }

    fn block_key(&self, term: usize, window: u32) -> Option<(usize, usize)> {
        self.terms[term].window(window).map(|(index, _)| (term, index))
    }

    fn block(&self, key: (usize, usize)) -> &Block { &self.terms[key.0].blocks[key.1] }

    pub(super) async fn expand(&mut self, scope: Option<&BTreeSet<Iid>>, examined: &mut usize) -> Result<ScoredRuns> {
        let ranker = &*self;
        let end = self.next.saturating_add(4).min(self.windows.len());
        let windows = &self.windows[self.next..end];
        let keys: BTreeSet<_> = windows.iter().flat_map(|&(_, window)| ranker.required.iter().chain(&ranker.scored)
            .filter_map(move |&term| ranker.block_key(term, window))).collect();
        let reader = &self.file.reader;
        reader.prefetch(keys.iter().map(|&key| self.block(key).page)).await?;
        let frequency = keys.into_iter().map(|key| {
            Ok((key, Frequencies::read(reader, self.block(key), self.file.manifest.documents)?))
        }).collect::<Result<BTreeMap<_, _>>>()?;
        let frequency_for = |term, window| self.block_key(term, window).map(|key| &frequency[&key]);
        let mut candidates = Vec::new();
        for &(_, window) in windows {
            let (start, end) = document_range(window);
            let ranges: Vec<_> = self.required.iter().map(|&term| {
                let posting = frequency_for(term, window).expect("all required terms have a block");
                (posting, posting.window(start, end))
            }).collect();
            let Some((lead, rows)) = ranges.iter().min_by_key(|(_, range)| range.len()) else { continue };
            for row in rows.clone() {
                *examined += 1;
                let doc = lead.document(row);
                if ranges.iter().all(|(posting, _)| posting.index(doc).is_some())
                    && scope.is_none_or(|scope| scope.contains(&Iid(self.file.manifest.doc_iid(doc))))
                    && self.file.visible(doc, self.bounds)
                {
                    candidates.push((window, doc));
                }
            }
        }

        let phrase_terms: BTreeSet<_> = self.phrases.iter().flatten().copied().collect();
        let position_keys: BTreeSet<_> = candidates.iter().flat_map(|&(window, _)| {
            phrase_terms.iter().filter_map(move |&term| ranker.block_key(term, window))
        }).collect();
        reader.prefetch(position_keys.iter().map(|&key| self.block(key).page + 1)).await?;
        let positions = position_keys.into_iter().map(|key| {
            Ok((key, frequency[&key].positions(reader, self.block(key))?))
        }).collect::<Result<BTreeMap<_, _>>>()?;
        let mut docs = Vec::with_capacity(candidates.len());
        let mut scores = Vec::with_capacity(candidates.len());
        let mut frequencies = Vec::with_capacity(self.scored.len());
        for (window, doc) in candidates {
            let occurrences = |term| {
                let key = self.block_key(term, window).expect("all phrase terms have a block");
                let row = frequency[&key].index(doc).expect("candidate contains all phrase terms");
                position_values(&positions[&key], row)
            };
            if !self.phrases.iter().all(|phrase| occurrences(phrase[0]).iter().any(|&start| {
                phrase.iter().skip(1).enumerate().all(|(offset, &term)| {
                    start.checked_add(offset as u32 + 1).is_some_and(|position| occurrences(term).binary_search(&position).is_ok())
                })
            })) { continue; }
            frequencies.clear();
            frequencies.extend(self.scored.iter().map(|&term| frequency_for(term, window).map_or(0, |posting| posting.frequency(doc))));
            let score = self.scorer.score(&frequencies, stored_length(self.file.manifest.token_length(doc) as usize));
            docs.push(doc);
            scores.push(score);
        }
        self.next = end;
        Ok(ScoredRuns { file: Arc::clone(&self.file), documents: UInt32Array::from(docs), scores: Float64Array::from(scores) })
    }
}
