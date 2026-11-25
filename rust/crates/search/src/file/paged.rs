use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, LargeListArray, StructArray, UInt32Array, builder::BooleanBuilder, cast::AsArray, types::{Float64Type, UInt32Type}};
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

#[derive(Clone, Copy)]
pub(super) struct PostingBlock {
    pub page: usize,
    pub slot: usize,
    pub count: usize,
}

impl PostingBlock {
    pub fn at(rows: &StructArray, row: usize) -> Self {
        let value = |index| rows.column(index).as_primitive::<UInt32Type>().value(row) as usize;
        Self { page: value(0), slot: value(1), count: value(2) }
    }
}

impl Block {
    fn posting(&self) -> PostingBlock {
        PostingBlock { page: self.page, slot: self.slot, count: self.count }
    }
}

pub(super) struct PostingPlan {
    terms: Vec<PlannedTerm>,
}

struct PlannedTerm {
    postings: PostingBuilder,
    blocks: Vec<PlannedBlock>,
}

struct PlannedBlock {
    page: usize,
    slot: usize,
    documents: Range<usize>,
    positions: Range<usize>,
    impacts: Range<usize>,
}

impl PostingPlan {
    pub fn new(terms: impl IntoIterator<Item = PostingBuilder>) -> Self {
        let mut page = FIRST_POSTING_PAGE;
        let mut slot = 0;
        let terms = terms.into_iter().map(|postings| {
            let blocks = if postings.len() <= TARGET_POSTINGS {
                let block = PlannedBlock {
                    page, slot, documents: 0..postings.len(),
                    positions: 0..postings.positions.len(), impacts: 0..postings.windows.len(),
                };
                slot += 1;
                if slot == POSTING_TERMS {
                    page += 2;
                    slot = 0;
                }
                vec![block]
            } else {
                if slot != 0 {
                    page += 2;
                    slot = 0;
                }
                let mut blocks = Vec::new();
                let mut first = 0;
                let mut first_position = 0;
                let mut first_impact = 0;
                while first < postings.len() {
                    let candidate = (first + TARGET_POSTINGS).min(postings.len()) - 1;
                    let last_window = window_of(postings.documents[candidate]);
                    let end = candidate + 1 + postings.documents[candidate + 1..]
                        .partition_point(|&doc| window_of(doc) == last_window);
                    let end_position = first_position + postings.position_counts[first..end].iter().sum::<usize>();
                    let end_impact = first_impact + postings.windows[first_impact..]
                        .partition_point(|impact| impact.window <= last_window);
                    blocks.push(PlannedBlock {
                        page, slot: 0, documents: first..end, positions: first_position..end_position,
                        impacts: first_impact..end_impact,
                    });
                    page += 2;
                    first = end;
                    first_position = end_position;
                    first_impact = end_impact;
                }
                blocks
            };
            PlannedTerm { postings, blocks }
        }).collect();
        Self { terms }
    }

    pub fn directory(&self) -> Result<StructArray> {
        let block_item = item_of(&Table::Blocks.fields()[0]);
        let arrow_schema::DataType::Struct(block_fields) = block_item.data_type() else { unreachable!() };
        let impact_item = item_of(&block_fields[3]);
        let arrow_schema::DataType::Struct(impact_fields) = impact_item.data_type() else { unreachable!() };
        let block_count = self.terms.iter().map(|term| term.blocks.len()).sum();
        let impact_count = self.terms.iter().map(|term| term.postings.windows.len()).sum();
        let mut pages = Vec::with_capacity(block_count);
        let mut slots = Vec::with_capacity(block_count);
        let mut counts = Vec::with_capacity(block_count);
        let mut windows = Vec::with_capacity(impact_count);
        let mut frequencies = Vec::with_capacity(impact_count);
        let mut lengths = Vec::with_capacity(impact_count);
        let mut same_frequency = BooleanBuilder::with_capacity(impact_count);
        let mut same_length = BooleanBuilder::with_capacity(impact_count);
        let mut scores = Vec::with_capacity(impact_count);
        for term in &self.terms {
            for block in &term.blocks {
                pages.push(u32::try_from(block.page).map_err(fault)?);
                slots.push(u32::try_from(block.slot).map_err(fault)?);
                counts.push(u32::try_from(block.documents.len()).map_err(fault)?);
                for impact in &term.postings.windows[block.impacts.clone()] {
                    windows.push(impact.window);
                    frequencies.push(impact.frequency);
                    lengths.push(impact.length);
                    same_frequency.append_value(impact.same_frequency);
                    same_length.append_value(impact.same_length);
                    scores.push(impact.max_unit_score);
                }
            }
        }
        let impacts = StructArray::try_new(impact_fields.clone(), vec![
            Arc::new(UInt32Array::from(windows)), Arc::new(UInt32Array::from(frequencies)),
            Arc::new(UInt32Array::from(lengths)), Arc::new(same_frequency.finish()),
            Arc::new(same_length.finish()), Arc::new(Float64Array::from(scores)),
        ], None).map_err(fault)?;
        let blocks_table = StructArray::try_new(block_fields.clone(), vec![
            Arc::new(UInt32Array::from(pages)), Arc::new(UInt32Array::from(slots)), Arc::new(UInt32Array::from(counts)),
            list(Arc::new(impacts), self.terms.iter().flat_map(|term| &term.blocks).map(|block| block.impacts.len()), impact_item)?,
        ], None).map_err(fault)?;
        Table::Blocks.array(vec![list(Arc::new(blocks_table), self.terms.iter().map(|term| term.blocks.len()), block_item)?])
    }

    pub fn ids(&self, minima: &[Option<i64>]) -> Result<StructArray> {
        let item = item_of(&Table::ImpactIds.fields()[0]);
        let arrow_schema::DataType::Struct(fields) = item.data_type() else { unreachable!() };
        let impacts = || self.terms.iter().flat_map(|term| &term.postings.windows);
        let array = StructArray::try_new(fields.clone(), vec![
            Arc::new(UInt32Array::from_iter_values(impacts().map(|impact| impact.window))),
            Arc::new(Int64Array::from_iter(impacts().map(|impact| minima[impact.window as usize]))),
            Arc::new(Int64Array::from_iter(impacts().map(|impact| impact.max_unit_id))),
        ], None).map_err(fault)?;
        Table::ImpactIds.array(vec![list(Arc::new(array), self.terms.iter().map(|term| term.postings.windows.len()), item)?])
    }

    pub fn write(self, writer: &mut layout::Writer<impl std::io::Write>) -> Result<Vec<layout::PageRef>> {
        self.emit(false, |table, data| writer.page([(table, data)]))
    }

    pub fn emit<T>(self, retain_pages: bool, mut page: impl FnMut(Table, StructArray) -> Result<T>) -> Result<Vec<T>> {
        let mut pages = Vec::new();
        let mut emit = |tables: [(Table, StructArray); 2]| -> Result<()> {
            for (table, data) in tables {
                pages.push(page(table, data)?);
            }
            Ok(())
        };
        let mut terms = self.terms.into_iter().peekable();
        while let Some(term) = terms.next() {
            if term.postings.len() <= TARGET_POSTINGS {
                let first_page = term.blocks[0].page;
                let group = std::iter::once(term).chain(std::iter::from_fn(|| {
                    terms.next_if(|term| term.blocks[0].page == first_page)
                }));
                let mut counts = Vec::new();
                let mut documents = Vec::new();
                let mut position_counts = Vec::new();
                let mut positions = Vec::new();
                for term in group {
                    counts.push(term.postings.len());
                    documents.extend(term.postings.documents);
                    position_counts.extend(term.postings.position_counts);
                    positions.extend(term.postings.positions);
                }
                emit(posting_pages(UInt32Array::from(documents), &position_counts, UInt32Array::from(positions), &counts)?)?;
            } else {
                let documents = UInt32Array::from(term.postings.documents);
                let positions = UInt32Array::from(term.postings.positions);
                let values = |array: &UInt32Array, range: Range<usize>| {
                    if retain_pages {
                        // Query builds charge each page for the full capacity of its backing buffers.
                        let mut values = Vec::new();
                        values.extend_from_slice(&array.values()[range]);
                        UInt32Array::from(values)
                    } else {
                        array.slice(range.start, range.len())
                    }
                };
                for block in term.blocks {
                    let count = block.documents.len();
                    let position_counts = &term.postings.position_counts[block.documents.clone()];
                    emit(posting_pages(values(&documents, block.documents), position_counts,
                        values(&positions, block.positions), &[count])?)?;
                }
            }
        }
        Ok(pages)
    }
}

fn posting_pages(documents: UInt32Array, position_counts: &[usize], positions: UInt32Array, counts: &[usize])
    -> Result<[(Table, StructArray); 2]> {
    let columns = Table::Frequencies.fields();
    let frequencies = Table::Frequencies.array(vec![
        list(Arc::new(documents), counts.iter().copied(), item_of(&columns[0]))?,
        list(Arc::new(UInt32Array::from_iter_values(position_counts.iter().map(|&count| count as u32))),
            counts.iter().copied(), item_of(&columns[1]))?,
    ])?;
    let outer = item_of(&Table::Positions.fields()[0]);
    let positions = list(Arc::new(positions), position_counts.iter().copied(), item_of(&outer))?;
    let positions = Table::Positions.array(vec![list(positions, counts.iter().copied(), outer)?])?;
    Ok([(Table::Frequencies, frequencies), (Table::Positions, positions)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::FORMAT_VERSION;

    #[test]
    fn split_posting_pages_keep_windows_positions_and_ipc_bytes() -> Result<()> {
        let positions = |doc| (0..doc % 3 + 1).map(|index| index * 3 + doc % 5).collect::<Vec<u32>>();
        let doc_start = 127u32;
        let doc_end = doc_start + 2 * TARGET_POSTINGS as u32 + 2;
        let plan = || {
            let small = || {
                let mut term = PostingBuilder::default();
                term.push(1, 2, vec![0], Some(50));
                term
            };
            let mut large = PostingBuilder::default();
            for doc in doc_start..doc_end {
                large.push(doc, 10, positions(doc), Some(i64::from(doc)));
            }
            PostingPlan::new((0..=POSTING_TERMS).map(|_| small()).chain([large, small(), small()]))
        };
        let durable = plan();
        let directory = durable.directory()?;
        let directory = directory.column(0).as_list::<i64>();
        let large = POSTING_TERMS + 1;
        let coordinates = |term| blocks(directory, term).iter().map(|block| (block.page, block.slot, block.count))
            .collect::<Vec<_>>();
        assert_eq!(coordinates(POSTING_TERMS - 1), [(2, POSTING_TERMS - 1, 1)]);
        assert_eq!(coordinates(POSTING_TERMS), [(4, 0, 1)]);
        assert_eq!(coordinates(large), [(6, 0, TARGET_POSTINGS + 1), (8, 0, TARGET_POSTINGS), (10, 0, 1)]);
        assert_eq!(coordinates(large + 2), [(12, 1, 1)]);

        let retained = plan().emit(true, |table, data| layout::record(FORMAT_VERSION, [(table, data)]))?;
        let mut next_doc = doc_start;
        for block in blocks(directory, large) {
            let page = block.page - FIRST_POSTING_PAGE;
            let frequencies = Frequencies::from_batch(&retained[page], &block, doc_end)?;
            let stored = frequencies.positions_from_batch(&retained[page + 1], &block)?;
            for row in 0..block.count {
                assert_eq!(frequencies.document(row), next_doc);
                assert_eq!(position_values(&stored, row), positions(next_doc).as_slice());
                next_doc += 1;
            }
        }
        assert_eq!(next_doc, doc_end);

        let mut actual = Vec::new();
        let mut writer = layout::Writer::new(&mut actual, FORMAT_VERSION)?;
        durable.write(&mut writer)?;
        writer.finish()?;

        let mut expected = Vec::new();
        let mut writer = layout::Writer::new(&mut expected, FORMAT_VERSION)?;
        for batch in retained { writer.write_batch(&batch)?; }
        writer.finish()?;
        assert_eq!(actual, expected);
        Ok(())
    }
}

pub(super) fn blocks(directory: &LargeListArray, term: usize) -> Vec<Block> {
    let rows = directory.value(term);
    let rows = rows.as_struct();
    let impacts = rows.column(3).as_list::<i64>();
    (0..rows.len()).map(|row| {
        let PostingBlock { page, slot, count } = PostingBlock::at(rows, row);
        let impact = impacts.value(row);
        let impact = impact.as_struct();
        let impact_column = |index| impact.column(index).as_primitive::<UInt32Type>();
        Block {
            page, slot, count,
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
        Self::checked_batch(batch, block.posting(), block.impacts.iter().map(|impact| impact.window), runs, false)
    }

    pub fn checked_batch(batch: &arrow_array::RecordBatch, block: PostingBlock, windows: impl Iterator<Item = u32>,
        runs: u32, prior_check: bool) -> Result<Self> {
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
            .map(|docs| window_of(docs[0])).eq(windows) {
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
        self.checked_positions(batch, block.slot, false)
    }

    pub fn checked_positions(&self, batch: &arrow_array::RecordBatch, slot: usize, prior_check: bool) -> Result<LargeListArray> {
        let table = layout::table(batch, Table::Positions);
        if slot >= table.len() { return Err(corrupt("Search block has no position slot")); }
        let outer = table.column(0).as_list::<i64>();
        let positions = outer.value(slot);
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

    pub(super) async fn expand(&mut self, scope: Option<rtdb_query::IidScope<'_>>, examined: &mut usize) -> Result<ScoredRuns> {
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
