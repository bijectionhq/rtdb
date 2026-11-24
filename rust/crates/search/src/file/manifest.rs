use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, LargeListArray, RecordBatch, StructArray, UInt32Array, cast::AsArray, types::{Int32Type, Int64Type, UInt8Type, UInt32Type, UInt64Type}};
use rtdb_types::Result;

use super::{
    FORMAT_VERSION, LEGACY_FORMAT_VERSION, NULL_DOC, NO_DOC, DocMatch, KeyedEvent, RowDoc, Section, artefact_digest,
    corrupt, op_of, sections_of, layout::{Reader, Table, table}, paged::{self, Block, FIRST_POSTING_PAGE}, postings::POSTING_TERMS,
};
use crate::member_bounds::{MemberStart, document_member};
use crate::rank_windows::{document_range, window_of};
use super::tree_file::DOCUMENT_ROWS;

pub(super) struct Term {
    pub ordinal: usize,
    pub frequency: u32,
    pub legacy_location: Option<(usize, usize)>,
}

pub(super) struct ScopedTerm {
    pub name: String,
    pub frequency: u32,
    pub blocks: Vec<Block>,
}

enum Terms {
    Flat { terms: StructArray, blocks: Option<LargeListArray> },
    Scoped { column: usize, terms: Vec<ScopedTerm> },
}

struct EventPage {
    start: u32,
    events: StructArray,
    row_runs: LargeListArray,
}

impl EventPage {
    fn read(batch: &RecordBatch, start: u32, documents: u32, columns: usize) -> Result<Self> {
        let events = table(batch, Table::Events);
        let row_runs = table(batch, Table::RowRuns).column(0).as_list::<i64>().clone();
        if row_runs.len() != columns || row_runs.value_offsets().windows(2).any(|pair| pair[1] - pair[0] != events.len() as i64)
            || row_runs.values().as_primitive::<UInt32Type>().values().iter().any(|&doc| doc >= documents && doc != NO_DOC && doc != NULL_DOC)
        {
            return Err(corrupt("Search file row-to-run map is inconsistent"));
        }
        Ok(Self { start, events, row_runs })
    }
}

struct RunPage {
    start: u32,
    runs: StructArray,
    windows: Option<Int64Array>,
}

impl RunPage {
    fn read(batch: &RecordBatch, start: u32, runs: StructArray, paged: bool) -> Result<Self> {
        let windows = paged.then(|| table(batch, Table::Windows).column(0).as_primitive::<Int64Type>().clone());
        if windows.as_ref().is_some_and(|windows| windows.len() != window_count(runs.len() as u32)) {
            return Err(corrupt("Search scoring windows disagree with their runs"));
        }
        Ok(Self { start, runs, windows })
    }
}

pub(super) struct Manifest {
    pub rows: u32,
    pub documents: u32,
    pub sections: Vec<Section>,
    pub members: Vec<MemberStart>,
    pub pages: UInt32Array,
    header: StructArray,
    columns: StructArray,
    events: Vec<EventPage>,
    runs: Vec<RunPage>,
    terms: Terms,
}

impl Manifest {
    pub(super) fn read(reader: &Reader, expected: &[String]) -> Result<Self> {
        let batch = reader.page(0)?;
        let paged = reader.format >= FORMAT_VERSION;
        let header = table(&batch, Table::Header);
        if header.len() != 1 || header.column(0).as_primitive::<Int32Type>().value(0) != reader.format
            || header.column(1).as_primitive::<Int32Type>().value(0) != rtdb_text::ANALYZER_VERSION
        {
            return Err(corrupt("Search file format or analyzer does not match the reader"));
        }
        let column_names = table(&batch, Table::Columns);
        if !column_names.column(0).as_string::<i32>().iter().eq(expected.iter().map(|name| Some(name.as_str()))) {
            return Err(corrupt("Search file columns do not match its definition"));
        }
        let visibility = if paged { reader.page(1)? } else { batch.clone() };
        let rows = u32::try_from(table(&visibility, Table::Events).len()).map_err(|_| corrupt("Search file has too many events"))?;
        let runs = table(&visibility, Table::Runs);
        let documents = u32::try_from(runs.len()).map_err(|_| corrupt("Search file has too many runs"))?;
        let events = EventPage::read(&visibility, 0, documents, expected.len())?;
        let runs = RunPage::read(&batch, 0, runs, paged)?;
        let terms = table(&batch, Table::Terms);
        validate_terms(&terms, reader.len(), expected.len(), documents, reader.format)?;
        let blocks = if paged {
            let directory = table(&batch, Table::Blocks).column(0).as_list::<i64>().clone();
            if directory.len() != terms.len() || directory.null_count() != 0 {
                return Err(corrupt("Search block directory disagrees with its terms and rows"));
            }
            validate_blocks(&directory, &terms, reader.len(), window_count(documents))?;
            Some(directory)
        } else { None };
        Self::new(&batch, &batch, rows, documents, vec![events], vec![runs], Terms::Flat { terms, blocks })
    }

    pub(super) fn read_tree(root: &RecordBatch, primary: &RecordBatch, documents: u32, column: usize,
        terms: Vec<ScopedTerm>, events: Vec<RecordBatch>, runs: BTreeMap<usize, RecordBatch>) -> Result<Self> {
        let rows = table(root, Table::Pages).column(0).as_primitive::<UInt32Type>().value(0);
        let columns = table(root, Table::Columns).len();
        let mut covered = 0u32;
        let mut event_pages = Vec::with_capacity(events.len());
        for batch in events {
            let start = page_start(&batch)?;
            let page = EventPage::read(&batch, start, documents, columns)?;
            if start != covered || page.events.is_empty() {
                return Err(corrupt("Search tree event pages do not cover its rows in order"));
            }
            covered = covered.checked_add(u32::try_from(page.events.len()).map_err(|_| corrupt("Search event page is too large"))?)
                .ok_or_else(|| corrupt("Search event page exceeds its row count"))?;
            event_pages.push(page);
        }
        if covered != rows { return Err(corrupt("Search tree event pages disagree with its root count")); }
        let runs = runs.into_iter().map(|(expected, batch)| {
            let start = page_start(&batch)?;
            let runs = table(&batch, Table::Runs);
            if start as usize != expected || start >= documents || expected % DOCUMENT_ROWS != 0
                || runs.len() != DOCUMENT_ROWS.min((documents - start) as usize) {
                return Err(corrupt("Search tree document page disagrees with its directory"));
            }
            RunPage::read(&batch, start, runs, true)
        }).collect::<Result<_>>()?;
        Self::new(root, primary, rows, documents, event_pages, runs, Terms::Scoped { column, terms })
    }

    fn new(root: &RecordBatch, primary: &RecordBatch, rows: u32, documents: u32, events: Vec<EventPage>, runs: Vec<RunPage>,
        terms: Terms) -> Result<Self> {
        let columns = table(root, Table::Columns);
        let run_counts = columns.column(1).as_primitive::<UInt32Type>();
        if documents >= NULL_DOC || run_counts.values().iter().map(|&count| u64::from(count)).sum::<u64>() != u64::from(documents) {
            return Err(corrupt("Search file column counts disagree with its runs"));
        }
        let pages = table(primary, Table::Pages).column(0).as_primitive::<UInt32Type>().clone();
        if !ordered_starts(pages.values(), rows) {
            return Err(corrupt("Search file page starts are out of order"));
        }
        let member_table = table(root, Table::Members);
        let member_pages = member_table.column(0).as_primitive::<UInt32Type>();
        let member_runs = member_table.column(1).as_primitive::<UInt32Type>();
        if member_table.is_empty() || !ordered_starts(member_pages.values(), pages.len() as u32)
            || !ordered_starts(member_runs.values(), documents)
        {
            return Err(corrupt("Search file member starts are out of order"));
        }
        let members = member_pages.values().iter().zip(member_runs.values().iter())
            .map(|(&page, &doc)| MemberStart { page, doc }).collect();
        let header = table(root, Table::Header);
        let sections = sections_of(&table(root, Table::Sections))?;
        Ok(Self { rows, documents, sections, members, pages, header, columns, events, runs, terms })
    }

    fn event_at(&self, row: u32) -> (&EventPage, usize) {
        let page = &self.events[self.events.partition_point(|page| page.start <= row) - 1];
        (page, (row - page.start) as usize)
    }

    fn run_at(&self, doc: u32) -> (&RunPage, usize) {
        let page = &self.runs[self.runs.partition_point(|page| page.start <= doc) - 1];
        (page, (doc - page.start) as usize)
    }

    pub(super) fn matches_artefact(&self, identity: &str, version: i64) -> bool {
        let versions = self.header.column(2).as_primitive::<Int64Type>();
        let identities = self.header.column(3).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("search identity layout");
        versions.is_valid(0) && identities.is_valid(0) && versions.value(0) == version && identities.value(0) == artefact_digest(identity)
    }

    pub(super) fn row_doc(&self, column: usize, row: u32) -> RowDoc {
        if row >= self.rows || column >= self.columns.len() { return RowDoc::Invisible; }
        let (page, index) = self.event_at(row);
        let offset = page.row_runs.value_offsets()[column] as usize;
        match page.row_runs.values().as_primitive::<UInt32Type>().value(offset + index) {
            NO_DOC => RowDoc::Invisible,
            NULL_DOC => RowDoc::Null,
            doc => RowDoc::Run(doc),
        }
    }

    pub(super) fn doc_class(&self, doc: u32) -> u8 {
        let (page, row) = self.run_at(doc);
        page.runs.column(3).as_primitive::<UInt8Type>().value(row)
    }

    pub(super) fn doc_match(&self, doc: u32) -> DocMatch {
        let member = document_member(&self.members, doc);
        DocMatch { doc, iid: self.doc_iid(doc), member: member as u32 }
    }

    pub(super) fn doc_iid(&self, doc: u32) -> [u8; 16] {
        let (page, row) = self.run_at(doc);
        fixed_keys(&page.runs).value(row).try_into().expect("fixed width iid")
    }

    pub(super) fn token_length(&self, doc: u32) -> u32 {
        let (page, row) = self.run_at(doc);
        page.runs.column(1).as_primitive::<UInt32Type>().value(row)
    }

    pub(super) fn has_full_event_keys(&self) -> bool {
        self.events.iter().all(|page| page.events.column(5).null_count() == 0)
    }

    pub(super) fn event_key(&self, row: u32) -> Option<&[u8]> {
        let (page, row) = self.event_at(row);
        let keys = page.events.column(5).as_binary::<i32>();
        keys.is_valid(row).then(|| keys.value(row))
    }

    pub(super) fn run_key(&self, doc: u32) -> Result<&[u8]> {
        let (page, row) = self.run_at(doc);
        let keys = page.runs.column(2).as_binary::<i32>();
        if row >= keys.len() || keys.is_null(row) { return Err(corrupt("Search run omitted its full identity")); }
        Ok(keys.value(row))
    }

    pub(super) fn run_key_buffer(&self, doc: u32) -> Result<arrow_buffer::Buffer> {
        let missing = || corrupt("Search run omitted its full identity");
        if doc >= self.documents { return Err(missing()); }
        let (page, row) = self.run_at(doc);
        let keys = page.runs.column(2).as_binary::<i32>();
        if row >= keys.len() || keys.is_null(row) { return Err(missing()); }
        let offsets = keys.value_offsets();
        let start = offsets[row] as usize;
        Ok(keys.values().slice_with_length(start, offsets[row + 1] as usize - start))
    }

    pub(super) fn rectangles(&self, doc: u32) -> impl Iterator<Item = [i64; 4]> + '_ {
        let (page, row) = self.run_at(doc);
        let list = page.runs.column(4).as_list::<i64>();
        let offsets = list.value_offsets();
        let values = list.values().as_struct();
        let bounds: [_; 4] = std::array::from_fn(|index| values.column(index).as_primitive::<Int64Type>());
        (offsets[row] as usize..offsets[row + 1] as usize).map(move |row| bounds.map(|column| column.value(row)))
    }

    pub(super) fn rows_of(&self, iid: &[u8; 16]) -> Range<u32> {
        let iid_at = |row| {
            let (page, row) = self.event_at(row);
            fixed_keys(&page.events).value(row)
        };
        let mut low = 0;
        let mut high = self.rows;
        while low < high {
            let middle = low + (high - low) / 2;
            if iid_at(middle) < iid.as_slice() { low = middle + 1; } else { high = middle; }
        }
        let start = low;
        high = self.rows;
        while low < high {
            let middle = low + (high - low) / 2;
            if iid_at(middle) <= iid.as_slice() { low = middle + 1; } else { high = middle; }
        }
        start..low
    }

    pub(super) fn event(&self, row: u32) -> Result<KeyedEvent> {
        let (page, row) = self.event_at(row);
        let bound = |index| page.events.column(index).as_primitive::<Int64Type>().value(row);
        Ok(KeyedEvent {
            position: super::read_event_position(page.events.column(6).as_ref(), row),
            system_from: bound(1), valid_from: bound(2), valid_to: bound(3),
            op: op_of(page.events.column(4).as_primitive::<UInt8Type>().value(row))?,
        })
    }

    pub(super) fn term(&self, column: usize, term: &str) -> Option<Term> {
        let terms = match &self.terms {
            Terms::Flat { terms, .. } => terms,
            Terms::Scoped { column: indexed, terms } => {
                if column != *indexed { return None; }
                let ordinal = terms.binary_search_by(|entry| entry.name.as_str().cmp(term)).ok()?;
                return Some(self.term_at(ordinal));
            }
        };
        let columns = terms.column(0).as_primitive::<UInt32Type>();
        let names = terms.column(1).as_string::<i32>();
        let mut first = 0;
        let mut end = terms.len();
        while first < end {
            let middle = first + (end - first) / 2;
            match (columns.value(middle) as usize, names.value(middle)).cmp(&(column, term)) {
                std::cmp::Ordering::Less => first = middle + 1,
                std::cmp::Ordering::Greater => end = middle,
                std::cmp::Ordering::Equal => return Some(self.term_at(middle)),
            }
        }
        None
    }

    fn term_at(&self, ordinal: usize) -> Term {
        match &self.terms {
            Terms::Flat { terms, blocks } => {
                let value = |column| terms.column(column).as_primitive::<UInt32Type>().value(ordinal);
                Term { ordinal, frequency: value(4), legacy_location: blocks.is_none().then(|| (value(2) as usize, value(3) as usize)) }
            }
            Terms::Scoped { terms, .. } => Term { ordinal, frequency: terms[ordinal].frequency, legacy_location: None },
        }
    }

    pub(super) fn terms(&self) -> impl ExactSizeIterator<Item = Term> + '_ {
        let len = match &self.terms {
            Terms::Flat { terms, .. } => terms.len(),
            Terms::Scoped { terms, .. } => terms.len(),
        };
        (0..len).map(|ordinal| self.term_at(ordinal))
    }

    pub(super) fn blocks(&self, term: &Term) -> Option<Vec<Block>> {
        match &self.terms {
            Terms::Flat { blocks, .. } => blocks.as_ref().map(|directory| paged::blocks(directory, term.ordinal)),
            Terms::Scoped { terms, .. } => Some(terms[term.ordinal].blocks.clone()),
        }
    }

    pub(super) fn window_id(&self, window: u32) -> Option<i64> {
        let (page, _) = self.run_at(document_range(window).0);
        let ids = page.windows.as_ref()?;
        let row = (window - window_of(page.start)) as usize;
        (row < ids.len() && ids.is_valid(row)).then(|| ids.value(row))
    }

    pub(super) fn column_totals(&self, index: usize) -> (u64, u64) {
        (
            u64::from(self.columns.column(1).as_primitive::<UInt32Type>().value(index)),
            self.columns.column(2).as_primitive::<UInt64Type>().value(index),
        )
    }
}

fn fixed_keys(events_or_runs: &StructArray) -> &FixedSizeBinaryArray {
    events_or_runs.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("search IID layout")
}

fn page_start(batch: &RecordBatch) -> Result<u32> {
    let pages = table(batch, Table::Pages);
    let starts = pages.column(0).as_primitive::<UInt32Type>();
    if starts.len() != 1 || starts.is_null(0) { return Err(corrupt("Search page has no start ordinal")); }
    Ok(starts.value(0))
}

pub(super) fn window_count(documents: u32) -> usize {
    documents.checked_sub(1).map_or(0, |last| window_of(last) as usize + 1)
}

fn ordered_starts(starts: &[u32], end: u32) -> bool {
    starts.first().is_none_or(|&first| first == 0) && starts.last().is_none_or(|&last| last <= end)
        && !starts.windows(2).any(|pair| pair[0] > pair[1])
}

fn validate_terms(terms: &StructArray, pages: usize, column_count: usize, run_count: u32, format: i32) -> Result<()> {
    let columns = terms.column(0).as_primitive::<UInt32Type>();
    let names = terms.column(1).as_string::<i32>();
    let term_pages = terms.column(2).as_primitive::<UInt32Type>();
    let indices = terms.column(3).as_primitive::<UInt32Type>();
    let frequencies = terms.column(4).as_primitive::<UInt32Type>();
    if format == LEGACY_FORMAT_VERSION && pages != terms.len().div_ceil(POSTING_TERMS) + 1 {
        return Err(corrupt("Search postings pages disagree with their directory"));
    }
    let mut previous = None;
    for row in 0..terms.len() {
        let column = columns.value(row) as usize;
        let term = names.value(row);
        if column >= column_count || term.is_empty() || previous.is_some_and(|previous| previous >= (column, term))
            || term_pages.value(row) as usize != row / POSTING_TERMS + 1 || indices.value(row) as usize != row % POSTING_TERMS
            || !(1..=run_count).contains(&frequencies.value(row))
        {
            return Err(corrupt("Search file term directory is invalid"));
        }
        previous = Some((column, term));
    }
    Ok(())
}

fn validate_blocks(directory: &LargeListArray, terms: &StructArray, pages: usize, windows: usize) -> Result<()> {
    let frequencies = terms.column(4).as_primitive::<UInt32Type>();
    let mut slots = BTreeSet::new();
    for term in 0..terms.len() {
        validate_term_blocks(&paged::blocks(directory, term), frequencies.value(term), windows, &mut slots)?;
    }
    let used_pages = slots.into_iter().map(|(page, _)| page).collect::<BTreeSet<_>>();
    if !used_pages.into_iter().eq((FIRST_POSTING_PAGE..pages).step_by(2)) || pages % 2 != 0 {
        return Err(corrupt("Search posting blocks disagree with Arrow pages"));
    }
    Ok(())
}

pub(super) fn validate_term_blocks(blocks: &[Block], frequency: u32, windows: usize, slots: &mut BTreeSet<(usize, usize)>) -> Result<()> {
    let mut count = 0usize;
    let mut previous_window = None;
    for block in blocks {
        if block.page < FIRST_POSTING_PAGE || (block.page - FIRST_POSTING_PAGE) % 2 != 0
            || block.slot >= POSTING_TERMS || block.count == 0 || block.impacts.is_empty() || !slots.insert((block.page, block.slot)) {
            return Err(corrupt("Search posting block exceeds its directory"));
        }
        count = count.checked_add(block.count).ok_or_else(|| corrupt("Search block count overflows"))?;
        for impact in &block.impacts {
            if impact.window as usize >= windows || impact.frequency == 0 || impact.length == 0
                || !impact.max_unit_score.is_finite() || impact.max_unit_score <= 0.0
                || previous_window.is_some_and(|previous| previous >= impact.window) {
                return Err(corrupt("Search posting block has invalid scoring windows"));
            }
            previous_window = Some(impact.window);
        }
    }
    if count != frequency as usize { return Err(corrupt("Search block counts disagree with term frequency")); }
    Ok(())
}

pub(super) fn postings_table(batch: &RecordBatch) -> StructArray { table(batch, Table::Postings) }
