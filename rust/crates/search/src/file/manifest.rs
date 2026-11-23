use std::ops::Range;

use arrow_array::{Array, BinaryArray, FixedSizeBinaryArray, Int64Array, LargeListArray, RecordBatch, StructArray, UInt32Array, cast::AsArray, types::{Int32Type, Int64Type, UInt8Type, UInt32Type, UInt64Type}};
use rtdb_types::Result;

use super::{
    FORMAT_VERSION, LEGACY_FORMAT_VERSION, NULL_DOC, NO_DOC, DocMatch, KeyedEvent, RowDoc, Section, artefact_digest,
    corrupt, op_of, sections_of, layout::{Reader, Table, table}, paged::{self, Block, FIRST_POSTING_PAGE}, postings::POSTING_TERMS,
};
use crate::member_bounds::{MemberStart, document_member};

pub(super) struct Term {
    pub ordinal: usize,
    pub page: usize,
    pub index: usize,
    pub frequency: u32,
}

pub(super) struct Manifest {
    pub rows: u32,
    pub documents: u32,
    pub sections: Vec<Section>,
    pub members: Vec<MemberStart>,
    pub pages: UInt32Array,
    header: StructArray,
    columns: StructArray,
    row_runs: LargeListArray,
    events: StructArray,
    runs: StructArray,
    terms: StructArray,
    blocks: Option<LargeListArray>,
    window_ids: Option<Int64Array>,
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
        let columns = table(&batch, Table::Columns);
        let names = columns.column(0).as_string::<i32>();
        if names.len() != expected.len() || !names.iter().eq(expected.iter().map(|name| Some(name.as_str()))) {
            return Err(corrupt("Search file columns do not match its definition"));
        }
        let visibility = if paged { reader.page(1)? } else { batch.clone() };
        let events = table(&visibility, Table::Events);
        let rows = u32::try_from(events.len()).map_err(|_| corrupt("Search file has too many events"))?;
        let runs = table(&visibility, Table::Runs);
        let documents = u32::try_from(runs.len()).map_err(|_| corrupt("Search file has too many runs"))?;
        let run_counts = columns.column(1).as_primitive::<UInt32Type>();
        if documents >= NULL_DOC || run_counts.values().iter().map(|&count| u64::from(count)).sum::<u64>() != u64::from(documents) {
            return Err(corrupt("Search file column counts disagree with its runs"));
        }
        let pages = table(&batch, Table::Pages).column(0).as_primitive::<UInt32Type>().clone();
        if !ordered_starts(pages.values(), rows) {
            return Err(corrupt("Search file page starts are out of order"));
        }
        let member_table = table(&batch, Table::Members);
        let member_pages = member_table.column(0).as_primitive::<UInt32Type>();
        let member_runs = member_table.column(1).as_primitive::<UInt32Type>();
        if member_table.is_empty() || !ordered_starts(member_pages.values(), pages.len() as u32)
            || !ordered_starts(member_runs.values(), documents)
        {
            return Err(corrupt("Search file member starts are out of order"));
        }
        let members = member_pages.values().iter().zip(member_runs.values().iter())
            .map(|(&page, &doc)| MemberStart { page, doc }).collect();
        let row_runs = table(&visibility, Table::RowRuns).column(0).as_any().downcast_ref::<LargeListArray>()
            .expect("search row-to-run layout").clone();
        if row_runs.len() != expected.len() || row_runs.value_offsets().windows(2).any(|pair| pair[1] - pair[0] != i64::from(rows))
            || row_runs.values().as_primitive::<UInt32Type>().values().iter().any(|&doc| doc >= documents && doc != NO_DOC && doc != NULL_DOC)
        {
            return Err(corrupt("Search file row-to-run map is inconsistent"));
        }
        let terms = table(&batch, Table::Terms);
        validate_terms(&terms, reader.len(), expected.len(), documents, reader.format)?;
        let (blocks, window_ids) = if paged {
            let windows = table(&batch, Table::Windows).column(0).as_primitive::<Int64Type>().clone();
            let directory = table(&batch, Table::Blocks).column(0).as_list::<i64>().clone();
            let count = documents.checked_sub(1).map_or(0, |doc| crate::rank_windows::window_of(doc) + 1) as usize;
            if windows.len() != count || directory.len() != terms.len() || directory.null_count() != 0 {
                return Err(corrupt("Search block directory disagrees with its terms and rows"));
            }
            validate_blocks(&directory, &terms, reader.len(), count)?;
            (Some(directory), Some(windows))
        } else { (None, None) };
        let sections = sections_of(&table(&batch, Table::Sections))?;
        Ok(Self { rows, documents, sections, members, pages, header, columns, row_runs, events, runs, terms, blocks, window_ids })
    }

    pub(super) fn matches_artefact(&self, identity: &str, version: i64) -> bool {
        let versions = self.header.column(2).as_primitive::<Int64Type>();
        let identities = self.header.column(3).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("search identity layout");
        versions.is_valid(0) && identities.is_valid(0) && versions.value(0) == version && identities.value(0) == artefact_digest(identity)
    }

    pub(super) fn row_doc(&self, column: usize, row: u32) -> RowDoc {
        if row >= self.rows || column >= self.row_runs.len() { return RowDoc::Invisible; }
        let offset = self.row_runs.value_offsets()[column] as usize;
        match self.row_runs.values().as_primitive::<UInt32Type>().value(offset + row as usize) {
            NO_DOC => RowDoc::Invisible,
            NULL_DOC => RowDoc::Null,
            doc => RowDoc::Run(doc),
        }
    }

    pub(super) fn doc_class(&self, doc: u32) -> u8 { self.runs.column(3).as_primitive::<UInt8Type>().value(doc as usize) }

    pub(super) fn doc_match(&self, doc: u32) -> DocMatch {
        let member = document_member(&self.members, doc);
        DocMatch { doc, iid: self.doc_iid(doc), member: member as u32 }
    }

    pub(super) fn doc_iid(&self, doc: u32) -> [u8; 16] {
        self.runs.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>()
            .expect("search iid layout").value(doc as usize).try_into().expect("fixed width iid")
    }

    pub(super) fn token_length(&self, doc: u32) -> u32 { self.runs.column(1).as_primitive::<UInt32Type>().value(doc as usize) }

    fn event_keys(&self) -> &BinaryArray {
        self.events.column(5).as_binary::<i32>()
    }

    pub(super) fn has_full_event_keys(&self) -> bool {
        self.event_keys().null_count() == 0
    }

    pub(super) fn event_key(&self, row: u32) -> Option<&[u8]> {
        let keys = self.event_keys();
        keys.is_valid(row as usize).then(|| keys.value(row as usize))
    }

    pub(super) fn run_key(&self, doc: u32) -> Result<&[u8]> {
        let keys = self.runs.column(2).as_binary::<i32>();
        let doc = doc as usize;
        if doc >= keys.len() || keys.is_null(doc) { return Err(corrupt("Search run omitted its full identity")); }
        Ok(keys.value(doc))
    }

    pub(super) fn rectangles(&self, doc: u32) -> impl Iterator<Item = [i64; 4]> + '_ {
        let list = self.runs.column(4).as_any().downcast_ref::<LargeListArray>().expect("search rectangle layout");
        let offsets = list.value_offsets();
        let values = list.values().as_struct();
        let bounds: [_; 4] = std::array::from_fn(|index| values.column(index).as_primitive::<Int64Type>());
        (offsets[doc as usize] as usize..offsets[doc as usize + 1] as usize).map(move |row| bounds.map(|column| column.value(row)))
    }

    pub(super) fn rows_of(&self, iid: &[u8; 16]) -> Range<u32> {
        let iids = self.events.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().expect("search iid layout");
        let mut low = 0;
        let mut high = self.rows;
        while low < high {
            let middle = low + (high - low) / 2;
            if iids.value(middle as usize) < iid.as_slice() { low = middle + 1; } else { high = middle; }
        }
        let start = low;
        high = self.rows;
        while low < high {
            let middle = low + (high - low) / 2;
            if iids.value(middle as usize) <= iid.as_slice() { low = middle + 1; } else { high = middle; }
        }
        start..low
    }

    pub(super) fn event(&self, row: u32) -> Result<KeyedEvent> {
        let row = row as usize;
        let bound = |index| self.events.column(index).as_primitive::<Int64Type>().value(row);
        Ok(KeyedEvent {
            system_from: bound(1), valid_from: bound(2), valid_to: bound(3),
            op: op_of(self.events.column(4).as_primitive::<UInt8Type>().value(row))?,
        })
    }

    pub(super) fn term(&self, column: usize, term: &str) -> Option<Term> {
        let columns = self.terms.column(0).as_primitive::<UInt32Type>();
        let terms = self.terms.column(1).as_string::<i32>();
        let mut first = 0;
        let mut end = terms.len();
        while first < end {
            let middle = first + (end - first) / 2;
            match (columns.value(middle) as usize, terms.value(middle)).cmp(&(column, term)) {
                std::cmp::Ordering::Less => first = middle + 1,
                std::cmp::Ordering::Greater => end = middle,
                std::cmp::Ordering::Equal => return Some(self.term_at(middle)),
            }
        }
        None
    }

    fn term_at(&self, ordinal: usize) -> Term {
        let value = |column| self.terms.column(column).as_primitive::<UInt32Type>().value(ordinal);
        Term { ordinal, page: value(2) as usize, index: value(3) as usize, frequency: value(4) }
    }

    pub(super) fn terms(&self) -> impl ExactSizeIterator<Item = Term> + '_ {
        (0..self.terms.len()).map(|ordinal| self.term_at(ordinal))
    }

    pub(super) fn blocks(&self, term: &Term) -> Option<Vec<Block>> {
        self.blocks.as_ref().map(|directory| paged::blocks(directory, term.ordinal))
    }

    pub(super) fn window_id(&self, window: u32) -> Option<i64> {
        let ids = self.window_ids.as_ref()?;
        let row = window as usize;
        (row < ids.len() && ids.is_valid(row)).then(|| ids.value(row))
    }

    pub(super) fn column_totals(&self, index: usize) -> (u64, u64) {
        (
            u64::from(self.columns.column(1).as_primitive::<UInt32Type>().value(index)),
            self.columns.column(2).as_primitive::<UInt64Type>().value(index),
        )
    }
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
    let mut slots = std::collections::BTreeSet::new();
    let mut used_pages = std::collections::BTreeSet::new();
    for term in 0..terms.len() {
        let blocks = paged::blocks(directory, term);
        let mut count = 0usize;
        let mut previous_window = None;
        for block in blocks {
            if block.page < FIRST_POSTING_PAGE || (block.page - FIRST_POSTING_PAGE) % 2 != 0
                || block.page + 1 >= pages || block.slot >= POSTING_TERMS || block.count == 0
                || block.impacts.is_empty() || !slots.insert((block.page, block.slot)) {
                return Err(corrupt("Search posting block exceeds its directory"));
            }
            used_pages.insert(block.page);
            count = count.checked_add(block.count).ok_or_else(|| corrupt("Search block count overflows"))?;
            for impact in block.impacts {
                if impact.window as usize >= windows || impact.frequency == 0 || impact.length == 0
                    || !impact.max_unit_score.is_finite() || impact.max_unit_score <= 0.0
                    || previous_window.is_some_and(|previous| previous >= impact.window) {
                    return Err(corrupt("Search posting block has invalid scoring windows"));
                }
                previous_window = Some(impact.window);
            }
        }
        if count != frequencies.value(term) as usize { return Err(corrupt("Search block counts disagree with term frequency")); }
    }
    if !used_pages.into_iter().eq((FIRST_POSTING_PAGE..pages).step_by(2)) || pages % 2 != 0 {
        return Err(corrupt("Search posting blocks disagree with Arrow pages"));
    }
    Ok(())
}

pub(super) fn postings_table(batch: &RecordBatch) -> StructArray { table(batch, Table::Postings) }
