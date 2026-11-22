use std::sync::Arc;

use arrow_array::{Array, LargeListArray, StructArray, UInt32Array, cast::AsArray, types::UInt32Type};
use rtdb_text::Query;
use rtdb_types::Result;

use super::{corrupt, fault, layout::{Table, item_of, list}};
use crate::rank_windows::{extend_impact, window_of};

pub(super) const POSTING_TERMS: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Impact {
    pub window: u32,
    pub frequency: u32,
    pub length: u32,
}

fn add_impact(impacts: &mut Vec<Impact>, doc: u32, frequency: u32, length: u32) {
    let window = window_of(doc);
    match impacts.last_mut() {
        Some(last) if last.window == window => {
            (last.frequency, last.length) = extend_impact(last.frequency, last.length, frequency, length);
        }
        _ => impacts.push(Impact { window, frequency, length }),
    }
}

#[derive(Default)]
pub(super) struct PostingBuilder {
    documents: Vec<u32>,
    position_counts: Vec<usize>,
    positions: Vec<u32>,
    windows: Vec<Impact>,
}

impl PostingBuilder {
    pub(super) fn len(&self) -> usize { self.documents.len() }

    pub(super) fn push(&mut self, doc: u32, length: u32, positions: Vec<u32>) {
        self.documents.push(doc);
        self.position_counts.push(positions.len());
        add_impact(&mut self.windows, doc, positions.len() as u32, length);
        self.positions.extend(positions);
    }
}

pub(super) fn posting_table(terms: Vec<PostingBuilder>, stored_impacts: usize) -> Result<StructArray> {
    let mut doc_counts = Vec::with_capacity(terms.len());
    let mut documents = Vec::new();
    let mut position_counts = Vec::new();
    let mut positions = Vec::new();
    let mut window_counts = Vec::with_capacity(terms.len());
    let mut windows = Vec::new();
    let mut max_frequencies = Vec::new();
    let mut min_lengths = Vec::new();
    for term in terms {
        let count = term.len();
        doc_counts.push(count);
        documents.extend(term.documents);
        position_counts.extend(term.position_counts);
        positions.extend(term.positions);
        if count >= stored_impacts {
            window_counts.push(term.windows.len());
            for impact in term.windows {
                windows.push(impact.window);
                max_frequencies.push(impact.frequency);
                min_lengths.push(impact.length);
            }
        } else {
            window_counts.push(0);
        }
    }
    let fields = Table::Postings.fields();
    let documents = list(Arc::new(UInt32Array::from(documents)), doc_counts.iter().copied(), item_of(&fields[0]))?;
    let position_lists = item_of(&fields[1]);
    let positions = list(Arc::new(UInt32Array::from(positions)), position_counts, item_of(&position_lists))?;
    let positions = list(positions, doc_counts, position_lists)?;
    let item = item_of(&fields[2]);
    let arrow_schema::DataType::Struct(window_fields) = item.data_type() else { unreachable!() };
    let windows = StructArray::try_new(window_fields.clone(), vec![
        Arc::new(UInt32Array::from(windows)), Arc::new(UInt32Array::from(max_frequencies)),
        Arc::new(UInt32Array::from(min_lengths)),
    ], None).map_err(fault)?;
    Table::Postings.array(vec![documents, positions, list(Arc::new(windows), window_counts, item)?])
}

pub(crate) struct Postings {
    documents: UInt32Array,
    positions: LargeListArray,
    windows: StructArray,
}

impl Postings {
    pub(super) fn from_table(table: &StructArray, row: usize) -> Result<Self> {
        if row >= table.len() { return Err(corrupt("Search term points outside its postings batch")); }
        let get = |column: usize| {
            let list = table.column(column).as_any().downcast_ref::<LargeListArray>()
                .expect("search postings layout");
            if list.is_null(row) { return Err(corrupt("Search term omits its postings")); }
            Ok(list.value(row))
        };
        let documents = get(0)?.as_primitive::<UInt32Type>().clone();
        let positions = get(1)?.as_any().downcast_ref::<LargeListArray>().expect("search positions layout").clone();
        let windows = get(2)?.as_struct().clone();
        Ok(Self { documents, positions, windows })
    }

    pub(super) fn validate(&self, frequency: usize, runs: u32) -> Result<()> {
        if self.documents.len() != frequency || self.positions.len() != frequency
            || self.documents.null_count() != 0 || self.positions.null_count() != 0
            || self.documents.values().last().is_some_and(|doc| *doc >= runs)
            || self.documents.values().windows(2).any(|pair| pair[0] >= pair[1])
            || self.positions.value_offsets().windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(corrupt("Search postings disagree with their directory"));
        }
        Ok(())
    }

    pub(crate) fn documents(&self) -> &[u32] { self.documents.values() }

    fn index(&self, doc: u32) -> Option<usize> { self.documents().binary_search(&doc).ok() }

    fn positions(&self, index: usize) -> &[u32] {
        let offsets = self.positions.value_offsets();
        let values = self.positions.values().as_primitive::<UInt32Type>();
        &values.values()[offsets[index] as usize..offsets[index + 1] as usize]
    }

    pub(crate) fn frequency(&self, doc: u32) -> u32 {
        self.index(doc).map_or(0, |index| self.positions(index).len() as u32)
    }

    pub(super) fn has_stored_impacts(&self) -> bool { !self.windows.is_empty() }

    pub(super) fn impacts(&self, stored_length: impl Fn(u32) -> u32) -> Vec<Impact> {
        if self.has_stored_impacts() {
            let window = self.windows.column(0).as_primitive::<UInt32Type>();
            let frequency = self.windows.column(1).as_primitive::<UInt32Type>();
            let length = self.windows.column(2).as_primitive::<UInt32Type>();
            return (0..self.windows.len()).map(|row| Impact {
                window: window.value(row), frequency: frequency.value(row), length: length.value(row),
            }).collect();
        }
        let mut impacts = Vec::new();
        for (index, &doc) in self.documents().iter().enumerate() {
            add_impact(&mut impacts, doc, self.positions(index).len() as u32, stored_length(doc));
        }
        impacts
    }
}

pub(super) struct Matching {
    terms: Vec<Postings>,
    lead: usize,
    phrases: Vec<Vec<usize>>,
}

impl Matching {
    pub(super) fn new(query: &Query, postings: impl FnMut(&str) -> Result<Option<Postings>>) -> Result<Option<Self>> {
        let Some(terms) = query.terms().iter().map(String::as_str).map(postings)
            .collect::<Result<Vec<_>>>()?.into_iter().collect::<Option<Vec<_>>>() else { return Ok(None) };
        let Some((lead, _)) = terms.iter().enumerate().min_by_key(|(_, term)| term.documents().len()) else { return Ok(None) };
        let phrases = query.phrases().iter().map(|phrase| {
            phrase.iter().map(|word| query.terms().iter().position(|term| term == word).expect("all phrase terms are collected")).collect()
        }).collect();
        Ok(Some(Self { terms, lead, phrases }))
    }

    pub(super) fn terms(&self) -> &[Postings] { &self.terms }

    pub(super) fn documents(&self) -> &[u32] { self.terms[self.lead].documents() }

    pub(super) fn includes(&self, doc: u32) -> bool {
        if self.phrases.is_empty() {
            return self.terms.iter().all(|term| term.index(doc).is_some());
        }
        let Some(positions) = self.terms.iter().map(|term| term.index(doc).map(|index| term.positions(index)))
            .collect::<Option<Vec<_>>>() else { return false };
        self.phrases.iter().all(|phrase| positions[phrase[0]].iter().any(|&start| {
            phrase.iter().skip(1).enumerate().all(|(offset, &term)| {
                start.checked_add(offset as u32 + 1).is_some_and(|position| positions[term].binary_search(&position).is_ok())
            })
        }))
    }
}
