use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use rtdb_columnar::MemoryReservation;
use rtdb_text::stored_length;
use rtdb_types::{Anomaly, EntityId, EventHeader, EventPosition, Iid, Result};

use super::{
    FORMAT_VERSION, TREE_FORMAT_VERSION, PACKED_FORMAT_VERSION, NO_DOC, NULL_DOC, IndexedRow, Section, SearchPage, TextRevision, STORED_IMPACTS, artefact_digest,
    fault, lifespan, op_code, sections_array, SearchPosition, event_positions, search_position,
    layout::{Table, Writer, item_of, list, record},
    paged::PostingPlan,
    postings::{POSTING_TERMS, PostingBuilder, posting_table},
};
use crate::{member_bounds::MemberStart, rectangle::merge_adjacent_valid_time};
use crate::term_directory::TermSummaries;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum BuildMode { Combined, Postings, History }

pub(super) struct Events {
    iid: FixedSizeBinaryBuilder,
    system_from: Vec<i64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    operation: Vec<u8>,
    keys: BinaryBuilder,
    positions: Vec<u32>,
    event_positions: Vec<Option<EventPosition>>,
}

impl Events {
    pub fn new() -> Self {
        Self {
            iid: FixedSizeBinaryBuilder::new(16), system_from: Vec::new(),
            valid_from: Vec::new(), valid_to: Vec::new(), operation: Vec::new(), keys: BinaryBuilder::new(), positions: Vec::new(), event_positions: Vec::new(),
        }
    }

    pub fn push(&mut self, header: EventHeader, key: Option<&[u8]>) -> Result<()> {
        self.iid.append_value(header.iid.0).map_err(fault)?;
        self.system_from.push(header.system_from.0);
        self.valid_from.push(header.valid_time.lower().0);
        self.valid_to.push(header.valid_time.upper().0);
        self.operation.push(op_code(header.op));
        self.keys.append_option(key);
        self.positions.push(header.commit_position);
        self.event_positions.push(header.position);
        Ok(())
    }

    pub fn finish(mut self) -> Result<(arrow_array::StructArray, Vec<u32>)> {
        let events = Table::Events.array(vec![
            Arc::new(self.iid.finish()), Arc::new(Int64Array::from(self.system_from)),
            Arc::new(Int64Array::from(self.valid_from)), Arc::new(Int64Array::from(self.valid_to)),
            Arc::new(UInt8Array::from(self.operation)), Arc::new(self.keys.finish()),
            Arc::new(event_positions(self.event_positions)?),
        ])?;
        Ok((events, self.positions))
    }
}

pub(super) struct Runs {
    iids: FixedSizeBinaryBuilder,
    lengths: Vec<u32>,
    keys: BinaryBuilder,
    classes: Vec<u8>,
    rectangles: [Vec<i64>; 4],
    rectangle_counts: Vec<usize>,
}

impl Runs {
    pub fn new() -> Self {
        Self {
            iids: FixedSizeBinaryBuilder::new(16), lengths: Vec::new(),
            keys: BinaryBuilder::new(),
            classes: Vec::new(), rectangles: Default::default(), rectangle_counts: Vec::new(),
        }
    }

    fn len(&self) -> Result<u32> {
        u32::try_from(self.lengths.len()).map_err(fault)
    }

    pub fn push(&mut self, iid: Iid, key: Option<&[u8]>, length: u32, class: u8, rectangles: Vec<[i64; 4]>) -> Result<()> {
        self.iids.append_value(iid.0).map_err(fault)?;
        self.lengths.push(length);
        self.keys.append_option(key);
        self.classes.push(class);
        self.rectangle_counts.push(rectangles.len());
        for rectangle in rectangles {
            for (column, bound) in self.rectangles.iter_mut().zip(rectangle) {
                column.push(bound);
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<arrow_array::StructArray> {
        let item = item_of(&Table::Runs.fields()[4]);
        let arrow_schema::DataType::Struct(fields) = item.data_type() else { unreachable!() };
        let rectangles = arrow_array::StructArray::try_new(fields.clone(), self.rectangles.into_iter()
            .map(|column| Arc::new(Int64Array::from(column)) as ArrayRef).collect(), None).map_err(fault)?;
        Table::Runs.array(vec![
            Arc::new(self.iids.finish()), Arc::new(UInt32Array::from(self.lengths)),
            Arc::new(self.keys.finish()),
            Arc::new(UInt8Array::from(self.classes)), list(Arc::new(rectangles), self.rectangle_counts, item)?,
        ])
    }
}

#[derive(Default)]
struct Column {
    runs: u32,
    tokens: u64,
    row_runs: Vec<u32>,
    postings: BTreeMap<String, PostingBuilder>,
}

enum Output<W> {
    File(Writer<W>),
    Batches(Vec<RecordBatch>),
}

impl<W: Write> Output<W> {
    fn page(&mut self, tables: impl IntoIterator<Item = (Table, StructArray)>) -> Result<()> {
        match self {
            Self::File(writer) => { writer.page(tables)?; }
            Self::Batches(pages) => pages.push(record(FORMAT_VERSION, tables)?),
        }
        Ok(())
    }
}

struct Builder {
    reservation: Option<Arc<MemoryReservation>>,
    estimated: usize,
    ceiling: i64,
    rows: u32,
    members: Vec<MemberStart>,
    pages: Vec<u32>,
    events: Events,
    runs: Runs,
    columns: Vec<Column>,
    sections: BTreeMap<u8, Section>,
    window_ids: Vec<Option<i64>>,
    format: i32,
    mode: BuildMode,
    revisions: Vec<FixedSizeBinaryBuilder>,
    doc_columns: Vec<u32>,
    doc_revisions: FixedSizeBinaryBuilder,
    doc_tokens: arrow_array::builder::LargeListBuilder<StringBuilder>,
    doc_positions: Vec<SearchPosition>,
    erases: BTreeMap<Iid, SearchPosition>,
}

impl Builder {
    fn new(columns: usize, ceiling: Option<i64>, reservation: Option<Arc<MemoryReservation>>, format: i32, mode: BuildMode) -> Self {
        Self {
            reservation, estimated: 0,
            ceiling: ceiling.unwrap_or(i64::MAX), rows: 0, members: Vec::new(), pages: Vec::new(),
            events: Events::new(), runs: Runs::new(), columns: (0..columns).map(|_| Column::default()).collect(),
            sections: BTreeMap::new(), window_ids: Vec::new(), format, mode,
            revisions: (0..if mode == BuildMode::History { columns } else { 0 }).map(|_| FixedSizeBinaryBuilder::new(33)).collect(),
            doc_columns: Vec::new(), doc_revisions: FixedSizeBinaryBuilder::new(32),
            doc_tokens: arrow_array::builder::LargeListBuilder::new(StringBuilder::new()),
            doc_positions: Vec::new(), erases: BTreeMap::new(),
        }
    }

    fn member(&mut self) -> Result<()> {
        self.members.push(MemberStart { page: u32::try_from(self.pages.len()).map_err(fault)?, doc: self.runs.len()? });
        Ok(())
    }

    fn reserve_row(&mut self, row: &IndexedRow<'_>) -> Result<()> {
        let Some(reservation) = &mut self.reservation else { return Ok(()); };
        let identity_bytes = row.key.as_ref().map_or(0, |key| key.len().saturating_mul(4));
        let row_bytes = row.values.iter().flatten().try_fold(
            256usize.saturating_add(identity_bytes).saturating_add(row.values.len().saturating_mul(16)),
            |bytes, tokens| {
                let offsets = tokens.value_offsets();
                let token_bytes = (offsets[tokens.len()] - offsets[0]) as usize;
                bytes.checked_add(token_bytes.saturating_mul(3))
                    .and_then(|bytes| bytes.checked_add(tokens.len().saturating_mul(56)))
                    .ok_or_else(|| fault("Search index allocation exceeds addressable memory"))
            },
        )?;
        self.estimated = self.estimated.checked_add(row_bytes).ok_or_else(|| fault("Search index allocation exceeds addressable memory"))?;
        let reservation = Arc::get_mut(reservation).expect("Search builder owns its allocation");
        if self.estimated > reservation.bytes() {
            reservation.grow_to(self.estimated)?;
        }
        Ok(())
    }

    fn put(&mut self, entities: &mut BTreeMap<Vec<u8>, Entity>, row: IndexedRow<'_>) -> Result<()> {
        if row.header.op == rtdb_types::EventOp::Redacted {
            return Err(fault("Search files require assertion-level redaction coverage"));
        }
        self.reserve_row(&row)?;
        if self.mode == BuildMode::Postings && row.header.op == rtdb_types::EventOp::Erase {
            self.erases.entry(row.header.iid).and_modify(|position| *position = (*position).max(search_position(row.header)))
                .or_insert_with(|| search_position(row.header));
        }
        if self.mode == BuildMode::History {
            if row.revisions.len() != self.revisions.len() { return Err(fault("History revisions do not cover its fields")); }
            for (column, revision) in self.revisions.iter_mut().zip(&row.revisions) {
                let mut encoded = [0; 33];
                if let Some(revision) = revision {
                    encoded[0] = 1;
                    encoded[1..].copy_from_slice(revision);
                }
                column.append_value(encoded).map_err(fault)?;
            }
        }
        if self.mode != BuildMode::Postings {
            self.events.push(row.header, row.key.as_deref())?;
            for column in &mut self.columns { column.row_runs.push(NO_DOC); }
        }
        if self.mode != BuildMode::History {
            let key = row.key.as_deref().unwrap_or_default();
            if !entities.contains_key(key) {
                entities.insert(key.to_vec(), Entity::new(self.columns.len()));
            }
            let entity = entities.get_mut(key).expect("key has an entity");
            entity.push(self.rows, row)?;
        }
        self.rows = self.rows.checked_add(1).ok_or_else(|| fault("A search file holds at most u32::MAX events"))?;
        Ok(())
    }

    fn entity(&mut self, iid: Iid, key: &[u8], entity: Entity) -> Result<()> {
        let key = (!key.is_empty()).then_some(key);
        let mut visible: Vec<Vec<[i64; 4]>> = vec![Vec::new(); entity.rows.len()];
        if self.mode == BuildMode::Postings {
            for (index, (_, header, _)) in entity.rows.iter().enumerate() {
                if header.op == rtdb_types::EventOp::Put
                    && self.erases.get(&iid).is_none_or(|position| search_position(*header) >= *position) {
                    visible[index].push([i64::MIN, i64::MAX, i64::MIN, i64::MAX]);
                }
            }
        } else {
            for (index, rectangle) in rtdb_temporal::visibility(entity.rows.iter().enumerate().map(|(index, (_, header, _))| (header, index)))? {
                visible[index].push([
                    rectangle.valid_time.lower().0, rectangle.valid_time.upper().0,
                    rectangle.system_time.lower().0, rectangle.system_time.upper().0,
                ]);
            }
        }
        for (column_index, (column, values)) in self.columns.iter_mut().zip(entity.columns).enumerate() {
            let mut members = vec![Vec::new(); values.tokens.len()];
            for (index, run) in values.row_tokens.into_iter().enumerate() {
                if visible[index].is_empty() { continue; }
                if let Some(run) = run {
                    members[run].push(index);
                } else if self.mode != BuildMode::Postings {
                    column.row_runs[entity.rows[index].0 as usize] = NULL_DOC;
                }
            }
            for ((tokens, revision), members) in values.tokens.into_iter().zip(values.revisions).zip(members) {
                if members.is_empty() { continue; }
                let doc = self.runs.len()?;
                if doc >= NULL_DOC { return Err(fault("A search file has too many runs")); }
                let sql_id = entity.rows[members[0]].2
                    .filter(|id| self.format >= FORMAT_VERSION && members.iter().all(|&index| entity.rows[index].2 == Some(*id)));
                let length = u32::try_from(tokens.len()).map_err(fault)?;
                let mut term_positions = BTreeMap::<&str, Vec<u32>>::new();
                for (position, token) in tokens.iter().flatten().enumerate() {
                    term_positions.entry(token).or_default().push(position as u32);
                }
                let norm = stored_length(length as usize);
                for (term, positions) in term_positions {
                    if let Some(postings) = column.postings.get_mut(term) {
                        postings.push(doc, norm, positions, sql_id);
                    } else {
                        let mut postings = PostingBuilder::default();
                        postings.push(doc, norm, positions, sql_id);
                        column.postings.insert(term.to_owned(), postings);
                    }
                }
                let rectangles = coalesce(members.iter().flat_map(|index| visible[*index].iter().copied()).collect());
                let (class, section) = lifespan(&rectangles, self.ceiling);
                self.sections.entry(class).and_modify(|current| current.widen(&section)).or_insert(section);
                let window = crate::rank_windows::window_of(doc) as usize;
                if window == self.window_ids.len() { self.window_ids.push(sql_id); }
                else { self.window_ids[window] = self.window_ids[window].zip(sql_id).map(|(first, id)| first.min(id)); }
                let sql_key = sql_id.map(|id| EntityId::Integer(id).handle_bytes());
                self.runs.push(iid, key.or(sql_key.as_deref()), length, class, rectangles)?;
                if self.mode == BuildMode::Postings {
                    let revision = revision.ok_or_else(|| fault("Search document has no logical text revision"))?;
                    self.doc_columns.push(column_index as u32);
                    self.doc_revisions.append_value(revision).map_err(fault)?;
                    self.doc_positions.push(members.iter().map(|&index| search_position(entity.rows[index].1))
                        .max().expect("a posting has at least one original event"));
                    self.doc_tokens.values().append_array(&tokens).map_err(fault)?;
                    self.doc_tokens.append(true);
                }
                column.runs += 1;
                column.tokens += u64::from(length);
                if self.mode != BuildMode::Postings {
                    for index in members { column.row_runs[entity.rows[index].0 as usize] = doc; }
                }
            }
        }
        Ok(())
    }

    fn finish(mut self, columns: &[String], version: Option<i64>, identity: &str, output: &mut Output<impl Write>) -> Result<(Vec<Section>, TermSummaries)> {
        if self.members.is_empty() { return Err(fault("A search file holds at least one member")); }
        let sections: Vec<_> = self.sections.into_values().collect();
        let mut summaries = TermSummaries::default();
        let header = Table::Header.array(vec![
            Arc::new(Int32Array::from(vec![self.format])),
            Arc::new(Int32Array::from(vec![rtdb_text::ANALYZER_VERSION])),
            Arc::new(Int64Array::from(vec![version])),
            Arc::new(arrow_array::FixedSizeBinaryArray::try_from_iter([artefact_digest(identity)].iter()).map_err(fault)?),
        ])?;
        let column_table = Table::Columns.array(vec![
            Arc::new(StringArray::from_iter_values(columns)), Arc::new(UInt32Array::from_iter_values(self.columns.iter().map(|column| column.runs))),
            Arc::new(UInt64Array::from_iter_values(self.columns.iter().map(|column| column.tokens))),
        ])?;
        let members = Table::Members.array(vec![
            Arc::new(UInt32Array::from_iter_values(self.members.iter().map(|member| member.page))),
            Arc::new(UInt32Array::from_iter_values(self.members.iter().map(|member| member.doc))),
        ])?;
        let pages = Table::Pages.array(vec![Arc::new(UInt32Array::from(self.pages))])?;
        let (row_runs, postings): (Vec<_>, Vec<_>) = self.columns.into_iter().map(|column| (column.row_runs, column.postings)).unzip();
        let counts: Vec<_> = row_runs.iter().map(Vec::len).collect();
        let row_runs = Table::RowRuns.array(vec![list(Arc::new(UInt32Array::from_iter_values(row_runs.into_iter().flatten())),
            counts, item_of(&Table::RowRuns.fields()[0]))?])?;
        let term_count = postings.iter().map(BTreeMap::len).sum::<usize>();
        if term_count > u32::MAX as usize { return Err(fault("A search file has too many terms")); }
        let mut term_columns = Vec::with_capacity(term_count);
        let mut term_text = StringBuilder::new();
        let mut term_frequency = Vec::with_capacity(term_count);
        for (column, postings) in postings.iter().enumerate() {
            for (term, postings) in postings {
                term_columns.push(column as u32);
                term_text.append_value(term);
                term_frequency.push(postings.len() as u32);
            }
        }
        let terms = Table::Terms.array(vec![
            Arc::new(UInt32Array::from(term_columns)), Arc::new(term_text.finish()),
            Arc::new(UInt32Array::from_iter_values((0..term_count).map(|index| (index / POSTING_TERMS + 1) as u32))),
            Arc::new(UInt32Array::from_iter_values((0..term_count).map(|index| (index % POSTING_TERMS) as u32))),
            Arc::new(UInt32Array::from(term_frequency)),
        ])?;
        let mut directory = vec![(Table::Header, header), (Table::Columns, column_table), (Table::Members, members),
            (Table::Pages, pages), (Table::Sections, sections_array(&sections)?), (Table::Terms, terms)];
        let (events, positions) = self.events.finish()?;
        let visibility = [(Table::Events, events), (Table::Runs, self.runs.finish()?), (Table::RowRuns, row_runs)];
        let postings = postings.into_iter().flat_map(BTreeMap::into_values);
        if self.format >= FORMAT_VERSION {
            let plan = PostingPlan::new(postings);
            if self.format >= TREE_FORMAT_VERSION {
                directory.push((Table::ImpactIds, plan.ids(&self.window_ids)?));
            }
            directory.push((Table::Windows, Table::Windows.array(vec![Arc::new(Int64Array::from(self.window_ids))])?));
            directory.push((Table::Blocks, plan.directory()?));
            if self.format >= TREE_FORMAT_VERSION {
                let revisions = if self.mode == BuildMode::History {
                    let digests = self.revisions.into_iter().map(|mut values| values.finish()).collect::<Vec<_>>();
                    let references = digests.iter().map(|values| values as &dyn Array).collect::<Vec<_>>();
                    Some(Table::TextRevisions.array(vec![list(arrow_select::concat::concat(&references).map_err(fault)?,
                        std::iter::repeat_n(self.rows as usize, columns.len()), item_of(&Table::TextRevisions.fields()[0]))?])?)
                } else { None };
                let documents = if self.mode == BuildMode::Postings {
                    let tokens = self.doc_tokens.finish();
                    Some(Table::TokenSequences.array(vec![Arc::new(UInt32Array::from(self.doc_columns)), Arc::new(self.doc_revisions.finish()),
                        list(tokens.values().clone(), (0..tokens.len()).map(|row| tokens.value_length(row) as usize),
                            item_of(&Table::TokenSequences.fields()[2]))?,
                        Arc::new(Int64Array::from_iter_values(self.doc_positions.iter().map(|(timestamp, _, _)| timestamp.0))),
                        Arc::new(UInt32Array::from_iter_values(self.doc_positions.iter().map(|(_, _, ordinal)| *ordinal))),
                        Arc::new(event_positions(self.doc_positions.iter().map(|(_, position, _)| *position))?)])?)
                } else { None };
                let erases = if !self.erases.is_empty() {
                    Some(Table::Erases.array(vec![
                        Arc::new(arrow_array::FixedSizeBinaryArray::try_from_iter(self.erases.keys().map(|iid| iid.0)).map_err(fault)?),
                        Arc::new(Int64Array::from_iter_values(self.erases.values().map(|(time, _, _)| time.0))),
                        Arc::new(UInt32Array::from_iter_values(self.erases.values().map(|(_, _, ordinal)| *ordinal))),
                        Arc::new(event_positions(self.erases.values().map(|(_, position, _)| *position))?),
                    ])?)
                } else { None };
                let Output::File(writer) = output else { unreachable!("Tree pages require durable offsets"); };
                summaries = super::tree_file::write(writer, directory, visibility, plan,
                    (self.format >= PACKED_FORMAT_VERSION).then_some(positions), revisions, documents, erases)?;
            } else {
                output.page(directory)?;
                output.page(visibility)?;
                plan.emit(|table, batch| output.page([(table, batch)]))?;
            }
        } else {
            directory.extend(visibility);
            output.page(directory)?;
            let mut postings = postings.peekable();
            while postings.peek().is_some() {
                let page = postings.by_ref().take(POSTING_TERMS).collect();
                output.page([(Table::Postings, posting_table(page, STORED_IMPACTS)?)])?;
            }
        }
        summaries.retain(self.reservation)?;
        Ok((sections, summaries))
    }
}

#[derive(Default)]
struct EntityColumn {
    row_tokens: Vec<Option<usize>>,
    tokens: Vec<StringArray>,
    revisions: Vec<Option<TextRevision>>,
    distinct: HashMap<u64, Vec<usize>>,
}

struct Entity {
    rows: Vec<(u32, EventHeader, Option<i64>)>,
    columns: Vec<EntityColumn>,
}

impl Entity {
    fn new(columns: usize) -> Self {
        Self { rows: Vec::new(), columns: (0..columns).map(|_| EntityColumn::default()).collect() }
    }

    fn push(&mut self, ordinal: u32, row: IndexedRow<'_>) -> Result<()> {
        let header = row.header;
        self.rows.push((ordinal, header, row.sql_id));
        let revisions = row.revisions.into_iter().chain(std::iter::repeat(None));
        for ((column, value), revision) in self.columns.iter_mut().zip(row.values).zip(revisions) {
            column.row_tokens.push(value.map(|tokens| {
                let mut hash = DefaultHasher::new();
                tokens.iter().for_each(|token| token.hash(&mut hash));
                revision.hash(&mut hash);
                let positions = column.distinct.entry(hash.finish()).or_default();
                if let Some(&position) = positions.iter().find(|&&position| column.tokens[position] == tokens && column.revisions[position] == revision) {
                    return position;
                }
                let position = column.tokens.len();
                positions.push(position);
                column.tokens.push(tokens);
                column.revisions.push(revision);
                position
            }));
        }
        Ok(())
    }
}

pub(super) fn coalesce(mut rectangles: Vec<[i64; 4]>) -> Vec<[i64; 4]> {
    rectangles.sort_unstable_by_key(|rectangle| (rectangle[2], rectangle[3], rectangle[0]));
    let mut merged = Vec::with_capacity(rectangles.len());
    for rectangle in rectangles {
        if let Some(last) = merged.last_mut()
            && let Some(joined) = merge_adjacent_valid_time(*last, rectangle)
        {
            *last = joined;
            continue;
        }
        merged.push(rectangle);
    }
    merged.sort_unstable_by_key(|rectangle| (rectangle[2], rectangle[0]));
    merged
}

pub(super) fn write_group<P, I>(
    columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, version: Option<i64>,
    ceiling: Option<i64>, output: impl Write, reservation: Option<Arc<MemoryReservation>>, format: i32,
) -> Result<Vec<Section>>
where P: IntoIterator<Item = Result<I>>, I: SearchPage,
{
    write_group_as(columns, members, identity, version, ceiling, output, reservation, format, BuildMode::Combined).map(|(sections, _)| sections)
}

pub(crate) fn write_group_as<P, I>(
    columns: &[String], members: impl IntoIterator<Item = P>, identity: &str, version: Option<i64>,
    ceiling: Option<i64>, output: impl Write, reservation: Option<Arc<MemoryReservation>>, format: i32, mode: BuildMode,
) -> Result<(Vec<Section>, TermSummaries)>
where P: IntoIterator<Item = Result<I>>, I: SearchPage,
{
    if format == super::TANTIVY_FORMAT_VERSION {
        let version = version.ok_or_else(|| fault("Legacy publication requires an ordered definition version"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let store = tantivy::directory::MmapDirectory::open(directory.path()).map_err(fault)?;
        let sections = super::legacy11::write_versioned_group(columns, members, identity, version, ceiling, store, output)?;
        return Ok((sections.into_iter().map(super::legacy11::Section::into_arrow).collect(), TermSummaries::default()));
    }
    let builder = build_group(columns, members, ceiling, reservation, format, mode)?;
    let mut output = Output::File(Writer::new(output, format)?);
    let result = builder.finish(columns, version, identity, &mut output)?;
    let Output::File(writer) = output else { unreachable!() };
    writer.finish()?;
    Ok(result)
}

pub(super) fn query_batches<I: SearchPage>(columns: &[String], pages: impl IntoIterator<Item = Result<I>>,
    ceiling: Option<i64>, reservation: Arc<MemoryReservation>) -> Result<Vec<RecordBatch>> {
    let builder = build_group(columns, [pages], ceiling, Some(reservation), FORMAT_VERSION, BuildMode::Combined)?;
    let mut output = Output::<std::io::Sink>::Batches(Vec::new());
    builder.finish(columns, None, "built", &mut output)?;
    let Output::Batches(pages) = output else { unreachable!() };
    Ok(pages)
}

fn build_group<P, I>(
    columns: &[String], members: impl IntoIterator<Item = P>, ceiling: Option<i64>,
    reservation: Option<Arc<MemoryReservation>>, format: i32, mode: BuildMode,
) -> Result<Builder>
where P: IntoIterator<Item = Result<I>>, I: SearchPage,
{
    let mut builder = Builder::new(columns.len(), ceiling, reservation, format, mode);
    let mut previous_member_iid = None;
    for pages in members {
        builder.member()?;
        let mut entities: BTreeMap<Vec<u8>, Entity> = BTreeMap::new();
        let mut member_iid = None;
        let mut previous_time = None;
        for page in pages {
            let mut page = page?;
            if mode != BuildMode::Postings { builder.pages.push(builder.rows); }
            for index in 0..page.len() {
                let row = page.row(index, columns)?;
                let iid = row.header.iid;
                if member_iid.is_none() && previous_member_iid.is_some_and(|previous| previous >= iid) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search file members do not hold disjoint IID ranges in order"));
                }
                if let Some(previous) = member_iid {
                    if previous > iid {
                        return Err(Anomaly::unsupported("rtdb.search/input-order", "Search rows are not in IID order"));
                    }
                    if previous != iid {
                        for (key, entity) in std::mem::take(&mut entities) {
                            builder.entity(previous, &key, entity)?;
                        }
                        previous_time = None;
                    }
                }
                if previous_time.is_some_and(|previous| previous < row.header.precedence()) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search rows are not in system-time-descending order"));
                }
                previous_time = Some(row.header.precedence());
                member_iid = Some(iid);
                builder.put(&mut entities, row)?;
            }
            page.complete();
        }
        if let Some(iid) = member_iid {
            for (key, entity) in entities {
                builder.entity(iid, &key, entity)?;
            }
            previous_member_iid = Some(iid);
        }
    }
    if builder.members.is_empty() { return Err(fault("A search file holds at least one member")); }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_build_bulk_run_tokens_keep_logical_file_bytes() -> Result<()> {
        struct Documents(Vec<StringArray>);

        impl SearchPage for Documents {
            fn len(&self) -> usize { self.0.len() }

            fn row(&mut self, row: usize, _: &[String]) -> Result<IndexedRow<'_>> {
                let tokens = self.0[row].clone();
                let header = EventHeader { position: None,
                    iid: Iid([row as u8 + 1; 16]), system_from: rtdb_types::Timestamp(11), commit_position: 1,
                    valid_time: rtdb_types::Interval::ALL, op: rtdb_types::EventOp::Put,
                };
                Ok(IndexedRow { header, key: None, sql_id: None, values: vec![Some(tokens)], revisions: vec![Some([row as u8; 32])] })
            }
        }

        let columns = ["body".to_owned()];
        let prefix = "x".repeat(1 << 16);
        let existing = StringArray::from(vec![prefix.as_str(), "éclair", "audit", "audit", "suffix"]);
        let documents = || Documents(vec![existing.slice(1, 3), existing.slice(3, 0), existing.slice(3, 2)]);
        let format = super::super::LOGICAL_FORMAT_VERSION;
        let mut old_tokens = arrow_array::builder::LargeListBuilder::new(StringBuilder::new());
        for tokens in &documents().0 {
            for token in tokens.iter().flatten() { old_tokens.values().append_value(token); }
            old_tokens.append(true);
        }

        let mut previous = build_group(&columns, [[Ok(documents())]], None, None, format, BuildMode::Postings)?;
        previous.doc_tokens = old_tokens;
        let mut expected = Vec::new();
        let mut output = Output::File(Writer::new(&mut expected, format)?);
        previous.finish(&columns, Some(1), "token-fixture", &mut output)?;
        let Output::File(writer) = output else { unreachable!() };
        writer.finish()?;

        let mut actual = Vec::new();
        let memory = rtdb_columnar::MemoryBudget::new(4096);
        write_group_as(&columns, [[Ok(documents())]], "token-fixture", Some(1), None, &mut actual,
            Some(memory.reserve(4096)?), format, BuildMode::Postings)?;
        assert_eq!(actual, expected);
        Ok(())
    }
}
