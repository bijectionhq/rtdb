use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, StringArray, UInt8Array, UInt32Array, UInt64Array,
    builder::{FixedSizeBinaryBuilder, Int64Builder, StringBuilder, UInt8Builder},
};
use rtdb_columnar::{EntityIdRef, MemoryReservation};
use rtdb_text::stored_length;
use rtdb_types::{Anomaly, EntityId, EventHeader, Iid, Result};

use super::{
    FORMAT_VERSION, NO_DOC, NULL_DOC, IndexedRow, Section, SearchPage, STORED_IMPACTS, artefact_digest,
    fault, lifespan, op_code, sections_array,
    layout::{Table, Writer, item_of, list},
    postings::{POSTING_TERMS, PostingBuilder, posting_table},
};
use crate::{member_bounds::MemberStart, rectangle::merge_adjacent_valid_time};

struct Events {
    iid: FixedSizeBinaryBuilder,
    system_from: Vec<i64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    operation: Vec<u8>,
}

impl Events {
    fn new() -> Self {
        Self {
            iid: FixedSizeBinaryBuilder::new(16), system_from: Vec::new(),
            valid_from: Vec::new(), valid_to: Vec::new(), operation: Vec::new(),
        }
    }

    fn push(&mut self, header: EventHeader) -> Result<()> {
        self.iid.append_value(header.iid.0).map_err(fault)?;
        self.system_from.push(header.system_from.0);
        self.valid_from.push(header.valid_time.lower().0);
        self.valid_to.push(header.valid_time.upper().0);
        self.operation.push(op_code(header.op));
        Ok(())
    }

    fn finish(mut self) -> Result<arrow_array::StructArray> {
        Table::Events.array(vec![
            Arc::new(self.iid.finish()), Arc::new(Int64Array::from(self.system_from)),
            Arc::new(Int64Array::from(self.valid_from)), Arc::new(Int64Array::from(self.valid_to)),
            Arc::new(UInt8Array::from(self.operation)),
        ])
    }
}

struct Runs {
    iids: FixedSizeBinaryBuilder,
    lengths: Vec<u32>,
    identity_type: UInt8Builder,
    identity_text: StringBuilder,
    identity_integer: Int64Builder,
    identity_uuid: FixedSizeBinaryBuilder,
    classes: Vec<u8>,
    rectangles: [Vec<i64>; 4],
    rectangle_counts: Vec<usize>,
}

impl Runs {
    fn new() -> Self {
        Self {
            iids: FixedSizeBinaryBuilder::new(16), lengths: Vec::new(),
            identity_type: UInt8Builder::new(), identity_text: StringBuilder::new(),
            identity_integer: Int64Builder::new(), identity_uuid: FixedSizeBinaryBuilder::new(16),
            classes: Vec::new(), rectangles: Default::default(), rectangle_counts: Vec::new(),
        }
    }

    fn len(&self) -> Result<u32> {
        u32::try_from(self.lengths.len()).map_err(fault)
    }

    fn push(&mut self, iid: Iid, id: Option<&EntityId>, length: u32, class: u8, rectangles: Vec<[i64; 4]>) -> Result<()> {
        self.iids.append_value(iid.0).map_err(fault)?;
        self.lengths.push(length);
        let (tag, text, integer, uuid) = match id {
            None => (0, None, None, None),
            Some(EntityId::String(value)) => (1, Some(value.as_str()), None, None),
            Some(EntityId::Keyword(value)) => (2, Some(value.as_str()), None, None),
            Some(EntityId::Integer(value)) => (3, None, Some(*value), None),
            Some(EntityId::Uuid(value)) => (4, None, None, Some(value)),
        };
        self.identity_type.append_value(tag);
        self.identity_text.append_option(text);
        self.identity_integer.append_option(integer);
        if let Some(uuid) = uuid {
            self.identity_uuid.append_value(uuid).map_err(fault)?;
        } else {
            self.identity_uuid.append_null();
        }
        self.classes.push(class);
        self.rectangle_counts.push(rectangles.len());
        for rectangle in rectangles {
            for (column, bound) in self.rectangles.iter_mut().zip(rectangle) {
                column.push(bound);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<arrow_array::StructArray> {
        let item = item_of(&Table::Runs.fields()[7]);
        let arrow_schema::DataType::Struct(fields) = item.data_type() else { unreachable!() };
        let rectangles = arrow_array::StructArray::try_new(fields.clone(), self.rectangles.into_iter()
            .map(|column| Arc::new(Int64Array::from(column)) as ArrayRef).collect(), None).map_err(fault)?;
        Table::Runs.array(vec![
            Arc::new(self.iids.finish()), Arc::new(UInt32Array::from(self.lengths)),
            Arc::new(self.identity_type.finish()), Arc::new(self.identity_text.finish()),
            Arc::new(self.identity_integer.finish()), Arc::new(self.identity_uuid.finish()),
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
}

impl Builder {
    fn new(columns: usize, ceiling: Option<i64>, reservation: Option<Arc<MemoryReservation>>) -> Self {
        Self {
            reservation, estimated: 0,
            ceiling: ceiling.unwrap_or(i64::MAX), rows: 0, members: Vec::new(), pages: Vec::new(),
            events: Events::new(), runs: Runs::new(), columns: (0..columns).map(|_| Column::default()).collect(),
            sections: BTreeMap::new(),
        }
    }

    fn member(&mut self) -> Result<()> {
        self.members.push(MemberStart { page: u32::try_from(self.pages.len()).map_err(fault)?, doc: self.runs.len()? });
        Ok(())
    }

    fn reserve_row(&mut self, row: &IndexedRow<'_>) -> Result<()> {
        let Some(reservation) = &mut self.reservation else { return Ok(()); };
        let identity_bytes = match row.identity {
            Some(EntityIdRef::String(text) | EntityIdRef::Keyword(text)) => text.len().saturating_mul(4),
            _ => 0,
        };
        let row_bytes = row.values.iter().flatten().try_fold(
            256usize.saturating_add(identity_bytes).saturating_add(row.values.len().saturating_mul(16)),
            |bytes, tokens| bytes.checked_add(((tokens.value_offsets()[tokens.len()] - tokens.value_offsets()[0]) as usize).saturating_mul(3))
                .and_then(|bytes| bytes.checked_add(tokens.len().saturating_mul(56)))
                .ok_or_else(|| fault("Search index allocation exceeds addressable memory")),
        )?;
        self.estimated = self.estimated.checked_add(row_bytes).ok_or_else(|| fault("Search index allocation exceeds addressable memory"))?;
        let reservation = Arc::get_mut(reservation).expect("Search builder owns its allocation");
        if self.estimated > reservation.bytes() {
            reservation.grow_to(self.estimated)?;
        }
        Ok(())
    }

    fn put(&mut self, entity: &mut Entity, row: IndexedRow<'_>) -> Result<()> {
        self.reserve_row(&row)?;
        self.events.push(row.header)?;
        for column in &mut self.columns { column.row_runs.push(NO_DOC); }
        entity.push(self.rows, row)?;
        self.rows = self.rows.checked_add(1).ok_or_else(|| fault("A search file holds at most u32::MAX events"))?;
        Ok(())
    }

    fn entity(&mut self, entity: Entity) -> Result<()> {
        let Some(iid) = entity.iid else { return Ok(()); };
        let mut visible: Vec<Vec<[i64; 4]>> = vec![Vec::new(); entity.rows.len()];
        for (index, rectangle) in rtdb_temporal::visibility(entity.rows.iter().enumerate().map(|(index, (_, header))| (header, index)))? {
            visible[index].push([
                rectangle.valid_time.lower().0, rectangle.valid_time.upper().0,
                rectangle.system_time.lower().0, rectangle.system_time.upper().0,
            ]);
        }
        for (column, values) in self.columns.iter_mut().zip(entity.columns) {
            let mut members = vec![Vec::new(); values.tokens.len()];
            for (index, run) in values.row_tokens.into_iter().enumerate() {
                if visible[index].is_empty() { continue; }
                if let Some(run) = run {
                    members[run].push(index);
                } else {
                    column.row_runs[entity.rows[index].0 as usize] = NULL_DOC;
                }
            }
            for (tokens, members) in values.tokens.into_iter().zip(members) {
                if members.is_empty() { continue; }
                let doc = self.runs.len()?;
                if doc >= NULL_DOC { return Err(fault("A search file has too many runs")); }
                let length = u32::try_from(tokens.len()).map_err(fault)?;
                let mut term_positions = BTreeMap::<&str, Vec<u32>>::new();
                for (position, token) in tokens.iter().flatten().enumerate() {
                    term_positions.entry(token).or_default().push(position as u32);
                }
                let norm = stored_length(length as usize);
                for (term, positions) in term_positions {
                    column.postings.entry(term.to_owned()).or_default().push(doc, norm, positions);
                }
                let rectangles = coalesce(members.iter().flat_map(|index| visible[*index].iter().copied()).collect());
                let (class, section) = lifespan(&rectangles, self.ceiling);
                self.sections.entry(class).and_modify(|current| current.widen(&section)).or_insert(section);
                self.runs.push(iid, entity.identity.as_ref(), length, class, rectangles)?;
                column.runs += 1;
                column.tokens += u64::from(length);
                for index in members { column.row_runs[entity.rows[index].0 as usize] = doc; }
            }
        }
        Ok(())
    }

    fn finish(self, columns: &[String], version: Option<i64>, identity: &str, output: impl Write) -> Result<Vec<Section>> {
        if self.members.is_empty() { return Err(fault("A search file holds at least one member")); }
        let sections: Vec<_> = self.sections.into_values().collect();
        let mut writer = Writer::new(output)?;
        let header = Table::Header.array(vec![
            Arc::new(Int32Array::from(vec![FORMAT_VERSION])),
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
        writer.page([
            (Table::Header, header), (Table::Columns, column_table), (Table::Members, members),
            (Table::Pages, pages), (Table::Events, self.events.finish()?), (Table::Runs, self.runs.finish()?),
            (Table::RowRuns, row_runs), (Table::Sections, sections_array(&sections)?), (Table::Terms, terms),
        ])?;
        let mut postings = postings.into_iter().flat_map(BTreeMap::into_values).peekable();
        while postings.peek().is_some() {
            let page = postings.by_ref().take(POSTING_TERMS).collect();
            writer.page([(Table::Postings, posting_table(page, STORED_IMPACTS)?)])?;
        }
        writer.finish()?;
        Ok(sections)
    }
}

#[derive(Default)]
struct EntityColumn {
    row_tokens: Vec<Option<usize>>,
    tokens: Vec<StringArray>,
    distinct: HashMap<u64, Vec<usize>>,
}

struct Entity {
    iid: Option<Iid>,
    identity: Option<EntityId>,
    rows: Vec<(u32, EventHeader)>,
    columns: Vec<EntityColumn>,
}

impl Entity {
    fn new(columns: usize) -> Self {
        Self { iid: None, identity: None, rows: Vec::new(), columns: (0..columns).map(|_| EntityColumn::default()).collect() }
    }

    fn push(&mut self, ordinal: u32, row: IndexedRow<'_>) -> Result<()> {
        let header = row.header;
        self.iid = Some(header.iid);
        if let Some(id) = row.identity {
            if let Some(previous) = &self.identity {
                if EntityIdRef::from(previous) != id {
                    return Err(fault("Owned search input has conflicting complete identities"));
                }
            } else {
                let id = id.into_owned();
                if !id.owned_locator_candidates().any(|candidate| candidate == header.iid) {
                    return Err(fault("Owned search input has conflicting complete identities"));
                }
                self.identity = Some(id);
            }
        }
        self.rows.push((ordinal, header));
        for (column, value) in self.columns.iter_mut().zip(row.values) {
            column.row_tokens.push(value.map(|tokens| {
                let mut hash = DefaultHasher::new();
                tokens.iter().for_each(|token| token.hash(&mut hash));
                let positions = column.distinct.entry(hash.finish()).or_default();
                if let Some(&position) = positions.iter().find(|&&position| column.tokens[position] == tokens) {
                    return position;
                }
                let position = column.tokens.len();
                positions.push(position);
                column.tokens.push(tokens);
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
    ceiling: Option<i64>, output: impl Write, reservation: Option<Arc<MemoryReservation>>,
) -> Result<Vec<Section>>
where P: IntoIterator<Item = Result<I>>, I: SearchPage,
{
    let mut builder = Builder::new(columns.len(), ceiling, reservation);
    let mut previous_member_iid = None;
    for pages in members {
        builder.member()?;
        let mut entity = Entity::new(columns.len());
        for page in pages {
            let page = page?;
            builder.pages.push(builder.rows);
            let tokens = page.tokenize(columns)?;
            for index in 0..page.len() {
                let row = page.row(index, &tokens)?;
                let iid = row.header.iid;
                if entity.iid.is_none() && previous_member_iid.is_some_and(|previous| previous >= iid) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search file members do not hold disjoint IID ranges in order"));
                }
                if entity.iid.is_some_and(|previous| previous > iid) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search rows are not in IID order"));
                }
                if entity.iid.is_some_and(|previous| previous != iid) {
                    builder.entity(std::mem::replace(&mut entity, Entity::new(columns.len())))?;
                }
                if entity.rows.last().is_some_and(|(_, last)| last.system_from < row.header.system_from) {
                    return Err(Anomaly::unsupported("rtdb.search/input-order", "Search rows are not in system-time-descending order"));
                }
                builder.put(&mut entity, row)?;
            }
        }
        previous_member_iid = entity.iid.or(previous_member_iid);
        builder.entity(entity)?;
    }
    builder.finish(columns, version, identity, output)
}
