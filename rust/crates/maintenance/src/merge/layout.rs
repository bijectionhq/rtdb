use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::{EventBatch, PageEvent}, ipc::{ArrowFile, ArrowFileWriter}, metadata::MetadataBuilder};
use rtdb_types::{Iid, LogicalType, Result, RunEndWidth, Timestamp};
use tokio_util::sync::CancellationToken;
use super::{MergeConfig, MergeFile, OutputSchema, check_disk, create_file, disk_remaining, limit_writer, open_input, read_page, row_bytes};
use super::planner::bucket;

#[derive(Clone, Debug)]
pub(super) struct Leaf {
    pub page: usize,
    pub path: Vec<u8>,
    pub rows: usize,
    pub min_recency: Option<Timestamp>,
    pub max_recency: Option<Timestamp>,
}

pub fn cell_boundary(recency: Timestamp, width: i64) -> Result<Timestamp> {
    if width <= 0 { return Err(crate::invalid("Recency cell width must be positive")); }
    let quotient = i128::from(recency.0).div_euclid(i128::from(width)) + i128::from(recency.0.rem_euclid(width) != 0);
    Ok(Timestamp(i64::try_from(quotient * i128::from(width)).map_err(crate::invalid)?))
}

pub fn cell_width(rows: usize, minimum: Timestamp, maximum: Timestamp, page_size: usize) -> Result<Option<i64>> {
    for width in [6 * 3_600_000_000, 24 * 3_600_000_000] {
        let count = i128::from(cell_boundary(maximum, width)?.0 / width) - i128::from(cell_boundary(minimum, width)?.0 / width) + 1;
        if rows as i128 >= 4 * page_size as i128 * count { return Ok(Some(width)); }
    }
    Ok(None)
}

struct FinalWriter<'a> {
    data: ArrowFileWriter,
    metadata: MetadataBuilder,
    schema: OutputSchema,
    arrow_schema: arrow_schema::SchemaRef,
    memory: &'a Arc<MemoryBudget>,
    metadata_charges: Vec<Arc<MemoryReservation>>,
    cancellation: &'a CancellationToken,
    directory: &'a Path,
    disk_limit: u64,
}

impl FinalWriter<'_> {
    fn leaf(&mut self, events: &[PageEvent]) -> Result<usize> {
        if self.cancellation.is_cancelled() { return Err(crate::closed()); }
        let batch = self.schema.batch(events, self.memory)?;
        self.page(batch.batch())
    }

    fn page(&mut self, batch: &arrow_array::RecordBatch) -> Result<usize> {
        if self.cancellation.is_cancelled() { return Err(crate::closed()); }
        let (leaf, metadata_charge) = self.metadata.write_leaf_reserved(batch, self.memory)?;
        limit_writer(&mut self.data, self.directory, self.disk_limit)?;
        self.data.write_event_page(batch, self.memory)?;
        self.metadata_charges.push(metadata_charge);
        Ok(leaf)
    }

    fn rows(&mut self, events: &[PageEvent], depth: usize, page_size: usize) -> Result<usize> {
        if events.is_empty() { return self.metadata.write_nil(); }
        if events.len() <= page_size || depth == 64 || events.first().map(|event| event.iid) == events.last().map(|event| event.iid) { return self.leaf(events); }
        let mut children = [None; 4];
        let mut start = 0;
        for (part, child) in children.iter_mut().enumerate() {
            let end = start + events[start..].partition_point(|event| usize::from(bucket(event.iid, depth)) <= part);
            if start < end { *child = Some(self.rows(&events[start..end], depth + 1, page_size)?); }
            start = end;
        }
        self.metadata.write_iid_branch(children)
    }

    fn tree(&mut self, reader: &mut ArrowFile, leaves: &[Leaf], depth: usize, page_size: usize) -> Result<usize> {
        if leaves.is_empty() { return self.metadata.write_nil(); }
        let total = leaves.iter().try_fold(0usize, |total, leaf| total.checked_add(leaf.rows).ok_or_else(|| crate::invalid("Page tree row count overflow")))?;
        if let [leaf] = leaves && leaf.rows > 0 && leaf.rows <= page_size {
            // Decoding can mark a struct's union field nullable when one of its legs can be null.
            let page = read_page(reader, leaf.page, self.memory)?;
            if page.batch().num_rows() != leaf.rows { return Err(crate::invalid("Spill page row count differs from its page tree")); }
            if page.batch().schema() == self.arrow_schema { return self.page(page.batch()); }
        }
        if (leaves.len() == 1 && leaves[0].path.len() == depth) || total <= page_size {
            let mut rows = Vec::new();
            let mut charges = Vec::new();
            for leaf in leaves {
                let (mut events, charge) = read_events(reader, leaf, self.memory)?;
                rows.append(&mut events);
                charges.push(charge);
            }
            return self.rows(&rows, depth, page_size);
        }
        if depth >= 64 { return Err(crate::invalid("Page tree exceeds IID width")); }
        let mut children = [None; 4];
        for (part, child) in children.iter_mut().enumerate() {
            let matching: Vec<_> = leaves.iter().filter(|leaf| leaf.path.get(depth) == Some(&(part as u8))).cloned().collect();
            if !matching.is_empty() { *child = Some(self.tree(reader, &matching, depth + 1, page_size)?); }
        }
        self.metadata.write_iid_branch(children)
    }
}

fn read_events(reader: &mut ArrowFile, leaf: &Leaf, memory: &Arc<MemoryBudget>) -> Result<(Vec<PageEvent>, Arc<MemoryReservation>)> {
    let batch = read_page(reader, leaf.page, memory)?;
    let charge = memory.reserve(super::decoded_bound(batch.batch())?)?;
    let events = EventBatch::new(batch.batch().clone())?.page_events()?;
    if events.len() != leaf.rows { return Err(crate::invalid("Spill page row count differs from its page tree")); }
    Ok((events, charge))
}

struct Level { depth: usize, current: usize, children: [Option<usize>; 4] }
enum Open { Rows, Entity(Vec<(Timestamp, usize)>) }
struct StreamingTrie {
    spine: Vec<Level>,
    open: Open,
    rows: Vec<PageEvent>,
    charges: Vec<Arc<MemoryReservation>>,
    start_depth: usize,
    page_size: usize,
}

impl StreamingTrie {
    fn new(start_depth: usize, page_size: usize) -> Self { Self { spine: Vec::new(), open: Open::Rows, rows: Vec::new(), charges: Vec::new(), start_depth, page_size } }
    fn next_depth(&self) -> usize { self.spine.last().map_or(self.start_depth, |level| level.depth + 1) }
    fn discard_head(&mut self, count: usize) { self.rows.drain(..count); self.charges.drain(..count); }

    fn entity_page(&mut self, count: usize, output: &mut FinalWriter<'_>) -> Result<()> {
        let recent = self.rows[..count].iter().filter_map(|event| event.recency).max().ok_or_else(|| crate::invalid("Entity recency page has no recency"))?;
        let index = output.leaf(&self.rows[..count])?;
        let Open::Entity(pages) = &mut self.open else { return Err(crate::invalid("Entity page has no entity state")); };
        pages.push((recent, index));
        self.discard_head(count);
        Ok(())
    }

    fn close_open(&mut self, output: &mut FinalWriter<'_>) -> Result<usize> {
        if matches!(self.open, Open::Entity(_)) {
            self.entity_page(self.rows.len(), output)?;
            let Open::Entity(mut pages) = std::mem::replace(&mut self.open, Open::Rows) else { return Err(crate::invalid("Entity state disappeared")); };
            pages.sort_by_key(|page| page.0);
            output.metadata.write_recency_branch(pages)
        } else {
            let index = output.leaf(&self.rows)?;
            self.rows.clear(); self.charges.clear();
            Ok(index)
        }
    }

    fn pop_to(&mut self, length: usize, output: &mut FinalWriter<'_>) -> Result<()> {
        while self.spine.len() > length {
            let level = self.spine.pop().ok_or_else(|| crate::invalid("Missing IID trie level"))?;
            let node = output.metadata.write_iid_branch(level.children)?;
            let parent = self.spine.last_mut().ok_or_else(|| crate::invalid("IID trie parent is missing"))?;
            parent.children[parent.current] = Some(node);
        }
        Ok(())
    }

    fn split(&mut self, output: &mut FinalWriter<'_>) -> Result<()> {
        if matches!(self.open, Open::Entity(_)) { return self.entity_page(self.page_size, output); }
        let first = self.rows[0].iid;
        let last = self.rows.last().ok_or_else(|| crate::invalid("Cannot split empty IID page"))?.iid;
        if first == last {
            self.open = Open::Entity(Vec::new());
            return self.entity_page(self.page_size, output);
        }
        let depth = self.next_depth();
        if depth >= 64 { return Err(crate::invalid("Distinct IIDs exceed trie depth")); }
        let mut level = Level { depth, current: usize::from(bucket(last, depth)), children: [None; 4] };
        let mut start = 0;
        while usize::from(bucket(self.rows[start].iid, depth)) != level.current {
            let part = bucket(self.rows[start].iid, depth);
            let end = start + self.rows[start..].partition_point(|event| bucket(event.iid, depth) == part);
            level.children[usize::from(part)] = Some(output.leaf(&self.rows[start..end])?);
            start = end;
        }
        self.spine.push(level);
        self.discard_head(start);
        Ok(())
    }

    fn separate_entity(&mut self, iid: Iid, output: &mut FinalWriter<'_>) -> Result<()> {
        let previous = self.rows[0].iid;
        let mut depth = self.next_depth();
        while depth < 64 && bucket(previous, depth) == bucket(iid, depth) {
            self.spine.push(Level { depth, current: usize::from(bucket(iid, depth)), children: [None; 4] });
            depth += 1;
        }
        if depth == 64 { return Err(crate::invalid("Cannot separate equal IIDs")); }
        let mut level = Level { depth, current: usize::from(bucket(iid, depth)), children: [None; 4] };
        level.children[usize::from(bucket(previous, depth))] = Some(self.close_open(output)?);
        self.spine.push(level);
        Ok(())
    }

    fn add(&mut self, event: PageEvent, output: &mut FinalWriter<'_>) -> Result<()> {
        if let Some(index) = self.spine.iter().position(|level| usize::from(bucket(event.iid, level.depth)) != level.current) {
            let child = self.close_open(output)?;
            let level = self.spine.last_mut().ok_or_else(|| crate::invalid("Missing IID trie level"))?;
            level.children[level.current] = Some(child);
            self.pop_to(index + 1, output)?;
            self.spine[index].current = usize::from(bucket(event.iid, self.spine[index].depth));
        } else if matches!(self.open, Open::Entity(_)) && self.rows.first().is_some_and(|row| row.iid != event.iid) {
            self.separate_entity(event.iid, output)?;
        }
        self.charges.push(output.memory.reserve(row_bytes(&event)?.checked_mul(2).ok_or_else(|| crate::invalid("Streaming row allocation overflow"))?)?);
        self.rows.push(event);
        while self.rows.len() > self.page_size { self.split(output)?; }
        Ok(())
    }

    fn finish(mut self, output: &mut FinalWriter<'_>) -> Result<usize> {
        if self.rows.is_empty() && self.spine.is_empty() { return output.metadata.write_nil(); }
        let root = self.close_open(output)?;
        if self.spine.is_empty() { return Ok(root); }
        let level = self.spine.last_mut().ok_or_else(|| crate::invalid("Missing IID trie level"))?;
        level.children[level.current] = Some(root);
        self.pop_to(1, output)?;
        output.metadata.write_iid_branch(self.spine[0].children)
    }
}

pub(super) fn normalize(input: &MergeFile, put: &Option<StructFields<LogicalType>>, config: &MergeConfig, memory: &Arc<MemoryBudget>,
    data_path: &Path, meta_path: &Path, cancellation: &CancellationToken) -> Result<(u64, rtdb_format::log::proto::TrieMetadata)> {
    let (mut reader, _reader_charge) = open_input(File::open(&input.path).map_err(crate::invalid)?, memory)?;
    let schema = OutputSchema { put: put.clone(), iid_encoding: (config.run_end_encoded_iids && input.recency.is_some()).then_some(RunEndWidth::I32), recency: input.carries_recency, keyed: input.keyed, include_origins: input.include_origins };
    let directory = data_path.parent().ok_or_else(|| crate::invalid("Output path has no parent"))?;
    let arrow_schema = schema.schema()?;
    let mut output = FinalWriter { data: ArrowFileWriter::new_bounded_reserved(create_file(data_path)?, &arrow_schema, disk_remaining(directory, config.temporary_byte_limit)?, memory)?, metadata: MetadataBuilder::new(input.carries_recency, true), schema, arrow_schema, memory, metadata_charges: Vec::new(), cancellation, directory, disk_limit: config.temporary_byte_limit };
    if !input.carries_recency {
        output.tree(&mut reader, &input.leaves, 0, config.page_size)?;
    } else if input.leaves.is_empty() { output.metadata.write_nil()?; }
    else {
        let minimum = input.leaves.iter().filter_map(|leaf| leaf.min_recency).min().ok_or_else(|| crate::invalid("Historical spill has no recency range"))?;
        let maximum = input.leaves.iter().filter_map(|leaf| leaf.max_recency).max().ok_or_else(|| crate::invalid("Historical spill has no recency range"))?;
        let rows = input.leaves.iter().try_fold(0usize, |rows, leaf| rows.checked_add(leaf.rows).ok_or_else(|| crate::invalid("Historical row count overflow")))?;
        let width = cell_width(rows, minimum, maximum, config.page_size)?;
        let part = input.part.as_slice();
        if let Some(width) = width {
            let mut cells: BTreeMap<Timestamp, StreamingTrie> = BTreeMap::new();
            for leaf in &input.leaves {
                let (events, _charge) = read_events(&mut reader, leaf, memory)?;
                for event in events {
                    let boundary = cell_boundary(event.recency.ok_or_else(|| crate::invalid("Historical row has no recency"))?, width)?;
                    cells.entry(boundary).or_insert_with(|| StreamingTrie::new(part.len(), config.page_size)).add(event, &mut output)?;
                }
            }
            let children = cells.into_iter().map(|(boundary, trie)| Ok((boundary, trie.finish(&mut output)?))).collect::<Result<_>>()?;
            let mut child = output.metadata.write_recency_branch(children)?;
            for bucket in part.iter().rev() {
                let mut children = [None; 4]; children[usize::from(*bucket)] = Some(child);
                child = output.metadata.write_iid_branch(children)?;
            }
        } else {
            let mut trie = StreamingTrie::new(0, config.page_size);
            for leaf in &input.leaves {
                let (events, _charge) = read_events(&mut reader, leaf, memory)?;
                for event in events { trie.add(event, &mut output)?; }
            }
            trie.finish(&mut output)?;
        }
    }
    limit_writer(&mut output.data, directory, config.temporary_byte_limit)?;
    let data = output.data.finish()?;
    let size = data.metadata().map_err(crate::invalid)?.len();
    let _metadata_serialization = memory.reserve(output.metadata.serialization_bound()?)?;
    let (metadata, details) = output.metadata.finish()?;
    let encoded = metadata.write()?;
    check_disk(directory, config.temporary_byte_limit, encoded.len() as u64)?;
    create_file(meta_path)?.write_all(&encoded).map_err(crate::invalid)?;
    Ok((size, details))
}
