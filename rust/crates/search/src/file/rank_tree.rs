use std::{io::Write, mem::size_of, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, LargeListArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
    builder::FixedSizeBinaryBuilder, cast::AsArray, types::{Float64Type, Int64Type, UInt8Type, UInt32Type, UInt64Type},
};
use rtdb_types::{MemoryBudget, MemoryReservation, Result};

use super::{
    corrupt, fault,
    layout::{PageRef, Table, Writer, item_of, list, table},
    page_tree::{PageKind, TreeReader},
    paged::PostingBlock,
};

const INLINE_WINDOWS: usize = 32;
const WINDOWS_PER_LEAF: usize = 256;
const FANOUT: usize = 32;
const MAX_HEIGHT: u8 = 8;

#[derive(Clone, Copy, Debug)]
pub(super) struct RankNode {
    pub first: u32,
    pub last: u32,
    pub height: u8,
    pub page: Option<PageRef>,
    pub frequency: u32,
    pub length: u32,
    pub unit_score: f64,
    pub minimum: Option<i64>,
    pub peak: Option<i64>,
}

impl RankNode {
    fn combine(mut self, other: Self) -> Self {
        self.last = other.last;
        self.frequency = self.frequency.max(other.frequency);
        self.length = self.length.min(other.length);
        self.minimum = lesser(self.minimum, other.minimum);
        if other.unit_score > self.unit_score {
            self.unit_score = other.unit_score;
            self.peak = other.peak;
        } else if other.unit_score == self.unit_score {
            self.peak = lesser(self.peak, other.peak);
        }
        self
    }
}

fn lesser(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    left.zip(right).map(|(left, right)| left.min(right))
}

pub(super) fn nodes_array(nodes: &[RankNode]) -> Result<StructArray> {
    let mut digests = FixedSizeBinaryBuilder::with_capacity(nodes.len(), 32);
    for node in nodes {
        digests.append_value(node.page.map_or([0; 32], |page| page.digest)).map_err(fault)?;
    }
    Table::RankNodes.array(vec![
        Arc::new(UInt32Array::from_iter_values(nodes.iter().map(|node| node.first))),
        Arc::new(UInt32Array::from_iter_values(nodes.iter().map(|node| node.last))),
        Arc::new(UInt8Array::from_iter_values(nodes.iter().map(|node| node.height))),
        Arc::new(UInt64Array::from_iter_values(nodes.iter().map(|node| node.page.map_or(0, |page| page.offset)))),
        Arc::new(UInt32Array::from_iter_values(nodes.iter().map(|node| node.page.map_or(0, |page| page.metadata)))),
        Arc::new(UInt64Array::from_iter_values(nodes.iter().map(|node| node.page.map_or(0, |page| page.body)))),
        Arc::new(digests.finish()),
        Arc::new(UInt32Array::from_iter_values(nodes.iter().map(|node| node.frequency))),
        Arc::new(UInt32Array::from_iter_values(nodes.iter().map(|node| node.length))),
        Arc::new(Float64Array::from_iter_values(nodes.iter().map(|node| node.unit_score))),
        Arc::new(Int64Array::from_iter(nodes.iter().map(|node| node.minimum))),
        Arc::new(Int64Array::from_iter(nodes.iter().map(|node| node.peak))),
    ])
}

pub(super) fn node_at(array: &StructArray, row: usize) -> Result<RankNode> {
    if row >= array.len() { return Err(corrupt("Search term has no rank directory")); }
    let uint = |index: usize| array.column(index).as_primitive::<UInt32Type>().value(row);
    let wide = |index: usize| array.column(index).as_primitive::<UInt64Type>().value(row);
    let signed = |index: usize| {
        let column = array.column(index).as_primitive::<Int64Type>();
        column.is_valid(row).then(|| column.value(row))
    };
    let offset = wide(3);
    let digests = array.column(6).as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>().expect("search digest layout");
    let node = RankNode {
        first: uint(0), last: uint(1), height: array.column(2).as_primitive::<UInt8Type>().value(row),
        page: (offset != 0).then(|| PageRef { offset, metadata: uint(4), body: wide(5), digest: digests.value(row).try_into().expect("search digest width") }),
        frequency: uint(7), length: uint(8), unit_score: array.column(9).as_primitive::<Float64Type>().value(row),
        minimum: signed(10), peak: signed(11),
    };
    if node.first > node.last || node.height > MAX_HEIGHT || node.frequency == 0 || node.length == 0
        || !node.unit_score.is_finite() || node.unit_score <= 0.0 || node.page.is_none() && node.height != 0 {
        return Err(corrupt("Search rank directory has invalid bounds"));
    }
    if let Some(page) = node.page { page.range()?; }
    Ok(node)
}

pub(super) struct RankWindow {
    pub window: u32,
    pub block: usize,
    pub frequency: u32,
    pub length: u32,
    pub unit_score: f64,
    pub minimum: Option<i64>,
    pub peak: Option<i64>,
}

pub(super) struct RankBlock {
    pub posting: PostingBlock,
    pub windows: Range<usize>,
}

pub(super) struct RankLeaf {
    pub blocks: Vec<RankBlock>,
    pub windows: Vec<RankWindow>,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl RankLeaf {
    pub fn read(blocks: &LargeListArray, ids: &LargeListArray, row: usize, memory: Option<&Arc<MemoryBudget>>) -> Result<Self> {
        if row >= blocks.len() || row >= ids.len() { return Err(corrupt("Search rank page is incomplete")); }
        let blocks = blocks.value(row);
        let blocks = blocks.as_struct();
        let impacts = blocks.column(3).as_list::<i64>();
        let offsets = impacts.value_offsets();
        let base = offsets[0] as usize;
        let range = base..offsets[blocks.len()] as usize;
        let impacts = impacts.values().as_struct();
        let uint = |column| impacts.column(column).as_primitive::<UInt32Type>().values();
        let scores = impacts.column(5).as_primitive::<Float64Type>().values();
        let ids = ids.value(row);
        let ids = ids.as_struct();
        let window_ids = ids.column(0).as_primitive::<UInt32Type>();
        let minimum = ids.column(1).as_primitive::<Int64Type>();
        let peak = ids.column(2).as_primitive::<Int64Type>();
        if range.is_empty() || &uint(0)[range.clone()] != window_ids.values().as_ref()
            || window_ids.values().windows(2).any(|pair| pair[0] >= pair[1])
            || scores[range.clone()].iter().any(|score| !score.is_finite() || *score <= 0.0) {
            return Err(corrupt("Search term impacts disagree with their ordering proof"));
        }
        let bytes = 256usize.saturating_add(blocks.len().saturating_mul(size_of::<RankBlock>()))
            .saturating_add(range.len().saturating_mul(size_of::<RankWindow>()));
        let reservation = memory.map(|memory| memory.reserve(bytes)).transpose()?;
        let mut block_descriptors = Vec::with_capacity(blocks.len());
        let mut windows = Vec::with_capacity(range.len());
        let frequencies = uint(1);
        let lengths = uint(2);
        for (block, offsets) in offsets.windows(2).enumerate() {
            let start = windows.len();
            for impact in offsets[0] as usize..offsets[1] as usize {
                let row = impact - base;
                windows.push(RankWindow { window: window_ids.value(row), block, frequency: frequencies[impact], length: lengths[impact],
                    unit_score: scores[impact], minimum: minimum.is_valid(row).then(|| minimum.value(row)),
                    peak: peak.is_valid(row).then(|| peak.value(row)) });
            }
            block_descriptors.push(RankBlock { posting: PostingBlock::at(blocks, block), windows: start..windows.len() });
        }
        Ok(Self { blocks: block_descriptors, windows, _reservation: reservation })
    }

    fn summary(&self, range: Range<usize>) -> RankNode {
        self.windows[range].iter().map(|window| RankNode {
            first: window.window, last: window.window, height: 0, page: None,
            frequency: window.frequency, length: window.length, unit_score: window.unit_score,
            minimum: window.minimum, peak: window.peak,
        }).reduce(RankNode::combine).expect("a rank leaf contains a window")
    }
}

pub(super) async fn children(reader: &TreeReader, node: RankNode) -> Result<Vec<RankNode>> {
    let parent = node.page.ok_or_else(|| corrupt("Search rank branch is inline"))?;
    let page = reader.page(parent, PageKind::Rank).await?;
    let entries = table(&page, Table::RankNodes);
    if entries.is_empty() || entries.len() > FANOUT { return Err(corrupt("Search rank branch fanout is invalid")); }
    let mut nodes = Vec::<RankNode>::with_capacity(entries.len());
    for row in 0..entries.len() {
        let child = node_at(&entries, row)?;
        let page = child.page.ok_or_else(|| corrupt("Search rank branch has an inline child"))?;
        if Some(node.height) != child.height.checked_add(1) || child.first < node.first || child.last > node.last
            || page.range()?.end > parent.offset || nodes.last().is_some_and(|last| last.last >= child.first) {
            return Err(corrupt("Search rank branch children are out of order"));
        }
        nodes.push(child);
    }
    Ok(nodes)
}

pub(super) async fn leaf_arrays(reader: &TreeReader, node: RankNode) -> Result<(StructArray, StructArray)> {
    let page = reader.page(node.page.ok_or_else(|| corrupt("Search rank leaf is inline"))?, PageKind::Rank).await?;
    Ok((table(&page, Table::Blocks), table(&page, Table::ImpactIds)))
}

pub(super) async fn leaves(reader: &TreeReader, root: RankNode, first: u32, last: u32) -> Result<Vec<RankNode>> {
    let mut nodes = vec![root];
    let mut leaves = Vec::new();
    while let Some(node) = nodes.pop() {
        if node.first > last || node.last < first { continue; }
        if node.height == 0 { leaves.push(node); }
        else { nodes.extend(children(reader, node).await?.into_iter().rev()); }
    }
    Ok(leaves)
}

fn slices(rows: &ArrayRef, range: Range<usize>, table: Table) -> Result<StructArray> {
    let length = range.len();
    table.array(vec![list(rows.slice(range.start, length), [length], item_of(&table.fields()[0]))?])
}

pub(super) fn write_term(writer: &mut Writer<impl Write>, blocks: &LargeListArray, ids: &LargeListArray, row: usize) -> Result<RankNode> {
    let decoded = RankLeaf::read(blocks, ids, row, None)?;
    let window_count = decoded.windows.len();
    if window_count <= INLINE_WINDOWS { return Ok(decoded.summary(0..window_count)); }
    let blocks = blocks.value(row);
    let ids = ids.value(row);
    let mut level = Vec::new();
    let mut remaining = decoded.blocks.iter().enumerate().peekable();
    let mut first_window = 0;
    while let Some((first_block, block)) = remaining.next() {
        let mut window_count = block.windows.len();
        let mut last_block = first_block;
        while let Some((_, next)) = remaining.peek() {
            if window_count + next.windows.len() > WINDOWS_PER_LEAF { break; }
            let (index, next) = remaining.next().expect("peeked rank block");
            last_block = index;
            window_count += next.windows.len();
        }
        let end_window = first_window + window_count;
        let node = decoded.summary(first_window..end_window);
        let page = writer.page([
            (Table::Blocks, slices(&blocks, first_block..last_block + 1, Table::Blocks)?),
            (Table::ImpactIds, slices(&ids, first_window..end_window, Table::ImpactIds)?),
        ])?;
        level.push(RankNode { page: Some(page), ..node });
        first_window = end_window;
    }
    while level.len() > 1 {
        let mut parents = Vec::new();
        for nodes in level.chunks(FANOUT) {
            let mut parent = nodes.iter().copied().reduce(RankNode::combine).expect("a rank branch has children");
            parent.height += 1;
            if parent.height > MAX_HEIGHT { return Err(fault("Search rank directory exceeds its maximum depth")); }
            parent.page = Some(writer.page([(Table::RankNodes, nodes_array(nodes)?)])?);
            parents.push(parent);
        }
        level = parents;
    }
    Ok(level[0])
}

pub(super) fn inline_rows(array: &LargeListArray, nodes: &[RankNode], start: usize, shape: Table) -> Result<StructArray> {
    let item = item_of(&shape.fields()[0]);
    let empty = arrow_array::new_empty_array(item.data_type());
    let values: Vec<_> = nodes.iter().enumerate().map(|(row, node)| {
        if node.page.is_some() { Arc::clone(&empty) } else { array.value(start + row) }
    }).collect();
    let references: Vec<_> = values.iter().map(|value| value.as_ref()).collect();
    let lengths = values.iter().map(|value| value.len());
    shape.array(vec![list(arrow_select::concat::concat(&references).map_err(fault)?, lengths, item)?])
}

pub(super) async fn materialize_blocks(reader: &TreeReader, term_pages: &[arrow_array::RecordBatch]) -> Result<StructArray> {
    let shape = Table::Blocks;
    let item = item_of(&shape.fields()[0]);
    let mut values = Vec::new();
    let mut counts = Vec::new();
    for page in term_pages {
        let inline = table(page, shape);
        let nodes = table(page, Table::RankNodes);
        let blocks = inline.column(0).as_list::<i64>();
        for row in 0..nodes.len() {
            let node = node_at(&nodes, row)?;
            let start = values.len();
            if node.page.is_some() {
                for leaf in leaves(reader, node, node.first, node.last).await? {
                    let (blocks, _) = leaf_arrays(reader, leaf).await?;
                    values.push(blocks.column(0).as_list::<i64>().value(0));
                }
            } else {
                values.push(blocks.value(row));
            }
            counts.push(values[start..].iter().map(|value| value.len()).sum());
        }
    }
    let references: Vec<_> = values.iter().map(|value| value.as_ref()).collect();
    let values = if references.is_empty() { arrow_array::new_empty_array(item.data_type()) }
        else { arrow_select::concat::concat(&references).map_err(fault)? };
    shape.array(vec![list(values, counts, item)?])
}
