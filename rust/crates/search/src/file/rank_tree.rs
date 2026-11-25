use std::{io::Write, mem::size_of, ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, LargeListArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
    builder::FixedSizeBinaryBuilder, cast::AsArray, types::{Float64Type, Int64Type, UInt8Type, UInt32Type, UInt64Type},
};
use futures::{Stream, TryStreamExt};
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
    _reservation: Arc<MemoryReservation>,
}

struct RankColumns {
    blocks: ArrayRef,
    ids: ArrayRef,
}

impl RankColumns {
    fn read(blocks: &LargeListArray, ids: &LargeListArray, row: usize) -> Result<Self> {
        if row >= blocks.len() || row >= ids.len() { return Err(corrupt("Search rank page is incomplete")); }
        let columns = Self { blocks: blocks.value(row), ids: ids.value(row) };
        let offsets = columns.offsets();
        let range = offsets[0] as usize..offsets[offsets.len() - 1] as usize;
        let impacts = columns.impacts();
        let windows = columns.ids.as_struct().column(0).as_primitive::<UInt32Type>().values();
        let scores = impacts.column(5).as_primitive::<Float64Type>().values();
        if range.is_empty() || &impacts.column(0).as_primitive::<UInt32Type>().values()[range.clone()] != windows.as_ref()
            || windows.windows(2).any(|pair| pair[0] >= pair[1])
            || scores[range].iter().any(|score| !score.is_finite() || *score <= 0.0) {
            return Err(corrupt("Search term impacts disagree with their ordering proof"));
        }
        Ok(columns)
    }

    fn offsets(&self) -> &[i64] { self.blocks.as_struct().column(3).as_list::<i64>().value_offsets() }

    fn impacts(&self) -> &StructArray {
        self.blocks.as_struct().column(3).as_list::<i64>().values().as_struct()
    }

    fn window(&self, row: usize) -> RankNode {
        let impact = self.offsets()[0] as usize + row;
        let impacts = self.impacts();
        let ids = self.ids.as_struct();
        let window = ids.column(0).as_primitive::<UInt32Type>().value(row);
        let optional_id = |column| {
            let values = ids.column(column).as_primitive::<Int64Type>();
            values.is_valid(row).then(|| values.value(row))
        };
        RankNode {
            first: window, last: window, height: 0, page: None,
            frequency: impacts.column(1).as_primitive::<UInt32Type>().value(impact),
            length: impacts.column(2).as_primitive::<UInt32Type>().value(impact),
            unit_score: impacts.column(5).as_primitive::<Float64Type>().value(impact),
            minimum: optional_id(1), peak: optional_id(2),
        }
    }

    fn summary(&self, range: Range<usize>) -> RankNode {
        range.map(|row| self.window(row)).reduce(RankNode::combine).expect("a rank leaf contains a window")
    }
}

impl RankLeaf {
    pub fn read(blocks: &LargeListArray, ids: &LargeListArray, row: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let columns = RankColumns::read(blocks, ids, row)?;
        let bytes = 256usize.saturating_add(columns.blocks.len().saturating_mul(size_of::<RankBlock>()))
            .saturating_add(columns.ids.len().saturating_mul(size_of::<RankWindow>()));
        let reservation = memory.reserve(bytes)?;
        let mut blocks = Vec::with_capacity(columns.blocks.len());
        let mut windows = Vec::with_capacity(columns.ids.len());
        let offsets = columns.offsets();
        let base = offsets[0] as usize;
        for (block, offsets) in offsets.windows(2).enumerate() {
            let start = windows.len();
            for impact in offsets[0] as usize..offsets[1] as usize {
                let node = columns.window(impact - base);
                windows.push(RankWindow { window: node.first, block, frequency: node.frequency, length: node.length,
                    unit_score: node.unit_score, minimum: node.minimum, peak: node.peak });
            }
            blocks.push(RankBlock { posting: PostingBlock::at(columns.blocks.as_struct(), block), windows: start..windows.len() });
        }
        Ok(Self { blocks, windows, _reservation: reservation })
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

const TRAVERSAL_NODES: usize = 1 + MAX_HEIGHT as usize * FANOUT;
pub(super) const TRAVERSAL_BYTES: usize = (TRAVERSAL_NODES + FANOUT) * size_of::<RankNode>();

pub(super) fn leaf_stream(reader: &TreeReader, root: RankNode, first: u32, last: u32) -> impl Stream<Item = Result<RankNode>> + '_ {
    let mut nodes = Vec::with_capacity(1 + root.height as usize * FANOUT);
    nodes.push(root);
    futures::stream::try_unfold(nodes, move |mut nodes| async move {
        while let Some(node) = nodes.pop() {
            if node.first > last || node.last < first { continue; }
            if node.height == 0 { return Ok(Some((node, nodes))); }
            nodes.extend(children(reader, node).await?.into_iter().rev());
        }
        Ok(None)
    })
}

pub(super) async fn leaves(reader: &TreeReader, root: RankNode, first: u32, last: u32) -> Result<Vec<RankNode>> {
    leaf_stream(reader, root, first, last).try_collect().await
}

fn slices(rows: &ArrayRef, range: Range<usize>, table: Table) -> Result<StructArray> {
    let length = range.len();
    table.array(vec![list(rows.slice(range.start, length), [length], item_of(&table.fields()[0]))?])
}

pub(super) fn write_term(writer: &mut Writer<impl Write>, blocks: &LargeListArray, ids: &LargeListArray, row: usize) -> Result<RankNode> {
    let columns = RankColumns::read(blocks, ids, row)?;
    let window_count = columns.ids.len();
    if window_count <= INLINE_WINDOWS { return Ok(columns.summary(0..window_count)); }
    let offsets = columns.offsets();
    let base = offsets[0] as usize;
    let mut level = Vec::with_capacity(columns.blocks.len());
    let mut first = 0;
    while first < columns.blocks.len() {
        let first_window = offsets[first] as usize - base;
        let mut end = first + 1;
        while end < columns.blocks.len() && offsets[end + 1] as usize - offsets[first] as usize <= WINDOWS_PER_LEAF {
            end += 1;
        }
        let end_window = offsets[end] as usize - base;
        let node = columns.summary(first_window..end_window);
        let page = writer.page([
            (Table::Blocks, slices(&columns.blocks, first..end, Table::Blocks)?),
            (Table::ImpactIds, slices(&columns.ids, first_window..end_window, Table::ImpactIds)?),
        ])?;
        level.push(RankNode { page: Some(page), ..node });
        first = end;
    }
    while level.len() > 1 {
        let mut parents = Vec::with_capacity(level.len().div_ceil(FANOUT));
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
