use rtdb_format::{data::PageEvent, metadata::MetadataNode};
use rtdb_types::{Iid, Result};
use super::{InputSegment, row_heap};

#[derive(Clone, Copy)]
pub(super) struct Page { pub segment: usize, pub page: usize }
pub(super) struct Task { pub path: Vec<u8>, pub pages: Vec<Page> }

fn expand(segment: usize, index: usize, inputs: &[InputSegment], output: &mut Vec<(usize, usize)>) {
    match &inputs[segment].metadata.nodes()[index] {
        MetadataNode::Nil => (),
        MetadataNode::RecencyBranch(children) => for (_, child) in children { expand(segment, *child, inputs, output); },
        _ => output.push((segment, index)),
    }
}

pub(super) fn matches_path(iid: Iid, path: &[u8]) -> bool { path.iter().enumerate().all(|(depth, expected)| bucket(iid, depth) == *expected) }

pub(super) fn bucket(iid: Iid, depth: usize) -> u8 {
    rtdb_trie::base4_bucket(&iid.0, depth).expect("the compaction path fits in the IID")
}

pub(super) fn plan(inputs: &[InputSegment], part: &[u8]) -> Result<Vec<Task>> {
    if part.len() > 64 || part.iter().any(|bucket| *bucket > 3) { return Err(crate::invalid("Compaction IID path is invalid")); }
    let mut initial = Vec::new();
    for (segment, input) in inputs.iter().enumerate() {
        if let Some(root) = input.metadata.nodes().len().checked_sub(1) { expand(segment, root, inputs, &mut initial); }
    }
    let mut stack = vec![(Vec::new(), initial)];
    let mut tasks = Vec::new();
    while let Some((path, nodes)) = stack.pop() {
        if nodes.is_empty() || path.iter().zip(part).any(|(left, right)| left != right) { continue; }
        if nodes.iter().any(|(segment, index)| matches!(inputs[*segment].metadata.nodes()[*index], MetadataNode::IidBranch(_))) {
            if path.len() == 64 { return Err(crate::invalid("Metadata trie exceeds IID width")); }
            for bucket in (0..4).rev() {
                let mut children = Vec::new();
                for &(segment, index) in &nodes {
                    match &inputs[segment].metadata.nodes()[index] {
                        MetadataNode::IidBranch(branch) => if let Some(child) = branch.get(bucket).copied().flatten() { expand(segment, child, inputs, &mut children); },
                        _ => children.push((segment, index)),
                    }
                }
                let mut path = path.clone();
                path.push(bucket as u8);
                stack.push((path, children));
            }
        } else {
            let pages = nodes.into_iter().map(|(segment, index)| {
                let MetadataNode::Leaf { data_page_index, .. } = &inputs[segment].metadata.nodes()[index] else { return Err(crate::invalid("Merge task has a non-leaf node")); };
                Ok(Page { segment, page: *data_page_index })
            }).collect::<Result<_>>()?;
            tasks.push(Task { path, pages });
        }
    }
    Ok(tasks)
}

pub(super) struct PageRows {
    pub source_precedence: usize,
    pub events: Vec<PageEvent>,
}

#[derive(Clone, Copy)]
pub(super) enum TieOrder {
    Legacy,
    Scan,
}

pub(super) struct RowHeap<'a> {
    streams: &'a [PageRows],
    heads: Vec<row_heap::Head>,
    tie_order: TieOrder,
}

impl<'a> RowHeap<'a> {
    pub fn new(streams: &'a [PageRows], tie_order: TieOrder) -> Result<Self> {
        let heads = row_heap::from_pages(streams.len(), |stream| streams[stream].events.len(), |head| Self::key(streams, tie_order, head))
            .ok_or_else(|| crate::invalid("Compaction page is not IID ascending and system-position descending"))?;
        Ok(Self { streams, heads, tie_order })
    }

    fn key(streams: &[PageRows], tie_order: TieOrder, (stream, row): row_heap::Head) -> row_heap::Key {
        let page = &streams[stream];
        let event = &page.events[row];
        let (priority, ordinal, leaf) = match tie_order {
            TieOrder::Legacy => (0, 0, 0),
            TieOrder::Scan => (page.source_precedence, event.page_ordinal(), stream),
        };
        (u128::from_be_bytes(event.iid.0), event.system_from.0, event.commit_position, priority, ordinal, leaf)
    }

    pub fn next(&mut self) -> Option<row_heap::Head> {
        let streams = self.streams;
        let tie_order = self.tie_order;
        let (stream, _) = *self.heads.first()?;
        Some(row_heap::take_minimum(&mut self.heads, streams[stream].events.len(), |head| Self::key(streams, tie_order, head)))
    }
}
