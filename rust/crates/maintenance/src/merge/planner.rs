use rtdb_format::{data::PageEvent, metadata::MetadataNode};
use rtdb_types::{Iid, Result};
use super::InputSegment;

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

pub(super) struct RowHeap { heads: Vec<(usize, usize)> }

impl RowHeap {
    pub fn new(streams: &[Vec<PageEvent>]) -> Result<Self> {
        let mut heads = Vec::new();
        for (index, rows) in streams.iter().enumerate() {
            for pair in rows.windows(2) {
                if pair[0].iid > pair[1].iid || (pair[0].iid == pair[1].iid && pair[0].system_from < pair[1].system_from) {
                    return Err(crate::invalid("Compaction page is not IID ascending and system-time descending"));
                }
            }
            if !rows.is_empty() { heads.push((index, 0)); }
        }
        let mut heap = Self { heads };
        for index in (0..heap.heads.len() / 2).rev() { heap.sift(index, streams); }
        Ok(heap)
    }

    fn precedes(left: (usize, usize), right: (usize, usize), streams: &[Vec<PageEvent>]) -> bool {
        let (left, right) = (&streams[left.0][left.1], &streams[right.0][right.1]);
        left.iid < right.iid || (left.iid == right.iid && left.system_from > right.system_from)
    }

    fn sift(&mut self, mut index: usize, streams: &[Vec<PageEvent>]) {
        let node = self.heads[index];
        loop {
            let mut child = 2 * index + 1;
            if child >= self.heads.len() { break; }
            if child + 1 < self.heads.len() && Self::precedes(self.heads[child + 1], self.heads[child], streams) { child += 1; }
            if !Self::precedes(self.heads[child], node, streams) { break; }
            self.heads[index] = self.heads[child];
            index = child;
        }
        self.heads[index] = node;
    }

    pub fn next(&mut self, streams: &[Vec<PageEvent>]) -> Option<(usize, usize)> {
        let result = *self.heads.first()?;
        if result.1 + 1 == streams[result.0].len() {
            let last = self.heads.pop()?;
            if !self.heads.is_empty() { self.heads[0] = last; }
        } else { self.heads[0].1 += 1; }
        if !self.heads.is_empty() { self.sift(0, streams); }
        Some(result)
    }
}
