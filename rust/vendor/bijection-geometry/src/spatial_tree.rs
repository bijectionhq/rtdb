//! A packed STR tree for bounded geometry admission and query refinement.
use crate::{
    boxes_meet,
    GeometryRefusal,
    GeometryWork,
};
struct Node {
    bbox: [f64; 4],
    item: Option<usize>,
    children: Vec<usize>,
}
pub struct PackedBoxes {
    nodes: Vec<Node>,
    root: Option<usize>,
}
impl PackedBoxes {
    pub fn new(boxes: &[[f64; 4]], work: &mut GeometryWork) -> Result<Self, GeometryRefusal> {
        let mut level_count = boxes.len();
        let mut node_count = level_count;
        while level_count > 1 {
            level_count = level_count.div_ceil(16);
            node_count = node_count.saturating_add(level_count);
        }
        // Reserve every node once, its child references, and conservative
        // scratch for both level vectors and stable sorting. This accounts
        // for the actual packed layout rather than an oversized per-leaf cap.
        let node_bytes = node_count.saturating_mul(std::mem::size_of::<Node>());
        let child_bytes = node_count.saturating_mul(std::mem::size_of::<usize>());
        let scratch_bytes = boxes.len().saturating_mul(4 * std::mem::size_of::<usize>());
        work.allocate(
            node_bytes
                .saturating_add(child_bytes)
                .saturating_add(scratch_bytes)
                .saturating_add(256),
        )?;
        work.charge(boxes.len())?;
        let mut nodes = Vec::with_capacity(node_count);
        nodes.extend(boxes.iter().enumerate().map(|(i, bbox)| Node {
            bbox: *bbox,
            item: Some(i),
            children: Vec::new(),
        }));
        let mut level: Vec<usize> = (0..nodes.len()).collect();
        while level.len() > 1 {
            let per = (((level.len() as f64 / 16.0).ceil().sqrt().ceil() as usize) * 16).max(16);
            work.charge(level.len() * (level.len().max(2).ilog2() as usize + 1))?;
            level.sort_by(|&a, &b| {
                (nodes[a].bbox[0] + nodes[a].bbox[2])
                    .total_cmp(&(nodes[b].bbox[0] + nodes[b].bbox[2]))
            });
            let mut next = Vec::new();
            for tile in level.chunks_mut(per) {
                work.charge(tile.len() * (tile.len().max(2).ilog2() as usize + 1))?;
                tile.sort_by(|&a, &b| {
                    (nodes[a].bbox[1] + nodes[a].bbox[3])
                        .total_cmp(&(nodes[b].bbox[1] + nodes[b].bbox[3]))
                });
                for children in tile.chunks(16) {
                    let mut bbox = [
                        f64::INFINITY,
                        f64::INFINITY,
                        f64::NEG_INFINITY,
                        f64::NEG_INFINITY,
                    ];
                    for &child in children {
                        let b = nodes[child].bbox;
                        bbox[0] = bbox[0].min(b[0]);
                        bbox[1] = bbox[1].min(b[1]);
                        bbox[2] = bbox[2].max(b[2]);
                        bbox[3] = bbox[3].max(b[3]);
                    }
                    next.push(nodes.len());
                    nodes.push(Node {
                        bbox,
                        item: None,
                        children: children.to_vec(),
                    });
                }
            }
            level = next;
        }
        Ok(Self {
            root: level.first().copied(),
            nodes,
        })
    }

    /// Best-first refinement of leaf pairs under a caller-supplied conservative
    /// distance bound. Only the frontier is retained, never a pair matrix.
    pub fn closest_pairs<F>(
        &self,
        other: &Self,
        mut best: f64,
        lower_bound: impl Fn([f64; 4], [f64; 4]) -> f64,
        threshold: impl Fn(f64) -> f64,
        mut refine: F,
        work: &mut GeometryWork,
    ) -> Result<f64, GeometryRefusal>
    where
        F: FnMut(usize, usize, f64, &mut GeometryWork) -> Result<f64, GeometryRefusal>,
    {
        use std::{
            cmp::Ordering,
            collections::BinaryHeap,
        };
        struct Candidate {
            lower: f64,
            left: usize,
            right: usize,
        }
        impl PartialEq for Candidate {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == Ordering::Equal
            }
        }
        impl Eq for Candidate {}
        impl PartialOrd for Candidate {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Candidate {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .lower
                    .total_cmp(&self.lower)
                    .then_with(|| other.left.cmp(&self.left))
                    .then_with(|| other.right.cmp(&self.right))
            }
        }
        let (Some(left), Some(right)) = (self.root, other.root) else {
            return Ok(best);
        };
        work.scoped(|work| {
            let mut pending = BinaryHeap::new();
            let enqueue = |left: usize,
                           right: usize,
                           best: f64,
                           pending: &mut BinaryHeap<Candidate>,
                           work: &mut GeometryWork|
             -> Result<(), GeometryRefusal> {
                work.charge(1)?;
                let lower = lower_bound(self.nodes[left].bbox, other.nodes[right].bbox);
                if lower < threshold(best) {
                    work.allocate(64)?;
                    pending.push(Candidate { lower, left, right });
                }
                Ok(())
            };
            enqueue(left, right, best, &mut pending, work)?;
            while let Some(Candidate { lower, left, right }) = pending.pop() {
                work.release(64);
                work.charge(1)?;
                if lower >= threshold(best) {
                    break;
                }
                let (a, b) = (&self.nodes[left], &other.nodes[right]);
                if let (Some(left), Some(right)) = (a.item, b.item) {
                    best = refine(left, right, best, work)?;
                } else {
                    let width = |node: &Node| {
                        (node.bbox[2] - node.bbox[0]).max(node.bbox[3] - node.bbox[1])
                    };
                    if a.item.is_none() && (b.item.is_some() || width(a) >= width(b)) {
                        for &left in &a.children {
                            enqueue(left, right, best, &mut pending, work)?;
                        }
                    } else {
                        for &right in &b.children {
                            enqueue(left, right, best, &mut pending, work)?;
                        }
                    }
                }
            }
            Ok(best)
        })
    }

    /// Derive a parallel immutable aggregate for every packed node. Children
    /// precede parents, so exact integer time ranges need no second tree.
    pub fn aggregates<T: Copy>(
        &self,
        items: &[T],
        merge: impl Fn(T, T) -> T,
        work: &mut GeometryWork,
    ) -> Result<Vec<T>, GeometryRefusal> {
        work.allocate(self.nodes.len().saturating_mul(std::mem::size_of::<T>()))?;
        work.charge(self.nodes.len().saturating_mul(2))?;
        let mut values: Vec<T> = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let value = if let Some(item) = node.item {
                items
                    .get(item)
                    .copied()
                    .ok_or_else(|| crate::invalid("Packed metadata differs from its leaves"))?
            } else {
                node.children
                    .iter()
                    .map(|&child| values[child])
                    .reduce(&merge)
                    .ok_or_else(|| crate::invalid("An internal packed node needs children"))?
            };
            values.push(value);
        }
        Ok(values)
    }

    /// Every intersecting unordered leaf pair, once. A dual-tree traversal
    /// avoids repeating a root-to-leaf search for each input edge and streams
    /// refinement through the caller's existing work account.
    pub fn pairs(
        &self,
        allow_nodes: &mut dyn FnMut(usize, usize) -> bool,
        visit: &mut dyn FnMut(usize, usize, &mut GeometryWork) -> Result<bool, GeometryRefusal>,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let Some(root) = self.root else {
            return Ok(false);
        };
        self.pair_nodes(root, root, allow_nodes, visit, work)
    }

    fn pair_nodes(
        &self,
        left: usize,
        right: usize,
        allow_nodes: &mut dyn FnMut(usize, usize) -> bool,
        visit: &mut dyn FnMut(usize, usize, &mut GeometryWork) -> Result<bool, GeometryRefusal>,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        work.charge(1)?;
        let (a, b) = (&self.nodes[left], &self.nodes[right]);
        if !boxes_meet(&a.bbox, &b.bbox) || !allow_nodes(left, right) {
            return Ok(false);
        }
        if let (Some(i), Some(j)) = (a.item, b.item) {
            return if i == j {
                Ok(false)
            } else {
                visit(i.min(j), i.max(j), work)
            };
        }
        if left == right {
            for (i, &child) in a.children.iter().enumerate() {
                for &other in &a.children[i..] {
                    if self.pair_nodes(child, other, allow_nodes, visit, work)? {
                        return Ok(true);
                    }
                }
            }
        } else {
            let width =
                |node: &Node| (node.bbox[2] - node.bbox[0]).max(node.bbox[3] - node.bbox[1]);
            if a.item.is_none() && (b.item.is_some() || width(a) >= width(b)) {
                for &child in &a.children {
                    if self.pair_nodes(child, right, allow_nodes, visit, work)? {
                        return Ok(true);
                    }
                }
            } else {
                for &child in &b.children {
                    if self.pair_nodes(left, child, allow_nodes, visit, work)? {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn search(
        &self,
        bbox: &[f64; 4],
        visit: &mut dyn FnMut(usize) -> bool,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        self.search_filtered(bbox, &mut |_| true, visit, work)
    }

    /// The additional node predicate can prune using parallel native metadata,
    /// without changing the tree or its existing spatial search semantics.
    pub fn search_filtered(
        &self,
        bbox: &[f64; 4],
        allow_node: &mut dyn FnMut(usize) -> bool,
        visit: &mut dyn FnMut(usize) -> bool,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let Some(root) = self.root else {
            return Ok(false);
        };
        self.search_node(root, bbox, allow_node, visit, work)
    }

    fn search_node(
        &self,
        id: usize,
        bbox: &[f64; 4],
        allow_node: &mut dyn FnMut(usize) -> bool,
        visit: &mut dyn FnMut(usize) -> bool,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        work.charge(1)?;
        let node = &self.nodes[id];
        if !allow_node(id) || !boxes_meet(&node.bbox, bbox) {
            return Ok(false);
        }
        if let Some(item) = node.item {
            return Ok(visit(item));
        }
        for &child in &node.children {
            if self.search_node(child, bbox, allow_node, visit, work)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Packed candidate edges, shared by admission, covers and metric refinement.
/// A WGS84 edge's box holds its whole great elliptic arc, bulge included,
/// and may pass 180 across the seam.
pub struct PackedEdges {
    pub edges: Vec<(crate::Pos, crate::Pos)>,
    pub(crate) tree: PackedBoxes,
    frame: crate::CoordinateFrame,
    /// Whether some WGS84 edge box passes 180: it holds the longitudes just
    /// west of the seam only one turn east of them.
    past_seam: bool,
}
impl PackedEdges {
    pub fn new(
        edges: Vec<(crate::Pos, crate::Pos)>,
        frame: &crate::CoordinateFrame,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        work.allocate(edges.len().saturating_mul(64))?;
        let boxes: Vec<_> = edges
            .iter()
            .map(|(p, q)| crate::arcs::edge_bounds(frame, *p, *q))
            .collect();
        let tree = PackedBoxes::new(&boxes, work)?;
        let past_seam =
            frame.is_geographic() && boxes.iter().any(|b| b[2] > 180.0);
        Ok(Self {
            edges,
            tree,
            frame: frame.clone(),
            past_seam,
        })
    }

    pub fn candidates(
        &self,
        a: crate::Pos,
        b: crate::Pos,
        work: &mut GeometryWork,
    ) -> Result<Vec<(crate::Pos, crate::Pos)>, GeometryRefusal> {
        self.in_box(crate::arcs::edge_bounds(&self.frame, a, b), work)
    }

    /// Edges whose boxes meet a box, east past 180 included, on either
    /// side.
    pub(crate) fn in_box(
        &self,
        bbox: [f64; 4],
        work: &mut GeometryWork,
    ) -> Result<Vec<(crate::Pos, crate::Pos)>, GeometryRefusal> {
        let geographic = self.frame.is_geographic();
        let mut queries = crate::arcs::turns(geographic, bbox);
        // The query's own images cover a query that passes the seam; an
        // edge box that passes 180 meets a query west of the seam only at
        // the query's image one turn east.
        let east = [bbox[0] + 360.0, bbox[1], bbox[2] + 360.0, bbox[3]];
        if self.past_seam && bbox[2] - bbox[0] < 360.0 && !queries.contains(&east) {
            queries.push(east);
        }
        let mut found = Vec::new();
        for query in queries {
            let mut count = 0;
            self.tree.search(
                &query,
                &mut |_| {
                    count += 1;
                    false
                },
                work,
            )?;
            work.allocate(count * 8)?;
            found.reserve(count);
            self.tree.search(
                &query,
                &mut |i| {
                    found.push(i);
                    false
                },
                work,
            )?;
        }
        found.sort_unstable();
        found.dedup();
        work.allocate(found.len() * 32)?;
        Ok(found.into_iter().map(|i| self.edges[i]).collect())
    }
}

#[cfg(test)]
mod pair_tests {
    use super::*;

    #[test]
    fn unordered_pairs_equal_exhaustive_closed_box_overlap() -> anyhow::Result<()> {
        let mut state = 17_u64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) as u32
        };
        let mut boxes = vec![
            [0., 0., 0., 0.],
            [0., 0., 0., 0.],
            [0., 0., 1., 1.],
            [1., 1., 2., 2.],
        ];
        for _ in 0..257 {
            let x = (next() % 64) as f64 / 8.;
            let y = (next() % 64) as f64 / 8.;
            boxes.push([
                x,
                y,
                x + (next() % 8) as f64 / 4.,
                y + (next() % 8) as f64 / 4.,
            ]);
        }
        let mut work = GeometryWork::default();
        let tree = PackedBoxes::new(&boxes, &mut work)?;
        let mut actual = Vec::new();
        assert!(!tree.pairs(
            &mut |_, _| true,
            &mut |i, j, _| {
                actual.push((i, j));
                Ok(false)
            },
            &mut work
        )?);
        actual.sort();
        let mut expected = Vec::new();
        for i in 0..boxes.len() {
            for j in i + 1..boxes.len() {
                if boxes_meet(&boxes[i], &boxes[j]) {
                    expected.push((i, j));
                }
            }
        }
        assert_eq!(
            actual, expected,
            "No duplicate or missing pair, including closed boundary and degenerate boxes"
        );
        Ok(())
    }
}
