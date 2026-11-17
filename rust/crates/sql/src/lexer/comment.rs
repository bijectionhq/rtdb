use rtdb_types::{AllocationScope, Result};
use super::{Cursor, size_error};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Edge { CloseStar, CloseSlash, OpenSlash, OpenStar, Any }

#[derive(Clone, Copy)]
struct State { edge: Edge, depth: usize }

struct States { ordered: Vec<State>, seen: Vec<u8> }

impl States {
    fn new() -> Self { Self { ordered: Vec::new(), seen: Vec::new() } }
    fn clear(&mut self) {
        for state in &self.ordered { self.seen[state.depth] = 0; }
        self.ordered.clear();
    }
    fn push(&mut self, edge: Edge, depth: usize, allocation: &AllocationScope) -> Result<()> {
        let bit = 1 << edge as u8;
        if self.seen.get(depth).is_some_and(|seen| seen & bit != 0) { return Ok(()); }
        let count = depth.checked_add(1).ok_or_else(size_error)?;
        grow(&mut self.seen, count, allocation)?;
        self.seen.resize(self.seen.len().max(count), 0);
        let count = self.ordered.len().checked_add(1).ok_or_else(size_error)?;
        grow(&mut self.ordered, count, allocation)?;
        self.ordered.push(State { edge, depth });
        self.seen[depth] |= bit;
        Ok(())
    }
    fn body(&mut self, depth: usize, allocation: &AllocationScope) -> Result<()> {
        self.push(Edge::CloseStar, depth, allocation)?;
        self.push(Edge::OpenSlash, depth, allocation)?;
        self.push(Edge::Any, depth, allocation)
    }
}

fn grow<T: Copy>(values: &mut Vec<T>, required: usize, allocation: &AllocationScope) -> Result<()> {
    if required <= values.capacity() { return Ok(()); }
    let capacity = values.capacity().checked_mul(2).ok_or_else(size_error)?.max(8).max(required);
    allocation.reserve(capacity.checked_mul(std::mem::size_of::<T>()).ok_or_else(size_error)?)?;
    let mut replacement = Vec::with_capacity(capacity);
    replacement.extend_from_slice(values);
    *values = replacement;
    Ok(())
}

pub(super) fn end<'sql>(start: Cursor<'sql>, allocation: &AllocationScope) -> Result<Option<Cursor<'sql>>> {
    let mut cursor = start.advance(2);
    let mut current = States::new();
    let mut next = States::new();
    current.body(0, allocation)?;
    let mut accepted = None;
    while let Some(value) = cursor.take() {
        next.clear();
        for state in &current.ordered {
            match (state.edge, value) {
                (Edge::CloseStar, '*') => next.push(Edge::CloseSlash, state.depth, allocation)?,
                (Edge::CloseSlash, '/') if state.depth == 0 => { accepted = Some(cursor); break; }
                (Edge::CloseSlash, '/') => next.body(state.depth - 1, allocation)?,
                (Edge::OpenSlash, '/') => next.push(Edge::OpenStar, state.depth, allocation)?,
                (Edge::OpenStar, '*') => next.body(state.depth.checked_add(1).ok_or_else(size_error)?, allocation)?,
                (Edge::Any, _) => next.body(state.depth, allocation)?,
                _ => (),
            }
        }
        if next.ordered.is_empty() { break; }
        std::mem::swap(&mut current, &mut next);
    }
    Ok(accepted)
}
