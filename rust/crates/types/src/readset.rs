//! Shared matching for ordered validation and invalidation.
//! The caller selects the world or unforked history and revision window.
//! A fixed Versions window already below the basis has no new Cell touches
//! within that window. Deployment-wide exclusions are considered separately.

use std::{ops::Bound, sync::Arc};

use crate::{Iid, Instant, Timestamp};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RelationId(pub Iid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Part {
    Presence,
    Payload(u8),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Cell<R = RelationId, I = Iid> {
    pub relation: R,
    pub id: I,
    pub part: Part,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Path<R = RelationId> {
    pub relation: R,
    pub id: Iid,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PathGeneration {
    pub ty: u32,
    pub protection: u32,
}

/// Full key in the path's bytewise order, including any physical tie-break.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PathKey(pub Vec<u8>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Span {
    pub lo: Bound<PathKey>,
    pub hi: Bound<PathKey>,
}

impl Span {
    pub fn contains(&self, key: &PathKey) -> bool {
        let above_lower = match &self.lo {
            Bound::Included(lo) => key >= lo,
            Bound::Excluded(lo) => key > lo,
            Bound::Unbounded => true,
        };
        let below_upper = match &self.hi {
            Bound::Included(hi) => key <= hi,
            Bound::Excluded(hi) => key < hi,
            Bound::Unbounded => true,
        };
        above_lower && below_upper
    }

    /// Use only when a limit stopped the scan. A completed scan covers its
    /// requested span, even if it returned no keys.
    pub fn through_last(mut self, direction: Direction, last: PathKey) -> Option<Self> {
        if !self.contains(&last) {
            return None;
        }
        match direction {
            Direction::Ascending => self.hi = Bound::Included(last),
            Direction::Descending => self.lo = Bound::Included(last),
        }
        Some(self)
    }
}

/// Partitions already visited are a planning hint. A new flowing label can
/// change the answer even when no read took place in that partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathRead<L, R = RelationId> {
    pub path: Path<R>,
    pub generation: PathGeneration,
    pub partitions: Arc<[L]>,
    pub span: Span,
    pub valid_at: Option<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadTarget<L, W, R = RelationId, I = Iid> {
    Point {
        cell: Cell<R, I>,
        partitions: Arc<[L]>,
        valid_at: Option<Instant>,
    },
    Interval(PathRead<L, R>),
    Count {
        read: PathRead<L, R>,
        observed: u64,
    },
    Bound {
        read: PathRead<L, R>,
        max: u64,
        charged: Option<L>,
    },
    Search {
        path: Path<R>,
        partitions: Arc<[L]>,
    },
    Versions {
        cell: Cell<R, I>,
        from: Timestamp,
        through: Timestamp,
        valid_at: Option<Instant>,
    },
    World {
        world: W,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Deadline(pub Timestamp);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadEntry<L, W, E, R = RelationId, I = Iid> {
    pub target: ReadTarget<L, W, R, I>,
    pub label: L,
    pub until: Option<Deadline>,
    pub exclusions: Arc<[E]>,
}

/// A half-open valid-time period. An open end includes every instant after its start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Period {
    pub from: Instant,
    pub to: Option<Instant>,
}

impl Period {
    fn holds(self, valid_at: Option<Instant>) -> bool {
        self.to.is_none_or(|to| self.from < to)
            && valid_at.is_none_or(|at| self.from <= at && self.to.is_none_or(|to| at < to))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Touch<L, W, E, R = RelationId, I = Iid> {
    Cell {
        cell: Cell<R, I>,
        old: Option<L>,
        new: Option<L>,
        period: Option<Period>,
    },
    Key {
        path: Path<R>,
        generation: PathGeneration,
        old: Option<(L, PathKey)>,
        new: Option<(L, PathKey)>,
        period: Option<Period>,
    },
    Exclusion {
        tag: E,
        partitions: Arc<[(R, L)]>,
    },
    World {
        world: W,
    },
}

pub trait LabelOrder<L> {
    fn flows(&self, from: &L, to: &L) -> bool;
}

impl<L, F: Fn(&L, &L) -> bool> LabelOrder<L> for F {
    fn flows(&self, from: &L, to: &L) -> bool {
        self(from, to)
    }
}

fn holds(period: Option<Period>, valid_at: Option<Instant>) -> bool {
    period.is_none_or(|period| period.holds(valid_at))
}

/// Counts and bounds use this predicate for invalidation. Their validation
/// verdict requires re-evaluating the count at the ordering position.
pub fn intersects<L: Eq, W: Eq, E: Eq, R: Eq, I: Eq>(
    entry: &ReadEntry<L, W, E, R, I>,
    touch: &Touch<L, W, E, R, I>,
    order: &(impl LabelOrder<L> + ?Sized),
) -> bool {
    let flows = |label: &L| order.flows(label, &entry.label);
    match touch {
        Touch::Cell {
            cell,
            old,
            new,
            period,
        } => {
            let in_frame = || old.iter().chain(new).any(flows);
            match &entry.target {
                ReadTarget::Point {
                    cell: read,
                    valid_at,
                    ..
                }
                | ReadTarget::Versions {
                    cell: read,
                    valid_at,
                    ..
                } => cell == read && holds(*period, *valid_at) && in_frame(),
                ReadTarget::Search { path, .. } => {
                    cell.relation == path.relation && holds(*period, None) && in_frame()
                }
                _ => false,
            }
        }
        Touch::Key {
            path,
            generation,
            old,
            new,
            period,
        } => {
            let (read, bound, charged) = match &entry.target {
                ReadTarget::Interval(read) | ReadTarget::Count { read, .. } => (read, false, None),
                ReadTarget::Bound { read, charged, .. } => (read, true, charged.as_ref()),
                ReadTarget::Search { path: searched, .. } => {
                    return path.relation == searched.relation
                        && holds(*period, None)
                        && old.iter().chain(new).any(|(label, _)| flows(label));
                }
                _ => return false,
            };
            if path != &read.path
                || *generation != read.generation
                || !holds(*period, read.valid_at)
            {
                return false;
            }
            let inside = |(label, key): &(L, PathKey)| {
                charged.is_none_or(|partition| partition == label)
                    && flows(label)
                    && read.span.contains(key)
            };
            let before = old.as_ref().is_some_and(inside);
            let after = new.as_ref().is_some_and(inside);
            if bound {
                after && !before
            } else {
                before || after
            }
        }
        Touch::Exclusion { tag, partitions } => {
            let covers = |relation: &R| {
                partitions
                    .iter()
                    .any(|(id, label)| id == relation && flows(label))
            };
            match &entry.target {
                ReadTarget::Point { .. }
                | ReadTarget::Interval(_)
                | ReadTarget::Versions { .. } => entry.exclusions.contains(tag),
                ReadTarget::Count { read, .. } => covers(&read.path.relation),
                ReadTarget::Search { path, .. } => covers(&path.relation),
                ReadTarget::Bound { .. } | ReadTarget::World { .. } => false,
            }
        }
        Touch::World { world } => {
            matches!(&entry.target, ReadTarget::World { world: read } if world == read)
        }
    }
}

#[cfg(test)]
mod tests;
