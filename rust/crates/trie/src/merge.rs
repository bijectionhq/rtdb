use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rtdb_types::{Anomaly, EventHeader, Iid, Result, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowRef { pub segment: usize, pub row: u32 }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRow<R> { pub header: EventHeader, pub source: R }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Head { iid: Iid, system_position: (Timestamp, i128), stream: usize, row: usize }

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other.iid.cmp(&self.iid)
            .then_with(|| self.system_position.cmp(&other.system_position))
            .then_with(|| other.stream.cmp(&self.stream))
            .then_with(|| other.row.cmp(&self.row))
    }
}
impl PartialOrd for Head { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }

pub struct EventMerge<'a, R> { streams: Vec<&'a [EventRow<R>]>, heads: BinaryHeap<Head> }

impl<'a, R> EventMerge<'a, R> {
    /// Each stream is IID ascending and system position descending.
    /// Ties retain input-stream precedence, then each stream's existing order.
    pub fn new(streams: Vec<&'a [EventRow<R>]>) -> Result<Self> {
        let mut heads = BinaryHeap::new();
        for (stream, rows) in streams.iter().enumerate() {
            for pair in rows.windows(2) {
                let left = &pair[0].header;
                let right = &pair[1].header;
                if left.iid > right.iid || (left.iid == right.iid && left.precedence() < right.precedence()) {
                    return Err(Anomaly::incorrect("rtdb/event-order", format!("Segment {stream} is not ordered by IID and descending system position")));
                }
            }
            if let Some(first) = rows.first() {
                heads.push(Head { iid: first.header.iid, system_position: first.header.precedence(), stream, row: 0 });
            }
        }
        Ok(Self { streams, heads })
    }
}

impl<'a, R> Iterator for EventMerge<'a, R> {
    type Item = &'a EventRow<R>;
    fn next(&mut self) -> Option<Self::Item> {
        let head = self.heads.pop()?;
        let result = &self.streams[head.stream][head.row];
        let row = head.row + 1;
        if let Some(next) = self.streams[head.stream].get(row) {
            self.heads.push(Head { iid: next.header.iid, system_position: next.header.precedence(), stream: head.stream, row });
        }
        Some(result)
    }
}
