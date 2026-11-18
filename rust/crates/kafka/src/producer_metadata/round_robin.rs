/// A partition in Java Cluster's per-topic encounter order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionChoice {
    /// The signed metadata partition ID, without renumbering.
    pub id: i32,
    /// Whether Cluster resolved a non-null leader, including an empty Node.
    pub has_leader: bool,
}

/// A borrowed topic partition list whose length fits Java List.size().
#[derive(Clone, Copy, Debug)]
pub struct PartitionChoices<'a>(&'a [PartitionChoice]);

/// A partition list outside the Java collection-size domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsupportedListLength(
    /// Rejected partition count.
    pub usize,
);

impl<'a> PartitionChoices<'a> {
    /// Borrows the snapshot's original per-topic encounter order.
    /// Rejects lengths above i32::MAX.
    pub fn new(partitions: &'a [PartitionChoice]) -> Result<Self, UnsupportedListLength> {
        if partitions.len() > i32::MAX as usize {
            Err(UnsupportedListLength(partitions.len()))
        } else {
            Ok(Self(partitions))
        }
    }
}

/// A chosen explicit partition retained by one send across enqueue retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectedPartition(i32);

impl SelectedPartition {
    /// Returns the selected signed partition ID or the no-leader fallback index.
    pub fn id(self) -> i32 { self.0 }

    pub(super) fn explicit(id: i32) -> Option<Self> {
        (id >= 0).then_some(Self(id))
    }
}

/// Java RoundRobinPartitioner's remainder-by-zero failure for an empty topic list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptyTopic;

/// One producer/topic's signed RoundRobin counter, retained through metadata eviction.
/// Its owner serializes sends and calls select exactly once for each partitioning attempt.
#[derive(Debug)]
pub struct RoundRobinCounter { next: i32 }

impl RoundRobinCounter {
    /// Starts at zero, as AtomicInteger's value in Kafka RoundRobinPartitioner does.
    pub const fn new() -> Self { Self { next: 0 } }

    /// Consumes one wrapping counter value before selecting from the supplied snapshot.
    /// Available partitions yield their original IDs; an entirely unavailable list yields an index.
    /// EmptyTopic consumes the counter too; successful results must be reused for enqueue retries.
    pub fn select(&mut self, choices: PartitionChoices<'_>) -> Result<SelectedPartition, EmptyTopic> {
        let value = (self.next & i32::MAX) as usize;
        self.next = self.next.wrapping_add(1);
        let available = choices.0.iter().filter(|partition| partition.has_leader);
        let count = available.clone().count();
        if count != 0 {
            let partition = available.skip(value % count).next().expect("counted available partition");
            Ok(SelectedPartition(partition.id))
        } else if choices.0.is_empty() {
            Err(EmptyTopic)
        } else {
            Ok(SelectedPartition((value % choices.0.len()) as i32))
        }
    }
}

#[cfg(test)]
#[path = "round_robin/tests.rs"]
mod tests;
