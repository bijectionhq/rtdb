//! The deployment change feed a directory backend may offer (`Directory::deployment_changes`).

use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::{DeploymentRecord, Version};

/// Deployment records as their changes apply, fanned out to every receiver.
/// A record is published only if its version is above the last one published for its id, so a feed relaying a lagging or replayed source never rewinds a record.
/// A receiver that falls more than `CAPACITY` records behind sees `RecvError::Lagged` and has missed changes.
pub struct ChangeFeed {
    sender: broadcast::Sender<DeploymentRecord>,
    published: Mutex<HashMap<String, Version>>,
}

impl ChangeFeed {
    pub const CAPACITY: usize = 1024;

    pub fn new() -> Self { Self { sender: broadcast::channel(Self::CAPACITY).0, published: Mutex::new(HashMap::new()) } }

    /// Receives every record published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<DeploymentRecord> { self.sender.subscribe() }

    /// Publishes `record` unless this version of it or a later one has been published; returns whether it was.
    pub fn publish(&self, record: DeploymentRecord) -> bool {
        let mut published = self.published.lock();
        if published.get(&record.id).is_some_and(|version| *version >= record.version) { return false; }
        published.insert(record.id.clone(), record.version);
        // Sent under the lock, so concurrent publishers cannot reorder one record's versions.
        let _ = self.sender.send(record);
        true
    }
}

impl Default for ChangeFeed {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::{Directory, MemoryDirectory};

    /// Receives until every id in `last` has reached its version there, asserting that each record's versions arrive one by one, in order.
    pub(crate) async fn expect_every_change(receiver: &mut broadcast::Receiver<DeploymentRecord>, mut seen: BTreeMap<String, Version>, last: &BTreeMap<String, Version>) {
        let all_seen = |seen: &BTreeMap<String, Version>| last.iter().all(|(id, version)| seen.get(id) == Some(version));
        while !all_seen(&seen) {
            let record = tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await
                .unwrap_or_else(|_| panic!("every change arrives; seen {seen:?} of {last:?}")).expect("the feed runs");
            let Some(expected_last) = last.get(&record.id) else { continue };
            let held = seen.get(&record.id).copied().unwrap_or(0);
            assert_eq!(record.version, held + 1, "{}: versions arrive in order with none skipped", record.id);
            assert!(record.version <= *expected_last);
            seen.insert(record.id.clone(), record.version);
        }
    }

    /// Advances `records` by `rounds` replaces each, concurrently across records; returns each one's last version.
    pub(crate) async fn replace_concurrently(directory: Arc<dyn Directory>, records: Vec<DeploymentRecord>, rounds: u64) -> BTreeMap<String, Version> {
        let mut writers = tokio::task::JoinSet::new();
        for mut record in records {
            let directory = directory.clone();
            writers.spawn(async move {
                for _ in 0..rounds {
                    record.epoch += 1;
                    let expected = record.version;
                    record = directory.replace_deployment(record, expected).await.expect("replace").applied().expect("one writer per record");
                }
                (record.id, record.version)
            });
        }
        let mut last = BTreeMap::new();
        while let Some(written) = writers.join_next().await { let (id, version) = written.expect("writer"); last.insert(id, version); }
        last
    }

    #[test]
    fn a_feed_never_publishes_an_older_version_of_a_record() {
        let feed = ChangeFeed::new();
        let mut receiver = feed.subscribe();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.version = 3;
        assert!(feed.publish(record.clone()));
        record.version = 2;
        assert!(!feed.publish(record.clone()));
        record.version = 3;
        assert!(!feed.publish(record.clone()));
        record.version = 4;
        assert!(feed.publish(record));
        assert_eq!(receiver.try_recv().unwrap().version, 3);
        assert_eq!(receiver.try_recv().unwrap().version, 4);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_memory_feed_delivers_every_change_in_version_order() {
        let directory = MemoryDirectory::new();
        let mut receiver = directory.deployment_changes().expect("the memory directory has a feed");
        let mut records = Vec::new();
        for index in 0..4 { records.push(directory.create_deployment(DeploymentRecord::new(format!("shop-{index}"), "acme", vec![], 0)).await.unwrap()); }
        let last = replace_concurrently(directory, records, 50).await;
        expect_every_change(&mut receiver, BTreeMap::new(), &last).await;
    }
}
