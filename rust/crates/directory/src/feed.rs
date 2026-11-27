//! The deployment change feed a directory backend may offer (`Directory::deployment_changes`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, broadcast, watch};

use crate::{DeploymentRecord, Version};

pub(crate) type WatchSender = watch::Sender<Option<DeploymentRecord>>;
type WatchReceiver = watch::Receiver<Option<DeploymentRecord>>;

pub(crate) struct DeploymentWatch {
    pub(crate) sender: WatchSender,
    pending: Mutex<Option<DeploymentRecord>>,
    refresh: AtomicBool,
    changed: Notify,
}

impl DeploymentWatch {
    fn offer(&self, record: DeploymentRecord) {
        let mut pending = self.pending.lock();
        if pending.as_ref().is_none_or(|pending| record.version > pending.version) {
            *pending = Some(record);
            self.changed.notify_one();
        }
    }

    /// A record from the feed, or a request to read the current record after the feed starts or reconnects.
    pub(crate) async fn next(&self) -> Option<DeploymentRecord> {
        loop {
            let notified = self.changed.notified();
            if let Some(record) = self.pending.lock().take() { return Some(record); }
            if self.refresh.swap(false, Ordering::Relaxed) { return None; }
            notified.await;
        }
    }
}

#[derive(Default)]
struct FeedState {
    published: HashMap<String, Version>,
    watches: HashMap<String, HashMap<Duration, Weak<DeploymentWatch>>>,
}

/// Deployment records as their changes apply.
/// The stream preserves per-record version order and reports lag when a subscriber misses changes.
/// Shared deployment watches coalesce changes to the newest version they have seen.
pub struct ChangeFeed {
    sender: broadcast::Sender<DeploymentRecord>,
    state: Mutex<FeedState>,
}

impl ChangeFeed {
    pub const CAPACITY: usize = 1024;

    pub fn new() -> Self { Self { sender: broadcast::channel(Self::CAPACITY).0, state: Mutex::new(FeedState::default()) } }

    /// Receives every record published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<DeploymentRecord> {
        let mut state = self.state.lock();
        if self.sender.receiver_count() == 0 { state.published = HashMap::new(); }
        self.sender.subscribe()
    }

    #[cfg(feature = "dynamodb")]
    pub(crate) fn has_broadcast_receivers(&self) -> bool { self.sender.receiver_count() != 0 }

    #[cfg(feature = "dynamodb")]
    pub(crate) fn refresh_watches(&self) {
        let watches: Vec<_> = self.state.lock().watches.values()
            .flat_map(|by_interval| by_interval.values().filter_map(Weak::upgrade)).collect();
        for watch in watches {
            watch.refresh.store(true, Ordering::Relaxed);
            watch.changed.notify_one();
        }
    }

    pub(crate) fn discard_unused_history(&self) {
        let mut state = self.state.lock();
        if self.sender.receiver_count() == 0 && !state.published.is_empty() {
            state.published = HashMap::new();
        }
    }

    pub(crate) fn acquire_watch(&self, id: &str, interval: Duration) -> (WatchReceiver, Option<Arc<DeploymentWatch>>) {
        let mut state = self.state.lock();
        let by_interval = state.watches.entry(id.to_owned()).or_default();
        if let Some(sender) = by_interval.get(&interval).and_then(Weak::upgrade) {
            return (sender.sender.subscribe(), None);
        }
        let (sender, receiver) = watch::channel(None);
        let sender = Arc::new(DeploymentWatch { sender, pending: Mutex::new(None), refresh: AtomicBool::new(false), changed: Notify::new() });
        by_interval.insert(interval, Arc::downgrade(&sender));
        (receiver, Some(sender))
    }

    pub(crate) fn remove_watch_if_idle(&self, id: &str, interval: Duration, sender: &Arc<DeploymentWatch>) -> bool {
        let mut state = self.state.lock();
        // A subscriber may have joined again after the poller observed its channel close.
        if sender.sender.receiver_count() != 0 { return false; }
        if let Some(by_interval) = state.watches.get_mut(id) {
            if by_interval.get(&interval).is_some_and(|current| current.ptr_eq(&Arc::downgrade(sender))) {
                by_interval.remove(&interval);
            }
            if by_interval.is_empty() { state.watches.remove(id); }
        }
        true
    }

    /// Offers a record to its keyed watches and to any consumers of the whole feed.
    pub fn publish(&self, record: DeploymentRecord) -> bool {
        let watches = {
            let mut state = self.state.lock();
            let broadcasting = self.sender.receiver_count() != 0;
            if broadcasting {
                if state.published.get(&record.id).is_some_and(|version| *version >= record.version) { return false; }
                state.published.insert(record.id.clone(), record.version);
            } else if !state.published.is_empty() {
                state.published = HashMap::new();
            }

            let watches: Vec<_> = state.watches.get(&record.id).into_iter()
                .flat_map(|by_interval| by_interval.values().filter_map(Weak::upgrade)).collect();
            if broadcasting {
                if watches.is_empty() {
                    let _ = self.sender.send(record);
                    return true;
                }
                let _ = self.sender.send(record.clone());
            }
            watches
        };
        for watcher in watches { watcher.offer(record.clone()); }
        true
    }
}

pub(crate) fn publish_newer(sender: &WatchSender, record: DeploymentRecord) {
    sender.send_if_modified(|held| {
        let newer = held.as_ref().is_none_or(|held| record.version > held.version);
        if newer { *held = Some(record); }
        newer
    });
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
                    record = directory.replace_deployment(record, expected).await.applied().expect("one writer per record");
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

    #[tokio::test]
    async fn keyed_watches_leave_unwatched_deployments_out_of_the_feed_state() {
        let feed = ChangeFeed::new();
        let mut active = DeploymentRecord::new("shop", "acme", vec![], 0);
        active.version = 1;
        let (_receiver, watch) = feed.acquire_watch(&active.id, Duration::from_millis(100));
        feed.publish(active.clone());

        let broadcast = feed.subscribe();
        let mut parked = DeploymentRecord::new("parked", "acme", vec![], 0);
        parked.version = 1;
        feed.publish(parked);
        assert_eq!(feed.state.lock().published.len(), 1);
        drop(broadcast);
        for _ in 0..10_000 {
            feed.publish(DeploymentRecord::new("parked", "acme", vec![], 0));
        }

        let watched = watch.unwrap().next().await.unwrap();
        assert_eq!(watched, active);
        let state = feed.state.lock();
        assert_eq!(state.watches.len(), 1);
        assert!(state.published.is_empty());
    }

    #[tokio::test]
    async fn an_active_keyed_watch_releases_broadcast_history_after_the_feed_goes_quiet() {
        let directory = MemoryDirectory::new();
        let active = directory.create_deployment(DeploymentRecord::new("active", "acme", vec![], 0)).await.unwrap();
        let mut watcher = crate::watch_deployment(directory.clone(), active.id, Duration::from_millis(5));
        drop(watcher.wait_for(|record| record.is_some()).await.unwrap());
        let feed = directory.change_feed().unwrap();
        let broadcast = feed.subscribe();
        directory.create_deployment(DeploymentRecord::new("parked", "acme", vec![], 0)).await.unwrap();
        assert_eq!(feed.state.lock().published.len(), 1);
        drop(broadcast);

        tokio::time::timeout(Duration::from_secs(2), async {
            while !feed.state.lock().published.is_empty() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn only_the_last_receiver_releases_a_shared_watch() {
        let directory = MemoryDirectory::new();
        let created = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let interval = Duration::from_secs(60);
        let receivers = (0..1000).map(|_| crate::watch_deployment(directory.clone(), created.id.clone(), interval)).collect::<Vec<_>>();
        let mut last = crate::watch_deployment(directory.clone(), created.id.clone(), interval);
        assert!(receivers.iter().all(|receiver| receiver.same_channel(&last)));
        let faster = crate::watch_deployment(directory.clone(), created.id.clone(), Duration::from_secs(1));
        assert!(!faster.same_channel(&last));
        drop(faster);
        drop(receivers);
        let mut next = created.clone();
        next.epoch = 1;
        let next = directory.replace_deployment(next, created.version).await.applied().unwrap();
        let seen = tokio::time::timeout(Duration::from_secs(1), last.wait_for(|record| record.as_ref() == Some(&next))).await.unwrap().unwrap();
        drop(seen);
        drop(last);
        let feed = directory.change_feed().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while feed.state.lock().watches.contains_key(&created.id) { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(feed.sender.receiver_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn holding_a_watch_snapshot_does_not_block_directory_writes() {
        let directory = MemoryDirectory::new();
        let created = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut receiver = crate::watch_deployment(directory.clone(), created.id.clone(), Duration::from_secs(60));
        let snapshot = receiver.wait_for(|record| record.is_some()).await.unwrap();
        let write = tokio::spawn(async move {
            let version = created.version;
            let mut next = created;
            next.epoch += 1;
            directory.replace_deployment(next, version).await.applied().unwrap()
        });
        let finished = tokio::time::timeout(Duration::from_secs(1), write).await;
        drop(snapshot);
        let stored = finished.expect("directory update while the watch snapshot is borrowed").unwrap();
        tokio::time::timeout(Duration::from_secs(1), receiver.wait_for(|record| record.as_ref() == Some(&stored))).await.unwrap().unwrap();
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
