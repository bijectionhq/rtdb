//! Following a deployment's record without trusting the order replies arrive in (`WatchesMayLagButNeverRewind`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior};

use crate::{DeploymentRecord, Directory, feed::{DeploymentWatch, publish_newer}};

/// With a change feed, the poll is only a safety net against changes the feed lost, so it runs this many times less often.
pub const SAFETY_NET_FACTOR: u32 = 10;

/// Publishes each record of `id` whose version is above the one last published, reading it at once and then as it changes.
/// It follows the directory's change feed where the backend has one, polling every `SAFETY_NET_FACTOR` intervals in case the feed lost a change, and polls every `interval` where it has none or the feed ends.
/// On shared feeds, receivers of the same deployment and interval share one watch and safety-net poller.
/// A failed or absent read leaves the latest known record available.
/// The watch stops once every receiver has been dropped; it must be called within a Tokio runtime.
pub fn watch_deployment(directory: Arc<dyn Directory>, id: String, interval: Duration) -> watch::Receiver<Option<DeploymentRecord>> {
    if let Some(feed) = directory.change_feed() {
        let (receiver, owner) = feed.acquire_watch(&id, interval);
        if let Some(sender) = owner { tokio::spawn(poll_shared(directory, id, interval, sender)); }
        return receiver;
    }
    let (sender, receiver) = watch::channel(None);
    // Subscribed before the first read, so no change applied after that read can be missed.
    let mut changes = directory.deployment_changes();
    tokio::spawn(async move {
        let mut ticker = ticker(if changes.is_some() { interval * SAFETY_NET_FACTOR } else { interval });
        loop {
            let record = tokio::select! {
                _ = sender.closed() => return,
                // The read runs in the arm's body rather than the select, so a busy feed cannot cancel it before it completes.
                _ = ticker.tick() => directory.deployment(&id).await.ok().flatten(),
                change = next_change(&mut changes) => match change {
                    Ok(record) => Some(record).filter(|record| record.id == id),
                    Err(RecvError::Lagged(_)) => { ticker.reset_immediately(); None }
                    Err(RecvError::Closed) => { changes = None; ticker = self::ticker(interval); None }
                },
            };
            let Some(record) = record else { continue };
            publish_newer(&sender, record);
        }
    });
    receiver
}

async fn poll_shared(directory: Arc<dyn Directory>, id: String, period: Duration, watcher: Arc<DeploymentWatch>) {
    let feed = directory.change_feed();
    let mut ticker = ticker(period.saturating_mul(SAFETY_NET_FACTOR));
    loop {
        let record = tokio::select! {
            _ = watcher.sender.closed() => {
                if feed.is_none_or(|feed| {
                    feed.discard_unused_history();
                    feed.remove_watch_if_idle(&id, period, &watcher)
                }) { return; }
                None
            }
            record = watcher.next() => match record {
                Some(record) => Some(record),
                None => directory.deployment(&id).await.ok().flatten(),
            },
            _ = ticker.tick() => {
                if let Some(feed) = feed { feed.discard_unused_history(); }
                directory.deployment(&id).await.ok().flatten()
            },
        };
        if let Some(record) = record { publish_newer(&watcher.sender, record); }
    }
}

fn ticker(period: Duration) -> Interval {
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker
}

async fn next_change(changes: &mut Option<broadcast::Receiver<DeploymentRecord>>) -> Result<DeploymentRecord, RecvError> {
    match changes {
        Some(changes) => changes.recv().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cas, ChangeFeed, MemberId, MemberRecord, MemoryDirectory, Version};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use rtdb_types::Result;

    /// Answers `deployment` from a script, as a lagging replica might, and offers `changes` as its feed.
    struct Scripted { replies: Mutex<Vec<DeploymentRecord>>, inner: Arc<MemoryDirectory>, changes: Option<ChangeFeed> }

    #[async_trait]
    impl Directory for Scripted {
        async fn reserve_installed_journal(&self, id: uuid::Uuid) -> Result<()> { self.inner.reserve_installed_journal(id).await }
        async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> { self.inner.create_deployment(record).await }
        async fn stage_hosted_fork(&self, child: DeploymentRecord, preparation: crate::ForkPreparation) -> Result<DeploymentRecord> {
            self.inner.stage_hosted_fork(child, preparation).await
        }
        async fn publish_fork(&self, child: DeploymentRecord, expected_parent: Version, preparation: Option<crate::ForkPublication>) -> Result<DeploymentRecord> { self.inner.publish_fork(child, expected_parent, preparation).await }
        async fn lineage_state(&self, lineage: &str) -> Result<crate::LineageState> { self.inner.lineage_state(lineage).await }
        async fn change_lineage(&self, lineage: &str, change: crate::LineageChange) -> Result<crate::LineageState> { self.inner.change_lineage(lineage, change).await }
        async fn authorize_owned_retirement(&self, id: &str, expected: crate::Version, retirement: crate::LineageRetirement) -> Result<crate::LineageState> {
            self.inner.authorize_owned_retirement(id, expected, retirement).await
        }
        async fn settle_owned_version(&self, id: &str, expected: crate::Version, retirement: crate::LineageRetirement) -> Result<crate::Cas<DeploymentRecord>> { self.inner.settle_owned_version(id, expected, retirement).await }
        async fn begin_reference_disposal(&self, id: &str, expected: crate::Version) -> Result<crate::ReferenceDisposal> { self.inner.begin_reference_disposal(id, expected).await }
        async fn settle_reference_disposal(&self, permit: &crate::ReferenceDisposal, record: DeploymentRecord, expected: crate::Version) -> Result<crate::Cas<DeploymentRecord>> { self.inner.settle_reference_disposal(permit, record, expected).await }
        async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
            let next = { let mut replies = self.replies.lock(); (!replies.is_empty()).then(|| replies.remove(0)) };
            match next { Some(record) => Ok(Some(record)), None => self.inner.deployment(id).await }
        }
        async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> { self.inner.deployment_by_name(name).await }
        async fn deployments(&self) -> Result<Vec<DeploymentRecord>> { self.inner.deployments().await }
        async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> { self.inner.deployments_held_by(member).await }
        async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> { self.inner.replace_deployment(record, expected).await }
        async fn register_member(&self, record: MemberRecord) -> Result<MemberRecord> { self.inner.register_member(record).await }
        async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>> { self.inner.member(id).await }
        async fn members(&self) -> Result<Vec<MemberRecord>> { self.inner.members().await }
        async fn replace_member(&self, record: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>> { self.inner.replace_member(record, expected).await }
        fn deployment_changes(&self) -> Option<broadcast::Receiver<DeploymentRecord>> { self.changes.as_ref().map(ChangeFeed::subscribe) }
    }

    async fn next_version(receiver: &mut watch::Receiver<Option<DeploymentRecord>>) -> Version {
        tokio::time::timeout(Duration::from_secs(5), receiver.changed()).await.expect("a change arrives").expect("the poller runs");
        receiver.borrow_and_update().as_ref().expect("a record").version
    }

    #[tokio::test]
    async fn a_watch_publishes_newer_versions_and_skips_older_ones() {
        let inner = MemoryDirectory::new();
        let created = inner.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut held = created.clone();
        held.epoch = 1;
        let held = inner.replace_deployment(held, created.version).await.unwrap().applied().unwrap();
        let stale = created.clone();
        let directory = Arc::new(Scripted { replies: Mutex::new(vec![held.clone(), stale]), inner: inner.clone(), changes: None });

        let mut receiver = watch_deployment(directory, created.id.clone(), Duration::from_millis(5));
        assert_eq!(next_version(&mut receiver).await, held.version);

        let mut parked = held.clone();
        parked.epoch = 2;
        let parked = inner.replace_deployment(parked, held.version).await.unwrap().applied().unwrap();
        assert_eq!(next_version(&mut receiver).await, parked.version, "the stale reply between them is never published");
        assert_eq!(receiver.borrow().as_ref().unwrap().epoch, 2);
    }

    async fn advanced(directory: &dyn Directory, record: &DeploymentRecord) -> DeploymentRecord {
        let mut next = record.clone();
        next.epoch += 1;
        directory.replace_deployment(next, record.version).await.unwrap().applied().unwrap()
    }

    #[tokio::test]
    async fn a_watch_over_the_feed_sees_a_change_within_milliseconds() {
        let directory = MemoryDirectory::new();
        let created = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut receiver = watch_deployment(directory.clone(), created.id.clone(), Duration::from_secs(60));
        assert_eq!(next_version(&mut receiver).await, created.version, "the first read is at once");

        for _ in 0..3 {
            let current = directory.deployment(&created.id).await.unwrap().unwrap();
            let changed = advanced(directory.as_ref(), &current).await;
            let seen = tokio::time::timeout(Duration::from_millis(250), receiver.changed()).await;
            assert!(seen.is_ok(), "the change arrives long before the next poll");
            assert_eq!(receiver.borrow_and_update().as_ref().unwrap().version, changed.version);
        }
    }

    #[tokio::test]
    async fn a_watch_whose_feed_drops_changes_converges_through_the_safety_net() {
        let inner = MemoryDirectory::new();
        let created = inner.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let directory = Arc::new(Scripted { replies: Mutex::new(vec![]), inner: inner.clone(), changes: Some(ChangeFeed::new()) });
        let mut receiver = watch_deployment(directory, created.id.clone(), Duration::from_millis(5));
        assert_eq!(next_version(&mut receiver).await, created.version);

        let changed = advanced(inner.as_ref(), &created).await;
        assert_eq!(next_version(&mut receiver).await, changed.version, "the safety-net poll finds what the silent feed never sent");
    }
}
