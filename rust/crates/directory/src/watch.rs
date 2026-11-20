//! Following a deployment's record without trusting the order replies arrive in (`WatchesMayLagButNeverRewind`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::{DeploymentRecord, Directory};

/// Polls `id` every `interval` and publishes each record whose version is above the one last published.
/// A failed or absent read publishes nothing, so a directory outage leaves watchers on the last record they saw.
/// The poller stops once every receiver has been dropped; it must be called within a Tokio runtime.
pub fn watch_deployment(directory: Arc<dyn Directory>, id: String, interval: Duration) -> watch::Receiver<Option<DeploymentRecord>> {
    let (sender, receiver) = watch::channel(None);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let read = tokio::select! {
                _ = sender.closed() => return,
                read = async { ticker.tick().await; directory.deployment(&id).await } => read,
            };
            let Ok(Some(record)) = read else { continue };
            sender.send_if_modified(|held: &mut Option<DeploymentRecord>| {
                let newer = held.as_ref().is_none_or(|held| record.version > held.version);
                if newer { *held = Some(record); }
                newer
            });
        }
    });
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cas, MemberId, MemberRecord, MemoryDirectory, Version};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use rtdb_types::Result;

    /// Answers `deployment` from a script, as a lagging replica might.
    struct Scripted { replies: Mutex<Vec<DeploymentRecord>>, inner: Arc<MemoryDirectory> }

    #[async_trait]
    impl Directory for Scripted {
        async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> { self.inner.create_deployment(record).await }
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
        let directory = Arc::new(Scripted { replies: Mutex::new(vec![held.clone(), stale]), inner: inner.clone() });

        let mut receiver = watch_deployment(directory, created.id.clone(), Duration::from_millis(5));
        assert_eq!(next_version(&mut receiver).await, held.version);

        let mut parked = held.clone();
        parked.epoch = 2;
        let parked = inner.replace_deployment(parked, held.version).await.unwrap().applied().unwrap();
        assert_eq!(next_version(&mut receiver).await, parked.version, "the stale reply between them is never published");
        assert_eq!(receiver.borrow().as_ref().unwrap().epoch, 2);
    }
}
