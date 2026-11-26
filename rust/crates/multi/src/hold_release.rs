//! What a failed directory change leaves behind, and settling the release of a hold.

use std::{fmt::Display, future::Future};

use rtdb_storage::release::{Backoff, Settled, settle};
use rtdb_types::Anomaly;

/// A directory change that failed, told apart by whether it may still have applied.
#[derive(Clone, Debug)]
pub(crate) enum ChangeFailure {
    /// The directory never stored the change: it was refused, or never sent.
    NotApplied(Anomaly),
    /// A compare-and-replace was sent and no reply came back, so it may have applied.
    Unknown(Anomaly),
}

impl ChangeFailure {
    pub(crate) fn may_have_applied(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

impl From<ChangeFailure> for Anomaly {
    fn from(failure: ChangeFailure) -> Self {
        match failure {
            ChangeFailure::NotApplied(error) | ChangeFailure::Unknown(error) => error,
        }
    }
}

/// Repeats an idempotent release until it applies or fails permanently,
/// logging each retry and a permanent failure against `hold`.
pub(crate) async fn release_hold<F, Fut>(hold: impl Display, release: F) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    let settled = settle(Backoff::RELEASE, release, |attempt, error, delay| {
        tracing::warn!(%hold, attempt, code = %error.code, retry_in = ?delay, "Hold release failed; retrying");
    }).await;
    if let Settled::Refused(error) = &settled {
        tracing::error!(%hold, code = %error.code, message = error.message.as_deref().unwrap_or(""), "Hold release was refused; the hold stays registered");
    }
    settled
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use rtdb_directory::{Cas, DeploymentRecord, Directory, MemberId, MemberRecord, MemoryDirectory, Version};
    use rtdb_types::{AnomalyCategory, Result};

    use super::*;
    use crate::admin::change_installation;

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Fault {
        /// The next deployment read fails.
        Read,
        /// The next deployment read is refused with an error no retry can clear.
        Refuse,
        /// The next replace applies and then loses its reply.
        LostReply,
    }

    /// A directory whose next requests fail as scripted.
    pub(crate) struct ScriptedDirectory {
        pub(crate) inner: Arc<MemoryDirectory>,
        faults: Mutex<VecDeque<Fault>>,
    }

    impl ScriptedDirectory {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self { inner: MemoryDirectory::new(), faults: Mutex::default() })
        }

        pub(crate) fn inject(&self, fault: Fault) {
            self.faults.lock().push_back(fault);
        }

        fn take(&self, matching: fn(Fault) -> bool) -> bool {
            let mut faults = self.faults.lock();
            let hit = faults.front().is_some_and(|fault| matching(*fault));
            if hit { faults.pop_front(); }
            hit
        }
    }

    fn unavailable() -> Anomaly {
        Anomaly::new(AnomalyCategory::Unavailable, "rtdb.test/directory-unavailable", "The scripted directory failed")
    }

    #[async_trait]
    impl Directory for ScriptedDirectory {
        async fn reserve_installed_journal(&self, id: uuid::Uuid) -> Result<()> { self.inner.reserve_installed_journal(id).await }
        async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> { self.inner.create_deployment(record).await }
        async fn stage_fork(&self, child: DeploymentRecord, preparation: rtdb_directory::ForkPreparation) -> Result<DeploymentRecord> { self.inner.stage_fork(child, preparation).await }
        async fn publish_fork(&self, child: DeploymentRecord, expected: Version, preparation: Option<rtdb_directory::ForkPublication>) -> Result<DeploymentRecord> {
            self.inner.publish_fork(child, expected, preparation).await
        }
        async fn lineage_state(&self, lineage: &str) -> Result<rtdb_directory::LineageState> { self.inner.lineage_state(lineage).await }
        async fn change_lineage(&self, lineage: &str, change: rtdb_directory::LineageChange) -> Result<rtdb_directory::LineageState> { self.inner.change_lineage(lineage, change).await }
        async fn authorize_owned_retirement(&self, id: &str, expected: Version, retirement: rtdb_directory::LineageRetirement) -> Result<rtdb_directory::LineageState> {
            self.inner.authorize_owned_retirement(id, expected, retirement).await
        }
        async fn settle_owned_version(&self, id: &str, expected: Version, retirement: rtdb_directory::LineageRetirement) -> Result<Cas<DeploymentRecord>> {
            self.inner.settle_owned_version(id, expected, retirement).await
        }
        async fn begin_reference_disposal(&self, id: &str, expected: Version) -> Result<rtdb_directory::ReferenceDisposal> { self.inner.begin_reference_disposal(id, expected).await }
        async fn settle_reference_disposal(&self, permit: &rtdb_directory::ReferenceDisposal, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
            self.inner.settle_reference_disposal(permit, record, expected).await
        }
        async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> {
            if self.take(|fault| matches!(fault, Fault::Read)) { return Err(unavailable()); }
            if self.take(|fault| matches!(fault, Fault::Refuse)) {
                return Err(Anomaly::incorrect("rtdb.test/directory-refused", "The scripted directory refused the read"));
            }
            self.inner.deployment(id).await
        }
        async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> { self.inner.deployment_by_name(name).await }
        async fn deployments(&self) -> Result<Vec<DeploymentRecord>> { self.inner.deployments().await }
        async fn deployments_held_by(&self, id: &MemberId) -> Result<Vec<DeploymentRecord>> { self.inner.deployments_held_by(id).await }
        async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
            let replaced = self.inner.replace_deployment(record, expected).await;
            if self.take(|fault| matches!(fault, Fault::LostReply)) { replaced?; return Err(unavailable()); }
            replaced
        }
        async fn register_member(&self, member: MemberRecord) -> Result<MemberRecord> { self.inner.register_member(member).await }
        async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>> { self.inner.member(id).await }
        async fn members(&self) -> Result<Vec<MemberRecord>> { self.inner.members().await }
        async fn replace_member(&self, member: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>> { self.inner.replace_member(member, expected).await }
        fn change_feed(&self) -> Option<&rtdb_directory::ChangeFeed> { self.inner.change_feed() }
    }

    async fn installation(directory: &ScriptedDirectory) -> DeploymentRecord {
        directory.create_deployment(DeploymentRecord::new("shop", "acme", Vec::new(), 0)).await.unwrap()
    }

    fn rename(record: &mut DeploymentRecord) -> Result<()> {
        record.org = "renamed".into();
        Ok(())
    }

    #[tokio::test]
    async fn an_applied_change_returns_the_stored_record() {
        let directory = ScriptedDirectory::new();
        let created = installation(&directory).await;
        let stored = change_installation(directory.as_ref(), &created.id, rename).await.unwrap();
        assert_eq!(stored.org, "renamed");
        assert!(stored.version > created.version);
    }

    #[tokio::test]
    async fn refused_and_unread_changes_are_not_applied() {
        let directory = ScriptedDirectory::new();
        let created = installation(&directory).await;
        let refusal = Anomaly::new(AnomalyCategory::Unavailable, "rtdb.test/refused", "The change is refused");
        let refused = change_installation(directory.as_ref(), &created.id, |_| Err(refusal.clone())).await.unwrap_err();
        assert!(matches!(&refused, ChangeFailure::NotApplied(error) if error.code == "rtdb.test/refused"));
        directory.inject(Fault::Read);
        assert!(!change_installation(directory.as_ref(), &created.id, rename).await.unwrap_err().may_have_applied());
        assert!(!change_installation(directory.as_ref(), "missing", rename).await.unwrap_err().may_have_applied());
        assert_eq!(directory.deployment(&created.id).await.unwrap().unwrap().version, created.version);
    }

    #[tokio::test]
    async fn a_lost_replace_reply_leaves_the_change_unknown() {
        let directory = ScriptedDirectory::new();
        let created = installation(&directory).await;
        directory.inject(Fault::LostReply);
        let failure = change_installation(directory.as_ref(), &created.id, rename).await.unwrap_err();
        assert!(failure.may_have_applied());
        assert_eq!(directory.deployment(&created.id).await.unwrap().unwrap().org, "renamed");
    }
}
