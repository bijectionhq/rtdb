//! What a failed directory change leaves behind, and settling the release of a hold.

use std::{fmt::Display, future::Future, time::Duration};

use rtdb_storage::release::{Backoff, Settled, settle};
use rtdb_types::Anomaly;
use tokio_util::sync::CancellationToken;

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

/// Repeats an idempotent release until it applies, fails permanently, or `stop`
/// is cancelled, logging each retry and any unfinished outcome against `hold`.
///
/// The release is unbounded while its owner runs, for witnessed holds whose
/// reconciliation only retires them once the owner stops.
pub(crate) async fn release_hold<F, Fut>(hold: impl Display, stop: &CancellationToken, release: F) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    release_with_backoff(hold, Backoff::RELEASE, stop, release).await
}

/// Repeats an idempotent release with a deadline cap, for holds that lease
/// expiry or active reconciliation retires even while the owner keeps running.
pub(crate) async fn release_hold_bounded<F, Fut>(
    hold: impl Display,
    stop: &CancellationToken,
    give_up_after: Duration,
    release: F,
) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    let backoff = Backoff { give_up_after: Some(give_up_after), ..Backoff::RELEASE };
    release_with_backoff(hold, backoff, stop, release).await
}

async fn release_with_backoff<F, Fut>(
    hold: impl Display,
    backoff: Backoff,
    stop: &CancellationToken,
    release: F,
) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    let settled = settle(backoff, stop, release, |attempt, error, delay| {
        tracing::warn!(%hold, attempt, code = %error.code, retry_in = ?delay, "Hold release failed; retrying");
    }).await;
    match &settled {
        Settled::Released => {}
        Settled::Refused(error) | Settled::GaveUp(error) =>
            tracing::error!(%hold, code = %error.code, message = error.message.as_deref().unwrap_or(""), "Hold release did not apply; the hold stays registered"),
        Settled::Stopped => tracing::warn!(%hold, "Hold release stopped with its owner; the hold stays registered"),
    }
    settled
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use rtdb_directory::{Cas, DeploymentRecord, Directory, MemberId, MemberRecord, MemoryDirectory, ReplaceOutcome, Version};
    use rtdb_types::{AnomalyCategory, Result};

    use super::*;
    use crate::admin::change_installation;

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Fault {
        /// The next deployment read fails.
        Read,
        /// The next deployment read is refused with an error no retry can clear.
        Refuse,
        /// The next deployment replace is definitely refused without applying.
        RefuseReplace,
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
        async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> ReplaceOutcome<DeploymentRecord> {
            if self.take(|fault| matches!(fault, Fault::RefuseReplace)) {
                return ReplaceOutcome::Refused(Anomaly::incorrect("rtdb.test/replace-refused", "The scripted directory refused the replace"));
            }
            let replaced = self.inner.replace_deployment(record, expected).await;
            if self.take(|fault| matches!(fault, Fault::LostReply)) {
                return match replaced {
                    ReplaceOutcome::Applied(_) | ReplaceOutcome::Conflict(_) | ReplaceOutcome::Unknown(_) => ReplaceOutcome::Unknown(unavailable()),
                    ReplaceOutcome::Refused(error) => ReplaceOutcome::Refused(error),
                };
            }
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
        directory.inject(Fault::RefuseReplace);
        let refused_replace = change_installation(directory.as_ref(), &created.id, rename).await.unwrap_err();
        assert!(matches!(&refused_replace, ChangeFailure::NotApplied(error) if error.code == "rtdb.test/replace-refused"));
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

    #[tokio::test]
    async fn release_hold_stops_when_its_shutdown_token_is_cancelled() {
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let settled = release_hold("test hold", &stop, move || {
            let cancelled = cancelled.clone();
            async move {
                cancelled.cancel();
                Err(unavailable())
            }
        }).await;
        assert!(matches!(settled, Settled::Stopped));
    }

    #[tokio::test]
    async fn release_hold_bounded_gives_up_after_its_deadline() {
        let stop = CancellationToken::new();
        let settled = release_hold_bounded("bounded hold", &stop, Duration::ZERO, || async {
            Err(unavailable())
        }).await;
        assert!(matches!(settled, Settled::GaveUp(error) if error.category == AnomalyCategory::Unavailable));
    }
}
