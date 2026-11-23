use std::sync::Arc;
use parking_lot::Mutex;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};
use crate::{Error, ReadLease, Result};

#[macro_use]
mod provider_task;

tokio::task_local! {
    static RETAINED_OWNER: Arc<dyn Send + Sync>;
    static READ_LEASE: Arc<ReadLease>;
    static HELD_VIEW: Arc<HeldView>;
    static AUDIENCE_OBJECT_READ: ();
}

/// Registrations covering the physical catalogs used by one execution.
pub struct HeldView {
    _owners: Arc<[Arc<dyn Send + Sync>]>,
}

impl HeldView {
    pub fn new(catalog: Arc<dyn Send + Sync>, registrations: Vec<Arc<crate::cache::HeldCatalogRead>>) -> Self {
        let owners = std::iter::once(catalog).chain(registrations.into_iter().map(|hold| hold as Arc<dyn Send + Sync>)).collect();
        Self { _owners: owners }
    }
}

pub struct IoRegion {
    closed: Mutex<bool>,
    tasks: TaskTracker,
    parent: Option<Arc<IoRegion>>,
    lifetime: Option<Arc<dyn Send + Sync>>,
    #[cfg(feature = "remote")]
    publication_journal: Option<Arc<dyn crate::remote::PublicationJournal>>,
}
pub(crate) struct IoAdmission {
    lifetimes: Vec<Arc<dyn Send + Sync>>,
    tokens: Vec<TaskTrackerToken>,
    lease: Option<Arc<ReadLease>>,
}
impl IoAdmission {
    pub(crate) fn and(mut self, other: Self) -> Self {
        self.tokens.extend(other.tokens);
        self.lifetimes.extend(other.lifetimes);
        self.lease = match (self.lease, other.lease) {
            (Some(mine), Some(theirs)) => Some(mine.earlier(theirs)),
            (mine, theirs) => mine.or(theirs),
        };
        self
    }
}
impl IoRegion {
    pub fn new() -> Arc<Self> {
        Self::region(None, None)
    }
    /// Extends the owner's lifetime through provider operations admitted while polling the work.
    /// A nested operation inherits the enclosing owners.
    pub async fn retaining<T>(
        owner: Arc<dyn Send + Sync>, work: impl std::future::Future<Output = T>,
    ) -> T {
        macro_rules! combine { ($parent:ident, $owner:ident) => {{
            if Arc::ptr_eq(&$parent, &$owner) { $owner } else { Arc::new(($parent, $owner)) }
        }}; }
        scope_io_owner!(RETAINED_OWNER, owner, work; combine).await
    }
    /// Carries a held view into both synchronous and provider-backed reads in this execution.
    pub fn reading<F: std::future::Future>(view: Arc<HeldView>, work: F) -> impl std::future::Future<Output = F::Output> {
        macro_rules! combine { ($parent:ident, $view:ident) => {{
            if Arc::ptr_eq(&$parent, &$view) { $view } else {
                Arc::new(HeldView { _owners: Arc::from([$parent as Arc<dyn Send + Sync>, $view]) })
            }
        }}; }
        scope_io_owner!(HELD_VIEW, view, work; combine)
    }

    /// Checks the lease when each provider operation is admitted while polling the work.
    /// A nested lease narrows the one in scope where `leased` is called.
    /// Returns the work itself or its scoped future, so a lease adds no copy of the work to the caller's future.
    pub fn leased<F: std::future::Future>(lease: Option<Arc<ReadLease>>, work: F) -> impl std::future::Future<Output = F::Output> {
        use tokio_util::either::Either;
        let lease = match (lease, READ_LEASE.try_with(Arc::clone).ok()) {
            (Some(lease), Some(parent)) => lease.earlier(parent),
            (Some(lease), None) => lease,
            (None, Some(parent)) => parent,
            (None, None) => return Either::Right(work),
        };
        Either::Left(READ_LEASE.scope(lease, work))
    }
    /// Selects the dedicated provider read transport within an audience object read.
    pub async fn audience_object_read<T>(work: impl std::future::Future<Output = T>) -> T {
        AUDIENCE_OBJECT_READ.scope((), work).await
    }
    /// Whether this storage operation must use the audience object path.
    pub fn requires_audience_transport() -> bool { AUDIENCE_OBJECT_READ.try_with(|_| ()).is_ok() }
    /// Operations admitted through this child also belong to every ancestor's drain.
    /// Closing a child leaves siblings open; closing an ancestor rejects subsequent admissions through the child.
    /// Idle child handles do not delay any drain.
    pub fn child(self: &Arc<Self>) -> Arc<Self> {
        self.child_holding(None)
    }
    /// Retain a provider lifetime in every admitted task. An unfinished task
    /// keeps its authority witness even if the async region owner is dropped.
    pub fn child_holding(self: &Arc<Self>, lifetime: Option<Arc<dyn Send + Sync>>) -> Arc<Self> {
        Self::region(Some(Arc::clone(self)), lifetime)
    }
    fn region(parent: Option<Arc<IoRegion>>, lifetime: Option<Arc<dyn Send + Sync>>) -> Arc<Self> {
        Arc::new(Self {
            closed: Mutex::new(false), tasks: TaskTracker::new(), parent, lifetime,
            #[cfg(feature = "remote")]
            publication_journal: None,
        })
    }
    /// Gives an unused region a durable owner for its provider writes.
    #[cfg(feature = "remote")]
    pub fn track_publications(&mut self, journal: Arc<dyn crate::remote::PublicationJournal>) {
        assert!(self.publication_journal.is_none(), "a region has one publication journal");
        self.publication_journal = Some(journal);
    }
    #[cfg(feature = "remote")]
    pub fn publication_journal(&self) -> Option<&dyn crate::remote::PublicationJournal> {
        self.publication_journal.as_deref()
    }
    pub(crate) fn admit(&self) -> Result<IoAdmission> {
        let closed = self.closed.lock();
        if *closed { return Err(Error::Closed("I/O region is closed".into())); }
        let lease = if self.parent.is_none() { READ_LEASE.try_with(Arc::clone).ok() } else { None };
        if let Some(lease) = &lease { lease.check()?; }
        let mut admission = self.parent.as_ref().map(|parent| parent.admit()).transpose()?
            .unwrap_or_else(|| {
                let mut lifetimes: Vec<Arc<dyn Send + Sync>> = retain_admitted_owner!(RETAINED_OWNER).into_iter().collect();
                if let Ok(view) = HELD_VIEW.try_with(Arc::clone) { lifetimes.push(view); }
                IoAdmission { tokens: Vec::new(), lifetimes, lease }
            });
        admission.tokens.push(self.tasks.token());
        admission.lifetimes.extend(self.lifetime.iter().cloned());
        Ok(admission)
    }
    pub(crate) fn close(&self) { let mut closed = self.closed.lock(); *closed = true; self.tasks.close(); }
    pub(crate) async fn wait(&self) { self.tasks.wait().await; }
    /// Rejects new work and joins every admitted operation, including tasks whose callers were cancelled.
    /// The owner awaits this before shutting down the Tokio runtime; dropping an I/O future only drops its waiter.
    pub async fn close_and_wait(&self) { self.close(); self.wait().await; }
    /// Runs blocking work within this region and returns its result to the caller.
    /// A closed region rejects the operation; worker panics return a task error.
    /// Dropping the future cancels only the waiter, so the owner must drain this region before runtime shutdown.
    /// An undelivered result is destroyed before the drain completes; a delivered result belongs to the caller.
    pub async fn run_blocking<T: Send + 'static>(&self, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        self.run(work).await
    }
    pub(crate) async fn run<T: Send + 'static>(&self, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let operation = self.admit()?;
        Self::run_admitted(operation, work).await
    }
    pub(crate) async fn run_admitted<T: Send + 'static>(operation: IoAdmission, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let lease = operation.lease.clone();
        let result = Self::run_admitted_unclassified(operation, work).await;
        match lease { Some(lease) => lease.classify(result), None => result }
    }
    async fn run_admitted_unclassified<T: Send + 'static>(operation: IoAdmission, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let operation = operation;
            drop(sender.send(work()));
            drop(operation);
        }).await.map_err(|error| Error::Task(error.to_string()))?;
        receiver.await.map_err(|error| Error::Task(error.to_string()))?
    }
    #[cfg(any(feature = "remote", test))]
    pub(crate) async fn run_async<T: Send + 'static>(&self, work: impl std::future::Future<Output = Result<T>> + Send + 'static) -> Result<T> {
        Self::run_async_admitted(self.admit()?, work).await
    }
    #[cfg(any(feature = "remote", test))]
    pub(crate) async fn run_async_admitted<T: Send + 'static>(operation: IoAdmission, work: impl std::future::Future<Output = Result<T>> + Send + 'static) -> Result<T> {
        let lease = operation.lease.clone();
        let result = Self::run_async_admitted_unclassified(operation, work).await;
        match lease { Some(lease) => lease.classify(result), None => result }
    }
    #[cfg(any(feature = "remote", test))]
    async fn run_async_admitted_unclassified<T: Send + 'static>(operation: IoAdmission, work: impl std::future::Future<Output = Result<T>> + Send + 'static) -> Result<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let audience = Self::requires_audience_transport();
        // Verus checks the shared captures with contract adapters for the coroutine blocks.
        macro_rules! spawn {
            ($worker:ident; $captures:tt => $body:block) => { tokio::spawn(async move $body) };
        }
        macro_rules! hold {
            ($custody:ident, $body:block) => {
                crate::audience_work::hold_provider_custody($custody, async move $body).await
            };
        }
        macro_rules! inherit {
            ($worker:ident, $budget:expr, $work:ident) => { crate::audience_work::inherit($budget, $work) };
        }
        macro_rules! deliver {
            ($sender:ident, $audience:ident, $work:ident) => {{
                let result = if $audience { Self::audience_object_read($work).await } else { $work.await };
                drop($sender.send(result));
            }};
        }
        spawn_provider_task!(operation, work, sender, audience;
            crate::audience_work::current(), spawn, hold, inherit, deliver
        ).await.map_err(|error| Error::Task(error.to_string()))?;
        receiver.await.map_err(|error| Error::Task(error.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::{Future, poll_fn}, task::Poll};

    struct HeldDrop {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Drop for HeldDrop {
        fn drop(&mut self) { self.entered.take().unwrap().send(()).unwrap(); self.release.recv().unwrap(); }
    }

    #[tokio::test]
    async fn drain_waits_for_cancelled_result_destruction() {
        let region = IoRegion::new();
        let retained = Arc::new(());
        let owner = Arc::downgrade(&retained);
        let (working, started) = tokio::sync::oneshot::channel();
        let (finish_work, work_release) = std::sync::mpsc::channel();
        let (dropping, drop_started) = tokio::sync::oneshot::channel();
        let (finish_drop, drop_release) = std::sync::mpsc::channel();
        let mut request = Box::pin(IoRegion::retaining(retained, region.run_blocking(move || {
            working.send(()).unwrap(); work_release.recv().unwrap();
            Ok(HeldDrop { entered: Some(dropping), release: drop_release })
        })));
        poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap(); drop(request); finish_work.send(()).unwrap();
        drop_started.await.unwrap();
        assert!(owner.upgrade().is_some());
        let mut close = Box::pin(region.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        finish_drop.send(()).unwrap(); close.await;
        assert!(owner.upgrade().is_none());
    }

    #[tokio::test]
    async fn delivered_result_is_owned_by_the_caller_after_drain() {
        let region = IoRegion::new();
        let result = region.run_blocking(|| Ok(Arc::new(()))).await.unwrap();
        let weak = Arc::downgrade(&result);
        region.close_and_wait().await;
        assert!(weak.upgrade().is_some());
        drop(result); assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn panicking_worker_releases_the_region_and_reports_the_join_failure() {
        let region = IoRegion::new();
        let result: Result<()> = region.run_blocking(|| panic!("injected I/O failure")).await;
        assert!(matches!(result, Err(Error::Task(_))));
        region.close_and_wait().await;
    }

    #[tokio::test]
    async fn cancelled_async_waiter_keeps_admitted_work_until_its_final_effect() {
        let region = IoRegion::new();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, finish) = tokio::sync::oneshot::channel();
        let result = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed = Arc::clone(&result);
        let mut request = Box::pin(region.run_async(async move {
            entered.send(()).unwrap();
            finish.await.unwrap();
            completed.store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }));
        poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap();
        drop(request);
        let mut close = Box::pin(region.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(region.run_async(async { Ok(()) }).await, Err(Error::Closed(_))));
        assert!(!result.load(std::sync::atomic::Ordering::Acquire));
        release.send(()).unwrap();
        close.await;
        assert!(result.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn panicking_async_operation_releases_admission_and_reports_join_failure() {
        let region = IoRegion::new();
        let result: Result<()> = region.run_async(async { panic!("injected asynchronous I/O failure") }).await;
        assert!(matches!(result, Err(Error::Task(_))));
        region.close_and_wait().await;
    }

    #[tokio::test]
    async fn parent_close_rejects_descendants_without_waiting_for_idle_handles() {
        let parent = IoRegion::new();
        let child = parent.child();
        let grandchild = child.child();
        parent.close_and_wait().await;
        assert!(matches!(child.admit(), Err(Error::Closed(_))));
        assert!(matches!(grandchild.admit(), Err(Error::Closed(_))));
        grandchild.close_and_wait().await;
        child.close_and_wait().await;
    }

    #[tokio::test]
    async fn child_drain_retains_cancelled_work_without_closing_siblings() {
        let parent = IoRegion::new();
        let child = parent.child();
        let sibling = parent.child();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, finish) = tokio::sync::oneshot::channel();
        let mut request = Box::pin(child.run_async(async move { entered.send(()).unwrap(); finish.await.unwrap(); Ok(()) }));
        poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap();
        drop(request);
        let mut child_close = Box::pin(child.close_and_wait());
        poll_fn(|context| { assert!(child_close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(child.admit(), Err(Error::Closed(_))));
        sibling.run_async(async { Ok(()) }).await.unwrap();
        let mut parent_close = Box::pin(parent.close_and_wait());
        poll_fn(|context| { assert!(parent_close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(sibling.admit(), Err(Error::Closed(_))));
        release.send(()).unwrap();
        child_close.await;
        parent_close.await;
    }

    #[tokio::test]
    async fn combined_admissions_retain_cancelled_result_destruction_in_both_regions() {
        let first = IoRegion::new();
        let second = IoRegion::new();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, finish) = std::sync::mpsc::channel();
        let (dropping, drop_started) = tokio::sync::oneshot::channel();
        let (finish_drop, drop_release) = std::sync::mpsc::channel();
        let admission = first.admit().unwrap().and(second.admit().unwrap());
        let mut request = Box::pin(IoRegion::run_admitted(admission, move || {
            entered.send(()).unwrap();
            finish.recv().unwrap();
            Ok(HeldDrop { entered: Some(dropping), release: drop_release })
        }));
        poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap();
        drop(request);
        release.send(()).unwrap();
        drop_started.await.unwrap();
        let mut first_close = Box::pin(first.close_and_wait());
        let mut second_close = Box::pin(second.close_and_wait());
        poll_fn(|context| {
            assert!(first_close.as_mut().poll(context).is_pending());
            assert!(second_close.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        finish_drop.send(()).unwrap();
        first_close.await;
        second_close.await;
    }
}
