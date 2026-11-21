use std::{collections::BTreeMap, sync::Arc};
use parking_lot::Mutex;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};
use crate::{Error, ObjectKey, Result};
use uuid::Uuid;

pub struct IoRegion {
    closed: Mutex<bool>,
    tasks: TaskTracker,
    parent: Option<Arc<IoRegion>>,
    lifetime: Option<Arc<dyn Send + Sync>>,
    pending_publications: Mutex<BTreeMap<Uuid, ObjectKey>>,
}
pub(crate) struct IoAdmission {
    tokens: Vec<TaskTrackerToken>,
    lifetimes: Vec<Arc<dyn Send + Sync>>,
}
impl IoAdmission {
    pub(crate) fn and(mut self, other: Self) -> Self {
        self.tokens.extend(other.tokens);
        self.lifetimes.extend(other.lifetimes);
        self
    }
}
impl IoRegion {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { closed: Mutex::new(false), tasks: TaskTracker::new(), parent: None, lifetime: None, pending_publications: Mutex::new(BTreeMap::new()) })
    }
    /// Operations admitted through this child also belong to every ancestor's drain.
    /// Closing a child leaves siblings open; closing an ancestor rejects subsequent admissions through the child.
    /// Idle child handles do not delay any drain.
    pub fn child(self: &Arc<Self>) -> Arc<Self> {
        self.child_holding(None)
    }
    /// Retain a provider lifetime in every admitted task. An unfinished task
    /// keeps its authority witness even if the async region owner is dropped.
    pub fn child_holding(self: &Arc<Self>, lifetime: Option<Arc<dyn Send + Sync>>) -> Arc<Self> {
        Arc::new(Self { closed: Mutex::new(false), tasks: TaskTracker::new(), parent: Some(Arc::clone(self)), lifetime, pending_publications: Mutex::new(BTreeMap::new()) })
    }
    pub fn has_unknown_publication(&self) -> bool {
        !self.pending_publications.lock().is_empty()
    }
    /// The exact writes the owner must retain after closing and joining the region.
    pub fn pending_publications(&self) -> BTreeMap<Uuid, ObjectKey> {
        self.pending_publications.lock().clone()
    }
    #[cfg(feature = "remote")]
    pub(crate) fn publication(&self, attempt: Uuid, key: ObjectKey) -> Publication<'_> {
        Publication { region: self, pending: Some((attempt, key)) }
    }
    pub(crate) fn admit(&self) -> Result<IoAdmission> {
        let closed = self.closed.lock();
        if *closed { return Err(Error::Closed("I/O region is closed".into())); }
        let mut admission = self.parent.as_ref().map(|parent| parent.admit()).transpose()?
            .unwrap_or_else(|| IoAdmission { tokens: Vec::new(), lifetimes: Vec::new() });
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
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let operation = operation;
            drop(sender.send(work.await));
            drop(operation);
        }).await.map_err(|error| Error::Task(error.to_string()))?;
        receiver.await.map_err(|error| Error::Task(error.to_string()))?
    }
}

#[cfg(feature = "remote")]
pub(crate) struct Publication<'a> {
    region: &'a IoRegion,
    pending: Option<(Uuid, ObjectKey)>,
}

#[cfg(feature = "remote")]
impl Publication<'_> {
    pub(crate) fn confirm(mut self) { self.pending.take(); }
}

#[cfg(feature = "remote")]
impl Drop for Publication<'_> {
    fn drop(&mut self) {
        if let Some((attempt, key)) = self.pending.take() {
            self.region.pending_publications.lock().insert(attempt, key);
        }
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
        let (working, started) = tokio::sync::oneshot::channel();
        let (finish_work, work_release) = std::sync::mpsc::channel();
        let (dropping, drop_started) = tokio::sync::oneshot::channel();
        let (finish_drop, drop_release) = std::sync::mpsc::channel();
        let mut request = Box::pin(region.run_blocking(move || {
            working.send(()).unwrap(); work_release.recv().unwrap();
            Ok(HeldDrop { entered: Some(dropping), release: drop_release })
        }));
        poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap(); drop(request); finish_work.send(()).unwrap();
        drop_started.await.unwrap();
        let mut close = Box::pin(region.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        finish_drop.send(()).unwrap(); close.await;
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
