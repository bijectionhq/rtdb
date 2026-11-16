use std::sync::Arc;
use parking_lot::Mutex;
use tokio_util::task::{TaskTracker, TaskTrackerToken};
use crate::{Error, Result};

pub struct IoRegion { closed: Mutex<bool>, tasks: TaskTracker }
impl IoRegion {
    pub fn new() -> Arc<Self> { Arc::new(Self { closed: Mutex::new(false), tasks: TaskTracker::new() }) }
    pub(crate) fn admit(&self) -> Result<TaskTrackerToken> {
        let closed = self.closed.lock();
        if *closed { return Err(Error::Closed("I/O region is closed".into())); }
        Ok(self.tasks.token())
    }
    pub(crate) fn close(&self) { let mut closed = self.closed.lock(); *closed = true; self.tasks.close(); }
    pub(crate) async fn wait(&self) { self.tasks.wait().await; }
    /// Rejects new work and joins every admitted operation, including blocking tasks whose callers were cancelled.
    /// The owner awaits this before shutting down the Tokio runtime; dropping an I/O future only drops its waiter.
    pub async fn close_and_wait(&self) { self.close(); self.wait().await; }
    pub(crate) async fn run<T: Send + 'static>(&self, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let operation = self.admit()?;
        Self::run_admitted(operation, work).await
    }
    pub(crate) async fn run_admitted<T: Send + 'static>(operation: TaskTrackerToken, work: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let operation = operation;
            drop(sender.send(work()));
            drop(operation);
        }).await.map_err(|error| Error::Task(error.to_string()))?;
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
        let (working, started) = tokio::sync::oneshot::channel();
        let (finish_work, work_release) = std::sync::mpsc::channel();
        let (dropping, drop_started) = tokio::sync::oneshot::channel();
        let (finish_drop, drop_release) = std::sync::mpsc::channel();
        let mut request = Box::pin(region.run(move || {
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
        let result = region.run(|| Ok(Arc::new(()))).await.unwrap();
        let weak = Arc::downgrade(&result);
        region.close_and_wait().await;
        assert!(weak.upgrade().is_some());
        drop(result); assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn panicking_worker_releases_the_region_and_reports_the_join_failure() {
        let region = IoRegion::new();
        let result: Result<()> = region.run(|| panic!("injected I/O failure")).await;
        assert!(matches!(result, Err(Error::Task(_))));
        region.close_and_wait().await;
    }
}
