use std::{future::{Future, poll_fn}, sync::Arc, task::Poll};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, oneshot};

tokio::task_local! { static VALUE: u64; }

#[tokio::test]
async fn task_local_is_installed_on_each_poll_and_spawn_starts_unbound() {
    let mut polls = 0;
    let work = poll_fn(|_| {
        assert_eq!(VALUE.try_with(|value| *value).ok(), Some(7));
        polls += 1;
        if polls == 1 { Poll::Pending } else { Poll::Ready(()) }
    });
    let mut scope = std::pin::pin!(VALUE.scope(7, work));
    VALUE.scope(3, poll_fn(|cx| {
        assert!(scope.as_mut().poll(cx).is_pending());
        assert_eq!(VALUE.try_with(|value| *value).ok(), Some(3));
        assert!(scope.as_mut().poll(cx).is_ready());
        assert_eq!(VALUE.try_with(|value| *value).ok(), Some(3));
        Poll::Ready(())
    })).await;
    assert!(VALUE.try_with(|_| ()).is_err());
    VALUE.scope(7, async {
        assert!(tokio::spawn(async { VALUE.try_with(|_| ()).is_err() }).await.unwrap());
    }).await;
}

#[tokio::test]
async fn detached_task_returns_its_permit_on_the_last_strong_drop() {
    let permits = Arc::new(Semaphore::new(1));
    let owner = Arc::new(Arc::clone(&permits).try_acquire_owned().unwrap());
    let retained = Arc::clone(&owner);
    let (finish, wait_for_finish) = oneshot::channel();
    let (released, wait_for_release) = oneshot::channel();
    let join = tokio::spawn(async move {
        wait_for_finish.await.unwrap();
        drop(retained);
        released.send(()).unwrap();
    });
    drop(join);
    drop(owner);
    assert_eq!(permits.available_permits(), 0);
    finish.send(()).unwrap();
    wait_for_release.await.unwrap();
    assert_eq!(permits.available_permits(), 1);
}

#[test]
fn mutex_guard_serializes_updates_to_the_same_allowance() {
    let allowance = Mutex::new(64);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..16 {
                    *allowance.lock() -= 1;
                }
            });
        }
    });
    assert_eq!(*allowance.lock(), 0);
}

#[tokio::test]
async fn pinned_poll_fn_keeps_the_owner_through_the_ready_poll() {
    let owner = Arc::new(());
    let witness = Arc::downgrade(&owner);
    let mut polls = 0;
    let mut drain = std::pin::pin!(super::hold_provider_custody(owner, poll_fn(|_| {
        assert!(witness.upgrade().is_some());
        polls += 1;
        if polls == 1 { Poll::Pending } else { Poll::Ready(()) }
    })));
    poll_fn(|cx| {
        assert!(drain.as_mut().poll(cx).is_pending());
        assert!(witness.upgrade().is_some());
        assert!(drain.as_mut().poll(cx).is_ready());
        assert!(witness.upgrade().is_none());
        Poll::Ready(())
    }).await;
}
