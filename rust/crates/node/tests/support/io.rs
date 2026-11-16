use std::{future::{Future, poll_fn}, sync::{Arc, Mutex, mpsc}, task::Poll};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{ObjectKey, ObjectStore, ReadReservation};
use tokio::sync::oneshot;

struct HeldRead {
    entered: Option<oneshot::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
    _reservation: Arc<MemoryReservation>,
}
impl Drop for HeldRead {
    fn drop(&mut self) {
        self.entered.take().unwrap().send(()).unwrap();
        self.release.get_mut().unwrap().recv().unwrap();
    }
}

pub async fn hold_cancelled_local_read(store: &Arc<dyn ObjectStore>) -> mpsc::Sender<()> {
    let (entered, started) = oneshot::channel();
    let (release, waiting) = mpsc::channel();
    let memory = MemoryBudget::new(1024);
    let reservation = ReadReservation::new(1, HeldRead { entered: Some(entered), release: Mutex::new(waiting), _reservation: memory.reserve(1).unwrap() });
    let key = ObjectKey::new("missing-held-read").unwrap();
    let mut request = Box::pin(store.get_reserved_range(&key, 0..1, reservation));
    poll_fn(|context| { assert!(request.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    started.await.unwrap();
    drop(request);
    release
}
