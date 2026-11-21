use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}},
};

#[derive(Default)]
pub struct QueryCpu {
    enabled: AtomicBool,
    nanos: AtomicU64,
}

impl QueryCpu {
    pub fn observe(&self) -> Option<u64> {
        if !self.enabled.load(Ordering::Relaxed) {
            thread_cpu_nanos()?;
            self.enabled.store(true, Ordering::Relaxed);
        }
        Some(self.nanos.load(Ordering::Relaxed))
    }

    pub fn is_observed(&self) -> bool { self.enabled.load(Ordering::Relaxed) }
}

pub(crate) fn measure<T>(counter: Arc<QueryCpu>, mut work: Pin<&mut impl Future<Output = T>>) -> impl Future<Output = T> + '_ {
    poll_fn(move |context| {
        let start = thread_cpu_nanos();
        let result = work.as_mut().poll(context);
        if let Some(elapsed) = start.zip(thread_cpu_nanos()).and_then(|(start, end)| end.checked_sub(start)) {
            counter.nanos.fetch_add(elapsed, Ordering::Relaxed);
        }
        result
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn thread_cpu_nanos() -> Option<u64> {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // clock_gettime writes only to the valid, uniquely borrowed timespec.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    if status != 0 { return None; }
    u64::try_from(time.tv_sec).ok()?.checked_mul(1_000_000_000)?.checked_add(u64::try_from(time.tv_nsec).ok()?)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn thread_cpu_nanos() -> Option<u64> { None }
