use std::time::Duration;

use rtdb_types::{Anomaly, Result};

pub(super) type ElapsedClock = dyn Fn() -> Option<Duration> + Send + Sync;

pub(super) const MAX_GRANT_MICROS: u64 = 5_000_000;
const CLOCK_RATE_BOUND_PPM: u64 = 1_000;
const PARTS_PER_MILLION: u64 = 1_000_000;
const LEGACY_CLOCK_OFFSET_MICROS: u64 = 250_000;

pub(super) const HOLDER_WINDOW: Duration = Duration::from_micros(
    MAX_GRANT_MICROS - rate_margin_micros(MAX_GRANT_MICROS),
);

pub(super) fn drain_window(legacy_grants: bool) -> Duration {
    let max_window = if legacy_grants {
        MAX_GRANT_MICROS + 2 * LEGACY_CLOCK_OFFSET_MICROS
    } else {
        MAX_GRANT_MICROS
    };
    Duration::from_micros(max_window + rate_margin_micros(max_window))
}

const fn rate_margin_micros(window: u64) -> u64 {
    (window * CLOCK_RATE_BOUND_PPM).div_ceil(PARTS_PER_MILLION)
}

pub(super) async fn wait_out(clock: &ElapsedClock, window: Duration) -> Result<()> {
    let started = clock().ok_or_else(unavailable)?;
    loop {
        let elapsed = clock()
            .and_then(|now| now.checked_sub(started))
            .ok_or_else(unavailable)?;
        let Some(remaining) = window.checked_sub(elapsed).filter(|remaining| !remaining.is_zero()) else {
            return Ok(());
        };
        // Tokio's timer may pause in suspend. Sample the continuous clock
        // again shortly after resume.
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn continuous_now() -> Option<Duration> {
    #[cfg(target_os = "linux")]
    let clock = libc::CLOCK_BOOTTIME;
    #[cfg(target_os = "macos")]
    let clock = libc::CLOCK_MONOTONIC_RAW;

    let mut sample = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // clock_gettime writes only to the live, uniquely borrowed timespec.
    if unsafe { libc::clock_gettime(clock, &mut sample) } != 0 {
        return None;
    }
    let seconds = u64::try_from(sample.tv_sec).ok()?;
    let nanos = u32::try_from(sample.tv_nsec).ok()?;
    (nanos < 1_000_000_000).then(|| Duration::new(seconds, nanos))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn continuous_now() -> Option<Duration> {
    None
}

pub(super) fn unavailable() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/activation-clock",
        "Activation requires a suspend-aware monotonic clock with a commissioned rate bound",
    )
}
