//! Micrometer's `ProcessorMetrics` gauges, read from the operating system rather than a JVM bean.

use std::time::{Duration, Instant};

/// The previous CPU reading, from which the next one's recent usage is measured.
pub(crate) struct CpuSampler { previous: Option<CpuSample> }
#[derive(Clone, Copy)]
struct CpuSample { at: Instant, process: Duration, system: Option<SystemTicks> }
#[derive(Clone, Copy)]
struct SystemTicks { busy: u64, total: u64 }

/// `None` is a value this platform cannot report, exported as NaN.
pub(crate) struct ProcessorReading { pub system_cpu_usage: Option<f64>, pub process_cpu_usage: Option<f64>, pub load_average: Option<f64> }

impl CpuSampler {
    pub fn new() -> Self { Self { previous: sample() } }

    /// Usage since the previous reading, like the JDK's `getCpuLoad` and `getProcessCpuLoad`, normalised over every processor.
    pub fn read(&mut self, processors: usize) -> ProcessorReading {
        let current = sample();
        if self.previous.is_none() { self.previous = current; }
        let (system_cpu_usage, process_cpu_usage) = match (self.previous, current) {
            (Some(previous), Some(current)) if current.at > previous.at => {
                let wall = current.at.duration_since(previous.at).as_secs_f64() * processors.max(1) as f64;
                let process = current.process.saturating_sub(previous.process).as_secs_f64() / wall;
                let system = previous.system.zip(current.system).and_then(|(previous, current)| {
                    let total = current.total.checked_sub(previous.total).filter(|total| *total > 0)?;
                    Some(current.busy.saturating_sub(previous.busy) as f64 / total as f64)
                });
                self.previous = Some(current);
                (system.map(|usage| usage.clamp(0.0, 1.0)), Some(process.clamp(0.0, 1.0)))
            }
            _ => (None, None),
        };
        ProcessorReading { system_cpu_usage, process_cpu_usage, load_average: load_average() }
    }
}

fn sample() -> Option<CpuSample> {
    Some(CpuSample { at: Instant::now(), process: process_cpu_time()?, system: system_ticks() })
}

#[cfg(unix)]
fn process_cpu_time() -> Option<Duration> {
    // SAFETY: `getrusage` only writes the `rusage` it is given, which is plain data valid when zeroed.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 { return None; }
    let time = |value: libc::timeval| Duration::from_secs(value.tv_sec.max(0) as u64) + Duration::from_micros(value.tv_usec.max(0) as u64);
    Some(time(usage.ru_utime) + time(usage.ru_stime))
}
#[cfg(not(unix))]
fn process_cpu_time() -> Option<Duration> { None }

/// The aggregate `cpu` line of Linux's `/proc/stat`: user, nice, system, idle, iowait, irq, softirq and steal ticks.
fn system_ticks() -> Option<SystemTicks> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    parse_system_ticks(stat.lines().next()?)
}

fn parse_system_ticks(line: &str) -> Option<SystemTicks> {
    let mut fields = line.split_ascii_whitespace();
    if fields.next()? != "cpu" { return None; }
    let ticks = fields.take(8).map(|field| field.parse::<u64>().ok()).collect::<Option<Vec<_>>>()?;
    if ticks.len() < 4 { return None; }
    let total = ticks.iter().try_fold(0u64, |total, tick| total.checked_add(*tick))?;
    let idle = ticks[3].checked_add(ticks.get(4).copied().unwrap_or(0))?;
    Some(SystemTicks { busy: total.checked_sub(idle)?, total })
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn load_average() -> Option<f64> {
    let mut load = [0f64; 1];
    // SAFETY: the buffer holds the one sample requested.
    (unsafe { libc::getloadavg(load.as_mut_ptr(), 1) } == 1 && load[0] >= 0.0).then_some(load[0])
}
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn load_average() -> Option<f64> { None }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_stat_busy_ticks_exclude_idle_and_iowait() {
        let ticks = parse_system_ticks("cpu  10 2 3 80 5 0 0 0 0 0").unwrap();
        assert_eq!((ticks.busy, ticks.total), (15, 100));
        assert!(parse_system_ticks("cpu0 1 2 3 4").is_none());
        assert!(parse_system_ticks("cpu  x").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn process_usage_is_a_fraction_of_every_processor() {
        let mut sampler = CpuSampler::new();
        let started = Instant::now();
        let mut spin = 0u64;
        while started.elapsed() < Duration::from_millis(50) { spin = std::hint::black_box(spin.wrapping_add(1)); }
        let reading = sampler.read(1);
        let usage = reading.process_cpu_usage.expect("unix reports process CPU time");
        assert!(usage > 0.0 && usage <= 1.0, "{usage}");
        #[cfg(target_os = "linux")]
        assert!(reading.system_cpu_usage.is_some_and(|usage| (0.0..=1.0).contains(&usage)));
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        assert!(reading.load_average.is_some_and(|load| load >= 0.0));
    }
}
