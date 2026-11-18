use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use rtdb_java_date::{DateClock, DateDisplay, DateEnvironment, DatePattern};
use rtdb_types::{Anomaly, MemoryBudget, Result};

use crate::admission::Admission;

use super::{T, Time, invalid, time::decode_long};

pub(crate) const HOST_CAPTURE: bool = cfg!(any(target_os = "linux", target_os = "macos"));
const INSTANT_PATTERN: &str = "yyyy-MM-dd'T'HH:mm:ss.SSS-00:00";
static MEMORY: LazyLock<Arc<MemoryBudget>> = LazyLock::new(|| MemoryBudget::new(16 << 20));
static INITIALIZATION: Mutex<()> = Mutex::new(());
static DISPLAY: OnceLock<DateDisplay> = OnceLock::new();
static PRINTED: OnceLock<Mutex<DatePattern>> = OnceLock::new();

struct ProcessClock;

impl DateClock for ProcessClock {
    fn now_millis(&self) -> i64 { chrono::Utc::now().timestamp_millis() }
}

fn captured<T>(slot: &'static OnceLock<T>, initialize: impl FnOnce() -> Result<T>) -> Result<&'static T> {
    if let Some(value) = slot.get() { return Ok(value); }
    let _initializing = INITIALIZATION.lock().map_err(|_| lock_error())?;
    if let Some(value) = slot.get() { return Ok(value); }
    let value = initialize()?;
    Ok(slot.get_or_init(|| value))
}

fn lock_error() -> Anomaly {
    Anomaly::fault("xtdb.expression/transit-date-text", "The process Date formatter lock was poisoned")
}

pub(crate) fn wire_millis(tag: &str, representation: &T) -> Result<i64> {
    match (tag, representation) {
        ("m", T::Integer(millis)) => Ok(*millis),
        ("m", T::String(text)) => decode_long(text),
        ("t", T::String(text)) => match Time::read_text(tag, text)? {
            Some(Time::LegacyDate(millis)) => Ok(millis),
            _ => Err(invalid()),
        },
        _ => Err(invalid()),
    }
}

pub(crate) fn plain(millis: i64, admission: Admission<'_>) -> Result<String> {
    let display = captured(&DISPLAY, || DateDisplay::capture_process(ProcessClock.now_millis(), &MEMORY))?;
    admission.reserve(display.maximum_output_bytes())?;
    display.format(millis)
}

pub(crate) fn readable(millis: i64, admission: Admission<'_>) -> Result<String> {
    let pattern = captured(&PRINTED, || {
        let clock = Arc::new(ProcessClock);
        let environment = DateEnvironment::capture_process(&MEMORY, clock.now_millis(), clock)?;
        Ok(Mutex::new(DatePattern::compile(INSTANT_PATTERN, environment, true)?))
    })?;
    let mut pattern = pattern.lock().map_err(|_| lock_error())?;
    let bound = pattern.maximum_output_bytes();
    admission.reserve(bound.checked_mul(2).and_then(|bytes| bytes.checked_add(128)).ok_or_else(crate::numeric::overflow)?)?;
    let value = pattern.format(millis)?;
    let mut text = String::with_capacity(8 + value.len());
    text.push_str("#inst \"");
    text.push_str(&value);
    text.push('"');
    Ok(text)
}
