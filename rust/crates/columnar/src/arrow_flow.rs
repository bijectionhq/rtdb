//! Opt-in counters for Arrow ownership and representation boundaries.

use std::fmt;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub struct Measures {
    pub layout: Option<&'static str>,
    pub rows: Option<usize>,
    pub encoded_bytes: Option<usize>,
    pub copied_bytes: Option<usize>,
    pub new_backing_bytes: Option<usize>,
    pub reservation_bytes: Option<usize>,
    pub input_buffer: Option<usize>,
    pub output_buffer: Option<usize>,
}

struct Settings {
    every: NonZeroU64,
    session: u128,
}

#[derive(Clone, Copy)]
pub struct Sample {
    session: u128,
    operation: u64,
}

pub fn sample() -> Option<Sample> {
    static SETTINGS: OnceLock<Option<Settings>> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let settings = SETTINGS
        .get_or_init(|| {
            let every = std::env::var("BIJECTION_ARROW_FLOW_SAMPLE_EVERY")
                .ok()?
                .parse()
                .ok()?;
            let session = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_nanos();
            Some(Settings { every, session })
        })
        .as_ref()?;
    let operation = NEXT.fetch_add(1, Ordering::Relaxed);
    (operation % settings.every.get() == 0).then_some(Sample {
        session: settings.session,
        operation,
    })
}

struct Number(Option<usize>);

impl fmt::Display for Number {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => fmt::Display::fmt(&value, out),
            None => out.write_str("null"),
        }
    }
}

impl Sample {
    /// Buffer numbers are scoped to this operation. A number can follow the
    /// same backing through a slice or ownership transfer.
    pub fn record(self, edge: &'static str, measures: Measures) {
        let _ = writeln!(
            io::stderr().lock(),
            concat!(
                "arrow-flow {{\"version\":1,\"process\":{},\"session\":\"{}\",\"operation\":{},",
                "\"edge\":\"{}\",\"rows\":{},\"encoded_bytes\":{},\"copied_bytes\":{},",
                "\"new_backing_bytes\":{},\"reservation_bytes\":{},",
                "\"input_buffer\":{},\"output_buffer\":{},\"layout\":\"{}\"}}"
            ),
            std::process::id(),
            self.session,
            self.operation,
            edge,
            Number(measures.rows),
            Number(measures.encoded_bytes),
            Number(measures.copied_bytes),
            Number(measures.new_backing_bytes),
            Number(measures.reservation_bytes),
            Number(measures.input_buffer),
            Number(measures.output_buffer),
            measures.layout.unwrap_or_default(),
        );
    }
}
