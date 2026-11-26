//! Opt-in counters for Arrow ownership and representation boundaries.

use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub struct Measures {
    pub layout: Option<&'static str>,
    pub fallback_reason: Option<&'static str>,
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
    parent: Option<TraceParent>,
}

/// The W3C trace context of the request an operation serves, so events from
/// Bijection and RTDB join per request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceParent {
    trace: u128,
    span: u64,
    flags: u8,
}

impl TraceParent {
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('-');
        let (Some("00"), Some(trace), Some(span), Some(flags), None) =
            (parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if trace.len() != 32 || span.len() != 16 || flags.len() != 2 {
            return None;
        }
        Some(Self {
            trace: u128::from_str_radix(trace, 16).ok().filter(|trace| *trace != 0)?,
            span: u64::from_str_radix(span, 16).ok().filter(|span| *span != 0)?,
            flags: u8::from_str_radix(flags, 16).ok()?,
        })
    }
}

impl fmt::Display for TraceParent {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "00-{:032x}-{:016x}-{:02x}", self.trace, self.span, self.flags)
    }
}

thread_local! {
    static CURRENT: Cell<Option<TraceParent>> = const { Cell::new(None) };
}

/// The trace context installed on this thread by [`scope`].
pub fn current() -> Option<TraceParent> {
    CURRENT.with(Cell::get)
}

/// Runs `body` with `parent` as the current trace context, restoring the
/// previous one afterwards, including on unwind.
pub fn scope<R>(parent: Option<TraceParent>, body: impl FnOnce() -> R) -> R {
    struct Restore(Option<TraceParent>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|current| current.set(self.0));
        }
    }
    let _restore = Restore(CURRENT.with(|current| current.replace(parent)));
    body()
}

/// A future that keeps the trace context it was created under while it is
/// polled, for work moved to another task.
pub struct Traced<F> {
    inner: F,
    parent: Option<TraceParent>,
}

pub fn traced<F: Future>(inner: F) -> Traced<F> {
    Traced { inner, parent: current() }
}

impl<F: Future> Future for Traced<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let parent = self.parent;
        // SAFETY: `inner` is structurally pinned: it is never moved out of `Traced`.
        let inner = unsafe { self.map_unchecked_mut(|traced| &mut traced.inner) };
        scope(parent, || inner.poll(context))
    }
}

fn settings() -> Option<&'static Settings> {
    static SETTINGS: OnceLock<Option<Settings>> = OnceLock::new();
    SETTINGS
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
        .as_ref()
}

pub fn sample() -> Option<Sample> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let settings = settings()?;
    let operation = NEXT.fetch_add(1, Ordering::Relaxed);
    (operation % settings.every.get() == 0).then_some(Sample {
        session: settings.session,
        operation,
        parent: current(),
    })
}

struct Parent(Option<TraceParent>);

impl fmt::Display for Parent {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(parent) => fmt::Display::fmt(&parent, out),
            None => Ok(()),
        }
    }
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
        // One write per record keeps it whole when stdout shares the descriptor.
        let line = format!(
            concat!(
                "arrow-flow {{\"version\":1,\"process\":{},\"session\":\"{}\",\"operation\":{},",
                "\"edge\":\"{}\",\"traceparent\":\"{}\",\"rows\":{},\"encoded_bytes\":{},\"copied_bytes\":{},",
                "\"new_backing_bytes\":{},\"reservation_bytes\":{},",
                "\"input_buffer\":{},\"output_buffer\":{},\"layout\":\"{}\",\"fallback_reason\":\"{}\"}}\n"
            ),
            std::process::id(),
            self.session,
            self.operation,
            edge,
            Parent(self.parent),
            Number(measures.rows),
            Number(measures.encoded_bytes),
            Number(measures.copied_bytes),
            Number(measures.new_backing_bytes),
            Number(measures.reservation_bytes),
            Number(measures.input_buffer),
            Number(measures.output_buffer),
            measures.layout.unwrap_or_default(),
            measures.fallback_reason.unwrap_or_default(),
        );
        let _ = io::stderr().lock().write_all(line.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    #[test]
    fn traceparent_round_trips_and_rejects_invalid_headers() {
        let parent = TraceParent::parse(PARENT).unwrap();
        assert_eq!(parent.to_string(), PARENT);
        for invalid in [
            "",
            "01-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            "00-00000000000000000000000000000000-0123456789abcdef-01",
            "00-0123456789abcdef0123456789abcdef-0000000000000000-01",
            "00-0123456789abcdef0123456789abcde-0123456789abcdef-01",
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01-00",
            "00-0123456789abcdef0123456789abcdeg-0123456789abcdef-01",
        ] {
            assert_eq!(TraceParent::parse(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn scope_restores_the_enclosing_trace_context_even_on_unwind() {
        let outer = TraceParent::parse(PARENT);
        let inner = TraceParent::parse("00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-00");
        scope(outer, || {
            scope(inner, || assert_eq!(current(), inner));
            assert_eq!(current(), outer);
            assert!(std::panic::catch_unwind(|| scope(inner, || panic!("unwind"))).is_err());
            assert_eq!(current(), outer);
        });
        assert_eq!(current(), None);
    }

    #[test]
    fn traced_future_keeps_its_trace_context_when_polled_on_another_thread() {
        let parent = TraceParent::parse(PARENT);
        let mut future = scope(parent, || traced(std::future::poll_fn(|_| Poll::Ready(current()))));
        let seen = std::thread::spawn(move || {
            let mut context = Context::from_waker(std::task::Waker::noop());
            Pin::new(&mut future).poll(&mut context)
        })
        .join()
        .unwrap();
        assert_eq!(seen, Poll::Ready(parent));
    }
}
