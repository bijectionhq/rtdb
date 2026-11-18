use super::*;
use std::{collections::VecDeque, future::Future, pin::Pin, sync::atomic::{AtomicBool, Ordering}, task::{Context, Poll}};
use rtdb_columnar::MemoryBudget;
use crate::connect::ConversionContext;

struct Source {
    frames: VecDeque<std::result::Result<Bytes, Error>>,
    pending: bool,
    dropped: Arc<AtomicBool>,
    _charge: Option<Arc<MemoryReservation>>,
}
impl Source {
    fn new(frames: impl IntoIterator<Item = std::result::Result<Bytes, Error>>) -> Self {
        Self { frames: frames.into_iter().collect(), pending: false, dropped: Arc::new(AtomicBool::new(false)), _charge: None }
    }
}
impl Stream for Source {
    type Item = std::result::Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.frames.pop_front() {
            Some(frame) => Poll::Ready(Some(frame)),
            None if self.pending => Poll::Pending,
            None => Poll::Ready(None),
        }
    }
}
impl Drop for Source {
    fn drop(&mut self) { self.dropped.store(true, Ordering::SeqCst); }
}

#[tokio::test]
async fn borrowed_windows_finish_the_owned_frame_before_demanding_a_later_error() {
    let source = Source::new([Ok(Bytes::from_static(b"abcdef")), Err(Error("Fixture read failed"))]);
    let mut input = BodyInput::new(source, 6);
    let window = NonZeroUsize::new(2).unwrap();
    assert_eq!(input.next_window(window).await.unwrap(), Some(b"ab".as_slice()));
    assert_eq!(input.next_window(window).await.unwrap(), Some(b"cd".as_slice()));
    assert_eq!(input.next_window(window).await.unwrap(), Some(b"ef".as_slice()));
    let failure = input.next_window(window).await.unwrap_err();
    assert_eq!(failure.message.as_deref(), Some("Fixture read failed"));
    assert_eq!(input.next_window(window).await.unwrap(), None);
}

#[tokio::test]
async fn empty_frames_and_zero_body_reach_stable_eof_without_a_data_window() {
    let source = Source::new([Ok(Bytes::new()), Ok(Bytes::new())]);
    let mut input = BodyInput::new(source, 0);
    let window = NonZeroUsize::new(1).unwrap();
    assert_eq!(input.next_window(window).await.unwrap(), None);
    assert_eq!(input.next_window(window).await.unwrap(), None);
    let memory = MemoryBudget::new(0);
    let response = RawResponse::collect(BodyInput::new(Source::new([]), 0), memory.reserve(0).unwrap()).await.unwrap();
    assert_eq!(response.bytes(), Some(b"".as_slice()));
    assert_eq!(RawResponse::NoContent.bytes(), None);
}

#[tokio::test]
async fn the_complete_arriving_frame_is_checked_before_any_of_its_bytes_are_exposed() {
    let mut input = BodyInput::new(Source::new([Ok(Bytes::from_static(b"12345"))]), 4);
    let window = NonZeroUsize::new(1).unwrap();
    let failure = input.next_window(window).await.unwrap_err();
    assert_eq!(failure.message.as_deref(), Some("Schema response exceeds its byte limit"));
    assert_eq!(input.next_window(window).await.unwrap(), None);

    let mut input = BodyInput::new(Source::new([Ok(Bytes::from_static(b"12")), Ok(Bytes::from_static(b"345"))]), 4);
    assert_eq!(input.next_window(window).await.unwrap(), Some(b"1".as_slice()));
    assert_eq!(input.next_window(window).await.unwrap(), Some(b"2".as_slice()));
    assert_eq!(input.next_window(window).await.unwrap_err(), failure);
    assert_eq!(input.next_window(window).await.unwrap(), None);
}

#[tokio::test]
async fn collection_retains_exact_duplicate_bytes_and_only_the_original_response_allowance() {
    let memory = MemoryBudget::new(1 << 16);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let maximum = 32;
    let charge = context.reserve(maximum * 4).unwrap();
    let source = Source::new([Ok(Bytes::from_static(br#"{"id":1,"#)), Ok(Bytes::new()), Ok(Bytes::from_static(br#""id":2}"#))]);
    let dropped = Arc::clone(&source.dropped);
    let response = RawResponse::collect(BodyInput::new(source, maximum), charge).await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    drop(context);
    assert_eq!(response.bytes(), Some(br#"{"id":1,"id":2}"#.as_slice()));
    assert_eq!(memory.used_bytes(), maximum * 4);
    drop(response);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn undersized_or_overflowing_allowance_fails_before_polling_the_source() {
    let memory = MemoryBudget::new(128);
    let source = futures::stream::poll_fn(|_| -> Poll<Option<std::result::Result<Bytes, Error>>> { panic!("source must not be demanded") });
    let failure = RawResponse::collect(BodyInput::new(source, 8), memory.reserve(31).unwrap()).await.err().unwrap();
    assert_eq!(failure.category, rtdb_types::AnomalyCategory::Fault);
    assert_eq!(failure.code, "xtdb.kafka-connect-source/registry-response-admission");
    assert_eq!(memory.used_bytes(), 0);
    let source = futures::stream::poll_fn(|_| -> Poll<Option<std::result::Result<Bytes, Error>>> { panic!("source must not be demanded") });
    let failure = RawResponse::collect(BodyInput::new(source, usize::MAX), memory.reserve(0).unwrap()).await.err().unwrap();
    assert!(failure.code.ends_with("/allocation-overflow"));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn fragmented_collection_admits_replacement_capacity_within_the_original_byte_bound() {
    let memory = MemoryBudget::new(256);
    let mut builder = RawBodyBuilder::new(31, memory.reserve(124).unwrap()).unwrap();
    let mut expected = Vec::new();
    for bytes in [b"a".as_slice(), b"bc", b"defgh", b"ijklmnop", b"qrstuvwxyz12345"] {
        builder.push(bytes).unwrap();
        expected.extend_from_slice(bytes);
        assert_eq!(builder.bytes, expected);
        assert!(builder.bytes.capacity() <= 31);
        assert_eq!(memory.used_bytes(), 124);
    }
    let failure = builder.push(b"6").unwrap_err();
    assert_eq!(failure.message.as_deref(), Some("Schema response exceeds its byte limit"));
    assert_eq!(builder.bytes, expected);
    drop(builder);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cancelling_partial_collection_releases_source_and_body_in_either_context_drop_order() {
    for context_first in [false, true] {
        let memory = MemoryBudget::new(1 << 16);
        let mut context = Some(ConversionContext::new(Arc::clone(&memory)));
        let charge = context.as_mut().unwrap().reserve(64).unwrap();
        let mut source = Source::new([Ok(Bytes::from_static(b"partial"))]);
        source.pending = true;
        source._charge = Some(memory.reserve(128).unwrap());
        let dropped = Arc::clone(&source.dropped);
        let mut collection = Box::pin(RawResponse::collect(BodyInput::new(source, 16), charge));
        let waker = futures::task::noop_waker();
        assert!(collection.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        assert_eq!(memory.used_bytes(), 192);
        assert!(!dropped.load(Ordering::SeqCst));
        if context_first { drop(context.take()); }
        assert_eq!(memory.used_bytes(), 192);
        drop(collection);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(memory.used_bytes(), if context_first { 0 } else { 64 });
        drop(context);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn a_read_failure_discards_partial_bytes_before_any_json_interpretation() {
    let memory = MemoryBudget::new(64);
    let source = Source::new([Ok(Bytes::from_static(b"{[")), Err(Error("Fixture read failed"))]);
    let dropped = Arc::clone(&source.dropped);
    let failure = RawResponse::collect(BodyInput::new(source, 16), memory.reserve(64).unwrap()).await.err().unwrap();
    assert_eq!(failure.message.as_deref(), Some("Fixture read failed"));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(memory.used_bytes(), 0);
}
