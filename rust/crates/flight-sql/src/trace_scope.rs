//! Installs a Flight request's W3C `traceparent` as the current Arrow-flow
//! trace context while its handler and its response body are polled, so
//! sampled events join the Bijection request that caused them.

use std::{future::Future, pin::Pin, task::{Context, Poll}};
use http_body::{Body, Frame, SizeHint};
use rtdb_columnar::arrow_flow::{self, TraceParent};
use tonic::{body::Body as TonicBody, codegen::{Service, http}, server::NamedService};

#[derive(Clone)]
pub(crate) struct TraceScope<S>(pub(crate) S);

impl<S: NamedService> NamedService for TraceScope<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<http::Request<B>> for TraceScope<S>
where
    S: Service<http::Request<B>, Response = http::Response<TonicBody>>,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    type Future = Scoped<S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let parent = request.headers().get("traceparent")
            .and_then(|value| value.to_str().ok()).and_then(TraceParent::parse);
        Scoped { inner: arrow_flow::scope(parent, || self.0.call(request)), parent }
    }
}

pub(crate) struct Scoped<F> {
    inner: F,
    parent: Option<TraceParent>,
}

impl<F, E> Future for Scoped<F>
where
    F: Future<Output = Result<http::Response<TonicBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let parent = self.parent;
        // SAFETY: `inner` is structurally pinned: it is never moved out of `Scoped`.
        let inner = unsafe { self.map_unchecked_mut(|scoped| &mut scoped.inner) };
        arrow_flow::scope(parent, || inner.poll(context)).map(|result| result.map(|response| match parent {
            // Streaming replies are produced while hyper polls the body, after the handler returned.
            Some(parent) => response.map(|body| TonicBody::new(ScopedBody { inner: body, parent })),
            None => response,
        }))
    }
}

struct ScopedBody {
    inner: TonicBody,
    parent: TraceParent,
}

impl Body for ScopedBody {
    type Data = <TonicBody as Body>::Data;
    type Error = <TonicBody as Body>::Error;

    fn poll_frame(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let parent = Some(self.parent);
        arrow_flow::scope(parent, || Pin::new(&mut self.inner).poll_frame(context))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::{future::poll_fn, sync::{Arc, Mutex}};
    use super::*;

    const PARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    /// Records the trace context seen by the handler and by each body poll.
    #[derive(Clone, Default)]
    struct Recording(Arc<Mutex<Vec<Option<TraceParent>>>>);

    struct RecordingBody(Recording, bool);

    impl Body for RecordingBody {
        type Data = tonic::codegen::Bytes;
        type Error = tonic::Status;

        fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.0.0.lock().unwrap().push(arrow_flow::current());
            if std::mem::replace(&mut self.1, true) { return Poll::Ready(None) }
            Poll::Ready(Some(Ok(Frame::data(tonic::codegen::Bytes::from_static(b"frame")))))
        }
    }

    impl Service<http::Request<()>> for Recording {
        type Response = http::Response<TonicBody>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }

        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            self.0.lock().unwrap().push(arrow_flow::current());
            std::future::ready(Ok(http::Response::new(TonicBody::new(RecordingBody(self.clone(), false)))))
        }
    }

    async fn serve(request: http::Request<()>) -> Vec<Option<TraceParent>> {
        let recording = Recording::default();
        let response = TraceScope(recording.clone()).call(request).await.unwrap();
        let mut body = response.into_body();
        while poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await.is_some() {}
        assert_eq!(arrow_flow::current(), None);
        let seen = recording.0.lock().unwrap().clone();
        seen
    }

    #[tokio::test]
    async fn handler_and_streamed_body_see_the_request_traceparent() {
        let request = http::Request::builder().header("traceparent", PARENT).body(()).unwrap();
        let parent = TraceParent::parse(PARENT);
        assert_eq!(serve(request).await, vec![parent; 3]);
    }

    #[tokio::test]
    async fn requests_without_a_valid_traceparent_carry_none() {
        let request = http::Request::builder().header("traceparent", "garbage").body(()).unwrap();
        assert_eq!(serve(request).await, vec![None; 3]);
    }
}
