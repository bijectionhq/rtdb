//! Serves `DoAction` with gRPC buffers sized to the call. The generated Flight
//! server runs every method through tonic's default prost codec, which
//! allocates an 8 KiB decode buffer and an 8 KiB encode buffer per call. Most
//! actions are far smaller. Here both buffers start small: the decoder reserves
//! each request's full length from its frame header, and the encoder grows to
//! fit each reply. Every other method goes through the generated server.

use std::{convert::Infallible, sync::Arc, task::{Context, Poll}};
use arrow_flight::{Action, Result as ActionResult, flight_service_server::{FlightService, FlightServiceServer}};
use tonic::{
    body::Body as TonicBody,
    codec::{BufferSettings, Codec},
    codegen::{BoxFuture, Service, StdError, http},
    server::{Grpc, NamedService, ServerStreamingService},
};
use tonic_prost::{ProstCodec, ProstDecoder, ProstEncoder};

const DO_ACTION: &str = "/arrow.flight.protocol.FlightService/DoAction";
const STARTING_BUFFER: usize = 256;
const YIELD_THRESHOLD: usize = 32 * 1024;

pub(crate) struct ActionRoute<T> {
    server: FlightServiceServer<T>,
    service: Arc<T>,
    decoding: usize,
    encoding: usize,
}

impl<T> ActionRoute<T> {
    /// `decoding` and `encoding` are the message limits the generated server enforces.
    pub(crate) fn new(service: T, decoding: usize, encoding: usize) -> Self {
        let service = Arc::new(service);
        let server = FlightServiceServer::from_arc(Arc::clone(&service))
            .max_decoding_message_size(decoding)
            .max_encoding_message_size(encoding);
        Self { server, service, decoding, encoding }
    }
}

impl<T> Clone for ActionRoute<T> {
    fn clone(&self) -> Self {
        Self { server: self.server.clone(), service: Arc::clone(&self.service), decoding: self.decoding, encoding: self.encoding }
    }
}

impl<T> NamedService for ActionRoute<T> {
    const NAME: &'static str = <FlightServiceServer<T> as NamedService>::NAME;
}

impl<T, B> Service<http::Request<B>> for ActionRoute<T>
where
    T: FlightService,
    B: http_body::Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() != DO_ACTION {
            return self.server.call(request);
        }
        let method = DoAction(Arc::clone(&self.service));
        let (decoding, encoding) = (self.decoding, self.encoding);
        Box::pin(async move {
            let mut grpc = Grpc::new(ActionCodec).apply_max_message_size_config(Some(decoding), Some(encoding));
            Ok(grpc.server_streaming(method, request).await)
        })
    }
}

struct DoAction<T>(Arc<T>);

impl<T: FlightService> ServerStreamingService<Action> for DoAction<T> {
    type Response = ActionResult;
    type ResponseStream = T::DoActionStream;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<Action>) -> Self::Future {
        let service = Arc::clone(&self.0);
        Box::pin(async move { service.do_action(request).await })
    }
}

struct ActionCodec;

impl Codec for ActionCodec {
    type Encode = ActionResult;
    type Decode = Action;
    type Encoder = ProstEncoder<ActionResult>;
    type Decoder = ProstDecoder<Action>;

    fn encoder(&mut self) -> Self::Encoder {
        ProstCodec::<ActionResult, Action>::raw_encoder(BufferSettings::new(STARTING_BUFFER, YIELD_THRESHOLD))
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstCodec::<ActionResult, Action>::raw_decoder(BufferSettings::new(STARTING_BUFFER, YIELD_THRESHOLD))
    }
}
