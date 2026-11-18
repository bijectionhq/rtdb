use std::{num::NonZeroUsize, sync::Arc};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, Result};
use crate::connect::error;
use super::Error;

pub(in crate::connect::registry) enum RawResponse { NoContent, Body(RawBody) }
pub(in crate::connect::registry) struct RawBody { bytes: Vec<u8>, _charge: Arc<MemoryReservation> }
impl RawResponse {
    /// The supplied charge must cover the byte vector's retained capacity through its final release.
    pub(in crate::connect::registry) fn body(bytes: Vec<u8>, charge: Arc<MemoryReservation>) -> Self {
        Self::Body(RawBody { bytes, _charge: charge })
    }
    pub(in crate::connect::registry) fn bytes(&self) -> Option<&[u8]> {
        match self { Self::NoContent => None, Self::Body(body) => Some(&body.bytes) }
    }
    pub(in crate::connect::registry) async fn collect<S>(mut input: BodyInput<S>, charge: Arc<MemoryReservation>) -> Result<Self>
    where S: Stream<Item = std::result::Result<Bytes, Error>> + Unpin {
        let mut builder = RawBodyBuilder::new(input.maximum, charge)?;
        let window = NonZeroUsize::new(8192).unwrap();
        while let Some(bytes) = input.next_window(window).await? { builder.push(bytes)?; }
        Ok(Self::body(builder.bytes, builder.charge))
    }
}

enum Window { Empty, Data { bytes: Bytes, position: usize }, End }
pub(in crate::connect::registry) struct BodyInput<S> { window: Window, source: S, received: usize, maximum: usize }
impl<S> BodyInput<S> where S: Stream<Item = std::result::Result<Bytes, Error>> + Unpin {
    pub(in crate::connect::registry) fn new(source: S, maximum: usize) -> Self {
        Self { window: Window::Empty, source, received: 0, maximum }
    }
    pub(in crate::connect::registry) async fn next_window(&mut self, maximum: NonZeroUsize) -> Result<Option<&[u8]>> {
        if matches!(self.window, Window::End) { return Ok(None); }
        let buffered = matches!(&self.window, Window::Data { bytes, position } if *position < bytes.len());
        if !buffered {
            self.window = Window::Empty;
            loop {
                let bytes = match self.source.next().await {
                    Some(Ok(bytes)) if bytes.is_empty() => continue,
                    Some(Ok(bytes)) => bytes,
                    Some(Err(cause)) => {
                        self.window = Window::End;
                        return Err(error("schema-registry", cause.without_url()));
                    }
                    None => { self.window = Window::End; return Ok(None); }
                };
                let Some(received) = self.received.checked_add(bytes.len()).filter(|received| *received <= self.maximum) else {
                    self.window = Window::End;
                    return Err(too_large());
                };
                self.received = received;
                self.window = Window::Data { bytes, position: 0 };
                break;
            }
        }
        let Window::Data { bytes, position } = &mut self.window else { unreachable!() };
        let start = *position;
        *position += maximum.get().min(bytes.len() - start);
        Ok(Some(&bytes[start..*position]))
    }
}

struct RawBodyBuilder { bytes: Vec<u8>, charge: Arc<MemoryReservation>, maximum: usize }
impl RawBodyBuilder {
    fn new(maximum: usize, charge: Arc<MemoryReservation>) -> Result<Self> {
        let bound = maximum.checked_mul(4).ok_or_else(overflow)?;
        if charge.bytes() < bound {
            return Err(Anomaly::fault("xtdb.kafka-connect-source/registry-response-admission", "Raw response reservation is smaller than its byte allowance"));
        }
        Ok(Self { bytes: Vec::new(), charge, maximum })
    }
    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        let required = self.bytes.len().checked_add(bytes.len()).filter(|required| *required <= self.maximum).ok_or_else(too_large)?;
        if required > self.bytes.capacity() {
            let capacity = self.bytes.capacity().checked_mul(2).ok_or_else(overflow)?.max(required).min(self.maximum);
            self.bytes.try_reserve_exact(capacity - self.bytes.len()).map_err(|_| error("allocation-failed", "Registry response allocation failed"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

fn overflow() -> Anomaly { error("allocation-overflow", "Schema response bound overflow") }
fn too_large() -> Anomaly { error("schema-registry", "Schema response exceeds its byte limit") }

#[cfg(test)]
mod tests;
