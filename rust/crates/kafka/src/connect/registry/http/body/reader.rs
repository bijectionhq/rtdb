use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use crate::connect::ConversionContext;
use super::{Anomaly, BodyInput, Bytes, Error, MemoryReservation, NonZeroUsize, ReadFailure, Result, Stream, error, overflow};

pub(in crate::connect::registry) struct BodyReader<S> {
    input: Option<BodyInput<S>>,
    runtime: Handle,
    cancellation: CancellationToken,
    _charge: std::sync::Arc<MemoryReservation>,
}

impl<S> BodyReader<S> where S: Stream<Item = std::result::Result<Bytes, Error>> + Unpin {
    pub(in crate::connect::registry) fn read(&mut self, destination: &mut [u8]) -> std::result::Result<usize, ReadFailure> {
        let Some(input) = &mut self.input else { return Ok(0); };
        let Some(maximum) = NonZeroUsize::new(destination.len()) else { return Ok(0); };
        let cancellation = &self.cancellation;
        self.runtime.block_on(async {
            let window = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(ReadFailure::Native(cancelled())),
                window = input.pull_window(maximum) => window?,
            };
            let Some(window) = window else { return Ok(0); };
            destination[..window.len()].copy_from_slice(window);
            Ok(window.len())
        })
    }

    pub(in crate::connect::registry) fn available(&self) -> i32 {
        self.input.as_ref().map_or(0, |input| i32::try_from(input.buffered()).unwrap_or(i32::MAX))
    }

    pub(in crate::connect::registry) fn close(&mut self) { self.input = None; }
}

pub(in crate::connect::registry) async fn decode_body<S, T, D>(
    input: BodyInput<S>, charge: std::sync::Arc<MemoryReservation>, region: &rtdb_storage::IoRegion,
    context: &mut ConversionContext, decode: D,
) -> Result<T>
where
    S: Stream<Item = std::result::Result<Bytes, Error>> + Send + Unpin + 'static,
    T: Send + 'static,
    D: FnOnce(&mut BodyReader<S>, &mut ConversionContext) -> Result<T> + Send + 'static,
{
    let task_bytes = std::mem::size_of::<(BodyReader<S>, D, T)>().checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)?;
    let mut parser = context.fork_parser(task_bytes)?;
    let token = CancellationToken::new();
    let _cancel_on_drop = token.clone().drop_guard();
    let runtime = Handle::current();
    let (result, parser) = region.run_blocking(move || {
        let mut reader = BodyReader { input: Some(input), runtime, cancellation: token, _charge: charge };
        let result = decode(&mut reader, &mut parser);
        Ok((result, parser))
    }).await.map_err(|cause| error("schema-registry", cause))?;
    context.adopt_parser(parser);
    result
}

fn cancelled() -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Interrupted,
        "xtdb.kafka-connect-source/registry-read-cancelled", "Registry response parser was cancelled")
}

#[cfg(test)]
mod tests;
