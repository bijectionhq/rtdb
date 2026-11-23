use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use bytes::Bytes;
use rdkafka::{ClientContext, error::{KafkaError, RDKafkaErrorCode}, producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext, PurgeConfig}};
use rtdb_storage::{Error, Result, log::LogMetadata, sealing::ClassicObjectKeys};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use crate::{ConfiguredLogClient, KafkaLogOptions, Progress, metadata, transport_error};

pub(crate) struct Delivery { pub reply: oneshot::Sender<Result<LogMetadata>>, pub _allocation: Arc<(OwnedSemaphorePermit, OwnedSemaphorePermit)> }
pub(crate) struct Queued { pub payload: Bytes, pub accepted: oneshot::Sender<Result<()>>, pub completion: Box<Delivery> }
pub(crate) struct DeliveryContext { pub progress: Arc<Progress>, pub topic: String, pub keys: Option<Arc<ClassicObjectKeys>> }
impl ClientContext for DeliveryContext {}
impl ProducerContext for DeliveryContext {
    type DeliveryOpaque = Box<Delivery>;
    fn delivery(&self, result: &DeliveryResult<'_>, completion: Self::DeliveryOpaque) {
        let result = match result {
            Ok(message) => metadata(message, &self.topic).and_then(|metadata| {
                if let Some(keys) = &self.keys {
                    let frame = rdkafka::Message::payload(message)
                        .ok_or_else(|| Error::Task("Kafka acknowledged a missing sealed frame".into()))?;
                    keys.record_kafka_publication(metadata.offset.0, frame)?;
                }
                Ok(metadata)
            }),
            Err((error, _)) => Err(transport_error(&self.topic, error.clone())),
        };
        if let Ok(metadata) = &result { self.progress.advance(metadata.offset.0); }
        let _ = completion.reply.send(result);
    }
}

pub struct KafkaLogDriver { producer: ConfiguredLogClient<BaseProducer<DeliveryContext>>, queue: mpsc::Receiver<Queued>, progress: Arc<Progress>, options: KafkaLogOptions, bytes: Arc<Semaphore>, records: Arc<Semaphore> }

impl KafkaLogDriver {
    pub(crate) fn new(producer: ConfiguredLogClient<BaseProducer<DeliveryContext>>, queue: mpsc::Receiver<Queued>, progress: Arc<Progress>, options: KafkaLogOptions, bytes: Arc<Semaphore>, records: Arc<Semaphore>) -> Self { Self { producer, queue, progress, options, bytes, records } }

    /// Shutdown rejects queued records and drains broker acknowledgements before returning.
    /// A delivery failure after submission can have an unknown broker outcome and must be reconciled by the replication protocol.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut pending: Option<Queued> = None;
        loop {
            if cancellation.is_cancelled() { break; }
            self.producer.poll(Duration::ZERO);
            if let Some(queued) = pending.take() {
                let Queued { payload, accepted, completion } = queued;
                let record = BaseRecord::<(), [u8], Box<Delivery>>::with_opaque_to(&self.options.topic, completion).partition(0).payload(payload.as_ref());
                match self.producer.send(record) {
                    Ok(()) => { let _ = accepted.send(Ok(())); }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), record)) => {
                        let completion = record.delivery_opaque;
                        pending = Some(Queued { payload, accepted, completion });
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge), _record)) => {
                        let _ = accepted.send(Err(Error::MessageTooLarge { size: payload.len(), maximum: self.options.maximum_message_bytes }));
                    }
                    Err((error, record)) => {
                        let _ = accepted.send(Err(transport_error(&self.options.topic, error.clone())));
                        let _ = record.delivery_opaque.reply.send(Err(transport_error(&self.options.topic, error)));
                    }
                }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                queued = self.queue.recv(), if pending.is_none() => match queued { Some(queued) => pending = Some(queued), None => break },
                _ = tokio::time::sleep(self.options.poll_interval) => (),
            }
        }
        self.progress.closed.store(true, Ordering::Release);
        self.bytes.close();
        self.records.close();
        self.queue.close();
        if let Some(queued) = pending { let _ = queued.completion.reply.send(Err(Error::Closed("Kafka shutdown before submission".into()))); }
        while let Ok(queued) = self.queue.try_recv() { let _ = queued.completion.reply.send(Err(Error::Closed("Kafka shutdown before submission".into()))); }
        let result = self.producer.flush(self.options.shutdown_timeout).map_err(|error| transport_error(&self.options.topic, error));
        if result.is_err() { self.producer.purge(PurgeConfig::default().queue().inflight()); self.producer.poll(Duration::ZERO); }
        result
    }
}

impl Drop for KafkaLogDriver {
    fn drop(&mut self) {
        self.progress.closed.store(true, Ordering::Release);
        self.bytes.close();
        self.records.close();
    }
}
