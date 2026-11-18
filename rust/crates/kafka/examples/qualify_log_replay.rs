use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::{KafkaClusterConfig, KafkaLog, KafkaLogOptions, KafkaObserver, KafkaWarning};
use rtdb_storage::{Error as StorageError, log::{Log, LogOffset}, object_store::ReadReservation};
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

struct Observer;
impl KafkaObserver for Observer {
    fn warning(&self, warning: KafkaWarning) { eprintln!("{warning:?}"); }
}

fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition { Ok(()) } else { Err(message.into()) }
}

async fn qualify_delivery(log: &KafkaLog) -> Result<()> {
    let timeout = Duration::from_secs(10);
    require(log.latest()? == LogOffset(-1), "qualification requires an empty fixture topic")?;
    let first = log.enqueue(Bytes::from_static(b"first"), Timestamp(0)).await?.durable().await?;
    let second = log.enqueue(Bytes::from_static(b"second"), Timestamp(0)).await?.durable().await?;
    require((first.offset, second.offset) == (LogOffset(0), LogOffset(1)), "broker returned unexpected fixture offsets")?;

    let mut tail = log.tail(LogOffset(-1)).await?;
    let replay = tail.replay().ok_or("Kafka replay capability is unavailable")?;
    let head = replay.poll_head(timeout).await?.ok_or("expected the first fixture record")?;
    require(head.metadata.offset == first.offset && head.metadata.timestamp.0 % 1000 == 0
        && head.payload_len == 5 && head.allocation_bound == 261,
        "prepared Kafka offset, timestamp, or charge differs from the fixture")?;
    let charge = head.allocation_bound;
    let too_small = MemoryBudget::new(charge - 1);
    require(too_small.reserve(charge).is_err(), "caller admitted an oversized reservation")?;
    let refused = ReadReservation::new(charge - 1, too_small.reserve(charge - 1)?);
    require(matches!(replay.read_head(head, refused).await, Err(StorageError::Task(_))), "Kafka accepted a short read reservation")?;
    require(too_small.used_bytes() == 0, "refusal retained its charge")?;
    require(replay.poll_head(Duration::ZERO).await? == Some(head), "refusal changed the pending Kafka record")?;

    let memory = MemoryBudget::new(charge);
    let cancelled = ReadReservation::new(charge, memory.reserve(charge)?);
    drop(replay.read_head(head, cancelled));
    require(memory.used_bytes() == 0, "cancellation retained its charge")?;
    require(replay.poll_head(Duration::ZERO).await? == Some(head), "cancellation changed the pending Kafka record")?;
    let reservation = ReadReservation::new(charge, memory.reserve(charge)?);
    let record = replay.read_head(head, reservation).await?;
    require(record.metadata == head.metadata && record.payload.as_ref() == b"first", "delivered Kafka record differs from its prepared head")?;
    let clone = record.payload.clone();
    drop(record);
    require(memory.used_bytes() == charge, "Kafka payload clone lost its delivery charge")?;
    drop(clone);
    require(memory.used_bytes() == 0, "Kafka payload's final owner retained its delivery charge")?;
    drop(tail);

    let mut tail = log.tail(first.offset).await?;
    let replay = tail.replay().ok_or("Kafka replay capability is unavailable after reopen")?;
    let head = replay.poll_head(timeout).await?.ok_or("expected the second fixture record after reopening")?;
    let memory = MemoryBudget::new(head.allocation_bound);
    let reservation = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound)?);
    let record = replay.read_head(head, reservation).await?;
    require(record.metadata.offset == second.offset && record.metadata == head.metadata && record.payload.as_ref() == b"second",
        "reopened Kafka tail delivered an unexpected record")?;
    require(replay.poll_head(timeout).await?.is_none(), "Kafka did not reach the broker tip")?;
    Ok(())
}

async fn qualify_retention(log: &KafkaLog, first_retained: i64) -> Result<()> {
    let attempted = log.tail(LogOffset(first_retained - 2)).await;
    require(matches!(attempted, Err(StorageError::UnavailableLogRange { requested, first_retained: low })
        if requested == first_retained - 1 && low == first_retained), "Kafka did not report the broker's first retained position")?;
    let mut tail = log.tail(LogOffset(first_retained - 1)).await?;
    let replay = tail.replay().ok_or("Kafka replay capability is unavailable above the retained boundary")?;
    let head = replay.poll_head(Duration::from_secs(10)).await?.ok_or("qualification requires a record at the first retained position")?;
    require(head.metadata.offset == LogOffset(first_retained), "Kafka skipped the broker's first retained record")?;
    let memory = MemoryBudget::new(head.allocation_bound);
    let reservation = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound)?);
    let record = replay.read_head(head, reservation).await?;
    require(record.metadata == head.metadata, "Kafka changed the retained record during delivery")
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = "pass BROKER TOPIC [--properties-file FILE] [--first-retained OFFSET]";
    let mut args = std::env::args().skip(1);
    let mut cluster = KafkaClusterConfig::new(args.next().ok_or(usage)?);
    let topic = args.next().ok_or(usage)?;
    require(topic.starts_with("rtdb-replay-"), "use a separately provisioned rtdb-replay- fixture topic")?;
    let mut first_retained = None;
    while let Some(option) = args.next() {
        match option.as_str() {
            "--properties-file" if cluster.properties_file.is_none() => cluster.properties_file = Some(args.next().ok_or(usage)?.into()),
            "--first-retained" if first_retained.is_none() => {
                let offset = args.next().ok_or(usage)?.parse::<i64>()?;
                require(offset > 0, "first retained offset must be positive")?;
                first_retained = Some(offset);
            }
            _ => return Err(usage.into()),
        }
    }
    let properties = cluster.properties()?;
    require(properties.get("bootstrap.servers") == Some(&cluster.bootstrap_servers)
        && properties.get("metadata.broker.list").is_none_or(|endpoint| endpoint == &cluster.bootstrap_servers),
        "the properties file must preserve the explicit broker endpoint")?;
    cluster.properties_file = None;
    cluster.properties_map = properties;
    let mut options = KafkaLogOptions::new(topic);
    options.auto_create_topic = false;
    options.read_only = first_retained.is_some();
    let opened = KafkaLog::open(&cluster, options, Arc::new(Observer), MemoryBudget::new(64 << 20)).await?;
    if let Some(offset) = first_retained {
        require(opened.driver.is_none(), "retention qualification opened a writer")?;
        qualify_retention(&opened.log, offset).await?;
        println!("Kafka retained offset {offset} verified");
    } else {
        let cancellation = CancellationToken::new();
        let driver = opened.driver.ok_or("qualification could not open an append driver")?;
        let writer = tokio::spawn(driver.run(cancellation.clone()));
        let result = qualify_delivery(&opened.log).await;
        cancellation.cancel();
        let stopped = writer.await?;
        result?;
        stopped?;
        println!("Kafka offsets 0 and 1 verified with selected replay admission");
    }
    Ok(())
}
