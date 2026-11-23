use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_types::Result;
use crate::{ArrowPgTransaction, Decoder, Lsn, Remote, SourceConfig, config::{identifier, replication_literal}, decoder::PG_EPOCH_MICROS, token::UpstreamAcknowledgement, vector_types::VectorTypes, wire::{Connection, Input, protocol, server_error}};

pub(crate) struct ChangeStream { connection: Connection, decoder: Decoder, received: Lsn }
pub(crate) struct StreamUpdate { pub received: Lsn, pub transaction: Option<ArrowPgTransaction>, pub reply_requested: bool }
impl ChangeStream {
    pub async fn open(remote: &Remote, config: &SourceConfig, start: Lsn, memory: Arc<MemoryBudget>) -> Result<Self> {
        let mut connection = Connection::open(remote, true, Arc::clone(&memory)).await?;
        connection.query("SET IntervalStyle = 'iso_8601'").await?;
        let vectors = VectorTypes::load(&mut connection, &memory).await?;
        connection.start_copy(&format!("START_REPLICATION SLOT {} LOGICAL {start} (proto_version '1', publication_names {})", identifier(&config.slot_name)?, replication_literal(&config.publication_name)?)).await?;
        Ok(Self { connection, decoder: Decoder::new(config.ignore_tables.clone(), memory).with_vector_types(vectors), received: start })
    }
    pub async fn read(&mut self) -> Result<StreamUpdate> {
        loop {
            let frame = self.connection.read().await?;
            match frame.tag {
                b'd' => {
                    let mut input = Input::new(&frame.bytes);
                    return match input.u8()? {
                        b'w' => {
                            let start = Lsn(input.u64()?); input.u64()?; input.i64()?; self.received = self.received.max(start);
                            let transaction = self.decoder.receive_arrow(input.rest())?;
                            if let Some(transaction) = &transaction {
                                self.received = self.received.max(transaction.lsn);
                            }
                            Ok(StreamUpdate { received: self.received, transaction, reply_requested: false })
                        }
                        b'k' => { self.received = self.received.max(Lsn(input.u64()?)); input.i64()?; let reply_requested = input.u8()? != 0; input.end()?; Ok(StreamUpdate { received: self.received, transaction: None, reply_requested }) }
                        _ => Err(protocol("Unknown replication CopyData message")),
                    };
                }
                b'N' | b'S' => {},
                b'E' => return Err(server_error(&frame.bytes)?),
                b'c' | b'C' | b'Z' => return Err(crate::wire::network("PostgreSQL ended the logical replication stream")),
                _ => return Err(protocol("Unexpected backend message during logical replication")),
            }
        }
    }
    pub(crate) async fn acknowledge(&mut self, permit: &UpstreamAcknowledgement) -> Result<()> {
        let confirmed = permit.through();
        let time = chrono::Utc::now().timestamp_micros().checked_sub(PG_EPOCH_MICROS).ok_or_else(|| protocol("Standby status timestamp overflow"))?;
        let mut bytes = Vec::with_capacity(34); bytes.push(b'r'); bytes.extend_from_slice(&self.received.0.to_be_bytes()); bytes.extend_from_slice(&confirmed.0.to_be_bytes()); bytes.extend_from_slice(&confirmed.0.to_be_bytes()); bytes.extend_from_slice(&time.to_be_bytes()); bytes.push(0);
        self.connection.send(b'd', &bytes).await
    }
}
