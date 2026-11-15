use prost::Message;
use rtdb_types::{Anomaly, BlockIndex, Result, StorageEpoch, StorageVersion, Term, TermSequence};

use crate::generated::xtdb::log::proto::{ReplicaLogMessage, SourceLogMessage};
use crate::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER};

pub use crate::generated::xtdb::log::proto::replica_log_message::Message as ReplicaBody;
pub use crate::generated::xtdb::log::proto::source_log_message::Message as SourceBody;

#[derive(Clone, Debug, PartialEq)]
pub enum SourceMessage {
    LegacyTransaction(Vec<u8>),
    Protobuf(SourceBody),
}

impl SourceMessage {
    /// Legacy transaction bytes include their 0xff header.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        let (&header, payload) = bytes.split_first().ok_or_else(|| invalid("Empty source message"))?;
        match header {
            LEGACY_TX_HEADER => Ok(Some(Self::LegacyTransaction(bytes.to_vec()))),
            LEGACY_FLUSH_HEADER => Ok(None),
            PROTOBUF_LOG_HEADER => {
                let decoded = SourceLogMessage::decode(payload).map_err(|error| invalid(error.to_string()))?;
                match decoded.message {
                    Some(SourceBody::FlushBlock(ref block)) if block.expected_block_idx.is_none() => Ok(None),
                    body => Ok(body.map(Self::Protobuf)),
                }
            }
            _ => Err(invalid(format!("Unknown source message header: {header}"))),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Self::LegacyTransaction(bytes) => {
                if bytes.first() != Some(&LEGACY_TX_HEADER) {
                    return Err(invalid("Legacy transaction is missing its 0xff header"));
                }
                Ok(bytes.clone())
            }
            Self::Protobuf(message) => Ok(headered(&SourceLogMessage { message: Some(message.clone()) })),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaEnvelope {
    pub term: Term,
    pub term_sequence: Option<TermSequence>,
    pub message: ReplicaBody,
}

impl ReplicaEnvelope {
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        let (&header, payload) = bytes.split_first().ok_or_else(|| invalid("Empty replica message"))?;
        if header != PROTOBUF_LOG_HEADER {
            return Err(invalid(format!("Unknown replica message header: {header}")));
        }
        let wire = ReplicaLogMessage::decode(payload).map_err(|error| invalid(error.to_string()))?;
        Ok(wire.message.map(|message| Self {
            term: Term(wire.term_id.unwrap_or_default()),
            term_sequence: wire.term_seq.map(TermSequence),
            message,
        }))
    }

    pub fn encode(&self) -> Vec<u8> {
        headered(&ReplicaLogMessage {
            message: Some(self.message.clone()),
            term_id: Some(self.term.0),
            term_seq: self.term_sequence.map(|sequence| sequence.0),
        })
    }

    pub fn with_sequence(mut self, sequence: TermSequence) -> Self {
        self.term_sequence = Some(sequence);
        self
    }

    /// The caller admits the envelope's term before reading the returned payload key.
    pub fn oversized_payload(&self, version: StorageVersion, epoch: StorageEpoch) -> Result<Option<String>> {
        let ReplicaBody::OversizedMessage(message) = &self.message else { return Ok(None) };
        let message_version = StorageVersion(message.storage_version.unwrap_or_default());
        let message_epoch = StorageEpoch(message.storage_epoch.unwrap_or_default());
        if message_version != version || message_epoch != epoch {
            return Err(Anomaly::fault(
                "rtdb.format/oversized-storage-mismatch",
                format!("Oversized payload belongs to version {} epoch {}, reader is version {} epoch {}", message_version.0, message_epoch.0, version.0, epoch.0),
            ));
        }
        let key = crate::keys::oversized_key(
            BlockIndex(message.block_index.unwrap_or_default()),
            message.payload_id.as_deref().unwrap_or_default(),
        )?;
        Ok(Some(key))
    }

    /// Checks the fetched record against the already-admitted envelope without admitting it a second time.
    pub fn unwrap_payload(&self, bytes: &[u8]) -> Result<Self> {
        if !matches!(self.message, ReplicaBody::OversizedMessage(_)) {
            return Err(invalid("Only an oversized envelope has an external payload"));
        }
        let payload = Self::decode(bytes)?.ok_or_else(|| invalid("Oversized payload has no replica message"))?;
        if payload.term != self.term || payload.term_sequence != self.term_sequence {
            return Err(Anomaly::fault("rtdb.format/oversized-term-mismatch", "Oversized payload term or sequence differs from its envelope"));
        }
        if matches!(payload.message, ReplicaBody::OversizedMessage(_)) {
            return Err(invalid("Nested oversized replica messages are invalid"));
        }
        Ok(payload)
    }
}

fn headered<M: Message>(message: &M) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(message.encoded_len() + 1);
    encoded.push(PROTOBUF_LOG_HEADER);
    message.encode(&mut encoded).expect("a Vec can hold the advertised encoded protobuf length");
    encoded
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-log-message", message)
}
