use prost::Message;
use sha2::{Digest, Sha256};
use rtdb_types::{Anomaly, BlockIndex, PhysicalObjectVersion, PlacedAttempt, Result, S3ObjectNamespace, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::stage_journal::{InvalidStageJournal, checked_journal_fields};
use crate::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER};

mod decoder;
mod decoder_leaves;
mod wire;
pub use decoder::{ReplicaEnvelope, ReplicaFrame, SourceMessage, decode_replica_outcome_attempt, encode_replica_outcome_attempt};
use wire::{skip_wire, wire_key};

const ADMITTED_OFFLOAD_STORAGE: i32 = -1;

pub use crate::generated::xtdb::log::proto::replica_log_message::Message as ReplicaBody;
pub use crate::generated::xtdb::log::proto::source_log_message::Message as SourceBody;

impl SourceMessage {
    /// Classifies the source envelope without allocating its transaction payload.
    /// Validates outer protobuf framing; nested message validation belongs to `decode`.
    pub fn is_transaction_encoded(bytes: &[u8]) -> Result<bool> {
        let (&header, payload) = bytes.split_first().ok_or_else(|| invalid("Empty source message"))?;
        match header {
            LEGACY_TX_HEADER => return Ok(true),
            LEGACY_FLUSH_HEADER => return Ok(false),
            PROTOBUF_LOG_HEADER => (),
            _ => return Err(invalid(format!("Unknown source message header: {header}"))),
        }
        let mut transaction = false;
        let mut cursor = 0;
        while cursor < payload.len() {
            let (field, wire, body) = wire_key(payload, cursor).map_err(invalid)?;
            if matches!(field, 1..=4 | 6 | 7) {
                if wire != 2 { return Err(invalid("Source message body must be length-delimited")); }
                transaction = field == 6;
            }
            cursor = skip_wire(payload, field, wire, body, 0).map_err(invalid)?;
        }
        Ok(transaction)
    }

    pub fn encoded_len(&self) -> usize {
        match self { Self::LegacyTransaction(bytes) => bytes.len(), Self::Protobuf(message) => 1 + message.encoded_len() }
    }

    /// Legacy transaction bytes include their 0xff header.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        Ok(Self::decode_wire(bytes)?.filter(|message| !matches!(message,
            Self::Protobuf(SourceBody::FlushBlock(block)) if block.expected_block_idx.is_none())))
    }

    /// Decodes the stored message before applying the source log reader's processing rules.
    pub fn decode_wire(bytes: &[u8]) -> Result<Option<Self>> {
        decoder::decode_source_wire(bytes)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        decoder::encode_source_wire(self)
    }
}

pub struct AcceptanceStageWitness {
    pub source_position: SourcePosition,
    pub ordered_at: Timestamp,
    pub clock_upper: Timestamp,
    pub cutoff: Timestamp,
    /// The sealed original-journal cutoff and receiver clock certificate, for a physically bounded acceptance.
    pub journal: Option<rtdb_types::TimedJournalCutoff>,
}

impl crate::log::proto::ResolvedTx {
    /// Coordinates retained with a strict committed transaction. The sample
    /// is the writer's admission clock, before quorum durability.
    pub fn checked_acceptance_stage(&self) -> Result<Option<AcceptanceStageWitness>> {
        let Some(stage) = &self.acceptance_stage else { return Ok(None); };
        let (Some(upper), Some(cutoff), Some(source), Some(ordered_at)) =
            (stage.clock_upper_micros, stage.cutoff_micros, self.src_msg_id, self.system_time_micros)
        else { return Err(invalid("Incomplete strict acceptance stage")); };
        let attempt = self.placed_attempt.as_deref().ok_or_else(|| invalid("Strict acceptance stage has no placed attempt"))?;
        let (attempt, _) = decode_replica_outcome_attempt(attempt)?;
        if self.committed != Some(true) || self.tx_id != Some(source) || self.db_op.is_some()
            || attempt.position() != SourcePosition(source)
            || upper <= 0 || upper >= cutoff || ordered_at >= cutoff
        {
            return Err(invalid("Strict acceptance stage does not match a committed placed transaction"));
        }
        let journal = checked_journal_fields(cutoff, stage.journal_cutoff_micros, stage.clock_certificate_sha256.as_deref())
            .map_err(|error| invalid(match error {
                InvalidStageJournal::Incomplete => "Strict acceptance stage has an incomplete journal cutoff",
                InvalidStageJournal::DigestLength => "Invalid clock certificate digest length",
            }))?
            .map(|(before, certificate)| {
                let certificate = certificate.try_into().map_err(|_| invalid("Invalid clock certificate digest length"))?;
                rtdb_types::TimedJournalCutoff::new(Timestamp(before), certificate)
            }).transpose()?;
        Ok(Some(AcceptanceStageWitness {
            source_position: SourcePosition(source), ordered_at: Timestamp(ordered_at),
            clock_upper: Timestamp(upper), cutoff: Timestamp(cutoff), journal,
        }))
    }
}

impl ReplicaFrame {
    pub fn has_external_payload(bytes: &[u8]) -> Result<bool> {
        decoder::has_external_payload(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decoder::decode_replica_frame(bytes)
    }
}

impl ReplicaEnvelope {
    /// Allows for both term fields at their largest wire widths, before the appender has assigned them.
    pub fn maximum_encoded_len(message: &ReplicaBody) -> usize {
        Self::framed_len(message, u64::MAX, Some(u64::MAX))
    }

    pub fn encoded_len(&self) -> usize {
        Self::framed_len(&self.message, self.term.0 as u64, self.term_sequence.map(|sequence| sequence.0 as u64))
    }

    fn framed_len(message: &ReplicaBody, term: u64, sequence: Option<u64>) -> usize {
        1 + message.encoded_len() + 1 + varint_length(term)
            + sequence.map_or(0, |sequence| 1 + varint_length(sequence))
    }

    pub fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        decoder::decode_replica_envelope(bytes)
    }

    pub fn encode(&self) -> Vec<u8> {
        decoder::encode_replica_envelope(self)
    }

    pub fn with_sequence(mut self, sequence: TermSequence) -> Self {
        self.term_sequence = Some(sequence);
        self
    }

    pub fn offloaded(&self, version: StorageVersion, epoch: StorageEpoch, block: BlockIndex, payload_id: String, bytes: &[u8]) -> Self {
        Self {
            term: self.term,
            term_sequence: self.term_sequence,
            message: ReplicaBody::OversizedMessage(crate::log::proto::OversizedMessage {
                storage_version: Some(version.0),
                storage_epoch: Some(epoch.0),
                block_index: Some(block.0),
                payload_id: Some(payload_id),
                payload_sha256: Some(Sha256::digest(bytes).to_vec()),
                publication: None,
            }),
        }
    }

    /// The caller admits the envelope's term before reading the returned payload key.
    pub fn oversized_payload(&self, version: StorageVersion, epoch: StorageEpoch) -> Result<Option<String>> {
        let ReplicaBody::OversizedMessage(message) = &self.message else { return Ok(None) };
        let message_version = if message.storage_version == Some(ADMITTED_OFFLOAD_STORAGE) {
            self.offload_publication()?.ok_or_else(|| invalid("Admitted offload has no publication"))?.0
        } else { StorageVersion(message.storage_version.unwrap_or_default()) };
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

    pub fn offload_publication(&self) -> Result<Option<(StorageVersion, PhysicalObjectVersion)>> {
        let ReplicaBody::OversizedMessage(message) = &self.message else { return Ok(None) };
        if message.storage_version != Some(ADMITTED_OFFLOAD_STORAGE) { return Ok(None); }
        let publication = message.publication.as_ref().ok_or_else(|| invalid("Admitted offload has no publication"))?;
        let (Some(version), Some(bucket), Some(key), Some(version_id)) =
            (publication.storage_version, &publication.bucket, &publication.key, &publication.version_id)
            else { return Err(invalid("Admitted offload has an incomplete publication")); };
        if version < 0 || bucket.is_empty() || key.is_empty() || version_id.is_empty() || version_id == "null"
            || message.payload_sha256.as_ref().is_none_or(|digest| digest.len() != 32)
        {
            return Err(invalid("Admitted offload has an invalid publication"));
        }
        Ok(Some((StorageVersion(version), PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: bucket.clone(), endpoint: publication.endpoint.clone(), region: publication.region.clone() },
            key: key.clone(), version: version_id.clone(),
        })))
    }

    pub fn bind_offload_publication(&mut self, version: PhysicalObjectVersion) -> Result<()> {
        let ReplicaBody::OversizedMessage(message) = &mut self.message else {
            return Err(invalid("An object publication requires an oversized envelope"));
        };
        message.publication = Some(crate::log::proto::ReplicaPayloadPublication {
            storage_version: message.storage_version,
            bucket: Some(version.namespace.bucket), endpoint: version.namespace.endpoint, region: version.namespace.region,
            key: Some(version.key), version_id: Some(version.version),
        });
        message.storage_version = Some(ADMITTED_OFFLOAD_STORAGE);
        self.offload_publication().map(drop)
    }

    /// Checks the fetched record against the already-admitted envelope without admitting it a second time.
    pub fn unwrap_payload(&self, bytes: &[u8]) -> Result<Self> {
        let ReplicaBody::OversizedMessage(reference) = &self.message else {
            return Err(invalid("Only an oversized envelope has an external payload"));
        };
        if let Some(expected) = &reference.payload_sha256
            && expected.as_slice() != Sha256::digest(bytes).as_slice()
        {
            return Err(Anomaly::fault("rtdb.format/oversized-payload-mismatch", "Oversized payload bytes differ from their replica reference"));
        }
        let payload = Self::decode(bytes)?.ok_or_else(|| invalid("Oversized payload has no replica message"))?;
        if payload.term != self.term || payload.term_sequence != self.term_sequence {
            return Err(Anomaly::fault("rtdb.format/oversized-term-mismatch", "Oversized payload term or sequence differs from its envelope"));
        }
        if matches!(payload.message, ReplicaBody::OversizedMessage(_)) {
            return Err(invalid("Nested oversized replica messages are invalid"));
        }
        if matches!(&payload.message, ReplicaBody::ResolvedTx(tx) if tx.acceptance_stage.is_some()) {
            return Err(invalid("Strict acceptance requires an inline replica record"));
        }
        Ok(payload)
    }
}

fn varint_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 128 { value >>= 7; length += 1; }
    length
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
