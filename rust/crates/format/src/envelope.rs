use prost::Message;
use sha2::{Digest, Sha256};
use rtdb_types::{Anomaly, BlockIndex, PhysicalObjectVersion, PlacedAttempt, Result, S3ObjectNamespace, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::generated::xtdb::log::proto::{ReplicaLogMessage, SourceLogMessage};
use crate::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER};

const ADMITTED_OFFLOAD_STORAGE: i32 = -1;

pub use crate::generated::xtdb::log::proto::replica_log_message::Message as ReplicaBody;
pub use crate::generated::xtdb::log::proto::source_log_message::Message as SourceBody;

#[derive(Clone, Debug, PartialEq)]
pub enum SourceMessage {
    LegacyTransaction(Vec<u8>),
    Protobuf(SourceBody),
}

impl SourceMessage {
    /// Classifies the source envelope without allocating its transaction payload.
    /// Validates outer protobuf framing; nested message validation belongs to `decode`.
    pub fn is_transaction_encoded(bytes: &[u8]) -> Result<bool> {
        let (&header, mut payload) = bytes.split_first().ok_or_else(|| invalid("Empty source message"))?;
        match header {
            LEGACY_TX_HEADER => return Ok(true),
            LEGACY_FLUSH_HEADER => return Ok(false),
            PROTOBUF_LOG_HEADER => (),
            _ => return Err(invalid(format!("Unknown source message header: {header}"))),
        }
        let mut transaction = false;
        while !payload.is_empty() {
            let (field, wire) = wire_key(&mut payload)?;
            if matches!(field, 1..=4 | 6 | 7) {
                if wire != 2 { return Err(invalid("Source message body must be length-delimited")); }
                transaction = field == 6;
            }
            skip_wire(&mut payload, field, wire, 0)?;
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
        let (&header, payload) = bytes.split_first().ok_or_else(|| invalid("Empty source message"))?;
        match header {
            LEGACY_TX_HEADER => Ok(Some(Self::LegacyTransaction(bytes.to_vec()))),
            LEGACY_FLUSH_HEADER => Ok(None),
            PROTOBUF_LOG_HEADER => {
                let decoded = SourceLogMessage::decode(payload).map_err(|error| invalid(error.to_string()))?;
                Ok(decoded.message.map(Self::Protobuf))
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

fn wire_varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut value = 0;
    for index in 0..10 {
        let (&byte, remaining) = bytes.split_first().ok_or_else(|| invalid("Truncated protobuf varint"))?;
        *bytes = remaining;
        if index == 9 && byte > 1 { return Err(invalid("Protobuf varint exceeds 64 bits")); }
        value |= u64::from(byte & 127) << (index * 7);
        if byte < 128 { return Ok(value); }
    }
    Err(invalid("Unterminated protobuf varint"))
}
fn wire_key(bytes: &mut &[u8]) -> Result<(u32, u8)> {
    let key = wire_varint(bytes)?;
    let field = u32::try_from(key >> 3).ok().filter(|field| (1..=0x1fff_ffff).contains(field)).ok_or_else(|| invalid("Invalid protobuf field number"))?;
    Ok((field, (key & 7) as u8))
}
fn skip_wire(bytes: &mut &[u8], field: u32, wire: u8, depth: usize) -> Result<()> {
    let length = match wire {
        0 => { wire_varint(bytes)?; return Ok(()); }
        1 => 8,
        2 => usize::try_from(wire_varint(bytes)?).map_err(|_| invalid("Protobuf field exceeds addressable memory"))?,
        3 => {
            if depth >= 100 { return Err(invalid("Protobuf group nesting exceeds limit")); }
            loop {
                let (nested_field, nested_wire) = wire_key(bytes)?;
                if nested_wire == 4 {
                    return if nested_field == field { Ok(()) } else { Err(invalid("Mismatched protobuf end group")) };
                }
                skip_wire(bytes, nested_field, nested_wire, depth + 1)?;
            }
        }
        5 => 4,
        _ => return Err(invalid("Invalid protobuf wire type")),
    };
    *bytes = bytes.get(length..).ok_or_else(|| invalid("Truncated protobuf field"))?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaEnvelope {
    pub term: Term,
    pub term_sequence: Option<TermSequence>,
    pub message: ReplicaBody,
}

pub struct ReplicaFrame {
    pub term: Term,
    pub term_sequence: Option<TermSequence>,
    pub message: Option<ReplicaBody>,
}

pub struct AcceptanceStageWitness {
    pub source_position: SourcePosition,
    pub ordered_at: Timestamp,
    pub clock_upper: Timestamp,
    pub cutoff: Timestamp,
    /// The sealed original-journal cutoff and receiver clock certificate, for a physically bounded acceptance.
    pub journal: Option<rtdb_types::TimedJournalCutoff>,
}

const RETIREMENT_MARKER: &[u8] = b"rtdb/placed-receipt-retirement/v1\0";

pub fn encode_replica_outcome_attempt(attempt: PlacedAttempt, retirement: bool) -> Vec<u8> {
    let bytes = attempt.encode();
    if !retirement { return bytes.to_vec(); }
    let mut encoded = Vec::with_capacity(RETIREMENT_MARKER.len() + bytes.len());
    encoded.extend_from_slice(RETIREMENT_MARKER);
    encoded.extend_from_slice(&bytes);
    encoded
}

pub fn decode_replica_outcome_attempt(bytes: &[u8]) -> Result<(PlacedAttempt, bool)> {
    match PlacedAttempt::decode(bytes) {
        Ok(attempt) => Ok((attempt, false)),
        Err(error) => {
            let attempt = bytes.strip_prefix(RETIREMENT_MARKER).ok_or(error)?;
            Ok((PlacedAttempt::decode(attempt)?, true))
        }
    }
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
        let journal = match (stage.journal_cutoff_micros, stage.clock_certificate_sha256.as_deref()) {
            (None, None) => None,
            (Some(before), Some(certificate)) if before >= cutoff => {
                let certificate = certificate.try_into().map_err(|_| invalid("Invalid clock certificate digest length"))?;
                Some(rtdb_types::TimedJournalCutoff::new(Timestamp(before), certificate)?)
            }
            _ => return Err(invalid("Strict acceptance stage has an incomplete journal cutoff")),
        };
        Ok(Some(AcceptanceStageWitness {
            source_position: SourcePosition(source), ordered_at: Timestamp(ordered_at),
            clock_upper: Timestamp(upper), cutoff: Timestamp(cutoff), journal,
        }))
    }
}

impl ReplicaFrame {
    pub fn has_external_payload(bytes: &[u8]) -> Result<bool> {
        let (&header, mut payload) = bytes.split_first().ok_or_else(|| invalid("Empty replica message"))?;
        if header != PROTOBUF_LOG_HEADER {
            return Err(invalid(format!("Unknown replica message header: {header}")));
        }
        while !payload.is_empty() {
            let (field, wire) = wire_key(&mut payload)?;
            skip_wire(&mut payload, field, wire, 0)?;
            if field == 8 && wire == 2 { return Ok(true); }
        }
        Ok(false)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (&header, payload) = bytes.split_first().ok_or_else(|| invalid("Empty replica message"))?;
        if header != PROTOBUF_LOG_HEADER {
            return Err(invalid(format!("Unknown replica message header: {header}")));
        }
        let wire = ReplicaLogMessage::decode(payload).map_err(|error| invalid(error.to_string()))?;
        if let Some(ReplicaBody::ResolvedTx(tx)) = &wire.message { tx.checked_acceptance_stage()?; }
        Ok(Self { term: Term(wire.term_id.unwrap_or_default()), term_sequence: wire.term_seq.map(TermSequence), message: wire.message })
    }

    pub fn into_envelope(self) -> Option<ReplicaEnvelope> {
        self.message.map(|message| ReplicaEnvelope { term: self.term, term_sequence: self.term_sequence, message })
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
        Ok(ReplicaFrame::decode(bytes)?.into_envelope())
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
