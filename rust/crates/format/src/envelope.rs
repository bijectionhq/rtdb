use prost::{Message, bytes::{BufMut, Bytes}, encoding::int64};
use sha2::{Digest, Sha256};
use rtdb_types::{Anomaly, BlockIndex, PhysicalObjectVersion, PlacedAttempt, Result, S3ObjectNamespace, SourcePosition, StorageEpoch, StorageVersion, Term, TermSequence, Timestamp};

use crate::stage_journal::{InvalidStageJournal, checked_journal_fields};
use crate::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER};

mod decoder;
mod decoder_leaves;
mod detached;
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
            if matches!(field, 1..=4 | 6..=9) {
                if wire != 2 { return Err(invalid("Source message body must be length-delimited")); }
                transaction = matches!(field, 6 | 9);
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
        Self::processed(Self::decode_wire(bytes))
    }

    pub fn decode_bytes(bytes: Bytes) -> Result<Option<Self>> {
        Self::processed(Self::decode_wire_bytes(bytes))
    }

    fn processed(message: Result<Option<Self>>) -> Result<Option<Self>> {
        Ok(message?.filter(|message| !matches!(message,
            Self::Protobuf(SourceBody::FlushBlock(block)) if block.expected_block_idx.is_none())))
    }

    /// Decodes the stored message before applying the source log reader's processing rules.
    pub fn decode_wire(bytes: &[u8]) -> Result<Option<Self>> {
        decoder::decode_source_wire(bytes)
    }

    pub fn decode_wire_bytes(bytes: Bytes) -> Result<Option<Self>> {
        match bytes.first().copied() {
            Some(LEGACY_TX_HEADER) => Ok(Some(Self::LegacyTransaction(bytes))),
            Some(LEGACY_FLUSH_HEADER) => Ok(None),
            Some(PROTOBUF_LOG_HEADER) => {
                let message = crate::log::proto::SourceLogMessage::decode(bytes.slice(1..)).map_err(|error| invalid(error.to_string()))?;
                Ok(message.message.map(Self::Protobuf))
            }
            Some(header) => Err(decoder_leaves::unknown_header(header, "source")),
            None => Err(invalid("Empty source message")),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        decoder::encode_source_wire(self)
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
fn peek_key(bytes: &mut &[u8]) -> Result<(u32, u8)> {
    let key = wire_varint(bytes)?;
    let field = u32::try_from(key >> 3).ok().filter(|field| (1..=0x1fff_ffff).contains(field)).ok_or_else(|| invalid("Invalid protobuf field number"))?;
    Ok((field, (key & 7) as u8))
}
#[derive(Debug, PartialEq, Eq)]
pub struct ReplicaPeek<'a> {
    pub term: Term,
    pub term_sequence: Option<TermSequence>,
    pub body: ReplicaPeekBody<'a>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReplicaPeekBody<'a> {
    Resolved { tx_id: Option<i64>, source: Option<i64>, time: Option<i64>, search_delta: Option<&'a [u8]> },
    NoOp(Option<i64>),
}

impl<'a> ReplicaPeek<'a> {
    /// Projects the common wire shapes. Other shapes go through the full decoder, including malformed records.
    pub fn read(bytes: &'a [u8]) -> Option<Self> {
        let (&PROTOBUF_LOG_HEADER, mut wire) = bytes.split_first()? else { return None; };
        let mut term = Term(0);
        let mut term_sequence = None;
        let mut body = None;
        while !wire.is_empty() {
            match peek_key(&mut wire).ok()? {
                (7, 0) => term = Term(wire_varint(&mut wire).ok()? as i64),
                (9, 0) => term_sequence = Some(TermSequence(wire_varint(&mut wire).ok()? as i64)),
                (1, 2) if body.is_none() => body = Some(Self::resolved(wire_bytes(&mut wire)?)?),
                (5, 2) if body.is_none() => body = Some(ReplicaPeekBody::NoOp(Self::noop(wire_bytes(&mut wire)?)?)),
                _ => return None,
            }
        }
        Some(Self { term, term_sequence, body: body? })
    }

    fn resolved(mut wire: &'a [u8]) -> Option<ReplicaPeekBody<'a>> {
        let (mut tx_id, mut source, mut time, mut search_delta) = (None, None, None, None);
        while !wire.is_empty() {
            match peek_key(&mut wire).ok()? {
                (1, 0) => tx_id = Some(wire_varint(&mut wire).ok()? as i64),
                (2, 0) => time = Some(wire_varint(&mut wire).ok()? as i64),
                (3, 0) => { wire_varint(&mut wire).ok()?; }
                (4 | 8, 2) => { wire_bytes(&mut wire)?; }
                (5, 2) => Self::table_entry(wire_bytes(&mut wire)?)?,
                (9, 0) => source = Some(wire_varint(&mut wire).ok()? as i64),
                (12, 2) => search_delta = Some(wire_bytes(&mut wire)?),
                _ => return None,
            }
        }
        Some(ReplicaPeekBody::Resolved { tx_id, source, time, search_delta })
    }

    fn table_entry(mut wire: &[u8]) -> Option<()> {
        while !wire.is_empty() {
            match peek_key(&mut wire).ok()? {
                (1, 2) => { std::str::from_utf8(wire_bytes(&mut wire)?).ok()?; }
                (2, 2) => { wire_bytes(&mut wire)?; }
                _ => return None,
            }
        }
        Some(())
    }

    fn noop(mut wire: &[u8]) -> Option<Option<i64>> {
        let mut source = None;
        while !wire.is_empty() {
            if peek_key(&mut wire).ok()? != (1, 0) { return None; }
            source = Some(wire_varint(&mut wire).ok()? as i64);
        }
        Some(source)
    }
}

fn wire_bytes<'a>(wire: &mut &'a [u8]) -> Option<&'a [u8]> {
    let length = usize::try_from(wire_varint(wire).ok()?).ok()?;
    let (field, remaining) = wire.split_at_checked(length)?;
    *wire = remaining;
    Some(field)
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

    pub fn decode_bytes(bytes: Bytes) -> Result<Self> {
        let Some(&header) = bytes.first() else { return Err(invalid("Empty replica message")); };
        if header != PROTOBUF_LOG_HEADER { return Err(decoder_leaves::unknown_header(header, "replica")); }
        let message = crate::log::proto::ReplicaLogMessage::decode(bytes.slice(1..)).map_err(|error| invalid(error.to_string()))?;
        decoder_leaves::check_stage(&message.message)?;
        Ok(Self { term: Term(message.term_id.unwrap_or_default()), term_sequence: message.term_seq.map(TermSequence), message: message.message })
    }
}

impl ReplicaEnvelope {
    /// Allows for both term fields at their largest wire widths, before the appender has assigned them.
    pub fn maximum_encoded_len(message: &ReplicaBody) -> usize {
        Self::framed_len(message, u64::MAX, Some(u64::MAX))
    }

    pub fn encoded_len(&self) -> usize {
        Self::encoded_body_len(self.term, self.term_sequence, &self.message)
    }

    pub fn encoded_body_len(term: Term, sequence: Option<TermSequence>, message: &ReplicaBody) -> usize {
        Self::framed_len(message, term.0 as u64, sequence.map(|sequence| sequence.0 as u64))
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

    pub fn encode_body(term: Term, sequence: Option<TermSequence>, message: &ReplicaBody, output: &mut impl BufMut) {
        output.put_u8(PROTOBUF_LOG_HEADER);
        // Prost sorts the oneof by its smallest tag, ahead of both term fields.
        message.encode(output);
        int64::encode(7, &term.0, output);
        if let Some(sequence) = sequence { int64::encode(9, &sequence.0, output); }
    }

    pub fn with_sequence(mut self, sequence: TermSequence) -> Self {
        self.term_sequence = Some(sequence);
        self
    }

    pub fn offloaded(&self, version: StorageVersion, epoch: StorageEpoch, block: BlockIndex, payload_id: String, bytes: &[u8]) -> Self {
        Self::offloaded_body(self.term, self.term_sequence, version, epoch, block, payload_id, bytes)
    }

    pub fn offloaded_body(term: Term, term_sequence: Option<TermSequence>, version: StorageVersion, epoch: StorageEpoch, block: BlockIndex, payload_id: String, bytes: &[u8]) -> Self {
        Self {
            term,
            term_sequence,
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
