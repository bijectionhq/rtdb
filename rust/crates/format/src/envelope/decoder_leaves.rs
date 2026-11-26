use crate::generated::xtdb::log::proto::{ReplicaLogMessage, SourceLogMessage};
use super::{PlacedAttempt, ReplicaBody, SourceBody, headered, invalid};
use prost::{Message, bytes::Bytes};

pub(super) type SourceBytes = Bytes;

pub(super) fn legacy_source(bytes: &[u8]) -> SourceBytes { Bytes::copy_from_slice(bytes) }
pub(super) use rtdb_types::Result;

pub(super) fn split_header(bytes: &[u8]) -> Option<(u8, &[u8])> {
    bytes.split_first().map(|(&header, body)| (header, body))
}

pub(super) fn unknown_header(header: u8, kind: &str) -> rtdb_types::Anomaly {
    invalid(format!("Unknown {kind} message header: {header}"))
}

pub(super) fn decode_source(bytes: &[u8]) -> Result<Option<SourceBody>> {
    let message = SourceLogMessage::decode(bytes).map_err(|error| invalid(error.to_string()))?;
    Ok(message.message)
}

pub(super) fn encode_source(message: &SourceBody) -> Vec<u8> {
    headered(&SourceLogMessage { message: Some(message.clone()) })
}

pub(super) fn decode_replica(bytes: &[u8]) -> Result<(Option<i64>, Option<i64>, Option<ReplicaBody>)> {
    let message = ReplicaLogMessage::decode(bytes).map_err(|error| invalid(error.to_string()))?;
    Ok((message.term_id, message.term_seq, message.message))
}

pub(super) fn encode_replica(term: i64, sequence: Option<i64>, message: &ReplicaBody) -> Vec<u8> {
    let wire = ReplicaLogMessage { message: Some(message.clone()), term_id: Some(term), term_seq: sequence };
    if !super::replica_uses_extension(message) { return headered(&wire); }
    let mut encoded = Vec::with_capacity(1 + crate::XTDB_REFUSAL_PREFIX.len() + wire.encoded_len());
    encoded.push(crate::RTDB_EXTENSION_LOG_HEADER);
    encoded.extend_from_slice(&crate::XTDB_REFUSAL_PREFIX);
    wire.encode(&mut encoded).expect("a Vec can hold the advertised encoded protobuf length");
    encoded
}

pub(super) fn check_stage(message: &Option<ReplicaBody>) -> Result<()> {
    if let Some(ReplicaBody::ResolvedTx(tx)) = message { tx.checked_acceptance_stage()?; }
    Ok(())
}

pub(super) fn decode_attempt(bytes: &[u8]) -> Result<PlacedAttempt> { PlacedAttempt::decode(bytes) }

pub(super) fn encode_attempt(attempt: PlacedAttempt) -> [u8; 96] { attempt.encode() }

pub(super) fn strip_prefix<'a>(bytes: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    bytes.strip_prefix(prefix)
}

pub(super) fn join_bytes(prefix: &[u8], body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(prefix.len() + body.len());
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(body);
    bytes
}
