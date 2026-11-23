use super::{PlacedAttempt, ReplicaBody, SourceBody, Term, TermSequence, invalid};
use super::decoder_leaves::{
    Result, check_stage, decode_attempt, decode_replica, decode_source, encode_attempt, encode_replica, encode_source, join_bytes,
    legacy_source, SourceBytes, split_header, strip_prefix, unknown_header,
};
use super::wire::scan_external_payload;
use crate::{LEGACY_FLUSH_HEADER, LEGACY_TX_HEADER, PROTOBUF_LOG_HEADER};

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use super::decoder_leaves::{attempt_bytes, attempt_from_bytes, replica_bytes, replica_from_bytes, source_bytes, source_from_bytes, valid_stage};
#[cfg(verus_keep_ghost)]
use super::wire::scan_complete;

pub const RETIREMENT_MARKER: &[u8; 34] = b"rtdb/placed-receipt-retirement/v1\0";

#[cfg_attr(not(verus_keep_ghost), derive(Clone, Debug, PartialEq))]
pub enum SourceMessage {
    LegacyTransaction(SourceBytes),
    Protobuf(SourceBody),
}

#[cfg_attr(not(verus_keep_ghost), derive(Clone, Debug, PartialEq))]
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

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Ok(Some(SourceMessage::LegacyTransaction(raw))) =>
            bytes@.len() > 0 && bytes@[0] == LEGACY_TX_HEADER && raw@ == bytes@,
        Ok(Some(SourceMessage::Protobuf(body))) =>
            bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER
                && source_from_bytes(bytes@.skip(1)) == Some(Some(body)),
        Ok(None) => bytes@.len() > 0 && (bytes@[0] == LEGACY_FLUSH_HEADER
            || bytes@[0] == PROTOBUF_LOG_HEADER && source_from_bytes(bytes@.skip(1)) == Some(None)),
        Err(_) => bytes@.len() == 0 || (bytes@[0] != LEGACY_FLUSH_HEADER && bytes@[0] != LEGACY_TX_HEADER
            && (bytes@[0] != PROTOBUF_LOG_HEADER || source_from_bytes(bytes@.skip(1)).is_none())),
    },
))]
pub(super) fn decode_source_wire(bytes: &[u8]) -> Result<Option<SourceMessage>> {
    let (header, payload) = split_header(bytes).ok_or_else(|| invalid("Empty source message"))?;
    match header {
        LEGACY_TX_HEADER => Ok(Some(SourceMessage::LegacyTransaction(legacy_source(bytes)))),
        LEGACY_FLUSH_HEADER => Ok(None),
        PROTOBUF_LOG_HEADER => {
            let message = match decode_source(payload)? {
                Some(body) => Some(SourceMessage::Protobuf(body)),
                None => None,
            };
            Ok(message)
        }
        _ => Err(unknown_header(header, "source")),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match (message, out) {
        (SourceMessage::LegacyTransaction(raw), Ok(encoded)) =>
            raw@.len() > 0 && raw@[0] == LEGACY_TX_HEADER && encoded@ == raw@,
        (SourceMessage::LegacyTransaction(raw), Err(_)) =>
            raw@.len() == 0 || raw@[0] != LEGACY_TX_HEADER,
        (SourceMessage::Protobuf(body), Ok(encoded)) =>
            encoded@ == seq![PROTOBUF_LOG_HEADER] + source_bytes(*body),
        (SourceMessage::Protobuf(_), Err(_)) => false,
    },
))]
pub(super) fn encode_source_wire(message: &SourceMessage) -> Result<Vec<u8>> {
    match message {
        SourceMessage::LegacyTransaction(bytes) => {
            let Some((LEGACY_TX_HEADER, _)) = split_header(bytes) else {
                return Err(invalid("Legacy transaction is missing its 0xff header"));
            };
            Ok(join_bytes(&[], bytes))
        }
        SourceMessage::Protobuf(body) => Ok(encode_source(body)),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        Ok(payload) => bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER && payload@ == bytes@.skip(1),
        Err(_) => bytes@.len() == 0 || bytes@[0] != PROTOBUF_LOG_HEADER,
    },
))]
fn replica_payload(bytes: &[u8]) -> Result<&[u8]> {
    let (header, payload) = split_header(bytes).ok_or_else(|| invalid("Empty replica message"))?;
    if header != PROTOBUF_LOG_HEADER { return Err(unknown_header(header, "replica")); }
    Ok(payload)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Ok(frame) ==> bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER
            && (replica_from_bytes(bytes@.skip(1)) matches Some((term, sequence, message))
            && valid_stage(message)
            && frame.term.0 == term.unwrap_or(0)
            && frame.term_sequence == (match sequence { Some(value) => Some(TermSequence(value)), None => None })
            && frame.message == message),
        out is Err ==> bytes@.len() == 0 || bytes@[0] != PROTOBUF_LOG_HEADER
            || replica_from_bytes(bytes@.skip(1)).is_none()
            || (replica_from_bytes(bytes@.skip(1)) matches Some((_, _, message)) && !valid_stage(message)),
))]
pub(super) fn decode_replica_frame(bytes: &[u8]) -> Result<ReplicaFrame> {
    let payload = replica_payload(bytes)?;
    let (term, sequence, message) = decode_replica(payload)?;
    check_stage(&message)?;
    let term = match term { Some(term) => term, None => 0 };
    let term_sequence = match sequence { Some(value) => Some(TermSequence(value)), None => None };
    Ok(ReplicaFrame { term: Term(term), term_sequence, message })
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Ok(envelope) ==> bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER
            && (replica_from_bytes(bytes@.skip(1)) matches Some((term, sequence, message))
            && valid_stage(message)
            && (envelope is None <==> message is None)
            && (envelope matches Some(value) ==> message == Some(value.message)
                && value.term.0 == term.unwrap_or(0)
                && value.term_sequence == (match sequence { Some(value) => Some(TermSequence(value)), None => None }))),
        out is Err ==> bytes@.len() == 0 || bytes@[0] != PROTOBUF_LOG_HEADER
            || replica_from_bytes(bytes@.skip(1)).is_none()
            || (replica_from_bytes(bytes@.skip(1)) matches Some((_, _, message)) && !valid_stage(message)),
))]
pub(super) fn decode_replica_envelope(bytes: &[u8]) -> Result<Option<ReplicaEnvelope>> {
    Ok(decode_replica_frame(bytes)?.into_envelope())
}

impl ReplicaFrame {
    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures
            (out is None) == (self.message is None),
            out matches Some(value) ==> self.message == Some(value.message)
                && value.term == self.term && value.term_sequence == self.term_sequence,
    ))]
    pub fn into_envelope(self) -> Option<ReplicaEnvelope> {
        match self.message {
            Some(message) => Some(ReplicaEnvelope { term: self.term, term_sequence: self.term_sequence, message }),
            None => None,
        }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out matches Ok(found) ==> bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER
        && scan_complete(bytes@.skip(1), 0, found),
))]
pub(super) fn has_external_payload(bytes: &[u8]) -> Result<bool> {
    let payload = replica_payload(bytes)?;
    let (found, _) = scan_external_payload(payload, 0).map_err(invalid)?;
    Ok(found)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out@ == seq![PROTOBUF_LOG_HEADER] + replica_bytes(envelope.term.0,
        match envelope.term_sequence { Some(sequence) => Some(sequence.0), None => None }, envelope.message),
))]
pub(super) fn encode_replica_envelope(envelope: &ReplicaEnvelope) -> Vec<u8> {
    let sequence = match envelope.term_sequence { Some(sequence) => Some(sequence.0), None => None };
    encode_replica(envelope.term.0, sequence, &envelope.message)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out@ == if retirement { RETIREMENT_MARKER@ + attempt_bytes(attempt) } else { attempt_bytes(attempt) },
        attempt_bytes(attempt).len() == 96 && attempt_from_bytes(attempt_bytes(attempt)) == Some(attempt),
))]
pub fn encode_replica_outcome_attempt(attempt: PlacedAttempt, retirement: bool) -> Vec<u8> {
    let bytes = encode_attempt(attempt);
    let prefix: &[u8] = if retirement { RETIREMENT_MARKER } else { &[] };
    join_bytes(prefix, &bytes)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Ok((attempt, retirement)) ==>
            bytes@ == (if retirement { RETIREMENT_MARKER@ + attempt_bytes(attempt) } else { attempt_bytes(attempt) })
            && attempt_bytes(attempt).len() == 96
            && attempt_from_bytes(if retirement { bytes@.skip(34) } else { bytes@ }) == Some(attempt),
        out is Err ==> attempt_from_bytes(bytes@).is_none()
            && (bytes@.len() < RETIREMENT_MARKER@.len()
                || bytes@.take(RETIREMENT_MARKER@.len() as int) != RETIREMENT_MARKER@
                || attempt_from_bytes(bytes@.skip(RETIREMENT_MARKER@.len() as int)).is_none()),
))]
pub fn decode_replica_outcome_attempt(bytes: &[u8]) -> Result<(PlacedAttempt, bool)> {
    match decode_attempt(bytes) {
        Ok(attempt) => Ok((attempt, false)),
        Err(error) => {
            let attempt = strip_prefix(bytes, RETIREMENT_MARKER).ok_or(error)?;
            Ok((decode_attempt(attempt)?, true))
        }
    }
}

#[cfg(verus_keep_ghost)]
verus! {
proof fn retirement_marker_keeps_its_protocol_spelling()
    ensures RETIREMENT_MARKER@ == b"rtdb/placed-receipt-retirement/v1\0"@,
{}
}
