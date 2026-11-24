use crate::{Record, journal_codec_leaves::*};
use crate::framing::{decode_located_record, frame_record};
use vstd::prelude::*;

verus! {

proof fn a_prost_record_round_trips_through_journal_framing(record: Record)
    requires encode(record).len() <= u32::MAX,
    ensures frame_matches(frame_bytes(encode(record)), record),
{
    let body = encode(record);
    let length = encode_be32(body.len() as u32);
    let checksum = encode_be32(crc(body));
    let frame = frame_bytes(body);
    be32_round_trips(body.len() as u32);
    be32_round_trips(crc(body));
    prost_record_round_trips(record);
    assert(frame.take(4) =~= length);
    assert(frame.subrange(4, 8) =~= checksum);
    assert(frame.skip(8) =~= body);
}

proof fn a_torn_encoding_cannot_match_a_shorter_record(record: Record, other: Record, cut: int)
    requires encode(record).len() <= u32::MAX, 0 <= cut < frame_bytes(encode(record)).len(),
    ensures !frame_matches(frame_bytes(encode(record)).take(cut), other),
{
    let body = encode(record);
    let length = encode_be32(body.len() as u32);
    let frame = frame_bytes(body);
    be32_round_trips(body.len() as u32);
    be32_round_trips(crc(body));
    if cut >= 8 {
        assert(frame.take(cut).take(4) =~= length);
    }
}

fn a_written_journal_frame_round_trips(record: Record, buffer: BytesMut) -> (out: Option<Record>)
    requires buffer@.len() == 0, encode(record).len() <= u32::MAX,
    ensures out == Some(record),
{
    let ghost original = record;
    let mut buffer = buffer;
    frame_record(record, &mut buffer);
    let bytes = Bytes { contents: buffer.contents };
    proof {
        assert(bytes@ =~= frame_bytes(encode(original)));
        a_prost_record_round_trips_through_journal_framing(original);
    }
    decode_located_record(&bytes)
}

}

use crate::{LEGACY_TX_HEADER, PlacedAttempt, TermSequence};
use crate::decoder::{
    RETIREMENT_MARKER, ReplicaEnvelope, SourceMessage, decode_replica_envelope, decode_replica_outcome_attempt, decode_source_wire,
    encode_replica_envelope, encode_replica_outcome_attempt, encode_source_wire,
};
use crate::decoder_leaves::{prost_replica_round_trips, prost_source_round_trips, replica_bytes, source_bytes, valid_stage};

#[verus_spec(out =>
    ensures out matches Ok((decoded, retired)) && decoded == attempt && retired == retirement,
)]
fn an_attempt_round_trips_in_its_own_form(attempt: PlacedAttempt, retirement: bool)
    -> crate::decoder_leaves::Result<(PlacedAttempt, bool)>
{
    let bytes = encode_replica_outcome_attempt(attempt, retirement);
    #[cfg(verus_keep_ghost)]
    proof! {
        if retirement {
            assert(bytes@.take(34) =~= RETIREMENT_MARKER@);
            assert(bytes@.skip(34) =~= crate::decoder_leaves::attempt_bytes(attempt));
        }
    }
    decode_replica_outcome_attempt(&bytes)
}

#[verus_spec(out =>
    requires valid_stage(Some(original.message)),
    ensures out matches Ok(Some(decoded)) && decoded == *original,
)]
fn a_valid_encoded_replica_envelope_round_trips(original: &ReplicaEnvelope)
    -> crate::decoder_leaves::Result<Option<ReplicaEnvelope>>
{
    let bytes = encode_replica_envelope(original);
    proof! {
        let sequence = match original.term_sequence { Some(sequence) => Some(sequence.0), None => None };
        prost_replica_round_trips(original.term.0, sequence, original.message);
        assert(bytes@.skip(1) =~= replica_bytes(original.term.0, sequence, original.message));
        assert(original.term_sequence == (match sequence { Some(value) => Some(TermSequence(value)), None => None }));
    }
    decode_replica_envelope(&bytes)
}

#[verus_spec(out =>
    requires match original {
        SourceMessage::LegacyTransaction(raw) => raw@.len() > 0 && raw@[0] == LEGACY_TX_HEADER,
        SourceMessage::Protobuf(_) => true,
    },
    ensures out matches Ok(Some(decoded)) && match (original, decoded) {
        (SourceMessage::LegacyTransaction(raw), SourceMessage::LegacyTransaction(copy)) => raw@ == copy@,
        (SourceMessage::Protobuf(original), SourceMessage::Protobuf(decoded)) => *original == decoded,
        _ => false,
    },
)]
fn a_valid_encoded_source_envelope_round_trips(original: &SourceMessage)
    -> crate::decoder_leaves::Result<Option<SourceMessage>>
{
    let bytes = encode_source_wire(original)?;
    proof! {
        crate::log_headers::log_headers_keep_their_protocol_values();
        if let SourceMessage::Protobuf(body) = original {
            prost_source_round_trips(*body);
            assert(bytes@.skip(1) =~= source_bytes(*body));
        }
    }
    decode_source_wire(&bytes)
}
