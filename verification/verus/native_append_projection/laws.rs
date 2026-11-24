use crate::decoder_leaves::{Result, replica_from_bytes, valid_stage};
use crate::{PROTOBUF_LOG_HEADER, ReplicaBody};
use crate::production::{ReplicaFrame, decode_replica_frame};
use vstd::prelude::*;

verus! {

spec fn decode_frame(bytes: Seq<u8>, frame: ReplicaFrame) -> bool {
    bytes.len() > 0 && bytes[0] == PROTOBUF_LOG_HEADER
        && (replica_from_bytes(bytes.skip(1)) matches Some((term, sequence, body))
            && valid_stage(body)
            && frame.term.0 == term.unwrap_or(0)
            && (match frame.term_sequence {
                Some(value) => Some(value.0),
                None => None,
            }) == sequence
            && frame.message == body)
}

fn decode_chosen_frame(bytes: &[u8]) -> (out: Result<ReplicaFrame>)
    ensures match out {
        Ok(frame) => decode_frame(bytes@, frame),
        Err(_) => bytes@.len() == 0 || bytes@[0] != PROTOBUF_LOG_HEADER
            || replica_from_bytes(bytes@.skip(1)).is_none()
            || (replica_from_bytes(bytes@.skip(1)) matches Some((_, _, body)) && !valid_stage(body)),
    },
{
    decode_replica_frame(bytes)
}

spec fn decoded_event_body(frame: ReplicaFrame, prost_body: Option<ReplicaBody>) -> bool {
    frame.message == prost_body
}

fn decoded_acknowledgement_preserves_body(frame: ReplicaFrame, protobuf_body: ReplicaBody)
    requires decoded_event_body(frame, Some(protobuf_body)),
{
    let envelope = frame.into_envelope();
    assert(envelope matches Some(value) && value.message == protobuf_body);
}

}
