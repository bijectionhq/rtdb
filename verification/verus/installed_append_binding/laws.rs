use crate::decoder::ReplicaEnvelope;
use crate::decoder_leaves::{Result, replica_from_bytes};
use crate::digest_leaf::sha256;
use crate::journal_identity::{JournalKey, journal_key_for_grant};
use crate::production::{chosen_result_at, matches_pending_append};
use crate::replay_cursor::select_record;
use crate::{RetainedRecord, AppendClass, Grant, Placement, ReplicaBody, TermSequence, PROTOBUF_LOG_HEADER};
use vstd::prelude::*;

verus! {

fn chosen_result_projection<T>(result: T, result_position: u64, decision_position: u64,
    class: AppendClass, pending_class: AppendClass, lsn: u64, expected_next: u64, writer_next: u64,
) -> (accepted: Option<T>)
    ensures accepted == if result_position == decision_position && class == pending_class
        && lsn == expected_next && lsn == writer_next { Some(result) } else { None },
{
    let result = chosen_result_at(result, result_position, decision_position);
    if matches_pending_append(class, pending_class, lsn, expected_next, writer_next) {
        result
    } else {
        None
    }
}

fn retains_the_exact_ciphertext(record: RetainedRecord, exact_checkpoint: bool, exact_through: bool,
    next: u64, history: u128, timestamp: i64, sealed_len: usize,
) -> (selected: Option<RetainedRecord>)
    ensures selected == if exact_checkpoint && exact_through
        && record.next == next && record.history == history
        && record.timestamp == timestamp && record.sealed_len == sealed_len
    { Some(record) } else { None },
{
    let exact_cut = record.next == next && record.history == history;
    let observed_time = record.timestamp;
    let observed_len = record.sealed_len;
    select_record(record, exact_checkpoint, exact_through, exact_cut, observed_time, timestamp,
        observed_len, sealed_len)
}

fn opens_under_recorded_key(placement: Placement, recorded: Grant) -> (key: JournalKey)
    ensures key.deployment == placement.deployment && key.incarnation == placement.incarnation
        && key.generation == recorded.key_generation,
{
    journal_key_for_grant(placement, recorded)
}

fn resolves_detached_payload(proxy: &ReplicaEnvelope, bytes: &[u8]) -> (result: Result<ReplicaEnvelope>)
    ensures result matches Ok(payload) ==> (
        proxy.message matches ReplicaBody::OversizedMessage(reference)
        && (reference.payload_sha256 matches Some(digest) ==> digest@ == sha256(bytes@))
        && payload.term == proxy.term && payload.term_sequence == proxy.term_sequence
        && !(payload.message is OversizedMessage)
        && (payload.message matches ReplicaBody::ResolvedTx(tx) ==> tx.acceptance_stage is None)
        && bytes@.len() > 0 && bytes@[0] == PROTOBUF_LOG_HEADER
        && (replica_from_bytes(bytes@.skip(1)) matches Some((term, sequence, body))
            && payload.term.0 == term.unwrap_or(0)
            && payload.term_sequence == match sequence { Some(value) => Some(TermSequence(value)), None => None }
            && body == Some(payload.message))),
{
    proxy.unwrap_payload(bytes)
}

}
