# Prost message decoding

Status: premise for the framing decoder proof area.
The RTDB lockfile selects Prost 0.14.6.

The input to `prost::Message::decode` is the entire supplied `Buf`.
Successful decoding consumes that buffer as protobuf fields and returns the generated message after applying its field and oneof merge rules.
An incomplete key, scalar, length-delimited field or group fails decoding.
A complete unknown field can be consumed and discarded.
A legal non-minimal varint and a valid extra protobuf field can contribute to a successful decode.
Decoding Prost's own encoding yields the original generated message, using value equality for the source, replica and journal schemas covered here.
The round-trip premise applies to values within the encoder's available memory and the decoder's supported nested-message depth.

The [envelope leaf](../framing_decoders/decoder_leaves.rs) relates the entire source or replica body to an abstract decoded value.
The [journal leaf](../framing_decoders/journal_leaves.rs) relates the exact declared journal body to an abstract decoded record.
They use normal whole-buffer `decode`.
The framing proof checks which byte span reaches each leaf and what the RTDB wrapper does with its result.

The authored conformance cases are `replica_decoding_consumes_unknown_fields_and_rejects_malformed_suffixes` in `rust/crates/format/tests/term_sequence.rs`, `generated_source_decoding_rejects_an_incomplete_field_after_a_known_body` in `rust/crates/format/tests/source_classification.rs`, and `journal_readers_require_the_declared_body_and_prost_consumes_that_entire_body` in `rust/crates/logkeeper/src/server/journal.rs`.
They were not run in the formal-only session.
