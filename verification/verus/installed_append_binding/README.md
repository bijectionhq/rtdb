# Installed append binding

The harness imports the shipped append guards, recovery cursor selector, journal key constructor, and detached-frame decoder.
The laws check the decisions used by `InstalledJournal::append`, `JournalLog::finish_append`, `Cursor::deliver`, `JournalLog::enqueue_at`, and `PartitionLogs::decode_oversized`.

| Lean definition or theorem | Shipped code and Verus law |
| --- | --- |
| `NativeAppend.chosenResultAt`, `NativeAppend.matchesPendingAppend`, `Composition.chosen_result_projection` | [Append selection](../../../rust/crates/multi/src/journal/append_selection.rs) returns the complete original result only at the receipt's decision position and checks the pending class and both LSNs in `chosen_result_projection`. The reconcile path passes the pending-class and LSN checks. |
| `NativeAppend.DecodeSource.openedFrame`, `NativeAppend.openedFrame_of_chosen_payload` | [The replay cursor](../../../rust/crates/multi/src/journal/replay_cursor.rs) preserves the complete selected record, including its ciphertext and grant, in `retains_the_exact_ciphertext`. `Cursor::deliver` passes comparisons against the prepared cut and hold, then opens the returned ciphertext. |
| `NativeAppend.recordedKey` | [Journal key construction](../../../rust/crates/multi/src/keys/journal_identity.rs) selects the installed deployment and incarnation and the chosen record's key generation in `opens_under_recorded_key`. The native call supplies the record's grant. |
| `NativeAppend.resolveDetached`, `NativeAppend.resolveDetached_preserves_body` | [Detached-frame decoding](../../../rust/crates/format/src/envelope/detached.rs) checks the fetched bytes against a present digest, decodes the exact bytes, preserves their complete protobuf body and requires the outer term and optional sequence. `resolves_detached_payload` checks that a successful result excludes a nested proxy, an absent body and a staged transaction. |

The Rust cut uses the exclusive LSN and the complete history fingerprint.
The harness represents their equality by scalar identities and keeps the ciphertext as an opaque identity on the same value as the grant.
Lean projects the request identity, digests, exclusive cut, expiry and byte interval into a `Protocol.Append`.
The first chosen command has native decision position one and Lean list index zero.
The authenticated request and result fields, request fingerprint and decision position supplied by Logkeeper remain inputs to that projection.
The native guards cover result coordinates and replay selection.

The decoder uses the registered Prost and SHA-256 contracts.
A legacy proxy can omit the digest.
The installed publisher supplies it, and the exact-version publication parser also requires a 32-byte digest.
The provider must return the bytes for that exact admitted object, and the key service must implement authenticated opening with the selected key.
The laws check the selector, key identity, and decoder.
Async fetch and opening, the relationship of Logkeeper replies to a chosen trace, and translation from a resolved protobuf body to database effects require separate refinement.

`area.json` declares six controls.
The tests exercise the pending append guards, opening a retained record with its recorded generation after rotation, and rejecting changed bytes that still decode to the same detached envelope.
