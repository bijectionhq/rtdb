# R12 · Native acknowledgement

Verus loads [the shipped decisions](../../../rust/crates/replication/src/readback_identity.rs) directly through [proof.rs](proof.rs).
The [area manifest](area.json) runs the production module and the independent [laws](laws.rs).
Each control weakens both a shipped function and its contract, so the verifier has to reject the defect at the named law.

## Correspondence to Lean

[NativeAcknowledgement.lean](../../multi/compose/NativeAcknowledgement.lean) models one pending native transaction with an arbitrary term, optional sequence, optional fingerprint, result and error.
The Rust term and sequence integers map to Lean `Int`, each fingerprint maps to a list of 32 `UInt8` values, and Rust `Result<T, E>` maps to Lean `Except E T`.

| Shipped decision | Lean definition and law |
| --- | --- |
| `own_readback_term` in `validate_own_readback`, pending error lookup and the own-transaction apply branch | `own_readback_term`, the term conjunct of `ExactReadback`. |
| `matching_present_integer` in `pending_tx_at` and `complete_record` | `matching_present_integer`, used by `matching_sequence`. An absent sequence cannot identify a pending write. |
| `readback_matches_transaction` in `validate_own_readback` | `readback_matches_transaction`, the resolved-body and present, byte-equal fingerprint conjunct of `ExactReadback`. |
| `transaction_completion` in the appender's enqueue branch, `finish_readback_failure` and the driver's own apply branch | `transaction_completion` and `nativeAcknowledgement`. Enqueue leaves the pending write waiting. An admitted mismatch at its identity requests completion with the error. Successful application forwards the applied outcome unchanged. |

`native_acknowledgement_refines_applied_record` composes the decisions in Lean.
It equates successful completion with an `ExactReadback` and a successful application carrying that very result.
The model has an exact-read-back reachability witness and concrete counterexamples to accepting enqueue, an altered payload, a foreign term, a different sequence or a staged result in place of the applied result.

## Native wiring and PX1

[The appender](../../../rust/crates/replication/src/appender.rs) stamps each pending write with its assigned sequence and, for a resolved transaction, the SHA-256 of the encoded frame before provider enqueue.
[PartitionLogs](../../../rust/crates/replication/src/logs.rs) fingerprints the delivered bytes for inline read-back and the recovered transaction frame for an oversized record.
[The driver](../../../rust/crates/replication/src/driver.rs) validates admitted read-back before the block and resolution buffers.
The own-transaction apply branch checks that the pending append still owns the resolver head, applies the partition and any committed catalog effect, and publishes progress.
Only then does it supply `ApplyFinished(Ok(result))`, forwarding the same `Arc<TransactionResult>` through `complete_record` that it published in applied progress.
The provider receipt has its own channel and can fail a pending write.
Completion takes the transaction's one-shot sender at most once.
Term loss can independently fail the pending write, and a closed caller can leave an applied record without a delivered acknowledgement.

In PX1's `AcknowledgedRecovery.lean` (commit `34a87874e2` on `codesign/p-x1`), this establishes the native acknowledgement decision used for the `initial : RecordedAppend` and `decodedAcknowledgement` hypothesis row.
Binding the replica frame to the exact chosen Logkeeper `Protocol.Append`, establishing the logical event returned by `semantics.decodeTx`, and projecting a native read and query into `readCoversAcknowledgement`, `exactReadCut`, `usesSelectedCut` and `queryUsesRead` remain composition obligations.
The proof treats returned fingerprints as byte values and relies on the journal delivery and hash binding contracts.
The async queue, one-shot delivery, storage effects, semantic replay and query materialization are outside the verified functions.

The Lean mutation runner rejects a completion decision that acknowledges enqueue, at `enqueued_does_not_acknowledge` and `enqueue_only_acknowledgement_is_invalid`.
