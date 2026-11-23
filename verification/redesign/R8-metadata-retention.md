# R8 · Retention of retry receipts and transaction metadata

## Goal

Retry receipts and transaction user metadata exist only for the receipt window (owner decision), and are then gone physically, not just hidden by valid time.
Metadata naming an erased unit is erased with it before the window ends.

## Converged design

- Store receipts and transaction metadata in an engine-owned keyspace whose checkpoint contains its live entries.
  R2 currently stores application receipts as bitemporal rows in `xt.idempotency_receipts` with a one-hour valid interval.
- Keep application retry receipts and placed attempt outcome receipts under their own identities.
  Expiring either receipt leaves the committed transaction coordinate valid as a causal basis.
  A closed epoch and the retained receipt frontier distinguish an unresolved attempt from an expired one.
- Project `xt.txs.user_metadata` from the retained metadata store.
  It expires after the receipt window and participates in privacy erasure when it names an erased unit.
- Expiry uses log time at resolution so replicas select the same entries.
  Sealed immutable log and checkpoint copies need the same physical retirement contract as the live keyspace.
  The retention claim depends on releasing the replay closure or revoking the applicable keys after a complete successor is published.

## Invariants

- **M1.** After the window, no storage class holds an expired receipt or the transaction metadata it covered.
- **M2.** Retry decisions are unchanged within the window (the R2 resolver test keeps passing).

## State on main (2026-11-21)

- R2 (`85a868c07`): the resolver checks receipts in `xt.idempotency_receipts` (`rust/crates/tx/src/resolver.rs`, `receipt_outcome` and `put_receipt`, `RECEIPT_WINDOW_MICROS`), keyed by principal and key with a body digest (`rust/crates/tx/src/source.rs`, `body_digest`).
- Application receipt validity and lookup use the recorded source-log instant.
  Lookup selects the newest visible receipt whose expiry is still ahead of the recorded log instant.
  A backward log-clock reading cannot hide an already committed, unexpired receipt before its start time.
  Legacy receipt rows retain their original stored intervals.
- Placed resolution writes a checkpointed outcome witness with a one-hour validity interval starting at the intake record's timestamp.
  The exact attempt key survives checkpoint recovery.
  The client receipt expires at the same one-hour boundary.
  A causal token issued after redemption remains usable across writer changes.
- Expired receipts and all of `xt.txs.user_metadata` remain in bitemporal history indefinitely.

## Handoff notes

1. Land the checkpointed keyspace and replay first.
   The physical expiry guarantee additionally needs coverage of log and checkpoint copies through R5–R7.
2. Compute duplicate and conflict outcomes from the logged request and the replayed receipt state.
3. Preserve the transaction id, system time and committed flag as history.
   Include structured error fields in the retention inventory before classifying them as historical or expiring metadata.

## Tests and proofs

- **Native:** after the window, a storage scan finds no expired receipt or metadata; a retry within the window still gets the duplicate or conflict outcome after a restart and a leader change.
- **Verus:** the receipt decision (same digest gives duplicate, different gives conflict, expired gives fresh), which R2 did not yet verify.
- **Mutation that must fail:** keeping one expired receipt; compare keys only, ignoring the digest.

## Open questions

- Which `xt.txs` columns survive the window.

## Bounded application retry identities

Native clients can set `rtdb_retry_v1: {nonce: '0123456789abcdef0123456789abcdef', not_after_micros: 123456789}` in transaction metadata.
The deadline is exclusive UTC microseconds and at most one hour ahead of the admission clock.
PgWire clients require `SHOW rtdb_bounded_retry` to return `1` on each connection before submitting this format.
The identity includes version, nonce and deadline under the authenticated principal and database history.
Timeouts and disconnects are retried under the same identity with the same body.
A new operation needs a new identity.
Classic databases scope the receipt table to the database storage.
Placed history fingerprints retain identity across writer epochs and exclude receipts inherited from a parent history.

A source transaction's versioned retry field must agree with its metadata.
The encoded transaction starts with a refusal assertion for readers that do not understand the field.
An upgraded resolver removes that assertion once it has verified the field.
It chooses the maximum of the previous durable retry clock and the exact source timestamp, independently of the projected SQL system time.
An accepted transition publishes the new clock and original outcome with its effects or after discarding an aborted batch.
The same body receives the original durable transaction result until the deadline, including an abort.
A different body receives `rtdb/idempotency-conflict`, and an elapsed identity always receives `rtdb/idempotency-expired`, including after later clock regressions.
Clock progress follows bounded retry log observations.
The physical retention obligations in M1 still apply to the new receipt rows and their original outcomes.

Lean checks the separation between append observation and commit order, irreversible observed expiry, preservation of the first terminal result, and same-body redemption for both commits and aborts.
It rejects reporting a smoothed observation, using a regressed clock, and replacing a terminal receipt.
Verus checks that expiry uses the exclusive boundary and that a regressed append observation cannot reopen an identity.
The native node test flushes the first terminal results, reopens the database, redeems the abort and commit, then verifies that expiry survives a second restart and clock rollback.
The resolver tests cover the compatibility assertion and reuse of a placed identity after writer turnover, with an independent fork history as a control.
