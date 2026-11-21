# R8 · Retention of retry receipts and transaction metadata

## Goal

Retry receipts and transaction user metadata exist only for the receipt window (owner decision), and are then gone physically, not just hidden by valid time.
Metadata naming an erased unit is erased with it before the window ends.

## Converged design

- Designers proposed an engine-owned system keyspace with no bitemporal history for receipts and transaction metadata, so expiry is physical by construction.
- R2 landed receipts as bitemporal rows in `xt.idempotency_receipts` instead (valid for one hour of system time, unwritable by SQL), to reuse the resolver's existing read path. R8 either moves them to a non-history keyspace or removes them physically with the R7 machinery; the non-history keyspace is simpler if it can be checkpointed and replayed deterministically.
- Move `xt.txs.user_metadata` under the same retention: kept for the receipt window, removed physically afterwards, and erasable with the unit it names.
- Expiry is measured in log time at resolution, so every replica removes the same rows.

## Invariants

- **M1.** After the window, no storage class holds an expired receipt or the transaction metadata it covered.
- **M2.** Retry decisions are unchanged within the window (the R2 resolver test keeps passing).

## State on main (2026-11-21)

- R2 (`85a868c07`): the resolver checks receipts in `xt.idempotency_receipts` (`rust/crates/tx/src/resolver.rs`, `receipt_outcome` and `put_receipt`, `RECEIPT_WINDOW_MICROS`), keyed by principal and key with a body digest (`rust/crates/tx/src/source.rs`, `body_digest`).
- Expired receipts and all of `xt.txs.user_metadata` remain in bitemporal history indefinitely.

## Handoff notes

1. Land after R7 if you remove rows physically through the eraser; a non-history keyspace can land earlier, but must be part of checkpoints so recovery restores live receipts.
2. Keep the resolver's decision deterministic: whatever store you choose, the duplicate or conflict outcome must be computed from the logged record and the replayed state only.
3. Decide what `xt.txs` keeps after the window (transaction id, system time, committed flag and error are history; metadata is not).

## Tests and proofs

- **Native:** after the window, a storage scan finds no expired receipt or metadata; a retry within the window still gets the duplicate or conflict outcome after a restart and a leader change.
- **Verus:** the receipt decision (same digest gives duplicate, different gives conflict, expired gives fresh), which R2 did not yet verify.
- **Mutation that must fail:** keeping one expired receipt; compare keys only, ignoring the digest.

## Open questions

- Which `xt.txs` columns survive the window.
