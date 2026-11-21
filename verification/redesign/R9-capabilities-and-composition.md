# R9 · Capabilities and the composed residence model

## Goal

Close the gap between verified decisions and how they are wired together.
Every recent real defect was compositional: cross-node reader stranding with every proof green, a cursor-future stack overflow, queries answered from memory on an unconfirmed catalog, and the system-time cliff that the verified strict-advance rule allowed.

## Converged design

1. **Capabilities.** Each premise a verified decision assumes becomes a value that only its establishing owner can construct, and the protected effect takes it:
   - a held view (from a successful hold CAS, R4) for every storage read and for answering a query, including from memory;
   - a deletion grant (an identity retired at or below the floor in one ledger version) for every provider delete;
   - a confirmed catalog cut, captured atomically with the partition value;
   - a current key grant (R6) for every decrypted cached page;
   - an exact active-loglet read index for every fresh snapshot capture;
   - a protected recovery closure for every replica trim and upstream acknowledgement.
   Verus proves the constructors; compile-fail harnesses show no other path builds a capability. The replica admission witness is the existing pattern (its compile-fail controls run in `verification/run.py`).
2. **One composed model.** A TLA+ specification of the residence (log clock, ledger, holds, floor, compaction with epochs, reset, roots, keys and forks, receipts) that refines the existing per-mechanism models.
3. **Trace validation.** Production and simulation emit an event at each linearization point for a hold, floor advance, publication, retirement, delete, erasure obligation, key revocation, attempt outcome, read index or recovery grant.
   Each record includes the relevant history, epoch and exact publication identity.
   A checker validates recorded traces against the composed model's next-state relation.
4. **Async shape.** Bound future and stack depth in cursor construction: recursive cursor futures are boxed or iterative, and replay the deepest plan fixtures under a bounded worker stack.

## State on main (2026-11-21)

- The replica admission witness and its compile-fail controls exist.
- Storage reads already pass through one admission point (`IoRegion::admit` in `rust/crates/storage/src/io_region.rs`), where today's read lease is checked; that is the natural place for the held-view capability.
- Peers recently reserved stack for nested page cursors and recursive column pruning (`4ffa2cf02`, `ea739f696`).
- The bounded placed serving, custody and key enrollment models cover target transitions.
- A composed authority model and production trace checker remain open.

## Handoff notes

1. Grow R9 with each increment rather than at the end: R4 adds the held view and deletion grant, R6 live key material, R7 erasure events.
2. Prefer capabilities that fit existing boundaries (`IoRegion::admit`, `ObjectStore::delete`, `QueryContext` construction) so the change is mostly types, not plumbing.
3. Keep the trace format shared with the native replays in `verification/traces/` so a model counterexample can become a production regression and the reverse.

## Tests and proofs

- **Compile-fail:** removing the held view from one read path, or calling a provider delete without a grant, must not compile.
- **Trace validation:** every mutation named in R4–R8 must be rejected by both TLC and the trace checker.
- **Native:** the deepest plan fixtures run under a bounded worker stack.

## Open questions

- The trace event schema and where traces are recorded in production (sampled or always).
