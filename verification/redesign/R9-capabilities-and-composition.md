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
- The first composed authority model covers snapshot, checkpoint, replay, fork and storage retirement. Production trace validation and the remaining authority domains are open.

## Composed storage and recovery

[ResidenceComposition](../tla/ResidenceComposition.tla) uses one state for the serving catalog, installed checkpoint, admitted query, provider read, recovery cursor, shared fork and directory phase.
A provider read retains its own dependency after the query closes.
An installed successor updates the trim bound, while an admitted recovery keeps the older suffix and publications readable.
The retirement transition needs the floor and all applicable dependencies before it can remove one immutable publication.
Whole-residence disposal requires the provider read to drain, excludes fork publication, preserves each required publication in the published fork's own store before removing it from the source, and confirms source absence before settlement.

The bounded model has two catalog cuts, three publications, one query, one in-flight provider read, one recovery cursor and one fork.
TLC explored 1,063 distinct states. Nine weakened rules violate the corresponding composed invariant.
The reachability cases cover exact reclamation while a current query is open and whole-residence settlement after a trim and fork copy.
Apalache independently checks the conjunction and the trim control.
See the [runner](../README.adoc#_run_the_studies) for reproduction.

In this slice, directory changes are atomic and publication identity remains stable across retries.
The model abstracts the network, staging retries, multiple simultaneous holds per kind, bitemporal resolution, key revocation and application-level receipt publication.
A native event trace must supply the actual cut, provider identity and directory permit used at each decision.
[ResidenceCompositionReplay](../tla/ResidenceCompositionReplay.tla) checks an ordered trace against the shared transition relation.
Its [authored fixture](../traces/residence-composition.json) projects catalog cuts and publication identities into the model.
A trace must reach the disposal receipt with the expected replica floor and fork copy.
Controls refuse a different physical publication, retirement before read drain, trimming an admitted recovery, erasing an uncopied fork dependency and a trace that ends before settlement.
The composed checker also replays the copy and removal effects captured through the native local-store disposal implementation.
The fixture uses a genuine directory disposal permit and a published fork with two inherited objects.
The checker validates the physical source and destination scopes before projecting the two publications into the model.
A control moves one removal ahead of the copy it needs and is rejected at the first step.
This replay begins after directory admission and ends at source absence. It exercises local storage effects, while the authored trace covers the full sequence through directory settlement.

A separate native checkpoint and trim replay exports committed directory revisions and physical floors observed from the logkeepers.
Its validator checks that authorization preserves replay holds and has a manifest from the same journal.
It covers one journal at a time. Native composition across query admission, hosted provider custody, directory settlement and recovery trimming remains open.
A model counterexample to a weakened rule shows why the rule matters for the modeled system.
The [recovery and custody model](../tla/RecoveryCustodyHandoff.tla) composes readable root and suffix replacement, covered upstream confirmation, independent copy admission, and cutover to deadline-bound revocation.
The [Lean proof](../lean/RecoveryCustodyHandoff.lean) checks preservation for arbitrary units and finite schedules under those authority premises.
See [R7](R7-eraser.md#verification) for the remaining provider obligations.

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
