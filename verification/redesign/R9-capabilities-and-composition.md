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

## Wave 5 implementation

| Increment | Composed transition and current authority |
| --- | --- |
| R1 | The ordinary attempt admits a requested system time through its append bound, and the external case clamps a future request in the model's first-append bound. |
| R2 | An attempt has one original body and terminal outcome. Redemption preserves the outcome, detects a changed body, and gives expiry precedence. |
| R3 | Installing the bounded successor advances its naming epoch. The trace projection checks the exact key, version and naming epoch; replay tests a substituted publication and reuse of the old epoch. Reset from the adopted primary landed separately. |
| R4 | Catalog admission and expiry of dependencies drive floor advancement. The trace distinguishes query release from drain of its in-flight provider operation. Directory-managed S3 object and replay-pack retirement require an opaque grant from the exact authorized lineage revision. |
| R5 | A published checkpoint protects recovery admission and gates model trim and upstream acknowledgement. The existing typed native trim grant carries its observed directory revision and certified anchor into application. |
| R6 | A revoked old-unit key makes access to its cached page fail under the held view. Installed journal object keys now bind to independently withdrawable immutable units. Directory-managed and classic custody remain subject to the provider coverage in [R6](R6-sealing.md). |
| R7 | Erasure closes preparation, requires a readable successor before arming, and then revokes the old key. Native archive observations carry the exact capture, manifest and deadline. Live installed SQL erasure still depends on the clean closure and recovery-slot premises in [R7](R7-eraser.md). |
| R8 | Receipt collection uses the exclusive observed-clock boundary. A lower raw observation after that boundary preserves expiry. Independent recovery checkpoints can exclude covered expired receipt L0. |

An active-loglet read index is constructed in the quorum client after validating the selected ensemble, stream and epoch.
Fresh implicit session bases keep the protected index alive through partition capture and check that the attachment still names the same database.
The installed journal continues to supply its own ordered barrier and retained cut.
PostgreSQL feedback consumes an acknowledgement permit derived from its checkpoint and recoverable applied extent.
The local replication term issues an opaque replay token only when its logs retain synced history and its store syncs checkpoints.
The provider adapter for directory-managed exact removal accepts a grant derived from the durable authorized lineage state, including on retry.
These values are passive evidence, while the existing journal, partition and directory remain the owners of their decisions.
Rust compile-fail documentation covers minting a fabricated index and retirement grant.

The composed replay uses the same next-state relation as the bounded search.
The [expanded authored trace](../traces/residence-authorities.json) interleaves a refusal receipt, a superseded loglet, a held old view, key revocation, upstream confirmation, trim, and a fork surviving disposal.
The projection checks exact residence, loglet, attempt, key-slot and publication identities before converting them to the model's finite vocabulary.
For a bounded projection with these event shapes, the standalone checker accepts the fixture format with `--tlc PATH --evidence DIR`.
The runner declares mutation controls for the expanded state and replay, including wrong time clamp, premature collection, rollback of the receipt clock, stale freshness authority, premature erasure or acknowledgement, and accesses after revocation.

Multi can record native observations into one new JSONL file per process via `RTDB_RESIDENCE_TRACE_PATH`.
Run `python3 verification/residence_trace.py TRACE.jsonl` to check exact retirement and trim correlations, catalog admission floors, read-index identities, scoped replay holds, scheduled archive revocation evidence and observed inline attempt outcomes.
The sink records decision observations after the owning path returns or observes authority, so directory revision orders catalog CAS observations.
The production and simulation owners can use the same sink when they run the same Multi machinery.
The existing native concurrent recovery and trim fixture now supplies a second capture to this checker and a changed-journal negative control.
The native checker validates correlations within a capture, while composed replay consumes an explicitly projected bounded history.
Native events can be missing after cancellation, ambiguous responses, opaque or offloaded outcomes, process failure or later enablement.
Use separate files for separate processes and keep diagnostic capture disabled when measuring strict timing.

Remaining capability boundaries include mandatory held views at every storage and memory answer, current per-unit grants at every cache release, full recovery-closure evidence at every upstream acknowledgement, and general provider deletion through one exact authority.
The authored projection abstracts plurality of held views, receipt identities and slots, network races, provider timing, and reconstruction from manifest contents.
The native runner also includes the existing thousand-conjunct fixture on the production worker stack.
Cross-process trace reconstruction, observation of every runtime linearization point, configured-provider evidence and execution of the worker-stack fixture remain open.
The expanded model, replay controls and Rust changes have source review only.

## Round 3 continuation

The storage pool issues an opaque catalog registration bound to its physical cut and pool residence.
Query entry points verify that authority before opening or advancing a cursor, returning an in-memory answer, or sending a delegated or direct owned answer.
Provider admission retains the held catalogs and registered owner through I/O drain.
Prepared audience queries reuse their registration on the same pool and within its admitted scope.

A shared byte or decoded-page cache requires an explicit opt-in by the underlying object owner.
Encrypted and captured object stores use fresh owner-mediated reads, and coordinated raw providers can cache ciphertext below decryption.
Already materialized plaintext owned by an individual cursor or other execution still needs an exact live-key fence through consumption.
That work depends on the per-unit authority and drain contract for each provider and capture kind.

A hosted PostgreSQL confirmation can advance from the locally adopted checkpoint only after the live recovery owner confirms its identical root and replica boundary.
Directory-managed journals require the installed manifest, active writer scope and usable replay start.
Installed journals compare the selected durable checkpoint against the current writer grant.
The feedback driver retries the checkpoint comparison as publication advances.
The early feedback path continues to require synced local source, replica, and object recovery.

The shared bounded model now requires a matching certificate with a publication quorum before acknowledgement or a fresh decision at the successor cut.
A successor loglet retains the installed decision floor.
Trimming and exact deletion require an installed recovery fence after replacement publication and older replay drain.
The native checker joins observed checkpoint, trim and offloaded-object retirement decisions by scope, revision, root, manifest and journal anchor.
It also checks that each logged receipt follows the observed covering quorum publication for the same stream, epoch and ensemble.
This join covers the recorded recovery and retirement owners.
The full native capture still needs the production T1 certificate-to-query handoff, T2's exact reachability census and recovery exclusion across owners, and traces that connect per-use key and held-view authority to answer delivery.
The new model, controls and native joins have source review only.
