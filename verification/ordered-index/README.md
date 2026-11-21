# Ordered index verification

The reference answer resolves the original captured event stream at the requested system and valid points, filters its rows, sorts by the complete order key, and takes the requested page.
The proof sources in this directory decompose that specification.
Their first revision has received source review and awaits an execution report.

| Property | Proof source | Production refinement still required |
| --- | --- | --- |
| Temporal projection and exact membership | `Temporal.lean` proves point lookup against independent event interpretation and a single visible span per IID. `Buckets.lean` proves exact coverage for a recursive dyadic decomposition. | Connect RTDB's newest-first temporal resolver, coalescing, and greedy bucket iterator to the model. |
| Signed timestamps and unbounded end | `domain.rs` includes the production timestamp conversion directly in the Verus harness. | Connect the admitted query point and the bucket iterator's shift and alignment to the conversion contract. |
| Ordered seek, eligibility and pagination | `SeekRange.lean` states the monotone seek partition and fence pruning laws. `seek.rs` includes the production binary search and directed slot lookup. `OrderedRead.lean` proves ordered merge, filtering before limit, and keyset continuation. | Connect Arrow values and SQL comparisons to the order and filters, and the tree cursor and heap to the ordered streams. |
| Replay and fallback | `Recovery.lean` proves interchangeable complete checkpoints and ordered tails. It also states exactly when the event count completes coverage. `Reachability.lean` states layout identity across a checkpoint split. | Establish that the canonical flushed inputs and live offset are an ordered substream of the exact capture. |
| Publication and reclamation | `Checkpoint.tla` models a captured reader, two workers, immutable objects, publication, collection, fallback, and crash. The two control configurations remove checked source identity and complete upload. `Reachability.lean` proves that a stale mark preserves later reachable pages under the per-block sharing rule. | Prove the sharing rule for `Tree::persist` and `store_roots`, and connect reader retention, leadership and immutable writes to the protocol. |
| Erasure | `Temporal.lean` proves that erase cuts the captured IID's prior history, including historical system-time reads. | Connect revision-directory scrubbing and physical collection across object and node caches to the model. `ErasureCleanup.tla` models the durable obligation across publication, crash, advancement and repair under a single fail-stop owner. |
| Bijection page witnesses | `OrderedRead.lean` proves that agreement over the consumed ordered prefix preserves the page, including a lookahead when it is observable. | Implement and prove the Bijection-to-RTDB refinement for world ancestry, labels, custody and range witnesses. The current RTDB accelerator serves SQL. |

`CheckpointCountOnly.cfg` should produce a wrong answer when a replay substitutes an event with a duplicate while preserving the count.
`CheckpointEarlyPublish.cfg` should expose a reachable manifest that references a missing object.
The model represents a page with its complete source prefix and abstracts page bytes, hash validation, and physical snapshots.
An accepted model run establishes its invariants within the configured bounds.

The sharing rule relies on page addresses deriving their generation from the source block that first writes them.
If a full rebuild stamps earlier pages with its requested final frontier, it can produce different bytes for an occupied immutable manifest slot.
The two-block recovery regression in `rust/crates/node/tests/native.rs` captures a manifest, removes a page, reopens, and requires the rebuilt manifest to retain its original bytes.

The cleanup marker is written before storing a scrubbed archive or starting a full replay repair.
A later checkpoint sweeps pending markers within its frontier, and clears them after deleting the unreachable pages.
`ErasureCleanupForgetOnAdvance.cfg` removes that carry-forward rule.
A physical erasure theorem across overlapping maintenance owners still needs a fence on page writes and a model of each replica's cached copies.

## Execution

Use Lean 4.35.0, Bijection's pinned Verus toolchain, and TLC from the authorized proof environment.
Write generated proof products and TLC state under an ignored task target directory.
Run each Lean module and compile `Temporal.lean` into the module search path before checking `Recovery.lean`.
Run Verus on `domain.rs` and `seek.rs` in this directory so the included paths resolve to the source used by RTDB.
Run TLC against each configuration, retaining the source identity, tool version, and each diagnostic trace.
