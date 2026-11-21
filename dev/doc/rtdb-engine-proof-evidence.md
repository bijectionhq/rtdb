# RTDB engine verification record

The [property catalog](rtdb-engine-proof-properties.md) defines the obligations and their original baselines.
The [per-property ledger](rtdb-engine-verification-ledger.md) records which parts of those obligations have machine-checked evidence or a reproducible observation.
The results below preserve the source revision and scope of each proof lane.

## Main evidence

The native kernel combined runs record source revisions, tool identities, configuration, diagnostics, and inputs in the [dated evidence ledger](../../verification/results/2026-11-21.adoc).
These results apply to the classic and full-text Rust paths at the particular revisions cited there.

| Catalog targets | Checked evidence on main | Open implementation boundary |
| --- | --- | --- |
| TIME01, TIME02, TIME04–TIME11 | Lean relates the descending ceiling scan to the captured-history point query, including the exclusive query upper bound. Verus checks the production interval intersection, ceiling vector split and overwrite, cell selection, clipping, and per-fragment admission. Both native resolvers agreed with the reference for 4,096 observations. | SQL normalization, snapshot assembly, input coverage and operation order, the event and allocation shell, and the full assembly and projection of Rust fragments. |
| VAL03, VAL04 · Type join and sparse fields | Lean proves associativity, commutativity and idempotence over arbitrary scalar families and flat struct fields. It distinguishes bottom, an explicit null, a field absent from both structs, and a field absent from one contributing struct. | Relate the model to the production type representations and prove nested collections, normalization, and the supported layout contracts. |
| VAL08 · Run-end layout | Lean proves round-trip expansion, strictly increasing exclusive ends, and equivalence of sliced reads to plain reads for arbitrary lists. Kani verifies Arrow's `RunEndBuffer::get_physical_index` for two runs and all admitted slices in each signed run-end width. | Relate the native encoders, representation equality, Arrow access over arbitrary run counts, and all selection, copy, and union paths to the logical model. |
| SCAN02, SCAN03, COMP02, COMP03, COMP08–COMP10 | Lean checks point reads through ordered compaction buckets, conservation of rows in the abstract mixed-trie traversal, and pointwise normalization of same-time cohorts. Native and Verus checks cover the production heap. Separate checks cover the bounded publish-and-adopt transition and exact tiered publication. | Compose actual scan and compaction producers, catalog adoption, and encoded and decoded coverage for all supported histories. |
| TX01, TX03, TX06, TX11, LOG04, LOG05 | The bounded kernel and read-back models cover transaction publication, refusal, and the append, delivery, apply, and acknowledgment sequence. A Verus harness checks production replica admission. | Prove the production transaction and log traces refine those transitions under the provider and transaction assumptions. |
| TX09 | Lean checks strictly ordered system times for arbitrary modeled mixed resolver histories and restart from the last completed time. Bounded TLC and a native catalog-clock replay check other portions of the same contract. | Refine the remaining ingress, time sources, resolve/apply paths, and provider-specific ordering. |
| LOG08, LOG09, GC05, STORE10, STORE11 | Bounded checkpoint discovery and replay models check a readable root and retained suffix, including the handoff at the observed log floor. A native test exercises the newer-checkpoint refresh and suffix replay. | Compose checkpoint publication, root and dependency discovery, trim authority and provider behavior over arbitrary executions. |
| LOG08, FMT01 · Packed log positions | Lean proves the exact signed epoch limit, lexicographic order, quotient and remainder extraction, and the start sentinel. Kani checks the Rust admission predicate and checks encoding and resumption on the admitted domain. JVM tests exercise the same boundary. | Compose the codec with admitted checkpoint metadata, all supported readers, and retained-log recovery. |
| IDX04 | Verus checks the Rust base-four IID bucket function. The Kani harness also checks all legal depths and the independent bit-extraction rule for width 2, 4, or 8 in the actual bucketer. | Prove every admitted row is placed and found under the corresponding serialized and live subtree through insertion, splitting, and replay. |
| SCAN07, GC01, GC02 | Lean proves the abstract generation retention invariant for arbitrarily many readers, objects, in-flight operations and steps. Verus checks the collector's production selection loop against the visibility observations it receives. | Refine the captured references, provider operations, and authority changes, including the full durable deletion protocol. |
| STORE04, STORE08 | An offline Azure provider replay exercised pending-copy acknowledgment and checked the corrected publication path. The checkpoint interruption model separates the provider's durable outcome from the reply. | Cover all provider adapters and their ambiguous, retried, and concurrent publication paths. |
| FTS04–FTS07 | Verus checks the production visibility merge and ranking cutoff decisions, and Lean proves ordered pages are preserved by the specified partition pruning. Native replays compare successful acceleration with the same admitted predicates and scores. | Establish corpus and candidate completeness through every artifact coverage mode, and compose the full search-and-scan refinement. |
| RES01, RES02 · Quantized admission | Lean proves that rounding covers the requested charge, that a settled refusal restores the prior accounting, and that an admitted reservation satisfies both the child and parent limits. The native regression exercises both rounded-grant overflow and total-grant overflow in the actual budget. | Relate arbitrary native ancestor chains and interleavings to the settled admission contract, and prove release and lifetime accounting for all reservation types. |
| COMPAT06 | The runner records checked input and tool digests, commands, diagnostics and the source revision. | Establish provenance across the whole release and compatibility matrix. |

Main's [kernel design](rtdb-formal-verification-design.adoc) gives the public observation and simulation contracts used by those studies.
The checked source boundaries and known counterexamples are documented alongside the [individual studies](rtdb-verification.adoc) and [runner](../../verification/README.adoc).

At baseline `2f1019b68`, the Rust encoder accepted epoch 32,768 and offset zero, returning `-9223372036854775808` and resume offset `-1` for that same epoch.
The signed order places that value below the observed epoch-32,767 position `9223090561878065152`.
The checked epoch domain is 0 through 32,767, with nonnegative 48-bit positions and the before-first sentinel at offset `-1`.
The model runner rejects weakening the upper limit to admit the sign bit or reducing the spacing between epochs. [Position theorems](../../verification/lean/MessagePosition.lean) · [Rust harnesses](../../rust/crates/types/tests/proofs.rs) · [JVM tests](../../core/src/test/kotlin/xtdb/util/MsgIdUtilTest.kt)

The type join model represents normalized scalar families by kind membership and nullability and applies the field rule at each key of a flat struct.
The runner rejects intersecting scalar legs and omitting the null contribution for a missing field. [Type join theorems](../../verification/lean/TypeJoin.lean)

The run encoding model uses decidable value equality and mathematical offsets.
The corresponding Rust check uses Arrow 58.4.1 on a 64-bit target, with positive increasing ends and a valid row in a covered slice.
The model runner rejects a closed run end and a read that drops the slice origin. [Run encoding theorems](../../verification/lean/RunEncoding.lean) · [Arrow harnesses](../../rust/crates/columnar/tests/proofs.rs)

At baseline `5eba8e9ea`, a failing quantized reservation returned `rtdb/allocation-overflow` with child usage `18446744073709551615` and parent usage `0` on a 64-bit build.
The regression uses accounting limits and reservation handles, with no allocation proportional to the requested size.
The Lean model compares counters after a reservation call settles and assumes the rounded grant uses a positive quantum.
The runner rejects omitting rollback and admitting a charge without the parent limit. [Memory-admission theorems](../../verification/lean/MemoryAdmission.lean) · [Native regression](../../rust/crates/types/tests/quantized_budget.rs)

## Multi evidence

The multi models describe the separate `e86af7f12` branch baseline and its directory, Logkeeper, and object-store assumptions.
The standalone Lean and TLA+ artifacts are available under [verification/multi](../../verification/multi).
The multi Rust checks and integration fixtures are recorded at [verification revision `b1e41f938`](https://github.com/bijectionhq/rtdb/blob/b1e41f938919f68d452a94a4ec493ebec7e6be50/dev/doc/rtdb-engine-proof-evidence.md).
The [recovery study](https://github.com/bijectionhq/rtdb/blob/e86af7f121b10fb9832c93fbeafdf9a85a5b7f6a/dev/doc/rtdb-recovery-properties.md#proof-status) enumerates S1–S7, the liveness assumptions, model bounds, and the outstanding production refinements.

| Catalog targets | Scoped evidence | Open implementation boundary |
| --- | --- | --- |
| TIME01–TIME02, TIME04–TIME11 | Lean proves pointwise agreement of modeled range emission and chronological replay for captured, per-IID ordered histories, and Kani checks actual one-split projection and half-open interval intersection on the multi branch. A native SQL oracle compared 1,120 observations spanning pruning settings, flush, compaction and reopen. | Prove the multi resolver slice/ceiling implementation for all range counts and refine its input assembly and SQL observations. |
| SCAN01–SCAN05, COMP02, COMP05 | Lean proves pruning under the premise that dropped events cannot decide the point, ordered-segment replay, and recency early-exit agreement for disjoint ranges. Native SQL exercises pruning and compaction, and Kani checks Rust recency for three ranges. | Prove the pruning premise and range-order obligations at the actual producers. |
| LOG08, FMT01 | Kani checks packed source and replica positions and resumption for epochs 0 through 32,767, nonnegative 48-bit offsets, and the `-1` start sentinel. It checks that all negative raw IDs resume from the start. The same harness is available on main. | Prove that each caller stays in the checked domain and that its persisted checkpoint supplies the correct boundary. |
| CELL09, recovery S2, S5 | The multi PgWire fixture preserves a checkpoint row and acknowledged tail changes after the loss of one, two, and all workers, with the directory, keys and three durable local journals available. | Generalize from the MinIO fixture to every allowed recovery and prove readiness and retention with the declared services. |
| LOG11 and recovery S4 | Lean proves that the first divergence of epoch labels in the compared retained interval is detected at the start or a run boundary. A suffix overwrite reconstructs the winner in an abstract journal that preserves record identity by stream, generation, epoch and slot. Kani checks multi Logkeeper's winner selection over three seal replies. | Refine the production identity and copy transitions, durable admission and recovery, and the Rust repair plan. |

The repair-prefix proof compares records at the same retained slot in one stream, generation, and retained coordinate system.
Its counterexample shows that equal epoch labels alone permit distinct records when the same-slot identity premise is removed. [Repair-prefix proof](../../verification/multi/lean/RepairPrefix.lean)

The journal model derives that premise over arbitrary traces from an empty journal.
It assumes a slot is originated at most once for its complete identity, copies preserve the record, and discarding replica coverage leaves the mathematical history of originations available to the proof.
The comparison assumes both run indexes describe their retained entries. [Retained-journal proof](../../verification/multi/lean/RetainedJournal.lean)

Run the abstract multi lane with Lean 4.36.0 and TLC 1.7.4 from the repository root.
It checks the ledger's catalog coverage, compiles the Lean source, and reruns all 14 bounded configurations, including the expected violations and reachability trace.

```sh
LEAN=/path/to/lean TLA2TOOLS_JAR=/path/to/tla2tools.jar bash verification/multi/check.sh
```

The supplementary Rust position and IID harnesses on main use Kani 0.68.0.

```sh
cd rust
cargo kani -p rtdb-types -p rtdb-trie --tests --output-format terse
cargo kani -p rtdb-types --lib --output-format terse
cargo kani -p rtdb-columnar --tests --output-format terse
```
