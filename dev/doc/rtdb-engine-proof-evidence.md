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
| QRY04, QRY05 · Left join predicate placement | Lean proves that a preserved-side `WHERE` predicate can filter the left input and an unpreserved-side `ON` conjunct can filter the right input. A `WHERE ... IS NULL` counterexample shows why prefiltering the nullable side can add a row despite reapplying the residual predicate. The native optimizer test checks these placements. | Relate the modeled predicates and rows to Rust plan construction and execution, including error and evaluation-context contracts and the other join kinds and rewrites. |
| QRY09, QRY10, SCAN06 · Ordered scans and distinct prefixes | The ordered-index models check sorted merge, early predicate application and keyset continuation, distinct runs and local limits, and page exclusion under valid fences. Production Verus checks cover seek decisions, and recorded native tests cover ordered distinct and keyset queries. | Relate SQL comparison, collation, null and other ordering rules, Arrow page metadata, and arbitrary grouping and window plans to their applicable reference semantics. |
| SESS01–SESS03, SESS06, SESS07 · Explicit transactions | Lean checks read and write phase resolution, preservation of the captured read basis throughout nonterminal commands, ordered buffering, submission at the modeled commit boundary, discard, and terminal closure. Native session tests exercise the same behaviors and a separate native node test retains a null partition entry from the captured basis. | Refine every protocol and runtime session path, including implicit transactions, capture and await, argument batch coalescing, memory failure, cancellation, and submission or replay outcomes. |
| SESS04, SCAN11 · Basis tokens | Lean proves that the componentwise maximum of two equal-length position vectors is the least token covering both, and that the modeled timestamp codec preserves partition positions and non-default time values. Rust tests exercise cross-database await-token merge, exact nanoseconds, and partition order. | Prove provider waits, explicit snapshot visibility, and partition choice in query entry points, including the open isolation semantics for null partition entries. |
| FMT06, FMT07 · Table slug allocation | Lean proves that selecting an unclaimed candidate preserves distinct slugs and recorded entries, and that more distinct candidates than occupied slugs suffice. Rust and JVM catalog regressions exercise the deterministic suffix choice. A native SQL regression persists three formerly colliding names across two checkpoints and confirms their separate rows after a fresh reopen. | Prove the production allocation and catalog replay for arbitrary histories, as well as key encoding across providers, historical aliases, and mixed-version allocation. |
| SCAN02, SCAN03, COMP02, COMP03, COMP08–COMP10 | Lean checks point reads through ordered compaction buckets, conservation of rows in the abstract mixed-trie traversal, and pointwise normalization of same-time cohorts. Native and Verus checks cover the production heap. Separate checks cover the bounded publish-and-adopt transition and exact tiered publication. | Compose actual scan and compaction producers, catalog adoption, and encoded and decoded coverage for all supported histories. |
| TX01, TX03, TX06, TX11, LOG04, LOG05 | The bounded kernel and read-back models cover transaction publication, refusal, and the append, delivery, apply, and acknowledgment sequence. A Verus harness checks production replica admission. | Prove the production transaction and log traces refine those transitions under the provider and transaction assumptions. |
| TX09 | Lean checks strictly ordered system times for arbitrary modeled mixed resolver histories and restart from the last completed time. Bounded TLC and a native catalog-clock replay check other portions of the same contract. | Refine the remaining ingress, time sources, resolve/apply paths, and provider-specific ordering. |
| LOG08, LOG09, GC05, STORE10, STORE11 | Bounded checkpoint discovery and replay models check a readable root and retained suffix, including the handoff at the observed log floor. A native test exercises the newer-checkpoint refresh and suffix replay. | Compose checkpoint publication, root and dependency discovery, trim authority and provider behavior over arbitrary executions. |
| LOG08, LOG10 · Prepared replay | A finite TLC model checks that a declined prefix admission triggers refresh before refusal, that installation follows admission, and that the driver uses the prepared cursor. The native recovery test verifies a newer checkpoint retry and that the driver keeps the admitted cursor. | Cover remote retention after admission, all provider error mappings, and fatal decoding and replay paths. |
| LOG08, CELL09 · Multi trim admission and catch-up | Multi registers a recovery hold before opening an ordinary cursor with a nonempty suffix. Verus checks the pending-trim boundary and the checkpoint and hold limits. A real local cell test observes concurrent physical trim, correct subscriber answers, and trim progress after catch-up. | Refine the logkeeper transport and ensemble-change paths, hosted stop confirmation, and checkpoint-object retention. |
| LOG08, CELL09 · Closing recovery holds | TLC checks ambiguous admission, cancellation, drain and confirmed owner death over 22,494 reachable states. Lean proves that closure preserves other holds and fences all older admissions, including an ambiguous admission with no visible hold. The Multi release worker uses the version-fenced close, and its local reaper has a selective-release test. | Demonstrate native ambiguous admission, hosted stop confirmation, and provider drain for all supported configurations. |
| LOG03, LOG08, STORE10 · Classic Kafka and S3 recovery | The native process fixture checkpoints a prefix to MinIO, acknowledges an update and delete above it, kills all three RTDB processes, and verifies current rows, the fixture table's bitemporal history, and a new write from a fresh worker. | Prove the end-to-end runtime refinement across arbitrary histories, broker and provider failures, retention, and deployment configurations. |
| LOG08, FMT01 · Packed log positions | Lean proves the exact signed epoch limit, lexicographic order, quotient and remainder extraction, and the start sentinel. Kani checks the Rust admission predicate and checks encoding and resumption on the admitted domain. JVM tests exercise the same boundary. | Compose the codec with admitted checkpoint metadata, all supported readers, and retained-log recovery. |
| LOG05 and Multi recovery S3 · Logkeeper seal | Lean proves that replaying any finite sequence of later promise events preserves the seal's lower bound, and composes this with majority intersection for a fixed ensemble. Kani checks production promise replay and empty-stream commit admission across all epoch and sync inputs. | Refine durable journal settlement, message handling, directory-issued epochs and ensemble changes. |
| LOG11 and Multi recovery S4 · Seal reply rank | Lean proves that the selected reply dominates all replies in an arbitrary nonempty list, and that it reaches a retained commit witness's slot in the same epoch or reports a later sync. Kani checks production ranking and its tie choice for one through three replies. | Establish which production replies witness the exact retained payloads and carry that through the repair and journal transitions. |
| LOG12 · Retained history audit | Lean proves chain equality for arbitrary page partitions, continuation from an adjacent digest, and the coverage and equality requirements for strict certification. Native contract tests compare online and stopped-journal digests across epochs and exercise quorum certification across multiple pages and trim. | Relate the accepted online and offline intervals and ordered records to the model under all journal and transport outcomes, including responder selection and physical validation. |
| LOG13 · Journal retirement | Lean proves that replay and compaction preserve the retirement marker, and that completion requires every addressed replica. Kani checks replay of the production `Drop` event and subsequent commit refusal on an empty stream. Native replicas exercise retirement retry, restart, and direct checks for removed payloads. | Prove the runtime journal and fsync transitions, the inventory of all required replicas after copy or move, and the storage and deletion receipt contracts. |
| CELL09, STORE10 and Multi recovery S2, S5 · Worker loss | A local MinIO and PgWire regression retains the checkpoint and acknowledged suffix, compares current rows and the full bitemporal projection, and verifies new writes after losing one, two, and all three compute workers. | Prove the complete activation and recovery path under the published durable-service and liveness assumptions. |
| IDX04 | Verus checks the Rust base-four IID bucket function. The Kani harness also checks all legal depths and the independent bit-extraction rule for width 2, 4, or 8 in the actual bucketer. | Prove every admitted row is placed and found under the corresponding serialized and live subtree through insertion, splitting, and replay. |
| SCAN07, GC01, GC02 | Lean proves the abstract generation retention invariant for arbitrarily many readers, objects, in-flight operations and steps. Verus checks the collector's production selection loop against the visibility observations it receives. | Refine the captured references, provider operations, and authority changes, including the full durable deletion protocol. |
| GC09, STORE03 · Hosted ordered-index retirement | Lean checks point equivalence after scrubbing known erase cuts. A bounded TLC model checks storage retirement, reader release, scrubbed replay, and conditional completion. Integration tests exercise a held reader and versioned local S3 provider. | Compose the directory and provider contracts with every hosted deployment, reader and primary-file custody path covered by physical disposal. |
| STORE03 · Read entitlements | Lean proves a lease timing inequality and preservation of read entitlement across arbitrary abstract pin transitions, including close while a read remains active. | Prove clock, I/O completion, decoded page ownership, cache eviction, and provider lifetime assumptions at production call sites. |
| STORE07 · Adopted ordered-index publication | Bounded TLC checks reachable objects and exact answers under checked source identity and complete upload. It finds a missing reachable object when publication precedes complete upload. | Link the model to production manifest admission and scope, ordinary and stale uploads, fallback reads, and all supported providers. |
| STORE04, STORE08 | An offline Azure provider replay exercised pending-copy acknowledgment and checked the corrected publication path. The checkpoint interruption model separates the provider's durable outcome from the reply. | Cover all provider adapters and their ambiguous, retried, and concurrent publication paths. |
| FTS04–FTS07 | Verus checks the production visibility merge and ranking cutoff decisions, and Lean proves ordered pages are preserved by the specified partition pruning. Native replays compare successful acceleration with the same admitted predicates and scores. | Establish corpus and candidate completeness through every artifact coverage mode, and compose the full search-and-scan refinement. |
| RES01, RES02 · Quantized admission | Lean proves that rounding covers the requested charge, that a settled refusal restores the prior accounting, and that an admitted reservation satisfies both the child and parent limits. The native regression exercises both rounded-grant overflow and total-grant overflow in the actual budget. | Relate arbitrary native ancestor chains and interleavings to the settled admission contract, and prove release and lifetime accounting for all reservation types. |
| COMPAT06 | The runner records checked input and tool digests, commands, diagnostics and the source revision. | Establish provenance across the whole release and compatibility matrix. |

Main's [kernel design](rtdb-formal-verification-design.adoc) gives the public observation and simulation contracts used by those studies.
The checked source boundaries and known counterexamples are documented alongside the [individual studies](rtdb-verification.adoc) and [runner](../../verification/README.adoc).

Lean 4.36.0 checked all seven ordered-index law modules and audited 19 exported theorems on the hosted retirement baseline `1a6eceb63`.
TLC 1.7.4 checked 540 reachable states for each hosted scope safety and conditional liveness model with two publishers and two reader permits.
The liveness check assumes weak fairness for the release and acknowledgement actions stated in the model.
The controls reached three distinct violations for unsettled uploads, undrained readers, and replay of uncleared content.
The original native and Verus checks and their revisions remain identified in the ordered-index record. [Ordered-index evidence](../../verification/ordered-index/README.md) · [Reader entitlement proofs](../../verification/lean/ReaderRetention.lean)

The prepared replay check enumerated 171 states and includes a reachable trace where trimming follows cursor admission.
The separate trim admission model assumes a single durable authority that atomically orders new recovery holds and trim authorization. [Prepared replay model](../../verification/tla/PreparedReplay.tla) · [Trim admission proof](../../verification/lean/ReplicaTrimAdmission.lean) · [Dated results](../../verification/results/2026-11-21.adoc)

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

The left join proof takes total predicates that read only the specified input side and return SQL `TRUE`, `FALSE` or unknown.
It compares ordered lists of row occurrences, preserving multiplicity and null extension. [Outer join theorems](../../verification/lean/OuterJoin.lean) · [Optimizer regression](../../rust/crates/query/src/optimize.rs)

The explicit session model starts with admitted transaction characteristics and takes read-basis capture and DML admission as inputs.
It represents the read basis by an opaque value and the buffer by the ordered logical operations.
The specification leaves isolation for a null partition time in a pinned basis open.
The runner rejects substituting a later basis, writing in a pending read-only transaction, submitting on rollback, reversing buffered order, and leaving a closed state. [Session theorems](../../verification/lean/ExplicitSession.lean) · [Session contract](tx.allium) · [Rust session tests](../../rust/crates/node/tests/session.rs)

The basis-token model assumes matching partition counts and represents the protobuf timestamp as its seconds and nanoseconds fields.
An absent time and an explicit Unix epoch use the same default timestamp and decode as absent, as documented and exercised by the Rust codec test.
The native read-transaction test captures a null partition entry, observes a later write from another session, and retains the same token.
The runner rejects merging positions by taking the smaller value and treating every timestamp in the first second as absent. [Basis token theorems](../../verification/lean/BasisTokens.lean) · [Rust token tests](../../rust/crates/format/tests/persistence.rs) · [Rust partition merge test](../../rust/crates/types/tests/semantics.rs) · [Native query test](../../rust/crates/node/tests/native.rs)

At baseline `4cfbe9807`, local native SQL committed inserts into `"a.b"` and `"a$b"` and returned their separate rows before checkpoint synchronization reported `rtdb.storage/immutable-object-conflict` for `tables/public$a$b/data/l00-rc-b00.arrow`.
The new allocation retains catalog entries with recorded slugs and looks for a free suffix when a fresh table's escaped name is already occupied.
Names-only blocks retain their historical resolution rule.
Lean proves that distinct positive decimal suffixes give distinct keys and that at most one more candidate than the number of occupied slugs is enough.
The preservation theorem assumes the existing registry has distinct slugs.
The runner rejects selecting occupied slugs, dropping recorded entries, and reducing the candidate count to the number of occupied slugs. [Table slug theorems](../../verification/lean/TableSlugs.lean) · [Rust catalog regression](../../rust/crates/index/tests/catalogs.rs) · [JVM catalog regression](../../core/src/test/kotlin/xtdb/catalog/TableCatalogTest.kt) · [Native SQL regression](../../rust/crates/node/tests/native.rs)

The Logkeeper theorems assume that seal replies represent recorded promises for the same journal identities in a fixed ensemble and that later promise events replay with `max`.
The rank theorem assumes a same-epoch seal reply covers the commit slot and proves a rank bound, leaving payload identity and completeness as separate obligations.
The runner rejects writer admission without checking the promise and selecting the smaller reply rank. [Promise theorems](../../verification/multi/lean/ReplicaPromise.lean) · [Winner theorems](../../verification/multi/lean/RecoveryWinner.lean) · [Rust harnesses](../../rust/crates/logkeeper/tests/proofs.rs)

The audit model assumes the same initialization digest, deterministic link function, and ordered retained records across online pages and the offline journal.
It treats unavailable replies as absent and counts one outcome per responding replica.
It checks insufficient agreeing coverage separately from digest disagreement.
The runner rejects resetting the chain at every page and accepting a matching digest without a quorum. [Audit theorems](../../verification/multi/lean/RetainedAudit.lean) · [Native audit tests](../../rust/crates/logkeeper/tests/contract.rs)

The retirement model projects the promised epoch and terminal bit, with distinct addressed replicas and a per-replica settlement outcome.
It assumes the compacted snapshot covers the preceding events and is followed by the original ordered suffix.
The runner rejects dropping the marker during compaction and declaring completion after any one replica reports settlement. [Retirement theorems](../../verification/multi/lean/TerminalJournal.lean) · [Native segment tests](../../rust/crates/logkeeper/tests/segments.rs) · [Native retry test](../../rust/crates/logkeeper/tests/faults.rs)

The worker-loss fixture keeps MinIO, the in-memory directory, local key files, the gateway, and three fsync-enabled journal replicas available.
Replacement compute nodes get fresh cache directories.
With a configured 1.5-second lease, the first integrated Multi run based on `e06764b1f` measured 1.58, 1.59, and 1.47 seconds from initiating worker loss to verifying the first recovered query results.
The run after prepared replay landed at `773adfde9` measured 1.65, 1.73, and 1.56 seconds in the same fixture.
These are local observations with a small checkpoint and suffix. [S3 fixture](../../rust/crates/multi/tests/s3.rs) · [Cell configuration](../../rust/crates/multi/tests/common/mod.rs)

The classic fixture keeps one Kafka broker and MinIO available, uses one partition in each precreated topic, and leaves the topic data and S3 checkpoint in place.
It verifies that all three RTDB processes exited after `SIGKILL` before starting a replacement with a fresh cache path.
On the runtime based on `773adfde9`, two local runs verified the recovered queries 1.08 and 1.10 seconds after initiating worker termination.
One earlier initialization attempt without precreated topics returned `xtdb.kafka/topic-metadata` with `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART`. [Classic process fixture](../../verification/classic-kafka-s3.sh)

Run the classic fixture with Docker, MinIO, the AWS CLI, and `psql` available.
It starts disposable local services and records its logs under `build/proof-runs/classic-kafka-s3`.

```sh
cargo build --locked --manifest-path rust/Cargo.toml -p rtdb-server --features kafka,s3 --bin rtdb
RTDB_CLASSIC_BIN="$PWD/rust/target/debug/rtdb" bash verification/classic-kafka-s3.sh
```

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
| CELL10 · Cross-cell transfer | TLC checks a single open writer, readiness of object and journal copies before routing, source retention through destination selection, and eventual activation under the declared fairness assumptions for one modeled transfer. | Refine production directory changes, object and journal copying, retries, disposal, and routing across hosted deployments. |
| GC10 · Disposal receipt | Bounded TLC checks covered credential expiry, separated inventory observations, and absence of modeled objects after a receipt. Its controls witness violations when credential or inventory requirements are weakened. | Refine actual inventories, provider completion, keys, journals, archives, and all namespace and installation identities covered by a receipt. |
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

The supplementary Rust position, IID, and Logkeeper harnesses on main use Kani 0.68.0.

```sh
cd rust
cargo kani -p rtdb-types -p rtdb-trie --tests --output-format terse
cargo kani -p rtdb-logkeeper --features simulation --tests --output-format terse
cargo kani -p rtdb-types --lib --output-format terse
cargo kani -p rtdb-columnar --tests --output-format terse
```


### Gateway write replay

Lean checks that resetting SQL settings preserves the effective transaction key and that reinstalling the same private command key and ordinal reproduces it.
The model takes any string application key as a fixed input.
It also proves that an arbitrary finite sequence of replacements retains the first recovery cutoff.
TLC checks a single unexpired command, a bounded clock, and an atomic duplicate guard.
The model finds duplicate mutations when SQL settings own the key or malformed application metadata suppresses the fallback, and finds deadline renewal when writer loss resets the cutoff.
Verus checks the exact Rust cutoff selector and rejects a mutation that replaces an existing cutoff.
The native Pgwire regression runs both duplicate cases against a real engine, and the gateway tests check repeated replacement and legacy refusal.
Durable idempotency-table refinement, time and expiration assumptions, volatile client-supplied application keys, and general multi-statement replay remain separate obligations. [Lean proof](../../verification/multi/lean/GatewayReplay.lean) · [TLA+ model](../../verification/multi/tla/RTDBGatewayReplay.tla) · [Verus checks](../../verification/verus/replay_deadline/area.json) · [Pgwire tests](../../rust/crates/pgwire/src/protocol.rs) · [Gateway tests](../../rust/crates/multi/src/session.rs)
