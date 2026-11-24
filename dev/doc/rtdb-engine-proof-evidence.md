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
| VAL08 · Run-end layout | Lean proves round-trip expansion and connects Arrow's hit and insertion-point cases to the same sliced row for arbitrary run counts. Kani checks Arrow's concrete lookup for two runs in each signed run-end width. | Establish the actual array and buffer contracts, and refine representation equality, selection, copy, and union paths. |
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
| IDX02, IDX04, IDX06, IDX07 | Verus checks the production Rust base-four IID digit, path comparison, partition, and recursive and one-level split. The Kani harness also checks all legal depths using independent bit extraction for widths 2, 4, and 8. | Check the native node construction and live insertion and traversal boundaries, then refine JVM splitting and connect both trees to replay. |
| IDX02, IDX03, IDX07, IDX09 | Verus checks the production native sort, merge, persisted order validation and leaf split. OpenJML checks order and multiplicity of the production JVM merge. An injective logical rank uniquely determines an ordered leaf from its row occurrences. | Establish IID stability at each runtime reader, prove JVM pending-run sorting, and propagate stored-run order through insertion, caches and traversal. |
| IDX01, IDX08 | Verus checks production Rust row addressing and proves that appending or joining adjacent chunks preserves the exact 16-byte IID at a retained ordinal under the ordered payload contract. Lean proves the corresponding arbitrary-history law. | Establish the Arrow and cached-header content contract and refine snapshot ownership across block transitions in both engines. |
| SCAN07, GC01, GC02 | Lean proves the abstract generation retention invariant for arbitrarily many readers, objects, in-flight operations and steps. Verus checks the collector's production selection loop against the visibility observations it receives. | Refine the captured references, provider operations, and authority changes, including the full durable deletion protocol. |
| GC09, STORE03 · Hosted ordered-index retirement | Lean checks point equivalence after scrubbing known erase cuts. A bounded TLC model checks storage retirement, reader release, scrubbed replay, and conditional completion. Integration tests exercise a held reader and versioned local S3 provider. | Compose the directory and provider contracts with every hosted deployment, reader and primary-file custody path covered by physical disposal. |
| STORE03 · Read entitlements | Lean proves a lease timing inequality and preservation of read entitlement across arbitrary abstract pin transitions, including close while a read remains active. | Prove clock, I/O completion, decoded page ownership, cache eviction, and provider lifetime assumptions at production call sites. |
| STORE07 · Adopted ordered-index publication | Bounded TLC checks reachable objects and exact answers under checked source identity and complete upload. It finds a missing reachable object when publication precedes complete upload. | Compose the [runtime-installed format bindings](../../verification/ordered-index/README.md#runtime-installed-format-bindings) with the abstract publication and scope premises, ordinary and stale uploads, fallback reads, and provider behavior. |
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

### Arrow run-end lookup

Lean checks the hit and insertion-point cases in Arrow 58.4.1's `RunEndBuffer::get_physical_index`.
The binary search is assumed to return a matching position on a hit or partition the run ends around the query on a miss.
For any strictly increasing covered run list, an exact hit advances to the following run and a miss uses the insertion point, giving the first end strictly greater than the absolute row.
The resulting run value agrees with the corresponding plain row for an arbitrary encoded slice.
The signed coordinate cast preserves the slice origin and relative row whenever their sum fits the selected signed width.
Lean 4.35.0 audited nine theorems and rejected five controls, including staying on the matched exclusive end and reading before the insertion point.
The concrete Arrow and fixed-width byte readers still need refinement against their buffer and array invariants. [Run encoding and physical position theorems](../../verification/lean/RunEncoding.lean) · [Bounded Arrow harnesses](../../rust/crates/columnar/tests/proofs.rs)

### Memory trie lookup and coverage

Lean checks that every split and finite sequence of modeled insertions preserves all row occurrences and that a routed lookup agrees with filtering the original insertion history by IID.
Regrouping the same history into different batches preserves both observations for any split policies, including policies that produce different tree shapes.
The model assumes a stable IID reader and the same deterministic bucket function for routing and lookup.
Lean 4.35.0 checked five exported theorems using only `propext` and `Quot.sound`.
The runner rejected controls that drop the maximum-depth leaf and look in the wrong child. [Lean model](../../verification/lean/MemoryTrie.lean)

The Rust test compares public trie insertion, leaf traversal, compaction, and persisted restore with a flat sort-and-filter oracle.
It covers five configurations, all four top-level buckets, a maximum-depth collision, repeated admitted row indices, an absent IID, and an earlier retained trie.
The native trie suite passed 20 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie`. [Rust contract test](../../rust/crates/trie/tests/contracts.rs)

Remaining work includes a machine-checked refinement of the Rust and JVM implementations, including leaf sorting, pending logs, traversal order, and the production lookup and row-reader call paths.

### Production IID path comparison

The Rust memory trie uses the Verus-checked base-four comparison for default-width root-path admission and for validating restored leaves.
The public bucketer validates the path first.
For any IID and any path of length at most 64, the proof establishes that equality means every path digit matches and that the first differing digit determines any non-equal result.
The native contract test compares the public bucketer with an independent unsigned lexicographic oracle at every path depth, including the empty path and a first difference at the last digit.
Verus 0.2026.11.05.31e44e6 checked nine functions and rejected all five mutations, including one that accepts an IID with a smaller first differing digit. [Verus study](../../verification/verus/iid_partition/area.json)
The native run passed 495 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Rust contract test](../../rust/crates/trie/tests/contracts.rs)

### Same-block staged resolution prefix

Lean checks that a queue can extend a captured snapshot exactly when each write names the current predecessor and block.
The resolver view places staged batches in newest-first order ahead of the applied history.
Moving the first stage into the applied prefix leaves the resolver's bitemporal answer unchanged if the published snapshot matches the overlay, including its block and transaction coordinates.
Staging leaves the external snapshot in place, and discarding the remaining queue preserves the applied prefix.
A batch with no rows in the table still advances the projected history's transaction coordinate.

The native regressions reject reversed predecessors and a wrong block in `with_staged`.
They check that moving an applied head out of the queue preserves the resolver basis and that captured readers cannot see staged data.
A separate regression reads the predecessor's staged write and then the previous argument row's write within the current transaction.
The proof takes one table's list of logical effects as input and assumes the segment and publication layers preserve that history.
The concrete Arrow segment representation, block turnover, and read-back fencing still need composition with those premises. [Lean model](../../verification/lean/StagedPrefix.lean) · [Native overlay tests](../../rust/crates/tx/tests/resolution.rs) · [Snapshot construction](../../rust/crates/index/src/snapshot.rs)

### Ordered apply produces the same leader and follower prefix

Lean checks that a successful local apply links the staged transaction to the partition's current transaction and block, advances the transaction key, keeps the source position nondecreasing, and appends the transaction to the published history.
The follower constructs the same staged link using its own prior state.
Starting from the same published state within one installed block, the follower reaches the same modeled state for every successful local prefix whose codec round-trips each logical transaction body.
The model stores table effects and outcomes as abstract payloads in order.
The controls remove the predecessor or source-order requirement, discard the published history, or stop the follower after one record. [Lean prefix model](../../verification/lean/ReplicaPrefix.lean)

A native regression runs the same admitted frames through a staging leader and a decoding follower.
After each of five transactions it compares published tables, schemas, typed events, transaction results and source progress.
The sequence includes a zero-row declaration, a transaction writing two tables, an abort after a write, an update and a delete. [Native replication tests](../../rust/crates/replication/src/tests.rs)

Full refinement still needs the production Arrow and Transit codecs to establish logical-body preservation across supported types, plus composition with block adoption, restart and the JVM path.

### A leader's read-back agrees with its pending transaction

Before handing the encoded replica frame to the log provider, the append pump publishes the assigned sequence and SHA-256 fingerprint together for a pending resolved transaction.
The driver checks an admitted resolved transaction at its own term against that pending sequence and fingerprint before the record can enter a hold buffer.
For an oversized record, it compares the fetched frame after checking the inner term and sequence against the admitted outer envelope.
Before applying the transaction, the driver also checks that the pending append owns the resolver's oldest staged transaction.
That staged transaction supplies the applied rows, catalog operation, result and source progress.

Lean checks that publishing requires both a confirmed append and the corresponding staged head.
Its frame agreement theorem assumes the compared frames do not collide under the fingerprint.
The four controls remove the sequence, record kind, fingerprint, or queue-head requirement.
Verus checks the exact production pending-sequence predicate and rejects defaulting an absent sequence to zero or treating a later sequence as equal. [Lean model](../../verification/lean/ReadBackIdentity.lean) · [Verus study](../../verification/verus/readback_identity/area.json)

A second Lean model proves that, under FIFO durability and delivery within one term, the transaction queue is the concatenation of the held read-backs, unread durable transactions and transactions awaiting durability.
The oldest held transaction is then the pending head.
Publishing consumes that head, and ending the term preserves the published prefix and the durable replay candidates while dropping the local pending queue.
The model projects out control frames and leaves admission of replay candidates to the term fence. [Lean queue model](../../verification/lean/QueuedReadBack.lean)

The native regressions exercise the appender and driver with an absent transaction ID at transaction zero, changed transaction metadata and error bytes, a control or unrecognized body at a pending transaction's sequence, a missing append or staged head, and a changed table payload or sequence fetched behind an oversized envelope.
They check that a rejected record leaves the transaction's partition state, catalog effects and applied progress untouched.
The positive oversized case reaches the original caller with the corresponding transaction result. [Native replication tests](../../rust/crates/replication/src/tests.rs)

The native interleaving regressions confirm a transaction while the provider's enqueue call still holds back its receipt, and reject a changed frame while an in-flight resolution owns the resolver queue.
The second regression checks that demotion has no mismatched held transaction to replay.
Lean checks a single pending write across all schedules of stamping, provider visibility, receipt, delivery and term end.
The model admits a valid frame before the receipt arrives, prevents a changed frame from becoming held or imported, and prevents a frame made visible after term end from applying in the ended term. [Lean interleaving model](../../verification/lean/ReadBackInterleaving.lean) · [Native interleaving regressions](../../rust/crates/replication/src/tests.rs)

End-to-end refinement still needs the concurrent appender and driver to refine the queue model, including a provider append that becomes durable after cancellation and replay through a later term.
The frame check applies to the native driver when it admits read-back at the current leader's term.
The Kotlin path checks the transaction ID against the staged head before applying that head's catalog operation.
A JVM regression supplies an attach in a read-back with no matching staged transaction and checks that the catalog and applied progress stay unchanged.
The existing attach regression checks that the catalog effect occurs on valid read-back. [JVM leader tests](../../core/src/test/kotlin/xtdb/indexer/LeaderLogProcessorTest.kt)
The remaining Kotlin refinement needs a check of exact append and frame agreement beyond comparing their transaction IDs.
Inherited recovery still relies on the log and object providers to return the stored records and on the follower decoder to reconstruct the same effects.

### Progress waits use their observed watermark

Lean checks terminal-state selection for both engines on a single observed progress snapshot.
The Kotlin comparison covers transaction and source waits and the ingestion-fault state.
The model's separate stopped state represents the native monitor.
A native wait can observe progress that reached its target before ingestion failed.
A Kotlin wait observing a failed state receives an `IngestionStoppedException` carrying the fault.
An unmet request continues waiting on an active snapshot and reports failure when it observes a terminal snapshot.
An older transaction request can observe that its watermark was reached while receiving no exact result from the monitor.
Cancelling one pending wait preserves the other subscription and the shared snapshot. [Lean wait model](../../verification/lean/ProgressWait.lean)

The native regression polls two waiting subscriptions, cancels one, then publishes a transaction and source progress to the other.
It checks the surviving wait's transaction result, source position and resume token together, and checks that an older transaction ID receives no newer result.
Existing native coverage exercises the reached-prefix and unmet-fault cases. [Native progress tests](../../rust/crates/replication/tests/progress.rs) · [Kotlin watchers](../../core/src/main/kotlin/xtdb/api/log/Watchers.kt)

Scheduler liveness, concurrent notification and cancellation races, and restart refinement remain open.

### Limit opening and count admission

The ordinary native limit opens the input before evaluating OFFSET and then FETCH.
An input that evaluates its rows eagerly can fail on open.
An expression deferred by a projection remains deferred when the bound fails.
A present FETCH and valid bounds that can be read directly from literals or captured parameters enable an ordered seek.
An invalid count falls back to ordinary opening, where the earlier input error keeps its precedence.

Lean proves the abstract open and bound order, the preservation of bound-expression errors, and equality of successfully decoded direct and ordinary counts.
The direct-count law assumes the expression denotes the captured scalar that the endpoint reads.
Verus checks the production count decoder and the capped offset-plus-fetch prefix, including the signed endpoint and overflowing unsigned sums.
Native statement tests exercise eager and deferred evaluation, and the ordered-index tests require both literal and parameter counts to seek without an ordinary candidate scan.
Full refinement still needs the async opener, the expression evaluator and optimizer's ordering and resource contracts. [Lean model](../../verification/lean/LimitOpen.lean) · [Verus study](../../verification/verus/limit_bounds/area.json) · [Production operators](../../rust/crates/query/src/operators.rs) · [Statement regression](../../rust/crates/node/tests/statement.rs)

### Submission clock and durable retry attribution

Ordinary keyed submissions on main use engine-owned receipts that bind the authenticated principal and request body to a committed outcome.
An older source format records the submission sample and position of its generated assertion.
The reader preserves that format and attributes a historical retry only when the resolver identifies that assertion at the encoded position.
It finds the original marker in the historical system-time snapshot using the encoded submission sample.
Verus checks the finite clock arithmetic and attribution by operation index.
A native regression reads an older source record, resolves a typed duplicate, and recovers its outcome after restart. [Native tests](../../rust/crates/node/tests/native.rs) · [Verus study](../../verification/verus/idempotency_window/area.json) · [Source transaction contract](db.allium)

### Private replay admission and marker authority

The Rust session accepts a trusted frontend's permit at synchronous commit of one buffered write operation.
The node takes a single submission sample after admission and refuses a sample at or after the permit's exclusive cutoff before source append.
A conflicting application key also refuses submission.
The wire record contains the effective key, admitted sample and cutoff.
Source recovery recomputes the validity interval and rejects incomplete or conflicting guard envelopes.

The resolver reads markers against the ordered resolution prefix before user operations run.
It writes the first successful command's marker together with the user's writes and the transaction receipt.
The marker lives in the reserved `xt` schema, which rejects ordinary DML and direct external event writes.
A duplicate records the original transaction key in its abort receipt and skips the user operations.
The await path returns the original committed key from that receipt after live apply or recovery.
A caller error during marker lookup aborts that attempt and leaves the resolver available for subsequent attempts.
An external-source writer's abort with code `rtdb.tx/private-replay-duplicate` retains its original error.

Verus checks the finite half-open interval and the strict cutoff in the production arithmetic.
Lean proves at most one user effect for arbitrary resolution sequences for one key and cutoff, including aborted attempts and clock samples that arrive in either order.
The installed marker overlaps every admitted submission's valid-time range, and later resolutions preserve its original outcome.
In the model, a caller error during marker lookup aborts that attempt and preserves the stored marker and user-effect count.
The model assumes one authoritative ordered prefix, atomic publication of user effects and the marker, and a common key and cutoff supplied by the trusted frontend.
Native regressions cover cutoff equality, conflicting keys, parallel attempts, out-of-order clock samples, expiry beyond the ordinary window, rejected writes to the marker, and recovery both of a flushed marker and of the duplicate receipt.
The format regression checks required field presence and distinguishes a zero timestamp from absence.
The deployed contract still depends on frontend eligibility and matching resolver support across the log's eligible writers.
Stable replay of aborted outcomes remains open. [Lean model](../../verification/lean/PrivateReplay.lean) · [Verus study](../../verification/verus/idempotency_window/area.json) · [Node tests](../../rust/crates/node/tests/native.rs) · [Wire tests](../../rust/crates/tx/tests/submission.rs) · [Transaction contract](db.allium)

The published Multi integration test withholds a committed UPDATE's reply, moves the deployment to another writer, and checks that the same client receives a completion and reads one increment. [Writer replacement regression](https://github.com/bijectionhq/rtdb/blob/7b4b6cd5427bbf04c841729eb532c18839b348e7/rust/crates/multi/tests/cell.rs#L2566)

### Production live-trie bucket grouping

Rust branch insertion and leaf splitting call the same checked grouping function.
For an arbitrary finite input at any branching depth, Verus proves that each successful child sequence is the input filtered by that child's base-four IID digit, including repeated rows and their original order.
On an IID read failure at a branching depth, the function returns that first modeled read error.
The proof assumes a finite exact-size iterator obeying the Rust iterator specification and a terminating reader whose results agree with the supplied per-row model.
The child-path law composes the production comparator and digit selector to show that child membership is parent membership plus agreement on the next digit.
The native tests exercise the last IID digit, repeated rows, and a read failure whose suffix remains unread.
Verus 0.2026.11.05.31e44e6 checked thirteen functions and rejected all eight mutations, including wrong-child placement, reversed order, and admitting an unreadable row.
The native run passed 497 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production grouping and tests](../../rust/crates/trie/src/partition_step.rs) · [Verus study](../../verification/verus/iid_partition/area.json)

### Production leaf split

Native overflow insertion and log compaction call the same checked subtree builder.
Verus proves that a successful split preserves the input root, places every indexed occurrence in a matching leaf prefix, and preserves rank order within each resulting leaf.
The sum of the occurrence counts in the result equals the count in the original run for each ordinal.
Each constructed child uses the prefix given by its bucket slot, and the proof checks the depth bound on all recursive calls.
A requested one-level split adds at most one level.
A recursive split ends at a fitting page or at maximum depth.
The proof assumes a valid ordered input run, a fixed IID lookup result per ordinal during the split, and a node constructor that reflects its path and leaf runs in bucket order.
The native regressions compare one-level and maximum-depth recursive splits of a repeated-ordinal run and replay an IID failure during recursion through the public insertion API.
Verus 0.2026.11.05.31e44e6 checked twenty-five functions and rejected thirteen controls, including dropping a child, swapping child slots, appending an incorrect path digit, splitting past maximum depth, and ignoring one-level mode.
The native run passed 510 tests with `CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production split](../../rust/crates/trie/src/leaf_split_step.rs) · [Native node construction](../../rust/crates/trie/src/memory.rs) · [Verus study](../../verification/verus/iid_partition/area.json)

### Trie sibling traversal order

Lean proves that the base-four path orders IIDs by unsigned byte order and that depth `d` selects byte `d / 4` and lane `d % 4`.
Verus separately checks that formula in the production Rust selector.
Under a stable 16-byte IID for every ordinal, rows from an earlier child slot precede all rows from a later child slot in the leaf rank.
For an ordered source run, if the visited child runs match their prefixes, are individually ordered and preserve each ordinal's count, concatenating them in slot order gives the exact original sequence.
The count premise includes repeated ordinals.
This composes at each branch given the ordered child runs, occurrence counts, and common IID interpretation.
The production traversal and node adapter still need a refinement proof against those premises.
Lean 4.35.0 audited seven theorems and rejected controls that select the wrong byte at depth, reverse sibling order, or drop the first visited row. [Sibling order proof](../../verification/lean/TrieSiblingOrder.lean) · [Checked Rust digit selector](../../rust/crates/trie/src/partition_step.rs) · [Checked subtree construction](#production-leaf-split)

### Production leaf merge

The production Rust merge reads both input heads through the IID reader and passes their keys and ordinals to the checked comparator.
Verus proves that with ordered input runs, representable allocation capacity, and stable IIDs on successful reads, the resulting run is ordered and contains every occurrence from the two runs.
The checked invariant accounts for each row exactly once between the emitted prefix and the two unread suffixes.
An injective rank gives one ordered output sequence for that multiset.
The error postcondition requires the returned error to come from a read of an input row.
The Rust tests pin the order of the reads, stopping on the first failure, and copying the unread tail.
An additional regression runs through the public leaf API and checks that a failed merge publishes no cache entry, a retry yields the complete sorted leaf, and later reads reuse that cached result.
The merge controls include reading the wrong head or returning a partial result as success after an IID read failed. [Production merge](../../rust/crates/trie/src/leaf_merge_step.rs) · [Leaf retry regression](../../rust/crates/trie/tests/contracts.rs)

### Production leaf sort and composition

Verus checks the native decoration loop that reads the IID for each pending ordinal and the projection that extracts every ordinal after sorting.
The proof assumes iterator laws, stable IIDs on successful reads, and the Rust standard library's `sort_unstable_by` contract that a total-order comparison yields an ordered permutation on return.
The actual comparison in the sort is checked against the same rank used for merging.
The composition used by a cache miss sorts the pending log and merges it with the stored run.
The composition used by a bulk overflow sorts the new batch and the pending log, merges them, and merges the result with the stored run.
Given an ordered stored run and representable total capacity, both results are ordered and contain every input occurrence, including duplicate ordinals.
A returned error comes from an input-row lookup.
The new native regression checks that a read failure stops each sort, a retry returns the full duplicate-preserving order, and a failed bulk insertion leaves the original trie readable.
Verus 0.2026.11.05.31e44e6 checked thirty-one functions and rejected fourteen mutations, including reversing the sort, discarding projected rows, and losing a run at either composed merge.
The native run passed 506 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production sort and composition](../../rust/crates/trie/src/leaf_sort_step.rs) · [Trie call sites](../../rust/crates/trie/src/memory.rs) · [Verus study](../../verification/verus/trie_leaf_merge/area.json)

The consolidated branch uses the Rust `Ord` interface for keyed rows and calls `sort_unstable`.
Verus checks the three comparison methods against the existing rank and discharges the standard sort contract's ordering-law premise.
The sort still relies on the standard-library contract for preserving every row occurrence and returning the order specified by that rank.
The runtime comparison uses the same IID order and descending ordinal tie-break. [Verus standard-library specification](https://github.com/verus-lang/verus/blob/9da409b/source/vstd/std_specs/slice.rs#L796-L804)

### Production leaf comparison

The Rust pending-log sort, two-run merge and persisted-leaf validation use the same row-key comparison.
Verus checks that the production comparator returns Less exactly when the logical rank is smaller and Equal exactly when both IID and ordinal agree.
The proof takes the standard Rust array comparator's lexicographic contract as a library assumption and uses Verus's specification for unsigned integer comparisons.
It checks the row tie-break and the combination with IID order in the actual Rust function.
The comparison controls reverse either the IID comparison or the row tie-break in the production comparator.

The JVM comparator uses Arrow's comparison of sixteen-byte IID views and compares row indices in descending order when the IIDs match.
Inspection of Arrow 19.0.0 bytecode found two word comparisons with byte-order normalization and a sign-bit flip before signed long comparison.
Lean proves that the sign-bit flip realizes unsigned comparison and that comparing the two words and then the reverse ordinal agrees with the specified leaf order.
The mapping assumes both readers deliver each row's stable sixteen IID bytes, the Arrow word loads represent those bytes in the documented order, and admitted row indices fit the JVM representation.
Lean 4.35.0 audited seven theorems and rejected signed-word comparison, reversed IID comparison, older-first row ties and a reversed ordinal suffix in the rank.
The Rust and JVM regressions each insert 4,096 rows and check the leaf against an independent order reference across single-byte values at every IID position, including duplicate IIDs.
The native run passed 504 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`.
Run the JVM case with `./gradlew :xtdb-core:test --tests 'xtdb.trie.MemoryHashTrieTest'`.
JVM pending-run sorting and end-to-end refinement of leaf publication through the Rust and JVM entry points remain open. [Comparator study](../../verification/verus/trie_leaf_merge/area.json) · [JVM order proof](../../verification/lean/IidByteOrder.lean) · [Rust regression](../../rust/crates/trie/tests/contracts.rs) · [JVM regression](../../core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt)

### Production JVM leaf merge

The Kotlin leaf calls the checked Java merge during a cache fill, log compaction, and bulk overflow.
On normal completion OpenJML proves the result is ordered by rank, both input arrays retain their contents, and a bijection matches every input position to a result position with the same row index.
The position correspondence preserves duplicates even when the same ordinal appears in both runs.
The proof assumes ordered input runs, representable allocation capacity, and a comparator whose results agree with a stable rank and whose writes are confined to its scratch state.
The Kotlin adapter uses the existing Arrow lookup and comparison, whose rank correspondence is described in the comparator study.
The regressions exercise duplicate ordinals across stored data, pending logs and a bulk batch, and retry a cache fill after the original IID lookup exception propagates.
OpenJML 21.0.29 with bundled Z3 5.1.0 checked the merge and rejected three controls that select the larger head, omit an unread suffix, or replace an emitted value.
Run the production proof with `openjml --esc --timeout=40 --timeout-method=65 --esc-max-warnings=1 --progress core/src/main/java/xtdb/trie/LeafMerge.java`.
The `--openjml` option of the verification runner checks the baseline and its controls. [Production Java merge](../../core/src/main/java/xtdb/trie/LeafMerge.java) · [Kotlin call sites](../../core/src/main/kotlin/xtdb/trie/MemoryHashTrie.kt) · [JVM regressions](../../core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt)

### Native persisted leaf order

The native importer checks each decoded row's path before calling the verified order validator on the stored run.
Given stable IID reads, Verus checks that success establishes the leaf rank order across every pair of positions.
If the validator returns an order error, the first inverted adjacent pair follows an ordered prefix.
A reader error propagates when either lookup of the current pair fails.
The regressions check repeated indices, an inversion confined to the final pair and a failed read after the path pass.
The persisted-order area uses the comparator and rank contracts checked by the leaf merge area.
Verus 0.2026.11.20.9da409b checked three functions for persisted-order validation and rejected four controls that skip the final pair, read the wrong row, admit an inversion, or reject equal rows.
The branch run passed 508 native tests with `CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production validator](../../rust/crates/trie/src/leaf_load_step.rs) · [Importer](../../rust/crates/trie/src/memory.rs) · [Verus study](../../verification/verus/trie_leaf_load/area.json)

### Leaf runs, compaction and persisted order

The Lean leaf model starts with an ordered compacted run and arbitrary pending rows.
It relates each edit to an independent row history that records appends and bucket filtering.
Across arbitrary sequences of modeled inserts, cache fills, compactions, overflow merges and splits, a read returns the unique ordered sequence for that history, including repeated row occurrences.
The general proof assumes a stable, injective rank and specializes it to the packed IID and descending ordinal rank for a stable IID reader.
A child split uses the stable filtered run for its bucket, and the overflow case sorts both pending runs and merges them into the compacted run.
The proof that checking each adjacent pair implies full order supplies the loader's starting invariant for the native persisted leaf.
A JVM protobuf leaf starts with the order serialized by the producer and requires a valid ordered input.
Lean 4.35.0 audited seven theorems and rejected four mutations covering stale cached rows, lost pending rows on overflow, missing bucket filtering and skipped adjacent comparison.
The model uses Lean's proved list sort and merge.
Connecting all production sort, merge, restore and reader paths to those model transitions remains an implementation refinement. [Leaf-run proof](../../verification/lean/LeafRuns.lean) · [Native leaf operations](../../rust/crates/trie/src/memory.rs) · [JVM leaf operations](../../core/src/main/kotlin/xtdb/trie/MemoryHashTrie.kt)

### Live row addresses across chunk layouts

The Rust live table uses the checked search for row and header reads, cached leaf addresses, and the block-copy path.
Verus proves that the search returns the last chunk start at or before the requested ordinal and the exact relative offset.
The owner law assumes contiguous chunks and proves that every in-range row has one nonempty owner, even when empty chunks cause repeated starts.
The production append preserves the layout and existing addresses on success and leaves the chunk vector intact on count overflow.
Replacing the last two chunks preserves the layout when the replacement has their combined length.
The ownership law composes with the replacement function to prove the new address of every existing row, including displacement by the first chunk's length for a row from the second.
The compact address API reports an error if the chunk index exceeds its 32-bit representation.
Lean proves that the returned address reads the same value as the flat layout and that arbitrary appends and contiguous chunk joins preserve every captured row ordinal.
The join model assumes the Arrow concatenation produces the original rows in the original order.
The native tests check chunk boundaries, repeated starts, retained reads after actual chunk coalescing, and trie reads from snapshots retained through discard and block rollover.
Lean 4.35.0 audited five theorems and rejected both mutations.
Verus 0.2026.11.05.31e44e6 checked nine functions and rejected five mutations.
The native run passed 503 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Lean model](../../verification/lean/LiveRowAddress.lean) · [Verus study](../../verification/verus/live_row_address/area.json) · [Snapshot regression](../../rust/crates/index/tests/live_index.rs)

### Retained IID contents in native chunks

Verus checks the production row-address search, append, and tail replacement with chunks carrying sixteen-byte IID values.
The checked reader returns the value at the unique owning cell of every in-range ordinal, even when empty chunks repeat a start address.
Appending a chunk preserves that value for every retained ordinal, including when an overflowing append returns an error.
Replacing the tail pair preserves the same value when the replacement's cells equal the earlier payload followed by the later payload.
The study rejects reading the first cell instead of the located cell, ignoring the appended length, and reversing the two payloads while preserving their combined extent.
The payload premise still needs to be established by Arrow concatenation and the live table's cached header reader. [Typed IID correspondence](../../verification/verus/live_iid_contents/contents.rs) · [Production row-address operations](../../rust/crates/index/src/row_address.rs) · [Live table and header reader](../../rust/crates/index/src/live.rs)

### JVM shared snapshot ownership

A published live snapshot owns one reference and each open reader owns one more.
The Lean model proves over arbitrary interleavings of modeled acquire, retire, and close operations that a reader's reference prevents final release.
It proves that the last close after retirement triggers release and that a released snapshot admits no further operations.
The JVM mapping uses the partition's snapshot lock to cover both choosing the cached snapshot and retaining it.
Refresh and block adoption replace or clear the published snapshot before dropping its reference under the same lock.
Each reader's close has a one-shot guard and the shared counter performs the final release on the transition to zero.
This mapping assumes the admitted holder count fits the JVM counter, snapshots are used within their held lifetime, and the Arrow slices retain valid buffers until closed.
The JVM race test holds an older snapshot while a concurrent block adoption and a demand for a fresher basis replace the cached snapshot.
It reads the captured row ordinal and IID through the held trie after the adoption finishes.
Lean 4.35.0 audited seven theorems and rejected controls that leave the publisher installed after retirement, release with a reader outstanding, or admit acquisition after retirement.
Proof of the runtime lock and allocator semantics remains part of the implementation refinement. [Snapshot contract](../../allium/live-index.allium) · [Cache acquisition and retirement](../../core/src/main/kotlin/xtdb/database/PartitionState.kt) · [Ownership proof](../../verification/lean/SnapshotOwnership.lean) · [JVM block-adoption race](../../core/src/test/kotlin/xtdb/indexer/LiveIndexTest.kt)

### JVM trie pending-log ownership

IDX05 requires a retained trie version to keep its rows through later changes, including writes to a sibling version from the same parent.
Before the fix, two inserts of rows 7 and 9 into versions derived from the same empty JVM trie both read back row 9.
The JVM single-insert and bounded-batch paths now write into a newly allocated array before the new leaf becomes visible.
The compiled JVM reproduction reads row 7 from the first sibling and row 9 from the second after the fix.
Bytecode inspection shows the store and bulk copy target the array returned by `Arrays.copyOf`.
The Lean model proves that the new leaf sees the previous visible prefix followed by the inserted rows and that arbitrary sequences of forks preserve every previously captured log.
The model checks a shared-buffer witness of the old implementation.
The instruction model proves that resizing and writing into the fresh destination produces the specified log for both a single store and a bulk copy.
Lean 4.35.0 audited six theorems and rejected mutations that reuse the parent's buffer, discard its visible prefix, or copy just the first incoming row of a batch.
The production mapping assumes Kotlin's array copy allocates a fresh array, bulk copying touches only its destination, published arrays stay read-only, and each trie reads stable IIDs at its captured row indices.
JVM regressions cover sibling single inserts, sibling batches below the threshold, and a sibling range insert reaching the compaction threshold.
Run these cases with `./gradlew :xtdb-core:test --tests 'xtdb.trie.MemoryHashTrieTest'`.
Whole-trie JVM refinement across splits and the native ownership and publication proof remain open. [Immutable trie contract](../../allium/memory-hash-trie.allium) · [Lean proof](../../verification/lean/JvmTriePendingLogs.lean) · [JVM regressions](../../core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt)

### Protobuf trie paths and ordered IID candidates

The JVM protobuf encoder previously dropped a nonempty root path and the JVM decoder assigned empty paths to that root and its descendants.
A reproduction serialized root `[2]` with leaf `[2, 1]` containing row 7 and then read row 7 at path `[]`.
The format carries a nonempty root path and reconstructs each child path from its bucket position, as the Rust codec does.
The actual JVM encoder and the Rust encoder emitted identical 24-byte messages for that trie, and the Rust decoder recovered the same root, empty buckets, and row.
A legacy message without a root path decodes as an empty-root trie.
Recovering a nonempty root from a historical export that omitted it requires the root path from an external source.
Lean proves that the wire round trip reconstructs every anchored path and preserves the order and multiplicity of rows selected by any IID-prefix candidate filter.
The model starts with a well-placed four-bucket tree whose leaf rows have already been compacted and fit the wire's signed row-index field.
Lean 4.35.0 audited six theorems and rejected mutations that omit the root or assign one child its neighbour's path.
The JVM regressions check a rootless legacy message, a nonempty root with sparse children and equal-IID rows, actual IID-path selection, and insertion after reopening.
Full refinement of the persistence and Arrow-backed lookup paths remains open. [Wire proof](../../verification/lean/TrieWirePaths.lean) · [JVM codec regressions](../../core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt)
Stable replay of aborted outcomes remains open in the Multi target. [Lean model](../../verification/lean/PrivateReplay.lean) · [Verus study](../../verification/verus/idempotency_window/area.json) · [Node tests](../../rust/crates/node/tests/native.rs) · [Wire tests](../../rust/crates/tx/tests/submission.rs) · [Transaction contract](db.allium)

## Multi evidence

The multi models describe the separate `e86af7f12` branch baseline and its directory, Logkeeper, and object-store assumptions.
The standalone Lean and TLA+ artifacts are available under [verification/multi](../../verification/multi).
The branch evidence describes the native SQL oracle and Kani runs on their original revision. [Verification revision `b1e41f938`](https://github.com/bijectionhq/rtdb/blob/b1e41f938919f68d452a94a4ec493ebec7e6be50/dev/doc/rtdb-engine-proof-evidence.md)
The temporal and recency models and their local runner are also available on main. [Temporal projection](../lean/TemporalProjection.lean) · [Recency](../lean/Recency.lean) · [Reproduction](../check-rtdb-proof-models.sh)
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
This earlier model abstracts marker retention and command eligibility. [Lean proof](../../verification/multi/lean/GatewayReplay.lean) · [TLA+ model](../../verification/multi/tla/RTDBGatewayReplay.tla) · [Verus checks](../../verification/verus/replay_deadline/area.json) · [Pgwire tests](../../rust/crates/pgwire/src/protocol.rs) · [Gateway tests](../../rust/crates/multi/src/session.rs)

### Replay permit and protected marker

The protected permit assigns one stable marker key and one exclusive cutoff to both the original submission and every replacement.
An admitted submission compares its own clock with the cutoff before append.
The marker is retained at least until that cutoff and cannot be written or deleted by ordinary SQL.
Lean proves at most one mutation for an arbitrary finite sequence of attempts and arbitrary clock samples, assuming ordered atomic guard and marker publication.
TLC covers either writer reaching submission first and checks five counterexamples where a delayed original, older writer, changing application key, early expiry or marker deletion defeats the corresponding safeguard.
The setup replay checker also exhibits an Execute arriving between an earlier eligibility decision and the end of the refusal drain.
The gateway rechecks the final pending frames before resending them.
Verus checks the production gateway's finite cutoff arithmetic, feature and age requirements, and the setup frame classification.
The Rust wire fixture checks the exact nonce and cutoff encoding, while the Pgwire regression checks refusal before executing any replayed batch.
The live-cell test withholds a committed reply, completes a holder move, and checks that the same client gets a reply and observes one mutation.
The proof assumes the acknowledged journal prefix, single ordered resolver, and authentic writer capabilities. [Lean proof](../../verification/multi/lean/ReplayPermit.lean) · [TLA+ model](../../verification/multi/tla/RTDBReplayPermit.tla) · [Verus checks](../../verification/verus/replay_admission/area.json) · [Cell tests](../../rust/crates/multi/tests/cell.rs)

### Replay across a legacy resolver

An older resolver can inherit an admitted source record independently of which Pgwire servers received the initial command and its retry.
The current permit uses a distinct Pgwire capability and writes a leading failing assertion in the transaction body.
The resolver consumes that assertion only when the transaction envelope identifies it and the operation matches exactly.
Legacy decoding leaves the assertion in the operation sequence, where its execution aborts before any user writes.
A resolver that recognizes the original permit can also return the existing marker before evaluating the assertion.
The native replay test drops the entire permit and the new field separately to exercise both legacy decode behaviors on the same ordered prefix.
It then resolves a compatible retry and verifies that the next compatible retry recovers the original committed transaction key.
Lean extends the at-most-one invariant to arbitrary interleavings of protected resolution and legacy assertion aborts.
TLC permits several already-appended attempts and a fresh choice of resolver for each record.
Removing the legacy assertion produces two effects.
The model assumes one ordered journal prefix and atomic guard and marker publication for a compatible resolution.
Previously issued records retain the compatibility requirements of their original permit contract. [Lean proof](../../verification/multi/lean/ReplayPermit.lean) · [TLA+ model](../../verification/multi/tla/RTDBReplayResolver.tla) · [Native resolver regression](../../rust/crates/tx/tests/resolution.rs)

### Multi serving endpoint failure

Lean proves that repeated pgwire failures reach any finite threshold when the address pair stays fixed, even while control answers.
Verus checks the production failure count, including reset after success or address replacement and saturation at the machine bound.
TLC checks that failed observations can confirm only the corresponding address pair and that a waiting startup eventually connects under fair probe, placement, and retry steps.
The model treats a reachable replacement and available directory and quorum as premises, with a startup budget sufficient for those steps.
The weakened variants mask data endpoint failure with a healthy control response, confirm death from stale observations, or abandon the first failed dial.
The native cell regression keeps the control endpoint responsive, breaks pgwire, and verifies the same client startup reaches the replacement and reads the committed data.
A separate native test changes the pgwire address between the probe and publication.
The serving probe runs before Pgwire authentication and occupies no authenticated session slot.
The capacity regression fills the writer's configured session slots and verifies that the controller keeps the writer live.
The serving preface runs before Pgwire startup.
If both preface paths fail and a fresh control sample reports overflow, the gateway can offer ordinary Pgwire startup on a direct connection.
An eligible gateway can relay through the same writer's signed control endpoint while the controller continues to see a healthy writer.
The relay is available when the writer's authentication rules are independent of peer addresses.
TLC checks a writer-wide serving stall, a gateway-only isolation, and missed probes during inspection overflow.
The gateway-only case includes simultaneous overflow and requires attempting the verified relay before accepting the direct fallback.
Removing the wire reply, fallback, authorization constraint, or overflow check produces a counterexample.
These results cover the serving handshake under a reachable successor or an authorized relay path. [Lean proof](../../verification/multi/lean/ServingFailure.lean) · [TLA+ serving paths](../../verification/multi/tla/RTDBGatewayPaths.tla) · [Verus checks](../../verification/verus/member_probe/area.json) · [Cell tests](../../rust/crates/multi/tests/cell.rs)

### Interrupted journal replacement and trim

The local integration test publishes an anchor and loses its directory response before physical trim.
It acknowledges an update and a delete above the checkpoint, loses a journal and the writer, and recovers onto a repaired ensemble.
The new writer sees the full bitemporal history and ordered tail, then accepts a new write after losing a second original journal.
TLC checks that a checkpoint and the selected ensemble still cover every acknowledged slot through interrupted publication, trim, and replacement.
Allowing replacement before the entire sealed suffix is copied, or trimming before checkpoint publication, breaks that invariant.
The model abstracts each durable ensemble to a contiguous prefix and uses the existing quorum and checkpoint recovery premises. [TLA+ model](../../verification/multi/tla/RTDBJournalReplacement.tla) · [Cell tests](../../rust/crates/multi/tests/cell.rs)

### Provider-backed interruption tests

The AWS S3 fixtures use separate storage namespaces for two cells and a physically separate archive bucket.
The move test interrupts key installation, loses a placement reply before application, and loses the next placement reply after application.
The restore test loses the fence reply, removes the serving objects, then interrupts completion after journal seal.
A later restore recovers the original checkpoint and bitemporal tail.
The deletion fixture puts an object into the fenced prefix after the first empty observation and verifies that the key survives until a new observation sequence succeeds.
The tests exercise real S3 object requests with local directory and logkeeper processes.
They supply a fixture task identity for hosted storage ownership and exercise the deletion interval with controlled timestamps. [Move and restore tests](../../rust/crates/multi/tests/cell.rs) · [Deletion tests](../../rust/crates/multi/tests/s3.rs) · [Deletion model](../../verification/multi/tla/RTDBDeletion.tla)

### Batched catalog reader admissions

A three-zone workload against the persistent TCP directory reproduced contention failures when concurrent sessions each tried to register catalog holds independently.
The serving store batches pending admissions and releases on the same residence record while retaining one identity per live catalog reader.
A batch resolves each admission against the authoritative floor and uses a CAS to order its mutations with external floor advances.
An uncertain release fences a stale admission even when its reader is absent.
The TCP directory regression confirms that 64 concurrent readers all receive distinct holds and require fewer directory updates than one CAS per reader.
TLC checks that batching respects the floor and that a completed release cannot be reversed by a stale admission. [TLA+ model](../../verification/multi/tla/RTDBCatalogReaderBatch.tla) · [Native regression](../../rust/crates/multi/src/catalog_readers.rs)

### Independent placed startup

A placed node prepares up to eight database opens concurrently and installs each result through its catalog owner.
Requests for the same name share an ordered queue.
The native test pauses a real storage admission after its directory CAS and verifies that a different deployment on the same node opens, writes, and reads while the first stays paused.
TLC checks progress for the second deployment even if the first remains blocked, and prohibits publication after the catalog owner observes shutdown.
Serial preparation and unfenced late publication produce counterexamples.
The progress argument assumes a free preparation slot, successful backing services for the independent deployment, and fair execution. [TLA+ model](../../verification/multi/tla/RTDBPlacedOpen.tla) · [Native regression](../../rust/crates/multi/tests/cell.rs)
