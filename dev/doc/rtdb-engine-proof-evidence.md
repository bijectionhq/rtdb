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
| IDX02, IDX04, IDX06, IDX07 | Verus checks the production Rust base-four IID digit, path comparison, and stable partition of an insertion or split batch at every valid depth. The Kani harness also checks all legal depths and the independent bit-extraction rule for width 2, 4, or 8 in the actual bucketer. | Compose grouping, leaf ordering, and recursive replacement at actual Rust and JVM call sites, then connect the resulting trees to traversal and replay. |
| IDX02, IDX03, IDX07 | Verus checks that the production Rust leaf merge returns every input occurrence in order. An injective logical rank uniquely determines the successful output sequence from those occurrences. | Compose the checked comparator and merge with the production readers, sorted-run construction, and the JVM merge. |
| IDX01, IDX08 | Verus checks the production Rust search, append, and adjacent chunk replacement against one contiguous-layout invariant. Lean proves that appending rows and joining adjacent chunks preserve existing ordinal reads. | Prove the Arrow row-content obligation and refine snapshot ownership across block transitions in both engines. |
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

### Production live-trie bucket grouping

Rust branch insertion and leaf splitting call the same checked grouping function.
For an arbitrary finite input at any branching depth, Verus proves that each successful child sequence is the input filtered by that child's base-four IID digit, including repeated rows and their original order.
On an IID read failure at a branching depth, the function returns that first modeled read error.
The proof assumes a finite exact-size iterator obeying the Rust iterator specification and a terminating reader whose results agree with the supplied per-row model.
The child-path law composes the production comparator and digit selector to show that child membership is parent membership plus agreement on the next digit.
The native tests exercise the last IID digit, repeated rows, and a read failure whose suffix remains unread.
Verus 0.2026.11.05.31e44e6 checked thirteen functions and rejected all eight mutations, including wrong-child placement, reversed order, and admitting an unreadable row.
The native run passed 497 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production grouping and tests](../../rust/crates/trie/src/partition_step.rs) · [Verus study](../../verification/verus/iid_partition/area.json)

### Production leaf merge

Rust leaf sorting and compaction use the verified two-run merge.
With sorted input runs, representable output capacity, and a fallible comparator consistent with a stable rank, Verus proves that a successful merge is sorted and its row multiset is the sum of the two input multisets.
An injective rank gives one ordered output sequence for that multiset.
The rank law proves that a fixed-width IID interpreted as an unsigned big-endian number, with a descending row-index suffix, agrees with the specified bytewise IID and row-index order.
The merge proof takes sorted input runs and the production comparator's agreement with that rank as premises.
The native tests exercise repeated rows, the unconsumed tail, and propagation of a comparison failure.
Verus 0.2026.11.05.31e44e6 checked fourteen functions and rejected four mutations that select the larger head, omit the tail, consume a row without emitting it, or reverse the order within an IID.
The native run passed 499 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`. [Production merge and tests](../../rust/crates/trie/src/leaf_merge_step.rs) · [Verus study](../../verification/verus/trie_leaf_merge/area.json)

### Production leaf comparison

The Rust pending-log sort, two-run merge and persisted-leaf validation use the same row-key comparison.
Verus checks that the production comparator returns Less exactly when the logical rank is smaller and Equal exactly when both IID and ordinal agree.
The proof takes the standard Rust array comparator's lexicographic contract as a library assumption and uses Verus's specification for unsigned integer comparisons.
It checks the row tie-break and the combination with IID order in the actual Rust function.
Verus 0.2026.11.05.31e44e6 checked fifteen functions in the comparator and merge study.
It rejected six controls, including reversing either the IID comparison or the row tie-break in the production comparator.

The JVM comparator uses Arrow's comparison of sixteen-byte IID views and compares row indices in descending order when the IIDs match.
Inspection of Arrow 19.0.0 bytecode found two word comparisons with byte-order normalization and a sign-bit flip before signed long comparison.
Lean proves that the sign-bit flip realizes unsigned comparison and that comparing the two words and then the reverse ordinal agrees with the specified leaf order.
The mapping assumes both readers deliver each row's stable sixteen IID bytes, the Arrow word loads represent those bytes in the documented order, and admitted row indices fit the JVM representation.
Lean 4.35.0 audited five theorems and rejected signed-word comparison, reversed IID comparison and older-first row ties.
The Rust and JVM regressions each insert 4,096 rows and check the leaf against an independent order reference across single-byte values at every IID position, including duplicate IIDs.
The native run passed 504 tests with `cargo test --manifest-path rust/Cargo.toml --locked -p rtdb-trie -p rtdb-format -p rtdb-index -p rtdb-query`.
Run the JVM case with `./gradlew :xtdb-core:test --tests 'xtdb.trie.MemoryHashTrieTest'`.
The sorted-run and leaf-state refinement remains open. [Comparator study](../../verification/verus/trie_leaf_merge/area.json) · [JVM order proof](../../verification/lean/IidByteOrder.lean) · [Rust regression](../../rust/crates/trie/tests/contracts.rs) · [JVM regression](../../core/src/test/kotlin/xtdb/trie/MemoryHashTrieTest.kt)

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
