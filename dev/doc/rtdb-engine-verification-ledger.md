# RTDB engine verification ledger

Each row tracks the named contract in the [engine property catalog](rtdb-engine-proof-properties.md).
The catalog pins the RTDB multi and full-text search baselines.
The [evidence record](rtdb-engine-proof-evidence.md) describes the precise scope, assumptions, and reproduction commands for checks.

- **Open** means this ledger has no property-specific evidence yet.
- **Partial** means a scoped theorem, implementation check, or runtime observation covers part of the contract.
- **Verified** requires a machine-checked argument covering the stated contract at its public boundary, including implementation refinement and explicit environment hypotheses where applicable.
- **Refuted** requires a reproducible counterexample under the stated contract and baseline.

Candidate entries retain their designation from the catalog.
A proof of a candidate is conditional on the explicitly stated contract and workload or fairness assumptions.

## Bitemporal history

| Target | Status | Recorded evidence |
| --- | --- | --- |
| TIME01 · Two independent axes | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean projection for a captured history](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME02 · Interval endpoints | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Rust interval predicates](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME03 · Open bounds | Open | — |
| TIME04 · Reference history | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean range projection and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME05 · Correction scope | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME06 · Delete semantics | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME07 · Erase semantics | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME08 · Erase and new writes | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME09 · Per-entity isolation | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME10 · Rectangle projection | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean range coverage and Rust one-split projection](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME11 · Specialized equivalence | Partial | [Captured-history and Rust temporal checks](rtdb-engine-proof-evidence.md#main-evidence) · [Lean range-to-point agreement and native query comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| TIME12 · Temporal context | Open | — |

## Values, types, and columnar representation

| Target | Status | Recorded evidence |
| --- | --- | --- |
| VAL01 · Identity encoding | Open | — |
| VAL02 · Name representation | Open | — |
| VAL03 · Type join | Partial | [Lean scalar and flat-struct join laws](rtdb-engine-proof-evidence.md#main-evidence) |
| VAL04 · Sparse schema | Partial | [Lean missing-field and null rules](rtdb-engine-proof-evidence.md#main-evidence) |
| VAL05 · Promotion and casts | Open | — |
| VAL06 · Equality, hashing, and ordering | Open | — |
| VAL07 · Extension types | Open | — |
| VAL08 · Physical encoding | Partial | [Lean sliced runs and Arrow physical-position correspondence](rtdb-engine-proof-evidence.md#arrow-run-end-lookup) |
| VAL09 · Batch schema | Open | — |
| VAL10 · Row and vector agreement | Open | — |

## Binding, planning, and relational execution

| Target | Status | Recorded evidence |
| --- | --- | --- |
| QRY01 · Binding | Open | — |
| QRY02 · Lowering | Open | — |
| QRY03 · Plan reuse | Open | — |
| QRY04 · Rewrite soundness | Partial | [Lean left join predicate laws and a native optimizer regression](rtdb-engine-proof-evidence.md#main-evidence) |
| QRY05 · Predicate placement | Partial | [Lean left join laws and nullable-side counterexample](rtdb-engine-proof-evidence.md#main-evidence) |
| QRY06 · Correlation | Open | — |
| QRY07 · Projection | Open | — |
| QRY08 · Join and set semantics | Open | — |
| QRY09 · Aggregation | Partial | [Ordered-index distinct runs and bounded query prefixes](rtdb-engine-proof-evidence.md#main-evidence) |
| QRY10 · Ordering and windows | Partial | [Ordered merge, filtering before limit and keyset continuation](rtdb-engine-proof-evidence.md#main-evidence) |
| QRY11 · Evaluation context | Partial | [Lean limit-opening order, checked production bounds, and native evaluation regressions](rtdb-engine-proof-evidence.md#limit-opening-and-count-admission) |
| QRY12 · Streaming equivalence | Open | — |
| QRY13 · Query framing | Open | — |
| QRY14 · Rewrite termination (Candidate) | Open | — |

## Scans, pruning, and distributed reads

| Target | Status | Recorded evidence |
| --- | --- | --- |
| SCAN01 · Source coverage | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| SCAN02 · Pruning is conservative | Partial | [Compaction bucket and mixed-trie models](rtdb-engine-proof-evidence.md#main-evidence) · [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| SCAN03 · Historical dependency coverage | Partial | [Compaction bucket and mixed-trie models](rtdb-engine-proof-evidence.md#main-evidence) · [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| SCAN04 · Optional metadata | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| SCAN05 · Residual filters | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#multi-evidence) |
| SCAN06 · Scan metadata agreement | Partial | [Ordered page exclusion under valid fences](rtdb-engine-proof-evidence.md#main-evidence) |
| SCAN07 · Catalog agreement | Partial | [Retained catalog observations](rtdb-engine-proof-evidence.md#main-evidence) |
| SCAN08 · Read tier equivalence | Open | — |
| SCAN09 · Fragment equivalence | Open | — |
| SCAN10 · Read guarantees | Open | — |
| SCAN11 · Per-partition bases | Partial | [Partition order in snapshot tokens and null-partition observation](rtdb-engine-proof-evidence.md#main-evidence) |

## Sessions and transaction boundaries

| Target | Status | Recorded evidence |
| --- | --- | --- |
| SESS01 · Legal states | Partial | [Lean explicit phases and terminal closure](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS02 · Access mode | Partial | [Lean first-statement resolution and Rust session tests](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS03 · Pinned read basis | Partial | [Lean explicit basis preservation and Rust session observations](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS04 · Await and snapshot tokens | Partial | [Lean componentwise await-token merge and native token checks](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS05 · Read your writes | Open | — |
| SESS06 · Buffer ownership | Partial | [Lean ordered buffer and modeled commit boundary](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS07 · Rollback and rejection | Partial | [Lean discard and failed-state rejection, and Rust session tests](rtdb-engine-proof-evidence.md#main-evidence) |
| SESS08 · Frontend parity | Open | — |
| SESS09 · Transaction handles | Open | — |
| SESS10 · Sync and async receipts | Open | — |

## Transaction execution, live state, and clocks

| Target | Status | Recorded evidence |
| --- | --- | --- |
| TX01 · All-or-nothing effects | Partial | [Publication and read-back protocol models](rtdb-engine-proof-evidence.md#main-evidence) · [Private replay admission and marker authority](rtdb-engine-proof-evidence.md#private-replay-admission-and-marker-authority) |
| TX02 · Ordered resolution | Partial | [Lean same-block prefix and native overlay admission](rtdb-engine-proof-evidence.md#same-block-staged-resolution-prefix) |
| TX03 · Staged isolation | Partial | [Publication and read-back protocol models](rtdb-engine-proof-evidence.md#main-evidence) · [Lean staged views and native overlay observations](rtdb-engine-proof-evidence.md#same-block-staged-resolution-prefix) |
| TX04 · Static DML equivalence | Open | — |
| TX05 · Ordered apply | Partial | [Lean prefix equivalence and native leader/follower replay](rtdb-engine-proof-evidence.md#ordered-apply-produces-the-same-leader-and-follower-prefix) |
| TX06 · Read-back agreement | Partial | [Publication and read-back protocol models](rtdb-engine-proof-evidence.md#main-evidence) · [Pending sequence, full-frame fingerprint and staged-head checks](rtdb-engine-proof-evidence.md#a-leaders-read-back-agrees-with-its-pending-transaction) |
| TX07 · Atomic snapshot publication | Open | — |
| TX08 · Progress monitor | Partial | [Lean terminal selection and native cancellation isolation](rtdb-engine-proof-evidence.md#progress-waits-use-their-observed-watermark) |
| TX09 · Transaction clocks | Partial | [Mixed system-clock proof and replay](rtdb-engine-proof-evidence.md#main-evidence) |
| TX10 · Statement semantics at commit | Open | — |
| TX11 · Known outcome | Partial | [Publication and read-back protocol models](rtdb-engine-proof-evidence.md#main-evidence) · [Native submission clock and durable retry attribution](rtdb-engine-proof-evidence.md#submission-clock-and-durable-retry-attribution) · [Private replay admission and marker authority](rtdb-engine-proof-evidence.md#private-replay-admission-and-marker-authority) |
| TX12 · Strict serializability (Candidate) | Open | — |

## Logs, leadership, and block adoption

| Target | Status | Recorded evidence |
| --- | --- | --- |
| LOG01 · One durable order | Open | — |
| LOG02 · Read progress | Open | — |
| LOG03 · Retention and durability assumptions | Partial | [Classic Kafka and S3 process recovery fixture](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG04 · Claim admission | Partial | [Kernel model and Rust term admission](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG05 · Term fencing | Partial | [Kernel and Rust term admission, and Multi Logkeeper seal proof](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG06 · Role transitions | Open | — |
| LOG07 · Block ownership | Open | — |
| LOG08 · Publication and adoption | Partial | [Checkpoint selection and suffix replay](rtdb-engine-proof-evidence.md#main-evidence) · [Rust position packing and resume](rtdb-engine-proof-evidence.md#multi-evidence) |
| LOG09 · Follower convergence | Partial | [Checkpoint selection and suffix replay](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG10 · Failure reporting | Partial | [Prepared cursor admission, refresh and refusal](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG11 · Stream separation | Partial | [Main Logkeeper reply rank](rtdb-engine-proof-evidence.md#main-evidence) · [Lean model of retained journal slot identity](rtdb-engine-proof-evidence.md#multi-evidence) |
| LOG12 · Retained-history audit | Partial | [Lean page-chain and quorum certification laws, and online and offline native tests](rtdb-engine-proof-evidence.md#main-evidence) |
| LOG13 · Terminal stream retirement | Partial | [Lean terminal control and all-replica settlement, Kani admission, and journal restart](rtdb-engine-proof-evidence.md#main-evidence) |

## Memory trie and live indexing

| Target | Status | Recorded evidence |
| --- | --- | --- |
| IDX01 · Exact lookup | Partial | [Lean routed lookup and Rust comparison with flat history](rtdb-engine-proof-evidence.md#memory-trie-lookup-and-coverage) · [Verified live row addressing](rtdb-engine-proof-evidence.md#live-row-addresses-across-chunk-layouts) |
| IDX02 · Total coverage | Partial | [Lean row occurrence conservation and Rust traversal checks](rtdb-engine-proof-evidence.md#memory-trie-lookup-and-coverage) · [Exact production bucket sequences](rtdb-engine-proof-evidence.md#production-live-trie-bucket-grouping) · [Multiplicity in the native leaf operations](rtdb-engine-proof-evidence.md#production-leaf-sort-and-composition) · [Multiplicity in the JVM merge](rtdb-engine-proof-evidence.md#production-jvm-leaf-merge) · [Leaf-run history and order](rtdb-engine-proof-evidence.md#leaf-runs-compaction-and-persisted-order) · [Checked native subtree construction](rtdb-engine-proof-evidence.md#production-leaf-split) |
| IDX03 · Leaf order | Partial | [Verified native sorting and merge composition](rtdb-engine-proof-evidence.md#production-leaf-sort-and-composition) · [Checked native persisted order](rtdb-engine-proof-evidence.md#native-persisted-leaf-order) · [Native comparator and JVM IID ordering](rtdb-engine-proof-evidence.md#production-leaf-comparison) · [Checked JVM merge](rtdb-engine-proof-evidence.md#production-jvm-leaf-merge) · [Leaf-run history and order](rtdb-engine-proof-evidence.md#leaf-runs-compaction-and-persisted-order) · [Checked native subtree construction](rtdb-engine-proof-evidence.md#production-leaf-split) · [Exact sibling visitation sequence](rtdb-engine-proof-evidence.md#trie-sibling-traversal-order) |
| IDX04 · Bucket routing | Partial | [Verified production grouping and child-path membership](rtdb-engine-proof-evidence.md#production-live-trie-bucket-grouping) · [Rust IID bucket selection](rtdb-engine-proof-evidence.md#multi-evidence) · [Checked native subtree construction](rtdb-engine-proof-evidence.md#production-leaf-split) |
| IDX05 · Immutable ancestors | Partial | [JVM pending-log ownership proof and sibling-version regressions](rtdb-engine-proof-evidence.md#jvm-trie-pending-log-ownership) |
| IDX06 · Bulk insertion equivalence | Partial | [Lean batch regrouping and Rust range, batch, and single insertion checks](rtdb-engine-proof-evidence.md#memory-trie-lookup-and-coverage) · [Verified branch insertion grouping](rtdb-engine-proof-evidence.md#production-live-trie-bucket-grouping) |
| IDX07 · Structural rewrite equivalence | Partial | [Checked native subtree construction](rtdb-engine-proof-evidence.md#production-leaf-split) · [Verified native compaction and overflow run order](rtdb-engine-proof-evidence.md#production-leaf-sort-and-composition) · [Checked JVM merge](rtdb-engine-proof-evidence.md#production-jvm-leaf-merge) · [Leaf-run history and order](rtdb-engine-proof-evidence.md#leaf-runs-compaction-and-persisted-order) · [Exact sibling visitation sequence](rtdb-engine-proof-evidence.md#trie-sibling-traversal-order) |
| IDX08 · Snapshot lifetime | Partial | [Verified row addressing, layout preservation, and native snapshot regression](rtdb-engine-proof-evidence.md#live-row-addresses-across-chunk-layouts) · [Native retained IID correspondence](rtdb-engine-proof-evidence.md#retained-iid-contents-in-native-chunks) · [JVM ownership proof and retained-IID race regression](rtdb-engine-proof-evidence.md#jvm-shared-snapshot-ownership) |
| IDX09 · Serialized correspondence | Partial | [Protobuf path round trip and ordered IID candidates](rtdb-engine-proof-evidence.md#protobuf-trie-paths-and-ordered-iid-candidates) · [Checked native persisted order](rtdb-engine-proof-evidence.md#native-persisted-leaf-order) |

## Persistent formats and compatibility

| Target | Status | Recorded evidence |
| --- | --- | --- |
| FMT01 · Codec round trips | Partial | [Signed position bounds and production codec checks](rtdb-engine-proof-evidence.md#main-evidence) · [Rust position packing and resume](rtdb-engine-proof-evidence.md#multi-evidence) · [Protobuf trie paths](rtdb-engine-proof-evidence.md#protobuf-trie-paths-and-ordered-iid-candidates) |
| FMT02 · Optional field presence | Partial | [Native source guard presence and legacy replay regression](rtdb-engine-proof-evidence.md#submission-clock-and-durable-retry-attribution) · [Private replay admission and marker authority](rtdb-engine-proof-evidence.md#private-replay-admission-and-marker-authority) |
| FMT03 · Arrow event fidelity | Open | — |
| FMT04 · Version dispatch | Open | — |
| FMT05 · Errors and referenced payloads | Open | — |
| FMT06 · Key and namespace fidelity | Partial | [Lean slug allocation and native checkpoint key collision regression](rtdb-engine-proof-evidence.md#main-evidence) |
| FMT07 · Stable table identities | Partial | [Lean preservation of recorded slugs and Rust and JVM catalog regressions](rtdb-engine-proof-evidence.md#main-evidence) |
| FMT08 · Canonical encoding | Open | — |
| FMT09 · Safe format errors | Open | — |

## Compaction and trie catalog transitions

| Target | Status | Recorded evidence |
| --- | --- | --- |
| COMP01 · Job selection | Open | — |
| COMP02 · Merge equivalence | Partial | [Compaction bucket and mixed-trie models](rtdb-engine-proof-evidence.md#main-evidence) · [Lean ordered segments and native compaction regression](rtdb-engine-proof-evidence.md#multi-evidence) |
| COMP03 · Identity and partitioning | Partial | [Compaction bucket and mixed-trie models](rtdb-engine-proof-evidence.md#main-evidence) |
| COMP04 · Metadata fidelity | Open | — |
| COMP05 · Recency agreement | Partial | [Lean recency fold and Rust three-range check](rtdb-engine-proof-evidence.md#multi-evidence) |
| COMP06 · Output determinism | Open | — |
| COMP07 · Catalog convergence | Open | — |
| COMP08 · Complete replacement | Partial | [Tiered and sibling publication studies](rtdb-engine-proof-evidence.md#main-evidence) |
| COMP09 · Safe adoption | Partial | [Tiered and sibling publication studies](rtdb-engine-proof-evidence.md#main-evidence) |
| COMP10 · Safe intermediate states | Partial | [Tiered and sibling publication studies](rtdb-engine-proof-evidence.md#main-evidence) |
| COMP11 · Retry and scheduling | Open | — |
| COMP12 · Recency writer bound | Open | — |

## Garbage collection, erasure, and maintenance

| Target | Status | Recorded evidence |
| --- | --- | --- |
| GC01 · Reachability | Partial | [Generation retention and collector selection](rtdb-engine-proof-evidence.md#main-evidence) |
| GC02 · Maintenance authority | Partial | [Generation retention and collector selection](rtdb-engine-proof-evidence.md#main-evidence) |
| GC03 · Trie deletion order | Open | — |
| GC04 · Trie cleanup convergence | Open | — |
| GC05 · Block retention | Partial | [Checkpoint selection and suffix replay](rtdb-engine-proof-evidence.md#main-evidence) |
| GC06 · Referenced replica payloads | Open | — |
| GC07 · Snapshot export | Open | — |
| GC08 · Compactor reset | Open | — |
| GC09 · Logical erase and physical disposal | Partial | [Hosted ordered-index scrub and retirement](rtdb-engine-proof-evidence.md#main-evidence) |
| GC10 · Receipt completeness | Partial | [Bounded S3 inventory and disposal receipt model](rtdb-engine-proof-evidence.md#multi-evidence) |

## Database membership, multi-cell placement, and lineage

| Target | Status | Recorded evidence |
| --- | --- | --- |
| CELL01 · Membership uniqueness | Open | — |
| CELL02 · Attach and detach | Open | — |
| CELL03 · Placement uniqueness | Open | — |
| CELL04 · Epoch monotonicity | Open | — |
| CELL05 · Watch consistency | Open | — |
| CELL06 · Fault detection independence | Open | — |
| CELL07 · Isolation and engine selection | Open | — |
| CELL08 · Planned handoff | Partial | [Protected permit replay through a real holder move](rtdb-engine-proof-evidence.md#replay-permit-and-protected-marker) |
| CELL09 · Cold activation | Partial | [Integrated Multi worker-loss and bitemporal replay regression](rtdb-engine-proof-evidence.md#main-evidence) |
| CELL10 · Cross-cell cutover | Partial | [Bounded cross-cell transfer and fair activation model](rtdb-engine-proof-evidence.md#multi-evidence) |
| CELL11 · Move retries | Open | — |
| CELL12 · Fork isolation | Open | — |
| CELL13 · Lineage reachability | Open | — |
| CELL14 · Directory format equivalence | Open | — |
| CELL15 · Static stability | Open | — |

## Object storage, caches, and hosted recovery

| Target | Status | Recorded evidence |
| --- | --- | --- |
| STORE01 · Provider refinement | Open | — |
| STORE02 · Range and cache agreement | Open | — |
| STORE03 · Read lifetime | Partial | [Lean read entitlements and hosted scope retirement](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE04 · Immutable publication | Partial | [Checkpoint publication and Azure completion replay](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE05 · Physical identity | Open | — |
| STORE06 · Writer authorization | Open | — |
| STORE07 · Publication visibility | Partial | [Ordered-index checkpoint publication model](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE08 · Multipart confirmation | Partial | [Checkpoint publication and Azure completion replay](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE09 · Archive completeness | Open | — |
| STORE10 · Restore equivalence | Partial | [Checkpoint selection, suffix replay, and integrated Multi worker loss](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE11 · Discovery contract | Partial | [Checkpoint selection and suffix replay](rtdb-engine-proof-evidence.md#main-evidence) |
| STORE12 · Key custody | Open | — |

## External sources and CDC

| Target | Status | Recorded evidence |
| --- | --- | --- |
| CDC01 · Source authority | Open | — |
| CDC02 · Transaction boundaries | Open | — |
| CDC03 · Token correspondence | Open | — |
| CDC04 · Safe acknowledgment | Open | — |
| CDC05 · Single confirmer | Open | — |
| CDC06 · Reopen | Open | — |
| CDC07 · Replayed delivery | Open | — |
| CDC08 · Snapshot boundary | Open | — |
| CDC09 · Mapping fidelity | Open | — |
| CDC10 · Schema resolution | Open | — |
| CDC11 · Reject and retry outcomes | Open | — |
| CDC12 · Ingestion backpressure (Candidate) | Open | — |

## Authorization, protocols, and foreign interfaces

| Target | Status | Recorded evidence |
| --- | --- | --- |
| API01 · Authentication binding | Open | — |
| API02 · Authorization coverage | Open | — |
| API03 · Deployment isolation | Open | — |
| API04 · Capability scope | Open | — |
| API05 · Pgwire state machine | Open | — |
| API06 · Parameters and results | Open | — |
| API07 · Prepared handles and portals | Open | — |
| API08 · Reply attribution | Partial | [Acknowledged permit and reply-loss regressions](rtdb-engine-proof-evidence.md#replay-permit-and-protected-marker) |
| API09 · Streaming ingestion | Open | — |
| API10 · Handle lifetime | Open | — |
| API11 · Cancellation isolation | Open | — |
| API12 · Authentication refresh | Open | — |
| API13 · Cross-deployment noninterference (Candidate) | Open | — |
| API14 · Bounded gateway replay | Partial | [Recovery cutoff inheritance](rtdb-engine-proof-evidence.md#gateway-write-replay) · [Protected submission cutoff](rtdb-engine-proof-evidence.md#replay-permit-and-protected-marker) |

## Resource accounting and operational progress

| Target | Status | Recorded evidence |
| --- | --- | --- |
| RES01 · Reservation ownership | Partial | [Lean refusal accounting and native grant overflow regression](rtdb-engine-proof-evidence.md#main-evidence) |
| RES02 · Budget enforcement | Partial | [Lean quantized grant and limit checks](rtdb-engine-proof-evidence.md#main-evidence) |
| RES03 · Error and cancel cleanup | Open | — |
| RES04 · Queue and frame admission | Open | — |
| RES05 · Scoped shutdown | Open | — |
| RES06 · Health and readiness | Partial | [Multi serving endpoint probes and recovery](rtdb-engine-proof-evidence.md#multi-serving-endpoint-failure) |
| RES07 · Placement demand | Open | — |
| RES08 · Physical memory envelope (Candidate) | Open | — |
| RES09 · Cancellation work bound (Candidate) | Open | — |
| RES10 · Foreground progress (Candidate) | Open | — |
| RES11 · Idle work | Open | — |
| RES12 · Placement work | Open | — |

## Bitemporal full-text search

| Target | Status | Recorded evidence |
| --- | --- | --- |
| FTS01 · Snapshot-scoped definitions | Open | — |
| FTS02 · Corpus visibility | Open | — |
| FTS03 · Analyzer agreement | Open | — |
| FTS04 · Visibility rectangles | Partial | [Visibility merge and ranked search checks](rtdb-engine-proof-evidence.md#main-evidence) |
| FTS05 · Candidate completeness | Partial | [Visibility merge and ranked search checks](rtdb-engine-proof-evidence.md#main-evidence) |
| FTS06 · Residual agreement | Partial | [Visibility merge and ranked search checks](rtdb-engine-proof-evidence.md#main-evidence) |
| FTS07 · Ranking equivalence | Partial | [Visibility merge and ranked search checks](rtdb-engine-proof-evidence.md#main-evidence) |
| FTS08 · Artifact independence | Open | — |
| FTS09 · Artifact lifecycle | Open | — |

## Compatibility, upgrade, and rollback

| Target | Status | Recorded evidence |
| --- | --- | --- |
| COMPAT01 · Observational parity | Open | — |
| COMPAT02 · Cross-version recovery | Open | — |
| COMPAT03 · Exclusive handoff | Open | — |
| COMPAT04 · Mixed-version outputs | Open | — |
| COMPAT05 · Release configuration | Open | — |
| COMPAT06 · Evidence provenance | Partial | [Runner input and tool provenance](rtdb-engine-proof-evidence.md#main-evidence) |
