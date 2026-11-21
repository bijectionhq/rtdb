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
| TIME01 · Two independent axes | Partial | [Lean projection for a captured history](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME02 · Interval endpoints | Partial | [Rust interval predicates](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME03 · Open bounds | Open | — |
| TIME04 · Reference history | Partial | [Lean range projection and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME05 · Correction scope | Partial | [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME06 · Delete semantics | Partial | [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME07 · Erase semantics | Partial | [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME08 · Erase and new writes | Partial | [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME09 · Per-entity isolation | Partial | [Lean point history and native SQL oracle](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME10 · Rectangle projection | Partial | [Lean range coverage and Rust one-split projection](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME11 · Specialized equivalence | Partial | [Lean range-to-point agreement and native query comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| TIME12 · Temporal context | Open | — |

## Values, types, and columnar representation

| Target | Status | Recorded evidence |
| --- | --- | --- |
| VAL01 · Identity encoding | Open | — |
| VAL02 · Name representation | Open | — |
| VAL03 · Type join | Open | — |
| VAL04 · Sparse schema | Open | — |
| VAL05 · Promotion and casts | Open | — |
| VAL06 · Equality, hashing, and ordering | Open | — |
| VAL07 · Extension types | Open | — |
| VAL08 · Physical encoding | Open | — |
| VAL09 · Batch schema | Open | — |
| VAL10 · Row and vector agreement | Open | — |

## Binding, planning, and relational execution

| Target | Status | Recorded evidence |
| --- | --- | --- |
| QRY01 · Binding | Open | — |
| QRY02 · Lowering | Open | — |
| QRY03 · Plan reuse | Open | — |
| QRY04 · Rewrite soundness | Open | — |
| QRY05 · Predicate placement | Open | — |
| QRY06 · Correlation | Open | — |
| QRY07 · Projection | Open | — |
| QRY08 · Join and set semantics | Open | — |
| QRY09 · Aggregation | Open | — |
| QRY10 · Ordering and windows | Open | — |
| QRY11 · Evaluation context | Open | — |
| QRY12 · Streaming equivalence | Open | — |
| QRY13 · Query framing | Open | — |
| QRY14 · Rewrite termination (Candidate) | Open | — |

## Scans, pruning, and distributed reads

| Target | Status | Recorded evidence |
| --- | --- | --- |
| SCAN01 · Source coverage | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| SCAN02 · Pruning is conservative | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| SCAN03 · Historical dependency coverage | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| SCAN04 · Optional metadata | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| SCAN05 · Residual filters | Partial | [Lean pruning premise and native SQL comparisons](rtdb-engine-proof-evidence.md#checked-scope) |
| SCAN06 · Scan metadata agreement | Open | — |
| SCAN07 · Catalog agreement | Open | — |
| SCAN08 · Read tier equivalence | Open | — |
| SCAN09 · Fragment equivalence | Open | — |
| SCAN10 · Read guarantees | Open | — |
| SCAN11 · Per-partition bases | Open | — |

## Sessions and transaction boundaries

| Target | Status | Recorded evidence |
| --- | --- | --- |
| SESS01 · Legal states | Open | — |
| SESS02 · Access mode | Open | — |
| SESS03 · Pinned read basis | Open | — |
| SESS04 · Await and snapshot tokens | Open | — |
| SESS05 · Read your writes | Open | — |
| SESS06 · Buffer ownership | Open | — |
| SESS07 · Rollback and rejection | Open | — |
| SESS08 · Frontend parity | Open | — |
| SESS09 · Transaction handles | Open | — |
| SESS10 · Sync and async receipts | Open | — |

## Transaction execution, live state, and clocks

| Target | Status | Recorded evidence |
| --- | --- | --- |
| TX01 · All-or-nothing effects | Open | — |
| TX02 · Ordered resolution | Open | — |
| TX03 · Staged isolation | Open | — |
| TX04 · Static DML equivalence | Open | — |
| TX05 · Ordered apply | Open | — |
| TX06 · Read-back agreement | Open | — |
| TX07 · Atomic snapshot publication | Open | — |
| TX08 · Progress monitor | Open | — |
| TX09 · Transaction clocks | Open | — |
| TX10 · Statement semantics at commit | Open | — |
| TX11 · Known outcome | Open | — |
| TX12 · Strict serializability (Candidate) | Open | — |

## Logs, leadership, and block adoption

| Target | Status | Recorded evidence |
| --- | --- | --- |
| LOG01 · One durable order | Open | — |
| LOG02 · Read progress | Open | — |
| LOG03 · Retention and durability assumptions | Open | — |
| LOG04 · Claim admission | Open | — |
| LOG05 · Term fencing | Open | — |
| LOG06 · Role transitions | Open | — |
| LOG07 · Block ownership | Open | — |
| LOG08 · Publication and adoption | Partial | [Rust position packing and resume](rtdb-engine-proof-evidence.md#checked-scope) |
| LOG09 · Follower convergence | Open | — |
| LOG10 · Failure reporting | Open | — |
| LOG11 · Stream separation | Open | — |
| LOG12 · Retained-history audit | Open | — |
| LOG13 · Terminal stream retirement | Open | — |

## Memory trie and live indexing

| Target | Status | Recorded evidence |
| --- | --- | --- |
| IDX01 · Exact lookup | Open | — |
| IDX02 · Total coverage | Open | — |
| IDX03 · Leaf order | Open | — |
| IDX04 · Bucket routing | Partial | [Rust IID bucket selection](rtdb-engine-proof-evidence.md#checked-scope) |
| IDX05 · Immutable ancestors | Open | — |
| IDX06 · Bulk insertion equivalence | Open | — |
| IDX07 · Structural rewrite equivalence | Open | — |
| IDX08 · Snapshot lifetime | Open | — |
| IDX09 · Serialized correspondence | Open | — |

## Persistent formats and compatibility

| Target | Status | Recorded evidence |
| --- | --- | --- |
| FMT01 · Codec round trips | Partial | [Rust position packing and resume](rtdb-engine-proof-evidence.md#checked-scope) |
| FMT02 · Optional field presence | Open | — |
| FMT03 · Arrow event fidelity | Open | — |
| FMT04 · Version dispatch | Open | — |
| FMT05 · Errors and referenced payloads | Open | — |
| FMT06 · Key and namespace fidelity | Open | — |
| FMT07 · Stable table identities | Open | — |
| FMT08 · Canonical encoding | Open | — |
| FMT09 · Safe format errors | Open | — |

## Compaction and trie catalog transitions

| Target | Status | Recorded evidence |
| --- | --- | --- |
| COMP01 · Job selection | Open | — |
| COMP02 · Merge equivalence | Partial | [Lean ordered segments and native compaction regression](rtdb-engine-proof-evidence.md#checked-scope) |
| COMP03 · Identity and partitioning | Open | — |
| COMP04 · Metadata fidelity | Open | — |
| COMP05 · Recency agreement | Partial | [Lean recency fold and Rust three-range check](rtdb-engine-proof-evidence.md#checked-scope) |
| COMP06 · Output determinism | Open | — |
| COMP07 · Catalog convergence | Open | — |
| COMP08 · Complete replacement | Open | — |
| COMP09 · Safe adoption | Open | — |
| COMP10 · Safe intermediate states | Open | — |
| COMP11 · Retry and scheduling | Open | — |
| COMP12 · Recency writer bound | Open | — |

## Garbage collection, erasure, and maintenance

| Target | Status | Recorded evidence |
| --- | --- | --- |
| GC01 · Reachability | Open | — |
| GC02 · Maintenance authority | Open | — |
| GC03 · Trie deletion order | Open | — |
| GC04 · Trie cleanup convergence | Open | — |
| GC05 · Block retention | Open | — |
| GC06 · Referenced replica payloads | Open | — |
| GC07 · Snapshot export | Open | — |
| GC08 · Compactor reset | Open | — |
| GC09 · Logical erase and physical disposal | Open | — |
| GC10 · Receipt completeness | Open | — |

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
| CELL08 · Planned handoff | Open | — |
| CELL09 · Cold activation | Partial | [Multi-mode worker-loss regression](rtdb-engine-proof-evidence.md#checked-scope) |
| CELL10 · Cross-cell cutover | Open | — |
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
| STORE03 · Read lifetime | Open | — |
| STORE04 · Immutable publication | Open | — |
| STORE05 · Physical identity | Open | — |
| STORE06 · Writer authorization | Open | — |
| STORE07 · Publication visibility | Open | — |
| STORE08 · Multipart confirmation | Open | — |
| STORE09 · Archive completeness | Open | — |
| STORE10 · Restore equivalence | Open | — |
| STORE11 · Discovery contract | Open | — |
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
| API08 · Reply attribution | Open | — |
| API09 · Streaming ingestion | Open | — |
| API10 · Handle lifetime | Open | — |
| API11 · Cancellation isolation | Open | — |
| API12 · Authentication refresh | Open | — |
| API13 · Cross-deployment noninterference (Candidate) | Open | — |

## Resource accounting and operational progress

| Target | Status | Recorded evidence |
| --- | --- | --- |
| RES01 · Reservation ownership | Open | — |
| RES02 · Budget enforcement | Open | — |
| RES03 · Error and cancel cleanup | Open | — |
| RES04 · Queue and frame admission | Open | — |
| RES05 · Scoped shutdown | Open | — |
| RES06 · Health and readiness | Open | — |
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
| FTS04 · Visibility rectangles | Open | — |
| FTS05 · Candidate completeness | Open | — |
| FTS06 · Residual agreement | Open | — |
| FTS07 · Ranking equivalence | Open | — |
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
| COMPAT06 · Evidence provenance | Open | — |
