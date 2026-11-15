# RTDB crate organization and test parity

## The current XTDB modules

XTDB's Gradle projects are packaging boundaries.
The most important boundaries for the Rust port cut across them.
For example, the public type representation lives in `api`, type merging lives in `core`, SQL planning calls Clojure, and the scan operator combines Kotlin and Clojure.
The inventory below uses upstream revision `c84f9334e`, the parent of RTDB's design commit. [Gradle modules](../../settings.gradle.kts) · [Architecture tour](high-level-tour.adoc)

| Current implementation | Responsibility that RTDB inherits | Where to look first |
| --- | --- | --- |
| `api` and `core/xtdb/api` | Values, errors, JDBC/Clojure client behavior, embedded node interfaces, configuration and extension points | [Public API](../../api/src/main/kotlin/xtdb/api/) · [Node and connection API](../../core/src/main/kotlin/xtdb/api/Xtdb.kt) |
| Arrow, types and expressions | Custom vector readers and writers, extensions, type joins, runtime expression specialization, scalar and aggregate functions | [Vectors](../../core/src/main/kotlin/xtdb/arrow/) · [Type design](types.md) · [Expression compiler](../../core/src/main/clojure/xtdb/expression.clj) |
| SQL, XTQL and logical plans | Parse, bind names and parameters, expand DML, lower to relational algebra, rewrite and decorrelate | [SQL](../../core/src/main/clojure/xtdb/sql.clj) · [XTQL](../../core/src/main/clojure/xtdb/xtql/) · [Logical plans](../../core/src/main/clojure/xtdb/logical_plan.clj) |
| Query execution and scan | Prepared queries, operators, live and persisted segments, predicate pushdown, merge planning and bitemporal resolution | [Query source](../../core/src/main/kotlin/xtdb/query/IQuerySource.kt) · [Operators](../../core/src/main/clojure/xtdb/operator/) · [Temporal calculation](../../core/src/main/kotlin/xtdb/bitemporal/) |
| Trie, catalog, metadata, storage and cache | File keys and layouts, block/table/trie catalogs, pruning statistics, object-store access and pinned cache entries | [Trie](../../core/src/main/kotlin/xtdb/trie/) · [Catalog transitions](trie-cat.allium) · [Storage](../../core/src/main/kotlin/xtdb/storage/) |
| Live index, database and transaction resolution | Stage table mutations, read predecessors, publish durable data, freeze snapshots, attach and detach databases | [Live index](../../allium/live-index.allium) · [Partition snapshot](../../core/src/main/kotlin/xtdb/indexer/Snapshot.kt) · [Transaction resolver](../../core/src/main/kotlin/xtdb/indexer/TxResolver.kt) |
| Logs, leader election and block cutting | Source/replica records, ordered enqueue, read-side fencing, block adoption, replay and progress waiters | [Log API](../../core/src/main/kotlin/xtdb/api/log/Log.kt) · [Processor](../../core/src/main/kotlin/xtdb/indexer/LogProcessor.kt) · [Protocol](../../allium/log-processor-lifecycle.allium) |
| Compaction and garbage collection | Choose merge jobs, calculate recency, publish deterministic files and reclaim two families of superseded files | [Compaction](compaction.allium) · [Trie GC](gc.allium) · [Block GC](block-gc.allium) |
| Session, pgwire, Flight SQL and ADBC | Connection-local state, transaction access modes, basis tokens, prepared statements, typed results and errors | [Session rules](tx.allium) · [Pgwire](../../core/src/main/clojure/xtdb/pgwire.clj) · [Flight SQL](../../core/src/main/kotlin/xtdb/flight_sql/) |
| Node, adapters and operations | Wire a node together, authenticate users, serve diagnostics, consume Kafka/Postgres and host cloud storage | [Node assembly](../../core/src/main/clojure/xtdb/node.clj) · [Adapters](../../modules/) · [CDC contract](pgsrc.allium) |

## Workspace design

Put the server in a Cargo workspace under `rust/` and retain the JVM tree as a buildable reference.
The proposed package names use the `rtdb-` prefix.
All packages begin as private workspace members.
Keep `lang/rust` as an external client of the built server.
An engine crate is justified by an independent contract, a genuine second consumer, a heavy optional dependency or a separate ABI.
Start with modules inside each crate for the responsibilities grouped below.

### Foundation and persistence

| Crate | Owns | Direct workspace dependencies |
| --- | --- | --- |
| `rtdb-types` | Passive semantic values: entity IDs and exact IID derivation, temporal dimensions and bounds, qualified names, logical type lattice, structured anomalies, typed positions and tokens | None |
| `rtdb-columnar` | Passive typed views and builders over Arrow arrays, XTDB extension fields, union legs, physical nullability, run-end encodings, selections and batch memory ownership | types |
| `rtdb-temporal` | Passive per-entity history resolver, interval ceiling and recency calculation over an ordered event header; returns surviving source-row references and temporal bounds | types |
| `rtdb-trie` | Passive memory trie, segment traversal, IID bucketing, sorted leaf iterators, page summaries and conservative metadata comparisons | types, columnar |
| `rtdb-format` | Passive codecs for Arrow data/meta files, keys, protobuf catalogs, source/replica envelopes, Transit payloads, legacy headers and storage-version dispatch | types, columnar, trie |
| `rtdb-storage` | Passive block/range I/O and log transport contracts, memory and local implementations, buffer pool and cache leases; cache waiters are monitors owned by the cache | types, format |
| `rtdb-index` | Passive live tables, staged write buffers, catalog transitions, partition generations, catalog queries and durable or transaction-local snapshots | types, columnar, trie, format |

The temporal crate accepts a borrowed event header and an opaque row reference.
Its output can identify the winning row without knowing whether that row lives in a mutable batch, a mapped file or remote storage.
Query scans and the compactor both use this implementation.
Specify how a change of IID resets the ceiling and what an erase does to older events.
A faithful translation of Kotlin's mutable ceiling can be faster to verify than a new algorithm; optimize it only after checking identical polygon boundaries. [Temporal test cases](../../core/src/test/kotlin/xtdb/bitemporal/PolygonCalculatorTest.kt)

Logical type lives in `types`; the mapping onto Arrow fields lives in `columnar`.
A nullable field, a union containing null, a missing struct child and the bottom type have distinct roles in XTDB.
Round-trip those through the physical schema and test value, type and encoding separately.
Use owned Arrow buffers with borrowed read views for kernels; retain an owning handle for any slice that outlives a batch callback.
Keep a memory reservation with the allocation through its last retained slice.
Only the embedded binding needs to export that ownership through a foreign ABI. [Type lattice and Arrow mapping](types.md) · [Vector tests](../../core/src/test/kotlin/xtdb/arrow/)

The format crate owns the bytes and the index crate owns the catalog state that those bytes restore.
Decoding a block produces a version-aware value; applying it reconstructs the table slugs, trie state, watermarks and term boundary.
Keep raw log positions, transaction IDs and sequences as different types.
A missing term sequence in an old record must remain distinguishable from an explicit zero in a leadership claim. [Block schema](../../core/src/main/proto/xtdb/meta/block/proto/block.proto) · [Term-sequence tests](../../core/src/test/kotlin/xtdb/api/log/ReplicaMessageTermSeqTest.kt)

### Planning and execution

| Crate | Owns | Direct workspace dependencies |
| --- | --- | --- |
| `rtdb-plan` | Passive typed expressions, relational algebra, statement and DML representations, catalog interface for binding, logical rewrites, stable diagnostic plan rendering | types |
| `rtdb-sql` | Passive SQL lexer/parser, binder and lowering into plan nodes; preserve dialect extensions, parameter numbering, source spans and errors | types, plan |
| `rtdb-xtql` | Passive EDN/XTQL reader, binding and lowering; preserve distinctions among keywords, symbols, literals, missing bindings and parameters | types, plan |
| `rtdb-expr` | Passive evaluation of bound scalar expressions over typed batches, function dispatch, casts, null handling, time-zone context and predicate metadata translation | types, columnar, plan, trie |
| `rtdb-query` | Passive prepared and executing queries, batch-stream operators, physical planning, spill/cancel, and one scan module merging snapshot segments and resolving visible events | types, columnar, temporal, trie, storage, index, plan, expr |

Have the caller orchestrate frontend selection.
For embedded XTQL in SQL, represent the embedded input as a parsed statement fragment, then dispatch to XTQL during preparation in the application service.
No parser needs to import the other parser.
An execution takes a bound request, one captured snapshot per referenced database, a storage reader, parameters, clock, memory budget and cancellation scope.
Resolve table OIDs, schema and rows from that same basis.
Read-only sessions pin a basis according to the session rules; an execution then acquires its own snapshot handles.
An index-time DML query receives a view scoped to the transaction's database. [Query catalog and DML expansion](../../core/src/main/kotlin/xtdb/query/IQuerySource.kt) · [SQL grammar](../../core/src/main/antlr/xtdb/antlr/Sql.g4)

Keep the scan inside `query` initially.
The reusable pieces already live below it: event merging/trie traversal in `trie`, temporal resolution in `temporal`, frozen visibility in `index` and reads in `storage`.
At a block handoff, an L0 file can already supply rows while the live table still contributes type information.
Expose the snapshot's selected segments and merged schema through accessors so operators cannot accidentally count those rows twice.
Use the same accessor for an ID lookup and a whole-table scan. [Partition snapshot implementation](../../core/src/main/kotlin/xtdb/indexer/Snapshot.kt)

Evaluate a library-backed sort, aggregate or join behind `query` only after the native expression contract is testable.
Require the same types, null behavior, results, cancellation and spill limits for the operator's entire original test family.
Keep custom temporal scan planning, XTDB name resolution and transaction semantics in RTDB.
A cloned implementation of every low-level Arrow vector would add a second memory-management project; start with fixtures that expose the specific differences the adapter must handle.

### Transactions, replication and maintenance

| Crate | Owns | Direct workspace dependencies |
| --- | --- | --- |
| `rtdb-tx` | Passive transaction resolver, ordered staged predecessors, static DML expansion, index-time execution of data-dependent DML, commit/abort outcomes and replica payload construction | types, columnar, format, index, plan, sql, xtql, query |
| `rtdb-replication` | Passive admission/election transition logic plus active per-partition log and leader-term drivers; append order, read-back/apply, block cut/adopt, pending completion and the applied-position monitor | types, format, storage, index, tx |
| `rtdb-maintenance` | Passive job selection, merge, recency partitioning, file writing and GC selection; active workers own bounded scheduling and cancellation | types, columnar, temporal, trie, format, storage, index |
| `rtdb-node` | Active lifecycle of databases and partitions, passive node service operations and session state machines, access control, system tables, preparation and adapter assembly hooks | types, format, storage, index, plan, sql, xtql, query, tx, replication, maintenance |

**Break the DML cycle at the snapshot.**
An UPDATE can need query planning and execution to discover which rows to write.
The leader's `tx` resolver asks `index` for a transaction view overlaying durable rows, staged predecessors and the open transaction's writes.
It executes the plan through `query`, builds the staged result, and hands the result to its owning term.
The query engine accepts a read view with a fixed lifetime.
It never obtains a node or calls back into a leader.
For submit-time expansion of a literal INSERT and index-time expansion of raw SQL from a log, test that both routes produce the same documented effects, time-zone handling and error behavior. [Transaction resolver](../../core/src/main/kotlin/xtdb/indexer/TxResolver.kt) · [Live-index visibility](../../allium/live-index.allium)

**Separate the two positions a leader can reach.**
Only the active term owns its resolver and outstanding writes; it may resolve ahead of durable application.
The long-lived partition reader owns term admission across promotion and demotion.
Read-back advances visible state before the monitor signals callers waiting for application.
Represent accepted enqueue, durable append and admitted apply as distinct events in both production and simulation.
A crash between any two can yield a different legitimate caller outcome. [Log enqueue contract](../../core/src/main/kotlin/xtdb/api/log/Log.kt) · [Leader apply](../../core/src/main/kotlin/xtdb/indexer/LeaderLogProcessor.kt)

**Let the protocol publish maintenance results.**
Maintenance computes a job, writes immutable objects and requests publication through a small capability provided by the node.
Route each message through its specified log and owner; only replay of the appropriate publication updates the serving catalog.
For trie GC, deletion happens before the term publishes the deletion record.
Block GC uses its own retention rules.
Expose a completion back to the worker where the protocol awaits read-back.
The replicated state machine and the maintenance worker do not need to import each other's implementations. [Database and compaction message flow](db.allium) · [Trie GC](gc.allium) · [Block GC](block-gc.allium)

### Protocols, optional adapters and release artifacts

| Package or package family | Ownership and dependency direction | First acceptance boundary |
| --- | --- | --- |
| `rtdb-pgwire` | Active listener and per-connection I/O loop, passive message codecs and PostgreSQL type mappings; calls the node session API | An unchanged driver uses simple and extended queries, binary parameters, transactions, cancellation and error recovery |
| `rtdb-flight-sql` | Active Flight service and expiring-handle manager, passive ticket codecs and Arrow streams; calls the same session API | ADBC client gets the expected metadata, transaction lifetime, authentication result and schema |
| `rtdb-ffi` | Separate native library with a versioned C ABI, opaque retained handles and Arrow release callbacks over node/query operations | A JVM binding retains a result past statement close, cancels a second stream and releases all native resources exactly once |
| `rtdb-kafka`, `rtdb-postgres-source` | Kafka implements the log and supported ingestion contracts; the Postgres adapter owns replication-slot I/O and source checkpoints; the node supplies submission and durable progress capabilities | Real broker reassignment, oversized messages, restart from a durable token and upstream WAL confirmation pass |
| `rtdb-s3`, `rtdb-azure`, `rtdb-gcs` | Optional crates implement storage traits over independent provider SDKs; SDK types stay private | Run range, upload, retry, interrupted read, deletion, credential and restore tests against each supported service |
| `rtdb-server` | Binary composition root constructs the runtime, adapters, credentials, telemetry, frontends and node; owns shutdown ordering | The released image passes the original client suites, startup, readiness, shutdown, diagnostics and restore tests |
| `rtdb-testkit`, `rtdb-conformance` | Test-only fixture codecs, virtual clock, deterministic transport and fault injection; separate runner drives JVM and Rust processes and owns parity reports | Run a case from a clean checkout by upstream revision and case ID, reproduce its inputs, and locate both results |

Keep authentication policy and authorization decisions in the node service so pgwire, Flight and embedded calls enforce the same grants.
Keep token negotiation and PostgreSQL-specific error serialization in their respective frontends.
Expose telemetry at state transitions as typed events or counters; the binary chooses exporters.
Use system-table providers over the same captured basis as ordinary queries, including metadata-only queries from drivers.
The workspace DAG can be checked in CI with Cargo metadata: production packages must build without the test harness, cloud SDKs enter through adapters, and leaf algorithms build without the asynchronous server runtime.

## What complete test parity means

For each test collected from the pinned original implementation, identify the behavior it asserts, execute it against Rust directly or through an equivalent native test, and retain evidence that the assertion was exercised.
**Every collected case needs an executable counterpart before parity can be claimed.**
A case with no agreed translation stays open.
Keep the original test source and expected data available so a reviewer can tell whether a port weakened the assertion.
A green Rust unit-test count alone cannot establish coverage.

### Inventory and case identity

1. Collect JUnit test descriptors, Clojure test vars, parameterized invocations, dynamic templates, fixtures, tags, feature flags and skipped cases across all Gradle projects at the pinned revision.
   Reconcile runtime discovery with the source tree and workflow definitions so a disabled class or unselected tag stays visible.
2. Add external cases separately: language clients under `lang/`, SQL logic records, database and Arrow golden files, Docker and cloud suites, and the failure scenarios generated by property tests.
   The checkout contains 611 tracked `.test` files under `sqlite-test/`; the standard SLT tasks select subsets and permit some failures.
   Record the upstream selection and per-record outcome, and distinguish the larger corpus from the regularly executed baseline.
3. Give each entry an identity comprising source revision, relative path, qualified test name or SLT record position, and parameter/fixture identity.
   Store the original assertion or its source location, owning Rust crate, counterpart test ID, required environment, comparison method and latest results for both engines.
   Track renames with an explicit alias; duplicate names cannot silently collapse two cases.
4. Materialize a machine-readable ledger under the proposed `rust/conformance/` tree and emit a per-suite report for each paired run.
   The collector should fail on newly discovered cases without mappings and on mappings whose Rust tests disappeared.
   A skipped cloud run or unavailable service is an unverified result.
   A permanently JVM-specific check, such as a JVM allocator lifetime test, remains separately identifiable and requires a reviewed native assertion of its resource invariant.
   Where the JVM API remains supported, exercise the original assertion through the binding as well.

Use Gradle for JVM collection and execution.
The reference lanes include unit, integration, property/simulation, nightly cloud, SLT, language clients and the tagged suites used by the release matrix.
Benchmark runs establish performance budgets in a separate report.
Track fixtures used by several cases by digest and generating revision. [Gradle test selection](../../build.gradle.kts) · [CI workflows](../../.github/workflows/) · [Language suites](../../lang/) · [Shared fixtures](../../src/testFixtures/)

A proposed ledger entry for one upstream regression looks like this.
The status records work left to do; the Rust target names describe proposed tests.

```yaml
reference: c84f9334e
case: xtdb.as-of-test/erased-entity-absent-from-system-time-window-before-erase-6358
source: src/test/clojure/xtdb/as_of_test.clj
owner: rtdb-query
counterparts:
  - target: rtdb-conformance::as_of::historical_window_after_erase
    comparison: typed_rows_at_equal_durable_basis
  - target: rtdb-query::scan::erase_survives_temporal_pruning
    comparison: result_with_and_without_pruning
requires: [local-log, local-storage]
status: unmapped_until_tests_exist
```

A run attaches its JVM binary digest, Rust revision, fixture digest, seed or trace ID, executed assertion IDs, outcomes and diagnostics to the entry.
Reject a mapping to a filtered-out test or a test that executed zero assertions.
For a single upstream test with multiple materially different assertions, record each assertion group and its corresponding output or native invariant.

### Four ways to preserve an assertion

| Upstream test style | Rust counterpart | Evidence of equivalence |
| --- | --- | --- |
| Network client or SQL behavior | Run the same client script and fixture unchanged against two server URLs whenever possible | Typed rows, column order, schema, SQLSTATE, side effects and transaction outcomes at equal bases |
| Pure JVM function or internal structure | Share a typed input/expected-output fixture and implement the same assertions in the owning Rust crate | Both produce the same result for each assertion, including boundary values, errors and internal states that the test intentionally inspects |
| File, log or plan golden | Replay exact input bytes through both readers; generate both writer directions; expose a canonical logical-plan view for plan tests | Verify bytes where they are a contract, otherwise compare independently decoded structure and semantics; retain operator placement for rewrite tests |
| Concurrent or property test | Retain generator domains, distributions, iteration counts and invariants; capture concrete operations, delivery decisions and injected failures as a replayable trace | Both implementations consume the same trace and satisfy an independent model, with diagnostics at the first divergence |

The existing SQL logic adapter can redefine the JVM planner and inject synthetic table metadata.
Port these fixture semantics into the harness; simply redirecting a connection string would change what it tests.
Likewise, an identical numeric RNG seed across Kotlin and Rust need not produce identical operations or schedules.
Persist the generated input stream and scheduling decisions, inject a virtual clock at the driver boundary, and keep the minimized failing trace in the corpus. [SQL logic adapter](../../src/test/clojure/xtdb/sql/logic_test/xtdb_engine.clj) · [Seeded dispatcher](../../core/src/test/kotlin/xtdb/SimulationTestBase.kt)

### Five concrete mappings to implement first

| Original assertion | Rust owner and fixture | Failure it catches |
| --- | --- | --- |
| The ceiling for one IID cannot affect the next IID | `temporal`: replay the three ordered events from the JVM test and compare the valid-time breakpoints and system-time ceilings | A stateful resolver returns correct point prices but wrong interval endpoints |
| A zero claim position survives encoding and is distinct from an absent position | `format` checks both serialized variants; `replication` feeds both into admission | A default protobuf value changes election behavior after replay |
| Plain and run-end encoded IID files coexist and agree across query and write paths | `columnar`, `format` and `query` verify multi-row runs and both live encodings. The full case compares query rows, pgwire IIDs, PATCH results, and Flight/ADBC IID values across multiple batches with one declared 16-byte fixed-binary field | One physical encoding silently drops versions for an entity |
| An erased entity is absent even in a system-time window before its erase | `query` runs the original as-of test after erase at the same durable position, then repeats across flush and compaction | File pruning skips the erase and resurrects old data |
| Independent simulated nodes converge on block watermarks, durable TX and snapshot basis | `replication` and `node` replay recorded `LogProcessorSimTest` operations with broker faults, role changes and block uploads | A leader and follower answer the same simple query but restart at different positions |

Original assertions appear in the following tests. [Per-IID ceilings](../../core/src/test/kotlin/xtdb/bitemporal/PolygonCalculatorTest.kt) · [Term sequences](../../core/src/test/kotlin/xtdb/api/log/ReplicaMessageTermSeqTest.kt) · [Mixed encoding](../../src/test/clojure/xtdb/mixed_iid_encoding_test.clj) · [Erase and time windows](../../src/test/clojure/xtdb/as_of_test.clj) · [Processor convergence](../../core/src/test/kotlin/xtdb/indexer/LogProcessorSimTest.kt).

### Comparison rules and release gates

- Fix the current time, default time zone, valid-time point, system-time point, durable transaction basis, storage version and feature settings.
  Compare ordered rows when order is specified, otherwise compare typed multisets preserving duplicates.
  Use type-specific rules for floating-point results and allow nondeterminism only where the individual assertion does.
- Compare statement lifecycle as well as rows: preparation errors versus execution errors, error codes, abort and retry behavior, await-token progress and visibility from another client.
  Keep expected failures and upstream exclusions attached to exact case IDs.
  A global SLT failure budget could let a regression replace an unrelated fixed case without changing the total.
- For each supported generation, run JVM→Rust and Rust→JVM storage recovery, including crashes after data upload, before metadata publication, after append, before apply and during handoff.
  Assert byte identity for retriable outputs sharing a key and visibility at several historical bases.
  Keep a model independent of both engines for the subset of histories it covers.
- Report discovery coverage, executable mapping coverage, cases passing in both, expected failures matching, unresolved differences, skipped/blocked cases and run environment per lane.
  A tested bug fix with a deliberate difference is a versioned behavior change and should be reported separately from strict parity.
  Promotion requires every in-scope original assertion accounted for and executed, zero unexplained divergences, and evidence for every supported configuration.
  Publish dormant upstream cases and their disposition alongside that result.
- When advancing upstream, regenerate the catalog, diff added/removed/changed assertions, run the old and new reference engines, and require an explicit baseline decision before updating fixtures.
  Store failure inputs once and replay them offline without credentials or production data.

## Implementation order

1. **Deliver the inventory before the engine.** Build the collector, case schema, JVM runner and report skeleton; run the JVM twice on fixed inputs to measure the oracle's own nondeterminism.
   Assign an owner and counterpart strategy to every original suite and make unmapped additions visible in CI.
2. **Ship the bottom crates together with their assertions.** Implement `types`, `columnar`, `temporal`, `trie` and read-side `format`.
   Read JVM-written blocks and test old and current representations, exact IIDs, interval boundaries and erase semantics.
3. **Serve a real read-only vertical slice.** Implement the read-only subset of `node` (session, snapshot acquisition and preparation) and `replication` (admission and follower apply), with `storage`, `index`, a focused `plan/sql/expr/query` subset, and a pgwire listener.
   Add dependencies on the writable term and resolver only when implementing the next milestone.
   Drive the corrected historical-price scenario through a stock client, a frozen snapshot and a replayed tail.
   Continue bringing up parser, planner, operator and XTQL families against their mapped suites.
4. **Introduce one writer, then adversarial schedules.** Add transaction overlays and local writes, then replicate the exact append/read-back/apply protocol.
   Differentially compare every durable transition through crash, restart and role change before enabling maintenance publishing.
5. **Qualify the complete release.** Add compaction and GC, Flight and embedded bindings, backend adapters, security and operational interfaces.
   Exercise the original client, cloud, property, SLT and failure matrices on built artifacts; rehearse the two-way migration in the [rewrite proposal](xtdb-rust-rewrite.md).

The first reviewable deliverable should be a parity report with actual upstream case IDs and a working pair of runners for the per-IID ceiling and term-sequence examples.
Add the mixed-file scan, erase window and multi-node trace as their implementing crates become executable.
That gives reviewers an executable definition of what each new crate must preserve and a way to reject a translation that passes for the wrong reason.
