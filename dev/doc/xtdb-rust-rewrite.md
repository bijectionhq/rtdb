# Design proposal for a Rust implementation of XTDB

Build a standalone Rust server that can open an existing XTDB database, serve its clients, and eventually take sole responsibility for writes, recovery, compaction, and garbage collection.
Start by making Rust a second reader of a database produced by the JVM server.
Qualify the first production migration with a bounded write pause and one generation of writers active at a time.
The JVM remains the reference implementation during development and a tested recovery option during the initial deployment window.
The [crate and test parity design](rtdb-crates-and-test-parity.md) maps each subsystem to a proposed Cargo package and defines how the original test assertions carry over.

Build the development reference from XTDB commit `c84f9334eb060ceb92d28ecdd4a06e6b625932fd`.
For a pilot, pin the deployed JVM distribution, configuration and intended rollback distribution independently.
The development reference determines test parity; the pilot binaries determine whether an operator can transfer ownership in both directions.
The design assumes a JVM-free server is a product requirement in its own right.
Performance targets below are proposed acceptance criteria, not measurements.

## 1. Define what an operator can replace

| Surface | First general release | Evidence required |
| --- | --- | --- |
| Queries and transactions | SQL and XTQL, temporal DML, session behavior, read and await tokens, authorization, error codes, result types and nullability | The same client programs and histories produce equivalent observable results |
| Data at rest and in flight | Read supported block and table catalogs, Arrow data and metadata, local and Kafka logs, legacy envelopes and externally stored oversized records | Restore each supported generation, replay the retained tail, compare history, then restart |
| Production interfaces | Pgwire, Flight SQL, supported ADBC workflows, configuration, health, metrics, auth and cloud deployment | Run client suites, cloud integration tests, and operator runbooks against a released Rust binary |
| Java and Clojure applications | Preserve remote clients, provide a compatibility binding for supported embedded calls, including resource lifetimes and cancellation | Run existing applications against the binding, publish an explicit migration for each incompatible extension |
| Upgrade and downgrade | Controlled transfer of write ownership, preserve every acknowledged transaction through rollback during the compatibility window | Rehearse JVM → Rust → JVM with writes, aborts, erases and a crash between handoffs |

Inventory deployed JVM modules, custom authenticators, log or storage factories, and in-process Arrow consumers with pilot operators.
A native executable cannot load an existing JVM implementation of a plugin interface by linking it as a Rust crate.
Offer an equivalent native adapter, a separately deployed bridge, or a documented migration before declaring the affected deployment eligible.
A pilot may cover local storage or S3 first.
General availability covers every backend and interface advertised for that release. [Server API](../../core/src/main/kotlin/xtdb/api/Xtdb.kt) · [Module inventory](../../settings.gradle.kts) · [Cross-language clients](../../lang/README.adoc)

## 2. Shape the implementation around owned state

Add a Cargo workspace under `rust/` beside the Gradle build.
Keep `lang/rust` as an ordinary client that can point to either server.
Build a single server binary and a test driver that consumes replay fixtures.
Use the companion crate map for package dependencies; the table below describes runtime ownership across those packages.
The client protocols, storage adapters, and asynchronous runtime may depend on the engine.
The type system, history algorithm, and catalog transitions can be tested synchronously with owned inputs.

| Proposed component | Role and owned state | Contract |
| --- | --- | --- |
| Types and formats | Passive values and codecs own logical types, IDs, intervals, tokens, and format-version mappings | Encode, decode, and validate against golden vectors from each supported JVM generation |
| Query planner and operators | Passive planner owns an immutable plan, a query execution owns its snapshot handles, batch buffers, memory reservations and cancellation scope | Bind parameters and names against the execution basis, yield typed Arrow batches and release resources on completion or cancellation |
| Session | Passive state machine owned by one connection or Flight transaction handle | Pending, reading, writing, and failed states determine legal operations, reading owns its pinned basis and writing owns its DML buffer |
| Partition state and snapshot | Passive partition state publishes a single immutable generation, each reader owns a retained reference to its snapshot | Catalogs, table schema, live rows and durable position agree at the snapshot boundary |
| Log processor and leader term | Active processor chooses between replica delivery and lifecycle events, a leader term owns ordered staging, block-cut state and outstanding appends | Admit each record at its position in the replica log, apply in order, demotion terminates outstanding work |
| Applied-position monitor | Monitor object owns the durable progress and error state on which clients wait | A waiter observes either its requested position or a terminal outcome, listener cancellation releases only its own wait |
| Compaction and GC | Active maintenance drivers schedule work, passive merge and deletion jobs own their bounded local buffers | A compaction job has reproducible output, physical deletion and catalog publication follow the ordering for that file family |
| Storage and log adapters | Passive I/O objects own client handles, cache entries and buffer leases | Expose range reads, immutable object publication and ordered log operations with explicit completion and error semantics |

Use separate Rust types for source position, replica position, transaction ID, block index, term, and sequence within a term.
Preserve their encoded widths, epochs and sentinel meanings at format boundaries.
Model an in-flight block and staged transactions on the states that own them.
Store the replica reader's term and sequence across role transitions at the partition level.
Publish a whole partition generation through one synchronized pointer swap.
An initial implementation can use a short-held lock around an immutable shared reference.
Retain each buffer until its final reader releases it.
During shutdown stop admitting new work, signal cancellation to database-owned tasks, await their termination, then release the underlying storage and buffers.
Test cancellation while an asynchronous read holds a cache lease and while an embedded consumer retains an exported batch.
Enforce byte budgets for scans, joins, live writes, compaction, and cache retention.
Collect peak memory and queue depth under concurrent workloads. [Partition state](../../core/src/main/kotlin/xtdb/database/PartitionValue.kt) · [Replica admission](../../core/src/main/kotlin/xtdb/indexer/LogProcessor.kt) · [Session model](tx.allium) · [Object roles](../../.claude/skills/xtdb-object-boundaries/SKILL.md)

## 3. Read and write the existing formats

Build the reader starting at the persisted block.
Locate the database and partition by storage version and epoch, load the last completed block, resolve table directories from recorded slugs, and restore table and trie catalogs.
Decode the data and metadata Arrow schemas, including extension types, dense unions, encoded historical IDs, and files written before particular metadata existed.
A missing pruning statistic means the scan has to consider that file.
An unrecognized required format or unavailable log range stops recovery with the database, partition, file or offset needed to diagnose it. [Block format](../../core/src/main/proto/xtdb/meta/block/proto/block.proto) · [Storage roots](../../core/src/main/kotlin/xtdb/api/storage/Storage.kt) · [Trie catalog](trie-cat.allium)

Generate protobuf bindings from the repository schemas.
Implement the framing around them, including legacy transaction headers, optional term sequence fields, Transit-encoded errors, and oversized payload references.
Implement ID encoding directly from fixtures for strings, keywords, signed integers and UUIDs.
Record the object keys, page boundaries, logical types, metadata values and content hashes for each storage fixture.
Exercise both directions with independent processes, first with a JVM writer and Rust reader, then with a Rust writer and JVM reader.
Use byte-identical output for every compaction or block job that either implementation can retry under an existing key.
A JVM node can upload data and crash before publishing its metadata, leaving the same job for Rust after an exclusive handoff.
Test both retry directions with an existing partial output.
A future format that changes bytes under those keys requires a distinct versioned namespace and a catalog migration readable by the designated fallback. [ID encoding](../../core/src/main/kotlin/xtdb/util/Iid.kt) · [Source envelope](../../core/src/main/kotlin/xtdb/api/log/SourceMessage.kt) · [Replica envelope](../../core/src/main/kotlin/xtdb/api/log/ReplicaMessage.kt) · [Compaction contract](compaction.allium)

## 4. Query and temporal execution

Keep an XTDB logical type representation above Arrow physical arrays.
Implement the type join, null and bottom types, nested structs and lists, union dispatch, extension values, decimals, timestamps, time zones, intervals and casts before declaring SQL expression parity.
Use the current grammar, plan fixtures and behavioral tests as inputs to a Rust parser and binder.
Normalize SQL and XTQL into a common typed logical plan and lower it to scans, filters, projections, joins, grouping, ordering and windows.
Bind dynamic table types and prepared-statement parameters at the points required by the reference engine.
Include a cache test that prepares a statement, changes a column's type through ingestion, and executes it again. [SQL grammar](../../core/src/main/antlr/xtdb/antlr/Sql.g4) · [Logical planning](../../core/src/main/clojure/xtdb/logical_plan.clj) · [Type semantics](types.md)

Implement one bitemporal scan shared by all frontends.
It opens the query snapshot, selects catalog entries and live segments, prunes candidate files and pages, merges events by internal ID and system time, and resolves visible rows and their temporal bounds.
A pushed-down value predicate must preserve events that can supersede or erase a candidate.
For example, searching for May prices above $10 must still see a later correction that lowers the May price to $9.
Keep the general interval resolver as the reference for specialized point-in-time paths.
Differentially test scans with pruning enabled and disabled, including queries projecting the four time columns, touching a half-open interval boundary, or spanning an erase. [Scan operator](../../core/src/main/clojure/xtdb/operator/scan.clj) · [Page selection](../../core/src/main/clojure/xtdb/trie.clj) · [Temporal resolution](../../core/src/main/kotlin/xtdb/operator/scan/EntityResolver.kt)

Use Rust Arrow buffers and IPC where the compatibility corpus verifies their behavior.
Evaluate DataFusion for conventional joins, grouping, sorting and spilling behind the physical plan boundary.
The adoption experiment must execute mixed-type joins, decimal aggregation, null handling, temporal values and cancellation under a memory cap.
Compare rows, schemas, errors, spill volume and peak memory against the JVM.
Adopt individual operators that pass.
Implement the rest using XTDB semantics over Arrow batches.
This keeps library selection off the critical path for the format reader and history resolver. [Expression compiler](../../core/src/main/clojure/xtdb/expression.clj) · [Operator implementations](../../core/src/main/clojure/xtdb/operator/)

## 5. Writes, replication and recovery

1. The frontend binds DML to the session context, applies access-mode and authorization checks, buffers a writing transaction, and submits it through the configured source.
2. The leader resolves each transaction in order against durable rows plus its staged predecessors, records a commit or abort, and enqueues the resolved message in the replica log with its leadership term and sequence.
3. The replica reader checks the term and sequence before dispatch.
   An old term, duplicate position, and gap have distinct outcomes.
   On read-back of an admitted transaction, the leader checks its staged head.
   A follower imports the encoded rows.
   Publish durable state before waking waiters for application.
   Preserve the distinction between an acknowledgement of submission and an acknowledgement that a transaction has been applied.
4. During a block cut, hold records behind the boundary in order.
   Publish immutable files and adopt the block on the matching upload record.
   A crash can leave uploaded files without an adopted catalog entry.
   Recovery starts from a confirmed block, seeds the fence from its boundary, and replays the retained replica tail.
5. Reproduce native-source and external-source progress separately.
   PostgreSQL CDC needs a tested mapping between durable XTDB progress and the WAL position it may confirm upstream.
   Advancing that position can allow PostgreSQL to discard data.

Implement admission and catalog transitions as synchronous state machines driven by explicit inputs.
Have the asynchronous shell execute appends, reads, uploads and timers and feed their outcomes back in recorded order.
Inject faults between enqueue, durable append, read-back, snapshot publication, boundary upload and adoption.
After cancellation, complete outstanding callers according to the reference protocol and reconcile any unknown result by its recorded identity before retrying. [Database processing model](db.allium) · [Leader implementation](../../core/src/main/kotlin/xtdb/indexer/LeaderLogProcessor.kt) · [Election and fencing](../../allium/log-processor-lifecycle.allium) · [CDC confirmations](pgsrc.allium)

Run compaction through the same temporal primitives as query execution.
Compare both the visible history before and after compaction and the output metadata used to skip pages.
Model trie GC and block GC separately.
Trie GC deletes superseded objects then publishes the deletion through the replica log.
Superseded block catalogs have their own startup and catch-up retention contract.
Fault tests must cover a crash after physical deletion and before publication, followed by an idempotent retry.
Exercise attach/detach, concurrent readers, a role change and restart while maintenance is pending. [Compaction implementation](../../core/src/main/kotlin/xtdb/compactor/SegmentMerge.kt) · [Trie GC](gc.allium) · [Block GC](block-gc.allium) · [Database lifecycle](../../allium/database-lifecycle.allium)

## 6. Client and operational parity

Route pgwire, Flight SQL and embedded bindings through the same session and transaction model.
Run real drivers through prepare/bind/execute, text and binary parameters, transaction abort and rollback, cancellation, backpressure, metadata discovery, and disconnect with an open transaction.
Verify SQLSTATE, error category, column order and types, PostgreSQL OIDs where observable, and Arrow extension metadata.
Test authentication and authorization failures through each frontend, including access after a database is detached or a session resets.
For embedded Java or Clojure, prototype a native binding early with one pinned Arrow batch, one canceled stream and a repeated open/close test.
Document which side owns each buffer and which operation releases it. [Pgwire](../../core/src/main/clojure/xtdb/pgwire.clj) · [Flight SQL](../../core/src/main/clojure/xtdb/flight_sql.clj) · [Transaction surfaces](tx.allium)

Ship the server binary, container image and configuration migrator together.
The migrator should resolve a deployment's backend, auth settings, module registrations, storage roots, and log identities and list unsupported options before cutover.
For Classic S3, the offline configuration report can compare a captured inventory of exact keys and provider version IDs with the literal JVM and legacy RTDB spellings for recorded logical paths.
The [Classic S3 preflight procedure](../../rust/crates/config/README.adoc#classic-s3-key-preflight) covers capture, key-layout eligibility and the upgrade decisions that follow it.
Export per-partition durable position, term, replication lag, live bytes, compaction backlog, object reads, cache behavior, rejected waits and query cancellations.
Validate clean shutdown, slow object storage, exhausted disk cache, credential rotation and restore from real backend snapshots in integration tests.

## 7. Conformance and performance

Use the [parity ledger and comparison rules](rtdb-crates-and-test-parity.md#what-complete-test-parity-means) as the acceptance contract for functional behavior.
Record the applied history, valid-time interval and system-time interval separately.
An execution snapshot bounds the system time it can observe.
For a paired read, stop or synchronize the readers at a named prefix and issue the same temporal query.
A fixed SQL timestamp alone cannot align fixtures that have applied different erases.

Benchmark the JVM and Rust binaries at the same commit-equivalent data state, configured memory budget, concurrency, object-store placement and compaction state.
Measure cold and warm ID lookups, bitemporal ranges, mixed-type joins, bulk ingest, replicated commits, compaction under load and restore.
Keep throughput, p50/p95/p99 latency, peak resident memory, bytes and requests to storage, and recovery time beside the fixture and configuration digest.
Repeat on the JVM first to estimate run-to-run variance.
As a provisional pilot trigger, investigate a repeatable p95 regression above 10% or throughput loss above 10%.
Set release limits against the pilot workload and its observed variance.
Any unexplained semantic or durability divergence blocks promotion regardless of throughput. [Benchmark workloads](../../modules/bench/src/main/clojure/xtdb/bench/)

## 8. Delivery sequence and staffing

The ranges below are cumulative planning estimates for five to eight full-time engineers with database internals, Rust, and XTDB production experience.
They assume access to operators and representative workloads and a controlled cutover.
The first six weeks should replace the most uncertain estimates with measurements.

| Elapsed time | Deliverable | Evidence to proceed |
| --- | --- | --- |
| Weeks 1–6 | Capture the upstream inventory and pair the JVM and Rust runners for the per-IID ceiling and absent/zero sequence fixtures | Both produce the original observations, injected comparator errors are caught, and remaining unmapped cases are reported |
| Months 2–5 | Rust historical reader and read-only replica follower | Reproduce the product-history fixture in §10 across live rows, compaction, restart and catch-up from a retained block |
| Months 5–10 | Writable standalone server, session state, temporal DML and local recovery | Differential histories pass through puts, corrections, deletes and erases, acknowledged writes survive process kills |
| Months 9–18 | Replicated writes, election, compaction, GC, Kafka and first cloud pilot, broaden query execution in parallel | Deterministic fault scenarios converge, shadow traffic agrees at a common basis, backlogs drain under sustained load |
| Months 16–30 | Remaining cloud adapters, CDC, client and embedded compatibility, configuration and operations | Two-way restore, driver matrix, authorization, performance budgets and a full cutover/rollback rehearsal pass for each pilot configuration |
| Months 24–36 | General availability and operational handoff | All advertised combinations pass the version matrix, pilot operators complete the agreed observation window and recovery drill |

Give durable formats and temporal semantics a named owner, replication and failure simulation a second, and SQL/types a third.
Staff client interfaces, conformance, release engineering and backend qualification alongside them.
Publish a compatibility matrix for every release and record any change in the JVM reference after the baseline as a separate decision.
Re-estimate at the read-only milestone if format compatibility needs custom Arrow writers, at the writable milestone if the fault model diverges, and before pilot if embedded bindings or a mandatory rolling upgrade expand the scope.

## 9. Production cutover and rollback

1. **Qualify the deployment.** Inventory databases, partitions, attached databases, writers, CDC sources, compactors, GC workers, client versions and restore procedures.
   Choose an operator-approved write-pause budget and recovery-time target.
   Reserve sufficient log retention and object versions for the rehearsal and rollback window.
2. **Shadow reads.** Run Rust with read-only credentials against the required store and log range, or against a versioned copy with an explicit replication position.
   Disable election, appends, compaction publication, GC and upstream CDC acknowledgements on the shadow.
   Compare requests at the same valid time, system time and durable basis, and retain reproducible mismatches.
3. **Record a handoff boundary.** Pause ingress and upstream intake, drain accepted work, reconcile ambiguous client outcomes, and flush and confirm outstanding blocks.
   Stop or fence every JVM publisher, including maintenance workers and upstream CDC acknowledgements.
   Reread the final log heads and capture each partition's finalized block, source and replica positions, durable transaction, term/sequence, storage version and epoch, and external token.
   Verify a restore from the protected files plus retained tail.
   Keep the manifest with the restore artifacts and record which operator or control plane owns the writer lease.
   A mismatch between a manifest position and the recovered position keeps ingress paused.
   Grant Rust the writer credentials after manifest verification and confirmation that prior writers have lost publication access.
4. **Transfer ownership.** Have Rust recover through the manifest and assert exact agreement.
   Exercise election and an identifiable canary write, confirm read-back, query it from a second node, restart a node, and check both current and historical results.
   Resume traffic gradually while comparing independent observations and auditing CDC progress.
5. **Reverse the handoff if required.** Before the first Rust mutation, the original JVM boundary suffices.
   After any Rust mutation, pause ingress again, drain and fence Rust, and recover the qualified JVM release through the last Rust position, including every acknowledged transaction and external token.
   Verify history and canary reads before reopening writes.
   Restoring the pre-cutover backup alone would lose subsequent acknowledged work.
   Require this reverse path to pass on a clone before the first production write.
   Inject a failure immediately after the Rust canary is acknowledged and another after a Rust compactor uploads only part of an existing job.
   The fallback must recover the canary and finish or retry the maintenance job without changing the visible history.
   If reverse recovery cannot reach the last acknowledged position, retain the Rust log and files, keep ingress paused, and recover forward from the verified artifacts.
6. **Close the rollback window deliberately.** Allow new encodings or features unreadable by the fallback release only after operators end that window and verify a new restore point.
   If a deployment requires simultaneous JVM and Rust writers, qualify mixed elections, bidirectional log decoding, equivalent compaction keys and bytes, and GC races as a separate release milestone.

Treat an unexplained history mismatch, an acknowledged write missing after replay, an unqualified storage generation, or a failed reverse handoff as a reason to hold the deployment at its last verified stage.
Observe retention and erasure policies on shadow copies, replay traces and backups as well as the serving cluster. [Backup and recovery](../../docs/src/content/docs/ops/backup-and-restore/overview.md) · [Block checkpoint fields](../../core/src/main/proto/xtdb/meta/block/proto/block.proto)

## 10. Read-only follower acceptance fixture

The first deliverable pairs the two small JVM and Rust tests in the [implementation sequence](rtdb-crates-and-test-parity.md#implementation-order).
The first end-to-end reader milestone uses a product history with known answers at each replay prefix.
Use a UTC clock with all changes recorded at midnight on the indicated dates in 2026.
Treat the system-time instants as inclusive points within the execution snapshot.
Write product A at $12 on April 1 and product B at $7 as a surviving control.
Write A at $15 effective June 1.
On July 1, correct A's price to $11 for the half-open valid-time interval [May 1, June 1).
On August 1, delete A effective that day.
On September 1, erase A.
The control stays at $7 throughout.

| Applied prefix | Valid-time instant | System-time instant | Expected result for A |
| --- | --- | --- | --- |
| June put | May 15 noon | June 1 midnight | $12 |
| July correction | May 15 noon | May 15 noon | $12 |
| July correction | May 15 noon | July 1 midnight | $11 |
| July correction | June 1 midnight | July 1 midnight | $15 |
| August delete | August 2 noon | August 1 midnight | No row |
| August delete | May 15 noon | August 1 midnight | $11 |
| September erase | May 15 noon | May 15 noon | No row |

Each prefix names the last applied user transaction.
Flush and compaction add control records, so capture a separate physical checkpoint and replay offset for each layout while holding the user history fixed.
For example, the query below runs at the July prefix and returns one row with price 11.

```sql
SELECT price
FROM products
  FOR VALID_TIME AS OF TIMESTAMP '2026-05-15T12:00:00Z'
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-07-01T00:00:00Z'
WHERE _id = 'A';
```

Capture the JVM's projected valid/system bounds as additional expected values.
Run each prefix with live rows, after flush, after compaction and after cold restart using fixture-specific checkpoints.
Run each query for A and B, first through normal pruning and then with temporal file/page filtering bypassed.
Keep the IID predicate and the visibility resolver active in both runs.
The June 1 query catches a correction that wrongly includes its upper endpoint.
The last two prefixes distinguish a delete that preserves May history from an erase that removes it.
Deliver the fixture generator, protected checkpoints and tails, typed expected results and bytes-read measurements for each layout. [Snapshot bounds](../../core/src/main/clojure/xtdb/operator/scan.clj) · [Original temporal tests](../../src/test/clojure/xtdb/as_of_test.clj)

**JVM reference result.**
At the pinned revision, all 112 price observations matched the table: seven queries × two products × four layouts × two temporal-pruning modes.
The local-node run checked the new transaction in memory, its flushed block, the result after explicit compaction, and the reopened database.
The compaction check required a live products trie above level zero.
The bypass returned every candidate from `trie/filter-pages`.
These observations cover the price projection; the Rust runner must also compare projected temporal bounds, record both binaries and attach the required receipts. [Local-node fixture](../../src/testFixtures/clojure/xtdb/test_util.clj) · [File and page filtering](../../core/src/main/clojure/xtdb/trie.clj)
