# RTDB engine properties to prove

This catalog inventories proof obligations at the engine's subsystem boundaries, from logical values to storage, replication, and client-visible behavior.
The baseline is the published RTDB multi revision `e86af7f12`.
Full-text search has its own section because its published specification is on `main` at `1c8d111ef`. [RTDB multi baseline](https://github.com/bijectionhq/rtdb/tree/e86af7f12) · [Full-text search baseline](https://github.com/bijectionhq/rtdb/tree/1c8d111ef)

Unmarked entries restate a documented contract or express a proof obligation for it.
**Candidate** marks a proposed extension to the contracts or a stronger claim that merits explicit agreement.
Each entry is a verification target, and the linked sources establish its intended scope.
The recovery study records which abstract theorems, bounded model checks, and implementation observations already exist for recovery properties S1–S7 and conditional progress. [Recovery proof status](rtdb-recovery-properties.md#proof-status)
Locally checked evidence for the engine catalog is tracked in the [engine verification record](rtdb-engine-proof-evidence.md).
The [verification ledger](rtdb-engine-verification-ledger.md) tracks each property by its catalog ID.

The scope is exhaustive at the contract level covered by the cited engine specifications and crate map.
Use the applicable language, format, frontend, and provider conformance inventories to enumerate individual inputs and supported versions. [Crate map and parity inventory](rtdb-crates-and-test-parity.md) · [Conformance lanes](../../rust/conformance/README.adoc)

## 1. Bitemporal history

Sources include the [time semantics](../../docs/src/content/docs/about/time-in-xtdb.md), [temporal operator reference](../../docs/src/content/docs/reference/main/stdlib/temporal.mdx), [engine history fixture](xtdb-rust-rewrite.md#10-read-only-follower-acceptance-fixture), and [Rust temporal resolver](../../rust/crates/temporal/src/lib.rs).

- **TIME01 · Two independent axes.** A query selects from a captured transaction prefix by system time and independently selects effective intervals by valid time.
- **TIME02 · Interval endpoints.** Point and range predicates respect the documented endpoint conventions, including coincident endpoints, empty intervals, and adjacent half-open ranges.
- **TIME03 · Open bounds.** Open ends and representable minimum and maximum timestamps retain their meaning through normalization, comparison, metadata, and projection.
- **TIME04 · Reference history.** For one entity, the resolver produces exactly the visible history defined by the ordered puts, deletes, and erases through the query's basis.
- **TIME05 · Correction scope.** A later correction changes visibility only over its effective valid-time overlap and retains earlier system-time versions where the history permits them.
- **TIME06 · Delete semantics.** A delete suppresses rows for the applicable valid-time range and system-time basis while preserving the supported historical record.
- **TIME07 · Erase semantics.** Once an erase is visible to a newly resolved snapshot, queries at earlier system times within that snapshot do not recover erased history.
- **TIME08 · Erase and new writes.** Same-ID events after an erase follow the specified ordering and do not reintroduce pre-erase versions.
- **TIME09 · Per-entity isolation.** The visibility state or interval ceiling for one IID has no influence on any other IID.
- **TIME10 · Rectangle projection.** The four projected time bounds describe exactly the visible history fragments returned by the interval resolver.
- **TIME11 · Specialized equivalence.** A point lookup, interval query, or specialized temporal path agrees with the general resolver wherever they describe the same requested slice.
- **TIME12 · Temporal context.** At a fixed basis and statement context, time-zone conversion, precision changes, and temporal defaults give the declared logical times across supported frontends.

## 2. Values, types, and columnar representation

Sources include [type design](types.md), the [foundation crate map](rtdb-crates-and-test-parity.md#foundation-and-persistence), and the [expression reference](../../rust/crates/expr/README.adoc).

- **VAL01 · Identity encoding.** Every supported logical ID has the specified stable IID encoding across storage formats, frontends, and JVM and Rust implementations.
- **VAL02 · Name representation.** Logical database, schema, table, and column identifiers preserve the reference case, qualification, and escaping rules at their storage and binding boundaries.
- **VAL03 · Type join.** The documented type join is total, associative, commutative, and idempotent over its stated domain, with the declared bottom and null behavior.
- **VAL04 · Sparse schema.** Missing fields, null values, bottom, and absent columns contribute to merged schemas according to the documented rules for their contexts.
- **VAL05 · Promotion and casts.** The scalar promotion and conversion rules preserve declared widths, rounding, overflow, precision, scale, and error behavior.
- **VAL06 · Equality, hashing, and ordering.** Operators use the equivalence, identity, hash, or ordering relation specified for their context, including nulls, heterogeneous values, NaNs, and signed zero.
- **VAL07 · Extension types.** Supported UUID, keyword, temporal, decimal, interval, and nested values retain their logical meaning across Arrow and frontend representations.
- **VAL08 · Physical encoding.** Plain, run-end-encoded, and supported union layouts admit the same logical rows, column types, and select or copy behavior.
- **VAL09 · Batch schema.** Every emitted batch conforms to the result schema, including nullability, nested fields, union legs, and decimal parameters.
- **VAL10 · Row and vector agreement.** Scalar evaluation, vector kernels, and supported expressions over alternate physical encodings agree on observable values and typed errors.

## 3. Binding, planning, and relational execution

Sources include the [query lifecycle](high-level-tour.adoc), [SQL reference](../../rust/crates/sql/README.adoc), [XTQL reference](../../rust/crates/xtql/README.adoc), and [query execution reference](../../rust/crates/query/README.adoc).

- **QRY01 · Binding.** SQL and XTQL resolve names, scopes, aliases, and parameters against the catalog and context specified for the statement.
- **QRY02 · Lowering.** Supported SQL and XTQL lower to relational operators with the same observable meaning as the source statement.
- **QRY03 · Plan reuse.** A prepared or cached plan is reused only within the schema, parameter, authorization, and context conditions for which it was bound.
- **QRY04 · Rewrite soundness.** Every enabled rewrite preserves output multiplicity, schema, required order, and specified errors under its stated preconditions.
- **QRY05 · Predicate placement.** Moving a predicate preserves three-valued logic, nullable outer-join sides, duplicate multiplicity, and the effect of statement inputs.
- **QRY06 · Correlation.** Decorrelation preserves inner scope, outer row dependencies, empty-result behavior, and scalar subquery cardinality errors.
- **QRY07 · Projection.** Scan and operator projection retain every dependency required by predicates, grouping, ranking, ordering, expressions, and metadata.
- **QRY08 · Join and set semantics.** Alternate join implementations and set operators preserve the documented treatment of nulls, duplicate rows, and unmatched inputs.
- **QRY09 · Aggregation.** Grouping, DISTINCT, FILTER, empty inputs, ordered-set aggregation, accumulator types, and result errors agree with the applicable reference semantics.
- **QRY10 · Ordering and windows.** Sort, window partitions and frames, and limit or offset observe the documented comparison, null ordering, and tie rules.
- **QRY11 · Evaluation context.** Optimizations preserve the scope and required evaluation behavior of statement clocks, volatile functions, coercions, and expressions that may raise errors.
- **QRY12 · Streaming equivalence.** Changing batch boundaries, selection layout, legal operator partitioning, or spill mode preserves the observable result under the declared ordering and comparison rules.
- **QRY13 · Query framing.** Cancellation or evaluation failure produces the terminal outcome allowed for the stream, and already emitted rows come from its own captured query execution.
- **QRY14 · Rewrite termination (Candidate).** For each rewrite pass that claims a fixpoint, a well-founded measure or explicit work limit guarantees termination and exposes exhausted work.

## 4. Scans, pruning, and distributed reads

Sources include the [Rust scan and replay design](xtdb-rust-rewrite.md#4-query-and-temporal-execution), [query reference](../../rust/crates/query/README.adoc), [trie catalog](trie-cat.allium), and [multi read tiers](../../allium/rtdb-multi.allium).

- **SCAN01 · Source coverage.** A snapshot scan includes the live and persisted sources needed to reconstruct the requested history, with correctly ordered event input for each IID.
- **SCAN02 · Pruning is conservative.** Removing files, pages, or IID prefixes using metadata preserves the exact result of a scan without pruning.
- **SCAN03 · Historical dependency coverage.** Candidate selection retains all events needed to resolve corrections, deletes, erases, and visibility for candidate rows.
- **SCAN04 · Optional metadata.** Missing, unsupported, or inapplicable optional statistics leave the relevant data eligible for exact evaluation.
- **SCAN05 · Residual filters.** Pruned and accelerated scans apply the complete predicate to the resolved rows with the specified SQL truth semantics.
- **SCAN06 · Scan metadata agreement.** Page bounds, bloom or equivalent summaries, null counts, and recency summaries conservatively describe the data that readers actually decode.
- **SCAN07 · Catalog agreement.** A scan uses the catalog, storage generation, and in-memory state associated with its captured snapshot.
- **SCAN08 · Read tier equivalence.** Holder, log-fed replica, and checkpoint-only reads at a mutually supported basis return the same authorized logical result.
- **SCAN09 · Fragment equivalence.** A distributed scan and its local fallback cover the same requested partitions exactly once at one basis, with the required projection and residual checks.
- **SCAN10 · Read guarantees.** Each advertised read mode enforces its own basis, freshness, and fencing requirements or returns its documented error or fallback.
- **SCAN11 · Per-partition bases.** A multi-partition query applies each partition's own captured or requested cut and interprets absent bounds according to the snapshot-token contract.

## 5. Sessions and transaction boundaries

Sources include the [session transition specification](tx.allium), [pgwire contract](../../rust/crates/pgwire/README.adoc), and [Flight SQL contract](../../rust/crates/flight-sql/README.adoc).

- **SESS01 · Legal states.** Every frontend reaches only supported pending, reading, writing, failed, and closed session states through legal transitions.
- **SESS02 · Access mode.** The first resolving statement selects the appropriate mode, and later operations respect the mode and the documented rejection rules.
- **SESS03 · Pinned read basis.** The first query of a reading transaction captures its basis, and further reads use that basis through transaction end.
- **SESS04 · Await and snapshot tokens.** Await tokens impose their declared applied lower bounds, and explicit snapshot bases enforce their declared visibility limits.
- **SESS05 · Read your writes.** A supported read after a synchronous commit or an awaited submission observes the required transaction at its permitted basis.
- **SESS06 · Buffer ownership.** A writing session owns its buffered DML and commit options until commit, rollback, reset, failure handling, or close makes the documented transition.
- **SESS07 · Rollback and rejection.** Rolled-back or rejected work has the documented effect on the buffer, session state, and externally visible database state.
- **SESS08 · Frontend parity.** SQL control commands and programmatic or protocol transaction controls produce their documented outcomes, basis changes, and error states.
- **SESS09 · Transaction handles.** Flight SQL handles authorize access for the intended session, renew and expire under their contract, and release resources with the documented terminal outcome.
- **SESS10 · Sync and async receipts.** Each acknowledgment exposes exactly the submission, commit, or application guarantee documented for that mode and deployment configuration.

## 6. Transaction execution, live state, and clocks

Sources include the [live index specification](../../allium/live-index.allium), [database processing specification](db.allium), and [transaction and replication crate map](rtdb-crates-and-test-parity.md#transactions-replication-and-maintenance).

- **TX01 · All-or-nothing effects.** Each resolved transaction contributes either its complete permitted effect or its defined abort outcome to the committed history.
- **TX02 · Ordered resolution.** Each resolver sees the correct committed state and its own ordered staged predecessors for the transaction's position.
- **TX03 · Staged isolation.** Staged writes remain private to their owning resolver until an admitted durable apply publishes them.
- **TX04 · Static DML equivalence.** Eagerly expanded DML and the corresponding reference DML produce equivalent effects, errors, and metadata at the specified transaction context.
- **TX05 · Ordered apply.** Admitted replica records apply at their defined positions, and leader and follower imports of the same resolved prefix produce equivalent state.
- **TX06 · Read-back agreement.** A leader matches its admitted local read-back to the correct staged transaction, disposes of speculative work at term end, and resolves pending callers.
- **TX07 · Atomic snapshot publication.** A captured partition generation contains mutually consistent catalog, schema, live rows, and progress, and retains that view until release.
- **TX08 · Progress monitor.** Awaiters see their requested applied progress or a defined terminal outcome, and cancelling one waiter preserves unrelated waits.
- **TX09 · Transaction clocks.** Native and externally sourced transaction times follow their respective ordering, override, and monotonicity contracts.
- **TX10 · Statement semantics at commit.** Data-dependent writes and assertions use the required transaction basis and return the documented commit or abort result.
- **TX11 · Known outcome.** The reported transaction result and transaction metadata agree with the committed or aborted record that backs them.
- **TX12 · Strict serializability (Candidate).** For an explicitly selected synchronous transaction surface and fresh-read mode, completed histories admit a legal sequential interpretation that preserves real-time precedence.

## 7. Logs, leadership, and block adoption

Sources include [classic log and election contracts](../../allium/log-processor-lifecycle.allium), the [database processing model](db.allium), and [Kafka-backed logs](../../docs/src/content/docs/ops/config/log/kafka.md).
LOG01–LOG10 primarily describe classic mode, and shared contracts need evidence for each implementation.
The separate [multi-mode recovery inventory](rtdb-recovery-properties.md) covers quorum recovery, and the [multi log service](../../allium/rtdb-multi.allium) supplies the remaining journal contracts below.

- **LOG01 · One durable order.** A valid durable log exposes the same total order to every reader, and a successful append returns the position of its own record.
- **LOG02 · Read progress.** A prepared read advances only on delivery, delivers in order, and reports a reached tip according to its contract.
- **LOG03 · Retention and durability assumptions.** Accepted records remain recoverable above the legal boundary given the configured broker, replication, and retention assumptions.
- **LOG04 · Claim admission.** Classic claims follow observation of the admitted log, rise above observed terms, and confer leadership at the defined claim position.
- **LOG05 · Term fencing.** At most one eligible term is confirmed, and stale claims, duplicates, gaps, and legacy sequence fields have their specified admission outcomes.
- **LOG06 · Role transitions.** Promotion, refusal, demotion, and shutdown preserve the admitted progress and leave running tasks and waiters in their documented states.
- **LOG07 · Block ownership.** A pending block and held records have a continuous owner across role changes until the matching adoption or defined terminal outcome.
- **LOG08 · Publication and adoption.** A completed block is adopted only with its matching identity, catalog, and boundary, and recovery resumes from the matching retained log position.
- **LOG09 · Follower convergence.** Followers at the same admitted replica position have equivalent logical state and recoverable catalog boundaries.
- **LOG10 · Failure reporting.** A missing required range, fatal decode error, or invalid admission returns the declared failure rather than silently advancing applied progress.
- **LOG11 · Stream separation.** Shared journals preserve deployment and stream identity through group commit, subscription, repair, and physical segment compaction.
- **LOG12 · Retained-history audit.** For the same committed interval and stream identity, online and offline verification agree on the defined digest, and a strict audit reports insufficient evidence when coverage is below quorum.
- **LOG13 · Terminal stream retirement.** Authorized retirement remains terminal through restart and journal compaction, and its completion accounts for every required replica.

## 8. Memory trie and live indexing

Sources include the [memory hash-trie specification](../../allium/memory-hash-trie.allium) and [live index specification](../../allium/live-index.allium).

- **IDX01 · Exact lookup.** Lookup by IID returns every index for that IID and excludes indices belonging to any other IID.
- **IDX02 · Total coverage.** Leaf traversal covers every admitted row index with the multiplicity and order declared by the trie contract.
- **IDX03 · Leaf order.** A leaf sorts by IID ascending and row index descending within each IID.
- **IDX04 · Bucket routing.** A row appears only in the subtree selected by its stored IID at each depth, including collisions and maximum-depth cases.
- **IDX05 · Immutable ancestors.** Adding rows, splitting a leaf, or compacting insertion logs leaves every previously published version valid.
- **IDX06 · Bulk insertion equivalence.** Batch insertion, range insertion, and their single-row counterpart give equivalent ordered lookup and traversal for the same admitted indices.
- **IDX07 · Structural rewrite equivalence.** Log compaction, merging, and branching preserve the trie contents and established order.
- **IDX08 · Snapshot lifetime.** Each retained trie can still resolve its indices against the associated row data after later transactions, aborts, and block turnover.
- **IDX09 · Serialized correspondence.** The serialized trie and live trie select equivalent IID ranges and rows under the supported storage conversion.

## 9. Persistent formats and compatibility

Sources include the [format crate reference](../../rust/crates/format/README.adoc), [format migration contract](xtdb-rust-rewrite.md#3-read-and-write-the-existing-formats), and [database catalog contracts](db.allium).

- **FMT01 · Codec round trips.** Supported block, table, trie, source-log, and replica-log formats decode to their specified logical contents.
- **FMT02 · Optional field presence.** Present zero, absent, and other supported optional values retain their distinct protocol meanings after encode and decode.
- **FMT03 · Arrow event fidelity.** Stored Arrow pages preserve exact logical values, row order, event kind, IID, and temporal data across supported physical layouts and compression.
- **FMT04 · Version dispatch.** Readers select the correct schema, codec, and object namespace for every supported storage or log version and reject unsupported required versions.
- **FMT05 · Errors and referenced payloads.** Legacy headers, structured errors, and offloaded payload references preserve their declared contents, identity, and replay order.
- **FMT06 · Key and namespace fidelity.** Keys encode and decode to the same intended database, table, generation, and object across supported local and remote providers.
- **FMT07 · Stable table identities.** Table OIDs and escaped slugs are established consistently across independent nodes replaying the same catalog history.
- **FMT08 · Canonical encoding.** For a format that specifies canonical bytes, encoding the same logical payload with the same version and options gives that byte sequence.
- **FMT09 · Safe format errors.** Truncated, malformed, mismatched, or unsupported input is rejected before it becomes usable catalog, stream, or query state.

## 10. Compaction and trie catalog transitions

Sources include the [compaction specification](compaction.allium), [trie catalog specification](trie-cat.allium), [recency cell design](recency-cells.md), and [maintenance crate reference](../../rust/crates/maintenance/README.adoc).

- **COMP01 · Job selection.** Equivalent eligible trie catalogs produce the same job definitions and input order.
- **COMP02 · Merge equivalence.** Querying the compacted output gives the same logical result and projected temporal bounds as querying its inputs at every supported basis.
- **COMP03 · Identity and partitioning.** Output partitioning and event ordering cover the input history according to its IID, recency, and historical range rules.
- **COMP04 · Metadata fidelity.** Compaction preserves type parameters and emits conservative page statistics over the actual output representation.
- **COMP05 · Recency agreement.** Computed recency and historical cell boundaries preserve scan correctness and obey the layout rules for mixed inputs, disabled settings, and independent readers.
- **COMP06 · Output determinism.** A logical job produces the same immutable output keys and bytes across retries and equivalent workers where the storage format requires them.
- **COMP07 · Catalog convergence.** Nodes processing the same admitted trie notifications converge on equivalent catalog states, including duplicate or out-of-date notifications under their documented rules.
- **COMP08 · Complete replacement.** An incomplete partition group does not supersede live input coverage ahead of the catalog's completion rule.
- **COMP09 · Safe adoption.** A result becomes visible only for the eligible catalog, generation, and input coverage it was built against.
- **COMP10 · Safe intermediate states.** Query results remain equivalent during allowed interleavings of independent compaction jobs, partial groups, publication, and follower adoption.
- **COMP11 · Retry and scheduling.** Stale, cancelled, or interrupted compaction work leaves inputs available until a valid alternate output is adopted, and eligible work can resume under the scheduler's availability assumptions.
- **COMP12 · Recency writer bound.** A writer of recency cells buffers at most the documented page allowance per cell plus the shared spare page.

## 11. Garbage collection, erasure, and maintenance

Sources include [trie GC](gc.allium), [block GC](block-gc.allium), [offline maintenance contracts](db.allium), [multi erasure rules](../../allium/rtdb-multi.allium), and the [bounded S3 erasure study](rtdb-recovery-properties.md#s3-erasure-ordering).

- **GC01 · Reachability.** Collection preserves objects and payloads reachable through every catalog, snapshot, or retained history covered by its retention contract.
- **GC02 · Maintenance authority.** Only the leader, reaper, or authorized maintenance actor for a cleanup class can perform that class of durable deletions and catalog updates.
- **GC03 · Trie deletion order.** Trie cleanup follows its prescribed order of physical deletion and replica-log publication, preserving live file references for the covered readers.
- **GC04 · Trie cleanup convergence.** Retrying an interrupted trie cleanup is safe, and followers applying its admitted record reach equivalent catalogs.
- **GC05 · Block retention.** Block GC preserves the current database and per-table checkpoint files and the retained startup or catch-up set required by policy.
- **GC06 · Referenced replica payloads.** Offloaded payloads remain readable for the supported lag window, and an unavailable required payload prevents silent advancement.
- **GC07 · Snapshot export.** A completed export identifies the selected recoverable block and copies its required objects consistently across retries.
- **GC08 · Compactor reset.** Given the required exclusive window, reset adopts the selected replacement catalog before deletion and preserves recoverability at interruption points covered by the contract.
- **GC09 · Logical erase and physical disposal.** The erase visibility guarantee and the separate physical deletion or key-destruction guarantees hold at their documented completion points.
- **GC10 · Receipt completeness.** A successful disposal receipt covers the recorded installation, namespaces, archives, replica journals, and key disposition within its declared inventory and provider assumptions.

## 12. Database membership, multi-cell placement, and lineage

Sources include the [node membership specification](../../allium/database-lifecycle.allium), [multi-mode specification](../../allium/rtdb-multi.allium), [implementation plan](rtdb-multi-plan.md), and [transfer model and observations](rtdb-recovery-properties.md#transfer-protocol-study).

- **CELL01 · Membership uniqueness.** A database name resolves to at most one held entry per node, and configured, skipped, running, and tearing-down entries obey their membership rules.
- **CELL02 · Attach and detach.** Admission, repeat instructions, durable membership replacement, teardown, and primary-database restrictions preserve the documented name-resolution state.
- **CELL03 · Placement uniqueness.** Each deployment resolves through one authoritative cell mapping and has at most one claimant for a writer epoch.
- **CELL04 · Epoch monotonicity.** Conditional directory updates preserve deployment identity and monotonic epochs across claims, release, drain, moves, and retries.
- **CELL05 · Watch consistency.** Directory watches may lag, and every delivered revision preserves the documented monotonic view and identity.
- **CELL06 · Fault detection independence.** A mistaken lease expiry or suspicion may change placement while durable write safety still holds under the protocol's fencing assumptions.
- **CELL07 · Isolation and engine selection.** Quarantined, placed, pinned, and draining deployments are assigned only to eligible members for their lifecycle, pool, and engine version.
- **CELL08 · Planned handoff.** Release and reacquisition preserve the admitted write outcomes and the supported retry semantics while ownership changes.
- **CELL09 · Cold activation.** Parking and demand-driven reactivation preserve durable state, and activation responses obey the documented readiness, pressure, and retry outcomes.
- **CELL10 · Cross-cell cutover.** A move publishes the destination route only when its required state is ready and preserves source custody until authorized cleanup.
- **CELL11 · Move retries.** Restarting a move resumes the recorded operation without switching installation identity, double-publishing a destination, or disposing of a current residence.
- **CELL12 · Fork isolation.** A fork exposes the allowed parent history at its fork basis, and subsequent writes to parent and child are independently resolved.
- **CELL13 · Lineage reachability.** Objects retained through shared lineage remain available to entitled readers across a concurrent fork, transfer, or deletion under the stated assumptions.
- **CELL14 · Directory format equivalence.** Supported directory record formats preserve placement identity, membership, epoch, and lookup results during mixed-version reads and controlled upgrade.
- **CELL15 · Static stability.** Under the documented durable-dependency and capacity assumptions, existing holders retain their allowed read and commit behavior during a directory outage, and directory-dependent changes wait.

## 13. Object storage, caches, and hosted recovery

Sources include the [storage publication contracts](../../rust/crates/storage/README.adoc), [S3](../../rust/crates/s3/README.adoc), [Azure](../../rust/crates/azure/README.adoc), and [GCS](../../rust/crates/gcs/README.adoc) adapters, the [multi object-storage contract](../../allium/rtdb-multi.allium), and the [archive restore record](rtdb-recovery-properties.md#archive-restore-and-physical-replay).

- **STORE01 · Provider refinement.** Provider operations implement the supported storage contract for range reads, immutable puts, conditional publication, inventory, errors, and retry semantics.
- **STORE02 · Range and cache agreement.** A cached or fresh range returns exactly the requested bytes of the same immutable object and generation.
- **STORE03 · Read lifetime.** A lease pins its immutable content for the supported read lifetime, and eviction can reclaim it at the documented release point.
- **STORE04 · Immutable publication.** A reader sees a complete authorized object or its documented unavailable state through upload, conflict, retry, and interruption.
- **STORE05 · Physical identity.** Publication, restore, move, and disposal preserve the recorded provider, root, namespace, generation, and object identity requirements.
- **STORE06 · Writer authorization.** Epoch and deployment constraints govern object publication, and held credentials have the documented permissions and expiry.
- **STORE07 · Publication visibility.** A reader reaches only the adopted, referenced objects for its basis, and completing unrelated or stale uploads does not extend that basis.
- **STORE08 · Multipart confirmation.** Ambiguous completion and retry produce the recorded outcome only when the required evidence confirms the intended object and generation.
- **STORE09 · Archive completeness.** An accepted archive corresponds to its captured checkpoint and remains sufficient for restore of that declared basis under the storage and key assumptions.
- **STORE10 · Restore equivalence.** Restoring an archive plus the required admitted suffix produces the same authorized logical history through the selected basis.
- **STORE11 · Discovery contract.** The checkpoint path locates required objects using its defined discovery strategy and treats a genuine missing dependency according to the published recovery contract.
- **STORE12 · Key custody.** A deployment's object and log data use the required key binding across read, restart, move, and authorized destruction, including the specified terminal state after key retirement.

## 14. External sources and CDC

Sources include the [PostgreSQL source specification](pgsrc.allium), [external database processing](db.allium), [PostgreSQL adapter](../../rust/crates/pg-source/README.adoc), [Kafka Connect reference](../../docs/src/content/docs/ops/external-sources/kafka-connect/reference.md), and [Schema Registry reference handling](rust-kafka-reference-resolution.adoc).

- **CDC01 · Source authority.** Data mutation on an externally sourced database is restricted to its configured source under the documented authorization and placement rules.
- **CDC02 · Transaction boundaries.** A supported upstream transaction becomes one ordered downstream logical import with its specified start, commit, abort, and metadata semantics.
- **CDC03 · Token correspondence.** Each upstream position is associated with the correct durably applied downstream prefix under the adapter's transaction and partition semantics.
- **CDC04 · Safe acknowledgment.** A source position is confirmed upstream only when its required downstream effects are durably recoverable.
- **CDC05 · Single confirmer.** Only the current eligible source owner advances confirmation, and confirmations do not move backward.
- **CDC06 · Reopen.** Reopening resumes from the recorded durable extent or takes the documented resnapshot path when that extent is unavailable.
- **CDC07 · Replayed delivery.** Redelivery, reconnect, and supported partition reassignment preserve the adapter's documented effect and retained progress.
- **CDC08 · Snapshot boundary.** Bootstrap and subsequent streaming together cover the supported source history at the declared cut.
- **CDC09 · Mapping fidelity.** Supported schema evolution, conversions, transforms, keys, tombstones, and coordinate metadata produce the documented typed records and outcomes.
- **CDC10 · Schema resolution.** Schema identity, configured reader versions, and references resolve to the intended definitions, with invalid or unsupported definitions yielding their declared outcomes.
- **CDC11 · Reject and retry outcomes.** Conversion, import, and transport failures follow the configured refusal, retry, or dead-letter behavior within the documented progress contract.
- **CDC12 · Ingestion backpressure (Candidate).** Admitted source work has explicit bounds on queued records and bytes, with progress or a documented error under sustained eligible capacity.

## 15. Authorization, protocols, and foreign interfaces

Sources include the [authentication crate](../../rust/crates/auth/README.adoc), [multi client and storage access](../../allium/rtdb-multi.allium), [pgwire](../../rust/crates/pgwire/README.adoc), [Flight SQL](../../rust/crates/flight-sql/README.adoc), and [native ABI](../../rust/crates/ffi/README.adoc).

- **API01 · Authentication binding.** Each admitted request is bound to the authenticated identity and its actual session or connection.
- **API02 · Authorization coverage.** Query, DML, catalogs, metadata, administrative actions, and external adapter paths enforce the permissions specified for their identities and targets.
- **API03 · Deployment isolation.** A multi-mode session can resolve only its deployment and explicitly granted resources across routing, snapshots, fragments, and reconnects.
- **API04 · Capability scope.** A gateway or control capability authorizes only its declared subject, deployment, and operations for its defined validity window.
- **API05 · Pgwire state machine.** Simple, extended, and COPY exchanges obey their framing, per-command completion, error recovery, cancel, and ReadyForQuery contracts.
- **API06 · Parameters and results.** Supported pgwire, Flight SQL, ADBC, and embedded transports preserve their declared parameter positions, nullability, value encodings, metadata, and error mapping.
- **API07 · Prepared handles and portals.** Prepared statements, portals, tickets, and handles remain scoped to their owner with the specified bind, execute, close, suspension, expiration, and recovery behavior.
- **API08 · Reply attribution.** Every reply is associated with its own command or pipeline cycle, including reconnects and replay of supported session state.
- **API09 · Streaming ingestion.** COPY and supported Flight or ADBC ingestion obey their declared transaction boundaries, resource limits, and cancellation outcomes.
- **API10 · Handle lifetime.** Retained native handles and Arrow buffers remain valid for the specified consumer lifetime, including detached children, and release their owners exactly as contracted.
- **API11 · Cancellation isolation.** Cancel, disconnect, expiration, and close release the affected work without invalidating unrelated sessions, requests, or retained results.
- **API12 · Authentication refresh.** Revalidation and expiration enforce the applicable rules for continuing or rejecting requests without mixing identities or sessions.
- **API13 · Cross-deployment noninterference (Candidate).** At fixed identities, grants, bases, and statement context, authorized data observations in one deployment are invariant under unrelated work in another deployment.
- **API14 · Bounded gateway replay.** The first replayable failure sets the pending command's cutoff, and every replacement attempt shares it until the gateway takes a reply for forwarding to the client.
  Expiration preserves the command's unknown outcome and ends gateway recovery.

## 16. Resource accounting and operational progress

Sources include the [query resource notes](../../rust/crates/query/README.adoc), [server lifecycle](../../rust/crates/server/README.adoc), [Kafka resource profiles](rust-kafka-native-account.adoc), and [multi admission and progress rules](../../allium/rtdb-multi.allium).

- **RES01 · Reservation ownership.** Each charge against a declared shared budget belongs to its retained resource until released or transferred exactly once.
- **RES02 · Budget enforcement.** Within the scope of each configured resource budget, admitted work respects the accounting limit and receives the defined outcome on exhaustion.
- **RES03 · Error and cancel cleanup.** Completion, early error, cancellation, and shutdown release scoped tasks, memory, streams, temporary files, and storage leases according to ownership.
- **RES04 · Queue and frame admission.** Where a transport, worker, or log declares a count or byte limit, pending work respects the limit across success, interruption, and retry.
- **RES05 · Scoped shutdown.** Shutdown joins the required workers and resolves outstanding callers or observers under the runtime and session contracts.
- **RES06 · Health and readiness.** Exposed status and metrics reflect the subsystem or deployment states they claim to observe, including readiness, faults, and progress.
- **RES07 · Placement demand.** Directory lookup and demand reporting preserve the documented placement and storage views when parked and active deployments coexist.
- **RES08 · Physical memory envelope (Candidate).** A specified workload envelope bounds actual process memory, including allocations outside the scoped execution and cache budgets.
- **RES09 · Cancellation work bound (Candidate).** Each supported cooperative execution path has an explicit bound on the work it can perform between cancellation checks.
- **RES10 · Foreground progress (Candidate).** Under defined fairness, demand, and capacity assumptions, background warming, maintenance, and cold opens leave progress for admitted foreground work.
- **RES11 · Idle work.** An idle deployment incurs only the storage, bookkeeping, and protocol work allowed in its residency state, and the log performs no periodic per-deployment writes solely for idleness.
- **RES12 · Placement work.** A holder-index placement pass visits the documented candidate set and verifies a candidate's current state before it affects placement.

## 17. Bitemporal full-text search

These targets refer to the separate published search specification and its implementation map on `main`. [Search specification](https://github.com/bijectionhq/rtdb/blob/1c8d111ef/dev/doc/fts.allium) · [Implementation and test oracle](https://github.com/bijectionhq/rtdb/blob/1c8d111ef/dev/doc/fts-implementation.adoc)

- **FTS01 · Snapshot-scoped definitions.** Search uses the index definition, versions, and artifact coverage visible at its query basis.
- **FTS02 · Corpus visibility.** The search corpus and scoring statistics represent exactly the rows visible for the requested valid and system times.
- **FTS03 · Analyzer agreement.** Indexed tokens and residual matching use the same analysis and preserve the documented handling of nulls and missing values.
- **FTS04 · Visibility rectangles.** The indexed rectangles for each text run represent the applicable versions and visibility intervals from the temporal resolver.
- **FTS05 · Candidate completeness.** Every row that may satisfy the resolved predicate is included in the accelerated candidate set.
- **FTS06 · Residual agreement.** Accelerated and unaccelerated evaluation apply the same full predicate to the same resolved rows.
- **FTS07 · Ranking equivalence.** Ranked pruning or early termination preserves the score order and top results under the query's declared tie semantics.
- **FTS08 · Artifact independence.** Missing, retired, unreadable, partial, or disabled artifact coverage preserves the observable query result through the supported fallback.
- **FTS09 · Artifact lifecycle.** Building, sharing, publishing, repairing, and reclaiming artifacts preserve deterministic identity and their snapshot-bound reachability contracts.

## 18. Compatibility, upgrade, and rollback

Sources include the [migration and rollback contract](xtdb-rust-rewrite.md#9-production-cutover-and-rollback), [parity inventory](rtdb-crates-and-test-parity.md#what-complete-test-parity-means), and [release matrix](../../rust/release/README.adoc).

- **COMPAT01 · Observational parity.** Supported Rust and JVM executions agree at the same admitted history and basis on typed rows, advertised schema, errors, and transaction effects.
- **COMPAT02 · Cross-version recovery.** Each supported writer-reader combination reopens the other's checkpoint and retained tail under the version matrix.
- **COMPAT03 · Exclusive handoff.** Upgrade or rollback preserves acknowledged history and authority across the documented cutover boundary.
- **COMPAT04 · Mixed-version outputs.** Any two versions allowed to complete the same pending immutable job obey its shared key, format, and byte-identity contract.
- **COMPAT05 · Release configuration.** A qualified configuration maps to the intended adapter and feature behavior for its released artifact and rejects unsupported combinations with their declared outcomes.
- **COMPAT06 · Evidence provenance.** A parity or release result can be traced to its exact source revision, artifact, topology, case selection, and observed outcome.

## Where to start

1. **Bitemporal history and scan soundness.** Build a small executable reference for TIME04–TIME11, then use it to check SCAN01–SCAN06 with pruning enabled and disabled.
2. **Compaction and collection.** Use the same reference for COMP02 and COMP08–COMP10, and model the separate reachability and publication obligations in GC01–GC06.
3. **Transaction visibility and leadership.** Check the state transitions for TX01–TX08 and LOG04–LOG09, then connect their implementation evidence to the existing recovery study.
4. **Isolation and source progress.** Model the authorization and identity boundaries for API02–API04, CELL03–CELL06, and CDC03–CDC07.
5. **Local implementation contracts.** Apply Lean to pure equivalence lemmas, Verus to suitable Rust components, and TLA+ to concurrent state machines as the relevant preconditions become precise.

Keep bounded model checks, abstract theorems, implementation refinement, and observed integration results distinct when recording evidence for these targets.
Treat provider behavior, fairness, supported formats, and configured resource limits as explicit assumptions of the corresponding claim.
Measure latency and physical memory against stated workload envelopes.
