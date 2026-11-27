# RTDB and upstream XTDB: deliberate design divergences

This document describes deliberate design differences between RTDB (`rust/crates/*`) and upstream XTDB at `0b9fa440f2`.
Classic assigns strictly increasing system times as XTDB does, advancing the previous time by one microsecond when the automatic time stalls and rejecting supplied times at or before the head.

Path abbreviations:

- `RC/` is `rust/crates/`.
- `UK/` is `core/src/main/kotlin/xtdb/`.
- `UC/` is `core/src/main/clojure/xtdb/`.
- `XM/` is `modules/`.

Each entry records what differs, why RTDB diverges, what a user or operator sees, and where the behaviour lives in the code.

---

## 1. Storage, log, and wire-format compatibility

### 1.1 Loud refusal framing for RTDB-only log records and block files

**What differs.**
Upstream XTDB decodes source records with header bytes `0xff`, `2`, and `3`, ignores the replica header byte, and parses `Block` and `TableBlock` protobuf files directly.
RTDB writes identical records and files for plain XTDB workloads, but frames any source record, replica record, or block file that uses an RTDB-only feature so that an upstream XTDB node refuses it immediately.
Source records using RTDB-only features take header byte `4`.
Replica records and `Block`/`TableBlock` files using RTDB-only features start their protobuf payload with a leading zero byte.
Every field RTDB adds to a shared XTDB protobuf message is numbered from `1000` upwards.
Four integrity annotations (`BlockUploaded.catalog_sha256`, `TrieDetails.published_files`, `OversizedMessage.payload_sha256`, and `Block.table_block_manifest`) remain unframed because XTDB loses nothing by ignoring them.

**Why.**
Silently skipping an RTDB-only record or reading a block without its RTDB-only semantics would let an XTDB follower diverge or apply duplicate writes.
Numbering extension fields from `1000` prevents field-number collisions with future upstream XTDB releases.

**What a user sees.**
An XTDB node can follow an RTDB log and open an RTDB store while only standard XTDB features are used.
Once a log or store records an RTDB-only feature, an XTDB node stops loudly with `IllegalArgumentException: Unknown message type: 4` on the source log or `InvalidProtocolBufferException: Protocol message contained an invalid tag (zero)` on the replica log or a block file.

**Where in the code.**
- `RC/format/src/envelope/extension.rs` (`source_uses_extension`, `replica_uses_extension`, `block_uses_extension`, `table_block_uses_extension`)
- `RC/format/src/envelope/log_headers.rs`
- `RC/format/tests/xtdb_refusal.rs`
- `core/src/main/proto/xtdb/log/proto/log.proto`
- `core/src/main/proto/xtdb/meta/block/proto/block.proto`
- `docs/src/content/docs/ops/config/log.md`

### 1.2 Trie key naming epochs after compactor reset or primary recovery

**What differs.**
Upstream `Trie.parseKey` accepts only standard level/recency/part/block segments.
RTDB appends `-e<lexhex>` to compacted trie keys whenever the catalog's naming epoch is greater than zero, and records `naming_epoch` on `Block` and `BlockUploaded`.
All records and blocks naming an epoch greater than zero are framed for XTDB refusal (§1.1).

**Why.**
Online compactor reset and recovery from a primary need newly compacted tries to avoid colliding with pre-reset object keys in immutable object storage.

**What a user sees.**
Plain workloads stay at epoch zero and remain XTDB-compatible.
After an online compactor reset or primary recovery, compacted object keys carry `-e<epoch>` and an upstream XTDB node refuses to follow the log or open the store from that block onwards.

**Where in the code.**
- `RC/format/src/keys.rs` (`TrieKey`)
- `RC/index/src/trie_catalog.rs` (`reset_from_primary`)
- `RC/replication/src/blocks.rs`

### 1.3 Extended local-log frame separators, lineage header, startup scan, and append rollback

**What differs.**
Upstream `LocalLog` writes only `0x1E` frame separators, reads only the final trailer on startup (`readLatestSubmittedOffset`), and leaves partial bytes on append failure.
RTDB adds `0x1D` (retired), `0x1C` (sealed/encrypted), and `0x1B` (retired and sealed) separators, and prefixes journals under sealed erasure or native event history with a 24-byte `\x1aRTDBJ01`+UUID lineage header and `.lineage` sidecar.
On startup, `inspect` walks every frame to validate the custody profile and 2^48 offset bound and rejects a torn final frame.
On an append write error, RTDB truncates the partial batch and re-syncs the file.

**Why.**
Crypto-shredding erasure and lineage-bound native journals need per-frame retirement/sealing markers and identity verification.
Rolling back partial appends and validating frames on startup prevent torn tails from corrupting recovery.

**What a user sees.**
Plain local logs use `0x1E` frames and remain readable by XTDB, though startup time scales linearly with log size instead of O(1).
Sealed, retired, or lineage-headed local logs cannot be opened by upstream XTDB.

**Where in the code.**
- `RC/storage/src/log/local.rs` (`inspect`, `RECORD_SEPARATOR`, `RETIRED_SEPARATOR`, `SEALED_SEPARATOR`, `RETIRED_SEALED_SEPARATOR`)
- `RC/storage/src/log/journal_file.rs`
- `RC/node/src/database.rs`

### 1.4 Local-log write batching and Apple `F_FULLFSYNC` opt-out

**What differs.**
Upstream `LocalLog` uses a 10-element channel and drains up to 10 queued messages per `force(true)` call.
RTDB uses a dedicated blocking writer and a queue with a default capacity of 64.
The writer drains up to 10 queued records per batch and stops grouping at 256 KiB.
Plain inline tails can read recent frames in one file read.
On Apple platforms, RTDB calls `F_FULLFSYNC` (falling back to `fsync` on `ENOTSUP`/`EINVAL`/`ENOTTY`) and exposes `RTDB_APPLE_SYNC=fsync` to weaken sync for local development.

**Why.**
The writer owns the journal handle and keeps filesystem synchronization off the async executor.
The Apple opt-out allows local development with ordinary `fsync`.

**What a user sees.**
Setting `RTDB_APPLE_SYNC=fsync` on macOS weakens crash durability compared to `F_FULLFSYNC`.

**Where in the code.**
- `RC/storage/src/log/local.rs` (`LocalLogDriver::run`, `ready_batch`)
- `RC/node/src/resources.rs` (`log_queue_capacity`)
- `RC/storage/src/durability.rs` (`sync_file`)

### 1.5 Create-only object publication, directory fsync, S3 conditional writes, and publication metadata

**What differs.**
Upstream `LocalStorage` moves temp files into place with `ATOMIC_MOVE` without fsync or conflict checks, and upstream S3 uploads multipart objects in parallel directly to the final key.
RTDB's local store writes a tempfile, fsyncs both the file and every parent directory, publishes with `persist_noclobber`, and raises `ConflictingObject` if differing bytes already exist.
RTDB's remote store publishes via `PutMode::Create` with byte comparison on conflict, uploads files as sequential 5 MiB parts to `.tmp/upload-<uuid>` followed by `copy_if_not_exists` and delete, requires S3 `S3ConditionalPut::ETagMatch` and `S3CopyIfNotExists::Multipart`, and attaches `rtdb-publication`/`rtdb-retirement` metadata and zero-byte tombstones.

**Why.**
Create-only publication and parent-directory fsyncs enforce immutability and crash durability for every published trie and block object.

**What a user sees.**
Block flushes and compaction outputs perform extra fsyncs locally and extra round trips on remote stores.
S3-compatible object stores that do not support conditional `ETagMatch` puts or conditional multipart copy are rejected.

**Where in the code.**
- `RC/storage/src/object_store.rs`
- `RC/storage/src/remote.rs` (`put_opened_file`)
- `RC/s3/src/lib.rs`

### 1.6 Optional storage encryption, physical ("default") erasure, and KMS writer store

**What differs.**
Upstream XTDB stores objects and spill files in plaintext and implements `ERASE` logically by writing erase events that wait for GC.
RTDB adds opt-in AES-256-GCM object sealing (`RTDB_CLASSIC_KEY_VAULT`), opt-in crypto-shredding default erasure (`RTDB_DEFAULT_LOCAL_ERASURE`, `RTDB_DEFAULT_KAFKA_ERASURE`, sealed Kafka topics) that writes sealed successor roots and destroys key slots (while disabling compaction, artefacts, GC, and export), per-file AES-128-CTR encrypted query spill files under local erasure custody, and `rtdb-store`, a KMS-encrypted immutable writer store.

**Why.**
Regulated deployments require cryptographic deletion of erased data across logs, storage, and temporary query spill files.

**What a user sees.**
Plain nodes are unaffected.
Nodes configured with a key vault or default erasure write encrypted logs and objects that XTDB cannot read, encrypt query spill files in memory-keyed AES-128-CTR, and run without background compaction, artefacts, GC, or export under default erasure.

**Where in the code.**
- `RC/storage/src/sealing/`
- `RC/replication/src/default_erasure.rs`
- `RC/kafka/src/erasure.rs`
- `RC/query/src/spill.rs`
- `RC/query/src/context.rs`
- `RC/store/src/`

### 1.7 Disk cache layout, owner lock, and heap buffer pool

**What differs.**
For a remote store with an XTDB-compatible URI and no Classic sealing keys, RTDB uses the upstream `c1/<first 32 hex digits of SHA-256(uri)>/` cache namespace.
Other cacheable stores use the RTDB namespace `~rtdb-store-<sha256>/0/`, keyed by provider, storage identity, and epoch.
RTDB guards the cache with an exclusive `.tmp/.owner-lock`.
Like upstream XTDB, supported Classic S3, Azure, and GCS stores fill a cold entry with one streamed GET.
RTDB reserves a 3 MiB transport window before issuing the request.
Upstream memory-maps individual record-batch byte slices in `MemoryCache`.
RTDB's `BufferPool` retains raw objects in heap buffers and stores decoded views (metadata, layouts, decoded pages) under the same `CacheBudget` mutex.

**Why.**
An explicit owner lock prevents two nodes from corrupting a shared cache directory.
Sharing one `CacheBudget` across raw bytes and decoded views enforces RTDB's hard process memory ceiling without relying on JVM direct-memory mapping.

**What a user sees.**
Give each running node its own disk-cache directory.
When streaming is unavailable or the memory budget cannot admit the transport window, RTDB uses reserved reads, including range GETs of up to 1 MiB where supported.
RTDB publishes a streamed fill after validating the response length and a clean end of stream.

**Where in the code.**
- `RC/storage/src/cache.rs` (`BufferPool`, `CacheBudget`)
- `RC/storage/src/disk_cache.rs`
- `RC/storage/src/remote.rs`
- `RC/node/src/resources.rs`

### 1.8 RTDB-only bucket prefixes, catalog-ledger retirement, and receipt-table L0 deletion

**What differs.**
Upstream XTDB stores only blocks, table data/meta tries, and oversized payloads in the bucket, deletes eligible tries directly during GC, and never deletes L0 trie files.
RTDB adds bucket prefixes for RTDB-only subsystems (`~trie-retirements/`, `ordered-index/v2/`, `tables/<slug>/artefacts/`, `search-runs/`, `search-pending/`, term/entity directories, `tables/<slug>/spatial/`, `~rtdb-v1/`, `.tmp/catalog-ledger`, `.tmp/upload-*`).
On remote stores with catalog-ledger coordination, level >= 1 trie releases go through retirement intents and delete exact S3 object versions.
For RTDB's expiring receipt tables (`xt.idempotency_receipts`, `xt.bounded_retry_receipts`, `xt.rtdb_replay_keys`, and placed-attempt tables), compaction folds every L0 into L1 and GC deletes their L0 files (framed for XTDB refusal).

**Why.**
RTDB's secondary indexes, native v1 incarnations, and expiring receipt tables need dedicated storage namespaces and safe versioned deletion.

**What a user sees.**
An XTDB node running GC against a bucket that also holds RTDB-only prefixes would leak those objects.
Any `TriesDeleted` record that deletes an L0 receipt-table trie is framed so XTDB stops rather than misapplying it.

**Where in the code.**
- `RC/format/src/keys.rs`
- `RC/storage/src/remote_catalog.rs`
- `RC/maintenance/src/retirement_intent.rs`
- `RC/maintenance/src/job.rs`
- `RC/maintenance/src/gc.rs`

### 1.9 Page IID-order validation on decode and GC missing-object error code

**What differs.**
Upstream XTDB trusts its persisted trie data pages on read and surfaces the object store's missing-file exception when a query reads a catalog older than the GC window.
RTDB checks that row IIDs are ordered the first time a persisted data page is decoded.
When a read requests an object already removed by GC, RTDB reports `rtdb.storage/missing-object` (`NotFound`, mapped to `XX000` and logged in pgwire).

**Why.**
Validating page order on first decode catches corrupt or malformed files early before temporal resolution relies on sorted IIDs.
Typed storage errors give uniform diagnostics across local and remote stores.

**What a user sees.**
A corrupt out-of-order data page fails immediately on first load instead of producing wrong query results.
Reading past the GC retention window fails with `XX000` as in XTDB, but carries RTDB's `rtdb.storage/missing-object` code and message.

**Where in the code.**
- `RC/query/src/scan.rs`
- `RC/columnar/src/read_check.rs`
- `RC/storage/src/error.rs`

---

## 2. Transactions, system time, and replication

### 2.1 Native `rtdb.v1` protocol, Classic vs v1 partitions, and `rtdb.store.v1`

**What differs.**
Upstream XTDB provides one linear SQL/XTQL write and read model per database.
RTDB ships two additional native protocols over Flight actions in every build: `rtdb.v1` (conditional `Transact` with idempotency keys, capabilities, guards/requires, `ops`/`otherwise`, decision clocks, funded appointments, branches `Fork`/`Merge`/`Renew`/`DropBranch`, a `Changes` stream, and per-principal private owned relations/worlds/audiences) and `rtdb.store.v1` (ordered versioned key-value bytes with `Read`/`Append`/`Watch`, `Bound`, derived writes, and scoped grants).
Ordinary SQL writes land on a database's Classic partition; once a Classic transaction applies, that database's `rtdb.v1` branch route is unavailable.

**Why.**
RTDB serves Bijection's transactional, branching, multi-tenant, and ordered-store workloads alongside XTDB-compatible SQL.

**What a user sees.**
Plain XTDB SQL workloads do not need `rtdb.v1` or `rtdb.store.v1`, though `rtdb dev` and `rtdb standalone` enable `defaultV1Serving` and disallow mixing Classic SQL writes and v1 writes in the same database.
Any database that executes `rtdb.v1` writes produces framed records that upstream XTDB refuses (§1.1).

**Where in the code.**
- `rust/proto/rtdb.v1.proto`
- `rust/proto/rtdb.store.v1.proto`
- `RC/flight-sql/src/v1/`
- `RC/tx/src/decision_clock.rs`
- `RC/node/src/receiver_clock.rs`
- `RC/query/src/owned_relation.rs`
- `RC/index/src/branches.rs`
- `rust/README.adoc`

### 2.2 Offered clock slots for native writes

**What differs.**
A native write that consumes an offered clock slot waits for the local clock to reach the slot's reading, up to 1.5 seconds.
The replica appender's serving-leader clock lease and its promotion gate have been removed.
Multi's ordered journal has its own [writer clock lease](../dev/doc/ordered-bytes-store.adoc) for bounded-drift sampling across writer handoffs.

**Why.**
Funded native operations can name the offered clock reading before submitting the write that consumes it.

**What a user sees.**
A native submit using an offered clock slot can wait before its source-log append.

**Where in the code.**
- `RC/tx/src/system_time.rs` (`clock_slot_wait`, `clock_slot_reading`)
- `RC/node/src/node.rs`
- `RC/flight-sql/src/owned_actions.rs`
- `RC/db/src/multi.rs`

### 2.3 Idempotent and bounded-retry SQL submissions

**What differs.**
Upstream XTDB has no built-in idempotency key or bounded-retry guard on SQL `Tx` log messages.
RTDB adds `Tx` extension fields (`idempotency_guard`, `replay_submission`, `bounded_retry`, `placed_attempt_seed`) backed by private receipt tables and a 1 h idempotency window, framed for XTDB refusal.

**Why.**
At-least-once clients and placed deployments need server-enforced deduplication across network retries and leader failovers.

**What a user sees.**
Clients that do not attach RTDB retry guards see standard XTDB transaction behaviour.
Clients using guards get deduplicated replay receipts, and the resulting framed records cannot be replayed by XTDB.

**Where in the code.**
- `RC/tx/src/replay_submission.rs`
- `RC/tx/src/bounded_retry.rs`
- `RC/tx/src/idempotency_window.rs`
- `RC/tx/src/source.rs`

### 2.4 Sealed snapshot tokens for placed databases and world-overlay refusal

**What differs.**
Plain RTDB snapshot tokens now match upstream XTDB's stateless base64 protobuf `TimeBasis` and are interoperable in both directions.
However, when the default database has a placed-receipt codec, RTDB emits an `rtdb-sealed-snapshot-v4:` token encrypted and bound to the issuing principal, history fingerprint, and storage generation.
Issuing or redeeming a snapshot token over a view with RTDB world overlays fails with `rtdb/snapshot-token-overlay` (`Unsupported`, SQLSTATE `0A000`).

**Why.**
Placed multi-tenant deployments cannot expose raw database topology or let one principal redeem another principal's or another storage generation's token, and XTDB's `TimeBasis` cannot represent world overlays.

**What a user sees.**
Standard SQL snapshot tokens work identically to XTDB and redeem across XTDB and RTDB.
Placed-database tokens are opaque `rtdb-sealed-snapshot-v4:` strings rejected by XTDB as `xtdb.basis/invalid-basis`, and overlay views refuse snapshot tokens with `0A000`.

**Where in the code.**
- `RC/node/src/sql_snapshots.rs` (`SEALED_PREFIX`, `encode`, `decode`, `overlaid`)

### 2.5 Replication driver gates, watcher fault recovery, and standalone compactor

**What differs.**
RTDB's replication driver keeps upstream's leader/follower election and log-processor pipeline, with several deliberate operational differences:
- `AppliedPrefix` requires every admitted replica position to apply once and in order before promotion, writer activation, or prefix publication (replacing the deleted SHA-256 digest chain).
- The term fence uses an explicit `Voided` state and checked arithmetic instead of upstream's `Long.MAX_VALUE` sentinel.
- Read-back identity matches staged writes by SHA-256 frame fingerprint and pointer identity (`readback_identity`) rather than `txId` alone.
- Progress watchers continue serving already-applied prefixes after a driver fault, fail only unmet waits, and clear the terminal fault on explicit driver restart (`clear_terminal`), whereas upstream `Watchers.notifyError` fails every subsequent await.
- On term-fencing journals (Logkeeper, placed deployments), leadership promotes only with an external writer grant and an empty read.
- RTDB adds `wire_await_time` (awaiting by system time), catalog/serving checkpoints, offline journal repair (`repair-checkpoint`, `retain-repair-base`), and an active standalone `compactor` command that opens followers and publishes for a separate writer.

**Why.**
These changes harden leader handoff, avoid sentinel-overflow edge cases, prevent already-committed reads from failing after a recoverable fault, and support hosted and standalone compactor topologies.

**What a user sees.**
Awaits on already-applied transactions survive a driver fault and resume after restart.
Offline repair and active standalone compaction work out of the box without a live leader in the same process.

**Where in the code.**
- `RC/replication/src/applied_prefix.rs`
- `RC/replication/src/driver.rs`
- `RC/replication/src/progress.rs` (`clear_terminal`)
- `RC/replication/src/readback_identity.rs`
- `RC/replication/src/repair.rs`
- `RC/node/src/node.rs` (`wire_await_time`)

### 2.6 Additional log backends and native Kafka Connect plugin model

**What differs.**
Alongside InMemory, Local, ReadOnlyLocal, and Kafka, RTDB adds Logkeeper quorum logs, `logkeeper-consensus://`, Multi hosted journals, and FileJournal.
For Kafka, RTDB uses `librdkafka`, translates supported Java client properties, and refuses `transactional.id`.
The native adapter supports Kerberos login, unsecured OAuthBearer login for development, and client-credentials OIDC with token refresh.
For OIDC, list the exact token endpoint in `RTDB_KAFKA_OAUTHBEARER_ALLOWED_URLS` before opening a broker client.
For Kafka Connect sources, RTDB replaces dynamic JVM classloading with a closed native registry of converters (String, primitives, ByteArray, JSON, Confluent Avro, JSON Schema, and Confluent Protobuf), SMTs (Cast, Flatten, Timestamp, etc.), and three predicates, returning `xtdb.kafka-connect-source/plugin-class-not-found` for unknown classes, and adds schema-registry data-contract rules with a DLQ producer.

**Why.**
A native Rust binary cannot load arbitrary Java JARs from a classpath and needs native quorum-log backends for hosted Multi clusters.

**What a user sees.**
RTDB rejects custom Java Kafka Connect converters, transforms and OAuth callbacks at configuration time.
Built-in Connect pipelines gain schema-registry data-contract rules and DLQ support without a JVM.

**Where in the code.**
- `RC/logkeeper/src/`
- `RC/journal-logkeeper/src/`
- `RC/kafka/src/config.rs`
- `RC/kafka/src/connect/converter.rs`
- `RC/kafka/src/connect/transforms.rs`
- `RC/kafka/src/connect/registry/`

---

## 3. SQL dialect, XTQL, types, and error reporting

### 3.1 SQL parser architecture and deliberate SQL extensions

**What differs.**
Upstream XTDB parses every statement with ANTLR (`Sql.g4`) and Clojure visitors.
RTDB parses queries and DML with a vendored `sqlparser-rs` (`0.63`) plus a hand-written control parser (`parse_control`), re-parsing rejected constructs through a ported ANTLR `directSqlStatement` recognizer (`grammar_rejected`) to preserve upstream parse errors.
RTDB deliberately accepts several SQL extensions that upstream rejects:
- optional column types in `CREATE [OR ALTER] TABLE` (contributing nullable types to the catalog without constraining writes);
- `CREATE`/`DROP` DDL for `SEARCH INDEX`, `VECTOR INDEX`, and ordered `INDEX`, plus `RTDB_SEARCH`, `RTDB_NEAREST`, `RTDB_HYBRID` table functions and full-text, vector, and spatial scalar functions;
- `COPY (query) TO STDOUT WITH (FORMAT 'arrow-stream')` (plus a `/* rtdb-history-label-dictionary-v1 */` variant);
- scalar subqueries in `UPDATE ... SET`;
- internal helpers `epoch_nanos_be` and `_id_eq_any`.
`RECORDS` emits columns in first-seen key order.
Unsupported window forms return a structured `xtdb/sql-unsupported` error.

**Why.**
The extensions expose RTDB's secondary indexes, Arrow streaming export, and typed table declarations while keeping standard XTDB SQL rejections intact.
Deterministic `RECORDS` column ordering and structured `Unsupported` errors avoid accidental JVM-specific behaviour.

**What a user sees.**
Queries using these extensions run on RTDB but fail with parse or planning errors on upstream XTDB.
Unsupported window forms use SQLSTATE `0A000`.
`RECORDS` output columns appear in first-seen order.

**Where in the code.**
- `RC/sql/src/dialect.rs`
- `RC/sql/src/control.rs`
- `RC/sql/src/bind.rs`
- `RC/sql/src/parser/`
- `RC/pgwire/src/copy.rs`
- `RC/sql/README.adoc`

### 3.2 Regex safety limits and XTQL reader / host-form whitelist

**What differs.**
Upstream XTDB delegates regexes to `java.util.regex` with no compile or backtracking cap, parses XTQL with the Clojure reader, and splices computed `regexp-replace` patterns directly into Clojure codegen.
RTDB translates Java regex syntax onto a vendored `fancy-regex` engine plus a UTF-16 interpreter for `\b{g}` and `\R`, enforcing hard limits (64 KiB pattern, nesting 128, repetition 10,000, 128 groups, and backtracking budgets) that fail with `rtdb.expr/regex-limit` (`ProgramLimitExceeded`, `54000`).
RTDB parses XTQL with a native EDN reader that supports portable escapes, tagged values and integral ratios.
Computed `regexp-replace` patterns use a whitelist of string and number functions, including `java.util.regex.Pattern/quote` and supported `clojure.string/*` operations.

**Why.**
Hard regex limits prevent catastrophic backtracking DoS on native worker threads.
A non-JVM engine cannot execute arbitrary Clojure reader macros or host expressions.

**What a user sees.**
Pathological or oversized regex patterns fail with SQLSTATE `54000` (`rtdb.expr/regex-limit`) instead of hanging.
Use literal strings for regex patterns and resolve Clojure reader conditionals before submitting XTQL.
An arbitrary JVM host expression or Java object tag fails during read or preparation.

**Where in the code.**
- `RC/expr/src/string/pattern.rs`
- `RC/expr/src/evaluate/invocation/host.rs`
- `RC/xtql/src/edn.rs`
- `RC/xtql/src/ast.rs`

### 3.3 Bundled OpenJDK time-zone database and `RTDB_TZDB_FILE`

**What differs.**
Upstream XTDB uses whichever `lib/tzdb.dat` the host JDK provides.
RTDB compiles in OpenJDK `tzdb.dat` (`2026e`, overridable at build time via `RTDB_TZDB_PATH`) and decodes it lazily on first region lookup.
At runtime, the server and Multi binaries install an optional `RTDB_TZDB_FILE` path at startup to replace the bundled database without rebuilding.
Legacy `java.util.Date` text parsing is ported from OpenJDK 25 CLDR tables in `rtdb-java-date`.

**Why.**
A standalone native binary has no JDK installation to supply `tzdb.dat` and still needs an operator-replaceable zone database.

**What a user sees.**
Time-zone transitions can differ from an XTDB node running on a JDK with a different IANA tzdb version unless both use the same `tzdb.dat`.
An unreadable `RTDB_TZDB_FILE` fails with `xtdb/tzdb-unavailable` on the first region lookup rather than aborting startup.

**Where in the code.**
- `RC/types/src/zone.rs` (`install_tzdb_file`, `tzdb_version`)
- `RC/types/build.rs`
- `RC/server/src/config/mod.rs`
- `RC/java-date/src/`

### 3.4 Host locale source for `UPPER` and `LOWER`

**What differs.**
Upstream XTDB uses `Locale.getDefault()` (which can be overridden with `-Duser.language`) for locale-sensitive `UPPER`/`LOWER` rules.
RTDB reads the host process display locale once at startup (`LC_MESSAGES` on Linux, host locale on macOS) and falls back to the root locale if the host locale cannot be read, with no `-Duser.language`-style override flag.

**Why.**
Reading the OS locale once matches the JVM's default behaviour without adding JVM-specific system-property flags.

**What a user sees.**
Turkish, Azeri, and Lithuanian case mappings follow the OS process locale (set via `LC_ALL`/`LC_MESSAGES` on Linux) rather than a JVM property.
A host with no readable locale starts normally and uses root Unicode case mapping.

**Where in the code.**
- `RC/expr/src/string.rs` (`case_language`, `language_or_root`)
- `RC/java-date/src/date_platform.rs` (`process_language`)

### 3.5 Extended scalar types and RTDB-specific error codes and SQLSTATEs

**What differs.**
Upstream XTDB rejects half-floats and maps unsigned Arrow integers onto signed vectors; its pgwire layer maps unknown `Incorrect` errors to `08P01` and Arrow OOMs to `XX000`.
RTDB's `ScalarType` includes `U8`–`U64` (reinterpreted as signed integers during execution) and `F16` (raising `rtdb/jvm-half-float` where upstream fails).
RTDB matches upstream's `XX000` (logged at ERROR) for memory exhaustion (`AnomalyKind::ResourceExhausted`), while mapping RTDB-only limit and range kinds to PostgreSQL-specific SQLSTATEs: `53400` (`ConfigurationLimitExceeded`), `54000` (`ProgramLimitExceeded`), and `22003` (`NumericValueOutOfRange`).
RTDB also emits `rtdb*` error codes for RTDB-only checks, with a workspace source test pinning the small allow-list of limit-shaped codes that legitimately keep `08P01`.

**Why.**
PostgreSQL clients benefit from distinct SQLSTATEs (`53400`, `54000`, `22003`) for RTDB-only configuration, program, and integer-width limits while preserving exact XTDB SQLSTATEs for every upstream error and for OOM.

**What a user sees.**
Exceeding an opt-in spill cap reports `53400`, hitting regex/expression/plan/batch size limits reports `54000`, and overflowing a pgwire integer width reports `22003`.
Out-of-memory failures report `XX000` and are logged at ERROR, matching XTDB.

**Where in the code.**
- `RC/types/src/logical.rs` (`ScalarType`)
- `RC/types/src/error.rs` (`AnomalyKind`)
- `RC/pgwire/src/error.rs` (`classify`, `kind_state`, `LIMIT_SHAPED_PROTOCOL_VIOLATIONS`)
- `RC/pg-codec/src/output.rs` (`integer_in_range`)

---

## 4. Query engine, accelerators, and memory model

### 4.1 Explicit memory budget, opt-in spill quota, and deterministic spill thresholds

**What differs.**
Upstream XTDB splits memory by JVM ergonomics (heap plus `0.9 ×` max direct memory with per-query Arrow child allocators) and has no spill cap.
RTDB enforces one explicit byte ceiling (`--memory-limit` or `RTDB_MEMORY_LIMIT_BYTES`, defaulting to half of physical or cgroup memory, or 1 GiB when unknown; embedded `NodeOptions::default` uses 512 MiB execution + 256 MiB cache).
Reservations account for Arrow buffers, rows, cells, log frames, and operator scratch up to the root budget.
Spill is unlimited by default (`QueryOptions::UNLIMITED_SPILL`), matching XTDB, with an opt-in per-query limit via `query.spillLimitBytes` or `RTDB_QUERY_SPILL_LIMIT_BYTES`.
Spill decisions use only fixed `QueryOptions` thresholds (`sort_memory_bytes = 64 MiB`, `join_build_rows = 100_000`, `materialized_spill_rows = 100_000`), never free budget bytes, and `GROUP BY`, aggregates, and `DISTINCT` stay in memory as in XTDB.
A refused allocation fails the query rather than silently switching strategy (except cache admission and optional search accelerators).

**Why.**
A native process has no GC heap or JVM direct-memory cap, so explicit admission control prevents OS OOM kills while keeping query plans deterministic for a given configuration.

**What a user sees.**
Memory-heavy queries can fail with `rtdb/memory-budget-exceeded` (`XX000`) at a different threshold than on a JVM with default ergonomics.
Operators who configure `query.spillLimitBytes` get a hard cap on temporary spill disk usage (failing with `rtdb.query/spill-limit`, SQLSTATE `53400`).

**Where in the code.**
- `RC/server/src/cli/memory.rs` (`default_memory_limit_bytes`, `cgroup_memory_limit`)
- `RC/types/src/memory.rs` (`MemoryBudget`)
- `RC/node/src/config.rs` (`NodeOptions`)
- `RC/query/src/context.rs` (`QueryOptions`)
- `RC/server/src/config/mod.rs` (`query_spill_limit`)

### 4.2 Interpreted and column-kernel expression evaluation instead of bytecode codegen

**What differs.**
Upstream XTDB compiles each expression per input vector schema into Clojure/JVM bytecode and memoises the compiled projection (`emit-projection`).
RTDB chooses a native Arrow kernel or a scalar interpreter at expression preparation.
A failing cell raises the scalar's own error, strict calls short-circuit on null in argument order as in XTDB, and `CASE` evaluates each branch only on its selected rows.

**Why.**
Arrow kernels handle supported column operations directly.
The scalar interpreter implements operations and layouts that require cell-wise evaluation.

**Where in the code.**
- `RC/query/src/columnar.rs`
- `RC/query/src/columnar/functions.rs`
- `RC/expr/src/evaluate.rs`
- `RC/expr/src/functions.rs`

### 4.3 Query optimizer, decorrelation edge cases, and `EXPLAIN` output

**What differs.**
Upstream XTDB runs Clojure zipper rewrites (`logical_plan.clj`, `rewrite.clj`) and formats `EXPLAIN` from those plans.
RTDB implements its own Rust optimizer (`optimize.rs`, `optimize/decorrelate.rs`), so `EXPLAIN` text and plan structure can differ from XTDB's.
RTDB ports XTDB's decorrelation rules (rules 1–4, 8, 9, `apply-table-col->unnest`, `select-mark-join->semi-join`, and the push-down rules) with deliberate exceptions:
- RTDB lifts a correlated selection above an unnest whose list reads no bound parameter (preserving the same rows), which XTDB does not;
- RTDB does not lift correlated selections from the null-supplying side of an outer join;
- ungrouped collecting/ordered aggregates under scalar/single/inner/left applies, parameterized percentile fractions, volatile dependent plans, and `PULL`/`PULL_MANY` keep their per-row apply.

**Why.**
The Rust optimizer targets semantic equivalence and safe decorrelation without replicating Clojure's internal zipper AST verbatim.

**What a user sees.**
Query results match XTDB, while `EXPLAIN` plans and the row order of unordered queries can differ.
Correlated shapes on the remaining per-row apply list execute once per outer row.

**Where in the code.**
- `RC/query/src/optimize.rs`
- `RC/query/src/optimize/decorrelate.rs`
- `RC/query/README.adoc`

### 4.4 Plan caching without a global SQL parse or emit cache

**What differs.**
Upstream XTDB maintains a 4,096-entry SQL parse cache, a 4,096-entry plan cache, per-plan emit caches, and a DML skeleton cache.
RTDB caches plans per prepared statement (`CachedPlan`, LRU of 4 parameter-type signatures), shares read plans across sessions in `SharedPlans` (128 plans within 1/64 of execution memory, capped at 16 MiB, excluding DML and `EXPLAIN`), and reuses prepared mutations across argument rows in `MutationPlans`.
Plan reuse checks catalog dependencies and the recorded join estimates.
Parsing is scoped to statement preparation.

**Why.**
Bounded caches charge retained plans to execution memory.

**What a user sees.**
Repeated preparations can reuse read plans when their dependencies and estimates still agree with the current snapshot.

**Where in the code.**
- `RC/node/src/statement.rs`
- `RC/query/src/shared_plans.rs`
- `RC/query/src/compiled.rs`
- `RC/tx/src/resolver.rs` (`MutationPlans`)

### 4.5 Scan accelerators: UUID `_id` ranges, IID-ordered `LIMIT`, and top-k sort

**What differs.**
Upstream XTDB narrows scans on `_id = v` equality only and executes `ORDER BY _id LIMIT n` and general `ORDER BY ... LIMIT n` through its block-based sorter.
RTDB additionally narrows `_id <`, `<=`, `>`, `>=`, and `BETWEEN` predicates to an IID interval when every `_id` in the table is a UUID (since a UUID is its own big-endian IID).
Over such tables, `ORDER BY _id LIMIT n` streams scan tasks in ascending or descending IID order with lazy per-leaf statistics and stops once `n` rows are produced.
For general sorts with `LIMIT <= 12_800` (`top_k_rows`), RTDB uses an in-memory top-k path with early rejection.

**Why.**
UUID keys already preserve order in the hash trie's IID space, allowing range scans and ordered limits to skip unrelated trie leaves without an extra index.

**What a user sees.**
UUID range queries, `ORDER BY _id LIMIT n` over UUID tables, and small top-k queries read fewer pages and run faster than on XTDB, while returning the same rows.

**Where in the code.**
- `RC/query/src/scan.rs` (`id_interval`, `iid_ordered_input`)
- `RC/query/src/sort.rs` (`SortPlan::top`)
- `RC/query/src/context.rs` (`top_k_rows`)

### 4.6 Full-text, vector, spatial, and ordered secondary indexes and artefact lane

**What differs.**
Upstream XTDB has no built-in full-text search, vector search, spatial index, or secondary ordered index subsystem.
RTDB includes bitemporal full-text search (`rtdb-search`, default feature of `rtdb-node`, `search.writeFormat` 11–16 defaulting to 13 so plain workloads write no entity-history artefacts), IVF vector search (`rtdb-vector`), spatial postings (`rtdb-spatial`), ordered secondary indexes (`ordered-index/v2/`), a background artefact build lane (`artefact_threads`), scan pushdowns (`MATCH`, `ORDER BY search_score DESC LIMIT n`, ordered-index narrowing), and system tables (`xt.search_indexes`, `xt.ordered_indexes`, `xt.vector_indexes`, `xt.ranked_indexes`, `xt.live_trie_artefacts`, `xt.live_ordered_index_storage`).

**Why.**
RTDB provides native bitemporal retrieval and ranking inside the database without an external search engine.

**What a user sees.**
Workloads that define no secondary indexes stay on XTDB-compatible storage (with `writeFormat` 13).
Defining a search, vector, spatial, or ordered index adds artefacts and table-block metadata framed for XTDB refusal (§1.1), and exposes the corresponding SQL functions and `xt.*` catalog tables.

**Where in the code.**
- `RC/search/src/`
- `RC/vector/src/`
- `RC/spatial/src/`
- `RC/node/src/ordered_index/`
- `RC/maintenance/src/artefact.rs`
- `RC/query/src/accelerate.rs`
- `RC/query/src/rank_cutoff.rs`

### 4.7 Internal execution representations (live chunks, scan merge, metadata, compaction, and cancellation)

**What differs.**
RTDB implements several core engine internals differently from XTDB while preserving observable query semantics:
- **Live index.** Each `LiveTable` version holds a persistent `LiveRelation` of immutable Arrow `IndexBatch` chunks with stable row addresses.
  Later versions share the prefix, and readers keep their original version.
  XTDB appends to a growable in-place `Relation`.
  RTDB's trie catalog shares immutable table and block maps across catalog versions.
- **Scan merge and temporal resolution.** `PartitionScan` runs a heap-based merge across event sources.
  Eligible Classic point scans track the valid-time gap around the point and seek to the next IID once the result is settled.
  Eligible range predicates run before payload gathering, and historical cuts that require full bounds use the polygon resolver.
- **Metadata pruning.** Leaf statistics are decoded as the scan visits each leaf.
  Byte equality probes use a validated borrowed view of standard serialized Roaring blooms.
  Other encodings go through the checked bitmap decoder.
- **Value boundaries:** while system columns (`_iid`, `_valid_*`, `_system_*`) are Arrow arrays, spill uses Arrow IPC, and pgwire encodes directly from Arrow where supported, several operators (non-columnar joins, aggregates, window, set ops), DML writes (`TransactionBuffer`), Flight bulk ingest, and compaction merges (`SegmentCompactor`, running parallel jobs on `spawn_blocking`) still convert through owned `Value` / `StoredEvent` trees.
- **Cancellation:** RTDB uses per-query `CancellationToken` trees and cooperative checks rather than JVM thread interrupts.

**Why.**
Immutable Rust data structures, cooperative async cancellation, and incremental columnarization fit Rust's ownership model without requiring unsafe shared-memory mutation.

**What a user sees.**
Snapshots retain their own live relation prefix as new writes arrive.
The representations above affect which queries and compactions decode or copy values.

**Where in the code.**
- `RC/index/src/live.rs`
- `RC/index/src/partition.rs`
- `RC/index/src/trie_catalog.rs`
- `RC/query/src/scan.rs`
- `RC/temporal/src/lib.rs`
- `RC/format/src/metadata.rs`
- `RC/query/src/page_metadata.rs`
- `RC/maintenance/src/merge.rs`
- `RC/maintenance/src/driver.rs`
- `RC/flight-sql/src/upload.rs`

---

## 5. Protocols, APIs, security, and deployment

### 5.1 pgwire connection model, `COPY IN` limit, and RTDB protocol extensions

**What differs.**
Upstream pgwire uses virtual threads capped by `num-threads` with a 1 KiB socket buffer and spools `COPY FROM STDIN` to an unbounded temp file.
RTDB runs one Tokio task plus a 64-message reader task per connection with a 64 KiB `BufWriter`, `TCP_NODELAY`, and no thread ceiling, and caps `COPY FROM STDIN` at `maximum_copy_bytes` (1 GiB by default).
RTDB also accepts opt-in startup parameters (`rtdb.capability`, `rtdb.require_installation`, `rtdb.gateway_*`), emits `rtdb.delegable` ParameterStatus and custom `'Y'` backend messages when opted in, and enforces TLS on installed standalone/multi listeners (`require_tls`).

**Why.**
Async tasks and 64 KiB buffering reduce syscall overhead and remove connection caps; capping `COPY IN` bounds untrusted disk usage; startup extensions support Multi gateways and capability-scoped sessions.

**What a user sees.**
Standard PostgreSQL clients see XTDB-compatible pgwire behaviour with lower connection overhead.
A `COPY FROM STDIN` exceeding 1 GiB is rejected, and plaintext connections to an installed TLS listener are refused.

**Where in the code.**
- `RC/pgwire/src/config.rs` (`maximum_copy_bytes`)
- `RC/pgwire/src/connection.rs`
- `RC/pgwire/src/copy.rs`
- `RC/pgwire/src/protocol.rs`

### 5.2 Flight SQL extensions, signed inspection tickets, and Flight TLS

**What differs.**
RTDB's plain Flight SQL `run` and `empty` tickets use upstream XTDB's unsigned JSON `QueryTicket` format.
A non-default search path uses the RTDB extension key `"searchPath"`.
The reader still accepts the `"awaitFloor"` key on earlier RTDB tickets.
Three deliberate RTDB extensions remain:
- control inspections (`SHOW`) issue HMAC-signed, session-bound `"inspect"` tickets that expire with the session;
- `ListActions` advertises `rtdb.v1.*` (and, when enabled, `rtdb.store.v1`, `rtdb.owned.*`, `rtdb.custody.v1`, `rtdb.admin.v1`, and hosted DoExchange uploads) alongside the standard Flight SQL actions;
- Flight handles bind to caller identity, and Flight SQL supports TLS on standalone, loopback-TLS, and private Multi listeners.

**Why.**
`SHOW` commands read session state that cannot safely be replayed by another caller, while native v1, admin, and hosted protocols share the Flight transport.

**What a user sees.**
Standard Flight SQL / ADBC queries produce and accept plain XTDB JSON tickets.
`ListActions` includes additional `rtdb.*` actions, and inspection tickets expire with their issuing session.
Hosted audience reads use the Root's ordinary store connection with kernel version tokens.

**Where in the code.**
- `RC/flight-sql/src/ticket.rs`
- `RC/flight-sql/src/service.rs`
- `RC/flight-sql/src/admin.rs`
- `RC/flight-sql/src/server.rs`

### 5.3 Authentication extensions and bounded Argon2 admission queue

**What differs.**
Upstream XTDB verifies Argon2id passwords on the JVM heap with unbounded concurrency, uses JSSE for TLS keystores, and has no per-database ACL grants or workload OIDC tag.
RTDB verifies passwords through `PasswordAdmission`, which runs at most one verification per CPU core on `spawn_blocking` against its own dedicated memory budget (sized to `cores × costliest configured hash`) so logins queue instead of competing with query memory.
RTDB adds per-database `grants` (`DENY`/`READ`/`READ_WRITE`), OIDC `issuerCaCertificate`/`workloadGrants`/`!ManagedWorkloadOidc`, per-database signed-claim key vaults, and native JKS/PKCS12 decoding over `rustls`/`aws-lc-rs`.

**Why.**
Unbounded 64 MiB Argon2id verifications can OOM a memory-budgeted native process during login storms.
Per-database grants and workload OIDC support multi-tenant hosted deployments.

**What a user sees.**
Standard `TRUST`, `PASSWORD`, `DEVICE_AUTH`, and `CLIENT_CREDENTIALS` flows behave as in XTDB, except login bursts queue smoothly at one verification per core rather than failing or exhausting query memory.
TLS cipher negotiation uses `rustls`/`aws-lc-rs` instead of JSSE.

**Where in the code.**
- `RC/auth/src/password.rs` (`PasswordAdmission`)
- `RC/auth/src/lib.rs` (`authorize_database`)
- `RC/auth/src/managed_workload_oidc.rs`
- `RC/tls/src/`

### 5.4 Embedded APIs, CLI commands, configuration extensions, and Multi cell runtime

**What differs.**
Upstream XTDB embeds via `Xtdb.openNode`, `Xtdb.Connection`, Clojure `xtdb.api`, classpath config discovery, EDN with `#env`, and JVM extension hooks (`!Edgar`, `!Gleif`, `!Recording`, `allocator`, `spanProcessor`, `instantSource`).
RTDB exposes a C ABI and a JDK 25 FFM binding through `NativeDatabase`, ADBC, and Clojure `Connectable`.
The binding supports native config files, listener and catalog discovery, a JDBC connection builder, typed transaction settings, and portable document and SQL operations.
Config loading uses native YAML or data-only EDN.
RTDB adds CLI subcommands (`dev`, `standalone`, `initialize-standalone`, `admin`, `classic-opening`, `native-dependent-custody`, `repair-checkpoint`, `retain-repair-base`, `inspect`, `--memory-limit`, `--build-info`), JSON output for file inspectors, the offline `rtdb-config` qualification binary, RTDB-only YAML keys and environment variables, and the separate `rtdb-multi` shared-cell runtime (`multiCheckpoints: {deferred, binary}`, binary branch records, gateways, logkeeper quorums, and deployment admin RPCs).

**Why.**
A native Rust engine cannot host arbitrary JVM extension plugins and needs native C/FFM embedding, offline config checks, and a multi-tenant cell runtime.

**What a user sees.**
Embedded callers use `NativeDatabase` and the native connection types.
The binding accepts native configuration for log, storage, and authentication providers.
YAML files using RTDB-only keys (such as `search.*`, `grants`, `query.spillLimitBytes`, `defaultV1Serving`, or `multiCheckpoints`) are rejected by upstream XTDB, and configs using JVM-only tags are rejected by RTDB.

**Where in the code.**
- `RC/ffi/src/`
- `RC/ffi/include/rtdb.h`
- `bindings/jvm/`
- `RC/server/src/cli.rs`
- `RC/server/src/config/mod.rs`
- `RC/server/src/standalone.rs`
- `RC/config/src/`
- `RC/multi/src/`

### 5.5 Container runtime, backend Cargo features, and cluster configurations

**What differs.**
Upstream XTDB ships an Alpine image running as user `xtdb` (UID/GID 20000) on a custom JRE with runtime backend modules.
RTDB's image matches upstream's `/usr/local/lib/xtdb` workdir, `-f config.yaml` command, `VOLUME /var/lib/xtdb`, default no-password / `XTDB_PASSWORD` auth, and `wget` `HEALTHCHECK`, while deliberately using Debian Bookworm, user `rtdb` (same UID/GID 20000), and `LD_PRELOAD=libjemalloc.so.2` (with legacy fallbacks in `rtdb-entrypoint` for `/etc/rtdb/config.yaml` and `/var/lib/rtdb`).
Remote storage and Kafka backends (`s3`, `azure`, `gcs`, `kafka`, `pg-source`, `cloudwatch`) are compile-time Cargo features rather than runtime modules, so `--variant local` omits them and `all-backends` bundles them.
The normal `rtdb` binary uses mimalloc as its Rust global allocator.
The heap-profiling build selects the `dhat` allocator.
The bundled `/etc/rtdb/cluster-default.yaml` and `/etc/rtdb/cluster-multi.yaml` files are RTDB-only cluster configs that read `RTDB_ROOT_PASSWORD`.

**Why.**
Debian Bookworm supplies `glibc`, `libsasl2`, and `libjemalloc2` cleanly for native builds, and Cargo features keep local-only binaries small.

**What a user sees.**
Drop-in container mounts at `/var/lib/xtdb` and `/usr/local/lib/xtdb/config.yaml` work as in XTDB.
Container tooling inside the image sees Debian (`apt`) and user `rtdb` instead of Alpine (`apk`) and `xtdb`, and enabling S3/Azure/GCS/Kafka requires a binary built with those features.

**Where in the code.**
- `rust/release/Dockerfile`
- `rust/release/entrypoint.sh`
- `rust/release/README.adoc`
- `RC/server/Cargo.toml`
- `RC/server/src/main.rs`

### 5.6 Observability: logging targets, JSON templates, metrics, tracing export, and crash logs

**What differs.**
RTDB's `tracing` subscriber honours `XTDB_LOG_FORMAT`, `XTDB_LOGGING_LEVEL[_NS]`, and `XTDB_LOG_JSON_TEMPLATE`.
It compiles bundled `classpath:` and supported `file:` Log4j2 templates at startup.
Native aliases accept upstream log-level selectors for components such as pgwire, indexing, Kafka Connect and cloud storage.
Metrics use 16 per-thread sample shards, publish native allocator gauges, omit JVM-only binders (`jvm.*`), and add RTDB-only metrics (`rtdb.cache.{limit,retained}.bytes`, `rtdb.gc.error`, `rtdb.kafka.warning`, `rtdb.replica.commit.*`, `query.linearizable`, `query.page`, `query.track`).
OTLP traces use HTTP protobuf.
CloudWatch and Azure metric exporters build native JSON payloads.
Crash logs under `crashes/<node-id>/<instant>/` include `crash.edn`, available transaction operations and live-table data, and transaction-local files when the resolver reports a failed table operation with a captured open table.
The driver currently supplies no query relation to the crash writer.

**Why.**
Native Rust binaries replace JVM-only libraries (Log4j2, Micrometer, OpenTelemetry Java SDK) with `tracing` and lightweight JSON exporters while preserving upstream environment variables, Prometheus exposition, and core metric names.

**What a user sees.**
Fine-grained Rust targets beyond the aliases use native module names.
Prometheus exposes native allocator and replica metrics alongside RTDB-specific counters.
The transaction crash path currently omits `query-rel.arrow`.

**Where in the code.**
- `RC/metrics/src/logging.rs` (`UPSTREAM_ALIASES`)
- `RC/metrics/src/logging/template.rs`
- `RC/metrics/src/lib.rs`
- `RC/server/src/prometheus.rs`
- `RC/server/src/tracing.rs`
- `RC/server/src/metric_export/`
- `RC/replication/src/crash_log.rs`
