# XTDB and RTDB compared

RTDB is the Rust port of XTDB in `rust/`.
This page records how closely it agrees with the reference engine and how fast each runs the same work.
Every number here comes from one RTDB revision, `15df08a4d`, compared against the reference XTDB checkout at `c84f9334e`.

## Correctness

### SQL logic tests

The SQL-logic runner executes each suite against both engines and compares every assertion.

| Suite | Assertions | Pass on both | Mismatches | Not evaluated by either |
| --- | ---: | ---: | ---: | ---: |
| `slt-test` | 60,304 | 48,136 | 0 | 12,168 |
| `slt-test-2` | 62,369 | 60,179 | 0 | 2,190 |
| `direct-sql-test` | 274 | 273 | 0 | 1 |

An assertion is not evaluated when the test file reserves it for another database (`onlyif` and `skipif`), or when it is a duplicate projection the suite suppresses.
Both engines skip exactly the same assertions, so the runner reports these as missing evidence rather than as differences.

### The reference test corpus

The corpus runner loads every XTDB Clojure test namespace and runs it against the native engine through its JVM binding.

**These numbers do not measure upstream conformance and are superseded until the corpus is re-run** (`rust/conformance/README.adoc`, "Native corpus").
The run behind them loaded this repository's copies of the tests, whose assertions RTDB edited, kept only each Var's first failure, and excused failing Vars by pattern rather than from a reviewed list.
The runner now loads upstream XTDB's own test sources and fixtures at the pinned commit, records every failure, excuses a Var only through `rust/conformance/corpus-exclusions.json`, and reports native-free passes as `pass-jvm-only` rather than as not applicable.

| Outcome | Tests |
| --- | ---: |
| Pass | 935 |
| Pass over the wire (pgwire or Flight SQL) | 157 |
| Fail | 0 |
| Error | 1 |
| Not applicable: exercise JVM internals (`jvm-internal`, `jvm-api-boundary`) | 571 |
| Not applicable: need unavailable infrastructure, have no assertions, or are integration-only | 72 |

The one error is `xtdb.indexer-test/test-multi-block-metadata`, which compares an on-disk block file with a golden file and has differed in every run so far; it remains open.
`xtdb.authn-test` failed to load in this run because its Docker container could not start in this environment, so it counts among the unavailable-infrastructure tests rather than as a failure; its tests are JVM-internal anyway.

## Performance

### Method

Both engines run as their packaged Docker images on the same host, one at a time, each container limited to 4 CPUs and 4 GiB of memory with no swap.

- **XTDB:** its own standalone build at `c84f9334e`, run with `-Xmx2g -XX:MaxDirectMemorySize=1536m`.
- **RTDB:** built with `build.py --engine rust --variant local` at `15df08a4d`, with a 3 GiB memory budget.

Both use a local log and local storage on a container volume, with pgwire on port 5432 and Flight SQL on port 3000.
The host is an Apple-silicon Mac running the containers in a Colima Linux VM, so absolute times include the VM's port forwarding.

The workload is the same for both engines and lives in [`bench/`](bench/):

- `bench.py` drives pgwire through psycopg 3.
- `bench_flight.py` drives Flight SQL through the ADBC driver.
- `run.sh` starts each container.
- `compare.py` prints the comparison tables.

Each engine ran twice, and the tables give the better of the two runs for each metric.
Latencies are medians over the operations in a run; throughputs are operations divided by wall-clock time.

The data is 100,000 orders over 1,000 customers.
The reads are:

- point lookups by `_id`;
- a filtered aggregate;
- a join with grouping;
- a top-100 sort;
- a valid-time as-of query;
- one entity's full valid-time history.

The writes are:

- bulk ingest in transactions of 1,000 rows;
- single-row INSERT, UPDATE and DELETE, each in its own transaction;
- a set-based UPDATE of 10% of the rows;
- 1,000 bitemporal updates in one transaction;
- read-your-write: an insert followed by a read of the same row.

There are also three concurrent workloads, each on eight connections: concurrent writers, concurrent readers, and a mix of 80% reads and 20% updates.

### pgwire

| Workload | XTDB | RTDB | RTDB relative to XTDB |
| --- | ---: | ---: | ---: |
| Startup to first query | 3.73 s | 0.01 s | 324× faster |
| Peak memory | 697 MB | 160 MB | 4.4× less |
| Bulk ingest | 44,154 rows/s | 41,389 rows/s | 0.94× |
| Point lookup by `_id` | 0.61 ms | 0.42 ms | 1.47× |
| Valid-time history of one entity | 0.55 ms | 0.43 ms | 1.28× |
| Filtered aggregate | 9.7 ms | 21.9 ms | 0.45× |
| Join with grouping | 42.6 ms | 99.9 ms | 0.43× |
| Top 100 by amount | 41.7 ms | 32.5 ms | 1.28× |
| Valid-time as-of aggregate | 9.6 ms | 24.6 ms | 0.39× |
| Single INSERT | 2.11 ms | 1.40 ms | 1.51× |
| Single UPDATE by `_id` | 3.27 ms | 1.61 ms | 2.03× |
| Single DELETE by `_id` | 2.97 ms | 1.52 ms | 1.95× |
| Set-based UPDATE of 10,000 rows | 124 ms | 132 ms | 0.94× |
| 1,000 bitemporal updates, one transaction | 0.23 s | 0.26 s | 0.86× |
| Read your write | 2.70 ms | 1.78 ms | 1.52× |
| 8 concurrent writers | 1,517 tx/s | 1,507 tx/s | 0.99× |
| 8 concurrent readers | 3,891 queries/s | 12,121 queries/s | 3.12× |
| Mixed 80/20 on 8 connections | 2,397 ops/s | 4,470 ops/s | 1.86× |

At the 95th percentile, RTDB's latency is lower than XTDB's for:

- point lookups: 0.46 ms against 0.97 ms;
- single-row writes;
- read-your-write: 3.1 ms against 4.5 ms.

### Flight SQL

| Workload | XTDB | RTDB | RTDB relative to XTDB |
| --- | ---: | ---: | ---: |
| Bulk ingest | 132,894 rows/s | 86,324 rows/s | 0.65× |
| Point lookup by `_id` | 2.72 ms | 1.80 ms | 1.51× |
| Valid-time history of one entity | 2.58 ms | 1.79 ms | 1.44× |
| Filtered aggregate | 26.4 ms | 18.0 ms | 1.47× |
| Join with grouping | 41.3 ms | 93.1 ms | 0.44× |
| Top 100 by amount | 49.3 ms | 24.6 ms | 2.00× |
| Full scan of 100,000 rows to the client | 312 ms | 317 ms | 0.99× |
| Single INSERT | 2.06 ms | 1.94 ms | 1.06× |
| Single UPDATE by `_id` | 2.72 ms | 2.12 ms | 1.28× |
| Single DELETE by `_id` | 2.84 ms | 1.89 ms | 1.50× |
| Read your write | 6.16 ms | 4.75 ms | 1.30× |
| 8 concurrent writers | 1,210 tx/s | 1,170 tx/s | 0.97× |
| 8 concurrent readers | 1,607 queries/s | 2,241 queries/s | 1.39× |

### Where RTDB is still slower

RTDB is slower than XTDB on 11 of the 32 metrics, and three of those are within 3%.

- **Joins.** RTDB builds its hash table and copies matched rows one row at a time, where XTDB's join works on whole Arrow vectors. This makes joins the largest gap on both protocols.
- **pgwire filtered aggregate and valid-time as-of.** These run more slowly than the same queries over Flight SQL. psycopg writes small integers as `int2` and `int4`, so `_id` is stored as a union of integer types. RTDB reads union columns value by value instead of through its column kernels.
- **Bulk ingest.** In a 4-CPU container, ingest competes with compaction, which decodes and re-encodes every event. XTDB's compactor copies Arrow columns instead.

The concurrent-writer numbers vary from run to run on this host for both engines, so treat the two concurrent-writer rows as level.

### How the gap was closed

In the first matched run of this benchmark over both protocols, RTDB was slower than XTDB on 24 of 32 metrics.
Each slow path was then profiled against the native server, compared with what the reference does, and changed.
The commits from `31c72e6ad` to `15df08a4d` carry the details.

- **Query plans now get the reference's rewrites.**
  Selections move past joins and into scans, joins are ordered by their conditions, columns are pruned through joins and aggregates, and an `_id` equality narrows the scan to the matching IIDs.
  DML inputs used to bypass the optimiser entirely, so an UPDATE by `_id` scanned its whole table.
- **Scans emit column pages.**
  Filters, plain projections, arithmetic, aggregates and small top-k sorts read whole columns.
  Where a kernel can't guarantee the same result as the row evaluator, it falls back to the evaluator, and property tests check the kernels against it.
- **Decoded data is cached.**
  Trie metadata, file layouts, data pages and event headers are cached instead of being decoded again by every query.
- **Grouping uses a hash index.**
  Finding a row's group used to scan every previous group.
- **Memory admission is cheaper.**
  It charges whole chunks of rows at once and reuses one lease per scope, and cancellation checks use a per-query token, so per-row bookkeeping no longer allocates or contends.
- **A transaction's reads of its own writes are incremental.**
  They extend one segment instead of rebuilding a snapshot of every write so far.
- **Transport overheads are gone.**
  pgwire output is buffered, and Flight SQL disables Nagle's algorithm and no longer recounts its registries on every call.

An independent review of these changes found several places where they changed behaviour, and each one was fixed with a regression test before these numbers were taken:

- float ordering of -0.0;
- the error a stacked filter raises;
- error and memory accounting in the page cache, top-k and column pages;
- a panic on nested columns.

The same review led to a fix for an older problem: a WHERE clause of about a thousand conjuncts used to abort the process with a stack overflow.
