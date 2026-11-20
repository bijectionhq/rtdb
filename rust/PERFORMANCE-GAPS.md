# RTDB performance parity with XTDB

The goal is parity or better across the matched pgwire and Flight SQL workloads at 100,000 and 1,000,000 rows, with transaction, query and persisted-format correctness preserved.
Track throughput, p50 and p95 latency, and peak memory under matched conditions.
The results below describe specific builds and workloads.

## Baseline

The recovered full benchmark used RTDB `f97d4877f` and XTDB `c84f9334e`, local storage and logs, 4 CPUs and a 4 GiB container limit.
It reported the best of two runs per metric.
Flight SQL ran on the same node after the pgwire suite, while the reported memory peak was sampled during pgwire only.
The parity branch starts from RTDB `c67d7a04f` and needs its own baseline.

| Workload | 100k, RTDB relative to XTDB | 1M, RTDB relative to XTDB | 1M values, RTDB / XTDB | Status |
| --- | ---: | ---: | --- | --- |
| Flight prepared INSERT | 0.75× | 0.45× | 77.2k / 170.5k rows/s | Open |
| pgwire batched INSERT | 1.19× | 0.86× | 46.8k / 54.4k rows/s | Open at 1M |
| pgwire concurrent INSERT | 1.05× | 0.84× | 1,766 / 2,095 tx/s | Open at 1M |
| pgwire UPDATE of 10% of orders | 2.21× | 0.82× | 728 / 599 ms | Open at 1M |
| pgwire peak container working set | 5.08× | 0.49× | 1,582 / 781 MB | Open at 1M |
| Flight full-scan fetch | 0.99× | 1.00× | 3,197 / 3,186 ms | Recheck measurement noise |
| Flight single INSERT | 1.14× | 0.98× | 1.74 / 1.70 ms | Recheck at 1M |
| Flight concurrent INSERT | 1.41× | 0.97× | 1,221 / 1,257 tx/s | Recheck at 1M |

The ratios above divide RTDB throughput by XTDB throughput, or XTDB cost by RTDB cost for latency and memory.
The same baseline favored RTDB on lookups, joins, filtered aggregates, top-k sorts, bitemporal queries and mixed pgwire traffic.
Keep those workloads in subsequent regression runs.

## Implementation differences and evidence

### Flight SQL and transaction submission

- Both engines accept Arrow arguments and materialize rows for static INSERT expansion.
  XTDB caches the DML planning environment and substitutes parameter values into its planned rows.
  RTDB derives argument column types once, then its baseline submission path prepares and evaluates expressions per cell.
  The previous CPU profile attributed roughly 15% of samples to static expansion, including invocation preparation and expression typing.
  Sources are [RTDB Flight arguments](crates/flight-sql/src/wire.rs), [RTDB static submission](crates/tx/src/submission.rs) and [XTDB static submission](../core/src/main/clojure/xtdb/sql.clj).
- The fresh-node comparison used the same published images, a 768 MiB cache setting, and two compaction workers in the enabled condition.
  In one run per condition, RTDB and XTDB reached 107.3k and 145.2k rows/s with compaction enabled, and 116.0k and 154.6k with it disabled.
  RTDB delivered about 74% and 75% of XTDB's throughput in the two conditions.
  This supports a foreground write gap in addition to compaction pressure.
- The recovered session resumed work on skipping expression preparation for simple cells.
  Measure that change independently and profile the remaining foreground cost before broadening it to a different submission representation.

### Compaction and peak memory

- Both engines write Arrow data and metadata through the same trie maintenance model.
  XTDB copies through vector readers and writers during compaction.
  RTDB already copies some columns directly from arrays, while remaining paths materialize page events and compute statistics and Bloom filters on output.
  The previous RTDB-only profile attributed about 45% of sampled CPU to compaction, including about 8.5% in Bloom-related work and 10% in statistics.
  That sample included time after the client finished, so the percentages identify CPU consumers and do not quantify the wall-clock gap.
  Sources are [RTDB merge layout](crates/maintenance/src/merge/layout.rs), [statistics](crates/format/src/statistics.rs), [Bloom filters](crates/format/src/bloom.rs) and [XTDB output writer](../core/src/main/kotlin/xtdb/compactor/OutWriter.kt).
- In the fresh Flight experiment, RTDB's sampled anonymous memory peaked at roughly 783 MiB with compaction enabled and 258 MiB with it disabled.
  XTDB's peaks were roughly 667 and 596 MiB.
  This isolates a compaction-dependent RTDB memory increase, including work that continues after the client's last write.
- XTDB maps record-batch ranges from local files and exposes them as Arrow buffers.
  RTDB can open a whole file to discover its layout, then read page ranges once the layout is cached.
  RTDB retains decoded pages and event headers in its own cache.
  That can add transient heap copies and retained anonymous memory at scale.
  Sources are [RTDB scan loading](crates/query/src/scan.rs), [RTDB buffer pool](crates/storage/src/cache.rs) and [XTDB memory cache](../core/src/main/kotlin/xtdb/cache/MemoryCache.kt).
- RTDB's live table stores immutable Arrow chunks and merges some by copying both batches.
  XTDB appends into a table relation and takes retained slices.
  This can add foreground copy and allocation cost during ingest, and can amplify staging work during large updates.
  Sources are [RTDB live index](crates/index/src/live.rs) and [XTDB live table](../core/src/main/kotlin/xtdb/indexer/LiveTable.kt).

## Verification

Compare a current RTDB baseline, each candidate, and the pinned XTDB reference with the same limits, storage setup, data shape and synchronous commit settings.
Alternate engine order across repetitions and publish the spread alongside the median.
Run both the full sequence and fresh Flight ingest, check row counts, and account for background compaction work.
Report process RSS, anonymous memory, cgroup working set and total cgroup charge together with stage timings.
Use compaction-disabled runs to identify causes and compaction-enabled runs to judge parity.
Keep the existing correctness tests and run the relevant Rust, JVM and conformance suites for changes affecting shared behavior.

## Conformance follow-ups

- The recovered corpus had one early node stop in the health test.
  Fifteen targeted and parallel reruns did not reproduce it.
  Keep the case open for a diagnostic trace if it recurs under concurrent build and test load.
- The processed-message-ID test awaits a flush and then compares submitted and processed positions.
  Compaction can submit a subsequent message between the flush barrier and the observations.
  Both engines allow that interleaving.
  The positions should be checked against a captured target if the test is revised.
