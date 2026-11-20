# RTDB performance parity with XTDB

The goal is parity or better across the matched pgwire and Flight SQL workloads at 100,000 and 1,000,000 rows, with transaction, query and persisted-format correctness preserved.
Track throughput, p50 and p95 latency, and peak memory under matched conditions.
The results below describe specific builds and workloads.

## Baseline

The recovered full benchmark used RTDB `f97d4877f` and XTDB `c84f9334e`, local storage and logs, 4 CPUs and a 4 GiB container limit.
It reported the best of two runs per metric.
Flight SQL ran on the same node after the pgwire suite, while the reported memory peak was sampled during pgwire only.
The parity branch starts from RTDB `c67d7a04f`.
The follow-up runs below use that revision or an explicitly named candidate.

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

## One-million-row follow-up

The table shows single sequential runs of the full benchmark, using the old RTDB and XTDB images and two compaction workers.
The jemalloc run used the same RTDB image with jemalloc preloaded and a background purge enabled.
Each row count check passed in these three runs.

| Metric | RTDB, glibc | XTDB | RTDB, jemalloc |
| --- | ---: | ---: | ---: |
| pgwire ingest | 36.5k rows/s | 55.0k rows/s | 48.4k rows/s |
| Flight ingest after pgwire | 95.0k rows/s | 151.9k rows/s | 103.5k rows/s |
| pgwire concurrent INSERT | 1,892 tx/s | 1,839 tx/s | 992 tx/s |
| Flight concurrent INSERT | 1,354 tx/s | 1,234 tx/s | 824 tx/s |
| pgwire UPDATE of 10% of orders | 740 ms | 640 ms | 578 ms |
| Peak working set during pgwire | 1,330 MiB | 690 MiB | 618 MiB |
| Peak working set during Flight | 1,650 MiB | 1,056 MiB | 659 MiB |

During early pgwire ingest in a separate allocator trace, the engine reported about 1.5 MiB of cache reservations as anonymous memory reached 708 MiB, with glibc reporting about 706 MiB of free arena space.
During the read workload the cache reservation approached 768 MiB, while glibc still reported about 1.8 GiB in free arena blocks and anonymous memory exceeded 2 GiB.
Cache reservations bound admission and are an estimate of allocations, while glibc reports allocator accounting and cgroups report residency.
The controlled allocator swap supports allocator retention as a major contributor to the observed memory gap.
A further run with glibc restricted to two arenas reached 663 MiB during pgwire and 618 MiB during Flight.
It handled only 290 pgwire concurrent writes per second, compared with 1,892 on the default setup.
Repeat the jemalloc comparison with current builds and investigate the concurrent-write regression before adopting an allocator setting.

One instrumented RTDB run returned a row count other than 1,000,000 immediately after Flight ingest.
The final assertion did not print the returned value.
Its throughput is excluded from the comparison.
Two subsequent full-sequence reruns on the original image passed the row count check.
The follow-up runner records the first wrong result, subsequent counts and protocol status if it recurs.

## Candidate changes

- `7455d1f99` specializes the numeric metadata loop by Arrow array type, eliminating the boxed per-cell converter.
  The format and maintenance suites passed all 170 tests, including numeric bounds under a nullable parent.
  End-to-end impact remains unmeasured.
- `435e56acc` skips invocation preparation for static SQL values it copies directly.
  It keeps date admission and retains the full expression evaluator for computed values, collections and mismatched parameter schemas.
  The expression, transaction and Flight SQL suites passed all 604 tests.
- `4da15e2ba` reads compaction inputs directly from node-local backing storage.
  A real local compaction matched the fallback output bytes under a temporary disk budget smaller than the input.
  The storage and maintenance suites passed all 131 tests.
- `ff72d7394` skips the same-system-time compaction pass for ordered L0 pages with distinct IIDs and ordinary put or delete intervals.
  The maintenance suite passed all 38 tests.
  Both compaction changes need matched throughput and memory measurements.

## Current branch measurements

Two matched runs per engine at 100,000 rows compared RTDB `435e56acc` with XTDB `c84f9334e`.
The containers used glibc defaults on RTDB, the same 4 CPU and 4 GiB limits, and two compaction workers.
Ranges are the two observed per-run values.

| Metric | RTDB median [range] | XTDB median [range] | RTDB relative to XTDB |
| --- | ---: | ---: | ---: |
| pgwire ingest | 50.8k [50.3k, 51.3k] rows/s | 40.8k [40.7k, 40.9k] rows/s | 1.25× |
| Flight prepared INSERT | 118.1k [112.5k, 123.7k] rows/s | 120.2k [114.6k, 125.8k] rows/s | 0.98× |
| Flight full-scan fetch p50 | 324.8 [321.3, 328.2] ms | 320.9 [316.6, 325.2] ms | 0.99× |
| Peak working set during pgwire | 126 [116, 136] MiB | 674 [662, 687] MiB | 5.36× less |
| Peak working set during Flight | 200 [196, 203] MiB | 674 [650, 699] MiB | 3.38× less |

At this size, the remaining Flight differences are within the observed run-to-run ranges.
At 1,000,000 rows the same two-per-engine comparison still has open gaps.

| Metric | RTDB median [range] | XTDB median [range] | RTDB relative to XTDB |
| --- | ---: | ---: | ---: |
| pgwire ingest | 47.4k [45.1k, 49.8k] rows/s | 53.6k [52.3k, 54.8k] rows/s | 0.89× |
| Flight prepared INSERT | 85.8k [59.0k, 112.7k] rows/s | 139.0k [131.3k, 146.8k] rows/s | 0.62× |
| pgwire concurrent INSERT | 1,561 [1,279, 1,842] tx/s | 1,628 [1,393, 1,864] tx/s | 0.96× |
| pgwire UPDATE of 10% of orders | 786 [719, 853] ms | 621 [610, 631] ms | 0.79× |
| Peak working set during pgwire | 1,347 [1,299, 1,394] MiB | 721 [684, 757] MiB | 1.87× as much |
| Peak working set during Flight | 1,937 [1,833, 2,040] MiB | 793 [762, 823] MiB | 2.44× as much |

The slower RTDB Flight run took 17.0 s against 8.9 s for the faster run, while cgroup CPU time rose from 12.1 to 14.8 s.
Both had zero CPU quota throttles during Flight ingest.
The slower run charged up to 3,815 MiB to the cgroup during Flight ingest, versus 3,519 MiB in the faster run.
Allocator and compaction comparisons are needed to explain the timing spread.
Both compaction changes landed after these images were built.

Reusing `435e56acc` with default jemalloc gave these two-run medians.

| Metric | 100k RTDB, default jemalloc | 1M RTDB, default jemalloc | 1M XTDB |
| --- | ---: | ---: | ---: |
| pgwire ingest | 54.3k rows/s | 39.8k rows/s | 53.6k rows/s |
| Flight prepared INSERT | 131.4k rows/s | 89.6k rows/s | 139.0k rows/s |
| Peak working set during pgwire | 173 MiB | 879 MiB | 721 MiB |
| Peak working set during Flight | 261 MiB | 875 MiB | 793 MiB |
| pgwire UPDATE of 10% of orders | 50 ms | 690 ms | 621 ms |

At 100,000 rows the only ratio below 1 in this set was Flight full-scan p95, with a 0.5% difference and overlapping observed ranges.
At 1,000,000 rows default jemalloc lowered memory and narrowed the update gap.
Flight ingest remained well behind XTDB, and pgwire ingest was slower than with glibc in this sample.
The system was shared with other Docker and host work during portions of these measurements.
The next comparison uses a dedicated Colima profile and retains the same limits inside each container.

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
