# RTDB performance gap inventory

See the [performance comparison](XTDB-VS-RTDB.md) for a summary and the [design audit](DESIGN-DIVERGENCES.md) for the historical source analysis.

## Latest matched results

On Mac mini 2, packaged RTDB `55277dbf3` met or exceeded XTDB `c84f9334e` on all 110 evaluated throughput, latency and memory medians.
The count covers 55 measurements per size across pgwire and Flight SQL, including startup, query and write latencies, concurrent throughput, and memory.
Each engine ran twice at each size with reversed engine order across repetitions.
The runners used local logs and storage, 4 CPUs, a 4 GiB memory limit with no swap, a 768 MiB cache and two compaction workers.
Flight SQL followed pgwire on the same node.
All eight full runs passed their row-count checks.

### 100,000 rows

The 1-minute host load was 5.80–7.22 at the start of these runs.

| Metric | RTDB median [range] | XTDB median [range] |
| --- | ---: | ---: |
| pgwire ingest | 67.7k [60.8k, 74.6k] rows/s | 51.2k [50.9k, 51.4k] rows/s |
| Flight prepared INSERT | 163.1k [143.5k, 182.8k] rows/s | 104.7k [81.9k, 127.5k] rows/s |
| pgwire concurrent INSERT | 2,685 [2,670, 2,700] tx/s | 2,313 [2,121, 2,505] tx/s |
| Flight concurrent INSERT | 1,520 [1,321, 1,719] tx/s | 717 [693, 740] tx/s |
| Flight single INSERT p50 | 1.471 [1.427, 1.515] ms | 1.979 [1.539, 2.418] ms |
| pgwire UPDATE of 10% of orders | 43 [38, 47] ms | 121 [113, 129] ms |
| Flight full-scan fetch p50 | 216 [204, 228] ms | 247 [208, 287] ms |
| Flight full-scan fetch p95 | 219 [205, 232] ms | 248 [208, 288] ms |
| Peak working set during pgwire | 126 [125, 127] MiB | 736 [673, 800] MiB |
| Peak working set during Flight | 185 [184, 185] MiB | 667 [646, 687] MiB |

### 1,000,000 rows

The 1-minute host load at the start of the full runs ranged from 7.25 to 12.17.

| Metric | RTDB median [range] | XTDB median [range] |
| --- | ---: | ---: |
| pgwire ingest, four runs per engine | 46.5k [40.7k, 51.1k] rows/s | 43.9k [34.2k, 67.0k] rows/s |
| Flight prepared INSERT | 125.0k [117.3k, 132.7k] rows/s | 77.7k [42.7k, 112.6k] rows/s |
| pgwire concurrent INSERT | 1,825 [1,757, 1,893] tx/s | 1,473 [1,289, 1,656] tx/s |
| Flight concurrent INSERT | 1,429 [1,149, 1,708] tx/s | 922 [867, 977] tx/s |
| Flight single INSERT p50 | 2.148 [1.412, 2.884] ms | 3.202 [3.075, 3.328] ms |
| pgwire UPDATE of 10% of orders | 631 [599, 662] ms | 780 [699, 862] ms |
| Flight full-scan fetch p50 | 2,379 [1,994, 2,764] ms | 3,006 [2,892, 3,120] ms |
| Flight full-scan fetch p95 | 2,383 [1,998, 2,768] ms | 3,155 [2,982, 3,328] ms |
| Peak working set during pgwire | 655 [602, 708] MiB | 885 [882, 888] MiB |
| Peak working set during Flight | 583 [580, 586] MiB | 868 [811, 924] MiB |

The two full pgwire ingest pairs gave different winners.
Their medians were 46.5k rows/s for RTDB (44.5k–48.5k) and 56.0k rows/s for XTDB (45.0k–67.0k).
We repeated that phase twice per engine using the same packaged images, fresh containers, data, batch size, limits and two compaction workers.
These runs ended after the row-count check for pgwire ingest, the first operation in the full benchmark.
RTDB measured 40.7k and 51.1k rows/s, and XTDB measured 34.2k and 42.7k rows/s.
RTDB won three of the four pairs, and XTDB recorded the highest individual throughput.

### Focused diagnostics

With compaction disabled in a separate diagnostic, median pgwire ingest was 54.2k rows/s for RTDB and 50.3k rows/s for XTDB.
The server CPU range rose from 15.4–16.7 seconds to 21.2–26.4 seconds for RTDB with compaction enabled, and from 28.9–29.1 to 41.6–48.9 seconds for XTDB.
Measured cgroup I/O pressure was below 0.13 seconds during ingest in these eight focused runs.
The parity calculation uses the compaction-enabled runs.

In one fresh million-row Flight diagnostic per engine, both clients checked the row count after ingest and after each scan.
Over three alternating scans of each kind, median Arrow-table delivery was 497 ms for RTDB and 506 ms for XTDB.
RTDB returned 977 Arrow batches and XTDB returned 2,048–3,072.
Fetching Python rows took 2,943 ms for RTDB and 2,985 ms for XTDB, including about three seconds of client CPU time in both cases.
This quantifies the client materialization cost in the full-scan benchmark.
The full-run reports and phase samples are in `mini2-packaged-55277dbf3`, and the ingest-only repeats are in `mini2-pgwire-on-55277dbf3` and `mini2-pgwire-off-55277dbf3`.
These directories are under `~/.cache/rtdb-parity-ff72d7394/build/bench-followup` on Mac mini 2.

## Original benchmark gaps

The recovered full benchmark used RTDB `f97d4877f` and XTDB `c84f9334e`, local storage and logs, 4 CPUs and a 4 GiB container limit.
It reported the best of two runs per metric.
Flight SQL ran on the same node after the pgwire suite, while the reported memory peak was sampled during pgwire only.
The parity branch starts from RTDB `c67d7a04f`.
Each subsequent comparison identifies its RTDB build.

| Workload | 100k, RTDB relative to XTDB | 1M, RTDB relative to XTDB | 1M values, RTDB / XTDB |
| --- | ---: | ---: | --- |
| Flight prepared INSERT | 0.75× | 0.45× | 77.2k / 170.5k rows/s |
| pgwire batched INSERT | 1.19× | 0.86× | 46.8k / 54.4k rows/s |
| pgwire concurrent INSERT | 1.05× | 0.84× | 1,766 / 2,095 tx/s |
| pgwire UPDATE of 10% of orders | 2.21× | 0.82× | 728 / 599 ms |
| pgwire peak container working set | 5.08× | 0.49× | 1,582 / 781 MB |
| Flight full-scan fetch | 0.99× | 1.00× | 3,197 / 3,186 ms |
| Flight single INSERT | 1.14× | 0.98× | 1.74 / 1.70 ms |
| Flight concurrent INSERT | 1.41× | 0.97× | 1,221 / 1,257 tx/s |

The ratios above divide RTDB throughput by XTDB throughput, or XTDB cost by RTDB cost for latency and memory.
The same baseline favored RTDB on lookups, joins, filtered aggregates, top-k sorts, bitemporal queries and mixed pgwire traffic.
The latest comparison includes those workloads.

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
- RTDB's local object writer synchronizes the staged file and each destination directory up to the storage root.
  XTDB's local object writer writes the temporary file and publishes it with an atomic move.
  RTDB's extra storage synchronization can add foreground wait when block flush and compaction share the disk with the logs.
  Sources are [RTDB local publication](crates/storage/src/object_store.rs) and [XTDB local publication](../core/src/main/kotlin/xtdb/storage/LocalStorage.kt).
- RTDB's live table stores immutable Arrow chunks and merges some by copying both batches.
  XTDB appends into a table relation and takes retained slices.
  This can add foreground copy and allocation cost during ingest, and can amplify staging work during large updates.
  Sources are [RTDB live index](crates/index/src/live.rs) and [XTDB live table](../core/src/main/kotlin/xtdb/indexer/LiveTable.kt).

## Changes included in the measured build

The packaged benchmark measures the combined effect of these changes.

- `7455d1f99` specializes the numeric metadata loop by Arrow array type, eliminating the boxed per-cell converter.
  The format and maintenance suites passed all 170 tests, including numeric bounds under a nullable parent.
- `435e56acc` skips invocation preparation for static SQL values it copies directly.
  It keeps date admission and retains the full expression evaluator for computed values, collections and mismatched parameter schemas.
  The expression, transaction and Flight SQL suites passed all 604 tests.
- `4da15e2ba` reads compaction inputs directly from node-local backing storage.
  A real local compaction matched the fallback output bytes under a temporary disk budget smaller than the input.
  The storage and maintenance suites passed all 131 tests.
- `ff72d7394` skips the same-system-time compaction pass for ordered L0 pages with distinct IIDs and ordinary put or delete intervals.
  The maintenance suite passed all 38 tests.
- `5d0b86b41` sets the measured jemalloc configuration in the native image and verifies that the runtime loads the library.
  All 55 release tool tests passed.
- `59e463386` reuses each input's current IID while merging sorted trie rows.
  Trie lookups borrow the existing header and return its IID.
- `786f26c5d` builds one inferred schema for rows with matching ordered fields and cell types.
  It retains the existing type join for heterogeneous rows.
- `075647865` reserves schema memory once per flat Flight parameter column and accounts for each decoded value.
  The combined trie, index, Flight SQL and transaction suites passed all 166 tests for these three write-path changes.
- `c0d1f479c` uses radix sort for large batches of page Bloom positions and accounts for its temporary buffer.
  A synthetic 5,120-position sort took 7–10 µs instead of 27 µs on Apple silicon.
  The format and maintenance suites passed 173 tests, including byte-for-byte Bloom output with nulls and selected rows.
- `55277dbf3` borrows plain argument values during static expansion and preallocates decoded document maps from the selected column count.
  The index, transaction and Flight SQL suites passed all 147 tests.

## Earlier measurements

### Early million-row allocator comparison

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
The later packaged runs tested jemalloc with its background purge enabled.

One instrumented RTDB run returned a row count other than 1,000,000 immediately after Flight ingest.
The final assertion did not print the returned value.
Its throughput is excluded from the comparison.
Two subsequent full-sequence reruns on the original image passed the row count check.
The follow-up runner records the first wrong result, subsequent counts and protocol status if it recurs.

### Earlier branch measurements with glibc and default jemalloc

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

At this size, the Flight differences fell within the observed run-to-run ranges.
The million-row runs on this build showed larger differences.

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
The Mac mini 2 runs used one benchmark container at a time under the same limits.

### Mac mini 2 allocator trials

The two matched 1,000,000-row pairs used RTDB `ff72d7394`, XTDB `c84f9334e` and glibc defaults.
All four runs completed their pgwire and Flight row-count checks.
One pair started with RTDB and the other started with XTDB.
Other work was active on the host.

| Metric | RTDB median [range] | XTDB median [range] |
| --- | ---: | ---: |
| pgwire ingest | 42.8k [42.5k, 43.0k] rows/s | 40.0k [32.5k, 47.5k] rows/s |
| Flight prepared INSERT | 64.2k [38.2k, 90.3k] rows/s | 110.0k [88.9k, 131.2k] rows/s |
| pgwire concurrent INSERT | 982 [660, 1,305] tx/s | 1,195 [1,138, 1,252] tx/s |
| Flight concurrent INSERT | 856 [621, 1,092] tx/s | 700 [613, 787] tx/s |
| pgwire UPDATE of 10% of orders | 1,187 [848, 1,527] ms | 800 [732, 868] ms |
| Flight full-scan fetch p50 | 3,145 [2,754, 3,536] ms | 3,429 [3,117, 3,741] ms |
| Flight full-scan fetch p95 | 3,310 [2,779, 3,842] ms | 3,561 [3,284, 3,839] ms |
| Peak working set during pgwire | 1,452 [1,450, 1,454] MiB | 747 [723, 771] MiB |
| Peak working set during Flight | 1,848 [1,656, 2,040] MiB | 787 [768, 806] MiB |

The slower RTDB Flight run used 26.2 seconds of wall time and 18.6 CPU seconds, compared with 11.1 seconds and 16.6 CPU seconds in the faster run.
Both showed zero CPU quota throttles during ingest.
Peak cgroup charge during Flight ingest was 3,250 MiB in the slower run and 3,682 MiB in the faster run.
The wall-time spread makes a single run unreliable for judging parity.
At 100,000 rows, the same two-pair comparison favored RTDB on pgwire ingest (41.0k versus 35.4k rows/s), the 10% UPDATE (80 versus 157 ms) and peak working set (124 versus 705 MiB during pgwire, 207 versus 645 MiB during Flight).
In the glibc trial, Flight ingest was 78.0k versus 96.1k rows/s and full-scan p50 was 339 versus 299 ms.
Flight single-row INSERT p95 was 3.4–12.2 ms versus 3.7–3.8 ms at 100,000 rows.
At 1,000,000 rows pgwire single-row INSERT p95 was 2.6–61.1 ms versus 2.8–3.4 ms, and the median DELETE p95 trailed on both protocols with overlapping ranges.

Two further runs per size preloaded jemalloc into the same RTDB image, with a background purge thread, one-second dirty-page decay and zero muzzy-page decay.
They ran after the glibc comparison on the same host.
The ranges below show both runs.

| Rows | Metric | RTDB with jemalloc | XTDB |
| ---: | --- | ---: | ---: |
| 100k | Flight prepared INSERT | 86.6k [72.5k, 100.8k] rows/s | 96.1k [89.0k, 103.2k] rows/s |
| 100k | Flight full-scan fetch p95 | 297 [282, 312] ms | 305 [288, 323] ms |
| 1M | Flight prepared INSERT | 108.0k [106.3k, 109.7k] rows/s | 110.0k [88.9k, 131.2k] rows/s |
| 1M | pgwire ingest | 47.1k [46.4k, 47.8k] rows/s | 40.0k [32.5k, 47.5k] rows/s |
| 1M | pgwire concurrent INSERT | 1,595 [1,339, 1,852] tx/s | 1,195 [1,138, 1,252] tx/s |
| 1M | pgwire UPDATE of 10% of orders | 675 [594, 756] ms | 800 [732, 868] ms |
| 1M | Peak working set during pgwire | 641 [622, 659] MiB | 747 [723, 771] MiB |
| 1M | Peak working set during Flight | 562 [539, 586] MiB | 787 [768, 806] MiB |

The same configuration improved the observed million-row memory and pgwire results in both runs.
The glibc run with a 61.1 ms pgwire single-INSERT p95 was followed by 3.09 ms and 2.12 ms with jemalloc.
In these trials, 100,000-row Flight ingest trailed the reference and the million-row Flight ingest median fell within the measured XTDB range.
The native release image built from `5d0b86b41` on Mac mini 2 loaded the packaged allocator and passed both build-information checks.
The experimental and packaged libraries are different builds of the same upstream jemalloc 5.3.0 revision.
Both report the same effective purge settings.

### Packaged allocator baseline

Two fresh full runs per engine compared packaged RTDB `5d0b86b41` with XTDB `c84f9334e` at each size on Mac mini 2.
All eight runs completed the row-count checks and reported results without client thread failures.
Host load averaged about 21 at the start and 11 near the end on a 12-logical-CPU machine.

| Rows | Metric | Packaged RTDB median [range] | XTDB median [range] |
| ---: | --- | ---: | ---: |
| 100k | pgwire ingest | 30.1k [25.8k, 34.4k] rows/s | 19.4k [16.2k, 22.6k] rows/s |
| 100k | Flight prepared INSERT | 44.8k [35.1k, 54.4k] rows/s | 29.3k [19.0k, 39.7k] rows/s |
| 100k | Flight single INSERT p95 | 31.0 [18.9, 43.2] ms | 13.1 [11.5, 14.7] ms |
| 100k | pgwire concurrent INSERT | 446 [193, 699] tx/s | 616 [594, 637] tx/s |
| 1M | pgwire ingest | 34.7k [25.3k, 44.2k] rows/s | 37.3k [26.9k, 47.8k] rows/s |
| 1M | Flight prepared INSERT | 83.6k [61.1k, 106.2k] rows/s | 115.6k [94.6k, 136.6k] rows/s |
| 1M | pgwire UPDATE of 10% of orders | 715 [607, 822] ms | 789 [635, 943] ms |
| 1M | Flight concurrent INSERT | 627 [534, 720] tx/s | 996 [909, 1,083] tx/s |
| 1M | Flight full-scan fetch p50 | 4,316 [4,276, 4,356] ms | 2,796 [2,609, 2,983] ms |
| 1M | Peak working set during pgwire | 707 [660, 753] MiB | 901 [900, 902] MiB |
| 1M | Peak working set during Flight | 607 [607, 607] MiB | 838 [755, 920] MiB |

All four million-row Flight runs showed zero container CPU quota throttles during ingest, full-scan fetch and single INSERT.
The two RTDB full-scan phases took 20.9–24.2 seconds and used 4.4–4.7 server CPU seconds.
XTDB took 13.1–15.0 seconds and used 3.0–4.7 server CPU seconds.
The [focused Flight diagnostic](#focused-diagnostics) measured Arrow delivery and Python row fetching on the later build.

### Flight and UPDATE CPU samples

Phase-specific sampling on Mac mini 2 used RTDB `ff72d7394` with jemalloc and restricted each recording to the measured operation.
With compaction disabled, fresh Flight ingest produced 1,043 samples, including 3.2% self time in `LiveTable::header` and 2.6% in trie merge.
With compaction enabled, the 1,385 samples included 4.9% in an unstable quicksort specialization and 2.1% directly in the two Bloom insertion routines.
The samples identify sorting and Bloom construction as active costs, while the quicksort symbol can include other callers.
During the set-based UPDATE, 474 samples put 2.3% self time in the Arrow cell allocation estimator and another 2.3% in one of the hash writer specializations.
Repeated document maps and live-trie lookups add work even without the compactor.

## Correctness checks

The Gradle test and property suites passed all 6,535 tests after the watermark check changed, including 398 native binding tests.
The 398 native binding tests also passed against a rebuilt library containing the write-path candidates.
The Rust replication suite passed all 60 tests.
All 59 tests in the Rust node's native integration suite passed.
Two Rust search tests failed during compaction with an Arrow page schema mismatch on both the parity branch and its `c67d7a04f` baseline.
The later fix on `main` (`dd7fe5232`) adds a regression test for that schema mismatch and postdates these checks and the performance measurements.

## Conformance follow-ups

- The recovered corpus had one early node stop in the health test.
  Fifteen targeted and parallel reruns did not reproduce it.
  Keep the case open for a diagnostic trace if it recurs under concurrent build and test load.
- The original processed-message-ID test compared two independently sampled live positions after a flush.
  The JVM Flight SQL suite reproduced the race with submitted position 2 and processed position 4.
  The branch test captures submitted positions before the flush and checks that processing has reached each captured position.
  Both engines permit background compaction to advance the processed watermark during the observations.
