# Bitemporal full-text search architecture experiments

The experiments compare changes to ranking, corpus statistics, and index layout against RTDB at `1c8d111ef`.
RTDB's log, snapshot, and temporal resolver remain authoritative for search.
The first candidate is a direct ranked search operator over the existing index files.
A derived search generation with its own compaction policy is a second candidate whose value depends on measured file fan-out and write amplification.

## What the baseline does

The ranker scores candidates against a snapshot corpus and passes a set of IIDs to the ordinary scan.
That scan can materialize a full-file MATCH bitmap on a cache miss, even for a small set of ranked IIDs.
Obtaining document frequencies still enumerates term postings across files, and the first call through a table corpus calculates them twice.
On a cold path with no local file, the runtime currently requests the complete search bundle.
These are separate costs and each needs its own measurement.
[Ranker and search answers](../../rust/crates/search/src/accelerate.rs) · [Scan handoff](../../rust/crates/query/src/scan.rs) · [Bundle reads](../../rust/crates/search/src/runtime.rs)

The previous benchmark compared correctness fixes using 100,000 synthetic documents, five indexed tries, and repeated queries on an Apple M4 Max.
Broad top-20 took approximately 44 ms with one MATCH and 52 ms with two, while selective top-20 took about 3.3 ms.
The benchmark used warm indexes and in-memory object storage.
It establishes a useful comparison workload for the warm path.

## Lessons from other systems

- **SlateDB** pins immutable storage state and the WAL boundary together.
  Its SST readers use block offsets to load individual ranges.
  That pattern fits a derived search generation published under RTDB's existing basis and retained for readers that pin the generation.
  [Checkpoint reader](https://github.com/slatedb/slatedb/blob/9aedb40017e4985dbde3702b9f75a82866a69a81/slatedb/src/db_reader.rs#L54-L105) · [SST block reader](https://github.com/slatedb/slatedb/blob/9aedb40017e4985dbde3702b9f75a82866a69a81/slatedb/src/sst_reader.rs#L254-L328)
- **Quickwit** opens immutable split footers, reads query-specific ranges, and cancels warmup when it can prove a required term absent.
  It publishes staged and replaced splits with the source checkpoint.
  We can test the read strategy with the existing Tantivy artifacts and test independent publication separately.
  [Split reader](https://github.com/quickwit-oss/quickwit/blob/bde6c855cb2838ab848cc23414836aef0508efd5/quickwit/quickwit-search/src/leaf.rs#L129-L195) · [Warmup](https://github.com/quickwit-oss/quickwit/blob/bde6c855cb2838ab848cc23414836aef0508efd5/quickwit/quickwit-search/src/leaf.rs#L306-L397) · [Publisher](https://github.com/quickwit-oss/quickwit/blob/bde6c855cb2838ab848cc23414836aef0508efd5/quickwit/quickwit-indexing/src/actors/publisher.rs#L234-L275)
- **Chroma** evaluates compacted records and a materialized log separately and excludes updated compacted IDs.
  An RTDB overlay must resolve the affected IID's relevant history or mask the affected rectangles because a correction to June can leave May visible.
  [Filter operator](https://github.com/chroma-core/chroma/blob/1a3ac2c7c623adbd03ed2508dc27b98cdf26016d/rust/worker/src/execution/operators/filter.rs#L846-L875)
- **Turbopuffer** found a median of about 1.5 postings per fragment when posting lists followed vector clusters, then moved to blocks targeting roughly 256 postings.
  Its later work keeps postings batched through scoring and fetches corpus statistics, postings, and filter data concurrently.
  Measure RTDB's postings per term per file and object request dependencies before choosing a block size or a different compaction policy.
  [Posting layout](https://turbopuffer.com/blog/fts-v2-postings) · [Batched scoring and cold reads](https://turbopuffer.com/v3)

## Variants

| Variant | Change | Comparison |
| --- | --- | --- |
| Baseline | Current trie-scoped Tantivy artifacts, corpus assembly, and scan handoff | Records the costs of filtering, scoring, verification, and fetching. |
| Corpus control | Calculate a score's term statistics once and avoid building complete posting vectors when cached counts and membership lookups suffice | Isolates avoidable work without changing index ownership. |
| Narrowed MATCH | Check postings only for the selected IIDs on a bitmap cache miss | Isolates predicate verification after ranked narrowing. |
| Direct ranked hits | Return verified row versions and their scores for eligible point queries, and fetch requested fields after top-k selection | Isolates the scan handoff and late materialization. |
| Corpus checkpoint | Store exact document count, rounded length sum, and per-term frequencies at a basis and apply exact deltas to the requested basis | Measures query savings against checkpoint and overlay maintenance. |
| Range reads | Fetch the relevant parts of existing immutable bundles through a range-capable object store | Isolates cold byte amplification and serial requests. |
| Independent search generations | Index a bounded, known transaction prefix in search-sized immutable segments and combine it with an exact overlay through the read basis | Measures the effect of file layout and compaction policy separately from the query operator and range reader. |

A verified hit, a pinned search generation, and a corpus checkpoint are passive values.
A hit carries the resolved row version, score, and any proof needed to reuse a predicate result.
A generation owns its immutable coverage, definition, and file inventory as one published value.
The corpus checkpoint owns its basis, visible-value statistics, and enough forward data to retract replaced contributions.
The builder is a single-writer transient and publishes through the existing RTDB maintenance and log machinery.
An alternate read path uses existing snapshot lifetimes and falls back to the canonical path when coverage, visibility, or ordering cannot be established.

## Correctness and measurement

Check ordered row versions and exact scores against exhaustive temporal resolution with independently computed corpus totals and the same field-norm rounding.
A scan configured without search acceleration can select the right rows without establishing the reference BM25 corpus.
For each indexed column, count visible non-null values, including non-text values with zero tokens, and count each term at most once per document.

Rows are selected at the query's valid and system bounds.
Default valid time comes from the query clock.
BM25 statistics use the snapshot's own instant on both axes, including for historical queries.
Advance the basis across a scheduled valid-time boundary using an unrelated transaction and check the changed statistics.
Check backdated corrections, overlapping intervals, same-system-time writes, erases, transaction-local rows, phrases, multiple predicates, score ties and SQL tie-breaks, pinned readers across compaction, and missing search coverage.
Check temporal ranges for row-version multiplicity.
[Full-text search contract](fts.allium) · [Live index snapshots](../../allium/live-index.allium)

Run paired warm and cold measurements with identical indexed text and increasingly fragmented files at a fixed document count.
Repeat with sparse and dense matches, varied scores and many ties, new and repeated query terms, and varying rates of temporal corrections.
Record p50 and p95 latency, postings and windows visited, temporal rechecks, field materialization, fallback rates, object requests and bytes, checkpoint lag, and data and search bytes written.
Use the same freshness and exactness semantics for every variant.

## Decision rule

Keep the smallest design that removes the measured bottlenecks.
A direct operator and exact checkpoint can improve execution with the existing trie ownership.
Independent search compaction earns its additional state and retention work if fan-out, cold I/O, or redundant rewriting remain material after those changes.

## Experiment results

### Corpus control on the warm baseline workload

Compare baseline `1c8d111ef` with corpus control `356643715`.
Both binaries used the locked Rust release profile on the same Apple M4 Max.
Each build ran four fresh 100,000-document datasets with five indexed tries, 24 timed queries per case per dataset, and alternating build order.
All 96 observations per case per build matched their expected results.

| Query | Baseline p50 | Corpus control p50 | Paired p50 change |
| --- | ---: | ---: | ---: |
| Count all | 6.39 ms | 6.34 ms | −0.8% |
| Count rare matches | 6.13 ms | 6.06 ms | −1.5% |
| Count common matches | 17.43 ms | 17.32 ms | −1.0% |
| Top 20, one MATCH | 43.98 ms | 43.33 ms | −1.6% |
| Top 20, selective MATCH | 3.30 ms | 3.10 ms | −5.9% |
| Top 20, two MATCH predicates | 52.15 ms | 51.11 ms | −2.0% |

The selective case improved in all four pairs, with p50 changes from −8.9% to −4.8%.
The broad cases showed smaller changes whose paired ranges included zero.
The corpus regression exercises overlapping files and successive snapshot instants, including activation and expiry of a scheduled valid-time interval.

### Narrowed MATCH control

Compare corpus control `356643715` with sparse MATCH `f9fe174e8` using the same paired benchmark.
The candidate checks postings for the selected IIDs when a scan has narrowed to at most 128 IIDs and a full bitmap is unavailable.
All 96 observations per case per build matched the oracle.
The median paired p50 change was +0.7% for selective top-20, +1.2% for broad top-20, and +1.8% for two MATCH predicates.
Each paired range crossed zero and one baseline run had elevated latency across all six cases.
This run gives no repeatable warm latency gain for sparse MATCH.
The exhaustive reference query warms each query's full-file bitmap, so these measurements cover hits in that cache.
The search regression checks the sparse path directly with phrase positions and unavailable answers for unselected rows.

### Exact snapshot corpus checkpoint

The checkpoint harness builds an IID-to-contribution map from a real SQL snapshot at `(basis, basis)`.
Each transition retracts the old contributions for changed IIDs and valid-time boundaries crossed, then reads authoritative point values for those IDs and adds their contributions.
The test supplies the log's touched IDs and the schedule explicitly.
It independently scans the whole visible corpus after every transition and compares all document counts, rounded lengths, and term frequencies.
It also checks exact ordered IDs and BM25 scores for current and historical results at each basis.

At 100,000 background documents, initial capture took 149 ms and its JSON representation occupied 4.87 MB.
The harness restored that checkpoint from disk, replayed serialized deltas, and checked the restored contributions and statistics after each update.

| Touched IIDs | Delta maintenance | Serialized delta | Full corpus rebuild |
| ---: | ---: | ---: | ---: |
| 1–3 | 0.20–0.41 ms | 60–156 B | 117–120 ms |
| 10 | 0.62 ms | 631 B | 118 ms |
| 100 | 13.93 ms | 5,961 B | 122 ms |
| 1,000 | 161.71 ms | 60,971 B | 119 ms |

Delta timing includes native SQL fetch and mutation of the in-memory corpus.
Serialization and replay checks run separately.
The batch uses SQL disjunctions of up to 128 ID predicates and exceeds the cost of a full rebuild at 1,000 changes in this run.
A production checkpoint needs a bulk IID lookup and a measured policy for replay versus rebuilding.
The optimized trie-scoped query path already avoids a fresh exhaustive SQL corpus scan.
The transitions cover a future valid interval starting and expiring through unrelated transactions, a backdated partial correction, deletion, erasure, and null, absent, and non-text values.
A same-transaction overwrite exposed an existing live-corpus ordering error.
The primary scan selected the latest appended version while the corpus counted the earlier version's terms.
The fix agrees with the live trie order and has a regression comparing exact BM25 scores before and after compaction.
[Live trie ordering](../../allium/memory-hash-trie.allium) · [Corpus regression](../../rust/crates/node/tests/search.rs)

### Verified ranked hits and independent generations

The generation harness loads rows from a native SQL point snapshot, builds immutable Tantivy bundles separately from primary compaction, and retains the resolved row identities and payloads with the generation.
It orders exact scores by descending BM25 and ascending SQL ID, and materializes only the winning rows.
A second measurement fetches the projected fields for those verified IDs through the ordinary primary scan.
Each measurement checks the complete top-20 result, including the system-time version and projected fields, against the unlimited SQL query.
It checks broad, selective, and phrase predicates with score ties.

A matched run at five files measured 16 queries per case.
The direct path used exact statistics already in memory.
The primary-fetch measurement includes a separate native SQL query over the winning IIDs at the same basis, with the ordinary payload scan.
The covered path times ranking and result construction from the pinned projection.

| Query | Native SQL p50 / p95 | Direct with primary fetch p50 / p95 | Direct with pinned payload p50 / p95 |
| --- | ---: | ---: | ---: |
| Broad | 45.33 / 48.28 ms | 15.08 / 15.85 ms | 14.12 / 14.55 ms |
| Selective | 2.57 / 3.38 ms | 2.13 / 2.29 ms | 0.76 / 1.03 ms |
| Phrase | 1.97 / 2.19 ms | 2.00 / 2.48 ms | 0.73 / 0.83 ms |

Broad p50 fell by 67% with the primary fetch included.
The small selective and phrase cases spent much of the remaining time serving that fetch.

The generation is pinned to one valid-time instant and a known system basis.
At a newer basis, the overlay reads the authoritative versions of touched IIDs at that same valid instant.
It shadows the base entries for those IIDs, including erases, and leaves all other entries intact.
The harness refuses a newer basis without an overlay covering the interval.
It validates both the pinned old reader and the new reader before and after a real primary flush, compaction, and search-artifact build.
The four-IID update includes a backdated correction over a bounded interval, an erase, another replacement, and a null value.
Overlay admission uses touched IDs supplied by the test.
Production publication, retention, and log-driven coverage would require additional integration.
Arbitrary temporal ranges, transactional scans, and plans with residual predicates continue to use the SQL search path in this experiment.

### Range-addressed reads and fan-out

Three runs on 100,000 documents held the corpus and point results fixed and varied the number of independently built search bundles.
Each run measured 16 queries per case.
Cold measurements open fresh readers with empty application caches over the local filesystem.
The OS page cache may serve reads.
A whole-bundle open reads each object once.
The range reader opens the same format and caches 64 KiB blocks for the duration of the query.
The figures measure local execution and transferred bytes on each read path.

| Search files | Selective warm p50 / p95 | Selective cold whole p50 / p95 | Selective cold range p50 / p95 | Whole reads / bytes | Range reads / bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 0.76 / 0.97 ms | 19.52 / 20.62 ms | 20.27 / 25.41 ms | 5 / 11.72 MB | 172 / 11.06 MB |
| 25 | 2.39 / 3.33 ms | 22.58 / 64.22 ms | 23.65 / 127.09 ms | 25 / 11.89 MB | 200 / 11.89 MB |
| 100 | 6.76 / 7.37 ms | 29.00 / 41.97 ms | 29.18 / 41.41 ms | 100 / 12.45 MB | 201 / 12.45 MB |

The same selective query scored 991 documents at all three file counts.
The broad query scored all 100,000 documents in each layout, including equal-score candidates that could win the SQL ID tie-break.
Its five-file warm ranking p50 was 13.88 ms with a pinned payload map.
Its five-file cold p50 was 32.54 ms for whole bundles and 33.24 ms for range reads.
The five-file phrase query saved 1.7% of transferred bytes with range reads.
The 25- and 100-file layouts transferred the full bundle bytes with both read strategies.
The sidecar is loaded as a single entry and reader construction touches multiple index components.
Meaningful cold savings likely require a separately addressable sidecar and batched query-directed prefetch.

### Search writes after four changed IIDs

| Layout | Initial generated search bytes | Replace affected files | Append one immutable overlay |
| --- | ---: | ---: | ---: |
| 5 files | 11,720,294 | 4,716,892 | 2,189 |
| 25 files | 11,886,950 | 1,922,688 | 2,189 |
| 100 files | 12,445,505 | 497,969 | 2,189 |

The overlay includes a small new search bundle and keeps the changed-IID mask in memory.
Replacing four affected shards is costly even when it reduces file count for search.
These numbers cover the immediate append and rewrite alternatives.
They exclude later overlay consolidation, manifest metadata, and retained generations.
With the same transaction, the existing trie-owned path added 2,027 bytes of search artifacts, 7,298 bytes of Arrow data, and 11,866 bytes of metadata during flush and compaction.
Those totals count new object keys and keep the old keys available for retained snapshots.
Independent base bundle sizes represent the point projection and exclude primary data and history, so their sizes do not represent total database storage.

### Reproducing the runs

The benchmark example uses real RTDB writes, snapshot queries, compaction, and the bundled search reader.
[Benchmark source](../../rust/crates/node/examples/fts_architecture_bench.rs)
The first command runs the merge workload with six timed query shapes.
The second runs checkpoint capture and replay under temporal churn.
The third compares the native SQL path and direct ranking at three file counts, with warm and reader-cold measurements.

```sh
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --example fts_architecture_bench -- 100000 24 0
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --example fts_architecture_bench -- checkpoint 100000
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --example fts_architecture_bench -- generations 100000 16
```

## Architecture decision

Keep the bitemporal resolver and the trie-owned search artifacts.
The best next change is a verified ranked-hit path that carries the winning row version and exact score into late primary materialization.
The benchmark demonstrates that execution shape at a fixed point with pinned coverage.
A production operator needs version locators, snapshot pinning, and an eligibility check for predicates, ranges, and SQL ordering.

Keep the corpus control and develop checkpointed statistics against the same snapshot basis.
Exact deltas need both log-derived touched IIDs and a validity boundary schedule.
Measure cold cache misses before making the sparse MATCH optimization a default.

Keep whole-bundle reads with the current format.
The five-file run transferred 94% of the whole-bundle bytes for selective ranking and needed 34 times as many reads.
Higher fan-out compounds reader overhead, so continue measuring the number of search files touched on real workloads.
Independent generation publication becomes worth its maintenance and retention costs when production fan-out regularly exceeds what primary compaction keeps small.
The four-IID update showed almost identical immediate search bytes for the present trie path and an appended overlay.

## Verification

The locked Rust workspace suite passed 4,864 tests, with 3 ignored.
The focused release run passed all 49 cases, including the same-transaction BM25 and ranged-reader regressions.
The full Gradle unit rerun passed all 3,187 cases.
The first Gradle run hit a five-second JVM compaction sync timeout, and that case subsequently passed in isolation and in the full rerun.
The benchmark harnesses also passed their independent result and score oracles on the release build.

## Follow-up on the same trie files

The follow-up compares the native ranked SQL plan with two verified-hit prototypes over a single running node.
All three use the same immutable trie files, snapshot basis, and corpus registry implementation.
The prototypes return exact index scores and fetch the projected fields from the primary table at the same valid and system instants.
The harness captures the integer SQL IDs and their IIDs from that table and checks the correspondence before ranking.
It times this capture separately.
The harness runs an unlimited SQL ordering as the oracle and compares every projected value, including the row's system time.
The experimental paths are enabled through the benchmark feature.

The first prototype uses the existing score-only window bounds and applies the SQL tie-break after scoring.
The second uses a window's lowest SQL ID and the maximum exact contribution from each scoring term's observed frequency and field length pairs.
The resulting upper bound is for the same score and ascending ID order the SQL query uses.
The observed pairs give an upper bound without the current summaries' upward rounding margin, so an equal score can be pruned when the window's lowest ID is too high.
The experiment reports first-use preparation separately from steady-state ranking.
The prototype covers a single MATCH and integer ID ordering at a valid-time and system-time point.
Ambiguous visibility, live changes, incomplete index coverage, and uncovered IDs use the native SQL path.

```sh
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --features fts-experiments --example fts_architecture_bench -- same-files 90000 12
```

### Follow-up results

Each variant used the same files within a dataset.
The 90,000-, 110,000-, and 1,000,000-document datasets used 5, 6, and 50 primary tries, backed by 2, 3, and 14 search bundles.
Each run interleaved 12 measurements per case and variant.
The first two sizes each ran twice on freshly built datasets.
All 900 timed queries matched their SQL reference, including fallbacks.

| Documents | Query | Native SQL p50 / p95, ms | Direct, score bounds p50 / p95, ms | Direct, score and ID bounds p50 / p95, ms | Documents scored with ID bounds |
| ---: | --- | ---: | ---: | ---: | ---: |
| 90,000 | Uniform scores, top 20 | 244.98 / 258.24 | 20.12 / 20.43 | 1.96 / 2.14 | 3,584 |
| 90,000 | Uniform scores, top 100 | 213.46 / 252.58 | 32.78 / 50.81 | 17.46 / 25.65 | 15,872 |
| 90,000 | Dense, 12 score groups | 36.72 / 37.57 | 20.20 / 20.59 | 7.98 / 8.31 | 32,256 |
| 90,000 | Selective conjunction | 1.86 / 2.06 | 1.44 / 1.68 | 1.42 / 1.52 | 793 |
| 90,000 | Selective phrase | 0.94 / 1.14 | 1.27 / 1.40 | 1.37 / 1.56 | 179 |
| 110,000 | Uniform scores, top 20 | 270.05 / 300.83 | 301.19 / 318.53 | 2.80 / 2.90 | 3,584 |
| 110,000 | Uniform scores, top 100 | 265.82 / 269.10 | 296.55 / 301.94 | 20.22 / 20.97 | 15,872 |
| 110,000 | Dense, 12 score groups | 266.83 / 268.42 | 297.84 / 299.32 | 11.75 / 12.06 | 32,256 |
| 110,000 | Selective conjunction | 2.92 / 3.19 | 1.86 / 2.03 | 1.76 / 1.84 | 793 |
| 110,000 | Selective phrase | 1.39 / 1.43 | 1.50 / 1.63 | 1.62 / 1.69 | 218 |
| 1,000,000 | Uniform scores, top 20 | 2,695.99 / 2,752.45 | 2,790.78 / 2,833.41 | 9.50 / 9.93 | 3,072 |
| 1,000,000 | Uniform scores, top 100 | 2,784.21 / 3,507.38 | 2,822.05 / 3,436.28 | 46.02 / 50.70 | 14,336 |
| 1,000,000 | Dense, 12 score groups | 2,725.57 / 2,823.19 | 2,791.78 / 2,863.11 | 35.75 / 36.62 | 38,912 |
| 1,000,000 | Selective conjunction | 113.68 / 115.93 | 30.88 / 31.78 | 14.11 / 17.60 | 3,169 |
| 1,000,000 | Selective phrase | 19.31 / 32.12 | 11.54 / 11.90 | 6.30 / 7.37 | 634 |

At 90,000 documents, the native SQL ranker handed all 90,000 tied candidates to the scan.
The score-only prototype scored all 90,000 and fetched just the winners.
The tie-aware path scored 3,584 in 28 windows for the same uniform top-20 result.
At 110,000 and 1,000,000 documents, the native SQL ranker and the score-only prototype hit the candidate limit on both broad shapes.
The score-only prototype then ran the native SQL plan, which accounts for the higher fallback latency in that column.
It had already scored 105,024 documents in the million-document run when a window batch crossed its cap.
With score and ID bounds, uniform top-20 at one million documents visited 24 windows and scored 3,072 documents.
The row count and amount of candidate work still depend on the score distribution and the limit.

The million-document run spent 667 ms once on SQL ID capture and verification.
The first uniform tie-aware query took 61 ms, including 50 ms to build its window summaries from warm search files.
The report and audit summaries retained about 1.25 MB in total across the 14 bundles, excluding the separate ID map.
The exact score envelopes are derived from the postings once per file and term and reused across queries.
These costs describe local execution with search files and corpus caches warmed by the reference query.

The oracle checked IDs, exact scores, projected fields, and the row's system time.
The additional temporal probes covered backdated corrections, nulls, deletes, erases, and reads at older system and valid instants after compaction.
An unflushed update selected native SQL, and ambiguous persisted hits also took that fallback.

### Follow-up decision

Use the existing trie-owned search files for a production implementation of direct ranking.
The score-only handoff explains a material part of the improvement below the cap.
The exact score and SQL ID bound is what prevents the demonstrated cliff above it.
The largest measured gains are for broad queries with large score ties.
The native plan remained faster for the small phrase case at 90,000 and 110,000 documents.
A production implementation should build tie metadata with the artifact, carry verified row identities into an in-engine batch fetch, and leave unsupported orders and visibility to the existing plan.
Measure how often real queries qualify and whether the extra artifact metadata pays for itself on fresh terms.

The focused release run passed all 50 search and node tests, including the exact score and ID bound regression.
The full Gradle unit rerun passed all 3,187 tests.
The first Gradle run had one JVM cache eviction assertion failure, which passed in isolation and in the full rerun.

The merged Rust experiment uses the prepared BM25 scorer and the shared posting-window index.
The search, node, and text suites passed 227 tests with `rtdb-node/fts-experiments` enabled, including the exact bound regression, and Cargo compiled the benchmark example.

## Query replay, filter order, and fallback

The workload harness uses the native SQL planner and executor from `621022aad`.
It generates tickets with skewed document lengths, recurring phrases, a mix of common and rare terms, and structured status and cohort fields.
The workload is synthetic and approximates ticket search shapes.
The measurements use a locked Rust release build with four Tokio workers on an Apple M4 Max and RTDB's in-memory object storage.
In this experiment, each measured query and its oracle used the same captured query context, trie files, and BM25 corpus.
The oracle evaluates MATCH from resolved row values and disables both ranked and unranked candidate narrowing.
Each comparison checks the ordered IDs, floating-point scores, projected values, and valid and system interval boundaries.

The replay starts six SQL readers and a writer for every wave.
Readers share a captured basis for that wave and run broad, previously unused term, phrase, multiple-predicate, historical-point, and valid-range queries.
The writer commits a batch of backdated replacements, including a document that carried a rare high-scoring term.
The oracle runs after the timed queries against the retained reader context.
The harness records actual overlap with the write, candidate decisions, scoring work, and verification and fallback reasons.

Filter-first ranking takes an exact set of IIDs from a structured SQL filter at the requested valid and system points.
It intersects those IIDs with matching index documents before scoring and runs the ordinary scan predicates on the resolved rows.
The benchmark measures the prebuilt set and a separate variant that includes the SQL time to build it.
The corpus is registered before applying either scope.
A temporal regression places a matching term and the required status on different versions of one entity and verifies that the entity stays out of the result before and after compaction.

The first fallback alternative goes straight to the SQL scan once ranking exhausts its candidate or verification budget.
The second uses the document frequency of a single required term across the opened bundles, plus live event counts, to predict when to avoid both ranking and unranked narrowing.
This estimates posting work before applying phrase, version, score, or structured constraints.
The fallback alternatives retain the same corpus registration, MATCH semantics, and final SQL sort.
One query has rare high-scoring winners behind a broad MATCH to test where a posting-count heuristic mispredicts.

```sh
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --features fts-experiments --example fts_architecture_bench -- workload 80000 20
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --features fts-experiments --example fts_architecture_bench -- workload 120000 20
```

### Workload results

The 80,000- and 120,000-document runs used 20 samples per filter and fallback variant.
Each also ran 20 waves of six concurrent readers, with 64 backdated puts per wave.
All 1,704 comparisons matched the exhaustive SQL oracle, including the temporal canaries.
The writer overlapped 104 of 120 timed queries at 80,000 documents and 100 of 120 at 120,000 documents.

| Replay documents | p50 | p95 | p99 | Ranked queries | Candidate-limit scan fallbacks |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 80,000 | 18.91 ms | 31.93 ms | 35.35 ms | 120 / 120 | 0 |
| 120,000 | 30.11 ms | 390.10 ms | 399.58 ms | 100 / 120 | 20 |

The broad query accounted for all 20 fallbacks at 120,000 documents.
Its p50 was 383.08 ms under replay.
Fresh-term, phrase, multiple-predicate, historical-point, and valid-range queries all completed ranked narrowing at that size, with p95 between 15.47 and 45.79 ms.
These percentiles describe the synthetic replay and its mix of query shapes.

| Documents | Eligible IIDs | Rank-first p50 | Prepared filter p50 | Build filter and rank p50 | Score evaluations, rank-first → filter-first |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 80,000 | 82 | 35.77 ms | 3.09 ms | 7.93 ms | 80,000 → 80 |
| 80,000 | 816 | 48.32 ms | 3.99 ms | 12.19 ms | 80,000 → 814 |
| 80,000 | 7,942 | 13.25 ms | 5.34 ms | 15.96 ms | 80,000 → 7,940 |
| 80,000 | 40,080 | 12.06 ms | 11.01 ms | 32.19 ms | 80,000 → 40,078 |
| 120,000 | 113 | 92.44 ms | 5.47 ms | 14.73 ms | 120,000 → 111 |
| 120,000 | 1,196 | 102.33 ms | 8.10 ms | 22.52 ms | 120,000 → 1,194 |
| 120,000 | 11,949 | 127.58 ms | 13.89 ms | 32.34 ms | 120,000 → 11,947 |
| 120,000 | 60,009 | 230.43 ms | 33.53 ms | 67.61 ms | 120,000 → 60,007 |

At 80,000 documents, building the scope on demand paid for itself at approximately 0.1% and 1% selectivity.
At 10% and 50%, the scope could be useful when already available and took the longer route when built by scanning the table.
Above the candidate cap, all four filter-first variants beat rank-first even with scope construction included.
Filter-first still visited the postings, so its savings came from scoring and validating fewer candidates.
The canary checked the filter on live and compacted state at three valid and system point pairs, including the old version after its correction expired.

| Workload, 120,000 documents | Current p50 / p95 | Skip retry p50 / p95 | Posting estimate p50 / p95 |
| --- | ---: | ---: | ---: |
| Uniform scoring | 428.17 / 432.06 ms | 385.37 / 389.97 ms | 269.67 / 272.01 ms |
| Rare high-scoring winners | 4.97 / 5.21 ms | 4.68 / 5.08 ms | 268.44 / 271.14 ms |
| Highly selective structured filter | 91.14 / 91.84 ms | 48.24 / 48.82 ms | 12.32 / 12.64 ms |
| Uniform scoring after corrections | 346.22 / 359.23 ms | 289.02 / 295.60 ms | 278.04 / 284.28 ms |
| Rare high-scoring winners after corrections | 7.31 / 7.51 ms | 7.21 / 7.47 ms | 277.41 / 279.79 ms |
| Structured filter after corrections | 122.21 / 126.33 ms | 65.43 / 69.63 ms | 14.40 / 14.97 ms |

Skipping the retry avoided another 120,000 unranked events when the native path had already spent its budget.
After corrections, the uniform-score query hit the verification budget after scoring 1,280 live candidates and then visited 120,000 unranked events on the current route.
Skipping that retry saved 57 ms at p50.
The posting estimate took the same early-scan decision for the two broad MATCH queries, even though the rare-winner query needed only 10,752 scores before corrections.
The phrase control kept its ranked path under all policies.

### Workload decision

Prioritize filter-first ranking when a cheap structured scope is available at the captured basis.
The benefit depends on filter selectivity, the cost of acquiring the scope, and whether rank-first would hit a budget.
Preserve the resolved-row predicate check and use the full snapshot corpus for scoring.

Test a direct-to-scan fallback after a candidate-budget miss when the unranked pass is also expected to cover most of the table.
The measured verification-budget cases were broad, so a production policy also needs coverage of narrow MATCH queries that can still benefit from unranked narrowing after verification gives up.
A posting-count threshold alone misclassifies broad MATCH queries with rare high-scoring winners.
Use the observed scoring frontier and verification work when evaluating any early decision for those shapes.

The focused release run passed all 56 Rust cases.
That run includes the end-to-end workload regression with a reduced candidate limit so it exercises both fallback decisions on a small fixture.
The full Gradle unit rerun passed all 3,187 cases.
The first Gradle run had one local health-check request fail to receive a response, and the case passed both in isolation and in the full rerun.

## Ordered-index scope in ranked SQL

Ranked MATCH now consumes the IID scope already supplied by the ordered index before it scores file and live events.
It still visits matching postings, uses the full snapshot corpus, and verifies the resolved row against the scan predicates.
After an unscoped candidate-budget miss, it skips the second narrowing pass only if pending and handed-out candidates already prove that the retry would exceed the distinct IID limit.
Declines without that proof follow the ordinary MATCH narrowing path.

The native SQL harness compares both policies on one captured context with the same SEARCH and `(status, cohort)` ordered indexes.
The measurement branch was based on main at `5eba8e9ea`.
Baseline applies the ordered scope after scoring, and the current route applies it before scoring.
Both timings include acquiring the scope from the ordered index.
In this experiment, the exhaustive SQL oracle resolved rows, evaluated MATCH from the result and used the same full corpus for scores.
All 348 result comparisons matched, including 36 temporal canaries across live and compacted state.
The comparisons checked ordered IDs, scores, projected values and both time intervals.

The measurements use the same Apple M4 Max and in-memory object storage as the earlier replay.
The benchmark alternates policy order and uses 12 samples per policy for each large case.
p95 uses nearest rank, which is the slowest of 12 samples here.
The 4,000-document smoke run uses two samples per policy.

| Documents | Query | Scope IIDs | Baseline p50 / p95, ms | Current p50 / p95, ms | Score evaluations, baseline → current |
| ---: | --- | ---: | ---: | ---: | ---: |
| 80,000 | Uniform scoring | — | 247.14 / 253.34 | 246.10 / 254.57 | 80,000 → 80,000 |
| 80,000 | Rare winners | — | 1.25 / 1.64 | 1.25 / 1.33 | 7,680 → 7,680 |
| 80,000 | Open, cohort < 2 | 82 | 25.12 / 25.76 | 3.13 / 3.28 | 80,000 → 80 |
| 80,000 | Open, cohort < 20 | 816 | 26.39 / 26.87 | 4.11 / 4.38 | 80,000 → 814 |
| 80,000 | Open, cohort < 200 | 7,942 | 9.12 / 10.06 | 6.12 / 6.31 | 80,000 → 7,940 |
| 80,000 | Open, cohort < 1,000 | — | 13.31 / 14.47 | 13.32 / 13.51 | 80,000 → 80,000 |
| 120,000 | Uniform scoring | — | 448.43 / 452.10 | 406.84 / 415.13 | 105,024 → 105,024 |
| 120,000 | Rare winners | — | 5.29 / 5.48 | 5.18 / 5.48 | 10,752 → 10,752 |
| 120,000 | Open, cohort < 2 | 113 | 86.24 / 89.08 | 6.02 / 6.30 | 120,000 → 111 |
| 120,000 | Open, cohort < 20 | 1,196 | 97.04 / 99.80 | 9.48 / 9.82 | 120,000 → 1,194 |
| 120,000 | Open, cohort < 200 | 11,949 | 123.97 / 161.89 | 16.21 / 16.89 | 120,000 → 11,947 |
| 120,000 | Open, cohort < 1,000 | — | 240.56 / 248.14 | 197.88 / 202.84 | 120,000 → 120,000 |

The two tightest scopes improved median latency by 6.4–14.3× across the two sizes.
The widest filter exceeded the ordered seek's scope limit, so its only material gain came from the fallback shortcut at 120,000 documents.
For that filter and the uniform-score query at 120,000 documents, the shortcut avoided all 120,000 events in the unranked retry.
The rare-winner controls kept the same scoring work at both sizes.
These measurements describe warmed synthetic point-in-time queries on local storage.

```sh
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --features fts-experiments --example fts_architecture_bench -- ranked-filter 80000 12
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node --features fts-experiments --example fts_architecture_bench -- ranked-filter 120000 12
```

The focused release run passed 61 Rust tests, including the SQL regression for both optimizations at a reduced candidate cap.
The separate native ordered-index integration run passed all eight tests.
The full Gradle unit run passed all 3,189 tests with a freshly built native library.
