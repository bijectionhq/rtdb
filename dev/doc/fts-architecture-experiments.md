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
[Ranker and search answers](../../rust/crates/search/src/accelerate.rs) · [Scan handoff](../../rust/crates/query/src/scan.rs) · [Corpus](../../rust/crates/search/src/corpus.rs) · [Bundle reads](../../rust/crates/search/src/runtime.rs)

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
