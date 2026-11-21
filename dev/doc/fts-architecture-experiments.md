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
