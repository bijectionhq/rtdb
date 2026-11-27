# RTDB and XTDB: design divergences

This page lists where RTDB (the Rust port, `rust/crates/*`) is built differently from XTDB (the JVM reference, `rtdb-ref`), and estimates where each difference can make RTDB slower or faster.
It is a deeper companion to [`XTDB-VS-RTDB.md`](XTDB-VS-RTDB.md), which gives the benchmark numbers.

Everything here comes from reading source; nothing was built, run or profiled.
Each claim names a file and function.
A claim marked **(inference)** is reasoned from the code rather than read directly from it, and every performance estimate is an inference unless it cites a measurement.

Path abbreviations:

- `R/` is `rust/crates/`.
- `XK/` is `rtdb-ref/core/src/main/kotlin/xtdb/`.
- `XC/` is `rtdb-ref/core/src/main/clojure/xtdb/`.
- `XM/` is `rtdb-ref/modules/`.

This page describes RTDB as of the commit that made leaf column statistics lazy (`perf(format): a read trie metadata file decodes a leaf's column statistics only when a scan visits it`).

One measurement corrects the reading: a profile of point lookups over a 1,000,000-row table in Docker spent 75% of its time in `MetadataFile::read_reader`.
The level-1 metadata file's decode bound (121 MB) exceeded the metadata cache's per-file limit, so every lookup decoded the whole file again.
That commit fixes it; the remaining costs of reading metadata are in [Reading metadata](#reading-metadata) and row 11 of the ranked table.

## Summary

RTDB copies XTDB's *architecture* closely:

- the same leader resolve → replica append → apply pipeline;
- the same L0/L1/L2 compaction job selection;
- the same hash-trie bucketing and limits;
- the same bitemporal polygon algorithm;
- the same 100k-row join threshold, 102,400-row sort blocks, and bloom/IID join pushdown.

The divergences are almost all in *representation and caching*:

1. RTDB converts Arrow data into owned `Value` trees at many points (writes, DML results, scans of system columns, most operators, pgwire output, compaction, spill), where XTDB stays on Arrow vectors from end to end.
2. RTDB caches no parsed, planned or compiled query, where XTDB caches all three.
3. RTDB reads whole objects into heap memory and decodes pages into a second cache, where XTDB memory-maps the byte slice of one record batch.
4. RTDB charges memory per row or per value through a shared atomic chain, where XTDB charges per Arrow buffer allocation.

These explain the measured pattern (inference):

- RTDB wins on startup, footprint, plain full scans and concurrency of short reads.
- RTDB loses on joins, aggregates over union columns and anything that falls off its column path.
- RTDB loses on point operations once the data outgrows the live index. At 1M rows, point lookups and updates are about 15× slower than XTDB, joins about 4.5× slower.

Point operations at 1M rows are where the per-file, per-page and per-statement costs below all add up.

---

## 1. Live index

### XTDB

- One Arrow `Relation` per table per block holds every transaction's rows: `XK/indexer/LiveTable.kt`, with `LiveTable.State(relation, trie)`.
- A transaction writes through `LiveTable.openTx`, a direct slice (`relation.openDirectSlice`) over the same memory. Rows are appended in place (`Tx.endOps`, `Tx.importRows` → `relation.append`).
- `Tx.commit` yields the next value by swapping the `State`. It does not copy rows.
- Schema growth is absorbed by the relation's vectors: dense-union legs and struct children are promoted in place.
- `LiveIndex.State` (`XK/indexer/LiveIndex.kt`) is a Clojure `PersistentHashMap` behind a `StateFlow`. Readers never lock.
- A snapshot (`XK/indexer/TableSnapshot.kt` `open`) is another `openDirectSlice` plus `trie.withIidReader`: a ref-counted view, O(1) per table.
- DML writes its query output straight into the live vectors: `OpenTx.executeSql` → `table(...).writePuts(RelationReader)` (`XK/api/tx/OpenTx.kt:233-290`).

### RTDB

- `LiveTable` (`R/index/src/live.rs`) holds `chunks: Vec<(u32, Arc<IndexBatch>)>`, one immutable Arrow `RecordBatch` per transaction, plus a persistent `MemoryHashTrie` over global row numbers.
- `LiveTable::append` clones the table: the chunk `Vec`, `column_types` and the trie root. It pushes the new chunk, path-copies the trie (`add_range`), then calls `merge_small_chunks`.
- `merge_small_chunks` (`live.rs:256`) concatenates the newest chunk into the previous one while the newest is at least as large, the two fit in a trie page (`page_limit`, 1024 rows by default; appends wait in a tail of up to `log_limit` chunks first), and the schemas are **exactly equal**. It uses `arrow_select::concat::concat_batches`, which re-copies the earlier chunk every time.
- Every row lookup (`locate`, `header`, `row`) runs a `partition_point` binary search over the chunks.
- Each `IndexBatch` decodes every row's header eagerly (`IndexBatch::from_owned_with_headers`).
- `LocatedLeaves` caches `(chunk, offset)` and headers per trie leaf per snapshot (`LiveTable::located_rows`). It sits behind a `std::sync::Mutex<HashMap>` keyed by leaf address. A clone starts empty, so every append invalidates the whole table's cache.
- `LiveIndex::apply` clones the `BTreeMap` of tables. `PartitionState::apply` (`R/index/src/partition.rs:81`) runs all of that **while holding the `parking_lot::RwLock` write lock**, which also clones the `TableCatalog` (`with_entries_for`).
- A transaction's pending writes are kept as rows: `TransactionBuffer` holds `Vec<StoredEvent>`, whose `StoredOperation::Put(StructFields<Value>)` is a `Value` tree per document (`live.rs:429-520`).
  - Each in-transaction read calls `TransactionBuffer::snapshot`, which turns the events added since the last read into a new `IndexBatch` (`IndexBatch::copy_events`) and appends a cloned segment.
  - `stage` re-encodes **all** of the transaction's events into Arrow (`materialize`).
  - Each `append` takes a `MemoryBudget` reservation per event.

### Divergence and impact

- **Union legs block chunk merging.** A column written as `int2` by one transaction and `int4` by another (psycopg does this) gives chunks with different schemas, so `merge_small_chunks` stops. The table then stays at one chunk per transaction. Every downstream gather handles many small sources, and `joined_union` / `Column::Values` fallbacks apply (see §3).
- **Per-transaction cost grows with chunk count.** Each append clones the chunk `Vec`, `BTreeMap` and catalog, and the merge re-copies up to 8k rows. With XTDB, an append writes only the new rows (inference: small, but it is on the single writer path and under the lock).
- **The write lock blocks readers.** The `RwLock` write lock is held while computing the next value, so `open_snapshot` waits behind apply. XTDB readers never wait.
- **The located-leaf cache dies on every commit.** Under a write-heavy mix (e.g. the 80/20 benchmark), every read after a commit re-locates and re-copies the headers of every leaf it touches (inference).
- **The write path changes representation twice.** Client Arrow becomes `Value` documents, then Arrow again (`IndexBatch::copy_events`).

### Recommendation

- Short term:
  - merge chunks whose schemas differ only in union legs by widening into the joined layout (the gather already knows how: `R/query/src/columnar.rs` `joined_union`);
  - compute `LiveIndex::apply` outside the lock, then swap;
  - keep the located-leaf cache keyed by leaf identity across appends that leave the leaf `Arc` unchanged.
- Long term: one growable Arrow builder per table (an append-only relation with a published row count), so that a transaction writes columns in place and a snapshot is a length-bounded view, as in XTDB.

---

## 2. Tries, on-disk layout and metadata pruning

### Memory hash trie

- XTDB, `XK/trie/MemoryHashTrie.kt`: `LOG_LIMIT = 64`, `PAGE_LIMIT = 1024`, 4-way branching (`Bucketer`). IIDs are compared in place in the Arrow `_iid` vector through an `ArrowBufPointer`.
- RTDB, `R/trie/src/memory.rs`: the same limits (`TrieConfig::default`) and 4-way buckets (`Bucketer::DEFAULT`, 2 bits). The structure is the same, and so is the "rebuild a leaf every 64 inserts" behaviour.
  - `IidReader for LiveTable` reads the IID through `LiveTable::header`, i.e. a chunk binary search plus a header copy per comparison (`live.rs:304`).
  - `Leaf::sorted_rows` caches the merged order in a `OnceLock`.
- Divergence: only the cost of an IID read, which is a binary search instead of a pointer. Low impact.

### On-disk trie files

- Both write a data file of Arrow pages of at most 1024 rows, plus a metadata file with an IID/recency-branch node vector and per-leaf column statistics, including blooms.
  - RTDB writes these with `MetadataBuilder` (`R/format/src/metadata.rs`).
  - XTDB writes them with `MetadataFileWriter` / `PageTrieWriter` (`XK/trie/`).
- The files are format-compatible: the corpus tests read each other's files.

### Reading metadata

- XTDB: `PageMetadata.open` (`XK/metadata/PageMetadata.kt`) loads the metadata file's single page as an Arrow `Relation`. `ArrowHashTrie(rel["nodes"])` walks the nodes in place, and `readPageIdxs` builds one `(colName, pageIdx) → row` map. Statistics are read as typed vector reads on demand.
  - The metadata predicate is compiled into a page-index test (`metadataPredicate?.build(pageMetadata)` in `XK/segment/BufferPoolSegment.kt`).
  - Bloom filters are `ImmutableRoaringBitmap` views over the vector's buffer, with no deserialisation (`XK/bloom/BloomUtils.kt` `bloomToBitmap`).
- RTDB: `MetadataFile::read_reader` decodes every node into `Vec<MetadataNode>`.
  - Leaf statistics are now `LeafColumns` (`metadata.rs`). A leaf read from a file decodes them each time a scan visits it, **into a whole `Vec<Value>` tree for the leaf** via `rtdb_columnar::read_value`, and does not keep them.
  - The cache sizes a file by the node arrays it retains (`MetadataFile::stored_bytes`), not by its decode bound.
  - `PageTest::matches` (`R/query/src/page_metadata.rs`) finds a column by linear search over those `Value` structs, using string field lookups (`column.field("col-name")`, `"root-col?"`).
  - Blooms are checked by `BloomFilter::from_bytes`, which **deserialises a `RoaringBitmap` per page per test**. The join pushdown does the same in `Pushdowns::page_matches` (`R/query/src/pushdown.rs:144-151`).
  - Decoded files are held as views in the database's `BufferPool`, under the node's memory cache (§6).
- Divergence and impact:
  - A point lookup that visits one leaf in each of N files pays N full statistic decodes on a cold cache, and a bloom deserialisation per leaf even when warm.
  - The lazy change removes the decoding of unvisited leaves, but not the `Value` materialisation or the bloom deserialisation of visited ones.
  - High for point operations once data is on disk (inference).
- Recommendation:
  - keep leaf statistics as Arrow arrays and resolve `(column, page) → row` once per file, as in `readPageIdxs`;
  - compile `PageTest` against column indexes once per file;
  - test blooms through a zero-copy view of the serialized roaring format (`roaring` can test containers in place, or cache the decoded bitmap per leaf).

### Trie catalog

- XTDB: Clojure persistent maps updated with `update-in` (`XC/trie_catalog.clj`).
- RTDB: `TrieCatalog::with_tries` (`R/index/src/trie_catalog.rs:196`) clones the whole catalog per table. `current_tries` allocates and sorts every trie of a table per call, and is called on every scan and every compaction planning pass. `PartitionState::add_tries` / `adopt_block` do this under the write lock.
- Impact: medium, and it grows with the number of tries.
- Recommendation: use `im`-style persistent maps, and cache each table's sorted live tries per catalog generation.

---

## 3. Scan path

### XTDB

`XK/operator/scan/ScanCursor.kt`, `XC/operator/scan.clj`.

1. Segments (live `MemorySegment`s and `BufferPoolSegment`s) are merged by `MergePlanner` into `MergeTask`s, one per IID path.
2. Per task, pages are loaded concurrently (`runBlocking { task.pages.map { async { it.loadDataPage(al) } } }`), each as a zero-copy `Relation` over a buffer-pool slice.
3. The `_iid` predicate is applied first as a selection on each page: `SingleIidSelector` binary-searches the iid vector, `MultiIidSelector` handles several.
4. Only the selected rows enter `EntityMerge`.
5. `AsOfResolver` is used when both temporal dimensions are points. It stops at the first covering event of an entity, so older versions are never visited. `PolygonResolver` handles everything else (`EntityResolver.kt`).
6. `BitemporalConsumer` records selected row indexes per page and builds typed Arrow temporal vectors. The output relation is a `select` view over the page, plus the four temporal vectors.
7. Column predicates (`colPreds`, compiled `SelectionSpec`s) are applied inside the scan to that output.

### RTDB

`R/query/src/scan.rs` (`PartitionScan`), `R/query/src/scan_tasks.rs`, `R/temporal/src/lib.rs`, `R/query/src/columnar.rs` (`gather`).

1. `initialize` visits **every leaf of every live segment** (filtered by `path_matches`). For each one it calls `located_rows` and `summarize_headers`.
   - Then, for each selected trie file, it fetches the metadata from the global cache or decodes it, and walks the node tree.
   - For every surviving leaf it materialises the leaf statistics and runs `Pushdowns::page_matches`, `may_satisfy_not_null` and `PageTest::matches`.
   - Every step takes a `MemoryBudget` reservation (candidate, summary, selector, stack and descriptor reservations).
2. `ScanTaskPlan` partitions pages by IID range.
3. `buffer_task` loads each page:
   - a decoded page held as a view by the `BufferPool` (§6), else
   - `BufferPool::get` of the **whole data file** on its first open, and a ranged read of the page once its layout is cached (§6), `read_page` (memcpy into a new `Vec`), `IndexBatch::from_owned` (decodes all row headers), then an order check of every row on first load.
4. It runs a `BinaryHeap` k-way merge over **every row in the task's IID range**. Rows not in `scan.iids` are dropped one by one (`iids.contains`), after the heap has visited them.
   - There is no binary search for the target IID inside a page: the only binary search finds the task's lower bound.
   - A point lookup therefore walks up to a full 1024-row leaf through the heap, per file that holds that leaf.
5. Every visited event goes through `HistoryResolver::resolve_fragments`, the general polygon calculator (`AdmittedPolygonCalculator::visit`).
   - There is no as-of fast path, so all older versions of an entity are processed even for a point-in-time query.
   - The ceiling's growth takes memory reservations.
6. `gather` builds the output columns:
   - `Column::Arrow` via `interleave` when all sources share a flat layout;
   - a dense union via `joined_union` when integer widths differ;
   - otherwise `Column::Values(Vec<Value>)`.
   - `_valid_from`, `_valid_to`, `_system_from`, `_system_to` and `_iid` are **always `Column::Values`**. Each timestamp cell is `Value::Timestamp { timezone: Some("UTC".into()) }`, a heap `String` per cell (`fn time`). Each `_iid` cell is a `Vec<u8>`.
7. Column predicates are applied above the scan by `BatchFilter`. Its kernels cover plain scalar columns only; anything else materialises every column of the batch into rows (§4).
8. An `_id` range confines the scan to an IID interval (`id_interval`), which XTDB has no counterpart for.
   - It applies where every `_id` of the table is a UUID, because a UUID is its own IID and orders as its bytes do. The conjuncts `_id <`, `<=`, `>`, `>=` and `BETWEEN` a UUID literal or parameter give one inclusive interval.
   - `path_matches` drops trie files, metadata paths, live leaves and scan tasks outside the interval, and `buffer_task` starts and ends its merge inside it.
   - The predicate still runs on the rows read. Like the `_id = v` narrowing, the interval trusts each row's IID to be the one its ID derives: a row replayed under another IID is not found by its ID.
   - A table holding an ID of any other type is not narrowed, since its IDs are hashed into their IIDs.
9. `ORDER BY _id ... LIMIT n` over such a table reads in IID order and stops (`iid_ordered_input`), which XTDB also has no counterpart for.
   - The scan hands its tasks out in ascending or descending IID order, and keeps each task's merged rows in one batch in that order.
   - The scan's predicates run on those batches, and the limit above stops pulling once it has its rows, so the remaining tasks load no page.
   - `initialize` walks the interval's metadata paths but leaves each leaf's statistics undecoded; `read_statistics` tests them when the scan reaches the leaf's task, from the metadata the pool holds.
   - It serves one `_id` sort key over a single partition, where no ordered index answers first. Any other sort is sorted as before.

### Divergence and impact

- **Point operations scale with versions and files rather than with matches.** XTDB does about log₂(1024) comparisons per page and stops at the first visible version. RTDB pushes every row of the leaf through a heap and the full polygon calculator, per file.
  - This grows with data size, since more L0/L1 files cover each IID path, and with update count, since an entity has more versions.
  - It is the most plausible size-dependent cause of the 15× point-lookup and update gap at 1M rows **(inference)**.
  - Compaction lag (§9) makes it worse, because uncompacted L0 files each cover the whole IID space.
- **System columns are never columnar.** Any query projecting or filtering on `_valid_from`, `_system_from` and the like (valid-time history, `FOR VALID_TIME AS OF` outputs, all UPDATE/DELETE inputs, which read `_iid` and the valid-time columns) goes through `Value` per cell, a `String` allocation per timestamp, and the row fallback in every operator above.
- **Decoded pages cost a page decode per cold read.** A page is decoded once into the pool, which then drops the raw file, so warm reads are cheap but every eviction costs a decode, where XTDB maps the raw page in place (inference).

### Recommendation

- Apply `scan.iids` as a per-page binary search before the merge, as `SingleIidSelector` / `MultiIidSelector` do. Skip leaves with no candidate IID before loading them.
- Add an as-of resolver that ends an entity at its first covering event (port `AsOfResolver`).
- Emit temporal columns as `TimestampMicrosecondArray` with a shared `"UTC"` timezone `Arc<str>`, and `_iid` as `FixedSizeBinaryArray`.
- Apply simple column predicates inside the scan on the gathered batch, so non-matching rows are never materialised.
- Cache file layout and page offsets, and read single pages by range (see §6).

---

## 4. Expression evaluation and value representation

### XTDB

- `XC/expression.clj`: `codegen-expr` / `codegen-call` multimethods emit Clojure code per expression, specialised to input vector types.
- `emit-projection` wraps it in a `(dotimes [idx row-count] …)` loop, `eval`s it into JVM bytecode, and memoises it (`util/lru-memoize`).
- Unions are read leg by leg (`continue-read-union`).
- Selections produce `VectorMask` → `int[]`, and outputs are `select` views.
- Comparators for sort and join are generated `IntBinaryOperator`s (`XC/expression/comparator.clj`).

### RTDB

- `R/types/src/value.rs` `Value` is a 40-variant owned enum (`Utf8(String)`, `Timestamp{.., timezone: Option<String>}`, `List(Vec<Value>)`, `Struct(StructFields)`, …).
- Rows are `OwnedRow { values: Vec<Value>, reservation }` (`R/query/src/cursor.rs`) grouped into `OwnedPage`. Batches are `ColumnBatch { columns: Vec<Column> }`, where `Column` is `Arrow` or `Values` (`R/query/src/columnar.rs`).
- `R/expr` is a recursive tree-walking interpreter over `&[Value]`. About 85k of its 103k lines are Unicode tables; the logic is about 11k lines, mostly in `evaluate.rs` and `evaluate/invocation.rs`.
- Per call, `functions::scalar_call`:
  - lowercases and replaces the function name (two `String`s);
  - re-runs `function_type` on a freshly built `Vec<LogicalType>`;
  - walks a chain of module dispatchers that each match on strings.
- Per row, `Execution::evaluate_invocation` (`R/query/src/context.rs`) does a cancellation check, two scope reservations, a `Vec` for outer rows, and nested `run_until_cancelled` futures. Row operators also call `scope.reset()` per row.
- Vectorised paths:
  - `columnar::kernel::truth`: comparisons, AND/OR/NOT, IS NULL, BETWEEN over plain I8–I64/F64/Utf8/Bool/µs-timestamp and dense unions of those.
  - `columnar::projection`: checked `+ - *`.
  - `columnar::functions`: projected EXTRACT, ROUND, ABS, FLOOR/CEIL and the one-argument float math, UPPER/LOWER, LENGTH, SUBSTRING and searched CASE.
    Each projection resolves its calls to `rtdb_expr::kernels::Function` once, and every vector it builds (literal copies, gathered selections, widened arguments, CASE inputs) is charged to the query's memory budget while it lives; a refusal fails the query rather than falling back.
    A batch with any failing cell is still recomputed by the row evaluator.
  - `vector.rs`: lowers a subset of expressions to DataFusion `PhysicalExpr`, **rebuilt per batch**, with an extra pass to scan divisors for zero.
  - `rtdb_expr::evaluate_batch` exists but has no production caller.
- Fallbacks: `BatchFilter` and `BatchExpressions` call `batch.rows()`, which materialises **every** column for every row, even when the expression reads one.
  - Columns that always fall back: union columns with string legs, temporal system columns (`Column::Values`), nested types, extension types (keyword, uuid, uri, transit, decimal), and float or non-ASCII ordering cases.
  - Tracing and EXPLAIN ANALYZE run the production column plan; stacked filters stay fused, EXPLAIN ANALYZE measures each fused stage as its own `select`, and a trace shows the fused stack as one span.

### Divergence and impact

- For expressions on the fallback path, RTDB pays an enum match, an owned clone and several allocations per cell per node. XTDB pays a typed, unboxed read inside JIT-compiled code.
- Inference: 10–100× per row, and the dominant cost of the aggregate and as-of benchmarks over pgwire, where `_id` is an integer union.
- **Recommendation:**
  - resolve function name, overload and result type once in `ExpressionInvocation::prepare`;
  - borrow cells instead of `retained_clone`;
  - materialise only the columns an expression reads;
  - cache the DataFusion lowering per (expression, input types);
  - extend the kernels and the lowering to dense unions with string legs and to temporal columns.
  - Longer term: compile expressions to closures over typed column readers, the closest Rust analogue of XTDB's codegen.

---

## 5. Operators

| Operator | XTDB | RTDB | Divergence / impact |
| --- | --- | --- | --- |
| Hash join | `XK/operator/join/` `BuildSide` (Arrow `Relation`, spills Arrow pages past 100k rows), `BuildSideMap` (open addressing over `IntVector`), `ProbeSide` (generated comparator), `JoinType` emits index lists and `select` views; `DiskHashJoin`/`Shuffle` Grace partitioning; bloom/IID pushdown to the probe scan | `R/query/src/join.rs`, `join_storage.rs` (`HashIndex` with the same skip array and 0.6 load, same partitioning), `pushdown.rs` (same pushdowns). But `JoinLayout::project_page` copies each input page; keys are hashed and compared as `Value` (`hash_values`, `sql_eq_in_timezone`); every output row is `OwnedRow::copy_parts_in`; spills are JSON. Columnar `BatchJoin` (`batch_join.rs`) only for inner equi-joins on bare columns, same projection shape, below 100k build rows, no tracing, and only when a parent asks for batches | Same algorithm, row representation. Main cause of the 0.43–0.44× join results and the 4.5× gap at 1M rows (inference). Make `BatchJoin` the default: hash Arrow key columns, emit `take` indices, keep build pages as Arrow |
| Spill | Arrow IPC pages (`XK/operator/join/Spill.kt`, `XC/operator/order_by.clj` `write-rel`) | `R/query/src/spill.rs`: `serde_json` of `Vec<Value>` **per row** (`SpillWriter::write`, `SpillReader::next`), reserving `retained × 8` / `length × 64 + 4096` bytes per row | Very high whenever anything spills. Use Arrow IPC (the format crate already writes it) |
| Aggregate | `XC/operator/group_by.clj`; `XK/arrow/agg/GroupMapper.kt` (`DistinctRelationMap` hash trie → I32 group-id vector per batch); per-aggregate output vectors indexed by group; compiled reducers | `R/query/src/aggregate.rs`: `GroupKeys` = `HashMap<u32, Vec<usize>>` with `OwnedRow` keys compared through `rows_equal` (a new `EvaluationScope` per cell compared); min/max clone, cast and reserve per row. Column path only for bare-column keys without FILTER/DISTINCT/ORDER BY/ARRAY_AGG/PERCENTILE, and its inputs are still `ColumnValues` (`Vec<Value>`) | High constant factor. Group ids per batch over Arrow keys; typed accumulators |
| DISTINCT aggregate, ARRAY_AGG, PERCENTILE | One `DistinctRelationMap` per group; index lists plus `copyRows` | `AggregateColumn::aggregate` rescans the whole JSON spill per input row (O(N²)); `finish` rescans the spill per group (O(G·N)) | Asymptotic. Per-group hash set / index list |
| Sort | `OrderByCursor`: 102,400-row blocks sorted by index with generated comparator, Arrow run files, 4-way merge | `R/query/src/sort.rs`, `sort/records.rs`: the same block size and fan-in, but switches to the JSON disk store after `sort_memory_bytes` = **8 MiB** (`context.rs:112`), i.e. about 12k rows of 10 columns; `write_run` re-reads records by seek + JSON decode; comparisons on `Value` | Medium-high on large sorts. Count by rows or raise the budget; spill Arrow |
| Top-k | No special path (block trim) | `SortPlan::top` / `top_batches` for limit ≤ 10,000: sorted `Vec` with early reject | RTDB faster (measured 1.28–2.0×) |
| Window | `XC/operator/window.clj` `WindowFnCursor`: in memory, `GroupMapper` partitions, one index sort | `R/query/src/window.rs`: finds each row's partition by **scanning the key spill file** (`PartitionPages::next_page`, O(N·P) with I/O); copies rows twice; per-cell `replace_value` | Asymptotic. Hash partition ids |
| DISTINCT | `DistinctRelationMap` over Arrow | `operators.rs` `Distinct`/`SeenRows` with `OwnedRow` copies; `equality_hash` (`operators.rs`) hashes a date-time as its wall-clock instant in the session zone, at any depth inside lists and structs; a set or map holding one still hashes by its size alone, as its struct members compare regardless of field order | Fixed for scalars, lists and structs; sets and maps of date-times stay O(N²) per size |
| INTERSECT/EXCEPT | `XC/operator/set.clj` hash `BuildSide` plus `ProbeSide` | `operators.rs` `SetCursor`: spools the right side to JSON, then `reader.rewind()` and a full scan **per left row** (`operators.rs:629`) | Asymptotic. Hash build side |
| LIMIT | `TopCursor` `select` over an index range | `Limit` moves `OwnedRow`s into a new page | Low |
| UNNEST | `UnnestCursor` `copyRange` plus selection | Per-row interpreter call, and a copy of the whole input row per element | Medium for wide rows |
| Apply | `ApplyCursor.kt`: dependent cursor per row with args as single-row `select`s | `operators.rs` `Apply` rebuilds the dependent operator tree per row and clones both sides | Similar shape, higher constant |
| Cross join | Selection vectors | Nested page × page loop with row copies | Medium |

---

## 6. Buffer pool and caching

### XTDB

`XK/storage/BufferPool.kt`, `LocalStorage.kt`, `RemoteBufferPool.kt`, `XK/cache/MemoryCache.kt`, `DiskCache.kt`, `PinningCache.kt`.

- Footers are cached in Caffeine (`arrowFooterCache`).
- `getRecordBatch(path, idx)` asks `MemoryCache` for the **byte slice of that one batch**. `PathLoader` does `FileChannel.map(READ_ONLY, offset, len)` and wraps it as an `ArrowBuf` with no copy. `MemoryCache` de-duplicates concurrent fetches through a `ConcurrentHashMap` of ref-counted slices, limited to 0.5 × direct memory.
- Remote stores add a disk tier: a Caffeine `AsyncCache` with pinning at 0.75 × disk, filled by a single streamed GET to a file (`XM/aws/.../S3.kt` `getObject(k, outPath)`).
- The OS page cache does the rest.

### RTDB

- `R/storage/src/cache.rs` `BufferPool::get` does a `head` and then `get_reserved_range(key, 0..size)`: **the whole object goes into a heap `Vec`**.
  - The defaults make this costly: `file_size_target` is 100 MB and `cache_bytes` is 256 MB (`R/node/src/config.rs:43-44`).
  - Objects have a per-entry `tokio::Mutex` for single-flight loads.
- The pool also holds decoded views of an object (`cache::View`: a whole decoded file, a file's layout, one page), under the same `CacheBudget` as the raw bytes.
  - Every pool sharing a budget keeps its entries in one recency list behind one `parking_lot::Mutex`, with O(1) lookup, touch, insert and removal.
  - Admitting bytes, raw or decoded, evicts the least recently used idle entries of any pool; an entry still leased or in use is skipped, since evicting it would free nothing.
  - A view that would take more than an eighth of the budget, or that does not fit, is not cached.
- Scans (`R/query/src/scan.rs`) cache decoded trie metadata, data-file layouts and decoded `IndexBatch` pages as views, and evict the raw object once its decoded form is admitted.
  - A data file whose layout is cached is read a page at a time with `get_range`.
  - A read the cache cannot admit is charged to the query's own budget instead (`get_or`, `get_range_or`).
- `ArrowFile::read_page` → `read_block` (`R/format/src/ipc.rs:106-141`) copies each page into a new `Vec`, then decodes it.
- `R/storage/src/disk_cache.rs`:
  - a miss downloads the object as **sequential 1 MB range GETs**;
  - `put_file` copies in 64 KB chunks;
  - each range read does `File::open` + seek + `read_exact` into a new `Vec`;
  - `trim` rescans every entry on every lease drop.

### Divergence and impact

- **Read amplification.** Up to file size ÷ page size: one 1024-row page from a 100 MB L2 file costs 100 MB of I/O and memcpy on a cold read.
- **Cache thrash.** Two or three large files exhaust the 256 MB pool.
- **Copies.** Each page is copied twice before decoding.
- **Contention.** Every pool sharing a budget shares one mutex; its operations are O(1), but the 8-reader workloads still serialise on it (inference).
- Overall: very high for cold point lookups over compacted data; medium under concurrency.

### Recommendation

- Cache the footer and layout per file, then fetch and cache by `(key, page)` byte range: a `get_range` against the object store, or `pread`/mmap against the disk cache and the local store.
- Drop the whole-file `BufferPool::get` from compaction, and from a scan's first open of a file, by inspecting the layout from ranged reads.
- Shard the pool's recency list, and key its views by an interned `(file id, page)` rather than an `ObjectKey`.
- Download disk-cache misses as one streamed GET.

---

## 7. Storage and log

### Local log

- **Same format, group commit and fsync policy.**
  - XTDB `XK/api/log/LocalLog.kt`: `0x1E | i32 size | i64 ts | payload | i64 offset`; one writer coroutine drains up to 10 appends and calls `force(true)` once per batch.
  - RTDB `R/storage/src/log/local.rs`: the same format (`HEADER_BYTES = 13`, `TRAILER_BYTES = 8`). `LocalLogDriver::run` drains up to 10 and calls `sync_file` once.
- **Divergences:**
  - Startup `inspect()` reads every record, where XTDB reads only the last trailer (`readLatestSubmittedOffset`). Startup is O(log size).
  - The replication driver prefers the replay tail (`R/replication/src/driver.rs` `read_replay_head`). `LocalTail::poll_head`/`read_head` open the file twice and hop through `spawn_blocking` twice **per record**, for both the source and replica logs, about four opens and four thread-pool hops per transaction (inference). The batched `LocalTail::poll` (up to 1024 records) is bypassed.
- Impact: medium for single-row transaction latency; startup cost grows with log size.
- Recommendation: keep the tail's file handle open and read batches; read only the trailer at startup.

### Object store and publication

- RTDB `R/storage/src/remote.rs` `put_opened_file` does HEAD, then sequential 5 MB parts to a temporary key, then `copy_if_not_exists` (a server-side copy), then DELETE.
  - XTDB uploads parts in parallel straight to the final key (`XK/multipart/SupportsMultipart.kt`), and small objects with HEAD + PUT.
- The local store (`R/storage/src/object_store.rs` `put`/`publish_file`) copies into a tempfile and fsyncs the file and every parent directory.
  - Block production (`R/replication/src/blocks.rs:237-240`) writes and fsyncs a tempfile *before* `put_file` copies and fsyncs it again.
  - XTDB `LocalStorage` writes a tmp file and does an `ATOMIC_MOVE`, with no copy and no fsync.
- RTDB's `block_lag` (`R/node/src/database.rs`) lists nothing on the leader, the database's only block writer, and on a follower probes forward from the highest block it has adopted or seen with one fresh HEAD per block, listing `blocks/` only when it knows of no stored block. XTDB lists after the current block and caches the result for 10 s (`BufferPool.kt` `latestAvailableBlockIndex`), so its lag can be up to 10 s stale.
- Impact: medium-high on block flush and compaction output latency, and extra round trips on S3/GCS/Azure.
- Recommendation: rename into the local store root; upload parts in parallel to the final key, keeping the create-only check as a conditional PUT on the last step.

---

## 8. Transaction pipeline

### Structure: equivalent

- RTDB:
  - submission (`R/node/src/node.rs:253` → `R/tx/src/submission.rs`, static INSERT expanded once, like XTDB's `toStaticOps` with `dml-cache`);
  - the source log;
  - `ReplicationDriver::run_inner` (`R/replication/src/driver.rs`), a single `tokio::select!` loop with one resolution in flight (`resolve_next_source`);
  - the replica log append (`appender.rs`);
  - apply (`apply_admitted`);
  - the block cut at `rows_per_block` (`enqueue_resolved` → `cut`).
- This mirrors `XK/indexer/LeaderLogProcessor.kt`, `TxResolver.kt`, `SourceLogTxIndexer.kt`, `ReplicaLogAppender.kt`, `BlockCutter.kt`.
- Thresholds:
  - RTDB's `ReplicationConfig::default` uses `rows_per_block` 100,000 and `flush_timeout` 4 h. The server config default is 102,400 (`R/server/src/config/mod.rs:202`).
  - XTDB uses 102,400 and a 15 min `flushDuration` (`XK/api/IndexerConfig.kt`).

### SQL DML with argument rows

- RTDB `R/tx/src/resolver.rs`:
  - `apply_inner` (`:363-370`) loops `for (index, row) in arguments.indexed_rows() { self.execute_sql_typed(sql, Some(row), …) }`.
  - `execute_sql_typed` (`:285-297`) does, **per row**: `self.snapshot()`, which runs `TransactionBuffer::snapshot` (re-encode the new events, append a cloned segment); a new `query_context`; `admit_sql_syntax`; `rtdb_sql::prepare_tx_in` (full parse, bind and plan); a new `PreparedQuery` in `execute`.
  - `execute_mutation` (`:328-345`) then reads each result cell back into a `Value` (`read_value`) and pushes `StoredEvent`s.
- XTDB also runs the statement once per argument row (`SourceLogTxIndexer.forEachArgRow`, `XK/indexer/SourceLogTxIndexer.kt:265-283`, slicing each row with `paramRel.select(selection)`).
  - But `prepareTxSql` → `prepareRa` hits the parse cache (`XK/query/SqlParser.kt` `singleQueryCache`, 4096), the plan cache (`XC/query.clj` `:plan-cache`, 4096, `PlanCacheKey` = AST + dbs + arg fields + table info) and the emit cache (`:emit-cache` per plan, keyed by scan and param types).
  - Results are written as Arrow (`writePuts`).
- Impact: high for parameterised UPDATE/DELETE/ASSERT/INSERT…SELECT batches and for the 1,000-update bitemporal transaction. The per-row cost is a full SQL front end plus a snapshot rebuild.
- Recommendation:
  - prepare the mutation once per (SQL text, argument types) and re-execute it with new parameters;
  - make `TransactionBuffer::snapshot` cheap when nothing new has been written since the last read (it already checks), and when only one row was added (append to a growable builder);
  - write mutation output batches straight into the transaction's Arrow buffer.

### Apply and block flush

- `PartitionState::apply` computes under the write lock (§1).
- `BlockStore::produce_cancellable` (`R/replication/src/blocks.rs:208-266`) runs as a future inside the driver's select loop, writes tables **sequentially**, and encodes leaves synchronously on the async worker (`write_node`, `BlockPages::copy` → `interleave_distinct_rows`, with a per-row `Value` fallback when chunk schemas differ, `live.rs:319-352`).
  - XTDB fans tables out on the IO dispatcher (`LiveTable.finishBlock` on `Map<TableRef, LiveTable>`, `XK/indexer/LiveTable.kt`).
- Impact: medium. Flush time grows with the number of tables, and the per-row fallback applies whenever union legs differ.
- Recommendation: run block encoding with `spawn_blocking` per table, and fix chunk-schema divergence (§1) so the `interleave` path always applies.

### Back-pressure and group commit

- The two are comparable: `command_capacity` 64, deferred commands ≤ 128, held replica records capped by `pending_block_records`/bytes. Log fsync batches up to 10 in both.
- The measured concurrent-writer throughput is level (1,507 vs 1,517 tx/s).

---

## 9. Compaction and GC

- **Job selection: equivalent.**
  - RTDB `R/maintenance/src/job.rs` `available_jobs`: L0 + partial L1 → L1 (recency-partitioned), L1H → L2H, fan-out of 4 per level by part, 1024-row pages.
  - This mirrors `XC/compactor/job_calculator.clj` and `dev/doc/compaction.allium`.
- **Merge: divergent.**
  - RTDB `SegmentCompactor::execute` (`R/maintenance/src/merge.rs:230-322`) decodes every input row into `StoredEvent` (`events.read_event(row)`), deep-clones each output event, re-encodes through `copy_event_batch_with_recency`, then decodes and re-encodes again in `merge/layout.rs` `normalize`.
  - Inputs are first downloaded to temp files in 1 MB chunks, even from `LocalStore`.
  - XTDB `XK/compactor/SegmentMerge.kt` copies with vector-level `rowCopier`s (`OutWriter.kt`) over pages loaded through the buffer pool.
- **Threading: divergent.**
  - RTDB `CompactionDriver::run` (`R/maintenance/src/driver.rs:71-133`) polls jobs as futures in one `FuturesUnordered` on one task. With no `spawn` or `spawn_blocking`, `compaction_threads = 2` gives concurrency but not parallelism. CPU work and blocking `std::fs` I/O run on a tokio worker, broken up only by a `yield_now`.
  - XTDB runs on `ioDispatcher.limitedParallelism(threadCount)` with CPUs/2 threads (`XK/compactor/Compactor.kt:181`).
- **Impact:** very high on compaction throughput (inference: about 10× the CPU per row). This produced the measured ingest gap ("compaction decodes and re-encodes every event").
  - It also stalls whichever queries share that worker.
  - Most importantly for reads, it lets L0 files accumulate, so every point lookup visits more files (§3).
- **GC:** comparable. `R/maintenance/src/gc.rs` (`TrieCollector`, `BlockCollector`, `buffer_unordered`) versus `XK/garbage_collector/`.
- **Recommendation:**
  - merge columnar: per output page, gather `(source page, row)` indices in IID/system-time order, then `interleave` columns, the same as `BlockPages::copy`;
  - run each job on `spawn_blocking` or a dedicated rayon/thread pool sized by `compaction_threads`;
  - read inputs through the range-reading buffer pool rather than temp-file downloads.

---

## 10. Front ends

### Connection model

- XTDB `XC/pgwire.clj` (`serve`, `conn-loop`, `start-client-reader!`): one virtual-thread handler per connection plus a reader thread with a 64-message queue. Concurrent connections are capped by `num-threads`. Output goes through a 1 KiB `BufferedOutputStream` (`XC/pgwire/io.clj` `socket-buffer-size`).
- RTDB `R/pgwire/src/server.rs`, `connection.rs`: a tokio task per connection with a reader task and `mpsc::channel(64)`, and a 64 KiB `BufWriter`.
- RTDB's model is equivalent or better: fewer syscalls and no connection ceiling. Query execution runs on the connection task, i.e. on runtime workers.

### Parse, plan and emit caching

- **XTDB:**
  - parse cache by SQL string (`XK/query/SqlParser.kt:66-80`);
  - plan cache (`XC/query.clj` `->query-source`, `PlanCacheKey`);
  - emit cache per plan (`emit-query`);
  - DML skeleton cache and `table-chains-cache` (`XC/sql.clj:3388`).
- **RTDB:**
  - `PreparedSql` (`R/node/src/statement.rs`) keeps its last optimized `PreparedQuery` and reuses it while its planning options and catalog lookups replay the same.
  - The node's `SharedPlans` (`R/query/src/shared_plans.rs`) shares query plans between sessions: preparing or rebinding a statement reuses a plan with the same source and planning options whose recorded catalog lookups replay the same answers, so a new pgwire Parse or Flight request for a known query skips binding and optimization.
  - It holds 128 plans in a budget of 1/64 of the execution budget, capped at 16 MiB; DML, `EXPLAIN` and transaction-resolver plans are not shared.
  - Each statement is still parsed once per preparation; there is no parse cache keyed by SQL text, and no emit cache.
- **Impact:** medium for short queries prepared afresh per request; parsing remains a per-preparation constant.
- **Recommendation:**
  - share DML plans prepared once per (SQL text, argument types) through the transaction resolver;
  - cache parsed syntax by SQL text ahead of the plan cache.

### Result encoding

- XTDB `cmd-exec-query` → `serialize-row` → `PgType.writeBinary/writeText(env, rdr, idx)` reads straight from the vector reader (`XK/pgwire/PgType.kt`).
- RTDB `ProtocolSession::stream` (`R/pgwire/src/protocol.rs:369`), per cell:
  - reads a `Value` (`read_value_as`);
  - calls `rtdb_pg_codec::encode`, which takes another `MemoryBudget` reservation, allocates a `Vec<u8>`, and for `Default`/JSON columns runs `to_ascii_lowercase` per cell (`R/pg-codec/src/output.rs:28-49`);
  - writes separately with `write_i32` + `write_all`.
  - There is also a reservation per row.
- Impact: medium-high for wide or long results.
- Recommendation: encode from `ArrayRef` by column type into a reused row buffer, with one reservation per batch.

### Ingest and parameter batches

- Flight SQL bulk ingest:
  - XTDB: `XK/flight_sql/XtdbProducer.kt` `acceptPutStatementBulkIngest` → `Connection.bulkIngest` → `TxOp.PutDocs` with an Arrow `RelationReader`.
  - RTDB: `R/flight-sql/src/upload.rs` `documents` turns each batch into `Vec<StructFields<Value>>`, cloning the column name per cell. The resolver then walks documents to compute IIDs (`R/tx/src/source.rs`), and the buffer re-encodes them (§1).
  - This matches the measured 0.65× Flight SQL bulk ingest.
- `executemany` over Flight SQL collapses into one argument batch on both sides, but RTDB's `SqlArguments` is rows of `Value`, and the resolver then re-plans per row (§8).
- Pipelined pgwire Executes run one tx op each on both sides.
- Recommendation: keep `PutDocs` documents as an Arrow `RecordBatch` from upload through the source log (already Arrow IPC, `R/tx/src/ipc.rs`) into `IndexBatch`, computing IIDs column-wise.

---

## 11. Concurrency model and memory management

### Runtime

- RTDB runs one multi-threaded tokio runtime (`R/server/src/main.rs`). Per database there are a replication driver task, a compaction driver task, GC tasks and log writers (`R/node/src/database.rs:160-247`). Blocking file I/O goes through `IoRegion::run` → `spawn_blocking`.
- These CPU-heavy paths run inline on async workers: compaction, block encode, query execution.
- XTDB uses virtual threads for connections and coroutine dispatchers (`Dispatchers.IO`, `limitedParallelism`) for compaction and block writes.
- Impact: a long compaction or block-encode step can delay unrelated queries on the same worker (inference).

### Lock hot spots in RTDB

- `PartitionState.value: RwLock<Arc<PartitionValue>>`: written during apply, `add_tries` and `adopt_block` with the computation inside the lock (`R/index/src/partition.rs`).
- `LiveTable.located: Mutex<HashMap>`, taken per leaf per scan (`live.rs:170`).
- `TransactionBuffer.segments: Mutex`, taken per in-transaction read.
- The three global scan cache mutexes, plus `BufferPool.entries` and `DiskCache.state`.
- `node/src/catalog.rs` `RwLock<Arc<DatabaseSet>>`.
- XTDB's equivalents are lock-free persistent values (`StateFlow` + `PersistentHashMap`), `ConcurrentHashMap` and Caffeine.

### Memory accounting

- XTDB:
  - each query gets a child `BufferAllocator` (`XC/query.clj:439`), and each pgwire connection its own (`XC/pgwire.clj:1673`);
  - accounting happens per Arrow buffer allocation, which grows geometrically;
  - operators bound memory by row thresholds.
- RTDB `R/types/src/memory.rs` `MemoryBudget::reserve`:
  - allocates an `Arc<MemoryReservation>`, then runs a CAS loop and `fetch_max` on **every ancestor up to one node-wide 512 MiB root** (`R/node/src/config.rs` `execution_memory_bytes`);
  - `scope()` adds a level without a tighter limit.
- RTDB reserves at these places:
  - per `OwnedRow`;
  - per evaluated row (twice in `evaluate_invocation`);
  - per cell compared (`rows_equal` → `EvaluationScope::new`, 4 KiB);
  - per pgwire cell;
  - per transaction event (`TransactionBuffer::append`);
  - per compaction row;
  - per spill row;
  - many times per scan initialisation;
  - and through `AllocationScope` (`R/types/src/allocation.rs`), which adds a `Mutex` per reservation.
- Bounds are deliberately pessimistic: 4 KiB scratch, ×8 and ×64 spill factors, up-front cell-size passes in `ColumnBatch::rows_admitting` (inference: early `rtdb/memory-budget-exceeded` under concurrency).
- Impact: high CPU in row paths, and cache-line contention on the root counter across all connections and background tasks (inference).
- Recommendation:
  - charge per page or per buffer growth through a per-operator lease;
  - give each query a child budget that batches its charges to the root (e.g. reserve in 1 MiB quanta);
  - remove the per-cell scope in `rows_equal`.

---

## 12. Other divergences found

- **Cancellation checks.** Most row operators check cancellation per row (`check_cancelled`, which takes the token's lock per its comment). Only the scan heap uses `check_cancelled_at` (every N rows). XTDB checks by thread interrupt. Low-medium.
- **Fused filters under profiling.** Tracing and EXPLAIN ANALYZE run the production plan, including fused stacked filters (`R/query/src/operators.rs`); EXPLAIN ANALYZE reports each fused `select`, while a trace has one span for the stack where XTDB has one per `select`.
- **Different block-size defaults.** `rows_per_block` is 100,000 in `ReplicationConfig::default` (`R/replication/src/driver.rs:51`) and 102,400 in server config. This is harmless, but embedded and server nodes cut blocks at different points.
- **Data files are validated on first decode** (`scan.rs` `load`, IID-order check per row). XTDB trusts its own files. This is a one-off cost per page thanks to the page cache.
- **Better than XTDB in RTDB:**
  - startup (no JVM, no class loading);
  - footprint;
  - the 64 KiB pgwire buffer;
  - top-k up to 10,000;
  - a per-page `interleave` gather for flat columns that beats XTDB's full scans to the client;
  - no connection ceiling.

---

## Ranked divergences by likely performance cost

Ranking is impact × breadth, judged from the code and the benchmark pattern (inference throughout).

| # | Divergence | Where it costs | One-line recommendation | Files |
| --: | --- | --- | --- | --- |
| 1 | Point-lookup scan walks every row of each candidate leaf through a heap and the full polygon resolver, per file: no in-page IID binary search, no as-of early exit | Point lookups, UPDATE/DELETE by `_id`, read-your-write; grows with files and versions. Not the measured 1M-row gap: `buffer_task` binary-searches each page to its task's IID range and drops rows outside `scan.iids` before resolving, so the per-row cost is the heap | Port `SingleIidSelector`/`MultiIidSelector` and `AsOfResolver`; skip leaves with no candidate IID before loading | `R/query/src/scan.rs` (`buffer_task`, `initialize`), `R/temporal/src/lib.rs` (`HistoryResolver`); ref `XK/operator/scan/ScanCursor.kt`, `SingleIidSelector.kt`, `EntityResolver.kt` |
| 2 | `BufferPool::get` reads the whole object into heap memory, then pages are copied again and cached a second time decoded | Cold reads over L1/L2 files (100 MB target against a 256 MB pool), cache thrash, memory | Footer/layout cache plus per-page range reads (pread/mmap/`get_range`); single decoded-page tier | `R/storage/src/cache.rs`, `R/format/src/ipc.rs` (`read_block`), `R/query/src/scan.rs` (`load`, `metadata_cache`), `R/storage/src/disk_cache.rs` |
| 3 | No parse or emit cache, and query plans are shared between sessions only through `SharedPlans`; DML re-parses and re-plans per argument row and rebuilds the tx snapshot per row | Every short query, every pgwire Execute and Flight DoGet, batched DML | Parse cache; prepare mutations once per (SQL, arg types) | `R/node/src/statement.rs`, `R/pgwire/src/protocol.rs`, `R/flight-sql/src/execute.rs`, `R/tx/src/resolver.rs:285-370`, `R/index/src/live.rs` (`TransactionBuffer::snapshot`) |
| 4 | Row-at-a-time `Value` operators: joins, aggregates, distinct, window, set ops, unnest all copy `OwnedRow`s; `BatchJoin` covers a narrow case | Joins (0.43×, 4.5× at 1M), group-by, anything above a scan | Make the batch path the default for joins and aggregates: Arrow keys, group-id vectors, `take` outputs | `R/query/src/join.rs`, `join_storage.rs`, `batch_join.rs`, `aggregate.rs`, `operators.rs`, `cursor.rs` |
| 5 | Interpreted expressions with per-call name lowering, type checking and allocations; kernels exclude unions with string legs, temporal and extension types; fallback materialises every column | Filters and projections over union `_id` (pgwire), temporal columns and functions | Resolve calls at prepare; borrow cells; materialise only read columns; cache DataFusion lowering; extend kernels to unions and timestamps | `R/expr/src/evaluate*.rs`, `functions.rs`, `R/query/src/columnar.rs` (`kernel`, `BatchFilter`), `vector.rs`, `context.rs` |
| 6 | Compaction decodes every event to `Value`, re-encodes twice, and runs as futures on one tokio task (no parallelism) | Ingest throughput, worker stalls, L0 backlog that worsens #1 | Columnar index gather plus `interleave`; run jobs on a blocking pool sized by `compaction_threads` | `R/maintenance/src/merge.rs`, `merge/layout.rs`, `driver.rs`; ref `XK/compactor/SegmentMerge.kt`, `OutWriter.kt` |
| 7 | Spill is per-row `serde_json`; four operators rescan a spill file per row or group (INTERSECT/EXCEPT, DISTINCT aggs, ARRAY_AGG/PERCENTILE, window partitions); sort spills after 8 MiB | Any large query; asymptotic blow-ups | Arrow IPC spill; hash structures instead of rescans; count sort blocks by rows | `R/query/src/spill.rs`, `operators.rs` (`SetCursor`), `aggregate.rs`, `window.rs`, `sort.rs`, `context.rs:112` |
| 8 | Fine-grained `MemoryBudget` reservations (Arc plus CAS chain to the node root) per row, cell, event and comparison | CPU everywhere in row paths; cross-connection contention | Per-operator leases charged per page; per-query child budget reserving in quanta | `R/types/src/memory.rs`, `allocation.rs`, callers throughout `R/query`, `R/pgwire`, `R/pg-codec`, `R/index/src/live.rs` |
| 9 | System columns (`_valid_*`, `_system_*`, `_iid`) gathered as `Value` with a `String` timezone per cell; nested columns too | Temporal queries, all DML inputs, history queries | Emit `TimestampMicrosecondArray` and `FixedSizeBinaryArray` | `R/query/src/columnar.rs` (`gather`, `time`) |
| 10 | Write path is Arrow → `Value` documents → Arrow (Flight ingest, PutDocs, DML results, `TransactionBuffer`); live chunks split by union-leg schema differences | Bulk ingest (0.65× Flight), per-transaction apply, block flush fallback | Keep documents as Arrow end to end; widen union legs so chunks merge | `R/flight-sql/src/upload.rs`, `R/tx/src/resolver.rs`, `R/tx/src/source.rs`, `R/index/src/live.rs` |
| 11 | Leaf metadata materialised as `Value` trees; page tests by string search; bloom bitmaps deserialised per check | Point lookups and filtered scans over many files | Arrow-resident statistics with a per-file `(col, page)` index; zero-copy bloom test | `R/format/src/metadata.rs`, `R/query/src/page_metadata.rs`, `R/query/src/pushdown.rs`, `R/format/src/bloom.rs` |
| 12 | pgwire encodes Arrow → `Value` → `Vec<u8>` per cell with reservations and lowercase per cell | Wide or long pgwire results | Encode from `ArrayRef` into a reused buffer | `R/pgwire/src/protocol.rs` (`stream`), `R/pg-codec/src/output.rs`, `R/pgwire/src/wire.rs` |
| 13 | Global mutex caches with String keys and O(n) retain/evict per insert; `BufferPool` and disk cache sort or rescan under lock | Concurrent readers, large scans | Sharded or lock-free cache (`moka`), interned keys, O(1) LRU | `R/query/src/scan.rs` (`metadata_cache`), `R/storage/src/cache.rs`, `disk_cache.rs` |
| 14 | `PartitionState::apply`, `add_tries` and `adopt_block` compute under the write lock; the trie catalog is deep-cloned per change; located-leaf cache reset per commit | Read latency under write load; grows with tries | Compute outside the lock and swap; persistent maps; keep leaf caches across appends | `R/index/src/partition.rs`, `trie_catalog.rs`, `live.rs` |
| 15 | Log replay tail reads one record per two opens and two `spawn_blocking`s; startup scans the whole log | Single-row tx latency; startup time grows with log size | Persistent handle with batched reads; read the trailer at startup | `R/storage/src/log/local.rs`, `R/replication/src/driver.rs` |
| 16 | Publication: tempfile + fsync + copy + fsync locally; temp-key multipart + server-side copy + delete remotely, with sequential parts; sequential 1 MB GETs on disk-cache miss | Block flush, compaction output, cold remote reads | Rename into place; parallel parts to the final key; one streamed GET | `R/storage/src/object_store.rs`, `remote.rs`, `disk_cache.rs`, `R/replication/src/blocks.rs` |
| 17 | Block production encodes tables sequentially on the driver task | Flush pauses with many tables | `spawn_blocking` per table | `R/replication/src/blocks.rs` |
| 18 | `equality_hash` gave every temporal-valued row the same hash (fixed, except for sets and maps holding a date-time) | DISTINCT/UNION on temporal columns became O(N²) | Hash a normalised instant | `R/query/src/operators.rs` `equality_cell_hash` |
