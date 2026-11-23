# Vector search in RTDB

This draft evaluates vector retrieval over RTDB's existing bitemporal tables.
The primary scan remains the authority for row visibility and SQL predicates.

## Exact retrieval

The native SQL function `COSINE_DISTANCE(left, right)` accepts numeric lists with the same positive dimension and returns a distance between 0 and 2.
A null input produces null, and either vector with a zero norm or a non-finite or non-numeric component raises an error.
The exact path resolves rows at the read basis, applies predicates, and selects the nearest eligible rows.

```sql
SELECT _id, COSINE_DISTANCE(embedding, ARRAY[1, 0, 0]) AS distance
FROM documents
WHERE status = 'open' AND embedding IS NOT NULL
ORDER BY distance, _id
LIMIT 10;
```
It serves as the oracle for approximate retrieval.
The ordered secondary index can provide a bounded IID scope for selective predicates before the query reads embeddings.

## Approximate retrieval

The experiment will use an explicit approximate query path.
It will probe immutable centroid blocks, include the uncovered writes at the captured basis, and rerank verified candidates with their full-precision vectors.
Initial ANN coverage is a current valid-time and system-time point.
The exact path covers historical instants and temporal ranges.

Chroma's indexed-segment and recent-log split motivates the exact overlay.
RTDB must reconcile the overlay at both temporal axes, including corrections, future validity, metadata-only updates, deletes, and erases.
Turbopuffer's centroid layout motivates batched range reads and filter-aware probing.
SlateDB's checkpoint and reader lifetime model motivates publishing coverage with the immutable generation and retaining the referenced data while a reader holds it.

- [Chroma query execution](https://www.trychroma.com/engineering/execution-engine)
- [turbopuffer's native filtering](https://turbopuffer.com/blog/native-filtering)
- [SlateDB reader checkpoints](https://slatedb.io/docs/design/readers/)

## Measurements

Compare exhaustive SQL, ordered-filter exact SQL, and centroid ANN at 100,000 and 1 million embeddings.
Use the public ANN-Benchmarks GloVe angular data with held-out queries.
Sweep selectivity with random and clustered metadata.
Record recall@10, result count, warm latency, cold object reads and bytes, build and update costs, and index bytes written per source update.
Compare temporal correctness to the exact SQL path, including retained readers across a concurrent write and coverage invalidation after a flush.

### Running the prototype

The prototype runs in the native Node benchmark and exposes approximate retrieval as a separate mode within that workload.
The SQL distance operator always computes exact distances.
The centroid index packs float32 vectors, numeric fixture IDs, and 16-byte IIDs into one Arrow IPC stream per cluster in an immutable object.
The benchmark uses spherical k-means for the centroids and ranks up to 64 candidates from the probed clusters before reranking with SQL.
The emulator serves that object from RTDB's local object store.
The cold ANN variant uses a fresh buffer pool for the index object; the warm variant admits the entire index object once before timing.
The operating system may still cache local file data.
These timings describe local object reads and do not include cloud latency.

The harness builds two ordinary ordered indexes for the structured predicates.
It compares each exact query against the same captured context with scan acceleration disabled.
The random metadata is deterministic and independent of the assigned centroid.
The clustered predicate groups whole centroid clusters and allows the prototype to prune clusters before opening their byte ranges.
This tests an ideal case for cluster summaries; a real summary would need to be derived from snapshot coverage and maintained with changes to indexed fields.

```sh
mkdir -p build/vector-search
curl -fL https://ann-benchmarks.com/glove-100-angular.hdf5 \
  -o build/vector-search/glove-100-angular.hdf5

for rows in 100000 1000000; do
  OPENBLAS_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=4 \
    uv run --no-project --with numpy==2.5.5 --with h5py==3.16.0 --with pyarrow==22.0.0 \
    python rust/crates/node/examples/support/vector_benchmark_data.py \
    build/vector-search/glove-100-angular.hdf5 "build/vector-search/glove-$rows" --rows "$rows"
done

cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node \
  --example fts_architecture_bench -- vectors build/vector-search/glove-100000 12 \
  > build/vector-search/result-100000.json
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node \
  --example fts_architecture_bench -- vectors-full build/vector-search/glove-1000000 12 \
  > build/vector-search/result-1000000.json
```

The downloaded GloVe data used for the run has SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`.
The run uses the first twelve held-out queries and independently trains 512 centroids for each size.
The dataset and generated Arrow IPC streams stay in the ignored build directory.

The experiment loads the primary table and centroid index from the same fixed input.
The benchmark owns the ANN generation for one process.
The index retains the fixture's valid-time point and captured flushed frontier.
The churn prototype reads the captured live segments for writes that change a row at the covered point.
A put or delete whose valid interval contains the point shadows the indexed version of that row.
An erase shadows the indexed version across valid time.
Changed IDs bypass centroid selection and the static metadata summaries.
The primary scan resolves their visible versions, reapplies predicates, and reranks them together with the untouched shortlist.
A different valid-time point, an advanced flushed frontier, a transaction overlay, or a tail over 1,024 events takes the exact SQL path.
A production generation would need a covered valid-time interval or an explicit schedule of validity boundaries, and durable publication and retirement tied to existing snapshot references.
An erase must eventually retire the vector bytes as well as removing the row from search results.

## Release measurements

The measurements use an Apple M4 Max, a locked Rust release build, and 100-dimensional GloVe vectors.
Within each size, primary table queries use the same captured snapshot and storage backend.
The 100,000-row fixture uses the in-memory table store and log.
The million-row fixture uses local file-backed table storage and logs so historical ordered-index pages and log records stay off the process heap.
It builds each index once and reuses the table for both the stationary and churn runs.
The exact paths alternate order for each query.
The 100,000-row stationary baseline uses commit `fe910aeb7`.
The churn experiment adds removal of stale versions before the candidate limit is applied.
Latency cells show p50 / p95 in milliseconds.
p95 is nearest rank and equals the largest of the twelve per-query samples.
Cold ANN latency includes probing, local range reads and SQL reranking.
Only the ANN object is cold, and its range counts exclude primary table reads.

### 100,000 embeddings

All 60 paired exact SQL queries matched and all five temporal checks passed.
Each ANN configuration returned the same rows with a fresh and a fully warmed ANN buffer pool.
The table load took 10.76 s, building the two ordered indexes took 82.35 s, and packing and publishing the centroid ranges took 0.61 s after offline training.
The packed object occupied 40,400,000 bytes.
The live insert took 1.49 ms and wrote zero bytes to that object.

| Filter | Exact scan | Ordered acceleration enabled |
| --- | ---: | ---: |
| None | 2,730.30 / 3,263.24 | 2,732.52 / 3,371.70 |
| Random, ~0.1% | 495.05 / 792.86 | 26.69 / 132.00 |
| Random, ~1% | 836.34 / 1,243.31 | 44.12 / 133.83 |
| Random, ~10% | 912.21 / 1,547.71 | 355.15 / 538.54 |
| Whole clusters, ~1% | 773.30 / 1,341.49 | 66.52 / 147.90 |

| Filter | Probes | Warm ANN | Cold ANN | Mean recall@10 | Below exact result count | Cold range reads / MiB, p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 8 | 24.40 / 170.45 | 35.73 / 115.54 | 57.5% | 0 / 12 | 8 / 0.74 |
| None | 32 | 21.09 / 68.53 | 64.69 / 163.31 | 79.2% | 0 / 12 | 32 / 2.71 |
| Random, ~0.1% | 8 | 0.48 / 17.90 | 5.85 / 32.83 | 10.8% | 12 / 12 | 8 / 0.74 |
| Random, ~0.1% | 32 | 2.78 / 69.27 | 31.90 / 69.07 | 26.7% | 9 / 12 | 32 / 2.71 |
| Random, ~1% | 8 | 3.94 / 69.46 | 41.21 / 190.16 | 17.5% | 2 / 12 | 8 / 0.74 |
| Random, ~1% | 32 | 33.43 / 99.71 | 32.44 / 163.31 | 42.5% | 0 / 12 | 32 / 2.71 |
| Random, ~10% | 8 | 12.55 / 116.52 | 47.52 / 204.94 | 42.5% | 0 / 12 | 8 / 0.74 |
| Random, ~10% | 32 | 26.10 / 142.31 | 57.46 / 96.01 | 63.3% | 0 / 12 | 32 / 2.71 |
| Whole clusters, ~1% | 8 | 18.06 / 118.86 | 65.33 / 147.66 | 100% | 0 / 12 | 6 / 0.47 |
| Whole clusters, ~1% | 32 | 11.78 / 139.76 | 53.79 / 223.86 | 100% | 0 / 12 | 6 / 0.47 |

At 32 probes the unfiltered worst query recalled 40% of its exact top 10.
The ~1% random filter filled all twelve pages at that probe budget and still recalled only 42.5% on average.
Result count alone is therefore an inadequate signal that the search has covered a filter.
The whole-cluster predicate could open every qualifying cluster within eight probes.
The stationary random filters selected 118, 1,042, and 9,925 eligible rows, and the whole-cluster filter selected 1,228.

### 1,000,000 embeddings

This baseline uses commit `9cdfc768b` and the original scalar primary-table scoring and centroid range format.
All 60 paired stationary exact queries matched, all five temporal checks passed, and warm and cold ANN returned identical rows.
Loading and flushing the table took 36.00 s.
Completing the ordered index builds took another 46 min 27 s.
Packing and publishing the centroid ranges took 3.51 s after offline training.
The packed index occupied 404,000,000 bytes, and the live-tail IID lookup occupied another 24,000,000 bytes.
The live insert took 12.43 ms and wrote zero bytes to the packed index.

| Filter | Exact scan | Ordered acceleration enabled |
| --- | ---: | ---: |
| None | 6,210.99 / 8,977.12 | 6,234.87 / 7,034.15 |
| Random, ~0.1% | 2,078.09 / 2,210.88 | 232.81 / 530.30 |
| Random, ~1% | 2,215.96 / 2,325.65 | 329.10 / 987.88 |
| Random, ~10% | 2,595.38 / 3,213.08 | 2,627.79 / 3,066.65 |
| Whole clusters, ~1% | 2,215.16 / 2,403.48 | 351.73 / 2,303.24 |

| Filter | Probes | Warm ANN | Cold ANN | Mean recall@10 | Below exact result count | Cold range reads / MiB, p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 8 | 173.43 / 192.14 | 174.13 / 192.65 | 74.2% | 0 / 12 | 8 / 6.68 |
| None | 32 | 172.25 / 189.12 | 170.17 / 192.41 | 91.7% | 0 / 12 | 32 / 26.03 |
| Random, ~0.1% | 8 | 86.26 / 104.06 | 86.97 / 101.61 | 23.3% | 0 / 12 | 8 / 6.68 |
| Random, ~0.1% | 32 | 160.06 / 207.34 | 161.37 / 196.05 | 46.7% | 0 / 12 | 32 / 26.03 |
| Random, ~1% | 8 | 177.94 / 190.86 | 177.60 / 187.20 | 35.8% | 0 / 12 | 8 / 6.68 |
| Random, ~1% | 32 | 170.43 / 191.66 | 172.91 / 190.65 | 63.3% | 0 / 12 | 32 / 26.03 |
| Random, ~10% | 8 | 166.09 / 211.71 | 168.60 / 216.56 | 55.0% | 0 / 12 | 8 / 6.68 |
| Random, ~10% | 32 | 169.43 / 227.08 | 170.98 / 214.71 | 76.7% | 0 / 12 | 32 / 26.03 |
| Whole clusters, ~1% | 8 | 168.90 / 184.85 | 169.80 / 185.50 | 100.0% | 0 / 12 | 6 / 5.17 |
| Whole clusters, ~1% | 32 | 170.37 / 184.96 | 170.29 / 182.28 | 100.0% | 0 / 12 | 6 / 5.17 |

At 32 probes the unfiltered worst query recalled 70% of its exact top ten.
The ~0.1% random filter always filled ten positions but averaged 46.7% recall at 32 probes, including one query with zero matches in the exact top ten.
The random filters selected 1,054, 9,982, and 99,573 rows, and the whole-cluster filter selected 13,429.
The 99,573-row scope exceeded the ordered candidate cap of 16,384 and fell back to a scan.

## Churn and snapshot coverage

```sh
cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node \
  --example fts_architecture_bench -- vectors-churn build/vector-search/glove-100000 4 \
  > build/vector-search/churn-100000.json
```

The million-row `vectors-full` command includes the churn run.
The churn run first tests an unindexed nearest neighbor, a retained snapshot, a bounded backdated correction, a future valid-time boundary, and an exact fallback after flushing.
For the additional churn workload, a single transaction removes the original shortlist of 64 nearest neighbors using a mixture of deletes and erases.
It also moves an existing row into the selective filters without changing its vector and inserts a new nearest neighbor.
A reader with a captured older basis runs concurrently with that transaction and checks its previous result.
The measurement then runs four held-out queries at both probe budgets across the same five filters.

The 100,000-row run removed 33 rows by delete and 31 by erase.
Reranking the stale shortlist plus the two puts yielded just two rows.
Shadowing stale entries before shortlisting restored all ten exact neighbors when all clusters were probed, with 67 changed IIDs.
All 20 paired exact SQL results matched and ANN warm/cold results also matched.
The churn transaction took 121.63 ms while the older reader was active and left the immutable ANN object untouched.
The IID-to-ID lookup added 2,400,000 bytes of resident metadata at this size.

| Filter | Exact p50, ms | Ordered p50, ms | 8 probes recall@10 | 32 probes recall@10 | 32 probes ANN warm / cold p50, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| None | 608.22 | 610.06 | 55.0% | 82.5% | 28.52 / 28.13 |
| Random, ~0.1% | 120.64 | 16.64 | 15.0% | 30.0% | 11.51 / 12.03 |
| Random, ~1% | 204.01 | 25.77 | 27.5% | 60.0% | 28.02 / 28.66 |
| Random, ~10% | 242.62 | 86.87 | 47.5% | 72.5% | 27.73 / 28.13 |
| Whole clusters, ~1% | 205.19 | 26.61 | 100% | 100% | 28.29 / 28.63 |

The million-row churn run removed 27 rows by delete and 37 by erase.
Reranking the stale shortlist plus the two puts returned two rows.
Shadowing 67 changed IIDs before shortlisting restored all ten exact neighbors when all 512 clusters were probed.
All 20 paired exact results matched, as did warm and cold ANN for each probe budget.
The churn transaction took 3.52 ms while the older reader was active.

| Filter | Exact p50, ms | Ordered p50, ms | 8 probes recall@10 | 32 probes recall@10 | 32 probes ANN warm / cold p50, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| None | 6,222.01 | 6,203.93 | 62.5% | 82.5% | 237.97 / 239.48 |
| Random, ~0.1% | 2,050.56 | 310.25 | 20.0% | 50.0% | 233.64 / 229.88 |
| Random, ~1% | 2,191.12 | 366.30 | 47.5% | 75.0% | 236.54 / 235.42 |
| Random, ~10% | 2,587.09 | 2,653.17 | 60.0% | 80.0% | 245.12 / 243.38 |
| Whole clusters, ~1% | 2,229.55 | 1,015.39 | 100.0% | 100.0% | 243.07 / 241.43 |

The stationary and churn runs occurred at different levels of host load.
Compare the query paths within each run and use the churn tests and recall results to assess the effect of changed rows.

## Architecture decision

Keep `COSINE_DISTANCE` and filtered exact SQL as the default query contract.
The existing ordered index already gives highly selective filters a faster route without changing any returned distance or row.
For exact retrieval, score compatible fixed-size Arrow float arrays in batches and prepare the query once.
Fetching projected values only for the winners remains a separate optimization.
The scalar oracle remains the reference for distance and tie behavior.

Use a secondary centroid index for the explicit approximate route.
Store stable row identity separately from the cluster address and resolve visible rows through the table snapshot.
Use the ordered scope and exact scoring when the candidate set is cheap to enumerate.
For broad ANN, start with warm centroid metadata, bounded concurrent reads of the relevant blocks, and full-precision reranking through SQL.
The low recall for random selective filters shows why a fixed global probe count cannot serve all filter shapes.
Cluster eligibility and per-cluster matching counts are candidates for adaptive probing, and the exact route supplies a fallback when a useful filtered search cannot stay within its budget.

Production ANN needs a durable definition, a published generation and a precise source frontier.
Keep the search generation pinned for the lifetime of the captured query.
Account for newly visible versions at valid-time boundaries and for the writes outside the published frontier.
An erase requires both immediate exclusion from results and eventual removal of the obsolete vector bytes under the index retirement protocol.
Those are the publication and lifetime obligations to take from the existing ordered index and SlateDB's checkpoint design.

Keep fixed-probe ANN in the benchmark while filtered recall and cold provider I/O remain below the evidence needed for a production planner.
The present cold measurements count local range requests and bytes.
The pack time excludes centroid training, and the churn timings leave full generation rebuilds outside the write path.
The rebuild and generation-retirement costs still need to be measured over sustained writes.

## Verification

The release Rust suites passed for the expression, SQL, query, format, index, transaction, FFI, Flight SQL, and node crates.
The new regressions cover Arrow C Data ingest through flush and query, Flight SQL ingest and INSERT SELECT, replay through the existing put operation, list and component nulls during gather, and ANN on a captured basis during churn.
The format and index suites passed again after the mixed-layout flush change.
The full Rust workspace passed compilation across all targets.

With the rebuilt native library supplied to Gradle, the unit suite passed 3,190 tests and the property suite passed 3,348 cases.
The property results include 3,300 numbered simulation iterations across 33 methods.
The integration suite passed 5,629 tests with one PostgreSQL-source skip and zero failures.
The Gradle runs emitted no reflection or boxed math warnings.

GitHub's JVM jobs fail in native ABI initialization because the workflow invokes Gradle without setting `xtdb.native.library`.
The draft and main runs report the same error at the library-path check. [Draft CI](https://github.com/bijectionhq/rtdb/actions/runs/45087814248) · [Main CI](https://github.com/bijectionhq/rtdb/actions/runs/45087416220)

## Arrow path inventory

The table records the state before the row-operator work below.

The workload uses a dense fixed-size Arrow float list per embedding, scalar predicate columns, and a stable IID per entity.
Arrow IPC is used where the data crosses a durable or process boundary.
In-memory code keeps typed buffers alive across those boundaries and creates new buffers when rows are gathered, reordered, or copied from the foreign allocator.

| Stage | Former conversion | Arrow path and remaining boundaries |
| --- | --- | --- |
| Benchmark preparation | NumPy matrices were written as separate raw float and cluster files, which Rust parsed one scalar at a time. | PyArrow writes fixed-size `float32` lists and `uint32` cluster ordinals in IPC streams. Rust borrows the decoded child buffers. |
| Native Rust, ADBC, and Flight SQL bulk ingest | The client batch was decoded into argument values, copied into per-row document maps, and converted back to Arrow by the source writer. | Compatible fixed-size float batches preserve typed columns and support null embedding rows and nullable scalar metadata. ADBC copies foreign buffers into native Arrow memory so its caller can release the original allocation. Flight retains the decoded batch. |
| SQL text and prepared arguments | SQL literals and generic parameter bindings use scalar values. General document construction can also materialize a projected embedding. | A compatible Arrow document produced by a dynamic SQL mutation can enter transaction staging directly. ADBC and Flight SQL bind fixed-size float vectors as Arrow and cosine reads their component buffers. The scalar evaluator handles SQL literals and bindings used by general expressions. PostgreSQL COPY, PATCH document merging, and incompatible document shapes use their general paths. |
| Source log and replay | Source IPC replay unpacked each vector into component values, transaction staging cloned the row, and the event builder rebuilt the array. | Compatible writes use the existing `put-docs` Arrow union leg with list and scalar validity bitmaps. Replay retains its document arrays and stages them alongside resolved IIDs and temporal intervals. The source protobuf remains the log envelope. |
| Replica log, flush, and compaction | The event copier rebuilt fixed-size list cells through scalar values. | The event copier gathers compatible fixed-size float buffers with list and component validity. It falls back for schema coercion. The replica log and same-layout flush copy Arrow batches, and a flush spanning different layouts can gather compatible columns. Page metadata still decodes fixed-size lists to count non-null cells. |
| Structured predicate indexes | The ordered index reads its key columns from Arrow pages during build and uses its own posting-tree codec for scalar keys and IIDs. | The vector stays in the primary data page. Ordered-index candidate IIDs feed the same exact scorer as an ordinary scan. |
| Exact scan and distance | Scan gather boxed each component, then scalar distance decoded each embedding and prepared the same query repeatedly. | The scan gathers compatible fixed-size float lists directly, including when another visible page lacks that column. A column-to-literal or column-to-parameter cosine expression prepares the query once, reads Float32 or Float64 child buffers, and emits an Arrow Float64 column. Unsupported layouts and general expressions continue through the scalar evaluator. |
| Centroid ranges and live tail | Cluster records decoded each float manually. Tail reconciliation depended on an IID-to-ID dictionary and read whole documents to identify new rows. | Each range is an Arrow IPC stream. The reader borrows the buffer pool's range and reads typed vector and IID arrays. Tail reconciliation uses event-header IIDs, including for new rows. |
| ANN reranking | Candidate IDs were formatted into a SQL disjunction and parsed again. | The prepared plan receives the typed IID scope directly. Primary-table visibility and residual predicates still apply before the Arrow scorer and top-k. |
| Top-k and sort spill | The generic sorter compares scalar key cells and retains result rows as values. | A cosine query with compatible column sort keys and `OFFSET + LIMIT` within the existing 10,000-row top-k bound uses Arrow gathers to retain and reorder the winning embedding rows. It reuses SQL ordering for keys, nulls, and ties. Generic sorts and spill still use the general row sorter. |
| Result export | The benchmark converted result columns back to generic values. PostgreSQL clients use PostgreSQL protocol values. | The benchmark reads typed ID and distance buffers. ADBC exports Arrow C Data and Flight SQL writes Arrow IPC. Result schema conformance shares compatible embedding buffers even when other columns need conversion. Flight holds the bound batch in Arrow until it issues the stateless ticket, which carries the parameter IPC at the wire boundary. PostgreSQL conversion occurs at its wire boundary. |

The Arrow path uses typed buffers in memory and Arrow IPC for persisted pages, log payloads, and Flight transfer. ADBC copies foreign buffers when it takes ownership. Top-k gathers write bounded Arrow result buffers in result order.

The relevant implementation boundaries are `ffi/src/{arrow,statement}.rs`, `flight-sql/src/upload.rs`, `tx/src/{source,values,resolver}.rs`, `index/src/live.rs`, `format/src/data.rs`, `query/src/{columnar,cosine}.rs`, and the native vector benchmark.
A compatible fast path must preserve missing and null values, invalid-component errors, distance and tie behavior, and the table snapshot's visibility.

When full-text indexes share a table with embeddings, the FTS builder and live text-corpus fallback can still read a complete mixed document before extracting text in `search/src/{lane,accelerate,corpus,runtime}.rs`.
That cost belongs to the full-text indexing and text-scoring paths.
The specialized vector benchmark uses ordered predicate indexes and resolves its live tail from headers.

The plain score and the scalar control in the updated benchmark run against the same captured source with scan acceleration disabled.
The scalar control puts the same distance call under a bound true condition so the specialized projection declines it.
The benchmark rotates the control, the plain Arrow score, and the exact ordered path, and asserts identical result IDs and distance bits for each query and filter.

### Arrow embedding storage across row operators

Native Float32 and Float64 lists retain Arrow component arrays across both column and row operators.
A row that needs independent ownership copies the typed component buffer and its validity bitmap.
Row record assembly, parameter execution, document merging, and result reconstruction can then pass or copy that buffer without creating a list of individual values.
Both fixed-size and variable-size Arrow lists keep their logical list kind.
When Float32 and Float64 layouts share a column, the Arrow union preserves the component width and nulls through merges, scans, scoring, and export.
Other SQL collection element types keep their existing semantics.

Arrow COPY borrows the document batch for storage when the input schema passes the typed document checks.
Source and replica encoding, live staging, flush, and compaction assemble supported vector fields from typed components, including when other document fields require row handling.
Page statistics count list parents with Arrow validity, and full-text builds and verification project only the data needed for text search.
Query parameters and embeddings carry typed components into ordinary expression evaluation, including cosine inside a CASE or a comparison.
Column gathers, joins, and distributed fragments preserve vector payloads through result assembly and Arrow export.
Compatible sort keys are compared from their native arrays with the same numeric ordering, null placement, and tie behavior.
Rows carrying embeddings spill the vector components in Arrow IPC together with descriptors for their positions in the row.
Row protocols such as PostgreSQL and JVM Transit encode client values at their interfaces.
Log inspection expands components when rendering its EDN text result.

#### Paired release benchmark

Both release binaries ran `fts_architecture_bench vectors-full build/vector-search/glove-100000 12` on the same Apple M4 Max with the same 100,000 100-dimensional GloVe vectors and in-memory primary storage.
The baseline binary was built at `b211a0ee7`, the last runtime change before audit revision `022b18a35`.
The CASE route uses the row evaluator in both runs and carries Arrow components in the new run.
The direct route uses the columnar cosine kernel in both runs.

| Stationary filter | CASE exact p50, ms, before → after | Direct exact p50, ms, before → after | Ordered exact p50, ms, before → after |
| --- | ---: | ---: | ---: |
| None | 4,614.02 → 497.25 | 564.43 → 203.82 | 565.81 → 203.93 |
| Random, ~0.1% | 12.62 → 7.80 | 6.80 → 6.80 | 5.10 → 5.20 |
| Random, ~1% | 59.59 → 16.41 | 10.44 → 10.34 | 8.82 → 8.88 |
| Random, ~10% | 469.25 → 59.09 | 12.56 → 11.88 | 19.49 → 19.13 |
| Whole clusters, ~1% | 68.16 → 17.40 | 10.49 → 10.39 | 9.17 → 9.11 |

Unfiltered CASE exact search was 9.28× as fast as the prior CASE route, and direct exact search was 2.77× as fast as the prior direct route.
Loading and flushing the fixture took 871.26 ms before and 573.66 ms after.
Under churn, unfiltered CASE p50 went from 4,637.85 to 496.15 ms and direct exact p50 went from 560.93 to 204.23 ms.
Unfiltered warm ANN at 32 probes went from 4.76 to 4.59 ms p50 with 79.2% mean recall@10 in both runs.

Each build passed the exact route comparisons for all 60 stationary and 20 churn query/filter cases, including result ID order and distance bits, and passed five temporal checks.
The two builds gave identical ANN recall and returned counts for every sample.
The Rust workspace passed 5,249 tests with 14 skips via Nextest using a 32 MiB debug test stack, and two doctests passed.
With the rebuilt native library, Gradle passed 3,190 unit tests, 3,348 property tests, and 5,629 integration tests with one skip.

### Current non-Arrow inventory

This source audit covers RTDB vector search at `fa969b755`, from client input and primary storage through exact SQL, the experimental ANN workload, and result export.
The audit follows list constructors and readers, calls that expand list components, row-to-batch conversions, codecs, and the callers of each transport.
An Arrow-backed `ListValue` inside a row is different from a `Vec<Value>` containing one value per component.
Arrow IPC inside a text envelope introduces extra encoding and copies even when the underlying components stay in Arrow format.

A format is required at the boundary that specifies it, such as PostgreSQL Bind and DataRow messages, an explicitly requested JSON result, or a durable log with existing readers.
An operation can also require a changed result shape, such as one row per UNNEST element, while keeping its data in Arrow.
The remaining internal copies, scalar materialization, and protocol choices are engine decisions.
XTDB exposes pgwire and Arrow clients as separate interfaces and keeps Arrow buffers available to its protocol encoders. [XTDB driver guide](https://docs.xtdb.com/drivers)

#### Engine paths that expand or repack components

| Path | What leaves Arrow or starts outside it | Required work and avoidable overhead | Source |
| --- | --- | --- | --- |
| Row-created numeric lists | The row constructor packs homogeneous Float32 or Float64 with optional nulls. Mixed Float32/Float64 created as row values, Float16, integers, decimals, other mixed numeric lists, and untyped empty or all-null lists use `Vec<Value>`. The engine can read a canonical dense Float32/Float64 Arrow union into packed storage, so the physical input matters even for the same mixture of widths. Cosine can read general finite numeric values through the row evaluator. The persisted-page statistics writer has no numeric metadata flavour for Float16. | SQL type and numeric behavior must survive. Representing supported components as individual values is an engine choice. A typed builder can preserve nulls and distinct widths, including typed empty lists. | [List storage](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/types/src/list_value.rs#L191) · [Arrow list reader](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/values.rs#L705) · [Numeric cosine](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/vector.rs#L108) · [Page statistics types](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/format/src/statistics.rs#L158) |
| SQL and logical-plan literals | Array expressions carry one expression per element. The direct scorer can prepare a literal-only array directly from those values. General array constructors evaluate elements into `Vec<Value>` before packing eligible lists. Logical-relation EDN also has a cell-by-cell list literal reader. | SQL and EDN text must be parsed. A bound constant can be packed once from its typed elements. Per-query arrays still have to evaluate their expressions. | [Cosine projection binding](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cosine.rs#L83) · [Expression materialization](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/evaluate/reader.rs#L376) · [EDN literal input](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/xtql/src/bind/ra.rs#L836) |
| SQL operations that change a list | Element casts, trims and other computed lists use readers that yield elements. Materializing those readers builds a component `Vec<Value>` before repacking Float32/64. Direct object casts of arrays also gather cast elements. `ARRAY_AGG` and collection subqueries build lists from scalar inputs or row values. When the aggregate collects whole packed embeddings, each inner embedding retains its Arrow buffer. | Casts and aggregates must compute their output, and trims must select a range. Typed builders or slices can do that work while preserving the existing cast, null and ordering rules. | [List expression reader](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/evaluate/reader.rs#L376) · [Cast reader](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/evaluate/reader.rs#L431) · [Array aggregate](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/aggregate.rs#L500) |
| `UNNEST` and indexed list readers | `UNNEST` calls `execute_list_async`, which expands even a packed embedding into a complete `Vec<Value>` before emitting one row per component. The indexed list event reader also yields batches of scalar values when consuming packed elements. | SQL requires one output row per element. It can emit an Arrow column of elements and use parent-row indices for the accompanying columns. XTDB uses child-vector ranges and indexed parent selection for list unnesting. | [UNNEST](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/operators.rs#L759) · [List events and expansion](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/evaluate/reader.rs#L228) · [XTDB UNNEST](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/clojure/xtdb/operator/unnest.clj#L44-L82) |
| List indexing, comparison, and hashing | Indexing reads one component value. Generic comparisons, including list-valued sort, equality join, grouping and DISTINCT keys, can visit and convert components one by one. The row-list hash also creates one component value at a time. These operations can keep the stored embedding packed while doing scalar work over it. | Reading, comparing, and hashing actual components is semantic work. Typed access can bypass per-component value objects while preserving SQL equality, sort ordering and hash semantics. | [Component access](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/types/src/list_value.rs#L40) · [Reader comparisons](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/evaluate/reader/compare.rs#L12) · [Row-list hash](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/hash.rs#L107) · [Sort key fallback](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/sort.rs#L241) |
| Generic list layout and extension handling | List and fixed-size list readers expand elements if the packed-float check declines. This includes float lists with child metadata and component unions outside plain Float32, Float64 and Null. A homogeneous result may repack after that expansion. Sets use their own scalar storage. LargeList, list-view layouts, dictionary and sparse union are unsupported by the native logical schema reader when they reach it as those physical types. | Physical layouts may need normalization, including validity, offsets and union tags. Supported layouts can normalize in Arrow, and the schema reader can continue to reject layouts outside its contract. | [List reader](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/values.rs#L705) · [Float union gate](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/types/src/list_value.rs#L87) · [Logical schema reader](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/schema.rs#L242) |
| Scoring scratch and fallback | Prepared cosine normalizes the reference into an engine-owned `Vec<f64>`. The specialized scorer reads candidate components from Arrow. CASE, predicates, two input columns and other unrecognized expression shapes use row evaluation when the optimizer leaves that shape intact. Unsupported or invalid batches can also fall back for normal error handling. The row evaluator reads packed Float32/64 components directly and returns a scalar score. | Cosine needs numeric arithmetic and may benefit from preparing the reference once. The native floating-point scratch buffer is computation, and the row fallback is a kernel-coverage question. | [Specialized scorer](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cosine.rs#L130) · [Cosine implementation](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/vector.rs#L5) |
| Page presence statistics | Parent validity counts cover plain Float32/64 list columns. A dense-union child falls through to the generic parent count, reading each selected list cell even though the cell's float components can stay in Arrow. Numeric bounds traverse child arrays. The compaction spec describes the bitmap rule broadly for floating-point lists, and the current fast path covers only primitive Float32/64 children. | A supported float list’s presence depends on the parent validity bitmap. Counting these parents can use the bitmap for float-only unions as well as primitive float children. | [Compaction spec](https://github.com/bijectionhq/rtdb/blob/fa969b755/dev/doc/compaction.allium#L212) · [Presence count](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/format/src/statistics.rs#L50) · [List statistics and fast-path gate](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/format/src/statistics.rs#L126) |
| Row spill of generic numeric lists | A row without packed vectors uses the JSON cell codec. In a row with packed vectors, those vectors are individual Arrow payloads in one IPC frame and the row structure and scalar fields are a JSON descriptor. Generic numeric list components still pass through the JSON codec, including when a different field in the same row is packed. | Spilled data needs an on-disk representation. XTDB writes sorted Arrow runs. RTDB could write batched Arrow spill pages and retain an index for callers that need random access. | [Spill format selection](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/spill.rs#L58) · [Spill descriptors](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/spill.rs#L108) · [XTDB Arrow spill](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/kotlin/xtdb/operator/ExternalSorter.kt#L62-L74) |

#### Paths that leave the Arrow batch and can retain packed components

| Path | Row path and cost | Required work and avoidable overhead | Source |
| --- | --- | --- | --- |
| Arrow bulk writes | ADBC, Flight SQL and pgwire Arrow COPY fall back to document rows if Arrow document admission declines, for example on nested or unsupported metadata, per-row system columns, incompatible IDs, or union components. Compatible Float32/64 fields then become owned per-row Arrow arrays, even when unrelated fields caused the fallback. ADBC C Data copies admitted foreign buffers into native ownership. | Admission must validate the schema and protect buffer lifetimes. Arrow C Data supports shared buffers with release callbacks. The native ownership copy and whole-batch fallback for unrelated metadata are implementation choices. | [Bulk admission](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/values.rs#L159) · [ADBC import](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/ffi/src/arrow.rs#L191) · [Flight ingest](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/flight-sql/src/upload.rs#L66) · [Arrow COPY](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/pgwire/src/copy.rs#L51) · [Arrow C Data contract](https://arrow.apache.org/docs/format/CDataInterface.html) |
| Native row writes and DML | `PutDocs`, `PatchDocs`, direct document APIs, dynamic SQL writes and temporal PATCH merge operate on document rows when their declared input or resolved output takes the row route. Existing packed components survive field copies. New arrays built by expression evaluation follow the expression rules above. | Public row calls supply row values, and a temporal mutation must resolve its row versions. Inside RTDB, an unchanged embedding can remain a buffer reference, and operations on multiple rows can assemble their outputs in batches. | [Transaction operations](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/values.rs#L407) · [Mutation resolution](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/resolver.rs#L637) · [Live row intake](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/index/src/live.rs#L553) |
| Source coalescing and replay | Document legs keep full batches when their Arrow layouts agree. A row leg or schema mismatch can force materialization. Replay also reads rows for PATCH, missing explicit IIDs, or layouts that fail canonical Arrow admission. Legacy transactions carry Arrow IPC in a different envelope. | Replay must preserve operation order, validity bounds, and identity. Column alignment and typed gathers can handle compatible fields even when operations use different document schemas. | [Source leg assembly](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/source.rs#L312) · [Source replay](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/source.rs#L471) · [Legacy envelope](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/source.rs#L60) |
| Prepared write arguments and source replay | Prepared Flight and ADBC updates can decode Arrow bindings into row arguments. The source SQL leg encodes the arguments as Arrow IPC and decodes them into `SqlArguments` rows on replay. Plain floating-point lists can retain packed components in each such row. | Durable replay requires recording the typed arguments. Arrow input can go into the existing IPC payload after any required SQL coercion, and replay can retain a batch until an expression needs a value. | [Flight update binding](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/flight-sql/src/upload.rs#L22) · [ADBC argument import](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/ffi/src/arrow.rs#L213) · [Source SQL arguments](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/source.rs#L514) |
| Event assembly, replay, flush and compaction | The event writer uses a typed gather when source fields and the merged layout agree. Otherwise it rebuilds affected fields from cells, including row-backed sources, layout changes and unsupported gathers. A packed cell's child buffers can be copied again as Arrow during that rebuild. | Writing a new page may require moving selected values and generating validity, offsets or union tags. Arrow gathers can perform those copies directly. Row construction for eligible fields is an implementation fallback. | [Event field assembly](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/format/src/data.rs#L392) · [Typed list builders](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/values.rs#L400) |
| Query parameter admission | The ADBC query import keeps a whole Arrow binding only for a single row with a fixed-size float list. The engine then retains the whole batch for non-null fixed-size float arguments used in eligible direct cosine projections. Variable-size arguments, parameter casts, tracing, query settings, other uses and unsupported plans can decode to rows. A retained argument can also be materialized if an operator declines. | SQL needs type resolution and any requested cast. Keeping a compatible argument as a typed list can be independent of whether the plan has a specialized cosine kernel. | [ADBC binding gate](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/ffi/src/arrow.rs#L216) · [Arrow parameter entry](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cursor.rs#L199) · [Eligibility](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cosine.rs#L45) · [Fallback](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/parameters.rs#L48) |
| Scan, joins and row operators | Incompatible scan gathers, generic joins, window and aggregate functions, sort and DISTINCT, residual filters, and unhandled projections can read row cells. A stored Float32/64 list read from a page then copies the child buffer into independently owned Arrow storage. Several operators can pass through or share that row payload. | Selecting, repeating and reshaping rows changes the output relation. Row references or Arrow gather can preserve compatible component buffers and restrict copying to results that need independent ownership. | [Scan gather](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/columnar.rs#L185) · [Query mode fallback](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cursor.rs#L269) · [Owned list read](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/values.rs#L705) |
| Batched joins and aggregates | Join gather and take preserve compatible Arrow vector payloads. Join key or layout fallback and generic aggregate inputs can still read cells from the batch. Aggregates that require full rows switch to row processing. Ordered and distinct aggregates also use row sorting or spill where their plans require it. | Matching keys and computing aggregate state are required work. The operator can apply row selections to Arrow payloads and use typed accumulation where the aggregate permits it. | [Batch join gather](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/batch_join.rs#L275) · [Aggregate input](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/aggregate.rs#L166) |
| Bounded top-k and generic sort | Bounded top-k compares supported native keys directly. Unsupported or computed keys can use the generic comparator. If a ranked payload cannot conform to a compatible gather layout, it reads winner cells and rebuilds the output. Unbounded, large or otherwise ineligible top-k plans can use row sorting and its spill codec. | Ordering and selection are required. The chosen rows can gather into Arrow directly where their layouts agree, and comparison of supported keys can use typed readers. | [Top-k eligibility](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/sort.rs#L59) · [Sort key fallback](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/sort.rs#L241) · [Ranked payload gather](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/columnar.rs#L480) |
| Result batches and wire schema conformance | Arrow export can forward a compatible batch or relayout its float list with shared children. Row/page results and incompatible declared layouts rebuild affected columns from cells. Matching component layouts can still feed Arrow builders directly from packed buffers. | The output must agree with the declared schema. XTDB shares agreeing buffers and builds only missing or incompatible parts. Apply the same rule to list children and dense-union layouts. | [Cursor batching](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/cursor.rs#L353) · [Declared-schema conformance](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/conform.rs#L27) · [XTDB ConformingUnloader](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/kotlin/xtdb/arrow/ConformingUnloader.kt#L14-L35) |

#### Protocols, text encodings and index state

| Path | Representation | Required work and avoidable overhead | Source |
| --- | --- | --- | --- |
| pgwire SQL parameters and results | Ordinary parameter and result paths use PostgreSQL text or binary cell codecs. List values may pass through JSON/JSON-LD or Transit serialization depending on the SQL type. Standard PostgreSQL `real[]` and `double precision[]` have no dedicated float-array codec here. Arrow file/stream COPY is a separate path. | PostgreSQL requires Bind cells and DataRow messages, including in binary mode. XTDB's pgwire layer encodes cells from vector readers. RTDB can likewise keep data in Arrow up to the selected cell codec. | [Bind decode](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/pgwire/src/protocol.rs#L187) · [Result encode](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/pgwire/src/protocol.rs#L510) · [Type mapping](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/types/src/pg.rs#L6) · [PostgreSQL message formats](https://www.postgresql.org/docs/current/protocol-message-formats.html) · [XTDB pgwire encoder](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/clojure/xtdb/pgwire.clj#L1161-L1172) |
| Transit COPY and JVM compatibility APIs | Transit JSON and MsgPack COPY parse document values component by component. JVM row SQL binds and transaction writes use Transit, logical-relation requests use EDN plus Transit arguments, and `NativeRows` renders results through a Transit cursor. ADBC Arrow C Data is a separate JVM path. | Transit and EDN are required when a caller selects those APIs and formats. Limit their parsing or rendering to that boundary. An Arrow client can use ADBC, and RTDB supports Arrow COPY for ingestion. | [COPY formats](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/pgwire/src/copy.rs#L34) · [JVM binding](https://github.com/bijectionhq/rtdb/blob/fa969b755/bindings/jvm/src/main/clojure/xtdb/rust.clj#L78) · [FFI row cursor](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/ffi/src/lib.rs#L339) · [XTDB client interfaces](https://docs.xtdb.com/drivers) |
| PostgreSQL source and Kafka Connect | Snapshot rows and change capture decode PostgreSQL text into document values. Kafka Connect JSON and Avro decode elements into values. Document conversion and Avro coercion can expand an already packed list with `into_values()` and then repack it before ingestion. | A connector must decode the upstream wire or event format. It can build Arrow columns directly for supported schemas. Unpacking and repacking a list after the initial decode is extra internal work. | [PostgreSQL source](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/pg-source/src/decoder.rs#L61) · [Kafka JSON](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/kafka/src/connect/converter.rs#L145) · [Kafka document conversion](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/kafka/src/connect/docs.rs#L69) · [Avro coercion](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/kafka/src/connect/avro/union.rs#L80) |
| Explicit text and opaque values | SQL string/JSON rendering, EXPLAIN literals and log inspection convert components to text. The inspection reader builds a recursive `OperationValue` before EDN rendering. An embedding explicitly stored inside a Transit value has opaque MsgPack contents, even in an Arrow binary column. | Requested text, EDN, and Transit must retain their format. The renderer can emit from typed readers. XTDB decodes source transaction operations only when the log query requests that output column. | [String rendering](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/expr/src/string/render.rs#L65) · [EXPLAIN](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/explain.rs#L775) · [Log inspection](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/inspection.rs#L63) · [Transit layout](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/columnar/src/schema.rs#L156) · [XTDB log projection](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/clojure/xtdb/log_tables.clj#L229-L245) |
| Flight SQL query tickets | Prepared-query arguments are Arrow IPC, then base64 text inside a JSON ticket. Fetch decodes base64 and reads the IPC stream back into Arrow. This costs a byte-level round trip over the whole parameter payload. | Flight tickets are opaque bytes. XTDB deliberately embeds the query, basis and base64 IPC arguments to avoid retaining query state between requests. A compatible, versioned binary ticket could retain that property and remove base64 and JSON. | [Ticket encoding](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/flight-sql/src/ticket.rs#L28) · [Ticket execution](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/flight-sql/src/execute.rs#L117) · [Arrow Flight ticket format](https://arrow.apache.org/docs/format/Flight.html#retrieving-data-via-doget) · [XTDB ticket codec](https://github.com/xtdb/xtdb/blob/dac3c94c19a0918d5efabbdffd7ddb5a0bbc67ff/core/src/main/kotlin/xtdb/flight_sql/QueryTicket.kt#L27-L53) · [XTDB stateless ticket change](https://github.com/xtdb/xtdb/commit/c5fec4d71) |
| Distributed scans | The fragment request serializes the scan and its portable predicates as JSON. A vector literal carried by a predicate serializes as expression items or list values. The response carries length-prefixed Arrow IPC batches as base64 in JSON. A generic result column can also round-trip between row cells and IPC at each end. | A cross-node request and response need framing and a stable versioned format. RTDB controls this protocol. It can carry IPC bytes directly and transport eligible predicate vectors as typed data, with compatibility during upgrades. | [Portable predicates](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/fragment.rs#L89) · [List Serde](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/types/src/list_value.rs#L217) · [Fragment codec](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/query/src/fragment.rs#L161) · [Control transport](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/multi/src/control.rs#L140) |
| Primary log envelopes and metadata | Transaction operations, user metadata and replica table data use Arrow IPC payloads in protobuf envelopes. Envelope encoding copies bytes. The control fields, IIDs, temporal bounds and transaction results are still represented as scalar metadata where they are consumed. | Durable logs need a stable serialized format. Preserve the existing Arrow IPC and protobuf contracts and reduce copies inside the encoder. XTDB assembles sized Arrow byte pieces and writes one final protobuf message. | [Source encoding](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/source.rs#L105) · [Replica table data](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/tx/src/resolver.rs#L87) · [Envelope codec](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/format/src/envelope.rs#L195) · [XTDB Arrow log encoding change](https://github.com/xtdb/xtdb/commit/f8468f4d0) |
| FTS and ordered-index sidecars | FTS postings, text tokenization, scalar ordered keys and IID candidate sets use their own representations. FTS builds and live verification read text by field. Ordered index key types exclude embedding lists. In both cases vector search can use the filtered IDs to read the embedding from primary Arrow data. | Posting lists, ordered keys and IIDs serve the index algorithms. The index formats can remain specialized while the embedding payload stays in primary Arrow storage. | [FTS field read](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/search/src/lane.rs#L176) · [Ordered key types](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/node/src/ordered_index/key.rs#L29) |
| Experimental ANN fixture and scoring | Fixture training uses HDF5 and NumPy before writing Arrow. Cluster object ranges store Arrow IPC with typed embeddings and IIDs. Range tables, centroid scores, the shortlist heap and churn masks use native Rust collections. The benchmark copies a query slice into an Arrow array for exact reranking. | Training matrices and search queues are computational state. The stored ANN vectors already use Arrow IPC. The per-query buffer copy in the benchmark is independent of the ANN index format. | [Fixture builder](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/node/examples/support/vector_benchmark_data.py#L16) · [ANN range reader and shortlist](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/node/examples/support/vector_index.rs#L169) · [Arrow query bind](https://github.com/bijectionhq/rtdb/blob/fa969b755/rust/crates/node/examples/support/vector_workload.rs#L101) |

The audit cross-checks all 27 entries in the historical inventory below and adds paths discovered by following list consumers and wire envelopes.

### Arrow implementation after merging RTDB main

The numbered rows correspond to the 28 entries above.
Native execution can retain an Arrow array, retain the array and an index selection, or keep a typed child array inside a scalar list value.
The latter preserves the numerical payload when an operator needs to own an individual row.
Durable logs and external protocols still encode their prescribed formats.

| # | Audit path | Implementation and boundary |
| --- | --- | --- |
| 1 | Row-created numeric lists | `ListValue` and `ListBuilder` retain signed and unsigned integers, Float16/32/64, decimal coefficients and validity in Arrow. Distinct component types use a dense union. Opened unsigned buffers can be reinterpreted at their signed width. |
| 2 | SQL and logical-plan literals | SQL expression materialization and EDN literal input append directly to the numeric builder. SQL and EDN still parse their own text and evaluate each expression that contributes a cell. |
| 3 | List-changing SQL | Casts and array aggregation append computed cells to the typed builder. Compatible trims slice the packed payload. An aggregate collecting whole vectors retains the inner Arrow payload even when its outer list is generic. |
| 4 | UNNEST and list readers | Stored lists use the scan's Arrow child selections and parent indices. List-expression pages carry typed numeric vectors, indexed integer series generate signed Arrow vectors, and mixed nonnumeric expressions retain their value semantics. |
| 5 | List indexing, comparison and hashing | Float comparison and ordering read typed components directly. Packed numeric hashing uses the same primitive codecs as Arrow cells. Indexing returns one value, and generic mixed-type comparison still evaluates the cells needed by SQL ordering. |
| 6 | Generic layout and extensions | Supported numeric child arrays preserve validity, dense union tags and logical decimal metadata. Compatible layout changes use Arrow kernels. Decorated and nonnumeric child types still follow their own logical readers. The native schema reader retains its accepted layout contract. |
| 7 | Scoring | Direct cosine reads selected Arrow cells. A simple Boolean CASE evaluates its guard and scores only selected rows from Arrow. Other expressions can read Arrow or packed views through the evaluator. Reference normalization and score accumulation use native computation state. |
| 8 | Page presence statistics | Ordinary list presence uses parent validity. Typed page readers compute child numeric statistics, including Float16. |
| 9 | Generic row spill | Row spill uses Arrow IPC, including per-record schemas for mixed fallback rows. Packed numeric children retain their widths and nulls when the result needs a new layout. |
| 10 | Arrow bulk writes | Arrow COPY and uploaded Arrow document batches submit typed columns through the node and transaction writer. Admission checks the schema, identifiers and validity. |
| 11 | Native row writes and DML | Columnar mutation output can pass document cells to the transaction writer. When a caller supplies rows or a mutation computes a row, the engine packs numerical lists before writing them. |
| 12 | Source coalescing and replay | The source writer coalesces compatible Arrow data and chooses Arrow fields for evolving layouts. The replay path reads eligible record batches under their original source envelope. |
| 13 | Prepared writes and replay | Native SQL arguments retain Arrow batches and preserve argument order through source IPC and replay. SQL parameters and documents retain separate schema and type rules. |
| 14 | Event assembly, replay and compaction | Event writers and compaction gather selected Arrow cells, align fields and union legs, and add required temporal, IID and validity metadata. |
| 15 | Query parameter admission | Native bindings retain typed arguments. The retained C Data ABI transfers immutable buffers with an independent owner. The default C Data ABI guarantees a snapshot from mutable input and the JVM adapter exports directly when all input columns share an allocator root. |
| 16 | Scans, joins and row operators | Scan selections retain Arrow sources and positions. When evaluation needs an individual numeric list, it reads a typed range and keeps the resulting payload packed. |
| 17 | Batched joins and aggregates | Compatible join keys and payloads use Arrow positions. Grouping and aggregate arithmetic keep their native algorithm state, and computed numeric list results use typed buffers. |
| 18 | Bounded top-k and sort | Sort keeps keys and row positions and gathers selected payloads through Arrow. Spill runs use Arrow IPC. |
| 19 | Result conformance | Arrow result clients receive batches aligned to their declared schema. Compatible arrays retain their original buffers, with Arrow relayout for changed physical fields and typed null padding. |
| 20 | pgwire | Bind parses the PostgreSQL format and DataRow emits PostgreSQL field framing. Supported outputs encode from Arrow cells. Arrow COPY remains available for clients sending Arrow data. |
| 21 | Transit COPY and JVM compatibility | Transit and EDN inputs parse their designated formats, and row results render those formats on request. Numeric arrays can enter typed storage during decoding. Arrow clients can remain columnar through C Data or Arrow IPC. |
| 22 | PostgreSQL source and Kafka Connect | PostgreSQL snapshot and WAL decoding produce typed batches. Kafka decodes JSON or Avro and keeps compatible numeric buffers through document conversion and coercion. Conversions that change schema still apply the connector's coercion rules. |
| 23 | Requested text and opaque values | Inspection can keep numeric arrays packed through operation decoding and render them directly. JSON, EDN, EXPLAIN and opaque Transit values keep their requested representations. |
| 24 | Flight tickets | Run tickets carry Arrow IPC bytes in a versioned binary frame. New readers also redeem legacy JSON tickets. In a mixed deployment, route a binary run ticket to a reader that supports its version. |
| 25 | Distributed scans | Replies carry bounded binary Arrow IPC. Eligible numeric predicate constants travel in a typed frame and return to the same predicate positions. A coordinator can still consume a legacy reply and evaluates its original predicates. |
| 26 | Durable log envelopes | The encoder transfers uniquely held IPC buffers into protobuf fields with their reservations. Shared inputs may require a copy. The final protobuf frame retains the existing byte format. |
| 27 | FTS and ordered sidecars | Postings, ordered keys and candidate IIDs serve their index algorithms. The embedding payload stays in primary Arrow data. |
| 28 | Experimental ANN | Exact reranking binds a slice of the query fixture's Arrow buffer. HDF5 and NumPy remain fixture inputs. Centroids, heaps, range metadata and churn masks are algorithm state. |

#### Qualification of the 28-entry audit

The first full run on implementation commit `e69f116926` passed 5,886 Rust workspace tests and seven doctests, with 15 Rust tests ignored.
The Gradle unit, property, and integration suites passed 3,243, 3,348, and 5,629 tests respectively, with one PostgreSQL source skip.
This run covered Arrow COPY followed by flush and vector ranking, Flight ticket compatibility, native C Data buffer custody, delegated Arrow predicate frames, and temporal ranking.

The final full rerun at `6ab9092e87` included the SQL CASE guard fix and passed 5,901 Rust workspace tests and seven doctests, with 15 Rust tests ignored.
Gradle passed 3,243 unit, 3,348 property, and 5,629 integration tests, with the same PostgreSQL source skip and no reflection or boxed-math warnings.
The guard regression verifies that a false or null guard leaves zero-norm vectors unread and that a selected zero-norm vector returns the expected error.
On the published commit `f908864281`, the Rust all-targets workspace check passed, along with 33 Flight SQL integration tests and 102 distributed cell tests.

The paired release measurement used the same 100,000 100-dimensional GloVe vectors and 12 query vectors, with in-memory primary storage on an Apple M4 Max.
The baseline was the pre-audit release binary at `fa969b755`.
The candidate included the CASE guard fix on the `5191a9f0ed` tree, which also incorporates later RTDB main work.
The first measurement of this integrated code spent about 2,002 ms p50 evaluating the unfiltered CASE route because SQL wraps a searched CASE guard in `IS TRUE`.
The fixed projection handles that guard over Arrow Boolean arguments and columns and skips disabled rows before reading their embeddings.

| Stationary filter | CASE exact p50, ms, before → after | Direct exact p50, ms, before → after | Ordered exact p50, ms, before → after |
| --- | ---: | ---: | ---: |
| None | 534.27 → 81.83 | 218.92 → 81.76 | 216.52 → 81.95 |
| Random, ~0.1% | 8.31 → 4.16 | 7.34 → 4.14 | 5.60 → 5.50 |
| Random, ~1% | 17.35 → 7.88 | 11.06 → 7.87 | 9.50 → 12.97 |
| Random, ~10% | 63.34 → 14.53 | 12.69 → 14.57 | 20.42 → 28.12 |
| Whole clusters, ~1% | 18.46 → 7.97 | 11.20 → 7.95 | 9.93 → 13.72 |

Unfiltered warm ANN at 32 probes improved from 4.94 ms to 3.20 ms p50, and unfiltered exact search under churn went from 214.81 ms to 82.67 ms p50.
Loading and flushing the fixture took 662 ms before and 967 ms after, and ordered-index build time went from 14.30 s to 8.05 s.
Both builds passed five temporal canaries and checked result ID order and distance bits between their exact routes for all 60 stationary and 20 churn cases.
All compared ANN samples had identical recall, returned counts, and expected result counts.

### Remaining scalar paths

**Historical audit before Arrow-backed row operators.**

This inventory covers embedding-bearing paths in the native Rust engine and its client interfaces at revision `3d1bb0a93`.
A scalar conversion here means that a vector or key becomes a generic Rust `Value`, or that an incoming protocol creates one directly.
The Arrow scorer reads typed buffers and prepares a query vector as a native `Vec<f64>`.

#### Writes, replay, and maintenance

| Path | Trigger and cost |
| --- | --- |
| **Page statistics on L0 flush and compaction** | **Runs even for compatible fixed-size Float32/64 embeddings.** The numeric bounds walk the typed child buffers. The presence count accepts only flat types, so it reads every non-null list as a `Value::FixedSizeList` with all its components just to count that row. This scales with the number of components written on each pass. [Statistics count](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/format/src/statistics.rs#L50) · [Fast-path gate](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/format/src/statistics.rs#L189) · [L0 writer](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/replication/src/blocks.rs#L375) · [Compaction writer](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/maintenance/src/merge/layout.rs#L50) |
| Native row APIs and row-based sources | `PutDocs`, `PatchDocs`, and the row transaction buffer take documents as values. Legacy JVM Transit operations and PostgreSQL-source bootstrap and change capture construct row documents or stored events from their respective wire values. Embeddings supplied through those entry points enter as row values. [Operations](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/values.rs#L381) · [Transit transaction](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/lib.rs#L438) · [PostgreSQL source decoder](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/pg-source/src/decoder.rs#L91) · [Mirror indexer](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/pg-source/src/indexer.rs#L11) |
| ADBC and Flight SQL bulk admission | The check is batch-wide. A nonempty batch needs unique field names, a non-null `_id` of signed integer or UTF-8 type, and at least one non-null fixed-size Float32/64 vector. Per-row system fields, annotated or unsupported fields, variable-length vectors, and null components in non-null vectors cause the general import path to decode rows, subject to regular input validation. An unsupported metadata field alone can send otherwise compatible embeddings through that path. Null embedding rows and nullable supported scalar columns can stay in Arrow. [Admission](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/values.rs#L167) · [ADBC import](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/arrow.rs#L191) · [Flight ingest](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/flight-sql/src/upload.rs#L60) |
| PostgreSQL COPY | Arrow file and stream COPY decode the IPC batch, read every cell as an execution value, and submit row `PutDocs`. Transit COPY also submits row documents. [COPY reader and submit](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/pgwire/src/copy.rs#L33) |
| Static SQL writes and DML parameters | The submission path expands eligible SQL mutations into row documents, including literal inserts. ADBC and Flight prepared updates and other control or DML execution decode bindings into `SqlArguments`. When arguments are logged, the source codec builds Arrow IPC from the values and reads them back into values. [SQL expansion](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/submission.rs#L44) · [ADBC update](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/statement.rs#L204) · [Flight update and control](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/flight-sql/src/upload.rs#L23) · [Flight execution](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/flight-sql/src/execute.rs#L73) · [Source argument codec](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/source.rs#L514) |
| Dynamic SQL writes and PATCH | The resolver retains columns for a put output with a non-null `doc` struct that passes Arrow document admission. Otherwise it reads `doc` row by row and appends row events. SQL struct construction and row copy can already have decoded an embedding upstream when they build that `doc`. Document PATCH goes through row input and temporal patch evaluation, which can read existing embedding values to assemble replacement rows. [Mutation output](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/resolver.rs#L637) · [PATCH](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/resolver.rs#L482) · [Struct projection](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1374) · [Row copy](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1464) |
| Source transaction coalescing | The document leg can concatenate Arrow batches when all inputs for that table and operation are Arrow and their full schemas agree. A row batch or a schema difference in that leg sends every typed document batch in the leg through `materialize`, then the row writer builds IPC. [Source document leg](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/source.rs#L312) |
| Source replay | Typed replay requires a put leg with explicit IIDs, non-null document structs, and a canonical supported layout. Other supported document legs use `Documents::read_arrow`, including PATCH, puts without explicit IIDs, and noncanonical layouts. The typed route still reads small control values such as IIDs and valid-time bounds. [Operation reader](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/source.rs#L468) · [IID derivation](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/values.rs#L267) |
| Event assembly, flush copies, and compaction rewrites | When the event copier cannot gather a field, it reads or clones its cells and rebuilds that entire output field. Triggers include a build that does not request the copy path, a row-backed event among the sources, changed physical layout or field annotations, missing values for a non-nullable field, and unsupported layouts such as a union containing embeddings. This decision is per column, including when a mixed-layout flush takes the page-reference path. [Event copier](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/format/src/data.rs#L377) · [L0 page copy](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/index/src/live.rs#L318) · [Compaction leaf](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/maintenance/src/merge/layout.rs#L50) |

#### Query parameters, scanning, and scoring

| Path | Trigger and cost |
| --- | --- |
| Input binding | ADBC retains a query binding as Arrow only for a single-row batch with a fixed-size Float32/64 list; other shapes use scalar argument rows. Native `Vec<Value>`, PostgreSQL bind, and JVM Transit bind accept scalar parameters. A SQL array literal also starts as scalar components, which the direct cosine projection can prepare once. [ADBC import](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/arrow.rs#L191) · [Native parameter API](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/node/src/statement.rs#L170) · [PostgreSQL bind](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/pgwire/src/protocol.rs#L187) · [Transit bind](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/lib.rs#L257) · [Cosine parameter preparation](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cosine.rs#L90) |
| Packed Arrow parameter admission | The retained path requires an ordinary row-producing query, no tracer or settings parameters, and a non-null compatible list without an explicit parameter cast. The vector parameter must be used only in recognized direct cosine projections. A use in another expression, or a plan outside the scan, projection, filter, and sort allow-list, decodes it when opening the query. The allow-list excludes scans with MATCH or pushed top-k. If column execution or a cosine batch declines after admission, the executor can materialize the retained parameters for the fallback. [Query admission](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cursor.rs#L199) · [Plan and use checks](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cosine.rs#L45) · [Parameter materialization](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/parameters.rs#L48) · [Batch fallback](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1148) |
| Columnar plan selection and row consumers | Joins, aggregation, windows, unnests, correlated subqueries, and set operations can introduce row operators. Consecutive filters, a filter over projected batches, unsupported projection expressions, system or inspection scans, and traced or analyzed execution can also take the row/page path. Plain scans opened in default row mode without recognized cosine use the row interface. Some subtrees still run kernels in batches, then `BatchRows` reads their columns as values for the next row operator. [Cursor selection](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cursor.rs#L269) · [Batch shapes and page adapter](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/operators.rs#L114) · [BatchRows](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L438) |
| Batched inner join | Build-side gather and probe-side take only preserve flat Arrow columns. A fixed-size float list on either side is materialized for each matching output row even when the join keys are eligible for batch processing. The build-row threshold can also hand the inputs to a row join. [Join threshold](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/batch_join.rs#L74) · [Join gather and take](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/batch_join.rs#L275) |
| Aggregate over batched input | The generic aggregate stores the selected input columns as per-cell values even when it receives batches. When a grouping key or aggregate argument is a vector, that read materializes its components. Aggregates that consume whole rows use the page path. [Aggregate batch input](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/aggregate.rs#L35) |
| Scan gather | The columnar scan reads vector cells as values if it cannot gather them under a compatible fixed-size float layout. Examples include variable-length or non-float lists, incompatible dimensions or component types across page layouts, and physical or logical layouts that cannot be reconciled. Null rows and columns absent on some pages have an Arrow path when the remaining layouts agree. If every source lacks the field, the scan supplies scalar null placeholders. [Stored column gather](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L185) · [Layout conformance](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L378) |
| Cosine expression shape | The specialized projection expects a direct `COSINE_DISTANCE` call with one input column and a parameter, literal, or list of literals. Two input columns, a dynamically constructed vector, or cosine under CASE, a cast, a predicate, or another expression can invoke the scalar evaluator if the optimizer leaves that shape in the execution plan. The paired benchmark deliberately uses the CASE route for its scalar control. [Expression matcher](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cosine.rs#L11) · [Scalar projection fallback](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1179) |
| Cosine query or batch data | Query preparation declines empty, zero-norm, non-finite, or null-component vectors and can decline when its preparation reservation fails. A scan batch with scalar input, incompatible width, zero-norm or non-finite data, or an unmasked null component can send the entire expression for that batch to the scalar evaluator, which preserves normal error semantics. A null embedding row yields a null score on the fast path for a valid query vector. [Query preparation and scorer](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cosine.rs#L104) · [Value evaluator and normalization](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/expr/src/vector.rs#L7) |
| Other filters and projections | If the built-in and DataFusion kernels decline, batch operators read the columns referenced by that expression and evaluate row by row. Struct construction, row copy with a vector field, and vector-valued COALESCE can also read vectors as values inside a batch projection. A scalar filter of metadata alone leaves embedding buffers available for later stages. [Batch filter](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L517) · [Expression fallback](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1179) · [Projection kernels](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L1367) |
| Distributed scan fragment | Typed columns travel as Arrow IPC. A column already represented as values on the worker is built into an IPC column marked as values, then decoded back into per-cell values by the coordinator. This adds a round trip for a vector column that fell back during gathering. [Fragment codec](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/fragment.rs#L158) |

#### Ranking and results

| Path | Trigger and cost |
| --- | --- |
| Bounded columnar top-k sort keys | Even the compatible cosine path reads each sort key as a `Value` and uses SQL's general comparator. For distance plus ID ordering, these are small scalar values. If another key references the embedding itself, key extraction reads the vector. [Key extraction](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/sort.rs#L106) |
| Bounded columnar top-k payload gather | The winner gather uses Arrow for compatible embedding arrays. If either source fails layout conformance or the resulting layout is outside the supported gather types, it reads that column's winning cells as values. [Winner gather](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/columnar.rs#L458) |
| Generic sort, row top-k, and spill | Unbounded sort, `OFFSET + LIMIT` above 10,000, unsupported keys or limit shapes, and plans that fail the columnar checks can use the row sorter. Its batch-assisted variant can score in Arrow and still materialize retained rows. Generic sort, and row-based joins that use the row spill codec, serialize spilled values as length-prefixed JSON. An embedding included in the spilled schema is encoded component by component. [Sort selection](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/operators.rs#L277) · [Batch-assisted row sort](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/sort.rs#L278) · [Sort records](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/sort/records.rs#L13) · [Join storage](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/join_storage.rs#L17) · [Spill codec](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/spill.rs#L45) |
| Output reconstruction and schema conformance | Row/page output uses scalar cells to rebuild every result column. Column mode keeps arrays it can conform and rebuilds individual incompatible columns from values. ADBC and Flight perform another declared-schema check with its own per-field fallback. Compatible fixed-size float relayout can share the original component buffer. [Cursor output](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/cursor.rs#L353) · [Client schema conformance](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/columnar/src/conform.rs#L27) |
| Row-protocol result export | PostgreSQL result streaming reads returned cells as values for its protocol encoder. Legacy JVM `NativeRows` requests the native Transit cursor, which reads Arrow cells and encodes them into Transit. Returning an embedding through either path materializes it. [PostgreSQL egress](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/pgwire/src/protocol.rs#L518) · [JVM cursor](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/bindings/jvm/src/main/java/xtdb/rust/NativeRows.java#L19) · [Transit egress](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/ffi/src/lib.rs#L339) |

#### Full-text search and inspection

| Path | Trigger and cost |
| --- | --- |
| Full-text artefact build and live postings | Artefact build and rebuild read complete put documents before extracting indexed text. Live posting maintenance also reads complete events, so a vector present in the same document can be decoded even if the index only covers text. [FTS artefact builder](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/search/src/lane.rs#L206) · [Live postings](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/search/src/runtime.rs#L54) |
| Full-text verification, ranking, and corpus | Live phrase verification, the ranked live path, and live-origin corpus reads can read complete events. The scan verifier also reads its projected scan columns through a row page when checking visible values or residuals. Uncached MATCH also reads the searched field as a value. The full-event and verifier paths can decode vectors on mixed text and embedding tables. [Live verification](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/search/src/accelerate.rs#L348) · [Live ranking](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/search/src/accelerate.rs#L502) · [Corpus](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/search/src/corpus.rs#L162) · [Scan verifier](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/scan.rs#L451) · [MATCH field read](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/query/src/scan.rs#L919) |
| Log inspection | Operation inspection recursively expands document values to inspection structs and lists, including fixed-size vector components. [Operation inspection](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/tx/src/inspection.rs#L23) |

Arrow file and stream IPC, ADBC buffer ownership copies, Flight tickets carrying Arrow IPC, and compatible Arrow gathers copy or encode buffers directly.
The experimental ANN range reader scores typed buffers, and a coverage failure routes to exact SQL, where the query and batch rules above determine whether scoring uses Arrow. [ANN routing](https://github.com/bijectionhq/rtdb/blob/3d1bb0a93/rust/crates/node/examples/support/vector_workload.rs#L161)

### Arrow benchmark at 100,000 embeddings

This run uses `d59df7dc6` with Arrow IPC fixture input, the columnar write path, typed centroid ranges, and typed IID reranking.
The fixture's vector, centroid, query, and cluster assignment buffers match the original test data bit for bit.
The 60 stationary and 20 churn cases matched the scalar control in ID order and distance bits under exact SQL.
Every ANN sample matched its original recall and result count.
In the full-probe churn canary, shadowing 67 changed IIDs restored ten neighbors from the two returned by the stale shortlist.

Loading and flushing the table took 0.86 s.
Completing both ordered indexes took 13.95 s.
Packing the centroid ranges took 67.43 ms after training.
The Arrow centroid object used 42,399,112 bytes.
Adding the IID column and IPC framing increased the original object by 1,999,112 bytes and removed the separate 2,400,000-byte resident IID dictionary.

The latency cells show p50 / p95 milliseconds.
The ANN columns show 32 probes.

**Stationary**

| Filter | Exact scalar control | Exact Arrow | Ordered exact Arrow | Warm ANN | Cold ANN | Cold reads / MiB, p50 | Mean recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 4,778.24 / 5,005.07 | 579.66 / 607.28 | 585.06 / 647.47 | 4.98 / 7.74 | 5.35 / 7.70 | 32 / 2.84 | 79.2% |
| Random, ~0.1% | 13.11 / 13.79 | 7.06 / 8.82 | 5.42 / 5.81 | 0.75 / 1.55 | 1.12 / 2.98 | 32 / 2.84 | 26.7% |
| Random, ~1% | 61.79 / 75.02 | 10.76 / 11.59 | 9.13 / 13.00 | 4.15 / 4.61 | 4.72 / 10.72 | 32 / 2.84 | 42.5% |
| Random, ~10% | 483.06 / 539.43 | 13.10 / 13.90 | 20.88 / 23.04 | 4.21 / 4.77 | 4.64 / 7.27 | 32 / 2.84 | 63.3% |
| Whole clusters, ~1% | 70.37 / 72.77 | 10.99 / 11.30 | 9.49 / 9.92 | 4.08 / 4.68 | 4.23 / 4.85 | 6 / 0.50 | 100.0% |

**Churn**

| Filter | Exact scalar control | Exact Arrow | Ordered exact Arrow | Warm ANN | Cold ANN | Cold reads / MiB, p50 | Mean recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 4,727.44 / 4,813.94 | 581.56 / 584.20 | 578.75 / 591.25 | 6.64 / 7.31 | 6.92 / 8.09 | 32 / 3.11 | 82.5% |
| Random, ~0.1% | 13.21 / 13.43 | 6.93 / 7.21 | 15.26 / 15.40 | 3.40 / 4.15 | 3.97 / 4.50 | 32 / 3.11 | 30.0% |
| Random, ~1% | 61.44 / 62.70 | 10.69 / 10.87 | 19.07 / 19.26 | 5.72 / 6.24 | 6.11 / 6.47 | 32 / 3.11 | 60.0% |
| Random, ~10% | 480.13 / 488.64 | 12.99 / 13.46 | 29.84 / 30.59 | 5.75 / 5.87 | 6.05 / 6.12 | 32 / 3.11 | 72.5% |
| Whole clusters, ~1% | 70.86 / 71.10 | 10.97 / 11.21 | 19.03 / 19.37 | 5.80 / 5.82 | 5.93 / 5.97 | 6 / 0.50 | 100.0% |

### Arrow benchmark at 1,000,000 embeddings

The file-backed workload uses the same Arrow code, filter definitions, and GloVe dataset as the paired 100,000-vector run.
All 60 stationary and 20 churn cases matched the scalar control in ID order and distance bits under exact SQL.
Each ANN sample kept the original recall and result count.
Shadowing 67 changed IIDs in the full-probe churn canary restored 10 exact neighbors from the 2 returned by the stale shortlist.

Loading and flushing the table took 14.02 s.
Completing both ordered indexes took 35 min 32 s.
Packing the Arrow centroid ranges took 0.87 s after training.
The centroid object used 420,401,416 bytes, with the IIDs carried by the Arrow batches in place of the original 24,000,000-byte dictionary.
The unfiltered exact query took 5,861.39 ms p50 with the batch scorer and 46,823.71 ms with the scalar control on the same captured snapshot, a 8.0× speedup.
Unfiltered ANN at 32 probes took 139.74 ms warm and 139.31 ms cold p50, with 91.7% mean recall@10.

The latency cells show p50 / p95 milliseconds.
The ANN columns show 32 probes.

**Stationary**

| Filter | Exact scalar control | Exact Arrow | Ordered exact Arrow | Warm ANN | Cold ANN | Cold reads / MiB, p50 | Mean recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 46,823.71 / 46,863.23 | 5,861.39 / 5,966.52 | 5,855.94 / 5,871.22 | 139.74 / 149.54 | 139.31 / 150.76 | 32 / 27.09 | 91.7% |
| Random, ~0.1% | 284.30 / 285.34 | 231.11 / 232.31 | 203.69 / 215.22 | 136.59 / 146.15 | 137.10 / 148.02 | 32 / 27.09 | 46.7% |
| Random, ~1% | 721.37 / 727.51 | 244.00 / 244.86 | 242.38 / 277.95 | 138.28 / 157.02 | 139.01 / 159.29 | 32 / 27.09 | 63.3% |
| Random, ~10% | 4,896.23 / 4,921.97 | 264.80 / 266.51 | 300.02 / 320.41 | 140.45 / 147.76 | 141.04 / 147.81 | 32 / 27.09 | 76.7% |
| Whole clusters, ~1% | 880.44 / 884.02 | 244.56 / 245.70 | 252.08 / 364.58 | 138.73 / 151.24 | 138.66 / 149.30 | 6 / 5.38 | 100.0% |

**Churn**

| Filter | Exact scalar control | Exact Arrow | Ordered exact Arrow | Warm ANN | Cold ANN | Cold reads / MiB, p50 | Mean recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 46,578.31 / 46,620.07 | 5,863.74 / 5,872.71 | 5,865.84 / 5,869.12 | 180.80 / 189.24 | 181.35 / 188.79 | 32 / 30.59 | 82.5% |
| Random, ~0.1% | 284.76 / 302.04 | 231.27 / 231.97 | 240.26 / 290.81 | 172.04 / 182.76 | 172.92 / 183.11 | 32 / 30.59 | 50.0% |
| Random, ~1% | 722.09 / 726.25 | 245.57 / 245.93 | 276.31 / 277.61 | 177.24 / 185.78 | 178.15 / 186.72 | 32 / 30.59 | 75.0% |
| Random, ~10% | 4,885.60 / 4,909.79 | 265.97 / 266.70 | 330.96 / 334.55 | 179.02 / 182.86 | 180.09 / 183.93 | 32 / 30.59 | 80.0% |
| Whole clusters, ~1% | 882.51 / 882.93 | 244.77 / 245.63 | 288.01 / 387.64 | 183.96 / 187.99 | 183.38 / 188.80 | 6 / 5.38 | 100.0% |

### Follow-up verification of Arrow binding and ranked output

Commit `b211a0ee7` reran the 100,000-vector fixture through the Arrow parameter entry point and bounded columnar top-k.
All 60 stationary and 20 churn query-and-filter cases matched the scalar comparator in result order and distance bits, and all five temporal checks passed.
The ANN recall, returned counts, and cold range counts matched the preceding 100,000-vector run for every sample.
Unfiltered exact p50 was 581.12 ms with the Arrow path and 4,691.70 ms with the scalar control on the same snapshot.
Separate ADBC and Flight SQL tests ingest fixed-size floats, bind the reference vector through Arrow, and return the ranked embedding column.
The ADBC test covers null embeddings, nullable metadata, stable ties, and an offset after flush.
A cursor regression asserts that a ranked query with an Arrow argument opens on the columnar execution path.
A result-conformance test verifies that schema changes share the original float component buffer.
