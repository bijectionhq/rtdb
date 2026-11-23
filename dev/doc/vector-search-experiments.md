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

The workload uses a dense fixed-size Arrow float list per embedding, scalar predicate columns, and a stable IID per entity.
Arrow IPC is used where the data crosses a durable or process boundary.
In-memory code keeps typed buffers alive across those boundaries and creates new buffers when rows are gathered, reordered, or copied from the foreign allocator.

| Stage | Former conversion | Arrow path and remaining boundaries |
| --- | --- | --- |
| Benchmark preparation | NumPy matrices were written as separate raw float and cluster files, which Rust parsed one scalar at a time. | PyArrow writes fixed-size `float32` lists and `uint32` cluster ordinals in IPC streams. Rust borrows the decoded child buffers. |
| Native Rust, ADBC, and Flight SQL bulk ingest | The client batch was decoded into argument values, copied into per-row document maps, and converted back to Arrow by the source writer. | Compatible dense batches preserve typed columns. ADBC copies foreign buffers into native Arrow memory so its caller can release the original allocation. Flight retains the decoded batch. |
| SQL text and prepared arguments | SQL literals and generic parameter bindings use scalar values. General document construction can also materialize a projected embedding. | A compatible Arrow document produced by a dynamic SQL mutation can enter transaction staging directly. SQL literals, generic bindings, PostgreSQL COPY, PATCH document merging, and document shapes with unsupported types or nulls retain their existing paths. |
| Source log and replay | Source IPC replay unpacked each vector into component values, transaction staging cloned the row, and the event builder rebuilt the array. | Compatible writes use the existing `put-docs` Arrow union leg. Replay retains its document arrays and stages them alongside resolved IIDs and temporal intervals. The source protobuf remains the log envelope. |
| Replica log, flush, and compaction | Persisted data was Arrow, but copying fixed-size list cells to new pages rebuilt them through scalar values when the direct flat-column path could not run. | The event copier gathers compatible fixed-size float buffers with list and component validity. It keeps the existing layout and falls back for schema coercion. The replica log and same-layout flush already copy Arrow batches. A flush spanning different layouts now keeps the page references and copies each compatible column directly. |
| Structured predicate indexes | The ordered index reads its key columns from Arrow pages during build and uses its own posting-tree codec for scalar keys and IIDs. | The vector stays in the primary data page. Ordered-index candidate IIDs feed the same exact scorer as an ordinary scan. |
| Exact scan and distance | Scan gather boxed each component, then scalar distance decoded each embedding and prepared the same query repeatedly. | The scan gathers compatible fixed-size float lists directly. A column-to-literal or column-to-parameter cosine expression prepares the query once, reads Float32 or Float64 child buffers, and emits an Arrow Float64 column. Unsupported layouts and general expressions continue through the scalar evaluator. |
| Centroid ranges and live tail | Cluster records decoded each float manually. Tail reconciliation depended on an IID-to-ID dictionary and read whole documents to identify new rows. | Each range is an Arrow IPC stream. The reader borrows the buffer pool's range and reads typed vector and IID arrays. Tail reconciliation uses event-header IIDs, including for new rows. |
| ANN reranking | Candidate IDs were formatted into a SQL disjunction and parsed again. | The prepared plan receives the typed IID scope directly. Primary-table visibility and residual predicates still apply before the Arrow scorer and top-k. |
| Top-k and sort spill | The generic sorter compares scalar key cells and retains result rows as values. | The vector benchmark sorts only distance and IID-resolved row IDs. Projections that return the original embedding can still materialize it for contenders retained by the generic sorter. |
| Result export | The benchmark converted result columns back to generic values. PostgreSQL clients use PostgreSQL protocol values. | The benchmark reads typed ID and distance buffers. ADBC and Flight SQL export Arrow results, subject to the existing output schema and sort paths. PostgreSQL conversion occurs at the wire boundary. |

The relevant implementation boundaries are `ffi/src/{arrow,statement}.rs`, `flight-sql/src/upload.rs`, `tx/src/{source,values,resolver}.rs`, `index/src/live.rs`, `format/src/data.rs`, `query/src/{columnar,cosine}.rs`, and the native vector benchmark.
A compatible fast path must preserve missing and null values, invalid-component errors, distance and tie behavior, and the table snapshot's visibility.

When full-text indexes share a table with embeddings, the FTS builder and live text-corpus fallback can still read a complete mixed document before extracting text in `search/src/{lane,accelerate,corpus,runtime}.rs`.
That cost belongs to the full-text indexing and text-scoring paths.
The specialized vector benchmark uses ordered predicate indexes and resolves its live tail from headers.

The plain score and the scalar control in the updated benchmark run against the same captured source with scan acceleration disabled.
The scalar control puts the same distance call under a bound true condition so the specialized projection declines it.
The benchmark rotates the control, the plain Arrow score, and the exact ordered path, and asserts identical result IDs and distance bits for each query and filter.

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
