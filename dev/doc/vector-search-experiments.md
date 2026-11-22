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
The centroid index packs the original float32 vectors into one immutable object with a byte range per cluster.
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
    uv run --no-project --with numpy==2.5.5 --with h5py==3.16.0 \
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
The dataset and generated binary files stay in the ignored build directory.

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

The stationary and churn runs occurred at different levels of host load.
Compare the query paths within each run and use the churn tests and recall results to assess the effect of changed rows.

## Architecture decision

Keep `COSINE_DISTANCE` and filtered exact SQL as the default query contract.
The existing ordered index already gives highly selective filters a faster route without changing any returned distance or row.
The next exact execution optimization should evaluate typed Arrow float buffers in batches and fetch other projected columns only for the winners.
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

Keep fixed-probe ANN in the benchmark until filtered recall and cold provider I/O have been measured against a batched exact baseline.
The present cold measurements count local range requests and bytes.
The pack time excludes centroid training, and the churn timings leave full generation rebuilds outside the write path.
The rebuild and generation-retirement costs still need to be measured over sustained writes.

## Verification

The native suite passed all 83 tests and the SQL array suite passed all six tests.
The separate release test of the centroid read path passed its full-shortlist removal, retained snapshot, and coverage fallback assertions.
The full Gradle unit suite passed all 3,190 tests with the native library built from the draft branch.

## Arrow path inventory

The vector workload uses fixed-size float lists in the primary table.
Here are the representation boundaries on that path and the work each one currently does.

| Stage | Current representation and extra work | Arrow path |
| --- | --- | --- |
| Benchmark preparation | The HDF5 reader and centroid trainer use NumPy arrays, then write separate raw floats and cluster IDs. The Rust loader parses the floats one at a time. | Write Arrow IPC record batches with fixed-size float lists and the cluster IDs beside them. |
| SQL and ADBC ingest | Native document operations carry a map per row and one `Value` per component. The ADBC ingest path turns Arrow rows back into those maps. The source transaction then builds Arrow columns for logging. | Keep typed input columns from ADBC and the benchmark loader. Validate row IDs and temporal bounds alongside the arrays. |
| Source replay and staging | The source log uses Arrow IPC. Replay materializes each document, transaction staging clones it, and the event builder recreates the arrays before the replica log. | Retain the input Arrow arrays through transaction resolution and event construction. Share the existing event layout and temporal operation order. |
| Flush, compaction, and scans | Event pages use Arrow. The direct gather for primitive columns excludes fixed-size lists, so embeddings decode into per-component values and are rebuilt when needed. | Gather compatible typed list children directly and preserve validity at both the list and component levels. |
| SQL distance | Scalar projection rebuilds row values and walks the query vector again for each embedding. The result may be converted back to Arrow before top-k. | Prepare one query vector per invocation and compute an Arrow double distance column directly from the selected embedding buffers. |
| Centroid index | The prototype writes a custom little-endian record per vector and decodes every component from bytes while scoring. | Store one range-addressable Arrow record batch per cluster and borrow buffers during probing. |
| ANN handoff | Reranking turns candidate IDs into SQL text with one disjunct per ID, reparses it, and then scores resolved rows. | Pass an internal typed IID scope and reuse the same Arrow distance kernel after resolving those rows. |
| Output | ADBC and Flight SQL use Arrow; PostgreSQL clients receive PostgreSQL protocol values. | Keep Arrow through execution and convert at the wire boundary selected by the client. |

The relevant implementation boundaries are `ffi/src/statement.rs`, `tx/src/source.rs`, `tx/src/values.rs`, `tx/src/resolver.rs`, `index/src/live.rs`, `format/src/data.rs`, `query/src/columnar.rs`, and the native vector benchmark.
A typed route must preserve missing and null values, invalid-component errors, numeric distance and tie behavior, and the table snapshot's visibility.
