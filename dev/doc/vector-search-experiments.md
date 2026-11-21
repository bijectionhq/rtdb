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
It will serve as the oracle for approximate retrieval.
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
Compare every temporal correctness case to the exact SQL path, including retained readers and rebuilds under concurrent writes.

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
  cargo run --manifest-path rust/Cargo.toml --locked --release -p rtdb-node \
    --example fts_architecture_bench -- vectors "build/vector-search/glove-$rows" 12 \
    > "build/vector-search/result-$rows.json"
done
```

The downloaded GloVe data used for the run has SHA-256 `544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4`.
The run uses the first twelve held-out queries and independently trains 512 centroids for each size.
The dataset and generated binary files stay in the ignored build directory.
