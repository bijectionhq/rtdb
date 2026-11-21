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
