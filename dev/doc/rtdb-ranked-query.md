# Exact ranked Query

Send read-only SQL in `rtdb.v1.QueryRequest` with the selected `Source.At.Exactly`, Arrow IPC parameters and `track = true`.
Negotiate `query.clear-rank-input` and set `require_clear_rank_input = true` on each ranked read.
The producer requires an unscoped, tracked exact basis with a complete empty protected contributor inventory over the applied branch view.
Treat failure or an absent capability as a route refusal.
Use the same branch, layer, basis, and valid instant for every phase of a rank.
Retain the Query trailer and its claim for every table read, including an empty window.
Admit the complete path and every staged value at the caller before supplying parameter bytes.

## Full text

`SEARCH_TOKEN_COUNT(body[, analyzer])` supplies the raw corpus length contribution.
`SEARCH_TERM_MATCHES(body, query[, analyzer])` supplies a boolean array in `SEARCH_TERMS(query[, analyzer])` order.
Aggregate the token counts and sum the true entries at each term ordinal across every admitted non-null document before applying MATCH.
Select the corpus with a materialized candidate CTE and a one-row overflow sentinel when there is a caller bound.
Refuse the result if the sentinel is reached.
Use `MATCH(body, query[, analyzer])` for phrase-aware hits and `COUNT(*)` on that same predicate for the total before paging.
Use `SEARCH_SCORE(body, query, documents, tokens, frequencies[, analyzer])` to order those hits by descending score and complete identity before applying offset and limit.
A direct MATCH on an indexed column can use indexed narrowing, including when the score uses supplied corpus statistics.

## Exact float32 nearest

`COSINE_DISTANCE_F32(embedding, probe)` requires equal-dimension finite float32 arrays and a nonzero probe of at most 4096 elements.
It returns null for a missing or zero-length-norm stored vector and computes distance with unscaled binary64 arithmetic in component order.
`ENTITY_SORT_KEY(_id)` yields the full typed identity as order-preserving bytes.
`RANK_ADMITTED_COUNT(count, maximum)` returns the count or raises `rtdb/rank-bound` if it exceeds the maximum.

Apply staged masks by full identity before unioning the staged replacements.
Apply filter predicates to stored rows and replacements before the candidate limit.
Use a materialized source with `LIMIT maximum + 1` and project the admitted count so overflow fails the request.
Score in a materialized CTE and count its non-null distances before offset and limit.
Return the total through a left join against the hit window so an empty page has one row with null identity and a non-null total.

```sql
WITH MATERIALIZED candidates AS (
    SELECT _id, embedding FROM docs FOR VALID_TIME AS OF $1
    WHERE embedding IS NOT NULL AND category = $3
    LIMIT 100001
), MATERIALIZED scores AS (
    SELECT _id, COSINE_DISTANCE_F32(embedding, $2) AS distance FROM candidates
), totals AS (
    SELECT RANK_ADMITTED_COUNT(COUNT(*), 100000) AS admitted,
           COUNT(distance) AS total FROM scores
), hits AS (
    SELECT _id, distance FROM scores WHERE distance IS NOT NULL
    ORDER BY distance, ENTITY_SORT_KEY(_id)
    LIMIT 20 OFFSET 0
)
SELECT hits._id, hits.distance, totals.total, totals.admitted, FALSE AS approximate
FROM totals LEFT JOIN hits ON TRUE
ORDER BY hits.distance, ENTITY_SORT_KEY(hits._id)
```

The exact recipe has `approximate = false`; it never requests an IVF shortlist.
Reject a zero or malformed probe at admission even when the candidate relation is empty.

## Exact spatial predicates and nearest

`SPATIAL_MATCH(geometry, region, encoding, predicate[, radius_m])` evaluates the existing exact WGS84 predicates on binary geometry.
Use equality on each typed partition column alongside the geometry predicate.
The stored encoding is `tagged-wkb-v1`, `bijection-geometry-v1` or `bijection-position-v1`; the query region uses tagged WKB.
`SPATIAL_DISTANCE(geometry, origin, encoding, metric[, radius_m[, after_radius_m]])` takes a Point origin and an explicit `wgs84_geometry_m` or `wgs84_reported_center_m` metric.
The first metric admits geometry rows, and the second also admits observed positions by their reported centers.
The optional upper radius is inclusive and the optional lower radius is exclusive.
A missing geometry or an ineligible distance gives SQL null.

Build a bounded, materialized source at the held valid instant with the equality predicates already applied.
Compute `SPATIAL_DISTANCE` once per candidate in a materialized CTE.
Project `_id`, `_rtdb_written_basis`, and the requested document columns from those same rows.
Require `query.written-basis` for the revision projection and check that each hit has a non-null revision.
Keep the original Query basis and claim, including when no row is returned.

For a nearest page of size `k`, use `WHERE distance IS NOT NULL` and order by `distance, ENTITY_SORT_KEY(_id)`, fetching `k + 1` rows.
The extra row's distance is the next-distance bound.
Keep the anchor from the last delivered row and use `distance > $after_distance OR (distance = $after_distance AND ENTITY_SORT_KEY(_id) > $after_key)` on the next exact-cut Query.
Keep the original radius and exclusive lower bound independent of that anchor.
For an unranked range order and seek by the complete identity key alone.
Bind any application continuation to the original basis, branch, layer, valid instant, projection, predicate, probe and exact partition values.
These SQL expressions perform bounded exact refinement over the resolved source.

## Declarations and readiness

Require `declare.ranked-index` in `rtdb.v1.Transact` and send `Declare.Index`.
`INDEX_KIND_SEARCH` uses `Index.search` (field 4) with a known analyzer and the history flag.
`INDEX_KIND_SPATIAL` uses `Index.spatial` (field 5) with the exact stored geometry encoding.
`INDEX_KIND_VECTOR` uses `Index.vector` (field 6) with `method = "ivf"`, dimensions from 1 to 4096, and recall in (0, 1].
For spatial and vector, send the value column first and then the ordered equality partition columns.

After an applied receipt, use a separate `Query` at `Exactly(receipt.basis)` to read `xt.ranked_indexes`.
Compare `_id`, `schema_name`, `table_name`, `kind`, `columns`, and the relevant `analyzer, history`, `geometry_encoding`, or `method, dimensions, recall` fields.
The `version` names the original declaration position and stays stable on an identical redeclaration.
The applied v1 snapshot reports `serving_mode = 'exact-sql'` and `artefacts_ready = false`.
The search accelerator can use the same declaration projected into `xt.search_indexes` for eligible MATCH plans.
A startup policy that requires a physical spatial or IVF index must gate the route on `artefacts_ready`.
