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
The bounded score window can use indexed ranking when the corpus statistics are bound parameters or literals and the MATCH and score use the same indexed analyzer and column.

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

The exact recipe has `approximate = false`.
Reject a zero or malformed probe at admission even when the candidate relation is empty.

## Native IVF

Negotiate `query.ranked-ivf` and `query.clear-rank-input` on the same v1 attachment.
Send `QueryRequest.ranked_ivf` (field 11) with `track = true`, `require_clear_rank_input = true` and `Source` set to `main`, `LAYER_MERGED` and `Exactly(basis)`.
Leave `sql`, `params_ipc`, `page` and `verify_ranked_artefacts` unset.
Select one user table and IVF index by schema, table, index name and the original declaration position from `xt.ranked_indexes.version`.
Supply the held valid instant in signed microseconds, a finite nonzero float32 probe of the declared width (1 to 4096), and `k` from 1 to 256.
The window starts at zero and has no continuation.

Use up to eight distinct declared partition columns as filters, each with 1 to 64 ordinary Binary values of up to 1024 bytes.
The server applies AND across columns and OR across a column's values.
For up to 64 staged mutations, send the complete typed `identity_key` of each masked row (up to 4096 bytes), and include a replacement only when the staged row has an embedding.
A replacement carries a finite float32 vector of the declared width and one optional Binary filter value per requested column in request order.
An omitted value represents SQL null, and a present empty byte string matches an empty byte string.
A mask hides the matching stored identity even when its replacement is absent, fails the filters, or has zero norm.

The mounted Default reader admits the current merged-main publication paired with the selected native basis and clock.
The request requires a complete clear contributor inventory, a retained current primary catalogue, and verified installed IVF files for the declared generation.
Missing coverage, a live frontier, relevant historical tries or exhaustion of the 65,536-IID budget can refuse the request.
Up to 256 MiB of persisted generations are read within the existing storage and query budgets.
Each round resolves visible stored rows for the shortlisted IIDs and applies masks and filters before ranking the original f32 vectors.
It adds eligible staged replacements and widens when fewer than `k` rows qualify, stopping at a full window, an exhausted shortlist or refusal.
Missing and zero-norm vectors contribute no distance.

The Arrow result has non-null `identity_key` (Binary) and `distance` (Float64), ordered by ascending cosine distance and then complete typed identity.
The handle encodes a string as tag 0 followed by UTF-8, a keyword as tag 1 followed by its UTF-8 name, an integer as tag 2 followed by its sign-biased big-endian 64-bit encoding, and a UUID as tag 3 followed by its 16 bytes.
`QueryTrailer.ranked_ivf` (field 10) gives the executed declaration generation, `candidate_count` and `approximate`, alongside the basis and branch-wide claim even for an empty window.
`candidate_count` counts the filtered, scored rows in the settled round, including replacements.
When `approximate = true`, the candidate count covers the shortlist, and closer eligible rows may exist beyond it.
When `approximate = false`, the reader exhausted the eligible IVF candidate set for the request.
Distances use the same component-order binary64 cosine arithmetic as `COSINE_DISTANCE_F32`.

For Bijection's `FactSnapshot::v1_nearest`, retain its path, placement, clear-presence, label, staged-value and posting-marker admission at the held cut.
Use the admitted relation in `public`, the path's vector column as the index name, the declaration's original version, and the path's `filter_column` names and encoded filter keys.
Map each staged address to its complete string handle using tag 0 and the UTF-8 `address.to_wire()` bytes.
Select replacement filter values by the requested ordinals, preserving missing values.
Decode and re-admit each returned address against the same path, validate the generation, basis, ordering, distinct keys and row count, and retain the trailer claim with the posting-marker claim where applicable.
Use `candidate_count` as the count for the returned candidate set and carry `approximate` with it.
A caller that requires index-backed execution should refuse an absent capability or an unavailable IVF request.
A caller that allows exact fallback can issue the exact SQL recipe at the same held basis after either refusal.

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
To return the bound without returning the extra document, materialize the ordered prefix and split it into the hit projection and a scalar bound as below.

```sql
WITH MATERIALIZED candidates AS (
    SELECT _id, _rtdb_written_basis, document, geometry
    FROM places FOR VALID_TIME AS OF $1 WHERE partition_key = $2
    LIMIT 100001
), admission AS (
    SELECT RANK_ADMITTED_COUNT(COUNT(*), 100000) AS admitted FROM candidates
), MATERIALIZED distances AS (
    SELECT _id, _rtdb_written_basis, document,
           SPATIAL_DISTANCE(geometry, $3, 'tagged-wkb-v1', 'wgs84_geometry_m', $4, $5) AS distance
    FROM candidates
), MATERIALIZED prefix AS (
    SELECT _id, _rtdb_written_basis, document, distance, ENTITY_SORT_KEY(_id) AS identity_key
    FROM distances WHERE distance IS NOT NULL
    ORDER BY distance, identity_key LIMIT 21
), hits AS (
    SELECT * FROM prefix ORDER BY distance, identity_key LIMIT 20
), omitted AS (
    SELECT distance FROM prefix ORDER BY distance, identity_key LIMIT 1 OFFSET 20
)
SELECT hits._id, hits._rtdb_written_basis, hits.document, hits.distance, hits.identity_key,
       omitted.distance AS next_distance_m, admission.admitted
FROM admission LEFT JOIN hits ON TRUE LEFT JOIN omitted ON TRUE
ORDER BY hits.distance, hits.identity_key
```

The first page omits the keyset predicate, and following pages add it to the prefix's filter.
The single null-identity result on an empty page carries the admitted count along with the Query trailer's claim.
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
The v1 catalog SQL reports `serving_mode = 'exact-sql'` and `approximate = false`.
Opt in to [physical readiness verification](rtdb-ranked-native-artefacts.md) to obtain `artefacts_ready = true` where the installed exact cut supplies persisted coverage.
The search accelerator can use the same declaration projected into `xt.search_indexes` for eligible MATCH plans.
A startup policy that requires persisted indexes can gate on `artefacts_ready`.
A native IVF consumer also requires the negotiated execution capability and a successful execution trailer for the requested generation.
