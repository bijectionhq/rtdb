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
