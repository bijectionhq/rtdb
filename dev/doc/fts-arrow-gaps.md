# RTDB FTS Arrow compatibility gaps

This source inventory covers native RTDB full-text search at `54ee733709` on `perf/arrow-fts`, from ingestion and replay to indexing, query execution, and client output.
The JVM XTDB implementation in this checkout provides the reference for Arrow operator boundaries and client parity.
RTDB already stores search manifests, postings, positions, and ranking bounds in Arrow IPC, and compatible paths can read indexed text from Arrow and return Arrow hit or SQL result batches.

See [FTS behavior](fts.allium) and [implementation notes](fts-implementation.adoc) for the search and temporal contracts.

## Input, replay, and schema compatibility

1. **Physical encodings of text.**
   The shared text reader borrows ordinary `Utf8`, including nullable cells, dense unions, and supported run-end encoding.
   `LargeUtf8`, `Utf8View`, dictionary-encoded text, and sparse unions lack a complete logical admission and FTS access path.
   The IPC decoder understands more physical layouts than the engine admits.
   This affects indexed source text, SQL and bound query text, and scan fallback.
   [Logical schema](../../rust/crates/columnar/src/schema.rs#L210) · [Text reader](../../rust/crates/columnar/src/text.rs#L14) · [IPC layout handling](../../rust/crates/format/src/ipc/nulls.rs#L61)

2. **Corpus collections and document extensions.**
   Five-argument `SEARCH_SCORE` and `SEARCH_CORPUS_FINGERPRINT` consume plain `List` and `FixedSizeList` cells, with no large-list or list-view path.
   Frequency lists still become a per-score count vector.
   The `set` Arrow extension is recognized by the logical schema but excluded from the document Arrow fast path.
   An unsupported neighboring field can send an entire upload through document values even when its indexed text is already canonical.
   [Search list reader](../../rust/crates/query/src/columnar/search.rs#L62) · [Corpus frequencies](../../rust/crates/query/src/columnar/search.rs#L194) · [Set extension](../../rust/crates/columnar/src/schema.rs#L184) · [Document layouts and metadata](../../rust/crates/tx/src/values.rs#L270)

3. **Public ingestion and SQL writes.**
   Flight SQL bulk ingest, pgwire Arrow COPY, embedded ADBC, and SQL document mutations have different admission gates and row fallbacks.
   Embedded ADBC bulk ingest only attempts direct ingestion for a single bound page.
   Canonical ingestion used by ADBC and pgwire can decline layouts with a null first cell, nested nullable data, or field metadata before trying document values.
   Static SQL puts can project compatible Arrow bindings into document columns, but unsupported projections and other mutations can materialize rows.
   Ordinary pgwire binds, Transit COPY, and scalar document APIs are explicit row-oriented input boundaries.
   [Flight SQL ingest](../../rust/crates/flight-sql/src/upload.rs#L68) · [Arrow COPY](../../rust/crates/pgwire/src/copy.rs#L90) · [ADBC bulk ingest](../../rust/crates/ffi/src/statement.rs#L214) · [Canonical eligibility](../../rust/crates/tx/src/values.rs#L333) · [SQL submission](../../rust/crates/tx/src/submission.rs#L65)

4. **Owned writes and source-log replay.**
   Owned Flight document writes turn Arrow payloads into document values, including reconstructing field presence where applicable.
   The Arrow source log retains document arrays on replay for compatible ordinary puts with recorded IIDs.
   Owned insert and replace, ordinary insert, patch, and ineligible puts reconstruct document values during replay.
   Ingestion and replay both need coverage for Arrow preservation.
   [Owned document decoding](../../rust/crates/flight-sql/src/upload.rs#L109) · [Owned write callers](../../rust/crates/flight-sql/src/owned_actions.rs#L653) · [Replay](../../rust/crates/tx/src/source.rs#L1325)

5. **Event page assembly and outgoing schema conformance.**
   Live and persisted event page assembly can gather compatible cells from Arrow positions and relayout columns.
   When that path declines a column, the page builder reads cells into values and rebuilds the column.
   Flight SQL, ADBC, and Arrow COPY OUT use result conformance, which reuses exact-layout buffers and tries physical relayout before reading and rebuilding cells.
   A projected payload field can hit this fallback even when MATCH and score columns are Arrow.
   ADBC also copies foreign Arrow buffers on bind to own their lifetime, which is an Arrow-to-Arrow copy.
   [Event assembly](../../rust/crates/format/src/data.rs#L605) · [Result conformance](../../rust/crates/columnar/src/conform.rs#L55) · [ADBC bind ownership](tx.allium#L19)

## Index construction and query execution

6. **Live postings and immutable search file construction.**
   The live index reads Arrow text one event at a time and accumulates a term-to-row map before publishing Arrow postings and token arrays.
   Compaction output, backfill, repair, and query-time artefact construction borrow source cells and build per-entity histories, visibility rectangles, token runs, and posting collections before writing Arrow IPC.
   An Arrow page input and output still leave room for shared batch tokenization and positional builders within these stages.
   [Live postings](../../rust/crates/search/src/runtime.rs#L49) · [Artefact build](../../rust/crates/search/src/lane.rs#L174) · [Artefact writer](../../rust/crates/search/src/file/write.rs#L189)

7. **Public MATCH candidates and verifier scope.**
   Live and persisted candidate discovery use `BTreeSet<Iid>`.
   The scan prunes against a sorted Arrow IID column but reconstructs a set for later accelerators and MATCH lookups.
   The verifier also consumes a set of IIDs.
   The selection should survive discovery, intersection, pruning, verification, and projection as sorted Arrow IIDs, vector positions, or masks.
   [Candidate collection](../../rust/crates/search/src/accelerate.rs#L335) · [Scan handoff](../../rust/crates/query/src/scan.rs#L495) · [Verifier](../../rust/crates/query/src/scan.rs#L856)

8. **MATCH masks, public ranking, and temporal fallback.**
   Resolved positions are processed individually when reading cached MATCH values and when borrowing and retokenizing fallback text.
   Ranking uses Rust candidate collections, deduplication, temporal rectangles, and verified score tuples before producing Arrow output columns.
   Eligible point scans reuse verified scores, while range scans and unverified rows score their resolved text.
   Keep the visibility checks, candidate bounds, and tie rules while improving the boundaries around them.
   [MATCH and score columns](../../rust/crates/query/src/scan.rs#L1306) · [Public ranking](../../rust/crates/search/src/accelerate.rs#L757) · [Ranking contract](fts.allium#L236)

9. **SQL expression coverage, operator composition, and sort.**
   Arrow kernels cover supported shapes of `MATCH`, `SEARCH_SCORE`, `SEARCH_LENGTH`, `SEARCH_TERMS`, and `SEARCH_CORPUS_FINGERPRINT`.
   Unsupported expressions or enclosing operators can lead to `Value` evaluation and row cursors.
   Batch sort can materialize unsupported keys while retaining Arrow payloads and IPC spill files.
   The row sort spill path uses JSON.
   Accelerator eligibility is independent of whether a plan can remain columnar, so qualify both with FTS enabled and with scan-only execution.
   [Search kernels](../../rust/crates/query/src/columnar/search.rs#L151) · [Scalar search expressions](../../rust/crates/expr/src/search.rs#L9) · [Operator fallback](../../rust/crates/query/src/operators.rs#L537) · [Sort keys](../../rust/crates/query/src/sort/batches.rs#L665) · [Row spill](../../rust/crates/query/src/spill.rs#L53)

10. **Owned held corpus, ranking, and native APIs.**
    The held corpus uses Arrow counts and visible-run arrays internally.
    Coverage and typed identity checks use maps and sets, while the ranking shortlist uses tuples and point verification can own a candidate identity.
    The native `search_corpus`, `ranked_search`, and `ranked_search_page` APIs return a corpus map or materialized hits.
    The native `ranked_search_batch` already returns Arrow hit columns with separate witnesses.
    Provide equivalent Arrow forms and retain legacy object-returning APIs as explicit adapters.
    [Corpus and ranking](../../rust/crates/search/src/accelerate.rs#L381) · [Native held APIs](../../rust/crates/node/src/node.rs#L1210)

## Wire, storage, and tooling

11. **Flight and PostgreSQL protocols.**
    `DeclareSearchPath.v1` has a binary relation prefix.
    `SearchHeld.v3/v4` use custom binary requests and already return Arrow hits.
    `DeclareSearchPath.v2` and `SearchHeld.v5` request Arrow IPC with strict single-batch schemas, including canonical UTF-8 and fixed-width fields.
    Ordinary pgwire uses PostgreSQL `DataRow`, and Arrow COPY supplies an IPC result transport on the same protocol.
    Preserve supported legacy wire contracts at the adapters.
    [Flight contracts](../../rust/crates/flight-sql/README.adoc#L339) · [Held request admission](../../rust/crates/flight-sql/src/owned_actions.rs#L1253) · [pgwire output](../../rust/crates/pgwire/src/protocol.rs#L599) · [Arrow COPY OUT](../../rust/crates/pgwire/src/copy.rs#L18)

12. **Artefact versions, held coverage, remote reads, and scan delegation.**
    Search IPC files have an RTDB-specific manifest and require the current format and analyzer versions.
    Older artefacts require rebuilding.
    Public search can rebuild or scan when eligible artefacts are unavailable, while held search requires complete format-11 coverage at its pinned cut.
    Production remote reads fetch the whole object before term-batch access, although the range-based reader exists in experiments.
    Delegated scan results carry Arrow, but scan-level MATCH, ranked scans, and IID-narrowed scans miss the delegation path.
    [Manifest validation](../../rust/crates/search/src/file/manifest.rs#L38) · [Held coverage](../../rust/crates/flight-sql/README.adoc#L366) · [Remote file access](../../rust/crates/search/src/runtime.rs#L186) · [Ranged experiment](../../rust/crates/node/examples/support/fts_generations.rs#L63) · [Scan delegation](../../rust/crates/query/src/scan.rs#L546)

13. **Search definitions, publication, and attestations.**
    Definition writes emit Arrow document columns, while definition readers create Rust control structs.
    Protobuf artefact records carry Arrow IPC statistics, and search decodes the statistics into section structs.
    Held basis, totals, scope, and witness references use schema metadata or native witness types.
    Keep publication, capabilities, validation, and witnesses as control fields unless a typed Arrow consumer needs the underlying data.
    [Definition writes](../../rust/crates/tx/src/resolver.rs#L852) · [Definition reads](../../rust/crates/search/src/definitions.rs#L73) · [Artefact publication](../../rust/crates/search/src/lane.rs#L104) · [Section statistics](../../rust/crates/search/src/file.rs#L166) · [Held metadata](../../rust/crates/flight-sql/README.adoc#L359)

14. **Diagnostic and experimental paths.**
    `SearchRow` and `search_pages` expose owned diagnostic rows.
    The workload and same-file experiments materialize SQL values, generation experiments synthesize owned search rows, and corpus checkpoint replay reconstructs maps from Arrow checkpoint and delta batches.
    Use Arrow APIs for benchmarked production data paths and keep independent scalar reference implementations for parity checks.
    [Diagnostic search pages](../../rust/crates/search/src/lane.rs#L271) · [Workload](../../rust/crates/node/examples/support/fts_workload.rs#L64) · [Same-file oracle](../../rust/crates/node/examples/support/fts_same_files.rs#L88) · [Generations](../../rust/crates/node/examples/support/fts_generations.rs#L27) · [Corpus checkpoints](../../rust/crates/node/examples/support/fts_corpus_checkpoint.rs#L54)

## Implementation handoff

- Use XTDB's selection and projection pattern to compose positions over retained vectors and pass columnar pages between operators.
  [Relation selection](../../core/src/main/kotlin/xtdb/arrow/RelationReader.kt#L67) · [Vector indirection](../../core/src/main/kotlin/xtdb/arrow/IndirectVector.kt#L60) · [Projection](../../core/src/main/kotlin/xtdb/operator/ProjectCursor.kt#L21)
- Share a typed Arrow admission and normalization contract across Flight, C Data, COPY, source-log replay, event pages, lookup, and result export.
  Preserve extension semantics and keep compatible buffers where possible.
  [XTDB set vector](../../core/src/main/kotlin/xtdb/arrow/SetVector.kt#L12) · [Conforming unloader](../../core/src/main/kotlin/xtdb/arrow/ConformingUnloader.kt#L59)
- Exercise encoding and batch-boundary parity alongside null versus absent, complete typed IDs, temporal visibility, corpus counts, score and tie ordering, and artefact rebuild or replay.
  Keep scalar decoding at explicit compatibility adapters, and keep heaps, lookup tables, and witness state where they serve the algorithm or protocol.
  [XTDB output parity](high-level-tour.adoc#L293)
- Finish the full implementation, regression coverage, and applicable specification updates before the first compilation or qualification run.
  Run qualification at the end under the repository's `AGENTS.md` and testing skill, and record the outcome against every inventory item.

## Implementation coverage

| Item | Data path and boundary |
| --- | --- |
| 1 | Shared logical admission, borrowed UTF-8 access and Arrow relayout cover large offsets, views, text dictionaries, sparse unions and run ends. |
| 2 | List element spans serve corpus and fingerprint kernels, and document ingestion retains the set extension. |
| 3 | Arrow ingestion admits nullable and attributed fields, and ADBC submits a bound stream's pages as one operation. |
| 4 | Fact payloads and compatible document replay retain Arrow arrays. Field presence travels with owned operations and world controls retain their native validation. |
| 5 | Event assembly and result conformance align compatible Arrow cells and their extension metadata. |
| 6 | Live and immutable search construction tokenize event pages into list vectors before positional posting assembly. |
| 7 | Candidate discovery, handoff, pruning, MATCH lookup and verification use sorted Arrow IIDs. |
| 8 | MATCH caches supply selected Boolean cells, and fallback and verification tokenize resolved positions in batches. |
| 9 | FTS operands and supported composition execute over Arrow, columnar operators retain payloads across expression evaluation, and sort spill frames carry Arrow IPC. |
| 10 | Held corpus and ranks expose Arrow batches, with legacy native APIs adapting the results. |
| 11 | Existing Flight action versions and PostgreSQL row protocols retain their framing and schema requirements. |
| 12 | Searches load validated posting ranges from range-capable stores. Delegated search scans carry the bound query and IID scope, and fall back locally for readers without search projection. |
| 13 | Publication, validation, capabilities, and witnesses remain typed control fields. |
| 14 | Benchmark paths retain Arrow query results and construct search files from event batches, and checkpoint replay references retained Arrow positions. |

## Qualification

The locked Rust workspace suite passed all targets with the FTS experiments feature enabled.
The result includes 5,671 passing tests, zero failures and 15 tests marked ignored.
The Gradle `test`, `integration-test`, and `property-test` tasks passed with the rebuilt native library and one Gradle worker.
The result includes 12,175 passing tests and one skipped PostgreSQL source test, with no reflection or boxed-math warnings.
The Arrow diagnostic smoke runs passed for corpus checkpoint replay, file generations, same-file ranking, and the 20,000-document architecture fixture.
The architecture fixture agreed with its reference results in all six query cases.
