# RTDB engine verification record

This record traces local evidence to the [engine property catalog](rtdb-engine-proof-properties.md) and the [per-property ledger](rtdb-engine-verification-ledger.md).
The code baseline for the checks is `e86af7f12`, with the proof harnesses and SQL regressions in this branch.
Abstract theorems, checks of actual Rust functions, and observations through the SQL interface have separate scopes.
The table records remaining end-to-end obligations for each area checked here.
Catalog targets absent from this table remain open in this verification pass.

## Checked scope

| Catalog target | Evidence | Open obligation |
| --- | --- | --- |
| TIME01 · Two independent axes | The Lean projection theorem agrees with chronological replay for a point on both axes within an already captured history. | Refine query basis capture, SQL binding, and snapshot assembly. |
| TIME02 · Interval endpoints | Kani checks the production Rust interval intersection and overlap predicates for all valid half-open intervals and points over signed 64-bit timestamps. | SQL syntax normalization and the other interval conversions. |
| TIME04–TIME09 · Point history | Lean proves that newest-first lookup and the descending interval ceiling scan have the same per-point result as chronological replay. Separate lemmas cover valid-time locality, delete, erase, writes after erase, exclusive valid-time ends, entity isolation, and future ordinary mutations. | Refine the Rust calculators and prove that the scan supplies the required event order and coverage. |
| TIME10 · Rectangle projection | Lean proves exact per-point coverage and multiplicity after clipping and updating a partition of valid time. Kani checks the Rust `PolygonView` for all signed 64-bit endpoints and ceilings when the domain has one interior split, including empty intervals at both timestamp extrema. | Prove the Rust slice selection, ceiling updates, and fragment emission for arbitrary partitions, including the exact bounds for the supported query projections. |
| TIME11 · Specialized equivalence | Lean relates the modeled range output to the point reference inside the query bounds. The native SQL regression compares point reads with a separate chronological reference. | Refine each implemented specialized path and the general resolver to the common semantics. |
| SCAN01–SCAN05 · Scan soundness | Lean proves that dropping events which cannot decide a point observation preserves that observation. The native regression checks results with temporal pruning enabled and bypassed, including a predicate that should exclude a corrected value. | Establish the proof premise for each implementation selector and all supported history and metadata layouts. |
| LOG08 and FMT01 · Packed positions | Kani checks Rust source and replica packing, extraction, natural order, and same-epoch and foreign-epoch resume for epochs 0 through 32,767 and nonnegative 48-bit offsets. A separate harness checks all negative message IDs and the packed `-1` start sentinel for those epochs. | Show that callers establish the checked packing domain, and prove the catalog-to-log boundary and persistent codec contracts. |
| IDX04 · Bucket routing | Kani checks the production Rust IID bucket against independent bit extraction for every legal depth, every 128-bit IID, and bucket widths of 2, 4, and 8 bits. | Prove that every insertion, split, and serialized traversal uses the corresponding correct path. |
| COMP02 · Merge equivalence | Lean proves that joining two already ordered history segments gives the same point result as replaying them in sequence. The native regression checks visible results before and after actual compaction. | An implementation theorem for merge selection, emitted rows, full range results, and each supported layout. |
| COMP05 · Recency agreement | Lean proves that early-exit recency equals a full fold over any list of descending, non-overlapping valid-time ranges. Kani checks Rust `Polygon::recency` for three ranges with arbitrary ordered signed 64-bit bounds and signed 64-bit ceilings. | Prove that the Rust writer and readers establish the ordering premise, and establish the implementation refinement for arbitrary range counts and supported storage layouts. |
| CELL09 and recovery S2 and S5 · Recovery after worker loss | A multi-mode SQL fixture preserves a published checkpoint row, acknowledged tail updates and deletes, and subsequent writes through the loss of one, two, and all three serving workers. | Prove the recovery and readiness guarantees across all admitted traces and provider implementations, including the control-plane and key prerequisites. |
| Recovery S4 · Logkeeper recovery decisions | Kani checks the production recovery winner for all triples of sealed replies by synced epoch, last position, and reply index on a tie. Lean proves that the first retained epoch divergence is detectable at the start or at a breakpoint from either run list. | Relate Rust `plan_repair` inputs and returned positions to the theorem, and prove that equal epochs imply equal payload prefixes under the protocol. |
| Recovery S1–S7 and conditional progress | The proof runner checks the Lean files and 14 TLC configurations covering log recovery, journal identity, cell transfer, and erasure. Five configurations satisfy their checks, and nine produce their expected invariant violation or reachability trace. | The recovery proof inventory lists the remaining protocol and implementation obligations. |

The Lean point model takes an already captured history in transaction order, with equal-time events ordered by input position.
It treats an erase in that history according to the documented erase semantics.
The projection theorem assumes nonincreasing system times for each IID in the descending input, a query point inside the valid-time domain, and a system time below the initial ceiling and query upper bound.
It shows zero or one observed range row at each point and permits eliding ordinary events at or above the query's exclusive system-time upper bound while retaining erases.
The valid-time clipping theorem preserves observations inside the query's valid-time range. [Ceiling model](../lean/TemporalCeiling.lean) · [Projection theorem](../lean/TemporalProjection.lean)

The filtering theorem assumes that every omitted event has no decision at the requested point, including when the event belongs to an older segment.
The recency theorem assumes each remaining range ends at or before the current range's lower bound.
The repair theorem compares epoch labels within a common retained interval using newest-first run lists, including the interval start as a possible boundary.
The Kani exploration of the dynamic Rust repair planner ended before a verification result. [Point-history theorems](../lean/TemporalHistory.lean) · [Recency theorem](../lean/Recency.lean) · [Repair boundary theorems](../lean/RepairBoundary.lean) · [Recovery proof status](rtdb-recovery-properties.md#proof-status)

The SQL regression exercises the production native node, transactions, SQL binding, scans, local log and object files, compaction, and reopening.
It runs 1,120 query comparisons across both pruning settings and five states: live, flushed, compacted, compacted with a live tail, and restarted.
Its observations cover the named fixture, timestamp grid, and projections. [SQL regression](../../rust/crates/node/tests/native.rs)

The worker-loss regression uses three workers, three durable local Logkeepers, a loopback MinIO bucket, an in-memory directory, and a local key service.
Each loss includes the active holder and stops the selected workers without parking their deployments.
After the full loss the fixture verifies every worker is stopped, then starts a worker with a fresh member ID and cache location.
The test verifies a newer writer epoch, the original published checkpoint, the exact current SQL rows, and a new acknowledged write following each loss.
The directory and key service remain available throughout.
Across three local runs, the elapsed time from initiating the loss to verifying the first recovered read was 1.48–1.49 seconds for one worker, 1.48–1.59 seconds for two workers, and 1.47–1.58 seconds for all three workers.
These timings describe the fixture and its configured 1.5-second worker lease. [Worker-loss regression](../../rust/crates/multi/tests/s3.rs) · [Multi-cell test fixture](../../rust/crates/multi/tests/common/mod.rs)

The Gradle unit, integration, and property suites passed locally with one tagged PostgreSQL integration case skipped.
The native engine crate run passed 589 tests.
The native cell and chaos suites passed 61 tests.
The Logkeeper segment test `streams_share_segments_that_a_trim_compacts_and_a_restart_replays` failed on the first combined native run and passed in isolation and in the subsequent 56-test Logkeeper run.

## Reproduce locally

Run Lean 4.36.0 and TLC 1.7.4 from the repository root.
The runner checks that the ledger covers the full catalog and compiles the Lean files, including their local imports.
The runner checks each committed configuration against its expected status and invariant diagnostic.
Its logs go under the ignored `build/proof-runs` directory.

```sh
LEAN=/path/to/lean TLA2TOOLS_JAR=/path/to/tla2tools.jar bash dev/check-rtdb-proof-models.sh
```

Run the Rust harnesses using Kani 0.68.0 from the Rust workspace.
The unwinding checks are part of each bounded result. [Kani installation](https://model-checking.github.io/kani/install-guide.html)

```sh
cd rust
cargo kani -p rtdb-temporal -p rtdb-types -p rtdb-trie --tests --output-format terse
cargo kani -p rtdb-temporal --lib --output-format terse
cargo kani -p rtdb-logkeeper --lib --output-format terse
```

Run the SQL regression from the repository root.

```sh
cargo test --locked --manifest-path rust/Cargo.toml -p rtdb-node --test native \
  bitemporal_sql_matches_ordered_history_through_flush_compaction_and_restart -- --exact
```

For the worker-loss regression, set `RTDB_S3_ENDPOINT` to a reachable S3-compatible endpoint and set `RTDB_S3_BUCKET` and the AWS credentials for a fixture bucket.
The test uses a unique prefix and skips the S3 path when the endpoint variable is absent.

```sh
cargo test --locked --manifest-path rust/Cargo.toml -p rtdb-multi --test s3 \
  checkpoint_and_durable_tail_survive_loss_of_one_two_or_all_workers -- --exact --nocapture
```

The rest of the catalog needs property-specific evidence.
Its operational latency and physical memory targets also require measurements against the stated workloads and provider assumptions.
