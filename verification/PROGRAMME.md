# RTDB verification programme

Nov 21, 2026 · Sacha Ichbiah

Nine workstreams remain before RTDB can claim its requirements end to end; each brief below is self-contained so it can be handed to one agent.
Source of truth: `dev/doc/rtdb-formal-verification-design.adoc`, `dev/doc/structural-redesign.adoc` and `verification/results/2026-11-21.adoc` on RTDB main.
This programme supersedes the earlier Claude Docs copy after the second structural review.

## Ground rules

Every agent works in RTDB (`github.com/bijectionhq/rtdb`), not Bijection, and follows the repo's own `AGENTS.md` and `dev/CODING.adoc`.

- **Worktree:** one private worktree per agent, created from a fresh `git fetch origin main`. Peers land several commits an hour, so refetch before every milestone and before pushing.
- **Authorization:** the owner authorized local proofs, tests and builds for this programme. Start every such command with `BIJECTION_LOCAL_EXECUTION=owner-authorized` as its first token; a leading `cd` makes the guard hook refuse.
- **Toolchain** (already installed in `/private/tmp/rtdb-formal.QMjPSJsX`): Verus and Z3 under `verus/verus-arm64-macos/`, `tla2tools.jar`, `alloy.jar`, `apalache-0.63.0/`, `maude/maude`, mCRL2 under `mcrl2-202607.0/mCRL2.app/Contents/bin`, AutoHyper and dotnet 8 under `autohyper-c94722d1/`. Lean is `~/.elan/toolchains/leanprover--lean4---v4.36.0/bin/lean`; Java is `/opt/homebrew/opt/openjdk@21/bin/java`.
- **Runner:** `verification/run.py` with all tool paths, `--rust-boundary --native-replay` and a fresh `--evidence` directory; the invocation is in `verification/README.adoc`. A full pass takes about an hour. A new Verus area is a directory under `verification/verus/` with `area.json`, `proof.rs` and `laws.rs`; the runner finds it automatically.
- **Evidence standard:** every claim needs a passing baseline, a negative control that fails at the declared function or invariant, and, for a design model, a reachability witness and stated bounds. Production proofs verify the shipped source, not a copy.
- **Ledger:** record each result in `verification/results/2026-11-21.adoc` and update the area's row in the design doc's *Reproduction and current evidence* table. Sentence per line in docs.
- **Tests:** run the affected crates' suites before landing; a change to indexing, compaction or GC also needs the native replays. macOS has no `timeout` command, so a `timeout N cargo …` check silently runs nothing.
- **Landing:** conventional commits, rebased onto the latest main, pushed straight to `main`. Review affected source after each rebase and follow the active owner authorization for execution. When a peer has already fixed the same thing, drop your commit (`git rebase --skip`) instead of merging both.
- **Before starting a workstream:** read the last day of `git log origin/main` for its area. Several peer sessions are active, and an obligation may already be closed.

## Owner decisions

The first review set the original choices on 2026-11-21.
The second review narrows the fork decision to shared custody and defines how erasure can commit.

| Decision | Outcome |
| --- | --- |
| Read basis and resumption | An await token is a causal lower bound. A time cap filters a captured prefix. SNAP-1 reopens only an authenticated exact applied cut with its original selectors and query clock, subject to current scope, retention and custody. Fresh admissions can include later corrections beneath the same cap. |
| Reader lifetime | Readers are protected by renewable holds on the residence ledger (R4). Until R4 lands, bounded monotonic leases (default 1 h, 1 min margin) protect them. |
| Erasure versus admitted views | A view admitted before an erasure keeps its answers until the erasure deadline (≤ 120 s); after that a read that needs destroyed key material fails with `rtdb/erased-under-view`. SNAP-1 and DUR-1 are amended accordingly. |
| Forks | Erasure reaches shared history within its key custody domain. Independently keyed copies have their own erasure authority. |
| Future system time | Explicit system time lies between the preceding SQL system time and the original source-log append observation, inclusive. SQL system time may tie. Live decisions use the independent installed clock. |
| Retry identities | An exact transaction retry is scoped to its registered receiver, database and principal, with a body digest and a conflict outcome. An owned publication orders its separate slot witness with the guarded decision and effects. Business intent and application command comparison belong to the application boundary. |
| Transaction metadata | Retained only for the receipt window, and erasable with its unit before then (R8). |
| Physical erasure and non-interference | OPT-2 is in scope with the declared bound; OPT-3 only for rows and search scores of fresh views. |
| XTDB compatibility | Not a constraint. |

## Workstreams

W1, W2, W4 and W6 can start now and in parallel; W7 runs alongside all of them, and W9 closes the programme.

| ID | Workstream | Requirements | Depends on | Exit gate |
| --- | --- | --- | --- | --- |
| W1 | Reader entitlement on every read and delete path | SNAP-1, GC-1, GC-2, LIVE-2 | R4 supersedes the lease mechanism | Every read path holds a lease, hold or registry entry, and every deletion path waits for it; native replays and Verus laws for each |
| W2 | Recovery holds and root closure across authorities | DUR-2, GC-1 | — | Recovery keeps a usable root and log suffix under concurrent collection on every authority and deletion path |
| W3 | Certified commit decisions and protected applied fresh cuts | TX-1, TX-2, SNAP-1, DUR-1 | W1 for capture custody | Acknowledgement and successive fresh reads preserve quorum-installed cuts through recovery. Fair quorum service can answer while an uncertified tail is stalled. |
| W4 | Temporal composition end to end | TIME-1, TIME-2 | — | Production contracts for event order, visitor shells and range decomposition refine the Lean reference |
| W5 | Compaction refinement | COMPACT-1 | W4 for the history relation | Merged output refines the input history through every publication route, including retry and restart |
| W6 | Ranked search completeness | FTS-1, FTS-2 | — | Planner, visibility and index provably supply a complete frontier, and native postings refine the modelled summaries |
| W7 | Effect-path and bypass audit | All | Runs alongside W1–W6 | Every production decision's in-scope entry points establish its premises or carry a recorded proof obligation |
| W8 | Liveness and optional contracts | LIVE-1, LIVE-2, OPT-1–3 | R7, R8 for erasure | A progress witness and a fairness mutation for each adopted liveness claim |
| W9 | Combined evidence and readiness matrix | All | W1–W8, R3–R9 | One committed tree, one full run, and a per-requirement readiness matrix |

## W1 · Reader entitlement on every read and delete path

Goal: no entitled reader on any node, and no admitted in-flight request, ever loses a file it depends on, whichever path deletes it.

**Already in place:** query cursors and served scan fragments hold bounded leases on monotonic clocks, checked when each provider request is admitted. The trie collector waits a full lease after first observing a trie, and in-process readers are also registered with it. Multi deletes become releases that the cell reaper erases after its grace. Verus area `read_lease` and two conformance replays cover this.
Query capture, delegated fragments and membership queries refuse unconfirmed or expired leases at admission with the retryable `rtdb.node/catalog-not-current` error.
The native query fixture also checks that offline admission needs no replica lease.
The [collector turnover study](tla/CollectorTurnover.tla) checks fresh observation and conditional reclamation in a successor term.
The corresponding native fixture replaces the trie collector with another clock origin and observes reclamation after the fresh lease interval.
The local receipt fixture reopens the collector before the earlier filesystem hold drains and observes that the installed retirement remains pending until that hold ends.
A shared-journal replay elects a successor while the predecessor's query is open, installs retirement under the new term, and observes the idle leader finish after the query drops its hold.
The remote adapter replay retains the catalog owner until the response stream drains after cancellation.
The hosted replay sends an installed release from the directory floor to the exact reaper and checks recovery after a lost provider response.
Source and external transaction resolution request a store catalog hold before evaluation.
The local ledger fixture verifies that the transaction and its cursor retain the hold, then observes the floor advance after cursor drop and a subsequent stale capture refused.

**Start from:** `storage/src/{read_lease,lease_decision,io_region}.rs`, `node/src/{read_currency,database,node}.rs`, `maintenance/src/gc.rs`, `index/src/partition.rs`; models `tla/{LeaseAdmission,ReaderIO,ResidenceReaderRetention}.tla`.

**Open obligations:**

1. Audit every other read path: leader transaction snapshots on stores without a catalog ledger, spill and materialization, export, administrative collection, compactor reset, and ordered-index reads.
2. Audit every other deletion path against the same contract: block GC, artefact removal and repair, ordered-index storage reaping, and Multi region release, deployment removal and forks.
3. Leadership turnover: extend the shared-journal handoff to hosted term placement and cover the provider-drain and reaper contracts across the same deployment.
4. Compose the currency, caller admission and provider drain with the collector under the model's clock and I/O premises. The [currency model](tla/ReadCurrencyAnchor.tla) checks competing tip samples, and [Verus](verus/read_currency/proof.rs) proves the production observation and confirmation steps.
5. Connect the peer study `ResidenceReaderRetention` (generation cutoff vs exact dependencies) to the Multi reaper's actual release records.
6. LIVE-2: extend the idle-successor retry and deferred-settlement checks to renewed residence holds and providers under qualification.

**Done when:** each path above has either a native replay that fails under the matching mutation or a recorded reason it cannot strand a reader.

## W2 · Recovery holds and root closure across authorities

Goal: a recovery always finds a complete, readable root and the replica suffix it needs, while compaction, collection and trimming continue on any authority.

**Already in place:** discovery advances past a collected root or table block; recovery cursors are admitted before checkpoint install; the trie collector protects the installed checkpoint's live and nascent tries; Multi replica recovery holds and checkpoint metadata holds exist, with TLC, Apalache, mCRL2, AutoHyper and Lean evidence.

Archive capture now acquires or inherits a source metadata hold and keeps it until the provider work drains.
A model compares exact physical and whole-residence source holds and records the extra reclamation possible with exact holds.
Archive and independent fork preparation now carry each planned provider version through the exact copy read, including for inherited packs and repairs.
Durable repair archive admission now covers serving, recapture and transfer. Local copies keep the existing maintenance witness through provider completion.
The S3 adapter now executes and settles a removal addressed to an exact immutable object version.
Same-lineage fork publication now advances a cell-local shared-reference revision atomically with the child and name, including forks of a fork.
Manifest withdrawals apply to the latest confirmed manifest and preserve other committed removals. The reaper withdraws shadowed fallbacks before removing local objects or pending-release records.
Multi serving-store fallback reads now register against the exact physical owner's residence and retain the registration through provider completion, including cancelled callers. Whole-residence disposal drains those registrations before copying the referrers and erasing the source.
Local reclamation reserves the same lineage used by fork preparation. Whole-residence disposal retains its own reservation across retries, advanced in the same directory transaction that checks the drained residence. It scans published referrers after closing the lineage and reopens publication atomically with the disposal receipt. A remote fork carries the generation observed before its first storage read, so it must reprepare after any intervening retirement.
Witnessed hosted trie releases retain their exact data and marker versions through effect authorization and settlement. The physical owner and lineage reopen together.
Hosted block packs that contain only checkpoint metadata now need complete, mature release coverage for one immutable provider version, a newer published pack in the same store, and no prohibiting recovery hold or reference.
Their remaining markers settle from exact absence of the retired version.
L0-bearing and spill packs remain outside this path. Old path-only pack releases and unversioned stores still need a retirement contract.
Administrative paths still need verification across old snapshots, overlapping provider effects and live hosted dependencies.

**Start from:** `replication/src/{checkpoint_discovery,replay_floor,blocks}.rs`, `multi/src/{archive,recovery_holds,replica_retention}.rs`; models `tla/{RecoveryReplay,InstalledCheckpoint,Checkpoint,ResidenceRetirement}.tla`, `mcrl2/CheckpointDiscovery.mcrl2`.

**Open obligations:**

1. Extend protection across checkpoint discovery, replacement, and several authorities at once.
2. Protect the required log suffix through the whole replay, not only at cursor admission.
3. Account for deletion entry points outside trie collection, such as block GC and log trimming.
4. Mixed-version deployments: every version allowed to authorize trims must honour the same hold contract.
5. Remote configurations: retry behaviour and eventual cleanup of abandoned staging.

**Done when:** DUR-2's acceptance observations (lost publication reply, omitted listing entry, pending copy) pass natively with controls, and the hold contract is verified at every trim and delete site.

## W3 · Commit and replica path from admission to applied state

Goal: every successful acknowledgement names an admitted, applied batch that recovers identically after failover and replay.

**Already in place:** Verus checks replica admission, system-time assignment (including the bound by the log clock) and the first-required-offset rule; compiler controls reject forging the admission witness; TLC compares serialized, validated and blind resolution.

Enrolled Classic journals sign the exact candidate digest with retained Ed25519 keys. The writer acknowledges after quorum installation, and authenticated fresh probes can stabilize a decision on journals that lack its bytes. Replay checks the actual digest or a local original vote. The directory records portable coverage with a checkpoint and checks that coverage on startup and trim. Sealed recovery checks retained decisions before copying the selected history. The ordered provider binds its protected barrier to the reader's enrollment and invocation. The Classic successor's inherited floor, cold ordered reader attachment and full applied-answer correspondence remain open proof obligations.

**Start from:** `replication/src/{admission,admission_step,driver,appender}.rs`, `tx/src/{resolver,system_time}.rs`; Verus areas `replica_admission`, `system_time`.

**Open obligations:**

1. Check `ActiveLogletReadIndex` and its controls for payload-only acknowledgement, single-copy exposure, digest binding, exact apply, trim and disjoint activation. The model includes write-back after loss of the publisher, a silent journal and ambiguous guarded settlement with an advancing clock. Refine probe and member authentication, checkpoint authority and successor installation to the implementation, including delayed durable replies, copied or cutoff-equal votes and exclusion inferred from silence.
2. Replay injected crashes at each transition natively.
3. Prove the partition publication transitions, scan coverage, and the composition from token to effect.
4. Trace replica decoding, deferred delivery, oversized payload resolution, leader and follower apply, and recovery imports.
5. Audit every resolver constructor for committed, staged and transaction-local views.
6. Durable-to-resolved handoff across providers. DUR-1 preserves the original positions, append observation, decision clock and outcome through replay, including an unresolved guarded tail. Controls drop the explicit zero-lead bound, resample the clock on replay and count copied or late votes.

**Done when:** stale-term and premature-acknowledgement mutations fail, and the evidence ties each acknowledgement to the same applied outcome after failover.

## W4 · Temporal composition end to end

Goal: every point and range read in production agrees with the bitemporal reference, including precedence, clipping, interval endpoints, DELETE and ERASE.

**Already in place:** Lean proves the oracle, erase projection, captured history and projected ceiling. Both Rust resolvers and the visibility rectangles agree with the reference on 4,096 generated observations. Verus checks the temporal, interval and ceiling decisions and the fragment refinement.

**Start from:** `rust/crates/temporal`; `lean/{History,Trace}.lean`; Verus areas `ceiling`, `temporal_admission`, `interval_intersection`.

**Open obligations:**

1. For TIME-1, select by original position and system-time cap, break ties by original position and ordinal, and preserve same-time steps at their positional cuts.
2. Write end-to-end contracts for the resource and visitor shells around the verified decisions.
3. Prove the observable decomposition of range answers into the verified point decisions.
4. For TIME-2 and SNAP-1, prove preservation of the original winner or explicit erasure across suffix extension, compaction, recovery and nested forks.
5. Exercise the 10/20/15/40 correction, tied times across a cut and expiry during idleness.
6. Compare paired histories with independent clock samples and the same funded observation schedule. Controls use order as the clock, floor the clock only on writes, charge hidden work to public turns, reopen on cap plus await, collapse tied positions and substitute an older winner after erasure.

The [exact-cut model](tla/ExactTemporalCut.tla) and [Lean selection laws](lean/ExactTemporalCut.lean) exercise the position and cap distinction.
The model includes no-lead, position-tie, erased-winner and cap-plus-await controls, with reachability cases for late correction, ties and erasure.
The native resolver imports the ordinary SQL history-time functions proved by [Verus](verus/system_time/proof.rs).
The directory clock and the runtime publisher call the transition functions in [receiver clock](verus/receiver_clock/proof.rs).
The hosted receiver stores the observation in its ordered result row. Replica application installs the stored rows without taking a new decision reading.
The SQL token owner retains the catalog and its interpretation in the issuing native node. Durable cross-residence re-admission, fully position-addressed owned writes and derived indexes, and funded appointments remain open production refinements.

**Done when:** the production path from stored events to returned rows refines the Lean reference, and the half-open, future-ERASE and tie mutations fail in the composed check.

## W5 · Compaction refinement

Goal: eligible compaction preserves every in-scope read at a fixed basis through every publication route.

**Already in place:** Verus checks the production merge heap and base-four bucketing. Lean proves overlay routing, row occurrence coverage, and same-time normalization under surrounding history. Native replays cover mixed IID depths, recency branches, erases, and the exact-generation catalog guard.

**Start from:** `maintenance/src/{merge,job,publication}.rs` and `merge/`; Verus areas `compaction_heap`, `iid_partition`, `tiered_publication`; `lean/` compaction files; `tla/CompactionGroup.tla`.

**Open obligations:**

1. Prove each accepted output piece represents exactly its input coverage.
2. Prove the merged bitemporal events refine the original history; this needs W4's history relation.
3. Cover retry, restart, overlapping jobs, metadata depth and clipping, and every route to publication.
4. Prove the heap invariants at source level for arbitrary page counts, not only the replayed families.
5. Cohort formation, erase handling and final layout through catalog publication.
6. Equal query answers through concurrent storage operations, with full data and metadata closure.

**Done when:** COMPACT-1's observation (the same basis before, during and after a partition group's publication) holds natively, and each listed route has a mutation that fails.

## W6 · Ranked search completeness

Goal: accelerated MATCH returns the reference scan's rows, and ranked early termination preserves SQL's top results.

**Already in place:** Lean proves the abstract frontier, conjunctive-match preservation and tie retention across pages. Z3 checks the binary64 rounding premises. Verus checks the production cutoff (strict dominance of both frontiers), posting assignment, scan range and impact accumulation. The native score witness passes, and its unpadded mutation fails.

**Start from:** `rust/crates/search`; Verus areas `search_visibility`, `search_windows`, `rank_cutoff`; `smt/rank-bound.smt2`.

**Open obligations:**

1. Prove that the planner, visibility verification and index supply a complete frontier for every qualifying row.
2. Prove that each supplied maximum covers its whole source.
3. Relate the native postings iterator and persisted sidecar to the modelled complete term-window summaries.
4. Discharge the frontier proof's premise: a qualifying row's matching event is present in a pending entry or an unexpanded window.
5. Extend the cutoff to the full SQL rank key where a conservative secondary-key bound is available, and test dense ties with offset and limit.

**Done when:** FTS-1 and FTS-2's comparisons (shared files, erasures, history modes, missing files, budget fallback, exact ties, multiple MATCH predicates) run natively against the scan oracle, with a dropped-predicate and a pruned-tie mutation failing at the expected law.

## W7 · Effect-path and bypass audit

Goal: every production effect passes through its owning admission and decision.

For placed audit coverage, model provisional intake, durable scope-owned credit, exact receiver admission, terminal effect or fence, and authentic succession.
The source model [PlacedReceiverSlots](tla/PlacedReceiverSlots.tla) covers expiry with pending receiver debt and fair settlement after recovery.
Its configurations are unexecuted.
Fail controls for use of volatile admission, split terminal append, late copies after a fence, reclaim of pending credit on expiry, and reissue from a stale cut.
Trace native and hosted paths through intake, the resolver, replica decoding and apply, checkpoint installation, status and causal redemption.
A parent or exporter's offer supplies evidence only for its own boundary.

The design doc's *Production boundary and responsibilities* table names one owner per area: replica admission and apply, resolution and overlay, temporal resolution, compaction and catalog, read entitlement and GC, storage and recovery, cache, and search. For each area:

1. Enumerate every production entry point that reaches the effect, including failure, retry and ownership-change paths.
2. Record which verified decision it passes through, and what keeps that decision valid until the effect happens.
3. Where a call-path audit shows the effect can be reached without admission, add a compiler-enforced token and a compile-fail control. The replica admission witness is the existing pattern; R9 generalizes it.
4. Cache: prove generation-specific finalization, and replay paused old loads against real file leases.

**Done when:** every in-scope entry point either establishes the decision's premises or carries a recorded proof obligation. A bypass with unknown behaviour keeps its system claim open.

## W8 · Liveness and optional contracts

Goal: state and witness the progress guarantees RTDB adopts, and verify the optional contracts the owner adopted.

1. LIVE-1: give an eligible request in stable service a progress witness, plus a mutation that disables the needed progress step or fairness assumption.
2. LIVE-2: show that a retired generation becomes reclaimable after its last obligation ends; this shares a witness with W1 item 6.
3. OPT-1: exercise reply loss, repair, and same- and changed-body retries against engine-owned receipts.
   For owned publications, establish that the decision, slot witness and consequences share one original position, repair applies current custody masks, and successors require authenticated exclusion and transfer before accepting inherited work.
   Controls remove the fence at restore or use SQL metadata expiry as evidence of exclusion.
   Application equality and schedule coverage refine their own owners over this transaction order.
4. OPT-2 and OPT-3: verify the eraser (R7) against the storage inventory: logs, checkpoints, object versions, caches, replicas, exports, backups and keys.

**Done when:** each adopted claim has its witness and its mutation.

## W9 · Combined evidence and readiness matrix

Goal: one reproducible claim per requirement, on one committed tree.

1. Re-run all proof areas, controls, model configurations and native traces together on one exact commit with `verification/run.py`, retaining tool identities, commands, input hashes and logs.
2. Publish a readiness matrix by requirement, with separate verdicts for model validity, acceptance expressiveness, minimality, production decisions, bypass coverage and composed behaviour.
3. State each trusted premise beside the claim that uses it, for example bounded clock drift and request duration.
4. Give each minimality claim a witness for its chosen weakening.
5. Link later changes to an accepted law, proof boundary or provider contract back to the affected requirement.

**Done when:** the matrix covers every requirement ID in the design doc, and each verdict cites evidence from that one run.

## Structural redesign (R3–R9)

Increments R1 (system time bounded by the log, `d85286ba2`), R2 (engine-owned retry receipts, `85a868c07`) and R3 (epoch-qualified physical names, `ae7b66e10`) are on main; the remaining six replace mechanisms rather than patch them, and supersede the matching obligations in W1, W2 and W8.
Each has a design and handoff note in [`verification/redesign/`](redesign/README.md).
The target contract and status are in `dev/doc/structural-redesign.adoc`.
The later serving review adds typed placed receipts, on-demand freshness checks, and a recoverability boundary for CDC.
See [Authority boundaries](redesign/authority-boundaries.md).

Logkeeper quorum admission binds physical journals from placement through recovery, read confirmation and retirement.
The [physical journal quorum model](tla/PhysicalJournalQuorum.tla) checks 4,500 states and distinguishes endpoint counting, trusting an opening route and confirming with the surviving members after a route change.
This evidence covers vote identity and intersection for one configured ensemble.

| ID | Increment | Depends on | Exit test |
| --- | --- | --- | --- |
| [R4](redesign/R4-residence-ledger.md) | Residence ledger (Multi directory record; classic conditional-write object) with renewable holds and a collection floor; delete `garbage_lifetime`, the fixed lease and collector wait, the reaper grace, wall-clock release stamps and the unbounded in-process registry | Peer model `verification/tla/ResidenceRetirement.tla` | A follower behind the log tip serves a held generation; the collector never deletes below a live hold |
| [R5](redesign/R5-recovery-roots.md) | Recovery roots as holds; L0 becomes collectable; compactor reset starts a new epoch from a root | R4 | Crash during compaction and reset recovers from a root plus the log suffix |
| [R6](redesign/R6-sealing.md) | Seal every durable unit under a publication-bound key slot in its custody domain | — | No recoverable source key survives revocation; independent copies use independent key slots |
| [R7](redesign/R7-eraser.md) | Separate horizon collection from privacy erasure; publish readable successors and arm revocation before committing | R5, R6 | A held view survives ordinary collection; privacy revocation meets its committed deadline while clean roots and independent exports remain readable |
| [R8](redesign/R8-metadata-retention.md) | Transaction metadata and retry receipts retained only for the receipt window, removed physically | R7 | Expired receipts and metadata are absent from every storage class |
| [R9](redesign/R9-capabilities-and-composition.md) | Capabilities for storage reads, provider deletes and cached pages; one composed residence model checked against emitted traces | Grows with R3–R8 | Every mutation named in the redesign fails under both TLC and the trace checker |

## Blocked: live provider qualification

Real-provider qualification needs cloud resources the owner must authorize, and the earlier AWS infrastructure has been torn down, so no agent should start it unprompted.
A mocked provider run and a real provider qualification establish different observations; the ledger keeps them separate.

- **Azure:** behaviour under load, remediation of a terminally failed copy destination, and eventual cleanup of abandoned staging. Only offline diagnostics against the production adapter exist today.
- **S3, GCS, Kafka:** partition and failover limits for each provider combination a release claims.
- **What it needs:** an owner request naming the providers and accounts, with each run recording exact source and provider identities, isolating its resources and cleaning them up.
