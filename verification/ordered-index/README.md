# Ordered index verification

The reference answer resolves the original captured event stream at the requested system and valid points, filters its rows, sorts by the complete order key, and takes the requested page.
The proof sources in this directory decompose that specification.
The checker results and the remaining production obligations appear below.

| Property | Proof source | Production refinement still required |
| --- | --- | --- |
| Temporal projection and exact membership | `Temporal.lean` specifies point lookup against independent event interpretation and a single visible span per IID. `Buckets.lean` proves exact coverage for a recursive dyadic decomposition and for any left-to-right tiling. | Connect RTDB's newest-first temporal resolver, coalescing, and greedy bucket iterator to the model. |
| Signed timestamps, unbounded end and bucket lookup | `domain.rs` checks production timestamp conversion, aligned bucket lookup and the maximal aligned step used by the bucket iterator. | Relate the iterator state and termination to the complete tiling in `Buckets.lean`. |
| Ordered seek, eligibility and pagination | `SeekRange.lean` states the monotone seek partition and fence pruning laws. `seek.rs` includes the production binary search and directed slot lookup. `OrderedRead.lean` states ordered merge, filtering before limit, and keyset continuation. | Connect Arrow values and SQL comparisons to the order and filters, and the tree cursor and heap to the ordered streams. |
| Replay and fallback | `Recovery.lean` states equivalence for complete checkpoints and ordered tails. It also states exactly when the event count completes coverage. `Reachability.lean` states layout identity across a checkpoint split. | The adopted L0 inventory supplies the flushed input identities and lengths. Connect live segment offsets and the immutable file bytes to the exact capture. |
| Publication and reclamation | `Checkpoint.tla` models a captured reader, two workers, immutable objects, publication, collection, fallback, and crash. The two control configurations remove checked source identity and complete upload. `Reachability.lean` states how a stale mark preserves later reachable pages under the per-block sharing rule. | Prove the sharing rule for `Tree::persist` and `store_roots`, and connect reader retention, leadership and immutable writes to the protocol. |
| Erasure | `Temporal.lean` states how erase cuts the captured IID's prior history, including historical system-time reads. | Connect revision-directory scrubbing and physical collection across object and node caches to the model. `ErasureCleanup.tla` models the durable cleanup obligation under a single fail-stop owner. `ErasureRelease.tla` checks the requirement for fenced writes and replica eviction. |
| Bijection page witnesses | `OrderedRead.lean` proves how agreement over the consumed ordered prefix preserves the page. `DistinctPage.lean` proves the ordered distinct candidate prefix, clearance before deduplication, and the preservation of global prefix keys under each local limit. | Bind the per-world SQL results and protected custody streams to the candidate sets used at acquisition and revalidation. |

`CheckpointCountOnly.cfg` should produce a wrong answer when a replay substitutes an event with a duplicate while preserving the count.
`CheckpointEarlyPublish.cfg` should expose a reachable manifest that references a missing object.
The model represents a page with its complete source prefix and abstracts page bytes, hash validation, and physical snapshots.
An accepted model run establishes its invariants within the configured bounds.

The sharing rule relies on page addresses deriving their generation from the source block that first writes them.
If a full rebuild stamps earlier pages with its requested final frontier, it can produce different bytes for an occupied immutable manifest slot.
The two-block recovery regression in `rust/crates/node/tests/native.rs` captures a manifest, removes a page, reopens, and requires the rebuilt manifest to retain its original bytes.

The cleanup marker is written before storing a scrubbed archive or starting a full replay repair.
A later checkpoint sweeps pending markers within its frontier, and clears them after deleting the unreachable pages.
`ErasureCleanupForgetOnAdvance.cfg` removes that carry-forward rule.
A physical erasure theorem across overlapping maintenance owners still needs a fence on page writes and a model of each replica's cached copies.

## Evidence boundary

Bijection's [cutover proof evidence](https://github.com/bijectionhq/bijection/blob/2fa0572878a0b812a38e33332818bbb687e8d8c0/verification/cutover-kernel-laws.md) records separate Lean, TLC and Verus checks on its existing kernel cutover.
The applicable page law is `hidden_write_preserves_page_and_cursor`, over Bijection's projected, ordered observation.
The native adapter obtains its candidates through the WorldStore scan and does world overlay, label filtering, custody checks and validation there.
The integration indexes `(world, key, label)` on the clear write relations.
The single-text-column `SELECT DISTINCT` plan counts after clearance and run
deduplication. Bijection merges those clear prefixes with protected candidates
and still resolves the overlay and authorizes its output under current custody.
Each RTDB request uses the same temporal basis and range as the WorldStore
scan. The final production obligations cover the SQL binding, overlay, and
release together.

The cleanup model assumes one fail-stop maintenance owner. Ordered-index
page reads bypass the durable disk cache, and replay holds its L0 input in
charged memory. Hosted checkpoint archives, storage forks, and relocation leave derived
index pages with their original source. Decoded pages belong to the active read
or maintenance operation. The replica does not keep an idle ordered-index
page cache or a live replay tip.
In production, a worker's term cancellation and an object-store `put` are separate operations.
An end-to-end physical erasure argument must cover an already-admitted write by a superseded owner and stale contents on every replica.
A proof of the cleanup marker under the fail-stop model establishes one step of that argument.

## Checked evidence

The owner-authorized MacBook Pro M1 run through Rex checked commit
`3b3200da657a47f21cbf6442911dd9b34e15611f` on 2026-11-21. The five checkpoint and
cleanup configurations ran at `be62820b550acba4917d56fafd0255dc441f1206`. Their
input files are identical at the checked commit.

- Lean 4.35.0 checked all six law modules with warnings promoted to errors.
  `Axioms.lean` audited 13 exported theorems. Their dependencies are drawn from
  `propext`, `Classical.choice`, and `Quot.sound`.
- Verus 0.2026.09.13.671956e with Rust 1.98.1 checked 10 functions in the domain
  harness and 6 in the seek harness. The harnesses include the production Rust
  files directly. Replacing the open upper bound with `u64::MAX` and using a
  forward slot for reverse seeks each failed the corresponding production
  postcondition.
- TLC 2.19 checked the following configurations. State counts for controls
  reflect exploration through the first reported failure.

| Configuration | Distinct states | Result |
| --- | ---: | --- |
| `Checkpoint` | 8,812,741 | All invariants held through depth 21 |
| `CheckpointCountOnly` | 273,439 | `AnswersExact` failed after replay substituted a duplicate for the second event |
| `CheckpointEarlyPublish` | 180 | `NewestReachable` failed after publication with missing objects |
| `ErasureCleanup` | 18 | `NoLostCleanup` held |
| `ErasureCleanupForgetOnAdvance` | 4 | `NoLostCleanup` failed after erase, publication, and advancement |
| `ErasureRelease` | 14 | `NoResurrectedObject` and `NoRetainedReplica` held |
| `ErasureReleaseSuperseded` | 15 | `NoResurrectedObject` failed when a write admitted before the fence completed after deletion and acknowledgement |
| `ErasureReleaseReplica` | 15 | `NoRetainedReplica` failed when acknowledgement followed deletion while a replica still held the page |

The release model's fence makes an already-admitted write from the old term
incapable of storing bytes after the term advances. Its acknowledgement also
requires eviction on the modeled replica. Production must establish these
conditions across its object store and replicas to claim physical completion.
The proof run checked the models and included Rust decision functions.

## Runtime integration

The native `ordered` filter passed eight cases on the merged engine with ordered distinct scans enabled.
The cases cover idempotent declaration, ordered distinct limits, both scan directions, exact temporal reads, reopen, rebuilding a damaged root, and use of the adopted L0 inventory across primary compaction.
All 18 trie catalog cases passed, as did the fresh-read disk-cache regression.
Lean 4.35.0 checked seven law modules and audited 18 theorems.
Verus 0.2026.11.05.31e44e6 on the MacBook Pro M1 checked ten domain functions and six seek functions using the included production sources.
Verus 0.2026.11.20.9da409b checked 20 obligations after adding the production bucket step.
Changing that step to choose a smaller bucket failed its maximal-width assertion.
TLC reran the erasure models and reproduced the three expected failures in their weakened controls.

The local Bijection PgWire fixture `registered_pages_agree_with_and_without_ordered_index` exercised hosted WorldStore authority and fixture custody against a local RTDB build using the traced query implementation recorded at `49bd0c804845235ab0721c95dda596b5ee4c68b6`.
The `scan_ordered_seek` attribute was present on both labelled ascending and descending reads before the index was dropped, and absent from the corresponding scans after the drop.
At one basis and observation, both paths returned the expected rows, continuation and authenticated consumed predicate, and both registrations revalidated.
On the published Bijection source `96b534c7c49ed01d57a307ebee42fadb083ae601`, the paired fixture and the full RTDB WorldStore conformance fixture both passed against the local hosted engine.
Hosted provider upload settlement and reader eviction remain production proof obligations.

On the hosted integration source `671274af9f8e92b912a2958f1b8ea75a8a602a75`,
Lean 4.35.0 checked seven law modules and audited 18 theorems through Rex on the M1.
The added laws cover distinct candidate order, clearance before duplicate
elimination, preservation under per-source limits, and replay input provenance
across primary compaction.

On the hosted continuation the M1 checked the RTDB domain and seek harnesses
again, with 10 and 6 verified functions respectively at
`d273ff8fc74c26d8a93279debcb206e72c2315de`.
On Bijection source `3811208c5b957bd06b9b8efaa55bc899611d582f`, the same
Verus toolchain checked eight registered-page functions and four protected
ingress functions. The joint controls failed
`a_registered_scan_has_no_extra_candidate` and
`custody_owned_write_is_sealed` at their expected postconditions. The
included Bijection decision sources are unchanged in the companion clearance
update at `aa7c7fd086423689a772ccd8fd6b75a1674a6cb7`.

Physical erasure of the remaining provider objects must bind garbage release,
superseded publication settlement, and active read scopes to the provider's
completion evidence.

## Generation retirement model

`ScopeRetirement.tla` models closing a generation to new uploads and reads, while already-admitted operations retain their original generation.
It permits a provider upload admitted before closure to finish after closure.
Erasure acknowledgement requires provider settlement and release of reader copies.
A replacement generation carries replayed content only after the model's scrub condition is satisfied.

TLC 2026.11.19.180144 explored all 540 reachable states with two publishers
and two replica permits. The model allows an upload admitted before closure
to finish afterward and requires the scrubbed replacement to be published
before closing the earlier scope.

| Control | First invariant violation |
| --- | --- |
| `SettleUploads = FALSE` | A retired object reappeared after acknowledgement. |
| `DrainReaders = FALSE` | A retired scope was acknowledged while a replica still held a page. |
| `ScrubReplay = FALSE` | The replacement scope carried erased content. |

`ScopeRetirementLiveness.cfg` explored the same 540 states. After the
replacement is published, acknowledgement eventually occurs under weak
fairness for closing, settlement, reader release, deletion, and acknowledgement.

Replay on consolidated RTDB discovers erase cuts from adopted L0 files before
writing any replacement page. `Recovery.lean` checks
`scrubbed_replay_preserves_points`: for one IID, filtering out strictly
earlier events preserves the point interpretation and retains source order
within the erase revision. Lean 4.35.0 checked the theorem with warnings
promoted to errors and reported dependencies on `propext` and `Quot.sound`.

Every checkpoint now names one page scope, determined by the last source
block containing an erase. Pages retain their original source-block
addresses inside that scope. Collection releases earlier scopes only after
the replacement checkpoint is durable, and leaves pages staged in later
scopes alone. Four ordered-index unit tests and eight native regressions
passed on the consolidated engine. The regressions cover exact repair when
an old dirty address cannot be reused, a same-revision erase and reinsertion,
and preservation of an unpublished later scope. The PgWire declaration and
keyset regression also passed.

The hosted directory records replica permits and provider publications before
page I/O. A watched replica shares its permit across concurrent operations
and releases it after its decoded pages and provider I/O drain. The directory
rejects new upload admissions immediately at closure. The reaper discovers old
scope prefixes through the version and multipart inventories, settles exact
journaled uploads, and purges all versions before removing the retirement
obligation. The local regression retained a generation after a read caller
was cancelled, then reclaimed it after I/O completed. It also exercised a
preexisting unregistered prefix and an outstanding storage region from
before index permits were tracked.

The hosted PgWire regression exercised erase, replica permits, the production
reaper, and the retained page against a version-enabled MinIO instance.
A separate S3 runtime regression checked noncurrent versions, a delete marker,
a prefix containing only a multipart upload, exact upload abort, and isolation
from a neighboring generation. These tests cover the local provider
implementation and the selected failure schedules. The model assumes the
provider's confirmed abort and inventory contracts. Primary L0 files are a
separate custody boundary.

## Execution

Use Lean 4.35.0, Bijection's pinned Verus toolchain, and TLC from the authorized proof environment.
Write generated proof products and TLC state under an ignored task target directory.
Run each Lean module and compile `Temporal.lean` into the module search path before checking `Recovery.lean`, then run `Axioms.lean` against the compiled modules.
Run Verus on `domain.rs` and `seek.rs` in this directory so the included paths resolve to the source used by RTDB.
Run TLC against each configuration, retaining the source identity, tool version, and each diagnostic trace.
