# Ordered index verification

The reference answer resolves the original captured event stream at the requested system and valid points, filters its rows, sorts by the complete order key, and takes the requested page.
The proof sources in this directory decompose that specification.
The checker results and the remaining production obligations appear below.

| Property | Proof source | Production boundary |
| --- | --- | --- |
| Temporal projection and exact membership | `Temporal.lean` specifies point lookup against independent event interpretation and a single visible span per IID. `Buckets.lean` proves exact coverage for a recursive dyadic decomposition and for any left-to-right tiling. | The projection uses the admitted temporal resolver. The composition uses exact tiling and checked timestamp and ceiling decisions. |
| Signed timestamps, unbounded end and bucket lookup | `domain.rs` checks production timestamp conversion, aligned bucket lookup and the maximal aligned step used by the bucket iterator. | The iterator supplies a complete aligned tiling. Each production step preserves the range and advances to the maximal aligned boundary. |
| Ordered seek, eligibility and pagination | `SeekRange.lean` states the monotone seek partition and fence pruning laws. `seek.rs` includes the production binary search and directed slot lookup. `OrderedRead.lean` states ordered merge, filtering before limit, and keyset continuation. | The native path interprets Arrow comparisons and filters before limiting the merged streams. The paired run checks that path at labelled forward and reverse reads. |
| Replay and fallback | `Recovery.lean` states equivalence for complete checkpoints and ordered tails. It also states exactly when the event count completes coverage. `Reachability.lean` states layout identity across a checkpoint split. The `verus/ordered_replay` area includes the local journal replay block and completion predicates. | The adopted L0 inventory supplies flushed identities and lengths. Exact immutable bytes and the live segments must come from the captured snapshot. The production maintenance shell owns fallback order and source admission. |
| Publication and reclamation | `Checkpoint.tla` models a captured reader, two workers, immutable objects, publication, collection, fallback, and crash. The two control configurations remove checked source identity and complete upload. `Reachability.lean` states how a stale mark preserves later reachable pages under the per-block sharing rule. | The missing-root regression checks stable addresses through repair. The protocol uses immutable publication and tracked replica permits. The installed layout and codec checks belong to the production bindings below. |
| Erasure | `Temporal.lean` states how erase cuts the captured IID's prior history, including historical system-time reads. | Capture-based replay scrubs before publishing. `ErasureCleanup.tla` checks the cleanup obligation, and `ScopeRetirement.tla` checks the provider admission, replica permit, settlement, and reclamation protocol. |
| Bijection page witnesses | `OrderedRead.lean` proves how agreement over the consumed ordered prefix preserves the page. `DistinctPage.lean` proves the ordered distinct candidate prefix, clearance before deduplication, and the preservation of global prefix keys under each local limit. | The paired fixture binds the per-world SQL results and custody streams to acquisition and revalidation. Complete SQL prefixes and authentic custody inputs are refinement premises. |

`CheckpointCountOnly.cfg` should produce a wrong answer when a replay substitutes an event with a duplicate while preserving the count.
`CheckpointEarlyPublish.cfg` should expose a reachable manifest that references a missing object.
The model represents a page with its complete source prefix and abstracts page bytes, hash validation, and physical snapshots.
An accepted model run establishes its invariants within the configured bounds.

### Runtime-installed format bindings

The runtime attachment owns the profile and passes it to the exact snapshot store. [Serving attachment](../../rust/crates/multi/src/journal/hosted/storage.rs) · [Exact snapshot](../../rust/crates/multi/src/journal/hosted/storage/snapshot.rs)
Definition addressing binds that profile to a separate layout identity at the same definition generation. [Definition](../../rust/crates/node/src/ordered_index/mod.rs)
Checkpoint admission and publication own the selected manifest's format, checksum, frontier, source count, and revision root. [Checkpoint](../../rust/crates/node/src/ordered_index/checkpoint.rs)
Tree page opening owns the bounded fetch, digest, reachable page formats, and validation of each opened page against its reference. [Tree](../../rust/crates/node/src/ordered_index/tree.rs)
Maintenance rebuilds from captured inputs, including pinned source versions for eligible projected reads, before publishing the replacement. [Maintenance](../../rust/crates/node/src/ordered_index/maintenance.rs)
The current models assume a correctly bound layout and page interpretation.
Composing the installed attachments, the Arrow decoder and reservation lifetimes, and exact provider recovery with those premises remains an open refinement.
Source fixtures compare shared and installed checkpoint reads in both materialized and projected form and exercise format refusal after admission and digest validation. [Contract tests](../../rust/crates/node/src/ordered_index/checkpoint.rs)

### Repair and cleanup

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
The hosted generation-retirement evidence below covers the later storage implementation.

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

Verus 0.2026.11.20.9da409b checked six obligations in `scope.rs`, which
includes the production directory's generation decisions. The checks prove
that the closure floor cannot move backward, a stale retirement preserves the
open suffix, and the replacement remains admissible at the exact boundary.
Changing the production admission from inclusive to strict failed its
postcondition.

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
from a neighboring generation. A provider fault regression also exercised the
production journal with a superseded writer. The reaper kept the scope while
its serving region was active, then confirmed abort and reclaimed its versions.
An attempted completion with the original upload ID failed after reclamation.
These tests cover the local provider
implementation and the selected failure schedules. The model assumes the
provider's confirmed abort and inventory contracts. Primary L0 files are a
separate custody boundary.

### Retiring dropped definitions

The captured catalog retains the definition fence after the last physical
scope is removed. The reaper discovers old definitions through provider
version and multipart inventories, including definitions that were already
dropped when generation tracking began. The hosted versioned-provider
regression verifies both erase and drop. Its node queries use the direct
catalog scan and passed with default Tokio stack sizing. The directory unit
test reclaimed an unregistered dropped definition and rejected admission
before and after reclamation.

TLC explored 16 reachable states in both `CatalogRetirement.cfg` and
`CatalogRetirementLiveness.cfg`. Ignoring previous versions left an old object
after acknowledgement. Discarding the durable catalog fence allowed a stale
capture to reopen the removed definition. The positive configurations
preserved its replacement.

## Temporal projection composition

Lean 4.35.0 checked that coalescing adjacent spans with an equal tuple
preserves every point. `bucket_postings_refine_table` composes exact bucket
tiling with the temporal replay laws. At one captured basis and valid point,
the postings for a single IID carry the authoritative value and contribute
at most one matching tuple. The exact tiling hypothesis is supplied by the
bucket laws, with Verus checking the signed timestamp mapping and maximal
aligned production step.

The core `History.lean` and `Ceiling.lean` checks also passed.
Verus checked 18 obligations in the production ceiling harness covering its
update, clipping window, and half-open cell geometry. The ordered projection
opens the same admitted history resolver used by the engine's scan.

## Page and read witness composition

Lean 4.35.0 checked `distinct_runs_refine_membership` and
`ordered_route_preserves_page_and_witness` with warnings promoted to errors.
Given the same authorized candidate membership and the exact captured source,
the ordered index and scan agree on the distinct stream, resolved rows,
lookahead, and keys inspected for registration. The axiom audit reported
only `propext`, `Classical.choice`, and `Quot.sound` across these laws.

The paired test reran the published RTDB engine at `1a6eceb63` with the
published Bijection integration at `96b534c7c49ed01d57a307ebee42fadb083ae601`.
The trace recorded both native seek directions for labelled pages, then
authoritative scans with the ordered index dropped. The fixture required equal
rows, continuation, the authenticated consumed predicate, and successful
revalidation at the same observation. It runs against a dedicated local
RTDB endpoint. The theorem assumes exact candidate membership and replay
coverage, whose production bindings are exercised by these runtime tests.

On newer Bijection main source `ebaa9bbe2f49afbb3ad6f2838affbdcd007a38a2`,
Verus checked 12 candidate-window and seven overlay-admission obligations
from the production files. Lean checked `RtdbCandidatePrefixes` and
`RtdbOverlayHistory`. Together these establish the retained first distinct
union prefix and the effect of keeping the protected history and clear suffix
through the newest clear reset. The proofs require authentic source prefixes,
ancestry, and operation decoding.

The newer paired runtime build used Bijection source `29cada504ef` with the
mask projection alias fix published at `8585a3489207d430d443f54d9e8bb9746a5a3906`.
The standalone RTDB binary was built from `d2d6945cf`. The local PgWire run
observed `scan_ordered_seek` for both labelled directions before the drop,
then the corresponding plain scans. Rows, continuation, and the authenticated
read witness matched and both registrations revalidated. The alias fix keeps
the positional mask decoder intact and gives each joined column a unique name.

The focused paged-overlay regression passed on Bijection source
`d00682f567c`, published as `c259639529869f6fb98ced7184fe7c896d4388d0`.
It uses a parent with 146 candidate keys and a child with a long run of
deletions and one inherited merge. The test checks the two remaining rows,
continuation, and exhaustion in both directions. The disk-backed PgWire
trace recorded native seeks while traversing the deleted candidates.

The broader WorldStore conformance reruns on the newer Bijection source
hit fixture lease deadlines. The last run used in-memory RTDB storage
with tracing disabled. It reached the second label-order pass and stopped
when the 300 ms lease in
`a_dropped_or_lapsed_world_refuses_the_whole_commit` expired before its
fixture fork committed. The focused registered-page and paged-overlay
regressions passed against the disk-backed PgWire engine.

## Execution

Use Lean 4.35.0, Bijection's pinned Verus toolchain, and TLC from the authorized proof environment.
Write generated proof products and TLC state under an ignored task target directory.
Compile `Temporal.lean`, `Buckets.lean`, `DistinctPage.lean`, and `OrderedRead.lean` into the module search path before checking `Recovery.lean`, then run `Axioms.lean` against the compiled modules.
Run Verus on `domain.rs`, `seek.rs`, and `scope.rs` so the included paths resolve to the production sources used by RTDB.
Run TLC against each configuration, retaining the source identity, tool version, and each diagnostic trace.
