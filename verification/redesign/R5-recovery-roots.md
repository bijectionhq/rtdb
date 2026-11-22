# R5 · Recovery roots as holds; collectable L0; reset from a root

## Goal

Make recovery depend on an explicit, held root instead of on files that are simply never deleted.
After R5, L0 files are ordinary garbage once compaction supersedes them (as in XTDB), recovery and compactor reset start from a recovery root plus the log suffix, and nothing in the store is a permanent plaintext source of truth.

## Converged design

- **Recovery root.** A published checkpoint whose manifest names its complete dependency closure (table blocks, trie data and metadata, artefacts), with exact publication identities.
  The root also records the first required replica position, the history and resume token at its cut, and the custody identities needed to open it.
  Recovery acquires the root and suffix hold as one admission before replay starts.
- **Roots are holds.** A root is a ledger hold (R4) without expiry. It is released only when a newer root is complete, and completeness is checked through its manifest. The collection floor (R4) therefore never passes a live root's closure.
- **L0 becomes collectable.** The collector's L0 refusal goes, and the catalog stops retaining L0 identities after primary compaction supersedes them.
- **Reset from a root.** A compactor reset rebuilds from a root rather than from retained L0 files, and starts a new naming epoch (the epoch already exists from R3).
- **Log suffix.** The replica log may be trimmed below the oldest live root's first required record.
  Recovery holds keep that suffix through replay.
  Upstream acknowledgement may use an applied token covered by a durable root and a protected committed suffix.
  See [the upstream acknowledgement boundary](authority-boundaries.md#recovery-roots-and-upstream-acknowledgement).

## Invariants

- **DUR-2.** At least one live root has a complete, readable closure and its required log suffix, under the supported fault model.
- **E2 (shared with R7).** Every live root's closure consists only of readable units (no destroyed key, no collected file).

## What R5 deletes

- The L0 guard in `TrieCollector::collect_table` (`rust/crates/maintenance/src/gc.rs`, "L0 recovery files cannot be garbage collected").
- Retained L0 identities in the trie catalog (`dev/doc/trie-cat.allium` line 9 and the code behind it).
- `reset_to_l0` in `rust/crates/index/src/partition.rs` and `rust/crates/index/src/trie_catalog.rs`, and the L0 replacement plan in `Node::prepare_compactor_reset` (`rust/crates/node/src/operations.rs`), replaced by a reset from a root.
- The `rtdb.replication/invalid-trie-removal` restriction on L0 deletions in the replication driver.

## State on main (2026-11-21)

- Discovery already advances to a newer advertised root when collection removes the selected root or a table block during startup, and recovery admits its replica cursor before installing a checkpoint (`rust/crates/replication/src/checkpoint_discovery.rs`, `replay_floor.rs`).
- Ordinary hosted recovery validates the directory's admitted block and replica boundary against the stored catalog before loading table metadata.
  Successor selection and replay retries preserve the journal and advance both coordinates.
  This binds the recovery coordinate. The manifest for its data and artefact dependencies remains open.
- New Rust checkpoint roots bind every registered table block by its stored SHA-256 digest before publication.
  Recovery checks the map's domain against the OID registry and checks the bytes before decoding.
  This authenticates the table metadata graph relative to the loaded root. The root's provider identity and the trie and artefact publications still need their own evidence.
- Multi fresh storage probes for checkpoint contents resolve through the durable pack or the provider's loose object.
  The writer probes the recovered current checkpoint as well as historical checkpoints before issuing a new confirmation.
  The bounded comparison checks storage presence at the probe and identifies the need for a hold through submission.
- Versioned S3 pack metadata, index and member slices carry the provider identity of the response that supplied them.
  A warm pack lookup refuses a different publication at the same physical path, and a fork copy requires a fresh index at the destination.
  Hosted directory publication records the exact logical root digest with its cut.
  Ordinary cold recovery verifies that receipt before table metadata, admits metadata and the replay floor together, and hands suffix protection over to its cursor.
  A successor must be admitted separately by the directory.
  Recorded physical provider versions and complete transitive dependencies are still needed for release certification.
- New archive and independent fork copies require the replay inventory derived from the checked table blocks.
  It contains live and nascent payload files, retained L0 payload files and the usable artefacts named by the catalog.
  The destination publishes after copying every required path. Older receipt and stage formats retain their original inventory scope.
  This evidence covers copy-time completeness.
  Hosted archive capture also holds its source root and table metadata through provider drain.
  Uninterrupted coverage can transfer to a retry on the same older checkpoint. Completed archives can be reopened independently.
  A separate release authority must keep the exact source payload publications available through the copy.
- The trie collector protects the installed checkpoint's live and nascent tries within one partition authority.
- Multi has recovery holds for the replica suffix (`rust/crates/multi/src/recovery_holds.rs`, peer commit `e298879a0`) and checkpoint-metadata holds (`rust/crates/multi/src/checkpoint_retention.rs`).
- Models: `verification/tla/{InstalledCheckpoint,RecoveryReplay,Checkpoint,ResidenceRetirement}.tla`, `verification/mcrl2/CheckpointDiscovery.mcrl2`, the AutoHyper two-run check, and the Lean checkpoint-metadata laws.
- Compactor reset names outputs in a new epoch (R3, `ae7b66e10`), so rebuilding from a root cannot collide with pre-reset names.

## Handoff notes

1. Land after R4: roots are holds, and the floor rule is what makes L0 collection safe.
2. Unify the Multi recovery and checkpoint-metadata holds with the R4 ledger instead of keeping a third retention mechanism.
3. Define the root manifest once (dependency closure plus first required replica record) and make discovery select only complete roots.
4. Remove the L0 guard and the retained identities in the same change that makes recovery root-based, so no window exists where L0 is collectable but still relied on.
5. Re-run the checkpoint replays and their controls (`verify_checkpoint_replay` in `verification/run.py`); the runner's node-faults test count must be updated if tests are added.

## Tests and proofs

- **Native:** crash during compaction and during reset, then recover from a root plus the log suffix; collect superseded L0 files and recover again; trim the log below the root and recover.
- **TLA+ / Apalache:** extend `InstalledCheckpoint.tla` and `ResidenceRetirement.tla` with roots as holds and L0 collection.
- **Mutations that must fail:** releasing a root before its successor is complete; collecting an L0 file still in a live root's closure; trimming the log above a live root's first required record.

## Source retention through copy completion

A verified copy has two independent obligations.
The destination must contain the complete inventory of the admitted cut before it can be published.
The source must keep the publications needed by the copy available until the admitted provider work has drained.
Native copy tests check destination completeness and the source metadata admission and release path.
The first source admission uses the current published checkpoint. A retry on an older root requires an existing live metadata floor or a verified archive in independent custody.
The metadata argument relies on monotone publication within one residence. A stale reaper observation predating the first hold cannot authorize a block at or above its observed head.

The source-handoff model compares two retention rules under concurrent checkpoint adoption and retirement.
A residence-wide hold prevents source collection for the whole attempt and can delay reclamation of unrelated publications.
An exact hold protects the admitted root and its dependency publications, including the complete physical pack for a member that still depends on it.
It requires an authenticated observation that binds the selected cut, directory custody and storage identities.
In both designs the copied destination takes its own custody before the source hold is released.

The model has two roots, a shared object, an inherited pack, a delayed collector and a provider read completing after cancellation.
It separates these properties:

- **Admission safety.** An accepted hold names the exact root and source residence that its requests use.
- **Retirement safety.** Collection cannot authorize removal of any publication required by an outstanding hold.
- **Handoff safety.** Only the durable complete destination may replace the source obligation for a successful copy.
- **Conditional progress.** Under a stable source and fair delivery, an admitted copy can complete. A canceled or finished attempt releases after its provider work drains.
- **Reclamation progress.** With no remaining references and fair collection, an unrelated retired publication can be reclaimed while exact holds are in flight.

Controls hold the wrong cut, authorize an object in the current root, ignore an authorized removal, omit the inherited pack from the hold, release before provider drain, publish a staged destination or suppress the handoff altogether.
The Lean model uses arbitrary sets of physical publications and proves that the admission, authorization, read and drain transitions preserve outstanding dependencies through any finite schedule.
The exact variant permits reclamation of an unrelated publication during an admitted read. The residence-wide variant blocks that reclamation.
The bounded liveness property assumes weak fairness for provider delivery, destination acceptance, handoff and cancellation drain.
Archive and independent-fork preparation now resolve logical members to the physical versions observed by their exact reads, including inherited packs and repair ciphertext.
They keep the logical path and planned length with the version, apply root and table digest checks, and reject if a body's observation differs.
An unversioned member relies on the immutable-object contract.
This observation is local to preparation. Holding it across reclaim requires a durable admission against the physical owner and a shared ordering with authorization at every corresponding collection entry point.
A production proof should cover the hold and retirement decisions, with directory compare-and-replace and provider version semantics stated as premises.

### Admission across physical owners

An inherited pack belongs to the authority responsible for the physical object reported in the body response.
A repair-backed member belongs to the authority responsible for the archive ciphertext.
In both cases the serving deployment's metadata hold can coexist with a separate physical owner.

A candidate composition acquires protection from each physical owner before it admits a copy.
Each owner orders admission against its already-authorized removals.
The copy begins only after all ownership groups have accepted, and it rereads the members against the admitted plan.
An incomplete acquisition releases the groups it already holds after their provider work drains.
This composition needs an ordering within each owner and an all-owners admission check at handoff.
The bounded comparison with atomic directory-wide admission preserves safety in both forms.
Separate admission reaches a partial acquisition, and it reaches cleanup after the second owner refuses.
The model checks cancellation and delayed removal. Concurrent owner transfer remains a separate obligation.

The production evidence needs exact storage responses at the admission boundary, a durable namespace and owner binding for each group, and proof that provider erasure requires the same authorization used by the admission test.
The final claim also needs a rule for providers that expose no stable object version.

### Repair custody and ordinary remote collection

Repair-backed serving and recapture name their repair archive in the directory admission.
For recapture the destination remains separately recorded, including when its provider still owes settlement.
A transfer takes a read hold from the original archive owner before materializing repair bytes into the separately held destination.
Provider-backed repair transfer uses the worker's hosted task witness for recovery of abandoned admissions.
On local serving storage the existing maintenance lock orders ordinary collection against copies and is retained by the admitted provider work.

The ordinary remote collector still works from a residence list and releases the visible object name.
Its exact-version design needs a durable authorization naming the provider copy before submitting a physical effect.
Task termination alone establishes that the task cannot issue another request. A mutation accepted by the provider can still complete.
The bounded settlement comparison preserves replacement availability for exact-version removal and for path removal that excludes publication and admission until settlement.
It reaches late completion with the replacement held in the exact variant.

The S3 adapter binds the removal to its configured bucket, endpoint, region and physical key, and requires an immutable provider version.
Every attempt starts with a complete version inventory and refuses to remove the current data version when another data version exists behind it.
If the target is present and eligible, the request sends its immutable version identifier and then reads a fresh inventory for evidence of its absence.
The evidence covers that exact key and version, with pagination checked before absence is accepted.
This relies on the provider honoring exact deletion, supplying complete version listings and never reusing an immutable version identity.
The retry model reaches settlement while a redundant exact request remains outstanding. Its later effect preserves other versions.
TLC explored 268 states in the bounded retry model and found a counterexample when the current-version guard was removed.
The model assumes concurrent publication adds a newer version.
The integration proof still needs to connect inherited references and physical-owner admissions to that provider action.

### Shared fork publication and referrer inventories

Shared forks publish their child record, name and an advanced lineage revision in one directory transaction.
A descendant sharing an inherited object advances the same cell-local revision.
A separately copied fork or a refused publication leaves that revision unchanged.
The revision has its own directory entry so a materialized residence can support local forks when its original ancestor lives in another cell.
Existing shared deployments are part of the initial manifest inventory. The revision witnesses subsequent publications.
Readers that compare revisions must read the directory strongly and include every actual manifest that predates their scan.
The local-volume reaper reserves the lineage using the current publication revision and preparation generation, then builds the actual referrer inventory inside that reservation.
The active sweep is tied to the volume and residence in the directory.
It closes its I/O region before releasing the reservation, and a later worker can recover abandoned local work only after it acquires the same volume maintenance lock.
The remote reaper still needs to bind the observed revision to a durable effect authorization.
The only operation that adds references initializes an unpublished fork's manifest under its fresh prefix.
Later edits withdraw exact name-owner pairs from the latest confirmed manifest and publish only at that snapshot's successor.
An immutable write can succeed in a lower slot that was already pruned while the request was in flight.
The read path confirms the current head after loading its bytes, and the withdrawal path confirms absence in the current head after a write.
Cleanup removes only slots strictly below a confirmed head.
The reaper drops a shadowed fallback before deleting the local object and retains the release record until its physical work succeeds.
This reasoning assumes authoritative listings, immutable conditional writes and monotone heads in open residence prefixes.
Already open readers retain their selected manifest. `CatalogReaders` registers a cut under the logical deployment and storage root.
For a serving fork's fallback, `ReferencingStore` now registers a `ReferenceRead` on the ancestor's directory record before reading from its provider path.
The registration compares the exact residence, the fork's organization and lineage, the storage root and both storage namespaces. The directory CAS orders this admission against the whole-residence fence. The provider task owns the registration until it completes, even if its caller cancels.
If the owner refuses admission, the reader checks its own copy again and propagates the admission error while the copy is unavailable. Disposal can resume once admitted operations drain. A successful transfer leaves the bytes readable from the fork's own prefix.
The local witness and the persisted hosted task identity let the existing reconciler recover abandoned admissions. An admission without usable process-death evidence is retained until the issuing worker releases it.
Per-read registration adds directory work on inherited misses. Its latency and throughput are unmeasured.
[Manifest model](../tla/MonotoneReferenceManifest.tla) · [Disposal interleavings](../tla/ReferenceReadDisposal.tla) · [General reader-set model](../lean/ReferenceReadDisposal.lean)

The revision covers publication. A shared fork must also carry a retirement generation from before it starts reading providers through the directory transaction that publishes its prepared manifest.
Checking that the lineage currently admits publication cannot distinguish a preparation that preceded an intervening retirement.
The candidate model admits retirement after active readers drain, advances the preparation generation, and closes admission to further readers.
It transfers existing references and confirms provider removal before reopening.
It covers one source version, one direct reader, and one shared fork.
Local fork preparation and local collection use the same persistent admission state across memory, network and DynamoDB directory clients.
Ordinary remote collection needs a durable retirement intent that orders physical-owner admission, reference publication and provider settlement.
Archive capture, fork preparation and transfer still have separate physical read paths. [Retirement admission model](../tla/LineageRetirementAdmission.tla)

### Serving and archive removal entry points

| Effect path | Current admission and evidence | Remaining composition obligation |
| --- | --- | --- |
| `reap_as_of` and `References::erase` | The reaper uses garbage records, checkpoint floors and existing manifests. The local sweep acquires a lineage reservation before reloading referrers and copies retained data before deleting the path. | Bind each remote retirement to one physical owner and version, validate referrers under that authority, and keep the intent until exact settlement. |
| `Base::initialize`, `Base::forget` and `Base::load` | Initialization adds references. Confirmed withdrawals only remove matching entries and require a current head before effect. | Connect the current-manifest contract to consumers that retain older manifests and to the authority closing and disposing a residence. |
| `erase_deployment` in disposal and old-residence cleanup | The administration path fences the residence, drains serving references admitted at that owner, transfers observed shared objects and checks the provider inventory under its prefix. | Order shared fork publication and administrative reads against the physical retirement authority. |
| `ordered_index_storage::reap` | The directory generation floor and reader registry close admission. Remote cleanup aborts journaled uploads and checks the generation prefix inventory. | Preserve the independent generation and residence bindings in any shared retirement API. |
| `finish_capture_retirement` and whole archive retirement | A capture is closed in the directory. Archive custody and the provider inventory govern physical erasure and final receipts. | Preserve capture identity and the archive's separate namespace when composing with serving retirement. |



## Open questions

- How many roots to keep, and whether a root is written per block or on a schedule (it bounds recovery time against storage).
