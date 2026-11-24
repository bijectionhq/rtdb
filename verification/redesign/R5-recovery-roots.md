# R5 · Recovery roots as holds; collectable L0; reset from a root

## Goal

Make recovery depend on an explicit, held root instead of on files that are simply never deleted.
After R5, L0 files are ordinary garbage once compaction supersedes them (as in XTDB), recovery and compactor reset start from a recovery root plus the log suffix, and nothing in the store is a permanent plaintext source of truth.

## Approved design

- **Recovery root.** A published checkpoint whose manifest names its complete dependency closure (table blocks, trie data and metadata, artefacts), with exact publication identities.
  The root also records the first required replica position, the history and resume token at its cut, and the custody identities needed to open it.
  Recovery acquires the root and suffix hold as one admission before replay starts.
- **Roots are holds.** Each selectable root holds its complete dependency manifest and protected replay coverage.
  The current recovery authority selects a verified successor and atomically publishes a dependency fence over the superseded history, incarnation, stream and interval.
  An alternate path remains eligible only while it has independently complete coverage under the current authority.
  Deletion grants preserve every still-eligible root and drain old admissions before releasing an exact publication.
- **L0 becomes collectable.** The collector's L0 refusal goes, and the catalog stops retaining L0 identities after primary compaction supersedes them.
- **Reset from a root.** A compactor reset rebuilds from a root rather than from retained L0 files, and starts a new naming epoch (the epoch already exists from R3).
- **Log suffix.** Recovery admits root and suffix together under the current authority.
  All frame dereferences use a brokered position-scoped capability through provider drain.
  An excluded position cannot confer access from a stale or returned journal, including through restored credentials or cached custody.
  Physical trim is independently recoverable housekeeping for the journal's fenced and drained prefix.
  Upstream acknowledgement may use an applied token covered by a durable root and a protected committed suffix.
  See [the upstream acknowledgement boundary](authority-boundaries.md#recovery-roots-and-upstream-acknowledgement).

## Invariants

- **DUR-2.** Every selectable path authenticates a complete root and sufficient committed replay under the current recovery authority.
  A switch preserves reconstruction of acknowledged history under the supported fault model and fences superseded dependencies, including on a returned journal.
- **GC-1, GC-2 and LIVE-2.** Preserve every live recovery dependency through drain.
  Use the physical owner's exact deletion grant once the dependency fence and the remaining root, copy and fork census establish absence.
  Excluded journals can stay unavailable while provider settlement proceeds.
- **E2 (shared with R7).** Every live root's closure consists only of readable units (no destroyed key, no collected file).

## What R5 deletes

- The L0 guard in `TrieCollector::collect_table` (`rust/crates/maintenance/src/gc.rs`, "L0 recovery files cannot be garbage collected").
- Retained L0 identities in the trie catalog (`dev/doc/trie-cat.allium` line 9 and the code behind it).
- The L0 replacement plan has been removed. The default reset verifies the installed checkpoint's primary inventory and relocates the selected compacted pairs into the replacement epoch.
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
  Reset loads the installed checkpoint's bound table inventories and checks the live and nascent primary pairs before preparing publication.
- The offline native `repair-checkpoint` command takes a known-good checkpoint digest and an exact later target digest from the same stopped local journal.
  It verifies the checkpoint publications and base table inventories, replays resolved writes into independent primary storage, and publishes a detached checkpoint with a new digest.
  The native regression takes the base from a snapshot export and removes its original primary inventory and the source's L0 files.
  It restarts from the rebuilt checkpoint and resumes writes in a fresh log epoch.
- `retain-repair-base` installs a separately exported current local checkpoint as a durable block collection bound.
  Its journal already retains synced records; the bound protects checkpoint metadata and offloaded replica payloads in the original store.
  Anchor installation and each block-file removal serialize on the local storage gate.
  The collector synchronizes the anchor before acting on a newer bound.
  Advancing the anchor makes older checkpoint files and payloads eligible under block retention.
  The archive owner keeps the detached primary copy available.
  Online reset and automatic L0 collection also need that recovery obligation to survive repair activation and apply at every collection entry point.

## Handoff notes

1. Land after R4: roots are holds, and deletion grants must preserve every publication in their closures.
2. Unify the Multi recovery and checkpoint-metadata holds with the R4 ledger instead of keeping a third retention mechanism.
3. Define the root manifest once (dependency closure plus first required replica record) and make discovery select only complete roots.
4. Remove the L0 guard and the retained identities in the same change that makes recovery root-based, so no window exists where L0 is collectable but still relied on.
5. Re-run the checkpoint replays and their controls (`verify_checkpoint_replay` in `verification/run.py`); the runner's node-faults test count must be updated if tests are added.

## Tests and proofs

- **Native:** crash during compaction and during reset, then recover from a root plus the log suffix; collect superseded L0 files and recover again; trim the log below the root and recover.
- **TLA+ / Apalache:** extend `InstalledCheckpoint.tla` and `ResidenceRetirement.tla` with roots as holds and L0 collection.
- **Mutations that must fail:** releasing a root before its successor is complete; collecting an L0 file still in a live root's closure; trimming the log above a live root's first required record.

## Local retention verification

[RepairAnchorRetention](../lean/RepairAnchorRetention.lean) proves coverage for arbitrary file identities and finite schedules of publication, head advance, anchor replacement, selection, removal, and restart.
The model assumes durable publication and a monotone serving head.
The native installer checks the exact current head and export before publishing the anchor.
The native collector holds the filesystem gate through the deletion decision and effect.
[The bounded TLC model](../tla/RepairAnchorRetention.tla) includes three block indices and checkpoint catalog, table and offload files at each index.
It distinguishes the visible anchor from its last durable value.
The check explores 5,766 states and rejects late admission of an older cut, removal without the anchor check, an acknowledged anchor lost on restart, and removal based on an unsynchronized successor.
It reaches physical removal of an older offload after advancing the anchor and restarting.
The native regression runs the automatic collector, repairs to an older target using its real offloaded transaction, and observes removal after anchoring a newer export.
The rebuild can select the persisted anchor directly under the original journal lock.
[Verus](../verus/local_repair/proof.rs) checks the Rust decisions for monotone anchor replacement, exact receipt comparison, collection ordering, and the bounded replay interval.
The native replay rejects a persisted boundary that disagrees with the checked base export, and the harness rejects a program that skips that comparison.
The filesystem lock, synchronization calls, and binding of the receipt to the actual journal media remain runtime premises.

Coordinated native local startup holds a shared filesystem recovery gate while it discovers its checkpoint, loads table metadata and admits its replica cursor.
Trie deletion grants and collection of old local checkpoint files or detached payloads require exclusive use of the gate through provider completion.
The startup gate protects the selection interval.
Native startup checks the data and metadata files named by live and nascent primary tries, including each recorded data length, before accepting the root.
Compactor reset uses the same inventory check.
Long-lived holds for complete recovery closures remain a separate obligation.

## Retirement after an adopted compactor reset

The coordinated reset stages the exact compacted trie pairs and the required successor block and naming epoch in the retirement journal before publication.
Once the installed replica prefix covers the replacement checkpoint and the selected trie names have been withdrawn, the leader can acknowledge their removal.
The same durable catalog floor as ordinary collection keeps the pairs available to readers of the old cut and rejects stale admissions after advancement.
The collector resumes those intents after restart regardless of the age threshold for new garbage.
A repaired local store reopened with a fresh journal can publish a catalog cut after it adopts the first checkpoint in that journal.
The native reset regression keeps the old files with a catalog reader held across restart, then settles the durable intent after that reader has drained without another block adoption.
[Verus](../verus/reset_retirement/proof.rs) checks the production adoption predicate and [TLC](../tla/ResetRetirement.tla) checks the bounded restart and release protocol.
The L0-inventory reset still depends on the original L0 inputs, and those files stay outside automatic collection.

Snapshot export verifies the retained primary inventory under a catalog hold. The plan carries that hold into every copy and publishes the root after its members.

## Retained checkpoint reset

Native local maintenance can replay from a detached retained anchor to its adopted checkpoint and install the resulting primary catalog in place.
It uses the maintenance writer's existing journal authority and synchronizes its rebuilt file pairs before requesting the catalog replacement.
The term refuses the replacement if the catalog or transaction head moved during preparation.
Compacted pairs from the rebuilt base use a name derived from the successor block in a range reserved for recovery.
The runtime preserves original L0 source descriptors for ordered replay and reconstructs absent pairs in the journal suffix.
Native ordered-index maintenance can rebuild a full projection from a synchronized epoch-zero journal when an adopted L0 data file is missing.
It checks the exact captured boundary, admitted intervening uploads, per-block L0 event counts and the total table event count, and establishes erase cuts before writing pages.
[Verus](../verus/ordered_replay/proof.rs) checks the production decisions for each recorded block and the complete inventory.
The native regression reopens a writer, removes a historical L0 input and the ordered pages, and compares the rebuilt bytes and indexed queries with the L0-based projection across an erasure.
Automatic L0 reclamation still requires coverage for replay archives, replay inputs outside the supported native journal, and journal and offload retention through the agreed recovery interval.

## Replay archive handoff model

[ReplaySourceHandoff](../tla/ReplaySourceHandoff.tla) checks the proposed switch from original L0 custody to a separately verified archive at a single checkpoint.
The archive owns its serving inventory and its exact-cut event source before new reads route to it.
An admitted source copy keeps its journal through provider drain.
An already admitted reader or an outstanding startup block confirmation keeps the original files after the switch.
The model reaches a state with an admitted archive reader and both original sources removed.
TLC checks 420 states and rejects publication before synchronization, a foreign replay cut, retirement during an original read or pending confirmation, and trimming before the copy drains.
The model takes the exact-cut check and independent durable archive content as inputs.
Hosted recovery can select an independently verified replay capture of the exact parked checkpoint.
The directory publishes the capture and the recovery digest for its archive custody together, and each startup retains the selected capture before verifying that digest.
[Verus](../verus/serving_replay_handoff/proof.rs) checks the production selection guard against a live holder, a changed checkpoint, a competing selection, and a replacement at the same block.
The capture format preserves the existing encrypted replay closure, including the original ordered inputs by logical name.
A new capture can authenticate the exact members of a pre-framing serving pack and write the same logical contents in the current framed format.
The copy checks publication identity and envelope size before funding the complete framed inventory.
Each body must match its inspected publication and plaintext length. Recovery compares trie data and search run extents to those declared by the checkpoint, and checks catalog digests on the plaintext.
The [copy proofs](../lean/CheckpointCopyEvidence.lean) cover inventory agreement and show that the ciphertext preflight admits every legacy member whose framed encoding fits.
The existing handoff adopts the independent framed archive for the same parked checkpoint, while the original pack stays immutable.
A parked complete repair can promote a framed replay archive of the same independently authenticated cut.
Later captures can read those inputs from an admitted archive after their original packs disappear.
The local and hosted Multi reapers can retire older original block packs for a parked root after verifying independent archive coverage.
For a mixed pack, the exact provider publication is the deletion unit.
Each member outside the replaced checkpoint metadata must have matching plaintext in the authenticated successor archive, or be a receipt L0 member covered by the receipt retirement rule.
The parked selection closes generic fork admission. The lineage reservation protects an in-progress fork, and its published references protect the original pack.
Age is no longer a condition of this replay-pack grant or of the checkpoint-metadata pack grant with exact release evidence for every member.
The local reaper holds the maintenance lock, lineage reservation and archive region while verifying and unlinking the pack. Hosted retirement uses the same lineage authority with exact version settlement or enrolled terminal tombstone evidence.
Hosted readers can remain admitted when their region uses the same selected archive and the archive covers every payload member, including receipt L0.
Raw readers still require drain.
In enrolled terminal S3, active writers can stay attached through the same archive. The directory fence excludes new uploads to the pack and checks for admitted uploads to that key. The conditional tombstone preserves exclusion after the directory releases its grant.
The grant also requires no declared descendant references and a certified replica prefix that passes the old boundary while retaining the published boundary.
Only the physical original pack is erased, so the archive continues to serve the logical L0 descriptors.
An in-place restore of an archive-backed installation must adopt a complete replay capture.
A parked installation can advance its serving archive after restore and preserves the earlier restore receipt.
The old repair capture becomes eligible for retirement after selection advances and its admitted readers drain.
[ArchiveEnvelopeReads](../tla/ArchiveEnvelopeReads.tla) checks that older archive envelopes can supply the same logical objects under a whole-object reservation and a current cache namespace.
Accepted framed members support authenticated range observations independently of the disk cache fill plan.
The [checkpoint-metadata Verus proofs](../verus/checkpoint_metadata/laws.rs) check the pack and journal bounds and exclude new admissions of the old metadata.
[LocalReplayPackRetirement](../tla/LocalReplayPackRetirement.tla) checks that new readers can open after erasure and detects weakened guards for original readers, raw copies, descendants, the journal and restore.
Hosted versioned and unversioned storage still require provider-specific retirement authority for mixed packs.

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

### Serving recovery inventory

Hosted publication derives the complete replay inventory from the authenticated root and table blocks.
It observes each member's length and physical version where the provider exposes one.
The directory commits to that ordered inventory together with the root digest, replica epoch and first required replica offset.
An unversioned member relies on the immutable-object contract.
Startup verifies the same commitment under its existing metadata and replay admission.
Flush, park and idle suspension share the writer's publication path.
Prefix trim requires the commitment, and a holder can certify a previously published cut in place.
A transfer or repair verifies its own copy and clears the commitment made against the previous storage view.
Startup, live replay and archived suffix preparation check the same replica anchor.
The trim grant names the exact journal and a prefix covered by its persisted directory anchor.
An authenticated archive publication releases its suffix's hold on the live journal.
The archive continues to protect its own capture.
An admitted archive copy retains the original journal range through provider drain, including when the pin expires or the archive publishes while work is outstanding.
[ReplicaTrimArchive](../tla/ReplicaTrimArchive.tla) checks both sides of the handoff and reaches physical trim while a verified archive still serves the capture.

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

The ordinary remote collector accepts loose-file releases that carry a matching installed publication and immutable S3 version.
The lineage reservation retains both provider identities: the released data version and the release marker version observed with its body.
Before erasure authorization the reaper reads the marker again, verifies the exact identity and contents, and installs a physical-owner fence covering both paths.
A declared reference to either path keeps it in place, including references with an owned shadow.
Data removal settles first. A fresh provider inventory must establish that each reserved version is absent before the directory can reopen the lineage.
The successful settlement transaction clears the physical-owner fence and reopens the lineage together. An older authorized reservation without a marker version can settle its data while leaving the marker for a version-aware pass.
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
The integrated hosted path keeps the old version while a current manifest points to its source location.
Its reservation closes fork, capture and transfer preparation, and its per-object source fence orders inherited reads against provider removal.
The reaper checks the fresh installed floor, provider identity and current manifests before authorizing erasure.
The authorization transaction checks the specific owner revision validated during preparation and the matching data and release fences.
A change to that record before authorization leaves the existing reservation available for revalidation.
The same owner check covers immutable version removal and never-versioned tombstones.
The shared directory contract exercises missing fences, incomplete fences and stale owner revisions across local, network and DynamoDB directories.
TLC explored 31 states, rejected both unsafe authorization mutations and reached settlement after revalidation.
[Owner authorization model](../tla/OwnedRetirementAuthorization.tla)
Only a reservation that has issued no provider effect may be abandoned.
After authorization, retries use the same stored provider version until a fresh inventory confirms it absent.
The hosted retirement model explored 47 states and rejected four unsafe protocol mutations.
The marker settlement model separately checks a replacement at the same marker key, a delayed exact removal, independent absence evidence and atomic reopening.
[Marker settlement model](../tla/ExactMarkerSettlement.tla)
It also reached a redundant provider effect after settlement while the replacement remained available.
The legacy collector retains covered loose file markers while their immutable publication witness is being recovered, even when a current-name listing hides the provider version.
The reaper authenticates retained receipts under the exact source residence key and matches the full installed cut and logical object.
The same two-version retirement protocol orders the recovered data and original marker against readers and confirms absence of each version.
The authorized reservation can finish from its persisted versions if the historical receipt or residence key later becomes unavailable.
For a hosted loose trie release, the physical owner verifies that the directory's authenticated checkpoint covers the installed removal and that older recovery admissions have drained.
Settle previously authorized retirements through their recorded publications before certifying this coverage across a rolling upgrade.
The retained receipt model explored 16 states, rejected four unsafe changes and reached settlement after losing the original receipt.
The ordinary hosted path sweep withdraws eligible inherited references.
Owned loose objects, owned packs and owned release markers remain until the provider retirement authority can settle them.
Keeping the marker keeps released bytes hidden after a reopen, even when no exact removal can proceed.
The hosted cell adapter rejects path deletion, so provider retirement and inventory are the outlets for physical removal.
The bounded path-release model explored eight states and catches removing bytes by name, dropping their marker before settlement, and reopening an immutable manifest slot to delayed writes.
[Hosted path-release model](../tla/HostedPathRelease.tla)
Releases without a matching immutable publication witness still need a separately settled protocol for unversioned providers.
[Retained receipt model](../tla/RetainedReleaseReceipt.tla)

### Hosted checkpoint pack collection

A release of a packed checkpoint metadata file records its logical key, the owned pack path and the provider version that supplied the member index.
The provider's HEAD, trailer and index must agree on that publication.
The collector accepts a block pack only when the index contains the pack's own catalog, every member is checkpoint metadata, and a mature marker for the exact same pack version covers each member.
A newer published checkpoint pack must be visible under the same storage root.
The physical owner's CAS compares the observed checkpoint with its current one and bounds both the pack's block and the greatest member block below every applicable metadata hold.
The lineage reservation and owner fence order this decision against published references and inherited readers.
Ordinary hosted path collection cannot erase an owned pack.
After exact retirement settles the pack, later passes may settle the associated markers against confirmed absence of that same version.
This also works while a replacement is visible at the original pack path.
Pack collection keeps mixed L0 packs, spill packs, unversioned packs and packs with only legacy path releases.
The model assumes immutable version identities, monotone checkpoint publication and authoritative provider inventory.
It abstracts the index decoder, directory transactions and snapshot execution.
[Bounded pack model](../tla/HostedPackRetirement.tla) · [Retention arithmetic proof](../verus/checkpoint_metadata/proof.rs)

Live provider qualification still needs rolling deployment evidence and the exact provider's listing, deletion and credential behavior.
The hosted reaper role needs transactional puts confined to the cell's `~reference-revision/` rows in `deployment-names` and a deployment-table condition check for whole-residence disposal.

### Never-versioned serving buckets

The Bijection hosted serving bucket policy explicitly denies enabling S3 versioning, replication and object lock for every principal.
The recovery bucket is versioned and serves a separate purpose.
The serving reaper points at the never-versioned bucket, so exact-version reclamation does not cover that deployment profile.
[Serving bucket policy](https://github.com/bijectionhq/bijection/blob/dca74b6ee7dae24a70b4aaa37f5a00072bcda041/services/big-brain/infra/runtime/cell/hosting_rtdb_storage.tf) · [Reaper configuration](https://github.com/bijectionhq/bijection/blob/dca74b6ee7dae24a70b4aaa37f5a00072bcda041/services/big-brain/infra/runtime/cell/hosting_rtdb_runtime.tf)

A provider mode that preserves the serving bucket contract is to retire a key by conditionally replacing its payload with a permanent tombstone.
The lineage reservation and physical-owner fence still order retirement against recovery, fork publication and provider reads.
The provider operation compares the selected ETag for a present object, or uses create-if-absent for a missing key with authenticated publication evidence.
If a competing create wins, the old reservation cannot overwrite its publication.
A fresh same-key observation of the tombstone and a verified never-versioned policy establish settlement.
The marker prevents a delayed create-if-absent request from reviving the retired key even after the lineage reopens.
Only whole-residence disposal can remove the tombstone, after publishing the residence's terminal fence and settling its outstanding uploads.
The key vault already uses this type of conditional withdrawal with post-operation observations.
[AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) · [Key vault implementation](../../rust/crates/multi/src/keys/s3.rs)

Provider status must be enforced for the lifetime of the residence.
A versioning change between the preflight read and the conditional write could retain the original payload beneath the tombstone even if a later verification refuses settlement.
Enabling versioning on an existing bucket also leaves pre-existing objects with `null` version IDs.
[AWS versioning semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)

Retirement requires an identity bound to the physical key and retirement evidence, a reaper-only conditional replacement grant, and reads and listings that treat authenticated tombstones as retired.
Path-based collectors must share that authority before a namespace is enrolled, so an ordinary delete cannot clear a tombstone or allow an ETag to be reused.
The bounded model covers one key, one held read, a previously admitted create and two outstanding conditional replacement requests.
TLC explored 62 states, rejected six unsafe changes, and reached settlement with an outstanding create and, in a separate witness, an outstanding retirement request.
Claiming full release coverage, qualifying the bucket policy and adapter, and handling historical keys that may already have been physically reused are still runtime obligations.
[Never-versioned retirement model](../tla/UnversionedRetirement.tla)

A tag identifies the observed contents under S3's conditional write contract.
For some S3 uploads, identical bytes at the same key can have the same tag across two publications.
An outstanding retirement must respect the second publication's retention rights even when the bytes are identical.
[Amazon S3 object metadata](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html) · [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

The comparison keeps the same retirement and reset requirements across three designs.
With a terminal physical key, the successor publishes under a fresh key, and a tombstone preserves the old key's terminal status.
A provider that compares immutable publication identity atomically can instead allow a second publication at the same physical key.
A tag-only comparison combined with physical key reuse admits a counterexample even when the source checked the original publication before sending the request.
The supported never-versioned design therefore needs end-to-end evidence that physical keys cannot be reused once admitted, including outstanding uploads and old writers.
A clean enrollment can establish that contract for its fresh namespace.
Historical keys need independent evidence of the same contract before they can be enrolled.
[Publication identity comparison](../tla/PublicationTagIdentity.tla)

The terminal profile binds a fresh allocation UUID and the provisioner's Ed25519 public key to a bucket-level `cell-terminal-<simple-uuid>` root separate from every legacy cell root.
The provisioner must fence ordinary writes while bootstrapping and enforce create-once publication and a permanent tombstone at every retired key for the duration of the allocation.
The enrollment command checks a signed qualification for the exact resolved provider and physical root, verifies the bucket status, and checks objects, versions, delete markers and uploads.
It installs the immutable qualification before the activation marker.
A restart can finish activation when the matching qualification is the only residue; after activation it can reopen the same allocation with its published objects.
The serving adapter, scoped writer factory and retirement inventory check the same qualification and activation marker before opening.
The bounded model checks those distinctions and exercises reopen after publication.
Its early-write counterexample assumes that an ordinary write bypasses the provisioning fence after the final inventory check.
[Enrollment model](../tla/TerminalNamespaceEnrollment.tla)

The native S3 adapter validates the exact physical namespace and key, executes a conditional zero-byte replacement, and requires fresh evidence of the same retirement token and never-versioned bucket status.
Cell-store reads and listings treat these tombstones as absent, and a conditional create conflict refuses to adopt the tombstone as a published object.
For loose files in an enrolled allocation, the write acknowledgement supplies the physical key and entity tag retained by the trie catalog or artefact record.
The collector binds that identity to the installed retirement receipt and admission floor before queueing a release.
The reaper compares the current provider key and tag to the release in its qualified allocation and reserves that data-and-marker pair.
It rechecks the pair and the fork references under the lineage reservation and validates the physical owner's read fence at directory authorization.
The owner's published checkpoint must cover the installed removal, and every admitted recovery must start after that cut.
The production reaper can resume the authorized pair from the directory after restart and settles its owner fence and lineage together.
Historical namespaces without equivalent publication evidence keep their released files.
The serving bucket policy must grant conditional replacement to the reaper and keep terminal tombstones outside path collection until residence disposal.

Whole-residence disposal also needs evidence that every retirement write has settled.
A redundant create-if-absent tombstone request can be harmless while the first tombstone remains visible and recreate the key if the whole residence is purged before that request completes.
The reaper records at most one multipart upload ID per authorized key in the lineage before it submits a part or a conditional completion.
A restart reuses the recorded upload, including when a previous completion reply was lost.
A fresh provider `NoSuchUpload` observation after abort closes that specific identity before the reaper forgets it.
The owner-and-lineage settlement transaction observes an empty upload journal in addition to the matching fences.
The runtime exercises the protocol against an S3 HTTP fixture and persists its journal across directory restart.
A bounded model exercises a redundant request arriving after purge with the upload already closed.
An untracked retry or early settlement can recreate the disposed key in its negative controls.
TLC explored 33 states and reached purge with a delayed completion whose closed upload could no longer affect storage.
An initiation that loses its response before admission can leave an empty upload for provider lifecycle cleanup.
Hosted qualification must establish conditional multipart completion for a zero-byte payload and confirm that a closed upload cannot publish after whole-residence disposal.
Enabling this profile in the hosted fleet still needs a matching reaper policy and live provider qualification.
The checkpoint-metadata collector binds terminal releases to the pack's allocation, physical key and strong entity tag.
It checks a release for every indexed member under that publication and authenticates the newer published root digest from the successor pack beyond the held metadata floor.
The selected release marker settles with the pack. Other terminal member markers remain until residence disposal.
Mixed packs with independent replay coverage use the separate parked-root retirement path.
Other mixed packs and previously unwitnessed files still need publication coverage.
[Disposal composition model](../tla/TombstoneDisposal.tla)

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
The hosted reaper also binds the publication revision to retirement admission and retains existing physical references in place.
The only operation that adds references initializes an unpublished fork's manifest under its fresh prefix.
Later edits withdraw exact name-owner pairs from the latest confirmed manifest and publish only at that snapshot's successor.
An immutable write can succeed in a lower slot that was already pruned while the request was in flight.
The read path confirms the current head after loading its bytes, and the withdrawal path confirms absence in the current head after a write.
An admitted local sweep removes only slots strictly below a confirmed head.
Hosted withdrawal retains earlier slots, including when withdrawal precedes an exact object retirement.
Hosted manifest history currently remains with the residence until inventory disposal or a separately admitted manifest retirement.
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
Whole-residence disposal uses a distinct phase of the lineage authority.
The directory closes the lineage under the version of the fenced, drained physical owner and advances the generation before the administration path loads the referrer inventory.
An interrupted disposal can resume against the same residence and generation.
The terminal directory transaction checks the exact disposal permit, installs the receipt and reopens the lineage together.
The local disposal bucket retains the maintenance witness through provider I/O.
Hosted disposal materializes declared fallback references from the fenced residence before it alters the source inventory.
An unreadable referenced payload keeps the residence fenced and the receipt pending.
The provider then aborts uploads and removes the listed versions and markers by exact identity before the first empty observation.
Both empty observations check the provider inventory and the current serving listing.
The native regressions cover an inherited payload, a fork's own shadow, and a declared payload unavailable at its source.
A local S3 lifecycle test exercises fork reads and a late-arriving object during disposal in both versioned and never-versioned buckets.
Hosted fork preparation, capture and the source side of transfer take a shared lineage hold before provider work begins.
An exact stopped-task observation can recover that hold after process death.
An unattested worker's abandoned hold remains pending until its issuer releases it.
The per-key owner fence covers inherited serving misses, including provider work that outlives cancellation.
[Production disposal model](../tla/ReferenceDisposalPublication.tla) · [Generation and residence model](../lean/ReferenceDisposalPublication.lean) · [Hosted exact retirement model](../tla/HostedExactRetirement.tla)

#### Hosted fork staging

Hosted fork preparation reserves the child ID in the directory before writing objects, keys or a replica tail.
Its staged record binds the source preparation, physical residence, object and key namespaces, and any journals receiving the tail.
The stage transaction installs a storage region for the destination's provider writes.
The fork writer records the exact multipart upload on that region before sending data or completion.
The directory atomically publishes the exact staged child, its serving name and the source lineage update after that region has drained.
Definitive rejection closes the staged record after its I/O drains.
A reaper recovering an abandoned stage checks that its source preparation was released, including recovery through attested task stoppage.
An abandoned stage with a retained preparation and no usable stoppage evidence stays fenced.
Closing the stage uses its exact record version, so a successful publication wins against a reaper holding an older observation.
Staged manifests do not enter the published reference inventory.
Stage closure retains unresolved publications while closing admission to new ones.
The reaper confirms the storage namespace and settles retained uploads by exact identity before disposing the residence.
The ordinary disposal path retires the recorded journals, checks both provider inventories across the credential horizon, and withdraws the staged key.
A cleanup proof for a process-stopped fork still depends on hosted task attestation and the provider's exact abort semantics.
The bounded model explored 178 states and rejects writes before staging, early name publication, recovery while preparation is held, stale recovery after publication, untracked submissions and clearing an upload before provider closure.
It reaches publication with a concurrent reaper observation, disposal after a name collision, and disposal after aborting a submitted upload.
The directory contract checks that a stale cleanup update conflicts with a published child.
The S3 lifecycle regression exercises a rejected stage with the same requested name as a published fork in versioned and never-versioned local buckets.
A second S3 regression exercises an unresolved fork publication through recovery, namespace verification, exact abort, and disposal.
[Staging and publication model](../tla/HostedForkStage.tla)

### Hosted original replay packs

The hosted reaper can release an older original pack from a parked root after the current replay archive independently covers its L0 members.
It authenticates the newer published block root and checks the certified replica prefix.
Existing readers, fork preparations, references and outstanding writer leases retain the pack.
In a versioned bucket, it reads the index and every verified member at one physical version.
The directory records exact retirement authority and holds the selected capture until provider inventory confirms absence of that version.
[Versioned replay pack model](../tla/HostedReplayPackRetirement.tla)

An enrolled never-versioned serving bucket binds the old pack's index and members to its strong entity tag.
An index beyond the verification budget keeps the pack in place.
The reaper holds the same capture and lineage while replacing the accepted publication with its permanent tombstone.
Provider confirmation binds the tombstone to the retirement token, and settlement waits for the recorded upload to drain.
A reaper restart resumes authorized work under the stored fence.
If the provider reports a missing or replaced publication after terminal authorization, the lineage and archive remain held until the authorized token can be confirmed.
[Never-versioned replay pack model](../tla/TerminalReplayPackRetirement.tla)

### Serving and archive removal entry points

| Effect path | Current admission and evidence | Remaining composition obligation |
| --- | --- | --- |
| Local path sweep and hosted retirement | The local sweep reserves its lineage and copies retained data before deleting paths. The hosted path sweep withdraws inherited names, and the hosted exact collector orders physical removal against the owner fence and published references. Versioned and qualified never-versioned original replay packs use independently verified archive custody. | Extend settlement to remaining unwitnessed releases. |
| `Base::initialize`, `Base::forget` and `Base::load` | Initialization adds references. Confirmed withdrawals only remove matching entries and require a current head before effect. | Connect the current-manifest contract to consumers that retain older manifests and to the authority closing and disposing a residence. |
| Local `erase_deployment` and hosted `observe_s3_inventory` in disposal and old-residence cleanup | The administration path fences the residence, drains serving references, closes the lineage and transfers published references. Hosted disposal then purges versions and uploads through the provider inventory. Settlement atomically publishes the receipt and reopens the lineage. | Extend physical-owner read custody to administrative copies and qualify provider settlement under failure on hosted storage. |
| `discard_local_fork` and hosted fork staging | The local path uses the unpublished prefix. Hosted preparation journals uploads on the staged child. Recovery closes admission and settles those upload IDs before ordinary inventory disposal. | Qualify exact provider settlement and task stoppage for hosted fork writers. |
| `ordered_index_storage::reap` | The directory generation floor and reader registry close admission. Remote cleanup aborts journaled uploads and checks the generation prefix inventory. | Preserve the independent generation and residence bindings in any shared retirement API. |
| `finish_capture_retirement` and whole archive retirement | A capture is closed in the directory. Archive custody and the provider inventory govern physical erasure and final receipts. | Preserve capture identity and the archive's separate namespace when composing with serving retirement. |



### Installed serving prefixes

Each installed writer grant has its own serving prefix, separate from checkpoint archives and from Directory-managed L0 collection.
[RTDBServingPrefixRetirement](../multi/tla/RTDBServingPrefixRetirement.tla) checks that retirement removes the prefix only after the fence, the trim to an independent checkpoint, the absence of retained records from the grant, and the drain of its writes, and that it never reaches the successor's prefix.
Each guard has a mutation that TLC rejects; the pending-preparation guard is implied by the retained-record guard in the model and is checked by Verus instead.
[Verus](../verus/serving_prefix_retirement/proof.rs) checks the production guards for supersession, the scoped write horizon and the combined evidence.

### Cell transfer attempts

[RTDBCellTransferAttempts](../multi/tla/RTDBCellTransferAttempts.tla) checks that a transfer that meets conflicting immutable bytes moves to a fresh generation only after closing and draining its attempt, that only the selected prepared attempt and its receipt can be routed, and that disposal never reaches a selected generation.
[Verus](../verus/cell_transfer_attempt/proof.rs) checks the production guards for closing, selection, activation and disposal.

## Open questions

- How many roots to keep, and whether a root is written per block or on a schedule (it bounds recovery time against storage).
