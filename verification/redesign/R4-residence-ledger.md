# R4 · Residence ledger, reader holds and deletion grants

## Goal

Replace every timer that today stands in for knowing who still reads a file with one linearizable record per database.
After R4, a file is deleted only when the ledger proves no admitted reader, recovery root or fork can need it.

## Approved design

The residence ledger orders eligible roots, admissions, retirement and deletion grants under one physical owner.
Multi uses the directory and lineage authority.
Classic uses a conditional-write head or a synchronized filesystem ledger in one lock domain.
The current authority and irreversible exclusions survive restore outside the data being restored.

The writer and replay installer publish complete immutable dependency manifests at each eligible cut.
Manifest edges bind the original basis and scope, including inherited layers, catalog and routing evidence, artefacts, replay coverage and the openable custody needed to use them.
A physical leaf names the owner, namespace, allocation incarnation, provider key and its immutable version or terminal creation identity.
A pack is one physical leaf. Each member that depends on it keeps its edge.
Keep explicit exhaustion so an incomplete or stale inventory supplies no negative evidence.

The authority orders claims from current publications, advertised historical scopes, holds, forks, in-flight work, recovery roots, and staging or transfer work.
Register the exact scope before storage I/O, and acquire additional coverage before expanding it.
The catalog floor governs admission to advertised history.
Closing issuance or renewal preserves already admitted work through drain.
An abandoned hold requires positive drain evidence or an enforced bound through the last possible provider access, including clock uncertainty.

The physical owner consumes the retirement receipt, checks zero reachability and closes new references in the same ordered decision that persists the grant.
The grant names one immutable publication.
Ambiguous writes retain their staging edges until reconciled or fenced.
An old authentic manifest cannot re-admit a granted publication.
Retries consume the same grant and settle only on exact removal, terminal replacement or conclusive absence.
The [Lean comparison](../lean/ResidenceReaderRetention.lean) explains why an unrelated earlier hold supplies no deletion evidence and must not block eligibility.

## Invariants

- **GC-1.** Admitted reads, forks, recovery roots and operations retain their complete dependency scopes through their promised lifetimes and drain.
- **GC-2.** The physical owner's ordered grant binds the exact immutable publication, establishes absence of eligible dependencies, and excludes later references and late publication.
- **LIVE-2.** Fair collection and eventual provider settlement reclaim each retired publication after its final dependency and fence, including during unrelated holds.

## What R4 deletes

- `GarbageConfig::garbage_lifetime` and the system-time arithmetic in `TrieCollector::default_garbage_as_of` (`maintenance/src/gc.rs`).
- The fixed read lease and its tip-anchored currency: `storage/src/{read_lease,lease_decision}.rs`, `node/src/read_currency.rs`, `Database::read_lease`, and the collector's first-observed wait (`TrieCollector::leased_out`). Keep the per-request check in `IoRegion`, re-anchored on the hold.
- The unbounded in-process registry in `index/src/partition.rs` (`Retention::readers`), or reduce it to a cache of ledger holds.
- Multi's reaper grace and wall-clock release stamps (`multi/src/store.rs` releases, `multi/src/main.rs` `--grace` check), once each release has deletion authority independent of its timestamp.

## State on main (2026-11-21)

- **Peer work, Multi side (extend it, do not duplicate it):**
  - `dd5a0b0fa` registers held catalog captures with the serving residence: `rust/crates/multi/src/catalog_readers.rs`, the directory's `catalog_readers` map (`CatalogReader { owner, storage_root, replica_epoch, cut }` in `rust/crates/directory/src/lib.rs`), and Verus area `verification/verus/catalog_reader_admission`.
  - `aa52621d3` models residence retirement and holds: `verification/tla/ResidenceRetirement.tla`.
  - `b3adb3df5` compares retention policies: `verification/tla/ResidenceReaderRetention.tla` and the Lean laws above.
- **Certificate and renewal evidence:** the [Lean certificate model](../lean/RetirementCertificate.lean) proves exact identity and hold preservation across arbitrary histories, under an authenticated retirement premise.
  The [TLC renewal model](../tla/RenewedResidenceHold.tla) checks two observations of one ticket, a delayed renewal reply and bounded in-flight work.
  The [SMT margin check](../smt/residence-clock-margin.smt2) covers separate monotonic clock rates and bounded provider use.
  The [Lean reclamation proof](../lean/ReaderReclamationMinimality.lean) shows that a collector can guarantee safety and progress only with observations or environment assumptions that eliminate outstanding use at the chosen collection point.
  When a live operation and an abandoned operation can produce the same observations for any finite prefix, a safe timing-only decision cannot reclaim that object.
  Use a provider-use bound that covers the last possible dependency access, or a sound observation of drain for providers whose use has no such bound.
  The production ledger still needs renewable holds and a capability consumed by the reaper.
- **Classic side:** Native local-journal and local-storage nodes coordinate trie retirement through the filesystem ledger below.
  Bounded monotonic leases and the collector eligibility delay still cover the paths that use them.
- **Directory implementations:** `MemoryDirectory`, `DynamoDirectory`, `RemoteDirectory` exist; there is no object-store-backed one.

## Currency of the existing lease

The foreground query path and the maintenance task share the same pending observation and confirmed clock anchor.
They sample a monotonic clock before reading the published replica tip.
A sample replaces an outstanding sample only if it names a greater tip.
Installed progress confirms the pending tip only when it reaches that position.
Confirmation consumes the sample and retains the later of its clock reading and the previous anchor.

[ReadCurrencyAnchor](../tla/ReadCurrencyAnchor.tla) models both observers with separate clock and tip reads.
It confirms that the installed prefix covers the sampled publication frontier and that the clock anchor never moves backward.
It reaches a confirmation while another observation is still in flight.
The controls cover reversing the clock and tip reads, confirming ahead of installation, moving the anchor backward and replacing a pending observation at an equal tip.
[Lean](../lean/ReadCurrencyAnchor.lean) proves coverage and monotonicity over arbitrary finite histories of recording, installing and confirming samples.
[Verus](../verus/read_currency/proof.rs) checks the production decisions under the surrounding mutex, including controls that weaken both an implementation and its contract.

The model assumes an advancing published frontier and monotone installed prefix within one replica epoch.
The proof of the synchronous decisions relies on the caller supplying the clock sample and installed position from that same authority.
Query capture, delegated fragments and membership queries check the lease at admission, including paths that can answer from memory. The native query fixture exercises the admission refusal and offline acceptance. The [collector turnover study](../tla/CollectorTurnover.tla) checks that a fresh trie selection in a successor term starts its own lease window and protects bounded in-flight I/O.
The native fixture observes the successor retain the files through that interval and reclaim them at expiry.
A native replay elects a successor on a shared journal while the predecessor holds an open cursor and observes the successor finish the installed retirement without another write.
The remote adapter fixture retains the catalog owner through cancellation until the provider finishes consuming the response body.
The hosted fixture passes an installed receipt through the directory floor, release queue and exact reaper and resumes after a lost provider reply.
The fixtures establish the local filesystem ledger and the hosted versioned release path with controlled providers.
A coordinated hold through hosted term placement and live-provider qualification remain open.

## Handoff notes

1. Start by reading the peer's `CatalogReaders` and `ResidenceRetirement.tla`, and the last day of commits touching `multi/src`, `directory/src` and `maintenance/src/gc.rs`. If a peer session is mid-way, coordinate through the programme rather than branching the design.
2. Define the ledger operations once, as a small trait both deployments implement (admit hold, renew, release, observe, advance floor), and implement it for Multi on the directory and for classic on conditional writes. Avoid making classic depend on the whole `Directory` trait.
3. Move physical retirement behind the grant owner, then remove each timer when the corresponding admission and recovery paths are covered.
4. Thread the hold through `QueryContext` where the read lease is today (`CatalogSnapshot::read_lease`, cursor `IoRegion::leased`), and through served scan fragments (`Node::scan_fragment`).
5. Classic conditional writes: check each provider adapter (`storage/src/remote.rs`, local store) supports the primitive you choose; record any provider without it as unsupported rather than emulating it.

## Production admission and reclamation paths

The current call paths serve as the refinement checklist for the residence ledger.
Each physical effect needs a release decision tied to the authority that admits its dependencies.

| Path | Current boundary | Remaining obligation |
| --- | --- | --- |
| Cursor and distributed scan | Snapshot admission registers a Multi catalog hold through the backing store. Cursor and fragment polling retain their context for provider work. | Connect all attempts and cancellation paths to a renewable hold or confirmed drain, including classic stores and transaction and export entry points. [Snapshot helper](../../rust/crates/query/src/context.rs) · [Cursor](../../rust/crates/query/src/cursor.rs) · [Fragment](../../rust/crates/query/src/fragment.rs) |
| Transaction resolver | Source and external transaction resolution acquire a catalog hold where the serving store supports one. The local ledger fixture checks that a query cursor over a transaction overlay can outlive the resolver and retain the hold until dropped. | Establish the cross-term contract on stores without a shared ledger and cover cancellation with in-flight provider dependencies. [Resolver](../../rust/crates/tx/src/resolver.rs) |
| Trie compaction output | Hosted retirement plans carry observed provider versions for witnessed loose files. The collector records an installed removal receipt and advances the admission floor before queueing file releases. A successor can retry an incomplete handoff from the pending plan. | Couple the installed receipt, every dependency hold, the admission floor and the provider identity in one certificate. [Collector](../../rust/crates/maintenance/src/gc.rs) · [Intent and receipt](../../rust/crates/maintenance/src/retirement_intent.rs) |
| Classic trie and block collection | Native local storage serializes reader ticket admission and the durable floor, and checks the installed receipt before physical trie deletion. Trie collection also uses the lease window and snapshot registry. The block collector uses an installed snapshot and configured retention. | Extend the classic ledger to qualified remote stores and the independent block and artefact collection paths. [Storage contract](../../rust/crates/storage/src/object_store.rs) · [Collectors](../../rust/crates/maintenance/src/gc.rs) |
| Multi loose and packed objects | Hosted exact settlement handles witnessed loose trie files and fully released checkpoint metadata packs. The local path eraser runs under the volume maintenance witness and the directory lineage reservation. The simulator can exercise its missing-reference control only on a memory store. | Extend the contract to L0-bearing packs, old path-only releases and unversioned providers. [Reaper](../../rust/crates/multi/src/store.rs) · [Fork references](../../rust/crates/multi/src/reference.rs) |
| Derived artefacts and ordered pages | Stored artefact builds and their fallback builds hold their captured input catalog through provider drain. The artefact lane retires definition files and unused builds. Hosted ordered pages have a separate directory-backed lifecycle and reaper. | Bind artefact retirement and repair to an exact publication, prove the last shared member is covered, and show how ordered-page scopes refine the residence ledger. [Artefact lane](../../rust/crates/maintenance/src/artefact.rs) · [Ordered pages](../../rust/crates/multi/src/ordered_index_storage.rs) |
| Captures, moves and installation disposal | The lifecycle paths use captured archive and installation identities, and the disposal code inventories provider objects. | Prove each lifecycle fence covers every affected reader, recovery and fork dependency, including provider work outstanding when the route changes. [Capture retirement](../../rust/crates/multi/src/archive/retirement.rs) · [Installation disposal](../../rust/crates/multi/src/admin.rs) |

### Local trie release records

Readers and the trie collector open the ledger under one canonical storage root.
Admission and advancement hold the same exclusive filesystem lock.
Each reader retains a shared lock on its ticket for the lifetime of its snapshot and the provider operations that inherit it.
Closing the reader or exiting its process releases the lock.
The collector can unlink an abandoned ticket after taking its exclusive lock.
A live reader whose cut cannot cover the proposed floor leaves the retirement pending.

A successful advance synchronizes the journal-scoped floor before any file effects.
A release reads the exact installed receipt and checks that floor before deleting the batch.
A collector can reopen the storage root and finish a partially released batch from the same receipt.
The local contract assumes every participating node uses the same filesystem lock domain and storage root.
An offline journal replacement starts a newer epoch from the latest durable checkpoint.
The successor can admit that checkpoint before it has applied a known cut in its new journal.
Any opaque or earlier held catalog delays retirement under the new journal.
An installed successor floor closes new admission from earlier journals.
An exclusive compactor reset resumes admission at the installed cut of its newly adopted naming epoch.
Classic S3, Azure and GCS use one conditional-write head under the classic storage root for reader registration, release and floor advancement.
The head makes registration and floor advancement one decision and avoids deriving current admission from a possibly stale list of ticket objects.
Every successful update advances the sequence, including a cancellation that fences a registration with a lost reply.
The current classic grant owner checks the stored receipt and uses the conservative cutoff until the complete dependency census is wired.
A pending decision keeps the intent available for retry.
Multi queues scoped releases and the provider consumes the existing exact lineage grant.
A provider request retains its ticket through cancellation and releases it on drain.
A hosted checkpoint subscription renews through a new admission of the same cut and file claim at the current floor.
It only replaces its offer after successful admission.
Existing executions keep their independent registrations through provider and delivery drain, including after refused renewal.
An abandoned ticket without drain evidence remains in the ledger.
Independent maintenance and complete recovery-root retention still require custody and settlement integration.

A bounded serving catalog claim records live compacted primaries and the immutable files selected by its active search runs.
Hosted search retirement submits an installed retired run directly to the exact storage owner.
An opaque or oversized claim keeps the conservative scope. Search staging and uncoordinated readers still need their separate retirement paths.

### Generic fork admission

The source lineage reservation precedes checkpoint inventory reads and orders them against the collector.
A same-organisation fork resolves the selected root, its table blocks, and every replay member to the original physical object or enclosing pack.
It publishes just those owner references and verifies that the recipient opens the same inventory before making the fork selectable.
A fork into a different organisation verifies and copies the selected inventory under the recipient's own storage key.
Historical block and timestamp admission require the precise dependencies to be present, independent of elapsed time.

### Hosted trie release records

The leader reads back the installed retirement receipt and registers its exact cut under the matching catalog admission floor.
The receipt names the logical file list by the digest of the selected plan.
Every queued release carries that residence, object namespace and installed cut.
The reaper consumes the release using a directory observation for the same storage root and journal that covers the certified cut.
A trie release from a legacy marker awaits a receipt in an enrolled residence.
[HostedTrieReclamation.tla](../tla/HostedTrieReclamation.tla) models the receipt, floor, release record, stale observations and a second residence with independent holds.
It treats file identity within one residence and completion of provider effects as premises.
The retirement plan already observes loose provider versions when available.
The hosted reaper settles witnessed loose files and covered checkpoint packs against their immutable S3 versions after validating current manifests and fencing inherited reads at the physical owner.
For older markers, it can recover the version from the authenticated receipt for the same installed cut and object.
Mixed-content packs, releases without a matching publication witness and provider qualification remain open.
The Bijection serving bucket denies versioning, so its physical retirement needs a separately qualified provider operation under the same directory authority.
[Hosted serving bucket contract](R5-recovery-roots.md#never-versioned-serving-buckets)

## Tests and proofs

- **Native:** a follower behind the log tip serves a held generation; the collector never deletes below a live hold; an abandoned hold expires after two sweeps and its generation becomes reclaimable (LIVE-2). Reuse the follower fixtures in `rust/crates/conformance/tests/node_faults.rs` (`follower_cursor_during_collection`).
- **Verus:** the hold admission predicate and the floor computation.
- **TLA+:** extend `ResidenceRetirement.tla` or `ResidenceReaderRetention.tla` with renewal, expiry, a follower behind the tip and a failed CAS; retire the age-based branch of `LeaseAdmission.tla`.
- **Mutations that must fail:** admitting a hold below `F`; expiring a hold after one sweep; computing `F` from an observation the CAS does not cover.

## Open questions

- The classic ledger layout (head object, chain, or per-follower holds).
- The values of `T`, `R` and the margin; the margin must still cover twice the clock-rate drift over `T` plus the longest provider request.
