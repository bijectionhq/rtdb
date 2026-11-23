# R7 · History collection and privacy erasure

## Two operations

History collection retires data below an authorized logical horizon.
A collection ticket identifies the selected history, its horizon and the exact replacement publication.
The residence floor and recovery roots protect outstanding readers and replay.
Collection preserves all observations above the horizon and leaves admitted reads entitled to their held view until release.
A call needing a physical erasure deadline uses privacy erasure for the protected material in its custody domain.
Bijection's routine reclaim currently issues SQL `ERASE` batches.
Its existing deployment contract targets 120 seconds after its irreversible destruction order and requires qualification for a fixed deadline.
The native handoff must preserve that distinction.
Migration requires a native collection operation and conversion of those call sites before deadline-driven SQL `ERASE` is enabled.
Tests must cover live keys in the same physical unit as retired history and a held view admitted before collection.

Privacy erasure removes specified history in one custody domain, including dependent forks.
An independently keyed copy has its own erasure authority and retention contract.
An authorized export establishes that boundary before an independent copy becomes readable.
Its source erasure receipt identifies the exact domain and disclosed custody boundary.
It states the fate of the source's own ciphertext and decryption grants.

## Admission, publication and deadline

1. Close admission of new key copies for the affected identities.
   A copy with an unknown outcome is resolved before erasure can commit, or covered by the same fenced revocation authority and therefore incapable of publishing an independent late grant.
2. Build successors for trie files, associated artefacts, and any root or log closure that contains erased history.
   Material that survives the erasure remains available through the successor roots.
   A protected log suffix can retain only ciphertext whose keys are covered by the same revocation or whose records are free of the targeted history.
3. Prove the clean closures are durable at the provider and ready for every affected root.
   A lost or ambiguous provider reply delays the commit until reconciled.
4. Atomically select the erasing generation, close affected admission and renewal in the publication order, and record destruction obligations for the exact old key slots.
   New views exclude the erased history from that decision.
   A promised deadline budgets both the maximum drain and the provider's evidenced destruction tail from the commit.
   Until a provider and cache policy qualify, a request may remain pending or receive a failure before a fixed deadline is committed.
5. Previously admitted views may finish under their original exclusive grant deadlines.
   A key authority may withdraw the required material after the drain, when no admitted work can use or release the old body.
   An old view that requires a withdrawn key receives `rtdb/erased-under-view`.
   The new root and its clean history remain readable.
6. Completion binds the erasing generation and provider evidence to the enumerated domain after the drain and key destruction have been confirmed.
   Unreachable ciphertext cleanup can proceed independently.

The committed deadline uses the authoritative clock and a measured bound for grant expiry, including clock error and outstanding I/O.
A provider that may grant indefinite access to an old key cannot take the transition in step 4.
Local expiry alone is insufficient if a persisted wrapping remains recoverable.
The deadline applies after commit, and preparation and provider reconciliation may take longer than 120 seconds.
This keeps the surviving root readable through deadline expiry, including when a provider stops answering during cleanup.

Search artefacts do not supply ambient statistics.
Two-argument scores therefore depend only on the resolved row and query.
Corpus-relative queries derive their statistics from the applicable admitted rows or receive them explicitly.

## Operation-only capture revocation

The local archive broker gives checkpoint readers and exporters key operations under the original capture authority.
Each captured object and archived replica segment uses a separate slot, bound to the common capture authority.
An independent stage reseals the verified plaintext with its own installation key.
The broker orders irreversible vault closure against every active cryptographic operation.
The directory publishes the revocation observation after that closure has completed.
A previously issued handle cannot decrypt another object after the observation, including ciphertext already retained by the handle's owner.
The source capture and its copies are distinct custody domains only after a completed independent re-encryption.
The authenticated preparation first closes new archive admissions, and the revocation commit waits for outstanding regions and provider operations.
Preparation can take an unbounded time.
The contract excludes plaintext already returned and depends on keeping vault material outside recoverable backups and older material-exporting binaries.
Serving history and existing exported keys still require a separately qualified deadline authority.

The scheduled capture action arms a single immutable due time at the same broker.
A previously prepared schedule can recover after an ambiguous outcome, and the broker recovers its schedule from durable storage before it performs cryptography after restart.
Expiry depends on the local authority clock reaching the recorded time.
Qualification of a bounded real-time deadline additionally requires an authoritative clock with an enforceable error bound.

## Installed journal adapter

An installed Logkeeper grant identifies the journal payload key generation.
Chosen replay uses that generation and reports `rtdb/erased-under-view` when the required key is durably revoked.
The Logkeeper rotation compares the exact writer grant, exact published checkpoint and journal head.
RTDB owns the clean successor, the evidence for all selectable roots, the ordered erasure commit, and the deadline checks for held material.
The installed custodian's coordinator checks the exact prepared and published head, validates the rotation receipt, and requires both recovery slots to attest the clean root and successor suffix generation before it calls the custody commit authority.
The custody adapter owns the durable admission fence, construction and staging of the clean closure, recovery slot attestations, and the atomic commit and expiry.
The native tests exercise the coordinator with Logkeeper application decisions and a local vault under an injected custody adapter.
Installed serving reads carry the journal-issued lease through provider access and check it again before returning a body.
Detached replica objects inherit the servicing lease for cryptography and open in the original writer's key namespace.
Installed log and object key operations have a local monotonic time limit.
These bounds cover the key operations and affected storage responses; decoded pages, query results, and other working copies require their own drain evidence.
Independent checkpoint member slots allow a staged successor to open with no dependency on the prior writer or checkpoint allocation's object keys.
The current provider profile keeps the order-relative 120-second operational target.
A fixed SQL deadline requires an enforcing custody authority.
The pinned Logkeeper application API publishes a checkpoint and rotates its writer key in distinct decisions.
It supplies no authenticated application response covering both crash-selectable durable slots.
A production fixed-deadline adapter requires a journal decision that orders closure with the survivor-root selection, authenticated recovery images from both slots, bounded plaintext and egress drain, and evidenced key destruction.
The [installed journal operator documentation](../../dev/doc/rtdb-multi-installed-journal.md#clean-checkpoint-and-erasure-commit-interface) describes the inputs and result of that coordinator.

## Verification

`ErasureCustody.tla` models adoption of a successor, a same-domain key copy with an unresolved provider outcome, a dependent fork, an independent export and a held old view.
It checks readable successors, bounded abstract revocation and independent export readability.
It abstracts the provider, the clock and all cache invalidation.
Certification also needs a native replay of copy-completed-with-lost-reply, cross-domain export, and revoke-after-recovery with a surviving row and an admitted reader.
Check the provider's actual key versions and cache grants when verifying a completion receipt.
Physical history collection and metadata retention are separate acceptance checks.

[CaptureKeyRevocation](../tla/CaptureKeyRevocation.tla) checks the local capture broker's prepare, provider closure, durable observation and inventory settlement.
A lost-reply witness recovers and settles a closed key while a separately keyed copy remains readable.
Controls expose stale-handle decryption, undrained active operations, raw material export, early publication, early inventory settlement and destination-key aliasing.

[CaptureScheduledExpiry](../tla/CaptureScheduledExpiry.tla) checks that acknowledged schedules survive restart and that an operation crossing expiry returns no cryptographic result.
The completion-time check assumes the authority's clock is monotonic within the modeled schedule.
Controls expose a lost schedule, an early acknowledgement, a changed due time and a late result.

[RecoveryCustodyHandoff](../lean/RecoveryCustodyHandoff.lean) proves joint safety for arbitrary unit identities and finite schedules of confirmation, independent copy admission, erasure commit, revocation, and physical retirement.
The replacement cut covers the acknowledged upstream position, and its declared primary and replay units are readable and disjoint from the retiring custody set.
A runtime refinement must establish that those units reconstruct the confirmed prefix.
An independent copy retains readable units outside that set.
At the deadline the old units have no usable grants, and their ciphertext can be removed.
A retry payload can be one of those old units while its committed coordinate remains covered by recovery.
[The TLC model](../tla/RecoveryCustodyHandoff.tla) explored 2,376 states and reached retirement with an independent copy and an advanced confirmation.
Controls cover incomplete replacement, a retained reference in the new suffix, a shared independent unit, early upstream confirmation, and a missed revocation deadline.
The transition at the deadline assumes a provider that can revoke all applicable grants.
Runtime provider qualification and the protocol for outstanding same-domain provider operations are separate obligations.

[R4](R4-residence-ledger.md) · [R5](R5-recovery-roots.md) · [R6](R6-sealing.md) · [Authority boundaries](authority-boundaries.md)
