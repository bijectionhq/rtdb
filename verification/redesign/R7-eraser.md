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
4. Atomically publish the erasing generation and arm revocation for the old key slots.
   The commit is the observable start of the deadline and makes new views exclude the erased history.
   This transition needs a custody provider that can guarantee irreversible revocation and expiry of all outstanding grants within 120 seconds of that commit.
   Until a provider and cache policy qualify, a request may remain pending or receive a failure before this commit.
5. Until the deadline, already admitted views retain their captured answer under bounded old grants.
   After the deadline, an old view that requires revoked key material receives `rtdb/erased-under-view`.
   The new root and its clean history remain readable.
6. A completion receipt binds the erasing generation and evidence of revocation to the enumerated domain.
   Provider cleanup of unreachable ciphertext can be retried after decryption becomes impossible.

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
The same key protects the captured root and its archived replica suffix.
An independent stage reseals the verified plaintext with its own installation key.
The broker orders irreversible vault closure against every active cryptographic operation.
The directory publishes the revocation observation after that closure has completed.
A previously issued handle cannot decrypt another object after the observation, including ciphertext already retained by the handle's owner.
The source capture and its copies are distinct custody domains only after a completed independent re-encryption.
The authenticated preparation first closes new archive admissions, and the revocation commit waits for outstanding regions and provider operations.
Preparation can take an unbounded time.
The contract excludes plaintext already returned and depends on keeping vault material outside recoverable backups and older material-exporting binaries.
Serving history and existing exported keys still require a separately qualified deadline authority.

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
