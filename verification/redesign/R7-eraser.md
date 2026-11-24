# R7 · History collection and privacy erasure

## Two operations

History collection retires data below an authorized logical horizon.
A collection ticket identifies the selected history, its horizon and the exact replacement publication.
The residence floor and recovery roots protect outstanding readers and replay.
Collection preserves all observations above the horizon and leaves admitted reads entitled to their held view until release.
A call needing a physical erasure deadline uses privacy erasure for the protected material in its custody domain.
Bijection's routine reclaim currently issues SQL `ERASE` batches.
A custody profile may offer a fixed physical-destruction deadline only if it can enforce the deadline during outage under its declared clock and fault bounds.
No such profile is installed today.
The installed profile keeps the 120-second operational target from the irreversible destruction order.
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
   Admission of a fixed deadline requires a profile that can enforce both bounds during outage.
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

Committed SQL declarations retain their own schema through replay and checkpointing.
The survivor schema joins those explicit declarations with types inferred from the surviving row events.

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
The custodian orders the exact prepared clean checkpoint, incumbent grant and journal head in one Logkeeper decision.
The same decision selects the clean checkpoint, rotates the payload key generation, and closes new admissions below the erasure replay floor and on the previous recovery roots.
The ordinary checkpoint publisher skips an erasure preparation.
Protected replica replay can reopen the original committed erase transaction, including co-ordered receipt erasures, and retain its positioned effects through the exact selected head. SQL staging checks that protected head against the installed placement and history, and checks the successor catalog coordinates against the replayed source. Staging rewrites live and persisted primary events in memory at that cut, retains the surviving event positions and rebuilds trie metadata and table statistics. It persists the independently sealed survivor closure and leaves derived runs to rebuild from that closure. Logkeeper's preparation binds the operation, custody domain, timing profile and rotation request. The provider seals the same binding into the successor manifest and the coordinator reopens it before requesting the switch.
Installed recovery uses the clean manifest's exact root and replay cut. Its first eligible replay is the successor suffix, with the ordered erasure decision supplying confirmation of the root.
The erasure coordinator reopens every exact member through its provider and verifies the body and its recorded publication before rotation.
It then checks the rotation receipt and the installed Logkeeper client's signed proof that both recovery slots on each voter in an accepting quorum were read back.
The custody adapter owns staging and verifying a complete survivor closure, draining plaintext and egress and enforcing a deadline backed by evidenced destruction of the retiring key slots.
The installed provider profile supports the operational target and refuses fixed-deadline admission.
The native tests exercise the coordinator with Logkeeper application decisions and a local vault under an injected custody adapter.
Installed serving reads carry the journal-issued lease through provider access and check it again before returning a body.
Detached replica objects inherit the servicing lease for cryptography and open in the original writer's key namespace.
The detached replay broker registers uploads before accepting a body and retains that region until provider operations join.
S3 and DynamoDB namespace withdrawal fences new unit grants, waits out the provider and delivery window, then settles the registered unit material. The wait covers existing bounded grants from these vaults.
Installed log and object key operations have a local monotonic time limit.
These bounds cover the key operations and affected storage responses; decoded pages, query results, and other working copies require their own drain evidence.
Independent checkpoint member slots allow a staged successor to open with no dependency on the prior writer or checkpoint allocation's object keys.
Retained plaintext delivery grants inherit the original object read lease, including when a key owner could still answer.
The provider operation cutoff and the catalog cutoff keep their own clock coordinates. The effective view and its delivery grants must satisfy both.
Query computation holding a catalog lease stops on cancellation or its local lease cutoff. Provider tasks retain their original I/O owner until they join.
The live installed SQL path includes the ordered serving lease even when the catalog yields only in-memory rows. Egress is bounded by the same lease. Expired PostgreSQL portals and Flight SQL tickets release their retained catalog holds.
Installed checkpoint discovery and staging acquire a durable ordered hold on the exact source root before reading. The lease bounds copy admission and provider tasks retain that hold through completion, including after cancellation.
The ordered switch closes renewal for every old-root lease.
Logkeeper reports old holds and unsettled checkpoints to the authenticated placement custodian.
The retirement loop requires both the joined-custody journal and ordered readback that old-root holds have ended before withdrawing old checkpoint or serving slots.
Checkpoint dispositions are retained before Logkeeper settles those allocations, and the settlement command binds the erasure floor.
The serving census requires original-vault receipts through the closing writer epoch, including outstanding retirement intents.
Destruction evidence binds both readbacks and the incumbent journal disposition.
The next erasure of this placement requires confirmed destruction for the prior cut.
The credential-separated replay broker stores enrolled root histories and live duties in one ordered custody-domain record.
The broker closes membership and admission together for the exact operation and generation.
Each root signs its original head, readable successor, ordered switch and joined-custody digest for that operation.
The broker commits the complete domain drain only after it verifies those root statements and every registered worker join.
Each root then attests its original provider's disposition against that domain drain.
The broker retains the aggregate completion and opens the next generation after all enlisted roots settle.
With custody control enabled, a detached broker request registers before accepting plaintext and signs its terminal observation after provider and socket drain.
The installed writer's staging call checks the broker's target and enrolled root, pauses user submissions, and replays the original committed erase marker through the exact certified head.
It builds the successor from the protected snapshot store.
The ordered erasure preparation keeps that head stable across normal appends and grants.
The placement coordinator submits its joined root witness to the broker and requires the completed domain drain before it arms key retirement.
It binds the verified provider disposition to the same drain when reporting root completion.
The broker checks installed query and copy registration signatures against each root's admission key.
The originating process uses a separate signer for each work owner's terminal evidence.
Query and transport custody follows the held catalog, and checkpoint copy custody follows the retained provider owner.
The mounted serving store and journal each retain a working-plaintext duty through their I/O regions.
The SQL submission entry registers before accepting a new transaction.
A missing join remains pending, including after loss of its process.
A fixed SQL deadline requires an enforcing custody authority, a complete survivor rewrite and retirement census, custody-wide working plaintext drain, and evidenced destruction.
The coordinator requires a signed fixed reservation before issuing the switch. The reservation binds the prepared successor, exact retiring key inventory, custody work roster and latest destruction time.
The fixed commitment binds both inventories and the deadline to the authenticated switch.
The original authority signs the drain of the same roster, with an observation for every working plaintext, egress and copy participant after admission closes.
Separate destruction evidence binds that drain to the provider disposition.
A late disposition is recorded as a missed fixed deadline. A pending provider result does not furnish a disposition.
The checkpoint custodian durably records the roster bound to the certified switch. Each duty binds its registered worker’s verification key and records a separately signed observation after closure. Destruction confirmation reopens that journal and requires every observation before withdrawing the incumbent journal key through its original provider. The durable provider observation binds the aggregate receipt, which is retained for later readback. A custody actor can durably register a duty in the incumbent key generation before starting work and must recheck the original grant after registration. The certified switch freezes the registered roster, including duties admitted by earlier writers in that generation. The ordered holds used by SQL, egress and checkpoint copies still need to be joined to the operation’s custody-wide roster for the production destruction receipt.
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

The installed writer discovers the first erase during certified transaction apply.
The root admission signer records the exact transaction and replica position in the broker generation.
Ordinary checkpoint publication preserves the replay suffix that contains that marker.
The installed operational adapter binds an independent PKCS#8 signer to the enrolled root.
A sole enrolled root can advance its committed marker to the certified switch automatically.
The provider retains the chosen rotation and the authenticated root and physical disposition records.
The installed provider uses a 120-second operational target and the existing custody profile offers no fixed physical-destruction deadline.
A shared hosted fork reserves membership at the original broker before referencing or copying its checkpoint closure.
A live Directory publication makes it a dependent root.
A rejected stage retains the reservation until Directory disposal.
The published membership binds the source checkpoint and the original resolved suffix before child transaction renumbering.
