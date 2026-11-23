# X1 · Acknowledged transaction recovery

Run `~/.elan/bin/lake build` from `verification/multi`.
Lake imports `Protocol`, `RecoveryCertificate`, `SelectionFence`, `ObjectWithdrawal`, and `JournalRefinement` from pinned Logkeeper main.
It also compiles the upstream Study 111 `MaintenanceBound` and Study 113 `DisposalLease` directly from that checkout and imports RTDB's `ReaderRetention`.
The pin is for the Lean reference.

## Theorems

[`acknowledged_transaction_survives_recovery`](AcknowledgedRecovery.lean) proves that, for any finite `Continuation` of the original chosen prefix and any valid `OrderedJournal.Trace`, the exact acknowledged append is still at its original decision position.
A fresh read at the certified same-installation basis contains the transaction's logical event if no later committed erasure in that prefix covers it.
The theorem also gives hold and application safety, exact mutation receipt reissue within the original horizon, and an exact registered and unwithdrawn physical version outside the lost AZ for each projected native replay read.
The fault ranges over every zone of the modeled deployment.
`recovery_read_is_exact_history` identifies the entire surviving logical history at the selected basis, and `recovered_query_observes_exact_history` gives an arbitrary query that consumes that projection the same result as evaluating it on the chosen history.

`Continuation` links seals by cohort and permits any finite number of in-cohort extensions and exact cohort repairs.
Its induction uses `Protocol.compatible` and `Protocol.transfer_extends_chosen_history`.
`recorded_append_reissues_while_live` uses the receipt projection in `RecoveryComposition` and `OrderedJournal.live_receipt_survives_recovery`.
`retained_journal_entry_survives_one_az` separately proves physical journal coverage from `JournalRefinement.CompleteCut` and a three-voter roster with distinct AZs.
For records below the retained floor, reconstruction goes through checkpoint fidelity and `CheckpointReplay.replay_suffix_from_checkpoint`.
Decision positions count control commands and the data LSN tracks the application stream.
`RecoveryProjection` makes the correspondence between the selected journal basis, the held checkpoint, the certificate scope, and the typed replay explicit.

`recovered_history_keeps_committed_fence` composes the ranked recovery and installed-basis results in `SelectionFence` at the same recovered decision history.
`selectable_image_keeps_mirrored_fence` proves dominance of every surviving eligible slot after a trace from mirrored acknowledgement.
The production projection from that fence to the clean root and generation is a D02 obligation.
`applyDecision` removes exactly the logical events covered by the interpretation of a chosen erasure commit.
Its induction proves preservation in the absence of a later covering commit.
An erasure can remove part of a transaction's history, so the projection must choose the appropriate event granularity and include stable transaction identity.
An arbitrary query can observe a later update, a delete, or no row from a transaction at its chosen basis.

`maintenance_reserve_survives_replacement`, `disposed_worker_cannot_admit_read`, and `leased_read_finishes_before_reclamation` give the capacity and lease conditions around recovery.
The maintenance lemma bounds retained records and room for the next maintenance append.
Transition from that bound to completion needs funded bytes, a runnable custodian, reachable services, and eventual settlement of holds.
The two lease lemmas concern new admission after disposal eligibility and completion before physical collection, respectively.

## Hypotheses and evidence

A named Lean witness establishes the result in the reference model.
The native evidence below identifies the relevant slice of an open production refinement, unless the row identifies a particular Verus guard.
PX1 runs the Lean witnesses and controls.
The inventory identifiers have their definitions in Logkeeper's `verification/formal-inventory.md`.

| Lean hypothesis or remaining obligation | Reference discharge, production evidence, or environment premise |
| --- | --- |
| `initial : RecordedAppend`, `decodedAcknowledgement` | L2, R12, X2. The acknowledgement must follow exact read-back and application of this append and project the same logical effects and outcome. RTDB `driver.rs::validate_own_readback` and `apply_admitted` are the refinement boundary. Installed-journal test `gateway_replaces_a_writer_and_resumes_its_prepared_provider_copy` is a native slice. Logkeeper `ordered_admission` specifies exact fingerprint matching and retention guards. The acknowledgement-to-applied-outcome refinement is open. |
| `chosenPath`, including each `Protocol.Evidence`, `Transfer.exactImage`, and seal/install ordering | L1, L3, L4, L11, L12, X2. The Lean agreement and transfer lemmas give prefix persistence. Logkeeper `a_surviving_group_recovers_the_grant_and_preserves_the_paid_append_window` and RTDB `funded_control_reconciles_lost_appends_and_releases_lost_reads_through_trim_and_repair` exercise recovery, repair, and receipt identity. Mapping ordered reducer decisions, compacted images, durable replies, and authenticated repair into the same ledger remains open. |
| `complete`, `chosenEntry`, `retained`, `roster`, `ab`, `ac`, `bc` | L11 and environment. `JournalRefinement.any_recovery_quorum_can_read_exact_entry` supplies the intersection proof. Deployment must qualify three distinct current voters on three independent AZs and a complete cut of durable copies at every installation, including repaired cohorts. The actual retained record bytes must project to the exact suffix selected for this read. Qualify by losing each AZ at commit, publication, and repair boundaries and reopening the surviving media. |
| `journalPath`, `journalInvariant`, and `read.admitted` | L1, L5, R2, R4, R10, X3. `OrderedJournal.protected_through_trace` proves the modeled floor and hold invariant and `CanServe` provides application readiness. RTDB `replica_admission` checks displaced terms and sequence classification. `a_journal_fenced_term_opens_without_a_claim_or_maintenance_record_across_replacements`, `a_worker_without_the_current_grant_never_leads_a_journal_fenced_term`, and the installed lost-read/repair test are native slices. Mapping the full grant, fresh barrier, hold release, and trim transitions remains open. |
| `initial.decoded`, `journalPath`, and `after.observed < …expiresAt.next` for reissue | L2. `RecoveryComposition.recorded_append_reissues_while_live` discharges receipt preservation under the modeled trace. The installed lost-append/repair test and Logkeeper's `ordered_admission` retention and fingerprint guards are native slices. Receipt expiry limits exact retry. Durable visibility uses the chosen history independently of this horizon. |
| `sameInstallation`, `readCoversAcknowledgement`, `exactReadCut`, `journalAgreesAtReadCut`, and `ReadProjection` cut bindings | R4, R12, X2, X3. The fresh barrier and authenticated journal projection must agree on installation, exact cut, and decoded decision prefix. The installed gateway replacement test and `expired_withdrawn_worker_disposal_releases_the_read_that_blocks_trim` exercise the adapter. The read-cut and command/data coordinate refinement remains open. |
| `exactScopeCut`, `selectedRoot`, `authenticPlan`, `checkpointInHistory`, and `typedReads` | R1, R10, X3. `ReplayClosure` and `ReplayCertificate` discharge modeled read coverage from that projection. `inline_upload_replay_reads_only_its_typed_dependency_closure` and `oversized_upload_replay_reads_only_its_typed_dependency_closure` are native conformance slices. Exact binding of all provider versions, decoded bodies, digest-bound adopted catalogs and the selected checkpoint interval remains open. |
| `ready : ZoneReady …` and `lost` | `RecoveryCertificate.zone_placement_refines_the_provider_contract` turns per-zone placement into held-version coverage. The environment must qualify survivor readability, exact provider identity, inventory, key availability, and no correlated AZ loss. Physical packs sharing one failure domain count once. Probe exact versions, including encrypted objects and provider allocations, while each AZ is unavailable. Certification of every protected root and retained suffix in production remains open. |
| `withdrawalTrace` and `registered` | L7, R7, X3. `ObjectWithdrawal.trace_preserves_separation` proves the per-object invariant from the trace. Logkeeper `exact_object_withdrawal_survives_compaction_and_cohort_repair` checks a durable fence. RTDB `a_retained_offloaded_payload_remains_replayable_after_its_delete_request` checks candidate isolation. Complete registration of every exact held provider version, preservation through compact/repaired state, and physical effect after fence, reader drain and exact-version match remain refinement obligations. |
| `mediaTrace`, `eligible`, `rules`, `durable`, `chosen`, `sealLater`, `sealing`, `repairs`, and `selected` | L10, L11, L13, R5. `SelectionFence` proves rank preservation and authenticated seeded selection in the model. Logkeeper `rotation_fences_both_slots_across_changed_majorities_and_repair` and RTDB `either_old_selectable_slot_blocks_commit_and_leaves_the_view_readable` exercise the media and evidence checks. Qualification must cover file and directory sync on deployed media. Authenticating both selectable slots at the exact fence and projecting the selected image to the successor root/generation remain production obligations. |
| `committedTargets` and the absence of a later covering commit | D02, L9, R5, R6, R11. Only a durable covering erasure commit, with its exact-head clean successor and irrevocable deadline, can interpret a control as removing the event. `exact_head_rotation_survives_lost_rotation_and_commit_replies` and `a_retained_record_uses_its_own_key_until_that_generation_expires` are native slices. Exact survivor rewrite, recovery using only the clean root and new key generation, and qualified atomic custody/deadline authority are open refinements. |
| `checkpointFidelity`, `suffixFidelity`, `readUsesReplay`, `usesSelectedCut`, and `queryUsesRead` | R11, R12, X2. `replay_suffix_from_checkpoint` supplies the fold identity. Native replay's use of every surviving transaction, exact effects and SQL read fidelity at the chosen basis require semantic refinement of archive/replay, apply, and serving. Typed object coverage alone supplies no apply or query semantics. |
| `MaintenanceBound.Reaches lane` and paid capacity | L6, R3. Study 111 proves three retained maintenance records and gives the per-term-claim control. The fenced-term replacement test exercises classification and claim-free regrant. The byte translation requires three funded maximum framed record charges, including exact reference charges and funded checkpoint/control operations. Peak disk funding and progress under repeated interruptions are deployment obligations. |
| `bounded`, `dispose`; `admittedBeforeRetirement`, `clockBound`, `admission`, `completionBound` | L8, R4, R7. Study 113 and `ReaderRetention` prove the two time inequalities. RTDB `read_lease` verifies the ordinary storage admission and grace decision leaves under clock and duration bounds. Installed `expired_withdrawn_worker_disposal_releases_the_read_that_blocks_trim` exercises disposal and trim. The installed serving/protection path must enforce the lease and settle or drain operations admitted earlier. Qualify clock rate/error and duration across restart, retirement, and loss of connectivity. |
| Installed scope and trust environment | D03, D05, L3, L12, R9. Qualify authenticated identities, transitively retained verification keys, signatures and hash binding, deterministic canonical decoding under Prost, and installed-only routing. The framing Verus area, installed admission tests, and bypass mutations cover local decisions. Environment qualification includes corrupted-media/codec tests, verifier vectors, and fail-closed provider version and conditional mutation probes. |
| Recovery progress | D06 and environment. The composition quantifies over finite valid continuations with a selected fresh read. Eventual selection, acquisition and delivery require reachable voters and stores, a live funded custodian, finite operation delays or eventual hold settlement, and fair scheduling. Qualification requires fault and restart runs with backlog, hold pressure and repeated replacement. |

## Controls

[`AcknowledgedRecoveryControls.lean`](AcknowledgedRecoveryControls.lean) checks a reachable append, grant replacement, two exact cohort transfers and a fresh protected read with any one zone lost, including coverage for the retained journal entry and offloaded replay payload.
It also proves counterexamples to the weakened properties rather than accepting a compiler failure as evidence.

| Checked property | Negative control |
| --- | --- |
| Chosen prefix and exact repair | A shorter choice omits the acknowledged position. An independently certified but non-exact repaired image substitutes a different append. |
| Voter fence and seeded recovery | A tear of the only updated slot reopens the old promise. A cohort seeded without the fence can select an empty prefix. |
| Journal coverage after one AZ | Two voters in the failed AZ leave no majority. A minority-only copy can miss the entire recovering majority. |
| Receipt preservation and hold safety | Exact expiry allows reissue to stop. Advancing trim past a held suffix breaks coverage. |
| Read readiness and result fidelity | A stale or alien applied cut cannot serve. Read admission can hold even when an unconstrained reply drops the transaction. |
| Typed replay, placement and custody | A base-only enumeration omits a reachable oversized payload. A single-zone placement cannot certify its loss. A grant for another physical generation cannot bind the selected object. |
| Ordered withdrawal | Either ignoring existing references or allowing a new reference after the fence violates separation. Without registration, a placement certificate can name only withdrawn versions. |
| Reconstructed and erased history | Omitting an event in either checkpoint or suffix loses it. A later covering commit removes it, while a non-covering control or an earlier commit preserves the append. |
| Maintenance and leases | Charging regrant exhausts a fixed reserve. Removing drift permits a late read. Removing the I/O margin permits completion at the reclamation instant. |
