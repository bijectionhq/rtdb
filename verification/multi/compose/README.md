# Installed journal composition

Run `~/.elan/bin/lake build` from `verification/multi`.
Lake imports `Protocol`, `RecoveryCertificate`, `SelectionFence`, `ObjectWithdrawal`, and `JournalRefinement` from pinned Logkeeper main.
It also compiles the upstream Study 111 `MaintenanceBound` and Study 113 `DisposalLease` directly from that checkout and imports RTDB's `ReaderRetention`.
The Lean reference and Rust workspace use the same Logkeeper revision.

## X1 · Acknowledged transaction recovery

[`acknowledged_transaction_survives_recovery`](AcknowledgedRecovery.lean) proves that, for any finite `Continuation` of the original chosen prefix and any valid `OrderedJournal.Trace`, the exact acknowledged append is still at its original decision position.
A fresh read at the certified same-installation basis contains the transaction's logical event if no later committed erasure in that prefix covers it.
The theorem also gives hold and application safety, exact mutation receipt reissue within the original horizon, and an exact registered and unwithdrawn physical version outside the lost AZ for each projected native replay read.
The fault ranges over every zone of the modeled deployment.
`recovery_read_is_exact_history` identifies the entire surviving logical history at the selected basis, and `recovered_query_observes_exact_history` gives an arbitrary query that consumes that projection the same result as evaluating it on the chosen history.

`Continuation` links seals by cohort and permits any finite number of in-cohort extensions and exact cohort repairs.
Its induction uses `Protocol.compatible` and `Protocol.transfer_extends_chosen_history`.
`recorded_append_reissues_while_live` uses the receipt projection in `RecoveryComposition` and `OrderedJournal.live_receipt_survives_recovery`.
`retained_journal_entry_survives_one_az` separately proves physical journal coverage from `JournalRefinement.CompleteCut` and a three-voter roster with distinct AZs.
For records below the retained floor, reconstruction goes through checkpoint fidelity and the fold identity `replay_suffix_from_checkpoint`.
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

## PX2 · Progress under stated premises

[`InstalledJournalProgress.lean`](InstalledJournalProgress.lean) proves `funded_maintenance_admits_next_record` for any reachable extended Study 111 lane about to append a fenced receiver terminal, boundary or upload.
It funds three checkpoint records plus the receiver budget using a maximum framed charge that includes references.
The receiver budget covers retained fenced terminals and one possible open reservation until effective trim.
Additional reservations can use a free unit, and a chosen fenced terminal keeps that unit charged until trim.
It also proves `advancing_bounded_clock_reaches_disposal_time` for any time trace that reaches the required real-time threshold with bounded journal lag, using Study 113's `DisposalLease.MayDispose`.
`withdrawn_holds_are_eventually_disposed` proves disposal on an arbitrary trace once time stays past that threshold, provided custody can reach the journal, the continuously enabled disposal decision is weakly fair, and a chosen disposal releases the holds.
The Lean controls exhibit a reachable boundary after two settlements with an underfunded reserve or an oversized charge, and an infinite time trace for each missing clock premise.

[`InstalledJournalProgress.tla`](InstalledJournalProgress.tla) checks the following temporal laws on one shared state machine.
Each law is checked with its own fairness scope and again with all the fairness clauses together.
Weak fairness (`WF`) acts on a continuously enabled transition.
Read admission needs strong fairness (`SF`) because expiry can disable admission between successful renewals.
Stuttering is allowed, and each conditional law starts once its triggering state occurs.

| Named law | Trigger and result | Required fair actions |
| --- | --- | --- |
| `CheckpointEventuallyPublishes` | A pending checkpoint settles any inherited receiver, then goes through boundary, upload, exact preparation, immutable copy and publication. | The recovery actions below, then `SettleInherited`, `AppendBoundary`, `AppendUpload`, `PrepareCheckpoint`, `CopyCheckpoint` and `PublishCheckpoint`, all `WF`. |
| `PublishedCheckpointEventuallyTrims` | Once the ordinary root is published, the floor reaches its boundary cut and the later upload stays charged. | `TickJournal`, `DisposeExpired`, `ReleaseHolds` and `Trim`, all `WF`. |
| `WithdrawnHoldsEventuallyDisposed` | The withdrawn enrollment's hold is removed once the chosen clock reaches its last lease end plus drift. If the enrollment never held a lease, disposal is eligible immediately. | `TickJournal` and `DisposeExpired`, both `WF`. |
| `FreshReadEventuallyAdmitted` | From a ready writer and a pending fresh invocation, a renewable lease eventually permits the exact chosen barrier cut to be admitted. | `RenewLease` with `WF` and `AdmitFreshRead` with `SF`. |
| `RequestedErasureEventuallyCommits` | A staged erasure obtains both authenticated clean images from each voter in the surviving majority and durably binds the cut and deadline. | `MirrorSome` and `CommitErasure`, both `WF`. |
| `CleanSlotsEventuallyCommitErasure`, `OpenReservationEventuallyCommitsErasure` | Once both slots are clean, even when checking from an explicitly usable reservation, the qualified authority eventually commits. | `CommitErasure` with `WF` and enough deadline headroom to keep that action enabled. |
| `WriterEventuallyGranted`, `WriterEventuallyReady` | After one AZ loss, the surviving journal grants the nominated writer, which materializes the exact grant cut and confirms the grant before reporting ready. | `GrantWriter` with `WF` for the grant, plus `ReplayGrant` and `ConfirmWriter` with `WF` for readiness. |

The TLC bounds are three voters in three AZs with each possible single AZ loss, one replacement grant, a retained upload plus one boundary and upload, one ordinary checkpoint, one withdrawn hold, one independent overlapping hold, one fresh read with repeatable two-tick leases, and one clean erasure attempt with two slots per surviving voter.
It explores zero or one previously settled receiver terminal plus zero or one inherited slot still requiring its terminal.
Five funded records cover the largest modeled case.
The four-record control reaches the boundary after both settlements and has no room for the upload.
The baseline keeps both surviving voters reachable throughout the run and pauses ordinary intake.
The disposal clock explores zero through four real ticks, monotone journal samples with one tick of clock error, a last lease end of two ticks or no lease, and one tick of disposal drift.
The real-time cap leaves room for the longest modeled disposal wait.
Lease aging can recur independently of the capped disposal clock.
The erasure starts after the ordinary checkpoint and read, at a quiesced, prepared cut with an authenticated fixed reservation and a provisioned successor key.
`StartErasure` abstracts the exact ordered rotation for that attempt.
Each clean-slot action represents authenticated readback at that same rotation, following Study 112's two-image gate.
The atomic decision follows W4's `JournalErasureDeadline`, with the installed fixed reservation as the deadline and a conservative clock sample as its commit guard.
In the baseline the reserved deadline is tick six, beyond the last conservative sample at tick five.
The shortened reservation shows that a usable deadline at the instant both slots become clean is insufficient under untimed fairness alone.
These ticks express a finite model bound and supply no elapsed-time guarantee.

Run `check-progress.sh` with `TLA2TOOLS_JAR` set to the TLC 2.19 jar and `JAVA_BIN` set if Java is outside `PATH`.
The runner checks the composed baseline, the separately scoped laws, and each single-premise control at its configured law.
Its reachability witness settles an inherited slot alongside a previously retained terminal, admits a fresh read at the chosen boundary before publication, settles the timed withdrawn hold, trims the published root, and commits erasure with the recovered writer ready.

## PX3 · Installed fresh-read bindings

The selected history is the certified decision prefix at the successful Barrier decision.
`InstalledRead.OrderedDataHistory` takes each chosen append's exact next data cut and leaves it unchanged across control decisions.
`exactReadCut` derives the cut of the held barrier and `readCoversAcknowledgement` covers any acknowledged append already in that prefix.
Application reaching a later offset permits the wait to complete, while catalog capture still requires the selected cut.
If the exact publication has passed, the worker retries the fresh barrier.
`ReadProjection.usesSelectedCut` derives the catalog basis from successful capture and the publication meaning.
`queryUsesRead` derives query input selection from the captured snapshot in the current query path.
A query's requested temporal filters still apply to that snapshot.

`NativeAppend.nativeAppend` projects the ordered record to `Protocol.Append`.
`decodedAcknowledgement` derives the acknowledged logical event from the exact chosen record and its opened, resolved frame.
The production Verus areas cover the [installed read decisions](../../verus/installed_read_selection/README.md) and the [native frame projection](../../verus/native_append_projection/README.md).
The Lean witness combines a chosen acknowledgement, writer replacement, two cohort repairs, a fresh barrier, and a query using the protected capture.
The added controls distinguish a cached route head, an alien digest at the same LSN, an ahead publication, a resampled query, an invalid empty capture, a lost invocation before its release horizon, and an unopened or unrelated append.

## R11 · Exact-head survivor checkpoint

[`SurvivorCheckpoint.lean`](SurvivorCheckpoint.lean) proves membership in the exact-head event set after filtering local marker positions and selected inherited origins.
It also proves that declared members are in the freshly allocated produced set and rejects observations of an old member with the same extent.
The [survivor checkpoint Verus area](../../verus/survivor_checkpoint/README.md) documents the production decisions and the native conformance fixture.

## R8 · Fork and export custody

[`ForkCustody.lean`](ForkCustody.lean) proves the custody invariant for every finite trace of one copy attempt between a source and a destination, with one source preparation and one copy hold.
The hold persists while the destination is unpublished and undisposed, and source withdrawal requires the hold to have ended.
Dependent publication keeps the source domain and independent export publication requires completed re-encryption.
Witnesses reach dependent publication with the hold still live, independent publication followed by release and withdrawal, and rejected-stage disposal followed by release.
The [fork custody Verus area](../../verus/fork_custody/README.md) relates the release guards to the replay broker.

## H4 · Chosen append, barrier and replay

`Composition.chosen_result_projection` constructs `RecordedAppend` from a chosen native result at the certified command position and the receipt insertion of the modeled append step.
`NativeAppend.openedFrame_of_chosen_payload` selects the ciphertext and the original grant's key generation.
`NativeAppend.resolveDetached_preserves_body` derives the resolved frame from the decoded bytes, digest and admitted term and sequence.
`NativeAppend.decodedAcknowledgement` covers inline, staged inline and detached delivery.
The [installed append binding area](../../verus/installed_append_binding/README.md) checks the shipped result guards, ciphertext selector, key derivation and detached-frame decoder.

`InstalledRead.PrefixProjection.headAtAdmission` derives the admission head by relating each chosen decision in the certified prefix to its ordered journal step.
The read starts at the Barrier step on that state.
`ReadProjection.journalAgreesAtReadCut` preserves the exact entry in the journal's chosen-cut map along any subsequent modeled steps.
`ReadProjection.sameInstallation` follows from ordered membership of the exact append that survived `Protocol` extension and cohort repair.
The [installed read selection](../../verus/installed_read_selection/README.md), [installed adapter](../../verus/installed_adapter/README.md), and [native acknowledgement](../../verus/native_acknowledgement/README.md) areas cover the shipped decisions at the boundaries of this composition.

`RecoveryProjection.checkpointFidelity` derives the meaning of a genesis or restored checkpoint from a `CheckpointReplay` witness over the chosen prefix.
`RecoveryProjection.suffixFidelity` derives the effect of the selected typed suffix by induction over `TypedReplay`.
A modeled transaction contributes its decoded event, maintenance has no logical effect, and a chosen control uses the committed erasure interpretation.
The split tracks the decision count and checks its projected data cut against the held checkpoint cut.
`recovery_read_is_exact_history` joins the two folds.
The [typed replay closure](../../verus/replay_closure/README.md) gives the native dependency set and the [ordered recovery cursor](../../verus/ordered_recovery_cursor/area.json) checks exact recovery coordinates.
The checkpoint and suffix witnesses require the native archive and driver to realize those logical effects.
Equal dependency sets can coexist with different logical contents.

The Lean controls cover a wrong decision position, a changed chosen debit, wrong key or ciphertext, a modified detached digest, an alien journal cut at the same frontier, a foreign placement, a stale admission head, and a missing event on either side of the checkpoint split.
The reachability witnesses include a detached transaction selected through two cohort repairs and a chosen writer replacement, and a nonempty restored checkpoint containing the chosen transaction.

## R14 · Installed replication driver

The [replication driver study](REPLICATION-DRIVER.md) checks step-or-stutter refinement for the resolved-transaction lane and projects the original chosen receipt for PX1 under explicit provider premises.
It also checks five recorded Rust driver executions and three rejected controls against the executable Lean transition function.

## Hypotheses and evidence

A named Lean witness establishes the result in the reference model.
The native evidence below identifies the relevant slice of an open production refinement, unless the row identifies a particular Verus guard.
PX3 and H4 check the Lean bindings and the shipped decisions listed above.
The inventory identifiers have their definitions in Logkeeper's `verification/formal-inventory.md`.

| Lean hypothesis or remaining obligation | Reference discharge, production evidence, or environment premise |
| --- | --- |
| `ChosenAppend`, `DecodeSource`, `NativeAppend.Binding` | L2, R12, X2. Lean derives the modeled receipt insertion and decoded acknowledgement. PR12 covers exact read-back and the successful-apply decision. H4 checks exact result guards, selected ciphertext and grant, key identity and detached decode. R14 derives a receipt and native binding in its asynchronous model given the exact provider certificate and resolved frame. The authenticated result and replay page must project to the actual certified decision and payload. Key-service opening, exact-version provider redemption and the logical interpretation of resolved transaction bytes remain premises. |
| `chosenPath`, including each `Protocol.Evidence`, `Transfer.exactImage`, and seal/install ordering | L1, L3, L4, L11, L12, X2. The Lean agreement and transfer lemmas give prefix persistence. Logkeeper `a_surviving_group_recovers_the_grant_and_preserves_the_paid_append_window` and RTDB `funded_control_reconciles_lost_appends_and_releases_lost_reads_through_trim_and_repair` exercise recovery, repair, and receipt identity. Mapping ordered reducer decisions, compacted images, durable replies, and authenticated repair into the same ledger remains open. |
| `complete`, `chosenEntry`, `retained`, `roster`, `ab`, `ac`, `bc` | L11 and environment. `JournalRefinement.any_recovery_quorum_can_read_exact_entry` supplies the intersection proof. Deployment must qualify three distinct current voters on three independent AZs and a complete cut of durable copies at every installation, including repaired cohorts. The actual retained record bytes must project to the exact suffix selected for this read. Qualify by losing each AZ at commit, publication, and repair boundaries and reopening the surviving media. |
| `journalPath`, `journalInvariant`, and `read.admitted` | L1, L5, R2, R4, R10, X3. `OrderedJournal.protected_through_trace` proves the modeled floor and hold invariant and `CanServe` provides application readiness. RTDB `replica_admission` checks displaced terms and sequence classification. `a_journal_fenced_term_opens_without_a_claim_or_maintenance_record_across_replacements`, `a_worker_without_the_current_grant_never_leads_a_journal_fenced_term`, and the installed lost-read/repair test are native slices. Mapping the full grant, fresh barrier, hold release, and trim transitions remains open. |
| `journalPath`, and `after.observed < …expiresAt.next` for reissue | L2. `RecoveryComposition.recorded_append_reissues_while_live` discharges receipt preservation under the modeled trace. The installed lost-append/repair test and Logkeeper's `ordered_admission` retention and fingerprint guards are native slices. Receipt expiry limits exact retry. Durable visibility uses the chosen history independently of this horizon. |
| `read.barrier.projected`, `read.admissionPath`, `read.ordered` | R4, R12, X2, X3. Given the projected prefix and continued journal trace, Lean derives `headAtAdmission`, `journalAgreesAtReadCut` and `sameInstallation`. The authenticated Logkeeper reducer and transport must produce the prefix and barrier witness. Native catalog coordinates still need to identify that publication within the same attachment. |
| `exactScopeCut`, `selectedRoot`, `authenticPlan`, `checkpointInHistory`, and `typedReads` | R1, R10, X3. `ReplayClosure` and `ReplayCertificate` discharge modeled read coverage from that projection. `inline_upload_replay_reads_only_its_typed_dependency_closure` and `oversized_upload_replay_reads_only_its_typed_dependency_closure` are native conformance slices. Exact binding of all provider versions, decoded bodies, digest-bound adopted catalogs and the selected checkpoint interval remains open. |
| `ready : ZoneReady …` and `lost` | `RecoveryCertificate.zone_placement_refines_the_provider_contract` turns per-zone placement into held-version coverage. The environment must qualify survivor readability, exact provider identity, inventory, key availability, and no correlated AZ loss. Physical packs sharing one failure domain count once. Probe exact versions, including encrypted objects and provider allocations, while each AZ is unavailable. Certification of every protected root and retained suffix in production remains open. |
| `withdrawalTrace` and `registered` | L7, R7, X3. `ObjectWithdrawal.trace_preserves_separation` proves the per-object invariant from the trace. Logkeeper `exact_object_withdrawal_survives_compaction_and_cohort_repair` checks a durable fence. RTDB `a_retained_offloaded_payload_remains_replayable_after_its_delete_request` checks candidate isolation. Complete registration of every exact held provider version, preservation through compact/repaired state, and physical effect after fence, reader drain and exact-version match remain refinement obligations. |
| `mediaTrace`, `eligible`, `rules`, `durable`, `chosen`, `sealLater`, `sealing`, `repairs`, and `selected` | L10, L11, L13, R5. `SelectionFence` proves rank preservation and authenticated seeded selection in the model. Logkeeper `rotation_fences_both_slots_across_changed_majorities_and_repair` and RTDB `either_old_selectable_slot_blocks_commit_and_leaves_the_view_readable` exercise the media and evidence checks. Qualification must cover file and directory sync on deployed media. Authenticating both selectable slots at the exact fence and projecting the selected image to the successor root/generation remain production obligations. |
| `committedTargets` and the absence of a later covering commit | D02, L9, R5, R6, R11. Only a durable covering erasure commit, with its exact-head clean successor and irrevocable deadline, can interpret a control as removing the event. `exact_head_rotation_survives_lost_rotation_and_commit_replies` and `a_retained_record_uses_its_own_key_until_that_generation_expires` are native slices. Exact survivor rewrite, recovery using only the clean root and new key generation, and qualified atomic custody/deadline authority are open refinements. |
| `checkpointAtCut`, `checkpointReplay`, `suffixReplay`, `publicationMeaning`, `readUsesReplay` | R1, R11, R12, X2. Lean derives the checkpoint and suffix equalities from typed execution witnesses. Native archive serialization, survivor rewrite, effectful driver replay and catalog publication must realize those witnesses. SQL and temporal query semantics remain separate. |
| `MaintenanceBound.SettlementReaches receiverBudget lane` and paid capacity | L6, R3. The checkpoint projection retains Study 111's three-record theorem and per-term-claim control. The extension budgets one unit per outstanding reservation or retained fenced terminal. The byte translation requires funding both counts at the maximum framed record charge, including exact references, and separately funding checkpoint/control operations. Peak disk funding, the bound on successive receiver obligations and progress under repeated interruptions are deployment obligations. |
| `bounded`, `dispose`; `admittedBeforeRetirement`, `clockBound`, `admission`, `completionBound` | L8, R4, R7. Study 113 and `ReaderRetention` prove the two time inequalities. RTDB `read_lease` verifies the ordinary storage admission and grace decision leaves under clock and duration bounds. Installed `expired_withdrawn_worker_disposal_releases_the_read_that_blocks_trim` exercises disposal and trim. The installed serving/protection path must enforce the lease and settle or drain operations admitted earlier. Qualify clock rate/error and duration across restart, retirement, and loss of connectivity. |
| Installed scope and trust environment | D03, D05, L3, L12, R9. Qualify authenticated identities, transitively retained verification keys, signatures and hash binding, deterministic canonical decoding under Prost, and installed-only routing. The framing Verus area, installed admission tests, and bypass mutations cover local decisions. Environment qualification includes corrupted-media/codec tests, verifier vectors, and fail-closed provider version and conditional mutation probes. |
| PX2 `HasMajority`, eligible nomination and activation capacity | D03, D06, L11 and R2. Qualify authenticated reachability of both surviving current voters after loss of each AZ and keep one paid recovery slot for an eligible replacement. `no-majority`, `no-candidate`, `no-activation-slot`, `unfair-grant`, `unfair-replay` and `unfair-confirm` stall the corresponding grant or readiness law. Probe the exact checkpoint and held suffix on the surviving provider under fault. `no-provider` stalls after grant. |
| PX2 `reached`, `retainedBound`, `nextBound`, `funded` | L6 and R3. The extended phase bound covers eligible appends across arbitrary modeled replacements and settlements. Qualify maximum framed and reference charges, the funded maintenance reserve and checkpoint allocation. `unfunded-lane` reaches a boundary whose upload cannot fit. `unfunded-checkpoint-copy` blocks the prepared copy with a funded lane. `unfair-settlement`, `unfair-boundary`, `unfair-upload`, `unfair-prepare`, `unfair-copy` and `unfair-publish` stall publication. The checkpoint projection needs reuse of selected progress records across replacement and effective trim before the next cycle. Abandoned chosen attempts that consume extra records remain outside the projection. |
| PX2 live custodian, hold settlement and trim | L5, L8 and R10. A runnable custodian can copy and publish the selected root and trim once the withdrawn and overlapping holds clear. `no-custodian` halts after publication, `holds-never-release` leaves a pin in place and `unfair-trim` leaves trim unchosen. Qualify maintenance over an idle journal and at full ordinary capacity with overlapping holds and lost responses. |
| PX2 `advances`, `boundedLag` and expiry disposal | Study 113 and L8. Real time must reach the final lease end plus drift plus worst-case journal lag. `unfair-tick`, `unbounded-clock` and `unfair-dispose` stall removal of a withdrawn enrollment's hold. Qualify clock behavior across outages and restart, and repeated custodian scans and expiry decisions for an idle journal. |
| PX2 usable renewal, reader slot and fair fresh admission | D01 and R4. The serving grant must stay current for the invocation and the journal must leave a reader slot and return a usable renewed lease. `writer-displaced`, `no-reader-slot`, `no-usable-renewal` and `unfair-renew` stall read admission. `weak-read-fairness` allows renewal and expiry to alternate forever without choosing the fresh barrier. Production qualification must measure renewal round trips and exact protected read admission under contention and loss. |
| PX2 clean images, qualified deadline authority and headroom | D02, Study 112, W4 and R5. The model starts from a prepared, quiesced clean successor and authenticated reservation. `stale-slot` and `unfair-mirror` leave the requested erasure short of clean evidence. `unqualified-authority` and `unfair-commit` stall after both slots are clean. `short-deadline` stalls after a clean, usable reservation expires. Qualify exact slot readback and enough time to complete the authenticated commit, drain, destruction and evidence tail; PX2 checks eventual commit only. |

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
