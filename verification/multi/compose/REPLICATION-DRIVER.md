# R14 · Replication driver refinement

[ReplicationDriver.lean](ReplicationDriver.lean) models the resolved-transaction lane of one installed, journal-fenced RTDB replica stream.
The worker starts at the empty base and may process any finite number of submitted writes, provider operations and restarts admitted by the model.
The model uses natural numbers for request identities, decision positions and journal cuts, and checks signed 64-bit exhaustion for the local term and sequence counters.
The journal's activation capacity is a parameter with a default of 1,024 for the executable fixtures.
An ordered release can free an activation slot.
The checker places no bound on trace length.

## Refinement

The abstraction projects `State.journal` and hides the worker, its submitted provider flights and its chosen-entry book.
A provider `choose` constructs one `NativeAppend.ChosenRecord`, projects it with `NativeAppend.nativeAppend`, and uses `Composition.projectAttempt` and `Composition.projectReceipt` to update the reference journal.
The choice requires the original grant to satisfy `OrderedJournal.Current` at ordering and the next data cut and term sequence to agree with the chosen prefix.
Decision positions can skip over journal control decisions.
A grant replacement projects to `OrderedJournal.Step.grant`.
Hold release, receipt expiry and availability project to the corresponding reference steps.
All worker actions, including a delayed or lost reply, leave the durable journal projection unchanged.
`step_refines` proves the step-or-stutter result for every admitted action.
`run_refines` lifts it to every finite execution.

An exact read-back carries `NativeAcknowledgement.ExactReadback` evidence into the application state.
Application starts after admission and reaches published progress only through `applyFinished` with the selected transaction identity.
`acknowledge_after_apply` derives the `nativeAcknowledgement` result.
`route_once` proves that no intervening action, including a crash, can reopen the logical completion identity after routing.
The model can receive and apply the chosen frame before provider enqueue returns.
The guarded append path keeps the appender occupied until the durability reply, while independent ordinary durability replies can arrive out of order.
Provider or effect failure spends the affected channel, and a successful follower replay publishes progress without a caller to acknowledge.

`acknowledgement_projects_for_PX1` derives `Composition.RecordedAppend` and `NativeAppend.Binding` for every positive route from the model's initial state.
It locates the receipt at the original choice and supplies an `OrderedJournal.Trace` from that state to the acknowledgement state, including any intervening receipt expiry.
The theorem takes two provider premises.
`Certificates` links a delivered choice to an authentic `Protocol.Seal` with that exact append at its decision position.
The frame-opening premise binds that append to the exact resolved event under the caller's interpretation of the transaction identity.
Exact receipt reissue still requires the horizon in `RecoveryComposition.recorded_append_reissues_while_live`.

## Native action boundaries

The following are source correspondences and premises for a Rust-to-model refinement.
The existing [replica admission](../../verus/replica_admission/area.json), [maintenance classification](../../verus/maintenance_class/README.md), [read-back identity](../../verus/readback_identity/area.json), [native acknowledgement](../../verus/native_acknowledgement/README.md) and [ordered recovery cursor](../../verus/ordered_recovery_cursor/area.json) Verus areas cover decision functions.
Proof of their composition through the Rust await points remains open.

| Action | Rust boundary still to discharge |
| --- | --- |
| Empty read, grant observation, promotion | `run_inner` and `lead_after_empty_read` route an empty batch and await `holds_writer_grant`. Prove that the recognized retained prefix, transferred-debt check, grant result and next term survive the await boundaries in `promote_term`, including source tail open and recovery confirmation. A local promotion can consume an observation that became stale before ordering. |
| Enqueue and dispatch | `enqueue_resolved`, `LeaderState::enqueue`, `ReplicaAppender::run` and `PendingWrite::stamp_append` must link the resolver head, queue order, one-shot identity and exact pre-provider stamp. Prove which operations may still choose after `observe_enqueue` loses its future to cancellation or an earlier receipt error. |
| Provider choice, return, durability and refusal | `PartitionLogs::enqueue_replica_with`, the installed adapter, sealing and Logkeeper must supply the chosen cut, correct original grant, complete references and authentic decision position. Prove that provider enqueue completion and `track_receipt` observe their exact requests, including detached acceptance, lost replies, guarded flight exclusion and out-of-order receipt resolution. The trace fixture uses `MemoryLog` with a gated provider and supplies synthetic decision metadata. |
| Read-back and replay | The tail poll, `read_replay_head`, decode, `TermFence::admit_record`, `resolve_payload` and `validate_own_readback` must bind the consumed and opened bytes to the admitted choice. For a local receipt, prove that the pending identity selected by term, sequence and fingerprint is the same logical request represented by the model's id. This includes ruling out aliasing on grant reuse or an appender restart. |
| Application and published offset | `apply_admitted` must select the same staged or reconstructed transaction, bind the resolver head, complete search, partition and catalog effects, publish that result through `ProgressMonitor::applied`, and advance the recognized prefix. Checkpoint restoration, replay from a nonzero floor, cross-cohort transfer and interpretation of Arrow and catalog effects remain separate obligations. |
| Acknowledgement and loss | Prove that `complete_record`, `PendingWrite::finish`, its destructor, `fail_pending` and cancellation race on the same mutex-protected sender. The Lean proof covers a single route per modeled request. The receiver can close before the send, and caller delivery and fairness remain runtime obligations. |
| Crash, demotion and restart | `run`, `demote`, appender join and the reopened provider must drop speculative work, preserve chosen records and rebuild the exact cursor. The model replays from the empty base and uses a separate action for ordered grant replacement. The native restart fixture reconstructs a driver on the same in-memory log. |

The model omits source resolution, legacy term claims, source and command polling, receipt-free controls, heartbeats, block boundaries and uploads, checkpoint write and adoption, the held prefix during resolution or block production, compaction, trie and table retirement, placed slot reservation and settlement, and capacity-cut demotion and retry.
A rejected input represents a disallowed model transition.
The model covers delivered bytes that fail the local fingerprint or resolved-body check at the expected record identity.
Other adversarial provider effects, such as forging a chosen decision, changing the replay floor, or returning an alien term as an exact chosen position, require the provider and adapter premises above.
Liveness is outside this safety refinement.

## Executable evidence

Run `python3 verification/multi/run_driver_traces.py` from the repository root with the guarded Cargo wrapper available.
`--trace-dir DIR` rechecks a capture without invoking Cargo.
The [native fixture](../../../rust/crates/replication/src/driver_trace_tests.rs) records the stamped and observed SHA-256 values, term, sequence, local request, transaction and raw replica offset.
It gates provider ordering and return independently and observes receipt, progress, term and fault effects from the real driver and appender.
[The checker](../check_driver_trace.lean) runs the same `step` used in the proofs and checks the resulting cursor, head, fault status and number of acknowledgements.
It normalizes the fixture's raw offset to a next-position cut and has no quorum certificates to verify.

[ReplicationDriverTraces.lean](ReplicationDriverTraces.lean) has executable reachability witnesses for replacement mid-append, read-back mismatch, out-of-order durability replies, restart replay, positive read-back before the provider reply and acknowledgement after receipt expiry.
It checks three rejected schedules.
The trace runner also injects these events into the Rust captures and requires the first rejection to occur at `acknowledge_after_apply`, `current_writer_at_choice` or `route_once`, respectively.
