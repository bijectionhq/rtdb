# R3 · Fenced maintenance classification and receiver settlement

The harness verifies the shipped `is_maintenance` classifier called by `PartitionLogs::enqueue_replica_with`.
The generated replica body is projected to its variant and, for `NoOp`, its optional source message ID and placed receiver reservation.
The installed append path decodes the terminal flag for `ResolvedTx` and carries the original classification through payload offload.
The harness supplies the decoded terminal flag and the installed journal's term fencing mode.

[MaintenanceClassification](../../multi/lean/MaintenanceClassification.lean) maps boundaries and uploads to `.blockBoundary` and `.blockUploaded`, no-ops to `.noOp`, resolved transactions to `.resolvedTx fencedTerminal`, and the remaining variants to `.ordinary`.
The Verus law `classifier_refines_record_charge` equates the classifier's result to Lean's `record_charge` for all those inputs.
A boundary, upload or fenced receiver terminal charges one maintenance record.
The controls drop either checkpoint charge, charge a fenced no-op, drop a fenced terminal's charge, and charge a current receiver's terminal.

`classifier_refines_maintenance_bound` uses the same `Shape`, `ReceiverSlot.outstanding`, `maintenance_append_allowed` and `funded_records` definitions as Lean.
The checkpoint projection is the pinned Study 111 lane, with one retained upload and at most one new boundary and upload.
A receiver reservation uses one unit of the receiver budget while the slot is open.
Ordinary completion releases that unit, and a successor's fence converts it into one retained maintenance record until trim.
A regrant can inherit an open slot and preserves its charge across further replacements.
`settlement_preserves_receiver_budget` proves this accounting over arbitrary modeled traces.
`reachable_maintenance_append_fits` proves that an eligible append fits a reserve of three records plus the receiver budget.
`five_records_are_reachable` witnesses two successive inherited slots ahead of the same checkpoint, followed by its boundary and upload.

The driver's boundary defers while a slot is open, so funding its settlement in maintenance keeps checkpoint progress independent of ordinary append headroom.
The model assumes that a deployment can bound the retained settlements plus any open reservation until effective trim.
Successive reservations can accumulate retained settlement charges while the driver admits one open slot at a time.
The model also assumes that the next checkpoint reuses the selected progress records across replacements and that effective trim removes the modeled settlements and leaves one upload.
The driver gates the next cut on publication, while the custodian advances the retained floor separately and overlapping holds can delay that advance.
The Verus harness checks the classifier and its per-append accounting under these premises.
It leaves terminal decoding and dispatch, exact receiver-slot read-back and replay, and enforcement of the budget in the production driver to separate refinement.
The [PX2 byte theorem and bounded progress model](../../multi/compose/README.md#px2--progress-under-stated-premises) state the corresponding funding and progress premises.
