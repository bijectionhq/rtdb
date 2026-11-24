# R3 · Fenced maintenance classification

The harness verifies the classifier called by `PartitionLogs::enqueue_replica_with`.
The generated replica body is projected to its variant and, for `NoOp`, its optional source message ID and whether it carries a placed receiver reservation.
The exhaustively matched Rust variants correspond to the generated `ReplicaLogMessage.message` variants.
Block boundaries and uploads project to the corresponding Lean kinds, no-ops project to `.noOp`, and all other variants project to `.ordinary`.
The append path queries the fence mode for no-ops, the only variant whose classification depends on the mode.
The append path chooses the class from the original body and reuses it if the payload is offloaded.

[MaintenanceClassification](../../multi/lean/MaintenanceClassification.lean) imports the pinned Logkeeper Study 111 lane model `MaintenanceBound`.
The Verus law `classifier_refines_three_record_bound` equates the production charge to Lean's `record_charge` for fenced bodies outside placed receiver transfer settlement.
The production classifier also charges fenced receiver terminals to maintenance.
The bound takes the absence of those terminals as a premise.
The bound follows from Lean's `Shape` and `maintenance_append_allowed`.
The idle phase starts with the previous upload, a pending boundary raises the count to two, and its upload raises it to three.
The Lean theorem `grant_fencing_bounds_maintenance_by_three` covers arbitrary finite modeled histories.
A regrant leaves the count unchanged, and publishing and trimming returns the lane to idle with one record.
`three_records_are_reachable` witnesses that the bound is tight.

The classifier proof covers charges in records.
The phase and trim schedule is a premise when applying the law to the running system.
The driver's block-cut guard observes the published checkpoint, and the custodian advances the retained floor separately.
An overlapping hold can delay that advance after publication.
Provider progress, holds that delay trim and reserve sizing in bytes are outside the classifier proof.
