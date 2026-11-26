# Journal custody

A logkeeper leaves a Multi cell by drain, balance, copy release and decommission.
The area verifies the shipped decisions that keep a deployment from naming a decommissioned journal and keep a released copy free of dependents, and [`JournalCustody.lean`](../../lean/JournalCustody.lean) proves the custody laws over an abstract model of the cell.

## Verus

The harness imports [`journal_admission.rs`](../../../rust/crates/directory/src/journal_admission.rs), which holds the pure decisions the directory and the release path call.

| Function | Checked behaviour |
| --- | --- |
| `epoch_follows` | A member write that changes its state or decommission needs a later eligibility epoch, and no write takes the epoch back. |
| `fenced_journals` | A deployment write fences exactly the logkeepers it newly names, and is refused when one of them no longer accepts references. |
| `copy_dependent` | A copy leaves custody only when its journal is draining and out of the ensemble, a recorded handoff installed the ensemble, and no other field, checkpoint decision, in-flight handoff, recovery or replay hold, staged fork, deletion or installed reservation depends on it. |

`journal_fence` in [`journal_fence.rs`](../../../rust/crates/directory/src/journal_fence.rs) builds the observations from the whole record's mentions of each logkeeper's identity and address, and both directories hold each fenced member's epoch in the same commit: the local directory under its lock, DynamoDB with a condition check in the same transaction.
`observe` in [`release.rs`](../../../rust/crates/multi/src/admin/release.rs) builds the copy observation from the record, the cell's other records and its installed-journal reservations.
Those projections and the DynamoDB condition are the area's premises.

## Lean

`JournalCustody.lean` models journals with a stage and an eligibility epoch, and deployments that retain journals, serve from an ensemble, have dependents on a copy, and acknowledge entries held by journals or a checkpoint.
Placements land from any earlier observation, interleaved arbitrarily with drains, certification, withdrawal, decommission, handoffs, appends, checkpoints, dependents, releases and disposal.
Over every reachable state it proves:

- no deployment retains a certified journal, so none references a decommissioned one (`no_deployment_retains_a_certified_journal`, `no_deployment_references_a_decommissioned_logkeeper`);
- a copy is released only with no dependent and outside the ensemble, nothing depends on a copy no longer retained, and every acknowledged entry stays in the checkpoint or on the ensemble (`a_release_has_no_dependents`, `nothing_depends_on_a_released_copy`, `every_acknowledged_entry_stays_durable`);
- a decommission lands only while no deployment retains the journal (`decommission_requires_no_custody`).

`an_evacuated_journal_is_released_and_decommissioned` reaches a state where a journal that served a deployment is drained, evacuated, released and decommissioned while the deployment keeps its acknowledged entry.

## Controls

`area.json` declares six source mutations: an eligibility change that keeps its epoch, admitting a certified journal, leaving a newly named journal unfenced, and releasing a copy with a staged fork, before a durable handoff, or while it serves.
`verify_journal_custody_lean` in the runner rejects five model mutations: a placement admitted on a stale epoch, certification without an epoch bump or without its scan, and a release with a dependent or of a serving copy.
