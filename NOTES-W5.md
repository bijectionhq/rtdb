# W5 handoff

## Shared wire types

Use `rtdb_types::rtdb_v1::{Basis, Branch, Layer, At, Claim, ClaimScope, ClaimSealer}`.
`Basis` has a 24-byte opaque encoding scoped to a database incarnation.
`Claim::from_bytes` carries a token through the wire, and `ClaimSealer::seal` and `open` share the authenticated encoding between Query and Transact.
Pass the same stable, incarnation-scoped 32-byte secret to both.
The first footprint conservatively covers the entire selected branch and layer.
Only issue a claim after opening the actual query basis, and authorize the query and transact independently of the token.

## Landing trace

| Existing path | Composition |
| --- | --- |
| PublishWorldCut modes 0–3 | Fork at the observed basis, with a writable flag and Bijection rows for logical or proposal time. |
| Modes 4, 5, 10, 11, 12 | Fork at the selected Root publication, with guards and publication or custody rows in the same Transact. |
| Modes 6, 8, 9 | Renew the retained or proposal branch, with the control row replacement, insert or delete in the same Transact. |
| Mode 7 | Fork a sibling at the predecessor's base, with a distinct expiry and create-only custody row. |
| Retained advance in modes 4 and 11 | Fork the newer basis under a new branch identity, swap the application pointer, and drop the predecessor if appropriate. |
| DecideHeldWorlds effects 0–4 | Put or Delete on the addressed branch, update the application clock row, drop, settle the proposal row, or make an explicit timed write with a guarded clock-row update. |
| Root kinds 0–2 | Apply branch-only effects, add Root observation guards, or add Root row writes and an attempt row in the same transaction. |
| Root kinds 3–4 | Drop retained custody and settle its control rows, optionally deleting the guarded Root row. |
| Root kinds 5–7 | Write or settle a proposal overlay, land the reviewed Root effects and drop the proposal, or close custody and delete the associated Root row. |
| Independent observations | Include claims on the separately observed branches in the same transaction. |
| SwitchPreparedProposal | The consumer prepares explicit Root writes, compares the parent and proposal observations, drops the proposal on success and installs receipt and proof rows. Use explicit Put and Delete with those guards, or Merge when the intended result is the untransformed overlay. |

`publish_world_cut_action` selects a cut, validates a predecessor and authority, then calls `fork_world_checked` with Root guards, effects and deadline.
`decide_held_worlds_action` admits each section, independent observations, Root section and optional refusal, then calls `commit_world_batch`.
Bijection's `commit_native_proposal_landing` compares original parent points, pages and counts, includes posting conditions and expects the offered clock's acceptance evidence when it publishes the prepared batch.
RTDB's `FundedMutationClock::admits_terminal` admits exactly one receipt inserted at the offered ID, with the matching request fingerprint and the original 8-byte reading plus 32-byte booking digest, including the refusal path.
Bijection checks the offer issuer, offered reading and lower source cut, and a receiver observation within the offered one-second window.
The data effects and control state can be expressed with operations, guarded rows and placeholders.
The plain window supplies inclusive and exclusive bounds on the writer's chosen system time.
It does not demonstrate the original quorum accepted before an exclusive deadline, attest an offered booking, or provide an advance reservation.
Those are genuine gaps if migration must preserve the existing acceptance or reservation contract.
There is also no wire reference for a fork at the basis of the transaction currently publishing its parent writes, or for a sub-transaction prefix of those writes.

The N4 journal contract and the two-mode decision supersede the original boundary rationale.
The terminal record addresses the remaining quorum-cutoff gap when the mode's original append acknowledgement proves durability and the writer can supply a qualified upper clock bound.
The older offer identity, reservation and booking are still application state.
`SwitchPreparedProposal` also prepares some writes with acceptance-relative validity and others with reviewed fixed periods.
Generic Merge preserves valid periods, so acceptance-relative replay uses explicit Put and Delete operations.
The staged private generation and any staging limit for large replay payloads remain an integration concern.

## Implementation decisions

- `rtdb_tx::transact::TransactState` is a passive fold owned by the selected mode's writer and replay driver.
  Its candidate is private to resolution, and its receipt and row effects publish together on apply.
- `rtdb_index::branches::BranchIndex` holds the branch topology, histories and ordered declarations.
  Its cut-specific `BranchView` is a passive reader value.
  `rtdb_query::branch_snapshot` projects that view into the existing temporal SQL scan.
- `PreparedAppend` is passive original-attempt evidence for the writer's continuation.
  The writer calls `after_original_ack` with a qualified upper clock bound after the original durable append returns.
  A restarted writer instead calls `recover_pending`.
  Settle publishes at the adjacent position reserved for TxBasis.
  A writer whose terminal slot became unavailable may order Refuse at a later position.
  Pending transactions hold no visible effects or receipt and block checkpoint capture.
- For validation, the driver calls `resolve` after capturing its fresh prefix.
  A mode advertises `linearizable` and `timed-transact` only when its driver supplies the corresponding guarantees.
  Query drivers open `At::Latest` after acquiring a linearizable barrier.
  `At::Exactly` names an applied transaction basis, including a terminal timed receipt.
- Branch names remain reserved for the incarnation.
  A descendant keeps its parent basis after the parent expires or drops, and a directly opened expired or dropped branch returns unknown-branch.
  A merge copies effective local valid-time fragments, rebases their written version to the merge transaction, and conflicts on overlapping target writes since the source fork.
  Drop is a separate operation.
- Declarations apply across the deployment.
  TTL gives each subsequent write an expiry measured from transaction time and keeps it shadowing older or inherited values after expiry.
  Count ranges use inclusive lower and exclusive upper byte bounds over a declared binary or UTF-8 column.
  Declared constraints check the merged candidate for each affected branch.
  The first implementation scans candidates and materializes a branch SQL snapshot from the retained histories.
- Checkpoint encoding carries the settled branch histories, receipts, declarations and lineage.
  Erase removes logical row payloads from new views and checkpoints.
  Key rotation, prior-reader revocation and unreadable-by proof are W3 integration work.

## Legacy surface and wave 2

No existing code was deleted.
The legacy `NativeActions` and replay formats still call the hosted resolver.
`tx/src/resolver/constraint_receipt.rs` binds legacy refusal and registration shapes.
`tx/src/hosted_control.rs` and `tx/src/resolver/hosted_control.rs` authenticate installed authority and source replay.
`WorldClockMode` still decodes retained, held and proposal history used by the owned reads, archives and replica replay.
Remove those paths together with their wire and archive consumers after Bijection switches.
The new transaction resolver uses only generic guards, operations and records.

- W1 needs to route the default SharedLog and Multi journal-contract driver through the same resolve and apply functions.
  The driver owns source admission, fencing, envelope encoding, committed read-back, and the terminal append sequence.
  The shared library takes committed positions and records without declaring a second journal interface.
- W4 can decode its envelopes into `Transact`, read `tx_status` after the appropriate barrier, and use `branch_snapshot` and `seal_branch_claim` for branch Query.
  Keep the two feature names disabled on routes that lack the corresponding guarantees.
- W3 needs to carry the fold checkpoint alongside the installed journal or replica cut, route the erase rewrite and release proof through key rotation, and preserve old-basis reads across trim.
- Production integration needs bounded retained history and incremental branch scans, plus native search and spatial conformance on branch-selected rows.
  Query tests exercise SQL aggregate and lineage using the shared scan.

## Verification

- `guarded-cargo.py rtdb test --locked -p rtdb-tx --lib transact::tests` passed all seven W5 regressions.
  Earlier guarded builds exposed range comparison and record decode typing errors, which were fixed before the passing run.
  The passing run reported existing warnings in other code.
- The focused `rtdb-types` claim codec regression passed before the guarded build instruction arrived.
- `guarded-cargo.py rtdb test --locked --workspace` failed during compilation of the unchanged `rtdb-logkeeper` unit tests.
  At `rust/crates/logkeeper/src/client/writer.rs:1413`, the assertion compares the `(i64, i64)` returned by `seal_recover_committed` with an `i64`.
  Both the assertion and signature are present at the upstream base.
- Scope coverage includes both capability configurations against the shared fold.
  Kafka, the new Logkeeper adapter, wire routing, native branch search and spatial ranking, and physical erasure have no end-to-end W5 result.
- No Gradle tasks or Bijection builds or tests ran.
