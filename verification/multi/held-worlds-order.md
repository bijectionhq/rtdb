# Ordered hosted actions

[HeldWorldsOrder.lean](lean/HeldWorldsOrder.lean) projects the hosted actions onto the chosen data cuts of [OrderedJournal.lean](lean/OrderedJournal.lean).
It distinguishes the source position from the assigned system revision.
The projection covers the five admitted Flight actions, including the kind-6 primary and constraint-refusal outcomes and the hosted staging refusal.
A publication requires equality with the installation authority at its ordered predecessor.
The publication contains one marker per submitted live context at its assigned revision with the submitted fingerprint.
The recovery laws preserve the chosen cut through every modeled journal step and exclude a different cut digest at the same index in the same history.
These model laws leave the correspondence from native recovery to the modeled chosen cut open.

[The Verus area](../verus/held_worlds_order/proof.rs) checks the shipping equality, refusal selection, staging cutoff, marker byte copy and equality, event revision, and source order decisions.
The [laws](../verus/held_worlds_order/laws.rs) use those implementations by path.
The [defect controls](../verus/held_worlds_order/area.json) change the shipped decision bodies.
The bindings below identify the async callers, codec, catalog, and journal work that the harness does not compile.

## Native correspondence

| Flight action | Resolver input and effects |
| --- | --- |
| `DecideHosted.v1` | `commit_hosted_with_reads` appends the decoded hosted controls to the primary operation list. A cutoff selects a strict durable predecessor and a failed staging check discards the candidate. An optional constraint alternative is separated at `HostedConstraintRefusal`. |
| `DecideHeldWorlds.v1` | `decide_held_worlds_action` sends the decoded primary controls, world sections and one fingerprint through `PlacedWorldHost::commit_world_batch` to `commit_owned_world_batch`. Kind 6 shares the Proposal section's attempt with the Root and emits one embedded marker on success. Its refusal uses that same context and fingerprint and retains the Proposal. |
| `PublishHeldWorldRetention.v3` | `publish_held_world_retention_action` passes decoded check-only controls and the exact attempt and fingerprint to `PlacedWorldHost::fork_world_checked`. |
| `PublishWorldCut.v1` and `DecideWorldCut.v1` | `publish_world_cut_action` carries the original controls, attempt and fingerprint through `fork_world_checked` to `register_owned_world_checked`. Only the decide form can carry a Root publication and a paired refusal. |

`decode_hosted_control_groups` requires an Int64 expectation column and decodes a true check flag through `hosted_control_row`.
The source operation preserves the check variant and expected stamp across `source_header` and `SourceTransaction::decode`.
`TxResolver::resolving_snapshot` combines the durable snapshot with its ordered staged queue and checks the resulting basis against its head.
`check_hosted_controls` looks up the exact table and ID in that base snapshot and compares the row's physical revision or covered absence with the decoded expectation.
The primary loop starts after preconditions succeed.
On constraint refusal, the resolver drops the primary buffer, reopens the same base, selects the original primary checks, and calls `apply_constraint_refusal`.
That method checks its preconditions before applying the refusal operations.
The staging deadline can refuse that selected transaction after resolution but cannot replace its checked predecessor.

`reserve_owned_attempt` and the node's refusal builder use the same marker document constructor.
The kind-6 refusal additionally compares the complete marker ID and fingerprint with the embedded primary publication before use.
Other hosted refusals match the original submitted marker rows, including full payloads for explicitly supplied markers.
The attempt relation requires create-only rows carrying the immutable fingerprint over all valid time.
`put_documents` uses the buffer's system revision and `TransactionBuffer::stage_event` requires every staged event to use that revision.
A model marker represents an accepted event after these operations and before any later retention expiry.
The remaining binding is from the admitted context list and fingerprint slot through native source encoding, chosen replica batch, and exact held-row decoding.

`resolve_next_source` advances the source watermark and uses that source position as the source transaction's ID.
The resolver enforces strict advances in source transaction ID and system time before staging.
The claim replaces an ordinary hosted control at the claim's assigned system time, which `read_hosted_control_point` recovers from the exact epoch slice.
`ordered_opened` derives the new intake start from the selected checkpoint and replica tail of the installation and seeds recovery on the same log epoch.
`promote_term` opens that source tail from applied progress and seeds the resolver with the same applied transaction key and source position.
The remaining proof must tie those recovered bytes, their catalog and source watermark, the Bijection claim receipt, and the resolver snapshot to one certified installation prefix.
Comparing numerically equal offsets from separate installations would leave that proof open.

## Authority row omission

An authenticated raw Flight caller can supply an arbitrary eligible check-only control to several of these actions.
The RTDB action shapes alone do not require `bj_installation/authority` on every request.

1. Establish an otherwise admissible `DecideHosted.v1` Root edit with an unchanging checked ordinary control and an installation-authority check at revision 10.
2. Order an installation owner claim at revision 11 on the same placement while leaving the other control and affected fact unchanged.
3. The original request with both checks conflicts at the ordered predecessor.
4. Repeat from the history immediately after the claim with a fresh attempt and the same edit, but omit the authority group and retain the other checked control.
   `commit_hosted_with_reads` admits the remaining group and `check_hosted_controls` compares only the supplied control.

Bijection's configured native callers must discharge authority-row presence and exact wire transport before this argument can cover every writer they issue.
Mandating this particular application row in RTDB would require a design choice between a typed installed authority policy and the existing caller-owned check contract.
This change preserves the existing caller-owned contract.
The affected owners are the configured hosted Flight gateway and Bijection's installation-writer guard construction.

The scope of the transaction proof is the published native Root, world and landing-control batch.
`fork_checkpoint_world_selected` can reserve directory custody before `register_owned_world_checked` orders the native registration.
An authority conflict can therefore leave that prerequisite custody reservation while refusing native registration.
A guarantee about those directory effects requires a separate ordering and cleanup contract.
