# Ordered hosted actions

[HeldWorldsOrder.lean](lean/HeldWorldsOrder.lean) projects the hosted actions onto the chosen data cuts of [OrderedJournal.lean](lean/OrderedJournal.lean).
It distinguishes the source position from the assigned system revision.
The projection covers the six Flight actions, including the kind-6 prepared switch and constraint refusal and the hosted staging refusal.
A publication requires the current installation claim revision, incarnation and writer grant at its ordered predecessor.
The publication contains one marker per submitted live context at its assigned revision with the submitted fingerprint.
The recovery laws preserve the chosen cut through every modeled journal step and exclude a different cut digest at the same index in the same history.
These model laws leave the correspondence from native recovery to the modeled chosen cut open.

[The Verus area](../verus/held_worlds_order/proof.rs) checks the shipping prepared switch and clock-slot selector, equality, refusal selection, staging cutoff, marker byte copy and equality, event revision, and source order decisions.
The [laws](../verus/held_worlds_order/laws.rs) use those implementations by path.
The [defect controls](../verus/held_worlds_order/area.json) change the shipped decision bodies.
The bindings below identify the async callers, codec, catalog, and journal work that the harness does not compile.

## Native correspondence

| Flight action | Resolver input and effects |
| --- | --- |
| `DecideHosted.v4` | `commit_hosted_with_reads` appends the decoded hosted controls to the primary operation list. A cutoff selects a strict durable predecessor and a failed staging check discards the candidate. An optional constraint alternative is separated at `HostedConstraintRefusal`. |
| `DecideHeldWorlds.v3` and `SwitchPreparedProposal.v2` | Both enter `decide_held_worlds_action`. Kind 6 requires the prepared action and its original offered clock slot. The action sends the primary controls, world sections and one fingerprint through `PlacedWorldHost::commit_world_batch` to `commit_owned_world_batch`. The prepared switch shares the Proposal section's attempt with the Root and emits one embedded marker on success. Its refusal uses that same context and fingerprint and retains the Proposal. |
| `PublishHeldWorldRetention.v5` | `publish_held_world_retention_action` passes decoded check-only controls and the exact attempt and fingerprint to `PlacedWorldHost::fork_world_checked`. |
| `PublishWorldCut.v2` and `DecideWorldCut.v2` | `publish_world_cut_action` carries the original controls, attempt and fingerprint through `fork_world_checked` to `register_owned_world_checked`. Only the decide form can carry a Root publication and a paired refusal. |

`decode_hosted_control_groups` requires an Int64 expectation column and decodes a true check flag through `hosted_control_row`.
Flight binds the required typed authority prefix to the single primary check for the installed authority.
The source operation preserves the check, expected stamp, incarnation and writer grant across `source_header` and `SourceTransaction::decode`.
`TxResolver::resolving_snapshot` combines the durable snapshot with its ordered staged queue and checks the resulting basis against its head.
`check_hosted_controls` looks up the exact table and ID in that base snapshot and compares the row's physical revision or covered absence with the decoded expectation.
The primary loop starts after preconditions succeed.
On constraint refusal, the resolver drops the primary buffer, reopens the same base, calls `apply_selected_refusal`, and retains the original primary writer check.
That method can omit a scope-only world check when it has no corresponding observation.
The installed authority check is shared with the alternative.
It runs the selected preconditions before applying refusal operations.
The prepared-generation candidate's own validation and installation still need a proof from the checked predecessor through the exact chosen replica body.
The staging deadline can refuse that selected transaction after resolution but cannot replace its checked predecessor.

`reserve_owned_attempt` and the node's refusal builder use the same marker document constructor.
The kind-6 refusal additionally compares the complete marker ID and fingerprint with the embedded primary publication before use.
Other hosted refusals match the original submitted marker rows, including full payloads for explicitly supplied markers.
The attempt relation requires create-only rows carrying the immutable fingerprint over all valid time.
`put_documents` uses the buffer's system revision and `TransactionBuffer::stage_event` requires every staged event to use that revision.
A model marker represents an accepted event after these operations and before any later retention expiry.
The remaining binding is from the admitted context list and fingerprint slot through native source encoding, chosen replica batch, and exact held-row decoding.

`resolve_next_source` advances the source watermark and uses that source position as the source transaction's ID.
The shared stage checks strict source transaction ID advance and nondecreasing system time.
Owned publication selects a strict revision before staging: automatic selection advances a stalled clock, an explicit owned revision must advance, and an offered slot must be strictly later than the selected predecessor.
The verified source-order and strict-time decisions are distinct, and the projection concerns a published owned action.
A shared strict stage would reject an ordinary SQL transaction at source 12 and time 10 after source 11 at time 10, even though the ordinary history permits that tie.
At the same predecessor a placed publication offered time 10 is refused, while automatic owned publication can select 11.
Keeping time selection with its existing owners preserves both histories.
The claim replaces an ordinary hosted control at the claim's assigned system time, which `read_hosted_control_point` recovers from the exact epoch slice.
`ordered_opened` derives the new intake start from the selected checkpoint and replica tail of the installation and seeds recovery on the same log epoch.
`promote_term` opens that source tail from applied progress and seeds the resolver with the same applied transaction key and source position.
The remaining proof must tie those recovered bytes, their catalog and source watermark, the Bijection claim receipt, and the resolver snapshot to one certified installation prefix.
Comparing numerically equal offsets from separate installations would leave that proof open.

## Installed authority and fork custody

The hosted commit actions require a typed authority prefix and an exact matching check-only control for `public.bj_installation/authority`.
The receiver derives placement scope from the authenticated attachment and refuses a missing, repeated, replaced or inconsistent authority check.
The ordered resolver selects that row independently of the caller's other controls.
It compares the row's physical stamp, incarnation and active writer grant at the selected predecessor before applying any primary or alternate effects.
The other checked controls cannot authorize a request for a replaced writer.
Generic placed Flight fact writes are limited to private layout readiness in this hosted gateway.

The directory precedes fork registration with a provisional pin claimed by one attempt.
A local refusal before submission can release that claim by an exact directory comparison.
An ordered result used to settle the pin requires the original durable source coordinate under the same hosted history.
A committed constraint alternative keeps its receipt and releases the provisional pin when the held catalog covers the original decision and its exact revision added no registration.
A conflicting row at the same revision leaves the pin unsettled.
A registered pin survives a competing refusal, and refusal preserves the predecessor.
[The pin decision](../verus/held_world_fork_pin/laws.rs) checks the command's original pin, reservation, incarnation and log epoch.
The caller also compares the entire retained binding and retries the directory version compare-and-set.
An indeterminate submission or unsuccessful directory update keeps custody.
Replacement recovery still needs an owner that can rediscover the original disposition from a retained reservation and finish its release.
