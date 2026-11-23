# Installed custodian retention (R10)

The custodian trims through the exact selected `Cut` only when its LSN is ahead of the floor.
For an ordinary native checkpoint, the exclusive LSN is its inclusive replica boundary plus one.
The trim law preserves the chosen history digest as well as the LSN.

| Shipped Rust decision | [Lean reference](../../multi/lean/InstalledJournalGuards.lean) |
| --- | --- |
| `checkpoint_next_lsn` | `checkpointNextLsn` and `checkpoint_next_lsn_is_boundary_successor` |
| `trim_target` | `trimTarget`, `trim_uses_selected_cut` and `native_trim_stops_at_boundary_successor` |
| `retention_actions` | `retentionActions`, `DisposalBeforeTrim` and `retention_attempts_disposal_before_trim` |

The signed boundary conversion accepts `-1` as the zero successor and rejects a negative successor or signed overflow.
On discovery, the decoded native catalog boundary is compared to the chosen boundary record at `cut.next_lsn - 1`.
On an offered checkpoint, `publish_checkpoint` uses the checked successor to request the exact chosen cut.
`retain_selected` submits the selected cut with the same route's checkpoint identity.
A genesis root or a root installed by erasure can have a different origin, and the exact-selected-cut law still applies.

The shipped agent consumes the verified plan in array order and awaits each operation.
It attempts disposal before the initial retention call and before the branch that may trim after publication.
The dispatch, operation outcomes and Logkeeper state transitions are outside Verus.
A disposal attempt can end in refusal or transport error, or Logkeeper may still have live holds when trim is attempted.
Logkeeper separately checks the selected checkpoint, exact cut and live replay holds before applying trim.
