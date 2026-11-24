# Installed owned-read custody

The proof imports the production constructors from `rust/crates/multi/src/journal/owned_read.rs`.
`journal::hosted::storage::snapshot::Source::retain` uses `protect_snapshot` to request the exact root and half-open replica suffix.
`InstalledJournal::release` uses `release_on_close` for an admitted hold, including holds dropped by Flight resource cleanup.

| Rust function | Lean reference in `verification/multi/compose/InstalledRead.lean` | Law |
| --- | --- | --- |
| `protect_snapshot` | `protectSnapshot` | `protected_snapshot_names_its_exact_scope` |
| `release_on_close` | `releaseOnClose` | `close_releases_the_original_hold` |

The Lean root is an opaque identifier.
The Verus harness models a hold as an opaque value with its identity and interval; the production constructor passes the whole Logkeeper hold.
The existing `installed_adapter` and `installed_read_selection` areas cover the fresh barrier, exact invocation, and uncertain release horizon.
`releaseAfterLostBarrier` names the corresponding Lean cleanup operation.

The snapshot control substitutes a `Barrier` for `ProtectCheckpoint`.
The close control substitutes route inspection for `Release`.
The Lean reference includes witnesses that both substitutions are distinct from their respective required commands.
The named Verus laws also reject each substituted implementation when its local postcondition is weakened accordingly.

The proof covers command construction.
Flight dispatch, handle lifetimes, RPC delivery and retry, and provider custody drain are outside the Verus model.
