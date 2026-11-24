# R7 · Exact reclamation and serving-slot retention

The harness imports the Rust decisions called by installed receipt settlement, the S3 provider and directory serving-slot retirement.
The Lean reference is [ExactReclamation](../../multi/compose/ExactReclamation.lean), which imports Logkeeper's `ObjectWithdrawal` and uses `ObjectWithdrawal.order_preserves_separation` for the ordered fence.

| Lean definition | Production decision and call site |
| --- | --- |
| `MatchingObjectFence` | `matches_object_fence` compares every byte of the name and publication fingerprints in `InstalledJournal::withdraw_object` and `InstalledPublicationDeletionGrant::from_journal`. |
| `ReleaseIsCovered` | `release_is_covered` requires a nonnegative receipt cut strictly below the trim floor, which must be at or before the selected checkpoint cut. |
| `ErasureRequiresCustodyJoin` | `erasure_requires_custody_join` selects the durable `joined-custody` lookup for an active erasure in `retirement_admitted`. |
| `ExactVersionRemoval`, `StableAbsence` | `exact_version_status` classifies the complete listing that `retire_version_at_path` uses to decide between issuing the exact delete, settling absence and refusing to expose older data. |
| `TerminalTargetMatches` | `matches_terminal_target` compares the provider address and conditional entity tag for an enrolled never-versioned publication. |
| `RetainsServingSlot` | `retains_serving_slot` distinguishes aliases in the exact loose, packed and local attempt censuses by both origin and attempt ID. |
| `RetainsPublicationScope` | `retains_publication_scope` keeps a shared scope while any co-custodied peer lacks deletion, recorded disposal or an empty set of storage regions. |

The async call order was traced by inspection.
`settle_receipts` checks the selected route, custody gate and installed exact release before requesting withdrawal.
Only a returned, matching `ObjectWithdrawn` receipt can construct `InstalledPublicationDeletionGrant` and enter provider settlement.
The versioned provider checks its namespace and path and sends the same version ID it just listed.
The never-versioned provider conditionally replaces the expected ETag with a tombstone and recognizes the same tombstone token on retry.
The proofs use the provider contracts for listing completeness, immutable version IDs, exact delete and conditional replacement.

Directory per-slot withdrawal uses the exact deletion grant after provider settlement.
The authorized retirement rechecks the alias census under the lineage reservation before committing its slot identity or pack plan.
The local orphan census starts after the attempt's region is removed and preserves aliases even in relocated packs.
Whole-scope withdrawal uses the disposing permit and the last-residence check, with separate authority for a rejected private fork stage.
Completeness and serialization of those censuses and vault execution are call-site premises.

The Lean model has one immutable delete target and one distinct replacement at the same key, one ordered withdrawal state and one abstract reader drain.
Its safety theorem covers traces of arbitrary finite length.
The model reaches a lost delete reply followed by settlement from fresh exact absence, including with a replacement present at the provider layer.
The hosted receipt path can retry from a missing current object and skips a visible mismatched current publication before it calls the provider.
Removing the complete-listing premise allows false settlement, and a path-only retry can remove the reachable replacement.

Ordinary read release retains the Logkeeper hold through tracked provider work.
`DisposeExpired` can remove a withdrawn worker's holds using lease end and clock drift, while `IoRegion` allows already admitted work to outlive its caller's timeout.
The ordinary receipt path can have an erasure floor of zero and goes directly from the Logkeeper fence through key withdrawal to provider settlement.
The active-erasure custody-join proof therefore leaves the general provider-read drain obligation open.
Lean exhibits an admissible lease-expiry time before an unbounded provider request completes.

The pinned Verus baseline checked 17 items and the seven production mutations yielded eight diagnostics at their declared laws.
The mutation harness uses `verification/run.py` and `area.json`.
Run `~/.elan/bin/lake build` under `verification/multi` to check the Lean model and its witnesses.
