# R7 · Exact reclamation and serving-slot retention

The harness imports the Rust decisions called by installed receipt settlement, the S3 provider and directory serving-slot retirement.
The Lean reference is [ExactReclamation](../../multi/compose/ExactReclamation.lean), which imports Logkeeper's `ObjectWithdrawal` and uses `ObjectWithdrawal.order_preserves_separation` for the ordered fence.

| Lean definition | Production decision and call site |
| --- | --- |
| `MatchingObjectFence` | `matches_object_fence` compares every byte of the name and publication fingerprints in `InstalledJournal::withdraw_object` and `InstalledPublicationDeletionGrant::after_provider_drain`. |
| `ReleaseIsCovered` | `release_is_covered` requires a nonnegative receipt cut strictly below the trim floor, which must be at or before the selected checkpoint cut. |
| `ErasureRequiresCustodyJoin` | `erasure_requires_custody_join` selects the durable `joined-custody` lookup for an active erasure in `retirement_admitted`. |
| `ProviderDrainCompleted` | `provider_drain_completed` in `ReadDrain::close_and_observe` requires a closed point-read gate and an empty exact-key census. Hosted receipt settlement requires this result before key withdrawal and provider disposal. |
| `HostedReceiptAction` | `hosted_receipt_action` in `settle_receipts` reconciles confirmed exact absence independently of the current head. A visible mismatch without exact absence defers. |
| `ExactVersionRemoval`, `StableAbsence` | `exact_version_status` classifies the complete listing that `retire_version_at_path` uses to decide between issuing the exact delete, settling absence and refusing to expose older data. |
| `TerminalTargetMatches` | `matches_terminal_target` compares the provider address and conditional entity tag for an enrolled never-versioned publication. |
| `RetainsServingSlot` | `retains_serving_slot` distinguishes aliases in the exact loose, packed and local attempt censuses by both origin and attempt ID. |
| `RetainsPublicationScope` | `retains_publication_scope` keeps a shared scope while any co-custodied peer lacks deletion, recorded disposal or an empty set of storage regions. |

The async call order was traced by inspection.
`settle_receipts` checks the selected route, custody gate and installed exact release before requesting withdrawal.
`InstalledPublicationDeletionGrant` requires a matching `ObjectWithdrawn` receipt and a closed, empty census.
Key withdrawal and provider deletion follow closure and an empty per-object read census.
The versioned provider checks its namespace and path and sends the same version ID it just listed.
The never-versioned provider conditionally replaces the expected ETag with a tombstone and recognizes the same tombstone token on retry.
The proofs use the provider contracts for listing completeness, immutable version IDs, exact delete and conditional replacement.

Directory per-slot withdrawal uses the exact deletion grant after provider settlement.
The authorized retirement rechecks the alias census under the lineage reservation before committing its slot identity or pack plan.
The local orphan census starts after the attempt's region is removed and preserves aliases even in relocated packs.
Whole-scope withdrawal uses the disposing permit and the last-residence check, with separate authority for a rejected private fork stage.
Completeness and serialization of those censuses and vault execution are call-site premises.

The Lean model has one immutable delete target and one distinct replacement at the same key, one ordered withdrawal state and one reader ticket.
It models the ticket's publication, admission check, provider start, completion, lease expiry and physical admission closure.
Its safety theorem covers traces of arbitrary finite length.
The model reaches a lost delete reply followed by hosted settlement from fresh exact absence with a replacement present.
It reaches an expired detached read after both fences and proves removal is refused at that step.
The controls allow expiry-based deletion, settlement from a replacement or an incomplete listing, and path-only deletion of the replacement.

A registration precedes the provider's open-gate observation and remains in the shared census through conclusive completion of the detached storage task.
The custodian confirms a permanent fence before listing that census.
A request that observes the open gate must already appear in a later listing unless it completed.
A request that registers after the fence cannot observe an open gate.
The asynchronous argument relies on consistent provider operations, permanent control records, unique read identities, and every serving reader using the same qualified namespace.
Those premises and the async dispatch are outside the Verus decision proofs.

The pinned Verus baseline checked 22 items and the ten production mutations yielded eleven diagnostics at their declared laws.
The mutation harness uses `verification/run.py` and `area.json`.
Run `~/.elan/bin/lake build` under `verification/multi` to check the Lean model and its witnesses.
