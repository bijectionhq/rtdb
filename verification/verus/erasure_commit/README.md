# Installed erasure commit

The harness imports the Rust decisions called by the installed erasure coordinator, custody evidence verifier and provider adapter.
It also imports the existing serving-prefix retirement decisions for the scoped namespace grant horizon.
The corresponding Lean package is [InstalledErasureCommit](../../multi/erasure/InstalledErasureCommit.lean).
Run `lake build` in `verification/multi/erasure` to build it.
This package pins a Logkeeper revision that exports the rotation evidence definitions and leaves the acknowledged-recovery model on its own reference revision.

| Shipped decision and caller | Lean definition or theorem |
| --- | --- |
| `strict_majority` and `mirrors_fence` in `covers_clean_rotation` with non-nil and strictly ordered voter identities | `Logkeeper.SelectionFence.StrictMajority`, `MirrorsFence`, `RotationEvidence` |
| `at_clean_checkpoint` in `covers_clean_rotation` using the prepared checkpoint and successor grant checked by `validate_rotation` | `AtCleanCheckpoint`, `authenticated_rotation_covers_recovery`, Study 112's `ErasureCommit.MayCommit`, `Fenced` and `Readable` |
| `fixed_deadline_admits` in `coordinate`, `same_reservation` in `matches_reservation`, `same_attempt` and `fixed_commit_admits` in `validate_commit` | `FixedDeadlineAdmits`, `SameReservation`, `SameAttempt`, `FixedCommitAdmits`, `a_fixed_commit_keeps_the_reserved_attempt` |
| `completion_status` in `DestructionEvidence::fixed_deadline_met`, `installed_admits` in `Installed::commit_and_arm`, and `operational_target_at` in `CommitRecord::read` and `validate_commit` | `CompletionStatus`, `fixed_completion_reports_late`, `an_operational_target_claims_no_deadline` |
| `ordered_holds_drained` in `confirm_destruction` and `leases_expired` in the serving-prefix provider path | `OrderedHoldsDrained`, `NamespaceGrantsDrained`, `serving_retirement_waits_for_all_grants_and_original_hold` |
| `withdrawal_follows_join` in `settle_journal_key` plus `destruction_follows_drain` and `observed_during_drain` in `CustodyEvidence::verify_destruction` | `WithdrawalFollowsJoin`, `DestructionFollowsDrain`, `ObservedDuringDrain`, `destruction_follows_every_observed_join` |

The installed Logkeeper client authenticates the signed rotation response against the installed group and exact receipt before the coordinator sees it.
The slot-pair decision checks the two physical image descriptors.
The successor-generation decision separately checks the payload-key generation.
The mapping to `SelectionFence.MirrorsFence` relies on the issuer signing both actual readbacks at its named ballot and prefix.
The Lean composition uses the reference majority theorem and Study 112's recovery theorem.
It takes as premises that selectable recovery images inherit the attested clean cut and the suffix obeys the generation fence.

The fixed authority signs the exact prepared operation and both inventories before rotation.
Its separate commit authentication adds the ordered switch and committed time.
The Rust decisions compare the prepared operation, switch, deadline and inventories on the fresh path.
The retry path relies on the custody authority returning the original durable commit, and verifies its signed state.
Fixed completion at the deadline is on time.
A late authenticated receipt remains valid evidence and the public completion predicate reports `Some(false)`.
An operational target computes the 120-second scheduling timestamp and the completion predicate returns `None`.

The source wiring requires the complete durable local roster, zero outstanding old-root holds, and the broker's complete domain drain before the coordinator admits retirement.
Journal-key withdrawal also waits for the observed join time, settled checkpoint retirement and the serving disposition.
The serving-prefix code waits for grant expiry and multipart drain in S3, or obtains an exclusive lifecycle lock for a local prefix.
The grant-expiry law applies to every grant whose lifetime, including the clock allowance, fits under the scoped bound.
The hold law relies on Logkeeper's count containing the original custody hold.
S3 and DynamoDB separately fence each remote unit-key namespace and await the shared provider and delivery bound before settling its registered unit slots.
Serving reads and query delivery keep their original lease through provider use.
Those call sites provide the premises for the grant and hold model by source inspection.
The broker census, exact association of provider observations with the original placement, provider effects, clock and enrollment qualification, signature construction and verification, and async ordering remain outside the Verus harness.
The installed adapter supplies the operational profile.
Fixed physical enforcement depends on an authority that can implement its signed commitment under the declared outage and clock bounds.

The negative controls weaken a shipped decision and its postcondition, then require failure at the named independent law in [`area.json`](area.json).
The Lean package includes concrete rejections for a missing slot, stale generation, mixed attempt, early destruction and a live hold or grant.
The reachability examples cover three voters with two mirrored endorsements and a fixed attempt admitted, committed and completed at its deadline.
The model quantifies over arbitrary finite evidence and duty rosters and uses unbounded natural numbers for times and identities.
The Rust harness checks the machine-width inputs, including overflow of the operational target and the serving lease horizon.
