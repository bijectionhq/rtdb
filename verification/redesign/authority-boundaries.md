# Authority boundaries in placed RTDB

This is the target design for the second structural review.
The TLA+ models cover small transition protocols and isolate their assumptions from the underlying service contracts.

## Submission, outcome, and read basis

A placed holder's volatile source position identifies provisional intake.
The durable receiver slot binds the installation and history, issuance namespace, authenticated scope and exact request.
It reserves evidence and settlement capacity before allowing a covered effect.
Exact retries redeem the same slot, and stale addresses cannot recreate it.
Positions in volatile intake can repeat after a writer change.

The resolver orders the effect, refusal or exclusion of all late copies with the slot's exact terminal witness.
Replica apply and checkpoint recovery preserve the original binding and decision position.
A synchronous response or redemption of a terminal receipt yields a causal token for the applied prefix containing the witness.
A provisional token must be redeemed before it supplies a read lower bound.

Receipt expiry ends the observation window.
Missing evidence at a cut may leave the outcome unresolved and does not release credit, close a slot or permit fresh issuance.
A sealed handoff installs the authentic predecessor prefix and transfers unresolved obligations before opening a new namespace.
Frozen attachments authenticate the original scope and open evidence within their held checkpoint cut.
An absent witness at that cut cannot certify a negative decision.

The passive slot and witness values carry exact identity and disposition.
The active holder selects between intake, append and shutdown.
The progress monitor publishes the applied prefix and wakes waiters.

The current placed outcome implementation and the [serving authority](../tla/PlacedServingAuthority.tla) and [frozen cut](../tla/FrozenReceiptCut.tla) models cover provisional receipt identity and terminal redemption.
Durable reservation, fencing and succession are implementation and proof obligations for the receiver slot contract.

Current source paths are [Multi intake](../../rust/crates/multi/src/logs.rs), [node submit and await](../../rust/crates/node/src/node.rs), [session basis](../../rust/crates/node/src/session.rs), and [installed progress](../../rust/crates/replication/src/driver.rs).

## Placement and freshness

The directory assigns writer epochs and locates an ensemble.
Its current renewable member lease also stops local serving after a directory outage.
With heartbeats disabled a subscriber's locally observed tip cannot establish freshness.

A sealed quorum loglet is the serving authority for an installed epoch.
A holder may append while the directory is unavailable if its loglet continues to confirm the epoch.
An explicit historical or at-least read uses the named history and an applied lower bound.
A fresh read takes an on-demand read index on the active loglet and waits for that prefix to apply before capture.
Both a holder and a subscriber can obtain the index.
An idle deployment generates no periodic heartbeat for read freshness.
Losing the active-epoch check, quorum, or replay coverage yields a retryable refusal, and a stale route must refresh.
A directory failure may stop activations, holder transitions and maintenance that changes residence or custody.
Reads whose storage entitlement requires a directory renewal stop when the current entitlement expires.
An already entitled read may continue under its existing hold and provider I/O fence.

The read index needs a quorum response from the exact loglet and history, including a durable committed-prefix witness and the seal or successor state.
A quorum of reachable servers that merely reports local tip values is insufficient unless the logkeeper protocol proves the reports intersect every completed write and identify a live loglet.
An ongoing move fences the predecessor, closes its committed prefix and activates the successor with that prefix before it can answer.
A read whose check overlaps a move can linearize on either side of that boundary.
The active-loglet identity must distinguish an assembly being prepared from the one that can serve.
Move recovery can sync the predecessor at the successor's epoch before installing the destination, so a matching epoch alone cannot establish that identity.
The predecessor needs a quorum-durable closed state that fences read-index votes; the successor needs a quorum-durable active state before it issues votes.
The states bind the deployment history and exact loglet configuration.

The certified-prefix contract in [structural redesign](../../dev/doc/structural-redesign.adoc) requires quorum installation before a writer returns an append receipt or a fresh reader exposes a cut.
The fresh reader samples a nonce-bound quorum of the active configuration and selects its greatest compatible installed certificate.
A reader may stabilize an exact partial decision by installing it on a quorum.
A staged suffix carries no read authority.
Seal and activation preserve the installed floor and a usable recovery closure.
Strict guards settle using original durable evidence and can leave a sealed attempt unresolved.
The serving attachment holds and applies the exact selected cut before using it for snapshot capture.
A gateway can retry an eligible, unanswered read in the same history.

The retained proof runs for the held-position frontier cover the prior one-quorum acknowledgement protocol.
They establish neither certified read-to-read monotonicity nor progress with an uncertified stalled tail.
The replacement model and controls belong to [W3](../PROGRAMME.md#w3--commit-and-replica-path-from-admission-to-applied-state).

The durable on-disk journal identity binds the logkeeper's directory registration.
The active ensemble fingerprint binds the declared journal identities and write quorum.
Every new transport session identifies its durable journal before admitting work.
The journal checks its identity on bound requests before deciding them.
Cell callers supply the expected journal for every slot, including for copy, repair, trim and retirement.
Low-level clients using discovery retain one observed identity per slot for the lifetime of the open ensemble and reject duplicate identities.
[PhysicalJournalQuorum.tla](../tla/PhysicalJournalQuorum.tla) checks that replies from aliased or rebound endpoints cannot form a quorum that misses a prior write.
The model treats identity and the durable receipt as observed together and abstracts the transport, disk and writer protocol.
All trim requests require a grant bound to the history, loglet, epoch and protected recovery frontier.

The placement agent is active.
The loglet client is a passive collaborator that returns an immutable read index.
The apply monitor owns the wait for a prefix.
A directory lease is scheduling evidence.
Removing lease-based self-fencing is gated on native read-index admission for every freshness mode and on a journal-identity audit.

The fixed-ensemble recovery model already exercises server promises, complete and partial sync, suffix repair and checkpoint-authorized trim.
The remaining proof has to compose that model with active-loglet transitions, committed-prefix read indexes, journal identity and the production trim grant.
The holder's confirmation round now counts positive checks only after the journal confirms that its promised and synced epochs match.
A blank or partially recovered journal refuses the check.
The new check has its own wire tag, so an older server cannot return a weaker confirmation in its place.
Check intersecting quorums and monotone acknowledged prefixes under crash, partial copies and ensemble movement.
Keep the existing term fence as an independent replica-stream check while it serves classic logs.
The new serving model assumes a commit prefix and an active-loglet check and therefore has no claim about the quorum implementation.

Native acceptance partitions the directory and then the quorum independently, observes zero periodic writes on an idle stream, and moves a deployment during a replica read.
Verify a read started after an acknowledged commit sees that commit even on a lagging subscriber.
Repeat with a stale route, a failed trim authorization and an explicit older basis.

See [the fixed-ensemble recovery evidence](../../dev/doc/rtdb-recovery-properties.md).

Current paths are [lease renewal](../../rust/crates/multi/src/lease.rs), [read fence](../../rust/crates/multi/src/agent.rs), [logkeeper client](../../rust/crates/logkeeper/src/client/mod.rs), and [journal state](../../rust/crates/logkeeper/src/server/state.rs).

## Recovery roots and upstream acknowledgement

Recoverability is a durable root plus a complete protected replica suffix that reconstructs the admitted state and its upstream resume token.
A provider log's durable acknowledgement qualifies only while the recovery authority preserves that closure.
The source may acknowledge a resume token once the recovery authority can issue this evidence through the token's durable replica record.
The root and suffix may advance independently if replacement coverage is published before release.
When the provider cannot establish that coverage, upstream acknowledgement remains at the last proven checkpoint.

The replica root and its suffix hold are passive immutable evidence.
The source connector sends acknowledgements after the applied monitor publishes the corresponding evidence.
The recovery authority owns retention and returns the release grant.
The PostgreSQL source can confirm an applied token on a standalone target whose local journals retain the synced prefix and whose local store syncs checkpoint publication.
Restart on the same media can recover the acknowledged transactions before a block is cut.
The hosted logkeeper path still requires a scope-specific recovery grant for early confirmation.
The storage model must exercise a lost copy reply and trim during restore.
Native CDC qualification must reconnect from an acknowledged token across a crash before block flush and show that neither the token's data nor a preceding transaction is lost.

See [R5](R5-recovery-roots.md), [Postgres source contract](../../dev/doc/pgsrc.allium), and [database processing](../../dev/doc/db.allium).

## Evidence boundaries

The existing term fence checks sequencing after an admitted frame.
Its proofs assume the supplied durable log has the committed-prefix and replay properties.
The new serving model checks the distinction between a provisional receipt, a durable coordinate and a per-read confirmation in a fixed two-epoch state space.
The custody model checks the separation of independently keyed copies and the publication-before-revocation sequence under an abstract deadline authority.
Production transition coverage for all of these authorities remains a separate exit condition.
