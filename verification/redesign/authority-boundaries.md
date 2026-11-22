# Authority boundaries in placed RTDB

This is the target design for the second structural review.
The TLA+ models cover small transition protocols and isolate their assumptions from the underlying service contracts.

## Submission, outcome, and read basis

The placed holder's source log is volatile.
Its positions can repeat after an epoch change because the new intake starts after the last source position found in the durable replica stream.
The serving protocol binds an asynchronous receipt to an exact attempt even when a successor reuses its position.

Placed intake records now carry the history and writer epoch of the opened log.
The resolver binds that scope to the record's source position and authenticated principal and stages an outcome witness with both commits and aborts.
The replica frame carries the full attempt identity, and the checkpointed outcome table indexes its digest to the transaction key.
Each intake includes a fresh nonce so the indexed digest alone does not disclose the principal.
The outcome witness has a logical receipt window beginning at the logged intake timestamp.
Physical expiry remains open.

The volatile intake assigns a typed attempt identity consisting of the database history identity, sealed writer epoch and local sequence.
An asynchronous submit returns a provisional receipt carrying that identity.
Apply publishes the mapping from attempt to durable outcome with the resolved replica frame, and preserves it through checkpoints for the declared receipt window.
The durable witness retains the full transaction key.
The connection uses the paired source and replica coordinates of the installed prefix to redeem a current-epoch attempt.
The progress monitor retains the last applied result with its exact attempt identity.
When that result has retired from the monitor, redemption reads the checkpointed witness and confirms the full transaction key.
A synchronous response or the redemption of an asynchronous receipt yields a causal token bound to a prefix that contains that outcome.
A provisional receipt is redeemed before it can supply a read lower bound.
A lost response is retried by the existing scoped application retry key and body digest.

After sealing and recovering the old writer, an absent attempt in the closed epoch receives an unresolved outcome once recovery proves the relevant prefix has been searched through its final position.
An absence beyond the retained receipt window receives an expired outcome.
An absent attempt in the current epoch can still be pending.
After migration, numeric-only receipts from the volatile intake carry insufficient identity and must fail explicitly at the placed boundary.
Classic deployments can continue to use the durable source position for their existing receipt format.
A database incarnation must also distinguish deletion and recreation at the same name.

The passive identity and outcome types hold immutable coordinates.
The active holder owns selection between admission, append and shutdown.
The progress monitor owns publication of the applied outcome and wakes waiters.
The immutable receipt is scoped to the authenticated principal, deployment history and attempt.
The native submission result and SQL session use the same versioned receipt representation.
A frozen attachment authenticates receipts in the deployment's history and checks them against the exact checkpoint root it admitted and loaded.
An applied token must name its journal and a position at or below that cut.
A provisional receipt needs an exact outcome witness in the checkpoint.
A missing outcome on the frozen attachment yields a retryable refusal.
The frozen receipt model checks identity, cut coverage and how the attachment classifies absence. [FrozenReceiptCut.tla](../tla/FrozenReceiptCut.tla)

Acceptance starts with a fault replay that pauses an intake after submission, seals the epoch, commits another transaction at the same local position and tries to redeem both receipts and use them as read bases.
It also covers an append that committed without a reply, an epoch change while awaiting, an outcome restored from a checkpoint, and independent installations serving the same database name.
The model `PlacedServingAuthority.tla` distinguishes an attempt from its position in the committed sequence.

Current paths are [Multi intake](../../rust/crates/multi/src/logs.rs), [node submit and await](../../rust/crates/node/src/node.rs), [session basis](../../rust/crates/node/src/session.rs), and [installed progress](../../rust/crates/replication/src/driver.rs).

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

The current writer returns an append receipt as soon as a quorum holds the entry, before every commit-watermark publication is acknowledged.
Sampling only published commit watermarks can therefore miss a completed write.
A quorum of exact active-loglet replies can conservatively bound completed writes by the largest held position among its replies, since that quorum intersects the write quorum.
Such a bound can include unpublished entries.
The subscriber waits until its committed prefix is applied through the bound and refuses freshness if it cannot reach it.
The read path can keep the one-quorum append contract with this rule.
[ActiveLogletReadIndex.tla](../tla/ActiveLogletReadIndex.tla) checks this rule with overlapping three-journal ensembles, two available positions and a single read.
It separates a synced predecessor from an active successor and allows publication to lag acknowledgement.
The move and activation are atomic in the model.
The native logkeeper persists activation only on the writer's target ensemble after recovery and installs the same activation on repaired replicas.
A read-index round requires matching stream, epoch, ensemble and write quorum and accepts a journal's held position only after its preceding journal work is durable.
A writer requires an activated write quorum before it opens.
During upgrade, logkeepers must serve the identity handshake before cell writers and subscribers enable identity-bound sessions.
The subscriber probes the replica log of its attached database and waits for its installed position before capturing an implicit query or transaction snapshot.
An explicit or already pinned basis keeps the history and time selected by the caller or transaction.
The gateway requests a subscriber that supports fresh reads for a deployment with linearizable reads enabled.
A subscriber that cannot establish its basis refuses before the read.
The gateway retries an unanswered command on the holder when the refusal leaves the backend idle.
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
