# RTDB multi recovery properties

Draft for formal verification, based on the `rtdb-multi` implementation at `cafde446`.
The first question is whether any allowed loss of compute can make a synchronous commit unrecoverable.
The second is when replacement compute can begin serving the correct state.

Start with **TLA+ and TLC** to check the interaction between Logkeeper recovery, checkpoint publication, and worker replacement.
Use **Verus** for a small Rust function once its contract is clear, and consider **Lean** if an unbounded protocol proof would justify maintaining a separate formalization.
These are proposed properties and experiments.

## Failure model and durable dependencies

The unit of safety is one deployment, including its storage generation and Logkeeper stream.
A worker crash clears the holder's in-memory intake, speculative state, and live index.
Recovery must work with empty replacement caches.
Any number of compute workers may disappear together, including the whole fleet.
A separate Logkeeper process crash preserves its journal if the disk survives, while permanent disk loss destroys that copy.
Messages and replies may be delayed, dropped, duplicated, or delivered after a lease expires.
An expired lease may trigger replacement, and fencing must remain safe if the previous holder is still running.

The normal recovery claim assumes surviving S3 objects, a consistent authoritative directory, the original decryption keys, and access to an intact quorum of the currently authoritative Logkeeper ensemble.
A three-member ensemble requires a quorum of two.
Durable Logkeeper replies assume successful journal sync, and quorum votes must come from distinct, correctly bound journals admitted to the ensemble.
A process restarted with its old journal and an empty process listening on the old address are separate model transitions.
In multi mode, the durable log contains resolved transactions on Logkeepers and the holder's intake is in memory.
Kafka inputs alone cannot reconstruct that log. [Log wiring](../../rust/crates/multi/src/logs.rs#L83-L119) · [Quorum and address resolution](../../rust/crates/multi/src/deployment.rs#L192-L217) · [Process startup](../../rust/crates/multi/src/process.rs#L151-L159)

Model the `COMMIT ASYNC` receipt, durable quorum commit, and synchronous success observed by a client as distinct events.
An async receipt can precede durable commit and its pending write may disappear with the holder.
A quorum-committed command whose reply was lost must survive, even though the client can be uncertain of its outcome.
An unacknowledged, uncommitted write may be kept or discarded by recovery under the protocol rules. [Commit modes](../../rust/crates/node/src/session.rs#L420-L450) · [Quorum receipts](../../rust/crates/logkeeper/src/client/writer.rs#L299-L339) · [Multi Allium spec](../../allium/rtdb-multi.allium)

## Safety properties

**S1 · One committed history.** Within a deployment's stream generation, a committed position identifies one resolved record for the rest of that generation.
After a Logkeeper durably seals a higher epoch, it refuses further appends from older epochs, including delayed requests.
Once a quorum has sealed, an older writer cannot commit an append first issued after the seal.
An append that overlaps the seal can finish using earlier durable acceptances, and its outcome must agree with recovery.

**S2 · Durable commit survives allowed loss.** Given the durable dependencies above and a live deployment, discarding any set of compute workers leaves every quorum-committed effect recoverable from the published checkpoint and a contiguous suffix.
Successful synchronous client responses are a subset of these commits.
Recovery represents each effect once and preserves its recorded order.

**S3 · Checkpoint publication authorizes trim.** A trim can discard a record only after an epoch-checked directory update publishes a usable checkpoint that covers it in the same stream.
A usable checkpoint references durable, readable objects and represents the committed prefix through its declared boundary.
Keep the boundary record required by the current implementation to resume source numbering.
A fork checkpoint from the parent's stream grants no permission to trim the child's stream.

**S4 · Repair preserves the committed prefix.** The winning history contains all committed records above the checkpoint.
A partial copy cannot advertise the target epoch as fully synced.
A delayed overwrite cannot drop records that the current writer already placed at that epoch.
An ensemble replacement must preserve the old committed suffix before acknowledging new work on the replacement.

**S5 · Readiness implies correct recovery.** A replacement reports writable readiness only after checkpoint load, required replay, and epoch confirmation.
A read that succeeds at a supported basis sees the state at that basis.
A checkpoint-only snapshot serves supported reads at or below its checkpoint and cannot satisfy a request for a later basis.

**S6 · A quorum represents real journals.** A new blank disk at a previously used address must enter through a safe admission or repair path before its vote represents an existing replica.
A failed journal open must leave that instance ineligible for quorum routing.
Check these as target properties and attempt to produce counterexamples for the current startup and address-binding paths.

**S7 · A keyed retry has one effect.** Within the supported idempotency window, replaying an eligible command under the same key after loss of its response returns the original committed outcome with no second mutation.
Transaction or protocol states that cannot be safely replayed may surface an unknown outcome.

Use a history of committed resolved records as a reference, independent of the candidate recovery algorithm.
Compare the recovered state with the fold of that history through the claimed basis, using an operation such as increment to expose duplicate application.
For a checkpoint at position `c` and a recovered basis `b`, the check is `Apply(CheckpointState(c), RecoveredSuffix(c, b)) = Apply(InitialState, CommittedPrefix(b))`.
The suffix excludes `c` and ends at `b`.
Use the initial empty state when no checkpoint has been published.
Fixed logical records are enough for the first model.
Later extensions can model bitemporal history and authorized erasure, fork lineage, and reclamation of objects shared by multiple deployments.
Relevant implementation paths include [writer recovery](../../rust/crates/logkeeper/src/client/writer.rs#L105-L226), [recovery decisions](../../rust/crates/logkeeper/src/client/recovery.rs#L24-L68), [activation and trimming](../../rust/crates/multi/src/agent.rs#L240-L333), [checkpoint publication](../../rust/crates/multi/src/agent.rs#L575-L608), [snapshot opening](../../rust/crates/multi/src/agent.rs#L544-L563), and [gateway retry](../../rust/crates/multi/src/session.rs#L311-L345).

### Failure cases that should be reachable

- A holder issues an async receipt and crashes before quorum append, and the pending write is absent after recovery.
- Two of three journals are unreachable, and a writer cannot reopen while that quorum remains unavailable.
- A write reaches only two journals before any checkpoint covers it, both copies are permanently destroyed, and the write is absent from the surviving S3 checkpoint and journal.
- Losing the directory or the encryption keys can make an otherwise intact S3 checkpoint unusable by the normal recovery path.

These cases make the durability boundary explicit.
In particular, survival with only Kafka and S3 is outside the current multi-mode guarantee.

## Progress and recovery time

For one affected deployment, assume that dependencies recover and remain available, an eligible worker remains alive, and a client keeps retrying with an admitted demand for that deployment.
Assume eventual delivery of retried requests and fair scheduling of continuously enabled recovery work.
Under those conditions, model the property that the deployment eventually has a confirmed writable holder and can serve a read at the required basis and commit a new request.
Exercise the same property after one worker goes down, after several do, and after every worker goes down and replacements register with empty caches.
A lost gateway connection can be re-established by the client under these assumptions.
A follow-up model can verify that concurrent cold opens stay within a configured budget and that background warming leaves capacity for admitted foreground work.
Compaction workers can be modeled as background I/O load and resumable work.

Treat recovery time as a separate staging measurement.
Time the first correct read at a defined basis, the first successful synchronous write, and the return to warm latency under a concurrent workload.
Record the costs of lease expiry, placement and queueing, journal reopen, checkpoint request count, replay bytes, and hot-cache refill.
Use remote S3 and a production-like directory with a client ledger of acknowledged operations independent of the failed workers.
The existing local benchmark verifies one killed holder with surviving nodes and batches of reconnecting clients. [Load harness](../../rust/crates/multi/tests/load.rs#L141-L225) · [Reported results](rtdb-multi-plan.md#L530-L560)

## Verification plan

1. **Model the protocol in TLA+.**
   Start with one deployment, three Logkeepers, two successive holders, one checkpoint, and a handful of writes with distinct payloads.
   Model pending messages and volatile state separately from synced journal state, published metadata, and durable objects.
   Use actions for append, sync, acknowledge, seal, partial repair, checkpoint upload, directory publication, trim, loss, restart, and reopen.
   Add a commit to the reference history when it first gains durable quorum coverage, including a minority record that recovery subsequently commits and a commit whose client reply is lost.
2. **Check safety first, then progress.**
   Use TLC to check S1–S5 on a fixed ensemble, then check the progress property with explicit fairness.
   Confirm that crash, restart, and a subsequent read and write are reachable, so the check exercises the recovery path.
   Add a second stream to check fork checkpoint and trim isolation, then add replacement admission, ensemble moves, and keyed retries.
   Record the finite bounds, assumptions, and counterexamples with the model. [TLC documentation](https://docs.tlapl.us/) · [Temporal properties and fairness](https://learning.tlapl.us/intro/tlc-config/)
3. **Validate the model against the Rust behavior.**
   Deliberately permit trim before publication, let an incomplete repair advance its synced epoch, and let a fresh disk vote under an old address.
   Each weakened rule should produce the expected invariant violation in a reachable test configuration.
   Map each transition to Rust and the Allium rule it represents, then turn relevant counterexamples into deterministic simulation or Rust regression tests. [Multi simulation and audit coverage](rtdb-multi-plan.md#stage-14-hardening-at-scale)
4. **Prove the narrowest useful implementation contract in Verus.**
   Start with the pure `winner` and `plan_repair` logic, after checking that the current Rust fits the supported subset.
   Specify the selected reply's ordering and the repair boundary, given valid seal replies and the protocol's agreed-prefix invariant.
   Keep network, async runtime, journal sync, and object-store contracts explicit at the proof boundary.
   Any trusted external specification remains an assumption of the proof. [Verus](https://github.com/verus-lang/verus) · [Trusted components](https://verus-lang.github.io/verus/guide/tcb.html)
5. **Choose one route to an unbounded protocol proof if needed.**
   Keep an inductive safety proof in TLAPS if the TLA+ model is the main artifact and the needed proof features fit.
   Lean is a viable alternative for an abstract, parameterized theorem that quorum intersection, durable seals, and recovery rules preserve the committed prefix across arbitrarily many epochs, with checkpoint correctness stated as a premise or proved separately.
   A Lean theorem needs an explicit mapping to the TLA+ model or the implementation before it can support claims about either. [TLA+ Proof System](https://proofs.tlapl.us/) · [Validating a Lean proof](https://lean-lang.org/doc/reference/latest/ValidatingProofs/)

Keep the first model faithful to the implementation where the Allium target runs ahead of it.
The Allium `RecoverTail` rule calls for verifying a checkpoint-anchored hash chain, while the Rust winner is selected by synced epoch and last LSN and replica digest comparison is a separate audit over a common retained interval.
The first model should expose the actual assumptions of that selection and leave a checkpoint-anchored digest as a possible extension. [Allium target](../../allium/rtdb-multi.allium#L1014-L1040) · [Rust selection](../../rust/crates/logkeeper/src/client/writer.rs#L197-L220) · [Retained audit](../../rust/crates/multi/src/admin.rs#L726-L735)

Turbopuffer is a reference for treating cache warmth separately from durability and minimizing the work on a cold read.
For RTDB, keep Logkeeper quorum commits and use those ideas to guide bounded warming and cold-read measurements. [turbopuffer architecture](https://turbopuffer.com/docs/architecture) · [Warm cache](https://turbopuffer.com/docs/warm-cache)

## First executable model

[RTDBRecovery.tla](../tla/RTDBRecovery.tla) models a single deployment on a fixed three-Logkeeper ensemble.
The journal, checkpoint, and volatile holder state are passive values, and `Next` exposes the actions that update them to TLC's scheduler.
Append delivery represents a durable journal write in one action.
The model tracks quorum commitment independently of the reply observed by the client.
It preserves pending messages across holder crashes so that a delayed append can race a successor's seal.
Checkpoint trim operates on transaction positions, with the production stream's extra boundary record folded into the checkpoint abstraction.
The progress check assumes fair recovery steps, the available journal majority, and a replacement within the model's epoch bound.

Run a configuration from the repository root using the [TLA+ 1.7.4 release jar](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4).

```sh
mkdir -p build/tla-tools build/tla-state
curl --fail --location https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar \
  --output build/tla-tools/tla2tools.jar
java -XX:+UseParallelGC -jar build/tla-tools/tla2tools.jar -workers 2 \
  -metadir build/tla-state/base -config dev/tla/RTDBRecovery.cfg dev/tla/RTDBRecovery.tla
```

| Configuration | TLC result |
| --- | --- |
| `RTDBRecovery.cfg` | Safety and eventual readiness hold across 342,407 distinct states with two write positions, two payloads, two epochs, and at most one lost journal. |
| `RTDBRecoveryRepeatedRestart.cfg` | The same properties hold across 52,259 distinct states with up to three epochs, one write position, and one payload. |
| `RTDBRecoveryEarlyTrim.cfg` | `TrimFollowsPublication` fails when an uploaded pack grants trim permission before its directory publication. |
| `RTDBRecoveryPartialSync.cfg` | `SyncedHistoryIsComplete` fails when a partially copied replica advertises the recovering epoch as synced. |
| `RTDBRecoveryLostQuorum.cfg` | `AcknowledgedWritesSurvive` fails when the only two journals with the acknowledged write are lost before the checkpoint covers it. |
| `RTDBRecoveryWitness.cfg` | The deliberately false `NoPostRestartReadAndWrite` invariant gives a reachable trace with a read of a pre-crash acknowledged write and a successful new write. |

Run the configurations serially and give each run a distinct `-metadir` to retain its output separately.
The next model extensions are fork and stream-generation trim isolation, ensemble replacement and journal identity, and client retry semantics.

## Proof status

The implementation checks in this section are pinned to `rtdb-multi` revision `3beb2ab5d56abdde034660687ff5c04815fbf069`.
An unbounded proof in this table refers to the specified abstract transitions.
Each open refinement obligation must connect those transitions to the Rust code before the result can establish an implementation guarantee.

| Property | Machine-checked evidence | Open implementation or modeling obligation |
| --- | --- | --- |
| S1 · One committed history | Fixed-ensemble TLC checks and the Lean quorum intersection and stale-append exclusion lemmas. | Prove persistent promises and ordered appends refine the voting premises, including overlapping accepts and ensemble changes. |
| S2 · Durable commit survives allowed loss | Fixed-ensemble TLC checks retention of quorum-committed effects, and Lean proves checkpoint-plus-suffix replay equals full replay for a complete ordered history. | Prove the unbounded committed-prefix invariant and validate that the stored format supplies the complete prefix and suffix. |
| S3 · Checkpoint publication authorizes trim | Lean proves for arbitrary trace length that trim retains the boundary of a durable same-stream checkpoint and an inherited checkpoint alone grants no trim permission. | Establish durable pack completeness, monotonic boundaries, storage generation checks, and the Rust publication and trim refinement. |
| S4 · Repair preserves the committed prefix | Fixed-ensemble TLC checks recovery selection and incomplete copying. | Prove the repair boundary and old-ensemble to new-ensemble handoff, including stale overwrite and failure during the copy. |
| S5 · Readiness implies correct recovery | TLC checks writable readiness and reaches a post-restart read and write, and Lean proves the exact basis and upper bound of successful abstract checkpoint-only reads. | Show the actual readiness and query paths establish checkpoint completeness, enforce the basis, and refine those checks. |
| S6 · A quorum represents real journals | TLC counterexamples for address reuse and early registration, plus a loopback reproduction of early registration with the Rust binary. | Reproduce address reuse against Rust and establish a durable identity and readiness contract for existing ensembles. |
| S7 · A keyed retry has one effect | Lean proves at most one mutation under the same protected permit for arbitrary admitted clock samples and retry order. TLC checks delayed originals, legacy writers, changing application keys, early expiry and marker deletion as weakened controls. Verus checks production admission helpers, and the live-cell test loses a committed reply and reads one effect on a replacement. | Prove the full refinement of ordered journal recovery and the resolver's protected marker transaction. Ambiguous batches, transaction controls and extended writes return an unknown outcome. |
| Conditional recovery liveness | Weak-fairness TLC checks for one deployment and an intact fixed-ensemble quorum. | Prove progress across the fault boundary with admissible demand, replacement compute, and the actual readiness and retry protocol. |

[CheckpointTrim.lean](../lean/CheckpointTrim.lean) distinguishes a deployment's own published checkpoint from an inherited read checkpoint.
The proof preserves the trim invariant across arbitrary interleavings of epoch changes, uploads, publications, inheritance, and trim.
Durable pack completeness is a premise at upload, and publication checks the writer epoch and preserves the previous boundary.
[Quorum.lean](../lean/Quorum.lean) proves intersection for any finite voting inventory and excludes a new stale append quorum when sealed members refuse the append.
Applying that theorem to Logkeepers requires distinct inventory entries tied to the correct journals and a durable promise that survives restart.
[IdempotencyWindow.lean](../lean/IdempotencyWindow.lean) checks arbitrary commit, reply, and disconnect traces for a single eligible key before expiration.
Its commit transition installs the outcome atomically with the only mutation, which is the contract the Rust resolver and idempotency table must establish.
[GatewayReplay.lean](../../verification/multi/lean/GatewayReplay.lean) proves effective-key preservation for a fixed application key or a private command key and ordinal.
It also proves that any number of replacements preserve the first recovery cutoff.
The [gateway model](../../verification/multi/tla/RTDBGatewayReplay.tla) checks a bounded single-command schedule with three weakened configurations.
[Verus checks the production cutoff-selection step](../../verification/verus/replay_deadline/proof.rs).
The full composition with the asynchronous gateway, SQL execution, and the durable journal remains open.
The private engine replay study checks the shared cutoff, protected marker creation and durable outcome lookup.
The deployed claim still requires the gateway to send the same permit on each attempt and to enforce writer compatibility and command eligibility. [Engine replay evidence](rtdb-engine-proof-evidence.md#private-replay-admission-and-marker-authority)
[CheckpointReplay.lean](../lean/CheckpointReplay.lean) proves that replaying exactly the suffix above a checkpoint gives the same result as folding the whole committed prefix through the requested basis.
It also proves that the abstract snapshot response uses the requested basis within the checkpoint's coverage.
The production refinement must connect the packed catalog and bitemporal query state to this complete logical prefix.
Run the proofs with [Lean 4.24.0](https://github.com/leanprover/lean4/releases/tag/v4.24.0).

```sh
lean dev/lean/CheckpointTrim.lean
lean dev/lean/CheckpointReplay.lean
lean dev/lean/Quorum.lean
lean dev/lean/IdempotencyWindow.lean
```

[RTDBRecoveryIdentity.tla](../tla/RTDBRecoveryIdentity.tla) checks two independent ways to violate S6.
With identity checks and eligibility after journal open enabled, TLC checks all 139 states of the bounded model.
With `RTDBRecoveryIdentityReuse.cfg`, a write reaches two journals before a fresh journal takes over one of their addresses.
A seal quorum made up of the fresh journal and the original journal that never held the write can then recover an empty history.
With `RTDBRecoveryIdentityStartup.cfg`, registration followed by a failed journal open leaves the process eligible.
In a loopback cell, pass a regular file as the Logkeeper's `--data` path to reproduce the startup failure with the Rust binary.
After 3.5 seconds, `status` reported one live Logkeeper while its advertised socket refused connections, and the process reported `Not a directory` during shutdown.
The original reproduction ran against a revision that registered the logkeeper before opening its journal and identified replicas by their network addresses. [Historical registration path](https://github.com/bijectionhq/rtdb/blob/3beb2ab5d56abdde034660687ff5c04815fbf069/rust/crates/multi/src/process.rs#L168-L176)
The current process opens and locks the journal before claiming membership.
The client associates the endpoint with the durable journal ID and verifies each new session before sending commands.
The journal checks the target identity on bound commands before acting.
Activation identifies a set of journals and its quorum size, allowing different routes to the same membership.
Recovery, ordinary appends, serving checks, replica repair and retention use that same connection boundary. [Connection](../../rust/crates/logkeeper/src/client/connection.rs) · [Ensemble](../../rust/crates/logkeeper/src/client/ensemble.rs)

### Upgrading cell journals

Upgrade logkeepers while retaining their journal directories, then quiesce directory writers, holders and cell administration while upgrading the clients that write journal inventories.
Keep a quorum of logkeepers available within each ensemble during the logkeeper rollout.
Upgraded readers accept legacy address inventories and packed parked records.
Directory clients that predate bound inventories must be upgraded before those inventories are written.
Reopen held deployments at a higher epoch to install the identity-bound activation.
Resume an interrupted legacy cell transfer with both source and destination directories available so it can bind the source membership before copying.
Before retiring or transferring an installation with an address-only inventory, verify each historical journal that received its replica stream.
Use retained `MEMBER` files and historical placement records to identify the journals, and collect their current routes.
Run `rtdb-multi list --directory DIR` to obtain the installation ID, inventory and version.
Supply a JSON object mapping every legacy address to an array of verified objects with `address` and `journal` fields.
Include all verified journals if more than one used the same legacy address.
Run `rtdb-multi bind-journal-inventory --directory DIR --id UUID --version N --bindings FILE` to record that inventory at the inspected version.
The command updates only the directory.
The next transfer or disposal uses the resulting journal IDs, and disposal waits for all named journals to affirm retirement.

## Archive restore and physical replay

A verified archive checkpoint is a representation of the captured transaction prefix.
Restore seals the serving journal and durably retains the last sealed replica position as the cutoff for compaction notices.
Replay still applies all transactions after the checkpoint and advances ordered replica progress.
A compaction notice in the sealed prefix describes the old serving objects and does not change the restored catalog.
Compaction after the cutoff remains eligible.
Moving a restored installation preserves the cutoff and copies the required archived objects before materializing the destination root.

The local cell regression opens the restored installation and reads both the checkpoint row and the later transaction.
It then moves the installation, removes the source archive, and reads the same logical result from the destination.

Lean 4.36.0 checks two additional laws in [CheckpointReplay.lean](../lean/CheckpointReplay.lean).
Filtering physical notices through the cutoff preserves the logical fold for every replay suffix, assuming a compaction notice leaves logical history invariant.
The model keeps a compaction strictly after the cutoff.
Dropping old transactions fails the first theorem, and suppressing every compaction fails the second.
These theorems depend on the abstract event classification. The cell test exercises the implementation with local storage and durable loopback journals.

## Transfer protocol study

[RTDBCellTransfer.tla](../tla/RTDBCellTransfer.tla) distinguishes fencing, the durable object and journal copies, the prepared receipt, route publication, activation, and source disposal.
The model treats each of these as an atomic observation.
It assumes an available source until authorized disposal and a destination with an independent physical identity.
Its progress check assumes weak fairness for the steps that prepare and activate the destination.
It covers one move. Repeated moves and exact earlier-residence receipts are exercised by the native move-and-return regression.

TLC 2026.11.19.180144 explored all 10 states of the baseline and checked the safety invariants and conditional progress.
Run the baseline from `dev/tla` with the pinned classes:

```sh
BIJECTION_LOCAL_EXECUTION=owner-authorized java -Xmx512m -cp "$TLA2TOOLS_JAR" \
  tlc2.TLC -workers 1 -noGenerateSpecTE -config RTDBCellTransfer.cfg RTDBCellTransfer.tla
```

Run a copy of the configuration for each independent control, using the same command with its configuration file.

| Change to baseline | Checked failure |
| --- | --- |
| `Mode = "early_route"` | The route points to a destination before both copies complete. |
| `Mode = "ignore_journal"` | Objects alone can produce a ready receipt and the route lacks a durable journal. |
| `Mode = "early_disposal"` | The fenced source disappears before the destination is selected. |
| `Mode = "reuse_identity"` | Disposal of the old physical identity removes the routed destination's objects. |
| `Mode = "allow_two_writers"` | The destination opens while the source writer is still active. |
| `SPECIFICATION Spec` | A behavior can stutter before the destination becomes active. |

On the integrated source, 79 multi library tests and 53 loopback cell tests passed.
The separate Logkeeper run passed 27 unit tests and 11 contract tests.
The transfer test moves a restored checkpoint and ordered tail, returns to an earlier cell after the old serving residence is disposed, and checks the retained custody and query result.
These runs used local stores and journals. S3 permissions and remote task-stop evidence still require provider qualification.

## S3 erasure ordering

The deletion model tracks the recorded and legacy write lease deadlines, a delayed upload, two empty inventories, and publication of a receipt after key destruction. [RTDBDeletion.tla](../tla/RTDBDeletion.tla)
The passing configuration assumes accepted uploads complete within one credential lifetime and administrative writers drain before deletion.
With a two-tick credential lifetime, two-tick upload delay, and seven ticks of modeled time, TLC checked 11,007 distinct states.

| Configuration | TLC result |
| --- | --- |
| `RTDBDeletion.cfg` | The first inventory follows expiry, the receipt follows separated observations, and the inventory remains empty after the receipt. |
| `RTDBDeletionWithoutLegacyHorizon.cfg` | The first inventory can run while a legacy write lease remains active. |
| `RTDBDeletionWithoutSpacing.cfg` | An upload admitted before expiry can complete after the receipt. |
| `RTDBDeletionUndrainedAdministrator.cfg` | A delayed administrative upload can recreate an object after the receipt. |

Use `dev/tla/RTDBDeletion.tla` as the module for all four configurations with the same TLC command above.
