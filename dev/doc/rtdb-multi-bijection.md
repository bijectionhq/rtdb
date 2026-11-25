# RTDB multi in Bijection

How the endgame in [`allium/rtdb-multi.allium`](../../allium/rtdb-multi.allium) fits Bijection's hosting infrastructure, and what it lets Bijection retire.
The staging of RTDB itself is [`rtdb-multi-plan.md`](rtdb-multi-plan.md); this page is the Bijection side of the same route.
Paths starting `bijection/` are in the Bijection repository.

## In one sentence

A Bijection deployment becomes an RTDB deployment, and three things collapse into one:

- **The Conductor becomes the RTDB holder.**
  It embeds an RTDB node and holds the deployment it serves.
- **Bijection's cell catalog becomes the RTDB directory.**
- **The commit path moves.**
  It goes through RTDB's per-deployment log on the cell's log servers instead of through the Publication Appliers and the Kafka cell history.

Bijection keeps what is specific to it: the kernel (labels, Cedar, tasks, receipts, branch semantics), Funrun and V8, search, connectors, Big Brain and the relays.
The storage engine, the ordering authority and most of the placement machinery become RTDB's.

## Component by component

| Bijection today | With RTDB multi | Fate |
| --- | --- | --- |
| **Usher**: terminates HTTP and sync connections, finds the serving Conductor | The same role, routing by the RTDB directory and affinity, and also terminating pgwire and Flight SQL for Python and ADBC | **Merged**: Usher is the RTDB gateway, with Bijection's protocols added |
| **Conductor**: claims deployments under a catalog `Placement{epoch, holder}`, hosts the application, owns the transaction, submits writes | Embeds an RTDB node; claiming a deployment is the RTDB claim (directory CAS, seal, recover), and the placement epoch is the RTDB epoch | **Kept**, now the holder; one placement authority instead of two |
| **Publication Appliers ×2 and the MSK Kafka cell history**: one ordered history per cell, guarded changes, receipts, recovery roots, the catalog | Ordering is per deployment, in the holder's resolver, made durable by a quorum append to the log servers; the catalog moves to the directory; recovery roots become RTDB checkpoints and manifests | **Retired** |
| **Cell catalog** (`Place`, `Claim`, `AdmitMember`, `RetireMember`, `SetCellRouting` in `bijection/crates/native_commit/src/catalog.rs`) | Directory records for deployments and members, plus the cell's routing data | **Becomes the directory** (decision 2) |
| **Tiered store, tenant LSM, `tenant_lsm_s3`, recovery archive, physical forks** | RTDB blocks, tries, packs, manifests, the reaper, lineage forks | **Retired** |
| **Flush pool** | Holders flush their own small packs; large flushes and search-segment builds may remain a pool | **Mostly retired** |
| **Compactor pool** (table runs and search) | Compaction jobs on the RTDB worker pool, adopted by the holder through `TriesAdded`; search merges stay | **Becomes the RTDB worker pool** |
| **Search pool** (2 GiB cache, affinity by storage namespace) | Unchanged role; builds from the deployment's change feed and reads RTDB objects; its affinity key is RTDB's storage identity | **Kept** |
| **Funrun** | Unchanged; its scoped reader reads through the Conductor at an exact RTDB basis | **Kept** |
| **`object_cache`** (range cache on the Conductor's NVMe) | RTDB's node-wide fair cache, keyed by immutable object and range | **Replaced** |
| **Cell Controller** (Go, Big Brain): ECS, members, placement, draining, demand metrics | Keeps ECS and capacity, and drives placement through directory CAS; the rendezvous score already exists in Go with shared test vectors | **Kept as the RTDB controller** |
| **DynamoDB key authority** | Implements RTDB's per-deployment data keys and per-epoch storage credentials | **Kept**, behind RTDB's `credentials` interface |
| **Aurora, the Big Brain API, the wake supervisor, relays, file and blob buckets** | Unchanged; a cell parks once every deployment in it is parked and its directory is quiescent | **Kept** |

## Request paths

### Mutation

1. Usher routes the request to the deployment's holder, the Conductor at rank 1.
2. Funrun runs the TypeScript in the Conductor's workspace; interactive transactions stay in Bijection, as in Architecture A of the RTDB architecture proposal.
3. The Conductor's admission produces a finished batch, with `ASSERT`s for read-set validation.
4. The embedded holder resolves it, appends it with one quorum append (about 2–4 ms), and applies it.
5. The Conductor returns the outcome and the basis.

The Kafka append, the Appliers' decision, the pgwire hop to a separate XTDB process and a second fence all leave the commit path.
The lease key in every read set that the `WorldStore` contract uses for "single writer per world" is no longer needed: the holder's epoch already fences the only writer.

The `CommitHostedStaged.v6` Flight action carries the exclusive journal cutoff and
SHA-256 of the exact verified receiver clock certificate envelope. A guarded
replica provider must retain this pair through original-journal recovery.
The writer seals the cutoff and digest in the record's acceptance stage and derives the guarded journal frame from that stage.
The timed acceptance audit requires the frame to repeat the sealed pair and returns the envelope digest with the original voter evidence.
Archive capture records the original stream's digest of each timed entry before opening its payload, so a resealed checkpoint transfer still names the entry its original voters hold.
`verification/multi/lean/TimedEvidenceAudit.lean` proves both rules: the audit reports only the sealed clock evidence, and a resealed copy names its exact original voters while an original append cannot carry a digest.

**Proof checked on 2026-11-23.** Verus 0.2026.11.20.9da409b verified six items and rejected all eleven controls in [`verification/verus/sealed_stage_audit`](../../verification/verus/sealed_stage_audit/area.json), each at its intended check.
The checked Rust decisions require both journal fields or neither, a positive journal cutoff at or above the stage cutoff, and a 32-byte digest with at least one nonzero byte.
Audit accepts equal optional pairs, comparing the cutoff and every byte of the digest, and the checked round trip accepts a writer's unchanged timed pair.

**Source inspection.** The resolver writes both fields from one typed cutoff, `enqueue_replica_with` hands the decoded stage's journal evidence to `GuardedAppend`, and Logkeeper derives the journal frame from it.
The audit calls the checked comparison on the sealed and framed pairs, then takes its reported cutoff and digest from the sealed pair.
Logkeeper validates the raw frame's paired fields on append and recovery.
The Verus correspondence relies on byte preservation in Rust's slice-to-array conversion, Protobuf encoding and deployment sealing, and on exact record selection and digest binding by the certified-range reader.
Receiver certificate verification, clock bounds, and quorum durability remain premises.

Source inspection places the Logkeeper sample under the writer lock after encryption and payload copying, before advancing the log position or sending a replica request.
The ordered journal and memory log use the same check under their append locks and support only guards without a journal cutoff.
The live upper sample is the clock reading plus one microsecond.
It must be representable, positive, at least the retained staging sample, and strictly below the guarded cutoff.
A supplied journal cutoff must be at or after the guarded cutoff.
The append timestamp is the clock reading itself.

When the optional journal condition holds, the decision on nonnegative inputs agrees with Bijection's `guardedReplicaAppend` in `PhysicalTiming.lean` for a supported provider.
Bijection's `exclusive_wire_cutoff` Verus law establishes that comparing an integral microsecond sample strictly against the ceiling-converted cutoff preserves the strict comparison in nanoseconds.
The RTDB check takes both cutoffs in microseconds.
Physical acceptance still depends on the installed clock bounds and timely original-quorum durability.

For a strict placed acceptance, `rtdb-multi audit-acceptance` reads one replica offset for the exact installation, incarnation and log epoch.
It requires a matching write quorum, checks the sealed record against that quorum's digest, and exports the original staging sample, cutoff and append timestamp.
Each journal records its own upper clock sample after syncing the original guarded append.
The sample is synced as local metadata before the reply and survives restart and compaction with the exact entry.
Only a sample strictly below the record's journal cutoff supplies a vote, including after recovery.
Recovery copies carry the entry without creating observations of the original append.
Recovery keeps a sealed guarded tip on an exact durable commit witness or a timely original quorum from the recorded journal roster.
Trimmed or non-responding journals and originals missing samples remain possible voters.
Recovery drops the tip only when those still capable of supplying a timely original vote cannot form the recorded write quorum.
The successor stays closed while the outcome is unknown.
Recovery of a retained guarded tip records its commit on the predecessor quorum before copying the tip to a new ensemble.
An interrupted copy can therefore recover the predecessor's selected outcome while the directory still names that predecessor.
On 2026-11-23 Verus 0.2026.11.20.9da409b checked 12 items in the [guarded-tail recovery area](../../verification/verus/guarded_tail_recovery/area.json), and all 16 mutations failed at their declared laws.
The checked Rust functions classify sealed and exact probe evidence, count each journal slot once, decide the tail, and check the predecessor commit target and threshold.
Source inspection traced journal identity and digest binding and the await of predecessor recovery before either destination copy path.
Durable journal acknowledgments, authenticated journal identity, exact digest equality, single-writer epoch ownership, and valid journal clock bounds remain premises.
The raw timed entry binds the original stream, journal identities and quorum size across moves and archives.
The audit reports current quorum agreement separately from the original quorum's sync bound.
It can read a retained original sample matching the certified entry even when that journal lacks a local commit marker.
Each journal in the recorded original roster contributes its earliest positive original sample strictly below the stored cutoff.
The sync bound is the recorded write-quorum-th smallest of those samples and is absent when fewer journals qualify.
The audit also reports its process's rounded clock immediately after establishing quorum agreement.
The audit time includes probe latency and requires bounded clock uncertainty when compared with the writer's sample.

**Proof checked on 2026-11-23.**
Verus 0.2026.11.20.9da409b checked 11 items in the [original sync audit area](../../verification/verus/original_sync_audit/area.json) over the shipped Logkeeper decisions.
The bound proof covers arbitrary observations for a valid recorded roster and quorum, including duplicate journal replies and ties between different journals.
The lookup proofs check exact reply coordinates, original sample admission, and selection of the carried digest for a resealed entry.
All 17 controls failed at their expected laws, covering foreign and duplicate votes, equal and late cutoffs, a later sample from the same journal, a weaker quorum, omitted ordering, copy and nonpositive votes, mismatched reply and lookup coordinates, lost carried digest, and missing server sample.

**Source inspection.**
The client supplies the certified entry's roster, quorum and cutoff to the bound calculation and probes original journals using authenticated endpoint identities.
The server computes the retained digest from the selected entry in the requested original stream.
The proof takes journal identity and sample provenance, the retained chain's binding to exact bytes, the validity of the recorded origin, and the installed physical clock bounds as premises.

The installed audience Flight gateway discovers its existing holder with its own DynamoDB client.
It makes strongly consistent point reads of the installed deployment and its assigned member, then dials the configured audience port on the member's advertised host.
A missing, stale, or parked placement makes the audience handshake unavailable.
The writer lifecycle remains with the placement owners. At the holder, acquisition verifies the recovered source and writer quorum.

Each installed holder prepays two Flight session seats per turn, and a shared directory claim makes each position exclusive across the cell's holders.
The off-turn mint reserves its local permit and then conditionally claims the deployment and seat index in DynamoDB.
The transaction checks the exact holder and writer epoch with the seat predecessor, so even a placement change after the gateway's read can fence a stale mint.
The running holder relinquishes the claim after the session and its retained I/O drain.
A successor can replace a charged claim with an exact ECS task-stop observation.
An interrupted or ambiguous claim is retired by advancing its revision from either the observed predecessor or the attempted claim revision.
A delayed duplicate write can only apply to its original predecessor.
The gateway and holder parse the same seat selector, and the gateway requires the holder to acknowledge the exact selector and mint attempt before wrapping a new session token.
A live seat can recover its original session under the same credential and exact mint attempt.
For the same deployment, the holder compares the candidate with the current local attachment before retiring a displaced or expired occupant.
Close, expiry and displacement can end a registry entry while its session still owns the permit.
A lost close request leaves the existing session in place until retirement, and a lost reply leaves the permit charged while original I/O drains.
The holder orders a successful audience data reply's local egress handoff under the same lock that orders session close.
An already admitted cleanup call can still acknowledge close.

On 2026-11-24, Verus 0.2026.11.20.9da409b checked eight obligations and rejected all 16 controls in the audience session seat area.
See [the area manifest](../../verification/verus/audience_session_seats/area.json) for the mutations and expected diagnostics.
The area checks the shipped alternate parser, registry size and indexing, attachment and admission decisions, and a lost-close model that invokes the shipped admission decision.
The handoff predicate refuses a closed session's data reply and allows its cleanup acknowledgement.
Source inspection ties the predicate and synchronous egress handoff to the session lock, which serializes them with close.
Source inspection ties vacancy to an acquired semaphore permit, retry to lookup of the original live registry entry, and retained provider I/O to a strong reference to the session through its I/O admission.
The local bound assumes those runtime ownership and serialization rules, the sampled catalog, and authenticated identity comparisons.

**Cross-holder proof checked on 2026-11-23.**
TLC 2026.11.19.180144 exhausted 9,522 distinct states for one assignment move and 25,252 for two moves, with two holders, two alternates for one prepaid turn, a three-tick ceiling and a two-tick sweep.
[The model](../../verification/tla/CrossHolderAudienceSeats.tla) checks seven invariants and one progress property, including capacity across holders, current routing and mint authority, local drain before reuse, retained I/O custody, cleanup-only standing, and registry retirement by ceiling plus sweep.
Nine controls failed their declared laws, and a witness served a replacement while an earlier holder still retained already-admitted I/O in the other seat.
On 2026-11-24, Verus 0.2026.11.20.9da409b verified five items for the shared claim, and all seven controls failed their declared checks.
[The claim proof area](../../verification/verus/audience_cross_holder_claim/area.json) checks admission against the exact free or stopped predecessor, release before or after a pending claim commits, and refusal by a delayed release against a distinct replacement revision.

**Cross-holder source inspection.**
The claim uses the ordinary holder directory client off-turn.
Each data action uses the independent audience read path to check current standing, and an authenticated close can act on the original session without opening a native connection.
The holder recovers the directory checkpoint at attachment time.
Audience captures confirm their own pinned catalog and projected floor under the current writer epoch, so a later directory maintenance checkpoint does not govern a held audience read.
The audience Flight contract refuses the whole-Root publication head and the provider-issuer control in reads and confirmation.
The holder fixes the 180-second deadline before its claim attempt, and the installed listener requests a sweep every 10 seconds.
The 190-second registry-retirement bound assumes a progressing monotonic clock and that the installed listener completes an expiry sweep within 10 seconds of the deadline.
Physical release also requires retained I/O to drain and the directory to accept or observe the fenced release, or exact task-stop evidence to arrive.
The correspondence assumes DynamoDB conditional-write atomicity and consistent reads within the cell, unique claim revisions, authenticated bearer routes, exact ECS stop records, and the session ownership and clock wiring inspected in Rust.
Cross-cell transfers and provider I/O drain time require separate evidence.

#### Prepared audience writer handoff

The active and prepared fleets must join the same placement authority, source incarnation, storage namespaces and original replica quorum.
The target node warms a protected committed cut while the incumbent holds the writer epoch.
It retains the preparation in the deployment record and issues its witness only after reading the durable reservation before the switch.
A signed control request binds the handoff, target, action and nonce.
The signed response binds the exact request and ordered witness.
At the switch the directory assigns the immediate successor epoch directly to the target.
The target confirms leadership on the original quorum before ordering its opening.
An ordinary park, restart claim, later holding or changed target supplies no settlement.
The authenticated settlement read rechecks the exact local attachment and quorum.

The scheduled move uses the commissioned clock error bound for the nodes and original journals.
The supplied switch deadline bounds the funded slot in which quorum recovery and route publication must finish.
The separately supplied incumbent drain cutoff is the greatest retained release bound.
Only actions on already-held snapshots can use the exact incumbent attachment after the fence, and they remain subject to their original per-action deadlines.
Timed appends keep the original journal cutoff and certificate.
An incumbent append whose cutoff could run past the fence is refused before journal dispatch.
The opening leaves a physical recovery interval within the switch slot.
Continuity at the public schedule depends on finishing that recovery and making the successor route usable at ingress before its first affected turn.

The control command is `rtdb-multi audience-writer-handoff`.
Its `prepare`, `switch` and `read` operations return canonical binary witnesses.
They require the exact handoff file, both member IDs, incarnation, installed cell secret, shared directory, clock error, switch deadline, and incumbent drain bound.
An abandoned unexecuted handoff retains its identity.
After a fence, retries open the same successor epoch even if its original slot has expired, and a late opening supplies no timely settlement.

#### Resource-enforced seat generations

The proposed durable seat fence gives the accepting resource one current generation for each deployment and turn.
The resource orders replacement and the generation check for each read, write and stream continuation together.
The replacement owns the logical seat at fence commit, and can recover that decision by exact readback if the reply is lost.
This transition needs no task-stop observation.
Physical capacity still includes requests and buffers held by the original generation unless their owner can retire them or bound them independently.
The installed cold object and pack transports have separate accepting orders from the directory claim.
A generation compare in Flight cannot change the ordering of requests that those providers have already received.
A reclaim deadline requires a service bound through durable fence commit and exact readback.
The current reservations and expiry sweep continue to cover the unfenced provider path.

**Resource model proof checked on 2026-11-24.**
TLC 2026.11.19.180144 exhausted 1,088 distinct states for one replacement and 13,120 for two in [the proposed resource model](../../verification/tla/AudienceSeatResourceFence.tla).
It checked exclusion of superseded I/O, unique logical seat ownership, acquisition at fence commit, and reclaim without stop evidence under weak fairness of the fence and exact readback.
All six mutations failed their intended laws.
Two witnesses reached a retained old charge after the fence and a successful successor operation with such a charge still present.
The model assumes that fence persistence and application share an accepting order and that fence and readback service eventually succeed.
The current Flight handoff change orders application only at local response admission.
Provider acceptance, already admitted reads and socket buffers keep their original custody boundaries.

#### Journal provenance evidence

On 2026-11-23, TLC 2026.11.19.180144 (`bc1b506`) exhausted 19,954 distinct states of [`JournalProvenance`](../../verification/tla/JournalProvenance.tla) for one journal stream, two retained entries, three writer epochs and two sample values.
It checked five safety laws covering original-only sync observation, refusal to replace a local original at the same LSN and entry epoch, preservation through compaction, restoration on replay, and exact reports of local provenance and samples.
Nine mutations failed their named laws.
They bypassed overwrite protection, dropped provenance or the sample during compaction or replay, sampled a copy locally, imported its remote sample, resampled a replayed original, or reported a copy as original.
Three witnesses report from a compacted base after restart and cover a sampled original, an original whose frame survived a crash before its sample was persisted, and a moved copy without a sample.
A fourth reports a later-epoch replacement without the superseded original's sample.

Source inspection ties the local marker to `Store::mutate`, which calls `track_sync` for the admitted `AppendObserved` and queues that observation with the append.
`StreamState::overwrite` checks for the observation slot at the matching LSN and entry epoch, including an empty slot before sampling or after a restart.
`Journal::flush` writes the origin marker with the append, then persists an observation after the append sync only while the same held entry owns the observation slot.
`Journal::compact` writes both pieces of evidence to the new base and `Journal::moved` changes payload locations.
`Replay::segment` restores the slot from the frame's origin marker and fills it from the observation frame, and `Store::digest_page` projects both into the exact reply.
A transfer into another journal uses the copy path with the receiving journal's own provenance.

The model assumes valid framing, successful durable publication of the modeled writes and bases, exact entry identification, and the surrounding authority and digest-read admission checks.
It abstracts the first frame sync and the second observation sync as separate transitions and takes filesystem recovery, clock bounds, cutoff enforcement and quorum selection as premises.

#### Framing decoder evidence

**Proof checked on 2026-11-24.** Verus 0.2026.11.20.9da409b with Rust 1.100.0 and Z3 4.16.0 checked 56 items in the [framing decoder area](../../verification/verus/framing_decoders/area.json).
All 27 mutations failed at their declared laws, including clearing the certification bit on write, on each of the two reads, and at the end of the composed round trip.

The checks cover the shipped source and replica header dispatch, replica frame and envelope mapping, eligible source, replica and journal round trips, fixed retirement bytes, and canonical Flight seat text.
The protobuf wire traversal checks field keys and lengths, progresses across the complete outer envelope, and bounds group nesting at 100.
Recovery hold detection validates all outer fields before using the presence of a length-delimited field 8.
The journal reader checks the declared length against the remaining extent before requesting the body, checks the checksum of the exact body, and decodes that body.
A proper prefix of an encoded frame cannot match a complete record, even when its shorter body could otherwise decode.
A read at a retained location also requires its whole extent to match the declared frame.
The frame writer and located decoder round-trip the whole record, including the certification bit, under the Prost contract.
The sequential reader returns the exact Prost-decoded record on every successful read, including the certification bit.
The Flight holder admits a seat header only for an audience handshake with exactly one value, and the parser requires the prepaid turn, one alternate digit, and the canonical non-nil attempt UUID.

**Source inspection.** Replay and retained payload lookup call the checked journal functions, and the Flight holder and gateway use the shared header names and seat parser.
The activation writer stores the publication mode in protobuf field 17 of the journal record, and replay restores it from that activation or from the compacted snapshot's own mode field.
The mode determines whether publication requires a decision certificate.
`JournalProvenance` abstracts retained entry origins and sync samples, and `ActiveLogletReadIndex` abstracts decisions and their quorum installation.
The [Prost contract](../../verification/verus/foreign_contracts/prost.md) is a premise for generated-message behavior, including full consumption, unknown fields and round trips within the supported nesting depth.
Protobuf permits non-minimal varints, valid extra fields and equivalent serializations.
The legacy source flush marker keeps its historical interpretation.
The remaining premises are exact reads, byte-buffer and endian operations, CRC calculation, available encoding capacity and journal bodies that fit the u32 length field, the existing placed-attempt codec and stage validator, canonical UUID formatting and parsing, and the HTTP metadata libraries' delivery of the header values.
Filesystem durability, application of replayed records to stream state, and the recovery decision to truncate the final segment are outside the decoder proof.
**Authored.** The Prost conformance cases include a wire-tag and reader fixture for both activation modes.
Native execution of those cases remains unverified.

### Query from the application

Funrun reads through the Conductor at the transaction's basis, served from the holder's tail and cache.

### Python, ADBC and Flight SQL

Usher terminates Flight SQL and routes by basis:

- a pinned or historical basis goes to the snapshot tier, on any node with a warm cache and no live index;
- a fresh basis goes to the holder, or to a log-fed subscriber if the deployment is hot.

This replaces Bijection's separate Flight SQL gateway.

#### Hosted write uploads

An installed placed writer can discover `rtdb.owned.DecideHostedStream.v1` through Flight `ListActions` and offer it in the `x-rtdb-owned-write` header on `DoExchange`.
Each `FlightData.data_body` carries one section or one complete Arrow IPC message.
The upload starts with the existing hosted decision prefix through the deadline and any journal cutoff, then the primary control groups and the packed fact schedule.
Each scheduled stream carries its relation and IPC byte length followed by its Arrow messages and IPC end marker.
An empty Flight frame seals the upload, and the owner requires clean end of input immediately after that seal.
The upload is limited to 16,384 frames and the existing 64 MiB message limit less 1,024 bytes, including the four-byte fact length accounted for in the unary carrier.
The owner allows 30 seconds to receive the complete upload.
The ordered decision uses the same installed writer proof, custody checks, effect ordinals, refusal alternative and commit receipt as the packed unary action.
An interrupted upload releases its request buffers, and a lost reply to a complete upload uses the original attempt recovery.

#### Audience action work

Each audience Flight action allows 256 object-store requests, 32 MiB of requested bodies, 4 MiB of provider metadata envelopes, 65,536 scan items and 64 MiB of decoding.
An exact object read spends its request, requested body and 16 KiB metadata envelope before dispatch, including a zero-body HEAD.
Exhaustion closes that ledger and refuses the operation.
Actions against one applied hold also spend that hold's shared allowance.
Nested work and detached provider attempts carry the same action ledger.
An independently admitted Flight action starts its own action ledger and continues to spend the original hold's aggregate when it uses that hold.
Cleanup closes use independent call capacity.

On 2026-11-23, Verus 0.2026.11.20.9da409b checked 14 items and rejected all 11 controls in the [audience action ceilings area](../../verification/verus/audience_action_ceilings/area.json).
That area checked the shipped action limits, requested-work charges, closed-ledger and retry decisions, and the decision to relinquish custody on readiness.

On 2026-11-23, the same Verus version checked 39 items and rejected all seven controls in the [audience drain wiring area](../../verification/verus/audience_drain_wiring/area.json).
The harness loads the production expressions for Flight owner capture, I/O admission scope, worker spawn, budget entry and debit, and the `poll_fn` body, and instantiates them with [RTDB-local runtime contracts](../../verification/verus/foreign_contracts/leaves.json).
It proves that any prefix of pending polls keeps the owner, that the ready poll relinquishes the worker's reference, and that dropping the caller's join handle preserves the original call permit, session seat permit, placement pin when present, and request memory reservation in the worker.
The reference model checks one release at the last strong drop, and the debit proof checks the exact action and turn charges against their acquired values, including refusal and ledger exhaustion.
The worker's poll observes the captured budget even when Tokio starts the spawned task with no task-local bindings.
The controls release on a pending poll, clear retained owners before spawn, discard an enclosing Flight owner, return the same permit twice, skip the turn debit, omit scope installation for the worker, and reset a nested budget.

Source inspection ties the shared expressions to the native Tokio and standard-library calls, the action wrapper, the I/O region's ancestor admission and task tracking, the Flight resource destructors, and result delivery or disposal before readiness.
The local contracts assume task-local poll and restore semantics, detach, last-reference and semaphore drop semantics, pin and poll behavior, and exclusive locked updates.
Their named conformance tests were authored and remain unrun.
The checked laws apply to provider work whose runtime stays live through drain and whose completion covers its provider effects.
Charge coverage before dispatch, retry-free audience transport, and provider completion time remain premises.

### Subscriptions, watches and exports

These read the deployment's log through `subscribe`, the RTDB counterpart of the `WorldStore` `changes` feed.
Convex-style reactivity stays on the holder, which already has the tail.

### Branch review and landing

- A durable branch is an RTDB fork in the same lineage: constant time, sharing the parent's objects.
- A review reads the fork.
- Landing is a commit at head, with the replay's inputs as its read set.
- Private, short-lived workspaces stay in Conductor memory, as today.

### Erasure

RTDB `ERASE` removes visible versions.
Assertion erasure keeps the original position and valid extent so the erased winner continues to shadow.
Each payload needs its own wrapped data key and destructible wrapping capability, including when several assertions belong to one erasure unit.

The local operation broker can stage an assertion envelope under a single-use identity scoped to the original deployment and incarnation.
It authenticates the wrapped key and sealed body for separate purposes and keeps the wrapping capability out of the general key and archive APIs.
A broker tombstone refuses both further opens and reuse of that stage.
The hosted S3 key service has a distinct object prefix for assertion capabilities.
It refuses versioned custody and uses conditional writes for staging and withdrawal.
Native ingress, exact ordered placement, current disclosure and the multi-custodian destruction barrier still need to use this envelope before assertion erasure can be offered.
See [the fixed assertion deadline](../../verification/redesign/R7-eraser.md#option-2-assertion-deadline) for the private custody ledger, passive backup frame and deployment obligations.

### Wake

A request for a parked deployment activates it with one directory CAS and one GET of its boot object, about 10–300 ms depending on the cache.
Whole-cell wake works as today, but recovery needs only the directory and object storage: there are no recovery roots and no retained Kafka suffix to replay.

## The `WorldStore` contract on RTDB multi

The contract is `bijection/crates/storage_contracts/src/world.rs`, described in `bijection/llm-wiki/wiki/storage-engine-contract.md`.

| Kernel operation | RTDB multi | Status |
| --- | --- | --- |
| `read(world, at, range)` | A query at a basis: the snapshot tier at or below the checkpoint, the holder or a subscriber above it | Covered |
| `commit` with label-filtered read-set validation and a deadline | A routed transaction with `ASSERT NOT EXISTS` per read range, as `XtdbWorldStore` does today; later a native validation hook in the resolver (Architecture B) | Covered; the native hook is an optimisation |
| `Op::Merge` (`AddI64`, `MaxU64`) evaluated at the commit position | The holder resolves serially, so `UPDATE … SET n = n + k` runs at the commit position | Covered, and simpler than the XTDB adapter's merge at read time |
| `fork(world, rev)` | Lineage fork, constant time | Covered |
| `pin(world, at, until)` | Not yet in the spec: the reaper protects only snapshots shorter than `max_snapshot_lifetime` | Gap 1 |
| `erase(unit)` | `ERASE` removes visible versions | Protected custody and physical erasure remain open |
| `changes(world, after)` | Log `subscribe` above the checkpoint; below it the log has been trimmed | Gap 2 |
| Receipts and lost-response recovery | The idempotency window covers `idempotency_window`; kernel receipts outlive it | Receipts stay kernel keys, unchanged |
| `drop_world` | Deployment deletion, or fork deletion with shared objects transferred | Covered |

## What the spec needs for Bijection

These are recorded as open questions in `rtdb-multi.allium`, to be folded into its rules as they are designed.

1. **Long-lived pins.**
   A deployment record carries pins, each a basis and an expiry.
   The reaper keeps every object reachable from a pinned manifest, and a flush keeps the tail a pin depends on reachable.
   Pinned reviews and Flight readers need this.
2. **A change feed below the checkpoint.**
   A subscriber that falls behind the trim point resumes from blocks, as a scan ordered by system time between two bases, then switches to the log, so exports and watches never see the gap.
3. **Embedding as a supported form.**
   The spec treats a node as a process.
   Bijection needs a node-library contract: a Conductor embeds the holder role, the gateway-to-node RPC becomes an in-process call, and the Conductor's memory budget and NVMe cache are the node's.
4. **Many forks per deployment.**
   Each fork is a directory record of about 300 bytes, with its own log only while active.
   The directory's sizing must count branches, not just deployments.
5. **Label flow at the commit position, natively** (optional).
   An admission hook in the resolver avoids one `ASSERT` per read range on large read sets; this is Architecture B's bounded admission hook.
6. **Search inputs.**
   Search segments are built from the change feed, as today but from a different source, unless search moves into RTDB later.

## What Bijection retires

**Outright** (storage engine and cache), about 82k lines:

| Crate | Lines |
| --- | ---: |
| `tiered_store` | 39k |
| `tenant_lsm` | 29k |
| `storage_format` | 7k |
| `lsm_compaction` | 2.7k |
| `object_cache` | 2.1k |
| `tenant_lsm_s3`, `storage_io`, `remote_storage` | 2.3k |

**In large part** (ordering, recovery, placement and physical branches), to be made precise from the boundary inventory in `storage-engine-contract.md` (about 140 entry points):

- the publication queue, recovery roots and archive, and catalog placement in `native_commit` (111k lines in all, much of it kernel that stays);
- `publication_workers` (12k), `metadata_domain` (20k), and metadata installation and assignment in `native_store_runtime` (11k);
- the physical-fork parts of `branch_engine` and `branch_runtime` (46k together).

**From operations**:

- the Publication Applier service pair;
- MSK Kafka on the commit path (it may stay for CDC sources);
- recovery-root custody;
- the separate XTDB or RTDB service, and its pgwire hop;
- two of the three placement authorities that would otherwise coexist: the catalog placement, the store's `WriterEpoch` and head lease, and RTDB's term.

**Conceptually**, Bijection keeps one fence (the RTDB epoch), one placement record, one log per deployment, one storage format, one cache and one reaper, where today it runs two orderings — the cell's Kafka history and the store's publication queue — each with its own fence and its own garbage collection.

## Migration

### Hosted Classic source seal

The authenticated Classic recovery listener accepts `seal_classic_migration_source` for a directory-backed Classic deployment.
Close application write and protected delivery admission and settle their outstanding work before selecting the capture.
`capture_checkpoint` returns the capture operation, source residence and log epoch, exact checkpoint coordinates and manifest digest.
Use `observe_classic_migration_source` under that recovery envelope with `source_manifest_sha256` to get the live Classic directory writer epoch.
The positive `classic_migration_source_observed` reply carries `point`, `manifest_sha256`, `source` (`incarnation`, `log_epoch`, `writer_epoch`), `current_checkpoint` and `directory_version` for the exact installation.
Compare `current_checkpoint` with `point.checkpoint` before the first seal send.
If a maintenance flush already advanced the checkpoint and no seal was sent, retain the outer migration intent and reserve a new capture and scratch attempt.
If a seal may have been sent, recover the same seal request.
An observation is a preflight snapshot, and the directory fence still checks the exact current checkpoint.
If the seal returns `classic_migration_capture_overtaken` for the exact selector, the directory has durably closed that scratch identity before a fence could select it.
Keep the outer migration intent and reserve a new capture and scratch attempt after that outcome.
Generic refusal, deferral and transport errors leave the attempt unresolved.

Send the ordinary recovery envelope with the exact source `name`, `authority` and `capture`.
The action carries `destination` (the expected native UUID), `stage_operation` (the stable attempt UUID), `source_manifest_sha256` and `source` with `incarnation`, `log_epoch` and `writer_epoch`.
Reuse all fields after an interrupted reply.
The Classic writer closes its submission gate, drains admitted transactions and records its applied source position in the directory fence before the recovery owner seals and verifies the original replica prefix.
The source terminal includes the drained source frontier, exact source and replica offsets, original transaction key, certified suffix digest where present, lifetime-floor digest and any outstanding placed debt.
The selected capture and private scratch stage remain available for recovery.

After sealing, `verify_checkpoint_transfer_target` can retain the certified suffix under the scratch stage's key.
For this action, both `destination` and `stage_operation` are the scratch UUID, and `staged_manifest_sha256` is the value in the sealed fence's `destination` stage.
The returned `transfer_target_verified` receipt carries the same terminal and the ciphertext digest of the exact scratch replay artifact.
The verifier replays from the copied checkpoint and checks the terminal transaction, source frontier, placed debt and lifetime floors before accepting the artifact.
The scratch stage can hold replay data for native import while its Classic replica and writer admission stay closed.
Use `inspect_classic_migration_inventory` under the original recovery envelope to read the prepared input from scratch storage.
Supply the promised native UUID as `destination`, the scratch UUID as `stage_operation`, and the exact `staged_manifest_sha256` and replay `ciphertext_sha256`.
The `classic_migration_inventory_inspected` reply carries the exact replay receipt, per-relation event commitments and counts, and a commitment to retained Classic Worlds, suffix pins, captures and pending capture releases.
The inventory distinguishes literal, Classic outcome, owned and system relations and flags unpositioned, redacted and erased events.
This observation works from the scratch checkpoint and certified replay artifact after source copy verification.
It checks the original terminal in the directory and takes no original journal or object admission.

The attempt UUID identifies a private Classic scratch stage independently of the native destination UUID.
Current Classic staging requires an active source holder and the current captured checkpoint.
An active retained World or replica suffix pin blocks this source fence.
`discard_classic_migration_stage` accepts the same selector before fencing and reports the exact scratch disposal receipt once available.
After fencing, recover the same attempt from its retained state.

### Private native candidate

The Original recovery owner authenticates the existing recovery envelope against the exact fenced Classic `name`, `authority` and `capture`.
The destination keeps that name and authority UUID, with a separately proved native catalog reservation, physical stream and encrypted manifest.
Candidate operations cross a private RTDB relay between the Original recovery owner and an enrolled native Multi peer.
The peer executes inside its running runtime with its own journal credential slot and storage authority.
The native ingress uses server-authenticated TLS and requires both an operation grant from the migration operator and proof from the Original recovery owner.
The operator approves the recorded Bijection Begin through its trusted approval signer.

All actions take a `selection` with `destination`, `stage_operation`, `staged_manifest_sha256`, `ciphertext_sha256`, `command_id`, `command_digest` and optional `original_receipt_table`.
`destination` equals the original authority UUID.
`stage_operation` selects the independent Classic scratch installation named `checkpoint-stage-{stage_operation}`.
The sealed stage intent and scratch directory record must authenticate that scratch name.
The command ID and digest identify Bijection's retained migration request.
Choose the exact public receipt-control table with `{schema, table}` when the migration needs Classic receipt read-through.
Every reply uses `classic_migration_destination` and returns the exact `selection`, old terminal transaction and frontiers, `terminal_sha256` and `request_sha256` in `evidence`.

- `prepare_classic_migration_destination` takes the exact retained `begin_sha256` and verifies the scratch replay.
  It returns `projection` and, for an installable projection within the 64 MiB checkpoint limit, `candidate_binding`.
  Repeat the same authenticated preparation after a lost reply to recover the same binding while the exact verified scratch remains held.
- `stage_classic_migration_destination` takes a stage `authorization`, verifies that scratch replay yields the approved binding and returns `projection` and the closed native `candidate`.
- `install_classic_migration_destination` takes an install `authorization` and the exact staged `projection_sha256`.
  The relay resolves the candidate stream's selected native peer, which needs the actual private writer lease to commit the restore checkpoint.
- `classic_migration_destination_status` takes a status `authorization` and reports the exact ordered reservation.
  Once the restore commits, any enrolled ingress peer can reopen that checkpoint through the native journal without acquiring a writer lease.
  The Original owner checks the retained directory fence and scratch bootstrap for status without replaying the artifact.
  Supply optional `classic_cut` with the old `tx_id` and `system_time` to verify an exact retained cut against the reopened native checkpoint and return `mapped_cut`.
- `read_classic_migration_original_receipt` takes a typed `id`, such as `{"String":"<original receipt row ID>"}`, and returns the selected Original control row history from the exact scratch replay.
  Each retained event includes its original valid-time extent, system time, position, digest and literal document where present.
  Pass `coverage` with `{table: {schema, table}, id}` to return `original_receipt_coverage` from that same verified replay.
  This read uses the Original recovery authority and scratch hold and can proceed independently of the native catalog.
  The receipt request frame is bounded at 4 KiB including its newline, and each returned control history is bounded at 1 MiB.

Once prepared, keep the same selection and full `candidate_binding` after any indeterminate response.
Stage, install and status echo that binding in `evidence` on success.
For a lost install response or an expired grant, get a fresh status grant for the same binding.
Grant expiry or an unavailable reply leaves the install outcome unresolved.
The native proof reports the candidate's catalog reservation basis and the restore record's LSN, writer epoch, key generation, manifest address, ciphertext digest, and checkpoint state digest.
The source and destination terminal times must agree.
A native terminal establishes private installation only.
The ordered catalog leaves the candidate closed to serving, workload Claim and ordinary attachment through the held-readiness step.

The Original reaper takes `--recovery-native-destination FILE` for relay configuration.
The JSON file has `entry` (host), `port`, `server_name`, `ca_file` (PEM), `catalog` (native catalog UUID), `approval_public_key` and `source_signing_key_file` (Ed25519 PKCS#8 DER).
Enrolled native runtimes take `RTDB_CLASSIC_CANDIDATE_INGRESS_FILE` with `port`, `certificate_file` and `private_key_file` (PEM), `approval_public_key` and `source_public_key`.
The ECS cluster module instead supplies `RTDB_CLASSIC_CANDIDATE_INGRESS_PORT` and `RTDB_CLASSIC_CANDIDATE_INGRESS_CONFIG` together.
The secret JSON holds `certificate_pem`, `private_key_pem`, `approval_public_key` and `source_public_key`.
Select one configuration source for the native node.
The TLS trust profile covers the ingress entry and eligible native peers on the configured private port so the Original relay can follow a selected-writer redirect within RTDB.
Keep the Original recovery authority and source signing key on the Original owner.
Keep the approval signing key under the independent migration approval authority.
The mounted files, private reachability and exact native ingress endpoint require operator commissioning.

`authorization` has fields `grant` and `signature` in that order in the signed relay transcript.
The `grant` fields in signing order are `binding`, `operation` and `expires_at_micros`.
The `binding` fields in signing order are `begin_sha256`, `request_sha256`, `catalog`, `name`, `destination`, `capture`, `scratch`, `terminal`, `terminal_sha256`, `artifact_sha256`, `projection_sha256`, `checkpoint_sha256`, `checkpoint_bytes` and `basis`.
`operation` is `{"operation":"stage"}`, `{"operation":"install"}` or `{"operation":"status","classic_cut":null}` with an optional exact transaction key in place of `null`.
A transaction key serializes as `{"tx_id":7,"system_time":200}` and a basis as `{"incarnation":[…],"position":8}`.
Use lowercase hyphenated UUIDs, decimal integers and JSON integer arrays for keys, digests and signatures.
The operator produces a 64-byte Ed25519 signature over `SHA-256("rtdb.multi/classic-candidate-approval/v1\0" || compact typed JSON(grant))`.
The Original relay separately signs `SHA-256("rtdb.multi/classic-candidate-original/v1\0" || compact typed JSON(authorization))`.
Each domain ends in one zero byte.
Expiry is in Unix microseconds and must be more than 250 ms and at most five minutes ahead of the verifier's clock.
The ordered catalog and restore record bind `SHA-256("rtdb.multi/classic-candidate-binding/v1\0" || compact typed JSON(binding))`.
The private relay request frames its `authorization` and `source_signature` JSON with a big-endian 32-bit length, then adds a big-endian 32-bit checkpoint length and the exact checkpoint bytes.
Stage, status and held-readiness actions carry zero checkpoint bytes.
The reply is a length-framed JSON outcome.

#### Ordered held readiness

Use `hold_classic_migration_destination_ready` with the same Original `selection` and a fresh `authorization` over the same full `candidate_binding`.
The operator approves the exact Bijection destination and placement from the reobserved retained Begin, and the exact Classic application fence from its matched application-drain proof.
Use `classic_migration_destination_ready_status` with the same `selection`, binding and readiness selector after a lost response.
Both actions authenticate the Original fence and scratch bootstrap and relay through the commissioned catalog audience without replaying scratch.

The signed operation is `{"operation":"hold_ready","ready":R}` or `{"operation":"held_ready_status","ready":R}`.
`R` has fields `endpoint`, `placement`, `classic_cut`, `native_basis`, `reserved_at` and `installation` in signing order.
`endpoint` is the exact expected managed Flight HTTPS origin from the retained Begin.
`placement` has fields `epoch` as a JSON integer, then `assignment` and `holder` as 16-byte JSON arrays.
Use the complete Classic application transaction key for `classic_cut` and its checked native `Basis` for `native_basis`.
Use the original native catalog reservation Basis for `reserved_at`.
`installation` uses the exact observed native checkpoint with fields `basis`, `system_time`, `checkpoint_lsn`, `checkpoint_writer_epoch`, `key_generation`, `checkpoint_sha256`, `manifest` and `manifest_ciphertext_sha256` in signing order.
The registered endpoint is an approved Bijection placement coordinate, and the relay authenticates the native peer independently with its commissioned TLS profile and native catalog audience.

The peer reopens the ordered native restore, checks the full physical installation and proves the application cut against the retained imported history.
The hold compares the original catalog reservation version and orders a single selected readiness value in that same closed candidate row.
The `classic_migration_destination` reply includes `candidate_binding`, `candidate`, `mapped_cut` and `held_ready` in `evidence`.
`held_ready` contains `binding_sha256`, `ready` and `recorded_at`, where `recorded_at` is the catalog Basis that ordered the hold.
An exact status before the hold commits omits `held_ready` and leaves the hold unresolved.
Status with a different readiness selector refuses after a hold commits.
Renewal or expiry of a grant leaves the recorded hold intact, and a fresh exact status returns the original `recorded_at` and ready value.

The held row keeps attach, workload Claim and application writes closed.
The readiness actions carry prepublication authority only.

#### Ordered native opening

Bijection's guarded route CAS orders the selected placement and the exact pending native opening interlock together.
The interlock holds the selected route and holder until Bijection's installed reconciler receives an exact ordered native terminal.
RTDB verifies the decision through `/bijection.native.v1.Administration/VerifyHostedHistoryOpening`, a unary gRPC call with raw UTF-8 JSON messages over mutually authenticated TLS.
RTDB sends `{format:1,history_id,target,expected:decision}` to the commissioned Bijection node and checks the reply's `format`, `history_id`, `source` and exact `hosted_history_opening_current` decision and held-ready record.
The `decision` fields are `scope` (`domain` and `authority` UUID), `attempt` (`command_id` and `command_digest`), `begin_sha256`, `applied_at` (`history_id` and `sequence`) and `decision_sha256`.
The `target` is the commissioned `AdministrationEndpoint.node` value.
Refused, unknown and unavailable answers leave the candidate closed.

Use `RTDB_CLASSIC_OPENING_INGRESS_FILE` to commission a separate private mTLS listener on enrolled native peers.
Its JSON fields are `port`, `certificate_file`, `private_key_file`, `client_ca_file`, `reconciler_certificate_sha256`, optional `operator_certificate_sha256` and optional `verifier`.
Certificate and key files are PEM, and each caller digest is the SHA-256 of its leaf certificate DER bytes, encoded as a 32-byte JSON array.
An opening operator requires a distinct certificate digest and a `verifier` with `endpoint`, `history_id`, `target`, `ca_file`, `client_certificate_file` and `client_private_key_file`.
The outbound client certificate must be installed in Bijection's dedicated opening verifier role.
A status-only peer can omit both the operator and verifier configuration.
The native ECS module uses an alternative bound selector with `RTDB_CLASSIC_OPENING_INGRESS_PORT` and `RTDB_CLASSIC_OPENING_INGRESS_CONFIG`.
Supply the port separately and the TLS material in the version-pinned JSON as `certificate_pem`, `private_key_pem` and `client_ca_pem`.
The optional verifier has `endpoint`, `history_id`, `target`, `ca_pem`, `client_certificate_pem` and `client_private_key_pem`.
Keep the caller leaf digest fields and the paired operator and verifier requirement.
Only one configuration form can be installed.
The operator follows any selected-writer redirect over the same commissioned TLS trust profile.
The Bijection reconciler uses its distinct read-only credential and neither peer operation consumes the Original recovery bearer.

The private request and reply each frame one UTF-8 JSON object with a big-endian 32-bit length and a 16 KiB limit.
Requests have `operation`, the complete `decision`, and `held_ready`.

- `open_classic_migration_destination` verifies the live interlock and orders an opening reservation against the exact closed held-ready Basis.
  The selected native writer reopens the imported stream and checks the closed Classic application row at the exact mapped cut.
  It installs the initial native fence at the exact authority point of the commissioned workload profile for the held assignment and epoch, then the native catalog orders `opened`.
  The mounted assignment issuer profile is held through native ordering and compared again at attachment.
- `exclude_classic_migration_opening` verifies the same live interlock and orders `excluded` only while the held-ready row still has no opening reservation.
  Opening and exclusion contend on that row and its ordered Basis.
  A reserved opening remains unresolved until the same operation completes.
- `classic_migration_opening_status` reads the exact historical terminal over the read-only credential.
  It does not call the live verifier, and returns `unresolved` while no positive terminal can be read for this selection.

Positive terminals carry `outcome`, `decision`, `held_ready` and the stable `ordered_at` native catalog Basis.
`opened` also carries `workload_fence` with `placement` and the exact native `applied` receipt coordinate from the selected stream.
An identical open retry can recover a lost data-stream append before ordering the native catalog terminal.
Historical status after loss or Bijection settlement reports that ordered outcome and carries no live authorization.
The exact initial workload fence or a later positively ordered fence on the same native stream admits managed workload claims and effects.
An advance requires a verified closed writer or vacant predecessor at or after the imported fence.
The active writer verifies the referenced receipt, ordered placement and exact current authority row before a write.
Scoped attempt settlement retains the original namespace, key, family and digest and exposes only terminals ordered after the imported fence.
Ordinary SQL, ordinary v1 writes and generic provisioning remain closed for this candidate, and the old Original keeps its independent close and retained receipt authority.

The literal projection covers positioned clear puts and deletes with unambiguous physical addresses.
It preserves valid intervals, system times and original written transaction identity.
It also covers uniquely anchored unpositioned events with disjoint same-transaction intervals and keeps their written basis absent.
Retained source transaction `n` maps to native position `n + 1`, provided the exact source clock and transaction are present in the imported prefix.
The proof commits to the complete mapping of admitted cuts and the literal source inventory.
An authenticated inline shared checkpoint can contribute canonical v1 receipts whose installed source coordinates map exactly to those cuts.
The copy preserves each canonical namespace, key, fingerprint and outcome, and its Original source coordinates remain in the retained evidence.
The application selects its Original control relation and exact typed request identity, and selects any separate registered-coverage control at the same cut.
Use the complete stamped control rows when matching request identity, retained coverage, caller standing, disclosure and the application outcome.
The application owns the exact control relation names, field interpretation and receipt namespace.
Retained history evidence carries no live authorization witness.
A native transaction reference is usable only when the Original receipt already carries authenticated evidence from that producer.
A Classic-only receipt stays keyed by its Original business identity throughout recovery.

Projection coverage identifies retained Classic bookkeeping, withheld protected carriers, owned relations, system relations and literal relations that need further event translation.
A sealed canonical checkpoint object needs its original generation and key evidence.
Canonical suffix decisions need authenticated shared-fold replay, and canonical branch, visibility, workload, erasure and custody controls need a compatible native representation.
The candidate also needs proof for any Classic outstanding placed reservation, source erasure or redaction, unresolved retained World, suffix pin, capture and pending release.
Reciprocal custody for protected history requires the Original contributor census, exact forward and reverse binding and positive holder, reader and release disposition.
The existing native checkpoint protocol requires distinct native source and target incarnations and an active Original ordering owner for that binding.
Keep the Classic media and credentials for retained obligations while the closed candidate awaits the missing proofs.

| Phase | Bijection | RTDB plan stages it needs |
| --- | --- | --- |
| **A0** (now) | `XtdbWorldStore` over pgwire against stock XTDB; the `WorldStore` conformance suite gates it | none |
| **A1** | The same adapter against RTDB in classic mode, proving parity | RTDB as it is |
| **A2** | The same adapter against RTDB multi behind a gateway; a deployment is an RTDB deployment, and the operator binding JSON is replaced by the directory | 0–4, 9 |
| **B1** | The Conductor embeds the holder; the cell catalog's placement epoch is the directory epoch; the adapter calls in process | 0–8, and gap 3 |
| **B2** | The kernel's storage moves onto `WorldStore` on RTDB for every domain; the tiered store and tenant LSM are retired; the Appliers stop ordering commits | 5–7, 10, and gaps 1–2 |
| **B3** | The Appliers and the Kafka cell history are retired; the cell catalog lives in the directory; the Go controller drives placement through directory CAS | 11 |
| **B4** | Durable branches become RTDB forks; deletion uses RTDB's provable deletion; Flight SQL goes through Usher | 12 |
| **C** | Cells and cell moves on RTDB; dedicated cells become the dedicated isolation class | 13 |

Each phase rolls back on its own, because the `WorldStore` conformance suite gates every swap.

## Decisions

1. **Embedded or separate.**
   Embedding the holder in the Conductor gives the largest simplification and the lowest latency: shared placement, no hop.
   It couples Conductor and RTDB release cycles.
   The recommendation is embedded, which phase B1 assumes.
2. **Who owns the directory.**
   Either the RTDB directory becomes Bijection's cell catalog, or Bijection's catalog implements RTDB's `Directory` contract; the Appliers already apply epoch-checked placement and could be the first backend.
   In the end there is exactly one, and the recommendation is the RTDB directory, since RTDB's single-writer safety rests on it.
   Its production backend is DynamoDB, which Bijection already runs for key authority, and its log servers are logkeepers on EC2 with instance-store NVMe, like the Conductor hosts.
3. **Search.**
   Built from the change feed with ownership unchanged, or moved into RTDB later.
4. **Files and blobs.**
   They stay in Bijection's per-tenant buckets; moving them under RTDB's storage roots and reaper is possible and not required.
