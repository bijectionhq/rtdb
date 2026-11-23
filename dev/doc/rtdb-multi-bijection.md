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
Recovery of a retained guarded tip records its commit on the predecessor quorum before copying the tip to a new ensemble.
An interrupted copy can therefore recover the predecessor's selected outcome while the directory still names that predecessor.
The raw timed entry binds the original stream, journal identities and quorum size across moves and archives.
The audit reports current quorum agreement separately from the original quorum's sync bound.
It can read a retained original sample matching the certified entry even when that journal lacks a local commit marker.
The sync bound is the original quorum's order statistic among distinct samples below the stored cutoff.
It remains absent if the required original samples are unavailable.
It also reports the audit process's rounded clock immediately after establishing quorum agreement.
The audit time includes probe latency and requires bounded clock uncertainty when compared with the writer's sample.

The installed audience Flight gateway discovers its existing holder with its own DynamoDB client.
It reads the exact deployment and that deployment's member by key and dials the configured audience port on the member's advertised host.
A missing, stale, or parked placement makes the audience handshake unavailable.
The writer lifecycle remains with the placement owners. At the holder, acquisition verifies the recovered source and writer quorum.

Each installed holder reserves two Flight session seats per prepaid turn.
The gateway and holder parse the same seat selector, and the gateway requires the holder to acknowledge the exact selector and mint attempt before wrapping a new session token.
A live seat can recover its original session under the same credential and exact mint attempt.
For the same deployment, the holder compares the candidate with the current local attachment before retiring a displaced or expired occupant.
Close, expiry and displacement can end a registry entry while its session still owns the permit.
A lost close request leaves the existing session in place until retirement, and a lost reply leaves the permit charged while original I/O drains.

On 2026-11-23, Verus 0.2026.11.20.9da409b checked seven obligations and rejected fourteen controls in the audience session seat area.
See [the area manifest](../../verification/verus/audience_session_seats/area.json) for the mutations and expected diagnostics.
The area checks the shipped alternate parser, registry size and indexing, attachment and admission decisions, and a lost-close model that invokes the shipped admission decision.
Source inspection ties vacancy to an acquired semaphore permit, retry to lookup of the original live registry entry, and retained provider I/O to a strong reference to the session through its I/O admission.
The checked bound applies to one installed holder reservation and assumes those runtime ownership and serialization rules, the sampled catalog, and authenticated identity comparisons.
Cross-holder placement and the time for provider I/O to drain require their own evidence.

### Query from the application

Funrun reads through the Conductor at the transaction's basis, served from the holder's tail and cache.

### Python, ADBC and Flight SQL

Usher terminates Flight SQL and routes by basis:

- a pinned or historical basis goes to the snapshot tier, on any node with a warm cache and no live index;
- a fresh basis goes to the holder, or to a log-fed subscriber if the deployment is hot.

This replaces Bijection's separate Flight SQL gateway.

### Subscriptions, watches and exports

These read the deployment's log through `subscribe`, the RTDB counterpart of the `WorldStore` `changes` feed.
Convex-style reactivity stays on the holder, which already has the tail.

### Branch review and landing

- A durable branch is an RTDB fork in the same lineage: constant time, sharing the parent's objects.
- A review reads the fork.
- Landing is a commit at head, with the replay's inputs as its read set.
- Private, short-lived workspaces stay in Conductor memory, as today.

### Erasure

RTDB `ERASE` removes visible versions. The kernel also requires every retained copy of erased protected data to become unreadable within its physical erasure bound. Meeting that bound requires per-unit custody for durable bytes and independent erasure evidence through restores.

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
