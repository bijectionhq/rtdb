# RTDB multi: implementation plan

This plan takes RTDB from today's single-cluster model to the endgame specified in [`allium/rtdb-multi.allium`](../../allium/rtdb-multi.allium): an unbounded number of deployments on shared, autoscaling cells, each deployment costing in proportion to its activity.
It is delivered as one long-running branch, `rtdb-multi`, merged as one PR, built from small commits that each carry their own tests and spec changes.

The spec is the target; this document is the route.
How the endgame fits Bijection's hosting infrastructure, and what it lets Bijection retire, is [`rtdb-multi-bijection.md`](rtdb-multi-bijection.md).
It names, for every part of the spec, the code it grows from, the code it replaces, and the order in which the work lands.
Paths are relative to `rust/crates/` unless they start with another top-level directory.

## Contents

1. [Implementation status](#implementation-status)
1. [Ground rules](#1-ground-rules)
2. [Where we start from](#2-where-we-start-from)
3. [Concept map: spec to code](#3-concept-map-spec-to-code)
4. [New crates](#4-new-crates)
5. [Stages](#5-stages)
6. [Existing specs affected](#6-existing-specs-affected)
7. [Testing strategy](#7-testing-strategy)
8. [Decision points](#8-decision-points)
9. [Commit sketch](#9-commit-sketch)

## Implementation status

What runs on the `rtdb-multi` branch, stage by stage, and where it still departs from the spec.
Update this section as work lands.

`rtdb-multi` runs every cell process: `directory`, `cell-map`, `logkeeper`, `node`, `gateway`, `controller`, `reaper`, plus the administrative commands `create`, `list`, `status`, `drain`, `pin-engine`, `roll-engine`, `move`, `move-cell`, `read-width`, `unquarantine`, `fork`, `delete`, `deletion-key`, `verify-deletion`, `audit` and `audit-log`.
`crates/multi/tests/cell.rs` drives an in-process cell (or two) through tokio-postgres; `tests/chaos.rs` runs a writer against a seeded sequence of faults and audits the history afterwards; `tests/footprint.rs` measures an idle deployment.

| Stage | Built | Departures and gaps |
| --- | --- | --- |
| 0 | `tests/footprint.rs`: in a debug build, an open idle (empty) deployment costs about 94 KiB resident and 9 tasks, and activates in about 8 ms; the disk cache is keyed by the store; a placed node's query opens only its own database ; `StorageRoot` builds every database's storage root and puts it under a provider's prefix | multi derives each deployment's prefix from its record, above the `StorageRoot`, rather than giving `StorageRoot` an epoch-named multi layout |
| 1 | a session on a placed node sees only its own database, including its transaction inspection commands and await positions; one pool's idle cache entries no longer starve another's; a node bounds its holdings' unflushed rows together (`--max-tail-rows`); a deployment's store refuses any other deployment ; in classic mode, a user list's `grants` limit users to the databases granted them | |
| 2 | the directory with in-memory, TCP and DynamoDB backends (DynamoDB tested against DynamoDB Local); members, leases, pools; `Membership::Placed`; claims without elections; idle terms append nothing. The local authority can persist its directory on a private volume. Each local holding reconciles against the shared watch from its claim version onward. | watches follow the backend's change feed (DynamoDB Streams on DynamoDB, a subscription over TCP), with a poll every ten intervals as a safety net; each process follows the cell feed once, and receivers with the same deployment and interval share one watch and safety-net poller; DynamoDB stream startup refreshes watched keys after its iterators open and scans the table only for whole-feed consumers; a separate cell-wide watch service remains open |
| 3 | one quorum append per transaction: the only durable stream is the replica stream, and the source log is the holder's in-process intake, started past every position the replica stream may hold; a transaction carrying an idempotency key in its metadata commits at most once, and a retry is answered with the original's outcome ; the gateway keys each command of a writable session and can replay an unanswered single autocommit write using a private permit acknowledged by both writer versions. A duplicate's outcome comes from durable metadata in its own deployment and at the retry's system time. | a command partly answered, or one that ends an explicit transaction begun on a former holder, is still reported as of unknown outcome; keying adds an assertion and a put to each transaction. A replay waits for acknowledgement of the protected key |
| 4 | the logkeeper: append-time epoch promises, seal, recovery by synced epoch, background repair, seal announcements and `confirm_leadership`, moving a stream to a new ensemble, segment files shared by every stream on a server, from which retained entries are read on demand: a server holds each stream's index of entry locations and a bounded cache of recently written or read payloads (`--cache-bytes`, 64 MiB by default). A journal opens before its member is advertised and retains its identity when the process restarts. | segments are per server rather than per writer member; segments are retired by compaction, which copies every retained entry into a new base segment, rather than one by one as their entries are trimmed |
| 5 | the store is fenced by the holder's epoch, cannot delete (a delete is a garbage record), and the reaper erases released objects and packs after a grace period. Managed S3 writes record their multipart identity before sending payload bytes and complete conditionally with a deployment-scoped grant. | Writer epochs in object paths and independently issued credentials remain open. The holder, conditional publication and checkpoint recovery checks protect the current storage layout. |
| 6 | a block flush writes one object, its block's pack, which holds the catalog and is the boot object; placed nodes flush within 15 minutes and on park; the directory's checkpoint names the last published block, and a cold start finds its latest block by probing forward from it, and reads that block's pack, which names every other, listing nothing. Explicit flushes publish the adopted checkpoint before replying. | the boot object inlines the catalog and per-table block files but not the top trie levels, which are read from their own level-0 pack entries |
| 7 | compaction workers compact the deployments that rank them first and relay the result to the holder, which publishes it; holders on such a cell run no compactor | |
| 8 | parking after idleness, under `--max-active`, on drain, and waking on the next connection; operation locks are released with their last users and trim watermarks with their holdings. Ordinary parking drains admitted writes and publishes the adopted checkpoint when releasing the holder. | Opt-in resident-idle retains the placement and checkpoint, closes local execution resources, and wakes through a new epoch. A local in-memory directory run measured 100,000 resident placements with zero additional node execution bytes or tasks. |
| 9 | the gateway reads pgwire frames, survives a planned move or a holder's death while the session is idle, replays the startup and prepared statements, and replays eligible unanswered commands after losing or receiving a refusal from a former holder; once the first holder has authenticated the client, each reconnection carries a capability the gateway signs with the cell's secret, which a node accepts in place of the password for that deployment alone. The pgwire gateway can terminate TLS and bind a connection to a specific installation. | The first connection's authentication runs on its node. The Flight SQL gateway supports one cell and binds server-local sessions to the current writer. Transparent Flight SQL recovery and cross-cell routing remain open. |
| 10 | read replicas fed by the log for read-only sessions within a deployment's read width; opt-in linearizable reads confirmed with the logkeepers before each query ; a snapshot session (`rtdb.read=snapshot`) reads the deployment as of its checkpoint on any node, the holder last, opened from object storage with frozen logs and no logkeeper, and closed before the reaper's grace runs out, serving any basis at or below the checkpoint ; a deployment whose persisted tries outgrow a node's disk cache (by default; `AgentOptions::fragments` sets the threshold and depth, and no command-line flag does yet) splits each whole-table scan into 16 trie-prefix fragments, each read at the coordinator's basis by the node that ranks first for `{storage identity}/{table}/{prefix}` among the nodes that may take placements, which opens a log-fed replica for it if it has none, and answered over the control protocol as Arrow IPC; a fragment whose reader fails or is not at the basis is read by the coordinator itself. Pgwire read-only sessions spread across the available replicas within the configured width. | scan fragments are split by the scan on the coordinating node, not by a cell-wide planner, and the size test is the deployment's persisted trie bytes against the node's disk-cache size rather than `PerMemberCacheShare`; a fragment carries the projection, the evaluated temporal bounds and the predicates any node evaluates alike, but not those on parameters, outer rows, the clock or chance, which the coordinator alone applies, and comes back whole in one response rather than streamed, a slice too large for one response being read by the coordinator instead; a reader at or past the coordinator's basis serves a fragment as of the basis, except that what was erased since is gone; `_id` lookups and multi-partition databases are never split |
| 11 | the controller reaps expired members, confirms silent nodes dead by probing, and rebalances; planned moves; quarantine to the isolation pool; `status` as the autoscaling signal. Member probes have bounded concurrency, drains move one holding at a time, and `status --placements-only` reads indexed demand by pool. | Autoscaling acts on the reported signal outside RTDB. The controller can rebalance using write rate, query CPU, live batch and cache byte watermarks. The controller can also adjust Pgwire read width from live session demand. |
| 12 | per-deployment AES-256-GCM keys for objects (above the disk cache) and log payloads; forks by reference into the parent's lineage on fresh logs: a fork's `~base` manifest names where each object it shares physically lives, and whatever erases a shared object (the reaper, the parent's deletion) copies it into the forks still referencing it first; a cell move materializes every reference; deletion durably drops the stream from addressed replicas, empties the inventory including S3 versions, destroys the local key and stores a signed receipt bound to the deployment’s namespaces; a fork can also be taken at a past block younger than the reaper's grace, referencing only what existed when that block was written. Local fork, reaper, move and disposal operations serialize their inventory mutations. Hosted disposal waits for storage admissions and retires every recorded replica journal. Cross-organisation forks copy the checkpoint and selected log tail with their own key. | a fork is of the current state, of a past block, or of a time at or after the checkpoint, whose tail up to it is copied into the fork's stream, rather than of any basis with its tail in a pack; a time before the checkpoint must be forked as a past block, and a tail holding an oversized transaction is refused; a forked tail's transactions take new ids in the fork's log epoch; a referenced object is copied out before its owner erases it rather than kept in place, and the reaper and forks use a grace interval, and enforcing concurrent reference publication requires the spec's lineage fence |
| 13 | persistent cell placement with DynamoDB and a local in-memory cell map, gateway forwarding, creation in the least occupied cell, restartable cell moves that carry the unflushed tail across; the old cell's reaper erases what a move left there once the grace has passed since the move. The destination has a durable object copy and sealed replica tail before cutover. Each residence retains independent storage and key custody. | The source is fenced during the copy. Provider-backed movement and cleanup still require staging qualification. The source reaper needs its key authority. |
| 14 | the chaos test and the history audit; `crates/simulation` runs one deployment's single-writer protocol from a seed (members claiming through the directory, three logkeeper state machines, a modelled writer, crashes, partitions, broken connections) and checks the epoch and log invariants after every step; it also runs a cell's storage lifecycle from a seed (deployments writing through their real store stacks, releasing, forking now and at a past block, reaper passes, deletions and cell moves, over in-memory buckets on a simulated clock, with injected pack read failures and move cleanup), checking that each deployment reads exactly what it wrote or inherited and has not released and nothing its parent wrote after the fork point, and gateway sessions (the real session over loopback TCP to stub backends that die during startup, before or after executing, between replies, or after preparing a named statement), checking that every command runs at most once, exactly once if the client was told it succeeded, and that the client sees only its own command's replies | three simulations of parts of a cell rather than one of a whole cell: the storage run has no nodes, directory or logkeepers and its admin sequences are modelled around the real calls, the session backends are stubs, with simple commands and extended-protocol prepared reads; combined directory, provider and runtime failures remain to be simulated |

A gateway started with `--config` uses the node YAML's `server.ssl` key store and requires PostgreSQL TLS from clients.
It terminates that frontend TLS connection before routing startup.
The node and cross-cell connections still use the cell's private network transport.

Hosting can install its existing deployment authority with `rtdb-multi install --id UUID --name NAME --org ORG`.
The directory allocates both name and authority conditionally, and an identical retry returns the same deployment.
This entry point enables linearizable reads on the holder.
A PostgreSQL startup database of `NAME@UUID`, with the UUID in lowercase 32-digit hexadecimal, requires that installation before connecting.
The gateway confirms the matched UUID in the startup parameter `rtdb_installation_id` after the backend checks its attached database.
A replacement backend must verify the same installation before the gateway replays session state.
Deploy nodes with the cell secret and this protocol support before routing gateways that require installation verification.
`SHOW rtdb_hosted_source_barrier` returns the deployment ID, applied transaction ID and ordered system time after the holder confirms the current directory assignment, confirms its epoch with the logkeeper quorum, and applies the sampled source cut.
The transaction ID and time are both null before any transaction is applied.
Hosted protocol 17 includes this barrier, the source owner read and both held-world valid window sources.
The holder must recover the checkpoint published in the directory or an authenticated successor before answering.
A gateway reconnect keeps the original deployment ID.
Restoring an older node or object copy requires a recoverable checkpoint and tail from the live directory and logkeeper ensemble.
The directory and acknowledged quorum stream must survive this restore, and the production logkeepers must acknowledge durable writes.
Recovery from a replacement directory or a rolled-back quorum requires a new deployment identity.

Nodes and logkeepers bound to a wildcard address require `--advertise-host` with their reachable DNS name or IP.
Each durable logkeeper stores its member ID in the journal directory.
A restart locks the original directory, waits for the previous lease to end, and reclaims the same member ID.
A named logkeeper keeps its advertised address for the lifetime of the journal.
To replace missing journal data, recover the journal or reconfigure the ensemble through its log authority.
Ensemble selection counts each advertised log endpoint once.

In AWS, the cell installs its directory tables independently of the runtime roles.
Use `--keys dynamodb:TABLE` for deployment keys in a dedicated table with string partition key `id`, separate from payload copies and without point-in-time recovery, streams or backup.
Deleting a key clears its material and keeps a tombstone that prevents recreating that identity on a later open.

For a local cell, initialize an existing private volume directory with
`rtdb-multi initialize-directory --state PATH`, then serve that same authority
with `rtdb-multi directory --state PATH --listen ADDR`. The process locks the
directory and durably records each change before exposing it. Keep its state
and the logkeeper journals outside any restored payload image. The directory
listener belongs on the trusted cell network.

The source directory records the fence epoch and destination before copying a cell transfer. Once the destination has its serving objects and key, it records the arrival and waits for routing to move and for the source to become a tombstone. Its first holder copies the sealed tail onto its logkeepers before accepting writes. Retrying a lost routing response continues that same transfer.

`rtdb-multi schedule-disposal --id UUID` fences an installation once in the
directory. A reaper configured with the cell's key service completes that
accepted request after the caller exits. `rtdb-multi delete --id UUID` can
also settle it synchronously. A holder records
its journal endpoints before it can copy or append a replica record.
Disposal retires the stream on each recorded journal and waits for its
obsolete segments to be replaced and removed. An unavailable journal keeps
disposal pending. The directory retains the receipt after that step, key
withdrawal and an empty serving prefix. `rtdb-multi disposal --id UUID` reads
the receipt. Gateway sessions close when they observe retirement. Object
versions, external backups and key copies in other cells have separate
physical custody.

A deployment fork stages its reference manifest and tail before publishing
the child. The directory checks the parent's eligible version in the same
transaction that creates the child and its name. Once retirement has fenced
the parent, a staged child cannot acquire a visible reference to its objects.
A publication with an uncertain outcome leaves the staged data available for
the directory to resolve.

On the private hosted lifecycle listener, every request includes its operation.
The optional disposal credential authorizes `schedule_disposal` and
`inspect_disposal` for a named root with an exact authority UUID. Scheduling
also fences an empty identity if retirement arrives before installation.
Inspection reads the directory without fencing. A settled response identifies
the retired writer epoch, identifies the provider inventory, and hashes the
receipt retained by the directory. On S3 the reaper accounts for object
versions, delete markers and unfinished uploads inside the deployment prefix
before reporting that inventory as empty.

Nodes register a serving storage region in the deployment record before
opening objects. Holders and compactors consult the current directory record
at the provider boundary before issuing a mutation. After an attachment
closes, its directory admission is removed only after its region's provider
work has drained, including work whose caller was cancelled. Disposal,
cross-cell transfer and same-history recovery wait for those admissions to
clear after fencing. On a standalone local disk, each provider operation
retains a lock in the independent key directory. After a node exits, the
reaper can recover its recorded region only by acquiring that same persisted lock.
Asynchronous local key opens use the blocking pool so a competing filesystem lock cannot stall the runtime thread.
A local serving node prepares one locked, durable storage-region witness before it is needed.
An admission consumes that witness once and starts replenishing the spare on the blocking pool.
When a spare is unavailable, the admission creates its own witness before publishing the region.
Normal shutdown removes an unused spare.
A process exit before shutdown can leave one unreferenced standby witness file.
A missing lock witness keeps the region pending. For remote
storage, a producer that exits without acknowledging drain keeps cleanup
pending until the provider's outstanding work can be accounted for.
Remote serving and checkpoint writers reserve an empty S3 multipart upload
and record its exact ID and object key in the directory before uploading
payload bytes. They complete the upload with `If-None-Match: *`. On
failure, the writer aborts the exact upload and confirms its ID is gone
before clearing the admission. The retained publication marker recovers
the success reply if S3 completed the object before a lost response.
A reservation interrupted before admission carries no payload, and the
bucket's incomplete-upload lifecycle rule collects it if it was created.

When the owner closes its I/O, a reaper can retire the region's remaining
upload IDs. On ECS, the region records the exact task ARN. An independent
ECS event delivery retains terminal STOPPED observations in the cell
directory. The observation authorizes the reaper to retire an open
region's upload IDs. Disposal continues only once the provider rejects
part inspection for every retained ID and the cell's object inventory can
be checked after cleanup.

`rtdb-multi capture --id UUID --operation UUID` retains the selected
published checkpoint and copies its catalog, table metadata and referenced
tries as ciphertext to the independent archive. The receipt fixes the
location, length and digest of each object. Retrying the operation recovers
the same checkpoint and destination. The directory registers both and the
capture's storage region before the first archive write. An archive binds
its media witness before payload can be copied. The original directory
accepts the verified manifest digest before the capture releases its region.
A hosted retry recovers that receipt from the original cell. The archive's
media key belongs to that capture and stays in the original cell's key vault.
It survives serving-key withdrawal. Application-level ciphertext is copied
as data and its keys are never part of the capture.

The cell's S3 root is `cell` under the configured prefix. The archive
namespace, S3 access and disposal inventory all derive from that root.
A remote capture registers the ECS task that owns its provider I/O.
Before a new hosted capture, the recovery owner asks the same residence's
holder to flush through its observed source position. It admits capture
after the directory publishes that block or a later cut in the same
residence. An interrupted capture retains its original directory cut.
`verify_checkpoint_capture` opens a fresh archive admission, reads the
witness and exact manifest, and verifies every object under the capture's
media key. The receipt binds the supplied observation ID to the original
capture point and digest.

On a parked installation, `capture_checkpoint` uses the already published cut.
The recovery listener accepts `retain_serving_replay` with the capture ID and `manifest_sha256` of that cut.
Upgrade the serving and recovery processes that share the directory before selecting a serving replay archive.
It verifies the original archive in independent custody and publishes its physical inventory receipt together with the selected capture.
The next holder and later capture operations can read the selected archive while replaying the original installation's ordered tail.
A parked installation can repeat the operation at a newer captured checkpoint.
Regions that already use an older source retain that archive until their provider work drains.
Direct forks of an installation with an attached archive require a separate materialization path.

`discard_checkpoint_capture` installs a permanent release for one capture
ID in its original cell. It can close an unattempted ID and prevents a late
capture from creating archive state. Physical retirement waits for the
recorded capture and repair readers, withdraws only the archive key, and
purges the capture's versions and unfinished uploads. The release receipt
and authority witness remain available. The serving installation keeps
its own key and placement.
A capture used by the current repair or selected for serving replay belongs to the serving closure until the installation replaces or releases it.

The recovery owner calls `rtdb-multi schedule-recovery-disposal --id UUID`
after the installation's serving receipt exists and recovery holds end.
The reaper settles each registered local archive after confirming its
media witness. `rtdb-multi recovery-disposal --id UUID` returns the ordered
retirement and observed absence. Remote archives stay pending until the
provider proves that versions and unfinished uploads are absent. Before
issuing its retirement receipt, the reaper withdraws every registered
capture's media key.

`rtdb-multi stage-checkpoint` materializes exactly one retained capture under
a fresh, held installation in the original cell. The authenticated reaper
endpoint also accepts `stage_checkpoint`, using the original capture's
authority and manifest and reserving `checkpoint-stage-<destination UUID>`.
It verifies the selected manifest, re-encrypts the objects with a fresh
stage media key, and retains the
ordered destination manifest and its digest. The staged record has no
placement, logkeeper ensemble or published checkpoint. The application
restore owner must admit its historical state before the target can serve.
Backing out uses ordinary exact-residence disposal.

`rtdb-multi read-checkpoint-stage` holds the exact staged inventory open on a
private loopback TLS reader. It authenticates the stage during PgWire startup,
exposes only its frozen block, and joins the reader's I/O before releasing its
storage admission. The reader has no disk cache or access to the source's
application key vault. A consuming restore owner must apply current
application custody and erasure authority before releasing protected data.
For hosted recovery, the reaper accepts `open_checkpoint_stage` on its
authenticated TLS connection. The request binds the original capture and
the exact stage receipt. After opening the verified inventory it writes
the acknowledgement and serves one read-only PgWire session on that same
connection. It closes the session on loss of the directory fence or when
the recovery action's lifetime expires.

The private `inspect_checkpoint_stage_owned_relations` action takes the same source, capture, and stage selectors as `open_checkpoint_stage`.
It requires a complete replay stage and returns the original transaction key, the captured owner namespace digest, the sorted physical relation names, and the canonical native schema declarations.
A stage without owned relations returns an empty inventory.
Multiple namespaces, unsupported native tables, and an owned namespace missing its declaration catalog refuse inspection.
The action returns metadata only and leaves the PgWire catalog unchanged.

The hosted `discard_checkpoint_stage` action binds the original capture
digest and stage operation. It reserves a disposal tombstone if a stage has
yet to appear, so a late stage cannot reopen it. It returns the retained
physical disposal receipt after the reaper has drained stage readers and
uploads, withdrawn the stage key and confirmed empty serving inventory.

`rtdb-multi restore` accepts the live directory's current checkpoint with
the original archive manifest and current key. It records a restore intent,
fences the old writer at the logkeepers, and verifies the objects. The
directory then admits the checkpoint archive as the source of its exact
manifest members. Reads verify their archived ciphertext and reseal each
under the current serving key. Objects outside that manifest come from
serving storage. A successor
recovers the same ordered tail before serving.
The directory keeps each accepted manifest and completion under its capture ID.
A retry observes that exact receipt after a later repair or cell move. It
does not open a new writer fence or access the archived objects.
Retain the archive while an installation uses it.

Hosted operators can give `rtdb-multi reaper` a private TLS recovery listener with its own credential, configured serving storage, independent archive, and current keys.
It admits an operation only for the exact name and authority in its configured organization.
A capture returns the pinned checkpoint and canonical archive digest.
A recovery request supplies that digest and capture ID, and repeats the completion receipt if the operation already finished.

**Fixed on the way.**

- A newly activated fork could publish its parent's boundary and trim its own log before its first flush completed.
- A failed object-store write leaves the release visible and retryable, and a failed pack read defers the erasure pass for that store.
- A failed startup tells the client that authentication ended, and the reconnect window covers the replacement holder's greeting and replay.
- The gateway restores the named statements acknowledged by the holder when idle failover follows an extended-protocol exchange.
- The gateway recovers a keyed command when its holder's TCP connection resets part way through dispatch.
- Publishing a fork checks the parent's lifecycle, writer epoch and storage identity, so deletion or a move can fence publication atomically.
- After a definitive fork rejection, the administrator attempts to remove the staged key, objects and tail independently.
  If cleanup fails, the orphaned artifacts still need removal.
- Deletion retains its epoch and key when storage still holds objects, so a later retry can finish the erasure.
- Local key destruction atomically replaces the key file with a durable marker that rejects future reads or installs for that deployment ID.

- A promotion that inherited a flushed block reopened its source tail behind the flush and stopped with "Source reader did not advance" (`ReplicationDriver::promote`); it affected classic failover too.
- A planned move whose target died afterwards left a move intent no node could ever satisfy.
- A recovery that died part-way through copying its history onto a server left that server unsynced, so the next recovery could prefer a staler server and lose committed entries (`StreamState::overwrite`, found by the simulation).
- An overwrite delivered after the writer's newer requests could truncate entries those had committed (`StreamState::overwrite`, found by the simulation).
- A live table written a row per transaction with long string ids failed at its 256th row with `rtdb/undersized-reservation` and stopped its database (`LiveTable::merge_small_chunks`); it affected classic mode too.
- Found reviewing the branch: a reconnection's capability named the user the client claimed rather than the principal its holder authenticated, and the control port served a deployment's rows, and published compactions, for anyone who could reach it.
- Found reviewing the branch: the control protocol authenticated nothing; in a cell with a secret every control request is now signed with it.

Standalone RTDB uses the same ordered journal and authorized PgWire gateway on one machine.
`initialize-standalone` creates the original directory, journal and key in a private empty directory supplied by the caller.
A successful initialization records the deployment UUID, journal ID and key digest.
`standalone` requires those original records and starts a loopback TLS gateway using a supplied certificate, private key and password file.
The journal accepts connections only inside the standalone process.
The gateway reports its bound port and installation UUID in the caller's private readiness file.
`rtdb-multi --build-info` reports the compiled source revision, time-zone data digest and standalone protocol.
Set `RTDB_REVISION` to the selected full source commit when building a distributable executable.
A supervisor can start the process with `--parent-stdin` so closing its private pipe requests shutdown.
The local directory, journal, keys and objects propagate namespace-flush failures to callers.
On Windows this uses a write-capable directory handle and `FlushFileBuffers`.
A missing file on a delete retry still requires a directory flush before confirming removal.

## 1. Ground rules

- **Classic mode stays.**
  A node started without the multi role behaves exactly as today: XTDB-compatible logs, storage layout and protocol, the conformance suite against the JVM reference, `./gradlew test`, `./gradlew property-test`.
  Multi mode is a new server role (`rtdb multi node|gateway|logkeeper|controller|reaper|worker`) built from the same engine crates.
  Where multi needs a different on-disk or on-log form (epoch-named keys, packs, manifests, the resolved-only log), it is a new storage version or a new loglet kind selected by the role, never a change to the classic form.
- **Tidy first.**
  Every stage starts with equivalence changes that give the multi path a seam to plug into, as separate `tidy:`/`refactor:` commits that could be cherry-picked to `main` on their own.
- **Each commit carries its tests and its spec delta.**
  A commit that changes behaviour covered by an Allium spec updates that spec in the same commit.
  `allium/rtdb-multi.allium` is updated whenever implementation teaches us the spec was wrong.
- **Illegal states unrepresentable.**
  The spec's sealed state machines (`Holding.state`, `Residency`, `Lifecycle`, `MemberState`) become Rust enums carrying exactly their valid data, driven by exhaustive `match`es.
  `StorageRoot` is the only constructor of object keys in multi mode.
- **Safety first, then cost, then elasticity.**
  The stages are ordered so that isolation and the single-writer fence are correct before anything is made cheap, and cheap before anything is made elastic.

## 2. Where we start from

What RTDB already provides, and what the endgame keeps from it:

| Today | Where | Kept as |
| --- | --- | --- |
| Many databases per node, each with its own logs, storage, live index, election, compactor and GC | `node/src/database.rs` (`Database`), `node/src/catalog.rs` (`DatabaseCatalog`) | A deployment is one `Database`; the per-database machinery is the unit a `Holding` drives |
| Runtime attach/detach through the primary database's source log | `node/src/node.rs` (`attach_database`, `detach_database`), `node/src/catalog.rs` (`CatalogEffects::apply_db_op`) | Replaced in multi mode by directory-driven placement; kept in classic mode |
| Two `Membership` kinds, `Durable` and `Fixed` | `node/src/catalog.rs` (`Membership`) | A third kind, `Placed`, follows the directory |
| Per-database leader election by term claims in the replica log, fence on read-back | `replication/src/driver.rs` (`Role`, `start_claim`, `promote`, `demote`, `consume_record`), `replication/src/admission.rs` (`TermFence`), `replication/src/appender.rs` (idle no-op heartbeat) | Replaced in multi mode by directory epochs and loglet seals; `TermFence` stays in classic mode |
| Resolution, staging, apply | `tx/src/resolver.rs` (`TxResolver`), `index/src/snapshot.rs` (`Snapshot::with_staged`), `index/src/partition.rs` (`PartitionState`) | Kept unchanged: the holder resolves and applies exactly as a leader does today |
| Follower replay of resolved transactions, block adoption at `BlockUploaded` | `replication/src/driver.rs` (follower path), `replication/src/state.rs` | Kept as the `Subscription` apply path and the holder's tail replay |
| The `Log` trait: enqueue, latest, tail, range | `storage/src/log/mod.rs` | Extended by a `Loglet` trait with conditional append and seal; `Log` stays for classic mode |
| Storage root `v06[_eNN]`, keys under `blocks/`, `tables/` | `format/src/keys.rs` (`storage_root`) | Classic layout kept; multi layout adds the `StorageRoot` prefix and `e{epoch}` segment |
| Recovery by listing `blocks/` | `replication/src/blocks.rs` (`BlockStore::recover`) | Replaced in multi mode by the directory checkpoint and the manifest |
| Per-database `BufferPool` on a shared `CacheBudget`, each pool evicting only its own entries | `storage/src/cache.rs` | Replaced by one node-wide fair cache |
| One `DiskCache` per node, namespaced by database name | `storage/src/disk_cache.rs` (`scope_in`), `node/src/resources.rs` (`cache_namespace`) | Rekeyed by storage identity |
| Every query snapshots every open database | `node/src/node.rs` (`query_context`) | Narrowed to the databases a query names |
| `authorize_database` always `Ok` | `auth/src/lib.rs` (`Principal`) | Real per-database authorization; one deployment per session |
| One pgwire listener choosing the database from the startup parameter | `pgwire/src/connection.rs` | Protocol layer reused by the gateway; execution moves behind an RPC |
| Compaction on every non-read-only node; trie and block GC on the leader | `node/src/database.rs` (`MaintenanceSupervisor::start_term`), `maintenance/src/` | Compaction becomes a worker pool; deletion moves to the reaper |
| `flush_timeout` 4 h, `rows_per_block` 100k | `replication/src/driver.rs` (`ReplicationConfig::default`) | Replaced in multi mode by size, replay-budget, age, idle and degradation triggers |
| Kafka log, one producer and consumer per log | `kafka/src/lib.rs` (`KafkaLog`), `kafka/src/driver.rs` | Kept for classic mode and for external sources; not a multi-mode log |
| Health and driver status per database | `node/src/health.rs`, `replication/src/driver.rs` (`DriverStatus`) | Feeds the controller's load vectors |

## 3. Concept map: spec to code

| Spec concept | Today's nearest code | What changes |
| --- | --- | --- |
| `Cell`, `CellMap` | none | New: cell wiring in `multi`, cell map in `directory` |
| `Directory` | `BlockCatalog::secondary_databases` (`format/src/catalog.rs`) as the only durable database list | New crate `directory`; the primary database stops being the membership authority in multi mode |
| `MemberRecord`, leases, suspicion | `NodeOptions.node_id` (`node/src/config.rs`), health endpoints | New: member registration, leases, gateway suspicion reports, controller confirmation |
| `DeploymentRecord` | `DatabaseConfig` (`core/src/main/proto/xtdb/database/proto/db.proto`) | New proto; `DatabaseConfig` becomes derived from the record (log = loglet chain, storage = `StorageRoot`) |
| `Epoch` | `Term` in `replication/src/admission.rs` | Epoch is allocated by directory CAS rather than read from the log |
| `Holding` and its states | `Role { Following, Leading(Term), Stopped }` in `replication/src/driver.rs` | New `HoldingDriver` alongside `ReplicationDriver`, sharing apply and block adoption |
| `Loglet`, `LogService` | `Log` trait in `storage/src/log/mod.rs`; `PartitionLogs` in `replication/src/logs.rs` | New `Loglet` trait; `VirtualLog` over a loglet chain; new crate `logkeeper` |
| `ResolvedTx` with idempotency key | `ResolvedTx` proto and `ReplicaRecord` (`format/src/log`) | New fields: idempotency key, intake position; `EpochStart`, `Handoff`, `Parked` records |
| Intake stream | the source log (`SourceBody`) | The source log's role survives only as an optional per-deployment intake loglet |
| `StorageRoot`, epoch-named keys | `storage_root`, `object_prefix` (`s3/src/lib.rs`, `gcs/src/lib.rs`, `azure/src/lib.rs`) | New multi storage version; `StorageRoot` is the only key constructor |
| `StorageCredential` | static credentials in S3/GCS/Azure configs | Credential service minting per-(deployment, epoch) put-if-absent credentials |
| `Checkpoint`, manifest, boot object | block catalog files under `blocks/` | Manifest object listing every live object; checkpoint pointer in the directory |
| Packs | per-table data/meta files | New pack format in `format`, range-readable by the trie readers |
| `CompactionJob`, adoption by `TriesAdded` | `maintenance/src/driver.rs`, `maintenance/src/job.rs`, `maintenance/src/publication.rs` | Jobs dispatched to workers by affinity; outputs adopted by the holder |
| Reaper, `GarbageCandidate` | `maintenance/src/gc.rs`, block GC | Nodes stop deleting; GC emits `TriesReleased`; the reaper deletes |
| `Residency` | none; an open database is fully resident | New resident-idle and parked states |
| `Subscription` | follower path of `ReplicationDriver` | Reused for log-fed read replicas created on demand |
| Snapshot read tier | `QueryBasis` and snapshot caps in `node/src/node.rs` | Any member serves reads at a basis at or below the checkpoint |
| `Session`, `Capability`, gateway | `node/src/session.rs`, `pgwire/src/connection.rs`, `flight-sql` | Session state moves to the gateway; nodes expose a statement RPC |
| Affinity | none in RTDB; Bijection's `crates/affinity` | New crate `affinity`, ported with its shared test vectors |
| Fair cache, quotas | `storage/src/cache.rs`, `types/src/memory.rs` (`MemoryBudget`) | Node-wide fair eviction, per-deployment floors, tail budget, token buckets |
| Quarantine | none | Activation crash counting in the controller |
| Lineage, fork, deletion | storage epoch (`StorageEpoch`) for restores | New lineage records, fork checkpoints, deletion proofs |

## 4. New crates

| Crate | Role in the spec | Depends on |
| --- | --- | --- |
| `affinity` | `Rank`, `Select`, `PrewarmOnJoin`; pure policy, ported from Bijection with `fixtures/vectors.json` | none |
| `directory` | `Directory` contract: `DeploymentRecord`, `MemberRecord`, `Lineage`, CAS, watches; an in-memory backend and a DynamoDB backend | `format`, the AWS SDK |
| `loglet` | `Loglet` trait, `VirtualLog`, loglet kinds (memory, local file, logkeeper client) | `storage`, `directory` |
| `logkeeper` | The log server: segments per writer, per-stream index, promise, group commit, seal announcements, trim | `loglet` |
| `holding` | `HoldingDriver`: claim, seal, recover, open, serve, flush, park, hand off | `replication`, `loglet`, `directory`, `tx`, `index` |
| `gateway` | Session termination for pgwire and Flight SQL, capabilities, idempotency keys, routing, redirects | `pgwire`, `flight-sql`, `affinity`, `directory` |
| `controller` | Suspicion confirmation, admission, drain, rebalance, drainer, quarantine, autoscaling signals, engine rollout | `directory`, `affinity`, `metrics` |
| `reaper` | Reachability sweep, scrubber, deletion proofs | `directory`, `format`, `s3`/`gcs`/`azure` |
| `credentials` | Per-(deployment, epoch) storage credentials, per-deployment data keys | `s3`/`gcs`/`azure`, KMS clients |
| `multi` | Cell wiring: role selection, configuration, the `rtdb multi` subcommands in `server` | all of the above |
| `simulation` | Deterministic simulation of a cell: directory, logkeepers, nodes, gateways, object store, clocks, faults | all of the above |

## 5. Stages

Each stage lists why it exists, the code it touches, what it adds, how it is tested, which specs change, and when it is done.
Stages 0 and 1 are useful to classic mode on their own and should be cherry-picked to `main` as they land.

### Stage 0: measure, and tidy the seams

**Why.**
The spec's residency design rests on the cost of an idle database, which nobody has measured (open question 1).
And several seams the later stages need are equivalence changes that can land now.

**Changes.**

1. A bench in `rust/bench` that opens N databases on one node against a local object store and reports, per database: resident memory by owner (`PartitionState`, catalogs, `BufferPool`, trie metadata), tasks, timers, log clients; and the time to open from a cold cache.
   This produces the number `resident_idle_target_bytes` must be checked against.
2. `tidy(format)`: introduce `StorageRoot` in `format/src/keys.rs` and route `storage_root` and every provider's `object_prefix` through it, with no change to the keys produced.
3. `perf(node)`: `Node::query_context` in `node/src/node.rs` opens snapshots only of the databases the plan references, rather than every open database.
4. `fix(storage)`: key the disk cache namespace by storage identity (provider, bucket, prefix, storage epoch) instead of database name, in `node/src/resources.rs` (`cache_namespace`) and `storage/src/disk_cache.rs` (`scope_in`), matching the JVM's `CachePrefix`.
   Today a re-attached name with a different bucket, prefix or epoch can be served stale cached objects.

**Tests.**
Unit tests for `StorageRoot` key equality with the old function; a node test that a query against one database does not open the others; a disk-cache test that two storage identities under one name never share entries.

**Specs.**
None change.

**Done when** the bench has run on a realistic schema and its numbers are recorded in this document.

### Stage 1: isolation in a shared process

**Why.**
Isolation is the precondition for putting two customers in one process, and it is valuable to classic mode too.

**Changes.**

1. **Root marker.**
   `open_storage` (`node/src/resources.rs`) writes, with put-if-absent, a marker object at the storage root naming the database's identity, and refuses to open a root whose marker names another.
2. **Authorization.**
   `Principal::authorize_database` (`auth/src/lib.rs`) checks grants instead of returning `Ok`; `query_context` already filters by it.
   A session may be restricted to exactly one database, which pgwire and Flight SQL enforce at connection time (`pgwire/src/connection.rs`).
   This is the spec's `OneDatabasePerSession` invariant in classic form.
3. **Fair memory cache.**
   `storage/src/cache.rs`: replace per-pool eviction with node-wide eviction across every `BufferPool` on a `CacheBudget`, with a per-pool reserved floor and eviction from the pool furthest over its weighted share (spec rule `EvictFairly`).
4. **Tail memory budget.**
   A node-wide budget for live indexes, which asks the largest database to flush first under pressure (spec rule `BoundTailMemory`), wired through `ReplicationDriver`'s flush request.

**Tests.**
Node tests: two databases sharing a prefix refuse to open; a principal without a grant cannot query, and cannot see another database's tables by qualified name; one database's idle cache entries cannot make another's reservation fail; a node over its tail budget flushes its largest database.

**Specs.**
`database-lifecycle.allium` gains the root marker; `live-index.allium` gains the tail budget.

**Done when** the classic suite and property tests pass, and the isolation tests above pass.

### Stage 2: the directory, membership and placed databases

**Why.**
Every later stage needs the directory as its authority.
This stage introduces it while keeping the existing logs and election, so placement can be tested on real databases before the log changes.

**Changes.**

1. **Crate `directory`.**
   The `Directory` trait (read, CAS conditional on epoch, watch), protos for `DeploymentRecord`, `MemberRecord` and `Lineage` in `format`, an in-memory backend for tests and simulation, and the DynamoDB backend for production (decision 1, settled).
   The DynamoDB backend:
   - one table per record kind (deployments, members, lineages), keyed by id, in on-demand capacity mode, in the cell's region only (global tables are not used: multi-region replication would break the linearizable CAS);
   - every epoch-bound change is an `UpdateItem` with a `ConditionExpression` on the expected epoch, and every read that a claim or a fence depends on is strongly consistent;
   - a fork, which writes the child and bumps the lineage version together, is one `TransactWriteItems`;
     The DynamoDB fork principal needs `dynamodb:ConditionCheckItem` on the deployments table for the parent version check.
     [DynamoDB transaction permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html)
   - a secondary index on `holder` answers "every deployment held by member X" for the drainer; it is eventually consistent, so the drainer re-reads each record strongly before it claims;
   - watches are DynamoDB Streams (new images), fanned out to gateways and nodes by a small watch service; a stream is ordered per item, consumers drop any image whose epoch is below the one they hold, and a late image costs a redirect, never a wrong answer;
   - member leases are conditional `UpdateItem`s every `member_heartbeat`, about 1,000 writes a second for a 500-member cell;
   - the contract suite runs against the in-memory backend and against DynamoDB Local.
2. **Members.**
   Each process registers a `MemberRecord` with a fresh `MemberId`, renews a lease every `member_heartbeat`, and is marked dead on expiry (spec rules `RenewLease`, `ExpireLease`).
3. **`Membership::Placed`.**
   A third variant in `node/src/catalog.rs` whose set is the deployments the directory names this member as holder or subscriber of.
   The node opens and closes databases through the existing `DatabaseFactory::open` and detach path (`node/src/node.rs`, `NodeDriver::attach`), driven by the directory watch rather than by `CatalogEffects::apply_db_op`.
   In multi mode the primary database carries no catalog of other databases.
4. **Placement-driven leadership on today's logs.**
   `ReplicationConfig.may_lead` (`replication/src/driver.rs`) becomes a per-database value derived from the directory: a node may claim only a database the directory has placed on it.
   The claim's term is the directory epoch instead of `highest_term_read + 1`, so the existing read-back fence (`TermFence`) still adjudicates, but only the placed node ever claims.
   The randomized claim timer is removed in multi mode.
5. **`DeploymentRecord` to `DatabaseConfig`.**
   A function deriving today's `DatabaseConfig` from a record, so `DatabaseFactory::open` is unchanged.

**Tests.**
Directory backend tests against the `Directory` contract invariants (`EveryEpochHasOneClaimant`, `EpochsOnlyRise`, `EpochBoundChangesAreConditional`).
Multi-node tests on the in-memory log: a database placed on one node is opened only there; moving the placement moves leadership with no election; a dead member's databases are reclaimed on demand.

**Specs.**
`database-lifecycle.allium` gains the placed membership source.
`log-processor-lifecycle.allium` gains the rule that, in multi mode, a claim requires placement and carries the directory epoch.

**Done when** a three-node test cell places, moves and fails over 1,000 databases on the in-memory log with no two confirmed leaders.

### Stage 3: one resolved log per deployment

**Why.**
Two logs per deployment and an append per second while idle are the dominant fixed costs (spec contract `LogService`).
This stage changes the log's shape; stage 4 changes where it lives.

**Changes.**

1. **Crate `loglet`.**
   A `Loglet` trait: conditional `append(epoch, lsn, records)`, `seal(epoch) -> SealReply`, `read`, `subscribe`, `trim`.
   A `VirtualLog` presenting a loglet chain from the directory as one dense log.
   Loglet kinds: memory and local file, both enforcing the promise at append time.
2. **Crate `holding`.**
   `HoldingDriver` implements the spec's `Holding` state machine: claim, seal, recover (greatest by epoch then lsn, chain verified), open (`EpochStart`), serve, depose.
   It reuses `TxResolver` (`tx/src/resolver.rs`) and the apply and block-adoption code from `ReplicationDriver` unchanged; the shared parts are first extracted from `replication/src/driver.rs` and `replication/src/state.rs` by `refactor(replication)` commits that leave classic behaviour identical.
3. **Routed submission.**
   `Node::submit` (`node/src/node.rs`) in multi mode sends a transaction to the holder (in process when the holder is local, by RPC otherwise) instead of appending to a source log.
   The resolved-transaction proto gains the idempotency key, and the holder keeps the `recent_keys` window (spec rules `AnswerRetryFromWindow`, `ResolveAndAppend`, `ApplyOnCommit`).
   When the duplicate guard aborts a submission, the holder reads its key from that deployment's durable transaction metadata and finds the committed result as of the submission's system time.
   Repeated outcome lookups return the original result after its key expires and is reused.
4. **System time across epochs.**
   The holder assigns `max(now, last_system_time + 1µs)`, seeded from the checkpoint and tail, and refuses to open on excessive skew (spec rule `RefuseToLeadWithSkewedClock`).
5. **No idle heartbeat.**
   The appender's no-op timer (`replication/src/appender.rs`) is not started in multi mode; liveness is the member lease.
6. **Optional intake.**
   An intake loglet per deployment for producers that do not wait, consumed by the holder (spec rules `AppendToIntake`, `ConsumeIntake`).
   External sources (`pg-source`, Kafka Connect) feed the holder directly.

**Tests.**
Loglet contract tests run against every loglet kind: refused stale-epoch appends, seal quorum intersection, recovery keeps every committed record.
Holding tests with a zombie holder that ignores every fence.
Idempotency tests: a retry across a failover returns the first outcome.
Property tests (`./gradlew property-test`) for the classic path, since the extraction touches indexing.

**Specs.**
`db.allium`: the source log is classic-only; the multi log is resolved-only.
`log-processor-lifecycle.allium`: classic only; multi leadership is `rtdb-multi.allium`'s.

**Done when** the stage 2 test cell runs on the resolved-only log with idempotent retries, and the classic suites are unchanged.

### Stage 4: the log server

**Why.**
The loglet kinds of stage 3 are single-process.
A cell needs a quorum log shared by every deployment, with group commit across deployments and per-deployment reads.

**Changes.**

1. **Crate `logkeeper`.**
   One append-only segment file per writer member, a per-deployment index mapping `(deployment, lsn)` to segment offsets, a per-deployment `LogServerStream` (promised epoch, last epoch and lsn, trim point), fsync before acknowledgement, group commit within `group_commit_window`, seal announcements in every round's acknowledgement, trim and segment reclamation.
   The logkeeper maintains retained byte counts and epoch ranges while applying journal records.
   A seal reads the epoch ranges without scanning each retained entry, and compaction eligibility reads the byte count of each stream.
   A logkeeper charges the original frame size and reserves queue capacity before changing the stream state.
   Capacity is released when the journal work finishes.
   A full queue waits for a durable round before accepting more work.
2. **Logkeeper loglet.**
   A client in `loglet` holding one connection per ensemble member per node, multiplexing every deployment the node holds, with write quorum 2 of 3 across zones.
   During recovery, the final overwrite acknowledgements also report the durable commit positions.
   An extra commit round is needed only when those acknowledgements leave fewer than a write quorum at the recovered commit position.
   A wire-level test drops the old writer's commit markers and checks that the successor confirms the recovered entry on a quorum before it is readable.
   A serving log uses the existing deployment key after the storage open has made it durable.
   A missing key stops the log open before it can reserve journal endpoints or change a quorum promise.
3. **Ensemble assignment.**
   The directory records each loglet's ensemble; the controller balances writers across logkeepers.
   Assignment spreads an ensemble across distinct declared zones where possible and retains surviving members during repair.
4. **Deployment of the logkeepers.**
   The logkeeper is the first production loglet (decision 2, settled).
   Logkeepers run on EC2 instances with instance-store NVMe, at least one per availability zone per ensemble, with ensembles spread so that losing a host re-replicates its streams to the rest of the cell.
   The `Loglet` trait still admits other kinds (a multiplexed Kafka loglet, an object-store WAL for a cheaper tier); none is built on this branch.

**Tests.**
Logkeeper unit tests; the loglet contract suite from stage 3 run against a three-logkeeper ensemble; fault injection (a killed server, a partitioned server, a slow fsync, a torn segment) in the simulation crate; throughput and latency benchmarks against the spec's estimates (2–4 ms p50 commit, tens of thousands of appends per second per server).

**Local performance evidence.**
A release build ran three trials per concurrency on an Apple M4 Max with three loopback TCP logkeepers, three journals on the same SSD, fsync enabled, and a two-member write quorum.
Each stream made 1,000 measured durable appends of 512 bytes with one append in flight per stream.
The measurement covers enqueue through durable receipt after writer setup and warmup.

| Concurrent streams | Quorum commits/s | Median commit latency (ms) | p99 commit latency (ms) |
| ---: | ---: | ---: | ---: |
| 1 | 4,651–6,924 | 0.139–0.146 | 0.248–0.285 |
| 64 | 37,990–51,919 | 1.021–1.045 | 4.880–11.020 |
| 256 | 50,507–60,314 | 3.835–4.089 | 10.459–14.254 |

After adding bounded admission and incremental retained-epoch summaries, one release repeat measured 8,720, 40,660, and 41,178 commits/s at 1, 64, and 256 streams respectively.
Median commit latencies were 0.111, 1.056, and 4.187 ms, and p99 latencies were 0.201, 1.680, and 21.346 ms.

```sh
RTDB_LOG_BENCH_STREAMS=64 RTDB_LOG_BENCH_APPENDS=1000 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-logkeeper --release \
  --test throughput -- --ignored --nocapture
```

The cross-availability-zone performance gate remains to be measured with one server per zone.
After adding durable stream drops, an interleaved rerun compared three trials of the earlier and current binaries at each concurrency on the same host.
The 64-stream median commit latency stayed between 1.007 and 1.030 ms across both builds, but current throughput varied from 8,587 to 40,412 commits/s against 39,622 to 53,284 earlier.
At 256 streams the current range was 48,842 to 61,942 commits/s against 48,271 to 65,516 earlier.
The source of the intermittent long tail and lower aggregate throughput on the 64-stream runs remains unverified.

**Specs.**
`rtdb-multi.allium`'s `LogService` contract, corrected by what the implementation teaches.

**Done when** the contract suite passes against a real ensemble under faults, and the benchmark meets the estimates or the spec's numbers are revised.

### Stage 5: the storage fence and manifests

#### Enrollment for terminal S3 storage

The terminal profile uses `s3://BUCKET?terminal=ALLOCATION.PUBLIC_KEY`, where `ALLOCATION` is a fresh UUID and `PUBLIC_KEY` is an Ed25519 public key encoded in unpadded base64url.
Provision the matching bucket-level `cell-terminal-<simple-uuid>` prefix and its create-once policy before granting serving credentials.
Bucket-level prefixes of that form are reserved for terminal allocations.
Keep serving credentials fenced until enrollment completes and preserve the policy and bootstrap records for the allocation's lifetime.
Serving principals create new objects, and the qualified retirement principal alone can conditionally replace a retired payload with its permanent tombstone.
Allocation disposal must drain provider writes before removing terminal objects.

Run `rtdb-multi enroll-terminal-storage --storage-root URL --qualification FILE` with provisioning credentials authorized to read the provider inventory and conditionally write the two bootstrap records.
The qualification file is a JSON envelope with `payload` and `signature` strings encoded in unpadded base64url.
The raw payload is a JSON object with `policy` set to `create_once_v1`, the `allocation` UUID, the provider-encoded physical `root` without a trailing slash, and `namespace` containing the exact `bucket`, `region`, and `endpoint` (`null` for the default endpoint).
The Ed25519 signature covers the bytes `rtdb.s3/terminal-enrollment/v1`, one zero byte, and the raw payload in order.
The signer attests that the named allocation has been protected since creation and is reserved for the same provider account and policies throughout its lifetime.
The command prints the enrolled allocation UUID.
The same command can settle an interrupted bootstrap, and the enrolled URL is the storage-root configuration for readers, scoped writers and the reaper.
The bucket policy and provider roles must be qualified independently of the command's inventory checks.

See [the recovery-root design](../../verification/redesign/R5-recovery-roots.md#never-versioned-serving-buckets) for the retirement and disposal requirements.

**Why.**
A deposed holder must be unable to damage storage, and recovery must not depend on listing (spec contract `ObjectStorage`).

**Changes.**

1. **Multi storage version.**
   `format/src/keys.rs` gains a multi layout: `StorageRoot.prefix` + `/e{epoch}/` + a short name hash + name.
   Classic `v06` keys are untouched.
2. **Manifest.**
   The block catalog is extended into a manifest listing every live object with key, size and checksum.
   `BlockStore::recover` (`replication/src/blocks.rs`) in multi mode reads the directory checkpoint and its manifest instead of listing `blocks/`.
   `block_lag` (`node/src/database.rs`) reads the checkpoint.
3. **Credentials.**
   Crate `credentials` mints per-(deployment, epoch) put-if-absent credentials scoped to the epoch prefix with a lifetime of at most `credential_ttl` (S3 STS session policies, GCS downscoped tokens, Azure user-delegation SAS).
   The provider crates (`s3`, `gcs`, `azure`) accept a refreshing credential source per store.
   On S3, `rtdb-multi node --s3-write-role ARN` opens one STS write session per active deployment store as needed.
   Run `rtdb-multi init-storage --storage-root ROOT --keys PATH` with the storage administrator's credentials before starting scoped nodes on a new cell.
   A scoped node's ambient identity reads the cell's objects, including inherited objects, and assumes the configured write role.
   Its deployment store uses the assumed identity for mutations.
   The node trusts its ambient STS identity to request the configured role with the generated session policy.
   The role session requests a 15-minute lifetime and permits writes under the deployment's existing storage prefix and version.
   The node rejects returned expirations beyond that lifetime plus one second of clock allowance.
   The node verifies the current writer or live compaction member against the directory before and after refreshing a session.
   Before the node uses a session, a conditional directory write retains the latest issued expiration for that deployment prefix plus the clock allowance.
   Upgrade every directory writer before relying on the recorded expiration, including writers of packed parked records.
   The session policy denies deletes and out-of-prefix writes, and requires conditional PUTs by default. [Enforce S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html)
   For providers such as MinIO that reject the S3 condition key, `--s3-compat-write-policy` keeps the prefix and delete restrictions, and the client sends the conditional header.
   Use `--sts-endpoint URL` when the STS service is at a custom endpoint.
   Storage paths that carry a holder epoch, a separate credential issuer and production AWS validation remain open.
   Multi checks the deployment's current lifecycle and prefix before an attach claims the store root.
   Readers verify an existing root marker and only a holder can create it.
   A writer confirms its epoch from the directory while its watch has no observation at or beyond that epoch.
   A compaction worker checks the current live deployment and prefix before writing each output.
4. **No deletes from nodes.**
   Trie GC and block GC (`maintenance/src/gc.rs`, started per leader term by `MaintenanceSupervisor::start_term` in `node/src/database.rs`) emit `TriesReleased` records instead of deleting.
5. **Crate `reaper`.**
   The reachability sweep per lineage (spec rule `ReapUnreachable`), conditional on the lineage version, and the scrubber (`ScrubOrphans`).

**Tests.**
A zombie holder that keeps writing after deposition produces only orphans the scrubber finds; a node credential cannot delete; recovery from a manifest with object listing disabled at the store; the reaper never deletes an object a live manifest references, including across a concurrent fork.

**Specs.**
`gc.allium` and `block-gc.allium`: classic-only for deletion; multi deletion is the reaper's.

**Done when** the simulation's zombie and partition scenarios show no reachable object damaged.

### Stage 6: packs, boot objects and the flush policy

**Why.**
Small deployments must cost one PUT per flush and one GET per cold start (spec rules `WritePackAndManifest`, `PublishCheckpoint`).

**Changes.**

1. **Pack format** in `format`: every table's trie data and metadata and the catalog delta in one object with a footer index; the trie readers in `trie` and `index` read by range.
2. **Boot object**: catalogs, schema and top trie levels inlined up to `boot_inline_limit`, so opening a deployment is one GET.
   A checkpointed root or materialized deployment skips the base-manifest listing and opens its packs by name.
   A fork with inherited objects still reads its base manifest.
3. **Flush triggers** in `HoldingDriver`: tail bytes, estimated replay, oldest-record age, idle, ensemble degradation, retention, rows (spec rules `FlushOn*`), replacing `flush_timeout` and `rows_per_block` for multi mode.
   A placed node honors shorter configured flush intervals and caps the interval at 15 minutes.
   After 60 seconds without use it flushes unflushed rows while keeping the holder and epoch.
   Each pass starts at most two idle flushes.
4. **Checkpoint publication**: a CAS conditional on the epoch, batched per node, then log trim.

**Tests.**
Golden pack round-trips; cold-start time from S3 measured by the stage 0 bench; flush trigger tests; trim never passes the checkpoint (`CheckpointNeverAheadOfLog`).

**Specs.**
`db.allium` block cycle: classic-only; the multi flush cycle is `rtdb-multi.allium`'s.

**Done when** a small deployment's flush is one pack PUT plus one CAS, and its cold start is one directory read plus one GET.

### Stage 7: compaction as a job pool

**Why.**
Compaction on every node holding a database duplicates work and costs even when idle (spec rules `RunCompactionJob`, `AdoptCompactionOutputs`, `MaintenanceClaim`).

**Changes.**

1. The holder computes jobs at flush with the existing deterministic planner (`maintenance/src/job.rs`) and enqueues them.
2. A worker role runs jobs from the queue, selected by affinity on `(deployment, job key)`, writes outputs with put-if-absent under deterministic names, and offers them to the holder.
3. The holder adopts outputs with `TriesAdded` and releases inputs with `TriesReleased` (`maintenance/src/publication.rs` becomes the adoption path).
4. A parked deployment with compaction debt gets a maintenance claim.
5. `MaintenanceSupervisor` (`node/src/database.rs`) starts no compactor in multi mode.
   A placed node that runs compaction shares its configured thread count as an execution limit across every deployment.
   A cancelled merge holds its slot through the completion of its object writes.

**Tests.**
Duplicated job execution is harmless; outputs of a stale job are abandoned and reaped; compaction results equal the classic compactor's on the same inputs (reusing `compaction.allium`'s merge tests).

**Specs.**
`compaction.allium`: where jobs run, classic and multi.

**Done when** a cell's compaction work is independent of the number of members holding each deployment.

### Stage 8: residency

**Why.**
A parked deployment must cost nothing but its record and objects (`CostFollowsActivity`).

**Changes.**

1. `Holding.residency` with `active`, `resident_idle` and `parked`.
   Set `--resident-idle-after SECONDS` to keep the placement after its writer has been idle for that interval.
   The node drains admitted writes and publishes an adopted checkpoint while advancing the writer epoch.
   The directory keeps the placement and last activity time, and the node closes its database, logs and placement watch.
   A request wakes the deployment by claiming the next epoch and opening its checkpoint on the same eligible member.
   A later park deadline, drain or planned move can release the idle placement directly.
   The node checks due park deadlines in small batches using the IDs and epochs recorded at suspension.
   The node skips a candidate from an earlier sweep once the deployment has awakened under another epoch.
   An open Flight SQL call counts as activity through response streaming or cancellation.
   The status command counts these placements separately as `resident_idle` and excludes them from the recommended active-node count.
   Upgrade placement writers before enabling this option.
   Ordinary parking releases the holder in the checkpoint publication CAS.
   Ordinary parking waits for admitted user transactions and pauses new submissions before flushing.
   It publishes the resulting checkpoint in the directory update that releases the holder.
   An explicit flush publishes its adopted checkpoint before it returns, while retaining the holder.
2. LRU parking under memory pressure after `min_residency` (`ParkUnderPressure`).
   The placed node starts parking its least recently touched holdings at 80 percent of the execution-memory limit and targets 75 percent.
   A new activation rechecks headroom after reclamation before claiming.
   It skips busy or recently touched candidates if their local operation state changed during selection.
   Reclamation takes up to eight candidates per pass and bounds the wait on another reclamation pass or a candidate's flush to 500 ms.
   It can park a holding with an open session when older holdings do not release enough memory.
   Gateways try the other eligible nodes and retry temporary pressure within an activation deadline.
   A planned handoff retains its reservation while retrying its target under pressure.
   Set `--min-residency SECONDS` to keep newly opened holdings out of memory-pressure reclamation for at least that long.
   The default is zero seconds.
   Explicit parks and drains can still release younger holdings.
3. Activation on demand from the gateway or the intake (`ActivateOnDemand`), admitted per node at `activations_per_node_per_second`.
   A placed node admits up to 200 cold claims in a rolling second by default, configurable with `--activations-per-second N`.
   The gateway tries other eligible nodes and retries a rate refusal within its activation deadline.
   Existing holdings and reserved handoffs can continue while the cold-activation budget is full.
4. Everything per database that today lives for the database's lifetime (tasks, timers, log tails, pools) is made lazy, guided by the stage 0 measurements.

**Tests.**
A node holding 100,000 resident-idle deployments stays within its memory target; a parked deployment has no task, timer, log-server state or subscription; reactivation latency from warm and cold caches.

**Specs.**
`live-index.allium`: the tail index is released on idleness.

**Done when** the resident-idle footprint meets the target measured in stage 0, or the target and the parking policy are revised.

**Local resident-idle measurement**

An in-process cell with one serving node, three fsync logkeepers and a memory directory stored 100,000 already-idle placements for the node.
The placements were provisioned in the directory without opening each database.
The process RSS increased by 168,378,368 bytes, including the directory, or 1,683 bytes per placement against the 65,536-byte target.
The node execution budget and task count stayed flat.
The benchmark then woke 32 deployments through the node agent under fresh epochs and measured 40.56 ms median and 105.30 ms maximum.
The cell tests separately verify that an acknowledged write survives suspension, detachment, reactivation and movement.

```sh
cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --test footprint \
  a_node_can_retain_one_hundred_thousand_idle_placements -- --ignored --nocapture
```

### Stage 9: the gateway

**Why.**
Moves are invisible to clients only if the gateway owns connections and session state (spec surface `ClientAccess`).

**Changes.**

1. **Split session from execution.**
   `node/src/session.rs` and `node/src/statement.rs` are refactored so that parameters, prepared statements, the transaction buffer up to commit and the session basis are a value the node receives per request.
   This is a `refactor(node)` commit with classic behaviour unchanged.
2. **Crate `gateway`.**
   Terminates pgwire (reusing `pgwire/src/protocol.rs` and `wire.rs`) and Flight SQL, authenticates, mints capabilities and idempotency keys, and sends statements to nodes over an RPC carrying the capability, the session state and the basis.
   Start the gateway with `--flight-listen ADDR` and the cell secret to expose a Flight SQL listener.
   Node Flight SQL endpoints must be enabled and reachable on the private cell network.
   A Flight SQL listener outside loopback requires the gateway's configured TLS certificate.
   Clients select a local deployment with the `x-xtdb-database` gRPC header, and can append `@INSTALLATION_ID` using the same UUID syntax as pgwire.
   The gateway forwards authentication to the writer with proof of the client's peer address bound to the writer's deployment ID and epoch.
   It wraps the backend's session metadata in an encrypted routing token and checks the writer assignment before forwarding another call.
   Request and response bodies stream through with HTTP/2 flow control, and the gateway holds its admission slots until the response ends.
   After a holder change, a client opens a new authenticated Flight SQL session to reach the new holder.
   For a single-installation `rtdb-multi standalone` process, pass `--owned-flight` to expose the same owned Flight actions on a second TLS loopback listener.
   The protocol 2 readiness record includes `owned_flight_port` only when that listener is enabled.
   Pass `--flight-control-relations` with a comma-separated allowlist to enable held control actions for the same authenticated standalone root principal.
3. **Crate `affinity`**, ported from Bijection's `crates/affinity` with its score vectors, and routing by it (`RouteRead`), with per-key in-flight bounds and pressure.
   Each pgwire gateway admits at most 4,096 connections overall and 256 for one deployment name by default.
   Set these limits with `--max-connections` and `--max-connections-per-deployment`.
   The overall limit includes connections that have not completed startup.
   When the overall limit is exhausted, the gateway closes new sockets before startup.
   A deployment at its limit rejects new sessions with SQLSTATE `53300`.
   Closing a session releases both its connection slots.
   Each gateway charges queued PostgreSQL frame bytes and their wire copies to a 1 GiB memory budget.
   Set the budget with `--max-buffered-bytes`.
   The receiver reserves memory using the declared frame size before reading the body.
   With this default, one frame and its encoded copy must fit within 1 GiB of available gateway memory.
4. **Redirects**: `NotHolder`, `Moved`, `Behind`, followed by the gateway without the client noticing.
   The gateway renews a deployment's idle protection at the backend connected to the session, including after a move.
   Sessions on one gateway share a heartbeat for each deployment and backend address until the last connection to that backend closes.
   Local pgwire startup, including routing and the holder's greeting, has a 35-second deadline.
   If a holder accepts a socket but stalls before `ReadyForQuery`, the gateway closes startup with SQLSTATE `57P03` so the client can retry.
   A retry waits for a placement change or the 100 ms fallback timer, whichever arrives first.
   A writer with gateway key support installs the key before executing the following command.
   SQL session changes and `DISCARD ALL` preserve the installed gateway key.
   For ordinary keyed transactions, string application keys take precedence over the gateway key and must stay stable for deduplication.
   An ordinary keyed transaction without a string application key uses the gateway's key and transaction ordinal.
   If installation fails, the writer closes its backend connection before it executes that command.
   The gateway resends an unanswered simple query under a private permit when both writers support the permit contract.
   The replacement acknowledges the permit and checks that the request is a single autocommit statement before executing it.
   A permitted write has one engine-owned marker under the command key, separate from application keys.
   The original and successor share the submission cutoff issued at command start.
   Both reject a permitted write before append if it reaches submission at or after the cutoff.
   The marker covers the entire permit window even when the writers' clocks differ.
   The gateway issues permits for up to 30 minutes from the initial command and keeps the same cutoff across replacements.
   An ambiguous batch, transaction-control command or extended Execute returns an unknown outcome.
   Extended-protocol setup can still be replayed before any execution frame is forwarded.
   Mixed-version cells continue to serve fresh commands.
   Roll gateways first and drain sessions using the old permit before relying on the new contract.
   The gateway authorizes replay only for writers advertising the current permit capability in both attempts.
   The current permit capability records a leading assertion that aborts before user operations when executed by a legacy resolver.
   A compatible resolver checks and removes that assertion before it runs the protected mutation.
   Serve pre-existing records issued under the old permit with a compatible resolver until their source prefix has resolved.
   Place the writers' backend listeners on the private cell network and expose the gateway listener to clients.
   The gateway uses one 30-second window from the first replayable failure until it takes a replacement's first reply for forwarding to the client.
   All attempts to recover the pending command share that window.
   If that window expires, the client receives SQLSTATE `08006` with an unknown outcome.
   Automatic write replay requires the original permit to still be admissible at the replacement.
   A storage-close reply during handoff requires a newer writer epoch for the next replay.
   A gateway routing directly to a known writer reads that member by ID before considering other candidates.
   A redirect is followed through the replacement node’s control endpoint so pgwire connects after the replacement confirms its endpoint and epoch.
   The writer checks its own membership by ID when it reuses a running placement.
   If a placed database detaches or changes while a query checks authority, the node refuses before planning so the gateway can retry.
   A direct Flight SQL session on a placed node supplies `x-xtdb-database` on its first authenticated request.
   Later calls can omit the header and use the bound deployment.
   A conflicting header or a ticket naming a different deployment is refused.
   Transaction and prepared-statement handles are scoped to their authenticated owner and local placement.
   Listing catalogs through a placed Flight SQL endpoint reports the session's deployment.
   A backend session belongs to the local placement it opens against.
   If the node opens a new placement with the same name, the gateway must establish a new backend session.
   A driver that stops during execution can leave the outcome unknown.
   The gateway replays eligible unanswered commands under their original permit.
   Writable pgwire sessions accept pipelined frames and dispatch one command cycle at a time.
   Each autocommit cycle gets its own key after the previous cycle's ReadyForQuery.
   In an explicit transaction, the cycles retain the transaction's key through commit or rollback.
   COPY input continues to stream before ReadyForQuery. [PostgreSQL protocol flow](https://www.postgresql.org/docs/current/protocol-flow.html)
   Extended COPY ignores Sync while consuming input and reports readiness on the Sync that follows completion or cancellation.
   A read-only pgwire listener rejects COPY input before accepting payload bytes.
   A successful DISCARD ALL clears the gateway's prepared statements before a later reconnect.
   At each idle command boundary, a placed node reports changes to the session’s await token over its private gateway connection.
   The gateway restores the token, including its removal, before replaying named prepared statements on the new backend.
   A replacement backend must support await-token restoration to resume a session with a saved token.
   Gateway reconnects restore the session timezone, parameters and default transaction access before preparing statements.
   Replaying the connection state keeps the startup database and authenticated principal.
   The gateway tracks settings and await-token support separately during a rolling update.
   SQL PREPARE and named pgwire Parse register replayable definitions with their preparation-time search path and timezone.
   The pgwire definition retains the parameter type OIDs supplied by the client.
   The successor restores the definitions in registration order before accepting another command.
   A single pgwire autocommit write sends `CommandComplete` after the commit succeeds.
   PGwire carries stable placement interruption codes independently of the configured error detail format. [PostgreSQL ErrorResponse format](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ERRORRESPONSE)
5. **Suspicion reports** to the controller on connection failure.

**Tests.**
Protocol tests through the gateway reuse the existing pgwire and Flight SQL suites; a client's session survives a planned handoff and a failover; the affinity score vectors match Bijection's.

**Specs.**
`tx.allium`: session state's home in multi mode.

**Done when** the pgwire and Flight SQL suites pass through a gateway against a three-node cell, through continuous handoffs.

### Stage 10: the read tiers

**Why.**
Reads must scale with read load without standing followers (spec rules `ServeOnWriter`, `ServeLinearizableReadOnWriter`, `ServeOnSubscriber`, `ServeSnapshot`, `WidenOnPressure`).

**Changes.**

1. `Subscription`: the follower apply path from `ReplicationDriver`, fed by `Loglet::subscribe` from the checkpoint, created when a deployment's read width admits a member and torn down when demand stops.
   Each gateway tries the replica with the fewest connected sessions within the deployment's affinity ranks and uses the holder if those replicas refuse.
   New sessions rotate the starting rank to spread simultaneous connection attempts.
2. The snapshot tier: any member serves reads at a basis at or below the checkpoint from object storage and cache, with no log.
3. Linearizable reads on the holder wait for the next round's seal announcements; on a subscriber they fetch a read index from the holder.
4. Read width widened and narrowed by the controller from per-key overflow.
5. Scan fragments for large deployments routed by `(storage identity, table, trie prefix)`.

**Tests.**
Read-your-writes across tiers; a zombie holder cannot serve a linearizable read; subscriber bounce-to-writer on lag; snapshot reads with the log unavailable.

**Specs.**
`rtdb-multi.allium` read rules, corrected by implementation.

**Done when** a hot deployment's reads spread across its read width with bounded staleness.

Nodes use the same eligible affinity ranks as the gateway when opening a log-fed read replica.
A narrower width retires out-of-rank replicas that serve only Pgwire reads, and their gateway sessions reconnect within the remaining width.
A replica serving scan fragments can remain attached.

Each fragment returns selected storage columns as Arrow vectors, preserving their row order and field layout.
A fragment can send more than one IPC frame when selected rows use different layouts.
Enable automatic width with `rtdb-multi controller --read-sessions-per-node N`.
With this policy enabled, the controller manages the width of live writer placements and manual width changes are subject to later adjustment.
The default maximum width is eight, including the writer, and `--max-read-width N` can set a different ceiling.
The controller samples authenticated read-only Pgwire connections on writers and replicas across gateways.
Snapshots, Flight SQL and writable Pgwire sessions are outside this demand signal.
Above the configured session count, the controller widens one rank at a time up to available affinity ranks and the configured ceiling.
Once a replica is admitted, capacity calculations assign eligible read sessions to replicas and reserve the writer for writes and fallback.
Wider routing opens replicas on demand for new connections and for sessions that reconnect.
Narrowing uses half the widening thresholds for three complete observations of the same width and writer epoch.
A missing sample in the writer’s pool resets that quiet sequence.
Widening can proceed on observed pressure when the next affinity rank has reported.
Upgrade the serving nodes and gateways before enabling the controller policy.


### Stage 11: the controller

**Why.**
Placement must follow load and the fleet must scale both ways (spec surface `ControlPlane`).

**Changes.**

1. Crate `controller` covers serving endpoint health, admission of prewarmed members, drain, load-watermark rebalancing, orphaned tails, quarantine and engine rollouts.
   The controller probes at most 16 live node endpoints concurrently.
   Each round checks the control response and the Pgwire listener's response to a private serving probe.
   The serving probe needs no authenticated Pgwire session slot.
   A listener whose control response predates the serving probe is checked for TCP reachability.
   Consecutive failures count only for one unchanged address pair.
   A ready round resets the count, and a member with either address changed cannot be confirmed dead by the old observation.
   A capable listener answers a private serving preface before the gateway sends a Pgwire startup message.
   The gateway can retry a missed preface or a failed control connection before the startup deadline.
   A placed node prepares up to eight independent database opens at a time.
   The catalog owner publishes ready databases and preserves request order for each name.
   The controller clears failure evidence when it observes startup inspection overflow during a probe.
   A gateway with a failed direct route can reach the same writer through a signed control-plane relay to that writer's local Pgwire listener.
   The relay runs the ordinary Pgwire authentication and is available when the node's authentication rules are independent of the peer address.
   A gateway's direct-path failure alone leaves placement unchanged.
   When the writer's serving endpoint fails from the controller's perspective, progress requires an eligible successor that reaches storage and a write quorum.
   A draining node hands off up to eight recently used deployments per pass and parks its idle deployments.
   It runs the handoffs one at a time.
   The source's identity and epoch must still match at reservation.
   A rebalancer ends its current pass when another operation has reserved the deployment or changed the source holding.
   The optional `--rebalance-write-bytes-per-second`, `--rebalance-query-cpu-ms-per-second`, `--rebalance-tail-bytes` and `--rebalance-cache-bytes` are per-node watermarks.
   The controller samples up to 16 eligible endpoints concurrently and chooses moves from the strongest configured pressure.
   The live tail sample counts the current block's retained event batches and their headers.
   The cache sample counts bytes charged to the deployment's in-memory pool, including decoded views and outstanding read leases.
   The write rate uses encoded source bytes accepted by the holder between two observations of the same epoch.
   The query CPU rate records thread CPU spent opening native queries and producing result batches on the holder.
   Linux and macOS nodes start measuring that counter on the first controller probe that requests it for a holding.
   With a rate watermark configured, a node with an unsampled holding waits for another observation before participating in moves.
   The next eligible member in the deployment's affinity order must fit the sampled load under every configured limit.
   Moves through the configured watermarks are limited to eight per pass by default.
   Use `--rebalance MOVES` to override that limit.
   A recently used deployment keeps serving on a draining node while its pool has no eligible successor.
2. Planned handoff (`IntendMove`, `HandOff`): subscription warm-up, hot-range prefetch from the source, admission pause, `Handoff` record, claim by move intent.
   The gateway, claim rule, and rebalancer use the same eligibility check for the general and isolation pools.
   Clearing quarantine fences the isolation holder so the next claim returns the deployment to the general pool.
   A planned move reserves a target for a bounded interval and releases the reservation if activation fails.
   The target can retry temporary memory pressure or directory write contention under the same reservation.
   The source park verifies the holding epoch and reservation, and a delayed target activation requires the reservation at its claim.
   Before releasing the placement, the source pauses transaction admission and waits for all admitted writes to apply.
   When the drained source has at most 64 live rows and 16 replica records since its last adopted boundary, it can park without uploading a block.
   The successor seals the quorum and recovers the durable tail before it confirms leadership.
   It checks the newest eight replica records for the source position before scanning older history.
   A node reuses encrypted disk-cache data for the same physical deployment across writer epochs, subscribers and snapshots.
   A new storage generation has a different cache identity.
   Once a write quorum acknowledges the recovered commit position, the successor can leave a delayed replica to background repair.
   The logkeeper integration test holds the third replica’s recovery overwrite and then confirms it catches up after the new holder writes.
   Recovery also finishes when a third replica drops the separate commit request that publishes the recovered tip.
   Larger tails use the source's flush path, and the source stops serving before it resumes admission.
   A placed writer waits 2 ms at the committed log tip before its claim.
   Control requests observe the replication driver's leadership and block-adoption updates directly.
   Start nodes with `--engine-version VERSION` when the package version does not identify their image.
   `pin-engine --name NAME --version VERSION` restricts claims, planned moves and compaction to the named engine version.
   With an active holder or placement reservation, a new pin must match that engine version.
   Forks inherit the pin, and DynamoDB keeps it when writing a packed parked record.
   After adding eligible nodes of the desired version, `roll-engine --version VERSION` starts drains where every current holding has a compatible destination in its pool.
   It reads the deployment inventory and preserves an eligible member for a pool with a parked deployment or an already-draining holding pinned to the old version.
   Serialize engine-pin changes with rollout scheduling for the same cell.
   These drains send holdings to the selected version, including idle holdings.
   A pinned cell move checks for an eligible destination of the pinned version before fencing the source and before changing cell placement.
   To move a deployment between compatible images, clear its pin, move it to a member of the new version, and set the new pin.
   Upgrade all placement and directory writers before using pins or starting a version-targeted drain.
   Lazy format conversion and mixed engine storage-format validation remain open.
3. Autoscaling signals per pool, published as metrics through `metrics` and `server/src/metric_export`: nodes by the binding `LoadVector` dimension, logkeepers by ingest, workers by queue depth, gateways by connections.
   `rtdb-multi status --placements-only` reports eligible and draining nodes, indexed holdings, and the holder-count target for each placement pool.
   This mode reads member holdings without scanning the parked deployment inventory.
   DynamoDB verifies each returned indexed holding against its current record, and a newly placed deployment appears once the holder index has caught up.

**Tests.**
Simulation: scale-out moves only the keys that prefer the new member; scale-in retires members only when empty; a poisonous deployment is quarantined after `quarantine_crashes`; the balancer converges without oscillating.

**Done when** a simulated cell under Zipf load scales out and in with planned-handoff write stalls under 30 ms.

**Local handoff and failover check**

The release workload placed 1,000 deployments on three nodes, moved each to a second node, and then killed the holder with the most current placements.
The three logkeepers used fsync and a two-member quorum.
Every deployment returned the result of exactly three counter writes, and each of the 285 deployments held by the failed node advanced to a later directory epoch.
The surviving nodes peaked at 443,264,941 and 442,159,543 execution bytes against a 512 MiB limit each.
A previous run without memory reclamation reached the execution budget and could not finish the same workload.
With the JVM suite running on the same host, the interval from move request to the first successful write had a median of 184.7 ms and p99 of 725.0 ms.
Two release runs without a build running concurrently used the same 1,000-deployment workload and eight concurrent operations.

| Handoff policy | Move and first write p50 | Move and first write p99 | Highest survivor execution memory | Kill through counter verification |
| --- | ---: | ---: | ---: | ---: |
| Flush every move | 81.6 ms | 222.4 ms | 422.2 MiB | 19.6 s |
| Replay the short tail | 39.2 ms | 45.3 ms | 102.1 MiB | 5.5 s |

The short-tail run measured the move alone at 33.5 ms p50 and 39.7 ms p99.
It verified all 1,000 counters and checked the later epochs of the 346 deployments held by the node that was killed.
With the 2 ms wait and status notifications, three spaced runs measured move-to-first-write p50 of 15.4–15.7 ms and p99 of 17.3–18.3 ms.
All counters matched after failover in the three runs, including the 341–351 deployments that changed epochs after their holder was killed.
A third back-to-back run hit a local `EADDRNOTAVAIL` on connection setup, and the spaced repeat completed.
With a connection issuing writes continuously, three release runs moved its deployment 48 times each.
The final counter matched all 316–398 acknowledged updates per run.
The largest inter-commit gap overlapping each move had a median of 18.6–22.1 ms, p95 of 25.8–27.3 ms, and p99 of 31.4–41.1 ms.
After introducing the admission pause, five spaced release runs of the same workload matched all 412–432 acknowledged updates per run.
The inter-commit gap was 14.8–16.0 ms at p50, 19.5–32.8 ms at p95, and 21.0–75.9 ms at p99.
The continuous-write p99 stayed below 30 ms in two of the five runs.
Five additional runs recorded both the client gap and the administration call.
The worst client gap in those runs ranged from 34.3 to 100.9 ms, with the corresponding administration calls taking 30.1 to 76.8 ms.
In the 100.9 ms case, the administration call finished in 39.0 ms.
With the cold activation quota and the admission pause, one 1,000-deployment release run activated the deployments in 1.56 seconds.
It measured a move-to-first-write p50 of 7.0 ms and p99 of 17.6 ms, then verified all counters and 342 new placements in 2.51 seconds after killing the busiest node.
With placement-triggered reconnects and cancellation-aware block collection, a repeat measured 6.9 ms p50 and 8.7 ms p99 and verified the 342 replacement placements in 2.49 seconds.
Five 48-move trials with continuous writes matched all 366–414 acknowledged updates per run.
Their p95 inter-commit gaps were 19.4–30.0 ms and their p99 gaps were 30.9–60.9 ms.

```sh
cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  writes_keep_progress_during_repeated_handoffs -- --ignored --nocapture
```

```sh
RTDB_CELL_HANDOFF_DEPLOYMENTS=1000 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  a_thousand_deployments_keep_their_history_through_handoff_and_failover -- --ignored --nocapture
```

Three alternating release comparisons used the same local build with durable retry lookup, 32 warm deployments, 16 workers, four connection slots per deployment and 32,768 updates.
The builds differed only in the number of simultaneous planned moves on the draining node.
All six runs completed and verified the final counter of each deployment.
Serial drains held scale-out p99 between 10.1 and 17.1 ms and scale-in p99 between 16.1 and 21.4 ms.
With two concurrent moves, scale-out p99 ranged from 10.4 to 18.3 ms and scale-in p99 ranged from 18.3 to 35.9 ms.
The tested build preceded the hosted transfer and archive integration.

After integrating hosted transfer and archive, four more alternating comparisons each verified all 32,768 writes against their final counters.
The checkpoint build measured scale-out p99 from 32.0 to 1,992.6 ms and scale-in p99 from 58.1 to 145.2 ms, at 2,275–4,608 writes/s overall.
The variant that sent both key opens and storage-region witness I/O to the blocking pool measured scale-out p99 from 52.3 to 120.9 ms and scale-in p99 from 130.3 to 270.8 ms, at 2,140–3,211 writes/s overall.
The comparisons leave the 30 ms acceptance gate open.
The key-open regression test holds a filesystem lock while the runtime services a timer.

One instrumented run of the same workload traced 21 moves.
Target database attach took 30.9 ms at the median and 197.1 ms at the slowest, while the source park RPC took 8.8 ms at the median and 16.7 ms at the slowest.
A second instrumented run traced 21 moves and measured local storage-region admission at 13.3 ms median and 103.4 ms maximum, checkpoint recovery at 5.5 ms median, and log opening at 5.1 ms median.
That run verified all 32,768 updates and measured end-to-end p99 of 50.0 ms during scale-out and 69.3 ms during scale-in.

Three alternating pairs with the same instrumented build compared on-demand witnesses with one preallocated standby witness per local node.
All six trials verified their 32,768 updates.
Per-run median storage-region admission ranged from 37.3 to 122.1 ms on demand and from 0.041 to 0.071 ms with a standby witness.
The trial ranges for scale-out p99 were 112.2–192.6 ms on demand and 38.1–165.4 ms with a standby witness.
The trial ranges for scale-in p99 were 141.4–217.7 ms on demand and 108.8–208.5 ms with a standby witness.
These trials shared the host with test builds and leave the 30 ms acceptance gate open.

Three paired runs on the same local host compared the 16-record and 64-live-row policy with a 64-record and 256-live-row policy whose larger tail had to fit in 128 KiB of writer-buffered data.
All six Zipf trials verified their 32,768 updates.
The existing policy measured 75.6–156.8 ms p99 during scale-out and 91.8–206.3 ms during scale-in.
The larger tail measured 87.3–200.8 ms during scale-out and 109.7–453.0 ms during scale-in.
Three 48-move continuous-write trials per policy measured gap p99 of 38.1–150.0 ms with the existing policy and 132.6–4,498.9 ms with the larger tail.
The move corresponding to the 4.5-second gap also took 4.5 seconds in the administration call.
The branch keeps the 16-record and 64-live-row limits.

Three seeded release pairs also compared the existing policy with one checkpoint prefetch per target, given a 100 ms window before the source parked.
The host was compiling and running tests during this comparison.
All three baseline Zipf trials and two of the three prefetch trials verified their 32,768 updates.
The remaining trial exposed a query racing with local placement removal, which the placed node now returns to the gateway as a retryable placement change.
Baseline p99 ranged from 68.8 to 188.2 ms during scale-out and from 119.7 to 253.5 ms during scale-in.
The completed prefetch trials measured 169.7–686.4 ms during scale-out and 97.1–138.8 ms during scale-in.
The 48-move gap p99 ranged from 100.3 to 2,358.4 ms on the baseline and from 129.2 to 3,032.8 ms with the prefetch.
The branch keeps the existing handoff path while the prefetch strategy is evaluated.

A second comparison ran three paired release trials with the same 32,768-update Zipf workload and 48 continuous handoffs per build.
In this experiment, opening the checkpoint and quorum log overlapped after authorizing the storage region.
All six Zipf counters and all six handoff counters verified on a shared development host.
Baseline scale-out p99 ranged from 105.8 to 255.9 ms and scale-in p99 ranged from 74.0 to 292.9 ms.
With overlapping opens, those ranges were 148.4–2,304.7 ms and 111.8–3,734.8 ms.
The 48-move gap p99 ranged from 122.3 to 182.2 ms on the baseline and from 133.8 to 212.5 ms with overlapping opens.
Checkpoint and quorum-log opens remain sequential.

### Stage 12: encryption, lineages, forks and deletion

**Why.**
Deletion must be provable and forks must be cheap (spec rules `Fork`, `ForkAcrossOrganisations`, `BeginDeletion`, `TransferSharedObjects`, `EraseAndProve`).

**Changes.**

1. Per-deployment data keys from KMS with the deployment as encryption context; per-object data keys wrapped by the owner's key; log payloads encrypted.
2. `Lineage` records with a version bumped by every fork and deletion.
3. Fork: a new record in the lineage with a fork checkpoint referencing the parent's objects plus one pack of the parent's tail to the fork basis; inherited reads capped at the fork basis; cross-organisation forks copied and re-encrypted.
   Fork reads retry their local copy if the reaper transfers an inherited object between the first local read and the read from its original owner.
   A reference-manifest read repeats the listing if a concurrent revision replaced the listed manifest.
   Use `rtdb-multi fork --org ORG` to copy a checkpoint or historical fork into another organisation in the same cell.
   The destination owns its key, lineage, checkpoint objects and replica tail.
   The directory publishes the child after copying its selected checkpoint and tail and checking the source's epoch and storage generation.
4. Deletion: fence by epoch bump, seal and drop the stream, wait `credential_ttl`, re-wrap shared objects to a surviving referrer, delete all versions, prove the prefix empty twice, destroy the key, sign the deletion record.
   The delete command durably drops the stream from every member of its current ensemble and any pending source ensemble before proceeding to object erasure.
   A missing replica leaves the deployment fenced in `deleting`.
   Retry after the replica returns.
   A terminal logkeeper record prevents reopening the stream after restart.
   Shared journal segments reclaim dropped payloads during compaction.
   All logkeepers must support the drop operation before using this deletion path, and binaries predating it cannot open a journal containing its record.
   Replica addresses retired before ensemble-transfer tracking was introduced require separate inventory.
   The current deletion path keeps a deployment in `deleting` for retry if objects remain.
   Deletion fences the deployment and waits for recorded S3 write sessions to expire before touching its object inventory or key.
   While the lease is active the delete command reports the earliest retry time and the deployment stays fenced.
   S3 deletion also waits 15 minutes and one second from fencing to cover credentials issued before lease tracking was enabled.
   Source-cell cleanup on S3 waits until the same interval has passed since cutover as well as for the configured reaper grace and any recorded write lease.
   Ordinary garbage collection skips a deployment once its lifecycle reaches `deleting`.
   On a shared local volume, forks, cell moves, deletion and a reaper pass take the same advisory file lock before reading the inventory and mutating it.
   A conflicting administrative command returns a retryable busy error, and the reaper retries on its next pass.
   Use the same configured local volume and upgraded processes for every administrative writer in the cell.
   After verifying the inventory and destroying the local key, it signs a receipt with the key service's Ed25519 signing key and stores it with the tombstone in one directory CAS.
   Retrying a completed deletion returns the stored receipt.
   Export the trusted public key with `rtdb-multi deletion-key --keys PATH` and verify the JSON from `delete` with `rtdb-multi verify-deletion --record PATH --public-key KEY`.
   The signed `facts` name the deployment, its fencing epoch, prefix, object and key namespaces, and the inventory scope.
   A local receipt has scope `current_objects`.
   Below the slash-delimited deployment prefix, S3 deletion aborts incomplete multipart uploads and verifies that their parts are gone, then removes retained versions and delete markers by version ID.
   It rechecks both inventories after these mutations before recording the first empty observation.
   It stores the first empty version and multipart inventories in the directory and reports the next retry time.
   Retry `delete` at least 15 minutes and one second after the first observation for a read-only second inventory.
   If the second inventory finds an object version or multipart upload, it resets the observation and keeps the key.
   A further `delete` attempt erases and starts the observation interval again.
   Once both empty observations are durable, the key service destroys the key and the signed receipt includes `first_empty_observed_at_ms` and `objects_erased_at_ms`.
   The S3 deletion role needs `s3:ListBucketVersions` and `s3:DeleteObjectVersion` alongside its current-object permissions. [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
   Multipart cleanup uses `s3:ListBucketMultipartUploads`, `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts`. [Multipart upload permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpuAndPermissions)
   MinIO's multipart prefix query returns exact key matches.
   Custom S3 endpoints use a paged bucket listing for multipart uploads and abort only entries under the deployment prefix.
   Upgrade all directory readers before an S3 deletion writes the new scope to its tombstone.
   Upgrade directory readers, writers, and receipt verifiers before an S3 deletion writes the observation journal and its signed timestamps.
   The earlier single-observation receipt remains verifiable with upgraded tools.
   A new deployment or fork uses an organisation identifier that fits one storage path component.
   Prefix erasure rejects overlap with another recorded deployment's prefix, including legacy records with older organisation identifiers.
   A holder binds its deployment to the node's configured namespaces in its first claim, and forks and cell moves carry the binding into their destination records.
   Deletion checks that binding before fencing, and a pristine root may take its first binding in the deletion CAS.
   Ensemble transfer records the old and intended logkeeper addresses before it starts copying records.
   Deletion waits for terminal drops from the current ensemble and every address recorded by an earlier transfer attempt.
   A deployment's retained copy addresses remain in its JSON directory record.
   Upgrade directory writers before relying on recorded copy history for new ensemble changes.
   An existing deployment without a binding acquires one from a configured holder before forking or moving.
   An older deletion already in progress can adopt the binding when all live nodes publish the same configuration.
   The full proof also requires a separately trusted, epoch-scoped storage credential issuer, provider KMS erasure and exclusion of concurrent administrative writers such as earlier reaper or lineage-transfer passes.
   A deployment moving between cells keeps ownership of its source tail until cutover.

**Hosted key custody.**
The hosted Multi binary can use a dedicated S3 key vault configured with `s3://BUCKET?region=REGION`.
The bucket must have never enabled versioning, and vault objects use S3 server-side encryption.
Creation uses conditional writes and verifies the installed value before returning.
Withdrawal conditionally replaces the current key with a permanent tombstone and rechecks bucket versioning before acknowledging the withdrawal.
The cell namespace and deletion signer are stored separately from per-deployment, stage and checkpoint keys.
The directory records the selected storage and key namespaces before a residence opens.

**Tests.**
A same-organisation fork copies no payload.
A cross-organisation fork copies the selected checkpoint and tail with an independent key and remains readable after the source is erased.
A deleted deployment's objects are unreadable from the moment its key is destroyed.

**Done when** deletion produces a signed record and the scrubber finds nothing left under the prefix.

### Stage 13: cells

**Why.**
Unbounded means adding cells (spec contract `CellMap`).

**Changes.**

1. The cell map, encoded in deployment hostnames where possible.
   `--cell-map dynamodb:PREFIX` reads and updates the map directly when `rtdb-multi` is built with the `dynamodb` feature.
   Use one AWS region for the map's strongly consistent reads and conditional writes.
   A placement write carries an idempotency token so a retried DynamoDB request returns the original write result.
   Provision `PREFIX-cells` and `PREFIX-placements` with a string partition key named `id`.
   `cell-map --dynamodb-prefix PREFIX` serves the same data over the existing TCP interface.
   `RTDB_DYNAMODB_ENDPOINT` selects DynamoDB Local and creates the tables for local testing.
2. Cell move: fork into the target cell, fence, copy the remaining tail, flip the map, leave a forwarding tombstone (`BeginCellMove`, `MoveCutover`).
   The source and target must have distinct physical object namespaces and key namespaces.
   A move verifies both before fencing the source.
   It refuses a shared namespace, a storage root inside the other deployment's cleanup prefix, or a conflicting destination key.
   Overlapping S3 prefixes in the same named bucket are rejected before writing namespace markers.
   Retry `move-cell` with the same source, destination and namespace configuration when preparation or cutover is interrupted.
   A retry before preparation completes copies to a fresh destination prefix under the original writer fence.
   The destination prefix must be disjoint from all deployment prefixes recorded in the destination directory.
   Once prepared, the source records the chosen prefix, the destination staged record waits for the placement switch, and retry finishes the same switch or source tombstone.
   The destination's holder imports the old log ensemble before log-fed replicas can serve it.
   The source reaper verifies the recorded object namespace and any supplied key namespace before erasing the old copy.
   Legacy tombstones without a recorded source binding require recovery of those historical namespace IDs before automatic cleanup.
   With S3 it empties the source's multipart and version inventories before destroying the source key copy.
   Source cleanup also waits for the stored write lease to expire.
   The tombstone records successful cleanup so future passes can skip it.
   A later pass with `--keys` rechecks storage before finishing a previous objects-only cleanup.
3. Format upgrades: readers for N-2 to N, rewrite on flush and compaction, a maintenance trickle for the rest.

**Done when** a deployment moves between two simulated cells with one fenced write stall.

### Stage 14: hardening at scale

Running alongside every stage from stage 2 onwards, and concluding the branch:

1. The `simulation` crate drives a whole cell deterministically, with simulated network, clocks, object store and disk faults.
2. A zombie harness freezes a holder mid-upload, mid-append and mid-flush and resumes it after its successor has taken over.
   The MinIO test holds the old node's conditional multipart completion for a pack until the successor writes and flushes.
   It verifies that the old flush waiter exits when deposed, the late upload reaches storage and receives HTTP 412, and the successor's stored pack and recovered rows remain intact.
   A separate MinIO test discards a successful multipart completion reply and verifies the adopted checkpoint after moving to another holder.
   A TCP proxy test holds complete append frames from the old writer before forwarding them to at least two of three fsync-enabled logkeepers.
   The successor seals those same servers, recovers the committed entry, and the servers refuse the delayed append before the successor writes at that position.
   This test drives the logkeepers directly.
3. A hash-chain auditor verifies, offline, that every deployment's history is one chain across epochs.
   The `audit` command compares a SHA-256 chain over the latest committed, retained interval shared by a write quorum.
   Each link covers the record position, epoch, timestamp and on-disk payload.
   The report includes the inclusive interval, each reachable replica's retained window, the required quorum, the computed digests and unavailable endpoints.
   It reports a divergence if the digests differ.
   A reachable empty replica cannot prevent the other replicas from being compared.
   `audit --require-quorum` reports failure when too few distinct endpoints can independently verify that interval.
   Auditing a pristine deployment leaves its data key unallocated.
   On a stopped logkeeper, `rtdb-multi audit-log --data PATH --stream STREAM --from LSN --through LSN --expect DIGEST` verifies the same interval directly from journal segments.
   The command takes a shared filesystem lock and reads the segments without modifying them.
   A damaged record or a digest mismatch makes the command fail.
   Save the JSON from an earlier `audit-log` run and provide it to a later run with `--prefix-report FILE` to continue the same digest across a trim.
   The saved report must name the same stream and end immediately before the retained interval being verified.
   Compare the result with an independently known full-chain digest using `--expect DIGEST`.
   The supplied report serves as evidence for the discarded prefix.
   Before trimming a published block’s replica prefix, the holder asks a durable write quorum for matching SHA-256 digests and records the agreed interval in the directory.
   The logkeepers hash bounded pages locally after the selected commit is durable, returning only the page digest and end position.
   The next trim extends that saved prefix, and a cell move carries it with the deployment.
   An installation whose prefix was trimmed before this feature can certify history only from its first available quorum-retained record.
   `audit` reports the recorded prefix as `replica_anchor` and, when a durable quorum can verify the adjacent suffix, reports the extended digest as `replica_continued_digest`.
   Save the `replica_anchor` object on its own as the offline command’s `--prefix-report`, and use the extended digest as `--expect`.
   Install the new directory, logkeeper and node versions together before relying on continuous anchors.
4. A staging cell holds ten million deployments (300 bytes each when parked) under Zipf activity.

**Local parked-density measurement**

An Apple M4 Max debug build created parked records directly in `MemoryDirectory` with three logkeeper IDs per record and one active keyed watch.
The run includes the directory's deployment and name indexes and uses process resident-set change divided by the number of parked records.

| Build | Parked deployments | Directory inserts/s | Resident bytes per record | JSON bytes for the last record | Additional Tokio tasks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before hosted recovery and custody | 100,000 | 175,214 | 1,548 | 515 | 0 |
| Before hosted recovery and custody | 1,000,000 | 152,846 | 1,544 | 516 | 0 |
| Hosted recovery and custody | 1,000,000 | 118,297 | 3,495 | 606 | 0 |
| Hosted recovery and custody with boxed index values | 1,000,000 | 133,613 | 2,450 | 606 | 0 |
| Hosted recovery and custody with operation history allocated on demand | 1,000,000 | 136,052 | 1,406 | 606 | 0 |
| Hosted recovery and custody with operation history allocated on demand | 10,000,000 | 132,558 | 829 | 607 | 0 |

The in-memory directory stores one boxed value for each deployment, so unused index slots reserve space for pointers instead of full records.
Transfer records, restore records and disposal receipts allocate their larger values only when an operation creates them.
The durable local directory file retains the same JSON shape.
The ten-million-record run measured resident pages at the end of the load and verified the keyed watch.
It used a single in-memory directory with no concurrent SQL workload.

```sh
RTDB_PARKED_BENCH_DEPLOYMENTS=1000000 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --test footprint \
  parked_directory_density_with_an_active_watch -- --ignored --exact --nocapture
```

**Local cell under Zipf load**

A release run on `94f351686` kept ten million unrelated deployments in memory while 16 writers drove 32 other deployments through real pgwire sessions and three fsync-enabled logkeepers.
All 32 active deployments started cold and acquired holders.
The workload verified every final counter across 32,768 writes, seven scale-out moves and a scale-in drain of 12 holdings.
The process peaked at 8.61 GiB of sampled RSS.
It sustained 3,766 writes/s with overall p99 of 18.9 ms and p99 of 16.4 ms outside scale transitions.
Scale-out and scale-in p99 reached 279.5 ms and 105.5 ms respectively.
Phase output reports connection startup and initialization separately from waiting for a connection slot and from the wire round trip.
The provider staging run still needs equivalent evidence from the production directory and storage.

```sh
RTDB_CELL_BENCH_PARKED=10000000 RTDB_CELL_BENCH_COLD=1 RTDB_CELL_BENCH_SCALE=1 \
RTDB_CELL_BENCH_CONNECTIONS=4 RTDB_CELL_BENCH_TIMEOUT_SECONDS=240 RTDB_TEST_PGWIRE_CONNECTIONS=256 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  durable_pgwire_updates_under_zipf_activity -- --ignored --exact --nocapture
```

**Three-availability-zone recovery and startup**

The AWS qualification ran a release build on one C7g.4xlarge and two C7g.large instances in three us-east-1 availability zones.
Each zone hosted a logkeeper with fsync enabled, and the directory used durable file-backed TCP service.
The cells used AWS S3 for data and a separate AWS S3 key vault.
The gateway and controller ran in separate zones.
The release builds used the `s3-key-vault` feature and disabled link-time optimization.

Each default-block run allocated 89 deployments, seeded 25, ran 2,048 acknowledged updates over 16 persistent sessions, and checked the final total.
The recovery run explicitly checkpointed 24 installations and acknowledged another update on each immediately before it killed the writer.
A burst admitted up to 24 clients at a time across 24 recovered installations and 64 previously unopened ones.
Each recovered installation returned the checkpoint and acknowledged tail, then accepted another write.

| Phase | Serial preparation `572d72d81f` | Parallel preparation `27590505d0` | Parallel preparation with gateway TCP_NODELAY |
| --- | ---: | ---: | ---: |
| Warm update p99 | 320 ms | 320 ms | 311 ms |
| Gateway-only partition startup p99, 24 sessions | 1.14 s | 1.14 s | 1.04 s |
| Recovered startup and verification query p99, 24 sessions | 8.53 s | 3.57 s | 2.90 s |
| Cold startup and verification query p99, 64 sessions | 7.43 s | 1.30 s | 1.33 s |
| End of the combined recovery and cold-start burst | 22.51 s | 5.37 s | 5.17 s |

Per-client measurements start when the benchmark admits the request.
Burst duration includes time waiting for admission.
The parallel build also includes the upstream checkpoint table manifest change.
The last column repeats that build with TCP_NODELAY on accepted gateway client sockets.
The partition trials blocked the gateway's direct Pgwire port to the only writer and allowed the controller's path.
All 24 clients in each trial reached that writer through its control relay.

A second workload forced a checkpoint after 64 rows or six seconds.
The serial build hit the startup deadline during the combined burst.
On the parallel build all 88 startups completed and all 24 recovered deployments accepted another write.
Recovery p99 was 9.91 s, cold-start p99 was 3.09 s, and the full burst took 10.69 s.
Warm update p99 in that forced-flush run was 2.23 s.
With TCP_NODELAY enabled, the forced-flush trial again completed all 88 startups and verified 24 post-recovery writes.
That trial recorded 10.12 s recovery p99, 3.30 s cold-start p99, 11.56 s to finish the burst, and 2.30 s warm update p99.


**DynamoDB parked item size**

For an unheld deployment with three UUID logkeeper IDs, the prepared DynamoDB item uses the following estimated storage from attribute names and values.
Both forms round-trip the same directory record.

| Representative deployment | JSON item | Packed item |
| --- | ---: | ---: |
| Pristine root | 695 B | 146 B |
| Previously active root with checkpoint and storage binding | 945 B | 218 B |
| Previously active root with three retained IPv4 journal endpoints | 1,020 B | 272 B |

The estimates exclude DynamoDB's 100-byte per-item storage overhead and the separate deployment-name item.
Numeric attributes use DynamoDB's approximate size formula. [DynamoDB item sizes and formats](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html)

Upgraded readers and Streams followers accept both formats with unchanged lookup keys and CAS conditions.
Upgrade every direct DynamoDB client, including admin and reaper processes and Streams followers, before enabling packed writes.
Use `dynamodb+packed:PREFIX` for writers to store eligible unheld deployments in the packed format.
Writes convert eligible rows as they are created or updated.
Switching writers back to `dynamodb:PREFIX` stops new packed writes.
Existing packed rows require a rewrite to JSON before downgrading any reader.
Held deployments, operation journals and records with non-UUID logkeeper IDs continue to use JSON.
Parked records with ordered-index fences also use JSON.
DynamoDB Local verifies mixed layouts, the holder index, conditional updates and Streams delivery.
Rebalancing and compaction placement query live members' holdings with bounded concurrency.
The DynamoDB holder index omits unheld rows and verifies each hit by a strongly consistent read.
An existing compaction assignment also checks the deployment record when its holder index entry is missing during a handoff.
The in-memory directory updates its holder index in the same critical section as each deployment record.
The production staging run still needs ten million records and an active Zipf workload.

**Local durable pgwire measurement**

Three release-build trials per worker count ran on an Apple M4 Max with one loopback gateway, three holders, and three fsync-enabled logkeepers on the same SSD.
Each worker chose from 32 warm deployments using a fixed, seeded Zipf distribution with exponent 1 and issued 256 updates.
One command could be in flight per deployment, so measured latency includes client queueing behind earlier updates to the same deployment.
After each run the benchmark read every counter and matched its value to the number of commands sent to that deployment.

| Workers | Updates per trial | Durable updates/s | p50 (ms) | p95 (ms) | p99 (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 256 | 1,194–1,241 | 0.746–0.786 | 1.157–1.269 | 1.629–1.834 |
| 16 | 4,096 | 3,977–4,062 | 1.069–1.075 | 13.382–13.945 | 15.238–15.851 |

```sh
RTDB_CELL_BENCH_DEPLOYMENTS=32 RTDB_CELL_BENCH_WORKERS=16 RTDB_CELL_BENCH_COMMANDS=256 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  durable_pgwire_updates_under_zipf_activity -- --ignored --nocapture
```

Three release trials extended the workload to 32,768 durable updates apiece and ran through two-node to three-node scale-out, then drained and removed the holder of the hottest deployment.
Each scale-out moved 10 deployments over 55–56 ms while 196–218 writes completed.
Each scale-in drained 10–11 deployments over 275–385 ms while 751–1,328 writes completed.
Every counter matched, with 1,488–2,965 updates/s and p99 of 27.7–97.7 ms across the trials.
Three repeats with placement-triggered reconnects and cancellation-aware block collection matched all counters at 1,678–3,290 updates/s.
The repeats measured p95 of 17.4–19.2 ms and p99 of 21.2–112.0 ms.
Three warm trials with direct drain handoffs matched all 32,768 writes per run.
Their p99 was 25.0–151.9 ms during scale-out, 96.6–132.5 ms during scale-in, and 104.1–128.4 ms outside both transitions.
With the client queue measured separately, one further warm trial with all 32,768 writes matched recorded 146.8 ms p99 waiting for the hottest deployment's connection and 13.1 ms p99 from send to reply on that connection.
The remaining 31 deployments recorded 17.6 ms end-to-end p99 in that trial.
Send-to-reply p99 was 25.3 ms during scale-out, 34.3 ms during scale-in, and 12.8 ms outside these intervals.
Other Rust builds were running on the host during part of the additional measurements.
The continuous workload still has inconsistent tail latency.
The workload reports latency separately for commands overlapping each transition and for commands outside both transitions.

```sh
RTDB_CELL_BENCH_SCALE=1 cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  durable_pgwire_updates_under_zipf_activity -- --ignored --nocapture
```

A local run with `RTDB_CELL_BENCH_COLD=1 RTDB_CELL_BENCH_PARKED=100000 RTDB_CELL_BENCH_SCALE=1` added 100,000 parked records and activated the 32 queried deployments on demand.
All counters matched the 32,768 writes, with 1,013 updates/s and 155.2 ms p99 including the connection queue.
Send-to-reply p99 was 31.9 ms during scale-out, 27.6 ms during scale-in, and 16.0 ms outside these intervals.
The staging acceptance run needs a production directory, ten million parked deployments, and repeatable p99 across live scale transitions.

Two paired release trials per connection setting used 16 workers, 16,384 writes, and a 256-connection limit on each holder.
All counters matched in every trial.
The four-connection workload used up to 128 pgwire sessions on the cell.

| Connections per deployment | Durable updates/s | Overall p99 (ms) | Scale-out p99 (ms) | Scale-in p99 (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3,489–3,736 | 17.5–24.9 | 16.1–16.6 | 39.3–43.9 |
| 4 | 5,218–5,385 | 11.5–11.8 | 14.2–19.3 | 32.4–39.3 |

With four connections, scale-in send-to-reply p99 was 10.7–19.1 ms.
The scale-in end-to-end p99 still exceeded 30 ms.

Three subsequent paired release trials used the same 256-connection limit, four sessions per deployment, and 32,768 writes per run.
Each run preserved the exact counter values through scale-out and scale-in.
The next table compares how many handoffs a draining member could execute concurrently.

| Concurrent handoffs | Durable updates/s | Overall p99 (ms) | Scale-out p99 (ms) | Scale-in p99 (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 2,958–4,953 | 14.0–23.7 | 12.7–119.7 | 18.4–208.3 |
| 2 | 4,570–4,827 | 13.8–14.8 | 12.8–24.2 | 11.1–19.5 |

That comparison configured the drain to run two handoffs at a time.
The current drain runs one at a time and takes up to eight per pass.
The optional latency limit fails a run when either scale transition reaches the specified p99.
On the integrated build with journal backpressure, shared gateway heartbeats and idle flushing, two of three trials passed the 30 ms limit in both transitions.
The third scale-in measured 36.4 ms p99, and every counter still matched.

```sh
RTDB_TEST_PGWIRE_CONNECTIONS=256 RTDB_CELL_BENCH_CONNECTIONS=4 RTDB_CELL_BENCH_SCALE=1 RTDB_CELL_BENCH_SCALE_P99_LIMIT_MS=30 \
  cargo test --manifest-path rust/Cargo.toml -p rtdb-multi --release --test load \
  durable_pgwire_updates_under_zipf_activity -- --ignored --nocapture
```

**Guarded gateway keys**

A release run moved all 1,000 deployments and then killed a holder responsible for 350 of them.
The surviving cell preserved the exact counter in every deployment.
Three release trials with 32 warm deployments, 16 workers and four connections per deployment each verified all 32,768 Zipf updates.
Those trials ran alongside JVM verification jobs and measured 100.9–276.2 ms p99 during scale-out and 101.5–163.5 ms during scale-in.
Three repeats after those verification jobs finished verified the same counter totals and measured 38.1–164.7 ms p99 during scale-out and 70.5–100.8 ms during scale-in.
Other workloads remained active on the shared host.
The 30 ms acceptance target remains open.

## 6. Existing specs affected

| Spec | Stage | Change |
| --- | --- | --- |
| `allium/database-lifecycle.allium` | 1, 2 | Root marker; placed membership source |
| `allium/live-index.allium` | 1, 8 | Node-wide tail budget; tail index released on idleness |
| `allium/log-processor-lifecycle.allium` | 2, 3 | Claims require placement in multi mode; then classic-only |
| `dev/doc/db.allium` | 3, 6 | Source log and block cycle classic-only; multi log and flush are `rtdb-multi.allium`'s |
| `dev/doc/tx.allium` | 9 | Session state lives on the gateway in multi mode |
| `dev/doc/gc.allium`, `dev/doc/block-gc.allium` | 5 | Deletion classic-only; multi deletion by the reaper |
| `dev/doc/compaction.allium` | 7 | Where jobs run |
| `allium/rtdb-multi.allium` | every stage | Corrected wherever implementation disagrees |

## 7. Testing strategy

- **Contract suites.**
  `Directory`, `Loglet` and `ObjectStorage` each get a contract test suite that every backend runs, in the style of Bijection's `world::conformance`.
- **Classic regression.**
  `./gradlew test` on every commit; `./gradlew property-test` on every commit touching indexing, compaction or GC; the JVM conformance suite on every stage boundary.
- **Deterministic simulation.**
  From stage 2, every multi rule is exercised in the `simulation` crate with fault injection; the spec's invariants (`OneEffectiveWriterPerEpoch`, `NoTwoWritersHaveEffect`, `SystemTimeRises`, `CheckpointNeverAheadOfLog`, `CostFollowsActivity`) are checked after every step.
- **Benchmarks.**
  The stage 0 bench, the logkeeper benchmark and a cell load test record the spec's quantitative estimates as measurements.

## 8. Decision points

Settled:

1. **Directory backend (stage 2): DynamoDB.**
   The directory's safety rests on one conditional write per record, which DynamoDB provides fully managed, and Bijection already runs it.
   Its watches are asynchronous, which the design tolerates.
   FoundationDB remains the backend to reach for if a cell has to run outside AWS; the `Directory` contract suite keeps the backends interchangeable.
2. **First production loglet (stage 4): the logkeeper.**
   It enforces the epoch at append time, group-commits across deployments and serves per-deployment reads, which no existing log does together.
   The `Loglet` trait keeps the choice reversible per deployment at its next epoch.

Open, each also an open question in the spec:

3. **Default read freshness and submission semantics (stages 3, 10).**
   Whether `at_basis` is the default read contract, and whether losing submit-and-walk-away without an intake is acceptable.
4. **Erasure contract with forks (stage 12).**
   Whether deletion of one lineage member proved by absence plus key destruction of what it alone owned meets the compliance contract.
5. **Resident-idle target (stage 8).**
   Fixed by the stage 0 measurement.

## 9. Commit sketch

Indicative, in order; each line is one or a few commits.

- `dev(multi): specify the rtdb multi endgame and plan its implementation`
- `bench: measure the resident cost and cold open of an idle database`
- `tidy(format): build object keys from a StorageRoot`
- `perf(node): a query snapshots only the databases it names`
- `fix(storage): key the disk cache by storage identity, not database name`
- `feat(storage): a storage root refuses a database it was not created for`
- `feat(auth): authorize database access, and confine a session to one database`
- `fix(storage): evict fairly across databases sharing a cache budget`
- `refactor(index): bound tail memory node-wide and flush the largest first`
- `feat(multi): the directory, members and leases`
- `refactor(node): a placed membership source opens what the directory places`
- `refactor(replication): leadership follows placement under a directory epoch`
- `refactor(replication): extract apply and block adoption from the replication driver`
- `refactor(multi): loglets with conditional append and seal`
- `refactor(multi): the holding driver claims, seals, recovers and serves`
- `refactor(multi): route submissions to the holder with idempotency keys`
- `refactor(multi): the logkeeper`
- `refactor(multi): epoch-named objects, manifests and per-epoch credentials`
- `refactor(multi): the reaper is the only deleter`
- `refactor(multi): packs, boot objects and the multi flush policy`
- `refactor(multi): compaction runs as a job pool adopted by the holder`
- `refactor(multi): residency and activation on demand`
- `refactor(node): session state is a value a node receives per request`
- `feat(multi): the gateway`
- `refactor(multi): subscriber and snapshot read tiers`
- `refactor(multi): the controller, planned handoff and quarantine`
- `refactor(multi): encryption, lineages, forks and provable deletion`
- `refactor(multi): cells and cell moves`
- `test(multi): deterministic cell simulation and the zombie harness`

`feat:` is reserved for what a user can see or do: the gateway, real authorization, the storage root refusal, and the `rtdb multi` roles when they become usable end to end.

### Standalone authority integration

The authority opens and locks its durable logkeeper journal before advertising the member.
A restarted process reclaims that journal's identity and a replacement journal requires its own identity.
Standalone persists the directory and the logkeeper journal in its private volume.

The cell-move protocol prepares an isolated destination generation after fencing the source and waiting for its admitted storage operations to drain.
An accepted archive needed by the selected checkpoint is materialized into that generation before it becomes eligible to serve.
A retry after a lost placement reply reuses the ready destination.

Hosted disposal by installation ID and classic deletion by name settle through the same fenced disposal operation.
It waits for admitted storage regions, reconciles remote writes against their recorded provider publication identities, and retires every recorded replica journal.
The terminal record retains the hosted disposal receipt and signed inventory receipt.
Deployment-scoped S3 credentials address the same cell-relative publication keys used by reconciliation.
