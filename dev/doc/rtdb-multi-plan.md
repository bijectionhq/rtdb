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

`rtdb-multi` runs every cell process: `directory`, `cell-map`, `logkeeper`, `node`, `gateway`, `controller`, `reaper`, plus the administrative commands `create`, `list`, `status`, `drain`, `move`, `move-cell`, `read-width`, `unquarantine`, `fork`, `delete` and `audit`.
`crates/multi/tests/cell.rs` drives an in-process cell (or two) through tokio-postgres; `tests/chaos.rs` runs a writer against a seeded sequence of faults and audits the history afterwards; `tests/footprint.rs` measures an idle deployment.

| Stage | Built | Departures and gaps |
| --- | --- | --- |
| 0 | `tests/footprint.rs`: in a debug build, an open idle (empty) deployment costs about 94 KiB resident and 9 tasks, and activates in about 8 ms; the disk cache is keyed by the store; a placed node's query opens only its own database ; `StorageRoot` builds every database's storage root and puts it under a provider's prefix | multi derives each deployment's prefix from its record, above the `StorageRoot`, rather than giving `StorageRoot` an epoch-named multi layout |
| 1 | a session on a placed node sees only its own database; one pool's idle cache entries no longer starve another's; a node bounds its holdings' unflushed rows together (`--max-tail-rows`); a deployment's store refuses any other deployment ; in classic mode, a user list's `grants` limit users to the databases granted them | |
| 2 | the directory with in-memory, TCP and DynamoDB backends (DynamoDB tested against DynamoDB Local); members, leases, pools; `Membership::Placed`; claims without elections; idle terms append nothing | watches follow the backend's change feed (DynamoDB Streams on DynamoDB, a subscription over TCP), with a poll every ten intervals as a safety net; there is no watch service, so every watcher follows the whole cell's feed itself |
| 3 | one quorum append per transaction: the only durable stream is the replica stream, and the source log is the holder's in-process intake, started past every position the replica stream may hold; a transaction carrying an idempotency key in its metadata commits at most once, and a retry is answered with the original's outcome ; the gateway keys every command of a writable session, and resends a command whose holder went away before any of its replies reached the client, under the same key | a command partly answered, or one that ends an explicit transaction begun on a former holder, is still reported as of unknown outcome; keying costs a writable command a pipelined `SET` and its transactions an assertion and a put |
| 4 | the logkeeper: append-time epoch promises, seal, recovery by synced epoch, background repair, seal announcements and `confirm_leadership`, moving a stream to a new ensemble, segment files shared by every stream on a server, from which retained entries are read on demand: a server holds each stream's index of entry locations and a bounded cache of recently written or read payloads (`--cache-bytes`, 64 MiB by default) | segments are per server rather than per writer member; segments are retired by compaction, which copies every retained entry into a new base segment, rather than one by one as their entries are trimmed |
| 5 | the store is fenced by the holder's epoch, cannot delete (a delete is a garbage record), and the reaper erases released objects and packs after a grace period | object names do not carry the writer's epoch: put-if-absent, the fence, and sealing an object under a nonce derived from its name and contents stand in for it, so a deposed holder's late upload of a block is the same bytes as its successor's, which takes it as its own; a cell keeps its objects in a local directory or an S3 bucket (`s3://bucket/prefix`), tested against MinIO; nodes write with the cell's credentials rather than per-epoch ones scoped to their prefix |
| 6 | a block flush writes one object, its block's pack, which holds the catalog and is the boot object; placed nodes flush within 15 minutes and on park; the directory's checkpoint names the last published block, and a cold start finds its latest block by probing forward from it, and reads that block's pack, which names every other, listing nothing | the boot object inlines the catalog and per-table block files but not the top trie levels, which are read from their own level-0 pack entries |
| 7 | compaction workers compact the deployments that rank them first and relay the result to the holder, which publishes it; holders on such a cell run no compactor | |
| 8 | parking after idleness, under `--max-active`, on drain, and waking on the next connection | resident-idle is not separate from active: a held idle deployment keeps its database open, which the measurement shows to be cheap |
| 9 | the gateway reads pgwire frames, survives a planned move or a holder's death while the session is idle, replays the startup and prepared statements, and resends a command a former holder refused as not held; once the first holder has authenticated the client, each reconnection carries a capability the gateway signs with the cell's secret, which a node accepts in place of the password for that deployment alone | the first connection's authentication is still the node's, relayed by the gateway |
| 10 | read replicas fed by the log for read-only sessions within a deployment's read width; opt-in linearizable reads confirmed with the logkeepers before each query ; a snapshot session (`rtdb.read=snapshot`) reads the deployment as of its checkpoint on any node, the holder last, opened from object storage with frozen logs and no logkeeper, and closed before the reaper's grace runs out, serving any basis at or below the checkpoint ; a deployment whose persisted tries outgrow a node's disk cache (by default; `AgentOptions::fragments` sets the threshold and depth, and no command-line flag does yet) splits each whole-table scan into 16 trie-prefix fragments, each read at the coordinator's basis by the node that ranks first for `{storage identity}/{table}/{prefix}` among the nodes that may take placements, which opens a log-fed replica for it if it has none, and answered over the control protocol as Arrow IPC; a fragment whose reader fails or is not at the basis is read by the coordinator itself | scan fragments are split by the scan on the coordinating node, not by a cell-wide planner, and the size test is the deployment's persisted trie bytes against the node's disk-cache size rather than `PerMemberCacheShare`; a fragment carries the projection, the evaluated temporal bounds and the predicates any node evaluates alike, but not those on parameters, outer rows, the clock or chance, which the coordinator alone applies, and comes back whole in one response rather than streamed, a slice too large for one response being read by the coordinator instead; a reader at or past the coordinator's basis serves a fragment as of the basis, except that what was erased since is gone; `_id` lookups and multi-partition databases are never split |
| 11 | the controller reaps expired members, confirms silent nodes dead by probing, and rebalances; planned moves; quarantine to the isolation pool; `status` as the autoscaling signal | autoscaling acts on the signal outside RTDB |
| 12 | per-deployment AES-256-GCM keys for objects (above the disk cache) and log payloads; forks by reference into the parent's lineage on fresh logs: a fork's `~base` manifest names where each object it shares physically lives, and whatever erases a shared object (the reaper, the parent's deletion) copies it into the forks still referencing it first; a cell move materializes every reference; deletion that seals, trims, destroys the key, erases and proves ; a fork can also be taken at a past block younger than the reaper's grace, referencing only what existed when that block was written | a fork is of the current state, of a past block, or of a time at or after the checkpoint, whose tail up to it is copied into the fork's stream, rather than of any basis with its tail in a pack; a time before the checkpoint must be forked as a past block, and a tail holding an oversized transaction is refused; a forked tail's transactions take new ids in the fork's log epoch; a referenced object is copied out before its owner erases it rather than kept in place, and the reaper and forks use a grace interval, and enforcing concurrent reference publication requires the spec's lineage fence |
| 13 | the cell map, gateway forwarding, creation in the least occupied cell, cell moves that carry the unflushed tail across; the old cell's reaper erases what a move left there once the grace has passed since the move | supply the old cell's key directory via `reaper --keys` to destroy that cell's copy of the key |
| 14 | the chaos test and the history audit; `crates/simulation` runs one deployment's single-writer protocol from a seed (members claiming through the directory, three logkeeper state machines, a modelled writer, crashes, partitions, broken connections) and checks the epoch and log invariants after every step; it also runs a cell's storage lifecycle from a seed (deployments writing through their real store stacks, releasing, forking now and at a past block, reaper passes, deletions and cell moves, over in-memory buckets on a simulated clock, with injected pack read failures and move cleanup), checking that each deployment reads exactly what it wrote or inherited and has not released and nothing its parent wrote after the fork point, and gateway sessions (the real session over loopback TCP to stub backends that die during startup, before or after executing, between replies, or after preparing a named statement), checking that every command runs at most once, exactly once if the client was told it succeeded, and that the client sees only its own command's replies | three simulations of parts of a cell rather than one of a whole cell: the storage run has no nodes, directory or logkeepers and its admin sequences are modelled around the real calls, the session backends are stubs, with simple commands and extended-protocol prepared reads; directory epoch changes, arbitrary provider failures and the zombie harness remain open |

A gateway started with `--config` uses the node YAML's `server.ssl` key store and requires PostgreSQL TLS from clients.
It terminates that frontend TLS connection before routing startup.
The node and cross-cell connections still use the cell's private network transport.

Hosting can install its existing deployment authority with `rtdb-multi install --id UUID --name NAME --org ORG`.
The directory allocates both name and authority conditionally, and an identical retry returns the same deployment.
This entry point enables linearizable reads on the holder.
A PostgreSQL startup database of `NAME@UUID`, with the UUID in lowercase 32-digit hexadecimal, requires that installation before connecting.
The gateway confirms the matched UUID in the startup parameter `rtdb_installation_id`.
`SHOW rtdb_hosted_source_barrier` returns the deployment ID after the holder confirms the current directory assignment, confirms its epoch with the logkeeper quorum, and applies the sampled source cut.
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
the retired writer epoch and hashes the receipt retained by the directory.

Nodes register a serving storage region in the deployment record before
opening objects. Holders and compactors consult the current directory record
at the provider boundary before issuing a mutation. After an attachment
closes, its directory admission is removed only after its region's provider
work has drained, including work whose caller was cancelled. Disposal,
cross-cell transfer and same-history recovery wait for those admissions to
clear after fencing. On a standalone local disk, each provider operation
retains a lock in the independent key directory. After a node exits, the
reaper can recover its recorded region only by acquiring that same
persisted lock. A missing lock witness keeps the region pending. For remote
storage, a producer that exits without acknowledging drain keeps cleanup
pending until the provider's outstanding work can be accounted for.
Each remote serving object is published with one conditional PUT of its
sealed bytes directly to its final key. A drained host region with a lost
write response keeps its directory admission. Successful writes and
definitive provider refusals permit release when its I/O has drained.

`rtdb-multi capture --id UUID --operation UUID` retains the selected
published checkpoint and copies its catalog, table metadata and referenced
tries as ciphertext to the independent archive. The receipt fixes the
location, length and digest of each object. Retrying the operation recovers
the same checkpoint and destination. The directory registers both and the
capture's storage region before the first archive write. An archive binds
its media witness before payload can be copied. The key vault stays outside
the capture.

The recovery owner calls `rtdb-multi schedule-recovery-disposal --id UUID`
after the installation's serving receipt exists and recovery holds end.
The reaper settles each registered local archive after confirming its
media witness. `rtdb-multi recovery-disposal --id UUID` returns the ordered
retirement and observed absence. Remote archives stay pending until the
provider proves that versions and unfinished uploads are absent.

`rtdb-multi restore` accepts the live directory's current checkpoint with
the original archive manifest and current key. It records a restore intent,
fences the old writer at the logkeepers, and verifies the objects. The
directory then admits the checkpoint archive as a read-only source for
missing objects. A successor recovers the same ordered tail before serving.
The directory keeps the accepted manifest and completion for retries.
Retain the archive while an installation uses it.

**Fixed on the way.**

- A newly activated fork could publish its parent's boundary and trim its own log before its first flush completed.
- A failed object-store write leaves the release visible and retryable, and a failed pack read defers the erasure pass for that store.
- A failed startup tells the client that authentication ended, and the reconnect window covers the replacement holder's greeting and replay.
- The gateway restores the named statements acknowledged by the holder when idle failover follows an extended-protocol exchange.

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
2. **Logkeeper loglet.**
   A client in `loglet` holding one connection per ensemble member per node, multiplexing every deployment the node holds, with write quorum 2 of 3 across zones.
3. **Ensemble assignment.**
   The directory records each loglet's ensemble; the controller balances writers across logkeepers.
4. **Deployment of the logkeepers.**
   The logkeeper is the first production loglet (decision 2, settled).
   Logkeepers run on EC2 instances with instance-store NVMe, at least one per availability zone per ensemble, with ensembles spread so that losing a host re-replicates its streams to the rest of the cell.
   The `Loglet` trait still admits other kinds (a multiplexed Kafka loglet, an object-store WAL for a cheaper tier); none is built on this branch.

**Tests.**
Logkeeper unit tests; the loglet contract suite from stage 3 run against a three-logkeeper ensemble; fault injection (a killed server, a partitioned server, a slow fsync, a torn segment) in the simulation crate; throughput and latency benchmarks against the spec's estimates (2–4 ms p50 commit, tens of thousands of appends per second per server).

**Specs.**
`rtdb-multi.allium`'s `LogService` contract, corrected by what the implementation teaches.

**Done when** the contract suite passes against a real ensemble under faults, and the benchmark meets the estimates or the spec's numbers are revised.

### Stage 5: the storage fence and manifests

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
3. **Flush triggers** in `HoldingDriver`: tail bytes, estimated replay, oldest-record age, idle, ensemble degradation, retention, rows (spec rules `FlushOn*`), replacing `flush_timeout` and `rows_per_block` for multi mode.
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
   Resident-idle drops the tail live index and keeps catalogs; parked releases the holder in one CAS after a final flush and `Parked` record.
2. LRU parking under memory pressure after `min_residency` (`ParkUnderPressure`).
3. Activation on demand from the gateway or the intake (`ActivateOnDemand`), admitted per node at `activations_per_node_per_second`.
4. Everything per database that today lives for the database's lifetime (tasks, timers, log tails, pools) is made lazy, guided by the stage 0 measurements.

**Tests.**
A node holding 100,000 resident-idle deployments stays within its memory target; a parked deployment has no task, timer, log-server state or subscription; reactivation latency from warm and cold caches.

**Specs.**
`live-index.allium`: the tail index is released on idleness.

**Done when** the resident-idle footprint meets the target measured in stage 0, or the target and the parking policy are revised.

### Stage 9: the gateway

**Why.**
Moves are invisible to clients only if the gateway owns connections and session state (spec surface `ClientAccess`).

**Changes.**

1. **Split session from execution.**
   `node/src/session.rs` and `node/src/statement.rs` are refactored so that parameters, prepared statements, the transaction buffer up to commit and the session basis are a value the node receives per request.
   This is a `refactor(node)` commit with classic behaviour unchanged.
2. **Crate `gateway`.**
   Terminates pgwire (reusing `pgwire/src/protocol.rs` and `wire.rs`) and Flight SQL, authenticates, mints capabilities and idempotency keys, and sends statements to nodes over an RPC carrying the capability, the session state and the basis.
3. **Crate `affinity`**, ported from Bijection's `crates/affinity` with its score vectors, and routing by it (`RouteRead`), with per-key in-flight bounds and pressure.
4. **Redirects**: `NotHolder`, `Moved`, `Behind`, followed by the gateway without the client noticing.
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
2. The snapshot tier: any member serves reads at a basis at or below the checkpoint from object storage and cache, with no log.
3. Linearizable reads on the holder wait for the next round's seal announcements; on a subscriber they fetch a read index from the holder.
4. Read width widened and narrowed by the controller from per-key overflow.
5. Scan fragments for large deployments routed by `(storage identity, table, trie prefix)`.

**Tests.**
Read-your-writes across tiers; a zombie holder cannot serve a linearizable read; subscriber bounce-to-writer on lag; snapshot reads with the log unavailable.

**Specs.**
`rtdb-multi.allium` read rules, corrected by implementation.

**Done when** a hot deployment's reads spread across its read width with bounded staleness.

### Stage 11: the controller

**Why.**
Placement must follow load and the fleet must scale both ways (spec surface `ControlPlane`).

**Changes.**

1. Crate `controller`: suspicion confirmation from corroborated gateway reports; admission of prewarmed members; drain (`DrainMember`); rebalancing on `LoadVector` watermarks (`Rebalance`); the drainer for orphaned tails (`DrainOrphanedTails`); quarantine (`CountActivationCrash`); engine rollouts (`RollEngine`).
2. Planned handoff (`IntendMove`, `HandOff`): subscription warm-up, hot-range prefetch from the source, admission pause, `Handoff` record, claim by move intent.
3. Autoscaling signals per pool, published as metrics through `metrics` and `server/src/metric_export`: nodes by the binding `LoadVector` dimension, logkeepers by ingest, workers by queue depth, gateways by connections.

**Tests.**
Simulation: scale-out moves only the keys that prefer the new member; scale-in retires members only when empty; a poisonous deployment is quarantined after `quarantine_crashes`; the balancer converges without oscillating.

**Done when** a simulated cell under Zipf load scales out and in with planned-handoff write stalls under 30 ms.

### Stage 12: encryption, lineages, forks and deletion

**Why.**
Deletion must be provable and forks must be cheap (spec rules `Fork`, `ForkAcrossOrganisations`, `BeginDeletion`, `TransferSharedObjects`, `EraseAndProve`).

**Changes.**

1. Per-deployment data keys from KMS with the deployment as encryption context; per-object data keys wrapped by the owner's key; log payloads encrypted.
2. `Lineage` records with a version bumped by every fork and deletion.
3. Fork: a new record in the lineage with a fork checkpoint referencing the parent's objects plus one pack of the parent's tail to the fork basis; inherited reads capped at the fork basis; cross-organisation forks copied and re-encrypted.
4. Deletion: fence by epoch bump, seal and drop the stream, wait `credential_ttl`, re-wrap shared objects to a surviving referrer, delete all versions, prove the prefix empty twice, destroy the key, sign the deletion record.

**Tests.**
A fork costs constant time regardless of size; a deleted parent's children still read their inherited data; a deleted deployment's objects are unreadable from the moment its key is destroyed.

**Done when** deletion produces a signed record and the scrubber finds nothing left under the prefix.

### Stage 13: cells

**Why.**
Unbounded means adding cells (spec contract `CellMap`).

**Changes.**

1. The cell map, encoded in deployment hostnames where possible.
2. Cell move: fork into the target cell, fence, copy the remaining tail, flip the map, leave a forwarding tombstone (`BeginCellMove`, `MoveCutover`).
3. Format upgrades: readers for N-2 to N, rewrite on flush and compaction, a maintenance trickle for the rest.

**Done when** a deployment moves between two simulated cells with one fenced write stall.

### Stage 14: hardening at scale

Running alongside every stage from stage 2 onwards, and concluding the branch:

1. The `simulation` crate drives a whole cell deterministically, with simulated network, clocks, object store and disk faults.
2. A zombie harness freezes a holder mid-upload, mid-append and mid-flush and resumes it after its successor has taken over.
3. A hash-chain auditor verifies, offline, that every deployment's history is one chain across epochs.
4. A staging cell holds ten million deployments (300 bytes each when parked) under Zipf activity.

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
