---
title: Log
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: earlier epochs rejected at startup

: A node whose log is at an *earlier* epoch than its indexed storage now refuses to start, pointing at [Out of Sync Log & Intact Storage](/ops/backup-and-restore/out-of-sync-log).
  See [Epochs](#epochs).

  Previously any epoch difference — in either direction — was treated as the start of a new epoch, logged at INFO, and offset validation was skipped.
  That is only safe going forwards.
  Because a message id packs the epoch above the offset, every id on an earlier-epoch log sorts below the storage watermark, so the log replayed as entirely stale and transactions submitted afterwards were acknowledged and then discarded without ever committing or aborting.

  To upgrade: nothing to do, unless a node is already running against an earlier epoch than its storage — in which case it will now fail to start, and the recovery is to restore the storage backup that matches the log, or to move forwards to a higher epoch.

v2.2: source/replica log split

: Each database now uses two logs under the hood — a source log (client writes) and a replica log (the indexing leader's output) — to support [single-writer indexing](/about/dbs-in-xtdb#database-architecture).
  See the [Kafka log documentation](log/kafka) for configuration details.
  The in-memory and local-disk log implementations are single-node only and unaffected.

v2.1: multi-database support

: As part of the multi-database support, the Kafka log-clusters were extracted - see the [Kafka log documentation](log/kafka) for more details.

</details>

One of the key components of an XTDB node is the log - this is a totally ordered log of all operations that have been applied to the database, generally persistent & shared between nodes.

## Implementations

We offer a number of separate implementations of the log, currently:

- Single-node log implementations, within `xtdb-core`:
    - [In memory](#in-memory): transient in-memory log.
    - [Local disk](#local-disk): log using the local filesystem.
- [Remote](#remote): multi-node log implementations using a remote service.

## In memory

By default, the log is a transient, in-memory log:

``` yaml
## default, no need to explicitly specify

## log: !InMemory
```

``` yaml
log: !InMemory
  # -- optional

  # (v2.2.1+) The number of most recent records each of the database's logs holds in memory.
  # Log tables and catching up after a leadership change can only read back this far.
  replayBufferSize: 4096 # default
```

If configured as an in-process node, you can also specify an [InstantSource](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/InstantSource.html) implementation - this is used to override the local machine's clock when providing a system-time timestamp for each message.

## Local disk

A single-node persistent log implementation that writes to a local directory.

``` yaml
log: !Local
  # -- required

  # The path to the local directory to store the log in.
  # (Can be set as an !Env value)
  path: /var/lib/xtdb/log
```

If configured as an in-process node, you can also specify an [InstantSource](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/InstantSource.html) implementation - this is used to override the local machine's clock when providing a system-time timestamp for each transaction.

## Remote

A multi-node persistent log implementation that uses a remote service to store the log.

We currently offer the following remote log implementations, available in their own modules:

- [Kafka](log/kafka): a log implementation that uses a Apache Kafka topic to store the log.

## Epochs

An **epoch** is a manually assigned, monotonically increasing integer used to identify the generation of the log in XTDB:

- Epochs allow a cluster to safely reset its log state following partial log loss, corruption, or intentional recovery operations, without requiring full reindexing of storage data.
- If not explicitly configured, nodes assume `epoch = 0`.

### Configuration

To configure an epoch, specify the `epoch` field inside the node's log configuration:

``` yaml
log: !<LogType>
  epoch: <new-epoch>
```

Where:

- `<LogType>` is the chosen log implementation (e.g., `!Kafka`, `!Local`).
- `<new-epoch>` is a positive integer no greater than 32,767 and strictly greater than the previous epoch.

All nodes within the same cluster **must** use an identical epoch value at startup.

Epochs only move forwards.
A node started with an epoch *below* the one its storage has already indexed refuses to start, and points at [Out of Sync Log & Intact Storage](/ops/backup-and-restore/out-of-sync-log) — there is no way to reconcile the two, because everything on the earlier log ranks below what storage has already processed.

#### Bumping an Epoch

:::caution
Beginning a new epoch will make the recovery of unindexed transactions from any previous epoch nearly impossible.
Therefore, it is recommended to only increment the epoch after you are confident that the original log is irrecoverable and you understand the potential consequences.

When applying a new epoch:

- Shut down all XTDB nodes to prevent divergence.
- Update each node's configuration with the new `epoch` value.
- (Optional) Prepare a clean log backend if required (e.g., create a new Kafka topic or clear the local log directory).
- Restart all nodes simultaneously with the updated configuration.

Once restarted, nodes will begin writing to the new log generation, and prior log history will be disregarded.

:::

## Reading an RTDB log with an XTDB node

RTDB writes the same log and storage formats as XTDB, so an XTDB node can follow a log and store that RTDB wrote — until RTDB uses a feature XTDB does not implement.
From then on, the XTDB node stops with an error at the first such record, rather than skipping it or applying it without the feature.

These RTDB features mark the records that use them:

Native `rtdb.v1` transactions

: Every source-log record of a native transaction.

Retry guards

: Transactions submitted with an `idempotency_key`, `rtdb_retry_v1` bounded retry or a private replay permit.

Owned-relation retirement

: The replica-log record that retires a table.

Online compactor reset, and recovery from a primary

: Every log record that names a data file of the new naming epoch (keys ending in `-e<epoch>`), and every block's upload record from then on, so that a node opening the store at a later block also stops.

Receipt-table cleanup in Multi mode

: The replica-log record that deletes a receipt table's L0 files, which XTDB never deletes.

A workload that uses none of these keeps writing exactly the records XTDB writes, and stays readable by XTDB.
Search, vector and entity-history artefact records, and the extra fields RTDB adds to ordinary transactions, are not marked: an XTDB node ignores them without changing any result it returns.

What the XTDB node reports:

Source log

: An error naming `Unknown message type: 4` — RTDB frames these records with header byte 4, which XTDB does not recognise.

Replica log

: `can't resolve oversized message at <id>: written at storage v1381254210/e0, …` — the record ends with a reference to storage version 1381254210 (`0x52544442`, ASCII `RTDB`), which no XTDB release writes.

RTDB never rewrites these records: once a log carries one, an XTDB node stops there every time it replays past it.
