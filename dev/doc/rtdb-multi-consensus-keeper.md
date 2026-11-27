# RTDB Multi on keeper-hosted consensus streams

RTDB Multi can run on Logkeeper's keeper-hosted consensus streams. In that
mode the keepers elect a leader per stream with Raft and admit replication
load per sender. They serve fresh heads through a quorum read index. A
restarted leader must win a new term before it serves reads. The stream
Writer and the installed ordered journals stay in service, and the default
Kafka and local modes are unchanged.

## Decision

Multi consumes the consensus keeper through the existing `journal-contract`
boundary that `allium/rtdb-multi.allium` names for the stream driver. RTDB
adds no new journal port. Logkeeper implements the contract as
`stream::consensus::ConsensusJournal`, and `rtdb-journal-logkeeper` exposes
it next to the stream Writer. The server selects it by journal URL.

- `logkeeper-consensus:///absolute/path/journal.json` selects consensus
  streams. The JSON file is Logkeeper's `JournalConfig`, with the keeper
  application addresses, the catalog keeper, either fixed `voters` or a
  `placement` of eligible keepers with their failure domains and an optional
  `draining` list, the quota for each created stream, the record limit and
  the operation deadline.
- `logkeeper+tls://...` continues to select the stream Writer.
- The node's client credentials are the same mutual-TLS files in both cases.
  Consensus keepers authorize principals under `urn:logkeeper:client:`.

The contract maps onto consensus streams as follows.

| Contract | Consensus keeper |
| --- | --- |
| `create(stream)` | Catalog claim of a genesis on the configured voters and quota, then voter bootstrap |
| `seal(stream, epoch)` | Compare-and-set seals from the current epoch, one step per epoch, with a fixed writer UUID. The keeper checks the authenticated principal, so a same-principal reseal succeeds after a restart |
| `append(stream, epoch, at, payload)` | A committed `Append` under the fence for `epoch`. The journal refuses a record above its configured limit before sending it |
| `head(stream)` | The leader's fresh quorum-confirmed head after local application |
| `read(stream, from, max, wait)` | One keeper's applied page, which may lag `head` |
| `trim`, `delete` | Committed `Trim` under the fence, and `Delete` |
| `acknowledge_recovery(stream)` | Replaces the fence that `keeper recover-stream` left with an ownerless one, so the next seal succeeds |

Replication admission, the restart read rule and the read index sit inside
the keepers, so the Multi driver needs no change to use them. Epoch zero is
never a sealed writer epoch, and Multi always seals epoch one or later.

## Immutable stream history

Set `multiJournal: {immutableStreams: true}` for a Multi cell using a Logkeeper consensus cluster with cluster version 4 finalized.
The loopback development equivalent is `rtdb dev --multi --multi-immutable-streams` with a complete Multi TLS profile and the consensus journal URL.
This setting applies to newly created stream IDs.
A residence verifies the durable stream mode before opening the history and presents a fresh session identity when seeking the writer epoch.
Seal binds that session to the exact committed head, and subsequent appends and trims carry its fence.
Followers request pages that cover their observed head and resume at the next LSN.
A trim of an immutable Multi stream checks the exact retained checkpoint announcement in a journal view that covers the writer's observed head.
The trim removes records through the checkpoint's cut and retains its announcement for replay.
A history change under the same ID faults the residence.
The keeper manages replica replacement for immutable streams.
To adopt this mode for existing databases, restore a verified application cut into a fresh database incarnation and stream before changing the database route.

## Operator recovery

For a legacy mutable stream that loses two of its three voters, an operator can run `logkeeper keeper recover-stream --acknowledge-data-loss` on the survivor.
The stream then ends at the survivor's head, and records acknowledged after it may be lost.
Every seal returns `JournalError::Recovered { head }` until a writer or an administrator acknowledges the recovery.

- **Resuming.** The Multi node that would take the writer role logs a `rtdb/journal-recovered` error event, counts it in `rtdb_db::journal_recoveries_acknowledged`, acknowledges the recovery and seals above the recovery's fence.
  It replays the recovered journal from its floor, writes its lease and then a `Recovered { head, epoch, time }` journal record, before any transaction.
  When several nodes acknowledge at once, the epoch fence leaves one writer, and a node that finds the journal already records the recovery's head does not record it again.
- **Why the journal.** The record is ordered with the history it describes and reaches every replica and reader through replay.
  It stays until a checkpoint trims past it; checkpoint images do not carry it, and the error event and counter remain the operator's record after that.
  RTDB's own object-store metadata has no order relative to the journal, and a reader would have to consult it on every catch-up to learn that its state is stale.
- **Readers and followers.** A node whose state is ahead of the recovered head reports `rtdb/journal-shortened`.
  A node whose last applied record the journal no longer holds, or that meets a recovery record after records it applied, reports `rtdb/journal-rewritten`.
  Each check re-reads the last applied record with the next page, so it holds after the new writer has written past the lost positions.
  Either way the node logs the loss and rebuilds its state from the journal floor.
- **Holding writes.** With `multiJournal: {holdWritesAfterRecovery: true}` the node instead refuses the writer role with `rtdb/journal-recovered` until someone else acknowledges the recovery, and records nothing.

## Independent database activation

A Multi runtime can bind a stable name and its exact database incarnation to a separately commissioned immutable activation journal.
A serving grant is derived from a fresh quorum-confirmed read of the active binding.
Renewal uses a reader identity and leaves the activation journal unchanged.
The holder measures its grant from before issuing that read, so a delayed reply cannot prolong its authority.
A successor publication compares the original activation revision, requires closed grant issuance, and waits a full conservative grant window after reading the committed freeze.
A restarted publication observes the freeze and waits again.
It rejects a retired incarnation as a successor.
The protocol allows at most five seconds in real time per grant and assumes that its suspend-aware monotonic clocks stay within 1,000 parts per million of real time.
Holders expire early and publication waits late under that rate allowance.
Linux uses `CLOCK_BOOTTIME` and macOS uses `CLOCK_MONOTONIC_RAW`.

Every member serving an activated database name must enforce the same authority.
Commission a fresh application stream before accepting client traffic.
Its encrypted journal envelopes bind the authority ID and database name from the first record onward.
A node with missing or different commissioning refuses the history, including after trim or checkpoint restore.
Keep the authority in a failure domain separate from both the predecessor and the successor.
The runtime rejects authority and application catalogs with a shared keeper identity.
Enroll the runtime's TLS identity only in the activation keeper cluster's `readers` list.
Use a different TLS identity with write access for the operator commands that change bindings.
The operator process owns the activation journal's writer credential and the runtime exposes reader operations through its activation handle.
The runtime gates client operations, peer writes, journal mutations, and retained result delivery on the incarnation's grant.
Transactions with an external custody or erasure effect, funded dispatch, and the existing local catalog's create, erase and checkpoint-transfer operations return an explicit refusal in activation mode.
Those capabilities require separately commissioned effect fences before they can serve an activated database name.

On installed Multi nodes, `RTDB_ACTIVATION_JOURNAL_URL` selects the independent immutable consensus authority and `RTDB_ACTIVATION_AUTHORITY_ID` identifies it.
Installed activation requires `multiJournal: {immutableStreams: true}` for the application journal.
The authority reader TLS files come from `RTDB_ACTIVATION_READER_CERTIFICATE_PATH`, `RTDB_ACTIVATION_READER_PRIVATE_KEY_PATH`, and `RTDB_ACTIVATION_CA_PATH`.
All replicas of the authority protect its state against rewind and survive failure of the databases they activate.

`rtdb multi-activation` supplies `bootstrap`, `status`, `upgrade-grants`, `candidate`, `freeze`, and `publish` operator actions.
`upgrade-grants` moves an existing binding to elapsed-time grants at the expected incarnation and revision.
After upgrade, older binaries reject that authority format.
Publication waits an additional allowance for any outstanding legacy wall-clock grants.
`freeze` and `publish` compare the expected incarnation and activation revision.
`candidate` derives the destination incarnation from the active name, its next revision, and the recovery operation ID.
Restore into that incarnation with `multi-backup restore --immutable-streams --authority-id UUID`.
`publish` independently opens the stopped destination, verifies its commissioned scope and committed portable restore, and binds the successor to that proof.
Use the same recovery operation ID for candidate selection and publication.
If publication is interrupted, retry with the same expected binding and operation ID.
A retry of the retained committed publication returns its binding even after the destination has started serving.
A local rehearsal can use an exclusively opened authority file in the same process as its participating runtimes.
Separate development processes can use a loopback immutable consensus authority through the installed activation environment variables.

On Unix, run the activation broker as a dedicated service account that alone can read the authority writer key.
Create its socket directory with that account as owner, the operator group as group, and mode `2750`.
Store the broker configuration in a regular file owned by the service account with mode `0600`.
The broker also checks the kernel-reported peer UID against `operator_uids`.

```json
{
  "authority_id": "dd76afcd-7722-4d6f-8692-dca52bf431d0",
  "authority_url": "logkeeper-consensus:///etc/rtdb/activation-journal.json",
  "authority_tls_dir": "/etc/rtdb/activation-writer",
  "operator_uids": [1100],
  "destination": {
    "data": "/var/lib/rtdb/staged",
    "journal_url": "logkeeper-consensus:///etc/rtdb/destination-journal.json",
    "journal_tls_dir": "/etc/rtdb/destination-journal"
  }
}
```

```sh
rtdb multi-activation serve --authority-socket /run/rtdb-activation/authority.sock --broker-config /etc/rtdb/activation-broker.json
rtdb multi-activation status xtdb --authority-socket /run/rtdb-activation/authority.sock
```

The broker accepts typed commands and obtains the installation proof from its configured stopped development destination.
An operator using `--authority-socket` supplies the expected binding and recovery operation ID for publication.
The broker configuration may omit `destination` when it serves status and lifecycle operations only.
The service account owns the staged data directory and both TLS directories. Operator accounts cannot modify those directories or their contents.
Serve the Unix socket on the authority host. The current installation verifier expects a locally mounted stopped Multi development destination and loopback keeper addresses.

## Batches, placement, funded appends and journal size

- **Batches.** The consensus `JournalBatchAppender` sends a Multi group as
  keeper batches of up to 256 records and 4 MiB. The keeper checks a whole
  batch before applying any record and commits it as one Raft entry. A
  retained prefix must match exactly, and an error can leave a committed
  prefix of whole batches, which Multi already recovers. Appends retry a
  transient `Unavailable` refusal with backoff until the deadline.
- **Placement.** With a `placement`, each created stream gets three eligible
  keepers in distinct failure domains, chosen by rendezvous ranking of the
  stream and keeper identities, so every node derives the same voters. Every
  5 seconds the Multi writer of each database re-reads the configuration
  file and, when one of its stream's voters is draining, ineligible or shares
  a domain, replaces that voter through Logkeeper's `replace`. Every other
  voter stays. A keeper that is down but still eligible is never replaced,
  because the quorum tolerates it. Operators drain a keeper by listing it in
  `draining` and stop it once no stream names it.
- **Funded appends.** With `RTDB_TIMED_ORIGINAL_TRUST_FILE`, the same keeper
  trust registry the stream Writer reads, consensus journals append and
  prove timed originals. The writer stages the exact payload on the first
  two voters of the stream's sorted roster. Each voter syncs it, samples its
  commissioned clock and signs the bound. The record then commits with both
  samples as evidence. Keepers need `timed_original` in their configuration
  in the stream keeper's mount format. Without trust,
  `maximum_timed_original_bytes` is `None` and funded appends stay
  unsupported, as before.
- **Journal size.** A consensus stream refuses appends at 262,144 retained records.
  The Multi writer checkpoints and trims once its retained journal suffix reaches 131,072 records, half that bound, so a checkpoint held back by a pending erasure has 131,072 records of headroom.
  The stream's byte quota must also hold that many records.
  Each residence keeps its retained suffix decoded in memory and replays it on open, so both grow with the bound.
  Before the writer checkpointed on its own, the server only checkpointed for erasure, so a long-running writer would have stalled on consensus streams.
  The stream Writer is limited only by its byte quota.
- **Checkpoints off the commit path.** By default that checkpoint runs under
  the database lock, so commits wait for it. With
  `multiCheckpoints: {deferred: true}` the writer captures the state under the
  lock, builds and uploads the manifest without it, then announces a
  checkpoint whose cut precedes the announcement and trims only through the
  cut. Replay installs the first retained checkpoint whose cut is the record
  before the floor and applies every retained record from the floor.
  `multiCheckpoints: {binary: true}` writes the checkpoint's transaction state
  in CBOR instead of JSON. Readers of this release install both kinds, and
  JSON checkpoints keep replaying. An older reader refuses a deferred floor
  and a binary checkpoint, so enable both only once every node of a
  deployment runs this release. The variables
  `RTDB_MULTI_DEFERRED_CHECKPOINTS` and `RTDB_MULTI_BINARY_CHECKPOINTS` still
  set a mode the configuration leaves out, with a deprecation warning. The
  phase timings of each checkpoint are a debug event, logged with
  `XTDB_LOGGING_LEVEL_DB_CHECKPOINT=debug`. The benches take
  `--checkpoints classic|binary|deferred`, default `deferred`.

## Portable application backups

`rtdb multi-backup export` captures a settled cut from a stopped, initialized development deployment, using its journal and object store to verify the current checkpoint dependencies.
An immutable export replays a quorum-confirmed head without acquiring the writer lease.
Legacy exports and destination installation acquire the writer lease.
For an ordinary cut, the archive carries the complete checkpoint, retained receipt index, and verified checkpoint files under the source key generation.
Its authenticated header binds the source incarnation, basis, system time, and journal position.
The archive contains a wrapped data key. The source generation key remains in its vault.

`rtdb multi-backup restore` reads the archive with an explicitly supplied source key vault and installs the cut into a fresh development incarnation and journal stream.
The source journal, object store, and process can be absent.
Keep source key custody available independently until a restore is admitted; retired key generations follow their existing deadlines.
The destination checkpoint records the source coordinates and archive digest so the committed installation can be read back after restart.
Each captured file is sealed under a fresh destination object identity.
The committed destination checkpoint retains the source-to-destination file index and the new file identities through subsequent checkpoints.
Opening the installation proof verifies the index and each relocated file using the destination's custody.
Recovered ordinary receipts use the new incarnation's basis, and the archive retains the original receipts.
The commands work with the local development journal and the optional loopback Logkeeper connections configured with `--journal-url` and `--tls-dir`.
For a version 4 immutable stream, pass `--immutable-streams` to export, restore, and activation publication.
Pass the commissioning `--authority-id UUID` when exporting a commissioned source or staging a commissioned destination.
A history created before durable commissioning can become an activated name by restoring a cut into a fresh commissioned incarnation.
Withdraw access to the uncommissioned predecessor's endpoints before publishing the first commissioned successor.

Existing inline archives remain readable.
Archives with files cover ordinary checkpoint attachments within the archive size limit and the current source key generation.
Active pins, files retained only through pin history, transaction object inventories, ranked catalogues, protected custody, and scoped or funded receipts require a separate portable transfer.
The export refuses incomplete erasure and key rotation.
The command stages a destination cut for local rehearsal; a shared database name requires its own activation decision and predecessor fence.

## Qualification

The [2026-11-26 consensus keeper results](../../verification/results/2026-11-26-consensus-keeper.adoc)
record the runs. To repeat them:

```sh
cd rust
# The Multi driver tests on real keepers in one process.
RTDB_MULTI_JOURNAL=consensus RUST_MIN_STACK=16777216 cargo test -p rtdb-db --lib multi::tests
# The contract suite through the RTDB adapter.
cargo test -p rtdb-journal-logkeeper
# The development runtime across a full keeper restart, a drained keeper,
# and funded proofs through the RTDB adapter.
cargo test -p rtdb-server --features multi --lib multi_dev_serves_and_reopens_on_consensus_keepers
cargo test -p rtdb-server --features multi --lib multi_dev_moves_its_stream_off_a_draining_keeper
cargo test -p rtdb-server --features multi --lib consensus_batch_proves_funded_timed_originals
# Restore an immutable cut with three independent keeper clusters.
cargo test -p rtdb-server --features multi --lib portable_cut_replaces_a_lost_cell_and_fences_its_return
# Separate keeper processes, rtdb dev --multi and rtdb.v1 commits over Flight.
python3 bench/v1_consensus_keepers.py --rtdb target/release/rtdb \
  --logkeeper /path/to/logkeeper output-directory
# The same commits on the local FileJournal, the stream Writer and consensus.
python3 bench/v1_journal_comparison.py --rtdb target/release/rtdb \
  --logkeeper /path/to/logkeeper comparison-directory
```

The process run needs OpenSSL, PyArrow and a release `logkeeper` built with
the `stream` feature at the pinned revision.

To qualify a local Logkeeper change before the Git dependency is published, pass a Cargo override containing both workspace crates:

```toml
[patch."https://github.com/bijectionhq/logkeeper.git"]
logkeeper = { path = "/path/to/logkeeper" }
journal-contract = { path = "/path/to/logkeeper/contract" }
```

Use `cargo --config /path/to/local-logkeeper.toml test ...` and keep the lockfile changes local to the qualification.
