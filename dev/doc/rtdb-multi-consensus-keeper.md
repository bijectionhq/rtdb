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

Replication admission, the restart read rule and the read index sit inside
the keepers, so the Multi driver needs no change to use them. Epoch zero is
never a sealed writer epoch, and Multi always seals epoch one or later.

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
- **Journal size.** A consensus stream refuses appends at 65,536 retained
  records. The Multi writer checkpoints and trims once its retained journal
  suffix reaches 32,768 records. Before this, the server only checkpointed
  for erasure, so a long-running writer would have stalled on consensus
  streams. The stream Writer is limited only by its byte quota.

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
# Separate keeper processes, rtdb dev --multi and rtdb.v1 commits over Flight.
python3 bench/v1_consensus_keepers.py --rtdb target/release/rtdb \
  --logkeeper /path/to/logkeeper output-directory
# The same commits on the local FileJournal, the stream Writer and consensus.
python3 bench/v1_journal_comparison.py --rtdb target/release/rtdb \
  --logkeeper /path/to/logkeeper comparison-directory
```

The process run needs OpenSSL, PyArrow and a release `logkeeper` built with
the `stream` feature at the pinned revision.
