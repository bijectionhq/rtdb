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
On a cluster with atomic checkpoint installation finalized, Multi installs an authenticated checkpoint and advances the logical floor through its cut in one keeper decision.
The decision retains the checkpoint position for replay and preserves every record after its cut, including writes made while a deferred checkpoint was built.
A key rotation can stage a clean checkpoint and select it once the provider has armed retirement. Multi verifies the exact committed announcement and the keeper checks its position, cut and digest under the current writer fence.
Older clusters use the same verified selection after appending the announcement. A cold reader uses the keeper's selected checkpoint when available, including when an earlier retained announcement covers the same cut.
A history change under the same ID faults the residence.
The keeper manages replica replacement for immutable streams.
To adopt this mode for existing databases, restore a verified application cut into a fresh database incarnation and stream before changing the database route.

## Operator recovery

For a legacy mutable stream that loses two of its three voters, an operator can run `logkeeper keeper recover-stream --acknowledge-data-loss` on the survivor.
The stream then ends at the survivor's head, and records acknowledged after it may be lost.
Every seal returns `JournalError::Recovered { head }` until a writer or an administrator acknowledges the recovery.

- **Resuming.** A Multi writer candidate that encounters the recovery logs a `rtdb/journal-recovered` error event and attempts to acknowledge it.
  It acknowledges with the journal contract's atomic recovery marker: the candidate replays the recovered journal from its floor, and one keeper decision appends a `Recovered { head, epoch, time }` record right after the recovered head, under the recovery's fence epoch, and lifts exactly that fence.
  The marker is the only record of a recovery; the writer never appends one of its own.
  At Logkeeper cluster level 10 the keeper writes its typed recovery entry (`journal_contract::RecoveryEntry`) at that position instead, with the writer's `Recovered` record inside it as the consumer's marker. The entry also carries the catalog's loss point (and whether it is exact), the source (survivor or object store) and the configuration epoch. Replay, followers and checkpoints read it from `Page::recoveries` (`Stream::decode_recovery`), and `JournalRecovery` reports those fields. An administrator's acknowledgement writes the entry without a marker, so the journal still records the recovery. No client append can produce the entry's kind.
  The accepting candidate's process counts the acknowledgement as soon as the journal confirms its marker, then seals above the fence.
  When several nodes acknowledge at once, the first marker lifts the fence and the others are refused with `Conflict` and compete for the writer role as usual, so the recovery is recorded once, by whichever node acknowledged it.
  Consensus keepers accept markers once the cluster version is finalized; a journal without them is only acknowledged, and the error event, the fault counter and the followers' rebuilds are what remain.
- **Why the journal.** The record is ordered with the history it describes and reaches every replica and reader through replay.
  A checkpoint that trims past it carries the recovery's epoch (`recovery_epoch`), and a follower that has not seen that recovery rebuilds instead of resuming past the checkpoint.
  RTDB's own object-store metadata has no order relative to the journal, and a reader would have to consult it on every catch-up to learn that its state is stale.
- **Readers and followers.** A node whose state is ahead of the recovered head reports `rtdb/journal-shortened`.
  A node whose last applied record the journal no longer holds, or that meets a recovery record after records it applied, reports `rtdb/journal-rewritten`.
  Each check re-reads the last applied record with the next page, so it holds after the new writer has written past the lost positions.
  Either way the node logs the loss and rebuilds its state from the journal floor.
- **Holding writes.** With `multiJournal: {holdWritesAfterRecovery: true}` the node instead refuses the writer role with `rtdb/journal-recovered` until someone else acknowledges the recovery, and records nothing.
- **Metrics.** The Multi server's registry exports both process-wide counts on `/metrics`: `rtdb_multi_journal_recoveries_acknowledged_total`, and `rtdb_multi_journal_recovery_faults_total` by `code` (`rtdb/journal-recovered`, `rtdb/journal-shortened`, `rtdb/journal-rewritten`).
  The acknowledgement metric covers atomic markers because the legacy acknowledgement returns the head even if another caller already cleared the fence.
  The fault metric counts reported anomalies, including held writes and readers that found a shortened or rewritten history.
- **Tested on real keepers.** `multi_resumes_after_a_real_keeper_recovery_and_followers_rebuild` runs on Logkeeper's `LocalCluster::start_spared`: five keepers in three domains and the catalog keeper.
  Voter 1 stops after the first commit, the other two voters acknowledge two more and are retired, and `recover-stream` runs on voter 1.
  The next writer acknowledges and records the recovery, and the old writer, both followers and a new reader all hold the first commit and the new ones, never the lost ones.

## Recovery key custody

The file key provider can hold an existing destination generation for an exact activation authority, recovery operation, and installation digest.
Holding, releasing, and retiring a generation serialize in its original vault.
Custody survives reopening the vault, and exact release retries recover the original result even after the key is destroyed.
The provider accepts holds before retirement is admitted, preserving every existing erasure deadline.
For a stopped local destination, pass the activation `--authority-url` and `--authority-tls-dir` to `attest` to retain the destination generation before emitting the proof.
The verifier requires a pending durable recovery and uses activation reader credentials.
The broker records the retained generation with the verified installation and advances that name to activation protocol 5.
Upgrade its readers and brokers before recording this proof.
Run `release-recovery-key` against the original vault with the installation proof and the same consensus configuration and CA after publication or supersession.
The provider can settle the hold after the verifier has exited.
Hosted journal and object custody still require their own provider evidence.

## Recovery journal custody

For a stopped destination on Logkeeper cluster level 11, give `attest` the destination custodian's `--journal-custodian-tls-dir` in addition to the activation reader credentials.
The verifier reads the selected checkpoint, checks the staged installation, and asks the keeper quorum to retain the exact announcement before emitting the proof.
The journal attestation authenticates that hold together with the installation's key evidence.
Configure the broker's `destination_journal_readers` map by cell UUID, with `journal_url` and `journal_tls_dir` for each original destination.
The broker reads the committed hold with that cell's journal reader before recording the installation.
Recording journal custody advances the name to activation protocol 6. Subsequent recoveries for that name require journal custody evidence.
Upgrade participating readers and brokers before recording the proof.
After publication or supersession, use `release-recovery-journal` with the original proof, activation reader credentials, and the destination custodian credential.
For a published hosted recovery, the proof must match the journal, generation and object custody in the retained publication.
The key custodian uses its separate `release-recovery-key` action.

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
Funded dispatch checks this fence before contacting the receiver clock, even when the application journal supports timed originals.
Ranked catalogue maintenance skips staging and publication for a commissioned incarnation until derivative custody is commissioned.

On installed Multi nodes, `RTDB_ACTIVATION_JOURNAL_URL` selects the independent immutable consensus authority and `RTDB_ACTIVATION_AUTHORITY_ID` identifies it.
Installed activation requires `multiJournal: {immutableStreams: true}` for the application journal.
The authority reader TLS files come from `RTDB_ACTIVATION_READER_CERTIFICATE_PATH`, `RTDB_ACTIVATION_READER_PRIVATE_KEY_PATH`, and `RTDB_ACTIVATION_CA_PATH`.
All replicas of the authority protect its state against rewind and survive failure of the databases they activate.

`rtdb multi-activation` supplies `bootstrap`, `status`, `upgrade-grants`, `candidate`, `freeze`, `attest`, and `publish` operator actions.
`upgrade-grants` moves an existing binding to elapsed-time grants at the expected incarnation and revision.
After upgrade, older binaries reject that authority format.
Publication waits an additional allowance for any outstanding legacy wall-clock grants.
`freeze` and `publish` compare the expected incarnation and activation revision.
`candidate` derives the destination incarnation from the active name, its next revision, and the recovery operation ID.
Restore into that incarnation with `multi-backup restore --immutable-streams --authority-id UUID`.
For same-host recovery, `publish` verifies the commissioned scope and committed portable restore in the configured stopped destination.
For cross-cell recovery, it verifies a signed installation attestation and records the destination cell with the successor.
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

The socket accepts typed commands. An operator supplies the expected binding and recovery operation ID.
For same-host verification, the broker account owns the stopped destination's data and TLS directories. The local verifier supports a stopped Multi development destination and loopback keeper addresses.

### Ranked catalogue admission

Configure `ranked_providers` in the broker configuration as a map from operator-selected names to S3 settings. Each allocation requires an explicit region. Keep the original bucket, endpoint and prefix registered while its recovery outcomes are needed. Credentials may rotate without changing the allocation identity.
Ranked commissioning requires a build with `backup-object-holds` and a versioned S3 allocation.

Commission a provider against the active binding before enabling the ranked writer for that incarnation.

```sh
rtdb multi-activation commission-ranked xtdb --authority-socket "$SOCKET" --provider "$PROVIDER" \
  --expected-incarnation "$PREDECESSOR" --expected-revision "$REVISION"
```

Reopen the native S3-backed writer after commissioning so it loads the binding.
Its serving object allocation must match the commissioned provider.
The writer records the exact catalogue at that provider before publishing the catalogue to its journal.
If a retry uses the same cut and writer epoch, it continues with the manifest already prepared at the provider.
A replacement writer can rebuild the cut under its own epoch, and the provider retains each admitted outcome.

For a commissioned ranked scope, `freeze-recovery` first closes activation grants and then closes admission at the original provider. Publication requires the resulting closure receipt. If the provider is unavailable, restore access and retry freeze with the same binding and operation ID. Replacing a pending recovery operation for the same predecessor preserves its closure. Commission the successor separately if it needs ranked publication.

The original provider closes ranked admission before checking the committed catalogue sequence against its inventory digest.
A missing or different committed entry leaves admission closed and prevents the closure receipt from reaching activation.
Restore the original committed entries and retry freeze for the same recovery.
Preserve committed entries against replacement through outcome discovery.

The commissioned ranked writer stages files and manifests at the original versioned allocation.
The writer records the exact multipart upload in the provider ledger before sending bytes.
The ledger permits one recorded upload per ranked scope.
A newer writer epoch drains an interrupted upload through the original provider before reserving its own upload.
Closure cancels any recorded upload and waits for the original provider to confirm that its parts can no longer be addressed.
Give the writer permission to verify bucket versioning.
Give both the writer and original ranked custodian permission to abort scoped multipart uploads and inspect their parts at the commissioned allocation.
Give both identities permission to read the staged object's metadata and list versions at its exact key so they can resolve an interrupted completion.
On AWS, grant `s3:GetObject` and `s3:ListBucket` so `HeadObject` can report a missing key, and grant `s3:ListBucketVersions` for the exact derivative key prefix. [HeadObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html) · [ListObjectVersions permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
Preserve the staged object versions and provider ledger records through outcome discovery, and keep unresolved completions visible at their current object keys.

Use `ranked-outcomes` with the exact predecessor binding to page through the original provider's admitted catalogues and independently recorded staged versions.
The activation authority finds the closure from the frozen binding or its retained recovery publication, including after later recoveries.
Supply `--operation-id` to require a match with the selected recovery.
The `staged` array identifies each completed object's exact provider version, build cut and key generation, including files and manifests that never reached a catalogue.
The inventory covers writes through recorded staging, and older writes require separate provider inventory evidence.
Use brokers that understand staged receipts before commissioning writers with recorded staging.
Pass the returned `next_after` value as `--after` until the response ends the scan.

```sh
rtdb multi-activation ranked-outcomes xtdb --authority-socket "$SOCKET" \
  --expected-incarnation "$PREDECESSOR" --expected-revision "$REVISION"
```

### Cross-cell publication

Register each destination's cell UUID and raw 32-byte Ed25519 public key in the broker configuration under `destination_attestors`, a JSON object keyed by cell UUID.
Omit `destination` on an authority host that publishes solely from registered attestations.
For journal custody, give the authority broker the registered destination reader credential.
Keep the destination writer credential, object custodian credential and private signing key on the destination side.
Keep the Ed25519 private key in a mode-0600 PKCS#8 DER file owned by the destination verifier account. Use a separate key for each cell.
The verifier account runs inside the destination cell and uses its read-only journal TLS principal and the stopped installation's existing data and key vault.

Run `candidate` with the expected binding, recovery operation ID and `--cell-id`.
Stage the portable archive on the selected cell using the returned incarnation, `--immutable-streams`, the authority ID and the same `--cell-id`.
Use the selected archive's SHA-256 from `multi-backup export` when requesting and presenting the attestation.

```sh
# On the destination, after a committed restore.
rtdb multi-activation attest xtdb --authority-id "$AUTHORITY" \
  --expected-incarnation "$PREDECESSOR" --expected-revision "$REVISION" --operation-id "$OPERATION" \
  --cell-id "$CELL" --manifest-sha256 "$ARCHIVE_SHA256" --attestation-signing-key /etc/rtdb/attestor.pkcs8 \
  --data /var/lib/rtdb/staged --journal-url logkeeper-consensus:///etc/rtdb/destination-journal.json \
  --tls-dir /etc/rtdb/destination-reader --immutable-streams > installation.json

# From the operator account, after freeze on the same expected binding.
rtdb multi-activation publish xtdb --authority-socket /run/rtdb-activation/authority.sock \
  --expected-incarnation "$PREDECESSOR" --expected-revision "$REVISION" --operation-id "$OPERATION" \
  --cell-id "$CELL" --manifest-sha256 "$ARCHIVE_SHA256" --installation-proof installation.json
```

The verifier replays the commissioned immutable journal, reconstructs the committed restore, and checks its relocated files before signing.
The receipt covers the authority, database, predecessor binding, recovery operation, destination cell, candidate, and the installed source cut and archive digest.
The bounded JSON receipt can travel through untrusted transport. The broker checks its signature against the key registered for the selected cell and verifies the selection against the current authority binding.
The destination can issue the same receipt again after restart. A retry after publication can omit the receipt and use the retained authority result.

The staged restore's committed checkpoint and subsequent checkpoints retain its data and file closure.
Keep the staged journal, object store and keys in the destination's custody until activation or until the authority advances past this recovery intent. The destination's storage administrators are trusted to retain those resources.
Starting a serving listener for the staged incarnation requires a grant for both its incarnation and cell.
Set `RTDB_ACTIVATION_CELL_ID` on the selected cell when serving or reopening the incarnation.
Cross-cell publication upgrades the binding to cell-aware grant protocol version 3. Earlier readers reject that binding.

### Durable recovery

Use the durable workflow for a new recovery. Its first write upgrades the name to
protocol version 4. Upgrade serving readers and operator brokers before that
write. Existing names on versions 1 through 3 retain their legacy commands.

Pass the same `--expected-incarnation`, `--expected-revision`, and
`--operation-id` to each command.

1. Run `begin-recovery --loss-policy lossless|accept-unknown-tail`. Pass
   `--cell-id` for a remote destination. The authority records the predecessor,
   operation, destination, and loss policy and returns the candidate incarnation.
2. Run `freeze-recovery` to close predecessor grant issuance for that exact
   intent. For a lossless move, run `seal-source` against the broker once the
   commissioned source has stopped. The broker waits out old grants and seals
   the immutable source journal with a quorum writer fence. It records the
   resulting committed head as the final source LSN. Export through that head.
3. Restore into the candidate. For a remote destination, issue the signed
   installation receipt with `attest` and present it to `record-install` with
   `--cell-id`, `--installation-proof`, and `--manifest-sha256`. For a stopped
   local destination, `record-install` can use the direct or broker-configured
   verifier. The authority retains the verified archive digest, source LSN,
   application position, and system time. A lossless installation must match
   the retained final source LSN. An accepted unknown tail records the chosen
   cut and leaves the final source LSN absent.
4. Run `publish-recovery`. The broker reopens the recorded installation and
   freeze from quorum, waits out predecessor authority, and compares the same
   intent when committing the successor. The destination and proof file may be
   offline by this point.

An accepted-unknown-tail recovery can record the installation before freezing.
`recovery-status` returns the pending intent or the latest completed operation
from the authority after restart. A retry of publication returns the same
retained result. The expected binding revision fences retries after another
successor has been published. To replace an unfinished attempt, supply
`--supersede-operation-id` with a new operation ID to `begin-recovery`. This
preserves an existing freeze and any sealed lossless source cut.

Lossless source sealing uses a broker-configured stopped deployment with its
incarnation, optional cell, and protected data directory and source journal
TLS credentials in `source.location`. The broker checks the local database ID,
journal marker, and encrypted commissioning scope before using the source
writer credential. The provider configuration is installed with the broker.
The socket request supplies the operation identity only.

These verification paths use the local object store and key vault.
The local procedure relies on operator custody of the destination volume and keys through publication.

### Hosted custody and effect prerequisites

Hosted recovery needs custody of the destination journal's recorded installation cut and provider retention of the exact checkpoint and relocated objects until publication or supersession.
The object custodian needs an enrolled provider identity and an independently authorized readback of the hold after restart.
The byte-store Version hold lives with the current writer and fences the Store purge floor.
For S3 Object Lock, commission a dedicated versioned allocation and bind both verification and recovery reads to the held versions.
Give the verifier version-read access and either `s3:GetObjectLegalHold` or `s3:GetObjectRetention` for the selected hold mechanism.
Object Lock allows delete markers and newer versions above a held version. [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
A legal hold carries a single status per object version. [S3 legal-hold request](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html)
Operations that share a version need an ordered record of which recovery operations still own its hold and release the provider hold only after the last owner settles.

With the `backup-object-holds` build feature, use `rtdb multi-backup stage` to install a portable `xtdb` backup into the S3 serving allocation for a pending recovery with a destination cell.
Configure the independent activation reader using the `RTDB_ACTIVATION_*` settings above.
Supply the predecessor incarnation and revision, recovery operation ID, source archive and vault, destination vault, destination journal writer TLS and versioned S3 bucket.
The activation journal selects the destination incarnation and cell.
Lossless staging requires the archive to end at the predecessor's sealed final journal position.
Before opening the destination stream or allocating its generation, staging records the exact intent, destination journal connection and original vault identity in the S3 allocation.
The destination vault retains the exact generation before the staging writer puts the relocated files or publishes their checkpoint.
Use staging writers that support tracked uploads before starting a new order.
For new staging orders, the S3 ledger records an exact multipart upload before the staging writer sends bytes.
A replacement staging writer claims the ledger and drains any upload left by the previous writer.
Staging closes and drains the ledger before returning a receipt for acquisition.
Save the stage result with the archive.
It reports the destination and the key hold bound to the same consensus trust and installation coordinates used by the activation proof.
A retry before the authority records the installation reopens the original checkpoint and checks its active key hold.
Run `rtdb multi-backup acquire-object-holds` as the object custodian with the same archive, destination incarnation, authority and cell, destination journal reader TLS, key vault and S3 allocation.
Give it the stage receipt and configure the independent activation reader used for staging.
Acquisition requires the original S3 namespace in that receipt.
For an earlier receipt without that namespace, rerun `stage` against the original allocation while the recovery is pending.
Acquisition authenticates the staged checkpoint, files and token key envelope and records their exact versions before applying legal holds.
The saved acquisition plan also carries the original activation intent.
Use the same settings and stage receipt to retry a partial acquisition.
The command reopens the recorded versions on retry and returns the verified readback after the original provider confirms every hold.
Keep current-name visibility on the held versions for the later `attest` step.
Finish acquisition and save the signed installation proof before recording the installation.
To settle an acquisition interrupted before attestation, first supersede the candidate in the activation authority.
Run `rtdb multi-backup abandon-object-acquisition --stage-receipt FILE --authority-socket PATH --journal-url URL --journal-custodian-tls-dir DIR --key-vault DIR --bucket BUCKET` with the original destination providers.
To recover a saved acquisition when the local stage receipt is lost, configure the destination's `object_namespace` in the broker's `destination_journal_readers` entry with `require_object_custody` enabled.
Use the bucket, endpoint and region reported in the stage receipt for the commissioned allocation.
Run the same command with `--authority-id UUID --incarnation UUID` in place of `--stage-receipt FILE` to load the plan from that allocation.
Plans created before the original intent was recorded still require the stage receipt.
The original activation broker verifies abandonment and the original destination journal on each attempt.
Settlement of a saved acquisition also verifies the selected checkpoint.
The command settles the keeper's exact checkpoint hold, deletes the exact planned S3 versions, verifies their absence, and releases the held key generation.
If staging stopped before saving an acquisition plan, use the same authority and incarnation selector to find its staging record.
The original allocation records a terminal decision in the acquisition slot, which fences any later acquisition.
The custodian closes the staging upload ledger and drains the recorded upload before retiring the abandoned journal and generation.
For new staging orders, it purges the abandoned candidate's object and wrapped-key prefixes and reports `objects_purged` when the provider confirms both are empty.
Staging orders created before upload tracking leave unheld ciphertext for ordinary storage cleanup.
The S3 custodian needs version listing, legal-hold read and write access, `s3:DeleteObjectVersion`, and conditional writes for tombstones that keep older versions hidden.
Use the same acquisition identity and provider settings to retry a partial settlement.
Before advancing the binding again, the activation authority archives the previous recovery publication in its own immutable journal.
The broker can use that publication to settle older acquisitions after activation compaction.
The staging writer can create its `staging` record under `multi/recovery-custody/`, and the custodian can create the acquisition and settlement records there.
Both identities need conditional access to the staging upload ledger and permission to abort and read the parts of its exact recorded upload.
The custodian also needs permission to list and purge versions, delete markers and multipart uploads under the abandoned candidate's two data prefixes.
Both identities need to read their records and distinguish absence from denied access.
Protect acquisition records against deletion and replacement through settlement.
Scope the custodian's S3 version reads and legal-hold writes to the operation-exclusive candidate paths.

With the `backup-object-holds` build feature, run `rtdb multi-backup verify-object-holds` against a commissioned portable install staged in the S3 serving allocation.
Supply the selected archive, destination incarnation, authority and cell, journal reader TLS, destination key vault and S3 bucket.
The command checks the archive digest, rereads the staged checkpoint, all of its files and the wrapped token key, and requires S3 legal hold `ON` on the exact versions that supplied those bytes.
It accepts a staged cut whose retained suffix contains the selected checkpoint and ordinary lease records.
Supply `--stage-receipt FILE` to reconstruct the hold from the installed checkpoint and check that the original vault still holds that exact generation.
Save the JSON result with the archive and the custodian's recovery records.
Pass it back with `--readback FILE` and the same archive and destination settings to reopen the recorded versions, including when a delete marker or a newer version masks a held object.
The command checks the complete inventory and journal cut against the saved readback and obtains fresh legal-hold status from S3.
On Unix, run `rtdb multi-backup attest` with the same archive and destination arguments, the saved readback and stage receipt, and the destination cell's protected Ed25519 PKCS8 signing key and its `--journal-custodian-tls-dir`.
An installation with a durable staging record requires its exact saved acquisition plan before attestation.
The hosted attestor matches the selected keeper checkpoint's announcement position, covered position and ciphertext digest to the authenticated replay checkpoint.
The attestor checks that the journal head still matches the object readback before asking the quorum to retain the checkpoint.
The original journal quorum must retain that checkpoint before the attestor issues the combined proof.
Register the destination's journal reader with the broker and set `require_object_custody` for the cell.
The broker confirms the exact keeper hold and records the signed object inventory digest.
Recording hosted custody advances the name to activation protocol 7.
Upgrade the broker and serving readers before recording hosted custody.
Later recoveries of that name need the same three custody types.
Use the independent activation reader configured for staging.
The attestor requires every current object name to resolve to its recorded held version.
It rechecks the original key hold and the pending recovery on that activation connection.
It emits the signed proof for the broker's `record-install --installation-proof FILE` option.
The proof includes the digest of the typed object readback, which the cell signature covers.
Keep the readback with the signed proof and stage receipt.
Update the broker to a build that accepts the hosted proof before recording it.
Keep current-name visibility under object custody through the serving handoff.
Retire the staging identity's ability to overwrite those names or add delete markers once the staged inventory is fixed.
On AWS S3, grant the verifier `s3:GetBucketVersioning` on the allocation and `s3:GetObjectLegalHold` on the staged objects.
Reading the current object names uses `s3:GetObject` and reopening from a saved readback uses `s3:GetObjectVersion`. [S3 GetObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax)
Reserve permission to change a legal hold or delete a version for the object custodian.
In the loopback MinIO qualification, the version reader could also read the current object name.
The pinned verifier always supplies the recorded version ID.
Journal retention, hosted provider enrollment and permission to publish require their own authority.

For operation-exclusive holds, use `rtdb multi-backup release-object-holds` with the saved readback, signed installation proof, protected activation broker socket and original S3 settings.
A published candidate can release its staged object holds after the original journal quorum confirms that its staged hold was released and a later serving checkpoint covers the staged announcement.
Release the journal hold first and let the successor commit that checkpoint through its ordinary checkpoint path.
The broker checks the published installation digests and the replacement checkpoint through the registered journal reader before authorizing removal of the temporary object holds.
An authorized object release retry accepts the provider’s exact version-absence result after the version has been removed.
For a published hosted recovery, give `release-recovery-key` the same original journal through `--journal-url` and `--journal-reader-tls-dir`.
The key custodian checks the matched installation in the retained activation decision and confirms the same replacement checkpoint before releasing the staged key hold.
On a name that has recorded hosted custody, settle the hold while the authority still retains the exact publication or abandonment decision.
For object release, the broker matches the registered cell signature and the original activation connection.
For an abandoned candidate, the retained decision must show supersession or publication of a different recovery from the same binding.
Run the first object authorization while that decision remains available to the broker.
The custodian records this authorization under `multi/recovery-custody/` before changing any legal hold.
Protect that prefix with create-only writes, custodian-only reads and writes, and retention through settlement.
On AWS, give the custodian `s3:GetBucketVersioning`, `s3:GetObjectLegalHold` and `s3:PutObjectLegalHold` for the allocation and staged candidate paths.
Give it `s3:GetObject` and conditional `s3:PutObject` on the ledger prefix. [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
Its ledger read must distinguish an absent key from denied access, which requires `s3:ListBucket` on AWS S3. [S3 GetObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
The command confirms `OFF` on each signed version and creates a completion record.
Retry with the same signed proof and readback to resume from the stored authorization or return the completed result.
Release after publication of the candidate requires a serving-custody handoff.

The installed server opens its generation keys through `FileKms` at `RTDB_KEY_DIRECTORY`.
A hosted key custodian must retain decryption of the exact copies' generations across worker and verifier loss and order hold placement and release against key retirement.
Key readability at the time of verification proves one read.
Bijection's native capture holds the original key groups enrolled by a management-signed grant until the threshold ledger records a disposition. [Capture hold](https://github.com/bijectionhq/bijection/blob/41f92ae5615786dab694a8643d6aaca55339c0ce/crates/quorum/src/custody_lifecycle.rs#L194-L230) [Disposition](https://github.com/bijectionhq/bijection/blob/41f92ae5615786dab694a8643d6aaca55339c0ce/crates/quorum/src/custody_lifecycle.rs#L166-L178)
Its captured Root reader provides read-only access while the original grant and reader session remain valid. [Captured Root reader](https://github.com/bijectionhq/bijection/blob/41f92ae5615786dab694a8643d6aaca55339c0ce/crates/quorum/src/custody_view/capture.rs#L355-L379) [Key operations](https://github.com/bijectionhq/bijection/blob/41f92ae5615786dab694a8643d6aaca55339c0ce/crates/quorum/src/custody_view/capture.rs#L454-L482)
Using Bijection for staged destination custody requires a writable original key group and a provider handle bound to the exact RTDB destination incarnation and generation.
Staging and serving wrap the destination journal and object data keys under that handle.
The verifier and successor reopen those envelopes through the same generation authority.
The custodian needs a provider route that orders the exact recovery hold and its release against retirement of that handle.
After the source capture settles and the staging and verifier processes exit, verify that the successor can still decrypt the staged journal and objects.
For an AWS KMS design, establish which authority can disable or schedule deletion of a held key, since a key in pending deletion cannot decrypt. [AWS KMS key deletion](https://docs.aws.amazon.com/kms/latest/developerguide/deleting-keys.html)
The object and key custodian receipts must bind the same recovery operation, destination incarnation and verified dependency closure.
Any finite hold must still cover the activation decision when it commits.
Release after publication requires a handoff to the destination's serving custody.

Ranked derivative publication needs an authority that admits staging for the current incarnation and fences or drains provider work before that incarnation loses authority.
The closed ranked prefix accounts for admitted catalogues.
The provider closure drains an outstanding recorded multipart upload and binds the exact completed versions it discovers, including a completion whose reply was lost.
Derivative custody must retain or retire the exact primary cut, derivative object versions and generation keys.
An assertion erasure report also needs signed settlements from each backup and derivative custodian in its ordered roster.

RTDB keeps commissioned funded dispatch fenced pending integration with an original booking issuer.
Commissioning requires the issuer to be enrolled for the exact activation authority, database, serving binding and receiver certificate. [Original funded admission contract](../../allium/rtdb-multi.allium)
The installed `fundedReceiverClock` transport supplies clock observations for that certificate and source incarnation.
`RTDB_TIMED_ORIGINAL_TRUST_FILE` pins the original keeper proof signers, and the receiver and keepers use the same guarded clock scale.
Give recovery an independently authenticated route to the original issuer and retain the original issuer and keeper verification identities through settlement.
The original issuer durably binds each accepting stage to its exact sealed append before the writer sends it to the original journal.
Logkeeper can verify the timed-original proof of a committed record after it moves from local storage to its original offload store. The stream's logical floor controls proof lookup in both locations. Preserve verifiable settlement evidence before a covering checkpoint trim or retirement removes the original proof. [Timed proof read](https://github.com/bijectionhq/logkeeper/blob/119f527310e40ad77765527ae1ac6040a6ae2670/src/stream/consensus/journal.rs#L371-L383)
A hold on the selected checkpoint preceding a funded append keeps its timed proof readable through writer replacement by blocking checkpoint replacement while the hold is active. [Checkpoint replacement](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/src/stream/consensus/state/checkpoint.rs#L199-L220)
Logkeeper accepts one checkpoint hold identity per stream and retains its terminal release record through later checkpoint replacements. [Exact checkpoint hold](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/src/stream/consensus/state/checkpoint.rs#L99-L123) [Released custody regression](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/src/stream/consensus/keeper/tests/authority.rs#L748-L761)
Logkeeper can export a portable commitment receipt with `attest_timed_original` on immutable streams after cluster feature level 13 is finalized. The original keeper signs the commitment after a quorum read barrier. The receipt includes the original signed timing samples. Retain the receipt at the original issuer before a covering trim or retirement. Verification uses the enrolled catalog and historical keeper keys and works after the source keepers shut down. [Commitment receipt](https://github.com/bijectionhq/logkeeper/blob/0f056c0570a2a68b74991e1a45f211a280dcfc6e/src/stream/timed.rs)
Read the complete funded scope roster from the original activation decision that closed predecessor grants, including scopes closed before certificate rotation.
Upgrade the activation broker and serving readers to activation protocol 9 before enrolling a funded scope.
Use the activation broker's `commission-funded` operation to enroll the original issuer's Ed25519 public key and exact receiver certificate against the current binding.
The authority orders enrollments by issuer identity and certificate digest and freezes that roster with the predecessor's grants.
After `freeze-recovery`, page through `funded-scopes` by predecessor incarnation and revision and follow `next_offset`.
The same lookup returns the frozen roster and recorded closures after supersession, publication, or later recoveries.

For each scope, authenticate the frozen binding to the original issuer and import its signed closure with `record-funded-closure`.
The closure is JSON with `scope_sha256`, `bookings`, `inventory_sha256`, and `signature`.
Digests and signatures are arrays of unsigned bytes.
The issuer signs the concatenation of the UTF-8 domain `rtdb.activation.funded-closure/v1` followed by one zero byte, the 32-byte scope digest returned by the broker, the eight-byte big-endian booking count, and the 32-byte inventory digest.
An empty inventory uses count zero and an all-zero digest.
The authority verifies the receipt against the enrolled issuer and refuses publication until every predecessor scope has an admission closure.
These broker operations enroll issuers and import closures. Funded dispatch and per-booking reconciliation still require the original issuer integration.

Obtain a durable closure for each scope and enumerate its complete booking inventory, including bookings whose replies were lost.
Resume from that same closure after a restart or replacement recovery operation.
Reconcile each booking with its original terminal disposition and verify the original keeper proof of every accepting append, including an accepted appointment activation followed by a refusal.
When a lookup misses or is trimmed, settle the stage only from preserved acceptance proof or an original exclusion and fence.
Logkeeper can commit an authorized append after the journal cutoff using signed samples that place durability before it, while the original writer and roster still have authority. [Timed append](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/src/stream/consensus/keeper.rs#L2513-L2564)
A lossless seal of the original immutable stream excludes an exact attempt when its authorized position is above the sealed committed head and its writer epoch is fenced by the seal.
Verify the stream, writer fence and head from the original seal receipt together. [Seal receipt](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/contract/src/lib.rs#L246-L252) [Atomic seal](https://github.com/bijectionhq/logkeeper/blob/14d7b7ec7c3d74667d95bebbe9ee143ca6eb8d98/src/stream/consensus/state.rs#L1113-L1152)
Keep successor funded admission closed until the original evidence settles every booking.

Bijection's managed inbox records a slot by commit label and scheduled reading, then binds it in the transaction that records the call's receipt. [Slot offer](https://github.com/bijectionhq/bijection/blob/56843bdd7c095fd209c3ea74e03b95eaa146b140/crates/kernel/src/pipeline/mod.rs#L5641-L5684) [Receipt binding](https://github.com/bijectionhq/bijection/blob/56843bdd7c095fd209c3ea74e03b95eaa146b140/crates/kernel/src/pipeline/mod.rs#L5858-L5898)
If the slot offer commits and its reply is lost before receipt binding, a retry chooses from the current store time and can select a new slot.
RTDB's original issuer must recover the exact same booking by request identity in that case.
Bijection's managed RTDB placement issuer mints version 6 writer-fence and version 7 first-placement provisioning grants. [Placement grant roles](https://github.com/bijectionhq/bijection/blob/56843bdd7c095fd209c3ea74e03b95eaa146b140/crates/cell_catalog/src/catalog/managed_workload.rs#L251-L285)
Managed funded appointments require an enrolled issuer for the version 2 funded-appointment grant. [Funded appointment contract](rtdb-v1.allium)

A hosted qualification requires a named non-production allocation with scoped object and key provider credentials, an independent activation quorum, and separate reader, operator and custodian identities.
Drive the same recovery through lost replies and process restart, prove the selected versions and keys stay usable with the verifier offline, and verify that supersession or publication settles only the selected operation's hold.
Exercise derivative staging and funded dispatch across that same handoff only after enrolling their effect authorities.

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

For a commissioned hosted source, use `multi-backup export-hosted` with the original journal's reader credential, S3 allocation, and generation key vault.
The command checks the immutable source's commissioning identity and creates an encrypted archive without advancing the source journal.
For lossless recovery, export after `seal-source` so the archive covers the final source LSN required by the activation broker.

```sh
rtdb multi-backup export-hosted --archive "$ARCHIVE" --incarnation "$PREDECESSOR" \
  --authority-id "$AUTHORITY" --journal-url "$SOURCE_JOURNAL" --tls-dir "$SOURCE_READER_TLS" \
  --key-vault "$SOURCE_KEY_VAULT" --bucket "$SOURCE_BUCKET" --region "$SOURCE_REGION"
```

For a source commissioned in a cell, also pass its original `--cell-id`.
Use `--database NAME` on hosted export, staging, object hold and attestation commands to select a commissioned name other than `xtdb`.
The object release command reads that name from the signed installation proof.

`rtdb multi-backup export` captures a settled cut from a stopped, initialized development deployment, using its journal and object store to verify the current checkpoint dependencies.
An immutable export takes the existing local journal lock and replays a quorum-confirmed head directly from the application stream using reader credentials.
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
Opening the installation proof rereads the selected replay checkpoint from its backing store and verifies the index and each relocated file using the destination's custody.
Recovered ordinary receipts use the new incarnation's basis, and the archive retains the original receipts.
The commands work with the local development journal and the optional loopback Logkeeper connections configured with `--journal-url` and `--tls-dir`.
For a version 4 immutable stream, pass `--immutable-streams` to export, restore, and activation publication.
Pass the commissioning `--authority-id UUID` when exporting a commissioned source or staging a commissioned destination.
A history created before durable commissioning can become an activated name by restoring a cut into a fresh commissioned incarnation.
Withdraw access to the uncommissioned predecessor's endpoints before publishing the first commissioned successor.

Existing inline archives remain readable.
Archives with files cover ordinary checkpoint attachments within the archive size limit and the current source key generation.
Active pins, files retained only through pin history, transaction object inventories, protected custody, and scoped or funded receipts require a separate portable transfer.
Ranked catalogues are rebuilt in the successor from the transferred database state.
Recovery reconciles their admissions at the original ranked provider.
The export refuses incomplete erasure and key rotation.
The command stages a destination cut for local rehearsal; a shared database name requires its own activation decision and predecessor fence.

## Qualification

The [2026-11-26 consensus keeper results](../../verification/results/2026-11-26-consensus-keeper.adoc)
record the runs. To repeat them:

```sh
cd rust
# The Multi driver tests on real keepers in one process.
RTDB_MULTI_JOURNAL=consensus RUST_MIN_STACK=16777216 cargo test -p rtdb-db --lib multi::tests
# Operator recovery end to end on real keepers (always on consensus keepers).
RUST_MIN_STACK=16777216 cargo test -p rtdb-db --lib multi_resumes_after_a_real_keeper_recovery
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
