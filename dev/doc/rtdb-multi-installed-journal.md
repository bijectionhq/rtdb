# Installed Logkeeper journals for RTDB Multi

An installed journal owns the writer roster and grant, append decisions, protected read cuts, checkpoint selection, and the replay floor for one deployment. Keep the logical deployment, incarnation, group and generation fixed during worker and voter replacement. The physical voter digest changes only during independently funded cohort repair.

Use a fresh deployment ID and Logkeeper placement for a cutover.
The ordered application, command, and compact recovery-image formats require matching workers and voters.
The ordered command and compact image admit reads as fresh invocations and release a read with a lost outcome by its identity.
They retain exact mutation receipts, custodian-owned checkpoint and trim commands, and exact trim credits.
Application wire version 9 adds the custodian's bounded query of withdrawn hold owners and their eligibility for expiry disposal.
An existing RTDB directory record and its log offsets do not certify a Logkeeper history.
Route the installation through the installed manifest when the initial group, storage and keys are ready.

## Provision the journal and storage

1. Install an odd cohort of at least three Logkeeper voters in distinct failure zones. Give each voter an exclusive persistent volume and a distinct TLS certificate and receipt-signing key. The stage and selected image alternate between two preallocated slots on each volume. Size the volume for the two full slots, group metadata and filesystem overhead. The initialization command creates new media. Restarts use the serving command and recover the existing media.
2. Give each daemon its local manifest with the identical group definition, exact voters and client certificate pins. Configure separate client identities for the administrator, each enrolled worker, the checkpoint custodian and gateways. Give the gateway principal the `route_reader` role. Keep client TLS keys and the voters' signing keys in independently mounted secret files.
3. For an empty installation, choose a nonzero initial history fingerprint, an initial checkpoint with `next_lsn` zero, zero root, manifest and custody provider digests, and an allocation at revision one. Choose a distinct nonzero allocation for the initial write window at serial one. Fund the initial ordinary window from `retained_data_bytes - maintenance_bytes`. Ordinary appends cannot spend the maintenance portion. RTDB requires the maintenance portion to cover three maximum-sized encoded log records plus their journal framing: a retained previous upload and an in-flight block boundary and upload. A replacement writer takes its term from the journal grant and appends no claim, so repeated replacement or capacity demotion between checkpoints adds no maintenance record. The application frame limit must fit the largest permitted payload plus authenticated request and receipt framing. Fund ordinary replay holds for open cursor intervals and overlapping reads. Logkeeper reserves four additional paid control slots for startup suffix and root holds across one replacement. Leave enough paid headroom to append a flush marker while earlier replay users retain their holds. Set `serving_lease` and `clock_drift` in milliseconds. A worker serves bytes only within its renewed lease, and a withdrawn worker's holds become disposable once journal time passes its last lease end plus `clock_drift`. Choose `clock_drift` to bound clock rate error between the voters' proposer clocks and the workers' monotonic clocks over one lease.
4. Initialize and run the journal with the same RTDB Multi image, or use the standalone Logkeeper CLI with its ordered daemon feature. The journal does not need directory connectivity.

   ```sh
   rtdb-multi initialize-ordered-journal --manifest /run/rtdb/voter.json
   rtdb-multi ordered-journal --manifest /run/rtdb/voter.json
   logkeeper ordered route /run/rtdb/voter.json DEPLOYMENT_UUID
   ```

5. Enroll each worker using `logkeeper ordered enroll CONTROL_JSON WORKER_UUID`. The control JSON contains the public `route` emitted above and the administrator's `credentials` paths. Record the returned worker UUID and serial in the voter client pin, the worker's route credential and each gateway endpoint. Roll the new client pins onto the existing voter media. Enrollment chooses a new serial in the journal and the certificate pin binds it to the intended worker.
6. Initialize the serving storage and vault using `rtdb-multi init-storage`. Use scoped S3 writes for the serving store. Provision a separate, enrolled terminal S3 namespace for checkpoint custody. It must preserve conditional-create semantics and its unversioned state for the lifetime of the installation. Qualify and enroll the archive using the existing terminal-storage procedure, then grant the custodian conditional replacement for terminal tombstones. Application copy credentials should permit only conditional creation. Bucket policy and lifecycle must preserve these tombstones. [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) · [Bucket policy enforcement](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html) For a local archive, initialize the archive volume with its own storage namespace. Read its physical provider fingerprint with `rtdb-multi ordered-archive-identity --archive-root ARCHIVE --archive-prefix PREFIX`.
7. Prepare the RTDB identity and install the serving key.
   The command prints one `DeploymentRecord` for use in the node manifest.
   In a mixed cell, supply `--directory DIR` to reserve the installed UUID before provisioning the key.
   The reservation is permanent, is invisible to deployment lookup and placement, and refuses a UUID already used by a directory deployment.

   ```sh
   rtdb-multi prepare-ordered-deployment --route /run/rtdb/public-route.json \
     --name DATABASE --org ORGANIZATION --keys KEY_VAULT --directory DIR
   ```

   Omit `--directory` for an installed-only cell with no shared directory.

## Install node and gateway routes

Set `--installed-journals /run/rtdb/node-placements.json` on each node and `--installed-journals /run/rtdb/gateway-placements.json` on the gateway. The hosted entrypoint also accepts the absolute mount path in `RTDB_INSTALLED_JOURNALS_FILE`. Continue to supply the cell secret used for RTDB control and gateway capabilities. A cell serving only installed journal placements can omit `--directory`. In a mixed cell, installed names are resolved from the authenticated journal and remaining names use the directory.

Upgrade directory readers before reserving IDs in a shared directory.
For an existing installed-only placement joining a mixed cell, rerun preparation with its exact route, coordinates and `--directory` before sharing the media with directory administration.
The node and the gateway's hosted installer also reserve their configured installed UUIDs before serving.
Configure the same directory for the nodes and for any controller, reaper or recovery service with access to the cell's serving media and keys.
The reservation carries no grant, checkpoint or retirement authority.
The [installed-journal bypass audit](../../verification/redesign/installed-journal-bypass.md) traces those authorities.

The manifests contain arrays. A node entry includes:

- `deployment` with the prepared RTDB deployment record.
- `worker` with the numeric RTDB `incarnation` and `log_epoch`, the public Logkeeper `route`, the worker's `credentials` paths, and an `authority` object whose `kind` is `worker` and whose `enrollment` matches the journal.
- `custody` with the same exact placement coordinates, a separate client certificate, and authority kind `custody`. Give the intended custodial nodes access to this service. A custodian can finish a staged checkpoint while the writer grant is vacant and the database is closed.
- `archive` with the independent terminal `CellStorage` configuration and `archive_prefix` beneath it.
- `endpoints` with each eligible `enrollment` and its exact `control` and `pgwire` addresses. The node's advertised addresses must match these entries.
- `checkpoint_reservations` with preassigned allocation UUIDs, the exact provider fingerprint in `custody.provider`, consecutive revisions starting after the installed checkpoint, and `usable_bytes` for each exact allocation. Fund peak old retention, the new copy and its metadata. Renew the reservation inventory before it runs out. Nodes and replacements must agree on an already prepared allocation.
- Optionally, `journal_window_bytes` to cap each automatically replenished append window. The actual funded amount is bounded by paid, currently reusable voter space.

A gateway entry includes `name`, the exact `deployment` UUID, a `journal` scope with routing authority and credential paths, and the same enrolled worker endpoints. Native PGWire and ordinary Flight SQL follow the journal route, authenticate worker readiness, and compare it with the current grant. A Flight session must reconnect when the journal orders a different grant. The ordered installation currently serves fresh reads on the holder. The explicit frozen-snapshot selector and audience Flight use separate recovery paths.

The installed Logkeeper adapter currently supports the writer-local staging deadline.
The physical receiver requires timed original quorum durability and refuses accepting writes on this adapter.

## Observe recovery and maintenance

A worker obtains a grant against the last observed writer state. It opens the exact checkpoint and protected suffix named by that decision, waits until replay has applied through the granted cut, and confirms the grant before reporting readiness. Old file writes remain scoped to their grant-specific serving prefix.
A protected replica delivery retains its journal hold while resolving an offloaded record.
The worker reads that object from the exact writer epoch recorded by Logkeeper and checks the chosen payload digest before using it.

The custodian obtains an ordered checkpoint preparation before copying data. The preparation keeps the exact source cut and its base alive. A replacement can use the completed provider manifest or continue the copy from the original immutable serving prefix.
With no prior preparation, the custodian can discover a chosen native boundary and upload, verify the original catalog, and prepare its provider copy independently of the writer grant.
The scan is tied to the exact selected base and the newest chosen maintenance cut at the start of inspection.
A changed base invalidates the scan, and staging still requires the new ordered preparation. An accepted block upload carries the digest of its exact encoded catalog. Installed recovery requires that digest before adopting or confirming the catalog. Before staging a new copy, the custodian reads and decodes the chosen native block boundary under the exact preparation. Publication and trim use the custodian identity independently of the writer grant. Trim names the exact currently selected reconstructable root and retains every suffix still held by an overlapping read or attachment. Its receipt names the old floor and the resulting floor and totals the encoded bytes released by append class. Ordinary windows replenish from the reusable voter space reported by the installed journal. On ordinary capacity pressure, the driver pauses ordinary intake, preserves the previously accepted source prefix, and uses the maintenance lane for promotion and checkpoint records. The custodian queries the exact candidate cut and orders preparation without borrowing an ordinary read hold.

Progress assumes a reachable installed quorum, a live custodian and provider namespace, a funded checkpoint allocation, and eventual release or verified disposition of overlapping holds. The bounded reserve admits a finite maintenance sequence. Repeated crashed attempts that each choose further maintenance records can consume that sequence's headroom. The enrolled RTDB worker classifies the typed replica records before sealing them. Its TLS credential carries that authority.

Past checkpoint archives first close in journal order. Reads and disposal verify the original physical storage identity before using or releasing that custody. Closure requires every hold and preparation referencing that archive to finish. The terminal provider replaces each archived member with a permanent conditional tombstone and fences the manifest last. A retry can recognize that final fence after the original bytes are gone. Tombstone metadata remains as provider closure evidence. A local archive uses a shared filesystem with reliable cross-process file locks and durable directory synchronization. Archive readers retain a shared lock. Retirement takes an exclusive lock, records a permanent closed marker, and removes the allocation including its private scratch directory. Serving prefixes for old writer epochs are retired separately from this archive retirement.

### Retire serving prefixes of earlier writers

Each writer grant writes below `STORAGE_PREFIX/~ordered/GENERATION/EPOCH/` and records its exact grant in a `~grant` object there when it opens.
The custodian retires the prefix of an earlier epoch on its maintenance pass, after it publishes and trims checkpoints.
It refuses until every condition holds:

- the journal names a later writer grant;
- the selected checkpoint is an independent archive and the journal floor equals its cut;
- no retained record from the old grant remains above that cut, and no pending preparation copies from it;
- the provider shows the prefix drained.

A local prefix is drained when no process holds its `EPOCH.lifecycle/lock`.
Writers, replica readers and preparation copies hold that lock for the life of their store, so a zombie process delays retirement until it exits.
Retirement takes the lock exclusively, writes a permanent `closed` marker and removes the prefix; a later open is refused.

On S3, retirement waits for the scoped write horizon after it first observes the fence, currently fifteen minutes and one second, then drains the prefix's multipart uploads.
A version-enabled bucket removes every version and delete marker under the prefix.
An enrolled never-versioned bucket replaces each member with a conditional tombstone and the `~grant` object last; tombstones remain as closure evidence.
Both need `s3:GetBucketVersioning`, `s3:ListBucketMultipartUploads`, `s3:AbortMultipartUpload` and `s3:ListBucketVersions` on the prefix, and create-only `s3:PutObject` for the intent and receipt; the first also needs `s3:DeleteObjectVersion`, and the second conditional `s3:PutObject` on the members.
An unversioned bucket without terminal enrollment is refused.

The intent and the settled receipt live in `STORAGE_PREFIX/~ordered/GENERATION/~retired/EPOCH` and `EPOCH.settled`.
A replacement custodian resumes from the intent.
A prefix written before grant records were introduced has no `~grant` object and is refused; account for it separately.
Local tests exercise this path end to end.
The S3 operations are covered by an HTTP fixture and, when `RTDB_S3_ENDPOINT` names a local S3 service, by a version-enabled bucket test; neither qualifies a deployed provider's IAM policy or lifecycle rules.

Read and recovery holds left by a disconnected worker can delay trim.
Do not withdraw a worker merely because a gateway stopped reaching it.
When retiring the worker, withdraw its exact enrollment with `logkeeper ordered withdraw CONTROL_JSON WORKER_UUID SERIAL`.
On each maintenance pass the installed custodian checks bounded pages of withdrawn enrollments that still own holds, even while the writer grant is vacant.
The journal derives each enrollment's eligibility from its selected state.
The journal permits expiry disposal once the chosen decision time reaches the last serving lease end plus `clock_drift`, or immediately after withdrawal if the enrollment never held a lease.
The custodian issues due disposals before trim and uses its local clock to attempt a decision when an idle journal's selected time has yet to advance.
It retries a lost outcome with the same invocation and uses a fresh invocation after a chosen `Unavailable`.
Trim can advance once all overlapping holds are released.

An administrator can also request expiry disposal manually with the same control manifest.

```sh
rtdb-multi ordered-dispose-expired-worker --manifest CONTROL_JSON --worker WORKER_UUID --serial SERIAL
```

If the command reports `Unavailable`, rerun it after the deadline.
For earlier disposal, use `logkeeper ordered dispose-worker` only after verifying that the exact enrollment's processes and effect scope have permanently shut down, and keep the external disposition digest with the deployment's custody records.

## Journal payload keys and erasure rotation

Preparing a fresh installed deployment provisions its serving key and an independent journal payload key at generation 1.
A writer seals both data and maintenance records with the key identified by the deployment incarnation and its grant's key generation.
Worker recovery and checkpoint custody use each chosen record's original grant to open it.
Ordinary writer replacement keeps the current generation.
Replay that requires a revoked generation returns `rtdb/erased-under-view`.
A missing key or unreadable ciphertext stops replay with a storage failure.
The local vault performs journal cryptography inside its revocable operation authority.
S3 and DynamoDB can store independent generation keys and check withdrawals, and still require qualification for scheduled erasure.

This format requires compatible clients and voters.
The app wire uses `LKAPPC08`, and the compact image uses `LKBASE05`.
Existing installed journals require a cutover to a freshly installed stream for this format.

### Clean checkpoint and erasure commit interface

The R7 erasure coordinator still needs clean successor construction and a durable erasure commit.
Its input is the erasure identity, custody domain, exact incumbent grant, quiesced journal cut, and the clean root and provider manifest for that cut.
The coordinator provisions the next journal key generation before ordering rotation.
The custodian publishes the clean checkpoint at the quiesced head and certifies its full storage closure.
An ordinary checkpoint cut precedes its upload record, so that ordinary publication needs a separate adaptation to represent the clean state at the quiesced head.

The holder orders Logkeeper's `RotateKey { grant, checkpoint }` against the exact selected checkpoint.
The accepted decision returns the new grant and its held recovery basis at the checkpoint cut.
A concurrent append changes the head and requires selecting a new cut and checking the clean closure again.
A retry after an uncertain decision uses the same request identity and exact command.
Recovery adopts the new grant and appends only under its generation from the cut.
Any historical confirmation evidence needed by recovery must be carried into that clean closure before revocation.

The erasure commit consumes durable evidence for every selectable slot, binding each slot's identity, clean root, cut, and suffix key obligations.
The publication authority must continue to exclude older selectable roots once it has issued that evidence.
The commit must durably bind the erasure identity, the retired key generation, the clean cut, and the provider's armed deadline as one observable result.
Retries reconcile to the same commit and expiry.
Existing holds keep their entitlement until the deadline, at most 120 seconds from the commit.
Clean publication and revocation must account for offloaded record bodies, retained plaintext and dependent storage in the custody domain.
The resulting provider and clock guarantees have to cover all outstanding key operations and grants.

## Repair the voter cohort

1. Create successor manifests that keep the same logical placement and identify the predecessor digest and its receipt-verification keys. Provision the new voter volumes and run `logkeeper ordered reserve-repair` on each successor to produce signed capacity tickets. The pending votes cannot serve the journal.
2. Collect a distinct target majority of tickets. From a reachable predecessor voter, use `logkeeper ordered seal-repair` with the target group definition and the tickets. The predecessor orders the exact successor and produces a signed transfer package for that selected image.
3. Deliver the package independently to the new voters and run `logkeeper ordered install-repair` on each funded volume. Start the successor from that media. A lost transfer reply can be recovered by reissuing the same source seal against the same destination.
4. Install or extend the authenticated application route chain with the exact successor digest and its endpoints. Keep the original issuer verification keys and include the new issuer keys. Reachable clients can follow the preinstalled successor when the predecessor is sealed or unreachable. Ordinary writer replacement keeps the existing cohort and never invokes this repair flow.
5. Record disposal of previous physical media separately. A successful repair establishes the exact installed continuation. Original unreachable storage remains a disposal obligation until the original storage owner can account for it.

## Verify the handoff and retention contract

The Lean models in `verification/multi/lean/OrderedCapacity.lean`, `OrderedJournal.lean`, and `OrderedQuorum.lean` state the system obligations.
The capacity model compares the saturated ordinary window with and without a funded maintenance sequence.
The journal model composes exact handoff, receipt retention, fresh read admission and release, checkpoint protection, provider disposition, and trim.
The quorum lemmas state the assumptions on durable promises and agreement of higher ballots.
The Lake package in `verification/multi` imports Logkeeper at the same source revision as the Rust workspace.
Run `lake build` from `verification/multi` to check the composition against that pinned revision and the directory's Lean toolchain.
`RecoveryComposition.lean` relates the exact decoded append selected by the voter model to the receipt retained by the RTDB trace, including across a modeled transfer of the exact predecessor image to a repaired cohort.
It also models publication and trim under custody authority.
Concrete wire decoding, durable media steps, provider evidence, and semantic fidelity of the database cut are separate refinement obligations.

The standalone Logkeeper Verus harness includes the admission functions used by the production command reducer and compact-image decoder. It covers the checked debits, the trim gate, and the expiry and fingerprint decisions that govern exact reissue. The full codec round trip and the disk and network boundaries also need their own evidence.

The native acceptance source in `rust/crates/multi/tests/ordered_journal.rs` covers lost replies through authenticated journal sockets, and exercises worker replacement and resumption of a prepared checkpoint through RTDB's PGWire and Flight gateways. The linked Logkeeper source has a compact-image round trip at full ordinary capacity and a torn inactive-slot scenario. RTDB's S3 HTTP fixture checks that an unsuccessful conditional tombstone needs retry and exact confirmation. Live-provider qualification still needs to exercise the deployed IAM policy, enrollment, conditional writes, and allocation disposition.

Before a hosted cutover, exercise failed voter and worker replacement with the directory unavailable, ordinary bytes and reader slots full, and a stalled provider operation. Keep the provider usable for at least one completion sequence. Confirm that the surviving quorum retains the original write result and grants a new writer without additional voter allocation. Confirm that a fresh statement reaches the barrier cut, that a protected old read delays trim, and that writes reopen after its hold closes and the exact archived checkpoint becomes usable. Transfer to independently reserved successor media before counting another failure zone as survivable.
