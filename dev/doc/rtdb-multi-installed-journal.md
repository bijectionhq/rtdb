# Installed Logkeeper journals for RTDB Multi

An installed journal owns the writer roster and grant, append decisions, protected read cuts, checkpoint selection, and the replay floor for one deployment. Keep the logical deployment, incarnation, group and generation fixed during worker and voter replacement. The physical voter digest changes only during independently funded cohort repair.

Use a fresh deployment ID and Logkeeper placement for a cutover.
The ordered application, command, and compact recovery-image formats require matching workers and voters.
The ordered command and compact image admit reads as fresh invocations and release a read with a lost outcome by its identity.
They retain exact mutation receipts, custodian-owned checkpoint and trim commands, and exact trim credits.
Application wire version 9 adds the custodian's bounded query of withdrawn hold owners and their eligibility for expiry disposal.
An existing RTDB directory record and its log offsets do not certify a Logkeeper history.
Route the installation through the installed manifest when the initial group, storage and keys are ready.

## Install hosted tenants at runtime

Start the gateway and nodes with `--installed-cell FILE` and the cell route secret.
Use the gateway's existing installation listener and lifecycle credentials.
Initialize each voter's media before it starts with an empty `groups` list.
Give the voters a common `management` array of administrator, route reader and worker certificate pins using the same role objects as the group `clients` array.
The voter binds management and application connections to their authenticated TLS identities.

The cell file contains the following fields.

| Field | Value |
| --- | --- |
| `cell` | Cell name |
| `ca` | Path to the cell's CA PEM file |
| `voters` | Odd roster of at least three entries with `voter` UUID, `zone`, `endpoint` as `host:port`, TLS `server_name`, `certificate` as a 32-byte SHA-256 array and Ed25519 `receipt_key` as 64 hex characters |
| `limits` | Logkeeper `Limits` for every new group |
| `archive_root` | Independent archive storage location, including the terminal scope for S3 |
| `keys` | Serving and journal key vault location |
| `maximum_prefix_bytes`, `maximum_application_frame` | Voter media prefix and application frame sizes for each group |
| `exchange_ms` | Management deadline in milliseconds, defaulting to 2000 |
| `administrator_pin`, `route_reader_pin`, optional `custodian_pin` | SHA-256 pins encoded as 32-byte arrays |
| `workers` | Ordered array of configured worker slots with `worker` UUID, `certificate` pin, `control` and `pgwire` addresses |
| `credentials` | Optional per-role objects `administrator`, `route_reader`, `worker` and `custodian`, each with a PEM `certificate` and `key` path |

Mount only the private keys that each process uses.
When a role has no credential entry, supply the `_CERTIFICATE` and `_KEY` path variables under its prefix `RTDB_ORDERED_ADMINISTRATOR`, `RTDB_ORDERED_ROUTE_READER`, `RTDB_ORDERED_WORKER` or `RTDB_ORDERED_CUSTODIAN`.
A node's certificate selects its configured worker slot.
Keep worker profiles and voter settings stable across an in-progress installation.

For incarnation 1, send `Install` with the tenant's deployment UUID and name.
The gateway derives stable placement coordinates from the UUID and sends the exact group definition to every voter.
A strict majority must persist the definition before the gateway reserves the deployment UUID and provisions the serving key and journal key generation 1.
The gateway enrolls the configured worker slots and publishes the name binding on a strict majority before returning the existing `Installed` reply.
Retry a lost reply with the same UUID, name and organization.
The gateway uses a matching voter majority for name discovery, and a node obtains the published route by deployment UUID when it first receives a matching activation.
The installed-cell gateway and node serve through this journal authority.
The static `--installed-journals` configuration remains available for a provisioned group.

`ScheduleDisposal` waits for the selected checkpoint to cover the journal head.
It withdraws the configured enrollments and retires the group after its holds and any checkpoint preparation have drained.
A matching voter majority persists the terminal retirement and the lifecycle endpoint returns `Scheduled`.
The retired route stays unavailable to workers and serving gateways.
The journal seal retains the voter media and does not supply a physical disposal receipt.

## Provision the journal and storage

1. Install an odd cohort of at least three Logkeeper voters in distinct failure zones. Give each voter an exclusive persistent volume and a distinct TLS certificate and receipt-signing key. The stage and selected image alternate between two preallocated slots on each volume. Size the volume for the two full slots, group metadata and filesystem overhead. The initialization command creates new media. Restarts use the serving command and recover the existing media.
2. Give each daemon its local manifest with the identical group definition, exact voters and client certificate pins. Configure separate client identities for the administrator, each enrolled worker, the checkpoint custodian and gateways. Give the gateway principal the `route_reader` role. Keep client TLS keys and the voters' signing keys in independently mounted secret files.
3. For an empty installation, choose a nonzero initial history fingerprint, an initial checkpoint with `next_lsn` zero, zero root, manifest and custody provider digests, and an allocation at revision one.
   Choose a distinct nonzero allocation for the initial write window at serial one.
   Fund the initial ordinary window from `retained_data_bytes - maintenance_bytes`.
   Ordinary appends cannot spend the maintenance portion.
   Checkpoint progress uses three maintenance records for the retained previous upload and an in-flight boundary and upload.
   Fund their maximum framed charges, including references, plus headroom for fenced receiver settlements that can accumulate before trim.
   A replacement writer takes its term from the journal grant and appends no term claim.
   The application frame limit must fit the largest permitted payload plus authenticated request and receipt framing.
   Fund ordinary replay holds for open cursor intervals and overlapping reads.
   Logkeeper reserves four additional paid control slots for startup suffix and root holds across one replacement.
   Leave enough paid headroom to append a flush marker while earlier replay users retain their holds.
   Set `serving_lease` and `clock_drift` in milliseconds.
   A worker serves bytes only within its renewed lease, and a withdrawn worker's holds become disposable once journal time passes its last lease end plus `clock_drift`.
   Choose `clock_drift` to bound clock rate error between the voters' proposer clocks and the workers' monotonic clocks over one lease.
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
- For SQL erasure, `custody_work` supplies the broker domain UUID and the path to the enrolled PKCS#8 `admission_key`. The installed custodian also supplies `erasure_signing_key` with the separate PKCS#8 credential that matches the root's broker-enrolled evidence key. Provide the custody broker route and its privileged token to the custodian.
- Optionally, `journal_window_bytes` to cap each automatically replenished append window. The actual funded amount is bounded by paid, currently reusable voter space.

A gateway entry includes `name`, the exact `deployment` UUID, a `journal` scope with routing authority and credential paths, and the same enrolled worker endpoints.
Native PGWire and ordinary Flight SQL follow the journal route, authenticate worker readiness, and compare it with the current grant.
A Flight session must reconnect when the journal orders a different grant.

The installed Logkeeper adapter currently supports the writer-local staging deadline.
The physical receiver requires timed original quorum durability and refuses accepting writes on this adapter.

### Owned Flight reads

An installed catalog hold protects an exact checkpoint root and the suffix ending at the captured catalog cut.
The worker checks the returned scope against those coordinates before it exposes a handle.

| Action | Admission and lifetime on an installed route |
| --- | --- |
| `OpenRead.v3` | Reserve the owned read's ordered present, confirm with a fresh `Barrier`, and use `ProtectCheckpoint` for the exact catalog at the reservation. If apply has already advanced past that catalog, retry with a new reservation. |
| `OpenFreshRead.v2` | Order a fresh `Barrier`, apply through its cut, and use `ProtectCheckpoint` for that exact catalog. If apply has already advanced, take a fresh barrier while retaining the caller's supplied transaction floor. |
| `PrepareAppliedRead.v3` | On an audience session, check the supplied transaction floor against the writer's applied catalog, admit its checkpoint and suffix with `ProtectCheckpoint`, and retain the preparation until session close or expiry. Each subsequent open checks the attachment and the prepared lower cut and acquires its own hold. |
| `OpenAppliedRead.v5` | Capture one already applied writer catalog covering the supplied exact transaction floor and admit its checkpoint and suffix with `ProtectCheckpoint`. Audience opens require a live preparation and a newer read generation. |
| `OpenWorldRead.v6` and world `ReadHeld*` | World reads require custody of the world and every retained parent. The installed route returns `rtdb.owned/ordered-world-unheld` on open because it cannot acquire that set as one exact basis. `ProtectRoot` retains one root. |
| `ReadHeld*` | Use the handle's protected catalog under its attachment and lease. The action set remains limited by the handle's ordinary or applied admission and by the audience projection. |
| `CloseRead.v1` | Retire the handle and send `Release` for its exact hold when the last user and its admitted I/O drain. |
| `CloseSession` | Retire the session and its handles. The preparation and read holds release independently after their last users drain. |

`ProtectRange` protects the replay of a bounded record interval.
An owned catalog also needs the root and its dependencies.
The PGWire `rtdb.read=snapshot` connection selector returns `rtdb.multi/ordered-pgwire-checkpoint-unheld` until the connection can carry a frozen, protected checkpoint.

A lost barrier reply triggers `ReleaseRead` with the original invocation identity.
Settlement tracks uncertain invocations through their retry horizon.
An exact protection receipt can be reconciled by retrying its original mutation identity.
A lost Flight open reply can leave a handle until close or expiry, and an audience open with a new generation retires the previous handle.
A lost preparation reply may require a new audience session.
Each read retry is a new admission under D01 at the same requested basis or floor.

For an installed audience, configure the existing audience listener and reader-seat headers.
The gateway resolves the ordered holder and dials its configured audience Flight port on the enrolled endpoint's host.
The node compares the exact ordered grant with its current leading attachment and checks the monotonic serving lease.
The durable reader-seat claim still requires the DynamoDB seat store and the installed deployment reservation.
The installed audience path can omit `--audience-journal-port`; classic audiences use that port and their dedicated directory read transport.

## Observe recovery and maintenance

A worker obtains a grant against the last observed writer state. It opens the exact checkpoint and protected suffix named by that decision, waits until replay has applied through the granted cut, and confirms the grant before reporting readiness. Old file writes remain scoped to their grant-specific serving prefix.
A protected replica delivery retains its journal hold while resolving an offloaded record.
The worker reads that object from the exact writer epoch recorded by Logkeeper and checks the chosen payload digest before using it.

The custodian obtains an ordered checkpoint preparation before copying data. The preparation keeps the exact source cut and its base alive. A replacement can use the completed provider manifest or continue the copy from the original immutable serving prefix.
With no prior preparation, the custodian can discover a chosen native boundary and upload, verify the original catalog, and prepare its provider copy independently of the writer grant.
The scan is tied to the exact selected base and the newest chosen maintenance cut at the start of inspection.
A changed base invalidates the scan, and staging still requires the new ordered preparation.
An accepted block upload carries the digest of its exact encoded catalog.
Installed recovery requires that digest before adopting or confirming the catalog.
Before staging a new copy, the custodian reads and decodes the chosen native block boundary under the exact preparation.
Publication and trim use the custodian identity independently of the writer grant.
Trim names the exact currently selected reconstructable root and retains every suffix still held by an overlapping read or attachment.
Its receipt names the old floor and the resulting floor and totals the encoded bytes released by append class.
Ordinary windows replenish from the reusable voter space reported by the installed journal.
On ordinary capacity pressure, the driver pauses ordinary intake, preserves the previously accepted source prefix, and uses the maintenance lane for block boundaries, uploads, and fenced terminals that settle inherited receiver slots.
The custodian queries the exact candidate cut and orders preparation without borrowing an ordinary read hold.

An installed driver promotes after an empty replica read and a positive check of the current grant.
It starts the successor of the highest observed replica term at sequence zero.
Promotion adds no claim to the replica log.
The enrolled RTDB worker classifies typed replica records before sealing them, under its TLS credential.

Progress assumes a reachable installed quorum, a live custodian and provider namespace, a funded checkpoint allocation, and eventual release or verified disposition of overlapping holds.
Study 111 budgets three checkpoint records across any number of replacements, covering the retained upload plus the next boundary and its upload.
A successor settles an inherited receiver slot before cutting the next boundary, even under ordinary append pressure.
The extended accounting adds one record's worth of headroom per retained fenced settlement or open receiver slot in the funded interval.
The driver admits one open slot at a time, and successive replacements can leave multiple settlements retained before trim.
Apply the bound where the receiver budget covers those obligations and effective trim removes the settlements and leaves one retained upload.
The receiver budget is a funding premise for progress, with deployment-specific sizing and admission still to qualify.
The [maintenance classifier proof](../../verification/verus/maintenance_class/README.md) checks the contribution of each append to that bound.

Local checkpoint staging uses the same shared allocation lock as readers and publishes each encrypted object by immutable creation.
A custodian that reuses another custodian's object or discovers its staged metadata synchronizes the file and directory entries before using it as durability evidence.

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

Certified SQL apply records the first committed erase with the broker.
Ordinary checkpoint publication preserves that marker.
An installed custodian automatically stages a successor when its exact rotation allocation is funded and the broker can authenticate the complete enrolled cohort.
For an installed fork, run `custody-enroll-installed-fork` against a custody node with both installed routes and the original evidence credentials.
The custodian opens both selected provider checkpoints and authenticates every inherited origin in the child's first allocation against the parent checkpoint.
Both enrolled evidence keys sign the exact placement cuts and the same event inventory digest.
Enroll the edge while both original fork checkpoints are selected and the domain is open.
After admission closes, every enrolled installed writer signs all local erase selectors at its certified head.
The broker waits for every signed cut, including roots with an empty local selection.
It projects the union of the other roots' selectors through authenticated fork maps.
Staging checks the local selectors against the exact head being rewritten and removes the projected inherited origins.
Every enrolled installed root must report a sealed successor bound to the same selection before any root requests rotation.
A directory-only root needs an authenticated ordered handoff before it can participate.
The handoff must fence the Directory writer and certify its exact recovery closure before selecting an installed route.
Its original key inventories and admitted custody stay in the broker's domain until their original authorities confirm disposition.
The installed custodian's erasure coordinator accepts an operation identity, custody domain and exact incumbent grant.
Its custody adapter quiesces the incumbent writer, including unfinished appends and offloaded writes, and reconciles outstanding copy admissions.
The adapter retains that fence through restart until the attempt has a durable resolution.
Ordinary checkpoint maintenance must leave the erasure preparation and its provider allocation to this authority.

The adapter orders a preparation and stages a clean successor at the quiesced journal head.
The coordinator provisions the next journal key and checks the whole cut, including its history fingerprint, against the current head.
It checks the preparation against the selected base, cut, incumbent grant, root and allocation before reopening the exact successor through the provider.
An ordinary checkpoint ends at the native block boundary before the upload record.
A clean successor must account for the upload and any later records through the quiesced head.
Its recovery metadata must let replay start at that head and recognize the selected catalog as confirmed without reopening older catalogs or records.

The producer must preserve surviving history and rebuild or remove every primary trie, retained replay input, index artefact and metadata object that carries the targeted history.
The immutable provider manifest must cover the clean recovery closure, including confirmation evidence.
A replacement opens that manifest under the exact chosen checkpoint and reconciles ambiguous staging outcomes against the same allocation.
Provider closure verification establishes availability and integrity.
The producer is responsible for determining which objects contain the targeted history and ensuring their exclusion.

The custodian sends Logkeeper's `PublishAndRotate { grant, checkpoint }` using the request identity retained with the erasure preparation. Its certified decision selects the clean root, rotates the payload key and closes prior root and replay admissions together.
The accepted result gives the next writer epoch and key generation and a held recovery basis with an empty suffix at the checkpoint cut.
An intervening append changes the head and the journal refuses rotation.
A retry of an uncertain outcome reuses the exact command and its original receipt horizon.
If that horizon is exhausted, custody must reconcile the operation before admitting a fresh attempt.

The coordinator verifies the receipt and its signed evidence of both durable slot readbacks from each voter in the accepting quorum.
An old view may continue work under its original lease. Existing decoded data cannot renew delivery beyond the lease that authorized its provider read.
The custody authority must persist the exact destruction obligations and later record completed drain and physical destruction evidence.
An old view requiring a revoked key receives `rtdb/erased-under-view`.
A fixed deadline requires an authority that bounds the drain and original-provider destruction under its installed clock and fault premises. The current provider profile retains the 120-second operational target.
The coordinator authenticates a fixed reservation before requesting rotation. The reservation binds the prepared attempt and retiring inventory. The independently authenticated commitment adds the exact ordered switch. Destruction evidence carries the drain time and provider disposition, and can report a missed fixed deadline.

The coordinator fixture supplies a staged root and a custody commit. Logkeeper's separate voter test exercises torn-slot recovery and authenticated slot evidence. Production SQL integration still needs the survivor producer, custody-wide working plaintext and egress drain, and the provider deadline and destruction authority.

### Queue reclamation inside the current writer's prefix

Installed serving storage records a delete request for an existing object owned by the current writer as a create-only candidate under `STORAGE_PREFIX/~ordered/GENERATION/EPOCH/~reclaim/v1/`.
Each record names the grant, storage namespace fingerprint, cell-relative object key and observed provider version when available.
Retries for the same observation converge on one record.
An inherited checkpoint member stays under its original checkpoint custody.
Block collection also submits offloaded payloads using the same block-age cutoff as checkpoint metadata.
The queue sits outside the database's storage root and retires with its writer prefix.

A successful request acknowledges the candidate and leaves the bytes available for reads and replay.
Installed trie receipt settlement requires an exact ordered withdrawal and qualified provider disposal.
Withdrawal serializes with new references and covers both crash-selectable checkpoint images, preparations, the typed closure of every retained suffix, and Logkeeper's replay, recovery, root and read holds.
An authenticated candidate whose exact version is absent settles even when a replacement is visible.
For an enrolled never-versioned provider, the matching retirement tombstone establishes completion.

Installed S3 serving storage enables the read-drain census for every worker and custodian.
Start both roles with census support when the cell first serves installed journals.
Each serving point read of an eligible trie file creates a unique durable registration before checking the object's admission fence.
The detached storage task removes its registration after conclusive provider completion, including when the caller has timed out.
Following the ordered withdrawal, the custodian closes physical read admission and checks the provider census before withdrawing the key or removing the exact publication.
A registration can outlast its lease.
An ambiguous provider result or a process loss can leave an orphaned registration.
Configure an alert on the custodian's WARN event `Provider read registration is blocking installed reclamation`.
The event includes the provider registration key and the cell-relative object key.
Reclamation continues to defer while any registration remains, regardless of its age.

The control prefix under the configured S3 cell root is `STORAGE_PREFIX/~ordered/GENERATION/EPOCH.lifecycle/.tmp/installed-read-drain/`.
Each object has a `closed` fence and registration keys under `reads/` inside its census directory.

- Serving roles, including the custodian when it reads a checkpoint source, need `s3:PutObject` for create-only writes and `s3:DeleteObject` for release on registration keys, plus `s3:GetObject` for `HeadObject` on fence keys. [Required S3 permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)
- The custodian also needs `s3:PutObject` on fence keys and `s3:ListBucket` on the bucket for complete `ListObjectsV2` scans of the registration prefixes.
- Qualify strongly consistent conditional creates, fence observations that return HTTP 404 for missing fences, and complete listings with the deployed credentials.
  S3 uses the caller's `s3:ListBucket` permission to distinguish a missing object from access denial for `HeadObject`. [S3 HeadObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
- Keep the control prefix outside bucket-wide and temporary-object lifecycle rules that expire or transition its objects.
  Preserve active and orphaned registrations, permanent fences, and any version or delete-marker history that determines their current visibility.
  Scope automatic expiration positively to other prefixes or tags. [S3 Lifecycle configuration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)

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
The Lake package in `verification/multi` and the Rust workspace use the same Logkeeper revision.
Run `lake build` from `verification/multi` to check the composition against that pinned revision and the directory's Lean toolchain.
`RecoveryComposition.lean` relates the exact decoded append selected by the voter model to the receipt retained by the RTDB trace, including across a modeled transfer of the exact predecessor image to a repaired cohort.
It also models publication and trim under custody authority.
Concrete wire decoding, durable media steps, provider evidence, and semantic fidelity of the database cut are separate refinement obligations.
The [X1 composition](../../verification/multi/compose/README.md) checks acknowledged transaction recovery and visibility under its named Lean hypotheses and records the corresponding production evidence and open refinements.
The fresh-read proofs cover the signed hold selection, exact catalog cut gate and captured query input.
The logical history ends at the chosen Barrier decision, and a catalog publication that has advanced past that cut requires a fresh admission.
The [installed append binding harness](../../verification/verus/installed_append_binding/README.md) checks the result guards, exact ciphertext selection, recorded key generation and detached decoder.
The Lean composition derives the barrier head and the preserved exact cut from the projected chosen prefix.
It derives the checkpoint and suffix equalities from typed execution witnesses that the native archive and replay driver must realize.

The standalone Logkeeper Verus harness includes the admission functions used by the production command reducer and compact-image decoder. It covers the checked debits, the trim gate, and the expiry and fingerprint decisions that govern exact reissue. The full codec round trip and the disk and network boundaries also need their own evidence.

The native acceptance source in `rust/crates/multi/tests/ordered_journal.rs` covers lost replies through authenticated journal sockets, and exercises worker replacement and resumption of a prepared checkpoint through RTDB's PGWire and Flight gateways. The linked Logkeeper source has a compact-image round trip at full ordinary capacity and a torn inactive-slot scenario. RTDB's S3 HTTP fixture checks that an unsuccessful conditional tombstone needs retry and exact confirmation. Live-provider qualification still needs to exercise the deployed IAM policy, enrollment, conditional writes, and allocation disposition.

Before a hosted cutover, exercise failed voter and worker replacement with the directory unavailable, ordinary bytes and reader slots full, and a stalled provider operation. Keep the provider usable for at least one completion sequence. Confirm that the surviving quorum retains the original write result and grants a new writer without additional voter allocation. Confirm that a fresh statement reaches the barrier cut, that a protected old read delays trim, and that writes reopen after its hold closes and the exact archived checkpoint becomes usable. Transfer to independently reserved successor media before counting another failure zone as survivable.
