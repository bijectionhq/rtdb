# Installed Logkeeper journals for RTDB Multi

An installed journal owns the writer roster and grant, append decisions, protected read cuts, checkpoint selection, and the replay floor for one deployment. Keep the logical deployment, incarnation, group and generation fixed during worker and voter replacement. The physical voter digest changes only during independently funded cohort repair.

Use a fresh deployment ID and Logkeeper placement for a cutover. An existing RTDB directory record and its log offsets do not certify a Logkeeper history. Route the installation through the installed manifest when the initial group, storage and keys are ready.

## Provision the journal and storage

1. Install an odd cohort of at least three Logkeeper voters in distinct failure zones. Give each voter an exclusive persistent volume and a distinct TLS certificate and receipt-signing key. The stage and selected image alternate between two preallocated slots on each volume. Size the volume for the two full slots, group metadata and filesystem overhead. The initialization command creates new media. Restarts use the serving command and recover the existing media.
2. Give each daemon its local manifest with the identical group definition, exact voters and client certificate pins. Configure separate client identities for the administrator, each enrolled worker, the checkpoint custodian and gateways. Give the gateway principal the `route_reader` role. Keep client TLS keys and the voters' signing keys in independently mounted secret files.
3. For an empty installation, choose a nonzero initial history fingerprint, an initial checkpoint with `next_lsn` zero, zero root, manifest and custody provider digests, and an allocation at revision one. Choose a distinct nonzero allocation for the initial write window at serial one. Its bytes must fit the retained-data limit. The application frame limit must fit the largest permitted payload plus authenticated request and receipt framing. Leave enough paid headroom to accommodate appends and a flush while earlier replay users retain their holds.
4. Initialize and run the journal with the same RTDB Multi image, or use the standalone Logkeeper CLI with its ordered daemon feature. The journal does not need directory connectivity.

   ```sh
   rtdb-multi initialize-ordered-journal --manifest /run/rtdb/voter.json
   rtdb-multi ordered-journal --manifest /run/rtdb/voter.json
   logkeeper ordered route /run/rtdb/voter.json DEPLOYMENT_UUID
   ```

5. Enroll each worker using `logkeeper ordered enroll CONTROL_JSON WORKER_UUID`. The control JSON contains the public `route` emitted above and the administrator's `credentials` paths. Record the returned worker UUID and serial in the voter client pin, the worker's route credential and each gateway endpoint. Roll the new client pins onto the existing voter media. Enrollment chooses a new serial in the journal and the certificate pin binds it to the intended worker.
6. Initialize the serving storage and vault using `rtdb-multi init-storage`. Use scoped S3 writes for the serving store. Provision a separate, enrolled terminal S3 namespace for checkpoint custody. It must preserve conditional-create semantics and its unversioned state for the lifetime of the installation. Qualify and enroll the archive using the existing terminal-storage procedure, then grant the custodian conditional replacement for terminal tombstones. Application copy credentials should permit only conditional creation. Bucket policy and lifecycle must preserve these tombstones. For a local archive, initialize the archive volume with its own storage namespace. Read its physical provider fingerprint with `rtdb-multi ordered-archive-identity --archive-root ARCHIVE --archive-prefix PREFIX`.
7. Prepare the RTDB identity and install the serving key. The command prints one `DeploymentRecord` for use in the node manifest.

   ```sh
   rtdb-multi prepare-ordered-deployment --route /run/rtdb/public-route.json \
     --name DATABASE --org ORGANIZATION --keys KEY_VAULT
   ```

## Install node and gateway routes

Set `--installed-journals /run/rtdb/node-placements.json` on each node and `--installed-journals /run/rtdb/gateway-placements.json` on the gateway. The hosted entrypoint also accepts the absolute mount path in `RTDB_INSTALLED_JOURNALS_FILE`. Continue to supply the cell secret used for RTDB control and gateway capabilities. A cell serving only installed journal placements can omit `--directory`. In a mixed cell, installed names are resolved from the authenticated journal and remaining names use the directory.

The manifests contain arrays. A node entry includes:

- `deployment` with the prepared RTDB deployment record.
- `worker` with the numeric RTDB `incarnation` and `log_epoch`, the public Logkeeper `route`, the worker's `credentials` paths, and an `authority` object whose `kind` is `worker` and whose `enrollment` matches the journal.
- `custody` with the same exact placement coordinates, a separate client certificate, and authority kind `custody`. Give the intended replacement workers access to this custody service so they can resume a staged checkpoint.
- `archive` with the independent terminal `CellStorage` configuration and `archive_prefix` beneath it.
- `endpoints` with each eligible `enrollment` and its exact `control` and `pgwire` addresses. The node's advertised addresses must match these entries.
- `checkpoint_reservations` with preassigned allocation UUIDs, the exact provider fingerprint in `custody.provider`, consecutive revisions starting after the installed checkpoint, and `usable_bytes` for each exact allocation. Fund peak old retention, the new copy and its metadata. Renew the reservation inventory before it runs out. Nodes and replacements must agree on an already prepared allocation.
- Optionally, `journal_window_bytes` to cap each automatically replenished append window. The actual funded amount is bounded by paid, currently reusable voter space.

A gateway entry includes `name`, the exact `deployment` UUID, a `journal` scope with routing authority and credential paths, and the same enrolled worker endpoints. Native PGWire and ordinary Flight SQL follow the journal route, authenticate worker readiness, and compare it with the current grant. A Flight session must reconnect when the journal orders a different grant. The ordered installation currently serves fresh reads on the holder. The explicit frozen-snapshot selector and audience Flight use separate recovery paths.

## Observe recovery and maintenance

A worker obtains a grant against the last observed writer state. It opens the exact checkpoint and protected suffix named by that decision, waits until replay has applied through the granted cut, and confirms the grant before reporting readiness. Old file writes remain scoped to their grant-specific serving prefix.

The custodian obtains an ordered checkpoint preparation before copying data. The preparation keeps the exact source cut and its base alive. A replacement can use the completed provider manifest or continue the copy from the original immutable serving prefix. Only a chosen reconstructable provider root advances the replay floor. Trim retains every suffix still held by an overlapping read or attachment. Paid journal windows replenish from reclaimed space in the voter slots.

Past checkpoint archives first close in journal order. Reads and disposal verify the original physical storage identity before using or releasing that custody. Closure requires every hold and preparation referencing that archive to finish. The terminal provider replaces each archived member with a permanent conditional tombstone and fences the manifest last. A retry can recognize that final fence after the original bytes are gone. Tombstone metadata remains as provider closure evidence. A local archive uses a shared filesystem with reliable cross-process file locks and durable directory synchronization. Archive readers retain a shared lock. Retirement takes an exclusive lock, records a permanent closed marker, and removes the allocation including its private scratch directory. Serving prefixes for old writer epochs remain separate from this archive retirement and need their own provider disposition.

A disconnected worker can leave holds behind. Do not withdraw a worker merely because a gateway stopped reaching it. Once operations confirms the exact enrollment's running processes and effect scope have permanently shut down, use `logkeeper ordered withdraw` followed by `logkeeper ordered dispose-worker` with the retained external disposition digest. Keep that evidence with the deployment's custody records.

## Repair the voter cohort

1. Create successor manifests that keep the same logical placement and identify the predecessor digest and its receipt-verification keys. Provision the new voter volumes and run `logkeeper ordered reserve-repair` on each successor to produce signed capacity tickets. The pending votes cannot serve the journal.
2. Collect a distinct target majority of tickets. From a reachable predecessor voter, use `logkeeper ordered seal-repair` with the target group definition and the tickets. The predecessor orders the exact successor and produces a signed transfer package for that selected image.
3. Deliver the package independently to the new voters and run `logkeeper ordered install-repair` on each funded volume. Start the successor from that media. A lost transfer reply can be recovered by reissuing the same source seal against the same destination.
4. Install or extend the authenticated application route chain with the exact successor digest and its endpoints. Keep the original issuer verification keys and include the new issuer keys. Reachable clients can follow the preinstalled successor when the predecessor is sealed or unreachable. Ordinary writer replacement keeps the existing cohort and never invokes this repair flow.
5. Record disposal of previous physical media separately. A successful repair establishes the exact installed continuation. Original unreachable storage remains a disposal obligation until the original storage owner can account for it.
