# RTDB v1 custody producer handoff

## Reviewed branch placement

`transact.custody-reviewed-placement` is available to the configured Original owner on routes that already admit native row custody.
The native transaction and release folds are shared by Default and Multi.
Installed writable Multi residences enable native row custody with the durable key vault and use the same feature set for single and batched decisions.
Default uses the native checkpoint and durable token custody attachment.
Both modes advertise reviewed placement to the authenticated Original owner after its provider mount confirms the current writer point and retained receipt.
An assigned follower and a restored attachment dependent on another custody authority have no ordinary row-custody capability.

1. Run a tracked, unpaged, exact merged Query on the writable source branch, with `custody_release_evidence` and `custody_review` selecting the group, winning occurrence and valid instant.
   `CustodyReadInventory.reviewed_source` reports the authenticated complete inventory digest and the Original and winning occurrence, including their physical authoring branches, ordinals and independently confirmed receipt keys, bases and clocks.
   The release token binds that source evidence to the same captured contributors, owner session and generation.
   An inherited winner can resolve through a retained ancestor after that ancestor drops.
2. Submit `ReserveReviewedCustodyPlacement` with the returned source cut and release token, the Original and selected occurrence commitments, and the exact destination, link, review and protected binary commitments.
   Put the configured Original writer's exact `Version` first in `requires` and include the tracked Query claim there.
   Set `window.before` no later than either the returned `proof_expires_at_micros` or the source branch expiry and pass `transact.custody-reviewed-placement` in `require`.
   Confirm `TxStatus` at the reservation key and query `xt.custody_placements` at that receipt's exact basis for the reviewed reserve key, inventory, review and reservation commitment.
3. Submit `LandCustodyBranch` as the final operation in the accepting arm under the same writer requirement and the application's review, publication and expiry requirements.
   Each entry supplies the exact prior reserve key and basis and its protected `PlaceCustodyRow` on `main`.
   Include any application receipt and proof rows earlier in the same arm.
   For an expiring branch, use `window.before` at or before its ordered expiry.
   The resolver checks the complete current inventory again with only that entry's own pending reservation excluded, verifies the source remains unchanged and requires an unoccupied destination.
4. Recover outcomes by key using `TxStatus`, including after the proof expires, the response is lost or the source branch drops.
   Match a `Then` receipt to `xt.custody_placements` and `xt.branch_lineage` at its exact `main` basis.
   Compare reserve and activation keys, source and review commitments, destination occurrence, validity, ordinal and digests, and the source drop basis.
   The existing custody group fence and release drain include the new placement and surviving inherited copies.

### Native reviewed placement backed by an Original assertion family

The reviewed producer and the Original-vault bridge have independent admission.
The planned steady-state Flight action is `rtdb.v1.OriginalFamilyLanding` and advertises `row-custody.original-family-landing` on the authenticated Original attachment after the native family authority is installed.
The reviewed producer can continue to advertise `transact.custody-reviewed-placement` independently.
The planned action and capability are currently gated.

`NativeFamilyLanding::from_placed_original` is the compatibility mount for an existing placed Original.
It binds the authenticated placed session, its exact directory writer, the existing assertion vault and a live native `MountedResponseRelease` on the same deployment.
The existing scoped successor grant and distinct reader and controller credentials continue to govern that compatibility route.
The native Original mount takes its authority from the installed v1 attachment, the current writer receipt and a positively bound family vault identity.
It has no directory dependency.

#### Native Original mount inputs

- Provide a stable family vault namespace, stage ownership and assertion line identity bound to the exact native database incarnation and mounted Original writer.
  Install the family provider under the existing authenticated custody owner and preserve that binding through restart and handoff.
- Enroll the family member and capability durably, then confirm the exact original native occurrence against its keyed writing receipt.
  Supply authenticated authorship and slot evidence for the original and winning rewrite, including the physical authoring branch when it differs from the review branch.
- Reuse `MountedResponseRelease` for the current writer, exact reviewed source and acceptance evidence, live group and native response and checkpoint-copy drain.
  The independent family reservation pins both stage identities before either can be sealed.
  Acceptance recovery by key confirms the exact applied placement, branch drop and paired application witnesses before activating the family binding.
- Supply a keyed, ordered Original family close with the exact elected writer, family, native group and family inventory.
  Include enrolled and pending stages in the close inventory.
  The family vault serializes closure with reservation, activation and admission.
  The native group revocation provides the separate outer occurrence fence.
- Install Original family release registration, promotion, cutoff and positive disposition under the generic custody surface.
  Opening requires the exact active family binding, a live mounted occurrence and the promoted Original family hold.
  Sending requires the existing fresh v1 native response admission for the exact part.
  Settlement requires the Original family release and checkpoint-copy terminals and the native group release and checkpoint-copy drain for their exact fences.

#### Reservation and acceptance

1. Reserve the Original family member and the winning native occurrence through the bridge before sealing either stage.
   Use the destination and link stage UUIDs for the native placement and link IDs.
   The reservation pins the review and its physical occurrence commitments, both native request keys, the target, the canonical application request digest and the exact receipt and proof cell locators.
   Supply the same Original and winning carrier bytes observed by the native review.
2. Seal the destination payload and link.
   Bind the SHA-256 of the byte-identical application receipt and proof cells with `bind_acceptance_body` before submitting the native reservation.
   The application body may contain the sealed link.
   `RootLandingTarget.receipt` commits the canonical landing request, while the bound body digest commits the eventual acceptance record.
3. Order the native reservation and landing with the paired receipt and proof writes.
   Call `activate` with the original acceptance key, including after a lost reply or branch drop.
   The bridge confirms both keyed `Then` receipts, both cells written at the accepting Basis, the exact Root placement and source drop.
   It recovers the opaque carrier and link from that exact occurrence, authenticates the frames and retains the same Root binding in the Original vault.
4. Pending stages grant no plaintext.
   Reuse the existing Original family registration and release manager with the same native mount on an activated binding.
   Original release admission and the final live hold check require both authorities.
5. After both fences, record the exact native group drain, which requires positive response and checkpoint-copy closure.
   The Original stage settlement still requires positive Original release and checkpoint-copy closure.
   Existing Original fence inventory includes the capabilities of a reservation whose activation never completes.

Protected exports remain subject to the managed checkpoint custody agreement.
The existing hosted assertion-family route continues to own its current consumers.

## Mount and delivery calls

1. An authenticated runtime host calls `ServerHandle::mount_original_custody_provider(database, CustodyProviderInstallation)` and retains the returned `CustodyProviderMount` for the original writer lifetime.
   The installation pins the exact original source and registry commitments, writer identity, and main-branch writer point.
   RTDB verifies the independently confirmed keyed writer receipt and current writer point before admitting the mount.
   A live attachment has one mount at a time.
2. Obtain `DatabaseCatalog::original_custody_response_owner(database)` and open `CustodyResponseOwner::response_release(clock, cancellation)` for the call.
   Use `MountedResponseRelease::capture_ranked(source)` before scoring or slicing.
   Its captured view retains every protected occurrence in the bounded native view, including earlier, inherited and shadowing inputs.
   A missing occurrence or oversized inventory closes this delivery route.
   The bound is 512 distinct `(group, occurrence)` pairs.
3. Register the exact response part, fixed cutoff, frame commitment and complete contributor inventory at the original recovery registry.
   Register the original carrier receipt for every occurrence, and stage then promote each matching release via the mounted owner.
4. `handoff_ranked(captured_views, registrations, frame, enqueue)` unions all ranked legs at the same cut and selected generation, checks that the same live mount captured each view, and checks that the registry binds the exact frame and complete set.
   The frame commitment is SHA-256 over `rtdb/v1/custody-response-frame\0` followed by the exact bytes the transport enqueues.
   It verifies every promoted registration, current writer, group keys and the captured Multi generation, then orders one admission for the whole part before synchronously calling the enqueue callback.
   The transport owns the exact one-use queue reservation and retains the supplied deadline through socket handoff.
5. Keep the original registry evidence until RTDB acknowledges the terminal for each occurrence.
   Use private cancellation only before promotion, or positive durable disposal from the original registry after the transport closes the exact part.
   Mount withdrawal and cutoff expiry close handoff, and an already ordered hold stays in the drain until its exact terminal.
   Dropping the mount or detaching its database invalidates previously acquired owners.

The separate Query and Changes wire paths continue to check the selected Multi generation for schema, batch and trailer frames.
Protected dependent restore or fork still needs an explicit destination custody binding.

## W10b agreement boundary

`OriginalCarrierObservations` is the trusted producer-side bridge that the runtime host must install.
W10b's `V1SourceFrontier` is process-local to Bijection today, so it still needs an authenticated read path from the RTDB host to the original frontier and response registry.
The common interface has three operations.

- `confirmed_writer(authority)` returns the `confirmed` writer's request key, opaque receipt Basis bytes and exact system time, under the pinned original installation and registry.
  RTDB verifies the keyed receipt and compares its Basis to the current installed writer point.
  The provider decides whether a pending claim can still authenticate that original writer.
- `registered_carrier(registration)` returns the exact registered occurrence and carrier receipt triplet and a live observation or the immutable positive disposal commitment.
  Source history, original installation and registry stay distinct from the transport history, producer, consumer, response ID and ordinal.
  The provider must authenticate the exact source and transport ownership, and expiry or missing state cannot be substituted for disposal evidence.
- `registered_response(authority, part)` reads the live original response part, its immutable cutoff, exact frame commitment and complete contributor set in one authenticated registry observation.
  Include all distinct occurrences that contributed, including non-hit text statistics, vector candidates, protected shadows and hybrid results with zero hits.
  The native captured set can include rows outside the final page and remains bound to its original view.
  The legacy rank inventory still uses hosted assertion-family coordinates and needs an explicit bridge to generic placement commitments before its consumers can move over.

The RTDB host owns provider mount and lookup, with the authenticated cross-process bridge below.
W10b owns the Bijection endpoint, the caller and its mapping for legacy, archived and scoped contributors and protected index bytes.
The same original writer attachment can recover an ordered terminal after loss of the transport registry row.
New admissions require a live original registry observation.

## Final qualification source cases

Run `native_original_custody_mount_rechecks_the_writer_and_withdraws_old_owners`, `registered_frame_rejects_omitted_contributors_and_transport_substitution`, and `ranked_capture_retains_inherited_and_shadowing_custody_groups_before_slicing` with the earlier custody and source-intake regressions.
Run `configured_custody_query_mounts_only_for_the_credentialed_owner`, `trusted_login_has_no_verified_custody_credential`, `provider_attestation_is_bound_to_request_challenge_and_payload`, and `custody_read_proof_preserves_the_full_inventory_and_selected_generation`.
Run `custody_admission_orders_the_whole_part_and_preserves_its_drain_after_restart` and `trusted_source_release_reopens_only_with_its_canonical_request_and_private_namespace`.
Exercise both drivers with a mounted live provider, distinct contributing groups on a zero-hit rank, provider or attachment withdrawal at enqueue, selected-generation destruction after capture, and writer displacement during an ordered response transition.
Use a writer replacement whose valid interval leaves the former writer visible at its configured point.
Recover a promoted terminal after reply loss and registry removal, and confirm protected export remains refused pending destination custody binding.
Delay a Flight Check reply across revocation and verify that it cannot enqueue and that a subsequent Admit is refused.
Lose a fresh Admit reply, checkpoint and restart, and recover the admission and its exact positive terminal without recreating the transport reservation.
Race Admit with revocation of either of two promoted contributors and with disposal of one contributor.
Confirm a different frame, reduced contributor set or changed cutoff on the same part cannot get a second admission.
No builds, tests, code generation or benchmarks ran during this source pass.

## Cross-process v1 contract for W10b

W10b owns the Bijection endpoint and caller on its W10 branch from `71384decd44`.
RTDB owns the configured HTTPS client, provider signature verification, attachment mount and private owner Flight action.
This contract uses JSON with unsigned byte arrays for fixed-width binary identifiers and opaque Basis bytes.
All objects reject extra keys and malformed widths.

### Original provider HTTPS endpoint

RTDB sends a POST to the exact configured HTTPS endpoint with `Authorization: Bearer <dedicated provider credential>` and `Content-Type: application/json`.
The endpoint must authenticate the credential for the exact original authority and read the independent frontier and original registry at a current durable barrier.
Each request has a fresh lower-case 32-hex-digit `challenge` and an `operation`.
`writer` adds `authority`, `carrier` adds `registration`, and `response` adds `authority` and `part`.

For a successful observation, return HTTP 200 with `{"payload_hex":"...","signature_hex":"..."}`.
The payload is the exact UTF-8 JSON bytes encoded in lower-case hex.
The 64-byte Ed25519 signature covers the bytes `rtdb/custody/provider/v1\0`, then SHA-256 of the exact raw request body, then SHA-256 of the exact decoded payload bytes, in that order.
RTDB verifies the detached signature with the configured 32-byte provider public key before interpreting the payload.
The challenge and exact request hash prevent replay across visits, operations and authorities.
The response body limit is 512 KiB.

- `writer` payload is `{"authority": authority, "receipt": receipt}`.
  The receipt carries `key`, `basis`, and `system_time_micros` and comes from the original independently confirmed writer frontier.
- `carrier` payload is `{"registration": registration, "receipt": receipt, "state":"live"}` or the same structure with `"state":"disposed"` and required `evidence_sha256`.
  The registry entry must match its full exact registration and original creator.
  Disposal must be positive immutable evidence that the original transport part has closed.
- `response` payload is `{"authority": authority, "part": part, "expires_at": micros, "frame_sha256": digest, "contributors": [[group,occurrence],...], "read_proofs_sha256": digest}`.
  For Flight the proof digest is required and observes the exact list registered for that frame.
  It is SHA-256 over `rtdb/v1/custody-read-proofs\0`, the big-endian u16 list count, then each opaque proof prefixed by its big-endian u64 byte length, in list order.
  In-process handoff uses its captured view directly and omits the proof digest.
  Return the live exact carrier part with the full distinct protected contributor set, including dependencies that never appear in the frame.
  The maximum is 512 contributors.

`authority` has `incarnation` (16), `source_history` (32), `installation` (16), `registry` (32), and `original_writer` (32).
`original_writer` is SHA-256 over `rtdb/v1/custody-original-writer\0`, the installation bytes and the confirmed writer request key.
`part` has `transport_history`, `producer`, `consumer`, `response` (16 each) and positive `ordinal`.
`registration` has `authority`, `group` (16), `original` (32), `occurrence` (32), `carrier_receipt_sha256` (32), `part` and `expires_at` in microseconds.
The carrier receipt commitment is SHA-256 over `rtdb/v1/custody-carrier-receipt\0`, key byte length as big-endian u64, key bytes, the opaque Basis bytes, and system time as big-endian signed i64 microseconds.

### Private Flight action

Use `rtdb.custody.v1.Release` on the authenticated Flight session for the configured original database and dedicated hosted owner.
It is a private JSON action alongside the ordinary protobuf APIs.
`prepare`, `stage`, `promote`, `cancel`, and `dispose` requests are `{"operation":"…","registration": registration}`.
`prepare` returns `{"registration": registration}`.
`stage`, `promote` and the producer-side `check` preflight return `{"ok":true}`.
`cancel` and `dispose` return `{"state":"cancelled"}` or `{"state":"disposed","evidence_sha256":[...]}` from the retained ordered terminal.
`drained` accepts `authority`, `group`, opaque `revoked_at` Basis and `inventory_sha256` and returns `{"drained":bool}`.

`check` and `admit` accept `registrations`, `frame_sha256` and `read_proofs` in addition to `operation`.
For native protected reads the query flag `custody_release_evidence` requests a sealed `custody_release_proof` in the Query trailer.
Preserve those opaque bytes on the same owner Flight session and original provider mount through the preflight.
Submit one to eight proofs from the same selected Basis and data generation, and register their exact ordered digest in the original response manifest.
The request body limit is 2 MiB.
The new custody proof attests the producer's complete protected occurrences and captured key generation.
The `930e66bec7` receiver clock proof remains separate and supplies no disposal or custody authorization.
A successful `check` reports the state at RTDB's barrier and grants no enqueue right.
`admit` verifies the same exact original manifest, then orders one private receipt for the whole response part under its original writer and immutable cutoff.
The ordered fold requires every exact contributor's promoted hold and live group at that decision.
RTDB sorts registrations by `(group, occurrence)` for the receipt and retains the read proof list in its original digest order.
The receipt key is unique to the response part on the original database, and the canonical request commitment binds the writer, selected cut and generation, exact frame, proof-list digest and all registrations.
The same key is retained with its immutable receipt through checkpoint and trim.
One successful promotion per contributor already contributes to the durable drain, so ordering admission keeps those holds counted without acquiring a second set of holds.

#### W10b transport reservation

Before sending `admit`, durably register the exact part, all contributing carrier registrations, frame and proof-list commitments, and fixed cutoff at the original registry.
Stage the exact frame bytes in one blocked, in-memory transport slot for that part and open connection, with the earlier of its local monotonic bound and immutable registered cutoff.
Only `{"state":"admitted","basis":[...]}` from that slot's original in-flight call may open the still-pending slot once before its cutoff.
The `basis` is the opaque ordered admission receipt Basis.
`{"state":"recorded","basis":[...]}` identifies an exact replay that reached the retained receipt while the live preflight still passed.
Use replay, refusal, interruption, a missing result or invalid result for cleanup only.
After disconnect, timeout, lost reply or restart, close that exact reservation and recover using its original carrier registrations and positive disposal evidence.
Persist the recovery coordinates and disposal evidence, with the blocked slot and its release authority confined to the original process and connection.
Never create a replacement reservation or send from a retry, a former process, a delayed Check reply, or an Admit reply for a closed slot.
An admission ordered before revocation remains a pre-fence send in the drain even if its reply was lost.
An Admit request ordered after revocation of any contributor cannot accept the part.
Record the positive terminal once the original transport has closed the part and can no longer send it, then retry `dispose` for each exact registration until RTDB acknowledges every terminal.
Keep W10b delivery and the RTDB custody capability gated until this producer contract and both drivers qualify together.

### RTDB host activation

Configure one `flightSql.custodyProviders` entry per original database on the ordinary listener and use the existing `flightSql.hostedControls.owner` principal.
That principal must use validated password or client credentials, with write access to the selected original database.
An anonymous, trust-only, scoped, fresh-replica or audience session cannot mount or use the provider.
Ordinary `Transact` and Query requests never mount a provider.
The owner-only action or explicitly requested custody read proof lazily mounts the configured HTTPS provider after checking its independently confirmed writer receipt and current writer point.
The route retains the mount for the live attachment and verifies a replacement attachment as a new mount.
A writer identity change requires updating the host configuration to the newly confirmed writer.
Supply the pinned writer request key commitment at process launch and reuse the exact prepared writer claim through initial confirmation.
For a successor, the configured writer point must select the successor's written version once it is current.
The ordered phase checks both the configured valid instant and the current decision time while preserving the original canonical release request bytes.
Standalone Multi uses `--owned-flight --flight-control-relations LIST --custody-provider-config FILE` with its existing isolated owner listener.
That file contains the same `custodyProviders` list at its root, with one entry for the original standalone database.

The configuration entry has `database`, `endpoint`, `bearerToken`, `providerPublicKey`, `incarnation`, `sourceHistory`, `installation`, `registry`, `originalWriter`, `writerTable`, `writerId` and `writerValidAtMicros`.
Use `!Env` for the dedicated bearer credential and lowercase hexadecimal for the public key and authority identifiers.
`writerTable` is `schema.table`, `writerId` is the exact string row identity, and `writerValidAtMicros` selects its stable valid instant.
Keep the ordinary Flight connection on the existing authenticated host transport and configure the provider endpoint as HTTPS.
The provider client declines redirects, uses a five-second request timeout and validates TLS plus the configured response signing key.

The first Query proof fields are `custody_release_evidence` at request tag 7 and `custody_release_proof` at trailer tag 6.
A proof is issued only for an unscoped native view with complete placement evidence and uses that view's selected key generation.
The cross-process bridge and action remain private while the active W10b consumers and protected reader coverage are incomplete.

### Default canonical delivery

The original provider identifies the unscoped v1 writer and carrier receipts by their exact request keys, opaque Basis and system time.
Default verifies each receipt's canonical source, replica and term installation in that namespace before matching the original carrier occurrence in the v1 fold.
Release phase and admission receipts stay in the private custody release namespace.
The Default Classic mirror serves its public projection and contributes no protected inventory, installed receipt proof or delivery generation.

A protected read or response admission carries the held native view's exact sealed checkpoint generation.
Default checks that generation against the checkpoint key authority at delivery and around ordered response admission.
If a survivor adopts a new generation, only the generation already held by an old view can authorize that view.
An inline view with protected contributors has no release proof and refuses delivery.
Qualify a sealed checkpoint selection or verified survivor adoption before serving protected data from a newly attached Default database.
Run `physical_receipt_follows_the_original_source_and_the_settled_replica`, `previously_persisted_native_rows_keep_their_exact_basis_without_a_physical_witness`, and the mounted owner test's held-generation retirement case.
The Default feature and source admission gates stay closed for the joint qualification of the source projection, protected readers, provider drain and erasure.

The Default checkpoint integration at `82af9f61db` includes the sealed object identity and the authenticated selected-checkpoint repair path.
Qualification must cover the crate-visible checkpoint bytes accessor and the pending-erasure refusal on ordinary reopen.

### Protected Multi checkpoint transfer

`PinnedExport::checkpoint_identity()` provides the verified v1 Basis, pin UUID bytes, exact manifest identity and ordered pin lifetime.
The Basis includes the source v1 incarnation.
The protected transfer requires a pin without a deadline so expiry cannot skip the positive copy drain.
`FamilyTransferSource` additionally pins the original hosted deployment, custody incarnation, source history, installation and writer commitment.
The destination binds a different deployment and v1 incarnation plus the immutable preparation commitment of its trusted installed owner.

`FamilyTransferAuthority::source_export` must independently resolve the complete protected contributor census of that exact live export and authenticate the agreement for the chosen destination.
`destination_custody` must independently resolve the same agreement from the destination's durable reverse binding.
The inventory includes all original occurrences and selected derived rows, with the original capabilities and SHA-256 of each exact opaque envelope.
The contract orders contributors by family UUID, original position and assertion ID, and orders each derived list by its `DerivedRow` identity.
The v1 digests cover the corresponding domain string in `transfer.rs` followed by the typed compact JSON bytes in declaration order.
The original vault addresses the record by source deployment, v1 incarnation and pin so a replacement cut, manifest, inventory or destination cannot use the same pin identity.

`prepare_checkpoint_transfer` confirms both observations and enrolls the same forward entry in every known contributing family before marking the reciprocal agreement bound.
It retains the exact agreement across restart and commits each forward entry in the same family CAS as the fence inventory.
Already bound state can replay its acknowledgement without providing a read grant.
An unknown family, a missing reverse agreement or a partial preparation cannot open the destination.

`record_checkpoint_transfer_drain` retains either positive proof of installed durable destination backing and closed destination copy workers or positive discard evidence for the exact transfer.
`source_copy_drain` must independently verify that the source closed all of its copy activity under the same agreement and destination terminal before the vault records drain.
`require_checkpoint_transfer_drained` consults that retained terminal before source pin release, including after reply loss.
The original family drain also waits on these transfer terminals while preserving the existing response hold drain.
An installed destination stays in the original family fanout after the export has drained.
`require_checkpoint_transfer_read` checks the exact original or derived envelope, current reciprocal installed binding, all forward entries and the original position and family fences.
An installed original reader also attaches the contributing family markers to the existing plaintext delivery scope.
Use it around opening and at the final held delivery barrier with the original payload capability and response hold.

`ManagedFamilyTransfer` implements the host-only `rtdb_node::ManagedCheckpointProvider` on the existing independent Local or S3 family vault.
The source obtains a non-expiring `RTEX2` pin through `MultiServing::pin_managed_checkpoint` after the managed catalog reserves the destination.
Its sealed manifest commits the pin UUID, native source cut, private destination incarnation and stable catalog request digest.
The catalog must bind the exact manifest while the destination is `TransferPending` and retain the resulting control-row Basis as `ManagedCheckpointReservation.reserved_at` across retries and readiness.
Construct `ManagedCheckpointReservation` from the pinned identity, reserved destination incarnation and request digest, and that bound control Basis.
The trusted host supplies the authenticated source's `OriginalAssertionFamilyReader`, original custody incarnation and independent vault to `ManagedFamilyTransfer::new`.
The original reader must resolve the same installed deployment as the native source Basis incarnation and report its hosted installation, owner namespace and writer.
The managed pin and provider are internal host interfaces with no customer codec or unconditional feature advertisement.

The provider claims one durable copy worker before inspecting the retained export.
It traverses every retained image, including historical and shadowed images, and authenticates each enrolled main-world `BJWC1` original and registered `BJWP1` derivative against its exact original capability, occurrence and row slot.
It hashes the entire opaque carrier, preserves all family contributors, and seals the exact census in the destination reverse record before enrolling all original forward entries.
The source and destination copy drains attest the same applied destination restore terminal and retained-image commitment.
The sole copy worker finishes reading the source before appending that terminal, which allows `recover_installation` to reconstruct a missing reverse terminal after restart or a lost append reply.
Call installation or recovery before marking the destination ready, and call `release_source` on the source writer only after both drains are retained.
Without an applied restore terminal, an interrupted or abandoned claim stays private and pinned until an authenticated copy abandonment or writer-fenced resumption is implemented.

Protected readers call `ManagedCheckpointProvider::require_read` with the selected destination generation, exact branch, table, row ID and outer ciphertext inside a `PlaintextView` delivery scope.
It authenticates the physical slot and original capability, checks the complete vault agreement, and attaches all original contributor fences and the selected destination generation to the final delivery checks.
The consumer still owns protected opening and the existing ordered response admission and durable release for the output.
The installed v1 Multi host exposes the transfer to trusted in-process callers through `DeploymentTransfer::ManagedFork`.
The existing Flight administration codecs select the ordinary restore route and let the installed Multi host choose custody for a fork based on the source inventory.
Where the legacy placed Original still exists, construct `ManagedFamilyTransferSource::from_placed_original` with its authenticated owner session, current directory record and existing independent vault.
Install that source through `MultiRuntime::with_managed_checkpoint_source` for the matching v1 incarnation on each participating host before opening the server.
The stock `rtdb start-multi` process mounts a native custody provider backed by the installed source journal and `CustodyVault`.
Legacy placed Original custody still requires the authenticated reader and its independent family vault in the hosting process.
The ordered catalog checks the exact private reservation at the selected writer for pin, install and release, and preserves the manifest-binding Basis after readiness.
The destination confirms the reciprocal provider terminal before the source writer releases its pin, and readiness follows the positive release acknowledgement.
A lost reply after the applied restore or source release recovers the same backing and terminal without repinning.
A trusted reader uses `Node::check_managed_checkpoint_read` in its `PlaintextView` with the already held carrier slot and destination generation before opening through the Original capability.
Delivery rechecks the attachment, destination generation and tracked Original contributor fences after opening, and the transport still requires its existing ordered hold and release.
For a source with native groups, the authorized stock Flight Fork chooses the private custody route and requires a pin without a deadline.
The native provider accepts the settled source head with exact positioned carrier receipts and a complete live group inventory.
The retained group state includes original occurrences, inactive reservations, activated placements and response release phases under the source incarnation.
Group key custody and original receipt lookup remain on the source.
Ordinary `BranchIndex::checkpoint_at` and `restore_in` still refuse custody groups.

The active source writer enrolls the full native inventory and one copy owner before the destination reads the pinned export.
The enrollment joins each contributing group's revocation inventory and prevents copy drain from being inferred from missing state.
The destination writes the reciprocal enrollment into its sealed restore checkpoint and retains the original occurrence coordinates as it changes the destination row coordinates.
The source writer requires a verified destination checkpoint before recording the native copy drain and releasing the pin.
The same destination terminal can be recovered from its applied checkpoint after a lost restore reply, and source recovery retains the forward enrollment after unpin and trim.
An interrupted copy claim with no applied destination terminal leaves the destination private and the pin held.

The installed native provider checks the exact inherited slot and outer ciphertext with the destination generation and live original contributor groups in the existing private read check.
Inherited deployments refuse ordinary Query, Changes, transaction admission and onward fork while the original response owner remains the authority for opening and release.
General `ROW_CUSTODY` advertisement still depends on the producer and reader qualification.
Uncatalogued owned payloads, rewritten world members, external checkpoint files, and governed Root carriers remain closed.
Root-derived rows need an authenticated dependent erasure projection and settlement before inherited reads can open.
`FamilyFence.transfers` exposes the exact forward roster to that projection, while the installed Flight fence reply retains its existing shape.

Qualify `managed_checkpoint_pin_is_exclusive_and_restores_after_a_lost_reply`, `census_accepts_only_exact_world_original_and_derived_carrier_forms`, `retained_frames_require_their_own_live_stage`, `checkpoint_transfer_replays_exact_custody_and_keeps_every_family_in_the_fence`, `checkpoint_transfer_refuses_unknown_or_partial_bindings_and_recovers_discard`, and `exact_export_pins_the_selected_branch_ancestry_after_trim`.
Qualify `managed_catalog_waits_for_confirmation_and_drains_and_recovers_a_lost_unpin_reply` for the private catalog and mounted destination attachment gates.
Qualify `native_checkpoint_keeps_original_occurrences_and_fences_reads_until_the_reciprocal_drain` for the native forward enrollment, reciprocal restore, unpin and original revocation fence.
At the installed boundary, lose replies after the source pin, destination append, reverse-vault terminal and source unpin, and recover the same terminal from trimmed history.
Exercise multiple original families and ranked derivatives, an unknown group or missing source carrier, a substituted row slot, wrong source and destination incarnations, a changed reservation Basis, and a concurrent fence of each contributing family.
Rotate the destination generation between protected opening and delivery, and verify that late replies cannot admit a send, that settled send evidence survives restart, and that ordinary export and unpin cannot bypass the managed fence.

## Scoped v1 reads and exact holds

The credential issuer may include `protected_reads: true` inside `rtdb_label_scope`.
On the configured Original attachment, `row-visibility.custody-release` allows `custody_release_evidence` in Query and Changes.
The trailer carries a single opaque `custody_release_proof` for the same projected input, including a complete empty inventory.
A scoped Query trailer omits `custody_release_inventory`.
A Query conservatively captures the complete protected history of the scoped input view before SQL evaluation and pagination.
Changes captures only the selected, live before and after images in the page.
The handles are local to the serving attachment and expire within 120 seconds.

Only the configured Original owner may call `rtdb.custody.v1.Release` with the scoped operations.
Send `resolve_scoped` with `read_proofs` to check and recover the actual Original contributor registrations' coordinates.
For a nested protected carrier, send `resolve_scoped_occurrences` with the same ordered `read_proofs` on the Original attachment.
The reply includes `projected_basis` as the 24 Basis bytes, `read_proofs_sha256` as the 32-byte commitment to that ordered proof list, and `inventory_b64` as standard padded base64 of the v1 `CustodyReadInventory`.
The bounded inventory confirms every physical branch, relation, typed entity handle, valid interval, written Basis, ordinal, carrier digest and Original receipt against the live custody index.
Match the exact selected carrier by its address and digest while retaining the complete contributor set for response admission.
An empty set encodes an empty inventory.
The private lookup is available only to the configured Original owner and returns metadata.
Public scoped Query and Changes still produce opaque handles.
The Original registers and promotes those occurrences through its existing receipt checks and binds the exact transport frame and opaque proof list to its independent response registry.
Send `check_scoped` or `admit_scoped` with `part`, `frame_sha256` and `read_proofs`.
RTDB reads the exact promoted registrations and verifies the Original registry, current writer, selected generation and group fences before ordering admission.
A fresh `admitted` permits the same one-use transport reservation and cutoff as the unscoped action.
`recorded` is recovery evidence.
`clear` confirms an empty contributor inventory against the Original registry.
The Original still uses its exact authenticated disposal and drain operations when the transport closes a protected reservation.

A scoped client can negotiate `row-visibility.exact-hold` and use the JSON Flight action `rtdb.v1.ScopedHold`.
`retain` takes `hold_id` as 16 random bytes, `basis` as 24 projected Basis bytes, `valid_at_micros`, `expires_at_micros`, and `publication` containing `table`, `row_id` and `written_basis`.
Use a qualified table, canonical entity handle and scoped written Basis for the clear publication row seen at that observation.
`reopen` and `release` take the same `hold_id`.
Retain and reopen return the hold ID, projected Basis and original valid observation and expiry.
Send the same hold ID in Query field 12, `scoped_hold`, with that projected Exactly basis to fix the valid observation across queries and pages.
The lease is exclusive at expiry, has a maximum duration of one hour and is charged to the verified scope and database.
Database and scope protection can close the lease early when the retained visible history is erased.

Before enabling a Bijection consumer, qualify the paired wire and Original provider in both serving modes.
Existing checkpoints that already erased labelled history without projection metadata cannot reconstruct that history from the clean fold.
Application funded-offer admission, cross-incarnation projected continuation, and hosted consumers with a separate Original provider attachment need their own evidence and qualification.
