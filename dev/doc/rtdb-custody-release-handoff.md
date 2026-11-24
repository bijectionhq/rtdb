# RTDB v1 custody producer handoff

The producer branch includes the Query and Changes frame generation fence from `1a5d1b7e5b`, the Default authenticated source intake, ranked contributor custody from `c11220484e`, and exact Multi export pinning from `fc89eea064`.
`ROW_CUSTODY` and protected exports remain gated.
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
