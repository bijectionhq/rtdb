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
   It verifies every promoted registration, current writer, group keys and the captured Multi generation before synchronously calling the enqueue callback.
   The transport owns single admission of the response part and must retain the supplied deadline through its queue and socket handoff.
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

Provider mount and lookup are trusted in-process server calls.
W10b and RTDB still need to agree and implement the authenticated cross-process provider transport, release action and recovery address encoding, plus mapping for legacy, archived, and scoped contributors and protected index bytes.
Once that bridge is installed, a producer can recover old terminals on the same original writer attachment without the transport registry row, while new admissions still require the live registry.

## Final qualification source cases

Run `native_original_custody_mount_rechecks_the_writer_and_withdraws_old_owners`, `registered_frame_rejects_omitted_contributors_and_transport_substitution`, and `ranked_capture_retains_inherited_and_shadowing_custody_groups_before_slicing` with the earlier custody and source-intake regressions.
Exercise both drivers with a mounted live provider, distinct contributing groups on a zero-hit rank, provider or attachment withdrawal at enqueue, selected-generation destruction after capture, and writer displacement during an ordered response transition.
Recover a promoted terminal after reply loss and registry removal, and confirm protected export remains refused pending destination custody binding.
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
- `response` payload is `{"authority": authority, "part": part, "expires_at": micros, "frame_sha256": digest, "contributors": [[group,occurrence],...]}`.
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
`stage`, `promote` and `check` return `{"ok":true}`.
`cancel` and `dispose` return `{"state":"cancelled"}` or `{"state":"disposed","evidence_sha256":[...]}` from the retained ordered terminal.
`drained` accepts `authority`, `group`, opaque `revoked_at` Basis and `inventory_sha256` and returns `{"drained":bool}`.

`check` accepts `registrations`, `frame_sha256` and `read_proofs` in addition to `operation`.
For native protected reads the query flag `custody_release_evidence` requests a sealed `custody_release_proof` in the Query trailer.
Preserve those opaque bytes on the same owner Flight session through final check.
The new custody proof attests the producer's complete protected occurrences and captured key generation.
The `930e66bec7` receiver clock proof remains separate and supplies no disposal or custody authorization.
The responding transport owns exact single admission and deadline enforcement for its queued bytes after the check.
