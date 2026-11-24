# RTDB v1 custody placement and response release handoff

`ideal/v1-custody-release` preserves `e8665c1428` and incorporates Default source intake `be2c141ce7` through `132991e222`.
The branch includes ranked contributor custody `c11220484e` and exact Multi export pinning `fc89eea064`.
Protected exact exports stay closed pending destination custody binding.
Default and Multi share the same resolved effects, checkpoint state and release rules.
`ROW_CUSTODY` remains unadvertised and the assertion-family Flight routes remain in service.

## Source in this branch

- `rtdb.v1.Op` tags 11 and 12 carry `ReserveCustodyPlacement` and `PlaceCustodyRow`.
  `xt.custody_placements` supplies the enrolled identity and its reservation and occurrence commitments to an unrestricted reader at the selected applied cut.
- Reserve records the exact original, reviewed source, destination target and interval, review commitment, and expected carrier and link digests.
  The write path hashes the supplied protected binary carrier and link before sealing them.
  Activation commits the row and checks the reservation, actual committed envelope, written basis and ordinal together.
  The resulting envelope first becomes usable at the ordered write, and destination and link identities can only be enrolled once per incarnation.
  A branch fork inherits its original occurrence and an explicit copy takes a new placement.
- `rtdb_node::serving::custody_release::ResponseRelease` is the shared adapter-facing entry point.
  Private registration, promotion, cancellation and disposal order on the same fold as revocation.
  Terminals keep the complete registration and positive disposal evidence through checkpoint and trim, with an internal receipt namespace.
  The adapter can ask for drain status against the exact ordered revocation basis and inventory digest.
  Retiring the group immediately denies new reads and destruction waits on the durable release drain.

## Original carrier integration

- The production RTDB entry point is `Database::original_custody_carrier_journal` on the original writable attachment.
  `AttachedCustodyCarrierJournal` verifies a separately retained writer receipt and the registered carrier receipt by keyed TxStatus on that attachment.
  It matches the carrier receipt's exact basis to the committed occurrence, binds its key, basis and system time into the durable release registration, and requires positive disposal evidence for the exact transport part.
  The main-branch writer point is pinned when installing the adapter.
  Its written basis guards the ordered command and is checked again with the live custody set at delivery.
- `OriginalCarrierObservations` is the minimum W10b bridge.
  `confirmed_writer(authority)` supplies the independently confirmed request key, opaque receipt Basis and system time from the original `V1SourceFrontier`, with the original installation, registry and current writer authenticated.
  Preserve the Basis bytes across the boundary and interpret them only inside RTDB.
  `registered_carrier(registration)` supplies the exact registered occurrence's receipt triplet and the live observation or immutable positive disposal commitment of that same response part.
  The independent registry must retain that evidence until RTDB acknowledges the terminal.
  Its unavailability, cutoff expiry or an unconfirmed writer cannot itself supply a terminal.
  Terminal recovery can proceed after removal of that registry row through the independently proven original writer and the retained exact terminal.
- Default carries the trusted command and internal receipt namespace in the authenticated source envelope and reconstructs the canonical command before resolving it.
  Multi submits that same writer-guarded request through its fenced pipeline.
  Public Flight and ordinary canonical intake exclude `Op::CustodyRelease`.
  Keep the protected gate closed until W10b and the hosted producer supply the original provider, the consumer uses it, and the release wire and recovery address are installed.
  Default's post-destruction protected write retry also needs a fingerprint-checked receipt lookup ahead of the source-log seal gate before exposing placement.

## Ranked and protected reader scope

- Keep every distinct `(custody group, occurrence)` from the authenticated ranked contributor inventory with the response part, including inherited and shadowing world rows, masked contributors, rows used only for text statistics, and vector candidates.
  The held text, nearest and hybrid paths need that inventory even on zero-hit replies and when the consumer emits only counts, scores or cursors.
  `ResponseRelease::check_all` compares the supplied registrations with the full expected set, then checks every promoted part and its entire group set on a fresh cut.
  Preserve the exact indexed landing association among the original carrier, its journal occurrence and its independently addressed derived selector through publication, ranking and erasure.
- Connect that owner to Query, Changes, archive and ranked handoff, and apply key or erasure enforcement to protected derived index bytes.
  Scoped custody enrollment and derived retrieval, destination re-binding for restore or dependent forks, and writer-owned retirement reconciliation after restart remain rollout dependencies.
- Link activation authenticates the bytes reserved in RTDB and co-orders their placement.
  The producer must supply any prepublication sealing, independently retired link key, or prepared switch review proof through its original authority and the activation transaction's ordered requirements.
  The legacy hosted prepared-switch and assertion-family routes still serve their current production consumers.

## Final local qualification

- Run `trusted_source_release_reopens_only_with_its_canonical_request_and_private_namespace`, `custody_reservation_activation_and_release_share_the_revocation_order`, `response_terminal_recovers_after_fence_lost_reply_and_registry_removal`, `placement_preparation_commits_the_exact_binary_carrier_and_link` and the extended `protected_cells_are_sealed_before_append_and_revocation_blocks_delivery`.
- Exercise actual default replica and Multi journal writes on both sides of the fence, lost reserve and activation replies, writer takeover, checkpoint and trim, cancellation racing promotion, and disposal recovery with the registry entry already removed.
  Substitute the original history, writer, receipt key or time, occurrence, link, reservation, target interval, response part and cutoff in turn.
  Race writer displacement between the independent observation and the ordered command in both drivers.
- In both modes qualify protected original, branch copy, older scoped cut and hidden-only revoke, zero-hit rank dependencies, independently keyed dependent restore or fork, and restart before key destruction with a promoted response outstanding.
  Check capability discovery and retain the legacy route until the replacement passes these cases.
- Follow the owner's RTDB and consumer landing sequence before the single local qualification in `FINAL-CHECKLIST.md`.
  This lane ran no builds, tests, code generation or benchmarks.
