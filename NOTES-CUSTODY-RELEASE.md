# RTDB v1 custody placement and response release handoff

`ideal/v1-custody-release` preserves `e8665c1428` and includes upstream `87fa31122c`.
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

## Integration dependencies

- Install `CustodyCarrierJournal` against the original attachment in each mode.
  Its `authority(attachment)` must attest the original source history, installation, registry and writer, even during recovery of a terminal whose registry row has been removed.
  Its `observe(registration, cancellation)` must authenticate the exact admitted carrier and occurrence, original registry row and full transport part at a current source barrier, and return the immutable positive disposal evidence digest when present.
  The existing Multi adapter depends on the old hosted writer, installation and external-key recovery tables.
  The generic Flight release action, recovery address serialization and migration of that producer and consumer are pending this adapter.
- Link activation authenticates bytes reserved in RTDB and co-orders their placement.
  A producer that needs prepublication sealing, independent link-key retirement, or a review proof for a prepared switch must supply that through its original authority and the activation transaction's ordered requirements.
  The legacy hosted prepared-switch contract is still needed until that producer is converted.
- Wire the release owner through Query, Changes, archive and ranked delivery, with current attachment admission at the handoff.
  Ranked delivery needs bounded inventories of every protected contributor including rows that affect scores without appearing in the result, plus key or erasure enforcement for protected derived index bytes.
  Carry the exact indexed landing association between the protected carrier, its journal occurrence and the independently addressed derived selector through publication, ranking and erasure.
  Scoped custody enrollment and scoped derived retrieval, destination re-binding for restore or dependent forks, and writer-owned retirement reconciliation after restart are rollout dependencies.
  The public placement ops remain behind the existing custody gate.
- The default source-log intake lane must route release commands only from the trusted adapter and preserve the internal receipt namespace across replay and redecision.
  Its public request codec must continue to exclude `Op::CustodyRelease`.
  The existing Changes parity work is owned separately.

## Final local qualification

- Run the new source regressions `custody_reservation_activation_and_release_share_the_revocation_order`, `response_terminal_recovers_after_fence_lost_reply_and_registry_removal`, `placement_preparation_commits_the_exact_binary_carrier_and_link` and the extended `protected_cells_are_sealed_before_append_and_revocation_blocks_delivery`.
- Exercise actual default replica and Multi journal writes on both sides of the fence, lost reserve and activation replies, writer takeover, checkpoint and trim, cancellation racing promotion, and disposal recovery with the registry entry already removed.
  Substitute the original history, writer, occurrence, link, reservation, target interval, response part and cutoff in turn.
- In both modes qualify protected original, branch copy, older scoped cut and hidden-only revoke, zero-hit rank dependencies, independently keyed dependent restore or fork, and restart before key destruction with a promoted response outstanding.
  Check capability discovery and retain the legacy route until the replacement passes these cases.
- Follow the owner's RTDB and consumer landing sequence before the single local qualification in `FINAL-CHECKLIST.md`.
  This lane ran no builds, tests, code generation or benchmarks.
