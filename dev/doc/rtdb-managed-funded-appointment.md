# Managed Original funded appointments

This document records the legacy managed Original funded-appointment receipt contract.
A current credential for the existing appointment can use `rtdb.v1.TxStatus` to read keyed status. The retained terminal uses `Receipt.funded_appointment` field 10.
The listener advertises `tx-status` and refuses fresh prepare, activate and consume requests with `rtdb/funding-kernel-unavailable`.
The Bijection Kernel owns live funding through its `sys.slots` ledger.

## Legacy credential scope

An existing deployment identifies its dedicated client ID sub-prefix using `authn.assignment.fundedAppointmentClientIdPrefix` in the `!ManagedWorkloadOidc` profile.
The assigned issuer uses its enrolled HTTPS origin, private issuer CA, audience and configured Original authority point.
It must authorize acquisition for the authenticated holder and derive the scope from the acknowledged installed catalog placement, the exact positive native `ClaimWriter` evidence and the durable funds approval.
The issuer supplies those facts through its authenticated `rtdb_workload_grant` UserInfo claim with `version = 2`.
Its `authority` object has `role = "funded-appointment"` and these fields.

| Field | Commissioned value |
| --- | --- |
| `fence` | Current ordered `Fence` with exact placement epoch, assignment ID, holder and namespaced placement receipt |
| `writer` | Exact `Applied` `ClaimWriter` receipt with namespace, key, Basis bytes and system time |
| `key` | Stable nonzero 32-byte native appointment key |
| `receipt` | Typed `Point` with `table`, `id` and explicit `valid_at` in microseconds |
| `intent_sha256` | Nonzero 32-byte business intent sent at prepare and final transact |
| `funding_policy_sha256` | Nonzero 32-byte commitment to the positive durable funds approval independently checked by the issuer |

Use the application’s installed receipt relation, exact typed ID and literal valid instant.
The application must retain its stable native key as the receipt’s transaction locator.

Keep the issuer, audience, UserInfo `sub`, database, incarnation, `grant_id` and full authority stable through renewal and recovery.
The five-minute maximum grant lifetime is further bounded by the access token expiry.
The grant identity and scope determine the private receipt namespace, so renewal can extend access to the same appointment without moving its existing records.

## Ordered checks and recovery

The original writer checked the exact current placement and positive namespaced `ClaimWriter` against the durable prefix before ordering each stage.
It verified tracked claims against the same assignment binding and checked the live writer again before the timed append of an activation or a final transaction.
The receiver's signed journal cutoff is bounded by the credential expiry and the original appointment bounds.
The prepared and activation cuts precede the advertised reading, and the terminal system time lies in the original decision window.
The keeper proof covers the exact sealed record under the current writer epoch.

Use `TxStatus` with the stable key and a fresh credential for the same current scope.
Pending status carries no appointment coordinates.
Join a terminal receipt to the exact application row and verify the business fingerprint, booking hash, advertised reading and activation provenance before acknowledging it.
If the exact terminal remains uncertain, resolve the Original receipt point through the independently authorized ordered exclusion with its own native key and await positive exclusion or terminal evidence.
A placement change closes the former writer's managed appointment route.

Managed appointment transaction and checkpoint version 5 retain the original receipt evidence under compatible readers.
