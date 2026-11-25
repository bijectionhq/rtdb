# Managed Original funded appointments

The managed Original uses the existing `rtdb.v1.Capabilities`, `PrepareFundedAppointment`, `ActivateFundedAppointment`, `Transact` and `TxStatus` actions on a dedicated client-credentials session.
The session advertises `transact.funded-appointment` when the authenticated appointment scope is current and the installed writer, receiver and original journal can support it.
The final request uses `TransactRequest.funded_appointment` field 9, and its committed receipt and exact status both use `Receipt.funded_appointment` field 10.
Use a separate `ReadRoot` credential for clear Original reads and tracked claims under the same placement fence and namespaced `ClaimWriter`.

## Issuer enrollment

Commission a dedicated client ID sub-prefix using `authn.assignment.fundedAppointmentClientIdPrefix` in the `!ManagedWorkloadOidc` profile.
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

The writer checks the exact current placement and positive namespaced `ClaimWriter` against the durable prefix before ordering each stage.
It verifies tracked claims against the same assignment binding and checks the live writer again before the timed append of an activation or a final transaction.
The receiver's signed journal cutoff is bounded by the credential expiry and the original appointment bounds.
The prepared and activation cuts precede the advertised reading, and the terminal system time lies in the original decision window.
The keeper proof covers the exact sealed record under the current writer epoch.

Recover a stage with its exact request or use `TxStatus` with the stable key and a fresh credential for the same current scope.
Pending status carries no appointment coordinates.
Join a terminal receipt to the exact application row and verify the business fingerprint, booking hash, advertised reading and activation provenance before acknowledging it.
If the exact terminal remains uncertain, resolve the Original receipt point through the independently authorized ordered exclusion with its own native key and await positive exclusion or terminal evidence.
A placement change closes the former writer's managed appointment route.

The funds-policy owner still needs to commission the positive durable approval and restricted acquisition path.
Keep the client ID sub-prefix unset until that path and the existing receiver and timed original keeper proofs are commissioned on the selected native Multi writer.
Managed appointment transaction and checkpoint version 5 requires compatible writers and readers before enrollment.

An audience-enabled W13 Original also requires the `row-visibility` publication feature when the final transaction advances Root, along with written-Basis reads of the exact dispatch, seat and receipt.
This source path serves clear Originals under the existing visibility and custody gates.
Commissioning the audience-enabled path requires a separately approved scope for its labelled assertion and publication rows and exact control and claim reads on that Original.
