# Assertion-family custody: the hosted wire contract

This is the Flight contract a placed writer's client uses to keep per-copy custody of a protected row.
The vault owner is `AssertionFamilies` in `rust/crates/multi/src/keys/assertion/family.rs`.
The Flight decoding lives in `rust/crates/flight-sql/src/owned_family.rs` and the placed host in `rust/crates/multi/src/process/owned_worlds/assertion_families.rs`.
Hosted protocol 215 adds the scope and enrolment actions and position admission.

## Model

A family is the private custody record of one protected row's original and all of its copies.
Its members are:

* the original occurrence, enrolled with its assertion and row capability before ordering and bound to its position after publication;
* later world rewrites, each enrolled under a fresh stage of an existing member at its exact world slot;
* each landing, reserved with a fresh destination capability and a distinct link capability, then activated at its ordered placement;
* each derived row, the search projection of a member into a derived search relation (`<partition>_search`), in the Root or in a world overlay.

Every copy has its own capability.
A capability is an `AssertionStage`, a one-use vault slot: it seals exactly one body, and an exact retry returns the first frame.
Landing therefore reseals the row under the reservation's fresh destination capability; it never reuses the original's.

A family is live until fenced.
A fence closes the whole family in both directions: fencing the original closes every copy and derived row, and fencing any active copy closes the original and every other member.
Fenced is permanent.
Settlement replaces the record with a durable denial once the fixed assertion deadline has signed its disposition.

## Admission

Every action requires owned native actions and the placed world host on the listener.
The session must be the placed writer: an authenticated non-empty principal, its database the attachment, idle and auto-commit, and the placed history at the attachment's writer epoch.
A reader, compactor, snapshot or fresh-replica session is refused with `rtdb.world/unavailable`.

Families are addressed within the attached deployment.
The vault record of client family `F` in deployment `D` is the first 16 bytes of `SHA-256("rtdb.owned/assertion-family/v1\0" ‖ D ‖ F)`.
A client cannot name another deployment's family, and a dependent fork, which has a different deployment identity, cannot open or fence its parent's families.

Every capability in a request must name the attached deployment and its current incarnation.
Any other capability is refused with `rtdb.owned/assertion-family-scope` before the vault is touched.
The Root line is the first 16 bytes of `SHA-256("rtdb.owned/assertion-line/v1\0" ‖ deployment UUID ‖ directory log epoch (signed i32 big-endian))`.
World positions identify the authoring world, including winners inherited by a child world.
A request position must name the Root line or a writable world registered for the authenticated principal in the attached deployment.
The original position must name its enrolled world.
The winning point supplies publication via its system revision in microseconds and the ordinal via `rtdb.owned.assertion_ordinal`.
World winners carry their physical authoring world in `rtdb.owned.assertion_line`.
Root point evidence uses an all-zero metadata line, which the client maps to the Root line in the scope reply.
A position outside the deployment's admitted lines is refused with `rtdb.owned/assertion-family-scope` before vault access.

## Encodings

All integers are unsigned big-endian.
Lengths are exact: trailing bytes are refused.

| Name | Bytes | Layout |
| --- | --- | --- |
| `uuid` | 16 | Raw UUID bytes; the nil UUID is refused wherever an identity is expected. |
| `position` | 32 | `rtdb_types::Position::to_bytes`: line generation (16), publication (u64), ordinal (u64, nonzero). |
| `assertion` | 32 | `rtdb_types::AssertionId::to_bytes`. |
| `stage` | 40 | deployment `uuid`, incarnation (u64, nonzero), stage `uuid`. |
| `placement` | 241 | `rtdb_types::PlacementBinding::to_bytes`. |
| `occurrence` | 104 | `position`, `assertion`, `stage`. |
| `reservation` | 176 | source `position`, destination `assertion`, capability `stage`, link `stage`, review commitment (32, nonzero). |
| `text` | 2 + n | u16 length (1–255) and UTF-8 bytes. |
| `key` | 2 + n | u16 length (1–512) and the opaque row key bytes. |
| `derived` | variable | member `assertion`, scope (u8: 0 Root, 1 world), world `uuid` when scope is 1, relation `text`, row `key`, capability flag (u8: 0 none, 1 present), capability `stage` when present. |

A successful action that returns no data replies with the single byte `0x01`.

## Actions

### `rtdb.owned.AssertionFamilyScope.v1`

Request: empty.

Reply: deployment `uuid`, current incarnation (u64, nonzero), Root line (16).

### `rtdb.owned.EnrollAssertionFamily.v1`

Request: the authoring prefix of `SealWorldOverlayRow.v1`, with no plaintext.

Enrols the original assertion and capability at the exact authoring slot of a registered writable Proposal or Held world.
An enrolled original can seal and register derived rows before it has a position.
An exact retry succeeds and a conflicting enrolment is refused.

### `rtdb.owned.EnrollFamilyWorldRow.v1`

Request: the authoring prefix of `SealWorldOverlayRow.v1`, then the existing member's `assertion`.

Enrols a fresh assertion and capability for a later world rewrite at the exact authoring slot.
An exact retry succeeds and conflicting reuse of a stage is refused.
The member retains its position, and every enrolled rewrite stage stays in the fence inventory.

### `rtdb.owned.RegisterAssertionFamily.v1`

Request: family `uuid`, original `occurrence`.

Binds the enrolled original to its position after the placed writer observes the ordered publication, including when recovering a successful attempt.
An exact retry succeeds.
A different original under the same family is refused with `rtdb.multi/assertion-family-mismatch`.
A fenced family refuses with `rtdb/erased-under-view`, and a position with a durable denial refuses the same way.

### `rtdb.owned.ReserveFamilyLanding.v1`

Request: family `uuid`, `reservation`.

Enrolls the destination and link capabilities before either can seal.
The source must be a member position (the original or an accepted landing), both capabilities must be distinct, unused anywhere in the family, and share one deployment and incarnation.
The destination assertion must differ from the original's.
An exact retry succeeds; a different reservation for the same destination is refused.

### `rtdb.owned.ActivateFamilyLanding.v2` (placed form)

Request: family `uuid`, `placement`, `reservation`.

Called after the ordered landing has published and the client has authenticated the destination binding and link.
The placement must name the reservation's destination assertion and destination stage, at a position that is not already a member.
The vault records the placement digest; an exact retry succeeds and a different placement is refused.
This action serializes against the fence and does not itself verify publication evidence.

### `rtdb.owned.SealFamilyLandingLink.v2` (placed form)

Request: family `uuid`, `reservation`, `placement`, destination scope (32), intent (32, nonzero), review slot (u64).

Seals the private landing link (`LandingLink`) under the reservation's link capability and returns its envelope.
The reservation must be enrolled in the live family.
The link body binds the intent, the source position, the destination position, the vault family identity and the review slot.
Retiring the link capability later leaves the destination row readable.

### `rtdb.owned.RequireLiveFamilyOccurrence.v1`

Request: family `uuid`, `occurrence`.

Succeeds only for the exact original or an accepted landing with its destination capability, while the family is live and the position has no denial.
A release owner calls it immediately before releasing a protected row.

### `rtdb.owned.RequireFamilyPositionOpen.v1`

Request: `position`.

Refuses with `rtdb/erased-under-view` when settlement has left a durable denial at the position.

### `rtdb.owned.RegisterFamilyDerived.v1`

Request: family `uuid`, count (u16, 1–256), that many `derived` entries.

Records the derived search rows of a member in the family inventory.
The member must be the original's assertion or a reserved landing's destination.
A world entry must carry a capability, which the registration enrolls; that capability then seals exactly that derived row.
A Root entry carries no capability, because Root derived relations are sealed under the broker-owned R6 slots of their relation.
Root entries are deduplicated by relation and key.
A world slot can enrol multiple fresh stages for the same member across rewrites.
An exact retry succeeds and conflicting reuse of a stage or slot is refused.
A family holds at most 2048 derived entries; the next registration is refused with `rtdb.multi/assertion-family-size`.

### `rtdb.owned.SealWorldOverlayRow.v1`

Request: family `uuid`, `stage`, `assertion`, codec (u32), disclosure scope (32), world `uuid`, relation `text`, row `key`, then the plaintext (the rest of the body, at most the log message limit).

Reply: the `RTAS` assertion envelope.

The stage must be enrolled in the live family for the named assertion: the original or a rewrite at its enrolled authoring slot, a reservation's destination capability with its destination, or a derived capability registered for exactly this world, relation and key.
The vault binds the envelope to its row: the authoring scope commitment is `SHA-256("rtdb.owned/world-overlay-row/v1\0" ‖ world ‖ u16 relation length ‖ relation ‖ u16 key length ‖ key ‖ disclosure scope)`.
An envelope therefore opens only under the same world, relation, key and disclosure scope.
The client writes the envelope as the overlay row's sealed payload in its world batch.

### `rtdb.owned.OpenWorldOverlayRow.v1`

Request: the same prefix as `SealWorldOverlayRow.v1`, then the envelope.

Reply: the plaintext.

The family must be live and the stage enrolled exactly as for sealing.
The plaintext is transient resolution input and does not authorize a history read.
A fenced or settled family, or a retired capability, refuses with `rtdb/erased-under-view`.

### `rtdb.owned.FenceAssertionFamily.v1`

Request: family `uuid`, selected `position`, erasure order `uuid`.

Reply: order `uuid`, vault inventory digest (32), then four counted sections, each a u32 count followed by its items: member `position`s, assertion `stage`s, link `stage`s and `derived` entries.

The selected position must be the original or an accepted landing.
The first fence records its order; a later fence with any order returns the same inventory under the first order.
The inventory covers every member position, every enrolled assertion capability including pending landings and derived capabilities, every link capability, and every derived entry.
The digest is `FamilyFence::vault_inventory`, the value the client binds into the erasure order's assertion-vault obligation.

### `rtdb.owned.SettleAssertionFamily.v1`

Request: family `uuid`.

Persists a denial for every member position and replaces the family with a permanent denial, after the fixed assertion deadline has a signed settlement for the family's order.
The fixed assertion profile is not installed on hosted cells, so this action is refused with `rtdb.owned/assertion-profile-uninstalled`.

## Bijection Root landing

A Bijection Root carrier contains the existing unit-sealed payload inside an assertion envelope sealed by the RTDB broker under the reservation's fresh destination stage.
The Root unit fence and the original family fence both govern release.
The landing authority verifies equivalent effective disclosure, including access to the Proposal and world, before probing or opening the source.
The receipt, destination and branch retirement share the prepared-generation switch at the protected publication label.
The receipt stores the sealed link under the distinct link stage; the Root carrier requires only the destination stage.

The offered clock is an acceptance premise.
RTDB assigns the ordered position when it resolves the prepared generation.
The pre-publication link therefore names the destination by the receipt digest and review slot, and the post-publication binding adds the exact accepted position selected by that slot.
The receipt binds the approved source slot, destination carrier and validity, disclosure commitment, and the exact payload and link envelopes.
The release owner authenticates the applied receipt and its proof, the installed carrier and original ordered position, and uses the same evidence when recovering an interrupted activation.
A constraint refusal, a stale prepared generation or a missing receipt grants no activation.
A reservation and sealed frames alone grant no release.

Every Root release path checks the exact activated binding against the current family before opening the outer envelope, including exact history, journal/replay, and derivative backfill.
An original reader lease and final release check still apply after opening.
An unavailable original vault or unproven binding fails closed.
A fenced winner continues to shadow older assertions.
A dependent restore must keep the outer envelope and resolve current custody through the original authority, or refuse release.
It cannot convert the payload to a receiver-only envelope.
Root erasure selects the authenticated landed occurrence and closes the same family as erasure of the Proposal original.
Root derivative registration and the ordered erase inventory remain part of the existing R6/R7 contract.

Hosted protocol 216 introduces the Root landing form.
The reservation request is unchanged.
All other fields below use the encodings above.

| Name | Bytes | Layout |
| --- | --- | --- |
| `root-target` | 104 | Receipt digest (32), review slot (u64), carrier address and valid extent commitment (32), effective disclosure commitment (32). |
| `root-binding` | 248 | `position`, `assertion`, destination stage UUID, `root-target`, SHA-256 of the destination envelope (32), SHA-256 of the link envelope (32). |

The receipt, address and extent, disclosure, and envelope commitments must be nonzero.
The client authenticates the address, extent and disclosure commitments from the installed Root carrier and its publication.
The vault binds Root payload seal/open to the target, family and destination identity in a Root-specific authoring domain.
The reservation names the original or another admitted family member position.
The seal request separately supplies the actual ordered position of the reviewed winning source, which can be a later rewrite of that member.
The link seals both source coordinates and the destination envelope digest.
Its authoring scope binds the private family identity and the Root target.

* `rtdb.owned.SealFamilyLandingPayload.v1` takes family, reservation, `root-target`, then the unit-sealed carrier bytes. It returns the destination assertion envelope.
* `rtdb.owned.OpenFamilyLandingPayload.v1` takes family, destination `stage`, `root-binding`, then the destination envelope. It verifies the active binding and exact envelope digest and returns the inner carrier. Opening requires the current family both before and after the broker unwrap.
* `rtdb.owned.SealFamilyLandingLink.v2` takes a binding-kind byte. Kind 0 is followed by the placed request fields above. Kind 1 is followed by family, reservation, `root-target`, the reviewed source `position`, then the destination envelope digest. It returns the separately sealed link envelope.
* `rtdb.owned.ActivateFamilyLanding.v2` takes a binding-kind byte. Kind 0 is followed by the placed request fields above. Kind 1 is followed by family, `root-binding`, reservation, then the link envelope. The vault authenticates the exact link before first activation, and accepts an exact activation retry independently of link retirement.

## Fence semantics across dependent forks

A fence takes effect in the vault, not in any one world, so it closes every world at once.
From the fence commit:

* `RequireLiveFamilyOccurrence.v1`, `OpenWorldOverlayRow.v1`, `SealWorldOverlayRow.v1` and every registration refuse for the family;
* a world overlay row sealed under a member or derived capability stops opening in the world that wrote it and in every world forked from it, because a forked world inherits the same envelope and the same capability;
* a dependent hosted fork never opens the family at all, since its deployment identity addresses a different vault record and its capabilities name another deployment.

Root derived rows carry no family capability.
The fence inventory lists them, and the client's ordered erasure commit erases every listed Root row in the same commit as the protected row's erasure.
That Root erasure follows R7: the replay broker projects the erase selectors through the authenticated fork bases of every dependent root.

Physical erasure follows the fixed assertion deadline.
Its assertion-vault obligation retires every assertion, link and derived capability in the fence inventory through the original vault routes.
Ciphertext then ages out with its world or archive.

## Errors

| Code | Category | Meaning |
| --- | --- | --- |
| `rtdb/erased-under-view` | Forbidden | The family is fenced or settled, the position is denied, or the capability is retired. |
| `rtdb.multi/assertion-family-mismatch` | Conflict | The request does not match the recorded family. |
| `rtdb.multi/assertion-family-contention` | Conflict | The record changed concurrently through every retry; the client retries. |
| `rtdb.multi/assertion-family-size` | Incorrect | The family record or its derived inventory exceeds its limit. |
| `rtdb.owned/assertion-family-scope` | Forbidden | A capability names another deployment or incarnation, or a position names a foreign line. |
| `rtdb.owned/assertion-profile-uninstalled` | Unsupported | Settlement requires the fixed assertion profile. |
| `rtdb.world/unavailable` | Incorrect | The session is not the placed writer or the host has no family vault. |
