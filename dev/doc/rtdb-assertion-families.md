# Assertion-family custody: the hosted wire contract

This is the Flight contract a placed writer's client uses to keep per-copy custody of a protected row.
The vault owner is `AssertionFamilies` in `rust/crates/multi/src/keys/assertion/family.rs`.
The Flight decoding lives in `rust/crates/flight-sql/src/owned_family.rs` and the placed host in `rust/crates/multi/src/process/owned_worlds/assertion_families.rs`.
Hosted protocol 215 adds the scope and enrolment actions, position admission, and ranking of sealed world projections.

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

### `rtdb.owned.ResolveFamilyMember.v1`

Hosted protocol 219 adds this action.

Request: family `uuid`, member `assertion`.
Reply: the exact admitted member `occurrence` in the live family.
A reviewed rewrite supplies its member assertion to recover the reservation coordinate while retaining its own authenticated winning position for the sealed link.
An unpositioned original and a pending destination refuse.

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
A Root entry names a reserved landing destination and uses the physical selector `bjrl1/<family hex>/<destination assertion hex>/<AddressRef>`.
All hex is lowercase, including the canonical AddressRef. The full selector fits the 512-byte key bound.
RTDB checks the selector's family and member against the request. Root entries are deduplicated by relation and physical selector.
A refused or interrupted reservation may retain its inventory entry. Its selector is disjoint from every ordinary Root projection at that address.
A world slot can enrol multiple fresh stages for the same member across rewrites.
A retained Root execution outcome may use a separately enrolled stage on the installed Root line.
For that artifact the `world` field names the original installed Root line, the member is an active landing on that line, the relation is `bj_fact_root_admission`, and the key is a 64-character lowercase hex commitment to the receipt and its disclosure.
The Root line is a sealing scope for this private artifact. A Root search projection uses its distinct physical selector and carries no stage.
The existing family envelope actions and the derived-stage response hold bind the same receipt artifact slot.
The retained outcome keeps its existing Root unit seal, and recall requires the original Root unit fence and the exact live family member.
The installed Root line, family vault and receipt must remain reachable.
The admitted output route covers a registered result with one exact landed Root point as its protected input.
On a held Original parent, the installed live writer can select an earlier exact Root publication and read its opaque protected carrier through the existing parent point route.
The earlier selection uses the held parent predecessor and requires matching historical declarations and intact purge coverage.
For a protected row, the same selected publication scopes both the carrier and the protected row journal.
The two points must agree on the winning publication, and the journal supplies the assertion ordinal for the original family check.
The application keeps the original unit and family admission through response-part release.
A shared field uses its same-cut Root winner selector and carrier, with its current original unit fence.
A nested hold also certifies the frozen ancestry edge and relation coverage before an earlier Root parent point.
At an exact frozen Held publication, the predecessor certifies the same transaction and accepted reading for the opaque carrier and protected journal.
Hosted protocol 225 uses `rtdb.owned.ReadHeldWorldPointAt.v2` for these exact earlier effective points.
For an exact frozen overlay winner on the live installed writer, `rtdb.owned.frozen_world_winner` carries 48 bytes as lowercase hex: authoring world UUID, system micros (i64), original event transaction (i64), original event ordinal (u64), and accepted Held reading micros (i64), in big-endian order.
RTDB verifies the original event transaction against the authoring world's accepted publication. The application checks that the carrier and its protected journal have distinct events from the same writer and transaction, with the same accepted reading.
A positive carrier still requires its family assertion ordinal and the original family and response admission. The protected journal uses its retained event position.
An explicit overlay tombstone returns zero rows and no positive point revision. The application admits a frozen negative only when both the carrier and journal have matching, explicit tombstone evidence. It refuses missing or mismatched evidence.
A successor archive copy needs a durable route authenticated to the Original deployment, incarnation, ordered journal position and current family authority.
A frozen capture proves the historical assertion and bytes. Recall through a new deployment also needs access to the Original ordered marker and release owner through any source retirement.
Successor replay of a family carrier refuses until that route is available.
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

### `rtdb.owned.PlanAssertionFamilyFence.v1`

Request: family `uuid`, selected `position`, erasure order `uuid`.

Hosted protocol 217 exposes a read-only plan.
The selected position must be a bound member.
The reply has the order, inventory digest and counted sections specified below.
If the vault already fenced the family, the reply preserves its first order.
The plan leaves the vault and source unchanged.
The retiring transaction must take a claim over this inventory before it can order an immutable marker.
The claim freezes the family record under its vault compare-and-swap and binds the exact placed source attempt.
The checked insert targets `bj_fact_assertion_family_fences` with the family UUID as 32 lowercase hexadecimal characters and a null expectation.
Hosted protocol 220 encodes its binary `body` as version 2 (u8), stable member `position`, winning carrier `position`, order `uuid` and inventory digest (32), in 113 bytes.
Each Root inventory entry requires an erase of its named relation and exact UTF-8 row key in the same transaction.
Each erased Root relation requires its `fact_sealed_search_erasures` marker.
Only selectors authenticated by the family claim can erase a live indexed Root search relation through this route.
The resulting purge frontier revokes older held ranked reads, while physical search-run withdrawal follows the existing retention path.
The same transaction carries an all-time `fact_unit_fences` row on the winning carrier's line with `unit_phase` set to the erasing state and `assertion_vault_inventory` set to the same digest.
For a world retirement, the transaction supplies one present observed point check for the same key in its local `fact_unit_column_payloads` overlay.
The resolver rereads that exact witnessed valid instant against the current ordered base.
It rejects ambiguous present witnesses and authenticates the winning carrier's physical world, publication and positive ordinal, the observed point version, and its sealed payload at the enrolled slot.
The original vault checks that the carrier belongs to the stable bound member from `ResolveFamilyMember.v1`.
A rewrite can have a different authoring line from its registered member.
A Root retirement supplies the active Root member in both position fields.
The ordered resolver reads the current Root carrier at the retiring fence identity and verifies its publication.
The original vault verifies that carrier's family, stage, destination assertion, target, payload digest and link digest against the exact accepted Root binding before it claims the fence.
The source refuses world winners that exist only in an inherited layer.
A nonterminal controlled Proposal retirement with Root erases uses world-batch Root kind 8.
Its Root publication section has the same hold and independent attempt as kind 2.
It retains the controlled Proposal receipt and overlay checks and admits the family marker, the exact planned Root erases, and their sealed-search erasure markers.
A controlled Proposal retirement without Root selectors uses kind 5.
Clients gate both new wire layouts on Flight advertising `rtdb.owned.DecideHeldWorlds.v3` and `ResolveFamilyMember.v1`. The hosted artifact floor is 220. The prepared landing switch keeps its action and wire.
At most one family fence can order in a transaction.
The source resolver validates the installed authority, the marker's absence, the retirement obligation, and the exact Root selectors before claiming the unchanged inventory.
Original-authority marker reads use a current source barrier for both ordinary and archive release.
A pending claim with no ordered marker leaves already-enrolled stages open under that current admission.
The vault finalizes the first ordered marker and can replace an aborted claim only once the original source's durable history covers the failed attempt.

### Original family response release holds

The original family vault serializes exact response-part holds with the pending claim and fence finalization.
An installed carrier-journal adapter must authenticate the linked original writer, installation, registry and full response part at a current source barrier.
Its source installation and transport cell history are separate coordinates.
This vault slice admits a carrier journal only on the original placed history.
An unavailable, restored or separately placed registry refuses until its exact original adapter is available.

Each hold binds the exact member and input, the transport part and its immutable cutoff.
Only the authenticated live registration can stage or promote, and current marker admission applies before and after a hold mutation and at final release.
Private cancellation consumes the exact attempt before a promotion can win.
A promoted hold closes only after positive durable disposal from the original carrier journal.
The original vault retains a terminal record before acknowledging disposal or removing a hold from the family roster.
A deadline refuses further release but supplies no disposal evidence.
A drain checks the current original marker and the exact inventory before declaring all family holds terminal.
Hosted protocol 221 advertises `rtdb.owned.FamilyResponseRelease.v1` to the installed writer.
It reuses the current original placement, hosted `bj_installation` incarnation and writer, and the canonical `bj_external_key_recovery` table.
Recovery requires the same linked original writer and leaves the registry row in place until the vault acknowledges terminal custody.
The action refuses mounted, restored or cross-source placements.

The request starts with one operation byte.
Prepare (0) carries family UUID (16), response part (72), the original epoch-millisecond cutoff (u64 big-endian), and input.
It returns provider origin (32), attempt (32), and the nonsecret recovery address (188).
Stage (1) carries origin, attempt, response part, address, and the same input.
Promote (2), check (3), cancel (4), and dispose (5) carry only origin, attempt, part, and address. Each returns byte 1.
The part orders cell history, Usher, Conductor and response UUIDs followed by the positive u64 ordinal.
Input kind 0 is the exact member occurrence. Kind 1 adds the world's ordered winning position and the complete world-row authoring prefix.
Hosted protocol 226 adds input kind 4 to `rtdb.owned.FamilyResponseRelease.v2` for an exact Root landing.
Kind 4 carries the 104-byte `occurrence` followed by the 248-byte `root-binding`.
The occurrence names the original Root line and agrees with the binding's position, assertion and stage.
Prepare and Stage require the exact activation, and the registration commits to the full binding.
Promote, Check, Cancel and Dispose keep the same registration encoding and cutoff.
The RTDB adapter derives the registry commitment from the original placed history, the installed incarnation, and the registry table name.
It authenticates the exact row creator against the current original `bj_installation` writer and checks the original registry address at the same barrier.
Registration observation also requires the current Original control view to cover the exact transaction sampled when it opened the carrier journal authority.
Flight and archive rank outputs also carry a bounded current marker check through the plaintext handoff.
The placed Original route checks the archive source fence before opening a Root landing and at family plaintext handoff.

### Retired Original control evidence

Hosting can use `OriginalAssertionFamilyReader::current_retired` to verify an authenticated archive selection against the current Original attachment.
The verifier requires exact coverage of the carrier publication, fence, retired frontier and captured cut in the physical Original history.
It derives the handoff commitment from the ordered `archive-source-fence.v1` row and checks that the same transaction recorded an empty writer and placement assignment for the exact original installation.
The current original custody fingerprint and Root audience tablet must match the approved selection.
Recovery can derive a writer commitment for an existing registry entry by proving its exact pre-fence transaction and checking its creator against the original installation at that transaction.
This control evidence requires the Original order to be reachable and the selected history to remain readable.
Successor release admission requires authenticated restore enrollment, an ordered control continuation and positive current disposition under the Original release owner.

### `rtdb.owned.RetiredOriginalFamily.v1`

The `owned.retired-original-family` feature operates on the still-hosted Original RTDB order for one held replay of an exact Root landing.
The current multi image attests the feature with `owned.hosted` under `rtdb.v1`.
An authenticated read session can discover the retired-family feature in `rtdb.v1.Capabilities` on a configured Original holder.
An approval worker authenticates the assigned target replay receipt and uses the existing Original recovery TLS client binding to request the endorsement.
The `protected_root_custody_identity` recovery action returns the Original placed history and logical family custody for the selected capture and manifest.
The `endorse_protected_root_custody` recovery action takes the exact lowercase-hex `RFC1` body and the same manifest, then returns its SHA-256 and the endorsement.
The reaper checks the recovery credential, organization, Original installation, accepted capture, incarnation, capture time and key namespace.
The installed CellSecret stays in RTDB's reaper and node.
Original custody orders the grant and its replay authorization together under `fenced-family-custody/<grant SHA-256>` and `fenced-family-replay/<replay binding SHA-256>` in the Original restore control table.
The grant binds the deployment incarnation, placed history, family vault, Original and target installations, archive fence and frontier, exact target replay receipt and Root activation.
The holder checks both exact signed rows at a fresh Original barrier after the retired frontier.
The reader and Original controller have distinct Ed25519 keys.
The successor uses an authenticated Flight session with read access to the current Original attachment and signs its request with the enrolled reader key.
Operation 0 checks the exact live activation and returns byte 1.
Operation 1 wraps the family response-release operations and requires the exact kind 4 input for prepare and stage.
Operation 2 takes the exact promoted registration followed by the Original outer envelope and returns its row-unit-sealed contents.
The response handoff rechecks the current grant, registry, family marker and cutoff.
The Original carrier registry keeps the same release addresses and records the enrollment locator as creator and the controller's signed `family_custody_proof` for the exact live or disposed phase.
The controller orders the disposed phase after the same response part has joined the Original row and family holds, and the provider acknowledges that ordered disposition on the same registration.
The controller uses operation 4 to plan the exact order and operation 3 to submit its ordinary inline Root retirement decision with the sealed installation revision and a zero writer field.
The ordered decision also requires the Original's authenticated hosted-control owner on the current attachment.
The resolver matches the same current paired enrollment and closed Original authority before the existing ordered predicates authenticate the selected carrier and retirement inventory.
The custody decision admits the Original Root fence and its bounded erasure and acknowledgement effects.
The `owned.retired-original-family-recovery` feature gives the Original controller terminal acknowledgement for a disposition on the same Root landing whose registration predates the archive fence.
Operation 5 takes the exact pre-fence transaction as signed big-endian i64 ID and i64 system microseconds, followed by the 324-byte Original registration.
Its caller authenticates as the current Original hosted-control owner and signs with the enrolled Original controller key.
The holder verifies the exact address and historical creator at that transaction, then confirms the same address and creator in a positively disposed current Original registry row.
An acknowledgement retry uses the same registration and returns byte 1.

### Ranked response contributor inventory

Ranked held text, nearest and hybrid replies include a complete bounded family inventory, including replies with no hits.
The inventory comes from the admitted corpus before result slicing. It covers non-hit text statistics and vector candidates, and includes protected world shadows that affect the inherited corpus.
The Root search and vector paths use exact corpus admission when the installed relation has the sealed projection field.
Protected partitions require the installed original rank owner before corpus or scoring work.
The statistics-only artefact route refuses protected partitions.
An indexed Root landing registers its physical derivative under `bjrl1/<family hex>/<destination assertion hex>/<canonical AddressRef>` before publication.
The family and assertion fields are 32 and 64 lowercase hexadecimal characters, respectively, and the full physical selector fits the 512-byte derivative key bound.
The ranker uses the canonical address for overlay masks, candidate identity, tie breaks, and output.
Custody and erasure use the full physical selector.
The installed writer uses the registered selector for both first publication and later backfill, and ordinary writes cannot replace an active landed carrier.
This publication contract requires a fresh installation and supported retained cuts with no previous raw family derivative slots.
A raw projection expresses ordinary protected custody and uses the existing corpus bounds.
Before admitting an unshadowed decorated Root projection, the ranker requires absence of the raw projection and reads the payload carrier at `row/<AddressRef>` and the protected journal at `row-position/row/<AddressRef>` at the same held valid and system cut.
The journal belongs to the installed base relation underlying the search relation.
The journal and carrier must share their publication, the derivative must have ordered at or after that publication, and the vault must authenticate the exact accepted binding, the registered physical selector, and the current original fence.
The Root contributor uses the journal assertion ordinal.
A decorated selector with a different carrier or with a visible raw alias refuses.
Hosted protocol 223 advertises the binary ranked-reply actions via Flight `ListActions`.
The Root actions are `SearchHeld.v7`, `NearestHeld.v4` and `HybridHeld.v4` in the `rtdb.owned.` namespace.
World text uses `SearchHeldWorld.v4`, `SearchHeldWorldAt.v3` and `SearchHeldWorldParentAt.v3`.
World vector and hybrid use `NearestHeldWorld.v3`, `NearestHeldWorldAt.v3`, `NearestHeldWorldParentAt.v3`, `HybridHeldWorld.v3`, `HybridHeldWorldAt.v3` and `HybridHeldWorldParentAt.v3`.
The action request bodies and rank-scope digest domains are unchanged.
Ranked replies require the `x-rtdb-owned-ipc: stream-v1` request header.
The first Flight action result body carries the binary inventory as a big-endian u16 entry count followed by entries in strictly increasing byte order.
Even a zero count has its own result body.
The Arrow IPC schema, any hit batch frames and the end marker follow in the same reply stream.
The reply admits the combined encoded Flight result sizes under the existing message limit before sending any frame.
An entry has a one-byte kind, family UUID (16), member assertion (32), stage (40), Root protected journal publication and ordinal (two u64s) or authored world UUID (16), then two u16-length-prefixed relation and key fields.
The bounds are 512 entries of at most 1024 bytes, at most 512 decorated Root selectors per rank, with one carrier, journal and raw alias probe per selector.
Missing custody evidence, unsupported carrier shapes, and excess inventory refuse.
`FamilyResponseRelease.v2` adds exact world-derived input kind 2 and exact Root-derived input kind 3 to the existing response-part hold.
The consumer retains the inventory in the invocation's original output custody even when it returns only counts, cursors or derived scores.
Hybrid reads retain the combined text and vector dependencies under that same bound.

### Shared column census

The installed writer discovers `rtdb.owned.ReadHeldWorldSharedColumnCensus.v2` through Flight `ListActions`.
The request is the existing held world handle (16 bytes), signed big-endian valid instant (8 bytes), big-endian u16 byte length (106), raw Binary Root BFCP2 prefix, then the unchanged ASCII `world-column/<64 lowercase hex>/` prefix.
The valid instant must be the held data clock.
The response has one dense Arrow identity union `_id` column, including on an empty census, and the existing `rtdb.owned.world_range_witness` metadata.
Root rows use type ID 4 (Binary) in unsigned lexicographic order, followed by World rows using type ID 0 (Utf8) in strictly increasing text order.
One completion covers the Binary `fact_unit_shared_column_carriers` prefix at the archived Root parent and the opaque text prefix in the selected `fact_unit_column_payloads` World overlay.
When the pin has frozen ancestors, the overlay scan joins their exact retained carrier identities with the selected local overlay.
Root selectors used for point reads sit outside the Root census and never count as overlay carriers.
The response contains at most 512 identities.
The nonce binds the parent pin, original writer placement, both source relations' purge frontiers and the selected text overlay's relation frontier.
The installer co-orders that nonce with its exact carrier point and current Root unit condition.
The scope is a writable world in the same placement.
Every retained ancestor requires exact opaque schema and purge coverage.
A restored destination cannot use a local census to substitute for an unavailable original source.
Use parent mode `02` of the existing held World point action for Binary Root rows, including on nested holds.
For batches use selector 4 of `rtdb.owned.ReadPoints.v1` with the same Arrow point body and Binary `_id`.
The archived Root point requires the live original writer scope and applies the exact typed purge floor.
World carriers and Root winner selectors continue to use the scoped Utf8 world-column key in `fact_unit_column_payloads`.

### Fence plan reply

Protocol 217 retires the former immediate fence action.

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
The landing authority verifies that Proposal and Root have the same installed payload audience before probing or opening the source.
This comparison includes the effective world payload-read gate at the reviewed source and the installed payload-read authority at Root.
Matching label strings alone does not establish equality.
Proposal authoring, review and approval grants give no payload-read standing.
Bijection's direct app landing resolves the approved branch at its original audience.
The Proposal owner and Root hold bind the deployment, serving incarnation, concrete label registration, installed grant catalog and exact table declaration.
The current caller's Clear grant and the original approval invitation are checked independently.
The private acceptance names that audience commitment and orders its current authority checks together with the selected world point and Root predecessor.
The admitted direct app cases are an acceptance-valid keyed creation and that creation followed by one registered rewrite of its original copy in the same Proposal.
Both require an unchanged Root publication, the same complete key and protected incarnation, and no Root winner.
For a pair, the reviewer authenticates both original receipt pairs and both exact ordered source cuts, including the retained creation and the winning rewrite.
Each registered execution receipt authenticates its sealed review commitment and its guest rerun command, which is sealed under the same source family.
Each receipt and observation consumes its place in the Proposal's original exact review budget.
The replay ciphertext has a fresh derived stage at the authoring Proposal, with relation `bj_fact_root_admission` and a 64-byte lowercase hexadecimal receipt-and-position commitment as its slot.
The original vault admits this command slot only at a live Proposal and includes its capability in the ordinary family fence inventory.
Both guest commands rerun and match their registered effects. In the atomic Root publication the final image supplies the single visible copy and any indexed projection.
The private receipt retains both compared decisions. The family reservation names the original member, and the sealed link names the actual winning world assertion and its order.
The receipt, destination and branch retirement share the prepared-generation switch at the protected publication label.
The receipt stores the sealed link under the distinct link stage; the Root carrier requires only the destination stage.

The offered clock is an acceptance premise.
RTDB assigns the ordered position when it resolves the prepared generation.
The pre-publication link therefore names the destination by the receipt digest and review slot, and the post-publication binding adds the exact accepted position selected by that slot.
The receipt binds the approved source slot, destination carrier and validity, disclosure commitment, and the exact payload and link envelopes.
The prepared switch includes an absent family marker check for each family in the protected Root carriers.
The original ordered resolver compares that exact set with the carriers being placed.
A fence that orders first therefore invalidates the applied switch even when the destination was reserved earlier.
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

## World search

A derived overlay row stores its projection in the declared opaque binary field `sealed_world_projection`.
The carrier is ASCII `BJWP1`, family UUID, deployment UUID, incarnation, stage UUID, assertion ID, member assertion ID, disclosure scope (32), and the `RTAS` envelope.
The codec is the u32 representation of ASCII `BJWR`.
Ranking takes the authoring world, relation and row key from the winning overlay layer and checks the current family and derived stage before and after opening the envelope.
The opened body is transient ranking input.
The body starts with the five-byte tag `BJPS\x01` and is wiped after ranking.
The binary body has a count followed by that many text paths, then a count followed by that many vector paths.
A text path stores its length-prefixed UTF-8 alias, 16-byte source column identity and length-prefixed UTF-8 value.
A vector path stores its length-prefixed UTF-8 column, a count followed by that many IEEE 754 Float32 components, and a count followed by that many filters.
A filter stores its length-prefixed UTF-8 column and length-prefixed raw sort key.
Counts, byte lengths and Float32 components are little-endian and four bytes wide.
The Bijection writer and RTDB reader use this body in the same release.
In current ranking, a fenced or retired derived winner continues to shadow a parent row and contributes no ranking input.
At an exact earlier cut, a fenced or retired winner refuses with `rtdb/erased-under-view` before text, nearest or hybrid ranking can return an altered corpus.
Historical nearest ranks the exact corpus.
Protected-row payload carriers retain their `BJWC1` framing.
Current and accepted-cut point reads can return the opaque carrier with the resolver's authoring line and ordinal.
Resolution uses that original authoring line for a winner inherited from a parent world.

## World overlay admission

Writable world overlays admit the installed custody controls `fact_unit_fences`, `fact_unit_column_payloads`, `fact_row_probe_floors`, `fact_row_legal_holds`, `fact_row_publications` and `fact_sealed_search_erasures` at the full valid interval.
An insert still requires original absence for its identity.
A checked Proposal settlement requires the matching point witness for each effect, and the existing bounded-period rules apply to ordinary facts.
The resolver checks each payload against the world’s installed schema.
A sealed derived projection is an opaque binary field on its declared search relation, carried as binary through source and resolved archive replay.

### Protected prepared receipt placement

Hosted protocol 218 admits a matched protected receipt and proof under `root_proposal_private_landing:` and `root_proposal_private_landing_proof:` in the existing hosted admission relation.
Only the original placed writer can read those controls.
The prepared audience has no access, and the protected switch cannot advance public publication frontiers or emit public task intents.
The switch retains the applied receipt and proof with the destination at one order.
The constraint-refusal pair keeps the same request, cleared activation and empty candidate roots.
Bijection owns the equivalent audience check before probing a protected source.

## Fence semantics across dependent forks

An ordered family fence takes effect at the original source publication and closes every world at once.
While the claim is pending, release checks that source before opening a capability.
The vault records the first order after reading the accepted marker.
From the fence commit:

* `RequireLiveFamilyOccurrence.v1`, `OpenWorldOverlayRow.v1`, `SealWorldOverlayRow.v1` and every registration refuse for the family;
* a world overlay row sealed under a member or derived capability stops opening in the world that wrote it and in every world forked from it, because a forked world inherits the same envelope and the same capability;
* a dependent hosted fork never opens the family at all, since its deployment identity addresses a different vault record and its capabilities name another deployment.

Root derived rows carry no family capability.
For an indexed first publication the same private prepared switch creates the Root carrier, row journal and physical projection.
It checks their exact absence, the absence of an ordinary projection at that AddressRef, and the absence of the ordered family fence.
The Proposal's raw-address projection, row carrier and protected journal must be witnessed at the same valid instant and ordered source transaction under the original pin.
The search relation must be the exact journal relation with `_search` appended.
The original placed vault authenticates the projection's exact reservation and inventory entry before generation staging.
The switch may also create missing, exactly checked search-relation aliases used by the existing label-unit erasure path.
Those aliases must name the co-ordered derived relation.
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
