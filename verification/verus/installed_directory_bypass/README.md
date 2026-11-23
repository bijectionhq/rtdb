# Installed journal directory bypass guards (R9)

D05 reserves the installed deployment's canonical UUID before a mixed cell shares the serving and checkpoint media with directory administration.
The laws import the memory reservation guard, the reservation fields and condition strings used by DynamoDB, and the node's directory route, move park and pressure guards.
The reference is [`RTDB.InstalledJournalGuards`](../../multi/lean/InstalledJournalGuards.lean).

| Shipped Rust decision | Lean definition and law |
| --- | --- |
| `memory_allows_insert` | `memoryAllowsInsert` and `reserved_identity_refuses_directory_insert` |
| `memory_allows_reservation` | `memoryAllowsReservation` and `directory_record_refuses_reservation` |
| `reservation_fields`, `NEW_ITEM`, `EXACT_VERSION` | `reservationFields`, `dynamoAllowsWrite` and `dynamo_reservation_refuses_directory_write` |
| `directory_route_allowed` | `directoryRouteAllowed` and `installed_name_refuses_directory_route` |
| `directory_move_park_allowed` | `directoryMoveParkAllowed` and `installed_name_refuses_directory_move_park` |
| `pressure_candidate` | `pressureCandidate` and `journal_holding_is_never_a_pressure_candidate` |

The bundled Lean theorem `directory_effect_guards_exclude_installed` uses the canonical UUID's installed reservation, the configured name's manifest entry and the installed holding's journal authority as premises.
The directory may have a separate record under the same display name for a different UUID.
The manifest controls the installed name on the node, and the reservation protects the installed UUID in the backend.
The memory backend checks reservation and insertion under the same state lock.
The DynamoDB proof checks the actual field list and expression spellings used by the backend.
For the abstract conditional write, an occupied primary key fails the fresh insert and an absent version fails expected-version equality.
DynamoDB's atomic conditional writes and transactions supply that semantics.
The SDK builder, table and expression bindings, item decoding, directory transport, manifest setup, and non-agent effect paths remain covered by the [effect-path audit](../../redesign/installed-journal-bypass.md).
The two backend cases use `id` and `version` from the deployed directory schema.

On the node, `require_directory_route` guards directory record resolution, wire park and flush, fragments and relay writes.
Move park checks the manifest separately since local shutdown can still vacate the journal grant.
Background compactor assignment shares the route decision.
The pressure predicate filters by `HoldingAuthority::Directory` before the candidate list is processed.
The proof checks selection, with the manifest and holding authority projections supplied by the caller.
Directory record resolution, the pressure candidate's subsequent activity checks, and awaiting or performing effects are outside Verus.
