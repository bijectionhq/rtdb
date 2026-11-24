# R11 · Exact-head survivor checkpoint

The baseline verifies the event decisions called by `Events::add`, the trie decision used by `Candidate::write_trie`, the fresh-allocation guard in `ErasureCoordinator::stage_survivor`, and the member check before `CheckpointCustody::prepare` inserts a manifest entry.
The harness imports those functions from their production source files.

| Law | Lean reference in [SurvivorCheckpoint](../../multi/compose/SurvivorCheckpoint.lean) | Production inputs |
| --- | --- | --- |
| `selects_exactly_the_surviving_events` | `survivorEvents`, `keepInheritedEvent`, `keepLocalEvent`, `beforeMarker` | The captured event inventory, the authenticated origin selection, and the greatest erase-marker position for the event's table and IID. Transaction ID is signed and the intra-transaction ordinal is unsigned. |
| `writes_a_trie_exactly_for_nonempty_survivors` | `writeSurvivorTrie` | The rebuilt live table's row count. |
| `declares_exactly_written_members` | `freshSurvivorAllocation`, `declaresWrittenMember`, `survivorMembers` | The candidate and prior allocation IDs, the observed address compared with the just-written address, and its observed extent. The address includes the allocation prefix and logical member path. |

The event law assumes the exact-head inventory and selector projections.
The member law assumes a successful write followed by a provider observation of its address and extent.
The new catalog starts from empty trie state, and the installed custodian separately authenticates the selected head and reopens the staged closure.
Replay completeness, source authentication, Arrow encoding, catalog traversal and provider effects remain outside these proofs.

The native `survivor_checkpoint_matches_the_exact_head_event_and_member_sets` fixture reopens a persisted native base, adds a local erase and a later put, and rewrites a captured head with an inherited-origin exclusion.
It decodes the root, table blocks and Arrow data, compares the full fixture event map, and checks exact equality between the declared and written object sets.
An append beyond the captured head and every path from the prior fixture checkpoint are absent from the result.
