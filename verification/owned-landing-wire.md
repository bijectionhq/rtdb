# FactStore landing Flight wire law

The hosted control group stream has a big-endian unsigned 16-bit group count.
Each group has a big-endian unsigned 16-bit relation byte length, the exact UTF-8 relation bytes, a big-endian unsigned 32-bit IPC byte length, and that exact Arrow IPC stream containing one batch.
A hosted control identifies its fields by name.
The installation comparison is one row in relation `bj_installation`: `_id` is UTF-8 `authority`, `_expected_micros` is a non-null signed Int64 microsecond stamp, and `_check_only` is Boolean true.
The admitted installation stamp is nonnegative.

For a check-only row, normalization unions the prior absent names with all non-control columns whose row cell is null.
Control names begin with `_`.
If no row needs omission, the batch is unchanged.
Otherwise normalization replaces or appends the `_absent` list of UTF-8 names and preserves the order and cells of all other columns.
The receiver strips only the listed null or undefined payload cells and decodes the three control fields by name.
The decoded comparison has the same relation, identity, check intent and exact stamp as the admitted row.

Each world publication replaces one 32-byte zero reservation in its prepared request with the submitted fingerprint, preserving the prefix and suffix.
In `DecideHeldWorlds.v1` and `SwitchPreparedProposal.v1` the slot begins after the one-byte flags, optional 16-byte clock-slot hold and eight-byte deadline.
The prepared Proposal switch uses the same decoder and requires the clock offer and Root kind 6.
In `PublishHeldWorldRetention.v3` it follows the 22-byte attempt identity after the hold, system revision, child, one-byte clock mode, expiry and deadline.
In `PublishWorldCut.v1` and `DecideWorldCut.v1` it follows the 22-byte attempt identity after the hold, world, pin, previous hold, capture, suffix, expiry, deadline and clock mode with any mode operand.
The action decoder reads the same 32 bytes and continues at the first byte after the slot.

On the co-ordered Root cut action, clock mode 12 carries an eight-byte signed
big-endian microsecond timestamp before the attempt identity. It selects an
exact retained Root publication under the original live hold. The previous
hold, capture and suffix fields are zero. The receiver resolves the original
transaction position, derives the archive and admits hosted controls only.
Registration checks the publication at its original position and orders the
current hosted authority controls independently of the current Root tip.

A held point response with a visible marker carries that row's `fingerprint` binary cell and the same row's signed microsecond system-from revision in schema metadata `rtdb.owned.revision_micros`, rendered as canonical decimal.
A missing point carries neither a row nor a revision.
Decoding the response preserves the fingerprint and revision of the selected row exactly.
The revision can precede the held present coordinate.

For Bijection's `OrderedAttempt::world_cut`, the fingerprint is SHA-256 over these consecutive parts.

1. The literal domain `bijection.native.world_cut_attempt.v1` without a length prefix.
2. The UTF-8 actor component, with an unsigned 64-bit big-endian byte length.
3. One role presence byte, followed for a present role by its length-prefixed UTF-8 bytes.
4. The length-prefixed UTF-8 clearance, followed by the unsigned 64-bit big-endian tablet count and the original frame's 16-byte tablets in set order.
5. The original context deadline as unsigned 64-bit big-endian nanoseconds.
6. The exact original prepared body with its zero fingerprint reservation, prefixed by its unsigned 64-bit big-endian byte length.

The original issued context identifies the attempt in the prepared body and the marker lookup.
The retained-world variant uses the separate `bijection.native.retained_world_cut_attempt.v1` domain.
The Proposal landing switch calls `OrderedAttempt::prepared_proposal` with the same field layout under `bijection.native.prepared_proposal_attempt.v1`.

The checked field decisions use Arrow IPC and Arrow cell access with their lossless serialization and typed name lookup semantics.
Revision text uses the standard canonical signed decimal formatter and parser.
The hash uses SHA-256's ordered update semantics.
The ordered resolver's comparison, marker write, and selection of the held record are separate source obligations.

RTDB's decoder laws are in [owned_landing_wire](verus/owned_landing_wire/laws.rs).
Bijection checks the encoder and hash input selection in its `factstore_landing` Verus area.

## Populated clear relation adoption

The private hosted Root decision accepts one fact group named by the predecessor layout.
Its single Arrow row has four non-null columns: Utf8 `_id` equal to the predecessor, Utf8 `_op` equal to `adopt-relation`, Utf8 `destination` equal to the staged layout, and signed Int64 `retain_micros` using the owned retirement bound.
The existing placed writer proof, create-only attempt marker, expected Root catalog and declaration controls, and held conditions travel in the same decision.

Use the same Bijection Root hold for all four reads.
For each endpoint call `HeldRead::read_point` on `fact_publications` with `point_key(fact_partition_head(relation), None)` at held present.
Turn the source's `Point::stamp` into its existing point check with both the SQL history microseconds and the 16-byte event position.
Turn the destination result into an absence check at that same present.
For each endpoint call the existing `ordered_population` on that hold with the installed empty-key population order from its layout.
Submit the returned full-span witness as `Evidence::IndexReplacementPopulation` through the durable range witness stream.
RTDB requires exactly one range witness per endpoint, the same witnessed transaction key and placed scope for both, a live witness deadline, and an exact match to each durable data frontier.
The source must have an original partition head and positioned history.
The staged destination must have a zero data frontier and a virgin data, journal and purge inventory.
The receiver compares the two witness frontiers to the durable predecessor, then derives the data and journal event counts for that same predecessor.
The population returned by a held count describes winners at its valid instant.
The receiver's data inventory includes every original put and delete and its journal inventory includes the coverage floor, so neither inventory is a caller-supplied field.

Carry the adoption group in the existing Bijection deployment attempt fingerprint.
The create-only marker and the placed outcome are installed with the successor and predecessor retirement.
Recover an ambiguous submission through the original placed outcome, or recover the matching fingerprint from the original Bijection attempt marker as other hosted releases do.
A repeated decision never copies the history a second time because its checked predecessor and virgin destination have already moved.
A different fingerprint under the same marker cannot recover that marker as its own outcome.
A fresh Flight submission has its own placed receipt and is subject to the same predecessor comparisons.

The production resolver still uses the [hosted decision checks](verus/owned_flight_decision/area.json) for installed authority, predecessor stamp equality and primary publication.
Adoption uses the same `revision_matches` decision for its held population frontiers.
The [read-back identity area](verus/readback_identity/area.json) covers the existing journal body confirmation used to install the ordered transaction.
The [change group area](verus/owned_change_group/area.json) covers the checked journal sequence used by the successor feed.
Relocation's complete file census, canonical-to-compound cell correspondence, and the composition through replica replay and erasure are additional source obligations.

### Clear cell correspondence

The current execution seam is `OwnedSchema::clear_compound_successor` and the returned `CompoundAdoption::convert`.
RTDB executes it over each original clear cell read from its own bounded history census.
It implements the concrete subset of Bijection's canonical grammar for tags 2, 3, 4, 5, 7, 8, 9, 10, 12 and 13.
Those tags carry null, Boolean, signed Int64, Float64 with preserved signed zero and canonical NaN, Date32, UTC microsecond Instant, Utf8 and Binary with canonical NUL escapes, recursively admitted lists, and closed records with sorted keys.
The conversion also implements Bijection's `v2_value`, `value_` and `is_present_` layouts and selects union legs by their admitted canonical value tag.
An explicit top-level canonical null stays in the declared Binary leg, and a missing field stays absent.
Unsupported scalar carriers, Decimal, references, clock or revision terms, open records, and values outside the installed compound shape refuse adoption.

The receiver's obligation is to establish exact correspondence against the source bytes it opened itself.
It must also establish one-to-one coverage at the original event coordinates, so a candidate digest or successful Arrow shape check alone cannot authorize replacement.
The value mapping can be supplied by the shared native_columnar owner once its concrete promotion is available in a receiver-linkable component with a bounded canonical input, exact installed compound declaration, explicit presence and null semantics, and a funded Arrow output.
RTDB can execute that same mapping over its checked inventory and build the ordered baseline.
If the caller supplies promoted Arrow instead, RTDB still needs a receiver-executed equivalence check for every positioned cell against its original canonical bytes before staging the candidate.
The current local subset and its source cases provide no correspondence evidence for the refused shapes.
