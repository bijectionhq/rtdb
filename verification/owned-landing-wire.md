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
In `DecideHeldWorlds.v1` the slot begins after the one-byte flags, optional 16-byte clock-slot hold and eight-byte deadline.
In `PublishHeldWorldRetention.v3` it follows the 22-byte attempt identity after the hold, system revision, child, expiry and deadline.
In `PublishWorldCut.v1` and `DecideWorldCut.v1` it follows the 22-byte attempt identity after the hold, world, pin, previous hold, capture, suffix, expiry, deadline and clock mode with any mode operand.
The action decoder reads the same 32 bytes and continues at the first byte after the slot.

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

The checked field decisions use Arrow IPC and Arrow cell access with their lossless serialization and typed name lookup semantics.
Revision text uses the standard canonical signed decimal formatter and parser.
The hash uses SHA-256's ordered update semantics.
The ordered resolver's comparison, marker write, and selection of the held record are separate source obligations.

RTDB's decoder laws are in [owned_landing_wire](verus/owned_landing_wire/laws.rs).
Bijection checks the encoder and hash input selection in its `factstore_landing` Verus area.
