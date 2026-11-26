//! Exact derived maintenance (`store_read::derive`, T3 converged): the one
//! function that keeps every derived key of a row at the store's ordering
//! point, also called retire-on-write.
//!
//! A row's derived keys are everything the host keeps about it outside its
//! events: index markers (ordered, search, vector and spatial entries),
//! counted search statistics, maintained timeline aggregates and its
//! lifetime expiry entry. The writer plans them from the NEW image only and
//! reads nothing; the store keeps the row's *derived line* (its
//! [`Manifest`], an interval map over valid time) at the row's own place, and
//! at ordering [`apply`] diffs the line against the new entries and emits
//! only what changed. Strict and blind writes use the same law, so one
//! maintenance rule covers every write, and conflicts and wakes are exact per
//! row: a write that leaves an entry unchanged writes no key for it.
//!
//! Five entry kinds, five exact laws:
//! - [`EntryKind::Marker`] (path and posting entries): a stamped enter where
//!   coverage begins and a stamped leave marker (empty value) where an entry
//!   the new image does not re-enter loses coverage. Readers resolve each
//!   candidate at their cut, as for every path entry.
//! - [`EntryKind::Boundary`] (counted search statistics): a stamped
//!   precomputed delta. The row contributes the entries of its segments that
//!   reach the line's horizon (the furthest valid instant any of its writes
//!   asserted), so the store emits exactly the multiset difference of that
//!   live set: an identical re-put emits nothing.
//! - [`EntryKind::Edged`] (timeline aggregates, the D63 boundary deltas): a
//!   record keyed by its valid-time edge, `identity ‖ ordered(edge) ‖ stamp ‖
//!   ordinal`, `+v` where coverage begins and `−v` where it ends (an open end
//!   records nothing). A read at valid instant `t` sums the records with edge
//!   `≤ t`, so a total is right at every instant with no scheduled work. This
//!   replaces the pipeline's own `slice_boundaries` records (DECISIONS-APPROVED
//!   ruling): both shells use this one mechanism.
//! - [`EntryKind::Single`] (expiry entries): one key, deleted when the entry
//!   loses coverage.
//! - [`EntryKind::Claim`] (unique claims, T7-4): an edged count of the
//!   claimed value, which the store judges once the append applies: no
//!   instant of the claim's coverage may count two claimants
//!   ([`claim_exceeded`]). This is the one judgment that may refuse an
//!   append, and it reads only the claimed value's records at the row's
//!   place and the levels it inherits.
//!
//! An entry's identity may carry placeholders the store fills at ordering:
//! a [`KeyStamp`] (the 16-byte stamp, for a `t.accepted()` column) and any
//! number of [`KeyInstant`]s (the stamp's micros plus an offset in the
//! instant encoding, for a value whose time is acceptance-relative: a
//! default-validity series `at` or timeline `validFrom`, an aggregate
//! boundary at the stamp, a field placed as `now + d`, wherever an ordered,
//! claim, vector filter, spatial partition or aggregate group key holds
//! it). Each is escaped for the segments enclosing it, so a filled key
//! sorts where its value does. Its values may carry [`ValueInstant`]s, the
//! stamp added to a maintained sum over such a field. The engine plans every
//! template from the new image; this function alone fills them, at the
//! stamp, before it diffs the line (T7-4: one owner of the fill).
//!
//! A row's line also keeps the exact inventory of every key it ever
//! emitted (each manifest version's `written_keys`): markers, singles,
//! statistics deltas and unretained edged records, and the entries a path
//! backfill wrote for its history ([`backfill`]). Erasure purges that whole
//! history ([`purge`], erase-derive): every one of those keys is deleted at
//! every stamp it was written, so no read at any cut, before the erasure or
//! after it, counts or reveals the erased row through a derived key. Only an
//! `Edged { retain: true }` contribution (`onErase: "retain"`) survives. A
//! compaction base that already folded some of those keys in is re-derived
//! without them by the engine in the same commit
//! (`owned_relations::owned::OwnedStore::rebase_without`), from the line as
//! of the base's cut ([`line_through`]).
//!
//! The function is pure, total and deterministic: [`apply`] and [`purge`]
//! return a result for every input, and a malformed derive is a request error
//! [`check`] decides before ordering. Every emitted key lies at the
//! manifest's own `(partition, world)` (P10, `emission_confined`), and
//! [`apply`] judges nothing: its outcome never depends on a manifest (P8).
//! The claim judgment ([`claims`], [`claim_levels`], [`claim_record`],
//! [`claim_exceeded`]) runs after every derive of the append applied, over
//! the claimed values' records, never over a manifest. Emitted writes count
//! toward no request limit.
//!
//! Both `fact_store::persistence::memory::MemoryPersistence` and RTDB's
//! resolver (`rtdb-index::branches::store::resolve_derives`, via the vendored
//! `vendor/bijection-store-derive`) run these exact bytes. The module imports
//! nothing outside `std`, so RTDB vendors it verbatim (pinned by SHA-256,
//! `DERIVE_DIGEST`), and the Verus area `owned_multi_place` includes these
//! exact bytes by `#[path]`.
//!
//! EVERY VERUS ANNOTATION IS `cfg`-GATED ON `verus_keep_ghost`: an ordinary
//! build gains no dependency and no code.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// Reserved base ordinal for writes emitted by `Derive`: above plain
/// mutation ordinals (`0..2^32`) and world ordinals (`2^32..2^48`), so
/// derived stamped keys never collide with plain writes or with each other.
pub const DERIVE_ORDINAL_BASE: u64 = 1u64 << 48;

/// Encoded width of a store stamp (micros big-endian with flipped sign bit,
/// then version big-endian).
#[cfg_attr(verus_keep_ghost, verus_verify)]
pub const STAMP_BYTES: usize = 16;

/// A stored key's place is its partition prefix (a big-endian `u16` length,
/// then that many bytes), a family byte, then its world as one escaped
/// segment (`store_read::layout::stored`): `partition ‖ family ‖
/// segment(world) ‖ …`. Two keys share a place when their partition prefixes
/// and world segments are equal, whatever their families: a row's manifest
/// (family `Row`), its markers (`Index`) and its statistics, aggregates and
/// expiry entries (`Persistence`) all lie at the row's place.

/// Encoded width of a precedence (`stamp || ordinal`).
pub const PRECEDENCE_BYTES: usize = 24;

/// Encoded width of a valid-time edge in an [`EntryKind::Edged`] key.
pub const EDGE_BYTES: usize = 8;

/// The open end of valid time (`store_read::event::VALID_MAX`): an interval
/// ending here records no end.
pub const OPEN_END: i64 = i64::MAX;

/// A half-open valid-time interval `[from, to)` on the `i64` axis. An empty
/// one (`from >= to`) changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct ValidInterval {
    pub from: i64,
    pub to: i64,
}

/// One bound of a derive's valid interval, resolved once the store assigns
/// the append's stamp (T7: a bound named relative to the present takes the
/// stamp).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub enum DeriveBound {
    /// A fixed instant ([`OPEN_END`] for an open end).
    At(i64),
    /// The stamp's micros plus this offset (saturating): a default-validity
    /// start (`Stamp(0)`), a lifetime's end (`Stamp(lifetime)`) or any
    /// period bound placed as `now + d`.
    Stamp(i64),
}

/// A derive's half-open valid interval `[from, to)`, each bound fixed or
/// acceptance-relative. A resolved interval may be empty, and an empty one
/// changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct DeriveInterval {
    pub from: DeriveBound,
    pub to: DeriveBound,
}

impl DeriveInterval {
    /// The fixed interval `[from, to)`.
    pub const fn fixed(from: i64, to: i64) -> Self {
        Self { from: DeriveBound::At(from), to: DeriveBound::At(to) }
    }

    /// `[stamp, to)`: default validity.
    pub const fn from_stamp(to: i64) -> Self {
        Self { from: DeriveBound::Stamp(0), to: DeriveBound::At(to) }
    }
}

/// How an entry's identity key carries the append's commit stamp (for
/// `t.accepted()` fields indexed by a path).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub enum KeyStamp {
    /// Overwrite 16 zero bytes at `offset` in `identity` with the raw stamp.
    Raw(u32),
    /// Replace 16 zero bytes at `offset` in `identity` with the segment-escaped
    /// stamp (`0x00 -> 0x00 0xff`), preserving escaped-segment order (F4).
    EscapedSegment(u32),
    /// Replace 16 zero bytes at `offset` in `identity` with the double-escaped
    /// stamp (`0x00 -> 0x00 0xff 0xff`), for a stamped column nested inside
    /// `path_values`'s segment inside `path_row`'s segment (`t.accepted()`).
    DoubleEscapedSegment(u32),
}

/// Encoded width of an instant-encoded stamp placeholder ([`KeyInstant`]).
pub const INSTANT_BYTES: usize = 8;

/// The deepest segment nesting a [`KeyInstant`] may be escaped for.
pub const MAX_INSTANT_ESCAPES: u8 = 4;

/// An instant-encoded stamp placeholder inside an entry's `identity` (T7:
/// an acceptance-relative write takes the commit's stamp). The
/// [`INSTANT_BYTES`] zero bytes at `offset` become the stamp's micros plus
/// `plus_micros` (saturating) in the instant encoding: big-endian with the
/// sign bit flipped, the first eight bytes of the stamp's encoding and the
/// order bytes of an Instant or Int64 sort key or fact cell. Each zero byte
/// of the fill is followed by `escapes` bytes `0xff`, one per escaped
/// segment enclosing the placeholder, so the key keeps every enclosing
/// segment's order. A series sample's `at`, a timeline fact's `validFrom`,
/// a maintained aggregate's boundary and any field placed as `now + d`
/// carry one wherever their value sits in an index key.
#[cfg_attr(not(verus_keep_ghost), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct KeyInstant {
    pub offset: u32,
    pub escapes: u8,
    pub plus_micros: i64,
}

/// Encoded width of a maintained sum a [`ValueInstant`] adds to: a
/// big-endian `i128`, as `store_read::tally` writes each measure.
pub const SUM_BYTES: usize = 16;

/// An acceptance-relative measure inside an entry's values (T7-4): the
/// store adds the stamp's micros plus `plus_micros` (saturating) to the
/// big-endian `i128` at `offset` of `enter_value` and subtracts it at the
/// same offset of `leave_value`, before the diff. A maintained aggregate's
/// sum over a field placed as `now + d` counts the stamp, never the entry
/// instant.
#[cfg_attr(not(verus_keep_ghost), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ValueInstant {
    pub offset: u32,
    pub plus_micros: i64,
}

/// The kind of one derived entry on a row's derived line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub enum EntryKind {
    /// Path, posting, vector or spatial marker:
    /// enter writes `identity ‖ stamp ‖ ordinal → enter_value`;
    /// leave writes `identity ‖ stamp ‖ ordinal → []`.
    Marker,
    /// Counted search statistics delta, live while its segment reaches the
    /// line's horizon: enter writes `identity ‖ stamp ‖ ordinal →
    /// enter_value`, leave writes `identity ‖ stamp ‖ ordinal → leave_value`.
    Boundary,
    /// Timeline aggregate delta, keyed by valid-time edge: coverage of
    /// `[from, to)` writes `identity ‖ ordered(from) ‖ stamp ‖ ordinal →
    /// enter_value` and, when `to` is not [`OPEN_END`], `identity ‖
    /// ordered(to) ‖ stamp ‖ ordinal → leave_value`. `retain`: a purge
    /// (erasure) keeps the contribution across history (`onErase:
    /// "retain"`), so its records are kept out of the purge inventory.
    Edged { retain: bool },
    /// Single-key derived entry (expiry index):
    /// enter writes `identity ‖ stamp ‖ ordinal → enter_value` (when
    /// `stamped`) or `identity → enter_value`; leave deletes that exact key.
    Single { stamped: bool },
    /// A unique claim (T7-4): the claimed value's count, kept as an
    /// [`EntryKind::Edged`] record pair under the value's identity
    /// (`identity ‖ ordered(edge) ‖ stamp ‖ ordinal → enter_value` where
    /// coverage begins, `→ leave_value` where it ends), each value a
    /// big-endian `i64` count (`+1`, `−1`). Once the append's derives apply,
    /// the store sums the identity's records over the derive's levels and
    /// refuses the whole append when any instant of the claim's new coverage
    /// counts more than one claimant ([`claim_exceeded`]): the one judgment
    /// of a unique claim, made on the filled key at ordering. Its records
    /// are listed in the purge inventory, so an erasure deletes them.
    Claim,
}

/// One derived entry planned by the writer for the new image.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct DeriveEntry {
    pub kind: EntryKind,
    /// Key prefix (or full key for an unstamped `Single`), confined to the
    /// target `(partition, world)`.
    pub identity: Vec<u8>,
    /// Optional stamp placeholder inside `identity`.
    pub key_stamp: Option<KeyStamp>,
    /// Instant-encoded stamp placeholders inside `identity`, disjoint from
    /// each other and from `key_stamp`'s region.
    pub key_instants: Vec<KeyInstant>,
    /// Acceptance-relative measures inside the values, disjoint sums.
    pub value_instants: Vec<ValueInstant>,
    /// Value written where coverage begins.
    pub enter_value: Vec<u8>,
    /// Value written where coverage ends (`[]` for `Marker` and `Single`,
    /// the negated delta for `Boundary` and `Edged`).
    pub leave_value: Vec<u8>,
}

/// One fallback level where a previous derived line may live:
/// `(manifest_prefix, through_stamp)`. `through` is `None` for the target
/// world's own live overlay, or `Some(fork_stamp)` for an inherited level.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct DeriveLevel {
    pub prefix: Vec<u8>,
    pub through: Option<[u8; STAMP_BYTES]>,
}

/// What a [`Derive`] write does to the row's derived line.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub enum DeriveMode {
    /// Replace `interval` on the derived line with `entries`, emitting only
    /// changed entries plus the new manifest version.
    Replace {
        interval: DeriveInterval,
        entries: Vec<DeriveEntry>,
    },
    /// Purge (`Erase`, `Expire`): delete every key the row's line ever
    /// emitted, recorded across all manifest versions (markers, singles,
    /// statistics deltas and unretained edged records, at every stamp), and
    /// delete the manifests. Nothing is retracted at the purge's stamp: the
    /// row's whole derived history goes, so a read at any cut sums and finds
    /// exactly what it would had the row never been written (erase-derive).
    Purge,
    /// A path backfill of the row's history (`install_path` over rows held
    /// before the path existed): write each entry as given (its key already
    /// names the stored precedence of the event it indexes) and add every
    /// one to the line's inventory, so a purge deletes them with the rest.
    /// The line's segments are unchanged (erase-derive).
    Backfill { writes: Vec<(Vec<u8>, Vec<u8>)> },
}

/// One derived write of a store append (`fact_store::persistence::Derive`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct Derive {
    /// The target row's derived-line prefix: `row(partition, world) ‖ DERIVED`.
    pub manifest_prefix: Vec<u8>,
    /// Ordered lookup levels (target world first, then ancestors and Root).
    pub levels: Vec<DeriveLevel>,
    pub mode: DeriveMode,
}

/// One segment stored in a row's derived-line manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct Segment {
    pub from: i64,
    pub to: i64,
    pub kind: EntryKind,
    pub identity: Vec<u8>,
    pub enter_value: Vec<u8>,
    pub leave_value: Vec<u8>,
    /// Concrete stored key of a live `Single` entry (empty otherwise).
    pub live_key: Vec<u8>,
}

/// One stored manifest version under `manifest_prefix ‖ stamp ‖ ordinal`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct Manifest {
    /// The row's derived line: an interval map of derived entries.
    pub segments: Vec<Segment>,
    /// The furthest valid instant any write of the row asserted
    /// (`i64::MIN` for an empty line): a `Boundary` entry is live while its
    /// segment reaches it.
    pub horizon: i64,
    /// Every concrete key this step emitted: `Marker` enters and leaves,
    /// `Single` keys, `Boundary` deltas and unretained `Edged` records (the
    /// exact inventory `Purge` deletes across history, so no erased value or
    /// contribution survives in any index, statistic or aggregate at any
    /// cut). Retained `Edged` records are never listed: they outlive erasure.
    pub written_keys: Vec<Vec<u8>>,
}

impl Manifest {
    /// The empty line: no segment, no horizon.
    pub fn empty() -> Self {
        Self { segments: Vec::new(), horizon: i64::MIN, written_keys: Vec::new() }
    }
}

/// One concrete key-value write emitted by [`apply`] or [`purge`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct EmittedWrite {
    pub key: Vec<u8>,
    /// `None` deletes `key`.
    pub value: Option<Vec<u8>>,
}

/// The output of [`apply`]: concrete writes to apply (in order), the new
/// manifest to store at `manifest_prefix ‖ stamp ‖ ordinal`, and the next
/// unused ordinal.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct Applied {
    pub writes: Vec<EmittedWrite>,
    pub manifest: Manifest,
    pub next_ordinal: u64,
}

/// A validation or codec refusal on a [`Derive`] or [`Manifest`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(verus_keep_ghost, verifier::external_derive)]
pub struct DeriveError {
    pub message: &'static str,
}

impl DeriveError {
    pub const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

#[cfg(not(verus_keep_ghost))]
impl std::fmt::Display for DeriveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

#[cfg(not(verus_keep_ghost))]
impl std::error::Error for DeriveError {}

#[cfg(verus_keep_ghost)]
verus! {

/// Whether two half-open intervals `[a_from, a_to)` and `[b_from, b_to)` overlap.
pub open spec fn meets_spec(a_from: i64, a_to: i64, b_from: i64, b_to: i64) -> bool {
    a_from < b_to && b_from < a_to
}

/// Left remainder of `[seg_from, seg_to)` after subtracting `[cut_from, cut_to)`.
pub open spec fn trim_left_spec(seg_from: i64, seg_to: i64, cut_from: i64, cut_to: i64) -> Option<(i64, i64)> {
    if !meets_spec(seg_from, seg_to, cut_from, cut_to) {
        Some((seg_from, seg_to))
    } else if seg_from < cut_from {
        Some((seg_from, cut_from))
    } else {
        None
    }
}

/// Right remainder of `[seg_from, seg_to)` after subtracting `[cut_from, cut_to)`.
pub open spec fn trim_right_spec(seg_from: i64, seg_to: i64, cut_from: i64, cut_to: i64) -> Option<(i64, i64)> {
    if meets_spec(seg_from, seg_to, cut_from, cut_to) && cut_to < seg_to {
        Some((cut_to, seg_to))
    } else {
        None
    }
}

/// Whether instant `t` belongs to `[from, to)`.
pub open spec fn in_interval(t: i64, from: i64, to: i64) -> bool {
    from <= t && t < to
}

/// Whether `t` is covered by at least one interval in `intervals`.
pub open spec fn point_covered(intervals: Seq<(i64, i64)>, t: i64) -> bool {
    exists|i: int| 0 <= i < intervals.len() && in_interval(t, #[trigger] intervals[i].0, intervals[i].1)
}

/// Whether every instant in `[from, to)` is covered by the union of `intervals`.
pub open spec fn interval_covered(intervals: Seq<(i64, i64)>, from: i64, to: i64) -> bool {
    forall|t: i64| in_interval(t, from, to) ==> point_covered(intervals, t)
}

/// Whether `a` and `b` share their first `len` bytes.
pub open spec fn prefix_eq(a: Seq<u8>, b: Seq<u8>, len: int) -> bool {
    len <= a.len() && len <= b.len() && forall|i: int| 0 <= i < len ==> #[trigger] a[i] == b[i]
}

/// The order-preserving `u64` image of an `i64` (sign bit flipped).
pub open spec fn ordered_spec(value: i64) -> u64 {
    (value as u64) ^ 0x8000_0000_0000_0000u64
}

/// The edged delta of one segment at instant `t`: `+1` inside its coverage
/// (`[from, to)`), `0` outside. The records an edged segment writes sum to
/// exactly this at every `t` (P9, `derive_is_interval_replace`).
pub open spec fn edged_coverage(from: i64, to: i64, t: i64) -> int {
    if in_interval(t, from, to) { 1 } else { 0 }
}

/// The records' prefix sum at `t` of one edged retraction over `[o_from,
/// o_to)`: `−1` at `o_from` and, unless open, `+1` at `o_to`.
pub open spec fn edged_retraction_sum(o_from: i64, o_to: i64, t: i64) -> int {
    (if o_from <= t { -1int } else { 0int }) + (if o_to != i64::MAX && o_to <= t { 1int } else { 0int })
}

/// P9 core: trimming `[seg_from, seg_to)` by `[cut_from, cut_to)` keeps
/// exactly the instants of `[seg_from, seg_to)` outside `[cut_from, cut_to)`.
pub proof fn lemma_trim_exact(seg_from: i64, seg_to: i64, cut_from: i64, cut_to: i64, t: i64)
    requires
        seg_from < seg_to,
        cut_from < cut_to,
    ensures
        (match trim_left_spec(seg_from, seg_to, cut_from, cut_to) {
            Some(l) => in_interval(t, l.0, l.1),
            None => false,
        } || match trim_right_spec(seg_from, seg_to, cut_from, cut_to) {
            Some(r) => in_interval(t, r.0, r.1),
            None => false,
        }) == (in_interval(t, seg_from, seg_to) && !in_interval(t, cut_from, cut_to)),
{
}

/// P9, edged: a retraction over the overlap `[o_from, o_to)` changes the
/// prefix sum of the records by exactly minus the overlap's coverage, so the
/// records keep summing to the line's coverage (the remainders') at every
/// instant before the open end.
pub proof fn lemma_edged_retraction_exact(o_from: i64, o_to: i64, t: i64)
    requires
        o_from < o_to,
        t < i64::MAX,
    ensures
        edged_retraction_sum(o_from, o_to, t) == -edged_coverage(o_from, o_to, t),
{
}

} // verus!

/// Whether two half-open intervals `[a_from, a_to)` and `[b_from, b_to)` meet.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == meets_spec(a_from, a_to, b_from, b_to)))]
pub fn meets(a_from: i64, a_to: i64, b_from: i64, b_to: i64) -> bool {
    a_from < b_to && b_from < a_to
}

/// Left remainder of `[seg_from, seg_to)` after replacing `[cut_from, cut_to)`.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == trim_left_spec(seg_from, seg_to, cut_from, cut_to)))]
pub fn trim_left(seg_from: i64, seg_to: i64, cut_from: i64, cut_to: i64) -> Option<(i64, i64)> {
    if !meets(seg_from, seg_to, cut_from, cut_to) {
        Some((seg_from, seg_to))
    } else if seg_from < cut_from {
        Some((seg_from, cut_from))
    } else {
        None
    }
}

/// Right remainder of `[seg_from, seg_to)` after replacing `[cut_from, cut_to)`.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == trim_right_spec(seg_from, seg_to, cut_from, cut_to)))]
pub fn trim_right(seg_from: i64, seg_to: i64, cut_from: i64, cut_to: i64) -> Option<(i64, i64)> {
    if meets(seg_from, seg_to, cut_from, cut_to) && cut_to < seg_to {
        Some((cut_to, seg_to))
    } else {
        None
    }
}

/// The order-preserving big-endian image of a valid-time edge (sign bit
/// flipped, as `store_read::stamp::ordered`).
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == ordered_spec(value)))]
pub fn ordered(value: i64) -> u64 {
    (value as u64) ^ 0x8000_0000_0000_0000
}

/// Whether the union of `intervals` covers every instant in `[from, to)`.
///
/// Advances a watermark `covered` from `from` toward `to`: every instant in
/// `[from, covered)` is covered, each pass that does not stop raises
/// `covered` strictly (so `to - covered` bounds the passes), and when no
/// interval extends `covered`, `covered` itself is an uncovered witness.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires from < to,
    ensures out == interval_covered(intervals@, from, to),
))]
pub fn covers_interval(intervals: &[(i64, i64)], from: i64, to: i64) -> bool {
    let mut covered = from;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        from <= covered,
        forall|t: i64| in_interval(t, from, covered) ==> point_covered(intervals@, t),
        decreases if covered < to { (to as int) - (covered as int) } else { 0 },
    ))]
    while covered < to {
        let mut next = covered;
        let mut i = 0usize;
        #[cfg_attr(verus_keep_ghost, verus_spec(invariant
            i <= intervals@.len(),
            from <= covered,
            covered < to,
            covered <= next,
            forall|t: i64| in_interval(t, from, next) ==> point_covered(intervals@, t),
            forall|k: int| 0 <= k < i && (#[trigger] intervals@[k]).0 <= covered ==> intervals@[k].1 <= next,
            decreases intervals@.len() - i,
        ))]
        while i < intervals.len() {
            let (a, b) = intervals[i];
            if a <= covered && b > next {
                next = b;
                #[cfg(verus_keep_ghost)]
                proof! {
                    assert forall|t: i64| in_interval(t, from, next) implies point_covered(intervals@, t) by {
                        if t < covered {
                            assert(in_interval(t, from, covered));
                        } else {
                            assert(in_interval(t, intervals@[i as int].0, intervals@[i as int].1));
                        }
                    };
                };
            }
            i += 1;
        }
        if next == covered {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(in_interval(covered, from, to));
                assert(!point_covered(intervals@, covered)) by {
                    if point_covered(intervals@, covered) {
                        let k = choose|k: int| 0 <= k < intervals@.len() && in_interval(covered, intervals@[k].0, intervals@[k].1);
                        assert(intervals@[k].0 <= covered);
                        assert(intervals@[k].1 <= next);
                    }
                };
            };
            return false;
        }
        covered = next;
    }
    true
}

/// Partition prefix length of `key`: a big-endian `u16` length `n` followed by
/// `n` partition bytes (`2 + n`).
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    match out {
        Some(n) => 2 <= n && n <= key@.len(),
        None => true,
    }
))]
pub fn partition_prefix_len(key: &[u8]) -> Option<usize> {
    if key.len() < 2 {
        return None;
    }
    let len = ((key[0] as usize) << 8) | (key[1] as usize);
    #[cfg(verus_keep_ghost)]
    proof! {
        let (high, low) = (key@[0], key@[1]);
        assert(((high as usize) << 8usize) | (low as usize) <= 0xffffusize) by (bit_vector);
    }
    if key.len() < 2 + len {
        return None;
    }
    Some(2 + len)
}

/// The end of the escaped segment that starts at `start` in `key` (past its
/// `0x00 0x01` terminator), when it is well formed.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn segment_end(key: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < key.len() {
        if key[i] == 0 {
            match key.get(i + 1) {
                Some(1) => return Some(i + 2),
                Some(0xff) => i += 2,
                _ => return None,
            }
        } else {
            i += 1;
        }
    }
    None
}

/// A stored key's place bounds: its partition prefix's length and the end
/// of its world segment, which follows the family byte.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn place_bounds(key: &[u8]) -> Option<(usize, usize)> {
    let p_len = partition_prefix_len(key)?;
    if key.len() <= p_len {
        return None;
    }
    Some((p_len, segment_end(key, p_len + 1)?))
}

/// Whether `a` lies in `b`'s partition.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn same_partition(a: &[u8], b: &[u8]) -> bool {
    match (partition_prefix_len(a), partition_prefix_len(b)) {
        (Some(pa), Some(pb)) => pa == pb && a[..pa] == b[..pb],
        _ => false,
    }
}

/// Whether `a` lies at `b`'s place: the same partition and the same world,
/// whatever its family (P10).
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn same_place(a: &[u8], b: &[u8]) -> bool {
    match (place_bounds(a), place_bounds(b)) {
        (Some((pa, ea)), Some((pb, eb))) => pa == pb && a[..pa] == b[..pb] && a[pa + 1..ea] == b[pb + 1..eb],
        _ => false,
    }
}

/// `key` re-rooted into the world whose escaped segment is `world`: its
/// partition, family and everything after its world are kept (P10). `None`
/// for a key with no well-formed place.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn reroot(key: &[u8], world: &[u8]) -> Option<Vec<u8>> {
    let (p_len, end) = place_bounds(key)?;
    let mut out = Vec::with_capacity(p_len + 1 + world.len() + key.len() - end);
    out.extend_from_slice(&key[..p_len + 1]);
    out.extend_from_slice(world);
    out.extend_from_slice(&key[end..]);
    Some(out)
}

/// Whether `a` and `b` share their first `len` bytes.
#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == prefix_eq(a@, b@, len as int)))]
pub fn same_prefix(a: &[u8], b: &[u8], len: usize) -> bool {
    if a.len() < len || b.len() < len {
        return false;
    }
    let mut i = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= len,
        len <= a@.len(),
        len <= b@.len(),
        forall|j: int| 0 <= j < i ==> #[trigger] a@[j] == b@[j],
        decreases len - i,
    ))]
    while i < len {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// `value`'s big-endian bytes. vstd does not specify `u64::to_be_bytes`;
/// the keys below only copy these bytes, so the type is the whole contract.
#[cfg_attr(verus_keep_ghost, verifier::external_body)]
fn be_bytes(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Build a stamped derived key `identity ‖ stamp ‖ ordinal.to_be_bytes()`,
/// preserving `identity` (and therefore its partition and world prefix).
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires stamp@.len() == 16,
    ensures
        out@.len() == identity@.len() + 24,
        prefix_eq(out@, identity@, identity@.len() as int),
))]
pub fn stamped_key(identity: &[u8], stamp: &[u8], ordinal: u64) -> Vec<u8> {
    let ord = be_bytes(ordinal);
    let mut out = Vec::with_capacity(identity.len().saturating_add(24));
    let mut i = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= identity@.len(),
        out@.len() == i,
        forall|j: int| 0 <= j < i ==> #[trigger] out@[j] == identity@[j],
        decreases identity@.len() - i,
    ))]
    while i < identity.len() {
        out.push(identity[i]);
        i += 1;
    }
    let mut s = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        s <= 16,
        stamp@.len() == 16,
        out@.len() == identity@.len() + s,
        forall|j: int| 0 <= j < identity@.len() ==> #[trigger] out@[j] == identity@[j],
        decreases 16 - s,
    ))]
    while s < 16 {
        out.push(stamp[s]);
        s += 1;
    }
    let mut o = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        o <= 8,
        out@.len() == identity@.len() + 16 + o,
        forall|j: int| 0 <= j < identity@.len() ==> #[trigger] out@[j] == identity@[j],
        decreases 8 - o,
    ))]
    while o < 8 {
        out.push(ord[o]);
        o += 1;
    }
    out
}

/// Build an edged derived key `identity ‖ ordered(edge) ‖ stamp ‖ ordinal`,
/// preserving `identity` (and therefore its partition and world prefix).
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires stamp@.len() == 16,
    ensures
        out@.len() == identity@.len() + 32,
        prefix_eq(out@, identity@, identity@.len() as int),
))]
pub fn edged_key(identity: &[u8], edge: i64, stamp: &[u8], ordinal: u64) -> Vec<u8> {
    let mut edged = Vec::with_capacity(identity.len().saturating_add(EDGE_BYTES));
    let mut i = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= identity@.len(),
        edged@.len() == i,
        forall|j: int| 0 <= j < i ==> #[trigger] edged@[j] == identity@[j],
        decreases identity@.len() - i,
    ))]
    while i < identity.len() {
        edged.push(identity[i]);
        i += 1;
    }
    let bytes = be_bytes(ordered(edge));
    let mut e = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        e <= 8,
        edged@.len() == identity@.len() + e,
        forall|j: int| 0 <= j < identity@.len() ==> #[trigger] edged@[j] == identity@[j],
        decreases 8 - e,
    ))]
    while e < 8 {
        edged.push(bytes[e]);
        e += 1;
    }
    let out = stamped_key(&edged, stamp, ordinal);
    #[cfg(verus_keep_ghost)]
    proof! {
        assert forall|j: int| 0 <= j < identity@.len() implies #[trigger] out@[j] == identity@[j] by {
            assert(out@[j] == edged@[j]);
        };
    };
    out
}

/// Resolve a `DeriveInterval` against the commit's `stamp_micros`. Total: a
/// resolved interval may be empty, and an empty one changes nothing.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn resolve_interval(interval: DeriveInterval, stamp_micros: i64) -> ValidInterval {
    let resolve = |bound: DeriveBound| match bound {
        DeriveBound::At(instant) => instant,
        DeriveBound::Stamp(plus) => stamp_micros.saturating_add(plus),
    };
    ValidInterval { from: resolve(interval.from), to: resolve(interval.to) }
}

/// The micros of an encoded stamp (its first eight bytes, big-endian with
/// the sign bit flipped).
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn stamp_micros(stamp: &[u8; STAMP_BYTES]) -> i64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&stamp[..8]);
    (u64::from_be_bytes(word) ^ (1 << 63)) as i64
}

/// The instant encoding of `stamp`'s micros plus `plus_micros`
/// (saturating): big-endian with the sign bit flipped.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn instant_fill(stamp: &[u8; STAMP_BYTES], plus_micros: i64) -> [u8; INSTANT_BYTES] {
    ((stamp_micros(stamp).saturating_add(plus_micros) as u64) ^ (1 << 63)).to_be_bytes()
}

/// `value` with the stamp's micros plus each instant's offset added
/// (`sign` 1) or subtracted (`sign` −1) at each sum of `instants`
/// (saturating). Total: a sum outside the value (which [`check`] refuses)
/// leaves it as it is.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn fill_values(value: &[u8], instants: &[ValueInstant], stamp: &[u8; STAMP_BYTES], sign: i128) -> Vec<u8> {
    let mut out = value.to_vec();
    for instant in instants {
        let start = instant.offset as usize;
        let Some(cell) = start.checked_add(SUM_BYTES).and_then(|end| out.get_mut(start..end)) else {
            continue;
        };
        let mut word = [0u8; SUM_BYTES];
        word.copy_from_slice(cell);
        let added = i128::from(stamp_micros(stamp).saturating_add(instant.plus_micros)).saturating_mul(sign);
        cell.copy_from_slice(&i128::from_be_bytes(word).saturating_add(added).to_be_bytes());
    }
    out
}

/// Every placeholder region of an entry: `(start, width, escapes)`, the
/// stamp's first, then each instant's.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn placeholder_regions(key_stamp: Option<KeyStamp>, key_instants: &[KeyInstant]) -> Vec<(usize, usize, usize)> {
    let mut regions = Vec::with_capacity(key_instants.len() + 1);
    match key_stamp {
        None => {},
        Some(KeyStamp::Raw(offset)) => regions.push((offset as usize, STAMP_BYTES, 0)),
        Some(KeyStamp::EscapedSegment(offset)) => regions.push((offset as usize, STAMP_BYTES, 1)),
        Some(KeyStamp::DoubleEscapedSegment(offset)) => regions.push((offset as usize, STAMP_BYTES, 2)),
    }
    for instant in key_instants {
        regions.push((instant.offset as usize, INSTANT_BYTES, usize::from(instant.escapes)));
    }
    regions
}

/// Fill every placeholder inside an entry's `identity` key: the stamp
/// placeholder with the stamp and each instant placeholder with its instant,
/// each escaped as its region asks, in offset order (offsets name the
/// template). Total: regions outside the identity or overlapping one another
/// (which [`check`] refuses) leave the identity as it is.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn fill_key_stamp(identity: &[u8], key_stamp: Option<KeyStamp>, key_instants: &[KeyInstant], stamp: &[u8; STAMP_BYTES]) -> Vec<u8> {
    let mut fills: Vec<(usize, usize, usize, Vec<u8>)> = Vec::with_capacity(key_instants.len() + 1);
    for (index, (start, width, escapes)) in placeholder_regions(key_stamp, key_instants).into_iter().enumerate() {
        let bytes = match (key_stamp.is_some(), index) {
            (true, 0) => stamp.to_vec(),
            (stamped, index) => instant_fill(stamp, key_instants[index - usize::from(stamped)].plus_micros).to_vec(),
        };
        fills.push((start, width, escapes, bytes));
    }
    if fills.is_empty() {
        return identity.to_vec();
    }
    fills.sort_by_key(|fill| fill.0);
    let mut out = Vec::with_capacity(identity.len() + STAMP_BYTES * 2);
    let mut at = 0usize;
    for (start, width, escapes, bytes) in fills {
        let Some(end) = start.checked_add(width).filter(|end| start >= at && *end <= identity.len()) else {
            return identity.to_vec();
        };
        out.extend_from_slice(&identity[at..start]);
        for byte in bytes {
            out.push(byte);
            if byte == 0 {
                for _ in 0..escapes {
                    out.push(0xff);
                }
            }
        }
        at = end;
    }
    out.extend_from_slice(&identity[at..]);
    out
}

/// Validate a [`Derive`] request before ordering (P10 confinement and
/// placeholder checks). Any failure is a request error decided from the
/// request alone.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn check(derive: &Derive) -> Result<(), DeriveError> {
    if place_bounds(&derive.manifest_prefix).is_none() {
        return Err(DeriveError::new("A derive manifest prefix carries its partition, family and world"));
    }
    for level in &derive.levels {
        if place_bounds(&level.prefix).is_none() || !same_partition(&level.prefix, &derive.manifest_prefix) {
            return Err(DeriveError::new("A derive lookup level must stay in the manifest's partition"));
        }
    }
    if let DeriveMode::Backfill { writes } = &derive.mode
        && writes.iter().any(|(key, _)| !same_place(key, &derive.manifest_prefix))
    {
        return Err(DeriveError::new("A backfilled entry must stay in the manifest's partition and world"));
    }
    if let DeriveMode::Replace { interval, entries } = &derive.mode {
        // Two bounds of one kind order statically; a fixed bound against an
        // acceptance-relative one resolves at the stamp, and an empty
        // resolution changes nothing.
        match (interval.from, interval.to) {
            (DeriveBound::At(from), DeriveBound::At(to)) | (DeriveBound::Stamp(from), DeriveBound::Stamp(to)) if from >= to => {
                return Err(DeriveError::new("A derive valid interval must be nonempty"));
            },
            _ => {},
        }
        for entry in entries {
            if !same_place(&entry.identity, &derive.manifest_prefix) {
                return Err(DeriveError::new("A derived entry must stay in the manifest's partition and world"));
            }
            let place_len = place_bounds(&entry.identity).map_or(0, |(_, end)| end);
            if let Some(KeyStamp::Raw(offset) | KeyStamp::EscapedSegment(offset) | KeyStamp::DoubleEscapedSegment(offset)) = entry.key_stamp {
                let start = offset as usize;
                let end = start.checked_add(STAMP_BYTES)
                    .ok_or_else(|| DeriveError::new("A derived entry key stamp offset overflows"))?;
                if start < place_len {
                    return Err(DeriveError::new("A derived entry key stamp lies inside the partition and world prefix"));
                }
                if end > entry.identity.len() {
                    return Err(DeriveError::new("A derived entry key stamp lies outside the entry identity"));
                }
                if entry.identity[start..end].iter().any(|&b| b != 0) {
                    return Err(DeriveError::new("A derived entry key stamp placeholder must be 16 zero bytes"));
                }
            }
            for instant in &entry.key_instants {
                let start = instant.offset as usize;
                let end = start.checked_add(INSTANT_BYTES)
                    .ok_or_else(|| DeriveError::new("A derived entry key instant offset overflows"))?;
                if start < place_len {
                    return Err(DeriveError::new("A derived entry key instant lies inside the partition and world prefix"));
                }
                if end > entry.identity.len() {
                    return Err(DeriveError::new("A derived entry key instant lies outside the entry identity"));
                }
                if entry.identity[start..end].iter().any(|&b| b != 0) {
                    return Err(DeriveError::new("A derived entry key instant placeholder must be 8 zero bytes"));
                }
                if instant.escapes > MAX_INSTANT_ESCAPES {
                    return Err(DeriveError::new("A derived entry key instant is escaped too deep"));
                }
            }
            let mut regions = placeholder_regions(entry.key_stamp, &entry.key_instants);
            regions.sort_by_key(|region| region.0);
            if regions.windows(2).any(|pair| pair[0].0 + pair[0].1 > pair[1].0) {
                return Err(DeriveError::new("A derived entry's key placeholders overlap"));
            }
            if !entry.value_instants.is_empty() && !matches!(entry.kind, EntryKind::Edged { .. }) {
                return Err(DeriveError::new("Only an edged entry's values carry an acceptance-relative sum"));
            }
            let mut sums: Vec<usize> = Vec::with_capacity(entry.value_instants.len());
            for instant in &entry.value_instants {
                let start = instant.offset as usize;
                let end = start.checked_add(SUM_BYTES)
                    .ok_or_else(|| DeriveError::new("A derived entry value instant offset overflows"))?;
                if end > entry.enter_value.len() || end > entry.leave_value.len() {
                    return Err(DeriveError::new("A derived entry value instant lies outside its values"));
                }
                sums.push(start);
            }
            sums.sort_unstable();
            if sums.windows(2).any(|pair| pair[0] + SUM_BYTES > pair[1]) {
                return Err(DeriveError::new("A derived entry's value instants overlap"));
            }
            if entry.kind == EntryKind::Claim && (entry.enter_value.len() < 8 || entry.leave_value.len() < 8) {
                return Err(DeriveError::new("A claim's values are its big-endian counts"));
            }
        }
    }
    Ok(())
}

/// Refuse any plain write whose key falls inside a derive's manifest prefix
/// (decided before ordering).
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn check_plain_overlap<'a>(derives: &[Derive], plain_keys: impl IntoIterator<Item = &'a [u8]>) -> Result<(), DeriveError> {
    if derives.is_empty() {
        return Ok(());
    }
    for key in plain_keys {
        for derive in derives {
            if key.starts_with(&derive.manifest_prefix) {
                return Err(DeriveError::new("A plain write cannot target a derive manifest prefix"));
            }
        }
    }
    Ok(())
}

/// Whether two derived entries are the same entry (kind, identity, values).
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn same_entry(a: &Segment, b: &Segment) -> bool {
    a.kind == b.kind && a.identity == b.identity && a.enter_value == b.enter_value && a.leave_value == b.leave_value
}

/// A put of `value` at `key`.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn put(key: Vec<u8>, value: Vec<u8>) -> EmittedWrite {
    EmittedWrite { key, value: Some(value) }
}

/// List an emitted edged record in the purge inventory, unless its entry is
/// retained (`onErase: "retain"`), whose records outlive erasure.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn record(written_keys: &mut Vec<Vec<u8>>, retain: bool, key: &[u8]) {
    if !retain {
        written_keys.push(key.to_vec());
    }
}

/// The `Boundary` entries live on a line: those of segments reaching its
/// horizon, as `(identity, enter, leave)`.
#[cfg_attr(verus_keep_ghost, verifier::external)]
fn live_boundaries(segments: &[Segment], horizon: i64) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    segments
        .iter()
        .filter(|segment| segment.kind == EntryKind::Boundary && segment.to == horizon)
        .map(|segment| (segment.identity.clone(), segment.enter_value.clone(), segment.leave_value.clone()))
        .collect()
}

/// Apply one `DeriveMode::Replace` step to the row's previous line (which
/// may be inherited from a parent world at its fork stamp, and is re-rooted
/// into the target world). Pure, total and deterministic: both
/// `MemoryPersistence` and RTDB's resolver run this exact function.
///
/// A malformed prefix or an empty interval changes nothing. Entries outside
/// the manifest's place are dropped, so every emitted key is confined (P10)
/// whatever the input.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn apply(
    manifest_prefix: &[u8],
    previous: &Manifest,
    interval: ValidInterval,
    entries: &[DeriveEntry],
    stamp: [u8; STAMP_BYTES],
    ordinal: u64,
) -> Applied {
    let unchanged = || Applied {
        writes: Vec::new(),
        manifest: Manifest { segments: previous.segments.clone(), horizon: previous.horizon, written_keys: Vec::new() },
        next_ordinal: ordinal,
    };
    let Some((m_partition, m_world_end)) = place_bounds(manifest_prefix) else { return unchanged() };
    if interval.from >= interval.to {
        return unchanged();
    }
    let target_world = &manifest_prefix[m_partition + 1..m_world_end];
    let mut ordinal = ordinal;

    // 1. The previous line, re-rooted into the target world when inherited
    //    (P10). A parent's live Single key is never the target's to delete.
    let prev: Vec<Segment> = previous
        .segments
        .iter()
        .filter(|segment| same_partition(&segment.identity, manifest_prefix))
        .filter_map(|segment| {
            let own = same_place(&segment.identity, manifest_prefix);
            let identity = if own { segment.identity.clone() } else { reroot(&segment.identity, target_world)? };
            let live_key = if own && same_place(&segment.live_key, manifest_prefix) { segment.live_key.clone() } else { Vec::new() };
            Some(Segment { identity, live_key, ..segment.clone() })
        })
        .collect();
    let horizon = previous.horizon.max(interval.to);

    // 2. The new entries over the interval, stamp placeholders filled in
    //    their keys and their values.
    let new: Vec<Segment> = entries
        .iter()
        .filter(|entry| same_place(&entry.identity, manifest_prefix))
        .map(|entry| Segment {
            from: interval.from,
            to: interval.to,
            kind: entry.kind,
            identity: fill_key_stamp(&entry.identity, entry.key_stamp, &entry.key_instants, &stamp),
            enter_value: fill_values(&entry.enter_value, &entry.value_instants, &stamp, 1),
            leave_value: fill_values(&entry.leave_value, &entry.value_instants, &stamp, -1),
            live_key: Vec::new(),
        })
        .collect();

    // 3. A new entry the previous line already holds identically over the
    //    whole interval is preserved: nothing about it changes. A stamped
    //    Single always moves to the new stamp.
    let covered: Vec<bool> = new
        .iter()
        .map(|entry| {
            if matches!(entry.kind, EntryKind::Single { stamped: true }) {
                return false;
            }
            let spans: Vec<(i64, i64)> = prev.iter().filter(|segment| same_entry(segment, entry)).map(|segment| (segment.from, segment.to)).collect();
            !spans.is_empty() && covers_interval(&spans, interval.from, interval.to)
        })
        .collect();
    let preserved = |segment: &Segment| new.iter().zip(&covered).any(|(entry, covered)| *covered && same_entry(segment, entry));

    let mut writes: Vec<EmittedWrite> = Vec::new();
    let mut written_keys: Vec<Vec<u8>> = Vec::new();
    let mut next: Vec<Segment> = Vec::new();
    let mut left_markers: Vec<Vec<u8>> = Vec::new();

    // 4. Trim every previous segment by the interval, and retire what the
    //    interval takes from those it meets and the new entries do not keep.
    for segment in &prev {
        if !meets(segment.from, segment.to, interval.from, interval.to) {
            next.push(segment.clone());
            continue;
        }
        let kept = preserved(segment);
        // A retired Single's key is deleted, so its remainders hold none.
        let remainder_key = if kept || !matches!(segment.kind, EntryKind::Single { .. }) { segment.live_key.clone() } else { Vec::new() };
        if let Some((from, to)) = trim_left(segment.from, segment.to, interval.from, interval.to) {
            next.push(Segment { from, to, live_key: remainder_key.clone(), ..segment.clone() });
        }
        if let Some((from, to)) = trim_right(segment.from, segment.to, interval.from, interval.to) {
            next.push(Segment { from, to, live_key: remainder_key.clone(), ..segment.clone() });
        }
        if kept {
            continue;
        }
        match segment.kind {
            EntryKind::Marker => {
                let reentered = new.iter().any(|entry| entry.kind == EntryKind::Marker && entry.identity == segment.identity);
                if !reentered && !left_markers.contains(&segment.identity) {
                    left_markers.push(segment.identity.clone());
                    let key = stamped_key(&segment.identity, &stamp, ordinal);
                    ordinal = ordinal.wrapping_add(1);
                    written_keys.push(key.clone());
                    writes.push(put(key, Vec::new()));
                }
            },
            // The live set's difference, below.
            EntryKind::Boundary => {},
            EntryKind::Edged { .. } | EntryKind::Claim => {
                let retain = segment.kind == (EntryKind::Edged { retain: true });
                let overlap_from = segment.from.max(interval.from);
                let overlap_to = segment.to.min(interval.to);
                let key = edged_key(&segment.identity, overlap_from, &stamp, ordinal);
                ordinal = ordinal.wrapping_add(1);
                record(&mut written_keys, retain, &key);
                writes.push(put(key, segment.leave_value.clone()));
                if overlap_to != OPEN_END {
                    let key = edged_key(&segment.identity, overlap_to, &stamp, ordinal);
                    ordinal = ordinal.wrapping_add(1);
                    record(&mut written_keys, retain, &key);
                    writes.push(put(key, segment.enter_value.clone()));
                }
            },
            EntryKind::Single { .. } => {
                if !segment.live_key.is_empty() {
                    writes.push(EmittedWrite { key: segment.live_key.clone(), value: None });
                }
            },
        }
    }

    // 5. Enter every new entry the previous line does not already hold.
    for (mut entry, covered) in new.into_iter().zip(covered) {
        if covered {
            if matches!(entry.kind, EntryKind::Single { .. }) {
                entry.live_key = prev
                    .iter()
                    .find(|segment| same_entry(segment, &entry) && !segment.live_key.is_empty())
                    .map(|segment| segment.live_key.clone())
                    .unwrap_or_default();
            }
            // An inherited Single still needs its own key in this world.
            let enter_single = matches!(entry.kind, EntryKind::Single { .. }) && entry.live_key.is_empty();
            if !enter_single {
                next.push(entry);
                continue;
            }
        }
        match entry.kind {
            EntryKind::Marker => {
                let key = stamped_key(&entry.identity, &stamp, ordinal);
                ordinal = ordinal.wrapping_add(1);
                written_keys.push(key.clone());
                writes.push(put(key, entry.enter_value.clone()));
            },
            EntryKind::Boundary => {},
            EntryKind::Edged { .. } | EntryKind::Claim => {
                let retain = entry.kind == (EntryKind::Edged { retain: true });
                let key = edged_key(&entry.identity, entry.from, &stamp, ordinal);
                ordinal = ordinal.wrapping_add(1);
                record(&mut written_keys, retain, &key);
                writes.push(put(key, entry.enter_value.clone()));
                if entry.to != OPEN_END {
                    let key = edged_key(&entry.identity, entry.to, &stamp, ordinal);
                    ordinal = ordinal.wrapping_add(1);
                    record(&mut written_keys, retain, &key);
                    writes.push(put(key, entry.leave_value.clone()));
                }
            },
            EntryKind::Single { stamped: true } => {
                let key = stamped_key(&entry.identity, &stamp, ordinal);
                ordinal = ordinal.wrapping_add(1);
                entry.live_key = key.clone();
                written_keys.push(key.clone());
                writes.push(put(key, entry.enter_value.clone()));
            },
            EntryKind::Single { stamped: false } => {
                let key = entry.identity.clone();
                entry.live_key = key.clone();
                written_keys.push(key.clone());
                writes.push(put(key, entry.enter_value.clone()));
            },
        }
        next.push(entry);
    }

    // 6. Statistics: the live set changes by exactly its multiset
    //    difference, so an unchanged document writes nothing.
    let before = live_boundaries(&prev, previous.horizon);
    let mut after = live_boundaries(&next, horizon);
    for (identity, _, leave) in before {
        if let Some(index) = after.iter().position(|(other, _, other_leave)| *other == identity && *other_leave == leave) {
            after.remove(index);
            continue;
        }
        let key = stamped_key(&identity, &stamp, ordinal);
        ordinal = ordinal.wrapping_add(1);
        written_keys.push(key.clone());
        writes.push(put(key, leave));
    }
    for (identity, enter, _) in after {
        let key = stamped_key(&identity, &stamp, ordinal);
        ordinal = ordinal.wrapping_add(1);
        written_keys.push(key.clone());
        writes.push(put(key, enter));
    }

    Applied {
        writes,
        manifest: Manifest { segments: next, horizon, written_keys },
        next_ordinal: ordinal,
    }
}

/// Apply one `DeriveMode::Purge` step across all manifest versions stored under
/// `manifest_prefix` (supplied in ascending stamp order, newest last). Total.
///
/// Emits a delete of every key any manifest version recorded as written
/// (markers, singles, statistics deltas, claims and unretained edged records, at
/// every stamp: the row's whole derived history), and of every manifest key
/// itself (so a later write finds no line), each once and confined to the
/// manifest's place. Nothing is retracted at the purge's own stamp: with
/// every delta and record of the row gone, a read at any cut sums exactly
/// the other rows' contributions (Lean `purge_is_recount_without`), and no
/// key anywhere still carries an erased value or contribution. A key a
/// compaction already folded into a base is gone already; its base is
/// re-derived by the engine in the same commit ([`line_through`]).
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn purge(manifest_prefix: &[u8], stored_manifests: &[(Vec<u8>, Manifest)]) -> Vec<EmittedWrite> {
    if place_bounds(manifest_prefix).is_none() {
        return Vec::new();
    }
    let mut writes = Vec::new();
    let mut deleted = std::collections::BTreeSet::<Vec<u8>>::new();
    for (manifest_key, manifest) in stored_manifests {
        for key in &manifest.written_keys {
            if same_place(key, manifest_prefix) && deleted.insert(key.clone()) {
                writes.push(EmittedWrite { key: key.clone(), value: None });
            }
        }
        if same_place(manifest_key, manifest_prefix) && deleted.insert(manifest_key.clone()) {
            writes.push(EmittedWrite { key: manifest_key.clone(), value: None });
        }
    }
    writes
}

/// The row's derived line as of the store stamp `through`: the newest of its
/// stored manifest versions (`(key, manifest)`, keys `manifest_prefix ‖ stamp
/// ‖ ordinal`, in ascending key order) stamped at or below it, if any. A
/// compaction base at `through` folded in exactly what this line held: an
/// edged entry's records sum, per edge, to its segments' coverage jumps (P9),
/// and a statistics delta log to its live boundaries. The engine subtracts
/// that from the base when the row is purged.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn line_through<'a>(manifest_prefix: &[u8], stored_manifests: &'a [(Vec<u8>, Manifest)], through: &[u8; STAMP_BYTES]) -> Option<&'a Manifest> {
    stored_manifests
        .iter()
        .filter(|(key, _)| {
            key.strip_prefix(manifest_prefix)
                .and_then(|rest| rest.get(..STAMP_BYTES))
                .is_some_and(|stamp| stamp <= &through[..])
        })
        .map(|(_, manifest)| manifest)
        .next_back()
}

// ---------------------------------------------------------------- Claims

/// The claims a [`Derive`] makes once the append's stamp fills it: each
/// [`EntryKind::Claim`] entry's filled identity, with the derive's resolved
/// coverage. A purge, an empty coverage or an entry outside the manifest's
/// place claims nothing.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn claims(derive: &Derive, stamp: &[u8; STAMP_BYTES]) -> Vec<(Vec<u8>, ValidInterval)> {
    let DeriveMode::Replace { interval, entries } = &derive.mode else { return Vec::new() };
    let coverage = resolve_interval(*interval, stamp_micros(stamp));
    if coverage.from >= coverage.to {
        return Vec::new();
    }
    entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Claim && same_place(&entry.identity, &derive.manifest_prefix))
        .map(|entry| (fill_key_stamp(&entry.identity, entry.key_stamp, &entry.key_instants, stamp), coverage))
        .collect()
}

/// Where a claim's records lie: its filled `identity` at each level of
/// `derive` (the target world first, then each inherited level, re-rooted
/// into that level's world, P10), with the stamp through which the level
/// counts (`None`: every record, the target's own and this append's). A
/// world's records retract what it overrides of its parents, so the claim's
/// count at an instant is the sum over every level.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn claim_levels(derive: &Derive, identity: &[u8]) -> Vec<(Vec<u8>, Option<[u8; STAMP_BYTES]>)> {
    let mut out: Vec<(Vec<u8>, Option<[u8; STAMP_BYTES]>)> = Vec::with_capacity(derive.levels.len().max(1));
    out.push((identity.to_vec(), None));
    for level in &derive.levels {
        let Some(through) = level.through else { continue };
        let Some((p_len, end)) = place_bounds(&level.prefix) else { continue };
        let Some(rerooted) = reroot(identity, &level.prefix[p_len + 1..end]) else { continue };
        out.push((rerooted, Some(through)));
    }
    out
}

/// The `(edge, count)` of a stored record of a claim at `identity` (one of
/// [`claim_levels`]): a key `identity ‖ ordered(edge) ‖ stamp ‖ ordinal`
/// stamped at or before `through`, whose value starts with its big-endian
/// `i64` count. `None` for any other key.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn claim_record(identity: &[u8], through: Option<&[u8; STAMP_BYTES]>, key: &[u8], value: &[u8]) -> Option<(i64, i64)> {
    let rest = key.strip_prefix(identity).filter(|rest| rest.len() == EDGE_BYTES + PRECEDENCE_BYTES)?;
    if through.is_some_and(|through| &rest[EDGE_BYTES..EDGE_BYTES + STAMP_BYTES] > through.as_slice()) {
        return None;
    }
    let mut edge = [0u8; EDGE_BYTES];
    edge.copy_from_slice(&rest[..EDGE_BYTES]);
    let mut count = [0u8; 8];
    count.copy_from_slice(value.get(..8)?);
    Some(((u64::from_be_bytes(edge) ^ (1 << 63)) as i64, i64::from_be_bytes(count)))
}

/// Whether a claim's records (`(edge, count)`, every level's, after the
/// append applied) count more than one claimant at some instant of
/// `coverage`: the count at instant `t` is the sum of the records with edge
/// `≤ t`, so it is checked at the coverage's start and at each edge inside
/// it.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn claim_exceeded(records: &[(i64, i64)], coverage: ValidInterval) -> bool {
    let mut records = records.to_vec();
    records.sort_unstable();
    let mut count: i128 = 0;
    let mut at = 0usize;
    while at < records.len() && records[at].0 <= coverage.from {
        count += i128::from(records[at].1);
        at += 1;
    }
    if count > 1 {
        return true;
    }
    while at < records.len() && records[at].0 < coverage.to {
        let edge = records[at].0;
        while at < records.len() && records[at].0 == edge {
            count += i128::from(records[at].1);
            at += 1;
        }
        if count > 1 {
            return true;
        }
    }
    false
}

/// Apply one `DeriveMode::Backfill` step to the row's previous line: emit
/// each backfilled entry as given, confined to the manifest's place (P10),
/// and keep the line's segments and horizon with those keys as this step's
/// inventory, so a purge deletes them (erase-derive). Total.
#[cfg_attr(verus_keep_ghost, verifier::external)]
pub fn backfill(manifest_prefix: &[u8], previous: &Manifest, writes: &[(Vec<u8>, Vec<u8>)], ordinal: u64) -> Applied {
    let confined = writes.iter().filter(|(key, _)| place_bounds(manifest_prefix).is_some() && same_place(key, manifest_prefix));
    let emitted: Vec<EmittedWrite> = confined.map(|(key, value)| put(key.clone(), value.clone())).collect();
    Applied {
        manifest: Manifest {
            segments: previous.segments.clone(),
            horizon: previous.horizon,
            written_keys: emitted.iter().map(|write| write.key.clone()).collect(),
        },
        writes: emitted,
        next_ordinal: ordinal,
    }
}

// ---------------------------------------------------------------- Binary codec

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn take<'a>(rest: &mut &'a [u8], len: usize) -> Result<&'a [u8], DeriveError> {
    if rest.len() < len {
        return Err(DeriveError::new("Truncated derive encoding"));
    }
    let (head, tail) = rest.split_at(len);
    *rest = tail;
    Ok(head)
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn take_u8(rest: &mut &[u8]) -> Result<u8, DeriveError> {
    Ok(take(rest, 1)?[0])
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn take_u32(rest: &mut &[u8]) -> Result<u32, DeriveError> {
    let b = take(rest, 4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn take_i64(rest: &mut &[u8]) -> Result<i64, DeriveError> {
    let b = take(rest, 8)?;
    Ok(i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn take_bytes(rest: &mut &[u8]) -> Result<Vec<u8>, DeriveError> {
    let len = take_u32(rest)? as usize;
    Ok(take(rest, len)?.to_vec())
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn encode_kind(kind: EntryKind) -> u8 {
    match kind {
        EntryKind::Marker => 1,
        EntryKind::Boundary => 2,
        EntryKind::Single { stamped: false } => 3,
        EntryKind::Single { stamped: true } => 4,
        EntryKind::Edged { retain: false } => 5,
        EntryKind::Edged { retain: true } => 6,
        EntryKind::Claim => 7,
    }
}

#[cfg_attr(verus_keep_ghost, verifier::external)]
fn decode_kind(tag: u8) -> Result<EntryKind, DeriveError> {
    match tag {
        1 => Ok(EntryKind::Marker),
        2 => Ok(EntryKind::Boundary),
        3 => Ok(EntryKind::Single { stamped: false }),
        4 => Ok(EntryKind::Single { stamped: true }),
        5 => Ok(EntryKind::Edged { retain: false }),
        6 => Ok(EntryKind::Edged { retain: true }),
        7 => Ok(EntryKind::Claim),
        _ => Err(DeriveError::new("Unknown derive entry kind")),
    }
}

impl Manifest {
    /// Canonical byte encoding stored under `manifest_prefix ‖ stamp ‖ ordinal`.
    #[cfg_attr(verus_keep_ghost, verifier::external)]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![1u8]; // format version 1
        out.extend_from_slice(&self.horizon.to_be_bytes());
        out.extend_from_slice(&(self.segments.len() as u32).to_be_bytes());
        for seg in &self.segments {
            out.extend_from_slice(&seg.from.to_be_bytes());
            out.extend_from_slice(&seg.to.to_be_bytes());
            out.push(encode_kind(seg.kind));
            put_bytes(&mut out, &seg.identity);
            put_bytes(&mut out, &seg.enter_value);
            put_bytes(&mut out, &seg.leave_value);
            put_bytes(&mut out, &seg.live_key);
        }
        out.extend_from_slice(&(self.written_keys.len() as u32).to_be_bytes());
        for key in &self.written_keys {
            put_bytes(&mut out, key);
        }
        out
    }

    #[cfg_attr(verus_keep_ghost, verifier::external)]
    pub fn decode(bytes: &[u8]) -> Result<Self, DeriveError> {
        let mut rest = bytes;
        let version = take_u8(&mut rest)?;
        if version != 1 {
            return Err(DeriveError::new("Unsupported derive manifest version"));
        }
        let horizon = take_i64(&mut rest)?;
        let seg_count = take_u32(&mut rest)? as usize;
        let mut segments = Vec::with_capacity(seg_count.min(256));
        for _ in 0..seg_count {
            let from = take_i64(&mut rest)?;
            let to = take_i64(&mut rest)?;
            let kind = decode_kind(take_u8(&mut rest)?)?;
            let identity = take_bytes(&mut rest)?;
            let enter_value = take_bytes(&mut rest)?;
            let leave_value = take_bytes(&mut rest)?;
            let live_key = take_bytes(&mut rest)?;
            segments.push(Segment { from, to, kind, identity, enter_value, leave_value, live_key });
        }
        let key_count = take_u32(&mut rest)? as usize;
        let mut written_keys = Vec::with_capacity(key_count.min(256));
        for _ in 0..key_count {
            written_keys.push(take_bytes(&mut rest)?);
        }
        if !rest.is_empty() {
            return Err(DeriveError::new("Trailing bytes in derive manifest"));
        }
        Ok(Self { segments, horizon, written_keys })
    }
}

impl Derive {
    /// Wire encoding carried in `rtdb.store.v1.AppendRequest.derives`.
    #[cfg_attr(verus_keep_ghost, verifier::external)]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![1u8]; // wire version 1
        put_bytes(&mut out, &self.manifest_prefix);
        out.extend_from_slice(&(self.levels.len() as u32).to_be_bytes());
        for level in &self.levels {
            put_bytes(&mut out, &level.prefix);
            match &level.through {
                None => out.push(0),
                Some(stamp) => {
                    out.push(1);
                    out.extend_from_slice(stamp);
                },
            }
        }
        match &self.mode {
            DeriveMode::Purge => out.push(0),
            DeriveMode::Backfill { writes } => {
                out.push(2);
                out.extend_from_slice(&(writes.len() as u32).to_be_bytes());
                for (key, value) in writes {
                    put_bytes(&mut out, key);
                    put_bytes(&mut out, value);
                }
            },
            DeriveMode::Replace { interval, entries } => {
                out.push(1);
                for bound in [interval.from, interval.to] {
                    let (tag, value) = match bound {
                        DeriveBound::At(instant) => (1u8, instant),
                        DeriveBound::Stamp(plus) => (2u8, plus),
                    };
                    out.push(tag);
                    out.extend_from_slice(&value.to_be_bytes());
                }
                out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
                for entry in entries {
                    out.push(encode_kind(entry.kind));
                    put_bytes(&mut out, &entry.identity);
                    match entry.key_stamp {
                        None => out.push(0),
                        Some(KeyStamp::Raw(offset)) => {
                            out.push(1);
                            out.extend_from_slice(&offset.to_be_bytes());
                        },
                        Some(KeyStamp::EscapedSegment(offset)) => {
                            out.push(2);
                            out.extend_from_slice(&offset.to_be_bytes());
                        },
                        Some(KeyStamp::DoubleEscapedSegment(offset)) => {
                            out.push(3);
                            out.extend_from_slice(&offset.to_be_bytes());
                        },
                    }
                    out.extend_from_slice(&(entry.key_instants.len() as u32).to_be_bytes());
                    for instant in &entry.key_instants {
                        out.extend_from_slice(&instant.offset.to_be_bytes());
                        out.push(instant.escapes);
                        out.extend_from_slice(&instant.plus_micros.to_be_bytes());
                    }
                    out.extend_from_slice(&(entry.value_instants.len() as u32).to_be_bytes());
                    for instant in &entry.value_instants {
                        out.extend_from_slice(&instant.offset.to_be_bytes());
                        out.extend_from_slice(&instant.plus_micros.to_be_bytes());
                    }
                    put_bytes(&mut out, &entry.enter_value);
                    put_bytes(&mut out, &entry.leave_value);
                }
            },
        }
        out
    }

    #[cfg_attr(verus_keep_ghost, verifier::external)]
    pub fn decode(bytes: &[u8]) -> Result<Self, DeriveError> {
        let mut rest = bytes;
        let version = take_u8(&mut rest)?;
        if version != 1 {
            return Err(DeriveError::new("Unsupported derive wire version"));
        }
        let manifest_prefix = take_bytes(&mut rest)?;
        let level_count = take_u32(&mut rest)? as usize;
        let mut levels = Vec::with_capacity(level_count.min(64));
        for _ in 0..level_count {
            let prefix = take_bytes(&mut rest)?;
            let through = match take_u8(&mut rest)? {
                0 => None,
                1 => {
                    let b = take(&mut rest, STAMP_BYTES)?;
                    let mut stamp = [0u8; STAMP_BYTES];
                    stamp.copy_from_slice(b);
                    Some(stamp)
                },
                _ => return Err(DeriveError::new("Invalid derive level bound tag")),
            };
            levels.push(DeriveLevel { prefix, through });
        }
        let mode = match take_u8(&mut rest)? {
            0 => DeriveMode::Purge,
            2 => {
                let count = take_u32(&mut rest)? as usize;
                let mut writes = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    let key = take_bytes(&mut rest)?;
                    let value = take_bytes(&mut rest)?;
                    writes.push((key, value));
                }
                DeriveMode::Backfill { writes }
            },
            1 => {
                let mut bound = || -> Result<DeriveBound, DeriveError> {
                    match take_u8(&mut rest)? {
                        1 => Ok(DeriveBound::At(take_i64(&mut rest)?)),
                        2 => Ok(DeriveBound::Stamp(take_i64(&mut rest)?)),
                        _ => Err(DeriveError::new("Invalid derive interval bound tag")),
                    }
                };
                let from = bound()?;
                let to = bound()?;
                let interval = DeriveInterval { from, to };
                let entry_count = take_u32(&mut rest)? as usize;
                let mut entries = Vec::with_capacity(entry_count.min(256));
                for _ in 0..entry_count {
                    let kind = decode_kind(take_u8(&mut rest)?)?;
                    let identity = take_bytes(&mut rest)?;
                    let key_stamp = match take_u8(&mut rest)? {
                        0 => None,
                        1 => Some(KeyStamp::Raw(take_u32(&mut rest)?)),
                        2 => Some(KeyStamp::EscapedSegment(take_u32(&mut rest)?)),
                        3 => Some(KeyStamp::DoubleEscapedSegment(take_u32(&mut rest)?)),
                        _ => return Err(DeriveError::new("Invalid derive key stamp tag")),
                    };
                    let instant_count = take_u32(&mut rest)? as usize;
                    let mut key_instants = Vec::with_capacity(instant_count.min(16));
                    for _ in 0..instant_count {
                        let offset = take_u32(&mut rest)?;
                        let escapes = take_u8(&mut rest)?;
                        let plus_micros = take_i64(&mut rest)?;
                        key_instants.push(KeyInstant { offset, escapes, plus_micros });
                    }
                    let value_count = take_u32(&mut rest)? as usize;
                    let mut value_instants = Vec::with_capacity(value_count.min(16));
                    for _ in 0..value_count {
                        let offset = take_u32(&mut rest)?;
                        let plus_micros = take_i64(&mut rest)?;
                        value_instants.push(ValueInstant { offset, plus_micros });
                    }
                    let enter_value = take_bytes(&mut rest)?;
                    let leave_value = take_bytes(&mut rest)?;
                    entries.push(DeriveEntry { kind, identity, key_stamp, key_instants, value_instants, enter_value, leave_value });
                }
                DeriveMode::Replace { interval, entries }
            },
            _ => return Err(DeriveError::new("Invalid derive mode tag")),
        };
        if !rest.is_empty() {
            return Err(DeriveError::new("Trailing bytes in derive wire message"));
        }
        Ok(Self { manifest_prefix, levels, mode })
    }
}

#[cfg(all(test, not(verus_keep_ghost)))]
mod tests {
    use super::*;

    const ROW: u8 = 16;
    const INDEX: u8 = 17;
    const PERSISTENCE: u8 = 32;

    /// A stored key `partition ‖ family ‖ segment(world) ‖ rest`, as
    /// `store_read::layout::stored` builds it.
    fn key(part: &[u8], family: u8, world: [u8; 16], rest: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(part.len() as u16).to_be_bytes());
        out.extend_from_slice(part);
        out.push(family);
        for byte in world {
            out.push(byte);
            if byte == 0 {
                out.push(0xff);
            }
        }
        out.extend_from_slice(&[0, 1]);
        out.extend_from_slice(rest);
        out
    }

    fn stamp(micros: u8, version: u8) -> [u8; STAMP_BYTES] {
        [0x80, 0, 0, 0, 0, 0, 0, micros, 0, 0, 0, 0, 0, 0, 0, version]
    }

    fn entry(kind: EntryKind, family: u8, name: &[u8], enter: &[u8], leave: &[u8]) -> DeriveEntry {
        DeriveEntry { kind, identity: key(b"p", family, [0; 16], name), key_stamp: None, key_instants: Vec::new(), value_instants: Vec::new(), enter_value: enter.to_vec(), leave_value: leave.to_vec() }
    }

    fn prefix() -> Vec<u8> {
        key(b"p", ROW, [0; 16], b"row\x04")
    }

    #[test]
    fn entries_of_every_family_share_the_rows_place() {
        assert!(same_place(&key(b"p", INDEX, [0; 16], b"x"), &prefix()));
        assert!(same_place(&key(b"p", PERSISTENCE, [0; 16], b"y"), &prefix()));
        assert!(!same_place(&key(b"p", INDEX, [1; 16], b"x"), &prefix()), "another world");
        assert!(!same_place(&key(b"q", INDEX, [0; 16], b"x"), &prefix()), "another partition");
        let rerooted = reroot(&key(b"p", INDEX, [1; 16], b"x"), &prefix()[4..4 + 34]).unwrap();
        assert_eq!(rerooted, key(b"p", INDEX, [0; 16], b"x"), "re-rooting swaps only the world segment");
    }

    #[test]
    fn identical_reput_emits_nothing() {
        let marker = entry(EntryKind::Marker, INDEX, b"idx_a", &[1], &[]);
        let stats = entry(EntryKind::Boundary, PERSISTENCE, b"stats", &[1], &[0xff]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[marker.clone(), stats.clone()], stamp(10, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(first.writes.len(), 2, "one marker enter, one statistics enter");
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 20, to: OPEN_END }, &[marker, stats], stamp(20, 2), DERIVE_ORDINAL_BASE);
        assert!(second.writes.is_empty(), "unchanged entries emit nothing");
    }

    #[test]
    fn a_move_leaves_and_enters_and_purge_deletes_both() {
        let a = entry(EntryKind::Marker, INDEX, b"idx_a", &[1], &[]);
        let b = entry(EntryKind::Marker, INDEX, b"idx_b", &[1], &[]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[a], stamp(10, 1), DERIVE_ORDINAL_BASE);
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 20, to: OPEN_END }, &[b], stamp(20, 2), DERIVE_ORDINAL_BASE);
        assert_eq!(second.writes.len(), 2);
        assert_eq!(second.writes[0].value, Some(Vec::new()), "leave marker for a");
        assert_eq!(second.writes[1].value, Some(vec![1]), "enter marker for b");
        let m1 = stamped_key(&prefix(), &stamp(10, 1), DERIVE_ORDINAL_BASE);
        let m2 = stamped_key(&prefix(), &stamp(20, 2), DERIVE_ORDINAL_BASE);
        let purged = purge(&prefix(), &[(m1, first.manifest), (m2, second.manifest)]);
        // Three concrete markers (enter a, leave a, enter b) and two manifests.
        assert_eq!(purged.len(), 5);
        assert!(purged.iter().all(|write| write.value.is_none()));
    }

    #[test]
    fn purge_deletes_the_rows_whole_counted_history_but_retained_records() {
        let stats = |n: u8| entry(EntryKind::Boundary, PERSISTENCE, b"stats", &[n], &[0x80 | n]);
        let edged = |v: u8| entry(EntryKind::Edged { retain: false }, PERSISTENCE, b"agg", &[v], &[0x80 | v]);
        let kept = |v: u8| entry(EntryKind::Edged { retain: true }, PERSISTENCE, b"kept", &[v], &[0x80 | v]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[stats(1), edged(1), kept(1)], stamp(10, 1), DERIVE_ORDINAL_BASE);
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 20, to: OPEN_END }, &[stats(2), edged(2), kept(2)], stamp(20, 2), DERIVE_ORDINAL_BASE);
        let emitted: Vec<Vec<u8>> = first.writes.iter().chain(&second.writes).map(|write| write.key.clone()).collect();
        let retained: Vec<Vec<u8>> = emitted.iter().filter(|key| key.windows(4).any(|w| w == b"kept")).cloned().collect();
        assert!(!retained.is_empty(), "the retained entry wrote its records");
        let m1 = stamped_key(&prefix(), &stamp(10, 1), DERIVE_ORDINAL_BASE + 100);
        let m2 = stamped_key(&prefix(), &stamp(20, 2), DERIVE_ORDINAL_BASE + 100);
        let purged: Vec<Vec<u8>> = purge(&prefix(), &[(m1.clone(), first.manifest), (m2.clone(), second.manifest)]).into_iter().map(|write| {
            assert!(write.value.is_none(), "a purge only deletes: nothing is retracted at its stamp");
            write.key
        }).collect();
        for key in &emitted {
            assert_eq!(purged.contains(key), !retained.contains(key), "every unretained key the line emitted, at either stamp, is deleted");
        }
        assert!(purged.contains(&m1) && purged.contains(&m2), "and every manifest version");
    }

    #[test]
    fn a_backfills_entries_join_the_inventory_a_purge_deletes() {
        let marker = entry(EntryKind::Marker, INDEX, b"idx_a", &[1], &[]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[marker], stamp(10, 1), DERIVE_ORDINAL_BASE);
        let historical = key(b"p", INDEX, [0; 16], b"idx_b-at-an-old-precedence");
        let foreign = key(b"q", INDEX, [0; 16], b"elsewhere");
        let filled = backfill(&prefix(), &first.manifest, &[(historical.clone(), vec![1]), (foreign.clone(), vec![1])], DERIVE_ORDINAL_BASE);
        assert_eq!(filled.writes, vec![EmittedWrite { key: historical.clone(), value: Some(vec![1]) }], "only the row's own place is written");
        assert_eq!(filled.manifest.segments, first.manifest.segments, "the line is unchanged");
        let derive = Derive {
            manifest_prefix: prefix(),
            levels: vec![DeriveLevel { prefix: prefix(), through: None }],
            mode: DeriveMode::Backfill { writes: vec![(historical.clone(), vec![1])] },
        };
        check(&derive).unwrap();
        assert_eq!(Derive::decode(&derive.encode()).unwrap(), derive);
        assert!(check(&Derive { mode: DeriveMode::Backfill { writes: vec![(foreign, vec![1])] }, ..derive }).is_err());
        let stored = [
            (stamped_key(&prefix(), &stamp(10, 1), DERIVE_ORDINAL_BASE + 100), first.manifest),
            (stamped_key(&prefix(), &stamp(20, 2), DERIVE_ORDINAL_BASE + 100), filled.manifest),
        ];
        assert!(purge(&prefix(), &stored).iter().any(|write| write.key == historical && write.value.is_none()));
    }

    #[test]
    fn the_line_through_a_cut_is_its_newest_version_at_or_below_it() {
        let a = entry(EntryKind::Marker, INDEX, b"idx_a", &[1], &[]);
        let b = entry(EntryKind::Marker, INDEX, b"idx_b", &[1], &[]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[a], stamp(10, 1), DERIVE_ORDINAL_BASE);
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 20, to: OPEN_END }, &[b], stamp(20, 2), DERIVE_ORDINAL_BASE);
        let stored = [
            (stamped_key(&prefix(), &stamp(10, 1), DERIVE_ORDINAL_BASE), first.manifest.clone()),
            (stamped_key(&prefix(), &stamp(20, 2), DERIVE_ORDINAL_BASE), second.manifest.clone()),
        ];
        assert_eq!(line_through(&prefix(), &stored, &stamp(5, 0)), None);
        assert_eq!(line_through(&prefix(), &stored, &stamp(10, 1)), Some(&first.manifest));
        assert_eq!(line_through(&prefix(), &stored, &stamp(15, 9)), Some(&first.manifest));
        assert_eq!(line_through(&prefix(), &stored, &stamp(30, 3)), Some(&second.manifest));
    }

    #[test]
    fn statistics_count_the_row_once_across_puts_and_a_delete() {
        let doc = |n: u8| entry(EntryKind::Boundary, PERSISTENCE, b"stats", &[n], &[0x80 | n]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: OPEN_END }, &[doc(1)], stamp(10, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(first.writes.iter().map(|w| w.value.clone()).collect::<Vec<_>>(), vec![Some(vec![1])]);
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 20, to: OPEN_END }, &[doc(2)], stamp(20, 2), DERIVE_ORDINAL_BASE);
        assert_eq!(second.writes.iter().map(|w| w.value.clone()).collect::<Vec<_>>(), vec![Some(vec![0x81]), Some(vec![2])], "old document out, new in");
        let deleted = apply(&prefix(), &second.manifest, ValidInterval { from: 30, to: OPEN_END }, &[], stamp(30, 3), DERIVE_ORDINAL_BASE);
        assert_eq!(deleted.writes.iter().map(|w| w.value.clone()).collect::<Vec<_>>(), vec![Some(vec![0x82])], "a delete takes the live document out");
    }

    #[test]
    fn an_edged_assertion_counts_out_the_overlap_and_back_in_after_it() {
        let edged = |v: u8| entry(EntryKind::Edged { retain: false }, PERSISTENCE, b"agg", &[v], &[0x80 | v]);
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: 20 }, &[edged(1)], stamp(1, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(first.writes.len(), 2, "+v at 10, -v at 20");
        let second = apply(&prefix(), &first.manifest, ValidInterval { from: 15, to: 18 }, &[edged(2)], stamp(2, 2), DERIVE_ORDINAL_BASE);
        // -1 at 15, +1 at 18 (the old slice's overlap), then +2 at 15, -2 at 18.
        assert_eq!(second.writes.len(), 4);
        assert_eq!(second.manifest.segments.len(), 3, "[10,15) old, [18,20) old, [15,18) new");
    }

    #[test]
    fn an_empty_interval_or_a_foreign_entry_changes_nothing() {
        let foreign = DeriveEntry { identity: key(b"q", INDEX, [0; 16], b"x"), ..entry(EntryKind::Marker, INDEX, b"x", &[1], &[]) };
        let empty = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 5, to: 5 }, &[entry(EntryKind::Marker, INDEX, b"x", &[1], &[])], stamp(1, 1), DERIVE_ORDINAL_BASE);
        assert!(empty.writes.is_empty());
        let confined = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 5, to: 6 }, &[foreign], stamp(1, 1), DERIVE_ORDINAL_BASE);
        assert!(confined.writes.is_empty(), "an entry outside the manifest's place is never emitted");
    }

    #[test]
    fn wire_and_manifest_round_trip() {
        let prefix = key(b"p", ROW, [1; 16], b"r\x04");
        let derive = Derive {
            manifest_prefix: prefix.clone(),
            levels: vec![DeriveLevel { prefix: prefix.clone(), through: None }],
            mode: DeriveMode::Replace {
                interval: DeriveInterval::from_stamp(OPEN_END),
                entries: vec![DeriveEntry {
                    kind: EntryKind::Edged { retain: true },
                    identity: key(b"p", PERSISTENCE, [1; 16], b"agg"),
                    key_stamp: None,
                    key_instants: vec![KeyInstant { offset: 40, escapes: 2, plus_micros: -7 }],
                    value_instants: Vec::new(),
                    enter_value: vec![0, 1],
                    leave_value: vec![0xff, 0xff],
                }],
            },
        };
        let mut derive = derive;
        if let DeriveMode::Replace { entries, .. } = &mut derive.mode {
            entries[0].identity.resize(48, 0);
        }
        check(&derive).unwrap();
        assert_eq!(Derive::decode(&derive.encode()).unwrap(), derive);
        let applied = apply(&prefix, &Manifest::empty(), ValidInterval { from: 1, to: 9 }, &[], stamp(1, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(Manifest::decode(&applied.manifest.encode()).unwrap(), applied.manifest);
    }

    #[test]
    fn an_instant_placeholder_takes_the_stamps_micros_escaped_per_segment() {
        // Micros 0x01 plus 1: the order bytes `80 00 00 00 00 00 00 02`.
        let stamp = stamp(1, 9);
        let template = [key(b"p", INDEX, [0; 16], b"i"), vec![0x08], vec![0; INSTANT_BYTES], vec![0x00, 0xff, 0x01]].concat();
        let offset = u32::try_from(template.len() - 3 - INSTANT_BYTES).unwrap();
        let instant = KeyInstant { offset, escapes: 2, plus_micros: 1 };
        let filled = fill_key_stamp(&template, None, &[instant], &stamp);
        let start = offset as usize;
        let mut expected = vec![0x80];
        for _ in 0..6 {
            expected.extend_from_slice(&[0x00, 0xff, 0xff]);
        }
        expected.push(0x02);
        assert_eq!(&filled[..start], &template[..start]);
        assert_eq!(&filled[start..start + expected.len()], expected.as_slice());
        assert_eq!(&filled[start + expected.len()..], &[0x00, 0xff, 0x01]);
        let marker = DeriveEntry { identity: template.clone(), key_instants: vec![instant], ..entry(EntryKind::Marker, INDEX, b"i", &[1], &[]) };
        let derive = Derive {
            manifest_prefix: prefix(),
            levels: Vec::new(),
            mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(OPEN_END), entries: vec![marker.clone()] },
        };
        check(&derive).unwrap();
        let applied = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 1, to: OPEN_END }, &[marker.clone()], stamp, DERIVE_ORDINAL_BASE);
        assert_eq!(applied.manifest.segments[0].identity, filled, "the line keeps the filled identity");
        let overlapping = DeriveEntry { key_instants: vec![instant, KeyInstant { offset: offset + 4, ..instant }], ..marker.clone() };
        let refused = Derive { mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(OPEN_END), entries: vec![overlapping] }, ..derive.clone() };
        assert!(check(&refused).is_err());
        let nonzero = DeriveEntry { identity: [&template[..start], &[1u8][..], &template[start + 1..]].concat(), ..marker };
        let refused = Derive { mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(OPEN_END), entries: vec![nonzero] }, ..derive };
        assert!(check(&refused).is_err());
    }

    #[test]
    fn a_relative_interval_resolves_at_the_stamp_and_round_trips() {
        let interval = DeriveInterval { from: DeriveBound::Stamp(5), to: DeriveBound::Stamp(9) };
        assert_eq!(resolve_interval(interval, 100), ValidInterval { from: 105, to: 109 });
        assert_eq!(resolve_interval(DeriveInterval { from: DeriveBound::At(3), to: DeriveBound::Stamp(1) }, 100), ValidInterval { from: 3, to: 101 });
        let derive = Derive { manifest_prefix: prefix(), levels: Vec::new(), mode: DeriveMode::Replace { interval, entries: Vec::new() } };
        check(&derive).unwrap();
        assert_eq!(Derive::decode(&derive.encode()).unwrap(), derive);
        let empty = Derive { mode: DeriveMode::Replace { interval: DeriveInterval { from: DeriveBound::Stamp(9), to: DeriveBound::Stamp(9) }, entries: Vec::new() }, ..derive };
        assert!(check(&empty).is_err(), "two relative bounds order statically");
    }

    #[test]
    fn a_value_instant_adds_the_stamp_to_its_sum_and_takes_it_back_on_leave() {
        let sum = |value: i128| [&1i64.to_be_bytes()[..], &1u16.to_be_bytes()[..], &value.to_be_bytes()[..]].concat();
        let mut edged = entry(EntryKind::Edged { retain: false }, PERSISTENCE, b"agg", &sum(2), &sum(-2));
        edged.value_instants = vec![ValueInstant { offset: 10, plus_micros: 3 }];
        let applied = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 7, to: OPEN_END }, &[edged.clone()], stamp(7, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(applied.writes[0].value, Some(sum(2 + 7 + 3)), "the sum counts the stamp plus its offset");
        assert_eq!(applied.manifest.segments[0].leave_value, sum(-(2 + 7 + 3)));
        let bad = DeriveEntry { value_instants: vec![ValueInstant { offset: 20, plus_micros: 0 }], ..edged };
        let derive = Derive { manifest_prefix: prefix(), levels: Vec::new(), mode: DeriveMode::Replace { interval: DeriveInterval::from_stamp(OPEN_END), entries: vec![bad] } };
        assert!(check(&derive).is_err(), "a sum outside the values");
    }

    #[test]
    fn a_claim_counts_its_claimants_at_every_instant_of_its_coverage() {
        let count = |n: i64| n.to_be_bytes().to_vec();
        let claim = entry(EntryKind::Claim, PERSISTENCE, b"claim", &count(1), &count(-1));
        let first = apply(&prefix(), &Manifest::empty(), ValidInterval { from: 10, to: 20 }, &[claim.clone()], stamp(1, 1), DERIVE_ORDINAL_BASE);
        assert_eq!(first.writes.len(), 2, "+1 at 10, -1 at 20");
        let records: Vec<(i64, i64)> = first.writes.iter().filter_map(|write| claim_record(&claim.identity, None, &write.key, write.value.as_deref()?)).collect();
        assert_eq!(records, vec![(10, 1), (20, -1)]);
        assert!(!claim_exceeded(&records, ValidInterval { from: 10, to: 20 }));
        let other = [records.clone(), vec![(15, 1)]].concat();
        assert!(claim_exceeded(&other, ValidInterval { from: 15, to: OPEN_END }), "a second claimant inside the first's coverage");
        let after = [records.clone(), vec![(20, 1)]].concat();
        assert!(!claim_exceeded(&after, ValidInterval { from: 20, to: OPEN_END }), "a claimant after the first's coverage ends");
        let through = stamp(0, 9);
        assert_eq!(claim_record(&claim.identity, Some(&through), &first.writes[0].key, &count(1)), None, "a record stamped after its level's cut");
        let derive = Derive {
            manifest_prefix: prefix(),
            levels: vec![
                DeriveLevel { prefix: prefix(), through: None },
                DeriveLevel { prefix: key(b"p", ROW, [1; 16], b"row\x04"), through: Some(through) },
            ],
            mode: DeriveMode::Replace { interval: DeriveInterval::fixed(10, 20), entries: vec![claim.clone()] },
        };
        assert_eq!(claims(&derive, &stamp(1, 1)), vec![(claim.identity.clone(), ValidInterval { from: 10, to: 20 })]);
        let levels = claim_levels(&derive, &claim.identity);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[1], (key(b"p", PERSISTENCE, [1; 16], b"claim"), Some(through)), "the parent's records under its own world");
        let purged = purge(&prefix(), &[(stamped_key(&prefix(), &stamp(1, 1), DERIVE_ORDINAL_BASE), first.manifest)]);
        assert!(first.writes.iter().all(|write| purged.iter().any(|deleted| deleted.key == write.key && deleted.value.is_none())), "a purge deletes the claim's records");
    }
}
