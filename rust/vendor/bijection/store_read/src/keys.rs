//! Stored keys of owned relations (ENGINE.md, "Data model").
//!
//! Every function takes any place convertible to a [`Place`] (the kernel's
//! `kernel_storage::persistence::layout::Place` converts) and any world id
//! convertible to its 16 bytes.
use sha2::{
    Digest,
    Sha256,
};

use crate::{
    event::{
        ordered_i64,
        unordered_i64,
        Precedence,
        PRECEDENCE_BYTES,
    },
    layout::{
        self,
        Family,
        Place,
        ROOT_WORLD,
    },
    rows,
    segment,
    store::KeyRange,
};

/// The suffix of a row's event keys (after the row base).
pub const EVENTS: u8 = 0x02;
/// The suffix of a row's purge floor.
pub const FLOOR: u8 = 0x03;

const RELATION_DOMAIN: &[u8] = b"bijection.owned.relation\0";
const PATH_DOMAIN: &[u8] = b"bijection.owned.path\0";

/// What a restore does with a relation, decided by its name alone
/// ([`restore_class`], D50.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreClass {
    /// Installation state, an application relation or a carried `bj_engine_*`
    /// relation: archived and replayed live.
    Carried,
    /// Authority (D22): archived as history and retracted at readmission, so
    /// nothing acts under it until it is re-issued.
    Inert,
    /// Inert, and a hold live at the archive cut comes back cancelled with its
    /// unit shredded (the private replay holds, store-cut-13).
    Cancelled,
    /// Installation-local placement, custody binding or serving mechanism:
    /// never archived. The destination writes its own.
    Skipped,
}

/// The prefix every internal relation is named under. An application
/// relation never is.
pub const INTERNAL_PREFIX: &str = "bj_";

/// The internal relation prefixes and their restore classes, the one place
/// they are defined, first match wins (so `bj_engine_replay_` precedes
/// `bj_engine_`):
///
/// - `bj_engine_replay_`: the ordered private replay holds
///   (`bj_engine_replay_<hex16>`, store-cut-13);
/// - `bj_engine_`: carried engine relations (assertion families and the
///   other `bj_engine_*` installation state);
/// - `bj_sys_`: the `sys.*` facts (D50: `sys.grants`, `sys.standings`,
///   `sys.root`, `sys.grant_reach`, `sys.organization_registry`,
///   `sys.browser_policy` and the steward condition inputs), one relation
///   per table and incarnation (`bj_sys_<table>_<incarnation key>`), and the
///   Root and component owners (`bj_sys_root_owner_<hex16>`, `bj_sys_component_owner_<hex16>`);
/// - `bj_local_`: the installation's writer row (`bj_local_installation`)
///   and the other placement-local relations.
///
/// Every other name is unclassified unless it is an application relation
/// ([`is_application_relation`]), and the archive refuses it: a new internal
/// relation that forgets its prefix is never restored silently.
/// No hosted control writes a relation under these prefixes.
pub const RESTORE_CLASSES: [(&str, RestoreClass); 4] = [
    ("bj_engine_replay_", RestoreClass::Cancelled),
    ("bj_engine_", RestoreClass::Carried),
    ("bj_sys_", RestoreClass::Inert),
    ("bj_local_", RestoreClass::Skipped),
];

/// The platform relation prefixes: the internal prefixes whose relations
/// carry the tablet mark and come back inert from a restore (D22,
/// `archive::RestorePolicy::inert_relations`).
pub const PLATFORM_PREFIXES: [&str; 2] = ["bj_sys_", "bj_engine_replay_"];

/// Whether `relation` is an application relation: exactly one of
/// `store_points::Partition::relation`'s per-partition forms,
/// `fact_<layout digest: 64 lowercase hex>` or
/// `fact_<table: 32 hex>_<t | y<part: 2 hex>>_<label id: 32 hex>`, with an
/// unmarked tablet.
pub fn is_application_relation(relation: &str) -> bool {
    let hex = |text: &str, length: usize| text.len() == length && text.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    let Some(rest) = relation.strip_prefix("fact_") else { return false };
    let named = match rest.split('_').collect::<Vec<_>>().as_slice() {
        [digest] => hex(digest, 64),
        [table, part, label] => hex(table, 32) && hex(label, 32)
            && (*part == "t" || part.strip_prefix('y').is_some_and(|part| hex(part, 2))),
        _ => false,
    };
    named && !is_platform_tablet(&tablet(relation))
}

/// The restore class of a declared relation: an internal relation by its
/// prefix, an application relation ([`is_application_relation`]) carried,
/// and every other name refused.
pub fn restore_class(relation: &str) -> anyhow::Result<RestoreClass> {
    if let Some((_, class)) = RESTORE_CLASSES.iter().find(|(prefix, _)| relation.starts_with(prefix)) {
        return Ok(*class);
    }
    anyhow::ensure!(
        is_application_relation(relation),
        "rtdb.owned/restore-unclassified: the relation {relation} has no restore class"
    );
    Ok(RestoreClass::Carried)
}

/// The high bit of a tablet's first byte marks a platform relation, so a
/// stored key tells a platform row from an application row by itself
/// ([`layout::row_tablet`] then [`is_platform_tablet`]).
const PLATFORM_MARK: u8 = 0x80;

/// Whether `relation` is one of the engine's platform relations.
pub fn is_platform_relation(relation: &str) -> bool {
    PLATFORM_PREFIXES.iter().any(|prefix| relation.starts_with(prefix))
}

/// Whether a stored tablet is a platform relation's (D50). Application
/// tablets never carry the mark.
pub fn is_platform_tablet(tablet: &[u8; 16]) -> bool {
    tablet[0] & PLATFORM_MARK != 0
}

/// The 16-byte tablet of a relation: the first 16 bytes of its name's
/// digest, with the first byte's high bit set for a platform relation and
/// clear for every other.
pub fn tablet(relation: &str) -> [u8; 16] {
    let digest = Sha256::digest([RELATION_DOMAIN, relation.as_bytes()].concat());
    let mut tablet: [u8; 16] = digest[..16].try_into().expect("A digest has 32 bytes");
    if is_platform_relation(relation) {
        tablet[0] |= PLATFORM_MARK;
    } else {
        tablet[0] &= !PLATFORM_MARK;
    }
    tablet
}

/// The 16-byte tablet of one path of a relation.
pub fn path_tablet(relation: &str, path: &str) -> [u8; 16] {
    let digest = Sha256::digest([PATH_DOMAIN, relation.as_bytes(), b"\0", path.as_bytes()].concat());
    digest[..16].try_into().expect("A digest has 32 bytes")
}

/// A typed row identity: `tag ‖ bytes`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub enum RowId {
    Utf8(String),
    Binary(Vec<u8>),
}

impl RowId {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Utf8(id) => [&[1u8][..], id.as_bytes()].concat(),
            Self::Binary(id) => [&[2u8][..], id].concat(),
        }
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        match bytes.split_first() {
            Some((1, id)) => Ok(Self::Utf8(String::from_utf8(id.to_vec())?)),
            Some((2, id)) => Ok(Self::Binary(id.to_vec())),
            _ => anyhow::bail!("Invalid owned row id"),
        }
    }
}

/// The row base: `Family::Row ‖ seg(tablet) ‖ seg(id)`.
pub fn row(place: impl Into<Place>, relation: &str, id: &RowId) -> Vec<u8> {
    let place = place.into();
    let segments = [segment::segment(&tablet(relation)), segment::segment(&id.encode())].concat();
    layout::stored(place, Family::Row as u8, &segments)
}

/// Every row of a relation at `place`.
pub fn relation_rows(place: impl Into<Place>, relation: &str) -> KeyRange {
    let place = place.into();
    KeyRange::prefix(&layout::stored(place, Family::Row as u8, &segment::segment(&tablet(relation))))
}

/// The id of a stored row key (base, touch, event or floor key) of `relation`.
pub fn row_id(place: impl Into<Place>, relation: &str, stored: &[u8]) -> anyhow::Result<RowId> {
    let place = place.into();
    let base = layout::stored(place, Family::Row as u8, &segment::segment(&tablet(relation)));
    anyhow::ensure!(stored.starts_with(&base), "The key is not a row of the relation");
    let mut probe = vec![Family::Row as u8];
    probe.extend_from_slice(&stored[base.len()..]);
    let (values, _) = segment::decode_prefix(&probe, 1).ok_or_else(|| anyhow::anyhow!("Invalid owned row key"))?;
    RowId::decode(&values[0])
}

pub fn presence(row: &[u8]) -> Vec<u8> {
    rows::presence(row)
}

pub fn field(row: &[u8], name: &str) -> Vec<u8> {
    rows::field(row, &layout::field_name(name))
}

pub fn events(row: &[u8]) -> KeyRange {
    KeyRange::prefix(&[row, &[EVENTS]].concat())
}

pub fn event(row: &[u8], precedence: Precedence) -> Vec<u8> {
    [row, &[EVENTS], &precedence.encode()].concat()
}

/// The precedence of an event key of `row`.
pub fn event_precedence(row: &[u8], stored: &[u8]) -> anyhow::Result<Precedence> {
    let suffix = stored.strip_prefix(row).and_then(|rest| rest.strip_prefix(&[EVENTS]));
    let suffix = suffix.ok_or_else(|| anyhow::anyhow!("Not an event key of the row"))?;
    anyhow::ensure!(suffix.len() == PRECEDENCE_BYTES, "Invalid owned event key");
    Precedence::decode(suffix)
}

pub fn floor(row: &[u8]) -> Vec<u8> {
    [row, &[FLOOR]].concat()
}

fn catalog(place: Place, kind: &str, parts: &[&[u8]]) -> Vec<u8> {
    let mut segments = segment::segment(format!("owned.{kind}").as_bytes());
    for part in parts {
        segments.extend_from_slice(&segment::segment(part));
    }
    layout::stored(place, Family::Persistence as u8, &segments)
}

/// The installation's commit head: `tx ‖ system_micros`.
pub fn head(place: impl Into<Place>) -> Vec<u8> {
    let place = place.into();
    catalog(place, "head", &[])
}

/// A commit's idempotency receipt: `tx ‖ system_micros`.
pub fn receipt(place: impl Into<Place>, key: &[u8]) -> Vec<u8> {
    let place = place.into();
    catalog(place, "receipt", &[key])
}

pub fn relation(place: impl Into<Place>, relation: &str) -> Vec<u8> {
    let place = place.into();
    catalog(place, "relation", &[relation.as_bytes()])
}

pub fn path(place: impl Into<Place>, relation: &str, path: &str) -> Vec<u8> {
    let place = place.into();
    catalog(place, "path", &[relation.as_bytes(), path.as_bytes()])
}

pub fn paths(place: impl Into<Place>, relation: &str) -> KeyRange {
    let place = place.into();
    let mut prefix = segment::segment(b"owned.path");
    prefix.extend_from_slice(&segment::segment(relation.as_bytes()));
    KeyRange::prefix(&layout::stored(place, Family::Persistence as u8, &prefix))
}

/// One commit group of a relation's journal: the `revision`-th commit that
/// wrote an event of the relation (revisions count from 1). Its value is
/// `tx i64 BE ‖ system_micros i64 BE`.
pub fn journal_entry(place: impl Into<Place>, relation: &str, revision: u64) -> Vec<u8> {
    let place = place.into();
    catalog(place, "journal", &[relation.as_bytes(), &revision.to_be_bytes()])
}

/// Every journal entry of a relation, in revision order.
pub fn journal(place: impl Into<Place>, relation: &str) -> KeyRange {
    let place = place.into();
    let mut prefix = segment::segment(b"owned.journal");
    prefix.extend_from_slice(&segment::segment(relation.as_bytes()));
    KeyRange::prefix(&layout::stored(place, Family::Persistence as u8, &prefix))
}

/// The relation's journal head: its last revision, `u64 BE`.
pub fn journal_head(place: impl Into<Place>, relation: &str) -> Vec<u8> {
    let place = place.into();
    catalog(place, "journal-head", &[relation.as_bytes()])
}

/// The entries of one `(values, id)` of a path: a path entry without its
/// precedence segment.
pub fn path_row(place: impl Into<Place>, relation: &str, path: &str, values: &[u8], id: &RowId) -> Vec<u8> {
    let place = place.into();
    let segments = [segment::segment(&path_tablet(relation, path)), segment::segment(values), segment::segment(&id.encode())].concat();
    layout::stored(place, Family::Index as u8, &segments)
}

/// One path entry: `Family::Index ‖ seg(path) ‖ seg(values) ‖ seg(id) ‖ seg(precedence)`.
pub fn path_entry(place: impl Into<Place>, relation: &str, path: &str, values: &[u8], id: &RowId, precedence: Precedence) -> Vec<u8> {
    let place = place.into();
    let segments = [
        segment::segment(&path_tablet(relation, path)),
        segment::segment(values),
        segment::segment(&id.encode()),
        segment::segment(&precedence.encode()),
    ]
    .concat();
    layout::stored(place, Family::Index as u8, &segments)
}

/// The decoded `(values, id)` of a path entry of `relation`'s `path`.
pub fn path_entry_parts(place: impl Into<Place>, relation: &str, path: &str, stored: &[u8]) -> anyhow::Result<(Vec<u8>, RowId)> {
    let place = place.into();
    let base = layout::stored(place, Family::Index as u8, &segment::segment(&path_tablet(relation, path)));
    anyhow::ensure!(stored.starts_with(&base), "The key is not an entry of the path");
    let mut probe = vec![Family::Index as u8];
    probe.extend_from_slice(&stored[base.len()..]);
    let (values, _) = segment::decode_prefix(&probe, 2).ok_or_else(|| anyhow::anyhow!("Invalid owned path entry"))?;
    Ok((values[0].clone(), RowId::decode(&values[1])?))
}

/// The path entries whose values lie within the bounds.
pub fn path_values(
    place: impl Into<Place>,
    relation: &str,
    path: &str,
    low: std::ops::Bound<&[u8]>,
    high: std::ops::Bound<&[u8]>,
) -> KeyRange {
    layout::index_values(place.into(), &path_tablet(relation, path), low, high)
}

/// One touch of a relation's change log (OWNED-MODEL-COMMITS §2.3):
/// `owned.changes ‖ relation ‖ tx ‖ ordinal`, written in the same append as
/// its event. One commit is the contiguous run under its tx.
pub fn change(place: impl Into<Place>, relation: &str, tx: i64, ordinal: u64) -> Vec<u8> {
    let place = place.into();
    catalog(place, "changes", &[relation.as_bytes(), &tx.to_be_bytes(), &ordinal.to_be_bytes()])
}

/// The change log of a relation for transactions in `[from_tx, to_tx)`.
pub fn change_window(place: impl Into<Place>, relation: &str, from_tx: i64, to_tx: i64) -> KeyRange {
    let place = place.into();
    let start = catalog(place, "changes", &[relation.as_bytes(), &from_tx.max(0).to_be_bytes()]);
    let end = catalog(place, "changes", &[relation.as_bytes(), &to_tx.max(0).to_be_bytes()]);
    KeyRange { start, end: Some(end) }
}

/// The `(tx, ordinal)` of a change key of `relation`.
pub fn change_position(place: impl Into<Place>, relation: &str, stored: &[u8]) -> anyhow::Result<(i64, u64)> {
    let place = place.into();
    let base = catalog(place, "changes", &[relation.as_bytes()]);
    anyhow::ensure!(stored.starts_with(&base), "The key is not a change of the relation");
    let mut probe = vec![Family::Persistence as u8];
    probe.extend_from_slice(&stored[base.len()..]);
    let values = segment::decode(&probe, 2).ok_or_else(|| anyhow::anyhow!("Invalid owned change key"))?;
    let tx = i64::from_be_bytes(values[0].as_slice().try_into()?);
    let ordinal = u64::from_be_bytes(values[1].as_slice().try_into()?);
    Ok((tx, ordinal))
}

/// A lifetime relation's expiry floor (store-cut G8, `owned::expiry`).
pub fn lifetime_floor(place: impl Into<Place>, relation: &str) -> Vec<u8> {
    let place = place.into();
    catalog(place, "lifetime-floor", &[relation.as_bytes()])
}

/// A declared capacity of a relation (`declare-capacity`).
pub fn capacity(place: impl Into<Place>, relation: &str, name: &str) -> Vec<u8> {
    let place = place.into();
    catalog(place, "capacity", &[relation.as_bytes(), name.as_bytes()])
}

/// Every declared capacity of a relation.
pub fn capacities(place: impl Into<Place>, relation: &str) -> KeyRange {
    let place = place.into();
    KeyRange::prefix(&catalog(place, "capacity", &[relation.as_bytes()]))
}

/// The world catalog lives at the Root world of the installation's
/// partition (OWNED-MODEL-WORLDS §3.2).
fn root_of(place: Place) -> Place {
    Place { world: ROOT_WORLD, ..place }
}

/// A world's registration record.
pub fn world_record(place: impl Into<Place>, world: impl Into<[u8; 16]>) -> Vec<u8> {
    let place = place.into();
    let world: [u8; 16] = world.into();
    catalog(root_of(place), "world", &[&world])
}

/// One application-clock publication of a world, by system time:
/// `owned.world_clock ‖ world ‖ ordered(system) ‖ ordered(tx)`, value the reading.
pub fn world_clock(place: impl Into<Place>, world: impl Into<[u8; 16]>, system: i64, tx: i64) -> Vec<u8> {
    let place = place.into();
    let world: [u8; 16] = world.into();
    catalog(root_of(place), "world_clock", &[&world, &ordered_i64(system), &ordered_i64(tx)])
}

/// A world's publications at exactly system time `system`.
pub fn world_clock_at(place: impl Into<Place>, world: impl Into<[u8; 16]>, system: i64) -> KeyRange {
    let place = place.into();
    let world: [u8; 16] = world.into();
    KeyRange::prefix(&catalog(root_of(place), "world_clock", &[&world, &ordered_i64(system)]))
}

/// One publication of a world, by reading: `owned.world_reading ‖ world ‖
/// ordered(reading)`, value `system ‖ tx`. Readings strictly increase
/// with tx, so the two indexes agree.
pub fn world_reading(place: impl Into<Place>, world: impl Into<[u8; 16]>, reading: i64) -> Vec<u8> {
    let place = place.into();
    let world: [u8; 16] = world.into();
    catalog(root_of(place), "world_reading", &[&world, &ordered_i64(reading)])
}

/// A world's publications by reading, up to and including `through`
/// (`None`: all of them).
pub fn world_readings(place: impl Into<Place>, world: impl Into<[u8; 16]>, through: Option<i64>) -> KeyRange {
    let place = place.into();
    let world: [u8; 16] = world.into();
    let all = KeyRange::prefix(&catalog(root_of(place), "world_reading", &[&world]));
    match through {
        None => all,
        Some(through) => KeyRange {
            start: all.start,
            end: KeyRange::prefix(&world_reading(place, world, through)).end,
        },
    }
}

/// The reading of a stored `world_reading` key.
pub fn world_reading_of(place: impl Into<Place>, world: impl Into<[u8; 16]>, stored: &[u8]) -> anyhow::Result<i64> {
    let place = place.into();
    let world: [u8; 16] = world.into();
    let base = catalog(root_of(place), "world_reading", &[&world]);
    anyhow::ensure!(stored.starts_with(&base), "The key is not a reading of the world");
    let mut probe = vec![Family::Persistence as u8];
    probe.extend_from_slice(&stored[base.len()..]);
    let values = segment::decode(&probe, 1).ok_or_else(|| anyhow::anyhow!("Invalid world reading key"))?;
    Ok(unordered_i64(values[0].as_slice().try_into()?))
}

/// One Root publication (`publish_owned_root_head`, RS:2652): a Root
/// commit that published an attempt's Root section (a world batch or fork
/// with Root writes, a retained delete or a Proposal landing). The key is
/// `owned.root_publication ‖ ordered(system) ‖ ordered(tx)`, its value
/// `system ‖ tx`; history is in the keys, so a fork selects the latest
/// publication at or before any cut without retained store versions.
pub fn root_publication(place: impl Into<Place>, system: i64, tx: i64) -> Vec<u8> {
    let place = place.into();
    catalog(root_of(place), "root_publication", &[&ordered_i64(system), &ordered_i64(tx)])
}

/// Every Root publication, in `(system, tx)` order.
pub fn root_publications(place: impl Into<Place>) -> KeyRange {
    let place = place.into();
    KeyRange::prefix(&catalog(root_of(place), "root_publication", &[]))
}

/// Every entry of one path.
pub fn path_all(place: impl Into<Place>, relation: &str, path: &str) -> KeyRange {
    let place = place.into();
    KeyRange::prefix(&layout::stored(place, Family::Index as u8, &segment::segment(&path_tablet(relation, path))))
}

/// The entries of one path at exactly `values` (a search posting run or
/// the document run), every id and precedence.
pub fn path_run(place: impl Into<Place>, relation: &str, path: &str, values: &[u8]) -> KeyRange {
    let place = place.into();
    let segments = [segment::segment(&path_tablet(relation, path)), segment::segment(values)].concat();
    KeyRange::prefix(&layout::stored(place, Family::Index as u8, &segments))
}

fn reverse_tx(tx: i64) -> [u8; 8] {
    (u64::MAX - u64::try_from(tx.max(0)).expect("Clamped to zero")).to_be_bytes()
}

/// The corpus counters of a counted search path after commit `tx`
/// (RANKED §4.1, Q2): `owned.search-stats ‖ relation ‖ path ‖ reversed tx`,
/// value `documents u64 ‖ tokens u64`. History is in the keys; the reversed
/// tx makes the first key at or after [`search_stats`]`(t)` the counters as
/// of `t`.
pub fn search_stats(place: impl Into<Place>, relation: &str, path: &str, tx: i64) -> Vec<u8> {
    let place = place.into();
    catalog(place, "search-stats", &[relation.as_bytes(), path.as_bytes(), &reverse_tx(tx)])
}

/// Every counter key of one search path.
pub fn search_stats_all(place: impl Into<Place>, relation: &str, path: &str) -> KeyRange {
    let place = place.into();
    KeyRange::prefix(&catalog(place, "search-stats", &[relation.as_bytes(), path.as_bytes()]))
}

/// The entries of one path whose values start with `prefix` (a spatial
/// path's partition).
pub fn path_run_prefix(place: impl Into<Place>, relation: &str, path: &str, prefix: &[u8]) -> KeyRange {
    let place = place.into();
    let segments = [segment::segment(&path_tablet(relation, path)), segment::escaped(prefix)].concat();
    KeyRange::prefix(&layout::stored(place, Family::Index as u8, &segments))
}

/// Every row key of the relation whose Binary id starts with `prefix`.
pub fn binary_id_prefix(place: impl Into<Place>, relation: &str, prefix: &[u8]) -> KeyRange {
    let place = place.into();
    let id = [&[2u8][..], prefix].concat();
    let segments = [segment::segment(&tablet(relation)), segment::escaped(&id)].concat();
    KeyRange::prefix(&layout::stored(place, Family::Row as u8, &segments))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The restore class is a closed match (D50.2): every internal prefix
    /// has its class, application relations are carried, and any other
    /// `bj_*` relation is refused.
    #[test]
    fn the_restore_class_is_a_closed_match() {
        for (relation, class) in [
            ("bj_sys_grants_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(), RestoreClass::Inert),
            ("bj_sys_root_owner_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(), RestoreClass::Inert),
            ("bj_engine_replay_aaaaaaaaaaaaaaaa".to_owned(), RestoreClass::Cancelled),
            ("bj_engine_af_families".to_owned(), RestoreClass::Carried),
            ("bj_local_installation".to_owned(), RestoreClass::Skipped),
            (format!("fact_{}", "ab".repeat(32)), RestoreClass::Carried),
            (format!("fact_{}_t_{}", "01".repeat(16), "02".repeat(16)), RestoreClass::Carried),
            (format!("fact_{}_y0a_{}", "01".repeat(16), "02".repeat(16)), RestoreClass::Carried),
        ] {
            assert_eq!(restore_class(&relation).unwrap(), class, "{relation}");
        }
        for unclassified in [
            "bj_installation".to_owned(), "bj_fact_root_admission".to_owned(), "bj_".to_owned(), "docs".to_owned(),
            "fact_publications".to_owned(), "fact_attempts".to_owned(),
            format!("fact_{}", "AB".repeat(32)), format!("fact_{}", "ab".repeat(31)),
            format!("fact_{}_z_{}", "01".repeat(16), "02".repeat(16)),
        ] {
            assert!(restore_class(&unclassified).unwrap_err().to_string().contains("rtdb.owned/restore-unclassified"), "{unclassified}");
        }
    }

    /// No application relation's tablet carries the platform mark, whatever
    /// its name, and every platform relation's does.
    #[test]
    fn no_application_tablet_carries_the_platform_mark() {
        for n in 0..20_000u32 {
            for relation in [format!("fact_{n:064x}"), format!("bj_fact_{n}"), format!("r{n}"), format!("bj_sysx{n}")] {
                assert!(!is_platform_tablet(&tablet(&relation)), "{relation}");
            }
            for prefix in PLATFORM_PREFIXES {
                assert!(is_platform_tablet(&tablet(&format!("{prefix}{n:032x}"))));
            }
        }
    }
}
