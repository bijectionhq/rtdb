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

/// The 16-byte tablet of a relation.
pub fn tablet(relation: &str) -> [u8; 16] {
    let digest = Sha256::digest([RELATION_DOMAIN, relation.as_bytes()].concat());
    digest[..16].try_into().expect("A digest has 32 bytes")
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
