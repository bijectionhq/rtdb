//! Reading stored ranges at a version, and resolving owned rows over them.
//!
//! [`RangeSource`] is all a read needs from a store: ordered pages of key
//! ranges at one version. `fact_store`'s `Persistence` provides it through
//! `read`; RTDB provides it over its local ordered storage. The resolution
//! here (purge floors, events, world levels) is the one every owned read
//! uses, ranked or not.
use std::future::Future;

use anyhow::{
    ensure,
    Context,
};

use crate::{
    event::{
        self,
        Cut,
        Event,
        Resolved,
    },
    keys::{
        self,
        RowId,
    },
    layout::Place,
    store::{
        Entry,
        KeyRange,
        Version,
    },
};

/// One batched range read: every range at one version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeRead {
    pub ranges: Vec<KeyRange>,
    /// `None` reads the latest committed version.
    pub at: Option<Version>,
    /// Resume strictly after this key, in the range that contains it.
    pub after: Option<Vec<u8>>,
    /// Maximum entries per range; the store may cap it lower.
    pub limit: usize,
}

/// One range's page: its entries in key order, and where to resume when the
/// limit stopped it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangePage {
    pub entries: Vec<Entry>,
    pub resume: Option<Vec<u8>>,
}

/// The pages of one [`RangeRead`], in request order, and their version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangePages {
    pub at: Version,
    pub pages: Vec<RangePage>,
}

/// Ordered pages of key ranges at a version: the one capability a read of
/// stored bytes needs.
pub trait RangeSource: Sync {
    fn read(&self, read: RangeRead) -> impl Future<Output = anyhow::Result<RangePages>> + Send;
}

/// Every entry of `range` at `at` (the latest when `None`), following
/// resumes at the version the first page read, and that version.
pub async fn scan_at<S: RangeSource + ?Sized>(source: &S, range: KeyRange, at: Option<Version>) -> anyhow::Result<(Version, Vec<Entry>)> {
    let mut out = Vec::new();
    let mut after = None;
    let mut version = at;
    loop {
        let page = source.read(RangeRead { ranges: vec![range.clone()], at: version, after: after.take(), limit: usize::MAX }).await?;
        version = Some(page.at);
        let page = page.pages.into_iter().next().context("A read returns one page per range")?;
        out.extend(page.entries);
        match page.resume {
            Some(resume) => after = Some(resume),
            None => return Ok((page_version(version), out)),
        }
    }
}

fn page_version(version: Option<Version>) -> Version {
    version.expect("Set by the first page")
}

/// Every entry of `range` at `at`.
pub async fn scan<S: RangeSource + ?Sized>(source: &S, range: KeyRange, at: Version) -> anyhow::Result<Vec<Entry>> {
    Ok(scan_at(source, range, Some(at)).await?.1)
}

/// The value of one key at `at` (the latest when `None`), and the version read.
pub async fn point_value<S: RangeSource + ?Sized>(source: &S, key: Vec<u8>, at: Option<Version>) -> anyhow::Result<(Version, Option<Vec<u8>>)> {
    let page = source.read(RangeRead { ranges: vec![KeyRange::point(&key)], at, after: None, limit: usize::MAX }).await?;
    let value = page.pages.into_iter().next().and_then(|page| page.entries.into_iter().next()).and_then(|entry| entry.value);
    Ok((page.at, value))
}

/// All events of one row at `at`, in key (precedence) order.
pub async fn row_events<S: RangeSource + ?Sized>(source: &S, row: &[u8], at: Version) -> anyhow::Result<Vec<Event>> {
    scan(source, keys::events(row), at)
        .await?
        .into_iter()
        .map(|entry| Event::decode(keys::event_precedence(row, &entry.key)?, entry.value.as_deref().unwrap_or_default()))
        .collect()
}

/// The purge floor of a row, read at the latest version: an erase reaches
/// every older hold and branch (RTDB `point_purge_guard`, ND:2591).
pub async fn purge_floor<S: RangeSource + ?Sized>(source: &S, row: &[u8]) -> anyhow::Result<Option<i64>> {
    let (_, floor) = point_value(source, keys::floor(row), None).await?;
    floor.map(|floor| Ok(i64::from_be_bytes(floor.as_slice().try_into().context("Invalid owned purge floor")?))).transpose()
}

/// Refuse a cut below the row's purge floor (`rtdb.owned/read-purged`).
pub async fn require_unpurged<S: RangeSource + ?Sized>(source: &S, row: &[u8], cut: &Cut) -> anyhow::Result<()> {
    if let Some(floor) = purge_floor(source, row).await? {
        ensure!(cut.system >= floor, "rtdb.owned/read-purged: the row was erased after this cut");
    }
    Ok(())
}

/// The events of one row as a cut at `version` sees them. Every read of
/// row content at a hold goes through here, so the purge floor holds on
/// every path: a cut below the row's floor is refused, whatever version,
/// world, pin or derived hold it came from (owner decision 1, ENGINE.md).
pub async fn held_events<S: RangeSource + ?Sized>(source: &S, row: &[u8], cut: &Cut, version: Version) -> anyhow::Result<Vec<Event>> {
    require_unpurged(source, row, cut).await?;
    row_events(source, row, version).await
}

/// One level of a world read: the world and the cut its events are read at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Level {
    pub world: [u8; 16],
    pub cut: Cut,
}

/// A level's events of one row with the row base, or `Err(row)` for a row
/// erased after the level's cut (its floor is read at the latest version,
/// so an erase reaches every older hold and branch).
pub async fn level_events<S: RangeSource + ?Sized>(
    source: &S,
    place: Place,
    level: &Level,
    relation: &str,
    id: &RowId,
    version: Version,
) -> anyhow::Result<Result<(Vec<u8>, Vec<Event>), Vec<u8>>> {
    let row = keys::row(Place { world: level.world, ..place }, relation, id);
    if let Some(floor) = purge_floor(source, &row).await?
        && level.cut.system < floor
    {
        return Ok(Err(row));
    }
    let events = row_events(source, &row, version).await?;
    Ok(Ok((row, events)))
}

/// The level that claims `valid`, and its winner: `(index, event)` with
/// `None` for a tombstone; `(levels.len(), None)` when no level claims it.
/// A row erased after a consulted level's cut refuses, or reads absent
/// when `erased_absent` (pages skip it, owner decision 1); either way its
/// floor joins the read set.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_levels<S: RangeSource + ?Sized>(
    source: &S,
    place: Place,
    levels: &[Level],
    relation: &str,
    id: &RowId,
    valid: i64,
    version: Version,
    erased_absent: bool,
    reads: &mut Vec<KeyRange>,
) -> anyhow::Result<(usize, Option<Event>)> {
    for (index, level) in levels.iter().enumerate() {
        let (row, events) = match level_events(source, place, level, relation, id, version).await? {
            Ok(found) => found,
            Err(row) if erased_absent => {
                reads.push(KeyRange::point(&keys::floor(&row)));
                return Ok((levels.len(), None));
            },
            Err(_) => anyhow::bail!("rtdb.owned/read-purged: the row was erased after this cut"),
        };
        match event::point(&events, &level.cut, valid) {
            Resolved::Absent => continue,
            Resolved::Present(event) => {
                reads.push(KeyRange::point(&keys::floor(&row)));
                return Ok((index, Some(event.clone())));
            },
            Resolved::Deleted(_) => {
                reads.push(KeyRange::point(&keys::floor(&row)));
                return Ok((index, None));
            },
            Resolved::Redacted(_) => anyhow::bail!("rtdb.temporal/erased-assertion: the point is an erased assertion"),
        }
    }
    Ok((levels.len(), None))
}
