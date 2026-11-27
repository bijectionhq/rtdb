//! Family 7: spatial paths over owned relations (store-cut RANKED §1.3,
//! §4.1, §4.2; RTDB `Spatial*`).
//!
//! The semantics are RTDB's `rust/crates/spatial/src/kernel.rs` over
//! Bijection's own `geometry` crate and [`crate::spatial_index`]: a stored
//! location is tagged WKB or a canonical Bijection geometry or position
//! image; a point keys its leaf cell under its accuracy class, a shape a
//! bounded cover, an empty geometry its sentinel and a null or absent one
//! `i64::MIN`.
//!
//! A spatial path's first column is the geometry and the rest are
//! partition columns (Binary). Each put image writes one entry per key at
//! `values = sha256(partition cells) ‖ ordered key`. A read reads the
//! candidate ranges of its region (or the whole key space for an uncovered
//! predicate) in batched store reads, resolves every candidate at the cut
//! and evaluates the predicate exactly, so the index only nominates. An
//! unbounded nearest read over a bound partition walks the cells best
//! first instead (`nearest_first`): it stops once the page is decided, so
//! rows that cannot reach the page are neither read nor resolved, nor can
//! they trip a work bound (coordinator Q2, store-cut-perf). There is no
//! server cursor: identity pages continue after `(id, partition)`, nearest
//! pages after `(distance, id, partition)`. The read set is every range the
//! region names. A read over several partitions (label partitions or
//! relations) ranks each and orders their union.
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{
        BTreeSet,
        BinaryHeap,
    },
};

use anyhow::{
    ensure,
    Context,
};
use geometry::{
    predicates::{
        self,
        prepared::{
            Preparation,
            PreparedGeometry,
        },
        CoverPredicate,
        TopologyPredicate,
    },
    AdmissionArray,
    AdmissionValue,
    AdmissionView,
    Coords,
    Geometry,
    GeometryDeclaration,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Kind,
    Size,
};
use sha2::{
    Digest,
    Sha256,
};

use super::frame::Frame;
use crate::{
    catalog::{
        self,
        PathKind,
    },
    event::{
        ordered_i64,
        unordered_i64,
        Image,
    },
    keys::{
        self,
        RowId,
    },
    layout::Place,
    source::{
        RangeRead,
        RangeSource,
    },
    spatial_index::{
        candidate_ranges,
        point_key,
        shape_keys,
        SearchCell,
    },
    spatial_key::{
        KeyRange as CellRange,
        CELL_LEVELS,
    },
    store::{
        Entry,
        KeyRange,
        Version,
    },
};

pub const WORK_STEPS: usize = 10_000_000;
pub const WORK_BYTES: usize = 8 << 20;
/// More refined candidates than this refuse the read (`rtdb.spatial/work-limit`, SR:24).
pub const MAX_SPATIAL_CANDIDATES: usize = 65_536;
pub const MAX_SPATIAL_PAGE: usize = 128;
pub const MAX_SPATIAL_PARTITION_COLUMNS: usize = 15;
/// The most partitions one spatial read names.
pub const MAX_SPATIAL_PARTITIONS: usize = 32;
/// The most ranges one store read names: a region's cover goes to the
/// store in batches of these, not one probe per range.
pub const MAX_READ_RANGES: usize = 256;
/// The entries a nearest probe reads of one frontier cell; a cell holding
/// more is split into its children.
const NEAREST_PROBE: usize = 64;
/// The frontier cells one nearest round probes in one read, at most.
const NEAREST_ROUND: usize = MAX_READ_RANGES;
/// What a cell's distance bound may exceed an exact distance by
/// (`geometry::cells` holds it to 1e-6 m).
const NEAREST_BOUND_SLACK_M: f64 = 1e-3;

const BINARY_CELL: u8 = 0x0e;
const NULL_CELL: u8 = 0x00;

fn invalid(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("rtdb.spatial/invalid: {message}")
}

/// RTDB `geometry_error`: a work bound is Unavailable, anything else invalid.
fn geometry_error(error: GeometryRefusal) -> anyhow::Error {
    if error.bound.as_ref().is_some_and(|bound| matches!(bound.kind, geometry::TierBound::WorkSteps | geometry::TierBound::AllocatedBytes)) {
        return anyhow::anyhow!("rtdb.spatial/work-limit: {error}");
    }
    invalid(error)
}

/// How a spatial path's geometry column stores its location.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpatialEncoding {
    /// `tagged-wkb-v1`: `0` shape, `1` position of unknown accuracy, `2 ‖
    /// f64 LE accuracy` position, then ISO WKB.
    TaggedWkb,
    /// `bijection-geometry-v1`: the canonical image of a geometry value.
    BijectionGeometry,
    /// `bijection-position-v1`: the canonical image of a position value.
    BijectionPosition,
}

impl SpatialEncoding {
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        Ok(match name {
            "tagged-wkb-v1" => Self::TaggedWkb,
            "bijection-geometry-v1" => Self::BijectionGeometry,
            "bijection-position-v1" => Self::BijectionPosition,
            _ => anyhow::bail!("rtdb.owned/spatial-encoding: unknown spatial geometry encoding {name}"),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::TaggedWkb => "tagged-wkb-v1",
            Self::BijectionGeometry => "bijection-geometry-v1",
            Self::BijectionPosition => "bijection-position-v1",
        }
    }
}

fn declaration() -> GeometryDeclaration {
    GeometryDeclaration { space: GeometrySpace::wgs84_xy(), size: Size::Large }
}

/// A decoded location: a geometry, and for an observed position its
/// accuracy (`Some(None)` unknown).
struct Location {
    geometry: Geometry,
    position_accuracy: Option<Option<f64>>,
}

impl Location {
    /// A tagged WKB location (`kernel.rs` `Location::decode`).
    fn decode(bytes: &[u8], work: &mut GeometryWork) -> anyhow::Result<Self> {
        let (&tag, payload) = bytes.split_first().ok_or_else(|| invalid("Missing location encoding"))?;
        let (position_accuracy, wkb) = match tag {
            0 => (None, payload),
            1 => (Some(None), payload),
            2 => {
                let (accuracy, wkb) = payload.split_first_chunk::<8>().ok_or_else(|| invalid("Missing position accuracy"))?;
                let accuracy = f64::from_le_bytes(*accuracy);
                ensure!(accuracy.is_finite() && accuracy >= 0.0, invalid("Position accuracy must be finite and nonnegative"));
                (Some(Some(accuracy)), wkb)
            },
            _ => return Err(invalid("Unknown location encoding")),
        };
        let declaration = declaration();
        let geometry = geometry::wkb::decode_with_work(wkb, &declaration.space, &declaration.limits(), work).map_err(geometry_error)?;
        ensure!(
            position_accuracy.is_none() || matches!(&geometry.coords, Coords::Point(_)),
            invalid("An observed position requires a nonempty Point")
        );
        Ok(Self { geometry, position_accuracy })
    }

    /// A stored location in the path's encoding; `None` for a canonical null.
    fn decode_stored(bytes: &[u8], encoding: SpatialEncoding, work: &mut GeometryWork) -> anyhow::Result<Option<Self>> {
        if encoding == SpatialEncoding::TaggedWkb {
            return Self::decode(bytes, work).map(Some);
        }
        let value = Canonical::decode(bytes, work)?;
        if matches!(value.view(), AdmissionView::Null) {
            return Ok(None);
        }
        let declaration = declaration();
        if encoding == SpatialEncoding::BijectionPosition {
            let position = geometry::admit_position_input(value, &declaration, work).map_err(geometry_error)?;
            return Ok(Some(Self {
                geometry: Geometry::new(Kind::Point, Coords::Point(position.point), declaration.space),
                position_accuracy: Some(position.accuracy_m),
            }));
        }
        let geometry = geometry::admit_value_input(value, &declaration, work).map_err(geometry_error)?;
        Ok(Some(Self { geometry, position_accuracy: None }))
    }

    fn is_point_geometry(&self) -> bool {
        self.position_accuracy.is_none() && self.geometry.heights.is_none() && matches!(&self.geometry.coords, Coords::Point(_))
    }

    fn keys(&self, work: &mut GeometryWork) -> anyhow::Result<Vec<i64>> {
        match &self.geometry.coords {
            Coords::Point(coords) => point_key(coords, self.position_accuracy.unwrap_or(Some(0.0)))
                .map(|key| vec![key])
                .map_err(|error| invalid(error.to_string())),
            _ => shape_keys(&self.geometry, work).map_err(geometry_error),
        }
    }
}

/// A canonical Bijection value image (sort-key bytes) viewed for geometry
/// admission without materialising it (RTDB `bijection_geometry.rs`):
/// `2` null, `3 ‖ u8` bool, `4 ‖ 8` int64 (not a coordinate), `5 ‖ 8`
/// ordered float, `9` text, `12` array and `13` object, both `00 00`
/// terminated, texts `00 01` terminated with `00 ff` an escaped NUL.
#[derive(Clone, Copy)]
struct Canonical<'a>(&'a [u8]);

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> anyhow::Result<&'a [u8]> {
    let (value, tail) = (*bytes).split_at_checked(length).ok_or_else(|| invalid("Bijection geometry is truncated"))?;
    *bytes = tail;
    Ok(value)
}

fn text<'a>(bytes: &mut &'a [u8]) -> anyhow::Result<Cow<'a, str>> {
    let mut escaped = None::<Vec<u8>>;
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).ok_or_else(|| invalid("Bijection geometry is truncated"))?;
        let chunk = take(bytes, end)?;
        match take(bytes, 2)? {
            [0, 1] => {
                return match escaped {
                    Some(mut decoded) => {
                        decoded.extend_from_slice(chunk);
                        String::from_utf8(decoded).map(Cow::Owned).map_err(|_| invalid("Bijection geometry text must be UTF-8"))
                    },
                    None => std::str::from_utf8(chunk).map(Cow::Borrowed).map_err(|_| invalid("Bijection geometry text must be UTF-8")),
                };
            },
            [0, 0xff] => {
                let decoded = escaped.get_or_insert_with(Vec::new);
                decoded.extend_from_slice(chunk);
                decoded.push(0);
            },
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
}

fn validate_text<'a>(bytes: &mut &'a [u8], work: &mut GeometryWork) -> anyhow::Result<&'a [u8]> {
    let start = *bytes;
    let mut valid_utf8 = true;
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).ok_or_else(|| invalid("Bijection geometry is truncated"))?;
        work.charge(end + 2).map_err(geometry_error)?;
        work.allocate(end + 1).map_err(geometry_error)?;
        let chunk = take(bytes, end)?;
        valid_utf8 &= std::str::from_utf8(chunk).is_ok();
        match take(bytes, 2)? {
            [0, 1] => break,
            [0, 0xff] => (),
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
    ensure!(valid_utf8, invalid("Bijection geometry text must be UTF-8"));
    Ok(&start[..start.len() - bytes.len() - 2])
}

fn float(bytes: &[u8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes.try_into().expect("eight bytes"));
    f64::from_bits(if encoded < (1 << 63) { !encoded } else { encoded ^ (1 << 63) })
}

fn validate(bytes: &mut &[u8], depth: usize, work: &mut GeometryWork) -> anyhow::Result<()> {
    ensure!(depth <= 64, invalid("Bijection geometry is too deeply nested"));
    work.charge(1).map_err(geometry_error)?;
    // The admission allowance of a stored image, whatever its decoder.
    work.allocate(std::mem::size_of::<geometry::Gv>()).map_err(geometry_error)?;
    match take(bytes, 1)?[0] {
        2 => (),
        3 => ensure!(take(bytes, 1)?[0] <= 1, invalid("Invalid Bijection geometry boolean")),
        4 => {
            take(bytes, 8)?;
        },
        5 => {
            let value = float(take(bytes, 8)?);
            ensure!(!value.is_nan() || value.to_bits() == 0x7ff8_0000_0000_0000, invalid("Invalid Bijection geometry float"));
        },
        9 => {
            validate_text(bytes, work)?;
        },
        12 => {
            while !bytes.starts_with(&[0, 0]) {
                validate(bytes, depth + 1, work)?;
            }
            take(bytes, 2)?;
        },
        13 => {
            let mut previous = None;
            while !bytes.starts_with(&[0, 0]) {
                let key = validate_text(bytes, work)?;
                ensure!(previous.is_none_or(|previous| previous < key), invalid("Bijection geometry fields must be strictly ordered"));
                work.allocate(std::mem::size_of::<String>()).map_err(geometry_error)?;
                previous = Some(key);
                validate(bytes, depth + 1, work)?;
            }
            take(bytes, 2)?;
        },
        _ => return Err(invalid("Unsupported canonical Bijection geometry value")),
    }
    Ok(())
}

fn skip_text(bytes: &mut &[u8]) {
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).expect("validated text");
        let last = bytes[end + 1] == 1;
        *bytes = &bytes[end + 2..];
        if last {
            return;
        }
    }
}

fn skip(bytes: &mut &[u8]) {
    let (&tag, tail) = bytes.split_first().expect("validated value");
    *bytes = tail;
    match tag {
        2 => (),
        3 => *bytes = &bytes[1..],
        4 | 5 => *bytes = &bytes[8..],
        9 => skip_text(bytes),
        12 | 13 => {
            while !bytes.starts_with(&[0, 0]) {
                if tag == 13 {
                    skip_text(bytes);
                }
                skip(bytes);
            }
            *bytes = &bytes[2..];
        },
        _ => unreachable!("validated value tag"),
    }
}

impl<'a> Canonical<'a> {
    fn decode(mut bytes: &'a [u8], work: &mut GeometryWork) -> anyhow::Result<Self> {
        let value = Self(bytes);
        validate(&mut bytes, 0, work)?;
        ensure!(bytes.is_empty(), invalid("Bijection geometry has trailing bytes"));
        Ok(value)
    }
}

/// The items of a validated canonical array.
#[derive(Clone, Copy)]
struct Items<'a> {
    rest: &'a [u8],
    len: usize,
}

impl<'a> IntoIterator for Items<'a> {
    type IntoIter = std::iter::Take<std::iter::FromFn<Box<dyn FnMut() -> Option<Canonical<'a>> + 'a>>>;
    type Item = Canonical<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut rest = self.rest;
        let next: Box<dyn FnMut() -> Option<Canonical<'a>> + 'a> = Box::new(move || {
            let value = Canonical(rest);
            skip(&mut rest);
            Some(value)
        });
        std::iter::from_fn(next).take(self.len)
    }
}

impl<'a> AdmissionArray<Canonical<'a>> for Items<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> AdmissionValue<'a> for Canonical<'a> {
    fn view(self) -> AdmissionView<'a, impl AdmissionArray<Self>, impl Iterator<Item = (Cow<'a, str>, Self)>> {
        match self.0[0] {
            2 => AdmissionView::Null,
            3 => AdmissionView::Bool(self.0[1] == 1),
            5 => AdmissionView::Float(float(&self.0[1..9])),
            9 => {
                let mut rest = &self.0[1..];
                AdmissionView::String(text(&mut rest).expect("validated text"))
            },
            12 => {
                let rest = &self.0[1..];
                let mut end = rest;
                let mut len = 0;
                while !end.starts_with(&[0, 0]) {
                    skip(&mut end);
                    len += 1;
                }
                AdmissionView::Array(Items { rest, len })
            },
            13 => {
                let mut rest = &self.0[1..];
                AdmissionView::Object(std::iter::from_fn(move || {
                    if rest.starts_with(&[0, 0]) {
                        return None;
                    }
                    let key = text(&mut rest).expect("validated key");
                    let value = Canonical(rest);
                    skip(&mut rest);
                    Some((key, value))
                }))
            },
            // An int64 is no coordinate in a canonical geometry (RTDB `Scalar::Other`).
            _ => AdmissionView::Other,
        }
    }
}

/// A spatial predicate (`kernel.rs` `Predicate`).
#[derive(Clone, Copy, Debug)]
pub enum Predicate {
    All,
    Topology(TopologyPredicate),
    WithinDistance(f64),
    Classification(Classification),
    Nearest { radius_m: Option<f64>, reported_center: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Inside,
    Outside,
    Uncertain,
    Unknown,
}

impl Predicate {
    pub fn parse(name: &str, radius_m: Option<f64>, reported_center: bool) -> anyhow::Result<Self> {
        ensure!(radius_m.is_none_or(|radius| radius.is_finite() && radius >= 0.0), invalid("Radius must be finite and nonnegative"));
        ensure!(name == "nearest" || !reported_center, invalid("Reported-center ordering requires nearest"));
        ensure!(matches!(name, "nearest" | "within_distance") || radius_m.is_none(), invalid("This predicate does not take a radius"));
        Ok(Self::Topology(match name {
            "all" => return Ok(Self::All),
            "intersects" => TopologyPredicate::Intersects,
            "disjoint" => TopologyPredicate::Disjoint,
            "equals" => TopologyPredicate::Equals,
            "covers" => TopologyPredicate::Covers,
            "covered_by" => TopologyPredicate::CoveredBy,
            "contains" => TopologyPredicate::Contains,
            "within" => TopologyPredicate::Within,
            "touches" => TopologyPredicate::Touches,
            "crosses" => TopologyPredicate::Crosses,
            "overlaps" => TopologyPredicate::Overlaps,
            "within_distance" => return Ok(Self::WithinDistance(radius_m.ok_or_else(|| invalid("Distance requires a radius"))?)),
            "inside" => return Ok(Self::Classification(Classification::Inside)),
            "outside" => return Ok(Self::Classification(Classification::Outside)),
            "uncertain" => return Ok(Self::Classification(Classification::Uncertain)),
            "unknown" => return Ok(Self::Classification(Classification::Unknown)),
            "nearest" => return Ok(Self::Nearest { radius_m, reported_center }),
            _ => return Err(invalid("Unknown spatial predicate")),
        }))
    }

    fn cover_predicate(self) -> Option<CoverPredicate> {
        Some(match self {
            Self::All
            | Self::Topology(TopologyPredicate::Disjoint)
            | Self::Classification(Classification::Outside | Classification::Unknown)
            | Self::Nearest { radius_m: None, .. } => return None,
            Self::Topology(_) => CoverPredicate::Touches,
            Self::WithinDistance(distance_m) | Self::Nearest { radius_m: Some(distance_m), .. } => CoverPredicate::Within { distance_m },
            Self::Classification(Classification::Inside) => CoverPredicate::ClassifyInside,
            Self::Classification(Classification::Uncertain) => CoverPredicate::Classify,
        })
    }

    fn prepare(self, region: &Geometry, work: &mut GeometryWork) -> anyhow::Result<PreparedGeometry> {
        let mut prepared = PreparedGeometry::new(region.clone(), Preparation::Topology, work).map_err(geometry_error)?;
        if matches!(self, Self::Topology(_) | Self::WithinDistance(_) | Self::Nearest { .. }) {
            prepared.prepare_reference_points();
        }
        if matches!(self, Self::WithinDistance(_) | Self::Nearest { .. } | Self::Classification(_)) {
            prepared.prepare_reference_segments();
        }
        Ok(prepared)
    }

    /// Outer `None`: the location is not eligible; inner: the exact nearest
    /// distance (`kernel.rs` `evaluate`).
    fn evaluate(self, value: &Location, region: &Geometry, prepared: Option<&PreparedGeometry>, work: &mut GeometryWork) -> anyhow::Result<Option<Option<f64>>> {
        let distance = |work: &mut GeometryWork| match prepared {
            Some(prepared) => prepared.exact_distance_from(&value.geometry, work),
            None => predicates::distance(&value.geometry, region, work),
        };
        let exact = match self {
            Self::All => return Ok(Some(None)),
            Self::Topology(predicate) => match prepared {
                Some(prepared) => prepared.topology_from(&value.geometry, predicate, work),
                None => predicates::topology(&value.geometry, region, predicate, work),
            },
            Self::WithinDistance(radius_m) => distance(work).map(|distance| distance.is_finite() && distance <= radius_m),
            Self::Classification(expected) => match (&value.geometry.coords, value.position_accuracy) {
                (Coords::Point(point), Some(accuracy)) => match prepared {
                    Some(prepared) => prepared.classify_exact(*point, accuracy, work),
                    None => predicates::classify(*point, accuracy, region, work),
                }
                .map(|actual| {
                    actual
                        == match expected {
                            Classification::Inside => "inside",
                            Classification::Outside => "outside",
                            Classification::Uncertain => "uncertain",
                            Classification::Unknown => "unknown",
                        }
                }),
                _ => return Ok(None),
            },
            Self::Nearest { radius_m, reported_center } => {
                if value.position_accuracy.is_some() && !reported_center {
                    return Ok(None);
                }
                let distance = distance(work).map_err(geometry_error)?;
                return Ok((distance.is_finite() && radius_m.is_none_or(|radius| distance <= radius)).then_some(Some(distance)));
            },
        };
        Ok(exact.map_err(geometry_error)?.then_some(None))
    }
}

/// The region of a read, prepared once (`read.rs` `PreparedRegion`).
struct Region {
    predicate: Predicate,
    geometry: Option<(Geometry, Option<PreparedGeometry>)>,
    /// Inclusive key ranges; `None` reads the whole key space.
    ranges: Option<Vec<CellRange>>,
}

impl Region {
    fn new(bytes: &[u8], predicate: Predicate) -> anyhow::Result<Self> {
        if matches!(predicate, Predicate::All) {
            ensure!(bytes.is_empty(), invalid("An unrestricted spatial read takes no region"));
            return Ok(Self { predicate, geometry: None, ranges: None });
        }
        let mut work = GeometryWork::with_limits(WORK_STEPS, WORK_BYTES);
        let region = Location::decode(bytes, &mut work)?;
        ensure!(
            !matches!(predicate, Predicate::Nearest { .. }) || matches!(region.geometry.coords, Coords::Point(_)),
            invalid("Nearest requires a nonempty Point origin")
        );
        if !region.geometry.is_empty() && let Some(cover) = predicate.cover_predicate() {
            let prepared = predicate.prepare(&region.geometry, &mut work)?;
            let ranges = candidate_ranges(&prepared, cover, &mut work).map_err(geometry_error)?;
            return Ok(Self { predicate, geometry: Some((region.geometry, Some(prepared))), ranges: Some(ranges) });
        }
        let prepared = if region.geometry.is_empty() || matches!(predicate, Predicate::Classification(Classification::Unknown)) {
            None
        } else {
            predicate.prepare(&region.geometry, &mut GeometryWork::with_limits(WORK_STEPS, WORK_BYTES)).ok()
        };
        Ok(Self { predicate, geometry: Some((region.geometry, prepared)), ranges: None })
    }

    fn evaluate(&self, candidate: &Location, work: &mut GeometryWork) -> anyhow::Result<Option<Option<f64>>> {
        match &self.geometry {
            None => Ok(Some(None)),
            Some((geometry, prepared)) => self.predicate.evaluate(candidate, geometry, prepared.as_ref(), work),
        }
    }
}

/// The 32-byte partition of an image: SHA-256 over each partition cell's
/// Binary bytes, length-framed (`postings.rs` `Partition::of`); an absent
/// or null cell is empty.
fn partition_of<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// The bytes of a Binary cell, `None` for a null.
fn binary_cell(cell: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    match cell.split_first() {
        Some((&NULL_CELL, [])) => Ok(None),
        Some((&BINARY_CELL, rest)) => {
            let mut out = Vec::with_capacity(rest.len());
            let mut bytes = rest.iter();
            loop {
                match bytes.next().context("rtdb.owned/spatial-path: a truncated Binary cell")? {
                    0 => match bytes.next().context("rtdb.owned/spatial-path: a truncated Binary cell")? {
                        0 => break,
                        0xff => out.push(0),
                        _ => anyhow::bail!("rtdb.owned/spatial-path: an invalid Binary cell"),
                    },
                    &byte => out.push(byte),
                }
            }
            ensure!(bytes.next().is_none(), "rtdb.owned/spatial-path: a spatial column holds Binary values");
            Ok(Some(out))
        },
        _ => anyhow::bail!("rtdb.owned/spatial-path: a spatial column holds Binary values"),
    }
}

fn cells(image: &Image, columns: &[String]) -> anyhow::Result<Vec<Vec<u8>>> {
    columns
        .iter()
        .map(|column| Ok(image.get(column).map(|cell| binary_cell(cell)).transpose()?.flatten().unwrap_or_default()))
        .collect()
}

fn entry_values(partition: &[u8; 32], key: i64) -> Vec<u8> {
    [&partition[..], &ordered_i64(key)].concat()
}

/// The entries one image writes into a spatial path over `columns`
/// (geometry first, then partition columns).
pub fn entries(columns: &[String], encoding: &str, image: &Image) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let encoding = SpatialEncoding::parse(encoding)?;
    let partition = partition_of(cells(image, &columns[1..])?.iter().map(Vec::as_slice));
    let keys = match image.get(&columns[0]).map(|cell| binary_cell(cell)).transpose()?.flatten() {
        None => vec![i64::MIN],
        Some(bytes) => {
            let mut work = GeometryWork::with_limits(WORK_STEPS, WORK_BYTES);
            match Location::decode_stored(&bytes, encoding, &mut work)? {
                None => vec![i64::MIN],
                Some(location) => location.keys(&mut work)?,
            }
        },
    };
    let keys = keys.into_iter().collect::<BTreeSet<_>>();
    Ok(keys.into_iter().map(|key| (entry_values(&partition, key), Vec::new())).collect())
}

/// One cell of a nearest frontier: its subtree, or (`own`) only the shapes
/// keyed at the cell itself.
#[derive(Clone, Copy, Debug)]
struct Frontier {
    bound: f64,
    cell: SearchCell,
    own: bool,
}

impl PartialEq for Frontier {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Frontier {}

impl PartialOrd for Frontier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Frontier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bound.total_cmp(&other.bound).then_with(|| self.cell.cmp(&other.cell)).then_with(|| self.own.cmp(&other.own))
    }
}

impl Frontier {
    fn new(cell: SearchCell, origin: geometry::Pos, own: bool) -> Self {
        Self { bound: cell.distance_lower_bound(origin), cell, own }
    }

    fn keys(&self) -> CellRange {
        if self.own { self.cell.own_range() } else { self.cell.range() }
    }

    /// Whether its entries may be split among children instead of read.
    fn splits(&self) -> bool {
        !self.own && self.cell.level() < CELL_LEVELS
    }
}

/// The `(limit + 1)`-th smallest distance among the matches a nearest page
/// may return: once every unread row is farther, the page is decided.
fn decided_distance(matched: &[SpatialRow], request: &SpatialQuery) -> Option<f64> {
    let SpatialOrder::Nearest { .. } = &request.order else { return None };
    let mut distances = matched
        .iter()
        .filter(|row| eligible(row, &request.order))
        .map(|row| row.distance_m.expect("ranked"))
        .collect::<Vec<_>>();
    if distances.len() <= request.limit {
        return None;
    }
    distances.sort_by(f64::total_cmp);
    Some(distances[request.limit])
}

/// The order and continuation of a spatial page.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SpatialOrder {
    /// Ascending `(id, partition)`, strictly after `after`.
    Identity { after: Option<(RowId, usize)> },
    /// Ascending `(distance, id, partition)` strictly after `after`, within
    /// the half-open radius window `(after_radius, radius]` (`SpatialRankOpen.v2`).
    Nearest {
        after: Option<NearestAfter>,
        #[serde(with = "super::wire::option_f64_bits")]
        after_radius_m: Option<f64>,
    },
}

/// A nearest continuation: the last row's distance, id and partition.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NearestAfter {
    #[serde(with = "super::wire::f64_bits")]
    pub distance_m: f64,
    pub id: RowId,
    pub partition: usize,
}

/// One partition of a spatial read: a relation's spatial path in one frame.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpatialPartition {
    pub frame: usize,
    pub relation: String,
    pub path: String,
    /// The partition's position in the caller's request.
    pub ordinal: usize,
}

/// A spatial read (`SpatialOpen`/`Next`, `SpatialRankOpen`/`Next`,
/// `SpatialHeld`, `SpatialProbe`'s matches).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpatialQuery {
    pub partitions: Vec<SpatialPartition>,
    /// The bound prefix of the partition columns' Binary values.
    pub partition: Vec<Vec<u8>>,
    /// The tagged WKB region; empty for `all`.
    pub region: Vec<u8>,
    pub predicate: String,
    #[serde(with = "super::wire::option_f64_bits")]
    pub radius_m: Option<f64>,
    pub reported_center: bool,
    pub valid: Option<i64>,
    pub order: SpatialOrder,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpatialRow {
    pub id: RowId,
    #[serde(with = "super::wire::option_f64_bits")]
    pub distance_m: Option<f64>,
    pub image: Image,
    pub revision_micros: i64,
    /// The location is a plain Point (`point-intersects-v1` certificates).
    pub point_geometry: bool,
    /// The partition's ordinal.
    pub partition: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpatialPage {
    pub present_micros: i64,
    pub valid_micros: i64,
    pub rows: Vec<SpatialRow>,
    pub more: bool,
    /// The next row's distance, for a ranked page with more.
    #[serde(with = "super::wire::option_f64_bits")]
    pub next_distance_m: Option<f64>,
    /// The read set of each partition, by ordinal.
    pub reads: Vec<Vec<KeyRange>>,
}

/// Whether a row lies strictly after the page's continuation.
fn eligible(row: &SpatialRow, order: &SpatialOrder) -> bool {
    match order {
        SpatialOrder::Identity { after } => after.as_ref().is_none_or(|(id, partition)| (&row.id, row.partition) > (id, *partition)),
        SpatialOrder::Nearest { after, after_radius_m } => {
            let distance = row.distance_m.expect("A nearest match has a distance");
            after_radius_m.is_none_or(|radius| distance > radius)
                && after.as_ref().is_none_or(|after| {
                    distance.total_cmp(&after.distance_m).then_with(|| row.id.cmp(&after.id)).then_with(|| row.partition.cmp(&after.partition)).is_gt()
                })
        },
    }
}

/// The global order of spatial rows.
fn order(rows: &mut [SpatialRow], order: &SpatialOrder) {
    match order {
        SpatialOrder::Identity { .. } => rows.sort_by(|left, right| (&left.id, left.partition).cmp(&(&right.id, right.partition))),
        SpatialOrder::Nearest { .. } => rows.sort_by(|left, right| {
            left.distance_m
                .expect("ranked")
                .total_cmp(&right.distance_m.expect("ranked"))
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.partition.cmp(&right.partition))
        }),
    }
}

/// Window the eligible rows of one or more parts of a spatial read: the
/// page, whether more follow, and the next row's distance.
pub fn window(request: &SpatialQuery, mut rows: Vec<SpatialRow>) -> (Vec<SpatialRow>, bool, Option<f64>) {
    rows.retain(|row| eligible(row, &request.order));
    order(&mut rows, &request.order);
    let more = rows.len() > request.limit;
    let next = match (&request.order, more) {
        (SpatialOrder::Nearest { .. }, true) => rows[request.limit].distance_m,
        _ => None,
    };
    rows.truncate(request.limit);
    (rows, more, next)
}

/// The first `limit + 1` eligible rows of a page in the global order: what
/// a part of a split read contributes to the merge.
pub fn head(request: &SpatialQuery, rows: Vec<SpatialRow>) -> Vec<SpatialRow> {
    let mut rows = rows.into_iter().filter(|row| eligible(row, &request.order)).collect::<Vec<_>>();
    order(&mut rows, &request.order);
    rows.truncate(request.limit.saturating_add(1));
    rows
}

/// What evaluating one spatial candidate needs.
struct Candidate<'a> {
    frame: &'a Frame,
    partition: &'a SpatialPartition,
    bound: &'a [Vec<u8>],
    column: &'a str,
    partitions: &'a [String],
    encoding: SpatialEncoding,
    region: &'a Region,
    valid: i64,
}

/// Every entry of each range at `at`: the ranges go to the store in batched
/// reads, and only a range that resumes is read again.
async fn scan_ranges<S: RangeSource + ?Sized>(source: &S, ranges: &[KeyRange], at: Version) -> anyhow::Result<Vec<Vec<Entry>>> {
    Ok(probe_ranges(source, ranges, at, usize::MAX, &vec![true; ranges.len()]).await?.into_iter().map(|(entries, _)| entries).collect())
}

/// The first `limit` entries of each range at `at`, in batched reads,
/// following resumes for the ranges `whole` marks; with each range's
/// entries, whether it held more than it returned.
async fn probe_ranges<S: RangeSource + ?Sized>(source: &S, ranges: &[KeyRange], at: Version, limit: usize, whole: &[bool]) -> anyhow::Result<Vec<(Vec<Entry>, bool)>> {
    let mut out = vec![(Vec::new(), false); ranges.len()];
    for (chunk, slice) in ranges.chunks(MAX_READ_RANGES).enumerate() {
        let page = source.read(RangeRead { ranges: slice.to_vec(), at: Some(at), after: None, limit }).await?;
        ensure!(page.pages.len() == slice.len(), "A read returns one page per range");
        for (offset, page) in page.pages.into_iter().enumerate() {
            let index = chunk * MAX_READ_RANGES + offset;
            out[index].0.extend(page.entries);
            let mut resume = page.resume;
            if !whole[index] {
                out[index].1 = resume.is_some();
                continue;
            }
            while let Some(after) = resume.take() {
                let more = source.read(RangeRead { ranges: vec![ranges[index].clone()], at: Some(at), after: Some(after), limit: usize::MAX }).await?;
                let more = more.pages.into_iter().next().context("A read returns one page per range")?;
                out[index].0.extend(more.entries);
                resume = more.resume;
            }
        }
    }
    Ok(out)
}

/// One candidate resolved at the cut and judged exactly: the row it
/// presents, when its location satisfies the predicate.
async fn spatial_candidate<S: RangeSource + ?Sized>(source: &S, candidate: &Candidate<'_>, id: RowId, work: &mut GeometryWork, reads: &mut Vec<KeyRange>) -> anyhow::Result<Option<SpatialRow>> {
    let Candidate { frame, partition, bound, column, partitions, encoding, region, valid } = candidate;
    let mut row_reads = Vec::new();
    let Some(winner) = frame.winner(source, &partition.relation, &id, *valid, &mut row_reads).await? else { return Ok(None) };
    let revision = winner.precedence.system_from;
    let Some(image) = winner.image else { return Ok(None) };
    if frame.world.is_some() {
        reads.extend(row_reads);
    }
    let cells = cells(&image, &partitions[..bound.len()])?;
    if cells != *bound {
        return Ok(None);
    }
    let ordinal = partition.ordinal;
    let Some(bytes) = image.get(*column).map(|cell| binary_cell(cell)).transpose()?.flatten() else {
        return Ok(region.geometry.is_none().then_some(SpatialRow { id, distance_m: None, image, revision_micros: revision, point_geometry: false, partition: ordinal }));
    };
    let outcome = work.scoped(|work| -> anyhow::Result<_> {
        let Some(location) = Location::decode_stored(&bytes, *encoding, work)? else { return Ok(None) };
        let point = location.is_point_geometry();
        Ok(region.evaluate(&location, work)?.map(|distance| (distance, point)))
    })?;
    Ok(outcome.map(|(distance_m, point_geometry)| SpatialRow { id, distance_m, image, revision_micros: revision, point_geometry, partition: ordinal }))
}

/// The matches of an unbounded nearest read, best first: a frontier of
/// cells ordered by their distance bound from `origin`, probed in rounds of
/// nearby cells (one store read each); a cell holding more than a probe is
/// split into its children (and, for shapes, the cell's own key). The walk
/// stops once the `(limit + 1)`-th eligible match is nearer than every
/// unread cell, so the page, its `more` and its next distance are those of
/// ranking every row. Every nominated row is resolved and judged exactly,
/// as a full read would judge it.
async fn nearest_first<S: RangeSource + ?Sized>(
    source: &S,
    request: &SpatialQuery,
    candidate: &Candidate<'_>,
    partition: &[u8; 32],
    origin: geometry::Pos,
    work: &mut GeometryWork,
    reads: &mut Vec<KeyRange>,
) -> anyhow::Result<Vec<SpatialRow>> {
    let (relation, path) = (&candidate.partition.relation, &candidate.partition.path);
    let places = candidate.frame.places();
    let version = candidate.frame.version;
    let mut frontier = SearchCell::roots()
        .into_iter()
        .chain(SearchCell::shape_roots())
        .map(|cell| Reverse(Frontier::new(cell, origin, false)))
        .collect::<BinaryHeap<_>>();
    let mut seen = BTreeSet::new();
    let mut matched = Vec::new();
    while let Some(Reverse(next)) = frontier.peek().copied() {
        let decided = decided_distance(&matched, request);
        if decided.is_some_and(|distance| distance + NEAREST_BOUND_SLACK_M < next.bound) {
            break;
        }
        // A round: the cells within twice the nearest bound, never past the
        // decided distance.
        let mut reach = (next.bound * 2.0).max(next.bound + 1.0);
        if let Some(distance) = decided {
            reach = reach.min(distance + NEAREST_BOUND_SLACK_M);
        }
        let mut round = Vec::new();
        while let Some(Reverse(top)) = frontier.peek().copied()
            && round.len() < NEAREST_ROUND
            && (round.is_empty() || top.bound <= reach)
        {
            frontier.pop();
            round.push(top);
        }
        let mut ranges = Vec::with_capacity(round.len() * places.len());
        let mut whole = Vec::with_capacity(ranges.capacity());
        for cell in &round {
            let keys = cell.keys();
            let (low, high) = (entry_values(partition, keys.start), entry_values(partition, keys.end));
            for place in &places {
                ranges.push(keys::path_values(*place, relation, path, std::ops::Bound::Included(&low), std::ops::Bound::Included(&high)));
                whole.push(!cell.splits());
            }
        }
        let pages = probe_ranges(source, &ranges, version, NEAREST_PROBE, &whole).await?;
        let mut nominated = Vec::new();
        for (index, cell) in round.iter().enumerate() {
            let cell_pages = &pages[index * places.len()..(index + 1) * places.len()];
            if cell.splits() && cell_pages.iter().any(|(_, more)| *more) {
                for child in cell.cell.children() {
                    frontier.push(Reverse(Frontier::new(child, origin, false)));
                }
                if cell.cell.is_shape() {
                    frontier.push(Reverse(Frontier::new(cell.cell, origin, true)));
                }
                continue;
            }
            for (place, (entries, _)) in places.iter().zip(cell_pages) {
                for entry in entries {
                    let (_, id) = keys::path_entry_parts(*place, relation, path, &entry.key)?;
                    if seen.insert(id.clone()) {
                        ensure!(seen.len() <= MAX_SPATIAL_CANDIDATES, "rtdb.spatial/work-limit: the spatial read nominates more than {MAX_SPATIAL_CANDIDATES} rows");
                        nominated.push(id);
                    }
                }
            }
        }
        for id in nominated {
            matched.extend(spatial_candidate(source, candidate, id, work, reads).await?);
        }
    }
    Ok(matched)
}

/// A Root-place range of a path, at another place (a world level).
fn rebase(range: &KeyRange, from: Place, to: Place, relation: &str, path: &str) -> KeyRange {
    if from == to {
        return range.clone();
    }
    let (old, new) = (keys::path_all(from, relation, path), keys::path_all(to, relation, path));
    // Every range of a path starts inside its tablet; an end outside it is
    // the tablet's own end.
    let swap = |key: &[u8]| match key.strip_prefix(old.start.as_slice()) {
        Some(rest) => Some([&new.start[..], rest].concat()),
        None => new.end.clone(),
    };
    KeyRange { start: swap(&range.start).expect("A path range starts in its tablet"), end: range.end.as_deref().and_then(swap) }
}

/// One partition's installed path, checked before the region is prepared
/// (the refusal order of a one-partition read).
struct Installed<'a> {
    frame: &'a Frame,
    partition: &'a SpatialPartition,
    columns: Vec<String>,
    encoding: SpatialEncoding,
    valid: i64,
}

async fn install<'a, S: RangeSource + ?Sized>(source: &S, frames: &'a [Frame], request: &SpatialQuery, partition: &'a SpatialPartition) -> anyhow::Result<Installed<'a>> {
    let frame = frames.get(partition.frame).context("A spatial partition names one of the request's frames")?;
    let (relation, path) = (&partition.relation, &partition.path);
    frame.admit(relation)?;
    let declaration = catalog::declaration(source, frame.place, relation, Some(frame.version)).await?.context("rtdb.owned/unknown-relation")?;
    let valid = request.valid.unwrap_or(frame.present_micros);
    ensure!(
        declaration.lifetime_micros.is_none() || valid == frame.present_micros,
        "rtdb.owned/lifetime-uncovered: a lifetime relation is read at the held present"
    );
    let installed = catalog::path(source, frame.place, relation, path, Some(frame.version))
        .await?
        .context("rtdb.owned/spatial-path: the relation has no such spatial path")?;
    let PathKind::Spatial { encoding } = &installed.kind else {
        anyhow::bail!("rtdb.owned/spatial-path: {relation}.{path} is not a spatial path");
    };
    let encoding = SpatialEncoding::parse(encoding)?;
    ensure!(request.partition.len() < installed.columns.len(), invalid("The partition binds more columns than the path has"));
    Ok(Installed { frame, partition, columns: installed.columns, encoding, valid })
}

/// The matched rows of one partition (every match the read judged, before
/// the window) and its read set.
async fn read_partition<S: RangeSource + ?Sized>(
    source: &S,
    request: &SpatialQuery,
    installed: &Installed<'_>,
    predicate: Predicate,
    region: &Region,
    work: &mut GeometryWork,
) -> anyhow::Result<(Vec<SpatialRow>, Vec<KeyRange>)> {
    let Installed { frame, partition, columns, encoding, valid } = installed;
    let (frame, partition) = (*frame, *partition);
    let (relation, path) = (&partition.relation, &partition.path);
    let partitions = &columns[1..];
    // The consulted ranges: per covered key range in the bound partition,
    // or the whole path when the partition is only a prefix or the
    // predicate is uncovered.
    let full = request.partition.len() == partitions.len();
    let bound_partition = full.then(|| partition_of(request.partition.iter().map(Vec::as_slice)));
    let mut ranges = Vec::new();
    match (&bound_partition, &region.ranges) {
        (Some(bound), Some(cells)) => {
            for cell in cells {
                let (low, high) = (entry_values(bound, cell.start), entry_values(bound, cell.end));
                ranges.push(keys::path_values(frame.place, relation, path, std::ops::Bound::Included(&low), std::ops::Bound::Included(&high)));
            }
        },
        (Some(bound), None) => ranges.push(keys::path_run_prefix(frame.place, relation, path, bound)),
        (None, _) => ranges.push(keys::path_all(frame.place, relation, path)),
    }
    let mut reads = match &frame.world {
        None => ranges.clone(),
        Some(world) => frame.world_reads(world, relation, path),
    };
    let candidate = Candidate { frame, partition, bound: &request.partition, column: &columns[0], partitions, encoding: *encoding, region, valid: *valid };
    let origin = match (&predicate, &bound_partition, &region.geometry) {
        (Predicate::Nearest { radius_m: None, .. }, Some(_), Some((geometry, _))) => match &geometry.coords {
            Coords::Point(origin) if !geometry.is_empty() => Some(*origin),
            _ => None,
        },
        _ => None,
    };
    let matched = match (origin, &bound_partition) {
        // An unbounded nearest walks the cells best first.
        (Some(origin), Some(bound)) => nearest_first(source, request, &candidate, bound, origin, work, &mut reads).await?,
        _ => {
            // The cover's ranges at every level, in batched reads.
            let mut batch = Vec::new();
            for place in frame.places() {
                for range in &ranges {
                    batch.push((place, rebase(range, frame.place, place, relation, path)));
                }
            }
            let pages = scan_ranges(source, &batch.iter().map(|(_, range)| range.clone()).collect::<Vec<_>>(), frame.version).await?;
            let mut candidates = BTreeSet::new();
            for ((place, _), entries) in batch.iter().zip(pages) {
                for entry in entries {
                    let (values, id) = keys::path_entry_parts(*place, relation, path, &entry.key)?;
                    // A prefix-bound read filters keys by the region's
                    // ranges; partition bytes are re-checked on the image.
                    if bound_partition.is_none() && let Some(cells) = &region.ranges {
                        let key = unordered_i64(values.get(32..40).context("Invalid owned spatial entry")?.try_into()?);
                        if !cells.iter().any(|cell| cell.start <= key && key <= cell.end) {
                            continue;
                        }
                    }
                    candidates.insert(id);
                    ensure!(candidates.len() <= MAX_SPATIAL_CANDIDATES, "rtdb.spatial/work-limit: the spatial read nominates more than {MAX_SPATIAL_CANDIDATES} rows");
                }
            }
            let mut matched = Vec::new();
            for id in candidates {
                if let SpatialOrder::Identity { after: Some((after, ordinal)) } = &request.order
                    && (&id, partition.ordinal) <= (after, *ordinal)
                {
                    continue;
                }
                matched.extend(spatial_candidate(source, &candidate, id, work, &mut reads).await?);
            }
            matched
        },
    };
    Ok((matched, reads))
}

/// Check a spatial read's bounds and prepare its predicate and region.
fn prepare(request: &SpatialQuery) -> anyhow::Result<(Predicate, Region)> {
    let predicate = Predicate::parse(&request.predicate, request.radius_m, request.reported_center)?;
    let nearest = matches!(predicate, Predicate::Nearest { .. });
    ensure!(nearest == matches!(request.order, SpatialOrder::Nearest { .. }), invalid("A nearest predicate pages by distance, any other by identity"));
    let region = Region::new(&request.region, predicate)?;
    Ok((predicate, region))
}

/// Every eligible match of a spatial read's partitions, and each
/// partition's read set by ordinal.
pub async fn gather<S: RangeSource + ?Sized>(source: &S, frames: &[Frame], request: &SpatialQuery) -> anyhow::Result<(Vec<SpatialRow>, Vec<(usize, Vec<KeyRange>)>)> {
    ensure!((1..=MAX_SPATIAL_PAGE).contains(&request.limit), "rtdb.spatial/invalid: a spatial page holds 1 to {MAX_SPATIAL_PAGE} rows");
    ensure!((1..=MAX_SPATIAL_PARTITIONS).contains(&request.partitions.len()), invalid("A spatial read names 1 to 32 partitions"));
    let mut installed = Vec::with_capacity(request.partitions.len());
    for partition in &request.partitions {
        installed.push(install(source, frames, request, partition).await?);
    }
    let (predicate, region) = prepare(request)?;
    let mut work = GeometryWork::with_limits(WORK_STEPS, WORK_BYTES);
    let distinct = request.partitions.iter().map(|partition| (frames.get(partition.frame).map(|frame| frame.place), &partition.relation, &partition.path)).collect::<BTreeSet<_>>();
    ensure!(distinct.len() == request.partitions.len(), invalid("A spatial read names each partition once"));
    let mut rows = Vec::new();
    let mut reads = Vec::with_capacity(request.partitions.len());
    for installed in &installed {
        let (matched, partition_reads) = read_partition(source, request, installed, predicate, &region, &mut work).await?;
        rows.extend(matched);
        reads.push((installed.partition.ordinal, partition_reads));
    }
    Ok((rows, reads))
}

/// A spatial page over the request's partitions. A part of a split read
/// (`head`) returns its first `limit + 1` rows instead of the window: the
/// merge decides `more` and the next distance over the union.
pub async fn page<S: RangeSource + ?Sized>(source: &S, frames: &[Frame], request: &SpatialQuery, head_only: bool) -> anyhow::Result<SpatialPage> {
    let first = frames.get(request.partitions.first().context("A spatial read names a partition")?.frame).context("A spatial partition names one of the request's frames")?;
    let (present_micros, valid_micros) = (first.page_present(), request.valid.unwrap_or(first.present_micros));
    let (rows, mut reads) = gather(source, frames, request).await?;
    let (rows, more, next_distance_m) = if head_only { (head(request, rows), false, None) } else { window(request, rows) };
    reads.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(SpatialPage { present_micros, valid_micros, rows, more, next_distance_m, reads: reads.into_iter().map(|(_, reads)| reads).collect() })
}
