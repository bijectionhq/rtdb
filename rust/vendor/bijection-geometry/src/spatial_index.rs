//! Private ordinary spatial-index entries. The series trajectory encoding
//! remains owned by `spatial_key`; changing this recipe never rewrites it.
//! Keys name `geometry::cells` cube-face cells on the auxiliary sphere: a
//! point keys its leaf under its accuracy class, a shape keys a bounded
//! cover of cells, and a query reads the cover of its region grown by each
//! class's reach.
use crate as geometry;
use geometry::{
    cells::{
        self,
        CellId,
    },
    geodesy::SEMI_MINOR_AXIS_METRES,
    predicates::{
        prepared::{
            Near,
            PreparedGeometry,
        },
        CoverPredicate,
        MAX_POINT_DISTANCE_METRES,
    },
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

use crate::spatial_key::{
    cover_ranges,
    descendants,
    key,
    merge,
    KeyRange,
    CELL_LEVELS,
};

pub const ENCODING: &str = "wgs84-aux-cube27-hilbert-v2";
pub const MAX_SHAPE_CELLS: usize = 32;
pub const MAX_QUERY_CELLS: usize = 128;

// Four category bits and 58 cell bits fit a nonnegative signed int64.
pub const ACCURACY_BOUNDS: [f64; 8] = [
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
    MAX_POINT_DISTANCE_METRES,
];
pub const UNKNOWN_CLASS: u8 = 8;
const SHAPE_CLASS: u8 = 9;
const EMPTY_CLASS: u8 = 10;

/// Why a reported accuracy cannot be keyed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccuracyRefusal;
impl std::fmt::Display for AccuracyRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Accuracy must be finite non-negative metres")
    }
}
impl std::error::Error for AccuracyRefusal {}

const _: () = assert!(SHAPE_CLASS < 16 && UNKNOWN_CLASS as usize == ACCURACY_BOUNDS.len());
const _: () = assert!(cells::ID_BITS + 4 < 64);

pub fn point_key(point: &Pos, accuracy: Option<f64>) -> Result<i64, AccuracyRefusal> {
    let class = match accuracy {
        None => UNKNOWN_CLASS,
        Some(radius) if radius.is_finite() && radius >= 0.0 => ACCURACY_BOUNDS
            .iter()
            .position(|bound| radius <= *bound)
            .unwrap_or(ACCURACY_BOUNDS.len() - 1)
            as u8,
        Some(_) => return Err(AccuracyRefusal),
    };
    Ok(key(class, cells::leaf_of(*point)))
}

/// Private maintained-nearest influence cells: the cover of the cap of
/// directions the radius can reach, without accuracy classes, since ranking
/// measures reported centres independently of their accuracy.
pub fn nearest_influence_keys(
    origin: Pos,
    radius_m: f64,
    work: &mut GeometryWork,
) -> Result<Vec<i64>, GeometryRefusal> {
    let radius_m =
        (radius_m + geometry::predicates::distance_slack(radius_m)).min(MAX_POINT_DISTANCE_METRES);
    let cap = cells::Cap {
        centre: geometry::arcs::unit(geometry::arcs::direction(origin)),
        // A radian of auxiliary angle spans at least b metres; a margin for
        // the angle's rounding.
        angle: radius_m / SEMI_MINOR_AXIS_METRES * (1.0 + 1e-9) + 1e-12,
    };
    Ok(cells::cover(&cap, MAX_SHAPE_CELLS, work)?
        .into_iter()
        .map(|cell| cell.raw() as i64)
        .collect())
}

/// Probe every possible containing influence cell. No point body is retained.
pub fn point_influence_keys(point: Pos) -> Vec<i64> {
    let leaf = cells::leaf_of(point);
    (0..=CELL_LEVELS)
        .map(|level| leaf.parent(level).raw() as i64)
        .collect()
}

/// A bounded cover of an admitted shape: the cells it meets, refined while
/// they fit, and the cells wholly inside it kept whole.
pub fn shape_keys(
    geometry: &Geometry,
    work: &mut GeometryWork,
) -> Result<Vec<i64>, GeometryRefusal> {
    if geometry.is_empty() {
        return Ok(vec![empty_key()]);
    }
    work.scoped(|work| {
        work.allocate(geometry.position_count().saturating_mul(64) + MAX_SHAPE_CELLS * 128)?;
        let prepared = PreparedGeometry::new(
            geometry.clone(),
            geometry::predicates::prepared::Preparation::Topology,
            work,
        )?;
        let cover = cells::cover(
            &Near {
                geometry: &prepared,
                reach_m: 0.0,
            },
            MAX_SHAPE_CELLS,
            work,
        )?;
        if cover.is_empty() {
            return Err(geometry::invalid(
                "A nonempty geometry must have an index covering",
            ));
        }
        Ok(cover.into_iter().map(|cell| key(SHAPE_CLASS, cell)).collect())
    })
}

/// An admitted empty shape is distinct from an absent location.
pub fn empty_key() -> i64 {
    (u64::from(EMPTY_CLASS) << cells::ID_BITS) as i64
}

pub fn shape_class_start() -> i64 {
    (u64::from(SHAPE_CLASS) << cells::ID_BITS) as i64
}

pub fn unknown_range() -> KeyRange {
    KeyRange {
        start: descendants(UNKNOWN_CLASS, CellId::faces()[0]).start,
        end: descendants(UNKNOWN_CLASS, CellId::faces()[5]).end,
    }
}

/// The key ranges a region's candidate rows lie in: for each accuracy class,
/// the cover of the region grown by that class's reach; unknown accuracy
/// where the predicate admits it; and shapes meeting the region grown by the
/// reach of an exact position, through their cells' ancestors too.
pub fn candidate_ranges(
    region: &PreparedGeometry,
    predicate: CoverPredicate,
    work: &mut GeometryWork,
) -> Result<Vec<KeyRange>, GeometryRefusal> {
    work.scoped(|work| {
        let max_ranges =
            2 * MAX_QUERY_CELLS * (ACCURACY_BOUNDS.len() + 1 + CELL_LEVELS as usize + 1);
        work.allocate(2 * max_ranges * std::mem::size_of::<KeyRange>())?;
        let cover_of = |reach: f64, work: &mut GeometryWork| {
            cells::cover(
                &Near {
                    geometry: region,
                    reach_m: reach.min(MAX_POINT_DISTANCE_METRES),
                },
                MAX_QUERY_CELLS,
                work,
            )
        };
        let mut ranges = Vec::with_capacity(max_ranges);
        for (class, accuracy) in ACCURACY_BOUNDS.iter().enumerate() {
            for cell in cover_of(predicate.reach(*accuracy), work)? {
                ranges.push(descendants(class as u8, cell));
            }
        }
        let exact = cover_of(predicate.reach(0.0), work)?;
        // Unknown accuracy never satisfies a positive classification.
        if matches!(
            predicate,
            CoverPredicate::Touches | CoverPredicate::Within { .. }
        ) {
            for cell in &exact {
                ranges.push(descendants(UNKNOWN_CLASS, *cell));
            }
        }
        ranges.extend(cover_ranges(SHAPE_CLASS, &exact, work)?);
        Ok(merge(ranges))
    })
}

/// A nearest frontier cell of one key class: a point accuracy class or the
/// shape class. A point keys its leaf; a shape keys each cell of its bounded
/// cover, any level, so a split shape cell keeps its own key as a separate
/// probe ([`Self::own_range`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SearchCell {
    class: u8,
    cell: CellId,
}
impl SearchCell {
    /// The six faces of every point class, class by class.
    pub fn roots() -> Vec<Self> {
        (0..=UNKNOWN_CLASS).flat_map(Self::class_roots).collect()
    }

    /// The six faces of the shape class.
    pub fn shape_roots() -> Vec<Self> {
        Self::class_roots(SHAPE_CLASS)
    }

    /// Whether entries may key this cell itself rather than a leaf below it.
    pub fn is_shape(self) -> bool {
        self.class == SHAPE_CLASS
    }

    /// Only the entries keyed at this cell itself: no child range holds them.
    pub fn own_range(self) -> KeyRange {
        let own = key(self.class, self.cell);
        KeyRange {
            start: own,
            end: own,
        }
    }

    /// The six faces of one class.
    pub fn class_roots(class: u8) -> Vec<Self> {
        CellId::faces()
            .into_iter()
            .map(|cell| Self { class, cell })
            .collect()
    }

    pub fn children(self) -> Vec<Self> {
        self.cell
            .children()
            .map(|children| {
                children
                    .into_iter()
                    .map(|cell| Self {
                        class: self.class,
                        cell,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn range(self) -> KeyRange {
        descendants(self.class, self.cell)
    }

    pub fn level(self) -> u32 {
        self.cell.level()
    }

    /// A lower bound on the distance from `origin` to any point keyed into
    /// the cell, and so to any shape keyed at the cell or below it, since
    /// a shape's cover holds every one of its points: b times the auxiliary
    /// angle to the cell.
    pub fn distance_lower_bound(self, origin: Pos) -> f64 {
        cells::distance_lower_bound(origin, self.cell)
    }
}
