//! Native series trajectory covers and the key arithmetic shared with
//! ordinary spatial indexes, which own their separate recipe in
//! `spatial_index`.
//!
//! Cells are `geometry::cells` cube-face cells on the auxiliary sphere: 27
//! quadtree levels in Hilbert order, a 58-bit id whose descendants occupy one
//! contiguous range. A key is a 4-bit class above the id; series covers use
//! class 6. Class bits belong to each physical owner and do not change source
//! geometry meaning.
use geometry::{
    cells::{
        self,
        CellId,
    },
    predicates::prepared::{
        Near,
        PreparedGeometry,
    },
    GeometryRefusal,
    GeometryWork,
    Pos,
};

/// Quadtree levels of a leaf cell.
pub const CELL_LEVELS: u32 = cells::MAX_LEVEL;
/// The category owned by native series trajectory covers.
pub const TRAJECTORY_CLASS: u8 = 6;
/// Cells covering one trajectory segment or one trajectory query region.
const TRAJECTORY_CELLS: usize = 8;

/// A disjoint, bounded multi-cell cover of a native trajectory segment, a
/// great elliptic arc. The time interval and logical series remain with its
/// owner. The arc's cells are found by exact crossing tests on padded cells,
/// so positions at ±180 and a pole's longitudes need no aliases.
pub fn trajectory_keys(
    a: Pos,
    b: Pos,
    work: &mut GeometryWork,
) -> Result<Vec<i64>, GeometryRefusal> {
    work.allocate(1024)?;
    let mut keys: Vec<i64> =
        cells::cover(&cells::ArcTarget::new(a, b), TRAJECTORY_CELLS, work)?
            .into_iter()
            .map(|cell| key(TRAJECTORY_CLASS, cell))
            .collect();
    work.charge(keys.len().saturating_mul(6))?;
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}

/// Candidate key intervals for the segments meeting a region. Exact geometry
/// refinement remains mandatory; ancestors and descendants both participate.
pub fn trajectory_ranges(
    region: &PreparedGeometry,
    work: &mut GeometryWork,
) -> Result<Vec<KeyRange>, GeometryRefusal> {
    work.allocate(1024 * std::mem::size_of::<KeyRange>())?;
    let cover = cells::cover(
        &Near {
            geometry: region,
            reach_m: 0.0,
        },
        TRAJECTORY_CELLS,
        work,
    )?;
    cover_ranges(TRAJECTORY_CLASS, &cover, work)
}

/// The key ranges that find every multi-cell cover meeting `cover`: each
/// cell's descendants, and each of its ancestors as one exact key. Merged.
pub(crate) fn cover_ranges(
    class: u8,
    cover: &[CellId],
    work: &mut GeometryWork,
) -> Result<Vec<KeyRange>, GeometryRefusal> {
    let mut ranges = Vec::with_capacity(cover.len().saturating_mul(CELL_LEVELS as usize + 1));
    for cell in cover {
        ranges.push(descendants(class, *cell));
        for level in 0..cell.level() {
            work.charge(1)?;
            let ancestor = key(class, cell.parent(level));
            ranges.push(KeyRange {
                start: ancestor,
                end: ancestor,
            });
        }
    }
    Ok(merge(ranges))
}

/// Sorted, overlapping and adjacent ranges joined.
pub(crate) fn merge(mut ranges: Vec<KeyRange>) -> Vec<KeyRange> {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<KeyRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end.saturating_add(1)
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

/// Decode one native series cover cell into a box holding it. These candidate
/// bounds never replace source trajectory refinement or its current custody.
pub fn trajectory_cell_bounds(
    encoded: i64,
    work: &mut GeometryWork,
) -> Result<GeoBounds, GeometryRefusal> {
    let invalid = || geometry::invalid("Invalid trajectory cell");
    let encoded = u64::try_from(encoded).map_err(|_| invalid())?;
    if encoded >> cells::ID_BITS != u64::from(TRAJECTORY_CLASS) {
        return Err(invalid());
    }
    let cell = CellId::from_raw(encoded & ((1u64 << cells::ID_BITS) - 1)).ok_or_else(invalid)?;
    let [west, south, east, north] = work.scoped(|work| cells::Cell::new(cell, work))?.bounds();
    Ok(GeoBounds {
        west,
        south,
        east,
        north,
    })
}

pub(crate) fn key(class: u8, cell: CellId) -> i64 {
    key_raw(class, cell.raw())
}

fn key_raw(class: u8, raw: u64) -> i64 {
    ((u64::from(class) << cells::ID_BITS) | raw) as i64
}

/// A closed longitude/latitude box; `east` may pass 180 by up to a turn.
#[derive(Clone, Copy, Debug)]
pub struct GeoBounds {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

/// Inclusive bounds on private spatial keys. These are candidate supersets;
/// the native predicate remains the authority for every returned row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start: i64,
    pub end: i64,
}

/// Every key of a class under a cell, the cell's own included.
pub(crate) fn descendants(class: u8, cell: CellId) -> KeyRange {
    let (first, last) = cell.range();
    KeyRange {
        start: key_raw(class, first),
        end: key_raw(class, last),
    }
}

#[cfg(test)]
mod trajectory_tests {
    use geometry::predicates::prepared::Preparation;

    use super::*;

    fn region(point: Pos) -> anyhow::Result<PreparedGeometry> {
        let geometry = geometry::Geometry::new(
            geometry::Kind::Point,
            geometry::Coords::Point(point),
            geometry::GeometrySpace::wgs84_xy(),
        );
        Ok(PreparedGeometry::new(
            geometry,
            Preparation::Topology,
            &mut GeometryWork::default(),
        )?)
    }

    #[test]
    fn segment_covers_include_interior_points_and_seam_aliases() -> anyhow::Result<()> {
        for (a, b) in [
            ([179.0, 0.0], [-179.0, 1.0]),
            ([-179.0, 1.0], [179.0, 0.0]),
            ([-80.0, -30.0], [80.0, 30.0]),
            ([180.0, -1.0], [-180.0, 1.0]),
            ([0.0, 89.0], [180.0, 89.0]),
            ([0.0, 0.0], [0.0, 0.0]),
        ] {
            let keys = trajectory_keys(a, b, &mut GeometryWork::default())?;
            assert!(!keys.is_empty() && keys.len() <= TRAJECTORY_CELLS);
            for fraction in [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0] {
                let point = geometry::trajectory::position_at(a, b, fraction);
                for longitude in if point[0].abs() == 180.0 {
                    vec![-180.0, 180.0]
                } else {
                    vec![point[0]]
                } {
                    let ranges = trajectory_ranges(
                        &region([longitude, point[1]])?,
                        &mut GeometryWork::default(),
                    )?;
                    assert!(
                        keys.iter().any(|key| ranges
                            .iter()
                            .any(|range| range.start <= *key && *key <= range.end)),
                        "a true trajectory contact must join the candidate cover"
                    );
                }
            }
            for key in &keys {
                let bounds = trajectory_cell_bounds(*key, &mut GeometryWork::default())?;
                assert!(bounds.west <= bounds.east && bounds.south <= bounds.north);
            }
        }
        assert!(trajectory_cell_bounds(-1, &mut GeometryWork::default()).is_err());
        assert!(trajectory_cell_bounds(0, &mut GeometryWork::default()).is_err());
        Ok(())
    }
}
