//! Private ordinary spatial-index entries. The series trajectory encoding
//! remains owned by `spatial_key`; changing this recipe never rewrites it.
//! Keys name `geometry::cells` cube-face cells on the auxiliary sphere: a
//! point keys its leaf under its accuracy class, a shape keys a bounded
//! cover of cells, and a query reads the cover of its region grown by each
//! class's reach.
use geometry::{
    cells::{
        self,
        CellId,
    },
    encounters::{
        padded_boxes,
        Metric,
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

#[cfg(test)]
mod tests {
    use geometry::{
        Coords,
        Kind,
    };

    use super::*;

    #[test]
    fn empty_geometry_has_a_private_sentinel() -> Result<(), GeometryRefusal> {
        let empty = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![]),
            geometry::GeometrySpace::wgs84_xy(),
        );
        assert_eq!(
            shape_keys(&empty, &mut GeometryWork::default())?,
            vec![empty_key()]
        );
        assert!(empty_key() > shape_class_start());
        Ok(())
    }

    fn rectangle(west: f64, south: f64, east: f64, north: f64) -> Geometry {
        Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![vec![
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]]),
            geometry::GeometrySpace::wgs84_xy(),
        )
    }

    fn prepared(geometry: Geometry) -> Result<PreparedGeometry, GeometryRefusal> {
        PreparedGeometry::new(
            geometry,
            geometry::predicates::prepared::Preparation::Topology,
            &mut GeometryWork::default(),
        )
    }

    fn point(p: Pos) -> Result<PreparedGeometry, GeometryRefusal> {
        prepared(Geometry::new(
            Kind::Point,
            Coords::Point(p),
            geometry::GeometrySpace::wgs84_xy(),
        ))
    }

    fn found(keys: &[i64], ranges: &[KeyRange]) -> bool {
        keys.iter().any(|key| {
            ranges
                .iter()
                .any(|range| range.start <= *key && *key <= range.end)
        })
    }

    #[test]
    fn work_exhaustion_refuses_the_whole_cover() -> Result<(), GeometryRefusal> {
        let geometry = rectangle(-0.01, 51.49, 0.01, 51.51);
        let mut exhausted = GeometryWork::default();
        exhausted.charge(geometry::TierBound::WorkSteps.limit() as usize)?;
        let error = shape_keys(&geometry, &mut exhausted).unwrap_err();
        assert_eq!(error.bound.unwrap().kind, geometry::TierBound::WorkSteps);
        Ok(())
    }

    #[test]
    fn multicell_cover_keeps_small_boundary_shapes_local() -> Result<(), GeometryRefusal> {
        for (west, south, east, north) in [
            (-0.01, 51.49, 0.01, 51.51),
            (30.0, -0.01, 30.02, 0.01),
            (2.34, 48.84, 2.36, 48.86),
            (179.99, -0.01, -179.99, 0.01),
        ] {
            let shape = rectangle(west, south, east, north);
            let keys = shape_keys(&shape, &mut GeometryWork::default())?;
            assert!(!keys.is_empty() && keys.len() <= MAX_SHAPE_CELLS);
            assert!(keys
                .iter()
                .all(|key| CellId::faces().iter().all(|face| *key != crate::spatial_key::key(SHAPE_CLASS, *face))));
            assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(keys, shape_keys(&shape, &mut GeometryWork::default())?);
            for x in [west, east] {
                for y in [south, (south + north) / 2.0, north] {
                    let ranges = candidate_ranges(
                        &point([x, y])?,
                        CoverPredicate::Touches,
                        &mut GeometryWork::default(),
                    )?;
                    assert!(found(&keys, &ranges), "{x} {y}");
                }
            }
            let far = candidate_ranges(
                &point([140.0, -30.0])?,
                CoverPredicate::Touches,
                &mut GeometryWork::default(),
            )?;
            assert!(!found(&keys, &far));
        }
        Ok(())
    }

    #[test]
    fn multipart_cover_and_unknown_accuracy_keep_their_meaning() -> Result<(), GeometryRefusal> {
        let shape = Geometry::new(
            Kind::MultiPoint,
            Coords::MultiPoint(vec![
                Some([179.99, 2.0]),
                Some([-179.99, 2.0]),
                Some([2.35, 48.85]),
            ]),
            geometry::GeometrySpace::wgs84_xy(),
        );
        let keys = shape_keys(&shape, &mut GeometryWork::default())?;
        for p in [[179.99, 2.0], [-179.99, 2.0], [2.35, 48.85]] {
            let ranges = candidate_ranges(
                &point(p)?,
                CoverPredicate::Touches,
                &mut GeometryWork::default(),
            )?;
            assert!(found(&keys, &ranges));
        }
        let unknown = unknown_range();
        for radius in [0.0, 150_000.0, 100_000_000.0, f64::MAX] {
            let key = point_key(&[0.0, 0.0], Some(radius)).unwrap();
            assert!(key < unknown.start || key > unknown.end);
        }
        assert!(point_key(&[0.0, 0.0], None).unwrap() >= unknown.start);
        assert!(point_key(&[0.0, 0.0], None).unwrap() <= unknown.end);
        Ok(())
    }

    #[test]
    fn nearest_cell_bounds_never_exceed_contained_point_distances() {
        for origin in [
            [0.0, 0.0],
            [179.99, 89.99],
            [-179.99, -89.99],
            [2.35, 48.85],
        ] {
            for target in [[0.0, 0.0], [10.0, 10.0], [-179.0, 0.0], [0.0, -90.0], [2.36, 48.85]] {
                let distance = geometry::predicates::point_distance(origin, target);
                let leaf = cells::leaf_of(target);
                for root in SearchCell::class_roots(0) {
                    if !root.cell.contains(leaf) {
                        continue;
                    }
                    let mut cell = root;
                    loop {
                        assert!(cell.distance_lower_bound(origin) <= distance + 1e-6);
                        let Some(next) = cell
                            .children()
                            .into_iter()
                            .find(|child| child.cell.contains(leaf))
                        else {
                            break;
                        };
                        cell = next;
                    }
                    assert_eq!(cell.level(), CELL_LEVELS);
                }
            }
        }
        assert_eq!(SearchCell::roots().len(), 6 * (UNKNOWN_CLASS as usize + 1));
    }

    #[test]
    fn nearest_shape_cover_bounds_never_exceed_exact_distances() -> Result<(), GeometryRefusal> {
        let space = geometry::GeometrySpace::wgs84_xy();
        let line = Geometry::new(
            Kind::LineString,
            Coords::Points(vec![[2.0, 48.0], [3.0, 49.5], [4.5, 49.0]]),
            space.clone(),
        );
        let far = Geometry::new(
            Kind::LineString,
            Coords::Points(vec![[100.0, -60.0], [140.0, -50.0]]),
            space.clone(),
        );
        // A line across the antimeridian keys cells on both sides of it.
        let seam = Geometry::new(
            Kind::LineString,
            Coords::Points(vec![[179.5, -1.0], [-179.5, 1.0]]),
            space.clone(),
        );
        for shape in [rectangle(-0.01, 51.49, 0.01, 51.51), line, far, seam] {
            let cells: Vec<CellId> = shape_keys(&shape, &mut GeometryWork::default())?
                .into_iter()
                .map(|key| {
                    CellId::from_raw(key as u64 & ((1u64 << cells::ID_BITS) - 1))
                        .expect("shape key cell")
                })
                .collect();
            for origin in [
                [0.0, 51.5],
                [0.0, 0.0],
                [3.0, 49.0],
                [179.9, 0.0],
                [-179.9, 0.5],
                [100.0, -60.0],
            ] {
                let exact = geometry::predicates::distance(
                    &Geometry::new(Kind::Point, Coords::Point(origin), space.clone()),
                    &shape,
                    &mut GeometryWork::default(),
                )?;
                let bound = cells
                    .iter()
                    .map(|cell| {
                        SearchCell {
                            class: SHAPE_CLASS,
                            cell: *cell,
                        }
                        .distance_lower_bound(origin)
                    })
                    .fold(f64::INFINITY, f64::min);
                assert!(bound <= exact + 1e-6, "{origin:?}: {bound} > {exact}");
                // Every ancestor of a cover cell is a frontier cell that holds
                // it; its bound may only be smaller.
                for cell in &cells {
                    for level in 0..cell.level() {
                        let ancestor = SearchCell {
                            class: SHAPE_CLASS,
                            cell: cell.parent(level),
                        };
                        assert!(ancestor.range().start <= key(SHAPE_CLASS, *cell));
                        assert!(key(SHAPE_CLASS, *cell) <= ancestor.range().end);
                        assert_ne!(ancestor.own_range().start, key(SHAPE_CLASS, *cell));
                    }
                }
            }
        }
        assert!(SearchCell::shape_roots().iter().all(|root| root.is_shape()));
        assert!(SearchCell::roots().iter().all(|root| !root.is_shape()));
        Ok(())
    }
}

/// A persistent series cover, exact event-time anchors and caller-owned
/// population ordinal. Discovery loads no event body.
pub struct CoveredMotion {
    pub from: i64,
    pub to: i64,
    pub group: usize,
    pub cell: i64,
}

/// Capture a conservative set of source pairs from maintained covers. The
/// caller adds dirty members and retains the complete relation alongside the
/// admission ceilings, before any later worker can see a newer index. None
/// means the source-pair allowance was exceeded; no prefix is an answer.
pub fn cover_pairs(
    motions: &[CoveredMotion],
    metric: &Metric,
    maximum: usize,
    work: &mut GeometryWork,
) -> Result<Option<Vec<(usize, usize)>>, GeometryRefusal> {
    metric.validate()?;
    // Persistent cells retain individual fixes/segments for corrections and
    // erasure. Discovery needs only each source cell's exact time union. A
    // spatial leaf containing repeated identical cells must not re-enumerate
    // every observation against every other observation.
    work.charge(
        motions
            .len()
            .saturating_mul(motions.len().max(2).ilog2() as usize + 1),
    )?;
    work.allocate(motions.len().saturating_mul(
        std::mem::size_of::<&CoveredMotion>() + std::mem::size_of::<CoveredMotion>(),
    ))?;
    let mut ordered: Vec<_> = motions.iter().collect();
    ordered.sort_unstable_by_key(|motion| (motion.group, motion.cell, motion.from, motion.to));
    let mut compact: Vec<CoveredMotion> = Vec::with_capacity(motions.len());
    for motion in ordered {
        work.charge(1)?;
        if motion.from > motion.to {
            return Err(geometry::invalid("Trajectory time anchors must be ordered"));
        }
        if let Some(previous) = compact.last_mut().filter(|previous| {
            previous.group == motion.group
                && previous.cell == motion.cell
                && motion.from <= previous.to
        }) {
            previous.to = previous.to.max(motion.to);
        } else {
            compact.push(CoveredMotion {
                from: motion.from,
                to: motion.to,
                group: motion.group,
                cell: motion.cell,
            });
        }
    }
    let motions = compact.as_slice();
    work.allocate(motions.len().saturating_mul(128))?;
    let possible = motions
        .len()
        .saturating_mul(motions.len().saturating_sub(1))
        / 2;
    work.allocate(possible.min(maximum.saturating_add(1)).saturating_mul(64))?;
    let mut boxes = Vec::with_capacity(motions.len());
    let mut times = Vec::with_capacity(motions.len());
    for motion in motions {
        work.charge(32)?;
        if motion.from > motion.to {
            return Err(geometry::invalid("Trajectory time anchors must be ordered"));
        }
        let bounds = crate::spatial_key::trajectory_cell_bounds(motion.cell, work)?;
        boxes.push([bounds.west, bounds.south, bounds.east, bounds.north]);
        times.push([motion.from, motion.to]);
    }
    let tree = geometry::spatial_tree::PackedBoxes::new(&boxes, work)?;
    let hulls = tree.aggregates(&times, |a, b| [a[0].min(b[0]), a[1].max(b[1])], work)?;
    let mut selected = std::collections::BTreeSet::new();
    for (index, motion) in motions.iter().enumerate() {
        work.charge(8)?;
        let (longitude_padding, latitude_padding) = metric.padding(boxes[index]);
        for bounds in padded_boxes(boxes[index], longitude_padding, latitude_padding) {
            let exceeded = tree.search_filtered(
                &bounds,
                &mut |node| hulls[node][0] <= motion.to && motion.from <= hulls[node][1],
                &mut |other| {
                    let other = &motions[other];
                    if motion.group < other.group
                        && motion.from <= other.to
                        && other.from <= motion.to
                    {
                        selected.insert((motion.group, other.group));
                    }
                    selected.len() > maximum
                },
                work,
            )?;
            if exceeded {
                return Ok(None);
            }
        }
    }
    work.allocate(
        selected
            .len()
            .saturating_mul(2 * std::mem::size_of::<usize>()),
    )?;
    Ok(Some(selected.into_iter().collect()))
}

#[cfg(test)]
mod encounter_tests {
    use geometry::encounters::{
        candidates,
        intervals,
        Motion,
    };

    use super::*;
    #[test]
    fn spherical_candidates_cover_poles_and_prune_distant_longitudes() -> anyhow::Result<()> {
        for (points, radius) in [
            ([[0.0, 89.999], [180.0, 89.999], [0.0, 0.0]], 300.0),
            ([[10.0, 12.0], [10.0001, 12.0], [80.0, 12.0]], 100.0),
        ] {
            let metric = Metric::LinearGeodesic { within_m: radius };
            let motions = points
                .iter()
                .enumerate()
                .map(|(group, point)| Motion {
                    from: 0,
                    to: 10,
                    group,
                    points: [*point; 2],
                })
                .collect::<Vec<_>>();
            assert_eq!(
                candidates(&motions, &metric, 10, &mut GeometryWork::default())?,
                vec![(0, 1)]
            );
            let mut cells = Vec::new();
            for (group, point) in points.iter().enumerate() {
                for cell in crate::spatial_key::trajectory_keys(
                    *point,
                    *point,
                    &mut GeometryWork::default(),
                )? {
                    cells.push(CoveredMotion {
                        from: 0,
                        to: 10,
                        group,
                        cell,
                    });
                }
            }
            assert_eq!(
                cover_pairs(&cells, &metric, 10, &mut GeometryWork::default())?,
                Some(vec![(0, 1)])
            );
            assert!(!metric
                .intervals([points[0]; 2], [points[1]; 2], &mut GeometryWork::default())?
                .is_empty());
        }
        assert!(serde_json::from_value::<Metric>(serde_json::json!({
            "model":"wgs84-linear-geodesic-v1", "within_m":1, "reference_latitude":45,
        }))
        .is_err());
        Ok(())
    }

    #[test]
    fn persistent_covers_preserve_contacts_seams_and_exact_time_anchors() -> anyhow::Result<()> {
        let epoch = 9_007_199_254_740_993i64;
        let paths = [
            [[179.0, 0.0], [-179.0, 0.0]],
            [[-180.0, 0.0]; 2],
            [[180.0, 0.0]; 2],
            [[-1.0, 0.0], [1.0, 0.0]],
            [[0.0, 0.0]; 2],
            [[0.0, 1.0]; 2],
            [[0.0, 90.0]; 2],
            [[180.0, 90.0]; 2],
        ];
        let mut covers = Vec::new();
        for (group, points) in paths.iter().enumerate() {
            for cell in crate::spatial_key::trajectory_keys(
                points[0],
                points[1],
                &mut GeometryWork::default(),
            )? {
                covers.push(CoveredMotion {
                    from: epoch,
                    to: epoch + 20,
                    group,
                    cell,
                });
            }
        }
        // A disjoint exact instant must not collide after binary64 conversion.
        covers.push(CoveredMotion {
            from: epoch + 21,
            to: epoch + 21,
            group: paths.len(),
            cell: covers[0].cell,
        });
        for latitude in [-90.0, 0.0, 45.0, 90.0] {
            for radius in [0.0, 1.0, 100_000_000.0] {
                let selected: std::collections::BTreeSet<_> = cover_pairs(
                    &covers,
                    &Metric::FixedLatitudeEquirectangular {
                        reference_latitude: latitude,
                        within_m: radius,
                    },
                    100,
                    &mut GeometryWork::default(),
                )?
                .expect("bounded complete relation")
                .into_iter()
                .collect();
                assert!(selected.iter().all(|&(_, right)| right < paths.len()));
                for left in 0..paths.len() {
                    for right in left + 1..paths.len() {
                        if !intervals(
                            paths[left],
                            paths[right],
                            latitude,
                            radius,
                            &mut GeometryWork::default(),
                        )?
                        .is_empty()
                        {
                            assert!(
                                selected.contains(&(left, right)),
                                "missing contact {left}/{right} at {latitude}/{radius}"
                            );
                        }
                    }
                }
            }
        }
        assert!(crate::spatial_key::trajectory_cell_bounds(-1, &mut GeometryWork::default()).is_err());
        assert!(crate::spatial_key::trajectory_cell_bounds(0, &mut GeometryWork::default()).is_err());
        assert!(crate::spatial_key::trajectory_cell_bounds(
            crate::spatial_index::point_key(&[0.0, 0.0], Some(0.0))?,
            &mut GeometryWork::default()
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn persistent_discovery_prunes_large_sparse_fleets_and_refuses_dense_prefixes(
    ) -> anyhow::Result<()> {
        let mut covers = Vec::new();
        for group in 0..4096 {
            let point = [
                -170.0 + (group % 64) as f64 * 5.0,
                -70.0 + (group / 64) as f64 * 2.0,
            ];
            for cell in
                crate::spatial_key::trajectory_keys(point, point, &mut GeometryWork::default())?
            {
                covers.push(CoveredMotion {
                    from: 0,
                    to: 0,
                    group,
                    cell,
                });
            }
        }
        assert!(cover_pairs(
            &covers,
            &Metric::FixedLatitudeEquirectangular {
                reference_latitude: 0.0,
                within_m: 1.0
            },
            1,
            &mut GeometryWork::default()
        )?
        .expect("complete sparse relation")
        .is_empty());
        let cell = crate::spatial_key::trajectory_keys(
            [0.0, 0.0],
            [0.0, 0.0],
            &mut GeometryWork::default(),
        )?[0];
        let dense: Vec<_> = (0..3)
            .map(|group| CoveredMotion {
                from: 0,
                to: 1,
                group,
                cell,
            })
            .collect();
        assert!(cover_pairs(
            &dense,
            &Metric::FixedLatitudeEquirectangular {
                reference_latitude: 0.0,
                within_m: 0.0
            },
            1,
            &mut GeometryWork::default()
        )?
        .is_none());
        Ok(())
    }

    #[test]
    fn source_cover_time_unions_bound_repeated_cells_and_preserve_gaps(
    ) -> Result<(), GeometryRefusal> {
        let point = [10.123, 12.345];
        let cells =
            crate::spatial_key::trajectory_keys(point, point, &mut GeometryWork::default())?;
        let cell = cells[0];
        let mut motions = Vec::new();
        for start in [0, 6000] {
            for time in start..start + 3000 {
                motions.push(CoveredMotion {
                    from: time,
                    to: time + 1,
                    group: 0,
                    cell,
                });
                motions.push(CoveredMotion {
                    from: time,
                    to: time,
                    group: 0,
                    cell,
                });
            }
        }
        for time in 4000..5000 {
            motions.push(CoveredMotion {
                from: time,
                to: time + 1,
                group: 1,
                cell,
            });
        }
        motions.push(CoveredMotion {
            from: 1500,
            to: 1600,
            group: 2,
            cell,
        });
        for metric in [
            Metric::LinearGeodesic { within_m: 0. },
            Metric::FixedLatitudeEquirectangular {
                reference_latitude: 12.,
                within_m: 0.,
            },
        ] {
            assert_eq!(
                cover_pairs(&motions, &metric, 10, &mut GeometryWork::default())?,
                Some(vec![(0, 2)])
            );
        }
        // Discrete adjacent fixes have a continuous-time gap between them.
        let instants = [
            CoveredMotion {
                from: 0,
                to: 0,
                group: 0,
                cell,
            },
            CoveredMotion {
                from: 2,
                to: 2,
                group: 0,
                cell,
            },
            CoveredMotion {
                from: 1,
                to: 1,
                group: 1,
                cell,
            },
        ];
        assert_eq!(
            cover_pairs(
                &instants,
                &Metric::LinearGeodesic { within_m: 0. },
                10,
                &mut GeometryWork::default()
            )?,
            Some(vec![])
        );
        Ok(())
    }
}

#[cfg(test)]
mod influence_tests {
    use super::*;

    #[test]
    fn nearest_influence_covers_threshold_ties_seams_poles_and_underfill() -> anyhow::Result<()> {
        for origin in [
            [0., 0.],
            [179.999, 1.],
            [-179.999, -1.],
            [0., 90.],
            [180., -90.],
        ] {
            for radius in [0., 1., 1000., MAX_POINT_DISTANCE_METRES] {
                let keys = nearest_influence_keys(origin, radius, &mut GeometryWork::default())?;
                for point in [
                    origin,
                    [-180., origin[1]],
                    [180., origin[1]],
                    [0., 90.],
                    [0., -90.],
                    [0.000001, 0.],
                ] {
                    if geometry::predicates::point_distance(origin, point) <= radius {
                        let probes = point_influence_keys(point);
                        assert!(
                            keys.iter().any(|key| probes.contains(key)),
                            "missing {origin:?} / {point:?} / {radius}"
                        );
                    }
                }
            }
        }
        let small = nearest_influence_keys([0., 0.], 1000., &mut GeometryWork::default())?;
        assert!(!point_influence_keys([90., 0.])
            .iter()
            .any(|key| small.contains(key)));
        Ok(())
    }
}
