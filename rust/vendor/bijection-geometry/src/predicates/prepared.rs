//! Immutable prepared geometry: exact topology decides membership; packed
//! bounds select candidates for metric refinement and conservative covers.
use std::ops::Deref;

use crate::{
    arcs,
    cells::{
        Cell,
        CellRelation,
        CellTarget,
    },
    geodesy::SEMI_MINOR_AXIS_METRES,
    spatial_tree::{
        PackedBoxes,
        PackedEdges,
    },
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Pos,
    Rel,
};

/// Immutable derived representations. Choosing metric preparation does not
/// change the distance contract; area membership remains exact when needed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Preparation {
    Metric,
    Topology,
}

pub struct PreparedGeometry {
    geometry: Geometry,
    /// Metric-only edges; prepared topology owns the same edges otherwise.
    index: Option<PackedEdges>,
    topology: Option<crate::topology::relation::PreparedTopology>,
    retained_bytes: usize,
    reference_segments: Option<Vec<(Pos, Pos)>>,
}

impl Deref for PreparedGeometry {
    type Target = Geometry;

    fn deref(&self) -> &Geometry {
        &self.geometry
    }
}

impl PreparedGeometry {
    pub fn new(
        geometry: Geometry,
        preparation: Preparation,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        Self::try_new(geometry, preparation, work).map_err(|(_, error)| error)
    }

    /// Returns the original geometry if its derived representation cannot be built.
    pub fn try_new(
        geometry: Geometry,
        preparation: Preparation,
        work: &mut GeometryWork,
    ) -> Result<Self, (Geometry, GeometryRefusal)> {
        let before = work.allocated_bytes;
        let (topology, index) = match Self::derive(&geometry, preparation, work) {
            Ok(derived) => derived,
            Err(error) => return Err((geometry, error)),
        };
        let retained_bytes = work.allocated_bytes - before
            + geometry.retained_bytes()
            + geometry
                .position_count()
                .saturating_mul(64)
                .saturating_add(geometry.space.retained_bytes())
                .saturating_add(std::mem::size_of::<Self>());
        Ok(Self {
            geometry,
            index,
            topology,
            retained_bytes,
            reference_segments: None,
        })
    }

    fn derive(
        geometry: &Geometry,
        preparation: Preparation,
        work: &mut GeometryWork,
    ) -> Result<(Option<crate::topology::relation::PreparedTopology>, Option<PackedEdges>), GeometryRefusal> {
        geometry.space.validate_kernel()?;
        let topology = if geometry.space.is_ground()
            && geometry.heights.is_none()
            && (preparation == Preparation::Topology || super::is_area(geometry))
        {
            Some(crate::topology::relation::PreparedTopology::new(
                geometry, work,
            )?)
        } else {
            None
        };
        let index = match topology {
            Some(_) => None,
            None => Some(PackedEdges::new(
                super::segments(geometry),
                &geometry.space.frame,
                work,
            )?),
        };
        Ok((topology, index))
    }

    pub fn into_geometry(self) -> Geometry {
        self.geometry
    }

    /// Conservative accounting includes the admitted geometry and topology.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn packed(&self) -> &PackedEdges {
        match (&self.topology, &self.index) {
            (Some(topology), _) => topology.packed(),
            (None, Some(index)) => index,
            (None, None) => unreachable!("prepared geometry has packed edges"),
        }
    }

    fn edges(&self) -> &[(Pos, Pos)] {
        &self.packed().edges
    }

    fn edge_tree(&self) -> &PackedBoxes {
        &self.packed().tree
    }

    pub(crate) fn point(
        &self,
        point: Pos,
        work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        self.topology
            .as_ref()
            .ok_or_else(|| {
                crate::unsupported_space("Point membership requires explicit ground projection")
            })?
            .relation(point, work)
    }

    pub(crate) fn trajectory_intervals(
        &self,
        a: Pos,
        b: Pos,
        work: &mut GeometryWork,
    ) -> Result<Vec<crate::trajectory::Interval>, GeometryRefusal> {
        self.topology
            .as_ref()
            .ok_or_else(|| {
                crate::unsupported_space(
                    "Trajectory membership requires explicit ground projection",
                )
            })?
            .trajectory_intervals(a, b, work)
    }

    /// Conservative closed rectangle contact: a corner in the geometry, or
    /// an edge whose arc box meets the rectangle. Candidate reservations are
    /// scoped by the covering caller.
    pub fn intersects_box(
        &self,
        bounds: [f64; 4],
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let [west, south, east, north] = bounds;
        let corners = [[west, south], [east, south], [east, north], [west, north]];
        for corner in corners {
            if self.point(corner, work)? != Rel::Outside {
                return Ok(true);
            }
        }
        self.topology
            .as_ref()
            .ok_or_else(|| {
                crate::unsupported_space("Box contact requires explicit ground projection")
            })?
            .meets_box(bounds, work)
    }

    /// How a cell stands to this geometry, by exact tests against the
    /// padded cell: an edge or a point of the geometry in it means it meets;
    /// otherwise each area holds the whole cell or none of it, which one
    /// corner settles.
    pub fn relate_cell(
        &self,
        cell: &Cell,
        work: &mut GeometryWork,
    ) -> Result<CellRelation, GeometryRefusal> {
        for (p, q) in self.packed().in_box(cell.bounds(), work)? {
            work.charge(8)?;
            if cell.meets_arc(arcs::direction(p), arcs::direction(q), work)? {
                return Ok(CellRelation::Meets);
            }
        }
        if self.topology.is_some() && super::is_area(&self.geometry) {
            let corner = arcs::position(cell.corners()[0]);
            if self.point(corner, work)? == Rel::Inside {
                return Ok(CellRelation::Inside);
            }
        }
        Ok(CellRelation::Misses)
    }

    /// A lower bound on the auxiliary angle from a cell the geometry misses
    /// to the geometry, or None when it is past `limit` radians.
    fn angle_to_cell(
        &self,
        cell: &Cell,
        limit: f64,
        work: &mut GeometryWork,
    ) -> Result<Option<f64>, GeometryRefusal> {
        let mut best: Option<f64> = None;
        for (p, q) in self.packed().in_box(widened(cell.bounds(), limit), work)? {
            work.charge(8)?;
            let angle = cell.angle_to_arc(arcs::direction(p), arcs::direction(q));
            if angle * (1.0 - 1e-12) <= limit && best.is_none_or(|best| angle < best) {
                best = Some(angle);
            }
        }
        Ok(best)
    }

    pub fn topology(
        &self,
        other: &Self,
        predicate: super::TopologyPredicate,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let unsupported =
            || crate::unsupported_space("Topology requires explicit ground projection");
        self.topology.as_ref().ok_or_else(unsupported)?.predicate(
            &self.geometry,
            &other.geometry,
            other.topology.as_ref().ok_or_else(unsupported)?,
            predicate,
            work,
        )
    }

    pub fn prepare_reference_points(&mut self) {
        if let Some(topology) = &mut self.topology {
            self.retained_bytes += topology.retain_points(&self.geometry);
        }
    }

    pub fn reference_points_bound(&self) -> usize {
        self.topology.as_ref().map_or(0, |topology| topology.reference_points_bound(&self.geometry))
    }

    pub fn reference_segments_bound(&self) -> usize {
        self.geometry.position_count().saturating_mul(128)
            .saturating_add(self.geometry.ring_count().saturating_mul(32))
            .saturating_add(self.geometry.component_count().saturating_mul(32))
            .saturating_add(256)
    }

    /// Retains the reference traversal order for exact distance and classification.
    pub fn prepare_reference_segments(&mut self) {
        if self.reference_segments.is_none() {
            let segments = super::segments(&self.geometry);
            self.retained_bytes += segments.capacity() * std::mem::size_of::<(Pos, Pos)>();
            self.reference_segments = Some(segments);
        }
    }

    pub fn topology_from(
        &self, left: &Geometry, predicate: super::TopologyPredicate, work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        match &self.topology {
            Some(right) => right.evaluate_from(left, &self.geometry, predicate, work),
            None => super::topology(left, &self.geometry, predicate, work),
        }
    }

    pub fn exact_distance_from(&self, left: &Geometry, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
        let Some(right) = self.reference_segments.as_deref() else {
            return super::distance(left, &self.geometry, work);
        };
        if matches!((&left.coords, &self.coords), (crate::Coords::Point(_), crate::Coords::Point(_))) {
            work.charge(1)?;
            return super::geometry_point_distance(left, &self.geometry);
        }
        if self.topology_from(left, super::TopologyPredicate::Intersects, work)? {
            return Ok(0.0);
        }
        super::reference_segment_distance(left, right, work)
    }

    pub fn classify_exact(
        &self, point: Pos, accuracy_m: Option<f64>, work: &mut GeometryWork,
    ) -> Result<&'static str, GeometryRefusal> {
        super::classify_with(point, accuracy_m, &self.geometry, self.topology.as_ref(),
            self.reference_segments.as_deref(), work)
    }

    pub fn intersects(
        &self,
        other: &Self,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        self.topology(other, super::TopologyPredicate::Intersects, work)
    }

    pub fn covered_by(
        &self,
        other: &Self,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        self.topology(other, super::TopologyPredicate::CoveredBy, work)
    }

    pub fn distance(&self, other: &Self, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
        if self.space != other.space {
            return Err(crate::invalid(
                "Spatial operands require the same coordinate space",
            ));
        }
        if matches!(
            (&self.coords, &other.coords),
            (crate::Coords::Point(_), crate::Coords::Point(_))
        ) {
            work.charge(1)?;
            return super::geometry_point_distance(self, other);
        }
        if !self.space.is_ground() || self.heights.is_some() || other.heights.is_some() {
            return Err(crate::unsupported_space(
                "Non-point distance requires explicit ground projection",
            ));
        }
        // Boundary distance covers every connected linear component. Only
        // area containment can make the distance zero without boundary contact.
        // Test one support point per path; boundary crossings are decided during
        // refinement, after the packed trees have excluded irrelevant pairs.
        for (support, area) in [(self, other), (other, self)] {
            if super::is_area(area) {
                for path in super::paths(support) {
                    if let Some(point) = path.first()
                        && area.point(*point, work)? != Rel::Outside
                    {
                        return Ok(0.0);
                    }
                }
            }
        }
        let left = self.edges();
        let right = other.edges();
        let (Some(a), Some(b)) = (left.first(), right.first()) else {
            return Err(crate::invalid("Empty geometry has no distance"));
        };
        let best = super::space_point_distance(&self.space, a.0, b.0)?;
        self.edge_tree().closest_pairs(
            other.edge_tree(),
            best,
            |left, right| super::distance_lower_bound(&self.space, left, right),
            |best| best - super::distance_slack(best),
            |a, b, best, work| {
                super::space_segment_distance(&self.space, left[a], right[b], best, work)
            },
            work,
        )
    }

    pub fn classify(
        &self,
        point: Pos,
        accuracy_m: Option<f64>,
        work: &mut GeometryWork,
    ) -> Result<&'static str, GeometryRefusal> {
        if !self.space.is_ground() {
            return Err(crate::invalid(
                "Classification requires explicit ground projection",
            ));
        }
        if self.kind == crate::Kind::GeometryCollection {
            return super::classify(point, accuracy_m, &self.geometry, work);
        }
        let Some(accuracy) = accuracy_m else {
            return Ok("unknown");
        };
        if !accuracy.is_finite() || accuracy < 0.0 {
            return Err(crate::invalid(
                "Accuracy must be finite non-negative metres",
            ));
        }
        let relation = self.point(point, work)?;
        if accuracy > 0.0 {
            let mut boundary = f64::INFINITY;
            for &edge in self.edges() {
                boundary = super::space_segment_distance(
                    &self.space,
                    (point, point),
                    edge,
                    boundary,
                    work,
                )?;
            }
            if super::within_slack(boundary, accuracy) {
                return Ok("uncertain");
            }
        }
        Ok(if relation == Rel::Outside {
            "outside"
        } else {
            "inside"
        })
    }
}

/// A longitude/latitude box widened by an auxiliary angle, for candidate
/// searches only: latitude by the angle with a margin for the step from
/// reduced to geodetic latitude, longitude by the widest the angle spans at
/// the box's poleward edge; past a pole every longitude.
fn widened(bounds: [f64; 4], angle: f64) -> [f64; 4] {
    let pad = angle.to_degrees() * 1.01 + 1e-9;
    let (south, north) = (bounds[1] - pad, bounds[3] + pad);
    if south <= -90.0 || north >= 90.0 {
        return [-180.0, south.max(-90.0), 180.0, north.min(90.0)];
    }
    let edge = south.abs().max(north.abs()).to_radians();
    let sine = libm::sin(angle) / libm::cos(edge);
    let spread = if sine >= 1.0 {
        180.0
    } else {
        libm::asin(sine).to_degrees() * 1.01 + 1e-9
    };
    if spread >= 180.0 || bounds[2] - bounds[0] + 2.0 * spread >= 360.0 {
        return [-180.0, south, 180.0, north];
    }
    [bounds[0] - spread, south, bounds[2] + spread, north]
}

/// The cells within `reach_m` metres of a prepared geometry. A radian of
/// auxiliary angle spans at least b metres, so a cell more than reach / b
/// radians away holds nothing within the reach.
pub struct Near<'a> {
    pub geometry: &'a PreparedGeometry,
    pub reach_m: f64,
}

impl CellTarget for Near<'_> {
    fn relate(&self, cell: &Cell, work: &mut GeometryWork) -> Result<CellRelation, GeometryRefusal> {
        let relation = self.geometry.relate_cell(cell, work)?;
        if relation != CellRelation::Misses || self.reach_m <= 0.0 {
            return Ok(relation);
        }
        let limit = self.reach_m / SEMI_MINOR_AXIS_METRES;
        if limit >= std::f64::consts::FRAC_PI_4 {
            return Ok(CellRelation::Meets);
        }
        Ok(match self.geometry.angle_to_cell(cell, limit, work)? {
            Some(_) => CellRelation::Meets,
            None => CellRelation::Misses,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::PreparedGeometry;
    use crate::{
        Coords,
        Geometry,
        GeometryDeclaration,
        GeometryRefusal,
        GeometrySpace,
        GeometryWork,
        Kind,
        Size,
    };

    #[test]
    fn spatial_arrow_one_sided_preparation_preserves_distance_bits_and_work() {
        use super::Preparation;
        let space = GeometrySpace::wgs84_xy();
        let line = |latitude| Geometry::new(
            Kind::LineString, Coords::Points(vec![[0.0, latitude], [2.0, latitude]]), space.clone(),
        );
        let right = line(2.0);
        let mut prepared = PreparedGeometry::new(right.clone(), Preparation::Topology, &mut GeometryWork::default()).unwrap();
        prepared.prepare_reference_segments();
        for left in [line(0.0), line(2.0), Geometry::new(Kind::Point, Coords::Point([1.0, 1.0]), space)] {
            for steps in [100, 100_000] {
                let mut reference = GeometryWork::with_limits(steps, 8 << 20);
                let mut cached = GeometryWork::with_limits(steps, 8 << 20);
                assert_eq!(
                    crate::predicates::distance(&left, &right, &mut reference).map(f64::to_bits),
                    prepared.exact_distance_from(&left, &mut cached).map(f64::to_bits),
                );
                assert_eq!(cached.usage().steps, reference.usage().steps);
                assert_eq!(cached.usage().peak_allocated_bytes, reference.usage().peak_allocated_bytes);
            }
        }
    }

    #[test]
    fn metric_preparation_preserves_pruning_and_exact_contact() -> anyhow::Result<()> {
        use super::Preparation;
        let space = GeometrySpace::wgs84_xy();
        let line = |latitude| {
            Geometry::new(
                Kind::LineString,
                Coords::Points((0..128).map(|i| [i as f64 / 1000., latitude]).collect()),
                space.clone(),
            )
        };
        let mut work = GeometryWork::with_limits(12000, 1 << 20);
        let left = PreparedGeometry::new(line(0.), Preparation::Metric, &mut work)?;
        let right = PreparedGeometry::new(line(0.01), Preparation::Metric, &mut work)?;
        let distance = left.distance(&right, &mut work)?;
        // The parallels are a meridian arc apart along their whole length.
        let expected = crate::geodesy::inverse(0.0, 0.0, 0.01, 0.0).s12;
        assert!((distance - expected).abs() <= crate::predicates::distance_slack(expected));
        assert!(work.usage().steps < 12000);
        let geometry = |coordinates| {
            Geometry::new(Kind::LineString, Coords::Points(coordinates), space.clone())
        };
        for (a, b, expected_zero) in [
            (vec![[0., 0.], [1., 1.]], vec![[0., 1.], [1., 0.]], true),
            (
                vec![[0., 0.], [1., 0.]],
                vec![[0., 1e-12], [1., 1e-12]],
                false,
            ),
            (
                vec![[180., -1.], [180., 1.]],
                vec![[-180., -1.], [-180., 1.]],
                true,
            ),
            (
                vec![[0., 90.], [0., 89.]],
                vec![[90., 90.], [90., 89.]],
                true,
            ),
        ] {
            let mut work = GeometryWork::default();
            let left = PreparedGeometry::new(geometry(a), Preparation::Metric, &mut work)?;
            let right = PreparedGeometry::new(geometry(b), Preparation::Metric, &mut work)?;
            assert_eq!(left.distance(&right, &mut work)? == 0., expected_zero);
        }
        Ok(())
    }

    #[test]
    fn point_membership_and_trajectory_refinement_share_exact_topology() -> anyhow::Result<()> {
        let geographic = GeometrySpace::wgs84_xy();
        let mut cartesian = serde_json::to_value(&geographic)?;
        cartesian["segments"] = serde_json::json!("coordinate_linear");
        cartesian["frame"] = json!({"kind":"cartesian","name":"point-membership-plane","unit":{"metres_per_unit":1.0}});
        for space in [geographic, serde_json::from_value(cartesian)?] {
            let area = Geometry::new(
                Kind::Polygon,
                Coords::Lines(vec![vec![[0., 0.], [1., 0.], [1., 1.], [0., 1.], [0., 0.]]]),
                space.clone(),
            );
            let mut preparation = GeometryWork::default();
            let prepared = PreparedGeometry::new(
                area.clone(),
                crate::predicates::prepared::Preparation::Topology,
                &mut preparation,
            )?;
            // WGS84 directions separate longitudes down to about 1e-300 degrees.
            let tiny = if space.frame == crate::CoordinateFrame::Wgs84 {
                1e-300
            } else {
                f64::from_bits(1)
            };
            for x in [-1e-12, -tiny, 0., tiny, 1e-12] {
                let point = [x, 0.5];
                let mut work = GeometryWork::default();
                let expected = if x < 0. { "outside" } else { "inside" };
                assert_eq!(prepared.classify(point, Some(0.), &mut work)?, expected);
                assert_eq!(
                    super::super::classify(point, Some(0.), &area, &mut work)?,
                    expected
                );
                let geometry = Geometry::new(Kind::Point, Coords::Point(point), space.clone());
                assert_eq!(
                    super::super::intersects(&geometry, &area, &mut work)?,
                    x >= 0.
                );
                for (start, end) in [(point, point), ([x, 0.25], [x, 0.75])] {
                    let intervals = crate::trajectory::intervals(&prepared, start, end, &mut work)?;
                    assert_eq!(intervals.is_empty(), x < 0.);
                    if x >= 0. {
                        assert_eq!((intervals[0].from, intervals[0].to), (0., 1.));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn prepared_topology_matches_reference_for_holes_parts_and_boundaries(
    ) -> Result<(), GeometryRefusal> {
        let fixtures = [
            json!({"type":"Point","coordinates":[0,0]}),
            json!({"type":"Point","coordinates":[1,0]}),
            json!({"type":"MultiPoint","coordinates":[[0,0],[2,2]]}),
            json!({"type":"LineString","coordinates":[[-4,0],[4,0]]}),
            json!({"type":"LineString","coordinates":[[-0.5,0],[0.5,0]]}),
            json!({"type":"MultiLineString","coordinates":[[[-3,-3],[-3,3]],[[3,-3],[3,3]]]}),
            json!({"type":"Polygon","coordinates":[[[-4,-4],[4,-4],[4,4],[-4,4],[-4,-4]]]}),
            json!({"type":"Polygon","coordinates":[
                [[-3,-3],[3,-3],[3,3],[-3,3],[-3,-3]],
                [[-1,-1],[-1,1],[1,1],[1,-1],[-1,-1]]
            ]}),
            json!({"type":"MultiPolygon","coordinates":[
                [[[-5,-5],[-4,-5],[-4,-4],[-5,-4],[-5,-5]]],
                [[[4,4],[5,4],[5,5],[4,5],[4,4]]]
            ]}),
            json!({"type":"MultiLineString","coordinates":[[[170,0],[180,0]],[[-180,0],[-170,0]]]}),
        ];
        let mut geometries = Vec::new();
        let mut prepared = Vec::new();
        for fixture in fixtures {
            let geometry = crate::admit_value(
                &crate::Gv::from(&fixture),
                &GeometryDeclaration {
                    space: GeometrySpace::wgs84_xy(),
                    size: Size::Standard,
                },
            )?;
            prepared.push(PreparedGeometry::new(
                geometry.clone(),
                crate::predicates::prepared::Preparation::Topology,
                &mut GeometryWork::default(),
            )?);
            geometries.push(geometry);
        }
        for (i, left) in geometries.iter().enumerate() {
            for (j, right) in geometries.iter().enumerate() {
                assert_eq!(
                    prepared[i].intersects(&prepared[j], &mut GeometryWork::default())?,
                    crate::predicates::intersects(left, right, &mut GeometryWork::default())?,
                    "intersects {i}, {j}"
                );
                assert_eq!(
                    prepared[i].covered_by(&prepared[j], &mut GeometryWork::default())?,
                    crate::predicates::covered_by(left, right, &mut GeometryWork::default())?,
                    "covered by {i}, {j}"
                );
            }
            for point in [[0.0, 0.0], [1.0, 0.0], [3.0, 0.0], [10.0, 10.0]] {
                for accuracy in [None, Some(0.0), Some(100.0)] {
                    assert_eq!(
                        prepared[i].classify(point, accuracy, &mut GeometryWork::default()),
                        crate::predicates::classify(
                            point,
                            accuracy,
                            left,
                            &mut GeometryWork::default()
                        ),
                        "classify {i}, {point:?}, {accuracy:?}"
                    );
                }
            }
        }
        for (left, right) in [(0, 1), (0, 7), (4, 7)] {
            let expected = crate::predicates::distance(
                &geometries[left],
                &geometries[right],
                &mut GeometryWork::default(),
            );
            let actual = prepared[left].distance(&prepared[right], &mut GeometryWork::default());
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert!(
                    (actual - expected).abs() <= crate::predicates::distance_slack(expected)
                ),
                (Err(actual), Err(expected)) => assert_eq!(actual, expected),
                (actual, expected) => {
                    panic!("distance {left}, {right}: {actual:?} differs from {expected:?}")
                },
            }
        }
        Ok(())
    }

    #[test]
    fn prepared_disjoint_large_polygons_fit_the_native_work_budget() -> Result<(), GeometryRefusal>
    {
        let polygon = |center: f64| {
            let mut ring: Vec<_> = (0..2048)
                .map(|i| {
                    let angle = i as f64 * std::f64::consts::TAU / 2048.0;
                    [center + libm::cos(angle), libm::sin(angle)]
                })
                .collect();
            ring.push(ring[0]);
            Geometry::new(
                Kind::Polygon,
                Coords::Lines(vec![ring]),
                crate::GeometrySpace::wgs84_xy(),
            )
        };
        let (left, right) = (polygon(0.0), polygon(10.0));
        assert!(!crate::predicates::intersects(
            &left,
            &right,
            &mut GeometryWork::default()
        )?);
        let mut work = GeometryWork::default();
        let left = PreparedGeometry::new(
            left,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        let right = PreparedGeometry::new(
            right,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        assert!(!left.intersects(&right, &mut work)?);
        Ok(())
    }
}
