//! Interior/boundary relations on exact arrangements of arcs. Samples are
//! exact directions inside proven cells, never rounded midpoints.
use num_traits::{
    Signed,
    Zero,
};

use super::*;
use crate::{
    arcs::Exact,
    spatial_tree::PackedEdges,
};

/// Named topology operations share one intersection matrix.
#[derive(Clone, Copy, Debug)]
pub enum Predicate {
    Intersects,
    Equals,
    Disjoint,
    Covers,
    CoveredBy,
    Contains,
    Within,
    Touches,
    Crosses,
    Overlaps,
}

impl Predicate {
    /// Which matrix regions the predicate reads: the operands' shared
    /// closures, the left closure outside the right, and the right outside
    /// the left.
    fn reads(self, left: i8, right: i8) -> (bool, bool, bool) {
        match self {
            Self::Intersects | Self::Disjoint | Self::Touches => (true, false, false),
            Self::Equals => (false, true, true),
            Self::Covers => (false, false, true),
            Self::CoveredBy => (false, true, false),
            Self::Contains => (true, false, true),
            Self::Within => (true, true, false),
            Self::Crosses => (true, left < right, left > right),
            Self::Overlaps => (true, true, true),
        }
    }

    fn transposed(self) -> Self {
        match self {
            Self::Contains => Self::Within,
            Self::Within => Self::Contains,
            Self::Covers => Self::CoveredBy,
            Self::CoveredBy => Self::Covers,
            Self::Intersects
            | Self::Equals
            | Self::Disjoint
            | Self::Touches
            | Self::Crosses
            | Self::Overlaps => self,
        }
    }
}

/// Prepared exact arrangement shared by predicates, classification and
/// trajectory membership. Admitted binary64 edges stay raw in one packed
/// tree for metric refinement and in one tree of arc boxes for topology;
/// only the candidates an operation meets are refined exactly.
pub(crate) struct PreparedTopology {
    frame: CoordinateFrame,
    index: PackedEdges,
    shape: Shape,
    dimension: i8,
    bounds: [f64; 4],
    preparation: TopologyCost,
    points: Option<std::collections::BTreeSet<[u64; 3]>>,
}

struct TopologyCost {
    steps: usize,
    bytes: usize,
    peak: usize,
}

impl TopologyCost {
    fn replay(&self, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
        if work.observer.is_some() || self.steps > work.remaining_steps()
            || self.peak > work.allocation_limit.saturating_sub(work.allocated_bytes)
        {
            return Ok(false);
        }
        work.charge(self.steps)?;
        work.allocate(self.peak)?;
        work.release(self.peak - self.bytes);
        Ok(true)
    }
}

fn dim(geometry: &Geometry) -> i8 {
    if geometry.is_empty() {
        return -1;
    }
    match &geometry.coords {
        Coords::Collection(members) => members.iter().map(dim).max().unwrap_or(-1),
        _ => match geometry.kind {
            Kind::Point | Kind::MultiPoint => 0,
            Kind::LineString | Kind::MultiLineString => {
                // Positions naming one direction (a pole's longitudes, or
                // ±180) are one point.
                let mut first = None::<Vector>;
                let mut extended = false;
                geometry.for_each(|p| {
                    let v = arcs::embed(&geometry.space.frame, *p);
                    if let Some(at) = first {
                        extended |= v != at;
                    } else {
                        first = Some(v);
                    }
                });
                i8::from(extended)
            },
            Kind::Polygon | Kind::MultiPolygon => 2,
            Kind::GeometryCollection => unreachable!("collection coordinates"),
        },
    }
}

/// Whether two boxes meet, or in a geographic chart their images one
/// longitude turn away.
fn reaches(bbox: &[f64; 4], bounds: &[f64; 4], geographic: bool) -> bool {
    let shifts: &[f64] = if geographic {
        &[0., -360., 360.]
    } else {
        &[0.]
    };
    shifts.iter().any(|shift| {
        crate::boxes_meet(
            bbox,
            &[
                (bounds[0] + shift).next_down(),
                bounds[1],
                (bounds[2] + shift).next_up(),
                bounds[3],
            ],
        )
    })
}

fn span(a: [f64; 4], b: [f64; 4]) -> [f64; 4] {
    [
        a[0].min(b[0]),
        a[1].min(b[1]),
        a[2].max(b[2]),
        a[3].max(b[3]),
    ]
}

fn dot(a: &Exact, b: &Exact) -> Rational {
    &a[0] * &b[0] + &a[1] * &b[1] + &a[2] * &b[2]
}

fn cross(a: &Exact, b: &Exact) -> Exact {
    [
        &a[1] * &b[2] - &a[2] * &b[1],
        &a[2] * &b[0] - &a[0] * &b[2],
        &a[0] * &b[1] - &a[1] * &b[0],
    ]
}

fn refs<'a>(vectors: &[&'a Exact]) -> Vec<&'a Rational> {
    vectors.iter().flat_map(|v| v.iter()).collect()
}

impl PreparedTopology {
    /// The raw edges and their packed tree, shared with metric preparation.
    pub(crate) fn packed(&self) -> &PackedEdges {
        &self.index
    }

    pub(crate) fn new(
        geometry: &Geometry,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        let initial_bytes = work.allocated_bytes;
        let initial_steps = work.steps;
        let previous_peak = std::mem::replace(&mut work.peak_allocated_bytes, initial_bytes);
        let result = (|| {
            geometry.space.validate_kernel()?;
            if !geometry.space.is_ground() || geometry.heights.is_some() {
                return Err(crate::unsupported_space(
                    "Topology requires explicit ground projection",
                ));
            }
            work.charge(geometry.position_count())?;
            let frame = geometry.space.frame.clone();
            let shape = Shape::new(
                &frame,
                &[geometry],
                Parts {
                    areas: true,
                    lines: true,
                    points: true,
                },
                work,
            )?;
            let mut bounds = [
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ];
            for edge in &shape.edges {
                if edge.role != Role::Collapsed {
                    bounds = span(bounds, arcs::arc_box(shape.geographic, edge.start, edge.end));
                }
            }
            if shape.geographic {
                for ring in shape.rings.iter().filter(|ring| !ring.hole) {
                    let full = [-180.0 - TOLERANCE, 180.0 + TOLERANCE];
                    if ring.region(0, false) {
                        bounds = span(bounds, [full[0], bounds[1], full[1], 90.0 + TOLERANCE]);
                    }
                    if ring.region(1, false) {
                        bounds = span(bounds, [full[0], -90.0 - TOLERANCE, full[1], bounds[3]]);
                    }
                }
            }
            let index = PackedEdges::new(
                shape.edges.iter().map(|edge| edge.raw).collect(),
                &frame,
                work,
            )?;
            Ok(Self {
                frame,
                index,
                dimension: dim(geometry),
                bounds,
                shape,
                points: None,
                preparation: TopologyCost {
                    steps: work.steps - initial_steps,
                    bytes: work.allocated_bytes - initial_bytes,
                    peak: work.peak_allocated_bytes - initial_bytes,
                },
            })
        })();
        work.peak_allocated_bytes = work.peak_allocated_bytes.max(previous_peak);
        result
    }

    pub(crate) fn retain_points(&mut self, geometry: &Geometry) -> usize {
        if self.dimension != 0 || self.points.is_some() { return 0; }
        let bytes = self.reference_points_bound(geometry);
        self.points = Some(build_point_set(geometry));
        bytes
    }

    pub(crate) fn reference_points_bound(&self, geometry: &Geometry) -> usize {
        if self.dimension != 0 || self.points.is_some() { return 0; }
        geometry.position_count().saturating_mul(80)
    }

    fn geographic(&self) -> bool {
        self.shape.geographic
    }

    /// Whether some edge's arc box meets a coordinate rectangle, or in WGS84
    /// its image one longitude turn away.
    pub(crate) fn meets_box(
        &self,
        bounds: [f64; 4],
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let shifts: &[f64] = if self.geographic() {
            &[0., -360., 360.]
        } else {
            &[0.]
        };
        for shift in shifts {
            let query = [bounds[0] + shift, bounds[1], bounds[2] + shift, bounds[3]];
            for id in self.shape.candidates(&query, work)? {
                work.charge(1)?;
                if self.shape.edges[id].role != Role::Collapsed {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn vertex(&self, p: Pos) -> Point {
        Point::vertex(arcs::embed(&self.frame, p))
    }

    pub(crate) fn relation(
        &self,
        raw: Pos,
        work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        Ok(match self.locate(&self.vertex(raw), work)? {
            0 => Rel::Inside,
            1 => Rel::Boundary,
            2 => Rel::Outside,
            _ => unreachable!("Exact topology location"),
        })
    }

    pub(crate) fn predicate(
        &self,
        a: &Geometry,
        b: &Geometry,
        other: &Self,
        predicate: Predicate,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        work.scoped(|work| evaluate_topologies(a, b, predicate, self, other, work))
    }

    pub(crate) fn evaluate_from(
        &self, left: &Geometry, right: &Geometry, predicate: Predicate, work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        validate_pair(left, right)?;
        if left.is_empty() || right.is_empty() {
            return Ok(empty_pair(left, right, predicate));
        }
        work.scoped(|work| {
            let left_topology = Self::new(left, work)?;
            if self.preparation.replay(work)? {
                evaluate_topologies(left, right, predicate, &left_topology, self, work)
            } else {
                let right_topology = Self::new(right, work)?;
                evaluate_topologies(left, right, predicate, &left_topology, &right_topology, work)
            }
        })
    }

    pub(crate) fn replay_relation(
        &self, geometry: &Geometry, point: Pos, work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        if self.preparation.replay(work)? {
            self.relation(point, work)
        } else {
            Self::new(geometry, work)?.relation(point, work)
        }
    }

    /// Interior 0, boundary 1 or exterior 2. Candidate edges and samples
    /// are released once the location is known.
    fn locate(&self, p: &Point, work: &mut GeometryWork) -> Result<usize, GeometryRefusal> {
        work.scoped(|work| self.location(p, work))
    }

    fn location(&self, p: &Point, work: &mut GeometryWork) -> Result<usize, GeometryRefusal> {
        match self.shape.area_relation(p, work)? {
            Rel::Inside => return Ok(0),
            Rel::Boundary => {
                // Coincident boundaries covered on every side are interior to
                // a collection's union. Examine every incident angular sector.
                let rays = self.rays(p, true, work)?;
                for heading in self.sectors(p, rays, work)? {
                    let sample = probe(p, &heading, &[self], work)?;
                    if self.shape.area_relation(&sample, work)? == Rel::Outside {
                        return Ok(1);
                    }
                }
                return Ok(0);
            },
            Rel::Outside => {},
        }
        let rays = self.rays(p, false, work)?;
        if !rays.is_empty() {
            // Dissolved linework uses the mod-2 boundary rule. Duplicate
            // segments do not change the union or cancel an exposed endpoint.
            return Ok(if rays.len() % 2 == 0 { 0 } else { 1 });
        }
        for id in self.shape.candidates(&self.shape.point_box(p), work)? {
            work.charge(1)?;
            let edge = &self.shape.edges[id];
            if edge.role == Role::Point && arcs::coincide(&Point::vertex(edge.start), p, work)? {
                return Ok(0);
            }
        }
        Ok(2)
    }

    /// The distinct headings leaving p along the area (or line) edges
    /// through it.
    fn rays(
        &self,
        p: &Point,
        area: bool,
        work: &mut GeometryWork,
    ) -> Result<Vec<Exact>, GeometryRefusal> {
        let mut result: Vec<Exact> = vec![];
        for id in self.shape.candidates(&self.shape.point_box(p), work)? {
            let edge = &self.shape.edges[id];
            let wanted = match edge.role {
                Role::Area(_) => area,
                Role::Line => !area,
                Role::Point | Role::Collapsed => false,
            };
            if !wanted {
                continue;
            }
            let (a, b) = edge.points();
            if !arcs::contains(p, &a, &b, work)? {
                continue;
            }
            for endpoint in [&a, &b] {
                if arcs::coincide(endpoint, p, work)? {
                    continue;
                }
                let heading = arcs::heading(self.geographic(), p, endpoint, work)?;
                let mut known = false;
                for prior in &result {
                    let values = refs(&[prior, &heading]);
                    known = crate::exact::arithmetic(work, &values, || {
                        cross(prior, &heading).iter().all(Zero::is_zero)
                            && dot(prior, &heading).is_positive()
                    })?;
                    if known {
                        break;
                    }
                }
                if !known {
                    work.allocate(heading.iter().map(crate::exact::bytes).sum())?;
                    result.push(heading);
                }
            }
        }
        Ok(result)
    }

    /// One heading inside each angular sector the rays cut around p.
    fn sectors(
        &self,
        p: &Point,
        rays: Vec<Exact>,
        work: &mut GeometryWork,
    ) -> Result<Vec<Exact>, GeometryRefusal> {
        let Some(first) = rays.first().cloned() else {
            return Ok(vec![]);
        };
        let p = p.value(work)?;
        // A basis of the tangent plane at p: the first ray, and that ray a
        // quarter turn counterclockwise.
        let quarter = crate::exact::arithmetic(work, &refs(&[&p, &first]), || {
            if self.geographic() {
                cross(&p, &first)
            } else {
                [-first[1].clone(), first[0].clone(), Rational::zero()]
            }
        })?;
        type Plane = [Rational; 2];
        let mut planar: Vec<Plane> = Vec::with_capacity(rays.len());
        for ray in &rays {
            let values = refs(&[ray, &first, &quarter]);
            let point = crate::exact::arithmetic(work, &values, || {
                let point = [dot(ray, &first), dot(ray, &quarter)];
                let norm = point[0].abs() + point[1].abs();
                [&point[0] / &norm, &point[1] / &norm]
            })?;
            work.allocate(crate::exact::bytes(&point[0]) + crate::exact::bytes(&point[1]))?;
            planar.push(point);
        }
        let half = |p: &Plane| p[1].is_negative() || p[1].is_zero() && p[0].is_negative();
        let turn = |a: &Plane, b: &Plane| &a[0] * &b[1] - &a[1] * &b[0];
        let words = planar
            .iter()
            .flatten()
            .map(|value| value.numer().bits().max(value.denom().bits()) as usize)
            .max()
            .unwrap_or(1)
            .max(1)
            .div_ceil(64);
        work.charge(
            planar
                .len()
                .saturating_mul(planar.len())
                .saturating_mul(words)
                .saturating_mul(words)
                .saturating_mul(32),
        )?;
        planar.sort_by(|a, b| {
            half(a)
                .cmp(&half(b))
                .then_with(|| turn(a, b).cmp(&Rational::zero()).reverse())
        });
        let mut result = Vec::with_capacity(planar.len());
        for index in 0..planar.len() {
            let a = &planar[index];
            let b = &planar[(index + 1) % planar.len()];
            let values = [&a[0], &a[1], &b[0], &b[1]];
            let direction = crate::exact::arithmetic(work, &values, || {
                let mut direction = [&a[0] + &b[0], &a[1] + &b[1]];
                if planar.len() == 1 {
                    direction = [-&a[1], a[0].clone()];
                } else if turn(a, b).is_negative() {
                    direction = direction.map(|value| -value);
                } else if direction.iter().all(Zero::is_zero) {
                    direction = [-&a[1], a[0].clone()];
                }
                direction
            })?;
            let values = refs(&[&first, &quarter]);
            let heading = crate::exact::arithmetic(work, &values, || {
                [0, 1, 2].map(|i| &first[i] * &direction[0] + &quarter[i] * &direction[1])
            })?;
            work.allocate(heading.iter().map(crate::exact::bytes).sum())?;
            result.push(heading);
        }
        Ok(result)
    }

    /// Maximal closed parameter intervals of the arc from a to b inside or on
    /// the topology. Fractions are angles on the auxiliary sphere, or exact
    /// planar fractions rounded once.
    pub(crate) fn trajectory_intervals(
        &self,
        a: Pos,
        b: Pos,
        work: &mut GeometryWork,
    ) -> Result<Vec<crate::trajectory::Interval>, GeometryRefusal> {
        let (start, end) = (arcs::embed(&self.frame, a), arcs::embed(&self.frame, b));
        let step = Edge {
            raw: (a, b),
            start,
            end,
            role: Role::Line,
        };
        let (from, to) = step.points();
        if arcs::coincide(&from, &to, work)? {
            return Ok(if self.locate(&from, work)? != 2 {
                vec![crate::trajectory::Interval {
                    from: 0.0,
                    to: 1.0,
                    start: a,
                    end: b,
                }]
            } else {
                vec![]
            });
        }
        if arcs::antipodal_vertices(start, end, work)? {
            return Err(crate::invalid(
                "A trajectory step between antipodal positions fixes no arc",
            ));
        }
        work.allocate(256)?;
        let candidates = self.shape.candidates(&self.shape.arc_box(&from, &to), work)?;
        let cuts = cuts(
            &step,
            candidates.iter().map(|id| &self.shape.edges[*id]),
            work,
        )?;
        let mut fractions = Vec::with_capacity(cuts.len());
        for (point, _) in &cuts {
            fractions.push(arcs::fraction(self.geographic(), &from, &to, point, work)?);
        }
        let refused = || crate::GeometryRefusal {
            code: crate::GeometryRefusalCode::SpatialPrecisionLoss,
            message: "Trajectory intervals cannot preserve their distinct boundaries in binary64 \
                      fractions; shorten the supporting observation interval"
                .to_owned(),
            bound: None,
        };
        let mut ranges = Vec::new();
        for index in 0..cuts.len() {
            if self.locate(&cuts[index].0, work)? != 2 {
                ranges.push((index, index));
            }
            if index + 1 < cuts.len() {
                let middle = arcs::between(&cuts[index].0, &cuts[index + 1].0, work)?;
                if self.locate(&middle, work)? != 2 {
                    ranges.push((index, index + 1));
                }
            }
        }
        ranges.sort();
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (from, to) in ranges {
            if let Some(last) = merged.last_mut()
                && from <= last.1
            {
                last.1 = last.1.max(to);
            } else {
                merged.push((from, to));
            }
        }
        work.allocate(
            merged
                .len()
                .saturating_mul(std::mem::size_of::<crate::trajectory::Interval>()),
        )?;
        let mut result: Vec<crate::trajectory::Interval> = Vec::with_capacity(merged.len());
        for (from, to) in merged {
            if from < to && fractions[from] >= fractions[to]
                || result.last().is_some_and(|last| last.to >= fractions[from])
            {
                return Err(refused());
            }
            let mut endpoint = |index: usize| -> Result<Pos, GeometryRefusal> {
                if index == 0 {
                    return Ok(a);
                }
                if index == cuts.len() - 1 {
                    return Ok(b);
                }
                match cuts[index].1 {
                    Some(raw) => Ok(raw),
                    None => Ok(arcs::snap(&self.frame, &cuts[index].0, work)?.0),
                }
            };
            result.push(crate::trajectory::Interval {
                from: fractions[from],
                to: fractions[to],
                start: endpoint(from)?,
                end: endpoint(to)?,
            });
        }
        Ok(result)
    }
}

/// A point a short way from p along `heading`, before the first edge of
/// `topologies` the probe meets, so it lies in an open cell beside p.
fn probe(
    p: &Point,
    heading: &Exact,
    topologies: &[&PreparedTopology],
    work: &mut GeometryWork,
) -> Result<Point, GeometryRefusal> {
    let geographic = topologies[0].geographic();
    let end = arcs::probe_end(geographic, p, heading, work)?;
    let mut found = Vec::new();
    for topology in topologies {
        let shape = &topology.shape;
        for id in shape.candidates(&shape.arc_box(p, &end), work)? {
            let edge = &shape.edges[id];
            if edge.role == Role::Collapsed {
                continue;
            }
            let (c, d) = edge.points();
            for x in arcs::meet(p, &end, &c, &d, work)? {
                found.push((x, ()));
            }
        }
    }
    let nearest = arcs::order_along(p, &end, found, work)?;
    match nearest.first() {
        Some((x, ())) => arcs::between(p, x, work),
        None => arcs::between(p, &end, work),
    }
}

fn validate_pair(a: &Geometry, b: &Geometry) -> Result<(), GeometryRefusal> {
    a.space.validate_kernel()?;
    if a.space != b.space || !a.space.is_ground() || a.heights.is_some() || b.heights.is_some() {
        return Err(crate::unsupported_space(
            "Topology requires matching ground coordinate spaces",
        ));
    }
    Ok(())
}

fn empty_pair(a: &Geometry, b: &Geometry, predicate: Predicate) -> bool {
    matches!(predicate, Predicate::Disjoint)
        || matches!(predicate, Predicate::Equals) && a.is_empty() && b.is_empty()
}

/// The DE-9IM matrix of two values, exact: nine symbols `F`, `0`, `1`, `2`
/// in the order II IB IE BI BB BE EI EB EE. Every region is computed (the
/// named predicates read only the ones they need). EE is not always 2 on
/// the sphere: it is read from the samples beside every edge piece.
pub(crate) fn matrix(
    a: &Geometry,
    b: &Geometry,
    work: &mut GeometryWork,
) -> Result<String, GeometryRefusal> {
    validate_pair(a, b)?;
    let cells = work.scoped(|work| {
        let mut cells = [[-1_i8; 3]; 3];
        if a.is_empty() || b.is_empty() {
            // The exterior of an empty value is everything.
            cells[2][2] = 2;
            if !a.is_empty() {
                let topology = PreparedTopology::new(a, work)?;
                cells[0][2] = topology.dimension;
                cells[1][2] = topology.boundary_dimension(a, None, work)?;
            }
            if !b.is_empty() {
                let topology = PreparedTopology::new(b, work)?;
                cells[2][0] = topology.dimension;
                cells[2][1] = topology.boundary_dimension(b, None, work)?;
            }
            return Ok(cells);
        }
        let left = PreparedTopology::new(a, work)?;
        let right = PreparedTopology::new(b, work)?;
        full_cells(a, b, &left, &right, work)
    })?;
    Ok(cells
        .iter()
        .flatten()
        .map(|cell| match cell {
            -1 => 'F',
            0 => '0',
            1 => '1',
            _ => '2',
        })
        .collect())
}

/// Every cell of the matrix of two non-empty values.
fn full_cells(
    a: &Geometry,
    b: &Geometry,
    left: &PreparedTopology,
    right: &PreparedTopology,
    work: &mut GeometryWork,
) -> Result<[[i8; 3]; 3], GeometryRefusal> {
    let geographic = left.geographic();
    let mut cells = [[-1_i8; 3]; 3];
    if !reaches(&left.bounds, &right.bounds, geographic) {
        // Disjoint closed sets: each lies in the other's exterior, and two
        // disjoint closed sets never cover the plane or the sphere.
        cells[0][2] = left.dimension;
        cells[1][2] = left.boundary_dimension(a, None, work)?;
        cells[2][0] = right.dimension;
        cells[2][1] = right.boundary_dimension(b, None, work)?;
        cells[2][2] = 2;
        return Ok(cells);
    }
    if left.dimension == 0 && right.dimension == 0 {
        let a_points = point_set(a, left.points.as_ref(), work)?;
        let b_points = point_set(b, right.points.as_ref(), work)?;
        if a_points.iter().any(|p| b_points.contains(p)) {
            cells[0][0] = 0;
        }
        if a_points.iter().any(|p| !b_points.contains(p)) {
            cells[0][2] = 0;
        }
        if b_points.iter().any(|p| !a_points.contains(p)) {
            cells[2][0] = 0;
        }
        cells[2][2] = 2;
        return Ok(cells);
    }
    if left.dimension == 0 || right.dimension == 0 {
        let transposed = left.dimension != 0;
        let (points, cached, topology, other) = if transposed {
            (b, right.points.as_ref(), left, a)
        } else {
            (a, left.points.as_ref(), right, b)
        };
        // Rows: the points (interior only); columns: the other value.
        let mut own = [[-1_i8; 3]; 3];
        let set = point_set(points, cached, work)?;
        for p in set.iter() {
            let location = topology.locate(&Point::vertex(p.map(f64::from_bits)), work)?;
            own[0][location] = 0;
        }
        // A finite point set covers no positive-dimensional interior, and
        // leaves the other's boundary uncovered unless that boundary is
        // finitely many points, all of them in the set.
        own[2][0] = topology.dimension;
        own[2][1] = topology.boundary_dimension(other, Some(&*set), work)?;
        own[2][2] = 2;
        if transposed {
            for (i, row) in own.iter().enumerate() {
                for (j, cell) in row.iter().enumerate() {
                    cells[j][i] = *cell;
                }
            }
        } else {
            cells = own;
        }
        return Ok(cells);
    }
    // Every edge piece of both values, its cuts and the samples beside it,
    // located in both: the regions they meet are the matrix.
    for own in [left, right] {
        for index in 0..own.shape.edges.len() {
            work.charge(1)?;
            if own.shape.edges[index].role == Role::Collapsed {
                continue;
            }
            work.scoped(|work| refine(own, index, left, right, &mut cells, work))?;
        }
    }
    if !geographic {
        // The plane's exterior to two bounded values is unbounded.
        cells[2][2] = 2;
    }
    Ok(cells)
}

impl PreparedTopology {
    /// The dimension of a value's boundary outside `covered` (a point set,
    /// by frame vertex): 1 for areas, 0 when some endpoint of odd degree of
    /// its lines is uncovered, else none (-1).
    fn boundary_dimension(
        &self,
        geometry: &Geometry,
        covered: Option<&std::collections::BTreeSet<[u64; 3]>>,
        work: &mut GeometryWork,
    ) -> Result<i8, GeometryRefusal> {
        if self.shape.has_areas() {
            return Ok(1);
        }
        if self.dimension < 1 {
            return Ok(-1);
        }
        let mut ends = Vec::new();
        line_ends(geometry, &mut ends);
        work.charge(ends.len())?;
        for end in ends {
            let vertex = arcs::embed(&self.frame, end).map(f64::to_bits);
            if covered.is_some_and(|set| set.contains(&vertex)) {
                continue;
            }
            if self.locate(&self.vertex(end), work)? == 1 {
                return Ok(0);
            }
        }
        Ok(-1)
    }
}

/// The first and last positions of every line of a value.
fn line_ends(geometry: &Geometry, ends: &mut Vec<Pos>) {
    match (&geometry.coords, geometry.kind) {
        (Coords::Collection(members), _) => members.iter().for_each(|member| line_ends(member, ends)),
        (Coords::Points(line), Kind::LineString) => ends.extend(line.first().into_iter().chain(line.last())),
        (Coords::Lines(lines), Kind::MultiLineString) => {
            for line in lines {
                ends.extend(line.first().into_iter().chain(line.last()));
            }
        },
        _ => {},
    }
}

pub(crate) fn evaluate(
    a: &Geometry,
    b: &Geometry,
    predicate: Predicate,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    validate_pair(a, b)?;
    if a.is_empty() || b.is_empty() {
        return Ok(empty_pair(a, b, predicate));
    }
    work.scoped(|work| {
        let left = PreparedTopology::new(a, work)?;
        let right = PreparedTopology::new(b, work)?;
        evaluate_topologies(a, b, predicate, &left, &right, work)
    })
}

fn evaluate_topologies(
    a: &Geometry,
    b: &Geometry,
    predicate: Predicate,
    left: &PreparedTopology,
    right: &PreparedTopology,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    validate_pair(a, b)?;
    if a.is_empty() || b.is_empty() {
        return Ok(empty_pair(a, b, predicate));
    }
    let geographic = left.geographic();
    if !reaches(&left.bounds, &right.bounds, geographic) {
        return Ok(matches!(predicate, Predicate::Disjoint));
    }
    if left.dimension == 0 || right.dimension == 0 {
        let mut cells = [[-1_i8; 3]; 3];
        if left.dimension == 0 && right.dimension == 0 {
            let a = point_set(a, left.points.as_ref(), work)?;
            let b = point_set(b, right.points.as_ref(), work)?;
            if a.iter().any(|p| b.contains(p)) {
                cells[0][0] = 0;
            }
            if a.iter().any(|p| !b.contains(p)) {
                cells[0][2] = 0;
            }
            if b.iter().any(|p| !a.contains(p)) {
                cells[2][0] = 0;
            }
            return Ok(evaluate_matrix(cells, 0, 0, predicate));
        }
        let (points, cached, topology, predicate) = if left.dimension == 0 {
            (a, left.points.as_ref(), right, predicate)
        } else {
            (b, right.points.as_ref(), left, predicate.transposed())
        };
        // A finite point set cannot cover a positive-dimensional interior.
        // Locate each distinct point once; no pair arrangement is needed.
        cells[2][0] = topology.dimension;
        for p in point_set(points, cached, work)?.iter() {
            let location = topology.locate(&Point::vertex(p.map(f64::from_bits)), work)?;
            cells[0][location] = 0;
        }
        return Ok(evaluate_matrix(cells, 0, topology.dimension, predicate));
    }
    if matches!(predicate, Predicate::Overlaps) && left.dimension != right.dimension {
        return Ok(false);
    }
    let (shared, left_outside, right_outside) = predicate.reads(left.dimension, right.dimension);
    let (left_area, right_area) = (left.shape.has_areas(), right.shape.has_areas());
    // A sample on an operand's edge lies in its closure and a probe sample
    // lies on no edge, so the left operand's edges reach the right's exterior
    // only through probes into the right's area, and conversely.
    let mut cells = [[-1_i8; 3]; 3];
    for (own, other, visit, outside) in [
        (
            left,
            right,
            shared || left_outside || right_outside && right_area,
            left_outside,
        ),
        (
            right,
            left,
            shared || right_outside || left_outside && left_area,
            right_outside,
        ),
    ] {
        if !visit {
            continue;
        }
        let first = std::ptr::eq(own, left);
        let cell = |own: usize, other: usize| if first { (own, other) } else { (other, own) };
        for index in 0..own.shape.edges.len() {
            work.charge(1)?;
            let edge = &own.shape.edges[index];
            if edge.role == Role::Collapsed {
                continue;
            }
            let bbox = arcs::arc_box(geographic, edge.start, edge.end);
            if !reaches(&bbox, &other.bounds, geographic) {
                // Every sample of an edge away from the other operand, and
                // every probe from it, lies in the other's exterior. A piece
                // has interior points of its own operand beside its middle.
                if outside {
                    let location = if edge.role == Role::Point {
                        own.locate(&Point::vertex(edge.start), work)?
                    } else {
                        0
                    };
                    let (i, j) = cell(location, 2);
                    cells[i][j] = cells[i][j].max(0);
                }
                continue;
            }
            work.scoped(|work| refine(own, index, left, right, &mut cells, work))?;
        }
    }
    Ok(evaluate_matrix(
        cells,
        left.dimension,
        right.dimension,
        predicate,
    ))
}

/// Locate one edge's vertices, the middles of its pieces between exact
/// intersections, and probes beside each middle, in both operands.
fn refine(
    own: &PreparedTopology,
    index: usize,
    left: &PreparedTopology,
    right: &PreparedTopology,
    cells: &mut [[i8; 3]; 3],
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    let edge = &own.shape.edges[index];
    let (a, b) = edge.points();
    if edge.role == Role::Point {
        let (i, j) = (left.locate(&a, work)?, right.locate(&a, work)?);
        cells[i][j] = cells[i][j].max(0);
        return Ok(());
    }
    let bbox = own.shape.arc_box(&a, &b);
    let mut others = Vec::new();
    for topology in [left, right] {
        for id in topology.shape.candidates(&bbox, work)? {
            if !(std::ptr::eq(topology, own) && id == index) {
                others.push(&topology.shape.edges[id]);
            }
        }
    }
    let cuts = cuts(edge, others, work)?;
    work.charge(cuts.len().saturating_mul(cuts.len()))?;
    for (p, _) in &cuts {
        let (i, j) = (left.locate(p, work)?, right.locate(p, work)?);
        cells[i][j] = cells[i][j].max(0);
    }
    let normal = arcs::left_normal(own.geographic(), &a, &b, work)?;
    let opposite = normal.clone().map(|value| -value);
    for pair in cuts.windows(2) {
        let middle = arcs::between(&pair[0].0, &pair[1].0, work)?;
        let (i, j) = (left.locate(&middle, work)?, right.locate(&middle, work)?);
        cells[i][j] = cells[i][j].max(1);
        for heading in [&normal, &opposite] {
            let sample = probe(&middle, heading, &[left, right], work)?;
            let (i, j) = (left.locate(&sample, work)?, right.locate(&sample, work)?);
            cells[i][j] = cells[i][j].max(2);
        }
    }
    Ok(())
}

/// Distinct points by their frame vertex: ±180 and a pole's longitudes
/// name one point in WGS84.
fn point_set<'a>(
    geometry: &Geometry,
    cached: Option<&'a std::collections::BTreeSet<[u64; 3]>>,
    work: &mut GeometryWork,
) -> Result<std::borrow::Cow<'a, std::collections::BTreeSet<[u64; 3]>>, GeometryRefusal> {
    let count = geometry.position_count();
    work.charge(count.saturating_mul(count.max(1).ilog2() as usize + 1))?;
    work.allocate(count.saturating_mul(80))?;
    Ok(match cached {
        Some(points) => std::borrow::Cow::Borrowed(points),
        None => std::borrow::Cow::Owned(build_point_set(geometry)),
    })
}

fn build_point_set(geometry: &Geometry) -> std::collections::BTreeSet<[u64; 3]> {
    let mut points = std::collections::BTreeSet::new();
    geometry.for_each(|p| {
        points.insert(arcs::embed(&geometry.space.frame, *p).map(f64::to_bits));
    });
    points
}

fn evaluate_matrix(
    cells: [[i8; 3]; 3],
    left_dimension: i8,
    right_dimension: i8,
    predicate: Predicate,
) -> bool {
    let ii = cells[0][0];
    match predicate {
        Predicate::Intersects => cells[..2]
            .iter()
            .any(|row| row[..2].iter().any(|value| *value >= 0)),
        Predicate::Disjoint => cells[..2]
            .iter()
            .all(|row| row[..2].iter().all(|value| *value < 0)),
        Predicate::Equals => {
            cells[2][0] < 0 && cells[2][1] < 0 && cells[0][2] < 0 && cells[1][2] < 0
        },
        Predicate::Covers => cells[2][0] < 0 && cells[2][1] < 0,
        Predicate::CoveredBy => cells[0][2] < 0 && cells[1][2] < 0,
        Predicate::Contains => ii >= 0 && cells[2][0] < 0 && cells[2][1] < 0,
        Predicate::Within => ii >= 0 && cells[0][2] < 0 && cells[1][2] < 0,
        Predicate::Touches => {
            ii < 0
                && [cells[0][1], cells[1][0], cells[1][1]]
                    .iter()
                    .any(|v| *v >= 0)
        },
        Predicate::Crosses => match left_dimension.cmp(&right_dimension) {
            std::cmp::Ordering::Less => ii >= 0 && cells[0][2] >= 0,
            std::cmp::Ordering::Greater => ii >= 0 && cells[2][0] >= 0,
            std::cmp::Ordering::Equal => left_dimension == 1 && ii == 0,
        },
        Predicate::Overlaps => {
            left_dimension == right_dimension
                && ii == left_dimension
                && cells[0][2] >= 0
                && cells[2][0] >= 0
        },
    }
}

#[cfg(test)]
#[path = "relation_tests.rs"]
mod tests;
