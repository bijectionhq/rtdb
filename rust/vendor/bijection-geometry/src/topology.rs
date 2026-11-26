//! Exact topology of great elliptic arcs in WGS84 and of segments in a
//! Cartesian plane, on the predicates of `arcs`. Vertices stay binary64;
//! crossings and samples are exact directions. Boundary pieces round new
//! vertices to binary64 coordinates and report that rounding in metres.
pub(crate) mod relation;

use crate::{
    arcs::{
        self,
        ArcIndex,
        Point,
        Ring,
        Vector,
    },
    exact::Rational,
    CoordinateFrame,
    Coords,
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Kind,
    Pos,
    Rel,
    TOLERANCE,
};

/// What one edge of a prepared operand bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// An edge of ring `ring`.
    Area(usize),
    Line,
    /// A point member, or a line piece between positions naming one direction.
    Point,
    /// A ring edge between positions naming one direction, such as two of a
    /// pole's longitudes: no edge at all.
    Collapsed,
}

pub(crate) struct Edge {
    pub raw: (Pos, Pos),
    pub start: Vector,
    pub end: Vector,
    pub role: Role,
}

impl Edge {
    pub(crate) fn points(&self) -> (Point, Point) {
        (Point::vertex(self.start), Point::vertex(self.end))
    }
}

struct RingInfo {
    polygon: usize,
    /// Whether the ring's left side contains each reference direction.
    left: [bool; 2],
    /// Whether the region the ring bounds lies on its left: a shell's area,
    /// or what a hole removes.
    region_left: bool,
    hole: bool,
}

impl RingInfo {
    /// Whether the polygon's interior lies left of the ring's edges.
    fn interior_left(&self) -> bool {
        self.region_left != self.hole
    }

    /// Whether the ring's region contains a point whose crossing parity
    /// from reference `k` is `flips`.
    fn region(&self, k: usize, flips: bool) -> bool {
        (self.left[k] != flips) == self.region_left
    }
}

/// Which members' parts a shape keeps.
#[derive(Clone, Copy)]
pub(crate) struct Parts {
    pub areas: bool,
    pub lines: bool,
    pub points: bool,
}

/// The edges and rings of a set of members, packed for exact queries. Edges
/// keep the metric order: ring edges by polygon, line pieces, then points.
pub(crate) struct Shape {
    pub geographic: bool,
    pub edges: Vec<Edge>,
    rings: Vec<RingInfo>,
    /// Each polygon's rings, its shell first.
    polygons: Vec<std::ops::Range<usize>>,
    /// Ring edges between two longitudes of one pole, with their ring: no
    /// arc, but the turn a lon/lat area integral takes there.
    pole_turns: Vec<((Pos, Pos), usize)>,
    index: ArcIndex,
}

/// The region side of a ring: in WGS84 a shell's area is its left and a
/// hole's its right, as counterclockwise shells and clockwise holes put
/// them; in a plane each ring bounds the side that does not reach infinity.
pub(crate) fn region_left(ring: &Ring, geographic: bool, hole: bool) -> bool {
    if geographic {
        !hole
    } else {
        ring.counterclockwise()
    }
}

impl Shape {
    pub(crate) fn new(
        frame: &CoordinateFrame,
        members: &[&Geometry],
        parts: Parts,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        let geographic = *frame == CoordinateFrame::Wgs84;
        let mut edges = Vec::new();
        let mut rings = Vec::new();
        let mut polygons = Vec::new();
        for member in members {
            collect(
                frame,
                member,
                parts,
                &mut edges,
                &mut rings,
                &mut polygons,
                work,
            )?;
        }
        let pole_turns = edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| {
                let (p, q) = edge.raw;
                edge.role == Role::Collapsed && p[1].abs() == 90.0 && q[1] == p[1] && p[0] != q[0]
            })
            .filter_map(|(id, edge)| ring_of_collapsed(&edges, id).map(|ring| (edge.raw, ring)))
            .collect();
        let mut shape = Self::index(geographic, edges, rings, polygons, work)?;
        shape.pole_turns = pole_turns;
        Ok(shape)
    }

    /// The shape of admitted polygons given as rings, shell first.
    pub(crate) fn of_rings(
        geographic: bool,
        polygons: &[&[Ring]],
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        let mut edges = Vec::new();
        let mut infos = Vec::new();
        let mut ranges = Vec::new();
        for (polygon, rings) in polygons.iter().enumerate() {
            let first = infos.len();
            for (index, ring) in rings.iter().enumerate() {
                let (vertices, positions) = (ring.vertices(), ring.positions());
                let n = vertices.len();
                work.allocate(n.saturating_mul(std::mem::size_of::<Edge>()))?;
                for i in 0..n {
                    let (start, end) = (vertices[i], vertices[(i + 1) % n]);
                    edges.push(Edge {
                        raw: (positions[i], positions[(i + 1) % n]),
                        start,
                        end,
                        role: Role::Area(infos.len()),
                    });
                }
                infos.push(RingInfo {
                    polygon,
                    left: [ring.left_contains(0), ring.left_contains(1)],
                    region_left: region_left(ring, geographic, index > 0),
                    hole: index > 0,
                });
            }
            ranges.push(first..infos.len());
        }
        Self::index(geographic, edges, infos, ranges, work)
    }

    fn index(
        geographic: bool,
        edges: Vec<Edge>,
        rings: Vec<RingInfo>,
        polygons: Vec<std::ops::Range<usize>>,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        work.allocate(edges.len().saturating_mul(32))?;
        let boxes: Vec<_> = edges
            .iter()
            .map(|edge| arcs::arc_box(geographic, edge.start, edge.end))
            .collect();
        let index = ArcIndex::new(geographic, &boxes, work)?;
        Ok(Self {
            geographic,
            edges,
            rings,
            polygons,
            pole_turns: Vec::new(),
            index,
        })
    }

    pub(crate) fn has_areas(&self) -> bool {
        !self.polygons.is_empty()
    }

    /// Whether a ring turns around a pole between two of its longitudes.
    pub(crate) fn has_pole_turns(&self) -> bool {
        !self.pole_turns.is_empty()
    }

    /// The polygon an edge bounds, if any.
    pub(crate) fn polygon_of(&self, edge: usize) -> Option<usize> {
        match self.edges[edge].role {
            Role::Area(ring) => Some(self.rings[ring].polygon),
            _ => None,
        }
    }

    pub(crate) fn interior_left(&self, edge: usize) -> Option<bool> {
        match self.edges[edge].role {
            Role::Area(ring) => Some(self.rings[ring].interior_left()),
            _ => None,
        }
    }

    pub(crate) fn candidates(
        &self,
        bounds: &[f64; 4],
        work: &mut GeometryWork,
    ) -> Result<Vec<usize>, GeometryRefusal> {
        self.index.search(bounds, work)
    }

    pub(crate) fn arc_box(&self, a: &Point, b: &Point) -> [f64; 4] {
        arcs::arc_box(self.geographic, a.approx(), b.approx())
    }

    pub(crate) fn point_box(&self, p: &Point) -> [f64; 4] {
        arcs::point_box(self.geographic, p.approx())
    }

    /// The box of the parity arc from p to its reference: p's meridian to
    /// the pole of its hemisphere, or the ray towards +x in a plane.
    fn ray_box(&self, p: &Point) -> [f64; 4] {
        let v = p.approx();
        if !self.geographic {
            let q = arcs::coordinates(false, v);
            let pad = |x: f64| TOLERANCE + 1e-12 * x.abs();
            return [
                q[0] - pad(q[0]),
                q[1] - pad(q[1]),
                f64::INFINITY,
                q[1] + pad(q[1]),
            ];
        }
        let q = arcs::position(v);
        let (south, north) = if v[2] >= 0.0 {
            (q[1] - TOLERANCE, 90.0 + TOLERANCE)
        } else {
            (-90.0 - TOLERANCE, q[1] + TOLERANCE)
        };
        if q[1].abs() > 89.99 {
            return [-180.0 - TOLERANCE, south, 180.0 + TOLERANCE, north];
        }
        [q[0] - TOLERANCE, south, q[0] + TOLERANCE, north]
    }

    /// Each polygon's relation to p.
    pub(crate) fn polygon_relations(
        &self,
        p: &Point,
        work: &mut GeometryWork,
    ) -> Result<Vec<Rel>, GeometryRefusal> {
        work.allocate(
            self.polygons
                .len()
                .saturating_add(self.rings.len())
                .saturating_mul(2),
        )?;
        let mut on = vec![false; self.polygons.len()];
        for id in self.candidates(&self.point_box(p), work)? {
            let edge = &self.edges[id];
            if let Role::Area(ring) = edge.role {
                let polygon = self.rings[ring].polygon;
                if !on[polygon] {
                    let (a, b) = edge.points();
                    on[polygon] = arcs::contains(p, &a, &b, work)?;
                }
            }
        }
        let (reference, k) = Ring::reference(self.geographic, p);
        let mut flips = vec![false; self.rings.len()];
        if !arcs::coincide(p, &reference, work)? {
            for id in self.candidates(&self.ray_box(p), work)? {
                let edge = &self.edges[id];
                if let Role::Area(ring) = edge.role
                    && !on[self.rings[ring].polygon] {
                        let (a, b) = edge.points();
                        flips[ring] ^= arcs::parity(&reference, p, &a, &b, work)?;
                    }
            }
        }
        Ok(self
            .polygons
            .iter()
            .enumerate()
            .map(|(polygon, rings)| {
                if on[polygon] {
                    Rel::Boundary
                } else if !rings.is_empty()
                    && self.rings[rings.start].region(k, flips[rings.start])
                    && rings
                        .clone()
                        .skip(1)
                        .all(|hole| !self.rings[hole].region(k, flips[hole]))
                {
                    Rel::Inside
                } else {
                    Rel::Outside
                }
            })
            .collect())
    }

    /// The union of the polygons' relation to p: inside one off its
    /// boundary, on some boundary, or outside them all.
    pub(crate) fn area_relation(
        &self,
        p: &Point,
        work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        if self.polygons.is_empty() {
            return Ok(Rel::Outside);
        }
        let relations = self.polygon_relations(p, work)?;
        Ok(if relations.contains(&Rel::Inside) {
            Rel::Inside
        } else if relations.contains(&Rel::Boundary) {
            Rel::Boundary
        } else {
            Rel::Outside
        })
    }
}

/// The points where two edges meet, each with the coordinates it has when
/// it is one of their vertices.
pub(crate) fn meet(
    edge: &Edge,
    other: &Edge,
    work: &mut GeometryWork,
) -> Result<Vec<(Point, Option<Pos>)>, GeometryRefusal> {
    let (a, b) = edge.points();
    let (c, d) = other.points();
    let raw = |v: Vector| {
        [
            (edge.start, edge.raw.0),
            (edge.end, edge.raw.1),
            (other.start, other.raw.0),
            (other.end, other.raw.1),
        ]
        .into_iter()
        .find(|(w, _)| *w == v)
        .map(|(_, p)| p)
    };
    Ok(arcs::meet(&a, &b, &c, &d, work)?
        .into_iter()
        .map(|p| {
            let raw = p.as_vertex().and_then(raw);
            (p, raw)
        })
        .collect())
}

/// The vertices of `edge` and every point where it meets `others`, in
/// order from its start.
pub(crate) fn cuts<'a>(
    edge: &Edge,
    others: impl IntoIterator<Item = &'a Edge>,
    work: &mut GeometryWork,
) -> Result<Vec<(Point, Option<Pos>)>, GeometryRefusal> {
    let (a, b) = edge.points();
    let mut found = Vec::new();
    for other in others {
        if other.role != Role::Collapsed {
            // The edge's own vertices are its ends already.
            found.extend(
                meet(edge, other, work)?
                    .into_iter()
                    .filter(|(p, _)| p.as_vertex().is_none_or(|v| v != edge.start && v != edge.end)),
            );
        }
    }
    if found.is_empty() {
        return Ok(vec![(a, Some(edge.raw.0)), (b, Some(edge.raw.1))]);
    }
    found.push((b.clone(), Some(edge.raw.1)));
    let mut ordered = arcs::order_along(&a, &b, found, work)?;
    ordered.insert(0, (a, Some(edge.raw.0)));
    Ok(ordered)
}

/// The ring of a collapsed ring edge: that of the nearest ring edge in the
/// same run of ring edges.
fn ring_of_collapsed(edges: &[Edge], id: usize) -> Option<usize> {
    let ring = |edge: &Edge| match edge.role {
        Role::Area(ring) => Some(ring),
        _ => None,
    };
    let run = |edge: &&Edge| matches!(edge.role, Role::Area(_) | Role::Collapsed);
    edges[..id]
        .iter()
        .rev()
        .take_while(run)
        .find_map(ring)
        .or_else(|| edges[id + 1..].iter().take_while(run).find_map(ring))
}

fn collect(
    frame: &CoordinateFrame,
    value: &Geometry,
    parts: Parts,
    edges: &mut Vec<Edge>,
    rings: &mut Vec<RingInfo>,
    polygons: &mut Vec<std::ops::Range<usize>>,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    work.charge(1)?;
    if let Coords::Collection(members) = &value.coords {
        for member in members {
            collect(frame, member, parts, edges, rings, polygons, work)?;
        }
        return Ok(());
    }
    let geographic = *frame == CoordinateFrame::Wgs84;
    work.allocate(
        value
            .position_count()
            .saturating_mul(std::mem::size_of::<Edge>() + 2 * std::mem::size_of::<Vector>()),
    )?;
    if parts.areas {
        for polygon in value.polygons() {
            let first = rings.len();
            let id = polygons.len();
            for (index, ring) in polygon.iter().enumerate() {
                // A cut along the antimeridian is interior: no edge.
                let ring = arcs::without_seam_cuts(geographic, ring);
                let mut vertices: Vec<Vector> = Vec::with_capacity(ring.len());
                let ring_id = rings.len();
                for pair in ring.windows(2) {
                    let (start, end) = (arcs::embed(frame, pair[0]), arcs::embed(frame, pair[1]));
                    let role = if start == end {
                        Role::Collapsed
                    } else {
                        Role::Area(ring_id)
                    };
                    edges.push(Edge {
                        raw: (pair[0], pair[1]),
                        start,
                        end,
                        role,
                    });
                    if vertices.last() != Some(&start) {
                        vertices.push(start);
                    }
                }
                while vertices.len() > 1 && vertices.first() == vertices.last() {
                    vertices.pop();
                }
                let (left, region) = if vertices.len() >= 3 {
                    let ring = Ring::trusted(geographic, vertices, work)?;
                    (
                        [ring.left_contains(0), ring.left_contains(1)],
                        region_left(&ring, geographic, index > 0),
                    )
                } else {
                    // A ring of fewer than three directions bounds nothing.
                    for edge in edges.iter_mut().rev().take(ring.len().saturating_sub(1)) {
                        edge.role = Role::Collapsed;
                    }
                    ([false; 2], true)
                };
                rings.push(RingInfo {
                    polygon: id,
                    left,
                    region_left: region,
                    hole: index > 0,
                });
            }
            polygons.push(first..rings.len());
        }
    }
    if parts.lines {
        for line in value.lines() {
            for pair in line.windows(2) {
                let (start, end) = (arcs::embed(frame, pair[0]), arcs::embed(frame, pair[1]));
                edges.push(Edge {
                    raw: (pair[0], pair[1]),
                    start,
                    end,
                    role: if start == end { Role::Point } else { Role::Line },
                });
            }
        }
    }
    if parts.points && matches!(value.kind, Kind::Point | Kind::MultiPoint) {
        let mut points = Vec::with_capacity(value.position_count());
        value.for_each(|p| points.push(*p));
        for p in points {
            let v = arcs::embed(frame, p);
            edges.push(Edge {
                raw: (p, p),
                start: v,
                end: v,
                role: Role::Point,
            });
        }
    }
    Ok(())
}

/// Whether two edges meet: closed arcs in WGS84, closed segments in a
/// plane; an edge between equal positions is a point.
pub(crate) fn contact(
    frame: &CoordinateFrame,
    a: (Pos, Pos),
    b: (Pos, Pos),
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    work.charge(4)?;
    let p = |q: Pos| Point::vertex(arcs::embed(frame, q));
    Ok(!arcs::meet(&p(a.0), &p(a.1), &p(b.0), &p(b.1), work)?.is_empty())
}

/// Whether an edge of a line joins antipodal vertices, which fix no arc.
pub(crate) fn antipodal_edge(
    frame: &CoordinateFrame,
    a: Pos,
    b: Pos,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    if *frame != CoordinateFrame::Wgs84 {
        return Ok(false);
    }
    arcs::antipodal_vertices(arcs::embed(frame, a), arcs::embed(frame, b), work)
}

/// The pairs (a, b), a < b, of rings whose boundaries meet.
pub(crate) fn meeting_rings(
    geographic: bool,
    rings: &[&Ring],
    work: &mut GeometryWork,
) -> Result<std::collections::HashSet<(usize, usize)>, GeometryRefusal> {
    let mut edges = Vec::new();
    let mut owners = Vec::new();
    for (r, ring) in rings.iter().enumerate() {
        let vertices = ring.vertices();
        let n = vertices.len();
        work.allocate(n.saturating_mul(80))?;
        for i in 0..n {
            edges.push((vertices[i], vertices[(i + 1) % n]));
            owners.push(r);
        }
    }
    let boxes: Vec<_> = edges
        .iter()
        .map(|(a, b)| arcs::arc_box(geographic, *a, *b))
        .collect();
    let index = ArcIndex::new(geographic, &boxes, work)?;
    let mut out = std::collections::HashSet::new();
    index.pairs(
        &mut |i, j, work| {
            let (a, b) = (owners[i], owners[j]);
            let pair = (a.min(b), a.max(b));
            if a != b && !out.contains(&pair) {
                let p = |v: Vector| Point::vertex(v);
                if !arcs::meet(&p(edges[i].0), &p(edges[i].1), &p(edges[j].0), &p(edges[j].1), work)?
                    .is_empty()
                {
                    work.allocate(64)?;
                    out.insert(pair);
                }
            }
            Ok(false)
        },
        work,
    )?;
    Ok(out)
}

/// Where p lies in the region a ring bounds.
fn region_relation(
    ring: &Ring,
    left: bool,
    p: Vector,
    work: &mut GeometryWork,
) -> Result<Rel, GeometryRefusal> {
    Ok(match ring.locate_direction(p, work)? {
        Rel::Boundary => Rel::Boundary,
        Rel::Inside if left => Rel::Inside,
        Rel::Outside if !left => Rel::Inside,
        Rel::Inside | Rel::Outside => Rel::Outside,
    })
}

/// Why a polygon's admitted rings do not form a polygon: a hole must lie
/// inside its shell, and holes apart, none touching another ring.
pub(crate) fn polygon_defect(
    geographic: bool,
    rings: &[Ring],
    work: &mut GeometryWork,
) -> Result<Option<&'static str>, GeometryRefusal> {
    if rings.len() <= 1 {
        return Ok(None);
    }
    let refs: Vec<&Ring> = rings.iter().collect();
    let meeting = meeting_rings(geographic, &refs, work)?;
    let sides: Vec<bool> = rings
        .iter()
        .enumerate()
        .map(|(index, ring)| region_left(ring, geographic, index > 0))
        .collect();
    let boxes: Vec<[f64; 4]> = rings
        .iter()
        .zip(&sides)
        .map(|(ring, left)| ring.bounds(*left))
        .collect();
    let first = |ring: &Ring| ring.vertices()[0];
    for i in 1..rings.len() {
        work.charge(rings[0].vertices().len())?;
        if meeting.contains(&(0, i))
            || region_relation(&rings[0], sides[0], first(&rings[i]), work)? != Rel::Inside
            || region_relation(&rings[i], sides[i], first(&rings[0]), work)? != Rel::Outside
        {
            return Ok(Some("hole outside or touching shell"));
        }
        for j in 1..i {
            work.charge(1)?;
            if crate::boxes_meet(&boxes[i], &boxes[j])
                && ({
                    work.charge(rings[i].vertices().len() + rings[j].vertices().len())?;
                    meeting.contains(&(j, i))
                        || region_relation(&rings[j], sides[j], first(&rings[i]), work)?
                            != Rel::Outside
                        || region_relation(&rings[i], sides[i], first(&rings[j]), work)?
                            != Rel::Outside
                })
            {
                return Ok(Some("overlapping holes"));
            }
        }
    }
    Ok(None)
}

/// Whether two edges are the two sides of a cut at the antimeridian: both
/// run along the ±180 meridian, one written at +180 (the eastern part's)
/// and the other at -180 (the western part's), as RFC 7946 splits a value
/// that crosses it. The shared meridian is then interior to their union.
fn antimeridian_cut(a: &Edge, b: &Edge) -> bool {
    let on = |edge: &Edge, longitude: f64| edge.raw.0[0] == longitude && edge.raw.1[0] == longitude;
    on(a, 180.0) && on(b, -180.0) || on(a, -180.0) && on(b, 180.0)
}

/// Whether any two of a multipart value's admitted polygons share an edge
/// or interior points. Isolated contacts are allowed, and so is a shared
/// edge along the antimeridian written at +180 in one part and -180 in the
/// other: a value that crosses it, split there. All parts share one
/// packed index: edges of different parts are compared once, each ring is
/// located in the others by one vertex, and only edges near another part are
/// refined.
pub(crate) fn parts_overlap(
    geographic: bool,
    parts: &[&[Ring]],
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    work.scoped(|work| {
        let shape = Shape::of_rings(geographic, parts, work)?;
        let mut contact = false;
        let mut overlap = false;
        shape.index.pairs(
            &mut |i, j, work| {
                if shape.polygon_of(i) == shape.polygon_of(j) {
                    return Ok(false);
                }
                let met = meet(&shape.edges[i], &shape.edges[j], work)?;
                if met.len() >= 2 {
                    if geographic && antimeridian_cut(&shape.edges[i], &shape.edges[j]) {
                        contact = true;
                        return Ok(false);
                    }
                    overlap = true;
                    return Ok(true);
                }
                contact |= !met.is_empty();
                Ok(false)
            },
            work,
        )?;
        if overlap {
            return Ok(true);
        }
        let inside_other = |relations: &[Rel], own: usize| {
            relations
                .iter()
                .enumerate()
                .any(|(polygon, relation)| polygon != own && *relation == Rel::Inside)
        };
        for (own, rings) in parts.iter().enumerate() {
            for ring in rings.iter() {
                let relations =
                    shape.polygon_relations(&Point::vertex(ring.vertices()[0]), work)?;
                if inside_other(&relations, own) {
                    return Ok(true);
                }
            }
        }
        if !contact {
            return Ok(false);
        }
        for id in 0..shape.edges.len() {
            let own = shape.polygon_of(id).expect("ring edges");
            let edge = &shape.edges[id];
            let (start, end) = edge.points();
            let found = work.scoped(|work| {
                let others: Vec<_> = shape
                    .candidates(&shape.arc_box(&start, &end), work)?
                    .into_iter()
                    .filter(|other| shape.polygon_of(*other) != Some(own))
                    .collect();
                if others.is_empty() {
                    return Ok(false);
                }
                let cuts = cuts(edge, others.iter().map(|other| &shape.edges[*other]), work)?;
                for pair in cuts.windows(2) {
                    let middle = arcs::between(&pair[0].0, &pair[1].0, work)?;
                    if inside_other(&shape.polygon_relations(&middle, work)?, own) {
                        return Ok(true);
                    }
                }
                Ok::<_, GeometryRefusal>(false)
            })?;
            if found {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// Exact Cartesian area accumulation shares the operation's work account.
pub(crate) fn add_area(
    sum: &mut Rational,
    term: &Rational,
    positive: bool,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    let next = crate::exact::arithmetic(work, &[sum, term], || {
        if positive {
            &*sum + term
        } else {
            &*sum - term
        }
    })?;
    work.allocate(crate::exact::bytes(&next))?;
    *sum = next;
    Ok(())
}

pub(crate) fn area(sum: &Rational, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
    use num_traits::ToPrimitive;
    crate::exact::arithmetic(work, &[sum], || {
        (sum / Rational::from_integer(2.into()))
            .to_f64()
            .ok_or_else(|| crate::invalid("Area exceeds finite binary64"))
    })?
}

/// Twice a Cartesian ring's signed area, exactly: the shoelace sum of its
/// admitted binary64 vertices.
pub(crate) fn twice_ring_area(
    ring: &[Pos],
    work: &mut GeometryWork,
) -> Result<Rational, GeometryRefusal> {
    let mut signed = Rational::from_integer(0.into());
    let Some(&first) = ring.first() else {
        return Ok(signed);
    };
    let mut previous = crate::exact::binary64(first, work)?;
    for &raw in &ring[1..] {
        let next = crate::exact::binary64(raw, work)?;
        let term = crate::exact::arithmetic(
            work,
            &[&previous[0], &previous[1], &next[0], &next[1]],
            || &previous[0] * &next[1] - &previous[1] * &next[0],
        )?;
        add_area(&mut signed, &term, true, work)?;
        previous = next;
    }
    Ok(signed)
}

/// Admitted Cartesian polygon parts are disjoint. Their exact signed-ring
/// reduction never cancels large world offsets.
pub(crate) fn polygon_area(
    geometry: &Geometry,
    work: &mut GeometryWork,
) -> Result<Rational, GeometryRefusal> {
    use num_traits::Signed;
    let mut sum = Rational::from_integer(0.into());
    for polygon in geometry.polygons() {
        for (index, ring) in polygon.iter().enumerate() {
            let signed = twice_ring_area(ring, work)?;
            add_area(&mut sum, &signed.abs(), index == 0, work)?;
        }
    }
    Ok(sum)
}

#[derive(Clone, Copy)]
pub(crate) enum BoundaryDomain {
    Union,
    Areas,
    Lines,
}

pub(crate) struct BoundaryPiece {
    pub start: Pos,
    pub end: Pos,
    pub is_area: bool,
    pub interior_left: bool,
    pub rounding_error_m: f64,
    pub twice_area: Rational,
}

/// The exposed boundary of the members' union, piece by piece: each edge is
/// cut where any other edge meets it, and a piece covered by another
/// member's interior, by area on both sides, or by an earlier duplicate is
/// dropped. New vertices are rounded to binary64 coordinates; a piece
/// reports the larger rounding of its ends in metres.
pub(crate) fn visit_boundary(
    geometry: &Geometry,
    members: &[&Geometry],
    domain: BoundaryDomain,
    work: &mut GeometryWork,
    visit: &mut impl FnMut(BoundaryPiece, &mut GeometryWork) -> Result<bool, GeometryRefusal>,
) -> Result<bool, GeometryRefusal> {
    work.scoped(|work| {
        let frame = &geometry.space.frame;
        let parts = Parts {
            areas: !matches!(domain, BoundaryDomain::Lines),
            lines: !matches!(domain, BoundaryDomain::Areas),
            points: matches!(domain, BoundaryDomain::Union),
        };
        let shape = Shape::new(frame, members, parts, work)?;
        let planar_area =
            matches!(domain, BoundaryDomain::Areas) && !shape.geographic;
        // Pole turns carry no arc, only the lon/lat area integral's turn
        // around the pole; a turn over one longitude range counts once.
        if matches!(domain, BoundaryDomain::Areas) {
            let mut seen: Vec<(f64, f64, f64)> = Vec::new();
            for &((start, end), ring) in &shape.pole_turns {
                work.charge(seen.len().saturating_add(1))?;
                let (low, high) = (start[0].min(end[0]), start[0].max(end[0]));
                if seen
                    .iter()
                    .any(|&(pole, a, b)| pole == start[1] && a < high && low < b)
                {
                    continue;
                }
                work.allocate(24)?;
                seen.push((start[1], low, high));
                if visit(
                    BoundaryPiece {
                        start,
                        end,
                        is_area: true,
                        interior_left: shape.rings[ring].interior_left(),
                        rounding_error_m: 0.0,
                        twice_area: Rational::from_integer(0.into()),
                    },
                    work,
                )? {
                    return Ok(true);
                }
            }
        }
        for id in 0..shape.edges.len() {
            let edge = &shape.edges[id];
            if edge.role == Role::Collapsed {
                continue;
            }
            let stop = work.scoped(|work| -> Result<bool, GeometryRefusal> {
                let (a, b) = edge.points();
                let is_area = matches!(edge.role, Role::Area(_));
                let candidates: Vec<usize> = shape
                    .candidates(&shape.arc_box(&a, &b), work)?
                    .into_iter()
                    .filter(|other| *other != id && shape.edges[*other].role != Role::Collapsed)
                    .collect();
                let cuts = if edge.role == Role::Point {
                    vec![(a.clone(), Some(edge.raw.0)), (b.clone(), Some(edge.raw.1))]
                } else {
                    cuts(edge, candidates.iter().map(|other| &shape.edges[*other]), work)?
                };
                let normal = arcs::left_normal(true, &a, &b, work)?;
                // Only edges on this edge's great circle can share a piece.
                let mut overlapping = Vec::new();
                for &other in &candidates {
                    let candidate = &shape.edges[other];
                    let (c, d) = candidate.points();
                    if edge.role == Role::Point
                        || candidate.role != Role::Point
                            && arcs::coplanar(&a, &b, &c, work)?
                            && arcs::coplanar(&a, &b, &d, work)?
                    {
                        overlapping.push(other);
                    }
                }
                for pair in cuts.windows(2) {
                    let middle = if edge.role == Role::Point {
                        Some(a.clone())
                    } else if !overlapping.is_empty() || shape.has_areas() {
                        Some(arcs::between(&pair[0].0, &pair[1].0, work)?)
                    } else {
                        None
                    };
                    let own = shape.interior_left(id);
                    let mut left = own == Some(true);
                    let mut right = own == Some(false);
                    let mut duplicate = false;
                    for &other in middle.as_ref().map_or(&[][..], |_| &overlapping[..]) {
                        let middle = middle.as_ref().expect("a sample");
                        let edge = &shape.edges[other];
                        let (c, d) = edge.points();
                        let other_area = matches!(edge.role, Role::Area(_));
                        let on = if edge.role == Role::Point {
                            arcs::coincide(middle, &c, work)?
                        } else {
                            arcs::contains(middle, &c, &d, work)?
                        };
                        if !on {
                            continue;
                        }
                        if other_area == is_area && other < id {
                            duplicate = true;
                        }
                        if other_area && edge.role != Role::Point && shape.edges[id].role != Role::Point {
                            let aligned = aligned(&normal, &c, &d, work)?;
                            let interior = shape.interior_left(other) == Some(aligned);
                            left |= interior;
                            right |= !interior;
                        }
                    }
                    if duplicate || left && right {
                        continue;
                    }
                    if let Some(middle) = &middle
                        && shape.has_areas()
                    {
                        let relation = shape.area_relation(middle, work)?;
                        if relation == Rel::Inside || !is_area && relation == Rel::Boundary {
                            continue;
                        }
                    }
                    let end = |point: &(Point, Option<Pos>), work: &mut GeometryWork| match point.1 {
                        Some(raw) => Ok::<_, GeometryRefusal>((raw, 0.0)),
                        None => arcs::snap(frame, &point.0, work),
                    };
                    let (start, start_error) = end(&pair[0], work)?;
                    let (finish, end_error) = end(&pair[1], work)?;
                    let twice_area = if planar_area {
                        let (p, q) = (arcs::planar(&pair[0].0, work)?, arcs::planar(&pair[1].0, work)?);
                        crate::exact::arithmetic(work, &[&p[0], &p[1], &q[0], &q[1]], || {
                            &p[0] * &q[1] - &p[1] * &q[0]
                        })?
                    } else {
                        Rational::from_integer(0.into())
                    };
                    work.allocate(crate::exact::bytes(&twice_area))?;
                    if visit(
                        BoundaryPiece {
                            start,
                            end: finish,
                            is_area,
                            interior_left: left,
                            rounding_error_m: start_error.max(end_error),
                            twice_area,
                        },
                        work,
                    )? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if stop {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// Whether arc cd runs the same way as the arc whose normal is `normal`,
/// on one great circle.
pub(crate) fn aligned(
    normal: &arcs::Exact,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    let other = arcs::left_normal(true, c, d, work)?;
    crate::exact::arithmetic(
        work,
        &normal.iter().chain(other.iter()).collect::<Vec<_>>(),
        || {
            use num_traits::Signed;
            (&normal[0] * &other[0] + &normal[1] * &other[1] + &normal[2] * &other[2]).is_positive()
        },
    )
}
