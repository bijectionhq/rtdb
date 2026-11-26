//! The gated overlay behind `realize`: intersection and difference of two
//! admitted values, and the ring assembly that turns oriented boundary
//! pieces back into polygons.
//!
//! Every edge of both operands is cut where any edge meets it (on the exact
//! predicates of `arcs`). A sample strictly inside each piece decides,
//! exactly, which sides of the piece lie in each operand's areas and whether
//! the piece lies in each operand's point set. A piece with the result's
//! interior on one side only bounds an area of the result; a piece in the
//! result with the interior on neither side is one of its lines; a cut or a
//! point member in the result that no kept piece reaches is one of its
//! points. Coordinates are the operands' own, except crossings, which are
//! rounded to binary64; the result reports the largest rounding of a vertex
//! it keeps, in metres (0 iff it keeps no new coordinate).
use std::collections::BTreeMap;

use serde_json::{
    json,
    Value,
};

use crate::{
    arcs::{
        self,
        Exact,
        Point,
    },
    topology::{
        self,
        Edge,
        Parts,
        Role,
        Shape,
    },
    CoordinateFrame,
    Geometry,
    GeometryDeclaration,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Gv,
    Pos,
    Rel,
};

/// The two overlays `realize` gates (union is a collection and needs none).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Overlay {
    Intersection,
    Difference,
}

/// A directed piece of a result's boundary or of one of its lines: for an
/// area, its interior lies on the left. Each end carries its rounding in
/// metres; `origin` names the operand edge it lies on, so consecutive pieces
/// of one edge join without their shared cut.
#[derive(Clone, Debug)]
pub(crate) struct Piece {
    pub start: Pos,
    pub end: Pos,
    pub start_error_m: f64,
    pub end_error_m: f64,
    pub origin: (usize, usize),
}

impl Piece {
    /// The piece with the interior on its left.
    pub(crate) fn oriented(self, interior_left: bool) -> Self {
        if interior_left {
            self
        } else {
            self.reversed()
        }
    }

    fn reversed(self) -> Self {
        Self {
            start: self.end,
            end: self.start,
            start_error_m: self.end_error_m,
            end_error_m: self.start_error_m,
            origin: self.origin,
        }
    }
}

/// What a construction created beyond its operands' coordinates: each piece
/// with a new coordinate, in the result's coordinates (a point as a piece
/// from itself to itself), and whether rounding collapsed a part of the
/// exact result. Outside a neighbourhood of these pieces of twice the bound,
/// the exact result and its realization are one set, with one interior,
/// boundary and exterior; `relate` reads a term through them.
#[derive(Debug, Default)]
pub(crate) struct Created {
    pub pieces: Vec<(Pos, Pos)>,
    pub collapsed: bool,
}

impl Created {
    pub(crate) fn extend(&mut self, other: Created) {
        self.pieces.extend(other.pieces);
        self.collapsed |= other.collapsed;
    }
}

fn refused(message: impl std::fmt::Display) -> GeometryRefusal {
    crate::invalid(format!("realize: {message}"))
}

/// The vertex key of a position: equal for positions naming one vertex
/// (±180 are one meridian).
pub(crate) fn key(frame: &CoordinateFrame, p: Pos) -> [u64; 3] {
    arcs::embed(frame, p).map(|x| (x + 0.0).to_bits())
}

/// Which sides of a directed piece (whose left normal is `normal`) an
/// operand's areas hold at a sample `m` strictly inside it, and whether the
/// operand's point set holds `m`.
struct Probe {
    left: bool,
    right: bool,
    on: bool,
}

fn probe(shape: &Shape, m: &Point, normal: &Exact, work: &mut GeometryWork) -> Result<Probe, GeometryRefusal> {
    let relation = shape.area_relation(m, work)?;
    if relation == Rel::Inside {
        return Ok(Probe { left: true, right: true, on: true });
    }
    let mut probe = Probe { left: false, right: false, on: relation == Rel::Boundary };
    for id in shape.candidates(&shape.point_box(m), work)? {
        let edge = &shape.edges[id];
        let (c, d) = edge.points();
        match edge.role {
            Role::Collapsed => {},
            Role::Point => probe.on |= arcs::coincide(m, &c, work)?,
            Role::Line => probe.on |= arcs::contains(m, &c, &d, work)?,
            Role::Area(_) => {
                if relation == Rel::Boundary && arcs::contains(m, &c, &d, work)? {
                    let interior = shape.interior_left(id) == Some(topology::aligned(normal, &c, &d, work)?);
                    probe.left |= interior;
                    probe.right |= !interior;
                }
            },
        }
    }
    Ok(probe)
}

/// Whether an operand's point set holds `p`.
fn member(shape: &Shape, p: &Point, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    if shape.area_relation(p, work)? != Rel::Outside {
        return Ok(true);
    }
    for id in shape.candidates(&shape.point_box(p), work)? {
        let edge = &shape.edges[id];
        let (c, d) = edge.points();
        let on = match edge.role {
            Role::Collapsed | Role::Area(_) => false,
            Role::Point => arcs::coincide(p, &c, work)?,
            Role::Line => arcs::contains(p, &c, &d, work)?,
        };
        if on {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether an edge before `(x, id)` (operand order, then edge order) holds
/// the sample: that edge's piece already stands for this one.
fn duplicate(shapes: &[Shape; 2], x: usize, id: usize, m: &Point, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    for (y, shape) in shapes.iter().enumerate().take(x + 1) {
        for other in shape.candidates(&shape.point_box(m), work)? {
            if y == x && other >= id {
                continue;
            }
            let edge = &shape.edges[other];
            if matches!(edge.role, Role::Area(_) | Role::Line) {
                let (c, d) = edge.points();
                if arcs::contains(m, &c, &d, work)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// The output coordinates of a cut and their rounding in metres.
fn end(frame: &CoordinateFrame, cut: &(Point, Option<Pos>), work: &mut GeometryWork) -> Result<(Pos, f64), GeometryRefusal> {
    match cut.1 {
        Some(raw) => Ok((raw, 0.0)),
        None => arcs::snap(frame, &cut.0, work),
    }
}

/// The overlay of two admitted values in `space`: its value, the largest
/// rounding of a coordinate it keeps, in metres, and what it created.
pub(crate) fn overlay(space: &GeometrySpace, a: &Geometry, b: &Geometry, operation: Overlay, work: &mut GeometryWork) -> Result<(Value, f64, Created), GeometryRefusal> {
    work.scoped(|work| {
        let mut created = Created::default();
        let frame = &space.frame;
        let all = Parts { areas: true, lines: true, points: true };
        let shapes = [Shape::new(frame, &[a], all, work)?, Shape::new(frame, &[b], all, work)?];
        let mut areas: Vec<Piece> = Vec::new();
        let mut lines: Vec<Piece> = Vec::new();
        // Points the result may hold beyond its pieces: point members, and
        // for an intersection every cut (where two boundaries touch).
        let mut loose: Vec<(Point, Option<Pos>)> = Vec::new();
        for (x, shape) in shapes.iter().enumerate() {
            for id in 0..shape.edges.len() {
                let edge = &shape.edges[id];
                match edge.role {
                    Role::Collapsed => continue,
                    Role::Point => {
                        if operation == Overlay::Intersection || x == 0 {
                            loose.push((Point::vertex(edge.start), Some(edge.raw.0)));
                        }
                        continue;
                    },
                    Role::Area(_) | Role::Line => {},
                }
                let (ea, eb) = edge.points();
                let bounds = shape.arc_box(&ea, &eb);
                let mut others: Vec<&Edge> = Vec::new();
                for (y, other) in shapes.iter().enumerate() {
                    for o in other.candidates(&bounds, work)? {
                        if (y, o) != (x, id) && other.edges[o].role != Role::Collapsed {
                            others.push(&other.edges[o]);
                        }
                    }
                }
                let cuts = topology::cuts(edge, others.iter().copied(), work)?;
                if operation == Overlay::Intersection {
                    work.allocate(cuts.len().saturating_mul(64))?;
                    loose.extend(cuts.iter().cloned());
                }
                let normal = arcs::left_normal(true, &ea, &eb, work)?;
                for pair in cuts.windows(2) {
                    let m = arcs::between(&pair[0].0, &pair[1].0, work)?;
                    if duplicate(&shapes, x, id, &m, work)? {
                        continue;
                    }
                    let pa = probe(&shapes[0], &m, &normal, work)?;
                    let pb = probe(&shapes[1], &m, &normal, work)?;
                    let (left, right, held) = match operation {
                        Overlay::Intersection => (pa.left && pb.left, pa.right && pb.right, pa.on && pb.on),
                        Overlay::Difference => (pa.left && !pb.left, pa.right && !pb.right, pa.on && !pb.on),
                    };
                    if left == right && (left || !held) {
                        continue;
                    }
                    let (start, start_error_m) = end(frame, &pair[0], work)?;
                    let (finish, end_error_m) = end(frame, &pair[1], work)?;
                    if start_error_m > 0.0 || end_error_m > 0.0 {
                        work.allocate(std::mem::size_of::<(Pos, Pos)>())?;
                        created.pieces.push((start, finish));
                    }
                    if key(frame, start) == key(frame, finish) {
                        // Distinct exact cuts that round to one vertex.
                        created.collapsed = true;
                        continue;
                    }
                    work.allocate(std::mem::size_of::<Piece>())?;
                    let piece = Piece { start, end: finish, start_error_m, end_error_m, origin: (x, id) };
                    if left == right {
                        lines.push(piece);
                    } else if left {
                        areas.push(piece);
                    } else {
                        areas.push(piece.reversed());
                    }
                }
            }
        }
        let mut deviation = 0.0_f64;
        let rings = rings(frame, areas, &mut deviation, &mut created.collapsed, work)?;
        let polygons = polygons(space, rings, &mut created.collapsed, work)?;
        let lines = chains(frame, lines, &mut deviation, work)?;
        // Loose points held by the result and reached by none of its parts.
        let mut reached: std::collections::BTreeSet<[u64; 3]> = std::collections::BTreeSet::new();
        for ring in polygons.iter().flatten().chain(lines.iter()) {
            work.charge(ring.len())?;
            reached.extend(ring.iter().map(|p| key(frame, *p)));
        }
        let mut points: Vec<(Pos, f64)> = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for cut in loose {
            let (position, error) = end(frame, &cut, work)?;
            let k = key(frame, position);
            if reached.contains(&k) || !seen.insert(k) {
                continue;
            }
            let held = match operation {
                Overlay::Intersection => member(&shapes[0], &cut.0, work)? && member(&shapes[1], &cut.0, work)?,
                Overlay::Difference => member(&shapes[0], &cut.0, work)? && !member(&shapes[1], &cut.0, work)?,
            };
            if held {
                if error > 0.0 {
                    created.pieces.push((position, position));
                }
                points.push((position, error));
            }
        }
        if !points.is_empty() && (!polygons.is_empty() || !lines.is_empty()) {
            let parts = construct(space, assemble(&polygons, &lines, &[]), work)?;
            let mut kept = Vec::new();
            for (position, error) in points {
                let single = Geometry::new(crate::Kind::Point, crate::Coords::Point(position), space.clone());
                if !crate::predicates::covered_by(&single, &parts, work)? {
                    kept.push((position, error));
                }
            }
            points = kept;
        }
        for (_, error) in &points {
            deviation = deviation.max(*error);
        }
        let positions: Vec<Pos> = points.into_iter().map(|(p, _)| p).collect();
        Ok((assemble(&polygons, &lines, &positions), deviation, created))
    })
}

/// Admit a constructed value in `space` at the large tier.
pub(crate) fn construct(space: &GeometrySpace, value: Value, work: &mut GeometryWork) -> Result<Geometry, GeometryRefusal> {
    let declaration = GeometryDeclaration::parse(space.clone(), "large").map_err(crate::unsupported_space)?;
    crate::admit_value_with_work(&Gv::from(&value), &declaration, work)
}

/// One value of the parts of a result: polygons, lines and points, as a
/// single kind when only one is present, else a collection.
pub(crate) fn assemble(polygons: &[Vec<Vec<Pos>>], lines: &[Vec<Pos>], points: &[Pos]) -> Value {
    let mut members = Vec::new();
    match polygons {
        [] => {},
        [one] => members.push(json!({ "type": "Polygon", "coordinates": one })),
        many => members.push(json!({ "type": "MultiPolygon", "coordinates": many })),
    }
    match lines {
        [] => {},
        [one] => members.push(json!({ "type": "LineString", "coordinates": one })),
        many => members.push(json!({ "type": "MultiLineString", "coordinates": many })),
    }
    match points {
        [] => {},
        [one] => members.push(json!({ "type": "Point", "coordinates": one })),
        many => members.push(json!({ "type": "MultiPoint", "coordinates": many })),
    }
    if members.len() == 1 {
        members.pop().expect("one member")
    } else {
        json!({ "type": "GeometryCollection", "geometries": members })
    }
}

/// The heading, in radians counterclockwise from east (or +x), of the arc
/// leaving `from` towards `to`: on the auxiliary sphere in WGS84, whose map
/// keeps the order of directions around a point.
fn heading(frame: &CoordinateFrame, from: Pos, to: Pos) -> f64 {
    match frame {
        CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. } => {
            let unit = |v: [f64; 3]| {
                let n = libm::sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]);
                v.map(|x| x / n)
            };
            let dot = |a: [f64; 3], b: [f64; 3]| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
            let cross = |a: [f64; 3], b: [f64; 3]| [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
            let (p, q) = (unit(arcs::direction(from)), unit(arcs::direction(to)));
            let along = dot(p, q);
            let t = [0, 1, 2].map(|i| q[i] - along * p[i]);
            let mut east = cross([0.0, 0.0, 1.0], p);
            if dot(east, east) < 1e-30 {
                east = [1.0, 0.0, 0.0];
            }
            let east = unit(east);
            let north = cross(p, east);
            libm::atan2(dot(t, north), dot(t, east))
        },
        CoordinateFrame::Cartesian { .. } => libm::atan2(to[1] - from[1], to[0] - from[0]),
    }
}

/// Join directed area pieces into closed rings, interior on the left. At a
/// vertex with several ways on, a ring takes the sharpest left turn, so
/// regions meeting at a point stay separate rings. A vertex between two
/// pieces of one operand edge is dropped (with its rounding); `deviation`
/// rises to the largest rounding of a vertex kept, and `collapsed` is set when
/// a piece or ring rounds to nothing.
pub(crate) fn rings(frame: &CoordinateFrame, pieces: Vec<Piece>, deviation: &mut f64, collapsed: &mut bool, work: &mut GeometryWork) -> Result<Vec<Vec<Pos>>, GeometryRefusal> {
    let count = pieces.len();
    let pieces: Vec<Piece> = pieces.into_iter().filter(|p| key(frame, p.start) != key(frame, p.end)).collect();
    *collapsed |= pieces.len() < count;
    work.charge(pieces.len().saturating_mul(4))?;
    work.allocate(pieces.len().saturating_mul(64))?;
    let mut outgoing: BTreeMap<[u64; 3], Vec<usize>> = BTreeMap::new();
    for (i, piece) in pieces.iter().enumerate() {
        outgoing.entry(key(frame, piece.start)).or_default().push(i);
    }
    let tau = 2.0 * core::f64::consts::PI;
    let mut next = vec![usize::MAX; pieces.len()];
    let mut taken = vec![false; pieces.len()];
    for (i, piece) in pieces.iter().enumerate() {
        let Some(ways) = outgoing.get(&key(frame, piece.end)) else {
            return Err(refused("the overlay's boundary does not close"));
        };
        work.charge(ways.len())?;
        let back = heading(frame, piece.end, piece.start);
        let mut best: Option<(f64, usize)> = None;
        for &j in ways {
            let mut turn = (back - heading(frame, pieces[j].start, pieces[j].end)).rem_euclid(tau);
            if turn == 0.0 {
                turn = tau;
            }
            if best.is_none_or(|(t, _)| turn < t) {
                best = Some((turn, j));
            }
        }
        let (_, j) = best.expect("a way on");
        if taken[j] {
            return Err(refused("the overlay's boundary does not assemble into rings"));
        }
        taken[j] = true;
        next[i] = j;
    }
    let mut prev = vec![0; pieces.len()];
    for (i, &j) in next.iter().enumerate() {
        prev[j] = i;
    }
    let mut seen = vec![false; pieces.len()];
    let mut out = Vec::new();
    for first in 0..pieces.len() {
        if seen[first] {
            continue;
        }
        // Start where the operand edge changes, so no kept vertex is lost.
        let mut start = first;
        for _ in 0..pieces.len() {
            let previous = prev[start];
            if previous == first || pieces[previous].origin != pieces[start].origin {
                break;
            }
            start = previous;
        }
        let mut ring = vec![pieces[start].start];
        *deviation = deviation.max(pieces[start].start_error_m);
        let mut i = start;
        loop {
            seen[i] = true;
            let j = next[i];
            if j == start || pieces[j].origin != pieces[i].origin {
                ring.push(pieces[i].end);
                *deviation = deviation.max(pieces[i].end_error_m);
            }
            i = j;
            if i == start {
                break;
            }
        }
        let first_position = ring[0];
        if let Some(last) = ring.last_mut() {
            *last = first_position;
        }
        if ring.len() >= 4 {
            out.push(ring);
        } else {
            *collapsed = true;
        }
    }
    Ok(out)
}

/// Join line pieces, in order, into chains; a vertex between two pieces of
/// one operand edge is dropped (with its rounding).
fn chains(frame: &CoordinateFrame, pieces: Vec<Piece>, deviation: &mut f64, work: &mut GeometryWork) -> Result<Vec<Vec<Pos>>, GeometryRefusal> {
    work.charge(pieces.len())?;
    let mut out: Vec<(Vec<Pos>, Vec<f64>, (usize, usize))> = Vec::new();
    for piece in pieces {
        match out.last_mut() {
            Some((chain, errors, origin)) if chain.last().is_some_and(|p| key(frame, *p) == key(frame, piece.start)) => {
                if *origin == piece.origin {
                    chain.pop();
                    errors.pop();
                }
                chain.push(piece.end);
                errors.push(piece.end_error_m);
                *origin = piece.origin;
            },
            _ => out.push((vec![piece.start, piece.end], vec![piece.start_error_m, piece.end_error_m], piece.origin)),
        }
    }
    for (_, errors, _) in &out {
        for e in errors {
            *deviation = deviation.max(*e);
        }
    }
    Ok(out.into_iter().map(|(chain, ..)| chain).collect())
}

/// Twice the signed lon/lat (or x/y) area of a ring and its longitude
/// winding in turns: counterclockwise is positive. Only picks which ring of a
/// geographic polygon is written first; topology never reads it.
fn orientation(frame: &CoordinateFrame, ring: &[Pos]) -> (f64, f64) {
    let geographic = frame.is_geographic();
    let origin = ring[0];
    let mut x = 0.0_f64;
    let mut winding = 0.0_f64;
    let mut points = Vec::with_capacity(ring.len());
    for pair in ring.windows(2) {
        let mut dx = pair[1][0] - pair[0][0];
        if geographic {
            dx = (dx + 180.0).rem_euclid(360.0) - 180.0;
            winding += dx;
        }
        points.push([x, pair[0][1] - origin[1]]);
        x += dx;
    }
    points.push([x, ring[ring.len() - 1][1] - origin[1]]);
    let twice: f64 = points.windows(2).map(|w| w[0][0] * w[1][1] - w[1][0] * w[0][1]).sum();
    (twice, winding / 360.0)
}

/// Whether ring `inner` lies in the open left side of `outer`: a point
/// strictly inside one of `inner`'s edges, located exactly. Rings of one
/// arrangement never cross or share an edge, so the first edge off `outer`
/// decides.
fn inside(inner: &arcs::Ring, outer: &arcs::Ring, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    let vertices = inner.vertices();
    let n = vertices.len();
    for i in 0..n {
        let m = arcs::between(&Point::vertex(vertices[i]), &Point::vertex(vertices[(i + 1) % n]), work)?;
        match outer.locate_point(&m, work)? {
            Rel::Inside => return Ok(true),
            Rel::Outside => return Ok(false),
            Rel::Boundary => {},
        }
    }
    Err(refused("two rings of the overlay share every edge"))
}

/// Group rings (interior on the left) into polygons, exactly on the sphere
/// or plane the frame's arcs live on. Rings i and j bound one component when
/// each lies on the other's interior side and every other ring holds both or
/// neither: no ring separates them. This needs no lon/lat orientation, so a
/// ring turning around a pole, or holding one, is grouped like any other.
/// A planar polygon is written shell first, its counterclockwise ring; a
/// geographic one reads the same region whichever ring comes first (a
/// shell's area is its left, a hole removes its right), so the first ring is
/// one that is counterclockwise in lon/lat without winding when there is one.
pub(crate) fn polygons(space: &GeometrySpace, rings: Vec<Vec<Pos>>, collapsed: &mut bool, work: &mut GeometryWork) -> Result<Vec<Vec<Vec<Pos>>>, GeometryRefusal> {
    let frame = &space.frame;
    let geographic = frame.is_geographic();
    let mut exact = Vec::with_capacity(rings.len());
    let mut kept = Vec::with_capacity(rings.len());
    for ring in rings {
        work.charge(ring.len())?;
        // A ring of fewer than three directions bounds nothing.
        match arcs::Ring::of_positions(geographic, &ring, work)? {
            Some(admitted) => {
                exact.push(admitted);
                kept.push(ring);
            },
            None => *collapsed = true,
        }
    }
    let rings = kept;
    let n = rings.len();
    work.charge(n.saturating_mul(n))?;
    // holds[i][j]: ring j lies on ring i's interior side.
    let mut holds = vec![vec![false; n]; n];
    for i in 0..n {
        for j in 0..n {
            if i != j {
                holds[i][j] = inside(&exact[j], &exact[i], work)?;
            }
        }
    }
    let together = |i: usize, j: usize| holds[i][j] && holds[j][i] && (0..n).all(|k| k == i || k == j || holds[k][i] == holds[k][j]);
    let mut group = vec![usize::MAX; n];
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for i in 0..n {
        if group[i] != usize::MAX {
            continue;
        }
        group[i] = groups.len();
        let mut members = vec![i];
        for j in i + 1..n {
            if group[j] == usize::MAX && together(i, j) {
                group[j] = groups.len();
                members.push(j);
            }
        }
        groups.push(members);
    }
    let mut out = Vec::with_capacity(groups.len());
    for members in groups {
        let first = if geographic {
            members
                .iter()
                .position(|&r| {
                    let (twice, winding) = orientation(frame, &rings[r]);
                    twice > 0.0 && winding.abs() < 0.5
                })
                .unwrap_or(0)
        } else {
            let Some(first) = members.iter().position(|&r| exact[r].counterclockwise()) else {
                return Err(refused("a planar polygon of the overlay has no outer ring"));
            };
            first
        };
        let shell = members[first];
        out.push(
            std::iter::once(shell)
                .chain(members.iter().copied().filter(|&r| r != shell))
                .map(|r| rings[r].clone())
                .collect(),
        );
    }
    Ok(out)
}
