//! Boundaries with formats whose WGS84 edges are straight in
//! longitude/latitude: GeoJSON as RFC 7946 reads it, Foundry and most files.
//!
//! Import reads a planar value as the region it bounds in the
//! longitude/latitude plane. Rings are reoriented by their planar winding
//! (shells counterclockwise, holes clockwise), each straight edge is
//! densified until the great elliptic arcs between its vertices stay within
//! the caller's tolerance of it, and parts cut at the antimeridian or closed
//! along a pole's line are joined into one region.
//!
//! Export does the reverse: arcs are densified until planar chords stay
//! within the same tolerance, lines and areas are cut at the antimeridian, and
//! an area's sides along the cut and around a pole are drawn on the ±180 and
//! ±90 lines. Cartesian frames already have planar edges, so both are plain
//! admission there.
use std::collections::HashMap;

use crate::{
    arcs::{
        self,
        Vector,
    },
    geodesy::SEMI_MAJOR_AXIS_METRES,
    geojson,
    number,
    space::CoordinateFrame,
    Coords,
    Geometry,
    GeometryDeclaration,
    GeometryRefusal,
    GeometryWork,
    Gv,
    Kind,
    Pos,
    Rel,
};

/// The range of `tolerance_m`: how far, in metres, a densified piece may
/// stray from the edge it stands for. A source states it from its own
/// accuracy. It is fixed per call: one relative to each piece's length would
/// shrink with every split and never settle, while a fixed one keeps export
/// then import stable, since each exported chord already fits its arc.
pub const PLANAR_TOLERANCE_RANGE_METRES: std::ops::RangeInclusive<f64> = 0.001..=10_000.0;

fn validate_tolerance(tolerance: f64) -> Result<(), GeometryRefusal> {
    if PLANAR_TOLERANCE_RANGE_METRES.contains(&tolerance) {
        Ok(())
    } else {
        Err(invalid("tolerance_m must be 0.001..10000 metres"))
    }
}
/// Bisection depth after which a piece is short enough to keep as it is.
const MAX_DEPTH: u32 = 48;
/// Pieces longer than this many radians are split before they are compared.
const MAX_PIECE: f64 = std::f64::consts::FRAC_PI_4;

fn lerp(p: Pos, q: Pos, t: f64) -> Pos {
    [p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t]
}

/// Whether the straight line from `p` to `q` in longitude/latitude (either
/// may lie a turn outside ±180) stays within the tolerance of the great
/// elliptic arc between them. A radian of auxiliary-sphere angle spans at
/// most a metres, so the estimate never understates.
fn fits(p: Pos, q: Pos, tolerance: f64) -> bool {
    let (a, b) = (arcs::direction(p), arcs::direction(q));
    if arcs::angle(a, b) > MAX_PIECE {
        return false;
    }
    [0.25, 0.5, 0.75].into_iter().all(|t| {
        let (angle, _) = arcs::nearest_on_arc(arcs::direction(lerp(p, q, t)), a, b);
        SEMI_MAJOR_AXIS_METRES * angle <= tolerance
    })
}

fn invalid(reason: &str) -> GeometryRefusal {
    crate::invalid(format!("Planar geometry: {reason}"))
}

// ------------------------------------------------------------------ import

/// Admits a value whose WGS84 edges are straight in longitude/latitude as the
/// same region with great elliptic edges.
pub fn from_planar(
    value: &Gv,
    declaration: &GeometryDeclaration,
    tolerance: f64,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    validate_tolerance(tolerance)?;
    if declaration.space.frame != CoordinateFrame::Wgs84 {
        return crate::admit_value_with_work(value, declaration, work);
    }
    if declaration.space.dimensions != crate::space::Dimensions::Xy {
        return Err(crate::unsupported_space(
            "Planar boundaries take WGS84 ground values",
        ));
    }
    // The conversion's scratch is released before admission accounts for
    // the value it produced.
    let converted = work.scoped(|work| import(value, tolerance, work))?;
    crate::admit_value_with_work(&converted, declaration, work)
}

fn kind(value: &Gv) -> Result<Kind, GeometryRefusal> {
    match value.get("type") {
        Some(Gv::String(name)) => Kind::parse(name).ok_or_else(|| invalid("unknown type")),
        _ => Err(invalid("missing type")),
    }
}

fn items(value: Option<&Gv>) -> Result<&[Gv], GeometryRefusal> {
    match value {
        Some(Gv::Array(items)) => Ok(items),
        _ => Err(invalid("expected an array")),
    }
}

fn position(value: &Gv) -> Result<Pos, GeometryRefusal> {
    match value {
        Gv::Array(items) if items.len() == 2 => match (number(&items[0]), number(&items[1])) {
            (Some(x), Some(y)) if x.abs() <= 180.0 && y.abs() <= 90.0 => Ok([x, y]),
            _ => Err(invalid("expected WGS84 longitude/latitude")),
        },
        _ => Err(invalid("expected WGS84 longitude/latitude")),
    }
}

fn positions(value: &Gv, work: &mut GeometryWork) -> Result<Vec<Pos>, GeometryRefusal> {
    let items = items(Some(value))?;
    work.charge(items.len())?;
    work.allocate(items.len().saturating_mul(16))?;
    items.iter().map(position).collect()
}

fn array(points: &[Pos]) -> Gv {
    Gv::Array(
        points
            .iter()
            .map(|p| Gv::Array(vec![Gv::Float(p[0]), Gv::Float(p[1])]))
            .collect(),
    )
}

fn rings_value(rings: &[Vec<Pos>]) -> Gv {
    Gv::Array(rings.iter().map(|ring| array(ring)).collect())
}

fn import(value: &Gv, tolerance: f64, work: &mut GeometryWork) -> Result<Gv, GeometryRefusal> {
    let kind = kind(value)?;
    let coordinates = value.get(kind.contents_key());
    Ok(match kind {
        Kind::Point | Kind::MultiPoint => value.clone(),
        Kind::GeometryCollection => geojson(
            kind,
            Gv::Array(
                items(coordinates)?
                    .iter()
                    .map(|member| import(member, tolerance, work))
                    .collect::<Result<_, _>>()?,
            ),
        ),
        Kind::LineString => {
            let line = positions(coordinates.unwrap_or(&Gv::Null), work)?;
            geojson(kind, array(&densify_line(&line, tolerance, work)?))
        },
        Kind::MultiLineString => geojson(
            kind,
            Gv::Array(
                items(coordinates)?
                    .iter()
                    .map(|line| Ok(array(&densify_line(&positions(line, work)?, tolerance, work)?)))
                    .collect::<Result<_, GeometryRefusal>>()?,
            ),
        ),
        Kind::Polygon | Kind::MultiPolygon => {
            let polygons = if kind == Kind::Polygon {
                vec![coordinates.unwrap_or(&Gv::Null)]
            } else {
                items(coordinates)?.iter().collect()
            };
            let mut structured: Vec<Vec<Vec<Pos>>> = Vec::new();
            let mut rings = Vec::new();
            for polygon in polygons {
                let mut part = Vec::new();
                for (index, ring) in items(Some(polygon))?.iter().enumerate() {
                    let mut ring = positions(ring, work)?;
                    if ring.len() < 4 || ring[0] != ring[ring.len() - 1] {
                        return Err(invalid("unclosed ring"));
                    }
                    // Shells run counterclockwise and holes clockwise in
                    // the plane, so the region lies left of each ring.
                    let hole = index > 0;
                    if (planar_area(&ring) < 0.0) != hole {
                        ring.reverse();
                    }
                    let ring = densify_line(&ring, tolerance, work)?;
                    part.push(ring.clone());
                    rings.push((ring, hole));
                }
                structured.push(part);
            }
            let polygons = if rings.iter().flat_map(|(ring, _)| ring).any(|p| on_seam(*p)) {
                join(rings, work)?
            } else {
                structured
            };
            let parts = polygons
                .iter()
                .map(|polygon| rings_value(polygon))
                .collect::<Vec<_>>();
            if kind == Kind::Polygon && parts.len() == 1 {
                geojson(Kind::Polygon, parts.into_iter().next().expect("one part"))
            } else {
                geojson(Kind::MultiPolygon, Gv::Array(parts))
            }
        },
    })
}

/// Twice the signed shoelace area of a closed ring in the plane.
fn planar_area(ring: &[Pos]) -> f64 {
    ring.windows(2)
        .map(|pair| pair[0][0] * pair[1][1] - pair[1][0] * pair[0][1])
        .sum()
}

/// The positions of a planar line with each straight edge split until the
/// arcs between consecutive positions stay within the tolerance of it.
fn densify_line(points: &[Pos], tolerance: f64, work: &mut GeometryWork) -> Result<Vec<Pos>, GeometryRefusal> {
    let mut out = Vec::with_capacity(points.len());
    if let Some(first) = points.first() {
        out.push(*first);
    }
    for pair in points.windows(2) {
        split_planar(pair[0], pair[1], tolerance, 0, &mut out, work)?;
    }
    Ok(out)
}

fn split_planar(
    p: Pos,
    q: Pos,
    tolerance: f64,
    depth: u32,
    out: &mut Vec<Pos>,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    work.charge(4)?;
    if depth >= MAX_DEPTH || fits(p, q, tolerance) {
        work.allocate(16)?;
        out.push(q);
        return Ok(());
    }
    let middle = lerp(p, q, 0.5);
    split_planar(p, middle, tolerance, depth + 1, out, work)?;
    split_planar(middle, q, tolerance, depth + 1, out, work)
}

/// The key of a direction: ±180 and every longitude of a pole name one point.
fn key(p: Pos) -> [u64; 3] {
    arcs::direction(p).map(f64::to_bits)
}

fn on_seam(p: Pos) -> bool {
    p[0].abs() == 180.0 || p[1].abs() == 90.0
}

fn cross(a: Vector, b: Vector) -> Vector {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn dot(a: Vector, b: Vector) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

/// The left turn, in radians, from the arc u→v onto the arc v→w at v.
fn turn(u: Pos, v: Pos, w: Pos) -> f64 {
    let (u, v, w) = (
        arcs::unit(arcs::direction(u)),
        arcs::unit(arcs::direction(v)),
        arcs::unit(arcs::direction(w)),
    );
    let incoming = cross(cross(u, v), v);
    let outgoing = cross(cross(v, w), v);
    libm::atan2(dot(cross(incoming, outgoing), v), dot(incoming, outgoing))
}

/// Joins planar parts into regions on the ellipsoid. Edges along a pole's
/// line have no length; edges along the antimeridian are split at each
/// other's vertices and cancel against their reverse, as the two sides of a
/// cut do. The rest are linked into rings, turning most to the left where
/// rings touch, and grouped into polygons.
fn join(
    rings: Vec<(Vec<Pos>, bool)>,
    work: &mut GeometryWork,
) -> Result<Vec<Vec<Vec<Pos>>>, GeometryRefusal> {
    let mut latitudes: Vec<f64> = rings
        .iter()
        .flat_map(|(ring, _)| ring)
        .filter(|p| on_seam(**p))
        .map(|p| p[1])
        .collect();
    latitudes.sort_by(f64::total_cmp);
    latitudes.dedup();
    let mut edges: Vec<(Pos, Pos, bool)> = Vec::new();
    for (ring, hole) in &rings {
        for pair in ring.windows(2) {
            let (p, q) = (pair[0], pair[1]);
            work.charge(1)?;
            if key(p) == key(q) {
                continue;
            }
            if on_seam(p) && on_seam(q) {
                // Along the antimeridian: split at every seam latitude
                // strictly between the ends.
                let side = if p[1].abs() == 90.0 { q[0] } else { p[0] };
                let mut stops: Vec<f64> = latitudes
                    .iter()
                    .copied()
                    .filter(|lat| *lat > p[1].min(q[1]) && *lat < p[1].max(q[1]))
                    .collect();
                if q[1] < p[1] {
                    stops.reverse();
                }
                let mut from = p;
                for lat in stops {
                    let to = [side, lat];
                    edges.push((from, to, *hole));
                    from = to;
                }
                edges.push((from, q, *hole));
            } else {
                edges.push((p, q, *hole));
            }
        }
    }
    work.allocate(edges.len().saturating_mul(64))?;
    // Cancel each edge against an unmatched reverse.
    let mut open: HashMap<([u64; 3], [u64; 3]), Vec<usize>> = HashMap::new();
    let mut live = vec![true; edges.len()];
    for (index, (p, q, _)) in edges.iter().enumerate() {
        work.charge(1)?;
        if let Some(other) = open.get_mut(&(key(*q), key(*p))).and_then(Vec::pop) {
            live[index] = false;
            live[other] = false;
        } else {
            open.entry((key(*p), key(*q))).or_default().push(index);
        }
    }
    let mut outgoing: HashMap<[u64; 3], Vec<usize>> = HashMap::new();
    for (index, (p, ..)) in edges.iter().enumerate() {
        if live[index] {
            outgoing.entry(key(*p)).or_default().push(index);
        }
    }
    let mut linked = Vec::new();
    for first in 0..edges.len() {
        if !live[first] {
            continue;
        }
        let start = key(edges[first].0);
        let mut ring = vec![edges[first].0];
        let mut hole = edges[first].2;
        let mut current = first;
        loop {
            work.charge(1)?;
            live[current] = false;
            let (from, to, edge_hole) = edges[current];
            hole &= edge_hole;
            ring.push(to);
            if key(to) == start {
                break;
            }
            let next = outgoing
                .get(&key(to))
                .into_iter()
                .flatten()
                .copied()
                .filter(|&index| live[index])
                .max_by(|&a, &b| turn(from, to, edges[a].1).total_cmp(&turn(from, to, edges[b].1)))
                .ok_or_else(|| invalid("rings do not close across the antimeridian"))?;
            current = next;
        }
        let last = ring.len() - 1;
        ring[last] = ring[0];
        if ring.len() >= 4 {
            linked.push((ring, hole));
        }
    }
    group(linked, work)
}

/// Polygons from rings whose region lies on their left. Two rings bound one
/// polygon exactly when each lies in the other's region: a shell and its
/// holes, holes of one shell, or two rings each bounding everything outside
/// the other. A ring that came from a shell is listed first.
fn group(
    rings: Vec<(Vec<Pos>, bool)>,
    work: &mut GeometryWork,
) -> Result<Vec<Vec<Vec<Pos>>>, GeometryRefusal> {
    // Admission checks each ring afterwards; here a ring only locates.
    let mut regions = Vec::with_capacity(rings.len());
    for (ring, _) in &rings {
        regions.push(
            work.scoped(|work| arcs::Ring::of_positions(true, ring, work))?
                .ok_or_else(|| invalid("a ring needs three positions"))?,
        );
    }
    // Whether ring `a` lies in ring `b`'s region, by its first vertex off
    // b's boundary.
    let within = |a: usize, b: usize, work: &mut GeometryWork| {
        for p in &rings[a].0 {
            match work.scoped(|work| regions[b].locate(*p, work))? {
                Rel::Inside => return Ok(true),
                Rel::Outside => return Ok(false),
                Rel::Boundary => {},
            }
        }
        Ok::<_, GeometryRefusal>(false)
    };
    // A ring can lie in another's region only when its first vertex meets
    // that region's box, or the box's image a turn away.
    let boxes: Vec<Vec<[f64; 4]>> = regions
        .iter()
        .map(|region| arcs::turns(true, region.bounds(true)))
        .collect();
    let near = |a: usize, b: usize| {
        let p = rings[a].0[0];
        boxes[b]
            .iter()
            .any(|bounds| crate::boxes_meet(&[p[0], p[1], p[0], p[1]], bounds))
    };
    let mut parent: Vec<usize> = (0..rings.len()).collect();
    fn root(parent: &mut [usize], mut k: usize) -> usize {
        while parent[k] != k {
            parent[k] = parent[parent[k]];
            k = parent[k];
        }
        k
    }
    for a in 0..rings.len() {
        for b in a + 1..rings.len() {
            work.charge(1)?;
            if root(&mut parent, a) != root(&mut parent, b)
                && near(a, b)
                && near(b, a)
                && within(a, b, work)?
                && within(b, a, work)?
            {
                let (x, y) = (root(&mut parent, a), root(&mut parent, b));
                parent[y] = x;
            }
        }
    }
    let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
    for k in 0..rings.len() {
        let r = root(&mut parent, k);
        match groups.iter_mut().find(|(group, _)| *group == r) {
            Some((_, members)) => members.push(k),
            None => groups.push((r, vec![k])),
        }
    }
    let mut rings: Vec<Option<(Vec<Pos>, bool)>> = rings.into_iter().map(Some).collect();
    Ok(groups
        .into_iter()
        .map(|(_, mut members)| {
            if let Some(shell) = members
                .iter()
                .position(|&k| !rings[k].as_ref().expect("ring").1)
            {
                members.swap(0, shell);
            }
            members
                .into_iter()
                .map(|k| rings[k].take().expect("ring").0)
                .collect()
        })
        .collect())
}

// ------------------------------------------------------------------ export

/// A value with straight longitude/latitude edges describing an admitted
/// geometry within `tolerance` metres, cut at the antimeridian.
pub fn to_planar(
    geometry: &Geometry,
    tolerance: f64,
    work: &mut GeometryWork,
) -> Result<Gv, GeometryRefusal> {
    validate_tolerance(tolerance)?;
    if geometry.space.frame != CoordinateFrame::Wgs84 {
        return Ok(Gv::from(geometry));
    }
    if geometry.heights.is_some() {
        return Err(crate::unsupported_space(
            "Planar boundaries take WGS84 ground values",
        ));
    }
    let polygons =
        |parts: Vec<Vec<Vec<Pos>>>| Gv::Array(parts.iter().map(|part| rings_value(part)).collect());
    Ok(match (geometry.kind, &geometry.coords) {
        (_, Coords::Collection(members)) => geojson(
            Kind::GeometryCollection,
            Gv::Array(
                members
                    .iter()
                    .map(|member| to_planar(member, tolerance, work))
                    .collect::<Result<_, _>>()?,
            ),
        ),
        (Kind::LineString, Coords::Points(line)) => {
            let parts = cut_line(line, tolerance, work)?;
            if parts.len() == 1 {
                geojson(Kind::LineString, array(&parts[0]))
            } else {
                geojson(Kind::MultiLineString, rings_value(&parts))
            }
        },
        (Kind::MultiLineString, Coords::Lines(lines)) => {
            let mut parts = Vec::new();
            for line in lines {
                parts.extend(cut_line(line, tolerance, work)?);
            }
            geojson(Kind::MultiLineString, rings_value(&parts))
        },
        (Kind::Polygon, Coords::Lines(rings)) => {
            let parts = cut_polygon(rings, tolerance, work)?;
            if parts.len() == 1 {
                geojson(Kind::Polygon, rings_value(&parts[0]))
            } else {
                geojson(Kind::MultiPolygon, polygons(parts))
            }
        },
        (Kind::MultiPolygon, Coords::Polygons(members)) => {
            let mut parts = Vec::new();
            for polygon in members {
                parts.extend(cut_polygon(polygon, tolerance, work)?);
            }
            geojson(Kind::MultiPolygon, polygons(parts))
        },
        _ => Gv::from(geometry),
    })
}

/// `lon` shifted by whole turns to lie within half a turn of `near`.
fn unwrap_near(lon: f64, near: f64) -> f64 {
    let shift = ((near - lon) / 360.0).round() * 360.0;
    lon + shift
}

/// A line's vertices with continuous longitudes: each arc densified until
/// planar chords stay within the tolerance, and an arc through a pole drawn up
/// its arriving meridian and down its leaving one along the pole's line.
fn unwrapped(points: &[Pos], tolerance: f64, work: &mut GeometryWork) -> Result<Vec<Pos>, GeometryRefusal> {
    let mut out: Vec<Pos> = Vec::with_capacity(points.len());
    let Some(first) = points.first() else {
        return Ok(out);
    };
    out.push(*first);
    for pair in points.windows(2) {
        let (p, q) = (pair[0], pair[1]);
        let at = *out.last().expect("a vertex");
        let bounds = arcs::arc_bounds(p, q);
        let pole = if bounds[3] >= 90.0 {
            Some(90.0)
        } else if bounds[1] <= -90.0 {
            Some(-90.0)
        } else {
            None
        };
        match pole {
            Some(lat) if p[1].abs() != 90.0 && q[1].abs() != 90.0 => {
                // Over the pole: up p's meridian, along the pole's line,
                // down q's meridian.
                let leave = unwrap_near(q[0], at[0]);
                out.extend([[at[0], lat], [leave, lat], [leave, q[1]]]);
            },
            _ if p[1].abs() == 90.0 => {
                let leave = unwrap_near(q[0], at[0]);
                if leave != at[0] {
                    out.push([leave, p[1]]);
                }
                out.push([leave, q[1]]);
            },
            _ if q[1].abs() == 90.0 => out.push([at[0], q[1]]),
            _ => {
                let end = [unwrap_near(q[0], at[0]), q[1]];
                split_arc(at, end, tolerance, 0, &mut out, work)?;
            },
        }
        work.charge(4)?;
        work.allocate(48)?;
    }
    Ok(out)
}

fn split_arc(
    p: Pos,
    q: Pos,
    tolerance: f64,
    depth: u32,
    out: &mut Vec<Pos>,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    work.charge(4)?;
    if depth >= MAX_DEPTH || fits(p, q, tolerance) {
        work.allocate(16)?;
        out.push(q);
        return Ok(());
    }
    let middle = arcs::position(arcs::interpolate(
        arcs::direction(p),
        arcs::direction(q),
        0.5,
    ));
    let middle = [unwrap_near(middle[0], p[0]), middle[1]];
    split_arc(p, middle, tolerance, depth + 1, out, work)?;
    split_arc(middle, q, tolerance, depth + 1, out, work)
}

/// The turn holding a continuous longitude: 0 for [-180, 180].
fn sheet(lon: f64) -> f64 {
    ((lon + 180.0) / 360.0).floor()
}

/// Splits continuous vertices where they cross a line 180 + 360k degrees,
/// at the crossing latitude on the edge's arc; each piece is returned in
/// [-180, 180], its ends on the cut written as ±180.
fn pieces(vertices: &[Pos]) -> Vec<Vec<Pos>> {
    let mut pieces: Vec<Vec<Pos>> = Vec::new();
    let mut current: Vec<Pos> = Vec::new();
    let mut current_sheet = None;
    let push = |piece: &mut Vec<Pos>, point: Pos, turn: f64| {
        let point = [point[0] - 360.0 * turn, point[1]];
        if piece.last() != Some(&point) {
            piece.push(point);
        }
    };
    for pair in vertices.windows(2) {
        let (p, q) = (pair[0], pair[1]);
        if p == q {
            continue;
        }
        let (low, high) = (p[0].min(q[0]), p[0].max(q[0]));
        let mut stops = vec![p];
        let first = ((low - 180.0) / 360.0).ceil() as i64;
        let last = ((high - 180.0) / 360.0).floor() as i64;
        let mut cuts: Vec<Pos> = (first..=last)
            .map(|k| 180.0 + 360.0 * k as f64)
            .filter(|line| *line > low && *line < high)
            .map(|line| [line, crossing_latitude(p, q, line)])
            .collect();
        if q[0] < p[0] {
            cuts.reverse();
        }
        stops.extend(cuts);
        stops.push(q);
        for piece in stops.windows(2) {
            let (a, b) = (piece[0], piece[1]);
            let turn = sheet((a[0] + b[0]) / 2.0);
            if current_sheet != Some(turn) {
                if current.len() >= 2 {
                    pieces.push(std::mem::take(&mut current));
                }
                current.clear();
                current_sheet = Some(turn);
                push(&mut current, a, turn);
            }
            push(&mut current, b, turn);
        }
    }
    if current.len() >= 2 {
        pieces.push(current);
    }
    pieces
}

/// The latitude where the arc p→q meets the meridian of the continuous
/// longitude `line`.
fn crossing_latitude(p: Pos, q: Pos, line: f64) -> f64 {
    let normal = cross(arcs::direction(p), arcs::direction(q));
    let meridian = arcs::direction([line, 0.0]);
    // The meridian's plane holds the z axis and its equatorial direction.
    let plane = cross(meridian, [0.0, 0.0, 1.0]);
    let mut point = cross(normal, plane);
    if dot(point, meridian) < 0.0 {
        point = point.map(|x| -x);
    }
    arcs::position(point)[1]
}

fn cut_line(
    line: &[Pos],
    tolerance: f64,
    work: &mut GeometryWork,
) -> Result<Vec<Vec<Pos>>, GeometryRefusal> {
    Ok(pieces(&unwrapped(line, tolerance, work)?))
}

/// Position along the frame's boundary walked counterclockwise: up the east
/// side, west along the top, down the west side, east along the bottom.
fn frame_parameter(p: Pos) -> f64 {
    if p[0] == 180.0 && p[1] < 90.0 {
        p[1] + 90.0
    } else if p[1] == 90.0 {
        180.0 + (180.0 - p[0])
    } else if p[0] == -180.0 {
        540.0 + (90.0 - p[1])
    } else {
        720.0 + (p[0] + 180.0)
    }
}

/// The frame's corners and their parameters.
const CORNERS: [(f64, Pos); 4] = [
    (180.0, [180.0, 90.0]),
    (540.0, [-180.0, 90.0]),
    (720.0, [-180.0, -90.0]),
    (0.0, [180.0, -90.0]),
];

fn planar_contains(ring: &[Pos], p: Pos) -> bool {
    let mut inside = false;
    for pair in ring.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if (a[1] > p[1]) != (b[1] > p[1])
            && p[0] < a[0] + (p[1] - a[1]) * (b[0] - a[0]) / (b[1] - a[1])
        {
            inside = !inside;
        }
    }
    inside
}

/// An admitted polygon as planar polygons: rings crossing the antimeridian
/// are cut there and the pieces closed along the frame with the region on
/// their left; holes join the piece holding them.
fn cut_polygon(
    rings: &[Vec<Pos>],
    tolerance: f64,
    work: &mut GeometryWork,
) -> Result<Vec<Vec<Vec<Pos>>>, GeometryRefusal> {
    let mut open: Vec<Vec<Pos>> = Vec::new();
    let mut closed: Vec<Vec<Pos>> = Vec::new();
    for ring in rings {
        let vertices = unwrapped(ring, tolerance, work)?;
        let mut parts = pieces(&vertices);
        if parts.len() == 1
            && parts[0].first() == parts[0].last()
            && sheet(vertices[0][0]) == sheet(vertices[vertices.len() - 1][0])
        {
            closed.push(parts.pop().expect("one piece"));
            continue;
        }
        // The ring's first and last pieces are one piece when the ring
        // starts away from the cut.
        if parts.len() >= 2 {
            let first = &parts[0];
            let last = &parts[parts.len() - 1];
            if last.last() == first.first() && first[0][0].abs() != 180.0 {
                let mut joined = parts.pop().expect("last piece");
                joined.extend_from_slice(&parts[0][1..]);
                parts[0] = joined;
            }
        }
        open.extend(parts);
    }
    work.allocate(open.len().saturating_mul(64))?;
    let mut shells: Vec<Vec<Pos>> = Vec::new();
    let mut used = vec![false; open.len()];
    for first in 0..open.len() {
        if used[first] {
            continue;
        }
        let mut ring: Vec<Pos> = Vec::new();
        let mut current = first;
        loop {
            work.charge(1)?;
            used[current] = true;
            for p in &open[current] {
                if ring.last() != Some(p) {
                    ring.push(*p);
                }
            }
            let end = frame_parameter(*open[current].last().expect("piece end"));
            // The next piece to start counterclockwise along the frame.
            let next = (0..open.len())
                .filter(|&index| !used[index] || index == first)
                .min_by(|&a, &b| {
                    let gap =
                        |index: usize| (frame_parameter(open[index][0]) - end).rem_euclid(1080.0);
                    gap(a).total_cmp(&gap(b))
                })
                .ok_or_else(|| invalid("pieces do not close along the antimeridian"))?;
            let gap = (frame_parameter(open[next][0]) - end).rem_euclid(1080.0);
            let mut corners: Vec<(f64, Pos)> = CORNERS
                .iter()
                .map(|(at, point)| ((at - end).rem_euclid(1080.0), *point))
                .filter(|(along, _)| *along > 0.0 && *along < gap)
                .collect();
            corners.sort_by(|a, b| a.0.total_cmp(&b.0));
            for (_, point) in corners {
                if ring.last() != Some(&point) {
                    ring.push(point);
                }
            }
            if next == first {
                let start = ring[0];
                if ring.last() != Some(&start) {
                    ring.push(start);
                }
                break;
            }
            current = next;
        }
        shells.push(ring);
    }
    let mut holes = Vec::new();
    for ring in closed {
        if planar_area(&ring) >= 0.0 {
            shells.push(ring);
        } else {
            holes.push(ring);
        }
    }
    let mut polygons: Vec<Vec<Vec<Pos>>> = shells.into_iter().map(|shell| vec![shell]).collect();
    for hole in holes {
        let probe = lerp(hole[0], hole[1], 0.5);
        match polygons
            .iter_mut()
            .find(|polygon| planar_contains(&polygon[0], probe))
        {
            Some(polygon) => polygon.push(hole),
            None => polygons.push(vec![
                vec![
                    [-180.0, -90.0],
                    [180.0, -90.0],
                    [180.0, 90.0],
                    [-180.0, 90.0],
                    [-180.0, -90.0],
                ],
                hole,
            ]),
        }
    }
    Ok(polygons)
}

#[cfg(test)]
#[path = "planar_tests.rs"]
mod tests;
