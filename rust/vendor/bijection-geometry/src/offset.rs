//! The metric constructions behind `realize`: `buffer` (the offset
//! `{p : sep(p, of) <= r}`) and the re-expression of a value through a map
//! of positions (`densify` and `recast`).
//!
//! A buffer is the union of `of`'s areas and, around each of its pieces (a
//! point, or an edge), the stadium of positions within `r` of it: two offset
//! curves and two caps, as chords between offset positions. A chord of a
//! circle or of an offset curve lies inside it and its ends on it, so a
//! stadium is held inside the exact set (`inner`) with its offset positions
//! at `r`, and around it (`outer`) with them at `r / cos(π/N)`. Each stadium
//! is measured: its boundary's distance to its piece, sampled at every vertex
//! and minimized along every chord, sets its Hausdorff bound and corrects its
//! radius until its side holds by a margin larger than the rounding of the
//! union's crossings. The union of stadiums within δ of theirs, on one side,
//! is within δ of the buffer on that side.
//!
//! A re-expression maps each edge's positions and subdivides the edge until
//! the target edge between mapped positions stays within the bound of the
//! mapped source edge, sampled at its quarter points.
use serde_json::{
    json,
    Value,
};

use crate::{
    overlay::{
        self,
        Piece,
    },
    CoordinateFrame,
    Geometry,
    GeometryRefusal,
    GeometryRefusalCode,
    GeometrySpace,
    GeometryWork,
    Pos,
};

/// Which side of the exact set a realized value holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Outer,
    Inner,
    Nearest,
}

impl Side {
    pub(crate) fn parse(side: &str) -> Option<Self> {
        match side {
            "outer" => Some(Self::Outer),
            "inner" => Some(Self::Inner),
            "nearest" => Some(Self::Nearest),
            _ => None,
        }
    }

    /// The side a monotone-decreasing operand must hold (difference's right).
    pub(crate) fn opposite(self) -> Self {
        match self {
            Self::Outer => Self::Inner,
            Self::Inner => Self::Outer,
            Self::Nearest => Self::Nearest,
        }
    }
}

pub(crate) fn bound_refusal(message: impl std::fmt::Display) -> GeometryRefusal {
    GeometryRefusal {
        code: GeometryRefusalCode::SpatialPrecisionLoss,
        message: format!("RealizeBound: {message}"),
        bound: None,
    }
}

fn refused(message: impl std::fmt::Display) -> GeometryRefusal {
    crate::invalid(format!("realize: {message}"))
}

/// The most vertices a buffer spends on a full turn.
const MAX_TURN: usize = 4096;
/// The most stations a buffer spends along one edge.
const MAX_STATIONS: usize = 1 << 16;

/// The position `metres` from `p` at `azimuth` degrees (clockwise from
/// north, or from +y): along the geodesic in WGS84, straight in a plane.
fn offset(space: &GeometrySpace, p: Pos, azimuth: f64, metres: f64) -> Pos {
    match &space.frame {
        CoordinateFrame::Wgs84 => {
            let to = crate::geodesy::direct(p[1], p[0], azimuth, metres);
            [to.lon2, to.lat2]
        },
        CoordinateFrame::Cartesian { unit, .. } => {
            let (sin, cos) = crate::geodesy::sincosd(azimuth);
            let units = metres / unit.metres_per_unit.get();
            [p[0] + units * sin, p[1] + units * cos]
        },
    }
}

/// The length of a piece in metres and its forward azimuth at `from`
/// towards `to`.
fn towards(space: &GeometrySpace, from: Pos, to: Pos) -> (f64, f64, f64) {
    match &space.frame {
        CoordinateFrame::Wgs84 => {
            let solved = crate::geodesy::inverse(from[1], from[0], to[1], to[0]);
            (solved.s12, solved.azi1, solved.azi2)
        },
        CoordinateFrame::Cartesian { unit, .. } => {
            let (dx, dy) = (to[0] - from[0], to[1] - from[1]);
            let azimuth = libm::atan2(dx, dy).to_degrees();
            (libm::hypot(dx, dy) * unit.metres_per_unit.get(), azimuth, azimuth)
        },
    }
}

/// The distance in metres from `x` to a piece.
fn distance(space: &GeometrySpace, x: Pos, piece: (Pos, Pos), work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
    crate::predicates::space_segment_distance(space, (x, x), piece, f64::INFINITY, work)
}

/// The least and greatest distance from a closed ring's chords to a piece:
/// the greatest at the vertices, the least also minimized along each chord.
fn measure(space: &GeometrySpace, ring: &[Pos], piece: (Pos, Pos), work: &mut GeometryWork) -> Result<(f64, f64), GeometryRefusal> {
    const R: f64 = 0.618_033_988_749_894_9;
    let (mut least, mut most) = (f64::INFINITY, 0.0_f64);
    for pair in ring.windows(2) {
        work.charge(40)?;
        let chord = (pair[0], pair[1]);
        let at = |t: f64, work: &mut GeometryWork| distance(space, crate::generator::along(space, chord, t), piece, work);
        let ends = (at(0.0, work)?, at(1.0, work)?);
        most = most.max(ends.0).max(ends.1);
        let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
        let (mut x1, mut x2) = (hi - R * (hi - lo), lo + R * (hi - lo));
        let (mut f1, mut f2) = (at(x1, work)?, at(x2, work)?);
        for _ in 0..36 {
            if f1 <= f2 {
                hi = x2;
                x2 = x1;
                f2 = f1;
                x1 = hi - R * (hi - lo);
                f1 = at(x1, work)?;
            } else {
                lo = x1;
                x1 = x2;
                f1 = f2;
                x2 = lo + R * (hi - lo);
                f2 = at(x2, work)?;
            }
        }
        least = least.min(ends.0).min(ends.1).min(f1).min(f2);
    }
    Ok((least, most))
}

/// The stadium of positions within `radius` of a piece, counterclockwise:
/// `turn` vertices on a full turn, `stations` chords along an edge.
fn stadium(space: &GeometrySpace, piece: (Pos, Pos), radius: f64, turn: usize, stations: usize, work: &mut GeometryWork) -> Result<Vec<Pos>, GeometryRefusal> {
    let (a, b) = piece;
    let step = 360.0 / turn as f64;
    let mut ring = Vec::new();
    if overlay::key(&space.frame, a) == overlay::key(&space.frame, b) {
        work.charge(turn)?;
        work.allocate(turn.saturating_mul(16))?;
        for j in 0..turn {
            ring.push(offset(space, a, -(j as f64) * step, radius));
        }
    } else {
        let half = turn.div_ceil(2);
        work.charge(stations.saturating_mul(4).saturating_add(turn))?;
        work.allocate(stations.saturating_add(half).saturating_mul(48))?;
        let (_, start_azimuth, end_azimuth) = towards(space, a, b);
        let stops: Vec<(Pos, f64)> = (0..=stations)
            .map(|i| {
                if i == 0 {
                    (a, start_azimuth)
                } else if i == stations {
                    (b, end_azimuth)
                } else {
                    let p = crate::generator::along(space, piece, i as f64 / stations as f64);
                    (p, towards(space, p, b).1)
                }
            })
            .collect();
        // Right side forward, the cap round b, the left side back, the cap
        // round a: decreasing azimuth turns counterclockwise.
        for &(p, azimuth) in &stops {
            ring.push(offset(space, p, azimuth + 90.0, radius));
        }
        for j in 1..half {
            ring.push(offset(space, b, end_azimuth + 90.0 - 180.0 * j as f64 / half as f64, radius));
        }
        for &(p, azimuth) in stops.iter().rev() {
            ring.push(offset(space, p, azimuth - 90.0, radius));
        }
        for j in 1..half {
            ring.push(offset(space, a, start_azimuth - 90.0 - 180.0 * j as f64 / half as f64, radius));
        }
    }
    if !ring.iter().flatten().all(|x| x.is_finite()) {
        return Err(refused("a buffer leaves the numerical domain"));
    }
    ring.dedup_by(|x, y| overlay::key(&space.frame, *x) == overlay::key(&space.frame, *y));
    ring.push(ring[0]);
    Ok(ring)
}

/// The pieces of a value a buffer offsets: its points, and the edges of its
/// lines and rings.
fn pieces(geometry: &Geometry) -> Vec<(Pos, Pos)> {
    let mut out = Vec::new();
    if matches!(geometry.kind, crate::Kind::Point | crate::Kind::MultiPoint) {
        geometry.for_each(|p| out.push((*p, *p)));
        return out;
    }
    if let crate::Coords::Collection(members) = &geometry.coords {
        return members.iter().flat_map(pieces).collect();
    }
    for line in geometry.lines() {
        out.extend(line.windows(2).map(|w| (w[0], w[1])));
        if line.len() == 1 {
            out.push((line[0], line[0]));
        }
    }
    for polygon in geometry.polygons() {
        for ring in polygon {
            out.extend(ring.windows(2).map(|w| (w[0], w[1])));
        }
    }
    out
}

/// `{p : sep(p, of) <= metres}` within `budget` metres on `side`: the
/// admitted value and the Hausdorff bound it meets.
pub(crate) fn buffer(space: &GeometrySpace, of: &Geometry, metres: f64, budget: f64, side: Side, work: &mut GeometryWork) -> Result<(Geometry, f64), GeometryRefusal> {
    if !(metres.is_finite() && metres >= 0.0) {
        return Err(refused("a buffer distance is finite and non-negative metres"));
    }
    if metres == 0.0 || of.is_empty() {
        // The closed value is its own zero buffer.
        return Ok((of.clone(), 0.0));
    }
    if !space.is_ground() {
        return Err(crate::unsupported_space("a buffer is defined on XY positions"));
    }
    if budget <= 0.0 {
        return Err(bound_refusal("a buffer of a positive distance is curved; it needs a positive deviation bound"));
    }
    // Half the bound to the chords' sag; the rest to measurement.
    let sag = (budget / 2.0).min(metres);
    let turn = if sag >= metres {
        8
    } else {
        let needed = core::f64::consts::PI / libm::acos(1.0 - sag / metres);
        if !(needed.is_finite() && needed <= MAX_TURN as f64) {
            return Err(bound_refusal(format!("a buffer of {metres} m within {budget} m needs more than {MAX_TURN} vertices a turn")));
        }
        (needed.ceil() as usize).max(8)
    };
    // The side holds by more than any crossing's rounding.
    let margin = (1e-6_f64).min(metres * 1e-3).min(budget / 4.0);
    let base = match side {
        Side::Inner => metres - margin,
        Side::Nearest => metres,
        Side::Outer => metres / libm::cos(core::f64::consts::PI / turn as f64) + margin,
    };
    let earth = crate::geodesy::SEMI_MAJOR_AXIS_METRES;
    let mut components = Vec::new();
    let mut deviation = 0.0_f64;
    for piece in pieces(of) {
        let (length, ..) = towards(space, piece.0, piece.1);
        let reach = earth * libm::sqrt(8.0 * sag / base.max(sag));
        let mut stations = ((length / reach).ceil() as usize).clamp(1, MAX_STATIONS);
        let mut radius = base;
        let mut settled = None;
        for _ in 0..8 {
            let ring = stadium(space, piece, radius, turn, stations, work)?;
            let (least, most) = measure(space, &ring, piece, work)?;
            let off = match side {
                Side::Outer => most - metres,
                Side::Inner => metres - least,
                Side::Nearest => (most - metres).max(metres - least),
            };
            if off > budget {
                if stations >= MAX_STATIONS {
                    break;
                }
                stations = (stations * 4).min(MAX_STATIONS);
                continue;
            }
            match side {
                Side::Outer if least < metres + margin / 2.0 => radius += metres + margin - least,
                Side::Inner if most > metres - margin / 2.0 => radius -= most - metres + margin,
                _ => {
                    settled = Some((ring, off));
                    break;
                },
            }
            if radius <= 0.0 {
                break;
            }
        }
        let Some((ring, off)) = settled else {
            return Err(bound_refusal(format!("a buffer of {metres} m does not meet the bound of {budget} m on its side")));
        };
        deviation = deviation.max(off);
        components.push(overlay::construct(space, json!({ "type": "Polygon", "coordinates": [ring] }), work)?);
    }
    for polygon in of.polygons() {
        components.push(overlay::construct(space, json!({ "type": "Polygon", "coordinates": polygon }), work)?);
    }
    // Dissolve: the exposed boundary of the union, rings reassembled.
    let members: Vec<&Geometry> = components.iter().collect();
    let mut found = Vec::new();
    crate::topology::visit_boundary(members[0], &members, crate::topology::BoundaryDomain::Areas, work, &mut |piece, work| {
        work.allocate(std::mem::size_of::<Piece>())?;
        let index = found.len();
        let piece = Piece {
            start: piece.start,
            end: piece.end,
            start_error_m: piece.rounding_error_m,
            end_error_m: piece.rounding_error_m,
            origin: (0, index),
        }
        .oriented(piece.interior_left);
        found.push(piece);
        Ok(false)
    })?;
    let mut rounding = 0.0_f64;
    let rings = overlay::rings(&space.frame, found, &mut rounding, work)?;
    if side != Side::Nearest && rounding > margin / 2.0 {
        return Err(bound_refusal("the buffer's crossings round past its side's margin"));
    }
    let polygons = overlay::polygons(space, rings, work)?;
    let value = overlay::assemble(&polygons, &[], &[]);
    Ok((overlay::construct(space, value, work)?, deviation + rounding))
}

/// Re-express a value from `source` into `target` through `map`,
/// subdividing each edge until the target edge stays within `budget` of the
/// mapped source edge. Answers the value (admitted in `target`) and the
/// bound met, plus `numeric_m` for the map's own error.
pub(crate) fn reexpress(
    source: &GeometrySpace,
    target: &GeometrySpace,
    of: &Geometry,
    map: &mut dyn FnMut(Pos, &mut GeometryWork) -> Result<Pos, GeometryRefusal>,
    budget: f64,
    numeric_m: f64,
    work: &mut GeometryWork,
) -> Result<(Geometry, f64), GeometryRefusal> {
    if !source.is_ground() || !target.is_ground() {
        return Err(crate::unsupported_space("a re-expression is defined on XY positions"));
    }
    let json: Value = serde_json::from_str(&of.js_json()).map_err(crate::invalid)?;
    let mut deviation = 0.0_f64;
    let value = walk(source, target, &json, map, budget, &mut deviation, work)?;
    let deviation = deviation + numeric_m;
    if deviation > budget {
        return Err(bound_refusal(format!("the re-expressed value is within {deviation} m, beyond the bound of {budget} m")));
    }
    Ok((overlay::construct(target, value, work)?, deviation))
}

type Map<'a> = dyn FnMut(Pos, &mut GeometryWork) -> Result<Pos, GeometryRefusal> + 'a;

fn position(value: &Value) -> Result<Pos, GeometryRefusal> {
    match value.as_array().map(Vec::as_slice) {
        Some([x, y, ..]) => Ok([x.as_f64().unwrap_or(f64::NAN), y.as_f64().unwrap_or(f64::NAN)]),
        _ => Err(refused("a position has two ordinates")),
    }
}

fn walk(source: &GeometrySpace, target: &GeometrySpace, value: &Value, map: &mut Map<'_>, budget: f64, deviation: &mut f64, work: &mut GeometryWork) -> Result<Value, GeometryRefusal> {
    let kind = value.get("type").and_then(Value::as_str).unwrap_or_default();
    let coordinates = value.get("coordinates").cloned().unwrap_or(Value::Null);
    let positions = |list: &Value| -> Result<Vec<Pos>, GeometryRefusal> {
        list.as_array().map_or(Ok(Vec::new()), |items| items.iter().map(position).collect())
    };
    Ok(match kind {
        "GeometryCollection" => {
            let members = value.get("geometries").and_then(Value::as_array).cloned().unwrap_or_default();
            let mut out = Vec::with_capacity(members.len());
            for member in &members {
                out.push(walk(source, target, member, map, budget, deviation, work)?);
            }
            json!({ "type": kind, "geometries": out })
        },
        "Point" => {
            if coordinates.as_array().is_none_or(Vec::is_empty) {
                value.clone()
            } else {
                json!({ "type": kind, "coordinates": map(position(&coordinates)?, work)? })
            }
        },
        "MultiPoint" => {
            let mut out = Vec::new();
            for p in positions(&coordinates)? {
                out.push(map(p, work)?);
            }
            json!({ "type": kind, "coordinates": out })
        },
        "LineString" => json!({ "type": kind, "coordinates": line(source, target, &positions(&coordinates)?, map, budget, deviation, work)? }),
        "MultiLineString" | "Polygon" => {
            let mut out = Vec::new();
            for part in coordinates.as_array().cloned().unwrap_or_default() {
                out.push(line(source, target, &positions(&part)?, map, budget, deviation, work)?);
            }
            json!({ "type": kind, "coordinates": out })
        },
        "MultiPolygon" => {
            let mut out = Vec::new();
            for polygon in coordinates.as_array().cloned().unwrap_or_default() {
                let mut rings = Vec::new();
                for ring in polygon.as_array().cloned().unwrap_or_default() {
                    rings.push(line(source, target, &positions(&ring)?, map, budget, deviation, work)?);
                }
                out.push(rings);
            }
            json!({ "type": kind, "coordinates": out })
        },
        _ => return Err(refused("an unknown geometry type")),
    })
}

/// A line's mapped positions, each edge subdivided until within budget.
fn line(source: &GeometrySpace, target: &GeometrySpace, positions: &[Pos], map: &mut Map<'_>, budget: f64, deviation: &mut f64, work: &mut GeometryWork) -> Result<Vec<Pos>, GeometryRefusal> {
    let mut out = Vec::new();
    let Some(&first) = positions.first() else {
        return Ok(out);
    };
    out.push(map(first, work)?);
    for pair in positions.windows(2) {
        let edge = (pair[0], pair[1]);
        let end = map(pair[1], work)?;
        // A stack of open intervals: (t0, image at t0, t1, image at t1).
        let start = *out.last().expect("a first position");
        let mut stack = vec![(0.0_f64, start, 1.0_f64, end, 0u32)];
        let mut emitted = Vec::new();
        while let Some((t0, p0, t1, p1, depth)) = stack.pop() {
            work.charge(8)?;
            let mut worst = 0.0_f64;
            let mut middle = p0;
            for (i, t) in [0.25, 0.5, 0.75].into_iter().enumerate() {
                let s = t0 + (t1 - t0) * t;
                let image = map(crate::generator::along(source, edge, s), work)?;
                if i == 1 {
                    middle = image;
                }
                worst = worst.max(crate::predicates::space_segment_distance(target, (image, image), (p0, p1), f64::INFINITY, work)?);
            }
            if worst > budget / 2.0 && depth < 30 {
                let tm = (t0 + t1) / 2.0;
                // Later half first on the stack, so the earlier pops first.
                stack.push((tm, middle, t1, p1, depth + 1));
                stack.push((t0, p0, tm, middle, depth + 1));
                continue;
            }
            *deviation = deviation.max(worst);
            emitted.push(p1);
        }
        work.allocate(emitted.len().saturating_mul(16))?;
        out.extend(emitted);
    }
    Ok(out)
}
