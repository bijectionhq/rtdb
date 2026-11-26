//! The geo algebra's generators (`@bijectionhq/geometry`'s `./algebra`): the
//! semantic core's `generator` operation. Each request is one generator over
//! values in one declared space; the reply is `{ value, usage }` or
//! `{ refusal, usage }`, as the `geometry` operation's.
//!
//! - `relate`: the exact DE-9IM matrix of two terms that realize exactly.
//! - `inverse`: the geodesic inverse problem, with the area under the geodesic.
//! - `direct`: the geodesic direct problem.
//! - `sep`: the separation of two such terms (or a value's boundary) and a
//!   witness pair attaining it.
//! - `chart`: positions through the closed chart table (UTM, UPS, cube face,
//!   affine) and back.
//! - `realize`: a term as an admitted value, with the Hausdorff bound it
//!   meets (0 iff it creates no coordinate) and the side it holds.
//!   - `value` is itself; `union` the collection of its operands.
//!   - `intersection` and `difference` are the gated overlay (`overlay`):
//!     crossings round to binary64, reported.
//!   - `boundary` of areas cuts their edges where they cross and rounds those
//!     crossings, reported.
//!   - `buffer` is the offset (`offset::buffer`), built on the requested side
//!     (`outer` covers the exact buffer, `inner` lies in it) and measured.
//!   - `densify` re-expresses a value in another edge model and `recast` in
//!     another space through a chart, subdivided until within the bound.
//!
//!   A rounded result keeps the `nearest` side only, except a buffer, which
//!   holds the side it was built for. An overlay, boundary, densification or
//!   recast of a rounded operand has no bound from it and is refused; a
//!   buffer carries its operand's bound. Past the bound or the side the core
//!   refuses `SpatialPrecisionLoss` (`RealizeBound`).
use serde::Deserialize;
use serde_json::Value;

use crate::{
    binding::GeometryAllowance,
    offset::Side,
    overlay::Overlay,
    CoordinateFrame,
    GeometryDeclaration,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Gv,
    Pos,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum GeneratorRequest {
    Relate {
        space: GeometrySpace,
        left: Term,
        right: Term,
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
    Inverse {
        space: GeometrySpace,
        from: [f64; 2],
        to: [f64; 2],
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
    Direct {
        space: GeometrySpace,
        from: [f64; 2],
        azimuth_degrees: f64,
        distance_m: f64,
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
    Sep {
        space: GeometrySpace,
        left: Term,
        right: Term,
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
    Chart {
        space: GeometrySpace,
        chart: Chart,
        direction: Direction,
        positions: Vec<[f64; 2]>,
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
    Realize {
        space: GeometrySpace,
        term: Term,
        size: String,
        max_deviation_m: f64,
        side: String,
        #[serde(default)]
        allowance: Option<GeometryAllowance>,
    },
}

/// A term over admitted values. Only `value` leaves are evaluated here.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Term {
    Value { value: Value },
    Union { left: Box<Term>, right: Box<Term> },
    Intersection { left: Box<Term>, right: Box<Term> },
    Difference { left: Box<Term>, right: Box<Term> },
    Boundary { of: Box<Term> },
    Buffer { of: Box<Term>, metres: f64 },
    Densify { of: Box<Term>, segments: Value },
    Recast { of: Box<Term>, target: Value, via: Option<Value> },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Hemisphere {
    North,
    South,
}

impl From<Hemisphere> for crate::mgrs::Hemisphere {
    fn from(value: Hemisphere) -> Self {
        match value {
            Hemisphere::North => Self::North,
            Hemisphere::South => Self::South,
        }
    }
}

/// A chart of the closed table.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Chart {
    Utm { zone: u8, hemisphere: Hemisphere },
    Ups { hemisphere: Hemisphere },
    Cube { face: u8 },
    Affine { matrix: [[f64; 3]; 2] },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Forward,
    Inverse,
}

fn refused(message: impl std::fmt::Display) -> GeometryRefusal {
    crate::invalid(message)
}

fn wgs84(space: &GeometrySpace, operation: &str) -> Result<(), GeometryRefusal> {
    space.validate().map_err(crate::unsupported_space)?;
    if space.frame != CoordinateFrame::Wgs84 || !space.is_ground() {
        return Err(crate::unsupported_space(format!("{operation} is defined on WGS84 XY positions")));
    }
    Ok(())
}

/// An operand of `relate` or `sep`: a value leaf, admitted in `space` at the
/// large tier (generator operands are values a caller already admitted), or
/// a term that realizes exactly (with no new coordinate).
fn value_of(space: &GeometrySpace, term: &Term, work: &mut GeometryWork) -> Result<crate::Geometry, GeometryRefusal> {
    let Term::Value { value } = term else {
        let realized = realize_term(space, term, 0.0, Side::Nearest, work)?;
        if realized.deviation_m > 0.0 {
            return Err(refused("this term creates coordinates; realize it within a bound and relate its value"));
        }
        return Ok(realized.geometry);
    };
    let declaration = GeometryDeclaration::parse(space.clone(), "large").map_err(crate::unsupported_space)?;
    crate::admit_value_with_work(&Gv::from(value), &declaration, work)
}

/// The area under the geodesic of `inverse` from `from`, in square metres:
/// the integral of the authalic zone area against longitude along it (the
/// counterclockwise area of the quadrilateral the geodesic, its endpoints'
/// meridians and the equator bound), by Gauss-Legendre quadrature over
/// panels of at most 100 km.
fn area_under(from: Pos, azimuth: f64, distance: f64, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
    if distance == 0.0 {
        return Ok(0.0);
    }
    let panels = (distance / 100_000.0).ceil().max(1.0) as usize;
    work.charge(panels.saturating_mul(crate::measure::GAUSS.len()))?;
    let width = distance / panels as f64;
    let mut sum = 0.0;
    for panel in 0..panels {
        let middle = width * (panel as f64 + 0.5);
        for (node, weight) in crate::measure::GAUSS {
            let s = middle + node * width / 2.0;
            let at = crate::geodesy::direct(from[1], from[0], azimuth, s);
            let phi = at.lat2.to_radians();
            let cos = libm::cos(phi);
            if cos == 0.0 {
                continue;
            }
            let dlon = libm::sin(at.azi2.to_radians()) / (crate::geodesy::prime_vertical_radius(phi) * cos);
            sum += weight * width / 2.0 * crate::geodesy::zone_area_per_radian(phi) * dlon;
        }
    }
    Ok(sum)
}

fn chart(chart: Chart, direction: Direction, positions: &[[f64; 2]], work: &mut GeometryWork) -> Result<Value, GeometryRefusal> {
    work.charge(positions.len())?;
    let mut out = Vec::with_capacity(positions.len());
    for &p in positions {
        if !p.iter().all(|x| x.is_finite()) {
            return Err(refused("a chart takes finite positions"));
        }
        let mapped: Option<[f64; 2]> = match (chart, direction) {
            (Chart::Utm { zone, hemisphere }, Direction::Forward) => {
                if !(1..=60).contains(&zone) {
                    return Err(refused("a UTM zone is 1 to 60"));
                }
                crate::trajectory::validate_position(p)?;
                let projected = crate::mgrs::utm_forward(p, zone, hemisphere.into());
                Some([projected.easting, projected.northing])
            },
            (Chart::Utm { zone, hemisphere }, Direction::Inverse) => {
                if !(1..=60).contains(&zone) {
                    return Err(refused("a UTM zone is 1 to 60"));
                }
                let projected = crate::mgrs::Projected { easting: p[0], northing: p[1] };
                Some(crate::mgrs::utm_inverse(projected, zone, hemisphere.into()))
            },
            (Chart::Ups { hemisphere }, Direction::Forward) => {
                crate::trajectory::validate_position(p)?;
                if (p[1] > 0.0) != (hemisphere == Hemisphere::North) && p[1] != 0.0 {
                    return Err(refused("a UPS chart sees its own hemisphere"));
                }
                let projected = crate::mgrs::ups_forward(p);
                Some([projected.easting, projected.northing])
            },
            (Chart::Ups { hemisphere }, Direction::Inverse) => {
                let projected = crate::mgrs::Projected { easting: p[0], northing: p[1] };
                Some(crate::mgrs::ups_inverse(projected, hemisphere.into()))
            },
            (Chart::Cube { face }, Direction::Forward) => {
                if face > 5 {
                    return Err(refused("a cube face is 0 to 5"));
                }
                crate::trajectory::validate_position(p)?;
                let v = crate::arcs::direction(p);
                let axis = usize::from(face % 3);
                let outward = if face < 3 { v[axis] } else { -v[axis] };
                // A face sees the directions on its own side only.
                (outward > 0.0).then(|| {
                    let (u, w) = crate::cells::face_uv(face, v);
                    [u, w]
                })
            },
            (Chart::Cube { face }, Direction::Inverse) => {
                if face > 5 {
                    return Err(refused("a cube face is 0 to 5"));
                }
                Some(crate::arcs::position(crate::cells::face_xyz(face, p[0], p[1])))
            },
            (Chart::Affine { matrix }, direction) => {
                if !matrix.iter().flatten().all(|x| x.is_finite()) {
                    return Err(refused("an affine chart's matrix is finite"));
                }
                let [[a, b, c], [d, e, f]] = matrix;
                match direction {
                    Direction::Forward => Some([a * p[0] + b * p[1] + c, d * p[0] + e * p[1] + f]),
                    Direction::Inverse => {
                        let det = a * e - b * d;
                        if det == 0.0 || !det.is_finite() {
                            return Err(refused("a singular affine chart has no inverse"));
                        }
                        let (x, y) = (p[0] - c, p[1] - f);
                        Some([(e * x - b * y) / det, (a * y - d * x) / det])
                    },
                }
            },
        };
        if let Some(q) = mapped
            && !q.iter().all(|x| x.is_finite())
        {
            return Err(refused("the chart does not reach this position"));
        }
        out.push(mapped.map_or(Value::Null, |q| serde_json::json!(q)));
    }
    Ok(Value::Array(out))
}

/// One operand of `sep`: a value, or the boundary of one (the pieces of its
/// areas' rings, else its lines' ends).
fn sep_pieces(space: &GeometrySpace, term: &Term, work: &mut GeometryWork) -> Result<(crate::Geometry, Vec<(Pos, Pos)>, bool), GeometryRefusal> {
    match term {
        Term::Boundary { of } => {
            let geometry = value_of(space, of, work)?;
            let pieces = boundary_pieces(&geometry);
            Ok((geometry, pieces, true))
        },
        _ => {
            let geometry = value_of(space, term, work)?;
            let pieces = crate::predicates::segments(&geometry);
            Ok((geometry, pieces, false))
        },
    }
}

fn boundary_pieces(geometry: &crate::Geometry) -> Vec<(Pos, Pos)> {
    use crate::{Coords, Kind};
    match (&geometry.coords, geometry.kind) {
        (Coords::Collection(members), _) => members.iter().flat_map(boundary_pieces).collect(),
        (_, Kind::Polygon | Kind::MultiPolygon) => crate::predicates::segments(geometry),
        (Coords::Points(line), Kind::LineString) if line.first() != line.last() => {
            line.first().into_iter().chain(line.last()).map(|p| (*p, *p)).collect()
        },
        (Coords::Lines(lines), Kind::MultiLineString) => lines
            .iter()
            .filter(|line| line.first() != line.last())
            .flat_map(|line| line.first().into_iter().chain(line.last()).map(|p| (*p, *p)))
            .collect(),
        _ => vec![],
    }
}

/// The position at parameter `t` of a kernel piece: along the great
/// elliptic arc (the auxiliary sphere's great circle) in WGS84, the straight
/// line in a Cartesian frame.
pub(crate) fn along(space: &GeometrySpace, piece: (Pos, Pos), t: f64) -> Pos {
    if t <= 0.0 || piece.0 == piece.1 {
        return piece.0;
    }
    if t >= 1.0 {
        return piece.1;
    }
    match space.frame {
        CoordinateFrame::Wgs84 => {
            let (a, b) = (crate::arcs::direction(piece.0), crate::arcs::direction(piece.1));
            let norm = |v: [f64; 3]| libm::sqrt(v[0] * v[0] + v[1] * v[1] + v[2] * v[2]);
            let (a, b) = (a.map(|x| x / norm(a)), b.map(|x| x / norm(b)));
            let mix = [0, 1, 2].map(|i| (1.0 - t) * a[i] + t * b[i]);
            crate::arcs::position(mix)
        },
        CoordinateFrame::Cartesian { .. } => crate::point_along(piece.0, piece.1, t),
    }
}

/// The parameter in [0, 1] minimizing `f`, by golden-section search.
fn golden(mut f: impl FnMut(f64) -> Result<f64, GeometryRefusal>) -> Result<(f64, f64), GeometryRefusal> {
    const R: f64 = 0.618_033_988_749_894_9;
    let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
    let (mut x1, mut x2) = (hi - R * (hi - lo), lo + R * (hi - lo));
    let (mut f1, mut f2) = (f(x1)?, f(x2)?);
    for _ in 0..48 {
        if f1 <= f2 {
            hi = x2;
            x2 = x1;
            f2 = f1;
            x1 = hi - R * (hi - lo);
            f1 = f(x1)?;
        } else {
            lo = x1;
            x1 = x2;
            f1 = f2;
            x2 = lo + R * (hi - lo);
            f2 = f(x2)?;
        }
    }
    let mut best = if f1 <= f2 { (x1, f1) } else { (x2, f2) };
    for t in [0.0, 1.0] {
        let value = f(t)?;
        if value < best.1 {
            best = (t, value);
        }
    }
    Ok(best)
}

/// The closest pair of positions on two pieces.
fn witness(space: &GeometrySpace, a: (Pos, Pos), b: (Pos, Pos), work: &mut GeometryWork) -> Result<(Pos, Pos, f64), GeometryRefusal> {
    work.charge(52 * 52)?;
    let inner = |s: f64| -> Result<(f64, f64), GeometryRefusal> {
        let p = along(space, a, s);
        golden(|t| crate::predicates::space_point_distance(space, p, along(space, b, t)))
    };
    let (s, _) = golden(|s| inner(s).map(|(_, d)| d))?;
    let (t, d) = inner(s)?;
    Ok((along(space, a, s), along(space, b, t), d))
}

/// `sep`: the infimum of distance between two sets and a pair attaining it,
/// or null when either is empty. Zero is decided by the exact topology.
fn sep(space: &GeometrySpace, left: &Term, right: &Term, work: &mut GeometryWork) -> Result<Value, GeometryRefusal> {
    let (a, a_pieces, a_boundary) = sep_pieces(space, left, work)?;
    let (b, b_pieces, b_boundary) = sep_pieces(space, right, work)?;
    if a_pieces.is_empty() || b_pieces.is_empty() {
        return Ok(Value::Null);
    }
    if !a_boundary && !b_boundary && crate::predicates::intersects(&a, &b, work)? {
        // A shared position: a vertex of one the other covers, else the
        // closest pair of their pieces (which then meet).
        for (own, other) in [(&a, &b), (&b, &a)] {
            for piece in crate::predicates::segments(own) {
                let point = crate::Geometry::new(crate::Kind::Point, crate::Coords::Point(piece.0), space.clone());
                if crate::predicates::covered_by(&point, other, work)? {
                    return Ok(serde_json::json!({ "distance_m": 0.0, "witness": [piece.0, piece.0] }));
                }
            }
        }
    }
    let mut best: Option<((Pos, Pos), (Pos, Pos), f64)> = None;
    for &p in &a_pieces {
        for &q in &b_pieces {
            work.charge(1)?;
            let bound = best.map_or(f64::INFINITY, |(_, _, d)| d);
            let d = crate::predicates::space_segment_distance(space, p, q, bound, work)?;
            if d < bound {
                best = Some((p, q, d));
            }
        }
    }
    let Some((p, q, distance)) = best else {
        return Ok(Value::Null);
    };
    let (x, y, _) = witness(space, p, q, work)?;
    Ok(serde_json::json!({ "distance_m": distance, "witness": [x, y] }))
}

/// A realized term: its admitted value (in the large tier until the reply
/// admits it at the request's), the Hausdorff bound it meets and the sides
/// it holds (both when exact; `nearest` always).
struct Realized {
    geometry: crate::Geometry,
    deviation_m: f64,
    outer: bool,
    inner: bool,
}

impl Realized {
    fn exact(geometry: crate::Geometry) -> Self {
        Self { geometry, deviation_m: 0.0, outer: true, inner: true }
    }

    /// A value rounded by `deviation_m`, which holds the nearest side only
    /// unless it is exact.
    fn rounded(geometry: crate::Geometry, deviation_m: f64) -> Self {
        let exact = deviation_m == 0.0;
        Self { geometry, deviation_m, outer: exact, inner: exact }
    }

    fn holds(&self, side: Side) -> bool {
        match side {
            Side::Outer => self.outer,
            Side::Inner => self.inner,
            Side::Nearest => true,
        }
    }
}

/// Which dimensions a value's non-empty members have: (points, lines, areas).
fn dimensions(geometry: &crate::Geometry) -> (bool, bool, bool) {
    use crate::{Coords, Kind};
    match (&geometry.coords, geometry.kind) {
        (Coords::Empty, _) => (false, false, false),
        (Coords::Collection(members), _) => members.iter().map(dimensions).fold((false, false, false), |a, b| (a.0 || b.0, a.1 || b.1, a.2 || b.2)),
        (_, Kind::Point | Kind::MultiPoint) => (geometry.position_count() > 0, false, false),
        (_, Kind::LineString | Kind::MultiLineString) => (false, geometry.position_count() > 0, false),
        (_, Kind::Polygon | Kind::MultiPolygon) => (false, false, geometry.position_count() > 0),
        _ => (false, false, false),
    }
}

use crate::overlay::construct;

/// The topological boundary of an admitted value, and the rounding it
/// reports. Points have none; a line's is its ends of odd degree (OGC's mod
/// 2 rule); areas' is the exposed boundary of their union, cut where edges
/// cross (each crossing rounded to binary64) and joined into lines. A cut
/// along the antimeridian is interior and no part of it.
fn boundary(space: &GeometrySpace, geometry: &crate::Geometry, work: &mut GeometryWork) -> Result<(crate::Geometry, f64), GeometryRefusal> {
    let empty = || serde_json::json!({ "type": "GeometryCollection", "geometries": [] });
    let key = |p: Pos| crate::arcs::embed(&space.frame, p).map(f64::to_bits);
    let value = match dimensions(geometry) {
        (_, false, false) => (empty(), 0.0),
        (false, true, false) => {
            let mut degree: Vec<([u64; 3], Pos, usize)> = Vec::new();
            for line in geometry.lines() {
                work.charge(line.len())?;
                let (Some(&first), Some(&last)) = (line.first(), line.last()) else {
                    continue;
                };
                for end in [first, last] {
                    match degree.iter_mut().find(|(k, ..)| *k == key(end)) {
                        Some(entry) => entry.2 += 1,
                        None => degree.push((key(end), end, 1)),
                    }
                }
            }
            let ends: Vec<Pos> = degree.into_iter().filter(|(.., n)| n % 2 == 1).map(|(_, p, _)| p).collect();
            if ends.is_empty() {
                (empty(), 0.0)
            } else {
                (serde_json::json!({ "type": "MultiPoint", "coordinates": ends }), 0.0)
            }
        },
        (false, false, true) => {
            let mut chains: Vec<Vec<Pos>> = Vec::new();
            let mut rounding = 0.0_f64;
            crate::topology::visit_boundary(geometry, &[geometry], crate::topology::BoundaryDomain::Areas, work, &mut |piece, _| {
                if key(piece.start) == key(piece.end) {
                    return Ok(false);
                }
                rounding = rounding.max(piece.rounding_error_m);
                match chains.last_mut() {
                    Some(chain) if chain.last().is_some_and(|p| key(*p) == key(piece.start)) => chain.push(piece.end),
                    _ => chains.push(vec![piece.start, piece.end]),
                }
                Ok(false)
            })?;
            if chains.is_empty() {
                (empty(), rounding)
            } else {
                (serde_json::json!({ "type": "MultiLineString", "coordinates": chains }), rounding)
            }
        },
        _ => return Err(refused("realize: boundary takes points, lines or areas, not a mix of dimensions")),
    };
    Ok((construct(space, value.0, work)?, value.1))
}

/// The space a realized term lands in: its operands' space, except a
/// `densify` (another edge model) or a `recast` (its target).
fn term_space(space: &GeometrySpace, term: &Term) -> Result<GeometrySpace, GeometryRefusal> {
    Ok(match term {
        Term::Densify { of, segments } => {
            let mut target = term_space(space, of)?;
            target.segments = serde_json::from_value(segments.clone()).map_err(crate::invalid)?;
            target
        },
        Term::Recast { target, .. } => serde_json::from_value(target.clone()).map_err(crate::invalid)?,
        Term::Value { .. } => space.clone(),
        Term::Union { left, .. } | Term::Intersection { left, .. } | Term::Difference { left, .. } => term_space(space, left)?,
        Term::Boundary { of } | Term::Buffer { of, .. } => term_space(space, of)?,
    })
}

/// An operand an overlay, recast or densification reads exactly: a rounded
/// operand has no bound through them.
fn exact_operand(realized: &Realized, what: &str) -> Result<(), GeometryRefusal> {
    if realized.deviation_m > 0.0 {
        return Err(refused(format!("realize: a {what} of a rounded term has no bound from it; realize the term and {what} its value")));
    }
    Ok(())
}

/// One position through a chart, in the units of the spaces it joins.
fn chart_position(c: Chart, direction: Direction, source: &GeometrySpace, target: &GeometrySpace, p: Pos, work: &mut GeometryWork) -> Result<Pos, GeometryRefusal> {
    let metres = |space: &GeometrySpace| match &space.frame {
        CoordinateFrame::Cartesian { unit, .. } => unit.metres_per_unit.get(),
        CoordinateFrame::Wgs84 => 1.0,
    };
    let projected = matches!(c, Chart::Utm { .. } | Chart::Ups { .. });
    let input = if projected && direction == Direction::Inverse { p.map(|x| x * metres(source)) } else { p };
    let answer = chart(c, direction, &[input], work)?;
    let Some(q) = answer.get(0).and_then(Value::as_array).and_then(|q| Some([q.first()?.as_f64()?, q.get(1)?.as_f64()?])) else {
        return Err(refused("realize: a recast position lies outside its chart"));
    };
    Ok(if projected && direction == Direction::Forward { q.map(|x| x / metres(target)) } else { q })
}

/// `recast`: re-express a value of `source` in `target` through a chart
/// (forward from WGS84, inverse into it, an affine map between Cartesian
/// frames), or without one between equal frames.
fn recast(source: &GeometrySpace, target: &GeometrySpace, via: Option<&Value>, of: &crate::Geometry, budget: f64, work: &mut GeometryWork) -> Result<(crate::Geometry, f64), GeometryRefusal> {
    target.validate().map_err(crate::unsupported_space)?;
    let Some(via) = via.filter(|via| !via.is_null()) else {
        if source == target {
            return Ok((of.clone(), 0.0));
        }
        if source.frame != target.frame {
            return Err(crate::unsupported_space("a recast between frames names a chart"));
        }
        return crate::offset::reexpress(source, target, of, &mut |p, _| Ok(p), budget, 0.0, work);
    };
    let c: Chart = serde_json::from_value(via.clone()).map_err(crate::invalid)?;
    let geographic = |space: &GeometrySpace| space.frame == CoordinateFrame::Wgs84;
    let direction = match (c, geographic(source), geographic(target)) {
        (Chart::Affine { .. }, false, false) => Direction::Forward,
        (Chart::Affine { .. }, ..) => return Err(crate::unsupported_space("an affine chart joins Cartesian frames")),
        (_, true, false) => Direction::Forward,
        (_, false, true) => Direction::Inverse,
        _ => return Err(crate::unsupported_space("a UTM, UPS or cube chart joins WGS84 and a Cartesian frame")),
    };
    // The map's own error: the projection series, else binary64 rounding.
    let mut largest = 0.0_f64;
    let (source_space, target_space) = (source.clone(), target.clone());
    let mut map = |p: Pos, work: &mut GeometryWork| {
        let q = chart_position(c, direction, &source_space, &target_space, p, work)?;
        largest = largest.max(q[0].abs()).max(q[1].abs());
        Ok(q)
    };
    let (geometry, deviation) = crate::offset::reexpress(source, target, of, &mut map, budget, 0.0, work)?;
    let numeric = match (&target.frame, c) {
        (_, Chart::Utm { .. } | Chart::Ups { .. }) => 1e-6,
        (CoordinateFrame::Cartesian { unit, .. }, _) => 4.0 * f64::EPSILON * largest.max(1.0) * unit.metres_per_unit.get(),
        (CoordinateFrame::Wgs84, _) => 1e-6,
    };
    let deviation = deviation + numeric;
    if deviation > budget {
        return Err(crate::offset::bound_refusal(format!("the recast value is within {deviation} m, beyond the bound of {budget} m")));
    }
    Ok((geometry, deviation))
}

/// Realize a term: evaluate it to an admitted value, the Hausdorff bound to
/// its exact set that the value meets (within `budget`), and the sides it
/// holds. `side` is the side the caller needs, so a construction that can
/// hold it does (a buffer), and a difference asks its right operand for the
/// opposite side.
fn realize_term(space: &GeometrySpace, term: &Term, budget: f64, side: Side, work: &mut GeometryWork) -> Result<Realized, GeometryRefusal> {
    match term {
        Term::Value { .. } => Ok(Realized::exact(value_of(space, term, work)?)),
        Term::Union { left, right } => {
            // The union of sets within δ of two sets is within δ of theirs,
            // on the side both hold.
            let (a, b) = (realize_term(space, left, budget, side, work)?, realize_term(space, right, budget, side, work)?);
            let geometry = crate::union::reduce([a.geometry, b.geometry], &term_space(space, left)?, work)?;
            Ok(Realized { geometry, deviation_m: a.deviation_m.max(b.deviation_m), outer: a.outer && b.outer, inner: a.inner && b.inner })
        },
        Term::Intersection { left, right } | Term::Difference { left, right } => {
            let operation = if matches!(term, Term::Intersection { .. }) { Overlay::Intersection } else { Overlay::Difference };
            let a = realize_term(space, left, budget, side, work)?;
            let b = realize_term(space, right, budget, if operation == Overlay::Difference { side.opposite() } else { side }, work)?;
            exact_operand(&a, "overlay")?;
            exact_operand(&b, "overlay")?;
            if a.geometry.space != b.geometry.space {
                return Err(crate::unsupported_space("an overlay's operands are in one space"));
            }
            let target = a.geometry.space.clone();
            let (value, deviation_m) = crate::overlay::overlay(&target, &a.geometry, &b.geometry, operation, work)?;
            Ok(Realized::rounded(construct(&target, value, work)?, deviation_m))
        },
        Term::Boundary { of } => {
            let of = realize_term(space, of, budget, Side::Nearest, work)?;
            if of.deviation_m > 0.0 {
                // A boundary is not bounded by its set's Hausdorff bound.
                return Err(refused("realize: the boundary of a rounded term has no bound from it; realize the term itself"));
            }
            let target = of.geometry.space.clone();
            let (geometry, deviation_m) = boundary(&target, &of.geometry, work)?;
            Ok(Realized::rounded(geometry, deviation_m))
        },
        Term::Buffer { of, metres } => {
            // A buffer is 1-Lipschitz in Hausdorff distance and monotone, so
            // its operand's bound and side carry through it.
            let of = realize_term(space, of, budget, side, work)?;
            let target = of.geometry.space.clone();
            let (geometry, own) = crate::offset::buffer(&target, &of.geometry, *metres, budget - of.deviation_m, side, work)?;
            let curved = own > 0.0;
            Ok(Realized {
                geometry,
                deviation_m: of.deviation_m + own,
                outer: of.outer && (!curved || side == Side::Outer),
                inner: of.inner && (!curved || side == Side::Inner),
            })
        },
        Term::Densify { of, .. } => {
            let target = term_space(space, term)?;
            let of = realize_term(space, of, budget, Side::Nearest, work)?;
            exact_operand(&of, "densification")?;
            target.validate().map_err(crate::unsupported_space)?;
            if target == of.geometry.space {
                return Ok(of);
            }
            let source = of.geometry.space.clone();
            let (geometry, deviation_m) = crate::offset::reexpress(&source, &target, &of.geometry, &mut |p, _| Ok(p), budget, 0.0, work)?;
            Ok(Realized::rounded(geometry, deviation_m))
        },
        Term::Recast { of, via, .. } => {
            let target = term_space(space, term)?;
            let of = realize_term(space, of, budget, Side::Nearest, work)?;
            exact_operand(&of, "recast")?;
            let source = of.geometry.space.clone();
            let (geometry, deviation_m) = recast(&source, &target, via.as_ref(), &of.geometry, budget, work)?;
            Ok(Realized::rounded(geometry, deviation_m))
        },
    }
}

fn realize(space: &GeometrySpace, term: &Term, size: &str, max_deviation_m: f64, side: &str, work: &mut GeometryWork) -> Result<Value, GeometryRefusal> {
    space.validate().map_err(crate::unsupported_space)?;
    if !(max_deviation_m.is_finite() && max_deviation_m >= 0.0) {
        return Err(refused("A deviation bound is finite and non-negative metres"));
    }
    let Some(wanted) = Side::parse(side) else {
        return Err(refused("A realized side is outer, inner or nearest"));
    };
    let realized = realize_term(space, term, max_deviation_m, wanted, work)?;
    if realized.deviation_m > max_deviation_m {
        return Err(crate::offset::bound_refusal(format!(
            "the realized value is within {} m of the term, beyond the bound of {max_deviation_m} m",
            realized.deviation_m
        )));
    }
    if !realized.holds(wanted) {
        return Err(crate::offset::bound_refusal(format!("a rounded value holds the nearest side only, not {side}")));
    }
    // The reply admits the value at the request's tier, in its own space.
    let declaration = GeometryDeclaration::parse(realized.geometry.space.clone(), size).map_err(crate::unsupported_space)?;
    let json: Value = serde_json::from_str(&realized.geometry.js_json()).expect("canonical geometry JSON");
    crate::admit_value_with_work(&Gv::from(&json), &declaration, work)?;
    Ok(serde_json::json!({ "value": json, "deviation_m": realized.deviation_m, "side": side }))
}

fn evaluate(request: GeneratorRequest, work: &mut GeometryWork) -> Result<Value, GeometryRefusal> {
    Ok(match request {
        GeneratorRequest::Relate { space, left, right, .. } => {
            space.validate().map_err(crate::unsupported_space)?;
            let a = value_of(&space, &left, work)?;
            let b = value_of(&space, &right, work)?;
            Value::String(crate::topology::relation::matrix(&a, &b, work)?)
        },
        GeneratorRequest::Inverse { space, from, to, .. } => {
            wgs84(&space, "inverse")?;
            crate::trajectory::validate_position(from)?;
            crate::trajectory::validate_position(to)?;
            work.charge(1)?;
            let solved = crate::geodesy::inverse(from[1], from[0], to[1], to[0]);
            let area = area_under(from, solved.azi1, solved.s12, work)?;
            serde_json::json!({
                "distance_m": solved.s12,
                "azimuth_from_degrees": solved.azi1,
                "azimuth_to_degrees": solved.azi2,
                "area_under_m2": area,
            })
        },
        GeneratorRequest::Direct { space, from, azimuth_degrees, distance_m, .. } => {
            wgs84(&space, "direct")?;
            crate::trajectory::validate_position(from)?;
            if !azimuth_degrees.is_finite() || !distance_m.is_finite() {
                return Err(refused("an azimuth and a distance are finite numbers"));
            }
            work.charge(1)?;
            let to = crate::geodesy::direct(from[1], from[0], azimuth_degrees, distance_m);
            serde_json::json!([to.lon2, to.lat2])
        },
        GeneratorRequest::Chart { space, chart: c, direction, positions, .. } => {
            space.validate().map_err(crate::unsupported_space)?;
            if !matches!(c, Chart::Affine { .. }) {
                wgs84(&space, "chart")?;
            }
            chart(c, direction, &positions, work)?
        },
        GeneratorRequest::Sep { space, left, right, .. } => {
            space.validate().map_err(crate::unsupported_space)?;
            sep(&space, &left, &right, work)?
        },
        GeneratorRequest::Realize { space, term, size, max_deviation_m, side, .. } => {
            realize(&space, &term, &size, max_deviation_m, &side, work)?
        },
    })
}

/// Answer one generator request: `{ value, usage }` or `{ refusal, usage }`.
pub fn invoke(input: Value) -> Value {
    let request = match serde_json::from_value::<GeneratorRequest>(input) {
        Ok(request) => request,
        Err(error) => return crate::binding::reply(Err(crate::invalid(error)), GeometryWork::with_limits(0, 0).usage()),
    };
    let allowance = match &request {
        GeneratorRequest::Relate { allowance, .. }
        | GeneratorRequest::Inverse { allowance, .. }
        | GeneratorRequest::Direct { allowance, .. }
        | GeneratorRequest::Sep { allowance, .. }
        | GeneratorRequest::Chart { allowance, .. }
        | GeneratorRequest::Realize { allowance, .. } => *allowance,
    };
    let mut work = allowance.map_or_else(GeometryWork::default, |allowance| {
        GeometryWork::with_limits(allowance.steps as usize, allowance.allocated_bytes as usize)
    });
    let result = evaluate(request, &mut work);
    crate::binding::reply(result, work.usage())
}

#[cfg(test)]
#[path = "generator_tests.rs"]
mod tests;
