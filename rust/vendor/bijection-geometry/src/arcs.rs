//! Exact predicates for great elliptic arcs on the WGS84 ellipsoid.
//!
//! An edge is the shorter arc of the curve where the ellipsoid meets the plane
//! through its centre and both vertices. Scaling z by a/b maps the ellipsoid
//! to a sphere and keeps planes through the origin, so every such edge is a
//! great-circle arc on that auxiliary sphere, where a vertex sits at its
//! reduced latitude: tan β = (1 − f) tan φ.
//!
//! A vertex is the homogeneous direction (cos λ, sin λ, (1 − f) tan φ), or
//! (0, 0, ±1) at a pole. Orientation is invariant under positive scaling, and
//! this form keeps vertices on one meridian, and on the equator, exactly
//! coplanar. Predicates follow S2 (Apache-2.0): a binary64 filter, exact
//! rational arithmetic, and symbolic perturbation, so only equal vertices are
//! degenerate.
//!
//! A Cartesian frame embeds (x, y) as the direction (x, y, 1): every vertex
//! lies in one open hemisphere, where the arc between two directions is the
//! plane's straight segment, so the same predicates decide planar topology.
use std::borrow::Cow;

use num_traits::{
    Signed,
    ToPrimitive,
    Zero,
};

use crate::{
    exact::{
        self,
        Rational,
    },
    geodesy,
    spatial_tree::PackedBoxes,
    CoordinateFrame,
    GeometryRefusal,
    GeometryWork,
    Pos,
    Rel,
    TOLERANCE,
};

/// A direction from the centre of the auxiliary sphere, of any positive length.
pub type Vector = [f64; 3];
pub(crate) type Exact = [Rational; 3];

const F1: f64 = 1.0 - geodesy::FLATTENING;
const NORTH: Vector = [0.0, 0.0, 1.0];
const SOUTH: Vector = [0.0, 0.0, -1.0];
/// The Cartesian plane's point at infinity along +x, outside every ring.
const EAST: Vector = [1.0, 0.0, 0.0];

/// The canonical auxiliary-sphere direction of a geodetic position, in degrees.
pub fn direction(p: Pos) -> Vector {
    if p[1] == 90.0 {
        return [0.0, 0.0, 1.0];
    }
    if p[1] == -90.0 {
        return [0.0, 0.0, -1.0];
    }
    let (sin_lon, cos_lon) = geodesy::sincosd(p[0]);
    let (sin_lat, cos_lat) = geodesy::sincosd(p[1]);
    // Adding zero turns -0 into +0, so equal positions compare equal.
    [cos_lon + 0.0, sin_lon + 0.0, F1 * (sin_lat / cos_lat) + 0.0]
}

/// The geodetic position, in degrees, of an auxiliary-sphere direction.
pub fn position(v: Vector) -> Pos {
    let horizontal = libm::hypot(v[0], v[1]);
    if horizontal == 0.0 {
        return [0.0, 90.0_f64.copysign(v[2])];
    }
    // tan φ = tan β / (1 − f), with tan β = z / horizontal.
    [
        libm::atan2(v[1], v[0]).to_degrees(),
        libm::atan2(v[2] / F1, horizontal).to_degrees(),
    ]
}

/// The vertex of a position in its frame. A WGS84 latitude beyond a pole
/// names the pole; a longitude names its turn, so ±180 are one meridian.
pub(crate) fn embed(frame: &CoordinateFrame, p: Pos) -> Vector {
    match frame {
        // A sphere frame's great circle through (cos λ, sin λ, tan φ) is the
        // auxiliary sphere's through the z-scaled direction: one curve, so
        // one embedding decides both frames' topology.
        CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. } => {
            direction([p[0], p[1].clamp(-90.0, 90.0)])
        },
        CoordinateFrame::Cartesian { .. } => [p[0] + 0.0, p[1] + 0.0, 1.0],
    }
}

/// The metres an auxiliary-sphere radian spans at most in a geographic
/// frame: the semi-major axis on WGS84; on a sphere of radius R, R stretched
/// by the z-scaling between the two directions (at most 1 / (1 − f)²).
pub(crate) fn radian_metres(frame: &CoordinateFrame) -> f64 {
    match frame {
        CoordinateFrame::Wgs84Sphere { radius_m } => radius_m.get() / (F1 * F1),
        CoordinateFrame::Wgs84 | CoordinateFrame::Cartesian { .. } => geodesy::SEMI_MAJOR_AXIS_METRES,
    }
}

/// The frame coordinates of a direction, rounded: for bounds and output.
pub(crate) fn coordinates(geographic: bool, v: Vector) -> Pos {
    if geographic {
        position(v)
    } else {
        [v[0] / v[2], v[1] / v[2]]
    }
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

fn norm(a: Vector) -> f64 {
    libm::hypot(libm::hypot(a[0], a[1]), a[2])
}

fn signum(value: &Rational) -> i8 {
    if value.is_positive() {
        1
    } else if value.is_negative() {
        -1
    } else {
        0
    }
}

fn exact_cross(a: &Exact, b: &Exact) -> Exact {
    [
        &a[1] * &b[2] - &a[2] * &b[1],
        &a[2] * &b[0] - &a[0] * &b[2],
        &a[0] * &b[1] - &a[1] * &b[0],
    ]
}

fn exact_dot(a: &Exact, b: &Exact) -> Rational {
    &a[0] * &b[0] + &a[1] * &b[1] + &a[2] * &b[2]
}

#[cfg(test)]
fn exact_vectors(
    vectors: &[Vector],
    work: &mut GeometryWork,
) -> Result<Vec<Exact>, GeometryRefusal> {
    vectors.iter().map(|v| exact::binary64(*v, work)).collect()
}

fn values<'a>(vectors: &[&'a Exact]) -> Vec<&'a Rational> {
    vectors.iter().flat_map(|v| v.iter()).collect()
}

/// The same direction scaled to a primitive integer vector when its
/// coordinates are dyadic, as every construction from binary64 vertices is:
/// scaling never moves a direction, and integers keep sums from growing.
fn primitive(v: Exact) -> Exact {
    let dyadic = v.iter().all(|value| {
        let denominator = value.denom();
        denominator.trailing_zeros() == Some(denominator.bits() - 1)
    });
    if !dyadic || v.iter().all(Zero::is_zero) {
        return v;
    }
    let shift = v
        .iter()
        .map(|value| value.denom().bits() - 1)
        .max()
        .unwrap_or(0);
    let integers = v.map(|value| value.numer() << (shift - (value.denom().bits() - 1)));
    let common = integers
        .iter()
        .filter_map(|value| value.trailing_zeros())
        .min()
        .unwrap_or(0);
    integers.map(|value| Rational::from_integer(value >> common))
}

/// A direction: an admitted binary64 vertex, or an exact construction from
/// vertices (an arc crossing, a sample inside a cell). Its binary64
/// approximation filters predicates.
#[derive(Clone, Debug)]
pub(crate) struct Point {
    approx: Vector,
    exact: Option<Box<Exact>>,
    /// Whether `approx` is within an ulp of every exact coordinate, so the
    /// filter's error bound holds.
    filtered: bool,
}

impl Point {
    pub(crate) fn vertex(v: Vector) -> Self {
        Self {
            approx: v,
            exact: None,
            filtered: true,
        }
    }

    pub(crate) fn exact(v: Exact, work: &mut GeometryWork) -> Result<Self, GeometryRefusal> {
        let v = exact::arithmetic(work, &values(&[&v]), || primitive(v.clone()))?;
        work.allocate(v.iter().map(exact::bytes).sum())?;
        // Scaled by its largest coordinate, the approximation never leaves
        // binary64's range, whatever the integers' size.
        let approx = exact::arithmetic(work, &values(&[&v]), || {
            let largest = v
                .iter()
                .map(Signed::abs)
                .max()
                .unwrap_or_else(Rational::zero);
            [0, 1, 2].map(|i| {
                if largest.is_zero() {
                    0.0
                } else {
                    (&v[i] / &largest).to_f64().unwrap_or(f64::NAN)
                }
            })
        })?;
        let filtered = (0..3).all(|i| {
            approx[i].is_finite()
                && (approx[i] == 0.0) == v[i].is_zero()
                && (approx[i] == 0.0 || approx[i].abs() >= 1e-290)
        });
        Ok(Self {
            approx,
            exact: Some(Box::new(v)),
            filtered,
        })
    }

    pub(crate) fn approx(&self) -> Vector {
        self.approx
    }

    /// The binary64 vertex this point is, when it is one.
    pub(crate) fn as_vertex(&self) -> Option<Vector> {
        self.exact.is_none().then_some(self.approx)
    }

    pub(crate) fn value(&self, work: &mut GeometryWork) -> Result<Cow<'_, Exact>, GeometryRefusal> {
        match &self.exact {
            Some(value) => Ok(Cow::Borrowed(value.as_ref())),
            None => Ok(Cow::Owned(exact::binary64(self.approx, work)?)),
        }
    }
}

/// The sign of det[a, b, c] from binary64 arithmetic, or 0 when rounding
/// could have changed it.
fn triage(a: Vector, b: Vector, c: Vector, ulps: f64) -> i8 {
    let bc = cross(b, c);
    let det = a[0] * bc[0] + a[1] * bc[1] + a[2] * bc[2];
    // Each term carries two rounded products and their difference; 8 units in
    // the last place of the absolute terms bound the total error.
    let magnitude = a[0].abs() * ((b[1] * c[2]).abs() + (b[2] * c[1]).abs())
        + a[1].abs() * ((b[2] * c[0]).abs() + (b[0] * c[2]).abs())
        + a[2].abs() * ((b[0] * c[1]).abs() + (b[1] * c[0]).abs());
    if magnitude <= 1e-280 || !magnitude.is_finite() {
        return 0;
    }
    let error = ulps * f64::EPSILON * magnitude;
    if det > error {
        1
    } else if det < -error {
        -1
    } else {
        0
    }
}

fn filter(a: &Point, b: &Point, c: &Point) -> i8 {
    if !(a.filtered && b.filtered && c.filtered) {
        return 0;
    }
    // A rounded construction moves each factor by an ulp: three more per term.
    let ulps = if a.exact.is_none() && b.exact.is_none() && c.exact.is_none() {
        8.0
    } else {
        24.0
    };
    triage(a.approx, b.approx, c.approx, ulps)
}

/// Whether two points are the same vector, exactly.
fn same(a: &Point, b: &Point, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    if a.approx != b.approx {
        return Ok(false);
    }
    if a.exact.is_none() && b.exact.is_none() {
        return Ok(true);
    }
    Ok(a.value(work)? == b.value(work)?)
}

/// Whether two points name one direction, whatever their lengths.
pub(crate) fn coincide(
    a: &Point,
    b: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    if same(a, b, work)? {
        return Ok(true);
    }
    if a.exact.is_none() && b.exact.is_none() {
        return Ok(false);
    }
    if a.filtered && b.filtered {
        let (x, y) = (a.approx, b.approx);
        if norm(cross(x, y)) > 1e-9 * norm(x) * norm(y) || dot(x, y) <= 0.0 {
            return Ok(false);
        }
    }
    let (x, y) = (a.value(work)?, b.value(work)?);
    exact::arithmetic(work, &values(&[&x, &y]), || {
        exact_cross(&x, &y).iter().all(Zero::is_zero) && exact_dot(&x, &y).is_positive()
    })
}

/// Whether a and b are antipodal: opposite directions, exactly.
fn opposite(a: &Point, b: &Point, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    if a.filtered && b.filtered && dot(a.approx, b.approx) >= 0.0 {
        return Ok(false);
    }
    let (x, y) = (a.value(work)?, b.value(work)?);
    exact::arithmetic(work, &values(&[&x, &y]), || {
        exact_cross(&x, &y).iter().all(Zero::is_zero) && exact_dot(&x, &y).is_negative()
    })
}

fn antipodal(a: Vector, b: Vector, work: &mut GeometryWork) -> Result<bool, GeometryRefusal> {
    opposite(&Point::vertex(a), &Point::vertex(b), work)
}

/// The exact sign of det[a, b, c]: 0 whenever the three are coplanar.
fn coplanar_sign(
    a: &Point,
    b: &Point,
    c: &Point,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    work.charge(1)?;
    let fast = filter(a, b, c);
    if fast != 0 {
        return Ok(fast);
    }
    if same(a, b, work)? || same(b, c, work)? || same(c, a, work)? {
        return Ok(0);
    }
    let (a, b, c) = (a.value(work)?, b.value(work)?, c.value(work)?);
    exact::arithmetic(work, &values(&[&a, &b, &c]), || {
        signum(&exact_dot(&a, &exact_cross(&b, &c)))
    })
}

/// Whether c lies on the great circle through a and b, exactly.
pub(crate) fn coplanar(
    a: &Point,
    b: &Point,
    c: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    Ok(coplanar_sign(a, b, c, work)? == 0)
}

/// The exact sign of det[a, b, c]: 0 whenever the three directions are
/// coplanar.
pub fn exact_sign(
    a: Vector,
    b: Vector,
    c: Vector,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    coplanar_sign(
        &Point::vertex(a),
        &Point::vertex(b),
        &Point::vertex(c),
        work,
    )
}

/// S2's symbolic perturbation of det[a, b, c] for exactly coplanar a < b < c,
/// with b × c exact: each vertex moves by an infinitesimal that depends on its
/// lexicographic rank, so the first non-zero term decides.
fn perturbed(a: &Exact, b: &Exact, c: &Exact, bc: &Exact) -> i8 {
    let terms: [&dyn Fn() -> i8; 13] = [
        &|| signum(&bc[2]),
        &|| signum(&bc[1]),
        &|| signum(&bc[0]),
        &|| signum(&(&c[0] * &a[1] - &c[1] * &a[0])),
        &|| signum(&c[0]),
        &|| -signum(&c[1]),
        &|| signum(&(&c[2] * &a[0] - &c[0] * &a[2])),
        &|| signum(&c[2]),
        &|| signum(&(&a[0] * &b[1] - &a[1] * &b[0])),
        &|| -signum(&b[0]),
        &|| signum(&b[1]),
        &|| signum(&a[0]),
        &|| 1,
    ];
    terms
        .iter()
        .map(|term| term())
        .find(|sign| *sign != 0)
        .unwrap_or(1)
}

/// The perturbed orientation of three points: 0 only when two are equal.
pub(crate) fn orient(
    a: &Point,
    b: &Point,
    c: &Point,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    work.charge(1)?;
    let fast = filter(a, b, c);
    if fast != 0 {
        return Ok(fast);
    }
    if same(a, b, work)? || same(b, c, work)? || same(c, a, work)? {
        return Ok(0);
    }
    let mut points = [
        a.value(work)?.into_owned(),
        b.value(work)?.into_owned(),
        c.value(work)?.into_owned(),
    ];
    let mut parity = 1;
    for (i, j) in [(0, 1), (1, 2), (0, 1)] {
        if points[j] < points[i] {
            points.swap(i, j);
            parity = -parity;
        }
    }
    let [a, b, c] = points;
    let sign = exact::arithmetic(work, &values(&[&a, &b, &c]), || {
        let bc = exact_cross(&b, &c);
        match signum(&exact_dot(&a, &bc)) {
            0 => perturbed(&a, &b, &c, &bc),
            sign => sign,
        }
    })?;
    Ok(parity * sign)
}

/// The orientation of a, b, c: +1 when c lies left of the arc from a to b
/// (counterclockwise seen from outside), −1 when right. Symbolic perturbation
/// decides coplanar triples, so the result is 0 only when two are equal.
pub fn sign(
    a: Vector,
    b: Vector,
    c: Vector,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    orient(
        &Point::vertex(a),
        &Point::vertex(b),
        &Point::vertex(c),
        work,
    )
}

/// S2's crossing sign of arcs ab and cd: +1 when they cross at a point
/// interior to both, 0 when a vertex of one equals a vertex of the other,
/// −1 otherwise.
pub(crate) fn crossing(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    if same(a, c, work)? || same(a, d, work)? || same(b, c, work)? || same(b, d, work)? {
        return Ok(0);
    }
    if same(a, b, work)? || same(c, d, work)? {
        return Ok(-1);
    }
    // The arcs cross exactly when ACB, CBD, BDA and DAC share one orientation.
    let acb = orient(a, c, b, work)?;
    for (p, q, r) in [(c, b, d), (b, d, a), (d, a, c)] {
        if orient(p, q, r, work)? != acb {
            return Ok(-1);
        }
    }
    Ok(1)
}

/// Whether the arc ab crosses the arc cd at a point interior to both: +1 when
/// it does, 0 when a vertex of one equals a vertex of the other, −1 otherwise.
pub fn crossing_sign(
    a: Vector,
    b: Vector,
    c: Vector,
    d: Vector,
    work: &mut GeometryWork,
) -> Result<i8, GeometryRefusal> {
    crossing(
        &Point::vertex(a),
        &Point::vertex(b),
        &Point::vertex(c),
        &Point::vertex(d),
        work,
    )
}

/// A direction orthogonal to `a`, fixed by `a` alone (S2's `Ortho`).
fn ortho(a: &Point) -> Point {
    let a = a.approx;
    let largest = (0..3)
        .max_by(|i, j| a[*i].abs().total_cmp(&a[*j].abs()))
        .unwrap_or(2);
    let mut temp = [0.012, 0.0053, 0.00457];
    temp[(largest + 2) % 3] = 1.0;
    Point::vertex(cross(a, temp))
}

/// Whether the arcs from `o` to a, b and c are met in that order
/// counterclockwise around `o`, starting from a (S2's `OrderedCCW`).
fn ordered_ccw(
    a: &Point,
    b: &Point,
    c: &Point,
    o: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    let mut count = 0;
    if orient(b, o, a, work)? >= 0 {
        count += 1;
    }
    if orient(c, o, b, work)? >= 0 {
        count += 1;
    }
    if orient(a, o, c, work)? > 0 {
        count += 1;
    }
    Ok(count >= 2)
}

/// For arcs ab and cd sharing a vertex, whether they cross in the sense that
/// keeps crossing parity consistent around the shared vertex (S2's
/// `VertexCrossing`).
fn vertex_crossing(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    if same(a, b, work)? || same(c, d, work)? {
        return Ok(false);
    }
    if same(a, c, work)? {
        return Ok(same(b, d, work)? || ordered_ccw(&ortho(a), d, b, a, work)?);
    }
    if same(b, d, work)? {
        return ordered_ccw(&ortho(b), c, a, b, work);
    }
    if same(a, d, work)? {
        return Ok(same(b, c, work)? || ordered_ccw(&ortho(a), c, b, a, work)?);
    }
    if same(b, c, work)? {
        return ordered_ccw(&ortho(b), d, a, b, work);
    }
    Ok(false)
}

/// Whether arc ab crosses arc cd, counting shared vertices by
/// `vertex_crossing` so that crossing parity stays consistent.
fn edge_or_vertex_crossing(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    match crossing(a, b, c, d, work)? {
        sign if sign < 0 => Ok(false),
        0 => vertex_crossing(a, b, c, d, work),
        _ => Ok(true),
    }
}

/// Whether p lies on the closed minor arc ab, exactly, at any scale. A
/// degenerate or antipodal ab contains only its own vertices.
pub(crate) fn contains(
    p: &Point,
    a: &Point,
    b: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    if same(p, a, work)? || same(p, b, work)? {
        return Ok(true);
    }
    if coplanar_sign(a, b, p, work)? != 0 {
        return Ok(false);
    }
    // On the great circle, p lies on the closed minor arc when neither a→p
    // nor p→b turns against a→b.
    let (p, a, b) = (p.value(work)?, a.value(work)?, b.value(work)?);
    exact::arithmetic(work, &values(&[&p, &a, &b]), || {
        let normal = exact_cross(&a, &b);
        !normal.iter().all(Zero::is_zero)
            && !exact_dot(&exact_cross(&a, &p), &normal).is_negative()
            && !exact_dot(&exact_cross(&p, &b), &normal).is_negative()
    })
}

/// Whether p lies on the closed minor arc ab, exactly. a and b must not be
/// equal or antipodal.
pub fn on_arc(
    p: Vector,
    a: Vector,
    b: Vector,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    contains(
        &Point::vertex(p),
        &Point::vertex(a),
        &Point::vertex(b),
        work,
    )
}

/// The exact direction where arc ab properly crosses arc cd: the cross
/// product of their planes' normals, oriented onto ab.
fn crossing_point(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<Point, GeometryRefusal> {
    let (a, b, c, d) = (
        a.value(work)?,
        b.value(work)?,
        c.value(work)?,
        d.value(work)?,
    );
    let x = exact::arithmetic(work, &values(&[&a, &b, &c, &d]), || {
        let normal = exact_cross(&a, &b);
        let x = exact_cross(&normal, &exact_cross(&c, &d));
        if exact_dot(&exact_cross(&a, &x), &normal).is_negative() {
            x.map(|value| -value)
        } else {
            x
        }
    })?;
    Point::exact(x, work)
}

/// The points where the closed arcs ab and cd meet: a proper crossing, the
/// vertices of one on the other, or the ends of a shared stretch.
pub(crate) fn meet(
    a: &Point,
    b: &Point,
    c: &Point,
    d: &Point,
    work: &mut GeometryWork,
) -> Result<Vec<Point>, GeometryRefusal> {
    work.charge(4)?;
    let mut found: Vec<Point> = Vec::new();
    let add = |p: &Point, found: &mut Vec<Point>, work: &mut GeometryWork| {
        for q in found.iter() {
            if coincide(p, q, work)? {
                return Ok::<_, GeometryRefusal>(());
            }
        }
        found.push(p.clone());
        Ok(())
    };
    if coincide(a, b, work)? || coincide(c, d, work)? {
        let (p, (e, f)) = if coincide(a, b, work)? {
            (a, (c, d))
        } else {
            (c, (a, b))
        };
        let degenerate = coincide(e, f, work)?;
        if (degenerate && coincide(p, e, work)?) || (!degenerate && contains(p, e, f, work)?) {
            add(p, &mut found, work)?;
        }
        return Ok(found);
    }
    let sc = coplanar_sign(a, b, c, work)?;
    let sd = coplanar_sign(a, b, d, work)?;
    let sa = coplanar_sign(c, d, a, work)?;
    let sb = coplanar_sign(c, d, b, work)?;
    if sc == 0 && sd == 0 {
        // One great circle: the shared stretch ends at vertices.
        for p in [a, b, c, d] {
            if contains(p, a, b, work)? && contains(p, c, d, work)? {
                add(p, &mut found, work)?;
            }
        }
        return Ok(found);
    }
    if sc == 0 || sd == 0 || sa == 0 || sb == 0 {
        // A vertex on the other's great circle: the only place they can meet.
        for (side, p, e, f) in [(sc, c, a, b), (sd, d, a, b), (sa, a, c, d), (sb, b, c, d)] {
            if side == 0 && contains(p, e, f, work)? {
                add(p, &mut found, work)?;
            }
        }
        return Ok(found);
    }
    // ACB, CBD, BDA and DAC are −sc, −sb, sd and sa.
    if sa == sd && sb == sc && sa == -sb {
        found.push(crossing_point(a, b, c, d, work)?);
    }
    Ok(found)
}

/// Points on arc ab, each with a payload, ordered from a by a key that
/// rises with the angle from a whatever a point's length. Points naming a
/// are dropped, and at one direction a vertex is kept over a construction.
pub(crate) fn order_along<T>(
    a: &Point,
    b: &Point,
    points: Vec<(Point, T)>,
    work: &mut GeometryWork,
) -> Result<Vec<(Point, T)>, GeometryRefusal> {
    let (start, end) = (a.value(work)?.into_owned(), b.value(work)?.into_owned());
    let normal = exact::arithmetic(work, &values(&[&start, &end]), || {
        exact_cross(&start, &end)
    })?;
    work.allocate(points.len().saturating_mul(256))?;
    let mut keyed = Vec::with_capacity(points.len());
    for (p, payload) in points {
        let value = p.value(work)?.into_owned();
        let key = exact::arithmetic(work, &values(&[&start, &normal, &value]), || {
            let turn = exact_dot(&exact_cross(&start, &value), &normal);
            // −cot θ / |n| rises with θ across (0, π).
            (!turn.is_zero()).then(|| -exact_dot(&start, &value) / turn)
        })?;
        if let Some(key) = key {
            work.allocate(exact::bytes(&key))?;
            keyed.push((key, p, payload));
        }
    }
    work.charge(
        keyed
            .len()
            .saturating_mul(keyed.len().max(2).ilog2() as usize + 1)
            .saturating_mul(8),
    )?;
    keyed.sort_by(|x, y| {
        x.0.cmp(&y.0)
            .then_with(|| y.1.exact.is_none().cmp(&x.1.exact.is_none()))
    });
    keyed.dedup_by(|later, earlier| later.0 == earlier.0);
    Ok(keyed.into_iter().map(|(_, p, payload)| (p, payload)).collect())
}

/// `2^exponent` as a rational.
fn power_of_two(exponent: i64) -> Rational {
    let one = Rational::from_integer(1.into());
    let magnitude = one.numer() << exponent.unsigned_abs();
    if exponent >= 0 {
        Rational::from_integer(magnitude)
    } else {
        Rational::new(1.into(), magnitude)
    }
}

/// About the binary exponent of a point's largest coordinate.
fn exponent(p: &Point) -> i64 {
    match &p.exact {
        Some(v) => v
            .iter()
            .filter(|value| !value.is_zero())
            .map(|value| value.numer().bits() as i64 - value.denom().bits() as i64)
            .max()
            .unwrap_or(0),
        None => {
            let largest = p.approx.iter().fold(0.0_f64, |m, value| m.max(value.abs()));
            if largest.is_finite() && largest > 0.0 {
                libm::ilogb(largest) as i64
            } else {
                0
            }
        },
    }
}

/// A point strictly between two distinct, non-antipodal directions on the
/// arc joining them: their sum, scaled to comparable lengths.
pub(crate) fn between(
    a: &Point,
    b: &Point,
    work: &mut GeometryWork,
) -> Result<Point, GeometryRefusal> {
    let shift = (exponent(a) - exponent(b)).clamp(-4000, 4000);
    let (x, y) = (a.value(work)?, b.value(work)?);
    let sum = exact::arithmetic(work, &values(&[&x, &y]), || {
        let factor = power_of_two(shift);
        [0, 1, 2].map(|i| &x[i] + &y[i] * &factor)
    })?;
    Point::exact(sum, work)
}

/// The left normal of arc ab where the sphere meets the plane it bounds:
/// the plane's normal on the sphere, the in-plane perpendicular (with no
/// homogeneous part) in a Cartesian frame.
pub(crate) fn left_normal(
    geographic: bool,
    a: &Point,
    b: &Point,
    work: &mut GeometryWork,
) -> Result<Exact, GeometryRefusal> {
    let (a, b) = (a.value(work)?, b.value(work)?);
    exact::arithmetic(work, &values(&[&a, &b]), || {
        if geographic {
            exact_cross(&a, &b)
        } else {
            // For (x, y, w) vertices: (w·a − w'·b) rotated a quarter turn left.
            let dx = &b[0] * &a[2] - &a[0] * &b[2];
            let dy = &b[1] * &a[2] - &a[1] * &b[2];
            [-dy, dx, Rational::zero()]
        }
    })
}

/// The direction leaving p towards q along their arc, as a vector to add to
/// p: tangent on the sphere, and with no homogeneous part in a plane.
pub(crate) fn heading(
    geographic: bool,
    p: &Point,
    q: &Point,
    work: &mut GeometryWork,
) -> Result<Exact, GeometryRefusal> {
    let (p, q) = (p.value(work)?, q.value(work)?);
    exact::arithmetic(work, &values(&[&p, &q]), || {
        let (scale, back) = if geographic {
            (exact_dot(&p, &p), exact_dot(&p, &q))
        } else {
            (p[2].clone(), q[2].clone())
        };
        [0, 1, 2].map(|i| &q[i] * &scale - &p[i] * &back)
    })
}

/// The far end of a short probe from p along `heading`: about a
/// sixty-fourth of a radian on the sphere, of a unit in a plane.
pub(crate) fn probe_end(
    geographic: bool,
    p: &Point,
    heading: &Exact,
    work: &mut GeometryWork,
) -> Result<Point, GeometryRefusal> {
    let p = p.value(work)?;
    let magnitude = |v: &Exact| {
        v.iter()
            .map(|value| value.abs().to_f64().unwrap_or(f64::INFINITY))
            .fold(0.0, f64::max)
    };
    // p·2^k + heading moves by |heading| / (2^k |p|): pick k for 1/64.
    let reach = if geographic {
        magnitude(heading) / magnitude(&p)
    } else {
        magnitude(heading) / p[2].abs().to_f64().unwrap_or(f64::INFINITY)
    };
    let shift = if reach.is_finite() && reach > 0.0 {
        (libm::ilogb(reach) as i64 + 7).clamp(-4000, 4000)
    } else {
        0
    };
    let end = exact::arithmetic(work, &values(&[&p, heading]), || {
        let factor = power_of_two(shift);
        [0, 1, 2].map(|i| &p[i] * &factor + &heading[i])
    })?;
    Point::exact(end, work)
}

/// The rounded frame coordinates of a construction and their distance from
/// it, in metres: a radian on the auxiliary sphere spans at most a metres.
pub(crate) fn snap(
    frame: &CoordinateFrame,
    p: &Point,
    work: &mut GeometryWork,
) -> Result<(Pos, f64), GeometryRefusal> {
    match frame {
        CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. } => {
            let approx = p.approx;
            let rounded = position(approx);
            let back = direction(rounded);
            let (x, y) = (norm(approx), norm(back));
            let chord = norm([0, 1, 2].map(|i| approx[i] / x - back[i] / y));
            // The approximation and normalization add a few ulps of angle.
            let error = (chord * (1.0 + 1e-12) + 16.0 * f64::EPSILON) * radian_metres(frame);
            if !rounded.iter().all(|v| v.is_finite()) || !error.is_finite() {
                return Err(crate::invalid("Arrangement exceeds the numerical domain"));
            }
            Ok((rounded, error))
        },
        CoordinateFrame::Cartesian { unit, .. } => {
            let value = p.value(work)?;
            let (rounded, error) = exact::arithmetic(work, &values(&[&value]), || {
                let point = [&value[0] / &value[2], &value[1] / &value[2]];
                let rounded = point
                    .each_ref()
                    .map(|v| v.to_f64().unwrap_or(f64::INFINITY));
                let error: f64 = point
                    .iter()
                    .zip(rounded)
                    .map(|(value, rounded)| match Rational::from_float(rounded) {
                        Some(raw) if raw == *value => 0.0,
                        Some(raw) => {
                            (value - raw)
                                .abs()
                                .to_f64()
                                .unwrap_or(f64::INFINITY)
                                .max(f64::from_bits(1))
                                * 2.0
                        },
                        None => f64::INFINITY,
                    })
                    .sum();
                (rounded, error)
            })?;
            let error = error * unit.metres_per_unit.get();
            if !rounded.iter().all(|v| v.is_finite()) || !error.is_finite() {
                return Err(crate::invalid("Arrangement exceeds the numerical domain"));
            }
            Ok((rounded, error))
        },
    }
}

/// The exact frame coordinates of a Cartesian point.
pub(crate) fn planar(
    p: &Point,
    work: &mut GeometryWork,
) -> Result<[Rational; 2], GeometryRefusal> {
    let value = p.value(work)?;
    exact::arithmetic(work, &values(&[&value]), || {
        [&value[0] / &value[2], &value[1] / &value[2]]
    })
}

/// The fraction of the arc from a to b at p on it: the angle on the
/// auxiliary sphere, the distance along a planar segment.
pub(crate) fn fraction(
    geographic: bool,
    a: &Point,
    b: &Point,
    p: &Point,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    if same(p, a, work)? {
        return Ok(0.0);
    }
    if same(p, b, work)? {
        return Ok(1.0);
    }
    if geographic {
        let angle = |x: Vector, y: Vector| libm::atan2(norm(cross(x, y)), dot(x, y));
        let unit = |v: Vector| {
            let length = norm(v);
            v.map(|value| value / length)
        };
        let (a, b, p) = (unit(a.approx), unit(b.approx), unit(p.approx));
        return Ok((angle(a, p) / angle(a, b)).clamp(0.0, 1.0));
    }
    let (a, b, p) = (planar(a, work)?, planar(b, work)?, planar(p, work)?);
    exact::arithmetic(work, &[&a[0], &a[1], &b[0], &b[1], &p[0], &p[1]], || {
        let d = [&b[0] - &a[0], &b[1] - &a[1]];
        let t = ((&p[0] - &a[0]) * &d[0] + (&p[1] - &a[1]) * &d[1])
            / (&d[0] * &d[0] + &d[1] * &d[1]);
        t.to_f64().unwrap_or(0.0).clamp(0.0, 1.0)
    })
}

/// A conservative longitude/latitude (or x/y) box of the arc between two
/// directions, padded by TOLERANCE: `arc_bounds`, so a WGS84 arc's poleward
/// bulge is inside and east may pass 180 across the seam. One reaching near
/// a pole spans every longitude, since a rounded direction's longitude is
/// unreliable there.
pub(crate) fn arc_box(geographic: bool, a: Vector, b: Vector) -> [f64; 4] {
    let (p, q) = (coordinates(geographic, a), coordinates(geographic, b));
    if !geographic {
        // A rounded construction is off by ulps of its own coordinates.
        let pad = |x: f64, y: f64| 1e-12 * x.abs().max(y.abs());
        let (dx, dy) = (pad(p[0], q[0]), pad(p[1], q[1]));
        let bounds = crate::edge_box(&(p, q));
        return [bounds[0] - dx, bounds[1] - dy, bounds[2] + dx, bounds[3] + dy];
    }
    let [west, south, east, north] = arc_bounds(p, q);
    let (south, north) = (south - TOLERANCE, north + TOLERANCE);
    if north >= 89.99 || south <= -89.99 || east - west >= 360.0 {
        return [-180.0 - TOLERANCE, south, 180.0 + TOLERANCE, north];
    }
    [west - TOLERANCE, south, east + TOLERANCE, north]
}

/// The box of an edge between two positions of a frame.
pub(crate) fn edge_bounds(frame: &CoordinateFrame, p: Pos, q: Pos) -> [f64; 4] {
    match frame {
        CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. } => {
            arc_box(true, embed(frame, p), embed(frame, q))
        },
        CoordinateFrame::Cartesian { .. } => crate::edge_box(&(p, q)),
    }
}

/// A box and, in WGS84, its images one longitude turn away where it passes
/// the seam: every box a search must try.
pub(crate) fn turns(geographic: bool, bounds: [f64; 4]) -> Vec<[f64; 4]> {
    let mut boxes = vec![bounds];
    if geographic && bounds[2] - bounds[0] < 360.0 {
        for (seam, shift) in [
            (bounds[0] < -180.0 + TOLERANCE, 360.0),
            (bounds[2] > 180.0 - TOLERANCE, -360.0),
        ] {
            if seam {
                boxes.push([bounds[0] + shift, bounds[1], bounds[2] + shift, bounds[3]]);
            }
        }
    }
    boxes
}

/// The box of one direction.
pub(crate) fn point_box(geographic: bool, v: Vector) -> [f64; 4] {
    arc_box(geographic, v, v)
}

/// A packed tree of arc boxes. In WGS84 a box touching the seam also
/// appears one turn away, so arcs meeting across ±180 are candidates.
pub(crate) struct ArcIndex {
    tree: PackedBoxes,
    owners: Vec<usize>,
    aliased: bool,
    geographic: bool,
}

impl ArcIndex {
    pub(crate) fn new(
        geographic: bool,
        boxes: &[[f64; 4]],
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        work.allocate(boxes.len().saturating_mul(2 * (32 + 8)))?;
        let mut packed = Vec::with_capacity(boxes.len());
        let mut owners = Vec::with_capacity(boxes.len());
        for (owner, bounds) in boxes.iter().enumerate() {
            for image in turns(geographic, *bounds) {
                packed.push(image);
                owners.push(owner);
            }
        }
        let aliased = owners.len() > boxes.len();
        Ok(Self {
            tree: PackedBoxes::new(&packed, work)?,
            owners,
            aliased,
            geographic,
        })
    }

    /// The distinct arcs whose boxes meet `bounds`, in index order.
    pub(crate) fn search(
        &self,
        bounds: &[f64; 4],
        work: &mut GeometryWork,
    ) -> Result<Vec<usize>, GeometryRefusal> {
        let mut found = Vec::new();
        for query in turns(self.geographic, *bounds) {
            let mut count: usize = 0;
            self.tree.search(
                &query,
                &mut |_| {
                    count += 1;
                    false
                },
                work,
            )?;
            work.allocate(count.saturating_mul(8))?;
            found.reserve(count);
            self.tree.search(
                &query,
                &mut |item| {
                    found.push(self.owners[item]);
                    false
                },
                work,
            )?;
        }
        work.charge(found.len().saturating_mul(found.len().max(2).ilog2() as usize + 1))?;
        found.sort_unstable();
        found.dedup();
        Ok(found)
    }

    /// Every unordered pair of distinct arcs whose boxes meet, once.
    pub(crate) fn pairs(
        &self,
        visit: &mut dyn FnMut(usize, usize, &mut GeometryWork) -> Result<bool, GeometryRefusal>,
        work: &mut GeometryWork,
    ) -> Result<bool, GeometryRefusal> {
        let mut seen = std::collections::HashSet::new();
        let owners = &self.owners;
        let aliased = self.aliased;
        self.tree.pairs(
            &mut |_, _| true,
            &mut |i, j, work| {
                let (i, j) = (owners[i].min(owners[j]), owners[i].max(owners[j]));
                if i == j {
                    return Ok(false);
                }
                if aliased {
                    if !seen.insert((i, j)) {
                        return Ok(false);
                    }
                    work.allocate(32)?;
                }
                visit(i, j, work)
            },
            work,
        )
    }
}

/// Whether a position lies on the antimeridian or at a pole.
fn on_seam(p: Pos) -> bool {
    p[0].abs() == 180.0 || p[1].abs() == 90.0
}

/// Whether a run of seam positions is a cut along the antimeridian, as RFC
/// 7946 writes a ring that winds around a pole or crosses the antimeridian
/// as one ring: out along one side of the meridian (written at +180, or
/// -180), through a pole or a turn on the meridian, and back along the other
/// side to the direction it left. Each leg runs monotonically toward the
/// turn, so the two legs cover the same arc once each, in opposite
/// directions, and the cut is interior to the region.
fn seam_cut(run: &[Pos]) -> bool {
    let pole = |p: &Pos| p[1].abs() == 90.0;
    let (Some(first), Some(last)) = (run.first(), run.last()) else {
        return false;
    };
    if run.len() < 2 || pole(first) || pole(last) || first[1] != last[1] || first[0] != -last[0] {
        return false;
    }
    let side = first[0];
    let Some(turn) = run.iter().position(|p| !pole(p) && p[0] == -side) else {
        return false;
    };
    let (out, back) = run.split_at(turn);
    if out.iter().any(|p| !pole(p) && p[0] != side) || back.iter().any(|p| !pole(p) && p[0] != -side) {
        return false;
    }
    // The out leg moves away from the start's latitude, the back leg toward
    // it, and they meet at one direction: a pole, or one latitude.
    let (out_end, back_start) = (out[out.len() - 1], back[0]);
    let rising = out_end[1] > first[1];
    let monotone = |leg: &[Pos], up: bool| leg.windows(2).all(|w| if up { w[1][1] >= w[0][1] } else { w[1][1] <= w[0][1] });
    // The legs meet at a pole, or cross the meridian at one latitude.
    let returning: Vec<Pos> = std::iter::once(out_end).chain(back.iter().copied()).collect();
    out.len() >= 2
        && out_end[1] != first[1]
        && (pole(&out_end) || out_end[1] == back_start[1])
        && monotone(out, rising)
        && monotone(&returning, !rising)
}

/// A ring as the sphere reads it: each cut along the antimeridian
/// ([`seam_cut`]) is interior, so its positions after the first are dropped
/// and the ring joins straight on from the direction the cut left (the same
/// direction it returns to). Rings without one are returned as they are.
pub(crate) fn without_seam_cuts(geographic: bool, ring: &[Pos]) -> Cow<'_, [Pos]> {
    let Some((_, open)) = ring.split_last() else {
        return Cow::Borrowed(ring);
    };
    let n = open.len();
    let Some(start) = (geographic && n >= 3).then(|| (0..n).find(|&i| !on_seam(open[i]))).flatten() else {
        return Cow::Borrowed(ring);
    };
    let at = |i: usize| open[(start + i) % n];
    let mut kept = Vec::with_capacity(n + 1);
    let mut changed = false;
    let mut i = 0;
    while i < n {
        if !on_seam(at(i)) {
            kept.push(at(i));
            i += 1;
            continue;
        }
        let mut j = i;
        while j < n && on_seam(at(j)) {
            j += 1;
        }
        let run: Vec<Pos> = (i..j).map(at).collect();
        if seam_cut(&run) {
            kept.push(run[0]);
            changed = true;
        } else {
            kept.extend(run);
        }
        i = j;
    }
    if !changed {
        return Cow::Borrowed(ring);
    }
    kept.push(kept[0]);
    Cow::Owned(kept)
}

/// A simple closed chain of arcs. Its left side is the region it bounds when
/// it is a shell, as GeoJSON's counterclockwise outer rings put it; rings may
/// contain a pole or more than a hemisphere, since orientation fixes the
/// side. Consecutive positions naming one direction, such as a pole's
/// longitudes, are one vertex.
#[derive(Clone, Debug)]
pub struct Ring {
    geographic: bool,
    vertices: Vec<Vector>,
    /// The position each vertex was admitted from, its first when several
    /// name one direction.
    positions: Vec<Pos>,
    /// Whether the left side contains each reference direction: the north
    /// and south poles, or a Cartesian plane's point at infinity.
    left: [bool; 2],
}

impl Ring {
    /// Admits a closed ring of WGS84 positions in degrees, its last position
    /// repeating its first.
    pub fn new(positions: &[Pos], work: &mut GeometryWork) -> Result<Self, GeometryRefusal> {
        Self::admit(&CoordinateFrame::Wgs84, positions, work)?.map_err(crate::invalid)
    }

    /// A closed ring in its frame, or why it is not simple.
    pub(crate) fn admit(
        frame: &CoordinateFrame,
        positions: &[Pos],
        work: &mut GeometryWork,
    ) -> Result<Result<Self, &'static str>, GeometryRefusal> {
        let Some((last, open)) = positions.split_last() else {
            return Err(crate::invalid("A ring needs positions"));
        };
        if open.first() != Some(last) {
            return Err(crate::invalid("A ring must end at its first position"));
        }
        let geographic = frame.is_geographic();
        let positions = without_seam_cuts(geographic, positions);
        let open = &positions[..positions.len() - 1];
        work.charge(open.len())?;
        work.allocate(open.len().saturating_mul(2 * std::mem::size_of::<Vector>()))?;
        let mut vertices: Vec<Vector> = Vec::with_capacity(open.len());
        let mut kept: Vec<Pos> = Vec::with_capacity(open.len());
        for p in open {
            if !(p[0].is_finite()
                && p[1].is_finite()
                && (!geographic || p[0].abs() <= 180.0 && p[1].abs() <= 90.0))
            {
                return Err(crate::invalid(
                    "Ring positions are finite coordinates of their frame",
                ));
            }
            let v = embed(frame, *p);
            if vertices.last() != Some(&v) {
                vertices.push(v);
                kept.push(*p);
            }
        }
        while vertices.len() > 1 && vertices.first() == vertices.last() {
            vertices.pop();
            kept.pop();
        }
        if vertices.len() < 3 {
            return Ok(Err("zero area ring"));
        }
        let mut sorted = vertices.clone();
        sorted.sort_by(|a, b| {
            a.iter()
                .zip(b)
                .map(|(x, y)| x.total_cmp(y))
                .find(|order| order.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Ok(Err("self intersection"));
        }
        let n = vertices.len();
        let edge = |i: usize| (Point::vertex(vertices[i]), Point::vertex(vertices[(i + 1) % n]));
        for i in 0..n {
            let (a, b) = edge(i);
            if opposite(&a, &b, work)? {
                return Ok(Err("An edge joins antipodal vertices, which fix no unique arc"));
            }
        }
        let boxes: Vec<_> = (0..n)
            .map(|i| arc_box(geographic, vertices[i], vertices[(i + 1) % n]))
            .collect();
        let index = ArcIndex::new(geographic, &boxes, work)?;
        let simple = !index.pairs(
            &mut |i, j, work| {
                let ((a, b), (c, d)) = (edge(i), edge(j));
                // Adjacent edges share exactly their common vertex.
                let adjacent = j == i + 1 || (i == 0 && j == n - 1);
                if !adjacent && crossing(&a, &b, &c, &d, work)? > 0 {
                    return Ok(true);
                }
                for (p, (e, f)) in [(&c, (&a, &b)), (&d, (&a, &b)), (&a, (&c, &d)), (&b, (&c, &d))] {
                    if !same(p, e, work)? && !same(p, f, work)? && contains(p, e, f, work)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
            work,
        )?;
        if !simple {
            return Ok(Err("self intersection"));
        }
        let mut ring = Self::trusted(geographic, vertices, work)?;
        ring.positions = kept;
        Ok(Ok(ring))
    }

    /// A ring of already admitted vertices.
    pub(crate) fn trusted(
        geographic: bool,
        vertices: Vec<Vector>,
        work: &mut GeometryWork,
    ) -> Result<Self, GeometryRefusal> {
        let references = if geographic { [NORTH, SOUTH] } else { [EAST, EAST] };
        let mut left = [false; 2];
        for (k, reference) in references.into_iter().enumerate() {
            left[k] = if k == 1 && !geographic {
                left[0]
            } else {
                left_contains(&vertices, reference, work)?
            };
        }
        let positions = vertices
            .iter()
            .map(|v| coordinates(geographic, *v))
            .collect();
        Ok(Self {
            geographic,
            vertices,
            positions,
            left,
        })
    }

    /// A ring of a closed run of positions taken as they are: consecutive
    /// positions naming one direction are one vertex, and simplicity is not
    /// checked. None when fewer than three vertices remain.
    pub(crate) fn of_positions(
        geographic: bool,
        positions: &[Pos],
        work: &mut GeometryWork,
    ) -> Result<Option<Self>, GeometryRefusal> {
        let mut vertices: Vec<Vector> = Vec::with_capacity(positions.len());
        for p in &positions[..positions.len().saturating_sub(1)] {
            let v = if geographic { direction(*p) } else { [p[0] + 0.0, p[1] + 0.0, 1.0] };
            if vertices.last() != Some(&v) {
                vertices.push(v);
            }
        }
        if vertices.len() > 1 && vertices.first() == vertices.last() {
            vertices.pop();
        }
        if vertices.len() < 3 {
            return Ok(None);
        }
        work.allocate(vertices.len().saturating_mul(48))?;
        Self::trusted(geographic, vertices, work).map(Some)
    }

    /// The reference direction for crossing parity from p, and whether the
    /// left side contains it: the pole of p's hemisphere, which is never
    /// antipodal to p.
    pub(crate) fn reference(geographic: bool, p: &Point) -> (Point, usize) {
        if !geographic {
            (Point::vertex(EAST), 0)
        } else if p.approx[2] >= 0.0 {
            (Point::vertex(NORTH), 0)
        } else {
            (Point::vertex(SOUTH), 1)
        }
    }

    /// Where a point lies relative to the left side: on an edge or vertex,
    /// inside, or outside.
    pub(crate) fn locate_point(
        &self,
        p: &Point,
        work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        let n = self.vertices.len();
        let edge = |i: usize| {
            (
                Point::vertex(self.vertices[i]),
                Point::vertex(self.vertices[(i + 1) % n]),
            )
        };
        for i in 0..n {
            let (a, b) = edge(i);
            if contains(p, &a, &b, work)? {
                return Ok(Rel::Boundary);
            }
        }
        let (reference, k) = Self::reference(self.geographic, p);
        let mut inside = self.left[k];
        if !coincide(p, &reference, work)? {
            for i in 0..n {
                let (a, b) = edge(i);
                inside ^= edge_or_vertex_crossing(&reference, p, &a, &b, work)?;
            }
        }
        Ok(if inside { Rel::Inside } else { Rel::Outside })
    }

    /// Where a direction lies: on an edge or vertex, inside, or outside.
    pub fn locate_direction(
        &self,
        p: Vector,
        work: &mut GeometryWork,
    ) -> Result<Rel, GeometryRefusal> {
        self.locate_point(&Point::vertex(p), work)
    }

    /// Where a geodetic position, in degrees, lies.
    pub fn locate(&self, p: Pos, work: &mut GeometryWork) -> Result<Rel, GeometryRefusal> {
        self.locate_direction(direction(p), work)
    }

    pub fn vertices(&self) -> &[Vector] {
        &self.vertices
    }

    pub(crate) fn positions(&self) -> &[Pos] {
        &self.positions
    }

    /// Whether the left side contains the reference `k` (see `reference`).
    pub(crate) fn left_contains(&self, k: usize) -> bool {
        self.left[k]
    }

    /// A Cartesian ring's left side is bounded, so it runs counterclockwise.
    pub(crate) fn counterclockwise(&self) -> bool {
        !self.left[0]
    }

    /// The box of the region on one side of the ring: its arcs' boxes, and
    /// every longitude to each pole that side contains.
    pub(crate) fn bounds(&self, left: bool) -> [f64; 4] {
        let n = self.vertices.len();
        let mut bounds = [
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ];
        for i in 0..n {
            let b = arc_box(self.geographic, self.vertices[i], self.vertices[(i + 1) % n]);
            bounds = [
                bounds[0].min(b[0]),
                bounds[1].min(b[1]),
                bounds[2].max(b[2]),
                bounds[3].max(b[3]),
            ];
        }
        if self.geographic {
            if self.left[0] == left {
                bounds = [-180.0 - TOLERANCE, bounds[1], 180.0 + TOLERANCE, 90.0 + TOLERANCE];
            }
            if self.left[1] == left {
                bounds = [-180.0 - TOLERANCE, -90.0 - TOLERANCE, 180.0 + TOLERANCE, bounds[3]];
            }
        }
        bounds
    }
}

/// Whether the left side of the closed chain `vertices` contains
/// `reference`, by S2's convention for a reference on the boundary: a
/// vertex is inside when the fixed direction ortho(vertex) points inside.
pub(crate) fn left_contains(
    vertices: &[Vector],
    reference: Vector,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    let n = vertices.len();
    let reference = Point::vertex(reference);
    // At most one vertex is antipodal to the reference.
    let mut i = 1 % n;
    if opposite(&Point::vertex(vertices[i]), &reference, work)? {
        i = (i + 1) % n;
    }
    let (a, b, c) = (
        Point::vertex(vertices[(i + n - 1) % n]),
        Point::vertex(vertices[i]),
        Point::vertex(vertices[(i + 1) % n]),
    );
    // The left side contains its vertex B, between A and C, exactly when
    // ortho(B) lies in the wedge ABC; parity carries that to the reference.
    let inside = ordered_ccw(&ortho(&b), &a, &c, &b, work)?;
    let mut crosses = false;
    for k in 0..n {
        let (e, f) = (Point::vertex(vertices[k]), Point::vertex(vertices[(k + 1) % n]));
        crosses ^= edge_or_vertex_crossing(&reference, &b, &e, &f, work)?;
    }
    Ok(inside != crosses)
}

/// Crossing parity of the arc from `reference` to p with arc ab, by S2's
/// convention at shared vertices.
pub(crate) fn parity(
    reference: &Point,
    p: &Point,
    a: &Point,
    b: &Point,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    edge_or_vertex_crossing(reference, p, a, b, work)
}

/// Whether a and b are antipodal, which fixes no arc.
pub(crate) fn antipodal_vertices(
    a: Vector,
    b: Vector,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    antipodal(a, b, work)
}

/// The unit vector along a direction.
pub fn unit(a: Vector) -> Vector {
    let length = norm(a);
    [a[0] / length, a[1] / length, a[2] / length]
}

/// The angle between two directions on the auxiliary sphere, in radians.
pub fn angle(a: Vector, b: Vector) -> f64 {
    libm::atan2(norm(cross(a, b)), dot(a, b))
}

/// The point `fraction` of the way along the minor arc from a to b on the
/// auxiliary sphere, by angle; the ends are returned exactly.
pub fn interpolate(a: Vector, b: Vector, fraction: f64) -> Vector {
    if fraction <= 0.0 {
        return a;
    }
    if fraction >= 1.0 {
        return b;
    }
    let (a, b) = (unit(a), unit(b));
    let omega = angle(a, b);
    if omega == 0.0 {
        return a;
    }
    let scale = libm::sin(omega);
    let p = libm::sin((1.0 - fraction) * omega) / scale;
    let q = libm::sin(fraction * omega) / scale;
    [
        p * a[0] + q * b[0],
        p * a[1] + q * b[1],
        p * a[2] + q * b[2],
    ]
}

/// The auxiliary-sphere angle from p to the nearest point of the minor arc
/// ab, and that point's fraction of the arc by angle.
pub fn nearest_on_arc(p: Vector, a: Vector, b: Vector) -> (f64, f64) {
    let (p, a, b) = (unit(p), unit(a), unit(b));
    let normal = cross(a, b);
    let length = norm(normal);
    let omega = angle(a, b);
    if length == 0.0 || omega == 0.0 {
        return (angle(p, a), 0.0);
    }
    let normal = [normal[0] / length, normal[1] / length, normal[2] / length];
    // The foot of p on the arc's great circle lies on the arc when it is on
    // the arc's side of both ends.
    let offset = dot(p, normal);
    let foot = [
        p[0] - offset * normal[0],
        p[1] - offset * normal[1],
        p[2] - offset * normal[2],
    ];
    if dot(cross(a, foot), normal) >= 0.0 && dot(cross(foot, b), normal) >= 0.0 && norm(foot) > 0.0
    {
        let along = angle(a, foot);
        return (
            libm::asin(offset.abs().min(1.0)),
            (along / omega).clamp(0.0, 1.0),
        );
    }
    let (start, end) = (angle(p, a), angle(p, b));
    if start <= end {
        (start, 0.0)
    } else {
        (end, 1.0)
    }
}

/// The longitude/latitude bounds, in degrees, of the great elliptic arc
/// between two positions: [west, south, east, north], with east up to 360°
/// past west so a box across the antimeridian stays one interval. An arc may
/// reach beyond its ends' latitudes to its great ellipse's highest or lowest
/// point; an arc over a pole spans every longitude.
pub fn arc_bounds(p: Pos, q: Pos) -> [f64; 4] {
    let (a, b) = (direction(p), direction(q));
    let mut south = p[1].min(q[1]);
    let mut north = p[1].max(q[1]);
    let normal = cross(unit(a), unit(b));
    if norm(normal) > 0.0 {
        let normal = unit(normal);
        // The highest point of the great circle is the pole projected onto
        // its plane; the lowest is its opposite.
        for pole in [[0.0, 0.0, 1.0], [0.0, 0.0, -1.0]] {
            let offset = dot(pole, normal);
            let top = [
                pole[0] - offset * normal[0],
                pole[1] - offset * normal[1],
                pole[2] - offset * normal[2],
            ];
            if norm(top) > 0.0
                && dot(cross(unit(a), top), normal) >= 0.0
                && dot(cross(top, unit(b)), normal) >= 0.0
            {
                let latitude = position(top)[1];
                south = south.min(latitude);
                north = north.max(latitude);
            }
        }
    }
    if north >= 90.0 || south <= -90.0 {
        return [-180.0, south.max(-90.0), 180.0, north.min(90.0)];
    }
    // A minor arc not over a pole turns less than 180° in longitude, the
    // shorter way between its ends.
    // The far end is taken as written, a turn on when it lies west of the
    // near one, so a bound equals its vertex exactly.
    // Adding a whole turn before reducing can round a tiny westward delta
    // to zero, turning a narrow arc into a full-longitude box.
    let delta = q[0] - p[0];
    let turn = if delta >= 180.0 {
        delta - 360.0
    } else if delta < -180.0 {
        delta + 360.0
    } else {
        delta
    };
    let (west, far) = if turn >= 0.0 { (p[0], q[0]) } else { (q[0], p[0]) };
    let east = if far >= west { far } else { far + 360.0 };
    [west, south, east, north]
}

/// The longitude/latitude bounds of a WGS84 geometry: its positions, its
/// great elliptic arcs and, for an area, every longitude up to each pole its
/// region holds. [west, south, east, north] with east up to a turn past west
/// (`covering`); None for an empty value.
pub fn geometry_bounds(
    geometry: &crate::Geometry,
    work: &mut GeometryWork,
) -> Result<Option<[f64; 4]>, GeometryRefusal> {
    geometries_bounds(std::slice::from_ref(geometry), work)
}

/// Aggregate native bounds before closing longitude gaps. Unioning each
/// member's already closed box can fill the largest gap of the whole group.
pub fn geometries_bounds(
    geometries: &[crate::Geometry],
    work: &mut GeometryWork,
) -> Result<Option<[f64; 4]>, GeometryRefusal> {
    let Some(first) = geometries.first() else {
        return Ok(None);
    };
    first.space.validate_kernel()?;
    let mut positions = 0usize;
    for geometry in geometries {
        if geometry.space != first.space {
            return Err(crate::unsupported_space(
                "Bounds require one coordinate space",
            ));
        }
        work.charge(geometry.position_count().saturating_add(1))?;
        positions = positions.saturating_add(geometry.position_count());
    }
    if matches!(first.space.frame, CoordinateFrame::Cartesian { .. }) {
        let mut bounds: Option<[f64; 4]> = None;
        for geometry in geometries {
            geometry.for_each(|p| {
                bounds = Some(match bounds {
                    None => [p[0], p[1], p[0], p[1]],
                    Some(b) => [
                        b[0].min(p[0]),
                        b[1].min(p[1]),
                        b[2].max(p[0]),
                        b[3].max(p[1]),
                    ],
                });
            });
        }
        return Ok(bounds);
    }
    // Vertices, edge bounds and two possible poles per ring. Reserve before
    // collecting, not after allocating the temporary cover.
    work.allocate(positions.saturating_mul(128))?;
    fn collect(
        geometry: &crate::Geometry,
        boxes: &mut Vec<[f64; 4]>,
        work: &mut GeometryWork,
    ) -> Result<(), GeometryRefusal> {
        if let crate::Coords::Collection(members) = &geometry.coords {
            for member in members {
                collect(member, boxes, work)?;
            }
            return Ok(());
        }
        geometry.for_each(|p| boxes.push([p[0], p[1], p[0], p[1]]));
        let rings = geometry.polygons().into_iter().flatten();
        for path in geometry.lines().into_iter().chain(rings) {
            work.charge(path.len())?;
            boxes.extend(path.windows(2).map(|pair| arc_bounds(pair[0], pair[1])));
        }
        for polygon in geometry.polygons() {
            let Some(shell) = polygon.first() else {
                continue;
            };
            let Some(ring) = work.scoped(|work| Ring::of_positions(true, shell, work))? else {
                continue;
            };
            for (k, pole) in [(0, 90.0), (1, -90.0)] {
                if ring.left_contains(k) {
                    boxes.push([-180.0, pole, 180.0, pole]);
                }
            }
        }
        Ok(())
    }
    let mut boxes = Vec::new();
    for geometry in geometries {
        collect(geometry, &mut boxes, work)?;
    }
    if boxes.is_empty() {
        return Ok(None);
    }
    let south = boxes.iter().map(|b| b[1]).fold(f64::INFINITY, f64::min);
    let north = boxes.iter().map(|b| b[3]).fold(f64::NEG_INFINITY, f64::max);
    let (west, east) = covering(boxes.iter().map(|b| (b[0], b[2])));
    Ok(Some([west, south, east, north]))
}

/// The shortest longitude interval, as (west, east) with west in [-180, 180)
/// and east up to a turn past it, covering intervals given as (west, east)
/// with east at or past west: the complement of the widest uncovered gap.
pub fn covering(intervals: impl Iterator<Item = (f64, f64)>) -> (f64, f64) {
    // Each interval within [-180, 180], split where it passes the seam.
    let mut spans: Vec<(f64, f64)> = Vec::new();
    for (west, east) in intervals {
        let length = east - west;
        if length.is_nan() || length >= 360.0 {
            return (-180.0, 180.0);
        }
        // Whole turns shift both ends exactly.
        let shift = (west + 180.0).div_euclid(360.0) * 360.0;
        let (start, end) = (west - shift, east - shift);
        if end > 180.0 {
            spans.extend([(start, 180.0), (-180.0, end - 360.0)]);
        } else {
            spans.push((start, end));
        }
    }
    if spans.is_empty() {
        return (f64::INFINITY, f64::NEG_INFINITY);
    }
    spans.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut runs: Vec<(f64, f64)> = Vec::new();
    for (start, end) in spans {
        match runs.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => runs.push((start, end)),
        }
    }
    // The gap after each run, the last one's wrapping round to the first;
    // the covering runs from the gap's far side round to its near side.
    let n = runs.len();
    let mut widest: Option<(f64, f64, f64)> = None;
    for k in 0..n {
        let (gap, west, east) = if k + 1 < n {
            (runs[k + 1].0 - runs[k].1, runs[k + 1].0, runs[k].1 + 360.0)
        } else {
            (runs[0].0 + 360.0 - runs[k].1, runs[0].0, runs[k].1)
        };
        if gap > 0.0 && widest.is_none_or(|(widest, ..)| gap > widest) {
            widest = Some((gap, west, east));
        }
    }
    let Some((_, west, east)) = widest else {
        return (-180.0, 180.0);
    };
    let shift = (west + 180.0).div_euclid(360.0) * 360.0;
    (west - shift, east - shift)
}

#[cfg(test)]
#[path = "arcs_tests.rs"]
mod tests;
