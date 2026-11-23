//! Distance between great elliptic arcs on the WGS84 ellipsoid. Arcs that
//! cross or touch are 0 apart. Otherwise the auxiliary sphere puts the
//! minimum at an end of one arc, so the distance is the least of four
//! point-to-arc distances. A point-to-arc distance starts from the point's
//! foot on the auxiliary sphere and is refined along the arc with Karney
//! geodesics. It is the ellipsoidal distance of two real points, so it never
//! understates, and the single minimum of the auxiliary-sphere distance along
//! the arc keeps the refinement in the right basin. A radian of
//! auxiliary-sphere angle spans at least b metres on the ellipsoid, which
//! bounds every distance below for pruning. Exhausting work refuses without
//! publishing an estimate.
use super::point_distance;
use crate::{
    arcs,
    geodesy::{
        SEMI_MAJOR_AXIS_METRES,
        SEMI_MINOR_AXIS_METRES,
    },
    GeometryRefusal,
    GeometryWork,
    Pos,
};

/// Radius of the sphere on which coordinate rectangles are bounded, in
/// metres.
pub(crate) const R: f64 = SEMI_MAJOR_AXIS_METRES;
/// The ellipsoid's metric is at least its smallest radius of curvature times
/// the unit sphere's in geodetic coordinates.
pub(crate) const LOWER_SCALE: f64 = super::MIN_CURVATURE_RADIUS_METRES / R;
/// Work charged for one ellipsoidal point distance.
const GEODESIC_WORK: usize = 4;
/// Golden-section searches resolve an arc to this many metres.
const RESOLUTION_METRES: f64 = 1e-4;
const MAX_STEPS: usize = 96;

/// A lower bound in metres for a distance of `angle` radians on the
/// auxiliary sphere, with a margin for the angle's rounding.
pub(crate) fn lower_bound(angle: f64) -> f64 {
    (SEMI_MINOR_AXIS_METRES * angle * (1.0 - 1e-12) - 1e-6).max(0.0)
}

/// The position `fraction` of the way along the geodesic from `a` to `b`.
pub(crate) fn geodesic_position(a: Pos, b: Pos, fraction: f64) -> Result<Pos, GeometryRefusal> {
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return Err(crate::invalid("Geodesic fraction must be in [0, 1]"));
    }
    if fraction == 0.0 || a == b {
        return Ok(a);
    }
    if fraction == 1.0 {
        return Ok(b);
    }
    let line = crate::geodesy::inverse(a[1], a[0], b[1], b[0]);
    if line.s12 >= super::MAX_POINT_DISTANCE_METRES - 1e-3 {
        return Err(crate::invalid(
            "Antipodal endpoints have no unique geodesic",
        ));
    }
    let point = crate::geodesy::direct(a[1], a[0], line.azi1, fraction * line.s12);
    Ok([point.lon2, point.lat2])
}

/// The position `fraction` of the way along the great elliptic arc from `a`
/// to `b`, by auxiliary-sphere angle; the ends are returned exactly.
pub(crate) fn arc_position(a: Pos, b: Pos, fraction: f64) -> Pos {
    if fraction <= 0.0 {
        a
    } else if fraction >= 1.0 {
        b
    } else {
        arcs::position(arcs::interpolate(
            arcs::direction(a),
            arcs::direction(b),
            fraction,
        ))
    }
}

/// The distance from `p` to the great elliptic arc a-b, or `best` when the
/// arc cannot come nearer.
pub(crate) fn point_to_arc(
    p: Pos,
    a: Pos,
    b: Pos,
    best: f64,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    const RATIO: f64 = 0.618_033_988_749_894_9;
    work.charge(1)?;
    let (pv, av, bv) = (arcs::direction(p), arcs::direction(a), arcs::direction(b));
    if av == bv {
        work.charge(GEODESIC_WORK)?;
        return Ok(best.min(point_distance(p, a)));
    }
    let (angle, along) = arcs::nearest_on_arc(pv, av, bv);
    if lower_bound(angle) >= best {
        return Ok(best);
    }
    let evaluate = |t: f64, work: &mut GeometryWork| -> Result<f64, GeometryRefusal> {
        work.charge(GEODESIC_WORK)?;
        Ok(point_distance(p, arc_position(a, b, t)))
    };
    // The distance along the arc has one minimum; golden-section search over
    // the whole arc finds it, and the auxiliary-sphere foot and the ends seed
    // the answer.
    let mut found = best.min(evaluate(along, work)?);
    for end in [0.0, 1.0] {
        found = found.min(evaluate(end, work)?);
    }
    let length = SEMI_MAJOR_AXIS_METRES * arcs::angle(av, bv);
    let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
    let (mut c, mut d) = (hi - RATIO * (hi - lo), lo + RATIO * (hi - lo));
    let (mut fc, mut fd) = (evaluate(c, work)?, evaluate(d, work)?);
    found = found.min(fc).min(fd);
    for _ in 0..MAX_STEPS {
        if (hi - lo) * length <= RESOLUTION_METRES {
            break;
        }
        if fc <= fd {
            (hi, d, fd) = (d, c, fc);
            c = hi - RATIO * (hi - lo);
            fc = evaluate(c, work)?;
            found = found.min(fc);
        } else {
            (lo, c, fc) = (c, d, fd);
            d = lo + RATIO * (hi - lo);
            fd = evaluate(d, work)?;
            found = found.min(fd);
        }
    }
    Ok(found)
}

/// Whether two great elliptic arcs share a point: a crossing, a shared
/// vertex, or a vertex of one on the other.
pub(crate) fn arcs_meet(
    a: (Pos, Pos),
    b: (Pos, Pos),
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    let (a0, a1) = (arcs::direction(a.0), arcs::direction(a.1));
    let (b0, b1) = (arcs::direction(b.0), arcs::direction(b.1));
    if a0 == b0 || a0 == b1 || a1 == b0 || a1 == b1 {
        return Ok(true);
    }
    if arcs::crossing_sign(a0, a1, b0, b1, work)? > 0 {
        return Ok(true);
    }
    for (p, (e, f)) in [
        (a0, (b0, b1)),
        (a1, (b0, b1)),
        (b0, (a0, a1)),
        (b1, (a0, a1)),
    ] {
        if e != f && arcs::on_arc(p, e, f, work)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn segment_distance(
    a: (Pos, Pos),
    b: (Pos, Pos),
    mut best: f64,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    work.charge(1)?;
    if arcs_meet(a, b, work)? {
        return Ok(0.0);
    }
    // Disjoint arcs are nearest at an end of one of them.
    for (p, (e, f)) in [(a.0, b), (a.1, b), (b.0, a), (b.1, a)] {
        best = point_to_arc(p, e, f, best, work)?;
    }
    Ok(best)
}
