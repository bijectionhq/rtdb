//! The metric of a `wgs84_sphere` frame: WGS84 longitude and latitude read
//! on a sphere of radius R, geodetic latitude as spherical latitude.
//!
//! Its edges (`great_circle`) are the sphere's minor great-circle arcs, which
//! are its geodesics. The unit vector (cos φ cos λ, cos φ sin λ, sin φ) is the
//! auxiliary-sphere direction (cos λ, sin λ, (1 − f) tan φ) scaled along z and
//! normalised, and a positive linear map keeps planes through the centre and
//! the minor arcs between directions: a great-circle edge is the same
//! longitude/latitude curve as WGS84's great elliptic one. Topology is
//! therefore decided on the auxiliary sphere for both frames (`arcs`), and
//! only the metric here differs: lengths are R times an angle and areas R²
//! times a spherical excess.
use crate::Pos;

/// The unit vector of a position on the sphere.
pub(crate) fn unit(p: Pos) -> [f64; 3] {
    let (sin_lon, cos_lon) = crate::geodesy::sincosd(p[0]);
    let (sin_lat, cos_lat) = crate::geodesy::sincosd(p[1].clamp(-90.0, 90.0));
    [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
}

fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(a: [f64; 3]) -> f64 {
    libm::sqrt(dot(a, a))
}

/// The central angle between two unit vectors, in radians, stable at every
/// separation.
pub(crate) fn angle_between(a: [f64; 3], b: [f64; 3]) -> f64 {
    libm::atan2(norm(cross(a, b)), dot(a, b))
}

/// The central angle between two positions, in radians.
pub(crate) fn angle(p: Pos, q: Pos) -> f64 {
    angle_between(unit(p), unit(q))
}

/// The inverse problem: the central angle from p to q in radians and the
/// forward azimuths at p and at q, in degrees clockwise from north.
pub(crate) fn inverse(p: Pos, q: Pos) -> (f64, f64, f64) {
    let (phi1, phi2) = (p[1].to_radians(), q[1].to_radians());
    let dlon = (q[0] - p[0]).to_radians();
    let (s1, c1) = (libm::sin(phi1), libm::cos(phi1));
    let (s2, c2) = (libm::sin(phi2), libm::cos(phi2));
    let (sd, cd) = (libm::sin(dlon), libm::cos(dlon));
    let azi1 = libm::atan2(sd * c2, c1 * s2 - s1 * c2 * cd).to_degrees();
    let azi2 = libm::atan2(sd * c1, -s1 * c2 + c1 * s2 * cd).to_degrees();
    (angle(p, q), azi1, azi2)
}

/// The direct problem: the position `sigma` radians from p along the great
/// circle leaving at `azimuth` degrees, and the forward azimuth there.
pub(crate) fn direct(p: Pos, azimuth: f64, sigma: f64) -> (Pos, f64) {
    let (phi1, lon1) = (p[1].to_radians(), p[0].to_radians());
    let theta = azimuth.to_radians();
    let (s1, c1) = (libm::sin(phi1), libm::cos(phi1));
    let (ss, cs) = (libm::sin(sigma), libm::cos(sigma));
    let (st, ct) = (libm::sin(theta), libm::cos(theta));
    let s2 = (s1 * cs + c1 * ss * ct).clamp(-1.0, 1.0);
    let phi2 = libm::asin(s2);
    let lon2 = lon1 + libm::atan2(st * ss * c1, cs - s1 * s2);
    let azi2 = libm::atan2(st * c1, c1 * cs * ct - s1 * ss).to_degrees();
    let mut lon = lon2.to_degrees();
    lon = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if lon == -180.0 && lon2 > 0.0 {
        lon = 180.0;
    }
    ([lon + 0.0, phi2.to_degrees()], azi2)
}

/// The angle, in radians, from p to the nearest point of the minor arc ab.
pub(crate) fn point_arc_angle(p: Pos, a: Pos, b: Pos) -> f64 {
    let (pv, av, bv) = (unit(p), unit(a), unit(b));
    let ends = angle_between(pv, av).min(angle_between(pv, bv));
    let n = cross(av, bv);
    let length = norm(n);
    if length == 0.0 {
        return ends;
    }
    let n = n.map(|x| x / length);
    // The foot of p on the arc's circle lies inside the arc when it is on
    // the inner side of both end planes.
    let foot = [0, 1, 2].map(|i| pv[i] - dot(pv, n) * n[i]);
    if norm(foot) == 0.0 {
        return ends;
    }
    if dot(cross(av, foot), n) >= 0.0 && dot(cross(foot, bv), n) >= 0.0 {
        let off = libm::asin(dot(pv, n).abs().min(1.0));
        return off.min(ends);
    }
    ends
}
