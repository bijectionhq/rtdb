//! WGS84 ellipsoid geodesy: principal radii of curvature, zone areas and the
//! geodesic inverse problem. Every transcendental function goes through the
//! pure-Rust `libm` crate, so native and Wasm results are bit-identical.
//!
//! [`inverse`] ports `geod_geninverse` from Charles Karney's GeographicLib C
//! library (C. F. F. Karney, "Algorithms for geodesics", J. Geodesy 87, 43–55,
//! 2013), with series order 6, specialised to the oblate WGS84 ellipsoid and
//! without the area (S12) output. GeographicLib carries this notice:
//!
//! The MIT License (MIT).
//!
//! Copyright (c) 2012-2025, Charles Karney
//!
//! Permission is hereby granted, free of charge, to any person
//! obtaining a copy of this software and associated documentation
//! files (the "Software"), to deal in the Software without
//! restriction, including without limitation the rights to use, copy,
//! modify, merge, publish, distribute, sublicense, and/or sell copies
//! of the Software, and to permit persons to whom the Software is
//! furnished to do so, subject to the following conditions:
//!
//! The above copyright notice and this permission notice shall be
//! included in all copies or substantial portions of the Software.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//! EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//! MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//! NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//! HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//! WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
//! DEALINGS IN THE SOFTWARE.
use std::f64::consts::PI;

/// WGS84 semi-major axis a, in metres.
pub const SEMI_MAJOR_AXIS_METRES: f64 = 6_378_137.0;
/// WGS84 flattening f.
pub const FLATTENING: f64 = 1.0 / 298.257_223_563;
/// Semi-minor axis b = a(1 − f), in metres.
pub const SEMI_MINOR_AXIS_METRES: f64 = SEMI_MAJOR_AXIS_METRES * F1;
/// First eccentricity squared e² = f(2 − f).
pub const ECCENTRICITY_SQUARED: f64 = FLATTENING * (2.0 - FLATTENING);
/// Second eccentricity squared e'² = e²/(1 − f)².
pub const SECOND_ECCENTRICITY_SQUARED: f64 = ECCENTRICITY_SQUARED / (F1 * F1);
/// Third flattening n = f/(2 − f).
pub const THIRD_FLATTENING: f64 = FLATTENING / (2.0 - FLATTENING);

const A: f64 = SEMI_MAJOR_AXIS_METRES;
const B: f64 = SEMI_MINOR_AXIS_METRES;
const F: f64 = FLATTENING;
const F1: f64 = 1.0 - FLATTENING;
const E2: f64 = ECCENTRICITY_SQUARED;
const EP2: f64 = SECOND_ECCENTRICITY_SQUARED;
const N: f64 = THIRD_FLATTENING;

const DEGREE: f64 = PI / 180.0;
/// sqrt(f64::MIN_POSITIVE), exactly 2^-511.
const TINY: f64 = f64::from_bits((1023 - 511) << 52);
const TOL0: f64 = f64::EPSILON;
const TOL1: f64 = 200.0 * TOL0;
/// sqrt(f64::EPSILON), exactly 2^-26.
const TOL2: f64 = f64::from_bits((1023 - 26) << 52);
const TOLB: f64 = TOL0;
const XTHRESH: f64 = 1000.0 * TOL2;
const MAXIT1: u32 = 20;
const MAXIT2: u32 = MAXIT1 + f64::MANTISSA_DIGITS + 10;

/// Series order of every expansion.
const ORDER: usize = 6;
const NC1: usize = ORDER;
const NC1P: usize = ORDER;
const NC2: usize = ORDER;
const NC3: usize = ORDER;

/// Meridional radius of curvature M(φ) = a(1 − e²)/(1 − e² sin²φ)^{3/2}, in
/// metres, for a geodetic latitude in radians.
pub fn meridional_radius(phi_rad: f64) -> f64 {
    let s = libm::sin(phi_rad);
    let w = 1.0 - E2 * s * s;
    A * (1.0 - E2) / (w * w.sqrt())
}

/// Prime vertical radius of curvature N(φ) = a/sqrt(1 − e² sin²φ), in
/// metres, for a geodetic latitude in radians.
pub fn prime_vertical_radius(phi_rad: f64) -> f64 {
    let s = libm::sin(phi_rad);
    A / (1.0 - E2 * s * s).sqrt()
}

/// Ellipsoid surface area between the equator and latitude φ (radians) per
/// radian of longitude, in square metres: b²/2 · [sinφ/(1 − e² sin²φ) +
/// atanh(e sinφ)/e]. Exactly odd in φ; both terms share a sign, so small
/// latitudes keep full relative accuracy.
pub fn zone_area_per_radian(phi_rad: f64) -> f64 {
    let s = libm::sin(phi_rad.abs());
    let e = E2.sqrt();
    let area = B * B / 2.0 * (s / (1.0 - E2 * s * s) + libm::atanh(e * s) / e);
    area.copysign(phi_rad)
}

/// Solution of the geodesic inverse problem between two points.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Inverse {
    /// Geodesic distance, in metres.
    pub s12: f64,
    /// Forward azimuth at the first point, in degrees clockwise from north.
    pub azi1: f64,
    /// Forward azimuth at the second point, in degrees clockwise from north.
    pub azi2: f64,
    /// Reduced length m12, in metres.
    pub m12: f64,
    /// Geodesic scale M12 of the second point relative to the first.
    pub geodesic_scale12: f64,
    /// Geodesic scale M21 of the first point relative to the second.
    pub geodesic_scale21: f64,
    /// Arc length on the auxiliary sphere, in degrees.
    pub a12: f64,
}

/// Shortest geodesic between two points given in degrees. Latitudes beyond
/// ±90° and non-finite inputs yield NaN outputs.
pub fn inverse(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Inverse {
    // Longitude difference in [-180, 180], carried with its rounding error.
    let (mut lon12, mut lon12s) = ang_diff(lon1, lon2);
    let mut lonsign = if lon12.is_sign_negative() { -1.0 } else { 1.0 };
    lon12 *= lonsign;
    lon12s *= lonsign;
    let lam12 = lon12 * DEGREE;
    let (slam12, clam12) = sincosde(lon12, lon12s);
    // Supplementary longitude difference.
    let lon12s = (180.0 - lon12) - lon12s;

    let mut lat1 = ang_round(lat_fix(lat1));
    let mut lat2 = ang_round(lat_fix(lat2));
    // Point 1 has the larger absolute latitude; a NaN latitude becomes lat1.
    let swapp = if lat1.abs() < lat2.abs() || lat2.is_nan() {
        -1.0
    } else {
        1.0
    };
    if swapp < 0.0 {
        lonsign *= -1.0;
        std::mem::swap(&mut lat1, &mut lat2);
    }
    let latsign = if lat1.is_sign_negative() { 1.0 } else { -1.0 };
    lat1 *= latsign;
    lat2 *= latsign;
    // Now 0 <= lon12 <= 180, -90 <= lat1 <= -0 and lat1 <= lat2 <= -lat1.

    let (sbet1, cbet1) = sincosd(lat1);
    let (sbet1, cbet1) = norm2(F1 * sbet1, cbet1);
    let cbet1 = cbet1.max(TINY);
    let (sbet2, cbet2) = sincosd(lat2);
    let (mut sbet2, mut cbet2) = norm2(F1 * sbet2, cbet2);
    cbet2 = cbet2.max(TINY);
    // Force bet2 = ±bet1 exactly when the sensitive difference vanishes.
    if cbet1 < -sbet1 {
        if cbet2 == cbet1 {
            sbet2 = sbet1.copysign(sbet2);
        }
    } else if sbet2.abs() == -sbet1 {
        cbet2 = cbet1;
    }
    let dn1 = (1.0 + EP2 * sq(sbet1)).sqrt();
    let dn2 = (1.0 + EP2 * sq(sbet2)).sqrt();

    let mut a12 = 0.0;
    let (mut salp1, mut calp1, mut salp2, mut calp2) = (0.0, 0.0, 0.0, 0.0);
    let (mut s12x, mut m12x) = (0.0, 0.0);
    let (mut scale12, mut scale21) = (0.0, 0.0);

    let mut meridian = lat1 == -90.0 || slam12 == 0.0;
    if meridian {
        // Head to the target longitude and arrive heading north.
        calp1 = clam12;
        salp1 = slam12;
        calp2 = 1.0;
        salp2 = 0.0;
        let (ssig1, csig1) = (sbet1, calp1 * cbet1);
        let (ssig2, csig2) = (sbet2, calp2 * cbet2);
        let mut sig12 = libm::atan2(
            (csig1 * ssig2 - ssig1 * csig2).max(0.0) + 0.0,
            csig1 * csig2 + ssig1 * ssig2,
        );
        let l = lengths(
            N, sig12, ssig1, csig1, dn1, ssig2, csig2, dn2, cbet1, cbet2, true,
        );
        s12x = l.s12b;
        m12x = l.m12b;
        scale12 = l.scale12;
        scale21 = l.scale21;
        // Zero-length geodesics may yield m12 < 0.
        if sig12 < TOL2 || m12x >= 0.0 {
            if sig12 < 3.0 * TINY || (sig12 < TOL0 && (s12x < 0.0 || m12x < 0.0)) {
                sig12 = 0.0;
                m12x = 0.0;
                s12x = 0.0;
            }
            m12x *= B;
            s12x *= B;
            a12 = sig12 / DEGREE;
        } else {
            // m12 < 0: too close to antipodal for the meridian to be shortest.
            meridian = false;
        }
    }

    if !meridian && sbet1 == 0.0 && lon12s >= F * 180.0 {
        // Equatorial geodesic.
        calp1 = 0.0;
        calp2 = 0.0;
        salp1 = 1.0;
        salp2 = 1.0;
        s12x = A * lam12;
        let sig12 = lam12 / F1;
        m12x = B * libm::sin(sig12);
        scale12 = libm::cos(sig12);
        scale21 = scale12;
        a12 = lon12 / F1;
    } else if !meridian {
        let start = inverse_start(sbet1, cbet1, sbet2, cbet2, lam12, slam12, clam12);
        salp1 = start.salp1;
        calp1 = start.calp1;
        if start.sig12 >= 0.0 {
            // Really short line solved on the auxiliary sphere.
            salp2 = start.salp2;
            calp2 = start.calp2;
            let dnm = start.dnm;
            s12x = start.sig12 * B * dnm;
            m12x = sq(dnm) * B * libm::sin(start.sig12 / dnm);
            scale12 = libm::cos(start.sig12 / dnm);
            scale21 = scale12;
            a12 = start.sig12 / DEGREE;
        } else {
            // Newton's method on lambda12(alp1) - lam12, restarting from the
            // bisection of a bracketing range whenever a step leaves it.
            let mut numit = 0;
            let (mut salp1a, mut calp1a, mut salp1b, mut calp1b) = (TINY, 1.0, TINY, -1.0);
            let mut tripn = false;
            let mut tripb = false;
            let lam = loop {
                let lam = lambda12(
                    sbet1,
                    cbet1,
                    dn1,
                    sbet2,
                    cbet2,
                    dn2,
                    salp1,
                    calp1,
                    slam12,
                    clam12,
                    numit < MAXIT1,
                );
                let v = lam.lam12;
                let threshold = (if tripn { 8.0 } else { 1.0 }) * TOL0;
                // NaN escapes like GeographicLib's reversed test.
                if tripb || v.abs() < threshold || v.is_nan() || numit == MAXIT2 {
                    break lam;
                }
                if v > 0.0 && (numit > MAXIT1 || calp1 / salp1 > calp1b / salp1b) {
                    salp1b = salp1;
                    calp1b = calp1;
                } else if v < 0.0 && (numit > MAXIT1 || calp1 / salp1 < calp1a / salp1a) {
                    salp1a = salp1;
                    calp1a = calp1;
                }
                let mut stepped = false;
                if numit < MAXIT1 && lam.dlam12 > 0.0 {
                    let dalp1 = -v / lam.dlam12;
                    if dalp1.abs() < PI {
                        let (sdalp1, cdalp1) = (libm::sin(dalp1), libm::cos(dalp1));
                        let nsalp1 = salp1 * cdalp1 + calp1 * sdalp1;
                        if nsalp1 > 0.0 {
                            (salp1, calp1) = norm2(nsalp1, calp1 * cdalp1 - salp1 * sdalp1);
                            // Slope can vanish, so converge on epsilon instead.
                            tripn = v.abs() <= 16.0 * TOL0;
                            stepped = true;
                        }
                    }
                }
                if !stepped {
                    (salp1, calp1) = norm2((salp1a + salp1b) / 2.0, (calp1a + calp1b) / 2.0);
                    tripn = false;
                    tripb = (salp1a - salp1).abs() + (calp1a - calp1) < TOLB
                        || (salp1 - salp1b).abs() + (calp1 - calp1b) < TOLB;
                }
                numit += 1;
            };
            salp2 = lam.salp2;
            calp2 = lam.calp2;
            let l = lengths(
                lam.eps, lam.sig12, lam.ssig1, lam.csig1, dn1, lam.ssig2, lam.csig2, dn2, cbet1,
                cbet2, true,
            );
            m12x = l.m12b * B;
            s12x = l.s12b * B;
            scale12 = l.scale12;
            scale21 = l.scale21;
            a12 = lam.sig12 / DEGREE;
        }
    }

    // Adding zero turns -0 into +0.
    let s12 = s12x + 0.0;
    let m12 = m12x + 0.0;

    // Undo the canonical transformation.
    if swapp < 0.0 {
        std::mem::swap(&mut salp1, &mut salp2);
        std::mem::swap(&mut calp1, &mut calp2);
        std::mem::swap(&mut scale12, &mut scale21);
    }
    salp1 *= swapp * lonsign;
    calp1 *= swapp * latsign;
    salp2 *= swapp * lonsign;
    calp2 *= swapp * latsign;

    Inverse {
        s12,
        azi1: atan2d(salp1, calp1),
        azi2: atan2d(salp2, calp2),
        m12,
        geodesic_scale12: scale12,
        geodesic_scale21: scale21,
        a12,
    }
}

/// Solution of the geodesic direct problem from one point.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Direct {
    /// Latitude of the second point, in degrees.
    pub lat2: f64,
    /// Longitude of the second point, in degrees in [-180, 180].
    pub lon2: f64,
    /// Forward azimuth at the second point, in degrees clockwise from north.
    pub azi2: f64,
}

/// The point `s12` metres along the geodesic leaving (`lat1`, `lon1`) at
/// azimuth `azi1`, all angles in degrees. Non-finite inputs yield NaN outputs.
pub fn direct(lat1: f64, lon1: f64, azi1: f64, s12: f64) -> Direct {
    // GeographicLib's geodesic line in distance mode (geod_lineinit and
    // geod_genposition), without longitude unrolling.
    let azi1 = ang_normalize(azi1);
    let (salp1, calp1) = sincosd(ang_round(azi1));
    let (sbet1, cbet1) = sincosd(ang_round(lat_fix(lat1)));
    let (sbet1, cbet1) = norm2(F1 * sbet1, cbet1);
    let cbet1 = cbet1.max(TINY);
    // alp0 in [0, pi/2 - |bet1|] is the azimuth at the equator crossing.
    let salp0 = salp1 * cbet1;
    let calp0 = libm::hypot(calp1, salp1 * sbet1);
    let somg1 = salp0 * sbet1;
    let csig1 = if sbet1 != 0.0 || calp1 != 0.0 {
        cbet1 * calp1
    } else {
        1.0
    };
    let comg1 = csig1;
    let (ssig1, csig1) = norm2(sbet1, csig1);
    let k2 = sq(calp0) * EP2;
    let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);

    let a1m1 = a1m1f(eps);
    let mut c1a = [0.0; ORDER + 1];
    c1f(eps, &mut c1a);
    let b11 = sin_series(ssig1, csig1, &c1a, NC1);
    let (s, c) = (libm::sin(b11), libm::cos(b11));
    // tau1 = sig1 + B11.
    let (stau1, ctau1) = (ssig1 * c + csig1 * s, csig1 * c - ssig1 * s);
    let mut c1pa = [0.0; ORDER + 1];
    c1pf(eps, &mut c1pa);
    let mut c3a = [0.0; ORDER + 1];
    c3f(eps, &mut c3a);
    let a3c = -F * salp0 * a3f(eps);
    let b31 = sin_series(ssig1, csig1, &c3a, NC3 - 1);

    let tau12 = s12 / (B * (1.0 + a1m1));
    let (s, c) = (libm::sin(tau12), libm::cos(tau12));
    let b12 = -sin_series(stau1 * c + ctau1 * s, ctau1 * c - stau1 * s, &c1pa, NC1P);
    let sig12 = tau12 - (b12 - b11);
    let (ssig12, csig12) = (libm::sin(sig12), libm::cos(sig12));
    let ssig2 = ssig1 * csig12 + csig1 * ssig12;
    let mut csig2 = csig1 * csig12 - ssig1 * ssig12;
    let sbet2 = calp0 * ssig2;
    let mut cbet2 = libm::hypot(salp0, calp0 * csig2);
    if cbet2 == 0.0 {
        // The geodesic reaches a pole.
        cbet2 = TINY;
        csig2 = TINY;
    }
    let (salp2, calp2) = (salp0, calp0 * csig2);
    let (somg2, comg2) = (salp0 * ssig2, csig2);
    let omg12 = libm::atan2(somg2 * comg1 - comg2 * somg1, comg2 * comg1 + somg2 * somg1);
    let lam12 = omg12 + a3c * (sig12 + (sin_series(ssig2, csig2, &c3a, NC3 - 1) - b31));
    let lon12 = lam12 / DEGREE;
    Direct {
        lat2: atan2d(sbet2, F1 * cbet2),
        lon2: ang_normalize(ang_normalize(lon1) + ang_normalize(lon12)),
        azi2: atan2d(salp2, calp2),
    }
}

fn sq(x: f64) -> f64 {
    x * x
}

/// Error-free sum: u + v = s + t exactly, with s = round(u + v).
fn sum_exact(u: f64, v: f64) -> (f64, f64) {
    let s = u + v;
    let up = s - v;
    let vpp = s - up;
    let up = up - u;
    let vpp = vpp - v;
    let t = if s != 0.0 { 0.0 - (up + vpp) } else { s };
    (s, t)
}

/// Horner evaluation of the order-`n` polynomial whose coefficients start at
/// `p[o]`, highest degree first.
const fn polyval(n: usize, p: &[f64], o: usize, x: f64) -> f64 {
    let mut y = p[o];
    let mut i = 1;
    while i <= n {
        y = y * x + p[o + i];
        i += 1;
    }
    y
}

fn norm2(sinx: f64, cosx: f64) -> (f64, f64) {
    let r = libm::hypot(sinx, cosx);
    (sinx / r, cosx / r)
}

fn lat_fix(x: f64) -> f64 {
    if x.abs() > 90.0 {
        f64::NAN
    } else {
        x
    }
}

/// The angle reduced to [-180, 180], keeping the sign of ±180.
fn ang_normalize(x: f64) -> f64 {
    let y = libm::remainder(x, 360.0);
    if y.abs() == 180.0 {
        180.0_f64.copysign(x)
    } else {
        y
    }
}

/// Difference y − x reduced to [-180, 180], with its rounding error.
fn ang_diff(x: f64, y: f64) -> (f64, f64) {
    let (d, t) = sum_exact(libm::remainder(-x, 360.0), libm::remainder(y, 360.0));
    let (mut d, t) = sum_exact(libm::remainder(d, 360.0), t);
    // Fix the sign of -180, 0 and 180.
    if d == 0.0 || d.abs() == 180.0 {
        d = d.copysign(if t == 0.0 { y - x } else { -t });
    }
    (d, t)
}

/// Rounds tiny angles to multiples of 2^-57 so values near zero become zero.
fn ang_round(x: f64) -> f64 {
    const Z: f64 = 1.0 / 16.0;
    let y = x.abs();
    let w = Z - y;
    let y = if w > 0.0 { Z - w } else { y };
    y.copysign(x)
}

pub(crate) fn sincosd(x: f64) -> (f64, f64) {
    let (r, q) = libm::remquo(x, 90.0);
    quadrant_sincos(r, q, x)
}

/// sincosd of x + t, where t is a small correction to x.
fn sincosde(x: f64, t: f64) -> (f64, f64) {
    let (r, q) = libm::remquo(x, 90.0);
    quadrant_sincos(ang_round(r + t), q, x)
}

/// Maps sin/cos of the exactly reduced angle r, |r| <= 45, to quadrant q.
fn quadrant_sincos(r: f64, q: i32, x: f64) -> (f64, f64) {
    let r = r * DEGREE;
    let (s, c) = (libm::sin(r), libm::cos(r));
    let (sinx, cosx) = match q & 3 {
        0 => (s, c),
        1 => (c, -s),
        2 => (-s, -c),
        _ => (-c, s),
    };
    let sinx = if sinx == 0.0 { sinx.copysign(x) } else { sinx };
    (sinx, cosx + 0.0)
}

/// atan2 in degrees, evaluated in [-45, 45] before mapping to the quadrant.
fn atan2d(y: f64, x: f64) -> f64 {
    let (mut y, mut x, mut q) = (y, x, 0);
    if y.abs() > x.abs() {
        std::mem::swap(&mut x, &mut y);
        q = 2;
    }
    if x.is_sign_negative() {
        x = -x;
        q += 1;
    }
    let ang = libm::atan2(y, x) / DEGREE;
    match q {
        1 => 180.0_f64.copysign(y) - ang,
        2 => 90.0 - ang,
        3 => -90.0 + ang,
        _ => ang,
    }
}

/// Clenshaw sum of c[i] sin(2 i x) for i in 1..=n.
fn sin_series(sinx: f64, cosx: f64, c: &[f64], n: usize) -> f64 {
    let mut k = n + 1;
    let ar = 2.0 * (cosx - sinx) * (cosx + sinx);
    let mut y0 = 0.0;
    if n & 1 == 1 {
        k -= 1;
        y0 = c[k];
    }
    let mut y1 = 0.0;
    for _ in 0..n / 2 {
        k -= 1;
        y1 = ar * y0 - y1 + c[k];
        k -= 1;
        y0 = ar * y1 - y0 + c[k];
    }
    2.0 * sinx * cosx * y0
}

struct Lengths {
    s12b: f64,
    m12b: f64,
    scale12: f64,
    scale21: f64,
}

/// Distance and reduced length divided by b, and the geodesic scales. Without
/// `distance`, only m12b is meaningful.
fn lengths(
    eps: f64,
    sig12: f64,
    ssig1: f64,
    csig1: f64,
    dn1: f64,
    ssig2: f64,
    csig2: f64,
    dn2: f64,
    cbet1: f64,
    cbet2: f64,
    distance: bool,
) -> Lengths {
    let mut ca = [0.0; ORDER + 1];
    let mut cb = [0.0; ORDER + 1];
    let a1m1 = a1m1f(eps);
    c1f(eps, &mut ca);
    let a2m1 = a2m1f(eps);
    c2f(eps, &mut cb);
    let m0 = a1m1 - a2m1;
    let a2 = 1.0 + a2m1;
    let a1 = 1.0 + a1m1;
    let mut s12b = 0.0;
    let j12 = if distance {
        let b1 = sin_series(ssig2, csig2, &ca, NC1) - sin_series(ssig1, csig1, &ca, NC1);
        s12b = a1 * (sig12 + b1);
        let b2 = sin_series(ssig2, csig2, &cb, NC2) - sin_series(ssig1, csig1, &cb, NC2);
        m0 * sig12 + (a1 * b1 - a2 * b2)
    } else {
        for (b, a) in cb.iter_mut().zip(ca).skip(1) {
            *b = a1 * a - a2 * *b;
        }
        m0 * sig12 + (sin_series(ssig2, csig2, &cb, NC2) - sin_series(ssig1, csig1, &cb, NC2))
    };
    // Parenthesised products cancel accurately for coincident points.
    let m12b = dn2 * (csig1 * ssig2) - dn1 * (ssig1 * csig2) - csig1 * csig2 * j12;
    let csig12 = csig1 * csig2 + ssig1 * ssig2;
    let t = EP2 * (cbet1 - cbet2) * (cbet1 + cbet2) / (dn1 + dn2);
    Lengths {
        s12b,
        m12b,
        scale12: csig12 + (t * ssig2 - csig2 * j12) * ssig1 / dn1,
        scale21: csig12 - (t * ssig1 - csig1 * j12) * ssig2 / dn2,
    }
}

/// Positive root k of k^4 + 2k^3 − (x² + y² − 1)k² − 2y²k − y² = 0.
fn astroid(x: f64, y: f64) -> f64 {
    let p = sq(x);
    let q = sq(y);
    let r = (p + q - 1.0) / 6.0;
    if q == 0.0 && r <= 0.0 {
        // y = 0 with |x| <= 1.
        return 0.0;
    }
    // S = r^3 s and the discriminant of the quadratic for T^3; multiplying
    // through by r avoids dividing by zero when r = 0.
    let s = p * q / 4.0;
    let r2 = sq(r);
    let r3 = r * r2;
    let disc = s * (s + 2.0 * r3);
    let mut u = r;
    if disc >= 0.0 {
        let mut t3 = s + r3;
        // The sign choice maximises |T3| and avoids cancellation.
        t3 += if t3 < 0.0 { -disc.sqrt() } else { disc.sqrt() };
        let t = libm::cbrt(t3);
        u += t + if t != 0.0 { r2 / t } else { 0.0 };
    } else {
        // Complex T; this cube root avoids cancellation, and disc < 0 implies r < 0.
        let ang = libm::atan2((-disc).sqrt(), -(s + r3));
        u += 2.0 * r * libm::cos(ang / 3.0);
    }
    let v = (sq(u) + q).sqrt();
    let uv = if u < 0.0 { q / (v - u) } else { u + v };
    let w = (uv - q) / (2.0 * v);
    uv / ((uv + sq(w)).sqrt() + w)
}

struct Start {
    sig12: f64,
    salp1: f64,
    calp1: f64,
    salp2: f64,
    calp2: f64,
    dnm: f64,
}

/// Starting azimuth for Newton's method, or the full solution (sig12 >= 0)
/// for really short lines.
fn inverse_start(
    sbet1: f64,
    cbet1: f64,
    sbet2: f64,
    cbet2: f64,
    lam12: f64,
    slam12: f64,
    clam12: f64,
) -> Start {
    let mut sig12 = -1.0;
    let (mut salp2, mut calp2, mut dnm) = (0.0, 0.0, 0.0);
    // bet12 = bet2 - bet1 in [0, pi); bet12a = bet2 + bet1 in (-pi, 0].
    let sbet12 = sbet2 * cbet1 - cbet2 * sbet1;
    let cbet12 = cbet2 * cbet1 + sbet2 * sbet1;
    let sbet12a = sbet2 * cbet1 + cbet2 * sbet1;
    let shortline = cbet12 >= 0.0 && sbet12 < 0.5 && cbet2 * lam12 < 0.5;
    let (mut somg12, mut comg12) = if shortline {
        // sin((bet1 + bet2)/2)^2.
        let mut sbetm2 = sq(sbet1 + sbet2);
        sbetm2 /= sbetm2 + sq(cbet1 + cbet2);
        dnm = (1.0 + EP2 * sbetm2).sqrt();
        let omg12 = lam12 / (F1 * dnm);
        (libm::sin(omg12), libm::cos(omg12))
    } else {
        (slam12, clam12)
    };

    let mut salp1 = cbet2 * somg12;
    let mut calp1 = if comg12 >= 0.0 {
        sbet12 + cbet2 * sbet1 * sq(somg12) / (1.0 + comg12)
    } else {
        sbet12a - cbet2 * sbet1 * sq(somg12) / (1.0 - comg12)
    };
    let ssig12 = libm::hypot(salp1, calp1);
    let csig12 = sbet1 * sbet2 + cbet1 * cbet2 * comg12;

    if shortline && ssig12 < etol2() {
        salp2 = cbet1 * somg12;
        calp2 = sbet12
            - cbet1
                * sbet2
                * if comg12 >= 0.0 {
                    sq(somg12) / (1.0 + comg12)
                } else {
                    1.0 - comg12
                };
        (salp2, calp2) = norm2(salp2, calp2);
        sig12 = libm::atan2(ssig12, csig12);
    } else if csig12 >= 0.0 || ssig12 >= 6.0 * N.abs() * PI * sq(cbet1) {
        // The zeroth-order spherical approximation suffices.
    } else {
        // Scale to coordinates with the antipode at the origin and the
        // singular point at y = 0, x = -1.
        let lam12x = libm::atan2(-slam12, -clam12);
        let k2 = sq(sbet1) * EP2;
        let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);
        let lamscale = F * cbet1 * a3f(eps) * PI;
        let betscale = lamscale * cbet1;
        let x = lam12x / lamscale;
        let y = sbet12a / betscale;
        if y > -TOL1 && x > -1.0 - XTHRESH {
            // Strip near the cut.
            salp1 = (-x).min(1.0);
            calp1 = -(1.0 - sq(salp1)).sqrt();
        } else {
            // Estimate omg12 from the astroid, then alp1 from the sphere.
            let k = astroid(x, y);
            let omg12a = lamscale * (-x * k / (1.0 + k));
            somg12 = libm::sin(omg12a);
            comg12 = -libm::cos(omg12a);
            salp1 = cbet2 * somg12;
            calp1 = sbet12a - cbet2 * sbet1 * sq(somg12) / (1.0 - comg12);
        }
    }
    // Sanity check on the starting guess that lets NaN through.
    if salp1 > 0.0 || salp1.is_nan() {
        (salp1, calp1) = norm2(salp1, calp1);
    } else {
        salp1 = 1.0;
        calp1 = 0.0;
    }
    Start {
        sig12,
        salp1,
        calp1,
        salp2,
        calp2,
        dnm,
    }
}

/// The sig12 threshold below which a line counts as really short.
fn etol2() -> f64 {
    0.1 * TOL2 / (F.abs().max(0.001) * (1.0 - F / 2.0).min(1.0) / 2.0).sqrt()
}

struct Lambda {
    lam12: f64,
    salp2: f64,
    calp2: f64,
    sig12: f64,
    ssig1: f64,
    csig1: f64,
    ssig2: f64,
    csig2: f64,
    eps: f64,
    dlam12: f64,
}

/// Longitude difference reached from azimuth alp1, minus the target, with
/// its derivative when `diffp`.
fn lambda12(
    sbet1: f64,
    cbet1: f64,
    dn1: f64,
    sbet2: f64,
    cbet2: f64,
    dn2: f64,
    salp1: f64,
    calp1: f64,
    slam120: f64,
    clam120: f64,
    diffp: bool,
) -> Lambda {
    // Break the degeneracy of the equatorial line, handled already.
    let calp1 = if sbet1 == 0.0 && calp1 == 0.0 {
        -TINY
    } else {
        calp1
    };
    let salp0 = salp1 * cbet1;
    let calp0 = libm::hypot(calp1, salp1 * sbet1);
    let (somg1, comg1) = (salp0 * sbet1, calp1 * cbet1);
    let (ssig1, csig1) = norm2(sbet1, comg1);

    // Enforce symmetries when |bet2| = -bet1.
    let salp2 = if cbet2 != cbet1 { salp0 / cbet2 } else { salp1 };
    let calp2 = if cbet2 != cbet1 || sbet2.abs() != -sbet1 {
        (sq(calp1 * cbet1)
            + if cbet1 < -sbet1 {
                (cbet2 - cbet1) * (cbet1 + cbet2)
            } else {
                (sbet1 - sbet2) * (sbet1 + sbet2)
            })
        .sqrt()
            / cbet2
    } else {
        calp1.abs()
    };
    let (somg2, comg2) = (salp0 * sbet2, calp2 * cbet2);
    let (ssig2, csig2) = norm2(sbet2, comg2);

    let sig12 = libm::atan2(
        (csig1 * ssig2 - ssig1 * csig2).max(0.0) + 0.0,
        csig1 * csig2 + ssig1 * ssig2,
    );
    let somg12 = (comg1 * somg2 - somg1 * comg2).max(0.0) + 0.0;
    let comg12 = comg1 * comg2 + somg1 * somg2;
    // eta = omg12 - lam120.
    let eta = libm::atan2(
        somg12 * clam120 - comg12 * slam120,
        comg12 * clam120 + somg12 * slam120,
    );
    let k2 = sq(calp0) * EP2;
    let eps = k2 / (2.0 * (1.0 + (1.0 + k2).sqrt()) + k2);
    let mut c3 = [0.0; ORDER + 1];
    c3f(eps, &mut c3);
    let b312 = sin_series(ssig2, csig2, &c3, NC3 - 1) - sin_series(ssig1, csig1, &c3, NC3 - 1);
    let domg12 = -F * a3f(eps) * salp0 * (sig12 + b312);
    let lam12 = eta + domg12;

    let dlam12 = if !diffp {
        0.0
    } else if calp2 == 0.0 {
        -2.0 * F1 * dn1 / sbet1
    } else {
        let l = lengths(
            eps, sig12, ssig1, csig1, dn1, ssig2, csig2, dn2, cbet1, cbet2, false,
        );
        l.m12b * (F1 / (calp2 * cbet2))
    };
    Lambda {
        lam12,
        salp2,
        calp2,
        sig12,
        ssig1,
        csig1,
        ssig2,
        csig2,
        eps,
        dlam12,
    }
}

/// A1 − 1, the mean of (d/dsigma) I1 − 1.
fn a1m1f(eps: f64) -> f64 {
    // (1 - eps) A1 - 1, polynomial in eps² of order 3.
    const COEFF: [f64; 5] = [1.0, 4.0, 64.0, 0.0, 256.0];
    let t = polyval(3, &COEFF, 0, sq(eps)) / COEFF[4];
    (t + eps) / (1.0 - eps)
}

/// Fourier coefficients C1[l] of B1, l in 1..=6.
fn c1f(eps: f64, c: &mut [f64; ORDER + 1]) {
    const COEFF: [f64; 18] = [
        -1.0, 6.0, -16.0, 32.0, // C1[1]/eps^1, order 2 in eps²
        -9.0, 64.0, -128.0, 2048.0, // C1[2]/eps^2, order 2
        9.0, -16.0, 768.0, // C1[3]/eps^3, order 1
        3.0, -5.0, 512.0, // C1[4]/eps^4, order 1
        -7.0, 1280.0, // C1[5]/eps^5, order 0
        -7.0, 2048.0, // C1[6]/eps^6, order 0
    ];
    fourier_coefficients(eps, &COEFF, NC1, c);
}

/// Fourier coefficients C1'[l] of the inverse of B1, l in 1..=6.
fn c1pf(eps: f64, c: &mut [f64; ORDER + 1]) {
    const COEFF: [f64; 18] = [
        205.0, -432.0, 768.0, 1536.0, // C1p[1]/eps^1, order 2 in eps²
        4005.0, -4736.0, 3840.0, 12288.0, // C1p[2]/eps^2, order 2
        -225.0, 116.0, 384.0, // C1p[3]/eps^3, order 1
        -7173.0, 2695.0, 7680.0, // C1p[4]/eps^4, order 1
        3467.0, 7680.0, // C1p[5]/eps^5, order 0
        38081.0, 61440.0, // C1p[6]/eps^6, order 0
    ];
    fourier_coefficients(eps, &COEFF, NC1P, c);
}

/// A2 − 1, the mean of (d/dsigma) I2 − 1.
fn a2m1f(eps: f64) -> f64 {
    // (1 + eps) A2 - 1, polynomial in eps² of order 3.
    const COEFF: [f64; 5] = [-11.0, -28.0, -192.0, 0.0, 256.0];
    let t = polyval(3, &COEFF, 0, sq(eps)) / COEFF[4];
    (t - eps) / (1.0 + eps)
}

/// Fourier coefficients C2[l] of B2, l in 1..=6.
fn c2f(eps: f64, c: &mut [f64; ORDER + 1]) {
    const COEFF: [f64; 18] = [
        1.0, 2.0, 16.0, 32.0, // C2[1]/eps^1, order 2 in eps²
        35.0, 64.0, 384.0, 2048.0, // C2[2]/eps^2, order 2
        15.0, 80.0, 768.0, // C2[3]/eps^3, order 1
        7.0, 35.0, 512.0, // C2[4]/eps^4, order 1
        63.0, 1280.0, // C2[5]/eps^5, order 0
        77.0, 2048.0, // C2[6]/eps^6, order 0
    ];
    fourier_coefficients(eps, &COEFF, NC2, c);
}

/// Evaluates C[l] = eps^l · P_l(eps²)/d_l from packed numerators and
/// denominators, as GeographicLib's C1f and C2f do.
fn fourier_coefficients(eps: f64, coeff: &[f64], n: usize, c: &mut [f64; ORDER + 1]) {
    let eps2 = sq(eps);
    let mut d = eps;
    let mut o = 0;
    for (l, cl) in c.iter_mut().enumerate().skip(1).take(n) {
        let m = (n - l) / 2;
        *cl = d * polyval(m, coeff, o, eps2) / coeff[o + m + 1];
        o += m + 2;
        d *= eps;
    }
}

/// A3 coefficients in eps, highest degree first, for this ellipsoid's n.
const A3X: [f64; ORDER] = {
    const COEFF: [f64; 18] = [
        -3.0, 128.0, // eps^5, order 0 in n
        -2.0, -3.0, 64.0, // eps^4, order 1
        -1.0, -3.0, -1.0, 16.0, // eps^3, order 2
        3.0, -1.0, -2.0, 8.0, // eps^2, order 2
        1.0, -1.0, 2.0, // eps^1, order 1
        1.0, 1.0, // eps^0, order 0
    ];
    let mut out = [0.0; ORDER];
    let (mut o, mut k, mut j) = (0, 0, ORDER);
    while j > 0 {
        j -= 1;
        let m = if ORDER - j - 1 < j { ORDER - j - 1 } else { j };
        out[k] = polyval(m, &COEFF, o, N) / COEFF[o + m + 1];
        k += 1;
        o += m + 2;
    }
    out
};

/// C3[l] coefficients in eps for l in 1..6, highest degree first, for this
/// ellipsoid's n.
const C3X: [f64; (NC3 * (NC3 - 1)) / 2] = {
    const COEFF: [f64; 45] = [
        3.0, 128.0, // C3[1], eps^5, order 0 in n
        2.0, 5.0, 128.0, // C3[1], eps^4, order 1
        -1.0, 3.0, 3.0, 64.0, // C3[1], eps^3, order 2
        -1.0, 0.0, 1.0, 8.0, // C3[1], eps^2, order 2
        -1.0, 1.0, 4.0, // C3[1], eps^1, order 1
        5.0, 256.0, // C3[2], eps^5, order 0
        1.0, 3.0, 128.0, // C3[2], eps^4, order 1
        -3.0, -2.0, 3.0, 64.0, // C3[2], eps^3, order 2
        1.0, -3.0, 2.0, 32.0, // C3[2], eps^2, order 2
        7.0, 512.0, // C3[3], eps^5, order 0
        -10.0, 9.0, 384.0, // C3[3], eps^4, order 1
        5.0, -9.0, 5.0, 192.0, // C3[3], eps^3, order 2
        7.0, 512.0, // C3[4], eps^5, order 0
        -14.0, 7.0, 512.0, // C3[4], eps^4, order 1
        21.0, 2560.0, // C3[5], eps^5, order 0
    ];
    let mut out = [0.0; (NC3 * (NC3 - 1)) / 2];
    let (mut o, mut k, mut l) = (0, 0, 1);
    while l < NC3 {
        let mut j = NC3;
        while j > l {
            j -= 1;
            let m = if NC3 - j - 1 < j { NC3 - j - 1 } else { j };
            out[k] = polyval(m, &COEFF, o, N) / COEFF[o + m + 1];
            k += 1;
            o += m + 2;
        }
        l += 1;
    }
    out
};

/// A3, the mean of (d/dsigma) I3.
fn a3f(eps: f64) -> f64 {
    polyval(ORDER - 1, &A3X, 0, eps)
}

/// Fourier coefficients C3[l] of B3, l in 1..6.
fn c3f(eps: f64, c: &mut [f64; ORDER + 1]) {
    let mut mult = 1.0;
    let mut o = 0;
    for (l, cl) in c.iter_mut().enumerate().take(NC3).skip(1) {
        let m = NC3 - l - 1;
        mult *= eps;
        *cl = mult * polyval(m, &C3X, o, eps);
        o += m + 1;
    }
}

#[cfg(test)]
#[allow(clippy::excessive_precision)]
mod tests {
    use super::*;

    fn check(name: &str, actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{name}: {actual:e} != {expected:e} ± {tolerance:e}"
        );
    }

    fn relative(name: &str, actual: f64, expected: f64, tolerance: f64) {
        check(name, actual, expected, tolerance * expected.abs());
    }

    #[test]
    fn geodesy_constants_are_exact() {
        assert_eq!(TINY * TINY, f64::MIN_POSITIVE);
        assert_eq!(TOL2 * TOL2, f64::EPSILON);
        assert_eq!(MAXIT2, 83);
        relative("b", B, 6_356_752.314_245_179, 1e-15);
        relative("e'²", EP2, E2 / (1.0 - E2), 1e-15);
        relative("n", N, (A - B) / (A + B), 1e-12);
    }

    #[test]
    fn geodesy_radii_match_closed_forms() {
        assert_eq!(meridional_radius(0.0), A * (1.0 - E2));
        assert_eq!(prime_vertical_radius(0.0), A);
        let polar = A * A / B;
        relative("M(90°)", meridional_radius(PI / 2.0), polar, 4e-16);
        relative("N(90°)", prime_vertical_radius(PI / 2.0), polar, 4e-16);
        relative("M(-90°)", meridional_radius(-PI / 2.0), polar, 4e-16);
    }

    #[test]
    fn geodesy_zone_area_is_odd_and_totals_the_ellipsoid() {
        let total = zone_area_per_radian(PI / 2.0) * 2.0 * PI * 2.0;
        relative("total area", total, 510_065_621_724_088.509_3, 1e-12);
        for phi in [1e-300, 1e-12, 1e-3, 0.4, 1.2, PI / 2.0] {
            assert_eq!(zone_area_per_radian(-phi), -zone_area_per_radian(phi));
        }
        assert_eq!(zone_area_per_radian(0.0), 0.0);
        // Near the equator the zone is M(0) N(0) phi = b² phi per radian.
        let phi = 1e-9;
        relative("small zone", zone_area_per_radian(phi), B * B * phi, 2e-15);
    }

    /// geodtest.c `testcases`: lat1, lon1, azi1, lat2, lon2, azi2, s12, a12,
    /// m12, M12, M21, S12.
    const TESTCASES: [[f64; 12]; 20] = [
        [
            35.60777,
            -139.44815,
            111.098748429560326,
            -11.17491,
            -69.95921,
            129.289270889708762,
            8935244.5604818305,
            80.50729714281974,
            6273170.2055303837,
            0.16606318447386067,
            0.16479116945612937,
            12841384694976.432,
        ],
        [
            55.52454,
            106.05087,
            22.020059880982801,
            77.03196,
            197.18234,
            109.112041110671519,
            4105086.1713924406,
            36.892740690445894,
            3828869.3344387607,
            0.80076349608092607,
            0.80101006984201008,
            61674961290615.615,
        ],
        [
            -21.97856,
            142.59065,
            -32.44456876433189,
            41.84138,
            98.56635,
            -41.84359951440466,
            8394328.894657671,
            75.62930491011522,
            6161154.5773110616,
            0.24816339233950381,
            0.24930251203627892,
            -6637997720646.717,
        ],
        [
            -66.99028,
            112.2363,
            173.73491240878403,
            -12.70631,
            285.90344,
            2.512956620913668,
            11150344.2312080241,
            100.278634181155759,
            6289939.5670446687,
            -0.17199490274700385,
            -0.17722569526345708,
            -121287239862139.744,
        ],
        [
            -17.42761,
            173.34268,
            -159.033557661192928,
            -15.84784,
            5.93557,
            -20.787484651536988,
            16076603.1631180673,
            144.640108810286253,
            3732902.1583877189,
            -0.81273638700070476,
            -0.81299800519154474,
            97825992354058.708,
        ],
        [
            32.84994,
            48.28919,
            150.492927788121982,
            -56.28556,
            202.29132,
            48.113449399816759,
            16727068.9438164461,
            150.565799985466607,
            3147838.1910180939,
            -0.87334918086923126,
            -0.86505036767110637,
            -72445258525585.010,
        ],
        [
            6.96833,
            52.74123,
            92.581585386317712,
            -7.39675,
            206.17291,
            90.721692165923907,
            17102477.2496958388,
            154.147366239113561,
            2772035.6169917581,
            -0.89991282520302447,
            -0.89986892177110739,
            -1311796973197.995,
        ],
        [
            -50.56724,
            -16.30485,
            -105.439679907590164,
            -33.56571,
            -94.97412,
            -47.348547835650331,
            6455670.5118668696,
            58.083719495371259,
            5409150.7979815838,
            0.53053508035997263,
            0.52988722644436602,
            41071447902810.047,
        ],
        [
            -58.93002,
            -8.90775,
            140.965397902500679,
            -8.91104,
            133.13503,
            19.255429433416599,
            11756066.0219864627,
            105.755691241406877,
            6151101.2270708536,
            -0.26548622269867183,
            -0.27068483874510741,
            -86143460552774.735,
        ],
        [
            -68.82867,
            -74.28391,
            93.774347763114881,
            -50.63005,
            -8.36685,
            34.65564085411343,
            3956936.926063544,
            35.572254987389284,
            3708890.9544062657,
            0.81443963736383502,
            0.81420859815358342,
            -41845309450093.787,
        ],
        [
            -10.62672,
            -32.0898,
            -86.426713286747751,
            5.883,
            -134.31681,
            -80.473780971034875,
            11470869.3864563009,
            103.387395634504061,
            6184411.6622659713,
            -0.23138683500430237,
            -0.23155097622286792,
            4198803992123.548,
        ],
        [
            -21.76221,
            166.90563,
            29.319421206936428,
            48.72884,
            213.97627,
            43.508671946410168,
            9098627.3986554915,
            81.963476716121964,
            6299240.9166992283,
            0.13965943368590333,
            0.14152969707656796,
            10024709850277.476,
        ],
        [
            -19.79938,
            -174.47484,
            71.167275780171533,
            -11.99349,
            -154.35109,
            65.589099775199228,
            2319004.8601169389,
            20.896611684802389,
            2267960.8703918325,
            0.93427001867125849,
            0.93424887135032789,
            -3935477535005.785,
        ],
        [
            -11.95887,
            -116.94513,
            92.712619830452549,
            4.57352,
            7.16501,
            78.64960934409585,
            13834722.5801401374,
            124.688684161089762,
            5228093.177931598,
            -0.56879356755666463,
            -0.56918731952397221,
            -9919582785894.853,
        ],
        [
            -87.85331,
            85.66836,
            -65.120313040242748,
            66.48646,
            16.09921,
            -4.888658719272296,
            17286615.3147144645,
            155.58592449699137,
            2635887.4729110181,
            -0.90697975771398578,
            -0.91095608883042767,
            42667211366919.534,
        ],
        [
            1.74708,
            128.32011,
            -101.584843631173858,
            -11.16617,
            11.87109,
            -86.325793296437476,
            12942901.1241347408,
            116.650512484301857,
            5682744.8413270572,
            -0.44857868222697644,
            -0.44824490340007729,
            10763055294345.653,
        ],
        [
            -25.72959,
            -144.90758,
            -153.647468693117198,
            -57.70581,
            -269.17879,
            -48.343983158876487,
            9413446.7452453107,
            84.664533838404295,
            6356176.6898881281,
            0.09492245755254703,
            0.09737058264766572,
            74515122850712.444,
        ],
        [
            -41.22777,
            122.32875,
            14.285113402275739,
            -7.57291,
            130.37946,
            10.805303085187369,
            3812686.035106021,
            34.34330804743883,
            3588703.8812128856,
            0.82605222593217889,
            0.82572158200920196,
            -2456961531057.857,
        ],
        [
            11.01307,
            138.25278,
            79.43682622782374,
            6.62726,
            247.05981,
            103.708090215522657,
            11911190.819018408,
            107.341669954114577,
            6070904.722786735,
            -0.29767608923657404,
            -0.29785143390252321,
            17121631423099.696,
        ],
        [
            -29.47124,
            95.14681,
            -163.779130441688382,
            -27.46601,
            -69.15955,
            -15.909335945554969,
            13487015.8381145492,
            121.294026715742277,
            5481428.9945736388,
            -0.51527225545373252,
            -0.51556587964721788,
            104679964020340.318,
        ],
    ];

    #[test]
    fn geodesy_inverse_matches_geographiclib_testcases() {
        for row in TESTCASES {
            let [lat1, lon1, azi1, lat2, lon2, azi2, s12, a12, m12, mm12, mm21, _] = row;
            let r = inverse(lat1, lon1, lat2, lon2);
            check("azi1", r.azi1, azi1, 1e-13);
            check("azi2", r.azi2, azi2, 1e-13);
            check("s12", r.s12, s12, 1e-8);
            check("a12", r.a12, a12, 1e-13);
            check("m12", r.m12, m12, 1e-8);
            check("M12", r.geodesic_scale12, mm12, 1e-15);
            check("M21", r.geodesic_scale21, mm21, 1e-15);
        }
    }

    #[test]
    fn geodesy_direct_matches_geographiclib_testcases() {
        for row in TESTCASES {
            let [lat1, lon1, azi1, lat2, lon2, azi2, s12, ..] = row;
            let r = direct(lat1, lon1, azi1, s12);
            check("lat2", r.lat2, lat2, 1e-13);
            check("lon2", ang_diff(lon2, r.lon2).0, 0.0, 1e-13);
            check("azi2", r.azi2, azi2, 1e-13);
        }
    }

    #[test]
    fn geodesy_direct_geod_solve1_and_round_trips() {
        // GeographicLib GeodSolve1: JFK towards Paris.
        let r = direct(40.63972222, -73.77888889, 53.5, 5850e3);
        check("lat2", r.lat2, 49.01467, 0.5e-5);
        check("lon2", r.lon2, 2.56106, 0.5e-5);
        check("azi2", r.azi2, 111.62947, 0.5e-5);
        for (lat1, lon1, lat2, lon2) in [
            (0.0, 0.0, 0.0, 1.0),
            (51.5, -0.1, 40.7, -74.0),
            (-33.9, 151.2, 35.7, 139.7),
            (89.0, 10.0, -89.0, -170.0),
            (10.0, 179.5, -10.0, -179.5),
        ] {
            let i = inverse(lat1, lon1, lat2, lon2);
            let d = direct(lat1, lon1, i.azi1, i.s12);
            check("round-trip lat2", d.lat2, lat2, 1e-11);
            check("round-trip lon2", ang_diff(lon2, d.lon2).0, 0.0, 1e-11);
        }
    }

    fn s12(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        inverse(lat1, lon1, lat2, lon2).s12
    }

    #[test]
    fn geodesy_geod_solve0() {
        let r = inverse(40.6, -73.8, 49.01666667, 2.55);
        check("azi1", r.azi1, 53.47022, 0.5e-5);
        check("azi2", r.azi2, 111.59367, 0.5e-5);
        check("s12", r.s12, 5853226.0, 0.5);
    }

    #[test]
    fn geodesy_geod_solve4_short_line() {
        check(
            "s12",
            s12(36.493349428792, 0.0, 36.49334942879201, 0.0000008),
            0.072,
            0.5e-3,
        );
    }

    #[test]
    fn geodesy_nearly_antipodal_geod_solve6_9_10_11() {
        let cases = [
            (
                88.202499451857,
                -88.202499451857,
                179.981022032992859592,
                20003898.214,
            ),
            (
                89.262080389218,
                -89.262080389218,
                179.992207982775375662,
                20003925.854,
            ),
            (
                89.333123580033,
                -89.333123580032997687,
                179.99295812360148422,
                20003926.881,
            ),
            (
                56.320923501171,
                -56.320923501171,
                179.664747671772880215,
                19993558.287,
            ),
            (
                52.784459512564,
                -52.784459512563990912,
                179.634407464943777557,
                19991596.095,
            ),
            (
                48.522876735459,
                -48.52287673545898293,
                179.599720456223079643,
                19989144.774,
            ),
        ];
        for (lat1, lat2, lon2, expected) in cases {
            check("s12", s12(lat1, 0.0, lat2, lon2), expected, 0.5e-3);
        }
    }

    #[test]
    fn geodesy_nan_inputs_geod_solve14_55_94() {
        for (lat1, lon1, lat2, lon2) in [
            (0.0, 0.0, 1.0, f64::NAN),
            (f64::NAN, 0.0, 0.0, 90.0),
            (f64::NAN, 0.0, 90.0, 9.0),
            (0.0, 0.0, f64::NAN, 90.0),
            (0.0, 0.0, 91.0, 0.0),
        ] {
            let r = inverse(lat1, lon1, lat2, lon2);
            assert!(
                r.azi1.is_nan() && r.azi2.is_nan() && r.s12.is_nan(),
                "{r:?}"
            );
        }
    }

    #[test]
    fn geodesy_equatorial_and_antipodal_geod_solve33() {
        let cases = [
            (0.0, 179.0, 90.0, 90.0, 19926189.0),
            (0.0, 179.5, 55.96650, 124.03350, 19980862.0),
            (0.0, 180.0, 0.0, 180.0, 20003931.0),
            (1.0, 180.0, 0.0, 180.0, 19893357.0),
        ];
        for (lat2, lon2, azi1, azi2, expected) in cases {
            let r = inverse(0.0, 0.0, lat2, lon2);
            check("azi1", r.azi1, azi1, 0.5e-5);
            check("azi2", r.azi2.abs(), azi2, 0.5e-5);
            check("s12", r.s12, expected, 0.5);
        }
    }

    #[test]
    fn geodesy_geod_solve59_close_to_180_apart() {
        let r = inverse(5.0, 0.00000000000001, 10.0, 180.0);
        check("azi1", r.azi1, 0.000000000000035, 1.5e-14);
        check("azi2", r.azi2, 179.99999999999996, 1.5e-14);
        check("s12", r.s12, 18345191.174332713, 5e-9);
    }

    #[test]
    fn geodesy_geod_solve74_short_line_outputs() {
        let r = inverse(54.1589, 15.3872, 54.1591, 15.3877);
        check("azi1", r.azi1, 55.723110355, 5e-9);
        check("azi2", r.azi2, 55.723515675, 5e-9);
        check("s12", r.s12, 39.527686385, 5e-9);
        check("a12", r.a12, 0.000355495, 5e-9);
        check("m12", r.m12, 39.527686385, 5e-9);
        check("M12", r.geodesic_scale12, 0.999999995, 5e-9);
        check("M21", r.geodesic_scale21, 0.999999995, 5e-9);
    }

    #[test]
    fn geodesy_vincenty_failures_geod_solve76_78_99() {
        let r = inverse(
            -(41.0 + 19.0 / 60.0),
            174.0 + 49.0 / 60.0,
            40.0 + 58.0 / 60.0,
            -(5.0 + 30.0 / 60.0),
        );
        check("azi1", r.azi1, 160.39137649664, 0.5e-11);
        check("azi2", r.azi2, 19.50042925176, 0.5e-11);
        check("s12", r.s12, 19960543.857179, 0.5e-6);
        let r = inverse(27.2, 0.0, -27.1, 179.5);
        check("azi1", r.azi1, 45.82468716758, 0.5e-11);
        check("azi2", r.azi2, 134.22776532670, 0.5e-11);
        check("s12", r.s12, 19974354.765767, 0.5e-6);
        let r = inverse(45.0, 0.0, -45.0, 179.572719);
        check("azi1", r.azi1, 90.00000028, 1e-8);
        check("azi2", r.azi2, 90.00000028, 1e-8);
        check("s12", r.s12, 19987083.007, 0.5e-3);
    }

    #[test]
    fn geodesy_scales_and_coincident_points_geod_solve80() {
        let r = inverse(0.0, 0.0, 0.0, 90.0);
        check("M12", r.geodesic_scale12, -0.00528427534, 0.5e-10);
        check("M21", r.geodesic_scale21, -0.00528427534, 0.5e-10);
        let r = inverse(0.0, 0.0, 1e-6, 1e-6);
        check("M12", r.geodesic_scale12, 1.0, 0.5e-10);
        check("M21", r.geodesic_scale21, 1.0, 0.5e-10);
        // Coincident points.
        let r = inverse(20.001, 0.0, 20.001, 0.0);
        check("a12", r.a12, 0.0, 1e-13);
        check("s12", r.s12, 0.0, 1e-8);
        check("azi1", r.azi1, 180.0, 1e-13);
        check("azi2", r.azi2, 180.0, 1e-13);
        check("m12", r.m12, 0.0, 1e-8);
        check("M12", r.geodesic_scale12, 1.0, 1e-15);
        check("M21", r.geodesic_scale21, 1.0, 1e-15);
        assert!(r.a12.is_sign_positive() && r.s12.is_sign_positive());
        assert!(r.m12.is_sign_positive());
        // Both endpoints at the north pole.
        let r = inverse(90.0, 0.0, 90.0, 180.0);
        check("a12", r.a12, 0.0, 1e-13);
        check("s12", r.s12, 0.0, 1e-8);
        check("azi1", r.azi1, 0.0, 1e-13);
        check("azi2", r.azi2, 180.0, 1e-13);
        check("m12", r.m12, 0.0, 1e-8);
        check("M12", r.geodesic_scale12, 1.0, 1e-15);
        check("M21", r.geodesic_scale21, 1.0, 1e-15);
    }

    #[test]
    fn geodesy_geod_solve92_nearly_coincident() {
        let r = inverse(37.757540000000006, -122.47018, 37.75754, -122.470177);
        check("azi1", r.azi1, 89.99999923, 1e-7);
        check("azi2", r.azi2, 90.00000106, 1e-7);
        check("s12", r.s12, 0.264, 0.5e-3);
    }

    /// Meridian arc length from latitude 0 to `phi`, by composite
    /// Gauss–Legendre quadrature of M(φ).
    fn meridian_arc(phi: f64) -> f64 {
        let panels = 64;
        let h = phi / f64::from(panels);
        let mut total = 0.0;
        for i in 0..panels {
            let mid = (f64::from(i) + 0.5) * h;
            for (x, w) in crate::measure::GAUSS {
                total += w * h / 2.0 * meridional_radius(mid + x * h / 2.0);
            }
        }
        total
    }

    #[test]
    fn geodesy_meridional_geodesics_match_the_meridian_arc() {
        let quarter = meridian_arc(PI / 2.0);
        relative("quarter meridian", quarter, 10_001_965.729_312_7, 1e-12);
        let r = inverse(0.0, 0.0, 90.0, 0.0);
        check("s12", r.s12, quarter, 1e-6);
        check("azi1", r.azi1, 0.0, 1e-13);
        check("azi2", r.azi2, 0.0, 1e-13);
        let r = inverse(10.0, 30.0, 50.0, 30.0);
        check(
            "s12",
            r.s12,
            meridian_arc(50f64.to_radians()) - meridian_arc(10f64.to_radians()),
            1e-6,
        );
        check("azi1", r.azi1, 0.0, 1e-13);
        check("azi2", r.azi2, 0.0, 1e-13);
        let r = inverse(50.0, 30.0, -10.0, 30.0);
        check(
            "s12",
            r.s12,
            meridian_arc(50f64.to_radians()) + meridian_arc(10f64.to_radians()),
            1e-6,
        );
        check("azi1", r.azi1.abs(), 180.0, 1e-13);
        // Pole to pole along any meridian.
        let r = inverse(90.0, 0.0, -90.0, 0.0);
        check("s12", r.s12, 2.0 * quarter, 2e-6);
        check("a12", r.a12, 180.0, 1e-13);
    }

    #[test]
    fn geodesy_equatorial_geodesics_follow_the_equator() {
        let r = inverse(0.0, 0.0, 0.0, 90.0);
        relative("s12", r.s12, A * PI / 2.0, 1e-15);
        assert_eq!((r.azi1, r.azi2), (90.0, 90.0));
        let r = inverse(0.0, 10.0, 0.0, -20.0);
        relative("s12", r.s12, A * 30f64.to_radians(), 1e-15);
        assert_eq!((r.azi1, r.azi2), (-90.0, -90.0));
    }

    #[test]
    fn geodesy_inverse_is_symmetric_under_reversal() {
        for row in TESTCASES {
            let forward = inverse(row[0], row[1], row[3], row[4]);
            let reverse = inverse(row[3], row[4], row[0], row[1]);
            check("s12", reverse.s12, forward.s12, 1e-8);
            check(
                "M12",
                reverse.geodesic_scale12,
                forward.geodesic_scale21,
                1e-15,
            );
        }
    }
}
