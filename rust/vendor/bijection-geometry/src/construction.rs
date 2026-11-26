//! Geodesic construction on the WGS84 ellipsoid. Coordinate output uses
//! binary64 directly; construction never introduces an implicit grid.
use crate::{
    geodesy,
    Coords,
    Geometry,
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Kind,
    Pos,
};

pub(super) fn destination(
    value: Geometry,
    bearing_degrees: f64,
    distance_m: f64,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.charge(1)?;
    if value.kind != Kind::Point {
        return Err(crate::invalid("Destination requires a Point"));
    }
    if value.is_empty() || distance_m == 0.0 {
        return Ok(value);
    }
    let Coords::Point(origin) = value.coords else {
        unreachable!("admitted Point coordinates")
    };
    let target = geodesy::direct(
        origin[1],
        origin[0],
        bearing_degrees.rem_euclid(360.0),
        distance_m,
    );
    let longitude = if target.lon2 >= 180.0 {
        target.lon2 - 360.0
    } else {
        target.lon2
    };
    let latitude = target.lat2;
    work.allocate(
        value
            .space
            .retained_bytes()
            .saturating_add(std::mem::size_of::<Geometry>()),
    )?;
    Ok(Geometry::new(
        Kind::Point,
        Coords::Point([
            if longitude == 0.0 { 0.0 } else { longitude },
            if latitude == 0.0 { 0.0 } else { latitude },
        ]),
        value.space,
    ))
}

/// Initial geodesic azimuth in degrees clockwise from true north, in
/// [0, 360); identical positions face north. The inverse of `destination`.
pub fn initial_bearing(from: Pos, to: Pos) -> f64 {
    if from == to {
        return 0.0;
    }
    let degrees = geodesy::inverse(from[1], from[0], to[1], to[0]).azi1;
    let bearing = (degrees + 360.0) % 360.0;
    if bearing == 360.0 || bearing == 0.0 {
        0.0
    } else {
        bearing
    }
}

/// The local azimuthal equidistant frame centred on `centre`: metres east and
/// north along the WGS84 geodesics from it. Its inverse is `destination`
/// along the position's bearing; an antipodal position has no direction.
pub fn local_frame(
    space: &GeometrySpace,
    centre: Pos,
    positions: &[Pos],
    work: &mut GeometryWork,
) -> Result<Vec<Pos>, GeometryRefusal> {
    if space.frame != crate::CoordinateFrame::Wgs84 || !space.is_ground() {
        return Err(crate::invalid(
            "A local frame requires WGS84 ground coordinates",
        ));
    }
    crate::trajectory::validate_position(centre)?;
    work.allocate(positions.len().saturating_mul(std::mem::size_of::<Pos>()))?;
    positions
        .iter()
        .map(|&position| {
            work.charge(4)?;
            crate::trajectory::validate_position(position)?;
            if position == centre {
                return Ok([0.0, 0.0]);
            }
            let line = geodesy::inverse(centre[1], centre[0], position[1], position[0]);
            if line.s12 >= crate::predicates::MAX_POINT_DISTANCE_METRES - 1e-3 {
                return Err(crate::invalid(
                    "A position is antipodal to the frame centre",
                ));
            }
            let (sine, cosine) = (
                libm::sin(line.azi1.to_radians()),
                libm::cos(line.azi1.to_radians()),
            );
            Ok([line.s12 * sine, line.s12 * cosine])
        })
        .collect()
}

/// A cap holding a value and every arc between its positions. Its centre is
/// that of the smallest cap on the auxiliary sphere, which holds the arcs too
/// while it is smaller than a hemisphere; its radius is a times that cap's
/// angle, since a radian there spans at most a metres. Typed outcomes
/// distinguish values no hemisphere holds and values with a single position.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoundingCap {
    Cap { centre: Pos, radius_m: f64 },
    Extent,
    Degenerate,
}

pub fn bounding_cap(
    value: &Geometry,
    work: &mut GeometryWork,
) -> Result<BoundingCap, GeometryRefusal> {
    if !value.space.frame.is_geographic() || !value.space.is_ground() {
        return Err(crate::invalid(
            "A bounding cap requires WGS84 or sphere ground coordinates",
        ));
    }
    // A sphere frame's cap lives on its own sphere, where caps smaller than
    // a hemisphere hold the great-circle arcs between their positions.
    let sphere = value.space.frame.sphere_radius_m();
    work.allocate(value.position_count().saturating_mul(48))?;
    let mut keyed = Vec::new();
    let mut index = 0u32;
    value.for_each(|p| {
        index = index.wrapping_add(1);
        keyed.push((
            index.wrapping_mul(0x9e37_79b1),
            match sphere {
                Some(_) => crate::sphere::unit(*p),
                None => crate::arcs::unit(crate::arcs::direction(*p)),
            },
        ));
    });
    work.charge(keyed.len().saturating_mul(4))?;
    if keyed.is_empty() {
        return Ok(BoundingCap::Degenerate);
    }
    // A fixed multiplicative hash of position order makes Welzl deterministic.
    keyed.sort_by_key(|(key, _)| *key);
    let points: Vec<[f64; 3]> = keyed.into_iter().map(|(_, v)| v).collect();
    let dot = |a: &[f64; 3], b: &[f64; 3]| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    let cross = |a: &[f64; 3], b: &[f64; 3]| {
        [
            a[1] * b[2] - a[2] * b[1],
            a[2] * b[0] - a[0] * b[2],
            a[0] * b[1] - a[1] * b[0],
        ]
    };
    let norm = |v: [f64; 3]| {
        let l = dot(&v, &v).sqrt();
        [v[0] / l, v[1] / l, v[2] / l]
    };
    let holds = |cap: &([f64; 3], f64), p: &[f64; 3]| dot(p, &cap.0) >= cap.1 - 1e-14;
    let two = |a: &[f64; 3], b: &[f64; 3]| {
        let c = norm([a[0] + b[0], a[1] + b[1], a[2] + b[2]]);
        (c, dot(a, &c))
    };
    let three = |a: &[f64; 3], b: &[f64; 3], d: &[f64; 3]| {
        let x = cross(
            &[b[0] - a[0], b[1] - a[1], b[2] - a[2]],
            &[d[0] - a[0], d[1] - a[1], d[2] - a[2]],
        );
        if dot(&x, &x).sqrt() < 1e-15 {
            return [two(a, b), two(a, d), two(b, d)]
                .into_iter()
                .reduce(|m, k| if k.1 < m.1 { k } else { m })
                .expect("three candidate caps");
        }
        let mut c = norm(x);
        if dot(&c, a) < 0.0 {
            c = [-c[0], -c[1], -c[2]];
        }
        (c, dot(a, &c))
    };
    let mut cap = (points[0], 1.0);
    for i in 1..points.len() {
        work.charge(1)?;
        if holds(&cap, &points[i]) {
            continue;
        }
        cap = (points[i], 1.0);
        for j in 0..i {
            work.charge(1)?;
            if holds(&cap, &points[j]) {
                continue;
            }
            cap = two(&points[i], &points[j]);
            for k in 0..j {
                work.charge(1)?;
                if !holds(&cap, &points[k]) {
                    cap = three(&points[i], &points[j], &points[k]);
                }
            }
        }
    }
    if cap.1.is_nan() || cap.1 <= 0.0 || points.iter().any(|p| !holds(&cap, p)) {
        return Ok(BoundingCap::Extent);
    }
    let angle = points
        .iter()
        .map(|p| {
            let c = cross(p, &cap.0);
            libm::atan2(dot(&c, &c).sqrt(), dot(p, &cap.0))
        })
        .fold(0.0, f64::max);
    let centre = match sphere {
        Some(_) => [
            libm::atan2(cap.0[1], cap.0[0]).to_degrees(),
            libm::atan2(cap.0[2], libm::hypot(cap.0[0], cap.0[1])).to_degrees(),
        ],
        None => crate::arcs::position(cap.0),
    };
    if angle >= std::f64::consts::FRAC_PI_2 * 0.99 {
        return Ok(BoundingCap::Extent);
    }
    let reach = sphere.unwrap_or(geodesy::SEMI_MAJOR_AXIS_METRES) * angle;
    Ok(if reach > 0.0 {
        BoundingCap::Cap {
            centre,
            radius_m: reach,
        }
    } else {
        BoundingCap::Degenerate
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        operations::Finite,
        GeometryOperation,
        GeometrySpace,
    };

    #[test]
    fn destination_obeys_ellipsoid_empty_and_zero_distance_contracts() -> anyhow::Result<()> {
        let space = GeometrySpace::wgs84_xy();
        let point = Geometry::new(Kind::Point, Coords::Point([0., 0.]), space.clone());
        // The equator is a geodesic of length a per radian of longitude.
        let op = GeometryOperation::Destination {
            bearing_degrees: Finite::try_from(90.).map_err(anyhow::Error::msg)?,
            distance_m: Finite::try_from(
                geodesy::SEMI_MAJOR_AXIS_METRES * std::f64::consts::FRAC_PI_2,
            )
            .map_err(anyhow::Error::msg)?,
        };
        let crate::operations::KernelResult::Geometry(result) =
            op.evaluate_pure(point, &mut GeometryWork::default())?
        else {
            unreachable!()
        };
        let Coords::Point(p) = result.coords else {
            unreachable!()
        };
        assert!((p[0] - 90.).abs() < 1e-12 && p[1].abs() < 1e-12);
        assert_eq!(result.space, space);
        let empty = Geometry::new(Kind::Point, Coords::Empty, space.clone());
        let crate::operations::KernelResult::Geometry(result) =
            op.evaluate_pure(empty.clone(), &mut GeometryWork::default())?
        else {
            unreachable!()
        };
        assert_eq!(result, empty);
        let exact = Geometry::new(
            Kind::Point,
            Coords::Point([0.1234567890123, 1.234567890123]),
            space,
        );
        assert_eq!(
            destination(exact.clone(), 0., 0., &mut GeometryWork::default())?,
            exact
        );
        assert!(destination(exact, 0., 1., &mut GeometryWork::with_limits(0, 1_000_000)).is_err());
        Ok(())
    }

    #[test]
    fn bearing_and_local_frame_invert_destination() -> anyhow::Result<()> {
        let space = GeometrySpace::wgs84_xy();
        assert_eq!(initial_bearing([1., 2.], [1., 2.]), 0.);
        assert!((initial_bearing([0., 0.], [1., 0.]) - 90.).abs() < 1e-12);
        assert!((initial_bearing([0., 0.], [0., -1.]) - 180.).abs() < 1e-12);
        let centre = [12.5, 41.9];
        let origin = Geometry::new(Kind::Point, Coords::Point(centre), space.clone());
        let target = destination(origin, 37., 25_000., &mut GeometryWork::default())?;
        let Coords::Point(target) = target.coords else {
            unreachable!()
        };
        assert!((initial_bearing(centre, target) - 37.).abs() < 1e-9);
        let [[x, y]] = local_frame(&space, centre, &[target], &mut GeometryWork::default())?[..]
        else {
            unreachable!()
        };
        assert!((libm::hypot(x, y) - 25_000.).abs() < 1e-6);
        assert!((libm::atan2(x, y).to_degrees() - 37.).abs() < 1e-9);
        assert_eq!(
            local_frame(&space, centre, &[centre], &mut GeometryWork::default())?,
            vec![[0., 0.]]
        );
        assert!(local_frame(
            &space,
            [0., 0.],
            &[[180., 0.]],
            &mut GeometryWork::default()
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn bounding_cap_holds_every_segment_and_types_its_refusals() {
        let space = GeometrySpace::wgs84_xy();
        let line = Geometry::new(
            Kind::LineString,
            Coords::Points(vec![[0., 0.], [2., 0.], [1., 1.]]),
            space.clone(),
        );
        let BoundingCap::Cap { centre, radius_m } =
            bounding_cap(&line, &mut GeometryWork::default()).unwrap()
        else {
            panic!("expected a cap")
        };
        assert!(radius_m > geodesy::SEMI_MAJOR_AXIS_METRES * 1f64.to_radians() * 0.99);
        assert!(centre[0] > 0. && centre[0] < 2. && centre[1] >= 0. && centre[1] < 1.);
        let point = Geometry::new(Kind::Point, Coords::Point([3., 4.]), space.clone());
        assert_eq!(
            bounding_cap(&point, &mut GeometryWork::default()).unwrap(),
            BoundingCap::Degenerate
        );
        let spread = Geometry::new(
            Kind::MultiPoint,
            Coords::Points(vec![[0., 0.], [120., 0.], [-120., 0.]]),
            space,
        );
        assert_eq!(
            bounding_cap(&spread, &mut GeometryWork::default()).unwrap(),
            BoundingCap::Extent
        );
    }
}
