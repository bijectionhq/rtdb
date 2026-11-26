//! Canonical set measurements, in metres and square metres. Overlapping and
//! duplicate components contribute once. Area and perimeter measure polygonal
//! support; length measures linear support. XYZ requires ground projection.
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    arcs,
    geodesy,
    CoordinateFrame,
    Coords,
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Measurement {
    Area,
    Length,
    Perimeter,
}

pub(crate) const GAUSS: [(f64, f64); 8] = [
    (-0.9602898564975363, 0.10122853629037626),
    (-0.7966664774136267, 0.22238103445337448),
    (-0.525532409916329, 0.31370664587788727),
    (-0.1834346424956498, 0.362683783378362),
    (0.1834346424956498, 0.362683783378362),
    (0.525532409916329, 0.31370664587788727),
    (0.7966664774136267, 0.22238103445337448),
    (0.9602898564975363, 0.10122853629037626),
];

/// Gauss–Legendre integral of `f` over `[start, end]`, one interval per
/// `step` of the variable.
fn integral(
    start: f64,
    end: f64,
    step: f64,
    work: &mut GeometryWork,
    f: impl Fn(f64) -> f64,
) -> Result<f64, GeometryRefusal> {
    let count = ((end - start).abs() / step).ceil().max(1.0) as usize;
    work.charge(count.saturating_mul(GAUSS.len() * 2))?;
    let mut sum = 0.0;
    for k in 0..count {
        for (x, weight) in GAUSS {
            let fraction = (k as f64 + (1.0 + x) / 2.0) / count as f64;
            sum += weight / (2.0 * count as f64) * f(start + (end - start) * fraction);
        }
    }
    Ok((end - start) * sum)
}

/// The auxiliary-sphere frame of the great elliptic arc p-q: unit start
/// vector, unit tangent there, and the arc's angle in radians.
pub(crate) fn arc_frame(p: Pos, q: Pos) -> Result<([f64; 3], [f64; 3], f64), GeometryRefusal> {
    let (a, b) = (
        arcs::unit(arcs::direction(p)),
        arcs::unit(arcs::direction(q)),
    );
    let omega = arcs::angle(a, b);
    if omega == 0.0 {
        return Ok((a, [0.0; 3], 0.0));
    }
    let normal = [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ];
    if normal == [0.0; 3] {
        return Err(crate::invalid(
            "An edge joins antipodal vertices, which fix no unique arc",
        ));
    }
    let n = arcs::unit(normal);
    let tangent = [
        n[1] * a[2] - n[2] * a[1],
        n[2] * a[0] - n[0] * a[2],
        n[0] * a[1] - n[1] * a[0],
    ];
    Ok((a, tangent, omega))
}

/// Length in metres of the great elliptic arc p-q: the ellipsoid's line
/// element along the arc's auxiliary-sphere parameter.
fn arc_length(p: Pos, q: Pos, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
    let (a, t, omega) = arc_frame(p, q)?;
    if omega == 0.0 {
        return Ok(0.0);
    }
    let (major, minor) = (
        geodesy::SEMI_MAJOR_AXIS_METRES,
        geodesy::SEMI_MINOR_AXIS_METRES,
    );
    integral(0.0, omega, 2.0_f64.to_radians(), work, |sigma| {
        let (s, c) = (libm::sin(sigma), libm::cos(sigma));
        let du = [
            t[0] * c - a[0] * s,
            t[1] * c - a[1] * s,
            t[2] * c - a[2] * s,
        ];
        libm::hypot(major * libm::hypot(du[0], du[1]), minor * du[2])
    })
}

/// The longitude turn, in degrees in (−180, 180], from p to q.
fn turn(p: Pos, q: Pos) -> f64 {
    let d = (q[0] - p[0]).rem_euclid(360.0);
    if d > 180.0 {
        d - 360.0
    } else {
        d
    }
}

/// The integral of the authalic zone area along the great elliptic arc p-q
/// against longitude, in square metres, and its longitude turn in degrees.
/// An arc from or to a pole runs along a meridian; an arc over a pole is two
/// meridians joined by a half turn there.
fn arc_zone(p: Pos, q: Pos, work: &mut GeometryWork) -> Result<(f64, f64), GeometryRefusal> {
    work.charge(1)?;
    if p[1].abs() == 90.0 && p[1] == q[1] {
        // Two longitudes of one pole: the boundary turns there.
        let d = turn(p, q);
        return Ok((
            geodesy::zone_area_per_radian(p[1].to_radians()) * d.to_radians(),
            d,
        ));
    }
    if p[1].abs() == 90.0 || q[1].abs() == 90.0 {
        return Ok((0.0, 0.0));
    }
    let d = turn(p, q);
    if d == 0.0 {
        return Ok((0.0, 0.0));
    }
    if d == 180.0 {
        let pole = if arcs::arc_bounds(p, q)[3] >= 90.0 {
            90.0_f64
        } else {
            -90.0
        };
        return Ok((
            geodesy::zone_area_per_radian(pole.to_radians()) * std::f64::consts::PI,
            d,
        ));
    }
    // On the auxiliary sphere the arc's plane, normal n, gives
    // tan β = −(n_x cos λ + n_y sin λ) / n_z along it; tan φ = tan β / (1 − f).
    let (a, b) = (arcs::direction(p), arcs::direction(q));
    let n = [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ];
    if n[2] == 0.0 {
        // A meridian plane: an equator crossing at a fixed longitude.
        return Ok((0.0, d));
    }
    let start = p[0].to_radians();
    let zone = integral(
        start,
        start + d.to_radians(),
        2.0_f64.to_radians(),
        work,
        |lambda| {
            let tan_beta = -(n[0] * libm::cos(lambda) + n[1] * libm::sin(lambda)) / n[2];
            geodesy::zone_area_per_radian(libm::atan(tan_beta / (1.0 - geodesy::FLATTENING)))
        },
    )?;
    Ok((zone, d))
}

/// The area of the whole ellipsoid, in square metres.
pub(crate) fn ellipsoid_area() -> f64 {
    4.0 * std::f64::consts::PI * geodesy::zone_area_per_radian(std::f64::consts::FRAC_PI_2)
}

pub(crate) fn edge_length(
    p: Pos,
    q: Pos,
    geometry: &Geometry,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    work.charge(1)?;
    Ok(match &geometry.space.frame {
        CoordinateFrame::Cartesian { unit, .. } => {
            libm::hypot(q[0] - p[0], q[1] - p[1]) * unit.metres_per_unit.get()
        },
        CoordinateFrame::Wgs84 => arc_length(p, q, work)?,
    })
}

pub(crate) fn ring_area(
    ring: &[Pos],
    geometry: &Geometry,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    work.charge(ring.len())?;
    Ok(match &geometry.space.frame {
        CoordinateFrame::Cartesian { unit, .. } => {
            let Some(origin) = ring.first() else {
                return Ok(0.0);
            };
            let area: f64 = ring
                .windows(2)
                .map(|edge| {
                    (edge[0][0] - origin[0]) * (edge[1][1] - origin[1])
                        - (edge[1][0] - origin[0]) * (edge[0][1] - origin[1])
                })
                .sum();
            area / 2.0 * unit.metres_per_unit.get() * unit.metres_per_unit.get()
        },
        CoordinateFrame::Wgs84 => {
            // Each arc sweeps the authalic zone between the equator and its
            // path. The result is the area left of the ring up to a multiple
            // of the ellipsoid's, which callers reduce: a ring around a pole
            // adds half the ellipsoid, and a pole vertex turns the ring between
            // the meridians it arrives and leaves on.
            let mut zone = 0.0;
            let mut turned = 0.0;
            for edge in ring.windows(2) {
                let (swept, turn_degrees) = arc_zone(edge[0], edge[1], work)?;
                zone += swept;
                turned += turn_degrees;
            }
            if ring.len() >= 4 && ring.first() == ring.last() {
                let open = &ring[..ring.len() - 1];
                let n = open.len();
                for (i, vertex) in open.iter().enumerate() {
                    let (before, after) = (open[(i + n - 1) % n], open[(i + 1) % n]);
                    if vertex[1].abs() == 90.0 && before[1].abs() != 90.0 && after[1].abs() != 90.0
                    {
                        let d = turn(before, after);
                        zone +=
                            geodesy::zone_area_per_radian(vertex[1].to_radians()) * d.to_radians();
                        turned += d;
                    }
                }
                if (turned / 360.0).round() != 0.0 {
                    zone -= ellipsoid_area() / 2.0;
                }
            }
            -zone
        },
    })
}

pub fn measure(
    geometry: &Geometry,
    measurement: Measurement,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    geometry.space.validate_kernel()?;
    if !geometry.space.is_ground() {
        return Err(crate::invalid(
            "Measurement requires explicit ground projection",
        ));
    }
    work.scoped(|work| {
        let mut west = false;
        let mut east = false;
        if geometry.space.frame == CoordinateFrame::Wgs84 && measurement == Measurement::Perimeter {
            work.charge(geometry.position_count())?;
            geometry.for_each(|point| {
                west |= point[0] == -180.0;
                east |= point[0] == 180.0;
            });
        }
        if matches!(geometry.coords, Coords::Collection(_))
            || measurement == Measurement::Length
            || west && east
        {
            use crate::topology::{
                visit_boundary,
                BoundaryDomain,
            };
            let mut sum = 0.0;
            let domain = if measurement == Measurement::Length {
                BoundaryDomain::Lines
            } else {
                BoundaryDomain::Areas
            };
            let mut exact_area = num_rational::BigRational::from_integer(0.into());
            visit_boundary(geometry, &[geometry], domain, work, &mut |piece, work| {
                if measurement == Measurement::Area {
                    match &geometry.space.frame {
                        CoordinateFrame::Cartesian { .. } => crate::topology::add_area(
                            &mut exact_area,
                            &piece.twice_area,
                            piece.interior_left,
                            work,
                        )?,
                        CoordinateFrame::Wgs84 => {
                            let value = ring_area(&[piece.start, piece.end], geometry, work)?;
                            sum += if piece.interior_left { value } else { -value };
                        },
                    }
                } else {
                    sum += edge_length(piece.start, piece.end, geometry, work)?;
                }
                Ok(false)
            })?;
            if measurement == Measurement::Area {
                match &geometry.space.frame {
                    CoordinateFrame::Cartesian { unit, .. } => {
                        sum = crate::topology::area(&exact_area, work)?
                            * unit.metres_per_unit.get()
                            * unit.metres_per_unit.get();
                    },
                    CoordinateFrame::Wgs84 => sum = sum.rem_euclid(ellipsoid_area()),
                }
            }
            return finite(sum);
        }
        work.allocate(
            geometry
                .position_count()
                .saturating_add(geometry.component_count())
                .saturating_mul(16),
        )?;
        let mut sum = 0.0;
        match measurement {
            Measurement::Area
                if matches!(geometry.space.frame, CoordinateFrame::Cartesian { .. }) =>
            {
                let CoordinateFrame::Cartesian { unit, .. } = &geometry.space.frame else {
                    unreachable!()
                };
                let exact = crate::topology::polygon_area(geometry, work)?;
                sum = crate::topology::area(&exact, work)?
                    * unit.metres_per_unit.get()
                    * unit.metres_per_unit.get();
            },
            Measurement::Area => {
                // Outer rings run counterclockwise and holes clockwise, so the
                // signed areas add up to the region's, modulo the ellipsoid.
                let geographic = geometry.space.frame == CoordinateFrame::Wgs84;
                for polygon in geometry.polygons() {
                    let mut area = 0.0;
                    for ring in polygon {
                        area += ring_area(&crate::arcs::without_seam_cuts(geographic, ring), geometry, work)?;
                    }
                    sum += area.rem_euclid(ellipsoid_area());
                }
            },
            Measurement::Length | Measurement::Perimeter => {
                // A ring's cut along the antimeridian is interior, no perimeter.
                let geographic = geometry.space.frame == CoordinateFrame::Wgs84;
                let lines: Vec<std::borrow::Cow<'_, [Pos]>> = if measurement == Measurement::Length {
                    geometry.lines().into_iter().map(|line| std::borrow::Cow::Borrowed(&line[..])).collect()
                } else {
                    geometry
                        .polygons()
                        .into_iter()
                        .flatten()
                        .map(|ring| crate::arcs::without_seam_cuts(geographic, ring))
                        .collect()
                };
                for line in lines {
                    for edge in line.windows(2) {
                        sum += edge_length(edge[0], edge[1], geometry, work)?;
                    }
                }
            },
        }
        finite(sum)
    })
}

fn finite(value: f64) -> Result<f64, GeometryRefusal> {
    if value.is_finite() {
        Ok(if value == 0.0 { 0.0 } else { value })
    } else {
        Err(crate::invalid("Measurement exceeds finite binary64"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        GeometrySpace,
        Gv,
        LinearUnit,
        STANDARD,
    };

    #[test]
    fn measurements_preserve_units_empties_holes_and_union_reduction() -> anyhow::Result<()> {
        let space = GeometrySpace {
            segments: crate::SegmentModel::CoordinateLinear,
            frame: CoordinateFrame::Cartesian {
                name: "local".into(),
                unit: LinearUnit {
                    metres_per_unit: crate::Positive::new(2.0).map_err(anyhow::Error::msg)?,
                },
            },
            ..GeometrySpace::wgs84_xy()
        };
        let polygon = json!({"type":"Polygon", "coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]],[[1,1],[1,3],[3,3],[3,1],[1,1]]]});
        let shape = crate::admit_geometry(&Gv::from(&polygon), &space, &STANDARD)?;
        assert_eq!(
            measure(&shape, Measurement::Area, &mut GeometryWork::default())?,
            48.0
        );
        assert_eq!(
            measure(&shape, Measurement::Perimeter, &mut GeometryWork::default())?,
            48.0
        );
        assert_eq!(
            measure(&shape, Measurement::Length, &mut GeometryWork::default())?,
            0.0
        );
        let collection = crate::admit_geometry(
            &Gv::from(&json!({"type":"GeometryCollection", "geometries":[polygon, polygon]})),
            &space,
            &STANDARD,
        )?;
        assert_eq!(
            measure(&collection, Measurement::Area, &mut GeometryWork::default())?,
            48.0
        );
        for kind in crate::Kind::ALL {
            let value = crate::geojson(kind, Gv::Array(vec![]));
            let empty = crate::admit_geometry(&value, &space, &STANDARD)?;
            for operation in [
                Measurement::Area,
                Measurement::Length,
                Measurement::Perimeter,
            ] {
                assert_eq!(
                    measure(&empty, operation, &mut GeometryWork::default())?,
                    0.0
                );
            }
        }
        assert!(measure(
            &shape,
            Measurement::Area,
            &mut GeometryWork::with_limits(0, 1024)
        )
        .is_err());
        Ok(())
    }

    /// A WGS84 polygon taken as given: rings keep their orientation.
    fn polygon(ring: &[[f64; 2]]) -> anyhow::Result<Geometry> {
        Ok(Geometry::new(
            crate::Kind::Polygon,
            Coords::Polygons(vec![vec![ring.to_vec()]]),
            GeometrySpace::wgs84_xy(),
        ))
    }

    #[test]
    fn ellipsoidal_measurements_follow_great_elliptic_arcs() -> anyhow::Result<()> {
        let mut work = GeometryWork::default();
        // The octant bounded by the equator and two meridians is an eighth of
        // the ellipsoid; its perimeter is a quarter equator and two quarter
        // meridians.
        let octant = polygon(&[[0.0, 0.0], [90.0, 0.0], [0.0, 90.0], [0.0, 0.0]])?;
        let area = measure(&octant, Measurement::Area, &mut work)?;
        assert!(
            (area / (ellipsoid_area() / 8.0) - 1.0).abs() < 1e-12,
            "{area}"
        );
        let perimeter = measure(&octant, Measurement::Perimeter, &mut work)?;
        let expected = geodesy::SEMI_MAJOR_AXIS_METRES * std::f64::consts::FRAC_PI_2
            + 2.0 * geodesy::inverse(0.0, 0.0, 90.0, 0.0).s12;
        assert!(
            (perimeter - expected).abs() < 1e-6,
            "{perimeter} {expected}"
        );
        // The antimeridian is not a boundary: a triangle across it has the
        // area of its mirror across the prime meridian.
        let near = measure(
            &polygon(&[[-10.0, 0.0], [10.0, 0.0], [0.0, 80.0], [-10.0, 0.0]])?,
            Measurement::Area,
            &mut work,
        )?;
        let seam = measure(
            &polygon(&[[170.0, 0.0], [-170.0, 0.0], [180.0, 80.0], [170.0, 0.0]])?,
            Measurement::Area,
            &mut work,
        )?;
        assert!((near / seam - 1.0).abs() < 1e-12, "{near} {seam}");
        // A ring around the pole is the sum of the three triangles it makes
        // with the pole.
        let ring = [[0.0, 80.0], [120.0, 81.0], [-120.0, 82.0], [0.0, 80.0]];
        let cap = measure(&polygon(&ring)?, Measurement::Area, &mut work)?;
        let mut sectors = 0.0;
        for pair in ring.windows(2) {
            sectors += measure(
                &polygon(&[pair[0], pair[1], [0.0, 90.0], pair[0]])?,
                Measurement::Area,
                &mut work,
            )?;
        }
        assert!((cap / sectors - 1.0).abs() < 1e-12, "{cap} {sectors}");
        // An arc bulges poleward of the parallel through its ends.
        let line = Geometry::new(
            crate::Kind::LineString,
            Coords::Points(vec![[0.0, 60.0], [60.0, 60.0]]),
            GeometrySpace::wgs84_xy(),
        );
        let length = measure(&line, Measurement::Length, &mut work)?;
        let parallel = geodesy::prime_vertical_radius(60.0_f64.to_radians())
            * libm::cos(60.0_f64.to_radians())
            * 60.0_f64.to_radians();
        assert!(length < parallel, "{length} {parallel}");
        assert!(
            length >= geodesy::inverse(60.0, 0.0, 60.0, 60.0).s12,
            "{length}"
        );
        Ok(())
    }
    #[test]
    fn exact_union_measurements_keep_redundant_vertices_and_pole_caps() -> anyhow::Result<()> {
        let value = serde_json::from_str::<serde_json::Value>(
            r#"{"type":"GeometryCollection","geometries":[{"type":"Polygon","coordinates":[[[-0.6404053884684828,-1.577935657249855],[2.016411399101036,-0.18462142230901335],[0.8553162033170012,2.029392567332252],[-1.8015005842525176,0.6360783323914105],[-0.6404053884684828,-1.577935657249855]]]},{"type":"Polygon","coordinates":[[[-0.6404053884684828,-1.577935657249855],[0.6880030053162766,-0.8812785397794343],[2.016411399101036,-0.18462142230901335],[0.8553162033170012,2.029392567332252],[-1.8015005842525176,0.6360783323914105],[-0.6404053884684828,-1.577935657249855]]]}]}"#,
        )?;
        let space = GeometrySpace {
            segments: crate::SegmentModel::CoordinateLinear,
            frame: CoordinateFrame::Cartesian {
                name: "local".into(),
                unit: LinearUnit::metre(),
            },
            ..GeometrySpace::wgs84_xy()
        };
        let geometry = crate::admit_geometry(&Gv::from(&value), &space, &STANDARD)?;
        for (measurement, expected) in [(Measurement::Area, 7.5), (Measurement::Perimeter, 11.0)] {
            assert!(
                (measure(&geometry, measurement, &mut GeometryWork::default())? - expected).abs()
                    < 1e-6
            );
        }
        Ok(())
    }
    #[test]
    fn large_linear_measurement_uses_spatial_candidates() {
        let coordinates: Vec<_> = (0..8192)
            .map(|i| vec![i as f64 * 0.0001, (i % 2) as f64 * 0.0001])
            .collect();
        let reply = crate::binding::invoke(
            serde_json::json!({"operation":"measure","measurement":"length","value":{"type":"LineString","coordinates":coordinates},"declaration":{"space":GeometrySpace::wgs84_xy(),"size":"large"}}),
        );
        assert!(
            reply["value"].as_f64().is_some_and(|value| value > 0.0),
            "{reply}"
        );
    }
}
