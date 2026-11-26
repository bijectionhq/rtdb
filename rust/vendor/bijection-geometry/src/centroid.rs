//! Centre of mass of the highest-dimensional nonempty support. Components
//! form a set: duplicate points, overlapping lines and overlapping polygons
//! contribute once. Cartesian support uses Euclidean mass; WGS84 support uses
//! the unit-sphere integral along declared coordinate-linear boundaries.
use crate::{
    topology::{
        visit_boundary,
        BoundaryDomain,
    },
    Coords,
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Kind,
    Pos,
};

fn dimension(value: &Geometry) -> Option<u8> {
    if value.is_empty() {
        return None;
    }
    if let Coords::Collection(members) = &value.coords {
        return members.iter().filter_map(dimension).max();
    }
    Some(match value.kind {
        Kind::Point | Kind::MultiPoint => 0,
        Kind::LineString | Kind::MultiLineString => 1,
        Kind::Polygon | Kind::MultiPolygon => 2,
        Kind::GeometryCollection => unreachable!("collection coordinates"),
    })
}

pub fn centroid(value: &Geometry, work: &mut GeometryWork) -> Result<Geometry, GeometryRefusal> {
    value.space.validate_kernel()?;
    if !value.space.is_ground() {
        return Err(crate::invalid(
            "Centroid requires explicit ground projection",
        ));
    }
    work.scoped(|work| {
        work.charge(
            value
                .component_count()
                .saturating_add(value.position_count()),
        )?;
        if let Coords::Point(point) = value.coords {
            work.allocate(std::mem::size_of::<Geometry>())?;
            return Ok(Geometry::new(
                Kind::Point,
                Coords::Point(point),
                value.space.clone(),
            ));
        }
        let Some(dimension) = dimension(value) else {
            return Ok(Geometry::new(
                Kind::Point,
                Coords::Empty,
                value.space.clone(),
            ));
        };
        let spherical = value.space.frame.is_geographic();
        let mut moment = [0.0; 3];
        let mut mass = 0.0;
        if dimension == 0 {
            work.allocate(
                value
                    .position_count()
                    .saturating_mul(std::mem::size_of::<Pos>()),
            )?;
            let mut points = Vec::with_capacity(value.position_count());
            value.for_each(|point| {
                let mut point = *point;
                if spherical {
                    if point[1].abs() == 90.0 {
                        point[0] = 0.0;
                    } else if point[0] == 180.0 {
                        point[0] = -180.0;
                    }
                }
                points.push(point);
            });
            work.charge(
                points
                    .len()
                    .saturating_mul((usize::BITS - points.len().leading_zeros()) as usize + 1),
            )?;
            points.sort_by(|a, b| a[0].total_cmp(&b[0]).then_with(|| a[1].total_cmp(&b[1])));
            points.dedup();
            for point in points {
                work.charge(1)?;
                let vector = if spherical {
                    crate::arcs::unit(crate::arcs::direction(point))
                } else {
                    [point[0], point[1], 0.0]
                };
                for axis in 0..3 {
                    moment[axis] += vector[axis];
                }
                mass += 1.0;
            }
        } else {
            let mut origin = [0.0; 2];
            let mut first = true;
            value.for_each(|point| {
                if first {
                    origin = *point;
                    first = false;
                }
            });
            visit_boundary(
                value,
                &[value],
                if dimension == 1 {
                    BoundaryDomain::Lines
                } else {
                    BoundaryDomain::Areas
                },
                work,
                &mut |piece, work| {
                    let (start, end) = (piece.start, piece.end);
                    if spherical {
                        // The mean direction on the auxiliary sphere: an arc of
                        // angle ω from a, with unit tangent t there, has
                        // ∫u dσ = a sin ω + t (1 − cos ω); an area's moment is
                        // half the sum of its edges' ω times their unit normals.
                        let sign = if dimension == 1 || piece.interior_left {
                            1.0
                        } else {
                            -1.0
                        };
                        work.charge(1)?;
                        let (a, t, omega) = crate::measure::arc_frame(start, end)?;
                        let normal = [
                            a[1] * t[2] - a[2] * t[1],
                            a[2] * t[0] - a[0] * t[2],
                            a[0] * t[1] - a[1] * t[0],
                        ];
                        for axis in 0..3 {
                            moment[axis] += sign
                                * if dimension == 1 {
                                    a[axis] * libm::sin(omega) + t[axis] * (1.0 - libm::cos(omega))
                                } else {
                                    omega * normal[axis] / 2.0
                                };
                        }
                        mass += if dimension == 1 { omega } else { omega / 2.0 };
                    } else if dimension == 1 {
                        let weight = libm::hypot(end[0] - start[0], end[1] - start[1]);
                        mass += weight;
                        for axis in 0..2 {
                            moment[axis] += ((start[axis] - origin[axis]) / 2.0
                                + (end[axis] - origin[axis]) / 2.0)
                                * weight;
                        }
                    } else {
                        let sign = if piece.interior_left { 1.0 } else { -1.0 };
                        let a = [start[0] - origin[0], start[1] - origin[1]];
                        let b = [end[0] - origin[0], end[1] - origin[1]];
                        let twice_area = sign * (a[0] * b[1] - b[0] * a[1]);
                        mass += twice_area / 2.0;
                        for axis in 0..2 {
                            moment[axis] += (a[axis] + b[axis]) * twice_area / 6.0;
                        }
                    }
                    Ok(false)
                },
            )?;
            if !spherical && mass != 0.0 {
                for axis in 0..2 {
                    moment[axis] += origin[axis] * mass;
                }
            }
        }
        let point = if spherical {
            let norm = libm::hypot(libm::hypot(moment[0], moment[1]), moment[2]);
            if !norm.is_finite() || norm <= mass.abs() * 1e-9 {
                return Err(crate::invalid(
                    "The centroid is undefined: the geometry's mass cancels on the sphere",
                ));
            }
            crate::arcs::position(moment)
        } else {
            if mass == 0.0 {
                return Err(crate::invalid("The centroid has no positive support mass"));
            }
            [moment[0] / mass, moment[1] / mass]
        };
        if point.iter().any(|v| !v.is_finite()) {
            return Err(crate::invalid("Centroid exceeds finite binary64"));
        }
        work.allocate(std::mem::size_of::<Geometry>())?;
        Ok(Geometry::new(
            Kind::Point,
            Coords::Point(point.map(|v| if v == 0.0 { 0.0 } else { v })),
            value.space.clone(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::CoordinateFrame;

    #[test]
    fn centroid_uses_unique_highest_dimensional_support_and_preserves_space() -> anyhow::Result<()>
    {
        for spherical in [false, true] {
            let space = if spherical {
                crate::GeometrySpace::wgs84_xy()
            } else {
                crate::GeometrySpace {
                    segments: crate::SegmentModel::CoordinateLinear,
                    frame: CoordinateFrame::Cartesian {
                        name: "local".into(),
                        unit: crate::LinearUnit::metre(),
                    },
                    ..crate::GeometrySpace::wgs84_xy()
                }
            };
            let polygon = json!({"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]});
            let admit =
                |raw| crate::admit_geometry(&crate::Gv::from(&raw), &space, &crate::STANDARD);
            let point =
                admit(json!({"type":"Point","coordinates":[2.123456789123,48.123456789123]}))?;
            assert_eq!(centroid(&point, &mut GeometryWork::default())?, point);
            let expected = centroid(&admit(polygon.clone())?, &mut GeometryWork::default())?;
            let collection = admit(
                json!({"type":"GeometryCollection","geometries":[polygon,polygon,{"type":"Point","coordinates":[80,80]}]}),
            )?;
            let actual = centroid(&collection, &mut GeometryWork::default())?;
            assert_eq!(actual.space, space);
            let (Coords::Point(a), Coords::Point(b)) = (actual.coords, expected.coords) else {
                anyhow::bail!("points")
            };
            assert!((a[0] - b[0]).abs() < 1e-10 && (a[1] - b[1]).abs() < 1e-10);
            if !spherical {
                assert!((a[0] - 1.).abs() < 1e-12 && (a[1] - 1.).abs() < 1e-12);
            }
            let empty = admit(json!({"type":"GeometryCollection","geometries":[]}))?;
            assert_eq!(
                centroid(&empty, &mut GeometryWork::default())?.coords,
                Coords::Empty
            );
            assert!(centroid(&collection, &mut GeometryWork::with_limits(0, 0)).is_err());
        }
        Ok(())
    }
}
