//! Union boundary refinement for uncertain positions on mixed collections.
use super::{
    point_relation,
    space_segment_distance,
    within_slack,
};
use crate::{
    topology::{
        visit_boundary,
        BoundaryDomain,
    },
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Pos,
    Rel,
};

/// A collection is a union. Member boundaries covered by another member's
/// interior, or by area on both sides of a shared edge, are not its boundary.
/// Refinement partitions edges at every crossing before deciding exposure.
pub(super) fn classify(
    point: Pos,
    accuracy: f64,
    geometry: &Geometry,
    members: &[Geometry],
    work: &mut GeometryWork,
) -> Result<&'static str, GeometryRefusal> {
    work.scoped(|work| {
        work.allocate(
            members
                .len()
                .saturating_mul(std::mem::size_of::<&Geometry>()),
        )?;
        let mut relevant = Vec::new();
        for member in members {
            match super::classify(point, Some(accuracy), member, work)? {
                "inside" => return Ok("inside"),
                "outside" => {},
                "uncertain" => {
                    let mut duplicate = false;
                    for prior in &relevant {
                        work.charge(member.position_count())?;
                        if *prior == member {
                            duplicate = true;
                            break;
                        }
                    }
                    if !duplicate {
                        relevant.push(member);
                    }
                },
                _ => unreachable!("known accuracy has a complete spatial classification"),
            }
        }
        if relevant.is_empty() {
            return Ok("outside");
        }
        if relevant.len() == 1 {
            return Ok("uncertain");
        }
        let inside = relevant.iter().try_fold(false, |inside, member| {
            Ok::<_, GeometryRefusal>(inside || point_relation(point, member, work)? != Rel::Outside)
        })?;
        let reaches = visit_boundary(
            geometry,
            &relevant,
            BoundaryDomain::Union,
            work,
            &mut |piece, work| {
                if piece.is_area
                    && geometry.space.frame == crate::CoordinateFrame::Wgs84
                    && piece.start[1].abs() == 90.0
                    && piece.end[1] == piece.start[1]
                {
                    return Ok(false);
                }
                let distance = space_segment_distance(
                    &geometry.space,
                    (point, point),
                    (piece.start, piece.end),
                    f64::INFINITY,
                    work,
                )?;
                Ok(within_slack(distance, accuracy + piece.rounding_error_m))
            },
        )?;
        Ok(if reaches {
            "uncertain"
        } else if inside {
            "inside"
        } else {
            "outside"
        })
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{
        json,
        Value,
    };

    use super::*;
    use crate::{
        CoordinateFrame,
        GeometryDeclaration,
        GeometrySpace,
        LinearUnit,
        Size,
    };

    fn square(west: f64, south: f64, east: f64, north: f64) -> Value {
        json!({"type":"Polygon","coordinates":[[[west,south],[east,south],[east,north],[west,north],[west,south]]]})
    }
    fn verify(
        value: Value,
        space: GeometrySpace,
        point: Pos,
        accuracy: f64,
        expected: &str,
    ) -> Result<(), GeometryRefusal> {
        let geometry = crate::admit_value(
            &crate::Gv::from(&value),
            &GeometryDeclaration {
                space,
                size: Size::Large,
            },
        )?;
        assert_eq!(
            super::super::classify(
                point,
                Some(accuracy),
                &geometry,
                &mut GeometryWork::default()
            )?,
            expected
        );
        let prepared = super::super::prepared::PreparedGeometry::new(
            geometry,
            crate::predicates::prepared::Preparation::Topology,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(
            prepared.classify(point, Some(accuracy), &mut GeometryWork::default())?,
            expected
        );
        Ok(())
    }
    #[test]
    fn collection_uncertainty_uses_exposed_union_boundaries() -> Result<(), GeometryRefusal> {
        let mut space = GeometrySpace::wgs84_xy();
        space.segments = crate::SegmentModel::CoordinateLinear;
        space.frame = CoordinateFrame::Cartesian {
            name: "local".into(),
            unit: LinearUnit::metre(),
        };
        let adjacent = json!({"type":"GeometryCollection","geometries":[square(-2.0,-1.0,0.0,1.0),square(0.0,-1.0,2.0,1.0)]});
        verify(adjacent, space.clone(), [0.0, 0.0], 0.25, "inside")?;
        let junction = json!({"type":"GeometryCollection","geometries":[square(-2.0,-2.0,0.0,2.0),square(0.0,-2.0,2.0,0.0),square(0.0,0.0,2.0,2.0),{"type":"Point","coordinates":[0,0]},{"type":"LineString","coordinates":[[0,-1],[0,1]]}]});
        verify(junction, space.clone(), [0.0, 0.0], 0.25, "inside")?;
        let diagonal = json!({"type":"GeometryCollection","geometries":[{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,0]]]},{"type":"Polygon","coordinates":[[[0,0],[1,1],[2,2],[0,2],[0,0]]]}]});
        verify(diagonal, space.clone(), [1.0, 1.0], 0.25, "inside")?;
        let duplicate = json!({"type":"GeometryCollection","geometries":[square(-2.0,-1.0,0.0,1.0),square(-2.0,-1.0,0.0,1.0)]});
        verify(duplicate, space.clone(), [0.0, 0.0], 0.25, "uncertain")?;
        let corners = json!({"type":"GeometryCollection","geometries":[square(-1.0,-1.0,0.0,0.0),square(0.0,0.0,1.0,1.0)]});
        verify(corners, space, [0.0, 0.0], 0.25, "uncertain")?;
        let seam = json!({"type":"GeometryCollection","geometries":[square(179.0,-1.0,180.0,1.0),square(-180.0,-1.0,-179.0,1.0)]});
        verify(
            seam,
            GeometrySpace::wgs84_xy(),
            [180.0, 0.0],
            100.0,
            "inside",
        )?;
        let north = json!({"type":"GeometryCollection","geometries":[square(-180.0,89.0,-90.0,90.0),square(-90.0,89.0,0.0,90.0),square(0.0,89.0,90.0,90.0),square(90.0,89.0,180.0,90.0)]});
        verify(
            north,
            GeometrySpace::wgs84_xy(),
            [0.0, 90.0],
            100.0,
            "inside",
        )?;
        let partial = json!({"type":"GeometryCollection","geometries":[square(-180.0,89.0,-90.0,90.0),square(-90.0,89.0,0.0,90.0),square(0.0,89.0,90.0,90.0)]});
        verify(
            partial,
            GeometrySpace::wgs84_xy(),
            [0.0, 90.0],
            100.0,
            "uncertain",
        )?;
        Ok(())
    }
    #[test]
    fn prepared_classification_never_implicitly_projects_height() -> Result<(), GeometryRefusal> {
        let geometry = crate::admit_value(
            &crate::Gv::from(&json!({"type":"Point","coordinates":[0,0,1]})),
            &GeometryDeclaration {
                space: GeometrySpace::wgs84_xyz(),
                size: Size::Standard,
            },
        )?;
        let prepared = super::super::prepared::PreparedGeometry::new(
            geometry.clone(),
            crate::predicates::prepared::Preparation::Topology,
            &mut GeometryWork::default(),
        )?;
        for accuracy in [None, Some(0.0), Some(1.0)] {
            assert!(super::super::classify(
                [0.0, 0.0],
                accuracy,
                &geometry,
                &mut GeometryWork::default()
            )
            .is_err());
            assert!(prepared
                .classify([0.0, 0.0], accuracy, &mut GeometryWork::default())
                .is_err());
        }
        Ok(())
    }
}
