//! Representation normalization preserves coordinates, dimensions and members.
//! It does not repair topology, snap coordinates or dissolve overlapping sets.
//! A WGS84 ring's orientation fixes its interior, so only Cartesian rings are
//! reoriented: shells counterclockwise, holes clockwise.
use std::cmp::Ordering;

use num_traits::Zero;

use crate::{
    Coords,
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Kind,
    Pos,
};

type Vertex = [f64; 3];
type Heights = Option<std::vec::IntoIter<f64>>;

fn compare(a: &Vertex, b: &Vertex) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.total_cmp(b))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

fn sequence(a: &[Vertex], b: &[Vertex]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| compare(a, b))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}

fn polygon_order(a: &[Vec<Vertex>], b: &[Vec<Vertex>]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| sequence(a, b))
        .find(|order| *order != Ordering::Equal)
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}

fn take(points: Vec<Pos>, heights: &mut Heights) -> Vec<Vertex> {
    points
        .into_iter()
        .map(|point| vertex(point, heights))
        .collect()
}

fn vertex(point: Pos, heights: &mut Heights) -> Vertex {
    [
        point[0],
        point[1],
        heights
            .as_mut()
            .map_or(0.0, |heights| heights.next().expect("admitted height")),
    ]
}

fn restore(points: Vec<Vertex>, heights: &mut Option<Vec<f64>>) -> Vec<Pos> {
    points
        .into_iter()
        .map(|point| position(point, heights))
        .collect()
}

fn position(point: Vertex, heights: &mut Option<Vec<f64>>) -> Pos {
    if let Some(heights) = heights {
        heights.push(point[2]);
    }
    [point[0], point[1]]
}

fn line(points: &mut [Vertex]) {
    let order = points
        .iter()
        .zip(points.iter().rev())
        .map(|(a, b)| compare(a, b))
        .find(|order| *order != Ordering::Equal);
    // Retain the established convention: the larger endpoint comes first.
    if order == Some(Ordering::Less) {
        points.reverse();
    }
}

fn polygon(
    rings: &mut [Vec<Vertex>],
    planar: bool,
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    for (index, ring) in rings.iter_mut().enumerate() {
        if ring.is_empty() {
            continue;
        }
        if planar {
            let xy: Vec<Pos> = ring.iter().map(|point| [point[0], point[1]]).collect();
            let counterclockwise = work.scoped(|work| {
                Ok::<_, GeometryRefusal>(
                    crate::topology::twice_ring_area(&xy, work)? > crate::exact::Rational::zero(),
                )
            })?;
            if counterclockwise != (index == 0) {
                ring.reverse();
            }
        }
        ring.pop();
        let first = ring
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| compare(a, b))
            .map(|(index, _)| index)
            .expect("admitted nonempty ring");
        ring.rotate_left(first);
        ring.push(ring[0]);
    }
    if !rings.is_empty() {
        rings[1..].sort_by(|a, b| sequence(a, b));
    }
    Ok(())
}

fn slots(value: &Geometry) -> usize {
    match &value.coords {
        Coords::Empty => 1,
        Coords::Point(_) => 1,
        Coords::MultiPoint(points) => points.len(),
        Coords::Points(points) => points.len(),
        Coords::Lines(lines) => lines.len() + lines.iter().map(Vec::len).sum::<usize>(),
        Coords::Polygons(polygons) => {
            polygons.len()
                + polygons
                    .iter()
                    .map(|rings| rings.len() + rings.iter().map(Vec::len).sum::<usize>())
                    .sum::<usize>()
        },
        Coords::Collection(members) => members.len() + members.iter().map(slots).sum::<usize>(),
    }
}

pub(crate) fn standardize(
    mut value: Geometry,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.scoped(|work| {
        let count = value.position_count();
        let slots = slots(&value);
        // Account comparison scans by coordinates, including shared prefixes.
        work.charge(
            slots
                .saturating_mul(slots.max(1).ilog2() as usize + 3)
                .saturating_add(value.component_count()),
        )?;
        work.allocate(
            value
                .retained_bytes()
                .saturating_mul(8)
                .saturating_add(count.saturating_mul(32))
                .saturating_add(value.component_count().saturating_mul(256)),
        )?;
        let planar = !value.space.frame.is_geographic();
        let mut input_heights = value.heights.take().map(Vec::into_iter);
        let mut output_heights = input_heights.as_ref().map(|_| Vec::with_capacity(count));
        value.coords = match value.coords {
            Coords::Empty => Coords::Empty,
            Coords::Point(point) => Coords::Point(position(
                vertex(point, &mut input_heights),
                &mut output_heights,
            )),
            Coords::MultiPoint(points) => {
                let mut points: Vec<_> = points
                    .into_iter()
                    .map(|point| point.map(|point| vertex(point, &mut input_heights)))
                    .collect();
                points.sort_by(|a, b| match (a, b) {
                    (Some(a), Some(b)) => compare(a, b),
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                });
                Coords::MultiPoint(
                    points
                        .into_iter()
                        .map(|point| point.map(|point| position(point, &mut output_heights)))
                        .collect(),
                )
            },
            Coords::Points(points) => {
                let mut points = take(points, &mut input_heights);
                line(&mut points);
                Coords::Points(restore(points, &mut output_heights))
            },
            Coords::Lines(lines) => {
                let mut lines: Vec<_> = lines
                    .into_iter()
                    .map(|points| take(points, &mut input_heights))
                    .collect();
                if value.kind == Kind::Polygon {
                    polygon(&mut lines, planar, work)?;
                } else {
                    lines.iter_mut().for_each(|points| line(points));
                    lines.sort_by(|a, b| sequence(a, b));
                }
                Coords::Lines(
                    lines
                        .into_iter()
                        .map(|points| restore(points, &mut output_heights))
                        .collect(),
                )
            },
            Coords::Polygons(polygons) => {
                let mut polygons: Vec<Vec<_>> = polygons
                    .into_iter()
                    .map(|rings| {
                        rings
                            .into_iter()
                            .map(|points| take(points, &mut input_heights))
                            .collect()
                    })
                    .collect();
                for rings in &mut polygons {
                    polygon(rings, planar, work)?;
                }
                polygons.sort_by(|a, b| polygon_order(a, b));
                Coords::Polygons(
                    polygons
                        .into_iter()
                        .map(|rings| {
                            rings
                                .into_iter()
                                .map(|points| restore(points, &mut output_heights))
                                .collect()
                        })
                        .collect(),
                )
            },
            Coords::Collection(members) => Coords::Collection(
                members
                    .into_iter()
                    .map(|member| standardize(member, work))
                    .collect::<Result<_, _>>()?,
            ),
        };
        value.heights = output_heights;
        Ok(value)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        GeometryDeclaration,
        GeometrySpace,
        Gv,
        Size,
    };

    #[test]
    fn empty_members_still_consume_sorting_work() {
        let value = Geometry::new(
            Kind::MultiPoint,
            Coords::MultiPoint(vec![None; 128]),
            GeometrySpace::wgs84_xy(),
        );
        assert!(standardize(value, &mut GeometryWork::with_limits(128, 1 << 20)).is_err());
    }

    #[test]
    fn exact_standardization_preserves_empty_members_and_height_order() -> anyhow::Result<()> {
        let declaration = GeometryDeclaration {
            space: GeometrySpace::wgs84_xyz(),
            size: Size::Large,
        };
        let input = json!({"type":"GeometryCollection","geometries":[
            {"type":"MultiLineString","coordinates":[[],[[0,0,1],[1,1,2]],[[0,0,3],[1,1,4]]]},
            {"type":"MultiPoint","coordinates":[[1,2,8],[],[1,2,7]]},
            {"type":"Polygon","coordinates":[[[0,0,3],[0,1e-12,2],[1e-12,0,1],[0,0,3]]]}
        ]});
        let value = crate::admit_value_with_work(
            &Gv::from(&input),
            &declaration,
            &mut GeometryWork::default(),
        )?;
        let once = standardize(value, &mut GeometryWork::default())?;
        let expected = json!({"type":"GeometryCollection","geometries":[
            {"type":"MultiLineString","coordinates":[[],[[1,1,2],[0,0,1]],[[1,1,4],[0,0,3]]]},
            {"type":"MultiPoint","coordinates":[[],[1,2,7],[1,2,8]]},
            {"type":"Polygon","coordinates":[[[0,0,3],[0,1e-12,2],[1e-12,0,1],[0,0,3]]]}
        ]});
        // A WGS84 ring's orientation is its interior: this clockwise ring is
        // everything but the tiny triangle, so it is not reversed.
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&once.js_json())?,
            expected
        );
        assert_eq!(
            standardize(once.clone(), &mut GeometryWork::default())?,
            once
        );
        Ok(())
    }
}
