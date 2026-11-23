//! Exact binary64 affine operands, rounded once per output ordinate. The
//! resulting value is admitted again in its unchanged coordinate space.
use num_traits::ToPrimitive;

use crate::{
    exact::{
        arithmetic,
        binary64,
        bytes,
        Rational,
    },
    operations::Finite,
    Coords,
    Geometry,
    GeometryDeclaration,
    GeometryRefusal,
    GeometryWork,
    Gv,
    Pos,
    Size,
};

fn map(
    value: &mut Geometry,
    matrix: &[[Rational; 4]; 3],
    work: &mut GeometryWork,
) -> Result<(), GeometryRefusal> {
    if let Coords::Collection(members) = &mut value.coords {
        for member in members {
            map(member, matrix, work)?;
        }
        return Ok(());
    }
    let mut heights = value.heights.as_mut().expect("declared XYZ").iter_mut();
    let mut position = |point: &mut Pos| -> Result<(), GeometryRefusal> {
        let height = heights.next().expect("admitted height");
        let output = work.scoped(|work| {
            let input = binary64([point[0], point[1], *height], work)?;
            let mut output = [0.0; 3];
            for (axis, row) in matrix.iter().enumerate() {
                work.allocate(bytes(&row[3]))?;
                let mut sum = row[3].clone();
                for (coefficient, coordinate) in row[..3].iter().zip(&input) {
                    sum = arithmetic(work, &[&sum, coefficient, coordinate], || {
                        &sum + coefficient * coordinate
                    })?;
                    work.allocate(bytes(&sum))?;
                }
                output[axis] = arithmetic(work, &[&sum], || sum.to_f64())?
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| crate::invalid("Affine coordinate exceeds finite binary64"))?;
                if output[axis] == 0.0 {
                    output[axis] = 0.0;
                }
            }
            Ok(output)
        })?;
        *point = [output[0], output[1]];
        *height = output[2];
        Ok(())
    };
    match &mut value.coords {
        Coords::Empty => {},
        Coords::Point(point) => position(point)?,
        Coords::MultiPoint(points) => {
            for point in points.iter_mut().flatten() {
                position(point)?;
            }
        },
        Coords::Points(points) => {
            for point in points {
                position(point)?;
            }
        },
        Coords::Lines(lines) => {
            for point in lines.iter_mut().flatten() {
                position(point)?;
            }
        },
        Coords::Polygons(polygons) => {
            for point in polygons.iter_mut().flatten().flatten() {
                position(point)?;
            }
        },
        Coords::Collection(_) => unreachable!("collections mapped above"),
    }
    Ok(())
}

pub(crate) fn apply(
    mut value: Geometry,
    matrix: &[[Finite; 4]; 3],
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    work.scoped(|work| {
        let matrix = [
            binary64(matrix[0].map(Finite::get), work)?,
            binary64(matrix[1].map(Finite::get), work)?,
            binary64(matrix[2].map(Finite::get), work)?,
        ];
        map(&mut value, &matrix, work)?;
        work.charge(
            value
                .position_count()
                .saturating_add(value.component_count()),
        )?;
        work.allocate(value.retained_bytes().saturating_mul(16))?;
        let raw = Gv::from(&value);
        crate::admit_value_with_work(
            &raw,
            &GeometryDeclaration {
                space: value.space,
                size: Size::Large,
            },
            work,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CoordinateFrame,
        Dimensions,
        GeometrySpace,
        Kind,
        LinearUnit,
        VerticalReference,
    };

    #[test]
    fn affine_rounds_after_cancellation_and_preserves_empty_xyz() -> anyhow::Result<()> {
        let space = GeometrySpace {
            segments: crate::SegmentModel::CoordinateLinear,
            frame: CoordinateFrame::Cartesian {
                name: "local".into(),
                unit: LinearUnit::metre(),
            },
            dimensions: Dimensions::Xyz {
                vertical: VerticalReference::CartesianAxis,
                unit: LinearUnit::metre(),
            },
            ..GeometrySpace::wgs84_xy()
        };
        let value = Geometry {
            kind: Kind::MultiPoint,
            coords: Coords::MultiPoint(vec![None, Some([1e16, 1.0])]),
            heights: Some(vec![-1e16]),
            space,
        };
        let matrix = [[1., 1., 1., 0.], [0., 1., 0., 0.], [0., 0., 1., 0.]]
            .map(|row| row.map(|value| Finite::try_from(value).unwrap()));
        let output = apply(value, &matrix, &mut GeometryWork::default())?;
        assert_eq!(
            output.coords,
            Coords::MultiPoint(vec![None, Some([1., 1.])])
        );
        assert_eq!(output.heights, Some(vec![-1e16]));
        Ok(())
    }
}
