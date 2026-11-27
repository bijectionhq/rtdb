use geometry::GeometryWork;
use rtdb_types::{Result, SpatialGeometryEncoding, Value};

use super::{Location, Predicate, WORK_BYTES, WORK_STEPS, invalid};
use crate::{EvalContext, compare::nullish, numeric::number};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpatialFunction {
    Match,
    Distance,
}

impl SpatialFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "spatial_match" => Self::Match,
            "spatial_distance" => Self::Distance,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::Match => "spatial_match",
            Self::Distance => "spatial_distance",
        }
    }

    pub(crate) fn eval(self, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        let [value, region, encoding, operation, bounds @ ..] = args else {
            return Err(invalid("A spatial expression requires a value, region, encoding and operation"));
        };
        if [value, region, encoding, operation].into_iter().any(nullish) {
            return Ok(Value::Null);
        }
        let (Value::Binary(value), Value::Binary(region), Value::Utf8(encoding), Value::Utf8(operation)) =
            (value, region, encoding, operation)
        else {
            return Err(invalid("Spatial expressions require binary geometry and text encoding and operation names"));
        };
        let encoding = SpatialGeometryEncoding::parse(encoding)?;
        let radius = bound(bounds.first())?;
        let after = bound(bounds.get(1))?;
        if after.zip(radius).is_some_and(|(after, radius)| after > radius) {
            return Err(invalid("The exclusive lower bound exceeds the radius"));
        }
        let predicate = match (self, operation.as_str()) {
            (Self::Distance, "wgs84_geometry_m") => Predicate::parse("nearest", radius, false)?,
            (Self::Distance, "wgs84_reported_center_m") => Predicate::parse("nearest", radius, true)?,
            (Self::Distance, _) => return Err(invalid("Unknown WGS84 distance metric")),
            (Self::Match, "nearest") => return Err(invalid("Use SPATIAL_DISTANCE for nearest")),
            _ => Predicate::parse(operation, radius, false)?,
        };
        let _lease = context.allocation.map(|scope| scope.memory_budget().reserve(WORK_BYTES)).transpose()?;
        let mut work = GeometryWork::with_limits(WORK_STEPS, WORK_BYTES);
        let region = if matches!(predicate, Predicate::All) {
            if !region.is_empty() {
                return Err(invalid("An unrestricted spatial read takes no region"));
            }
            None
        } else {
            let region = Location::decode(region, &mut work)?;
            predicate.validate_region(&region)?;
            Some(region)
        };
        let Some(value) = Location::decode_stored(value, encoding, &mut work)? else {
            return Ok(Value::Null);
        };
        let Some(region) = region else { return Ok(Value::Bool(true)); };
        let outcome = predicate.evaluate(&value, &region.geometry, None, &mut work)?;
        Ok(match predicate {
            Predicate::Nearest { .. } => outcome.flatten()
                .filter(|distance| after.is_none_or(|bound| *distance > bound))
                .map_or(Value::Null, Value::F64),
            _ => Value::Bool(outcome.is_some()),
        })
    }
}

fn bound(value: Option<&Value>) -> Result<Option<f64>> {
    value.filter(|value| !nullish(value)).map(|value| {
        number(value).filter(|bound| bound.is_finite() && *bound >= 0.0)
            .ok_or_else(|| invalid("A spatial bound must be finite and nonnegative"))
    }).transpose()
}

#[cfg(test)]
mod tests {
    use geometry::{Coords, Geometry, GeometrySpace, Kind};

    use super::*;

    #[test]
    fn exact_spatial_expressions_share_topology_distance_and_boundary_rules() {
        let point = |x| Value::Binary([vec![0], Geometry::new(Kind::Point, Coords::Point([x, 0.0]),
            GeometrySpace::wgs84_xy()).wkb()].concat().into());
        let context = EvalContext::default();
        let mut args = [point(1.0), point(1.0), Value::Utf8("tagged-wkb-v1".into()),
            Value::Utf8("equals".into()), Value::Null, Value::Null];
        assert_eq!(SpatialFunction::Match.eval(&args[..4], &context).unwrap(), Value::Bool(true));
        args[3] = Value::Utf8("wgs84_geometry_m".into());
        args[4] = Value::F64(0.0);
        assert_eq!(SpatialFunction::Distance.eval(&args, &context).unwrap(), Value::F64(0.0));
        args[5] = Value::F64(0.0);
        assert_eq!(SpatialFunction::Distance.eval(&args, &context).unwrap(), Value::Null);
        args[1] = point(2.0);
        assert_eq!(SpatialFunction::Distance.eval(&args[..5], &context).unwrap(), Value::Null);
    }
}
