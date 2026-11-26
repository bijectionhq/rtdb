//! Value binding for the shared kernel. Frontends marshal values; all admission
//! and codec decisions remain here, including valid empty values.
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::{
    GeometryDeclaration,
    GeometryRefusal,
    GeometryWork,
    Gv,
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(deny_unknown_fields)]
pub struct GeometryAllowance {
    pub steps: u32,
    pub allocated_bytes: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum GeometryRequest {
    Supports {
        operator: crate::GeometryOperation,
        space: crate::GeometrySpace,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    Evaluate {
        operator: crate::GeometryOperation,
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    Measure {
        measurement: crate::measure::Measurement,
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// Great elliptic pieces may use an adjacent WGS84 longitude turn.
    /// They are kernel coordinates, not stored GeoJSON geometry values.
    SegmentDistance {
        space: crate::GeometrySpace,
        left: [[f64; 2]; 2],
        right: [[f64; 2]; 2],
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// The position a fraction of the way along the WGS84 geodesic.
    GeodesicPosition {
        start: [f64; 2],
        end: [f64; 2],
        fraction: f64,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// Initial geodesic azimuth in degrees, the inverse of `destination`.
    Bearing {
        from: [f64; 2],
        to: [f64; 2],
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// The smallest cap holding a WGS84 value and its segments.
    BoundingCap {
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// Coordinate bounds of complete native geometries in one space, including
    /// curved edges and held poles. Empty members contribute no bounds; east
    /// may be one turn past west.
    Bounds {
        values: Vec<Value>,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// Metres east and north in the azimuthal equidistant frame at `centre`.
    LocalFrame {
        space: crate::GeometrySpace,
        centre: [f64; 2],
        positions: Vec<[f64; 2]>,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    SimplifyTrajectory {
        runs: Vec<Vec<[f64; 2]>>,
        tolerance_m: f64,
        max_points: u32,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// The `geometry_union` reducer for external accumulators: callers drop
    /// missing values; an empty population yields an empty collection.
    Union {
        values: Vec<Value>,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// Admits a WGS84 value whose edges are straight in longitude/latitude
    /// (RFC 7946 GeoJSON, most files) as the same region with great
    /// elliptic edges: rings reoriented, edges densified, cut parts joined.
    FromPlanar {
        value: Value,
        declaration: GeometryDeclaration,
        /// How far a densified piece may stray from its edge, in metres.
        tolerance_m: f64,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    /// A value with edges straight in longitude/latitude describing an
    /// admitted one within the distance slack, cut at the antimeridian.
    ToPlanar {
        value: Value,
        declaration: GeometryDeclaration,
        /// How far a densified piece may stray from its edge, in metres.
        tolerance_m: f64,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    Admit {
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    AdmitPosition {
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    GroundProjection {
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    Encode {
        value: Value,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
    Decode {
        bytes: Vec<u8>,
        declaration: GeometryDeclaration,
        #[serde(default)]
        #[cfg_attr(feature = "bindings", ts(optional))]
        allowance: Option<GeometryAllowance>,
    },
}

fn evaluate(request: GeometryRequest) -> Result<Value, GeometryRefusal> {
    let allowance = match &request {
        GeometryRequest::Supports { allowance, .. }
        | GeometryRequest::Evaluate { allowance, .. }
        | GeometryRequest::Measure { allowance, .. }
        | GeometryRequest::Union { allowance, .. }
        | GeometryRequest::FromPlanar { allowance, .. }
        | GeometryRequest::ToPlanar { allowance, .. }
        | GeometryRequest::Admit { allowance, .. }
        | GeometryRequest::AdmitPosition { allowance, .. }
        | GeometryRequest::GroundProjection { allowance, .. }
        | GeometryRequest::Encode { allowance, .. }
        | GeometryRequest::Decode { allowance, .. }
        | GeometryRequest::SegmentDistance { allowance, .. } => *allowance,
        GeometryRequest::GeodesicPosition { allowance, .. }
        | GeometryRequest::Bearing { allowance, .. }
        | GeometryRequest::BoundingCap { allowance, .. }
        | GeometryRequest::Bounds { allowance, .. }
        | GeometryRequest::LocalFrame { allowance, .. }
        | GeometryRequest::SimplifyTrajectory { allowance, .. } => *allowance,
    };
    let mut work = allowance.map_or_else(GeometryWork::default, |allowance| {
        GeometryWork::with_limits(allowance.steps as usize, allowance.allocated_bytes as usize)
    });
    fn admit(
        value: &Value,
        declaration: &GeometryDeclaration,
        work: &mut GeometryWork,
    ) -> Result<crate::Geometry, GeometryRefusal> {
        GeometryDeclaration::parse(declaration.space.clone(), declaration.size.name())
            .map_err(crate::unsupported_space)?;
        crate::admit_value_with_work(&Gv::from(value), declaration, work)
    }
    let value = (|| -> Result<Value, GeometryRefusal> {
        Ok(match request {
            GeometryRequest::Supports {
                operator, space, ..
            } => {
                operator
                    .output_space(&space)
                    .map_err(crate::unsupported_space)?;
                serde_json::json!(operator.supports_pure_kernel(&space))
            },
            GeometryRequest::Evaluate {
                operator,
                value,
                declaration,
                ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                match operator.evaluate_pure(geometry, &mut work)? {
                    crate::operations::KernelResult::Geometry(value) => {
                        serde_json::from_str(&value.js_json()).expect("canonical geometry")
                    },
                    crate::operations::KernelResult::Number(value) => serde_json::json!(value),
                }
            },
            GeometryRequest::Measure {
                measurement,
                value,
                declaration,
                ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                serde_json::json!(crate::measure::measure(&geometry, measurement, &mut work)?)
            },
            GeometryRequest::SegmentDistance {
                space, left, right, ..
            } => {
                serde_json::json!(crate::predicates::segments_distance(
                    &space, left, right, &mut work
                )?)
            },
            GeometryRequest::GeodesicPosition {
                start,
                end,
                fraction,
                ..
            } => {
                work.charge(3)?;
                crate::trajectory::validate_position(start)?;
                crate::trajectory::validate_position(end)?;
                serde_json::json!(crate::predicates::segment_distance::geodesic_position(
                    start, end, fraction
                )?)
            },
            GeometryRequest::Bearing { from, to, .. } => {
                work.charge(1)?;
                crate::trajectory::validate_position(from)?;
                crate::trajectory::validate_position(to)?;
                serde_json::json!(crate::construction::initial_bearing(from, to))
            },
            GeometryRequest::BoundingCap {
                value, declaration, ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                serde_json::to_value(crate::construction::bounding_cap(&geometry, &mut work)?)
                    .expect("bounding cap")
            },
            GeometryRequest::Bounds {
                values,
                declaration,
                ..
            } => {
                GeometryDeclaration::parse(declaration.space.clone(), declaration.size.name())
                    .map_err(crate::unsupported_space)?;
                work.allocate(
                    values
                        .len()
                        .saturating_mul(std::mem::size_of::<crate::Geometry>()),
                )?;
                let geometries = values
                    .iter()
                    .map(|value| admit(value, &declaration, &mut work))
                    .collect::<Result<Vec<_>, _>>()?;
                serde_json::json!(crate::arcs::geometries_bounds(&geometries, &mut work)?)
            },
            GeometryRequest::LocalFrame {
                space,
                centre,
                positions,
                ..
            } => serde_json::json!(crate::construction::local_frame(
                &space, centre, &positions, &mut work
            )?),
            GeometryRequest::SimplifyTrajectory {
                runs,
                tolerance_m,
                max_points,
                ..
            } => serde_json::to_value(crate::trajectory::simplify(
                &runs,
                tolerance_m,
                max_points as usize,
                &mut work,
            )?)
            .expect("trajectory simplification"),
            GeometryRequest::Union {
                values,
                declaration,
                ..
            } => {
                let members = values
                    .iter()
                    .map(|value| admit(value, &declaration, &mut work))
                    .collect::<Result<Vec<_>, _>>()?;
                let geometry = crate::union::reduce(members, &declaration.space, &mut work)?;
                serde_json::from_str(&geometry.js_json()).expect("canonical union JSON")
            },
            GeometryRequest::FromPlanar {
                value,
                declaration,
                tolerance_m,
                ..
            } => {
                GeometryDeclaration::parse(declaration.space.clone(), declaration.size.name())
                    .map_err(crate::unsupported_space)?;
                let geometry =
                    crate::planar::from_planar(&Gv::from(&value), &declaration, tolerance_m, &mut work)?;
                serde_json::from_str(&geometry.js_json()).expect("canonical geometry JSON")
            },
            GeometryRequest::ToPlanar {
                value,
                declaration,
                tolerance_m,
                ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                let planar = crate::planar::to_planar(&geometry, tolerance_m, &mut work)?;
                let mut text = String::new();
                crate::js_json(&planar, &mut text)?;
                serde_json::from_str(&text).expect("planar geometry JSON")
            },
            GeometryRequest::Admit {
                value, declaration, ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                serde_json::from_str(&geometry.js_json()).expect("canonical geometry JSON")
            },
            GeometryRequest::AdmitPosition {
                value, declaration, ..
            } => {
                GeometryDeclaration::parse(declaration.space.clone(), declaration.size.name())
                    .map_err(crate::unsupported_space)?;
                let position =
                    crate::admit_position_with_work(&Gv::from(&value), &declaration, &mut work)?;
                let point = crate::Geometry::new(
                    crate::Kind::Point,
                    crate::Coords::Point(position.point),
                    declaration.space,
                );
                let point: Value =
                    serde_json::from_str(&point.js_json()).expect("canonical position point JSON");
                serde_json::json!({"point": point, "accuracy_m": position.accuracy_m.map(|value| if value == 0.0 { 0.0 } else { value })})
            },
            GeometryRequest::GroundProjection {
                value, declaration, ..
            } => {
                let geometry =
                    admit(&value, &declaration, &mut work)?.ground_projection(&mut work)?;
                serde_json::from_str(&geometry.js_json())
                    .expect("canonical projected geometry JSON")
            },
            GeometryRequest::Encode {
                value, declaration, ..
            } => {
                let geometry = admit(&value, &declaration, &mut work)?;
                let bytes = geometry.wkb_len();
                work.allocate(
                    bytes
                        .saturating_mul(1 + std::mem::size_of::<Value>())
                        .saturating_add(256),
                )?;
                work.charge(bytes.saturating_add(geometry.component_count()))?;
                serde_json::json!(geometry.wkb())
            },
            GeometryRequest::Decode {
                bytes, declaration, ..
            } => {
                GeometryDeclaration::parse(declaration.space.clone(), declaration.size.name())
                    .map_err(crate::unsupported_space)?;
                let geometry = crate::wkb::decode_with_work(
                    &bytes,
                    &declaration.space,
                    &declaration.limits(),
                    &mut work,
                )?;
                serde_json::from_str(&geometry.js_json()).expect("canonical geometry JSON")
            },
        })
    })();
    Ok(reply(value, work.usage()))
}

/// A value and the physical work consumed before success or refusal.
pub fn reply(result: Result<Value, GeometryRefusal>, usage: crate::GeometryWorkUsage) -> Value {
    match result {
        Ok(value) => serde_json::json!({"value":value,"usage":usage}),
        Err(refusal) => refusal_reply(refusal, usage),
    }
}

fn refusal_reply(refusal: GeometryRefusal, usage: crate::GeometryWorkUsage) -> Value {
    serde_json::json!({"refusal": {
        "code": refusal.code,
        "message": refusal.message,
        "bound": refusal.bound.map(|bound| serde_json::json!({"kind": bound.kind.name(), "limit": bound.limit})),
    }, "usage":usage})
}

pub fn invoke(input: Value) -> Value {
    let result = serde_json::from_value::<GeometryRequest>(input)
        .map_err(crate::invalid)
        .and_then(evaluate);
    match result {
        Ok(value) => value,
        Err(refusal) => refusal_reply(refusal, GeometryWork::with_limits(0, 0).usage()),
    }
}
