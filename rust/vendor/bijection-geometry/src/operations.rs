//! Mathematical geometry operations, their parameters and pure kernels.
//! Logical row contracts and execution placement belong to query semantics.
use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};

use crate::{
    measure::Measurement,
    CoordinateFrame,
    Dimensions,
    GeometrySpace,
};

#[derive(Clone, Copy)]
enum SpaceRule {
    Xy,
    Wgs84Xy,
    Preserve,
    Xyz,
    CartesianXyz,
}

#[derive(Clone, Copy)]
enum PureKernel {
    Measure(Measurement),
    GroundProjection,
    Centroid,
    Standardize,
    Affine3d,
    Destination,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferCap {
    Round,
    Square,
    Flat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferJoin {
    Round,
    Mitre,
    Bevel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overlay {
    Intersection,
    Union,
    Difference,
    SymmetricDifference,
}

/// Finite binary64 constants normalize signed zero at semantic admission.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Finite(f64);
impl Eq for Finite {}
impl Finite {
    pub fn get(self) -> f64 {
        self.0
    }
}
impl TryFrom<f64> for Finite {
    type Error = String;

    fn try_from(value: f64) -> Result<Self, String> {
        if !value.is_finite() {
            return Err("A numerical constant is finite".into());
        }
        Ok(Self(if value == 0.0 { 0.0 } else { value }))
    }
}
impl<'de> Deserialize<'de> for Finite {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let number = serde_json::Number::deserialize(deserializer)?;
        let value = number
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("A numerical constant is finite"))?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

/// An explicit coordinate map and its mathematical domain. Resource names are
/// logical operands: their content changes the map; installation paths do not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinateMap {
    pub pipeline: String,
    pub resources: BTreeMap<String, String>,
    pub source_extent: Option<[Finite; 4]>,
    pub target_extent: Option<[Finite; 4]>,
}

impl CoordinateMap {
    pub fn validate(&self) -> Result<(), String> {
        if self.pipeline.trim().is_empty()
            || self.pipeline.len() > 16_384
            || self.pipeline.chars().any(char::is_control)
        {
            return Err("A coordinate map requires a bounded explicit operation".into());
        }
        if self.resources.len() > 64
            || self.resources.iter().any(|(name, digest)| {
                name.is_empty()
                    || name.len() > 256
                    || matches!(name.as_str(), "." | "..")
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
                    || digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return Err("Coordinate map resources require names and exact SHA-256 content".into());
        }
        for extent in [&self.source_extent, &self.target_extent]
            .into_iter()
            .flatten()
        {
            if extent[0].get() >= extent[2].get() || extent[1].get() >= extent[3].get() {
                return Err("A coordinate map extent has increasing coordinate bounds".into());
            }
        }
        Ok(())
    }
}

/// Operand roles and result domains also generate the SDK authoring surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GeometryOperand {
    Geometry,
    Point,
    Metres,
}

impl GeometryOperand {
    /// Operand domains drive literal admission and logical typing alike.
    pub fn geometry_kinds(self) -> Option<&'static [crate::Kind]> {
        match self {
            Self::Geometry => Some(&crate::Kind::ALL),
            Self::Point => Some(&[crate::Kind::Point]),
            Self::Metres => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GeometryResult {
    Geometry,
    Same,
    Point,
    Polygons,
    Number,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct GeometryOperatorDescription {
    pub id: &'static str,
    pub operands: &'static [GeometryOperand],
    pub result: GeometryResult,
}

macro_rules! geometry_operations {
    ($( $variant:ident $( { $($field:ident: $field_type:ty),* $(,)? } )? =>
        ($id:literal, [$($operand:ident),*], $result:ident,
         $space_rule:ident, $kernel:expr) ),* $(,)?) => {
        /// Mathematical parameters are independent of libraries and work policy.
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(tag = "kind", deny_unknown_fields)]
        pub enum GeometryOperation {
            $( #[serde(rename = $id)] $variant $( { $($field: $field_type),* } )? ),*
        }
        pub const GEOMETRY_OPERATORS: &[GeometryOperatorDescription] = &[
            $( GeometryOperatorDescription {
                id: $id,
                operands: &[$(GeometryOperand::$operand),*],
                result: GeometryResult::$result,
            } ),*
        ];
        impl GeometryOperation {
            pub fn description(&self) -> GeometryOperatorDescription {
                match self {
                    $( Self::$variant $( { $($field: _),* } )? => GeometryOperatorDescription {
                        id: $id,
                        operands: &[$(GeometryOperand::$operand),*],
                        result: GeometryResult::$result,
                    } ),*
                }
            }
            pub fn arity(&self) -> usize { self.description().operands.len() }
            fn space_rule(&self) -> SpaceRule {
                match self {
                    $(Self::$variant $( { $($field: _),* } )? => SpaceRule::$space_rule),*
                }
            }
            fn pure_kernel(&self) -> Option<PureKernel> {
                match self { $(Self::$variant $( { $($field: _),* } )? => $kernel),* }
            }

        }
    };
}

geometry_operations! {
    Buffer { cap: BufferCap, join: BufferJoin, quadrant_segments: u16 } => ("buffer", [Geometry, Metres], Polygons, Xy, None),
    Simplify => ("simplify", [Geometry, Metres], Same, Xy, None),
    Centroid => ("centroid", [Geometry], Point, Xy, Some(PureKernel::Centroid)),
    Standardize => ("standardize", [Geometry], Same, Preserve, Some(PureKernel::Standardize)),
    ConvexHull => ("convex_hull", [Geometry], Geometry, Xy, None),
    Boundary => ("boundary", [Geometry], Geometry, Xy, None),
    Area => ("area", [Geometry], Number, Xy, Some(PureKernel::Measure(Measurement::Area))),
    Length => ("length", [Geometry], Number, Xy, Some(PureKernel::Measure(Measurement::Length))),
    Perimeter => ("perimeter", [Geometry], Number, Xy, Some(PureKernel::Measure(Measurement::Perimeter))),
    Overlay { operation: Overlay } => ("overlay", [Geometry, Geometry], Geometry, Xy, None),
    Transform { target: GeometrySpace, mapping: CoordinateMap } => ("transform", [Geometry], Same, Preserve, None),
    GroundProjection => ("ground_projection", [Geometry], Geometry, Xyz, Some(PureKernel::GroundProjection)),
    Affine3d { matrix: [[Finite; 4]; 3] } => ("affine_3d", [Geometry], Same, CartesianXyz, Some(PureKernel::Affine3d)),
    Destination { bearing_degrees: Finite, distance_m: Finite } => ("destination", [Point], Point, Wgs84Xy, Some(PureKernel::Destination)),
}

impl GeometryOperation {
    /// Mathematical parameter and coordinate-space validation is independent
    /// of the implementation selected to execute this operation.
    pub fn output_space(&self, space: &GeometrySpace) -> Result<GeometrySpace, String> {
        space.validate()?;
        match self.space_rule() {
            SpaceRule::Xy | SpaceRule::Wgs84Xy if !space.is_ground() => {
                return Err(
                    "Planar and surface operations require explicit ground projection".into(),
                )
            },
            SpaceRule::Wgs84Xy if space.frame != CoordinateFrame::Wgs84 => {
                return Err("A spherical destination requires a WGS84 frame".into());
            },
            SpaceRule::Xyz | SpaceRule::CartesianXyz
                if !matches!(space.dimensions, Dimensions::Xyz { .. }) =>
            {
                return Err("Height operations require declared XYZ geometry".into())
            },
            SpaceRule::CartesianXyz
                if !matches!(space.frame, CoordinateFrame::Cartesian { .. }) =>
            {
                return Err("Affine geometry requires a Cartesian frame".into())
            },
            SpaceRule::Xy
            | SpaceRule::Wgs84Xy
            | SpaceRule::Preserve
            | SpaceRule::Xyz
            | SpaceRule::CartesianXyz => {},
        }
        if let Self::Destination { distance_m, .. } = self
            && (distance_m.get() < 0.0
                || distance_m.get() > crate::predicates::MAX_POINT_DISTANCE_METRES)
        {
            return Err("Destination distance is zero to half a meridian".into());
        }
        if let Self::Buffer {
            quadrant_segments, ..
        } = self
            && !(1..=64).contains(quadrant_segments)
        {
            return Err("Buffer quadrants require between one and 64 segments".into());
        }
        if let Self::Transform { target, mapping } = self {
            mapping.validate()?;
            target.validate()?;
            if space.dimensions != target.dimensions {
                return Err(
                    "Coordinate transformation preserves dimensions; ground projection and \
                     vertical conversion are explicit"
                        .into(),
                );
            }
            return Ok(target.clone());
        }
        Ok(if matches!(self, Self::GroundProjection) {
            GeometrySpace {
                dimensions: Dimensions::Xy,
                ..space.clone()
            }
        } else {
            space.clone()
        })
    }

    pub fn supports_pure_kernel(&self, space: &GeometrySpace) -> bool {
        self.output_space(space).is_ok()
            && space.validate_kernel().is_ok()
            && match self.pure_kernel() {
                Some(PureKernel::Measure(_) | PureKernel::Centroid) => space.is_ground(),
                Some(PureKernel::GroundProjection) => !space.is_ground(),
                Some(PureKernel::Standardize | PureKernel::Affine3d | PureKernel::Destination) => {
                    true
                },
                None => false,
            }
    }

    pub fn evaluate_pure(
        &self,
        value: crate::Geometry,
        work: &mut crate::GeometryWork,
    ) -> Result<KernelResult, crate::GeometryRefusal> {
        self.output_space(&value.space)
            .map_err(crate::unsupported_space)?;
        if !self.supports_pure_kernel(&value.space) {
            return Err(crate::invalid(
                "No pure kernel for the declared geometry operation",
            ));
        }
        match self.pure_kernel() {
            Some(PureKernel::Measure(measurement)) => Ok(KernelResult::Number(
                crate::measure::measure(&value, measurement, work)?,
            )),
            Some(PureKernel::GroundProjection) => {
                Ok(KernelResult::Geometry(value.ground_projection(work)?))
            },
            Some(PureKernel::Centroid) => Ok(KernelResult::Geometry(crate::centroid::centroid(
                &value, work,
            )?)),
            Some(PureKernel::Standardize) => Ok(KernelResult::Geometry(
                crate::standardize::standardize(value, work)?,
            )),
            Some(PureKernel::Affine3d) => {
                let Self::Affine3d { matrix } = self else {
                    unreachable!("affine kernel parameters")
                };
                Ok(KernelResult::Geometry(crate::affine::apply(
                    value, matrix, work,
                )?))
            },
            Some(PureKernel::Destination) => {
                let Self::Destination {
                    bearing_degrees,
                    distance_m,
                } = self
                else {
                    unreachable!("destination kernel parameters")
                };
                Ok(KernelResult::Geometry(crate::construction::destination(
                    value,
                    bearing_degrees.get(),
                    distance_m.get(),
                    work,
                )?))
            },
            None => Err(crate::invalid(
                "No pure kernel for the declared geometry operation",
            )),
        }
    }
}

/// Carrier adapters preserve the scalar's analyzed result domain.
pub enum KernelResult {
    Geometry(crate::Geometry),
    Number(f64),
}
