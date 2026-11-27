//! Geometry meaning, independent of admission limits and physical placement.
use std::{
    cmp::Ordering,
    hash::{
        Hash,
        Hasher,
    },
};

use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};

/// A finite positive numerical parameter. The constructor and wire decoder
/// enforce the invariant, so equality and hashing never encounter NaN or -0.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Positive(f64);

impl Positive {
    pub fn new(value: f64) -> Result<Self, String> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err("A numerical parameter is finite and positive".into())
        }
    }

    pub fn get(self) -> f64 {
        self.0
    }
}
impl Eq for Positive {}
impl PartialOrd for Positive {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Positive {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl Hash for Positive {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
impl<'de> Deserialize<'de> for Positive {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // A JSON number buffered by an enclosing untagged or internally
        // tagged enum keeps its exact token; `Number` reads both forms.
        let value = serde_json::Number::deserialize(deserializer)?
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("A numerical parameter is a finite number"))?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoordinateFrame {
    /// Longitude and latitude in degrees on WGS84, in that order.
    Wgs84,
    /// A named Cartesian frame. A projected CRS is a particular named frame;
    /// its transformation remains an explicit computation.
    Cartesian { name: String, unit: LinearUnit },
}

impl CoordinateFrame {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Wgs84 => Ok(()),
            Self::Cartesian { name, .. } => {
                if name.is_empty() || name.len() > 4096 || name.chars().any(char::is_control) {
                    return Err("A Cartesian frame has a bounded nonempty name".into());
                }
                Ok(())
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(deny_unknown_fields)]
pub struct LinearUnit {
    #[cfg_attr(feature = "bindings", ts(type = "number"))]
    pub metres_per_unit: Positive,
}

impl LinearUnit {
    pub const fn metre() -> Self {
        Self {
            metres_per_unit: Positive(1.0),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Dimensions {
    Xy,
    Xyz {
        vertical: VerticalReference,
        unit: LinearUnit,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerticalReference {
    Wgs84Ellipsoid,
    CartesianAxis,
    Named { name: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum SegmentModel {
    CoordinateLinear,
    /// The shorter intersection of WGS84 with the plane through the two
    /// vertices and the ellipsoid's centre. This is not a geodesic.
    GreatElliptic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum TopologyModel {
    Binary64Exact,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(deny_unknown_fields)]
pub struct NumericalPolicy {
    /// Topology uses exact admitted coordinates; metric error never thickens a
    /// boundary.
    pub topology: TopologyModel,
    /// Absolute distance error, in metres, independent of the coordinate frame.
    #[cfg_attr(feature = "bindings", ts(type = "number"))]
    pub distance_absolute_m: Positive,
    #[cfg_attr(feature = "bindings", ts(type = "number"))]
    pub distance_relative: Positive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[serde(deny_unknown_fields)]
pub struct GeometrySpace {
    pub frame: CoordinateFrame,
    pub dimensions: Dimensions,
    pub segments: SegmentModel,
    pub numerics: NumericalPolicy,
}

impl NumericalPolicy {
    pub const fn binary64() -> Self {
        Self {
            topology: TopologyModel::Binary64Exact,
            distance_absolute_m: Positive(1e-3),
            distance_relative: Positive(1e-7),
        }
    }
}

impl GeometrySpace {
    pub const fn wgs84_xy() -> Self {
        Self::wgs84(Dimensions::Xy)
    }

    pub const fn wgs84_xyz() -> Self {
        Self::wgs84(Dimensions::Xyz {
            vertical: VerticalReference::Wgs84Ellipsoid,
            unit: LinearUnit::metre(),
        })
    }

    pub const fn wgs84(dimensions: Dimensions) -> Self {
        Self {
            frame: CoordinateFrame::Wgs84,
            dimensions,
            segments: SegmentModel::GreatElliptic,
            numerics: NumericalPolicy::binary64(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.numerics.distance_relative.get() >= 1.0 {
            return Err("Relative distance error is less than one".into());
        }
        self.frame.validate()?;
        match (&self.frame, self.segments) {
            (CoordinateFrame::Wgs84, SegmentModel::GreatElliptic)
            | (CoordinateFrame::Cartesian { .. }, SegmentModel::CoordinateLinear) => {},
            (CoordinateFrame::Wgs84, SegmentModel::CoordinateLinear)
            | (CoordinateFrame::Cartesian { .. }, SegmentModel::GreatElliptic) => {
                return Err(
                    "Segment model must match the coordinate frame; convert explicitly".into(),
                );
            },
        }
        if let Dimensions::Xyz {
            vertical: VerticalReference::Named { name },
            ..
        } = &self.dimensions
            && (name.is_empty() || name.len() > 4096 || name.chars().any(char::is_control))
        {
            return Err("A vertical reference has a bounded nonempty name".into());
        }
        // The frame fixes the metric: WGS84 measures on its ellipsoid and a
        // Cartesian frame in its declared unit.
        match (&self.frame, &self.dimensions) {
            (CoordinateFrame::Wgs84, Dimensions::Xy)
            | (
                CoordinateFrame::Wgs84,
                Dimensions::Xyz {
                    vertical: VerticalReference::Wgs84Ellipsoid,
                    ..
                },
            )
            | (CoordinateFrame::Cartesian { .. }, _) => Ok(()),
            _ => Err("Ordinate references must match their coordinate frame".into()),
        }
    }

    /// Native and portable kernels implement this explicit numerical contract.
    pub fn validate_kernel(&self) -> Result<(), crate::GeometryRefusal> {
        self.validate().map_err(crate::unsupported_space)?;
        if self.numerics != NumericalPolicy::binary64() {
            return Err(crate::unsupported_space("Unsupported numerical policy"));
        }
        Ok(())
    }

    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.frame {
                CoordinateFrame::Wgs84 => 0,
                CoordinateFrame::Cartesian { name, .. } => name.capacity(),
            }
            + match &self.dimensions {
                Dimensions::Xyz {
                    vertical: VerticalReference::Named { name },
                    ..
                } => name.capacity(),
                Dimensions::Xy | Dimensions::Xyz { .. } => 0,
            }
    }

    pub fn is_ground(&self) -> bool {
        self.dimensions == Dimensions::Xy
    }

    /// GeoArrow has no great-elliptic edge model. A native extension prevents
    /// generic consumers from silently treating these WKB vertices as linear.
    pub fn arrow_extension(&self) -> &'static str {
        match self.segments {
            SegmentModel::CoordinateLinear => "geoarrow.wkb",
            SegmentModel::GreatElliptic => "bijection.geometry",
        }
    }
}
