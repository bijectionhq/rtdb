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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoordinateFrame {
    /// Longitude and latitude in degrees on WGS84, in that order.
    Wgs84,
    /// WGS84 longitude and latitude in degrees read on the sphere of radius
    /// `radius_m`, geodetic latitude as spherical latitude: the warehouse
    /// `GEOGRAPHY` model. It measures on that sphere, so a sphere answer is
    /// never a geodesic one; its edges are the same coordinate curves as
    /// WGS84's (see `SegmentModel::GreatCircle`), so topology agrees.
    Wgs84Sphere {
        #[cfg_attr(feature = "bindings", ts(type = "number"))]
        radius_m: Positive,
    },
    /// A named Cartesian frame. A projected CRS is a particular named frame;
    /// its transformation remains an explicit computation.
    Cartesian { name: String, unit: LinearUnit },
}

impl CoordinateFrame {
    /// The sphere frame of radius `radius_m` metres.
    pub fn wgs84_sphere(radius_m: f64) -> Result<Self, String> {
        Ok(Self::Wgs84Sphere { radius_m: Positive::new(radius_m)? })
    }

    /// Whether positions are longitude and latitude in degrees (WGS84 or its
    /// sphere reading): the frames whose edges are arcs of one auxiliary
    /// sphere and whose topology is decided there.
    pub fn is_geographic(&self) -> bool {
        matches!(self, Self::Wgs84 | Self::Wgs84Sphere { .. })
    }

    /// The radius, in metres, of a sphere frame.
    pub fn sphere_radius_m(&self) -> Option<f64> {
        match self {
            Self::Wgs84Sphere { radius_m } => Some(radius_m.get()),
            Self::Wgs84 | Self::Cartesian { .. } => None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Wgs84 | Self::Wgs84Sphere { .. } => Ok(()),
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
#[serde(deny_unknown_fields)]
pub struct LinearUnit {
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Dimensions {
    Xy,
    Xyz {
        vertical: VerticalReference,
        unit: LinearUnit,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VerticalReference {
    Wgs84Ellipsoid,
    CartesianAxis,
    Named { name: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentModel {
    CoordinateLinear,
    /// The shorter intersection of WGS84 with the plane through the two
    /// vertices and the ellipsoid's centre. This is not a geodesic.
    GreatElliptic,
    /// The shorter great-circle arc of a `wgs84_sphere` frame, geodetic
    /// latitude read as spherical latitude. The direction (cos λ, sin λ,
    /// tan φ) is the great elliptic one, (cos λ, sin λ, (1 − f) tan φ),
    /// scaled along z, and a positive linear map keeps planes through the
    /// centre and minor arcs: both models name the same longitude/latitude
    /// curve, so they differ in metric only.
    GreatCircle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyModel {
    Binary64Exact,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericalPolicy {
    /// Topology uses exact admitted coordinates; metric error never thickens a
    /// boundary.
    pub topology: TopologyModel,
    /// Absolute distance error, in metres, independent of the coordinate frame.
    pub distance_absolute_m: Positive,
    pub distance_relative: Positive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    /// The sphere space of radius `radius_m` metres (XY, great-circle edges).
    pub fn wgs84_sphere(radius_m: f64) -> Result<Self, String> {
        Ok(Self {
            frame: CoordinateFrame::wgs84_sphere(radius_m)?,
            dimensions: Dimensions::Xy,
            segments: SegmentModel::GreatCircle,
            numerics: NumericalPolicy::binary64(),
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
            | (CoordinateFrame::Wgs84Sphere { .. }, SegmentModel::GreatCircle)
            | (CoordinateFrame::Cartesian { .. }, SegmentModel::CoordinateLinear) => {},
            _ => {
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
            | (CoordinateFrame::Wgs84Sphere { .. }, Dimensions::Xy)
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
                CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. } => 0,
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

    /// GeoArrow has no great-elliptic edge model, and its spherical edges
    /// carry no radius. A native extension prevents generic consumers from
    /// silently treating these WKB vertices as linear.
    pub fn arrow_extension(&self) -> &'static str {
        match self.segments {
            SegmentModel::CoordinateLinear => "geoarrow.wkb",
            SegmentModel::GreatElliptic | SegmentModel::GreatCircle => "bijection.geometry",
        }
    }
}
