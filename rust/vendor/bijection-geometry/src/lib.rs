//! Pure geometry values, coordinate spaces, admission, canonical codecs and
//! deterministic numerical kernels. Native values, storage keys, execution
//! authority and dataset lifecycle belong to their existing host owners.
//!
//! A declaration separates mathematical space from admission size limits.
//! Coordinates retain their frame and dimensions; no operation implicitly
//! repairs, reprojects or removes heights. Valid empty values remain distinct
//! from absent input. Query operators own their supported operand domains.
//!
//! Every transcendental function goes through `libm`, so native and Wasm
//! builds return identical bits; a source test refuses the `std` methods.
use std::fmt::{
    self,
    Write as _,
};

/// Deterministic maths for spatial owners outside this crate.
pub use libm;

mod affine;
pub mod arcs;
pub mod binding;
pub mod cells;
/// Shared physical bounds generated from the geometry package's bounds schema.
pub mod bounds_generated;
pub mod centroid;
mod construction;
pub mod encounters;
mod exact;
pub mod envelope;
pub mod generator;
pub mod geodesy;
pub mod measure;
pub mod mgrs;
mod offset;
pub mod operations;
mod overlay;
pub mod planar;
use bounds_generated as bounds;
pub use operations::GeometryOperation;

/// Largest admitted absolute WGS84 ellipsoidal height, in metres.
pub const HEIGHT_LIMIT_METRES: f64 = bounds::GEOMETRY_HEIGHT_LIMIT_METRES as f64;
/// Conservative coordinate-plane padding for physical candidate bounds.
pub const TOLERANCE: f64 = bounds::GEOMETRY_TOLERANCE_DEGREES;
pub mod predicates;
pub mod space;
pub mod spatial_tree;
mod sphere;
mod standardize;
mod topology;
pub use space::{
    CoordinateFrame,
    Dimensions,
    GeometrySpace,
    LinearUnit,
    NumericalPolicy,
    Positive,
    SegmentModel,
    TopologyModel,
    VerticalReference,
};
pub mod trajectory;
pub mod union;
pub mod wkb;

#[cfg(test)]
mod value_tests;

/// A physical admission allowance, independent of mathematical space.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Size {
    Standard,
    Large,
}

impl Size {
    pub fn name(self) -> &'static str {
        match self {
            Size::Standard => "standard",
            Size::Large => "large",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "standard" => Some(Size::Standard),
            "large" => Some(Size::Large),
            _ => None,
        }
    }
}

/// What a geometry property declares: its profile and size tier.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct GeometryDeclaration {
    pub space: GeometrySpace,
    pub size: Size,
}

impl GeometryDeclaration {
    /// The declaration of `{ space, size }`, refused when the space is
    /// invalid or the tier name is unknown.
    pub fn parse(space: GeometrySpace, size: &str) -> Result<Self, String> {
        space.validate()?;
        let size =
            Size::parse(size).ok_or_else(|| format!("Unknown geometry admission tier {size:?}"))?;
        Ok(Self { space, size })
    }

    /// The tier's bounds.
    pub fn limits(&self) -> Limits {
        match self.size {
            Size::Standard => STANDARD,
            Size::Large => LARGE,
        }
    }

    /// Whether every value admitted under `self` is admitted under `other`.
    pub fn within(&self, other: &GeometryDeclaration) -> bool {
        self.space == other.space && self.size <= other.size
    }
}

/// A bound of the large tier a value exceeds, named in the bound refusal
/// registry with the limit the check enforces ([`LARGE`]), in its own unit:
/// the elements of one coordinate array, the positions and rings of one
/// value, the bytes of its JSON text. The tier is sized to the engine (one
/// array holds at most 8192 elements, the value layer's `MAX_ARRAY_LEN`,
/// which refuses a longer array before any validator sees it; one stored
/// document at most 1 MiB, `MAX_USER_SIZE`), but a refusal names the tier
/// bound that refused, never an engine constant it did not check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TierBound {
    Array,
    Positions,
    Rings,
    Bytes,
    WorkSteps,
    AllocatedBytes,
    Components,
    Depth,
    WkbBytes,
}

impl TierBound {
    pub fn name(self) -> &'static str {
        match self {
            TierBound::Array => "geometry_array",
            TierBound::Components => "geometry_components",
            TierBound::Depth => "geometry_depth",
            TierBound::WkbBytes => "geometry_wkb_bytes",
            TierBound::Positions => "geometry_positions",
            TierBound::Rings => "geometry_rings",
            TierBound::Bytes => "geometry_bytes",
            TierBound::WorkSteps => "geometry_work_steps",
            TierBound::AllocatedBytes => "geometry_allocated_bytes",
        }
    }

    /// The limit the large tier enforces, in the bound's own unit.
    pub fn limit(self) -> u64 {
        (match self {
            TierBound::Array | TierBound::Components => LARGE.ring_coordinates,
            TierBound::Depth => 16,
            TierBound::WkbBytes => LARGE.wkb_max_bytes(true),
            TierBound::Positions => LARGE.coordinates,
            TierBound::Rings => LARGE.rings,
            TierBound::Bytes => LARGE.bytes,
            TierBound::WorkSteps => bounds::GEOMETRY_WORK_STEPS,
            TierBound::AllocatedBytes => bounds::GEOMETRY_WORK_ALLOCATED_BYTES,
        }) as u64
    }
}

/// The resource category and exact limit that this admission checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GeometryBound {
    pub kind: TierBound,
    pub limit: u64,
}

/// Stable failure categories shared by native and portable adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum GeometryRefusalCode {
    InvalidGeometry,
    UnsupportedCoordinateSpace,
    SpatialPrecisionLoss,
    SpatialWorkLimit,
    ReadWorkLimit,
}

impl GeometryRefusalCode {
    pub fn name(self) -> &'static str {
        match self {
            Self::InvalidGeometry => "InvalidGeometry",
            Self::UnsupportedCoordinateSpace => "UnsupportedCoordinateSpace",
            Self::SpatialPrecisionLoss => "SpatialPrecisionLoss",
            Self::SpatialWorkLimit => "SpatialWorkLimit",
            Self::ReadWorkLimit => "ReadWorkLimit",
        }
    }
}

/// Why a value is not admitted: the twins' text (`Invalid geometry: ...`)
/// and, for a resource refusal at any tier, its category and enforced limit.
#[derive(Clone, Debug, PartialEq)]
pub struct GeometryRefusal {
    pub code: GeometryRefusalCode,
    pub message: String,
    pub bound: Option<GeometryBound>,
}

impl fmt::Display for GeometryRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GeometryRefusal {}

pub fn invalid(reason: impl fmt::Display) -> GeometryRefusal {
    GeometryRefusal {
        code: GeometryRefusalCode::InvalidGeometry,
        message: format!("Invalid geometry: {reason}"),
        bound: None,
    }
}

pub fn unsupported_space(reason: impl fmt::Display) -> GeometryRefusal {
    GeometryRefusal {
        code: GeometryRefusalCode::UnsupportedCoordinateSpace,
        message: format!("Unsupported coordinate space: {reason}"),
        bound: None,
    }
}

/// A size tier of the 2D profile (TypeScript `geometryLimits` /
/// `geometryLimitsLarge`). Every resource refusal names its enforced bound.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub bytes: usize,
    pub coordinates: usize,
    pub rings: usize,
    pub ring_coordinates: usize,
    pub standard: bool,
}

pub const STANDARD: Limits = Limits {
    bytes: bounds::GEOMETRY_STANDARD_BYTES,
    coordinates: bounds::GEOMETRY_STANDARD_COORDINATES,
    rings: bounds::GEOMETRY_STANDARD_RINGS,
    ring_coordinates: bounds::GEOMETRY_STANDARD_RING_COORDINATES,
    standard: true,
};
pub const LARGE: Limits = Limits {
    bytes: bounds::GEOMETRY_LARGE_BYTES,
    coordinates: bounds::GEOMETRY_LARGE_COORDINATES,
    rings: bounds::GEOMETRY_LARGE_RINGS,
    ring_coordinates: bounds::GEOMETRY_LARGE_RING_COORDINATES,
    standard: false,
};

impl Limits {
    pub fn detail(&self, text: impl FnOnce() -> String) -> String {
        if self.standard {
            String::new()
        } else {
            format!(" ({})", text())
        }
    }

    /// The refusal of a value over the tier: past the large tier it names
    /// the tier bound that refused.
    fn over(&self, reason: String, bound: TierBound) -> GeometryRefusal {
        GeometryRefusal {
            code: GeometryRefusalCode::SpatialWorkLimit,
            message: format!("Invalid geometry: {reason}"),
            bound: Some(GeometryBound {
                kind: bound,
                limit: (match bound {
                    TierBound::Array | TierBound::Components => self.ring_coordinates,
                    TierBound::Depth => 16,
                    TierBound::WkbBytes => self.wkb_max_bytes(true),
                    TierBound::Positions => self.coordinates,
                    TierBound::Rings => self.rings,
                    TierBound::Bytes => self.bytes,
                    TierBound::WorkSteps => bounds::GEOMETRY_WORK_STEPS,
                    TierBound::AllocatedBytes => bounds::GEOMETRY_WORK_ALLOCATED_BYTES,
                }) as u64,
            }),
        }
    }

    /// Largest WKB of one value of the tier: 16 bytes per position (24 with
    /// heights), a count per ring, a header per member.
    pub fn wkb_max_bytes(&self, z: bool) -> usize {
        self.coordinates * (if z { 24 } else { 16 })
            + self.rings * 4
            + (self.ring_coordinates + 1) * (9 + if z { 24 } else { 16 })
    }
}

/// A decoded JSON-like value as the twins see it: a number is an integer
/// (Python `int`, JSON text without a fraction) or a float.
#[derive(Clone, Debug, PartialEq)]
pub enum Gv {
    Null,
    Bool(bool),
    Int64(i64),
    Float(f64),
    String(String),
    /// Anything no geometry member holds (bytes, a big integer).
    Other,
    Array(Vec<Gv>),
    Object(Vec<(String, Gv)>),
}

impl Gv {
    pub fn get(&self, key: &str) -> Option<&Gv> {
        match self {
            Gv::Object(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
}

impl From<&serde_json::Value> for Gv {
    fn from(value: &serde_json::Value) -> Self {
        use serde_json::Value;
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Number(value) => match value.as_i64() {
                Some(value) => Self::Int64(value),
                None => value.as_f64().map(Self::Float).unwrap_or(Self::Other),
            },
            Value::String(value) => Self::String(value.clone()),
            Value::Array(values) => Self::Array(values.iter().map(Self::from).collect()),
            Value::Object(values) => Self::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::from(value)))
                    .collect(),
            ),
        }
    }
}

/// A borrowed, validated input to geometry admission.
#[doc(hidden)]
pub trait ValueView<'a>: Copy {
    fn scalar(self) -> Scalar;
    fn text(self) -> Option<std::borrow::Cow<'a, str>>;
    fn array(self) -> Option<impl ExactSizeIterator<Item = Self>>;
    fn object(self) -> Option<impl Iterator<Item = (std::borrow::Cow<'a, str>, Self)>>;

    fn field(self, name: &str) -> Option<Self> {
        self.object()?.find(|(key, _)| key == name).map(|(_, value)| value)
    }

    fn number(self) -> Option<f64> {
        match self.scalar() {
            Scalar::Float(value) if value.is_finite() => Some(value),
            Scalar::Integer(value) => Some(value as f64),
            _ => None,
        }
    }

    fn is_empty_array(self) -> bool {
        self.array().is_some_and(|items| items.len() == 0)
    }
}

#[doc(hidden)]
pub enum Scalar {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Other,
}

impl<'a> ValueView<'a> for &'a Gv {
    fn scalar(self) -> Scalar {
        match *self {
            Gv::Null => Scalar::Null,
            Gv::Bool(value) => Scalar::Bool(value),
            Gv::Int64(value) => Scalar::Integer(value),
            Gv::Float(value) => Scalar::Float(value),
            _ => Scalar::Other,
        }
    }

    fn text(self) -> Option<std::borrow::Cow<'a, str>> {
        if let Gv::String(text) = self { Some(text.as_str().into()) } else { None }
    }

    fn array(self) -> Option<impl ExactSizeIterator<Item = Self>> {
        if let Gv::Array(items) = self { Some(items.iter()) } else { None }
    }

    fn object(self) -> Option<impl Iterator<Item = (std::borrow::Cow<'a, str>, Self)>> {
        if let Gv::Object(fields) = self {
            Some(fields.iter().map(|(name, value)| (name.as_str().into(), value)))
        } else {
            None
        }
    }
}

/// Admits a decoded value as a geometry of one of `types` under
/// `declaration`, the engine validator's check of a property declared
/// `geometry: { space, size }`. Bindings admit the same values; they
/// twins also return the canonical value (`-0` as `0`), which the engine
/// never returns: it stores the value as written, as it stores every
/// float64, and every reader that admits it canonicalises it. A native
/// series alone stores an archived geometry payload field canonically, since
/// its change log holds its WKB (`series_model::series::change_log`).
pub fn admit(
    value: &Gv,
    types: &[Kind],
    declaration: &GeometryDeclaration,
) -> Result<Geometry, GeometryRefusal> {
    admit_declared_with_work(value, types, declaration, &mut GeometryWork::default())
}

fn require_kind(g: Geometry, types: &[Kind]) -> Result<Geometry, GeometryRefusal> {
    if !types.contains(&g.kind) {
        return Err(invalid(format!(
            "expected {}",
            types
                .iter()
                .map(|t| t.name())
                .collect::<Vec<_>>()
                .join(" or ")
        )));
    }
    Ok(g)
}

/// A declared stored value's admission under one enclosing work allowance.
/// Preserves ellipsoid heights; projection into a spatial ground query is an
/// explicit operation, independent of this lossless storage codec.
pub fn admit_declared_with_work(
    value: &Gv,
    types: &[Kind],
    declaration: &GeometryDeclaration,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    let geometry = admit_value_with_work(value, declaration, work)?;
    require_kind(geometry, types)
}

/// Admit a value in its declared space and physical allowance.
pub fn admit_value(
    value: &Gv,
    declaration: &GeometryDeclaration,
) -> Result<Geometry, GeometryRefusal> {
    admit_value_with_work(value, declaration, &mut GeometryWork::default())
}

pub fn admit_value_with_work(
    value: &Gv,
    declaration: &GeometryDeclaration,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    admit_view_with_work(value, declaration, work)
}

#[doc(hidden)]
pub fn admit_view_with_work<'a>(
    value: impl ValueView<'a>,
    declaration: &GeometryDeclaration,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    admit_coordinates(value, &declaration.space, &declaration.limits(), work)
}

// ------------------------------------------------------------------ geometry

/// Python `min(a, b)`: `a` unless `b` is smaller.
pub fn pmin(a: f64, b: f64) -> f64 {
    if b < a {
        b
    } else {
        a
    }
}

/// Python `max(a, b)`: `a` unless `b` is larger.
pub fn pmax(a: f64, b: f64) -> f64 {
    if b > a {
        b
    } else {
        a
    }
}

pub type Pos = [f64; 2];

/// A GeoJSON geometry type, in the profile's canonical order.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum Kind {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
}

impl Kind {
    pub const ALL: [Self; 7] = [
        Self::Point,
        Self::MultiPoint,
        Self::LineString,
        Self::MultiLineString,
        Self::Polygon,
        Self::MultiPolygon,
        Self::GeometryCollection,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "Point" => Kind::Point,
            "MultiPoint" => Kind::MultiPoint,
            "LineString" => Kind::LineString,
            "MultiLineString" => Kind::MultiLineString,
            "Polygon" => Kind::Polygon,
            "MultiPolygon" => Kind::MultiPolygon,
            "GeometryCollection" => Kind::GeometryCollection,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::Point => "Point",
            Kind::MultiPoint => "MultiPoint",
            Kind::LineString => "LineString",
            Kind::MultiLineString => "MultiLineString",
            Kind::Polygon => "Polygon",
            Kind::MultiPolygon => "MultiPolygon",
            Kind::GeometryCollection => "GeometryCollection",
        }
    }

    /// Nesting depth of a position (geometry.py `_DEPTH`).
    pub fn contents_key(self) -> &'static str {
        match self {
            Self::GeometryCollection => "geometries",
            Self::Point
            | Self::MultiPoint
            | Self::LineString
            | Self::MultiLineString
            | Self::Polygon
            | Self::MultiPolygon => "coordinates",
        }
    }

    pub fn depth(self) -> usize {
        match self {
            Kind::Point => 1,
            Kind::MultiPoint | Kind::LineString => 2,
            Kind::MultiLineString | Kind::Polygon => 3,
            Kind::MultiPolygon => 4,
            Kind::GeometryCollection => 0,
        }
    }

    pub fn wkb_code(self) -> i64 {
        match self {
            Kind::Point => 1,
            Kind::LineString => 2,
            Kind::Polygon => 3,
            Kind::MultiPoint => 4,
            Kind::MultiLineString => 5,
            Kind::MultiPolygon => 6,
            Kind::GeometryCollection => 7,
        }
    }

    pub fn from_wkb_code(code: i64) -> Option<Self> {
        Some(match code {
            1 => Kind::Point,
            2 => Kind::LineString,
            3 => Kind::Polygon,
            4 => Kind::MultiPoint,
            5 => Kind::MultiLineString,
            6 => Kind::MultiPolygon,
            7 => Kind::GeometryCollection,
            _ => return None,
        })
    }

    /// The member type of a multipart type (`kind[5:]`).
    pub fn member(self) -> Option<Self> {
        match self {
            Kind::MultiPoint => Some(Kind::Point),
            Kind::MultiLineString => Some(Kind::LineString),
            Kind::MultiPolygon => Some(Kind::Polygon),
            _ => None,
        }
    }
}

/// Geometry components preserve typed empties. MultiPoint members may be
/// empty; a LineString contains only positions. Heights enumerate nonempty
/// positions in coordinate order.
#[derive(Clone, Debug, PartialEq)]
pub enum Coords {
    /// A valid empty value, with its geometry kind retained by `Geometry`.
    Empty,
    Point(Pos),
    MultiPoint(Vec<Option<Pos>>),
    Points(Vec<Pos>),
    Lines(Vec<Vec<Pos>>),
    Polygons(Vec<Vec<Vec<Pos>>>),
    Collection(Vec<Geometry>),
}

/// An admitted (or, inside the preview, candidate) geometry; `heights` holds
/// the 3D profile's heights in coordinate order.
#[derive(Clone, Debug, PartialEq)]
pub struct Geometry {
    pub space: GeometrySpace,
    pub kind: Kind,
    pub coords: Coords,
    pub heights: Option<Vec<f64>>,
}

impl Geometry {
    pub fn is_empty(&self) -> bool {
        self.position_count() == 0
    }

    pub fn new(kind: Kind, coords: Coords, space: GeometrySpace) -> Self {
        Self {
            space,
            kind,
            coords,
            heights: None,
        }
    }

    pub fn for_each(&self, mut f: impl FnMut(&Pos)) {
        self.for_each_with(&mut f);
    }

    fn for_each_with(&self, f: &mut dyn FnMut(&Pos)) {
        match &self.coords {
            Coords::Collection(members) => {
                for member in members {
                    member.for_each_with(f);
                }
            },
            Coords::Empty => {},
            Coords::Point(p) => f(p),
            Coords::MultiPoint(ps) => ps.iter().flatten().for_each(f),
            Coords::Points(ps) => ps.iter().for_each(f),
            Coords::Lines(ls) => ls.iter().flatten().for_each(f),
            Coords::Polygons(ps) => ps.iter().flatten().flatten().for_each(f),
        }
    }

    pub fn component_count(&self) -> usize {
        1 + match &self.coords {
            Coords::Collection(members) => members.iter().map(Self::component_count).sum::<usize>(),
            _ => 0,
        }
    }

    pub fn position_count(&self) -> usize {
        let mut n = 0;
        self.for_each(|_| n += 1);
        n
    }

    pub fn ring_count(&self) -> usize {
        match (&self.coords, self.kind) {
            (Coords::Lines(rings), Kind::Polygon) => rings.len(),
            (Coords::Polygons(ps), _) => ps.iter().map(Vec::len).sum(),
            (Coords::Collection(members), _) => members.iter().map(Self::ring_count).sum(),
            _ => 0,
        }
    }

    pub fn lines(&self) -> Vec<&Vec<Pos>> {
        match &self.coords {
            Coords::Collection(members) => members.iter().flat_map(Self::lines).collect(),
            Coords::Points(ps) if self.kind == Kind::LineString => vec![ps],
            Coords::Lines(ls) if self.kind == Kind::MultiLineString => ls.iter().collect(),
            _ => vec![],
        }
    }

    pub fn polygons(&self) -> Vec<&Vec<Vec<Pos>>> {
        match &self.coords {
            Coords::Lines(rings) if self.kind == Kind::Polygon => vec![rings],
            Coords::Polygons(ps) => ps.iter().collect(),
            Coords::Collection(members) => members.iter().flat_map(Self::polygons).collect(),
            _ => vec![],
        }
    }

    /// Map a complete value once, preserving collection members and heights.
    pub fn map_value<T>(
        &self,
        position: impl Fn(&Pos, Option<f64>) -> T,
        array: impl Fn(Vec<T>) -> T,
        object: impl Fn(Kind, T) -> T,
    ) -> T {
        self.map_value_with(&position, &array, &object)
    }

    fn map_value_with<T>(
        &self,
        position: &dyn Fn(&Pos, Option<f64>) -> T,
        array: &dyn Fn(Vec<T>) -> T,
        object: &dyn Fn(Kind, T) -> T,
    ) -> T {
        let mut next = 0;
        let mut pos = |p: &Pos| {
            let h = self.heights.as_ref().map(|h| h[next]);
            next += 1;
            position(p, h)
        };
        let contents = match &self.coords {
            Coords::Empty => array(vec![]),
            Coords::Point(p) => pos(p),
            Coords::MultiPoint(ps) => array(
                ps.iter()
                    .map(|p| p.as_ref().map_or_else(|| array(vec![]), &mut pos))
                    .collect(),
            ),
            Coords::Points(ps) => array(ps.iter().map(&mut pos).collect()),
            Coords::Lines(ls) => array(
                ls.iter()
                    .map(|l| array(l.iter().map(&mut pos).collect()))
                    .collect(),
            ),
            Coords::Polygons(ps) => array(
                ps.iter()
                    .map(|rs| {
                        array(
                            rs.iter()
                                .map(|r| array(r.iter().map(&mut pos).collect()))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
            Coords::Collection(members) => array(
                members
                    .iter()
                    .map(|member| member.map_value_with(position, array, object))
                    .collect(),
            ),
        };
        object(self.kind, contents)
    }

    /// Explicit height removal preserves the represented set. A vertical line
    /// collapses to a Point; mixed collapsed multipart results remain geometry.
    pub fn ground_projection(mut self, work: &mut GeometryWork) -> Result<Self, GeometryRefusal> {
        work.charge(self.position_count().saturating_add(1))?;
        self.space.dimensions = Dimensions::Xy;
        self.heights = None;
        self.coords = match self.coords {
            Coords::Points(mut points) if self.kind == Kind::LineString => {
                points.dedup();
                if points.len() == 1 {
                    self.kind = Kind::Point;
                    Coords::Point(points[0])
                } else {
                    Coords::Points(points)
                }
            },
            Coords::Lines(mut lines) if self.kind == Kind::MultiLineString => {
                for line in &mut lines {
                    line.dedup();
                }
                if lines.iter().any(|line| line.len() == 1) {
                    work.allocate(lines.len().saturating_mul(
                        std::mem::size_of::<Geometry>() + self.space.retained_bytes(),
                    ))?;
                    let members = lines
                        .into_iter()
                        .map(|line| {
                            if line.len() == 1 {
                                Self::new(Kind::Point, Coords::Point(line[0]), self.space.clone())
                            } else {
                                Self::new(
                                    Kind::LineString,
                                    Coords::Points(line),
                                    self.space.clone(),
                                )
                            }
                        })
                        .collect();
                    self.kind = Kind::GeometryCollection;
                    Coords::Collection(members)
                } else {
                    Coords::Lines(lines)
                }
            },
            Coords::Collection(members) => Coords::Collection(
                members
                    .into_iter()
                    .map(|member| member.ground_projection(work))
                    .collect::<Result<_, _>>()?,
            ),
            coordinates => coordinates,
        };
        Ok(self)
    }

    pub fn retained_bytes(&self) -> usize {
        let positions = |ps: &Vec<Pos>| ps.capacity().saturating_mul(std::mem::size_of::<Pos>());
        let lines = |ls: &Vec<Vec<Pos>>| {
            ls.capacity() * std::mem::size_of::<Vec<Pos>>()
                + ls.iter().map(positions).sum::<usize>()
        };
        self.space.retained_bytes()
            + std::mem::size_of::<Self>()
            + self.heights.as_ref().map_or(0, |h| h.capacity() * 8)
            + match &self.coords {
                Coords::Empty | Coords::Point(_) => 0,
                Coords::MultiPoint(ps) => ps
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Option<Pos>>()),
                Coords::Points(ps) => positions(ps),
                Coords::Lines(ls) => lines(ls),
                Coords::Polygons(ps) => {
                    ps.capacity() * std::mem::size_of::<Vec<Vec<Pos>>>()
                        + ps.iter().map(lines).sum::<usize>()
                },
                Coords::Collection(members) => {
                    members.capacity() * std::mem::size_of::<Self>()
                        + members.iter().map(Self::retained_bytes).sum::<usize>()
                },
            }
    }

    pub fn js_json(&self) -> String {
        self.map_value(
            |p, h| {
                let mut s = format!("[{},{}", json::number(p[0]), json::number(p[1]));
                if let Some(h) = h {
                    s.push(',');
                    s.push_str(&json::number(h));
                }
                s.push(']');
                s
            },
            |items| format!("[{}]", items.join(",")),
            |kind, contents| {
                format!(
                    "{{\"type\":\"{}\",\"{}\":{contents}}}",
                    kind.name(),
                    kind.contents_key()
                )
            },
        )
    }
}

impl From<&Geometry> for Gv {
    fn from(value: &Geometry) -> Self {
        value.map_value(
            |point, height| Gv::Array(point.iter().copied().chain(height).map(Gv::Float).collect()),
            Gv::Array,
            geojson,
        )
    }
}

pub fn geojson(kind: Kind, coordinates: Gv) -> Gv {
    Gv::Object(vec![
        ("type".into(), Gv::String(kind.name().into())),
        (kind.contents_key().into(), coordinates),
    ])
}

/// The canonical JSON text of a decoded value: bytes are not a geometry
/// member.
pub fn js_json(value: &Gv, out: &mut String) -> Result<(), GeometryRefusal> {
    match value {
        Gv::Null => out.push_str("null"),
        Gv::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Gv::Int64(i) if i.unsigned_abs() >= 1 << 53 => out.push_str(&json::number(*i as f64)),
        Gv::Int64(i) => {
            let _ = write!(out, "{i}");
        },
        Gv::Float(f) if f.is_finite() => out.push_str(&json::number(*f)),
        Gv::Float(_) => out.push_str("null"),
        Gv::String(s) => json::string(s, out),
        Gv::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                js_json(item, out)?;
            }
            out.push(']');
        },
        Gv::Object(fields) => {
            out.push('{');
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json::string(k, out);
                out.push(':');
                js_json(v, out)?;
            }
            out.push('}');
        },
        Gv::Other => return Err(invalid("expected geometry object")),
    }
    Ok(())
}

/// Count canonical JSON bytes without constructing a second geometry-sized
/// string. One bounded scalar scratch reservation covers number formatting.
fn json_length<'a>(
    value: impl ValueView<'a>,
    work: &mut GeometryWork,
    depth: usize,
) -> Result<usize, GeometryRefusal> {
    if depth > 64 {
        return Err(invalid("Geometry value nesting exceeds 64"));
    }
    work.charge(1)?;
    fn quoted(value: &str, work: &mut GeometryWork) -> Result<usize, GeometryRefusal> {
        work.charge(value.len())?;
        Ok(2 + value
            .chars()
            .map(|value| match value {
                '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
                value if (value as u32) < 0x20 => 6,
                value => value.len_utf8(),
            })
            .sum::<usize>())
    }
    Ok(match value.scalar() {
        Scalar::Null => 4,
        Scalar::Bool(value) => if value { 4 } else { 5 },
        Scalar::Integer(value) if value.unsigned_abs() >= 1 << 53 => json::number_len(value as f64),
        Scalar::Integer(value) => decimal_digits(value.unsigned_abs()) + usize::from(value < 0),
        Scalar::Float(value) if value.is_finite() => json::number_len(value),
        Scalar::Float(_) => 4,
        Scalar::Other => {
            if let Some(text) = value.text() {
                quoted(&text, work)?
            } else if let Some(values) = value.array() {
                let mut length = 2 + values.len().saturating_sub(1);
                for value in values {
                    length += json_length(value, work, depth + 1)?;
                }
                length
            } else if let Some(values) = value.object() {
                let mut length = 2;
                for (index, (key, value)) in values.enumerate() {
                    length += usize::from(index != 0) + quoted(&key, work)? + 1
                        + json_length(value, work, depth + 1)?;
                }
                length
            } else {
                return Err(invalid("expected geometry object"));
            }
        },
    })
}

fn decimal_digits(value: u64) -> usize {
    value.checked_ilog10().unwrap_or(0) as usize + 1
}

#[cfg(test)]
#[test]
fn spatial_arrow_decimal_length_matches_rendered_json_across_binary64() {
    let check = |value| assert_eq!(json::number_len(value), json::number(value).len(), "{value:e}");
    for value in [-0.0, 0.0, 1e-7, 1e-6, -1e-6, 1e20, 1e21, -1e21, f64::MAX, f64::from_bits(1)] {
        check(value);
        let previous = f64::from_bits(value.to_bits().saturating_sub(1));
        if previous.is_finite() {
            check(previous);
        }
        let next = f64::from_bits(value.to_bits() + 1);
        if next.is_finite() {
            check(next);
        }
    }
    let mut bits = 0x5f43_9edc_4411_2a31u64;
    for _ in 0..8192 {
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        let value = f64::from_bits(bits);
        if value.is_finite() {
            check(value);
        }
    }
}

// ------------------------------------------------------------------ admission

/// A finite number as Python admits it (`int` or `float`, never `bool`),
/// with whether it is an integer (an int64, or a zero the profile
/// normalizes to the integer 0).
pub fn number(x: &Gv) -> Option<f64> {
    match x {
        Gv::Float(f) if f.is_finite() => Some(*f),
        Gv::Int64(i) => Some(*i as f64),
        _ => None,
    }
}

fn array<'a, V: ValueView<'a>>(
    v: Option<V>,
    limits: &Limits,
) -> Result<impl ExactSizeIterator<Item = V>, GeometryRefusal> {
    let items = v.and_then(ValueView::array).ok_or_else(|| invalid("array bound"))?;
    if items.len() > limits.ring_coordinates {
        return Err(limits.over(
            format!(
                "array bound{}",
                limits.detail(|| format!(
                    "{} elements in one array, at most {}",
                    items.len(),
                    limits.ring_coordinates
                ))
            ),
            TierBound::Array,
        ));
    }
    Ok(items)
}

/// Optional enclosing native read allowance. Portable evaluation has none.
pub trait WorkObserver: Send + Sync {
    fn charge(&self, steps: usize) -> Result<(), GeometryRefusal>;
    fn allocate(&self, bytes: usize) -> Result<(), GeometryRefusal>;
    fn release(&self, bytes: usize);
}

/// One admission's work and owned-allocation account; not host RSS.
pub struct GeometryWork {
    steps: usize,
    allocated_bytes: usize,
    peak_allocated_bytes: usize,
    step_limit: usize,
    allocation_limit: usize,
    observer: Option<std::sync::Arc<dyn WorkObserver>>,
    observed_bytes: usize,
}

impl Default for GeometryWork {
    fn default() -> Self {
        Self::with_limits(
            TierBound::WorkSteps.limit() as usize,
            TierBound::AllocatedBytes.limit() as usize,
        )
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct GeometryWorkUsage {
    pub steps: usize,
    pub peak_allocated_bytes: usize,
}

impl GeometryWork {
    pub fn with_limits(steps: usize, allocated_bytes: usize) -> Self {
        Self {
            observer: None,
            observed_bytes: 0,
            steps: 0,
            allocated_bytes: 0,
            peak_allocated_bytes: 0,
            step_limit: steps.min(TierBound::WorkSteps.limit() as usize),
            allocation_limit: allocated_bytes.min(TierBound::AllocatedBytes.limit() as usize),
        }
    }

    pub fn observe(&mut self, observer: std::sync::Arc<dyn WorkObserver>) {
        self.observer = Some(observer);
    }

    /// Capacity available to optional planning work before doing that work.
    pub fn remaining_steps(&self) -> usize {
        self.step_limit.saturating_sub(self.steps)
    }

    pub fn usage(&self) -> GeometryWorkUsage {
        GeometryWorkUsage {
            steps: self.steps,
            peak_allocated_bytes: self.peak_allocated_bytes,
        }
    }

    /// Release temporary reservations on success and refusal without resetting
    /// the work already spent by the enclosing read.
    pub fn scoped<T, E>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let allocated = self.allocated_bytes;
        let observed = self.observed_bytes;
        let result = operation(self);
        if let Some(observer) = &self.observer {
            observer.release(self.observed_bytes.saturating_sub(observed));
        }
        self.allocated_bytes = allocated;
        self.observed_bytes = observed;
        result
    }

    fn take(
        used: &mut usize,
        amount: usize,
        bound: TierBound,
        limit: usize,
    ) -> Result<(), GeometryRefusal> {
        if amount > limit.saturating_sub(*used) {
            return Err(GeometryRefusal {
                code: GeometryRefusalCode::SpatialWorkLimit,
                message: format!("Geometry operation exceeds {} ({limit})", bound.name()),
                bound: Some(GeometryBound {
                    kind: bound,
                    limit: limit as u64,
                }),
            });
        }
        *used += amount;
        Ok(())
    }

    pub fn charge(&mut self, steps: usize) -> Result<(), GeometryRefusal> {
        if steps > self.remaining_steps() {
            return Self::take(
                &mut self.steps,
                steps,
                TierBound::WorkSteps,
                self.step_limit,
            );
        }
        if let Some(observer) = &self.observer {
            observer.charge(steps)?;
        }
        Self::take(
            &mut self.steps,
            steps,
            TierBound::WorkSteps,
            self.step_limit,
        )
    }

    pub fn allocate(&mut self, bytes: usize) -> Result<(), GeometryRefusal> {
        Self::take(
            &mut self.allocated_bytes,
            bytes,
            TierBound::AllocatedBytes,
            self.allocation_limit,
        )?;
        if let Some(observer) = &self.observer
            && let Err(error) = observer.allocate(bytes)
        {
            self.allocated_bytes -= bytes;
            return Err(error);
        }
        self.observed_bytes += bytes;
        self.peak_allocated_bytes = self.peak_allocated_bytes.max(self.allocated_bytes);
        Ok(())
    }

    /// Native host allocations already retained under their own lifetime lease.
    pub fn allocate_retained(&mut self, bytes: usize) -> Result<(), GeometryRefusal> {
        Self::take(
            &mut self.allocated_bytes,
            bytes,
            TierBound::AllocatedBytes,
            self.allocation_limit,
        )?;
        self.peak_allocated_bytes = self.peak_allocated_bytes.max(self.allocated_bytes);
        Ok(())
    }

    pub fn release(&mut self, bytes: usize) {
        assert!(
            bytes <= self.allocated_bytes,
            "Invalid geometry allocation release"
        );
        self.allocated_bytes -= bytes;
        let observed = bytes.min(self.observed_bytes);
        self.observed_bytes -= observed;
        if let Some(observer) = &self.observer {
            observer.release(observed);
        }
    }
}

struct Admission<'l, 'w> {
    limits: &'l Limits,
    space: &'l GeometrySpace,
    work: &'w mut GeometryWork,
    count: usize,
    rings: usize,
    heights: Option<Vec<f64>>,
}

impl Admission<'_, '_> {
    /// One position, -0 normalized to 0, and whether both coordinates are
    /// Python integers.
    fn point<'a, V: ValueView<'a>>(&mut self, v: Option<V>) -> Result<Pos, GeometryRefusal> {
        self.work.charge(1)?;
        self.count += 1;
        if self.count > self.limits.coordinates {
            let limits = self.limits;
            return Err(limits.over(
                format!(
                    "coordinate bound{}",
                    limits.detail(|| format!("more than {} positions", limits.coordinates))
                ),
                TierBound::Positions,
            ));
        }
        let refused = || {
            invalid(match (&self.space.frame, &self.space.dimensions) {
                (CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. }, Dimensions::Xy) => {
                    "expected WGS84 longitude/latitude"
                },
                (CoordinateFrame::Wgs84 | CoordinateFrame::Wgs84Sphere { .. }, Dimensions::Xyz { .. }) => {
                    "expected WGS84 longitude/latitude and height"
                },
                (CoordinateFrame::Cartesian { .. }, Dimensions::Xy) => {
                    "expected finite XY coordinates"
                },
                (CoordinateFrame::Cartesian { .. }, Dimensions::Xyz { .. }) => {
                    "expected finite XYZ coordinates"
                },
            })
        };
        let mut items = v.and_then(ValueView::array).ok_or_else(refused)?;
        if items.len() != if self.heights.is_some() { 3 } else { 2 } {
            return Err(refused());
        }
        let mut next = || items.next().and_then(ValueView::number).ok_or_else(refused);
        let (x, y) = (next()?, next()?);
        if self.space.frame.is_geographic() && (x.abs() > 180.0 || y.abs() > 90.0) {
            return Err(refused());
        }
        if let Some(heights) = &mut self.heights {
            self.work.allocate(16)?;
            let height = next()?;
            if (self.space.frame == CoordinateFrame::Wgs84
                && height.abs()
                    * match &self.space.dimensions {
                        Dimensions::Xyz { unit, .. } => unit.metres_per_unit.get(),
                        Dimensions::Xy => 1.0,
                    }
                    > HEIGHT_LIMIT_METRES)
                || height.abs() > f64::MAX.sqrt().sqrt() / 16.0
            {
                return Err(invalid("height bound"));
            }
            heights.push(if height == 0.0 { 0.0 } else { height });
        }
        let coordinate_limit = f64::MAX.sqrt().sqrt() / 16.0;
        if x.abs() > coordinate_limit || y.abs() > coordinate_limit {
            return Err(invalid("Coordinate exceeds the binary64 topology domain"));
        }
        Ok([
            if x == 0.0 { 0.0 } else { x },
            if y == 0.0 { 0.0 } else { y },
        ])
    }

    fn line<'a, V: ValueView<'a>>(&mut self, v: Option<V>, ring: bool) -> Result<Vec<Pos>, GeometryRefusal> {
        let items = array(v, self.limits)?;
        self.work.allocate(items.len().saturating_mul(32))?;
        let height_start = self.heights.as_ref().map_or(0, Vec::len);
        let mut ps = Vec::with_capacity(items.len());
        for item in items {
            ps.push(self.point(Some(item))?);
        }
        if !ring && ps.is_empty() {
            return Ok(ps);
        }
        if ps.len() < if ring { 4 } else { 2 } {
            return Err(invalid("short line/ring"));
        }
        for i in 1..ps.len() {
            if ps[i - 1] == ps[i]
                && (ring
                    || self.heights.as_ref().is_none_or(|heights| {
                        heights[height_start + i - 1] == heights[height_start + i]
                    }))
            {
                return Err(invalid("duplicate adjacent coordinate"));
            }
            if topology::antipodal_edge(&self.space.frame, ps[i - 1], ps[i], self.work)? {
                return Err(invalid(
                    "An edge joins antipodal vertices, which fix no unique arc",
                ));
            }
        }
        if ring {
            self.rings += 1;
            let over = self.rings > self.limits.rings;
            if over {
                let limits = self.limits;
                return Err(limits.over(
                    format!(
                        "ring bound or unclosed ring{}",
                        limits.detail(|| format!("more than {} rings", limits.rings))
                    ),
                    TierBound::Rings,
                ));
            }
            if ps[0] != ps[ps.len() - 1]
                || self.heights.as_ref().is_some_and(|heights| {
                    heights[height_start] != heights[height_start + ps.len() - 1]
                })
            {
                return Err(invalid("ring bound or unclosed ring"));
            }
        }
        Ok(ps)
    }

    /// A closed ring of great elliptic arcs (or segments), simple.
    fn ring<'a>(&mut self, v: impl ValueView<'a>) -> Result<(Vec<Pos>, arcs::Ring), GeometryRefusal> {
        let ps = self.line(Some(v), true)?;
        let ring = self.work.scoped(|work| {
            work.charge(ps.len())?;
            arcs::Ring::admit(&self.space.frame, &ps, work)
        })?;
        let ring = ring.map_err(invalid)?;
        self.work
            .allocate(ring.vertices().len().saturating_mul(std::mem::size_of::<arcs::Vector>()))?;
        Ok((ps, ring))
    }

    fn polygon<'a, V: ValueView<'a>>(&mut self, v: Option<V>) -> Result<(Vec<Vec<Pos>>, Vec<arcs::Ring>), GeometryRefusal> {
        let items = array(v, self.limits)?;
        let mut positions = Vec::with_capacity(items.len());
        let mut rings = Vec::with_capacity(items.len());
        for item in items {
            let (ps, ring) = self.ring(item)?;
            positions.push(ps);
            rings.push(ring);
        }
        let geographic = self.space.frame.is_geographic();
        if let Some(defect) = topology::polygon_defect(geographic, &rings, self.work)? {
            return Err(invalid(defect));
        }
        Ok((positions, rings))
    }
}

/// Canonical geometry admission in a declared frame and physical allowance.
/// Language bindings use this kernel; nothing is repaired.
pub fn admit_geometry(
    value: &Gv,
    space: &GeometrySpace,
    limits: &Limits,
) -> Result<Geometry, GeometryRefusal> {
    admit_coordinates(value, space, limits, &mut GeometryWork::default())
}

fn admit_coordinates<'a>(
    value: impl ValueView<'a>,
    space: &GeometrySpace,
    limits: &Limits,
    work: &mut GeometryWork,
) -> Result<Geometry, GeometryRefusal> {
    admit_coordinates_nested(value, space, limits, work, 0)
}

fn admit_coordinates_nested<'a>(
    value: impl ValueView<'a>,
    space: &GeometrySpace,
    limits: &Limits,
    work: &mut GeometryWork,
    depth: usize,
) -> Result<Geometry, GeometryRefusal> {
    if depth > 16 {
        return Err(limits.over(
            "geometry collection nesting exceeds 16".into(),
            TierBound::Depth,
        ));
    }
    space.validate_kernel()?;
    work.charge(1)?;
    work.allocate(space.retained_bytes())?;
    let mut members = value.object().ok_or_else(|| invalid("expected geometry object"))?;
    let kind = value.field("type").and_then(ValueView::text)
        .and_then(|name| Kind::parse(&name))
        .ok_or_else(|| invalid("unsupported geometry type"))?;
    if members.any(|(key, _)| key != "type" && key != kind.contents_key())    {
        return Err(invalid("unsupported member"));
    }
    let size = work.scoped(|work| {
        work.allocate(256)?;
        json_length(value, work, 0)
    })?;
    if size > limits.bytes {
        return Err(limits.over(
            format!(
                "byte bound{}",
                limits.detail(|| format!("{size} bytes, at most {}", limits.bytes))
            ),
            TierBound::Bytes,
        ));
    }
    if kind == Kind::GeometryCollection {
        let members = array(value.field("geometries"), limits)?;
        work.allocate(
            members
                .len()
                .saturating_mul(std::mem::size_of::<Geometry>()),
        )?;
        let mut geometries = Vec::with_capacity(members.len());
        let mut components = 1usize;
        let mut positions = 0usize;
        let mut rings = 0usize;
        for member in members {
            let geometry = work
                .scoped(|work| admit_coordinates_nested(member, space, limits, work, depth + 1))?;
            components = components.saturating_add(geometry.component_count());
            if components > limits.ring_coordinates {
                return Err(limits.over("collection component bound".into(), TierBound::Components));
            }
            positions = positions.saturating_add(geometry.position_count());
            rings = rings.saturating_add(geometry.ring_count());
            if positions > limits.coordinates {
                return Err(limits.over("position bound".into(), TierBound::Positions));
            }
            if rings > limits.rings {
                return Err(limits.over("ring bound".into(), TierBound::Rings));
            }
            work.allocate(geometry.retained_bytes())?;
            geometries.push(geometry);
        }
        return Ok(Geometry {
            space: space.clone(),
            kind,
            coords: Coords::Collection(geometries),
            heights: (!space.is_ground()).then(Vec::new),
        });
    }
    let mut admission = Admission {
        limits,
        space,
        work,
        count: 0,
        rings: 0,
        heights: (!space.is_ground()).then(Vec::new),
    };
    let coordinates = value.field("coordinates");
    let coords = match kind {
        Kind::GeometryCollection => unreachable!("collection admitted above"),
        Kind::Point if coordinates.is_some_and(ValueView::is_empty_array) => {
            Coords::Empty
        },
        Kind::Point => Coords::Point(admission.point(coordinates)?),
        Kind::MultiPoint => {
            let items = array(coordinates, limits)?;
            admission.work.allocate(
                items
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<Pos>>()),
            )?;
            let ps = items
                .map(|x| {
                    admission.work.charge(1)?;
                    if x.is_empty_array() { Ok(None) } else { admission.point(Some(x)).map(Some) }
                })
                .collect::<Result<Vec<_>, GeometryRefusal>>()?;
            Coords::MultiPoint(ps)
        },
        Kind::LineString => {
            Coords::Points(admission.line(coordinates, false)?)
        },
        Kind::MultiLineString => {
            let items = array(coordinates, limits)?;
            let ls = items
                .map(|x| admission.line(Some(x), false))
                .collect::<Result<Vec<_>, GeometryRefusal>>()?;
            Coords::Lines(ls)
        },
        Kind::Polygon => Coords::Lines(admission.polygon(coordinates)?.0),
        Kind::MultiPolygon => {
            let items = array(coordinates, limits)?;
            let (ps, rings): (Vec<_>, Vec<_>) = items
                .map(|x| admission.polygon(Some(x)))
                .collect::<Result<Vec<_>, GeometryRefusal>>()?
                .into_iter()
                .unzip();
            let geographic = space.frame.is_geographic();
            // Parts may touch at isolated points, never overlap or share an
            // edge, except the two sides of a cut at the antimeridian (+180
            // in one part, -180 in the other).
            let parts: Vec<&[arcs::Ring]> = rings.iter().map(Vec::as_slice).collect();
            if parts.len() > 1 && topology::parts_overlap(geographic, &parts, admission.work)? {
                return Err(invalid("multipart polygons must be disjoint"));
            }
            Coords::Polygons(ps)
        },
    };
    let mut geometry = Geometry::new(kind, coords, space.clone());
    geometry.heights = admission.heights;
    Ok(geometry)
}

// ------------------------------------------------------------------ topology

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rel {
    Inside,
    Outside,
    Boundary,
}

pub fn edge_box(e: &(Pos, Pos)) -> [f64; 4] {
    let (a, b) = e;
    [
        pmin(a[0], b[0]) - TOLERANCE,
        pmin(a[1], b[1]) - TOLERANCE,
        pmax(a[0], b[0]) + TOLERANCE,
        pmax(a[1], b[1]) + TOLERANCE,
    ]
}

pub fn boxes_meet(x: &[f64; 4], y: &[f64; 4]) -> bool {
    x[0] <= y[2] && y[0] <= x[2] && x[1] <= y[3] && y[1] <= x[3]
}

pub fn py_min_all(values: impl Iterator<Item = f64>) -> f64 {
    values.reduce(pmin).expect("a non-empty sequence")
}

pub fn py_max_all(values: impl Iterator<Item = f64>) -> f64 {
    values.reduce(pmax).expect("a non-empty sequence")
}

/// Refine each conservative unordered edge pair once through the packed tree.
pub fn meeting_pairs(
    edges: &[(Pos, Pos)],
    visit: &mut dyn FnMut(usize, usize, &mut GeometryWork) -> Result<bool, GeometryRefusal>,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    let before = work.allocated_bytes;
    let outcome = (|| {
        work.allocate(edges.len().saturating_mul(std::mem::size_of::<[f64; 4]>()))?;
        let boxes: Vec<[f64; 4]> = edges.iter().map(edge_box).collect();
        let tree = spatial_tree::PackedBoxes::new(&boxes, work)?;
        tree.pairs(&mut |_, _| true, visit, work)
    })();
    work.release(work.allocated_bytes - before);
    outcome
}

pub fn edges_of(line: &[Pos]) -> Vec<(Pos, Pos)> {
    line.windows(2).map(|w| (w[0], w[1])).collect()
}

/// The point at parameter t of segment a-b (a and b exactly at 0 and 1).
pub fn point_along(a: Pos, b: Pos, t: f64) -> Pos {
    if t == 0.0 {
        a
    } else if t == 1.0 {
        b
    } else {
        [a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])]
    }
}

pub fn ring_edges(rings: &[Vec<Pos>]) -> Vec<(Pos, Pos)> {
    rings.iter().flat_map(|r| edges_of(r)).collect()
}

// ------------------------------------------------------------------ numerics

pub(crate) fn square(x: f64) -> f64 {
    x * x
}

/// The canonical JSON encoding: compact separators, members in their stored
/// order, UTF-8 text, and numbers in ECMAScript's shortest round-trip form.
pub mod json {
    use std::fmt::Write as _;

    use serde_json::Value;

    /// Shortest round-trip decimal digits of a positive finite number and
    /// the position of its decimal point.
    fn shortest(x: f64, digits: &mut [u8; 17]) -> (&str, i32) {
        let mut text = std::io::Cursor::new([0; 32]);
        std::io::Write::write_fmt(&mut text, format_args!("{x:e}")).expect("fits binary64");
        let length = text.position() as usize;
        let text = std::str::from_utf8(&text.get_ref()[..length]).expect("ASCII decimal");
        let (mantissa, exponent) = text.split_once('e').expect("exponent form");
        let mut count = 0;
        for digit in mantissa.bytes().filter(|&byte| byte != b'.') {
            digits[count] = digit;
            count += 1;
        }
        while count > 1 && digits[count - 1] == b'0' {
            count -= 1;
        }
        let digits = std::str::from_utf8(&digits[..count]).expect("ASCII decimal digits");
        (digits, exponent.parse::<i32>().expect("exponent") + 1)
    }

    pub(crate) fn number_len(x: f64) -> usize {
        if x == 0.0 {
            return 1;
        }
        let mut scratch = [0; 17];
        let (digits, point) = shortest(x.abs(), &mut scratch);
        let count = digits.len();
        let body = match point {
            1..=21 => (point as usize).max(count + usize::from(count > point as usize)),
            -5..=0 => 2 + (-point) as usize + count,
            _ => count + usize::from(count > 1) + 2
                + super::decimal_digits((point - 1).unsigned_abs() as u64),
        };
        usize::from(x < 0.0) + body
    }

    /// ECMAScript `Number::toString` of a finite double; zero of either sign
    /// is `0`.
    pub fn number(x: f64) -> String {
        if x == 0.0 {
            return "0".into();
        }
        let mut scratch = [0; 17];
        let (digits, point) = shortest(x.abs(), &mut scratch);
        let k = digits.len() as i32;
        let sign = if x < 0.0 { "-" } else { "" };
        if k <= point && point <= 21 {
            return format!("{sign}{digits}{}", "0".repeat((point - k) as usize));
        }
        if 0 < point && point <= 21 {
            let (whole, fraction) = digits.split_at(point as usize);
            return format!("{sign}{whole}.{fraction}");
        }
        if -6 < point && point <= 0 {
            return format!("{sign}0.{}{digits}", "0".repeat((-point) as usize));
        }
        let e = point - 1;
        let fraction = if k > 1 {
            format!(".{}", &digits[1..])
        } else {
            String::new()
        };
        format!(
            "{sign}{}{fraction}e{}{}",
            &digits[..1],
            if e >= 0 { '+' } else { '-' },
            e.abs()
        )
    }

    /// A JSON string: `"`, `\` and controls escaped, other text as UTF-8.
    pub fn string(s: &str, out: &mut String) {
        out.push('"');
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                c if (c as u32) < 0x20 => {
                    let _ = write!(out, "\\u{:04x}", c as u32);
                },
                c => out.push(c),
            }
        }
        out.push('"');
    }

    /// The canonical text of a JSON value.
    pub fn write(value: &Value) -> String {
        fn write(value: &Value, out: &mut String) {
            match value {
                Value::Null => out.push_str("null"),
                Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
                Value::Number(n) => match n.as_f64() {
                    Some(f) if n.is_f64() => out.push_str(&number(f)),
                    _ => out.push_str(&n.to_string()),
                },
                Value::String(s) => string(s, out),
                Value::Array(items) => {
                    out.push('[');
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        write(item, out);
                    }
                    out.push(']');
                },
                Value::Object(members) => {
                    out.push('{');
                    for (i, (k, v)) in members.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        string(k, out);
                        out.push(':');
                        write(v, out);
                    }
                    out.push('}');
                },
            }
        }
        let mut out = String::new();
        write(value, &mut out);
        out
    }
}

/// One point estimate. Null accuracy is unknown, independent of index tiers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Position {
    pub point: Pos,
    pub accuracy_m: Option<f64>,
}

pub fn admit_position(
    value: &Gv,
    declaration: &GeometryDeclaration,
) -> Result<Position, GeometryRefusal> {
    admit_position_with_work(value, declaration, &mut GeometryWork::default())
}

pub fn admit_position_with_work(
    value: &Gv,
    declaration: &GeometryDeclaration,
    work: &mut GeometryWork,
) -> Result<Position, GeometryRefusal> {
    admit_position_view_with_work(value, declaration, work)
}

#[doc(hidden)]
pub fn admit_position_view_with_work<'a>(
    value: impl ValueView<'a>,
    declaration: &GeometryDeclaration,
    work: &mut GeometryWork,
) -> Result<Position, GeometryRefusal> {
    if !declaration.space.is_ground() {
        return Err(invalid("Positions require XY coordinates"));
    }
    let object = value.object().ok_or_else(|| invalid("A position is an object"))?;
    if object.take(3).count() != 2 {
        return Err(invalid("A position has point and accuracy_m"));
    }
    let accuracy_m = match value.field("accuracy_m") {
        Some(radius) if matches!(radius.scalar(), Scalar::Null) => None,
        Some(radius) if radius.number().is_some_and(|radius| radius >= 0.0) => radius.number(),
        _ => {
            return Err(invalid(
                "Accuracy must be finite non-negative metres or explicit null",
            ))
        },
    };
    let point = value
        .field("point")
        .ok_or_else(|| invalid("A position requires a point"))?;
    let geometry = require_kind(admit_view_with_work(point, declaration, work)?, &[Kind::Point])?;
    let Coords::Point(point) = geometry.coords else {
        return Err(invalid("A position requires a Point"));
    };
    Ok(Position { point, accuracy_m })
}

mod spatial_key;
pub use spatial_key::KeyRange;
pub mod spatial_index;
