//! Bounded reference spatial execution in a declared coordinate space.
//! Index covers accelerate these answers and never replace their predicates.
pub(crate) mod collection;
pub mod prepared;
pub use crate::topology::relation::Predicate as TopologyPredicate;

/// Interior and boundary predicates share the exact arc relation owner.
pub fn topology(
    a: &Geometry,
    b: &Geometry,
    operation: TopologyPredicate,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    crate::topology::relation::evaluate(a, b, operation, work)
}
use segment_distance::{
    LOWER_SCALE,
    R,
};

use crate::{
    Coords,
    Geometry,
    GeometryRefusal,
    GeometryWork,
    Kind,
    Pos,
    Rel,
};
/// The smallest WGS84 radius of curvature, b²/a: the meridian's at the
/// equator. A radian of arc in any direction spans at least this many metres.
pub const MIN_CURVATURE_RADIUS_METRES: f64 = crate::geodesy::SEMI_MINOR_AXIS_METRES
    * crate::geodesy::SEMI_MINOR_AXIS_METRES
    / crate::geodesy::SEMI_MAJOR_AXIS_METRES;
/// The largest WGS84 radius of curvature, a²/b, at the poles.
pub const MAX_CURVATURE_RADIUS_METRES: f64 = crate::geodesy::SEMI_MAJOR_AXIS_METRES
    * crate::geodesy::SEMI_MAJOR_AXIS_METRES
    / crate::geodesy::SEMI_MINOR_AXIS_METRES;
/// The longest WGS84 geodesic: half a meridian, between antipodes.
pub const MAX_POINT_DISTANCE_METRES: f64 = 20_003_931.458_625_447;

/// A query region. Unlike a stored polygon it can span every longitude,
/// cross the seam, or have zero area without inventing invalid GeoJSON.
#[derive(Clone, Copy)]
pub struct BoxRegion {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl BoxRegion {
    /// Longitude spans split at the antimeridian for shared index covers.
    pub fn parts(self) -> Vec<(f64, f64)> {
        if self.west > self.east {
            vec![(self.west, 180.0), (-180.0, self.east)]
        } else {
            vec![(self.west, self.east)]
        }
    }

    /// The box as arcs. Parallels are not great elliptic arcs, so each side
    /// along one is densified to arcs of at most half a degree of longitude,
    /// which stay within about 30 m of it. A band around every longitude is
    /// its southern parallel's shell less its northern parallel's hole.
    pub fn geometry(self) -> Geometry {
        let space = crate::GeometrySpace::wgs84_xy;
        let parallel = |latitude: f64, from: f64, to: f64| -> Vec<Pos> {
            let steps = (((to - from).abs() / 0.5).ceil() as usize).max(1);
            (0..=steps)
                .map(|i| {
                    let longitude = if i == steps {
                        to
                    } else {
                        from + (to - from) * i as f64 / steps as f64
                    };
                    [longitude, latitude]
                })
                .collect()
        };
        let parts = self.parts();
        if self.south == self.north {
            if self.west == self.east {
                return Geometry::new(
                    Kind::Point,
                    Coords::Point([self.west, self.south]),
                    space(),
                );
            }
            let lines = parts
                .into_iter()
                .map(|(west, east)| parallel(self.south, west, east))
                .collect();
            return Geometry::new(Kind::MultiLineString, Coords::Lines(lines), space());
        }
        if self.west == self.east {
            return Geometry::new(
                Kind::LineString,
                Coords::Points(vec![[self.west, self.south], [self.east, self.north]]),
                space(),
            );
        }
        let closed = |mut ring: Vec<Pos>| {
            ring.push(ring[0]);
            ring
        };
        let polygons = if self.west == -180.0 && self.east == 180.0 {
            let shell = |latitude: f64| closed(parallel(latitude, -180.0, 179.5));
            let hole = |latitude: f64| closed(parallel(latitude, 180.0, -179.5));
            match (self.south > -90.0, self.north < 90.0) {
                (true, true) => vec![vec![shell(self.south), hole(self.north)]],
                (true, false) => vec![vec![shell(self.south)]],
                (false, true) => vec![vec![hole(self.north)]],
                // Every direction: the two hemispheres, sharing the equator.
                (false, false) => vec![vec![shell(0.0)], vec![hole(0.0)]],
            }
        } else {
            parts
                .into_iter()
                .map(|(west, east)| {
                    let mut ring = parallel(self.south, west, east);
                    ring.extend(parallel(self.north, east, west));
                    vec![closed(ring)]
                })
                .collect()
        };
        Geometry::new(Kind::MultiPolygon, Coords::Polygons(polygons), space())
    }
}

pub fn classify_box(
    point: Pos,
    accuracy_m: Option<f64>,
    area: BoxRegion,
    work: &mut GeometryWork,
) -> Result<&'static str, GeometryRefusal> {
    work.charge(1)?;
    work.allocate(512)?;
    let Some(accuracy) = accuracy_m else {
        return Ok("unknown");
    };
    if !accuracy.is_finite() || accuracy < 0.0 {
        return Err(crate::invalid(
            "Accuracy must be finite non-negative metres",
        ));
    }
    let longitude_inside = |longitude| {
        if area.west > area.east {
            longitude >= area.west || longitude <= area.east
        } else {
            longitude >= area.west && longitude <= area.east
        }
    };
    let inside = point[1] >= area.south
        && point[1] <= area.north
        && (longitude_inside(point[0])
            || point[1].abs() == 90.0
            || (point[0].abs() == 180.0 && longitude_inside(-point[0])));
    if accuracy == 0.0 {
        return Ok(if inside { "inside" } else { "outside" });
    }
    // A box's sides along parallels are coordinate ranges, not arcs: the
    // nearest point of one is on the point's own meridian when the range
    // holds it, and an end otherwise. Its sides along meridians are arcs,
    // halved so no half joins the poles.
    let mut distance = f64::INFINITY;
    let mut sides = 0;
    for (west, east) in area.parts() {
        for latitude in [area.south, area.north] {
            if latitude.abs() == 90.0 {
                continue;
            }
            sides += 1;
            work.charge(4)?;
            let within = [point[0], point[0] - 360.0, point[0] + 360.0]
                .into_iter()
                .any(|longitude| west <= longitude && longitude <= east);
            distance = distance.min(if within {
                point_distance(point, [point[0], latitude])
            } else {
                point_distance(point, [west, latitude]).min(point_distance(point, [east, latitude]))
            });
        }
    }
    if area.west != -180.0 || area.east != 180.0 {
        let middle = (area.south + area.north) / 2.0;
        for longitude in [area.west, area.east] {
            for (from, to) in [(area.south, middle), (middle, area.north)] {
                sides += 1;
                distance = segment_distance(
                    (point, point),
                    ([longitude, from], [longitude, to]),
                    distance,
                    work,
                )?;
            }
        }
    }
    if sides == 0 {
        return Ok("inside");
    }
    Ok(if within_slack(distance, accuracy) {
        "uncertain"
    } else if inside {
        "inside"
    } else {
        "outside"
    })
}

fn paths(geometry: &Geometry) -> Vec<&[Pos]> {
    match &geometry.coords {
        Coords::Collection(members) => members.iter().flat_map(paths).collect(),
        Coords::Empty => vec![],
        Coords::Point(point) => vec![std::slice::from_ref(point)],
        Coords::MultiPoint(points) => points.iter().flatten().map(std::slice::from_ref).collect(),
        Coords::Points(points) => vec![points],
        Coords::Lines(lines) => lines.iter().map(Vec::as_slice).collect(),
        Coords::Polygons(polygons) => polygons.iter().flatten().map(Vec::as_slice).collect(),
    }
}
fn vertices(geometry: &Geometry) -> Vec<Pos> {
    let mut points = Vec::with_capacity(geometry.position_count());
    geometry.for_each(|point| points.push(*point));
    points
}
fn segments(geometry: &Geometry) -> Vec<(Pos, Pos)> {
    if let Coords::Collection(members) = &geometry.coords {
        return members.iter().flat_map(segments).collect();
    }
    if matches!(geometry.kind, Kind::Point | Kind::MultiPoint) {
        return vertices(geometry).into_iter().map(|p| (p, p)).collect();
    }
    paths(geometry)
        .iter()
        .flat_map(|path| path.windows(2).map(|pair| (pair[0], pair[1])))
        .collect()
}
fn is_area(geometry: &Geometry) -> bool {
    match &geometry.coords {
        Coords::Collection(members) => members.iter().any(is_area),
        _ => matches!(geometry.kind, Kind::Polygon | Kind::MultiPolygon),
    }
}
fn point_relation(
    point: Pos,
    geometry: &Geometry,
    work: &mut GeometryWork,
) -> Result<Rel, GeometryRefusal> {
    crate::topology::relation::PreparedTopology::new(geometry, work)?.relation(point, work)
}
pub fn intersects(
    a: &Geometry,
    b: &Geometry,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    topology(a, b, TopologyPredicate::Intersects, work)
}

/// Boundary-inclusive coverage uses the same exact arrangement as interiors.
pub fn covered_by(
    a: &Geometry,
    b: &Geometry,
    work: &mut GeometryWork,
) -> Result<bool, GeometryRefusal> {
    topology(a, b, TopologyPredicate::CoveredBy, work)
}

/// Geodesic distance on the WGS84 ellipsoid between two ground-profile
/// points, in metres.
pub fn point_distance(a: Pos, b: Pos) -> f64 {
    if a == b || a[1] == b[1] && (a[1].abs() == 90.0 || (a[0] - b[0]).abs() == 360.0) {
        return 0.0;
    }
    crate::geodesy::inverse(a[1], a[0], b[1], b[0]).s12
}
const SLACK_METRES: f64 = 1e-3;
const SLACK_RATIO: f64 = 1e-7;

/// The profile's distance tolerance: one millimetre plus 1e-7 of distance.
pub fn distance_slack(distance: f64) -> f64 {
    SLACK_METRES + SLACK_RATIO * distance
}

/// Whether a computed boundary distance reaches `limit` metres under the
/// profile's tolerance. [`distance_reach`] is its inverse.
fn within_slack(distance: f64, limit: f64) -> bool {
    distance - distance_slack(distance) <= limit
}

/// The farthest distance [`within_slack`] accepts for `limit`:
/// `d - (a + r * d) <= limit` exactly when `d <= (limit + a) / (1 - r)`.
pub fn distance_reach(limit: f64) -> f64 {
    (limit + SLACK_METRES) / (1.0 - SLACK_RATIO)
}

/// An indexable predicate as a candidate cover sees it. A cover keeps every
/// row the predicate can accept; refinement still evaluates the predicate.
#[derive(Clone, Copy, Debug)]
pub enum CoverPredicate {
    /// `intersects`, `covered_by` or `covers` against the region.
    Touches,
    /// `classify(position, region)` is "inside" or "uncertain", for a geometry
    /// or box region.
    Classify,
    /// A confidently inside position must have its reported centre inside.
    ClassifyInside,
    ClassifyInsideCircle {
        radius_m: f64,
    },
    /// `classify_circle(position, ..)` is "inside" or "uncertain".
    ClassifyCircle {
        radius_m: f64,
    },
    /// `distance(row, region)` is at most `distance_m`.
    Within {
        distance_m: f64,
    },
}

impl CoverPredicate {
    /// Metres beyond the region's bounds that hold every row this predicate
    /// accepts when the row's accuracy is at most `accuracy_m` (zero for a
    /// shape). Non-decreasing in `accuracy_m`. Rows accepted within
    /// [`crate::TOLERANCE`] degrees are held by the cover's larger degree pad,
    /// not by these metres.
    pub fn reach(self, accuracy_m: f64) -> f64 {
        match self {
            // Membership and touching are decided within TOLERANCE degrees.
            Self::Touches => 0.0,
            // `classify` and `classify_box` report "uncertain" through
            // `within_slack(boundary, accuracy)`, and a position outside the
            // region is no farther from its bounds than from its boundary.
            Self::Classify => distance_reach(accuracy_m),
            Self::ClassifyInside => 0.0,
            Self::ClassifyInsideCircle { radius_m } => radius_m,
            // `classify_circle` is "outside" once the centre distance exceeds
            // radius plus accuracy.
            Self::ClassifyCircle { radius_m } => radius_m + accuracy_m,
            // `distance` is attained by points of both operands, so it never
            // reports under their separation; the slack absorbs rounding.
            Self::Within { distance_m } => distance_m + distance_slack(distance_m),
        }
    }
}

pub(crate) mod segment_distance;
use segment_distance::segment_distance;

pub fn distance(
    a: &Geometry,
    b: &Geometry,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    if matches!((&a.coords, &b.coords), (Coords::Point(_), Coords::Point(_))) {
        work.charge(1)?;
        return geometry_point_distance(a, b);
    }
    if intersects(a, b, work)? {
        return Ok(0.0);
    }
    let right = segments(b);
    let mut best = f64::INFINITY;
    for left in segments(a) {
        for &right in &right {
            work.charge(1)?;
            best = space_segment_distance(&a.space, left, right, best, work)?;
        }
    }
    Ok(best)
}

/// Classify a known radius independently of the physical index's classes.
pub fn classify(
    point: Pos,
    accuracy_m: Option<f64>,
    area: &Geometry,
    work: &mut GeometryWork,
) -> Result<&'static str, GeometryRefusal> {
    area.space.validate_kernel()?;
    if !area.space.is_ground() {
        return Err(crate::invalid(
            "Classification requires explicit ground projection",
        ));
    }
    let Some(accuracy) = accuracy_m else {
        return Ok("unknown");
    };
    if !accuracy.is_finite() || accuracy < 0.0 {
        return Err(crate::invalid(
            "Accuracy must be finite non-negative metres",
        ));
    }
    if accuracy > 0.0
        && is_area(area)
        && let Coords::Collection(members) = &area.coords
    {
        return collection::classify(point, accuracy, area, members, work);
    }
    let relation = point_relation(point, area, work)?;
    if accuracy == 0.0 {
        return Ok(if relation == Rel::Outside {
            "outside"
        } else {
            "inside"
        });
    }
    let mut boundary = f64::INFINITY;
    for edge in segments(area) {
        boundary = space_segment_distance(&area.space, (point, point), edge, boundary, work)?;
    }
    if within_slack(boundary, accuracy) {
        return Ok("uncertain");
    }
    Ok(if relation == Rel::Outside {
        "outside"
    } else {
        "inside"
    })
}

/// A spherical circle is a query region, independent of storage geometry.
pub fn classify_circle(
    point: Pos,
    accuracy_m: Option<f64>,
    center: Pos,
    radius_m: f64,
    work: &mut GeometryWork,
) -> Result<&'static str, GeometryRefusal> {
    work.charge(1)?;
    if !radius_m.is_finite() || radius_m < 0.0 {
        return Err(crate::invalid("Radius must be finite non-negative metres"));
    }
    let Some(accuracy) = accuracy_m else {
        return Ok("unknown");
    };
    if !accuracy.is_finite() || accuracy < 0.0 {
        return Err(crate::invalid(
            "Accuracy must be finite non-negative metres",
        ));
    }
    if radius_m >= MAX_POINT_DISTANCE_METRES {
        return Ok("inside");
    }
    let distance = point_distance(point, center);
    Ok(if distance + accuracy <= radius_m {
        "inside"
    } else if distance - accuracy > radius_m {
        "outside"
    } else {
        "uncertain"
    })
}

/// Distance in a declared coordinate space. Topology remains coordinate-linear;
/// this metric never changes the meaning of an XYZ value into a ground value.
pub fn space_point_distance(
    space: &crate::GeometrySpace,
    a: Pos,
    b: Pos,
) -> Result<f64, GeometryRefusal> {
    space.validate_kernel()?;
    if !space.is_ground() {
        return Err(crate::invalid(
            "Point coordinates require their declared dimensions",
        ));
    }
    let distance = match &space.frame {
        crate::CoordinateFrame::Wgs84 => point_distance(a, b),
        crate::CoordinateFrame::Cartesian { unit, .. } => {
            libm::hypot(a[0] - b[0], a[1] - b[1]) * unit.metres_per_unit.get()
        },
    };
    if distance.is_finite() {
        Ok(distance)
    } else {
        Err(crate::invalid("Distance exceeds the numerical domain"))
    }
}
/// Conservative distance between all points in two coordinate rectangles.
/// Spherical chord balls and latitude gaps both hold lon/lat-linear pieces;
/// the ellipsoid's smallest curvature radius scales them to WGS84.
fn distance_lower_bound(space: &crate::GeometrySpace, a: [f64; 4], b: [f64; 4]) -> f64 {
    let horizontal = 0.0_f64.max(a[0] - b[2]).max(b[0] - a[2]);
    let vertical = 0.0_f64.max(a[1] - b[3]).max(b[1] - a[3]);
    match &space.frame {
        crate::CoordinateFrame::Cartesian { unit, .. } => {
            libm::hypot(horizontal, vertical) * unit.metres_per_unit.get()
        },
        crate::CoordinateFrame::Wgs84 => {
            let ball = |bounds: [f64; 4]| {
                let center = [(bounds[0] + bounds[2]) / 2.0, (bounds[1] + bounds[3]) / 2.0];
                let near = if bounds[1] * bounds[3] <= 0.0 {
                    0.0
                } else {
                    bounds[1].abs().min(bounds[3].abs()).to_radians()
                };
                let radius = libm::hypot(
                    ((bounds[3] - bounds[1]) / 2.0).to_radians(),
                    ((bounds[2] - bounds[0]) / 2.0).to_radians() * libm::cos(near),
                );
                let (longitude, latitude) = (center[0].to_radians(), center[1].to_radians());
                (
                    [
                        libm::cos(latitude) * libm::cos(longitude),
                        libm::cos(latitude) * libm::sin(longitude),
                        libm::sin(latitude),
                    ],
                    radius,
                )
            };
            let (a, ar) = ball(a);
            let (b, br) = ball(b);
            let chord = libm::hypot(libm::hypot(a[0] - b[0], a[1] - b[1]), a[2] - b[2]);
            (LOWER_SCALE * R * vertical.to_radians().max(chord - ar - br) - R * 1e-12).max(0.0)
        },
    }
}

/// Complete distance between kernel pieces: great elliptic arcs in WGS84,
/// straight lines in a Cartesian frame. WGS84 accepts longitudes one turn past
/// the seam, as trajectory interpolation produces them.
pub fn segments_distance(
    space: &crate::GeometrySpace,
    left: [Pos; 2],
    right: [Pos; 2],
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    space.validate_kernel()?;
    if !space.is_ground() {
        return Err(crate::invalid("Segment distance requires XY coordinates"));
    }
    work.charge(4)?;
    work.allocate(std::mem::size_of::<[Pos; 4]>())?;
    for point in left.iter().chain(&right) {
        if point.iter().any(|coordinate| !coordinate.is_finite()) {
            return Err(crate::invalid("Segment coordinates must be finite"));
        }
        if space.frame == crate::CoordinateFrame::Wgs84
            && (point[0].abs() > 540.0 || point[1].abs() > 90.0)
        {
            return Err(crate::invalid(
                "Segment coordinates exceed adjacent WGS84 turns",
            ));
        }
    }
    // Great elliptic arcs have no seam: longitudes a turn apart are the same
    // direction.
    let best = space_segment_distance(
        space,
        (left[0], left[1]),
        (right[0], right[1]),
        f64::INFINITY,
        work,
    )?;
    if !best.is_finite() {
        return Err(crate::invalid(
            "Segment distance exceeds the numerical domain",
        ));
    }
    Ok(best)
}

pub(crate) fn space_segment_distance(
    space: &crate::GeometrySpace,
    a: (Pos, Pos),
    b: (Pos, Pos),
    best: f64,
    work: &mut GeometryWork,
) -> Result<f64, GeometryRefusal> {
    if space.frame == crate::CoordinateFrame::Wgs84 {
        return segment_distance(a, b, best, work);
    }
    if crate::topology::contact(&space.frame, a, b, work)? {
        return Ok(0.0);
    }
    let point_segment = |p: Pos, edge: (Pos, Pos)| -> Result<f64, GeometryRefusal> {
        let d = [edge.1[0] - edge.0[0], edge.1[1] - edge.0[1]];
        let length = libm::hypot(d[0], d[1]);
        if length == 0.0 {
            return space_point_distance(space, p, edge.0);
        }
        let unit = [d[0] / length, d[1] / length];
        let t = ((p[0] - edge.0[0]) * unit[0] + (p[1] - edge.0[1]) * unit[1]).clamp(0.0, length);
        space_point_distance(space, p, [edge.0[0] + t * unit[0], edge.0[1] + t * unit[1]])
    };
    work.charge(4)?;
    let mut distance = best;
    for (point, edge) in [(a.0, b), (a.1, b), (b.0, a), (b.1, a)] {
        distance = distance.min(point_segment(point, edge)?);
    }
    Ok(distance)
}

/// Point distance, including explicitly scaled Cartesian height ordinates.
/// It never interprets XYZ surfaces as volumes or discards the third ordinate.
pub fn geometry_point_distance(left: &Geometry, right: &Geometry) -> Result<f64, GeometryRefusal> {
    left.space.validate_kernel()?;
    if left.space != right.space {
        return Err(crate::invalid(
            "Spatial operands require the same coordinate space",
        ));
    }
    let (Coords::Point(a), Coords::Point(b)) = (&left.coords, &right.coords) else {
        return Err(crate::invalid("Point distance requires Point operands"));
    };
    if left.space.is_ground() {
        return space_point_distance(&left.space, *a, *b);
    }
    let (
        crate::CoordinateFrame::Cartesian { unit, .. },
        crate::Dimensions::Xyz {
            unit: vertical_unit,
            ..
        },
    ) = (&left.space.frame, &left.space.dimensions)
    else {
        return Err(crate::invalid(
            "XYZ distance requires a Cartesian coordinate frame",
        ));
    };
    let z = |geometry: &Geometry| {
        geometry
            .heights
            .as_ref()
            .filter(|heights| heights.len() == 1)
            .map(|heights| heights[0])
            .ok_or_else(|| crate::invalid("XYZ Point requires its third ordinate"))
    };
    let distance = libm::hypot(
        libm::hypot(a[0] - b[0], a[1] - b[1]) * unit.metres_per_unit.get(),
        (z(left)? - z(right)?) * vertical_unit.metres_per_unit.get(),
    );
    if distance.is_finite() {
        Ok(distance)
    } else {
        Err(crate::invalid("Distance exceeds the numerical domain"))
    }
}
