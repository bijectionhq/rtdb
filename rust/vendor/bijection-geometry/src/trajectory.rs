//! The native trajectory's coordinate model and parameter-space intersections.
//! Time stays with the series owner; a geometric crossing is a binary64
//! fraction between exact temporal anchors, never a floating-point epoch.
use crate::{
    predicates::prepared::PreparedGeometry,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

pub const PROFILE: &str = "wgs84-2d-great-elliptic-v1";

mod simplify;
pub use simplify::{
    simplify,
    Simplification,
};

pub fn validate_position(point: Pos) -> Result<(), GeometryRefusal> {
    if point[0].is_finite()
        && point[1].is_finite()
        && (-180.0..=180.0).contains(&point[0])
        && (-90.0..=90.0).contains(&point[1])
    {
        Ok(())
    } else {
        Err(crate::invalid(
            "Trajectory position requires WGS84 ground coordinates",
        ))
    }
}

/// Consecutive positions move along the great elliptic arc between them,
/// which antipodal positions do not fix.
pub fn validate_motion(a: Pos, b: Pos) -> Result<(), GeometryRefusal> {
    crate::measure::arc_frame(a, b).map(|_| ())
}

/// Signed displacement along the shorter longitude arc.
pub fn longitude_delta(a: Pos, b: Pos) -> f64 {
    let delta = b[0] - a[0];
    if delta > 180.0 {
        delta - 360.0
    } else if delta < -180.0 {
        delta + 360.0
    } else {
        delta
    }
}

/// Admitted endpoints and a parameter in [0, 1]: the position that fraction of
/// the way along the great elliptic arc between them, by auxiliary-sphere
/// angle. Exact endpoints retain their reported seam convention.
pub fn position_at(a: Pos, b: Pos, fraction: f64) -> Pos {
    crate::predicates::segment_distance::arc_position(a, b, fraction)
}

/// The path profile of geodesic interpolation.
pub const GEODESIC_PROFILE: &str = "wgs84-2d-geodesic-v1";

/// How a trajectory moves between consecutive fixes. `LinearShortest` is the
/// native default: the great elliptic arc, parameterized by auxiliary-sphere
/// angle, which every region, encounter and index kernel follows.
/// `Geodesic` follows the WGS84 ellipsoidal geodesic at constant speed, so a
/// fraction of the time is that fraction of the geodesic length; it is an
/// explicit presentation model whose paths only geodesic-aware consumers
/// accept.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Interpolation {
    #[default]
    LinearShortest,
    Geodesic,
}

impl Interpolation {
    /// The path profile a trajectory answer under this model declares.
    pub fn profile(self) -> &'static str {
        match self {
            Self::LinearShortest => PROFILE,
            Self::Geodesic => GEODESIC_PROFILE,
        }
    }

    pub fn of_profile(profile: &str) -> Option<Self> {
        [Self::LinearShortest, Self::Geodesic]
            .into_iter()
            .find(|interpolation| interpolation.profile() == profile)
    }

    /// The position `fraction` of the way from `a` to `b` under this model.
    /// Exact endpoints are returned as reported. A geodesic refuses antipodal
    /// endpoints, which fix no unique path; [`validate_motion`] decides arcs.
    pub fn position_at(self, a: Pos, b: Pos, fraction: f64) -> Result<Pos, GeometryRefusal> {
        match self {
            Self::LinearShortest => {
                if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
                    return Err(crate::invalid("Trajectory fraction must be in [0, 1]"));
                }
                Ok(position_at(a, b, fraction))
            },
            Self::Geodesic => {
                crate::predicates::segment_distance::geodesic_position(a, b, fraction)
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Interval {
    pub from: f64,
    pub to: f64,
    pub start: Pos,
    pub end: Pos,
}

/// Maximal closed parameter intervals where a great elliptic arc trajectory
/// meets an admitted region; fractions are angles along that arc. A tangent
/// is a zero-duration interval. Stationary
/// paths can occupy a region for the full interval. Seam aliases name the same
/// trajectory position, including paths lying on the meridian itself.
pub fn intervals(
    area: &PreparedGeometry,
    a: Pos,
    b: Pos,
    work: &mut GeometryWork,
) -> Result<Vec<Interval>, GeometryRefusal> {
    area.trajectory_intervals(a, b, work)
}

#[cfg(test)]
mod tests {
    use super::{
        intervals,
        position_at,
    };
    use crate::{
        predicates::prepared::PreparedGeometry,
        Coords,
        Geometry,
        GeometryRefusal,
        GeometryWork,
        Kind,
        Pos,
    };

    fn rectangle(west: f64, south: f64, east: f64, north: f64) -> Vec<Pos> {
        vec![
            [west, south],
            [east, south],
            [east, north],
            [west, north],
            [west, south],
        ]
    }
    fn selected(geometry: Geometry, a: Pos, b: Pos) -> Result<Vec<(f64, f64)>, GeometryRefusal> {
        let mut work = GeometryWork::default();
        let area = PreparedGeometry::new(
            geometry,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        Ok(intervals(&area, a, b, &mut work)?
            .into_iter()
            .map(|part| (part.from, part.to))
            .collect())
    }

    #[test]
    fn geodesic_interpolation_moves_at_constant_geodesic_speed() -> Result<(), GeometryRefusal> {
        use super::Interpolation;
        let distance = |a: Pos, b: Pos| crate::geodesy::inverse(a[1], a[0], b[1], b[0]).s12;
        for (a, b) in [
            ([-60.0, 60.0], [60.0, 60.0]),
            ([179.0, -10.0], [-179.0, 20.0]),
            ([2.35, 48.85], [-73.98, 40.75]),
        ] {
            let total = distance(a, b);
            for fraction in [0.25, 0.5, 0.9] {
                let p = Interpolation::Geodesic.position_at(a, b, fraction)?;
                assert!((distance(a, p) - fraction * total).abs() < 1e-6 * total.max(1.0));
                assert!((distance(p, b) - (1.0 - fraction) * total).abs() < 1e-6 * total.max(1.0));
                assert_eq!(
                    Interpolation::LinearShortest.position_at(a, b, fraction)?,
                    position_at(a, b, fraction)
                );
            }
            for model in [Interpolation::LinearShortest, Interpolation::Geodesic] {
                assert_eq!(model.position_at(a, b, 0.0)?, a);
                assert_eq!(model.position_at(a, b, 1.0)?, b);
                assert_eq!(Interpolation::of_profile(model.profile()), Some(model));
                assert!(model.position_at(a, b, 1.5).is_err());
            }
        }
        // Along a meridian both paths coincide, but the geodesic's halfway
        // time is its halfway distance, about 40.12 degrees north, while the
        // arc's halfway reduced latitude is about 40.09 degrees north.
        let (a, b) = ([0.0, 0.0], [0.0, 80.0]);
        let geodesic = Interpolation::Geodesic.position_at(a, b, 0.5)?;
        let arc = Interpolation::LinearShortest.position_at(a, b, 0.5)?;
        assert!((distance(a, geodesic) - distance(geodesic, b)).abs() < 1e-3);
        assert!(geodesic[1] > arc[1]);
        assert!(distance(geodesic, arc) > 1_000.0, "{geodesic:?} {arc:?}");
        assert!(Interpolation::Geodesic
            .position_at([0.0, 0.0], [180.0, 0.0], 0.5)
            .is_err());
        assert_eq!(Interpolation::of_profile("unknown"), None);
        assert_eq!(Interpolation::default(), Interpolation::LinearShortest);
        Ok(())
    }

    /// Fractions along an arc are angles on the auxiliary sphere, where
    /// reduced latitudes move a meridian's halfway point by about 1e-6.
    fn close(actual: Vec<(f64, f64)>, expected: &[(f64, f64)]) {
        assert_eq!(actual.len(), expected.len(), "{actual:?} {expected:?}");
        for (a, e) in actual.iter().zip(expected) {
            assert!(
                (a.0 - e.0).abs() < 1e-5 && (a.1 - e.1).abs() < 1e-5,
                "{actual:?} {expected:?}"
            );
        }
    }

    #[test]
    fn trajectory_region_clips_holes_contacts_and_stationary_paths() -> Result<(), GeometryRefusal>
    {
        let shell = rectangle(-1.0, -1.0, 1.0, 1.0);
        let area = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![shell.clone()]),
            crate::GeometrySpace::wgs84_xy(),
        );
        close(selected(area.clone(), [-2.0, 0.0], [2.0, 0.0])?, &[(0.25, 0.75)]);
        // The step's longer arc bulges north of the square's top arc.
        assert!(selected(area.clone(), [-2.0, 1.0], [2.0, 1.0])?.is_empty());
        assert_eq!(
            selected(area.clone(), [-2.0, 1.0], [-1.0, 1.0])?,
            vec![(1.0, 1.0)]
        );
        assert_eq!(
            selected(area.clone(), [0.0, 0.0], [0.0, 0.0])?,
            vec![(0.0, 1.0)]
        );
        assert!(selected(area, [2.0, 0.0], [2.0, 0.0])?.is_empty());
        // Holes run clockwise.
        let mut hole = rectangle(-0.5, -0.5, 0.5, 0.5);
        hole.reverse();
        let holed = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![shell, hole]),
            crate::GeometrySpace::wgs84_xy(),
        );
        close(
            selected(holed, [-2.0, 0.0], [2.0, 0.0])?,
            &[(0.25, 0.375), (0.625, 0.75)],
        );
        Ok(())
    }

    #[test]
    fn trajectory_region_keeps_thin_visits() -> Result<(), GeometryRefusal> {
        let area = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![rectangle(0.0, -1.0, 1e-10, 1.0)]),
            crate::GeometrySpace::wgs84_xy(),
        );
        let ranges = selected(area, [-80.0, 0.0], [80.0, 0.0])?;
        assert_eq!(ranges.len(), 1);
        assert!((ranges[0].0 - 0.5).abs() < 1e-12);
        assert!(ranges[0].1 > ranges[0].0);
        assert!(
            ranges[0].1 - ranges[0].0 < 1e-12,
            "a fixed parameter epsilon would discard this visit"
        );
        Ok(())
    }

    #[test]
    fn isolated_seam_contacts_keep_zero_duration() -> Result<(), GeometryRefusal> {
        for longitude in [-180., 180.] {
            let region = Geometry::new(
                Kind::Point,
                Coords::Point([longitude, 0.]),
                crate::GeometrySpace::wgs84_xy(),
            );
            let contacts = selected(region, [170., 0.], [-170., 0.])?;
            assert_eq!(contacts.len(), 1);
            assert_eq!(contacts[0].0, contacts[0].1);
            assert!((contacts[0].0 - 0.5).abs() < 1e-12);
        }
        Ok(())
    }

    #[test]
    fn trajectory_cells_preserve_exact_contacts_and_refuse_collapsed_boundaries(
    ) -> Result<(), GeometryRefusal> {
        let line = |y| {
            Geometry::new(
                Kind::LineString,
                Coords::Points(vec![[0., y], [1., y]]),
                crate::GeometrySpace::wgs84_xy(),
            )
        };
        // A subnormal latitude's direction rounds onto the equator itself.
        for y in [1e-300, -1e-300, 1e-12, -1e-12] {
            assert!(selected(line(y), [-1., 0.], [2., 0.])?.is_empty());
        }
        close(
            selected(line(0.), [-1., 0.], [2., 0.])?,
            &[(1. / 3., 2. / 3.)],
        );
        let point = Geometry::new(
            Kind::Point,
            Coords::Point([0.1, 0.]),
            crate::GeometrySpace::wgs84_xy(),
        );
        let mut work = GeometryWork::default();
        let point = PreparedGeometry::new(
            point,
            crate::predicates::prepared::Preparation::Topology,
            &mut work,
        )?;
        let contacts = intervals(&point, [-1., 0.], [2., 0.], &mut work)?;
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].from, contacts[0].to);
        assert_eq!(contacts[0].start, [0.1, 0.]);
        assert_eq!(contacts[0].end, [0.1, 0.]);
        // 1e-300 degrees of longitude is a distinct direction whose angle
        // from the step's start rounds to its neighbour's.
        let thin = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![rectangle(0., -1., 1e-300, 1.)]),
            crate::GeometrySpace::wgs84_xy(),
        );
        let refusal = selected(thin, [-1., 0.], [1., 0.]).unwrap_err();
        assert_eq!(
            refusal.code,
            crate::GeometryRefusalCode::SpatialPrecisionLoss,
            "{refusal:?}"
        );
        // Two components separated by a real gap cannot become one visit when
        // their exact cut parameters round to the same binary64 fraction.
        let gap = Geometry::new(
            Kind::MultiLineString,
            Coords::Lines(vec![
                vec![[-1., 0.], [0., 0.]],
                vec![[1e-300, 0.], [1., 0.]],
            ]),
            crate::GeometrySpace::wgs84_xy(),
        );
        assert_eq!(
            selected(gap, [-2., 0.], [2., 0.]).unwrap_err().code,
            crate::GeometryRefusalCode::SpatialPrecisionLoss
        );
        Ok(())
    }

    #[test]
    fn trajectory_region_splits_the_seam_and_unifies_its_aliases() -> Result<(), GeometryRefusal> {
        let area = Geometry::new(
            Kind::MultiPolygon,
            Coords::Polygons(vec![
                vec![rectangle(179.5, -1.0, 180.0, 1.0)],
                vec![rectangle(-180.0, -1.0, -179.5, 1.0)],
            ]),
            crate::GeometrySpace::wgs84_xy(),
        );
        for (a, b) in [([179.0, 0.0], [-179.0, 0.0]), ([-179.0, 0.0], [179.0, 0.0])] {
            close(selected(area.clone(), a, b)?, &[(0.25, 0.75)]);
        }
        let west = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![rectangle(-180.0, -1.0, -179.5, 1.0)]),
            crate::GeometrySpace::wgs84_xy(),
        );
        assert_eq!(
            selected(west.clone(), [180.0, 0.0], [180.0, 0.0])?,
            vec![(0.0, 1.0)]
        );
        close(
            selected(west.clone(), [180.0, 0.0], [-180.0, 2.0])?,
            &[(0.0, 0.5)],
        );
        close(
            selected(west.clone(), [180.0, 0.0], [-179.0, 0.0])?,
            &[(0.0, 0.5)],
        );
        assert_eq!(
            selected(west, [179.0, 0.0], [180.0, 0.0])?,
            vec![(1.0, 1.0)]
        );
        let greenwich = Geometry::new(
            Kind::Polygon,
            Coords::Lines(vec![rectangle(-1.0, -1.0, 1.0, 1.0)]),
            crate::GeometrySpace::wgs84_xy(),
        );
        assert!(selected(greenwich, [179.0, 0.0], [-179.0, 0.0])?.is_empty());
        // Motion follows the great elliptic arc: across the seam along the
        // equator, and over the pole between nearly opposite meridians.
        let seam = position_at([170.0, 0.0], [-170.0, 0.0], 0.5);
        assert!((seam[0].abs() - 180.0).abs() <= 1e-9 && seam[1].abs() <= 1e-9);
        let over = position_at([0.0, 0.0], [180.0, 2.0], 0.5);
        assert!(over[0] == 0.0 && over[1] > 89.0);
        Ok(())
    }
}
