//! Continuous separation metrics and their conservative spatial/time covers.
//! Both paths move along native great elliptic arcs. Distance
//! models remain explicit and independent of reported position accuracy.
use crate::{
    trajectory::longitude_delta,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

pub mod geodesic;

/// One declared separation model governs candidate dilation and refinement.
/// The geodesic model follows native great elliptic arc motion; the
/// equirectangular model is an explicit local approximation of it.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bindings", derive(ts_rs::TS))]
#[cfg_attr(feature = "bindings", ts(rename = "TrajectoryEncounterMetric"))]
#[serde(tag = "model", deny_unknown_fields)]
pub enum Metric {
    #[serde(rename = "wgs84-fixed-latitude-equirectangular-v1")]
    FixedLatitudeEquirectangular {
        reference_latitude: f64,
        within_m: f64,
    },
    #[serde(rename = "wgs84-linear-geodesic-v1")]
    LinearGeodesic { within_m: f64 },
}
impl Metric {
    pub fn validate(&self) -> Result<(), GeometryRefusal> {
        match self {
            Self::FixedLatitudeEquirectangular {
                reference_latitude,
                within_m,
            } => {
                metric_scales(*reference_latitude, *within_m)?;
                Ok(())
            },
            Self::LinearGeodesic { within_m } => geodesic::validate_radius(*within_m),
        }
    }

    /// Conservative dilation of a retained cell or path envelope. On a
    /// sphere, a radius limits latitude travel; the smallest cosine on that
    /// expanded band bounds longitude travel. Reaching a pole keeps all
    /// longitudes. Profile distance slack conservatively encloses numerical
    /// contacts.
    pub fn padding(&self, bounds: [f64; 4]) -> (f64, f64) {
        match self {
            Self::FixedLatitudeEquirectangular {
                reference_latitude,
                within_m,
            } => {
                let [east, north] =
                    metric_scales(*reference_latitude, 0.0).expect("validated encounter metric");
                (
                    if east == 0.0 {
                        360.0
                    } else {
                        (within_m / east).min(360.0)
                    },
                    (within_m / north).min(180.0),
                )
            },
            Self::LinearGeodesic { within_m } => {
                // A radian of latitude or of longitude on a parallel's
                // circle spans at least the smallest radius of curvature.
                let latitude = (crate::predicates::distance_reach(*within_m)
                    / (crate::predicates::segment_distance::LOWER_SCALE
                        * crate::predicates::segment_distance::R))
                    .to_degrees()
                    .min(180.0);
                let outer = bounds[1].abs().max(bounds[3].abs()) + latitude;
                (
                    if outer >= 90.0 {
                        360.0
                    } else {
                        (latitude / libm::cos(outer.to_radians())).min(360.0)
                    },
                    latitude,
                )
            },
        }
    }

    pub fn intervals(
        &self,
        left: [Pos; 2],
        right: [Pos; 2],
        work: &mut GeometryWork,
    ) -> Result<Vec<Encounter>, GeometryRefusal> {
        match self {
            Self::FixedLatitudeEquirectangular {
                reference_latitude,
                within_m,
            } => intervals(left, right, *reference_latitude, *within_m, work),
            Self::LinearGeodesic { within_m } => geodesic::intervals(left, right, *within_m, work),
        }
    }
}

pub const PROFILE: &str = "wgs84-fixed-latitude-equirectangular-v1";

#[derive(Clone, Debug, PartialEq)]
pub struct Encounter {
    pub from: f64,
    pub to: f64,
    pub closest: f64,
    pub distance_m: f64,
}

fn metric_scales(reference_latitude: f64, within_m: f64) -> Result<[f64; 2], GeometryRefusal> {
    if !reference_latitude.is_finite()
        || reference_latitude.abs() > 90.0
        || !within_m.is_finite()
        || within_m < 0.0
        || within_m > 100_000_000.0
    {
        return Err(crate::invalid(
            "Encounter metric needs a WGS84 reference latitude and radius 0..100000000 metres",
        ));
    }
    // Metres per degree on the ellipsoid at the reference latitude.
    let phi = reference_latitude.to_radians();
    let north = crate::geodesy::meridional_radius(phi).to_radians();
    let east = if reference_latitude.abs() == 90.0 {
        0.0
    } else {
        (crate::geodesy::prime_vertical_radius(phi) * libm::cos(phi)).to_radians()
    };
    Ok([east, north])
}

/// An already admitted path and its caller-owned grouping identity. Exact
/// microsecond ranges are kept independently of floating-point coordinates.
pub struct Motion {
    pub from: i64,
    pub to: i64,
    pub group: usize,
    pub points: [Pos; 2],
}

impl Motion {
    /// Coordinate admission precedes this temporal consistency check. An
    /// instant cannot describe motion between different locations, and
    /// antipodal fixes fix no unique arc.
    pub fn validate(&self) -> Result<(), GeometryRefusal> {
        if self.from > self.to {
            return Err(crate::invalid("Trajectory time anchors must be ordered"));
        }
        let [start, end] = self.points;
        let stationary =
            start[1] == end[1] && (start[0] == end[0] || (start[0] - end[0]).abs() == 360.0);
        if self.from == self.to && !stationary {
            return Err(crate::invalid("An instantaneous path has one position"));
        }
        crate::trajectory::validate_motion(start, end)
    }

    fn at(&self, time: i64) -> Pos {
        let fraction = if self.from == self.to {
            0.0
        } else {
            (i128::from(time) - i128::from(self.from)) as f64
                / (i128::from(self.to) - i128::from(self.from)) as f64
        };
        crate::trajectory::position_at(self.points[0], self.points[1], fraction)
    }
}

/// Continuous refinement over the common exact time anchors. Fractions refer
/// to these aligned paths; no absolute epoch is converted to binary64.
pub struct AlignedEncounters {
    pub from: i64,
    pub to: i64,
    pub left: [Pos; 2],
    pub right: [Pos; 2],
    pub intervals: Vec<Encounter>,
}

pub fn refine(
    left: &Motion,
    right: &Motion,
    metric: &Metric,
    work: &mut GeometryWork,
) -> Result<AlignedEncounters, GeometryRefusal> {
    work.charge(8)?;
    left.validate()?;
    right.validate()?;
    metric.validate()?;
    let from = left.from.max(right.from);
    let to = left.to.min(right.to);
    if from > to {
        return Ok(AlignedEncounters {
            from,
            to: from,
            left: left.points,
            right: right.points,
            intervals: vec![],
        });
    }
    let left = [left.at(from), left.at(to)];
    let right = [right.at(from), right.at(to)];
    let intervals = metric.intervals(left, right, work)?;
    Ok(AlignedEncounters {
        from,
        to,
        left,
        right,
        intervals,
    })
}

/// Conservative wrapped spatial covers for this exact trajectory profile.
fn path_boxes(points: [Pos; 2], longitude_padding: f64, latitude_padding: f64) -> Vec<[f64; 4]> {
    padded_boxes(
        crate::arcs::arc_bounds(points[0], points[1]),
        longitude_padding,
        latitude_padding,
    )
}

pub fn padded_boxes(
    bounds: [f64; 4],
    longitude_padding: f64,
    latitude_padding: f64,
) -> Vec<[f64; 4]> {
    let west = bounds[0] - longitude_padding - crate::TOLERANCE;
    let east = bounds[2] + longitude_padding + crate::TOLERANCE;
    let south = (bounds[1] - latitude_padding - crate::TOLERANCE).max(-90.0);
    let north = (bounds[3] + latitude_padding + crate::TOLERANCE).min(90.0);
    if east - west >= 360.0 {
        return vec![[-180.0, south, 180.0, north]];
    }
    let turns = ((west + 180.0) / 360.0).floor();
    let west = west - 360.0 * turns;
    let east = east - 360.0 * turns;
    if east > 180.0 {
        vec![
            [west, south, 180.0, north],
            [-180.0, south, east - 360.0, north],
        ]
    } else {
        vec![[west, south, east, north]]
    }
}

/// Build one native packed spatial index, annotate its nodes with exact time
/// hulls, and enumerate pairs for exact refinement. Covers can overselect; they
/// never establish an encounter. Exhaustion refuses the entire candidate set.
pub fn candidates(
    motions: &[Motion],
    metric: &Metric,
    maximum: usize,
    work: &mut GeometryWork,
) -> Result<Vec<(usize, usize)>, GeometryRefusal> {
    metric.validate()?;
    work.allocate(motions.len().saturating_mul(256))?;
    let possible = motions
        .len()
        .saturating_mul(motions.len().saturating_sub(1))
        / 2;
    work.allocate(possible.min(maximum.saturating_add(1)).saturating_mul(128))?;
    work.charge(
        motions
            .len()
            .saturating_mul(motions.len().max(2).ilog2() as usize + 1),
    )?;
    let mut order: Vec<_> = (0..motions.len()).collect();
    order.sort_by_key(|&index| (motions[index].from, index));
    let mut boxes = Vec::new();
    let mut owners = Vec::new();
    let mut times = Vec::new();
    for index in order {
        let motion = &motions[index];
        motion.validate()?;
        for bbox in path_boxes(motion.points, 0.0, 0.0) {
            boxes.push(bbox);
            owners.push(index);
            times.push([motion.from, motion.to]);
        }
    }
    let tree = crate::spatial_tree::PackedBoxes::new(&boxes, work)?;
    let hulls = tree.aggregates(&times, |a, b| [a[0].min(b[0]), a[1].max(b[1])], work)?;
    let mut selected = std::collections::BTreeSet::new();
    for (index, motion) in motions.iter().enumerate() {
        work.charge(8)?;
        let bounds = crate::arcs::arc_bounds(motion.points[0], motion.points[1]);
        let (longitude_padding, latitude_padding) =
            metric.padding([0.0, bounds[1], 0.0, bounds[3]]);
        for bbox in path_boxes(motion.points, longitude_padding, latitude_padding) {
            let exceeded = tree.search_filtered(
                &bbox,
                &mut |node| hulls[node][0] <= motion.to && motion.from <= hulls[node][1],
                &mut |leaf| {
                    let other = owners[leaf];
                    if other > index
                        && motions[other].group != motion.group
                        && motion.from <= motions[other].to
                        && motions[other].from <= motion.to
                    {
                        selected.insert((index, other));
                    }
                    selected.len() > maximum
                },
                work,
            )?;
            if exceeded {
                return Err(crate::invalid(format!(
                    "Encounter candidate work exceeds {maximum} motion pairs"
                )));
            }
        }
    }
    work.allocate(
        selected
            .len()
            .saturating_mul(2 * std::mem::size_of::<usize>()),
    )?;
    Ok(selected.into_iter().collect())
}

/// Maximal closed intervals within the radius. All input positions must be
/// admitted WGS84 points. The metric uses the shortest relative longitude at
/// each instant, including aliases at the seam. Tangencies are retained.
pub fn intervals(
    left: [Pos; 2],
    right: [Pos; 2],
    reference_latitude: f64,
    within_m: f64,
    work: &mut GeometryWork,
) -> Result<Vec<Encounter>, GeometryRefusal> {
    let [east, north] = metric_scales(reference_latitude, within_m)?;
    work.charge(128)?;
    work.allocate(1024)?;
    let initial = [right[0][0] - left[0][0], right[0][1] - left[0][1]];
    let velocity = [
        longitude_delta(right[0], right[1]) - longitude_delta(left[0], left[1]),
        (right[1][1] - right[0][1]) - (left[1][1] - left[0][1]),
    ];
    // The relative unwrapped longitude can change by up to 360 degrees.
    // Its shortest representative changes slope branch at these meridians.
    let mut cuts = vec![0.0, 1.0];
    if velocity[0] != 0.0 {
        for branch in -3..=3 {
            let at = (180.0 + 360.0 * f64::from(branch) - initial[0]) / velocity[0];
            if at > 0.0 && at < 1.0 {
                cuts.push(at);
            }
        }
    }
    cuts.sort_unstable_by(f64::total_cmp);
    cuts.dedup();
    let mut answer: Vec<Encounter> = Vec::new();
    for cut in cuts.windows(2) {
        let (from, to) = (cut[0], cut[1]);
        let middle = initial[0] + velocity[0] * ((from + to) / 2.0);
        let turns = ((middle + 180.0) / 360.0).floor();
        let a = [
            (initial[0] - 360.0 * turns + velocity[0] * from) * east,
            (initial[1] + velocity[1] * from) * north,
        ];
        let v = [velocity[0] * east, velocity[1] * north];
        let speed = libm::hypot(v[0], v[1]);
        let (start, end, closest, distance_m) = if speed == 0.0 {
            let distance = libm::hypot(a[0], a[1]);
            if distance > within_m {
                continue;
            }
            (from, to, from, distance)
        } else {
            let unit = [v[0] / speed, v[1] / speed];
            let along = -(a[0] * unit[0] + a[1] * unit[1]);
            let perpendicular = (a[0] * unit[1] - a[1] * unit[0]).abs();
            if perpendicular > within_m {
                continue;
            }
            // Product form avoids subtracting two nearly equal squares.
            let half_width = ((within_m - perpendicular) * (within_m + perpendicular)).sqrt();
            let start = ((along - half_width) / speed).max(0.0);
            let end = ((along + half_width) / speed).min(to - from);
            if start > end {
                continue;
            }
            let at = (along / speed).clamp(start, end);
            let distance = libm::hypot(a[0] + v[0] * at, a[1] + v[1] * at);
            (from + start, from + end, from + at, distance)
        };
        if let Some(previous) = answer.last_mut().filter(|previous| start <= previous.to) {
            previous.to = previous.to.max(end);
            if distance_m < previous.distance_m {
                previous.closest = closest;
                previous.distance_m = distance_m;
            }
        } else {
            answer.push(Encounter {
                from: start,
                to: end,
                closest,
                distance_m,
            });
        }
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refinement_aligns_exact_epoch_differences_and_keeps_overlap_anchors() -> anyhow::Result<()> {
        let epoch = 9_007_199_254_740_993;
        let left = Motion {
            from: epoch,
            to: epoch + 10,
            group: 0,
            points: [[0.0, 0.0], [10.0, 0.0]],
        };
        let right = Motion {
            from: epoch + 3,
            to: epoch + 7,
            group: 1,
            points: [[3.0, 0.0], [7.0, 0.0]],
        };
        // Clipping moves along the arc, so the clipped ends round; a
        // micrometre radius absorbs that.
        let metric = Metric::FixedLatitudeEquirectangular {
            reference_latitude: 0.0,
            within_m: 1e-6,
        };
        let answer = refine(&left, &right, &metric, &mut GeometryWork::default())?;
        assert_eq!((answer.from, answer.to), (epoch + 3, epoch + 7));
        for (clipped, exact) in answer.left.iter().zip(right.points) {
            assert!((clipped[0] - exact[0]).abs() <= 1e-12 && clipped[1] == exact[1]);
        }
        assert_eq!(answer.right, right.points);
        assert_eq!(answer.intervals.len(), 1);
        assert_eq!(
            (answer.intervals[0].from, answer.intervals[0].to),
            (0.0, 1.0)
        );
        Ok(())
    }

    #[test]
    fn refinement_handles_full_int64_spans_without_epoch_rounding() -> anyhow::Result<()> {
        let left = Motion {
            from: i64::MIN,
            to: i64::MAX,
            group: 0,
            points: [[-1.0, 0.0], [1.0, 0.0]],
        };
        let right = Motion {
            from: 0,
            to: 0,
            group: 1,
            points: [[0.0, 0.0]; 2],
        };
        let metric = Metric::LinearGeodesic { within_m: 1.0 };
        let answer = refine(&left, &right, &metric, &mut GeometryWork::default())?;
        assert_eq!((answer.from, answer.to), (0, 0));
        assert_eq!(answer.left, right.points);
        assert!(!answer.intervals.is_empty());
        Ok(())
    }

    #[test]
    fn invalid_motion_is_refused_even_for_empty_temporal_overlap() -> anyhow::Result<()> {
        let left = Motion {
            from: 0,
            to: 0,
            group: 0,
            points: [[0.0, 0.0], [1.0, 0.0]],
        };
        let right = Motion {
            from: 2,
            to: 3,
            group: 1,
            points: [[0.0, 0.0]; 2],
        };
        let metric = Metric::LinearGeodesic { within_m: 1.0 };
        assert!(refine(&left, &right, &metric, &mut GeometryWork::default()).is_err());
        assert!(candidates(&[left, right], &metric, 10, &mut GeometryWork::default()).is_err());
        let seam = Motion {
            from: 0,
            to: 0,
            group: 0,
            points: [[-180.0, 2.0], [180.0, 2.0]],
        };
        seam.validate()?;
        Ok(())
    }

    #[test]
    fn spatial_time_candidates_cover_native_contacts_and_seam_aliases() -> anyhow::Result<()> {
        let epoch = 9_007_199_254_740_993i64;
        let paths = [
            [[179.0, 0.0], [-179.0, 0.0]],
            [[-179.0, 0.0], [179.0, 0.0]],
            [[180.0, 0.0]; 2],
            [[-180.0, 0.0]; 2],
            [[0.0, 90.0]; 2],
            [[180.0, 90.0]; 2],
            [[-1.0, 0.0], [1.0, 0.0]],
            [[0.0, 0.0]; 2],
            [[0.0, 1.0]; 2],
        ];
        let mut motions: Vec<_> = paths
            .into_iter()
            .enumerate()
            .map(|(group, points)| Motion {
                from: epoch,
                to: epoch + 20,
                group,
                points,
            })
            .collect();
        motions.push(Motion {
            from: epoch + 10,
            to: epoch + 10,
            group: 20,
            points: [[180.0, 0.0]; 2],
        });
        motions.push(Motion {
            from: epoch + 21,
            to: epoch + 22,
            group: 21,
            points: [[0.0, 0.0]; 2],
        });
        for latitude in [-90.0, 0.0, 45.0, 90.0] {
            for radius in [0.0, 1.0, 100_000_000.0] {
                let selected: std::collections::BTreeSet<_> = candidates(
                    &motions,
                    &Metric::FixedLatitudeEquirectangular {
                        reference_latitude: latitude,
                        within_m: radius,
                    },
                    4000,
                    &mut GeometryWork::default(),
                )?
                .into_iter()
                .collect();
                for i in 0..motions.len() {
                    for j in i + 1..motions.len() {
                        let (a, b) = (&motions[i], &motions[j]);
                        let from = a.from.max(b.from);
                        let to = a.to.min(b.to);
                        if from > to || a.group == b.group {
                            continue;
                        }
                        let at = |motion: &Motion, time: i64| {
                            crate::trajectory::position_at(
                                motion.points[0],
                                motion.points[1],
                                if motion.from == motion.to {
                                    0.0
                                } else {
                                    (i128::from(time) - i128::from(motion.from)) as f64
                                        / (i128::from(motion.to) - i128::from(motion.from)) as f64
                                },
                            )
                        };
                        let exact = intervals(
                            [at(a, from), at(a, to)],
                            [at(b, from), at(b, to)],
                            latitude,
                            radius,
                            &mut GeometryWork::default(),
                        )?;
                        if !exact.is_empty() {
                            assert!(
                                selected.contains(&(i, j)),
                                "missing pair {i}/{j} at {latitude}/{radius}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn packed_candidates_prune_wide_fleets_and_disjoint_exact_instants() -> anyhow::Result<()> {
        let epoch = 9_007_199_254_740_993i64;
        let wide: Vec<_> = (0..1000)
            .map(|group| Motion {
                from: epoch,
                to: epoch + 10,
                group,
                points: [[-170.0 + group as f64 / 3.0, 0.0]; 2],
            })
            .collect();
        assert!(candidates(
            &wide,
            &Metric::FixedLatitudeEquirectangular {
                reference_latitude: 0.0,
                within_m: 1.0
            },
            1,
            &mut GeometryWork::default()
        )?
        .is_empty());
        let disjoint: Vec<_> = (0..1000)
            .map(|group| Motion {
                from: epoch + group as i64 * 2,
                to: epoch + group as i64 * 2,
                group,
                points: [[0.0, 0.0]; 2],
            })
            .collect();
        assert!(candidates(
            &disjoint,
            &Metric::FixedLatitudeEquirectangular {
                reference_latitude: 0.0,
                within_m: 0.0
            },
            1,
            &mut GeometryWork::default()
        )?
        .is_empty());
        let dense: Vec<_> = (0..3)
            .map(|group| Motion {
                from: epoch,
                to: epoch + 10,
                group,
                points: [[0.0, 0.0]; 2],
            })
            .collect();
        assert!(candidates(
            &dense,
            &Metric::FixedLatitudeEquirectangular {
                reference_latitude: 0.0,
                within_m: 0.0
            },
            1,
            &mut GeometryWork::default()
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn encounters_are_continuous_and_keep_tangencies() -> anyhow::Result<()> {
        // Metres per degree east along the equator.
        let metres = crate::geodesy::SEMI_MAJOR_AXIS_METRES.to_radians();
        let crossing = intervals(
            [[-1.0, 0.0], [1.0, 0.0]],
            [[0.0, 0.0]; 2],
            0.0,
            metres / 2.0,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(crossing.len(), 1);
        // Metric scaling and the square root can round the analytic roots by
        // a binary64 step. Contact topology and the closest instant stay exact.
        assert!((crossing[0].from - 0.25).abs() <= 2.0 * f64::EPSILON);
        assert!((crossing[0].to - 0.75).abs() <= 2.0 * f64::EPSILON);
        assert_eq!(crossing[0].closest, 0.5);
        assert_eq!(crossing[0].distance_m, 0.0);
        let tangent = intervals(
            [[-1.0, 1.0], [1.0, 1.0]],
            [[0.0, 0.0]; 2],
            0.0,
            crate::geodesy::meridional_radius(0.0).to_radians(),
            &mut GeometryWork::default(),
        )?;
        assert_eq!((tangent[0].from, tangent[0].to), (0.5, 0.5));
        Ok(())
    }

    #[test]
    fn encounters_resolve_seams_and_metric_degeneracy() -> anyhow::Result<()> {
        for sign in [-1.0, 1.0] {
            let crossing = intervals(
                [[179.0 * sign, 0.0], [-179.0 * sign, 0.0]],
                [[180.0, 0.0]; 2],
                0.0,
                0.0,
                &mut GeometryWork::default(),
            )?;
            assert_eq!(crossing.len(), 1);
            assert_eq!((crossing[0].from, crossing[0].to), (0.5, 0.5));
        }
        assert_eq!(
            intervals(
                [[-180.0, 0.0]; 2],
                [[180.0, 0.0]; 2],
                0.0,
                0.0,
                &mut GeometryWork::default()
            )?[0]
                .to,
            1.0
        );
        assert_eq!(
            intervals(
                [[0.0, 90.0]; 2],
                [[180.0, 90.0]; 2],
                90.0,
                0.0,
                &mut GeometryWork::default()
            )?[0]
                .to,
            1.0
        );
        assert!(intervals(
            [[0.0, 0.0]; 2],
            [[0.0, 1.0]; 2],
            0.0,
            1.0,
            &mut GeometryWork::default()
        )?
        .is_empty());
        assert!(intervals(
            [[0.0, 0.0]; 2],
            [[0.0, 0.0]; 2],
            f64::NAN,
            0.0,
            &mut GeometryWork::default()
        )
        .is_err());
        Ok(())
    }
}
