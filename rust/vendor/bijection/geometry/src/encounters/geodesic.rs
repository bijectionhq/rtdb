//! WGS84 geodesic separation of two points moving along their great elliptic
//! arcs at constant auxiliary-sphere angular speed. Critical points come from
//! the analytic derivative of the auxiliary-sphere dot product, with bounded
//! subdivision rather than a grid of sampled positions; separations between
//! them are ellipsoidal.
use super::Encounter;
use crate::{
    predicates::point_distance,
    trajectory::position_at,
    GeometryRefusal,
    GeometryWork,
    Pos,
};

pub const PROFILE: &str = "wgs84-linear-geodesic-v1";
/// Boundary isolation resolution between the exact overlap anchors.
/// This is numerical precision, independently of reported position accuracy.
pub const FRACTION_PRECISION: f64 = 2.2737367544323206e-13; // 2^-42
pub const DISTANCE_PRECISION: f64 = 64.0 * f64::EPSILON * crate::geodesy::SEMI_MAJOR_AXIS_METRES;
const CRITICAL_DEPTH: u32 = 50;

#[derive(Clone, Copy)]
struct Wave {
    frequency: f64,
    cosine: f64,
    sine: f64,
}

/// The auxiliary-sphere frame of a motion: unit start, unit tangent and the
/// angle swept, zero for a stationary point.
fn motion(path: [Pos; 2]) -> ([f64; 3], [f64; 3], f64) {
    crate::measure::arc_frame(path[0], path[1]).unwrap_or_else(|_| {
        (
            crate::arcs::unit(crate::arcs::direction(path[0])),
            [0.0; 3],
            0.0,
        )
    })
}

/// u₁·u₂ for u = a cos(ωt) + t sin(ωt): the products expand into waves at
/// ω₁ − ω₂ and ω₁ + ω₂. Its critical points are the separation's, and equal
/// frequencies combine, including exact cancellation for shared motion.
fn derivative_waves(left: [Pos; 2], right: [Pos; 2]) -> Vec<Wave> {
    let (a1, t1, w1) = motion(left);
    let (a2, t2, w2) = motion(right);
    let dot = |x: [f64; 3], y: [f64; 3]| x[0] * y[0] + x[1] * y[1] + x[2] * y[2];
    let (aa, at, ta, tt) = (dot(a1, a2), dot(a1, t2), dot(t1, a2), dot(t1, t2));
    let mut waves: Vec<Wave> = Vec::new();
    for (frequency, cosine, sine) in [
        (w1 - w2, 0.5 * (aa + tt), 0.5 * (ta - at)),
        (w1 + w2, 0.5 * (aa - tt), 0.5 * (at + ta)),
    ] {
        if frequency == 0.0 {
            continue;
        }
        let sine = sine * frequency.signum();
        let frequency = frequency.abs();
        if let Some(wave) = waves.iter_mut().find(|wave| wave.frequency == frequency) {
            wave.cosine += cosine;
            wave.sine += sine;
        } else {
            waves.push(Wave {
                frequency,
                cosine,
                sine,
            });
        }
    }
    // Unit vectors round, so shared motion leaves residues near ε rather
    // than exact zeros; they move the separation by under DISTANCE_PRECISION.
    waves.retain(|wave| libm::hypot(wave.cosine, wave.sine) > 16.0 * f64::EPSILON);
    waves
}

fn critical_points(waves: &[Wave], work: &mut GeometryWork) -> Result<Vec<f64>, GeometryRefusal> {
    let first_bound: f64 = waves
        .iter()
        .map(|wave| libm::hypot(wave.cosine, wave.sine) * wave.frequency)
        .sum();
    let second_bound: f64 = waves
        .iter()
        .map(|wave| libm::hypot(wave.cosine, wave.sine) * crate::square(wave.frequency))
        .sum();
    if second_bound == 0.0 {
        return Ok(Vec::new());
    }
    work.allocate(4096)?;
    let mut pending = vec![(0.0, 1.0, 0)];
    let mut roots: Vec<(f64, f64)> = Vec::new();
    while let Some((from, to, depth)) = pending.pop() {
        work.charge(1 + waves.len() * 4)?;
        let middle = (from + to) * 0.5;
        let derivative: f64 = waves
            .iter()
            .map(|wave| {
                let phase = wave.frequency * middle;
                wave.frequency * (wave.sine * libm::cos(phase) - wave.cosine * libm::sin(phase))
            })
            .sum();
        // Mean-value bound of the derivative over the whole interval. The
        // relative roundoff allowance is scaled to these combined waves.
        let error = second_bound * (to - from) * 0.5 + first_bound * 16.0 * f64::EPSILON;
        if derivative.abs() > error {
            continue;
        }
        if depth == CRITICAL_DEPTH || middle == from || middle == to {
            if let Some(previous) = roots
                .last_mut()
                .filter(|previous| to - previous.0 <= FRACTION_PRECISION)
            {
                previous.1 = to;
            } else {
                work.allocate(256)?;
                roots.push((from, to));
            }
        } else {
            // LIFO visits intervals in ascending order; stack depth is at
            // most 51. Resource exhaustion propagates without a prefix.
            pending.push((middle, to, depth + 1));
            pending.push((from, middle, depth + 1));
        }
    }
    Ok(roots
        .into_iter()
        .map(|(from, to)| (from + to) * 0.5)
        .collect())
}

fn distance(left: [Pos; 2], right: [Pos; 2], fraction: f64) -> f64 {
    point_distance(
        position_at(left[0], left[1], fraction),
        position_at(right[0], right[1], fraction),
    )
}

/// Instants where the two positions coincide: an end, or a critical point of
/// the separation at zero.
fn zero_contacts(
    left: [Pos; 2],
    right: [Pos; 2],
    waves: &[Wave],
    work: &mut GeometryWork,
) -> Result<Vec<f64>, GeometryRefusal> {
    let mut candidates = vec![0.0, 1.0];
    candidates.extend(critical_points(waves, work)?);
    Ok(candidates
        .into_iter()
        .filter(|at| distance(left, right, *at) <= DISTANCE_PRECISION)
        .collect())
}

pub(super) fn validate_radius(within_m: f64) -> Result<(), GeometryRefusal> {
    if !within_m.is_finite() || !(0.0..=100_000_000.0).contains(&within_m) {
        return Err(crate::invalid(
            "Geodesic encounters require radius 0..100000000 metres",
        ));
    }
    Ok(())
}

/// All positions must already pass ground-profile admission.
pub fn intervals(
    left: [Pos; 2],
    right: [Pos; 2],
    within_m: f64,
    work: &mut GeometryWork,
) -> Result<Vec<Encounter>, GeometryRefusal> {
    validate_radius(within_m)?;
    work.charge(128)?;
    work.allocate(2048)?;
    if left[0][1].abs() == 90.0
        && left
            .iter()
            .chain(right.iter())
            .all(|point| point[1] == left[0][1])
    {
        return Ok(vec![Encounter {
            from: 0.0,
            to: 1.0,
            closest: 0.0,
            distance_m: 0.0,
        }]);
    }
    let waves = derivative_waves(left, right);
    if waves.is_empty() {
        let value = distance(left, right, 0.0);
        return Ok(if value <= within_m + DISTANCE_PRECISION {
            vec![Encounter {
                from: 0.0,
                to: 1.0,
                closest: 0.0,
                distance_m: value,
            }]
        } else {
            Vec::new()
        });
    }
    if within_m == 0.0 {
        let mut contacts = zero_contacts(left, right, &waves, work)?;
        contacts.sort_by(f64::total_cmp);
        contacts.dedup();
        return Ok(contacts
            .into_iter()
            .map(|at| Encounter {
                from: at,
                to: at,
                closest: at,
                distance_m: distance(left, right, at),
            })
            .collect());
    }
    let mut cuts = vec![0.0];
    let endpoint_distances = [distance(left, right, 0.0), distance(left, right, 1.0)];
    cuts.extend(critical_points(&waves, work)?.into_iter().filter(|at| {
        !(*at <= FRACTION_PRECISION && endpoint_distances[0] == within_m)
            && !(1.0 - *at <= FRACTION_PRECISION && endpoint_distances[1] == within_m)
    }));
    cuts.push(1.0);
    cuts.sort_by(f64::total_cmp);
    cuts.dedup();
    let distances = cuts
        .iter()
        .map(|at| distance(left, right, *at))
        .collect::<Vec<_>>();
    let mut tangent = vec![false; cuts.len()];
    for index in 1..cuts.len() - 1 {
        let value = distances[index];
        tangent[index] = value <= distances[index - 1]
            && value <= distances[index + 1]
            && distances[index - 1] > within_m
            && distances[index + 1] > within_m
            && (value - within_m).abs() <= DISTANCE_PRECISION;
    }
    let mut pieces = Vec::new();
    for index in 0..cuts.len() - 1 {
        work.charge(8)?;
        let (from, to) = (cuts[index], cuts[index + 1]);
        let inside_from = !tangent[index] && distances[index] <= within_m;
        let inside_to = !tangent[index + 1] && distances[index + 1] <= within_m;
        let (start, end) = match (inside_from, inside_to) {
            (false, false) => continue,
            (true, true) => (from, to),
            (true, false) if distances[index] == within_m => (from, from),
            (false, true) if distances[index + 1] == within_m => (to, to),
            (is_inside, _) => {
                let (mut low, mut high) = (from, to);
                while high - low > FRACTION_PRECISION {
                    work.charge(32)?;
                    let middle = (low + high) * 0.5;
                    if (distance(left, right, middle) <= within_m) == is_inside {
                        low = middle;
                    } else {
                        high = middle;
                    }
                }
                let boundary = (low + high) * 0.5;
                if is_inside {
                    (from, boundary)
                } else {
                    (boundary, to)
                }
            },
        };
        let closest = if distances[index] <= distances[index + 1] {
            from
        } else {
            to
        };
        pieces.push(Encounter {
            from: start,
            to: end,
            closest,
            distance_m: distance(left, right, closest),
        });
    }
    for (index, at) in cuts.iter().copied().enumerate() {
        if tangent[index]
            || ((index == 0 || distances[index] <= distances[index - 1])
                && (index + 1 == cuts.len() || distances[index] <= distances[index + 1])
                && distances[index] >= within_m
                && distances[index] - within_m <= DISTANCE_PRECISION
                && (index == 0 || distances[index - 1] > within_m)
                && (index + 1 == cuts.len() || distances[index + 1] > within_m))
        {
            pieces.push(Encounter {
                from: at,
                to: at,
                closest: at,
                distance_m: distances[index],
            });
        }
    }
    work.charge(
        pieces
            .len()
            .saturating_mul(pieces.len().max(2).ilog2() as usize + 1),
    )?;
    pieces.sort_by(|a, b| a.from.total_cmp(&b.from).then(a.to.total_cmp(&b.to)));
    let mut answer: Vec<Encounter> = Vec::new();
    for piece in pieces {
        if let Some(previous) = answer
            .last_mut()
            .filter(|previous| piece.from <= previous.to)
        {
            previous.to = previous.to.max(piece.to);
            if piece.distance_m < previous.distance_m {
                previous.closest = piece.closest;
                previous.distance_m = piece.distance_m;
            }
        } else {
            answer.push(piece);
        }
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicates::distance_slack;

    #[test]
    fn spherical_crossings_and_tangencies_keep_continuous_contacts() -> Result<(), GeometryRefusal>
    {
        let path = [[-1.0, 0.0], [1.0, 0.0]];
        let origin = [[0.0, 0.0]; 2];
        // The equator is a geodesic of length a per radian of longitude.
        let radius = crate::geodesy::SEMI_MAJOR_AXIS_METRES * 0.25_f64.to_radians();
        let selected = intervals(path, origin, radius, &mut GeometryWork::default())?;
        assert_eq!(selected.len(), 1);
        assert!((selected[0].from - 0.375).abs() <= FRACTION_PRECISION);
        assert!((selected[0].to - 0.625).abs() <= FRACTION_PRECISION);
        assert!((selected[0].closest - 0.5).abs() <= FRACTION_PRECISION);
        assert!(selected[0].distance_m < 1e-5);
        let target = [[0.0, 1.0]; 2];
        let radius = point_distance([0.0, 0.0], target[0]);
        let tangent = intervals(path, target, radius, &mut GeometryWork::default())?;
        assert_eq!(tangent.len(), 1);
        assert!((tangent[0].from - 0.5).abs() <= FRACTION_PRECISION);
        assert_eq!(tangent[0].from, tangent[0].to);
        let upper = intervals(
            [[-1.0, 0.0], [0.0, 0.0]],
            target,
            radius,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(upper.len(), 1);
        assert_eq!(
            (upper[0].from, upper[0].to),
            (1.0, 1.0),
            "an endpoint contact must retain the exact excluded upper anchor"
        );
        let lower = intervals(
            [[0.0, 0.0], [1.0, 0.0]],
            target,
            radius,
            &mut GeometryWork::default(),
        )?;
        assert_eq!((lower[0].from, lower[0].to), (0.0, 0.0));
        Ok(())
    }

    #[test]
    fn spherical_zero_radius_keeps_seams_poles_and_shared_movement() -> Result<(), GeometryRefusal>
    {
        let seam = intervals(
            [[179.0, 0.0], [-179.0, 0.0]],
            [[180.0, 0.0]; 2],
            0.0,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(seam.len(), 1);
        assert_eq!((seam[0].from, seam[0].to), (0.5, 0.5));
        let path = [[-30.0, -45.0], [130.0, 72.0]];
        let shared = intervals(path, path, 0.0, &mut GeometryWork::default())?;
        assert_eq!(shared.len(), 1);
        assert_eq!(
            (shared[0].from, shared[0].to, shared[0].distance_m),
            (0.0, 1.0, 0.0)
        );
        // A pole's longitude is not part of its position, so these paths
        // share their arc.
        let poles = intervals(
            [[0.0, -90.0], [0.0, -10.0]],
            [[90.0, -90.0], [0.0, -10.0]],
            0.0,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(poles.len(), 1);
        assert_eq!((poles[0].from, poles[0].to), (0.0, 1.0));
        let pole = intervals(
            [[0.0, 90.0], [179.0, 90.0]],
            [[-40.0, 90.0], [-179.0, 90.0]],
            0.0,
            &mut GeometryWork::default(),
        )?;
        assert_eq!(
            (pole[0].from, pole[0].to, pole[0].distance_m),
            (0.0, 1.0, 0.0)
        );
        Ok(())
    }

    #[test]
    fn spherical_intervals_agree_with_point_distance_away_from_boundaries(
    ) -> Result<(), GeometryRefusal> {
        let mut seed = 41u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 11) as f64) / ((1u64 << 53) as f64)
        };
        for _ in 0..32 {
            let mut point = || [next() * 360.0 - 180.0, next() * 170.0 - 85.0];
            let left = [point(), point()];
            let right = [point(), point()];
            let radius = 6_000_000.0;
            let selected = intervals(left, right, radius, &mut GeometryWork::default())?;
            for step in 0..=256 {
                let at = f64::from(step) / 256.0;
                let separation = distance(left, right, at);
                if (separation - radius).abs() <= distance_slack(separation) {
                    continue;
                }
                assert_eq!(
                    selected
                        .iter()
                        .any(|interval| interval.from <= at && at <= interval.to),
                    separation < radius,
                    "{left:?} {right:?} at {at}"
                );
            }
            for interval in selected {
                assert!(interval.from <= interval.closest && interval.closest <= interval.to);
                assert!(
                    (distance(left, right, interval.closest) - interval.distance_m).abs() < 1e-6
                );
            }
        }
        Ok(())
    }

    #[test]
    fn spherical_bounds_refuse_complete_results() -> Result<(), GeometryRefusal> {
        let mut work = GeometryWork::default();
        work.charge(crate::TierBound::WorkSteps.limit() as usize)?;
        assert!(intervals([[0.0, 0.0]; 2], [[0.0, 0.0]; 2], 1.0, &mut work)
            .unwrap_err()
            .bound
            .is_some());
        for radius in [-1.0, f64::INFINITY, f64::NAN, 100_000_001.0] {
            assert!(intervals(
                [[0.0, 0.0]; 2],
                [[0.0, 0.0]; 2],
                radius,
                &mut GeometryWork::default()
            )
            .is_err());
        }
        Ok(())
    }
}
