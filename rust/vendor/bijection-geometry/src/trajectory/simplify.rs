//! Derived track simplification measured against the native drawn segment, a
//! great elliptic arc. Candidate lower and upper bounds avoid refining points
//! that cannot win a split; every refinement uses the shared arc distance.
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    arcs,
    predicates::{
        distance_slack,
        point_distance,
        segment_distance::{
            arc_position,
            lower_bound,
        },
    },
    GeometryRefusal,
    GeometrySpace,
    GeometryWork,
    Pos,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Simplification {
    pub tolerance_m: f64,
    pub kept: Vec<Vec<u32>>,
}

fn distance(point: Pos, a: Pos, b: Pos, work: &mut GeometryWork) -> Result<f64, GeometryRefusal> {
    work.scoped(|work| {
        crate::predicates::segments_distance(
            &GeometrySpace::wgs84_xy(),
            [point, point],
            [a, b],
            work,
        )
    })
}

fn significance(
    points: &[Pos],
    tolerance: f64,
    work: &mut GeometryWork,
) -> Result<Vec<f64>, GeometryRefusal> {
    work.allocate(points.len().saturating_mul(40))?;
    let mut values = vec![f64::INFINITY; points.len()];
    if points.len() < 3 {
        return Ok(values);
    }
    let mut stack = vec![(0, points.len() - 1, f64::INFINITY)];
    while let Some((first, last, cap)) = stack.pop() {
        work.charge(1)?;
        if last - first < 2 {
            continue;
        }
        let (a, b) = (points[first], points[last]);
        let (av, bv) = (arcs::direction(a), arcs::direction(b));
        let (index, best) = work.scoped(|work| {
            work.allocate((last - first - 1).saturating_mul(64))?;
            // Each point's auxiliary-sphere foot gives an upper bound; b times
            // its auxiliary angle a lower one.
            let mut near = Vec::with_capacity(last - first - 1);
            let mut floor = f64::NEG_INFINITY;
            for point in &points[first + 1..last] {
                work.charge(4)?;
                let (angle, along) = arcs::nearest_on_arc(arcs::direction(*point), av, bv);
                let upper = point_distance(*point, arc_position(a, b, along));
                floor = floor.max(lower_bound(angle));
                near.push((*point, upper));
            }
            let (mut index, mut best) = (first + 1, -1.0);
            for (i, (point, upper)) in near.into_iter().enumerate() {
                if upper + distance_slack(upper) < floor {
                    continue;
                }
                let candidate = distance(point, a, b, work)?;
                if candidate > best {
                    best = candidate;
                    index = first + 1 + i;
                }
            }
            Ok::<_, GeometryRefusal>((index, best))
        })?;
        let bounded = best.min(cap);
        if bounded <= tolerance {
            work.charge(last - first - 1)?;
            values[first + 1..last].fill(bounded);
        } else {
            values[index] = bounded;
            stack.push((first, index, bounded));
            stack.push((index, last, bounded));
        }
    }
    Ok(values)
}

pub fn simplify(
    runs: &[Vec<Pos>],
    tolerance_m: f64,
    max_points: usize,
    work: &mut GeometryWork,
) -> Result<Simplification, GeometryRefusal> {
    let count = runs.iter().map(Vec::len).sum::<usize>();
    if count > 10000
        || runs.len() > 10000
        || !(2..=10000).contains(&max_points)
        || !tolerance_m.is_finite()
        || !(0.0..=100000.0).contains(&tolerance_m)
    {
        return Err(crate::invalid(
            "Invalid trajectory simplification allowance or tolerance",
        ));
    }
    for point in runs.iter().flatten() {
        super::validate_position(*point)?;
    }
    work.charge(count)?;
    work.allocate(
        count
            .saturating_mul(64)
            .saturating_add(runs.len().saturating_mul(64)),
    )?;
    let required = runs.iter().map(|run| run.len().min(2)).sum::<usize>();
    if required > max_points {
        return Err(crate::invalid("Trajectory run endpoints exceed max_points"));
    }
    let values = runs
        .iter()
        .map(|run| significance(run, tolerance_m, work))
        .collect::<Result<Vec<_>, _>>()?;
    let mut interior = values
        .iter()
        .flatten()
        .copied()
        .filter(|v| v.is_finite())
        .collect::<Vec<_>>();
    work.charge(
        interior
            .len()
            .saturating_mul(interior.len().max(2).ilog2() as usize + 1),
    )?;
    interior.sort_by(|a, b| b.total_cmp(a));
    let tolerance_m =
        if required + interior.iter().filter(|v| **v > tolerance_m).count() > max_points {
            interior[max_points - required]
        } else {
            tolerance_m
        };
    let kept = values
        .into_iter()
        .map(|run| {
            run.into_iter()
                .enumerate()
                .filter_map(|(i, value)| (value > tolerance_m).then_some(i as u32))
                .collect()
        })
        .collect();
    Ok(Simplification { tolerance_m, kept })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simplification_preserves_source_indices_and_refuses_without_a_proof() -> anyhow::Result<()> {
        let runs = vec![
            vec![[179.0, 0.0], [-179.5, 0.0], [-178.0, 0.0]],
            vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]],
        ];
        let result = simplify(&runs, 1.0, 10, &mut GeometryWork::default())?;
        assert_eq!(result.kept, vec![vec![0, 2], vec![0, 1, 2]]);
        let reduced = simplify(&runs, 1.0, 4, &mut GeometryWork::default())?;
        assert_eq!(reduced.kept, vec![vec![0, 2], vec![0, 2]]);
        assert!(reduced.tolerance_m > 1.0);
        assert!(simplify(&runs, 1.0, 3, &mut GeometryWork::default()).is_err());
        assert!(simplify(&runs, 1.0, 10, &mut GeometryWork::with_limits(0, 1 << 20)).is_err());
        Ok(())
    }
}
