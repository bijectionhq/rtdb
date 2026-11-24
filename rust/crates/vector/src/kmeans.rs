use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

/// A partition with at most this many rows is one cluster: its members are all candidates.
pub(crate) const FLAT_ROWS: usize = 256;
const MAX_CLUSTERS: usize = 1024;
const ITERATIONS: usize = 8;
const TRAINING_ROWS_PER_CLUSTER: usize = 32;
const MAX_TRAINING_ROWS: usize = 32_768;

/// The number of clusters for a partition of `rows` unit vectors: about the square root of the rows.
pub(crate) fn cluster_count(rows: usize) -> usize {
    if rows <= FLAT_ROWS { return 1; }
    ((rows as f64).sqrt().round() as usize).clamp(1, MAX_CLUSTERS)
}

/// Spherical k-means over unit vectors, with every choice a function of the input order.
/// Seeds are evenly spaced rows, training runs on an evenly spaced sample, ties go to the lower cluster,
/// and every sum accumulates in f64 in row order, so the same rows in the same order give the same bytes.
/// Returns the unit centroids, flattened, and each row's cluster.
pub(crate) fn train(rows: &[&[f32]], dimensions: usize, cancellation: &CancellationToken) -> Result<(Vec<f32>, Vec<u32>)> {
    let clusters = cluster_count(rows.len()).min(rows.len()).max(1);
    if rows.is_empty() { return Ok((Vec::new(), Vec::new())); }
    let mut centroids: Vec<f32> = (0..clusters).flat_map(|cluster| rows[cluster * rows.len() / clusters].iter().copied()).collect();
    if clusters > 1 {
        let sample_size = clusters.saturating_mul(TRAINING_ROWS_PER_CLUSTER).min(MAX_TRAINING_ROWS).min(rows.len());
        let sample: Vec<&[f32]> = (0..sample_size).map(|index| rows[index * rows.len() / sample_size]).collect();
        let mut assignment = vec![u32::MAX; sample.len()];
        for _ in 0..ITERATIONS {
            if cancellation.is_cancelled() { return Err(cancelled()); }
            let mut changed = false;
            for (row, slot) in sample.iter().zip(assignment.iter_mut()) {
                let nearest = nearest(row, &centroids, dimensions);
                changed |= *slot != nearest;
                *slot = nearest;
            }
            if !changed { break; }
            let mut sums = vec![0f64; clusters * dimensions];
            for (row, cluster) in sample.iter().zip(&assignment) {
                let sum = &mut sums[*cluster as usize * dimensions..(*cluster as usize + 1) * dimensions];
                for (total, component) in sum.iter_mut().zip(row.iter()) { *total += f64::from(*component); }
            }
            for (cluster, sum) in sums.chunks_exact(dimensions).enumerate() {
                let norm = sum.iter().map(|value| value * value).sum::<f64>().sqrt();
                // An empty or cancelling cluster keeps its previous centroid.
                if !(norm.is_finite() && norm > 0.0) { continue; }
                for (centroid, total) in centroids[cluster * dimensions..(cluster + 1) * dimensions].iter_mut().zip(sum) {
                    *centroid = (total / norm) as f32;
                }
            }
        }
    }
    let mut assignment = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        if index % 4096 == 0 && cancellation.is_cancelled() { return Err(cancelled()); }
        assignment.push(nearest(row, &centroids, dimensions));
    }
    Ok((centroids, assignment))
}

/// The cluster whose centroid has the largest inner product with the row; the lower index wins a tie.
fn nearest(row: &[f32], centroids: &[f32], dimensions: usize) -> u32 {
    let mut best = (0u32, f64::NEG_INFINITY);
    for (cluster, centroid) in centroids.chunks_exact(dimensions).enumerate() {
        let similarity = row.iter().zip(centroid).map(|(left, right)| f64::from(*left) * f64::from(*right)).sum::<f64>();
        if similarity > best.1 { best = (cluster as u32, similarity); }
    }
    best.0
}

pub(crate) fn cancelled() -> Anomaly {
    Anomaly::new(AnomalyCategory::Interrupted, "rtdb.vector/cancelled", "Vector generation work was cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(seed: u64, dimensions: usize) -> Vec<f32> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut values: Vec<f64> = (0..dimensions).map(|_| {
            state ^= state << 13; state ^= state >> 7; state ^= state << 17;
            (state % 2001) as f64 - 1000.0
        }).collect();
        let norm = values.iter().map(|value| value * value).sum::<f64>().sqrt();
        values.iter_mut().for_each(|value| *value /= norm);
        values.into_iter().map(|value| value as f32).collect()
    }

    #[test]
    fn training_is_a_function_of_the_rows_in_order() {
        let rows: Vec<Vec<f32>> = (0..2_000).map(|seed| unit(seed, 8)).collect();
        let borrowed: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let token = CancellationToken::new();
        let first = train(&borrowed, 8, &token).unwrap();
        let second = train(&borrowed, 8, &token).unwrap();
        assert_eq!(first.0.iter().map(|value| value.to_bits()).collect::<Vec<_>>(), second.0.iter().map(|value| value.to_bits()).collect::<Vec<_>>());
        assert_eq!(first.1, second.1);
        assert_eq!(first.0.len(), cluster_count(2_000) * 8);
    }

    #[test]
    fn a_small_partition_is_one_cluster() {
        let rows: Vec<Vec<f32>> = (0..FLAT_ROWS as u64).map(|seed| unit(seed, 4)).collect();
        let borrowed: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let (centroids, assignment) = train(&borrowed, 4, &CancellationToken::new()).unwrap();
        assert_eq!(centroids.len(), 4);
        assert!(assignment.iter().all(|cluster| *cluster == 0));
    }
}
