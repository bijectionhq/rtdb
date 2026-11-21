import argparse
from pathlib import Path

import h5py
import numpy as np


def normalize(vectors):
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    if np.any(~np.isfinite(norms)) or np.any(norms == 0):
        raise ValueError("The benchmark requires finite, nonzero vectors")
    return vectors / norms


def train_centroids(vectors, count, rng):
    sample_indices = rng.choice(len(vectors), min(32_768, len(vectors)), replace=False)
    samples = normalize(vectors[sample_indices])
    centroids = samples[rng.choice(len(samples), count, replace=False)].copy()
    for _ in range(12):
        nearest = np.argmax(samples @ centroids.T, axis=1)
        sums = np.zeros_like(centroids)
        np.add.at(sums, nearest, samples)
        empty = ~np.any(sums, axis=1)
        sums[empty] = samples[rng.choice(len(samples), np.count_nonzero(empty), replace=False)]
        centroids = normalize(sums)
    return centroids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--centroids", type=int, default=512)
    parser.add_argument("--queries", type=int, default=64)
    args = parser.parse_args()

    with h5py.File(args.source) as data:
        if not 1 <= args.centroids <= min(32_768, args.rows) or not 1 <= args.rows <= len(data["train"]):
            parser.error("rows and centroids must fit the training data")
        if not 1 <= args.queries <= len(data["test"]):
            parser.error("queries must fit the held-out data")
        train = np.asarray(data["train"][:args.rows], dtype="<f4")
        queries = np.asarray(data["test"][:args.queries], dtype="<f4")

    rng = np.random.default_rng(61)
    centroids = train_centroids(train, args.centroids, rng)

    args.output.mkdir(parents=True, exist_ok=True)
    train.tofile(args.output / "vectors.f32")
    queries.tofile(args.output / "queries.f32")
    centroids.astype("<f4", copy=False).tofile(args.output / "centroids.f32")
    with (args.output / "clusters.u32").open("wb") as membership:
        for offset in range(0, len(train), 4096):
            batch = normalize(train[offset:offset + 4096])
            nearest = np.argmax(batch @ centroids.T, axis=1)
            nearest.astype("<u4").tofile(membership)

    (args.output / "dimensions").write_text(f"{train.shape[1]}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
