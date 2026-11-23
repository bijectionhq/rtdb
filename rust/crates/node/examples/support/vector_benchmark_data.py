import argparse
from pathlib import Path

import h5py
import numpy as np
import pyarrow as pa


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


def write_embeddings(path, vectors, clusters=None):
    children = pa.array(vectors.reshape(-1), type=pa.float32())
    item = pa.field("item", pa.float32(), nullable=False)
    vector_type = pa.list_(item, vectors.shape[1])
    arrays = [pa.FixedSizeListArray.from_arrays(children, type=vector_type)]
    fields = [pa.field("embedding", vector_type, nullable=False)]
    if clusters is not None:
        arrays.append(pa.array(clusters, type=pa.uint32()))
        fields.append(pa.field("cluster", pa.uint32(), nullable=False))
    batch = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))
    with pa.OSFile(str(path), "wb") as output, pa.ipc.new_stream(output, batch.schema) as stream:
        stream.write_batch(batch)


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
    membership = np.empty(len(train), dtype=np.uint32)
    for offset in range(0, len(train), 4096):
        batch = normalize(train[offset:offset + 4096])
        membership[offset:offset + len(batch)] = np.argmax(batch @ centroids.T, axis=1)
    write_embeddings(args.output / "vectors.arrow", train, membership)
    write_embeddings(args.output / "queries.arrow", queries)
    write_embeddings(args.output / "centroids.arrow", centroids)


if __name__ == "__main__":
    main()
